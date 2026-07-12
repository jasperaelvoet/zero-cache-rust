//! The live COPY-streaming half of `initial-sync.ts`'s `copyBinary`/
//! `copyText`/`copyTable`: run `COPY (SELECT ...) TO STDOUT` against upstream
//! Postgres via `tokio_postgres::Client::copy_out` (a real, off-the-shelf
//! `tokio-postgres` API — logical replication needed `replication_conn.rs`'s
//! hand-rolled frontend/backend protocol handling because `tokio-postgres`
//! has no `START_REPLICATION` support, but plain `COPY ... TO STDOUT` is a
//! first-class `tokio-postgres` feature, so no protocol-level
//! reimplementation is needed here), decode each row via the ported
//! `pg_copy_binary::BinaryCopyParser` + `make_binary_decoder` (binary format)
//! or [`crate::pg_copy_text`]'s `TextCopyParser` + `make_text_decoder` (text
//! format, `ZERO_INITIAL_SYNC_TEXT_COPY`), and insert into the SQLite replica
//! via [`crate::StatementRunner`].
//!
//! The table's download is described by a [`TableCopyPlan`] (SQL + per-column
//! decoders), built once per table and consumed either sequentially
//! ([`copy_table_with_plan`]) or by one of several parallel reader
//! connections streaming decoded rows into a channel
//! ([`stream_table_to_channel`], the `ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS`
//! path driven from `initial_sync::copy_all`).

use futures_util::StreamExt;
use tokio_postgres::Client;

use zero_cache_types::initial_sync_sql::make_download_statements;
use zero_cache_types::pg_copy_binary::{
    has_binary_decoder, make_binary_decoder, text_cast_decoder, BinaryColumnSpec, BinaryCopyParser,
};
use zero_cache_types::specs::PublishedTableSpec;

use crate::initial_sync_metrics::CopyFormat;
use crate::pg_copy_text::{make_text_decoder, TextCopyParser, TextDecoder};
use crate::row_apply::to_sql_value;
use crate::{DbError, StatementRunner, Value};

/// Errors from the table-copy path.
#[derive(Debug, thiserror::Error)]
pub enum CopyTableError {
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Copy(#[from] zero_cache_types::pg_copy_binary::CopyError),
    #[error(transparent)]
    Db(#[from] DbError),
    #[error("row from COPY had {got} fields, expected {expected} (column count mismatch)")]
    FieldCountMismatch { got: usize, expected: usize },
    #[error("COPY stream ended with {0} unparsed bytes buffered")]
    TrailingBytes(usize),
}

/// Result of copying one table. Port of the row-count half of `CopyResult`
/// (the metrics/timing fields are not tracked here — no OTel dependency in
/// this port, see `observability`'s scope notes elsewhere).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyTableResult {
    pub rows: usize,
}

enum Decoder {
    Binary(zero_cache_types::pg_copy_binary::BinaryDecoder),
    Text,
}

enum RowValueDecoders {
    /// Binary COPY: per-column binary decoders (columns without one were cast
    /// to `::text` in the SELECT and use the text-cast decoder).
    Binary(Vec<Decoder>),
    /// Text COPY: per-column text decoders.
    Text(Vec<TextDecoder>),
}

/// Everything needed to copy one table: the COPY statement, the replica
/// INSERT, and the per-column decoders. Built once, then consumed by a
/// sequential copy or a parallel reader worker.
pub(crate) struct TableCopyPlan {
    pub(crate) copy_sql: String,
    pub(crate) insert_sql: String,
    pub(crate) lite_name: String,
    ncols: usize,
    decoders: RowValueDecoders,
}

/// Streaming parse state for one in-flight table copy (rows can span COPY
/// data chunks in both formats).
pub(crate) enum RowParserState {
    Binary {
        parser: BinaryCopyParser,
        pending: Vec<Option<Vec<u8>>>,
    },
    Text {
        parser: TextCopyParser,
    },
}

impl TableCopyPlan {
    /// Builds the plan for `table`. `cols` is the column order to copy — it
    /// must match `lite_table_name`'s own column order for the generated
    /// `INSERT` to bind correctly. `sample_rate`/`max_rows` inject the
    /// `TABLESAMPLE BERNOULLI` / `LIMIT` clauses used by shadow sync.
    pub(crate) fn build(
        table: &PublishedTableSpec,
        cols: &[String],
        lite_table_name: &str,
        format: CopyFormat,
        sample_rate: Option<f64>,
        max_rows: Option<i64>,
    ) -> TableCopyPlan {
        let column_spec = |col: &String| {
            table
                .columns
                .iter()
                .find(|(name, _)| name == col)
                .map(|(_, spec)| BinaryColumnSpec {
                    type_oid: spec.type_oid,
                    data_type: spec.column.data_type.clone(),
                    pg_type_class: spec.column.pg_type_class,
                    elem_pg_type_class: spec.column.elem_pg_type_class,
                })
        };

        let (select_exprs, decoders, format_suffix) = match format {
            CopyFormat::Binary => {
                let exprs =
                    zero_cache_types::initial_sync_sql::make_binary_select_exprs(table, cols);
                let decoders: Vec<Decoder> = cols
                    .iter()
                    .map(|col| match column_spec(col) {
                        Some(spec) if has_binary_decoder(&spec) => make_binary_decoder(&spec)
                            .map(Decoder::Binary)
                            .unwrap_or(Decoder::Text),
                        _ => Decoder::Text,
                    })
                    .collect();
                (
                    Some(exprs),
                    RowValueDecoders::Binary(decoders),
                    " (FORMAT binary)",
                )
            }
            CopyFormat::Text => {
                // Text format needs no `::text` casts — everything already
                // arrives as text; each column gets a typed text decoder.
                let fallback = |col: &String| BinaryColumnSpec {
                    type_oid: 0,
                    data_type: col.clone(),
                    pg_type_class: None,
                    elem_pg_type_class: None,
                };
                let decoders: Vec<TextDecoder> = cols
                    .iter()
                    .map(|col| {
                        make_text_decoder(&column_spec(col).unwrap_or_else(|| fallback(col)))
                    })
                    .collect();
                (None, RowValueDecoders::Text(decoders), "")
            }
        };

        let stmts =
            make_download_statements(table, cols, sample_rate, max_rows, select_exprs.as_deref());
        let copy_sql = format!("COPY ({}) TO STDOUT{format_suffix}", stmts.select.trim());

        let placeholders = std::iter::repeat_n("?", cols.len())
            .collect::<Vec<_>>()
            .join(",");
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            zero_cache_types::sql::id(lite_table_name),
            zero_cache_types::sql::id_list(cols.iter().map(String::as_str)),
            placeholders
        );

        TableCopyPlan {
            copy_sql,
            insert_sql,
            lite_name: lite_table_name.to_string(),
            ncols: cols.len(),
            decoders,
        }
    }

    pub(crate) fn new_parser(&self) -> RowParserState {
        match self.decoders {
            RowValueDecoders::Binary(_) => RowParserState::Binary {
                parser: BinaryCopyParser::new(),
                pending: Vec::new(),
            },
            RowValueDecoders::Text(_) => RowParserState::Text {
                parser: TextCopyParser::new(),
            },
        }
    }

    /// Decodes the rows completed by one COPY data chunk into replica bind
    /// values.
    pub(crate) fn decode_chunk(
        &self,
        state: &mut RowParserState,
        chunk: &[u8],
    ) -> Result<Vec<Vec<Value>>, CopyTableError> {
        match (state, &self.decoders) {
            (RowParserState::Binary { parser, pending }, RowValueDecoders::Binary(decoders)) => {
                pending.extend(parser.parse(chunk)?);
                let mut rows = Vec::new();
                while pending.len() >= self.ncols {
                    let row_fields: Vec<Option<Vec<u8>>> = pending.drain(0..self.ncols).collect();
                    rows.push(decode_binary_row(&row_fields, decoders)?);
                }
                Ok(rows)
            }
            (RowParserState::Text { parser }, RowValueDecoders::Text(decoders)) => parser
                .parse(chunk)
                .into_iter()
                .map(|fields| decode_text_row(&fields, decoders))
                .collect(),
            _ => unreachable!("parser state built by new_parser always matches the plan format"),
        }
    }

    /// Verifies the stream ended on a row boundary.
    pub(crate) fn finish(&self, state: &RowParserState) -> Result<(), CopyTableError> {
        match state {
            RowParserState::Binary { pending, .. } => {
                if !pending.is_empty() {
                    return Err(CopyTableError::FieldCountMismatch {
                        got: pending.len(),
                        expected: self.ncols,
                    });
                }
            }
            RowParserState::Text { parser } => {
                if parser.pending_bytes() > 0 {
                    return Err(CopyTableError::TrailingBytes(parser.pending_bytes()));
                }
            }
        }
        Ok(())
    }
}

/// Streams `plan`'s table from upstream and inserts each row into the replica
/// — the single-connection sequential path.
pub(crate) async fn copy_table_with_plan(
    pg: &Client,
    db: &StatementRunner,
    plan: &TableCopyPlan,
) -> Result<CopyTableResult, CopyTableError> {
    let stream = pg.copy_out(&plan.copy_sql).await?;
    // `CopyOutStream` is `!Unpin`; pin it on the stack. Uses `futures_util`
    // (already a dependency) rather than `tokio::pin!` so this compiles in the
    // normal build without pulling `tokio`'s macros in.
    futures_util::pin_mut!(stream);

    let mut state = plan.new_parser();
    let mut rows = 0usize;
    while let Some(chunk) = stream.next().await {
        for values in plan.decode_chunk(&mut state, &chunk?)? {
            db.run(&plan.insert_sql, &values)?;
            rows += 1;
        }
    }
    plan.finish(&state)?;
    Ok(CopyTableResult { rows })
}

/// Streams `plan`'s table on `pg`, sending each decoded row to the writer's
/// channel tagged with `table_idx` — one parallel `TableCopyWorkers` reader's
/// work for one table. Returns the number of rows streamed. A closed channel
/// (the writer hit an error and hung up) aborts quietly; the writer reports
/// its own error.
pub(crate) async fn stream_table_to_channel(
    pg: &Client,
    plan: &TableCopyPlan,
    table_idx: usize,
    tx: &tokio::sync::mpsc::Sender<(usize, Vec<Value>)>,
) -> Result<usize, CopyTableError> {
    let stream = pg.copy_out(&plan.copy_sql).await?;
    futures_util::pin_mut!(stream);

    let mut state = plan.new_parser();
    let mut rows = 0usize;
    while let Some(chunk) = stream.next().await {
        for values in plan.decode_chunk(&mut state, &chunk?)? {
            if tx.send((table_idx, values)).await.is_err() {
                return Ok(rows);
            }
            rows += 1;
        }
    }
    plan.finish(&state)?;
    Ok(rows)
}

/// Streams `table`'s rows from upstream Postgres (via a real `COPY ... TO
/// STDOUT (FORMAT binary)`) into the already-created SQLite table
/// `lite_table_name`, using a plain parameterless multi-value `INSERT` per
/// row. `cols` is the column order to copy — must match `lite_table_name`'s
/// own column order for the generated `INSERT` to bind correctly.
pub async fn copy_table_binary(
    pg: &Client,
    db: &StatementRunner,
    table: &PublishedTableSpec,
    cols: &[String],
    lite_table_name: &str,
) -> Result<CopyTableResult, CopyTableError> {
    let plan = TableCopyPlan::build(table, cols, lite_table_name, CopyFormat::Binary, None, None);
    copy_table_with_plan(pg, db, &plan).await
}

/// Like [`copy_table_binary`] but over the default text COPY format
/// (`ZERO_INITIAL_SYNC_TEXT_COPY`): `COPY (SELECT ...) TO STDOUT`, rows parsed
/// per the text-format TSV rules and converted with the same typed text
/// conversion the pgoutput replication path applies.
pub async fn copy_table_text(
    pg: &Client,
    db: &StatementRunner,
    table: &PublishedTableSpec,
    cols: &[String],
    lite_table_name: &str,
) -> Result<CopyTableResult, CopyTableError> {
    let plan = TableCopyPlan::build(table, cols, lite_table_name, CopyFormat::Text, None, None);
    copy_table_with_plan(pg, db, &plan).await
}

fn decode_binary_row(
    fields: &[Option<Vec<u8>>],
    decoders: &[Decoder],
) -> Result<Vec<Value>, CopyTableError> {
    if fields.len() != decoders.len() {
        return Err(CopyTableError::FieldCountMismatch {
            got: fields.len(),
            expected: decoders.len(),
        });
    }
    Ok(fields
        .iter()
        .zip(decoders)
        .map(|(field, decoder)| {
            let lite_value = match field {
                None => zero_cache_types::lite::LiteValue::Null,
                Some(buf) => match decoder {
                    Decoder::Binary(d) => d(buf),
                    Decoder::Text => text_cast_decoder(buf),
                },
            };
            to_sql_value(&lite_value)
        })
        .collect())
}

fn decode_text_row(
    fields: &[Option<String>],
    decoders: &[TextDecoder],
) -> Result<Vec<Value>, CopyTableError> {
    if fields.len() != decoders.len() {
        return Err(CopyTableError::FieldCountMismatch {
            got: fields.len(),
            expected: decoders.len(),
        });
    }
    Ok(fields
        .iter()
        .zip(decoders)
        .map(|(field, decoder)| {
            let lite_value = match field {
                None => zero_cache_types::lite::LiteValue::Null,
                Some(text) => decoder(text),
            };
            to_sql_value(&lite_value)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use zero_cache_types::specs::{ColumnSpec, PublishedColumnSpec};

    fn table_spec() -> PublishedTableSpec {
        PublishedTableSpec {
            name: "widgets".into(),
            schema: "public".into(),
            oid: 1,
            schema_oid: None,
            columns: vec![
                (
                    "id".to_string(),
                    PublishedColumnSpec {
                        column: ColumnSpec {
                            pos: 1,
                            data_type: "int4".into(),
                            pg_type_class: None,
                            elem_pg_type_class: None,
                            character_maximum_length: None,
                            not_null: None,
                            dflt: None,
                        },
                        type_oid: zero_cache_types::pg_types::INT4,
                    },
                ),
                (
                    "name".to_string(),
                    PublishedColumnSpec {
                        column: ColumnSpec {
                            pos: 2,
                            data_type: "text".into(),
                            pg_type_class: None,
                            elem_pg_type_class: None,
                            character_maximum_length: None,
                            not_null: None,
                            dflt: None,
                        },
                        type_oid: zero_cache_types::pg_types::TEXT,
                    },
                ),
            ],
            primary_key: Some(vec!["id".into()]),
            replica_identity: None,
            publications: BTreeMap::new(),
        }
    }

    #[test]
    fn text_plan_copies_without_casts_and_binary_plan_declares_the_format() {
        let spec = table_spec();
        let cols = vec!["id".to_string(), "name".to_string()];
        let binary = TableCopyPlan::build(&spec, &cols, "widgets", CopyFormat::Binary, None, None);
        assert!(binary.copy_sql.ends_with("TO STDOUT (FORMAT binary)"));
        let text = TableCopyPlan::build(&spec, &cols, "widgets", CopyFormat::Text, None, None);
        assert!(text.copy_sql.ends_with("TO STDOUT"), "{}", text.copy_sql);
        assert!(!text.copy_sql.contains("::text"));
    }

    #[test]
    fn plan_injects_tablesample_and_limit_for_shadow_sync() {
        let spec = table_spec();
        let cols = vec!["id".to_string()];
        let plan = TableCopyPlan::build(
            &spec,
            &cols,
            "widgets",
            CopyFormat::Binary,
            Some(0.5),
            Some(10),
        );
        assert!(plan.copy_sql.contains("TABLESAMPLE BERNOULLI(50)"));
        assert!(plan.copy_sql.contains("LIMIT 10"));
    }

    #[test]
    fn text_chunks_decode_to_typed_values_across_boundaries() {
        let spec = table_spec();
        let cols = vec!["id".to_string(), "name".to_string()];
        let plan = TableCopyPlan::build(&spec, &cols, "widgets", CopyFormat::Text, None, None);
        let mut state = plan.new_parser();
        let mut rows = plan.decode_chunk(&mut state, b"1\ta\n2\t").unwrap();
        rows.extend(plan.decode_chunk(&mut state, b"\\N\n").unwrap());
        plan.finish(&state).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Value::Integer(1), Value::Text("a".into())],
                vec![Value::Integer(2), Value::Null],
            ]
        );
    }

    async fn connect_local_pg() -> Option<Client> {
        let conn_str = std::env::var("ZERO_TEST_PG_URL")
            .unwrap_or_else(|_| "host=/tmp dbname=postgres".to_string());
        match tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                Some(client)
            }
            Err(_) => None,
        }
    }

    #[tokio::test]
    async fn live_copies_real_rows_from_postgres_into_sqlite() {
        let Some(pg) = connect_local_pg().await else {
            eprintln!("skipping: no local Postgres reachable");
            return;
        };
        let test_table = format!("zero_test_initial_sync_copy_{}", std::process::id());
        pg.batch_execute(&format!("DROP TABLE IF EXISTS {test_table}; CREATE TABLE {test_table} (id int4 PRIMARY KEY, name text)")).await.unwrap();
        pg.batch_execute(&format!(
            "INSERT INTO {test_table} (id, name) VALUES (1, 'a'), (2, 'b'), (3, NULL)"
        ))
        .await
        .unwrap();

        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();

        let mut spec = table_spec();
        spec.name = test_table.clone();
        let cols = vec!["id".to_string(), "name".to_string()];

        let result = copy_table_binary(&pg, &db, &spec, &cols, "widgets")
            .await
            .unwrap();
        assert_eq!(result.rows, 3);

        let rows = db
            .query_uncached("SELECT id, name FROM widgets ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][1].1, Value::Text("a".to_string()));
        assert_eq!(
            rows[2][1].1,
            Value::Null,
            "NULL must round-trip through the binary COPY format"
        );

        pg.batch_execute(&format!("DROP TABLE {test_table}"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn live_copy_of_an_empty_table_yields_zero_rows() {
        let Some(pg) = connect_local_pg().await else {
            eprintln!("skipping: no local Postgres reachable");
            return;
        };
        let test_table = format!("zero_test_initial_sync_copy_empty_{}", std::process::id());
        pg.batch_execute(&format!("DROP TABLE IF EXISTS {test_table}; CREATE TABLE {test_table} (id int4 PRIMARY KEY, name text)")).await.unwrap();

        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();

        let mut spec = table_spec();
        spec.name = test_table.clone();
        let cols = vec!["id".to_string(), "name".to_string()];
        let result = copy_table_binary(&pg, &db, &spec, &cols, "widgets")
            .await
            .unwrap();
        assert_eq!(result.rows, 0);

        pg.batch_execute(&format!("DROP TABLE {test_table}"))
            .await
            .unwrap();
    }

    /// The text COPY path produces the same replica contents as the binary
    /// path — NULLs, escaped text (tabs/newlines), and typed values included.
    #[tokio::test]
    async fn live_text_copy_matches_binary_copy() {
        let Some(pg) = connect_local_pg().await else {
            eprintln!("skipping: no local Postgres reachable");
            return;
        };
        let test_table = format!("zero_test_text_copy_{}", std::process::id());
        pg.batch_execute(&format!(
            "DROP TABLE IF EXISTS {test_table}; \
             CREATE TABLE {test_table} (id int4 PRIMARY KEY, name text); \
             INSERT INTO {test_table} (id, name) VALUES \
               (1, E'tab\\there'), (2, E'nl\\nthere'), (3, NULL), (4, 'back\\slash')"
        ))
        .await
        .unwrap();

        let db_bin = StatementRunner::open_in_memory().unwrap();
        let db_txt = StatementRunner::open_in_memory().unwrap();
        for db in [&db_bin, &db_txt] {
            db.exec("CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)")
                .unwrap();
        }

        let mut spec = table_spec();
        spec.name = test_table.clone();
        let cols = vec!["id".to_string(), "name".to_string()];

        let bin = copy_table_binary(&pg, &db_bin, &spec, &cols, "widgets")
            .await
            .unwrap();
        let txt = copy_table_text(&pg, &db_txt, &spec, &cols, "widgets")
            .await
            .unwrap();
        assert_eq!(bin.rows, 4);
        assert_eq!(txt.rows, 4);

        let read = |db: &StatementRunner| {
            db.query_uncached("SELECT id, name FROM widgets ORDER BY id", &[])
                .unwrap()
        };
        assert_eq!(
            read(&db_bin),
            read(&db_txt),
            "text COPY must land byte-identical rows to binary COPY"
        );

        pg.batch_execute(&format!("DROP TABLE {test_table}"))
            .await
            .unwrap();
    }
}
