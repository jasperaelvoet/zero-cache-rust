//! A first real live slice of `initial-sync.ts`'s COPY-streaming half —
//! `copyBinary`/`copyTable`'s actual work: run `COPY ... TO STDOUT (FORMAT
//! binary)` against upstream Postgres via `tokio_postgres::Client::copy_out`
//! (a real, off-the-shelf `tokio-postgres` API — logical replication needed
//! `replication_conn.rs`'s hand-rolled frontend/backend protocol handling
//! because `tokio-postgres` has no `START_REPLICATION` support, but plain
//! `COPY ... TO STDOUT` is a first-class `tokio-postgres` feature, so no
//! protocol-level reimplementation is needed here), decode each row via the
//! already-ported `pg_copy_binary::BinaryCopyParser` +
//! `make_binary_decoder`/`text_cast_decoder`, and insert into the SQLite
//! replica via [`crate::StatementRunner`].
//!
//! Composes three pieces from prior rounds that had no live consumer yet:
//! `initial_sync_sql::make_download_statements`/`make_binary_select_exprs`
//! (the SELECT statement), `pg_copy_binary`'s streaming parser/decoders
//! (the wire format), and `StatementRunner`/`row_apply::to_sql_value`'s
//! `LiteValue`-to-SQLite bind-value mapping (the write side).
//!
//! Scope: this is `copyTable` for ONE already-schema-created SQLite table —
//! NOT ported: `initialSync`/`shadowInitialSync` themselves (upstream
//! connection setup, replica schema/index creation via
//! `createLiteTables`/`createLiteIndices`, replication-slot setup via
//! `createReplicaAndSlot`, `TableCopyWorkers` parallelism, OTel metrics,
//! shadow-sync verification). This module is the innermost live-I/O loop
//! those would drive per table, proven end-to-end against a real local
//! Postgres instance rather than left as an untested design.

use futures_util::StreamExt;
use tokio_postgres::Client;

use zero_cache_types::initial_sync_sql::make_download_statements;
use zero_cache_types::pg_copy_binary::{
    has_binary_decoder, make_binary_decoder, text_cast_decoder, BinaryColumnSpec, BinaryCopyParser,
};
use zero_cache_types::specs::PublishedTableSpec;

use crate::row_apply::to_sql_value;
use crate::{DbError, StatementRunner, Value};

/// Errors from [`copy_table_binary`].
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
}

/// Result of copying one table. Port of the row-count/byte-count half of
/// `CopyResult` (the metrics/timing fields are not tracked here — no OTel
/// dependency in this port, see `observability`'s scope notes elsewhere).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyTableResult {
    pub rows: usize,
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
    let select_exprs = zero_cache_types::initial_sync_sql::make_binary_select_exprs(table, cols);
    let stmts = make_download_statements(table, cols, None, None, Some(&select_exprs));

    let decoders: Vec<Decoder> = cols
        .iter()
        .map(|col| {
            let spec = table
                .columns
                .iter()
                .find(|(name, _)| name == col)
                .map(|(_, spec)| BinaryColumnSpec {
                    type_oid: spec.type_oid,
                    data_type: spec.column.data_type.clone(),
                    pg_type_class: spec.column.pg_type_class,
                    elem_pg_type_class: spec.column.elem_pg_type_class,
                });
            match &spec {
                Some(spec) if has_binary_decoder(spec) => make_binary_decoder(spec)
                    .map(Decoder::Binary)
                    .unwrap_or(Decoder::Text),
                _ => Decoder::Text,
            }
        })
        .collect();

    let copy_sql = format!("COPY ({}) TO STDOUT (FORMAT binary)", stmts.select.trim());
    let stream = pg.copy_out(&copy_sql).await?;
    // `CopyOutStream` is `!Unpin`; pin it on the stack. Uses `futures_util`
    // (already a dependency) rather than `tokio::pin!` so this compiles in the
    // normal build without pulling `tokio` in as a non-dev dependency.
    futures_util::pin_mut!(stream);

    let mut parser = BinaryCopyParser::new();
    let mut pending_fields: Vec<Option<Vec<u8>>> = Vec::new();
    let mut rows = 0usize;

    let placeholders = std::iter::repeat_n("?", cols.len())
        .collect::<Vec<_>>()
        .join(",");
    let insert_sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        zero_cache_types::sql::id(lite_table_name),
        zero_cache_types::sql::id_list(cols.iter().map(String::as_str)),
        placeholders
    );

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let fields = parser.parse(&chunk)?;
        pending_fields.extend(fields);

        while pending_fields.len() >= cols.len() {
            let row_fields: Vec<Option<Vec<u8>>> = pending_fields.drain(0..cols.len()).collect();
            let values = decode_row(&row_fields, &decoders, cols)?;
            db.run(&insert_sql, &values)?;
            rows += 1;
        }
    }

    if !pending_fields.is_empty() {
        return Err(CopyTableError::FieldCountMismatch {
            got: pending_fields.len(),
            expected: cols.len(),
        });
    }

    Ok(CopyTableResult { rows })
}

enum Decoder {
    Binary(zero_cache_types::pg_copy_binary::BinaryDecoder),
    Text,
}

fn decode_row(
    fields: &[Option<Vec<u8>>],
    decoders: &[Decoder],
    cols: &[String],
) -> Result<Vec<Value>, CopyTableError> {
    if fields.len() != decoders.len() {
        return Err(CopyTableError::FieldCountMismatch {
            got: fields.len(),
            expected: decoders.len(),
        });
    }
    let _ = cols;
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
}
