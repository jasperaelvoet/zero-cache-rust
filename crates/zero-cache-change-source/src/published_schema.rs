//! Port of the pure pieces of `change-source/pg/schema/published.ts` — the
//! upstream-schema introspection that `initialSync`'s `getPublicationInfo`
//! runs to discover which tables/columns/indexes a set of publications
//! exposes.
//!
//! Ported here:
//! * [`quote_literal`] / [`literal_list`] — `pg-format`'s `literal()` string
//!   escaping, used to splice the publication-name list into the query safely
//!   (an injection-safety concern, so faithful escaping matters).
//! * [`published_schema_query`] — the verbatim introspection SQL (the
//!   `publishedSchemaQuery` builder), with the publication list spliced in.
//! * [`check_published_columns_consistency`] — the pure validation that a
//!   table appearing in multiple publications exposes the same column set
//!   (the `publishedColumns` loop in `getPublicationInfo`).
//!
//! Also ported (this note previously said these were NOT — now stale):
//! [`get_publication_info`] runs the query over a live connection and
//! deserializes its `publishedSchema` JSON into
//! `PublishedTableSpec`/`PublishedIndexSpec` via
//! `zero_cache_types::published_schema_json`. It is consumed by
//! `zero_cache_sqlite::initial_sync` (which now introspects its own table specs
//! at the slot snapshot rather than taking them as input) and covered by the
//! live `live_get_publication_info_parses_specs` test. It also runs the
//! multi-publication column-consistency check on the live path (M9). The
//! `replicaIdentityColumns` denormalization (M10) is NOT reproduced — see the
//! doc on [`get_publication_info`] for why (the change-apply key path derives
//! the key from the pgoutput relation, not from a spec field).

use std::collections::{BTreeMap, BTreeSet};

/// Port of `pg-format`'s `literal(str)` for a single string value: wraps in
/// single quotes, doubling embedded single quotes; if the value contains a
/// backslash, doubles backslashes and prefixes the whole literal with ` E`
/// (Postgres escape-string syntax), matching node-pg-format exactly.
pub fn quote_literal(value: &str) -> String {
    let mut has_backslash = false;
    let mut quoted = String::from("'");
    for ch in value.chars() {
        match ch {
            '\'' => quoted.push_str("''"),
            '\\' => {
                quoted.push_str("\\\\");
                has_backslash = true;
            }
            _ => quoted.push(ch),
        }
    }
    quoted.push('\'');
    if has_backslash {
        format!(" E{quoted}")
    } else {
        quoted
    }
}

/// Port of `literal(array)`: each element quoted via [`quote_literal`], joined
/// with `, ` — the form `${literal(publications)}` expands to inside an
/// `IN (...)` clause.
pub fn literal_list<S: AsRef<str>>(values: &[S]) -> String {
    values
        .iter()
        .map(|v| quote_literal(v.as_ref()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Port of `publishedSchemaQuery(publications)`: the introspection query that
/// returns a single `publishedSchema` JSON object `{tables, indexes}`. Verbatim
/// from upstream, with the publication list spliced in via [`literal_list`] at
/// the two `${literal(publications)}` sites.
pub fn published_schema_query<S: AsRef<str>>(publications: &[S]) -> String {
    let pubs = literal_list(publications);
    format!(
        r#"
WITH published_columns AS (SELECT
  pc.oid::int8 AS "oid",
  nspname AS "schema",
  pc.relnamespace::int8 AS "schemaOID" ,
  pc.relname AS "name",
  pc.relreplident AS "replicaIdentity",
  attnum AS "pos",
  attname AS "col",
  pt.typname AS "type",
  atttypid::int8 AS "typeOID",
  pt.typtype,
  elem_pt.typtype AS "elemTyptype",
  NULLIF(atttypmod, -1) AS "maxLen",
  attndims "arrayDims",
  attnotnull AS "notNull",
  pg_get_expr(pd.adbin, pd.adrelid) as "dflt",
  NULLIF(ARRAY_POSITION(conkey, attnum), -1) AS "keyPos",
  pb.rowfilter as "rowFilter",
  pb.pubname as "publication"
FROM pg_attribute
JOIN pg_class pc ON pc.oid = attrelid
JOIN pg_namespace pns ON pns.oid = relnamespace
JOIN pg_type pt ON atttypid = pt.oid
LEFT JOIN pg_type elem_pt ON elem_pt.oid = pt.typelem
JOIN pg_publication_tables as pb ON
  pb.schemaname = nspname AND
  pb.tablename = pc.relname AND
  attname = ANY(pb.attnames)
LEFT JOIN pg_constraint pk ON pk.contype = 'p' AND pk.connamespace = relnamespace AND pk.conrelid = attrelid
LEFT JOIN pg_attrdef pd ON pd.adrelid = attrelid AND pd.adnum = attnum
WHERE pb.pubname IN ({pubs}) AND
      (current_setting('server_version_num')::int >= 160000 OR attgenerated = '')
ORDER BY nspname, pc.relname),

tables AS (SELECT json_build_object(
  'oid', "oid",
  'schema', "schema",
  'schemaOID', "schemaOID",
  'name', "name",
  'replicaIdentity', "replicaIdentity",
  'columns', json_object_agg(
    DISTINCT
    col,
    jsonb_build_object(
      'pos', "pos",
      'dataType', CASE WHEN "arrayDims" = 0
                       THEN "type"
                       ELSE substring("type" from 2) || repeat('[]', "arrayDims") END,
      'pgTypeClass', "typtype",
      'elemPgTypeClass', "elemTyptype",
      'typeOID', "typeOID",
      'characterMaximumLength', CASE WHEN "typeOID" = 1043 OR "typeOID" = 1042
                                     THEN "maxLen" - 4
                                     ELSE "maxLen" END,
      'notNull', "notNull",
      'dflt', "dflt"
    )
  ),
  'primaryKey', ARRAY( SELECT json_object_keys(
    json_strip_nulls(
      json_object_agg(
        DISTINCT "col", "keyPos" ORDER BY "keyPos"
      )
    )
  )),
  'publications', json_object_agg(
    DISTINCT
    "publication",
    jsonb_build_object('rowFilter', "rowFilter")
  )
) AS "table" FROM published_columns
  GROUP BY "schema", "schemaOID", "name", "oid", "replicaIdentity"),

  indexed_columns AS (SELECT
      pg_indexes.schemaname as "schema",
      pg_indexes.tablename as "tableName",
      pg_indexes.indexname as "name",
      index_column.name as "col",
      CASE WHEN pg_index.indoption[index_column.pos-1] & 1 = 1 THEN 'DESC' ELSE 'ASC' END as "dir",
      pg_index.indisunique as "unique",
      pg_index.indisprimary as "isPrimaryKey",
      pg_index.indisreplident as "isReplicaIdentity",
      pg_index.indimmediate as "isImmediate"
    FROM pg_indexes
    JOIN pg_namespace ON pg_indexes.schemaname = pg_namespace.nspname
    JOIN pg_class pc ON
      pc.relname = pg_indexes.indexname
      AND pc.relnamespace = pg_namespace.oid
    JOIN pg_publication_tables as pb ON
      pb.schemaname = pg_indexes.schemaname AND
      pb.tablename = pg_indexes.tablename
    JOIN pg_index ON pg_index.indexrelid = pc.oid
    JOIN LATERAL (
      SELECT array_agg(attname) as attnames, array_agg(attgenerated != '') as generated FROM pg_attribute
        WHERE attrelid = pg_index.indrelid
          AND attnum = ANY( (pg_index.indkey::smallint[] )[:pg_index.indnkeyatts - 1] )
    ) as indexed ON true
    JOIN LATERAL (
      SELECT pg_attribute.attname as name, col.index_pos as pos
        FROM UNNEST( (pg_index.indkey::smallint[])[:pg_index.indnkeyatts - 1] )
          WITH ORDINALITY as col(table_pos, index_pos)
        JOIN pg_attribute ON attrelid = pg_index.indrelid AND attnum = col.table_pos
    ) AS index_column ON true
    LEFT JOIN pg_constraint ON pg_constraint.conindid = pc.oid
    WHERE pb.pubname IN ({pubs})
      AND pg_index.indexprs IS NULL
      AND pg_index.indpred IS NULL
      AND (pg_constraint.contype IS NULL OR pg_constraint.contype IN ('p', 'u'))
      AND indexed.attnames <@ pb.attnames
      AND (current_setting('server_version_num')::int >= 160000 OR false = ALL(indexed.generated))
    ORDER BY
      pg_indexes.schemaname,
      pg_indexes.tablename,
      pg_indexes.indexname,
      index_column.pos ASC),

    indexes AS (SELECT json_build_object(
      'schema', "schema",
      'tableName', "tableName",
      'name', "name",
      'unique', "unique",
      'isPrimaryKey', "isPrimaryKey",
      'isReplicaIdentity', "isReplicaIdentity",
      'isImmediate', "isImmediate",
      'columns', json_object_agg("col", "dir")
    ) AS index FROM indexed_columns
      GROUP BY "schema", "tableName", "name", "unique",
         "isPrimaryKey", "isReplicaIdentity", "isImmediate")

    SELECT json_build_object(
      'tables', COALESCE((SELECT json_agg("table") FROM tables), '[]'::json),
      'indexes', COALESCE((SELECT json_agg("index") FROM indexes), '[]'::json)
    ) as "publishedSchema"
  "#
    )
}

/// Errors from [`get_publication_info`].
#[derive(Debug, thiserror::Error)]
pub enum PublicationInfoError {
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error("introspection query returned no publishedSchema row")]
    NoResult,
    #[error("parsing publishedSchema JSON: {0}")]
    Json(String),
    #[error(transparent)]
    Spec(#[from] zero_cache_types::published_schema_json::ParseSpecError),
    /// M9: a table published in multiple publications exposes differing column
    /// sets — upstream `getPublicationInfo` throws the same error.
    #[error(transparent)]
    ColumnConsistency(#[from] ColumnMismatch),
}

/// Port of `getPublicationInfo`'s FIRST query: per (schema, table), the set of
/// columns each publication exposes. Upstream runs this alongside the
/// `publishedSchemaQuery` and uses it purely for the multi-publication
/// column-consistency check. The `attnames` array comes back as a JSON array
/// per publication via `json_object_agg(pubname, attnames)`.
pub fn published_columns_query<S: AsRef<str>>(publications: &[S]) -> String {
    let pubs = literal_list(publications);
    format!(
        r#"
    SELECT
      schemaname AS "schema",
      tablename AS "table",
      json_object_agg(pubname, attnames) AS "publications"
      FROM pg_publication_tables pb
      WHERE pb.pubname IN ({pubs})
      GROUP BY schemaname, tablename
"#
    )
}

/// Runs [`published_columns_query`] over `client` and parses each row into a
/// [`PublishedColumns`] (publication name -> exposed column set), the input to
/// [`check_published_columns_consistency`].
async fn fetch_published_columns<S: AsRef<str>>(
    client: &tokio_postgres::Client,
    publications: &[S],
) -> Result<Vec<PublishedColumns>, PublicationInfoError> {
    let q = published_columns_query(publications);
    let msgs = client.simple_query(&q).await?;
    let mut out = Vec::new();
    for m in &msgs {
        let tokio_postgres::SimpleQueryMessage::Row(row) = m else {
            continue;
        };
        let (Some(table), Some(pubs_json)) = (row.get("table"), row.get("publications")) else {
            continue;
        };
        let json = zero_cache_shared::bigint_json::parse(pubs_json)
            .map_err(|e| PublicationInfoError::Json(e.to_string()))?;
        let mut publications = BTreeMap::new();
        if let zero_cache_shared::bigint_json::JsonValue::Object(entries) = json {
            for (pubname, cols) in entries {
                let mut set = BTreeSet::new();
                if let zero_cache_shared::bigint_json::JsonValue::Array(items) = cols {
                    for item in items {
                        if let zero_cache_shared::bigint_json::JsonValue::String(s) = item {
                            set.insert(s);
                        }
                    }
                }
                publications.insert(pubname, set);
            }
        }
        out.push(PublishedColumns {
            table: table.to_string(),
            publications,
        });
    }
    Ok(out)
}

/// The published tables and indexes for a set of publications — the live,
/// self-contained counterpart to upstream's `getPublicationInfo` (minus the
/// `publications` metadata query).
///
/// M9: before parsing the schema, this runs [`published_columns_query`] and
/// [`check_published_columns_consistency`] on the live path (previously
/// test-only), so a table exported with different column sets across
/// publications fails here exactly as upstream's `getPublicationInfo` throws
/// "exported with different columns" — rather than the port silently picking an
/// arbitrary set.
///
/// M10: `replicaIdentityColumns` denormalization (upstream `published.ts:185`)
/// is intentionally NOT reproduced here. The change-apply key path does not
/// consume a per-table `replicaIdentityColumns` list from the spec: it derives
/// the row key from the pgoutput `Relation` message's key flags
/// (`pg_to_change::translate` → `RowKey`), which cover REPLICA IDENTITY
/// `d`(default)/`i`(index) — the flagged columns are exactly upstream's
/// denormalized set for those cases. `row_apply::get_key` additionally supports
/// the `Full` case by falling back to the replica table's own primary key.
/// REPLICA IDENTITY FULL is now handled (M10 fixed): `pg_to_change::relation_of`
/// maps a no-flagged-key `Full` relation to `RowKeyKind::Full`, and
/// `change_dispatcher` runs `row_apply::get_key` (with the spec's primary key)
/// on both UPDATE and DELETE so the row is keyed by its PK rather than every
/// column. Duplicating the denormalization into this introspection spec is still
/// intentionally avoided — the consumer is the translate/dispatch path. Residual:
/// FULL-identity *backfill* still requires non-empty `RowKey.columns` and would
/// need the PK plumbed in the same way.
pub async fn get_publication_info<S: AsRef<str>>(
    client: &tokio_postgres::Client,
    publications: &[S],
) -> Result<
    (
        Vec<zero_cache_types::specs::PublishedTableSpec>,
        Vec<zero_cache_types::specs::PublishedIndexSpec>,
    ),
    PublicationInfoError,
> {
    // M9: reject a table exported with differing columns across publications.
    let published_columns = fetch_published_columns(client, publications).await?;
    check_published_columns_consistency(&published_columns)?;

    let q = published_schema_query(publications);
    // `simple_query` returns the `json` column as text, which we feed to the
    // port's bigint-aware JSON parser (`oid`/`typeOID` are int8 upstream).
    let msgs = client.simple_query(&q).await?;
    let json_text = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => {
                r.get("publishedSchema").map(str::to_owned)
            }
            _ => None,
        })
        .ok_or(PublicationInfoError::NoResult)?;
    let json = zero_cache_shared::bigint_json::parse(&json_text)
        .map_err(|e| PublicationInfoError::Json(e.to_string()))?;
    let specs = zero_cache_types::published_schema_json::published_schema_from_json(&json)?;
    Ok(specs)
}

/// One row of `getPublicationInfo`'s first query: a table and, per publication
/// it belongs to, the set of columns that publication exposes.
#[derive(Debug, Clone)]
pub struct PublishedColumns {
    pub table: String,
    /// publication name -> exposed column set.
    pub publications: BTreeMap<String, BTreeSet<String>>,
}

/// Port of `getPublicationInfo`'s consistency check: a table published in
/// multiple publications must expose the same column set in each. Returns the
/// name of the first offending table (and both column sets) on mismatch,
/// mirroring upstream's thrown error.
pub fn check_published_columns_consistency(
    rows: &[PublishedColumns],
) -> Result<(), ColumnMismatch> {
    for row in rows {
        let mut expected: Option<&BTreeSet<String>> = None;
        for cols in row.publications.values() {
            match expected {
                None => expected = Some(cols),
                Some(exp) if exp != cols => {
                    return Err(ColumnMismatch {
                        table: row.table.clone(),
                        expected: exp.iter().cloned().collect(),
                        found: cols.iter().cloned().collect(),
                    });
                }
                Some(_) => {}
            }
        }
    }
    Ok(())
}

/// A table exposed with differing column sets across publications.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMismatch {
    pub table: String,
    pub expected: Vec<String>,
    pub found: Vec<String>,
}

impl std::fmt::Display for ColumnMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Table {} is exported with different columns: [{}] vs [{}]",
            self.table,
            self.expected.join(","),
            self.found.join(",")
        )
    }
}

impl std::error::Error for ColumnMismatch {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_literal_wraps_and_doubles_single_quotes() {
        assert_eq!(quote_literal("zero_data"), "'zero_data'");
        assert_eq!(quote_literal("O'Brien"), "'O''Brien'");
    }

    #[test]
    fn quote_literal_escapes_backslashes_with_e_prefix() {
        // A backslash triggers Postgres escape-string syntax ` E'...'` with the
        // backslash doubled, matching node-pg-format.
        assert_eq!(quote_literal(r"a\b"), r" E'a\\b'");
    }

    #[test]
    fn literal_list_joins_quoted_values() {
        let pubs = ["zero_data", "zero_metadata"];
        assert_eq!(literal_list(&pubs), "'zero_data', 'zero_metadata'");
    }

    #[test]
    fn published_schema_query_splices_publication_list_at_both_sites() {
        let q = published_schema_query(&["zero_data"]);
        // The publication literal appears in both the columns CTE and the
        // indexes CTE `WHERE pb.pubname IN (...)` clauses.
        assert_eq!(q.matches("pb.pubname IN ('zero_data')").count(), 2);
        assert!(q.contains("WITH published_columns AS"));
        assert!(q.contains("indexed_columns AS"));
        assert!(q.contains(r#"as "publishedSchema""#));
    }

    fn cols(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn consistency_passes_when_all_publications_agree() {
        let rows = vec![PublishedColumns {
            table: "issue".into(),
            publications: BTreeMap::from([
                ("pub_a".into(), cols(&["id", "title"])),
                ("pub_b".into(), cols(&["title", "id"])), // same set, any order
            ]),
        }];
        assert!(check_published_columns_consistency(&rows).is_ok());
    }

    #[test]
    fn consistency_fails_on_differing_column_sets() {
        let rows = vec![PublishedColumns {
            table: "issue".into(),
            publications: BTreeMap::from([
                ("pub_a".into(), cols(&["id", "title"])),
                ("pub_b".into(), cols(&["id"])),
            ]),
        }];
        let err = check_published_columns_consistency(&rows).unwrap_err();
        assert_eq!(err.table, "issue");
        // BTreeMap iterates publications in name order, so pub_a (id,title) is
        // the expected baseline and pub_b (id) is the mismatch.
        assert_eq!(err.expected, vec!["id".to_string(), "title".to_string()]);
        assert_eq!(err.found, vec!["id".to_string()]);
    }

    /// Live: run the verbatim introspection SQL against real Postgres and
    /// confirm it is well-formed and returns the published table/index for a
    /// real publication. Guards against a typo in the large ported SQL string.
    #[tokio::test]
    async fn live_query_returns_published_table_and_index() {
        let conn_str = std::env::var("ZERO_TEST_PG")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into());
        let Ok(client) = crate::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        client
            .batch_execute(
                "DROP TABLE IF EXISTS ps_test CASCADE; \
                 CREATE TABLE ps_test(id int primary key, name text); \
                 DROP PUBLICATION IF EXISTS ps_test_pub; \
                 CREATE PUBLICATION ps_test_pub FOR TABLE ps_test;",
            )
            .await
            .unwrap();

        // `simple_query` returns column values as text, so the `json`
        // `publishedSchema` comes back as its JSON string — no serde_json
        // dependency needed just to prove the SQL is well-formed.
        let q = published_schema_query(&["ps_test_pub"]);
        let msgs = client
            .simple_query(&q)
            .await
            .expect("introspection query is valid SQL");
        let json = msgs
            .iter()
            .find_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => {
                    r.get("publishedSchema").map(str::to_owned)
                }
                _ => None,
            })
            .expect("a publishedSchema row");

        assert!(
            json.contains("ps_test"),
            "published schema should mention ps_test: {json}"
        );
        assert!(
            json.contains("\"isPrimaryKey\""),
            "published indexes should include a primary key entry: {json}"
        );

        client
            .batch_execute("DROP PUBLICATION ps_test_pub; DROP TABLE ps_test;")
            .await
            .unwrap();
    }

    /// Live: end-to-end `get_publication_info` — run the introspection query
    /// against real Postgres AND deserialize its JSON into real specs.
    #[tokio::test]
    async fn live_get_publication_info_parses_specs() {
        let conn_str = std::env::var("ZERO_TEST_PG")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into());
        let Ok(client) = crate::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute(
                "DROP TABLE IF EXISTS gpi_test CASCADE; \
                 CREATE TABLE gpi_test(id int primary key, name text not null); \
                 DROP PUBLICATION IF EXISTS gpi_test_pub; \
                 CREATE PUBLICATION gpi_test_pub FOR TABLE gpi_test;",
            )
            .await
            .unwrap();

        let (tables, indexes) = get_publication_info(&client, &["gpi_test_pub"])
            .await
            .unwrap();

        let t = tables
            .iter()
            .find(|t| t.name == "gpi_test")
            .expect("gpi_test in specs");
        assert_eq!(t.schema, "public");
        let col_names: Vec<&str> = t.columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(col_names, ["id", "name"], "columns sorted by pos");
        assert_eq!(t.primary_key, Some(vec!["id".to_string()]));
        assert!(t.publications.contains_key("gpi_test_pub"));
        // The `name text not null` column round-trips its not-null flag.
        assert_eq!(t.columns[1].1.column.not_null, Some(true));

        assert!(
            indexes
                .iter()
                .any(|i| i.table_name == "gpi_test" && i.is_primary_key == Some(true)),
            "primary-key index present"
        );

        client
            .batch_execute("DROP PUBLICATION gpi_test_pub; DROP TABLE gpi_test;")
            .await
            .unwrap();
    }

    #[test]
    fn published_columns_query_splices_publication_list() {
        let q = published_columns_query(&["zero_data", "zero_metadata"]);
        assert!(q.contains("json_object_agg(pubname, attnames)"));
        assert!(q.contains("FROM pg_publication_tables pb"));
        assert!(q.contains("pb.pubname IN ('zero_data', 'zero_metadata')"));
        assert!(q.contains(r#"GROUP BY schemaname, tablename"#));
    }

    #[test]
    fn consistency_single_publication_always_ok() {
        let rows = vec![PublishedColumns {
            table: "issue".into(),
            publications: BTreeMap::from([("only".into(), cols(&["a", "b", "c"]))]),
        }];
        assert!(check_published_columns_consistency(&rows).is_ok());
    }
}
