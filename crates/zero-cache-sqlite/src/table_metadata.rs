//! Port of `zero-cache/src/services/replicator/schema/table-metadata.ts`.
//!
//! Table-level replication controls: the minimum row version to apply to a
//! table (used to force re-download after a schema change) and upstream backfill
//! metadata, stored in `_zero.tableMetadata`.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::{stringify, JsonValue};
use zero_cache_types::names::{lite_table_name, TableName};

use crate::{DbError, StatementRunner, Value};

/// DDL for the table-metadata table. Port of `CREATE_TABLE_METADATA_TABLE`.
pub const CREATE_TABLE_METADATA_TABLE: &str = r#"
CREATE TABLE "_zero.tableMetadata" (
    "schema"           TEXT NOT NULL,
    "table"            TEXT NOT NULL,
    "minRowVersion"    TEXT NOT NULL DEFAULT "00",
    "upstreamMetadata" TEXT,
    "metadata"         TEXT,  -- deprecated
    PRIMARY KEY ("schema", "table")
);
"#;

/// Tracks per-table replication metadata. Port of `TableMetadataTracker`.
pub struct TableMetadataTracker<'a> {
    db: &'a StatementRunner,
}

impl<'a> TableMetadataTracker<'a> {
    pub fn new(db: &'a StatementRunner) -> Self {
        TableMetadataTracker { db }
    }

    /// Upserts the upstream (backfill) metadata for a table. Port of
    /// `setUpstreamMetadata`.
    pub fn set_upstream_metadata(
        &self,
        schema: &str,
        name: &str,
        metadata: &JsonValue,
    ) -> Result<(), DbError> {
        self.db.run(
            r#"INSERT INTO "_zero.tableMetadata" ("schema", "table", "upstreamMetadata")
               VALUES (?, ?, ?)
               ON CONFLICT ("schema", "table")
               DO UPDATE SET "upstreamMetadata" = excluded."upstreamMetadata""#,
            &[
                Value::Text(schema.to_string()),
                Value::Text(name.to_string()),
                Value::Text(stringify(metadata)),
            ],
        )?;
        Ok(())
    }

    /// Upserts the minimum row version for a table. Port of `setMinRowVersion`.
    pub fn set_min_row_version(
        &self,
        schema: &str,
        name: &str,
        version: &str,
    ) -> Result<(), DbError> {
        self.db.run(
            r#"INSERT INTO "_zero.tableMetadata" ("schema", "table", "minRowVersion")
               VALUES (?, ?, ?)
               ON CONFLICT ("schema", "table")
               DO UPDATE SET "minRowVersion" = excluded."minRowVersion""#,
            &[
                Value::Text(schema.to_string()),
                Value::Text(name.to_string()),
                Value::Text(version.to_string()),
            ],
        )?;
        Ok(())
    }

    /// Returns a map from lite table name to minimum row version. Port of
    /// `getMinRowVersions`.
    pub fn get_min_row_versions(&self) -> Result<BTreeMap<String, String>, DbError> {
        let rows = self.db.query_uncached(
            r#"SELECT "schema", "table" as "name", "minRowVersion" FROM "_zero.tableMetadata""#,
            &[],
        )?;
        Ok(rows
            .into_iter()
            .map(|r| {
                let schema = text(&r[0].1);
                let name = text(&r[1].1);
                let version = text(&r[2].1);
                (
                    lite_table_name(&TableName {
                        schema: &schema,
                        name: &name,
                    }),
                    version,
                )
            })
            .collect())
    }

    /// Renames a table's metadata row. Port of `rename`.
    pub fn rename(
        &self,
        old_schema: &str,
        old_name: &str,
        new_schema: &str,
        new_name: &str,
    ) -> Result<(), DbError> {
        self.db.run(
            r#"UPDATE "_zero.tableMetadata" SET "schema" = ?, "table" = ?
               WHERE "schema" = ? AND "table" = ?"#,
            &[
                Value::Text(new_schema.to_string()),
                Value::Text(new_name.to_string()),
                Value::Text(old_schema.to_string()),
                Value::Text(old_name.to_string()),
            ],
        )?;
        Ok(())
    }

    /// Drops a table's metadata row. Port of `drop`.
    pub fn drop(&self, schema: &str, name: &str) -> Result<(), DbError> {
        self.db.run(
            r#"DELETE FROM "_zero.tableMetadata" WHERE "schema" = ? AND "table" = ?"#,
            &[
                Value::Text(schema.to_string()),
                Value::Text(name.to_string()),
            ],
        )?;
        Ok(())
    }
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_TABLE_METADATA_TABLE).unwrap();
        db
    }

    /// `{rowKey:{columns:[cols...]}}`
    fn row_key_meta(cols: &[&str]) -> JsonValue {
        JsonValue::Object(vec![(
            "rowKey".into(),
            JsonValue::Object(vec![(
                "columns".into(),
                JsonValue::Array(
                    cols.iter()
                        .map(|c| JsonValue::String((*c).into()))
                        .collect(),
                ),
            )]),
        )])
    }

    fn dump(db: &StatementRunner) -> Vec<(String, String, String, Option<String>)> {
        db.query_uncached(
            r#"SELECT "schema", "table", "minRowVersion", "upstreamMetadata"
               FROM "_zero.tableMetadata" ORDER BY "schema", "table""#,
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|r| {
            let upstream = match &r[3].1 {
                Value::Text(s) => Some(s.clone()),
                _ => None,
            };
            (text(&r[0].1), text(&r[1].1), text(&r[2].1), upstream)
        })
        .collect()
    }

    #[test]
    fn set_rename_drop() {
        let db = setup();
        let t = TableMetadataTracker::new(&db);
        assert!(t.get_min_row_versions().unwrap().is_empty());

        t.set_upstream_metadata("public", "foo", &row_key_meta(&["id"]))
            .unwrap();
        t.set_min_row_version("internal", "bar", "123").unwrap();

        assert_eq!(
            dump(&db),
            vec![
                ("internal".into(), "bar".into(), "123".into(), None),
                (
                    "public".into(),
                    "foo".into(),
                    "00".into(),
                    Some(r#"{"rowKey":{"columns":["id"]}}"#.into())
                ),
            ]
        );

        assert_eq!(
            t.get_min_row_versions().unwrap(),
            BTreeMap::from([
                ("foo".to_string(), "00".to_string()),
                ("internal.bar".to_string(), "123".to_string()),
            ])
        );

        // Updates preserve other columns.
        t.set_min_row_version("public", "foo", "2b8a").unwrap();
        t.set_upstream_metadata("internal", "bar", &row_key_meta(&["a", "b"]))
            .unwrap();
        assert_eq!(
            dump(&db),
            vec![
                (
                    "internal".into(),
                    "bar".into(),
                    "123".into(),
                    Some(r#"{"rowKey":{"columns":["a","b"]}}"#.into())
                ),
                (
                    "public".into(),
                    "foo".into(),
                    "2b8a".into(),
                    Some(r#"{"rowKey":{"columns":["id"]}}"#.into())
                ),
            ]
        );

        // Rename and drop.
        t.rename("internal", "bar", "public", "baz").unwrap();
        t.drop("public", "foo").unwrap();
        assert_eq!(
            dump(&db),
            vec![(
                "public".into(),
                "baz".into(),
                "123".into(),
                Some(r#"{"rowKey":{"columns":["a","b"]}}"#.into())
            )]
        );
    }
}
