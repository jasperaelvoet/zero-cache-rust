//! Port of the SQLite-backed store in
//! `zero-cache/src/services/replicator/schema/column-metadata.ts`.
//!
//! The pure conversion helpers ([`ColumnMetadata`] etc.) live in
//! `zero-cache-types::column_metadata`; this is the CRUD store over the
//! `_zero.column_metadata` table.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::{stringify, JsonValue};
use zero_cache_types::column_metadata::{
    lite_type_string_to_metadata, pg_column_spec_to_metadata, ColumnMetadata,
};
use zero_cache_types::specs::{ColumnSpec, LiteTableSpec};

use crate::{DbError, StatementRunner, Value};

/// DDL for the column-metadata table. Port of `CREATE_COLUMN_METADATA_TABLE`.
pub const CREATE_COLUMN_METADATA_TABLE: &str = r#"
CREATE TABLE "_zero.column_metadata" (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    upstream_type TEXT NOT NULL,
    is_not_null INTEGER NOT NULL,
    is_enum INTEGER NOT NULL,
    is_array INTEGER NOT NULL,
    character_max_length INTEGER,
    backfill TEXT,
    PRIMARY KEY (table_name, column_name)
);
"#;

/// CRUD store over `_zero.column_metadata`. Port of `ColumnMetadataStore` (the
/// per-connection instance cache is a JS optimization and is omitted).
pub struct ColumnMetadataStore<'a> {
    db: &'a StatementRunner,
}

fn b(v: bool) -> Value {
    Value::Integer(if v { 1 } else { 0 })
}
fn cml(v: Option<i64>) -> Value {
    v.map(Value::Integer).unwrap_or(Value::Null)
}

impl<'a> ColumnMetadataStore<'a> {
    pub fn new(db: &'a StatementRunner) -> Self {
        ColumnMetadataStore { db }
    }

    /// Whether the `_zero.column_metadata` table exists. Port of `hasTable`.
    pub fn has_table(&self) -> Result<bool, DbError> {
        let rows = self.db.query_uncached(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='_zero.column_metadata'",
            &[],
        )?;
        Ok(!rows.is_empty())
    }

    /// Inserts metadata for a column (from a Postgres spec). Port of `insert`.
    /// Errors on a primary-key conflict.
    pub fn insert(
        &self,
        table_name: &str,
        column_name: &str,
        spec: &ColumnSpec,
        backfill: Option<&JsonValue>,
    ) -> Result<(), DbError> {
        let m = pg_column_spec_to_metadata(spec);
        self.db.run(
            r#"INSERT INTO "_zero.column_metadata"
               (table_name, column_name, upstream_type, is_not_null, is_enum, is_array, character_max_length, backfill)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
            &[
                Value::Text(table_name.to_string()),
                Value::Text(column_name.to_string()),
                Value::Text(m.upstream_type),
                b(m.is_not_null),
                b(m.is_enum),
                b(m.is_array),
                cml(m.character_max_length),
                backfill.map(|v| Value::Text(stringify(v))).unwrap_or(Value::Null),
            ],
        )?;
        Ok(())
    }

    /// Updates a column (possibly renaming it). Port of `update`.
    pub fn update(
        &self,
        table_name: &str,
        old_column_name: &str,
        new_column_name: &str,
        spec: &ColumnSpec,
    ) -> Result<(), DbError> {
        let m = pg_column_spec_to_metadata(spec);
        self.db.run(
            r#"UPDATE "_zero.column_metadata"
               SET column_name = ?, upstream_type = ?, is_not_null = ?, is_enum = ?, is_array = ?, character_max_length = ?
               WHERE table_name = ? AND column_name = ?"#,
            &[
                Value::Text(new_column_name.to_string()),
                Value::Text(m.upstream_type),
                b(m.is_not_null),
                b(m.is_enum),
                b(m.is_array),
                cml(m.character_max_length),
                Value::Text(table_name.to_string()),
                Value::Text(old_column_name.to_string()),
            ],
        )?;
        Ok(())
    }

    /// Clears the `backfill` marker for a column. Port of `clearBackfilling`.
    pub fn clear_backfilling(&self, table_name: &str, column_name: &str) -> Result<(), DbError> {
        self.db.run(
            r#"UPDATE "_zero.column_metadata" SET backfill = NULL WHERE table_name = ? AND column_name = ?"#,
            &[Value::Text(table_name.to_string()), Value::Text(column_name.to_string())],
        )?;
        Ok(())
    }

    pub fn delete_column(&self, table_name: &str, column_name: &str) -> Result<(), DbError> {
        self.db.run(
            r#"DELETE FROM "_zero.column_metadata" WHERE table_name = ? AND column_name = ?"#,
            &[
                Value::Text(table_name.to_string()),
                Value::Text(column_name.to_string()),
            ],
        )?;
        Ok(())
    }

    pub fn delete_table(&self, table_name: &str) -> Result<(), DbError> {
        self.db.run(
            r#"DELETE FROM "_zero.column_metadata" WHERE table_name = ?"#,
            &[Value::Text(table_name.to_string())],
        )?;
        Ok(())
    }

    pub fn rename_table(&self, old_table_name: &str, new_table_name: &str) -> Result<(), DbError> {
        self.db.run(
            r#"UPDATE "_zero.column_metadata" SET table_name = ? WHERE table_name = ?"#,
            &[
                Value::Text(new_table_name.to_string()),
                Value::Text(old_table_name.to_string()),
            ],
        )?;
        Ok(())
    }

    /// Reads a column's metadata. Port of `getColumn`.
    pub fn get_column(
        &self,
        table_name: &str,
        column_name: &str,
    ) -> Result<Option<ColumnMetadata>, DbError> {
        let row = self.db.get(
            r#"SELECT upstream_type, is_not_null, is_enum, is_array, character_max_length, backfill
               FROM "_zero.column_metadata" WHERE table_name = ? AND column_name = ?"#,
            &[
                Value::Text(table_name.to_string()),
                Value::Text(column_name.to_string()),
            ],
        )?;
        Ok(row.map(|r| row_to_metadata(&r, 0)))
    }

    /// Reads all columns of a table. Port of `getTable`.
    pub fn get_table(&self, table_name: &str) -> Result<BTreeMap<String, ColumnMetadata>, DbError> {
        let rows = self.db.query_uncached(
            r#"SELECT column_name, upstream_type, is_not_null, is_enum, is_array, character_max_length, backfill
               FROM "_zero.column_metadata" WHERE table_name = ?"#,
            &[Value::Text(table_name.to_string())],
        )?;
        Ok(rows
            .into_iter()
            .map(|r| (text(&r[0].1), row_to_metadata(&r, 1)))
            .collect())
    }
}

/// Populates the metadata table from tables using pipe notation (migration
/// helper). Port of `populateFromExistingTables`.
pub fn populate_from_existing_tables(
    db: &StatementRunner,
    tables: &[LiteTableSpec],
) -> Result<(), DbError> {
    for table in tables {
        for (column_name, spec) in &table.columns {
            let m = lite_type_string_to_metadata(&spec.data_type, spec.character_maximum_length);
            db.run(
                r#"INSERT INTO "_zero.column_metadata"
                   (table_name, column_name, upstream_type, is_not_null, is_enum, is_array, character_max_length)
                   VALUES (?, ?, ?, ?, ?, ?, ?)"#,
                &[
                    Value::Text(table.name.clone()),
                    Value::Text(column_name.clone()),
                    Value::Text(m.upstream_type),
                    b(m.is_not_null),
                    b(m.is_enum),
                    b(m.is_array),
                    cml(m.character_max_length),
                ],
            )?;
        }
    }
    Ok(())
}

/// Builds a [`ColumnMetadata`] from a row whose type columns start at `base`
/// (`upstream_type, is_not_null, is_enum, is_array, character_max_length,
/// backfill`).
fn row_to_metadata(r: &crate::Row, base: usize) -> ColumnMetadata {
    let int = |i: usize| matches!(r[i].1, Value::Integer(n) if n != 0);
    let cml = match r[base + 4].1 {
        Value::Integer(n) => Some(n),
        _ => None,
    };
    let is_backfilling = !matches!(r[base + 5].1, Value::Null);
    ColumnMetadata {
        upstream_type: text(&r[base].1),
        is_not_null: int(base + 1),
        is_enum: int(base + 2),
        is_array: int(base + 3),
        character_max_length: cml,
        is_backfilling,
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
    use zero_cache_types::specs::ColumnSpec;

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_COLUMN_METADATA_TABLE).unwrap();
        db
    }

    fn spec(data_type: &str, not_null: bool) -> ColumnSpec {
        ColumnSpec {
            pos: 0,
            data_type: data_type.into(),
            pg_type_class: None,
            elem_pg_type_class: None,
            character_maximum_length: None,
            not_null: Some(not_null),
            dflt: None,
        }
    }

    #[test]
    fn creates_table_and_enforces_primary_key() {
        let db = setup();
        let store = ColumnMetadataStore::new(&db);
        assert!(store.has_table().unwrap());
        store
            .insert("users", "id", &spec("int8", true), None)
            .unwrap();
        // Duplicate (table, column) violates the PK.
        assert!(store
            .insert("users", "id", &spec("int4", false), None)
            .is_err());
    }

    #[test]
    fn insert_and_read_metadata() {
        let db = setup();
        let store = ColumnMetadataStore::new(&db);
        store
            .insert("users", "id", &spec("int8", true), None)
            .unwrap();
        assert_eq!(
            store.get_column("users", "id").unwrap().unwrap(),
            ColumnMetadata {
                upstream_type: "int8".into(),
                is_not_null: true,
                is_enum: false,
                is_array: false,
                character_max_length: None,
                is_backfilling: false,
            }
        );

        // With a backfill marker.
        store
            .insert(
                "users",
                "value",
                &spec("text", false),
                Some(&JsonValue::Number(123.0)),
            )
            .unwrap();
        let m = store.get_column("users", "value").unwrap().unwrap();
        assert_eq!(m.upstream_type, "text");
        assert!(m.is_backfilling);
    }

    #[test]
    fn update_column_metadata() {
        let db = setup();
        let store = ColumnMetadataStore::new(&db);
        store
            .insert("users", "name", &spec("varchar", false), None)
            .unwrap();
        let mut s = spec("varchar", true);
        s.character_maximum_length = Some(200);
        store.update("users", "name", "full_name", &s).unwrap();
        let m = store.get_column("users", "full_name").unwrap().unwrap();
        assert_eq!(m.upstream_type, "varchar");
        assert!(m.is_not_null);
        assert_eq!(m.character_max_length, Some(200));
        assert!(store.get_column("users", "name").unwrap().is_none());
    }

    #[test]
    fn clear_backfilling() {
        let db = setup();
        let store = ColumnMetadataStore::new(&db);
        let boo = JsonValue::String("boo".into());
        store
            .insert("users", "id", &spec("int8", false), Some(&boo))
            .unwrap();
        store
            .insert("users", "val", &spec("text", false), Some(&boo))
            .unwrap();
        assert!(
            store
                .get_column("users", "id")
                .unwrap()
                .unwrap()
                .is_backfilling
        );
        assert!(
            store
                .get_column("users", "val")
                .unwrap()
                .unwrap()
                .is_backfilling
        );

        store.clear_backfilling("users", "val").unwrap();
        assert!(
            store
                .get_column("users", "id")
                .unwrap()
                .unwrap()
                .is_backfilling
        );
        assert!(
            !store
                .get_column("users", "val")
                .unwrap()
                .unwrap()
                .is_backfilling
        );
    }

    #[test]
    fn delete_and_rename() {
        let db = setup();
        let store = ColumnMetadataStore::new(&db);
        store
            .insert("users", "id", &spec("int8", false), None)
            .unwrap();
        store
            .insert("users", "name", &spec("varchar", false), None)
            .unwrap();
        store.delete_column("users", "name").unwrap();
        assert_eq!(store.get_table("users").unwrap().len(), 1);

        store
            .insert("posts", "id", &spec("int8", false), None)
            .unwrap();
        store.rename_table("users", "people").unwrap();
        assert_eq!(store.get_table("people").unwrap().len(), 1);
        store.delete_table("people").unwrap();
        assert_eq!(store.get_table("people").unwrap().len(), 0);
        assert_eq!(store.get_table("posts").unwrap().len(), 1);
    }
}
