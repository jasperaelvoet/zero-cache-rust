//! Port of the row-key-derivation core of `ChangeProcessor` in
//! `zero-cache/src/services/replicator/change-processor.ts` (the `#getKey`
//! method): the logic that turns an incoming insert/update/delete row plus its
//! relation's replica-identity into the key used for the changelog and SQL
//! WHERE clause.
//!
//! The full `ChangeProcessor` (insert/update/delete/DDL apply, table-spec
//! caching, change-log integration) is substantial and depends on pieces not
//! yet wired together (backfill tracking, table metadata); this ports the
//! pure, independently-correct key-extraction step first.

use zero_cache_types::lite::LiteValue;
use zero_cache_types::sql::id;

use thiserror::Error;

use crate::change_log::ChangeLog;
use crate::{DbError, StatementRunner, Value};

/// The row-key kind from the relation's replica identity, mirroring
/// `MessageRelation.rowKey.type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKeyKind {
    Default,
    Nothing,
    Full,
    Index,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GetKeyError {
    #[error("Cannot replicate table \"{0}\" without a PRIMARY KEY or UNIQUE INDEX")]
    NoKeyColumns(String),
}

/// Derives the row key from `row` given the relation's declared key columns
/// (or, for `replicaIdentity == full`, the table's primary key). Port of
/// `ChangeProcessor.#getKey`.
///
/// - `row` / `num_cols`: the (already lite-converted) incoming row and its
///   column count, mirroring the `{row, numCols}` returned by `liteRow`.
/// - `relation_row_key_columns`: the relation's declared row-key columns
///   (`relation.rowKey.columns`); ignored when `kind == Full`.
/// - `table_name`: used only for the error message.
/// - `table_primary_key`: the replica table's primary key, used when
///   `kind == Full` (a full-row replica identity means the wire message
///   carries no explicit key, so the table's own primary key is used).
///
/// When the row already has exactly the key columns (the common
/// replica-identity-default case), the row is returned unchanged (matching
/// the TS fast path that avoids allocating a new object).
pub fn get_key(
    row: &[(String, LiteValue)],
    num_cols: usize,
    kind: RowKeyKind,
    relation_row_key_columns: &[String],
    table_name: &str,
    table_primary_key: Option<&[String]>,
) -> Result<Vec<(String, LiteValue)>, GetKeyError> {
    let key_columns: &[String] = if kind != RowKeyKind::Full {
        relation_row_key_columns
    } else {
        table_primary_key.unwrap_or(&[])
    };

    if key_columns.is_empty() {
        return Err(GetKeyError::NoKeyColumns(table_name.to_string()));
    }

    if num_cols == key_columns.len() {
        return Ok(row.to_vec());
    }

    let mut key = Vec::with_capacity(key_columns.len());
    for col in key_columns {
        if let Some((_, v)) = row.iter().find(|(name, _)| name == col) {
            key.push((col.clone(), v.clone()));
        }
    }
    Ok(key)
}

/// Converts a [`LiteValue`] to a rusqlite bind [`Value`]. Whole-number
/// `Number`s bind as `INTEGER` (matching better-sqlite3's default numeric
/// storage for JS integers); everything else as `REAL`. Out-of-`i64`-range
/// `Big` values fall back to their decimal string (SQLite has no arbitrary
/// precision integer type).
pub(crate) fn to_sql_value(v: &LiteValue) -> Value {
    match v {
        LiteValue::Null => Value::Null,
        LiteValue::Text(s) => Value::Text(s.clone()),
        LiteValue::Blob(b) => Value::Blob(b.clone()),
        LiteValue::Number(n) => {
            if n.fract() == 0.0 && n.is_finite() && n.abs() < 9.2e18 {
                Value::Integer(*n as i64)
            } else {
                Value::Real(*n)
            }
        }
        LiteValue::Big(b) => match b.to_string().parse::<i64>() {
            Ok(i) => Value::Integer(i),
            Err(_) => Value::Text(b.to_string()),
        },
    }
}

/// Errors from applying a row change.
#[derive(Debug, Error)]
pub enum ApplyError {
    #[error(transparent)]
    Key(#[from] GetKeyError),
    #[error(transparent)]
    Db(#[from] DbError),
}

/// Applies insert/update/delete row changes to the SQLite replica and records
/// them in the change log. Port of the row-mutation portion of
/// `ChangeProcessor` (`processInsert`/`processUpdate`/`processDelete`,
/// `#upsert`, `#delete`); DDL handling, table-spec caching, and backfill
/// tracking are not part of this port.
pub struct RowApplier<'a> {
    pub db: &'a StatementRunner,
    pub change_log: ChangeLog<'a>,
}

impl<'a> RowApplier<'a> {
    pub fn new(db: &'a StatementRunner) -> Self {
        RowApplier {
            db,
            change_log: ChangeLog::new(db),
        }
    }

    /// `INSERT OR REPLACE`s `row` into `table`. Port of `#upsert`.
    pub fn upsert(&self, table: &str, row: &[(String, LiteValue)]) -> Result<(), DbError> {
        let columns: Vec<String> = row.iter().map(|(c, _)| id(c)).collect();
        let placeholders = vec!["?"; row.len()].join(",");
        let sql = format!(
            "INSERT OR REPLACE INTO {} ({}) VALUES ({placeholders})",
            id(table),
            columns.join(",")
        );
        let values: Vec<Value> = row.iter().map(|(_, v)| to_sql_value(v)).collect();
        self.db.run(&sql, &values)?;
        Ok(())
    }

    /// Deletes the row matching `row_key` from `table`. Port of `#delete`.
    pub fn delete(&self, table: &str, row_key: &[(String, LiteValue)]) -> Result<(), DbError> {
        let conds: Vec<String> = row_key
            .iter()
            .map(|(c, _)| format!("{}=?", id(c)))
            .collect();
        let sql = format!("DELETE FROM {} WHERE {}", id(table), conds.join(" AND "));
        let values: Vec<Value> = row_key.iter().map(|(_, v)| to_sql_value(v)).collect();
        self.db.run(&sql, &values)?;
        Ok(())
    }

    /// `UPDATE`s the row matching `key` with `row`'s values; returns the
    /// number of rows changed. Port of the `UPDATE ... SET ... WHERE ...` in
    /// `processUpdate` (the caller decides whether to fall back to
    /// [`upsert`](Self::upsert) when zero rows changed, matching the "resumptive
    /// replication" behavior).
    pub fn update(
        &self,
        table: &str,
        row: &[(String, LiteValue)],
        key: &[(String, LiteValue)],
    ) -> Result<u64, DbError> {
        let set_exprs: Vec<String> = row.iter().map(|(c, _)| format!("{}=?", id(c))).collect();
        let conds: Vec<String> = key.iter().map(|(c, _)| format!("{}=?", id(c))).collect();
        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            id(table),
            set_exprs.join(","),
            conds.join(" AND ")
        );
        let mut values: Vec<Value> = row.iter().map(|(_, v)| to_sql_value(v)).collect();
        values.extend(key.iter().map(|(_, v)| to_sql_value(v)));
        let result = self.db.run(&sql, &values)?;
        Ok(result.changes)
    }

    /// Processes an insert: upserts the row, then (unless the relation has no
    /// row key at all, in which case the row can't participate in IVM) logs a
    /// set-op keyed by the row's key. Port of `processInsert`.
    ///
    /// `row` must already include the `_0_version` column value.
    pub fn process_insert(
        &self,
        table: &str,
        row: &[(String, LiteValue)],
        row_key_kind: RowKeyKind,
        relation_row_key_columns: &[String],
        table_primary_key: Option<&[String]>,
        version: &str,
        pos: &mut i64,
    ) -> Result<(), ApplyError> {
        self.upsert(table, row)?;

        if relation_row_key_columns.is_empty() && row_key_kind != RowKeyKind::Full {
            // No PRIMARY KEY / UNIQUE INDEX: written to the replica but not
            // recorded in the change log (can't participate in IVM).
            return Ok(());
        }

        let key = get_key(
            row,
            row.len(),
            row_key_kind,
            relation_row_key_columns,
            table,
            table_primary_key,
        )?;
        self.change_log
            .log_set_op(version, *pos, table, &to_json_row_key(&key), None)
            .map_err(ApplyError::Db)?;
        *pos += 1;
        Ok(())
    }

    /// Processes an update. `old_key`, if present, is the row's key *before*
    /// the update (set when the update changed the key, or the relation's
    /// replica identity is `full`); `new_key` is the key after. Order of
    /// operations matters and is preserved from upstream: log the delete of
    /// `old_key` (if any) before logging the set of `new_key`, then attempt
    /// the `UPDATE`, falling back to [`upsert`](Self::upsert) if it affected
    /// zero rows (a row added to the publication mid-stream, "resumptive
    /// replication"). Port of `processUpdate`.
    ///
    /// `row` must already include the `_0_version` column value.
    #[allow(clippy::too_many_arguments)]
    pub fn process_update(
        &self,
        table: &str,
        row: &[(String, LiteValue)],
        old_key: Option<&[(String, LiteValue)]>,
        new_key: &[(String, LiteValue)],
        version: &str,
        pos: &mut i64,
    ) -> Result<(), ApplyError> {
        if let Some(old_key) = old_key {
            self.change_log
                .log_delete_op(version, *pos, table, &to_json_row_key(old_key))
                .map_err(ApplyError::Db)?;
            *pos += 1;
        }
        self.change_log
            .log_set_op(version, *pos, table, &to_json_row_key(new_key), None)
            .map_err(ApplyError::Db)?;
        *pos += 1;

        let curr_key = old_key.unwrap_or(new_key);
        let changes = self.update(table, row, curr_key)?;
        if changes == 0 {
            self.upsert(table, row)?;
        }
        Ok(())
    }

    /// Processes a delete: removes the row, then logs a delete-op. Port of
    /// `processDelete`.
    pub fn process_delete(
        &self,
        table: &str,
        key: &[(String, LiteValue)],
        version: &str,
        pos: &mut i64,
    ) -> Result<(), ApplyError> {
        self.delete(table, key)?;
        self.change_log
            .log_delete_op(version, *pos, table, &to_json_row_key(key))
            .map_err(ApplyError::Db)?;
        *pos += 1;
        Ok(())
    }
}

/// Converts a lite row key to the JSON-valued form [`ChangeLog`] expects.
/// `Blob` keys are not expected in practice (row keys are scalar/bigint) and
/// map to `Null` rather than failing.
fn to_json_row_key(
    key: &[(String, LiteValue)],
) -> Vec<(String, zero_cache_shared::bigint_json::JsonValue)> {
    use zero_cache_shared::bigint_json::JsonValue;
    key.iter()
        .map(|(k, v)| {
            let jv = match v {
                LiteValue::Null => JsonValue::Null,
                LiteValue::Text(s) => JsonValue::String(s.clone()),
                LiteValue::Number(n) => JsonValue::Number(*n),
                LiteValue::Big(b) => JsonValue::BigInt(b.clone()),
                LiteValue::Blob(_) => JsonValue::Null,
            };
            (k.clone(), jv)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;

    fn row(pairs: &[(&str, LiteValue)]) -> Vec<(String, LiteValue)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn default_identity_uses_relation_row_key_columns() {
        let r = row(&[("id", LiteValue::Number(1.0))]);
        let key = get_key(&r, 1, RowKeyKind::Default, &["id".to_string()], "t", None).unwrap();
        // Fast path: numCols == keyColumns.len() -> row returned as-is.
        assert_eq!(key, r);
    }

    #[test]
    fn partial_row_extracts_only_key_columns() {
        let r = row(&[
            ("id", LiteValue::Number(1.0)),
            ("name", LiteValue::Text("bob".into())),
        ]);
        let key = get_key(&r, 2, RowKeyKind::Default, &["id".to_string()], "t", None).unwrap();
        assert_eq!(key, row(&[("id", LiteValue::Number(1.0))]));
    }

    #[test]
    fn full_identity_uses_table_primary_key() {
        let r = row(&[
            ("id", LiteValue::Number(1.0)),
            ("name", LiteValue::Text("bob".into())),
        ]);
        let pk = vec!["id".to_string()];
        let key = get_key(&r, 2, RowKeyKind::Full, &[], "t", Some(&pk)).unwrap();
        assert_eq!(key, row(&[("id", LiteValue::Number(1.0))]));
    }

    #[test]
    fn errors_without_key_columns() {
        let r = row(&[("id", LiteValue::Number(1.0))]);
        let err = get_key(&r, 1, RowKeyKind::Default, &[], "issues", None).unwrap_err();
        assert_eq!(err, GetKeyError::NoKeyColumns("issues".to_string()));
    }

    #[test]
    fn errors_when_full_identity_table_has_no_primary_key() {
        let r = row(&[("id", LiteValue::Number(1.0))]);
        let err = get_key(&r, 1, RowKeyKind::Full, &[], "issues", None).unwrap_err();
        assert_eq!(err, GetKeyError::NoKeyColumns("issues".to_string()));
    }

    #[test]
    fn supports_bigint_and_compound_keys() {
        let r = row(&[
            ("org_id", LiteValue::Big(BigInt::from(42))),
            ("issue_id", LiteValue::Number(7.0)),
            ("title", LiteValue::Text("bug".into())),
        ]);
        let key = get_key(
            &r,
            3,
            RowKeyKind::Default,
            &["org_id".to_string(), "issue_id".to_string()],
            "issues",
            None,
        )
        .unwrap();
        assert_eq!(
            key,
            row(&[
                ("org_id", LiteValue::Big(BigInt::from(42))),
                ("issue_id", LiteValue::Number(7.0))
            ])
        );
    }

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issues(id INT PRIMARY KEY, title TEXT, _0_version TEXT)")
            .unwrap();
        db.exec(crate::change_log::CREATE_CHANGELOG_SCHEMA).unwrap();
        db
    }

    fn read_row(db: &StatementRunner, id_val: i64) -> Option<String> {
        db.query_uncached(
            "SELECT title FROM issues WHERE id = ?",
            &[Value::Integer(id_val)],
        )
        .unwrap()
        .into_iter()
        .next()
        .map(|r| match &r[0].1 {
            Value::Text(s) => s.clone(),
            _ => String::new(),
        })
    }

    #[test]
    fn process_insert_writes_row_and_change_log() {
        let db = setup();
        let applier = RowApplier::new(&db);
        let row_data = row(&[
            ("id", LiteValue::Number(1.0)),
            ("title", LiteValue::Text("bug".into())),
            ("_0_version", LiteValue::Text("01".into())),
        ]);
        applier
            .process_insert(
                "issues",
                &row_data,
                RowKeyKind::Default,
                &["id".to_string()],
                None,
                "01",
                &mut 0,
            )
            .unwrap();

        assert_eq!(read_row(&db, 1), Some("bug".to_string()));
        let entry = applier
            .change_log
            .get_latest_row_op(
                "issues",
                &to_json_row_key(&row(&[("id", LiteValue::Number(1.0))])),
            )
            .unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().op, "s");
    }

    #[test]
    fn process_insert_without_key_skips_change_log() {
        let db = setup();
        let applier = RowApplier::new(&db);
        let row_data = row(&[
            ("id", LiteValue::Number(2.0)),
            ("title", LiteValue::Text("no-key".into())),
            ("_0_version", LiteValue::Text("01".into())),
        ]);
        applier
            .process_insert(
                "issues",
                &row_data,
                RowKeyKind::Default,
                &[],
                None,
                "01",
                &mut 0,
            )
            .unwrap();
        assert_eq!(read_row(&db, 2), Some("no-key".to_string()));
        let entry = applier
            .change_log
            .get_latest_row_op(
                "issues",
                &to_json_row_key(&row(&[("id", LiteValue::Number(2.0))])),
            )
            .unwrap();
        assert!(entry.is_none());
    }

    #[test]
    fn process_delete_removes_row_and_logs() {
        let db = setup();
        let applier = RowApplier::new(&db);
        let row_data = row(&[
            ("id", LiteValue::Number(3.0)),
            ("title", LiteValue::Text("gone".into())),
            ("_0_version", LiteValue::Text("01".into())),
        ]);
        applier
            .process_insert(
                "issues",
                &row_data,
                RowKeyKind::Default,
                &["id".to_string()],
                None,
                "01",
                &mut 0,
            )
            .unwrap();
        assert_eq!(read_row(&db, 3), Some("gone".to_string()));

        let key = row(&[("id", LiteValue::Number(3.0))]);
        applier
            .process_delete("issues", &key, "02", &mut 0)
            .unwrap();
        assert_eq!(read_row(&db, 3), None);

        let entry = applier
            .change_log
            .get_latest_row_op("issues", &to_json_row_key(&key))
            .unwrap()
            .unwrap();
        assert_eq!(entry.op, "d");
    }

    #[test]
    fn update_falls_back_to_upsert_when_no_rows_changed() {
        let db = setup();
        let applier = RowApplier::new(&db);
        // No existing row with id=9: UPDATE affects 0 rows.
        let key = row(&[("id", LiteValue::Number(9.0))]);
        let new_row = row(&[
            ("id", LiteValue::Number(9.0)),
            ("title", LiteValue::Text("resumptive".into())),
            ("_0_version", LiteValue::Text("03".into())),
        ]);
        let changes = applier.update("issues", &new_row, &key).unwrap();
        assert_eq!(changes, 0);
        // Caller falls back to upsert (mirroring processUpdate's behavior).
        applier.upsert("issues", &new_row).unwrap();
        assert_eq!(read_row(&db, 9), Some("resumptive".to_string()));
    }

    #[test]
    fn update_modifies_existing_row() {
        let db = setup();
        let applier = RowApplier::new(&db);
        let row_data = row(&[
            ("id", LiteValue::Number(4.0)),
            ("title", LiteValue::Text("old".into())),
            ("_0_version", LiteValue::Text("01".into())),
        ]);
        applier
            .process_insert(
                "issues",
                &row_data,
                RowKeyKind::Default,
                &["id".to_string()],
                None,
                "01",
                &mut 0,
            )
            .unwrap();

        let key = row(&[("id", LiteValue::Number(4.0))]);
        let updated = row(&[
            ("id", LiteValue::Number(4.0)),
            ("title", LiteValue::Text("new".into())),
            ("_0_version", LiteValue::Text("02".into())),
        ]);
        let changes = applier.update("issues", &updated, &key).unwrap();
        assert_eq!(changes, 1);
        assert_eq!(read_row(&db, 4), Some("new".to_string()));
    }

    #[test]
    fn process_update_key_unchanged() {
        let db = setup();
        let applier = RowApplier::new(&db);
        let mut pos = 0i64;
        let row_data = row(&[
            ("id", LiteValue::Number(5.0)),
            ("title", LiteValue::Text("old".into())),
            ("_0_version", LiteValue::Text("01".into())),
        ]);
        applier
            .process_insert(
                "issues",
                &row_data,
                RowKeyKind::Default,
                &["id".to_string()],
                None,
                "01",
                &mut pos,
            )
            .unwrap();
        assert_eq!(pos, 1);

        let key = row(&[("id", LiteValue::Number(5.0))]);
        let updated = row(&[
            ("id", LiteValue::Number(5.0)),
            ("title", LiteValue::Text("new".into())),
            ("_0_version", LiteValue::Text("02".into())),
        ]);
        applier
            .process_update("issues", &updated, None, &key, "02", &mut pos)
            .unwrap();
        // No oldKey -> only one change-log write (the set-op), pos advances by 1.
        assert_eq!(pos, 2);
        assert_eq!(read_row(&db, 5), Some("new".to_string()));

        let entry = applier
            .change_log
            .get_latest_row_op("issues", &to_json_row_key(&key))
            .unwrap()
            .unwrap();
        assert_eq!(entry.state_version, "02");
        assert_eq!(entry.op, "s");
    }

    #[test]
    fn process_update_key_changed_deletes_old_and_logs_new() {
        let db = setup();
        let applier = RowApplier::new(&db);
        let mut pos = 0i64;
        let row_data = row(&[
            ("id", LiteValue::Number(6.0)),
            ("title", LiteValue::Text("v1".into())),
            ("_0_version", LiteValue::Text("01".into())),
        ]);
        applier
            .process_insert(
                "issues",
                &row_data,
                RowKeyKind::Default,
                &["id".to_string()],
                None,
                "01",
                &mut pos,
            )
            .unwrap();

        // Key changes from id=6 to id=7 (simulating a PK update).
        let old_key = row(&[("id", LiteValue::Number(6.0))]);
        let new_key = row(&[("id", LiteValue::Number(7.0))]);
        let updated = row(&[
            ("id", LiteValue::Number(7.0)),
            ("title", LiteValue::Text("v2".into())),
            ("_0_version", LiteValue::Text("02".into())),
        ]);
        // UPDATE targets old_key (the row's current identity in the table).
        applier
            .process_update("issues", &updated, Some(&old_key), &new_key, "02", &mut pos)
            .unwrap();

        // Both a delete-op (old) and set-op (new) were logged -> pos advances by 2.
        assert_eq!(pos, 3);

        let old_entry = applier
            .change_log
            .get_latest_row_op("issues", &to_json_row_key(&old_key))
            .unwrap()
            .unwrap();
        assert_eq!(old_entry.op, "d");
        let new_entry = applier
            .change_log
            .get_latest_row_op("issues", &to_json_row_key(&new_key))
            .unwrap()
            .unwrap();
        assert_eq!(new_entry.op, "s");
    }

    #[test]
    fn process_update_resumptive_fallback() {
        let db = setup();
        let applier = RowApplier::new(&db);
        let mut pos = 0i64;
        // No existing row: UPDATE affects 0 rows, so process_update upserts instead.
        let key = row(&[("id", LiteValue::Number(8.0))]);
        let new_row = row(&[
            ("id", LiteValue::Number(8.0)),
            ("title", LiteValue::Text("resumed".into())),
            ("_0_version", LiteValue::Text("01".into())),
        ]);
        applier
            .process_update("issues", &new_row, None, &key, "01", &mut pos)
            .unwrap();
        assert_eq!(read_row(&db, 8), Some("resumed".to_string()));
    }
}
