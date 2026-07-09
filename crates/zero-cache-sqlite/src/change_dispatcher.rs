//! Port of the top-level dispatch/transaction orchestration of
//! `ChangeProcessor`/`TransactionProcessor` in
//! `zero-cache/src/services/replicator/change-processor.ts`: the table-spec
//! cache, the begin/commit/rollback state machine, and routing an incoming
//! [`Change`](zero_cache_change_source::data::Change) to the right
//! [`RowApplier`]/[`DdlApplier`] method.
//!
//! Not ported (see module docs on [`crate::ddl_apply`] and
//! [`crate::row_apply`]): the SQLITE_BUSY retry loop around transaction
//! start (`#beginTransaction`'s retry-on-busy behavior — a
//! deployment/operational concern, not core logic), and `backfill`/
//! `backfill-completed` message handling (backfill tracking is unported).

use std::collections::HashMap;

use zero_cache_change_source::data::{Change, RowKeyKind as ChangeRowKeyKind};
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_types::lite::{lite_row, JsonFormat as LiteJsonFormat, PgValue};
use zero_cache_types::specs::LiteTableSpec;

use crate::ddl_apply::{DdlApplier, DdlError};
use crate::lite_tables::list_tables;
use crate::row_apply::{ApplyError, RowApplier, RowKeyKind};
use crate::{DbError, StatementRunner};

/// The outcome of committing a transaction. Port of `CommitResult`.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitResult {
    pub watermark: String,
    pub schema_changed: bool,
    pub num_change_log_entries: i64,
}

/// Errors from dispatching a change.
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("Already in a transaction")]
    AlreadyInTransaction,
    #[error("Received message outside of transaction")]
    NoActiveTransaction,
    #[error("Unknown table {0}")]
    UnknownTable(String),
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Apply(#[from] ApplyError),
    #[error(transparent)]
    Ddl(#[from] DdlError),
}

fn row_key_kind(kind: Option<zero_cache_change_source::data::RowKeyKind>) -> RowKeyKind {
    match kind {
        Some(ChangeRowKeyKind::Full) => RowKeyKind::Full,
        Some(ChangeRowKeyKind::Nothing) => RowKeyKind::Nothing,
        Some(ChangeRowKeyKind::Index) => RowKeyKind::Index,
        Some(ChangeRowKeyKind::Default) | None => RowKeyKind::Default,
    }
}

/// Converts a wire [`JsonValue`] row into [`PgValue`]s for `liteRow`
/// conversion. `Blob`/`Bytes` values are not representable in the JSON wire
/// format (they arrive pre-encoded, e.g. as base64 strings, in practice) so
/// `JsonValue::String` maps to `PgValue::String` rather than attempting a
/// bytes decode here.
fn json_row_to_pg(row: &[(String, JsonValue)]) -> Vec<(String, PgValue)> {
    row.iter()
        .map(|(k, v)| (k.clone(), json_to_pg(v)))
        .collect()
}

fn json_to_pg(v: &JsonValue) -> PgValue {
    match v {
        JsonValue::Null => PgValue::Null,
        JsonValue::Bool(b) => PgValue::Bool(*b),
        JsonValue::Number(n) => PgValue::Number(*n),
        JsonValue::BigInt(b) => PgValue::BigInt(b.clone()),
        JsonValue::String(s) => PgValue::String(s.clone()),
        JsonValue::Array(items) => PgValue::Array(items.iter().map(json_to_pg).collect()),
        JsonValue::Object(entries) => PgValue::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), json_to_pg(v)))
                .collect(),
        ),
    }
}

/// Orchestrates applying a stream of [`Change`]s to the SQLite replica,
/// maintaining the table-spec cache and per-transaction change-log position
/// counter. Port of `ChangeProcessor` + `TransactionProcessor`.
pub struct ChangeDispatcher<'a> {
    db: &'a StatementRunner,
    row: RowApplier<'a>,
    ddl: DdlApplier<'a>,
    table_specs: HashMap<String, LiteTableSpec>,
    json_format: LiteJsonFormat,

    // Per-transaction state (reset on `begin`).
    in_transaction: bool,
    version: String,
    pos: i64,
    schema_changed: bool,
    num_change_log_entries: i64,
}

impl<'a> ChangeDispatcher<'a> {
    pub fn new(db: &'a StatementRunner) -> Result<Self, DbError> {
        let table_specs = Self::load_table_specs(db)?;
        Ok(ChangeDispatcher {
            db,
            row: RowApplier::new(db),
            ddl: DdlApplier::new(db),
            table_specs,
            json_format: LiteJsonFormat::Parsed,
            in_transaction: false,
            version: String::new(),
            pos: 0,
            schema_changed: false,
            num_change_log_entries: 0,
        })
    }

    fn load_table_specs(db: &StatementRunner) -> Result<HashMap<String, LiteTableSpec>, DbError> {
        Ok(list_tables(db)?
            .into_iter()
            .map(|t| (t.name.clone(), t))
            .collect())
    }

    fn reload_table_specs(&mut self) -> Result<(), DbError> {
        self.table_specs = Self::load_table_specs(self.db)?;
        Ok(())
    }

    fn table_spec(&self, name: &str) -> Result<&LiteTableSpec, DispatchError> {
        self.table_specs
            .get(name)
            .ok_or_else(|| DispatchError::UnknownTable(name.to_string()))
    }

    /// Starts a transaction at `commit_version` (the watermark the pending
    /// changes will be recorded under). Port of `#beginTransaction` (sans the
    /// SQLITE_BUSY retry loop).
    pub fn begin(&mut self, commit_version: &str) -> Result<(), DispatchError> {
        if self.in_transaction {
            return Err(DispatchError::AlreadyInTransaction);
        }
        self.db.begin()?;
        self.in_transaction = true;
        self.version = commit_version.to_string();
        self.pos = 0;
        self.schema_changed = false;
        self.num_change_log_entries = 0;
        Ok(())
    }

    /// Applies a data or schema change within the current transaction. Port
    /// of the `switch (msg.tag)` dispatch in `#processMessage` plus the
    /// individual `TransactionProcessor.process*` methods it routes to.
    pub fn apply(&mut self, change: &Change) -> Result<(), DispatchError> {
        if !self.in_transaction {
            return Err(DispatchError::NoActiveTransaction);
        }
        match change {
            Change::Insert { relation, new } => {
                let table_name = lite_table_name(&relation.schema, &relation.name);
                let spec = self.table_spec(&table_name)?.clone();
                let pg_row = json_row_to_pg(new);
                let converted = lite_row(&pg_row, &spec, self.json_format);
                let mut row = converted.row;
                row.push((
                    zero_cache_types::pg_to_lite::ZERO_VERSION_COLUMN_NAME.to_string(),
                    zero_cache_types::lite::LiteValue::Text(self.version.clone()),
                ));
                self.row.process_insert(
                    &table_name,
                    &row,
                    row_key_kind(relation.row_key.kind),
                    &relation.row_key.columns,
                    spec.primary_key.as_deref(),
                    &self.version,
                    &mut self.pos,
                )?;
                self.num_change_log_entries += 1;
                Ok(())
            }
            Change::Delete { relation, key } => {
                let table_name = lite_table_name(&relation.schema, &relation.name);
                let spec = self.table_spec(&table_name)?.clone();
                let pg_key = json_row_to_pg(key);
                let converted = lite_row(&pg_key, &spec, self.json_format);
                self.row.process_delete(
                    &table_name,
                    &converted.row,
                    &self.version,
                    &mut self.pos,
                )?;
                self.num_change_log_entries += 1;
                Ok(())
            }
            Change::Truncate { relations } => {
                for relation in relations {
                    let table_name = lite_table_name(&relation.schema, &relation.name);
                    self.db.run(
                        &format!("DELETE FROM {}", zero_cache_types::sql::id(&table_name)),
                        &[],
                    )?;
                    self.ddl
                        .change_log
                        .log_truncate_op(&self.version, &table_name)?;
                    self.num_change_log_entries += 1;
                }
                Ok(())
            }
            Change::CreateTable(create) => {
                let lite_spec =
                    zero_cache_types::pg_to_lite::map_postgres_to_lite(&create.spec, None)
                        .map_err(DdlError::from)?;
                self.ddl.create_table(&lite_spec, &self.version)?;
                self.num_change_log_entries += 1;
                self.reload_table_specs()?;
                self.schema_changed = true;
                Ok(())
            }
            Change::DropTable { id } => {
                let lite_name = lite_table_name(&id.schema, &id.name);
                self.ddl
                    .drop_table(&id.schema, &id.name, &lite_name, &self.version)?;
                self.num_change_log_entries += 1;
                self.reload_table_specs()?;
                self.schema_changed = true;
                Ok(())
            }
            Change::RenameTable { old, new } => {
                let old_lite = lite_table_name(&old.schema, &old.name);
                let new_lite = lite_table_name(&new.schema, &new.name);
                self.ddl.rename_table(
                    &old.schema,
                    &old.name,
                    &new.schema,
                    &new.name,
                    &old_lite,
                    &new_lite,
                    &self.version,
                )?;
                self.num_change_log_entries += 1;
                self.reload_table_specs()?;
                self.schema_changed = true;
                Ok(())
            }
            Change::DropColumn { table, column } => {
                let lite_name = lite_table_name(&table.schema, &table.name);
                self.ddl.drop_column(
                    &lite_name,
                    column,
                    &table.schema,
                    &table.name,
                    &self.version,
                )?;
                self.num_change_log_entries += 1;
                self.reload_table_specs()?;
                self.schema_changed = true;
                Ok(())
            }
            Change::AddColumn { table, column, .. } => {
                let lite_name = lite_table_name(&table.schema, &table.name);
                self.ddl.add_column(
                    &lite_name,
                    &column.name,
                    &column.spec,
                    &table.schema,
                    &table.name,
                    &self.version,
                )?;
                self.num_change_log_entries += 1;
                self.reload_table_specs()?;
                self.schema_changed = true;
                Ok(())
            }
            Change::UpdateColumn { table, old, new } => {
                let lite_name = lite_table_name(&table.schema, &table.name);
                self.ddl.update_column(
                    &lite_name,
                    &old.name,
                    &new.name,
                    &old.spec,
                    &new.spec,
                    &table.schema,
                    &table.name,
                    &self.version,
                )?;
                self.num_change_log_entries += 1;
                self.reload_table_specs()?;
                self.schema_changed = true;
                Ok(())
            }
            Change::CreateIndex { spec } => {
                self.ddl.create_index(spec, &self.version)?;
                self.num_change_log_entries += 1;
                self.schema_changed = true;
                Ok(())
            }
            Change::DropIndex { id } => {
                let lite_name = lite_table_name(&id.schema, &id.name);
                self.ddl.drop_index(&lite_name)?;
                Ok(())
            }
            Change::Update { relation, key, new } => {
                let table_name = lite_table_name(&relation.schema, &relation.name);
                let spec = self.table_spec(&table_name)?.clone();
                let kind = row_key_kind(relation.row_key.kind);

                let pg_new = json_row_to_pg(new);
                let converted_new = lite_row(&pg_new, &spec, self.json_format);
                let mut row = converted_new.row.clone();
                row.push((
                    zero_cache_types::pg_to_lite::ZERO_VERSION_COLUMN_NAME.to_string(),
                    zero_cache_types::lite::LiteValue::Text(self.version.clone()),
                ));

                // `key`, if present, holds the row's key *before* the update
                // (set when the update changed the key, or replicaIdentity ==
                // full).
                let old_key = match key {
                    Some(k) => {
                        let pg_key = json_row_to_pg(k);
                        let converted_key = lite_row(&pg_key, &spec, self.json_format);
                        Some(
                            crate::row_apply::get_key(
                                &converted_key.row,
                                converted_key.num_cols,
                                kind,
                                &relation.row_key.columns,
                                &table_name,
                                spec.primary_key.as_deref(),
                            )
                            .map_err(ApplyError::from)?,
                        )
                    }
                    None => None,
                };
                let new_key = crate::row_apply::get_key(
                    &converted_new.row,
                    converted_new.num_cols,
                    kind,
                    &relation.row_key.columns,
                    &table_name,
                    spec.primary_key.as_deref(),
                )
                .map_err(ApplyError::from)?;

                let logged_old_key = old_key.is_some();
                self.row.process_update(
                    &table_name,
                    &row,
                    old_key.as_deref(),
                    &new_key,
                    &self.version,
                    &mut self.pos,
                )?;
                // process_update always logs a set-op for new_key, plus a
                // delete-op for old_key if present.
                self.num_change_log_entries += if logged_old_key { 2 } else { 1 };
                Ok(())
            }
            // Backfill messages are not yet wired at this dispatch layer
            // (backfill tracking itself is unported).
            Change::Backfill { .. } | Change::BackfillCompleted { .. } => Ok(()),
            Change::UpdateTableMetadata { table, new, .. } => {
                self.ddl.table_metadata.set_upstream_metadata(
                    &table.schema,
                    &table.name,
                    &JsonValue::Object(
                        new.row_key
                            .iter()
                            .chain(new.extra.iter())
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect(),
                    ),
                )?;
                Ok(())
            }
            Change::Begin { .. } | Change::Commit | Change::Rollback => {
                Err(DispatchError::AlreadyInTransaction) // Framing messages don't route through `apply`.
            }
        }
    }

    /// Commits the current transaction. Port of `TransactionProcessor.processCommit`.
    pub fn commit(&mut self, watermark: &str) -> Result<CommitResult, DispatchError> {
        if !self.in_transaction {
            return Err(DispatchError::NoActiveTransaction);
        }
        self.db.commit()?;
        self.in_transaction = false;
        Ok(CommitResult {
            watermark: watermark.to_string(),
            schema_changed: self.schema_changed,
            num_change_log_entries: self.num_change_log_entries,
        })
    }

    /// Aborts the current transaction. Port of `TransactionProcessor.abort`
    /// (invoked via `ChangeProcessor#fail`/`abort`).
    pub fn rollback(&mut self) -> Result<(), DispatchError> {
        if self.in_transaction {
            self.db.rollback()?;
            self.in_transaction = false;
        }
        Ok(())
    }

    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }
}

fn lite_table_name(schema: &str, name: &str) -> String {
    if schema == "public" {
        name.to_string()
    } else {
        format!("{schema}.{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_log::CREATE_CHANGELOG_SCHEMA;
    use crate::column_metadata::CREATE_COLUMN_METADATA_TABLE;
    use crate::table_metadata::CREATE_TABLE_METADATA_TABLE;
    use zero_cache_change_source::data::{Relation, RowKey};
    use zero_cache_types::specs::TableSpec;

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_TABLE_METADATA_TABLE).unwrap();
        db.exec(CREATE_COLUMN_METADATA_TABLE).unwrap();
        db
    }

    fn relation(name: &str, key_cols: &[&str]) -> Relation {
        Relation {
            schema: "public".into(),
            name: name.into(),
            row_key: RowKey {
                columns: key_cols.iter().map(|s| s.to_string()).collect(),
                kind: None,
            },
            columns: vec![],
        }
    }

    #[test]
    fn full_transaction_create_table_insert_commit() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();

        dispatcher.begin("01").unwrap();
        let table_spec = TableSpec {
            name: "issues".into(),
            schema: "public".into(),
            columns: vec![(
                "id".into(),
                zero_cache_types::specs::ColumnSpec::new("text", 1),
            )],
            primary_key: Some(vec!["id".into()]),
        };
        dispatcher
            .apply(&Change::CreateTable(
                zero_cache_change_source::data::TableCreate {
                    spec: table_spec,
                    metadata: None,
                    backfill: None,
                },
            ))
            .unwrap();
        let result = dispatcher.commit("01").unwrap();
        assert!(result.schema_changed);
        assert_eq!(result.num_change_log_entries, 1);

        // Now insert a row in a second transaction.
        dispatcher.begin("02").unwrap();
        dispatcher
            .apply(&Change::Insert {
                relation: relation("issues", &["id"]),
                new: vec![("id".to_string(), JsonValue::String("a".into()))],
            })
            .unwrap();
        let result2 = dispatcher.commit("02").unwrap();
        assert_eq!(result2.num_change_log_entries, 1);

        let rows = db.query_uncached("SELECT id FROM issues", &[]).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn insert_then_delete_within_separate_transactions() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();

        dispatcher.begin("01").unwrap();
        dispatcher
            .apply(&Change::CreateTable(
                zero_cache_change_source::data::TableCreate {
                    spec: TableSpec {
                        name: "issues".into(),
                        schema: "public".into(),
                        columns: vec![(
                            "id".into(),
                            zero_cache_types::specs::ColumnSpec::new("text", 1),
                        )],
                        primary_key: Some(vec!["id".into()]),
                    },
                    metadata: None,
                    backfill: None,
                },
            ))
            .unwrap();
        dispatcher.commit("01").unwrap();

        dispatcher.begin("02").unwrap();
        dispatcher
            .apply(&Change::Insert {
                relation: relation("issues", &["id"]),
                new: vec![("id".to_string(), JsonValue::String("a".into()))],
            })
            .unwrap();
        dispatcher.commit("02").unwrap();
        assert_eq!(
            db.query_uncached("SELECT id FROM issues", &[])
                .unwrap()
                .len(),
            1
        );

        dispatcher.begin("03").unwrap();
        dispatcher
            .apply(&Change::Delete {
                relation: relation("issues", &["id"]),
                key: vec![("id".to_string(), JsonValue::String("a".into()))],
            })
            .unwrap();
        dispatcher.commit("03").unwrap();
        assert!(db
            .query_uncached("SELECT id FROM issues", &[])
            .unwrap()
            .is_empty());
    }

    #[test]
    fn rollback_discards_changes() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        dispatcher.begin("01").unwrap();
        dispatcher
            .apply(&Change::CreateTable(
                zero_cache_change_source::data::TableCreate {
                    spec: TableSpec {
                        name: "issues".into(),
                        schema: "public".into(),
                        columns: vec![(
                            "id".into(),
                            zero_cache_types::specs::ColumnSpec::new("text", 1),
                        )],
                        primary_key: Some(vec!["id".into()]),
                    },
                    metadata: None,
                    backfill: None,
                },
            ))
            .unwrap();
        dispatcher.rollback().unwrap();

        let tables = db
            .query_uncached(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='issues'",
                &[],
            )
            .unwrap();
        assert!(tables.is_empty());
        assert!(!dispatcher.in_transaction());
    }

    #[test]
    fn double_begin_errors() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        dispatcher.begin("01").unwrap();
        assert!(matches!(
            dispatcher.begin("02"),
            Err(DispatchError::AlreadyInTransaction)
        ));
    }

    #[test]
    fn apply_outside_transaction_errors() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        let result = dispatcher.apply(&Change::Truncate { relations: vec![] });
        assert!(matches!(result, Err(DispatchError::NoActiveTransaction)));
    }

    #[test]
    fn truncate_deletes_rows_and_logs() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        dispatcher.begin("01").unwrap();
        dispatcher
            .apply(&Change::CreateTable(
                zero_cache_change_source::data::TableCreate {
                    spec: TableSpec {
                        name: "issues".into(),
                        schema: "public".into(),
                        columns: vec![(
                            "id".into(),
                            zero_cache_types::specs::ColumnSpec::new("text", 1),
                        )],
                        primary_key: Some(vec!["id".into()]),
                    },
                    metadata: None,
                    backfill: None,
                },
            ))
            .unwrap();
        dispatcher.commit("01").unwrap();

        dispatcher.begin("02").unwrap();
        dispatcher
            .apply(&Change::Insert {
                relation: relation("issues", &["id"]),
                new: vec![("id".to_string(), JsonValue::String("a".into()))],
            })
            .unwrap();
        dispatcher
            .apply(&Change::Truncate {
                relations: vec![relation("issues", &["id"])],
            })
            .unwrap();
        let result = dispatcher.commit("02").unwrap();
        assert_eq!(result.num_change_log_entries, 2);
        assert!(db
            .query_uncached("SELECT id FROM issues", &[])
            .unwrap()
            .is_empty());
    }

    fn issues_two_col_spec() -> TableSpec {
        TableSpec {
            name: "issues".into(),
            schema: "public".into(),
            columns: vec![
                (
                    "id".into(),
                    zero_cache_types::specs::ColumnSpec::new("text", 1),
                ),
                (
                    "title".into(),
                    zero_cache_types::specs::ColumnSpec::new("text", 2),
                ),
            ],
            primary_key: Some(vec!["id".into()]),
        }
    }

    #[test]
    fn update_key_unchanged_modifies_row() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();

        dispatcher.begin("01").unwrap();
        dispatcher
            .apply(&Change::CreateTable(
                zero_cache_change_source::data::TableCreate {
                    spec: issues_two_col_spec(),
                    metadata: None,
                    backfill: None,
                },
            ))
            .unwrap();
        dispatcher
            .apply(&Change::Insert {
                relation: relation("issues", &["id"]),
                new: vec![
                    ("id".to_string(), JsonValue::String("a".into())),
                    ("title".to_string(), JsonValue::String("old".into())),
                ],
            })
            .unwrap();
        dispatcher.commit("01").unwrap();

        dispatcher.begin("02").unwrap();
        dispatcher
            .apply(&Change::Update {
                relation: relation("issues", &["id"]),
                key: None, // key unchanged: replica identity default.
                new: vec![
                    ("id".to_string(), JsonValue::String("a".into())),
                    ("title".to_string(), JsonValue::String("new".into())),
                ],
            })
            .unwrap();
        let result = dispatcher.commit("02").unwrap();
        assert_eq!(result.num_change_log_entries, 1);

        let rows = db
            .query_uncached(
                "SELECT title FROM issues WHERE id = ?",
                &[crate::Value::Text("a".into())],
            )
            .unwrap();
        assert_eq!(rows[0][0].1, crate::Value::Text("new".into()));
    }

    #[test]
    fn update_key_changed_moves_row_identity() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();

        dispatcher.begin("01").unwrap();
        dispatcher
            .apply(&Change::CreateTable(
                zero_cache_change_source::data::TableCreate {
                    spec: issues_two_col_spec(),
                    metadata: None,
                    backfill: None,
                },
            ))
            .unwrap();
        dispatcher
            .apply(&Change::Insert {
                relation: relation("issues", &["id"]),
                new: vec![
                    ("id".to_string(), JsonValue::String("a".into())),
                    ("title".to_string(), JsonValue::String("t".into())),
                ],
            })
            .unwrap();
        dispatcher.commit("01").unwrap();

        // Update changes the primary key from "a" to "b"; `key` carries the
        // old identity, matching what Postgres sends when the key changes.
        dispatcher.begin("02").unwrap();
        dispatcher
            .apply(&Change::Update {
                relation: relation("issues", &["id"]),
                key: Some(vec![("id".to_string(), JsonValue::String("a".into()))]),
                new: vec![
                    ("id".to_string(), JsonValue::String("b".into())),
                    ("title".to_string(), JsonValue::String("t".into())),
                ],
            })
            .unwrap();
        let result = dispatcher.commit("02").unwrap();
        // Both a delete-op (old key) and a set-op (new key) were logged.
        assert_eq!(result.num_change_log_entries, 2);

        assert!(db
            .query_uncached(
                "SELECT id FROM issues WHERE id = ?",
                &[crate::Value::Text("a".into())]
            )
            .unwrap()
            .is_empty());
        assert_eq!(
            db.query_uncached(
                "SELECT id FROM issues WHERE id = ?",
                &[crate::Value::Text("b".into())]
            )
            .unwrap()
            .len(),
            1
        );
    }
}
