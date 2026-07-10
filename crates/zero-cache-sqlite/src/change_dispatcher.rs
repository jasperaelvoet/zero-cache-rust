//! Port of the top-level dispatch/transaction orchestration of
//! `ChangeProcessor`/`TransactionProcessor` in
//! `zero-cache/src/services/replicator/change-processor.ts`: the table-spec
//! cache, the begin/commit/rollback state machine, and routing an incoming
//! [`Change`](zero_cache_change_source::data::Change) to the right
//! [`RowApplier`]/[`DdlApplier`] method.
//!
//! Not ported: the SQLITE_BUSY retry loop around transaction start
//! (`#beginTransaction`'s retry-on-busy behavior — a deployment/operational
//! concern). The replica-side `backfill` and `backfill-completed` semantics are
//! implemented here; scheduling/restarting those streams remains the
//! change-source service's responsibility.

use std::collections::{HashMap, HashSet};

use zero_cache_change_source::data::{
    BackfillId, Change, DownloadStatus, Relation, RowKeyKind as ChangeRowKeyKind, TableMetadata,
};
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_types::lite::{lite_row, JsonFormat as LiteJsonFormat, LiteValue, PgValue};
use zero_cache_types::specs::LiteTableSpec;
use zero_cache_types::{pg_to_lite::ZERO_VERSION_COLUMN_NAME, sql::id};

use crate::change_log::{DEL_OP, SET_OP};
use crate::column_metadata::ColumnMetadataStore;
use crate::ddl_apply::{DdlApplier, DdlError};
use crate::lite_tables::list_tables_including_backfilling;
use crate::row_apply::{to_json_row_key, to_sql_value, ApplyError, RowApplier, RowKeyKind};
use crate::{DbError, StatementRunner, Value};

/// The outcome of committing a transaction. Port of `CommitResult`.
#[derive(Debug, Clone, PartialEq)]
pub struct CommitResult {
    pub watermark: String,
    /// Progress reported by the last backfill completion in this transaction,
    /// when the change source included a status payload. Port of
    /// `CommitResult.completedBackfill`.
    pub completed_backfill: Option<CompletedBackfill>,
    pub schema_changed: bool,
    pub num_change_log_entries: i64,
}

/// A backfill made visible by a committed transaction. This is intentionally a
/// replica-side fact rather than a scheduler state: the change-source owns
/// starting/restarting work, while consumers can use this result for progress
/// reporting once the schema change is durable.
#[derive(Debug, Clone, PartialEq)]
pub struct CompletedBackfill {
    pub table: String,
    pub columns: Vec<String>,
    pub status: DownloadStatus,
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
    #[error("Invalid backfill for table {table}: {message}")]
    InvalidBackfill { table: String, message: String },
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

fn table_metadata_json(metadata: &TableMetadata) -> JsonValue {
    JsonValue::Object(
        metadata
            .row_key
            .iter()
            .chain(metadata.extra.iter())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}

fn backfill_id_json(backfill: &BackfillId) -> JsonValue {
    JsonValue::Object(
        backfill
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}

/// Orchestrates applying a stream of [`Change`]s to the SQLite replica,
/// maintaining the table-spec cache and per-transaction change-log position
/// counter. Port of `ChangeProcessor` + `TransactionProcessor`.
pub struct ChangeDispatcher<'a> {
    db: &'a StatementRunner,
    row: RowApplier<'a>,
    ddl: DdlApplier<'a>,
    table_specs: HashMap<String, LiteTableSpec>,
    /// Table -> columns whose metadata contains a backfill id. This mirrors
    /// upstream's `LiteTableSpecWithReplicationStatus.backfilling`; it is kept
    /// separately because the public Rust `LiteTableSpec` deliberately models
    /// only the underlying table shape.
    backfilling_columns: HashMap<String, Vec<String>>,
    json_format: LiteJsonFormat,

    // Per-transaction state (reset on `begin`).
    in_transaction: bool,
    version: String,
    pos: i64,
    schema_changed: bool,
    num_change_log_entries: i64,
    completed_backfill: Option<CompletedBackfill>,
}

impl<'a> ChangeDispatcher<'a> {
    pub fn new(db: &'a StatementRunner) -> Result<Self, DbError> {
        let table_specs = Self::load_table_specs(db)?;
        let backfilling_columns = Self::load_backfilling_columns(db, &table_specs)?;
        Ok(ChangeDispatcher {
            db,
            row: RowApplier::new(db),
            ddl: DdlApplier::new(db),
            table_specs,
            backfilling_columns,
            json_format: LiteJsonFormat::Parsed,
            in_transaction: false,
            version: String::new(),
            pos: 0,
            schema_changed: false,
            num_change_log_entries: 0,
            completed_backfill: None,
        })
    }

    fn load_table_specs(db: &StatementRunner) -> Result<HashMap<String, LiteTableSpec>, DbError> {
        Ok(list_tables_including_backfilling(db)?
            .into_iter()
            .map(|t| (t.name.clone(), t))
            .collect())
    }

    fn reload_table_specs(&mut self) -> Result<(), DbError> {
        self.table_specs = Self::load_table_specs(self.db)?;
        self.backfilling_columns = Self::load_backfilling_columns(self.db, &self.table_specs)?;
        Ok(())
    }

    fn load_backfilling_columns(
        db: &StatementRunner,
        table_specs: &HashMap<String, LiteTableSpec>,
    ) -> Result<HashMap<String, Vec<String>>, DbError> {
        let metadata = ColumnMetadataStore::new(db);
        if !metadata.has_table()? {
            return Ok(HashMap::new());
        }

        let mut result = HashMap::new();
        for table_name in table_specs.keys() {
            let columns: Vec<String> = metadata
                .get_table(table_name)?
                .into_iter()
                .filter_map(|(column, metadata)| metadata.is_backfilling.then_some(column))
                .collect();
            if !columns.is_empty() {
                result.insert(table_name.clone(), columns);
            }
        }
        Ok(result)
    }

    fn table_spec(&self, name: &str) -> Result<&LiteTableSpec, DispatchError> {
        self.table_specs
            .get(name)
            .ok_or_else(|| DispatchError::UnknownTable(name.to_string()))
    }

    /// Returns `None` when no table-level backfill is active. Returns
    /// `Some([])` when a backfill is active but the particular row did not set
    /// a backfilling column; this is semantically distinct in the change log.
    fn backfilled_columns_for_row(
        &self,
        table_name: &str,
        row: &[(String, LiteValue)],
    ) -> Option<Vec<String>> {
        let backfilling = self.backfilling_columns.get(table_name)?;
        Some(
            backfilling
                .iter()
                .filter(|column| row.iter().any(|(name, _)| name == *column))
                .cloned()
                .collect(),
        )
    }

    /// Whether every upstream (non-version) column is being backfilled. A
    /// table created in that state, and indexes created for it, must stay
    /// hidden until completion instead of emitting a reset-op.
    fn table_is_fully_backfilling(&self, table_name: &str) -> Result<bool, DispatchError> {
        let source_columns = self
            .table_spec(table_name)?
            .columns
            .iter()
            .filter(|(name, _)| name != ZERO_VERSION_COLUMN_NAME)
            .count();
        Ok(self.backfilling_columns.get(table_name).map_or(0, Vec::len) == source_columns)
    }

    /// Applies one snapshot batch from a change-source backfill. Backfill rows
    /// are deliberately not written to the change log: the table/column is
    /// still hidden, and `backfill-completed` will reset consumers once every
    /// snapshot row has been merged.
    ///
    /// The merge is guarded by the latest live row operation. A delete newer
    /// than the snapshot must not be resurrected, and a live write to a
    /// backfilling column newer than the snapshot must win over its old
    /// snapshot value. This ports `TransactionProcessor.processBackfill`.
    fn process_backfill(
        &mut self,
        relation: &Relation,
        columns: &[String],
        watermark: &str,
        row_values: &[Vec<JsonValue>],
    ) -> Result<(), DispatchError> {
        let table_name = lite_table_name(&relation.schema, &relation.name);
        let spec = self.table_spec(&table_name)?.clone();

        if relation.row_key.columns.is_empty() {
            return Err(DispatchError::InvalidBackfill {
                table: table_name,
                message: "backfill requires relation row-key columns".into(),
            });
        }

        let mut cols = relation.row_key.columns.clone();
        cols.extend(columns.iter().cloned());
        let mut seen = HashSet::new();
        for column in &cols {
            if !seen.insert(column) {
                return Err(DispatchError::InvalidBackfill {
                    table: table_name,
                    message: format!("column {column:?} appears more than once"),
                });
            }
            if spec.column(column).is_none() {
                return Err(DispatchError::InvalidBackfill {
                    table: table_name,
                    message: format!("unknown column {column:?}"),
                });
            }
        }

        let version_column = ZERO_VERSION_COLUMN_NAME.to_string();
        let insert_columns = cols
            .iter()
            .chain(std::iter::once(&version_column))
            .map(|column| id(column))
            .collect::<Vec<_>>()
            .join(",");
        let placeholders = std::iter::repeat_n("?", cols.len() + 1)
            .collect::<Vec<_>>()
            .join(",");
        let conflict_columns = relation
            .row_key
            .columns
            .iter()
            .map(|column| id(column))
            .collect::<Vec<_>>()
            .join(",");

        for values in row_values {
            if values.len() != cols.len() {
                return Err(DispatchError::InvalidBackfill {
                    table: table_name.clone(),
                    message: format!(
                        "row has {} values but {} key/backfill columns were declared",
                        values.len(),
                        cols.len()
                    ),
                });
            }

            let input: Vec<(String, JsonValue)> = cols
                .iter()
                .zip(values)
                .map(|(column, value)| (column.clone(), value.clone()))
                .collect();
            let converted = lite_row(&json_row_to_pg(&input), &spec, self.json_format);
            let row_key = crate::row_apply::get_key(
                &converted.row,
                converted.num_cols,
                row_key_kind(relation.row_key.kind),
                &relation.row_key.columns,
                &table_name,
                spec.primary_key.as_deref(),
            )
            .map_err(ApplyError::from)?;
            let latest = self
                .ddl
                .change_log
                .get_latest_row_op(&table_name, &to_json_row_key(&row_key))?;

            if latest.as_ref().is_some_and(|row_op| {
                row_op.op == DEL_OP && row_op.state_version.as_str() > watermark
            }) {
                // The row was deleted after this snapshot was taken.
                continue;
            }

            let updates: Vec<&String> = match latest.as_ref() {
                Some(row_op) if row_op.op == SET_OP => cols
                    .iter()
                    .filter(|column| {
                        row_op
                            .backfilling_column_versions
                            .get(*column)
                            .map_or("", String::as_str)
                            <= watermark
                    })
                    .collect(),
                _ => cols.iter().collect(),
            };
            if updates.is_empty() {
                // Every backfilling value already has a newer live write.
                continue;
            }

            let update_columns = updates
                .iter()
                .map(|column| {
                    let column = id(column);
                    format!("{column}=excluded.{column}")
                })
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "INSERT INTO {} ({insert_columns}) VALUES ({placeholders}) ON CONFLICT ({conflict_columns}) DO UPDATE SET {update_columns}",
                id(&table_name),
            );
            let mut bindings: Vec<Value> = converted
                .row
                .iter()
                .map(|(_, value)| to_sql_value(value))
                .collect();
            bindings.push(Value::Text(watermark.to_string()));
            self.db.run(&sql, &bindings)?;
        }
        Ok(())
    }

    /// Clears the backfill markers and emits the reset that makes the newly
    /// populated columns visible. Port of `processBackfillCompleted`.
    fn process_backfill_completed(
        &mut self,
        relation: &Relation,
        columns: &[String],
        status: Option<&DownloadStatus>,
    ) -> Result<(), DispatchError> {
        let table_name = lite_table_name(&relation.schema, &relation.name);
        let completed_columns: Vec<String> = relation
            .row_key
            .columns
            .iter()
            .chain(columns.iter())
            .cloned()
            .collect();

        for column in &completed_columns {
            self.ddl
                .column_metadata
                .clear_backfilling(&table_name, column)?;
        }
        self.ddl
            .bump_versions(&table_name, &relation.schema, &relation.name, &self.version)?;
        self.reload_table_specs()?;
        self.schema_changed = true;
        self.num_change_log_entries += 1;
        if let Some(status) = status {
            self.completed_backfill = Some(CompletedBackfill {
                table: table_name,
                columns: completed_columns,
                status: status.clone(),
            });
        }
        Ok(())
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
        self.completed_backfill = None;
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
                let backfilled = self.backfilled_columns_for_row(&table_name, &row);
                row.push((
                    ZERO_VERSION_COLUMN_NAME.to_string(),
                    LiteValue::Text(self.version.clone()),
                ));
                let pos_before = self.pos;
                self.row.process_insert_with_backfill(
                    &table_name,
                    &row,
                    row_key_kind(relation.row_key.kind),
                    &relation.row_key.columns,
                    spec.primary_key.as_deref(),
                    &self.version,
                    &mut self.pos,
                    backfilled.as_deref(),
                )?;
                self.num_change_log_entries += self.pos - pos_before;
                Ok(())
            }
            Change::Delete { relation, key } => {
                let table_name = lite_table_name(&relation.schema, &relation.name);
                let spec = self.table_spec(&table_name)?.clone();
                let pg_key = json_row_to_pg(key);
                let converted = lite_row(&pg_key, &spec, self.json_format);
                let pos_before = self.pos;
                self.row.process_delete(
                    &table_name,
                    &converted.row,
                    &self.version,
                    &mut self.pos,
                )?;
                self.num_change_log_entries += self.pos - pos_before;
                Ok(())
            }
            Change::Truncate { relations } => {
                for relation in relations {
                    let table_name = lite_table_name(&relation.schema, &relation.name);
                    self.db
                        .run(&format!("DELETE FROM {}", id(&table_name)), &[])?;
                    self.ddl
                        .change_log
                        .log_truncate_op(&self.version, &table_name)?;
                    self.num_change_log_entries += 1;
                }
                Ok(())
            }
            Change::CreateTable(create) => {
                if let Some(metadata) = &create.metadata {
                    self.ddl.table_metadata.set_upstream_metadata(
                        &create.spec.schema,
                        &create.spec.name,
                        &table_metadata_json(metadata),
                    )?;
                }
                let lite_spec =
                    zero_cache_types::pg_to_lite::map_postgres_to_lite(&create.spec, None)
                        .map_err(DdlError::from)?;
                let all_columns_backfilled = create
                    .backfill
                    .as_ref()
                    .map_or(0, |backfill| backfill.len())
                    == create.spec.columns.len();
                self.ddl.create_table_with_visibility(
                    &lite_spec,
                    &self.version,
                    !all_columns_backfilled,
                )?;
                for (column_name, column_spec) in &create.spec.columns {
                    let backfill = create
                        .backfill
                        .as_ref()
                        .and_then(|backfills| backfills.get(column_name))
                        .map(backfill_id_json);
                    self.ddl.column_metadata.insert(
                        &lite_spec.name,
                        column_name,
                        column_spec,
                        backfill.as_ref(),
                    )?;
                }
                self.reload_table_specs()?;
                if !all_columns_backfilled {
                    self.num_change_log_entries += 1;
                    self.schema_changed = true;
                }
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
            Change::AddColumn {
                table,
                column,
                table_metadata,
                backfill,
            } => {
                if let Some(metadata) = table_metadata {
                    self.ddl.table_metadata.set_upstream_metadata(
                        &table.schema,
                        &table.name,
                        &table_metadata_json(metadata),
                    )?;
                }
                let lite_name = lite_table_name(&table.schema, &table.name);
                let backfill_json = backfill.as_ref().map(backfill_id_json);
                let make_visible = backfill.is_none();
                self.ddl.add_column_with_visibility(
                    &lite_name,
                    &column.name,
                    &column.spec,
                    &table.schema,
                    &table.name,
                    &self.version,
                    backfill_json.as_ref(),
                    make_visible,
                )?;
                self.reload_table_specs()?;
                if make_visible {
                    self.num_change_log_entries += 1;
                    self.schema_changed = true;
                }
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
                let table_name = lite_table_name(&spec.schema, &spec.table_name);
                let table_is_hidden = self.table_is_fully_backfilling(&table_name)?;
                self.ddl
                    .create_index_with_visibility(spec, &self.version, !table_is_hidden)?;
                self.reload_table_specs()?;
                if !table_is_hidden {
                    self.num_change_log_entries += 1;
                    self.schema_changed = true;
                }
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
                let backfilled = self.backfilled_columns_for_row(&table_name, &converted_new.row);
                let mut row = converted_new.row.clone();
                row.push((
                    ZERO_VERSION_COLUMN_NAME.to_string(),
                    LiteValue::Text(self.version.clone()),
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

                let pos_before = self.pos;
                self.row.process_update_with_backfill(
                    &table_name,
                    &row,
                    old_key.as_deref(),
                    &new_key,
                    &self.version,
                    &mut self.pos,
                    backfilled.as_deref(),
                )?;
                self.num_change_log_entries += self.pos - pos_before;
                Ok(())
            }
            Change::Backfill {
                relation,
                columns,
                watermark,
                row_values,
                ..
            } => self.process_backfill(relation, columns, watermark, row_values),
            Change::BackfillCompleted {
                relation,
                columns,
                status,
                ..
            } => self.process_backfill_completed(relation, columns, status.as_ref()),
            Change::UpdateTableMetadata { table, new, .. } => {
                self.ddl.table_metadata.set_upstream_metadata(
                    &table.schema,
                    &table.name,
                    &table_metadata_json(new),
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
            completed_backfill: self.completed_backfill.take(),
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
        self.completed_backfill = None;
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
    use crate::column_metadata::{ColumnMetadataStore, CREATE_COLUMN_METADATA_TABLE};
    use crate::table_metadata::CREATE_TABLE_METADATA_TABLE;
    use zero_cache_change_source::data::{ColumnDef, DownloadStatus, Relation, RowKey};
    use zero_cache_types::specs::{ColumnSpec, Direction, IndexSpec, TableSpec};

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

    /// A realistic online-schema-evolution sequence from upstream's
    /// `change-processor` backfill tests:
    ///
    /// 1. add a column with a backfill id (the schema must remain hidden);
    /// 2. process live writes/deletes after the backfill snapshot;
    /// 3. merge old snapshot values without clobbering those newer changes;
    /// 4. expose the column only on `backfill-completed`.
    #[test]
    fn add_column_backfill_defers_visibility_merges_snapshot_and_completes() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        let issues = relation("issues", &["id"]);

        // Create the source table and the unique row-key index that a
        // backfill's `ON CONFLICT` merge requires.
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
            .apply(&Change::CreateIndex {
                spec: IndexSpec {
                    name: "issues_pkey".into(),
                    table_name: "issues".into(),
                    schema: "public".into(),
                    unique: true,
                    columns: vec![("id".into(), Direction::Asc)],
                },
            })
            .unwrap();
        dispatcher.commit("01").unwrap();

        dispatcher.begin("02").unwrap();
        for (id, title) in [("a", "alpha"), ("b", "beta"), ("c", "gamma")] {
            dispatcher
                .apply(&Change::Insert {
                    relation: issues.clone(),
                    new: vec![
                        ("id".into(), JsonValue::String(id.into())),
                        ("title".into(), JsonValue::String(title.into())),
                    ],
                })
                .unwrap();
        }
        dispatcher.commit("02").unwrap();

        // The untranslatable/defaulted column arrives with a backfill id. Its
        // SQLite column and metadata appear immediately, but no reset-op or
        // min-row-version bump exposes it to consumers yet.
        dispatcher.begin("03").unwrap();
        dispatcher
            .apply(&Change::AddColumn {
                table: zero_cache_change_source::data::Identifier {
                    schema: "public".into(),
                    name: "issues".into(),
                },
                column: ColumnDef {
                    name: "score".into(),
                    spec: ColumnSpec::new("int4", 3),
                },
                table_metadata: None,
                backfill: Some(std::collections::BTreeMap::from([(
                    "attNum".into(),
                    JsonValue::Number(3.0),
                )])),
            })
            .unwrap();
        let added = dispatcher.commit("03").unwrap();
        assert!(!added.schema_changed);
        assert_eq!(added.num_change_log_entries, 0);
        assert!(
            ColumnMetadataStore::new(&db)
                .get_column("issues", "score")
                .unwrap()
                .unwrap()
                .is_backfilling
        );
        assert!(db
            .query_uncached(
                r#"SELECT 1 FROM "_zero.changeLog2" WHERE stateVersion = '03' AND op = 'r'"#,
                &[],
            )
            .unwrap()
            .is_empty());

        // These changes happened after the snapshot at watermark 03. The
        // update's backfilling-column version must protect `a.score`; the
        // delete must prevent `c` from being resurrected.
        dispatcher.begin("04").unwrap();
        dispatcher
            .apply(&Change::Update {
                relation: issues.clone(),
                key: None,
                new: vec![
                    ("id".into(), JsonValue::String("a".into())),
                    ("title".into(), JsonValue::String("alpha-live".into())),
                    ("score".into(), JsonValue::Number(99.0)),
                ],
            })
            .unwrap();
        dispatcher
            .apply(&Change::Delete {
                relation: issues.clone(),
                key: vec![("id".into(), JsonValue::String("c".into()))],
            })
            .unwrap();
        dispatcher.commit("04").unwrap();
        let live_a = crate::change_log::ChangeLog::new(&db)
            .get_latest_row_op(
                "issues",
                &vec![("id".into(), JsonValue::String("a".into()))],
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            live_a.backfilling_column_versions.get("score"),
            Some(&"04".to_string()),
            "the live value must be marked newer than the snapshot"
        );

        dispatcher.begin("05").unwrap();
        dispatcher
            .apply(&Change::Backfill {
                relation: issues.clone(),
                columns: vec!["score".into()],
                watermark: "03".into(),
                row_values: vec![
                    vec![JsonValue::String("a".into()), JsonValue::Number(1.0)],
                    vec![JsonValue::String("b".into()), JsonValue::Number(2.0)],
                    vec![JsonValue::String("c".into()), JsonValue::Number(3.0)],
                ],
                status: None,
            })
            .unwrap();
        let merged = dispatcher.commit("05").unwrap();
        assert!(!merged.schema_changed);
        assert_eq!(merged.num_change_log_entries, 0);

        let rows = db
            .query_uncached("SELECT id, title, score FROM issues ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2, "post-snapshot delete stays deleted");
        assert_eq!(rows[0][0].1, crate::Value::Text("a".into()));
        assert_eq!(rows[0][1].1, crate::Value::Text("alpha-live".into()));
        assert_eq!(rows[0][2].1, crate::Value::Integer(99));
        assert_eq!(rows[1][0].1, crate::Value::Text("b".into()));
        assert_eq!(rows[1][2].1, crate::Value::Integer(2));

        // Completion clears the marker, bumps the min row version, and emits
        // the one reset-op that makes the new column visible to clients.
        dispatcher.begin("06").unwrap();
        dispatcher
            .apply(&Change::BackfillCompleted {
                relation: issues,
                columns: vec!["score".into()],
                watermark: "03".into(),
                status: Some(DownloadStatus {
                    rows: 3.0,
                    total_rows: 3.0,
                    total_bytes: Some(42.0),
                }),
            })
            .unwrap();
        let completed = dispatcher.commit("06").unwrap();
        assert!(completed.schema_changed);
        assert_eq!(completed.num_change_log_entries, 1);
        assert_eq!(
            completed.completed_backfill,
            Some(CompletedBackfill {
                table: "issues".into(),
                columns: vec!["id".into(), "score".into()],
                status: DownloadStatus {
                    rows: 3.0,
                    total_rows: 3.0,
                    total_bytes: Some(42.0),
                },
            })
        );
        assert!(
            !ColumnMetadataStore::new(&db)
                .get_column("issues", "score")
                .unwrap()
                .unwrap()
                .is_backfilling
        );
        assert_eq!(
            dispatcher
                .ddl
                .table_metadata
                .get_min_row_versions()
                .unwrap()
                .get("issues"),
            Some(&"06".to_string())
        );
        assert_eq!(
            db.query_uncached(
                r#"SELECT op FROM "_zero.changeLog2"
                   WHERE stateVersion = '06' AND "table" = 'issues'"#,
                &[],
            )
            .unwrap()[0][0]
                .1,
            crate::Value::Text("r".into())
        );
    }

    #[test]
    fn schema_changes_keep_column_metadata_in_sync() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        let metadata = ColumnMetadataStore::new(&db);

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
        dispatcher.commit("01").unwrap();
        assert_eq!(
            metadata
                .get_table("issues")
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["id".to_string(), "title".to_string()]
        );

        dispatcher.begin("02").unwrap();
        dispatcher
            .apply(&Change::RenameTable {
                old: zero_cache_change_source::data::Identifier {
                    schema: "public".into(),
                    name: "issues".into(),
                },
                new: zero_cache_change_source::data::Identifier {
                    schema: "public".into(),
                    name: "tickets".into(),
                },
            })
            .unwrap();
        dispatcher.commit("02").unwrap();
        assert!(metadata.get_table("issues").unwrap().is_empty());
        assert_eq!(metadata.get_table("tickets").unwrap().len(), 2);

        dispatcher.begin("03").unwrap();
        dispatcher
            .apply(&Change::DropColumn {
                table: zero_cache_change_source::data::Identifier {
                    schema: "public".into(),
                    name: "tickets".into(),
                },
                column: "title".into(),
            })
            .unwrap();
        dispatcher.commit("03").unwrap();
        assert!(metadata.get_column("tickets", "title").unwrap().is_none());
        assert!(metadata.get_column("tickets", "id").unwrap().is_some());

        dispatcher.begin("04").unwrap();
        dispatcher
            .apply(&Change::DropTable {
                id: zero_cache_change_source::data::Identifier {
                    schema: "public".into(),
                    name: "tickets".into(),
                },
            })
            .unwrap();
        dispatcher.commit("04").unwrap();
        assert!(metadata.get_table("tickets").unwrap().is_empty());
    }

    #[test]
    fn fully_backfilled_create_table_and_index_wait_for_completion_reset() {
        let db = setup();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        let issues = relation("issues", &["id"]);
        let backfill_id = |att_num| {
            std::collections::BTreeMap::from([(
                "attNum".to_string(),
                JsonValue::Number(att_num as f64),
            )])
        };

        dispatcher.begin("01").unwrap();
        dispatcher
            .apply(&Change::CreateTable(
                zero_cache_change_source::data::TableCreate {
                    spec: issues_two_col_spec(),
                    metadata: None,
                    backfill: Some(std::collections::BTreeMap::from([
                        ("id".into(), backfill_id(1)),
                        ("title".into(), backfill_id(2)),
                    ])),
                },
            ))
            .unwrap();
        dispatcher
            .apply(&Change::CreateIndex {
                spec: IndexSpec {
                    name: "issues_pkey".into(),
                    table_name: "issues".into(),
                    schema: "public".into(),
                    unique: true,
                    columns: vec![("id".into(), Direction::Asc)],
                },
            })
            .unwrap();
        let created = dispatcher.commit("01").unwrap();
        assert!(!created.schema_changed);
        assert_eq!(created.num_change_log_entries, 0);

        dispatcher.begin("02").unwrap();
        dispatcher
            .apply(&Change::Backfill {
                relation: issues.clone(),
                columns: vec!["title".into()],
                watermark: "01".into(),
                row_values: vec![vec![
                    JsonValue::String("a".into()),
                    JsonValue::String("copied".into()),
                ]],
                status: None,
            })
            .unwrap();
        dispatcher.commit("02").unwrap();

        dispatcher.begin("03").unwrap();
        dispatcher
            .apply(&Change::BackfillCompleted {
                relation: issues,
                columns: vec!["title".into()],
                watermark: "01".into(),
                status: None,
            })
            .unwrap();
        let completed = dispatcher.commit("03").unwrap();
        assert!(completed.schema_changed);
        assert_eq!(completed.num_change_log_entries, 1);
        assert_eq!(
            db.query_uncached("SELECT id, title FROM issues", &[])
                .unwrap(),
            vec![vec![
                ("id".into(), crate::Value::Text("a".into())),
                ("title".into(), crate::Value::Text("copied".into())),
            ]]
        );
    }
}
