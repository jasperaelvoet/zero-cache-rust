//! Leapfrogging concurrent snapshots used by the Zero v1.7 view-syncer.
//!
//! The replicator is the only committed writer. A client-group view-syncer
//! owns two `BEGIN CONCURRENT` connections: one holds its previous version and
//! the other advances to replica head. Changes are read from the head snapshot,
//! resolved against both endpoints, and may be simulated on `prev` for IVM
//! before that transaction is rolled back and reused as the next head.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use zero_cache_shared::bigint_json::{parse, JsonValue};
use zero_cache_types::sql::id;

use crate::change_log::{ChangeLog, ChangeLogRow, RESET_OP, SET_OP, TRUNCATE_OP};
use crate::replication_state::get_replication_state;
use crate::{DbError, Row, StatementRunner, Value};

/// Builds the snapshotter's table map from the live replica metadata.
pub fn snapshot_table_specs(
    db: &StatementRunner,
) -> Result<(BTreeMap<String, SnapshotTableSpec>, BTreeSet<String>), DbError> {
    let tables = crate::lite_tables::list_tables(db)?;
    let indexes = crate::lite_tables::list_indexes(db)?;
    let minimums = crate::table_metadata::TableMetadataTracker::new(db)
        .get_min_row_versions()
        .unwrap_or_default();
    let all = tables.iter().map(|table| table.name.clone()).collect();
    let specs = tables
        .into_iter()
        .filter_map(|table| {
            let primary_key = table.primary_key?;
            let unique_keys = indexes
                .iter()
                .filter(|index| index.table_name == table.name && index.unique)
                .map(|index| {
                    index
                        .columns
                        .iter()
                        .map(|(column, _)| column.clone())
                        .collect()
                })
                .collect();
            // Restore each column's declared ZQL value type (`fromSQLiteTypes`
            // upstream): Postgres booleans are stored as SQLite 0/1 and JSON as
            // text, but must reach clients as JSON true/false / parsed objects.
            // The incremental (poke) path resolves rows straight from these
            // specs, so the type map has to travel with the spec — otherwise a
            // boolean update would ship `1` instead of `true`.
            let column_types = table
                .columns
                .iter()
                .map(|(name, spec)| {
                    let value_type =
                        zero_cache_types::lite::lite_type_to_zql_value_type(&spec.data_type)
                            .unwrap_or(zero_cache_types::pg_data_type::ValueType::String);
                    (name.clone(), value_type)
                })
                .collect();
            Some((
                table.name.clone(),
                SnapshotTableSpec {
                    name: table.name.clone(),
                    columns: table.columns.into_iter().map(|(name, _)| name).collect(),
                    column_types,
                    primary_key,
                    unique_keys,
                    min_row_version: minimums.get(&table.name).cloned(),
                },
            ))
        })
        .collect();
    Ok((specs, all))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResetPipelinesReason {
    AdvancementTimeout,
    ScalarSubquery,
    SchemaChange,
    Truncation,
    PermissionsChange,
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error("snapshotter has already been initialized")]
    AlreadyInitialized,
    #[error("snapshotter has not been initialized")]
    NotInitialized,
    #[error("reset pipelines ({reason:?}): {message}")]
    Reset {
        reason: ResetPipelinesReason,
        message: String,
    },
    #[error("malformed row key {0}")]
    MalformedRowKey(String),
    #[error("change for unknown table {0}")]
    UnknownTable(String),
    #[error("missing value for {table} {row_key}")]
    MissingValue { table: String, row_key: String },
    #[error("snapshot diff is no longer valid: {0}")]
    InvalidDiff(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotTableSpec {
    pub name: String,
    pub columns: Vec<String>,
    /// Declared ZQL value type per column, restored from the SQLite replica's
    /// column types. Consumers that turn raw SQLite storage values into wire
    /// rows (notably the incremental poke path) use this to emit booleans as
    /// `true`/`false` and JSON columns as parsed values rather than raw `0`/`1`
    /// / text. Missing entries default to `String`.
    pub column_types: BTreeMap<String, zero_cache_types::pg_data_type::ValueType>,
    pub primary_key: Vec<String>,
    pub unique_keys: Vec<Vec<String>>,
    pub min_row_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotChange {
    pub table: String,
    pub prev_values: Vec<Row>,
    pub next_value: Option<Row>,
    pub row_key: Vec<(String, JsonValue)>,
}

pub struct Snapshot {
    db: StatementRunner,
    app_id: String,
    pub version: String,
}

impl Snapshot {
    fn create(
        db_file: &str,
        app_id: &str,
        page_cache_size_kib: Option<usize>,
    ) -> Result<Self, SnapshotError> {
        let db = StatementRunner::open_snapshot(db_file, page_cache_size_kib)?;
        let version = get_replication_state(&db)?.state_version;
        Ok(Self {
            db,
            app_id: app_id.to_string(),
            version,
        })
    }

    fn reset_to_head(mut self) -> Result<Self, SnapshotError> {
        self.db.rollback()?;
        self.db.begin_concurrent()?;
        self.version = get_replication_state(&self.db)?.state_version;
        Ok(self)
    }

    pub fn db(&self) -> &StatementRunner {
        &self.db
    }

    pub fn db_mut(&mut self) -> &mut StatementRunner {
        &mut self.db
    }

    pub fn app_id(&self) -> &str {
        &self.app_id
    }
}

pub struct Snapshotter {
    db_file: String,
    app_id: String,
    page_cache_size_kib: Option<usize>,
    current: Option<Snapshot>,
    previous: Option<Snapshot>,
    /// Number of times [`with_current_shared`](Self::with_current_shared) had to
    /// reopen a fresh snapshot because a shared replica handle outlived the
    /// closure (a leaked operator-graph cycle). Should stay 0 — a nonzero value
    /// is a real perf regression (a full snapshot reopen per hydration).
    reopens_from_leak: usize,
}

impl Snapshotter {
    pub fn new(
        db_file: impl Into<String>,
        app_id: impl Into<String>,
        page_cache_size_kib: Option<usize>,
    ) -> Self {
        Self {
            db_file: db_file.into(),
            app_id: app_id.into(),
            page_cache_size_kib,
            current: None,
            previous: None,
            reopens_from_leak: 0,
        }
    }

    /// Test/diagnostic accessor: how many times a leaked shared replica handle
    /// forced a fresh-snapshot reopen. Expected to be 0.
    pub fn reopens_from_leak(&self) -> usize {
        self.reopens_from_leak
    }

    pub fn init(&mut self) -> Result<&Snapshot, SnapshotError> {
        if self.current.is_some() {
            return Err(SnapshotError::AlreadyInitialized);
        }
        self.current = Some(Snapshot::create(
            &self.db_file,
            &self.app_id,
            self.page_cache_size_kib,
        )?);
        Ok(self.current.as_ref().expect("just initialized"))
    }

    pub fn current(&self) -> Result<&Snapshot, SnapshotError> {
        self.current.as_ref().ok_or(SnapshotError::NotInitialized)
    }

    /// Lends the `current` snapshot's connection to `f` as a shared handle, then
    /// reclaims it. This is how the view-syncer's transient hydration graph reads
    /// the replica WITHOUT opening a connection per source: every source is built
    /// over this one shared handle (mirroring upstream `pipeline-driver`'s
    /// `#getSource`, which reads `snapshotter.current().db`), so a client-group
    /// pipeline holds two wal2 connections total (`current` + `previous`)
    /// regardless of query/table count.
    ///
    /// The `Rc` is strictly scoped — never stored on a field — so `Snapshotter`
    /// (and the `PipelineDriver` that owns it) stay `Send` between calls. `f` MUST
    /// drop every clone of the handle before returning; if one leaks, the
    /// connection cannot be reclaimed, so rather than leave `current` empty
    /// (which would poison the owning group), a fresh snapshot is reopened at
    /// head and the leak is logged.
    pub fn with_current_shared<R>(
        &mut self,
        f: impl FnOnce(&Rc<RefCell<StatementRunner>>) -> R,
    ) -> Result<R, SnapshotError> {
        let Snapshot {
            db,
            app_id,
            version,
        } = self.current.take().ok_or(SnapshotError::NotInitialized)?;
        let shared = Rc::new(RefCell::new(db));
        let out = f(&shared);
        match Rc::into_inner(shared) {
            Some(cell) => {
                self.current = Some(Snapshot {
                    db: cell.into_inner(),
                    app_id,
                    version,
                });
            }
            None => {
                self.reopens_from_leak += 1;
                eprintln!(
                    "snapshotter: shared replica handle outlived hydration; \
                     reopening a fresh snapshot at head"
                );
                self.current = Some(Snapshot::create(
                    &self.db_file,
                    &self.app_id,
                    self.page_cache_size_kib,
                )?);
            }
        }
        Ok(out)
    }

    pub fn advance_without_diff(&mut self) -> Result<(&mut Snapshot, &Snapshot), SnapshotError> {
        let current = self.current.take().ok_or(SnapshotError::NotInitialized)?;
        let next = match self.previous.take() {
            Some(previous) => previous.reset_to_head()?,
            None => Snapshot::create(&self.db_file, &self.app_id, self.page_cache_size_kib)?,
        };
        self.previous = Some(current);
        self.current = Some(next);
        Ok((
            self.previous.as_mut().expect("previous assigned"),
            self.current.as_ref().expect("current assigned"),
        ))
    }

    pub fn advance(
        &mut self,
        syncable_tables: &BTreeMap<String, SnapshotTableSpec>,
        all_table_names: &BTreeSet<String>,
    ) -> Result<SnapshotDiff, SnapshotError> {
        let (prev, curr) = self.advance_without_diff()?;
        materialize_diff(prev, curr, syncable_tables, all_table_names)
    }

    /// Rolls back both ephemeral snapshots. Connections are then closed by
    /// normal Rust drop semantics.
    pub fn destroy(mut self) -> Result<(), SnapshotError> {
        if let Some(snapshot) = self.current.take() {
            snapshot.db.rollback()?;
        }
        if let Some(snapshot) = self.previous.take() {
            snapshot.db.rollback()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotDiff {
    pub prev_version: String,
    pub curr_version: String,
    pub changes: usize,
    pub rows: Vec<SnapshotChange>,
}

fn materialize_diff(
    prev: &mut Snapshot,
    curr: &Snapshot,
    syncable_tables: &BTreeMap<String, SnapshotTableSpec>,
    all_table_names: &BTreeSet<String>,
) -> Result<SnapshotDiff, SnapshotError> {
    let entries = ChangeLog::new(curr.db()).read_since(&prev.version)?;
    let mut rows = Vec::with_capacity(entries.len());
    for entry in &entries {
        if let Some(change) = resolve_change(prev, curr, entry, syncable_tables, all_table_names)? {
            rows.push(change);
        }
    }
    Ok(SnapshotDiff {
        prev_version: prev.version.clone(),
        curr_version: curr.version.clone(),
        changes: entries.len(),
        rows,
    })
}

fn resolve_change(
    prev: &Snapshot,
    curr: &Snapshot,
    entry: &ChangeLogRow,
    syncable_tables: &BTreeMap<String, SnapshotTableSpec>,
    all_table_names: &BTreeSet<String>,
) -> Result<Option<SnapshotChange>, SnapshotError> {
    match entry.op.as_str() {
        RESET_OP => {
            return Err(SnapshotError::Reset {
                reason: ResetPipelinesReason::SchemaChange,
                message: format!("schema for table {} has changed", entry.table),
            })
        }
        TRUNCATE_OP => {
            return Err(SnapshotError::Reset {
                reason: ResetPipelinesReason::Truncation,
                message: format!("table {} has been truncated", entry.table),
            })
        }
        _ => {}
    }

    let Some(spec) = syncable_tables.get(&entry.table) else {
        if all_table_names.contains(&entry.table) {
            return Ok(None);
        }
        return Err(SnapshotError::UnknownTable(entry.table.clone()));
    };
    if spec
        .min_row_version
        .as_deref()
        .is_some_and(|minimum| minimum >= entry.state_version.as_str())
    {
        return Err(SnapshotError::InvalidDiff(format!(
            "change @{} is not newer than {} minRowVersion",
            entry.state_version,
            spec.min_row_version.as_deref().unwrap_or_default()
        )));
    }

    let row_key = parse_row_key(&entry.row_key)?;
    let next_value = if entry.op == SET_OP {
        Some(
            get_row(curr.db(), spec, &row_key)?.ok_or_else(|| SnapshotError::MissingValue {
                table: entry.table.clone(),
                row_key: entry.row_key.clone(),
            })?,
        )
    } else {
        None
    };
    let prev_values = match &next_value {
        Some(next) => get_unique_conflicts(prev.db(), spec, next)?,
        None => get_row(prev.db(), spec, &row_key)?.into_iter().collect(),
    };
    if prev_values.is_empty() && next_value.is_none() {
        return Ok(None);
    }

    if entry.table == format!("{}.permissions", prev.app_id())
        && permissions_changed(&prev_values, next_value.as_ref())
    {
        return Err(SnapshotError::Reset {
            reason: ResetPipelinesReason::PermissionsChange,
            message: "permissions have changed".to_string(),
        });
    }

    Ok(Some(SnapshotChange {
        table: entry.table.clone(),
        prev_values,
        next_value,
        row_key,
    }))
}

fn parse_row_key(text: &str) -> Result<Vec<(String, JsonValue)>, SnapshotError> {
    match parse(text).map_err(|_| SnapshotError::MalformedRowKey(text.to_string()))? {
        JsonValue::Object(values) => Ok(values),
        _ => Err(SnapshotError::MalformedRowKey(text.to_string())),
    }
}

fn get_row(
    db: &StatementRunner,
    spec: &SnapshotTableSpec,
    key: &[(String, JsonValue)],
) -> Result<Option<Row>, SnapshotError> {
    let conditions = key
        .iter()
        .map(|(column, _)| format!("{} = ?", id(column)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let columns = spec
        .columns
        .iter()
        .map(|column| id(column))
        .collect::<Vec<_>>();
    let params = key
        .iter()
        .map(|(_, value)| json_to_sql(value))
        .collect::<Vec<_>>();
    Ok(db.get(
        &format!(
            "SELECT {} FROM {} WHERE {conditions}",
            columns.join(","),
            id(&spec.name)
        ),
        &params,
    )?)
}

fn get_unique_conflicts(
    db: &StatementRunner,
    spec: &SnapshotTableSpec,
    row: &Row,
) -> Result<Vec<Row>, SnapshotError> {
    let values: BTreeMap<_, _> = row.iter().cloned().collect();
    let mut keys = vec![spec.primary_key.clone()];
    keys.extend(spec.unique_keys.clone());
    keys.sort();
    keys.dedup();
    let valid: Vec<_> = keys
        .into_iter()
        .filter(|key| {
            key.iter().all(|column| {
                values
                    .get(column)
                    .is_some_and(|value| !matches!(value, Value::Null))
            })
        })
        .collect();
    if valid.is_empty() {
        return Ok(Vec::new());
    }
    let where_clause = valid
        .iter()
        .map(|key| {
            format!(
                "({})",
                key.iter()
                    .map(|column| format!("{} = ?", id(column)))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            )
        })
        .collect::<Vec<_>>()
        .join(" OR ");
    let params = valid
        .iter()
        .flat_map(|key| key.iter().map(|column| values[column].clone()))
        .collect::<Vec<_>>();
    let columns = spec
        .columns
        .iter()
        .map(|column| id(column))
        .collect::<Vec<_>>();
    Ok(db.all(
        &format!(
            "SELECT {} FROM {} WHERE {where_clause}",
            columns.join(","),
            id(&spec.name)
        ),
        &params,
    )?)
}

fn json_to_sql(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Integer(i64::from(*value)),
        JsonValue::Number(value) if value.fract() == 0.0 => Value::Integer(*value as i64),
        JsonValue::Number(value) => Value::Real(*value),
        JsonValue::String(value) => Value::Text(value.clone()),
        JsonValue::BigInt(value) => Value::Text(value.to_string()),
        other => Value::Text(other.stringify()),
    }
}

fn permissions_changed(previous: &[Row], next: Option<&Row>) -> bool {
    let next_permissions = next.and_then(|row| row.iter().find(|(k, _)| k == "permissions"));
    previous
        .iter()
        .any(|row| row.iter().find(|(key, _)| key == "permissions") != next_permissions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_log::CREATE_CHANGELOG_SCHEMA;
    use crate::replication_state::{init_replication_state, update_replication_watermark};

    fn temp_db() -> String {
        std::env::temp_dir()
            .join(format!(
                "zero-snapshotter-{}-{}.db",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .to_string_lossy()
            .into_owned()
    }

    fn setup(path: &str) -> StatementRunner {
        let db = StatementRunner::open_file(path).unwrap();
        init_replication_state(&db, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(
            "CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, _0_version TEXT NOT NULL)",
        )
        .unwrap();
        db
    }

    fn specs() -> BTreeMap<String, SnapshotTableSpec> {
        BTreeMap::from([(
            "issue".to_string(),
            SnapshotTableSpec {
                name: "issue".to_string(),
                columns: vec!["id".into(), "title".into(), "_0_version".into()],
                column_types: BTreeMap::new(),
                primary_key: vec!["id".into()],
                unique_keys: vec![],
                min_row_version: Some("00".into()),
            },
        )])
    }

    #[test]
    fn snapshots_leapfrog_and_resolve_incremental_rows() {
        let path = temp_db();
        let writer = setup(&path);
        let mut snapshotter = Snapshotter::new(&path, "zero", None);
        assert_eq!(snapshotter.init().unwrap().version, "00");

        writer
            .run("INSERT INTO issue VALUES (1, 'first', '01')", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "01",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(1.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "01").unwrap();

        let diff = snapshotter
            .advance(&specs(), &BTreeSet::from(["issue".to_string()]))
            .unwrap();
        assert_eq!(
            (diff.prev_version.as_str(), diff.curr_version.as_str()),
            ("00", "01")
        );
        assert_eq!(diff.changes, 1);
        assert!(diff.rows[0].prev_values.is_empty());
        assert_eq!(
            diff.rows[0].next_value.as_ref().unwrap()[1].1,
            Value::Text("first".into())
        );

        writer
            .run(
                "UPDATE issue SET title='second', _0_version='02' WHERE id=1",
                &[],
            )
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "02",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(1.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "02").unwrap();

        let diff = snapshotter
            .advance(&specs(), &BTreeSet::from(["issue".to_string()]))
            .unwrap();
        assert_eq!(
            (diff.prev_version.as_str(), diff.curr_version.as_str()),
            ("01", "02")
        );
        assert_eq!(
            diff.rows[0].prev_values[0][1].1,
            Value::Text("first".into())
        );
        assert_eq!(
            diff.rows[0].next_value.as_ref().unwrap()[1].1,
            Value::Text("second".into())
        );

        snapshotter.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn with_current_shared_reuses_the_connection_and_pins_the_snapshot() {
        let path = temp_db();
        let writer = setup(&path);
        let mut snapshotter = Snapshotter::new(&path, "zero", None);
        snapshotter.init().unwrap();
        assert_eq!(snapshotter.current().unwrap().version, "00");

        // Advance the WRITER's head past the snapshot WITHOUT advancing the
        // snapshotter. A reclaimed connection keeps its pinned "00" read
        // snapshot; a reopened one would jump to head "01".
        writer
            .run("INSERT INTO issue VALUES (1, 'x', '01')", &[])
            .unwrap();
        update_replication_watermark(&writer, "01").unwrap();

        // Every graph source in a pipeline is built over this one shared handle.
        let count = snapshotter
            .with_current_shared(|db| {
                db.borrow()
                    .get("SELECT count(*) FROM issue", &[])
                    .unwrap()
                    .unwrap()[0]
                    .1
                    .clone()
            })
            .unwrap();
        // The shared handle reads the PINNED "00" snapshot, so the post-snapshot
        // insert is invisible — sources share one consistent snapshot.
        assert_eq!(count, Value::Integer(0));
        // current() is restored and STILL pinned at "00": the connection was
        // reclaimed in place, not reopened at head (which would read "01").
        assert_eq!(snapshotter.current().unwrap().version, "00");

        snapshotter.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn schema_reset_aborts_incremental_advancement() {
        let path = temp_db();
        let writer = setup(&path);
        let mut snapshotter = Snapshotter::new(&path, "zero", None);
        snapshotter.init().unwrap();
        ChangeLog::new(&writer).log_reset_op("01", "issue").unwrap();
        update_replication_watermark(&writer, "01").unwrap();
        let error = snapshotter
            .advance(&specs(), &BTreeSet::from(["issue".to_string()]))
            .unwrap_err();
        assert!(matches!(
            error,
            SnapshotError::Reset {
                reason: ResetPipelinesReason::SchemaChange,
                ..
            }
        ));
        drop(writer);
        let _ = std::fs::remove_file(path);
    }
}
