//! Materializes a subscriber's change-log catch-up into concrete row diffs.
//!
//! When a view-syncer subscriber reconnects it reads the durable change-log
//! ([`crate::change_log::ChangeLog::read_since`]) for every row changed since
//! its watermark. But the change-log records only the row *key* and the op
//! (`s` set / `d` delete) — not the full row. To feed those changes into an
//! incremental view (IVM) the subscriber must resolve each entry into a
//! concrete change: a delete carries just the key, while a set must be paired
//! with the row's *current* full contents, read back from the replica table
//! (the change-log coalesces to the latest op, so a surviving `set` row is
//! guaranteed present in the replica). This module performs that resolution —
//! the bridge between the durable catch-up read and the IVM `apply_to_source`
//! feed the existing pipeline already drives for live changes.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::{parse, JsonValue};
use zero_cache_types::pg_data_type::ValueType;
use zero_cache_types::sql::id;

use crate::change_log::ChangeLogRow;
use crate::{DbError, StatementRunner, Value};

/// A resolved catch-up change ready to feed an incremental view.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedChange {
    /// The row exists as of the catch-up watermark; carries its full current
    /// contents (all replica columns, including the internal `_0_version`).
    Set {
        table: String,
        row: Vec<(String, Value)>,
    },
    /// The row was deleted; carries only its key columns (parsed from the
    /// change-log `rowKey`).
    Delete {
        table: String,
        key: Vec<(String, JsonValue)>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum CatchupError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error("malformed change-log rowKey {0:?}: {1}")]
    BadRowKey(String, String),
    #[error("unknown change-log op {0:?} (expected \"s\" or \"d\")")]
    UnknownOp(String),
    #[error("set row for {table} key {key} missing from replica")]
    MissingSetRow { table: String, key: String },
}

/// Parses a change-log `rowKey` (canonical JSON object) into ordered
/// `(column, value)` key pairs.
/// Parses a change-log `rowKey` (canonical JSON object text) into ordered
/// `(column, value)` pairs.
pub fn parse_row_key(row_key: &str) -> Result<Vec<(String, JsonValue)>, CatchupError> {
    match parse(row_key).map_err(|e| CatchupError::BadRowKey(row_key.to_string(), e.to_string()))? {
        JsonValue::Object(entries) => Ok(entries),
        _ => Err(CatchupError::BadRowKey(
            row_key.to_string(),
            "not a JSON object".into(),
        )),
    }
}

/// Binds a JSON key value to the SQLite parameter used to look the row up.
/// Only the scalar types that appear in a primary/unique key are handled;
/// anything else is bound via its JSON text form (matching how such keys are
/// stored/compared elsewhere in this port).
fn key_value_to_param(v: &JsonValue) -> Value {
    match v {
        JsonValue::Number(n) if n.fract() == 0.0 => Value::Integer(*n as i64),
        JsonValue::Number(n) => Value::Real(*n),
        JsonValue::String(s) => Value::Text(s.clone()),
        JsonValue::Bool(b) => Value::Integer(i64::from(*b)),
        JsonValue::BigInt(b) => Value::Text(b.to_string()),
        JsonValue::Null => Value::Null,
        other => Value::Text(other.stringify()),
    }
}

/// Resolves one change-log entry against the replica `db`. A delete yields the
/// key; a set reads the row's current full contents back from the replica
/// table.
pub fn resolve_change_log_row(
    db: &StatementRunner,
    entry: &ChangeLogRow,
) -> Result<ResolvedChange, CatchupError> {
    let key = parse_row_key(&entry.row_key)?;
    match entry.op.as_str() {
        "d" => Ok(ResolvedChange::Delete {
            table: entry.table.clone(),
            key,
        }),
        "s" => {
            // SELECT * FROM <table> WHERE <k1> = ? AND <k2> = ? ...
            let where_clause = key
                .iter()
                .map(|(col, _)| format!("{} = ?", id(col)))
                .collect::<Vec<_>>()
                .join(" AND ");
            let params: Vec<Value> = key.iter().map(|(_, v)| key_value_to_param(v)).collect();
            let sql = format!("SELECT * FROM {} WHERE {where_clause}", id(&entry.table));
            match db.get(&sql, &params)? {
                Some(row) => Ok(ResolvedChange::Set {
                    table: entry.table.clone(),
                    row,
                }),
                None => Err(CatchupError::MissingSetRow {
                    table: entry.table.clone(),
                    key: entry.row_key.clone(),
                }),
            }
        }
        other => Err(CatchupError::UnknownOp(other.to_string())),
    }
}

/// Resolves a whole catch-up batch (in order). Errors on the first bad entry.
pub fn resolve_catchup(
    db: &StatementRunner,
    entries: &[ChangeLogRow],
) -> Result<Vec<ResolvedChange>, CatchupError> {
    entries
        .iter()
        .map(|e| resolve_change_log_row(db, e))
        .collect()
}

/// Per-column declared ZQL value types for a replica table, as the snapshotter
/// derives them (`fromSQLiteTypes`): the map used to restore raw SQLite storage
/// values (0/1 booleans, JSON text) to their wire representation.
pub type ColumnTypes = BTreeMap<String, ValueType>;

/// A resolved catch-up change whose `Set` row values have been restored to
/// their declared ZQL type. This is the type-aware analogue of
/// [`ResolvedChange`]: [`resolve_change_log_row`] does `SELECT *` and returns
/// raw SQLite [`Value`]s, so a boolean caught up from the change-log would ship
/// as `1` instead of `true` and a JSON column as its raw text. Restoring the
/// column types here mirrors the snapshotter/pipeline path
/// (`SnapshotTableSpec::column_types` → `sql_value_to_zql`), which carries the
/// type map alongside the row for exactly this reason.
#[derive(Debug, Clone, PartialEq)]
pub enum TypedResolvedChange {
    /// The row exists as of the catch-up watermark; carries its full current
    /// contents with each value restored to its declared ZQL type.
    Set {
        table: String,
        row: Vec<(String, JsonValue)>,
    },
    /// The row was deleted; carries only its key columns.
    Delete {
        table: String,
        key: Vec<(String, JsonValue)>,
    },
}

/// Restores one raw SQLite storage value to its declared ZQL type. Mirrors the
/// snapshotter/pipeline conversion (`sql_value_to_zql`): a `Boolean` column's
/// stored 0/1 becomes JSON `true`/`false`, a `Json` column's text is parsed,
/// and every other type falls through to its generic JSON form. A missing type
/// (`None`) defaults to the generic form, matching `column_types` lookups that
/// default to `String`.
pub fn restore_lite_value(value: &Value, value_type: Option<ValueType>) -> JsonValue {
    match value_type {
        Some(ValueType::Boolean) => match value {
            Value::Null => JsonValue::Null,
            Value::Integer(value) => JsonValue::Bool(*value != 0),
            Value::Real(value) => JsonValue::Bool(*value != 0.0),
            Value::Text(value) => JsonValue::Bool(matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "t" | "true"
            )),
            Value::Blob(value) => JsonValue::Bool(!value.is_empty() && value != b"0"),
        },
        Some(ValueType::Json) => match value {
            Value::Null => JsonValue::Null,
            Value::Text(value) => parse(value).unwrap_or_else(|_| JsonValue::String(value.clone())),
            other => generic_lite_value(other),
        },
        _ => generic_lite_value(value),
    }
}

fn generic_lite_value(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Integer(value) => JsonValue::Number(*value as f64),
        Value::Real(value) => JsonValue::Number(*value),
        Value::Text(value) => JsonValue::String(value.clone()),
        Value::Blob(value) => JsonValue::String(String::from_utf8_lossy(value).into_owned()),
    }
}

/// Reads the declared ZQL value type of every column of `table` from the live
/// replica schema — the single-table analogue of the snapshotter's
/// `column_types` derivation. Unknown SQLite types default to `String`. An
/// absent table yields an empty map (every column then restores generically).
pub fn column_types_for(db: &StatementRunner, table: &str) -> Result<ColumnTypes, CatchupError> {
    let tables = crate::lite_tables::list_tables(db)?;
    Ok(tables
        .into_iter()
        .find(|spec| spec.name == table)
        .map(|spec| {
            spec.columns
                .into_iter()
                .map(|(name, column)| {
                    let value_type =
                        zero_cache_types::lite::lite_type_to_zql_value_type(&column.data_type)
                            .unwrap_or(ValueType::String);
                    (name, value_type)
                })
                .collect()
        })
        .unwrap_or_default())
}

/// Resolves one change-log entry like [`resolve_change_log_row`], then restores
/// each `Set` row value to its declared ZQL type using `column_types`.
pub fn resolve_change_log_row_typed(
    db: &StatementRunner,
    entry: &ChangeLogRow,
    column_types: &ColumnTypes,
) -> Result<TypedResolvedChange, CatchupError> {
    match resolve_change_log_row(db, entry)? {
        ResolvedChange::Delete { table, key } => Ok(TypedResolvedChange::Delete { table, key }),
        ResolvedChange::Set { table, row } => Ok(TypedResolvedChange::Set {
            table,
            row: row
                .into_iter()
                .map(|(column, value)| {
                    let restored = restore_lite_value(&value, column_types.get(&column).copied());
                    (column, restored)
                })
                .collect(),
        }),
    }
}

/// Resolves a whole catch-up batch with column-type restoration. Column types
/// are read from the replica once per distinct table and reused across the
/// batch. Errors on the first bad entry.
pub fn resolve_catchup_typed(
    db: &StatementRunner,
    entries: &[ChangeLogRow],
) -> Result<Vec<TypedResolvedChange>, CatchupError> {
    let mut type_cache: BTreeMap<String, ColumnTypes> = BTreeMap::new();
    entries
        .iter()
        .map(|entry| {
            if !type_cache.contains_key(&entry.table) {
                type_cache.insert(entry.table.clone(), column_types_for(db, &entry.table)?);
            }
            resolve_change_log_row_typed(db, entry, &type_cache[&entry.table])
        })
        .collect()
}

/// The distinct set of tables touched by a run of change-log entries — the
/// commit-side input to query invalidation
/// (`zero-cache-view-syncer::query_invalidation::invalidated_query_hashes`).
/// A commit is typically read via [`crate::change_log::ChangeLog::read_since`]
/// (the entries after the subscriber's last watermark); this collapses those
/// entries to the bare table names that changed, which is exactly what a
/// query's `referenced_tables` read-set is matched against. Sorted/deduped.
pub fn changed_tables(entries: &[ChangeLogRow]) -> std::collections::BTreeSet<String> {
    entries.iter().map(|e| e.table.clone()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_log::{ChangeLog, RowKey, CREATE_CHANGELOG_SCHEMA};

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, \"_0_version\" TEXT)")
            .unwrap();
        db
    }

    fn rk(id_val: i64) -> RowKey {
        vec![("id".to_string(), JsonValue::Number(id_val as f64))]
    }

    #[test]
    fn resolves_a_set_to_the_current_replica_row() {
        let db = setup();
        db.run(
            "INSERT INTO issue (id, title, \"_0_version\") VALUES (1, 'hello', '01')",
            &[],
        )
        .unwrap();
        let cl = ChangeLog::new(&db);
        cl.log_set_op("01", 0, "issue", &rk(1), None).unwrap();

        let entries = cl.read_since("00").unwrap();
        let resolved = resolve_catchup(&db, &entries).unwrap();
        assert_eq!(resolved.len(), 1);
        match &resolved[0] {
            ResolvedChange::Set { table, row } => {
                assert_eq!(table, "issue");
                assert_eq!(row[0], ("id".to_string(), Value::Integer(1)));
                assert_eq!(row[1], ("title".to_string(), Value::Text("hello".into())));
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }

    #[test]
    fn resolves_a_delete_to_the_key_only() {
        let db = setup();
        let cl = ChangeLog::new(&db);
        cl.log_delete_op("02", 0, "issue", &rk(7)).unwrap();

        let entries = cl.read_since("00").unwrap();
        let resolved = resolve_catchup(&db, &entries).unwrap();
        assert_eq!(
            resolved,
            vec![ResolvedChange::Delete {
                table: "issue".into(),
                key: vec![("id".into(), JsonValue::Number(7.0))],
            }]
        );
    }

    #[test]
    fn set_then_delete_of_same_row_coalesces_to_a_single_delete() {
        // The change-log keeps only the latest op per (table,row); a set then a
        // delete of id=1 leaves just the delete for catch-up.
        let db = setup();
        db.run(
            "INSERT INTO issue (id, title, \"_0_version\") VALUES (1, 'x', '01')",
            &[],
        )
        .unwrap();
        let cl = ChangeLog::new(&db);
        cl.log_set_op("01", 0, "issue", &rk(1), None).unwrap();
        cl.log_delete_op("02", 0, "issue", &rk(1)).unwrap();

        let resolved = resolve_catchup(&db, &cl.read_since("00").unwrap()).unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(matches!(resolved[0], ResolvedChange::Delete { .. }));
    }

    #[test]
    fn text_key_binds_correctly() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec("CREATE TABLE t (k TEXT PRIMARY KEY, v TEXT)")
            .unwrap();
        db.run("INSERT INTO t (k, v) VALUES ('abc', 'val')", &[])
            .unwrap();
        let cl = ChangeLog::new(&db);
        cl.log_set_op(
            "01",
            0,
            "t",
            &vec![("k".to_string(), JsonValue::String("abc".into()))],
            None,
        )
        .unwrap();

        let resolved = resolve_catchup(&db, &cl.read_since("00").unwrap()).unwrap();
        match &resolved[0] {
            ResolvedChange::Set { row, .. } => {
                assert_eq!(row[0], ("k".to_string(), Value::Text("abc".into())));
                assert_eq!(row[1], ("v".to_string(), Value::Text("val".into())));
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }

    #[test]
    fn lagged_subscriber_catches_up_only_past_its_last_watermark() {
        // Models the `FanoutEvent::Lagged` recovery path: a subscriber that
        // fell behind re-catches-up via `read_since(last_watermark)`. The
        // boundary is STRICTLY-after (`stateVersion > ?`), so a subscriber
        // that last saw watermark "02" must receive ONLY the commit at "03" —
        // re-delivering "01"/"02" would double-apply already-seen changes.
        let db = setup();
        for (id, ver) in [(1, "01"), (2, "02"), (3, "03")] {
            db.run(
                "INSERT INTO issue (id, title, \"_0_version\") VALUES (?, ?, ?)",
                &[
                    Value::Integer(id),
                    Value::Text(format!("t{id}")),
                    Value::Text(ver.to_string()),
                ],
            )
            .unwrap();
        }
        let cl = ChangeLog::new(&db);
        cl.log_set_op("01", 0, "issue", &rk(1), None).unwrap();
        cl.log_set_op("02", 0, "issue", &rk(2), None).unwrap();
        cl.log_set_op("03", 0, "issue", &rk(3), None).unwrap();

        // Subscriber's last-seen watermark is "02"; only "03" is new.
        let resolved = resolve_catchup(&db, &cl.read_since("02").unwrap()).unwrap();
        assert_eq!(
            resolved.len(),
            1,
            "only the single commit after the last watermark is caught up"
        );
        match &resolved[0] {
            ResolvedChange::Set { table, row } => {
                assert_eq!(table, "issue");
                assert_eq!(row[0], ("id".to_string(), Value::Integer(3)));
                assert_eq!(row[1], ("title".to_string(), Value::Text("t3".into())));
            }
            other => panic!("expected the id=3 Set, got {other:?}"),
        }
    }

    #[test]
    fn typed_catchup_restores_boolean_and_json_column_types() {
        // A `SELECT *` catch-up returns raw SQLite values (a bool stored 0/1, a
        // JSON column stored as text). The typed path restores them to their
        // declared ZQL type — booleans to JSON true/false, JSON to a parsed
        // value — exactly as the snapshotter/pipeline path does.
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(
            "CREATE TABLE issue (id INTEGER PRIMARY KEY, active bool, tags json, \"_0_version\" TEXT)",
        )
        .unwrap();
        db.run(
            "INSERT INTO issue (id, active, tags, \"_0_version\") VALUES (1, 1, ?, '01')",
            &[Value::Text(r#"["a","b"]"#.into())],
        )
        .unwrap();
        let cl = ChangeLog::new(&db);
        cl.log_set_op("01", 0, "issue", &rk(1), None).unwrap();

        let entries = cl.read_since("00").unwrap();

        // The untyped path still ships the raw storage values.
        let untyped = resolve_catchup(&db, &entries).unwrap();
        match &untyped[0] {
            ResolvedChange::Set { row, .. } => {
                assert_eq!(row[1], ("active".to_string(), Value::Integer(1)));
                assert_eq!(
                    row[2],
                    ("tags".to_string(), Value::Text(r#"["a","b"]"#.into()))
                );
            }
            other => panic!("expected Set, got {other:?}"),
        }

        // The typed path restores declared column types.
        let typed = resolve_catchup_typed(&db, &entries).unwrap();
        assert_eq!(typed.len(), 1);
        match &typed[0] {
            TypedResolvedChange::Set { table, row } => {
                assert_eq!(table, "issue");
                assert_eq!(row[0], ("id".to_string(), JsonValue::Number(1.0)));
                assert_eq!(row[1], ("active".to_string(), JsonValue::Bool(true)));
                assert_eq!(
                    row[2],
                    (
                        "tags".to_string(),
                        JsonValue::Array(vec![
                            JsonValue::String("a".into()),
                            JsonValue::String("b".into()),
                        ])
                    )
                );
            }
            other => panic!("expected typed Set, got {other:?}"),
        }
    }

    #[test]
    fn typed_catchup_delete_carries_key_unchanged() {
        let db = setup();
        let cl = ChangeLog::new(&db);
        cl.log_delete_op("02", 0, "issue", &rk(7)).unwrap();
        let typed = resolve_catchup_typed(&db, &cl.read_since("00").unwrap()).unwrap();
        assert_eq!(
            typed,
            vec![TypedResolvedChange::Delete {
                table: "issue".into(),
                key: vec![("id".into(), JsonValue::Number(7.0))],
            }]
        );
    }

    #[test]
    fn restore_lite_value_matches_snapshotter_conversion() {
        // Boolean: 0/1 -> false/true; falsey text/blob handled.
        assert_eq!(
            restore_lite_value(&Value::Integer(0), Some(ValueType::Boolean)),
            JsonValue::Bool(false)
        );
        assert_eq!(
            restore_lite_value(&Value::Integer(1), Some(ValueType::Boolean)),
            JsonValue::Bool(true)
        );
        // JSON text is parsed; unparseable text falls back to a JSON string.
        assert_eq!(
            restore_lite_value(&Value::Text("{\"k\":1}".into()), Some(ValueType::Json)),
            JsonValue::Object(vec![("k".into(), JsonValue::Number(1.0))])
        );
        // No declared type -> generic conversion (integer stays a number).
        assert_eq!(
            restore_lite_value(&Value::Integer(5), None),
            JsonValue::Number(5.0)
        );
    }

    #[test]
    fn changed_tables_collapses_entries_to_distinct_table_names() {
        // A commit touching `issue` twice and `comment` once collapses to the
        // two distinct tables — the invalidation matcher's commit-side input.
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        let cl = ChangeLog::new(&db);
        cl.log_set_op("01", 0, "issue", &rk(1), None).unwrap();
        cl.log_set_op("01", 1, "issue", &rk(2), None).unwrap();
        cl.log_delete_op("01", 2, "comment", &rk(9)).unwrap();

        let tables = changed_tables(&cl.read_since("00").unwrap());
        assert_eq!(
            tables,
            std::collections::BTreeSet::from(["issue".to_string(), "comment".to_string()])
        );
    }
}
