//! Partial port of
//! `zero-cache/src/services/replicator/schema/replication-state.ts`.
//!
//! Replication metadata used for incremental view maintenance and catchup:
//! the `_zero.replicationConfig`, `_zero.replicationState`, and
//! `_zero.runtimeEvents` tables, with their init/get/update helpers.
//!
//! The full schema also creates the change-log and column/table metadata tables
//! (from sibling modules not yet ported); those are omitted here and created
//! once those modules land. The helpers below only touch the three tables above.

use zero_cache_shared::bigint_json::{parse, stringify, JsonValue};

use crate::{DbError, StatementRunner, Value};

/// DDL for the three replication-state tables ported so far.
pub const CREATE_REPLICATION_STATE_SCHEMA: &str = r#"
CREATE TABLE "_zero.replicationConfig" (
    replicaVersion TEXT NOT NULL,
    publications TEXT NOT NULL,
    initialSyncContext TEXT DEFAULT '{}',
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
);
CREATE TABLE "_zero.replicationState" (
    stateVersion TEXT NOT NULL,
    writeTimeMs INTEGER,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
);
CREATE TABLE "_zero.runtimeEvents" (
    event TEXT PRIMARY KEY ON CONFLICT REPLACE,
    timestamp TEXT NOT NULL DEFAULT (current_timestamp)
);
"#;

/// The `stateVersion` of the replica. Port of `ReplicationState`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationState {
    pub state_version: String,
}

/// Subscription state plus the initial-sync context. Port of
/// `SubscriptionStateAndContext`.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionStateAndContext {
    pub replica_version: String,
    pub publications: Vec<String>,
    pub initial_sync_context: JsonValue,
    pub watermark: String,
}

/// Creates the replication-state tables. Port of `createReplicationStateTables`.
pub fn create_replication_state_tables(db: &StatementRunner) -> Result<(), DbError> {
    db.exec(CREATE_REPLICATION_STATE_SCHEMA)
}

/// Records a runtime event (`sync` | `upgrade` | `vacuum`), replacing any prior
/// row for the same event. Port of `recordEvent`.
pub fn record_event(db: &StatementRunner, event: &str) -> Result<(), DbError> {
    db.run(
        r#"INSERT INTO "_zero.runtimeEvents" (event) VALUES (?)"#,
        &[Value::Text(event.to_string())],
    )?;
    Ok(())
}

/// Initializes replication state. Port of `initReplicationState`. `publications`
/// are sorted and JSON-encoded; `initial_sync_context` is JSON-encoded.
pub fn init_replication_state(
    db: &StatementRunner,
    publications: &[String],
    watermark: &str,
    initial_sync_context: &JsonValue,
    create_tables: bool,
) -> Result<(), DbError> {
    if create_tables {
        create_replication_state_tables(db)?;
    }
    let mut sorted = publications.to_vec();
    sorted.sort();
    let pubs_json = stringify(&JsonValue::Array(
        sorted.into_iter().map(JsonValue::String).collect(),
    ));
    db.run(
        r#"INSERT INTO "_zero.replicationConfig" (replicaVersion, publications, initialSyncContext) VALUES (?, ?, ?)"#,
        &[
            Value::Text(watermark.to_string()),
            Value::Text(pubs_json),
            Value::Text(stringify(initial_sync_context)),
        ],
    )?;
    db.run(
        r#"INSERT INTO "_zero.replicationState" (stateVersion, writeTimeMs) VALUES (?, unixepoch('subsec') * 1000)"#,
        &[Value::Text(watermark.to_string())],
    )?;
    record_event(db, "sync")?;
    Ok(())
}

/// Returns runtime events ordered by timestamp ascending, as `(event,
/// timestamp)` pairs. Port of `getAscendingEvents` (timestamps kept as the raw
/// SQLite string rather than a parsed `Date`).
pub fn get_ascending_events(db: &StatementRunner) -> Result<Vec<(String, String)>, DbError> {
    let rows = db.query_uncached(
        r#"SELECT event, timestamp FROM "_zero.runtimeEvents" ORDER BY timestamp ASC"#,
        &[],
    )?;
    Ok(rows
        .into_iter()
        .map(|r| (text(&r[0].1), text(&r[1].1)))
        .collect())
}

/// Updates the replication watermark. Port of `updateReplicationWatermark`.
pub fn update_replication_watermark(db: &StatementRunner, watermark: &str) -> Result<(), DbError> {
    db.run(
        r#"UPDATE "_zero.replicationState" SET stateVersion=?, writeTimeMs=unixepoch('subsec') * 1000"#,
        &[Value::Text(watermark.to_string())],
    )?;
    Ok(())
}

/// Returns the current replication state. Port of `getReplicationState`.
pub fn get_replication_state(db: &StatementRunner) -> Result<ReplicationState, DbError> {
    let row = db
        .get(r#"SELECT stateVersion FROM "_zero.replicationState""#, &[])?
        .ok_or_else(|| DbError("no replication state".into()))?;
    Ok(ReplicationState {
        state_version: text(&row[0].1),
    })
}

/// Returns subscription state and initial-sync context. Port of
/// `getSubscriptionStateAndContext`.
pub fn get_subscription_state_and_context(
    db: &StatementRunner,
) -> Result<SubscriptionStateAndContext, DbError> {
    let row = db
        .get(
            r#"SELECT c.replicaVersion, c.publications, c.initialSyncContext, s.stateVersion as watermark
               FROM "_zero.replicationConfig" as c
               JOIN "_zero.replicationState" as s ON c.lock = s.lock"#,
            &[],
        )?
        .ok_or_else(|| DbError("no subscription state".into()))?;

    let publications = parse_string_array(&text(&row[1].1))?;
    let initial_sync_context =
        parse(&text(&row[2].1)).map_err(|e| DbError(format!("bad initialSyncContext: {e}")))?;
    Ok(SubscriptionStateAndContext {
        replica_version: text(&row[0].1),
        publications,
        initial_sync_context,
        watermark: text(&row[3].1),
    })
}

fn parse_string_array(json: &str) -> Result<Vec<String>, DbError> {
    match parse(json).map_err(|e| DbError(format!("bad publications: {e}")))? {
        JsonValue::Array(items) => Ok(items
            .into_iter()
            .map(|v| match v {
                JsonValue::String(s) => s,
                _ => String::new(),
            })
            .collect()),
        _ => Err(DbError("publications is not an array".into())),
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
        let ctx = JsonValue::Object(vec![("foo".into(), JsonValue::String("bar".into()))]);
        init_replication_state(
            &db,
            &["zero_data".into(), "zero_metadata".into()],
            "0a",
            &ctx,
            true,
        )
        .unwrap();
        db
    }

    #[test]
    fn initial_replication_state() {
        let db = setup();
        let cfg = db
            .query_uncached(
                r#"SELECT replicaVersion, publications, initialSyncContext, lock FROM "_zero.replicationConfig""#,
                &[],
            )
            .unwrap();
        assert_eq!(text(&cfg[0][0].1), "0a");
        assert_eq!(text(&cfg[0][1].1), r#"["zero_data","zero_metadata"]"#);
        assert_eq!(text(&cfg[0][2].1), r#"{"foo":"bar"}"#);

        let state = db
            .query_uncached(
                r#"SELECT stateVersion, writeTimeMs FROM "_zero.replicationState""#,
                &[],
            )
            .unwrap();
        assert_eq!(text(&state[0][0].1), "0a");
        assert!(matches!(state[0][1].1, Value::Integer(n) if n > 0));

        let events = get_ascending_events(&db).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].0, "sync");
        assert!(!events[0].1.is_empty());
    }

    #[test]
    fn runtime_events() {
        let db = setup();
        record_event(&db, "upgrade").unwrap();
        record_event(&db, "vacuum").unwrap();
        record_event(&db, "vacuum").unwrap(); // REPLACE keeps a single vacuum row
        let events = get_ascending_events(&db).unwrap();
        let names: Vec<&str> = events.iter().map(|(e, _)| e.as_str()).collect();
        assert_eq!(names, vec!["sync", "upgrade", "vacuum"]);
    }

    #[test]
    fn subscription_state() {
        let db = setup();
        let s = get_subscription_state_and_context(&db).unwrap();
        assert_eq!(s.replica_version, "0a");
        assert_eq!(s.publications, vec!["zero_data", "zero_metadata"]);
        assert_eq!(
            s.initial_sync_context,
            JsonValue::Object(vec![("foo".into(), JsonValue::String("bar".into()))])
        );
        assert_eq!(s.watermark, "0a");
    }

    #[test]
    fn get_versions_and_update_watermark() {
        let db = setup();
        assert_eq!(
            get_replication_state(&db).unwrap(),
            ReplicationState {
                state_version: "0a".into()
            }
        );

        update_replication_watermark(&db, "0f").unwrap();
        assert_eq!(get_replication_state(&db).unwrap().state_version, "0f");
        // replicaVersion stays; watermark follows stateVersion.
        let s = get_subscription_state_and_context(&db).unwrap();
        assert_eq!(s.replica_version, "0a");
        assert_eq!(s.watermark, "0f");

        update_replication_watermark(&db, "0r").unwrap();
        assert_eq!(get_replication_state(&db).unwrap().state_version, "0r");
        assert_eq!(
            get_subscription_state_and_context(&db).unwrap().watermark,
            "0r"
        );
    }
}
