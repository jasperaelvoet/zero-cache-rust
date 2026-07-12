//! Replica runtime events (`_zero.runtimeEvents`) and the startup VACUUM —
//! port of upstream `services/replicator/schema/replication-state.ts`'s
//! `recordEvent`/`getAscendingEvents` and the `replica.vacuumIntervalHours`
//! logic in `workers/replicator.ts` `prepare()`.
//!
//! One row per event type (`sync` | `upgrade` | `vacuum`), each keeping only
//! its latest timestamp via `ON CONFLICT REPLACE`. At startup, when the most
//! recent event of ANY type is older than the configured interval, the
//! replica is VACUUMed in place (heavyweight: rewrites the file, needs ~2× db
//! size on disk) and a `vacuum` event is recorded.

use crate::{DbError, StatementRunner, Value};

/// Upstream's `CREATE TABLE` for the events table (SQLite).
pub const CREATE_RUNTIME_EVENTS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS "_zero.runtimeEvents" (
  event TEXT PRIMARY KEY ON CONFLICT REPLACE,
  timestamp TEXT NOT NULL DEFAULT (current_timestamp)
);
"#;

/// Records (or refreshes) a runtime event with the current timestamp.
pub fn record_event(db: &StatementRunner, event: &str) -> Result<(), DbError> {
    db.exec(CREATE_RUNTIME_EVENTS_TABLE)?;
    db.exec(&format!(
        r#"INSERT INTO "_zero.runtimeEvents" (event) VALUES ('{event}')"#
    ))
}

/// Milliseconds since the most recent runtime event of any type, or `None`
/// when no events exist (a fresh replica — initial sync records `sync`).
/// `now_ms` is injected for testability; timestamps are SQLite
/// `current_timestamp` UTC strings (`YYYY-MM-DD HH:MM:SS`).
pub fn millis_since_last_event(db: &StatementRunner, now_ms: i64) -> Result<Option<i64>, DbError> {
    db.exec(CREATE_RUNTIME_EVENTS_TABLE)?;
    // unixepoch() parses the stored text timestamp as UTC.
    let rows = db.query_uncached(
        r#"SELECT CAST(unixepoch(timestamp) AS INTEGER) AS ts
           FROM "_zero.runtimeEvents" ORDER BY timestamp DESC LIMIT 1"#,
        &[],
    )?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let ts_secs = row
        .into_iter()
        .next()
        .and_then(|(_, v)| match v {
            Value::Integer(n) => Some(n),
            _ => None,
        })
        .unwrap_or(0);
    Ok(Some(now_ms - ts_secs * 1000))
}

/// Startup VACUUM per `ZERO_REPLICA_VACUUM_INTERVAL_HOURS`: when the last
/// sync/upgrade/vacuum event is older than `interval_hours`, run a plain
/// in-place `VACUUM` and record a `vacuum` event. Returns whether a VACUUM
/// ran. A replica with no recorded events is left alone (initial sync will
/// stamp `sync`).
pub fn maybe_vacuum_at_startup(db: &StatementRunner, interval_hours: f64) -> Result<bool, DbError> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default();
    let Some(elapsed_ms) = millis_since_last_event(db, now_ms)? else {
        return Ok(false);
    };
    if (elapsed_ms as f64) / 3_600_000.0 <= interval_hours {
        return Ok(false);
    }
    db.exec("VACUUM")?;
    record_event(db, "vacuum")?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_keep_only_the_latest_timestamp_per_type() {
        let db = StatementRunner::open_in_memory().unwrap();
        record_event(&db, "sync").unwrap();
        record_event(&db, "sync").unwrap();
        let rows = db
            .query_uncached(r#"SELECT COUNT(*) FROM "_zero.runtimeEvents""#, &[])
            .unwrap();
        let count = match rows.into_iter().next().unwrap().into_iter().next() {
            Some((_, Value::Integer(n))) => n,
            other => panic!("unexpected {other:?}"),
        };
        assert_eq!(count, 1, "ON CONFLICT REPLACE keeps one row per event");
    }

    #[test]
    fn elapsed_time_is_measured_from_the_most_recent_event() {
        let db = StatementRunner::open_in_memory().unwrap();
        assert_eq!(millis_since_last_event(&db, 0).unwrap(), None);

        db.exec(CREATE_RUNTIME_EVENTS_TABLE).unwrap();
        db.exec(
            r#"INSERT INTO "_zero.runtimeEvents" (event, timestamp)
               VALUES ('sync', '2026-01-01 00:00:00')"#,
        )
        .unwrap();
        db.exec(
            r#"INSERT INTO "_zero.runtimeEvents" (event, timestamp)
               VALUES ('vacuum', '2026-01-02 00:00:00')"#,
        )
        .unwrap();
        // 2026-01-02T00:00:00Z = 1767312000; one hour later:
        let now_ms = (1_767_312_000 + 3600) * 1000;
        assert_eq!(
            millis_since_last_event(&db, now_ms).unwrap(),
            Some(3_600_000)
        );
    }

    #[test]
    fn vacuum_runs_only_when_the_interval_elapsed_and_stamps_the_event() {
        let db = StatementRunner::open_in_memory().unwrap();
        // Fresh replica: no events -> no vacuum.
        assert!(!maybe_vacuum_at_startup(&db, 1.0).unwrap());

        // A recent event -> below interval -> no vacuum.
        record_event(&db, "sync").unwrap();
        assert!(!maybe_vacuum_at_startup(&db, 1.0).unwrap());

        // Backdate the event 2 hours -> 1h interval elapsed -> vacuum + stamp.
        db.exec(
            r#"UPDATE "_zero.runtimeEvents"
               SET timestamp = datetime('now', '-2 hours') WHERE event = 'sync'"#,
        )
        .unwrap();
        assert!(maybe_vacuum_at_startup(&db, 1.0).unwrap());
        // The fresh 'vacuum' stamp means an immediate re-check does nothing.
        assert!(!maybe_vacuum_at_startup(&db, 1.0).unwrap());
    }
}
