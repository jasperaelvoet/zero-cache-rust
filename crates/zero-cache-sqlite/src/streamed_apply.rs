//! Applies changes streamed from a change-streamer to a VIEW-SYNCER's local
//! replica.
//!
//! A view-syncer node does not own the Postgres replication slot — instead it
//! receives resolved row changes from the change-streamer (over the network)
//! and applies them here: the row is written to the replica, recorded in the
//! local change-log (so this node's own connected clients get pokes through the
//! usual `commit_dispatch`/`rehydrate` path), and the replica watermark is
//! advanced — all in one transaction per commit.

use zero_cache_shared::bigint_json::JsonValue;

use crate::change_log::{ChangeLog, RowKey};
use crate::replication_state::update_replication_watermark;
use crate::{DbError, StatementRunner, Value};

/// One resolved change in a streamed commit.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamedChange {
    /// Upsert `row` (full column set) into `table`; `row_key` is its primary key
    /// (for the change-log).
    Set {
        table: String,
        row_key: RowKey,
        row: Vec<(String, Value)>,
    },
    /// Delete the row identified by `row_key` from `table`.
    Del { table: String, row_key: RowKey },
}

/// Binds a JSON key value to the SQLite parameter used to look a row up.
fn key_param(v: &JsonValue) -> Value {
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

fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Applies one streamed commit (all `changes` at `watermark`) atomically:
/// upserts/deletes rows, records each in the change-log, and advances the
/// replica watermark. On any error the whole commit rolls back.
pub fn apply_streamed_commit(
    db: &StatementRunner,
    watermark: &str,
    changes: &[StreamedChange],
) -> Result<(), DbError> {
    db.exec("BEGIN")?;
    let result = (|| -> Result<(), DbError> {
        let cl = ChangeLog::new(db);
        for (pos, ch) in changes.iter().enumerate() {
            match ch {
                StreamedChange::Set {
                    table,
                    row_key,
                    row,
                } => {
                    let cols = row
                        .iter()
                        .map(|(c, _)| ident(c))
                        .collect::<Vec<_>>()
                        .join(",");
                    let placeholders = vec!["?"; row.len()].join(",");
                    let sql = format!(
                        "INSERT OR REPLACE INTO {} ({cols}) VALUES ({placeholders})",
                        ident(table)
                    );
                    let params: Vec<Value> = row.iter().map(|(_, v)| v.clone()).collect();
                    db.run(&sql, &params)?;
                    cl.log_set_op(watermark, pos as i64, table, row_key, None)?;
                }
                StreamedChange::Del { table, row_key } => {
                    let where_ = row_key
                        .iter()
                        .map(|(c, _)| format!("{} = ?", ident(c)))
                        .collect::<Vec<_>>()
                        .join(" AND ");
                    let sql = format!("DELETE FROM {} WHERE {where_}", ident(table));
                    let params: Vec<Value> = row_key.iter().map(|(_, v)| key_param(v)).collect();
                    db.run(&sql, &params)?;
                    cl.log_delete_op(watermark, pos as i64, table, row_key)?;
                }
            }
        }
        update_replication_watermark(db, watermark)?;
        Ok(())
    })();
    match result {
        Ok(()) => {
            db.exec("COMMIT")?;
            Ok(())
        }
        Err(e) => {
            let _ = db.exec("ROLLBACK");
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_log::CREATE_CHANGELOG_SCHEMA;
    use crate::replication_state::{get_replication_state, CREATE_REPLICATION_STATE_SCHEMA};

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_REPLICATION_STATE_SCHEMA).unwrap();
        db.exec(
            r#"INSERT INTO "_zero.replicationState" (stateVersion, writeTimeMs) VALUES ('00', 0)"#,
        )
        .unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, \"_0_version\" TEXT)")
            .unwrap();
        db
    }

    fn rk(id: i64) -> RowKey {
        vec![("id".to_string(), JsonValue::Number(id as f64))]
    }

    #[test]
    fn applies_set_then_delete_with_changelog_and_watermark() {
        let db = setup();

        // Commit 1 at "01": upsert two rows.
        apply_streamed_commit(
            &db,
            "01",
            &[
                StreamedChange::Set {
                    table: "issue".into(),
                    row_key: rk(1),
                    row: vec![
                        ("id".into(), Value::Integer(1)),
                        ("title".into(), Value::Text("first".into())),
                        ("_0_version".into(), Value::Text("01".into())),
                    ],
                },
                StreamedChange::Set {
                    table: "issue".into(),
                    row_key: rk(2),
                    row: vec![
                        ("id".into(), Value::Integer(2)),
                        ("title".into(), Value::Text("second".into())),
                        ("_0_version".into(), Value::Text("01".into())),
                    ],
                },
            ],
        )
        .unwrap();

        // Both rows landed; watermark advanced; change-log has 2 entries.
        let rows = db
            .query_uncached("SELECT id FROM issue ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(get_replication_state(&db).unwrap().state_version, "01");
        assert_eq!(ChangeLog::new(&db).read_since("00").unwrap().len(), 2);

        // Commit 2 at "02": delete row 1.
        apply_streamed_commit(
            &db,
            "02",
            &[StreamedChange::Del {
                table: "issue".into(),
                row_key: rk(1),
            }],
        )
        .unwrap();

        let ids: Vec<i64> = db
            .query_uncached("SELECT id FROM issue ORDER BY id", &[])
            .unwrap()
            .iter()
            .map(|r| match r[0].1 {
                Value::Integer(n) => n,
                _ => panic!(),
            })
            .collect();
        assert_eq!(ids, vec![2], "row 1 deleted");
        assert_eq!(get_replication_state(&db).unwrap().state_version, "02");
        // The delete is visible to a catch-up reader since "01".
        assert!(!ChangeLog::new(&db).read_since("01").unwrap().is_empty());
    }

    #[test]
    fn a_failed_change_rolls_back_the_whole_commit() {
        let db = setup();
        // Second change targets a nonexistent table -> the whole commit rolls
        // back, so the first row must NOT persist.
        let res = apply_streamed_commit(
            &db,
            "01",
            &[
                StreamedChange::Set {
                    table: "issue".into(),
                    row_key: rk(1),
                    row: vec![
                        ("id".into(), Value::Integer(1)),
                        ("title".into(), Value::Text("x".into())),
                    ],
                },
                StreamedChange::Set {
                    table: "nonexistent".into(),
                    row_key: rk(9),
                    row: vec![("id".into(), Value::Integer(9))],
                },
            ],
        );
        assert!(res.is_err());
        assert_eq!(
            db.query_uncached("SELECT count(*) FROM issue", &[])
                .unwrap()[0][0]
                .1,
            Value::Integer(0),
            "first row rolled back"
        );
        assert_eq!(get_replication_state(&db).unwrap().state_version, "00");
    }
}
