//! Port of `zero-cache/src/services/replicator/schema/change-log.ts`.
//!
//! The Change Log is a cross-table index of row changes ordered by state
//! version, used to compute a minimal diff to advance a pipeline from one
//! version to another. It stores identifiers only (table + normalized row key +
//! op), not row contents.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::{parse, stringify, JsonValue};
use zero_cache_types::row_key::normalized_key_order;

use crate::{DbError, StatementRunner, Value};

pub const SET_OP: &str = "s";
pub const DEL_OP: &str = "d";
pub const TRUNCATE_OP: &str = "t";
pub const RESET_OP: &str = "r";

/// DDL for the change-log table. Port of `CREATE_CHANGELOG_SCHEMA`.
pub const CREATE_CHANGELOG_SCHEMA: &str = r#"
CREATE TABLE "_zero.changeLog2" (
    "stateVersion"              TEXT NOT NULL,
    "pos"                       INT  NOT NULL,
    "table"                     TEXT NOT NULL,
    "rowKey"                    TEXT NOT NULL,
    "op"                        TEXT NOT NULL,
    "backfillingColumnVersions" TEXT DEFAULT '{}',
    PRIMARY KEY("stateVersion", "pos"),
    UNIQUE("table", "rowKey")
);
"#;

/// A row key: ordered `(column, value)` pairs (values as JSON).
pub type RowKey = Vec<(String, JsonValue)>;

/// A raw change-log entry, as returned by [`ChangeLog::get_latest_row_op`]. Port
/// of `RawChangeLogEntry`.
#[derive(Debug, Clone, PartialEq)]
pub struct RawChangeLogEntry {
    pub state_version: String,
    pub table: String,
    pub row_key: String,
    pub op: String,
    pub backfilling_column_versions: BTreeMap<String, String>,
}

/// One change-log entry as returned by [`ChangeLog::read_since`] during
/// subscriber catch-up: the row's latest op at `state_version`/`pos`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeLogRow {
    pub state_version: String,
    pub pos: i64,
    pub table: String,
    /// The row key as stored (canonical JSON text).
    pub row_key: String,
    /// `s` (set) or `d` (delete) — the op the change-log records.
    pub op: String,
}

/// Records row and table-wide changes in the change log. Port of `ChangeLog`.
pub struct ChangeLog<'a> {
    db: &'a StatementRunner,
}

impl<'a> ChangeLog<'a> {
    pub fn new(db: &'a StatementRunner) -> Self {
        ChangeLog { db }
    }

    /// Logs a set (insert/update). `backfilled` distinguishes "no backfill"
    /// (`None`, clears vestigial versions) from "backfill in progress"
    /// (`Some`, merges column versions). Port of `logSetOp`.
    pub fn log_set_op(
        &self,
        version: &str,
        pos: i64,
        table: &str,
        row: &RowKey,
        backfilled: Option<&[String]>,
    ) -> Result<String, DbError> {
        self.log_row_op(version, pos, table, row, SET_OP, backfilled)
    }

    /// Logs a delete (always clears backfilling versions). Port of `logDeleteOp`.
    pub fn log_delete_op(
        &self,
        version: &str,
        pos: i64,
        table: &str,
        row: &RowKey,
    ) -> Result<String, DbError> {
        self.log_row_op(version, pos, table, row, DEL_OP, None)
    }

    /// Logs a table truncation (`pos = -1`, rowKey = version). Port of
    /// `logTruncateOp`.
    pub fn log_truncate_op(&self, version: &str, table: &str) -> Result<(), DbError> {
        self.log_table_wide_op(version, table, TRUNCATE_OP)
    }

    /// Logs a table reset / schema change. Port of `logResetOp`.
    pub fn log_reset_op(&self, version: &str, table: &str) -> Result<(), DbError> {
        self.log_table_wide_op(version, table, RESET_OP)
    }

    /// Returns the latest op recorded for a row, or `None`. Port of
    /// `getLatestRowOp`.
    pub fn get_latest_row_op(
        &self,
        table: &str,
        row: &RowKey,
    ) -> Result<Option<RawChangeLogEntry>, DbError> {
        let row_key = row_key_string(row)?;
        let result = self.db.get(
            r#"SELECT stateVersion, "table", rowKey, op, backfillingColumnVersions
               FROM "_zero.changeLog2" WHERE "table" = ? AND rowKey = JSON(?)"#,
            &[Value::Text(table.to_string()), Value::Text(row_key)],
        )?;
        match result {
            None => Ok(None),
            Some(r) => Ok(Some(RawChangeLogEntry {
                state_version: text(&r[0].1),
                table: text(&r[1].1),
                row_key: text(&r[2].1),
                op: text(&r[3].1),
                backfilling_column_versions: parse_string_map(&text(&r[4].1))?,
            })),
        }
    }

    /// Reads all change-log entries committed strictly after `after_version`,
    /// in commit order (`stateVersion`, then `pos`). This is the durable
    /// catch-up read a reconnecting subscriber performs: it holds a watermark
    /// (its last-seen `stateVersion`) and replays every row change recorded
    /// since, so it converges to the current replica state without re-reading
    /// the upstream Postgres stream. Port of the change-log side of
    /// `Storer`/`Subscriber` catchup (the query, not the live streaming).
    ///
    /// Because the change-log keeps only the *latest* op per `(table, rowKey)`
    /// (the `UNIQUE(table, rowKey)` constraint + `INSERT OR REPLACE`), catch-up
    /// naturally coalesces multiple changes to the same row into one — a
    /// subscriber that was behind sees each row's final state, not its full
    /// history, which is exactly what an incremental view needs.
    pub fn read_since(&self, after_version: &str) -> Result<Vec<ChangeLogRow>, DbError> {
        let rows = self.db.all(
            r#"SELECT stateVersion, pos, "table", rowKey, op
               FROM "_zero.changeLog2"
               WHERE stateVersion > ?
               ORDER BY stateVersion, pos"#,
            &[Value::Text(after_version.to_string())],
        )?;
        rows.into_iter()
            .map(|r| {
                Ok(ChangeLogRow {
                    state_version: text(&r[0].1),
                    pos: match r[1].1 {
                        Value::Integer(n) => n,
                        _ => 0,
                    },
                    table: text(&r[2].1),
                    row_key: text(&r[3].1),
                    op: text(&r[4].1),
                })
            })
            .collect()
    }

    fn log_row_op(
        &self,
        version: &str,
        pos: i64,
        table: &str,
        row: &RowKey,
        op: &str,
        backfilled: Option<&[String]>,
    ) -> Result<String, DbError> {
        let row_key = row_key_string(row)?;
        match backfilled {
            None => {
                self.db.run(
                    r#"INSERT OR REPLACE INTO "_zero.changeLog2"
                       (stateVersion, pos, "table", rowKey, op)
                       VALUES (?, ?, ?, JSON(?), ?)"#,
                    &[
                        Value::Text(version.to_string()),
                        Value::Integer(pos),
                        Value::Text(table.to_string()),
                        Value::Text(row_key.clone()),
                        Value::Text(op.to_string()),
                    ],
                )?;
            }
            Some(cols) => {
                let versions = stringify(&JsonValue::Object(
                    cols.iter()
                        .map(|c| (c.clone(), JsonValue::String(version.to_string())))
                        .collect(),
                ));
                self.db.run(
                    r#"INSERT INTO "_zero.changeLog2"
                       (stateVersion, pos, "table", rowKey, op, backfillingColumnVersions)
                       VALUES (?, ?, ?, JSON(?), ?, JSON(?))
                       ON CONFLICT ("table", rowKey) DO UPDATE
                         SET stateVersion = excluded.stateVersion,
                             pos = excluded.pos,
                             op = excluded.op,
                             backfillingColumnVersions = json_patch(
                               backfillingColumnVersions, excluded.backfillingColumnVersions)"#,
                    &[
                        Value::Text(version.to_string()),
                        Value::Integer(pos),
                        Value::Text(table.to_string()),
                        Value::Text(row_key.clone()),
                        Value::Text(op.to_string()),
                        Value::Text(versions),
                    ],
                )?;
            }
        }
        Ok(row_key)
    }

    fn log_table_wide_op(&self, version: &str, table: &str, op: &str) -> Result<(), DbError> {
        self.db.run(
            r#"INSERT OR REPLACE INTO "_zero.changeLog2"
               (stateVersion, pos, "table", rowKey, op)
               VALUES (?, -1, ?, ?, ?)"#,
            &[
                Value::Text(version.to_string()),
                Value::Text(table.to_string()),
                Value::Text(version.to_string()),
                Value::Text(op.to_string()),
            ],
        )?;
        Ok(())
    }
}

/// Normalizes `row` (sort by column) and serializes it as a JSON object string.
fn row_key_string(row: &RowKey) -> Result<String, DbError> {
    let ordered = normalized_key_order(row).map_err(|e| DbError(e.to_string()))?;
    Ok(stringify(&JsonValue::Object(ordered)))
}

fn parse_string_map(json: &str) -> Result<BTreeMap<String, String>, DbError> {
    match parse(json).map_err(|e| DbError(e.to_string()))? {
        JsonValue::Object(entries) => Ok(entries
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    match v {
                        JsonValue::String(s) => s,
                        other => stringify(&other),
                    },
                )
            })
            .collect()),
        _ => Err(DbError("expected a JSON object".into())),
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
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db
    }

    /// Builds a row key from `(col, int)` pairs.
    fn rk(pairs: &[(&str, i64)]) -> RowKey {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), JsonValue::Number(*v as f64)))
            .collect()
    }

    /// Dumps the change log ordered by (stateVersion, pos) as tuples of
    /// (stateVersion, pos, table, rowKey, op, backfillingColumnVersions).
    fn dump(db: &StatementRunner) -> Vec<(String, i64, String, String, String, String)> {
        db.query_uncached(
            r#"SELECT stateVersion, pos, "table", rowKey, op, backfillingColumnVersions
               FROM "_zero.changeLog2" ORDER BY stateVersion, pos"#,
            &[],
        )
        .unwrap()
        .into_iter()
        .map(|r| {
            let n = match r[1].1 {
                Value::Integer(n) => n,
                _ => 0,
            };
            (
                text(&r[0].1),
                n,
                text(&r[2].1),
                text(&r[3].1),
                text(&r[4].1),
                text(&r[5].1),
            )
        })
        .collect()
    }

    #[test]
    fn read_since_returns_entries_after_watermark_in_order() {
        let db = setup();
        let cl = ChangeLog::new(&db);
        // Three commits at ascending versions.
        cl.log_set_op("01", 0, "foo", &rk(&[("id", 1)]), None)
            .unwrap();
        cl.log_set_op("02", 0, "foo", &rk(&[("id", 2)]), None)
            .unwrap();
        cl.log_delete_op("03", 0, "foo", &rk(&[("id", 1)])).unwrap();

        // A subscriber at watermark "01" sees only commits 02 and 03.
        let since_01 = cl.read_since("01").unwrap();
        assert_eq!(
            since_01,
            vec![
                ChangeLogRow {
                    state_version: "02".into(),
                    pos: 0,
                    table: "foo".into(),
                    row_key: r#"{"id":2}"#.into(),
                    op: "s".into()
                },
                ChangeLogRow {
                    state_version: "03".into(),
                    pos: 0,
                    table: "foo".into(),
                    row_key: r#"{"id":1}"#.into(),
                    op: "d".into()
                },
            ]
        );

        // A fresh subscriber (watermark "00") replays everything still present.
        // Note the change-log keeps only the latest op per (table,row): id=1's
        // set at 01 was replaced by its delete at 03, so only two rows remain.
        let all = cl.read_since("00").unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(
            all.iter()
                .map(|r| r.state_version.clone())
                .collect::<Vec<_>>(),
            ["02", "03"]
        );

        // A caught-up subscriber (watermark at the latest) sees nothing.
        assert!(cl.read_since("03").unwrap().is_empty());
    }

    #[test]
    fn change_log_set_and_delete() {
        let db = setup();
        let cl = ChangeLog::new(&db);

        assert_eq!(
            cl.log_set_op("01", 0, "foo", &rk(&[("a", 1), ("b", 2)]), None)
                .unwrap(),
            r#"{"a":1,"b":2}"#
        );
        // Column order in the input does not matter (normalized).
        assert_eq!(
            cl.log_set_op("01", 1, "foo", &rk(&[("b", 3), ("a", 2)]), None)
                .unwrap(),
            r#"{"a":2,"b":3}"#
        );
        assert_eq!(
            cl.log_set_op("01", 2, "bar", &rk(&[("b", 2), ("a", 1)]), None)
                .unwrap(),
            r#"{"a":1,"b":2}"#
        );
        assert_eq!(
            cl.log_set_op("01", 3, "bar", &rk(&[("a", 2), ("b", 3)]), None)
                .unwrap(),
            r#"{"a":2,"b":3}"#
        );

        assert_eq!(
            dump(&db),
            vec![
                (
                    "01".into(),
                    0,
                    "foo".into(),
                    r#"{"a":1,"b":2}"#.into(),
                    "s".into(),
                    "{}".into()
                ),
                (
                    "01".into(),
                    1,
                    "foo".into(),
                    r#"{"a":2,"b":3}"#.into(),
                    "s".into(),
                    "{}".into()
                ),
                (
                    "01".into(),
                    2,
                    "bar".into(),
                    r#"{"a":1,"b":2}"#.into(),
                    "s".into(),
                    "{}".into()
                ),
                (
                    "01".into(),
                    3,
                    "bar".into(),
                    r#"{"a":2,"b":3}"#.into(),
                    "s".into(),
                    "{}".into()
                ),
            ]
        );

        // Delete moves the (bar, {a:2,b:3}) entry to version 02 (UNIQUE table+rowKey).
        assert_eq!(
            cl.log_delete_op("02", 0, "bar", &rk(&[("b", 3), ("a", 2)]))
                .unwrap(),
            r#"{"a":2,"b":3}"#
        );
        assert_eq!(
            dump(&db),
            vec![
                (
                    "01".into(),
                    0,
                    "foo".into(),
                    r#"{"a":1,"b":2}"#.into(),
                    "s".into(),
                    "{}".into()
                ),
                (
                    "01".into(),
                    1,
                    "foo".into(),
                    r#"{"a":2,"b":3}"#.into(),
                    "s".into(),
                    "{}".into()
                ),
                (
                    "01".into(),
                    2,
                    "bar".into(),
                    r#"{"a":1,"b":2}"#.into(),
                    "s".into(),
                    "{}".into()
                ),
                (
                    "02".into(),
                    0,
                    "bar".into(),
                    r#"{"a":2,"b":3}"#.into(),
                    "d".into(),
                    "{}".into()
                ),
            ]
        );
    }

    #[test]
    fn table_wide_ops() {
        let db = setup();
        let cl = ChangeLog::new(&db);
        cl.log_truncate_op("05", "foo").unwrap();
        cl.log_reset_op("06", "bar").unwrap();
        let rows = dump(&db);
        assert!(rows.contains(&(
            "05".into(),
            -1,
            "foo".into(),
            "05".into(),
            "t".into(),
            "{}".into()
        )));
        assert!(rows.contains(&(
            "06".into(),
            -1,
            "bar".into(),
            "06".into(),
            "r".into(),
            "{}".into()
        )));
    }

    #[test]
    fn get_latest_row_op() {
        let db = setup();
        let cl = ChangeLog::new(&db);
        cl.log_set_op(
            "123",
            0,
            "foo",
            &rk(&[("a", 1), ("b", 2)]),
            Some(&["c".into(), "b".into()]),
        )
        .unwrap();
        cl.log_set_op("123", 1, "bar", &rk(&[("b", 1), ("a", 2)]), None)
            .unwrap();

        assert_eq!(
            cl.get_latest_row_op("bar", &rk(&[("a", 3), ("b", 4)]))
                .unwrap(),
            None
        );

        let foo = cl
            .get_latest_row_op("foo", &rk(&[("b", 2), ("a", 1)]))
            .unwrap()
            .unwrap();
        assert_eq!(foo.state_version, "123");
        assert_eq!(foo.table, "foo");
        assert_eq!(foo.row_key, r#"{"a":1,"b":2}"#);
        assert_eq!(foo.op, "s");
        assert_eq!(
            foo.backfilling_column_versions,
            BTreeMap::from([
                ("b".to_string(), "123".to_string()),
                ("c".to_string(), "123".to_string())
            ])
        );

        let bar = cl
            .get_latest_row_op("bar", &rk(&[("a", 2), ("b", 1)]))
            .unwrap()
            .unwrap();
        assert_eq!(bar.row_key, r#"{"a":2,"b":1}"#);
        assert!(bar.backfilling_column_versions.is_empty());
    }
}
