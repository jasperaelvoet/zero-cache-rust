//! SQLite wrapper for zero-cache.
//!
//! Ports the parts of the `zqlite` package's `Database`/`StatementCache` that
//! zero-cache uses, plus `zero-cache/src/db/statements.ts`'s `StatementRunner`.
//! Backed by [`rusqlite`] (bundled SQLite).
//!
//! Note: the upstream stack uses the `@rocicorp/zero-sqlite3` fork, which adds
//! `BEGIN CONCURRENT`. Vanilla SQLite lacks it; [`StatementRunner::begin`]
//! (standard) is the portable path, and [`StatementRunner::begin_concurrent`]
//! issues `BEGIN CONCURRENT` verbatim for use against the fork.

pub mod change_dispatcher;
pub mod change_fanout;
pub mod change_log;
pub mod change_stream_loop;
pub mod change_streamer_service;
pub mod column_metadata;
pub mod create;
pub mod database_storage;
pub mod db_maintenance;
pub mod ddl_apply;
pub mod explain_queries;
pub mod initial_sync;
pub mod initial_sync_copy;
pub mod initial_sync_metrics;
pub mod ivm_bridge;
pub mod lite_tables;
pub mod migration;
pub mod pipeline;
pub mod query_builder;
pub mod replication_apply;
pub mod replication_state;
pub mod replication_supervisor;
pub mod replicator_setup;
pub mod resolve_scalar_subqueries;
pub mod row_apply;
#[cfg(feature = "scanstatus")]
pub mod scanstatus;
pub mod sql_inline;
pub mod sqlite_cost_model;
pub mod sqlite_stat_fanout;
pub mod sqlite_table_source;
pub mod statement_cache;
pub mod streamed_apply;
pub mod subscriber_catchup;
pub mod table_metadata;

pub use rusqlite::types::Value;
use rusqlite::Connection;
use std::cell::RefCell;
use std::collections::HashSet;
use thiserror::Error;

/// A database error.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct DbError(pub String);

impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError(e.to_string())
    }
}

/// The result of a `run` (non-query) statement. Port of better-sqlite3's
/// `RunResult`.
#[derive(Debug, Clone, PartialEq)]
pub struct RunResult {
    pub changes: u64,
    pub last_insert_rowid: i64,
}

/// A returned row: ordered `(column, value)` pairs.
pub type Row = Vec<(String, Value)>;

/// A cached-statement runner over a SQLite connection. Port of `StatementRunner`
/// (which wraps a `StatementCache`).
pub struct StatementRunner {
    conn: Connection,
    /// Distinct SQL strings prepared so far, tracked for [`Self::cache_size`].
    seen: RefCell<HashSet<String>>,
}

impl StatementRunner {
    /// Wraps an existing connection.
    pub fn new(conn: Connection) -> Self {
        StatementRunner {
            conn,
            seen: RefCell::new(HashSet::new()),
        }
    }

    /// Opens (creating if absent) a file-backed database as the WRITER, in WAL
    /// mode so concurrent readers ([`open_file_readonly`](Self::open_file_readonly))
    /// can read the same file while this connection writes. This is the shared
    /// replica the replicator owns and the view-syncer connections read.
    ///
    /// WAL is the mode upstream zero-cache runs its SQLite replica in; a 5 s
    /// `busy_timeout` avoids spurious `SQLITE_BUSY` under concurrent access.
    pub fn open_file(path: &str) -> Result<Self, DbError> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        Ok(Self::new(conn))
    }

    /// Opens a READ-ONLY connection to an existing WAL replica file — one per
    /// view-syncer connection, so many readers can query the shared replica
    /// concurrently while the replicator writes. Fails if the file does not
    /// exist (the writer must have created it first).
    pub fn open_file_readonly(path: &str) -> Result<Self, DbError> {
        use rusqlite::OpenFlags;
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        Ok(Self::new(conn))
    }

    /// Opens an in-memory database (`:memory:`).
    pub fn open_in_memory() -> Result<Self, DbError> {
        Ok(Self::new(Connection::open_in_memory()?))
    }

    /// Executes SQL directly (no caching, no params). Port of `Database.exec`.
    pub fn exec(&self, sql: &str) -> Result<(), DbError> {
        self.conn.execute_batch(sql)?;
        Ok(())
    }

    /// Extracts the `sqlite3_stmt_scanstatus_v2` loops for `sql` — the live
    /// scan statistics the ported query-planner cost model
    /// (`zero_cache_zql::planner_cost::estimate_cost`) consumes. Gated behind
    /// the off-by-default `scanstatus` feature (needs SQLite built with
    /// `SQLITE_ENABLE_STMT_SCANSTATUS`; see [`crate::scanstatus`]).
    #[cfg(feature = "scanstatus")]
    pub fn scanstatus_loops(
        &self,
        sql: &str,
    ) -> Result<Vec<crate::scanstatus::ScanstatusLoop>, String> {
        // SAFETY: `handle()` is this connection's live `sqlite3*`; `loops_for`
        // prepares/finalizes its own statement on it.
        unsafe { crate::scanstatus::loops_for(self.conn.handle(), sql) }
    }

    /// The number of distinct cached statements. Port of `StatementCache.size`.
    pub fn cache_size(&self) -> usize {
        self.seen.borrow().len()
    }

    fn record(&self, sql: &str) {
        if !self.seen.borrow().contains(sql) {
            self.seen.borrow_mut().insert(sql.to_string());
        }
    }

    /// Prepares (or reuses) a statement and runs it. Port of `run`.
    pub fn run(&self, sql: &str, params: &[Value]) -> Result<RunResult, DbError> {
        self.record(sql);
        let mut stmt = self.conn.prepare_cached(sql)?;
        let changes = stmt.execute(rusqlite::params_from_iter(params.iter()))?;
        Ok(RunResult {
            changes: changes as u64,
            last_insert_rowid: self.conn.last_insert_rowid(),
        })
    }

    /// Prepares (or reuses) a statement and returns the first row. Port of `get`.
    pub fn get(&self, sql: &str, params: &[Value]) -> Result<Option<Row>, DbError> {
        self.record(sql);
        let mut stmt = self.conn.prepare_cached(sql)?;
        let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = stmt.query(rusqlite::params_from_iter(params.iter()))?;
        match rows.next()? {
            Some(row) => Ok(Some(read_row(row, &cols)?)),
            None => Ok(None),
        }
    }

    /// Prepares (or reuses) a statement and returns all rows. Port of `all`.
    pub fn all(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        self.record(sql);
        let mut stmt = self.conn.prepare_cached(sql)?;
        let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = stmt.query(rusqlite::params_from_iter(params.iter()))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(read_row(row, &cols)?);
        }
        Ok(out)
    }

    /// Runs `PRAGMA {pragma}` and returns the resulting rows. Port of
    /// `Database.pragma`.
    pub fn pragma(&self, pragma: &str) -> Result<Vec<Row>, DbError> {
        self.query_uncached(&format!("PRAGMA {pragma}"), &[])
    }

    /// Queries all rows without touching the statement cache. Used by test
    /// assertions that read the DB independently (like `expectTables`).
    pub fn query_uncached(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let mut stmt = self.conn.prepare(sql)?;
        let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let mut rows = stmt.query(rusqlite::params_from_iter(params.iter()))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(read_row(row, &cols)?);
        }
        Ok(out)
    }

    // ----- transaction convenience methods (port of the sugar methods) -----

    pub fn begin(&self) -> Result<RunResult, DbError> {
        self.run("BEGIN", &[])
    }

    /// Issues `BEGIN CONCURRENT` (requires the zero-sqlite3 fork).
    pub fn begin_concurrent(&self) -> Result<RunResult, DbError> {
        self.run("BEGIN CONCURRENT", &[])
    }

    pub fn begin_immediate(&self) -> Result<RunResult, DbError> {
        self.run("BEGIN IMMEDIATE", &[])
    }

    pub fn commit(&self) -> Result<RunResult, DbError> {
        self.run("COMMIT", &[])
    }

    pub fn rollback(&self) -> Result<RunResult, DbError> {
        self.run("ROLLBACK", &[])
    }
}

fn read_row(row: &rusqlite::Row<'_>, cols: &[String]) -> Result<Row, DbError> {
    let mut out = Vec::with_capacity(cols.len());
    for (i, name) in cols.iter().enumerate() {
        let value: Value = row.get(i)?;
        out.push((name.clone(), value));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(n: i64) -> Value {
        Value::Integer(n)
    }

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE foo(id INT PRIMARY KEY)").unwrap();
        db
    }

    /// Reads all `id`s from `foo` in order (helper mirroring `expectTables`).
    fn ids(db: &StatementRunner) -> Vec<i64> {
        db.query_uncached("SELECT id FROM foo ORDER BY id", &[])
            .unwrap()
            .into_iter()
            .map(|row| match row[0].1 {
                Value::Integer(n) => n,
                _ => panic!("expected integer id"),
            })
            .collect()
    }

    #[test]
    fn file_replica_writer_and_concurrent_reader() {
        // A unique temp path (no external tempfile dep; process id + a counter).
        let dir = std::env::temp_dir();
        let path = dir
            .join(format!("zc_replica_test_{}.db", std::process::id()))
            .to_string_lossy()
            .into_owned();
        // Clean any stale file + WAL sidecars.
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }

        // Writer creates the table in WAL mode and inserts.
        let writer = StatementRunner::open_file(&path).unwrap();
        writer
            .exec("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO t(id, v) VALUES (1, 'a')", &[])
            .unwrap();

        // A separate read-only connection sees the committed row while the
        // writer is still open (WAL concurrent read).
        let reader = StatementRunner::open_file_readonly(&path).unwrap();
        let rows = reader
            .query_uncached("SELECT v FROM t WHERE id = 1", &[])
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].1, Value::Text("a".into()));

        // A read-only connection cannot write.
        assert!(reader.exec("INSERT INTO t(id, v) VALUES (2, 'b')").is_err());

        drop(writer);
        drop(reader);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    #[test]
    fn statement_caching() {
        let db = setup();
        assert_eq!(db.cache_size(), 0);

        db.run("INSERT INTO foo(id) VALUES(?)", &[int(123)])
            .unwrap();
        assert_eq!(ids(&db), vec![123]);
        assert_eq!(db.cache_size(), 1);

        db.run("INSERT INTO foo(id) VALUES(?)", &[int(456)])
            .unwrap();
        assert_eq!(ids(&db), vec![123, 456]);
        // Same INSERT SQL -> still one cached statement.
        assert_eq!(db.cache_size(), 1);

        let first = db.get("SELECT * FROM foo", &[]).unwrap().unwrap();
        assert_eq!(first, vec![("id".to_string(), Value::Integer(123))]);
        assert_eq!(db.cache_size(), 2);

        let all = db.all("SELECT * FROM foo", &[]).unwrap();
        assert_eq!(
            all,
            vec![
                vec![("id".to_string(), Value::Integer(123))],
                vec![("id".to_string(), Value::Integer(456))],
            ]
        );
        assert_eq!(db.cache_size(), 2);
    }

    #[test]
    fn convenience_methods() {
        let db = setup();
        // Vanilla SQLite: use standard BEGIN (BEGIN CONCURRENT needs the fork).
        db.begin().unwrap();
        db.run("INSERT INTO foo(id) VALUES(?)", &[int(321)])
            .unwrap();
        db.run("INSERT INTO foo(id) VALUES(?)", &[int(456)])
            .unwrap();
        assert_eq!(ids(&db), vec![321, 456]);

        db.rollback().unwrap();
        assert_eq!(ids(&db), Vec::<i64>::new());

        db.begin().unwrap();
        db.run("INSERT INTO foo(id) VALUES(?)", &[int(987)])
            .unwrap();
        db.commit().unwrap();
        assert_eq!(ids(&db), vec![987]);
    }

    #[test]
    fn rollback_without_transaction_throws() {
        let db = setup();
        let err = db.rollback().unwrap_err();
        assert!(
            err.0.contains("cannot rollback - no transaction is active"),
            "unexpected error: {}",
            err.0
        );
    }
}
