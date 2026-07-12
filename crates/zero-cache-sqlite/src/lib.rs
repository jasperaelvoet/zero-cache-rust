//! SQLite wrapper for zero-cache.
//!
//! Ports the parts of the `zqlite` package's `Database`/`StatementCache` that
//! zero-cache uses, plus `zero-cache/src/db/statements.ts`'s `StatementRunner`.
//! Backed by [`rusqlite`] linked to the pinned `@rocicorp/zero-sqlite3` v1.1.2
//! engine. That engine supplies the WAL2, `BEGIN CONCURRENT`, scan-status, and
//! compile-time behavior required by Zero v1.7.

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
pub mod pg_copy_text;
pub mod pipeline;
pub mod query_builder;
pub mod replication_apply;
pub mod replication_state;
pub mod replication_supervisor;
pub mod replicator_setup;
pub mod resolve_scalar_subqueries;
pub mod row_apply;
pub mod runtime_events;
#[cfg(feature = "scanstatus")]
pub mod scanstatus;
pub mod shadow_sync;
pub mod snapshotter;
pub mod sql_inline;
pub mod sqlite_cost_model;
pub mod sqlite_source;
pub mod sqlite_stat_fanout;
pub mod sqlite_table_source;
pub mod statement_cache;
pub mod streamed_apply;
pub mod subscriber_catchup;
pub mod table_metadata;
pub mod zero_sqlite;

pub use database_storage::DatabaseStorage;
pub use rusqlite::types::Value;
use rusqlite::Connection;
pub use sqlite_source::SqliteSource;
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

/// Whether an error is SQLite refusing to convert an existing database between
/// `wal` and `wal2` journal modes in place (either direction) — the signal
/// that an on-disk replica was left in an incompatible journal mode.
fn is_incompatible_journal_mode(e: &DbError) -> bool {
    let msg = e.0.to_ascii_lowercase();
    msg.contains("cannot change from wal to wal2") || msg.contains("cannot change from wal2 to wal")
}

/// Removes a replica file and its WAL sidecars (`-wal`, `-wal2`, `-shm`).
/// Best-effort: a missing file is not an error.
fn remove_replica_files(path: &str) {
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
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

    /// Opens (creating if absent) a file-backed database as the WRITER, in WAL2
    /// mode so concurrent readers ([`open_file_readonly`](Self::open_file_readonly))
    /// can read the same file while this connection writes. This is the shared
    /// replica the replicator owns and the view-syncer connections read.
    ///
    /// WAL2 plus `BEGIN CONCURRENT` is the snapshot model used by Zero v1.7.
    ///
    /// SQLite cannot convert an existing database between `wal` and `wal2`
    /// journal modes in place. A replica file left in plain `wal` mode by a
    /// prior deploy (e.g. an official zero-cache image, or a Postgres-provider
    /// export) therefore makes `PRAGMA journal_mode = WAL2` fail. Because the
    /// replica is disposable — always rebuilt from upstream by initial sync —
    /// an incompatible pre-existing file is wiped and recreated fresh rather
    /// than treated as a fatal startup error.
    pub fn open_file(path: &str) -> Result<Self, DbError> {
        match Self::try_open_file_wal2(path) {
            Ok(runner) => Ok(runner),
            Err(e) if is_incompatible_journal_mode(&e) => {
                // The on-disk file is in an incompatible journal mode. Remove
                // it and its WAL sidecars, then recreate empty in WAL2 (initial
                // sync repopulates it). A view-syncer never hits this — it only
                // opens an already-WAL2 replica read-only.
                remove_replica_files(path);
                Self::try_open_file_wal2(path)
            }
            Err(e) => Err(e),
        }
    }

    fn try_open_file_wal2(path: &str) -> Result<Self, DbError> {
        let conn = Connection::open(path)?;
        zero_sqlite::install_unicode_case_functions(&conn)?;
        zero_sqlite::verify_engine(&conn)?;
        let mode: String = conn.query_row("PRAGMA journal_mode = WAL2", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal2") {
            return Err(DbError(format!(
                "Zero SQLite refused WAL2 journal mode (returned `{mode}`)"
            )));
        }
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
        zero_sqlite::install_unicode_case_functions(&conn)?;
        zero_sqlite::verify_engine(&conn)?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        Ok(Self::new(conn))
    }

    /// Opens a writable connection used only for a view-syncer's ephemeral
    /// `BEGIN CONCURRENT` snapshot. All simulated writes are rolled back.
    pub fn open_snapshot(path: &str, page_cache_size_kib: Option<usize>) -> Result<Self, DbError> {
        use rusqlite::OpenFlags;
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
        )?;
        zero_sqlite::install_unicode_case_functions(&conn)?;
        zero_sqlite::verify_engine(&conn)?;
        let mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal2") {
            return Err(DbError(format!(
                "replica db must be in wal2 mode (current: {mode})"
            )));
        }
        conn.pragma_update(None, "synchronous", "OFF")?;
        if let Some(kib) = page_cache_size_kib {
            conn.pragma_update(None, "cache_size", -(kib as i64))?;
        }
        let db = Self::new(conn);
        db.begin_concurrent()?;
        Ok(db)
    }

    /// Opens an in-memory database (`:memory:`).
    pub fn open_in_memory() -> Result<Self, DbError> {
        let conn = Connection::open_in_memory()?;
        zero_sqlite::install_unicode_case_functions(&conn)?;
        zero_sqlite::verify_engine(&conn)?;
        Ok(Self::new(conn))
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

    #[test]
    fn open_file_self_heals_a_wal_mode_replica_into_wal2() {
        // A prior deploy (e.g. official zero-cache) can leave the replica in
        // plain `wal` mode; SQLite cannot convert it to `wal2` in place. The
        // disposable replica must be wiped and recreated rather than fail
        // startup — reproduce that exact situation and prove recovery.
        let dir = std::env::temp_dir().join(format!("zc_wal_heal_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("replica.db");
        let path_str = path.to_str().unwrap();

        // Create a NON-empty database in plain `wal` mode (so an in-place
        // switch to wal2 is genuinely refused, as on the persistent volume).
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            let mode: String = conn
                .query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))
                .unwrap();
            assert_eq!(mode, "wal");
            conn.execute_batch("CREATE TABLE t(x); INSERT INTO t VALUES (1);")
                .unwrap();
        }

        // A direct wal2 open must fail on this file…
        assert!(
            StatementRunner::try_open_file_wal2(path_str).is_err(),
            "expected wal->wal2 conversion to be refused in place"
        );
        // …but open_file self-heals: wipes and recreates in wal2.
        let db = StatementRunner::open_file(path_str).expect("open_file should self-heal");
        let mode: String = db
            .query_uncached("PRAGMA journal_mode", &[])
            .unwrap()
            .into_iter()
            .next()
            .and_then(|row| match row.into_iter().next() {
                Some((_, Value::Text(m))) => Some(m),
                _ => None,
            })
            .unwrap();
        assert!(mode.eq_ignore_ascii_case("wal2"), "mode was {mode}");
        // The stale table is gone (fresh db); initial sync would repopulate.
        assert!(db.query_uncached("SELECT x FROM t", &[]).is_err());

        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
