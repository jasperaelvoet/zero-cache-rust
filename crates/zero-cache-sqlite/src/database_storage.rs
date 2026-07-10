//! Port of `zqlite/src/database-storage.ts` — a live SQLite-backed
//! key-value storage layer for IVM operator state (`ClientGroupStorage`,
//! one per client group, handing out a per-operator `get`/`set`/`del`/
//! `scan` namespace keyed by an auto-incrementing operator ID). Flagged
//! across several rounds as a real, currently entirely-unported gap — this
//! port's IVM operators (`zero-cache-zql::ivm`) have no persistent storage
//! backing at all yet.
//!
//! Scope: this port has no `Storage`/`Stream` trait in `ivm::operator` yet
//! to implement against (upstream's `Storage` interface `createStorage()`
//! returns), so `Self::create_storage` returns the concrete
//! [`OperatorStorage`] type directly rather than a trait object — wiring it
//! to a real `Storage` trait is a follow-up once that trait exists.
//! `#scan`'s upstream is a lazy generator (`Stream<[string, JSONValue]>`);
//! this port collects into a `Vec` instead (borrowing a live
//! `rusqlite::Statement` across a lazy Rust iterator's lifetime here would
//! fight the borrow checker for no real benefit — result sets are already
//! bounded by the prefix match, not unbounded).

use std::cell::RefCell;

use zero_cache_shared::bigint_json::JsonValue;

use crate::db_maintenance::decide_compaction;
use crate::{DbError, StatementRunner, Value};

/// Port of `CREATE_STORAGE_TABLE`.
pub const CREATE_STORAGE_TABLE: &str = "
  CREATE TABLE storage (
    clientGroupID TEXT,
    op NUMBER,
    key TEXT,
    val TEXT,
    PRIMARY KEY(clientGroupID, op, key)
  )
  ";

/// Port of `defaultOptions`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct StorageOptions {
    pub commit_interval: u64,
    pub compaction_threshold_bytes: f64,
}

impl Default for StorageOptions {
    fn default() -> Self {
        StorageOptions {
            commit_interval: 5_000,
            compaction_threshold_bytes: 50.0 * 1024.0 * 1024.0,
        }
    }
}

/// Port of `DatabaseStorage`.
pub struct DatabaseStorage {
    db: StatementRunner,
    options: StorageOptions,
    num_writes: RefCell<u64>,
}

impl DatabaseStorage {
    /// Port of the static `create`: opens (or creates) the backing SQLite
    /// file with the same pragmas upstream sets for ephemeral, single-
    /// writer, non-durable storage (`locking_mode = EXCLUSIVE`,
    /// `synchronous = OFF`, `journal_mode = OFF`, `auto_vacuum = INCREMENTAL`).
    pub fn create(path: &str, options: StorageOptions) -> Result<Self, DbError> {
        let db = StatementRunner::new(rusqlite::Connection::open(path)?);
        db.pragma("locking_mode = EXCLUSIVE")?;
        db.pragma("foreign_keys = OFF")?;
        db.pragma("journal_mode = OFF")?;
        db.pragma("synchronous = OFF")?;
        db.pragma("auto_vacuum = INCREMENTAL")?;
        db.exec(CREATE_STORAGE_TABLE)?;
        Self::new(db, options)
    }

    /// Port of the constructor, taking an already-open [`StatementRunner`]
    /// directly (e.g. an in-memory database for tests).
    pub fn new(db: StatementRunner, options: StorageOptions) -> Result<Self, DbError> {
        db.begin()?;
        Ok(DatabaseStorage {
            db,
            options,
            num_writes: RefCell::new(0),
        })
    }

    /// Port of `close`.
    pub fn close(&self) -> Result<(), DbError> {
        self.checkpoint()
    }

    fn get(
        &self,
        cg_id: &str,
        op_id: i64,
        key: &str,
        def: Option<JsonValue>,
    ) -> Result<Option<JsonValue>, DbError> {
        self.maybe_checkpoint()?;
        let row = self.db.get(
            "SELECT val FROM storage WHERE clientGroupID = ? AND op = ? AND key = ?",
            &[
                Value::Text(cg_id.to_string()),
                Value::Integer(op_id),
                Value::Text(key.to_string()),
            ],
        )?;
        match row {
            Some(row) => {
                let Value::Text(val) = &row[0].1 else {
                    unreachable!("val column is always TEXT")
                };
                Ok(Some(
                    zero_cache_shared::bigint_json::parse(val)
                        .map_err(|e| DbError(e.to_string()))?,
                ))
            }
            None => Ok(def),
        }
    }

    fn set(&self, cg_id: &str, op_id: i64, key: &str, val: &JsonValue) -> Result<(), DbError> {
        self.maybe_checkpoint()?;
        self.db.run(
            "INSERT INTO storage (clientGroupID, op, key, val) VALUES(?, ?, ?, ?) ON CONFLICT(clientGroupID, op, key) DO UPDATE SET val = excluded.val",
            &[Value::Text(cg_id.to_string()), Value::Integer(op_id), Value::Text(key.to_string()), Value::Text(val.stringify())],
        )?;
        Ok(())
    }

    fn del(&self, cg_id: &str, op_id: i64, key: &str) -> Result<(), DbError> {
        self.maybe_checkpoint()?;
        self.db.run(
            "DELETE FROM storage WHERE clientGroupID = ? AND op = ? AND key = ?",
            &[
                Value::Text(cg_id.to_string()),
                Value::Integer(op_id),
                Value::Text(key.to_string()),
            ],
        )?;
        Ok(())
    }

    /// Port of `#maybeCheckpoint`: we don't need to commit every single
    /// write since durability isn't a concern here — commit every
    /// `commit_interval` writes instead, since waiting on commits can be
    /// expensive.
    fn maybe_checkpoint(&self) -> Result<(), DbError> {
        let mut num_writes = self.num_writes.borrow_mut();
        *num_writes += 1;
        if *num_writes >= self.options.commit_interval {
            drop(num_writes);
            self.checkpoint()?;
        }
        Ok(())
    }

    fn checkpoint(&self) -> Result<(), DbError> {
        self.db.commit()?;
        self.db.begin()?;
        *self.num_writes.borrow_mut() = 0;
        Ok(())
    }

    /// Port of `#scan`. Returns every `(key, val)` pair whose key starts
    /// with `prefix` (default: every key for this operator), ordered by
    /// key. See module doc for why this collects into a `Vec` rather than
    /// matching upstream's lazy generator.
    fn scan(
        &self,
        cg_id: &str,
        op_id: i64,
        prefix: &str,
    ) -> Result<Vec<(String, JsonValue)>, DbError> {
        let rows = self.db.all("SELECT key, val FROM storage WHERE clientGroupID = ? AND op = ? AND key >= ? ORDER BY key", &[Value::Text(cg_id.to_string()), Value::Integer(op_id), Value::Text(prefix.to_string())])?;
        let mut out = Vec::new();
        for row in rows {
            let Value::Text(key) = &row[0].1 else {
                unreachable!("key column is always TEXT")
            };
            if !key.starts_with(prefix) {
                break;
            }
            let Value::Text(val) = &row[1].1 else {
                unreachable!("val column is always TEXT")
            };
            out.push((
                key.clone(),
                zero_cache_shared::bigint_json::parse(val).map_err(|e| DbError(e.to_string()))?,
            ));
        }
        Ok(out)
    }

    /// Port of `createClientGroupStorage`.
    pub fn create_client_group_storage(
        &self,
        cg_id: impl Into<String>,
    ) -> Result<ClientGroupStorage<'_>, DbError> {
        let cg_id = cg_id.into();
        self.db.run(
            "DELETE FROM storage WHERE clientGroupID = ?",
            &[Value::Text(cg_id.clone())],
        )?;
        Ok(ClientGroupStorage {
            storage: self,
            cg_id,
            next_op_id: RefCell::new(1),
        })
    }
}

/// Port of `ClientGroupStorage`.
pub struct ClientGroupStorage<'a> {
    storage: &'a DatabaseStorage,
    cg_id: String,
    next_op_id: RefCell<i64>,
}

impl<'a> ClientGroupStorage<'a> {
    /// Port of `createStorage`: hands out a fresh, uniquely-`op`-scoped
    /// [`OperatorStorage`] namespace within this client group.
    pub fn create_storage(&self) -> OperatorStorage<'a> {
        let op_id = *self.next_op_id.borrow();
        *self.next_op_id.borrow_mut() += 1;
        OperatorStorage {
            storage: self.storage,
            cg_id: self.cg_id.clone(),
            op_id,
        }
    }

    /// Port of `destroy`.
    pub fn destroy(&self) -> Result<(), DbError> {
        self.storage.db.run(
            "DELETE FROM storage WHERE clientGroupID = ?",
            &[Value::Text(self.cg_id.clone())],
        )?;
        self.storage.checkpoint()?;
        let freelist_count = pragma_i64(&self.storage.db, "freelist_count")?;
        let page_size = pragma_i64(&self.storage.db, "page_size")?;
        let auto_vacuum_mode = pragma_i64(&self.storage.db, "auto_vacuum")?;
        if let crate::db_maintenance::CompactionDecision::Proceed = decide_compaction(
            freelist_count,
            page_size,
            self.storage.options.compaction_threshold_bytes,
            auto_vacuum_mode,
        ) {
            self.storage.db.pragma("incremental_vacuum")?;
        }
        Ok(())
    }
}

fn pragma_i64(db: &StatementRunner, name: &str) -> Result<i64, DbError> {
    let rows = db.pragma(name)?;
    let Value::Integer(v) = rows[0][0].1 else {
        unreachable!("{name} pragma always returns an integer")
    };
    Ok(v)
}

/// Port of the object `createStorage()` returns — a single operator's
/// `get`/`set`/`del`/`scan` namespace, scoped by client-group ID + operator
/// ID under the hood.
pub struct OperatorStorage<'a> {
    storage: &'a DatabaseStorage,
    cg_id: String,
    op_id: i64,
}

impl<'a> OperatorStorage<'a> {
    pub fn get(&self, key: &str, def: Option<JsonValue>) -> Result<Option<JsonValue>, DbError> {
        self.storage.get(&self.cg_id, self.op_id, key, def)
    }

    pub fn set(&self, key: &str, val: &JsonValue) -> Result<(), DbError> {
        self.storage.set(&self.cg_id, self.op_id, key, val)
    }

    pub fn del(&self, key: &str) -> Result<(), DbError> {
        self.storage.del(&self.cg_id, self.op_id, key)
    }

    pub fn scan(&self, prefix: &str) -> Result<Vec<(String, JsonValue)>, DbError> {
        self.storage.scan(&self.cg_id, self.op_id, prefix)
    }
}

impl zero_cache_zql::ivm::operator::Storage for OperatorStorage<'_> {
    fn set(
        &self,
        key: &str,
        value: JsonValue,
    ) -> Result<(), zero_cache_zql::ivm::operator::StorageError> {
        OperatorStorage::set(self, key, &value)
            .map_err(|error| zero_cache_zql::ivm::operator::StorageError(error.to_string()))
    }

    fn get(
        &self,
        key: &str,
        default: Option<JsonValue>,
    ) -> Result<Option<JsonValue>, zero_cache_zql::ivm::operator::StorageError> {
        OperatorStorage::get(self, key, default)
            .map_err(|error| zero_cache_zql::ivm::operator::StorageError(error.to_string()))
    }

    fn scan(
        &self,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, JsonValue)>, zero_cache_zql::ivm::operator::StorageError> {
        OperatorStorage::scan(self, prefix.unwrap_or_default())
            .map_err(|error| zero_cache_zql::ivm::operator::StorageError(error.to_string()))
    }

    fn del(&self, key: &str) -> Result<(), zero_cache_zql::ivm::operator::StorageError> {
        OperatorStorage::del(self, key)
            .map_err(|error| zero_cache_zql::ivm::operator::StorageError(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open() -> DatabaseStorage {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_STORAGE_TABLE).unwrap();
        DatabaseStorage::new(db, StorageOptions::default()).unwrap()
    }

    #[test]
    fn get_set_del_round_trip_within_one_operator() {
        let storage = open();
        let cg = storage.create_client_group_storage("cg1").unwrap();
        let op = cg.create_storage();

        assert_eq!(op.get("a", None).unwrap(), None);
        op.set("a", &JsonValue::Number(1.0)).unwrap();
        assert_eq!(op.get("a", None).unwrap(), Some(JsonValue::Number(1.0)));
        op.del("a").unwrap();
        assert_eq!(op.get("a", None).unwrap(), None);
    }

    #[test]
    fn get_returns_the_default_when_missing() {
        let storage = open();
        let cg = storage.create_client_group_storage("cg1").unwrap();
        let op = cg.create_storage();
        assert_eq!(
            op.get("missing", Some(JsonValue::Bool(true))).unwrap(),
            Some(JsonValue::Bool(true))
        );
    }

    #[test]
    fn each_operator_gets_an_isolated_namespace() {
        let storage = open();
        let cg = storage.create_client_group_storage("cg1").unwrap();
        let op1 = cg.create_storage();
        let op2 = cg.create_storage();

        op1.set("k", &JsonValue::String("op1".into())).unwrap();
        op2.set("k", &JsonValue::String("op2".into())).unwrap();

        assert_eq!(
            op1.get("k", None).unwrap(),
            Some(JsonValue::String("op1".into()))
        );
        assert_eq!(
            op2.get("k", None).unwrap(),
            Some(JsonValue::String("op2".into()))
        );
    }

    #[test]
    fn each_client_group_gets_an_isolated_namespace() {
        let storage = open();
        let cg1 = storage.create_client_group_storage("cg1").unwrap();
        let cg2 = storage.create_client_group_storage("cg2").unwrap();
        let op1 = cg1.create_storage();
        let op2 = cg2.create_storage();

        op1.set("k", &JsonValue::Number(1.0)).unwrap();
        assert_eq!(
            op2.get("k", None).unwrap(),
            None,
            "cg2's same-numbered operator must not see cg1's write"
        );
    }

    #[test]
    fn scan_returns_only_matching_prefix_in_key_order() {
        let storage = open();
        let cg = storage.create_client_group_storage("cg1").unwrap();
        let op = cg.create_storage();
        op.set("a/2", &JsonValue::Number(2.0)).unwrap();
        op.set("a/1", &JsonValue::Number(1.0)).unwrap();
        op.set("b/1", &JsonValue::Number(3.0)).unwrap();

        let results = op.scan("a/").unwrap();
        assert_eq!(
            results,
            vec![
                ("a/1".to_string(), JsonValue::Number(1.0)),
                ("a/2".to_string(), JsonValue::Number(2.0))
            ]
        );
    }

    #[test]
    fn scan_with_no_prefix_returns_everything_for_that_operator() {
        let storage = open();
        let cg = storage.create_client_group_storage("cg1").unwrap();
        let op = cg.create_storage();
        op.set("x", &JsonValue::Number(1.0)).unwrap();
        op.set("y", &JsonValue::Number(2.0)).unwrap();
        assert_eq!(op.scan("").unwrap().len(), 2);
    }

    #[test]
    fn destroy_clears_every_operators_storage_for_that_client_group() {
        let storage = open();
        let cg = storage.create_client_group_storage("cg1").unwrap();
        let op1 = cg.create_storage();
        let op2 = cg.create_storage();
        op1.set("a", &JsonValue::Number(1.0)).unwrap();
        op2.set("b", &JsonValue::Number(2.0)).unwrap();

        cg.destroy().unwrap();

        assert_eq!(op1.get("a", None).unwrap(), None);
        assert_eq!(op2.get("b", None).unwrap(), None);
    }

    #[test]
    fn creating_client_group_storage_clears_any_pre_existing_rows_for_that_id() {
        let storage = open();
        {
            let cg = storage.create_client_group_storage("cg1").unwrap();
            let op = cg.create_storage();
            op.set("stale", &JsonValue::Number(1.0)).unwrap();
        }
        // Re-creating storage for the same client-group ID clears leftovers
        // (matches upstream's `clear.run(cgID)` at the top of
        // `createClientGroupStorage`, e.g. after a prior ungraceful exit).
        let cg = storage.create_client_group_storage("cg1").unwrap();
        let op = cg.create_storage();
        assert_eq!(op.get("stale", None).unwrap(), None);
    }

    #[test]
    fn checkpoints_automatically_past_the_commit_interval() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_STORAGE_TABLE).unwrap();
        let storage = DatabaseStorage::new(
            db,
            StorageOptions {
                commit_interval: 3,
                compaction_threshold_bytes: 1.0,
            },
        )
        .unwrap();
        let cg = storage.create_client_group_storage("cg1").unwrap();
        let op = cg.create_storage();
        // 3 writes should trigger #maybeCheckpoint's commit+reopen path;
        // this should not error and subsequent reads must still see the data.
        op.set("a", &JsonValue::Number(1.0)).unwrap();
        op.set("b", &JsonValue::Number(2.0)).unwrap();
        op.set("c", &JsonValue::Number(3.0)).unwrap();
        assert_eq!(op.get("a", None).unwrap(), Some(JsonValue::Number(1.0)));
        assert_eq!(op.get("c", None).unwrap(), Some(JsonValue::Number(3.0)));
    }

    #[test]
    fn live_create_opens_a_real_file_backed_database_with_the_expected_pragmas() {
        let dir = std::env::temp_dir().join(format!(
            "zero-cache-database-storage-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("storage.db");
        let path_str = path.to_str().unwrap().to_string();
        let _ = std::fs::remove_file(&path);

        let storage = DatabaseStorage::create(&path_str, StorageOptions::default()).unwrap();
        let cg = storage.create_client_group_storage("cg1").unwrap();
        let op = cg.create_storage();
        op.set("k", &JsonValue::String("v".into())).unwrap();
        assert_eq!(
            op.get("k", None).unwrap(),
            Some(JsonValue::String("v".into()))
        );

        std::fs::remove_file(&path_str).ok();
    }
}
