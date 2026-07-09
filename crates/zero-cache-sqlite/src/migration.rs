//! Port of `zero-cache/src/db/migration-lite.ts`.
//!
//! A forward-only SQLite schema-migration framework with version bookkeeping in
//! a `_zero.versionHistory` table. Setup migrations bootstrap a blank database;
//! incremental migrations move an existing one forward.
//!
//! Differences from the TS source (behavior-preserving): operations are
//! synchronous closures rather than async; `LogContext` is dropped; the
//! performance-only pragmas (`journal_mode = OFF`, `locking_mode = EXCLUSIVE`,
//! etc.) are omitted; and the runner takes an open [`StatementRunner`] rather
//! than a path.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::{DbError, StatementRunner, Value};

/// A migration operation: runs SQL against the database, returning an error
/// message on failure (matching the TS `Operations` promise rejection).
pub type Operations = Box<dyn Fn(&StatementRunner) -> Result<(), String>>;

/// A single migration. Port of `Migration`.
#[derive(Default)]
pub struct Migration {
    pub migrate_schema: Option<Operations>,
    pub migrate_data: Option<Operations>,
    pub min_safe_version: Option<i64>,
}

/// Mapping of `destinationVersion -> Migration`. A `BTreeMap` keeps the
/// versions sorted ascending. Port of `IncrementalMigrationMap`.
pub type IncrementalMigrationMap = BTreeMap<i64, Migration>;

/// Version bookkeeping. Port of `VersionHistory`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionHistory {
    pub data_version: i64,
    pub schema_version: i64,
    pub min_safe_version: i64,
}

/// A failed `PRAGMA quick_check`. Port of `DatabaseIntegrityError`.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("SQLite quick_check failed for {debug_name}: {}", issues.join("; "))]
pub struct DatabaseIntegrityError {
    pub debug_name: String,
    pub issues: Vec<String>,
}

/// Errors from [`run_schema_migrations`].
#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("Cannot run {debug_name} at schema v{code_version} because rollback limit is v{min_safe_version}")]
    RollbackLimit {
        debug_name: String,
        code_version: i64,
        min_safe_version: i64,
    },
    #[error("{0}")]
    Op(String),
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Integrity(#[from] DatabaseIntegrityError),
    #[error("{0}")]
    Assert(String),
}

/// Reads (creating the table if needed) the version history. Port of
/// `getVersionHistory`.
pub fn get_version_history(db: &StatementRunner) -> Result<VersionHistory, DbError> {
    db.exec(
        r#"CREATE TABLE IF NOT EXISTS "_zero.versionHistory" (
            dataVersion INTEGER NOT NULL,
            schemaVersion INTEGER NOT NULL,
            minSafeVersion INTEGER NOT NULL,
            lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );"#,
    )?;
    let row = db.get(
        r#"SELECT dataVersion, schemaVersion, minSafeVersion FROM "_zero.versionHistory""#,
        &[],
    )?;
    match row {
        Some(r) => Ok(VersionHistory {
            data_version: int(&r, 0),
            schema_version: int(&r, 1),
            min_safe_version: int(&r, 2),
        }),
        None => Ok(VersionHistory {
            data_version: 0,
            schema_version: 0,
            min_safe_version: 0,
        }),
    }
}

fn int(row: &crate::Row, idx: usize) -> i64 {
    match row[idx].1 {
        Value::Integer(n) => n,
        _ => 0,
    }
}

/// Upserts the version history. Port of `updateVersionHistory`.
fn update_version_history(
    db: &StatementRunner,
    prev: VersionHistory,
    new_version: i64,
    min_safe_version: Option<i64>,
) -> Result<VersionHistory, MigrationError> {
    if new_version <= 0 {
        return Err(MigrationError::Assert("newVersion must be positive".into()));
    }
    let meta = VersionHistory {
        data_version: new_version,
        // schemaVersion never moves backwards.
        schema_version: new_version.max(prev.schema_version),
        min_safe_version: get_min_safe_version(prev, min_safe_version),
    };
    db.run(
        r#"INSERT INTO "_zero.versionHistory" (dataVersion, schemaVersion, minSafeVersion, lock)
           VALUES (?, ?, ?, 1)
           ON CONFLICT (lock) DO UPDATE
           SET dataVersion=excluded.dataVersion,
               schemaVersion=excluded.schemaVersion,
               minSafeVersion=excluded.minSafeVersion"#,
        &[
            Value::Integer(meta.data_version),
            Value::Integer(meta.schema_version),
            Value::Integer(meta.min_safe_version),
        ],
    )?;
    Ok(meta)
}

/// Bumps the rollback limit up to (never below) the proposed version. Port of
/// `getMinSafeVersion`.
fn get_min_safe_version(current: VersionHistory, proposed: Option<i64>) -> i64 {
    match proposed {
        None => current.min_safe_version,
        Some(p) if current.min_safe_version >= p => current.min_safe_version,
        Some(p) => p,
    }
}

fn run_migration(
    db: &StatementRunner,
    versions: VersionHistory,
    dest: i64,
    migration: &Migration,
) -> Result<VersionHistory, MigrationError> {
    if versions.schema_version < dest {
        if let Some(op) = &migration.migrate_schema {
            op(db).map_err(MigrationError::Op)?;
        }
    }
    if versions.data_version < dest {
        if let Some(op) = &migration.migrate_data {
            op(db).map_err(MigrationError::Op)?;
        }
    }
    update_version_history(db, versions, dest, migration.min_safe_version)
}

/// Runs `PRAGMA quick_check`, raising [`DatabaseIntegrityError`] on any issue.
/// Port of `assertDatabaseIntegrity`.
pub fn assert_database_integrity(
    db: &StatementRunner,
    debug_name: &str,
) -> Result<(), MigrationError> {
    let rows = db.pragma("quick_check")?;
    let issues: Vec<String> = if rows.is_empty() {
        vec!["PRAGMA quick_check returned no rows".into()]
    } else {
        rows.iter()
            .filter_map(|r| match &r[0].1 {
                Value::Text(s) if s != "ok" => Some(s.clone()),
                _ => None,
            })
            .collect()
    };
    if !issues.is_empty() {
        return Err(DatabaseIntegrityError {
            debug_name: debug_name.to_string(),
            issues,
        }
        .into());
    }
    Ok(())
}

/// Runs a closure inside a `BEGIN EXCLUSIVE` transaction, committing on success
/// and rolling back on error. Port of `runTransaction`.
fn run_transaction<T>(
    db: &StatementRunner,
    tx: impl FnOnce(&StatementRunner) -> Result<T, MigrationError>,
) -> Result<T, MigrationError> {
    db.run("BEGIN EXCLUSIVE", &[])?;
    match tx(db) {
        Ok(result) => {
            db.run("COMMIT", &[])?;
            Ok(result)
        }
        Err(e) => {
            if let Err(rollback_err) = db.run("ROLLBACK", &[]) {
                return Err(MigrationError::Op(format!(
                    "Transaction failed and rollback also failed: operation error = {e}; rollback error = {rollback_err}"
                )));
            }
            Err(e)
        }
    }
}

/// Ensures the schema is compatible with the current code, migrating as needed.
/// Port of `runSchemaMigrations`.
pub fn run_schema_migrations(
    db: &StatementRunner,
    debug_name: &str,
    setup_migration: &Migration,
    incremental: &IncrementalMigrationMap,
) -> Result<(), MigrationError> {
    if incremental.is_empty() {
        return Err(MigrationError::Assert(
            "Must specify a at least one version migration".into(),
        ));
    }
    let first_version = *incremental.keys().next().unwrap();
    if first_version <= 0 {
        return Err(MigrationError::Assert(
            "Versions must be non-zero positive numbers".into(),
        ));
    }
    let code_version = *incremental.keys().next_back().unwrap();

    let mut versions = run_transaction(db, |tx| {
        let v = get_version_history(tx)?;
        if code_version < v.min_safe_version {
            return Err(MigrationError::RollbackLimit {
                debug_name: debug_name.to_string(),
                code_version,
                min_safe_version: v.min_safe_version,
            });
        }
        if v.data_version > code_version {
            // Roll the data version back down to the code version.
            return update_version_history(tx, v, code_version, None);
        }
        Ok(v)
    })?;
    let initial_data_version = versions.data_version;

    if versions.data_version < code_version {
        // Build the migration list: setup-only for a blank db, else incremental.
        let steps: Vec<(i64, &Migration)> = if versions.data_version == 0 {
            vec![(code_version, setup_migration)]
        } else {
            incremental.iter().map(|(k, m)| (*k, m)).collect()
        };

        for (dest, migration) in steps {
            if versions.data_version < dest {
                versions = run_transaction(db, |tx| {
                    let mut v = get_version_history(tx)?;
                    if v.data_version < dest {
                        v = run_migration(tx, v, dest, migration)?;
                        if v.data_version != dest {
                            return Err(MigrationError::Assert(format!(
                                "Migration did not reach target version: expected {dest}, got {}",
                                v.data_version
                            )));
                        }
                    }
                    Ok(v)
                })?;
            }
        }

        if initial_data_version > 0 {
            assert_database_integrity(db, debug_name)?;
        }
    } else {
        assert_database_integrity(db, debug_name)?;
    }

    if versions.data_version != code_version {
        return Err(MigrationError::Assert(format!(
            "Final dataVersion ({}) does not match codeVersion ({code_version})",
            versions.data_version
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_db() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(r#"CREATE TABLE "MigrationHistory" (event TEXT)"#)
            .unwrap();
        db
    }

    fn set_pre_schema(db: &StatementRunner, v: VersionHistory) {
        get_version_history(db).unwrap(); // create table
        db.run(
            r#"INSERT INTO "_zero.versionHistory" (dataVersion, schemaVersion, minSafeVersion)
               VALUES (?, ?, ?)"#,
            &[
                Value::Integer(v.data_version),
                Value::Integer(v.schema_version),
                Value::Integer(v.min_safe_version),
            ],
        )
        .unwrap();
    }

    /// An op that records `"{name}-at({dataVersion})"` in MigrationHistory.
    fn log_history(name: &'static str) -> Operations {
        Box::new(move |db: &StatementRunner| {
            let meta = get_version_history(db).map_err(|e| e.to_string())?;
            db.run(
                "INSERT INTO MigrationHistory (event) VALUES (?)",
                &[Value::Text(format!("{name}-at({})", meta.data_version))],
            )
            .map_err(|e| e.to_string())?;
            Ok(())
        })
    }

    fn fails(msg: &'static str) -> Operations {
        Box::new(move |_db| Err(msg.to_string()))
    }

    fn history_events(db: &StatementRunner) -> Vec<String> {
        db.query_uncached("SELECT event FROM MigrationHistory", &[])
            .unwrap()
            .into_iter()
            .map(|r| match &r[0].1 {
                Value::Text(s) => s.clone(),
                _ => String::new(),
            })
            .collect()
    }

    fn vh(d: i64, s: i64, m: i64) -> VersionHistory {
        VersionHistory {
            data_version: d,
            schema_version: s,
            min_safe_version: m,
        }
    }

    #[test]
    fn sorts_and_runs_multiple_migrations() {
        let db = setup_db();
        set_pre_schema(&db, vh(2, 2, 1));
        let mut migrations = IncrementalMigrationMap::new();
        migrations.insert(
            5,
            Migration {
                migrate_schema: Some(log_history("second-schema")),
                migrate_data: Some(log_history("second-data")),
                min_safe_version: None,
            },
        );
        migrations.insert(
            4,
            Migration {
                migrate_schema: Some(log_history("first-schema")),
                ..Default::default()
            },
        );
        migrations.insert(
            7,
            Migration {
                min_safe_version: Some(2),
                ..Default::default()
            },
        );
        migrations.insert(
            8,
            Migration {
                migrate_schema: Some(log_history("third-schema")),
                ..Default::default()
            },
        );

        run_schema_migrations(&db, "debug-name", &Migration::default(), &migrations).unwrap();

        assert_eq!(get_version_history(&db).unwrap(), vh(8, 8, 2));
        assert_eq!(
            history_events(&db),
            vec![
                "first-schema-at(2)",
                "second-schema-at(4)",
                "second-data-at(4)",
                "third-schema-at(7)",
            ]
        );
    }

    #[test]
    fn initial_migration() {
        let db = setup_db();
        let setup = Migration {
            migrate_schema: Some(log_history("initial-schema")),
            migrate_data: Some(log_history("initial-data")),
            min_safe_version: Some(1),
        };
        let mut migrations = IncrementalMigrationMap::new();
        migrations.insert(
            3,
            Migration {
                migrate_schema: Some(fails("should not be called")),
                ..Default::default()
            },
        );

        run_schema_migrations(&db, "debug-name", &setup, &migrations).unwrap();
        assert_eq!(get_version_history(&db).unwrap(), vh(3, 3, 1));
        assert_eq!(
            history_events(&db),
            vec!["initial-schema-at(0)", "initial-data-at(0)"]
        );
    }

    #[test]
    fn updates_and_preserves_versions() {
        // updates max version
        let db = setup_db();
        set_pre_schema(&db, vh(12, 12, 6));
        let mut m = IncrementalMigrationMap::new();
        m.insert(
            13,
            Migration {
                migrate_data: Some(Box::new(|_| Ok(()))),
                ..Default::default()
            },
        );
        run_schema_migrations(&db, "d", &Migration::default(), &m).unwrap();
        assert_eq!(get_version_history(&db).unwrap(), vh(13, 13, 6));

        // preserves schemaVersion when higher than data
        let db = setup_db();
        set_pre_schema(&db, vh(12, 14, 6));
        run_schema_migrations(&db, "d", &Migration::default(), &m).unwrap();
        assert_eq!(get_version_history(&db).unwrap(), vh(13, 14, 6));
    }

    #[test]
    fn rollback_to_earlier_version() {
        let db = setup_db();
        set_pre_schema(&db, vh(10, 10, 8));
        let mut m = IncrementalMigrationMap::new();
        m.insert(
            8,
            Migration {
                migrate_data: Some(fails("should not be run")),
                ..Default::default()
            },
        );
        run_schema_migrations(&db, "d", &Migration::default(), &m).unwrap();
        assert_eq!(get_version_history(&db).unwrap(), vh(8, 10, 8));
    }

    #[test]
    fn disallows_rollback_before_limit() {
        let db = setup_db();
        set_pre_schema(&db, vh(10, 10, 8));
        let mut m = IncrementalMigrationMap::new();
        m.insert(
            7,
            Migration {
                migrate_data: Some(fails("nope")),
                ..Default::default()
            },
        );
        let err = run_schema_migrations(&db, "debug-name", &Migration::default(), &m).unwrap_err();
        assert!(err
            .to_string()
            .contains("Cannot run debug-name at schema v7 because rollback limit is v8"));
        assert_eq!(get_version_history(&db).unwrap(), vh(10, 10, 8));
    }

    #[test]
    fn rollback_limit_bumping() {
        // bump past current version
        let db = setup_db();
        set_pre_schema(&db, vh(1, 1, 0));
        let mut m = IncrementalMigrationMap::new();
        m.insert(
            11,
            Migration {
                min_safe_version: Some(11),
                ..Default::default()
            },
        );
        run_schema_migrations(&db, "d", &Migration::default(), &m).unwrap();
        assert_eq!(get_version_history(&db).unwrap(), vh(11, 11, 11));

        // does not move backwards
        let db = setup_db();
        set_pre_schema(&db, vh(10, 10, 6));
        let mut m = IncrementalMigrationMap::new();
        m.insert(
            11,
            Migration {
                min_safe_version: Some(3),
                ..Default::default()
            },
        );
        run_schema_migrations(&db, "d", &Migration::default(), &m).unwrap();
        assert_eq!(get_version_history(&db).unwrap(), vh(11, 11, 6));
    }

    #[test]
    fn only_updates_version_for_successful_migrations() {
        let db = setup_db();
        set_pre_schema(&db, vh(12, 12, 6));
        let mut m = IncrementalMigrationMap::new();
        m.insert(
            13,
            Migration {
                migrate_data: Some(log_history("successful")),
                ..Default::default()
            },
        );
        m.insert(
            14,
            Migration {
                migrate_data: Some(fails("fails to get to 14")),
                ..Default::default()
            },
        );
        let err = run_schema_migrations(&db, "d", &Migration::default(), &m).unwrap_err();
        assert!(err.to_string().contains("fails to get to 14"));
        assert_eq!(get_version_history(&db).unwrap(), vh(13, 13, 6));
        assert_eq!(history_events(&db), vec!["successful-at(12)"]);
    }
}
