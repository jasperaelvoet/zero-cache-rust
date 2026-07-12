//! Port of `initial-sync.ts`'s `shadowInitialSync`: a *throwaway* initial
//! sync used as a production canary — it copies a (sampled, capped) snapshot
//! of the published tables into a temporary SQLite file, verifies the copy,
//! and deletes everything. No replication slot is created and nothing about
//! the serving replica is touched; the value is the exercise itself (does the
//! current publication set still initial-sync cleanly, and how fast?).
//!
//! Differences from the real [`crate::initial_sync`] flow, matching upstream:
//! - the snapshot comes from a plain `REPEATABLE READ READ ONLY` transaction
//!   (`pg_export_snapshot()`), NOT from `CREATE_REPLICATION_SLOT`;
//! - every per-table SELECT gets ` TABLESAMPLE BERNOULLI(<pct>)` (when
//!   `0 < sample_rate < 1`) and ` LIMIT <max_rows_per_table>` appended;
//! - after the copy, each table is verified against the replica
//!   (`COUNT(*)` == rows copied);
//! - the temp directory is always removed, success or failure.
//!
//! Scheduling (upstream's interval timer) is the server crate's job; this is
//! only the core one-shot helper.

use std::path::{Path, PathBuf};

use crate::change_log::CREATE_CHANGELOG_SCHEMA;
use crate::column_metadata::CREATE_COLUMN_METADATA_TABLE;
use crate::initial_sync::{copy_all, CopyTuning, InitialSyncError, InitialSyncOptions};
use crate::replication_state::init_replication_state;
use crate::table_metadata::CREATE_TABLE_METADATA_TABLE;
use crate::{StatementRunner, Value};

/// Result of one shadow sync: per-table copied row counts and the total wall
/// time of the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowSyncReport {
    pub tables: Vec<(String, usize)>,
    pub elapsed_ms: u64,
}

/// Removes the shadow sync's temp directory on drop — success, error, or
/// panic — so a failed canary never leaks replica files.
struct TempDirGuard(PathBuf);

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Runs one shadow initial sync against `conn_str`'s upstream. See the module
/// docs for semantics. `tmp_dir` overrides the parent of the throwaway
/// directory (default [`std::env::temp_dir`]).
pub async fn shadow_initial_sync(
    conn_str: &str,
    publications: &[String],
    sample_rate: f64,
    max_rows_per_table: u64,
    tmp_dir: Option<&Path>,
) -> Result<ShadowSyncReport, InitialSyncError> {
    let started = std::time::Instant::now();

    // Fresh throwaway directory (cleaned up by the guard no matter how this
    // function exits).
    let base = tmp_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(std::env::temp_dir);
    let dir = base.join(format!(
        "zero-shadow-sync-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir)
        .map_err(|e| InitialSyncError::Introspect(format!("creating shadow temp dir: {e}")))?;
    let _guard = TempDirGuard(dir.clone());

    let replica_path = dir.join("shadow-replica.db");
    let db = StatementRunner::open_file(&replica_path.to_string_lossy())?;

    let pg = zero_cache_change_source::pg_connection::connect(conn_str)
        .await
        .map_err(|e| InitialSyncError::Introspect(format!("connecting for shadow sync: {e}")))?;

    // A plain read-only snapshot — no replication slot in shadow mode. The
    // snapshot is exported (as upstream does) so parallel copy workers could
    // adopt it; the timeout override keeps a long canary copy from being
    // killed by `idle_in_transaction_session_timeout`.
    pg.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
        .await?;
    let result = async {
        pg.batch_execute("SET LOCAL idle_in_transaction_session_timeout = 0")
            .await?;
        let _snapshot: String = pg
            .query_one("SELECT pg_export_snapshot()", &[])
            .await?
            .get(0);

        // A shadow replica still gets a real version stamp; without a slot,
        // the current WAL position is the natural analogue of the slot's
        // consistent point.
        let lsn: String = pg
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await?
            .get(0);
        let replica_version = zero_cache_types::lsn::to_state_version_string(&lsn)
            .map_err(|e| InitialSyncError::Lsn(lsn.clone(), e.to_string()))?;

        db.exec(CREATE_CHANGELOG_SCHEMA)?;
        db.exec(CREATE_TABLE_METADATA_TABLE)?;
        db.exec(CREATE_COLUMN_METADATA_TABLE)?;
        let context = zero_cache_shared::bigint_json::JsonValue::Object(Vec::new());
        init_replication_state(&db, publications, &replica_version, &context, true)?;

        // Introspect at the snapshot and copy — with sampling/capping.
        let (tables, pub_indexes) =
            zero_cache_change_source::published_schema::get_publication_info(&pg, publications)
                .await
                .map_err(|e| InitialSyncError::Introspect(e.to_string()))?;
        let indexes: Vec<zero_cache_types::specs::IndexSpec> = pub_indexes
            .iter()
            .map(zero_cache_types::published_schema_json::to_index_spec)
            .collect();
        let tuning = CopyTuning {
            sample_rate: (sample_rate > 0.0 && sample_rate < 1.0).then_some(sample_rate),
            max_rows_per_table: Some(max_rows_per_table as i64),
        };
        let table_rows = copy_all(
            &pg,
            &[],
            &db,
            &tables,
            &indexes,
            &replica_version,
            &InitialSyncOptions::default(),
            tuning,
        )
        .await?;

        // Verify: every table exists in the shadow replica and holds exactly
        // the rows the copy reported (upstream's shadow verification).
        for (table, rows) in &table_rows {
            let count = db
                .query_uncached(
                    &format!("SELECT COUNT(*) FROM {}", zero_cache_types::sql::id(table)),
                    &[],
                )
                .map_err(|e| {
                    InitialSyncError::Introspect(format!(
                        "shadow verify: table {table} unreadable: {e}"
                    ))
                })?;
            let got = match count.first().and_then(|r| r.first()) {
                Some((_, Value::Integer(n))) => *n as usize,
                other => {
                    return Err(InitialSyncError::Introspect(format!(
                        "shadow verify: table {table} COUNT(*) returned {other:?}"
                    )))
                }
            };
            if got != *rows {
                return Err(InitialSyncError::Introspect(format!(
                    "shadow verify: table {table} has {got} rows, copy reported {rows}"
                )));
            }
        }
        Ok(table_rows)
    }
    .await;

    // Release the snapshot transaction either way.
    let _ = match &result {
        Ok(_) => pg.batch_execute("COMMIT").await,
        Err(_) => pg.batch_execute("ROLLBACK").await,
    };
    // Close the SQLite file before the guard removes the directory.
    drop(db);

    Ok(ShadowSyncReport {
        tables: result?,
        elapsed_ms: started.elapsed().as_millis() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_conn_str() -> String {
        std::env::var("ZERO_TEST_PG_URL")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into())
    }

    /// Shadow sync end-to-end against live Postgres: a 100-row table shadow
    /// syncs with `max_rows_per_table = 10`, the report is capped accordingly,
    /// and the throwaway directory is gone afterwards.
    #[tokio::test]
    async fn live_shadow_sync_caps_rows_and_cleans_up() {
        let conn_str = test_conn_str();
        let Ok(pg) = zero_cache_change_source::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS shadow_sync_src CASCADE; \
             CREATE TABLE shadow_sync_src(id int primary key, val text not null); \
             INSERT INTO shadow_sync_src(id, val) \
               SELECT g, 'row-' || g FROM generate_series(1, 100) g; \
             DROP PUBLICATION IF EXISTS shadow_sync_pub; \
             CREATE PUBLICATION shadow_sync_pub FOR TABLE shadow_sync_src;",
        )
        .await
        .unwrap();

        // Use a dedicated parent dir so the cleanup assertion below sees only
        // this run's throwaway directory.
        let parent = std::env::temp_dir().join(format!("shadow-sync-test-{}", std::process::id()));
        std::fs::create_dir_all(&parent).unwrap();

        let report = shadow_initial_sync(
            &conn_str,
            &["shadow_sync_pub".to_string()],
            1.0,
            10,
            Some(&parent),
        )
        .await
        .unwrap();

        let (table, rows) = report
            .tables
            .iter()
            .find(|(name, _)| name == "shadow_sync_src")
            .expect("the published table appears in the report");
        assert_eq!(table, "shadow_sync_src");
        assert!(
            *rows <= 10 && *rows > 0,
            "LIMIT 10 caps the copy, got {rows}"
        );

        // The throwaway directory is gone.
        let leftovers: Vec<_> = std::fs::read_dir(&parent)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("zero-shadow-sync-"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "shadow temp dirs must be removed, found {leftovers:?}"
        );

        std::fs::remove_dir_all(&parent).ok();
        pg.batch_execute("DROP PUBLICATION shadow_sync_pub; DROP TABLE shadow_sync_src;")
            .await
            .unwrap();
    }

    /// A failed shadow sync still removes its temp directory — the cleanup
    /// guard runs on the error path too. An unreachable upstream fails after
    /// the throwaway directory (and replica file) already exist, so this
    /// needs no live Postgres.
    #[tokio::test]
    async fn shadow_sync_cleans_up_on_failure() {
        let parent =
            std::env::temp_dir().join(format!("shadow-sync-errtest-{}", std::process::id()));
        std::fs::create_dir_all(&parent).unwrap();

        let result = shadow_initial_sync(
            "host=127.0.0.1 port=1 user=nobody dbname=nowhere connect_timeout=1",
            &["some_pub".to_string()],
            1.0,
            10,
            Some(&parent),
        )
        .await;
        assert!(result.is_err(), "an unreachable upstream must fail");

        let leftovers = std::fs::read_dir(&parent)
            .unwrap()
            .filter_map(|e| e.ok())
            .count();
        assert_eq!(leftovers, 0, "temp dir removed on the error path too");
        std::fs::remove_dir_all(&parent).ok();
    }
}
