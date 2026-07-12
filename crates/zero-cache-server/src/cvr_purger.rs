//! CVR garbage collection — port of upstream's `CVRPurger`
//! (`services/view-syncer/cvr-purger.ts`, scheduled by `server/reaper.ts`).
//!
//! Inactive CVRs (client groups whose `instances.lastActive` is older than
//! `ZERO_CVR_GARBAGE_COLLECTION_INACTIVITY_THRESHOLD_HOURS`) are purged in
//! batches: child rows deleted bottom-up, the instance row tombstoned
//! (`deleted = true`, `version = '00'`), and month-old tombstones hard
//! deleted. A reconnecting client whose CVR was purged hits the empty-CVR /
//! non-empty-client-version check and receives ClientNotFound, restarting
//! with a fresh CVR — exactly upstream's contract.
//!
//! Scheduling matches upstream: purge every
//! `ZERO_CVR_GARBAGE_COLLECTION_INITIAL_INTERVAL_SECONDS` while there is a
//! backlog, exponentially backing off (×2 per idle round) to a 16-minute cap
//! when there is nothing to purge; the batch size starts at
//! `ZERO_CVR_GARBAGE_COLLECTION_INITIAL_BATCH_SIZE` and grows by that amount
//! whenever purging fails to keep up with new inactivity. Batch size 0
//! disables GC. Concurrency safety comes from `FOR UPDATE SKIP LOCKED` on the
//! instances row — an actively-updating view-syncer's CVR is skipped.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use zero_cache_change_source::pg_connection;
use zero_cache_types::shards::{cvr_schema, ShardId};

/// Upstream `MAX_PURGE_INTERVAL_MS` (16 minutes).
const MAX_PURGE_INTERVAL_MS: u64 = 16 * 60 * 1000;
/// Upstream `TOMBSTONE_PURGE_THRESHOLD` (31 days).
const TOMBSTONE_PURGE_THRESHOLD_MS: i64 = 31 * 24 * 3600 * 1000;

/// Config for the purger, derived from the three official options.
#[derive(Debug, Clone)]
pub struct CvrPurgerConfig {
    pub inactivity_threshold_ms: i64,
    pub initial_interval_ms: u64,
    pub initial_batch_size: u64,
}

impl CvrPurgerConfig {
    pub fn from_options(
        inactivity_threshold_hours: f64,
        initial_interval_seconds: f64,
        initial_batch_size: u64,
    ) -> Self {
        CvrPurgerConfig {
            inactivity_threshold_ms: (inactivity_threshold_hours * 3_600_000.0) as i64,
            initial_interval_ms: (initial_interval_seconds * 1000.0) as u64,
            initial_batch_size,
        }
    }

    /// The tombstone hard-delete horizon: `max(31 days, inactivityThreshold)`.
    pub fn tombstone_purge_threshold_ms(&self) -> i64 {
        std::cmp::max(TOMBSTONE_PURGE_THRESHOLD_MS, self.inactivity_threshold_ms)
    }
}

/// One purge round's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PurgeResult {
    pub purged: usize,
    pub remaining: i64,
}

/// Child tables deleted bottom-up before tombstoning the instance, in
/// upstream's exact order (FK integrity: `desires` → `queries` is not a PK
/// prefix, so no cascade is relied upon; `rowsVersion` deliberately has no FK).
const CHILD_TABLES: [&str; 5] = ["desires", "queries", "clients", "rows", "rowsVersion"];

/// Runs one purge transaction: select victims (`FOR UPDATE SKIP LOCKED`),
/// delete child rows, tombstone instances, reap old tombstones, and recount
/// the remaining backlog. `now_ms` is injected for testability.
pub async fn purge_inactive_cvrs(
    client: &tokio_postgres::Client,
    schema: &str,
    config: &CvrPurgerConfig,
    max_cvrs: u64,
    now_ms: i64,
) -> Result<PurgeResult, tokio_postgres::Error> {
    let threshold = now_ms - config.inactivity_threshold_ms;
    let tombstone_threshold = now_ms - config.tombstone_purge_threshold_ms();

    client
        .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
        .await?;
    let result = purge_in_txn(client, schema, threshold, tombstone_threshold, max_cvrs).await;
    match &result {
        Ok(_) => client.batch_execute("COMMIT").await?,
        Err(_) => {
            let _ = client.batch_execute("ROLLBACK").await;
        }
    }
    result
}

async fn purge_in_txn(
    client: &tokio_postgres::Client,
    schema: &str,
    threshold_ms: i64,
    tombstone_threshold_ms: i64,
    max_cvrs: u64,
) -> Result<PurgeResult, tokio_postgres::Error> {
    // lastActive is a TIMESTAMPTZ; upstream binds epoch-ms numbers and lets
    // Postgres coerce. tokio-postgres has no implicit coercion, so convert
    // explicitly with to_timestamp(ms / 1000.0).
    let victims = client
        .query(
            &format!(
                r#"SELECT "clientGroupID" FROM {schema}."instances"
                   WHERE NOT "deleted" AND "lastActive" < to_timestamp($1::double precision / 1000.0)
                   ORDER BY "lastActive" ASC
                   LIMIT $2
                   FOR UPDATE SKIP LOCKED"#,
                schema = quoted(schema)
            ),
            &[&(threshold_ms as f64), &(max_cvrs as i64)],
        )
        .await?;
    let ids: Vec<String> = victims.iter().map(|r| r.get(0)).collect();

    if !ids.is_empty() {
        for table in CHILD_TABLES {
            client
                .execute(
                    &format!(
                        r#"DELETE FROM {schema}."{table}" WHERE "clientGroupID" = ANY($1)"#,
                        schema = quoted(schema)
                    ),
                    &[&ids],
                )
                .await?;
        }
        // Tombstone: keep the row (profileID / lastActive preserved for
        // stats); version '00' (the empty NEW_CVR_VERSION) makes a
        // reconnecting client fail the version check → ClientNotFound.
        client
            .execute(
                &format!(
                    r#"UPDATE {schema}."instances"
                       SET "deleted" = TRUE, "version" = '00', "ttlClock" = 0,
                           "replicaVersion" = NULL, "owner" = NULL,
                           "grantedAt" = NULL, "clientSchema" = NULL
                       WHERE "clientGroupID" = ANY($1)"#,
                    schema = quoted(schema)
                ),
                &[&ids],
            )
            .await?;
    }

    client
        .execute(
            &format!(
                r#"DELETE FROM {schema}."instances"
                   WHERE "deleted" AND "lastActive" < to_timestamp($1::double precision / 1000.0)"#,
                schema = quoted(schema)
            ),
            &[&(tombstone_threshold_ms as f64)],
        )
        .await?;

    let remaining: i64 = client
        .query_one(
            &format!(
                r#"SELECT COUNT(*) FROM {schema}."instances"
                   WHERE NOT "deleted" AND "lastActive" < to_timestamp($1::double precision / 1000.0)"#,
                schema = quoted(schema)
            ),
            &[&(threshold_ms as f64)],
        )
        .await?
        .get(0);

    Ok(PurgeResult {
        purged: ids.len(),
        remaining,
    })
}

fn quoted(schema: &str) -> String {
    format!("\"{schema}\"")
}

/// The pure scheduling step, extracted for tests: given the previous state
/// and a purge result, returns the next `(batch_size, interval_ms, purgeable)`
/// per upstream's algorithm.
pub fn next_schedule(
    config: &CvrPurgerConfig,
    batch_size: u64,
    interval_ms: u64,
    prior_purgeable: Option<i64>,
    result: PurgeResult,
) -> (u64, u64, Option<i64>) {
    let mut batch = batch_size;
    // Linear growth: the backlog grew despite purging.
    if let Some(prior) = prior_purgeable {
        if result.remaining > prior {
            batch += config.initial_batch_size;
        }
    }
    let interval = if result.remaining > 0 {
        config.initial_interval_ms
    } else {
        std::cmp::min(interval_ms * 2, MAX_PURGE_INTERVAL_MS)
    };
    (batch, interval, Some(result.remaining))
}

/// Runs the purger loop until `shutdown` flips. Connects lazily and rides out
/// errors with upstream's 25ms→10s backoff.
pub async fn run_cvr_purger(
    cvr_db: String,
    shard: ShardId,
    config: CvrPurgerConfig,
    shutdown: Arc<AtomicBool>,
) {
    if config.initial_batch_size == 0 {
        crate::warn!("CVR garbage collection disabled (initial batch size 0)");
        return;
    }
    let Ok(schema) = cvr_schema(&shard) else {
        crate::warn!("CVR purger: invalid shard for schema name; not running");
        return;
    };
    let mut batch_size = config.initial_batch_size;
    let mut interval_ms = config.initial_interval_ms;
    let mut purgeable: Option<i64> = None;
    let mut error_backoff = std::time::Duration::from_millis(25);

    while !shutdown.load(Ordering::SeqCst) {
        let outcome = async {
            let client = pg_connection::connect(&cvr_db).await?;
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or_default();
            purge_inactive_cvrs(&client, &schema, &config, batch_size, now_ms)
                .await
                .map_err(pg_connection::PgError::from)
        }
        .await;

        match outcome {
            Ok(result) => {
                error_backoff = std::time::Duration::from_millis(25);
                if result.purged > 0 {
                    crate::info!(
                        "CVR GC purged {} inactive client group(s); {} remaining",
                        result.purged,
                        result.remaining
                    );
                }
                let (b, i, p) = next_schedule(&config, batch_size, interval_ms, purgeable, result);
                batch_size = b;
                interval_ms = i;
                purgeable = p;
                sleep_unless_shutdown(std::time::Duration::from_millis(interval_ms), &shutdown)
                    .await;
            }
            Err(e) => {
                crate::warn!("CVR GC round failed: {e}; backing off");
                sleep_unless_shutdown(error_backoff, &shutdown).await;
                error_backoff =
                    std::cmp::min(error_backoff * 2, std::time::Duration::from_secs(10));
            }
        }
    }
}

async fn sleep_unless_shutdown(duration: std::time::Duration, shutdown: &AtomicBool) {
    let deadline = tokio::time::Instant::now() + duration;
    while tokio::time::Instant::now() < deadline && !shutdown.load(Ordering::SeqCst) {
        let remaining = deadline - tokio::time::Instant::now();
        tokio::time::sleep(std::cmp::min(
            remaining,
            std::time::Duration::from_millis(250),
        ))
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> CvrPurgerConfig {
        CvrPurgerConfig::from_options(48.0, 60.0, 25)
    }

    #[test]
    fn option_conversion_matches_upstream() {
        let c = config();
        assert_eq!(c.inactivity_threshold_ms, 48 * 3_600_000);
        assert_eq!(c.initial_interval_ms, 60_000);
        assert_eq!(c.initial_batch_size, 25);
        // Tombstone horizon: max(31 days, threshold).
        assert_eq!(
            c.tombstone_purge_threshold_ms(),
            TOMBSTONE_PURGE_THRESHOLD_MS
        );
        let long = CvrPurgerConfig::from_options(24.0 * 40.0, 60.0, 25);
        assert_eq!(long.tombstone_purge_threshold_ms(), 40 * 24 * 3_600_000i64);
    }

    #[test]
    fn scheduling_backs_off_exponentially_when_idle() {
        let c = config();
        let idle = PurgeResult {
            purged: 0,
            remaining: 0,
        };
        let (b, i, p) = next_schedule(&c, 25, 60_000, None, idle);
        assert_eq!((b, i), (25, 120_000));
        let (b, i, _) = next_schedule(&c, b, i, p, idle);
        assert_eq!((b, i), (25, 240_000));
        // Cap at 16 minutes.
        let (_, i, _) = next_schedule(&c, 25, MAX_PURGE_INTERVAL_MS, Some(0), idle);
        assert_eq!(i, MAX_PURGE_INTERVAL_MS);
    }

    #[test]
    fn scheduling_resets_to_fast_interval_while_backlogged() {
        let c = config();
        let backlogged = PurgeResult {
            purged: 25,
            remaining: 10,
        };
        let (_, i, _) = next_schedule(&c, 25, MAX_PURGE_INTERVAL_MS, Some(50), backlogged);
        assert_eq!(i, 60_000);
    }

    #[test]
    fn batch_grows_linearly_when_backlog_grows() {
        let c = config();
        // remaining (30) > prior purgeable (20): grow by the initial batch.
        let grew = PurgeResult {
            purged: 25,
            remaining: 30,
        };
        let (b, _, p) = next_schedule(&c, 25, 60_000, Some(20), grew);
        assert_eq!(b, 50);
        assert_eq!(p, Some(30));
        // Backlog shrinking: batch stays (never shrinks).
        let shrank = PurgeResult {
            purged: 50,
            remaining: 5,
        };
        let (b, _, _) = next_schedule(&c, b, 60_000, p, shrank);
        assert_eq!(b, 50);
        // First round (no prior): no growth even with a backlog.
        let (b, _, _) = next_schedule(&c, 25, 60_000, None, grew);
        assert_eq!(b, 25);
    }

    /// Live: full purge round against the CVR schema — inactive group purged
    /// and tombstoned, active group kept, reconnect-shaped version check story
    /// preserved (version reset to '00'). Skips without a test Postgres.
    #[tokio::test]
    async fn live_purges_inactive_and_keeps_active_groups() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into());
        let Ok(client) = pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        let shard = ShardId {
            app_id: format!("gc_test_{}", std::process::id()),
            shard_num: 0,
        };
        // Provision the real CVR schema.
        if crate::cvr_provision::provision_cvr_schema(&conn_str, &shard)
            .await
            .is_err()
        {
            eprintln!("skipping: could not provision CVR schema");
            return;
        }
        let schema = cvr_schema(&shard).unwrap();
        let q = |sql: &str| sql.replace("{s}", &format!("\"{schema}\""));

        let now_ms: i64 = 1_800_000_000_000; // fixed 'now' for determinism
        let stale = now_ms - 100 * 3_600_000; // 100h old
        let fresh = now_ms - 3_600_000; // 1h old
        client
            .execute(
                &q(r#"INSERT INTO {s}."instances"
                     ("clientGroupID", "version", "lastActive", "ttlClock", "deleted")
                     VALUES ('stale-group', '0a', to_timestamp($1::double precision / 1000.0), 0, false),
                            ('fresh-group', '0b', to_timestamp($2::double precision / 1000.0), 0, false)"#),
                &[&(stale as f64), &(fresh as f64)],
            )
            .await
            .unwrap();
        client
            .execute(
                &q(
                    r#"INSERT INTO {s}."rowsVersion" ("clientGroupID", "version")
                     VALUES ('stale-group', '0a')"#,
                ),
                &[],
            )
            .await
            .unwrap();

        let config = config();
        let result = purge_inactive_cvrs(&client, &schema, &config, 25, now_ms)
            .await
            .unwrap();
        assert_eq!(result.purged, 1);
        assert_eq!(result.remaining, 0);

        // stale-group is tombstoned with the empty version; fresh-group intact.
        let row = client
            .query_one(
                &q(r#"SELECT "deleted", "version" FROM {s}."instances"
                     WHERE "clientGroupID" = 'stale-group'"#),
                &[],
            )
            .await
            .unwrap();
        assert!(row.get::<_, bool>(0));
        assert_eq!(row.get::<_, String>(1), "00");
        let fresh_deleted: bool = client
            .query_one(
                &q(r#"SELECT "deleted" FROM {s}."instances"
                     WHERE "clientGroupID" = 'fresh-group'"#),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert!(!fresh_deleted);
        // Child rows are gone.
        let rows_version: i64 = client
            .query_one(
                &q(r#"SELECT COUNT(*) FROM {s}."rowsVersion" WHERE "clientGroupID" = 'stale-group'"#),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(rows_version, 0);

        // Tombstones older than the horizon are hard-deleted on a later round.
        // Keep the fresh group genuinely active at `later` (touch its
        // lastActive forward) so the only work this round is tombstone reaping.
        let later = now_ms + config.tombstone_purge_threshold_ms() + 1;
        client
            .execute(
                &q(r#"UPDATE {s}."instances"
                     SET "lastActive" = to_timestamp($1::double precision / 1000.0)
                     WHERE "clientGroupID" = 'fresh-group'"#),
                &[&((later - 3_600_000) as f64)],
            )
            .await
            .unwrap();
        // (lastActive of the stale tombstone is still `stale`, past the horizon.)
        let result = purge_inactive_cvrs(&client, &schema, &config, 25, later)
            .await
            .unwrap();
        assert_eq!(result.purged, 0);
        let stale_rows: i64 = client
            .query_one(
                &q(r#"SELECT COUNT(*) FROM {s}."instances" WHERE "clientGroupID" = 'stale-group'"#),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(stale_rows, 0);

        // Cleanup.
        client
            .batch_execute(&format!("DROP SCHEMA \"{schema}\" CASCADE"))
            .await
            .ok();
    }
}
