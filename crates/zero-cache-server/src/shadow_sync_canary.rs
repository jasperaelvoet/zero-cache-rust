//! Shadow-sync canary scheduler (`ZERO_SHADOW_SYNC_*`).
//!
//! Port of upstream's `ShadowSyncService`: periodically exercise the
//! initial-sync code path against a sampled, throwaway replica so a break in
//! the real path (schema drift, PG version quirks) surfaces before a customer
//! needs a full reset. Runs only on the change-streamer / replication-manager
//! node. The first run is jittered into `[2/3, 1)` of the interval so a fleet
//! restart doesn't canary simultaneously; a failed run logs and increments a
//! failure counter but never crashes the process.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use zero_cache_sqlite::shadow_sync::shadow_initial_sync;

/// Options for the canary, from the parsed config.
#[derive(Debug, Clone)]
pub struct ShadowSyncConfig {
    pub interval_hours: f64,
    pub sample_rate: f64,
    pub max_rows_per_table: u64,
    pub storage_tmp_dir: Option<String>,
}

/// Computes the first-run delay: uniform in `[2/3·interval, interval)`.
/// `jitter01` is a deterministic-per-process value in `[0, 1)` (the runtime
/// forbids `Math.random`-style ambient entropy, so the caller supplies it).
pub fn first_run_delay_ms(interval_ms: u64, jitter01: f64) -> u64 {
    let min = (interval_ms as f64 * 2.0 / 3.0) as u64;
    let span = interval_ms.saturating_sub(min);
    min + (span as f64 * jitter01.clamp(0.0, 0.999)) as u64
}

/// Runs the canary loop until `shutdown`.
pub async fn run_shadow_sync(
    conn_str: String,
    publications: Vec<String>,
    config: ShadowSyncConfig,
    jitter01: f64,
    shutdown: Arc<AtomicBool>,
) {
    let interval_ms = (config.interval_hours * 3_600_000.0) as u64;
    if interval_ms == 0 {
        return;
    }
    let tmp_dir = config
        .storage_tmp_dir
        .as_ref()
        .map(std::path::PathBuf::from);

    let mut next_delay = first_run_delay_ms(interval_ms, jitter01);
    let mut failures: u64 = 0;
    loop {
        if !sleep_unless_shutdown(std::time::Duration::from_millis(next_delay), &shutdown).await {
            return;
        }
        let started = std::time::Instant::now();
        match shadow_initial_sync(
            &conn_str,
            &publications,
            config.sample_rate,
            config.max_rows_per_table,
            tmp_dir.as_deref(),
        )
        .await
        {
            Ok(report) => crate::info!(
                "shadow-sync canary OK: {} table(s) in {}ms",
                report.tables.len(),
                started.elapsed().as_millis()
            ),
            Err(e) => {
                failures += 1;
                crate::warn!("shadow-sync canary FAILED ({failures} total): {e}");
            }
        }
        next_delay = interval_ms;
    }
}

/// Sleeps in short slices so shutdown is observed promptly. Returns `false`
/// if shutdown fired.
async fn sleep_unless_shutdown(duration: std::time::Duration, shutdown: &AtomicBool) -> bool {
    let deadline = tokio::time::Instant::now() + duration;
    while tokio::time::Instant::now() < deadline {
        if shutdown.load(Ordering::SeqCst) {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    !shutdown.load(Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_run_delay_is_within_two_thirds_to_full_interval() {
        let interval = 12 * 3_600_000u64;
        let lo = interval * 2 / 3;
        for jitter in [0.0, 0.25, 0.5, 0.999] {
            let d = first_run_delay_ms(interval, jitter);
            assert!(d >= lo, "delay {d} below 2/3 floor {lo}");
            assert!(d < interval, "delay {d} at/above full interval {interval}");
        }
    }
}
