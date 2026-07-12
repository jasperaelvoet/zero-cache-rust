//! Replication lag reporting (`ZERO_REPLICATION_LAG_REPORT_INTERVAL_MS`).
//!
//! Port of upstream's `LagReporter` (`change-source/pg/change-source.ts`):
//! periodically emit a WAL logical message via `pg_logical_emit_message`
//! carrying a send timestamp, then observe it flow back through the
//! replication stream. The delay between emit and receipt is the replication
//! lag, exposed as the `zero.replication.*_lag` gauges. A `<= 0` interval
//! disables reporting; if the message isn't observed before the next
//! interval, a retry is emitted and `lag_report_retries` increments.
//!
//! The message prefix is `{appID}/{shardNum}/lag-report/v1` and the payload is
//! `{"id","sendTimeMs","commitTimeMs"}`, matching upstream so an official
//! consumer would decode it identically.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use zero_cache_change_source::pg_connection;

const MESSAGE_SUFFIX: &str = "/lag-report/v1";

/// The lag-report WAL-message prefix for a shard.
pub fn message_prefix(app_id: &str, shard_num: i64) -> String {
    format!("{app_id}/{shard_num}{MESSAGE_SUFFIX}")
}

/// Shared lag state: the send time of the outstanding report (0 = none), and
/// the retry counter. The emit task and the stream observer share it.
#[derive(Default)]
pub struct LagState {
    /// `sendTimeMs` of the currently-outstanding report, or 0 when none.
    outstanding_send_ms: AtomicI64,
    /// Reports emitted whose acknowledgement (round-trip) never arrived
    /// before the next interval (upstream `lag_report_retries`).
    retries: AtomicI64,
    /// The most recently observed total lag (ms), for inspection/metrics.
    last_total_lag_ms: AtomicI64,
}

impl LagState {
    pub fn last_total_lag_ms(&self) -> i64 {
        self.last_total_lag_ms.load(Ordering::Relaxed)
    }
    pub fn retries(&self) -> i64 {
        self.retries.load(Ordering::Relaxed)
    }
}

/// Parses a received lag-report payload's `sendTimeMs`, returning the total
/// lag (`now - sendTimeMs`) when the prefix matches. Called from the apply
/// loop's message observer.
pub fn observe_message(
    state: &LagState,
    prefix: &str,
    msg_prefix: &str,
    payload: &[u8],
    receive_ms: i64,
) -> Option<i64> {
    if msg_prefix != prefix {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let send_ms = value.get("sendTimeMs").and_then(|v| v.as_i64())?;
    // This report is acknowledged; clear the outstanding marker.
    state.outstanding_send_ms.store(0, Ordering::Relaxed);
    let total_lag = receive_ms - send_ms;
    state.last_total_lag_ms.store(total_lag, Ordering::Relaxed);
    Some(total_lag)
}

/// Emits one lag-report WAL message on `client`. Postgres 17+ takes the
/// `flush` argument; on older servers the 3-arg form is used (an extra
/// ~50-100ms idle latency, as upstream notes). `now_ms` is the send time.
async fn emit_report(
    client: &tokio_postgres::Client,
    prefix: &str,
    now_ms: i64,
    pg17_plus: bool,
) -> Result<(), tokio_postgres::Error> {
    let id = format!("{:x}", now_ms);
    let payload = format!(r#"{{"id":"{id}","sendTimeMs":{now_ms},"commitTimeMs":{now_ms}}}"#);
    if pg17_plus {
        client
            .execute(
                "SELECT pg_logical_emit_message(false, $1, $2, true)",
                &[&prefix, &payload],
            )
            .await?;
    } else {
        client
            .execute(
                "SELECT pg_logical_emit_message(false, $1, $2)",
                &[&prefix, &payload],
            )
            .await?;
    }
    Ok(())
}

/// Runs the periodic lag-report emitter until `shutdown`. Reconnects on error
/// and rides out transient failures. A `<= 0` interval never starts this.
pub async fn run_lag_reporter(
    conn_str: String,
    prefix: String,
    interval_ms: i64,
    pg17_plus: bool,
    state: Arc<LagState>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) {
    if interval_ms <= 0 {
        return;
    }
    let interval = std::time::Duration::from_millis(interval_ms as u64);
    let client = loop {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        match pg_connection::connect(&conn_str).await {
            Ok(c) => break c,
            Err(e) => {
                crate::warn!("lag reporter: connect failed: {e}; retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    };
    while !shutdown.load(Ordering::SeqCst) {
        // If the previous report is still outstanding, this is a missed
        // round-trip: count a retry (upstream `lag_report_retries`).
        if state.outstanding_send_ms.load(Ordering::Relaxed) != 0 {
            state.retries.fetch_add(1, Ordering::Relaxed);
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default();
        state.outstanding_send_ms.store(now_ms, Ordering::Relaxed);
        if let Err(e) = emit_report(&client, &prefix, now_ms, pg17_plus).await {
            crate::warn!("lag reporter: emit failed: {e}");
        }
        let deadline = tokio::time::Instant::now() + interval;
        while tokio::time::Instant::now() < deadline && !shutdown.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(250).min(interval)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_matches_upstream_shape() {
        assert_eq!(message_prefix("zero", 0), "zero/0/lag-report/v1");
        assert_eq!(
            message_prefix("hunting_game_staging", 0),
            "hunting_game_staging/0/lag-report/v1"
        );
    }

    #[test]
    fn observing_a_matching_report_yields_total_lag_and_clears_outstanding() {
        let state = LagState::default();
        state.outstanding_send_ms.store(1000, Ordering::Relaxed);
        let prefix = message_prefix("zero", 0);
        let payload = br#"{"id":"a","sendTimeMs":1000,"commitTimeMs":1000}"#;
        let lag = observe_message(&state, &prefix, &prefix, payload, 1120);
        assert_eq!(lag, Some(120));
        assert_eq!(state.last_total_lag_ms(), 120);
        assert_eq!(state.outstanding_send_ms.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_foreign_prefix_is_ignored() {
        let state = LagState::default();
        let prefix = message_prefix("zero", 0);
        let payload = br#"{"sendTimeMs":1000}"#;
        assert_eq!(
            observe_message(&state, &prefix, "other/prefix", payload, 2000),
            None
        );
    }
}
