//! Lazy startup (`ZERO_LAZY_STARTUP`).
//!
//! Port of upstream's runner behavior (`run-worker.ts` + `zero-dispatcher.ts`):
//! when enabled, the heavyweight replication stack (Postgres replication
//! stream, initial sync) is not started until the first sync WebSocket
//! connection arrives. Health/admin HTTP routes do NOT trigger startup —
//! only a sync handoff does. Single-node mode only, as upstream.
//!
//! Mechanics: `arm()` installs the process-wide trigger before the listener
//! starts. The accept path calls [`ensure_started`] for each sync upgrade —
//! a no-op when lazy startup is not armed; otherwise it fires the trigger
//! (idempotently) and waits until the replication stack reports ready, so the
//! first client's handshake proceeds only against a synced replica.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

struct LazyState {
    trigger: tokio::sync::Notify,
    triggered: AtomicBool,
    ready: Arc<AtomicBool>,
}

static STATE: OnceLock<Arc<LazyState>> = OnceLock::new();

/// Arms lazy startup. The caller (main) awaits `triggered()` on the returned
/// handle and then starts the replication stack, flipping `ready` when
/// serving is possible. Call at most once.
pub fn arm(ready: Arc<AtomicBool>) -> LazyTrigger {
    let state = Arc::new(LazyState {
        trigger: tokio::sync::Notify::new(),
        triggered: AtomicBool::new(false),
        ready,
    });
    let _ = STATE.set(state.clone());
    LazyTrigger { state }
}

/// The main-side handle to an armed lazy start.
pub struct LazyTrigger {
    state: Arc<LazyState>,
}

impl LazyTrigger {
    /// Resolves when the first sync request fires the trigger.
    pub async fn triggered(&self) {
        if self.state.triggered.load(Ordering::SeqCst) {
            return;
        }
        self.state.trigger.notified().await;
    }
}

/// Fires the lazy trigger (first sync request) and waits for readiness. No-op
/// returning immediately when lazy startup is not armed. Returns `false` if
/// readiness did not arrive within `timeout` (the caller should drop the
/// connection; the client will retry).
pub async fn ensure_started(timeout: std::time::Duration) -> bool {
    let Some(state) = STATE.get() else {
        return true;
    };
    if !state.triggered.swap(true, Ordering::SeqCst) {
        crate::info!("lazy startup — first sync request received; starting replication");
        state.trigger.notify_one();
    }
    if state.ready.load(Ordering::SeqCst) {
        return true;
    }
    let deadline = tokio::time::Instant::now() + timeout;
    while !state.ready.load(Ordering::SeqCst) {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unarmed_lazy_start_is_a_no_op() {
        // Without arm(), sync connections proceed immediately.
        assert!(ensure_started(std::time::Duration::from_millis(10)).await);
    }
}
