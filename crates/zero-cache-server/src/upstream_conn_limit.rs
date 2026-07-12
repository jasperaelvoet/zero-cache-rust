//! Upstream mutation-connection bound (`ZERO_UPSTREAM_MAX_CONNS`).
//!
//! Upstream sizes a per-worker Postgres pool from `upstream.maxConns` for
//! committing CRUD mutations (the replication stream uses its own dedicated
//! connection on top, as documented). This single-process server opens one
//! lazily-created upstream client per CRUD-pushing WebSocket connection, so
//! the equivalent bound is a semaphore over concurrently-open upstream
//! mutation clients: connection number `maxConns + 1` waits until one closes
//! rather than opening an unbounded number of Postgres connections.

use std::sync::{Arc, OnceLock};

static SEMAPHORE: OnceLock<Option<Arc<tokio::sync::Semaphore>>> = OnceLock::new();

/// Installs the bound once at startup. Zero (possible only via the hidden
/// per-worker override) is clamped to 1 so a permit can always exist.
pub fn init(max_conns: usize) {
    let _ = SEMAPHORE.set(Some(Arc::new(tokio::sync::Semaphore::new(std::cmp::max(
        max_conns, 1,
    )))));
}

/// Acquires a slot for one upstream mutation client, waiting when the bound
/// is exhausted. Returns `None` (unlimited) when [`init`] has not run (tests).
pub async fn acquire() -> Option<tokio::sync::OwnedSemaphorePermit> {
    match SEMAPHORE.get() {
        Some(Some(sem)) => sem.clone().acquire_owned().await.ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn uninitialized_limit_is_unlimited() {
        // init() not called in this test binary path: acquire returns None
        // (unlimited) rather than blocking.
        if SEMAPHORE.get().is_none() {
            assert!(acquire().await.is_none());
        }
    }
}
