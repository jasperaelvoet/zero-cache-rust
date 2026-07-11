//! Process-local per-client-group barrier for deferred CVR row flushes.
//!
//! When `ZERO_DEFER_CVR_ROWS` is enabled, a CVR transition commits its
//! configuration (the durable cookie + the optimistic-concurrency CAS)
//! synchronously, then flushes the row records off the hydration critical path
//! in a spawned task.  That split would break the durable invariant that "a
//! client never observes a config cookie whose corresponding rows are not yet
//! committed" if a reconnect could load durable rows before the deferred flush
//! landed.
//!
//! On the single-node, server-authoritative deployment this port targets, this
//! barrier restores the invariant: every deferred row flush for a group is
//! chained after the previous one (so `rowsVersion` advances monotonically and
//! in order), and any CVR load for the group first awaits the group's latest
//! pending flush via [`RowFlushBarrier::wait_for_pending`].  Because the load
//! blocks until the rows land, the config cookie and its rows are still observed
//! atomically on this node.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

/// Process-global bound on how many deferred CVR row flushes may run their
/// critical section (pool connection + `flush_cvr_rows_transition`) at once.
///
/// The deferred path (`ZERO_DEFER_CVR_ROWS`) moves the 1000-row CVR row-record
/// flush off the hydration critical path, but each such flush still holds a
/// `CvrPool` connection while writing 1000 rows to Postgres. Under a burst of
/// reconnecting client groups, hundreds of these background flushes would
/// otherwise seize most of the pool and saturate Postgres, starving the small
/// synchronous config/version flushes that ARE on the critical path. A single
/// process-wide semaphore caps that background load, leaving pool slots and
/// Postgres CPU free for the config flushes.
///
/// A deferred flush acquires a permit BEFORE it takes a pool connection and
/// releases it on completion. This is orthogonal to the per-group
/// [`RowFlushBarrier`]: the barrier still chains a group's flushes in strict
/// order (monotonic `rowsVersion`); the limiter only bounds how many groups'
/// flushes run their critical section simultaneously. Waiting for a permit is
/// correct even when a reconnect awaits its group's pending flush — the flush
/// still completes and signals its slot, only later.
#[derive(Clone)]
pub struct DeferFlushLimiter {
    permits: Arc<Semaphore>,
}

impl DeferFlushLimiter {
    /// Creates a limiter admitting at most `limit` concurrent deferred flushes
    /// (clamped to at least 1 so a misconfigured `0` never deadlocks).
    pub fn new(limit: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(limit.max(1))),
        }
    }

    /// Acquires a permit, waiting if the limit is currently saturated. Returns
    /// `None` only if the semaphore was closed (never, in practice). The permit
    /// is held for the flush's critical section and released when dropped.
    pub async fn acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.permits.clone().acquire_owned().await.ok()
    }

    #[cfg(test)]
    fn available(&self) -> usize {
        self.permits.available_permits()
    }
}

/// One deferred flush's completion signal.  Awaiters observe `done` (set before
/// waking) and fall back to `notify` to be woken exactly once the flush task has
/// both run and signalled.
struct FlushSlot {
    done: AtomicBool,
    notify: Notify,
}

impl FlushSlot {
    fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn complete(&self) {
        // Publish completion before waking so a late awaiter that checks `done`
        // after we notify still returns immediately.
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        if self.done.load(Ordering::Acquire) {
            return;
        }
        // Register interest before re-checking `done` to close the race with a
        // `complete()` that lands between the first check and `notified()`.
        let notified = self.notify.notified();
        if self.done.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }
}

/// Per-client-group chain of deferred row flushes.
#[derive(Default)]
pub struct RowFlushBarrier {
    /// The most recently enqueued flush's slot, or `None` when no deferred flush
    /// is outstanding.  Completing the latest slot implies every earlier slot in
    /// the chain has already completed, so a load only ever awaits this one.
    latest: Mutex<Option<Arc<FlushSlot>>>,
}

impl RowFlushBarrier {
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserves a slot for a newly deferred flush and returns
    /// `(previous, current)`.  The caller's spawned task must `await` `previous`
    /// (if any) before writing, then call [`RowFlushBarrier::complete`] with
    /// `current`.  This chaining keeps flushes for one group strictly ordered.
    fn enqueue(&self) -> (Option<Arc<FlushSlot>>, Arc<FlushSlot>) {
        let current = Arc::new(FlushSlot::new());
        let mut latest = self.latest.lock().unwrap_or_else(|p| p.into_inner());
        let previous = latest.replace(current.clone());
        (previous, current)
    }

    /// Awaits the group's latest pending deferred flush, if any.  Call this
    /// before reading durable CVR rows for the group so a reconnect never
    /// observes a cookie whose rows have not landed.
    pub async fn wait_for_pending(&self) {
        let slot = self
            .latest
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some(slot) = slot {
            slot.wait().await;
        }
    }

    /// Spawns `flush` as the next link in this group's deferred-flush chain.
    /// The future runs only after the previous deferred flush for the group has
    /// completed, preserving monotonic `rowsVersion` ordering.
    pub fn spawn_chained<F>(self: &Arc<Self>, flush: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let (previous, current) = self.enqueue();
        let barrier = self.clone();
        tokio::spawn(async move {
            if let Some(previous) = previous {
                previous.wait().await;
            }
            flush.await;
            current.complete();
            // Drop the chain tail once it has drained so a long-lived group does
            // not pin a completed slot forever.
            let mut latest = barrier.latest.lock().unwrap_or_else(|p| p.into_inner());
            if latest
                .as_ref()
                .is_some_and(|slot| Arc::ptr_eq(slot, &current))
            {
                *latest = None;
            }
        });
    }
}

/// A process-wide registry of per-client-group barriers, mirroring the
/// `cvr_transition_locks` map so a connection and its reconnects share one
/// barrier for their group.
#[derive(Clone, Default)]
pub struct RowFlushBarriers {
    inner: Arc<Mutex<std::collections::HashMap<String, Weak<RowFlushBarrier>>>>,
}

impl RowFlushBarriers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the shared barrier for `client_group_id`, creating it if absent
    /// and pruning entries whose last handle has been dropped.
    pub fn get_or_create(&self, client_group_id: &str) -> Arc<RowFlushBarrier> {
        let mut map = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        map.retain(|_, barrier| barrier.strong_count() > 0);
        if let Some(barrier) = map.get(client_group_id).and_then(Weak::upgrade) {
            barrier
        } else {
            let barrier = Arc::new(RowFlushBarrier::new());
            map.insert(client_group_id.to_string(), Arc::downgrade(&barrier));
            barrier
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[tokio::test]
    async fn wait_returns_immediately_with_no_pending_flush() {
        let barrier = Arc::new(RowFlushBarrier::new());
        // Should not hang.
        barrier.wait_for_pending().await;
    }

    #[tokio::test]
    async fn wait_blocks_until_deferred_flush_completes() {
        let barrier = Arc::new(RowFlushBarrier::new());
        let flushed = Arc::new(AtomicBool::new(false));
        let flushed_in_task = flushed.clone();
        let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
        barrier.spawn_chained(async move {
            // Hold the flush open until the test releases the gate.
            let _ = gate_rx.await;
            flushed_in_task.store(true, Ordering::SeqCst);
        });

        // A waiter must not observe completion while the flush is still gated.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let waiter = tokio::spawn({
            let barrier = barrier.clone();
            async move { barrier.wait_for_pending().await }
        });
        assert!(!waiter.is_finished());
        assert!(!flushed.load(Ordering::SeqCst));

        gate_tx.send(()).unwrap();
        // The waiter must now unblock, and only after the flush actually ran.
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("waiter must unblock once the deferred flush lands")
            .unwrap();
        assert!(flushed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn chained_flushes_run_in_enqueue_order() {
        let barrier = Arc::new(RowFlushBarrier::new());
        let order = Arc::new(Mutex::new(Vec::new()));
        let counter = Arc::new(AtomicUsize::new(0));
        for i in 0..25 {
            let order = order.clone();
            let counter = counter.clone();
            barrier.spawn_chained(async move {
                // Yield to make out-of-order completion likely absent chaining.
                tokio::task::yield_now().await;
                let seq = counter.fetch_add(1, Ordering::SeqCst);
                order.lock().unwrap().push((i, seq));
            });
        }
        barrier.wait_for_pending().await;
        let order = order.lock().unwrap().clone();
        assert_eq!(order.len(), 25);
        for (i, (enqueued, ran)) in order.iter().enumerate() {
            assert_eq!(*enqueued, i, "flush {i} ran out of order");
            assert_eq!(*ran, i, "flush {i} completed out of order");
        }
    }

    #[tokio::test]
    async fn limiter_caps_concurrent_critical_sections() {
        // With a limit of 1, two "deferred flushes" must not run their guarded
        // critical section at the same time.
        let limiter = DeferFlushLimiter::new(1);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        // First flush acquires the only permit and holds it until released.
        let first = tokio::spawn({
            let limiter = limiter.clone();
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            async move {
                let _permit = limiter.acquire().await.expect("permit");
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                let _ = release_rx.await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
            }
        });

        // Give the first task time to take the permit and enter its section.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Second flush must block on acquire() while the first holds the permit.
        let second = tokio::spawn({
            let limiter = limiter.clone();
            let in_flight = in_flight.clone();
            let max_seen = max_seen.clone();
            async move {
                let _permit = limiter.acquire().await.expect("permit");
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                in_flight.fetch_sub(1, Ordering::SeqCst);
            }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        // The second task cannot have entered its section yet.
        assert!(!second.is_finished());
        assert_eq!(in_flight.load(Ordering::SeqCst), 1);

        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .expect("first completes")
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("second completes")
            .unwrap();

        // Concurrency never exceeded the limit, and the permit is returned.
        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.available(), 1);
    }

    #[test]
    fn registry_shares_one_barrier_per_group() {
        let barriers = RowFlushBarriers::new();
        let a = barriers.get_or_create("group-a");
        let a2 = barriers.get_or_create("group-a");
        let b = barriers.get_or_create("group-b");
        assert!(Arc::ptr_eq(&a, &a2));
        assert!(!Arc::ptr_eq(&a, &b));
    }
}
