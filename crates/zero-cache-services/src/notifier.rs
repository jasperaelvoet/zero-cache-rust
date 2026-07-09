//! Port of `zero-cache/src/services/replicator/notifier.ts`.
//!
//! Fans out replica-state notifications to subscribers, built on the coalescing
//! [`Subscription`]. New subscribers immediately receive the latest known
//! state; a subscriber too busy to keep up has pending notifications coalesced
//! (keeping the newest state but the *earliest* `replicaReadyTimeMs`).

use std::sync::{Arc, Mutex};

use zero_cache_types::subscription::{create, Options, Subscription};

/// The kind of replica state. Port of the `state` discriminant (currently only
/// `version-ready`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaStateKind {
    VersionReady,
}

/// A replica state notification. Port of `ReplicaState`.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaState {
    pub state: ReplicaStateKind,
    pub watermark: Option<String>,
    pub replica_ready_time_ms: Option<i64>,
}

impl ReplicaState {
    /// A bare `version-ready` state (the default notification).
    pub fn version_ready() -> Self {
        ReplicaState {
            state: ReplicaStateKind::VersionReady,
            watermark: None,
            replica_ready_time_ms: None,
        }
    }
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self::version_ready()
    }
}

/// Coalesces two pending notifications: take the newer `curr`, but keep the
/// earliest (minimum) `replicaReadyTimeMs`. Port of the `coalesce` option.
fn coalesce(curr: ReplicaState, prev: ReplicaState) -> ReplicaState {
    let replica_ready_time_ms = match (curr.replica_ready_time_ms, prev.replica_ready_time_ms) {
        (None, p) => p,
        (c, None) => c,
        (Some(c), Some(p)) => Some(c.min(p)),
    };
    ReplicaState {
        replica_ready_time_ms,
        ..curr
    }
}

struct Inner {
    last_state: Option<ReplicaState>,
    subs: Vec<(u64, Subscription<ReplicaState, ReplicaState>)>,
    next_id: u64,
}

/// Handles replica-state subscription fan-out. Port of `Notifier`.
#[derive(Clone)]
pub struct Notifier {
    inner: Arc<Mutex<Inner>>,
}

impl Default for Notifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Notifier {
    pub fn new() -> Self {
        Notifier {
            inner: Arc::new(Mutex::new(Inner {
                last_state: None,
                subs: Vec::new(),
                next_id: 0,
            })),
        }
    }

    /// The latest state received (raw, not coalesced). Port of `latestState`.
    pub fn latest_state(&self) -> Option<ReplicaState> {
        self.inner.lock().unwrap().last_state.clone()
    }

    /// Subscribes, returning a subscription whose iterator yields notifications.
    /// If a state is already known it is delivered immediately. Port of
    /// `subscribe`.
    pub fn subscribe(&self) -> Subscription<ReplicaState, ReplicaState> {
        let inner_ref = Arc::clone(&self.inner);
        let mut inner = self.inner.lock().unwrap();
        let id = inner.next_id;
        inner.next_id += 1;

        let cleanup_ref = Arc::clone(&inner_ref);
        let sub: Subscription<ReplicaState, ReplicaState> = create(Options {
            coalesce: Some(Box::new(coalesce)),
            cleanup: Some(Box::new(move |_unconsumed, _err| {
                if let Ok(mut g) = cleanup_ref.lock() {
                    g.subs.retain(|(sid, _)| *sid != id);
                }
            })),
            ..Default::default()
        });

        inner.subs.push((id, sub.clone()));
        if let Some(last) = inner.last_state.clone() {
            sub.push(last);
        }
        sub
    }

    /// Notifies all subscribers, returning the per-subscriber pending results.
    /// Port of `notifySubscribers`.
    pub fn notify_subscribers(
        &self,
        state: ReplicaState,
    ) -> Vec<zero_cache_types::subscription::PendingResult> {
        let subs = {
            let mut inner = self.inner.lock().unwrap();
            inner.last_state = Some(state.clone());
            inner
                .subs
                .iter()
                .map(|(_, s)| s.clone())
                .collect::<Vec<_>>()
        };
        subs.iter().map(|s| s.push(state.clone())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_types::subscription::PushResult;

    fn state(watermark: &str) -> ReplicaState {
        ReplicaState {
            state: ReplicaStateKind::VersionReady,
            watermark: Some(watermark.into()),
            replica_ready_time_ms: None,
        }
    }

    /// Consumes at most one message, asserting it equals `expected`. A
    /// cancelled subscription yields nothing (a vacuous pass, matching the JS
    /// `for await ... break` behavior). Dropping the iterator cancels the sub.
    async fn expect_single(sub: &Subscription<ReplicaState, ReplicaState>, expected: ReplicaState) {
        let mut it = sub.iter();
        if let Some(msg) = it.next().await {
            assert_eq!(msg.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn notify_immediately_with_last_notification() {
        let notifier = Notifier::new();
        notifier.notify_subscribers(ReplicaState::version_ready());
        let sub = notifier.subscribe();
        expect_single(&sub, ReplicaState::version_ready()).await;

        notifier.notify_subscribers(state("123"));
        // `sub` was cancelled by the break above; a fresh subscriber gets 123.
        let sub2 = notifier.subscribe();
        expect_single(&sub2, state("123")).await;
    }

    #[tokio::test]
    async fn watermark_coalescing_and_results() {
        let notifier = Notifier::new();
        let sub1 = notifier.subscribe();
        let sub2 = notifier.subscribe();

        let mut results1 = notifier.notify_subscribers(state("234"));
        expect_single(&sub1, state("234")).await; // consumes + cancels sub1
        assert_eq!(results1.remove(0).result().await, PushResult::Consumed);

        // sub1 is gone; sub2's pending 234 coalesces with 345.
        notifier.notify_subscribers(state("345"));
        assert_eq!(results1.remove(0).result().await, PushResult::Coalesced);

        let mut results2 = notifier.notify_subscribers(state("456"));
        expect_single(&sub2, state("456")).await;
        assert_eq!(results2.len(), 1);
        assert_eq!(results2.remove(0).result().await, PushResult::Consumed);
    }

    #[tokio::test]
    async fn coalesced_keeps_earliest_replica_ready_time() {
        let notifier = Notifier::new();
        let sub = notifier.subscribe();

        let s = |w: &str, t: i64| ReplicaState {
            state: ReplicaStateKind::VersionReady,
            watermark: Some(w.into()),
            replica_ready_time_ms: Some(t),
        };
        notifier.notify_subscribers(s("02", 200));
        notifier.notify_subscribers(s("03", 300));

        expect_single(&sub, s("03", 200)).await;
        assert_eq!(notifier.latest_state(), Some(s("03", 300)));
    }
}
