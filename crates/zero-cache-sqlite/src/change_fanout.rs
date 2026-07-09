//! Live multi-subscriber fan-out for committed changes — the hub half of the
//! change-streamer's `Storer`/`Subscriber` fan-out.
//!
//! The change-stream loop ([`crate::change_stream_loop::run_change_stream`])
//! consumes the *single* Postgres replication stream and writes each committed
//! transaction to the durable change-log. Many view-syncer subscribers then
//! need those commits, but they must NOT each re-read Postgres — instead one
//! writer fans out to N subscribers. This module is that fan-out: a
//! [`ChangeFanout`] hub that live subscribers register with and that the loop
//! publishes each commit's watermark to.
//!
//! A reconnecting subscriber first *catches up* from the change-log
//! ([`crate::change_log::ChangeLog::read_since`]) up to the current watermark,
//! then follows this live channel for new commits — the standard
//! catchup-then-stream handoff. This hub is the "then follows the live channel"
//! half; the pure back-pressure/watermark decision logic for a slow subscriber
//! is already ported in `zero-cache-services::subscriber_backpressure`.
//!
//! Built on `tokio::sync::broadcast`: each commit is delivered to every current
//! subscriber; a subscriber that falls too far behind the channel's buffer gets
//! a `Lagged` signal (the caller then re-catches-up from the change-log rather
//! than dropping data — the same recovery path a fresh subscriber uses).

use tokio::sync::broadcast;

/// A committed transaction announcement fanned out to subscribers: the
/// watermark it committed at, plus whether it changed the schema (subscribers
/// may need to react to DDL) and how many change-log entries it wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitNotification {
    pub watermark: String,
    pub schema_changed: bool,
    pub num_change_log_entries: i64,
}

impl From<&crate::change_dispatcher::CommitResult> for CommitNotification {
    fn from(c: &crate::change_dispatcher::CommitResult) -> Self {
        CommitNotification {
            watermark: c.watermark.clone(),
            schema_changed: c.schema_changed,
            num_change_log_entries: c.num_change_log_entries,
        }
    }
}

/// What a subscriber received when polling the live channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanoutEvent {
    /// A new commit was fanned out.
    Commit(CommitNotification),
    /// The subscriber fell behind the channel buffer by `skipped` commits and
    /// must re-catch-up from the change-log (`read_since` its last watermark)
    /// before following the live channel again. No commit data is lost — it is
    /// still durably in the change-log.
    Lagged { skipped: u64 },
    /// The hub was dropped (the change-stream loop stopped); no more commits.
    Closed,
}

/// A live subscriber's handle: a receiver on the fan-out channel.
pub struct FanoutSubscriber {
    rx: broadcast::Receiver<CommitNotification>,
}

impl FanoutSubscriber {
    /// Awaits the next fan-out event.
    pub async fn recv(&mut self) -> FanoutEvent {
        match self.rx.recv().await {
            Ok(c) => FanoutEvent::Commit(c),
            Err(broadcast::error::RecvError::Lagged(skipped)) => FanoutEvent::Lagged { skipped },
            Err(broadcast::error::RecvError::Closed) => FanoutEvent::Closed,
        }
    }

    /// Non-blocking poll: returns `None` if no event is currently buffered.
    pub fn try_recv(&mut self) -> Option<FanoutEvent> {
        match self.rx.try_recv() {
            Ok(c) => Some(FanoutEvent::Commit(c)),
            Err(broadcast::error::TryRecvError::Lagged(skipped)) => {
                Some(FanoutEvent::Lagged { skipped })
            }
            Err(broadcast::error::TryRecvError::Closed) => Some(FanoutEvent::Closed),
            Err(broadcast::error::TryRecvError::Empty) => None,
        }
    }
}

/// The fan-out hub. The change-stream loop holds one and calls [`publish`] on
/// each commit; each view-syncer subscriber calls [`subscribe`] to follow.
///
/// [`publish`]: ChangeFanout::publish
/// [`subscribe`]: ChangeFanout::subscribe
pub struct ChangeFanout {
    tx: broadcast::Sender<CommitNotification>,
}

impl ChangeFanout {
    /// Creates a hub whose per-subscriber buffer holds up to `capacity` commits
    /// before a slow subscriber is told to re-catch-up (`Lagged`).
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(1));
        ChangeFanout { tx }
    }

    /// Registers a new live subscriber. It only receives commits published
    /// *after* this call — a subscriber must catch up from the change-log up to
    /// the current watermark first, then start following, to avoid a gap.
    pub fn subscribe(&self) -> FanoutSubscriber {
        FanoutSubscriber {
            rx: self.tx.subscribe(),
        }
    }

    /// Fans a commit out to all current subscribers. Returns the number of
    /// subscribers it was delivered to (0 if none are listening — not an
    /// error; the change-log remains the durable record).
    pub fn publish(&self, commit: CommitNotification) -> usize {
        self.tx.send(commit).unwrap_or(0)
    }

    /// The number of live subscribers currently registered.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(watermark: &str) -> CommitNotification {
        CommitNotification {
            watermark: watermark.into(),
            schema_changed: false,
            num_change_log_entries: 1,
        }
    }

    #[tokio::test]
    async fn fans_out_each_commit_to_every_subscriber_in_order() {
        let hub = ChangeFanout::new(16);
        let mut a = hub.subscribe();
        let mut b = hub.subscribe();
        let mut c = hub.subscribe();
        assert_eq!(hub.subscriber_count(), 3);

        assert_eq!(hub.publish(commit("01")), 3, "delivered to all three");
        assert_eq!(hub.publish(commit("02")), 3);

        for sub in [&mut a, &mut b, &mut c] {
            assert_eq!(sub.recv().await, FanoutEvent::Commit(commit("01")));
            assert_eq!(sub.recv().await, FanoutEvent::Commit(commit("02")));
        }
    }

    #[tokio::test]
    async fn late_subscriber_only_sees_commits_after_it_joined() {
        let hub = ChangeFanout::new(16);
        hub.publish(commit("01")); // no subscribers yet
        let mut late = hub.subscribe();
        hub.publish(commit("02"));

        // The late subscriber must catch up "01" from the change-log; on the
        // live channel it only sees "02".
        assert_eq!(late.recv().await, FanoutEvent::Commit(commit("02")));
        assert_eq!(late.try_recv(), None, "nothing else buffered");
    }

    #[tokio::test]
    async fn slow_subscriber_is_told_to_recatchup_on_overflow() {
        // Capacity 2: publishing 3 without draining overflows the slow one.
        let hub = ChangeFanout::new(2);
        let mut slow = hub.subscribe();
        hub.publish(commit("01"));
        hub.publish(commit("02"));
        hub.publish(commit("03"));

        // First recv reports the lag (skipped the overflowed commit), signaling
        // the caller to re-catch-up from the change-log — no data is lost.
        match slow.recv().await {
            FanoutEvent::Lagged { skipped } => assert!(skipped >= 1),
            other => panic!("expected Lagged, got {other:?}"),
        }
        // After the lag signal, the newest buffered commits are still available.
        assert_eq!(slow.recv().await, FanoutEvent::Commit(commit("02")));
        assert_eq!(slow.recv().await, FanoutEvent::Commit(commit("03")));
    }

    #[tokio::test]
    async fn subscribers_see_closed_when_hub_dropped() {
        let hub = ChangeFanout::new(4);
        let mut sub = hub.subscribe();
        hub.publish(commit("01"));
        drop(hub);
        // Buffered commit still delivered, then Closed.
        assert_eq!(sub.recv().await, FanoutEvent::Commit(commit("01")));
        assert_eq!(sub.recv().await, FanoutEvent::Closed);
    }

    #[test]
    fn commit_notification_from_dispatcher_result() {
        let cr = crate::change_dispatcher::CommitResult {
            watermark: "0a".into(),
            schema_changed: true,
            num_change_log_entries: 5,
        };
        let n = CommitNotification::from(&cr);
        assert_eq!(
            n,
            CommitNotification {
                watermark: "0a".into(),
                schema_changed: true,
                num_change_log_entries: 5
            }
        );
    }
}
