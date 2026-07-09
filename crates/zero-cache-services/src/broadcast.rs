//! Port of `services/change-streamer/broadcast.ts`'s consensus-based
//! flow-control timeout algorithm — the `checkProgress`/`#setDone` decision
//! logic of `Broadcast`, split from the actual message-sending/ack-tracking
//! machinery (see `change_streamer_forwarder.rs`'s module doc for the same
//! established boundary: subscribers are modeled generically as any
//! `Eq + Hash + Clone` identifier, not the real `Subscriber` websocket
//! entity, which remains unported).
//!
//! Determinism convention: `performance.now()` is taken as an explicit
//! `now: i64` (milliseconds) parameter everywhere, matching every other
//! ambient-clock-reading module in this port; `Broadcast.constructor`'s
//! implicit `#start = performance.now()` becomes an explicit `now` argument
//! to [`Broadcast::new`].
//!
//! NOT ported: the actual `sub.send(change)` broadcast + `.catch()`/
//! `.finally()` completion wiring (real I/O, needs `Subscriber`), the
//! `#logWithState` diagnostic logging (left to the caller, matching this
//! port's LogContext-free convention — [`Broadcast::check_progress`]
//! returns whether it just decided to log-worthy states via its return
//! value and [`Broadcast::completed_count`]/[`Broadcast::pending_count`]
//! rather than logging itself).

use std::collections::HashSet;
use std::hash::Hash;

/// Port of `BroadcastReleaseMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseMode {
    AllSubscribers,
    ConsensusTimeout,
}

/// Port of the `Broadcast` class's progress-tracking state and
/// `checkProgress`/`#setDone`/`#markCompleted` decision logic.
pub struct Broadcast<T: Eq + Hash + Clone> {
    pending: HashSet<T>,
    completed_count: usize,
    is_done: bool,
    release_mode: Option<ReleaseMode>,
    majority: usize,
    start: i64,
    latest_completed: i64,
}

impl<T: Eq + Hash + Clone> Broadcast<T> {
    /// Port of the constructor's tracking-state setup (the actual
    /// `sub.send(change)` fan-out is the caller's responsibility — see
    /// module doc). `now` stands in for the implicit `performance.now()`
    /// read at construction time.
    pub fn new(subscribers: impl IntoIterator<Item = T>, now: i64) -> Self {
        let pending: HashSet<T> = subscribers.into_iter().collect();
        let majority = pending.len() / 2 + 1;
        let mut broadcast = Broadcast {
            pending,
            completed_count: 0,
            is_done: false,
            release_mode: None,
            majority,
            start: now,
            latest_completed: i64::MAX,
        };
        // "set done if there are no subscribers (mainly for tests)"
        if broadcast.pending.is_empty() {
            broadcast.set_done(ReleaseMode::AllSubscribers);
        }
        broadcast
    }

    fn set_done(&mut self, release_mode: ReleaseMode) {
        if self.is_done {
            return;
        }
        self.is_done = true;
        self.release_mode = Some(release_mode);
    }

    /// Port of `#markCompleted`: called by the caller once a subscriber's
    /// send has actually completed (or failed — upstream's `.catch(() =>
    /// {})` treats a failed send as completed too, so callers should call
    /// this either way).
    pub fn mark_completed(&mut self, sub: &T, now: i64) {
        if !self.pending.remove(sub) {
            return;
        }
        self.completed_count += 1;
        self.latest_completed = now;
        if self.pending.is_empty() {
            self.set_done(ReleaseMode::AllSubscribers);
        }
    }

    pub fn is_done(&self) -> bool {
        self.is_done
    }

    /// Port of the `releaseMode` getter (defaults to `AllSubscribers` when
    /// not yet done, matching upstream's `?? 'all-subscribers'`).
    pub fn release_mode(&self) -> ReleaseMode {
        self.release_mode.unwrap_or(ReleaseMode::AllSubscribers)
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn completed_count(&self) -> usize {
        self.completed_count
    }

    /// Milliseconds elapsed since construction. Port of `checkProgress`'s
    /// `elapsed` diagnostic value, exposed for callers that want to log it
    /// (see module doc: this port doesn't log on the caller's behalf).
    pub fn elapsed_ms(&self, now: i64) -> i64 {
        now - self.start
    }

    /// Port of `checkProgress`: the consensus-based timeout decision. Once
    /// a majority of subscribers have completed, the broadcast is released
    /// early if no further completions land within
    /// `flow_control_consensus_padding_ms` of the last one — see the
    /// upstream doc comment on `checkProgress` for the full algorithm
    /// rationale. Returns `true` if the broadcast was already done or was
    /// just marked done by this call.
    pub fn check_progress(&mut self, flow_control_consensus_padding_ms: i64, now: i64) -> bool {
        if self.is_done {
            return true;
        }
        if self.pending.is_empty() {
            return true;
        }
        if self.completed_count < self.majority {
            return false;
        }
        if now - self.latest_completed >= flow_control_consensus_padding_ms {
            self.set_done(ReleaseMode::ConsensusTimeout);
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_subscriber_set_is_done_immediately() {
        let b: Broadcast<&str> = Broadcast::new([], 0);
        assert!(b.is_done());
        assert_eq!(b.release_mode(), ReleaseMode::AllSubscribers);
    }

    #[test]
    fn majority_is_floor_half_plus_one() {
        // 4 subscribers -> majority 3 (floor(4/2)+1).
        let b: Broadcast<&str> = Broadcast::new(["a", "b", "c", "d"], 0);
        assert_eq!(b.majority, 3);
        // 1 subscriber -> majority 1 (waiting for all, single-node case).
        let b: Broadcast<&str> = Broadcast::new(["a"], 0);
        assert_eq!(b.majority, 1);
    }

    #[test]
    fn done_when_all_subscribers_complete() {
        let mut b = Broadcast::new(["a", "b"], 0);
        assert!(!b.is_done());
        b.mark_completed(&"a", 10);
        assert!(!b.is_done());
        b.mark_completed(&"b", 20);
        assert!(b.is_done());
        assert_eq!(b.release_mode(), ReleaseMode::AllSubscribers);
    }

    #[test]
    fn check_progress_waits_until_majority_completes() {
        let mut b = Broadcast::new(["a", "b", "c"], 0); // majority = 2
        b.mark_completed(&"a", 100);
        assert!(
            !b.check_progress(1000, 100),
            "only 1/3 completed, below majority"
        );
    }

    #[test]
    fn check_progress_releases_after_padding_once_majority_reached() {
        let mut b = Broadcast::new(["a", "b", "c"], 0); // majority = 2
        b.mark_completed(&"a", 100);
        b.mark_completed(&"b", 200); // majority reached at t=200
        assert!(
            !b.check_progress(500, 300),
            "padding hasn't elapsed yet (100ms < 500ms)"
        );
        assert!(!b.is_done());
        assert!(
            b.check_progress(500, 700),
            "500ms have elapsed since the last completion at t=200"
        );
        assert!(b.is_done());
        assert_eq!(b.release_mode(), ReleaseMode::ConsensusTimeout);
    }

    #[test]
    fn check_progress_resets_the_padding_clock_on_each_new_completion() {
        let mut b = Broadcast::new(["a", "b", "c"], 0);
        b.mark_completed(&"a", 100);
        b.mark_completed(&"b", 200); // majority reached
        assert!(
            !b.check_progress(500, 600),
            "400ms since last completion, still under 500ms padding"
        );
        b.mark_completed(&"c", 650); // a NEW completion resets the clock (and also finishes everyone)
        assert!(
            b.is_done(),
            "all subscribers completed before the timeout ever fired"
        );
        assert_eq!(
            b.release_mode(),
            ReleaseMode::AllSubscribers,
            "finished via completion, not timeout"
        );
    }

    #[test]
    fn check_progress_is_idempotent_once_done() {
        let mut b = Broadcast::new(["a"], 0);
        b.mark_completed(&"a", 5);
        assert!(b.check_progress(1000, 999_999));
        assert_eq!(b.release_mode(), ReleaseMode::AllSubscribers);
    }

    #[test]
    fn elapsed_ms_measures_from_construction() {
        let b: Broadcast<&str> = Broadcast::new(["a"], 100);
        assert_eq!(b.elapsed_ms(150), 50);
    }

    #[test]
    fn mark_completed_ignores_an_unknown_or_already_completed_subscriber() {
        let mut b = Broadcast::new(["a", "b"], 0);
        b.mark_completed(&"a", 10);
        b.mark_completed(&"a", 20); // already completed, must not double-count
        assert_eq!(b.completed_count(), 1);
        b.mark_completed(&"unknown", 30); // never was pending
        assert_eq!(b.completed_count(), 1);
    }
}
