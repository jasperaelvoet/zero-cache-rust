//! Partial port of `services/change-streamer/forwarder.ts`'s `Forwarder`.
//!
//! Scope: only the queued-vs-active subscriber bookkeeping —
//! [`SubscriberSet::add`]/[`SubscriberSet::remove`]/
//! [`SubscriberSet::update_on_tag`], the port of `add()`/`remove()`/
//! `#updateActiveSubscribers()`. This is the actual "fan-out" decision
//! logic: which subscribers are eligible to receive the next forwarded
//! change vs. queued until the in-flight transaction finishes.
//!
//! NOT ported (real gaps, not oversights): `Broadcast` (the actual
//! message-delivery/flow-control mechanism — `forward`/
//! `forwardWithFlowControl`), the observability metrics/gauges, the
//! progress-monitor timer, and `Subscriber` itself (the live
//! websocket-connected entity, 382 lines, unported). Subscribers are
//! modeled generically here as any `Eq + Hash + Clone` identifier — a real
//! caller would use a connection id or an `Rc`/`Arc` handle to a
//! `Subscriber` once that's ported.

use std::collections::HashSet;
use std::hash::Hash;

/// Tracks which subscribers are `active` (eligible to receive forwarded
/// changes right now) vs `queued` (added mid-transaction; held back until
/// the transaction commits or rolls back, matching `Storer.catchup()`'s
/// equivalent interpretation of transaction boundaries — see the module
/// doc on `add()` in the original). Port of the subscriber-bookkeeping
/// slice of `Forwarder`.
#[derive(Debug, Clone, Default)]
pub struct SubscriberSet<T: Eq + Hash + Clone> {
    active: HashSet<T>,
    queued: HashSet<T>,
    in_transaction: bool,
}

impl<T: Eq + Hash + Clone> SubscriberSet<T> {
    pub fn new() -> Self {
        SubscriberSet {
            active: HashSet::new(),
            queued: HashSet::new(),
            in_transaction: false,
        }
    }

    /// Port of `add()`: while a transaction is in flight, new subscribers
    /// are queued (so they don't see a partial transaction) rather than
    /// activated immediately.
    pub fn add(&mut self, sub: T) {
        if self.in_transaction {
            self.queued.insert(sub);
        } else {
            self.active.insert(sub);
        }
    }

    /// Port of `remove()`'s set-membership half (the `sub.close()` call is
    /// the caller's responsibility here, since this type has no I/O of its
    /// own). Returns whether `sub` was present in either set.
    pub fn remove(&mut self, sub: &T) -> bool {
        let was_active = self.active.remove(sub);
        let was_queued = self.queued.remove(sub);
        was_active || was_queued
    }

    /// Port of `#updateActiveSubscribers`: called with each forwarded
    /// change's tag. `"begin"` opens a transaction window (new `add()`s
    /// queue); `"commit"`/`"rollback"` close it and flush all queued
    /// subscribers into `active`. Any other tag is a no-op, matching the
    /// `switch` statement's implicit default case.
    pub fn update_on_tag(&mut self, tag: &str) {
        match tag {
            "begin" => self.in_transaction = true,
            "commit" | "rollback" => {
                self.in_transaction = false;
                for sub in self.queued.drain() {
                    self.active.insert(sub);
                }
            }
            _ => {}
        }
    }

    pub fn active(&self) -> &HashSet<T> {
        &self.active
    }

    pub fn queued(&self) -> &HashSet<T> {
        &self.queued
    }

    pub fn is_in_transaction(&self) -> bool {
        self.in_transaction
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_outside_transaction_goes_active() {
        let mut s: SubscriberSet<&str> = SubscriberSet::new();
        s.add("a");
        assert!(s.active().contains("a"));
        assert!(s.queued().is_empty());
    }

    #[test]
    fn add_during_transaction_goes_queued() {
        let mut s: SubscriberSet<&str> = SubscriberSet::new();
        s.update_on_tag("begin");
        s.add("a");
        assert!(s.queued().contains("a"));
        assert!(!s.active().contains("a"));
    }

    #[test]
    fn commit_flushes_queued_into_active() {
        let mut s: SubscriberSet<&str> = SubscriberSet::new();
        s.update_on_tag("begin");
        s.add("a");
        s.update_on_tag("commit");
        assert!(s.active().contains("a"));
        assert!(s.queued().is_empty());
        assert!(!s.is_in_transaction());
    }

    #[test]
    fn rollback_also_flushes_queued_into_active() {
        let mut s: SubscriberSet<&str> = SubscriberSet::new();
        s.update_on_tag("begin");
        s.add("a");
        s.update_on_tag("rollback");
        assert!(s.active().contains("a"));
        assert!(s.queued().is_empty());
    }

    #[test]
    fn other_tags_are_noop() {
        let mut s: SubscriberSet<&str> = SubscriberSet::new();
        s.add("a");
        s.update_on_tag("insert");
        assert!(s.active().contains("a"));
        assert!(!s.is_in_transaction());
    }

    #[test]
    fn remove_deletes_from_either_set_and_reports_presence() {
        let mut s: SubscriberSet<&str> = SubscriberSet::new();
        s.add("a");
        assert!(s.remove(&"a"));
        assert!(!s.active().contains("a"));
        assert!(!s.remove(&"a"));
    }

    #[test]
    fn remove_from_queued_set() {
        let mut s: SubscriberSet<&str> = SubscriberSet::new();
        s.update_on_tag("begin");
        s.add("a");
        assert!(s.remove(&"a"));
        assert!(s.queued().is_empty());
    }

    #[test]
    fn subscribers_added_after_commit_within_new_transaction_queue_again() {
        let mut s: SubscriberSet<&str> = SubscriberSet::new();
        s.update_on_tag("begin");
        s.update_on_tag("commit");
        s.update_on_tag("begin");
        s.add("a");
        assert!(s.queued().contains("a"));
    }
}
