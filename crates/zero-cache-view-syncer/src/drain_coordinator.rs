//! Port of `DrainCoordinator` (`view-syncer/drain-coordinator.ts`) — the pure
//! scheduling core that decides *when* a view-syncer should drain its
//! connections onto another server.
//!
//! Background: draining happens two ways. An **elective** drain occurs when a
//! view-syncer, about to process a replication event, notices `should_drain()`
//! is true and exits its loop voluntarily (preferred — low variance). A
//! **forced** drain is imposed by the Syncer when a server has no work to
//! process electively, via a timeout. The Syncer kicks the whole process off
//! with `drain_next_in(0)`, which flips `should_drain()` to true and arms the
//! force-drain timeout; each electively-drained view-syncer then calls
//! `drain_next_in(my_hydration_time)` to space out the next drain.
//!
//! Scope: the `@rocicorp/resolver` promises (`draining`, `forceDrainTimeout`)
//! and the `setTimeout`/`clearTimeout` machinery are real async orchestration
//! this port doesn't drive — no event loop to hook into (consistent with
//! `time_slice_timer.rs`'s stance on cooperative scheduling). What IS ported is
//! the deterministic bookkeeping those wrappers guard: the `next_drain_time`
//! computation, the `should_drain()` comparison, and the `interval /
//! TARGET_UTILIZATION` / force-drain-deadline arithmetic. `now: i64`
//! (milliseconds, matching JS `Date.now()`) is an explicit parameter on every
//! method rather than read ambiently (this port's determinism convention).

/// The target (additional) utilization to impose on the server that receives
/// the drained connections. Draining `interval` is divided by this so the
/// receiving server gets breathing room for its own normal processing.
pub const TARGET_UTILIZATION: f64 = 0.6;

/// Padding (ms) added on top of the drain interval before a forced drain
/// fires, giving an elective drain a chance to happen first.
pub const FORCE_DRAIN_PADDING: i64 = 2;

/// Port of `DrainCoordinator`'s deterministic state. `next_drain_time == 0`
/// means "no drain scheduled" (matching upstream's own `#nextDrainTime = 0`
/// sentinel — a real `Date.now()` is never legitimately zero).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DrainCoordinator {
    next_drain_time: i64,
}

impl DrainCoordinator {
    pub fn new() -> Self {
        DrainCoordinator::default()
    }

    /// Port of `shouldDrain()`: true once a drain has been scheduled
    /// (`next_drain_time != 0`) and its scheduled time has arrived.
    pub fn should_drain(&self, now: i64) -> bool {
        self.next_drain_time != 0 && self.next_drain_time <= now
    }

    /// Port of `drainNextIn(interval)`'s state transition. Schedules the next
    /// drain `interval / TARGET_UTILIZATION` milliseconds after `now` and
    /// returns the force-drain timeout *duration* (`scaled_interval +
    /// FORCE_DRAIN_PADDING`, relative to `now`) that upstream arms its
    /// `setTimeout` for. Panics if `should_drain()` was
    /// not already true (i.e. `next_drain_time > now`), matching upstream's
    /// `assert(this.#nextDrainTime <= now, ...)` — this method must only be
    /// called by a caller that has just observed a drain is due (or by the
    /// Syncer's initial `drain_next_in(0)`).
    pub fn drain_next_in(&mut self, interval: i64, now: i64) -> i64 {
        assert!(
            self.next_drain_time <= now,
            "drain_next_in() should only be called if should_drain()"
        );
        // Increase the timeout between drains to give the receiving server
        // space to perform normal processing.
        let interval = (interval as f64 / TARGET_UTILIZATION) as i64;
        self.next_drain_time = now + interval;
        interval + FORCE_DRAIN_PADDING
    }

    /// Port of `get nextDrainTime()` (exposed for testing upstream too).
    pub fn next_drain_time(&self) -> i64 {
        self.next_drain_time
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_coordinator_never_drains() {
        let dc = DrainCoordinator::new();
        assert!(!dc.should_drain(0));
        assert!(!dc.should_drain(1_000_000));
        assert_eq!(dc.next_drain_time(), 0);
    }

    #[test]
    fn drain_next_in_zero_schedules_immediately() {
        // The Syncer's kickoff: drain_next_in(0) at now=1000.
        let mut dc = DrainCoordinator::new();
        let deadline = dc.drain_next_in(0, 1000);
        // interval 0 / 0.6 = 0, so next drain is now and force deadline is
        // now + padding.
        assert_eq!(dc.next_drain_time(), 1000);
        // `deadline` is a relative timeout duration (upstream's
        // `setTimeout(..., interval + padding)`), not an absolute time.
        assert_eq!(deadline, 0 + FORCE_DRAIN_PADDING);
        assert!(dc.should_drain(1000), "due exactly at now");
    }

    #[test]
    fn interval_is_scaled_by_target_utilization() {
        let mut dc = DrainCoordinator::new();
        // 60ms hydration time / 0.6 = 100ms until next drain.
        let deadline = dc.drain_next_in(60, 1000);
        assert_eq!(dc.next_drain_time(), 1100);
        assert_eq!(deadline, 100 + FORCE_DRAIN_PADDING);
    }

    #[test]
    fn should_drain_is_false_before_and_true_at_or_after_scheduled_time() {
        let mut dc = DrainCoordinator::new();
        dc.drain_next_in(60, 1000); // next_drain_time = 1100
        assert!(!dc.should_drain(1099));
        assert!(dc.should_drain(1100));
        assert!(dc.should_drain(1101));
    }

    #[test]
    #[should_panic(expected = "should only be called if should_drain")]
    fn drain_next_in_panics_if_not_yet_due() {
        let mut dc = DrainCoordinator::new();
        dc.drain_next_in(60, 1000); // next_drain_time = 1100
                                    // Calling again before 1100 violates the invariant.
        dc.drain_next_in(60, 1050);
    }

    #[test]
    fn successive_elective_drains_reschedule_from_each_now() {
        let mut dc = DrainCoordinator::new();
        dc.drain_next_in(0, 1000); // schedule at 1000
        assert!(dc.should_drain(1000));
        // Elective drain fires at 1200 with a 30ms hydration time.
        let deadline = dc.drain_next_in(30, 1200);
        assert_eq!(dc.next_drain_time(), 1200 + 50); // 30/0.6 = 50
        assert_eq!(deadline, 50 + FORCE_DRAIN_PADDING);
    }
}
