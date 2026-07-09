//! Port of `TimeSliceTimer` (`view-syncer.ts`, the file's second exported
//! class alongside `ViewSyncerService`) — a lap-timer that accumulates
//! elapsed wall-clock time across cooperative-yield boundaries, used to
//! measure how much CPU time a hydration/advance pass actually burns
//! between yields to the event loop.
//!
//! Scope: the actual cooperative yield (`yieldProcess`, which `await`s a
//! real turn of the Node event loop) is NOT modeled — this port has no
//! async-scheduling equivalent, consistent with this project's established
//! stance on cooperative/generator scheduling elsewhere (`ivm`'s module
//! docs). What IS ported is the lap-timing bookkeeping around it: `start`/
//! `stop`, and the pairing of `stop_lap`+`start_lap` that a real yield
//! would sandwich. Every method takes `now: f64` as an explicit parameter
//! (this port's determinism convention) instead of reading `performance.now()`
//! ambiently.

/// Port of `TimeSliceTimer`. `start == 0.0` represents "not running"
/// (matching upstream's own sentinel, since a lap can never start at
/// exactly wall-clock zero in practice).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct TimeSliceTimer {
    total: f64,
    start: f64,
}

impl TimeSliceTimer {
    pub fn new() -> Self {
        TimeSliceTimer::default()
    }

    /// Port of `startWithoutYielding`: resets accumulated time and begins a
    /// lap. (Upstream's `start()` additionally `await`s a cooperative yield
    /// first, then calls this — the yield itself isn't modeled here.)
    pub fn start_without_yielding(&mut self, now: f64) {
        self.total = 0.0;
        self.start_lap(now);
    }

    fn start_lap(&mut self, now: f64) {
        assert_eq!(self.start, 0.0, "already running");
        self.start = now;
    }

    /// Port of `elapsedLap`: time elapsed in the current lap. Panics if no
    /// lap is running (matching upstream's assert).
    pub fn elapsed_lap(&self, now: f64) -> f64 {
        assert_ne!(self.start, 0.0, "not running");
        now - self.start
    }

    fn stop_lap(&mut self, now: f64) {
        assert_ne!(self.start, 0.0, "not running");
        self.total += now - self.start;
        self.start = 0.0;
    }

    /// Port of `yieldProcess`: stops the current lap and immediately starts
    /// a new one. Upstream sandwiches a real cooperative yield between the
    /// two — not modeled here (see module doc), so the two `now` values a
    /// caller passes for the stop/restart should be the two timestamps
    /// actually observed immediately before and after its own yield point.
    pub fn yield_process(&mut self, stop_now: f64, restart_now: f64) {
        self.stop_lap(stop_now);
        self.start_lap(restart_now);
    }

    /// Port of `stop`: ends the current lap and returns the total elapsed
    /// time across every lap.
    pub fn stop(&mut self, now: f64) -> f64 {
        self.stop_lap(now);
        self.total
    }

    /// Port of `totalElapsed`: the total elapsed time so far, whether the
    /// timer is currently running or already stopped.
    pub fn total_elapsed(&self, now: f64) -> f64 {
        if self.start == 0.0 {
            self.total
        } else {
            self.total + (now - self.start)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_without_yielding_resets_and_begins_a_lap() {
        let mut timer = TimeSliceTimer::new();
        timer.start_without_yielding(100.0);
        assert_eq!(timer.elapsed_lap(150.0), 50.0);
    }

    #[test]
    #[should_panic(expected = "already running")]
    fn start_lap_panics_if_already_running() {
        let mut timer = TimeSliceTimer::new();
        timer.start_without_yielding(100.0);
        timer.start_lap(150.0);
    }

    #[test]
    #[should_panic(expected = "not running")]
    fn elapsed_lap_panics_if_not_running() {
        let timer = TimeSliceTimer::new();
        timer.elapsed_lap(100.0);
    }

    #[test]
    fn stop_returns_total_across_a_single_lap() {
        let mut timer = TimeSliceTimer::new();
        timer.start_without_yielding(100.0);
        assert_eq!(timer.stop(180.0), 80.0);
    }

    #[test]
    fn yield_process_accumulates_across_laps() {
        // `now` values deliberately avoid the sentinel `0.0` used to mean
        // "not running" (matching upstream, which relies on `performance.now()`
        // never legitimately returning exactly zero).
        let mut timer = TimeSliceTimer::new();
        timer.start_without_yielding(1.0);
        timer.yield_process(11.0, 11.0); // first lap: 1->11 = 10 elapsed
        timer.yield_process(26.0, 26.0); // second lap: 11->26 = 15 elapsed
        assert_eq!(timer.stop(41.0), 10.0 + 15.0 + 15.0); // + third lap 26->41
    }

    #[test]
    fn total_elapsed_works_while_running_and_after_stopping() {
        let mut timer = TimeSliceTimer::new();
        timer.start_without_yielding(1.0);
        assert_eq!(
            timer.total_elapsed(31.0),
            30.0,
            "still running: total + time since start"
        );
        let stopped = timer.stop(31.0);
        assert_eq!(
            timer.total_elapsed(999.0),
            stopped,
            "once stopped, further `now` values don't matter"
        );
    }
}
