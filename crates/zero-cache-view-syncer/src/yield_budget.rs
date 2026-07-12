//! The `ZERO_YIELD_THRESHOLD_MS` cooperative-yield facility — the port of
//! how upstream wires `config.yieldThresholdMs` into `PipelineDriver`
//! (`server/syncer.ts` lines 201–225 build the `yieldThresholdMs` thunk;
//! `pipeline-driver.ts` compares `timer.elapsedLap() > this.#yieldThresholdMs()`
//! at lines 1080/1156 and emits `'yield'` sentinels the async caller awaits).
//!
//! Upstream's thunk is dynamic per call: the effective threshold is
//! `max(yieldThresholdMs / 4, 2)` while a priority op is running (yield much
//! more eagerly so the priority op gets the thread) and
//! `max(yieldThresholdMs, 2)` otherwise. [`effective_yield_threshold`] ports
//! that computation and [`YieldBudget::from_config`] composes it with an
//! injected `is_priority_op_running` probe, mirroring `syncer.ts`'s
//! `() => isPriorityOpRunning() ? priorityOpRunningYieldThresholdMs :
//! normalYieldThresholdMs`.
//!
//! Where upstream's generator machinery yields `'yield'` markers up to an
//! async driver, this port's equivalent is [`YieldBudget::maybe_yield`]: an
//! async method that calls `tokio::task::yield_now().await` once the current
//! lap exceeds the threshold, then starts a new lap. Synchronous loops that
//! cannot await (this port's pipeline drivers are sync — see
//! `graph_pipeline_driver`) can instead poll [`YieldBudget::should_yield`],
//! return control to their async caller, and mark the boundary with
//! [`YieldBudget::note_yielded`] — the same stop-lap/start-lap pairing
//! `time_slice_timer::TimeSliceTimer::yield_process` models.
//!
//! No env/config reads here: the server crate resolves
//! `ZERO_YIELD_THRESHOLD_MS` and injects it.

use std::time::{Duration, Instant};

/// Upstream's floor: `Math.max(..., 2)` milliseconds (`syncer.ts` lines
/// 201–205) — a threshold can never be configured below 2ms, so a
/// pathological `ZERO_YIELD_THRESHOLD_MS=0` cannot turn every row into a
/// yield.
pub const MIN_YIELD_THRESHOLD: Duration = Duration::from_millis(2);

/// Port of `syncer.ts`'s threshold selection: while a priority op is running
/// the effective threshold is `max(threshold_ms / 4, 2ms)` (yield eagerly),
/// otherwise `max(threshold_ms, 2ms)`. The division is computed in
/// microseconds so e.g. the default `threshold_ms = 10` yields the same
/// `2.5ms` upstream's float division produces, not a truncated `2ms`.
pub fn effective_yield_threshold(threshold_ms: u64, priority_op_running: bool) -> Duration {
    let base = if priority_op_running {
        Duration::from_micros(threshold_ms.saturating_mul(1000) / 4)
    } else {
        Duration::from_millis(threshold_ms)
    };
    base.max(MIN_YIELD_THRESHOLD)
}

/// A lap-based cooperative-yield budget: tracks how long the current
/// uninterrupted work lap has run and whether it has exceeded the (possibly
/// dynamic) threshold. See the module doc for the upstream mapping.
pub struct YieldBudget {
    threshold: Box<dyn Fn() -> Duration + Send>,
    last_yield: Instant,
}

impl std::fmt::Debug for YieldBudget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("YieldBudget")
            .field("threshold", &(self.threshold)())
            .field("lap_elapsed", &self.last_yield.elapsed())
            .finish()
    }
}

impl YieldBudget {
    /// A budget with a dynamic threshold, consulted on every check — the
    /// port of upstream's `yieldThresholdMs: () => number` thunk parameter
    /// (`pipeline-driver.ts` line 274). The lap starts now.
    pub fn new(threshold: impl Fn() -> Duration + Send + 'static) -> Self {
        YieldBudget {
            threshold: Box::new(threshold),
            last_yield: Instant::now(),
        }
    }

    /// A budget with a constant threshold.
    pub fn fixed(threshold: Duration) -> Self {
        YieldBudget::new(move || threshold)
    }

    /// The full upstream wiring in one constructor: a configured
    /// `ZERO_YIELD_THRESHOLD_MS` value plus an `isPriorityOpRunning` probe,
    /// composed through [`effective_yield_threshold`] exactly as
    /// `syncer.ts` composes its two precomputed thresholds.
    pub fn from_config(
        threshold_ms: u64,
        is_priority_op_running: impl Fn() -> bool + Send + 'static,
    ) -> Self {
        YieldBudget::new(move || effective_yield_threshold(threshold_ms, is_priority_op_running()))
    }

    /// Whether the current lap has exceeded the threshold — the port of
    /// `timer.elapsedLap() > this.#yieldThresholdMs()` (`pipeline-driver.ts`
    /// lines 1080/1156). Synchronous loops poll this and return control to
    /// their async caller when it turns true.
    pub fn should_yield(&self) -> bool {
        self.last_yield.elapsed() > (self.threshold)()
    }

    /// Starts a new lap without yielding — for callers that surrendered the
    /// thread by some other means (e.g. a sync loop that returned to an
    /// async driver which awaited on its own).
    pub fn note_yielded(&mut self) {
        self.last_yield = Instant::now();
    }

    /// Yields to the tokio scheduler if the lap budget is exhausted, then
    /// starts a new lap. Returns whether a yield actually happened — the
    /// async-native equivalent of upstream's `'yield'` sentinel +
    /// `TimeSliceTimer.yieldProcess` pairing.
    pub async fn maybe_yield(&mut self) -> bool {
        if !self.should_yield() {
            return false;
        }
        tokio::task::yield_now().await;
        self.note_yielded();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn effective_threshold_uses_the_configured_value_when_no_priority_op() {
        assert_eq!(
            effective_yield_threshold(10, false),
            Duration::from_millis(10)
        );
        assert_eq!(
            effective_yield_threshold(100, false),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn effective_threshold_quarters_while_a_priority_op_runs() {
        // Default threshold 10ms -> 2.5ms, matching upstream's float
        // division (not a truncated 2ms).
        assert_eq!(
            effective_yield_threshold(10, true),
            Duration::from_micros(2500)
        );
        assert_eq!(
            effective_yield_threshold(100, true),
            Duration::from_millis(25)
        );
    }

    #[test]
    fn effective_threshold_never_drops_below_two_ms() {
        assert_eq!(effective_yield_threshold(0, false), MIN_YIELD_THRESHOLD);
        assert_eq!(effective_yield_threshold(0, true), MIN_YIELD_THRESHOLD);
        assert_eq!(effective_yield_threshold(1, false), MIN_YIELD_THRESHOLD);
        // 4ms / 4 = 1ms -> clamped to 2ms.
        assert_eq!(effective_yield_threshold(4, true), MIN_YIELD_THRESHOLD);
    }

    #[test]
    fn should_yield_is_false_within_budget_and_true_after_the_lap_exceeds_it() {
        let budget = YieldBudget::fixed(Duration::from_millis(1));
        assert!(!budget.should_yield(), "a fresh lap is within budget");
        std::thread::sleep(Duration::from_millis(5));
        assert!(budget.should_yield(), "5ms lap exceeds a 1ms budget");
    }

    #[test]
    fn should_yield_stays_false_under_a_generous_budget() {
        let budget = YieldBudget::fixed(Duration::from_secs(3600));
        std::thread::sleep(Duration::from_millis(3));
        assert!(!budget.should_yield());
    }

    #[test]
    fn dynamic_threshold_is_consulted_on_every_check() {
        // The thunk semantics: flipping the priority flag changes the
        // effective threshold immediately, mid-lap.
        let priority = Arc::new(AtomicBool::new(false));
        let probe = Arc::clone(&priority);
        let budget = YieldBudget::new(move || {
            if probe.load(Ordering::SeqCst) {
                Duration::ZERO
            } else {
                Duration::from_secs(3600)
            }
        });
        std::thread::sleep(Duration::from_millis(3));
        assert!(!budget.should_yield(), "generous threshold: within budget");
        priority.store(true, Ordering::SeqCst);
        assert!(budget.should_yield(), "zero threshold: same lap now over");
    }

    #[tokio::test]
    async fn maybe_yield_is_a_noop_within_budget() {
        let mut budget = YieldBudget::fixed(Duration::from_secs(3600));
        assert!(!budget.maybe_yield().await);
    }

    #[tokio::test]
    async fn maybe_yield_yields_once_over_budget_and_starts_a_new_lap() {
        let mut budget = YieldBudget::fixed(Duration::from_millis(1));
        std::thread::sleep(Duration::from_millis(5));
        assert!(budget.maybe_yield().await, "over budget: must yield");
        assert!(
            !budget.should_yield(),
            "the yield started a fresh lap, immediately within budget"
        );
    }

    #[tokio::test]
    async fn from_config_composes_threshold_and_priority_probe() {
        // threshold 3_600_000ms normally; with the priority op running the
        // effective threshold is a quarter of that — still enormous, so the
        // observable contract here is just that the probe is consulted and
        // the composition never yields spuriously within budget.
        let priority = Arc::new(AtomicBool::new(false));
        let probe = Arc::clone(&priority);
        let mut budget = YieldBudget::from_config(3_600_000, move || probe.load(Ordering::SeqCst));
        assert!(!budget.maybe_yield().await);
        priority.store(true, Ordering::SeqCst);
        assert!(!budget.maybe_yield().await);
    }
}
