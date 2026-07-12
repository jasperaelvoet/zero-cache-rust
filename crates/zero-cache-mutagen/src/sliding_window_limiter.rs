//! Per-user mutation rate limiter, ported from
//! `packages/zero-cache/src/services/limiter/sliding-window-limiter.ts`.
//!
//! Two-window weighted sliding window: time is divided into fixed
//! `window_size_ms` periods and the limiter tracks a *prior* and a *next*
//! window, each with a call count. The effective total for a trailing window
//! ending at `now` is
//!
//! ```text
//! total = prior.count * fraction + next.count
//! ```
//!
//! where `fraction` is the portion of the prior window still covered by the
//! trailing window (clamped to `0..=1`). A call is allowed while
//! `total < max_mutations`, and — matching upstream — the count is only
//! incremented for *allowed* calls, so throttled retries cannot lock a user
//! out indefinitely.
//!
//! Unlike upstream (which reads `Date.now()` internally), this port is pure:
//! the caller injects `now_ms` into [`SlidingWindowLimiter::can_do`]. The
//! windows are created lazily on the first call, which is observably
//! equivalent to upstream's construct-time initialization because a call is
//! always counted in the fixed window period containing its own timestamp.
//! [`SlidingWindowLimiter::can_do_now`] is a convenience wrapper over the
//! system clock for production call sites (upstream `mutagen.ts` constructs
//! the limiter only when `perUserMutationLimit.max` is configured, and maps a
//! denied call to a `MutationRateLimited` error with message
//! `"Rate limit exceeded"`).

use std::time::{SystemTime, UNIX_EPOCH};

/// One fixed window period: `[start, start + window_size_ms)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Window {
    start: i64,
    count: u64,
}

/// Sliding-window rate limiter allowing at most `max_mutations` calls per
/// trailing `window_size_ms` window (weighted across two fixed windows).
#[derive(Debug, Clone)]
pub struct SlidingWindowLimiter {
    window_size_ms: i64,
    max_mutations: u64,
    /// `(prior, next)`, created lazily on the first `can_do` call.
    windows: Option<(Window, Window)>,
}

impl SlidingWindowLimiter {
    /// Creates a limiter allowing `max_mutations` per `window_size_ms`
    /// trailing window. `max_mutations == 0` denies every call.
    ///
    /// # Panics
    ///
    /// Panics if `window_size_ms` is not positive.
    pub fn new(window_size_ms: i64, max_mutations: u64) -> Self {
        assert!(window_size_ms > 0, "window_size_ms must be positive");
        Self {
            window_size_ms,
            max_mutations,
            windows: None,
        }
    }

    /// Returns whether a call at `now_ms` (Unix epoch milliseconds) is
    /// allowed, and if so records it. Throttled calls are not recorded.
    pub fn can_do(&mut self, now_ms: i64) -> bool {
        let w = self.window_size_ms;
        let (mut prior, mut next) = self.windows.unwrap_or_else(|| Self::new_windows(w, now_ms));

        // If the trailing window is completely past the next window, we need
        // fresh windows (this only happens after a long idle period).
        if now_ms - w > next.start + w {
            (prior, next) = Self::new_windows(w, now_ms);
        }

        // Has the trailing window moved completely into the next window?
        // Then rotate the windows.
        if now_ms - w >= next.start {
            prior = next;
            next = Window {
                start: prior.start + w,
                count: 0,
            };
        }

        let total_calls = Self::total_calls(w, prior, next, now_ms);
        let can_do = total_calls < self.max_mutations as f64;

        // Counts are only bumped when the call is allowed, so excessive
        // retries while throttled cannot lock the user out continuously.
        if can_do {
            if now_ms < next.start {
                prior.count += 1;
            } else {
                next.count += 1;
            }
        }

        self.windows = Some((prior, next));
        can_do
    }

    /// Convenience wrapper over [`Self::can_do`] using the system clock.
    pub fn can_do_now(&mut self) -> bool {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.can_do(now_ms)
    }

    /// The weighted call total for a trailing window ending at `now_ms`,
    /// without mutating any state (upstream `totalCallsForTime`).
    pub fn total_calls_for_time(&self, now_ms: i64) -> f64 {
        match self.windows {
            Some((prior, next)) => Self::total_calls(self.window_size_ms, prior, next, now_ms),
            None => 0.0,
        }
    }

    fn total_calls(window_size_ms: i64, prior: Window, next: Window, now_ms: i64) -> f64 {
        let fraction = if now_ms < prior.start + window_size_ms {
            1.0
        } else {
            // Portion of the prior window still covered by the trailing
            // window `[now - window_size_ms, now]`, clamped to 0..=1.
            // Upstream asserts `fraction <= 1` (only violated if time runs
            // backwards); we clamp instead of panicking in the serving path.
            let f = (prior.start + (window_size_ms - 1) - (now_ms - window_size_ms)) as f64
                / window_size_ms as f64;
            f.clamp(0.0, 1.0)
        };
        prior.count as f64 * fraction + next.count as f64
    }

    /// Fresh `(prior, next)` windows for `now_ms`, with the prior window
    /// clamped to a `window_size_ms` boundary.
    fn new_windows(window_size_ms: i64, now_ms: i64) -> (Window, Window) {
        let start = now_ms - now_ms.rem_euclid(window_size_ms);
        (
            Window { start, count: 0 },
            Window {
                start: start + window_size_ms,
                count: 0,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::SlidingWindowLimiter;

    // Ported from upstream sliding-window-limiter.test.ts, with `now`
    // injected instead of vitest fake timers.

    #[test]
    fn all_mutations_occur_at_prior_window_start() {
        let mut limiter = SlidingWindowLimiter::new(10, 10);
        for _ in 0..10 {
            assert!(limiter.can_do(10));
        }

        // 11th call fails
        assert!(!limiter.can_do(10));
        // and 12th of course
        assert!(!limiter.can_do(10));

        // failed limiter calls do not bump the count
        assert_eq!(limiter.total_calls_for_time(10), 10.0);
    }

    // Sliding window setup should look like:
    // |----|----|
    //|----|
    #[test]
    fn all_mutations_occur_at_prior_window_end() {
        // prior window end is start + window_size_ms - 1
        let mut limiter = SlidingWindowLimiter::new(10, 10);
        for _ in 0..10 {
            assert!(limiter.can_do(9));
        }

        // 11th call fails
        assert!(!limiter.can_do(9));
    }

    #[test]
    fn fill_the_window_then_slide_the_window() {
        let mut limiter = SlidingWindowLimiter::new(10, 10);
        for _ in 0..10 {
            assert!(limiter.can_do(9));
        }

        assert_eq!(limiter.total_calls_for_time(9), 10.0);

        // sliding out of the past with no new writes should decimate the count
        for i in 0..10 {
            assert_eq!(limiter.total_calls_for_time(10 + i), (10 - i - 1) as f64);
        }

        // sliding into the future while writing should keep the count constant
        for i in 0..10 {
            limiter.can_do(10 + i);
            assert_eq!(limiter.total_calls_for_time(10 + i), 10.0);
        }
    }

    // Upstream's "all mutations occur at next window start/end" tests rely on
    // constructing the limiter at t=0 and calling later; with lazy window
    // creation the equivalent behavior is exercised by seeding one call in
    // the prior window and then filling the next window.

    #[test]
    fn mutations_in_next_window_count_fully() {
        let mut limiter = SlidingWindowLimiter::new(10, 10);
        // Seed the prior window [0, 10).
        assert!(limiter.can_do(0));

        // At t=19 the trailing window covers none of the prior window
        // (fraction 0), so the full budget is available in the next window.
        for _ in 0..10 {
            assert!(limiter.can_do(19));
        }

        // 11th call fails
        assert!(!limiter.can_do(19));
        assert_eq!(limiter.total_calls_for_time(19), 10.0);
    }

    #[test]
    fn weighted_carryover_halfway_into_next_window() {
        let mut limiter = SlidingWindowLimiter::new(10, 10);
        // Fill the prior window [0, 10) at its end.
        for _ in 0..10 {
            assert!(limiter.can_do(9));
        }
        assert!(!limiter.can_do(9));

        // Halfway into the next window the prior count contributes half:
        // fraction = (0 + 9 - (14 - 10)) / 10 = 0.5 -> total 5.
        assert_eq!(limiter.total_calls_for_time(14), 5.0);

        // Exactly 5 more calls fit before the weighted total reaches 10.
        for _ in 0..5 {
            assert!(limiter.can_do(14));
        }
        assert!(!limiter.can_do(14));
        assert_eq!(limiter.total_calls_for_time(14), 10.0);
    }

    #[test]
    fn throttled_attempts_do_not_consume_budget() {
        let mut limiter = SlidingWindowLimiter::new(10, 3);
        for _ in 0..3 {
            assert!(limiter.can_do(5));
        }
        // Hammering while throttled must not extend the lockout.
        for _ in 0..100 {
            assert!(!limiter.can_do(5));
        }
        assert_eq!(limiter.total_calls_for_time(5), 3.0);

        // Once the trailing window has fully slid past the burst, the
        // budget is available again despite the throttled attempts.
        assert!(limiter.can_do(16));
    }

    #[test]
    fn rotation_preserves_next_window_count() {
        let mut limiter = SlidingWindowLimiter::new(10, 10);
        assert!(limiter.can_do(9)); // prior window [0, 10): count 1
        for _ in 0..3 {
            assert!(limiter.can_do(12)); // next window [10, 20): count 3
        }

        // t=24 rotates: [10, 20) becomes the prior window with count 3, and
        // fraction = (10 + 9 - (24 - 10)) / 10 = 0.5, so total = 1.5 before
        // this call is recorded in the fresh next window.
        assert!(limiter.can_do(24));
        assert_eq!(limiter.total_calls_for_time(24), 2.5);
    }

    #[test]
    fn full_idle_period_resets_windows() {
        let mut limiter = SlidingWindowLimiter::new(10, 10);
        for _ in 0..10 {
            assert!(limiter.can_do(9));
        }
        assert!(!limiter.can_do(9));

        // Well past both windows: fresh windows, full budget again.
        assert!(limiter.can_do(100));
        assert_eq!(limiter.total_calls_for_time(100), 1.0);
        for _ in 0..9 {
            assert!(limiter.can_do(100));
        }
        assert!(!limiter.can_do(100));
    }

    #[test]
    fn max_zero_blocks_everything() {
        let mut limiter = SlidingWindowLimiter::new(10, 0);
        assert!(!limiter.can_do(0));
        assert!(!limiter.can_do(5));
        assert!(!limiter.can_do(1_000));
        assert_eq!(limiter.total_calls_for_time(1_000), 0.0);
    }

    #[test]
    fn can_do_now_uses_system_clock() {
        let mut limiter = SlidingWindowLimiter::new(60_000, 2);
        assert!(limiter.can_do_now());
        assert!(limiter.can_do_now());
        assert!(!limiter.can_do_now());
    }

    #[test]
    #[should_panic(expected = "window_size_ms must be positive")]
    fn zero_window_size_panics() {
        SlidingWindowLimiter::new(0, 10);
    }
}
