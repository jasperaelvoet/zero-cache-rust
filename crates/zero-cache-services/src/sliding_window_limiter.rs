//! Port of `zero-cache/src/services/limiter/sliding-window-limiter.ts`.
//!
//! A sliding-window rate limiter allowing at most `max_mutations` per window.
//! The window is split into a prior and next half; the total is the next
//! window's count plus the prior window's count weighted by the fraction of the
//! window still overlapping the sliding window.
//!
//! The upstream class reads `Date.now()` internally; this port takes the
//! current time (`now`, epoch millis) as an explicit parameter so callers
//! inject the clock (and tests drive it deterministically).

/// A single window: its start time (ms) and event count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub start: i64,
    pub count: i64,
}

/// Port of `SlidingWindowLimiter`.
pub struct SlidingWindowLimiter {
    window_size_ms: i64,
    max_mutations: i64,
    prior_window: Window,
    next_window: Window,
}

impl SlidingWindowLimiter {
    /// Creates a limiter, seeding the windows from `now`. Port of the
    /// constructor.
    pub fn new(window_size_ms: i64, max_mutations: i64, now: i64) -> Self {
        let (prior, next) = Self::new_windows(window_size_ms, now);
        SlidingWindowLimiter {
            window_size_ms,
            max_mutations,
            prior_window: prior,
            next_window: next,
        }
    }

    /// Whether the action is allowed at `now`, incrementing the count if so.
    /// Port of `canDo`.
    pub fn can_do(&mut self, now: i64) -> bool {
        // If the sliding window is completely past the next window, reset.
        if now - self.window_size_ms > self.next_window.start + self.window_size_ms {
            let (prior, next) = Self::new_windows(self.window_size_ms, now);
            self.prior_window = prior;
            self.next_window = next;
        }

        // If the sliding window has moved completely into the next window, rotate.
        if now - self.window_size_ms >= self.next_window.start {
            self.rotate_windows();
        }

        let total_calls = self.total_calls_for_time(now);
        let can_do = total_calls < self.max_mutations as f64;

        // Only increment on success, so excessive retries don't lock a user out.
        if can_do {
            if now < self.next_window.start {
                self.prior_window.count += 1;
            } else {
                self.next_window.count += 1;
            }
        }
        can_do
    }

    /// The weighted total call count at `now`. Port of `totalCallsForTime`.
    pub fn total_calls_for_time(&self, now: i64) -> f64 {
        let mut fraction = if now < self.prior_window.start + self.window_size_ms {
            1.0
        } else {
            (self.prior_window.start + (self.window_size_ms - 1) - (now - self.window_size_ms))
                as f64
                / self.window_size_ms as f64
        };
        if fraction < 0.0 {
            fraction = 0.0;
        }
        assert!(
            fraction <= 1.0,
            "The past cannot contribute more than a full window."
        );
        self.prior_window.count as f64 * fraction + self.next_window.count as f64
    }

    pub fn prior_window(&self) -> Window {
        self.prior_window
    }

    pub fn next_window(&self) -> Window {
        self.next_window
    }

    fn new_windows(window_size_ms: i64, now: i64) -> (Window, Window) {
        let start = now - now.rem_euclid(window_size_ms);
        (
            Window { start, count: 0 },
            Window {
                start: start + window_size_ms,
                count: 0,
            },
        )
    }

    fn rotate_windows(&mut self) {
        self.prior_window = self.next_window;
        self.next_window = Window {
            start: self.prior_window.start + self.window_size_ms,
            count: 0,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_mutations_at_prior_window_start() {
        // setSystemTime(10); new(10, 10)
        let mut limiter = SlidingWindowLimiter::new(10, 10, 10);
        for _ in 0..10 {
            assert!(limiter.can_do(10));
        }
        assert!(!limiter.can_do(10));
        assert!(!limiter.can_do(10));
        assert_eq!(limiter.total_calls_for_time(10), 10.0);
    }

    #[test]
    fn all_mutations_at_prior_window_end() {
        let mut limiter = SlidingWindowLimiter::new(10, 10, 9);
        for _ in 0..10 {
            assert!(limiter.can_do(9));
        }
        assert!(!limiter.can_do(9));
    }

    #[test]
    fn fill_then_slide() {
        let mut limiter = SlidingWindowLimiter::new(10, 10, 9);
        for _ in 0..10 {
            assert!(limiter.can_do(9));
        }
        assert_eq!(limiter.total_calls_for_time(9), 10.0);

        // As the window slides, the prior window's weighted contribution decays.
        for i in 0..10 {
            assert_eq!(limiter.total_calls_for_time(10 + i), (10 - i - 1) as f64);
        }
        // Refilling the next window keeps the running total at capacity.
        for i in 0..10 {
            limiter.can_do(10 + i);
            assert_eq!(limiter.total_calls_for_time(10 + i), 10.0);
        }
    }

    #[test]
    fn all_mutations_at_next_window_start() {
        let mut limiter = SlidingWindowLimiter::new(10, 10, 0);
        for _ in 0..10 {
            assert!(limiter.can_do(10));
        }
        assert!(!limiter.can_do(10));
    }

    #[test]
    fn all_mutations_at_next_window_end() {
        let mut limiter = SlidingWindowLimiter::new(10, 10, 0);
        for _ in 0..10 {
            assert!(limiter.can_do(19));
        }
        assert!(!limiter.can_do(19));
    }
}
