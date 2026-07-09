//! Port of the pure back-pressure and process-rate logic embedded in
//! `change-streamer/subscriber.ts`'s `Subscriber` class and its private
//! `ByteBackpressureGate` helper.
//!
//! `Subscriber` itself is live orchestration — it owns a `Subscription<string>`
//! downstream, `@rocicorp/resolver` promises, and an async `#drainBacklog`
//! loop pushing bytes through real back-pressured I/O — none of which this port
//! drives. What IS extracted here is the deterministic decision logic those
//! wrappers guard:
//!
//! * [`ByteBackpressureGate`] — the byte high/low-water gate that decides when
//!   a producing `send()` must block and when blocked producers get released.
//!   Upstream models each blocked producer as a `Resolver<void>` in a
//!   `#waiters` array; here a producer is just a count, and the release methods
//!   report how many waiters woke so a caller can resolve exactly that many.
//! * [`process_rate`] — the `getStats().processRate` computation (messages/sec
//!   between the oldest and newest retained sample), and [`push_sample`] /
//!   [`MAX_SAMPLES`], the bounded-history bookkeeping of `sampleProcessRate`.
//! * [`supports_message`] — the protocol-version gate `Subscriber.supportsMessage`
//!   applies before sending a change downstream.

pub const DEFAULT_BACKLOG_HIGH_WATER_BYTES: i64 = 16 * 1024 * 1024;
pub const DEFAULT_BACKLOG_LOW_WATER_RATIO: f64 = 0.8;

/// Default cap on retained process-rate samples (upstream's
/// `sampleProcessRate(now, maxSamples = 10)`).
pub const MAX_SAMPLES: usize = 10;

/// Port of `ByteBackpressureGate`. Tracks how many producers are currently
/// blocked waiting for backlog space; the actual promise resolution is the
/// caller's job (this port has no resolver), so the release methods return the
/// count of producers that should be woken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteBackpressureGate {
    high_water_bytes: i64,
    low_water_bytes: i64,
    waiters: usize,
}

impl ByteBackpressureGate {
    /// Port of the constructor: `highWaterBytes` is floored at 1, and the low
    /// water mark is `highWaterBytes * clamp(lowWaterRatio, 0, 1)`.
    pub fn new(high_water_bytes: i64, low_water_ratio: f64) -> Self {
        let high_water_bytes = high_water_bytes.max(1);
        let low_water_bytes = (high_water_bytes as f64 * low_water_ratio.clamp(0.0, 1.0)) as i64;
        ByteBackpressureGate {
            high_water_bytes,
            low_water_bytes,
            waiters: 0,
        }
    }

    pub fn high_water_bytes(&self) -> i64 {
        self.high_water_bytes
    }

    /// Number of producers currently blocked (upstream's `#waiters.length`).
    pub fn waiting(&self) -> usize {
        self.waiters
    }

    /// Port of `waitForSpace(bufferedBytes)`. Returns `true` if the caller must
    /// block (the gate has recorded a new waiter); `false` if there is still
    /// space and the producer may proceed immediately. A returned `true`
    /// corresponds to upstream pushing a `Resolver` onto `#waiters` and handing
    /// back its unresolved promise.
    pub fn wait_for_space(&mut self, buffered_bytes: i64) -> bool {
        if buffered_bytes < self.high_water_bytes {
            return false;
        }
        self.waiters += 1;
        true
    }

    /// Port of `releaseIfUnderLowWater(bufferedBytes)`. If any producers are
    /// blocked and the backlog has fallen to/below the low water mark, releases
    /// them all (a low water mark releases in batches rather than one-at-a-time
    /// around the high water boundary). Returns the number released.
    pub fn release_if_under_low_water(&mut self, buffered_bytes: i64) -> usize {
        if self.waiters == 0 || buffered_bytes > self.low_water_bytes {
            return 0;
        }
        self.release_all()
    }

    /// Port of `releaseAll()`: wakes every blocked producer (used on close, when
    /// no future drain could release them). Returns the number released.
    pub fn release_all(&mut self) -> usize {
        let released = self.waiters;
        self.waiters = 0;
        released
    }
}

/// Port of `Subscriber.supportsMessage(tag)`: an `update-table-metadata` change
/// is only understood by subscribers on protocol version >= 5; all other tags
/// are always supported.
pub fn supports_message(protocol_version: u32, tag: &str) -> bool {
    match tag {
        "update-table-metadata" => protocol_version >= 5,
        _ => true,
    }
}

/// A retained process-rate sample: a running `processed` total observed at
/// `timestamp` (ms). Port of the elements of `Subscriber.#samples`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ProcessSample {
    pub processed: i64,
    pub timestamp: f64,
}

/// Port of `sampleProcessRate`'s bounded-history bookkeeping: appends a new
/// sample, first evicting from the front until fewer than `max_samples` remain
/// (so that after the push the length is at most `max_samples`). Matches
/// upstream's `while (length >= maxSamples) shift()` then `push`.
pub fn push_sample(samples: &mut Vec<ProcessSample>, sample: ProcessSample, max_samples: usize) {
    while samples.len() >= max_samples {
        samples.remove(0);
    }
    samples.push(sample);
}

/// Port of `getStats().processRate`: messages/second between the oldest and
/// newest retained sample. Returns 0 when fewer than two samples exist or when
/// the two samples share a timestamp (matching upstream's `seconds === 0`
/// guard, which also avoids a divide-by-zero).
pub fn process_rate(samples: &[ProcessSample]) -> f64 {
    if samples.len() < 2 {
        return 0.0;
    }
    let from = &samples[0];
    let to = &samples[samples.len() - 1];
    let processed = (to.processed - from.processed) as f64;
    let seconds = (to.timestamp - from.timestamp) / 1000.0;
    if seconds == 0.0 {
        0.0
    } else {
        processed / seconds
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_floors_high_water_and_clamps_ratio() {
        let g = ByteBackpressureGate::new(0, 0.8);
        assert_eq!(g.high_water_bytes(), 1, "high water floored to 1");

        let g = ByteBackpressureGate::new(100, 2.0);
        assert_eq!(g.low_water_bytes, 100, "ratio clamped to 1.0");
        let g = ByteBackpressureGate::new(100, -1.0);
        assert_eq!(g.low_water_bytes, 0, "ratio clamped to 0.0");
        let g = ByteBackpressureGate::new(100, 0.8);
        assert_eq!(g.low_water_bytes, 80);
    }

    #[test]
    fn wait_for_space_only_blocks_at_or_above_high_water() {
        let mut g = ByteBackpressureGate::new(100, 0.8);
        assert!(!g.wait_for_space(99), "below high water: proceed");
        assert_eq!(g.waiting(), 0);
        assert!(g.wait_for_space(100), "at high water: block");
        assert!(g.wait_for_space(500), "above high water: block");
        assert_eq!(g.waiting(), 2);
    }

    #[test]
    fn release_if_under_low_water_batches_all_waiters() {
        let mut g = ByteBackpressureGate::new(100, 0.8); // low water = 80
        g.wait_for_space(100);
        g.wait_for_space(200);
        assert_eq!(g.waiting(), 2);
        // Still above low water: nobody released.
        assert_eq!(g.release_if_under_low_water(81), 0);
        assert_eq!(g.waiting(), 2);
        // At/below low water: all released at once.
        assert_eq!(g.release_if_under_low_water(80), 2);
        assert_eq!(g.waiting(), 0);
        // No waiters left: nothing to do.
        assert_eq!(g.release_if_under_low_water(0), 0);
    }

    #[test]
    fn release_all_wakes_everyone() {
        let mut g = ByteBackpressureGate::new(1, 0.8);
        g.wait_for_space(5);
        g.wait_for_space(5);
        g.wait_for_space(5);
        assert_eq!(g.release_all(), 3);
        assert_eq!(g.waiting(), 0);
        assert_eq!(g.release_all(), 0);
    }

    #[test]
    fn supports_message_gates_update_table_metadata_on_v5() {
        assert!(!supports_message(4, "update-table-metadata"));
        assert!(supports_message(5, "update-table-metadata"));
        assert!(supports_message(1, "commit"), "other tags always supported");
        assert!(supports_message(1, "insert"));
    }

    #[test]
    fn push_sample_keeps_history_bounded() {
        let mut samples = vec![ProcessSample {
            processed: 0,
            timestamp: 0.0,
        }];
        for i in 1..=15 {
            push_sample(
                &mut samples,
                ProcessSample {
                    processed: i,
                    timestamp: i as f64,
                },
                MAX_SAMPLES,
            );
        }
        assert_eq!(samples.len(), MAX_SAMPLES);
        // Oldest retained is processed=6 (0..5 evicted), newest is 15.
        assert_eq!(samples.first().unwrap().processed, 6);
        assert_eq!(samples.last().unwrap().processed, 15);
    }

    #[test]
    fn process_rate_zero_with_fewer_than_two_samples() {
        assert_eq!(process_rate(&[]), 0.0);
        assert_eq!(
            process_rate(&[ProcessSample {
                processed: 5,
                timestamp: 100.0
            }]),
            0.0
        );
    }

    #[test]
    fn process_rate_zero_when_timestamps_equal() {
        let samples = [
            ProcessSample {
                processed: 0,
                timestamp: 100.0,
            },
            ProcessSample {
                processed: 10,
                timestamp: 100.0,
            },
        ];
        assert_eq!(process_rate(&samples), 0.0);
    }

    #[test]
    fn process_rate_messages_per_second() {
        // 20 messages processed across 2 seconds => 10/sec.
        let samples = [
            ProcessSample {
                processed: 5,
                timestamp: 1000.0,
            },
            ProcessSample {
                processed: 15,
                timestamp: 2000.0,
            },
            ProcessSample {
                processed: 25,
                timestamp: 3000.0,
            },
        ];
        assert_eq!(process_rate(&samples), 10.0);
    }
}
