//! Port of `packages/shared/src/logarithmic-histogram.ts`.
//!
//! Tracks the distribution of non-negative values in logarithmically-spaced
//! buckets, growing on demand, with hex serialization. Useful for
//! exponentially-distributed data (e.g. latencies, row counts).

use thiserror::Error;

/// A logarithmic histogram. Port of `LogarithmicHistogram`.
#[derive(Debug)]
pub struct LogarithmicHistogram {
    /// Bucket counts; index 0 is the underflow bucket (values in `[0, 1)`).
    counts: Vec<u32>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HistogramError {
    #[error("Value must not be negative, got: {0}")]
    NegativeValue(String),
    #[error("Invalid hex string: {0}")]
    InvalidHex(String),
}

impl Default for LogarithmicHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LogarithmicHistogram {
    /// A fresh histogram with the underflow bucket and one regular bucket.
    pub fn new() -> Self {
        LogarithmicHistogram { counts: vec![0, 0] }
    }

    /// Adds `value` to its bucket, growing the bucket array if needed. Port of
    /// `add`.
    pub fn add(&mut self, value: f64) -> Result<(), HistogramError> {
        if value < 0.0 {
            return Err(HistogramError::NegativeValue(js_number_string(value)));
        }
        if value < 1.0 {
            self.counts[0] += 1;
            return Ok(());
        }

        let index = (value.log2().floor() as i64 + 1) as usize;
        if index >= self.counts.len() {
            self.counts.resize(index + 1, 0);
        }
        self.counts[index] += 1;
        Ok(())
    }

    /// A read-only view of the bucket counts (index 0 = underflow). Port of
    /// the `counts` getter.
    pub fn counts(&self) -> &[u32] {
        &self.counts
    }

    /// The `[min, max)` value range for each bucket. Port of `getBucketRanges`.
    pub fn bucket_ranges(&self) -> Vec<(f64, f64)> {
        let mut ranges = vec![(0.0, 1.0)];
        for i in 1..self.counts.len() {
            ranges.push((2f64.powi(i as i32 - 1), 2f64.powi(i as i32)));
        }
        ranges
    }

    /// Serializes to a hex string: 8 hex chars (4 bytes) per bucket. Port of
    /// `toHexString`.
    pub fn to_hex_string(&self) -> String {
        self.counts.iter().map(|c| format!("{c:08x}")).collect()
    }

    /// Deserializes from a hex string produced by [`to_hex_string`]. Port of
    /// `fromHexString`.
    ///
    /// [`to_hex_string`]: LogarithmicHistogram::to_hex_string
    pub fn from_hex_string(hex: &str) -> Result<LogarithmicHistogram, HistogramError> {
        if !hex.len().is_multiple_of(8) {
            return Err(HistogramError::InvalidHex(hex.to_string()));
        }
        let mut counts = Vec::with_capacity(hex.len() / 8);
        for chunk in hex.as_bytes().chunks(8) {
            let s = std::str::from_utf8(chunk).unwrap();
            let n = u32::from_str_radix(s, 16)
                .map_err(|_| HistogramError::InvalidHex(hex.to_string()))?;
            counts.push(n);
        }
        Ok(LogarithmicHistogram { counts })
    }
}

fn js_number_string(n: f64) -> String {
    if n.fract() == 0.0 && n.is_finite() {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initializes_with_default_values() {
        let h = LogarithmicHistogram::new();
        assert_eq!(h.counts().len(), 2);
        assert!(h.counts().iter().all(|&c| c == 0));
    }

    #[test]
    fn adds_values_and_resizes_dynamically() {
        let mut h = LogarithmicHistogram::new();
        h.add(0.5).unwrap();
        h.add(0.9).unwrap();
        h.add(4.0).unwrap();
        h.add(4.1).unwrap();
        h.add(1024.0).unwrap();
        assert_eq!(h.counts(), &[2, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1]);
    }

    #[test]
    fn calculates_bucket_ranges_dynamically() {
        let mut h = LogarithmicHistogram::new();
        h.add(1024.0).unwrap();
        let ranges = h.bucket_ranges();
        assert_eq!(ranges.len(), 12);
        assert_eq!(ranges[0], (0.0, 1.0));
        assert_eq!(ranges[1], (1.0, 2.0));
        assert_eq!(*ranges.last().unwrap(), (1024.0, 2048.0));
    }

    #[test]
    fn serializes_and_deserializes_resized_histogram() {
        let mut original = LogarithmicHistogram::new();
        original.add(0.5).unwrap();
        original.add(4.0).unwrap();
        original.add(32.0).unwrap();
        original.add(1024.0).unwrap();

        let hex = original.to_hex_string();
        let deserialized = LogarithmicHistogram::from_hex_string(&hex).unwrap();
        assert_eq!(deserialized.counts(), original.counts());
        assert_eq!(deserialized.to_hex_string(), hex);
    }

    #[test]
    fn handles_serialization_of_large_counts() {
        let mut h = LogarithmicHistogram::new();
        for _ in 0..1_000_000 {
            h.add(1.0).unwrap();
        }
        let hex = h.to_hex_string();
        let deserialized = LogarithmicHistogram::from_hex_string(&hex).unwrap();
        assert_eq!(deserialized.counts(), h.counts());
    }

    #[test]
    fn rejects_invalid_hex_string() {
        assert_eq!(
            LogarithmicHistogram::from_hex_string("invalid").unwrap_err(),
            HistogramError::InvalidHex("invalid".to_string())
        );
    }

    #[test]
    fn rejects_negative_values() {
        let mut h = LogarithmicHistogram::new();
        assert_eq!(
            h.add(-1.0),
            Err(HistogramError::NegativeValue("-1".to_string()))
        );
    }
}
