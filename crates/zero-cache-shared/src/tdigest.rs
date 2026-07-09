//! Port of `packages/shared/src/tdigest.ts`.
//! Apache License 2.0 — https://github.com/influxdata/tdigest
//!
//! A data structure for accurate on-line accumulation of rank-based
//! statistics such as quantiles and trimmed means, used for latency
//! percentile tracking.

use thiserror::Error;

use crate::binary_search::binary_search;
use crate::centroid::{sort_centroid_list, Centroid, CentroidList};

/// The JSON serialization of a digest: `[compression, mean0, weight0, mean1,
/// weight1, ...]`. Port of `TDigestJSON`.
pub type TDigestJson = Vec<f64>;

#[derive(Debug, Error, PartialEq, Eq)]
#[error("Invalid centroids array")]
pub struct InvalidCentroidsError;

/// A t-digest. Port of the `TDigest` class.
pub struct TDigest {
    pub compression: f64,
    max_processed: usize,
    max_unprocessed: usize,
    processed: CentroidList,
    unprocessed: CentroidList,
    cumulative: Vec<f64>,
    processed_weight: f64,
    unprocessed_weight: f64,
    min: f64,
    max: f64,
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new(1000.0)
    }
}

impl TDigest {
    pub fn new(compression: f64) -> Self {
        let max_processed = processed_size(0, compression);
        let max_unprocessed = unprocessed_size(0, compression);
        TDigest {
            compression,
            max_processed,
            max_unprocessed,
            processed: Vec::new(),
            unprocessed: Vec::new(),
            cumulative: Vec::new(),
            processed_weight: 0.0,
            unprocessed_weight: 0.0,
            min: f64::MAX,
            max: -f64::MAX,
        }
    }

    /// Reconstructs a digest from its [`TDigestJson`] form. Port of
    /// `TDigest.fromJSON`.
    pub fn from_json(data: &[f64]) -> Result<TDigest, InvalidCentroidsError> {
        if data.is_empty() || data.len() % 2 != 1 {
            return Err(InvalidCentroidsError);
        }
        let mut digest = TDigest::new(data[0]);
        let mut i = 1;
        while i < data.len() {
            digest.add(data[i], data[i + 1]);
            i += 2;
        }
        Ok(digest)
    }

    /// Resets the digest to empty. Port of `reset`.
    pub fn reset(&mut self) {
        self.processed.clear();
        self.unprocessed.clear();
        self.cumulative.clear();
        self.processed_weight = 0.0;
        self.unprocessed_weight = 0.0;
        self.min = f64::MAX;
        self.max = -f64::MAX;
    }

    /// Adds a value with the given weight (default 1). Port of `add`.
    pub fn add(&mut self, mean: f64, weight: f64) {
        self.add_centroid(Centroid::new(mean, weight));
    }

    /// Adds multiple centroids. Port of `addCentroidList`.
    pub fn add_centroid_list(&mut self, list: &CentroidList) {
        for c in list {
            self.add_centroid(*c);
        }
    }

    /// Adds a single centroid. Weights that are `<= 0`, non-finite, or NaN
    /// means are ignored. Port of `addCentroid`.
    pub fn add_centroid(&mut self, c: Centroid) {
        if c.mean.is_nan() || c.weight <= 0.0 || c.weight.is_nan() || !c.weight.is_finite() {
            return;
        }
        self.unprocessed.push(Centroid::new(c.mean, c.weight));
        self.unprocessed_weight += c.weight;

        if self.processed.len() > self.max_processed
            || self.unprocessed.len() > self.max_unprocessed
        {
            self.process();
        }
    }

    /// Merges `other`'s processed centroids into this digest. Port of `merge`.
    pub fn merge(&mut self, other: &mut TDigest) {
        other.process();
        let list = other.processed.clone();
        self.add_centroid_list(&list);
    }

    fn process(&mut self) {
        if self.unprocessed.is_empty() && self.processed.len() <= self.max_processed {
            return;
        }

        self.unprocessed.append(&mut self.processed.clone());
        self.processed.clear();
        sort_centroid_list(&mut self.unprocessed);

        self.processed.push(self.unprocessed[0]);
        self.processed_weight += self.unprocessed_weight;
        self.unprocessed_weight = 0.0;

        let mut so_far = self.unprocessed[0].weight;
        let mut limit = self.processed_weight * self.integrated_q(1.0);
        for i in 1..self.unprocessed.len() {
            let centroid = self.unprocessed[i];
            let projected = so_far + centroid.weight;
            if projected <= limit {
                so_far = projected;
                let last = self.processed.last_mut().unwrap();
                last.add(&centroid);
            } else {
                let k1 = self.integrated_location(so_far / self.processed_weight);
                limit = self.processed_weight * self.integrated_q(k1 + 1.0);
                so_far += centroid.weight;
                self.processed.push(centroid);
            }
        }
        self.min = self.min.min(self.processed[0].mean);
        self.max = self.max.max(self.processed.last().unwrap().mean);
        self.unprocessed.clear();
    }

    /// Returns a copy of the processed centroids. Port of `centroids`.
    pub fn centroids(&mut self) -> CentroidList {
        self.process();
        self.processed.clone()
    }

    /// The total weight (count) of all values added. Port of `count`.
    pub fn count(&mut self) -> f64 {
        self.process();
        self.processed_weight
    }

    /// Serializes to [`TDigestJson`]. Port of `toJSON`.
    pub fn to_json(&mut self) -> TDigestJson {
        self.process();
        let mut data = vec![self.compression];
        for c in &self.processed {
            data.push(c.mean);
            data.push(c.weight);
        }
        data
    }

    fn update_cumulative(&mut self) {
        if let Some(&last) = self.cumulative.last() {
            if last == self.processed_weight {
                return;
            }
        }
        let n = self.processed.len() + 1;
        self.cumulative = vec![0.0; n];

        let mut prev = 0.0;
        for (i, centroid) in self.processed.iter().enumerate() {
            let cur = centroid.weight;
            self.cumulative[i] = prev + cur / 2.0;
            prev += cur;
        }
        self.cumulative[self.processed.len()] = prev;
    }

    /// Returns the approximate quantile (0..=1) of the distribution, or `NaN`
    /// for empty/out-of-range input. Port of `quantile`.
    pub fn quantile(&mut self, q: f64) -> f64 {
        self.process();
        self.update_cumulative();
        if !(0.0..=1.0).contains(&q) || self.processed.is_empty() {
            return f64::NAN;
        }
        if self.processed.len() == 1 {
            return self.processed[0].mean;
        }
        let index = q * self.processed_weight;
        if index <= self.processed[0].weight / 2.0 {
            return self.min
                + (2.0 * index / self.processed[0].weight) * (self.processed[0].mean - self.min);
        }

        let cumulative = &self.cumulative;
        let lower = binary_search(cumulative.len(), |i| -cumulative[i] + index);

        if lower + 1 != cumulative.len() {
            let z1 = index - self.cumulative[lower - 1];
            let z2 = self.cumulative[lower] - index;
            return weighted_average(
                self.processed[lower - 1].mean,
                z2,
                self.processed[lower].mean,
                z1,
            );
        }

        let z1 = index - self.processed_weight - self.processed[lower - 1].weight / 2.0;
        let z2 = self.processed[lower - 1].weight / 2.0 - z1;
        weighted_average(self.processed.last().unwrap().mean, z1, self.max, z2)
    }

    /// Returns the CDF for a given value. Port of `cdf`.
    pub fn cdf(&mut self, x: f64) -> f64 {
        self.process();
        self.update_cumulative();
        match self.processed.len() {
            0 => return 0.0,
            1 => {
                let width = self.max - self.min;
                if x <= self.min {
                    return 0.0;
                }
                if x >= self.max {
                    return 1.0;
                }
                if x - self.min <= width {
                    return 0.5;
                }
                return (x - self.min) / width;
            }
            _ => {}
        }

        if x <= self.min {
            return 0.0;
        }
        if x >= self.max {
            return 1.0;
        }
        let m0 = self.processed[0].mean;
        if x <= m0 {
            if m0 - self.min > 0.0 {
                return ((x - self.min) / (m0 - self.min)) * self.processed[0].weight
                    / self.processed_weight
                    / 2.0;
            }
            return 0.0;
        }
        let mn = self.processed.last().unwrap().mean;
        if x >= mn {
            if self.max - mn > 0.0 {
                return 1.0
                    - ((self.max - x) / (self.max - mn)) * self.processed.last().unwrap().weight
                        / self.processed_weight
                        / 2.0;
            }
            return 1.0;
        }

        let processed = &self.processed;
        let upper = binary_search(processed.len(), |i| {
            let d = x - processed[i].mean;
            if d == 0.0 {
                1.0
            } else {
                d
            }
        });

        let z1 = x - self.processed[upper - 1].mean;
        let z2 = self.processed[upper].mean - x;
        weighted_average(self.cumulative[upper - 1], z2, self.cumulative[upper], z1)
            / self.processed_weight
    }

    fn integrated_q(&self, k: f64) -> f64 {
        (((k.min(self.compression) * std::f64::consts::PI) / self.compression
            - std::f64::consts::PI / 2.0)
            .sin()
            + 1.0)
            / 2.0
    }

    fn integrated_location(&self, q: f64) -> f64 {
        (self.compression * ((2.0 * q - 1.0).asin() + std::f64::consts::PI / 2.0))
            / std::f64::consts::PI
    }
}

/// Bytes needed for a digest of compression `comp`. Port of
/// `byteSizeForCompression`.
pub fn byte_size_for_compression(comp: f64) -> i64 {
    let c = comp as i64;
    c * 40
}

fn weighted_average(x1: f64, w1: f64, x2: f64, w2: f64) -> f64 {
    if x1 <= x2 {
        weighted_average_sorted(x1, w1, x2, w2)
    } else {
        weighted_average_sorted(x2, w2, x1, w1)
    }
}

fn weighted_average_sorted(x1: f64, w1: f64, x2: f64, w2: f64) -> f64 {
    let x = (x1 * w1 + x2 * w2) / (w1 + w2);
    // JS `Math.max`/`Math.min` propagate NaN; Rust's `f64::max`/`min` do not
    // (they return the non-NaN operand), so use explicit NaN-propagating
    // comparisons to match upstream exactly.
    js_max(x1, js_min(x, x2))
}

fn js_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.min(b)
    }
}

fn js_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}

fn processed_size(size: usize, compression: f64) -> usize {
    if size == 0 {
        (compression.ceil() as usize) * 2
    } else {
        size
    }
}

fn unprocessed_size(size: usize, compression: f64) -> usize {
    if size == 0 {
        (compression.ceil() as usize) * 8
    } else {
        size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_cases() {
        let mut td = TDigest::new(1000.0);
        assert_eq!(td.count(), 0.0);

        let mut td = TDigest::new(1000.0);
        td.add(5.0, 1.0);
        td.add(4.0, 1.0);
        assert_eq!(td.count(), 2.0);
    }

    #[test]
    fn quantile_deterministic_cases() {
        let mut td = TDigest::new(1000.0);
        for x in [1.0, 2.0, 3.0, 4.0, 5.0] {
            td.add(x, 1.0);
        }
        assert_eq!(td.quantile(0.5), 3.0);

        let mut td = TDigest::new(1000.0);
        td.add(555.349107, 1.0);
        td.add(432.842597, 1.0);
        assert_eq!(td.quantile(0.25), 432.842597);

        let mut td = TDigest::new(1000.0);
        for x in [1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 4.0, 3.0, 2.0, 1.0] {
            td.add(x, 1.0);
        }
        assert_eq!(td.quantile(0.5), 3.0);
        assert_eq!(td.quantile(0.99), 5.0);
    }

    #[test]
    fn cdf_deterministic_cases() {
        let mut td = TDigest::new(1000.0);
        for x in [1.0, 2.0, 3.0, 4.0, 5.0] {
            td.add(x, 1.0);
        }
        assert!((td.cdf(3.0) - 0.5).abs() < 1e-9);

        let mut td = TDigest::new(1000.0);
        for x in [1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 4.0, 3.0, 2.0, 1.0] {
            td.add(x, 1.0);
        }
        assert!((td.cdf(4.0) - 0.75).abs() < 1e-9);
        assert!((td.cdf(5.0) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn odd_inputs() {
        let mut td = TDigest::new(1000.0);
        td.add(f64::NAN, 1.0);
        td.add(1.0, f64::NAN);
        td.add(1.0, 0.0);
        td.add(1.0, -1000.0);
        assert_eq!(td.count(), 0.0);

        // Infinite values are allowed.
        td.add(1.0, 1.0);
        td.add(2.0, 1.0);
        td.add(f64::INFINITY, 1.0);
        assert_eq!(td.quantile(0.5), 2.0);
        assert!(td.quantile(0.9).is_nan());
    }

    #[test]
    fn centroids_case() {
        let mut td = TDigest::new(3.0);
        for x in [1.0, 2.0, 3.0, 4.0, 5.0] {
            td.add(x, 1.0);
        }
        let got = td.centroids();
        let want = vec![
            Centroid::new(1.0, 1.0),
            Centroid::new(2.5, 2.0),
            Centroid::new(4.0, 1.0),
            Centroid::new(5.0, 1.0),
        ];
        assert_eq!(got, want);
    }

    #[test]
    fn json_empty_and_single() {
        let mut empty = TDigest::new(1000.0);
        assert_eq!(empty.to_json(), vec![1000.0]);
        let mut restored = TDigest::from_json(&empty.to_json()).unwrap();
        assert_eq!(restored.count(), 0.0);
        assert_eq!(restored.compression, 1000.0);

        let mut single = TDigest::new(100.0);
        single.add(42.0, 5.0);
        assert_eq!(single.to_json(), vec![100.0, 42.0, 5.0]);
        let mut restored = TDigest::from_json(&single.to_json()).unwrap();
        assert_eq!(restored.count(), 5.0);
        assert_eq!(restored.quantile(0.5), 42.0);
    }

    #[test]
    fn json_round_trip_preserves_quantiles() {
        let mut original = TDigest::new(500.0);
        for i in 1..=100 {
            original.add(i as f64, 1.0);
        }
        let json = original.to_json();
        assert_eq!(json.len() % 2, 1);

        let mut restored = TDigest::from_json(&json).unwrap();
        assert_eq!(restored.compression, original.compression);
        assert_eq!(restored.count(), original.count());

        for q in [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99] {
            let a = original.quantile(q);
            let b = restored.quantile(q);
            assert!((a - b).abs() < 0.001, "q={q} a={a} b={b}");
        }
    }

    #[test]
    fn from_json_rejects_invalid_length() {
        assert!(TDigest::from_json(&[1000.0, 1.0, 2.0, 3.0]).is_err());
    }

    #[test]
    fn reset_clears_state() {
        let mut td = TDigest::new(1000.0);
        for x in [1.0, 2.0, 3.0, 4.0, 5.0] {
            td.add(x, 1.0);
        }
        let q1 = td.quantile(0.5);
        td.reset();
        for x in [1.0, 2.0, 3.0, 4.0, 5.0] {
            td.add(x, 1.0);
        }
        assert_eq!(td.quantile(0.5), q1);
    }

    #[test]
    fn merge_empty_is_noop() {
        let mut a = TDigest::new(1000.0);
        for x in [1.0, 2.0, 3.0, 4.0, 5.0] {
            a.add(x, 1.0);
        }
        let c1 = a.centroids();
        let mut empty = TDigest::new(1000.0);
        a.merge(&mut empty);
        let c2 = a.centroids();
        assert_eq!(c1, c2);
    }
}
