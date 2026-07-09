//! Port of `packages/shared/src/centroid.ts`.
//! Apache License 2.0 — https://github.com/influxdata/tdigest

/// A centroid: the average position (mean) of all points in a cluster, and
/// their total weight.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Centroid {
    pub mean: f64,
    pub weight: f64,
}

impl Centroid {
    pub fn new(mean: f64, weight: f64) -> Self {
        Centroid { mean, weight }
    }

    /// Merges `r` into this centroid in place. Panics if `r.weight < 0`
    /// (matching the TS `throw`).
    pub fn add(&mut self, r: &Centroid) {
        assert!(r.weight >= 0.0, "centroid weight cannot be less than zero");
        if self.weight != 0.0 {
            self.weight += r.weight;
            self.mean += (r.weight * (r.mean - self.mean)) / self.weight;
        } else {
            self.weight = r.weight;
            self.mean = r.mean;
        }
    }
}

/// A list of centroids, expected sorted by mean ascending.
pub type CentroidList = Vec<Centroid>;

/// Sorts `centroids` by mean, ascending.
pub fn sort_centroid_list(centroids: &mut [Centroid]) {
    centroids.sort_by(|a, b| a.mean.total_cmp(&b.mean));
}
