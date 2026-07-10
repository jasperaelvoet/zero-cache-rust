//! Load/benchmark harness core: latency statistics, aggregation, resource-usage
//! parsing, and the report model. Kept separate from the driver (`main.rs`) so
//! the pure logic is unit-tested with no server.

use std::collections::BTreeMap;

/// A collected series of latency samples (milliseconds), for percentile stats.
#[derive(Debug, Default, Clone)]
pub struct LatencySeries {
    samples_ms: Vec<f64>,
}

impl LatencySeries {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn record(&mut self, ms: f64) {
        self.samples_ms.push(ms);
    }
    pub fn merge(&mut self, other: &LatencySeries) {
        self.samples_ms.extend_from_slice(&other.samples_ms);
    }
    pub fn len(&self) -> usize {
        self.samples_ms.len()
    }
    pub fn is_empty(&self) -> bool {
        self.samples_ms.is_empty()
    }

    /// The p-quantile (0.0–1.0) via nearest-rank on the sorted samples.
    pub fn percentile(&self, p: f64) -> f64 {
        if self.samples_ms.is_empty() {
            return 0.0;
        }
        let mut s = self.samples_ms.clone();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let rank = (p.clamp(0.0, 1.0) * (s.len() as f64 - 1.0)).round() as usize;
        s[rank]
    }
    pub fn mean(&self) -> f64 {
        if self.samples_ms.is_empty() {
            return 0.0;
        }
        self.samples_ms.iter().sum::<f64>() / self.samples_ms.len() as f64
    }
    pub fn max(&self) -> f64 {
        self.samples_ms.iter().cloned().fold(0.0, f64::max)
    }
    /// `(p50, p90, p99, max, mean)` in ms.
    pub fn summary(&self) -> LatencySummary {
        LatencySummary {
            count: self.len(),
            p50: self.percentile(0.50),
            p90: self.percentile(0.90),
            p99: self.percentile(0.99),
            max: self.max(),
            mean: self.mean(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LatencySummary {
    pub count: usize,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub max: f64,
    pub mean: f64,
}

/// The aggregate outcome of a load run against one target.
#[derive(Debug, Default, Clone)]
pub struct RunReport {
    pub target: String,
    /// Traffic shape (`ping`, `hydrate`, `fanout`, or `reconnect`).
    pub workload: String,
    pub clients: usize,
    pub connected_ok: usize,
    /// Clients that received a complete initial `pokeEnd` after requesting a
    /// nonempty query. Meaningful for hydrate/fanout/reconnect workloads.
    pub hydrated_ok: usize,
    /// Clients that completed the second WebSocket handshake using the cookie
    /// produced by their first hydration. Meaningful for reconnect workloads.
    pub reconnected_ok: usize,
    pub duration_s: f64,
    pub connect: LatencySeries,
    pub hydration: LatencySeries,
    pub reconnect: LatencySeries,
    pub ping_rtt: LatencySeries,
    /// Number of post-hydration `pokeStart` frames observed while an external
    /// writer was generating fan-out traffic.
    pub fanout_pokes: u64,
    /// Error reason -> count.
    pub errors: BTreeMap<String, usize>,
    pub frames_received: u64,
    /// Peak/mean container resource usage during the run (if sampled).
    pub resource: Option<ResourceStats>,
}

impl RunReport {
    pub fn error(&mut self, reason: impl Into<String>) {
        *self.errors.entry(reason.into()).or_insert(0) += 1;
    }
    /// Total pings answered / second across the run.
    pub fn throughput_ops_s(&self) -> f64 {
        if self.duration_s <= 0.0 {
            return 0.0;
        }
        self.ping_rtt.len() as f64 / self.duration_s
    }
    pub fn success_rate(&self) -> f64 {
        if self.clients == 0 {
            return 0.0;
        }
        self.connected_ok as f64 / self.clients as f64
    }
}

/// Peak + mean container resource usage (from `docker stats`).
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceStats {
    pub peak_cpu_pct: f64,
    pub mean_cpu_pct: f64,
    pub peak_mem_mib: f64,
    pub mean_mem_mib: f64,
    pub samples: usize,
}

/// Parses a `docker stats` CPU field like `"12.34%"` into a percentage.
pub fn parse_cpu_pct(s: &str) -> Option<f64> {
    s.trim().trim_end_matches('%').parse().ok()
}

/// Parses a `docker stats` memory-usage field like `"45.6MiB / 1.9GiB"` (uses
/// the first value) into MiB.
pub fn parse_mem_mib(s: &str) -> Option<f64> {
    let first = s.split('/').next()?.trim();
    let (num, unit) = first.split_at(
        first
            .find(|c: char| c.is_alphabetic())
            .unwrap_or(first.len()),
    );
    let v: f64 = num.trim().parse().ok()?;
    let mib = match unit.trim() {
        "GiB" => v * 1024.0,
        "MiB" => v,
        "KiB" => v / 1024.0,
        "B" => v / (1024.0 * 1024.0),
        "GB" => v * 953.674, // decimal → MiB approx
        "MB" => v * 0.953674,
        "kB" => v / 1024.0,
        _ => return None,
    };
    Some(mib)
}

/// Folds a series of `(cpu_pct, mem_mib)` samples into peak/mean stats.
pub fn resource_stats(samples: &[(f64, f64)]) -> Option<ResourceStats> {
    if samples.is_empty() {
        return None;
    }
    let n = samples.len() as f64;
    let peak_cpu = samples.iter().map(|(c, _)| *c).fold(0.0, f64::max);
    let mean_cpu = samples.iter().map(|(c, _)| *c).sum::<f64>() / n;
    let peak_mem = samples.iter().map(|(_, m)| *m).fold(0.0, f64::max);
    let mean_mem = samples.iter().map(|(_, m)| *m).sum::<f64>() / n;
    Some(ResourceStats {
        peak_cpu_pct: peak_cpu,
        mean_cpu_pct: mean_cpu,
        peak_mem_mib: peak_mem,
        mean_mem_mib: mean_mem,
        samples: samples.len(),
    })
}

/// Renders a human-readable report block for one target.
pub fn render_report(r: &RunReport) -> String {
    let ping = r.ping_rtt.summary();
    let conn = r.connect.summary();
    let hydration = r.hydration.summary();
    let reconnect = r.reconnect.summary();
    let mut out = String::new();
    out.push_str(&format!("── {} ──\n", r.target));
    out.push_str(&format!(
        "  workload: {}  clients: {}  connected: {} ({:.1}%)  duration: {:.1}s\n",
        r.workload,
        r.clients,
        r.connected_ok,
        r.success_rate() * 100.0,
        r.duration_s
    ));
    out.push_str(&format!(
        "  connect ms:  p50 {:.1}  p99 {:.1}  max {:.1}\n",
        conn.p50, conn.p99, conn.max
    ));
    if !r.hydration.is_empty() {
        out.push_str(&format!(
            "  hydrate ms:  p50 {:.1}  p99 {:.1}  max {:.1}  (n={}, complete={})\n",
            hydration.p50, hydration.p99, hydration.max, hydration.count, r.hydrated_ok
        ));
    }
    if !r.reconnect.is_empty() {
        out.push_str(&format!(
            "  reconnect ms: p50 {:.1}  p99 {:.1}  max {:.1}  (n={}, complete={})\n",
            reconnect.p50, reconnect.p99, reconnect.max, reconnect.count, r.reconnected_ok
        ));
    }
    out.push_str(&format!(
        "  ping RTT ms: p50 {:.2}  p90 {:.2}  p99 {:.2}  max {:.2}  (n={})\n",
        ping.p50, ping.p90, ping.p99, ping.max, ping.count
    ));
    out.push_str(&format!(
        "  throughput:  {:.0} pings/s   frames rcvd: {}\n",
        r.throughput_ops_s(),
        r.frames_received
    ));
    if r.fanout_pokes > 0 {
        out.push_str(&format!(
            "  live fan-out pokes observed: {}\n",
            r.fanout_pokes
        ));
    }
    if let Some(res) = &r.resource {
        out.push_str(&format!(
            "  resource:    CPU peak {:.0}% mean {:.0}%   mem peak {:.0}MiB mean {:.0}MiB\n",
            res.peak_cpu_pct, res.mean_cpu_pct, res.peak_mem_mib, res.mean_mem_mib
        ));
    }
    if !r.errors.is_empty() {
        out.push_str("  errors:\n");
        for (reason, n) in &r.errors {
            out.push_str(&format!("    {n:>6}  {reason}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_from_a_known_series() {
        let mut s = LatencySeries::new();
        for v in 1..=100 {
            s.record(v as f64);
        }
        // nearest-rank median of 1..=100 is 50 or 51.
        assert!((s.percentile(0.50) - 50.5).abs() <= 0.5);
        assert_eq!(s.percentile(0.99), 99.0);
        assert_eq!(s.max(), 100.0);
        assert!((s.mean() - 50.5).abs() < 1e-9);
    }

    #[test]
    fn empty_series_is_zero() {
        let s = LatencySeries::new();
        assert_eq!(s.percentile(0.5), 0.0);
        assert_eq!(s.summary().count, 0);
    }

    #[test]
    fn parse_cpu_and_mem_fields() {
        assert_eq!(parse_cpu_pct("12.34%"), Some(12.34));
        assert_eq!(parse_mem_mib("45.6MiB / 1.9GiB"), Some(45.6));
        assert!((parse_mem_mib("2GiB / 4GiB").unwrap() - 2048.0).abs() < 1e-6);
        assert_eq!(parse_mem_mib("garbage"), None);
    }

    #[test]
    fn resource_stats_peak_and_mean() {
        let s = resource_stats(&[(10.0, 100.0), (30.0, 300.0), (20.0, 200.0)]).unwrap();
        assert_eq!(s.peak_cpu_pct, 30.0);
        assert_eq!(s.mean_cpu_pct, 20.0);
        assert_eq!(s.peak_mem_mib, 300.0);
        assert_eq!(s.mean_mem_mib, 200.0);
        assert_eq!(s.samples, 3);
    }

    #[test]
    fn report_throughput_and_success_rate() {
        let mut r = RunReport {
            clients: 10,
            connected_ok: 9,
            duration_s: 2.0,
            ..Default::default()
        };
        for _ in 0..100 {
            r.ping_rtt.record(1.0);
        }
        assert_eq!(r.throughput_ops_s(), 50.0);
        assert!((r.success_rate() - 0.9).abs() < 1e-9);
        r.error("connect refused");
        r.error("connect refused");
        assert_eq!(r.errors["connect refused"], 2);
    }
}
