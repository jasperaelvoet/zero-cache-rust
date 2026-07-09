//! Port of `zero-cache/src/observability/metrics.ts` — the metrics-instrument
//! registry: the `zero.{category}.{name}` naming convention, the get-or-create
//! instrument cache, the latency-histogram bucket boundaries, and the
//! `recordMs` millisecond→second conversion.
//!
//! Upstream this wraps `@opentelemetry/api`'s `Meter`; the OTel SDK/exporter is
//! process-level and pluggable. This port keeps the same *structure and
//! semantics* over an in-memory backend (`InMemoryBackend`) so the naming,
//! caching, up/down accounting, and ms→s conversion are all faithfully ported
//! and testable without a running collector. A real deployment supplies a
//! backend that forwards to OTel; the registry logic is identical either way.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Port of the `Category` union — the metric-name prefix segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    /// Postgres → replica.
    Replication,
    /// Health of replica and litestream backup.
    Replica,
    /// Replica → client.
    Sync,
    Mutation,
    Server,
}

impl Category {
    pub fn as_str(self) -> &'static str {
        match self {
            Category::Replication => "replication",
            Category::Replica => "replica",
            Category::Sync => "sync",
            Category::Mutation => "mutation",
            Category::Server => "server",
        }
    }
}

/// The fully-qualified instrument name, matching upstream's
/// `` `zero.${category}.${name}` `` template.
pub fn metric_name(category: Category, name: &str) -> String {
    format!("zero.{}.{}", category.as_str(), name)
}

/// Bucket boundaries (in **seconds**) for zero's latency histograms — the exact
/// `LATENCY_HISTOGRAM_BOUNDARIES_S` from upstream (1 ms – 30 s, ~2× log steps).
pub const LATENCY_HISTOGRAM_BOUNDARIES_S: [f64; 14] = [
    0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0,
];

/// The backend a recorded observation is forwarded to. A real deployment
/// implements this over the OTel SDK; [`InMemoryBackend`] captures observations
/// for tests. Instrument identity is the fully-qualified metric name.
pub trait MetricsBackend: Send + Sync {
    /// A monotonic or up/down counter add (`delta` may be negative for an
    /// up/down counter).
    fn add(&self, name: &str, delta: f64);
    /// A histogram observation, already converted to the histogram's unit.
    fn record(&self, name: &str, value: f64);
}

/// A test/inspection backend: sums counter adds and collects histogram
/// observations per instrument name.
#[derive(Debug, Default)]
pub struct InMemoryBackend {
    inner: Mutex<InMemoryState>,
}

#[derive(Debug, Default)]
struct InMemoryState {
    counters: HashMap<String, f64>,
    observations: HashMap<String, Vec<f64>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// The current summed value of a counter/up-down-counter.
    pub fn counter_value(&self, name: &str) -> f64 {
        self.inner
            .lock()
            .unwrap()
            .counters
            .get(name)
            .copied()
            .unwrap_or(0.0)
    }

    /// All histogram observations recorded for `name`, in order.
    pub fn observations(&self, name: &str) -> Vec<f64> {
        self.inner
            .lock()
            .unwrap()
            .observations
            .get(name)
            .cloned()
            .unwrap_or_default()
    }

    /// Renders the current metrics in Prometheus text-exposition format — the
    /// standard PULL-based scrape output, so metrics can be exported over an
    /// HTTP `/metrics` endpoint with no external collector process required.
    ///
    /// OTel metric names (`zero.replication.commit`) are mapped to Prometheus
    /// names (`zero_replication_commit`, dots→underscores). Counters emit a
    /// single value line; histograms emit the aggregate `_count` and `_sum`
    /// (seconds) series. Output is deterministic (sorted by name).
    pub fn render_prometheus(&self) -> String {
        let state = self.inner.lock().unwrap();
        let mut out = String::new();

        let mut counters: Vec<(&String, &f64)> = state.counters.iter().collect();
        counters.sort_by(|a, b| a.0.cmp(b.0));
        for (name, value) in counters {
            let pn = prometheus_name(name);
            out.push_str(&format!("# TYPE {pn} counter\n{pn} {value}\n"));
        }

        let mut hists: Vec<(&String, &Vec<f64>)> = state.observations.iter().collect();
        hists.sort_by(|a, b| a.0.cmp(b.0));
        for (name, obs) in hists {
            let pn = prometheus_name(name);
            let count = obs.len();
            let sum: f64 = obs.iter().sum();
            out.push_str(&format!("# TYPE {pn} histogram\n"));
            // Cumulative `_bucket{le=...}` lines over the standard latency
            // boundaries — the form `histogram_quantile()` needs. Each bucket is
            // the count of observations <= that boundary (cumulative).
            for boundary in LATENCY_HISTOGRAM_BOUNDARIES_S {
                let le_count = obs.iter().filter(|&&v| v <= boundary).count();
                out.push_str(&format!("{pn}_bucket{{le=\"{boundary}\"}} {le_count}\n"));
            }
            // The mandatory +Inf bucket equals the total count.
            out.push_str(&format!("{pn}_bucket{{le=\"+Inf\"}} {count}\n"));
            out.push_str(&format!("{pn}_sum {sum}\n{pn}_count {count}\n"));
        }
        out
    }

    /// Renders the current metrics as an OTLP/HTTP JSON `ExportMetricsServiceRequest`
    /// payload — the body a PUSH-based exporter POSTs to an OTel collector's
    /// `/v1/metrics` endpoint. This is the OTLP serialization (the wire format a
    /// collector ingests); the only piece that additionally needs a *running*
    /// collector is the HTTP delivery of this body.
    ///
    /// Counters map to OTLP `sum` (monotonic, cumulative temporality=2);
    /// latency histograms map to OTLP `histogram` with `bucketCounts` /
    /// `explicitBounds` over [`LATENCY_HISTOGRAM_BOUNDARIES_S`]. Deterministic
    /// (name-sorted). The instrumentation scope is `zero`, matching upstream's
    /// `metrics.getMeter('zero')`.
    pub fn render_otlp_json(&self) -> String {
        let state = self.inner.lock().unwrap();
        let mut metrics_json: Vec<String> = Vec::new();

        let mut counters: Vec<(&String, &f64)> = state.counters.iter().collect();
        counters.sort_by(|a, b| a.0.cmp(b.0));
        for (name, value) in counters {
            metrics_json.push(format!(
                "{{\"name\":\"{}\",\"sum\":{{\"aggregationTemporality\":2,\"isMonotonic\":true,\
                 \"dataPoints\":[{{\"asDouble\":{}}}]}}}}",
                json_escape(name),
                value
            ));
        }

        let mut hists: Vec<(&String, &Vec<f64>)> = state.observations.iter().collect();
        hists.sort_by(|a, b| a.0.cmp(b.0));
        for (name, obs) in hists {
            let count = obs.len();
            let sum: f64 = obs.iter().sum();
            // Non-cumulative per-bucket counts (OTLP bucketCounts are per-bucket,
            // with one extra overflow bucket past the last bound).
            let mut bucket_counts: Vec<usize> = Vec::new();
            let mut prev = f64::NEG_INFINITY;
            for boundary in LATENCY_HISTOGRAM_BOUNDARIES_S {
                bucket_counts.push(obs.iter().filter(|&&v| v > prev && v <= boundary).count());
                prev = boundary;
            }
            bucket_counts.push(obs.iter().filter(|&&v| v > prev).count()); // overflow
            let bounds: Vec<String> = LATENCY_HISTOGRAM_BOUNDARIES_S
                .iter()
                .map(|b| b.to_string())
                .collect();
            let counts: Vec<String> = bucket_counts.iter().map(|c| c.to_string()).collect();
            metrics_json.push(format!(
                "{{\"name\":\"{}\",\"histogram\":{{\"aggregationTemporality\":2,\
                 \"dataPoints\":[{{\"count\":{},\"sum\":{},\"bucketCounts\":[{}],\
                 \"explicitBounds\":[{}]}}]}}}}",
                json_escape(name),
                count,
                sum,
                counts.join(","),
                bounds.join(",")
            ));
        }

        format!(
            "{{\"resourceMetrics\":[{{\"scopeMetrics\":[{{\"scope\":{{\"name\":\"zero\"}},\
             \"metrics\":[{}]}}]}}]}}",
            metrics_json.join(",")
        )
    }
}

/// Minimal JSON string escaping for metric names (`"` and `\`).
fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Maps an OTel-style dotted metric name to a Prometheus metric name
/// (dots and any non-`[a-zA-Z0-9_:]` char become `_`).
fn prometheus_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == ':' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

impl MetricsBackend for InMemoryBackend {
    fn add(&self, name: &str, delta: f64) {
        *self
            .inner
            .lock()
            .unwrap()
            .counters
            .entry(name.to_string())
            .or_insert(0.0) += delta;
    }
    fn record(&self, name: &str, value: f64) {
        self.inner
            .lock()
            .unwrap()
            .observations
            .entry(name.to_string())
            .or_default()
            .push(value);
    }
}

/// A counter / up-down counter handle. Port of the `Counter`/`UpDownCounter`
/// wrappers — carries its resolved name and forwards adds to the backend.
#[derive(Clone)]
pub struct Counter {
    name: String,
    backend: Arc<dyn MetricsBackend>,
}

impl Counter {
    /// Adds `delta` (negative allowed only for an up-down counter, matching
    /// upstream — this type does not enforce non-negativity, the creation site
    /// does).
    pub fn add(&self, delta: f64) {
        self.backend.add(&self.name, delta);
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A latency histogram. Port of `LatencyHistogram`: `record_ms` takes raw
/// **milliseconds** and converts to seconds (the `unit: 's'` convention)
/// before recording — callers never pre-divide.
#[derive(Clone)]
pub struct LatencyHistogram {
    name: String,
    backend: Arc<dyn MetricsBackend>,
}

impl LatencyHistogram {
    /// Record an elapsed duration in **milliseconds**; stored as seconds.
    pub fn record_ms(&self, duration_ms: f64) {
        self.backend.record(&self.name, duration_ms / 1000.0);
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// The instrument registry — port of the module-level `getMeter` + `cache`
/// wrappers. Instruments are created lazily and memoized by name, so repeated
/// `get_or_create_*` calls for the same metric return the same handle (matching
/// upstream's `cache()`).
pub struct Metrics {
    backend: Arc<dyn MetricsBackend>,
    counters: Mutex<HashMap<String, Counter>>,
    up_down_counters: Mutex<HashMap<String, Counter>>,
    histograms: Mutex<HashMap<String, LatencyHistogram>>,
}

impl Metrics {
    pub fn new(backend: Arc<dyn MetricsBackend>) -> Self {
        Metrics {
            backend,
            counters: Mutex::new(HashMap::new()),
            up_down_counters: Mutex::new(HashMap::new()),
            histograms: Mutex::new(HashMap::new()),
        }
    }

    /// Port of `getOrCreateCounter`. Keyed by the bare `name` (as upstream's
    /// cache is), the instrument's reported name is the fully-qualified form.
    pub fn get_or_create_counter(&self, category: Category, name: &str) -> Counter {
        self.counters
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert_with(|| Counter {
                name: metric_name(category, name),
                backend: self.backend.clone(),
            })
            .clone()
    }

    /// Port of `getOrCreateUpDownCounter` (a separate instrument namespace, so a
    /// counter and up-down-counter may share a bare name without colliding).
    pub fn get_or_create_up_down_counter(&self, category: Category, name: &str) -> Counter {
        self.up_down_counters
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert_with(|| Counter {
                name: metric_name(category, name),
                backend: self.backend.clone(),
            })
            .clone()
    }

    /// Port of `getOrCreateLatencyHistogram` — a seconds-unit histogram with the
    /// standard [`LATENCY_HISTOGRAM_BOUNDARIES_S`] boundaries baked in.
    pub fn get_or_create_latency_histogram(
        &self,
        category: Category,
        name: &str,
    ) -> LatencyHistogram {
        self.histograms
            .lock()
            .unwrap()
            .entry(name.to_string())
            .or_insert_with(|| LatencyHistogram {
                name: metric_name(category, name),
                backend: self.backend.clone(),
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_name_matches_the_zero_category_name_template() {
        assert_eq!(
            metric_name(Category::Sync, "hydration-time"),
            "zero.sync.hydration-time"
        );
        assert_eq!(
            metric_name(Category::Replication, "commit"),
            "zero.replication.commit"
        );
    }

    #[test]
    fn latency_boundaries_match_upstream() {
        assert_eq!(LATENCY_HISTOGRAM_BOUNDARIES_S.len(), 14);
        assert_eq!(LATENCY_HISTOGRAM_BOUNDARIES_S[0], 0.001);
        assert_eq!(LATENCY_HISTOGRAM_BOUNDARIES_S[13], 30.0);
    }

    #[test]
    fn counter_add_forwards_the_fully_qualified_name_to_the_backend() {
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Metrics::new(backend.clone());
        let c = metrics.get_or_create_counter(Category::Mutation, "applied");
        c.add(1.0);
        c.add(2.0);
        assert_eq!(backend.counter_value("zero.mutation.applied"), 3.0);
    }

    #[test]
    fn get_or_create_returns_the_same_cached_instrument() {
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Metrics::new(backend.clone());
        // Two lookups of the same name accumulate into one instrument.
        metrics
            .get_or_create_counter(Category::Server, "connections")
            .add(1.0);
        metrics
            .get_or_create_counter(Category::Server, "connections")
            .add(1.0);
        assert_eq!(backend.counter_value("zero.server.connections"), 2.0);
    }

    #[test]
    fn up_down_counter_accepts_negative_deltas() {
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Metrics::new(backend.clone());
        let g = metrics.get_or_create_up_down_counter(Category::Server, "active");
        g.add(3.0);
        g.add(-1.0);
        assert_eq!(backend.counter_value("zero.server.active"), 2.0);
    }

    #[test]
    fn latency_histogram_converts_ms_to_seconds() {
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Metrics::new(backend.clone());
        let h = metrics.get_or_create_latency_histogram(Category::Sync, "hydration-time");
        h.record_ms(1500.0); // 1500 ms -> 1.5 s
        h.record_ms(250.0); // 250 ms -> 0.25 s
        assert_eq!(
            backend.observations("zero.sync.hydration-time"),
            vec![1.5, 0.25]
        );
    }

    #[test]
    fn prometheus_name_maps_dots_to_underscores() {
        assert_eq!(
            prometheus_name("zero.replication.commit"),
            "zero_replication_commit"
        );
        assert_eq!(
            prometheus_name("zero.sync.hydration-time"),
            "zero_sync_hydration_time"
        );
    }

    #[test]
    fn render_prometheus_emits_counters_and_histograms() {
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Metrics::new(backend.clone());
        metrics
            .get_or_create_counter(Category::Replication, "commit")
            .add(3.0);
        let h = metrics.get_or_create_latency_histogram(Category::Sync, "hydration-time");
        h.record_ms(1500.0);
        h.record_ms(500.0);

        let text = backend.render_prometheus();
        // Counter line.
        assert!(
            text.contains("# TYPE zero_replication_commit counter\nzero_replication_commit 3\n"),
            "got:\n{text}"
        );
        // Histogram aggregate lines (1.5s + 0.5s = 2s over 2 observations).
        assert!(text.contains("# TYPE zero_sync_hydration_time histogram\n"));
        assert!(
            text.contains("zero_sync_hydration_time_count 2\n"),
            "got:\n{text}"
        );
        assert!(
            text.contains("zero_sync_hydration_time_sum 2\n"),
            "got:\n{text}"
        );
    }

    #[test]
    fn render_prometheus_histogram_emits_cumulative_le_buckets() {
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Metrics::new(backend.clone());
        let h = metrics.get_or_create_latency_histogram(Category::Sync, "poke-time");
        // Observations: 0.5 ms (0.0005 s), 3 ms (0.003 s), 40 ms (0.04 s).
        h.record_ms(0.5);
        h.record_ms(3.0);
        h.record_ms(40.0);

        let text = backend.render_prometheus();
        // le=0.001 (1 ms): only the 0.0005 s obs -> 1.
        assert!(
            text.contains("zero_sync_poke_time_bucket{le=\"0.001\"} 1\n"),
            "got:\n{text}"
        );
        // le=0.005 (5 ms): 0.0005 and 0.003 -> 2.
        assert!(
            text.contains("zero_sync_poke_time_bucket{le=\"0.005\"} 2\n"),
            "got:\n{text}"
        );
        // le=0.05 (50 ms): all three -> 3.
        assert!(
            text.contains("zero_sync_poke_time_bucket{le=\"0.05\"} 3\n"),
            "got:\n{text}"
        );
        // The mandatory +Inf bucket and count agree.
        assert!(text.contains("zero_sync_poke_time_bucket{le=\"+Inf\"} 3\n"));
        assert!(text.contains("zero_sync_poke_time_count 3\n"));
    }

    #[test]
    fn render_otlp_json_emits_sum_and_histogram_metrics() {
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Metrics::new(backend.clone());
        metrics
            .get_or_create_counter(Category::Replication, "commit")
            .add(3.0);
        let h = metrics.get_or_create_latency_histogram(Category::Sync, "poke-time");
        h.record_ms(3.0); // 0.003 s

        let json = backend.render_otlp_json();
        // Envelope + scope.
        assert!(json.starts_with(
            "{\"resourceMetrics\":[{\"scopeMetrics\":[{\"scope\":{\"name\":\"zero\"}"
        ));
        // Counter as a monotonic sum with the raw OTel dotted name.
        assert!(
            json.contains("\"name\":\"zero.replication.commit\",\"sum\":{\"aggregationTemporality\":2,\"isMonotonic\":true"),
            "got:\n{json}"
        );
        assert!(json.contains("\"asDouble\":3"), "got:\n{json}");
        // Histogram with count/sum/bucketCounts/explicitBounds.
        assert!(
            json.contains("\"name\":\"zero.sync.poke-time\",\"histogram\":"),
            "got:\n{json}"
        );
        assert!(json.contains("\"count\":1"), "got:\n{json}");
        assert!(
            json.contains("\"explicitBounds\":[0.001,0.002,0.005"),
            "got:\n{json}"
        );
        // 14 boundaries -> 15 bucketCounts (one overflow); the 0.003s obs lands
        // in the bucket for (0.002, 0.005], i.e. index 2.
        assert!(
            json.contains("\"bucketCounts\":[0,0,1,0,0,0,0,0,0,0,0,0,0,0,0]"),
            "got:\n{json}"
        );
    }

    #[test]
    fn render_prometheus_is_deterministically_sorted() {
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Metrics::new(backend.clone());
        metrics
            .get_or_create_counter(Category::Server, "connections")
            .add(1.0);
        metrics
            .get_or_create_counter(Category::Replication, "commit")
            .add(1.0);
        let text = backend.render_prometheus();
        // "zero_replication_commit" sorts before "zero_server_connections".
        let repl = text.find("zero_replication_commit").unwrap();
        let srv = text.find("zero_server_connections").unwrap();
        assert!(repl < srv, "output must be name-sorted:\n{text}");
    }
}
