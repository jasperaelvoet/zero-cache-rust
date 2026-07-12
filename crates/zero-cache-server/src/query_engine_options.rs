//! Query-engine option flags (`ZERO_ENABLE_QUERY_PLANNER`,
//! `ZERO_ENABLE_QUERY_COVERING`, `ZERO_QUERY_HYDRATION_STATS`,
//! `ZERO_YIELD_THRESHOLD_MS`), installed once at startup and consulted from
//! the hydration/advancement paths.
//!
//! Planner note: pipeline builds go through the zql
//! `build_pipeline_planned` seam, which plans only when a cost model is
//! provided; `enable_planner=false` guarantees the naive declared ordering
//! regardless of any cost model being available.

use std::sync::OnceLock;

#[derive(Debug, Clone)]
pub struct QueryEngineOptions {
    pub enable_planner: bool,
    pub enable_covering: bool,
    pub hydration_stats: bool,
    pub yield_threshold_ms: u64,
    /// `ZERO_AUTH_REVALIDATE_INTERVAL_SECONDS` (0 = disabled).
    pub auth_revalidate_interval_seconds: u64,
    /// `ZERO_AUTH_RETRANSFORM_INTERVAL_SECONDS` (0 = disabled).
    pub auth_retransform_interval_seconds: u64,
}

impl Default for QueryEngineOptions {
    fn default() -> Self {
        QueryEngineOptions {
            enable_planner: true,
            enable_covering: true,
            hydration_stats: false,
            yield_threshold_ms: 10,
            auth_revalidate_interval_seconds: 300,
            auth_retransform_interval_seconds: 300,
        }
    }
}

static OPTIONS: OnceLock<QueryEngineOptions> = OnceLock::new();

/// Installs the options once at startup.
pub fn init(options: QueryEngineOptions) {
    let _ = OPTIONS.set(options);
}

/// The installed options, or upstream defaults before [`init`] (tests).
pub fn get() -> &'static QueryEngineOptions {
    static DEFAULT: OnceLock<QueryEngineOptions> = OnceLock::new();
    OPTIONS
        .get()
        .unwrap_or_else(|| DEFAULT.get_or_init(QueryEngineOptions::default))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_upstream() {
        let d = QueryEngineOptions::default();
        assert!(d.enable_planner);
        assert!(d.enable_covering);
        assert!(!d.hydration_stats);
        assert_eq!(d.yield_threshold_ms, 10);
    }
}
