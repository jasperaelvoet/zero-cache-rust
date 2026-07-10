//! Port of the pure summary/metrics/labeling logic in
//! `change-source/pg/initial-sync.ts` — the parts of the initial-sync
//! orchestration that make deterministic decisions independent of the live
//! Postgres COPY stream and the OpenTelemetry SDK.
//!
//! What is NOT ported: `initialSync` itself and its live siblings (opening a
//! Postgres connection, `SET TRANSACTION SNAPSHOT`, `COPY ... TO STDOUT`
//! streaming, the `TransactionPool`) — real I/O the live COPY path in
//! `initial_sync_copy.rs` already partially drives — and the OTel counter /
//! histogram instruments themselves (no OTel dependency in this port, same
//! stance as `observability`'s `metrics.ts`). What IS ported is the pure logic
//! those instruments are fed: the per-run label maps, the "which metrics does a
//! run record" decision, the copy-summary reduction, and the slow-flush log
//! predicate.

use std::collections::BTreeMap;

/// Port of `CopyFormat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Binary,
    Text,
}

impl CopyFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            CopyFormat::Binary => "binary",
            CopyFormat::Text => "text",
        }
    }
}

/// Port of `InitialSyncMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitialSyncMode {
    Initial,
    Shadow,
}

impl InitialSyncMode {
    pub fn as_str(self) -> &'static str {
        match self {
            InitialSyncMode::Initial => "initial",
            InitialSyncMode::Shadow => "shadow",
        }
    }
}

/// Port of a run's outcome (`InitialSyncRunMetricAttrs.result`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunResult {
    Success,
    Error,
}

impl RunResult {
    pub fn as_str(self) -> &'static str {
        match self {
            RunResult::Success => "success",
            RunResult::Error => "error",
        }
    }
}

/// Port of `SLOW_COPY_FLUSH_MS`: a per-table flush slower than this (and that
/// actually flushed rows) is logged at info level.
pub const SLOW_COPY_FLUSH_MS: f64 = 10_000.0;

/// Port of `INITIAL_SYNC_DURATION_HISTOGRAM_BOUNDARIES_S`.
pub const INITIAL_SYNC_DURATION_HISTOGRAM_BOUNDARIES_S: [f64; 13] = [
    1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1200.0, 2400.0, 3600.0, 7200.0,
];

/// Port of `initialSyncMetricAttrs`: the base label map attached to every
/// initial-sync instrument. A `BTreeMap` gives the deterministic key ordering
/// this port favors (JS object insertion order is irrelevant to OTel labels).
pub fn initial_sync_metric_attrs(
    sync_mode: InitialSyncMode,
    copy_format: CopyFormat,
) -> BTreeMap<&'static str, String> {
    let mut m = BTreeMap::new();
    m.insert("sync_mode", sync_mode.as_str().to_string());
    m.insert("copy_format", copy_format.as_str().to_string());
    m
}

/// Port of `initialSyncRunMetricAttrs`: the base labels plus the run `result`.
pub fn initial_sync_run_metric_attrs(
    sync_mode: InitialSyncMode,
    copy_format: CopyFormat,
    result: RunResult,
) -> BTreeMap<&'static str, String> {
    let mut m = initial_sync_metric_attrs(sync_mode, copy_format);
    m.insert("result", result.as_str().to_string());
    m
}

/// Timing/volume stats for one initial-sync run (port of
/// `recordInitialSyncRunMetrics`'s `stats` argument). All fields but
/// `duration_ms` are optional (absent when the run errored before that phase).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct RunStats {
    pub duration_ms: f64,
    pub rows: Option<i64>,
    pub copy_bytes: Option<i64>,
    pub copy_ms: Option<f64>,
    pub copy_other_ms: Option<f64>,
    pub flush_ms: Option<f64>,
    pub index_ms: Option<f64>,
}

/// The set of metric emissions a single run produces — the *decision* half of
/// `recordInitialSyncRunMetrics`, decoupled from the OTel instruments it would
/// call. `runs`/`duration_ms` are always emitted; the rest are gated on a
/// successful result and on the corresponding stat being present (and, for
/// counters, positive), exactly matching upstream's `if` ladder.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct RecordedRunMetrics {
    /// `initialSyncRuns().add(1)` — always 1.
    pub runs: i64,
    /// `initialSyncDuration().recordMs(...)` — always present.
    pub duration_ms: f64,
    pub copy_duration_ms: Option<f64>,
    pub copy_other_duration_ms: Option<f64>,
    pub flush_duration_ms: Option<f64>,
    pub index_duration_ms: Option<f64>,
    /// `initialSyncRows().add(rows)` — only when rows > 0.
    pub rows: Option<i64>,
    /// `initialSyncCompletedCopyStream().add(copyBytes)` — only when > 0.
    pub completed_copy_bytes: Option<i64>,
}

/// Port of `recordInitialSyncRunMetrics`'s decision logic: returns which
/// instruments would fire and with what values, given a run's stats and result.
pub fn recorded_run_metrics(stats: &RunStats, result: RunResult) -> RecordedRunMetrics {
    let mut out = RecordedRunMetrics {
        runs: 1,
        duration_ms: stats.duration_ms,
        ..RecordedRunMetrics::default()
    };
    if result == RunResult::Success {
        out.copy_duration_ms = stats.copy_ms;
        out.copy_other_duration_ms = stats.copy_other_ms;
        out.flush_duration_ms = stats.flush_ms;
        out.index_duration_ms = stats.index_ms;
        out.rows = stats.rows.filter(|&r| r > 0);
        out.completed_copy_bytes = stats.copy_bytes.filter(|&b| b > 0);
    }
    out
}

/// The per-table copy stats `initialSyncCopySummary` reduces over (subset of
/// upstream's `CopyResult` — only the fields the summary reads).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CopyResult {
    pub rows: i64,
    pub flush_ms: f64,
    pub copy_bytes: i64,
}

/// Port of `initialSyncCopySummary`: totals the per-table copy results and
/// pairs them with the overall COPY-phase duration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CopySummary {
    pub tables: usize,
    pub rows: i64,
    pub copy_ms: f64,
    pub flush_ms: f64,
    pub copy_bytes: i64,
}

pub fn initial_sync_copy_summary(results: &[CopyResult], copy_ms: f64) -> CopySummary {
    let mut rows = 0;
    let mut flush_ms = 0.0;
    let mut copy_bytes = 0;
    for r in results {
        rows += r.rows;
        flush_ms += r.flush_ms;
        copy_bytes += r.copy_bytes;
    }
    CopySummary {
        tables: results.len(),
        rows,
        copy_ms,
        flush_ms,
        copy_bytes,
    }
}

/// Port of `logSlowCopyFlush`'s guard: a flush is logged only when it took at
/// least `SLOW_COPY_FLUSH_MS` *and* actually flushed rows. The logging itself
/// (`lc.info?.(...)`) is the caller's job — this returns whether to log.
pub fn should_log_slow_copy_flush(elapsed_ms: f64, flushed_rows: i64) -> bool {
    elapsed_ms >= SLOW_COPY_FLUSH_MS && flushed_rows != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_metric_attrs_include_result() {
        let base = initial_sync_metric_attrs(InitialSyncMode::Shadow, CopyFormat::Text);
        assert_eq!(base.get("sync_mode").unwrap(), "shadow");
        assert_eq!(base.get("copy_format").unwrap(), "text");
        assert!(!base.contains_key("result"));

        let run = initial_sync_run_metric_attrs(
            InitialSyncMode::Initial,
            CopyFormat::Binary,
            RunResult::Error,
        );
        assert_eq!(run.get("sync_mode").unwrap(), "initial");
        assert_eq!(run.get("copy_format").unwrap(), "binary");
        assert_eq!(run.get("result").unwrap(), "error");
    }

    #[test]
    fn errored_run_records_only_runs_and_duration() {
        let stats = RunStats {
            duration_ms: 500.0,
            rows: Some(100),
            copy_bytes: Some(9999),
            copy_ms: Some(200.0),
            copy_other_ms: Some(50.0),
            flush_ms: Some(30.0),
            index_ms: Some(20.0),
        };
        let m = recorded_run_metrics(&stats, RunResult::Error);
        assert_eq!(m.runs, 1);
        assert_eq!(m.duration_ms, 500.0);
        // Everything success-gated stays absent even though stats are present.
        assert_eq!(m.copy_duration_ms, None);
        assert_eq!(m.flush_duration_ms, None);
        assert_eq!(m.index_duration_ms, None);
        assert_eq!(m.rows, None);
        assert_eq!(m.completed_copy_bytes, None);
    }

    #[test]
    fn successful_run_records_present_phases_and_positive_counters() {
        let stats = RunStats {
            duration_ms: 500.0,
            rows: Some(100),
            copy_bytes: Some(9999),
            copy_ms: Some(200.0),
            copy_other_ms: None, // absent phase stays absent
            flush_ms: Some(30.0),
            index_ms: Some(20.0),
        };
        let m = recorded_run_metrics(&stats, RunResult::Success);
        assert_eq!(m.copy_duration_ms, Some(200.0));
        assert_eq!(m.copy_other_duration_ms, None);
        assert_eq!(m.flush_duration_ms, Some(30.0));
        assert_eq!(m.index_duration_ms, Some(20.0));
        assert_eq!(m.rows, Some(100));
        assert_eq!(m.completed_copy_bytes, Some(9999));
    }

    #[test]
    fn zero_valued_counters_are_not_recorded() {
        let stats = RunStats {
            duration_ms: 10.0,
            rows: Some(0),
            copy_bytes: Some(0),
            ..RunStats::default()
        };
        let m = recorded_run_metrics(&stats, RunResult::Success);
        assert_eq!(m.rows, None, "0 rows not recorded");
        assert_eq!(m.completed_copy_bytes, None, "0 bytes not recorded");
    }

    #[test]
    fn copy_summary_totals_across_tables() {
        let results = [
            CopyResult {
                rows: 10,
                flush_ms: 1.5,
                copy_bytes: 100,
            },
            CopyResult {
                rows: 5,
                flush_ms: 2.5,
                copy_bytes: 50,
            },
        ];
        let s = initial_sync_copy_summary(&results, 42.0);
        assert_eq!(s.tables, 2);
        assert_eq!(s.rows, 15);
        assert_eq!(s.flush_ms, 4.0);
        assert_eq!(s.copy_bytes, 150);
        assert_eq!(s.copy_ms, 42.0);
    }

    #[test]
    fn copy_summary_of_no_tables_is_zeroed() {
        let s = initial_sync_copy_summary(&[], 0.0);
        assert_eq!(s.tables, 0);
        assert_eq!(s.rows, 0);
        assert_eq!(s.copy_bytes, 0);
    }

    #[test]
    fn slow_flush_logged_only_when_slow_and_nonempty() {
        assert!(!should_log_slow_copy_flush(9_999.0, 100), "under threshold");
        assert!(!should_log_slow_copy_flush(20_000.0, 0), "no rows flushed");
        assert!(
            should_log_slow_copy_flush(10_000.0, 1),
            "at threshold with rows"
        );
        assert!(should_log_slow_copy_flush(50_000.0, 100));
    }
}
