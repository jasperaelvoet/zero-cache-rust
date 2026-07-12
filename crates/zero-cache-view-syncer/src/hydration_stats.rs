//! The `ZERO_QUERY_HYDRATION_STATS` rows-considered tracker — the port of
//! upstream's `runtimeDebugFlags.trackRowCountsVended` accounting in
//! `pipeline-driver.ts` (lines 702–720): during a hydration, count how many
//! rows each table vended into the pipeline, and for hydrations slower than
//! the slow-hydrate threshold log one `"<table> VENDED: ..."` line per table
//! plus a final `"Total rows considered: N"` line.
//!
//! Pure accounting only: the tracker is constructed with the resolved config
//! flag (no env reads here — the server crate resolves
//! `ZERO_QUERY_HYDRATION_STATS` and injects it), recording is a no-op when
//! disabled (upstream's `debugDelegate` is simply `undefined` then), and the
//! orchestrator that owns the row-fetch loop decides when to emit
//! [`HydrationStats::summary_lines`] (upstream gates on
//! `hydrationTimeMs > slowHydrateThreshold`).

use std::collections::BTreeMap;

/// The finished accounting for one hydration: per-table rows-considered
/// counts (sorted by table name, deterministic) and their total — the two
/// figures upstream logs as `"<table> VENDED"` entries and
/// `"Total rows considered: N"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HydrationStatsSummary {
    pub per_table: Vec<(String, u64)>,
    pub total: u64,
}

/// Counts rows considered per table during one hydration. See module doc.
#[derive(Debug, Clone, Default)]
pub struct HydrationStats {
    enabled: bool,
    counts: BTreeMap<String, u64>,
}

impl HydrationStats {
    /// `enabled` is the resolved `ZERO_QUERY_HYDRATION_STATS` value. A
    /// disabled tracker records nothing (zero bookkeeping on the hot row
    /// loop beyond one branch), matching upstream's absent `debugDelegate`.
    pub fn new(enabled: bool) -> Self {
        HydrationStats {
            enabled,
            counts: BTreeMap::new(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Records `n` more rows considered for `table`. No-op when disabled.
    pub fn record(&mut self, table: &str, n: u64) {
        if !self.enabled || n == 0 {
            return;
        }
        // Only allocate the key for a table's first record.
        match self.counts.get_mut(table) {
            Some(count) => *count += n,
            None => {
                self.counts.insert(table.to_string(), n);
            }
        }
    }

    /// Total rows considered across all tables so far.
    pub fn total(&self) -> u64 {
        self.counts.values().sum()
    }

    /// Consumes the tracker into its per-table counts + total. Tables are
    /// sorted by name (deterministic log/assertion order).
    pub fn into_summary(self) -> HydrationStatsSummary {
        let total = self.total();
        HydrationStatsSummary {
            per_table: self.counts.into_iter().collect(),
            total,
        }
    }

    /// The upstream log shape (`pipeline-driver.ts` lines 718–720): one
    /// `"<table> VENDED: <n>"` line per table, then
    /// `"Total rows considered: <total>"`. The caller applies its own
    /// slow-hydrate gating before emitting these.
    pub fn summary_lines(&self) -> Vec<String> {
        let mut lines: Vec<String> = self
            .counts
            .iter()
            .map(|(table, n)| format!("{table} VENDED: {n}"))
            .collect();
        lines.push(format!("Total rows considered: {}", self.total()));
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_tracker_records_nothing() {
        let mut stats = HydrationStats::new(false);
        stats.record("issue", 5);
        stats.record("comment", 3);
        assert!(!stats.is_enabled());
        assert_eq!(stats.total(), 0);
        assert_eq!(
            stats.into_summary(),
            HydrationStatsSummary {
                per_table: vec![],
                total: 0
            }
        );
    }

    #[test]
    fn record_accumulates_per_table() {
        let mut stats = HydrationStats::new(true);
        stats.record("issue", 5);
        stats.record("issue", 2);
        stats.record("comment", 3);
        stats.record("emoji", 0); // zero rows: no entry materialized
        assert_eq!(stats.total(), 10);
        assert_eq!(
            stats.into_summary(),
            HydrationStatsSummary {
                per_table: vec![("comment".into(), 3), ("issue".into(), 7)],
                total: 10
            }
        );
    }

    #[test]
    fn summary_is_sorted_by_table_name() {
        let mut stats = HydrationStats::new(true);
        stats.record("zebra", 1);
        stats.record("alpha", 2);
        stats.record("mid", 3);
        let summary = stats.into_summary();
        assert_eq!(
            summary
                .per_table
                .iter()
                .map(|(t, _)| t.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "mid", "zebra"]
        );
    }

    #[test]
    fn summary_lines_match_the_upstream_log_shape() {
        let mut stats = HydrationStats::new(true);
        stats.record("issue", 40);
        stats.record("comment", 2);
        assert_eq!(
            stats.summary_lines(),
            vec![
                "comment VENDED: 2".to_string(),
                "issue VENDED: 40".to_string(),
                "Total rows considered: 42".to_string(),
            ]
        );
    }

    #[test]
    fn empty_enabled_tracker_still_reports_a_zero_total_line() {
        let stats = HydrationStats::new(true);
        assert_eq!(
            stats.summary_lines(),
            vec!["Total rows considered: 0".to_string()]
        );
    }
}
