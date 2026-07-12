//! Shadow-mode query-covering detection (`ZERO_ENABLE_QUERY_COVERING`).
//!
//! Upstream compares each newly hydrated query against running queries with
//! the same root table and logs aggregate coverage stats — shadow only:
//! nothing is reused or short-circuited. This port implements the same
//! diagnostic at exact-transformation granularity: a hydration whose
//! `(root table, transformation hash)` matches one already hydrated counts
//! as covered. (Upstream's structural AST-subsumption check is broader; the
//! exact-match form is the subset this port can assert cheaply — noted in
//! PORTING.md.)
//!
//! A summary line in upstream's shape (`query coverage shadow summary`,
//! covered/uncovered totals) is logged every [`SUMMARY_EVERY`] observations.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

const SUMMARY_EVERY: u64 = 50;

#[derive(Default)]
struct ShadowState {
    hydrated: HashMap<String, HashSet<String>>,
    total: u64,
    covered: u64,
}

static STATE: Mutex<Option<ShadowState>> = Mutex::new(None);

/// Records a root-query hydration observation. Returns whether it was
/// covered by an already-running query (for tests); logs the periodic
/// summary. No-op when covering detection is disabled.
pub fn note_hydration(root_table: &str, transformation_hash: &str) -> bool {
    if !crate::query_engine_options::get().enable_covering {
        return false;
    }
    let mut guard = STATE.lock().unwrap();
    let state = guard.get_or_insert_with(ShadowState::default);
    let hashes = state.hydrated.entry(root_table.to_string()).or_default();
    let covered = hashes.contains(transformation_hash);
    hashes.insert(transformation_hash.to_string());
    state.total += 1;
    if covered {
        state.covered += 1;
    }
    if state.total.is_multiple_of(SUMMARY_EVERY) {
        crate::info!(
            "query coverage shadow summary: mode=shadow totalHydratedQueries={} \
             coveredHydratedQueries={} uncoveredHydratedQueries={}",
            state.total,
            state.covered,
            state.total - state.covered
        );
    }
    covered
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_repeat_hydrations_count_as_covered() {
        // Covering is enabled by default (upstream default true).
        assert!(!note_hydration("issues", "h1"));
        assert!(note_hydration("issues", "h1"));
        // Same hash on a different root table is NOT covered.
        assert!(!note_hydration("comments", "h1"));
    }
}
