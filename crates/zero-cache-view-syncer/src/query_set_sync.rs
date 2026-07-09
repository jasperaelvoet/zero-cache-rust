//! Port of a pure decision fragment from `ViewSyncerService#addAndRemoveQueries`
//! (view-syncer.ts) — a second slice of `ViewSyncerService` itself,
//! alongside `view_syncer_lifecycle.rs`. `#addAndRemoveQueries` is the
//! method that actually wires a client's query set to the IVM pipeline
//! (hydration, CVR tracking, pokes); most of it is deeply coupled to
//! `CVRQueryDrivenUpdater`/the live IVM `pipelines` object/pokers, none of
//! which exist in this port yet. What IS extractable and pure is the
//! "should we force a CVR version bump for already-hydrated queries being
//! re-executed" decision — the block right before `updater.trackQueries`
//! that upstream's own comment explains:
//!
//! > For already-gotten queries being re-executed without a stateVersion
//! > or transformationHash change, trackQueries does not bump
//! > configVersion. Force a bump so any row diff produced by received()
//! > gets propagated to the client via a poke.
//!
//! This module ports exactly that decision, independent of the CVR
//! updater it feeds into.

use std::collections::{HashMap, HashSet};

use crate::cvr_updater::ensure_new_version;
use crate::cvr_version::CvrVersion;

/// A query being added/re-executed. Port of the trimmed shape
/// `#addAndRemoveQueries` reads for this decision (`id`/`transformationHash`
/// — `ast`/`name` aren't needed here).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddedQuery {
    pub id: String,
    pub transformation_hash: String,
}

/// Port of `sameHashRehydratedQueryIDs`: queries being added whose
/// `transformationHash` is unchanged from what the CVR already has
/// recorded for that query id (i.e. genuinely a re-execution, not a real
/// query change).
pub fn same_hash_rehydrated_query_ids(
    cvr_query_transformation_hashes: &HashMap<String, String>,
    add_queries: &[AddedQuery],
) -> Vec<String> {
    add_queries
        .iter()
        .filter(|q| cvr_query_transformation_hashes.get(&q.id) == Some(&q.transformation_hash))
        .map(|q| q.id.clone())
        .collect()
}

/// Port of `trackQueriesWillBumpVersion`: whether `updater.trackQueries`
/// will bump the CVR's `configVersion` on its own — a state-version
/// advance, any removals, or any add whose transformation hash actually
/// changed all cause a bump; re-executing unchanged queries alone does not.
pub fn track_queries_will_bump_version(
    current_state_version: i64,
    cvr_state_version: i64,
    remove_count: usize,
    cvr_query_transformation_hashes: &HashMap<String, String>,
    add_queries: &[AddedQuery],
) -> bool {
    current_state_version > cvr_state_version
        || remove_count > 0
        || add_queries
            .iter()
            .any(|q| cvr_query_transformation_hashes.get(&q.id) != Some(&q.transformation_hash))
}

/// Why a forced version bump was needed. Port of the `reason` computed for
/// the `#sameHashRehydrationVersionBumps` metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForcedBumpReason {
    /// Some re-executed queries had row-set-signature drift, others were
    /// missing a pipeline entirely.
    Mixed,
    RowSetSignatureDrift,
    MissingPipeline,
}

/// Port of the whole decision block: given the already-computed
/// `same_hash_rehydrated_query_ids` and whether `trackQueries` will bump
/// the version on its own, decides whether a bump must be forced and, if
/// so, why (for the metric upstream records) — `None` means no forced bump
/// is needed (either there's nothing to force, or `trackQueries` will
/// already bump it).
pub fn decide_forced_version_bump(
    same_hash_rehydrated_query_ids: &[String],
    track_queries_will_bump_version: bool,
    drifted_query_ids: &HashSet<String>,
) -> Option<ForcedBumpReason> {
    if same_hash_rehydrated_query_ids.is_empty() || track_queries_will_bump_version {
        return None;
    }

    let drifted = same_hash_rehydrated_query_ids
        .iter()
        .filter(|id| drifted_query_ids.contains(*id))
        .count();
    let missing = same_hash_rehydrated_query_ids.len() - drifted;

    Some(if drifted > 0 && missing > 0 {
        ForcedBumpReason::Mixed
    } else if drifted > 0 {
        ForcedBumpReason::RowSetSignatureDrift
    } else {
        ForcedBumpReason::MissingPipeline
    })
}

/// WIRING: composes [`decide_forced_version_bump`] with
/// `cvr_updater::ensure_new_version` — the actual `updater.ensureNewVersion()`
/// call upstream makes when a forced bump is decided (right before
/// `trackQueries` in `#addAndRemoveQueries`). Neither piece calls the
/// other independently anywhere else in this port; this is the first place
/// they're composed, proving the decision function's result is directly
/// usable to drive the real CVR-version state machine, not just a
/// standalone bool/enum. Returns the reason if a bump was forced (and
/// mutates `current_version` via `ensure_new_version`), or `None` if no
/// bump was needed (leaving `current_version` untouched).
#[allow(clippy::too_many_arguments)]
pub fn apply_forced_version_bump_if_needed(
    cvr_query_transformation_hashes: &HashMap<String, String>,
    add_queries: &[AddedQuery],
    remove_count: usize,
    current_state_version: i64,
    cvr_state_version: i64,
    drifted_query_ids: &HashSet<String>,
    orig_version: &CvrVersion,
    current_version: &mut CvrVersion,
) -> Option<ForcedBumpReason> {
    let same_hash = same_hash_rehydrated_query_ids(cvr_query_transformation_hashes, add_queries);
    let will_bump = track_queries_will_bump_version(
        current_state_version,
        cvr_state_version,
        remove_count,
        cvr_query_transformation_hashes,
        add_queries,
    );
    let reason = decide_forced_version_bump(&same_hash, will_bump, drifted_query_ids)?;
    ensure_new_version(orig_version, current_version);
    Some(reason)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cvr_hashes(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn added(id: &str, hash: &str) -> AddedQuery {
        AddedQuery {
            id: id.into(),
            transformation_hash: hash.into(),
        }
    }

    #[test]
    fn same_hash_rehydrated_ids_matches_unchanged_queries() {
        let cvr = cvr_hashes(&[("q1", "h1"), ("q2", "h2")]);
        let add = vec![added("q1", "h1"), added("q2", "h2-new")];
        assert_eq!(
            same_hash_rehydrated_query_ids(&cvr, &add),
            vec!["q1".to_string()]
        );
    }

    #[test]
    fn same_hash_rehydrated_ids_excludes_brand_new_queries() {
        let cvr = cvr_hashes(&[]);
        let add = vec![added("q1", "h1")];
        assert!(same_hash_rehydrated_query_ids(&cvr, &add).is_empty());
    }

    #[test]
    fn track_queries_bumps_on_state_version_advance() {
        let cvr = cvr_hashes(&[("q1", "h1")]);
        assert!(track_queries_will_bump_version(
            10,
            5,
            0,
            &cvr,
            &[added("q1", "h1")]
        ));
    }

    #[test]
    fn track_queries_bumps_on_any_removal() {
        let cvr = cvr_hashes(&[]);
        assert!(track_queries_will_bump_version(5, 5, 1, &cvr, &[]));
    }

    #[test]
    fn track_queries_bumps_on_changed_transformation_hash() {
        let cvr = cvr_hashes(&[("q1", "old")]);
        assert!(track_queries_will_bump_version(
            5,
            5,
            0,
            &cvr,
            &[added("q1", "new")]
        ));
    }

    #[test]
    fn track_queries_does_not_bump_for_pure_rehydration() {
        let cvr = cvr_hashes(&[("q1", "h1")]);
        assert!(!track_queries_will_bump_version(
            5,
            5,
            0,
            &cvr,
            &[added("q1", "h1")]
        ));
    }

    #[test]
    fn no_forced_bump_when_nothing_rehydrated() {
        assert_eq!(
            decide_forced_version_bump(&[], false, &HashSet::new()),
            None
        );
    }

    #[test]
    fn no_forced_bump_when_track_queries_already_bumps() {
        assert_eq!(
            decide_forced_version_bump(&["q1".to_string()], true, &HashSet::new()),
            None
        );
    }

    #[test]
    fn forced_bump_reason_is_missing_pipeline_when_none_drifted() {
        let ids = vec!["q1".to_string(), "q2".to_string()];
        assert_eq!(
            decide_forced_version_bump(&ids, false, &HashSet::new()),
            Some(ForcedBumpReason::MissingPipeline)
        );
    }

    #[test]
    fn forced_bump_reason_is_drift_when_all_drifted() {
        let ids = vec!["q1".to_string()];
        let drifted: HashSet<String> = ["q1".to_string()].into_iter().collect();
        assert_eq!(
            decide_forced_version_bump(&ids, false, &drifted),
            Some(ForcedBumpReason::RowSetSignatureDrift)
        );
    }

    #[test]
    fn forced_bump_reason_is_mixed_when_some_drifted_some_missing() {
        let ids = vec!["q1".to_string(), "q2".to_string()];
        let drifted: HashSet<String> = ["q1".to_string()].into_iter().collect();
        assert_eq!(
            decide_forced_version_bump(&ids, false, &drifted),
            Some(ForcedBumpReason::Mixed)
        );
    }

    fn v(state: &str) -> CvrVersion {
        CvrVersion {
            state_version: state.into(),
            config_version: None,
        }
    }

    #[test]
    fn apply_forced_bump_actually_bumps_the_version_when_needed() {
        let cvr = cvr_hashes(&[("q1", "h1")]);
        let orig = v("05");
        let mut current = orig.clone();
        let reason = apply_forced_version_bump_if_needed(
            &cvr,
            &[added("q1", "h1")],
            0,
            5,
            5,
            &HashSet::new(),
            &orig,
            &mut current,
        );
        assert_eq!(reason, Some(ForcedBumpReason::MissingPipeline));
        assert_ne!(
            current, orig,
            "the WIRED composition must actually mutate current_version, not just report a reason"
        );
    }

    #[test]
    fn apply_forced_bump_leaves_version_untouched_when_track_queries_already_bumps() {
        let cvr = cvr_hashes(&[("q1", "old")]);
        let orig = v("05");
        let mut current = orig.clone();
        // A real transformation-hash change means trackQueries bumps on its own.
        let reason = apply_forced_version_bump_if_needed(
            &cvr,
            &[added("q1", "new")],
            0,
            5,
            5,
            &HashSet::new(),
            &orig,
            &mut current,
        );
        assert_eq!(reason, None);
        assert_eq!(current, orig);
    }

    #[test]
    fn apply_forced_bump_is_idempotent_like_ensure_new_version() {
        let cvr = cvr_hashes(&[("q1", "h1")]);
        let orig = v("05");
        let mut current = orig.clone();
        apply_forced_version_bump_if_needed(
            &cvr,
            &[added("q1", "h1")],
            0,
            5,
            5,
            &HashSet::new(),
            &orig,
            &mut current,
        );
        let after_first = current.clone();
        // A second forced-bump decision against the SAME orig must not bump again.
        apply_forced_version_bump_if_needed(
            &cvr,
            &[added("q1", "h1")],
            0,
            5,
            5,
            &HashSet::new(),
            &orig,
            &mut current,
        );
        assert_eq!(current, after_first);
    }
}
