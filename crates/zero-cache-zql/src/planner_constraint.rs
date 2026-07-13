//! Port of `zql/src/planner` (the query-cost planner/optimizer, ~4900
//! lines across `planner-graph.ts`/`planner-node.ts`/`planner-join.ts`/
//! `planner-fan-in.ts`/`planner-fan-out.ts`/`planner-source.ts`/
//! `planner-terminus.ts`/`planner-connection.ts`/`planner-builder.ts`/
//! `planner-debug.ts`, plus this module) — a directory-coverage scan found
//! this entire directory had ZERO representation in PORTING.md. Given the
//! size, this is a real, substantial, entirely unstarted gap, not
//! something a single round closes; this module ports the one genuinely
//! tiny, self-contained, dependency-free piece as a first toehold (same
//! approach `view_syncer_lifecycle.rs` took for `ViewSyncerService`):
//! `planner-constraint.ts`'s `mergeConstraints`.
//!
//! NOT ported (the real remaining gap): the planner graph itself
//! (`PlannerNode`/`PlannerJoin`/`PlannerFanIn`/`PlannerFanOut`/
//! `PlannerSource`/`PlannerTerminus`/`PlannerConnection`), cost estimation,
//! constraint propagation through the graph, and `planner-builder.ts`'s
//! AST-to-plan-graph construction — all of it deeply coupled to a shared
//! mutable graph structure with parent/child node references, not
//! extractable as isolated pure functions the way this one was.

use std::collections::BTreeSet;

/// Port of `PlannerConstraint` (`Record<string, undefined>` — used purely
/// as an existence-only set of column names the planner already knows a
/// downstream join/filter will constrain, values are never meaningfully
/// read). Modeled as a `BTreeSet<String>` rather than a map-to-unit, since
/// that's what the type is actually used as.
pub type PlannerConstraint = BTreeSet<String>;

/// Port of `mergeConstraints`: unions two optional constraint sets
/// (`{...a, ...b}` on `Record<string, undefined>` keys is exactly a set
/// union — the `undefined` values carry no information to merge/override).
/// `None` is upstream's `undefined`, meaning "no constraint from this
/// branch" — union with `None` is a no-op, matching the early returns.
pub fn merge_constraints(
    a: Option<&PlannerConstraint>,
    b: Option<&PlannerConstraint>,
) -> Option<PlannerConstraint> {
    match (a, b) {
        (None, None) => None,
        (Some(a), None) => Some(a.clone()),
        (None, Some(b)) => Some(b.clone()),
        (Some(a), Some(b)) => Some(a.union(b).cloned().collect()),
    }
}

/// Port of `planner-join.ts`'s `translateConstraintsForFlippedJoin`:
/// remaps an incoming constraint's keys from parent-space to child-space
/// via POSITIONAL correspondence between `parent_keys`/`child_keys` (e.g.
/// `parentConstraint = {issueID, projectID}` / `childConstraint = {id,
/// projectID}` means "whatever key was at parent-position 0 (`issueID`)
/// maps to child-position 0 (`id`)").
///
/// `parent_keys`/`child_keys` are taken as ordered `&[String]` slices
/// rather than `PlannerConstraint`s (unlike `merge_constraints`) because
/// this function's correctness genuinely depends on key ORDER — upstream
/// relies on JS `Object.keys()`'s insertion-order guarantee to do the
/// positional mapping. `PlannerConstraint`'s `BTreeSet<String>`
/// representation (chosen for `merge_constraints`, where only set-union
/// semantics matter) would silently reorder keys alphabetically and
/// produce wrong mappings here — a real representation mismatch worth
/// naming explicitly rather than forcing this function through the same
/// type and getting subtly wrong results.
pub fn translate_constraints_for_flipped_join(
    incoming_constraint: Option<&PlannerConstraint>,
    parent_keys: &[String],
    child_keys: &[String],
) -> Option<PlannerConstraint> {
    let incoming = incoming_constraint?;
    let mut translated = PlannerConstraint::new();
    for key in incoming {
        if let Some(index) = parent_keys.iter().position(|k| k == key) {
            // Upstream (planner-join.ts:43) does `translated[childKeys[index]]`
            // with no bounds check: when `index >= childKeys.length`,
            // `childKeys[index]` is `undefined`, which JS stringifies to the
            // literal key `"undefined"`. Match that rather than silently
            // dropping the overflow key. (Unreachable in practice — parent
            // and child key lists are always the same length — but kept
            // faithful to upstream.)
            let child_key = child_keys
                .get(index)
                .cloned()
                .unwrap_or_else(|| "undefined".to_string());
            translated.insert(child_key);
        }
    }
    if translated.is_empty() {
        None
    } else {
        Some(translated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(cols: &[&str]) -> PlannerConstraint {
        cols.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn merging_with_none_returns_the_other_side_unchanged() {
        let a = set(&["id"]);
        assert_eq!(merge_constraints(Some(&a), None), Some(a.clone()));
        assert_eq!(merge_constraints(None, Some(&a)), Some(a));
    }

    #[test]
    fn merging_both_none_is_none() {
        assert_eq!(merge_constraints(None, None), None);
    }

    #[test]
    fn merging_two_constraints_unions_their_columns() {
        let a = set(&["id", "assignee_id"]);
        let b = set(&["status"]);
        assert_eq!(
            merge_constraints(Some(&a), Some(&b)),
            Some(set(&["id", "assignee_id", "status"]))
        );
    }

    #[test]
    fn merging_overlapping_constraints_deduplicates() {
        let a = set(&["id", "status"]);
        let b = set(&["status", "priority"]);
        assert_eq!(
            merge_constraints(Some(&a), Some(&b)),
            Some(set(&["id", "status", "priority"]))
        );
    }

    fn keys(cols: &[&str]) -> Vec<String> {
        cols.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn translate_is_none_for_an_absent_incoming_constraint() {
        assert_eq!(
            translate_constraints_for_flipped_join(None, &keys(&["issueID"]), &keys(&["id"])),
            None
        );
    }

    #[test]
    fn translate_maps_a_key_by_position() {
        let incoming = set(&["issueID"]);
        let parent = keys(&["issueID", "projectID"]);
        let child = keys(&["id", "projectID"]);
        assert_eq!(
            translate_constraints_for_flipped_join(Some(&incoming), &parent, &child),
            Some(set(&["id"]))
        );
    }

    #[test]
    fn translate_maps_multiple_keys_by_their_own_positions() {
        let incoming = set(&["issueID", "projectID"]);
        let parent = keys(&["issueID", "projectID"]);
        let child = keys(&["id", "projectID"]);
        assert_eq!(
            translate_constraints_for_flipped_join(Some(&incoming), &parent, &child),
            Some(set(&["id", "projectID"]))
        );
    }

    #[test]
    fn translate_overflow_index_maps_to_the_literal_undefined_key() {
        // When a matched parent position has no corresponding child key
        // (`index >= child_keys.len()`), upstream's `childKeys[index]` is
        // `undefined`, stringifying to the literal `"undefined"` key. Match
        // that instead of dropping the key. (Unreachable in practice.)
        let incoming = set(&["projectID"]);
        let parent = keys(&["issueID", "projectID"]);
        let child = keys(&["id"]);
        assert_eq!(
            translate_constraints_for_flipped_join(Some(&incoming), &parent, &child),
            Some(set(&["undefined"]))
        );
    }

    #[test]
    fn translate_drops_a_key_not_present_in_parent_keys() {
        let incoming = set(&["unrelatedKey"]);
        let parent = keys(&["issueID"]);
        let child = keys(&["id"]);
        assert_eq!(
            translate_constraints_for_flipped_join(Some(&incoming), &parent, &child),
            None,
            "a key with no positional match in parent_keys must be dropped, not passed through"
        );
    }
}
