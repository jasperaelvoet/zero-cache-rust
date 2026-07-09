//! Port of the pure per-row decision logic inside `CVRQueryDrivenUpdater#received`
//! (view-syncer/cvr.ts) — the row-reconciliation half of `CVRQueryDrivenUpdater`
//! flagged as the next slice after `cvr_query_driven_updater.rs`'s query-side
//! port. `received` itself does real Postgres I/O
//! (`this._cvrStore.getRowRecords()`/`putRowRecord`/`delRowRecord`) around
//! this decision; this module ports the decision ONLY — given one row's
//! existing CVR state and this update's data, what merged ref-counts result,
//! what gets written to the row-record store, and what (if any) patch goes
//! to the client — leaving the actual store calls to a caller that has a
//! live `CVRStore`.
//!
//! Scope deviation: upstream keys everything by the structured `RowID`
//! (`{schema, table, rowKey}`) via a `CustomKeyMap`. `RowID` doesn't derive
//! `Hash`/`Ord` in this port (its `row_key: BTreeMap<String, JsonValue>`
//! contains `f64`, which isn't hashable/totally-ordered) and adding that
//! isn't this module's job. So the row-batch accumulator state
//! (`#receivedRows`/`#lastPatches`) is generic over any `K: Clone + Eq +
//! Hash` row-id key — a caller derives a stable key (e.g. a formatted
//! string) from its `RowID` however it likes.

use std::collections::HashMap;

use crate::cvr_ref_counts::{merge_ref_counts, RefCounts};
use crate::cvr_version::{cmp_versions, max_version, CvrVersion};

/// The minimal existing-row state this decision needs. Port of the
/// relevant fields of `RowRecord` (`rowVersion`, `patchVersion`,
/// `refCounts`).
#[derive(Debug, Clone, PartialEq)]
pub struct ExistingRow {
    pub row_version: String,
    pub patch_version: CvrVersion,
    pub ref_counts: Option<RefCounts>,
}

/// One row update as received from query execution. Port of `RowUpdate`,
/// minus the actual row `contents` payload (only whether it's present
/// matters for this decision — see module doc).
#[derive(Debug, Clone, PartialEq)]
pub struct RowUpdateInput {
    pub version: Option<String>,
    pub has_contents: bool,
    pub ref_counts: Option<RefCounts>,
}

/// What to write to the row-record store for this row. Port of the
/// `putRowRecord`/`delRowRecord` branch.
#[derive(Debug, Clone, PartialEq)]
pub enum RowStoreWrite {
    /// `putRowRecord({id, rowVersion, patchVersion, refCounts: merged})`.
    Put {
        row_version: String,
        patch_version: CvrVersion,
        merged_ref_counts: Option<RefCounts>,
    },
    /// `delRowRecord(id)` — a row that was added and then removed within
    /// the same update, with no prior CVR record to speak of.
    Delete,
}

/// The client-facing patch (if any) this row update produces. Port of the
/// `'del'`/`'put'` branches of the `patches.push(...)` calls (the `id`/
/// `contents` payload itself is the caller's job to attach — this is just
/// the op + version).
#[derive(Debug, Clone, PartialEq)]
pub enum RowClientPatch {
    Del { to_version: CvrVersion },
    Put { to_version: CvrVersion },
}

/// Tracks the last patch sent for a row, for dedup — port of
/// `#lastPatches`'s value shape (`RowPatchInfo`).
#[derive(Debug, Clone, PartialEq)]
pub struct LastPatchInfo {
    /// `None` represents the tombstone (`rowVersion: null`) case.
    pub row_version: Option<String>,
    pub to_version: CvrVersion,
}

/// The full outcome of processing one received row. Port of everything one
/// iteration of `received`'s `for (const [id, update] of rows.entries())`
/// loop body decides.
#[derive(Debug, Clone, PartialEq)]
pub struct RowOutcome {
    pub store_write: RowStoreWrite,
    pub client_patch: Option<RowClientPatch>,
}

/// Port of `#assertNewVersion`: the CVR version must already have been
/// bumped above `orig` by the time any row is processed (the poke-start
/// message declares the final cookie before any poke parts from `received`
/// go out). Panics if not — a caller bug (queries were tracked without a
/// version bump), not a recoverable condition, matching upstream's
/// `assert`.
pub fn assert_new_version(orig: &CvrVersion, current: &CvrVersion) {
    assert!(
        cmp_versions(&Some(orig.clone()), &Some(current.clone())) < 0,
        "Expected CVR version to have been bumped above original"
    );
}

/// Port of one iteration of `received`'s row-processing loop. `received_rows`/
/// `last_patches` are the caller-owned accumulators standing in for
/// `#receivedRows`/`#lastPatches` (see module doc for the `K` deviation);
/// both are updated in place, matching upstream's `this.#receivedRows.set`/
/// `this.#lastPatches.set` side effects.
#[allow(clippy::too_many_arguments)]
pub fn process_received_row<K: Clone + Eq + std::hash::Hash>(
    id: K,
    existing: Option<&ExistingRow>,
    update: &RowUpdateInput,
    remove_hashes: Option<&std::collections::HashSet<String>>,
    orig_version: &CvrVersion,
    cvr_version: &CvrVersion,
    received_rows: &mut HashMap<K, Option<RefCounts>>,
    last_patches: &mut HashMap<K, LastPatchInfo>,
) -> RowOutcome {
    let previously_received = received_rows.get(&id).cloned();

    let merged = match &previously_received {
        Some(prev) => merge_ref_counts(prev.as_ref(), update.ref_counts.as_ref(), None),
        None => merge_ref_counts(
            existing.and_then(|e| e.ref_counts.as_ref()),
            update.ref_counts.as_ref(),
            remove_hashes,
        ),
    };
    received_rows.insert(id.clone(), merged.clone());

    let new_row_version: Option<String> = if merged.is_none() {
        None
    } else {
        update.version.clone()
    };
    let patch_version = if existing.is_some_and(|e| Some(e.row_version.clone()) == new_row_version)
    {
        // existing row is unchanged
        existing.unwrap().patch_version.clone()
    } else {
        assert_new_version(orig_version, cvr_version);
        cvr_version.clone()
    };

    let row_version = update
        .version
        .clone()
        .or_else(|| existing.map(|e| e.row_version.clone()));
    let store_write = match &row_version {
        Some(rv) => RowStoreWrite::Put {
            row_version: rv.clone(),
            patch_version: patch_version.clone(),
            merged_ref_counts: merged.clone(),
        },
        None => RowStoreWrite::Delete,
    };

    let last_patch = last_patches.get(&id).cloned();
    let to_version = max_version(&patch_version, last_patch.as_ref().map(|p| &p.to_version));

    let client_patch = if merged.is_none() {
        if existing.is_some() || previously_received.is_some() {
            if last_patch.as_ref().map(|p| p.row_version.clone()) != Some(None) {
                last_patches.insert(
                    id.clone(),
                    LastPatchInfo {
                        row_version: None,
                        to_version: to_version.clone(),
                    },
                );
                Some(RowClientPatch::Del { to_version })
            } else {
                None
            }
        } else {
            None
        }
    } else if update.has_contents {
        let rv = row_version
            .clone()
            .expect("rowVersion is required when contents is present");
        let should_emit = match &last_patch {
            None => true,
            Some(p) => match &p.row_version {
                None => true,
                Some(prev_rv) => prev_rv < &rv,
            },
        };
        if should_emit {
            last_patches.insert(
                id.clone(),
                LastPatchInfo {
                    row_version: Some(rv),
                    to_version: to_version.clone(),
                },
            );
            Some(RowClientPatch::Put { to_version })
        } else {
            None
        }
    } else {
        None
    };

    RowOutcome {
        store_write,
        client_patch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> CvrVersion {
        CvrVersion {
            state_version: s.into(),
            config_version: None,
        }
    }

    fn rc(pairs: &[(&str, i64)]) -> RefCounts {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    #[test]
    fn brand_new_row_with_contents_puts_and_patches() {
        let mut received_rows = HashMap::new();
        let mut last_patches = HashMap::new();
        let update = RowUpdateInput {
            version: Some("v1".into()),
            has_contents: true,
            ref_counts: Some(rc(&[("q1", 1)])),
        };

        let outcome = process_received_row(
            "row1",
            None,
            &update,
            None,
            &v("01"),
            &v("02"),
            &mut received_rows,
            &mut last_patches,
        );

        assert_eq!(
            outcome.store_write,
            RowStoreWrite::Put {
                row_version: "v1".into(),
                patch_version: v("02"),
                merged_ref_counts: Some(rc(&[("q1", 1)]))
            }
        );
        assert_eq!(
            outcome.client_patch,
            Some(RowClientPatch::Put {
                to_version: v("02")
            })
        );
    }

    #[test]
    fn row_that_drops_to_zero_refs_after_being_previously_synced_still_puts_with_null_refcounts_and_sends_del(
    ) {
        // Port note: upstream deliberately uses `update.version` (not the
        // merged-refcounts-derived `newRowVersion`) to decide the store
        // write, so a row dropping to zero refs but still carrying a
        // version still gets a `putRowRecord` (with `refCounts: null`,
        // i.e. a tombstone row-record) rather than a `delRowRecord` — only
        // a MISSING version produces a `delRowRecord` (see the
        // `added_then_removed_within_same_update...` test below). The
        // client still gets a 'del' patch either way.
        let mut received_rows = HashMap::new();
        let mut last_patches = HashMap::new();
        let existing = ExistingRow {
            row_version: "v1".into(),
            patch_version: v("01"),
            ref_counts: Some(rc(&[("q1", 1)])),
        };
        let mut remove: std::collections::HashSet<String> = std::collections::HashSet::new();
        remove.insert("q1".to_string());
        let update = RowUpdateInput {
            version: Some("v1".into()),
            has_contents: false,
            ref_counts: None,
        };

        let outcome = process_received_row(
            "row1",
            Some(&existing),
            &update,
            Some(&remove),
            &v("01"),
            &v("02"),
            &mut received_rows,
            &mut last_patches,
        );

        assert_eq!(
            outcome.store_write,
            RowStoreWrite::Put {
                row_version: "v1".into(),
                patch_version: v("02"),
                merged_ref_counts: None
            }
        );
        assert_eq!(
            outcome.client_patch,
            Some(RowClientPatch::Del {
                to_version: v("02")
            })
        );
    }

    #[test]
    fn dedupes_repeated_put_of_same_row_version() {
        let mut received_rows = HashMap::new();
        let mut last_patches = HashMap::new();
        last_patches.insert(
            "row1",
            LastPatchInfo {
                row_version: Some("v1".into()),
                to_version: v("02"),
            },
        );
        let update = RowUpdateInput {
            version: Some("v1".into()),
            has_contents: true,
            ref_counts: Some(rc(&[("q1", 1)])),
        };
        let existing = ExistingRow {
            row_version: "v1".into(),
            patch_version: v("02"),
            ref_counts: Some(rc(&[("q1", 1)])),
        };

        let outcome = process_received_row(
            "row1",
            Some(&existing),
            &update,
            None,
            &v("01"),
            &v("02"),
            &mut received_rows,
            &mut last_patches,
        );

        assert_eq!(
            outcome.client_patch, None,
            "same rowVersion as last patch should be deduped, not re-sent"
        );
    }

    #[test]
    fn newer_row_version_overrides_dedupe() {
        let mut received_rows = HashMap::new();
        let mut last_patches = HashMap::new();
        last_patches.insert(
            "row1",
            LastPatchInfo {
                row_version: Some("v1".into()),
                to_version: v("02"),
            },
        );
        let update = RowUpdateInput {
            version: Some("v2".into()),
            has_contents: true,
            ref_counts: Some(rc(&[("q1", 1)])),
        };

        let outcome = process_received_row(
            "row1",
            None,
            &update,
            None,
            &v("02"),
            &v("03"),
            &mut received_rows,
            &mut last_patches,
        );

        assert_eq!(
            outcome.client_patch,
            Some(RowClientPatch::Put {
                to_version: v("03")
            })
        );
    }

    #[test]
    fn added_then_removed_within_same_update_has_no_row_version_and_deletes() {
        // No existing row, ref_counts merges to None (all zero), and no
        // update.version means row_version ends up None -> Delete store
        // write, but a 'del' patch is only sent if the row was previously
        // known (existing or previously_received) — here it's genuinely
        // brand new, so per upstream no patch is sent either.
        let mut received_rows = HashMap::new();
        let mut last_patches = HashMap::new();
        let update = RowUpdateInput {
            version: None,
            has_contents: false,
            ref_counts: None,
        };

        let outcome = process_received_row(
            "row1",
            None,
            &update,
            None,
            &v("01"),
            &v("02"),
            &mut received_rows,
            &mut last_patches,
        );

        assert_eq!(outcome.store_write, RowStoreWrite::Delete);
        assert_eq!(outcome.client_patch, None);
    }
}
