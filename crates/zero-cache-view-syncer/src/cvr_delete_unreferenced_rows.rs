//! Port of `CVRQueryDrivenUpdater#deleteUnreferencedRows`/`#deleteUnreferencedRow`
//! (view-syncer/cvr.ts) — Step 5 of the CVR Sync Algorithm, and the last
//! named piece of `CVRQueryDrivenUpdater` itself (the query-side is
//! `cvr_query_driven_updater.rs`, the `received` row-decision half is
//! `cvr_row_received.rs`; this closes the third and final piece). Given
//! every existing row associated with the queries that were just executed
//! or removed, decides which rows are no longer referenced by ANY
//! surviving query and must be deleted.
//!
//! Scope: same split as `cvr_row_received.rs` — this ports the pure
//! decision (what to write to the row-record store, which rows to delete,
//! what client patches to emit), not the real
//! `CVRStore.getRowRecords()`/`putRowRecord` I/O the caller performs
//! around it. Same `K: Clone + Eq + Hash` generic row-id deviation as
//! `cvr_row_received.rs`, for the same reason (`RowID` isn't
//! hashable/orderable in this port).

use std::collections::{HashMap, HashSet};

use crate::cvr_ref_counts::{merge_ref_counts, RefCounts};
use crate::cvr_row_received::{assert_new_version, LastPatchInfo};
use crate::cvr_version::{max_version, CvrVersion};

/// An existing row potentially associated with a just-executed/removed
/// query. Port of the fields of `RowRecord` this decision reads.
#[derive(Debug, Clone, PartialEq)]
pub struct ExistingRow<K> {
    pub id: K,
    pub row_version: String,
    pub patch_version: CvrVersion,
    pub ref_counts: Option<RefCounts>,
}

/// The row-record store write `#deleteUnreferencedRow` always performs
/// (`putRowRecord` is called unconditionally, even for a row being
/// deleted — the tombstone itself is a `RowRecord` with `refCounts: null`).
#[derive(Debug, Clone, PartialEq)]
pub struct RowRecordWrite<K> {
    pub id: K,
    pub row_version: String,
    pub patch_version: CvrVersion,
    pub ref_counts: Option<RefCounts>,
}

/// The outcome of running `delete_unreferenced_rows` over a batch of
/// existing rows.
#[derive(Debug, Clone, PartialEq)]
pub struct DeletionResult<K> {
    /// Every row-record write to persist, in input order (port of each
    /// `putRowRecord` call `#deleteUnreferencedRow` makes).
    pub row_writes: Vec<RowRecordWrite<K>>,
    /// The client-facing `'del'` patches (row id + `toVersion`) for rows
    /// that are now fully unreferenced, deduped against `last_patches`.
    pub patches: Vec<(K, CvrVersion)>,
}

/// Port of `#deleteUnreferencedRow` for one row: skips it entirely if
/// `received_rows` already has a truthy (non-null) entry for it (i.e. it
/// was actually returned by the just-executed queries, so it's still
/// referenced and `received()` already handled its store write) — matches
/// upstream's `if (this.#receivedRows.get(existing.id)) return null;`
/// truthy check (a `None`/absent entry OR an explicit `null`/`None` entry
/// both count as "not received", so the row must be re-evaluated here).
fn delete_unreferenced_row<K: Clone + Eq + std::hash::Hash>(
    existing: &ExistingRow<K>,
    received_rows: &HashMap<K, Option<RefCounts>>,
    removed_or_executed_query_ids: &HashSet<String>,
    orig_version: &CvrVersion,
    cvr_version: &CvrVersion,
) -> Option<RowRecordWrite<K>> {
    if matches!(received_rows.get(&existing.id), Some(Some(_))) {
        return None;
    }

    let new_ref_counts = merge_ref_counts(
        existing.ref_counts.as_ref(),
        None,
        Some(removed_or_executed_query_ids),
    );

    // If still referenced (by some other, non-removed/executed query), keep
    // the existing patchVersion — the row's contents/existence haven't
    // changed from the client's perspective. If fully unreferenced, it
    // needs a fresh patchVersion for the delete poke.
    let patch_version = match &new_ref_counts {
        Some(_) => existing.patch_version.clone(),
        None => {
            assert_new_version(orig_version, cvr_version);
            cvr_version.clone()
        }
    };

    Some(RowRecordWrite {
        id: existing.id.clone(),
        row_version: existing.row_version.clone(),
        patch_version,
        ref_counts: new_ref_counts,
    })
}

/// Port of `deleteUnreferencedRows`. `existing_rows` stands in for
/// `await this.#existingRows` (the caller already resolved that real-I/O
/// lookup). Panics if `removed_or_executed_query_ids` is empty but
/// `received_rows` is non-empty (port of the query-less-update assertion —
/// a config-only change should never have received any rows).
pub fn delete_unreferenced_rows<K: Clone + Eq + std::hash::Hash>(
    existing_rows: &[ExistingRow<K>],
    received_rows: &HashMap<K, Option<RefCounts>>,
    removed_or_executed_query_ids: &HashSet<String>,
    orig_version: &CvrVersion,
    cvr_version: &CvrVersion,
    last_patches: &mut HashMap<K, LastPatchInfo>,
) -> DeletionResult<K> {
    if removed_or_executed_query_ids.is_empty() {
        assert!(
            received_rows.is_empty(),
            "Expected no received rows for query-less update, got {}",
            received_rows.len()
        );
        return DeletionResult {
            row_writes: vec![],
            patches: vec![],
        };
    }

    let mut row_writes = Vec::new();
    let mut patches = Vec::new();

    for existing in existing_rows {
        let Some(write) = delete_unreferenced_row(
            existing,
            received_rows,
            removed_or_executed_query_ids,
            orig_version,
            cvr_version,
        ) else {
            continue;
        };
        let deleted = write.ref_counts.is_none();
        row_writes.push(write.clone());

        if !deleted {
            continue;
        }

        let last_patch = last_patches.get(&write.id).cloned();
        if let Some(lp) = &last_patch {
            if lp.row_version.is_none() {
                continue; // a 'del' was already sent for this row — dedupe.
            }
        }
        let to_version = max_version(cvr_version, last_patch.as_ref().map(|p| &p.to_version));
        patches.push((write.id.clone(), to_version.clone()));
        last_patches.insert(
            write.id.clone(),
            LastPatchInfo {
                row_version: None,
                to_version,
            },
        );
    }

    DeletionResult {
        row_writes,
        patches,
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

    fn row(id: &str, ref_counts: Option<RefCounts>) -> ExistingRow<&str> {
        ExistingRow {
            id,
            row_version: "v1".into(),
            patch_version: v("01"),
            ref_counts,
        }
    }

    #[test]
    fn query_less_update_with_no_received_rows_returns_empty() {
        let received_rows: HashMap<&str, Option<RefCounts>> = HashMap::new();
        let mut last_patches = HashMap::new();
        let result = delete_unreferenced_rows::<&str>(
            &[],
            &received_rows,
            &HashSet::new(),
            &v("01"),
            &v("01"),
            &mut last_patches,
        );
        assert!(result.row_writes.is_empty());
        assert!(result.patches.is_empty());
    }

    #[test]
    #[should_panic(expected = "Expected no received rows for query-less update")]
    fn query_less_update_with_received_rows_panics() {
        let mut received_rows: HashMap<&str, Option<RefCounts>> = HashMap::new();
        received_rows.insert("row1", Some(rc(&[("q1", 1)])));
        let mut last_patches = HashMap::new();
        delete_unreferenced_rows(
            &[],
            &received_rows,
            &HashSet::new(),
            &v("01"),
            &v("01"),
            &mut last_patches,
        );
    }

    #[test]
    fn row_already_received_this_cycle_is_skipped_entirely() {
        let existing = vec![row("row1", Some(rc(&[("q1", 1)])))];
        let mut received_rows: HashMap<&str, Option<RefCounts>> = HashMap::new();
        received_rows.insert("row1", Some(rc(&[("q1", 1)])));
        let mut removed = HashSet::new();
        removed.insert("q2".to_string());
        let mut last_patches = HashMap::new();

        let result = delete_unreferenced_rows(
            &existing,
            &received_rows,
            &removed,
            &v("01"),
            &v("01"),
            &mut last_patches,
        );
        assert!(
            result.row_writes.is_empty(),
            "a row already handled by received() should not be re-processed"
        );
    }

    #[test]
    fn row_still_referenced_by_a_surviving_query_keeps_patch_version_no_delete() {
        let existing = vec![row("row1", Some(rc(&[("q1", 1), ("q2", 1)])))];
        let received_rows: HashMap<&str, Option<RefCounts>> = HashMap::new();
        let mut removed = HashSet::new();
        removed.insert("q1".to_string()); // q1 removed, q2 still references it
        let mut last_patches = HashMap::new();

        let result = delete_unreferenced_rows(
            &existing,
            &received_rows,
            &removed,
            &v("01"),
            &v("02"),
            &mut last_patches,
        );

        assert_eq!(result.row_writes.len(), 1);
        assert_eq!(
            result.row_writes[0].patch_version,
            v("01"),
            "still-referenced row keeps its existing patchVersion"
        );
        assert_eq!(result.row_writes[0].ref_counts, Some(rc(&[("q2", 1)])));
        assert!(
            result.patches.is_empty(),
            "still-referenced row should not produce a delete patch"
        );
    }

    #[test]
    fn row_fully_unreferenced_gets_new_patch_version_and_delete_patch() {
        let existing = vec![row("row1", Some(rc(&[("q1", 1)])))];
        let received_rows: HashMap<&str, Option<RefCounts>> = HashMap::new();
        let mut removed = HashSet::new();
        removed.insert("q1".to_string());
        let mut last_patches = HashMap::new();

        let result = delete_unreferenced_rows(
            &existing,
            &received_rows,
            &removed,
            &v("01"),
            &v("02"),
            &mut last_patches,
        );

        assert_eq!(result.row_writes.len(), 1);
        assert_eq!(
            result.row_writes[0].patch_version,
            v("02"),
            "fully unreferenced row gets a fresh patchVersion"
        );
        assert_eq!(result.row_writes[0].ref_counts, None);
        assert_eq!(result.patches, vec![("row1", v("02"))]);
    }

    #[test]
    fn dedupes_against_an_already_sent_delete_patch() {
        let existing = vec![row("row1", Some(rc(&[("q1", 1)])))];
        let received_rows: HashMap<&str, Option<RefCounts>> = HashMap::new();
        let mut removed = HashSet::new();
        removed.insert("q1".to_string());
        let mut last_patches = HashMap::new();
        last_patches.insert(
            "row1",
            LastPatchInfo {
                row_version: None,
                to_version: v("02"),
            },
        );

        let result = delete_unreferenced_rows(
            &existing,
            &received_rows,
            &removed,
            &v("01"),
            &v("03"),
            &mut last_patches,
        );

        assert_eq!(
            result.row_writes.len(),
            1,
            "the row-record write still happens even when the client patch is deduped"
        );
        assert!(
            result.patches.is_empty(),
            "a 'del' already sent for this row should not be re-sent"
        );
    }

    #[test]
    fn multiple_rows_processed_independently() {
        let existing = vec![
            row("row1", Some(rc(&[("q1", 1)]))),
            row("row2", Some(rc(&[("q1", 1), ("q2", 1)]))),
        ];
        let received_rows: HashMap<&str, Option<RefCounts>> = HashMap::new();
        let mut removed = HashSet::new();
        removed.insert("q1".to_string());
        let mut last_patches = HashMap::new();

        let result = delete_unreferenced_rows(
            &existing,
            &received_rows,
            &removed,
            &v("01"),
            &v("02"),
            &mut last_patches,
        );

        assert_eq!(result.row_writes.len(), 2);
        assert_eq!(
            result.patches,
            vec![("row1", v("02"))],
            "row2 still has q2 referencing it, so only row1 should be deleted"
        );
    }
}
