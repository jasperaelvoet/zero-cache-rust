//! Port of `CVRQueryDrivenUpdater`'s core state-mutation logic
//! (view-syncer/cvr.ts) — the prerequisite named in the previous round as
//! blocking `ViewSyncerService#addAndRemoveQueries`'s actual IVM wiring
//! (`updater.trackQueries`'s output feeds `#pipelines.addQuery`/
//! `removeQuery`). This is a third `ViewSyncerService`-adjacent slice,
//! same incremental-extraction pattern as `view_syncer_lifecycle.rs`/
//! `query_set_sync.rs`.
//!
//! Scope: ports `#trackExecuted`/`#trackRemoved`/`trackQueries` — the
//! actual CVR-state mutation and patch generation for queries being
//! executed or removed — as free functions operating on `&mut Cvr` plus an
//! explicit `tracked: &mut HashSet<String>` standing in for
//! `#removedOrExecutedQueryIDs` (the caller owns this across a whole
//! `trackQueries` call, matching upstream's per-updater-instance field).
//! NOT ported: `#lookupRowsForExecutedAndRemovedQueries` (needs a live
//! `CVRStore.getRowRecords()` — real Postgres I/O), `received`/
//! `deleteUnreferencedRows`/`flush` (the row-reconciliation half of
//! `CVRQueryDrivenUpdater`, coupled to the same store), and the
//! `RowSetSignatureProvider` callback machinery. This module is the
//! query-side half only — real progress on the named prerequisite, not a
//! claim that `CVRQueryDrivenUpdater` (let alone `ViewSyncerService`) is
//! fully ported.

use std::collections::HashSet;

use crate::cvr_types::{Cvr, PatchOp, QueryPatch, QueryRecord};
use crate::cvr_updater::ensure_new_version;
use crate::cvr_version::CvrVersion;

/// Panics if `query` is an internal query — port of `assertNotInternal`
/// (`#trackRemoved` must only be called on non-internal queries, matching
/// upstream's assertion that internal queries are never in the
/// "desired-by-a-client" removal path).
fn assert_not_internal(query: &QueryRecord) {
    assert!(
        !matches!(query, QueryRecord::Internal(_)),
        "Query is internal"
    );
}

/// Reads a query's current `transformationHash`, uniformly across the
/// three `QueryRecord` variants.
fn transformation_hash(query: &QueryRecord) -> Option<&str> {
    match query {
        QueryRecord::Client(q) => q.base.transformation_hash.as_deref(),
        QueryRecord::Custom(q) => q.base.transformation_hash.as_deref(),
        QueryRecord::Internal(q) => q.transformation_hash.as_deref(),
    }
}

/// Whether this query has not yet reached the "gotten" state
/// (`patchVersion === undefined`) — only client/custom queries track this;
/// internal queries have no `patchVersion` field at all (upstream's
/// `query.type !== 'internal' && query.patchVersion === undefined` check).
fn needs_got_patch(query: &QueryRecord) -> bool {
    match query {
        QueryRecord::Client(q) => q.base.patch_version.is_none(),
        QueryRecord::Custom(q) => q.base.patch_version.is_none(),
        QueryRecord::Internal(_) => false,
    }
}

fn set_transformation(query: &mut QueryRecord, hash: &str, version: &CvrVersion) {
    match query {
        QueryRecord::Client(q) => {
            q.base.transformation_hash = Some(hash.to_string());
            q.base.transformation_version = Some(version.clone());
        }
        QueryRecord::Custom(q) => {
            q.base.transformation_hash = Some(hash.to_string());
            q.base.transformation_version = Some(version.clone());
        }
        QueryRecord::Internal(q) => {
            q.transformation_hash = Some(hash.to_string());
            q.transformation_version = Some(version.clone());
        }
    }
}

fn set_got(query: &mut QueryRecord, version: &CvrVersion) {
    match query {
        QueryRecord::Client(q) => q.base.patch_version = Some(version.clone()),
        QueryRecord::Custom(q) => q.base.patch_version = Some(version.clone()),
        QueryRecord::Internal(_) => unreachable!("needs_got_patch is false for internal queries"),
    }
}

/// Port of `#trackExecuted`. Panics if `query_id` was already tracked this
/// cycle (matching upstream's assertion), or if `query_id` isn't present in
/// `cvr.queries` (upstream's bare property access would produce `undefined`
/// and then throw on `.transformationHash` — this port makes that failure
/// explicit instead). Returns the "got" query patch if this transition
/// moved the query from desired-only to gotten, or an empty `Vec`
/// otherwise (including the no-op case where the hash didn't change).
pub fn track_executed(
    cvr: &mut Cvr,
    orig_version: &CvrVersion,
    tracked: &mut HashSet<String>,
    query_id: &str,
    new_transformation_hash: &str,
) -> Vec<QueryPatch> {
    assert!(
        !tracked.contains(query_id),
        "Query {query_id} already tracked as executed or removed"
    );
    tracked.insert(query_id.to_string());

    let query = cvr
        .queries
        .get(query_id)
        .unwrap_or_else(|| panic!("Query {query_id} not found in CVR"));
    if transformation_hash(query) == Some(new_transformation_hash) {
        return vec![];
    }

    let transformation_version = ensure_new_version(orig_version, &mut cvr.version);
    let query = cvr.queries.get_mut(query_id).expect("checked above");

    let mut patches = vec![];
    if needs_got_patch(query) {
        set_got(query, &transformation_version);
        patches.push(QueryPatch {
            op: PatchOp::Put,
            id: query_id.to_string(),
            client_id: None,
        });
    }
    set_transformation(query, new_transformation_hash, &transformation_version);
    patches
}

/// Port of `#trackRemoved`. Panics if `query_id` was already tracked this
/// cycle, if it isn't present in `cvr.queries`, or if it's an internal
/// query (matching `assertNotInternal`).
pub fn track_removed(
    cvr: &mut Cvr,
    orig_version: &CvrVersion,
    tracked: &mut HashSet<String>,
    query_id: &str,
) -> QueryPatch {
    let query = cvr
        .queries
        .get(query_id)
        .unwrap_or_else(|| panic!("Query {query_id} not found in CVR"));
    assert_not_internal(query);
    assert!(
        !tracked.contains(query_id),
        "Query {query_id} already tracked as executed or removed"
    );
    tracked.insert(query_id.to_string());

    cvr.queries.remove(query_id);
    ensure_new_version(orig_version, &mut cvr.version);
    QueryPatch {
        op: PatchOp::Del,
        id: query_id.to_string(),
        client_id: None,
    }
}

/// Port of `trackQueries`: runs `track_executed` for every executed query
/// then `track_removed` for every removed query (upstream's exact order —
/// `[executed.map(...), removed.map(...)].flat()`), and stamps every
/// resulting patch with the FINAL post-bump `cvr.version` (matching
/// upstream computing `toVersion` from `this._cvr.version` only after all
/// tracking calls have run, not each patch's version at creation time).
/// Returns `(new_version, patches)`.
pub fn track_queries(
    cvr: &mut Cvr,
    orig_version: &CvrVersion,
    tracked: &mut HashSet<String>,
    executed: &[(String, String)],
    removed: &[String],
) -> (CvrVersion, Vec<QueryPatch>) {
    let mut patches = Vec::new();
    for (id, hash) in executed {
        patches.extend(track_executed(cvr, orig_version, tracked, id, hash));
    }
    for id in removed {
        patches.push(track_removed(cvr, orig_version, tracked, id));
    }
    (cvr.version.clone(), patches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_types::{ClientQueryRecord, ExternalQueryBase};
    use std::collections::BTreeMap;

    fn v(state: &str) -> CvrVersion {
        CvrVersion {
            state_version: state.into(),
            config_version: None,
        }
    }

    fn empty_cvr(state_version: &str) -> Cvr {
        Cvr {
            id: "cg1".into(),
            version: v(state_version),
            last_active: 0.0,
            ttl_clock: crate::cvr_types::TtlClock::from_number(0.0),
            replica_version: None,
            clients: BTreeMap::new(),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        }
    }

    fn client_query(id: &str, transformation_hash: Option<&str>, gotten: bool) -> QueryRecord {
        QueryRecord::Client(ClientQueryRecord {
            base: ExternalQueryBase {
                id: id.into(),
                transformation_hash: transformation_hash.map(String::from),
                transformation_version: None,
                row_set_signature: None,
                client_state: BTreeMap::new(),
                patch_version: if gotten { Some(v("01")) } else { None },
            },
            ast: zero_cache_protocol::ast::Ast::default(),
        })
    }

    #[test]
    fn track_executed_new_query_bumps_version_and_marks_gotten() {
        let mut cvr = empty_cvr("01");
        cvr.queries
            .insert("q1".into(), client_query("q1", None, false));
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();

        let patches = track_executed(&mut cvr, &orig, &mut tracked, "q1", "hash1");

        assert_eq!(
            patches,
            vec![QueryPatch {
                op: PatchOp::Put,
                id: "q1".into(),
                client_id: None
            }]
        );
        assert_ne!(cvr.version, orig, "version should have been bumped");
        let QueryRecord::Client(q) = &cvr.queries["q1"] else {
            panic!()
        };
        assert_eq!(q.base.transformation_hash.as_deref(), Some("hash1"));
        assert!(q.base.patch_version.is_some());
    }

    #[test]
    fn track_executed_unchanged_hash_is_a_full_noop() {
        let mut cvr = empty_cvr("01");
        cvr.queries
            .insert("q1".into(), client_query("q1", Some("hash1"), true));
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();

        let patches = track_executed(&mut cvr, &orig, &mut tracked, "q1", "hash1");

        assert!(patches.is_empty());
        assert_eq!(
            cvr.version, orig,
            "version should NOT bump when hash is unchanged"
        );
    }

    #[test]
    fn track_executed_already_gotten_query_still_updates_hash_without_got_patch() {
        let mut cvr = empty_cvr("01");
        cvr.queries
            .insert("q1".into(), client_query("q1", Some("old"), true));
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();

        let patches = track_executed(&mut cvr, &orig, &mut tracked, "q1", "new");

        assert!(
            patches.is_empty(),
            "already-gotten query re-transformed should not re-emit a got patch"
        );
        assert_ne!(
            cvr.version, orig,
            "version should still bump on a real hash change"
        );
        let QueryRecord::Client(q) = &cvr.queries["q1"] else {
            panic!()
        };
        assert_eq!(q.base.transformation_hash.as_deref(), Some("new"));
    }

    #[test]
    #[should_panic(expected = "already tracked")]
    fn track_executed_same_query_twice_panics() {
        let mut cvr = empty_cvr("01");
        cvr.queries
            .insert("q1".into(), client_query("q1", None, false));
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();
        track_executed(&mut cvr, &orig, &mut tracked, "q1", "h1");
        track_executed(&mut cvr, &orig, &mut tracked, "q1", "h2");
    }

    #[test]
    fn track_removed_deletes_query_and_bumps_version() {
        let mut cvr = empty_cvr("01");
        cvr.queries
            .insert("q1".into(), client_query("q1", Some("h1"), true));
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();

        let patch = track_removed(&mut cvr, &orig, &mut tracked, "q1");

        assert_eq!(
            patch,
            QueryPatch {
                op: PatchOp::Del,
                id: "q1".into(),
                client_id: None
            }
        );
        assert!(!cvr.queries.contains_key("q1"));
        assert_ne!(cvr.version, orig);
    }

    #[test]
    #[should_panic(expected = "is internal")]
    fn track_removed_internal_query_panics() {
        let mut cvr = empty_cvr("01");
        cvr.queries.insert(
            "q1".into(),
            QueryRecord::Internal(crate::cvr_types::InternalQueryRecord {
                id: "q1".into(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
                ast: zero_cache_protocol::ast::Ast::default(),
            }),
        );
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();
        track_removed(&mut cvr, &orig, &mut tracked, "q1");
    }

    #[test]
    fn track_queries_processes_executed_then_removed_and_stamps_final_version() {
        let mut cvr = empty_cvr("01");
        cvr.queries
            .insert("q1".into(), client_query("q1", None, false));
        cvr.queries
            .insert("q2".into(), client_query("q2", Some("h2"), true));
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();

        let (new_version, patches) = track_queries(
            &mut cvr,
            &orig,
            &mut tracked,
            &[("q1".to_string(), "h1".to_string())],
            &["q2".to_string()],
        );

        assert_eq!(patches.len(), 2);
        assert_eq!(
            patches[0],
            QueryPatch {
                op: PatchOp::Put,
                id: "q1".into(),
                client_id: None
            }
        );
        assert_eq!(
            patches[1],
            QueryPatch {
                op: PatchOp::Del,
                id: "q2".into(),
                client_id: None
            }
        );
        assert_eq!(new_version, cvr.version);
        assert!(!cvr.queries.contains_key("q2"));
    }

    #[test]
    fn track_queries_with_only_removals_is_query_less_state_update() {
        let mut cvr = empty_cvr("01");
        cvr.queries
            .insert("q1".into(), client_query("q1", Some("h1"), true));
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();

        let (_, patches) = track_queries(&mut cvr, &orig, &mut tracked, &[], &["q1".to_string()]);
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0].op, PatchOp::Del);
    }
}
