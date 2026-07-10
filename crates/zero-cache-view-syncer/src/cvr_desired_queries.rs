//! Port of the "which desired queries need a CVR update" decision logic in
//! `CVRConfigDrivenUpdater.putDesiredQueries`
//! (`zero-cache/src/services/view-syncer/cvr.ts`).
//!
//! This extracts the pure business rule — given a client's desired-query list
//! and the CVR's current query state, which queries are new, reactivated, or
//! need a TTL bump — from the surrounding method, which also performs
//! telemetry recording and `CVRStore` mutation (both out of scope for this
//! port). Not exported upstream / no dedicated test; tests here are written
//! from the documented semantics and the method's control flow.

use crate::cvr_types::{ClientQueryState, QueryRecord};
use zero_cache_zql::ttl::{compare_ttl, Ttl};

/// A client's request to desire a query at a given TTL.
#[derive(Debug, Clone)]
pub struct DesiredQuery {
    pub hash: String,
    pub ttl: Ttl,
}

/// Returns the subset of `desired`'s hashes that require a CVR update for
/// `client_id`, given the CVR's `existing_queries`. Port of the `needed` set
/// computation in `putDesiredQueries` (the `for (const q of queries)` loop),
/// excluding telemetry (`recordQueryForTelemetry`) and the mutation that
/// follows it.
///
/// A query needs updating when:
/// - it doesn't exist yet in the CVR (new query), or
/// - it exists but the client has no state for it, or had inactivated it
///   (reactivation), or
/// - the client already desires it, but the requested TTL is strictly
///   greater than the current one (TTL bump).
///
/// Internal queries are never included (a client cannot desire them).
pub fn compute_needed_queries(
    desired: &[DesiredQuery],
    existing_queries: &std::collections::BTreeMap<String, QueryRecord>,
    client_id: &str,
) -> std::collections::BTreeSet<String> {
    let mut needed = std::collections::BTreeSet::new();

    for q in desired {
        let Some(existing) = existing_queries.get(&q.hash) else {
            needed.insert(q.hash.clone());
            continue;
        };

        let client_state: Option<&std::collections::BTreeMap<String, ClientQueryState>> =
            match existing {
                QueryRecord::Internal(_) => continue,
                QueryRecord::Client(c) => Some(&c.base.client_state),
                QueryRecord::Custom(c) => Some(&c.base.client_state),
            };
        let client_state = client_state.unwrap();

        let old = client_state.get(client_id);
        match old {
            None => {
                needed.insert(q.hash.clone());
            }
            Some(old) if old.inactivated_at.is_some() => {
                needed.insert(q.hash.clone());
            }
            Some(old) => {
                if compare_ttl(&q.ttl, &Ttl::Millis(old.ttl)) > 0 {
                    needed.insert(q.hash.clone());
                }
            }
        }
    }

    needed
}

/// A client's request to desire a query, carrying enough to construct a fresh
/// [`QueryRecord`] if the query doesn't already exist in the CVR. Port of the
/// object shape `putDesiredQueries` accepts per query.
#[derive(Debug, Clone)]
pub struct DesiredQueryRequest {
    pub hash: String,
    pub ast: Option<zero_cache_protocol::ast::Ast>,
    pub name: Option<String>,
    pub args: Option<Vec<zero_cache_shared::bigint_json::JsonValue>>,
    pub ttl: Option<Ttl>,
}

/// Applies a client's desired-query list to `cvr`, mutating its `clients` and
/// `queries` maps in place and returning the patches to send the client. Port
/// of the mutation half of `CVRConfigDrivenUpdater.putDesiredQueries` (the
/// portion after the `needed` set is computed) — `CVRStore` persistence
/// (`putQuery`/`putDesiredQuery`) is out of scope, matching the boundary used
/// throughout this port: this is the CVR-state transition, not its storage.
///
/// `orig_version` is the CVR's version at the start of the update session
/// (before any mutations in this call), used by [`crate::cvr_updater::ensure_new_version`]
/// to decide whether a version bump is needed.
///
/// Panics if a query id collides with a reserved internal query id (matching
/// `assertNotInternal`).
pub fn put_desired_queries(
    cvr: &mut crate::cvr_types::Cvr,
    orig_version: &crate::cvr_version::CvrVersion,
    client_id: &str,
    queries: &[DesiredQueryRequest],
) -> Vec<crate::client_patch::PatchToVersion> {
    use crate::client_patch::{Patch, PatchToVersion};
    use crate::cvr_ref_counts::new_query_record;
    use crate::cvr_types::{ClientQueryState, ClientRecord, PatchOp, QueryPatch};
    use crate::cvr_updater::ensure_new_version;
    use zero_cache_zql::ttl::{clamp_ttl, DEFAULT_TTL_MS};

    cvr.clients
        .entry(client_id.to_string())
        .or_insert_with(|| ClientRecord {
            id: client_id.to_string(),
            desired_query_ids: vec![],
        });

    let desired: Vec<DesiredQuery> = queries
        .iter()
        .map(|q| DesiredQuery {
            hash: q.hash.clone(),
            ttl: q.ttl.clone().unwrap_or(Ttl::Millis(DEFAULT_TTL_MS)),
        })
        .collect();
    let needed = compute_needed_queries(&desired, &cvr.queries, client_id);
    if needed.is_empty() {
        return vec![];
    }

    let new_version = ensure_new_version(orig_version, &mut cvr.version);

    let client = cvr.clients.get_mut(client_id).unwrap();
    let mut ids: std::collections::BTreeSet<String> =
        client.desired_query_ids.iter().cloned().collect();
    ids.extend(needed.iter().cloned());
    // BTreeSet iteration is already lexicographically sorted, matching
    // `toSorted(union(current, needed), stringCompare)`.
    client.desired_query_ids = ids.into_iter().collect();

    let mut patches = Vec::with_capacity(needed.len());
    for id in &needed {
        let q = queries
            .iter()
            .find(|q| &q.hash == id)
            .expect("needed id must be in queries");
        let (clamped_ttl, _) = clamp_ttl(&q.ttl.clone().unwrap_or(Ttl::Millis(DEFAULT_TTL_MS)));

        let mut query = cvr.queries.get(id).cloned().unwrap_or_else(|| {
            new_query_record(id, q.ast.as_ref(), q.name.as_deref(), q.args.as_deref())
        });

        let client_state = ClientQueryState {
            inactivated_at: None,
            ttl: clamped_ttl,
            deleted: false,
            version: new_version.clone(),
        };
        match &mut query {
            QueryRecord::Internal(_) => panic!("Query ID {id} is reserved for internal use"),
            QueryRecord::Client(c) => {
                c.base
                    .client_state
                    .insert(client_id.to_string(), client_state);
            }
            QueryRecord::Custom(c) => {
                c.base
                    .client_state
                    .insert(client_id.to_string(), client_state);
            }
        }
        cvr.queries.insert(id.clone(), query);

        patches.push(PatchToVersion {
            patch: Patch::Config(QueryPatch {
                op: PatchOp::Put,
                id: id.clone(),
                client_id: Some(client_id.to_string()),
            }),
            to_version: new_version.clone(),
        });
    }
    patches
}

/// Removes queries from a client's desired set, either marking them inactive
/// (if `inactivated_at` is `Some`, e.g. for TTL-based eviction) or fully
/// clearing their client state (if `None`, e.g. an explicit client
/// unsubscribe). Port of the private `#deleteQueries`, which backs
/// `markDesiredQueriesAsInactive`, `deleteDesiredQueries`, and
/// `clearDesiredQueries`.
///
/// `orig_version` is the CVR's version at the start of the update session (see
/// [`put_desired_queries`]). Panics if a query id is reserved for internal use,
/// or if a query is already inactivated and `inactivated_at` is `Some`
/// (matching upstream's `assert`s).
pub fn delete_queries(
    cvr: &mut crate::cvr_types::Cvr,
    orig_version: &crate::cvr_version::CvrVersion,
    client_id: &str,
    query_hashes: &[String],
    inactivated_at: Option<crate::cvr_types::TtlClock>,
) -> Vec<crate::client_patch::PatchToVersion> {
    use crate::client_patch::{Patch, PatchToVersion};
    use crate::cvr_types::{ClientRecord, PatchOp, QueryPatch};
    use crate::cvr_updater::ensure_new_version;
    use zero_cache_zql::ttl::{clamp_ttl, Ttl};

    cvr.clients
        .entry(client_id.to_string())
        .or_insert_with(|| ClientRecord {
            id: client_id.to_string(),
            desired_query_ids: vec![],
        });
    let client = &cvr.clients[client_id];
    let current: std::collections::BTreeSet<String> =
        client.desired_query_ids.iter().cloned().collect();
    let unwanted: std::collections::BTreeSet<String> = query_hashes.iter().cloned().collect();
    let remove: Vec<String> = current.intersection(&unwanted).cloned().collect();
    if remove.is_empty() {
        return vec![];
    }

    let new_version = ensure_new_version(orig_version, &mut cvr.version);
    let remaining: Vec<String> = current.difference(&unwanted).cloned().collect();
    cvr.clients.get_mut(client_id).unwrap().desired_query_ids = remaining;

    let mut patches = Vec::with_capacity(remove.len());
    for id in &remove {
        let Some(query) = cvr.queries.get_mut(id) else {
            continue; // Already removed; matches the `if (!query) continue` guard.
        };

        let client_state = match query {
            QueryRecord::Internal(_) => panic!("Query ID {id} is reserved for internal use"),
            QueryRecord::Client(c) => &mut c.base.client_state,
            QueryRecord::Custom(c) => &mut c.base.client_state,
        };

        match inactivated_at {
            None => {
                client_state.remove(client_id);
            }
            Some(inactivated_at) => {
                if let Some(state) = client_state.get_mut(client_id) {
                    assert!(
                        state.inactivated_at.is_none(),
                        "Query {id} is already inactivated"
                    );
                    let (ttl, _) = clamp_ttl(&Ttl::Millis(state.ttl));
                    *state = crate::cvr_types::ClientQueryState {
                        inactivated_at: Some(inactivated_at),
                        ttl,
                        deleted: true,
                        version: new_version.clone(),
                    };
                }
                // If there was no client state, nothing to update (matches
                // upstream: "client state can be missing if the query never
                // transformed").
            }
        }

        patches.push(PatchToVersion {
            patch: Patch::Config(QueryPatch {
                op: PatchOp::Del,
                id: id.clone(),
                client_id: Some(client_id.to_string()),
            }),
            to_version: new_version.clone(),
        });
    }
    patches
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_types::{ClientQueryRecord, ExternalQueryBase};
    use crate::cvr_version::empty_cvr_version;
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::Ast;

    fn client_query(client_state: BTreeMap<String, ClientQueryState>) -> QueryRecord {
        QueryRecord::Client(ClientQueryRecord {
            base: ExternalQueryBase {
                id: "q1".into(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
                client_state,
                patch_version: None,
            },
            ast: Ast::table("issues"),
        })
    }

    fn state(inactivated: bool, ttl: f64) -> ClientQueryState {
        ClientQueryState {
            inactivated_at: if inactivated {
                Some(crate::cvr_types::TtlClock::from_number(1.0))
            } else {
                None
            },
            ttl,
            deleted: inactivated,
            version: empty_cvr_version(),
        }
    }

    #[test]
    fn new_query_is_needed() {
        let existing = BTreeMap::new();
        let desired = vec![DesiredQuery {
            hash: "q1".into(),
            ttl: Ttl::Millis(1000.0),
        }];
        let needed = compute_needed_queries(&desired, &existing, "c1");
        assert!(needed.contains("q1"));
    }

    #[test]
    fn internal_query_never_needed() {
        let mut existing = BTreeMap::new();
        existing.insert(
            "q1".to_string(),
            QueryRecord::Internal(crate::cvr_types::InternalQueryRecord {
                id: "q1".into(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
                ast: Ast::table("issues"),
            }),
        );
        let desired = vec![DesiredQuery {
            hash: "q1".into(),
            ttl: Ttl::Millis(1000.0),
        }];
        assert!(compute_needed_queries(&desired, &existing, "c1").is_empty());
    }

    #[test]
    fn no_client_state_is_reactivation() {
        let mut existing = BTreeMap::new();
        existing.insert("q1".to_string(), client_query(BTreeMap::new()));
        let desired = vec![DesiredQuery {
            hash: "q1".into(),
            ttl: Ttl::Millis(1000.0),
        }];
        assert!(compute_needed_queries(&desired, &existing, "c1").contains("q1"));
    }

    #[test]
    fn inactivated_client_state_is_reactivation() {
        let mut cs = BTreeMap::new();
        cs.insert("c1".to_string(), state(true, 1000.0));
        let mut existing = BTreeMap::new();
        existing.insert("q1".to_string(), client_query(cs));
        let desired = vec![DesiredQuery {
            hash: "q1".into(),
            ttl: Ttl::Millis(1000.0),
        }];
        assert!(compute_needed_queries(&desired, &existing, "c1").contains("q1"));
    }

    #[test]
    fn active_with_same_or_lower_ttl_not_needed() {
        let mut cs = BTreeMap::new();
        cs.insert("c1".to_string(), state(false, 1000.0));
        let mut existing = BTreeMap::new();
        existing.insert("q1".to_string(), client_query(cs));

        let same_ttl = vec![DesiredQuery {
            hash: "q1".into(),
            ttl: Ttl::Millis(1000.0),
        }];
        assert!(compute_needed_queries(&same_ttl, &existing, "c1").is_empty());

        let lower_ttl = vec![DesiredQuery {
            hash: "q1".into(),
            ttl: Ttl::Millis(500.0),
        }];
        assert!(compute_needed_queries(&lower_ttl, &existing, "c1").is_empty());
    }

    #[test]
    fn ttl_bump_is_needed() {
        let mut cs = BTreeMap::new();
        cs.insert("c1".to_string(), state(false, 1000.0));
        let mut existing = BTreeMap::new();
        existing.insert("q1".to_string(), client_query(cs));

        let higher_ttl = vec![DesiredQuery {
            hash: "q1".into(),
            ttl: Ttl::Millis(2000.0),
        }];
        assert!(compute_needed_queries(&higher_ttl, &existing, "c1").contains("q1"));
    }

    #[test]
    fn different_client_sees_no_state_as_reactivation() {
        let mut cs = BTreeMap::new();
        cs.insert("c1".to_string(), state(false, 1000.0));
        let mut existing = BTreeMap::new();
        existing.insert("q1".to_string(), client_query(cs));

        // c2 has never desired q1: treated as new/reactivation for c2.
        let desired = vec![DesiredQuery {
            hash: "q1".into(),
            ttl: Ttl::Millis(500.0),
        }];
        assert!(compute_needed_queries(&desired, &existing, "c2").contains("q1"));
    }

    fn empty_cvr() -> crate::cvr_types::Cvr {
        crate::cvr_types::Cvr {
            id: "cvr1".into(),
            version: empty_cvr_version(),
            last_active: 0.0,
            ttl_clock: crate::cvr_types::TtlClock::from_number(0.0),
            replica_version: None,
            clients: BTreeMap::new(),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        }
    }

    #[test]
    fn put_desired_queries_new_query_creates_client_and_query() {
        let mut cvr = empty_cvr();
        let orig = cvr.version.clone();
        let req = DesiredQueryRequest {
            hash: "q1".into(),
            ast: Some(zero_cache_protocol::ast::Ast::table("issues")),
            name: None,
            args: None,
            ttl: Some(Ttl::Millis(1000.0)),
        };
        let patches = put_desired_queries(&mut cvr, &orig, "c1", &[req]);

        assert_eq!(patches.len(), 1);
        assert!(cvr.clients.contains_key("c1"));
        assert_eq!(cvr.clients["c1"].desired_query_ids, vec!["q1".to_string()]);
        assert!(cvr.queries.contains_key("q1"));
        // Version was bumped since orig == cvr.version before the call.
        assert_ne!(cvr.version, orig);

        match &cvr.queries["q1"] {
            QueryRecord::Client(c) => {
                assert!(c.base.client_state.contains_key("c1"));
                assert_eq!(c.base.client_state["c1"].inactivated_at, None);
                assert!(!c.base.client_state["c1"].deleted);
            }
            _ => panic!("expected a client query record"),
        }
    }

    #[test]
    fn put_desired_queries_noop_when_nothing_needed() {
        let mut cvr = empty_cvr();
        let orig = cvr.version.clone();
        let req = DesiredQueryRequest {
            hash: "q1".into(),
            ast: Some(zero_cache_protocol::ast::Ast::table("issues")),
            name: None,
            args: None,
            ttl: Some(Ttl::Millis(1000.0)),
        };
        put_desired_queries(&mut cvr, &orig, "c1", std::slice::from_ref(&req));
        let version_after_first = cvr.version.clone();

        // Same request again: already active with same TTL -> nothing needed,
        // no version bump, no new patch.
        let orig2 = cvr.version.clone();
        let patches = put_desired_queries(&mut cvr, &orig2, "c1", &[req]);
        assert!(patches.is_empty());
        assert_eq!(cvr.version, version_after_first);
    }

    #[test]
    fn put_desired_queries_ttl_bump_updates_existing_query() {
        let mut cvr = empty_cvr();
        let orig = cvr.version.clone();
        let low_ttl = DesiredQueryRequest {
            hash: "q1".into(),
            ast: Some(zero_cache_protocol::ast::Ast::table("issues")),
            name: None,
            args: None,
            ttl: Some(Ttl::Millis(1000.0)),
        };
        put_desired_queries(&mut cvr, &orig, "c1", &[low_ttl]);

        let orig2 = cvr.version.clone();
        let high_ttl = DesiredQueryRequest {
            hash: "q1".into(),
            ast: Some(zero_cache_protocol::ast::Ast::table("issues")),
            name: None,
            args: None,
            ttl: Some(Ttl::Millis(5000.0)),
        };
        let patches = put_desired_queries(&mut cvr, &orig2, "c1", &[high_ttl]);
        assert_eq!(patches.len(), 1);
        match &cvr.queries["q1"] {
            QueryRecord::Client(c) => assert_eq!(c.base.client_state["c1"].ttl, 5000.0),
            _ => panic!("expected a client query record"),
        }
    }

    fn desire_q1(cvr: &mut crate::cvr_types::Cvr, client_id: &str, ttl_ms: f64) {
        let orig = cvr.version.clone();
        let req = DesiredQueryRequest {
            hash: "q1".into(),
            ast: Some(zero_cache_protocol::ast::Ast::table("issues")),
            name: None,
            args: None,
            ttl: Some(Ttl::Millis(ttl_ms)),
        };
        put_desired_queries(cvr, &orig, client_id, &[req]);
    }

    #[test]
    fn delete_queries_none_removes_client_state_entirely() {
        let mut cvr = empty_cvr();
        desire_q1(&mut cvr, "c1", 1000.0);

        let orig = cvr.version.clone();
        let patches = delete_queries(&mut cvr, &orig, "c1", &["q1".to_string()], None);
        assert_eq!(patches.len(), 1);
        assert!(cvr.clients["c1"].desired_query_ids.is_empty());
        match &cvr.queries["q1"] {
            QueryRecord::Client(c) => assert!(!c.base.client_state.contains_key("c1")),
            _ => panic!("expected a client query record"),
        }
    }

    #[test]
    fn delete_queries_some_marks_inactive_with_clamped_ttl() {
        let mut cvr = empty_cvr();
        desire_q1(&mut cvr, "c1", 1000.0);

        let orig = cvr.version.clone();
        let inactivated_at = crate::cvr_types::TtlClock::from_number(42.0);
        let patches = delete_queries(
            &mut cvr,
            &orig,
            "c1",
            &["q1".to_string()],
            Some(inactivated_at),
        );
        assert_eq!(patches.len(), 1);
        // desiredQueryIDs no longer lists it, but client state is retained (inactivated).
        assert!(cvr.clients["c1"].desired_query_ids.is_empty());
        match &cvr.queries["q1"] {
            QueryRecord::Client(c) => {
                let state = &c.base.client_state["c1"];
                assert_eq!(state.inactivated_at, Some(inactivated_at));
                assert_eq!(state.ttl, 1000.0);
                assert!(state.deleted);
            }
            _ => panic!("expected a client query record"),
        }
    }

    #[test]
    fn delete_queries_noop_when_not_desired() {
        let mut cvr = empty_cvr();
        let orig = cvr.version.clone();
        let patches = delete_queries(&mut cvr, &orig, "c1", &["nonexistent".to_string()], None);
        assert!(patches.is_empty());
        assert_eq!(cvr.version, orig);
    }

    #[test]
    #[should_panic(expected = "already inactivated")]
    fn delete_queries_panics_on_double_inactivation() {
        let mut cvr = empty_cvr();
        desire_q1(&mut cvr, "c1", 1000.0);

        let inactivated_at = crate::cvr_types::TtlClock::from_number(42.0);
        let orig = cvr.version.clone();
        delete_queries(
            &mut cvr,
            &orig,
            "c1",
            &["q1".to_string()],
            Some(inactivated_at),
        );

        // Re-insert into desiredQueryIDs to force delete_queries to consider
        // it again (in practice this path is reached via markDesiredQueriesAsInactive
        // being called again for a query already inactivated for this client).
        cvr.clients
            .get_mut("c1")
            .unwrap()
            .desired_query_ids
            .push("q1".to_string());
        let orig2 = cvr.version.clone();
        delete_queries(
            &mut cvr,
            &orig2,
            "c1",
            &["q1".to_string()],
            Some(inactivated_at),
        );
    }
}
