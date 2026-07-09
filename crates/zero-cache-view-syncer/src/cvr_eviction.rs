//! Port of `getInactiveQueries` and `nextEvictionTime` from
//! `zero-cache/src/services/view-syncer/cvr.ts`.
//!
//! Determines which queries in a CVR are inactive for *every* client in the
//! group (and thus eligible for TTL-based eviction), and when the next
//! eviction should occur.

use crate::cvr_types::{Cvr, QueryRecord, TtlClock};
use zero_cache_zql::ttl::{clamp_ttl, Ttl};

/// An inactive query: its hash (== query id), when it was inactivated, and its
/// (clamped) TTL in milliseconds.
#[derive(Debug, Clone, PartialEq)]
pub struct InactiveQuery {
    pub hash: String,
    pub inactivated_at: TtlClock,
    pub ttl_ms: f64,
}

/// Returns the queries in `cvr` that are inactive for every client that
/// references them, sorted by soonest-to-expire first. A query is only
/// eligible once *all* referencing clients have inactivated it; among those
/// clients the one with the furthest-future expiration wins (the query stays
/// "alive" until the last client's TTL lapses). Port of `getInactiveQueries`.
pub fn get_inactive_queries(cvr: &Cvr) -> Vec<InactiveQuery> {
    let mut inactive: Vec<InactiveQuery> = Vec::new();

    for (query_id, query) in &cvr.queries {
        let client_state = match query {
            QueryRecord::Internal(_) => continue,
            QueryRecord::Client(c) => &c.base.client_state,
            QueryRecord::Custom(c) => &c.base.client_state,
        };

        let mut current: Option<InactiveQuery> = None;
        let mut any_active = false;
        for state in client_state.values() {
            let Some(inactivated_at) = state.inactivated_at else {
                any_active = true;
                break;
            };
            let (clamped_ttl, _) = clamp_ttl(&Ttl::Millis(state.ttl));

            match &mut current {
                None => {
                    current = Some(InactiveQuery {
                        hash: query_id.clone(),
                        inactivated_at,
                        ttl_ms: clamped_ttl,
                    });
                }
                Some(existing) => {
                    let (existing_ttl, _) = clamp_ttl(&Ttl::Millis(existing.ttl_ms));
                    // Keep the client whose expiration (inactivatedAt + ttl) is
                    // furthest in the future.
                    if existing_ttl + existing.inactivated_at.as_number()
                        < inactivated_at.as_number() + clamped_ttl
                    {
                        existing.ttl_ms = clamped_ttl;
                        existing.inactivated_at = inactivated_at;
                    }
                }
            }
        }

        if any_active {
            continue;
        }
        if let Some(entry) = current {
            inactive.push(entry);
        }
    }

    // Oldest (soonest-to-expire) first: by expiration time (inactivatedAt +
    // ttl) ascending, using ttl as a tiebreak-independent term matching the
    // upstream comparator exactly.
    inactive.sort_by(|a, b| {
        if a.ttl_ms == b.ttl_ms {
            a.inactivated_at
                .as_number()
                .partial_cmp(&b.inactivated_at.as_number())
                .unwrap()
        } else {
            let av = a.inactivated_at.as_number() + a.ttl_ms;
            let bv = b.inactivated_at.as_number() + b.ttl_ms;
            av.partial_cmp(&bv).unwrap()
        }
    });

    inactive
}

/// Returns the epoch-ms time of the next query eviction, or `None` if no
/// query is currently eligible. Port of `nextEvictionTime`.
pub fn next_eviction_time(cvr: &Cvr) -> Option<TtlClock> {
    let mut next: Option<f64> = None;
    for q in get_inactive_queries(cvr) {
        let expire = q.inactivated_at.as_number() + q.ttl_ms;
        if next.is_none_or(|n| expire < n) {
            next = Some(expire);
        }
    }
    next.map(TtlClock::from_number)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_types::{ClientQueryRecord, ClientQueryState, ExternalQueryBase};
    use crate::cvr_version::empty_cvr_version;
    use std::collections::BTreeMap;

    fn make_cvr() -> Cvr {
        Cvr {
            id: "cvr1".into(),
            version: empty_cvr_version(),
            last_active: 0.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: None,
            clients: BTreeMap::new(),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        }
    }

    fn client_query(client_states: Vec<(&str, ClientQueryState)>) -> QueryRecord {
        QueryRecord::Client(ClientQueryRecord {
            base: ExternalQueryBase {
                id: "q1".into(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
                client_state: client_states
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v))
                    .collect(),
                patch_version: None,
            },
            ast: zero_cache_protocol::ast::Ast::table("issues"),
        })
    }

    fn state(inactivated_at: Option<f64>, ttl: f64) -> ClientQueryState {
        ClientQueryState {
            inactivated_at: inactivated_at.map(TtlClock::from_number),
            ttl,
            deleted: inactivated_at.is_some(),
            version: empty_cvr_version(),
        }
    }

    #[test]
    fn active_client_excludes_query() {
        let mut cvr = make_cvr();
        cvr.queries.insert(
            "q1".into(),
            client_query(vec![
                ("c1", state(Some(100.0), 1000.0)),
                ("c2", state(None, 1000.0)),
            ]),
        );
        assert_eq!(get_inactive_queries(&cvr), vec![]);
    }

    #[test]
    fn all_inactive_query_is_eligible() {
        let mut cvr = make_cvr();
        cvr.queries.insert(
            "q1".into(),
            client_query(vec![("c1", state(Some(100.0), 1000.0))]),
        );
        let inactive = get_inactive_queries(&cvr);
        assert_eq!(inactive.len(), 1);
        assert_eq!(inactive[0].hash, "q1");
        assert_eq!(inactive[0].inactivated_at.as_number(), 100.0);
        assert_eq!(inactive[0].ttl_ms, 1000.0);
    }

    #[test]
    fn keeps_latest_expiring_client() {
        let mut cvr = make_cvr();
        // c1 expires at 100+1000=1100, c2 expires at 500+1000=1500 -> keep c2.
        cvr.queries.insert(
            "q1".into(),
            client_query(vec![
                ("c1", state(Some(100.0), 1000.0)),
                ("c2", state(Some(500.0), 1000.0)),
            ]),
        );
        let inactive = get_inactive_queries(&cvr);
        assert_eq!(inactive.len(), 1);
        assert_eq!(inactive[0].inactivated_at.as_number(), 500.0);
    }

    #[test]
    fn sorted_soonest_first() {
        let mut cvr = make_cvr();
        cvr.queries.insert(
            "q_late".into(),
            client_query(vec![("c1", state(Some(1000.0), 1000.0))]),
        );
        cvr.queries.insert(
            "q_early".into(),
            client_query(vec![("c1", state(Some(0.0), 1000.0))]),
        );
        let inactive = get_inactive_queries(&cvr);
        assert_eq!(inactive[0].hash, "q_early");
        assert_eq!(inactive[1].hash, "q_late");
    }

    #[test]
    fn next_eviction_time_is_soonest() {
        let mut cvr = make_cvr();
        cvr.queries.insert(
            "q_late".into(),
            client_query(vec![("c1", state(Some(1000.0), 1000.0))]),
        );
        cvr.queries.insert(
            "q_early".into(),
            client_query(vec![("c1", state(Some(0.0), 500.0))]),
        );
        assert_eq!(next_eviction_time(&cvr), Some(TtlClock::from_number(500.0)));
    }

    #[test]
    fn no_inactive_queries_yields_none() {
        let mut cvr = make_cvr();
        cvr.queries
            .insert("q1".into(), client_query(vec![("c1", state(None, 1000.0))]));
        assert_eq!(next_eviction_time(&cvr), None);
    }
}
