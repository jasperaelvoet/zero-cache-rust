//! Cross-client query ref-counting for one client group — the piece that makes
//! a *shared* pipeline correct when a group has more than one connection.
//!
//! Upstream's `ViewSyncerService` keeps ONE `PipelineDriver` per client group
//! and ref-counts each query across the group's clients: a query is hydrated
//! (`pipeline.addQuery`) when the FIRST client desires it and torn down
//! (`pipeline.removeQuery`) when the LAST client drops it
//! (`mono-src/packages/zero-cache/src/services/view-syncer/view-syncer.ts`,
//! where `#pipelines` is shared and desires are tracked per client). Today the
//! Rust port tracks desired queries per *connection*
//! (`DesiredQueriesHandler.tracked` / `desired_puts`,
//! `crates/zero-cache-server/src/live_connection.rs`), so a shared driver would
//! double-add a query desired by two connections. This structure is the group's
//! authoritative desire map; the group service consults it to decide exactly
//! when to add/remove a query on the shared pipeline.
//!
//! Dead code until the bootstrap wiring (redesign §6 B4) routes a group's
//! connections through the shared driver; kept standalone and unit-tested so the
//! ref-count invariants are pinned before that wiring lands.

use std::collections::{BTreeMap, BTreeSet};

/// Whether a desire change crossed a hydration boundary — i.e. whether the
/// caller must now add or remove the query on the shared pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTransition {
    /// The query gained its first desirer: hydrate it on the shared pipeline.
    Hydrate,
    /// The query lost its last desirer: remove it from the shared pipeline.
    Remove,
    /// The query's desirer set changed but it is still (or still not) desired
    /// by someone else: the pipeline is untouched.
    Unchanged,
}

/// The group's `query_hash -> {client_id}` desire map. One per client group,
/// owned by the group service.
#[derive(Default)]
pub struct GroupQuerySet {
    desirers: BTreeMap<String, BTreeSet<String>>,
}

impl GroupQuerySet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that `client_id` desires `query_hash`. Returns
    /// [`QueryTransition::Hydrate`] only when this is the query's first
    /// desirer across the whole group (so the caller hydrates it once);
    /// otherwise [`QueryTransition::Unchanged`]. Idempotent: a client
    /// re-desiring a query it already desires never re-hydrates.
    pub fn add_desire(&mut self, client_id: &str, query_hash: &str) -> QueryTransition {
        let desirers = self.desirers.entry(query_hash.to_string()).or_default();
        let was_empty = desirers.is_empty();
        desirers.insert(client_id.to_string());
        if was_empty {
            QueryTransition::Hydrate
        } else {
            QueryTransition::Unchanged
        }
    }

    /// Records that `client_id` no longer desires `query_hash`. Returns
    /// [`QueryTransition::Remove`] only when the last desirer dropped it (so the
    /// caller removes it from the pipeline once); otherwise
    /// [`QueryTransition::Unchanged`]. A no-op for a desire that was not held.
    pub fn remove_desire(&mut self, client_id: &str, query_hash: &str) -> QueryTransition {
        let Some(desirers) = self.desirers.get_mut(query_hash) else {
            return QueryTransition::Unchanged;
        };
        if !desirers.remove(client_id) {
            return QueryTransition::Unchanged;
        }
        if desirers.is_empty() {
            self.desirers.remove(query_hash);
            QueryTransition::Remove
        } else {
            QueryTransition::Unchanged
        }
    }

    /// Drops every desire held by `client_id` (a disconnect). Returns the query
    /// hashes that lost their last desirer and so must be removed from the
    /// shared pipeline, in deterministic order.
    pub fn remove_client(&mut self, client_id: &str) -> Vec<String> {
        let mut removed = Vec::new();
        // Collect first to avoid mutating while iterating.
        let hashes: Vec<String> = self.desirers.keys().cloned().collect();
        for hash in hashes {
            if self.remove_desire(client_id, &hash) == QueryTransition::Remove {
                removed.push(hash);
            }
        }
        removed
    }

    /// Forgets every desirer of `query_hash`, returning its previous desirers.
    /// Used by the group loop's transformation-hash guard to fully drop a query
    /// so it re-hydrates from scratch when a connection presents a divergent
    /// transformed AST for an already-active hash.
    pub fn clear_query(&mut self, query_hash: &str) -> BTreeSet<String> {
        self.desirers.remove(query_hash).unwrap_or_default()
    }

    /// Whether any client in the group currently desires `query_hash` (i.e. it
    /// has a live pipeline).
    pub fn is_active(&self, query_hash: &str) -> bool {
        self.desirers.contains_key(query_hash)
    }

    /// Whether `client_id` currently desires `query_hash`. Used to filter a
    /// group-wide advance (which spans every connection's queries) down to the
    /// queries THIS connection asked for.
    pub fn client_desires(&self, client_id: &str, query_hash: &str) -> bool {
        self.desirers
            .get(query_hash)
            .is_some_and(|clients| clients.contains(client_id))
    }

    /// The set of queries with a live pipeline, in deterministic order.
    pub fn active_queries(&self) -> Vec<String> {
        self.desirers.keys().cloned().collect()
    }

    /// Number of queries with a live pipeline.
    pub fn active_len(&self) -> usize {
        self.desirers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_desirer_hydrates_and_last_removes() {
        let mut set = GroupQuerySet::new();
        assert_eq!(set.add_desire("c1", "q"), QueryTransition::Hydrate);
        assert!(set.is_active("q"));
        assert_eq!(set.remove_desire("c1", "q"), QueryTransition::Remove);
        assert!(!set.is_active("q"));
    }

    #[test]
    fn second_desirer_does_not_rehydrate_and_first_drop_does_not_remove() {
        let mut set = GroupQuerySet::new();
        assert_eq!(set.add_desire("c1", "q"), QueryTransition::Hydrate);
        // A second connection desiring the same query must NOT re-add it.
        assert_eq!(set.add_desire("c2", "q"), QueryTransition::Unchanged);
        // One client dropping it must NOT remove it while another still wants it.
        assert_eq!(set.remove_desire("c1", "q"), QueryTransition::Unchanged);
        assert!(set.is_active("q"));
        // The last client dropping it removes it.
        assert_eq!(set.remove_desire("c2", "q"), QueryTransition::Remove);
        assert!(!set.is_active("q"));
    }

    #[test]
    fn re_desiring_is_idempotent() {
        let mut set = GroupQuerySet::new();
        assert_eq!(set.add_desire("c1", "q"), QueryTransition::Hydrate);
        assert_eq!(set.add_desire("c1", "q"), QueryTransition::Unchanged);
        // Still a single desirer, so one drop removes it.
        assert_eq!(set.remove_desire("c1", "q"), QueryTransition::Remove);
    }

    #[test]
    fn removing_an_unheld_desire_is_a_noop() {
        let mut set = GroupQuerySet::new();
        assert_eq!(set.remove_desire("c1", "q"), QueryTransition::Unchanged);
        set.add_desire("c1", "q");
        // A different client that never desired it dropping it is a no-op.
        assert_eq!(set.remove_desire("c2", "q"), QueryTransition::Unchanged);
        assert!(set.is_active("q"));
    }

    #[test]
    fn remove_client_returns_only_last_desired_queries() {
        let mut set = GroupQuerySet::new();
        set.add_desire("c1", "shared");
        set.add_desire("c2", "shared");
        set.add_desire("c1", "solo");
        // c1 disconnects: "solo" loses its last desirer, "shared" is still held
        // by c2.
        let removed = set.remove_client("c1");
        assert_eq!(removed, vec!["solo".to_string()]);
        assert!(set.is_active("shared"));
        assert!(!set.is_active("solo"));
        assert_eq!(set.active_queries(), vec!["shared".to_string()]);
        assert_eq!(set.active_len(), 1);
    }
}
