//! A CVR-backed desired-queries handler — the stateful core a served
//! connection's [`crate::connection_dispatch::ConnectionAction`] drives.
//!
//! When a client sends `initConnection` or `changeDesiredQueries`, the server
//! must fold the message's `UpQueriesPatch` into the client group's CVR (Client
//! View Record) — registering newly-desired queries, bumping TTLs, and removing
//! unwanted ones — and emit the resulting config patches back to the client.
//! This wraps the already-ported CVR state transitions
//! ([`crate::cvr_desired_queries::put_desired_queries`] / `delete_queries`)
//! behind a per-connection handler that takes a decoded protocol patch and
//! returns the patches to send downstream.
//!
//! Scope: the CVR *state transition* plus conversion of a loaded durable CVR
//! back into active wire-level desired-query PUTs. PostgreSQL persistence and
//! row hydration are owned by the surrounding view-syncer/server layers. This
//! turns a wire `changeDesiredQueries` into concrete CVR mutations + client
//! patches while preserving reconnect state.

use std::collections::BTreeMap;

use zero_cache_protocol::queries_patch::{UpQueriesPatch, UpQueriesPatchOp, UpQueriesPutOp};
use zero_cache_zql::ttl::Ttl;

use crate::client_patch::PatchToVersion;
use crate::cvr_desired_queries::{delete_queries, put_desired_queries, DesiredQueryRequest};
use crate::cvr_types::{Cvr, TtlClock};
use crate::cvr_version::{empty_cvr_version, CvrVersion};

/// A per-connection handler owning the client group's CVR state.
pub struct CvrQueryHandler {
    pub cvr: Cvr,
    client_id: String,
}

impl CvrQueryHandler {
    /// Creates a handler with a fresh, empty CVR for `client_group_id` at the
    /// given `replica_version` (the version the replica was synced to). One
    /// client (`client_id`) is implied; further clients are created lazily by
    /// the CVR transitions.
    pub fn new(client_group_id: &str, client_id: &str, replica_version: Option<String>) -> Self {
        let cvr = Cvr {
            id: client_group_id.to_string(),
            version: empty_cvr_version(),
            last_active: 0.0,
            ttl_clock: TtlClock(0.0),
            replica_version,
            clients: BTreeMap::new(),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        };
        CvrQueryHandler {
            cvr,
            client_id: client_id.to_string(),
        }
    }

    /// Wraps an already-loaded durable CVR for a live connection.  The
    /// persisted CVR owns the client group's version, client schema, query
    /// records, and desired-query state; creating a fresh handler here would
    /// discard all of that state on reconnect.
    pub fn from_cvr(cvr: Cvr, client_group_id: &str, client_id: &str) -> Self {
        assert_eq!(
            cvr.id, client_group_id,
            "loaded CVR identity must match the connection client group"
        );
        CvrQueryHandler {
            cvr,
            client_id: client_id.to_string(),
        }
    }

    /// Reconstructs the wire-level PUT operations for this client's active
    /// desired queries.  This is used only to rebuild the live hydration index
    /// after loading a durable CVR; the server does not send these PUTs back as
    /// a new client request.  Query records without active state for this
    /// client (including internal records and expired/inactivated desires) are
    /// intentionally skipped.
    pub fn desired_puts_for_client(&self) -> Vec<UpQueriesPutOp> {
        let Some(client) = self.cvr.clients.get(&self.client_id) else {
            return Vec::new();
        };

        client
            .desired_query_ids
            .iter()
            .filter_map(|hash| {
                let query = self.cvr.queries.get(hash)?;
                match query {
                    crate::cvr_types::QueryRecord::Client(query) => {
                        let state = query.base.client_state.get(&self.client_id)?;
                        (state.inactivated_at.is_none() && !state.deleted).then(|| UpQueriesPutOp {
                            hash: hash.clone(),
                            ttl: Some(state.ttl),
                            ast: Some(query.ast.clone()),
                            name: None,
                            args: None,
                        })
                    }
                    crate::cvr_types::QueryRecord::Custom(query) => {
                        let state = query.base.client_state.get(&self.client_id)?;
                        (state.inactivated_at.is_none() && !state.deleted).then(|| UpQueriesPutOp {
                            hash: hash.clone(),
                            ttl: Some(state.ttl),
                            ast: None,
                            name: Some(query.name.clone()),
                            args: Some(query.args.clone()),
                        })
                    }
                    crate::cvr_types::QueryRecord::Internal(_) => None,
                }
            })
            .collect()
    }

    /// The CVR's current version.
    pub fn version(&self) -> &CvrVersion {
        &self.cvr.version
    }

    /// This connection's own client id (from the connect URL). A pushResponse
    /// must only carry mutations for THIS client — upstream's pusher fans each
    /// mutation result out to its own client's connection; a client rejects any
    /// result whose clientID isn't its own ("received mutation for the wrong
    /// client"), which is FATAL and closes the socket.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Repoints this handler at another client in the SAME client group without
    /// touching the (group-scoped) CVR. The per-group processor loop
    /// (`zero-cache-server`'s `group_processor`) owns ONE handler for the whole
    /// group and switches its active-client perspective per desired-queries
    /// request, so a client's desired PUT/DEL is attributed to the right client
    /// — the in-place, clone-free analogue of rebuilding via [`Self::from_cvr`].
    pub fn set_client_id(&mut self, client_id: &str) {
        self.client_id = client_id.to_string();
    }

    /// Seeds the CVR version to the client's connect cookie (`cookieToVersion`),
    /// so a reconnecting client's first poke bases at exactly the cookie it
    /// holds (upstream `ClientHandler.#baseVersion`) — avoiding "unexpected base
    /// cookie during sync". Subsequent hydration bumps advance beyond it.
    pub fn seed_version(&mut self, version: CvrVersion) {
        self.cvr.version = version;
    }

    /// Applies a decoded `UpQueriesPatch` (from `initConnection` /
    /// `changeDesiredQueries`) to the CVR, returning the config patches to send
    /// the client. Puts register/reactivate/TTL-bump queries; dels remove named
    /// queries; a `clear` removes every query the client currently desires.
    ///
    /// The original CVR version is snapshotted before any mutation so the
    /// underlying transitions bump the version at most once across the whole
    /// patch (matching upstream's single update session).
    pub fn apply_desired_queries_patch(&mut self, patch: &UpQueriesPatch) -> Vec<PatchToVersion> {
        let orig_version = self.cvr.version.clone();

        let mut puts: Vec<DesiredQueryRequest> = Vec::new();
        let mut dels: Vec<String> = Vec::new();
        let mut clear = false;

        for op in patch {
            match op {
                UpQueriesPatchOp::Put(p) => puts.push(DesiredQueryRequest {
                    hash: p.hash.clone(),
                    ast: p.ast.clone(),
                    name: p.name.clone(),
                    args: p.args.clone(),
                    ttl: p.ttl.map(Ttl::Millis),
                }),
                UpQueriesPatchOp::Del(d) => dels.push(d.hash.clone()),
                UpQueriesPatchOp::Clear(_) => clear = true,
            }
        }

        // A `clear` removes everything the client currently desires (evaluated
        // against the pre-mutation state), in addition to any explicit dels.
        if clear {
            if let Some(client) = self.cvr.clients.get(&self.client_id) {
                dels.extend(client.desired_query_ids.iter().cloned());
            }
        }

        let mut patches = Vec::new();
        if !puts.is_empty() {
            patches.extend(put_desired_queries(
                &mut self.cvr,
                &orig_version,
                &self.client_id,
                &puts,
            ));
        }
        if !dels.is_empty() {
            // Hard removal (inactivated_at = None) — the client explicitly
            // dropped these queries.
            patches.extend(delete_queries(
                &mut self.cvr,
                &orig_version,
                &self.client_id,
                &dels,
                None,
            ));
        }
        patches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::queries_patch::{QueriesClearOp, QueriesDelOp, UpQueriesPutOp};

    // A custom query needs both name and args (a client/AST query would set
    // `ast` instead and leave name/args unset).
    fn put(hash: &str, ttl: Option<f64>) -> UpQueriesPatchOp {
        UpQueriesPatchOp::Put(UpQueriesPutOp {
            hash: hash.into(),
            ttl,
            ast: None,
            name: Some("q".into()),
            args: Some(vec![]),
        })
    }

    #[test]
    fn putting_a_new_query_registers_it_and_bumps_the_version() {
        let mut h = CvrQueryHandler::new("group1", "client1", Some("01".into()));
        assert_eq!(h.version(), &empty_cvr_version());

        let patches = h.apply_desired_queries_patch(&vec![put("q1", Some(1000.0))]);
        assert!(
            !patches.is_empty(),
            "a new query produces at least one patch"
        );
        assert_ne!(h.version(), &empty_cvr_version(), "version bumped");
        // The query is now in the CVR and desired by the client.
        assert!(h.cvr.queries.contains_key("q1"));
        assert_eq!(
            h.cvr.clients["client1"].desired_query_ids,
            vec!["q1".to_string()]
        );
    }

    #[test]
    fn re_putting_the_same_query_at_same_ttl_is_a_no_op() {
        let mut h = CvrQueryHandler::new("g", "c", None);
        h.apply_desired_queries_patch(&vec![put("q1", Some(1000.0))]);
        let v1 = h.version().clone();
        let patches = h.apply_desired_queries_patch(&vec![put("q1", Some(1000.0))]);
        assert!(patches.is_empty(), "no change -> no patches");
        assert_eq!(h.version(), &v1, "version unchanged");
    }

    #[test]
    fn deleting_a_desired_query_removes_it() {
        let mut h = CvrQueryHandler::new("g", "c", None);
        h.apply_desired_queries_patch(&vec![put("q1", Some(1000.0)), put("q2", Some(1000.0))]);
        assert_eq!(h.cvr.clients["c"].desired_query_ids.len(), 2);

        let patches = h.apply_desired_queries_patch(&vec![UpQueriesPatchOp::Del(QueriesDelOp {
            hash: "q1".into(),
        })]);
        assert!(!patches.is_empty());
        assert_eq!(h.cvr.clients["c"].desired_query_ids, vec!["q2".to_string()]);
    }

    #[test]
    fn clear_removes_every_currently_desired_query() {
        let mut h = CvrQueryHandler::new("g", "c", None);
        h.apply_desired_queries_patch(&vec![put("q1", Some(1000.0)), put("q2", Some(1000.0))]);
        assert_eq!(h.cvr.clients["c"].desired_query_ids.len(), 2);

        h.apply_desired_queries_patch(&vec![UpQueriesPatchOp::Clear(QueriesClearOp)]);
        assert!(
            h.cvr.clients["c"].desired_query_ids.is_empty(),
            "clear dropped all desired queries"
        );
    }

    #[test]
    fn a_ttl_bump_updates_an_existing_query() {
        let mut h = CvrQueryHandler::new("g", "c", None);
        h.apply_desired_queries_patch(&vec![put("q1", Some(1000.0))]);
        let v1 = h.version().clone();
        // A strictly greater TTL is a needed update.
        let patches = h.apply_desired_queries_patch(&vec![put("q1", Some(5000.0))]);
        assert!(!patches.is_empty(), "TTL bump produces patches");
        assert_ne!(h.version(), &v1, "version bumped on TTL change");
    }

    #[test]
    fn loading_a_cvr_reconstructs_active_client_puts() {
        let mut original = CvrQueryHandler::new("g", "c", None);
        original.apply_desired_queries_patch(&vec![UpQueriesPatchOp::Put(UpQueriesPutOp {
            hash: "q1".into(),
            ttl: Some(2_000.0),
            ast: Some(zero_cache_protocol::ast::Ast::table("issues")),
            name: None,
            args: None,
        })]);

        let loaded = CvrQueryHandler::from_cvr(original.cvr.clone(), "g", "c");
        let puts = loaded.desired_puts_for_client();
        assert_eq!(puts.len(), 1);
        let put = &puts[0];
        assert_eq!(put.hash, "q1");
        assert_eq!(put.ttl, Some(2_000.0));
        assert_eq!(
            put.ast.as_ref().map(|ast| ast.table.as_str()),
            Some("issues")
        );
    }
}
