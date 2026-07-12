//! Port of `services/view-syncer/connection-context-manager.ts`'s core
//! state machine — the `ConnectionContextManager` flagged as the
//! prerequisite blocking `PusherService`/`PushWorker`'s `Queue`-based
//! lifecycle wrapper (`enqueuePush`/`ackMutationResponses` both need
//! `mustGetConnectionContext`/`getConnectionContext`, and `PushWorker`'s
//! `#processPush` calls `validateConnection`/`failConnection`).
//!
//! Scope, deliberate: this ports the pure clientID/wsID/revision
//! bookkeeping and the provisional->validated connection lifecycle
//! (`registerConnection`/`getConnectionContext`/`validateConnection`/
//! `failConnection`/`closeConnection`/`planMaintenance`/background-
//! connection selection) — the actual state machine `PusherService` needs
//! to exist at all. NOT ported: the FULL `initConnection` (needs
//! `InitConnectionBody`/header-ALLOWLIST-filtering against `ZeroConfig`,
//! which this port doesn't have — see `normalize.rs`'s CLI-parsing
//! decision, still deferred) and `updateAuth` (needs `resolveAuth`/
//! legacy-JWT validation, a separate auth subsystem).
//!
//! `queryContext`/`mutateContext` (`ConnectionFetchContext`) ARE now
//! carried on `ConnectionContext`, closing a gap named across several
//! prior rounds (`pusher_batch::MutateContext`/`ConnCtx` were the
//! deliberately-separate simplified shape a caller needed until this
//! landed). Scope deviation: `allowed_url_patterns` is a `Vec<String>` of
//! raw pattern strings rather than compiled `URLPattern`s (no URL-pattern
//! library in this port), and `custom_headers`/`request_headers` are
//! carried as plain maps rather than allowlist-FILTERED against config
//! (upstream's `filterHeaders` needs the `ZeroConfig` allowlist this port
//! doesn't have) — a caller passes in already-whatever-headers-it-wants,
//! matching the same "this port has no CLI/env config layer yet" boundary
//! `normalize.rs`'s module doc names. `register_connection` builds a
//! `ConnectionContext` with EMPTY fetch contexts (matching upstream's
//! `registerConnection` before any config is known); the new
//! `update_fetch_contexts` lets a caller attach real query/push URLs +
//! headers afterward — the part of `initConnection` this round DOES port
//! (its header-allowlist-filtering/`InitConnectionBody` parsing half
//! remains the documented gap).
//!
//! `now` is taken as an explicit parameter everywhere instead of an
//! ambient `Date.now()`/injected clock closure, matching this port's
//! determinism convention.

use std::collections::HashMap;

use zero_cache_protocol::error::{ErrorBody, ProtocolError};
use zero_cache_protocol::error_kind::ErrorKind;
use zero_cache_protocol::error_origin::ErrorOrigin;

/// Port of `ConnectionState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Provisional,
    Validated,
}

/// Port of `UserState`. `id: None` means logged out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserState {
    pub id: Option<String>,
}

/// Port of `ConnectionValidation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionValidation {
    ClientFallback,
    ServerValidated { validated_user_id: Option<String> },
}

/// Port of `ConnectionSelector`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionSelector {
    pub client_id: String,
    pub ws_id: String,
}

/// Port of `HeaderOptions`, trimmed per module doc (`apiKey`/headers are
/// plain caller-supplied maps, not allowlist-filtered against config).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HeaderOptions {
    pub api_key: Option<String>,
    pub custom_headers: Option<std::collections::BTreeMap<String, String>>,
    pub request_headers: Option<std::collections::BTreeMap<String, String>>,
    pub cookie: Option<String>,
    pub origin: Option<String>,
}

/// Port of `ConnectionFetchContext`. `allowed_url_patterns` is a `Vec<String>`
/// of raw pattern strings rather than compiled `URLPattern`s — see module
/// doc.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectionFetchContext {
    pub url: Option<String>,
    pub allowed_url_patterns: Vec<String>,
    pub header_options: HeaderOptions,
}

/// A snapshot of one live connection. Trimmed from upstream's
/// `ConnectionContext` — no `auth`/`profileID`/`baseCookie`/
/// `protocolVersion` (see module doc for `queryContext`/`mutateContext`,
/// which ARE now carried).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionContext {
    pub state: ConnectionState,
    pub client_id: String,
    pub ws_id: String,
    pub user: UserState,
    pub revision: u64,
    pub revalidate_at: Option<i64>,
    pub insertion_order: u64,
    pub query_context: ConnectionFetchContext,
    pub mutate_context: ConnectionFetchContext,
}

/// Port of `GroupAuthState`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GroupAuthState {
    pub pinned_user: Option<UserState>,
    pub background_connection: Option<ConnectionSelector>,
    pub retransform_at: Option<i64>,
    pub maintenance_not_before_at: Option<i64>,
}

/// Port of `planMaintenance`'s return shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenancePlan {
    pub due_revalidations: Vec<ConnectionContext>,
    pub due_retransform: bool,
    pub earliest_deadline_at: Option<i64>,
}

fn min_defined(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (None, b) => b,
        (a, None) => a,
        (Some(a), Some(b)) => Some(a.min(b)),
    }
}

fn compare_by_insertion_order(a: &ConnectionContext, b: &ConnectionContext) -> std::cmp::Ordering {
    a.insertion_order
        .cmp(&b.insertion_order)
        .then_with(|| a.ws_id.cmp(&b.ws_id))
}

fn not_found_error() -> ProtocolError {
    ProtocolError::new(ErrorBody::new(
        ErrorKind::InvalidConnectionRequest,
        "Connection auth state was not available for this websocket.",
        Some(ErrorOrigin::ZeroCache),
    ))
}

/// Port of `ConnectionContextManagerImpl` — see module doc for exact scope.
pub struct ConnectionContextManager {
    connections: HashMap<String, ConnectionContext>,
    group: GroupAuthState,
    revalidate_interval_ms: Option<i64>,
    retransform_interval_ms: Option<i64>,
    shared_retransform_ready: bool,
    next_insertion_order: u64,
}

impl ConnectionContextManager {
    pub fn new(revalidate_interval_ms: Option<i64>, retransform_interval_ms: Option<i64>) -> Self {
        ConnectionContextManager {
            connections: HashMap::new(),
            group: GroupAuthState::default(),
            revalidate_interval_ms,
            retransform_interval_ms,
            shared_retransform_ready: false,
            next_insertion_order: 0,
        }
    }

    /// Port of `registerConnection` (simplified — see module doc for what's
    /// not carried onto `ConnectionContext`).
    pub fn register_connection(
        &mut self,
        selector: &ConnectionSelector,
        user_id: Option<String>,
    ) -> ConnectionContext {
        self.remove_connection(selector, None);
        self.next_insertion_order += 1;
        let connection = ConnectionContext {
            state: ConnectionState::Provisional,
            client_id: selector.client_id.clone(),
            ws_id: selector.ws_id.clone(),
            revision: 0,
            user: UserState { id: user_id },
            revalidate_at: None,
            insertion_order: self.next_insertion_order,
            query_context: ConnectionFetchContext::default(),
            mutate_context: ConnectionFetchContext::default(),
        };
        self.store_connection(connection.clone());
        self.refresh_background_connection_context(None);
        self.update_background_retransform_deadline(false, None);
        connection
    }

    /// The header-allowlist-filtering-free part of `initConnection`:
    /// attaches real query/push `ConnectionFetchContext`s to an already-
    /// registered connection, bumping its revision (matching upstream —
    /// `initConnection` always bumps, even when nothing material changed)
    /// and demoting it back to `Provisional` (port of `#demoteConnection`,
    /// same as `initConnection`'s real behavior: a context change means
    /// re-validation is needed). Returns `None` if the connection doesn't
    /// exist (upstream would throw `ProtocolError(InvalidConnectionRequest)`
    /// via `#mustGetConnectionContext` — surfaced as `Result` here instead).
    pub fn update_fetch_contexts(
        &mut self,
        selector: &ConnectionSelector,
        query_context: ConnectionFetchContext,
        mutate_context: ConnectionFetchContext,
    ) -> Result<ConnectionContext, ProtocolError> {
        let connection = self.must_get_connection_context(selector)?.clone();
        let updated = ConnectionContext {
            revision: connection.revision + 1,
            query_context,
            mutate_context,
            ..connection
        };
        Ok(self.demote_connection(updated))
    }

    /// Port of `validateConnection`. Returns `Ok(None)` for the stale-
    /// revision/gone-connection no-op cases upstream returns `undefined`
    /// for, and `Err` for the two `ProtocolErrorWithLevel` throw paths.
    pub fn validate_connection(
        &mut self,
        selector: &ConnectionSelector,
        revision: u64,
        validation: ConnectionValidation,
        now: i64,
    ) -> Result<Option<(ConnectionContext, GroupAuthState)>, ProtocolError> {
        let Some(connection) = self.get_connection_context(selector).cloned() else {
            return Ok(None);
        };
        if connection.revision != revision {
            return Ok(None);
        }

        let validated_user_state = match &validation {
            ConnectionValidation::ServerValidated { validated_user_id } => {
                let validated = UserState {
                    id: validated_user_id.clone(),
                };
                if connection.user.id != validated.id {
                    return Err(ProtocolError::new(ErrorBody::new(
                        ErrorKind::Unauthorized,
                        "Connection userID does not match validated server userID.",
                        Some(ErrorOrigin::ZeroCache),
                    )));
                }
                Some(validated)
            }
            ConnectionValidation::ClientFallback => None,
        };

        let incoming_user_state = validated_user_state.unwrap_or_else(|| connection.user.clone());

        if let Some(pinned) = &self.group.pinned_user {
            if pinned.id != incoming_user_state.id {
                return Err(ProtocolError::new(ErrorBody::new(
                    ErrorKind::Unauthorized,
                    "Client groups are pinned to a single userID. Connection userID does not match existing client group userID.",
                    Some(ErrorOrigin::ZeroCache),
                )));
            }
        } else {
            self.group.pinned_user = Some(incoming_user_state);
        }

        let mut validated_connection = connection;
        validated_connection.state = ConnectionState::Validated;
        validated_connection.revalidate_at = self.next_revalidate_at(now);
        self.store_connection(validated_connection.clone());
        self.refresh_background_connection_context(Some(validated_connection.clone()));
        self.update_background_retransform_deadline(false, now.into());

        Ok(Some((validated_connection, self.group.clone())))
    }

    /// Port of `failConnection` (delegates to `#removeConnection` with a
    /// revision guard, matching upstream).
    pub fn fail_connection(
        &mut self,
        selector: &ConnectionSelector,
        revision: u64,
    ) -> Option<ConnectionContext> {
        self.remove_connection(selector, Some(revision))
    }

    /// Port of `closeConnection`.
    pub fn close_connection(&mut self, selector: &ConnectionSelector) -> Option<ConnectionContext> {
        self.remove_connection(selector, None)
    }

    pub fn get_connection_context(
        &self,
        selector: &ConnectionSelector,
    ) -> Option<&ConnectionContext> {
        let connection = self.connections.get(&selector.client_id)?;
        if connection.ws_id != selector.ws_id {
            return None;
        }
        Some(connection)
    }

    pub fn must_get_connection_context(
        &self,
        selector: &ConnectionSelector,
    ) -> Result<&ConnectionContext, ProtocolError> {
        self.get_connection_context(selector)
            .ok_or_else(not_found_error)
    }

    pub fn get_background_connection_context(&self) -> Option<&ConnectionContext> {
        let selector = self.group.background_connection.as_ref()?;
        self.get_connection_context(selector)
    }

    pub fn get_group_state(&self) -> &GroupAuthState {
        &self.group
    }

    /// Port of `planMaintenance`.
    pub fn plan_maintenance(&self, now: i64) -> MaintenancePlan {
        let mut due_revalidations = Vec::new();
        let mut earliest_deadline_at = self.group.retransform_at;

        for connection in self.connections.values() {
            if connection.state != ConnectionState::Validated {
                continue;
            }
            let Some(revalidate_at) = connection.revalidate_at else {
                continue;
            };
            if revalidate_at <= now {
                due_revalidations.push(connection.clone());
            }
            earliest_deadline_at = min_defined(earliest_deadline_at, Some(revalidate_at));
        }

        let due_retransform = self.group.retransform_at.is_some_and(|at| at <= now);
        let maintenance_not_before_at = self.group.maintenance_not_before_at;

        if let Some(not_before) = maintenance_not_before_at {
            if not_before > now {
                if let Some(deadline) = earliest_deadline_at {
                    return MaintenancePlan {
                        due_revalidations: vec![],
                        due_retransform: false,
                        earliest_deadline_at: Some(deadline.max(not_before)),
                    };
                }
            }
        }

        due_revalidations.sort_by(compare_by_insertion_order);
        MaintenancePlan {
            due_revalidations,
            due_retransform,
            earliest_deadline_at,
        }
    }

    fn remove_connection(
        &mut self,
        selector: &ConnectionSelector,
        revision: Option<u64>,
    ) -> Option<ConnectionContext> {
        let connection = self.get_connection_context(selector)?.clone();
        if let Some(revision) = revision {
            if connection.revision != revision {
                return None;
            }
        }
        self.connections.remove(&connection.client_id);
        self.refresh_background_connection_context(None);
        self.update_background_retransform_deadline(false, None);
        Some(connection)
    }

    fn store_connection(&mut self, connection: ConnectionContext) {
        self.connections
            .insert(connection.client_id.clone(), connection);
    }

    /// Port of `#demoteConnection`: stores the (already-modified) connection
    /// back as `Provisional` with no revalidation deadline, then re-derives
    /// background-connection/retransform-deadline state, matching every
    /// other mutation method's trailing side effects.
    fn demote_connection(&mut self, connection: ConnectionContext) -> ConnectionContext {
        let demoted = ConnectionContext {
            state: ConnectionState::Provisional,
            revalidate_at: None,
            ..connection
        };
        self.store_connection(demoted.clone());
        self.refresh_background_connection_context(None);
        self.update_background_retransform_deadline(false, None);
        demoted
    }

    fn refresh_background_connection_context(&mut self, preferred: Option<ConnectionContext>) {
        if let Some(preferred) = &preferred {
            if preferred.state == ConnectionState::Validated {
                let current = self.get_background_connection_context().cloned();
                if let Some(current) = &current {
                    if current.client_id == preferred.client_id && current.ws_id == preferred.ws_id
                    {
                        return;
                    }
                    return;
                }
                self.set_background_connection(Some(ConnectionSelector {
                    client_id: preferred.client_id.clone(),
                    ws_id: preferred.ws_id.clone(),
                }));
                return;
            }
        }

        if let Some(current) = self.get_background_connection_context() {
            if current.state == ConnectionState::Validated {
                return;
            }
        }

        let next = self
            .connections
            .values()
            .filter(|c| c.state == ConnectionState::Validated)
            .max_by(|a, b| {
                a.insertion_order
                    .cmp(&b.insertion_order)
                    .then_with(|| a.ws_id.cmp(&b.ws_id))
            })
            .cloned();
        self.set_background_connection(next.map(|c| ConnectionSelector {
            client_id: c.client_id,
            ws_id: c.ws_id,
        }));
    }

    fn set_background_connection(&mut self, selector: Option<ConnectionSelector>) {
        if self.group.background_connection == selector {
            return;
        }
        self.group.background_connection = selector;
    }

    fn update_background_retransform_deadline(&mut self, reset: bool, now: Option<i64>) {
        let has_background = self.get_background_connection_context().is_some();
        if !has_background
            || self.retransform_interval_ms.is_none()
            || !self.shared_retransform_ready
        {
            if self.group.retransform_at.is_some() {
                self.group.retransform_at = None;
            }
            return;
        }

        if reset || self.group.retransform_at.is_none() {
            if let Some(now) = now {
                self.group.retransform_at = Some(now + self.retransform_interval_ms.unwrap());
            }
        }
    }

    fn next_revalidate_at(&self, now: i64) -> Option<i64> {
        self.revalidate_interval_ms.map(|ms| now + ms)
    }

    pub fn set_shared_retransform_ready(&mut self, ready: bool, now: i64) {
        if self.shared_retransform_ready == ready {
            return;
        }
        self.shared_retransform_ready = ready;
        self.update_background_retransform_deadline(true, Some(now));
    }

    /// Port of `deferMaintenance`.
    pub fn defer_maintenance(&mut self, kind: MaintenanceKind, now: i64) {
        let interval_ms = match kind {
            MaintenanceKind::Revalidate => self.revalidate_interval_ms,
            MaintenanceKind::Retransform => self.retransform_interval_ms,
        };
        let Some(interval_ms) = interval_ms else {
            return;
        };
        let candidate = now + interval_ms;
        self.group.maintenance_not_before_at = Some(
            self.group
                .maintenance_not_before_at
                .unwrap_or(0)
                .max(candidate),
        );
    }
}

/// Port of `deferMaintenance`'s `kind` parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceKind {
    Revalidate,
    Retransform,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sel(client_id: &str, ws_id: &str) -> ConnectionSelector {
        ConnectionSelector {
            client_id: client_id.into(),
            ws_id: ws_id.into(),
        }
    }

    #[test]
    fn register_creates_provisional_connection() {
        let mut mgr = ConnectionContextManager::new(None, None);
        let conn = mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        assert_eq!(conn.state, ConnectionState::Provisional);
        assert_eq!(conn.revision, 0);
        assert_eq!(conn.user.id, Some("u1".into()));
    }

    #[test]
    fn register_replaces_existing_connection_for_same_client() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        let conn = mgr.register_connection(&sel("c1", "ws2"), Some("u1".into()));
        assert_eq!(conn.ws_id, "ws2");
        assert!(mgr.get_connection_context(&sel("c1", "ws1")).is_none());
    }

    #[test]
    fn get_connection_context_requires_matching_ws_id() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        assert!(mgr.get_connection_context(&sel("c1", "ws2")).is_none());
        assert!(mgr.get_connection_context(&sel("c1", "ws1")).is_some());
    }

    #[test]
    fn must_get_connection_context_errors_when_missing() {
        let mgr = ConnectionContextManager::new(None, None);
        let err = mgr
            .must_get_connection_context(&sel("nope", "ws1"))
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidConnectionRequest);
    }

    #[test]
    fn validate_connection_promotes_to_validated_and_pins_group_user() {
        let mut mgr = ConnectionContextManager::new(Some(1000), None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));

        let (conn, group) = mgr
            .validate_connection(
                &sel("c1", "ws1"),
                0,
                ConnectionValidation::ClientFallback,
                100,
            )
            .unwrap()
            .unwrap();
        assert_eq!(conn.state, ConnectionState::Validated);
        assert_eq!(conn.revalidate_at, Some(1100));
        assert_eq!(
            group.pinned_user,
            Some(UserState {
                id: Some("u1".into())
            })
        );
        assert_eq!(group.background_connection, Some(sel("c1", "ws1")));
    }

    #[test]
    fn validate_connection_stale_revision_is_a_noop() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        let result = mgr
            .validate_connection(
                &sel("c1", "ws1"),
                5,
                ConnectionValidation::ClientFallback,
                0,
            )
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn validate_connection_missing_connection_is_a_noop() {
        let mut mgr = ConnectionContextManager::new(None, None);
        let result = mgr
            .validate_connection(
                &sel("gone", "ws1"),
                0,
                ConnectionValidation::ClientFallback,
                0,
            )
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn validate_connection_server_validated_mismatch_errors() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        let err = mgr
            .validate_connection(
                &sel("c1", "ws1"),
                0,
                ConnectionValidation::ServerValidated {
                    validated_user_id: Some("other".into()),
                },
                0,
            )
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unauthorized);
    }

    #[test]
    fn validate_connection_second_user_conflicting_with_pinned_group_errors() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        mgr.register_connection(&sel("c2", "ws1"), Some("u2".into()));
        mgr.validate_connection(
            &sel("c1", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();

        let err = mgr
            .validate_connection(
                &sel("c2", "ws1"),
                0,
                ConnectionValidation::ClientFallback,
                0,
            )
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unauthorized);
    }

    #[test]
    fn fail_connection_removes_it_and_clears_background() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        mgr.validate_connection(
            &sel("c1", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();
        assert!(mgr.get_background_connection_context().is_some());

        let removed = mgr.fail_connection(&sel("c1", "ws1"), 0).unwrap();
        assert_eq!(removed.client_id, "c1");
        assert!(mgr.get_connection_context(&sel("c1", "ws1")).is_none());
        assert!(mgr.get_background_connection_context().is_none());
    }

    #[test]
    fn fail_connection_stale_revision_is_a_noop() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        assert!(mgr.fail_connection(&sel("c1", "ws1"), 99).is_none());
        assert!(mgr.get_connection_context(&sel("c1", "ws1")).is_some());
    }

    #[test]
    fn close_connection_removes_regardless_of_revision() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        assert!(mgr.close_connection(&sel("c1", "ws1")).is_some());
        assert!(mgr.get_connection_context(&sel("c1", "ws1")).is_none());
    }

    #[test]
    fn background_connection_stays_sticky_once_validated() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        mgr.validate_connection(
            &sel("c1", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();
        // A second connection under a fallback validation with a matching
        // pinned userID becomes validated too, but shouldn't steal the slot.
        mgr.register_connection(&sel("c2", "ws1"), Some("u1".into()));
        mgr.validate_connection(
            &sel("c2", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();

        assert_eq!(
            mgr.get_background_connection_context().unwrap().client_id,
            "c1"
        );
    }

    #[test]
    fn plan_maintenance_reports_due_revalidations_sorted_by_insertion_order() {
        let mut mgr = ConnectionContextManager::new(Some(100), None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        mgr.validate_connection(
            &sel("c1", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();
        mgr.register_connection(&sel("c2", "ws1"), Some("u1".into()));
        mgr.validate_connection(
            &sel("c2", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();

        let plan = mgr.plan_maintenance(200);
        assert_eq!(plan.due_revalidations.len(), 2);
        assert_eq!(plan.due_revalidations[0].client_id, "c1");
        assert_eq!(plan.due_revalidations[1].client_id, "c2");
    }

    #[test]
    fn plan_maintenance_respects_maintenance_not_before() {
        let mut mgr = ConnectionContextManager::new(Some(10), None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        // revalidate_at = 0 + 10 = 10.
        mgr.validate_connection(
            &sel("c1", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();
        // not_before = 200 + 10 = 210, deferred well past revalidate_at.
        mgr.defer_maintenance(MaintenanceKind::Revalidate, 200);

        // At now=120, revalidate_at(10) has passed (would normally be due),
        // but maintenance_not_before_at(210) hasn't — the gate should
        // suppress reporting it as due and push the deadline out instead.
        let plan = mgr.plan_maintenance(120);
        assert!(plan.due_revalidations.is_empty());
        assert!(!plan.due_retransform);
        assert_eq!(plan.earliest_deadline_at, Some(210));
    }

    #[test]
    fn plan_maintenance_reports_a_shared_retransform_when_its_deadline_passes() {
        // Retransform is ONE shared deadline per client group (not
        // per-connection): it only arms once a background connection exists
        // AND the shared-retransform machinery reported ready.
        let mut mgr = ConnectionContextManager::new(None, Some(100));
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        mgr.validate_connection(
            &sel("c1", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();
        assert_eq!(
            mgr.get_group_state().retransform_at,
            None,
            "not armed until shared retransform is ready"
        );

        mgr.set_shared_retransform_ready(true, 0);
        assert_eq!(mgr.get_group_state().retransform_at, Some(100));

        let plan = mgr.plan_maintenance(50);
        assert!(!plan.due_retransform, "deadline not reached yet");
        assert_eq!(plan.earliest_deadline_at, Some(100));

        let plan = mgr.plan_maintenance(100);
        assert!(plan.due_retransform, "deadline reached");
        assert!(plan.due_revalidations.is_empty(), "revalidation disabled");
    }

    #[test]
    fn retransform_deadline_clears_when_the_background_connection_goes_away() {
        let mut mgr = ConnectionContextManager::new(None, Some(100));
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        mgr.validate_connection(
            &sel("c1", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();
        mgr.set_shared_retransform_ready(true, 0);
        assert_eq!(mgr.get_group_state().retransform_at, Some(100));

        mgr.close_connection(&sel("c1", "ws1"));
        assert_eq!(mgr.get_group_state().retransform_at, None);
        let plan = mgr.plan_maintenance(500);
        assert!(!plan.due_retransform);
        assert_eq!(plan.earliest_deadline_at, None);
    }

    #[test]
    fn defer_maintenance_retransform_suppresses_a_due_retransform_until_not_before() {
        let mut mgr = ConnectionContextManager::new(None, Some(100));
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        mgr.validate_connection(
            &sel("c1", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();
        mgr.set_shared_retransform_ready(true, 0); // retransform_at = 100

        // Defer at now=100: not_before = 100 + 100 = 200.
        mgr.defer_maintenance(MaintenanceKind::Retransform, 100);

        // At now=150 the retransform deadline (100) has passed, but the
        // defer gate (200) hasn't: suppressed, deadline pushed to 200.
        let plan = mgr.plan_maintenance(150);
        assert!(!plan.due_retransform);
        assert_eq!(plan.earliest_deadline_at, Some(200));

        // Once the gate passes, the retransform is due again.
        let plan = mgr.plan_maintenance(200);
        assert!(plan.due_retransform);
    }

    #[test]
    fn defer_maintenance_is_a_noop_when_the_kinds_interval_is_disabled() {
        // Interval None = feature disabled (the 0-seconds config case):
        // deferMaintenance must not install a not-before gate.
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.defer_maintenance(MaintenanceKind::Revalidate, 100);
        mgr.defer_maintenance(MaintenanceKind::Retransform, 100);
        assert_eq!(mgr.get_group_state().maintenance_not_before_at, None);
    }

    #[test]
    fn defer_maintenance_keeps_the_furthest_not_before() {
        let mut mgr = ConnectionContextManager::new(Some(100), Some(10));
        mgr.defer_maintenance(MaintenanceKind::Revalidate, 100); // 200
        mgr.defer_maintenance(MaintenanceKind::Retransform, 150); // 160 < 200
        assert_eq!(
            mgr.get_group_state().maintenance_not_before_at,
            Some(200),
            "an earlier candidate must not pull the gate back in"
        );
    }

    #[test]
    fn plan_maintenance_empty_manager_has_no_deadline() {
        let mgr = ConnectionContextManager::new(None, None);
        let plan = mgr.plan_maintenance(0);
        assert!(plan.due_revalidations.is_empty());
        assert!(!plan.due_retransform);
        assert_eq!(plan.earliest_deadline_at, None);
    }

    #[test]
    fn register_connection_starts_with_empty_fetch_contexts() {
        let mut mgr = ConnectionContextManager::new(None, None);
        let conn = mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        assert_eq!(conn.query_context, ConnectionFetchContext::default());
        assert_eq!(conn.mutate_context, ConnectionFetchContext::default());
    }

    #[test]
    fn update_fetch_contexts_attaches_real_urls_and_bumps_revision() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));

        let query_context = ConnectionFetchContext {
            url: Some("https://api.example/query".into()),
            allowed_url_patterns: vec!["https://api.example/*".into()],
            header_options: HeaderOptions {
                cookie: Some("session=abc".into()),
                ..Default::default()
            },
        };
        let mutate_context = ConnectionFetchContext {
            url: Some("https://api.example/mutate".into()),
            ..Default::default()
        };

        let updated = mgr
            .update_fetch_contexts(
                &sel("c1", "ws1"),
                query_context.clone(),
                mutate_context.clone(),
            )
            .unwrap();
        assert_eq!(updated.query_context, query_context);
        assert_eq!(updated.mutate_context, mutate_context);
        assert_eq!(
            updated.revision, 1,
            "initConnection always bumps the revision"
        );
    }

    #[test]
    fn update_fetch_contexts_demotes_a_validated_connection_back_to_provisional() {
        let mut mgr = ConnectionContextManager::new(None, None);
        mgr.register_connection(&sel("c1", "ws1"), Some("u1".into()));
        mgr.validate_connection(
            &sel("c1", "ws1"),
            0,
            ConnectionValidation::ClientFallback,
            0,
        )
        .unwrap();

        let updated = mgr
            .update_fetch_contexts(
                &sel("c1", "ws1"),
                ConnectionFetchContext::default(),
                ConnectionFetchContext::default(),
            )
            .unwrap();
        assert_eq!(
            updated.state,
            ConnectionState::Provisional,
            "a context change should demote back to provisional, matching #demoteConnection"
        );
    }

    #[test]
    fn update_fetch_contexts_errors_when_connection_missing() {
        let mut mgr = ConnectionContextManager::new(None, None);
        let err = mgr
            .update_fetch_contexts(
                &sel("nope", "ws1"),
                ConnectionFetchContext::default(),
                ConnectionFetchContext::default(),
            )
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidConnectionRequest);
    }
}
