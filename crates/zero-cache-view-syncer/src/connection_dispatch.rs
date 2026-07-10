//! Per-connection upstream-message router — the decision layer of a served
//! WebSocket connection loop.
//!
//! A client connection is a loop: decode an incoming frame
//! (`zero-cache-protocol::up_json::upstream_from_json`), decide what it means
//! for this connection, act (mutate CVR desired-queries, reply, close), and
//! encode any downstream response. This module is the middle "decide" step: it
//! maps a decoded [`Upstream`] message to a [`ConnectionAction`] the connection
//! loop then carries out against the live view-syncer/CVR machinery.
//!
//! Keeping this a pure `Upstream -> ConnectionAction` classification (no live
//! CVR/socket handles) makes the routing independently testable and mirrors how
//! upstream's `syncConnection`/`handleMessage` first switches on the message
//! tag before dispatching to the stateful handler. The stateful handlers
//! themselves (applying a desired-queries patch to the CVR, running the query,
//! emitting pokes) are the existing `cvr_*` / `view_syncer_*` modules; this just
//! names which one each message drives.

use zero_cache_protocol::change_desired_queries::ChangeDesiredQueriesBody;
use zero_cache_protocol::connect::InitConnectionBody;
use zero_cache_protocol::delete_clients::DeleteClientsBody;
use zero_cache_protocol::inspect_up::InspectUpBody;
use zero_cache_protocol::pull::PullRequestBody;
use zero_cache_protocol::push::{AckMutationResponsesBody, PushBody};
use zero_cache_protocol::up::Upstream;
use zero_cache_protocol::update_auth::UpdateAuthBody;

/// What a connection should do in response to an upstream message. Each variant
/// names the stateful handler the connection loop invokes next.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum ConnectionAction {
    /// A `ping` — the loop replies with a `pong` downstream frame. No state
    /// change.
    Pong,
    /// First message on a connection: initialize the client's desired queries
    /// (and client schema, if provided). Carries the whole init body for the
    /// handler to apply.
    Initialize(Box<InitConnectionBody>),
    /// Update the client group's desired-query set (add/remove/clear queries).
    UpdateDesiredQueries(ChangeDesiredQueriesBody),
    /// Inactivate/delete the named clients from the client group.
    DeleteClients(DeleteClientsBody),
    /// A `push`: one or more client mutations to apply. Carries the whole
    /// push body for the handler (CRUD mutations go to `apply_crud_mutation`
    /// against upstream Postgres; custom mutations are the caller's concern).
    Push(PushBody),
    /// A `pull`: mutation-recovery catchup request. The handler may answer
    /// with a `pull` response carrying last-mutation-id changes.
    Pull(PullRequestBody),
    /// Refreshes the auth token attached to this connection/client group.
    UpdateAuth(UpdateAuthBody),
    /// Acknowledges a mutation response so the pusher can clean up stored
    /// response state.
    AckMutationResponses(AckMutationResponsesBody),
    /// Debug/inspection protocol request.
    Inspect(InspectUpBody),
    /// Deprecated `closeConnection` message — tear the connection down.
    Close,
}

/// Whether an `initConnection` must be the FIRST message on a connection.
/// Upstream treats a second `initConnection` (or any data message before the
/// first `initConnection`) as a protocol error; the connection loop enforces
/// ordering, and [`dispatch_upstream`] surfaces the classification it needs via
/// [`ConnectionAction::Initialize`] vs the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitState {
    /// No `initConnection` seen yet on this connection.
    AwaitingInit,
    /// The connection has been initialized.
    Initialized,
}

/// A protocol-ordering violation detected while routing a message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DispatchError {
    #[error("received {tag} before initConnection")]
    MessageBeforeInit { tag: &'static str },
    #[error("received a second initConnection on an already-initialized connection")]
    DuplicateInit,
}

fn tag_of(msg: &Upstream) -> &'static str {
    match msg {
        Upstream::InitConnection(_) => "initConnection",
        Upstream::Ping(_) => "ping",
        Upstream::DeleteClients(_) => "deleteClients",
        Upstream::ChangeDesiredQueries(_) => "changeDesiredQueries",
        Upstream::Pull(_) => "pull",
        Upstream::Push(_) => "push",
        Upstream::UpdateAuth(_) => "updateAuth",
        Upstream::AckMutationResponses(_) => "ackMutationResponses",
        Upstream::Inspect(_) => "inspect",
        Upstream::CloseConnection(_) => "closeConnection",
    }
}

/// Routes an upstream message to a [`ConnectionAction`], enforcing the
/// `initConnection`-first ordering. `ping` is always allowed (a keepalive can
/// arrive before init). Returns the action plus the connection's new
/// [`InitState`].
pub fn dispatch_upstream(
    msg: Upstream,
    state: InitState,
) -> Result<(ConnectionAction, InitState), DispatchError> {
    match msg {
        Upstream::Ping(_) => Ok((ConnectionAction::Pong, state)),
        Upstream::InitConnection(body) => match state {
            InitState::AwaitingInit => Ok((
                ConnectionAction::Initialize(Box::new(body)),
                InitState::Initialized,
            )),
            InitState::Initialized => Err(DispatchError::DuplicateInit),
        },
        other => {
            // All other messages require an initialized connection.
            if state == InitState::AwaitingInit {
                return Err(DispatchError::MessageBeforeInit {
                    tag: tag_of(&other),
                });
            }
            let action = match other {
                Upstream::ChangeDesiredQueries(b) => ConnectionAction::UpdateDesiredQueries(b),
                Upstream::DeleteClients(b) => ConnectionAction::DeleteClients(b),
                Upstream::Pull(b) => ConnectionAction::Pull(b),
                Upstream::Push(b) => ConnectionAction::Push(b),
                Upstream::UpdateAuth(b) => ConnectionAction::UpdateAuth(b),
                Upstream::AckMutationResponses(b) => ConnectionAction::AckMutationResponses(b),
                Upstream::Inspect(b) => ConnectionAction::Inspect(b),
                Upstream::CloseConnection(_) => ConnectionAction::Close,
                // ping/init handled above.
                Upstream::Ping(_) | Upstream::InitConnection(_) => unreachable!(),
            };
            Ok((action, state))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::close_connection::CloseConnectionBody;
    use zero_cache_protocol::ping::PingBody;

    #[test]
    fn ping_is_allowed_before_and_after_init_without_changing_state() {
        let (a, s) = dispatch_upstream(Upstream::Ping(PingBody), InitState::AwaitingInit).unwrap();
        assert_eq!(a, ConnectionAction::Pong);
        assert_eq!(s, InitState::AwaitingInit);
        let (a, s) = dispatch_upstream(Upstream::Ping(PingBody), InitState::Initialized).unwrap();
        assert_eq!(a, ConnectionAction::Pong);
        assert_eq!(s, InitState::Initialized);
    }

    #[test]
    fn first_init_initializes_the_connection() {
        let body = InitConnectionBody::default();
        let (a, s) = dispatch_upstream(
            Upstream::InitConnection(body.clone()),
            InitState::AwaitingInit,
        )
        .unwrap();
        assert_eq!(a, ConnectionAction::Initialize(Box::new(body)));
        assert_eq!(s, InitState::Initialized);
    }

    #[test]
    fn second_init_is_a_protocol_error() {
        let err = dispatch_upstream(
            Upstream::InitConnection(InitConnectionBody::default()),
            InitState::Initialized,
        )
        .unwrap_err();
        assert_eq!(err, DispatchError::DuplicateInit);
    }

    #[test]
    fn data_messages_require_init_first() {
        // Before init: rejected with the offending tag.
        let err = dispatch_upstream(
            Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
                desired_queries_patch: vec![],
                traceparent: None,
            }),
            InitState::AwaitingInit,
        )
        .unwrap_err();
        assert_eq!(
            err,
            DispatchError::MessageBeforeInit {
                tag: "changeDesiredQueries"
            }
        );

        // After init: routed to the desired-queries handler.
        let (a, s) = dispatch_upstream(
            Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
                desired_queries_patch: vec![],
                traceparent: None,
            }),
            InitState::Initialized,
        )
        .unwrap();
        assert!(matches!(a, ConnectionAction::UpdateDesiredQueries(_)));
        assert_eq!(s, InitState::Initialized);
    }

    #[test]
    fn delete_clients_and_close_route_after_init() {
        let (a, _) = dispatch_upstream(
            Upstream::DeleteClients(DeleteClientsBody::default()),
            InitState::Initialized,
        )
        .unwrap();
        assert!(matches!(a, ConnectionAction::DeleteClients(_)));

        let (a, _) = dispatch_upstream(
            Upstream::CloseConnection(CloseConnectionBody),
            InitState::Initialized,
        )
        .unwrap();
        assert_eq!(a, ConnectionAction::Close);
    }

    #[test]
    fn push_routes_after_init_and_is_rejected_before() {
        let body = PushBody {
            client_group_id: "cg1".into(),
            mutations: vec![],
            push_version: 1.0,
            schema_version: None,
            timestamp: 0.0,
            request_id: "r1".into(),
            traceparent: None,
        };
        let err =
            dispatch_upstream(Upstream::Push(body.clone()), InitState::AwaitingInit).unwrap_err();
        assert_eq!(err, DispatchError::MessageBeforeInit { tag: "push" });

        let (a, s) = dispatch_upstream(Upstream::Push(body), InitState::Initialized).unwrap();
        assert!(matches!(a, ConnectionAction::Push(_)));
        assert_eq!(s, InitState::Initialized);
    }

    #[test]
    fn pull_routes_after_init_and_is_rejected_before() {
        let body = PullRequestBody {
            client_group_id: "cg1".into(),
            cookie: None,
            request_id: "r1".into(),
        };
        let err =
            dispatch_upstream(Upstream::Pull(body.clone()), InitState::AwaitingInit).unwrap_err();
        assert_eq!(err, DispatchError::MessageBeforeInit { tag: "pull" });

        let (a, s) = dispatch_upstream(Upstream::Pull(body), InitState::Initialized).unwrap();
        assert!(matches!(a, ConnectionAction::Pull(_)));
        assert_eq!(s, InitState::Initialized);
    }

    #[test]
    fn update_auth_routes_after_init_and_is_rejected_before() {
        let body = UpdateAuthBody {
            auth: "token".into(),
        };
        let err = dispatch_upstream(Upstream::UpdateAuth(body.clone()), InitState::AwaitingInit)
            .unwrap_err();
        assert_eq!(err, DispatchError::MessageBeforeInit { tag: "updateAuth" });

        let (a, s) = dispatch_upstream(Upstream::UpdateAuth(body), InitState::Initialized).unwrap();
        assert!(matches!(a, ConnectionAction::UpdateAuth(_)));
        assert_eq!(s, InitState::Initialized);
    }

    #[test]
    fn ack_mutation_responses_routes_after_init_and_is_rejected_before() {
        let body = AckMutationResponsesBody {
            mutation_id: zero_cache_protocol::mutation_id::MutationId {
                id: 1.0,
                client_id: "c1".into(),
            },
        };
        let err = dispatch_upstream(
            Upstream::AckMutationResponses(body.clone()),
            InitState::AwaitingInit,
        )
        .unwrap_err();
        assert_eq!(
            err,
            DispatchError::MessageBeforeInit {
                tag: "ackMutationResponses"
            }
        );

        let (a, s) =
            dispatch_upstream(Upstream::AckMutationResponses(body), InitState::Initialized)
                .unwrap();
        assert!(matches!(a, ConnectionAction::AckMutationResponses(_)));
        assert_eq!(s, InitState::Initialized);
    }

    #[test]
    fn inspect_routes_after_init_and_is_rejected_before() {
        let body = InspectUpBody::Version { id: "i1".into() };
        let err = dispatch_upstream(Upstream::Inspect(body.clone()), InitState::AwaitingInit)
            .unwrap_err();
        assert_eq!(err, DispatchError::MessageBeforeInit { tag: "inspect" });

        let (a, s) = dispatch_upstream(Upstream::Inspect(body), InitState::Initialized).unwrap();
        assert!(matches!(a, ConnectionAction::Inspect(_)));
        assert_eq!(s, InitState::Initialized);
    }

    #[test]
    fn delete_clients_before_init_is_rejected() {
        let err = dispatch_upstream(
            Upstream::DeleteClients(DeleteClientsBody::default()),
            InitState::AwaitingInit,
        )
        .unwrap_err();
        assert_eq!(
            err,
            DispatchError::MessageBeforeInit {
                tag: "deleteClients"
            }
        );
    }
}
