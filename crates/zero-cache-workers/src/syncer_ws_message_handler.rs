//! Port of the pure routing decision inside `SyncerWsMessageHandler#handleMessage`'s
//! `'push'` case (`syncer-ws-message-handler.ts`) — the logic that decides
//! WHERE a push message should go (custom-mutation pusher vs. legacy CRUD
//! mutagen) and which early-exit/error outcomes apply, before any of the
//! actual mutation processing happens.
//!
//! Scope: `SyncerWsMessageHandler` itself is not ported — it's a thin
//! dispatch shell wired to `ViewSyncer`/`Mutagen`/`Pusher`/
//! `ConnectionContextManager`, none of which (except the last) exist in
//! this port yet, and OTEL tracing/`Lock`-based mutation ordering aren't
//! modeled. What IS ported is the actual decision tree the `'push'` case
//! runs before it touches any of those: clientGroupID validation, the
//! empty-mutations fast path, and the custom-vs-CRUD routing with its two
//! "not configured" error paths (`pusher` unset / `mutagen` unset) — pure
//! logic a caller can drive once those services exist.

use zero_cache_protocol::error::ErrorBody;
use zero_cache_protocol::error_kind::ErrorKind;
use zero_cache_protocol::error_origin::ErrorOrigin;

/// Which kind of mutation a push's first (and only) mutation is. Port of
/// the `mutations[0].type === 'custom'` check upstream makes — modeled as
/// an enum rather than inspecting a full mutation payload, since this
/// crate's `Mutation`/`CustomMutation`/`CRUDMutation` wire types live in
/// `zero-cache-mutagen::crud_ops`, not here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    Custom,
    Crud,
}

/// The routing decision for one push message. Port of the `'push'` case's
/// control flow, minus the actual mutation processing (`Pusher::enqueuePush`/
/// `Mutagen::processMutation`), which a caller performs after receiving
/// `RouteToPusher`/`RouteToMutagen`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushRouting {
    /// `clientGroupID` in the mutation didn't match the connection's —
    /// close with `InvalidPush`.
    ClientGroupMismatch(ErrorBody),
    /// No mutations in the push — nothing to do.
    NoOp,
    /// The first mutation is `custom` but no `Pusher` is configured
    /// (`ZERO_MUTATE_URL` unset) — close with `InvalidPush`.
    PusherNotConfigured(ErrorBody),
    /// Route to the pusher (custom mutation, pusher available).
    RouteToPusher,
    /// The first mutation is CRUD but legacy CRUD support is disabled (no
    /// `Mutagen`) — close with `InvalidPush`.
    MutagenNotConfigured(ErrorBody),
    /// Route to mutagen (CRUD mutation, mutagen available).
    RouteToMutagen,
}

/// Port of the decision tree at the top of `SyncerWsMessageHandler#handleMessage`'s
/// `'push'` case: `clientGroupID` validation -> empty-mutations fast path
/// -> custom-vs-CRUD routing (each side gated on its service being
/// configured).
pub fn route_push(
    msg_client_group_id: &str,
    connection_client_group_id: &str,
    mutation_count: usize,
    first_mutation_kind: Option<MutationKind>,
    has_pusher: bool,
    has_mutagen: bool,
) -> PushRouting {
    if msg_client_group_id != connection_client_group_id {
        return PushRouting::ClientGroupMismatch(ErrorBody::new(
            ErrorKind::InvalidPush,
            format!(
                "clientGroupID in mutation \"{msg_client_group_id}\" does not match clientGroupID of connection \"{connection_client_group_id}"
            ),
            Some(ErrorOrigin::ZeroCache),
        ));
    }

    if mutation_count == 0 {
        return PushRouting::NoOp;
    }

    match first_mutation_kind {
        Some(MutationKind::Custom) => {
            if !has_pusher {
                return PushRouting::PusherNotConfigured(ErrorBody::new(
                    ErrorKind::InvalidPush,
                    "A ZERO_MUTATE_URL must be set in order to process custom mutations.",
                    Some(ErrorOrigin::ZeroCache),
                ));
            }
            PushRouting::RouteToPusher
        }
        Some(MutationKind::Crud) | None => {
            if !has_mutagen {
                return PushRouting::MutagenNotConfigured(ErrorBody::new(
                    ErrorKind::InvalidPush,
                    "Support for legacy CRUD mutations is disabled",
                    Some(ErrorOrigin::ZeroCache),
                ));
            }
            PushRouting::RouteToMutagen
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mismatched_client_group_id_errors() {
        let result = route_push("cg2", "cg1", 1, Some(MutationKind::Crud), true, true);
        match result {
            PushRouting::ClientGroupMismatch(body) => {
                assert_eq!(body.kind, ErrorKind::InvalidPush);
                assert!(body.message.contains("cg2"));
                assert!(body.message.contains("cg1"));
            }
            other => panic!("expected ClientGroupMismatch, got {other:?}"),
        }
    }

    #[test]
    fn client_group_mismatch_takes_priority_over_empty_mutations() {
        // Matches upstream's ordering: the clientGroupID check runs before
        // the mutations.length === 0 check.
        let result = route_push("cg2", "cg1", 0, None, true, true);
        assert!(matches!(result, PushRouting::ClientGroupMismatch(_)));
    }

    #[test]
    fn empty_mutations_is_a_noop() {
        assert_eq!(
            route_push("cg1", "cg1", 0, None, true, true),
            PushRouting::NoOp
        );
    }

    #[test]
    fn custom_mutation_routes_to_pusher_when_configured() {
        assert_eq!(
            route_push("cg1", "cg1", 1, Some(MutationKind::Custom), true, true),
            PushRouting::RouteToPusher
        );
    }

    #[test]
    fn custom_mutation_without_pusher_errors() {
        let result = route_push("cg1", "cg1", 1, Some(MutationKind::Custom), false, true);
        match result {
            PushRouting::PusherNotConfigured(body) => assert_eq!(body.kind, ErrorKind::InvalidPush),
            other => panic!("expected PusherNotConfigured, got {other:?}"),
        }
    }

    #[test]
    fn crud_mutation_routes_to_mutagen_when_configured() {
        assert_eq!(
            route_push("cg1", "cg1", 1, Some(MutationKind::Crud), true, true),
            PushRouting::RouteToMutagen
        );
    }

    #[test]
    fn crud_mutation_without_mutagen_errors() {
        let result = route_push("cg1", "cg1", 1, Some(MutationKind::Crud), true, false);
        match result {
            PushRouting::MutagenNotConfigured(body) => {
                assert_eq!(body.kind, ErrorKind::InvalidPush)
            }
            other => panic!("expected MutagenNotConfigured, got {other:?}"),
        }
    }

    #[test]
    fn custom_mutation_pusher_availability_is_independent_of_mutagen() {
        // A custom mutation should route to the pusher even if mutagen is
        // unavailable — the two configs are checked independently, per
        // upstream's per-branch config checks.
        assert_eq!(
            route_push("cg1", "cg1", 1, Some(MutationKind::Custom), true, false),
            PushRouting::RouteToPusher
        );
    }
}
