//! Port of the connection-termination half of `pusher.ts`'s
//! `PushWorker#fanOutResponses` — the `PusherService`/`PushWorker`
//! streaming-service wrapper flagged as the last unported piece of
//! pusher.ts (queue/HTTP-forwarding transport is done: see
//! `pusher_batch.rs`/`api_fetch.rs`/`api_request.rs`).
//!
//! Scope: only the "successful push, but does a mutation result inside it
//! demand the connection be terminated" decision is ported here — i.e. the
//! `else` branch of `#fanOutResponses` (grouping `response.mutations` by
//! `clientID`, scanning each client's mutations in order for an
//! out-of-order-mutation error, since `Subscription<Downstream>.fail()`
//! must fire in that case). NOT ported: the whole-push-failed branch (needs
//! `PushFailedBody`/`ErrorReason`, not yet ported to
//! `zero-cache-protocol`), and everything requiring a live
//! `ConnectionContextManager`/`Subscription`/WebSocket — this is the pure
//! decision function a caller with those pieces would drive.

use zero_cache_protocol::mutation_id::MutationId;
use zero_cache_protocol::mutation_result::{MutationError, MutationResponse, ZeroErrorKind};

/// A client whose connection must be terminated because one of its
/// mutations in this push response came back out-of-order. Port of the
/// `connectionTerminations` callback closures' effect (`#failDownstream`),
/// minus the actual failure I/O.
#[derive(Debug, Clone, PartialEq)]
pub struct MutationTermination {
    pub client_id: String,
    /// Every mutation ID in the client's group, in arrival order — matches
    /// upstream's `mutations.map(m => ({clientID, id}))` (the whole group,
    /// not just the offending mutation).
    pub mutation_ids: Vec<MutationId>,
    pub message: String,
}

/// Port of `#fanOutResponses`'s success-path scan: groups `mutations` by
/// `id.clientID` (preserving first-seen order, like upstream's `groupBy`
/// over a JS `Map`), and for each group, finds the first mutation whose
/// result is a `ZeroErrorKind::OooMutation` error. If found, that whole
/// client's group is reported as needing termination — mirrors upstream's
/// `break`-after-first-match then `connectionTerminations.push(...)` (a
/// client is terminated at most once per response, even if terminations
/// aren't literally fired until after the whole scan, since JS builds the
/// closures list first then invokes it after the loop).
pub fn find_fatal_terminations(mutations: &[MutationResponse]) -> Vec<MutationTermination> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: std::collections::HashMap<String, Vec<&MutationResponse>> =
        std::collections::HashMap::new();

    for m in mutations {
        let key = m.id.client_id.clone();
        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        groups.entry(key).or_default().push(m);
    }

    let mut terminations = Vec::new();
    for client_id in order {
        let group = &groups[&client_id];
        let fatal = group.iter().find(|m| matches!(&m.result, zero_cache_protocol::mutation_result::MutationResult::Error(MutationError::Zero(z)) if z.error == ZeroErrorKind::OooMutation));
        if fatal.is_some() {
            terminations.push(MutationTermination {
                client_id: client_id.clone(),
                mutation_ids: group.iter().map(|m| m.id.clone()).collect(),
                message: "mutation was out of order".to_string(),
            });
        }
    }

    terminations
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::mutation_result::{MutationOk, MutationResult, MutationZeroError};

    fn ok(client_id: &str, id: f64) -> MutationResponse {
        MutationResponse {
            id: MutationId {
                id,
                client_id: client_id.into(),
            },
            result: MutationResult::Ok(MutationOk { data: None }),
        }
    }

    fn ooo(client_id: &str, id: f64) -> MutationResponse {
        MutationResponse {
            id: MutationId {
                id,
                client_id: client_id.into(),
            },
            result: MutationResult::Error(MutationError::Zero(MutationZeroError {
                error: ZeroErrorKind::OooMutation,
                details: None,
            })),
        }
    }

    #[test]
    fn no_terminations_when_all_ok() {
        let mutations = vec![ok("c1", 1.0), ok("c1", 2.0), ok("c2", 1.0)];
        assert!(find_fatal_terminations(&mutations).is_empty());
    }

    #[test]
    fn ooo_error_terminates_its_client_only() {
        let mutations = vec![ok("c1", 1.0), ooo("c1", 2.0), ok("c2", 1.0)];
        let terms = find_fatal_terminations(&mutations);
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].client_id, "c1");
        assert_eq!(
            terms[0].mutation_ids.len(),
            2,
            "termination carries the whole client group, not just the offending mutation"
        );
        assert_eq!(terms[0].message, "mutation was out of order");
    }

    #[test]
    fn app_errors_do_not_terminate() {
        use zero_cache_protocol::mutation_result::MutationAppError;
        let app_err = MutationResponse {
            id: MutationId {
                id: 1.0,
                client_id: "c1".into(),
            },
            result: MutationResult::Error(MutationError::App(MutationAppError {
                message: Some("boom".into()),
                details: None,
            })),
        };
        assert!(find_fatal_terminations(&[app_err]).is_empty());
    }

    #[test]
    fn multiple_clients_can_each_terminate_independently() {
        let mutations = vec![ooo("c1", 1.0), ooo("c2", 1.0)];
        let terms = find_fatal_terminations(&mutations);
        assert_eq!(terms.len(), 2);
        let ids: Vec<_> = terms.iter().map(|t| t.client_id.clone()).collect();
        assert!(ids.contains(&"c1".to_string()));
        assert!(ids.contains(&"c2".to_string()));
    }

    #[test]
    fn preserves_first_seen_client_order() {
        let mutations = vec![ok("c2", 1.0), ooo("c1", 1.0), ooo("c2", 2.0)];
        let terms = find_fatal_terminations(&mutations);
        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].client_id, "c2");
        assert_eq!(terms[1].client_id, "c1");
    }

    #[test]
    fn empty_input_returns_empty() {
        assert!(find_fatal_terminations(&[]).is_empty());
    }
}
