//! Port of the request-side (client -> server) portion of
//! `zero-protocol/src/push.ts`/`mutation.ts` — the `push` upstream message and
//! its `pushResponse` reply. This is the wire counterpart to
//! `mutation_result.rs` (the downstream `MutationResponse` shape used inside a
//! poke's `mutationsPatch`, ported earlier and explicitly deferring the
//! *request* types to "the unported mutagen/pusher.ts mutation-ingestion
//! engine" — this module is that deferred piece, at the pure-data-model layer;
//! the actual CRUD-op JSON decode lives in `zero-cache-mutagen` (which already
//! owns `CrudOp`), not here — `CrudMutation::ops_json` carries the raw ops
//! array un-decoded so this crate doesn't take on `zero-cache-mutagen` as a
//! dependency (mutagen already depends on protocol; the reverse would cycle).

use zero_cache_shared::bigint_json::JsonValue;

use crate::mutation_id::MutationId;
use crate::mutation_result::MutationResponse;

/// Name of the internal custom mutation the server fire-and-forgets to the
/// app's push endpoint to prune stored mutation-result rows up to an acked
/// mutation ID. Port of `zero-protocol/src/mutation.ts`'s
/// `CLEANUP_RESULTS_MUTATION_NAME`.
pub const CLEANUP_RESULTS_MUTATION_NAME: &str = "_zero_cleanupResults";

/// Port of `crudMutationSchema`. `ops_json` is `args[0].ops` — the raw
/// (still-JSON) CRUD op array; see module doc for why it isn't decoded here.
#[derive(Debug, Clone, PartialEq)]
pub struct CrudMutation {
    pub id: f64,
    pub client_id: String,
    pub ops_json: JsonValue,
    pub timestamp: f64,
}

/// Port of `customMutationSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct CustomMutation {
    pub id: f64,
    pub client_id: String,
    pub name: String,
    pub args: Vec<JsonValue>,
    pub timestamp: f64,
}

/// Port of `mutationSchema` (`crudMutationSchema | customMutationSchema`).
#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    Crud(CrudMutation),
    Custom(CustomMutation),
}

impl Mutation {
    pub fn id(&self) -> MutationId {
        match self {
            Mutation::Crud(m) => MutationId {
                id: m.id,
                client_id: m.client_id.clone(),
            },
            Mutation::Custom(m) => MutationId {
                id: m.id,
                client_id: m.client_id.clone(),
            },
        }
    }
}

/// Port of `pushBodySchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct PushBody {
    pub client_group_id: String,
    pub mutations: Vec<Mutation>,
    pub push_version: f64,
    pub schema_version: Option<f64>,
    pub timestamp: f64,
    pub request_id: String,
    pub traceparent: Option<String>,
}

/// Port of `pushOkSchema` (the non-deprecated half of `pushResponseBodySchema`
/// — the deprecated top-level `pushErrorSchema` variants are not modeled,
/// matching upstream's own steer toward `['error', {...}]` messages instead).
#[derive(Debug, Clone, PartialEq)]
pub struct PushOk {
    pub mutations: Vec<MutationResponse>,
}

/// Port of `ackMutationResponsesMessageSchema`'s body: the `MutationID` being
/// acknowledged by the client so the pusher can clean up stored responses.
#[derive(Debug, Clone, PartialEq)]
pub struct AckMutationResponsesBody {
    pub mutation_id: MutationId,
}
