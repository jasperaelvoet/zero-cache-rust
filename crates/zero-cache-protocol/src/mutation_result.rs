//! Port of the `MutationResult`/`MutationResponse` portion of
//! `zero-protocol/src/mutation.ts` — the piece `mutations-patch.ts` (and so
//! `poke.ts`) needs. Does not port the mutation *request* types
//! (`CRUDMutation`/`CustomMutation`/`mapCRUD`) — those belong to the
//! unported mutagen/pusher.ts mutation-ingestion engine, not the downstream
//! sync protocol this module serves.

use zero_cache_shared::bigint_json::JsonValue;

use crate::mutation_id::MutationId;

/// A successful mutation's result. Port of `MutationOk` (`{data?: JSON}`).
#[derive(Debug, Clone, PartialEq)]
pub struct MutationOk {
    pub data: Option<JsonValue>,
}

/// An app-level mutation failure (`ApplicationError`, `error: 'app'`). Port
/// of the `appErrorSchema` variant of `MutationError`.
#[derive(Debug, Clone, PartialEq)]
pub struct MutationAppError {
    pub message: Option<String>,
    pub details: Option<JsonValue>,
}

/// A zero-cache-level mutation failure — out-of-order or already-processed.
/// Port of the `zeroErrorSchema` variant of `MutationError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZeroErrorKind {
    /// Deprecated: push oooMutation errors are now represented as `['error',
    /// {...}]` messages, kept only for wire compatibility.
    OooMutation,
    AlreadyProcessed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MutationZeroError {
    pub error: ZeroErrorKind,
    pub details: Option<JsonValue>,
}

/// Port of `MutationError` (`appErrorSchema | zeroErrorSchema`).
#[derive(Debug, Clone, PartialEq)]
pub enum MutationError {
    App(MutationAppError),
    Zero(MutationZeroError),
}

/// Port of `MutationResult` (`mutationOkSchema | mutationErrorSchema`).
#[derive(Debug, Clone, PartialEq)]
pub enum MutationResult {
    Ok(MutationOk),
    Error(MutationError),
}

/// Port of `MutationResponse`.
#[derive(Debug, Clone, PartialEq)]
pub struct MutationResponse {
    pub id: MutationId,
    pub result: MutationResult,
}
