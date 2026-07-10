//! Port of `zero-protocol/src/up.ts`.
//!
//! The upstream (client->server) message vocabulary — symmetric to
//! `poke.rs`'s downstream messages. Scope: covers everything with a pure
//! data shape already ported, including `push` (see `push.rs`/`push_json.rs`
//! — the CRUD-mutation submission path; the ops array itself is decoded by
//! `zero-cache-mutagen`, which already owns `CrudOp`) and `pull` (mutation
//! recovery request shape), `updateAuth`, `ackMutationResponses`, and
//! `inspect` (debug tooling request shape).

use crate::change_desired_queries::ChangeDesiredQueriesBody;
use crate::close_connection::CloseConnectionBody;
use crate::connect::InitConnectionBody;
use crate::delete_clients::DeleteClientsBody;
use crate::inspect_up::InspectUpBody;
use crate::ping::PingBody;
use crate::pull::PullRequestBody;
use crate::push::{AckMutationResponsesBody, PushBody};
use crate::update_auth::UpdateAuthBody;

/// Port of `Upstream`, restricted to the variants this crate has ported the
/// body type for (see module doc).
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum Upstream {
    InitConnection(InitConnectionBody),
    Ping(PingBody),
    DeleteClients(DeleteClientsBody),
    ChangeDesiredQueries(ChangeDesiredQueriesBody),
    Pull(PullRequestBody),
    Push(PushBody),
    UpdateAuth(UpdateAuthBody),
    AckMutationResponses(AckMutationResponsesBody),
    Inspect(InspectUpBody),
    /// Deprecated; kept for wire compatibility with older clients.
    CloseConnection(CloseConnectionBody),
}
