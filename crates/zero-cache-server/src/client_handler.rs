//! The first LIVE wiring of `ClientHandler`'s `PokeHandler` — composing
//! `zero-cache-view-syncer::client_handler_poke`'s pure decisions
//! (`should_send_poke`/`should_include_patch`/`should_flush_poke_part`/
//! `decide_poke_end`) and `client_handler_row_patch::make_row_patch` with
//! the real wire types (`zero_cache_protocol::poke`) and a REAL
//! `WsConnection` (this crate's `tokio-tungstenite` layer) to actually
//! send `pokeStart`/`pokePart`/`pokeEnd` messages over a live socket.
//!
//! Both patch kinds `addPatch` handles are now wired: row patches (via
//! `make_row_patch`) and query (config) patches — `desiredQueriesPatches`
//! (keyed by client id) for a client-scoped desired-query change, or
//! `gotQueriesPatch` for a client-group-wide "got" change, matching
//! upstream's `patch.clientID ? desiredQueriesPatches[...] :
//! gotQueriesPatch` branch exactly. `PokePartBody`'s wire serialization
//! for these two fields was added to `zero_cache_protocol::poke_json`
//! alongside this (previously only `rowsPatch` was covered there).
//!
//! `lastMutationIDChanges`/`mutationsPatch` (both `'put'` and `'del'`) are
//! now fully wired too, via row-table classification against
//! `zero_clients_table`/`zero_mutations_table` (constructor parameters,
//! matching upstream's `${upstreamSchema(shard)}.clients`/`.mutations`
//! construction). `#updateLMIDs`'s logic lives in
//! `client_handler_row_patch::update_lmids`; the mutations-table id/result
//! parsing in `parse_mutation_del_id`/`parse_mutation_put`. `addPatch` now
//! handles every branch upstream's does except metrics
//! (`#pokeTime`/`#pokeTransactions`/`#pokedRows`), which are NOT ported.

use zero_cache_protocol::mutation_id::MutationId;
use zero_cache_protocol::mutations_patch::{MutationDelOp, MutationPatchOp, MutationPutOp};
use zero_cache_protocol::poke::{PokeEndBody, PokeMessage, PokePartBody, PokeStartBody};
use zero_cache_protocol::poke_json::poke_message_json;
use zero_cache_protocol::queries_patch::{QueriesDelOp, QueriesPatchOp, QueriesPutOp};
use zero_cache_protocol::row_patch::RowPatchOp;
use zero_cache_view_syncer::client_handler_poke::{
    decide_poke_end, should_flush_poke_part, should_include_patch, should_send_poke, PokeEndAction,
};
use zero_cache_view_syncer::client_handler_row_patch::{
    make_row_patch, parse_mutation_del_id, parse_mutation_put, update_lmids, LmidUpdate,
};
use zero_cache_view_syncer::client_patch::{ClientRowPatch, Patch, PatchToVersion};
use zero_cache_view_syncer::cvr_types::PatchOp;
use zero_cache_view_syncer::cvr_version::{
    version_to_cookie, version_to_nullable_cookie, CvrVersion, NullableCvrVersion, VersionError,
};

use crate::ws_connection::{WsConnection, WsConnectionError};

/// Errors from a live poke cycle.
#[derive(Debug, thiserror::Error)]
pub enum PokeError {
    #[error(transparent)]
    Ws(#[from] WsConnectionError),
    #[error(transparent)]
    Version(#[from] VersionError),
    #[error("add_patch called on an already-ended poke")]
    PokeAlreadyEnded,
    #[error("Patches were sent but finalVersion is not greater than baseVersion")]
    InvalidPokeEndVersion,
    #[error(
        "malformed clients-table row (missing/invalid clientGroupID, clientID, or lastMutationID)"
    )]
    MalformedClientsRow,
    #[error("malformed mutations-table row (missing/invalid clientID/mutationID/result)")]
    MalformedMutationsRow,
}

/// A `ClientHandler`-equivalent driving real `pokeStart`/`pokePart`/
/// `pokeEnd` sends over a live `WsConnection`. Port of the state
/// `ClientHandler` tracks across poke cycles (`#baseVersion`/`#everPoked`)
/// — see module doc for what's not ported.
pub struct ClientHandler {
    ws_id: String,
    client_group_id: String,
    zero_clients_table: String,
    zero_mutations_table: String,
    base_version: NullableCvrVersion,
    ever_poked: bool,
}

/// One in-progress poke transaction. Port of `startPoke`'s closure-captured
/// local state (`pokeStarted`/`body`/`partCount`), now real fields since
/// Rust has no closures-capturing-mutable-locals-across-async-calls
/// pattern as ergonomic as JS's here.
pub struct PokeCycle<'a> {
    conn: &'a mut WsConnection,
    poke_id: String,
    client_group_id: String,
    zero_clients_table: String,
    zero_mutations_table: String,
    base_version: NullableCvrVersion,
    poke_started: bool,
    body: Option<PokePartBody>,
    part_count: u32,
    ended: bool,
}

impl ClientHandler {
    pub fn new(
        ws_id: impl Into<String>,
        client_group_id: impl Into<String>,
        zero_clients_table: impl Into<String>,
        zero_mutations_table: impl Into<String>,
        base_version: NullableCvrVersion,
    ) -> Self {
        ClientHandler {
            ws_id: ws_id.into(),
            client_group_id: client_group_id.into(),
            zero_clients_table: zero_clients_table.into(),
            zero_mutations_table: zero_mutations_table.into(),
            base_version,
            ever_poked: false,
        }
    }

    /// Port of `ClientHandler#startPoke`. Returns `None` if the client is
    /// already caught up (matching upstream's `NOOP` `PokeHandler`) — the
    /// caller sends nothing at all in that case.
    pub fn start_poke<'a>(
        &mut self,
        conn: &'a mut WsConnection,
        tentative_version: &CvrVersion,
    ) -> Option<PokeCycle<'a>> {
        if !should_send_poke(&self.base_version, tentative_version, self.ever_poked) {
            return None;
        }
        Some(PokeCycle {
            conn,
            poke_id: version_to_cookie(tentative_version)
                .expect("tentative_version always encodes"),
            client_group_id: self.client_group_id.clone(),
            zero_clients_table: self.zero_clients_table.clone(),
            zero_mutations_table: self.zero_mutations_table.clone(),
            base_version: self.base_version.clone(),
            poke_started: false,
            body: None,
            part_count: 0,
            ended: false,
        })
    }

    /// Called once a `PokeCycle` finishes, to commit its effect on this
    /// handler's tracked state — port of `end`'s `this.#baseVersion =
    /// finalVersion; this.#everPoked = true;` side effects, kept out of
    /// `PokeCycle` itself since it doesn't own the `ClientHandler`.
    pub fn commit_poke(&mut self, final_version: CvrVersion) {
        self.base_version = Some(final_version);
        self.ever_poked = true;
    }

    pub fn ws_id(&self) -> &str {
        &self.ws_id
    }
}

impl PokeCycle<'_> {
    async fn ensure_started(&mut self) -> Result<(), PokeError> {
        if !self.poke_started {
            let start = PokeStartBody {
                poke_id: self.poke_id.clone(),
                base_cookie: version_to_nullable_cookie(&self.base_version)?,
                schema_versions: None,
                timestamp: None,
            };
            self.conn
                .send_json(&poke_message_json(&PokeMessage::Start(start)))
                .await?;
            self.poke_started = true;
        }
        Ok(())
    }

    fn body_mut(&mut self) -> &mut PokePartBody {
        self.body.get_or_insert_with(|| PokePartBody {
            poke_id: self.poke_id.clone(),
            ..Default::default()
        })
    }

    async fn flush_body(&mut self) -> Result<(), PokeError> {
        if let Some(body) = self.body.take() {
            self.conn
                .send_json(&poke_message_json(&PokeMessage::Part(body)))
                .await?;
            self.part_count = 0;
        }
        Ok(())
    }

    /// Port of the returned `PokeHandler.addPatch`'s `'row'`/`'query'`
    /// branches — see module doc for what's still excluded.
    pub async fn add_patch(&mut self, patch_to_version: PatchToVersion) -> Result<(), PokeError> {
        if self.ended {
            return Err(PokeError::PokeAlreadyEnded);
        }
        if !should_include_patch(&patch_to_version.to_version, &self.base_version) {
            return Ok(());
        }

        self.ensure_started().await?;
        match &patch_to_version.patch {
            Patch::Row(row_patch) => {
                let table = match row_patch {
                    ClientRowPatch::Put(p) => &p.id.table,
                    ClientRowPatch::Delete(p) => &p.id.table,
                };
                if *table == self.zero_clients_table {
                    // Port of `#updateLMIDs`: only 'put' (i.e. `ClientRowPatch::Put`)
                    // carries anything to extract; 'del'/'constrain' are a
                    // documented no-op upstream.
                    if let ClientRowPatch::Put(p) = row_patch {
                        match update_lmids(&self.client_group_id, &p.contents)
                            .map_err(|_| PokeError::MalformedClientsRow)?
                        {
                            LmidUpdate::Change {
                                client_id,
                                last_mutation_id,
                            } => {
                                self.body_mut()
                                    .last_mutation_id_changes
                                    .get_or_insert_with(std::collections::BTreeMap::new)
                                    .insert(client_id, last_mutation_id);
                            }
                            LmidUpdate::IgnoredWrongClientGroup { .. } => {} // matches upstream's log-and-ignore
                        }
                    }
                } else if *table == self.zero_mutations_table {
                    match row_patch {
                        ClientRowPatch::Put(p) => {
                            let mutation = parse_mutation_put(&p.contents)
                                .map_err(|_| PokeError::MalformedMutationsRow)?;
                            self.body_mut()
                                .mutations_patch
                                .get_or_insert_with(Vec::new)
                                .push(MutationPatchOp::Put(MutationPutOp { mutation }));
                        }
                        ClientRowPatch::Delete(p) => {
                            let (client_id, mutation_id) = parse_mutation_del_id(&p.id.row_key)
                                .map_err(|_| PokeError::MalformedMutationsRow)?;
                            self.body_mut()
                                .mutations_patch
                                .get_or_insert_with(Vec::new)
                                .push(MutationPatchOp::Del(MutationDelOp {
                                    id: MutationId {
                                        id: mutation_id,
                                        client_id,
                                    },
                                }));
                        }
                    }
                } else {
                    let wire_patch: RowPatchOp = make_row_patch(row_patch);
                    self.body_mut()
                        .rows_patch
                        .get_or_insert_with(Vec::new)
                        .push(wire_patch);
                }
            }
            Patch::Config(query_patch) => {
                let op = match query_patch.op {
                    PatchOp::Put => QueriesPatchOp::Put(QueriesPutOp {
                        hash: query_patch.id.clone(),
                        ttl: None,
                    }),
                    PatchOp::Del => QueriesPatchOp::Del(QueriesDelOp {
                        hash: query_patch.id.clone(),
                    }),
                };
                match &query_patch.client_id {
                    Some(client_id) => self
                        .body_mut()
                        .desired_queries_patches
                        .get_or_insert_with(std::collections::BTreeMap::new)
                        .entry(client_id.clone())
                        .or_default()
                        .push(op),
                    None => self
                        .body_mut()
                        .got_queries_patch
                        .get_or_insert_with(Vec::new)
                        .push(op),
                }
            }
        }

        self.part_count += 1;
        if should_flush_poke_part(self.part_count) {
            self.flush_body().await?;
        }
        Ok(())
    }

    /// Port of `PokeHandler.cancel`.
    pub async fn cancel(mut self) -> Result<(), PokeError> {
        if self.poke_started {
            let end = PokeEndBody {
                poke_id: self.poke_id.clone(),
                cookie: String::new(),
                cancel: Some(true),
            };
            self.conn
                .send_json(&poke_message_json(&PokeMessage::End(end)))
                .await?;
        }
        self.ended = true;
        Ok(())
    }

    /// Port of `PokeHandler.end`. Returns `true` if anything was actually
    /// sent (matching `ClientHandler`'s subsequent `#baseVersion`/
    /// `#everPoked` commit only mattering when something happened —
    /// callers should call `ClientHandler::commit_poke` regardless per
    /// upstream, which unconditionally commits even on the `Noop` path
    /// having done nothing observable).
    pub async fn end(
        mut self,
        final_version: &CvrVersion,
        force_initial_poke: bool,
    ) -> Result<bool, PokeError> {
        let action = decide_poke_end(
            self.poke_started,
            &self.base_version,
            final_version,
            force_initial_poke,
        )
        .map_err(|_| PokeError::InvalidPokeEndVersion)?;

        match action {
            PokeEndAction::Noop => {
                self.ended = true;
                return Ok(false);
            }
            PokeEndAction::SendPokeStartFirst => {
                let start = PokeStartBody {
                    poke_id: self.poke_id.clone(),
                    base_cookie: version_to_nullable_cookie(&self.base_version)?,
                    schema_versions: None,
                    timestamp: None,
                };
                self.conn
                    .send_json(&poke_message_json(&PokeMessage::Start(start)))
                    .await?;
                self.poke_started = true;
            }
            PokeEndAction::ProceedToEnd => {}
        }

        self.flush_body().await?;
        let end = PokeEndBody {
            poke_id: self.poke_id.clone(),
            cookie: version_to_cookie(final_version).expect("final_version always encodes"),
            cancel: None,
        };
        self.conn
            .send_json(&poke_message_json(&PokeMessage::End(end)))
            .await?;
        self.ended = true;
        Ok(true)
    }
}
