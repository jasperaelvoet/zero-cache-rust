//! The per-client-group processing loop (redesign §6, group-loop plan
//! increment 2) — the structural core of client-group ownership on the
//! `ZERO_GROUP_OWNERSHIP` (flag-on) path.
//!
//! Upstream serves a client group from ONE `ViewSyncerService`: one CVR, one
//! operator graph, and a single run loop that, on each replica commit, advances
//! the group pipeline ONCE and fans the resulting poke to every connected
//! client (`startPoke(clients, newVersion)`). This module is that loop.
//!
//! A [`GroupProcessor`] task is spawned lazily on the group's first attach and
//! owns:
//!
//! - ONE [`GroupTransitionCore`] — the group CVR lives HERE, in the loop, so a
//!   commit does exactly one advance + one CVR transition + one durable flush
//!   (the [`CvrPersistence`] CAS remains the cross-node guard). No per-connection
//!   `GroupCvrCell` check-out/clone.
//! - ONE [`FanoutSubscriber`] — the group's single subscription to the replica
//!   fan-out (not one per connection).
//! - a map of attached connections, each with its own outbound frame channel and
//!   its own [`ConnectionPokeState`] (the per-connection delivery cursors
//!   `build_poke_frames` consumes).
//!
//! The loop replaces the per-connection commit relay (`serve_synced_connection`'s
//! commit branch × every connection's own advance + CVR transition), which
//! processed each commit N times for N connections.
//!
//! ## Transformation-hash keying
//!
//! The port reuses a query's wire hash as its transformation hash, so two
//! connections sending the same hash but resolving to DIFFERENT transformed ASTs
//! (the theoretical per-connection read-permission case) could otherwise
//! silently share one pipeline. Within a client GROUP this cannot actually
//! happen — a group is a single auth context, so custom-query transforms
//! (pure functions of name+args) and read permissions (bound to the group-wide
//! auth data) resolve identically for every connection. As defense in depth the
//! loop records a transform fingerprint per active query hash and, on the first
//! divergent fingerprint, LOUDLY resets the shared query
//! ([`SharedGroupPipeline::reset_query`]) so it re-hydrates from the new AST —
//! it can never keep serving rows built from a different transform.
//!
//! [`CvrPersistence`]: crate::live_connection::CvrPersistence
//! [`SharedGroupPipeline::reset_query`]: zero_cache_view_syncer::group_shared_pipeline::SharedGroupPipeline::reset_query

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::{mpsc, oneshot};

use zero_cache_protocol::ast::Ast;
use zero_cache_protocol::poke::{PokeEndBody, PokeMessage, PokePartBody, PokeStartBody};
use zero_cache_protocol::poke_json::poke_message_json;
use zero_cache_protocol::queries_patch::UpQueriesPatchOp;
use zero_cache_protocol::row_patch::Row;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_sqlite::change_fanout::{FanoutEvent, FanoutSubscriber};
use zero_cache_view_syncer::client_handler_poke::should_include_patch;
use zero_cache_view_syncer::client_patch::PatchToVersion;
use zero_cache_view_syncer::cvr_types::{Cvr, RowId, RowRecord};
use zero_cache_view_syncer::cvr_updater::ensure_new_version;
use zero_cache_view_syncer::cvr_version::{
    cookie_to_version, version_to_cookie, version_to_nullable_cookie, CvrVersion,
};

use crate::group_transition::{GroupTransitionCore, StagedPatch};
use crate::serve_connection::HandlerOutcome;

/// The per-connection delivery state a poke is built against — the fields that
/// separate cleanly from the group-scoped [`GroupTransitionCore`]. Each attached
/// connection owns one; the loop threads it through [`build_poke_frames`].
pub(crate) struct ConnectionPokeState {
    /// Monotonic poke counter for this connection (`poke{n}` ids).
    pub(crate) poke_seq: u64,
    /// The cookie the client presented at connect — the base of its FIRST poke.
    pub(crate) initial_base_version: Option<CvrVersion>,
    /// The cookie last advertised on this socket; the next poke chains from it.
    pub(crate) last_poke_version: Option<CvrVersion>,
    /// Last-mutation-ids already delivered on this connection (delta cursor).
    pub(crate) poked_last_mutation_ids: std::collections::BTreeMap<String, i64>,
}

impl ConnectionPokeState {
    fn new(initial_base_version: Option<CvrVersion>) -> Self {
        ConnectionPokeState {
            poke_seq: 0,
            initial_base_version,
            last_poke_version: None,
            poked_last_mutation_ids: std::collections::BTreeMap::new(),
        }
    }
}

/// One attached connection: its outbound frame channel and poke cursors.
struct ConnectionHandle {
    writer_tx: mpsc::UnboundedSender<Vec<String>>,
    poke: ConnectionPokeState,
}

/// A group CVR/row snapshot handed back for a per-connection `inspect`.
pub(crate) struct InspectSnapshot {
    pub(crate) cvr: Cvr,
    pub(crate) row_records: Vec<RowRecord>,
    pub(crate) row_bodies: Vec<(RowId, Row)>,
}

/// A command submitted to a group's processor loop. Every command that produces
/// wire frames delivers them through the target connection's `writer_tx`; the
/// `reply` one-shot only signals the loop has finished processing (so the
/// connection preserves ordering relative to its next client frame).
pub(crate) enum GroupCommand {
    Attach {
        client_id: String,
        base_cookie: Option<CvrVersion>,
        writer_tx: mpsc::UnboundedSender<Vec<String>>,
        reply: oneshot::Sender<()>,
    },
    Detach {
        client_id: String,
    },
    ChangeDesiredQueries {
        client_id: String,
        patch: Vec<UpQueriesPatchOp>,
        resolved_asts: HashMap<String, Option<Ast>>,
        /// Normalized client-schema JSON to record on the group CVR (Initialize
        /// only; `None` for `changeDesiredQueries`).
        client_schema: Option<JsonValue>,
        force: bool,
        reply: oneshot::Sender<()>,
    },
    InspectSnapshot {
        reply: oneshot::Sender<InspectSnapshot>,
    },
}

/// A `Send + Clone` handle to a group's processor loop. Connections hold a clone
/// and submit [`GroupCommand`]s; the loop exits when the last handle drops (its
/// command channel closes). The registry keeps only a [`Weak`] reference so a
/// group is torn down when its last connection disconnects.
#[derive(Clone)]
pub(crate) struct GroupProcessorHandle {
    inner: Arc<GroupProcessorInner>,
}

struct GroupProcessorInner {
    commands: mpsc::UnboundedSender<GroupCommand>,
}

impl GroupProcessorHandle {
    fn send(&self, command: GroupCommand) -> Result<(), ()> {
        self.inner.commands.send(command).map_err(|_| ())
    }

    /// Registers a connection with the loop, handing it the connection's
    /// outbound frame channel and its connect base cookie. Awaits the loop
    /// acknowledging the attach so the connection is known before its first
    /// desired-queries change.
    pub(crate) async fn attach(
        &self,
        client_id: String,
        base_cookie: Option<CvrVersion>,
        writer_tx: mpsc::UnboundedSender<Vec<String>>,
    ) -> Result<(), ()> {
        let (reply, rx) = oneshot::channel();
        self.send(GroupCommand::Attach {
            client_id,
            base_cookie,
            writer_tx,
            reply,
        })?;
        rx.await.map_err(|_| ())
    }

    pub(crate) fn detach(&self, client_id: String) {
        let _ = self.send(GroupCommand::Detach { client_id });
    }

    /// Applies a connection's desired-queries patch through the group loop. The
    /// resulting config + hydration pokes are pushed into the connection's
    /// writer channel by the loop; this awaits the transition completing.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn change_desired_queries(
        &self,
        client_id: String,
        patch: Vec<UpQueriesPatchOp>,
        resolved_asts: HashMap<String, Option<Ast>>,
        client_schema: Option<JsonValue>,
        force: bool,
    ) -> Result<(), ()> {
        let (reply, rx) = oneshot::channel();
        self.send(GroupCommand::ChangeDesiredQueries {
            client_id,
            patch,
            resolved_asts,
            client_schema,
            force,
            reply,
        })?;
        rx.await.map_err(|_| ())
    }

    pub(crate) async fn inspect_snapshot(&self) -> Result<InspectSnapshot, ()> {
        let (reply, rx) = oneshot::channel();
        self.send(GroupCommand::InspectSnapshot { reply })?;
        rx.await.map_err(|_| ())
    }
}

/// Process-wide `clientGroupID -> GroupProcessorHandle` registry (server-side
/// companion to view-syncer's `ClientGroupRegistry`). A live handle exists iff
/// the group's loop is running; connections reuse it, and it is reaped once the
/// loop exits.
#[derive(Default)]
pub(crate) struct GroupProcessorRegistry {
    inner: Mutex<HashMap<String, Weak<GroupProcessorInner>>>,
}

impl GroupProcessorRegistry {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// Returns the group's processor handle, spawning the loop from `spawn` on
    /// the first connection of the group. `spawn` produces the loop's owned
    /// inputs (core + subscriber) and is only invoked when no live loop exists.
    pub(crate) fn get_or_spawn(
        &self,
        group_id: &str,
        spawn: impl FnOnce() -> GroupProcessorSpawn,
    ) -> GroupProcessorHandle {
        let mut map = self.lock();
        if let Some(existing) = map.get(group_id).and_then(Weak::upgrade) {
            return GroupProcessorHandle { inner: existing };
        }
        let GroupProcessorSpawn { core, subscriber } = spawn();
        let (commands, commands_rx) = mpsc::unbounded_channel();
        let inner = Arc::new(GroupProcessorInner { commands });
        let group_id_owned = group_id.to_string();
        tokio::spawn(async move {
            let mut processor = GroupProcessor::new(core, subscriber, group_id_owned);
            processor.run(commands_rx).await;
        });
        map.insert(group_id.to_string(), Arc::downgrade(&inner));
        map.retain(|_, weak| weak.strong_count() > 0);
        GroupProcessorHandle { inner }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Weak<GroupProcessorInner>>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The owned inputs a spawned group loop takes over.
pub(crate) struct GroupProcessorSpawn {
    pub(crate) core: GroupTransitionCore,
    pub(crate) subscriber: FanoutSubscriber,
}

/// The per-group processing loop itself.
pub(crate) struct GroupProcessor {
    core: GroupTransitionCore,
    subscriber: FanoutSubscriber,
    connections: HashMap<String, ConnectionHandle>,
    group_id: String,
    /// Transform fingerprint per active query hash (the transformation-hash
    /// guard; see the module doc).
    transform_fingerprints: HashMap<String, u64>,
    /// Instrumentation: number of commit-driven pipeline advances the loop has
    /// performed. Tests pin this to prove commits are processed ONCE per group,
    /// not once per connection.
    advance_count: Arc<AtomicU64>,
}

impl GroupProcessor {
    fn new(core: GroupTransitionCore, subscriber: FanoutSubscriber, group_id: String) -> Self {
        GroupProcessor {
            core,
            subscriber,
            connections: HashMap::new(),
            group_id,
            transform_fingerprints: HashMap::new(),
            advance_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// A shared handle to the advance counter, for test instrumentation.
    #[cfg(test)]
    fn advance_counter(&self) -> Arc<AtomicU64> {
        self.advance_count.clone()
    }

    async fn run(&mut self, mut commands: mpsc::UnboundedReceiver<GroupCommand>) {
        loop {
            tokio::select! {
                // Prefer commands (attach / desired-queries / inspect) over commit
                // events so an init transition is applied promptly rather than
                // after a burst of commits — keeping hydration latency low.
                biased;
                command = commands.recv() => {
                    // All handles dropped: the group's last connection is gone.
                    let Some(command) = command else { break };
                    self.handle_command(command).await;
                }
                event = self.subscriber.recv() => {
                    match event {
                        FanoutEvent::Commit(_) | FanoutEvent::Lagged { .. } => {
                            // Coalesce a burst: `advance()` always leapfrogs to
                            // the replica's current head, so draining the queued
                            // notifications and advancing ONCE catches up every
                            // pending commit (the same coalescing the per-
                            // connection relay does at serve_connection.rs).
                            while let Some(pending) = self.subscriber.try_recv() {
                                if matches!(pending, FanoutEvent::Closed) {
                                    break;
                                }
                            }
                            self.process_commit().await;
                        }
                        FanoutEvent::Closed => {
                            // The replicator stopped; keep serving current views.
                        }
                    }
                }
            }
        }
    }

    async fn handle_command(&mut self, command: GroupCommand) {
        match command {
            GroupCommand::Attach {
                client_id,
                base_cookie,
                writer_tx,
                reply,
            } => {
                self.connections.insert(
                    client_id,
                    ConnectionHandle {
                        writer_tx,
                        poke: ConnectionPokeState::new(base_cookie),
                    },
                );
                let _ = reply.send(());
            }
            GroupCommand::Detach { client_id } => {
                self.connections.remove(&client_id);
                // Drop this client's solely-desired queries from the shared
                // pipeline; the CVR keeps its desired records for reconnect.
                if let Some(pipeline) = self.core.query_pipeline.as_ref() {
                    pipeline.remove_group_client(&client_id);
                }
            }
            GroupCommand::ChangeDesiredQueries {
                client_id,
                patch,
                resolved_asts,
                client_schema,
                force,
                reply,
            } => {
                self.process_desired_queries(
                    &client_id,
                    &patch,
                    &resolved_asts,
                    client_schema,
                    force,
                )
                .await;
                let _ = reply.send(());
            }
            GroupCommand::InspectSnapshot { reply } => {
                let snapshot = InspectSnapshot {
                    cvr: self.core.cvr_handler.cvr.clone(),
                    row_records: (*self.core.row_records).clone(),
                    row_bodies: (*self.core.row_bodies).clone(),
                };
                let _ = reply.send(snapshot);
            }
        }
    }

    /// Commit path: ONE advance + ONE CVR transition + ONE durable flush, then a
    /// row/LMID poke fanned to every attached connection (upstream
    /// `startPoke(clients, newVersion)`).
    async fn process_commit(&mut self) {
        if self.connections.is_empty() {
            // No one to poke; the pipeline catches up on the next hydration's
            // advance-to-head. Skip the flush entirely.
            return;
        }
        let before = self.core.cvr_handler.cvr.clone();
        let orig_version = self.core.cvr_handler.version().clone();
        // Re-executing unchanged queries doesn't bump the CVR version on its own;
        // the row-processing path needs the version above `orig` before any row
        // is emitted. Bump once up front (matches `rehydrate_tracked`).
        ensure_new_version(&orig_version, &mut self.core.cvr_handler.cvr.version);
        self.advance_count.fetch_add(1, Ordering::Relaxed);
        let patches = match self.core.advance_group_pipeline_to_patches() {
            Ok(patches) => patches,
            Err(error) => {
                crate::warn!("group {} advance failed: {error}", self.group_id);
                return;
            }
        };
        // Persist BEFORE poking: a client is only told it is at a version once
        // that version is durably committed (the CAS is the cross-node guard).
        if let Err(error) = self.core.persist_transition(&before).await {
            // On a single node the CAS cannot lose; a lost CAS on a multi-node
            // deployment means another node owns the newer CVR. We do not fan a
            // poke for an un-committed transition (full reconciliation is a
            // later increment).
            crate::warn!(
                "group {} commit flush failed (poke withheld): {error}",
                self.group_id
            );
            return;
        }
        self.core.refresh_last_mutation_ids();
        self.fan_to_all(&orig_version, &patches);
    }

    /// Desired-queries path for one connection: apply + hydrate the group CVR
    /// ONCE, push the config poke then the staged hydration poke into the
    /// REQUESTER's writer (same two-frame FIFO order as `take_pending_hydration`),
    /// and fan the got/row patches to the OTHER connections (group semantics).
    async fn process_desired_queries(
        &mut self,
        client_id: &str,
        patch: &[UpQueriesPatchOp],
        resolved_asts: &HashMap<String, Option<Ast>>,
        client_schema: Option<JsonValue>,
        force: bool,
    ) {
        if !self.connections.contains_key(client_id) {
            return;
        }
        self.core.set_active_client(client_id);
        if let Some(schema) = client_schema {
            let _ = zero_cache_view_syncer::cvr_client_state::set_client_schema(
                &mut self.core.cvr_handler.cvr,
                &schema,
            );
        }
        self.guard_transform_divergence(patch, resolved_asts);

        let before = self.core.cvr_handler.cvr.clone();
        let staged = match self.core.apply_desired_patch_staged(patch, resolved_asts) {
            Ok(staged) => staged,
            Err(error) => {
                crate::warn!(
                    "group {} desired-queries apply failed: {error}",
                    self.group_id
                );
                if let Some(handle) = self.connections.get(client_id) {
                    let _ = handle.writer_tx.send(vec![persistence_error_frame(&error)]);
                }
                return;
            }
        };
        if let Err(error) = self.core.persist_transition(&before).await {
            crate::warn!(
                "group {} desired-queries flush failed (poke withheld): {error}",
                self.group_id
            );
            return;
        }
        self.core.refresh_last_mutation_ids();

        let StagedPatch {
            orig_version,
            config_version,
            config,
            hydration,
        } = staged;

        // Requester: config poke, then chained hydration poke (or one merged
        // poke when either half is empty) — byte-for-byte the staged handoff.
        let mut requester_frames: Vec<String> = Vec::new();
        if config.is_empty() || hydration.is_empty() {
            let mut merged = config;
            if hydration.is_empty() && !merged.is_empty() {
                self.core.cvr_handler.cvr.version = config_version.clone();
            }
            merged.extend(hydration.clone());
            if let Some(handle) = self.connections.get_mut(client_id) {
                let outcome = build_poke_frames(
                    &mut handle.poke,
                    &self.core,
                    orig_version.clone(),
                    merged,
                    force,
                );
                requester_frames.extend(outcome.responses);
            }
        } else if let Some(handle) = self.connections.get_mut(client_id) {
            let config_outcome = build_poke_frames(
                &mut handle.poke,
                &self.core,
                orig_version.clone(),
                config,
                force,
            );
            requester_frames.extend(config_outcome.responses);
            let hydration_outcome = build_poke_frames(
                &mut handle.poke,
                &self.core,
                config_version.clone(),
                hydration.clone(),
                false,
            );
            requester_frames.extend(hydration_outcome.responses);
        }
        if let Some(handle) = self.connections.get(client_id) {
            if !requester_frames.is_empty() {
                let _ = handle.writer_tx.send(requester_frames);
            }
        }

        // Every OTHER connection in the group receives the row/got patches (the
        // config/desired patches are the requester's alone). The per-connection
        // base-cookie filter drops anything a connection already holds.
        if !hydration.is_empty() {
            let hydration_base = config_version;
            let others: Vec<String> = self
                .connections
                .keys()
                .filter(|cid| cid.as_str() != client_id)
                .cloned()
                .collect();
            for other in others {
                if let Some(handle) = self.connections.get_mut(&other) {
                    let outcome = build_poke_frames(
                        &mut handle.poke,
                        &self.core,
                        hydration_base.clone(),
                        hydration.clone(),
                        false,
                    );
                    if !outcome.responses.is_empty() {
                        let _ = handle.writer_tx.send(outcome.responses);
                    }
                }
            }
        }
    }

    /// Fans one poke (built per connection from `patches`) to every attached
    /// connection, dropping closed channels.
    fn fan_to_all(&mut self, orig_version: &CvrVersion, patches: &[PatchToVersion]) {
        let client_ids: Vec<String> = self.connections.keys().cloned().collect();
        for client_id in client_ids {
            if let Some(handle) = self.connections.get_mut(&client_id) {
                let outcome = build_poke_frames(
                    &mut handle.poke,
                    &self.core,
                    orig_version.clone(),
                    patches.to_vec(),
                    false,
                );
                if !outcome.responses.is_empty() {
                    let _ = handle.writer_tx.send(outcome.responses);
                }
            }
        }
    }

    /// Records each put's transform fingerprint; on the first DIVERGENT
    /// fingerprint for an already-active query hash, resets the shared query so
    /// it re-hydrates from the new AST (the module-doc guard). Unreachable in the
    /// single-auth group model; here as defense in depth.
    fn guard_transform_divergence(
        &mut self,
        patch: &[UpQueriesPatchOp],
        resolved_asts: &HashMap<String, Option<Ast>>,
    ) {
        for op in patch {
            let UpQueriesPatchOp::Put(p) = op else {
                continue;
            };
            let fingerprint =
                transform_fingerprint(resolved_asts.get(&p.hash).and_then(|a| a.as_ref()));
            match self.transform_fingerprints.get(&p.hash) {
                Some(existing) if *existing != fingerprint => {
                    crate::warn!(
                        "group {} query {} resolved to a divergent transformed AST; resetting the shared pipeline",
                        self.group_id,
                        p.hash
                    );
                    if let Some(pipeline) = self.core.query_pipeline.as_mut() {
                        pipeline.reset_query(&p.hash);
                    }
                    self.transform_fingerprints
                        .insert(p.hash.clone(), fingerprint);
                }
                Some(_) => {}
                None => {
                    self.transform_fingerprints
                        .insert(p.hash.clone(), fingerprint);
                }
            }
        }
    }
}

/// A stable fingerprint of a resolved (transformed) AST — the JSON string
/// hashed. `None` (an unresolvable put) fingerprints to 0.
fn transform_fingerprint(ast: Option<&Ast>) -> u64 {
    use std::hash::{Hash, Hasher};
    let Some(ast) = ast else { return 0 };
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    // ASTs are not `Hash`; hash their canonical JSON encoding instead.
    zero_cache_protocol::ast_json::ast_to_json(ast)
        .stringify()
        .hash(&mut hasher);
    hasher.finish()
}

fn persistence_error_frame(error: &str) -> String {
    format!(
        r#"["error",{{"kind":"Internal","message":"CVR persistence failed: {}"}}]"#,
        error.replace('"', "\\\"")
    )
}

/// Builds a 3-frame poke for ONE connection from `patches`, advancing that
/// connection's [`ConnectionPokeState`]. A group-loop-local reshaping of
/// `DesiredQueriesHandler::build_poke_outcome` (which drives the flag-OFF path
/// unchanged): it reads the group's already-refreshed last-mutation-ids and CVR
/// version from `core` but keeps all delivery cursors on `poke`, so the loop can
/// fan one group transition out to every connection's own poke chain.
pub(crate) fn build_poke_frames(
    poke: &mut ConnectionPokeState,
    core: &GroupTransitionCore,
    orig_version: CvrVersion,
    patches: Vec<PatchToVersion>,
    force: bool,
) -> HandlerOutcome {
    use std::collections::BTreeMap;

    let base_version = if poke.poke_seq == 0 {
        poke.initial_base_version.clone()
    } else {
        poke.last_poke_version
            .clone()
            .or_else(|| Some(orig_version.clone()))
    };
    let patches: Vec<_> = patches
        .into_iter()
        .filter(|patch| should_include_patch(&patch.to_version, &base_version))
        .collect();
    let lmid_changes: BTreeMap<String, f64> = core
        .last_mutation_ids
        .iter()
        .filter(|(client_id, last_mutation_id)| {
            poke.poked_last_mutation_ids.get(*client_id) != Some(*last_mutation_id)
        })
        .map(|(client_id, last_mutation_id)| (client_id.clone(), *last_mutation_id as f64))
        .collect();

    if patches.is_empty() && lmid_changes.is_empty() && !force {
        return HandlerOutcome::empty();
    }
    let poke_id = {
        poke.poke_seq += 1;
        format!("poke{}", poke.poke_seq)
    };
    let mut poke_msgs = if patches.is_empty() {
        let Ok(base_cookie) = version_to_nullable_cookie(&base_version) else {
            return HandlerOutcome::empty();
        };
        let Ok(cookie) = version_to_cookie(core.cvr_handler.version()) else {
            return HandlerOutcome::empty();
        };
        zero_cache_view_syncer::poke_builder::PokeMessages {
            start: PokeStartBody {
                poke_id: poke_id.clone(),
                base_cookie,
                schema_versions: None,
                timestamp: None,
            },
            part: PokePartBody {
                poke_id: poke_id.clone(),
                last_mutation_id_changes: None,
                desired_queries_patches: None,
                got_queries_patch: None,
                rows_patch: None,
                mutations_patch: None,
            },
            end: PokeEndBody {
                poke_id: poke_id.clone(),
                cookie,
                cancel: None,
            },
        }
    } else {
        let Ok(Some(built)) = zero_cache_view_syncer::poke_builder::build_poke(
            &poke_id,
            &base_version,
            &patches,
            None,
        ) else {
            return HandlerOutcome::empty();
        };
        built
    };
    poke_msgs.part.last_mutation_id_changes = (!lmid_changes.is_empty()).then_some(lmid_changes);
    poke.poked_last_mutation_ids = core.last_mutation_ids.clone();
    let advertised_version = cookie_to_version(Some(&poke_msgs.end.cookie))
        .ok()
        .flatten();
    let start = poke_message_json(&PokeMessage::Start(poke_msgs.start));
    let end = poke_message_json(&PokeMessage::End(poke_msgs.end));
    poke.last_poke_version = advertised_version;
    let mut responses = vec![start];
    if let Some(rows) = poke_msgs.part.rows_patch.clone() {
        // Exact v1.7.0 ClientHandler rule: flush after 100 patches.
        const PART_COUNT_FLUSH_THRESHOLD: usize = 100;
        let leading_patch_count = poke_msgs
            .part
            .desired_queries_patches
            .as_ref()
            .map(|patches| patches.values().map(Vec::len).sum::<usize>())
            .unwrap_or(0)
            + poke_msgs
                .part
                .got_queries_patch
                .as_ref()
                .map(Vec::len)
                .unwrap_or(0)
            + poke_msgs
                .part
                .last_mutation_id_changes
                .as_ref()
                .map(BTreeMap::len)
                .unwrap_or(0)
            + poke_msgs
                .part
                .mutations_patch
                .as_ref()
                .map(Vec::len)
                .unwrap_or(0);
        let mut remaining = rows;
        let mut first = true;
        while !remaining.is_empty() {
            let capacity = if first {
                PART_COUNT_FLUSH_THRESHOLD
                    .saturating_sub(leading_patch_count)
                    .max(1)
            } else {
                PART_COUNT_FLUSH_THRESHOLD
            };
            let take = remaining.len().min(capacity);
            let tail = remaining.split_off(take);
            let mut part = poke_msgs.part.clone();
            part.rows_patch = Some(remaining);
            if !first {
                part.got_queries_patch = None;
                part.desired_queries_patches = None;
                part.last_mutation_id_changes = None;
            }
            responses.push(poke_message_json(&PokeMessage::Part(part)));
            remaining = tail;
            first = false;
        }
    } else {
        responses.push(poke_message_json(&PokeMessage::Part(poke_msgs.part)));
    }
    responses.push(end);
    HandlerOutcome::send(responses)
}

/// A desired PUT op referencing a raw table AST — a small builder for tests.
#[cfg(test)]
fn put_table(hash: &str, table: &str) -> (UpQueriesPatchOp, HashMap<String, Option<Ast>>) {
    use zero_cache_protocol::queries_patch::UpQueriesPutOp;
    let ast = Ast::table(table);
    let op = UpQueriesPatchOp::Put(UpQueriesPutOp {
        hash: hash.to_string(),
        ttl: None,
        ast: Some(ast.clone()),
        name: None,
        args: None,
    });
    let resolved = HashMap::from([(hash.to_string(), Some(ast))]);
    (op, resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use zero_cache_protocol::queries_patch::UpQueriesPutOp;

    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_sqlite::change_log::{ChangeLog, CREATE_CHANGELOG_SCHEMA};
    use zero_cache_sqlite::replication_state::{
        init_replication_state, update_replication_watermark,
    };
    use zero_cache_sqlite::snapshotter::snapshot_table_specs;
    use zero_cache_sqlite::StatementRunner;
    use zero_cache_view_syncer::group_registry::{ClientGroupRegistry, GroupBuilderDeps};

    use crate::live_connection::DesiredQueriesHandler;
    use crate::sync_service::SyncService;

    fn path(tag: &str) -> String {
        use std::sync::atomic::Ordering;
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir()
            .join(format!(
                "zc-group-processor-{}-{}-{tag}.db",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed),
            ))
            .to_string_lossy()
            .into_owned()
    }

    /// A replica shaped like a replicated table (with `_0_version`), seeded with
    /// two rows, plus a change-log so the pipeline can advance.
    fn seed_replica(tag: &str) -> String {
        let path = path(tag);
        let db = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&db, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT NOT NULL, _0_version TEXT NOT NULL)")
            .unwrap();
        db.run("INSERT INTO issue VALUES (1, 'alpha', '00')", &[])
            .unwrap();
        db.run("INSERT INTO issue VALUES (2, 'beta', '00')", &[])
            .unwrap();
        drop(db);
        path
    }

    fn commit_title(replica: &str, service: &SyncService, id: i64, title: &str, version: &str) {
        let db = StatementRunner::open_file(replica).unwrap();
        db.exec(&format!(
            "UPDATE issue SET title='{title}', _0_version='{version}' WHERE id={id}"
        ))
        .unwrap();
        ChangeLog::new(&db)
            .log_set_op(
                version,
                0,
                "issue",
                &vec![("id".to_string(), JsonValue::Number(id as f64))],
                None,
            )
            .unwrap();
        update_replication_watermark(&db, version).unwrap();
        drop(db);
        service.publish_commit(version, false, 1);
    }

    /// Builds a group loop over a shared pipeline for `group_id`, returning the
    /// processor (not yet running), its advance counter, and the service Arc.
    fn build_processor(
        replica: &str,
        service: &SyncService,
        group_id: &str,
    ) -> (GroupProcessor, Arc<AtomicU64>) {
        let reader = StatementRunner::open_file_readonly(replica).unwrap();
        let (specs, all_tables) = snapshot_table_specs(&reader).unwrap();
        let registry = ClientGroupRegistry::new(GroupBuilderDeps {
            db_file: replica.to_string(),
            app_id: "zero".into(),
            page_cache_size_kib: None,
            table_specs: specs,
            all_table_names: all_tables,
        });
        let group_service = registry.get_or_create(group_id).unwrap();
        // The service Arc is held by the core's shared pipeline, so the registry
        // (holding only a Weak) can be dropped here.
        let core = DesiredQueriesHandler::new(reader, group_id, "__loop__")
            .with_shared_pipeline(group_service, "__loop__")
            .into_core();
        let subscriber = service.subscribe();
        let processor = GroupProcessor::new(core, subscriber, group_id.to_string());
        let counter = processor.advance_counter();
        (processor, counter)
    }

    /// A test connection: a frame receiver + a submit closure. Frames the loop
    /// pushed are collected here.
    struct TestConn {
        rx: mpsc::UnboundedReceiver<Vec<String>>,
    }

    impl TestConn {
        fn drain(&mut self) -> Vec<String> {
            let mut out = Vec::new();
            while let Ok(batch) = self.rx.try_recv() {
                out.extend(batch);
            }
            out
        }
    }

    async fn attach(processor: &mut GroupProcessor, client_id: &str) -> TestConn {
        let (tx, rx) = mpsc::unbounded_channel();
        let (reply, reply_rx) = oneshot::channel();
        processor
            .handle_command(GroupCommand::Attach {
                client_id: client_id.to_string(),
                base_cookie: None,
                writer_tx: tx,
                reply,
            })
            .await;
        reply_rx.await.unwrap();
        TestConn { rx }
    }

    async fn desire_all_issues(processor: &mut GroupProcessor, client_id: &str, hash: &str) {
        let (op, resolved) = put_table(hash, "issue");
        let (reply, reply_rx) = oneshot::channel();
        processor
            .handle_command(GroupCommand::ChangeDesiredQueries {
                client_id: client_id.to_string(),
                patch: vec![op],
                resolved_asts: resolved,
                client_schema: None,
                force: false,
                reply,
            })
            .await;
        reply_rx.await.unwrap();
    }

    /// Three connections in one group, then one commit: each writer receives
    /// exactly one poke batch and the pipeline advanced exactly ONCE — the pin
    /// that proves N× per-commit processing is gone.
    #[tokio::test]
    async fn one_commit_advances_once_and_pokes_every_connection() {
        let replica = seed_replica("advance-once");
        let service = SyncService::new(64);
        let (mut processor, advances) = build_processor(&replica, &service, "g1");

        let mut a = attach(&mut processor, "ca").await;
        let mut b = attach(&mut processor, "cb").await;
        let mut c = attach(&mut processor, "cc").await;
        desire_all_issues(&mut processor, "ca", "q").await;
        desire_all_issues(&mut processor, "cb", "q").await;
        desire_all_issues(&mut processor, "cc", "q").await;
        // Clear hydration frames.
        let _ = (a.drain(), b.drain(), c.drain());

        commit_title(&replica, &service, 1, "alpha-updated", "01");
        // Deliver the fanned commit event to the loop directly.
        assert!(matches!(
            processor.subscriber.recv().await,
            FanoutEvent::Commit(_)
        ));
        processor.process_commit().await;

        assert_eq!(
            advances.load(Ordering::Relaxed),
            1,
            "one advance for one commit"
        );
        for (name, conn) in [("a", &mut a), ("b", &mut b), ("c", &mut c)] {
            let frames = conn.drain();
            let starts = frames.iter().filter(|f| f.contains("pokeStart")).count();
            assert_eq!(
                starts, 1,
                "{name} received exactly one poke batch: {frames:?}"
            );
            assert!(
                frames.iter().any(|f| f.contains("alpha-updated")),
                "{name} received the committed row: {frames:?}"
            );
        }

        let _ = std::fs::remove_file(&replica);
    }

    /// A connection that joins AFTER a commit still hydrates the group's CURRENT
    /// state (catchup), and both connections then see the next commit.
    #[tokio::test]
    async fn late_joining_connection_gets_catchup() {
        let replica = seed_replica("late-join");
        let service = SyncService::new(64);
        let (mut processor, _advances) = build_processor(&replica, &service, "g1");

        let mut a = attach(&mut processor, "ca").await;
        desire_all_issues(&mut processor, "ca", "q").await;
        let _ = a.drain();

        commit_title(&replica, &service, 1, "alpha-updated", "01");
        assert!(matches!(
            processor.subscriber.recv().await,
            FanoutEvent::Commit(_)
        ));
        processor.process_commit().await;
        assert!(a.drain().iter().any(|f| f.contains("alpha-updated")));

        // B joins late and desires the same query: it hydrates the current state.
        let mut b = attach(&mut processor, "cb").await;
        desire_all_issues(&mut processor, "cb", "q").await;
        let b_frames = b.drain();
        assert!(
            b_frames.iter().any(|f| f.contains("gotQueriesPatch")),
            "late joiner got the query: {b_frames:?}"
        );
        assert!(
            b_frames.iter().any(|f| f.contains("alpha-updated")),
            "late joiner hydrates the CURRENT (post-commit) row: {b_frames:?}"
        );

        // The next commit reaches BOTH.
        commit_title(&replica, &service, 2, "beta-updated", "02");
        assert!(matches!(
            processor.subscriber.recv().await,
            FanoutEvent::Commit(_)
        ));
        processor.process_commit().await;
        assert!(a.drain().iter().any(|f| f.contains("beta-updated")));
        assert!(b.drain().iter().any(|f| f.contains("beta-updated")));

        let _ = std::fs::remove_file(&replica);
    }

    /// A connection detaching mid-life does not wedge the loop: the remaining
    /// connection keeps receiving commits.
    #[tokio::test]
    async fn detach_does_not_wedge_the_loop() {
        let replica = seed_replica("detach");
        let service = SyncService::new(64);
        let (mut processor, _advances) = build_processor(&replica, &service, "g1");

        let mut a = attach(&mut processor, "ca").await;
        let mut b = attach(&mut processor, "cb").await;
        desire_all_issues(&mut processor, "ca", "q").await;
        desire_all_issues(&mut processor, "cb", "q").await;
        let _ = (a.drain(), b.drain());

        // A detaches.
        processor
            .handle_command(GroupCommand::Detach {
                client_id: "ca".to_string(),
            })
            .await;

        commit_title(&replica, &service, 1, "alpha-updated", "01");
        assert!(matches!(
            processor.subscriber.recv().await,
            FanoutEvent::Commit(_)
        ));
        processor.process_commit().await;

        // B still receives the commit; A's channel got nothing new.
        assert!(b.drain().iter().any(|f| f.contains("alpha-updated")));
        assert!(
            a.drain().is_empty(),
            "detached connection receives no further pokes"
        );

        let _ = std::fs::remove_file(&replica);
    }

    /// B's newly-desired query causes A to also receive the row/got patches
    /// (group semantics), and both poke chains stay valid.
    #[tokio::test]
    async fn new_desired_query_fans_rows_to_the_whole_group() {
        let replica = seed_replica("group-semantics");
        let service = SyncService::new(64);
        let (mut processor, _advances) = build_processor(&replica, &service, "g1");

        let mut a = attach(&mut processor, "ca").await;
        let mut b = attach(&mut processor, "cb").await;
        // A desires a narrow query (id=1 only); B desires nothing yet.
        {
            let ast = Ast {
                table: "issue".into(),
                where_: Some(zero_cache_protocol::ast::Condition::Simple {
                    op: zero_cache_protocol::ast::SimpleOperator::Eq,
                    left: zero_cache_protocol::ast::ValuePosition::Column(
                        zero_cache_protocol::ast::ColumnReference { name: "id".into() },
                    ),
                    right: zero_cache_protocol::ast::ValuePosition::Literal(
                        zero_cache_protocol::ast::LiteralValue::Number(1.0),
                    ),
                }),
                ..Default::default()
            };
            let op = UpQueriesPatchOp::Put(UpQueriesPutOp {
                hash: "qa".into(),
                ttl: None,
                ast: Some(ast.clone()),
                name: None,
                args: None,
            });
            let (reply, reply_rx) = oneshot::channel();
            processor
                .handle_command(GroupCommand::ChangeDesiredQueries {
                    client_id: "ca".into(),
                    patch: vec![op],
                    resolved_asts: HashMap::from([("qa".to_string(), Some(ast))]),
                    client_schema: None,
                    force: false,
                    reply,
                })
                .await;
            reply_rx.await.unwrap();
        }
        let _ = (a.drain(), b.drain());

        // B desires a query covering id=2. A must ALSO receive the row/got
        // patches for it (one shared group view).
        {
            let ast = Ast {
                table: "issue".into(),
                where_: Some(zero_cache_protocol::ast::Condition::Simple {
                    op: zero_cache_protocol::ast::SimpleOperator::Eq,
                    left: zero_cache_protocol::ast::ValuePosition::Column(
                        zero_cache_protocol::ast::ColumnReference { name: "id".into() },
                    ),
                    right: zero_cache_protocol::ast::ValuePosition::Literal(
                        zero_cache_protocol::ast::LiteralValue::Number(2.0),
                    ),
                }),
                ..Default::default()
            };
            let op = UpQueriesPatchOp::Put(UpQueriesPutOp {
                hash: "qb".into(),
                ttl: None,
                ast: Some(ast.clone()),
                name: None,
                args: None,
            });
            let (reply, reply_rx) = oneshot::channel();
            processor
                .handle_command(GroupCommand::ChangeDesiredQueries {
                    client_id: "cb".into(),
                    patch: vec![op],
                    resolved_asts: HashMap::from([("qb".to_string(), Some(ast))]),
                    client_schema: None,
                    force: false,
                    reply,
                })
                .await;
            reply_rx.await.unwrap();
        }

        let a_frames = a.drain();
        assert!(
            a_frames.iter().any(|f| f.contains("beta")),
            "A receives B's query's row (group semantics): {a_frames:?}"
        );
        let b_frames = b.drain();
        assert!(
            b_frames
                .iter()
                .any(|f| f.contains("gotQueriesPatch") && f.contains("qb"))
                || b_frames.iter().any(|f| f.contains("gotQueriesPatch")),
            "B got its query: {b_frames:?}"
        );
        // Neither chain carried an error.
        assert!(!a_frames.iter().any(|f| f.starts_with("[\"error\"")));
        assert!(!b_frames.iter().any(|f| f.starts_with("[\"error\"")));

        let _ = std::fs::remove_file(&replica);
    }
}
