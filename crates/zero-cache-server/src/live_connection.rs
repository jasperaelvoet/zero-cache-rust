//! The real per-connection handler `run_accept_loop`/`serve_connection`
//! actually drive — the literal closure passed to
//! [`crate::sync_server::run_accept_loop`]'s `make_handler` seam, closing the
//! last-named gap: `live_hydration`'s composition was previously proven as a
//! standalone call, not the handler a served connection invokes.
//!
//! [`DesiredQueriesHandler`] owns one connection's CVR state and its own
//! replica handle, and its [`DesiredQueriesHandler::on_action`] IS the
//! `FnMut(ConnectionAction) -> HandlerOutcome` [`crate::serve_connection::serve_connection`]
//! calls. On `Initialize`/`UpdateDesiredQueries` it applies the patch to the
//! CVR ([`CvrQueryHandler::apply_desired_queries_patch`]), hydrates any
//! newly-put query this connection recognizes against the real SQLite replica
//! ([`live_hydration::hydrate_patches_from_sqlite`]), merges both patch sets
//! into ONE wire poke (`build_poke`), and returns the poke's three frames
//! (`pokeStart`/`pokePart`/`pokeEnd`) JSON-encoded via `poke_message_json` —
//! ready for `serve_connection` to send.
//!
//! Scope: hydration can now serve desired-query puts that carry an AST by
//! introspecting the AST root table from SQLite, plus single/compound/nested
//! `related` child reads constrained by the fetched parent rows. Name-only/
//! custom query puts still use the small hardcoded query-hash -> table registry
//! unless a transform is available, and full planner-backed query execution is
//! still outside this slice. Live inspect `analyzeQuery` is broader: it
//! introspects table/column/primary-key metadata from SQLite for the requested
//! AST graph before analysis.

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use zero_cache_auth::policy::PermissionsConfig;
use zero_cache_auth::read_authorizer::{
    bind_permissions_auth_data, transform_and_hash_query_with_auth_data,
};
use zero_cache_auth::write_authorizer::{authorize_mutation_with_exists, NormalizedCrudOp};
use zero_cache_mutagen::api_request::HeaderOptions;
use zero_cache_mutagen::crud_ops::CrudOp;
use zero_cache_mutagen::crud_ops_json::crud_ops_from_json;
use zero_cache_mutagen::last_mutation_id::{check_mutation_id, MutationIdCheck};
use zero_cache_mutagen::orchestration::plan_mutation_sql;
use zero_cache_protocol::ast::{Condition, CorrelatedSubquery};
use zero_cache_protocol::custom_queries::TransformRequestQuery;
use zero_cache_protocol::inspect_down::InspectDownBody;
use zero_cache_protocol::inspect_down_json::inspect_down_message_json;
use zero_cache_protocol::inspect_up::InspectUpBody;
use zero_cache_protocol::mutation_result::{
    MutationError, MutationOk, MutationResponse, MutationResult, MutationZeroError, ZeroErrorKind,
};
use zero_cache_protocol::poke::{PokeEndBody, PokeMessage, PokePartBody, PokeStartBody};
use zero_cache_protocol::poke_json::poke_message_json;
use zero_cache_protocol::pull::{PullRequestBody, PullResponseBody};
use zero_cache_protocol::pull_json::pull_response_message_json;
use zero_cache_protocol::push::{
    CustomMutation, Mutation, PushBody, PushOk, CLEANUP_RESULTS_MUTATION_NAME,
};
use zero_cache_protocol::push_json::push_ok_message_json;
use zero_cache_protocol::queries_patch::{UpQueriesPatchOp, UpQueriesPutOp};
use zero_cache_protocol::query_hash::hash_of_name_and_args;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_shared::timed_cache::TimedCache;
use zero_cache_sqlite::lite_tables::list_tables;
use zero_cache_sqlite::StatementRunner;
use zero_cache_types::shards::ShardId;
use zero_cache_view_syncer::connection_dispatch::ConnectionAction;
use zero_cache_view_syncer::cvr_config_store::flush_cvr_config_transition_with_rows;
use zero_cache_view_syncer::cvr_query_handler::CvrQueryHandler;
use zero_cache_view_syncer::cvr_row_cache_sql::RowUpdate;
use zero_cache_view_syncer::cvr_types::RowRecord;
use zero_cache_view_syncer::cvr_version::{
    cookie_to_version, version_to_cookie, version_to_nullable_cookie,
};
use zero_cache_view_syncer::pipeline_driver::PipelineDriver;
use zero_cache_view_syncer::transform_query_fetch::fetch_and_shape_transform_response;
use zero_cache_view_syncer::transform_query_response::{
    HashedTransformResponse, TransformedAndHashed, TransformedOrErrored,
};
use zero_cache_zql::builder::filter::{create_predicate_with_exists, ExistsFn};
use zero_cache_zql::ivm::data::{values_equal, Row as ZqlRow};

use crate::analyze_query::{analyze_sqlite_ast_query, AnalyzeQueryError};
use crate::cvr_pool::CvrPool;
use crate::group_transition::GroupTransitionCore;
use crate::inspect_handler::handle_inspect;
use crate::inspector_delegate::InspectorDelegate;
use crate::serve_connection::HandlerOutcome;

/// Wall-clock milliseconds since the Unix epoch (the `Date.now()` upstream uses
/// for mutation `timestamp`s). Zero before the epoch (unreachable in practice).
fn now_millis() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

pub struct CustomQueryTransformHttpConfig {
    pub client: reqwest::Client,
    pub url: String,
    pub schema: String,
    pub app_id: String,
    pub api_key: Option<String>,
    pub custom_headers: Vec<(String, String)>,
    pub request_headers: Vec<(String, String)>,
    pub auth_raw: Option<String>,
    pub cookie: Option<String>,
    pub origin: Option<String>,
    pub now_ms: i64,
    cache: TimedCache<String, TransformedAndHashed>,
}

/// Verification settings retained on a live connection so an `updateAuth`
/// frame can safely replace the JWT claims used by compiled permissions. Holds
/// the shared, startup-resolved [`crate::auth_token::TokenVerifier`] (any of
/// secret / static JWK / remote JWKS) so revalidation uses the same key source
/// as the connection gate.
#[derive(Clone)]
pub struct AuthVerifier {
    verifier: std::sync::Arc<crate::auth_token::TokenVerifier>,
    issuer: Option<String>,
    audience: Option<String>,
}

impl AuthVerifier {
    pub fn new(
        verifier: std::sync::Arc<crate::auth_token::TokenVerifier>,
        issuer: Option<String>,
        audience: Option<String>,
    ) -> Self {
        Self {
            verifier,
            issuer,
            audience,
        }
    }

    fn verify(
        &self,
        token: &str,
    ) -> Result<crate::auth_token::Claims, crate::auth_token::AuthError> {
        // Synchronous revalidation: a remote JWKS reuses the key set already
        // cached at connect time (no refetch), keeping `on_action` non-async.
        self.verifier.verify_sync(
            token,
            crate::auth_token::now_unix(),
            self.issuer.as_deref(),
            self.audience.as_deref(),
            // Subject pinning is enforced at the connection gate (which knows
            // the connecting userID); this per-token re-check only revalidates
            // signature/expiry/issuer/audience.
            None,
        )
    }
}

/// Per-connection durable CVR writer backed by the sync service's shared,
/// bounded CVR pool, matching official zero-cache's database lifecycle.
pub struct CvrPersistence {
    pool: CvrPool,
    shard: ShardId,
    task_id: String,
    last_connect_time_ms: f64,
    /// Process-global throttle for deferred row flushes. Present only on the
    /// deferral path; unused (and `None`) when `ZERO_DEFER_CVR_ROWS` is off.
    defer_flush_limiter: Option<crate::cvr_row_flush_barrier::DeferFlushLimiter>,
}

impl CvrPersistence {
    pub fn new(
        pool: CvrPool,
        shard: ShardId,
        task_id: impl Into<String>,
        last_connect_time_ms: f64,
    ) -> Self {
        Self {
            pool,
            shard,
            task_id: task_id.into(),
            last_connect_time_ms,
            defer_flush_limiter: None,
        }
    }

    /// Attaches the process-global deferred-flush concurrency limiter. Only set
    /// on the deferral path; the synchronous flush path never consults it.
    pub fn with_defer_flush_limiter(
        mut self,
        limiter: crate::cvr_row_flush_barrier::DeferFlushLimiter,
    ) -> Self {
        self.defer_flush_limiter = Some(limiter);
        self
    }

    pub(crate) async fn flush(
        &mut self,
        before: &zero_cache_view_syncer::cvr_types::Cvr,
        after: &zero_cache_view_syncer::cvr_types::Cvr,
        row_updates: &[RowUpdate],
    ) -> Result<(), String> {
        let mut client = self.pool.get().await?;
        flush_cvr_config_transition_with_rows(
            &mut client,
            &self.shard,
            &self.task_id,
            self.last_connect_time_ms,
            &before.version,
            before,
            after,
            row_updates,
        )
        .await
        // `tokio_postgres::Error` deliberately has the terse Display value
        // "db error".  Sending only that outer value made every CVR SQL
        // failure indistinguishable to both operators and clients. Preserve
        // the source chain, which contains PostgreSQL's SQLSTATE and message.
        .map_err(|error| format_error_chain(&error))
    }

    /// Commits ONLY the configuration/version transaction (with the
    /// optimistic-concurrency CAS) for a transition, deferring the row records.
    /// Returns the `rowsVersion` cookie the deferred rows flush must use.
    pub(crate) async fn flush_config_only(
        &mut self,
        before: &zero_cache_view_syncer::cvr_types::Cvr,
        after: &zero_cache_view_syncer::cvr_types::Cvr,
    ) -> Result<String, String> {
        let mut client = self.pool.get().await?;
        zero_cache_view_syncer::cvr_config_store::flush_cvr_config_transition_no_rows(
            &mut client,
            &self.shard,
            &self.task_id,
            self.last_connect_time_ms,
            &before.version,
            before,
            after,
        )
        .await
        .map_err(|error| format_error_chain(&error))
    }

    /// Spawns the deferred rows-only flush for a transition, chained through the
    /// group's barrier so flushes land in order and a reconnect load can await
    /// them. Owns clones of everything it needs so it can outlive the handler.
    pub(crate) fn spawn_deferred_rows_flush(
        &self,
        barrier: &std::sync::Arc<crate::cvr_row_flush_barrier::RowFlushBarrier>,
        client_group_id: String,
        row_updates: Vec<RowUpdate>,
        rows_version: String,
    ) {
        let pool = self.pool.clone();
        let shard = self.shard.clone();
        let limiter = self.defer_flush_limiter.clone();
        barrier.spawn_chained(async move {
            // Acquire a process-global permit BEFORE taking a pool connection so
            // background row flushes cannot seize most of the CvrPool / saturate
            // Postgres, leaving headroom for synchronous config flushes on the
            // critical path. The permit is held for the whole flush and released
            // when `_permit` drops. Chaining/ordering is unchanged: the barrier
            // has already sequenced this flush after the group's previous one.
            let _permit = match &limiter {
                Some(limiter) => limiter.acquire().await,
                None => None,
            };
            let mut client = match pool.get().await {
                Ok(client) => client,
                Err(error) => {
                    crate::warn!("deferred CVR row flush skipped for {client_group_id}: {error}");
                    return;
                }
            };
            if let Err(error) = zero_cache_view_syncer::cvr_config_store::flush_cvr_rows_transition(
                &mut client,
                &shard,
                &client_group_id,
                &row_updates,
                &rows_version,
            )
            .await
            {
                crate::warn!(
                    "deferred CVR row flush failed for {client_group_id}: {}",
                    format_error_chain(&error)
                );
            }
        });
    }

    pub(crate) async fn load(
        &self,
        client_group_id: &str,
    ) -> Result<
        (
            zero_cache_view_syncer::cvr_types::Cvr,
            Vec<zero_cache_view_syncer::cvr_types::RowRecord>,
        ),
        String,
    > {
        // Port of upstream `CVRStore.load`: `RowsBehind` is a transient state —
        // the CVR's configuration version was committed but its deferred row
        // records (possibly flushed by ANOTHER task in a multi-node deployment)
        // haven't landed yet. Upstream waits and retries (500ms × 10 attempts)
        // instead of failing the connection.
        const LOAD_ATTEMPT_INTERVAL_MS: u64 = 500;
        const MAX_LOAD_ATTEMPTS: usize = 10;
        let mut behind = None;
        for attempt in 0..MAX_LOAD_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(LOAD_ATTEMPT_INTERVAL_MS))
                    .await;
            }
            let client = self.pool.get().await?;
            let loaded = zero_cache_view_syncer::cvr_store_pg::load_cvr(
                &client,
                &self.shard,
                client_group_id,
                &self.task_id,
                self.last_connect_time_ms,
            )
            .await
            .map_err(|error| format_error_chain(&error))?;
            let cvr = match loaded {
                zero_cache_view_syncer::cvr_store_pg::LoadCvrOutcome::Loaded(cvr) => cvr,
                zero_cache_view_syncer::cvr_store_pg::LoadCvrOutcome::RowsBehind {
                    version,
                    rows_version,
                } => {
                    behind = Some((version, rows_version));
                    continue;
                }
            };
            let rows = zero_cache_view_syncer::cvr_store_pg::get_row_records(
                &client,
                &self.shard,
                client_group_id,
            )
            .await
            .map_err(|error| format_error_chain(&error))?
            .into_values()
            .collect();
            return Ok((cvr, rows));
        }
        let (version, rows_version) = behind.expect("retry loop exhausted without a RowsBehind");
        Err(format!(
            "max attempts exceeded waiting for CVR@{version} to catch up from {}",
            rows_version.as_deref().unwrap_or("<none>")
        ))
    }
}

fn format_error_chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        let detail = error.to_string();
        if detail != message {
            message.push_str(": ");
            message.push_str(&detail);
        }
        source = error.source();
    }
    message
}

/// Fetches and shapes ONE custom-query transform against `ZERO_QUERY_URL`,
/// returning the query name, its args, and either the transformed AST or the
/// failure message. Standalone (takes owned config, not `&self`) so many of
/// these can be driven concurrently via `join_all`. Each call uses its own
/// short-lived response cache — the shared connection cache cannot be borrowed
/// mutably across concurrent fetches, and within a single patch every query id
/// is distinct so there is nothing to share.
#[allow(clippy::too_many_arguments)]
async fn fetch_custom_query_transform(
    client: reqwest::Client,
    url: String,
    schema: String,
    app_id: String,
    api_key: Option<String>,
    custom_headers: Vec<(String, String)>,
    request_headers: Vec<(String, String)>,
    auth_raw: Option<String>,
    cookie: Option<String>,
    origin: Option<String>,
    now_ms: i64,
    name: String,
    args: Vec<JsonValue>,
) -> (
    String,
    Vec<JsonValue>,
    Result<zero_cache_protocol::ast::Ast, String>,
) {
    let id = hash_of_name_and_args(&name, &args);
    let request = vec![TransformRequestQuery {
        id: id.clone(),
        name: name.clone(),
        args: args.clone(),
    }];
    let headers = HeaderOptions {
        api_key: api_key.as_deref(),
        custom_headers: &custom_headers,
        request_headers: &request_headers,
        auth_raw: auth_raw.as_deref(),
        cookie: cookie.as_deref(),
        origin: origin.as_deref(),
    };
    let mut cache = TimedCache::new(5000);
    let response = match fetch_and_shape_transform_response(
        &client,
        &url,
        &schema,
        &app_id,
        &headers,
        &request,
        vec![],
        &mut cache,
        |cache_id| cache_id.to_string(),
        now_ms,
    )
    .await
    {
        Ok(response) => response,
        Err(e) => return (name, args, Err(e.to_string())),
    };

    let result = match response {
        HashedTransformResponse::Failed(body) => Err(body.message),
        HashedTransformResponse::Success { result, .. } => {
            let mut resolved =
                Err("custom query transform response did not include requested query".to_string());
            for item in result {
                match item {
                    TransformedOrErrored::Ok(t) if t.id == id => {
                        resolved = Ok(t.transformed_ast);
                        break;
                    }
                    TransformedOrErrored::Errored(e) if e.id == id => {
                        resolved = Err(e
                            .message
                            .unwrap_or_else(|| "custom query transform failed".to_string()));
                        break;
                    }
                    _ => {}
                }
            }
            resolved
        }
    };
    (name, args, result)
}

fn empty_auth_data() -> JsonValue {
    JsonValue::Object(vec![])
}

fn auth_subject(auth_data: &JsonValue) -> Option<String> {
    let JsonValue::Object(fields) = auth_data else {
        return None;
    };
    fields.iter().find_map(|(name, value)| {
        (name == "sub")
            .then_some(value)
            .and_then(|value| match value {
                JsonValue::String(value) => Some(value.clone()),
                _ => None,
            })
    })
}

impl CustomQueryTransformHttpConfig {
    pub fn new(
        url: impl Into<String>,
        schema: impl Into<String>,
        app_id: impl Into<String>,
    ) -> Self {
        CustomQueryTransformHttpConfig {
            client: reqwest::Client::new(),
            url: url.into(),
            schema: schema.into(),
            app_id: app_id.into(),
            api_key: None,
            custom_headers: Vec::new(),
            request_headers: Vec::new(),
            auth_raw: None,
            cookie: None,
            origin: None,
            now_ms: 0,
            cache: TimedCache::new(5000),
        }
    }
}

/// One served connection's stateful handler: owns its CVR and a replica
/// handle. This is literally the closure a connection's factory hands to
/// `serve_connection` — see the live test for the real wiring.
pub struct DesiredQueriesHandler {
    /// The GROUP-scoped half: replica handle, working CVR, row records/bodies,
    /// desired puts, query pipeline, durable CVR persistence and the
    /// defer/barrier/group-cell plumbing — extracted as
    /// [`crate::group_transition::GroupTransitionCore`] (group-loop plan
    /// increment 1). Everything else on this struct is per-CONNECTION state:
    /// poke delivery cursors plus auth/mutation/inspect.
    core: GroupTransitionCore,
    inspector_delegate: InspectorDelegate,
    poke_seq: u64,
    /// The cookie supplied by this client in the WebSocket handshake. The
    /// first poke is always based on the client's view (`None` for fresh), not
    /// on a newer durable server CVR that may already exist for the group.
    initial_base_version: Option<zero_cache_view_syncer::cvr_version::CvrVersion>,
    /// Last cookie actually advertised on this socket. Durable group state can
    /// advance through another client between this connection's pokes; the
    /// next baseCookie must still chain from what this client received.
    last_poke_version: Option<zero_cache_view_syncer::cvr_version::CvrVersion>,
    inspect_protocol_version: u32,
    inspect_server_version: String,
    inspect_development_mode: bool,
    inspect_admin_password: Option<String>,
    /// Compiled row/cell permissions from `ZERO_SCHEMA_JSON`, if configured.
    /// When absent we preserve the legacy no-permissions deployment behavior;
    /// when present, missing table/action rules are default-deny.
    read_permissions: Option<PermissionsConfig>,
    /// Verified JWT payload used to bind compiled `authData` static values.
    auth_data: JsonValue,
    /// The authenticated identity is pinned for a connection/client group;
    /// accepting a token for another subject on `updateAuth` would otherwise
    /// let one client group switch users mid-stream.
    auth_subject: Option<String>,
    auth_verifier: Option<AuthVerifier>,
    auth_raw: Option<String>,
    custom_query_transform_http: Option<CustomQueryTransformHttpConfig>,
    /// Last mutation IDs already included in a poke on this connection. The
    /// replicated `<shard>.clients` table is the source of truth; this map is
    /// only the per-connection delta cursor used to avoid resending unchanged
    /// acknowledgements on every data poke.
    poked_last_mutation_ids: BTreeMap<String, i64>,
    /// Mutation responses the client has not acknowledged yet. Production
    /// zero-cache persists/cleans this through the pusher; this demo handler
    /// keeps the same lifecycle in memory so `ackMutationResponses` is no
    /// longer a no-op.
    pending_mutation_responses: Vec<MutationResponse>,
    /// When set (synced mode), `push` mutations are applied to UPSTREAM Postgres
    /// via `apply_crud_mutation` (and flow back through replication), instead of
    /// the read-only local replica. `(libpq conn string, mutation schema)`.
    upstream_push: Option<(String, String)>,
    /// A lazily-opened upstream Postgres client for the push path (one per
    /// connection). `apply_crud_mutation` needs `&mut Client`.
    upstream_client: Option<tokio_postgres::Client>,
    /// Held while `upstream_client` is open (ZERO_UPSTREAM_MAX_CONNS bound).
    upstream_conn_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    /// When set, `push` mutations are forwarded to the app's custom-mutator API
    /// server (`ZERO_MUTATE_URL`) instead of applied locally/upstream.
    mutate_api: Option<crate::custom_mutation::MutateApi>,
    /// Legacy cookie-only resume flag used when no durable CVR database is
    /// configured. Synced deployments load the CVR instead and leave this
    /// unset so the loaded server version remains authoritative.
    resume_requires_ack: bool,
    /// Serializes reload→apply→flush transitions for every connection in one
    /// client group. PostgreSQL still performs the authoritative version CAS;
    /// this lock prevents two local connection handlers from repeatedly
    /// building on stale snapshots of that same durable CVR.
    cvr_transition_lock: Option<std::sync::Arc<tokio::sync::Mutex<()>>>,
    pending_hydration: Option<(
        zero_cache_view_syncer::cvr_version::CvrVersion,
        Vec<zero_cache_view_syncer::client_patch::PatchToVersion>,
    )>,
}

impl DesiredQueriesHandler {
    pub fn new(db: StatementRunner, client_group_id: &str, client_id: &str) -> Self {
        Self::with_inspect_options(
            db,
            client_group_id,
            client_id,
            51,
            env!("CARGO_PKG_VERSION").to_string(),
            true,
            None,
        )
    }

    /// Seeds the connection's initial bearer token (the `authToken` from the
    /// connect handshake), so the first forwarded mutation/query carries it as
    /// `Authorization: Bearer …` — matching upstream, where the connection
    /// context holds `auth.raw` from connect. A later `updateAuth` overrides it.
    pub fn with_auth(mut self, token: Option<String>) -> Self {
        if token.is_some() {
            self.auth_raw = token;
        }
        self
    }

    /// Supplies the already-verified JWT payload for compiled `authData`
    /// binding.  This is separate from [`Self::with_auth`] because an opaque
    /// bearer token is valid for forwarding but must never be trusted as
    /// authorization data.
    pub fn with_auth_data(mut self, auth_data: JsonValue) -> Self {
        self.auth_subject = auth_subject(&auth_data);
        self.auth_data = auth_data;
        self
    }

    /// Enables safe JWT revalidation for later `updateAuth` frames.
    pub fn with_auth_verifier(mut self, verifier: AuthVerifier) -> Self {
        self.auth_verifier = Some(verifier);
        self
    }

    /// Seeds the CVR from the client's connect `baseCookie` so a RECONNECTING
    /// client's first poke bases at the cookie it holds (not `null`). Without
    /// this, every reconnect fails with "unexpected base cookie during sync"
    /// because the fresh per-connection CVR would send `baseCookie:null` while
    /// the client is at e.g. `"00:01"`. Empty/absent cookie = a fresh client
    /// (leave the CVR empty → first poke bases at `null`).
    pub fn with_base_cookie(mut self, cookie: Option<String>) -> Self {
        let cookie = cookie.filter(|c| !c.is_empty());
        if let Ok(Some(version)) =
            zero_cache_view_syncer::cvr_version::cookie_to_version(cookie.as_deref())
        {
            self.initial_base_version = Some(version.clone());
            self.core.cvr_handler.seed_version(version);
            self.resume_requires_ack = true;
        }
        self
    }

    pub fn with_inspect_options(
        db: StatementRunner,
        client_group_id: &str,
        client_id: &str,
        inspect_protocol_version: u32,
        inspect_server_version: String,
        inspect_development_mode: bool,
        inspect_admin_password: Option<String>,
    ) -> Self {
        DesiredQueriesHandler {
            core: GroupTransitionCore::new(db, client_group_id, client_id),
            inspector_delegate: InspectorDelegate::new(),
            poke_seq: 0,
            initial_base_version: None,
            last_poke_version: None,
            inspect_protocol_version,
            inspect_server_version,
            inspect_development_mode,
            inspect_admin_password,
            read_permissions: None,
            auth_data: empty_auth_data(),
            auth_subject: None,
            auth_verifier: None,
            auth_raw: None,
            custom_query_transform_http: None,
            poked_last_mutation_ids: BTreeMap::new(),
            pending_mutation_responses: Vec::new(),
            upstream_push: None,
            upstream_client: None,
            upstream_conn_permit: None,
            mutate_api: None,
            resume_requires_ack: false,
            cvr_transition_lock: None,
            pending_hydration: None,
        }
    }

    pub fn with_pipeline_driver(mut self, pipeline_driver: PipelineDriver) -> Self {
        self.core.query_pipeline = Some(crate::query_pipeline::QueryPipeline::Owned(Box::new(
            pipeline_driver,
        )));
        self
    }

    /// Wires this connection to its client group's SHARED pipeline instead of a
    /// per-connection driver (redesign §6, `ZERO_GROUP_OWNERSHIP`). `client_id`
    /// keys the group's cross-client query ref-count.
    pub fn with_shared_pipeline(
        mut self,
        service: std::sync::Arc<zero_cache_view_syncer::group_registry::GroupService>,
        client_id: impl Into<String>,
    ) -> Self {
        self.core.query_pipeline = Some(crate::query_pipeline::QueryPipeline::Shared {
            service,
            client_id: client_id.into(),
        });
        self
    }

    /// Forwards `push` mutations to the app's custom-mutator API server
    /// (`ZERO_MUTATE_URL`). Takes priority over the CRUD/upstream path.
    pub fn with_mutate_api(
        mut self,
        url: String,
        api_key: Option<String>,
        schema: String,
        app_id: String,
    ) -> Self {
        self.mutate_api = Some(crate::custom_mutation::MutateApi::new(
            url, api_key, schema, app_id,
        ));
        self
    }

    /// Like [`Self::with_mutate_api`] but also forwarding the client's session
    /// cookie + allowed client headers to the mutate server (cookie-auth apps).
    #[allow(clippy::too_many_arguments)]
    pub fn with_mutate_api_forwarding(
        mut self,
        url: String,
        api_key: Option<String>,
        schema: String,
        app_id: String,
        cookie: Option<String>,
        custom_headers: Vec<(String, String)>,
        request_headers: Vec<(String, String)>,
    ) -> Self {
        self.mutate_api =
            Some(
                crate::custom_mutation::MutateApi::new(url, api_key, schema, app_id)
                    .with_forwarding(cookie, custom_headers, request_headers),
            );
        self
    }

    /// Routes `push` mutations to UPSTREAM Postgres (schema `schema`, connected
    /// via libpq `conn_str`) instead of the local replica — the production write
    /// path. The mutation flows to Postgres and replicates back into the replica.
    pub fn with_upstream_push(mut self, conn_str: String, schema: String) -> Self {
        self.upstream_push = Some((conn_str, schema));
        self
    }

    pub fn with_custom_query_transform_http(
        mut self,
        config: CustomQueryTransformHttpConfig,
    ) -> Self {
        self.custom_query_transform_http = Some(config);
        self
    }

    pub fn with_read_permissions(mut self, permissions: PermissionsConfig) -> Self {
        self.read_permissions = Some(permissions);
        self
    }

    /// Configures compiled permissions for both read hydration and CRUD write
    /// authorization.  Kept distinct from the older `with_read_permissions`
    /// name so production bootstrap makes the write boundary explicit.
    pub fn with_permissions(self, permissions: PermissionsConfig) -> Self {
        self.with_read_permissions(permissions)
    }

    /// Replaces the fresh per-connection CVR with the durable state loaded for
    /// this client group and rebuilds the hydration index from its active
    /// desired queries. The loaded server version remains authoritative; a
    /// reconnect cookie is not allowed to overwrite it.
    pub fn with_loaded_cvr(mut self, cvr: zero_cache_view_syncer::cvr_types::Cvr) -> Self {
        let client_id = self.core.cvr_handler.client_id().to_string();
        self.core.cvr_handler =
            CvrQueryHandler::from_cvr(cvr, &self.core.client_group_id, &client_id);
        self.core.desired_puts = self
            .core
            .cvr_handler
            .desired_puts_for_client()
            .into_iter()
            .map(|put| (put.hash.clone(), put))
            .collect();
        self.resume_requires_ack = false;
        self
    }

    pub fn with_cvr_persistence(mut self, persistence: CvrPersistence) -> Self {
        self.core.cvr_persistence = Some(persistence);
        self
    }

    pub fn with_cvr_transition_lock(
        mut self,
        lock: std::sync::Arc<tokio::sync::Mutex<()>>,
    ) -> Self {
        self.cvr_transition_lock = Some(lock);
        self
    }

    /// Enables deferring the CVR row-record flush off the hydration critical
    /// path. Requires a row-flush barrier (see
    /// [`Self::with_cvr_row_flush_barrier`]); without one, the handler keeps the
    /// synchronous combined flush so the durable invariant is never at risk.
    pub fn with_defer_cvr_rows(mut self, defer: bool) -> Self {
        self.core.defer_cvr_rows = defer;
        self
    }

    pub fn with_cvr_row_flush_barrier(
        mut self,
        barrier: std::sync::Arc<crate::cvr_row_flush_barrier::RowFlushBarrier>,
    ) -> Self {
        self.core.cvr_row_flush_barrier = Some(barrier);
        self
    }

    pub fn with_loaded_row_records(mut self, row_records: std::sync::Arc<Vec<RowRecord>>) -> Self {
        self.core.row_records = row_records;
        self
    }

    /// Attaches the client group's shared CVR cell (group-ownership path).
    pub fn with_group_cvr(
        mut self,
        cell: std::sync::Arc<zero_cache_view_syncer::group_cvr::GroupCvrCell>,
    ) -> Self {
        self.core.group_cvr = Some(cell);
        self
    }

    pub fn add_custom_query_transform(
        &mut self,
        name: &str,
        args: &[JsonValue],
        ast: zero_cache_protocol::ast::Ast,
    ) {
        self.inspector_delegate
            .add_custom_query_transform(name, args, ast);
    }

    pub fn pending_mutation_response_count(&self) -> usize {
        self.pending_mutation_responses.len()
    }

    /// Refreshes the group-scoped working state (adopt the group cell or
    /// reload the durable CVR) via the core, discarding any staged hydration
    /// derived from the previous state when the state was replaced.
    async fn refresh_durable_cvr(&mut self) -> Result<(), String> {
        if self.core.refresh_durable_cvr().await? {
            self.pending_hydration = None;
        }
        Ok(())
    }

    pub fn auth_raw(&self) -> Option<&str> {
        self.auth_raw.as_deref()
    }

    /// This connection's client id.
    pub(crate) fn client_id(&self) -> String {
        self.core.cvr_handler.client_id().to_string()
    }

    /// The connect base cookie parsed to a CVR version (the base of this
    /// connection's first poke), for handing to the group processor loop.
    pub(crate) fn initial_base_version(
        &self,
    ) -> Option<zero_cache_view_syncer::cvr_version::CvrVersion> {
        self.initial_base_version.clone()
    }

    /// Whether the first desired-queries transition must force an empty poke to
    /// re-advertise the resumed cookie (a reconnecting client with a base
    /// cookie). Consumed once, like the flag-off `on_action` init path.
    pub(crate) fn take_resume_requires_ack(&mut self) -> bool {
        std::mem::take(&mut self.resume_requires_ack)
    }

    /// Consumes this handler, yielding its group-scoped transition core. The
    /// per-group processor loop builds a handler with the normal builders (to
    /// reuse their wiring), then takes ownership of the core to drive the whole
    /// group. Flag-off never calls this.
    pub(crate) fn into_core(self) -> GroupTransitionCore {
        self.core
    }

    /// Loads a group CVR/row snapshot into this connection handler's core so a
    /// per-connection read (e.g. `inspect` analyze-query) sees the group's
    /// current state owned by the processor loop. Flag-on connection path only.
    pub(crate) fn load_group_snapshot(
        &mut self,
        cvr: zero_cache_view_syncer::cvr_types::Cvr,
        row_records: Vec<RowRecord>,
        row_bodies: Vec<(
            zero_cache_view_syncer::cvr_types::RowId,
            zero_cache_protocol::row_patch::Row,
        )>,
    ) {
        let client_id = self.core.cvr_handler.client_id().to_string();
        self.core.cvr_handler =
            CvrQueryHandler::from_cvr(cvr, &self.core.client_group_id, &client_id);
        self.core.row_records = std::sync::Arc::new(row_records);
        self.core.row_bodies = std::sync::Arc::new(row_bodies);
    }

    /// Runs the connection's async pre-steps for a desired-queries patch (custom
    /// query transform fetch + per-connection read-permission resolution) and
    /// returns the resolved transformed ASTs the processor loop applies. Flag-on
    /// connection path only; the group CVR itself is transitioned in the loop.
    pub(crate) async fn resolve_desired_patch(
        &mut self,
        patch: &[UpQueriesPatchOp],
    ) -> HashMap<String, Option<zero_cache_protocol::ast::Ast>> {
        let failures = self
            .fetch_missing_custom_query_transforms_for_patch(patch)
            .await;
        // Group-loop path: the resolved-AST map is submitted to the processor,
        // so a terminal connection error cannot be returned from here. Surface
        // the failure at error level (not a silent `warn!`) so a broken
        // transform endpoint / rejected forwarded cookie is visible rather than
        // leaving the affected queries stuck at `type:"unknown"`.
        for (name, error) in &failures {
            crate::error!("custom-query transform for '{name}' FAILED (group path): {error}");
        }
        self.resolve_patch_asts(patch)
    }

    fn persistence_failure(error: String) -> HandlerOutcome {
        HandlerOutcome {
            responses: vec![format!(
                r#"["error",{{"kind":"Internal","message":"CVR persistence failed: {}"}}]"#,
                error.replace('"', "\\\"")
            )],
            keep_open: false,
        }
    }

    /// The real handler: `serve_connection`/`run_accept_loop` call this for
    /// every routed [`ConnectionAction`].
    pub fn on_action(&mut self, action: ConnectionAction) -> HandlerOutcome {
        match action {
            ConnectionAction::Initialize(body) => {
                self.store_client_schema(&body);
                let force = std::mem::take(&mut self.resume_requires_ack);
                self.apply_and_poke(&body.desired_queries_patch, force)
            }
            ConnectionAction::UpdateDesiredQueries(body) => {
                self.apply_and_poke(&body.desired_queries_patch, false)
            }
            ConnectionAction::Push(body) => self.apply_push(&body),
            ConnectionAction::Inspect(body) => self.apply_inspect(body),
            ConnectionAction::AckMutationResponses(body) => self.apply_ack_mutation_response(&body),
            ConnectionAction::UpdateAuth(body) => self.apply_update_auth(&body),
            ConnectionAction::Pull(body) => self.apply_pull(&body),
            // Same boundary: the wire/router layer is ported, while
            // resolving auth and pusher cleanup need the full view-syncer /
            // pusher service wrappers.
            ConnectionAction::DeleteClients(_)
            | ConnectionAction::Close
            | ConnectionAction::Pong => HandlerOutcome::empty(),
        }
    }

    pub async fn on_action_async(&mut self, action: ConnectionAction) -> HandlerOutcome {
        let changes_cvr = matches!(
            &action,
            ConnectionAction::Initialize(_) | ConnectionAction::UpdateDesiredQueries(_)
        );
        let _transition_guard = if changes_cvr {
            match self.cvr_transition_lock.clone() {
                Some(lock) => Some(lock.lock_owned().await),
                None => None,
            }
        } else {
            None
        };
        if changes_cvr {
            const MAX_CVR_RETRIES: usize = 8;
            for attempt in 0..MAX_CVR_RETRIES {
                if let Err(error) = self.refresh_durable_cvr().await {
                    return Self::persistence_failure(error);
                }
                // Building a poke advances per-socket delivery cursors before
                // the durable CAS is attempted. If that CAS loses, the poke is
                // discarded and must not become the base for the retry: the
                // client never saw it. Keep the delivery state transactional
                // with the CVR transition as well.
                let delivery_checkpoint = (
                    self.poke_seq,
                    self.last_poke_version.clone(),
                    self.poked_last_mutation_ids.clone(),
                );
                let result = match action.clone() {
                    ConnectionAction::Initialize(body) => {
                        let before = self.core.cvr_handler.cvr.clone();
                        self.store_client_schema(&body);
                        let failures = self
                            .fetch_missing_custom_query_transforms_for_patch(
                                &body.desired_queries_patch,
                            )
                            .await;
                        // A failed transform must not tear down the whole
                        // connection — that would also drop unrelated mutations
                        // and successfully-transformed queries. Upstream fails
                        // the individual query; here we proceed with the
                        // transition (the un-transformed query simply has no
                        // plan) and surface the failure loudly.
                        Self::log_transform_failures(&failures);
                        let force = self.resume_requires_ack;
                        let outcome =
                            self.apply_and_poke_staged(&body.desired_queries_patch, force);
                        self.core
                            .persist_transition(&before)
                            .await
                            .map(|()| outcome)
                    }
                    ConnectionAction::UpdateDesiredQueries(body) => {
                        let before = self.core.cvr_handler.cvr.clone();
                        let failures = self
                            .fetch_missing_custom_query_transforms_for_patch(
                                &body.desired_queries_patch,
                            )
                            .await;
                        Self::log_transform_failures(&failures);
                        let outcome =
                            self.apply_and_poke_staged(&body.desired_queries_patch, false);
                        self.core
                            .persist_transition(&before)
                            .await
                            .map(|()| outcome)
                    }
                    _ => unreachable!("changes_cvr only matches query-set actions"),
                };
                match result {
                    Ok(outcome) => {
                        self.resume_requires_ack = false;
                        return outcome;
                    }
                    Err(error)
                        if error.contains("concurrent modification")
                            && attempt + 1 < MAX_CVR_RETRIES =>
                    {
                        self.poke_seq = delivery_checkpoint.0;
                        self.last_poke_version = delivery_checkpoint.1;
                        self.poked_last_mutation_ids = delivery_checkpoint.2;
                        crate::warn!(
                            "retrying concurrent CVR transition for {} (attempt {}/{})",
                            self.core.client_group_id,
                            attempt + 2,
                            MAX_CVR_RETRIES
                        );
                        tokio::task::yield_now().await;
                    }
                    Err(error) => return Self::persistence_failure(error),
                }
            }
            unreachable!("CVR retry loop always returns")
        }
        match action {
            ConnectionAction::Initialize(_) | ConnectionAction::UpdateDesiredQueries(_) => {
                unreachable!("CVR-changing actions return from the retry loop")
            }
            ConnectionAction::Inspect(body) => self.apply_inspect_async(body).await,
            // Custom mutators take priority: forward the push to the app's
            // mutate API server (writes land upstream + replicate back).
            ConnectionAction::Push(body) if self.mutate_api.is_some() => {
                let api = self.mutate_api.clone().expect("checked");
                let responses =
                    crate::custom_mutation::forward_push(&api, &body, self.auth_raw.as_deref())
                        .await;
                self.mutation_responses_outcome(responses)
            }
            ConnectionAction::Push(body) if self.upstream_push.is_some() => {
                self.apply_push_upstream(&body).await
            }
            // For custom-mutator deployments, acking mutation responses must also
            // fire-and-forget a cleanup mutation to the app's push endpoint so it
            // prunes stored results (upstream `PusherService.ackMutationResponses`).
            ConnectionAction::AckMutationResponses(body) if self.mutate_api.is_some() => {
                let outcome = self.apply_ack_mutation_response(&body);
                // Fire-and-forget cleanup POST. Extract owned/`&str` values before
                // the await so the future never holds `&self` (not `Sync`).
                let api = self.mutate_api.clone().expect("checked");
                let push = Self::build_cleanup_push(
                    &self.core.client_group_id,
                    &body.mutation_id,
                    now_millis(),
                );
                let _ = crate::custom_mutation::forward_push(&api, &push, self.auth_raw.as_deref())
                    .await;
                outcome
            }
            other => self.on_action(other),
        }
    }

    /// Applies a `push`'s CRUD mutations to UPSTREAM Postgres via
    /// `apply_crud_mutation` (the live-tested executor), returning per-mutation
    /// responses. Used in synced mode where the local replica is read-only.
    async fn apply_push_upstream(&mut self, push: &PushBody) -> HandlerOutcome {
        let (conn_str, schema) = self.upstream_push.clone().expect("upstream configured");

        // Lazily open (once per connection) the upstream client, bounded by
        // ZERO_UPSTREAM_MAX_CONNS: the permit is held for the client's
        // lifetime so concurrently-open upstream mutation connections never
        // exceed the configured pool size.
        if self.upstream_client.is_none() {
            self.upstream_conn_permit = crate::upstream_conn_limit::acquire().await;
            match zero_cache_change_source::pg_connection::connect(&conn_str).await {
                Ok(c) => self.upstream_client = Some(c),
                Err(e) => {
                    // Report every mutation as failed if we can't reach upstream.
                    let responses = push
                        .mutations
                        .iter()
                        .map(|m| MutationResponse {
                            id: m.id(),
                            result: MutationResult::Error(MutationError::App(
                                zero_cache_protocol::mutation_result::MutationAppError {
                                    message: Some(format!("upstream connect failed: {e}")),
                                    details: None,
                                },
                            )),
                        })
                        .collect();
                    return self.mutation_responses_outcome(responses);
                }
            }
        }
        let mut responses = Vec::with_capacity(push.mutations.len());
        for mutation in &push.mutations {
            let id = mutation.id();
            let Mutation::Crud(crud) = mutation else {
                responses.push(MutationResponse {
                    id,
                    result: MutationResult::Error(MutationError::App(
                        zero_cache_protocol::mutation_result::MutationAppError {
                            message: Some("custom mutations are not supported".into()),
                            details: None,
                        },
                    )),
                });
                continue;
            };
            let ops = match crud_ops_from_json(&crud.ops_json) {
                Ok(ops) => ops,
                Err(e) => {
                    responses.push(MutationResponse {
                        id,
                        result: MutationResult::Error(MutationError::App(
                            zero_cache_protocol::mutation_result::MutationAppError {
                                message: Some(e.to_string()),
                                details: None,
                            },
                        )),
                    });
                    continue;
                }
            };
            // ZERO_PER_USER_MUTATION_LIMIT_*: upstream's Mutagen refuses the
            // mutation with `MutationRateLimited` / "Rate limit exceeded"
            // before processing (throttled attempts don't consume budget).
            if !crate::mutation_rate_limit::can_do(&push.client_group_id) {
                responses.push(MutationResponse {
                    id,
                    result: MutationResult::Error(MutationError::App(
                        zero_cache_protocol::mutation_result::MutationAppError {
                            message: Some("Rate limit exceeded".into()),
                            details: None,
                        },
                    )),
                });
                continue;
            }
            // Authorize from the replicated snapshot before borrowing the
            // upstream client. `apply_crud_mutation` still confirms the
            // mutation ID when this is false, but runs no CRUD SQL.
            let authorized = self.authorize_crud_ops(&ops);
            let client = self.upstream_client.as_mut().expect("connected above");
            let result = zero_cache_mutagen::apply_mutation::apply_crud_mutation(
                client,
                &schema,
                &push.client_group_id,
                &crud.client_id,
                crud.id as i64,
                &ops,
                authorized,
                false, // not error mode
            )
            .await;
            match result {
                // Zero treats a replay as an idempotent no-op.  In
                // particular, it does not send a second mutation result to
                // the client: the original response was already delivered
                // (or the client will retry again).  Sending an
                // `alreadyProcessed` response here diverges from the real
                // Mutagen path and can make the client advance its response
                // bookkeeping twice.
                Ok(applied) if matches!(applied.check, MutationIdCheck::AlreadyProcessed(_)) => {
                    continue;
                }
                Ok(applied) => {
                    let result = match applied.check {
                        MutationIdCheck::Ok => MutationResult::Ok(MutationOk { data: None }),
                        MutationIdCheck::Unexpected(error) => {
                            MutationResult::Error(MutationError::Zero(MutationZeroError {
                                error: ZeroErrorKind::OooMutation,
                                details: Some(JsonValue::String(error.error_body.message)),
                            }))
                        }
                        MutationIdCheck::AlreadyProcessed(_) => unreachable!(),
                    };
                    responses.push(MutationResponse { id, result });
                }
                Err(e) => responses.push(MutationResponse {
                    id,
                    result: MutationResult::Error(MutationError::App(
                        zero_cache_protocol::mutation_result::MutationAppError {
                            message: Some(e.to_string()),
                            details: None,
                        },
                    )),
                }),
            }
        }
        self.mutation_responses_outcome(responses)
    }

    /// Resolves every custom-query AST missing from the patch by fetching its
    /// transform from `ZERO_QUERY_URL`, registering the successes on the
    /// inspector delegate. Returns the `(query name, error)` list of transforms
    /// that FAILED — the caller turns a non-empty list into a terminal
    /// connection error so the client stops waiting instead of hanging with the
    /// query stuck at `type:"unknown"` (a silent `warn!` used to leave the
    /// client spinning forever). Fetches run CONCURRENTLY so one slow/blocked
    /// app-query server cannot serialize/stall the whole init.
    async fn fetch_missing_custom_query_transforms_for_patch(
        &mut self,
        patch: &[UpQueriesPatchOp],
    ) -> Vec<(String, String)> {
        let Some(config) = self.custom_query_transform_http.as_ref() else {
            return Vec::new();
        };

        let mut to_fetch: Vec<(String, Vec<JsonValue>)> = Vec::new();
        for op in patch {
            let UpQueriesPatchOp::Put(p) = op else {
                continue;
            };
            if p.ast.is_some() {
                continue;
            }
            let Some(name) = p.name.as_deref() else {
                continue;
            };
            let args = p.args.clone().unwrap_or_default();
            if self
                .inspector_delegate
                .transform_custom_query(name, &args)
                .is_some()
            {
                continue;
            }
            crate::debug!(
                "fetching custom-query transform for '{name}' ({} arg(s))",
                args.len()
            );
            to_fetch.push((name.to_string(), args));
        }
        if to_fetch.is_empty() {
            return Vec::new();
        }

        // Snapshot the transform endpoint config once so each query can be
        // fetched in its own future (owned data, no borrow of `self`).
        let client = config.client.clone();
        let url = config.url.clone();
        let schema = config.schema.clone();
        let app_id = config.app_id.clone();
        let api_key = config.api_key.clone();
        let custom_headers = config.custom_headers.clone();
        let request_headers = config.request_headers.clone();
        let auth_raw = config.auth_raw.clone().or_else(|| self.auth_raw.clone());
        let cookie = config.cookie.clone();
        let origin = config.origin.clone();
        let now_ms = config.now_ms;

        let fetches = to_fetch.into_iter().map(|(name, args)| {
            fetch_custom_query_transform(
                client.clone(),
                url.clone(),
                schema.clone(),
                app_id.clone(),
                api_key.clone(),
                custom_headers.clone(),
                request_headers.clone(),
                auth_raw.clone(),
                cookie.clone(),
                origin.clone(),
                now_ms,
                name,
                args,
            )
        });
        let results = futures_util::future::join_all(fetches).await;

        let mut failures = Vec::new();
        for (name, args, outcome) in results {
            match outcome {
                Ok(ast) => {
                    crate::debug!("registered transform for query '{name}'");
                    self.inspector_delegate
                        .add_custom_query_transform(&name, &args, ast);
                }
                Err(e) => {
                    crate::warn!("custom-query transform for '{name}' FAILED: {e}");
                    failures.push((name, e));
                }
            }
        }
        failures
    }

    /// Surfaces custom-query transform failures loudly (a bare `warn!` per query
    /// buried them). These are NOT connection-terminal: closing the socket would
    /// also drop unrelated mutations and successfully-transformed queries in the
    /// same transition. Upstream fails the individual query instead; until the
    /// port delivers a per-query error patch, the un-transformed query simply
    /// contributes no plan/rows and the rest of the transition proceeds.
    fn log_transform_failures(failures: &[(String, String)]) {
        for (name, error) in failures {
            crate::error!("custom-query transform for '{name}' FAILED: {error}");
        }
    }

    async fn apply_inspect_async(&mut self, body: InspectUpBody) -> HandlerOutcome {
        let InspectUpBody::AnalyzeQuery {
            value,
            ast,
            name,
            args,
            ..
        } = &body
        else {
            return self.apply_inspect(body);
        };

        // Gate before any (network) transform work, matching upstream's
        // authenticate-first `handleInspect`.
        if let Some(unauthenticated) = self.inspect_unauthenticated_response(body.id()) {
            return unauthenticated;
        }

        let args = args.clone().unwrap_or_default();
        if ast.is_none()
            && value.is_none()
            && name.as_deref().is_some_and(|name| {
                self.inspector_delegate
                    .transform_custom_query(name, &args)
                    .is_none()
            })
        {
            if let Some(name) = name.as_deref() {
                if let Err(e) = self
                    .fetch_and_register_custom_query_transform(name, &args)
                    .await
                {
                    return HandlerOutcome::send(vec![inspect_down_message_json(
                        &InspectDownBody::Error {
                            id: body.id().to_string(),
                            value: e,
                        },
                    )]);
                }
            }
        }

        self.apply_inspect(body)
    }

    async fn fetch_and_register_custom_query_transform(
        &mut self,
        name: &str,
        args: &[JsonValue],
    ) -> Result<(), String> {
        let Some(config) = self.custom_query_transform_http.as_mut() else {
            return Ok(());
        };
        let id = hash_of_name_and_args(name, args);
        let request = vec![TransformRequestQuery {
            id: id.clone(),
            name: name.to_string(),
            args: args.to_vec(),
        }];
        let headers = HeaderOptions {
            api_key: config.api_key.as_deref(),
            custom_headers: &config.custom_headers,
            request_headers: &config.request_headers,
            auth_raw: config.auth_raw.as_deref().or(self.auth_raw.as_deref()),
            cookie: config.cookie.as_deref(),
            origin: config.origin.as_deref(),
        };

        let response = fetch_and_shape_transform_response(
            &config.client,
            &config.url,
            &config.schema,
            &config.app_id,
            &headers,
            &request,
            vec![],
            &mut config.cache,
            |cache_id| cache_id.to_string(),
            config.now_ms,
        )
        .await
        .map_err(|e| e.to_string())?;

        match response {
            HashedTransformResponse::Failed(body) => Err(body.message),
            HashedTransformResponse::Success { result, .. } => {
                for item in result {
                    match item {
                        TransformedOrErrored::Ok(t) if t.id == id => {
                            self.inspector_delegate.add_custom_query_transform(
                                name,
                                args,
                                t.transformed_ast,
                            );
                            return Ok(());
                        }
                        TransformedOrErrored::Errored(e) if e.id == id => {
                            return Err(e
                                .message
                                .unwrap_or_else(|| "custom query transform failed".to_string()));
                        }
                        _ => {}
                    }
                }
                Err("custom query transform response did not include requested query".to_string())
            }
        }
    }

    /// The upstream inspect auth gate (`handleInspect`): every op except
    /// `authenticate` requires the client group to have authenticated with the
    /// admin password. Returns the `authenticated:false` response to send when
    /// the gate fails, or `None` when the caller may proceed.
    fn inspect_unauthenticated_response(&self, id: &str) -> Option<HandlerOutcome> {
        if self
            .inspector_delegate
            .is_authenticated(&self.core.client_group_id, self.inspect_development_mode)
        {
            None
        } else {
            Some(HandlerOutcome::send(vec![inspect_down_message_json(
                &InspectDownBody::Authenticated {
                    id: id.to_string(),
                    value: false,
                },
            )]))
        }
    }

    fn apply_inspect(&mut self, body: InspectUpBody) -> HandlerOutcome {
        if let InspectUpBody::AnalyzeQuery {
            id,
            value,
            options,
            ast,
            name,
            args,
        } = body
        {
            // analyze-query must pass the same admin-password gate as every other
            // non-authenticate inspect op (upstream `handleInspect`); the special
            // case below otherwise bypasses `handle_inspect`'s check entirely.
            if let Some(unauthenticated) = self.inspect_unauthenticated_response(&id) {
                return unauthenticated;
            }
            let args = args.unwrap_or_default();
            let synced_query_id = name
                .as_deref()
                .map(|name| hash_of_name_and_args(name, &args));
            let transformed = match (ast.as_ref().or(value.as_ref()), name.as_deref()) {
                (Some(ast), _) => Some(ast.clone()),
                (None, Some(name)) => self
                    .inspector_delegate
                    .transform_custom_query(name, &args)
                    .cloned(),
                (None, None) => None,
            };
            let transformed = transformed.map(|ast| self.apply_read_permissions(&id, ast));
            let unresolved_custom_name = if transformed.is_none() {
                name.as_deref()
            } else {
                None
            };
            let catalog = transformed
                .as_ref()
                .ok_or(AnalyzeQueryError::MissingAst)
                .and_then(|ast| {
                    crate::analyze_query::analyze_catalog_from_sqlite_ast(&self.core.db, ast)
                });
            let response = match catalog.and_then(|catalog| {
                analyze_sqlite_ast_query(
                    &self.core.db,
                    &catalog,
                    transformed.as_ref(),
                    unresolved_custom_name,
                    options.as_ref(),
                    synced_query_id.as_deref(),
                    &self.core.row_records,
                    &self.core.row_bodies,
                )
            }) {
                Ok(value) => InspectDownBody::AnalyzeQuery { id, value },
                Err(e) => InspectDownBody::Error {
                    id,
                    value: e.to_string(),
                },
            };
            return HandlerOutcome::send(vec![inspect_down_message_json(&response)]);
        }

        let admin_password = self.inspect_admin_password.as_deref();
        let response = handle_inspect(
            body,
            &self.core.cvr_handler.cvr,
            &self.core.row_records,
            &mut self.inspector_delegate,
            &self.core.client_group_id,
            self.inspect_protocol_version,
            &self.inspect_server_version,
            self.inspect_development_mode,
            |password| admin_password.is_some_and(|expected| expected == password),
        );
        HandlerOutcome::send(vec![inspect_down_message_json(&response)])
    }

    fn apply_read_permissions(
        &self,
        query_id: &str,
        ast: zero_cache_protocol::ast::Ast,
    ) -> zero_cache_protocol::ast::Ast {
        let Some(permissions) = &self.read_permissions else {
            return ast;
        };
        transform_and_hash_query_with_auth_data(query_id, &ast, permissions, &self.auth_data, false)
            .transformed_ast
    }

    /// Computes the authorization verdict for one CRUD mutation against the
    /// same replica snapshot used for live reads.  A configured permissions
    /// document is fail-closed: an unknown table, forged primary-key shape,
    /// missing row, malformed policy, or database read failure all deny the
    /// write.  With no configured document we retain the historical opt-in
    /// behavior for deployments that have not supplied `ZERO_SCHEMA_JSON`.
    fn authorize_crud_ops(&self, ops: &[CrudOp]) -> bool {
        let Some(permissions) = &self.read_permissions else {
            return true;
        };
        let Ok(table_primary_keys) = self.replica_table_primary_keys() else {
            return false;
        };
        if !self.ops_match_replica_schema(ops, &table_primary_keys) {
            return false;
        }
        let bound_permissions = bind_permissions_auth_data(permissions, &self.auth_data);
        let tables = bound_permissions.tables.as_ref();
        let Some(tables) = tables else {
            return false;
        };
        let known_tables = table_primary_keys.keys().cloned().collect();
        let exists: ExistsFn<'_> = Rc::new(|related, row| self.policy_related_exists(related, row));

        authorize_mutation_with_exists(
            ops.to_vec(),
            &known_tables,
            tables,
            |upsert| {
                Self::key_values(&upsert.primary_key, &upsert.value)
                    .and_then(|key| self.lookup_replica_row(&upsert.table_name, &key))
                    .is_some()
            },
            |table, key| self.lookup_replica_row(table, key),
            |op| self.resulting_replica_row(op),
            exists,
        )
        .unwrap_or(false)
    }

    fn replica_table_primary_keys(&self) -> Result<BTreeMap<String, Vec<String>>, ()> {
        let tables = list_tables(&self.core.db).map_err(|_| ())?;
        Ok(tables
            .into_iter()
            .filter_map(|table| table.primary_key.map(|key| (table.name, key)))
            .collect())
    }

    fn ops_match_replica_schema(
        &self,
        ops: &[CrudOp],
        table_primary_keys: &BTreeMap<String, Vec<String>>,
    ) -> bool {
        ops.iter().all(|op| {
            let (table_name, primary_key, supplied_key) = match op {
                CrudOp::Insert(op) => (
                    &op.table_name,
                    &op.primary_key,
                    Self::key_values(&op.primary_key, &op.value),
                ),
                CrudOp::Upsert(op) => (
                    &op.table_name,
                    &op.primary_key,
                    Self::key_values(&op.primary_key, &op.value),
                ),
                CrudOp::Update(op) => (
                    &op.table_name,
                    &op.primary_key,
                    Self::key_values(&op.primary_key, &op.value),
                ),
                CrudOp::Delete(op) => (
                    &op.table_name,
                    &op.primary_key,
                    Self::key_values_from_record(&op.primary_key, &op.value),
                ),
            };
            table_primary_keys.get(table_name) == Some(primary_key) && supplied_key.is_some()
        })
    }

    fn key_values(
        primary_key: &[String],
        row: &[(String, JsonValue)],
    ) -> Option<Vec<(String, JsonValue)>> {
        primary_key
            .iter()
            .map(|column| {
                row.iter()
                    .find(|(name, _)| name == column)
                    .map(|(_, value)| (column.clone(), value.clone()))
            })
            .collect()
    }

    fn key_values_from_record(
        primary_key: &[String],
        row: &BTreeMap<String, JsonValue>,
    ) -> Option<Vec<(String, JsonValue)>> {
        primary_key
            .iter()
            .map(|column| row.get(column).map(|value| (column.clone(), value.clone())))
            .collect()
    }

    fn quote_sql_identifier(identifier: &str) -> String {
        format!(r#""{}""#, identifier.replace('"', "\"\""))
    }

    fn json_to_sqlite(value: &JsonValue) -> zero_cache_sqlite::Value {
        match value {
            JsonValue::Null => zero_cache_sqlite::Value::Null,
            JsonValue::Bool(value) => zero_cache_sqlite::Value::Integer(i64::from(*value)),
            JsonValue::Number(value)
                if value.is_finite()
                    && value.fract() == 0.0
                    && *value >= i64::MIN as f64
                    && *value <= i64::MAX as f64 =>
            {
                zero_cache_sqlite::Value::Integer(*value as i64)
            }
            JsonValue::Number(value) => zero_cache_sqlite::Value::Real(*value),
            JsonValue::BigInt(value) => zero_cache_sqlite::Value::Text(value.to_string()),
            JsonValue::String(value) => zero_cache_sqlite::Value::Text(value.clone()),
            JsonValue::Array(_) | JsonValue::Object(_) => {
                zero_cache_sqlite::Value::Text(value.stringify())
            }
        }
    }

    fn sqlite_to_json(value: &zero_cache_sqlite::Value) -> JsonValue {
        match value {
            zero_cache_sqlite::Value::Null => JsonValue::Null,
            zero_cache_sqlite::Value::Integer(value) => JsonValue::Number(*value as f64),
            zero_cache_sqlite::Value::Real(value) => JsonValue::Number(*value),
            zero_cache_sqlite::Value::Text(value) => JsonValue::String(value.clone()),
            zero_cache_sqlite::Value::Blob(value) => {
                JsonValue::String(String::from_utf8_lossy(value).into_owned())
            }
        }
    }

    fn lookup_replica_row(&self, table_name: &str, key: &[(String, JsonValue)]) -> Option<ZqlRow> {
        if key.is_empty() {
            return None;
        }
        let clauses = key
            .iter()
            .map(|(column, _)| format!("{} IS ?", Self::quote_sql_identifier(column)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "SELECT * FROM {} WHERE {clauses} LIMIT 1",
            Self::quote_sql_identifier(table_name)
        );
        let params = key
            .iter()
            .map(|(_, value)| Self::json_to_sqlite(value))
            .collect::<Vec<_>>();
        self.core.db.get(&sql, &params).ok().flatten().map(|row| {
            row.into_iter()
                .map(|(column, value)| (column, Self::sqlite_to_json(&value)))
                .collect()
        })
    }

    fn all_replica_rows(&self, table_name: &str) -> Option<Vec<ZqlRow>> {
        let sql = format!("SELECT * FROM {}", Self::quote_sql_identifier(table_name));
        self.core.db.query_uncached(&sql, &[]).ok().map(|rows| {
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|(column, value)| (column, Self::sqlite_to_json(&value)))
                        .collect()
                })
                .collect()
        })
    }

    fn resulting_replica_row(&self, op: &NormalizedCrudOp) -> ZqlRow {
        match op {
            NormalizedCrudOp::Insert(op) => op.value.clone(),
            NormalizedCrudOp::Update(op) => {
                let Some(key) = Self::key_values(&op.primary_key, &op.value) else {
                    return Vec::new();
                };
                let Some(mut row) = self.lookup_replica_row(&op.table_name, &key) else {
                    return Vec::new();
                };
                for (column, value) in &op.value {
                    if let Some((_, existing)) = row.iter_mut().find(|(name, _)| name == column) {
                        *existing = value.clone();
                    } else {
                        row.push((column.clone(), value.clone()));
                    }
                }
                row
            }
            NormalizedCrudOp::Delete(_) => Vec::new(),
        }
    }

    fn policy_related_exists(&self, related: &CorrelatedSubquery, parent: &ZqlRow) -> bool {
        if related.correlation.parent_field.is_empty()
            || related.correlation.parent_field.len() != related.correlation.child_field.len()
        {
            return false;
        }
        let Some(children) = self.all_replica_rows(&related.subquery.table) else {
            return false;
        };
        children.into_iter().any(|child| {
            let correlated = related
                .correlation
                .parent_field
                .iter()
                .zip(&related.correlation.child_field)
                .all(|(parent_field, child_field)| {
                    let null = JsonValue::Null;
                    let parent_value = parent
                        .iter()
                        .find(|(name, _)| name == parent_field)
                        .map(|(_, value)| value)
                        .unwrap_or(&null);
                    let child_value = child
                        .iter()
                        .find(|(name, _)| name == child_field)
                        .map(|(_, value)| value)
                        .unwrap_or(&null);
                    values_equal(parent_value, child_value)
                });
            correlated
                && related
                    .subquery
                    .where_
                    .as_ref()
                    .map(|condition| self.policy_condition_matches(condition, &child))
                    .unwrap_or(true)
        })
    }

    fn policy_condition_matches(&self, condition: &Condition, row: &ZqlRow) -> bool {
        let exists: ExistsFn<'_> = Rc::new(|related, row| self.policy_related_exists(related, row));
        create_predicate_with_exists(condition, exists)(row)
    }

    /// Applies a `push`'s CRUD mutations and returns a `pushResponse` frame.
    ///
    /// Scope decision: `serve_connection`'s handler contract
    /// (`FnMut(ConnectionAction) -> HandlerOutcome`) is synchronous, but the
    /// real upstream-Postgres executor
    /// (`zero_cache_mutagen::apply_mutation::apply_crud_mutation`, live-tested
    /// separately this round) is async `tokio-postgres` I/O — calling it here
    /// would require blocking the connection's task or a broader async-handler
    /// refactor across `serve_connection`/`sync_server`. Instead this applies
    /// each mutation's ops directly against the connection's own (synchronous)
    /// SQLite replica handle via the SAME `plan_mutation_sql` statement
    /// planner and the SAME `crud_ops_json`/`check_mutation_id` decode/check
    /// logic the real executor uses — proving the full decode -> plan -> apply
    /// -> respond pipeline for real, with the one substitution being which
    /// database receives the writes. A production deployment wires
    /// `apply_crud_mutation` into an async-capable handler instead.
    fn apply_push(&mut self, push: &PushBody) -> HandlerOutcome {
        let mut responses = Vec::with_capacity(push.mutations.len());
        for mutation in &push.mutations {
            let id = mutation.id();
            let Mutation::Crud(crud) = mutation else {
                // Custom mutators are a separate, unported subsystem (they
                // call out to a user-supplied API server, not local SQL).
                responses.push(MutationResponse {
                    id,
                    result: MutationResult::Error(MutationError::App(
                        zero_cache_protocol::mutation_result::MutationAppError {
                            message: Some(
                                "custom mutations are not supported by this handler".into(),
                            ),
                            details: None,
                        },
                    )),
                });
                continue;
            };

            let last = *self
                .core
                .last_mutation_ids
                .get(&crud.client_id)
                .unwrap_or(&0);
            let received = crud.id as i64;
            match check_mutation_id(&crud.client_id, received, last + 1) {
                MutationIdCheck::Unexpected(e) => {
                    responses.push(MutationResponse {
                        id,
                        result: MutationResult::Error(MutationError::Zero(MutationZeroError {
                            error: ZeroErrorKind::OooMutation,
                            details: Some(JsonValue::String(e.error_body.message.clone())),
                        })),
                    });
                    continue;
                }
                // A stale retry is an idempotent no-op in upstream Zero.  It
                // is deliberately omitted from pushResponse; emitting an
                // `alreadyProcessed` mutation result is observably different
                // and can desynchronise the client's pending-response queue.
                MutationIdCheck::AlreadyProcessed(_) => continue,
                MutationIdCheck::Ok => {}
            }
            self.core
                .last_mutation_ids
                .insert(crud.client_id.clone(), received);

            match crud_ops_from_json(&crud.ops_json) {
                Ok(ops) => {
                    // An unauthorized mutation is still acknowledged (the
                    // last-mutation-id was advanced above), but it schedules
                    // no SQL — matching mutagen's authorization contract.
                    let authorized = self.authorize_crud_ops(&ops);
                    let mut apply_err = None;
                    for stmt in plan_mutation_sql(&ops, false, authorized) {
                        if let Err(e) = self.core.db.exec(&stmt) {
                            apply_err = Some(e.to_string());
                            break;
                        }
                    }
                    responses.push(MutationResponse {
                        id,
                        result: match apply_err {
                            None => MutationResult::Ok(MutationOk { data: None }),
                            Some(msg) => MutationResult::Error(MutationError::App(
                                zero_cache_protocol::mutation_result::MutationAppError {
                                    message: Some(msg),
                                    details: None,
                                },
                            )),
                        },
                    });
                }
                Err(e) => {
                    responses.push(MutationResponse {
                        id,
                        result: MutationResult::Error(MutationError::App(
                            zero_cache_protocol::mutation_result::MutationAppError {
                                message: Some(e.to_string()),
                                details: None,
                            },
                        )),
                    });
                }
            }
        }

        self.mutation_responses_outcome(responses)
    }

    /// Builds the `pushResponse` frame for a set of mutation responses and
    /// records them as pending (for `ackMutationResponses`).
    fn mutation_responses_outcome(&mut self, responses: Vec<MutationResponse>) -> HandlerOutcome {
        // Only relay results for THIS connection's client. A push can carry
        // mutations for OTHER clients in the group (Replicache re-pushes dead
        // clients' unconfirmed mutations through whichever client is connected);
        // upstream's pusher fans each result out to its own client's connection.
        // Sending another client's result here is FATAL on the client side
        // ("received mutation for the wrong client") and closes the socket.
        let own = self.core.cvr_handler.client_id().to_string();
        let (mine, others): (Vec<_>, Vec<_>) =
            responses.into_iter().partition(|r| r.id.client_id == own);
        if !others.is_empty() {
            crate::debug!(
                "dropping {} mutation result(s) for other clients in the group (not {own})",
                others.len()
            );
        }
        if mine.is_empty() {
            return HandlerOutcome::empty();
        }

        // An out-of-order mutation is fatal: upstream's pusher fails the
        // client's downstream (`#failDownstream`) with a `PushFailed` /
        // `OutOfOrderMutation` body instead of relaying the result, so the
        // client re-initializes and re-pushes in order. Relaying the oooMutation
        // as an ordinary `pushResponse` would leave the client's mutation queue
        // stuck. Any mutations after the offending one are dropped.
        if let Some(termination) =
            zero_cache_mutagen::pusher_response::find_fatal_terminations(&mine)
                .into_iter()
                .find(|termination| termination.client_id == own)
        {
            let body = zero_cache_protocol::error::PushFailedBody {
                reason: zero_cache_protocol::error::PushFailedReason::Server(
                    zero_cache_protocol::error_reason::ErrorReason::OutOfOrderMutation,
                ),
                mutation_ids: termination.mutation_ids,
                message: termination.message,
                details: None,
            };
            return HandlerOutcome {
                responses: vec![body.to_error_frame_json()],
                keep_open: false,
            };
        }

        self.pending_mutation_responses.extend(mine.clone());
        HandlerOutcome::send(vec![push_ok_message_json(&PushOk { mutations: mine })])
    }

    fn apply_ack_mutation_response(
        &mut self,
        ack: &zero_cache_protocol::push::AckMutationResponsesBody,
    ) -> HandlerOutcome {
        self.pending_mutation_responses
            .retain(|response| response.id != ack.mutation_id);
        HandlerOutcome::empty()
    }

    /// Builds the `_zero_cleanupResults` cleanup push body (pure; testable
    /// without the network). Mirrors upstream's `cleanupBody`: one `single`
    /// cleanup arg carrying the client group/client and `upToMutationID`.
    fn build_cleanup_push(
        client_group_id: &str,
        up_to: &zero_cache_protocol::mutation_id::MutationId,
        timestamp: f64,
    ) -> PushBody {
        let args = vec![zero_cache_shared::bigint_json::JsonValue::Object(vec![
            (
                "type".into(),
                zero_cache_shared::bigint_json::JsonValue::String("single".into()),
            ),
            (
                "clientGroupID".into(),
                zero_cache_shared::bigint_json::JsonValue::String(client_group_id.to_string()),
            ),
            (
                "clientID".into(),
                zero_cache_shared::bigint_json::JsonValue::String(up_to.client_id.clone()),
            ),
            (
                "upToMutationID".into(),
                zero_cache_shared::bigint_json::JsonValue::Number(up_to.id),
            ),
        ])];
        PushBody {
            client_group_id: client_group_id.to_string(),
            mutations: vec![Mutation::Custom(CustomMutation {
                // Fire-and-forget: not tracked, so id 0 (upstream comment).
                id: 0.0,
                client_id: up_to.client_id.clone(),
                name: CLEANUP_RESULTS_MUTATION_NAME.to_string(),
                args,
                timestamp,
            })],
            push_version: 1.0,
            schema_version: None,
            timestamp,
            request_id: format!(
                "cleanup-{}-{}-{}",
                client_group_id, up_to.client_id, up_to.id
            ),
            traceparent: None,
        }
    }

    fn apply_update_auth(
        &mut self,
        body: &zero_cache_protocol::update_auth::UpdateAuthBody,
    ) -> HandlerOutcome {
        let Some(verifier) = &self.auth_verifier else {
            // Opaque auth is forwarded to custom endpoints, but it carries no
            // server-trusted decoded claims and therefore cannot change
            // compiled permission evaluation.
            self.auth_raw = Some(body.auth.clone());
            return HandlerOutcome::empty();
        };
        let claims = match verifier.verify(&body.auth) {
            Ok(claims) => claims,
            Err(error) => {
                return HandlerOutcome {
                    responses: vec![format!(
                        r#"["error",{{"kind":"AuthInvalidated","message":"{}"}}]"#,
                        error.to_string().replace('"', "\\\"")
                    )],
                    keep_open: false,
                };
            }
        };
        if let Some(subject) = self.auth_subject.as_deref() {
            if subject != claims.sub {
                return HandlerOutcome {
                    responses: vec![
                        r#"["error",{"kind":"Unauthorized","message":"auth subject changed"}]"#
                            .to_string(),
                    ],
                    keep_open: false,
                };
            }
        }
        self.auth_raw = Some(body.auth.clone());
        self.auth_subject = Some(claims.sub);
        self.auth_data = claims.decoded;

        // A refreshed JWT can alter every transformed query. Re-hydrate now
        // so rows that were visible under the prior auth snapshot are removed
        // instead of lingering until an unrelated upstream commit.
        self.rehydrate_tracked()
    }

    /// Periodic auth revalidation (`ZERO_AUTH_REVALIDATE_INTERVAL_SECONDS`):
    /// re-verify the connection's current token (signature, expiry, issuer,
    /// and audience). Returns `Some(error frame)` when the token is no longer
    /// valid, so the caller closes the connection (matching upstream's
    /// scheduled-revalidation disconnect). Returns `None` when still valid, or
    /// when there is no server-trusted verifier or no token — nothing to
    /// revalidate.
    pub fn revalidate_auth(&self) -> Option<String> {
        let verifier = self.auth_verifier.as_ref()?;
        let token = self.auth_raw.as_deref()?;
        match verifier.verify(token) {
            Ok(_) => None,
            Err(error) => Some(format!(
                r#"["error",{{"kind":"AuthInvalidated","message":"{}"}}]"#,
                error.to_string().replace('"', "\\\"")
            )),
        }
    }

    fn apply_pull(&self, body: &PullRequestBody) -> HandlerOutcome {
        let last_mutation_id_changes = self
            .core
            .last_mutation_ids
            .iter()
            .map(|(client_id, last_mutation_id)| (client_id.clone(), *last_mutation_id as f64))
            .collect();
        HandlerOutcome::send(vec![pull_response_message_json(&PullResponseBody {
            cookie: version_to_cookie(self.core.cvr_handler.version())
                .expect("live CVR versions always encode as pull cookies"),
            request_id: body.request_id.clone(),
            last_mutation_id_changes,
        })])
    }

    /// Retains the client's declared schema (from `initConnection`) on the CVR
    /// via the validated `set_client_schema` transition: set-on-first-use, and
    /// on a later connection defensively reject (rather than overwrite) a schema
    /// that differs from the stored one — matching upstream's `setClientSchema`.
    /// Previously the received schema was dropped.
    ///
    /// A mismatch `Err` is currently swallowed (the stored schema is left
    /// intact, which is the invariant that matters); surfacing it as a
    /// downstream `error` frame that closes the connection is deferred with the
    /// rest of this demo handler's error-frame path. No-op when the client sent
    /// no schema.
    fn store_client_schema(&mut self, body: &zero_cache_protocol::connect::InitConnectionBody) {
        if let Some(schema) = &body.client_schema {
            // Canonicalize (sort tables/columns) before storing so the CVR's
            // order-sensitive JSON equality check matches upstream's
            // order-insensitive `deepEqual` on `setClientSchema`: two clients
            // sending the same schema in different key orders compare equal.
            let normalized = zero_cache_protocol::client_schema::normalize_client_schema(schema);
            let json = zero_cache_protocol::up_json::client_schema_to_json(&normalized);
            let _ = zero_cache_view_syncer::cvr_client_state::set_client_schema(
                &mut self.core.cvr_handler.cvr,
                &json,
            );
        }
    }

    fn apply_and_poke(&mut self, patch: &[UpQueriesPatchOp], force: bool) -> HandlerOutcome {
        let resolved_asts = self.resolve_patch_asts(patch);
        let (orig_version, patches) = match self.core.apply_desired_patch(patch, &resolved_asts) {
            Ok(applied) => applied,
            Err(error) => return Self::persistence_failure(error),
        };

        if force && patches.is_empty() {
            zero_cache_view_syncer::cvr_updater::ensure_new_version(
                &orig_version,
                &mut self.core.cvr_handler.cvr.version,
            );
        }
        self.build_poke_outcome(orig_version, patches, force)
    }

    fn apply_and_poke_staged(&mut self, patch: &[UpQueriesPatchOp], force: bool) -> HandlerOutcome {
        let resolved_asts = self.resolve_patch_asts(patch);
        let staged = match self.core.apply_desired_patch_staged(patch, &resolved_asts) {
            Ok(staged) => staged,
            Err(error) => return Self::persistence_failure(error),
        };
        let crate::group_transition::StagedPatch {
            orig_version,
            config_version,
            mut config,
            hydration,
        } = staged;
        if config.is_empty() || hydration.is_empty() {
            if hydration.is_empty() && !config.is_empty() {
                self.core.cvr_handler.cvr.version = config_version;
            }
            config.extend(hydration);
            return self.build_poke_outcome(orig_version, config, force);
        }
        self.pending_hydration = Some((config_version, hydration));
        self.build_poke_outcome(orig_version, config, force)
    }

    /// Takes the staged hydration poke after the initial desired-query poke has
    /// been written. The synced transport emits it immediately after the config
    /// poke is on the wire — server-pushed, as upstream's view-syncer run loop
    /// does, never gated on client input.
    pub fn take_pending_hydration(&mut self) -> HandlerOutcome {
        let Some((base, patches)) = self.pending_hydration.take() else {
            return HandlerOutcome::empty();
        };
        self.build_poke_outcome(base, patches, false)
    }

    /// Re-hydrates the connection's currently-desired queries against the
    /// (now-updated) replica and returns any resulting incremental poke. Called
    /// on each upstream commit so a client that already holds a query receives
    /// live row changes — the live-sync counterpart to [`apply_and_poke`], which
    /// only hydrates *newly* put queries. Returns `HandlerOutcome::empty()` when
    /// nothing the client tracks changed.
    pub fn rehydrate_tracked(&mut self) -> HandlerOutcome {
        let orig_version = self.core.cvr_handler.version().clone();
        // Re-executing an unchanged query doesn't bump the CVR version on its
        // own (`track_executed` only bumps on a transformation-hash change), but
        // the row-processing path requires the version to be above `orig` before
        // any row is emitted. Bump it once up front for the incremental poke.
        zero_cache_view_syncer::cvr_updater::ensure_new_version(
            &orig_version,
            &mut self.core.cvr_handler.cvr.version,
        );
        if self.core.query_pipeline.is_some() {
            return match self.core.advance_pipeline_to_patches() {
                Ok(patches) => self.build_poke_outcome(orig_version, patches, false),
                Err(error) => Self::persistence_failure(error),
            };
        }
        if self.core.desired_puts.is_empty() {
            return self.build_poke_outcome(orig_version, Vec::new(), false);
        }
        let puts: Vec<UpQueriesPutOp> = self.core.desired_puts.values().cloned().collect();
        let mut patches = Vec::new();
        for p in &puts {
            // Allow the already-tracked query to be RE-executed: clearing the
            // `tracked` marker lets `track_executed` run again, while the row
            // hydration still diffs against the CVR's existing rows, so only the
            // rows that actually changed since the last version are poked.
            self.core.tracked.remove(&p.hash);
            let transformed_ast = self.resolve_transformed_ast(p);
            match self
                .core
                .hydrate_put(p, transformed_ast, &orig_version, false, false)
            {
                Ok(hydration) => patches.extend(hydration),
                Err(error) => return Self::persistence_failure(error),
            }
        }
        self.build_poke_outcome(orig_version, patches, false)
    }

    /// Async fan-out variant: persist any row/config changes before exposing
    /// the resulting live poke to the client.
    pub async fn rehydrate_tracked_async(&mut self) -> HandlerOutcome {
        let _transition_guard = match self.cvr_transition_lock.clone() {
            Some(lock) => Some(lock.lock_owned().await),
            None => None,
        };
        // ZERO_YIELD_THRESHOLD_MS: the synchronous hydration below runs on
        // the executor; a budget over the configured threshold keeps this
        // task from monopolizing a worker across retries (upstream's
        // pipeline-driver #shouldYield during hydration).
        let mut yield_budget = zero_cache_view_syncer::yield_budget::YieldBudget::fixed(
            zero_cache_view_syncer::yield_budget::effective_yield_threshold(
                crate::query_engine_options::get().yield_threshold_ms,
                false,
            ),
        );
        const MAX_CVR_RETRIES: usize = 8;
        for attempt in 0..MAX_CVR_RETRIES {
            yield_budget.maybe_yield().await;
            if let Err(error) = self.refresh_durable_cvr().await {
                return Self::persistence_failure(error);
            }
            let delivery_checkpoint = (
                self.poke_seq,
                self.last_poke_version.clone(),
                self.poked_last_mutation_ids.clone(),
            );
            let before = self.core.cvr_handler.cvr.clone();
            let outcome = self.rehydrate_tracked();
            match self.core.persist_transition(&before).await {
                Ok(()) => return outcome,
                Err(error)
                    if error.contains("concurrent modification")
                        && attempt + 1 < MAX_CVR_RETRIES =>
                {
                    self.poke_seq = delivery_checkpoint.0;
                    self.last_poke_version = delivery_checkpoint.1;
                    self.poked_last_mutation_ids = delivery_checkpoint.2;
                    tokio::task::yield_now().await;
                }
                Err(error) => return Self::persistence_failure(error),
            }
        }
        unreachable!("CVR retry loop always returns")
    }

    /// Builds a 3-frame poke `HandlerOutcome` from accumulated patches, or empty
    /// if there are none.
    fn build_poke_outcome(
        &mut self,
        orig_version: zero_cache_view_syncer::cvr_version::CvrVersion,
        patches: Vec<zero_cache_view_syncer::client_patch::PatchToVersion>,
        force: bool,
    ) -> HandlerOutcome {
        use zero_cache_view_syncer::client_handler_poke::should_include_patch;
        let base_version = if self.poke_seq == 0 {
            self.initial_base_version.clone()
        } else {
            self.last_poke_version
                .clone()
                .or_else(|| Some(orig_version.clone()))
        };
        let patches: Vec<_> = patches
            .into_iter()
            .filter(|patch| should_include_patch(&patch.to_version, &base_version))
            .collect();
        self.core.refresh_last_mutation_ids();
        let lmid_changes: BTreeMap<String, f64> = self
            .core
            .last_mutation_ids
            .iter()
            .filter(|(client_id, last_mutation_id)| {
                self.poked_last_mutation_ids.get(*client_id) != Some(*last_mutation_id)
            })
            .map(|(client_id, last_mutation_id)| (client_id.clone(), *last_mutation_id as f64))
            .collect();

        if patches.is_empty() && lmid_changes.is_empty() && !force {
            return HandlerOutcome::empty();
        }
        let poke_id = {
            self.poke_seq += 1;
            format!("poke{}", self.poke_seq)
        };
        // The poke's `baseCookie` must equal the client's CURRENT cookie, or the
        // client's Replicache rejects it ("unexpected base cookie during sync").
        // When this CVR is still at the initial/empty version (a fresh client,
        // or a reconnecting one whose cookie we can't resume — this port keeps
        // CVR state per-connection), the base cookie must be NULL: a fresh sync
        // for the fresh client, a reset for the reconnecting one. Both accept
        // `null`; only a stale non-null base cookie errors. Subsequent pokes
        // then chain from the assigned (non-empty) version.
        let mut poke = if patches.is_empty() {
            let Ok(base_cookie) = version_to_nullable_cookie(&base_version) else {
                return HandlerOutcome::empty();
            };
            let Ok(cookie) = version_to_cookie(self.core.cvr_handler.version()) else {
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
            let Ok(Some(poke)) = zero_cache_view_syncer::poke_builder::build_poke(
                &poke_id,
                &base_version,
                &patches,
                None,
            ) else {
                return HandlerOutcome::empty();
            };
            poke
        };
        poke.part.last_mutation_id_changes = (!lmid_changes.is_empty()).then_some(lmid_changes);
        self.poked_last_mutation_ids = self.core.last_mutation_ids.clone();
        crate::debug!(
            "poke {} base={:?} cookie={} rows={} got={} desired={}",
            poke_id,
            poke.start.base_cookie,
            poke.end.cookie,
            poke.part.rows_patch.as_ref().map(|r| r.len()).unwrap_or(0),
            poke.part
                .got_queries_patch
                .as_ref()
                .map(|g| g.len())
                .unwrap_or(0),
            poke.part
                .desired_queries_patches
                .as_ref()
                .map(|d| d.len())
                .unwrap_or(0),
        );
        let advertised_version = cookie_to_version(Some(&poke.end.cookie)).ok().flatten();
        let start = poke_message_json(&PokeMessage::Start(poke.start));
        let end = poke_message_json(&PokeMessage::End(poke.end));
        self.last_poke_version = advertised_version;
        let mut responses = vec![start];
        if let Some(rows) = poke.part.rows_patch.clone() {
            // Exact v1.7.0 ClientHandler rule: flush after 100 patches, not
            // after a byte or SQLite-row threshold. Query/LMID/mutation
            // patches already accumulated in the first body count toward the
            // first 100, so a got-query plus 1,000 rows becomes 11 parts.
            const PART_COUNT_FLUSH_THRESHOLD: usize = 100;
            let leading_patch_count = poke
                .part
                .desired_queries_patches
                .as_ref()
                .map(|patches| patches.values().map(Vec::len).sum::<usize>())
                .unwrap_or(0)
                + poke
                    .part
                    .got_queries_patch
                    .as_ref()
                    .map(Vec::len)
                    .unwrap_or(0)
                + poke
                    .part
                    .last_mutation_id_changes
                    .as_ref()
                    .map(BTreeMap::len)
                    .unwrap_or(0)
                + poke
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
                let mut part = poke.part.clone();
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
            responses.push(poke_message_json(&PokeMessage::Part(poke.part)));
        }
        responses.push(end);
        HandlerOutcome::send(responses)
    }

    /// Resolves the AST one desired PUT op hydrates with: the put's own AST
    /// or the registered custom-query transform, with THIS connection's
    /// compiled read permissions applied. Read policies are applied to BOTH
    /// raw client ASTs and already-resolved custom-query ASTs before any
    /// metadata lookup or SQLite read. Previously the live hydration path used
    /// `p.ast` directly, so a client could bypass the inspect-only authorizer
    /// by sending a raw AST in `desiredQueriesPatch`.
    fn resolve_transformed_ast(&self, p: &UpQueriesPutOp) -> Option<zero_cache_protocol::ast::Ast> {
        let args = p.args.clone().unwrap_or_default();
        let source_ast = p.ast.clone().or_else(|| {
            p.name
                .as_deref()
                .and_then(|name| self.inspector_delegate.transform_custom_query(name, &args))
                .cloned()
        });
        source_ast.map(|ast| self.apply_read_permissions(&p.hash, ast))
    }

    /// Resolves the transformed AST for every PUT in a desired-queries patch —
    /// the per-connection half (custom-query transform + read permissions) the
    /// group core's patch application consumes.
    fn resolve_patch_asts(
        &self,
        patch: &[UpQueriesPatchOp],
    ) -> HashMap<String, Option<zero_cache_protocol::ast::Ast>> {
        patch
            .iter()
            .filter_map(|op| match op {
                UpQueriesPatchOp::Put(p) => Some((p.hash.clone(), self.resolve_transformed_ast(p))),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    #[test]
    fn cleanup_push_matches_upstream_cleanup_body() {
        let up_to = zero_cache_protocol::mutation_id::MutationId {
            id: 7.0,
            client_id: "c1".into(),
        };
        let push = DesiredQueriesHandler::build_cleanup_push("cg1", &up_to, 1234.0);
        assert_eq!(push.client_group_id, "cg1");
        assert_eq!(push.push_version, 1.0);
        assert_eq!(push.request_id, "cleanup-cg1-c1-7");
        assert_eq!(push.mutations.len(), 1);
        let Mutation::Custom(m) = &push.mutations[0] else {
            panic!("expected a custom mutation");
        };
        assert_eq!(m.id, 0.0);
        assert_eq!(m.client_id, "c1");
        assert_eq!(m.name, CLEANUP_RESULTS_MUTATION_NAME);
        // One `single` cleanup arg carrying the group/client and upToMutationID.
        assert_eq!(m.args.len(), 1);
        let JsonValue::Object(fields) = &m.args[0] else {
            panic!("expected an object arg");
        };
        let get = |k: &str| fields.iter().find(|(name, _)| name == k).map(|(_, v)| v);
        assert!(matches!(get("type"), Some(JsonValue::String(s)) if s == "single"));
        assert!(matches!(get("clientGroupID"), Some(JsonValue::String(s)) if s == "cg1"));
        assert!(matches!(get("clientID"), Some(JsonValue::String(s)) if s == "c1"));
        assert!(matches!(get("upToMutationID"), Some(JsonValue::Number(n)) if *n == 7.0));
    }
    use tokio_tungstenite::tungstenite::Message;

    #[test]
    fn log_transform_failures_does_not_panic_on_special_chars() {
        // Failures are logged loudly (not connection-terminal): closing the
        // socket would also drop unrelated mutations and successful queries in
        // the same transition. This just exercises the logging path.
        let failures = vec![
            ("friendsState".to_string(), "401 Unauthorized".to_string()),
            (
                "gameMessages".to_string(),
                r#"server said "no" \ bad"#.to_string(),
            ),
        ];
        DesiredQueriesHandler::log_transform_failures(&failures);
        DesiredQueriesHandler::log_transform_failures(&[]);
    }

    #[derive(Debug, thiserror::Error)]
    #[error("db error")]
    struct OuterDbError {
        #[source]
        source: InnerDbError,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("duplicate key violates unique constraint")]
    struct InnerDbError;

    #[test]
    fn cvr_errors_include_the_database_source_chain() {
        let error = OuterDbError {
            source: InnerDbError,
        };
        assert_eq!(
            format_error_chain(&error),
            "db error: duplicate key violates unique constraint"
        );
    }
    fn seeded_db() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        db.run(
            "INSERT INTO issue (id, title) VALUES (1, 'wired end to end')",
            &[],
        )
        .unwrap();
        db
    }

    #[test]
    fn initial_poke_includes_replicated_last_mutation_ids() {
        let db = seeded_db();
        db.exec(
            r#"CREATE TABLE "zero_0.clients" (
                "clientGroupID" TEXT NOT NULL,
                "clientID" TEXT NOT NULL,
                "lastMutationID" INTEGER NOT NULL
            );
            INSERT INTO "zero_0.clients" VALUES ('group1', 'old-client', 7);
            INSERT INTO "zero_0.clients" VALUES ('other-group', 'ignored', 99);"#,
        )
        .unwrap();
        let mut handler = DesiredQueriesHandler::new(db, "group1", "new-client");
        let put = UpQueriesPatchOp::Put(zero_cache_protocol::queries_patch::UpQueriesPutOp {
            hash: "issue-q".into(),
            ttl: None,
            ast: Some(zero_cache_protocol::ast::Ast::table("issue")),
            name: None,
            args: None,
        });

        let poke = handler.on_action(ConnectionAction::UpdateDesiredQueries(
            zero_cache_protocol::change_desired_queries::ChangeDesiredQueriesBody {
                desired_queries_patch: vec![put],
                traceparent: None,
            },
        ));

        assert_eq!(poke.responses.len(), 3);
        assert!(
            poke.responses[1].contains(r#""lastMutationIDChanges":{"old-client":7}"#),
            "the initial poke must confirm mutations from the previous client in the group: {}",
            poke.responses[1]
        );
        assert!(!poke.responses[1].contains("ignored"));
    }

    #[test]
    fn metadata_only_commit_emits_last_mutation_id_poke() {
        let db = seeded_db();
        db.exec(
            r#"CREATE TABLE "zero_0.clients" (
                "clientGroupID" TEXT NOT NULL,
                "clientID" TEXT NOT NULL,
                "lastMutationID" INTEGER NOT NULL
            );
            INSERT INTO "zero_0.clients" VALUES ('group1', 'client1', 1);"#,
        )
        .unwrap();
        let mut handler = DesiredQueriesHandler::new(db, "group1", "client1");

        let first = handler.rehydrate_tracked();
        assert_eq!(first.responses.len(), 3);
        assert!(first.responses[1].contains(r#""lastMutationIDChanges":{"client1":1}"#));

        handler
            .core
            .db
            .run(
                r#"UPDATE "zero_0.clients" SET "lastMutationID" = 2 WHERE "clientID" = 'client1'"#,
                &[],
            )
            .unwrap();
        let second = handler.rehydrate_tracked();
        assert_eq!(second.responses.len(), 3);
        assert!(second.responses[1].contains(r#""lastMutationIDChanges":{"client1":2}"#));
    }

    /// Live-sync core: a handler reading a WAL replica hydrates a query
    /// (initial poke), and after the WRITER commits a new row,
    /// `rehydrate_tracked` emits an incremental poke carrying that row — the
    /// per-connection commit relay `serve_synced_connection` drives.
    #[test]
    fn rehydrate_tracked_pokes_new_and_deleted_rows() {
        let path = std::env::temp_dir()
            .join(format!("zc_livesync_{}.db", std::process::id()))
            .to_string_lossy()
            .into_owned();
        for s in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{s}"));
        }

        // Writer (the replicator, here simulated) owns the replica.
        let writer = StatementRunner::open_file(&path).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue (id, title) VALUES (1, 'first')", &[])
            .unwrap();

        // The connection reads its own read-only view.
        let reader = StatementRunner::open_file_readonly(&path).unwrap();
        let mut handler = DesiredQueriesHandler::new(reader, "cg1", "c1");

        // Desire `issue`; initial poke carries row 1.
        let put = UpQueriesPatchOp::Put(zero_cache_protocol::queries_patch::UpQueriesPutOp {
            hash: "issue-q".into(),
            ttl: None,
            ast: Some(zero_cache_protocol::ast::Ast::table("issue")),
            name: None,
            args: None,
        });
        let initial = handler.on_action(ConnectionAction::UpdateDesiredQueries(
            zero_cache_protocol::change_desired_queries::ChangeDesiredQueriesBody {
                desired_queries_patch: vec![put],
                traceparent: None,
            },
        ));
        assert_eq!(initial.responses.len(), 3, "initial 3-frame poke");
        assert!(
            initial.responses[1].contains("first"),
            "initial poke has row 1: {}",
            initial.responses[1]
        );

        // The writer commits a new row (an upstream change replicated in).
        writer
            .run(
                "INSERT INTO issue (id, title) VALUES (2, 'live-update')",
                &[],
            )
            .unwrap();

        // On the commit, re-hydration pokes the new row.
        let live = handler.rehydrate_tracked();
        assert_eq!(live.responses.len(), 3, "incremental 3-frame poke");
        assert!(
            live.responses[1].contains("live-update"),
            "live poke carries the newly committed row: {}",
            live.responses[1]
        );

        // A subsequent disappearance must reconcile against the prior CVR
        // row set and emit the same row-id as a delete. Passing empty existing
        // row collections to hydration used to silently leave clients stale.
        writer.run("DELETE FROM issue WHERE id = 2", &[]).unwrap();
        let deleted = handler.rehydrate_tracked();
        assert_eq!(deleted.responses.len(), 3, "incremental delete poke");
        assert!(
            deleted.responses[1].contains(r#""op":"del""#),
            "live poke carries a delete: {}",
            deleted.responses[1]
        );
        assert!(
            deleted.responses[1].contains(r#""id":{"id":2}"#),
            "delete identifies row 2: {}",
            deleted.responses[1]
        );
        assert!(
            !handler.core.row_bodies.iter().any(|(_, row)| row
                .iter()
                .any(|(field, value)| { field == "id" && *value == JsonValue::Number(2.0) })),
            "deleted row body must not remain in the connection cache"
        );

        drop(handler);
        drop(writer);
        for s in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{s}"));
        }
    }

    /// BYTE-PARITY ANCHOR for the group-ownership refactor (group-loop plan
    /// increment 1): scripts a connection through
    /// connect → initConnection(put) → replica commit + rehydrate → del and
    /// pins the EXACT ordered wire frames. Later increments (the per-group
    /// processor loop) must keep this frame sequence byte-identical on the
    /// flag-off path; this is the last test the plan's final cleanup removes.
    #[test]
    fn scripted_connection_frames_are_byte_identical() {
        let path = std::env::temp_dir()
            .join(format!("zc_framepin_{}.db", std::process::id()))
            .to_string_lossy()
            .into_owned();
        for s in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{s}"));
        }

        // Writer (the replicator, here simulated) owns the replica.
        let writer = StatementRunner::open_file(&path).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        writer
            .run(
                "INSERT INTO issue (id, title) VALUES (1, 'pinned row')",
                &[],
            )
            .unwrap();

        // "connect": a fresh handler over the connection's read-only view.
        let reader = StatementRunner::open_file_readonly(&path).unwrap();
        let mut handler = DesiredQueriesHandler::new(reader, "cg-pin", "c-pin");
        let mut frames: Vec<String> = Vec::new();

        // initConnection desiring `issue`.
        let init = handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                desired_queries_patch: vec![UpQueriesPatchOp::Put(
                    zero_cache_protocol::queries_patch::UpQueriesPutOp {
                        hash: "issue-q".into(),
                        ttl: None,
                        ast: Some(zero_cache_protocol::ast::Ast::table("issue")),
                        name: None,
                        args: None,
                    },
                )],
                ..Default::default()
            },
        )));
        assert!(init.keep_open);
        frames.extend(init.responses);

        // A replica commit, then the commit-driven re-hydration.
        writer
            .run("INSERT INTO issue (id, title) VALUES (2, 'live row')", &[])
            .unwrap();
        let live = handler.rehydrate_tracked();
        assert!(live.keep_open);
        frames.extend(live.responses);

        // The client drops the query.
        let del = handler.on_action(ConnectionAction::UpdateDesiredQueries(
            zero_cache_protocol::change_desired_queries::ChangeDesiredQueriesBody {
                desired_queries_patch: vec![UpQueriesPatchOp::Del(
                    zero_cache_protocol::queries_patch::QueriesDelOp {
                        hash: "issue-q".into(),
                    },
                )],
                traceparent: None,
            },
        ));
        assert!(del.keep_open);
        frames.extend(del.responses);

        let expected: Vec<String> = vec![
            // initConnection: one merged 3-frame poke (desired+got+rows).
            r#"["pokeStart",{"pokeID":"poke1","baseCookie":null}]"#.into(),
            concat!(
                r#"["pokePart",{"pokeID":"poke1","#,
                r#""desiredQueriesPatches":{"c-pin":[{"op":"put","hash":"issue-q"}]},"#,
                r#""gotQueriesPatch":[{"op":"put","hash":"issue-q"}],"#,
                r#""rowsPatch":[{"op":"put","tableName":"issue","value":{"id":1,"title":"pinned row"}}]}]"#,
            )
            .into(),
            r#"["pokeEnd",{"pokeID":"poke1","cookie":"00:01"}]"#.into(),
            // The commit's incremental poke carries only the new row.
            r#"["pokeStart",{"pokeID":"poke2","baseCookie":"00:01"}]"#.into(),
            concat!(
                r#"["pokePart",{"pokeID":"poke2","#,
                r#""rowsPatch":[{"op":"put","tableName":"issue","value":{"id":2,"title":"live row"}}]}]"#,
            )
            .into(),
            r#"["pokeEnd",{"pokeID":"poke2","cookie":"00:02"}]"#.into(),
            // The del: a desired-queries config poke.
            r#"["pokeStart",{"pokeID":"poke3","baseCookie":"00:02"}]"#.into(),
            r#"["pokePart",{"pokeID":"poke3","desiredQueriesPatches":{"c-pin":[{"op":"del","hash":"issue-q"}]}}]"#
                .into(),
            r#"["pokeEnd",{"pokeID":"poke3","cookie":"00:03"}]"#.into(),
        ];
        assert_eq!(frames, expected, "wire frames must stay byte-identical");

        drop(handler);
        drop(writer);
        for s in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{s}"));
        }
    }

    fn select_permissions(condition: zero_cache_protocol::ast::Condition) -> PermissionsConfig {
        zero_cache_auth::policy::PermissionsConfig {
            tables: Some(BTreeMap::from([(
                "issue".to_string(),
                zero_cache_auth::policy::TablePermissionsEntry {
                    row: Some(zero_cache_auth::policy::AssetPermissions {
                        select: Some(vec![condition]),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )])),
        }
    }

    async fn spawn_transform_response_server(response_body: String) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let body = response_body.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        format!("http://{addr}/query")
    }

    async fn spawn_transform_response_server_capturing_request(
        response_body: String,
    ) -> (String, tokio::sync::oneshot::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap_or(0);
            let _ = tx.send(String::from_utf8_lossy(&buf[..n]).into_owned());
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
        (format!("http://{addr}/query"), rx)
    }

    #[test]
    fn initialize_retains_the_client_schema_on_the_cvr() {
        use zero_cache_protocol::client_schema::{
            ClientSchema, ColumnSchema, TableSchema, ValueType,
        };
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "c1");

        // No schema stored before initConnection.
        assert_eq!(handler.core.cvr_handler.cvr.client_schema, None);

        let schema = ClientSchema {
            tables: vec![(
                "issue".into(),
                TableSchema {
                    columns: vec![(
                        "id".into(),
                        ColumnSchema {
                            value_type: ValueType::String,
                        },
                    )],
                    primary_key: vec!["id".into()],
                },
            )],
        };
        handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                client_schema: Some(schema.clone()),
                ..Default::default()
            },
        )));

        // The received schema is retained on the CVR as its JSON encoding.
        let stored = zero_cache_protocol::up_json::client_schema_to_json(&schema);
        assert_eq!(
            handler.core.cvr_handler.cvr.client_schema,
            Some(stored.clone())
        );

        // A later initConnection with a DIFFERENT schema must NOT overwrite the
        // stored one (validated `set_client_schema` rejects the change).
        let other = ClientSchema {
            tables: vec![(
                "issue".into(),
                TableSchema {
                    columns: vec![(
                        "id".into(),
                        ColumnSchema {
                            value_type: ValueType::Number,
                        },
                    )],
                    primary_key: vec!["id".into()],
                },
            )],
        };
        handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                client_schema: Some(other),
                ..Default::default()
            },
        )));
        assert_eq!(
            handler.core.cvr_handler.cvr.client_schema,
            Some(stored),
            "a mismatching schema is rejected, the original is retained"
        );
    }

    #[test]
    fn reconnect_with_empty_desired_patch_is_acknowledged() {
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "client1")
            .with_base_cookie(Some("00:01".into()));

        let outcome = handler.on_action(ConnectionAction::Initialize(Box::default()));

        assert_eq!(outcome.responses.len(), 3);
        assert!(outcome.responses[0].contains(r#""baseCookie":"00:01""#));
        assert!(outcome.responses[2].contains(r#""cookie":"00:02""#));
    }

    #[test]
    fn initialize_client_schema_comparison_is_column_order_insensitive() {
        use zero_cache_protocol::client_schema::{
            ClientSchema, ColumnSchema, TableSchema, ValueType,
        };
        let col = |vt| ColumnSchema { value_type: vt };
        // Same logical schema, columns declared in two different orders.
        let make = |cols: Vec<(&str, ValueType)>| ClientSchema {
            tables: vec![(
                "issue".into(),
                TableSchema {
                    columns: cols
                        .into_iter()
                        .map(|(n, vt)| (n.to_string(), col(vt)))
                        .collect(),
                    primary_key: vec!["id".into()],
                },
            )],
        };
        let schema_a = make(vec![
            ("id", ValueType::String),
            ("title", ValueType::String),
        ]);
        let schema_b = make(vec![
            ("title", ValueType::String),
            ("id", ValueType::String),
        ]);

        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "c1");
        let init = |s| {
            ConnectionAction::Initialize(Box::new(
                zero_cache_protocol::connect::InitConnectionBody {
                    client_schema: Some(s),
                    ..Default::default()
                },
            ))
        };
        handler.on_action(init(schema_a));
        let after_first = handler.core.cvr_handler.cvr.client_schema.clone();
        // The reordered-but-equivalent schema must be accepted (normalized to
        // the same canonical form), leaving the stored schema unchanged.
        handler.on_action(init(schema_b));
        assert_eq!(
            handler.core.cvr_handler.cvr.client_schema, after_first,
            "reordered columns normalize to the same schema; not treated as a mismatch"
        );
    }

    #[test]
    fn inspect_action_returns_encoded_downstream_frame() {
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            seeded_db(),
            "group1",
            "client1",
            51,
            "test-version".to_string(),
            true,
            None,
        );

        let outcome = handler.on_action(ConnectionAction::Inspect(
            zero_cache_protocol::inspect_up::InspectUpBody::Version {
                id: "inspect-version".to_string(),
            },
        ));

        assert_eq!(outcome.responses.len(), 1);
        assert_eq!(
            outcome.responses[0],
            r#"["inspect",{"id":"inspect-version","op":"version","value":"test-version"}]"#
        );
    }

    /// The full loop, for real: `run_accept_loop` spawns a connection, its
    /// `make_handler` factory builds a `DesiredQueriesHandler` over a REAL
    /// seeded SQLite replica, a REAL client sends `initConnection` desiring
    /// an AST query, and the response poke — sent over the REAL
    /// socket by `serve_connection`'s normal send path, no test-only
    /// shortcut — carries the REAL row title from SQLite.
    #[tokio::test]
    async fn run_accept_loop_serves_real_hydrated_pokes_end_to_end() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |_id| {
                    let db = seeded_db();
                    let mut handler = DesiredQueriesHandler::new(db, "group1", "c1");
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-all","ast":{"table":"issue"}}
                ]}]"#,
            ))
            .await
            .unwrap();

        // pokeStart, pokePart (with the real row), pokeEnd.
        let start = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(start.contains("pokeStart"), "got {start}");
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("pokePart"), "got {part}");
        assert!(
            part.contains("wired end to end"),
            "real replica content on the wire: {part}"
        );
        let end = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(end.contains("pokeEnd"), "got {end}");

        server.await.unwrap();
    }

    #[test]
    fn analyze_query_requires_inspector_authentication() {
        // Non-development mode + an admin password configured, and a unique
        // group id so the global authenticated-set from other tests can't leak.
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            seeded_db(),
            "analyze-auth-gate-group",
            "c1",
            51,
            "v".to_string(),
            false,
            Some("admin-pw".to_string()),
        );
        let analyze = InspectUpBody::AnalyzeQuery {
            id: "a1".into(),
            value: None,
            options: None,
            ast: None,
            name: None,
            args: None,
        };
        let outcome = handler.apply_inspect(analyze);
        // Unauthenticated => `authenticated:false`, analysis never runs.
        assert_eq!(outcome.responses.len(), 1);
        assert!(
            outcome.responses[0].contains("\"op\":\"authenticated\"")
                && outcome.responses[0].contains("\"value\":false"),
            "expected authenticated:false gate, got {}",
            outcome.responses[0]
        );
    }

    #[tokio::test]
    async fn run_accept_loop_serves_real_inspect_version_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = seeded_db();
                    let mut handler = DesiredQueriesHandler::with_inspect_options(
                        db,
                        "group1",
                        &format!("client{id}"),
                        51,
                        "inspect-live-test".to_string(),
                        true,
                        None,
                    );
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();
        client
            .send(Message::text(
                r#"["inspect",{"op":"version","id":"inspect-version"}]"#,
            ))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            response,
            r#"["inspect",{"id":"inspect-version","op":"version","value":"inspect-live-test"}]"#
        );

        server.await.unwrap();
    }

    #[tokio::test]
    async fn live_inspect_queries_include_hydrated_row_count() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = seeded_db();
                    let mut handler = DesiredQueriesHandler::with_inspect_options(
                        db,
                        "group1",
                        &format!("client{id}"),
                        51,
                        "inspect-live-test".to_string(),
                        true,
                        None,
                    );
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-all","ast":{"table":"issue"}}
                ]}]"#,
            ))
            .await
            .unwrap();
        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let _part = client.next().await.unwrap().unwrap().into_text().unwrap();
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        client
            .send(Message::text(
                r#"["inspect",{"op":"queries","id":"inspect-queries"}]"#,
            ))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(response.contains("\"op\":\"queries\""), "got {response}");
        assert!(
            response.contains("\"queryID\":\"issue-all\""),
            "got {response}"
        );
        assert!(response.contains("\"rowCount\":1"), "got {response}");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn live_inspect_analyze_query_reads_real_sqlite_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec("INSERT INTO issue (id, title) VALUES (1, 'match me'), (2, 'skip me')")
                        .unwrap();
                    let mut handler = DesiredQueriesHandler::with_inspect_options(
                        db,
                        "group1",
                        &format!("client{id}"),
                        51,
                        "inspect-live-test".to_string(),
                        true,
                        None,
                    );
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();
        client
            .send(Message::text(
                r#"["inspect",{"op":"analyze-query","id":"inspect-analyze",
                    "ast":{"table":"issue",
                        "orderBy":[["id","asc"]],
                        "start":{"row":{"id":1},"exclusive":true},
                        "limit":1
                    },
                    "options":{"vendedRows":true}
                }]"#,
            ))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(
            response.contains("\"op\":\"analyze-query\""),
            "got {response}"
        );
        assert!(response.contains("\"readRowCount\":1"), "got {response}");
        assert!(response.contains("\"skip me\""), "got {response}");
        assert!(!response.contains("match me"), "got {response}");
        assert!(response.contains("\"sqlitePlans\""), "got {response}");

        server.await.unwrap();
    }

    #[test]
    fn inspect_analyze_query_applies_configured_read_permissions() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        db.exec("INSERT INTO issue (id, title) VALUES (1, 'allowed'), (2, 'denied')")
            .unwrap();
        let allowed_title = zero_cache_protocol::ast::Condition::Simple {
            op: zero_cache_protocol::ast::SimpleOperator::Eq,
            left: zero_cache_protocol::ast::ValuePosition::Column(
                zero_cache_protocol::ast::ColumnReference {
                    name: "title".to_string(),
                },
            ),
            right: zero_cache_protocol::ast::ValuePosition::Literal(
                zero_cache_protocol::ast::LiteralValue::String("allowed".to_string()),
            ),
        };
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            db,
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        )
        .with_read_permissions(select_permissions(allowed_title));

        let outcome = handler.on_action(ConnectionAction::Inspect(
            zero_cache_protocol::inspect_up::InspectUpBody::AnalyzeQuery {
                id: "inspect-authorized".to_string(),
                value: None,
                options: Some(zero_cache_protocol::inspect_up::AnalyzeQueryOptions {
                    vended_rows: Some(true),
                    synced_rows: None,
                    join_plans: None,
                }),
                ast: Some(zero_cache_protocol::ast::Ast::table("issue")),
                name: None,
                args: None,
            },
        ));

        assert_eq!(outcome.responses.len(), 1);
        let response = &outcome.responses[0];
        assert!(
            response.contains("\"op\":\"analyze-query\""),
            "got {response}"
        );
        assert!(response.contains("\"readRowCount\":1"), "got {response}");
        assert!(response.contains("\"allowed\""), "got {response}");
        assert!(!response.contains("\"denied\""), "got {response}");
    }

    #[test]
    fn inspect_analyze_query_introspects_tables_outside_demo_catalog() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE project (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.exec("INSERT INTO project (id, name) VALUES (1, 'from introspected schema')")
            .unwrap();
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            db,
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        );

        let outcome = handler.on_action(ConnectionAction::Inspect(
            zero_cache_protocol::inspect_up::InspectUpBody::AnalyzeQuery {
                id: "inspect-introspected".to_string(),
                value: None,
                options: Some(zero_cache_protocol::inspect_up::AnalyzeQueryOptions {
                    vended_rows: Some(true),
                    synced_rows: None,
                    join_plans: None,
                }),
                ast: Some(zero_cache_protocol::ast::Ast::table("project")),
                name: None,
                args: None,
            },
        ));

        assert_eq!(outcome.responses.len(), 1);
        let response = &outcome.responses[0];
        assert!(
            response.contains("\"op\":\"analyze-query\""),
            "got {response}"
        );
        assert!(response.contains("\"readRowCount\":1"), "got {response}");
        assert!(
            response.contains("\"from introspected schema\""),
            "got {response}"
        );
    }

    #[test]
    fn inspect_analyze_query_uses_registered_custom_query_transform() {
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            seeded_db(),
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        );
        handler.add_custom_query_transform(
            "issueByTitle",
            &[JsonValue::String("wired end to end".to_string())],
            zero_cache_protocol::ast::Ast {
                table: "issue".to_string(),
                where_: Some(zero_cache_protocol::ast::Condition::Simple {
                    op: zero_cache_protocol::ast::SimpleOperator::Eq,
                    left: zero_cache_protocol::ast::ValuePosition::Column(
                        zero_cache_protocol::ast::ColumnReference {
                            name: "title".to_string(),
                        },
                    ),
                    right: zero_cache_protocol::ast::ValuePosition::Literal(
                        zero_cache_protocol::ast::LiteralValue::String(
                            "wired end to end".to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            },
        );

        let outcome = handler.on_action(ConnectionAction::Inspect(
            zero_cache_protocol::inspect_up::InspectUpBody::AnalyzeQuery {
                id: "inspect-custom".to_string(),
                value: None,
                options: Some(zero_cache_protocol::inspect_up::AnalyzeQueryOptions {
                    vended_rows: Some(true),
                    synced_rows: None,
                    join_plans: None,
                }),
                ast: None,
                name: Some("issueByTitle".to_string()),
                args: Some(vec![JsonValue::String("wired end to end".to_string())]),
            },
        ));

        assert_eq!(outcome.responses.len(), 1);
        let response = &outcome.responses[0];
        assert!(
            response.contains("\"op\":\"analyze-query\""),
            "got {response}"
        );
        assert!(response.contains("\"readRowCount\":1"), "got {response}");
        assert!(response.contains("\"wired end to end\""), "got {response}");
    }

    #[tokio::test]
    async fn async_inspect_analyze_query_uses_http_custom_query_transform() {
        let args = vec![JsonValue::String("wired end to end".to_string())];
        let query_id =
            zero_cache_protocol::query_hash::hash_of_name_and_args("issueByTitle", &args);
        let url = spawn_transform_response_server(format!(
            r#"{{
                "kind":"QueryResponse",
                "queries":[{{
                    "id":"{query_id}",
                    "name":"issueByTitle",
                    "ast":{{
                        "table":"issue",
                        "where":{{
                            "type":"simple","op":"=",
                            "left":{{"type":"column","name":"title"}},
                            "right":{{"type":"literal","value":"wired end to end"}}
                        }}
                    }}
                }}]
            }}"#
        ))
        .await;
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            seeded_db(),
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        )
        .with_custom_query_transform_http(CustomQueryTransformHttpConfig::new(
            url, "public", "app1",
        ));

        let outcome = handler
            .on_action_async(ConnectionAction::Inspect(
                zero_cache_protocol::inspect_up::InspectUpBody::AnalyzeQuery {
                    id: "inspect-http-custom".to_string(),
                    value: None,
                    options: Some(zero_cache_protocol::inspect_up::AnalyzeQueryOptions {
                        vended_rows: Some(true),
                        synced_rows: None,
                        join_plans: None,
                    }),
                    ast: None,
                    name: Some("issueByTitle".to_string()),
                    args: Some(args),
                },
            ))
            .await;

        assert_eq!(outcome.responses.len(), 1);
        let response = &outcome.responses[0];
        assert!(
            response.contains("\"op\":\"analyze-query\""),
            "got {response}"
        );
        assert!(response.contains("\"readRowCount\":1"), "got {response}");
        assert!(response.contains("\"wired end to end\""), "got {response}");
    }

    /// Proves the AST-to-SQL generalization: a client that sends a real AST
    /// (`ast.where_`, not just a bare `name`+`args` custom query) gets a poke
    /// containing ONLY the rows matching that condition — filtered by real
    /// SQL (`SqliteTableSource::fetch_filtered`), not by a hardcoded catalog
    /// entry. Two rows exist upstream; the query's `where title = 'match me'`
    /// must exclude the other.
    #[tokio::test]
    async fn ast_where_condition_is_pushed_into_real_sql_and_filters_the_poke() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |_id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (1, 'match me')", &[])
                        .unwrap();
                    db.run(
                        "INSERT INTO issue (id, title) VALUES (2, 'not this one')",
                        &[],
                    )
                    .unwrap();
                    let mut handler = DesiredQueriesHandler::new(db, "group1", "c1");
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // A real AST with a real where_ condition, exactly what
        // up_json::upstream_from_json decodes off the wire from a real client.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-all","ast":{"table":"issue","where":{
                        "type":"simple","op":"=",
                        "left":{"type":"column","name":"title"},
                        "right":{"type":"literal","value":"match me"}
                    }}}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("match me"), "matching row present: {part}");
        assert!(
            !part.contains("not this one"),
            "non-matching row excluded by real SQL: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    #[test]
    fn top_level_where_exists_hydrates_the_matching_child_rows() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        db.exec("CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)")
            .unwrap();
        db.exec("INSERT INTO issue (id, title) VALUES (1, 'matched parent'), (2, 'other parent')")
            .unwrap();
        db.exec(
            "INSERT INTO comments (id, issueID, body) VALUES \
             (10, 1, 'exists witness'), \
             (11, 1, 'filtered child'), \
             (12, 2, 'other parent child')",
        )
        .unwrap();

        let ast_json = zero_cache_shared::bigint_json::parse(
            r#"{
                "table":"issue",
                "where":{"type":"correlatedSubquery","op":"EXISTS","related":{
                    "correlation":{"parentField":["id"],"childField":["issueID"]},
                    "subquery":{"table":"comments","where":{
                        "type":"simple","op":"=",
                        "left":{"type":"column","name":"body"},
                        "right":{"type":"literal","value":"exists witness"}
                    }}
                }}
            }"#,
        )
        .unwrap();
        let ast = zero_cache_protocol::ast_json::ast_from_json(&ast_json).unwrap();
        let mut handler = DesiredQueriesHandler::new(db, "group1", "client1");
        let outcome = handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                desired_queries_patch: vec![UpQueriesPatchOp::Put(UpQueriesPutOp {
                    hash: "issue-where-exists".to_string(),
                    ttl: None,
                    ast: Some(ast),
                    name: None,
                    args: None,
                })],
                ..Default::default()
            },
        )));

        assert_eq!(outcome.responses.len(), 3);
        let part = &outcome.responses[1];
        assert!(part.contains("matched parent"), "got {part}");
        assert!(part.contains("exists witness"), "got {part}");
        assert!(!part.contains("other parent"), "got {part}");
        assert!(!part.contains("filtered child"), "got {part}");
    }

    #[test]
    fn related_where_exists_hydrates_nested_witness_rows() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        db.exec("CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)")
            .unwrap();
        db.exec("CREATE TABLE reactions (id INTEGER PRIMARY KEY, commentID INTEGER, emoji TEXT)")
            .unwrap();
        db.exec("INSERT INTO issue (id, title) VALUES (1, 'nested parent')")
            .unwrap();
        db.exec(
            "INSERT INTO comments (id, issueID, body) VALUES \
             (10, 1, 'comment with reaction'), \
             (11, 1, 'comment without reaction')",
        )
        .unwrap();
        db.exec(
            "INSERT INTO reactions (id, commentID, emoji) VALUES \
             (100, 10, 'nested witness'), \
             (101, 10, 'filtered reaction')",
        )
        .unwrap();

        let ast_json = zero_cache_shared::bigint_json::parse(
            r#"{
                "table":"issue",
                "related":[{
                    "correlation":{"parentField":["id"],"childField":["issueID"]},
                    "subquery":{"table":"comments","where":{
                        "type":"correlatedSubquery","op":"EXISTS","related":{
                            "correlation":{"parentField":["id"],"childField":["commentID"]},
                            "subquery":{"table":"reactions","where":{
                                "type":"simple","op":"=",
                                "left":{"type":"column","name":"emoji"},
                                "right":{"type":"literal","value":"nested witness"}
                            }}
                        }
                    }}
                }]
            }"#,
        )
        .unwrap();
        let ast = zero_cache_protocol::ast_json::ast_from_json(&ast_json).unwrap();
        let mut handler = DesiredQueriesHandler::new(db, "group1", "client1");
        let outcome = handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                desired_queries_patch: vec![UpQueriesPatchOp::Put(UpQueriesPutOp {
                    hash: "related-where-exists".to_string(),
                    ttl: None,
                    ast: Some(ast),
                    name: None,
                    args: None,
                })],
                ..Default::default()
            },
        )));

        assert_eq!(outcome.responses.len(), 3);
        let part = &outcome.responses[1];
        assert!(part.contains("nested parent"), "got {part}");
        assert!(part.contains("comment with reaction"), "got {part}");
        assert!(part.contains("nested witness"), "got {part}");
        assert!(!part.contains("comment without reaction"), "got {part}");
        assert!(!part.contains("filtered reaction"), "got {part}");
    }

    /// A real AST carrying `orderBy` + `limit`: the top-N rows under the
    /// ordering are the only ones synced. Because `limit` decides *which* rows
    /// are in the top-N (not just their display order), this proves the SQL
    /// `ORDER BY` and the top-N truncation are both live end to end — a
    /// different `orderBy` would sync a different set of rows.
    #[tokio::test]
    async fn ast_order_by_and_limit_hydrate_only_the_top_n_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |_id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (1, 'apple')", &[])
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (2, 'mango')", &[])
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (3, 'zebra')", &[])
                        .unwrap();
                    let mut handler = DesiredQueriesHandler::new(db, "group1", "c1");
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // orderBy title DESC, limit 2 -> only 'zebra' and 'mango' are in the
        // top-2; 'apple' (lowest under DESC) must be excluded.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-all","ast":{"table":"issue",
                        "orderBy":[["title","desc"]],"limit":2}}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("zebra"), "top row present: {part}");
        assert!(part.contains("mango"), "second row present: {part}");
        assert!(
            !part.contains("apple"),
            "row beyond the limit under the ordering must be excluded: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    /// A real AST carrying a `start` cursor bound: the root read resumes
    /// strictly after the boundary row (`exclusive: true`) under the ordering,
    /// so the boundary row and everything before it are excluded from the poke.
    /// Proves the cursor is pushed into SQL end to end, not applied in memory.
    #[tokio::test]
    async fn ast_start_cursor_resumes_the_root_read_after_the_boundary_row() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (1, 'first')", &[])
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (2, 'second')", &[])
                        .unwrap();
                    db.run("INSERT INTO issue (id, title) VALUES (3, 'third')", &[])
                        .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // orderBy id ASC, start strictly after {id:1} -> only ids 2 and 3.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-all","ast":{"table":"issue",
                        "orderBy":[["id","asc"]],
                        "start":{"row":{"id":1},"exclusive":true}}}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(
            part.contains("second"),
            "row after the cursor present: {part}"
        );
        assert!(
            part.contains("third"),
            "row after the cursor present: {part}"
        );
        assert!(
            !part.contains("first"),
            "boundary row excluded by exclusive start cursor: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn desired_query_hydration_uses_ast_table_outside_demo_catalog() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE project (id INTEGER PRIMARY KEY, name TEXT)")
                        .unwrap();
                    db.run(
                        "INSERT INTO project (id, name) VALUES (1, 'hydrated by ast')",
                        &[],
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"project-all","ast":{"table":"project"}}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(
            part.contains("hydrated by ast"),
            "AST-only query should hydrate from an introspected table: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    #[test]
    fn desired_query_hydration_uses_registered_custom_query_transform() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE project (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.exec(
            "INSERT INTO project (id, name) VALUES \
             (1, 'custom transform match'), \
             (2, 'custom transform skip')",
        )
        .unwrap();
        let args = vec![JsonValue::String("custom transform match".to_string())];
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            db,
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        );
        handler.add_custom_query_transform(
            "projectByName",
            &args,
            zero_cache_protocol::ast::Ast {
                table: "project".to_string(),
                where_: Some(zero_cache_protocol::ast::Condition::Simple {
                    op: zero_cache_protocol::ast::SimpleOperator::Eq,
                    left: zero_cache_protocol::ast::ValuePosition::Column(
                        zero_cache_protocol::ast::ColumnReference {
                            name: "name".to_string(),
                        },
                    ),
                    right: zero_cache_protocol::ast::ValuePosition::Literal(
                        zero_cache_protocol::ast::LiteralValue::String(
                            "custom transform match".to_string(),
                        ),
                    ),
                }),
                ..Default::default()
            },
        );

        let outcome = handler.on_action(ConnectionAction::Initialize(Box::new(
            zero_cache_protocol::connect::InitConnectionBody {
                desired_queries_patch: vec![UpQueriesPatchOp::Put(
                    zero_cache_protocol::queries_patch::UpQueriesPutOp {
                        hash: "custom-project".to_string(),
                        ttl: None,
                        ast: None,
                        name: Some("projectByName".to_string()),
                        args: Some(args),
                    },
                )],
                ..Default::default()
            },
        )));

        assert_eq!(outcome.responses.len(), 3);
        let part = &outcome.responses[1];
        assert!(part.contains("custom transform match"), "got {part}");
        assert!(!part.contains("custom transform skip"), "got {part}");
    }

    #[tokio::test]
    async fn async_desired_query_hydration_fetches_custom_query_transform() {
        let args = vec![JsonValue::String("async transform match".to_string())];
        let query_id =
            zero_cache_protocol::query_hash::hash_of_name_and_args("projectByName", &args);
        let url = spawn_transform_response_server(format!(
            r#"{{
                "kind":"QueryResponse",
                "queries":[{{
                    "id":"{query_id}",
                    "name":"projectByName",
                    "ast":{{
                        "table":"project",
                        "where":{{
                            "type":"simple","op":"=",
                            "left":{{"type":"column","name":"name"}},
                            "right":{{"type":"literal","value":"async transform match"}}
                        }}
                    }}
                }}]
            }}"#
        ))
        .await;

        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE project (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.exec(
            "INSERT INTO project (id, name) VALUES \
             (1, 'async transform match'), \
             (2, 'async transform skip')",
        )
        .unwrap();
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            db,
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        )
        .with_custom_query_transform_http(CustomQueryTransformHttpConfig::new(
            url, "public", "app1",
        ));

        let outcome = handler
            .on_action_async(ConnectionAction::Initialize(Box::new(
                zero_cache_protocol::connect::InitConnectionBody {
                    desired_queries_patch: vec![UpQueriesPatchOp::Put(
                        zero_cache_protocol::queries_patch::UpQueriesPutOp {
                            hash: query_id,
                            ttl: None,
                            ast: None,
                            name: Some("projectByName".to_string()),
                            args: Some(args),
                        },
                    )],
                    ..Default::default()
                },
            )))
            .await;

        assert_eq!(outcome.responses.len(), 3);
        let hydration = handler.take_pending_hydration();
        assert_eq!(hydration.responses.len(), 3);
        let part = &hydration.responses[1];
        assert!(part.contains("async transform match"), "got {part}");
        assert!(!part.contains("async transform skip"), "got {part}");
    }

    #[tokio::test]
    async fn desired_query_hydration_fetches_top_level_related_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec(
                        "CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)",
                    )
                    .unwrap();
                    db.exec("INSERT INTO issue (id, title) VALUES (1, 'parent issue')")
                        .unwrap();
                    db.exec(
                        "INSERT INTO comments (id, issueID, body) VALUES \
                         (10, 1, 'related child'), \
                         (11, 99, 'unrelated child')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-with-comments","ast":{
                        "table":"issue",
                        "related":[{
                            "correlation":{"parentField":["id"],"childField":["issueID"]},
                            "subquery":{"table":"comments"}
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("parent issue"), "got {part}");
        assert!(part.contains("related child"), "got {part}");
        assert!(!part.contains("unrelated child"), "got {part}");
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    /// A related subquery's `limit` is applied PER PARENT, not globally: each
    /// of the two parent issues keeps its own top-2 comments under the child
    /// ordering, so 4 comments survive out of 6 — proving the per-parent fetch
    /// path, since a global truncate would have kept only 2 comments total.
    #[tokio::test]
    async fn desired_query_hydration_applies_related_limit_per_parent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec(
                        "CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)",
                    )
                    .unwrap();
                    db.exec(
                        "INSERT INTO issue (id, title) VALUES (1, 'issue one'), (2, 'issue two')",
                    )
                    .unwrap();
                    // Three comments per issue; ids ascending so the kept top-2
                    // per parent are the two lowest-id comments of each issue.
                    db.exec(
                        "INSERT INTO comments (id, issueID, body) VALUES \
                         (10, 1, 'one-keep-a'), (11, 1, 'one-keep-b'), (12, 1, 'one-DROP'), \
                         (20, 2, 'two-keep-a'), (21, 2, 'two-keep-b'), (22, 2, 'two-DROP')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-with-comments","ast":{
                        "table":"issue",
                        "related":[{
                            "correlation":{"parentField":["id"],"childField":["issueID"]},
                            "subquery":{"table":"comments","orderBy":[["id","asc"]],"limit":2}
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        // Both parents keep their own top-2.
        assert!(
            part.contains("one-keep-a") && part.contains("one-keep-b"),
            "issue 1 top-2: {part}"
        );
        assert!(
            part.contains("two-keep-a") && part.contains("two-keep-b"),
            "issue 2 top-2: {part}"
        );
        // The 3rd comment of each parent is dropped by the per-parent limit.
        assert!(
            !part.contains("one-DROP"),
            "issue 1 over-limit comment dropped: {part}"
        );
        assert!(
            !part.contains("two-DROP"),
            "issue 2 over-limit comment dropped: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    /// A related subquery's `start` cursor is now honored (previously root-only,
    /// ignored for related): the child read resumes strictly after the boundary
    /// row under the child ordering, so the boundary comment is excluded while
    /// later ones survive. Per-parent batching itself is covered by the related
    /// `limit` test; this proves the cursor reaches the related fetch at all.
    #[tokio::test]
    async fn desired_query_hydration_applies_related_start_cursor() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec(
                        "CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)",
                    )
                    .unwrap();
                    db.exec("INSERT INTO issue (id, title) VALUES (1, 'the issue')")
                        .unwrap();
                    db.exec(
                        "INSERT INTO comments (id, issueID, body) VALUES \
                         (10, 1, 'comment-boundary'), (11, 1, 'comment-after-a'), \
                         (12, 1, 'comment-after-b')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // related comments orderBy id ASC, start strictly after {id:10}.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-with-comments","ast":{
                        "table":"issue",
                        "related":[{
                            "correlation":{"parentField":["id"],"childField":["issueID"]},
                            "subquery":{"table":"comments","orderBy":[["id","asc"]],
                                "start":{"row":{"id":10},"exclusive":true}}
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(
            part.contains("comment-after-a"),
            "row after cursor present: {part}"
        );
        assert!(
            part.contains("comment-after-b"),
            "row after cursor present: {part}"
        );
        assert!(
            !part.contains("comment-boundary"),
            "boundary comment excluded by exclusive related start: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    /// A many-to-many (junction) relationship: `issue -> issueLabel -> label`,
    /// where the `issueLabel` hop is marked `hidden`. zero-cache syncs junction
    /// rows to the client like any related rows — `hidden` is a CLIENT-side
    /// view-shaping concern (`zql/ivm/view-apply-change.ts`), not a server
    /// hydration one (the server `pipeline-driver` has no `hidden` handling).
    /// So the poke must carry the target label rows, reached by the recursive
    /// related path traversing the nested hop off the junction rows.
    #[tokio::test]
    async fn desired_query_hydration_traverses_a_hidden_junction_many_to_many() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec(
                        "CREATE TABLE issueLabel (id INTEGER PRIMARY KEY, issueID INTEGER, labelID INTEGER)",
                    )
                    .unwrap();
                    db.exec("CREATE TABLE label (id INTEGER PRIMARY KEY, name TEXT)")
                        .unwrap();
                    db.exec("INSERT INTO issue (id, title) VALUES (1, 'the issue')")
                        .unwrap();
                    db.exec(
                        "INSERT INTO issueLabel (id, issueID, labelID) VALUES \
                         (100, 1, 5), (101, 1, 6), (102, 99, 7)",
                    )
                    .unwrap();
                    db.exec(
                        "INSERT INTO label (id, name) VALUES \
                         (5, 'label-bug'), (6, 'label-urgent'), (7, 'label-unrelated')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // issue -> issueLabel (hidden junction) -> label
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-with-labels","ast":{
                        "table":"issue",
                        "related":[{
                            "hidden":true,
                            "correlation":{"parentField":["id"],"childField":["issueID"]},
                            "subquery":{"table":"issueLabel","related":[{
                                "correlation":{"parentField":["labelID"],"childField":["id"]},
                                "subquery":{"table":"label"}
                            }]}
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        // Target labels reached through the junction are poked.
        assert!(
            part.contains("label-bug"),
            "target label via junction: {part}"
        );
        assert!(
            part.contains("label-urgent"),
            "target label via junction: {part}"
        );
        // A label whose junction row belongs to a different issue is excluded.
        assert!(
            !part.contains("label-unrelated"),
            "label for another issue's junction excluded: {part}"
        );
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn desired_query_hydration_fetches_compound_related_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec(
                        "CREATE TABLE locale_issue (\
                         id INTEGER PRIMARY KEY, \
                         tenantID INTEGER, \
                         issueID INTEGER, \
                         title TEXT)",
                    )
                    .unwrap();
                    db.exec(
                        "CREATE TABLE locale_comment (\
                         id INTEGER PRIMARY KEY, \
                         tenantID INTEGER, \
                         issueID INTEGER, \
                         body TEXT)",
                    )
                    .unwrap();
                    db.exec(
                        "INSERT INTO locale_issue (id, tenantID, issueID, title) VALUES \
                         (1, 1, 10, 'tenant one issue ten'), \
                         (2, 2, 20, 'tenant two issue twenty')",
                    )
                    .unwrap();
                    db.exec(
                        "INSERT INTO locale_comment (id, tenantID, issueID, body) VALUES \
                         (10, 1, 10, 'compound child one'), \
                         (20, 2, 20, 'compound child two'), \
                         (30, 1, 20, 'cross-product child'), \
                         (40, 2, 10, 'other cross-product child')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"locale-issue-with-comments","ast":{
                        "table":"locale_issue",
                        "related":[{
                            "correlation":{
                                "parentField":["tenantID","issueID"],
                                "childField":["tenantID","issueID"]
                            },
                            "subquery":{"table":"locale_comment"}
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("tenant one issue ten"), "got {part}");
        assert!(part.contains("tenant two issue twenty"), "got {part}");
        assert!(part.contains("compound child one"), "got {part}");
        assert!(part.contains("compound child two"), "got {part}");
        assert!(!part.contains("cross-product child"), "got {part}");
        assert!(!part.contains("other cross-product child"), "got {part}");
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn desired_query_hydration_fetches_nested_related_rows() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                |id| {
                    let db = StatementRunner::open_in_memory().unwrap();
                    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
                        .unwrap();
                    db.exec(
                        "CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)",
                    )
                    .unwrap();
                    db.exec(
                        "CREATE TABLE reactions (id INTEGER PRIMARY KEY, commentID INTEGER, emoji TEXT)",
                    )
                    .unwrap();
                    db.exec("INSERT INTO issue (id, title) VALUES (1, 'nested parent')")
                        .unwrap();
                    db.exec(
                        "INSERT INTO comments (id, issueID, body) VALUES \
                         (10, 1, 'nested child'), \
                         (11, 99, 'unrelated child')",
                    )
                    .unwrap();
                    db.exec(
                        "INSERT INTO reactions (id, commentID, emoji) VALUES \
                         (100, 10, 'nested reaction'), \
                         (101, 11, 'reaction on unrelated child'), \
                         (102, 999, 'unrelated reaction')",
                    )
                    .unwrap();
                    let mut handler =
                        DesiredQueriesHandler::new(db, "group1", &format!("client{id}"));
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[
                    {"op":"put","hash":"issue-with-nested-related","ast":{
                        "table":"issue",
                        "related":[{
                            "correlation":{"parentField":["id"],"childField":["issueID"]},
                            "subquery":{
                                "table":"comments",
                                "related":[{
                                    "correlation":{"parentField":["id"],"childField":["commentID"]},
                                    "subquery":{"table":"reactions"}
                                }]
                            }
                        }]
                    }}
                ]}]"#,
            ))
            .await
            .unwrap();

        let _start = client.next().await.unwrap().unwrap().into_text().unwrap();
        let part = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(part.contains("nested parent"), "got {part}");
        assert!(part.contains("nested child"), "got {part}");
        assert!(part.contains("nested reaction"), "got {part}");
        assert!(!part.contains("unrelated child"), "got {part}");
        assert!(!part.contains("reaction on unrelated child"), "got {part}");
        assert!(!part.contains("unrelated reaction"), "got {part}");
        let _end = client.next().await.unwrap().unwrap().into_text().unwrap();

        server.await.unwrap();
    }

    /// Proves the write path end to end: a real client connects, sends a real
    /// Live: the production write path. A handler configured with
    /// `with_upstream_push` routes a `push`'s CRUD insert to UPSTREAM Postgres
    /// via `apply_crud_mutation`; the row genuinely lands upstream (read back
    /// independently) and the `lastMutationID` advances. Skips without a test
    /// Postgres.
    #[tokio::test]
    async fn upstream_push_applies_mutation_to_postgres() {
        let base = std::env::var("ZERO_TEST_PG")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into());
        let Ok(pg) = zero_cache_change_source::pg_connection::connect(&base).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP SCHEMA IF EXISTS pushup CASCADE; CREATE SCHEMA pushup; \
             CREATE TABLE pushup.clients (\"clientGroupID\" TEXT, \"clientID\" TEXT, \
               \"lastMutationID\" BIGINT, PRIMARY KEY(\"clientGroupID\",\"clientID\")); \
             CREATE TABLE pushup.issue (id TEXT PRIMARY KEY, title TEXT);",
        )
        .await
        .unwrap();

        // Connect with search_path set so unqualified op tables resolve to pushup.
        let conn = format!("{base} options='-c search_path=pushup'");
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "c1")
            .with_upstream_push(conn, "pushup".to_string());

        let outcome = handler
            .on_action_async(ConnectionAction::Push(PushBody {
                client_group_id: "group1".to_string(),
                mutations: vec![Mutation::Crud(zero_cache_protocol::push::CrudMutation {
                    id: 1.0,
                    client_id: "c1".to_string(),
                    timestamp: 1.0,
                    ops_json: zero_cache_shared::bigint_json::parse(
                        r#"[{"op":"insert","tableName":"issue","primaryKey":["id"],"value":{"id":"a","title":"from client push"}}]"#,
                    )
                    .unwrap(),
                })],
                push_version: 1.0,
                schema_version: None,
                timestamp: 1.0,
                request_id: "req1".to_string(),
                traceparent: None,
            }))
            .await;
        assert_eq!(outcome.responses.len(), 1);
        assert!(
            outcome.responses[0].contains("pushResponse"),
            "got {}",
            outcome.responses[0]
        );

        // The row is genuinely upstream, and lastMutationID advanced.
        let row = pg
            .query_one("SELECT title FROM pushup.issue WHERE id = 'a'", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "from client push");
        let lmid = pg
            .query_one(
                "SELECT \"lastMutationID\" FROM pushup.clients WHERE \"clientID\" = 'c1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(lmid.get::<_, i64>(0), 1);

        pg.batch_execute("DROP SCHEMA pushup CASCADE;").await.ok();
    }

    /// `push` message carrying a CRUD insert, and gets back a real
    /// `pushResponse` frame over the real socket — and the row genuinely
    /// landed in the replica (read back via a fresh query, not just trusting
    /// the response).
    #[tokio::test]
    async fn push_message_applies_a_real_crud_mutation_and_responds() {
        // A temp-file-backed (not `:memory:`) replica, so a second connection
        // opened after the push can independently read back what was
        // committed — proving the row landed, not just that the response
        // claimed success.
        let db_path =
            std::env::temp_dir().join(format!("zero_push_test_{}.sqlite3", std::process::id()));
        let _ = std::fs::remove_file(&db_path);
        {
            let setup = StatementRunner::new(rusqlite::Connection::open(&db_path).unwrap());
            setup
                .exec("CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT)")
                .unwrap();
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let db_path_for_handler = db_path.clone();
        let server = tokio::spawn(async move {
            crate::sync_server::run_accept_loop_bounded(
                listener,
                move |_id| {
                    let db = StatementRunner::new(
                        rusqlite::Connection::open(&db_path_for_handler).unwrap(),
                    );
                    let mut handler = DesiredQueriesHandler::new(db, "group1", "c1");
                    move |action: ConnectionAction| handler.on_action(action)
                },
                Some(1),
            )
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();

        // A push (like any other data message) requires the connection to
        // already be initialized. An empty desired-queries patch produces no
        // response frames, so no extra read is needed here.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();

        // A real push, exactly the wire shape `up_json`/`push_json` decode.
        client
            .send(Message::text(
                r#"["push", {
                    "clientGroupID": "group1", "pushVersion": 1, "timestamp": 1, "requestID": "req1",
                    "mutations": [
                        {"type": "crud", "id": 1, "clientID": "c1", "timestamp": 1,
                         "args": [{"ops": [
                            {"op": "insert", "tableName": "issue", "primaryKey": ["id"],
                             "value": {"id": "1", "title": "from a real push"}}
                         ]}]}
                    ]
                }]"#,
            ))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(response.contains("pushResponse"), "got {response}");
        assert!(response.contains("\"clientID\":\"c1\""), "got {response}");
        assert!(
            !response.contains("\"error\""),
            "no error in a clean insert: {response}"
        );

        server.await.unwrap();

        // Read back via a FRESH connection to the same file — proving the row
        // was actually committed to the replica, not just that the response
        // claimed success.
        let verify = StatementRunner::new(rusqlite::Connection::open(&db_path).unwrap());
        let rows = verify
            .query_uncached("SELECT title FROM issue WHERE id = '1'", &[])
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "the row committed by the push is visible to a new connection"
        );
        assert_eq!(
            rows[0][0].1,
            zero_cache_sqlite::Value::Text("from a real push".into())
        );
        let _ = std::fs::remove_file(&db_path);
    }

    #[test]
    fn ack_mutation_responses_clears_pending_response_state() {
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "c1");
        let mutation_id = zero_cache_protocol::mutation_id::MutationId {
            id: 1.0,
            client_id: "c1".to_string(),
        };

        let push_outcome = handler.on_action(ConnectionAction::Push(PushBody {
            client_group_id: "group1".to_string(),
            mutations: vec![Mutation::Crud(zero_cache_protocol::push::CrudMutation {
                id: mutation_id.id,
                client_id: mutation_id.client_id.clone(),
                timestamp: 1.0,
                ops_json: zero_cache_shared::bigint_json::parse(
                    r#"[{"op":"insert","tableName":"issue","primaryKey":["id"],"value":{"id":2,"title":"pending ack"}}]"#,
                )
                .unwrap(),
            })],
            push_version: 1.0,
            schema_version: None,
            timestamp: 1.0,
            request_id: "req1".to_string(),
            traceparent: None,
        }));

        assert_eq!(push_outcome.responses.len(), 1);
        assert_eq!(handler.pending_mutation_response_count(), 1);

        let ack_outcome = handler.on_action(ConnectionAction::AckMutationResponses(
            zero_cache_protocol::push::AckMutationResponsesBody {
                mutation_id: mutation_id.clone(),
            },
        ));

        assert!(ack_outcome.responses.is_empty());
        assert_eq!(handler.pending_mutation_response_count(), 0);
    }

    #[test]
    fn pull_returns_current_cookie_and_last_mutation_id_changes() {
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "c1");

        let push_outcome = handler.on_action(ConnectionAction::Push(PushBody {
            client_group_id: "group1".to_string(),
            mutations: vec![Mutation::Crud(zero_cache_protocol::push::CrudMutation {
                id: 1.0,
                client_id: "c1".to_string(),
                timestamp: 1.0,
                ops_json: zero_cache_shared::bigint_json::parse(
                    r#"[{"op":"insert","tableName":"issue","primaryKey":["id"],"value":{"id":2,"title":"pull-visible lmid"}}]"#,
                )
                .unwrap(),
            })],
            push_version: 1.0,
            schema_version: None,
            timestamp: 1.0,
            request_id: "push-req1".to_string(),
            traceparent: None,
        }));
        assert_eq!(push_outcome.responses.len(), 1);

        let pull_outcome = handler.on_action(ConnectionAction::Pull(
            zero_cache_protocol::pull::PullRequestBody {
                client_group_id: "group1".to_string(),
                cookie: None,
                request_id: "pull-req1".to_string(),
            },
        ));

        assert_eq!(pull_outcome.responses.len(), 1);
        assert_eq!(
            pull_outcome.responses[0],
            r#"["pull",{"cookie":"00","requestID":"pull-req1","lastMutationIDChanges":{"c1":1}}]"#
        );
    }

    #[tokio::test]
    async fn durable_cvr_handler_flushes_before_returning_a_poke() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".into()
        });
        let Ok(cvr_client) = zero_cache_change_source::pg_connection::connect(&conn_str).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        let shard = zero_cache_types::shards::ShardId {
            app_id: "livehandlercvr".into(),
            shard_num: 0,
        };
        cvr_client
            .batch_execute("DROP SCHEMA IF EXISTS \"livehandlercvr_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for statement in
            zero_cache_view_syncer::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap()
        {
            cvr_client.batch_execute(&statement).await.unwrap();
        }

        let mut handler = DesiredQueriesHandler::new(seeded_db(), "cg-live", "client-live")
            .with_cvr_persistence(CvrPersistence::new(
                CvrPool::new(conn_str.clone(), 2),
                shard.clone(),
                "task-live",
                1_000.0,
            ));
        let outcome = handler
            .on_action_async(ConnectionAction::Initialize(Box::new(
                zero_cache_protocol::connect::InitConnectionBody {
                    desired_queries_patch: vec![UpQueriesPatchOp::Put(UpQueriesPutOp {
                        hash: "issue-live".into(),
                        ttl: None,
                        ast: Some(zero_cache_protocol::ast::Ast::table("issue")),
                        name: None,
                        args: None,
                    })],
                    ..Default::default()
                },
            )))
            .await;
        assert!(outcome.keep_open);
        assert_eq!(outcome.responses.len(), 3);

        let verify = zero_cache_change_source::pg_connection::connect(&conn_str)
            .await
            .unwrap();
        let zero_cache_view_syncer::cvr_store_pg::LoadCvrOutcome::Loaded(loaded) =
            zero_cache_view_syncer::cvr_store_pg::load_cvr(
                &verify,
                &shard,
                "cg-live",
                "task-live",
                2_000.0,
            )
            .await
            .unwrap()
        else {
            panic!("durable CVR should be caught up before the poke is returned")
        };
        assert_eq!(
            loaded.clients["client-live"].desired_query_ids,
            vec!["issue-live"]
        );
        assert!(loaded.queries.contains_key("issue-live"));
        verify
            .batch_execute("DROP SCHEMA \"livehandlercvr_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    #[test]
    fn update_auth_stores_latest_auth_token() {
        let mut handler = DesiredQueriesHandler::new(seeded_db(), "group1", "client1");

        let outcome = handler.on_action(ConnectionAction::UpdateAuth(
            zero_cache_protocol::update_auth::UpdateAuthBody {
                auth: "new-token".to_string(),
            },
        ));

        assert!(outcome.responses.is_empty());
        assert_eq!(handler.auth_raw(), Some("new-token"));
    }

    #[tokio::test]
    async fn async_http_custom_query_transform_uses_updated_auth() {
        let args = vec![JsonValue::String("wired end to end".to_string())];
        let query_id =
            zero_cache_protocol::query_hash::hash_of_name_and_args("issueByTitle", &args);
        let (url, request_rx) = spawn_transform_response_server_capturing_request(format!(
            r#"{{
                "kind":"QueryResponse",
                "queries":[{{
                    "id":"{query_id}",
                    "name":"issueByTitle",
                    "ast":{{"table":"issue"}}
                }}]
            }}"#
        ))
        .await;
        let mut handler = DesiredQueriesHandler::with_inspect_options(
            seeded_db(),
            "group1",
            "client1",
            51,
            "inspect-live-test".to_string(),
            true,
            None,
        )
        .with_custom_query_transform_http(CustomQueryTransformHttpConfig::new(
            url, "public", "app1",
        ));

        handler.on_action(ConnectionAction::UpdateAuth(
            zero_cache_protocol::update_auth::UpdateAuthBody {
                auth: "fresh-token".to_string(),
            },
        ));
        let outcome = handler
            .on_action_async(ConnectionAction::Inspect(
                zero_cache_protocol::inspect_up::InspectUpBody::AnalyzeQuery {
                    id: "inspect-http-auth".to_string(),
                    value: None,
                    options: None,
                    ast: None,
                    name: Some("issueByTitle".to_string()),
                    args: Some(args),
                },
            ))
            .await;

        assert_eq!(outcome.responses.len(), 1);
        let request = request_rx.await.unwrap();
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer fresh-token"),
            "request should carry refreshed auth header: {request}"
        );
    }

    /// A retried push with a stale mutation id is silently ignored, matching
    /// the upstream Mutagen path (which returns no second mutation result).
    #[tokio::test]
    async fn push_replay_is_ignored_while_a_new_mutation_in_the_same_batch_advances() {
        // Keep the regression deterministic at the handler boundary: the
        // transport has separate framing tests, while this case is about the
        // mutation-id state machine and response filtering.
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT)")
            .unwrap();
        let mut direct = DesiredQueriesHandler::new(db, "group1", "c1");
        let ops = |id: &str, title: &str| {
            zero_cache_shared::bigint_json::parse(&format!(
                r#"[{{"op":"insert","tableName":"issue","primaryKey":["id"],"value":{{"id":"{id}","title":"{title}"}}}}]"#
            ))
            .unwrap()
        };
        let mutation = |id: f64, ops_json: JsonValue| {
            Mutation::Crud(zero_cache_protocol::push::CrudMutation {
                id,
                client_id: "c1".into(),
                ops_json,
                timestamp: 1.0,
            })
        };
        let push = |mutations| PushBody {
            client_group_id: "group1".into(),
            mutations,
            push_version: 1.0,
            schema_version: None,
            timestamp: 1.0,
            request_id: "replay-test".into(),
            traceparent: None,
        };
        let first = direct.on_action(ConnectionAction::Push(push(vec![mutation(
            1.0,
            ops("1", "first"),
        )])));
        assert_eq!(first.responses.len(), 1);
        let mixed = direct.on_action(ConnectionAction::Push(push(vec![
            mutation(1.0, ops("1", "first")),
            mutation(2.0, ops("2", "second")),
        ])));
        assert_eq!(mixed.responses.len(), 1);
        assert!(mixed.responses[0].contains("\"id\":2"));
        assert!(!mixed.responses[0].contains("\"id\":1"));
    }

    #[test]
    fn compiled_permissions_filter_raw_ast_desired_queries_with_auth_data() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id TEXT PRIMARY KEY, owner TEXT, title TEXT)")
            .unwrap();
        db.exec(
            "INSERT INTO issue (id, owner, title) VALUES \
             ('1', 'alice', 'alice-only'), ('2', 'bob', 'bob-secret')",
        )
        .unwrap();
        let permissions = zero_cache_auth::compiled_permissions::parse_compiled_permissions_json(
            r#"{"permissions":{"tables":{"issue":{"row":{"select":[["allow",{
              "type":"simple","op":"=",
              "left":{"type":"column","name":"owner"},
              "right":{"type":"static","anchor":"authData","field":"sub"}
            }]]}}}}}"#,
        )
        .unwrap();
        let auth_data = zero_cache_shared::bigint_json::parse(r#"{"sub":"alice"}"#).unwrap();
        let mut handler = DesiredQueriesHandler::new(db, "cg-permissions", "c-permissions")
            .with_permissions(permissions)
            .with_auth_data(auth_data);

        // This is deliberately a raw AST rather than an inspect request or a
        // transformed custom query: it used to bypass read authorization.
        let outcome = handler.on_action(ConnectionAction::UpdateDesiredQueries(
            zero_cache_protocol::change_desired_queries::ChangeDesiredQueriesBody {
                desired_queries_patch: vec![UpQueriesPatchOp::Put(UpQueriesPutOp {
                    hash: "raw-issue-query".into(),
                    ttl: None,
                    ast: Some(zero_cache_protocol::ast::Ast::table("issue")),
                    name: None,
                    args: None,
                })],
                traceparent: None,
            },
        ));

        assert_eq!(outcome.responses.len(), 3);
        let part = &outcome.responses[1];
        assert!(part.contains("alice-only"), "allowed row missing: {part}");
        assert!(
            !part.contains("bob-secret"),
            "raw desired-query AST leaked a row disallowed by compiled permissions: {part}"
        );
    }

    #[test]
    fn compiled_permissions_block_unauthorized_and_forged_primary_key_crud_writes() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id TEXT PRIMARY KEY, owner TEXT, title TEXT)")
            .unwrap();
        db.exec("INSERT INTO issue (id, owner, title) VALUES ('1', 'alice', 'original')")
            .unwrap();
        let permissions = zero_cache_auth::compiled_permissions::parse_compiled_permissions_json(
            r#"{"permissions":{"tables":{"issue":{"row":{"update":{"preMutation":[["allow",{"type":"simple","op":"=","left":{"type":"column","name":"owner"},"right":{"type":"static","anchor":"authData","field":"sub"}}]],"postMutation":[["allow",{"type":"simple","op":"=","left":{"type":"column","name":"owner"},"right":{"type":"static","anchor":"authData","field":"sub"}}]]}}}}}}"#,
        )
        .unwrap();
        let mut handler = DesiredQueriesHandler::new(db, "cg-write-permissions", "c1")
            .with_permissions(permissions)
            .with_auth_data(zero_cache_shared::bigint_json::parse(r#"{"sub":"bob"}"#).unwrap());

        let push = |id, primary_key: &str, title: &str| {
            PushBody {
            client_group_id: "cg-write-permissions".into(),
            mutations: vec![Mutation::Crud(zero_cache_protocol::push::CrudMutation {
                id,
                client_id: "c1".into(),
                ops_json: zero_cache_shared::bigint_json::parse(&format!(
                    r#"[{{"op":"update","tableName":"issue","primaryKey":["{primary_key}"],"value":{{"id":"1","owner":"alice","title":"{title}"}}}}]"#,
                ))
                .unwrap(),
                timestamp: id,
            })],
            push_version: 1.0,
            schema_version: None,
            timestamp: id,
            request_id: format!("request-{id}"),
            traceparent: None,
        }
        };

        // Bob cannot edit Alice's row. The mutation is acknowledged but the
        // database row is untouched, exactly like upstream's authorizer path.
        let denied = handler.on_action(ConnectionAction::Push(push(1.0, "id", "blocked")));
        assert!(denied.responses[0].contains("pushResponse"));
        let title = handler
            .core
            .db
            .query_uncached("SELECT title FROM issue WHERE id = '1'", &[])
            .unwrap()[0][0]
            .1
            .clone();
        assert_eq!(title, zero_cache_sqlite::Value::Text("original".into()));

        // A client must also not be able to forge a different primary-key
        // declaration to steer authorization/SQL at a row it does not own.
        let forged = handler.on_action(ConnectionAction::Push(push(2.0, "owner", "forged")));
        assert!(forged.responses[0].contains("pushResponse"));
        let title = handler
            .core
            .db
            .query_uncached("SELECT title FROM issue WHERE id = '1'", &[])
            .unwrap()[0][0]
            .1
            .clone();
        assert_eq!(title, zero_cache_sqlite::Value::Text("original".into()));
    }
}
