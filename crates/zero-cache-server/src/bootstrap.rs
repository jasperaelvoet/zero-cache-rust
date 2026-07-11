//! The outer process shell: bind a listener, own the shared [`SyncService`],
//! and run the WebSocket accept loop until shutdown. This is the wiring a
//! `main` performs — it turns the tested orchestration pieces into a running
//! server process.
//!
//! `run_server` owns the accept side: it accepts TCP connections, upgrades each
//! to a WebSocket, sends the `connected` greeting, and serves the per-connection
//! message loop ([`crate::serve_connection::serve_connection_async`]) with a
//! handler the caller supplies per connection. It runs until the `shutdown`
//! signal fires, then stops accepting (in-flight connections are spawned tasks
//! and wind down on their own). The replicator half (the supervised apply loop
//! that calls [`SyncService::publish_commit`]) is spawned separately by `main`
//! and shares the same [`SyncService`] handle.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::oneshot;

use std::future::Future;
use std::pin::Pin;

use crate::cvr_pool::CvrPool;
use crate::live_connection::{AuthVerifier, CvrPersistence, DesiredQueriesHandler};
use crate::serve_connection::serve_connection_async;
use crate::serve_connection::HandlerOutcome;
use crate::sync_service::SyncService;
use crate::ws_connection::{init_connection_from_payload, WsConnection};
use zero_cache_sqlite::StatementRunner;
use zero_cache_view_syncer::connection_dispatch::ConnectionAction;

/// A per-connection message handler as the accept loop expects it: an
/// `FnMut(ConnectionAction) -> Future<HandlerOutcome>` that is `Send + 'static`
/// so it can run in the connection's spawned task.
pub type BoxedHandler =
    Box<dyn FnMut(ConnectionAction) -> Pin<Box<dyn Future<Output = HandlerOutcome> + Send>> + Send>;

/// Builds the LIVE per-connection handler backed by a real
/// [`DesiredQueriesHandler`] (the CVR/query-tracking view-syncer machinery),
/// replacing the keepalive-only stand-in. Each connection gets its own replica
/// [`StatementRunner`] and its own handler state; the handler is shared into the
/// per-action async closure via an `Arc<tokio::sync::Mutex<_>>` so it can be
/// mutated across awaits inside the connection's task.
///
/// `client_group_id`/`client_id` identify the connection's CVR; a real
/// deployment derives them from the `initConnection` URL params, this derives
/// them from the connection id so each socket gets a distinct group.
pub fn live_handler(connection_id: u64, db: StatementRunner) -> BoxedHandler {
    live_handler_with_permissions(connection_id, db, None)
}

/// Like [`live_handler`], but applies an already parsed compiled permissions
/// document.  Standalone mode has no verified JWT handshake context, so static
/// auth values resolve to `null`; synced mode uses the richer path below.
pub fn live_handler_with_permissions(
    connection_id: u64,
    db: StatementRunner,
    permissions: Option<zero_cache_auth::policy::PermissionsConfig>,
) -> BoxedHandler {
    let client_group_id = format!("cg{connection_id}");
    let client_id = format!("c{connection_id}");
    let mut inner = DesiredQueriesHandler::new(db, &client_group_id, &client_id);
    if let Some(permissions) = permissions {
        inner = inner.with_permissions(permissions);
    }
    let handler = std::sync::Arc::new(tokio::sync::Mutex::new(inner));
    Box::new(move |action| {
        let handler = handler.clone();
        Box::pin(async move { handler.lock().await.on_action_async(action).await })
    })
}

/// Static server configuration a `main` resolves from env/flags.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// `host:port` to bind the sync WebSocket listener on.
    pub listen_addr: String,
    /// Per-connection fan-out buffer depth (commits) before a slow connection
    /// must re-catch-up from the change-log.
    pub fanout_capacity: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            listen_addr: "0.0.0.0:4848".to_string(),
            fanout_capacity: 1024,
        }
    }
}

/// Binds the sync listener. Separated from [`run_server`] so a caller (or test)
/// can learn the bound address (useful with an ephemeral `:0` port).
pub async fn bind(config: &ServerConfig) -> std::io::Result<TcpListener> {
    TcpListener::bind(&config.listen_addr).await
}

/// Runs the accept loop over `listener`, serving each connection with the
/// handler `make_handler(connection_id)` produces, until `shutdown` fires.
/// Returns the number of connections accepted.
///
/// `service` is the shared hub the replicator publishes commits to and each
/// connection subscribes to; it is held here so the accept side and the
/// replicator side share one fan-out. (This signature keeps the per-connection
/// handler caller-supplied so `main` wires the live view-syncer/CVR machinery
/// while tests supply a lightweight stand-in.)
pub async fn run_server<F, H, Fut>(
    listener: TcpListener,
    service: Arc<SyncService>,
    shutdown: oneshot::Receiver<()>,
    mut make_handler: F,
) -> u64
where
    F: FnMut(u64) -> H,
    H: FnMut(ConnectionAction) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = HandlerOutcome> + Send + 'static,
{
    // Instrument accepted connections under zero.server.connections, sharing
    // the service's metrics registry (routes to the same OTel backend as the
    // replicator's commit counter).
    let connections = service.metrics().get_or_create_counter(
        zero_cache_services::metrics::Category::Server,
        "connections",
    );
    // Keep the hub alive for the lifetime of the accept loop; connections clone
    // subscriptions off it via `service.subscribe()` inside their handlers.
    let _service = service;
    let mut next_id: u64 = 0;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return next_id,
            accepted = listener.accept() => {
                let Ok((tcp, _peer)) = accepted else { return next_id };
                let id = next_id;
                next_id += 1;
                connections.add(1.0);
                let handler = make_handler(id);
                tokio::spawn(async move {
                    let Ok(mut conn) = WsConnection::accept(tcp).await else { return };
                    if conn.send_connected(&format!("ws{id}"), 0.0).await.is_err() {
                        return;
                    }
                    let _ = serve_connection_async(&mut conn, handler).await;
                });
            }
        }
    }
}

/// Production public listener: the upstream-compatible HTTP surface and strict
/// sync URL validation share the same port as WebSocket connections.
pub async fn run_public_server<F, H, Fut>(
    listener: TcpListener,
    service: Arc<SyncService>,
    shutdown: oneshot::Receiver<()>,
    public: crate::public_http::PublicEndpointConfig,
    mut make_handler: F,
) -> u64
where
    F: FnMut(u64) -> H,
    H: FnMut(ConnectionAction) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = HandlerOutcome> + Send + 'static,
{
    let connections = service.metrics().get_or_create_counter(
        zero_cache_services::metrics::Category::Server,
        "connections",
    );
    let _service = service;
    let mut next_id = 0;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return next_id,
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)), if public.keepalive_timeout_ms.is_some() => {
                if public.keepalive_expired() {
                    crate::info!("keepalive timeout elapsed; draining public listener");
                    return next_id;
                }
            }
            accepted = listener.accept() => {
                let Ok((tcp, _peer)) = accepted else { return next_id };
                let public = public.clone();
                let id = next_id;
                let handler = make_handler(id);
                let connections = connections.clone();
                tokio::spawn(async move {
                    let tcp = match crate::public_http::dispatch(tcp, &public, None).await {
                        crate::public_http::PublicDisposition::Upgrade(tcp) => tcp,
                        crate::public_http::PublicDisposition::Handled => return,
                    };
                    connections.add(1.0);
                    let Ok(mut conn) = WsConnection::accept(tcp).await else { return };
                    if conn.send_connected(&format!("ws{id}"), 0.0).await.is_err() {
                        return;
                    }
                    let _ = serve_connection_async(&mut conn, handler).await;
                });
                next_id += 1;
            }
        }
    }
}

/// Synced-mode accept loop: each connection is served from a read-only view of
/// the shared replica at `replica_path` AND subscribed to the fan-out, so it
/// receives live pokes on upstream commits (via
/// [`crate::serve_connection::serve_synced_connection`]). Runs until `shutdown`.
/// Per-connection handler dependencies: how pushes and custom queries are
/// handled. All optional — absent means that path is disabled.
#[derive(Clone, Default)]
pub struct HandlerDeps {
    /// Public-port HTTP routes and strict upstream WebSocket routing. Tests and
    /// embedders may omit this to retain the low-level WebSocket-only harness.
    pub public_endpoint: Option<crate::public_http::PublicEndpointConfig>,
    /// Durable CVR connection settings. When present, each accepted client
    /// group loads its CVR from this Postgres database before serving init.
    pub cvr: Option<CvrRuntimeConfig>,
    /// Parsed compiled permissions from `ZERO_SCHEMA_JSON`. Shared across
    /// connections and cloned into each connection handler.
    pub permissions: Option<Arc<zero_cache_auth::policy::PermissionsConfig>>,
    /// `(upstream libpq conn string, mutation schema)` — route CRUD pushes to
    /// upstream Postgres.
    pub upstream_push: Option<(String, String)>,
    /// `(query_url, api_key, schema, app_id)` — custom synced-query API server.
    pub query_api: Option<(String, Option<String>, String, String)>,
    /// `(mutate_url, api_key, schema, app_id)` — custom-mutator API server.
    pub mutate_api: Option<(String, Option<String>, String, String)>,
    /// Forward the client's `Cookie` header to the query / mutate API servers
    /// (`ZERO_QUERY_FORWARD_COOKIES` / `ZERO_MUTATE_FORWARD_COOKIES`).
    pub query_forward_cookies: bool,
    pub mutate_forward_cookies: bool,
    /// Client request-header names forwarded to each API server
    /// (`ZERO_QUERY_ALLOWED_CLIENT_HEADERS` / `ZERO_MUTATE_ALLOWED_CLIENT_HEADERS`,
    /// lowercased).
    pub query_allowed_client_headers: Vec<String>,
    pub mutate_allowed_client_headers: Vec<String>,
}

#[derive(Clone)]
pub struct CvrRuntimeConfig {
    pub connection_string: String,
    pub max_connections: usize,
    pub shard: zero_cache_types::shards::ShardId,
    pub task_id: String,
}

pub async fn run_synced_server(
    listener: TcpListener,
    service: Arc<SyncService>,
    shutdown: oneshot::Receiver<()>,
    replica_path: String,
    deps: HandlerDeps,
) -> u64 {
    use crate::live_connection::DesiredQueriesHandler;
    use crate::serve_connection::serve_synced_connection;
    use zero_cache_sqlite::StatementRunner;

    let connections = service.metrics().get_or_create_counter(
        zero_cache_services::metrics::Category::Server,
        "connections",
    );
    // Optional auth: when a key source is configured (ZERO_AUTH_JWK,
    // ZERO_AUTH_SECRET, or ZERO_AUTH_JWKS_URL — resolved in that upstream
    // priority order), every connection must present a JWT (in its
    // Sec-WebSocket-Protocol payload) that verifies against it.
    let token_verifier: Arc<crate::auth_token::TokenVerifier> = Arc::new(
        crate::auth_token::TokenVerifier::from_config(
            std::env::var("ZERO_AUTH_SECRET").ok().as_deref(),
            std::env::var("ZERO_AUTH_JWK").ok().as_deref(),
            std::env::var("ZERO_AUTH_JWKS_URL").ok().as_deref(),
        )
        .unwrap_or_else(|error| {
            crate::warn!("invalid auth key configuration ({error}); auth DISABLED");
            crate::auth_token::TokenVerifier::Disabled
        }),
    );
    let auth_issuer: Option<Arc<String>> = std::env::var("ZERO_AUTH_ISSUER")
        .ok()
        .filter(|s| !s.is_empty())
        .map(Arc::new);
    // Official zero-cache owns one bounded CVR pool per sync worker and shares
    // it across client groups. This process has one sync service, so it owns one
    // equivalent shared pool rather than one PostgreSQL client per WebSocket.
    let cvr_pool = deps
        .cvr
        .as_ref()
        .map(|config| CvrPool::new(&config.connection_string, config.max_connections));
    let cvr_transition_locks = Arc::new(std::sync::Mutex::new(std::collections::HashMap::<
        String,
        std::sync::Weak<tokio::sync::Mutex<()>>,
    >::new()));
    // Read once at startup: default OFF keeps CVR persistence a single
    // synchronous config+rows transaction. When ON, the row-record flush is
    // deferred off the hydration critical path behind a process-local barrier.
    // Upstream defers the CVR row-record flush BY DEFAULT (optimistic poke:
    // the config/version commits synchronously, the row records flush off the
    // hydration critical path). Match that default; the env var remains only as
    // a temporary escape hatch to force the old synchronous path while the
    // multi-node deferred-flush correctness is validated under soak, and will be
    // deleted once that lands (the plan removes Rust-only knobs).
    let defer_cvr_rows = std::env::var("ZERO_DEFER_CVR_ROWS")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE" | "no"))
        .unwrap_or(true);
    let cvr_row_flush_barriers = crate::cvr_row_flush_barrier::RowFlushBarriers::new();
    // Process-global bound on how many deferred row flushes run their critical
    // section (pool connection + 1000-row write) at once, so background flushes
    // cannot starve the synchronous config flushes on the hydration critical
    // path. Read once at startup; small default leaves most of the CVR pool free
    // for config flushes. Only consulted on the `ZERO_DEFER_CVR_ROWS` path.
    let defer_flush_concurrency = std::env::var("ZERO_CVR_DEFER_FLUSH_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4);
    let cvr_defer_flush_limiter =
        crate::cvr_row_flush_barrier::DeferFlushLimiter::new(defer_flush_concurrency);
    let auth_audience: Option<Arc<String>> = std::env::var("ZERO_AUTH_AUDIENCE")
        .ok()
        .filter(|s| !s.is_empty())
        .map(Arc::new);
    let unauthorized = service.metrics().get_or_create_counter(
        zero_cache_services::metrics::Category::Server,
        "unauthorized",
    );
    let mut next_id: u64 = 0;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return next_id,
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)), if deps.public_endpoint.as_ref().is_some_and(|public| public.keepalive_timeout_ms.is_some()) => {
                if deps.public_endpoint.as_ref().is_some_and(crate::public_http::PublicEndpointConfig::keepalive_expired) {
                    crate::info!("keepalive timeout elapsed; draining public listener");
                    return next_id;
                }
            }
            accepted = listener.accept() => {
                let Ok((tcp, _peer)) = accepted else { return next_id };
                let id = next_id;
                next_id += 1;
                connections.add(1.0);
                let subscriber = service.subscribe();
                let replica_path = replica_path.clone();
                let deps = deps.clone();
                let cvr_pool = cvr_pool.clone();
                let cvr_transition_locks = cvr_transition_locks.clone();
                let cvr_row_flush_barriers = cvr_row_flush_barriers.clone();
                let cvr_defer_flush_limiter = cvr_defer_flush_limiter.clone();
                let permissions = deps.permissions.clone();
                let token_verifier = token_verifier.clone();
                let auth_issuer = auth_issuer.clone();
                let auth_audience = auth_audience.clone();
                let unauthorized = unauthorized.clone();
                tokio::spawn(async move {
                    let tcp = if let Some(public) = deps.public_endpoint.as_ref() {
                        match crate::public_http::dispatch(tcp, public, Some(&replica_path)).await {
                            crate::public_http::PublicDisposition::Upgrade(tcp) => tcp,
                            crate::public_http::PublicDisposition::Handled => return,
                        }
                    } else {
                        tcp
                    };
                    let Ok(mut conn) = WsConnection::accept(tcp).await else { return };

                    // Auth gate: reject unauthenticated connections before serving.
                    // Upstream pins the token's `sub` to the connecting `userID`
                    // query param, so a valid token minted for a different user
                    // cannot be replayed on another user's connection.
                    let connect_user_id = conn
                        .request_uri
                        .as_deref()
                        .and_then(|uri| crate::ws_connection::query_param(uri, "userID"))
                        .filter(|value| !value.is_empty());
                    if !token_verifier
                        .authorize(
                            conn.sec_protocol_payload.as_deref(),
                            crate::auth_token::now_unix(),
                            auth_issuer.as_deref().map(|s| s.as_str()),
                            auth_audience.as_deref().map(|s| s.as_str()),
                            connect_user_id.as_deref(),
                        )
                        .await
                    {
                        unauthorized.add(1.0);
                        let _ = conn
                            .send_json(r#"["error",{"kind":"AuthInvalidated","message":"unauthenticated"}]"#)
                            .await;
                        return;
                    }

                    // Each connection gets its own read-only replica view.
                    let Ok(db) = StatementRunner::open_file_readonly(&replica_path) else {
                        return;
                    };
                    let (pipeline_specs, all_pipeline_tables) =
                        match zero_cache_sqlite::snapshotter::snapshot_table_specs(&db) {
                            Ok(specs) => specs,
                            Err(error) => {
                                crate::warn!("failed to load snapshot table specs: {error}");
                                return;
                            }
                        };
                    let pipeline_driver =
                        match zero_cache_view_syncer::pipeline_driver::PipelineDriver::new(
                            &replica_path,
                            std::env::var("ZERO_APP_ID").unwrap_or_else(|_| "zero".into()),
                            // Upstream never overrides the snapshot connection's
                            // SQLite page cache via env (the production Snapshotter
                            // is constructed without a pageCacheSizeKib); leave it
                            // at the engine default rather than exposing a
                            // Rust-only ZERO_REPLICA_PAGE_CACHE_SIZE_KIB knob.
                            None,
                            pipeline_specs,
                            all_pipeline_tables,
                        ) {
                            Ok(driver) => driver,
                            Err(error) => {
                                crate::warn!("failed to initialize persistent IVM: {error}");
                                return;
                            }
                        };
                    // v1.7 requires real client identity in the connect URL.
                    // Synthetic identities were a Rust-port fallback and would
                    // corrupt reconnect/CVR ownership semantics.
                    let uri = conn.request_uri.as_deref().unwrap_or("");
                    let Some(client_group_id) =
                        crate::ws_connection::query_param(uri, "clientGroupID")
                            .filter(|value| !value.is_empty())
                    else {
                        crate::warn!("rejecting sync connection without clientGroupID");
                        return;
                    };
                    let Some(client_id) = crate::ws_connection::query_param(uri, "clientID")
                        .filter(|value| !value.is_empty())
                    else {
                        crate::warn!("rejecting sync connection without clientID");
                        return;
                    };
                    let cvr_transition_lock = cvr_pool.as_ref().map(|_| {
                        let mut locks = cvr_transition_locks.lock().unwrap();
                        locks.retain(|_, lock| lock.strong_count() > 0);
                        if let Some(lock) = locks.get(&client_group_id).and_then(|lock| lock.upgrade())
                        {
                            lock
                        } else {
                            let lock = Arc::new(tokio::sync::Mutex::new(()));
                            locks.insert(client_group_id.clone(), Arc::downgrade(&lock));
                            lock
                        }
                    });
                    // One shared row-flush barrier per client group, used only
                    // when deferring rows. A reconnect awaits it before reading
                    // durable rows so the deferred flush is observed atomically.
                    let cvr_row_flush_barrier = (defer_cvr_rows && cvr_pool.is_some())
                        .then(|| cvr_row_flush_barriers.get_or_create(&client_group_id));
                    // Seed the connection's bearer token from the connect
                    // handshake so the FIRST forwarded mutation/query is
                    // authenticated (a mobile client authenticates with a token,
                    // not a cookie; without this the app's mutate server 401s).
                    let connect_auth = conn
                        .sec_protocol_payload
                        .as_deref()
                        .and_then(crate::ws_connection::auth_token_from_payload);
                    // Only a JWT verified by the configured key source becomes
                    // `authData` for compiled permissions. Opaque tokens are
                    // safe to forward to app endpoints, but never trusted for
                    // server-side row/cell authorization.
                    let auth_data = if token_verifier.is_enabled() {
                        match connect_auth.as_deref() {
                            Some(token) => token_verifier
                                .verify(
                                    token,
                                    crate::auth_token::now_unix(),
                                    auth_issuer.as_deref().map(|issuer| issuer.as_str()),
                                    auth_audience.as_deref().map(|audience| audience.as_str()),
                                    connect_user_id.as_deref(),
                                )
                                .await
                                .ok()
                                .map(|claims| claims.decoded)
                                .unwrap_or_else(|| {
                                    zero_cache_shared::bigint_json::JsonValue::Object(vec![])
                                }),
                            None => zero_cache_shared::bigint_json::JsonValue::Object(vec![]),
                        }
                    } else {
                        zero_cache_shared::bigint_json::JsonValue::Object(vec![])
                    };
                    let auth_verifier = token_verifier.is_enabled().then(|| {
                        AuthVerifier::new(
                            token_verifier.clone(),
                            auth_issuer.as_deref().map(|issuer| issuer.as_str().to_string()),
                            auth_audience
                                .as_deref()
                                .map(|audience| audience.as_str().to_string()),
                        )
                    });
                    // The client's persisted cookie — a RECONNECTING client sends
                    // it so its first poke bases at that cookie (not null).
                    let base_cookie = crate::ws_connection::query_param(uri, "baseCookie");
                    // The official JS client carries initConnection in the
                    // WebSocket subprotocol on both fresh connects and
                    // reconnects. Consume it regardless of baseCookie: if a
                    // fresh header init is ignored, the client never sends a
                    // duplicate text frame and every query remains loading.
                    let header_init = conn
                        .sec_protocol_payload
                        .as_deref()
                        .and_then(init_connection_from_payload);
                    crate::info!(
                        "client connected: clientGroupID={client_group_id} clientID={client_id} baseCookie={:?} auth={} cookie={}",
                        base_cookie.as_deref().filter(|s| !s.is_empty()),
                        connect_auth.is_some(),
                        conn.cookie.is_some(),
                    );
                    // A configured CVR is authoritative for reconnects. Load
                    // it before constructing the handler so an empty
                    // initConnection patch resumes the persisted desired set
                    // instead of creating a fresh, cookie-only CVR.
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|duration| duration.as_secs_f64() * 1000.0)
                        .unwrap_or(0.0);
                    let mut loaded_cvr = None;
                    let mut loaded_rows = None;
                    let mut cvr_persistence = None;
                    if let (Some(cvr_config), Some(shared_cvr_pool)) =
                        (deps.cvr.clone(), cvr_pool.as_ref())
                    {
                        // Await any pending deferred row flush for this group so
                        // the connect-time load never reads durable rows that a
                        // spawned flush has not committed yet (single-node
                        // invariant preservation).
                        if let Some(barrier) = cvr_row_flush_barrier.as_ref() {
                            barrier.wait_for_pending().await;
                        }
                        let cvr_client = match shared_cvr_pool.get().await {
                            Ok(client) => client,
                            Err(error) => {
                                crate::warn!(
                                    "CVR connection failed for clientGroupID={client_group_id}: {error}"
                                );
                                return;
                            }
                        };
                        let mut loaded = None;
                        for attempt in 0..10 {
                            match zero_cache_view_syncer::cvr_store_pg::load_cvr(
                                &cvr_client,
                                &cvr_config.shard,
                                &client_group_id,
                                &cvr_config.task_id,
                                now_ms,
                            ).await {
                                Ok(zero_cache_view_syncer::cvr_store_pg::LoadCvrOutcome::Loaded(cvr)) => {
                                    loaded = Some(cvr);
                                    break;
                                }
                                Ok(zero_cache_view_syncer::cvr_store_pg::LoadCvrOutcome::RowsBehind { version, rows_version }) => {
                                    crate::debug!("CVR rows are behind for clientGroupID={client_group_id}: cvr={version} rows={rows_version:?} (attempt {}/{})", attempt + 1, 10);
                                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                                }
                                Err(error) => {
                                    crate::warn!("CVR load failed for clientGroupID={client_group_id}: {error}");
                                    return;
                                }
                            }
                        }
                        let Some(loaded) = loaded else {
                            crate::warn!("CVR rows remained behind for clientGroupID={client_group_id} after retrying");
                            return;
                        };
                        let rows = match zero_cache_view_syncer::cvr_store_pg::get_row_records(
                            &cvr_client,
                            &cvr_config.shard,
                            &client_group_id,
                        )
                        .await
                        {
                            Ok(rows) => rows.into_values().collect(),
                            Err(error) => {
                                crate::warn!(
                                    "CVR row-cache load failed for clientGroupID={client_group_id}: {error}"
                                );
                                return;
                            }
                        };
                        loaded_cvr = Some(loaded);
                        loaded_rows = Some(rows);
                        let mut persistence = CvrPersistence::new(
                            shared_cvr_pool.clone(),
                            cvr_config.shard,
                            cvr_config.task_id,
                            now_ms,
                        );
                        // Only the deferral path spawns background row flushes,
                        // so only it consults the throttle. Attaching it here
                        // leaves the synchronous flush path byte-identical.
                        if cvr_row_flush_barrier.is_some() {
                            persistence = persistence
                                .with_defer_flush_limiter(cvr_defer_flush_limiter.clone());
                        }
                        cvr_persistence = Some(persistence);
                    }
                    let mut handler = DesiredQueriesHandler::new(db, &client_group_id, &client_id)
                        .with_pipeline_driver(pipeline_driver)
                        .with_auth(connect_auth)
                        .with_base_cookie(base_cookie);
                    if let Some(cvr) = loaded_cvr {
                        handler = handler.with_loaded_cvr(cvr);
                    }
                    if let Some(rows) = loaded_rows {
                        handler = handler.with_loaded_row_records(rows);
                    }
                    if let Some(persistence) = cvr_persistence {
                        handler = handler.with_cvr_persistence(persistence);
                    }
                    if let Some(lock) = cvr_transition_lock {
                        handler = handler.with_cvr_transition_lock(lock);
                    }
                    if let Some(barrier) = cvr_row_flush_barrier {
                        handler = handler
                            .with_defer_cvr_rows(true)
                            .with_cvr_row_flush_barrier(barrier);
                    }
                    if let Some(permissions) = permissions {
                        handler = handler
                            .with_permissions((*permissions).clone())
                            .with_auth_data(auth_data);
                    }
                    if let Some(verifier) = auth_verifier {
                        handler = handler.with_auth_verifier(verifier);
                    }
                    if let Some((conn_str, schema)) = deps.upstream_push {
                        handler = handler.with_upstream_push(conn_str, schema);
                    }
                    // Forward the whitelisted client request headers (by lowercased
                    // name) to an app API server — the `allowed-client-headers`
                    // contract. hunting-game whitelists `cookie` for session auth.
                    let client_headers = conn.request_headers.clone();
                    let client_cookie = conn.cookie.clone();
                    let filter_headers = |allowed: &[String]| -> Vec<(String, String)> {
                        client_headers
                            .iter()
                            .filter(|(k, _)| allowed.iter().any(|a| a == k))
                            .cloned()
                            .collect()
                    };
                    // Custom synced queries (ZERO_QUERY_URL): fetch query ASTs
                    // from the app's query API server.
                    if let Some((url, api_key, schema, app_id)) = deps.query_api {
                        let mut qcfg = crate::live_connection::CustomQueryTransformHttpConfig::new(
                            url, schema, app_id,
                        );
                        qcfg.api_key = api_key;
                        if deps.query_forward_cookies {
                            qcfg.cookie = client_cookie.clone();
                        }
                        qcfg.custom_headers = filter_headers(&deps.query_allowed_client_headers);
                        handler = handler.with_custom_query_transform_http(qcfg);
                    }
                    // Custom mutators (ZERO_MUTATE_URL): forward custom mutations
                    // to the app's mutate API server.
                    if let Some((url, api_key, schema, app_id)) = deps.mutate_api {
                        let cookie = if deps.mutate_forward_cookies {
                            client_cookie.clone()
                        } else {
                            None
                        };
                        let custom_headers = filter_headers(&deps.mutate_allowed_client_headers);
                        handler = handler.with_mutate_api_forwarding(
                            url,
                            api_key,
                            schema,
                            app_id,
                            cookie,
                            custom_headers,
                        );
                    }
                    if conn.send_connected(&format!("ws{id}"), 0.0).await.is_err() {
                        return;
                    }
                    let initial_state = if let Some(body) = header_init {
                        let outcome = handler
                            .on_action_async(ConnectionAction::Initialize(Box::new(body)))
                            .await;
                        for frame in outcome.responses {
                            if conn.send_json(&frame).await.is_err() {
                                return;
                            }
                        }
                        zero_cache_view_syncer::connection_dispatch::InitState::Initialized
                    } else {
                        zero_cache_view_syncer::connection_dispatch::InitState::AwaitingInit
                    };
                    let (sink, stream) = conn.into_split();
                    let _ = serve_synced_connection(sink, stream, handler, subscriber, initial_state).await;
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    /// The assembled server shell, exercised over a real socket: `run_server`
    /// binds, greets, and serves a real client through `initConnection` and
    /// `ping` (answered with `pong`), then a `shutdown` signal stops the accept
    /// loop and `run_server` returns the accepted-connection count.
    #[tokio::test]
    async fn run_server_serves_a_client_then_shuts_down() {
        let config = ServerConfig {
            listen_addr: "127.0.0.1:0".into(),
            fanout_capacity: 16,
        };
        let listener = bind(&config).await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Retain the metrics backend so the connection counter can be checked.
        let backend = std::sync::Arc::new(zero_cache_services::metrics::InMemoryBackend::new());
        let metrics =
            std::sync::Arc::new(zero_cache_services::metrics::Metrics::new(backend.clone()));
        let service = Arc::new(SyncService::with_metrics(config.fanout_capacity, metrics));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let server = tokio::spawn(async move {
            run_server(listener, service, shutdown_rx, |_id| {
                |_action: ConnectionAction| async { HandlerOutcome::empty() }
            })
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let greeting = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(greeting.starts_with("[\"connected\","));

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();
        client.send(Message::text(r#"["ping",{}]"#)).await.unwrap();
        let pong = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(pong, r#"["pong",{}]"#);

        // The accept loop instrumented the connection.
        assert_eq!(backend.counter_value("zero.server.connections"), 1.0);

        // Close the client, then signal shutdown; the accept loop returns.
        client.send(Message::Close(None)).await.unwrap();
        shutdown_tx.send(()).unwrap();
        let accepted = server.await.unwrap();
        assert_eq!(accepted, 1, "one connection was accepted before shutdown");
    }

    /// The assembled server run with the LIVE `DesiredQueriesHandler`-backed
    /// handler (not the keepalive stand-in): a real client initializes, changes
    /// its desired queries, and pings — the connection is served through the
    /// real view-syncer handler end-to-end and stays alive (pong returns).
    #[tokio::test]
    async fn run_server_with_live_handler_serves_a_real_client() {
        let config = ServerConfig {
            listen_addr: "127.0.0.1:0".into(),
            fanout_capacity: 16,
        };
        let listener = bind(&config).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = Arc::new(SyncService::new(config.fanout_capacity));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let server = tokio::spawn(async move {
            run_server(listener, service, shutdown_rx, |id| {
                let db = StatementRunner::open_in_memory().unwrap();
                live_handler(id, db)
            })
            .await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let greeting = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(greeting.starts_with("[\"connected\","));

        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();
        client
            .send(Message::text(
                r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"clear"}]}]"#,
            ))
            .await
            .unwrap();
        client.send(Message::text(r#"["ping",{}]"#)).await.unwrap();

        // The real handler processed init + change without tearing the
        // connection down; the inline pong confirms the loop is still live.
        let pong = loop {
            let msg = client.next().await.unwrap().unwrap().into_text().unwrap();
            if msg == r#"["pong",{}]"# {
                break msg;
            }
            // Any poke frames the handler emits for the change are fine; keep
            // reading until the pong.
        };
        assert_eq!(pong, r#"["pong",{}]"#);

        client.send(Message::Close(None)).await.unwrap();
        shutdown_tx.send(()).unwrap();
        assert_eq!(server.await.unwrap(), 1);
    }

    #[tokio::test]
    async fn run_server_shuts_down_cleanly_with_no_connections() {
        let config = ServerConfig {
            listen_addr: "127.0.0.1:0".into(),
            fanout_capacity: 8,
        };
        let listener = bind(&config).await.unwrap();
        let service = Arc::new(SyncService::new(config.fanout_capacity));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            run_server(listener, service, shutdown_rx, |_id| {
                |_action: ConnectionAction| async { HandlerOutcome::empty() }
            })
            .await
        });
        shutdown_tx.send(()).unwrap();
        assert_eq!(server.await.unwrap(), 0);
    }
}
