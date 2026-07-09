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

use crate::live_connection::DesiredQueriesHandler;
use crate::serve_connection::serve_connection_async;
use crate::serve_connection::HandlerOutcome;
use crate::sync_service::SyncService;
use crate::ws_connection::WsConnection;
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
    let client_group_id = format!("cg{connection_id}");
    let client_id = format!("c{connection_id}");
    let handler = std::sync::Arc::new(tokio::sync::Mutex::new(DesiredQueriesHandler::new(
        db,
        &client_group_id,
        &client_id,
    )));
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

/// Synced-mode accept loop: each connection is served from a read-only view of
/// the shared replica at `replica_path` AND subscribed to the fan-out, so it
/// receives live pokes on upstream commits (via
/// [`crate::serve_connection::serve_synced_connection`]). Runs until `shutdown`.
/// Per-connection handler dependencies: how pushes and custom queries are
/// handled. All optional — absent means that path is disabled.
#[derive(Clone, Default)]
pub struct HandlerDeps {
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

    let connections = service
        .metrics()
        .get_or_create_counter(zero_cache_services::metrics::Category::Server, "connections");
    // Optional admission control: bound concurrent live connections so a
    // stampede can't exhaust memory/FDs. `ZERO_MAX_CONNECTIONS` unset = unbounded.
    let max_connections: Option<usize> = std::env::var("ZERO_MAX_CONNECTIONS")
        .ok()
        .and_then(|s| s.parse().ok());
    let limiter = max_connections.map(|n| Arc::new(tokio::sync::Semaphore::new(n)));
    let rejected = service
        .metrics()
        .get_or_create_counter(zero_cache_services::metrics::Category::Server, "rejected");
    // Optional auth: when ZERO_AUTH_SECRET is set, every connection must present
    // a valid HS256 JWT (in its Sec-WebSocket-Protocol payload).
    let auth_secret: Option<Arc<Vec<u8>>> = std::env::var("ZERO_AUTH_SECRET")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| Arc::new(s.into_bytes()));
    let auth_issuer: Option<Arc<String>> = std::env::var("ZERO_AUTH_ISSUER")
        .ok()
        .filter(|s| !s.is_empty())
        .map(Arc::new);
    let auth_audience: Option<Arc<String>> = std::env::var("ZERO_AUTH_AUDIENCE")
        .ok()
        .filter(|s| !s.is_empty())
        .map(Arc::new);
    let unauthorized = service
        .metrics()
        .get_or_create_counter(zero_cache_services::metrics::Category::Server, "unauthorized");
    let mut next_id: u64 = 0;
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return next_id,
            accepted = listener.accept() => {
                let Ok((tcp, _peer)) = accepted else { return next_id };
                let id = next_id;
                next_id += 1;
                // Admission control: grab a permit (held for the connection's
                // lifetime); at capacity, drop the socket instead of spawning.
                let permit = match &limiter {
                    Some(sem) => match sem.clone().try_acquire_owned() {
                        Ok(p) => Some(p),
                        Err(_) => {
                            rejected.add(1.0);
                            drop(tcp); // over capacity — refuse
                            continue;
                        }
                    },
                    None => None,
                };
                connections.add(1.0);
                let subscriber = service.subscribe();
                let replica_path = replica_path.clone();
                let deps = deps.clone();
                let auth_secret = auth_secret.clone();
                let auth_issuer = auth_issuer.clone();
                let auth_audience = auth_audience.clone();
                let unauthorized = unauthorized.clone();
                tokio::spawn(async move {
                    let _permit = permit; // released when the connection ends
                    let Ok(mut conn) = WsConnection::accept(tcp).await else { return };

                    // Auth gate: reject unauthenticated connections before serving.
                    if !crate::auth_token::authorize_connection(
                        conn.sec_protocol_payload.as_deref(),
                        auth_secret.as_deref().map(|v| v.as_slice()),
                        crate::auth_token::now_unix(),
                        auth_issuer.as_deref().map(|s| s.as_str()),
                        auth_audience.as_deref().map(|s| s.as_str()),
                    ) {
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
                    // Honor the client's real identity from the connect URL
                    // (`/sync/vN/connect?clientGroupID=…&clientID=…`), as a real
                    // @rocicorp/zero client sends — so CVR state keys correctly.
                    // Fall back to synthetic ids for lenient/legacy clients.
                    let uri = conn.request_uri.as_deref().unwrap_or("");
                    let client_group_id = crate::ws_connection::query_param(uri, "clientGroupID")
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| format!("cg{id}"));
                    let client_id = crate::ws_connection::query_param(uri, "clientID")
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| format!("c{id}"));
                    // Seed the connection's bearer token from the connect
                    // handshake so the FIRST forwarded mutation/query is
                    // authenticated (a mobile client authenticates with a token,
                    // not a cookie; without this the app's mutate server 401s).
                    let connect_auth = conn
                        .sec_protocol_payload
                        .as_deref()
                        .and_then(crate::ws_connection::auth_token_from_payload);
                    // The client's persisted cookie — a RECONNECTING client sends
                    // it so its first poke bases at that cookie (not null).
                    let base_cookie = crate::ws_connection::query_param(uri, "baseCookie");
                    crate::info!(
                        "client connected: clientGroupID={client_group_id} clientID={client_id} baseCookie={:?} auth={} cookie={}",
                        base_cookie.as_deref().filter(|s| !s.is_empty()),
                        connect_auth.is_some(),
                        conn.cookie.is_some(),
                    );
                    let mut handler = DesiredQueriesHandler::new(db, &client_group_id, &client_id)
                        .with_auth(connect_auth)
                        .with_base_cookie(base_cookie);
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
                    let (sink, stream) = conn.into_split();
                    let _ = serve_synced_connection(sink, stream, handler, subscriber).await;
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
