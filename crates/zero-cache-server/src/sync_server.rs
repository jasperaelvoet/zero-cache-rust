//! The accept-and-dispatch loop — the worker layer that turns a listening
//! socket into a served sync endpoint by spawning one [`serve_connection`] task
//! per client.
//!
//! This is the top of the transport stack: `run_accept_loop` accepts TCP
//! connections, completes the WebSocket handshake ([`WsConnection::accept`]),
//! sends the `connected` greeting, and hands each connection to a freshly built
//! handler running in its own task — so many clients are served concurrently.
//! It is the Rust counterpart of `syncer.ts`'s per-connection worker spawn.
//!
//! The per-connection *handler* (the stateful CVR/view-syncer core) is supplied
//! by a factory `make_handler: FnMut(u64) -> H` called once per connection with
//! the connection's id; each handler owns its client's state. Handlers run in
//! spawned tasks, so `H` is `Send + 'static` — a real deployment's handler
//! holds its own view-syncer/CVR resources rather than sharing a `!Send`
//! `rusqlite` connection across tasks.

use std::future::Future;

use tokio::net::TcpListener;

use crate::serve_connection::{serve_connection, serve_connection_async, HandlerOutcome};
use crate::ws_connection::WsConnection;
use zero_cache_view_syncer::connection_dispatch::ConnectionAction;

/// Accepts connections forever, spawning a serve task per client. Each accepted
/// connection is assigned a monotonically increasing id (used as the `wsid` in
/// the `connected` greeting — a deterministic per-process counter rather than a
/// random/`Date.now()` id, matching this port's no-ambient-clock convention).
///
/// Returns only if `accept` errors (the listener closed); individual connection
/// failures are logged-and-dropped, never bringing down the loop. For bounded
/// runs (tests, graceful drain) use [`run_accept_loop_bounded`].
pub async fn run_accept_loop<F, H>(listener: TcpListener, make_handler: F)
where
    F: FnMut(u64) -> H,
    H: FnMut(ConnectionAction) -> HandlerOutcome + Send + 'static,
{
    run_accept_loop_bounded(listener, make_handler, None).await;
}

/// Like [`run_accept_loop`] but stops after accepting `max_connections`
/// connections when `Some` (each is still served to completion in its own
/// task). `None` runs forever. Returns the number of connections accepted.
pub async fn run_accept_loop_bounded<F, H>(
    listener: TcpListener,
    mut make_handler: F,
    max_connections: Option<u64>,
) -> u64
where
    F: FnMut(u64) -> H,
    H: FnMut(ConnectionAction) -> HandlerOutcome + Send + 'static,
{
    let mut next_id: u64 = 0;
    loop {
        if let Some(max) = max_connections {
            if next_id >= max {
                return next_id;
            }
        }
        let (tcp, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(_) => return next_id, // listener closed
        };
        let id = next_id;
        next_id += 1;
        let handler = make_handler(id);
        tokio::spawn(async move {
            let mut conn = match WsConnection::accept(tcp).await {
                Ok(c) => c,
                Err(_) => return, // handshake failed — drop this connection only
            };
            // Greet with the assigned wsid; timestamp is the caller's clock
            // (0.0 here — this layer has no ambient clock).
            if conn.send_connected(&format!("ws{id}"), 0.0).await.is_err() {
                return;
            }
            let _ = serve_connection(&mut conn, handler).await;
        });
    }
}

/// Async-handler variant of [`run_accept_loop`].
///
/// This is for per-connection handlers that need to await real I/O while
/// handling protocol actions, for example calling a user's HTTP query
/// transform endpoint during inspect `analyze-query`.
pub async fn run_accept_loop_async<F, H, Fut>(listener: TcpListener, make_handler: F)
where
    F: FnMut(u64) -> H,
    H: FnMut(ConnectionAction) -> Fut + Send + 'static,
    Fut: Future<Output = HandlerOutcome> + Send + 'static,
{
    run_accept_loop_async_bounded(listener, make_handler, None).await;
}

/// Like [`run_accept_loop_bounded`] but awaits each handler action.
pub async fn run_accept_loop_async_bounded<F, H, Fut>(
    listener: TcpListener,
    mut make_handler: F,
    max_connections: Option<u64>,
) -> u64
where
    F: FnMut(u64) -> H,
    H: FnMut(ConnectionAction) -> Fut + Send + 'static,
    Fut: Future<Output = HandlerOutcome> + Send + 'static,
{
    let mut next_id: u64 = 0;
    loop {
        if let Some(max) = max_connections {
            if next_id >= max {
                return next_id;
            }
        }
        let (tcp, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(_) => return next_id,
        };
        let id = next_id;
        next_id += 1;
        let handler = make_handler(id);
        tokio::spawn(async move {
            let mut conn = match WsConnection::accept(tcp).await {
                Ok(c) => c,
                Err(_) => return,
            };
            if conn.send_connected(&format!("ws{id}"), 0.0).await.is_err() {
                return;
            }
            let _ = serve_connection_async(&mut conn, handler).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::{Arc, Mutex};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    /// Two real clients connect concurrently to one accept loop; each is served
    /// in its own task, gets a distinct `wsid` greeting, and its ping is
    /// answered with a pong — proving the dispatch spawns independent
    /// per-client serve loops.
    #[tokio::test]
    async fn accepts_and_serves_multiple_clients_concurrently() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Record which connection ids the factory built handlers for.
        let ids = Arc::new(Mutex::new(Vec::<u64>::new()));
        let ids_srv = ids.clone();

        let server = tokio::spawn(async move {
            run_accept_loop_bounded(
                listener,
                move |id| {
                    ids_srv.lock().unwrap().push(id);
                    // A trivial handler: no downstream frames, stays open.
                    move |_action: ConnectionAction| HandlerOutcome::empty()
                },
                Some(2),
            )
            .await
        });

        async fn connect_ping_expect_pong(addr: std::net::SocketAddr) -> String {
            let request = format!("ws://{addr}/sync").into_client_request().unwrap();
            let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
            let greeting = client
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_text()
                .unwrap()
                .to_string();
            client.send(Message::text(r#"["ping",{}]"#)).await.unwrap();
            let pong = client.next().await.unwrap().unwrap().into_text().unwrap();
            assert_eq!(pong, r#"["pong",{}]"#);
            client.send(Message::Close(None)).await.unwrap();
            greeting
        }

        // Two concurrent clients.
        let (g1, g2) = tokio::join!(
            connect_ping_expect_pong(addr),
            connect_ping_expect_pong(addr),
        );
        assert!(g1.starts_with("[\"connected\","));
        assert!(g2.starts_with("[\"connected\","));

        let accepted = server.await.unwrap();
        assert_eq!(
            accepted, 2,
            "the bounded loop accepted exactly two connections"
        );

        // Both connections got distinct ids (ws0 / ws1 across the two greetings).
        let mut built = ids.lock().unwrap().clone();
        built.sort();
        assert_eq!(built, vec![0, 1]);
        let greetings = format!("{g1}{g2}");
        assert!(
            greetings.contains("ws0") && greetings.contains("ws1"),
            "distinct wsids: {greetings}"
        );
    }

    #[tokio::test]
    async fn async_accept_loop_awaits_handler_actions() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            run_accept_loop_async_bounded(
                listener,
                |_id| {
                    move |action: ConnectionAction| async move {
                        match action {
                            ConnectionAction::Inspect(_) => {
                                tokio::task::yield_now().await;
                                HandlerOutcome::send(vec![
                                    r#"["inspect",{"id":"async-loop","op":"version","value":"ok"}]"#
                                        .into(),
                                ])
                            }
                            _ => HandlerOutcome::empty(),
                        }
                    }
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
            .send(Message::text(r#"["inspect",{"op":"version","id":"v"}]"#))
            .await
            .unwrap();

        let response = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(
            response,
            r#"["inspect",{"id":"async-loop","op":"version","value":"ok"}]"#
        );
        client.send(Message::Close(None)).await.unwrap();

        assert_eq!(server.await.unwrap(), 1);
    }
}
