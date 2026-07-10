//! The live per-connection serve loop — the async glue that drives a real
//! WebSocket through the full sync-protocol pipeline:
//!
//!   recv text frame -> decode (`up_json::upstream_from_json`)
//!                    -> route  (`connection_dispatch::dispatch_upstream`)
//!                    -> act    (caller's handler: CVR / view-syncer)
//!                    -> encode & send downstream (pokes, pong, ...).
//!
//! This wires the three already-ported decision/protocol layers
//! ([`WsConnection`], `zero-cache-protocol::up_json`,
//! `zero-cache-view-syncer::connection_dispatch`) onto a real socket. `ping` is
//! answered with `pong` inline; every other routed [`ConnectionAction`] is
//! handed to a caller-supplied handler that performs the stateful work and
//! returns any downstream frames to send back (e.g. a poke sequence). The loop
//! enforces `initConnection`-first ordering via the router's [`InitState`] and
//! ends on clean close, `Close`, a protocol/decoder error, or when the handler
//! asks to stop.
//!
//! Scope: the handler itself (applying a desired-queries patch to the CVR,
//! running queries, producing pokes) is the caller's — the live CVR/view-syncer
//! wiring is separate. This module owns the socket I/O + protocol framing only.

use std::future::Future;

use zero_cache_protocol::up_json::upstream_from_json;
use zero_cache_view_syncer::connection_dispatch::{
    dispatch_upstream, ConnectionAction, DispatchError, InitState,
};

use crate::ws_connection::{WsConnection, WsConnectionError};

#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    #[error(transparent)]
    Ws(#[from] WsConnectionError),
    #[error("malformed upstream message: {0}")]
    Decode(String),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
}

/// What a connection handler decided after acting on a message.
pub struct HandlerOutcome {
    /// Downstream JSON frames to send back to the client, in order (e.g. a
    /// `pokeStart`/`pokePart`/`pokeEnd` sequence).
    pub responses: Vec<String>,
    /// Whether to keep the connection open.
    pub keep_open: bool,
}

impl HandlerOutcome {
    /// Continue with no downstream frames.
    pub fn empty() -> Self {
        HandlerOutcome {
            responses: Vec::new(),
            keep_open: true,
        }
    }
    /// Continue, sending `responses`.
    pub fn send(responses: Vec<String>) -> Self {
        HandlerOutcome {
            responses,
            keep_open: true,
        }
    }
}

/// Serves one accepted connection until it closes. Reads frames, decodes and
/// routes each, answers `ping` with `pong` inline, and hands other actions to
/// `handle` (which returns downstream frames + whether to stay open).
///
/// `handle` is `FnMut(ConnectionAction) -> HandlerOutcome`; it is NOT called
/// for `Pong` (handled inline) and receives `Close` right before the loop ends
/// so a caller can flush a goodbye. A decode/dispatch error terminates the
/// connection (returning the error) — matching upstream, which closes the
/// socket on a protocol violation.
pub async fn serve_connection<H>(conn: &mut WsConnection, mut handle: H) -> Result<(), ServeError>
where
    H: FnMut(ConnectionAction) -> HandlerOutcome,
{
    let mut state = InitState::AwaitingInit;
    while let Some(text) = conn.recv_text().await? {
        let msg = zero_cache_shared::bigint_json::parse(&text)
            .map_err(|e| ServeError::Decode(e.to_string()))
            .and_then(|json| {
                upstream_from_json(&json).map_err(|e| ServeError::Decode(e.to_string()))
            })?;

        let (action, next_state) = dispatch_upstream(msg, state)?;
        state = next_state;

        match action {
            ConnectionAction::Pong => {
                // Reply to keepalive inline.
                conn.send_json(r#"["pong",{}]"#).await?;
            }
            ConnectionAction::Close => {
                let outcome = handle(ConnectionAction::Close);
                for r in &outcome.responses {
                    conn.send_json(r).await?;
                }
                break;
            }
            other => {
                let outcome = handle(other);
                for r in &outcome.responses {
                    conn.send_json(r).await?;
                }
                if !outcome.keep_open {
                    break;
                }
            }
        }
    }
    Ok(())
}

/// Async-handler variant of [`serve_connection`].
///
/// This is the transport bridge needed by stateful handlers that must await
/// real I/O while handling an action, such as HTTP-backed custom-query
/// transforms for inspect `analyze-query`. The wire/dispatch behavior is the
/// same as [`serve_connection`]; only the action callback is awaited.
pub async fn serve_connection_async<H, Fut>(
    conn: &mut WsConnection,
    mut handle: H,
) -> Result<(), ServeError>
where
    H: FnMut(ConnectionAction) -> Fut,
    Fut: Future<Output = HandlerOutcome>,
{
    let mut state = InitState::AwaitingInit;
    while let Some(text) = conn.recv_text().await? {
        let msg = zero_cache_shared::bigint_json::parse(&text)
            .map_err(|e| ServeError::Decode(e.to_string()))
            .and_then(|json| {
                upstream_from_json(&json).map_err(|e| ServeError::Decode(e.to_string()))
            })?;

        let (action, next_state) = dispatch_upstream(msg, state)?;
        state = next_state;

        match action {
            ConnectionAction::Pong => {
                let outcome = handle(ConnectionAction::Pong).await;
                if let Some((first, rest)) = outcome.responses.split_first() {
                    conn.send_json(first).await?;
                    conn.send_json(r#"["pong",{}]"#).await?;
                    for r in rest {
                        conn.send_json(r).await?;
                    }
                } else {
                    conn.send_json(r#"["pong",{}]"#).await?;
                }
            }
            ConnectionAction::Close => {
                let outcome = handle(ConnectionAction::Close).await;
                for r in &outcome.responses {
                    conn.send_json(r).await?;
                }
                break;
            }
            other => {
                let outcome = handle(other).await;
                for r in &outcome.responses {
                    conn.send_json(r).await?;
                }
                if !outcome.keep_open {
                    break;
                }
            }
        }
    }
    Ok(())
}

/// Serves one connection in SYNCED mode: multiplexes incoming client frames
/// AND server-initiated pokes driven by the fan-out. On each upstream commit
/// (a `FanoutEvent::Commit`) the handler re-hydrates the connection's tracked
/// queries and any resulting poke is pushed to the client — this is what makes
/// the connection *live*.
///
/// `sink`/`stream` are the split halves of an already-greeted [`WsConnection`];
/// `handler` owns this connection's CVR/query state; `subscriber` is its
/// fan-out subscription. Returns when the client closes or the socket errors.
pub async fn serve_synced_connection(
    mut sink: crate::ws_connection::WsSink,
    mut stream: crate::ws_connection::WsStream,
    mut handler: crate::live_connection::DesiredQueriesHandler,
    mut subscriber: zero_cache_sqlite::change_fanout::FanoutSubscriber,
    initial_state: InitState,
) -> Result<(), ServeError> {
    use crate::ws_connection::{recv_text_from, send_text_to};
    use zero_cache_sqlite::change_fanout::FanoutEvent;

    async fn emit(
        sink: &mut crate::ws_connection::WsSink,
        frames: Vec<String>,
    ) -> Result<(), ServeError> {
        for f in frames {
            send_text_to(sink, &f)
                .await
                .map_err(|e| ServeError::Decode(e.to_string()))?;
        }
        Ok(())
    }

    let mut state = initial_state;
    loop {
        tokio::select! {
            // A client frame.
            frame = recv_text_from(&mut stream) => {
                let Some(text) = frame else { break };
                let msg = zero_cache_shared::bigint_json::parse(&text)
                    .map_err(|e| ServeError::Decode(e.to_string()))
                    .and_then(|json| {
                        upstream_from_json(&json).map_err(|e| ServeError::Decode(e.to_string()))
                    })?;
                let (action, next_state) = dispatch_upstream(msg, state)?;
                state = next_state;
                match action {
                    ConnectionAction::Pong => {
                        let _ = handler.on_action_async(ConnectionAction::Pong).await;
                        emit(&mut sink, vec![r#"["pong",{}]"#.to_string()]).await?;
                    }
                    ConnectionAction::Close => {
                        let outcome = handler.on_action_async(ConnectionAction::Close).await;
                        emit(&mut sink, outcome.responses).await?;
                        break;
                    }
                    other => {
                        let outcome = handler.on_action_async(other).await;
                        let keep = outcome.keep_open;
                        emit(&mut sink, outcome.responses).await?;
                        // Upstream pushes the hydration poke (rows + gotQueriesPatch)
                        // as soon as it is built, chained on the config poke's
                        // cookie — never gated on client input.
                        let staged = handler.take_pending_hydration();
                        emit(&mut sink, staged.responses).await?;
                        if !keep {
                            break;
                        }
                    }
                }
            }
            // An upstream commit fanned out to this connection -> live poke.
            event = subscriber.recv() => {
                match event {
                    FanoutEvent::Commit(_) | FanoutEvent::Lagged { .. } => {
                        // Coalesce a burst of commits into a single advance+poke.
                        // `advance()` always leapfrogs the pipeline to the replica's
                        // CURRENT head (it reads the whole change-log diff since the
                        // pipeline's last version), so draining the queued
                        // notifications and advancing ONCE catches up every pending
                        // commit — matching upstream's per-client poke coalescing.
                        // Processing each commit separately instead makes a lagging
                        // connection fall further behind under fan-out load (the
                        // per-connection collapse the bench showed). A `Lagged`
                        // notification means the broadcast dropped messages, but the
                        // change-log (not the broadcast) is the source of truth, so
                        // advancing to head still reconciles it.
                        while let Some(pending) = subscriber.try_recv() {
                            if matches!(pending, FanoutEvent::Closed) {
                                break;
                            }
                        }
                        let outcome = handler.rehydrate_tracked_async().await;
                        emit(&mut sink, outcome.responses).await?;
                    }
                    FanoutEvent::Closed => {
                        // The replicator stopped; keep serving the client its
                        // current view but no more live updates will arrive.
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::{Arc, Mutex};
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    /// Live end-to-end: a real client connects and sends `initConnection`,
    /// `ping`, and `changeDesiredQueries` frames; the server serves them
    /// through the full decode->route->act pipeline, answering ping with pong
    /// and handing the query actions to a handler that records them and emits a
    /// poke. Proves the whole connection loop over a real socket.
    #[tokio::test]
    async fn serves_a_real_client_through_the_full_pipeline() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let actions = Arc::new(Mutex::new(Vec::<String>::new()));
        let actions_srv = actions.clone();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut conn = WsConnection::accept(tcp).await.unwrap();
            conn.send_connected("ws1", 1.0).await.unwrap();
            serve_connection(&mut conn, move |action| {
                let label = match &action {
                    ConnectionAction::Initialize(_) => "init",
                    ConnectionAction::UpdateDesiredQueries(_) => "change",
                    ConnectionAction::DeleteClients(_) => "delete",
                    ConnectionAction::Pull(_) => "pull",
                    ConnectionAction::Push(_) => "push",
                    ConnectionAction::UpdateAuth(_) => "updateAuth",
                    ConnectionAction::AckMutationResponses(_) => "ack",
                    ConnectionAction::Inspect(_) => "inspect",
                    ConnectionAction::Close => "close",
                    ConnectionAction::Pong => "pong",
                };
                actions_srv.lock().unwrap().push(label.to_string());
                if label == "change" {
                    // Respond to a query change with a poke.
                    HandlerOutcome::send(vec![
                        r#"["pokeStart",{"pokeID":"p1","baseCookie":null}]"#.into()
                    ])
                } else {
                    HandlerOutcome::empty()
                }
            })
            .await
            .unwrap();
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();

        // Skip the `connected` greeting.
        let greeting = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(greeting.starts_with("[\"connected\","));

        // 1) initConnection, 2) ping, 3) changeDesiredQueries.
        client
            .send(Message::text(
                r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();
        client.send(Message::text(r#"["ping",{}]"#)).await.unwrap();
        client
            .send(Message::text(
                r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"clear"}]}]"#,
            ))
            .await
            .unwrap();

        // Expect a pong (from the ping) and a pokeStart (from the change).
        let pong = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(pong, r#"["pong",{}]"#);
        let poke = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(poke.contains("pokeStart"), "got {poke}");

        // Close from the client side.
        client.send(Message::Close(None)).await.unwrap();
        server.await.unwrap();

        let recorded = actions.lock().unwrap().clone();
        assert_eq!(recorded, vec!["init".to_string(), "change".to_string()]);
    }

    /// A `closeConnection` frame runs the handler once with
    /// [`ConnectionAction::Close`], flushes any farewell response, and cleanly
    /// ends the serve loop (returns `Ok`) — the application-level teardown
    /// path, distinct from a socket-level close (which ends via `recv_text`
    /// returning `None`). Proves the `ConnectionAction::Close => break` branch
    /// end-to-end: the server task completes without the client closing the
    /// socket first, and the farewell frame reaches the client.
    #[tokio::test]
    async fn close_connection_frame_ends_the_loop_after_flushing() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let actions = Arc::new(Mutex::new(Vec::<String>::new()));
        let actions_srv = actions.clone();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut conn = WsConnection::accept(tcp).await.unwrap();
            conn.send_connected("ws1", 1.0).await.unwrap();
            serve_connection(&mut conn, move |action| {
                let label = match &action {
                    ConnectionAction::Initialize(_) => "init",
                    ConnectionAction::Close => "close",
                    _ => "other",
                };
                actions_srv.lock().unwrap().push(label.to_string());
                if label == "close" {
                    // A farewell response must still be flushed before break.
                    HandlerOutcome::send(vec![r#"["error",{"kind":"Rehome"}]"#.into()])
                } else {
                    HandlerOutcome::empty()
                }
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
            .send(Message::text(r#"["closeConnection",{}]"#))
            .await
            .unwrap();

        // The farewell frame arrives, then the stream ends (server broke out
        // of the loop and dropped the socket).
        let farewell = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(farewell.contains("error"), "got {farewell}");
        // The server task returned Ok (clean application-level teardown).
        server.await.unwrap().unwrap();

        let recorded = actions.lock().unwrap().clone();
        assert_eq!(recorded, vec!["init".to_string(), "close".to_string()]);
    }

    /// A malformed (non-JSON) frame terminates the connection with
    /// [`ServeError::Decode`] rather than being silently ignored — the decode
    /// boundary surfaced live over a real socket.
    #[tokio::test]
    async fn malformed_frame_terminates_with_decode_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut conn = WsConnection::accept(tcp).await.unwrap();
            conn.send_connected("ws1", 1.0).await.unwrap();
            serve_connection(&mut conn, |_| HandlerOutcome::empty()).await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();
        client
            .send(Message::text("this is not json"))
            .await
            .unwrap();

        let result = server.await.unwrap();
        assert!(
            matches!(result, Err(ServeError::Decode(_))),
            "expected Decode error, got {result:?}"
        );
    }

    /// A data message before `initConnection` terminates the connection with a
    /// protocol error (the ordering the router enforces, surfaced live).
    #[tokio::test]
    async fn rejects_data_before_init() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut conn = WsConnection::accept(tcp).await.unwrap();
            conn.send_connected("ws1", 1.0).await.unwrap();
            serve_connection(&mut conn, |_| HandlerOutcome::empty()).await
        });

        let request = format!("ws://{addr}/sync").into_client_request().unwrap();
        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let _greeting = client.next().await.unwrap().unwrap();
        client
            .send(Message::text(
                r#"["changeDesiredQueries",{"desiredQueriesPatch":[]}]"#,
            ))
            .await
            .unwrap();

        let result = server.await.unwrap();
        assert!(
            matches!(
                result,
                Err(ServeError::Dispatch(
                    DispatchError::MessageBeforeInit { .. }
                ))
            ),
            "expected MessageBeforeInit, got {result:?}"
        );
    }

    #[tokio::test]
    async fn async_handler_can_await_before_responding() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut conn = WsConnection::accept(tcp).await.unwrap();
            conn.send_connected("ws1", 1.0).await.unwrap();
            serve_connection_async(&mut conn, |action| async move {
                match action {
                    ConnectionAction::Inspect(_) => {
                        tokio::task::yield_now().await;
                        HandlerOutcome::send(vec![
                            r#"["inspect",{"id":"async","op":"version","value":"ok"}]"#.into(),
                        ])
                    }
                    _ => HandlerOutcome::empty(),
                }
            })
            .await
            .unwrap();
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
            r#"["inspect",{"id":"async","op":"version","value":"ok"}]"#
        );

        client.send(Message::Close(None)).await.unwrap();
        server.await.unwrap();
    }
}
