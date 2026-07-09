//! A real WebSocket sync connection, partial port of the accept-and-greet
//! half of `workers/syncer.ts`'s connection handling plus
//! `zero-protocol/src/connect.ts`'s handshake convention: after opening a
//! websocket, the client waits for a `connected` message before it starts
//! sending `initConnection`/pokes flow.
//!
//! Scope for this increment: accept a connection, read the client's
//! `Sec-WebSocket-Protocol` header (which upstream uses to carry the
//! init-connection message + auth token — see
//! `zero_cache_protocol::connect::decode_sec_protocols`) before the
//! handshake completes, send a `connected` message, then let the caller
//! send an arbitrary sequence of JSON text frames (e.g. a poke sequence).
//! This proves a real client<->server WebSocket round-trip using the
//! already-ported sync protocol data types — NOT a full sync-server
//! implementation. Message *bodies* (poke patches, mutation results, etc.)
//! are passed through as caller-supplied JSON text rather than serialized
//! from the typed `zero_cache_protocol` structs, since those don't have a
//! JSON (de)serializer yet (a real gap, tracked in `PORTING.md`).

use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use zero_cache_protocol::connect::{decode_sec_protocols, SecProtocolError};

#[derive(Debug, thiserror::Error)]
pub enum WsConnectionError {
    #[error("websocket handshake failed: {0}")]
    Handshake(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("failed to decode Sec-WebSocket-Protocol header: {0}")]
    SecProtocol(#[from] SecProtocolError),
}

/// An accepted server-side sync connection.
pub struct WsConnection {
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    /// The raw JSON payload decoded from the client's
    /// `Sec-WebSocket-Protocol` header (the `{"initConnectionMessage":...,
    /// "authToken":...}` object `encode_sec_protocols` produced), if the
    /// client sent one. `None` if the header was absent — some clients
    /// send `initConnection` as a regular message instead once connected.
    pub sec_protocol_payload: Option<String>,
}

impl WsConnection {
    /// Accepts a websocket handshake on `tcp`, capturing the
    /// `Sec-WebSocket-Protocol` header (if present) and echoing it back
    /// unmodified — required by the WebSocket protocol for the client to
    /// accept the handshake when it requested a subprotocol. Port of the
    /// accept-time portion of `syncer.ts`'s connection setup.
    pub async fn accept(tcp: TcpStream) -> Result<Self, WsConnectionError> {
        let captured = Arc::new(Mutex::new(None::<String>));
        let captured_for_cb = captured.clone();

        let callback =
            move |req: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
                if let Some(value) = req.headers().get("Sec-WebSocket-Protocol") {
                    if let Ok(s) = value.to_str() {
                        *captured_for_cb.lock().unwrap() = Some(s.to_string());
                        response
                            .headers_mut()
                            .insert("Sec-WebSocket-Protocol", value.clone());
                    }
                }
                Ok(response)
            };

        let stream =
            tokio_tungstenite::accept_hdr_async(MaybeTlsStream::Plain(tcp), callback).await?;

        let sec_protocol_payload = match captured.lock().unwrap().take() {
            Some(header_value) => Some(decode_sec_protocols(&header_value)?),
            None => None,
        };

        Ok(WsConnection {
            stream,
            sec_protocol_payload,
        })
    }

    /// Sends a `connected` message. Port of the `['connected', {wsid,
    /// timestamp}]` tuple message, hand-serialized to JSON text (see module
    /// doc on why: no typed JSON serializer for `ConnectedBody` yet).
    pub async fn send_connected(
        &mut self,
        wsid: &str,
        timestamp_ms: f64,
    ) -> Result<(), WsConnectionError> {
        let json = format!(
            "[\"connected\",{{\"wsid\":{},\"timestamp\":{timestamp_ms}}}]",
            json_string(wsid)
        );
        self.stream.send(Message::text(json)).await?;
        Ok(())
    }

    /// Sends an arbitrary pre-serialized JSON text message (e.g. a poke
    /// sequence built by the caller). Kept generic rather than typed per
    /// message kind, matching this increment's scope.
    pub async fn send_json(&mut self, json: &str) -> Result<(), WsConnectionError> {
        self.stream.send(Message::text(json.to_string())).await?;
        Ok(())
    }

    /// Reads the next text message, or `None` on a clean close.
    pub async fn recv_text(&mut self) -> Result<Option<String>, WsConnectionError> {
        loop {
            match self.stream.next().await {
                None => return Ok(None),
                Some(Ok(Message::Text(t))) => return Ok(Some(t.to_string())),
                Some(Ok(Message::Close(_))) => return Ok(None),
                Some(Ok(_)) => continue, // ping/pong/binary — ignored at this scope
                Some(Err(e)) => return Err(e.into()),
            }
        }
    }
}

/// Minimal JSON string escaping for the hand-serialized messages above.
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    /// Live end-to-end: a real client connects over a real TCP socket,
    /// sends a `Sec-WebSocket-Protocol` header carrying an
    /// `encode_sec_protocols`-encoded payload, and the server decodes it,
    /// completes the handshake (echoing the header back, since the
    /// WebSocket protocol requires that for the client to accept it), sends
    /// `connected`, then a hand-built poke sequence — proving both
    /// directions of a real socket round-trip using the ported protocol
    /// types.
    #[tokio::test]
    async fn accepts_connection_decodes_sec_protocol_and_sends_connected_then_poke() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut conn = WsConnection::accept(tcp).await.unwrap();
            let payload = conn.sec_protocol_payload.clone();
            conn.send_connected("ws1", 12345.0).await.unwrap();
            conn.send_json("[\"pokeStart\",{\"pokeID\":\"p1\",\"baseCookie\":null}]")
                .await
                .unwrap();
            conn.send_json("[\"pokeEnd\",{\"pokeID\":\"p1\",\"cookie\":\"01\"}]")
                .await
                .unwrap();
            payload
        });

        let encoded = zero_cache_protocol::connect::encode_sec_protocols(
            Some(r#"["initConnection",{"desiredQueriesPatch":[]}]"#),
            Some("test-token"),
        );
        let mut request = format!("ws://{addr}/sync").into_client_request().unwrap();
        request
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", encoded.parse().unwrap());

        let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();

        let connected = client.next().await.unwrap().unwrap();
        assert!(connected
            .into_text()
            .unwrap()
            .starts_with("[\"connected\","));

        let poke_start = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(poke_start.contains("\"pokeStart\""));
        let poke_end = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(poke_end.contains("\"pokeEnd\""));

        let payload = server.await.unwrap();
        assert!(
            payload.is_some(),
            "server should have decoded a Sec-WebSocket-Protocol payload"
        );
        assert!(payload.unwrap().contains("test-token"));
    }

    #[test]
    fn json_string_escapes_quotes_and_backslashes() {
        assert_eq!(json_string("a\"b\\c"), "\"a\\\"b\\\\c\"");
    }
}
