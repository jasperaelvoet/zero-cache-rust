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

use zero_cache_protocol::connect::{decode_sec_protocols, InitConnectionBody, SecProtocolError};
use zero_cache_protocol::up::Upstream;
use zero_cache_protocol::up_json::upstream_from_json;
use zero_cache_shared::bigint_json::{parse, JsonValue};

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
    /// The request-target of the handshake (origin-form, e.g.
    /// `/sync/v51/connect?clientGroupID=…&clientID=…`), as a real
    /// `@rocicorp/zero` client sends. Used to recover the client's identity
    /// (`clientGroupID`/`clientID`/`baseCookie`) from the connect URL.
    pub request_uri: Option<String>,
    /// The client's `Cookie` header (session cookie), captured at the handshake
    /// so it can be forwarded to the query/mutate API servers when
    /// `ZERO_QUERY_FORWARD_COOKIES`/`ZERO_MUTATE_FORWARD_COOKIES` is set — the
    /// auth path for apps that authenticate via a session cookie.
    pub cookie: Option<String>,
    /// All client request headers (lowercased names), for
    /// `allowed-client-headers` forwarding.
    pub request_headers: Vec<(String, String)>,
}

/// Extracts a query-string parameter from a request-target like
/// `/sync/v51/connect?clientGroupID=abc&clientID=def`. Percent-decoding is
/// limited to `%XX` byte escapes, sufficient for the ASCII ids zero sends.
pub fn query_param(request_uri: &str, key: &str) -> Option<String> {
    let query = request_uri.split_once('?').map(|(_, q)| q)?;
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if k == key {
            return Some(percent_decode(v));
        }
    }
    None
}

/// Extracts the client's `authToken` from a decoded `Sec-WebSocket-Protocol`
/// payload (`{"initConnectionMessage":…,"authToken":"…"}` — the shape
/// `encode_sec_protocols` produces). This is the client's INITIAL bearer token,
/// sent at connect (a real `@rocicorp/zero` client with `new Zero({auth})`
/// puts it here); it must seed the connection's auth so the very first
/// forwarded mutation/query carries `Authorization: Bearer …`, before any
/// `updateAuth` refresh. Returns `None` for `"authToken":null` or if absent.
pub fn auth_token_from_payload(decoded: &str) -> Option<String> {
    let marker = "\"authToken\":";
    let start = decoded.find(marker)? + marker.len();
    let rest = decoded[start..].trim_start();
    if rest.starts_with("null") {
        return None;
    }
    let rest = rest.strip_prefix('"')?;
    // Take up to the next unescaped quote (tokens are base64url/JWT — no quotes).
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Extracts the optional `initConnection` message carried in the websocket
/// subprotocol payload. Real Zero clients use this fast path when reconnecting
/// with a persisted cookie; it must be applied after the `connected` greeting
/// even though no text init frame follows on the socket.
pub fn init_connection_from_payload(payload: &str) -> Option<InitConnectionBody> {
    let JsonValue::Object(fields) = parse(payload).ok()? else {
        return None;
    };
    let message = fields
        .iter()
        .find(|(name, _)| name == "initConnectionMessage")
        .map(|(_, value)| value)?;
    match upstream_from_json(message).ok()? {
        Upstream::InitConnection(body) => Some(body),
        _ => None,
    }
}

/// `ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES` (upstream default 10 MiB), read once per
/// process. Applied to every accepted client WebSocket so oversized incoming
/// messages are rejected by the protocol layer before parsing — upstream's
/// `ws` `maxPayload` contract. (The internal change-streamer WebSocket is not
/// governed by this option upstream either.)
pub fn configured_max_payload_bytes() -> usize {
    static CELL: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CELL.get_or_init(|| {
        std::env::var("ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10 * 1024 * 1024)
    })
}

impl WsConnection {
    /// Accepts a websocket handshake on `tcp`, capturing the
    /// `Sec-WebSocket-Protocol` header (if present) and echoing it back
    /// unmodified — required by the WebSocket protocol for the client to
    /// accept the handshake when it requested a subprotocol. Port of the
    /// accept-time portion of `syncer.ts`'s connection setup.
    #[allow(clippy::result_large_err)]
    pub async fn accept(tcp: TcpStream) -> Result<Self, WsConnectionError> {
        Self::accept_with_max_payload(tcp, Some(configured_max_payload_bytes())).await
    }

    /// [`Self::accept`] with an explicit incoming-message size limit —
    /// upstream's `ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES` (`ws` `maxPayload`):
    /// messages exceeding the limit are rejected by the protocol layer before
    /// parsing. `None` uses tungstenite's defaults.
    #[allow(clippy::result_large_err)]
    pub async fn accept_with_max_payload(
        tcp: TcpStream,
        max_payload_bytes: Option<usize>,
    ) -> Result<Self, WsConnectionError> {
        let captured = Arc::new(Mutex::new(None::<String>));
        let captured_for_cb = captured.clone();
        let captured_uri = Arc::new(Mutex::new(None::<String>));
        let captured_uri_cb = captured_uri.clone();
        let captured_headers = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
        let captured_headers_cb = captured_headers.clone();

        let callback =
            move |req: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
                *captured_uri_cb.lock().unwrap() = Some(req.uri().to_string());
                // Capture all client request headers (lowercased names) for
                // cookie / allowed-client-header forwarding to app API servers.
                let mut hdrs = Vec::new();
                for (name, value) in req.headers() {
                    if let Ok(v) = value.to_str() {
                        hdrs.push((name.as_str().to_ascii_lowercase(), v.to_string()));
                    }
                }
                *captured_headers_cb.lock().unwrap() = hdrs;
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

        let ws_config = max_payload_bytes.map(|max| {
            tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                .max_message_size(Some(max))
                // Frames are bounded by messages; cap them identically so a
                // single oversized frame is rejected as early as possible.
                .max_frame_size(Some(max))
        });
        let stream = tokio_tungstenite::accept_hdr_async_with_config(
            MaybeTlsStream::Plain(tcp),
            callback,
            ws_config,
        )
        .await?;

        let sec_protocol_payload = match captured.lock().unwrap().take() {
            Some(header_value) => Some(decode_sec_protocols(&header_value)?),
            None => None,
        };
        let request_uri = captured_uri.lock().unwrap().take();
        let request_headers = std::mem::take(&mut *captured_headers.lock().unwrap());
        let cookie = request_headers
            .iter()
            .find(|(k, _)| k == "cookie")
            .map(|(_, v)| v.clone());

        Ok(WsConnection {
            stream,
            sec_protocol_payload,
            request_uri,
            cookie,
            request_headers,
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

    /// Sends a binary frame (used by the change-streamer to transfer a replica
    /// snapshot to a view-syncer).
    pub async fn send_binary(&mut self, bytes: Vec<u8>) -> Result<(), WsConnectionError> {
        self.stream.send(Message::binary(bytes)).await?;
        Ok(())
    }

    /// Receives the next binary frame, or `None` on close (skips text frames).
    pub async fn recv_binary(&mut self) -> Result<Option<Vec<u8>>, WsConnectionError> {
        loop {
            match self.stream.next().await {
                None => return Ok(None),
                Some(Ok(Message::Binary(b))) => return Ok(Some(b.to_vec())),
                Some(Ok(Message::Close(_))) => return Ok(None),
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(e.into()),
            }
        }
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

/// The write half of a split [`WsConnection`].
pub type WsSink =
    futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
/// The read half of a split [`WsConnection`].
pub type WsStream = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

impl WsConnection {
    /// Splits the connection into independent write/read halves so a serve loop
    /// can send server-initiated frames (pokes) while concurrently awaiting
    /// client frames (`tokio::select!`). Call after [`send_connected`](Self::send_connected).
    pub fn into_split(self) -> (WsSink, WsStream) {
        self.stream.split()
    }
}

/// Reads the next text frame from a [`WsStream`], skipping control/binary
/// frames; returns `None` on close/quiet/error.
pub async fn recv_text_from(stream: &mut WsStream) -> Option<String> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => return Some(t.to_string()),
            Some(Ok(Message::Close(_))) | None => return None,
            Some(Ok(_)) => continue,
            Some(Err(_)) => return None,
        }
    }
}

/// Sends a text frame on a [`WsSink`].
pub async fn send_text_to(sink: &mut WsSink, text: &str) -> Result<(), WsConnectionError> {
    sink.send(Message::text(text.to_string())).await?;
    Ok(())
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

    #[test]
    fn query_param_parses_zero_connect_url() {
        let uri = "/sync/v51/connect?clientGroupID=abc&clientID=def&wsid=123&baseCookie=&lmid=1";
        assert_eq!(query_param(uri, "clientGroupID").as_deref(), Some("abc"));
        assert_eq!(query_param(uri, "clientID").as_deref(), Some("def"));
        assert_eq!(query_param(uri, "baseCookie").as_deref(), Some(""));
        assert_eq!(query_param(uri, "missing"), None);
    }

    #[test]
    fn query_param_percent_decodes() {
        let uri = "/sync/v51/connect?clientGroupID=a%2Fb%20c";
        assert_eq!(query_param(uri, "clientGroupID").as_deref(), Some("a/b c"));
    }

    #[test]
    fn query_param_no_query_is_none() {
        assert_eq!(query_param("/sync", "clientID"), None);
    }

    #[test]
    fn auth_token_extraction() {
        assert_eq!(
            auth_token_from_payload(r#"{"initConnectionMessage":null,"authToken":"eyJabc.def"}"#)
                .as_deref(),
            Some("eyJabc.def")
        );
        assert_eq!(
            auth_token_from_payload(r#"{"initConnectionMessage":null,"authToken":null}"#),
            None
        );
        assert_eq!(auth_token_from_payload("{}"), None);
    }

    /// The captured request URI carries the client's identity: a real client
    /// connecting to `/sync/v51/connect?...` has its clientGroupID/clientID
    /// recoverable from `request_uri`.
    #[tokio::test]
    async fn accept_captures_the_request_uri_for_client_identity() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            WsConnection::accept(tcp).await.unwrap().request_uri
        });
        let _client = tokio_tungstenite::connect_async(format!(
            "ws://{addr}/sync/v51/connect?clientGroupID=grp7&clientID=cli9&lmid=0"
        ))
        .await
        .unwrap();
        let uri = server.await.unwrap().expect("uri captured");
        assert_eq!(query_param(&uri, "clientGroupID").as_deref(), Some("grp7"));
        assert_eq!(query_param(&uri, "clientID").as_deref(), Some("cli9"));
    }

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
