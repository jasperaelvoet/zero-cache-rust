//! Upstream-compatible HTTP and WebSocket dispatch for the public Zero port.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use base64::Engine;
use futures_util::SinkExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::Message;

use zero_cache_protocol::error::ErrorBody;
use zero_cache_protocol::error_kind::ErrorKind;
use zero_cache_protocol::error_origin::ErrorOrigin;

use crate::http_dispatch::{peek_request, send_response, HttpResponse, RequestHead};

#[derive(Debug, Clone)]
pub struct PublicEndpointConfig {
    pub admin_password: Option<String>,
    pub development_mode: bool,
    pub keepalive_timeout_ms: Option<u64>,
    last_keepalive_ms: Arc<AtomicU64>,
}

impl PublicEndpointConfig {
    pub fn new(
        admin_password: Option<String>,
        development_mode: bool,
        keepalive_timeout_ms: Option<u64>,
    ) -> Self {
        Self {
            admin_password,
            development_mode,
            keepalive_timeout_ms,
            // `0` sentinel = no keepalive recorded yet. Upstream's HeartbeatMonitor
            // is opt-in (`#lastHeartbeat = 0`): the shutdown countdown only starts
            // once the first `/keepalive` arrives, so a deployment that never sends
            // heartbeats is never torn down.
            last_keepalive_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn keepalive_expired(&self) -> bool {
        let last = self.last_keepalive_ms.load(Ordering::Relaxed);
        // Monitor is inactive until the first heartbeat is seen.
        if last == 0 {
            return false;
        }
        self.keepalive_timeout_ms
            .is_some_and(|timeout| now_ms().saturating_sub(last) > timeout)
    }

    pub fn record_keepalive(&self) {
        self.last_keepalive_ms.store(now_ms(), Ordering::Relaxed);
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub enum PublicDisposition {
    Upgrade(TcpStream),
    Handled,
}

pub async fn dispatch(
    stream: TcpStream,
    config: &PublicEndpointConfig,
    replica_path: Option<&str>,
) -> PublicDisposition {
    let request = match peek_request(&stream).await {
        Ok(request) => request,
        Err(error) => {
            send_response(
                stream,
                HttpResponse::text("400 Bad Request", error.to_string()),
            )
            .await;
            return PublicDisposition::Handled;
        }
    };

    if request.is_websocket_upgrade() {
        match validate_sync_request(&request) {
            Ok(()) => return PublicDisposition::Upgrade(stream),
            Err(body) => {
                // Upstream (`workers/connection.ts` `#closeWithError` /
                // `websocket-handoff.ts` `onError`) does NOT reject the HTTP
                // handshake — a failed handshake reads as a hanging/opaque
                // connection to browsers. Instead it always completes the
                // WebSocket upgrade, then delivers a typed `["error", …]`
                // downstream frame and closes the socket.
                close_with_error(stream, &body).await;
                return PublicDisposition::Handled;
            }
        }
    }

    // Upstream fastify auto-exposes a HEAD route for every GET route
    // (`exposeHeadRoutes`): same status/headers, no body.
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("GET" | "HEAD", "/") => HttpResponse::text("200 OK", "OK"),
        ("GET" | "HEAD", "/keepalive") => {
            config.record_keepalive();
            HttpResponse::text("200 OK", "OK")
        }
        ("GET" | "HEAD", "/statz") => {
            if !admin_authorized(&request, config) {
                unauthorized("Statz Protected Area")
            } else {
                statz(&request, replica_path)
            }
        }
        ("GET" | "HEAD", "/heapz") => {
            if !admin_authorized(&request, config) {
                unauthorized("Heapz Protected Area")
            } else {
                heapz()
            }
        }
        _ => HttpResponse::text("404 Not Found", "Not Found"),
    };
    send_response(stream, response.for_method(&request.method)).await;
    PublicDisposition::Handled
}

fn validate_sync_request(request: &RequestHead) -> Result<(), ErrorBody> {
    let protocol_version = parse_sync_path(&request.path).ok_or_else(|| {
        ErrorBody::new(
            ErrorKind::InvalidConnectionRequest,
            format!("Invalid URL: {}", request.target),
            Some(ErrorOrigin::ZeroCache),
        )
    })?;
    // Returns a `VersionNotSupported` `ErrorBody` (port of `Connection#init`).
    zero_cache_workers::connection::check_protocol_version(protocol_version)?;
    let query_pairs = parse_query_pairs(&request.target);
    let headers: Vec<(String, String)> = request
        .headers
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    zero_cache_workers::connect_params::get_connect_params(protocol_version, &query_pairs, &headers)
        .map(|_| ())
        .map_err(|error| {
            ErrorBody::new(
                ErrorKind::InvalidConnectionRequest,
                error.to_string(),
                Some(ErrorOrigin::ZeroCache),
            )
        })
}

/// Serializes an [`ErrorBody`] as the downstream `["error", {…}]` wire frame a
/// client receives (port of `connection.ts` `sendError`'s
/// `send(lc, ws, ['error', errorBody])`). `origin` is included only when set,
/// matching `errorBodySchema`.
fn error_frame_json(body: &ErrorBody) -> String {
    let mut fields = serde_json::Map::new();
    fields.insert("kind".into(), body.kind.as_str().into());
    fields.insert("message".into(), body.message.clone().into());
    if let Some(origin) = body.origin {
        fields.insert("origin".into(), origin.as_str().into());
    }
    serde_json::Value::Array(vec![
        serde_json::Value::String("error".into()),
        serde_json::Value::Object(fields),
    ])
    .to_string()
}

/// Completes the WebSocket upgrade on `stream`, sends a typed `["error", …]`
/// downstream frame, and closes — upstream's `closeWithError` behavior for a
/// connection that fails validation. The handshake is completed even when the
/// failure was a malformed `Sec-WebSocket-Protocol` payload (the raw header is
/// echoed back without decoding), so the client still gets the error frame
/// rather than an opaque hung/failed handshake.
async fn close_with_error(stream: TcpStream, body: &ErrorBody) {
    let callback = |req: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
        if let Some(value) = req.headers().get("Sec-WebSocket-Protocol") {
            response
                .headers_mut()
                .insert("Sec-WebSocket-Protocol", value.clone());
        }
        Ok(response)
    };
    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
        return;
    };
    let _ = ws.send(Message::text(error_frame_json(body))).await;
    let _ = ws.close(None).await;
}

/// Official route pattern: `(/:base)/sync/v:version/connect`.
pub fn parse_sync_path(path: &str) -> Option<i64> {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    let sync = match segments.as_slice() {
        ["sync", version, "connect"] => Some(*version),
        [base, "sync", version, "connect"] if !base.is_empty() => Some(*version),
        _ => None,
    }?;
    sync.strip_prefix('v')?.parse().ok()
}

fn parse_query_pairs(target: &str) -> Vec<(String, String)> {
    target
        .split_once('?')
        .map(|(_, query)| {
            query
                .split('&')
                .filter(|pair| !pair.is_empty())
                .map(|pair| {
                    let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
                    (percent_decode(name), percent_decode(value))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&value[index + 1..index + 3], 16) {
                decoded.push(byte);
                index += 3;
                continue;
            }
        }
        decoded.push(if bytes[index] == b'+' {
            b' '
        } else {
            bytes[index]
        });
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn admin_authorized(request: &RequestHead, config: &PublicEndpointConfig) -> bool {
    if config.admin_password.is_none() && config.development_mode {
        return true;
    }
    let Some(expected) = config.admin_password.as_deref() else {
        return false;
    };
    let Some(encoded) = request
        .header("authorization")
        .and_then(|value| value.strip_prefix("Basic "))
    else {
        return false;
    };
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(credentials) = std::str::from_utf8(&decoded) else {
        return false;
    };
    credentials
        .split_once(':')
        .is_some_and(|(_, password)| constant_time_eq(password.as_bytes(), expected.as_bytes()))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    let length = left.len().max(right.len());
    for index in 0..length {
        difference |= usize::from(*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0));
    }
    difference == 0
}

fn unauthorized(realm: &str) -> HttpResponse {
    let mut response = HttpResponse::text("401 Unauthorized", "Unauthorized");
    response
        .headers
        .push(("WWW-Authenticate", format!("Basic realm=\"{realm}\"")));
    response
}

fn statz(request: &RequestHead, replica_path: Option<&str>) -> HttpResponse {
    let metadata = replica_path.and_then(|path| std::fs::metadata(path).ok());
    let all_stats = serde_json::json!({
        "replica": {
            "path": replica_path,
            "bytes": metadata.as_ref().map(std::fs::Metadata::len),
            "readonly": metadata.as_ref().map(std::fs::Metadata::permissions).map(|p| p.readonly()),
        },
        "os": {
            "availableParallelism": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1),
            "arch": std::env::consts::ARCH,
            "platform": std::env::consts::OS,
        }
    });
    let query = request
        .target
        .split_once('?')
        .map(|(_, query)| query)
        .unwrap_or("");
    let groups: Option<Vec<String>> = query
        .split('&')
        .find_map(|part| part.strip_prefix("group="))
        .map(|groups| groups.split(',').map(percent_decode).collect());
    let mut selected = serde_json::Map::new();
    for name in groups.unwrap_or_else(|| vec!["replica".into(), "os".into()]) {
        if let Some(value) = all_stats.get(&name) {
            selected.insert(name, value.clone());
        }
    }
    let stats = serde_json::Value::Object(selected);
    let json = query.split('&').any(|part| part == "format=json");
    let pretty = query
        .split('&')
        .any(|part| part == "pretty" || part.starts_with("pretty="));
    if json {
        let body = if pretty {
            serde_json::to_string_pretty(&stats)
        } else {
            serde_json::to_string(&stats)
        }
        .unwrap_or_else(|_| "{}".to_string());
        HttpResponse::json("200 OK", body)
    } else {
        let mut body = String::new();
        if let Some(groups) = stats.as_object() {
            for (name, value) in groups {
                body.push_str(&format!(
                    "\n=== {name} ===\n\n{}\n",
                    serde_json::to_string_pretty(value).unwrap_or_default()
                ));
            }
        }
        HttpResponse::text("200 OK", body)
    }
}

fn heapz() -> HttpResponse {
    // Upstream `/heapz` streams a V8 `.heapsnapshot`. This runtime has no V8, so
    // there is no such artifact to produce. Return an honest error rather than a
    // fabricated attachment impersonating a real heap snapshot.
    HttpResponse::text(
        "501 Not Implemented",
        "heap snapshots are not available in the native (non-V8) runtime",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn parses_only_official_sync_paths() {
        assert_eq!(parse_sync_path("/sync/v51/connect"), Some(51));
        assert_eq!(parse_sync_path("/app/sync/v30/connect"), Some(30));
        assert_eq!(parse_sync_path("/sync"), None);
        assert_eq!(parse_sync_path("/a/b/sync/v51/connect"), None);
        assert_eq!(parse_sync_path("/sync/v51/changes"), None);
    }

    #[test]
    fn keepalive_monitor_is_opt_in_until_first_heartbeat() {
        // With a zero timeout the monitor would expire instantly IF it were
        // armed at startup. Upstream's HeartbeatMonitor is opt-in, so before any
        // heartbeat it must report not-expired regardless of elapsed wall time.
        let config = PublicEndpointConfig::new(None, false, Some(0));
        assert!(
            !config.keepalive_expired(),
            "monitor must be inactive before the first keepalive"
        );
        // Once a heartbeat arms the countdown, a past-due deadline expires. Use a
        // deadline already in the past (timeout 0, then let the clock advance) by
        // recording, then asserting expiry after the recorded instant is stale.
        config.last_keepalive_ms.store(1, Ordering::Relaxed); // far in the past
        assert!(
            config.keepalive_expired(),
            "an armed monitor past its deadline must expire"
        );
    }

    #[test]
    fn admin_password_uses_basic_auth_password() {
        let encoded = base64::engine::general_purpose::STANDARD.encode("zero:secret");
        let request = RequestHead {
            method: "GET".into(),
            target: "/statz".into(),
            path: "/statz".into(),
            headers: BTreeMap::from([("authorization".into(), format!("Basic {encoded}"))]),
        };
        assert!(admin_authorized(
            &request,
            &PublicEndpointConfig::new(Some("secret".into()), false, None)
        ));
    }

    /// Upstream fastify exposes a HEAD route for every GET route
    /// (`exposeHeadRoutes`): status and headers as GET, no body. Health
    /// checkers commonly probe with HEAD.
    #[tokio::test]
    async fn head_keepalive_returns_headers_without_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config = PublicEndpointConfig::new(None, true, Some(60_000));
        let server = {
            let config = config.clone();
            tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.unwrap();
                dispatch(tcp, &config, None).await
            })
        };
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"HEAD /keepalive HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");
        // Content-Length advertises what GET would return ("OK"), but the
        // response ends with the blank line — no body follows.
        assert!(response.contains("Content-Length: 2"), "{response}");
        assert!(response.ends_with("\r\n\r\n"), "{response}");
        assert!(matches!(server.await.unwrap(), PublicDisposition::Handled));
        // A HEAD probe is still a heartbeat.
        assert_ne!(config.last_keepalive_ms.load(Ordering::Relaxed), 0);
    }

    /// Upstream never rejects the WebSocket handshake on a validation failure
    /// (`connection.ts` `#closeWithError` / `websocket-handoff.ts` `onError`):
    /// it completes the upgrade, then delivers a typed `["error", …]`
    /// downstream frame and closes. An unsupported protocol version must arrive
    /// as a `VersionNotSupported` error frame over the socket, not an HTTP 400.
    #[tokio::test]
    async fn unsupported_protocol_version_is_delivered_as_downstream_error_frame() {
        use futures_util::StreamExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            dispatch(tcp, &PublicEndpointConfig::new(None, true, None), None).await
        });

        // v9999 is far above the server's PROTOCOL_VERSION.
        let (mut client, _) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/sync/v9999/connect"))
                .await
                .expect("handshake should complete, not be rejected");
        let frame = client.next().await.unwrap().unwrap().into_text().unwrap();
        assert!(frame.starts_with("[\"error\","), "{frame}");
        assert!(
            frame.contains("\"kind\":\"VersionNotSupported\""),
            "{frame}"
        );
        assert!(frame.contains("\"origin\":\"zeroCache\""), "{frame}");
        assert!(matches!(server.await.unwrap(), PublicDisposition::Handled));
    }

    #[test]
    fn error_frame_serializes_kind_message_and_origin() {
        let body = ErrorBody::new(
            ErrorKind::InvalidConnectionRequest,
            "Invalid URL: /nope",
            Some(ErrorOrigin::ZeroCache),
        );
        assert_eq!(
            error_frame_json(&body),
            r#"["error",{"kind":"InvalidConnectionRequest","message":"Invalid URL: /nope","origin":"zeroCache"}]"#
        );
    }

    #[tokio::test]
    async fn root_is_served_on_the_public_listener() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            dispatch(tcp, &PublicEndpointConfig::new(None, true, None), None).await
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.ends_with("OK"));
        assert!(matches!(server.await.unwrap(), PublicDisposition::Handled));
    }
}
