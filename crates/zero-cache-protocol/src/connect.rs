//! Port of `zero-protocol/src/connect.ts`.
//!
//! After opening a websocket the client waits for a `connected` message
//! from the server, then sends `initConnection`. The server withholds
//! pokes until `initConnection` arrives, to avoid syncing queries the
//! client no longer wants.

use std::collections::BTreeMap;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;

use crate::client_schema::ClientSchema;
use crate::delete_clients::DeleteClientsBody;
use crate::queries_patch::UpQueriesPatch;

/// Port of `ConnectedBody`.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectedBody {
    pub wsid: String,
    pub timestamp: Option<f64>,
}

/// Port of `InitConnectionBody`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct InitConnectionBody {
    pub desired_queries_patch: UpQueriesPatch,
    /// Sent only when the client has no server snapshot (cookie) yet —
    /// once it does, the server is assumed to already have this client
    /// group's schema in the CVR store.
    pub client_schema: Option<ClientSchema>,
    pub deleted: Option<DeleteClientsBody>,
    pub user_push_url: Option<String>,
    pub user_push_headers: Option<BTreeMap<String, String>>,
    pub user_query_url: Option<String>,
    pub user_query_headers: Option<BTreeMap<String, String>>,
    /// Client ids currently active in the client group, so the server can
    /// inactivate queries from clients that are no longer alive.
    pub active_clients: Option<Vec<String>>,
    /// W3C traceparent header for distributed tracing.
    pub traceparent: Option<String>,
}

/// Errors decoding a WebSocket `Sec-WebSocket-Protocol` header value. Port
/// of the failure modes of `decodeSecProtocols` (malformed base64/URI
/// encoding, or non-UTF-8 bytes — a bad JSON payload is surfaced by the
/// caller's own JSON deserialization instead, not modeled here since this
/// crate doesn't serialize `InitConnectionBody` to/from JSON yet).
#[derive(Debug, thiserror::Error)]
pub enum SecProtocolError {
    #[error("invalid percent-encoding: {0}")]
    PercentDecode(String),
    #[error("invalid base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("invalid UTF-8: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

/// Percent-decodes `s` (the subset of percent-encoding `encodeURIComponent`
/// produces for a base64 alphabet: only `%XX` triples need decoding, since
/// base64's own characters `+/=` are the only non-alphanumeric bytes
/// `encodeURIComponent` would escape here). Minimal by design — this isn't
/// a general URI-component decoder, just enough to invert
/// [`percent_encode`].
fn percent_decode(s: &str) -> Result<Vec<u8>, SecProtocolError> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return Err(SecProtocolError::PercentDecode(s.to_string()));
            }
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3])
                .map_err(|_| SecProtocolError::PercentDecode(s.to_string()))?;
            let byte = u8::from_str_radix(hex, 16)
                .map_err(|_| SecProtocolError::PercentDecode(s.to_string()))?;
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    Ok(out)
}

/// Percent-encodes the standard-base64-only characters `encodeURIComponent`
/// would escape in a base64 string: `+`, `/`, `=`.
fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'+' => "%2B".to_string(),
            b'/' => "%2F".to_string(),
            b'=' => "%3D".to_string(),
            _ => (b as char).to_string(),
        })
        .collect()
}

/// Encodes the `(initConnectionMessage, authToken)` pair carried in a
/// WebSocket's `Sec-WebSocket-Protocol` header. Port of
/// `encodeSecProtocols`. Takes the already-JSON-serialized
/// `init_connection_message_json` (this crate has no JSON serializer for
/// `InitConnectionBody` yet) rather than the structured value, unlike
/// upstream which serializes inline.
pub fn encode_sec_protocols(
    init_connection_message_json: Option<&str>,
    auth_token: Option<&str>,
) -> String {
    // Upstream builds an object whose values are `undefined` when absent and
    // then calls JSON.stringify, which omits those keys rather than encoding
    // them as null. Preserve that exact wire shape: official zero-cache
    // distinguishes `{}` from explicit null fields during connection setup.
    let mut fields = Vec::new();
    if let Some(message) = init_connection_message_json {
        fields.push(format!("\"initConnectionMessage\":{message}"));
    }
    if let Some(token) = auth_token {
        let token = zero_cache_shared::bigint_json::JsonValue::String(token.to_string());
        fields.push(format!("\"authToken\":{}", token.stringify()));
    }
    let payload = format!("{{{}}}", fields.join(","));
    let b64 = BASE64_STANDARD.encode(payload.as_bytes());
    percent_encode(&b64)
}

/// Decodes a `Sec-WebSocket-Protocol` header value back into its raw JSON
/// payload string (`{"initConnectionMessage":...,"authToken":...}`). Port
/// of `decodeSecProtocols`, minus JSON parsing (returns the JSON text for
/// the caller to parse once this crate has an `InitConnectionBody`
/// deserializer).
pub fn decode_sec_protocols(sec_protocol: &str) -> Result<String, SecProtocolError> {
    let decoded = percent_decode(sec_protocol)?;
    let b64_str = String::from_utf8(decoded)?;
    let json_bytes = BASE64_STANDARD.decode(b64_str.as_bytes())?;
    Ok(String::from_utf8(json_bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_init_connection_message_and_auth_token() {
        let json = r#"["initConnection",{"desiredQueriesPatch":[]}]"#;
        let encoded = encode_sec_protocols(Some(json), Some("tok123"));
        let decoded = decode_sec_protocols(&encoded).unwrap();
        assert_eq!(
            decoded,
            format!("{{\"initConnectionMessage\":{json},\"authToken\":\"tok123\"}}")
        );
    }

    #[test]
    fn round_trips_with_no_message_or_token() {
        let encoded = encode_sec_protocols(None, None);
        let decoded = decode_sec_protocols(&encoded).unwrap();
        assert_eq!(decoded, "{}");
    }

    #[test]
    fn absent_message_is_omitted_when_auth_is_present() {
        let encoded = encode_sec_protocols(None, Some("tok123"));
        let decoded = decode_sec_protocols(&encoded).unwrap();
        assert_eq!(decoded, r#"{"authToken":"tok123"}"#);
    }

    #[test]
    fn encoding_is_percent_and_url_safe() {
        let encoded = encode_sec_protocols(None, None);
        // No raw '+' '/' '=' should survive unescaped (percent-encoded instead).
        assert!(!encoded.contains('+'));
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains('='));
    }

    #[test]
    fn connected_body_construction() {
        let body = ConnectedBody {
            wsid: "ws1".into(),
            timestamp: Some(123.0),
        };
        assert_eq!(body.wsid, "ws1");
    }
}
