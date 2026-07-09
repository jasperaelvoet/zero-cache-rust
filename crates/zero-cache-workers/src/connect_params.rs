//! Port of `zero-cache/src/workers/connect-params.ts` — parses a client's
//! WebSocket connect request (query string + headers) into `ConnectParams`,
//! the struct `ConnectionContextManager::register_connection` and the rest
//! of the connect path consume.
//!
//! Scope deviation: `initConnectionMsg` is kept as an opaque JSON string
//! (`Option<String>`, the raw decoded `Sec-WebSocket-Protocol` payload)
//! rather than a parsed `InitConnectionMessage` — this crate has no
//! `InitConnectionMessage` deserializer yet (`zero_cache_protocol::connect`
//! only ports `decode_sec_protocols`'s raw-JSON-text half; see that
//! module's doc). `auth`/token extraction from the decoded payload is done
//! via a minimal ad-hoc scan (see `extract_auth_token`) rather than a full
//! JSON parse into a typed shape, for the same reason. Both are documented,
//! narrower versions of the same "this crate has no InitConnectionBody JSON
//! deserializer yet" gap `zero_cache_protocol::connect`'s module doc
//! already names.

use std::collections::BTreeMap;

use zero_cache_protocol::connect::{decode_sec_protocols, SecProtocolError};

use crate::url_params::{UrlParams, UrlParamsError};

/// Port of `ConnectParams`, trimmed per the module doc (`initConnectionMsg`
/// is raw JSON text, not a parsed message).
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectParams {
    pub protocol_version: i64,
    pub client_id: String,
    pub client_group_id: String,
    pub profile_id: Option<String>,
    pub base_cookie: Option<String>,
    pub timestamp: i64,
    pub lm_id: i64,
    pub ws_id: String,
    pub debug_perf: bool,
    pub auth: Option<String>,
    pub user_id: Option<String>,
    pub init_connection_msg: Option<String>,
    pub http_cookie: Option<String>,
    pub origin: Option<String>,
    pub request_headers: BTreeMap<String, String>,
}

/// Port of `getConnectParams`'s failure surface: either a `UrlParamsError`
/// (malformed/missing query params) or a `SecProtocolError` (malformed
/// `Sec-WebSocket-Protocol` header), or a missing header entirely (upstream
/// throws via `must()`).
#[derive(Debug, thiserror::Error)]
pub enum ConnectParamsError {
    #[error(transparent)]
    UrlParams(#[from] UrlParamsError),
    #[error(transparent)]
    SecProtocol(#[from] SecProtocolError),
    #[error("missing sec-websocket-protocol header")]
    MissingSecProtocolHeader,
}

/// Best-effort extraction of `authToken` from the decoded
/// `{"initConnectionMessage":...,"authToken":...}` JSON text, without a
/// full JSON parser — see module doc. Looks for a `"authToken":"..."` or
/// `"authToken":null` tail (the shape `encode_sec_protocols` always
/// produces), which is all this port needs since it doesn't consume other
/// fields of the payload structurally yet.
fn extract_auth_token(decoded_json: &str) -> Option<String> {
    let marker = "\"authToken\":";
    let start = decoded_json.find(marker)? + marker.len();
    let rest = &decoded_json[start..];
    if rest.starts_with("null") {
        return None;
    }
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

/// Port of `getConnectParams`. `headers` is a plain case-sensitive
/// key/value list (a caller normalizes casing before calling, matching how
/// Node's `IncomingHttpHeaders` already lower-cases keys) — `sec-websocket-
/// protocol`/`cookie`/`origin` are looked up by their lower-case names.
pub fn get_connect_params(
    protocol_version: i64,
    query_pairs: &[(String, String)],
    headers: &[(String, String)],
) -> Result<ConnectParams, ConnectParamsError> {
    let params = UrlParams::new(query_pairs);

    let client_id = params.get_required("clientID")?;
    let client_group_id = params.get_required("clientGroupID")?;
    let profile_id = params.get("profileID", false)?;
    let base_cookie = params.get("baseCookie", false)?;
    let timestamp = params.get_integer_required("ts")?;
    let lm_id = params.get_integer_required("lmid")?;
    let ws_id = params.get("wsid", false)?.unwrap_or_default();
    let user_id = params.get("userID", false)?;
    let debug_perf = params.get_boolean("debugPerf");

    let header = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    };

    let sec_protocol =
        header("sec-websocket-protocol").ok_or(ConnectParamsError::MissingSecProtocolHeader)?;
    let decoded = decode_sec_protocols(&sec_protocol)?;
    let auth = extract_auth_token(&decoded);
    let init_connection_msg = if decoded == "{\"initConnectionMessage\":null,\"authToken\":null}" {
        None
    } else {
        Some(decoded)
    };

    let mut request_headers = BTreeMap::new();
    for (k, v) in headers {
        request_headers.insert(k.clone(), v.clone());
    }

    Ok(ConnectParams {
        protocol_version,
        client_id,
        client_group_id,
        profile_id,
        base_cookie,
        timestamp,
        lm_id,
        ws_id,
        debug_perf,
        auth,
        user_id,
        init_connection_msg,
        http_cookie: header("cookie"),
        origin: header("origin"),
        request_headers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::connect::encode_sec_protocols;

    fn q(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn h(sec_protocol: &str, extra: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut headers = vec![(
            "sec-websocket-protocol".to_string(),
            sec_protocol.to_string(),
        )];
        headers.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        headers
    }

    #[test]
    fn parses_required_and_optional_query_params() {
        let query = q(&[
            ("clientID", "c1"),
            ("clientGroupID", "cg1"),
            ("ts", "100"),
            ("lmid", "5"),
        ]);
        let sec = encode_sec_protocols(None, Some("tok"));
        let headers = h(
            &sec,
            &[("cookie", "session=abc"), ("origin", "https://example.com")],
        );

        let params = get_connect_params(1, &query, &headers).unwrap();
        assert_eq!(params.client_id, "c1");
        assert_eq!(params.client_group_id, "cg1");
        assert_eq!(params.timestamp, 100);
        assert_eq!(params.lm_id, 5);
        assert_eq!(params.ws_id, "");
        assert_eq!(params.auth, Some("tok".into()));
        assert_eq!(params.http_cookie, Some("session=abc".into()));
        assert_eq!(params.origin, Some("https://example.com".into()));
        assert!(!params.debug_perf);
    }

    #[test]
    fn missing_required_query_param_errors() {
        let query = q(&[("clientID", "c1")]);
        let sec = encode_sec_protocols(None, None);
        let headers = h(&sec, &[]);
        assert!(get_connect_params(1, &query, &headers).is_err());
    }

    #[test]
    fn missing_sec_websocket_protocol_header_errors() {
        let query = q(&[
            ("clientID", "c1"),
            ("clientGroupID", "cg1"),
            ("ts", "100"),
            ("lmid", "5"),
        ]);
        let err = get_connect_params(1, &query, &[]).unwrap_err();
        assert!(matches!(err, ConnectParamsError::MissingSecProtocolHeader));
    }

    #[test]
    fn debug_perf_true_is_parsed() {
        let query = q(&[
            ("clientID", "c1"),
            ("clientGroupID", "cg1"),
            ("ts", "100"),
            ("lmid", "5"),
            ("debugPerf", "true"),
        ]);
        let sec = encode_sec_protocols(None, None);
        let headers = h(&sec, &[]);
        let params = get_connect_params(1, &query, &headers).unwrap();
        assert!(params.debug_perf);
    }

    #[test]
    fn no_auth_token_yields_none() {
        let query = q(&[
            ("clientID", "c1"),
            ("clientGroupID", "cg1"),
            ("ts", "100"),
            ("lmid", "5"),
        ]);
        let sec = encode_sec_protocols(None, None);
        let headers = h(&sec, &[]);
        let params = get_connect_params(1, &query, &headers).unwrap();
        assert_eq!(params.auth, None);
        assert_eq!(params.init_connection_msg, None);
    }

    #[test]
    fn init_connection_message_json_is_preserved_opaquely() {
        let query = q(&[
            ("clientID", "c1"),
            ("clientGroupID", "cg1"),
            ("ts", "100"),
            ("lmid", "5"),
        ]);
        let msg_json = r#"["initConnection",{"desiredQueriesPatch":[]}]"#;
        let sec = encode_sec_protocols(Some(msg_json), None);
        let headers = h(&sec, &[]);
        let params = get_connect_params(1, &query, &headers).unwrap();
        assert!(params.init_connection_msg.unwrap().contains(msg_json));
    }

    #[test]
    fn request_headers_are_collected() {
        let query = q(&[
            ("clientID", "c1"),
            ("clientGroupID", "cg1"),
            ("ts", "100"),
            ("lmid", "5"),
        ]);
        let sec = encode_sec_protocols(None, None);
        let headers = h(&sec, &[("x-custom", "value")]);
        let params = get_connect_params(1, &query, &headers).unwrap();
        assert_eq!(
            params.request_headers.get("x-custom"),
            Some(&"value".to_string())
        );
    }
}
