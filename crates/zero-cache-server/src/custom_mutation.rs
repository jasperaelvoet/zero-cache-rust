//! Custom-mutator forwarding — the `ZERO_MUTATE_URL` write path.
//!
//! When an app uses custom mutators (`defineMutator`), the client pushes
//! `type:"custom"` mutations (name + args). zero-cache does NOT run them; it
//! forwards the push to the app's mutate API server, which executes the
//! server-authoritative mutator against Postgres and returns per-mutation
//! results. This module builds that request, POSTs it (via
//! `zero_cache_mutagen::api_fetch::fetch_from_api_server`), and parses the
//! response into the `pushResponse` this port relays to the client. The writes
//! land in Postgres and replicate back through the normal path.

use zero_cache_mutagen::api_fetch::{fetch_from_api_server, ApiSource};
use zero_cache_mutagen::api_request::HeaderOptions;
use zero_cache_protocol::mutation_id::MutationId;
use zero_cache_protocol::mutation_result::{
    MutationAppError, MutationError, MutationOk, MutationResponse, MutationResult,
    MutationZeroError, ZeroErrorKind,
};
use zero_cache_protocol::push::{Mutation, PushBody};
use zero_cache_shared::bigint_json::JsonValue;

/// Config for a connection's mutate API server.
#[derive(Clone)]
pub struct MutateApi {
    pub client: reqwest::Client,
    pub url: String,
    pub api_key: Option<String>,
    pub schema: String,
    pub app_id: String,
    /// The client's `Cookie` header, forwarded when `ZERO_MUTATE_FORWARD_COOKIES`
    /// is set (session-cookie auth on the mutate server).
    pub cookie: Option<String>,
    /// Client request headers to forward (from `allowed-client-headers`).
    pub custom_headers: Vec<(String, String)>,
}

impl MutateApi {
    pub fn new(url: String, api_key: Option<String>, schema: String, app_id: String) -> Self {
        MutateApi {
            client: reqwest::Client::new(),
            url,
            api_key,
            schema,
            app_id,
            cookie: None,
            custom_headers: Vec::new(),
        }
    }

    /// Sets the forwarded session cookie + allowed client headers.
    pub fn with_forwarding(
        mut self,
        cookie: Option<String>,
        custom_headers: Vec<(String, String)>,
    ) -> Self {
        self.cookie = cookie;
        self.custom_headers = custom_headers;
        self
    }
}

/// Converts this port's `bigint_json::JsonValue` to `serde_json::Value` (the
/// type `fetch_from_api_server` speaks).
pub fn to_serde(v: &JsonValue) -> serde_json::Value {
    match v {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::Bool(b) => serde_json::Value::Bool(*b),
        JsonValue::Number(n) => serde_json::Number::from_f64(*n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        JsonValue::BigInt(b) => {
            // Prefer an exact integer; fall back to a string for out-of-range.
            if let Ok(i) = i64::try_from(b.clone()) {
                serde_json::Value::Number(i.into())
            } else {
                serde_json::Value::String(b.to_string())
            }
        }
        JsonValue::String(s) => serde_json::Value::String(s.clone()),
        JsonValue::Array(items) => serde_json::Value::Array(items.iter().map(to_serde).collect()),
        JsonValue::Object(fields) => serde_json::Value::Object(
            fields.iter().map(|(k, val)| (k.clone(), to_serde(val))).collect(),
        ),
    }
}

/// Builds the push-request body POSTed to the mutate API server, matching
/// upstream `pusher.ts`'s `PushBody` shape.
pub fn build_push_request(push: &PushBody) -> serde_json::Value {
    let mutations: Vec<serde_json::Value> = push
        .mutations
        .iter()
        .map(|m| match m {
            Mutation::Custom(c) => serde_json::json!({
                "type": "custom",
                "id": c.id,
                "clientID": c.client_id,
                "name": c.name,
                "args": c.args.iter().map(to_serde).collect::<Vec<_>>(),
                "timestamp": c.timestamp,
            }),
            Mutation::Crud(c) => serde_json::json!({
                "type": "crud",
                "id": c.id,
                "clientID": c.client_id,
                "args": [ to_serde(&c.ops_json) ],
                "timestamp": c.timestamp,
            }),
        })
        .collect();
    // Build the object explicitly so OPTIONAL fields are OMITTED when absent
    // rather than serialized as `null`. Upstream's `pushBodySchema` types
    // `schemaVersion`/`auth`/`traceparent` as `.optional()` (i.e. `number |
    // undefined`, never nullable), and JS `JSON.stringify` drops `undefined`
    // keys. Emitting `"schemaVersion":null` makes the app's valita parser throw
    // `Expected number at schemaVersion. Got null` (the hunting-game failure).
    let mut obj = serde_json::Map::new();
    obj.insert("clientGroupID".into(), serde_json::json!(push.client_group_id));
    obj.insert("mutations".into(), serde_json::Value::Array(mutations));
    obj.insert("pushVersion".into(), serde_json::json!(push.push_version));
    if let Some(sv) = push.schema_version {
        obj.insert("schemaVersion".into(), serde_json::json!(sv));
    }
    obj.insert("timestamp".into(), serde_json::json!(push.timestamp));
    obj.insert("requestID".into(), serde_json::json!(push.request_id));
    if let Some(tp) = &push.traceparent {
        obj.insert("traceparent".into(), serde_json::json!(tp));
    }
    serde_json::Value::Object(obj)
}

/// Parses a mutate API server response (`{mutations:[{id,result}]}`) into this
/// port's `MutationResponse`s.
pub fn parse_mutate_response(resp: &serde_json::Value) -> Vec<MutationResponse> {
    let Some(arr) = resp.get("mutations").and_then(|m| m.as_array()) else {
        return vec![];
    };
    arr.iter()
        .filter_map(|m| {
            let id_obj = m.get("id")?;
            let client_id = id_obj.get("clientID")?.as_str()?.to_string();
            let id = id_obj.get("id")?.as_f64()?;
            let result = m.get("result");
            let mr = match result {
                // The result's `error` field is the DISCRIMINANT ("app" |
                // "oooMutation" | "alreadyProcessed"), matching upstream's
                // appErrorSchema/zeroErrorSchema. The human message lives in
                // `message` (app) and arbitrary context in `details`.
                Some(r) if r.get("error").is_some() => {
                    let kind = r.get("error").and_then(|e| e.as_str()).unwrap_or("app");
                    let details = r.get("details").map(serde_to_bigint);
                    match kind {
                        "oooMutation" => MutationResult::Error(MutationError::Zero(
                            MutationZeroError { error: ZeroErrorKind::OooMutation, details },
                        )),
                        "alreadyProcessed" => MutationResult::Error(MutationError::Zero(
                            MutationZeroError {
                                error: ZeroErrorKind::AlreadyProcessed,
                                details,
                            },
                        )),
                        // "app" (and any unknown) → application error, carrying
                        // the mutator's real `message` (e.g. "You are already in
                        // a game") + details, so the client surfaces it verbatim.
                        _ => MutationResult::Error(MutationError::App(MutationAppError {
                            message: r.get("message").and_then(|s| s.as_str()).map(String::from),
                            details,
                        })),
                    }
                }
                Some(r) => MutationResult::Ok(MutationOk {
                    data: r.get("data").map(serde_to_bigint),
                }),
                None => MutationResult::Ok(MutationOk { data: None }),
            };
            Some(MutationResponse {
                id: MutationId { id, client_id },
                result: mr,
            })
        })
        .collect()
}

fn serde_to_bigint(v: &serde_json::Value) -> JsonValue {
    match v {
        serde_json::Value::Null => JsonValue::Null,
        serde_json::Value::Bool(b) => JsonValue::Bool(*b),
        serde_json::Value::Number(n) => JsonValue::Number(n.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(s) => JsonValue::String(s.clone()),
        serde_json::Value::Array(items) => {
            JsonValue::Array(items.iter().map(serde_to_bigint).collect())
        }
        serde_json::Value::Object(fields) => JsonValue::Object(
            fields.iter().map(|(k, val)| (k.clone(), serde_to_bigint(val))).collect(),
        ),
    }
}

/// Forwards `push` to the mutate API server and returns per-mutation responses.
/// On a transport/HTTP failure, every mutation is reported as an app error so
/// the client still gets a `pushResponse`.
pub async fn forward_push(
    api: &MutateApi,
    push: &PushBody,
    auth_raw: Option<&str>,
) -> Vec<MutationResponse> {
    let body = build_push_request(push);
    let names: Vec<&str> = push
        .mutations
        .iter()
        .map(|m| match m {
            Mutation::Custom(c) => c.name.as_str(),
            Mutation::Crud(_) => "_zero_crud",
        })
        .collect();
    crate::debug!(
        "forwarding push to mutate server {} (schema={}, appID={}, cookie={}, bearer={}): {} mutation(s) {:?}",
        api.url,
        api.schema,
        api.app_id,
        api.cookie.is_some(),
        auth_raw.is_some(),
        push.mutations.len(),
        names,
    );
    let headers = HeaderOptions {
        api_key: api.api_key.as_deref(),
        custom_headers: &api.custom_headers,
        request_headers: &[],
        auth_raw,
        cookie: api.cookie.as_deref(),
        origin: None,
    };
    match fetch_from_api_server(
        &api.client,
        ApiSource::Push,
        &api.url,
        &api.schema,
        &api.app_id,
        &headers,
        &body,
    )
    .await
    {
        Ok(resp) => {
            let responses = parse_mutate_response(&resp);
            let errors = responses
                .iter()
                .filter(|r| matches!(r.result, MutationResult::Error(_)))
                .count();
            if errors > 0 {
                crate::warn!(
                    "mutate server returned {errors} error(s) of {} mutation(s): {resp}",
                    responses.len()
                );
            } else {
                crate::debug!("mutate server OK: {} mutation result(s)", responses.len());
            }
            responses
        }
        Err(e) => {
            crate::warn!("mutate server call FAILED ({}): {e}", api.url);
            push.mutations
                .iter()
                .map(|m| MutationResponse {
                    id: m.id(),
                    result: MutationResult::Error(MutationError::App(MutationAppError {
                        message: Some(format!("mutate API server error: {e}")),
                        details: None,
                    })),
                })
                .collect()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::push::CustomMutation;

    fn custom_push() -> PushBody {
        PushBody {
            client_group_id: "cg1".into(),
            mutations: vec![Mutation::Custom(CustomMutation {
                id: 1.0,
                client_id: "c1".into(),
                name: "awardXP".into(),
                args: vec![JsonValue::Number(50.0)],
                timestamp: 1.0,
            })],
            push_version: 1.0,
            schema_version: None,
            timestamp: 1.0,
            request_id: "r1".into(),
            traceparent: None,
        }
    }

    #[test]
    fn builds_the_push_request_body() {
        let body = build_push_request(&custom_push());
        assert_eq!(body["clientGroupID"], "cg1");
        assert_eq!(body["pushVersion"], 1.0);
        let m = &body["mutations"][0];
        assert_eq!(m["type"], "custom");
        assert_eq!(m["name"], "awardXP");
        assert_eq!(m["args"][0], 50.0);
    }

    #[test]
    fn optional_schema_version_is_omitted_not_null() {
        // schemaVersion is `number().optional()` upstream — absent must be
        // OMITTED, not `null` (else the app's valita parser rejects it).
        let none = build_push_request(&custom_push()); // schema_version: None
        assert!(
            none.as_object().unwrap().get("schemaVersion").is_none(),
            "schemaVersion must be absent, got: {none}"
        );
        let mut with_sv = custom_push();
        with_sv.schema_version = Some(3.0);
        let body = build_push_request(&with_sv);
        assert_eq!(body["schemaVersion"], 3.0);
    }

    /// A minimal mock mutate API server: replies `status` + `body` to one POST.
    async fn spawn_mock(status: u16, body: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = vec![0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });
        format!("http://{addr}/push")
    }

    #[tokio::test]
    async fn forwards_custom_push_to_mutate_server_and_relays_response() {
        let url = spawn_mock(
            200,
            r#"{"mutations":[{"id":{"clientID":"c1","id":1},"result":{"data":{"xp":50}}}]}"#,
        )
        .await;
        let api = MutateApi::new(url, None, "public".into(), "zero".into());
        let responses = forward_push(&api, &custom_push(), Some("Bearer tok")).await;
        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0].id.client_id, "c1");
        assert!(matches!(responses[0].result, MutationResult::Ok(_)));
    }

    #[tokio::test]
    async fn server_error_reports_app_errors_for_every_mutation() {
        let url = spawn_mock(500, r#"{"error":"boom"}"#).await;
        let api = MutateApi::new(url, None, "public".into(), "zero".into());
        let responses = forward_push(&api, &custom_push(), None).await;
        assert_eq!(responses.len(), 1);
        assert!(matches!(
            responses[0].result,
            MutationResult::Error(MutationError::App(_))
        ));
    }

    #[test]
    fn parses_ok_and_error_responses() {
        // Real upstream shapes: app error carries the message in `message`
        // (not `details`); zero errors use the `error` discriminant.
        let resp = serde_json::json!({
            "mutations": [
                {"id": {"clientID":"c1","id":1}, "result": {"data": {"xp": 50}}},
                {"id": {"clientID":"c1","id":2}, "result": {"error":"app","message":"You are already in a game","details":{"code":409}}},
                {"id": {"clientID":"c1","id":3}, "result": {"error":"alreadyProcessed"}},
                {"id": {"clientID":"c1","id":4}, "result": {"error":"oooMutation","details":"gap"}},
            ]
        });
        let parsed = parse_mutate_response(&resp);
        assert_eq!(parsed.len(), 4);
        assert!(matches!(parsed[0].result, MutationResult::Ok(_)));
        match &parsed[1].result {
            MutationResult::Error(MutationError::App(e)) => {
                assert_eq!(e.message.as_deref(), Some("You are already in a game"));
                assert!(e.details.is_some(), "app error details preserved");
            }
            other => panic!("expected app error, got {other:?}"),
        }
        assert!(matches!(
            parsed[2].result,
            MutationResult::Error(MutationError::Zero(MutationZeroError {
                error: ZeroErrorKind::AlreadyProcessed,
                ..
            }))
        ));
        assert!(matches!(
            parsed[3].result,
            MutationResult::Error(MutationError::Zero(MutationZeroError {
                error: ZeroErrorKind::OooMutation,
                ..
            }))
        ));
    }
}
