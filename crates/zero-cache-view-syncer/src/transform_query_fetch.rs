//! Assembles `custom-queries/transform-query.ts`'s `CustomQueryTransformer#transform`
//! for real: glues together every previously-separate piece —
//! `transform_query_cache_key::get_cache_key`, `transform_query_response`'s
//! cache-split/response-shaping, `zero_cache_protocol::query_server_json`'s
//! response parser, and `zero_cache_mutagen::api_fetch::fetch_from_api_server`
//! (the actual retrying HTTP client) — into one live, callable function.
//! This closes the "custom-queries HTTP/wire slice" gap named across
//! several prior rounds: `fetch_from_api_server` and the pure
//! response-shaping logic both already existed but nothing wired them
//! together with real JSON (de)serialization in between.
//!
//! Scope: ports `transform()`'s non-empty-request path (the empty-request
//! short-circuit — return `cached: true` with zero HTTP calls — is
//! `transform_query_response::split_cached_and_uncached` returning an empty
//! `request`, handled by the caller not calling this fn at all, matching
//! how `#requestTransform`'s `queryIDs` derivation already lives in
//! `api_fetch`). `validate()` (the empty-request auth-maintenance path,
//! which always hits the server even with zero queries) is NOT ported —
//! still just a thin wrapper with no decision logic of its own; a caller
//! could build it directly from `zero_cache_mutagen::api_fetch::fetch_from_api_server`
//! with an empty request array.
//!
//! `JsonValue` (this crate's hand-rolled codec) <-> `serde_json::Value`
//! (what `reqwest`/`fetch_from_api_server` speak) conversion is done via a
//! JSON-text round trip (`stringify` + `serde_json::from_str`, and the
//! reverse) rather than a hand-written structural mapper — both are
//! complete JSON models, so a round trip through the same wire text they'd
//! both produce is exact and needs no per-variant matching code, unlike
//! `ast_json.rs`/`query_server_json.rs` which map into this port's own
//! richer typed structures, not just another JSON tree.

use zero_cache_mutagen::api_fetch::{fetch_from_api_server, ApiSource, FetchError};
use zero_cache_mutagen::api_request::HeaderOptions;
use zero_cache_protocol::custom_queries::TransformRequestQuery;
use zero_cache_protocol::query_server_json::query_response_from_json;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_shared::timed_cache::TimedCache;

use crate::transform_query_response::{
    shape_transform_response, HashedTransformResponse, TransformedAndHashed,
};

fn to_serde_json(v: &JsonValue) -> serde_json::Value {
    serde_json::from_str(&v.stringify()).expect("JsonValue::stringify always produces valid JSON")
}

fn from_serde_json(v: &serde_json::Value) -> JsonValue {
    zero_cache_shared::bigint_json::parse(&v.to_string())
        .expect("serde_json always produces valid JSON")
}

fn transform_request_query_to_json(q: &TransformRequestQuery) -> JsonValue {
    JsonValue::Object(vec![
        ("id".to_string(), JsonValue::String(q.id.clone())),
        ("name".to_string(), JsonValue::String(q.name.clone())),
        ("args".to_string(), JsonValue::Array(q.args.clone())),
    ])
}

/// Error performing a live `/transform` request. Distinguishes a
/// transport/HTTP-level failure ([`FetchError`]) from a well-formed-but-
/// unparseable response body (the JSON didn't match `queryResponseSchema`
/// at all — a wire-protocol mismatch, not the same thing as a
/// `TransformFailed` response body, which is a valid, successfully-parsed
/// failure).
#[derive(Debug, thiserror::Error)]
pub enum TransformFetchError {
    #[error(transparent)]
    Fetch(#[from] FetchError),
    #[error("malformed /query response: {0}")]
    MalformedResponse(#[from] zero_cache_protocol::query_server_json::QueryResponseJsonError),
}

/// Performs a real `/transform` request for `request` (the already-split
/// uncached queries from `transform_query_response::split_cached_and_uncached`)
/// and shapes the result, merging in `cached_responses` and populating
/// `cache` for every success. Port of the second half of `transform()` +
/// `#requestTransform`'s `operation: 'transform'` path, fully wired to a
/// real HTTP call this time (`transform_query_response::shape_transform_response`
/// already did the pure shaping half, taking an already-fetched
/// `QueryResponse` — this fn is what actually produces one).
#[allow(clippy::too_many_arguments)]
pub async fn fetch_and_shape_transform_response(
    client: &reqwest::Client,
    url: &str,
    schema: &str,
    app_id: &str,
    headers: &HeaderOptions<'_>,
    request: &[TransformRequestQuery],
    cached_responses: Vec<TransformedAndHashed>,
    cache: &mut TimedCache<String, TransformedAndHashed>,
    cache_key: impl FnMut(&str) -> String,
    now: i64,
) -> Result<HashedTransformResponse, TransformFetchError> {
    let body = to_serde_json(&JsonValue::Array(vec![
        JsonValue::String("transform".to_string()),
        JsonValue::Array(
            request
                .iter()
                .map(transform_request_query_to_json)
                .collect(),
        ),
    ]));

    let raw = fetch_from_api_server(
        client,
        ApiSource::Transform,
        url,
        schema,
        app_id,
        headers,
        &body,
    )
    .await?;
    let response = query_response_from_json(&from_serde_json(&raw))?;
    Ok(shape_transform_response(
        response,
        cached_responses,
        cache,
        cache_key,
        now,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform_query_response::TransformedOrErrored;

    /// A minimal local HTTP server (no external mocking crate — plain
    /// `tokio::net::TcpListener`, same pattern as
    /// `zero_cache_mutagen::api_fetch`'s own test server) so this test is a
    /// REAL request over a real socket, not a mocked HTTP client.
    async fn spawn_query_response_server(
        response_status: u16,
        response_body: &'static str,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let body = response_body;
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    let resp = format!(
                        "HTTP/1.1 {response_status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn live_transform_request_round_trips_a_real_http_response() {
        let url = spawn_query_response_server(
            200,
            r#"{"kind":"QueryResponse","queries":[{"id":"q1","name":"myQuery","ast":{"table":"issues"}}]}"#,
        )
        .await;
        let client = reqwest::Client::new();
        let mut cache: TimedCache<String, TransformedAndHashed> = TimedCache::new(5000);
        let request = vec![TransformRequestQuery {
            id: "q1".into(),
            name: "myQuery".into(),
            args: vec![],
        }];

        let result = fetch_and_shape_transform_response(
            &client,
            &url,
            "public",
            "app1",
            &HeaderOptions::default(),
            &request,
            vec![],
            &mut cache,
            |id| id.to_string(),
            0,
        )
        .await
        .unwrap();

        match result {
            HashedTransformResponse::Success { result, cached } => {
                assert!(!cached);
                assert_eq!(result.len(), 1);
                match &result[0] {
                    TransformedOrErrored::Ok(t) => assert_eq!(t.id, "q1"),
                    _ => panic!("expected Ok"),
                }
            }
            _ => panic!("expected Success"),
        }
        // The cache was actually populated by a REAL round trip.
        assert!(cache.get(&"q1".to_string(), 0).is_some());
    }

    #[tokio::test]
    async fn live_transform_failed_response_is_propagated() {
        let url = spawn_query_response_server(
            200,
            r#"{"kind":"TransformFailed","origin":"zeroCache","reason":"internal","message":"boom","queryIDs":["q1"]}"#,
        )
        .await;
        let client = reqwest::Client::new();
        let mut cache: TimedCache<String, TransformedAndHashed> = TimedCache::new(5000);
        let request = vec![TransformRequestQuery {
            id: "q1".into(),
            name: "myQuery".into(),
            args: vec![],
        }];

        let result = fetch_and_shape_transform_response(
            &client,
            &url,
            "public",
            "app1",
            &HeaderOptions::default(),
            &request,
            vec![],
            &mut cache,
            |id| id.to_string(),
            0,
        )
        .await
        .unwrap();

        assert!(matches!(result, HashedTransformResponse::Failed(body) if body.message == "boom"));
    }

    #[tokio::test]
    async fn a_non_ok_http_status_surfaces_as_a_fetch_error() {
        let url = spawn_query_response_server(500, "server error").await;
        let client = reqwest::Client::new();
        let mut cache: TimedCache<String, TransformedAndHashed> = TimedCache::new(5000);
        let request = vec![TransformRequestQuery {
            id: "q1".into(),
            name: "myQuery".into(),
            args: vec![],
        }];

        let err = fetch_and_shape_transform_response(
            &client,
            &url,
            "public",
            "app1",
            &HeaderOptions::default(),
            &request,
            vec![],
            &mut cache,
            |id| id.to_string(),
            0,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, TransformFetchError::Fetch(_)));
    }
}
