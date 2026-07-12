//! The actual HTTP call half of `custom/fetch.ts`'s `fetchFromAPIServer` —
//! closes the "no HTTP client dependency" gap `api_request.rs` deferred.
//! Uses `reqwest` (added as a real dependency this round, same pattern as
//! adding `tokio-postgres`/`tokio-tungstenite` when those subsystems
//! needed a real client).
//!
//! Scope: [`fetch_from_api_server`] performs the POST, retries on
//! 502/504/connect-failure up to `MAX_ATTEMPTS` with
//! `api_request::get_backoff_delay_ms` between attempts, and returns the
//! parsed JSON response body on success. NOT ported: response-body
//! validation against a `valita` schema (`validator.parse(json, {mode:
//! 'passthrough'})` — this crate has no schema-validation library, so the
//! response is returned as a generic `serde_json::Value` for the caller to
//! interpret), `apiErrorFromResult`'s legacy-error-shape detection, and all
//! OpenTelemetry metrics recording (observability, not request logic).
//! `jitter` per attempt is generated via a simple xorshift-seeded PRNG
//! (this port's established substitute for `Math.random()` where
//! determinism isn't required — network retry timing has no correctness
//! dependency on the exact jitter value).

use std::time::Duration;

use zero_cache_protocol::error_kind::ErrorKind;
use zero_cache_protocol::error_reason::ErrorReason;

use crate::api_request::{
    build_final_url, build_request_headers, HeaderOptions, ReservedParamError,
};

const MAX_ATTEMPTS: u32 = 4;

/// Port of `apiFailedBody`'s `ErrorBody` shape, trimmed to the fields this
/// module populates (`mutationIDs`/`queryIDs` are the caller's
/// responsibility to fill in, since this function doesn't know which
/// mutations were in the failed push).
#[derive(Debug, Clone, PartialEq)]
pub struct ApiFailure {
    pub kind: ErrorKind,
    pub reason: ErrorReason,
    pub message: String,
    pub status: Option<u16>,
    pub body_preview: Option<String>,
}

impl ApiFailure {
    /// The HTTP status class for this failure, e.g. `503` → `"5xx"`, `404` →
    /// `"4xx"` — the `http_status_class` metric attribute upstream's
    /// `custom/fetch.ts` computes as `` `${Math.floor(status / 100)}xx` ``.
    /// `None` when the failure carries no HTTP status (e.g. a connection
    /// error).
    pub fn http_status_class(&self) -> Option<String> {
        self.status.map(|s| format!("{}xx", s / 100))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error(transparent)]
    ReservedParam(#[from] ReservedParamError),
    /// Port of `apiFailedBody`'s failure outcomes — an HTTP-level or
    /// transport-level failure after exhausting retries.
    #[error("{}", .0.message)]
    Api(ApiFailure),
}

/// Which upstream JSON shape (`push` or `transform`) this fetch is for —
/// determines whether a failure becomes `PushFailed` or `TransformFailed`.
/// Port of `fetchFromAPIServer`'s `source` parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiSource {
    Push,
    Transform,
}

/// Pure core of `custom/fetch.ts`'s `getBodyPreview`: returns `body`
/// unchanged if it is at most 512 characters, otherwise its first 512
/// characters followed by `...`. Upstream slices by UTF-16 code units
/// (`String.slice(0, 512)`); this slices by `char`s — identical for ASCII/BMP
/// bodies (which API responses are in practice) and, unlike a raw byte slice,
/// it can never split a multi-byte UTF-8 sequence (so it cannot panic, which a
/// prior `&t[..512]` here could on a non-ASCII body straddling byte 512).
pub fn body_preview(body: &str) -> String {
    match body.char_indices().nth(512) {
        None => body.to_string(),
        Some((byte_idx, _)) => format!("{}...", &body[..byte_idx]),
    }
}

fn failed_kind(source: ApiSource) -> ErrorKind {
    match source {
        ApiSource::Push => ErrorKind::PushFailed,
        ApiSource::Transform => ErrorKind::TransformFailed,
    }
}

/// A minimal xorshift PRNG, seeded per call — this port's established
/// substitute for `Math.random()` where the exact value doesn't need to be
/// deterministic/reproducible (see module doc).
struct Xorshift(u64);
impl Xorshift {
    fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % 1000) as f64 / 1000.0
    }
}

/// Port of `fetchFromAPIServer`: POSTs `body` (already-serialized JSON) to
/// `url` (after appending the reserved `schema`/`appID` params), retrying
/// on 502/504 or a connection failure up to [`MAX_ATTEMPTS`] times with
/// exponential backoff. Returns the parsed JSON response on success.
///
/// NOT ported here: `urlMatch`'s allowed-URL-pattern check — callers
/// should perform that themselves before calling this function (it needs a
/// URL-pattern-matching library this crate doesn't depend on).
pub async fn fetch_from_api_server(
    client: &reqwest::Client,
    source: ApiSource,
    url: &str,
    schema: &str,
    app_id: &str,
    headers: &HeaderOptions<'_>,
    body: &serde_json::Value,
) -> Result<serde_json::Value, FetchError> {
    let final_url = build_final_url(url, schema, app_id)?;
    let header_pairs = build_request_headers(headers);

    let mut rng = Xorshift(0x9E3779B97F4A7C15 ^ (final_url.len() as u64 + 1));

    for attempt in 1..=MAX_ATTEMPTS {
        let mut req = client.post(&final_url).json(body);
        for (k, v) in &header_pairs {
            req = req.header(k.as_str(), v.as_str());
        }

        match req.send().await {
            Ok(response) => {
                let status = response.status();
                if !status.is_success() {
                    let will_retry = (status.as_u16() == 502 || status.as_u16() == 504)
                        && attempt < MAX_ATTEMPTS;
                    if will_retry {
                        tokio::time::sleep(Duration::from_millis(
                            crate::api_request::get_backoff_delay_ms(
                                attempt,
                                rng.next_f64() * 100.0,
                            ) as u64,
                        ))
                        .await;
                        continue;
                    }
                    let body_preview = response.text().await.ok().map(|t| body_preview(&t));
                    return Err(FetchError::Api(ApiFailure {
                        kind: failed_kind(source),
                        reason: ErrorReason::Http,
                        message: format!(
                            "Fetch from API server returned non-OK status {}",
                            status.as_u16()
                        ),
                        status: Some(status.as_u16()),
                        body_preview,
                    }));
                }
                return response.json::<serde_json::Value>().await.map_err(|e| {
                    FetchError::Api(ApiFailure {
                        kind: failed_kind(source),
                        reason: ErrorReason::Parse,
                        message: format!("Failed to parse response from API server: {e}"),
                        status: Some(status.as_u16()),
                        body_preview: None,
                    })
                });
            }
            Err(e) => {
                let will_retry = e.is_connect() && attempt < MAX_ATTEMPTS;
                if will_retry {
                    tokio::time::sleep(Duration::from_millis(
                        crate::api_request::get_backoff_delay_ms(attempt, rng.next_f64() * 100.0)
                            as u64,
                    ))
                    .await;
                    continue;
                }
                return Err(FetchError::Api(ApiFailure {
                    kind: failed_kind(source),
                    reason: ErrorReason::Internal,
                    message: format!("Fetch from API server threw error: {e}"),
                    status: None,
                    body_preview: None,
                }));
            }
        }
    }
    unreachable!("loop always returns within MAX_ATTEMPTS iterations")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn http_status_class_buckets_by_hundreds() {
        let mk = |status: Option<u16>| ApiFailure {
            kind: ErrorKind::PushFailed,
            reason: ErrorReason::Http,
            message: String::new(),
            status,
            body_preview: None,
        };
        assert_eq!(mk(Some(503)).http_status_class().as_deref(), Some("5xx"));
        assert_eq!(mk(Some(404)).http_status_class().as_deref(), Some("4xx"));
        assert_eq!(mk(Some(200)).http_status_class().as_deref(), Some("2xx"));
        assert_eq!(mk(None).http_status_class(), None);
    }

    #[test]
    fn body_preview_returns_short_bodies_unchanged() {
        assert_eq!(body_preview(""), "");
        assert_eq!(body_preview("short body"), "short body");
        let exactly_512 = "a".repeat(512);
        assert_eq!(body_preview(&exactly_512), exactly_512);
    }

    #[test]
    fn body_preview_truncates_long_bodies_with_ellipsis() {
        let long = "a".repeat(513);
        let preview = body_preview(&long);
        assert_eq!(preview, format!("{}...", "a".repeat(512)));
    }

    #[test]
    fn body_preview_never_splits_a_multibyte_char() {
        // 511 ASCII bytes then a 3-byte char at char index 511; the 512-char
        // cutoff falls right after it. A byte slice `&t[..512]` would panic
        // mid-char here; the char-based slice must not.
        let body = format!("{}{}", "a".repeat(511), "€€".repeat(10));
        let preview = body_preview(&body);
        // 512 chars kept (511 'a' + 1 '€'), then "...".
        assert!(preview.ends_with("..."));
        assert_eq!(preview.chars().count(), 512 + 3); // 512 chars + "..."
        assert!(preview.starts_with(&"a".repeat(511)));
    }

    /// A minimal local HTTP server (no external mocking crate — plain
    /// `tokio::net::TcpListener`) so this test is a REAL request over a
    /// real socket, not a mocked HTTP client.
    async fn spawn_test_server(response_status: u16, response_body: &'static str) -> String {
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
        format!("http://{addr}/push")
    }

    #[tokio::test]
    async fn successful_fetch_returns_parsed_json() {
        let url = spawn_test_server(200, r#"{"mutations":[]}"#).await;
        let client = reqwest::Client::new();
        let result = fetch_from_api_server(
            &client,
            ApiSource::Push,
            &url,
            "myapp_0",
            "myapp",
            &HeaderOptions::default(),
            &serde_json::json!({"clientGroupID": "cg1"}),
        )
        .await
        .unwrap();
        assert_eq!(result, serde_json::json!({"mutations": []}));
    }

    #[tokio::test]
    async fn non_retryable_http_error_fails_immediately() {
        let url = spawn_test_server(400, r#"{"error":"bad request"}"#).await;
        let client = reqwest::Client::new();
        let err = fetch_from_api_server(
            &client,
            ApiSource::Push,
            &url,
            "s",
            "a",
            &HeaderOptions::default(),
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        let FetchError::Api(failure) = err else {
            panic!("expected Api error")
        };
        assert_eq!(failure.kind, ErrorKind::PushFailed);
        assert_eq!(failure.status, Some(400));
    }

    #[tokio::test]
    async fn reserved_param_in_url_errors_before_any_request() {
        let client = reqwest::Client::new();
        let err = fetch_from_api_server(
            &client,
            ApiSource::Push,
            "http://example.invalid/push?schema=x",
            "s",
            "a",
            &HeaderOptions::default(),
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, FetchError::ReservedParam(_)));
    }

    #[tokio::test]
    async fn transform_source_produces_transform_failed_kind() {
        let url = spawn_test_server(500, "{}").await;
        let client = reqwest::Client::new();
        let err = fetch_from_api_server(
            &client,
            ApiSource::Transform,
            &url,
            "s",
            "a",
            &HeaderOptions::default(),
            &serde_json::json!({}),
        )
        .await
        .unwrap_err();
        let FetchError::Api(failure) = err else {
            panic!()
        };
        assert_eq!(failure.kind, ErrorKind::TransformFailed);
    }

    /// Live proof of the retry path: a server that fails with 502 twice
    /// then succeeds. Uses a real counter shared across real connections,
    /// not a mock — proves the retry loop actually re-sends the request.
    /// `start_paused` auto-advances the two real backoff sleeps (~400ms)
    /// the moment the runtime idles, without touching the backoff logic.
    #[tokio::test(start_paused = true)]
    async fn retries_on_502_then_succeeds() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_clone = attempts.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let n = attempts_clone.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    let (status, body) = if n < 2 {
                        (502, "{}")
                    } else {
                        (200, r#"{"ok":true}"#)
                    };
                    let resp = format!(
                        "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        let url = format!("http://{addr}/push");
        let client = reqwest::Client::new();
        let result = fetch_from_api_server(
            &client,
            ApiSource::Push,
            &url,
            "s",
            "a",
            &HeaderOptions::default(),
            &serde_json::json!({}),
        )
        .await
        .unwrap();
        assert_eq!(result, serde_json::json!({"ok": true}));
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "should have retried twice before succeeding"
        );
    }
}
