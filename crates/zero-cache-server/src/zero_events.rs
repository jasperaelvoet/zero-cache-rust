//! ZeroEvents → CloudEvents publishing (`ZERO_CLOUD_EVENT_SINK_ENV` /
//! `ZERO_CLOUD_EVENT_EXTENSION_OVERRIDES_ENV`).
//!
//! Port of `src/observability/events.ts`: the two options hold the NAMES of
//! other env vars (modeled on knative's `K_SINK` / `K_CE_OVERRIDES` bindings).
//! When a sink is configured, lifecycle ZeroEvents are POSTed to it as
//! structured-mode CloudEvents whose `data` is the gzip+base64 of the event
//! JSON (upstream sets `datacontenttype: text/plain`,
//! `datacontentencoding: gzip`). Without a sink, events are logged at info.
//!
//! Delivery matches upstream: up to [`MAX_PUBLISH_ATTEMPTS`] attempts with
//! exponential backoff starting at 500ms; `publish` is fire-and-forget,
//! `publish_critical` awaits the outcome.

use std::io::Write;
use std::sync::{Arc, OnceLock};

use base64::Engine;

/// Upstream `MAX_PUBLISH_ATTEMPTS`.
const MAX_PUBLISH_ATTEMPTS: u32 = 6;
/// Upstream initial retry backoff.
const INITIAL_BACKOFF_MS: u64 = 500;

/// The replication status event type emitted at replication lifecycle points.
pub const REPLICATION_STATUS_V1: &str = "zero/events/status/replication/v1";

struct Sink {
    uri: String,
    /// Extension attributes merged onto every outbound CloudEvent.
    extensions: Vec<(String, serde_json::Value)>,
    /// CloudEvents `source` — the task ID, as upstream.
    source: String,
    client: reqwest::Client,
}

static SINK: OnceLock<Option<Arc<Sink>>> = OnceLock::new();

/// Errors initializing the event sink from the env indirection.
#[derive(Debug, thiserror::Error)]
pub enum EventSinkError {
    #[error("ZERO_CLOUD_EVENT_SINK_ENV names env var {0:?}, which is not set")]
    MissingSinkVar(String),
    #[error("env var {0:?} (ZERO_CLOUD_EVENT_EXTENSION_OVERRIDES_ENV) is not valid JSON: {1}")]
    InvalidOverrides(String, String),
}

/// Initializes the process-wide event sink. `sink_env` / `overrides_env` are
/// the option values (names of other env vars); `source` is the task ID.
/// Matches upstream `initEventSink`: a configured-but-missing sink var is a
/// startup error; no sink var configured means log-only publishing.
pub fn init(
    sink_env: Option<&str>,
    overrides_env: Option<&str>,
    source: &str,
) -> Result<(), EventSinkError> {
    let sink = match sink_env {
        None => None,
        Some(var_name) => {
            let uri = std::env::var(var_name)
                .ok()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| EventSinkError::MissingSinkVar(var_name.to_string()))?;
            let extensions = match overrides_env.and_then(|name| {
                std::env::var(name)
                    .ok()
                    .filter(|s| !s.is_empty())
                    .map(|json| (name.to_string(), json))
            }) {
                None => Vec::new(),
                Some((name, json)) => {
                    let parsed: serde_json::Value = serde_json::from_str(&json).map_err(|e| {
                        EventSinkError::InvalidOverrides(name.clone(), e.to_string())
                    })?;
                    parsed
                        .get("extensions")
                        .and_then(|v| v.as_object())
                        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                        .unwrap_or_default()
                }
            };
            Some(Arc::new(Sink {
                uri,
                extensions,
                source: source.to_string(),
                client: reqwest::Client::new(),
            }))
        }
    };
    let _ = SINK.set(sink);
    Ok(())
}

/// Builds the structured-mode CloudEvent JSON for `event_type` + `data_json`.
/// Pure, for testability: `id` and `time` are injected.
fn cloud_event_json(
    event_type: &str,
    data_json: &str,
    source: &str,
    extensions: &[(String, serde_json::Value)],
    id: &str,
    time_rfc3339: &str,
) -> serde_json::Value {
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(data_json.as_bytes()).ok();
    let gzipped = gz.finish().unwrap_or_default();
    let data_b64 = base64::engine::general_purpose::STANDARD.encode(gzipped);

    let mut obj = serde_json::json!({
        "specversion": "1.0",
        "id": id,
        "source": source,
        "type": event_type,
        "time": time_rfc3339,
        "datacontenttype": "text/plain",
        "datacontentencoding": "gzip",
        "data": data_b64,
    });
    if let Some(map) = obj.as_object_mut() {
        for (k, v) in extensions {
            map.insert(k.clone(), v.clone());
        }
    }
    obj
}

async fn deliver(sink: &Sink, body: serde_json::Value, event_type: &str) -> bool {
    let mut backoff = std::time::Duration::from_millis(INITIAL_BACKOFF_MS);
    for attempt in 1..=MAX_PUBLISH_ATTEMPTS {
        let result = sink
            .client
            .post(&sink.uri)
            .header("content-type", "application/cloudevents+json")
            .json(&body)
            .send()
            .await;
        match result {
            Ok(resp) if resp.status().is_success() => return true,
            Ok(resp) => crate::warn!(
                "ZeroEvent {event_type} publish attempt {attempt} failed: HTTP {}",
                resp.status()
            ),
            Err(e) => crate::warn!("ZeroEvent {event_type} publish attempt {attempt} failed: {e}"),
        }
        if attempt < MAX_PUBLISH_ATTEMPTS {
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
    }
    false
}

fn make_event(sink: &Sink, event_type: &str, data_json: &str) -> serde_json::Value {
    let id = format!(
        "{:x}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    );
    let now = std::time::SystemTime::now();
    let time = humantime_rfc3339(now);
    cloud_event_json(
        event_type,
        data_json,
        &sink.source,
        &sink.extensions,
        &id,
        &time,
    )
}

/// RFC 3339 UTC timestamp without external deps.
fn humantime_rfc3339(t: std::time::SystemTime) -> String {
    let secs = t
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default() as i64;
    // Civil-from-days (Howard Hinnant's algorithm) — UTC only.
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Publishes a ZeroEvent (fire-and-forget). Without a configured sink, the
/// event is logged at info, matching upstream's fallback publisher. Safe to
/// call from any tokio runtime; a no-op before [`init`].
pub fn publish(event_type: &'static str, data_json: String) {
    match SINK.get() {
        Some(Some(sink)) => {
            let sink = sink.clone();
            let body = make_event(&sink, event_type, &data_json);
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    deliver(&sink, body, event_type).await;
                });
            }
        }
        _ => crate::info!("ZeroEvent: {event_type} {data_json}"),
    }
}

/// Publishes a ZeroEvent and awaits delivery (upstream `publishCriticalEvent`).
pub async fn publish_critical(event_type: &str, data_json: String) {
    match SINK.get() {
        Some(Some(sink)) => {
            let body = make_event(sink, event_type, &data_json);
            deliver(sink, body, event_type).await;
        }
        _ => crate::info!("ZeroEvent: {event_type} {data_json}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn cloud_event_shape_matches_upstream_binding() {
        let ev = cloud_event_json(
            REPLICATION_STATUS_V1,
            r#"{"status":"OK"}"#,
            "task-123",
            &[("env".into(), serde_json::json!("staging"))],
            "id-1",
            "2026-07-12T00:00:00Z",
        );
        assert_eq!(ev["specversion"], "1.0");
        assert_eq!(ev["source"], "task-123");
        assert_eq!(ev["type"], REPLICATION_STATUS_V1);
        assert_eq!(ev["datacontenttype"], "text/plain");
        assert_eq!(ev["datacontentencoding"], "gzip");
        // Extension overrides are merged as top-level attributes.
        assert_eq!(ev["env"], "staging");

        // data round-trips: base64 -> gunzip -> original JSON.
        let b64 = ev["data"].as_str().unwrap();
        let gz = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        let mut out = String::new();
        flate2::read::GzDecoder::new(&gz[..])
            .read_to_string(&mut out)
            .unwrap();
        assert_eq!(out, r#"{"status":"OK"}"#);
    }

    #[test]
    fn rfc3339_formatting_is_correct() {
        use std::time::{Duration, UNIX_EPOCH};
        assert_eq!(humantime_rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        // 2026-07-12T15:19:26Z = 1783696766 (spot-checked epoch value).
        let t = UNIX_EPOCH + Duration::from_secs(1_752_333_566);
        let s = humantime_rfc3339(t);
        assert!(s.starts_with("2025-07-12T"), "{s}");
    }

    #[test]
    fn init_requires_the_named_sink_var() {
        // Names an env var that is not set -> startup error (upstream `must`).
        let err = init(Some("ZC_TEST_MISSING_SINK_VAR"), None, "t").unwrap_err();
        assert!(matches!(err, EventSinkError::MissingSinkVar(_)));
    }
}
