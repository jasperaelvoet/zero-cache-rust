//! Black-box wire conformance checks against a pinned official Zero server.
//!
//! This deliberately speaks the WebSocket protocol directly instead of calling
//! Rust internals.  A passing unit suite cannot establish compatibility when a
//! client sees different initialization, hydration, reconnect, or live-row
//! behavior on the wire. The test records the same scenarios against two independently
//! seeded targets, normalizes only server-generated opaque identifiers, and
//! compares the resulting transcripts exactly.
//!
//! Run this only against the checked-in pinned Compose stack:
//!
//! ```text
//! KEEP_UP=1 scripts/bench.sh 1 1
//! ZERO_CONFORMANCE_RUST_URL=ws://127.0.0.1:4848 \
//! ZERO_CONFORMANCE_REFERENCE_URL=ws://127.0.0.1:4849 \
//! ZERO_CONFORMANCE_RUST_PG_URL=postgresql://postgres:postgres@127.0.0.1:5432/zero \
//! ZERO_CONFORMANCE_REFERENCE_PG_URL=postgresql://postgres:postgres@127.0.0.1:5433/zero \
//!   cargo test -p zero-cache-server --test reference_conformance -- \
//!   --ignored --nocapture
//! ```
//!
//! The official reference is `zero/v1.7.0` (commit `6863de5`, protocol v51),
//! pinned to an OCI manifest digest in `bench/docker-compose.bench.yml`.

use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use zero_cache_protocol::connect::encode_sec_protocols;

const PROTOCOL_VERSION: u32 = 51;
const FRAME_TIMEOUT: Duration = Duration::from_secs(12);

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone, Debug)]
struct Target {
    label: &'static str,
    origin: String,
    db_url: String,
}

impl Target {
    fn from_env(label: &'static str, variable: &str, db_variable: &str) -> Result<Self, String> {
        let origin = std::env::var(variable).map_err(|_| {
            format!(
                "{variable} is required; start the pinned benchmark stack and set both conformance URLs"
            )
        })?;
        let db_url = std::env::var(db_variable)
            .map_err(|_| format!("{db_variable} is required for live replication scenarios"))?;
        Ok(Self {
            label,
            origin: base_origin(&origin),
            db_url,
        })
    }
}

/// A direct WebSocket client plus the frames it sent and received.  Keeping the
/// sent frames in the artifact makes a future failing trace replayable.
struct Session {
    socket: Socket,
    sent: Vec<Value>,
    received: Vec<Value>,
    base_cookie: Option<String>,
}

impl Session {
    async fn open(
        target: &Target,
        group_id: &str,
        client_id: &str,
        wsid: &str,
        base_cookie: Option<&str>,
    ) -> Result<Self, String> {
        let url = connect_url(&target.origin, group_id, client_id, wsid, base_cookie);
        let mut request = url
            .clone()
            .into_client_request()
            .map_err(|error| format!("{}: invalid URL {url:?}: {error}", target.label))?;
        let protocol = encode_sec_protocols(None, None);
        request.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            protocol.parse().map_err(|error| {
                format!(
                    "{}: invalid Sec-WebSocket-Protocol header: {error}",
                    target.label
                )
            })?,
        );

        let (socket, _) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|error| format!("{}: websocket connect: {error}", target.label))?;
        let mut session = Self {
            socket,
            sent: Vec::new(),
            received: Vec::new(),
            base_cookie: base_cookie.map(str::to_owned),
        };

        let greeting = session.next_text().await?;
        if message_tag(&greeting) != Some("connected") {
            return Err(format!(
                "{}: expected connected greeting, got {}",
                target.label,
                compact_json(&greeting)
            ));
        }
        session.received.push(greeting);
        Ok(session)
    }

    async fn send(&mut self, message: Value) -> Result<(), String> {
        self.socket
            .send(Message::text(message.to_string()))
            .await
            .map_err(|error| format!("websocket send: {error}"))?;
        self.sent.push(message);
        Ok(())
    }

    /// Receives text frames until a frame tagged `tag` arrives. Interleaved
    /// poke frames are deliberately retained: ordering is part of the wire
    /// contract.
    async fn receive_through(&mut self, tag: &str) -> Result<(), String> {
        let deadline = Instant::now() + FRAME_TIMEOUT;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| {
                    format!(
                        "timed out waiting for {tag}; frames: {}",
                        self.frames_json()
                    )
                })?;
            let message = self.next_text_with_timeout(remaining).await?;
            let matches_tag = message_tag(&message) == Some(tag);
            self.received.push(message);
            if matches_tag {
                return Ok(());
            }
        }
    }

    async fn receive_hydration_and_pong(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + FRAME_TIMEOUT;
        let mut saw_rows = false;
        let mut saw_pong = false;
        let mut saw_end_after_rows = false;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| {
                    format!(
                        "timed out waiting for hydration+pong; frames: {}",
                        self.frames_json()
                    )
                })?;
            let message = self.next_text_with_timeout(remaining).await?;
            match message_tag(&message) {
                Some("pong") => saw_pong = true,
                Some("pokePart") => {
                    saw_rows |= message
                        .get(1)
                        .and_then(|body| body.get("rowsPatch"))
                        .is_some();
                }
                Some("pokeEnd") if saw_rows => saw_end_after_rows = true,
                _ => {}
            }
            self.received.push(message);
            if saw_rows && saw_pong && saw_end_after_rows {
                return Ok(());
            }
        }
    }

    /// Close frames are best-effort: the prior transcript is what matters, and
    /// a server that already closed after its final poke is valid here.
    async fn close(&mut self) {
        let _ = self.socket.send(Message::Close(None)).await;
    }

    fn latest_cookie(&self) -> Option<String> {
        self.received.iter().rev().find_map(|message| {
            if message_tag(message) == Some("pokeEnd") {
                message
                    .get(1)
                    .and_then(|body| body.get("cookie"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            } else {
                None
            }
        })
    }

    fn trace(&self) -> Value {
        json!({
            "connect": {"baseCookie": self.base_cookie},
            "sent": self.sent,
            "received": self.received,
        })
    }

    fn frames_json(&self) -> String {
        Value::Array(self.received.clone()).to_string()
    }

    async fn next_text(&mut self) -> Result<Value, String> {
        self.next_text_with_timeout(FRAME_TIMEOUT).await
    }

    async fn next_text_with_timeout(&mut self, timeout: Duration) -> Result<Value, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| "timed out waiting for a text frame".to_string())?;
            let next = tokio::time::timeout(remaining, self.socket.next())
                .await
                .map_err(|_| "timed out waiting for a text frame".to_string())?;
            match next {
                Some(Ok(Message::Text(text))) => {
                    return serde_json::from_str(&text)
                        .map_err(|error| format!("server sent non-JSON text {text:?}: {error}"));
                }
                Some(Ok(Message::Close(frame))) => {
                    return Err(format!("server closed websocket: {frame:?}"));
                }
                Some(Ok(_)) => continue,
                Some(Err(error)) => return Err(format!("websocket receive: {error}")),
                None => return Err("server closed websocket without a frame".to_string()),
            }
        }
    }
}

/// Connect once, initialize an empty client group (including the mandatory
/// first-group client schema), then prove the connection remains usable.
async fn init_scenario(target: &Target, scope: &str) -> Result<Value, String> {
    let group = format!("conformance-init-{scope}");
    let client = format!("conformance-init-client-{scope}");
    let mut session = Session::open(target, &group, &client, "init", None).await?;
    session
        .send(init_message(Value::Array(Vec::new()), true))
        .await?;
    session.send(json!(["ping", {}])).await?;
    session.receive_through("pong").await?;
    session.close().await;
    Ok(json!({"scenario": "init", "connections": [session.trace()]}))
}

/// A new group asks for a real table query. This must produce a completed poke
/// (and therefore catches failures hidden by greeting/ping-only tests).
async fn query_scenario(target: &Target, scope: &str) -> Result<Value, String> {
    let group = format!("conformance-query-{scope}");
    let client = format!("conformance-query-client-{scope}");
    let mut session = Session::open(target, &group, &client, "query", None).await?;
    session
        .send(init_message(default_query_patch(), true))
        .await?;
    session
        .receive_through("pokeEnd")
        .await
        .map_err(|error| format!("initial desired poke: {error}"))?;
    session.send(json!(["ping", {}])).await?;
    session
        .receive_hydration_and_pong()
        .await
        .map_err(|error| format!("initial hydration: {error}"))?;
    session.close().await;
    Ok(json!({"scenario": "query", "connections": [session.trace()]}))
}

/// Hydrate a fresh group, take the opaque cookie emitted by the server, and
/// reconnect with it. This verifies both cookie continuity and the second
/// connection's init/ping sequence without assuming a particular cookie
/// string or server-generated poke ID.
async fn reconnect_scenario(target: &Target, scope: &str) -> Result<Value, String> {
    let group = format!("conformance-reconnect-{scope}");
    let client = format!("conformance-reconnect-client-{scope}");

    let mut first = Session::open(target, &group, &client, "reconnect-first", None).await?;
    first
        .send(init_message(default_query_patch(), true))
        .await?;
    first.receive_through("pokeEnd").await?;
    first.send(json!(["ping", {}])).await?;
    first.receive_hydration_and_pong().await?;
    let cookie = first.latest_cookie().ok_or_else(|| {
        format!(
            "{}: hydration completed without a cookie: {}",
            target.label,
            first.frames_json()
        )
    })?;
    first.close().await;

    let mut second =
        Session::open(target, &group, &client, "reconnect-second", Some(&cookie)).await?;
    // A reconnect relies on the CVR state identified by its base cookie, so it
    // does not declare clientSchema again. This distinction is protocol-visible.
    second
        .send(init_message(Value::Array(Vec::new()), false))
        .await?;
    second.send(json!(["ping", {}])).await?;
    second.receive_through("pong").await?;
    second.close().await;

    Ok(json!({
        "scenario": "reconnect",
        "connections": [first.trace(), second.trace()],
    }))
}

async fn execute_sql(target: &Target, sql: &str) -> Result<(), String> {
    let (client, connection) = tokio_postgres::connect(&target.db_url, tokio_postgres::NoTls)
        .await
        .map_err(|error| format!("{}: postgres connect: {error}", target.label))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(sql)
        .await
        .map_err(|error| format!("{}: postgres execute: {error}", target.label))
}

/// Subscribe to one row, then update and delete it in the authoritative
/// Postgres. Both servers must push the same row replacement and delete, with
/// the same cookie/frame sequencing.
async fn live_update_delete_scenario(target: &Target, scope: &str) -> Result<Value, String> {
    let row_id = format!("conformance-live-{scope}");
    execute_sql(
        target,
        &format!(
            "INSERT INTO issue (id,title,owner,open,rank) VALUES ('{}','before','conformance',true,2001)",
            row_id.replace('\'', "''")
        ),
    )
    .await?;

    let group = format!("conformance-live-{scope}");
    let client = format!("conformance-live-client-{scope}");
    let mut session = Session::open(target, &group, &client, "live", None).await?;
    session
        .send(init_message(filtered_query_patch(&row_id), true))
        .await?;
    session
        .receive_through("pokeEnd")
        .await
        .map_err(|error| format!("initial desired poke: {error}"))?;
    session.send(json!(["ping", {}])).await?;
    session
        .receive_hydration_and_pong()
        .await
        .map_err(|error| format!("initial hydration: {error}"))?;

    execute_sql(
        target,
        &format!(
            "UPDATE issue SET title='after', rank=2002 WHERE id='{}'",
            row_id.replace('\'', "''")
        ),
    )
    .await?;
    session
        .receive_through("pokeEnd")
        .await
        .map_err(|error| format!("update poke: {error}"))?;

    execute_sql(
        target,
        &format!(
            "DELETE FROM issue WHERE id='{}'",
            row_id.replace('\'', "''")
        ),
    )
    .await?;
    session
        .receive_through("pokeEnd")
        .await
        .map_err(|error| format!("delete poke: {error}"))?;
    session.close().await;
    Ok(json!({"scenario": "live-update-delete", "connections": [session.trace()]}))
}

async fn run_scenario(target: &Target, name: &str, scope: &str) -> Result<Value, String> {
    match name {
        "init" => init_scenario(target, scope).await,
        "query" => query_scenario(target, scope).await,
        "reconnect" => reconnect_scenario(target, scope).await,
        "live-update-delete" => live_update_delete_scenario(target, scope).await,
        _ => Err(format!("unknown conformance scenario {name}")),
    }
}

fn init_message(desired_queries_patch: Value, include_schema: bool) -> Value {
    let mut body = Map::new();
    body.insert("desiredQueriesPatch".to_string(), desired_queries_patch);
    if include_schema {
        body.insert("clientSchema".to_string(), client_schema());
    }
    Value::Array(vec![
        Value::String("initConnection".to_string()),
        Value::Object(body),
    ])
}

/// Matches the deterministic `issue` schema from `bench/seed.sql` and the
/// pinned reference stack. The initial initConnection for every new group must
/// carry this field; omitting it is deliberately not a valid test shortcut.
fn client_schema() -> Value {
    json!({
        "tables": {
            "issue": {
                "columns": {
                    "id": {"type": "string"},
                    "title": {"type": "string"},
                    "owner": {"type": "string"},
                    "open": {"type": "boolean"},
                    "rank": {"type": "number"}
                },
                "primaryKey": ["id"]
            }
        }
    })
}

fn default_query_patch() -> Value {
    json!([{
        "op": "put",
        "hash": "conformance-issue-all",
        "ast": {"table": "issue"}
    }])
}

fn filtered_query_patch(id: &str) -> Value {
    json!([{
        "op": "put",
        "hash": format!("conformance-issue-{id}"),
        "ast": {
            "table": "issue",
            "where": {
                "type": "simple",
                "op": "=",
                "left": {"type": "column", "name": "id"},
                "right": {"type": "literal", "value": id}
            }
        }
    }])
}

fn base_origin(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    trimmed
        .find("/sync")
        .map(|position| trimmed[..position].to_string())
        .unwrap_or_else(|| trimmed.to_string())
}

fn connect_url(
    origin: &str,
    group_id: &str,
    client_id: &str,
    wsid: &str,
    base_cookie: Option<&str>,
) -> String {
    format!(
        "{origin}/sync/v{PROTOCOL_VERSION}/connect?clientGroupID={}&clientID={}&wsid={}&schemaVersion=1&baseCookie={}&ts=0&lmid=0",
        query_component(group_id),
        query_component(client_id),
        query_component(wsid),
        query_component(base_cookie.unwrap_or("")),
    )
}

fn query_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn message_tag(value: &Value) -> Option<&str> {
    value
        .as_array()
        .and_then(|items| items.first())
        .and_then(Value::as_str)
}

/// Replace only values that servers are allowed to generate differently.
/// Cookie and poke-ID values use a stable per-trace token map, preserving the
/// important relationship that a later reconnect's `baseCookie` refers to an
/// earlier `pokeEnd.cookie`.
fn normalize_transcript(value: &Value) -> Value {
    let mut opaque_values = BTreeMap::<(String, String), String>::new();
    let mut counters = BTreeMap::<String, usize>::new();
    normalize_value(value, None, &mut opaque_values, &mut counters)
}

fn normalize_value(
    value: &Value,
    field_name: Option<&str>,
    opaque_values: &mut BTreeMap<(String, String), String>,
    counters: &mut BTreeMap<String, usize>,
) -> Value {
    match value {
        Value::Array(items) => {
            let canonical = canonicalize_pong_interleaving(items);
            Value::Array(
                canonical
                    .iter()
                    .map(|item| normalize_value(item, None, opaque_values, counters))
                    .collect(),
            )
        }
        Value::Object(object) => {
            let mut normalized = Map::new();
            for (key, item) in object {
                normalized.insert(
                    key.clone(),
                    normalize_value(item, Some(key), opaque_values, counters),
                );
            }
            Value::Object(normalized)
        }
        Value::String(raw) if matches!(field_name, Some("pokeID" | "cookie" | "baseCookie")) => {
            // Cookies appear under both `cookie` and `baseCookie`; share their
            // namespace so equality across the reconnect survives normalization.
            let namespace = if matches!(field_name, Some("cookie" | "baseCookie")) {
                "cookie"
            } else {
                "pokeID"
            };
            let map_key = (namespace.to_string(), raw.clone());
            let next = opaque_values.entry(map_key).or_insert_with(|| {
                let count = counters.entry(namespace.to_string()).or_insert(0);
                *count += 1;
                format!("<{namespace}:{}>", *count)
            });
            Value::String(next.clone())
        }
        Value::String(_) if field_name == Some("wsid") => Value::String("<wsid>".to_string()),
        Value::Number(_) if field_name == Some("timestamp") => {
            Value::String("<timestamp>".to_string())
        }
        _ => value.clone(),
    }
}

/// Ping replies are independent of poke transactions and may race with an
/// already-started hydration. Canonicalize a pong received between pokeStart
/// and pokeEnd to immediately after that pokeEnd; all state-changing frame
/// ordering remains exact.
fn canonicalize_pong_interleaving(items: &[Value]) -> Vec<Value> {
    let is_frame_list = !items.is_empty()
        && items.iter().all(|item| {
            item.as_array()
                .and_then(|frame| frame.first())
                .and_then(Value::as_str)
                .is_some()
        });
    if !is_frame_list {
        return items.to_vec();
    }
    let mut out = Vec::with_capacity(items.len());
    let mut poke_open = false;
    let mut deferred_pongs = Vec::new();
    for item in items {
        match message_tag(item) {
            Some("pokeStart") => {
                poke_open = true;
                out.push(item.clone());
            }
            Some("pong") if poke_open => deferred_pongs.push(item.clone()),
            Some("pokeEnd") if poke_open => {
                out.push(item.clone());
                out.append(&mut deferred_pongs);
                poke_open = false;
            }
            _ => out.push(item.clone()),
        }
    }
    out.append(&mut deferred_pongs);
    out
}

fn compact_json(value: &Value) -> String {
    let rendered = value.to_string();
    const LIMIT: usize = 1_200;
    let mut chars = rendered.chars();
    let compact: String = chars.by_ref().take(LIMIT).collect();
    if chars.next().is_some() {
        format!("{compact}…")
    } else {
        rendered
    }
}

fn first_difference(left: &Value, right: &Value, path: &str) -> Option<String> {
    match (left, right) {
        (Value::Array(left), Value::Array(right)) => {
            if left.len() != right.len() {
                return Some(format!(
                    "{path}: array lengths {} != {}",
                    left.len(),
                    right.len()
                ));
            }
            left.iter()
                .zip(right)
                .enumerate()
                .find_map(|(index, (l, r))| first_difference(l, r, &format!("{path}[{index}]")))
        }
        (Value::Object(left), Value::Object(right)) => {
            let left_keys: Vec<_> = left.keys().collect();
            let right_keys: Vec<_> = right.keys().collect();
            if left_keys != right_keys {
                return Some(format!(
                    "{path}: object keys {left_keys:?} != {right_keys:?}"
                ));
            }
            left.iter()
                .find_map(|(key, l)| first_difference(l, &right[key], &format!("{path}.{key}")))
        }
        _ if left != right => Some(format!("{path}: {left} != {right}")),
        _ => None,
    }
}

fn unique_scope() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{}-{nanos}", std::process::id())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires independently seeded Rust and pinned-reference servers; see this file's module docs"]
async fn pinned_reference_transcripts_match_rust() {
    let rust = Target::from_env(
        "rust",
        "ZERO_CONFORMANCE_RUST_URL",
        "ZERO_CONFORMANCE_RUST_PG_URL",
    )
    .unwrap();
    let reference = Target::from_env(
        "reference",
        "ZERO_CONFORMANCE_REFERENCE_URL",
        "ZERO_CONFORMANCE_REFERENCE_PG_URL",
    )
    .unwrap();
    let scope = unique_scope();

    let scenarios = std::env::var("ZERO_CONFORMANCE_SCENARIOS")
        .unwrap_or_else(|_| "init,query,reconnect,live-update-delete".to_string());
    for scenario in scenarios.split(',').filter(|scenario| !scenario.is_empty()) {
        let reference_trace = run_scenario(&reference, scenario, &scope)
            .await
            .unwrap_or_else(|error| panic!("reference {scenario} scenario failed: {error}"));
        let rust_trace = run_scenario(&rust, scenario, &scope)
            .await
            .unwrap_or_else(|error| panic!("rust {scenario} scenario failed: {error}"));

        let normalized_reference = normalize_transcript(&reference_trace);
        let normalized_rust = normalize_transcript(&rust_trace);
        assert_eq!(
            normalized_rust,
            normalized_reference,
            "{scenario} transcript differs from pinned official Zero\n\
             first difference: {}\n\
             reference raw: {}\n\
             rust raw: {}\n\
             reference normalized: {}\n\
             rust normalized: {}",
            first_difference(&normalized_rust, &normalized_reference, "$")
                .unwrap_or_else(|| "<none>".to_string()),
            compact_json(&reference_trace),
            compact_json(&rust_trace),
            compact_json(&normalized_reference),
            compact_json(&normalized_rust),
        );
    }
}

#[test]
fn normalization_preserves_cookie_continuity_but_hides_opaque_values() {
    let trace = json!({
        "connections": [
            {"received": [["connected", {"wsid": "a", "timestamp": 1}], ["pokeEnd", {"pokeID": "p-a", "cookie": "cookie-a"}]]},
            {"connect": {"baseCookie": "cookie-a"}, "received": [["connected", {"wsid": "b", "timestamp": 2}], ["pokeEnd", {"pokeID": "p-b", "cookie": "cookie-b"}]]}
        ]
    });
    let normalized = normalize_transcript(&trace);
    assert_eq!(
        normalized["connections"][0]["received"][1][1]["cookie"],
        normalized["connections"][1]["connect"]["baseCookie"],
        "the reconnect must still refer to the first connection's cookie"
    );
    assert_eq!(
        normalized["connections"][0]["received"][0][1]["wsid"],
        "<wsid>"
    );
    assert_eq!(
        normalized["connections"][0]["received"][0][1]["timestamp"],
        "<timestamp>"
    );
}

#[test]
fn normalization_canonicalizes_only_pong_inside_an_open_poke() {
    let interleaved = json!([
        ["pokeStart", {"pokeID":"p","baseCookie":null}],
        ["pong", {}],
        ["pokePart", {"pokeID":"p","rowsPatch":[]}],
        ["pokeEnd", {"pokeID":"p","cookie":"c"}]
    ]);
    let completed_first = json!([
        ["pokeStart", {"pokeID":"p","baseCookie":null}],
        ["pokePart", {"pokeID":"p","rowsPatch":[]}],
        ["pokeEnd", {"pokeID":"p","cookie":"c"}],
        ["pong", {}]
    ]);
    assert_eq!(
        normalize_transcript(&interleaved),
        normalize_transcript(&completed_first)
    );
}

#[test]
fn connect_url_uses_real_protocol_path_and_escapes_cookie() {
    let url = connect_url(
        "ws://localhost:4848",
        "group one",
        "client",
        "ws",
        Some("00:01/with space"),
    );
    assert!(url.starts_with("ws://localhost:4848/sync/v51/connect?"));
    assert!(url.contains("clientGroupID=group%20one"));
    assert!(url.contains("baseCookie=00%3A01%2Fwith%20space"));
}
