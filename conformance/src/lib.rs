//! Differential conformance harness for the WebSocket sync protocol.
//!
//! The idea: drive TWO sync servers — this Rust `zero-cache` and the official
//! `rocicorp/zero` — with the *identical* sequence of client frames, capture
//! each server's responses, [`normalize`] away non-deterministic values
//! (connection ids, timestamps, poke ids, cookies…), and assert the two
//! servers produced the *same normalized responses*. Any structural or ordering
//! difference in how the protocol is handled shows up as a diff.
//!
//! The harness is transport-only: it speaks the wire protocol, so it compares
//! observable behavior (what a real client sees) rather than internals. It runs
//! against any two `ws://` endpoints, so it also self-checks (Rust vs Rust) to
//! validate the harness and the server's own determinism.

use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// How long to wait for a frame before deciding "no more output".
const RECV_TIMEOUT: Duration = Duration::from_millis(1500);

/// A connected WebSocket client to one server under test.
pub struct WsClient {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl WsClient {
    /// Connects to `url` (e.g. `ws://127.0.0.1:4848/sync`).
    pub async fn connect(url: &str) -> Result<Self, String> {
        let req = url
            .into_client_request()
            .map_err(|e| format!("bad url {url}: {e}"))?;
        let (ws, _resp) = tokio_tungstenite::connect_async(req)
            .await
            .map_err(|e| format!("connect {url}: {e}"))?;
        Ok(WsClient { ws })
    }

    /// Sends one text frame.
    pub async fn send(&mut self, text: &str) -> Result<(), String> {
        self.ws
            .send(Message::text(text.to_string()))
            .await
            .map_err(|e| format!("send: {e}"))
    }

    /// Receives the next text frame, or `None` if the socket goes quiet for
    /// [`RECV_TIMEOUT`] or closes.
    pub async fn recv(&mut self) -> Option<String> {
        loop {
            match tokio::time::timeout(RECV_TIMEOUT, self.ws.next()).await {
                Err(_) => return None,               // timed out -> quiet
                Ok(None) => return None,             // stream ended
                Ok(Some(Err(_))) => return None,     // protocol/close error
                Ok(Some(Ok(msg))) => match msg {
                    Message::Text(t) => return Some(t.to_string()),
                    Message::Close(_) => return None,
                    // Ignore control/binary frames for protocol comparison.
                    _ => continue,
                },
            }
        }
    }

    /// Drains up to `max` frames (stops early on quiet/close).
    pub async fn drain(&mut self, max: usize) -> Vec<String> {
        let mut out = Vec::new();
        while out.len() < max {
            match self.recv().await {
                Some(f) => out.push(f),
                None => break,
            }
        }
        out
    }
}

/// One step of a [`Scenario`].
#[derive(Debug, Clone)]
pub enum Step {
    /// Send a client frame verbatim.
    Send(String),
    /// Collect up to N response frames (stops early if the server goes quiet).
    Expect(usize),
}

/// A named sequence of protocol interactions to replay against a server.
#[derive(Debug, Clone)]
pub struct Scenario {
    pub name: &'static str,
    pub steps: Vec<Step>,
}

impl Scenario {
    pub fn new(name: &'static str) -> Self {
        Scenario {
            name,
            steps: Vec::new(),
        }
    }
    pub fn send(mut self, frame: &str) -> Self {
        self.steps.push(Step::Send(frame.to_string()));
        self
    }
    pub fn expect(mut self, n: usize) -> Self {
        self.steps.push(Step::Expect(n));
        self
    }
}

/// The captured, normalized responses from running a scenario against a server:
/// one inner `Vec<Value>` per `Expect` step, in order.
pub type Capture = Vec<Vec<Value>>;

/// Runs `scenario` against the server at `url`, returning its normalized
/// responses. Each `Send` writes a frame; each `Expect(n)` collects and
/// normalizes up to `n` response frames.
pub async fn run(url: &str, scenario: &Scenario) -> Result<Capture, String> {
    let mut client = WsClient::connect(url).await?;
    let mut capture = Capture::new();
    for step in &scenario.steps {
        match step {
            Step::Send(frame) => client.send(frame).await?,
            Step::Expect(n) => {
                let frames = client.drain(*n).await;
                capture.push(frames.iter().map(|f| normalize_frame(f)).collect());
            }
        }
    }
    Ok(capture)
}

/// Compares two captures and returns a human-readable diff, or `None` if they
/// are equivalent.
pub fn diff(a: &Capture, b: &Capture, a_label: &str, b_label: &str) -> Option<String> {
    if a == b {
        return None;
    }
    let mut out = String::new();
    let steps = a.len().max(b.len());
    for i in 0..steps {
        let av = a.get(i);
        let bv = b.get(i);
        if av != bv {
            out.push_str(&format!("  step {i}:\n"));
            out.push_str(&format!(
                "    {a_label}: {}\n",
                av.map(|f| render_frames(f)).unwrap_or_else(|| "<missing>".into())
            ));
            out.push_str(&format!(
                "    {b_label}: {}\n",
                bv.map(|f| render_frames(f)).unwrap_or_else(|| "<missing>".into())
            ));
        }
    }
    Some(out)
}

fn render_frames(frames: &[Value]) -> String {
    let parts: Vec<String> = frames.iter().map(|v| v.to_string()).collect();
    format!("[{}]", parts.join(", "))
}

/// Keys whose values are server-generated / non-deterministic and must be
/// masked before comparison (a real client never depends on their exact value).
const VOLATILE_KEYS: &[&str] = &[
    "wsid",
    "timestamp",
    "ts",
    "pokeID",
    "requestID",
    "baseCookie",
    "cookie",
    "lastMutationID",
    "grantedAt",
    "lastActive",
];

/// Parses a frame as JSON and normalizes it: volatile values are replaced with
/// a stable type-tagged placeholder (e.g. `"<number>"`), recursively. A frame
/// that isn't valid JSON is wrapped as `["<non-json>", "<text>"]` so servers
/// that emit non-JSON errors still compare structurally.
pub fn normalize_frame(frame: &str) -> Value {
    match serde_json::from_str::<Value>(frame) {
        Ok(v) => normalize_value(&v),
        Err(_) => Value::String("<non-json>".to_string()),
    }
}

fn normalize_value(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = BTreeMap::new();
            for (k, val) in map {
                if VOLATILE_KEYS.contains(&k.as_str()) {
                    out.insert(k.clone(), Value::String(placeholder(val)));
                } else {
                    out.insert(k.clone(), normalize_value(val));
                }
            }
            Value::Object(out.into_iter().collect())
        }
        Value::Array(items) => Value::Array(items.iter().map(normalize_value).collect()),
        other => other.clone(),
    }
}

fn placeholder(v: &Value) -> String {
    match v {
        Value::Null => "<null>".into(),
        Value::Bool(_) => "<bool>".into(),
        Value::Number(_) => "<number>".into(),
        Value::String(_) => "<string>".into(),
        Value::Array(_) => "<array>".into(),
        Value::Object(_) => "<object>".into(),
    }
}

/// The standard scenario battery — the protocol behaviors that are observable
/// without a seeded schema/data set, so they can be compared across servers.
pub fn scenarios() -> Vec<Scenario> {
    vec![
        // The greeting frame every server sends on connect.
        Scenario::new("handshake").expect(1),
        // A keepalive ping is answered with a pong; the greeting comes first.
        Scenario::new("ping_pong")
            .expect(1)
            .send(r#"["ping",{}]"#)
            .expect(1),
        // initConnection with an empty desired-query set, then a ping.
        Scenario::new("init_then_ping")
            .expect(1)
            .send(r#"["initConnection",{"desiredQueriesPatch":[]}]"#)
            .send(r#"["ping",{}]"#)
            .expect(2),
        // A data message before initConnection is a protocol-ordering violation.
        Scenario::new("data_before_init")
            .expect(1)
            .send(r#"["changeDesiredQueries",{"desiredQueriesPatch":[]}]"#)
            .expect(2),
        // A malformed (non-JSON) frame.
        Scenario::new("malformed_frame")
            .expect(1)
            .send("this is not json")
            .expect(1),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalize_masks_volatile_values_but_keeps_structure() {
        let n = normalize_frame(r#"["connected",{"wsid":"ws7","timestamp":12345}]"#);
        assert_eq!(
            n,
            json!(["connected", {"wsid": "<string>", "timestamp": "<number>"}])
        );
    }

    #[test]
    fn normalize_is_stable_for_pong() {
        assert_eq!(normalize_frame(r#"["pong",{}]"#), json!(["pong", {}]));
    }

    #[test]
    fn normalize_masks_nested_poke_ids_and_cookies() {
        let n = normalize_frame(
            r#"["pokeStart",{"pokeID":"poke-02","baseCookie":"01","schemaVersions":{"minSupportedVersion":1}}]"#,
        );
        assert_eq!(
            n,
            json!(["pokeStart", {
                "pokeID": "<string>",
                "baseCookie": "<string>",
                "schemaVersions": {"minSupportedVersion": 1}
            }])
        );
    }

    #[test]
    fn non_json_frames_normalize_to_a_stable_marker() {
        assert_eq!(normalize_frame("boom"), json!("<non-json>"));
    }

    #[test]
    fn diff_is_none_for_equal_captures_and_some_otherwise() {
        let a: Capture = vec![vec![json!(["pong", {}])]];
        let b: Capture = vec![vec![json!(["pong", {}])]];
        assert!(diff(&a, &b, "rust", "ref").is_none());

        let c: Capture = vec![vec![json!(["error", {}])]];
        assert!(diff(&a, &c, "rust", "ref").is_some());
    }

    #[test]
    fn scenario_builder_records_steps_in_order() {
        let s = Scenario::new("x").expect(1).send(r#"["ping",{}]"#).expect(1);
        assert_eq!(s.steps.len(), 3);
        assert!(matches!(s.steps[0], Step::Expect(1)));
        assert!(matches!(s.steps[1], Step::Send(_)));
        assert!(matches!(s.steps[2], Step::Expect(1)));
    }
}
