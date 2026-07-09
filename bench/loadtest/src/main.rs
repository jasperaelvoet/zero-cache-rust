//! zero-loadtest — drives many concurrent WebSocket clients against a
//! zero-cache server and reports latency / throughput / resource usage. Can run
//! against two targets (`--compare`) for a head-to-head vs `rocicorp/zero`.
//!
//! Usage (env or flags; flags win):
//!   --url ws://127.0.0.1:4848/sync   (LOAD_URL)      target server
//!   --clients 1000                   (LOAD_CLIENTS)  concurrent clients
//!   --duration 20                    (LOAD_DURATION) seconds of sustained load
//!   --ramp 5                         (LOAD_RAMP)     seconds to stagger connects
//!   --ping-interval 250              (LOAD_PING_MS)  ms between pings per client
//!   --burst                          all clients connect at once (thundering herd)
//!   --container zero-cache           (LOAD_CONTAINER) docker container for `docker stats`
//!   --compare --ref-url ws://…:4849/sync --ref-container zero-ref
//!
//! Note: at high client counts raise the fd limit (`ulimit -n 100000`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use zero_loadtest::{render_report, resource_stats, ResourceStats, RunReport};

struct Config {
    url: String,
    clients: usize,
    duration: Duration,
    ramp: Duration,
    ping_interval: Duration,
    burst: bool,
    container: Option<String>,
    compare: bool,
    ref_url: Option<String>,
    ref_container: Option<String>,
}

fn env_or(flag_val: Option<String>, env: &str, default: &str) -> String {
    flag_val
        .or_else(|| std::env::var(env).ok())
        .unwrap_or_else(|| default.to_string())
}

fn parse_config() -> Config {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let flag = |name: &str| -> Option<String> {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1).cloned())
    };
    let has = |name: &str| args.iter().any(|a| a == name);

    Config {
        url: env_or(flag("--url"), "LOAD_URL", "ws://127.0.0.1:4848/sync"),
        clients: env_or(flag("--clients"), "LOAD_CLIENTS", "1000")
            .parse()
            .unwrap_or(1000),
        duration: Duration::from_secs(
            env_or(flag("--duration"), "LOAD_DURATION", "20")
                .parse()
                .unwrap_or(20),
        ),
        ramp: Duration::from_secs(
            env_or(flag("--ramp"), "LOAD_RAMP", "5")
                .parse()
                .unwrap_or(5),
        ),
        ping_interval: Duration::from_millis(
            env_or(flag("--ping-interval"), "LOAD_PING_MS", "250")
                .parse()
                .unwrap_or(250),
        ),
        burst: has("--burst"),
        container: flag("--container").or_else(|| std::env::var("LOAD_CONTAINER").ok()),
        compare: has("--compare"),
        ref_url: flag("--ref-url").or_else(|| std::env::var("LOAD_REF_URL").ok()),
        ref_container: flag("--ref-container").or_else(|| std::env::var("LOAD_REF_CONTAINER").ok()),
    }
}

/// One client's session outcome.
#[derive(Default)]
struct ClientResult {
    connected: bool,
    connect_ms: f64,
    ping_rtts: Vec<f64>,
    frames: u64,
    error: Option<String>,
}

/// The zero sync protocol version this harness speaks (matches
/// `@rocicorp/zero`'s `PROTOCOL_VERSION`). The real connect path is
/// `/sync/v{PROTOCOL_VERSION}/connect`.
const PROTOCOL_VERSION: u32 = 51;

/// Normalizes a target into a base origin (`ws://host:port`), stripping a
/// trailing `/sync`, `/sync/vN/connect`, or `/`.
fn base_origin(url: &str) -> String {
    let u = url.trim_end_matches('/');
    if let Some(idx) = u.find("/sync") {
        u[..idx].to_string()
    } else {
        u.to_string()
    }
}

/// Builds the real `@rocicorp/zero` connect URL for client `i`:
/// `{base}/sync/v{PV}/connect?clientGroupID=…&clientID=…&…`. Each client is a
/// distinct group so their CVR state is independent.
fn connect_url(base: &str, i: usize) -> String {
    format!(
        "{base}/sync/v{PROTOCOL_VERSION}/connect\
         ?clientGroupID=lt-g{i}&clientID=lt-c{i}&wsid={i}\
         &schemaVersion=1&baseCookie=&ts=0&lmid=0"
    )
}

/// The `Sec-WebSocket-Protocol` value an official Zero client sends with no
/// initial message and no auth. JavaScript's `JSON.stringify` omits both
/// `undefined` fields, so the encoded payload is `{}` (not explicit `null`
/// fields). Official zero-cache distinguishes those shapes during handshake.
fn sec_protocol_no_auth() -> String {
    use base64::Engine;
    let payload = "{}";
    let b64 = base64::engine::general_purpose::STANDARD.encode(payload.as_bytes());
    // encodeURIComponent escapes base64's +, /, = (the only non-alphanumerics).
    b64.replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D")
}

/// Runs a single client session until `deadline`. `base` is the target origin;
/// `i` selects this client's identity in the connect URL.
async fn client_session(
    base: String,
    i: usize,
    start_delay: Duration,
    ping_interval: Duration,
    deadline: Instant,
) -> ClientResult {
    let mut r = ClientResult::default();
    if !start_delay.is_zero() {
        tokio::time::sleep(start_delay).await;
    }
    let url = connect_url(&base, i);
    let mut req = match url.into_client_request() {
        Ok(r) => r,
        Err(e) => {
            r.error = Some(format!("bad url: {e}"));
            return r;
        }
    };
    // Real zero requires the Sec-WebSocket-Protocol handshake header.
    if let Ok(v) = sec_protocol_no_auth().parse() {
        req.headers_mut().insert("Sec-WebSocket-Protocol", v);
    }

    let t0 = Instant::now();
    let ws = match tokio_tungstenite::connect_async(req).await {
        Ok((ws, _)) => ws,
        Err(e) => {
            r.error = Some(format!("connect: {}", classify(&e.to_string())));
            return r;
        }
    };
    r.connect_ms = t0.elapsed().as_secs_f64() * 1000.0;
    r.connected = true;
    let (mut sink, mut stream) = ws.split();

    // Greeting.
    match stream.next().await {
        Some(Ok(Message::Text(t))) if t.starts_with("[\"connected\"") => r.frames += 1,
        _ => {
            r.error = Some("no greeting".into());
            return r;
        }
    }
    // Init.
    if sink
        .send(Message::text(
            r#"["initConnection",{"desiredQueriesPatch":[],"clientSchema":{"tables":{"issue":{"columns":{"id":{"type":"string"},"title":{"type":"string"},"owner":{"type":"string"},"open":{"type":"boolean"},"rank":{"type":"number"}},"primaryKey":["id"]}}}}]"#,
        ))
        .await
        .is_err()
    {
        r.error = Some("init send".into());
        return r;
    }

    // Sustained ping loop, measuring RTT.
    while Instant::now() < deadline {
        let ping_at = Instant::now();
        if sink.send(Message::text(r#"["ping",{}]"#)).await.is_err() {
            r.error = Some("ping send".into());
            break;
        }
        // Read until the pong (count any interim frames, e.g. pokes).
        let mut got_pong = false;
        while !got_pong {
            match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => {
                    r.frames += 1;
                    if t == r#"["pong",{}]"# {
                        r.ping_rtts.push(ping_at.elapsed().as_secs_f64() * 1000.0);
                        got_pong = true;
                    }
                }
                Ok(Some(Ok(Message::Close(_)))) | Ok(None) => {
                    r.error = Some("closed".into());
                    return r;
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => {
                    r.error = Some(format!("recv: {}", classify(&e.to_string())));
                    return r;
                }
                Err(_) => {
                    r.error = Some("pong timeout".into());
                    return r;
                }
            }
        }
        tokio::time::sleep(ping_interval).await;
    }
    r
}

/// Collapses noisy error strings into a small set of reasons.
fn classify(e: &str) -> String {
    let e = e.to_lowercase();
    if e.contains("too many open files") {
        "fd limit (raise ulimit -n)".into()
    } else if e.contains("connection refused") {
        "connection refused".into()
    } else if e.contains("reset") {
        "connection reset".into()
    } else if e.contains("timed out") || e.contains("timeout") {
        "timeout".into()
    } else {
        e.chars().take(40).collect()
    }
}

/// Background sampler: polls `docker stats` for `container` until `stop` is set;
/// returns the folded resource stats.
async fn sample_resources(container: String, stop: Arc<AtomicBool>) -> Option<ResourceStats> {
    let samples = Arc::new(Mutex::new(Vec::<(f64, f64)>::new()));
    while !stop.load(Ordering::SeqCst) {
        if let Ok(out) = tokio::process::Command::new("docker")
            .args([
                "stats",
                "--no-stream",
                "--format",
                "{{.CPUPerc}}|{{.MemUsage}}",
                &container,
            ])
            .output()
            .await
        {
            let line = String::from_utf8_lossy(&out.stdout);
            if let Some((cpu, mem)) = line.trim().split_once('|') {
                if let (Some(c), Some(m)) = (
                    zero_loadtest::parse_cpu_pct(cpu),
                    zero_loadtest::parse_mem_mib(mem),
                ) {
                    samples.lock().unwrap().push((c, m));
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let s = samples.lock().unwrap();
    resource_stats(&s)
}

/// Runs the whole load against one target and builds its report.
async fn run_target(url: &str, container: Option<&str>, cfg: &Config) -> RunReport {
    let mut report = RunReport {
        target: url.to_string(),
        clients: cfg.clients,
        ..Default::default()
    };

    // Resource sampler (docker).
    let stop = Arc::new(AtomicBool::new(false));
    let sampler = container.map(|c| {
        let c = c.to_string();
        let stop = stop.clone();
        tokio::spawn(async move { sample_resources(c, stop).await })
    });

    let run_start = Instant::now();
    let deadline = run_start + cfg.ramp + cfg.duration;
    let mut tasks = Vec::with_capacity(cfg.clients);
    for i in 0..cfg.clients {
        let start_delay = if cfg.burst {
            Duration::ZERO
        } else {
            // Spread connects across the ramp window.
            Duration::from_secs_f64(
                cfg.ramp.as_secs_f64() * (i as f64) / (cfg.clients.max(1) as f64),
            )
        };
        let base = base_origin(url);
        let pi = cfg.ping_interval;
        tasks.push(tokio::spawn(client_session(
            base,
            i,
            start_delay,
            pi,
            deadline,
        )));
    }

    for t in tasks {
        match t.await {
            Ok(res) => {
                if res.connected {
                    report.connect.record(res.connect_ms);
                }
                if res.connected && res.error.is_none() {
                    report.connected_ok += 1;
                }
                for rtt in res.ping_rtts {
                    report.ping_rtt.record(rtt);
                }
                report.frames_received += res.frames;
                if let Some(e) = res.error {
                    report.error(e);
                }
            }
            Err(_) => report.error("client task panicked"),
        }
    }
    report.duration_s = run_start.elapsed().as_secs_f64();

    if let Some(sampler) = sampler {
        stop.store(true, Ordering::SeqCst);
        report.resource = sampler.await.ok().flatten();
    }
    report
}

#[tokio::main]
async fn main() {
    let cfg = parse_config();
    eprintln!(
        "zero-loadtest: {} clients, {}s (ramp {}s), ping every {}ms{}",
        cfg.clients,
        cfg.duration.as_secs(),
        cfg.ramp.as_secs(),
        cfg.ping_interval.as_millis(),
        if cfg.burst { ", BURST" } else { "" }
    );

    let primary = run_target(&cfg.url, cfg.container.as_deref(), &cfg).await;
    println!("\n{}", render_report(&primary));

    let mut reference_failed = false;
    if cfg.compare {
        if let Some(ref_url) = &cfg.ref_url {
            eprintln!("comparing against reference {ref_url}…");
            let reference = run_target(ref_url, cfg.ref_container.as_deref(), &cfg).await;
            reference_failed = reference.success_rate() < 0.95 || reference.ping_rtt.is_empty();
            println!("{}", render_report(&reference));
            println!("{}", render_comparison(&primary, &reference));
        } else {
            eprintln!("--compare set but no --ref-url / LOAD_REF_URL");
        }
    }

    // Non-zero exit if the primary target had poor connectivity.
    if primary.success_rate() < 0.95 || primary.ping_rtt.is_empty() || reference_failed {
        eprintln!(
            "WARNING: benchmark target failed to sustain the workload (primary success {:.1}%)",
            primary.success_rate() * 100.0,
        );
        std::process::exit(1);
    }
}

/// A compact side-by-side table for the two targets.
fn render_comparison(a: &RunReport, b: &RunReport) -> String {
    let (pa, pb) = (a.ping_rtt.summary(), b.ping_rtt.summary());
    let mut out = String::from("── comparison (A=primary  B=reference) ──\n");
    let row = |label: &str, av: String, bv: String| format!("  {label:<22} A {av:<14} B {bv}\n");
    out.push_str(&row(
        "connected",
        format!("{:.1}%", a.success_rate() * 100.0),
        format!("{:.1}%", b.success_rate() * 100.0),
    ));
    out.push_str(&row(
        "ping p50/p99 ms",
        format!("{:.2}/{:.2}", pa.p50, pa.p99),
        format!("{:.2}/{:.2}", pb.p50, pb.p99),
    ));
    out.push_str(&row(
        "throughput ping/s",
        format!("{:.0}", a.throughput_ops_s()),
        format!("{:.0}", b.throughput_ops_s()),
    ));
    if let (Some(ra), Some(rb)) = (&a.resource, &b.resource) {
        out.push_str(&row(
            "CPU peak %",
            format!("{:.0}", ra.peak_cpu_pct),
            format!("{:.0}", rb.peak_cpu_pct),
        ));
        out.push_str(&row(
            "mem peak MiB",
            format!("{:.0}", ra.peak_mem_mib),
            format!("{:.0}", rb.peak_mem_mib),
        ));
    }
    out
}
