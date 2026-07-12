//! Live multi-connection client-group test — the correctness gate for
//! client-group ownership (redesign §6 C2).
//!
//! Two REAL WebSocket connections join the SAME client group against one
//! server with a REAL Postgres-backed CVR, then live replication commits land
//! while one connection changes its desired queries. Upstream serves a client
//! group from ONE ViewSyncerService (one CVR, one pipeline); this test pins
//! the observable contract of that model:
//!
//! 1. both clients hydrate the shared query (the second is seeded, not
//!    re-hydrated),
//! 2. every live commit reaches BOTH clients,
//! 3. each client's poke chain is self-consistent (every `pokeStart.baseCookie`
//!    equals that client's previous `pokeEnd.cookie`),
//! 4. no CVR conflict ever escapes to the wire as an error frame.
//!
//! Requires a live test Postgres (`ZERO_TEST_PG_URL`, `scripts/test.sh
//! --with-pg`); skips gracefully without one.

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

use zero_cache_server::bootstrap::{run_synced_server, CvrRuntimeConfig, HandlerDeps};
use zero_cache_server::sync_service::SyncService;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_sqlite::change_log::ChangeLog;
use zero_cache_sqlite::replication_state::update_replication_watermark;
use zero_cache_sqlite::StatementRunner;

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn test_pg_url() -> String {
    std::env::var("ZERO_TEST_PG_URL")
        .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into())
}

/// Seeds a replica shaped like a real replicated table (including the
/// `_0_version` column the replicator adds), so the pipeline's advance path
/// sees production-shaped rows.
fn seed_replica(tag: &str) -> String {
    let path = std::env::temp_dir().join(format!("zc_multiconn_{}_{tag}.db", std::process::id()));
    let path = path.to_str().unwrap().to_string();
    let _ = std::fs::remove_file(&path);
    let db = StatementRunner::open_file(&path).unwrap();
    zero_cache_sqlite::replication_state::init_replication_state(
        &db,
        &[],
        "00",
        &JsonValue::Object(vec![]),
        true,
    )
    .unwrap();
    db.exec(zero_cache_sqlite::change_log::CREATE_CHANGELOG_SCHEMA)
        .unwrap();
    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT NOT NULL, _0_version TEXT NOT NULL)")
        .unwrap();
    db.exec("INSERT INTO issue (id, title, _0_version) VALUES (1, 'alpha', '00')")
        .unwrap();
    db.exec("INSERT INTO issue (id, title, _0_version) VALUES (2, 'beta', '00')")
        .unwrap();
    drop(db);
    path
}

struct Server {
    addr: std::net::SocketAddr,
    service: Arc<SyncService>,
    shutdown: Option<oneshot::Sender<()>>,
    handle: tokio::task::JoinHandle<u64>,
    replica_path: String,
}

impl Server {
    async fn boot(replica_path: String, deps: HandlerDeps) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service = Arc::new(SyncService::new(64));
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(run_synced_server(
            listener,
            service.clone(),
            rx,
            replica_path.clone(),
            deps,
        ));
        Server {
            addr,
            service,
            shutdown: Some(tx),
            handle,
            replica_path,
        }
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.handle.await;
        let _ = std::fs::remove_file(&self.replica_path);
    }
}

/// Connects to `group`/`client` and sends `initConnection` desiring `patch`.
async fn connect_in_group(
    addr: std::net::SocketAddr,
    group: &str,
    client: &str,
    patch: &str,
) -> Client {
    let req =
        format!("ws://{addr}/sync/v51/connect?clientGroupID={group}&clientID={client}&lmid=0")
            .into_client_request()
            .unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let greeting = ws.next().await.unwrap().unwrap().into_text().unwrap();
    assert!(
        greeting.starts_with("[\"connected\""),
        "greeting: {greeting}"
    );
    ws.send(Message::text(format!(
        r#"["initConnection",{{"desiredQueriesPatch":{patch}}}]"#
    )))
    .await
    .unwrap();
    ws
}

/// One client's accumulated frame log: pumps frames on demand and verifies the
/// poke-chain invariant over everything received so far.
struct ClientLog {
    name: &'static str,
    ws: Client,
    frames: Vec<String>,
}

impl ClientLog {
    fn new(name: &'static str, ws: Client) -> Self {
        ClientLog {
            name,
            ws,
            frames: Vec::new(),
        }
    }

    /// Reads frames (20s inactivity deadline) until `pred` matches one, keeping
    /// everything received. Panics with the full log if the deadline expires.
    /// The deadline is generous because the group-ownership hydration path is
    /// CPU-heavy and this test runs concurrently with the rest of the workspace
    /// suite (`cargo test --workspace`), which can saturate the machine.
    async fn pump_until(&mut self, what: &str, pred: impl Fn(&str) -> bool) {
        if self.frames.iter().any(|frame| pred(frame)) {
            return;
        }
        loop {
            let frame = tokio::time::timeout(std::time::Duration::from_secs(20), self.ws.next())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "{}: timed out waiting for {what}\nframes so far:\n{}",
                        self.name,
                        self.frames.join("\n")
                    )
                });
            let Some(Ok(Message::Text(text))) = frame else {
                continue;
            };
            let text = text.to_string();
            let done = pred(&text);
            self.frames.push(text);
            if done {
                return;
            }
        }
    }

    /// Drains any frames already buffered on the socket, so chain checks see
    /// complete poke trios. Buffered frames arrive back-to-back, so a short
    /// idle gap means the socket is dry — except mid-poke (a pokeStart without
    /// its pokeEnd), where we keep waiting so a slow flush can't split a trio.
    async fn drain_briefly(&mut self) {
        let start = std::time::Instant::now();
        loop {
            match tokio::time::timeout(std::time::Duration::from_millis(100), self.ws.next()).await
            {
                Ok(Some(Ok(Message::Text(text)))) => self.frames.push(text.to_string()),
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(_))) | Ok(None) => return,
                Err(_) => {
                    let starts = self
                        .frames
                        .iter()
                        .filter(|f| f.starts_with("[\"pokeStart\""))
                        .count();
                    let ends = self
                        .frames
                        .iter()
                        .filter(|f| f.starts_with("[\"pokeEnd\""))
                        .count();
                    if starts == ends || start.elapsed() > std::time::Duration::from_secs(3) {
                        return;
                    }
                }
            }
        }
    }

    fn assert_no_errors(&self) {
        for frame in &self.frames {
            assert!(
                !frame.starts_with("[\"error\""),
                "{}: server sent an error frame: {frame}\nfull log:\n{}",
                self.name,
                self.frames.join("\n")
            );
            assert!(
                !frame.contains("concurrent modification"),
                "{}: CVR conflict escaped to the wire: {frame}",
                self.name
            );
        }
    }

    /// Every pokeStart must base at exactly the cookie this client last
    /// received (null before the first poke) — the invariant a real client's
    /// Replicache enforces ("unexpected base cookie during sync").
    fn assert_poke_chain(&self) {
        let mut last_cookie = serde_json::Value::Null;
        for frame in &self.frames {
            let parsed: serde_json::Value = match serde_json::from_str(frame) {
                Ok(value) => value,
                Err(_) => continue,
            };
            let (kind, body) = (
                parsed.get(0).and_then(|v| v.as_str()).unwrap_or(""),
                parsed.get(1).cloned().unwrap_or(serde_json::Value::Null),
            );
            match kind {
                "pokeStart" => {
                    let base = body
                        .get("baseCookie")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    assert_eq!(
                        base,
                        last_cookie,
                        "{}: pokeStart base cookie broke the chain\nframe: {frame}\nfull log:\n{}",
                        self.name,
                        self.frames.join("\n")
                    );
                }
                "pokeEnd" => {
                    if let Some(cookie) = body.get("cookie") {
                        last_cookie = cookie.clone();
                    }
                }
                _ => {}
            }
        }
    }
}

/// Applies one committed row update to the replica and fans it out, exactly as
/// the replicator does: row write + change-log entry + watermark, then
/// `publish_commit`.
fn commit_issue_title(replica: &str, service: &SyncService, id: i64, title: &str, version: &str) {
    let db = StatementRunner::open_file(replica).unwrap();
    db.exec(&format!(
        "UPDATE issue SET title = '{title}', _0_version = '{version}' WHERE id = {id}"
    ))
    .unwrap();
    ChangeLog::new(&db)
        .log_set_op(
            version,
            0,
            "issue",
            &vec![("id".to_string(), JsonValue::Number(id as f64))],
            None,
        )
        .unwrap();
    update_replication_watermark(&db, version).unwrap();
    drop(db);
    service.publish_commit(version, false, 1);
}

/// Group ownership WITHOUT a durable CVR store: the group's in-memory CVR
/// cell is the only source of truth, and it alone must give the group's
/// connections one consistent state. The second client joins LATE (after the
/// first commit), exercising the bootstrap seeding from the live group state.
/// Runs without Postgres.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_without_durable_cvr_still_shares_one_state() {
    let replica = seed_replica("no_durable");
    let deps = HandlerDeps {
        group_ownership: Some(true),
        ..Default::default()
    };
    let server = Server::boot(replica.clone(), deps).await;

    let all_issues = r#"[{"op":"put","hash":"q-issues","ast":{"table":"issue"}}]"#;
    let mut a = ClientLog::new(
        "client-a",
        connect_in_group(server.addr, "gnd", "ca", all_issues).await,
    );
    a.pump_until("initial hydration", |t| t.contains("gotQueriesPatch"))
        .await;
    a.pump_until("initial rows", |t| t.contains("alpha")).await;

    // First live commit lands while only A is connected.
    commit_issue_title(&replica, &server.service, 1, "alpha-updated", "01");
    a.pump_until("live update for commit 01", |t| t.contains("alpha-updated"))
        .await;

    // B joins LATE: it must hydrate the group's CURRENT state (the updated
    // title, not the seed) and be marked got.
    let mut b = ClientLog::new(
        "client-b",
        connect_in_group(server.addr, "gnd", "cb", all_issues).await,
    );
    b.pump_until("late-join hydration (got)", |t| {
        t.contains("gotQueriesPatch")
    })
    .await;
    b.pump_until("late-join rows reflect the commit", |t| {
        t.contains("alpha-updated")
    })
    .await;

    // A second commit must reach BOTH connections.
    commit_issue_title(&replica, &server.service, 2, "beta-updated", "02");
    a.pump_until("live update for commit 02", |t| t.contains("beta-updated"))
        .await;
    b.pump_until("live update for commit 02", |t| t.contains("beta-updated"))
        .await;

    a.drain_briefly().await;
    b.drain_briefly().await;
    a.assert_no_errors();
    b.assert_no_errors();
    a.assert_poke_chain();
    b.assert_poke_chain();

    server.shutdown().await;
}

/// Group semantics through the processor loop (group-loop plan increment 2):
/// two connections in ONE group desire DIFFERENT narrow queries, and each
/// connection receives the OTHER's query rows too — the group serves one shared
/// view. Both poke chains stay self-consistent and no error frame escapes. Runs
/// without Postgres (the in-memory group CVR is the source of truth).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_desired_query_fans_rows_to_the_whole_group() {
    let replica = seed_replica("group_fanout");
    let deps = HandlerDeps {
        group_ownership: Some(true),
        ..Default::default()
    };
    let server = Server::boot(replica.clone(), deps).await;

    // A desires only issue id=1 ('alpha').
    let only_1 = r#"[{"op":"put","hash":"qa","ast":{"table":"issue","where":{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1}}}}]"#;
    let mut a = ClientLog::new(
        "client-a",
        connect_in_group(server.addr, "gfan", "ca", only_1).await,
    );
    a.pump_until("A initial got", |t| t.contains("gotQueriesPatch"))
        .await;
    a.pump_until("A initial row", |t| t.contains("alpha")).await;

    // B joins and desires only issue id=2 ('beta'). A must ALSO receive that
    // row — one shared group view — even though A never desired qb.
    let only_2 = r#"[{"op":"put","hash":"qb","ast":{"table":"issue","where":{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":2}}}}]"#;
    let mut b = ClientLog::new(
        "client-b",
        connect_in_group(server.addr, "gfan", "cb", only_2).await,
    );
    b.pump_until("B got", |t| t.contains("gotQueriesPatch"))
        .await;
    b.pump_until("B row", |t| t.contains("beta")).await;

    // The row B's query introduced (id=2 'beta') is fanned to A too.
    a.pump_until("A receives B's query row (group semantics)", |t| {
        t.contains("beta")
    })
    .await;

    a.drain_briefly().await;
    b.drain_briefly().await;
    a.assert_no_errors();
    b.assert_no_errors();
    a.assert_poke_chain();
    b.assert_poke_chain();

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_connections_in_one_group_share_cvr_and_both_see_live_commits() {
    let connection_string = test_pg_url();
    let Ok(cvr_client) = zero_cache_change_source::pg_connection::connect(&connection_string).await
    else {
        eprintln!("skipping multi-connection group test: no test Postgres available");
        return;
    };
    let shard = zero_cache_types::shards::ShardId {
        app_id: "gcvrmc".into(),
        shard_num: 0,
    };
    cvr_client
        .batch_execute("DROP SCHEMA IF EXISTS \"gcvrmc_0/cvr\" CASCADE;")
        .await
        .unwrap();
    for statement in
        zero_cache_view_syncer::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap()
    {
        cvr_client.batch_execute(&statement).await.unwrap();
    }

    let replica = seed_replica("shared_group");
    let deps = HandlerDeps {
        cvr: Some(CvrRuntimeConfig {
            connection_string: connection_string.clone(),
            max_connections: 8,
            shard: shard.clone(),
            task_id: "multiconn-task".into(),
        }),
        group_ownership: Some(true),
        ..Default::default()
    };
    let server = Server::boot(replica.clone(), deps).await;

    // Two connections, SAME client group, distinct clients, desiring the SAME
    // query. The group's shared pipeline hydrates it once; the second desirer
    // is seeded from the active query.
    let all_issues = r#"[{"op":"put","hash":"q-issues","ast":{"table":"issue"}}]"#;
    let mut a = ClientLog::new(
        "client-a",
        connect_in_group(server.addr, "gmc", "ca", all_issues).await,
    );
    a.pump_until("initial hydration (rows + got)", |t| {
        t.contains("gotQueriesPatch")
    })
    .await;
    a.pump_until("initial rows", |t| t.contains("alpha")).await;

    let mut b = ClientLog::new(
        "client-b",
        connect_in_group(server.addr, "gmc", "cb", all_issues).await,
    );
    b.pump_until("initial hydration (rows + got)", |t| {
        t.contains("gotQueriesPatch")
    })
    .await;
    b.pump_until("initial rows", |t| t.contains("alpha")).await;

    // A live commit updates a row BOTH clients track: each must receive it.
    commit_issue_title(&replica, &server.service, 1, "alpha-updated", "01");
    a.pump_until("live update for commit 01", |t| t.contains("alpha-updated"))
        .await;
    b.pump_until("live update for commit 01", |t| t.contains("alpha-updated"))
        .await;

    // Overlap a desired-queries change on B with another live commit — the
    // classic CVR write/write interleave inside one group.
    let second_query = r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"q-issues-2","ast":{"table":"issue","where":{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":2}}}}]}]"#;
    b.ws.send(Message::text(second_query)).await.unwrap();
    commit_issue_title(&replica, &server.service, 2, "beta-updated", "02");

    a.pump_until("live update for commit 02", |t| t.contains("beta-updated"))
        .await;
    b.pump_until("live update for commit 02", |t| t.contains("beta-updated"))
        .await;
    b.pump_until("second query got", |t| {
        t.contains("q-issues-2") && t.contains("gotQueriesPatch")
    })
    .await;

    // Let any trailing poke frames (pokeEnd after the matched pokePart) land
    // before checking the chains.
    a.drain_briefly().await;
    b.drain_briefly().await;

    a.assert_no_errors();
    b.assert_no_errors();
    a.assert_poke_chain();
    b.assert_poke_chain();

    server.shutdown().await;
    cvr_client
        .batch_execute("DROP SCHEMA \"gcvrmc_0/cvr\" CASCADE;")
        .await
        .unwrap();
}
