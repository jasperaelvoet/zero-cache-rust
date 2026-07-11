//! Deep real-client e2e suite modeled on — and deliberately harder than — the
//! production hunting-game Zero app (`/Users/.../hunting-game`).
//!
//! Every test spins up the REAL server (`run_synced_server` + `HandlerDeps`),
//! connects a REAL WebSocket client, desires a query by pushing its AST straight
//! through `initConnection` (no mock query server needed), and asserts which
//! rows hydrate to the client. The replica is a rich multi-table graph shaped
//! like hunting-game: users / public_user projections, bidirectional
//! friendships, games, players (with self-referential `arrestedBy` / `kickedBy`
//! edges), audience-discriminated composite-PK player_location rows, team-scoped
//! messages, and time-ordered progression events.
//!
//! The query shapes here reproduce hunting-game's real reads
//! (`getEndScreenSummary`, `getGameState`, `getGameMessages`, `getFriendProfile`,
//! `getLocation`, the recent-XP/coin windows) and then push PAST them:
//!   * deeper `related` chains (5 hops) than the app's 3–4,
//!   * the self-referential player→player edges the app declares but never reads,
//!   * keyset `start` cursor pagination the app never uses,
//!   * `IN` / `NOT IN` / `IS NULL` / `LIKE` / `ILIKE` / range operators,
//!   * `orderBy desc` + `limit` recency windows,
//!   * top-level OR-of-EXISTS friend authorization combined WITH `related` output.
//!
//! Tests are grouped:
//!   A. Correctness the port is expected to satisfy (pure hydration, no PG).
//!   B. KNOWN-GAP tests: they assert the CORRECT semantics for shapes the
//!      current builder cannot yet handle (a correlated subquery under an OR
//!      nested inside an AND — exactly `getGameState`/`getGameMessages`/
//!      `getLocation`). These are expected to FAIL until the graph path covers
//!      the shape; they are the executable spec for that work, not a regression.
//!   C. Live fan-out through a complex query in a shared client group (needs a
//!      test Postgres for the CVR; skips gracefully without one).

use std::sync::Arc;
use std::time::{Duration, Instant};

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

// ----------------------------------------------------------------------------
// Harness
// ----------------------------------------------------------------------------

fn unique_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn init_replica_metadata(db: &StatementRunner) {
    zero_cache_sqlite::replication_state::init_replication_state(
        db,
        &[],
        "00",
        &JsonValue::Object(vec![]),
        true,
    )
    .unwrap();
    db.exec(zero_cache_sqlite::change_log::CREATE_CHANGELOG_SCHEMA)
        .unwrap();
}

/// Writes a fresh replica from raw DDL + inserts, closes the writer.
fn seed_sql(tag: &str, stmts: &[&str]) -> String {
    let path = std::env::temp_dir().join(format!(
        "zc_hg_{}_{tag}_{}.db",
        std::process::id(),
        unique_id()
    ));
    let path = path.to_str().unwrap().to_string();
    let _ = std::fs::remove_file(&path);
    let db = StatementRunner::open_file(&path).unwrap();
    init_replica_metadata(&db);
    for s in stmts {
        db.exec(s).unwrap();
    }
    drop(db);
    path
}

/// The rich hunting-game-shaped graph most tests hydrate against.
///
/// Scenario: alice(1) is a HUNTER in an active STANDARD game g1. bob(2) is a
/// RUNNER and alice's ACCEPTED friend. carol(3) is a SUPERVISOR and a stranger
/// (only a PENDING request). dave(4) is a RUNNER who LEFT and was arrested by
/// alice. erin(5) is a HUNTER kicked by alice. alice(1) is also a RUNNER in a
/// second, not-yet-started HIDE_AND_SEEK game g2.
fn seed_world() -> String {
    seed_sql(
        "world",
        &[
            // users + restricted public projection
            "CREATE TABLE user (id INTEGER PRIMARY KEY, name TEXT, totalXP INTEGER, coinBalance INTEGER, isPro INTEGER)",
            "INSERT INTO user (id, name, totalXP, coinBalance, isPro) VALUES \
             (1,'alice',500,100,1),(2,'bob',300,40,0),(3,'carol',900,10,0),(4,'dave',20,0,0),(5,'erin',75,5,0)",
            "CREATE TABLE public_user (id INTEGER PRIMARY KEY, name TEXT, totalXP INTEGER)",
            "INSERT INTO public_user (id, name, totalXP) VALUES \
             (1,'alice',500),(2,'bob',300),(3,'carol',900),(4,'dave',20),(5,'erin',75)",
            // bidirectional friendship: alice<->bob ACCEPTED; carol->alice PENDING
            "CREATE TABLE friendship (id INTEGER PRIMARY KEY, senderId INTEGER, receiverId INTEGER, status TEXT, createdAt INTEGER)",
            "INSERT INTO friendship (id, senderId, receiverId, status, createdAt) VALUES \
             (1,1,2,'ACCEPTED',1000),(2,3,1,'PENDING',2000)",
            // games
            "CREATE TABLE game (id INTEGER PRIMARY KEY, joinCode TEXT, isStarted INTEGER, gameMode TEXT, playableAreaId INTEGER, createdAt INTEGER)",
            "INSERT INTO game (id, joinCode, isStarted, gameMode, playableAreaId, createdAt) VALUES \
             (1,'ABC',1,'STANDARD',10,100),(2,'XYZ',0,'HIDE_AND_SEEK',NULL,200)",
            // players — note self-referential arrestedById / kickedById
            "CREATE TABLE player (id INTEGER PRIMARY KEY, username TEXT, userId INTEGER, gameId INTEGER, \
             role TEXT, hasLeft INTEGER, leftAt INTEGER, arrestedById INTEGER, kickedById INTEGER, createdAt INTEGER)",
            "INSERT INTO player (id, username, userId, gameId, role, hasLeft, leftAt, arrestedById, kickedById, createdAt) VALUES \
             (1,'p1_hunter',1,1,'HUNTER',NULL,NULL,NULL,NULL,100), \
             (2,'p2_runner',2,1,'RUNNER',NULL,NULL,NULL,NULL,200), \
             (3,'p3_super',3,1,'SUPERVISOR',NULL,NULL,NULL,NULL,300), \
             (4,'p4_left',4,1,'RUNNER',5000,5000,1,NULL,400), \
             (5,'p5_kicked',5,1,'HUNTER',NULL,NULL,NULL,1,500), \
             (6,'p6_g2',1,2,'RUNNER',NULL,NULL,NULL,NULL,600)",
            // audience-discriminated composite-PK locations
            "CREATE TABLE player_location (playerId INTEGER, gameId INTEGER, audience TEXT, \
             latitude REAL, longitude REAL, updatedAt INTEGER, PRIMARY KEY (playerId, gameId, audience))",
            "INSERT INTO player_location (playerId, gameId, audience, latitude, longitude, updatedAt) VALUES \
             (1,1,'HUNTER',11.1,1.0,10),(2,1,'HUNTER',22.2,2.0,20),(2,1,'RUNNER',33.3,3.0,30)",
            // team-scoped chat
            "CREATE TABLE message (id INTEGER PRIMARY KEY, fromPlayerId INTEGER, toTeam TEXT, content TEXT, gameId INTEGER, time INTEGER)",
            "INSERT INTO message (id, fromPlayerId, toTeam, content, gameId, time) VALUES \
             (1,1,'EVERYONE','msg_everyone',1,10),(2,1,'HUNTER','msg_hunters',1,20), \
             (3,2,'RUNNER','msg_runners',1,30),(4,3,'SUPERVISOR','msg_super',1,40)",
            // event-sourced progression (XP + COIN), time-ordered
            "CREATE TABLE progression_event (id INTEGER PRIMARY KEY, userId INTEGER, gameId INTEGER, \
             kind TEXT, type TEXT, amount INTEGER, createdAt INTEGER)",
            "INSERT INTO progression_event (id, userId, gameId, kind, type, amount, createdAt) VALUES \
             (1,1,1,'XP','ev_kill_a',10,10),(2,1,1,'XP','ev_kill_b',20,20),(3,1,1,'COIN','ev_reward',5,25), \
             (4,1,1,'XP','ev_win',30,30),(5,1,1,'XP','ev_bonus',40,40),(6,1,1,'COIN','ev_spend',-5,45), \
             (7,1,1,'XP','ev_streak',50,50),(8,2,1,'XP','ev_bob',15,15)",
            "CREATE TABLE playable_area (id INTEGER PRIMARY KEY, type TEXT, radiusLatitude REAL)",
            "INSERT INTO playable_area (id, type, radiusLatitude) VALUES (10,'CIRCLE',0.5)",
            "CREATE TABLE player_travel_path (id INTEGER PRIMARY KEY, playerId INTEGER, gameId INTEGER, pointsJson TEXT)",
            "INSERT INTO player_travel_path (id, playerId, gameId, pointsJson) VALUES (1,1,1,'path_p1')",
        ],
    )
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

/// Connects to the given group/client and desires `ast` (a raw JSON AST string)
/// under `hash`.
async fn connect_ast(
    addr: std::net::SocketAddr,
    group: &str,
    client: &str,
    hash: &str,
    ast: &str,
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
    let patch = format!(r#"[{{"op":"put","hash":"{hash}","ast":{ast}}}]"#);
    ws.send(Message::text(format!(
        r#"["initConnection",{{"desiredQueriesPatch":{patch}}}]"#
    )))
    .await
    .unwrap();
    ws
}

/// Boots a single-connection server against `replica` with no app APIs, desires
/// `ast`, and returns (server, hydration-frames-joined). Non-panicking: collects
/// until the query is marked got + a short trailing drain, or a hard cap.
async fn hydrate_ast(replica: String, ast: &str) -> (Server, String) {
    let server = Server::boot(replica, HandlerDeps::default()).await;
    let mut ws = connect_ast(server.addr, "cg", "c1", "q", ast).await;
    let frames = collect_hydration(&mut ws).await;
    (server, frames)
}

/// Reads text frames until the query is acknowledged (`gotQueriesPatch`) plus a
/// brief drain for trailing row pokes, or a 4s hard cap. Never panics, so
/// known-gap tests fail via a clean assertion rather than a harness timeout.
async fn collect_hydration(ws: &mut Client) -> String {
    let mut acc = String::new();
    let start = Instant::now();
    // Phase 1: accumulate until the query is marked got, or 4s.
    while start.elapsed() < Duration::from_secs(4) {
        match tokio::time::timeout(Duration::from_millis(800), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                acc.push_str(t.as_ref());
                acc.push('\n');
                if acc.contains("gotQueriesPatch") {
                    break;
                }
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) | Ok(None) => break, // socket closed (e.g. server task panicked)
            Err(_) => {}                          // idle tick; keep waiting until the hard cap
        }
    }
    // Phase 2: drain trailing frames (already-computed row pokes) for ~700ms.
    let drain_start = Instant::now();
    while drain_start.elapsed() < Duration::from_millis(700) {
        match tokio::time::timeout(Duration::from_millis(350), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                acc.push_str(t.as_ref());
                acc.push('\n');
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    acc
}

fn assert_has(frames: &str, needle: &str, why: &str) {
    assert!(
        frames.contains(needle),
        "expected `{needle}` in hydration ({why})\n--- frames ---\n{frames}"
    );
}

fn assert_absent(frames: &str, needle: &str, why: &str) {
    assert!(
        !frames.contains(needle),
        "`{needle}` LEAKED into hydration ({why})\n--- frames ---\n{frames}"
    );
}

// ============================================================================
// A. Correctness the port is expected to satisfy
// ============================================================================

/// `getEndScreenSummary`, deepened: player → user, travelPath, game → (players →
/// publicUser), progressionEvents(filtered+ordered), playableArea. Four levels
/// of `related` fan-out with per-relation WHERE/orderBy on the leaves.
#[tokio::test]
async fn deep_nested_related_end_screen_summary() {
    let ast = r#"{
        "table":"player",
        "where":{"type":"and","conditions":[
            {"type":"simple","op":"=","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":1}},
            {"type":"simple","op":"IS","left":{"type":"column","name":"hasLeft"},"right":{"type":"literal","value":null}}
        ]},
        "related":[
            {"correlation":{"parentField":["userId"],"childField":["id"]},"subquery":{"table":"user"},"system":"client"},
            {"correlation":{"parentField":["id"],"childField":["playerId"]},"subquery":{"table":"player_travel_path"},"system":"client"},
            {"correlation":{"parentField":["gameId"],"childField":["id"]},"subquery":{
                "table":"game",
                "related":[
                    {"correlation":{"parentField":["id"],"childField":["gameId"]},"subquery":{
                        "table":"player","orderBy":[["createdAt","asc"]],
                        "related":[{"correlation":{"parentField":["userId"],"childField":["id"]},"subquery":{"table":"public_user"},"system":"client"}]
                    },"system":"client"},
                    {"correlation":{"parentField":["id"],"childField":["gameId"]},"subquery":{
                        "table":"progression_event",
                        "where":{"type":"simple","op":"=","left":{"type":"column","name":"kind"},"right":{"type":"literal","value":"XP"}},
                        "orderBy":[["createdAt","asc"]]
                    },"system":"client"},
                    {"correlation":{"parentField":["playableAreaId"],"childField":["id"]},"subquery":{"table":"playable_area"},"system":"client"}
                ]
            },"system":"client"}
        ],
        "orderBy":[["createdAt","desc"]]
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    // Root player for alice in g1 (p1). p6 is her g2 player — the query is not
    // scoped to a game, so multiple root players may hydrate, but the g1 branch
    // must carry the full tree.
    assert_has(&frames, "p1_hunter", "root player hydrates");
    assert_has(&frames, "path_p1", "travelPath relation");
    assert_has(&frames, "ev_win", "filtered XP progression events");
    assert_has(&frames, "CIRCLE", "playableArea relation");
    // Nested players → publicUser: bob/carol appear via the game's roster.
    assert_has(&frames, "p2_runner", "game roster relation");
    // COIN events must be excluded by the kind=XP filter on that relation.
    assert_absent(&frames, "ev_reward", "COIN event excluded from XP relation");
    server.shutdown().await;
}

/// A child relation filtered + ordered + limited: game.related(players where
/// role IN (HUNTER,RUNNER) orderBy createdAt asc limit 2).
#[tokio::test]
async fn related_child_filtered_ordered_limited() {
    let ast = r#"{
        "table":"game",
        "where":{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1}},
        "related":[{"correlation":{"parentField":["id"],"childField":["gameId"]},"subquery":{
            "table":"player",
            "where":{"type":"simple","op":"IN","left":{"type":"column","name":"role"},"right":{"type":"literal","value":["HUNTER","RUNNER"]}},
            "orderBy":[["createdAt","asc"]],
            "limit":2
        },"system":"client"}]
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    // First two by createdAt asc among IN(HUNTER,RUNNER): p1(100), p2(200).
    assert_has(&frames, "p1_hunter", "first player by createdAt asc");
    assert_has(&frames, "p2_runner", "second player by createdAt asc");
    assert_absent(&frames, "p3_super", "SUPERVISOR excluded by IN filter");
    assert_absent(&frames, "p5_kicked", "beyond limit 2");
    server.shutdown().await;
}

/// Keyset pagination the app never uses: XP events for alice, orderBy createdAt
/// asc, `start` at createdAt=20 EXCLUSIVE, limit 2 → the two events strictly
/// after the cursor.
#[tokio::test]
async fn keyset_start_cursor_pagination() {
    let ast = r#"{
        "table":"progression_event",
        "where":{"type":"and","conditions":[
            {"type":"simple","op":"=","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":1}},
            {"type":"simple","op":"=","left":{"type":"column","name":"kind"},"right":{"type":"literal","value":"XP"}}
        ]},
        "orderBy":[["createdAt","asc"]],
        "start":{"row":{"createdAt":20},"exclusive":true},
        "limit":2
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    // XP asc for alice: 10,20,30,40,50. After 20 exclusive, limit 2 → 30,40.
    assert_has(&frames, "ev_win", "createdAt=30 after cursor");
    assert_has(&frames, "ev_bonus", "createdAt=40 after cursor");
    assert_absent(&frames, "ev_kill_a", "createdAt=10 before cursor");
    assert_absent(
        &frames,
        "ev_kill_b",
        "createdAt=20 is the exclusive boundary",
    );
    assert_absent(&frames, "ev_streak", "createdAt=50 beyond limit");
    server.shutdown().await;
}

/// `getRecentXPEvents` shape: orderBy createdAt DESC + limit — the most-recent
/// window.
#[tokio::test]
async fn order_by_desc_recency_window() {
    let ast = r#"{
        "table":"progression_event",
        "where":{"type":"and","conditions":[
            {"type":"simple","op":"=","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":1}},
            {"type":"simple","op":"=","left":{"type":"column","name":"kind"},"right":{"type":"literal","value":"XP"}}
        ]},
        "orderBy":[["createdAt","desc"]],
        "limit":3
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    // Newest 3 XP: 50,40,30.
    assert_has(&frames, "ev_streak", "newest XP (50)");
    assert_has(&frames, "ev_bonus", "second newest (40)");
    assert_has(&frames, "ev_win", "third newest (30)");
    assert_absent(&frames, "ev_kill_a", "oldest, beyond limit 3");
    assert_absent(&frames, "ev_kill_b", "beyond limit 3");
    server.shutdown().await;
}

/// `NOT IN` excludes a value set.
#[tokio::test]
async fn not_in_operator_excludes_set() {
    let ast = r#"{
        "table":"player",
        "where":{"type":"and","conditions":[
            {"type":"simple","op":"=","left":{"type":"column","name":"gameId"},"right":{"type":"literal","value":1}},
            {"type":"simple","op":"NOT IN","left":{"type":"column","name":"role"},"right":{"type":"literal","value":["SUPERVISOR"]}}
        ]}
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    assert_has(&frames, "p1_hunter", "HUNTER not in excluded set");
    assert_has(&frames, "p2_runner", "RUNNER not in excluded set");
    assert_absent(&frames, "p3_super", "SUPERVISOR excluded by NOT IN");
    server.shutdown().await;
}

/// `IS NULL` (active players) vs `IS NOT NULL` (players who left).
#[tokio::test]
async fn is_null_and_is_not_null_partition_players() {
    let active = r#"{
        "table":"player",
        "where":{"type":"and","conditions":[
            {"type":"simple","op":"=","left":{"type":"column","name":"gameId"},"right":{"type":"literal","value":1}},
            {"type":"simple","op":"IS","left":{"type":"column","name":"hasLeft"},"right":{"type":"literal","value":null}}
        ]}
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), active).await;
    assert_has(&frames, "p1_hunter", "active player");
    assert_absent(
        &frames,
        "p4_left",
        "left player excluded by hasLeft IS NULL",
    );
    server.shutdown().await;

    let left = r#"{
        "table":"player",
        "where":{"type":"simple","op":"IS NOT","left":{"type":"column","name":"hasLeft"},"right":{"type":"literal","value":null}}
    }"#;
    let (server2, frames2) = hydrate_ast(seed_world(), left).await;
    assert_has(&frames2, "p4_left", "left player matches IS NOT NULL");
    assert_absent(
        &frames2,
        "p1_hunter",
        "active player excluded by IS NOT NULL",
    );
    server2.shutdown().await;
}

/// Two-sided range: `getActiveDiscount` shape — enabled AND start<=now AND
/// end>=now, using `<=` and `>=`.
#[tokio::test]
async fn two_sided_range_filter_active_window() {
    let replica = seed_sql(
        "discount",
        &[
            "CREATE TABLE discount (id INTEGER PRIMARY KEY, name TEXT, enabled INTEGER, startDate INTEGER, endDate INTEGER)",
            "INSERT INTO discount (id, name, enabled, startDate, endDate) VALUES \
             (1,'past',1,10,20),(2,'active',1,50,150),(3,'future',1,200,300),(4,'disabled',0,50,150)",
        ],
    );
    // now = 100.
    let ast = r#"{
        "table":"discount",
        "where":{"type":"and","conditions":[
            {"type":"simple","op":"=","left":{"type":"column","name":"enabled"},"right":{"type":"literal","value":1}},
            {"type":"simple","op":"<=","left":{"type":"column","name":"startDate"},"right":{"type":"literal","value":100}},
            {"type":"simple","op":">=","left":{"type":"column","name":"endDate"},"right":{"type":"literal","value":100}}
        ]}
    }"#;
    let (server, frames) = hydrate_ast(replica, ast).await;
    assert_has(&frames, "\"active\"", "the currently-active discount");
    assert_absent(&frames, "\"past\"", "ended before now");
    assert_absent(&frames, "\"future\"", "starts after now");
    assert_absent(&frames, "\"disabled\"", "disabled flag");
    server.shutdown().await;
}

/// `LIKE` and `ILIKE` text search over usernames.
#[tokio::test]
async fn like_and_ilike_text_search() {
    let like = r#"{
        "table":"player",
        "where":{"type":"simple","op":"LIKE","left":{"type":"column","name":"username"},"right":{"type":"literal","value":"%runner%"}}
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), like).await;
    assert_has(&frames, "p2_runner", "matches %runner%");
    assert_absent(&frames, "p1_hunter", "does not match %runner%");
    server.shutdown().await;

    // ILIKE is case-insensitive: uppercase pattern still matches lowercase data.
    let ilike = r#"{
        "table":"player",
        "where":{"type":"simple","op":"ILIKE","left":{"type":"column","name":"username"},"right":{"type":"literal","value":"%HUNTER%"}}
    }"#;
    let (server2, frames2) = hydrate_ast(seed_world(), ilike).await;
    assert_has(&frames2, "p1_hunter", "ILIKE matches case-insensitively");
    assert_absent(&frames2, "p2_runner", "does not match %HUNTER%");
    server2.shutdown().await;
}

/// `getFriendProfile`, exceeded: top-level OR-of-EXISTS friend authorization
/// (visible only if a friendship exists either direction) COMBINED WITH a
/// `related` fan-out that returns the friend's players. The app does the auth
/// OR-of-exists but never also fans out related rows from the same root.
#[tokio::test]
async fn or_of_exists_authorization_with_related_output() {
    // target = bob(2); me = alice(1). ACCEPTED friendship (1->2) authorizes it.
    let ast = r#"{
        "table":"public_user",
        "where":{"type":"and","conditions":[
            {"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":2}},
            {"type":"or","conditions":[
                {"type":"correlatedSubquery","op":"EXISTS","related":{
                    "correlation":{"parentField":["id"],"childField":["receiverId"]},
                    "subquery":{"table":"friendship","where":{"type":"and","conditions":[
                        {"type":"simple","op":"=","left":{"type":"column","name":"senderId"},"right":{"type":"literal","value":1}},
                        {"type":"simple","op":"=","left":{"type":"column","name":"status"},"right":{"type":"literal","value":"ACCEPTED"}}
                    ]}}}},
                {"type":"correlatedSubquery","op":"EXISTS","related":{
                    "correlation":{"parentField":["id"],"childField":["senderId"]},
                    "subquery":{"table":"friendship","where":{"type":"and","conditions":[
                        {"type":"simple","op":"=","left":{"type":"column","name":"receiverId"},"right":{"type":"literal","value":1}},
                        {"type":"simple","op":"=","left":{"type":"column","name":"status"},"right":{"type":"literal","value":"ACCEPTED"}}
                    ]}}}}
            ]}
        ]},
        "related":[{"correlation":{"parentField":["id"],"childField":["userId"]},"subquery":{"table":"player"},"system":"client"}]
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    assert_has(&frames, "\"bob\"", "authorized friend profile hydrates");
    assert_has(&frames, "p2_runner", "friend's related player fans out");
    assert_absent(&frames, "\"carol\"", "non-target must not leak");
    server.shutdown().await;
}

/// `NOT EXISTS`: users who are NOT in any game (have no player row). Only carol,
/// dave, erin have players... actually all seeded users do — so seed a fresh
/// world where a user has no players and assert only they match.
#[tokio::test]
async fn not_exists_finds_users_without_players() {
    let replica = seed_sql(
        "notexists",
        &[
            "CREATE TABLE user (id INTEGER PRIMARY KEY, name TEXT)",
            "INSERT INTO user (id, name) VALUES (1,'playing'),(2,'idle')",
            "CREATE TABLE player (id INTEGER PRIMARY KEY, userId INTEGER, username TEXT)",
            "INSERT INTO player (id, userId, username) VALUES (1,1,'p_playing')",
        ],
    );
    let ast = r#"{
        "table":"user",
        "where":{"type":"correlatedSubquery","op":"NOT EXISTS","related":{
            "correlation":{"parentField":["id"],"childField":["userId"]},
            "subquery":{"table":"player"}
        }}
    }"#;
    let (server, frames) = hydrate_ast(replica, ast).await;
    assert_has(
        &frames,
        "\"idle\"",
        "user with no player matches NOT EXISTS",
    );
    assert_absent(&frames, "\"playing\"", "user with a player excluded");
    server.shutdown().await;
}

/// Self-referential player→player edge the app declares but never traverses:
/// `arrestedBy` (player.arrestedById → player.id). Hydrate dave and pull his
/// arrester's row via a `related` fan-out back into the same table.
#[tokio::test]
async fn self_referential_arrested_by_relation() {
    let ast = r#"{
        "table":"player",
        "where":{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":4}},
        "related":[{"correlation":{"parentField":["arrestedById"],"childField":["id"]},"subquery":{"table":"player"},"system":"client"}]
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    assert_has(&frames, "p4_left", "the arrested player (root)");
    assert_has(
        &frames,
        "p1_hunter",
        "the arrester, via self-referential related",
    );
    server.shutdown().await;
}

/// Reverse self-referential edge: `kicksMade` (player.id → player.kickedById).
/// Hydrate alice's player and pull the players she kicked.
#[tokio::test]
async fn self_referential_kicks_made_reverse_relation() {
    let ast = r#"{
        "table":"player",
        "where":{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1}},
        "related":[{"correlation":{"parentField":["id"],"childField":["kickedById"]},"subquery":{"table":"player"},"system":"client"}]
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    assert_has(&frames, "p1_hunter", "the kicker (root)");
    assert_has(
        &frames,
        "p5_kicked",
        "the kicked player, via reverse self-ref related",
    );
    server.shutdown().await;
}

/// Composite-PK, audience-discriminated correlated EXISTS (the crux of
/// `getLocation`): player_location rows whose owning player is a HUNTER.
#[tokio::test]
async fn composite_pk_location_hunter_owned_exists() {
    let ast = r#"{
        "table":"player_location",
        "where":{"type":"correlatedSubquery","op":"EXISTS","related":{
            "correlation":{"parentField":["playerId"],"childField":["id"]},
            "subquery":{"table":"player","where":{"type":"simple","op":"=","left":{"type":"column","name":"role"},"right":{"type":"literal","value":"HUNTER"}}}
        }}
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    // playerId=1 is a HUNTER → its location (11.1). playerId=2 is a RUNNER →
    // both its rows (22.2/33.3) excluded even though one has audience HUNTER.
    assert_has(&frames, "11.1", "HUNTER-owned location");
    assert_absent(
        &frames,
        "22.2",
        "RUNNER-owned location excluded (audience is not the owner's role)",
    );
    assert_absent(&frames, "33.3", "RUNNER-owned location excluded");
    server.shutdown().await;
}

/// Five-hop `related` chain — deeper than anything in the app (max 3–4):
/// user → players → game → messages → fromPlayer → publicUser.
#[tokio::test]
async fn five_hop_related_chain() {
    let ast = r#"{
        "table":"user",
        "where":{"type":"simple","op":"=","left":{"type":"column","name":"id"},"right":{"type":"literal","value":1}},
        "related":[{"correlation":{"parentField":["id"],"childField":["userId"]},"subquery":{
            "table":"player",
            "where":{"type":"simple","op":"=","left":{"type":"column","name":"gameId"},"right":{"type":"literal","value":1}},
            "related":[{"correlation":{"parentField":["gameId"],"childField":["id"]},"subquery":{
                "table":"game",
                "related":[{"correlation":{"parentField":["id"],"childField":["gameId"]},"subquery":{
                    "table":"message",
                    "related":[{"correlation":{"parentField":["fromPlayerId"],"childField":["id"]},"subquery":{
                        "table":"player",
                        "related":[{"correlation":{"parentField":["userId"],"childField":["id"]},"subquery":{"table":"public_user"},"system":"client"}]
                    },"system":"client"}]
                },"system":"client"}]
            },"system":"client"}]
        },"system":"client"}]
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    assert_has(&frames, "\"alice\"", "root user");
    assert_has(&frames, "msg_everyone", "4-hops-deep messages");
    // Deepest leaf: publicUser of a message's author (carol authored the
    // SUPERVISOR message; her public_user row must reach the client).
    assert_has(&frames, "msg_super", "message from the supervisor");
    server.shutdown().await;
}

// ============================================================================
// B. KNOWN-GAP tests — executable spec for shapes the builder can't yet handle.
//    These assert CORRECT semantics and are EXPECTED TO FAIL until the graph
//    path supports a correlated subquery under an OR nested inside an AND
//    (exactly getGameState / getGameMessages / getLocation). Do NOT "fix" by
//    weakening the assertions; fix by teaching the pipeline the shape.
// ============================================================================

/// `getGameState` core: game.related(players WHERE and(hasLeft IS NULL, or(
/// exists(game isStarted=false), exists(game whereExists(players where I am a
/// SUPERVISOR))))). The OR-of-EXISTS lives INSIDE an AND inside a related
/// subquery — the shape the current pipeline rejects.
#[tokio::test]
async fn game_state_visibility_or_of_exists_inside_and() {
    let ast = r#"{
        "table":"game",
        "where":{"type":"correlatedSubquery","op":"EXISTS","related":{
            "correlation":{"parentField":["id"],"childField":["gameId"]},
            "subquery":{"table":"player","where":{"type":"and","conditions":[
                {"type":"simple","op":"=","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":1}},
                {"type":"simple","op":"IS","left":{"type":"column","name":"hasLeft"},"right":{"type":"literal","value":null}}
            ]}}
        }},
        "related":[{"correlation":{"parentField":["id"],"childField":["gameId"]},"subquery":{
            "table":"player",
            "where":{"type":"and","conditions":[
                {"type":"simple","op":"IS","left":{"type":"column","name":"hasLeft"},"right":{"type":"literal","value":null}},
                {"type":"or","conditions":[
                    {"type":"correlatedSubquery","op":"EXISTS","related":{
                        "correlation":{"parentField":["gameId"],"childField":["id"]},
                        "subquery":{"table":"game","where":{"type":"simple","op":"=","left":{"type":"column","name":"isStarted"},"right":{"type":"literal","value":0}}}
                    }},
                    {"type":"correlatedSubquery","op":"EXISTS","related":{
                        "correlation":{"parentField":["gameId"],"childField":["id"]},
                        "subquery":{"table":"game","where":{"type":"correlatedSubquery","op":"EXISTS","related":{
                            "correlation":{"parentField":["id"],"childField":["gameId"]},
                            "subquery":{"table":"player","where":{"type":"and","conditions":[
                                {"type":"simple","op":"=","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":1}},
                                {"type":"simple","op":"=","left":{"type":"column","name":"role"},"right":{"type":"literal","value":"SUPERVISOR"}}
                            ]}}
                        }}}}
                    }
                ]}
            ]},
            "orderBy":[["createdAt","asc"]]
        },"system":"client"}]
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    // alice is a HUNTER in g1 (not a supervisor) and g1 is started, so the
    // first OR branch (isStarted=false) is false and the second (I am a
    // supervisor) is false → NO active players in g1 pass the relation filter.
    // But g2 IS not started, so in g2 alice's fellow players would be visible.
    // The observable contract: the query completes and hydrates the game roots
    // without dropping the connection. Here we assert the g2 (not-started)
    // branch surfaces its player.
    assert_has(
        &frames,
        "p6_g2",
        "not-started game exposes players via the OR branch",
    );
    server.shutdown().await;
}

/// `getGameMessages`: 5-branch OR mixing a plain cmp with three
/// and(cmp, exists→whereExists) role branches — the team-visibility tree. Same
/// or-of-exists-inside-and shape at top level on `message`.
#[tokio::test]
async fn game_messages_team_visibility_tree() {
    // me = alice(1), a HUNTER in g1. She should see EVERYONE + HUNTER + her own
    // messages, but NOT RUNNER- or SUPERVISOR-only messages.
    let ast = r#"{
        "table":"message",
        "where":{"type":"and","conditions":[
            {"type":"correlatedSubquery","op":"EXISTS","related":{
                "correlation":{"parentField":["gameId"],"childField":["id"]},
                "subquery":{"table":"game","where":{"type":"correlatedSubquery","op":"EXISTS","related":{
                    "correlation":{"parentField":["id"],"childField":["gameId"]},
                    "subquery":{"table":"player","where":{"type":"and","conditions":[
                        {"type":"simple","op":"=","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":1}},
                        {"type":"simple","op":"IS","left":{"type":"column","name":"hasLeft"},"right":{"type":"literal","value":null}}
                    ]}}
                }}}
            }},
            {"type":"or","conditions":[
                {"type":"simple","op":"=","left":{"type":"column","name":"toTeam"},"right":{"type":"literal","value":"EVERYONE"}},
                {"type":"correlatedSubquery","op":"EXISTS","related":{
                    "correlation":{"parentField":["fromPlayerId"],"childField":["id"]},
                    "subquery":{"table":"player","where":{"type":"simple","op":"=","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":1}}}
                }},
                {"type":"and","conditions":[
                    {"type":"simple","op":"=","left":{"type":"column","name":"toTeam"},"right":{"type":"literal","value":"HUNTER"}},
                    {"type":"correlatedSubquery","op":"EXISTS","related":{
                        "correlation":{"parentField":["gameId"],"childField":["id"]},
                        "subquery":{"table":"game","where":{"type":"correlatedSubquery","op":"EXISTS","related":{
                            "correlation":{"parentField":["id"],"childField":["gameId"]},
                            "subquery":{"table":"player","where":{"type":"and","conditions":[
                                {"type":"simple","op":"=","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":1}},
                                {"type":"simple","op":"=","left":{"type":"column","name":"role"},"right":{"type":"literal","value":"HUNTER"}}
                            ]}}
                        }}}
                    }}
                ]},
                {"type":"and","conditions":[
                    {"type":"simple","op":"=","left":{"type":"column","name":"toTeam"},"right":{"type":"literal","value":"RUNNER"}},
                    {"type":"correlatedSubquery","op":"EXISTS","related":{
                        "correlation":{"parentField":["gameId"],"childField":["id"]},
                        "subquery":{"table":"game","where":{"type":"correlatedSubquery","op":"EXISTS","related":{
                            "correlation":{"parentField":["id"],"childField":["gameId"]},
                            "subquery":{"table":"player","where":{"type":"and","conditions":[
                                {"type":"simple","op":"=","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":1}},
                                {"type":"simple","op":"=","left":{"type":"column","name":"role"},"right":{"type":"literal","value":"RUNNER"}}
                            ]}}
                        }}}
                    }}
                ]}
            ]}
        ]},
        "orderBy":[["time","desc"]]
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    assert_has(
        &frames,
        "msg_everyone",
        "EVERYONE messages visible to all members",
    );
    assert_has(
        &frames,
        "msg_hunters",
        "HUNTER messages visible to a hunter",
    );
    assert_absent(
        &frames,
        "msg_super",
        "SUPERVISOR-only messages hidden from a hunter",
    );
    // alice is a HUNTER in g1, so the RUNNER branch is false; the only RUNNER
    // message (msg_runners) is not from her, so it must be hidden.
    assert_absent(
        &frames,
        "msg_runners",
        "RUNNER-only messages hidden from a hunter",
    );
    server.shutdown().await;
}

/// Ordering must be reflected in the ROW ORDER delivered to the client, not just
/// which rows. The app relies on this for every `orderBy(...).limit(...)` list.
/// This asserts the poke delivers newest-first; it documents the gap if the
/// builder emits PK order instead of the query's orderBy.
#[tokio::test]
async fn order_by_desc_reflected_in_delivered_row_order() {
    let ast = r#"{
        "table":"progression_event",
        "where":{"type":"and","conditions":[
            {"type":"simple","op":"=","left":{"type":"column","name":"userId"},"right":{"type":"literal","value":1}},
            {"type":"simple","op":"=","left":{"type":"column","name":"kind"},"right":{"type":"literal","value":"XP"}}
        ]},
        "orderBy":[["createdAt","desc"]]
    }"#;
    let (server, frames) = hydrate_ast(seed_world(), ast).await;
    // Expect streak(50) to be delivered before kill_a(10) in the frame stream.
    let pos_streak = frames.find("ev_streak");
    let pos_first = frames.find("ev_kill_a");
    assert!(
        matches!((pos_streak, pos_first), (Some(s), Some(f)) if s < f),
        "orderBy desc must deliver newest (ev_streak) before oldest (ev_kill_a)\n--- frames ---\n{frames}"
    );
    server.shutdown().await;
}

// ============================================================================
// C. Live fan-out through a complex query in a shared client group.
//    Needs a test Postgres for the CVR (ZERO_TEST_PG_URL / scripts/test.sh
//    --with-pg); skips gracefully without one.
// ============================================================================

fn test_pg_url() -> String {
    std::env::var("ZERO_TEST_PG_URL")
        .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into())
}

/// Seeds a production-shaped replica (with the `_0_version` column the
/// replicator adds) carrying a game + two players, so the live-commit path sees
/// production-shaped rows.
fn seed_live_replica(tag: &str) -> String {
    let path = std::env::temp_dir().join(format!("zc_hg_live_{}_{tag}.db", std::process::id()));
    let path = path.to_str().unwrap().to_string();
    let _ = std::fs::remove_file(&path);
    let db = StatementRunner::open_file(&path).unwrap();
    init_replica_metadata(&db);
    db.exec("CREATE TABLE game (id INTEGER PRIMARY KEY, joinCode TEXT, _0_version TEXT NOT NULL)")
        .unwrap();
    db.exec("INSERT INTO game (id, joinCode, _0_version) VALUES (1, 'ABC', '00')")
        .unwrap();
    db.exec("CREATE TABLE player (id INTEGER PRIMARY KEY, username TEXT, gameId INTEGER, _0_version TEXT NOT NULL)")
        .unwrap();
    db.exec("INSERT INTO player (id, username, gameId, _0_version) VALUES (1, 'p1_start', 1, '00'), (2, 'p2_start', 1, '00')")
        .unwrap();
    drop(db);
    path
}

/// Applies a committed player-username update and fans it out exactly as the
/// replicator does.
fn commit_player_username(
    replica: &str,
    service: &SyncService,
    id: i64,
    username: &str,
    version: &str,
) {
    let db = StatementRunner::open_file(replica).unwrap();
    db.exec(&format!(
        "UPDATE player SET username = '{username}', _0_version = '{version}' WHERE id = {id}"
    ))
    .unwrap();
    ChangeLog::new(&db)
        .log_set_op(
            version,
            0,
            "player",
            &vec![("id".to_string(), JsonValue::Number(id as f64))],
            None,
        )
        .unwrap();
    update_replication_watermark(&db, version).unwrap();
    drop(db);
    service.publish_commit(version, false, 1);
}

async fn pump_until(
    ws: &mut Client,
    log: &mut Vec<String>,
    what: &str,
    pred: impl Fn(&str) -> bool,
) {
    if log.iter().any(|f| pred(f)) {
        return;
    }
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap_or_else(|_| {
                panic!("timed out waiting for {what}\nframes:\n{}", log.join("\n"))
            });
        let Some(Ok(Message::Text(t))) = frame else {
            continue;
        };
        let t = t.to_string();
        let done = pred(&t);
        log.push(t);
        if done {
            return;
        }
    }
}

/// Two clients in one group desire a game.related(players) query. A live commit
/// updates a related child row; BOTH clients must receive the update through the
/// shared pipeline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_commit_propagates_through_related_query_to_group() {
    let connection_string = test_pg_url();
    let Ok(cvr_client) = zero_cache_change_source::pg_connection::connect(&connection_string).await
    else {
        eprintln!("skipping live group test: no test Postgres available");
        return;
    };
    let shard = zero_cache_types::shards::ShardId {
        app_id: "hglive".into(),
        shard_num: 0,
    };
    cvr_client
        .batch_execute("DROP SCHEMA IF EXISTS \"hglive_0/cvr\" CASCADE;")
        .await
        .unwrap();
    for statement in
        zero_cache_view_syncer::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap()
    {
        cvr_client.batch_execute(&statement).await.unwrap();
    }

    let replica = seed_live_replica("related");
    let deps = HandlerDeps {
        cvr: Some(CvrRuntimeConfig {
            connection_string: connection_string.clone(),
            max_connections: 8,
            shard: shard.clone(),
            task_id: "hglive-task".into(),
        }),
        group_ownership: Some(true),
        ..Default::default()
    };
    let server = Server::boot(replica.clone(), deps).await;

    let ast = r#"{"table":"game","related":[{"correlation":{"parentField":["id"],"childField":["gameId"]},"subquery":{"table":"player","orderBy":[["id","asc"]]},"system":"client"}]}"#;

    let mut a = connect_ast(server.addr, "hg", "ca", "q-game", ast).await;
    let mut b = connect_ast(server.addr, "hg", "cb", "q-game", ast).await;
    let (mut la, mut lb) = (Vec::new(), Vec::new());
    pump_until(&mut a, &mut la, "a initial roster", |t| {
        t.contains("p2_start")
    })
    .await;
    pump_until(&mut b, &mut lb, "b initial roster", |t| {
        t.contains("p2_start")
    })
    .await;

    // Live commit updates a related child row.
    commit_player_username(&replica, &server.service, 2, "p2_renamed", "01");
    pump_until(&mut a, &mut la, "a sees child update", |t| {
        t.contains("p2_renamed")
    })
    .await;
    pump_until(&mut b, &mut lb, "b sees child update", |t| {
        t.contains("p2_renamed")
    })
    .await;

    for log in [&la, &lb] {
        for frame in log {
            assert!(
                !frame.starts_with("[\"error\""),
                "server error frame: {frame}"
            );
            assert!(
                !frame.contains("concurrent modification"),
                "CVR conflict leaked: {frame}"
            );
        }
    }

    server.shutdown().await;
    cvr_client
        .batch_execute("DROP SCHEMA \"hglive_0/cvr\" CASCADE;")
        .await
        .unwrap();
}
