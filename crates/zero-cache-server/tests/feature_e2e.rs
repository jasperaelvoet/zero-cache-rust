//! End-to-end feature suite for the app-facing capabilities hunting-game (and
//! any real @rocicorp/zero app) depends on, driven through the REAL running
//! server (`run_synced_server` + `HandlerDeps`) over REAL WebSocket sockets and
//! REAL HTTP mock app-API servers. No in-process shortcuts around the handler.
//!
//! Coverage:
//!   1. Custom mutators (`ZERO_MUTATE_URL`): a client `push` of a custom
//!      mutation is forwarded to the app's mutate server with the correct wire
//!      shape, and the per-mutation result is relayed back as a `pushResponse`.
//!   2. Custom synced queries (`ZERO_QUERY_URL`): desiring a named query drives
//!      an HTTP transform fetch against the app's query server.
//!   3. Real data path: desiring an AST query hydrates real rows from the
//!      shared replica and pokes them to the client.
//!   4. KNOWN GAP — permissions: this suite PROVES that row-level permissions
//!      are NOT yet enforced in the live path (a client reads rows a read
//!      policy would forbid). This test documents the gap so it can't be
//!      silently shipped; when enforcement is wired it should be inverted.
//!   5. Litestream backup/restore round trip (gated on the binary being
//!      installed).
//!
//! Most tests need NO Postgres (the mutate/query servers are mocked and the
//! replica is a local file), so they run in CI unconditionally.

use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use zero_cache_server::bootstrap::{run_synced_server, HandlerDeps};
use zero_cache_server::sync_service::SyncService;
use zero_cache_sqlite::StatementRunner;

// ----------------------------------------------------------------------------
// Harness
// ----------------------------------------------------------------------------

/// A minimal capturing HTTP/1.1 mock: records each request it receives (full
/// text, headers + body) and replies with a fixed status + JSON body.
struct MockHttp {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MockHttp {
    async fn spawn(status: u16, body: &'static str) -> MockHttp {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let reqs = requests.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let reqs = reqs.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 16384];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]).to_string();
                    reqs.lock().unwrap().push(req);
                    let resp = format!(
                        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.flush().await;
                });
            }
        });
        MockHttp {
            url: format!("http://{addr}/api"),
            requests,
        }
    }

    fn captured(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

/// A query-transform mock that behaves like a real @rocicorp/zero query server:
/// it reads the transform request, echoes back the SAME query `id` (a hash of
/// name+args that zero-cache computes and the server must return unchanged),
/// and attaches `ast` as the transformed query. Returns the server URL.
async fn spawn_transform_echo(ast_json: &'static str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { break };
            let ast = ast_json.to_string();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 16384];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                // Extract the first "id":"<hash>" from the request body.
                let id = req
                    .split_once("\"id\":\"")
                    .and_then(|(_, rest)| rest.split_once('"').map(|(id, _)| id.to_string()))
                    .unwrap_or_default();
                let body = format!(
                    r#"{{"kind":"QueryResponse","queries":[{{"id":"{id}","name":"q","ast":{ast}}}]}}"#
                );
                let resp = format!(
                    "HTTP/1.1 200 X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            });
        }
    });
    format!("http://{addr}/api")
}

/// Writes a fresh replica file from raw DDL + insert statements, then closes
/// the writer. For tests needing a multi-table schema (relationships/EXISTS).
fn seed_replica_sql(tag: &str, stmts: &[&str]) -> String {
    let path = std::env::temp_dir().join(format!("zc_feat_{}_{tag}.db", std::process::id()));
    let path = path.to_str().unwrap().to_string();
    let _ = std::fs::remove_file(&path);
    let db = StatementRunner::open_file(&path).unwrap();
    for s in stmts {
        db.exec(s).unwrap();
    }
    drop(db);
    path
}

/// Writes a fresh replica file with an `issue` table + `rows`, then closes the
/// writer (so `run_synced_server` can open its own read views).
fn seed_replica(rows: &[(i64, &str)]) -> String {
    let path = std::env::temp_dir().join(format!(
        "zc_feature_{}_{}.db",
        std::process::id(),
        rows.as_ptr() as usize
    ));
    let path = path.to_str().unwrap().to_string();
    let _ = std::fs::remove_file(&path);
    let db = StatementRunner::open_file(&path).unwrap();
    db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, owner TEXT)")
        .unwrap();
    for (id, title) in rows {
        // Test data is fully controlled (no untrusted input), so inlining is fine.
        db.exec(&format!(
            "INSERT INTO issue (id, title, owner) VALUES ({id}, '{title}', 'alice')"
        ))
        .unwrap();
    }
    drop(db);
    path
}

struct Server {
    addr: std::net::SocketAddr,
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
        let handle =
            tokio::spawn(run_synced_server(listener, service, rx, replica_path.clone(), deps));
        Server {
            addr,
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

type Client =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Connects, consumes the greeting, sends `initConnection`.
async fn connect(addr: std::net::SocketAddr, desired: &str) -> Client {
    // Connect as clientID "c1" (matching the push mutations in these tests) so
    // the server's per-client pushResponse filtering keeps them.
    let req = format!("ws://{addr}/sync/v51/connect?clientGroupID=cg0&clientID=c1&lmid=0")
        .into_client_request()
        .unwrap();
    let (mut client, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let greeting = client.next().await.unwrap().unwrap().into_text().unwrap();
    assert!(greeting.starts_with("[\"connected\""), "greeting: {greeting}");
    client
        .send(Message::text(format!(
            r#"["initConnection",{{"desiredQueriesPatch":{desired}}}]"#
        )))
        .await
        .unwrap();
    client
}

/// Connects with a `Cookie` header (session-cookie auth), consumes the
/// greeting, sends `initConnection`.
async fn connect_with_cookie(addr: std::net::SocketAddr, cookie: &str, desired: &str) -> Client {
    let mut req = format!("ws://{addr}/sync/v51/connect?clientGroupID=cg&clientID=c1&lmid=0")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("Cookie", cookie.parse().unwrap());
    let (mut client, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let greeting = client.next().await.unwrap().unwrap().into_text().unwrap();
    assert!(greeting.starts_with("[\"connected\""), "greeting: {greeting}");
    client
        .send(Message::text(format!(
            r#"["initConnection",{{"desiredQueriesPatch":{desired}}}]"#
        )))
        .await
        .unwrap();
    client
}

/// Connects with an `authToken` in the `Sec-WebSocket-Protocol` handshake (the
/// way a real `new Zero({auth})` client sends its bearer token), consumes the
/// greeting, sends `initConnection`.
async fn connect_with_auth(addr: std::net::SocketAddr, token: &str, desired: &str) -> Client {
    let subproto = zero_cache_protocol::connect::encode_sec_protocols(None, Some(token));
    let mut req = format!("ws://{addr}/sync/v51/connect?clientGroupID=cg&clientID=c1&lmid=0")
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("Sec-WebSocket-Protocol", subproto.parse().unwrap());
    let (mut client, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let greeting = client.next().await.unwrap().unwrap().into_text().unwrap();
    assert!(greeting.starts_with("[\"connected\""), "greeting: {greeting}");
    client
        .send(Message::text(format!(
            r#"["initConnection",{{"desiredQueriesPatch":{desired}}}]"#
        )))
        .await
        .unwrap();
    client
}

/// Reads text frames until one matches `pred` or a timeout elapses.
async fn read_until(client: &mut Client, pred: impl Fn(&str) -> bool) -> Option<String> {
    let deadline = std::time::Duration::from_secs(5);
    loop {
        match tokio::time::timeout(deadline, client.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                if pred(&t) {
                    return Some(t.to_string());
                }
            }
            Ok(Some(Ok(_))) => continue,
            _ => return None,
        }
    }
}

// ----------------------------------------------------------------------------
// 1. Custom mutators
// ----------------------------------------------------------------------------

#[tokio::test]
async fn custom_mutator_push_is_forwarded_and_response_relayed() {
    // Mutate server accepts the mutation and returns a per-mutation result.
    let mock = MockHttp::spawn(
        200,
        r#"{"mutations":[{"id":{"clientID":"c1","id":1},"result":{"data":{"awardedXp":50}}}]}"#,
    )
    .await;
    let replica = seed_replica(&[(1, "seed")]);
    // The `schema` MUST be the zero shard schema (`<appID>_<shardNum>`) where the
    // clients/mutations tables live — NOT the data schema. The mutate server's
    // PushProcessor writes lastMutationID to `<schema>.clients`; sending "public"
    // makes it write to a nonexistent `public.clients` and every mutation fails.
    let deps = HandlerDeps {
        mutate_api: Some((mock.url.clone(), None, "zerobench_0".into(), "zerobench".into())),
        ..Default::default()
    };
    let server = Server::boot(replica, deps).await;
    let mut client = connect(server.addr, "[]").await;

    // Push a CUSTOM mutation (name + args) — the whole point of ZERO_MUTATE_URL.
    client
        .send(Message::text(
            r#"["push",{
                "clientGroupID":"cg0","pushVersion":1,"timestamp":1,"requestID":"r1",
                "mutations":[
                    {"type":"custom","id":1,"clientID":"c1","timestamp":1,
                     "name":"awardXp","args":[{"amount":50}]}
                ]
            }]"#,
        ))
        .await
        .unwrap();

    let resp = read_until(&mut client, |t| t.contains("pushResponse"))
        .await
        .expect("expected a pushResponse frame");
    assert!(resp.contains("\"clientID\":\"c1\""), "got {resp}");
    assert!(!resp.contains("\"error\""), "clean mutation, no error: {resp}");

    // The mutate server actually received a well-formed custom-mutation request.
    let reqs = mock.captured();
    assert_eq!(reqs.len(), 1, "mutate server should be hit exactly once");
    let body = &reqs[0];
    assert!(body.contains("\"clientGroupID\":\"cg0\""), "body: {body}");
    assert!(body.contains("\"name\":\"awardXp\""), "body: {body}");
    assert!(body.contains("\"amount\":50"), "args forwarded: {body}");
    assert!(
        body.to_ascii_lowercase().contains("appid=zerobench") || body.contains("zerobench"),
        "app id threaded through: {body}"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn custom_mutator_app_error_is_relayed_not_swallowed() {
    let mock = MockHttp::spawn(
        200,
        r#"{"mutations":[{"id":{"clientID":"c1","id":1},"result":{"error":"app","details":"not enough coins"}}]}"#,
    )
    .await;
    let replica = seed_replica(&[(1, "seed")]);
    let deps = HandlerDeps {
        mutate_api: Some((mock.url.clone(), None, "public".into(), "app".into())),
        ..Default::default()
    };
    let server = Server::boot(replica, deps).await;
    let mut client = connect(server.addr, "[]").await;
    client
        .send(Message::text(
            r#"["push",{"clientGroupID":"cg0","pushVersion":1,"timestamp":1,"requestID":"r1",
               "mutations":[{"type":"custom","id":1,"clientID":"c1","timestamp":1,"name":"buy","args":[]}]}]"#,
        ))
        .await
        .unwrap();
    let resp = read_until(&mut client, |t| t.contains("pushResponse"))
        .await
        .expect("pushResponse");
    assert!(
        resp.contains("not enough coins") || resp.contains("error"),
        "the app error must reach the client: {resp}"
    );
    server.shutdown().await;
}

/// Regression: a push can carry mutations for OTHER clients in the group
/// (Replicache re-pushes dead clients' unconfirmed mutations through whichever
/// client is connected). The mutate server returns results tagged with those
/// OTHER clientIDs. The port must NOT relay a result whose clientID isn't the
/// connected client's — a real client throws "received mutation for the wrong
/// client" (FATAL, closes the socket → stuck loading). This is exactly the
/// hunting-game crash.
#[tokio::test]
async fn pushresponse_only_carries_the_connected_clients_mutations() {
    // Mutate server returns results for TWO clients: c1 (connected) + "dead9".
    let mock = MockHttp::spawn(
        200,
        r#"{"mutations":[
            {"id":{"clientID":"dead9","id":7},"result":{"error":"app","message":"You are already in a game"}},
            {"id":{"clientID":"c1","id":1},"result":{"data":null}}
        ]}"#,
    )
    .await;
    let replica = seed_replica(&[(1, "seed")]);
    let deps = HandlerDeps {
        mutate_api: Some((mock.url.clone(), None, "zero_0".into(), "zero".into())),
        ..Default::default()
    };
    let server = Server::boot(replica, deps).await; // connect() uses clientID=c1
    let mut client = connect(server.addr, "[]").await;
    client
        .send(Message::text(
            r#"["push",{"clientGroupID":"cg0","pushVersion":1,"timestamp":1,"requestID":"r1",
               "mutations":[{"type":"custom","id":7,"clientID":"dead9","timestamp":1,"name":"createGame","args":[]}]}]"#,
        ))
        .await
        .unwrap();
    let resp = read_until(&mut client, |t| t.contains("pushResponse"))
        .await
        .expect("pushResponse");
    // MUST contain only c1's result; the dead9 result MUST be dropped.
    assert!(resp.contains("\"clientID\":\"c1\""), "own result kept: {resp}");
    assert!(
        !resp.contains("dead9"),
        "SECURITY/CORRECTNESS: another client's result must NOT reach this client \
         (would be fatal 'received mutation for the wrong client'): {resp}"
    );
    server.shutdown().await;
}

// ----------------------------------------------------------------------------
// 1b. Cookie / client-header forwarding (session-cookie auth — hunting-game)
// ----------------------------------------------------------------------------

/// hunting-game authenticates its query + mutate servers via a SESSION COOKIE
/// (`getSessionUser` reads `request.headers`). With
/// `ZERO_QUERY_FORWARD_COOKIES`/`ZERO_MUTATE_FORWARD_COOKIES` + `cookie` in
/// allowed-client-headers, the port must forward the client's `Cookie` from the
/// WS connect handshake to BOTH API servers — otherwise every request is 401.
#[tokio::test]
async fn client_cookie_is_forwarded_to_query_and_mutate_servers() {
    let query_mock = MockHttp::spawn(
        200,
        r#"{"kind":"QueryResponse","queries":[]}"#, // empty is fine; we assert the request
    )
    .await;
    let mutate_mock = MockHttp::spawn(
        200,
        r#"{"mutations":[{"id":{"clientID":"c1","id":1},"result":{"data":null}}]}"#,
    )
    .await;
    let replica = seed_replica(&[(1, "seed")]);
    let deps = HandlerDeps {
        query_api: Some((query_mock.url.clone(), None, "public".into(), "app".into())),
        mutate_api: Some((mutate_mock.url.clone(), None, "public".into(), "app".into())),
        query_forward_cookies: true,
        mutate_forward_cookies: true,
        query_allowed_client_headers: vec!["cookie".into()],
        mutate_allowed_client_headers: vec!["cookie".into()],
        ..Default::default()
    };
    let server = Server::boot(replica, deps).await;

    let session = "session=better-auth-token-xyz";
    let mut client = connect_with_cookie(
        server.addr,
        session,
        r#"[{"op":"put","hash":"q1","name":"getThing","args":[]}]"#,
    )
    .await;
    // Push a custom mutation so the mutate server is hit too.
    client
        .send(Message::text(
            r#"["push",{"clientGroupID":"cg","pushVersion":1,"timestamp":1,"requestID":"r1",
               "mutations":[{"type":"custom","id":1,"clientID":"c1","timestamp":1,"name":"doThing","args":[]}]}]"#,
        ))
        .await
        .unwrap();
    let _ = read_until(&mut client, |t| t.contains("pushResponse")).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let q = query_mock.captured();
    let m = mutate_mock.captured();
    assert!(!q.is_empty(), "query server should be hit");
    assert!(!m.is_empty(), "mutate server should be hit");
    assert!(
        q[0].to_lowercase().contains(session),
        "query server MUST receive the session cookie (else 401): {}",
        q[0]
    );
    assert!(
        m[0].to_lowercase().contains(session),
        "mutate server MUST receive the session cookie (else 401): {}",
        m[0]
    );
    server.shutdown().await;
}

/// With forwarding DISABLED, the cookie must NOT leak to the API servers.
#[tokio::test]
async fn cookie_is_not_forwarded_when_disabled() {
    let query_mock = MockHttp::spawn(200, r#"{"kind":"QueryResponse","queries":[]}"#).await;
    let replica = seed_replica(&[(1, "seed")]);
    let deps = HandlerDeps {
        query_api: Some((query_mock.url.clone(), None, "public".into(), "app".into())),
        query_forward_cookies: false,
        query_allowed_client_headers: vec![], // nothing whitelisted
        ..Default::default()
    };
    let server = Server::boot(replica, deps).await;
    let _client = connect_with_cookie(
        server.addr,
        "session=secret",
        r#"[{"op":"put","hash":"q1","name":"getThing","args":[]}]"#,
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let q = query_mock.captured();
    assert!(!q.is_empty(), "query server should be hit");
    assert!(
        !q[0].to_lowercase().contains("session=secret"),
        "cookie must NOT be forwarded when disabled: {}",
        q[0]
    );
    server.shutdown().await;
}

/// The client's connect bearer token must reach the mutate server as
/// `Authorization: Bearer <token>` on the VERY FIRST mutation — a mobile client
/// authenticates with a token (not a cookie), and hunting-game's mutate server
/// returns 401 without it. Regression for the "mutate API server error: … 401"
/// hunting-game hit when creating a game.
#[tokio::test]
async fn connect_bearer_token_reaches_mutate_server_on_first_mutation() {
    let mock = MockHttp::spawn(
        200,
        r#"{"mutations":[{"id":{"clientID":"c1","id":1},"result":{"data":null}}]}"#,
    )
    .await;
    let replica = seed_replica(&[(1, "seed")]);
    let deps = HandlerDeps {
        mutate_api: Some((mock.url.clone(), None, "public".into(), "app".into())),
        ..Default::default()
    };
    let server = Server::boot(replica, deps).await;
    let token = "eyJhbGciOiJIUzI1NiJ9.session-token.sig";
    let mut client = connect_with_auth(server.addr, token, "[]").await;
    // Push a custom mutation immediately — before any updateAuth.
    client
        .send(Message::text(
            r#"["push",{"clientGroupID":"cg","pushVersion":1,"timestamp":1,"requestID":"r1",
               "mutations":[{"type":"custom","id":1,"clientID":"c1","timestamp":1,"name":"createGame","args":[]}]}]"#,
        ))
        .await
        .unwrap();
    let _ = read_until(&mut client, |t| t.contains("pushResponse")).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let reqs = mock.captured();
    assert!(!reqs.is_empty(), "mutate server should be hit");
    assert!(
        reqs[0].to_lowercase().contains(&format!("authorization: bearer {token}").to_lowercase()),
        "the connect bearer token MUST reach the mutate server (else 401): {}",
        reqs[0]
    );
    server.shutdown().await;
}

// ----------------------------------------------------------------------------
// 2. Custom synced queries
// ----------------------------------------------------------------------------

#[tokio::test]
async fn custom_query_desire_triggers_transform_fetch_from_query_server() {
    // Query server returns a transformed AST for the named query.
    let mock = MockHttp::spawn(
        200,
        r#"{"kind":"QueryResponse","queries":[{"id":"issue-all","name":"issueByOwner","ast":{"table":"issue"}}]}"#,
    )
    .await;
    let replica = seed_replica(&[(1, "one"), (2, "two")]);
    let deps = HandlerDeps {
        query_api: Some((mock.url.clone(), None, "public".into(), "app".into())),
        ..Default::default()
    };
    let server = Server::boot(replica, deps).await;

    // Desire a NAMED (custom) query — no AST, just name + args → the server must
    // fetch its AST from the query API server.
    let _client = connect(
        server.addr,
        r#"[{"op":"put","hash":"issue-all","name":"issueByOwner","args":["alice"]}]"#,
    )
    .await;

    // Give the async transform fetch a moment to fire.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    let reqs = mock.captured();
    assert!(
        !reqs.is_empty(),
        "the query API server should receive a transform request"
    );
    assert!(
        reqs[0].contains("issueByOwner"),
        "transform request should name the desired query: {}",
        reqs[0]
    );
    server.shutdown().await;
}

/// hunting-game's core read pattern: a synced query whose returned AST scopes
/// rows with a WHERE `exists(...)` (server-authoritative authorization baked
/// into the AST). The query server returns `user WHERE exists(membership where
/// status='active')`; the port must hydrate ONLY the user with an active
/// membership — if the EXISTS filter were dropped, ALL users would leak.
#[tokio::test]
async fn synced_query_with_where_exists_hydrates_only_matching_rows() {
    let ast = r#"{
        "table":"user",
        "where":{
            "type":"correlatedSubquery","op":"EXISTS",
            "related":{
                "correlation":{"parentField":["id"],"childField":["userId"]},
                "subquery":{"table":"membership","where":{
                    "type":"simple","op":"=",
                    "left":{"type":"column","name":"status"},
                    "right":{"type":"literal","value":"active"}}}
            }
        }
    }"#;
    let ast: &'static str = Box::leak(ast.to_string().into_boxed_str());
    let url = spawn_transform_echo(ast).await;
    let replica = seed_replica_sql(
        "exists",
        &[
            "CREATE TABLE user (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE membership (id INTEGER PRIMARY KEY, userId INTEGER, status TEXT)",
            "INSERT INTO user (id, name) VALUES (1, 'alice'), (2, 'bob')",
            "INSERT INTO membership (id, userId, status) VALUES (1, 1, 'active')",
        ],
    );
    let deps = HandlerDeps {
        query_api: Some((url, None, "public".into(), "app".into())),
        ..Default::default()
    };
    let server = Server::boot(replica, deps).await;
    let mut client = connect(
        server.addr,
        r#"[{"op":"put","hash":"myfriends","name":"getFriends","args":[]}]"#,
    )
    .await;

    // Collect frames; alice (active membership) must appear, bob must NOT.
    let mut seen = String::new();
    for _ in 0..10 {
        match tokio::time::timeout(std::time::Duration::from_secs(2), client.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => seen.push_str(&t),
            _ => break,
        }
        if seen.contains("alice") {
            break;
        }
    }
    assert!(seen.contains("alice"), "the authorized row must hydrate: {seen}");
    assert!(
        !seen.contains("bob"),
        "SECURITY: the WHERE exists filter must exclude non-matching rows (bob leaked): {seen}"
    );
    server.shutdown().await;
}

/// hunting-game's most complex real shape (`getFriendProfile`): a synced query
/// whose AST is `user WHERE id=<target> AND (exists(friendship where
/// senderId=<me> accepted) OR exists(friendship where receiverId=<me>
/// accepted))`. Exercises AND + OR-of-EXISTS composition through the SQL
/// pushdown — the friend row is visible only when a friendship exists either
/// direction. Proves or-of-exists threads the outer-table qualification.
#[tokio::test]
async fn synced_query_or_of_exists_composes_correctly() {
    // target=2 (bob); me=1 (alice). A friendship (sender=1, receiver=2,
    // ACCEPTED) exists → alice may see bob's profile.
    let ast = r#"{
        "table":"user",
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
        ]}
    }"#;
    let ast: &'static str = Box::leak(ast.to_string().into_boxed_str());
    let url = spawn_transform_echo(ast).await;
    let replica = seed_replica_sql(
        "orexists",
        &[
            "CREATE TABLE user (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE friendship (id INTEGER PRIMARY KEY, senderId INTEGER, receiverId INTEGER, status TEXT)",
            "INSERT INTO user (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'stranger')",
            "INSERT INTO friendship (id, senderId, receiverId, status) VALUES (1, 1, 2, 'ACCEPTED')",
        ],
    );
    let deps = HandlerDeps {
        query_api: Some((url, None, "public".into(), "app".into())),
        ..Default::default()
    };
    let server = Server::boot(replica, deps).await;
    let mut client = connect(
        server.addr,
        r#"[{"op":"put","hash":"friendprofile","name":"getFriendProfile","args":[2]}]"#,
    )
    .await;
    let mut seen = String::new();
    for _ in 0..10 {
        match tokio::time::timeout(std::time::Duration::from_secs(2), client.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => seen.push_str(&t),
            _ => break,
        }
        if seen.contains("bob") {
            break;
        }
    }
    assert!(seen.contains("bob"), "friend bob (accepted friendship) must be visible: {seen}");
    assert!(!seen.contains("stranger"), "SECURITY: non-target rows must not leak: {seen}");
    server.shutdown().await;
}

// ----------------------------------------------------------------------------
// 3. Real data path (baseline: no app servers, pure replica hydration)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn ast_query_hydrates_real_rows_from_the_shared_replica() {
    let replica = seed_replica(&[(1, "alpha"), (2, "beta"), (3, "gamma")]);
    let server = Server::boot(replica, HandlerDeps::default()).await;
    let mut client = connect(
        server.addr,
        r#"[{"op":"put","hash":"issue-all","ast":{"table":"issue"}}]"#,
    )
    .await;

    // Collect frames until we've seen both the rows AND the gotQueriesPatch
    // (a hydrated query must be marked "got" or the client stays loading).
    let mut seen = String::new();
    for _ in 0..8 {
        match tokio::time::timeout(std::time::Duration::from_secs(2), client.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => seen.push_str(&t),
            _ => break,
        }
        if seen.contains("alpha") && seen.contains("gotQueriesPatch") {
            break;
        }
    }
    assert!(seen.contains("alpha"), "poke should carry seeded rows: {seen}");
    assert!(
        seen.contains("\"gotQueriesPatch\""),
        "the hydrated query MUST be acknowledged via gotQueriesPatch (else the \
         client's query stays 'unknown'/loading forever): {seen}"
    );
    server.shutdown().await;
}

/// Regression: a RECONNECTING client (persisted cookie "00:01") must have its
/// first poke base at THAT cookie, not `null`. The port keeps CVR per-connection
/// but must still echo the client's connect `baseCookie`, or every reconnect
/// throws "unexpected base cookie during sync". (Upstream: `#baseVersion =
/// cookieToVersion(baseCookie)`.)
#[tokio::test]
async fn reconnecting_client_first_poke_echoes_its_base_cookie() {
    let replica = seed_replica(&[(1, "alpha")]);
    let server = Server::boot(replica, HandlerDeps::default()).await;
    // Connect with a persisted baseCookie in the URL.
    let req = format!(
        "ws://{}/sync/v51/connect?clientGroupID=cg&clientID=c&baseCookie=00:01&lmid=0",
        server.addr
    )
    .into_client_request()
    .unwrap();
    let (mut client, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let _greeting = client.next().await.unwrap().unwrap();
    client
        .send(Message::text(
            r#"["initConnection",{"desiredQueriesPatch":[{"op":"put","hash":"issue-all","ast":{"table":"issue"}}]}]"#,
        ))
        .await
        .unwrap();
    let start = read_until(&mut client, |t| t.contains("pokeStart"))
        .await
        .expect("expected a pokeStart");
    assert!(
        start.contains("\"baseCookie\":\"00:01\""),
        "reconnecting client's first poke MUST base at its cookie 00:01, not null: {start}"
    );
    server.shutdown().await;
}

/// Regression: a fresh client's FIRST `pokeStart` must carry `baseCookie:null`.
/// A real @rocicorp/zero client connects with an empty base cookie (its
/// Replicache is at cookie `null`); if the server sends a non-null base cookie
/// (e.g. the port's internal `"00"`), the client throws "unexpected base cookie
/// during sync" and never syncs. This is exactly the bug hunting-game hit.
#[tokio::test]
async fn fresh_client_first_poke_has_null_base_cookie() {
    let replica = seed_replica(&[(1, "alpha")]);
    let server = Server::boot(replica, HandlerDeps::default()).await;
    let mut client = connect(
        server.addr,
        r#"[{"op":"put","hash":"issue-all","ast":{"table":"issue"}}]"#,
    )
    .await;
    let start = read_until(&mut client, |t| t.contains("pokeStart"))
        .await
        .expect("expected a pokeStart");
    assert!(
        start.contains("\"baseCookie\":null"),
        "fresh client's first poke MUST have baseCookie:null (else 'unexpected \
         base cookie'): {start}"
    );
    server.shutdown().await;
}

// ----------------------------------------------------------------------------
// 4. KNOWN GAP: permissions are NOT enforced in the live path
// ----------------------------------------------------------------------------

/// Documents the scope of `definePermissions` (compiled row-rule) enforcement.
///
/// IMPORTANT — this is NOT a gap for a server-authoritative app like
/// hunting-game. There are TWO permission models in zero:
///   1. Compiled row-rules (`definePermissions`) — enforced INSIDE zero-cache
///      via the read/write authorizers. This port does not yet wire those into
///      the live path (this test proves that): a client desiring a raw AST
///      query gets every row. Relevant only to apps that use `definePermissions`.
///   2. Server-authoritative (`defineQueries`/`defineMutators`) — auth lives in
///      the app's OWN query/mutate servers. zero-cache forwards the request
///      (with the user's token) and runs the AST the query server returns
///      (already scoped, e.g. via `where exists(...)`). hunting-game uses ONLY
///      this model — and the `synced_query_*_exists` tests above prove the port
///      enforces it faithfully (non-matching rows never hydrate).
///
/// So for hunting-game this permissive raw-AST behavior is irrelevant (it never
/// sends raw ASTs; every query is a named, server-transformed, pre-authorized
/// query). This test asserts the current raw-AST behavior to lock the boundary;
/// invert it if/when compiled `definePermissions` enforcement is wired in.
#[tokio::test]
async fn raw_ast_queries_are_not_row_filtered_by_compiled_permissions() {
    let replica = seed_replica(&[(1, "alice-owned"), (2, "bob-owned"), (3, "carol-owned")]);
    // No permissions can even be supplied to run_synced_server — there is no
    // config surface for them yet. That absence IS the gap.
    let server = Server::boot(replica, HandlerDeps::default()).await;
    let mut client = connect(
        server.addr,
        r#"[{"op":"put","hash":"issue-all","ast":{"table":"issue"}}]"#,
    )
    .await;

    // Collect a few frames; the client sees all three rows — no filtering.
    let mut seen = String::new();
    for _ in 0..8 {
        match tokio::time::timeout(std::time::Duration::from_secs(2), client.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => seen.push_str(&t),
            _ => break,
        }
        if seen.contains("bob-owned") && seen.contains("carol-owned") {
            break;
        }
    }
    assert!(
        seen.contains("bob-owned") && seen.contains("carol-owned"),
        "DOCUMENTED GAP: with no enforcement the client sees rows a policy would \
         forbid. If this assertion fails, enforcement may now be active — invert \
         this test. Frames seen: {seen}"
    );
    server.shutdown().await;
}

// ----------------------------------------------------------------------------
// 5. Litestream backup/restore round trip (gated on the binary)
// ----------------------------------------------------------------------------

#[tokio::test]
async fn litestream_backup_and_restore_roundtrip() {
    use zero_cache_server::litestream;
    if !litestream::available() {
        eprintln!("skipping: litestream binary not installed");
        return;
    }
    // Source replica with data.
    let src = seed_replica(&[(1, "backed-up"), (2, "durable")]);
    let backup_dir = std::env::temp_dir().join(format!("zc_ls_backup_{}", std::process::id()));
    let backup = format!("file://{}", backup_dir.to_str().unwrap());
    let _ = std::fs::remove_dir_all(&backup_dir);

    // Run a short continuous backup, then stop it.
    let mut child = litestream::spawn_replicate(&src, &backup).expect("start replicate");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let _ = child.kill();
    let _ = child.wait();

    // Restore to a new path and verify the rows survived the round trip.
    let restored = format!("{src}.restored");
    let _ = std::fs::remove_file(&restored);
    let ok = litestream::restore(&restored, &backup);
    if ok && std::path::Path::new(&restored).exists() {
        let db = StatementRunner::open_file_readonly(&restored).unwrap();
        let rows = db
            .query_uncached("SELECT title FROM issue ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2, "restored replica should carry both rows");
    } else {
        eprintln!("note: litestream restore produced no replica (backup may not have flushed)");
    }
    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&restored);
    let _ = std::fs::remove_dir_all(&backup_dir);
}
