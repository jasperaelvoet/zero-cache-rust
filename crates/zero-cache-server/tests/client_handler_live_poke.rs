//! Live proof of `zero-cache-server::client_handler`'s `ClientHandler`/
//! `PokeCycle`: a real `pokeStart` -> `pokePart` -> `pokeEnd` sequence,
//! driven entirely by the pure decisions in
//! `zero-cache-view-syncer::client_handler_poke` composed with real wire
//! serialization (`poke_message_json`), sent over a REAL WebSocket
//! connection and received by a REAL connected `tokio-tungstenite` client
//! — no mocking anywhere in the chain. This is the live counterpart to
//! `query_hydration.rs`'s CVR-side proof: that one proved the CVR decision
//! layer composes with the real IVM layer; this one proves the poke
//! decision layer composes with the real WebSocket send layer.

use futures_util::StreamExt;
use zero_cache_protocol::row_patch::RowPatchOp;
use zero_cache_server::client_handler::ClientHandler;
use zero_cache_server::ws_connection::WsConnection;
use zero_cache_view_syncer::client_patch::{
    ClientPutRowPatch, ClientRowPatch, Patch, PatchToVersion,
};
use zero_cache_view_syncer::cvr_types::RowId;
use zero_cache_view_syncer::cvr_version::CvrVersion;

fn v(s: &str) -> CvrVersion {
    CvrVersion {
        state_version: s.into(),
        config_version: None,
    }
}

fn row_id(table: &str, key: &str) -> RowId {
    RowId {
        schema: "public".into(),
        table: table.into(),
        row_key: std::collections::BTreeMap::from([(
            "id".to_string(),
            zero_cache_shared::bigint_json::JsonValue::String(key.into()),
        )]),
    }
}

async fn connect_pair() -> (
    WsConnection,
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client_task = tokio::spawn(async move {
        tokio_tungstenite::connect_async(format!("ws://{addr}/sync"))
            .await
            .unwrap()
            .0
    });
    let (tcp, _) = listener.accept().await.unwrap();
    let server = WsConnection::accept(tcp).await.unwrap();
    let client = client_task.await.unwrap();
    (server, client)
}

/// Live proof: a full poke cycle (two row patches, one filtered out as
/// stale) produces exactly `pokeStart` -> `pokePart` -> `pokeEnd` real
/// frames on the wire, decodable back into the exact patches sent.
#[tokio::test]
async fn full_poke_cycle_sends_real_frames_over_a_real_socket() {
    let (mut server, mut client) = connect_pair().await;

    let mut handler = ClientHandler::new(
        "ws1",
        "cg1",
        "zero_0.clients",
        "zero_0.mutations",
        Some(v("01")),
    );
    let mut cycle = handler
        .start_poke(&mut server, &v("02"))
        .expect("client is behind, should poke");

    // A real patch that should be included (toVersion > baseVersion).
    cycle
        .add_patch(PatchToVersion {
            patch: Patch::Row(ClientRowPatch::Put(ClientPutRowPatch {
                id: row_id("issues", "1"),
                contents: vec![(
                    "title".into(),
                    zero_cache_shared::bigint_json::JsonValue::String("bug".into()),
                )],
            })),
            to_version: v("02"),
        })
        .await
        .unwrap();

    // A stale patch (toVersion == baseVersion) that should be silently skipped.
    cycle
        .add_patch(PatchToVersion {
            patch: Patch::Row(ClientRowPatch::Put(ClientPutRowPatch {
                id: row_id("issues", "2"),
                contents: vec![],
            })),
            to_version: v("01"),
        })
        .await
        .unwrap();

    let sent_something = cycle.end(&v("02"), false).await.unwrap();
    assert!(sent_something);
    handler.commit_poke(v("02"));

    let start_frame = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();
    assert!(
        start_frame.starts_with("[\"pokeStart\","),
        "expected a pokeStart frame, got: {start_frame}"
    );
    assert!(start_frame.contains("\"baseCookie\":\"01\""));

    let part_frame = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();
    assert!(
        part_frame.starts_with("[\"pokePart\","),
        "expected a pokePart frame, got: {part_frame}"
    );
    assert!(part_frame.contains("\"rowsPatch\""));
    assert!(
        part_frame.contains("\"title\":\"bug\""),
        "the stale patch (row id 2) should NOT be present: {part_frame}"
    );
    assert!(
        !part_frame.contains("\"id\":\"2\""),
        "the stale patch should have been filtered out: {part_frame}"
    );

    let end_frame = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();
    assert!(
        end_frame.starts_with("[\"pokeEnd\","),
        "expected a pokeEnd frame, got: {end_frame}"
    );
    assert!(end_frame.contains("\"cookie\":\"02\""));
}

/// Live proof: a client already caught up gets NO poke at all —
/// `start_poke` returns `None` and nothing is ever sent over the socket.
#[tokio::test]
async fn already_caught_up_client_receives_nothing() {
    let (mut server, mut client) = connect_pair().await;

    let mut handler = ClientHandler::new(
        "ws1",
        "cg1",
        "zero_0.clients",
        "zero_0.mutations",
        Some(v("02")),
    );
    // ever_poked defaults to false in a fresh handler, but the test wants
    // to prove the ALREADY-poked-and-caught-up path specifically.
    handler.commit_poke(v("02"));
    let cycle = handler.start_poke(&mut server, &v("02"));
    assert!(
        cycle.is_none(),
        "an already-caught-up, already-poked client should get no PokeCycle at all"
    );

    // Prove nothing arrives by racing a short timeout against recv.
    let result = tokio::time::timeout(std::time::Duration::from_millis(100), client.next()).await;
    assert!(
        result.is_err(),
        "no frame should have been sent to an already-caught-up client"
    );
}

/// Live proof: a poke with no patches added still sends
/// pokeStart/pokeEnd but with an EMPTY rows_patch part — matching
/// `end`'s `SendPokeStartFirst` path for a version-only advance.
#[tokio::test]
async fn poke_with_no_patches_still_advances_the_version() {
    let (mut server, mut client) = connect_pair().await;

    let mut handler = ClientHandler::new(
        "ws1",
        "cg1",
        "zero_0.clients",
        "zero_0.mutations",
        Some(v("01")),
    );
    let cycle = handler.start_poke(&mut server, &v("02")).unwrap();
    let sent_something = cycle.end(&v("02"), false).await.unwrap();
    assert!(sent_something);

    let start_frame = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();
    assert!(start_frame.starts_with("[\"pokeStart\","));

    // No pokePart should have been sent since no patches were ever added
    // (body stays None, flush_body is a no-op) — the very next frame is
    // pokeEnd directly.
    let end_frame = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();
    assert!(end_frame.starts_with("[\"pokeEnd\","), "expected pokeEnd to come right after pokeStart with no pokePart in between, got: {end_frame}");
}

/// Sanity: `make_row_patch`'s real output round-trips through the wire
/// serializer correctly for a `Del` op too, not just `Put`.
#[tokio::test]
async fn del_row_patch_is_sent_correctly() {
    let (mut server, mut client) = connect_pair().await;

    let mut handler = ClientHandler::new(
        "ws1",
        "cg1",
        "zero_0.clients",
        "zero_0.mutations",
        Some(v("01")),
    );
    let mut cycle = handler.start_poke(&mut server, &v("02")).unwrap();
    cycle
        .add_patch(PatchToVersion {
            patch: Patch::Row(ClientRowPatch::Delete(
                zero_cache_view_syncer::client_patch::ClientDeleteRowPatch {
                    id: row_id("issues", "9"),
                },
            )),
            to_version: v("02"),
        })
        .await
        .unwrap();
    cycle.end(&v("02"), false).await.unwrap();

    let _start = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();
    let part_frame = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();
    assert!(
        part_frame.contains("\"op\":\"del\""),
        "expected a del row patch op: {part_frame}"
    );

    let _ = RowPatchOp::Del; // type used above only for documentation clarity
}

/// Live proof of the query-patch path: a client-scoped desired-query patch
/// lands in `desiredQueriesPatches[clientID]`, and a client-group-wide got
/// patch lands in `gotQueriesPatch` — matching upstream's `patch.clientID
/// ? desiredQueriesPatches[...] : gotQueriesPatch` branch exactly, proven
/// against real serialized wire bytes, not just constructed Rust values.
#[tokio::test]
async fn query_patches_route_to_the_right_wire_field() {
    use zero_cache_view_syncer::cvr_types::{PatchOp, QueryPatch};

    let (mut server, mut client) = connect_pair().await;

    let mut handler = ClientHandler::new(
        "ws1",
        "cg1",
        "zero_0.clients",
        "zero_0.mutations",
        Some(v("01")),
    );
    let mut cycle = handler.start_poke(&mut server, &v("02")).unwrap();

    // Client-scoped desired-query patch.
    cycle
        .add_patch(PatchToVersion {
            patch: Patch::Config(QueryPatch {
                op: PatchOp::Put,
                id: "hash1".into(),
                client_id: Some("c1".into()),
            }),
            to_version: v("02"),
        })
        .await
        .unwrap();
    // Client-group-wide "got" patch (no clientID).
    cycle
        .add_patch(PatchToVersion {
            patch: Patch::Config(QueryPatch {
                op: PatchOp::Del,
                id: "hash2".into(),
                client_id: None,
            }),
            to_version: v("02"),
        })
        .await
        .unwrap();

    cycle.end(&v("02"), false).await.unwrap();

    let _start = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();
    let part_frame = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();

    assert!(
        part_frame
            .contains("\"desiredQueriesPatches\":{\"c1\":[{\"op\":\"put\",\"hash\":\"hash1\"}]}"),
        "expected the client-scoped patch under desiredQueriesPatches[c1]: {part_frame}"
    );
    assert!(
        part_frame.contains("\"gotQueriesPatch\":[{\"op\":\"del\",\"hash\":\"hash2\"}]"),
        "expected the client-group-wide patch under gotQueriesPatch: {part_frame}"
    );
}

/// Live proof of row-table classification: a `zero_0.clients` row-put
/// patch is intercepted and reclassified into `lastMutationIDChanges`
/// (NOT emitted as a plain `rowsPatch` entry), and a `zero_0.mutations`
/// row-delete patch is reclassified into `mutationsPatch` — matching
/// upstream's `#updateLMIDs`/`mutationsPatch` branches, proven against
/// real wire bytes.
#[tokio::test]
async fn special_tables_are_reclassified_instead_of_sent_as_plain_row_patches() {
    let (mut server, mut client) = connect_pair().await;

    let mut handler = ClientHandler::new(
        "ws1",
        "cg1",
        "zero_0.clients",
        "zero_0.mutations",
        Some(v("01")),
    );
    let mut cycle = handler.start_poke(&mut server, &v("02")).unwrap();

    // A clients-table row -> should become a lastMutationIDChanges entry.
    let clients_row_id = zero_cache_view_syncer::cvr_types::RowId {
        schema: "public".into(),
        table: "zero_0.clients".into(),
        row_key: std::collections::BTreeMap::from([(
            "id".to_string(),
            zero_cache_shared::bigint_json::JsonValue::String("row1".into()),
        )]),
    };
    let clients_contents = vec![
        (
            "clientGroupID".to_string(),
            zero_cache_shared::bigint_json::JsonValue::String("cg1".into()),
        ),
        (
            "clientID".to_string(),
            zero_cache_shared::bigint_json::JsonValue::String("c1".into()),
        ),
        (
            "lastMutationID".to_string(),
            zero_cache_shared::bigint_json::JsonValue::Number(5.0),
        ),
    ];
    cycle
        .add_patch(PatchToVersion {
            patch: Patch::Row(ClientRowPatch::Put(ClientPutRowPatch {
                id: clients_row_id,
                contents: clients_contents,
            })),
            to_version: v("02"),
        })
        .await
        .unwrap();

    // A mutations-table row delete -> should become a mutationsPatch del entry.
    let mutations_row_id = zero_cache_view_syncer::cvr_types::RowId {
        schema: "public".into(),
        table: "zero_0.mutations".into(),
        row_key: std::collections::BTreeMap::from([
            (
                "clientID".to_string(),
                zero_cache_shared::bigint_json::JsonValue::String("c1".into()),
            ),
            (
                "mutationID".to_string(),
                zero_cache_shared::bigint_json::JsonValue::Number(3.0),
            ),
        ]),
    };
    cycle
        .add_patch(PatchToVersion {
            patch: Patch::Row(ClientRowPatch::Delete(
                zero_cache_view_syncer::client_patch::ClientDeleteRowPatch {
                    id: mutations_row_id,
                },
            )),
            to_version: v("02"),
        })
        .await
        .unwrap();

    cycle.end(&v("02"), false).await.unwrap();

    let _start = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();
    let part_frame = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();

    assert!(
        !part_frame.contains("\"rowsPatch\""),
        "neither special-table row should appear in rowsPatch: {part_frame}"
    );
    assert!(
        part_frame.contains("\"lastMutationIDChanges\":{\"c1\":5}"),
        "expected the clients-table row reclassified into lastMutationIDChanges: {part_frame}"
    );
    assert!(
        part_frame.contains(
            "\"mutationsPatch\":[{\"op\":\"del\",\"id\":{\"clientID\":\"c1\",\"id\":3}}]"
        ),
        "expected the mutations-table row reclassified into mutationsPatch: {part_frame}"
    );
}

/// Live proof of the mutations-table `'put'` branch: a real row-put
/// carrying a JSON-encoded `result` column is parsed into a full
/// `MutationResponse` (including its `MutationResult` discriminated
/// union) and lands correctly in `mutationsPatch` on the real wire.
#[tokio::test]
async fn mutations_table_put_parses_the_full_mutation_result() {
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_view_syncer::cvr_types::RowId;

    let (mut server, mut client) = connect_pair().await;

    let mut handler = ClientHandler::new(
        "ws1",
        "cg1",
        "zero_0.clients",
        "zero_0.mutations",
        Some(v("01")),
    );
    let mut cycle = handler.start_poke(&mut server, &v("02")).unwrap();

    let mutations_row_id = RowId {
        schema: "public".into(),
        table: "zero_0.mutations".into(),
        row_key: std::collections::BTreeMap::from([
            ("clientID".to_string(), JsonValue::String("c1".into())),
            ("mutationID".to_string(), JsonValue::Number(9.0)),
        ]),
    };
    let contents = vec![
        ("clientGroupID".to_string(), JsonValue::String("cg1".into())),
        ("clientID".to_string(), JsonValue::String("c1".into())),
        ("mutationID".to_string(), JsonValue::Number(9.0)),
        (
            "result".to_string(),
            JsonValue::Object(vec![("data".to_string(), JsonValue::String("ok".into()))]),
        ),
    ];
    cycle
        .add_patch(PatchToVersion {
            patch: Patch::Row(ClientRowPatch::Put(ClientPutRowPatch {
                id: mutations_row_id,
                contents,
            })),
            to_version: v("02"),
        })
        .await
        .unwrap();
    cycle.end(&v("02"), false).await.unwrap();

    let _start = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();
    let part_frame = client
        .next()
        .await
        .unwrap()
        .unwrap()
        .into_text()
        .unwrap()
        .to_string();

    assert!(
        !part_frame.contains("\"rowsPatch\""),
        "the mutations-table row should NOT appear in rowsPatch: {part_frame}"
    );
    assert!(
        part_frame.contains("\"mutationsPatch\":[{\"op\":\"put\",\"mutation\":{\"id\":{\"clientID\":\"c1\",\"id\":9},\"result\":{\"data\":\"ok\"}}}]"),
        "expected the full parsed MutationResponse in mutationsPatch: {part_frame}"
    );
}
