//! The FULL originally-stated priority slice, end-to-end, live: a real
//! Postgres `INSERT` streams through raw replication into a real SQLite
//! replica, through a registered `TableSource`+`Filter` query (compiled
//! from a real AST `Condition`) to produce an IVM delta, which is then
//! serialized as a real `pokePart` JSON message and sent over a REAL
//! WebSocket connection to a REAL connected client.
//!
//! This is the capstone integration test tying together, in one place:
//! `zero-cache-change-source` (replication_conn + pgoutput + pg_to_change),
//! `zero-cache-sqlite` (ChangeDispatcher + pipeline::run_until +
//! ivm_bridge), `zero-cache-zql` (TableSource + Filter +
//! builder::filter::create_predicate), `zero-cache-protocol`
//! (poke_json::poke_message_json), and `zero-cache-server`
//! (ws_connection::WsConnection) — every subsystem this port's priority
//! slice named, wired together and proven against real Postgres and a real
//! socket. No mocks anywhere in the chain.

use zero_cache_change_source::data::{Change, TableCreate};
use zero_cache_change_source::pg_connection;
use zero_cache_change_source::pg_to_change::RelationTracker;
use zero_cache_change_source::replication_conn::ReplicationConn;
use zero_cache_protocol::ast::{
    ColumnReference, Condition, Direction, LiteralValue, SimpleOperator, ValuePosition,
};
use zero_cache_protocol::poke::{PokeMessage, PokePartBody};
use zero_cache_protocol::poke_json::poke_message_json;
use zero_cache_protocol::row_patch::{RowPatchOp, RowPutOp};
use zero_cache_server::ws_connection::WsConnection;
use zero_cache_sqlite::change_dispatcher::ChangeDispatcher;
use zero_cache_sqlite::change_log::CREATE_CHANGELOG_SCHEMA;
use zero_cache_sqlite::column_metadata::CREATE_COLUMN_METADATA_TABLE;
use zero_cache_sqlite::ivm_bridge::apply_to_source;
use zero_cache_sqlite::pipeline::run_until;
use zero_cache_sqlite::table_metadata::CREATE_TABLE_METADATA_TABLE;
use zero_cache_sqlite::StatementRunner;
use zero_cache_types::specs::{ColumnSpec, TableSpec};
use zero_cache_zql::builder::filter::create_predicate;
use zero_cache_zql::ivm::filter::Filter;
use zero_cache_zql::ivm::operator::Change as IvmChange;
use zero_cache_zql::ivm::table_source::TableSource;

fn test_host_port() -> (String, u16) {
    let url = std::env::var("ZERO_TEST_PG_TCP").unwrap_or_else(|_| "localhost:54329".to_string());
    let mut parts = url.splitn(2, ':');
    (
        parts.next().unwrap().to_string(),
        parts.next().unwrap().parse().unwrap(),
    )
}

#[tokio::test]
async fn real_postgres_insert_reaches_a_real_websocket_client_as_a_poke() {
    let (host, port) = test_host_port();
    let conn_str = format!("host={host} port={port} user=postgres dbname=postgres");
    let Ok(pg) = pg_connection::connect(&conn_str).await else {
        eprintln!("skipping: no local test Postgres available");
        return;
    };

    pg.batch_execute(
        "DROP TABLE IF EXISTS e2e_test CASCADE; \
         CREATE TABLE e2e_test(id int primary key, active bool); \
         DROP PUBLICATION IF EXISTS e2e_test_pub; \
         CREATE PUBLICATION e2e_test_pub FOR TABLE e2e_test;",
    )
    .await
    .unwrap();
    pg.batch_execute(
        "SELECT pg_drop_replication_slot('e2e_test_slot') WHERE EXISTS \
         (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'e2e_test_slot');",
    )
    .await
    .ok();
    pg.query(
        "SELECT * FROM pg_create_logical_replication_slot('e2e_test_slot', 'pgoutput')",
        &[],
    )
    .await
    .unwrap();

    // --- Local SQLite replica, schema pre-created (initial sync stand-in) ---
    let db = StatementRunner::open_in_memory().unwrap();
    db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
    db.exec(CREATE_TABLE_METADATA_TABLE).unwrap();
    db.exec(CREATE_COLUMN_METADATA_TABLE).unwrap();
    let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
    dispatcher.begin("00").unwrap();
    dispatcher
        .apply(&Change::CreateTable(TableCreate {
            spec: TableSpec {
                name: "e2e_test".into(),
                schema: "public".into(),
                columns: vec![
                    ("id".into(), ColumnSpec::new("int4", 1)),
                    ("active".into(), ColumnSpec::new("bool", 2)),
                ],
                primary_key: Some(vec!["id".into()]),
            },
            metadata: None,
            backfill: None,
        }))
        .unwrap();
    dispatcher.commit("00").unwrap();

    // --- Real replication connection ---
    let conn = ReplicationConn::connect(&host, port, "postgres", "postgres", None)
        .await
        .unwrap();
    let mut stream = conn
        .start_replication("e2e_test_slot", "e2e_test_pub", "0/0")
        .await
        .unwrap();
    let mut tracker = RelationTracker::new();

    // --- Registered query: SELECT * FROM e2e_test WHERE active = true ---
    // (`pg_to_change` now typed-decodes `bool` columns into `JsonValue::Bool`.)
    let mut source = TableSource::new(
        "e2e_test",
        vec!["id".into()],
        vec![("id".into(), Direction::Asc)],
    );
    let condition = Condition::Simple {
        op: SimpleOperator::Eq,
        left: ValuePosition::Column(ColumnReference {
            name: "active".into(),
        }),
        right: ValuePosition::Literal(LiteralValue::Bool(true)),
    };
    let predicate = create_predicate(&condition);
    let filter = Filter::new(move |row: &zero_cache_zql::ivm::data::Row| predicate(row));
    let mut deltas: Vec<IvmChange> = Vec::new();

    // --- Real WebSocket server + client over a real TCP socket ---
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client_task = tokio::spawn(async move {
        use futures_util::StreamExt;
        let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/sync"))
            .await
            .unwrap();
        client
            .next()
            .await
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap()
            .to_string() // the pokePart message
    });
    let (tcp, _) = listener.accept().await.unwrap();
    let mut ws = WsConnection::accept(tcp).await.unwrap();

    // --- Drive the pipeline ---
    pg.batch_execute("INSERT INTO e2e_test(id, active) VALUES (7, true)")
        .await
        .unwrap();

    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        run_until(
            &mut stream,
            &mut dispatcher,
            &mut tracker,
            || true,
            |change| {
                for source_change in apply_to_source(&mut source, change) {
                    if let Some(delta) = filter.push(source_change) {
                        deltas.push(delta);
                    }
                }
            },
        ),
    )
    .await
    .expect("timed out waiting for the insert to produce an IVM delta")
    .unwrap();

    assert_eq!(
        deltas.len(),
        1,
        "expected exactly one IVM delta: {deltas:?}"
    );
    let IvmChange::Add(node) = &deltas[0] else {
        panic!("expected Add, got {:?}", deltas[0])
    };

    // --- Serialize the delta as a real pokePart and send it over the real socket ---
    let poke = PokeMessage::Part(PokePartBody {
        poke_id: "p1".into(),
        rows_patch: Some(vec![RowPatchOp::Put(RowPutOp {
            table_name: "e2e_test".into(),
            value: node.row.clone(),
        })]),
        ..Default::default()
    });
    let json = poke_message_json(&poke);
    ws.send_json(&json).await.unwrap();

    let received = client_task.await.unwrap();
    assert_eq!(
        received, json,
        "the real WebSocket client should receive exactly the serialized poke"
    );
    assert!(
        received.contains("\"id\":7"),
        "poke should carry the inserted row: {received}"
    );

    drop(stream);
    let mut dropped = false;
    for _ in 0..20 {
        if pg
            .query("SELECT pg_drop_replication_slot('e2e_test_slot')", &[])
            .await
            .is_ok()
        {
            dropped = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(dropped);
    pg.batch_execute("DROP PUBLICATION e2e_test_pub; DROP TABLE e2e_test;")
        .await
        .unwrap();
}
