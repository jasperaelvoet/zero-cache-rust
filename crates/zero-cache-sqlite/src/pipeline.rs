//! The top-level replication apply loop: wires
//! `zero_cache_change_source::replication_conn` (raw `START_REPLICATION`
//! streaming) + `pgoutput` (binary decode) + `pg_to_change` (translation to
//! [`Change`]) into this crate's [`ChangeDispatcher`], so a real Postgres
//! logical-replication stream can be applied to a real SQLite replica.
//!
//! This is glue only — every piece it calls is independently ported and
//! tested elsewhere (`replication_conn`, `pgoutput`, `pg_to_change`,
//! `ChangeDispatcher`/`RowApplier`/`DdlApplier`). What's new here is driving
//! them together: translating pgoutput's per-message LSNs into the
//! version/watermark strings `ChangeDispatcher::begin`/`commit` expect (via
//! `zero_cache_types::lsn`), and looping `ReplicationStream::next_event`
//! into `ChangeDispatcher::apply`.
//!
//! Not ported: reconnect/resume-from-LSN logic, standby status update
//! (keepalive) replies, and backfill — this loop applies a live stream
//! start-to-finish for as long as the caller keeps polling it, matching the
//! scope of everything it's built from.

use num_bigint::BigInt;
use zero_cache_change_source::data::Change;
use zero_cache_change_source::pg_to_change::{RelationTracker, TranslateError};
use zero_cache_change_source::pgoutput::PgoutputMessage;
use zero_cache_change_source::replication_conn::{
    ReplicationError, ReplicationEvent, ReplicationStream,
};
use zero_cache_types::lexi_version::Version;
use zero_cache_types::state_version::major_version_to_string;

use crate::change_dispatcher::{ChangeDispatcher, DispatchError};

/// Errors driving the replication-to-SQLite apply loop.
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error(transparent)]
    Replication(#[from] ReplicationError),
    #[error(transparent)]
    Translate(#[from] TranslateError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
}

/// Converts a pgoutput end-LSN (the position *after* this message) into the
/// watermark string `ChangeDispatcher` uses as its transaction version.
fn lsn_to_watermark(lsn: u64) -> String {
    major_version_to_string(Version::Big(BigInt::from(lsn))).expect("64-bit LSN always encodes")
}

/// Reads and applies replication events to `dispatcher` until `should_stop`
/// returns true after a commit, or the stream ends. Returns the number of
/// transactions committed.
///
/// `should_stop` is checked only at transaction boundaries (right after a
/// `Change::Commit` is applied) so a caller never observes a half-applied
/// transaction — matching how `ChangeDispatcher` itself is transaction-
/// scoped.
///
/// `on_change` is invoked with every data/schema `Change` right after it's
/// applied to `dispatcher` (i.e. once it's durably reflected in the SQLite
/// replica) — the hook a caller uses to also drive an IVM pipeline (see
/// `crate::ivm_bridge`) without this function needing to know about ZQL/IVM
/// itself.
pub async fn run_until<F, C>(
    stream: &mut ReplicationStream,
    dispatcher: &mut ChangeDispatcher<'_>,
    tracker: &mut RelationTracker,
    mut should_stop: F,
    mut on_change: C,
) -> Result<usize, PipelineError>
where
    F: FnMut() -> bool,
    C: FnMut(&Change),
{
    let mut committed = 0;
    let mut pending_watermark = String::new();

    while let Some(event) = stream.next_event().await? {
        let ReplicationEvent::Data {
            end_lsn, message, ..
        } = event
        else {
            continue; // keepalive — no reply support yet, see module docs
        };

        if let PgoutputMessage::Begin { .. } = &message {
            pending_watermark = lsn_to_watermark(end_lsn);
        }

        let Some(change) = tracker.translate(&message)? else {
            continue; // e.g. Relation — updates tracker state only
        };

        match change {
            Change::Begin { .. } => dispatcher.begin(&pending_watermark)?,
            Change::Commit => {
                dispatcher.commit(&pending_watermark)?;
                committed += 1;
                if should_stop() {
                    break;
                }
            }
            Change::Rollback => dispatcher.rollback()?,
            other => {
                dispatcher.apply(&other)?;
                on_change(&other);
            }
        }
    }

    Ok(committed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_log::CREATE_CHANGELOG_SCHEMA;
    use crate::column_metadata::CREATE_COLUMN_METADATA_TABLE;
    use crate::table_metadata::CREATE_TABLE_METADATA_TABLE;
    use crate::StatementRunner;
    use zero_cache_change_source::data::TableCreate;
    use zero_cache_change_source::pg_connection;
    use zero_cache_change_source::replication_conn::ReplicationConn;
    use zero_cache_types::specs::{ColumnSpec, TableSpec};

    fn test_host_port() -> (String, u16) {
        let url =
            std::env::var("ZERO_TEST_PG_TCP").unwrap_or_else(|_| "localhost:54329".to_string());
        let mut parts = url.splitn(2, ':');
        (
            parts.next().unwrap().to_string(),
            parts.next().unwrap().parse().unwrap(),
        )
    }

    /// The whole-pipeline proof: a real Postgres `INSERT` streamed through
    /// our own raw replication connection, decoded, translated, and applied
    /// to a real (in-memory) SQLite replica — no mocks anywhere in the
    /// chain. This is the top-level integration test for the priority
    /// end-to-end slice (Postgres replication -> local store).
    #[tokio::test]
    async fn real_postgres_insert_lands_in_sqlite() {
        let (host, port) = test_host_port();
        let conn_str = format!("host={host} port={port} user=postgres dbname=postgres");
        let Ok(pg) = pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        pg.batch_execute(
            "DROP TABLE IF EXISTS pipeline_test CASCADE; \
             CREATE TABLE pipeline_test(id int primary key, title text); \
             DROP PUBLICATION IF EXISTS pipeline_test_pub; \
             CREATE PUBLICATION pipeline_test_pub FOR TABLE pipeline_test;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('pipeline_test_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'pipeline_test_slot');",
        )
        .await
        .ok();
        pg.query(
            "SELECT * FROM pg_create_logical_replication_slot('pipeline_test_slot', 'pgoutput')",
            &[],
        )
        .await
        .unwrap();

        // Real SQLite replica, with the target table already created via the
        // ported DDL apply path (standing in for initial sync, which is
        // unported) so the row Insert has somewhere to land.
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_TABLE_METADATA_TABLE).unwrap();
        db.exec(CREATE_COLUMN_METADATA_TABLE).unwrap();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        dispatcher.begin("00").unwrap();
        dispatcher
            .apply(&Change::CreateTable(TableCreate {
                spec: TableSpec {
                    name: "pipeline_test".into(),
                    schema: "public".into(),
                    columns: vec![
                        ("id".into(), ColumnSpec::new("int4", 1)),
                        ("title".into(), ColumnSpec::new("text", 2)),
                    ],
                    primary_key: Some(vec!["id".into()]),
                },
                metadata: None,
                backfill: None,
            }))
            .unwrap();
        dispatcher.commit("00").unwrap();

        let conn = ReplicationConn::connect(&host, port, "postgres", "postgres", None)
            .await
            .unwrap();
        let mut stream = conn
            .start_replication("pipeline_test_slot", "pipeline_test_pub", "0/0")
            .await
            .unwrap();
        let mut tracker = RelationTracker::new();

        pg.batch_execute(
            "INSERT INTO pipeline_test(id, title) VALUES (42, 'through-the-pipeline')",
        )
        .await
        .unwrap();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_until(&mut stream, &mut dispatcher, &mut tracker, || true, |_| {}),
        )
        .await
        .expect("timed out waiting for the insert to be applied");
        assert_eq!(result.unwrap(), 1);

        let rows = db
            .query_uncached("SELECT id, title FROM pipeline_test", &[])
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "expected the real Postgres INSERT to land in SQLite: {rows:?}"
        );

        drop(stream);
        let mut dropped = false;
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('pipeline_test_slot')", &[])
                .await
                .is_ok()
            {
                dropped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(dropped);
        pg.batch_execute("DROP PUBLICATION pipeline_test_pub; DROP TABLE pipeline_test;")
            .await
            .unwrap();
    }

    /// The FULL whole-pipeline-slice proof: a real Postgres `INSERT` streams
    /// through replication into SQLite (as in the test above) AND, via
    /// `on_change` + `crate::ivm_bridge`, into a registered `TableSource` +
    /// `Filter` query — producing an actual incremental-view-maintenance
    /// delta out the other end. This is both halves of the user's stated
    /// priority slice (Postgres replication -> local store -> ZQL/IVM)
    /// running together against a live database.
    #[tokio::test]
    async fn real_postgres_insert_produces_ivm_delta() {
        use crate::ivm_bridge::apply_to_source;
        use zero_cache_protocol::ast::Direction;
        use zero_cache_zql::ivm::filter::Filter;
        use zero_cache_zql::ivm::operator::{Change as IvmChange, Node as IvmNode};
        use zero_cache_zql::ivm::table_source::TableSource;

        let (host, port) = test_host_port();
        let conn_str = format!("host={host} port={port} user=postgres dbname=postgres");
        let Ok(pg) = pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        pg.batch_execute(
            "DROP TABLE IF EXISTS ivm_test CASCADE; \
             CREATE TABLE ivm_test(id int primary key, active bool); \
             DROP PUBLICATION IF EXISTS ivm_test_pub; \
             CREATE PUBLICATION ivm_test_pub FOR TABLE ivm_test;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('ivm_test_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'ivm_test_slot');",
        )
        .await
        .ok();
        pg.query(
            "SELECT * FROM pg_create_logical_replication_slot('ivm_test_slot', 'pgoutput')",
            &[],
        )
        .await
        .unwrap();

        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_TABLE_METADATA_TABLE).unwrap();
        db.exec(CREATE_COLUMN_METADATA_TABLE).unwrap();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        dispatcher.begin("00").unwrap();
        dispatcher
            .apply(&Change::CreateTable(TableCreate {
                spec: TableSpec {
                    name: "ivm_test".into(),
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

        let conn = ReplicationConn::connect(&host, port, "postgres", "postgres", None)
            .await
            .unwrap();
        let mut stream = conn
            .start_replication("ivm_test_slot", "ivm_test_pub", "0/0")
            .await
            .unwrap();
        let mut tracker = RelationTracker::new();

        // The registered query: `SELECT * FROM ivm_test WHERE active = true`,
        // as a REAL AST `Condition` compiled through
        // `zero_cache_zql::builder::filter::create_predicate` — not a
        // hand-wired Rust closure — proving the query-driven path, not just
        // the `Filter` mechanism. `pg_to_change` now typed-decodes `bool`
        // columns into `JsonValue::Bool` (see its module doc), so the
        // condition's literal is a native bool, not Postgres's `"t"` text
        // representation.
        use zero_cache_protocol::ast::{
            ColumnReference, Condition, LiteralValue, SimpleOperator, ValuePosition,
        };
        use zero_cache_zql::builder::filter::create_predicate;

        let mut source = TableSource::new(
            "ivm_test",
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

        pg.batch_execute("INSERT INTO ivm_test(id, active) VALUES (1, true)")
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
            deltas,
            vec![IvmChange::Add(IvmNode::new(vec![
                ("id".into(), zero_cache_shared::bigint_json::JsonValue::Number(1.0)),
                ("active".into(), zero_cache_shared::bigint_json::JsonValue::Bool(true)),
            ]))],
            "expected a real Postgres INSERT to flow all the way to a live IVM Add delta: {deltas:?}"
        );

        drop(stream);
        let mut dropped = false;
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('ivm_test_slot')", &[])
                .await
                .is_ok()
            {
                dropped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(dropped);
        pg.batch_execute("DROP PUBLICATION ivm_test_pub; DROP TABLE ivm_test;")
            .await
            .unwrap();
    }

    /// Live proof that `ivm::join::reeval_exists_after_child_change` (via
    /// `ivm_bridge::apply_to_child_and_reeval_exists`) stays correct when
    /// driven by a REAL Postgres replication stream, not just manually
    /// pushed test rows: a real `INSERT INTO comments` flips a real
    /// `issues` row's EXISTS status live.
    #[tokio::test]
    async fn real_postgres_child_insert_flips_exists_check() {
        use crate::ivm_bridge::apply_to_child_and_reeval_exists;
        use zero_cache_protocol::ast::{Correlation, Direction};
        use zero_cache_zql::ivm::table_source::TableSource;

        let (host, port) = test_host_port();
        let conn_str = format!("host={host} port={port} user=postgres dbname=postgres");
        let Ok(pg) = pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        pg.batch_execute(
            "DROP TABLE IF EXISTS ivm_comments, ivm_issues CASCADE; \
             CREATE TABLE ivm_issues(id int primary key); \
             CREATE TABLE ivm_comments(id int primary key, \"issueID\" int); \
             DROP PUBLICATION IF EXISTS ivm_join_pub; \
             CREATE PUBLICATION ivm_join_pub FOR TABLE ivm_issues, ivm_comments;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('ivm_join_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'ivm_join_slot');",
        )
        .await
        .ok();
        pg.query(
            "SELECT * FROM pg_create_logical_replication_slot('ivm_join_slot', 'pgoutput')",
            &[],
        )
        .await
        .unwrap();

        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_TABLE_METADATA_TABLE).unwrap();
        db.exec(CREATE_COLUMN_METADATA_TABLE).unwrap();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        dispatcher.begin("00").unwrap();
        dispatcher
            .apply(&Change::CreateTable(TableCreate {
                spec: TableSpec {
                    name: "ivm_issues".into(),
                    schema: "public".into(),
                    columns: vec![("id".into(), ColumnSpec::new("int4", 1))],
                    primary_key: Some(vec!["id".into()]),
                },
                metadata: None,
                backfill: None,
            }))
            .unwrap();
        dispatcher
            .apply(&Change::CreateTable(TableCreate {
                spec: TableSpec {
                    name: "ivm_comments".into(),
                    schema: "public".into(),
                    columns: vec![
                        ("id".into(), ColumnSpec::new("int4", 1)),
                        ("issueID".into(), ColumnSpec::new("int4", 2)),
                    ],
                    primary_key: Some(vec!["id".into()]),
                },
                metadata: None,
                backfill: None,
            }))
            .unwrap();
        dispatcher.commit("00").unwrap();

        let conn = ReplicationConn::connect(&host, port, "postgres", "postgres", None)
            .await
            .unwrap();
        let mut stream = conn
            .start_replication("ivm_join_slot", "ivm_join_pub", "0/0")
            .await
            .unwrap();
        let mut tracker = RelationTracker::new();

        let mut issues = TableSource::new(
            "ivm_issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "ivm_comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let correlation = Correlation {
            parent_field: vec!["id".into()],
            child_field: vec!["issueID".into()],
        };
        let exists_flips: std::cell::RefCell<Vec<(zero_cache_zql::ivm::data::Row, bool)>> =
            std::cell::RefCell::new(Vec::new());

        pg.batch_execute("INSERT INTO ivm_issues(id) VALUES (1)")
            .await
            .unwrap();
        pg.batch_execute("INSERT INTO ivm_comments(id, \"issueID\") VALUES (10, 1)")
            .await
            .unwrap();

        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_until(
                &mut stream,
                &mut dispatcher,
                &mut tracker,
                || !exists_flips.borrow().is_empty(),
                |change| {
                    // Keep the parent TableSource in sync too (no join re-eval needed for its own changes).
                    crate::ivm_bridge::apply_to_source(&mut issues, change);
                    let flips = apply_to_child_and_reeval_exists(
                        &mut comments,
                        change,
                        &issues,
                        &correlation,
                    );
                    exists_flips.borrow_mut().extend(flips);
                },
            ),
        )
        .await
        .expect("timed out waiting for the child insert to flip the EXISTS check")
        .unwrap();

        let exists_flips = exists_flips.into_inner();
        assert_eq!(
            exists_flips,
            vec![(vec![("id".into(), zero_cache_shared::bigint_json::JsonValue::Number(1.0))], true)],
            "a real Postgres INSERT into the child table should flip the parent's live EXISTS status: {exists_flips:?}"
        );

        drop(stream);
        let mut dropped = false;
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('ivm_join_slot')", &[])
                .await
                .is_ok()
            {
                dropped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(dropped);
        pg.batch_execute("DROP PUBLICATION ivm_join_pub; DROP TABLE ivm_comments, ivm_issues;")
            .await
            .unwrap();
    }

    /// Live proof that `ivm::join::reeval_relationship_after_child_change`
    /// (via `ivm_bridge::apply_to_child_and_reeval_relationship`) stays
    /// correct when driven by a REAL Postgres replication stream: a real
    /// `INSERT INTO comments` re-derives the real `issues` row's joined
    /// `comments` relationship live — the full row-nesting counterpart to
    /// `real_postgres_child_insert_flips_exists_check` above.
    #[tokio::test]
    async fn real_postgres_child_insert_updates_relationship() {
        use crate::ivm_bridge::apply_to_child_and_reeval_relationship;
        use zero_cache_protocol::ast::{Correlation, Direction};
        use zero_cache_zql::ivm::table_source::TableSource;

        let (host, port) = test_host_port();
        let conn_str = format!("host={host} port={port} user=postgres dbname=postgres");
        let Ok(pg) = pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        pg.batch_execute(
            "DROP TABLE IF EXISTS ivm_rel_comments, ivm_rel_issues CASCADE; \
             CREATE TABLE ivm_rel_issues(id int primary key); \
             CREATE TABLE ivm_rel_comments(id int primary key, \"issueID\" int); \
             DROP PUBLICATION IF EXISTS ivm_rel_pub; \
             CREATE PUBLICATION ivm_rel_pub FOR TABLE ivm_rel_issues, ivm_rel_comments;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('ivm_rel_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'ivm_rel_slot');",
        )
        .await
        .ok();
        pg.query(
            "SELECT * FROM pg_create_logical_replication_slot('ivm_rel_slot', 'pgoutput')",
            &[],
        )
        .await
        .unwrap();

        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_TABLE_METADATA_TABLE).unwrap();
        db.exec(CREATE_COLUMN_METADATA_TABLE).unwrap();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        dispatcher.begin("00").unwrap();
        dispatcher
            .apply(&Change::CreateTable(TableCreate {
                spec: TableSpec {
                    name: "ivm_rel_issues".into(),
                    schema: "public".into(),
                    columns: vec![("id".into(), ColumnSpec::new("int4", 1))],
                    primary_key: Some(vec!["id".into()]),
                },
                metadata: None,
                backfill: None,
            }))
            .unwrap();
        dispatcher
            .apply(&Change::CreateTable(TableCreate {
                spec: TableSpec {
                    name: "ivm_rel_comments".into(),
                    schema: "public".into(),
                    columns: vec![
                        ("id".into(), ColumnSpec::new("int4", 1)),
                        ("issueID".into(), ColumnSpec::new("int4", 2)),
                    ],
                    primary_key: Some(vec!["id".into()]),
                },
                metadata: None,
                backfill: None,
            }))
            .unwrap();
        dispatcher.commit("00").unwrap();

        let conn = ReplicationConn::connect(&host, port, "postgres", "postgres", None)
            .await
            .unwrap();
        let mut stream = conn
            .start_replication("ivm_rel_slot", "ivm_rel_pub", "0/0")
            .await
            .unwrap();
        let mut tracker = RelationTracker::new();

        let mut issues = TableSource::new(
            "ivm_rel_issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "ivm_rel_comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let correlation = Correlation {
            parent_field: vec!["id".into()],
            child_field: vec!["issueID".into()],
        };
        let updates: std::cell::RefCell<Vec<zero_cache_zql::ivm::operator::Node>> =
            std::cell::RefCell::new(Vec::new());

        pg.batch_execute("INSERT INTO ivm_rel_issues(id) VALUES (1)")
            .await
            .unwrap();
        pg.batch_execute("INSERT INTO ivm_rel_comments(id, \"issueID\") VALUES (10, 1)")
            .await
            .unwrap();

        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_until(
                &mut stream,
                &mut dispatcher,
                &mut tracker,
                || !updates.borrow().is_empty(),
                |change| {
                    crate::ivm_bridge::apply_to_source(&mut issues, change);
                    let nodes = apply_to_child_and_reeval_relationship(
                        &mut comments,
                        change,
                        &issues,
                        &correlation,
                        "comments",
                    );
                    updates.borrow_mut().extend(nodes);
                },
            ),
        )
        .await
        .expect("timed out waiting for the child insert to update the relationship")
        .unwrap();

        let updates = updates.into_inner();
        assert_eq!(
            updates.len(),
            1,
            "expected exactly one parent-relationship update: {updates:?}"
        );
        assert_eq!(
            updates[0].row,
            vec![(
                "id".into(),
                zero_cache_shared::bigint_json::JsonValue::Number(1.0)
            )]
        );
        assert_eq!(
            updates[0].relationships["comments"],
            vec![zero_cache_zql::ivm::operator::Node::new(vec![
                ("id".into(), zero_cache_shared::bigint_json::JsonValue::Number(10.0)),
                ("issueID".into(), zero_cache_shared::bigint_json::JsonValue::Number(1.0)),
            ])],
            "a real Postgres INSERT into the child table should re-derive the parent's live joined relationship: {updates:?}"
        );

        drop(stream);
        let mut dropped = false;
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('ivm_rel_slot')", &[])
                .await
                .is_ok()
            {
                dropped = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(dropped);
        pg.batch_execute(
            "DROP PUBLICATION ivm_rel_pub; DROP TABLE ivm_rel_comments, ivm_rel_issues;",
        )
        .await
        .unwrap();
    }
}
