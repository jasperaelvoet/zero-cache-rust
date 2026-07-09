//! The top-level `initialSync` driver — the orchestration half of
//! `change-source/pg/initial-sync.ts`'s `initialSync`, sequencing the live
//! primitives that prior rounds built individually into one end-to-end
//! snapshot backfill of upstream Postgres into the SQLite replica.
//!
//! The sequence (mirroring upstream, minus the pieces noted below):
//!   1. compute the initial replica version from the slot's `consistent_point`
//!      LSN (`to_state_version_string`);
//!   2. create the replica meta-tables and seed `_zero.replicationConfig`/
//!      `_zero.replicationState` (`init_replication_state`);
//!   3. bind the copy connection to the slot's exported snapshot
//!      (`BEGIN ISOLATION LEVEL REPEATABLE READ` + `SET TRANSACTION SNAPSHOT`)
//!      so every table is copied at exactly the slot's consistent point;
//!   4. create each SQLite table from its published spec
//!      (`map_postgres_to_lite` + `DdlApplier::create_table`);
//!   5. binary-`COPY` each table's rows in at the snapshot
//!      (`initial_sync_copy::copy_table_binary`);
//!   6. create the published indexes (`DdlApplier::create_index`);
//!   7. commit the copy transaction.
//!
//! What is NOT covered here (the pieces still needing a live introspection
//! query or config that this port hasn't built): `getPublicationInfo` — the
//! upstream-schema introspection that produces the `PublishedTableSpec`s /
//! `IndexSpec`s — is the caller's input here rather than run internally; and
//! `ensurePublishedTables`/`checkUpstreamConfig` (the DDL that *creates* the
//! publication and validates `wal_level` upstream) are assumed already done.
//! Slot creation itself lives in `zero-cache-change-source`'s
//! `ReplicationConn::create_logical_replication_slot`, which produces the
//! [`SlotInfo`] this driver consumes. Everything from the snapshot binding
//! through the committed replica is real, live, and exercised end-to-end
//! against Postgres in the test below.

use tokio_postgres::Client;

use zero_cache_types::specs::{IndexSpec, PublishedTableSpec};

use crate::change_log::CREATE_CHANGELOG_SCHEMA;
use crate::column_metadata::CREATE_COLUMN_METADATA_TABLE;
use crate::ddl_apply::{DdlApplier, DdlError};
use crate::initial_sync_copy::{copy_table_binary, CopyTableError};
use crate::replication_state::init_replication_state;
use crate::table_metadata::CREATE_TABLE_METADATA_TABLE;
use crate::{DbError, StatementRunner};

/// The exported-snapshot info an initial sync copies at — the subset of
/// `ReplicationConn::create_logical_replication_slot`'s `CreatedSlot` this
/// driver needs. Kept as a local type so this crate doesn't depend on
/// `zero-cache-change-source` purely for a two-field struct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotInfo {
    /// The slot's `consistent_point` LSN (`X/Y` hex form); the initial replica
    /// version is derived from this.
    pub consistent_point: String,
    /// The exported snapshot name to `SET TRANSACTION SNAPSHOT` to.
    pub snapshot_name: String,
}

#[derive(Debug, thiserror::Error)]
pub enum InitialSyncError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Ddl(#[from] DdlError),
    #[error(transparent)]
    Copy(#[from] CopyTableError),
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error("malformed consistent_point LSN {0:?}: {1}")]
    Lsn(String, String),
    #[error("introspecting published schema: {0}")]
    Introspect(String),
}

/// Result of a completed initial sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialSyncResult {
    /// The replica version (LexiVersion of the slot LSN's major component) the
    /// replica is now at.
    pub replica_version: String,
    /// Per-table copied row counts, in the order `tables` was given.
    pub table_rows: Vec<(String, usize)>,
}

/// Runs a full initial sync of `tables` (and their `indexes`) from upstream
/// `pg` into the SQLite replica `db`, at the exported snapshot in `slot`.
///
/// `pg` must be a dedicated connection: this opens a `REPEATABLE READ`
/// transaction on it, binds it to the exported snapshot, and commits at the
/// end. `db` must be a fresh replica (this creates the `_zero.*` meta-tables).
/// `publications` is the list of upstream publication names, recorded in
/// `_zero.replicationConfig`.
pub async fn run_initial_sync(
    pg: &Client,
    db: &StatementRunner,
    slot: &SlotInfo,
    publications: &[String],
    tables: &[PublishedTableSpec],
    indexes: &[IndexSpec],
) -> Result<InitialSyncResult, InitialSyncError> {
    // 1. Initial replica version from the slot's consistent point.
    let replica_version = zero_cache_types::lsn::to_state_version_string(&slot.consistent_point)
        .map_err(|e| InitialSyncError::Lsn(slot.consistent_point.clone(), e.to_string()))?;

    // 2. Meta-tables + replication config/state. `init_replication_state`
    //    creates the replicationConfig/replicationState tables (create_tables:
    //    true); the change-log / table-metadata / column-metadata tables the
    //    DDL applier writes to are created here alongside them.
    db.exec(CREATE_CHANGELOG_SCHEMA)?;
    db.exec(CREATE_TABLE_METADATA_TABLE)?;
    db.exec(CREATE_COLUMN_METADATA_TABLE)?;
    let context = zero_cache_shared::bigint_json::JsonValue::Object(Vec::new());
    init_replication_state(db, publications, &replica_version, &context, true)?;

    // 3. Bind the copy connection to the slot's exported snapshot so every
    //    table is read at exactly the consistent point.
    pg.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await?;
    pg.batch_execute(&format!(
        "SET TRANSACTION SNAPSHOT '{}'",
        slot.snapshot_name
    ))
    .await?;

    // 4-6. Create tables, copy rows, create indexes.
    let result = copy_all(pg, db, tables, indexes, &replica_version).await;

    // 7. Always end the upstream transaction; propagate the copy error (if
    //    any) after cleaning up so the connection isn't left mid-transaction.
    match result {
        Ok(table_rows) => {
            pg.batch_execute("COMMIT").await?;
            Ok(InitialSyncResult {
                replica_version,
                table_rows,
            })
        }
        Err(e) => {
            // Best-effort rollback; the original error is the interesting one.
            let _ = pg.batch_execute("ROLLBACK").await;
            Err(e)
        }
    }
}

/// Discards a replica's contents so initial sync can rebuild it from a fresh
/// snapshot — the execution half of a supervised `Resync`
/// ([`crate::replication_supervisor::SupervisorDecision::Resync`]).
///
/// When the upstream schema drifts from the schema the replica was built from,
/// the replica cannot follow the change incrementally; upstream throws away
/// the replica file and re-runs initial sync. This port's replica is a single
/// SQLite database, so the equivalent is dropping every user/metadata table
/// (everything except SQLite's internal `sqlite_*` objects) — after which
/// [`run_initial_sync_introspected`] can recreate the change-log/metadata
/// schema and re-copy the (new-schema) published tables into the SAME db
/// handle. Returns the names of the tables dropped, in a deterministic order.
pub fn reset_replica_for_resync(db: &StatementRunner) -> Result<Vec<String>, DbError> {
    let rows = db.query_uncached(
        "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE 'sqlite_%' ORDER BY name",
        &[],
    )?;
    let names: Vec<String> = rows
        .into_iter()
        .filter_map(|r| match r.into_iter().next() {
            Some((_, crate::Value::Text(name))) => Some(name),
            _ => None,
        })
        .collect();
    // Disable FK enforcement for the drop sweep so referential order between
    // replicated tables doesn't matter (upstream discards the whole file).
    db.exec("PRAGMA foreign_keys = OFF")?;
    for name in &names {
        db.exec(&format!(
            "DROP TABLE IF EXISTS \"{}\"",
            name.replace('"', "\"\"")
        ))?;
    }
    db.exec("PRAGMA foreign_keys = ON")?;
    Ok(names)
}

/// Self-introspecting initial sync: like [`run_initial_sync`], but instead of
/// taking the table/index specs as input it discovers them *at the slot's
/// snapshot* via `get_publication_info` — matching upstream, which runs
/// `getPublicationInfo` inside the same `SET TRANSACTION SNAPSHOT` transaction
/// that the bulk COPY uses, so the schema and the data are read at exactly the
/// same consistent point. This is the self-driving core: given a slot and the
/// publication set, it produces a fully populated replica with no externally
/// supplied specs.
///
/// The remaining seam before a single top-level entry point is slot creation
/// (`ReplicationConn::create_logical_replication_slot`) and
/// `setup_tables_and_replication`, both already live-tested; a caller sequences
/// `check_upstream_config` → `setup_tables_and_replication` → create slot →
/// this.
pub async fn run_initial_sync_introspected(
    pg: &Client,
    db: &StatementRunner,
    slot: &SlotInfo,
    publications: &[String],
) -> Result<InitialSyncResult, InitialSyncError> {
    let replica_version = zero_cache_types::lsn::to_state_version_string(&slot.consistent_point)
        .map_err(|e| InitialSyncError::Lsn(slot.consistent_point.clone(), e.to_string()))?;

    db.exec(CREATE_CHANGELOG_SCHEMA)?;
    db.exec(CREATE_TABLE_METADATA_TABLE)?;
    db.exec(CREATE_COLUMN_METADATA_TABLE)?;
    let context = zero_cache_shared::bigint_json::JsonValue::Object(Vec::new());
    init_replication_state(db, publications, &replica_version, &context, true)?;

    pg.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        .await?;
    pg.batch_execute(&format!(
        "SET TRANSACTION SNAPSHOT '{}'",
        slot.snapshot_name
    ))
    .await?;

    // Introspect the published schema AT the snapshot, then copy it.
    let result = async {
        let (tables, pub_indexes) =
            zero_cache_change_source::published_schema::get_publication_info(pg, publications)
                .await
                .map_err(|e| InitialSyncError::Introspect(e.to_string()))?;
        let indexes: Vec<IndexSpec> = pub_indexes
            .iter()
            .map(zero_cache_types::published_schema_json::to_index_spec)
            .collect();
        copy_all(pg, db, &tables, &indexes, &replica_version).await
    }
    .await;

    match result {
        Ok(table_rows) => {
            pg.batch_execute("COMMIT").await?;
            Ok(InitialSyncResult {
                replica_version,
                table_rows,
            })
        }
        Err(e) => {
            let _ = pg.batch_execute("ROLLBACK").await;
            Err(e)
        }
    }
}

/// Connection + shard parameters for the top-level [`run_full_initial_sync`].
/// The two connection descriptions are separate because the replication slot is
/// created over a raw replication-protocol connection
/// (`ReplicationConn::connect`, which needs host/port/user/dbname) while the
/// upstream-config check, publication DDL, and bulk COPY use ordinary
/// `tokio-postgres` clients (`conn_str`).
#[derive(Debug, Clone)]
pub struct InitialSyncParams {
    /// libpq-style connection string for the ordinary query/copy connections.
    pub conn_str: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub dbname: String,
    /// The replication slot to create (the shard's slot name).
    pub slot_name: String,
}

/// The single top-level initial-sync entry point — sequences every live piece
/// prior rounds built and tested individually into one call: validate the
/// upstream config, set up the shard's publications/schema, create the
/// replication slot (fixing a consistent snapshot), and snapshot-copy the
/// published schema+data into `db`. Returns the completed [`InitialSyncResult`]
/// plus the shard's full publication set.
///
/// The Rust counterpart of `initial-sync.ts`'s `initialSync`, assembled from
/// `check_upstream_config` → `setup_tables_and_replication` →
/// `ReplicationConn::create_logical_replication_slot` →
/// `run_initial_sync_introspected`. The one upstream step still deferred is
/// `setupTriggers` (event-trigger DDL for schema-change detection); a caller
/// that needs DDL-change detection runs it after this returns.
pub async fn run_full_initial_sync(
    params: &InitialSyncParams,
    db: &StatementRunner,
    requested: &zero_cache_types::shards::ShardConfig,
) -> Result<(InitialSyncResult, Vec<String>), InitialSyncError> {
    use zero_cache_change_source::pg_connection;

    let ie = |e: &dyn std::fmt::Display| InitialSyncError::Introspect(e.to_string());

    // 1. Validate wal_level/version on an admin connection.
    let admin = pg_connection::connect(&params.conn_str)
        .await
        .map_err(|e| ie(&e))?;
    pg_connection::check_upstream_config(&admin)
        .await
        .map_err(|e| ie(&e))?;

    // 2. Ensure the shard's publications + schema (returns the full pub set).
    let publications =
        zero_cache_change_source::shard_schema::setup_tables_and_replication(&admin, requested)
            .await
            .map_err(|e| ie(&e))?;

    // 3. Create the replication slot (fixes the consistent snapshot). Keep the
    //    raw replication connection alive until after the copy so the exported
    //    snapshot stays valid.
    let mut rconn = zero_cache_change_source::replication_conn::ReplicationConn::connect(
        &params.host,
        params.port,
        &params.user,
        &params.dbname,
    )
    .await
    .map_err(|e| ie(&e))?;
    let slot = rconn
        .create_logical_replication_slot(&params.slot_name)
        .await
        .map_err(|e| ie(&e))?;

    // 4. Snapshot-copy the published schema + data on a dedicated connection.
    let copy_conn = pg_connection::connect(&params.conn_str)
        .await
        .map_err(|e| ie(&e))?;
    let result = run_initial_sync_introspected(
        &copy_conn,
        db,
        &SlotInfo {
            consistent_point: slot.consistent_point.clone(),
            snapshot_name: slot.snapshot_name.clone(),
        },
        &publications,
    )
    .await;

    drop(rconn);
    Ok((result?, publications))
}

async fn copy_all(
    pg: &Client,
    db: &StatementRunner,
    tables: &[PublishedTableSpec],
    indexes: &[IndexSpec],
    replica_version: &str,
) -> Result<Vec<(String, usize)>, InitialSyncError> {
    let applier = DdlApplier::new(db);

    // Create every table first (upstream's createLiteTables), then copy. The
    // lite table name is the mapped spec's `name`.
    let mut lite = Vec::with_capacity(tables.len());
    for spec in tables {
        // `map_postgres_to_lite` operates on a `TableSpec` (schema + column
        // specs); project the published spec down to one, dropping the
        // publication/type-oid metadata it doesn't use.
        let table_spec = published_to_table_spec(spec);
        let lite_spec =
            zero_cache_types::pg_to_lite::map_postgres_to_lite(&table_spec, Some(replica_version))
                .map_err(DdlError::from)?;
        applier.create_table(&lite_spec, replica_version)?;
        lite.push(lite_spec);
    }

    let mut table_rows = Vec::with_capacity(tables.len());
    for (spec, lite_spec) in tables.iter().zip(&lite) {
        // Copy only the upstream columns — NOT the appended `_0_version`
        // column (which exists on the lite table but not upstream, and is
        // filled by the lite table's `DEFAULT '<version>'`, so the INSERT
        // omitting it lands the initial version automatically).
        let cols: Vec<String> = spec.columns.iter().map(|(name, _)| name.clone()).collect();
        let copied = copy_table_binary(pg, db, spec, &cols, &lite_spec.name).await?;
        table_rows.push((lite_spec.name.clone(), copied.rows));
    }

    // Indexes are created after the data is in place (matching upstream's
    // createLiteIndices, which runs post-copy).
    for index in indexes {
        applier.create_index(index, replica_version)?;
    }

    Ok(table_rows)
}

/// Projects a `PublishedTableSpec` down to the `TableSpec` that
/// `map_postgres_to_lite` consumes (schema + `(name, ColumnSpec)` columns +
/// primary key), discarding the publication/`type_oid`/replica-identity
/// metadata that only the copy and replication paths need.
fn published_to_table_spec(spec: &PublishedTableSpec) -> zero_cache_types::specs::TableSpec {
    zero_cache_types::specs::TableSpec {
        name: spec.name.clone(),
        schema: spec.schema.clone(),
        columns: spec
            .columns
            .iter()
            .map(|(name, c)| (name.clone(), c.column.clone()))
            .collect(),
        primary_key: spec.primary_key.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_change_source::pg_connection;
    use zero_cache_change_source::replication_conn::ReplicationConn;
    use zero_cache_types::specs::{ColumnSpec, PublishedColumnSpec};

    fn test_conn_str() -> String {
        std::env::var("ZERO_TEST_PG")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into())
    }

    fn test_host_port() -> (String, u16) {
        let url =
            std::env::var("ZERO_TEST_PG_TCP").unwrap_or_else(|_| "localhost:54329".to_string());
        let (h, p) = url.split_once(':').unwrap();
        (h.to_string(), p.parse().unwrap())
    }

    /// Builds the `PublishedTableSpec` for the `foo(id int pk, name text)`
    /// table the live test creates upstream. Mirrors what `getPublicationInfo`
    /// would introspect (which this driver takes as input rather than running).
    fn foo_spec() -> PublishedTableSpec {
        let col = |name: &str, data_type: &str, pos: i64, type_oid: i64| {
            (
                name.to_string(),
                PublishedColumnSpec {
                    type_oid,
                    column: ColumnSpec::new(data_type, pos),
                },
            )
        };
        PublishedTableSpec {
            oid: 0,
            schema_oid: None,
            replica_identity: None,
            schema: "public".into(),
            name: "foo".into(),
            columns: vec![col("id", "int4", 1, 23), col("name", "text", 2, 25)],
            primary_key: Some(vec!["id".into()]),
            publications: Default::default(),
        }
    }

    #[tokio::test]
    async fn live_initial_sync_copies_tables_at_slot_snapshot() {
        let Ok(pg) = pg_connection::connect(&test_conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        pg.batch_execute(
            "DROP TABLE IF EXISTS foo CASCADE; \
             CREATE TABLE foo(id int primary key, name text); \
             INSERT INTO foo(id, name) VALUES (1, 'one'), (2, 'two'); \
             DROP PUBLICATION IF EXISTS zero_isync_pub; \
             CREATE PUBLICATION zero_isync_pub FOR TABLE foo;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('zero_isync_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'zero_isync_slot');",
        )
        .await
        .ok();

        // Create the slot (fixes the snapshot) over the raw replication conn.
        let (host, port) = test_host_port();
        let mut rconn = ReplicationConn::connect(&host, port, "postgres", "postgres")
            .await
            .unwrap();
        let slot = rconn
            .create_logical_replication_slot("zero_isync_slot")
            .await
            .unwrap();

        // Commit a row AFTER the slot's snapshot — it must NOT be copied.
        pg.batch_execute("INSERT INTO foo(id, name) VALUES (3, 'after')")
            .await
            .unwrap();

        // Run the driver on a dedicated copy connection.
        let copy_conn = pg_connection::connect(&test_conn_str()).await.unwrap();
        let db = StatementRunner::open_in_memory().unwrap();
        let result = run_initial_sync(
            &copy_conn,
            &db,
            &SlotInfo {
                consistent_point: slot.consistent_point.clone(),
                snapshot_name: slot.snapshot_name.clone(),
            },
            &["zero_isync_pub".to_string()],
            &[foo_spec()],
            &[],
        )
        .await
        .unwrap();

        assert_eq!(
            result.table_rows,
            vec![("foo".to_string(), 2)],
            "only pre-snapshot rows copied"
        );
        assert!(!result.replica_version.is_empty());

        // The replica should hold exactly ids 1 and 2 (not 3).
        let rows = db
            .query_uncached("SELECT id FROM foo ORDER BY id", &[])
            .unwrap();
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| match r[0].1 {
                crate::Value::Integer(n) => n,
                ref v => panic!("unexpected id value {v:?}"),
            })
            .collect();
        assert_eq!(
            ids,
            vec![1, 2],
            "row committed after the snapshot must not appear"
        );

        // replicationConfig recorded the publication + version.
        let cfg = db
            .query_uncached(
                r#"SELECT publications, replicaVersion FROM "_zero.replicationConfig""#,
                &[],
            )
            .unwrap();
        assert_eq!(cfg.len(), 1);

        // Cleanup upstream.
        drop(rconn);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('zero_isync_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION zero_isync_pub; DROP TABLE foo;")
            .await
            .unwrap();
    }

    /// The self-driving path: `run_initial_sync_introspected` discovers the
    /// table/index specs itself (no `foo_spec()` handed in) via
    /// `get_publication_info` at the slot snapshot, then copies.
    #[tokio::test]
    async fn live_initial_sync_introspects_its_own_specs() {
        let Ok(pg) = pg_connection::connect(&test_conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS isync_intro CASCADE; \
             CREATE TABLE isync_intro(id int primary key, name text not null); \
             INSERT INTO isync_intro(id, name) VALUES (10, 'a'), (20, 'b'); \
             DROP PUBLICATION IF EXISTS isync_intro_pub; \
             CREATE PUBLICATION isync_intro_pub FOR TABLE isync_intro;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('isync_intro_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'isync_intro_slot');",
        )
        .await
        .ok();

        let (host, port) = test_host_port();
        let mut rconn = ReplicationConn::connect(&host, port, "postgres", "postgres")
            .await
            .unwrap();
        let slot = rconn
            .create_logical_replication_slot("isync_intro_slot")
            .await
            .unwrap();
        pg.batch_execute("INSERT INTO isync_intro(id, name) VALUES (30, 'after')")
            .await
            .unwrap();

        let copy_conn = pg_connection::connect(&test_conn_str()).await.unwrap();
        let db = StatementRunner::open_in_memory().unwrap();
        let result = run_initial_sync_introspected(
            &copy_conn,
            &db,
            &SlotInfo {
                consistent_point: slot.consistent_point.clone(),
                snapshot_name: slot.snapshot_name.clone(),
            },
            &["isync_intro_pub".to_string()],
        )
        .await
        .unwrap();

        assert_eq!(result.table_rows, vec![("isync_intro".to_string(), 2)]);
        let rows = db
            .query_uncached("SELECT id FROM isync_intro ORDER BY id", &[])
            .unwrap();
        let ids: Vec<i64> = rows
            .iter()
            .map(|r| match r[0].1 {
                crate::Value::Integer(n) => n,
                ref v => panic!("unexpected id value {v:?}"),
            })
            .collect();
        assert_eq!(ids, vec![10, 20], "introspected copy respects the snapshot");
        // The introspected schema created the `name` column (proving specs came
        // from get_publication_info, not a hand-built spec).
        let cols = db
            .query_uncached("SELECT name FROM isync_intro ORDER BY id", &[])
            .unwrap();
        assert_eq!(cols.len(), 2);

        drop(rconn);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('isync_intro_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION isync_intro_pub; DROP TABLE isync_intro;")
            .await
            .unwrap();
    }

    /// Initial-sync a table with VARIED column types (bool / numeric / bigint /
    /// timestamptz / jsonb) — exercising each type's binary-COPY field decoder
    /// through the real snapshot-copy pipeline, which the existing int/text-only
    /// tests do not. A broken decoder for any of these would error the copy or
    /// produce a bad insert; here the row copies cleanly and reads back.
    #[tokio::test]
    async fn live_initial_sync_copies_varied_column_types() {
        let Ok(pg) = pg_connection::connect(&test_conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS isync_types CASCADE; \
             CREATE TABLE isync_types( \
               id int primary key, flag boolean, amount numeric, big bigint, \
               ts timestamptz, meta jsonb); \
             INSERT INTO isync_types(id, flag, amount, big, ts, meta) VALUES \
               (1, true, 12.34, 9000000000, '2024-03-15T12:00:00Z', '{\"k\":1}'); \
             DROP PUBLICATION IF EXISTS isync_types_pub; \
             CREATE PUBLICATION isync_types_pub FOR TABLE isync_types;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('isync_types_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'isync_types_slot');",
        )
        .await
        .ok();

        let (host, port) = test_host_port();
        let mut rconn = ReplicationConn::connect(&host, port, "postgres", "postgres")
            .await
            .unwrap();
        let slot = rconn
            .create_logical_replication_slot("isync_types_slot")
            .await
            .unwrap();

        let copy_conn = pg_connection::connect(&test_conn_str()).await.unwrap();
        let db = StatementRunner::open_in_memory().unwrap();
        let result = run_initial_sync_introspected(
            &copy_conn,
            &db,
            &SlotInfo {
                consistent_point: slot.consistent_point.clone(),
                snapshot_name: slot.snapshot_name.clone(),
            },
            &["isync_types_pub".to_string()],
        )
        .await
        .unwrap();

        assert_eq!(
            result.table_rows,
            vec![("isync_types".to_string(), 1)],
            "the varied-type row copied via all field decoders"
        );
        // The row reads back with all typed columns present and non-null.
        let rows = db
            .query_uncached(
                "SELECT id, flag, amount, big, ts, meta FROM isync_types",
                &[],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].1, crate::Value::Integer(1), "id");
        for (i, name) in ["flag", "amount", "big", "ts", "meta"].iter().enumerate() {
            assert_ne!(
                rows[0][i + 1].1,
                crate::Value::Null,
                "{name} decoded to a non-null value"
            );
        }

        drop(rconn);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('isync_types_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION isync_types_pub; DROP TABLE isync_types;")
            .await
            .unwrap();
    }

    /// The single top-level entry point end-to-end: `run_full_initial_sync`
    /// validates the upstream, sets up the shard's publications/schema, creates
    /// the slot, and snapshot-copies — all from connection params + a
    /// `ShardConfig`, with no manual slot/spec wiring.
    #[tokio::test]
    async fn live_run_full_initial_sync_end_to_end() {
        let conn_str = test_conn_str();
        let Ok(pg) = pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        let app = "zerofull";
        // Clean any prior run (shard schema, slot, table, app publication).
        pg.batch_execute(&zero_cache_change_source::shard_schema::drop_shard(app, 0))
            .await
            .ok();
        pg.batch_execute(&format!(r#"DROP SCHEMA IF EXISTS "{app}" CASCADE;"#))
            .await
            .ok();
        pg.batch_execute(
            "DROP TABLE IF EXISTS full_sync_test CASCADE; \
             CREATE TABLE full_sync_test(id int primary key, label text not null); \
             INSERT INTO full_sync_test(id, label) VALUES (1, 'x'), (2, 'y'); \
             DROP PUBLICATION IF EXISTS full_sync_pub; \
             CREATE PUBLICATION full_sync_pub FOR TABLE full_sync_test;",
        )
        .await
        .unwrap();
        let slot_name = format!("{app}_0_full_slot");
        pg.batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{slot_name}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}');"
        ))
        .await
        .ok();

        let (host, port) = test_host_port();
        let params = InitialSyncParams {
            conn_str: conn_str.clone(),
            host,
            port,
            user: "postgres".into(),
            dbname: "postgres".into(),
            slot_name: slot_name.clone(),
        };
        let requested = zero_cache_types::shards::ShardConfig {
            app_id: app.into(),
            shard_num: 0,
            publications: vec!["full_sync_pub".into()],
        };

        let db = StatementRunner::open_in_memory().unwrap();
        let (result, publications) = run_full_initial_sync(&params, &db, &requested)
            .await
            .unwrap();

        // The full publication set includes the app + metadata publication.
        assert!(publications.contains(&"full_sync_pub".to_string()));
        assert!(publications.iter().any(|p| p.contains("_metadata_")));
        // Data copied into the replica. The full publication set includes the
        // shard's internal metadata tables (permissions/clients/mutations), so
        // check the user table specifically rather than the whole set.
        assert_eq!(
            result
                .table_rows
                .iter()
                .find(|(n, _)| n == "full_sync_test"),
            Some(&("full_sync_test".to_string(), 2))
        );
        let rows = db
            .query_uncached("SELECT id FROM full_sync_test ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2);

        // Teardown: drop slot, shard schema, table, publication.
        for _ in 0..20 {
            if pg
                .query(
                    &format!("SELECT pg_drop_replication_slot('{slot_name}')"),
                    &[],
                )
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute(&zero_cache_change_source::shard_schema::drop_shard(app, 0))
            .await
            .ok();
        pg.batch_execute(&format!(r#"DROP SCHEMA IF EXISTS "{app}" CASCADE;"#))
            .await
            .ok();
        pg.batch_execute(
            "DROP PUBLICATION IF EXISTS full_sync_pub; DROP TABLE IF EXISTS full_sync_test;",
        )
        .await
        .unwrap();
    }

    /// End-to-end supervised RESYNC against LIVE Postgres: a replica is built
    /// from one schema, the upstream schema then changes incompatibly, and the
    /// resync path (`reset_replica_for_resync` → `run_initial_sync_introspected`
    /// on the SAME db handle, from a fresh snapshot) rebuilds the replica to
    /// the NEW schema with re-copied data. This is the execution half of the
    /// `Resync` supervisor decision — proving a drifted replica can be brought
    /// back into agreement without a fresh db handle.
    #[tokio::test]
    async fn live_resync_rebuilds_replica_to_the_new_schema() {
        let Ok(pg) = pg_connection::connect(&test_conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS resync_test CASCADE; \
             CREATE TABLE resync_test(id int primary key, name text not null); \
             INSERT INTO resync_test(id, name) VALUES (1, 'one'); \
             DROP PUBLICATION IF EXISTS resync_pub; \
             CREATE PUBLICATION resync_pub FOR TABLE resync_test;",
        )
        .await
        .unwrap();
        let drop_slot = |n: &str| {
            format!(
                "SELECT pg_drop_replication_slot('{n}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{n}');"
            )
        };
        pg.batch_execute(&drop_slot("resync_slot_a")).await.ok();
        pg.batch_execute(&drop_slot("resync_slot_b")).await.ok();

        let (host, port) = test_host_port();
        let db = StatementRunner::open_in_memory().unwrap();

        // ---- Initial sync from the ORIGINAL schema (id, name). ----
        let mut rconn_a = ReplicationConn::connect(&host, port, "postgres", "postgres")
            .await
            .unwrap();
        let slot_a = rconn_a
            .create_logical_replication_slot("resync_slot_a")
            .await
            .unwrap();
        let copy_a = pg_connection::connect(&test_conn_str()).await.unwrap();
        run_initial_sync_introspected(
            &copy_a,
            &db,
            &SlotInfo {
                consistent_point: slot_a.consistent_point.clone(),
                snapshot_name: slot_a.snapshot_name.clone(),
            },
            &["resync_pub".to_string()],
        )
        .await
        .unwrap();
        drop(rconn_a);
        // Replica has the original single-column-plus-name row.
        assert_eq!(
            db.query_uncached("SELECT name FROM resync_test", &[])
                .unwrap()
                .len(),
            1
        );

        // ---- Upstream schema drifts: add a column, change data. ----
        pg.batch_execute(
            "ALTER TABLE resync_test ADD COLUMN priority int; \
             UPDATE resync_test SET priority = 5 WHERE id = 1; \
             INSERT INTO resync_test(id, name, priority) VALUES (2, 'two', 9);",
        )
        .await
        .unwrap();

        // ---- Execute the resync: reset the replica, re-sync from a fresh
        //      snapshot into the SAME db handle. ----
        let dropped = reset_replica_for_resync(&db).unwrap();
        assert!(
            dropped.iter().any(|t| t == "resync_test"),
            "the user table was dropped in the reset, got {dropped:?}"
        );
        // The reset really removed the replica table.
        assert!(db.query_uncached("SELECT 1 FROM resync_test", &[]).is_err());

        let mut rconn_b = ReplicationConn::connect(&host, port, "postgres", "postgres")
            .await
            .unwrap();
        let slot_b = rconn_b
            .create_logical_replication_slot("resync_slot_b")
            .await
            .unwrap();
        let copy_b = pg_connection::connect(&test_conn_str()).await.unwrap();
        run_initial_sync_introspected(
            &copy_b,
            &db,
            &SlotInfo {
                consistent_point: slot_b.consistent_point.clone(),
                snapshot_name: slot_b.snapshot_name.clone(),
            },
            &["resync_pub".to_string()],
        )
        .await
        .unwrap();
        drop(rconn_b);

        // Replica now reflects the NEW schema: the `priority` column exists and
        // both rows (with their new values) were re-copied.
        let rows = db
            .query_uncached("SELECT id, priority FROM resync_test ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2, "both post-drift rows re-copied");
        let priorities: Vec<i64> = rows
            .iter()
            .map(|r| match r[1].1 {
                crate::Value::Integer(n) => n,
                ref v => panic!("unexpected priority {v:?}"),
            })
            .collect();
        assert_eq!(
            priorities,
            vec![5, 9],
            "the newly-added column's data is present post-resync"
        );

        for n in ["resync_slot_a", "resync_slot_b"] {
            for _ in 0..20 {
                if pg
                    .query(&format!("SELECT pg_drop_replication_slot('{n}')"), &[])
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
        pg.batch_execute("DROP PUBLICATION resync_pub; DROP TABLE resync_test;")
            .await
            .unwrap();
    }
}
