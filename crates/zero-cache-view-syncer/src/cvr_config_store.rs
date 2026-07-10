//! Durable CVR configuration persistence.
//!
//! The upstream `CVRStore` persists an instance, clients, query records, and
//! desired-query rows together whenever a connection changes its query set.
//! The port already had the PostgreSQL load and flush primitives, but no
//! adapter from the in-memory [`Cvr`] that the live handler owns to those SQL
//! write shapes.  This module is that adapter.  It intentionally concerns only
//! configuration state; row-record persistence remains the responsibility of
//! the hydration/row-cache layer.

use std::collections::BTreeMap;

use tokio_postgres::Client;

use zero_cache_protocol::ast_json::ast_to_json;
use zero_cache_types::shards::ShardId;

use crate::cvr_flush_sql::{DesireWrite, InstanceWrite, QueryFullWrite, QueryPartialWrite};
use crate::cvr_row_cache_sql::RowUpdate;
use crate::cvr_store_pg::{flush_cvr_with_clients, LoadCvrError};
use crate::cvr_types::{ClientQueryState, Cvr, ExternalQueryBase, QueryRecord};
use crate::cvr_version::{version_to_cookie, CvrVersion, VersionError};

/// All SQL-row shapes derived from one before/after CVR transition.
///
/// Keeping this materialized representation public makes the persistence
/// boundary independently testable and lets the live handler construct it
/// before it sends a poke.  That ordering is important: a reconnect must
/// never observe a cookie the server has advertised but failed to persist.
#[derive(Debug, Clone, PartialEq)]
pub struct CvrConfigWrites {
    pub instance: InstanceWrite,
    pub instance_version: String,
    pub client_ids: Vec<String>,
    pub queries: Vec<QueryFullWrite>,
    pub desires: Vec<DesireWrite>,
    /// The configuration-only store does not yet own row records, but it must
    /// keep `rowsVersion` aligned with the instance so loading this CVR does
    /// not permanently report `RowsBehind`.  Reconnect hydration rebuilds the
    /// corresponding row state from the replica.
    pub rows_version: String,
}

#[derive(Debug, thiserror::Error)]
pub enum CvrConfigStoreError {
    #[error(transparent)]
    Version(#[from] VersionError),
    #[error(transparent)]
    Store(#[from] LoadCvrError),
}

fn maybe_version(v: &Option<CvrVersion>) -> Result<Option<String>, VersionError> {
    v.as_ref().map(version_to_cookie).transpose()
}

fn external_query_write(
    client_group_id: &str,
    base: &ExternalQueryBase,
    ast: Option<zero_cache_shared::bigint_json::JsonValue>,
    name: Option<String>,
    args: Option<zero_cache_shared::bigint_json::JsonValue>,
) -> Result<QueryFullWrite, VersionError> {
    Ok(QueryFullWrite {
        client_group_id: client_group_id.to_string(),
        query_hash: base.id.clone(),
        client_ast: ast,
        query_name: name,
        query_args: args,
        patch_version: maybe_version(&base.patch_version)?,
        transformation_hash: base.transformation_hash.clone(),
        transformation_version: maybe_version(&base.transformation_version)?,
        internal: Some(false),
        deleted: false,
        row_set_signature: base.row_set_signature.clone(),
    })
}

fn query_write(cvr: &Cvr, query: &QueryRecord) -> Result<QueryFullWrite, VersionError> {
    match query {
        QueryRecord::Client(q) => {
            external_query_write(&cvr.id, &q.base, Some(ast_to_json(&q.ast)), None, None)
        }
        QueryRecord::Custom(q) => external_query_write(
            &cvr.id,
            &q.base,
            None,
            Some(q.name.clone()),
            Some(zero_cache_shared::bigint_json::JsonValue::Array(
                q.args.clone(),
            )),
        ),
        QueryRecord::Internal(q) => Ok(QueryFullWrite {
            client_group_id: cvr.id.clone(),
            query_hash: q.id.clone(),
            client_ast: Some(ast_to_json(&q.ast)),
            query_name: None,
            query_args: None,
            patch_version: None,
            transformation_hash: q.transformation_hash.clone(),
            transformation_version: maybe_version(&q.transformation_version)?,
            internal: Some(true),
            deleted: false,
            row_set_signature: q.row_set_signature.clone(),
        }),
    }
}

fn active_desire(
    client_group_id: &str,
    client_id: &str,
    query_hash: &str,
    state: &ClientQueryState,
) -> Result<DesireWrite, VersionError> {
    Ok(DesireWrite {
        client_group_id: client_group_id.to_string(),
        client_id: client_id.to_string(),
        query_hash: query_hash.to_string(),
        patch_version: version_to_cookie(&state.version)?,
        deleted: state.deleted,
        ttl_ms: state.ttl,
        inactivated_at: state.inactivated_at.map(|v| v.0),
    })
}

fn external_client_states(cvr: &Cvr) -> BTreeMap<(String, String), ClientQueryState> {
    let mut states = BTreeMap::new();
    for (query_hash, query) in &cvr.queries {
        let clients = match query {
            QueryRecord::Client(q) => Some(&q.base.client_state),
            QueryRecord::Custom(q) => Some(&q.base.client_state),
            QueryRecord::Internal(_) => None,
        };
        if let Some(clients) = clients {
            for (client_id, state) in clients {
                states.insert((client_id.clone(), query_hash.clone()), state.clone());
            }
        }
    }
    states
}

/// Converts a CVR transition into durable writes.
///
/// `before` is deliberately required.  A hard desired-query delete removes
/// the in-memory client state, while the durable protocol needs a tombstone so
/// another connection can catch up.  Diffing the two snapshots retains that
/// critical information instead of silently resurrecting deleted queries on a
/// reconnect.
pub fn config_writes_from_transition(
    before: &Cvr,
    after: &Cvr,
    task_id: &str,
    now_ms: f64,
) -> Result<CvrConfigWrites, VersionError> {
    let instance_version = version_to_cookie(&after.version)?;
    let mut queries = Vec::with_capacity(after.queries.len());
    for query in after.queries.values() {
        queries.push(query_write(after, query)?);
    }
    queries.sort_by(|a, b| a.query_hash.cmp(&b.query_hash));

    let before_states = external_client_states(before);
    let after_states = external_client_states(after);
    let mut desires = Vec::with_capacity(after_states.len() + before_states.len());
    for ((client_id, query_hash), state) in &after_states {
        desires.push(active_desire(&after.id, client_id, query_hash, state)?);
    }
    // A query explicitly removed by the client has no state in `after`.  The
    // upstream store writes a deleted desire row at the transition's new CVR
    // version; reproduce that exact durable signal here.
    for ((client_id, query_hash), state) in &before_states {
        if !after_states.contains_key(&(client_id.clone(), query_hash.clone())) {
            desires.push(DesireWrite {
                client_group_id: after.id.clone(),
                client_id: client_id.clone(),
                query_hash: query_hash.clone(),
                patch_version: instance_version.clone(),
                deleted: true,
                ttl_ms: state.ttl,
                inactivated_at: None,
            });
        }
    }
    desires.sort_by(|a, b| (&a.client_id, &a.query_hash).cmp(&(&b.client_id, &b.query_hash)));

    let mut client_ids: Vec<String> = after.clients.keys().cloned().collect();
    client_ids.sort();

    Ok(CvrConfigWrites {
        instance: InstanceWrite {
            client_group_id: after.id.clone(),
            version: after.version.clone(),
            last_active: now_ms,
            ttl_clock: after.ttl_clock,
            replica_version: after.replica_version.clone(),
            owner: task_id.to_string(),
            granted_at: now_ms,
            client_schema: after.client_schema.clone(),
            profile_id: after.profile_id.clone(),
        },
        instance_version: instance_version.clone(),
        client_ids,
        queries,
        desires,
        rows_version: instance_version,
    })
}

/// Atomically persists the configuration changes represented by a CVR
/// transition.  `expected_before_version` must be the version before the
/// handler applied an incoming message; PostgreSQL checks it under a row lock
/// to reject concurrent writers rather than silently dropping a client update.
pub async fn flush_cvr_config_transition(
    client: &mut Client,
    shard: &ShardId,
    task_id: &str,
    last_connect_time_ms: f64,
    expected_before_version: &CvrVersion,
    before: &Cvr,
    after: &Cvr,
) -> Result<(), CvrConfigStoreError> {
    flush_cvr_config_transition_with_rows(
        client,
        shard,
        task_id,
        last_connect_time_ms,
        expected_before_version,
        before,
        after,
        &[],
    )
    .await
}

/// Like [`flush_cvr_config_transition`] but includes the row-cache updates
/// produced while hydrating the same transition.  Keeping metadata and row
/// records in the same PostgreSQL transaction is what makes a reconnect's
/// `rowsVersion` claim trustworthy: a client never observes a config cookie
/// whose corresponding rows have not been committed yet.
pub async fn flush_cvr_config_transition_with_rows(
    client: &mut Client,
    shard: &ShardId,
    task_id: &str,
    last_connect_time_ms: f64,
    expected_before_version: &CvrVersion,
    before: &Cvr,
    after: &Cvr,
    row_updates: &[RowUpdate],
) -> Result<(), CvrConfigStoreError> {
    let writes = config_writes_from_transition(before, after, task_id, last_connect_time_ms)?;
    let expected_version = version_to_cookie(expected_before_version)?;
    flush_cvr_with_clients(
        client,
        shard,
        &after.id,
        task_id,
        last_connect_time_ms,
        &expected_version,
        &writes.instance,
        &writes.instance_version,
        &writes.queries,
        &[] as &[QueryPartialWrite],
        &writes.desires,
        &writes.client_ids,
        row_updates,
        &writes.rows_version,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use zero_cache_protocol::ast::Ast;
    use zero_cache_zql::ttl::Ttl;

    use super::*;
    use crate::cvr_desired_queries::{delete_queries, put_desired_queries, DesiredQueryRequest};
    use crate::cvr_schema_sql::create_cvr_schema_statements;
    use crate::cvr_store_pg::{load_cvr, LoadCvrOutcome};
    use crate::cvr_types::{Cvr, TtlClock};
    use crate::cvr_version::empty_cvr_version;

    fn empty_cvr() -> Cvr {
        Cvr {
            id: "cg1".into(),
            version: empty_cvr_version(),
            last_active: 0.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: Some("01".into()),
            clients: BTreeMap::new(),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        }
    }

    fn put(cvr: &mut Cvr) {
        let before = cvr.version.clone();
        put_desired_queries(
            cvr,
            &before,
            "c1",
            &[DesiredQueryRequest {
                hash: "q1".into(),
                ast: Some(Ast::table("issues")),
                name: None,
                args: None,
                ttl: Some(Ttl::Millis(1234.0)),
            }],
        );
    }

    #[test]
    fn transition_writes_include_client_query_and_desire() {
        let before = empty_cvr();
        let mut after = before.clone();
        put(&mut after);

        let writes = config_writes_from_transition(&before, &after, "task-1", 1000.0).unwrap();
        assert_eq!(writes.client_ids, vec!["c1"]);
        assert_eq!(writes.queries.len(), 1);
        assert_eq!(writes.queries[0].query_hash, "q1");
        let Some(zero_cache_shared::bigint_json::JsonValue::Object(ast)) =
            &writes.queries[0].client_ast
        else {
            panic!("client query must persist its AST")
        };
        assert!(ast.iter().any(|(key, value)| {
            key == "table"
                && value == &zero_cache_shared::bigint_json::JsonValue::String("issues".into())
        }));
        assert_eq!(writes.desires.len(), 1);
        assert!(!writes.desires[0].deleted);
        assert_eq!(writes.desires[0].ttl_ms, 1234.0);
        assert_eq!(writes.instance.owner, "task-1");
    }

    #[test]
    fn transition_writes_tombstone_a_hard_desired_query_delete() {
        let mut before = empty_cvr();
        put(&mut before);
        let mut after = before.clone();
        let version = after.version.clone();
        delete_queries(&mut after, &version, "c1", &["q1".into()], None);

        let writes = config_writes_from_transition(&before, &after, "task-1", 1000.0).unwrap();
        assert_eq!(writes.desires.len(), 1);
        assert!(writes.desires[0].deleted);
        assert_eq!(writes.desires[0].query_hash, "q1");
        assert_eq!(writes.desires[0].patch_version, writes.instance_version);
    }

    #[tokio::test]
    async fn config_transition_round_trips_and_persists_a_delete_tombstone() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(mut client) = zero_cache_change_source::pg_connection::connect(&conn_str).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "cvrconfigstore".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrconfigstore_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for statement in create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&statement).await.unwrap();
        }

        let before = empty_cvr();
        let mut after = before.clone();
        put(&mut after);
        flush_cvr_config_transition(
            &mut client,
            &shard,
            "task-1",
            1_000.0,
            &before.version,
            &before,
            &after,
        )
        .await
        .unwrap();

        let LoadCvrOutcome::Loaded(loaded) = load_cvr(&client, &shard, "cg1", "task-1", 1_000.0)
            .await
            .unwrap()
        else {
            panic!("freshly flushed config must be caught up")
        };
        assert_eq!(loaded.clients["c1"].desired_query_ids, vec!["q1"]);
        assert!(loaded.queries.contains_key("q1"));

        let before_delete = after.clone();
        let mut after_delete = after;
        let version = after_delete.version.clone();
        delete_queries(&mut after_delete, &version, "c1", &["q1".into()], None);
        flush_cvr_config_transition(
            &mut client,
            &shard,
            "task-1",
            2_000.0,
            &before_delete.version,
            &before_delete,
            &after_delete,
        )
        .await
        .unwrap();

        let LoadCvrOutcome::Loaded(loaded_after_delete) =
            load_cvr(&client, &shard, "cg1", "task-1", 2_000.0)
                .await
                .unwrap()
        else {
            panic!("deleted config must remain caught up")
        };
        assert!(loaded_after_delete.clients["c1"]
            .desired_query_ids
            .is_empty());
        let row = client
            .query_one(
                "SELECT deleted FROM \"cvrconfigstore_0/cvr\".desires WHERE \"clientGroupID\" = 'cg1' AND \"clientID\" = 'c1' AND \"queryHash\" = 'q1'",
                &[],
            )
            .await
            .unwrap();
        assert!(row.get::<_, Option<bool>>(0).unwrap_or(false));

        client
            .batch_execute("DROP SCHEMA \"cvrconfigstore_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }
}
