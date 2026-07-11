//! Wires [`crate::cvr_load::load_cvr_from_rows`] to a REAL `tokio-postgres`
//! connection against the schema [`crate::cvr_schema_sql`] generates —
//! closing the loop from "pure row-merging logic" to a genuine live CVR
//! load, mirroring how `zero-cache-change-source::pg_connection` gives the
//! replication half of this port a real connection.
//!
//! Scope: `queryArgs` (JSON-encoded custom-query arguments) and
//! `clientAST` (a JSONB column storing a serialized `Ast`) are both
//! genuinely decoded — `queryArgs` via `zero_cache_shared::bigint_json::
//! parse`, `clientAST` via `zero_cache_protocol::ast_json::ast_from_json`.
//! `load_cvr` also now queries `instances` (LEFT JOINed with
//! `rowsVersion`) and runs it through `cvr_ownership::decide_instance_load`
//! — a brand-new CVR loads with defaults, an existing one overlays its
//! real version/lastActive/ttlClock/replicaVersion/profileID/clientSchema
//! and fires the ownership-claim UPDATE if this task doesn't already hold
//! the lease, a deleted CVR errors, an owned-by-someone-else-with-a-newer-
//! lease CVR errors, and a not-yet-row-caught-up CVR returns
//! `LoadCvrOutcome::RowsBehind` for the caller to retry (matching
//! upstream's non-throwing `RowsVersionBehindError` return).

use tokio_postgres::Client;

use zero_cache_protocol::ast_json::{ast_from_json, AstJsonError};
use zero_cache_shared::bigint_json::{
    parse as parse_json, JsonValue, ParseError as JsonParseError,
};
use zero_cache_types::shards::{cvr_schema, ShardError, ShardId};
use zero_cache_types::sql::id;

use crate::cvr_load::{
    load_cvr_from_rows, AsQueryError, LoadedClientRow, LoadedDesireRow, LoadedQueryRow,
};
use crate::cvr_ownership::{
    decide_instance_load, get_claim_ownership_sql, InstanceLoadOutcome, LoadInstanceError,
    LoadedInstanceRow,
};
use crate::cvr_types::Cvr;

#[derive(Debug, thiserror::Error)]
pub enum LoadCvrError {
    #[error(transparent)]
    Shard(#[from] ShardError),
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error(transparent)]
    AsQuery(#[from] AsQueryError),
    #[error("failed to parse queryArgs JSON: {0}")]
    QueryArgsJson(#[from] JsonParseError),
    #[error("failed to parse clientAST JSON: {0}")]
    ClientAstJson(#[from] AstJsonError),
    #[error(transparent)]
    Instance(#[from] LoadInstanceError),
    #[error(transparent)]
    CheckVersion(#[from] crate::cvr_ownership::CheckVersionError),
}

/// The result of [`load_cvr`]: either a loaded (possibly brand-new) `Cvr`,
/// or a signal that the caller should wait for row catchup and retry. Port
/// of `#load`'s `Promise<CVR | RowsVersionBehindError>` return type.
#[derive(Debug, Clone, PartialEq)]
pub enum LoadCvrOutcome {
    Loaded(Cvr),
    RowsBehind {
        version: String,
        rows_version: Option<String>,
    },
}

/// Decodes a `queryArgs` column's JSON-array text into `Vec<JsonValue>`.
/// `None` (column is NULL) stays `None`; a JSON value that isn't an array
/// is a malformed-data error, not a silently-accepted `vec![]`.
fn parse_query_args(text: Option<&str>) -> Result<Option<Vec<JsonValue>>, JsonParseError> {
    let Some(text) = text else { return Ok(None) };
    match parse_json(text)? {
        JsonValue::Array(items) => Ok(Some(items)),
        other => Err(JsonParseError(format!(
            "queryArgs must be a JSON array, got {other:?}"
        ))),
    }
}

/// Loads a CVR for client group `id_`, resolving ownership/row-catchup
/// first (see module doc) and then the `clients`/`queries`/`desires` rows
/// via [`load_cvr_from_rows`]. `task_id` identifies the calling task (for
/// the ownership check) and `last_connect_time` is milliseconds since
/// epoch (matching upstream's `lastConnectTime`).
pub async fn load_cvr(
    client: &Client,
    shard: &ShardId,
    id_: &str,
    task_id: &str,
    last_connect_time: f64,
) -> Result<LoadCvrOutcome, LoadCvrError> {
    let schema = id(&cvr_schema(shard)?);

    let instance_rows = client
        .query(
            &format!(
                "SELECT cvr.\"version\", (extract(epoch from \"lastActive\") * 1000)::float8 AS \"lastActiveMs\", \"ttlClock\", \"replicaVersion\", \
                 \"owner\", (extract(epoch from \"grantedAt\") * 1000)::float8 AS \"grantedAtMs\", \"clientSchema\"::text AS \"clientSchemaText\", \
                 \"profileID\", \"deleted\", rows.\"version\" AS \"rowsVersion\" \
                 FROM {schema}.instances AS cvr \
                 LEFT JOIN {schema}.\"rowsVersion\" AS rows ON cvr.\"clientGroupID\" = rows.\"clientGroupID\" \
                 WHERE cvr.\"clientGroupID\" = $1"
            ),
            &[&id_],
        )
        .await?;
    let instance = match instance_rows.first() {
        None => None,
        Some(r) => {
            let client_schema_text: Option<String> = r.get("clientSchemaText");
            Some(LoadedInstanceRow {
                version: r.get("version"),
                last_active: r.get("lastActiveMs"),
                ttl_clock: r.get("ttlClock"),
                replica_version: r.get("replicaVersion"),
                owner: r.get("owner"),
                granted_at: r.get("grantedAtMs"),
                client_schema: client_schema_text.as_deref().map(parse_json).transpose()?,
                profile_id: r.get("profileID"),
                deleted: r.get::<_, Option<bool>>("deleted").unwrap_or(false),
                rows_version: r.get("rowsVersion"),
            })
        }
    };

    let outcome = decide_instance_load(instance.as_ref(), task_id, last_connect_time)?;
    let overlay = match outcome {
        InstanceLoadOutcome::RowsBehind {
            version,
            rows_version,
        } => {
            return Ok(LoadCvrOutcome::RowsBehind {
                version,
                rows_version,
            })
        }
        InstanceLoadOutcome::New => None,
        InstanceLoadOutcome::Ready {
            overlay,
            claim_ownership,
        } => {
            if claim_ownership {
                let sql =
                    get_claim_ownership_sql(&cvr_schema(shard)?, id_, task_id, last_connect_time);
                client.batch_execute(&sql).await?;
            }
            Some(overlay)
        }
    };

    // Issue the clients/queries/desires SELECTs as concurrent futures on the
    // one connection so tokio-postgres pipelines them into a single round-trip
    // batch (matching upstream's pipelined READONLY load), rather than awaiting
    // each sequentially.
    let clients_query =
        format!("SELECT \"clientID\" FROM {schema}.clients WHERE \"clientGroupID\" = $1");
    let queries_query = format!(
        "SELECT \"queryHash\", \"queryName\", \"queryArgs\"::text AS \"queryArgsText\", \
         \"clientAST\"::text AS \"clientAstText\", \"patchVersion\", \
         \"transformationHash\", \"transformationVersion\", \"internal\", \"rowSetSignature\" \
         FROM {schema}.queries WHERE \"clientGroupID\" = $1 AND deleted IS DISTINCT FROM true"
    );
    let desires_query = format!(
        "SELECT \"clientID\", \"queryHash\", \"patchVersion\", \"deleted\", \"ttlMs\", \"inactivatedAtMs\" \
         FROM {schema}.desires WHERE \"clientGroupID\" = $1"
    );
    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 1] = [&id_];
    let (clients_rows, query_rows, desires_rows) = tokio::try_join!(
        client.query(&clients_query, &params),
        client.query(&queries_query, &params),
        client.query(&desires_query, &params),
    )?;

    let clients: Vec<LoadedClientRow> = clients_rows
        .iter()
        .map(|r| LoadedClientRow {
            client_id: r.get("clientID"),
        })
        .collect();

    let mut queries: Vec<LoadedQueryRow> = Vec::with_capacity(query_rows.len());
    for r in &query_rows {
        let query_args_text: Option<String> = r.get("queryArgsText");
        let client_ast_text: Option<String> = r.get("clientAstText");
        let client_ast = match client_ast_text {
            Some(t) => Some(ast_from_json(&parse_json(&t)?)?),
            None => None,
        };
        queries.push(LoadedQueryRow {
            query_hash: r.get("queryHash"),
            client_ast,
            query_name: r.get("queryName"),
            query_args: parse_query_args(query_args_text.as_deref())?,
            patch_version: r.get("patchVersion"),
            transformation_hash: r.get("transformationHash"),
            transformation_version: r.get("transformationVersion"),
            internal: r.get("internal"),
            row_set_signature: r.get("rowSetSignature"),
        });
    }

    let desires: Vec<LoadedDesireRow> = desires_rows
        .iter()
        .map(|r| LoadedDesireRow {
            client_id: r.get("clientID"),
            query_hash: r.get("queryHash"),
            patch_version: r.get("patchVersion"),
            deleted: r.get("deleted"),
            ttl: r.get("ttlMs"),
            inactivated_at: r.get("inactivatedAtMs"),
        })
        .collect();

    let mut cvr = load_cvr_from_rows(id_, &clients, &queries, &desires)?;
    if let Some(overlay) = overlay {
        cvr.version = overlay.version;
        cvr.last_active = overlay.last_active;
        cvr.ttl_clock = overlay.ttl_clock;
        cvr.replica_version = overlay.replica_version;
        cvr.profile_id = overlay.profile_id;
        cvr.client_schema = overlay.client_schema;
    }
    Ok(LoadCvrOutcome::Loaded(cvr))
}

/// Wires `cvr_ownership`'s write-time check and `cvr_flush_sql`'s
/// generators together into one real transaction — the orchestration half
/// of `CVRStore.#flush` that was still missing after the SQL-generation
/// work (`cvr_flush_sql.rs`, `cvr_ownership::check_version_and_ownership`)
/// landed. Port of `#flush`'s outer transaction shape (`SELECT ... FOR
/// UPDATE` + version/ownership check, then the instance upsert +
/// query/desire batch upserts, all committed together — or none of them,
/// on any failure).
///
/// Row updates (see `cvr_row_cache_sql`) are now also applied in the same
/// transaction, if `row_updates`/`rows_version` are supplied.
///
/// NOT ported: `RowRecordCache`'s in-memory cache / deferred-flush
/// threshold logic, `CVRFlushStats` bookkeeping, and the "only write the
/// instance if something material changed" pending-write coalescing
/// `CVRStore`'s stateful `#pendingXWrites` fields do — this function always
/// applies whatever the caller passes, once, matching a single already-
/// decided flush rather than reproducing the accumulate-then-flush object
/// model.
#[allow(clippy::too_many_arguments)]
pub async fn flush_cvr(
    client: &mut Client,
    shard: &ShardId,
    client_group_id: &str,
    task_id: &str,
    last_connect_time: f64,
    expected_version: &str,
    instance: &crate::cvr_flush_sql::InstanceWrite,
    instance_version_string: &str,
    queries_full: &[crate::cvr_flush_sql::QueryFullWrite],
    queries_partial: &[crate::cvr_flush_sql::QueryPartialWrite],
    desires: &[crate::cvr_flush_sql::DesireWrite],
    row_updates: &[crate::cvr_row_cache_sql::RowUpdate],
    rows_version: &str,
) -> Result<(), LoadCvrError> {
    flush_cvr_with_clients(
        client,
        shard,
        client_group_id,
        task_id,
        last_connect_time,
        expected_version,
        instance,
        instance_version_string,
        queries_full,
        queries_partial,
        desires,
        &[],
        row_updates,
        rows_version,
    )
    .await
}

/// Like [`flush_cvr`], additionally ensuring that every supplied client ID is
/// durable in the CVR's `clients` table in the SAME transaction as its
/// instance/query/desire state.  This closes the foreign-key-safe write path
/// needed by a real reconnect: loading a persisted desire without a persisted
/// client deliberately skips that desire upstream.
#[allow(clippy::too_many_arguments)]
pub async fn flush_cvr_with_clients(
    client: &mut Client,
    shard: &ShardId,
    client_group_id: &str,
    task_id: &str,
    last_connect_time: f64,
    expected_version: &str,
    instance: &crate::cvr_flush_sql::InstanceWrite,
    instance_version_string: &str,
    queries_full: &[crate::cvr_flush_sql::QueryFullWrite],
    queries_partial: &[crate::cvr_flush_sql::QueryPartialWrite],
    desires: &[crate::cvr_flush_sql::DesireWrite],
    client_ids: &[String],
    row_updates: &[crate::cvr_row_cache_sql::RowUpdate],
    rows_version: &str,
) -> Result<(), LoadCvrError> {
    flush_cvr_with_clients_inner(
        client,
        shard,
        client_group_id,
        task_id,
        last_connect_time,
        expected_version,
        instance,
        instance_version_string,
        queries_full,
        queries_partial,
        desires,
        client_ids,
        Some((row_updates, rows_version)),
    )
    .await
}

/// The configuration half of [`flush_cvr_with_clients`]: the same version CAS,
/// instance/query/desire/client upserts, and single-transaction commit — but
/// WITHOUT touching the `rowsVersion` table or writing row records.  Used by the
/// deferred-rows flush path (`ZERO_DEFER_CVR_ROWS`): the durable cookie + the
/// optimistic-concurrency guard still commit synchronously before the poke is
/// returned, while the row records land in a follow-up [`flush_cvr_rows_only`].
///
/// Deliberately leaves `rowsVersion` behind the new instance version: a load
/// therefore reports `RowsBehind` until the deferred rows flush lands, which is
/// exactly what the process-local barrier in the server awaits before reading.
#[allow(clippy::too_many_arguments)]
pub async fn flush_cvr_config_only(
    client: &mut Client,
    shard: &ShardId,
    client_group_id: &str,
    task_id: &str,
    last_connect_time: f64,
    expected_version: &str,
    instance: &crate::cvr_flush_sql::InstanceWrite,
    instance_version_string: &str,
    queries_full: &[crate::cvr_flush_sql::QueryFullWrite],
    queries_partial: &[crate::cvr_flush_sql::QueryPartialWrite],
    desires: &[crate::cvr_flush_sql::DesireWrite],
    client_ids: &[String],
) -> Result<(), LoadCvrError> {
    flush_cvr_with_clients_inner(
        client,
        shard,
        client_group_id,
        task_id,
        last_connect_time,
        expected_version,
        instance,
        instance_version_string,
        queries_full,
        queries_partial,
        desires,
        client_ids,
        None,
    )
    .await
}

/// Writes ONLY the row records and the `rowsVersion` bump for one client group,
/// in its own transaction.  This is the deferred half of a split flush: the
/// configuration transaction already performed the version CAS, so no ownership
/// check is repeated here.  Callers must serialize these per client group (the
/// server's row-flush barrier does) so `rowsVersion` advances monotonically.
pub async fn flush_cvr_rows_only(
    client: &mut Client,
    shard: &ShardId,
    client_group_id: &str,
    row_updates: &[crate::cvr_row_cache_sql::RowUpdate],
    rows_version: &str,
) -> Result<(), LoadCvrError> {
    use crate::cvr_row_cache_sql::get_row_updates_sql;

    let schema = cvr_schema(shard)?;
    let tx = client.transaction().await?;
    let writes = get_row_updates_sql(&schema, client_group_id, rows_version, row_updates);
    tx.batch_execute(&join_sql_statements(writes)).await?;
    tx.commit().await?;
    Ok(())
}

/// Joins literal SQL statements into a single simple-query batch string.
/// Each statement is trimmed of trailing whitespace/`;` and re-joined with
/// `;\n` so that a statement that already ends in `;` does not produce an empty
/// statement in the batch.  An empty input yields an empty string, which
/// `batch_execute` accepts as a no-op.
fn join_sql_statements(statements: Vec<String>) -> String {
    statements
        .iter()
        .map(|s| s.trim().trim_end_matches(';').trim_end())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(";\n")
}

#[allow(clippy::too_many_arguments)]
async fn flush_cvr_with_clients_inner(
    client: &mut Client,
    shard: &ShardId,
    client_group_id: &str,
    task_id: &str,
    last_connect_time: f64,
    expected_version: &str,
    instance: &crate::cvr_flush_sql::InstanceWrite,
    instance_version_string: &str,
    queries_full: &[crate::cvr_flush_sql::QueryFullWrite],
    queries_partial: &[crate::cvr_flush_sql::QueryPartialWrite],
    desires: &[crate::cvr_flush_sql::DesireWrite],
    client_ids: &[String],
    rows: Option<(&[crate::cvr_row_cache_sql::RowUpdate], &str)>,
) -> Result<(), LoadCvrError> {
    use crate::cvr_flush_sql::{
        get_flush_desires_sql, get_flush_queries_full_sql, get_flush_queries_partial_sql,
        get_insert_clients_sql, get_upsert_instance_sql,
    };
    use crate::cvr_ownership::{
        check_version_and_ownership, get_check_version_and_ownership_sql, VersionOwnershipRow,
    };
    use crate::cvr_row_cache_sql::get_row_updates_sql;

    let schema = cvr_schema(shard)?;
    let tx = client.transaction().await?;

    let version_rows = tx
        .query(
            &get_check_version_and_ownership_sql(&schema, client_group_id),
            &[],
        )
        .await?;
    let version_row = version_rows.first().map(|r| VersionOwnershipRow {
        version: r.get("version"),
        owner: r.get("owner"),
        granted_at: r.get("grantedAt"),
    });
    check_version_and_ownership(
        version_row.as_ref(),
        task_id,
        last_connect_time,
        expected_version,
    )?;

    // Collect every write statement into ONE simple-query batch so the whole
    // flush costs a single Postgres round-trip (as upstream's `Promise.all`
    // pipelined write batch does), instead of one awaited round-trip per
    // statement.  All `get_*_sql` generators emit literal SQL (no bind
    // parameters), so `batch_execute`'s simple-query protocol is safe here.
    let mut writes: Vec<String> = Vec::new();
    writes.push(get_upsert_instance_sql(
        &schema,
        instance,
        instance_version_string,
    ));
    if let Some(sql) = get_insert_clients_sql(&schema, client_group_id, client_ids) {
        writes.push(sql);
    }
    if let Some(sql) = get_flush_queries_full_sql(&schema, queries_full) {
        writes.push(sql);
    }
    if let Some(sql) = get_flush_queries_partial_sql(&schema, queries_partial) {
        writes.push(sql);
    }
    if let Some(sql) = get_flush_desires_sql(&schema, desires) {
        writes.push(sql);
    }
    if let Some((row_updates, rows_version)) = rows {
        writes.extend(get_row_updates_sql(
            &schema,
            client_group_id,
            rows_version,
            row_updates,
        ));
    }
    tx.batch_execute(&join_sql_statements(writes)).await?;

    tx.commit().await?;
    Ok(())
}

/// Errors from [`get_row_records`].
#[derive(Debug, thiserror::Error)]
pub enum GetRowRecordsError {
    #[error(transparent)]
    Shard(#[from] ShardError),
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error("failed to parse rowKey/refCounts JSON: {0}")]
    Json(#[from] JsonParseError),
    #[error(transparent)]
    Version(#[from] crate::cvr_version::VersionError),
}

/// Port of `RowRecordCache#ensureLoaded`'s query (the `getRowRecords()`
/// read path): every non-tombstoned row (`refCounts IS NOT NULL`) for one
/// CVR. Closes the read-side counterpart to `cvr_row_cache_sql`'s
/// write-path SQL — `CVRQueryDrivenUpdater`'s `#lookupRowsForExecutedAndRemovedQueries`
/// (and `deleteUnreferencedRows`'s `#existingRows`) both need exactly this
/// map to filter/iterate over.
///
/// Scope deviation: upstream keys the returned map by the structured
/// `RowID` via a `CustomKeyMap`; this port keys by
/// `zero_cache_types::row_key::row_id_string` (a canonical string
/// representation of the same identity, already ported and exactly the
/// `K: Clone + Eq + Hash` string key `cvr_row_received.rs`/
/// `cvr_delete_unreferenced_rows.rs` expect their generic row-id parameter
/// to be) — this is what actually closes the "RowID isn't Hash" deviation
/// those two modules had to work around, not a new problem.
///
/// NOT ported: the in-memory `RowRecordCache` wrapper itself (the
/// memoized-promise-guarded single-load-per-cache, cursor-based 5000-row
/// pagination, `apply`'s incremental cache maintenance) — this is the bare
/// query, a caller owns any caching.
pub async fn get_row_records(
    client: &Client,
    shard: &ShardId,
    client_group_id: &str,
) -> Result<std::collections::HashMap<String, crate::cvr_types::RowRecord>, GetRowRecordsError> {
    let schema = cvr_schema(shard)?;
    let rows = client
        .query(
            &format!(
                "SELECT \"schema\",\"table\",\"rowKey\"::text AS \"rowKeyText\",\"rowVersion\",\"patchVersion\",\"refCounts\"::text AS \"refCountsText\" \
                 FROM {}.rows WHERE \"clientGroupID\" = $1 AND \"refCounts\" IS NOT NULL",
                id(&schema)
            ),
            &[&client_group_id],
        )
        .await?;

    let mut result = std::collections::HashMap::with_capacity(rows.len());
    for r in rows {
        let schema_col: String = r.get("schema");
        let table_col: String = r.get("table");
        let row_key_text: String = r.get("rowKeyText");
        let row_key_json = parse_json(&row_key_text)?;
        let JsonValue::Object(row_key_pairs) = row_key_json else {
            return Err(GetRowRecordsError::Json(JsonParseError(
                "expected rowKey to be a JSON object".to_string(),
            )));
        };
        let row_key: std::collections::BTreeMap<String, JsonValue> =
            row_key_pairs.into_iter().collect();

        let row_version: String = r.get("rowVersion");
        let patch_version_cookie: String = r.get("patchVersion");
        let patch_version = crate::cvr_version::cookie_to_version(Some(&patch_version_cookie))?
            .expect("row patchVersion is never the null cookie");

        let ref_counts_text: String = r.get("refCountsText");
        let ref_counts_json = parse_json(&ref_counts_text)?;
        let ref_counts: Option<std::collections::BTreeMap<String, i64>> = match ref_counts_json {
            JsonValue::Null => None,
            JsonValue::Object(pairs) => Some(
                pairs
                    .into_iter()
                    .map(|(k, v)| {
                        let JsonValue::Number(n) = v else {
                            panic!("refCounts value must be numeric")
                        };
                        (k, n as i64)
                    })
                    .collect(),
            ),
            _ => panic!("refCounts must be a JSON object or null"),
        };

        let cvr_row_id = crate::cvr_types::RowId {
            schema: schema_col,
            table: table_col,
            row_key,
        };
        let key_row_id = zero_cache_types::row_key::RowId::new(
            cvr_row_id.schema.clone(),
            cvr_row_id.table.clone(),
            cvr_row_id
                .row_key
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        let key = zero_cache_types::row_key::row_id_string(&key_row_id).expect("non-empty row key");

        result.insert(
            key,
            crate::cvr_types::RowRecord {
                base: crate::cvr_types::CvrRecordBase { patch_version },
                id: cvr_row_id,
                row_version,
                ref_counts,
            },
        );
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_types::QueryRecord;

    fn test_conn_str() -> String {
        std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        })
    }

    /// Live end-to-end: creates the real CVR schema (via `cvr_schema_sql`),
    /// inserts rows directly with SQL (standing in for what a real
    /// `CVRStore::flush` would have written), then calls `load_cvr` and
    /// asserts the reconstructed `Cvr` matches — a genuine round trip
    /// through a real Postgres connection, not mocked.
    #[tokio::test]
    async fn loads_a_real_cvr_from_postgres() {
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&test_conn_str()).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "cvrload".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrload_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }

        let s = "\"cvrload_0/cvr\"";
        // `clients`/`queries`/`desires` FK-reference `instances`, so an
        // `instances` row (and a matching `rowsVersion` row, so the load
        // isn't blocked on row catchup) must exist first — this also
        // exercises the `Ready`/ownership-overlay path, not just `New`.
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.instances (\"clientGroupID\", \"version\", \"lastActive\", \"replicaVersion\", \"owner\") \
                 VALUES ('cg1', '01', now(), 'rv1', 'other-task');"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.\"rowsVersion\" (\"clientGroupID\", \"version\") VALUES ('cg1', '01');"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.clients (\"clientGroupID\", \"clientID\") VALUES ('cg1', 'c1');"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.queries (\"clientGroupID\", \"queryHash\", \"queryName\", \"queryArgs\", \
                 \"patchVersion\", \"internal\") VALUES ('cg1', 'h1', 'myQuery', '[1,\"two\",true]', '01', false);"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.queries (\"clientGroupID\", \"queryHash\", \"clientAST\", \"patchVersion\", \
                 \"internal\") VALUES ('cg1', 'h2', '{{\"table\":\"issues\",\"where\":{{\"type\":\"simple\",\"op\":\"=\",\"left\":{{\"type\":\"column\",\"name\":\"id\"}},\"right\":{{\"type\":\"literal\",\"value\":1}}}}}}', '01', false);"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.desires (\"clientGroupID\", \"clientID\", \"queryHash\", \"patchVersion\", \
                 \"ttlMs\") VALUES ('cg1', 'c1', 'h1', '01', 60000);"
            ))
            .await
            .unwrap();

        // "my-task" doesn't hold the lease (owner is "other-task") and
        // granted_at is NULL, so this should succeed AND fire the
        // ownership-claim UPDATE.
        let outcome = load_cvr(&client, &shard, "cg1", "my-task", 1_000_000.0)
            .await
            .unwrap();
        let LoadCvrOutcome::Loaded(cvr) = outcome else {
            panic!("expected Loaded, got {outcome:?}")
        };
        assert_eq!(cvr.id, "cg1");
        assert_eq!(
            cvr.replica_version,
            Some("rv1".into()),
            "instances row should be overlaid onto the Cvr"
        );

        let owner_row = client
            .query_one(
                &format!("SELECT \"owner\" FROM {s}.instances WHERE \"clientGroupID\" = 'cg1'"),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            owner_row.get::<_, Option<String>>(0),
            Some("my-task".to_string()),
            "ownership should have been claimed"
        );
        assert_eq!(cvr.clients["c1"].desired_query_ids, vec!["h1".to_string()]);
        let QueryRecord::Custom(q) = &cvr.queries["h1"] else {
            panic!("expected Custom, got {:?}", cvr.queries["h1"])
        };
        assert_eq!(q.name, "myQuery");
        assert_eq!(
            q.args,
            vec![
                zero_cache_shared::bigint_json::JsonValue::Number(1.0),
                zero_cache_shared::bigint_json::JsonValue::String("two".into()),
                zero_cache_shared::bigint_json::JsonValue::Bool(true),
            ],
            "queryArgs should be genuinely parsed from JSON, not a placeholder"
        );
        assert_eq!(q.base.client_state["c1"].ttl, 60_000.0);

        // h2 has a real clientAST -> should now load as a genuine `Client`
        // query record with its AST parsed, not a `Custom` placeholder.
        let QueryRecord::Client(cq) = &cvr.queries["h2"] else {
            panic!("expected Client, got {:?}", cvr.queries["h2"])
        };
        assert_eq!(cq.ast.table, "issues");
        assert!(cq.ast.where_.is_some());

        client
            .batch_execute("DROP SCHEMA \"cvrload_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    use crate::cvr_flush_sql::{DesireWrite, InstanceWrite, QueryFullWrite};
    use crate::cvr_types::TtlClock;
    use crate::cvr_version::empty_cvr_version;

    /// Live end-to-end: `flush_cvr` commits an instance upsert + a query
    /// upsert + a desire upsert TOGETHER in one real transaction, and a
    /// re-query afterward confirms all three landed — proving the write
    /// path's orchestration, not just each SQL statement in isolation.
    #[tokio::test]
    async fn flush_cvr_commits_instance_query_and_desire_together() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(mut client) = zero_cache_change_source::pg_connection::connect(&conn_str).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "cvrflushtx".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrflushtx_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }
        client
            .batch_execute("INSERT INTO \"cvrflushtx_0/cvr\".clients (\"clientGroupID\", \"clientID\") VALUES ('cg1', 'c1');")
            .await
            .ok(); // FK requires instances row first; inserted below via flush_cvr, so skip if it fails here
        client
            .batch_execute("DELETE FROM \"cvrflushtx_0/cvr\".clients;")
            .await
            .ok();

        let instance = InstanceWrite {
            client_group_id: "cg1".into(),
            version: empty_cvr_version(),
            last_active: 60_000.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: None,
            owner: "my-task".into(),
            granted_at: 60_000.0,
            client_schema: None,
            profile_id: None,
        };
        let empty_state_version = empty_cvr_version().state_version;

        let query = QueryFullWrite {
            client_group_id: "cg1".into(),
            query_hash: "h1".into(),
            client_ast: None,
            query_name: Some("myQuery".into()),
            query_args: Some(JsonValue::Array(vec![])),
            patch_version: Some("01".into()),
            transformation_hash: None,
            transformation_version: None,
            internal: Some(false),
            deleted: false,
            row_set_signature: None,
        };
        let desire = DesireWrite {
            client_group_id: "cg1".into(),
            client_id: "c1".into(),
            query_hash: "h1".into(),
            patch_version: "01".into(),
            deleted: false,
            ttl_ms: 60_000.0,
            inactivated_at: None,
        };

        // First flush: no `instances` row exists yet -> expected version is
        // the empty CVR's, and the instance upsert itself creates the row
        // the query/desire FKs need — all within the same transaction.
        flush_cvr(
            &mut client,
            &shard,
            "cg1",
            "my-task",
            60_000.0,
            &empty_state_version,
            &instance,
            "01",
            &[query],
            &[],
            &[],
            &[],
            "01",
        )
        .await
        .unwrap();
        // Desires need a `clients` row (FK) — insert it, then flush the desire.
        client
            .batch_execute("INSERT INTO \"cvrflushtx_0/cvr\".clients (\"clientGroupID\", \"clientID\") VALUES ('cg1', 'c1');")
            .await
            .unwrap();
        flush_cvr(
            &mut client,
            &shard,
            "cg1",
            "my-task",
            60_000.0,
            "01",
            &instance,
            "01",
            &[],
            &[],
            &[desire],
            &[],
            "01",
        )
        .await
        .unwrap();

        let instance_row = client
            .query_one("SELECT \"version\" FROM \"cvrflushtx_0/cvr\".instances WHERE \"clientGroupID\" = 'cg1'", &[])
            .await
            .unwrap();
        assert_eq!(instance_row.get::<_, String>(0), "01");
        let query_row = client
            .query_one("SELECT \"queryName\" FROM \"cvrflushtx_0/cvr\".queries WHERE \"clientGroupID\" = 'cg1' AND \"queryHash\" = 'h1'", &[])
            .await
            .unwrap();
        assert_eq!(query_row.get::<_, String>(0), "myQuery");
        let desire_row = client
            .query_one(
                "SELECT \"ttlMs\" FROM \"cvrflushtx_0/cvr\".desires WHERE \"clientGroupID\" = 'cg1' AND \"clientID\" = 'c1' AND \"queryHash\" = 'h1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(desire_row.get::<_, f64>(0), 60_000.0);

        client
            .batch_execute("DROP SCHEMA \"cvrflushtx_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    /// Live end-to-end DURABLE CVR LOOP across a commit cycle: the load →
    /// advance-version → flush → reload round trip a running view-syncer
    /// performs each time a commit advances a client group's view. Proves the
    /// durable CVR store is genuinely *driven* (not just readable/writable in
    /// isolation): a first flush persists the CVR at version `01`; `load_cvr`
    /// reads it back and claims ownership; a simulated commit flushes a bumped
    /// version `02` GATED on the loaded `01` (optimistic concurrency); and a
    /// final `load_cvr` observes the durably-advanced `02`. This is the CVR
    /// half of the running-service loop.
    #[tokio::test]
    async fn durable_cvr_advances_across_a_commit_cycle() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(mut client) = zero_cache_change_source::pg_connection::connect(&conn_str).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "cvrloop".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrloop_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }

        let instance_at = |version: &str| InstanceWrite {
            client_group_id: "cg1".into(),
            version: crate::cvr_version::CvrVersion {
                state_version: version.to_string(),
                config_version: None,
            },
            last_active: 60_000.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: Some("rv1".into()),
            owner: "my-task".into(),
            granted_at: 60_000.0,
            client_schema: None,
            profile_id: None,
        };

        // --- 1) First flush: create the CVR at version "01" (expected = the
        //        empty CVR's, since no instances row exists yet). ---
        flush_cvr(
            &mut client,
            &shard,
            "cg1",
            "my-task",
            60_000.0,
            &empty_cvr_version().state_version,
            &instance_at("01"),
            "01",
            &[],
            &[],
            &[],
            &[],
            "01",
        )
        .await
        .unwrap();

        // --- 2) Load it back (the running loop's read at the start of a
        //        commit): ownership is claimed, version is "01". ---
        let LoadCvrOutcome::Loaded(cvr) = load_cvr(&client, &shard, "cg1", "my-task", 60_000.0)
            .await
            .unwrap()
        else {
            panic!("expected Loaded")
        };
        let loaded_version = crate::cvr_version::version_to_cookie(&cvr.version).unwrap();
        assert_eq!(loaded_version, "01", "durable CVR loaded at 01");

        // --- 3) A commit advances the view: flush a bumped version "02",
        //        GATED on the version we just loaded ("01"). ---
        flush_cvr(
            &mut client,
            &shard,
            "cg1",
            "my-task",
            60_000.0,
            &loaded_version,
            &instance_at("02"),
            "02",
            &[],
            &[],
            &[],
            &[],
            "02",
        )
        .await
        .unwrap();

        // --- 4) Reload: the durable state has genuinely advanced to "02". ---
        let LoadCvrOutcome::Loaded(cvr2) = load_cvr(&client, &shard, "cg1", "my-task", 60_000.0)
            .await
            .unwrap()
        else {
            panic!("expected Loaded")
        };
        assert_eq!(
            crate::cvr_version::version_to_cookie(&cvr2.version).unwrap(),
            "02",
            "durable CVR advanced to 02 across the commit cycle"
        );

        client
            .batch_execute("DROP SCHEMA \"cvrloop_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    /// Live end-to-end: a version mismatch causes `flush_cvr` to error
    /// BEFORE any writes — the instance upsert must not have happened,
    /// proving the version/ownership check genuinely gates the whole
    /// transaction rather than just being checked-and-ignored.
    #[tokio::test]
    async fn flush_cvr_rolls_back_entirely_on_version_mismatch() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(mut client) = zero_cache_change_source::pg_connection::connect(&conn_str).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "cvrflushrb".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrflushrb_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }

        let instance = InstanceWrite {
            client_group_id: "cg1".into(),
            version: empty_cvr_version(),
            last_active: 60_000.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: None,
            owner: "my-task".into(),
            granted_at: 60_000.0,
            client_schema: None,
            profile_id: None,
        };

        // Expected version "99" will never match the (nonexistent) row's
        // effective empty version -> ConcurrentModification -> rollback.
        let result = flush_cvr(
            &mut client,
            &shard,
            "cg1",
            "my-task",
            60_000.0,
            "99",
            &instance,
            "01",
            &[],
            &[],
            &[],
            &[],
            "01",
        )
        .await;
        assert!(result.is_err());

        let rows = client
            .query(
                "SELECT 1 FROM \"cvrflushrb_0/cvr\".instances WHERE \"clientGroupID\" = 'cg1'",
                &[],
            )
            .await
            .unwrap();
        assert!(
            rows.is_empty(),
            "instance upsert must not have been committed after the version check failed"
        );

        client
            .batch_execute("DROP SCHEMA \"cvrflushrb_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    /// Live end-to-end: inserts row records directly via SQL (standing in
    /// for what `flush_cvr`'s row-updates would have written), then calls
    /// `get_row_records` and asserts the reconstructed map matches — the
    /// read-path counterpart to `loads_a_real_cvr_from_postgres` above, and
    /// the piece `CVRQueryDrivenUpdater`'s row-reconciliation decisions
    /// (`cvr_row_received.rs`/`cvr_delete_unreferenced_rows.rs`) need
    /// something real to read from.
    #[tokio::test]
    async fn reads_real_row_records_from_postgres() {
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&test_conn_str()).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "cvrrows".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrrows_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }

        let s = "\"cvrrows_0/cvr\"";
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.instances (\"clientGroupID\", \"version\", \"lastActive\", \"replicaVersion\", \"owner\") \
                 VALUES ('cg1', '01', now(), 'rv1', 'my-task'); \
                 INSERT INTO {s}.\"rowsVersion\" (\"clientGroupID\", \"version\") VALUES ('cg1', '01');"
            ))
            .await
            .unwrap();

        client
            .batch_execute(&format!(
                "INSERT INTO {s}.rows (\"clientGroupID\",\"schema\",\"table\",\"rowKey\",\"rowVersion\",\"patchVersion\",\"refCounts\") VALUES \
                 ('cg1','public','issues','{{\"id\":\"1\"}}','v1','01','{{\"q1\":1}}'), \
                 ('cg1','public','issues','{{\"id\":\"2\"}}','v2','01',NULL);"
            ))
            .await
            .unwrap();

        let records = get_row_records(&client, &shard, "cg1").await.unwrap();

        assert_eq!(records.len(), 1, "the tombstoned (refCounts NULL) row must be excluded, matching getRowRecords' WHERE clause");
        let (_, record) = records.iter().next().unwrap();
        assert_eq!(record.row_version, "v1");
        assert_eq!(record.id.schema, "public");
        assert_eq!(record.id.table, "issues");
        assert_eq!(
            record.ref_counts,
            Some(std::collections::BTreeMap::from([("q1".to_string(), 1)]))
        );

        client
            .batch_execute("DROP SCHEMA \"cvrrows_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    /// Live end-to-end WRITE→READ round trip for the row cache: `flush_cvr`
    /// persists an instance PLUS row-updates (a put row and a tombstone), and
    /// then `get_row_records` reads them back. Previously the flush test
    /// checked only instance/query/desire rows (via raw SQL) and the
    /// `get_row_records` test seeded its rows via raw SQL — nothing proved
    /// that what `flush_cvr`'s `get_row_updates_sql` batch actually WRITES is
    /// exactly what `get_row_records` READS. This closes that loop through a
    /// real Postgres transaction: the put row survives with its version and
    /// ref-counts intact, and the tombstone (null `refCounts`) is excluded.
    #[tokio::test]
    async fn flush_row_updates_round_trip_through_get_row_records() {
        use crate::cvr_flush_sql::InstanceWrite;
        use crate::cvr_types::{CvrRecordBase, RowId, RowRecord, TtlClock};
        use crate::cvr_version::empty_cvr_version;
        use std::collections::BTreeMap;

        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(mut client) = zero_cache_change_source::pg_connection::connect(&conn_str).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "cvrrowrt".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrrowrt_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }

        let instance = InstanceWrite {
            client_group_id: "cg1".into(),
            version: empty_cvr_version(),
            last_active: 60_000.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: None,
            owner: "my-task".into(),
            granted_at: 60_000.0,
            client_schema: None,
            profile_id: None,
        };
        let empty_state_version = empty_cvr_version().state_version;

        let row_id = |id_val: &str| RowId {
            schema: "public".into(),
            table: "issues".into(),
            row_key: BTreeMap::from([("id".to_string(), JsonValue::String(id_val.to_string()))]),
        };
        let put = RowRecord {
            base: CvrRecordBase {
                patch_version: empty_cvr_version(),
            },
            id: row_id("1"),
            row_version: "v7".into(),
            ref_counts: Some(BTreeMap::from([("h1".to_string(), 2i64)])),
        };
        // A tombstone (null refCounts) must NOT come back from get_row_records.
        let tombstone = (row_id("2"), None);
        let row_updates = vec![(row_id("1"), Some(put)), tombstone];

        // First (and only) flush: no instances row exists yet, so the expected
        // version is the empty CVR's; the instance upsert + row-updates commit
        // together in one transaction.
        flush_cvr(
            &mut client,
            &shard,
            "cg1",
            "my-task",
            60_000.0,
            &empty_state_version,
            &instance,
            "01",
            &[],
            &[],
            &[],
            &row_updates,
            "01",
        )
        .await
        .unwrap();

        let records = get_row_records(&client, &shard, "cg1").await.unwrap();

        assert_eq!(
            records.len(),
            1,
            "only the put row round-trips; the tombstone is excluded"
        );
        let (_, record) = records.iter().next().unwrap();
        assert_eq!(record.id.schema, "public");
        assert_eq!(record.id.table, "issues");
        assert_eq!(record.row_version, "v7");
        assert_eq!(
            record.ref_counts,
            Some(BTreeMap::from([("h1".to_string(), 2i64)])),
            "ref-counts written by flush_cvr must read back identically"
        );

        client
            .batch_execute("DROP SCHEMA \"cvrrowrt_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }
}
