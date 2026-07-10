//! Port of the SQL-generating half of `CVRStore.putInstance` — the first
//! slice of `CVRStore`'s WRITE path (`#flush`), which this port hasn't
//! touched until now (only `#load` — schema, row-merge, ownership — has
//! been ported so far). `putInstance` queues the `instances` row upsert
//! that every `#flush` call includes when there's anything material to
//! flush ("the CVR instance itself is only updated if there are material
//! changes to flush").
//!
//! Also now covers `#flushDesires` (a single-form bulk `json_to_recordset`
//! upsert into `desires`).
//!
//! Scope: SQL text generation only, matching this port's established
//! pattern (`zero-cache-sqlite::create`, `zero-cache-mutagen::sql`,
//! `cvr_schema_sql`, `last_mutation_id::get_upsert_last_mutation_id_sql`)
//! of porting a statement's SQL shape before the live transaction/pipeline
//! machinery around it. Rows are inlined as a JSON array literal
//! (`json_to_recordset('[...]'::json)`) rather than passed as a bound
//! parameter, since this crate has no parameterized-query builder for bulk
//! array parameters — values are still escaped via `bigint_json::stringify`
//! so this remains injection-safe, same representation choice already made
//! for `zero-cache-mutagen::sql`. NOT ported: `#flushQueries` (has TWO
//! upsert forms — full updates and partial-only column updates — more
//! complex than `#flushDesires`'s single form; a further increment),
//! `#checkVersionAndOwnership` (now in `cvr_ownership.rs`), the row-cache
//! (`#rowCache.executeRowUpdates`/`apply`), or `#flush`'s overall
//! transaction orchestration.

use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_types::sql::{id, lit};

use crate::cvr_types::TtlClock;
use crate::cvr_version::CvrVersion;

/// The `instances` row `putInstance` upserts. Port of the inline `change:
/// InstancesRow` object built in `putInstance`.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceWrite {
    pub client_group_id: String,
    pub version: CvrVersion,
    /// Milliseconds since epoch.
    pub last_active: f64,
    pub ttl_clock: TtlClock,
    pub replica_version: Option<String>,
    pub owner: String,
    /// Milliseconds since epoch — port of `lastConnectTime`, the value
    /// `#flush` passes as `grantedAt`.
    pub granted_at: f64,
    pub client_schema: Option<JsonValue>,
    pub profile_id: Option<String>,
}

/// Builds the batched insert used to make a CVR's known clients durable.
///
/// Upstream's `CVRStore.insertClient()` queues one `INSERT` per client.  The
/// Rust port flushes a whole CVR configuration at once, so a JSON-recordset
/// insert is both equivalent and avoids a round trip per client.  The rows
/// are inserted after the `instances` upsert (which owns the foreign key) and
/// before any subsequent reconnect can load the CVR.
pub fn get_insert_clients_sql(
    cvr_schema: &str,
    client_group_id: &str,
    client_ids: &[String],
) -> Option<String> {
    if client_ids.is_empty() {
        return None;
    }

    let mut client_ids = client_ids.to_vec();
    client_ids.sort();
    client_ids.dedup();
    let rows = JsonValue::Array(
        client_ids
            .into_iter()
            .map(|client_id| {
                JsonValue::Object(vec![
                    (
                        "clientGroupID".into(),
                        JsonValue::String(client_group_id.to_string()),
                    ),
                    ("clientID".into(), JsonValue::String(client_id)),
                ])
            })
            .collect(),
    );
    let rows_json = lit(&rows.stringify());
    let schema = id(cvr_schema);
    Some(format!(
        "INSERT INTO {schema}.clients (\"clientGroupID\",\"clientID\") \
         SELECT \"clientGroupID\",\"clientID\" \
         FROM json_to_recordset({rows_json}::json) AS x(\"clientGroupID\" TEXT,\"clientID\" TEXT) \
         ON CONFLICT (\"clientGroupID\",\"clientID\") DO NOTHING"
    ))
}

fn timestamp_ms_sql(ms: f64) -> String {
    format!("to_timestamp({})", ms / 1000.0)
}

fn nullable_text_sql(v: &Option<String>) -> String {
    match v {
        Some(s) => lit(s),
        None => "NULL".to_string(),
    }
}

fn nullable_jsonb_sql(v: &Option<JsonValue>) -> String {
    match v {
        Some(json) => format!("{}::jsonb", lit(&json.stringify())),
        None => "NULL".to_string(),
    }
}

/// Port of `putInstance`'s `INSERT ... ON CONFLICT ("clientGroupID") DO
/// UPDATE SET ...` template. `version_string` is the caller-supplied
/// `versionString(version)` result (this crate's `cvr_version` module
/// already has that conversion — kept as a parameter here rather than
/// re-deriving it, to keep this function focused on SQL shape).
pub fn get_upsert_instance_sql(
    cvr_schema: &str,
    write: &InstanceWrite,
    version_string: &str,
) -> String {
    let columns = [
        "\"clientGroupID\"",
        "\"version\"",
        "\"lastActive\"",
        "\"ttlClock\"",
        "\"replicaVersion\"",
        "\"owner\"",
        "\"grantedAt\"",
        "\"clientSchema\"",
        "\"profileID\"",
    ];
    let values = vec![
        lit(&write.client_group_id),
        lit(version_string),
        timestamp_ms_sql(write.last_active),
        write.ttl_clock.as_number().to_string(),
        nullable_text_sql(&write.replica_version),
        lit(&write.owner),
        timestamp_ms_sql(write.granted_at),
        nullable_jsonb_sql(&write.client_schema),
        nullable_text_sql(&write.profile_id),
    ];
    let set_clause: Vec<String> = columns
        .iter()
        .zip(&values)
        .map(|(c, v)| format!("{c} = {v}"))
        .collect();
    format!(
        "INSERT INTO {}.instances ({}) VALUES ({}) ON CONFLICT (\"clientGroupID\") DO UPDATE SET {}",
        id(cvr_schema),
        columns.join(","),
        values.join(","),
        set_clause.join(","),
    )
}

/// One pending `desires` row update. Port of the object `#flushDesires`
/// builds from `#pendingDesireUpdates` entries (`{clientGroupID, clientID,
/// queryHash, patchVersion, deleted, ttl}`, pre-`convertTTLValues`).
#[derive(Debug, Clone, PartialEq)]
pub struct DesireWrite {
    pub client_group_id: String,
    pub client_id: String,
    pub query_hash: String,
    pub patch_version: String,
    pub deleted: bool,
    /// Milliseconds; negative (upstream's convention: `ttl ?? -1`) means
    /// "forever" and is dropped (both `ttl`/`ttlMs` columns become NULL).
    pub ttl_ms: f64,
    /// Milliseconds, if the client-query pair has been inactivated.
    pub inactivated_at: Option<f64>,
}

/// The four derived fields `convertTTLValues` computes per row. Port of its
/// return type.
struct ConvertedTtl {
    ttl_interval_seconds: Option<f64>,
    ttl_ms: Option<f64>,
    inactivated_at_timestamp: Option<f64>,
    inactivated_at_ms: Option<f64>,
}

fn convert_ttl_values(inactivated_at: Option<f64>, ttl_ms: f64) -> ConvertedTtl {
    ConvertedTtl {
        ttl_interval_seconds: if ttl_ms < 0.0 {
            None
        } else {
            Some(ttl_ms / 1000.0)
        },
        ttl_ms: if ttl_ms < 0.0 { None } else { Some(ttl_ms) },
        inactivated_at_timestamp: inactivated_at.map(|ms| ms / 1000.0),
        inactivated_at_ms: inactivated_at,
    }
}

fn json_number_or_null(v: Option<f64>) -> JsonValue {
    v.map(JsonValue::Number).unwrap_or(JsonValue::Null)
}

/// Port of `#flushDesires`. Returns `None` for an empty `rows` (matches
/// upstream's `if (this.#pendingDesireUpdates.size === 0) return null`).
pub fn get_flush_desires_sql(cvr_schema: &str, rows: &[DesireWrite]) -> Option<String> {
    if rows.is_empty() {
        return None;
    }

    let json_rows: Vec<JsonValue> = rows
        .iter()
        .map(|row| {
            let converted = convert_ttl_values(row.inactivated_at, row.ttl_ms);
            JsonValue::Object(vec![
                (
                    "clientGroupID".into(),
                    JsonValue::String(row.client_group_id.clone()),
                ),
                ("clientID".into(), JsonValue::String(row.client_id.clone())),
                (
                    "queryHash".into(),
                    JsonValue::String(row.query_hash.clone()),
                ),
                (
                    "patchVersion".into(),
                    JsonValue::String(row.patch_version.clone()),
                ),
                ("deleted".into(), JsonValue::Bool(row.deleted)),
                (
                    "ttl".into(),
                    json_number_or_null(converted.ttl_interval_seconds),
                ),
                ("ttlMs".into(), json_number_or_null(converted.ttl_ms)),
                (
                    "inactivatedAt".into(),
                    json_number_or_null(converted.inactivated_at_timestamp),
                ),
                (
                    "inactivatedAtMs".into(),
                    json_number_or_null(converted.inactivated_at_ms),
                ),
            ])
        })
        .collect();
    let rows_json = lit(&JsonValue::Array(json_rows).stringify());
    let schema = id(cvr_schema);

    Some(format!(
        "INSERT INTO {schema}.desires (\"clientGroupID\",\"clientID\",\"queryHash\",\"patchVersion\",\"deleted\",\"ttl\",\"ttlMs\",\"inactivatedAt\",\"inactivatedAtMs\") \
         SELECT \"clientGroupID\",\"clientID\",\"queryHash\",\"patchVersion\",\"deleted\",\"ttl\",\"ttlMs\", \
         CASE WHEN \"inactivatedAt\" IS NULL THEN NULL ELSE to_timestamp(\"inactivatedAt\" / 1000.0) END, \"inactivatedAtMs\" \
         FROM json_to_recordset({rows_json}::json) AS x(\"clientGroupID\" TEXT,\"clientID\" TEXT,\"queryHash\" TEXT,\"patchVersion\" TEXT,\"deleted\" BOOLEAN,\"ttl\" INTERVAL,\"ttlMs\" DOUBLE PRECISION,\"inactivatedAt\" DOUBLE PRECISION,\"inactivatedAtMs\" DOUBLE PRECISION) \
         ON CONFLICT (\"clientGroupID\",\"clientID\",\"queryHash\") DO UPDATE SET \
         \"patchVersion\" = excluded.\"patchVersion\",\"deleted\" = excluded.\"deleted\",\"ttl\" = excluded.\"ttl\",\"ttlMs\" = excluded.\"ttlMs\",\"inactivatedAt\" = excluded.\"inactivatedAt\",\"inactivatedAtMs\" = excluded.\"inactivatedAtMs\""
    ))
}

/// A full `queries` row update — every column known. Port of the object
/// `#flushQueries` builds from a full `#pendingQueryUpdates` entry
/// (`QueriesRow`, with `queryArgs` pre-stringified as upstream's comment
/// explains: "handle postgres.js boolean array bug").
#[derive(Debug, Clone, PartialEq)]
pub struct QueryFullWrite {
    pub client_group_id: String,
    pub query_hash: String,
    pub client_ast: Option<JsonValue>,
    pub query_name: Option<String>,
    pub query_args: Option<JsonValue>,
    pub patch_version: Option<String>,
    pub transformation_hash: Option<String>,
    pub transformation_version: Option<String>,
    pub internal: Option<bool>,
    pub deleted: bool,
    pub row_set_signature: Option<String>,
}

/// A partial `queries` row update — only the fields actually set are
/// applied, others keep their existing value. Port of the
/// `{...Set: bool, ...: T | null}` shape `#flushQueries` builds from
/// `#pendingQueryPartialUpdates` entries.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueryPartialWrite {
    pub client_group_id: String,
    pub query_hash: String,
    pub patch_version: Option<String>,
    pub deleted: Option<bool>,
    pub transformation_hash: Option<String>,
    pub transformation_version: Option<String>,
    pub row_set_signature: Option<String>,
}

fn json_str_or_null(v: &Option<String>) -> JsonValue {
    v.clone().map(JsonValue::String).unwrap_or(JsonValue::Null)
}

fn json_bool_or_null(v: Option<bool>) -> JsonValue {
    v.map(JsonValue::Bool).unwrap_or(JsonValue::Null)
}

/// Port of `#flushQueries`'s full-update batch (the `if
/// (this.#pendingQueryUpdates.size > 0)` branch). Returns `None` for empty
/// `rows`.
pub fn get_flush_queries_full_sql(cvr_schema: &str, rows: &[QueryFullWrite]) -> Option<String> {
    if rows.is_empty() {
        return None;
    }
    let json_rows: Vec<JsonValue> = rows
        .iter()
        .map(|row| {
            JsonValue::Object(vec![
                (
                    "clientGroupID".into(),
                    JsonValue::String(row.client_group_id.clone()),
                ),
                (
                    "queryHash".into(),
                    JsonValue::String(row.query_hash.clone()),
                ),
                (
                    "clientAST".into(),
                    row.client_ast.clone().unwrap_or(JsonValue::Null),
                ),
                ("queryName".into(), json_str_or_null(&row.query_name)),
                // Pre-stringified, matching upstream's own workaround.
                (
                    "queryArgs".into(),
                    row.query_args
                        .as_ref()
                        .map(|v| JsonValue::String(v.stringify()))
                        .unwrap_or(JsonValue::Null),
                ),
                ("patchVersion".into(), json_str_or_null(&row.patch_version)),
                (
                    "transformationHash".into(),
                    json_str_or_null(&row.transformation_hash),
                ),
                (
                    "transformationVersion".into(),
                    json_str_or_null(&row.transformation_version),
                ),
                ("internal".into(), json_bool_or_null(row.internal)),
                ("deleted".into(), JsonValue::Bool(row.deleted)),
                (
                    "rowSetSignature".into(),
                    json_str_or_null(&row.row_set_signature),
                ),
            ])
        })
        .collect();
    let rows_json = lit(&JsonValue::Array(json_rows).stringify());
    let schema = id(cvr_schema);

    Some(format!(
        "INSERT INTO {schema}.queries (\"clientGroupID\",\"queryHash\",\"clientAST\",\"queryName\",\"queryArgs\",\"patchVersion\",\"transformationHash\",\"transformationVersion\",\"internal\",\"deleted\",\"rowSetSignature\") \
         SELECT \"clientGroupID\",\"queryHash\",\"clientAST\",\"queryName\", \
         CASE WHEN \"queryArgs\" IS NULL THEN NULL ELSE \"queryArgs\"::json END, \
         \"patchVersion\",\"transformationHash\",\"transformationVersion\",\"internal\",\"deleted\",\"rowSetSignature\" \
         FROM json_to_recordset({rows_json}::json) AS x(\"clientGroupID\" TEXT,\"queryHash\" TEXT,\"clientAST\" JSONB,\"queryName\" TEXT,\"queryArgs\" TEXT,\"patchVersion\" TEXT,\"transformationHash\" TEXT,\"transformationVersion\" TEXT,\"internal\" BOOLEAN,\"deleted\" BOOLEAN,\"rowSetSignature\" TEXT) \
         ON CONFLICT (\"clientGroupID\",\"queryHash\") DO UPDATE SET \
         \"clientAST\" = excluded.\"clientAST\",\"queryName\" = excluded.\"queryName\", \
         \"queryArgs\" = CASE WHEN excluded.\"queryArgs\" IS NULL THEN NULL ELSE excluded.\"queryArgs\"::json END, \
         \"patchVersion\" = excluded.\"patchVersion\",\"transformationHash\" = excluded.\"transformationHash\", \
         \"transformationVersion\" = excluded.\"transformationVersion\",\"internal\" = excluded.\"internal\", \
         \"deleted\" = excluded.\"deleted\",\"rowSetSignature\" = excluded.\"rowSetSignature\""
    ))
}

/// Port of `#flushQueries`'s partial-only-update batch (the `if
/// (partialOnly.size > 0)` branch): a plain `UPDATE ... FROM
/// json_to_recordset(...)` where each column is only overwritten if its
/// `<field>Set` flag is true, otherwise the existing value (`q.<field>`) is
/// kept. Returns `None` for empty `rows`.
pub fn get_flush_queries_partial_sql(
    cvr_schema: &str,
    rows: &[QueryPartialWrite],
) -> Option<String> {
    if rows.is_empty() {
        return None;
    }
    let json_rows: Vec<JsonValue> = rows
        .iter()
        .map(|row| {
            JsonValue::Object(vec![
                (
                    "clientGroupID".into(),
                    JsonValue::String(row.client_group_id.clone()),
                ),
                (
                    "queryHash".into(),
                    JsonValue::String(row.query_hash.clone()),
                ),
                (
                    "patchVersionSet".into(),
                    JsonValue::Bool(row.patch_version.is_some()),
                ),
                ("patchVersion".into(), json_str_or_null(&row.patch_version)),
                ("deletedSet".into(), JsonValue::Bool(row.deleted.is_some())),
                ("deleted".into(), json_bool_or_null(row.deleted)),
                (
                    "transformationHashSet".into(),
                    JsonValue::Bool(row.transformation_hash.is_some()),
                ),
                (
                    "transformationHash".into(),
                    json_str_or_null(&row.transformation_hash),
                ),
                (
                    "transformationVersionSet".into(),
                    JsonValue::Bool(row.transformation_version.is_some()),
                ),
                (
                    "transformationVersion".into(),
                    json_str_or_null(&row.transformation_version),
                ),
                (
                    "rowSetSignatureSet".into(),
                    JsonValue::Bool(row.row_set_signature.is_some()),
                ),
                (
                    "rowSetSignature".into(),
                    json_str_or_null(&row.row_set_signature),
                ),
            ])
        })
        .collect();
    let rows_json = lit(&JsonValue::Array(json_rows).stringify());
    let schema = id(cvr_schema);

    Some(format!(
        "UPDATE {schema}.queries AS q SET \
         \"patchVersion\" = CASE WHEN u.\"patchVersionSet\" THEN u.\"patchVersion\" ELSE q.\"patchVersion\" END, \
         \"deleted\" = CASE WHEN u.\"deletedSet\" THEN u.\"deleted\" ELSE q.\"deleted\" END, \
         \"transformationHash\" = CASE WHEN u.\"transformationHashSet\" THEN u.\"transformationHash\" ELSE q.\"transformationHash\" END, \
         \"transformationVersion\" = CASE WHEN u.\"transformationVersionSet\" THEN u.\"transformationVersion\" ELSE q.\"transformationVersion\" END, \
         \"rowSetSignature\" = CASE WHEN u.\"rowSetSignatureSet\" THEN u.\"rowSetSignature\" ELSE q.\"rowSetSignature\" END \
         FROM json_to_recordset({rows_json}::json) AS u(\"clientGroupID\" TEXT,\"queryHash\" TEXT,\"patchVersionSet\" BOOLEAN,\"patchVersion\" TEXT,\"deletedSet\" BOOLEAN,\"deleted\" BOOLEAN,\"transformationHashSet\" BOOLEAN,\"transformationHash\" TEXT,\"transformationVersionSet\" BOOLEAN,\"transformationVersion\" TEXT,\"rowSetSignatureSet\" BOOLEAN,\"rowSetSignature\" TEXT) \
         WHERE q.\"clientGroupID\" = u.\"clientGroupID\" AND q.\"queryHash\" = u.\"queryHash\""
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_version::empty_cvr_version;

    fn write() -> InstanceWrite {
        InstanceWrite {
            client_group_id: "cg1".into(),
            version: empty_cvr_version(),
            last_active: 60_000.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: Some("rv1".into()),
            owner: "task-1".into(),
            granted_at: 60_000.0,
            client_schema: None,
            profile_id: None,
        }
    }

    #[test]
    fn insert_clients_sql_is_batched_deduplicated_and_escaped() {
        let sql = get_insert_clients_sql(
            "app_0/cvr",
            "group'o",
            &[
                "client-b".into(),
                "client-a".into(),
                "client-a".into(),
                "client'o".into(),
            ],
        )
        .unwrap();

        assert!(sql.starts_with("INSERT INTO \"app_0/cvr\".clients"));
        assert!(sql.contains("json_to_recordset("));
        assert!(sql.contains("ON CONFLICT (\"clientGroupID\",\"clientID\") DO NOTHING"));
        assert_eq!(sql.matches("client-a").count(), 1, "deduplicated");
        assert!(sql.contains("group''o"));
        assert!(sql.contains("client''o"));
    }

    #[test]
    fn insert_clients_sql_skips_an_empty_set() {
        assert_eq!(get_insert_clients_sql("app_0/cvr", "cg1", &[]), None);
    }

    #[test]
    fn upsert_instance_sql_shape() {
        let sql = get_upsert_instance_sql("app_0/cvr", &write(), "01");
        assert!(sql.starts_with("INSERT INTO \"app_0/cvr\".instances"));
        assert!(sql.contains("ON CONFLICT (\"clientGroupID\") DO UPDATE SET"));
        assert!(sql.contains("'cg1'"));
        assert!(sql.contains("'01'"));
        assert!(sql.contains("to_timestamp(60)"));
        assert!(sql.contains("'rv1'"));
        assert!(sql.contains("'task-1'"));
    }

    #[test]
    fn null_replica_version_and_profile_id_render_as_null() {
        let sql = get_upsert_instance_sql("app_0/cvr", &write(), "01");
        assert!(sql.contains("\"profileID\" = NULL"));
    }

    #[test]
    fn client_schema_renders_as_jsonb_literal() {
        let mut w = write();
        w.client_schema = Some(JsonValue::Object(vec![(
            "tables".into(),
            JsonValue::Object(vec![]),
        )]));
        let sql = get_upsert_instance_sql("app_0/cvr", &w, "01");
        assert!(sql.contains("'{\"tables\":{}}'::jsonb"));
    }

    #[test]
    fn quotes_in_values_are_escaped() {
        let mut w = write();
        w.owner = "o'brien".into();
        let sql = get_upsert_instance_sql("app_0/cvr", &w, "01");
        assert!(sql.contains("'o''brien'"));
    }

    /// Live verification: the generated upsert SQL actually runs against a
    /// real Postgres instance (insert, then re-run to prove the ON
    /// CONFLICT UPDATE path too), same "run it for real" standard used for
    /// `cvr_schema_sql`'s DDL. Skips gracefully if no local test Postgres
    /// is reachable.
    #[tokio::test]
    async fn upsert_instance_sql_actually_runs_against_real_postgres() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = zero_cache_types::shards::ShardId {
            app_id: "cvrflush".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrflush_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }

        let sql = get_upsert_instance_sql("cvrflush_0/cvr", &write(), "01");
        client.batch_execute(&sql).await.unwrap(); // insert path
        client.batch_execute(&sql).await.unwrap(); // ON CONFLICT update path

        let row = client
            .query_one("SELECT \"version\", \"owner\" FROM \"cvrflush_0/cvr\".instances WHERE \"clientGroupID\" = 'cg1'", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "01");
        assert_eq!(row.get::<_, String>(1), "task-1");

        client
            .batch_execute("DROP SCHEMA \"cvrflush_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    fn desire(ttl_ms: f64, inactivated_at: Option<f64>) -> DesireWrite {
        DesireWrite {
            client_group_id: "cg1".into(),
            client_id: "c1".into(),
            query_hash: "h1".into(),
            patch_version: "01".into(),
            deleted: false,
            ttl_ms,
            inactivated_at,
        }
    }

    #[test]
    fn flush_desires_empty_rows_returns_none() {
        assert_eq!(get_flush_desires_sql("app_0/cvr", &[]), None);
    }

    #[test]
    fn flush_desires_sql_shape() {
        let sql = get_flush_desires_sql("app_0/cvr", &[desire(60_000.0, None)]).unwrap();
        assert!(sql.starts_with("INSERT INTO \"app_0/cvr\".desires"));
        assert!(sql.contains("json_to_recordset("));
        assert!(sql
            .contains("ON CONFLICT (\"clientGroupID\",\"clientID\",\"queryHash\") DO UPDATE SET"));
    }

    #[test]
    fn flush_desires_negative_ttl_means_forever_and_is_null() {
        let sql = get_flush_desires_sql("app_0/cvr", &[desire(-1.0, None)]).unwrap();
        assert!(sql.contains("\"ttl\":null"));
        assert!(sql.contains("\"ttlMs\":null"));
    }

    #[test]
    fn flush_desires_positive_ttl_converts_to_seconds_and_ms() {
        let sql = get_flush_desires_sql("app_0/cvr", &[desire(60_000.0, None)]).unwrap();
        assert!(sql.contains("\"ttl\":60"));
        assert!(sql.contains("\"ttlMs\":60000"));
    }

    #[test]
    fn flush_desires_inactivated_at_is_carried_in_both_forms() {
        let sql = get_flush_desires_sql("app_0/cvr", &[desire(60_000.0, Some(5000.0))]).unwrap();
        assert!(sql.contains("\"inactivatedAt\":5"));
        assert!(sql.contains("\"inactivatedAtMs\":5000"));
    }

    /// Live verification: the generated bulk upsert actually runs and
    /// produces the expected row, including the timestamp round-trip
    /// through `to_timestamp(.../1000.0)`.
    #[tokio::test]
    async fn flush_desires_sql_actually_runs_against_real_postgres() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = zero_cache_types::shards::ShardId {
            app_id: "cvrflushd".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrflushd_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }
        client
            .batch_execute("INSERT INTO \"cvrflushd_0/cvr\".instances (\"clientGroupID\", \"version\", \"lastActive\") VALUES ('cg1', '00', now());")
            .await
            .unwrap();
        client
            .batch_execute(
                "INSERT INTO \"cvrflushd_0/cvr\".clients (\"clientGroupID\", \"clientID\") VALUES ('cg1', 'c1');",
            )
            .await
            .unwrap();
        client
            .batch_execute(
                "INSERT INTO \"cvrflushd_0/cvr\".queries (\"clientGroupID\", \"queryHash\", \"queryName\", \"patchVersion\", \"internal\") VALUES ('cg1', 'h1', 'q', '00', false);",
            )
            .await
            .unwrap();

        let sql = get_flush_desires_sql("cvrflushd_0/cvr", &[desire(60_000.0, None)]).unwrap();
        client.batch_execute(&sql).await.unwrap();
        client.batch_execute(&sql).await.unwrap(); // ON CONFLICT update path

        let row = client
            .query_one(
                "SELECT \"patchVersion\", \"ttlMs\" FROM \"cvrflushd_0/cvr\".desires WHERE \"clientGroupID\" = 'cg1' AND \"clientID\" = 'c1' AND \"queryHash\" = 'h1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "01");
        assert_eq!(row.get::<_, f64>(1), 60_000.0);

        client
            .batch_execute("DROP SCHEMA \"cvrflushd_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    fn full_write() -> QueryFullWrite {
        QueryFullWrite {
            client_group_id: "cg1".into(),
            query_hash: "h1".into(),
            client_ast: None,
            query_name: Some("myQuery".into()),
            query_args: Some(JsonValue::Array(vec![JsonValue::Number(1.0)])),
            patch_version: Some("01".into()),
            transformation_hash: None,
            transformation_version: None,
            internal: Some(false),
            deleted: false,
            row_set_signature: None,
        }
    }

    #[test]
    fn flush_queries_full_empty_rows_returns_none() {
        assert_eq!(get_flush_queries_full_sql("app_0/cvr", &[]), None);
    }

    #[test]
    fn flush_queries_full_sql_shape() {
        let sql = get_flush_queries_full_sql("app_0/cvr", &[full_write()]).unwrap();
        assert!(sql.starts_with("INSERT INTO \"app_0/cvr\".queries"));
        assert!(sql.contains("ON CONFLICT (\"clientGroupID\",\"queryHash\") DO UPDATE SET"));
        assert!(sql.contains("\"queryName\":\"myQuery\""));
        // queryArgs is pre-stringified (a JSON string containing JSON), not a bare array.
        assert!(sql.contains("\"queryArgs\":\"[1]\""));
    }

    #[test]
    fn flush_queries_partial_empty_rows_returns_none() {
        assert_eq!(get_flush_queries_partial_sql("app_0/cvr", &[]), None);
    }

    #[test]
    fn flush_queries_partial_sql_shape_and_set_flags() {
        let row = QueryPartialWrite {
            client_group_id: "cg1".into(),
            query_hash: "h1".into(),
            patch_version: Some("02".into()),
            deleted: None,
            transformation_hash: None,
            transformation_version: None,
            row_set_signature: None,
        };
        let sql = get_flush_queries_partial_sql("app_0/cvr", &[row]).unwrap();
        assert!(sql.starts_with("UPDATE \"app_0/cvr\".queries AS q SET"));
        assert!(sql.contains("\"patchVersionSet\":true"));
        assert!(sql.contains("\"deletedSet\":false"));
        assert!(sql.contains(
            "WHERE q.\"clientGroupID\" = u.\"clientGroupID\" AND q.\"queryHash\" = u.\"queryHash\""
        ));
    }

    /// Live verification: both the full-update and partial-update forms
    /// actually run against real Postgres, and the partial form correctly
    /// leaves untouched fields alone.
    #[tokio::test]
    async fn flush_queries_sql_actually_runs_against_real_postgres() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = zero_cache_types::shards::ShardId {
            app_id: "cvrflushq".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrflushq_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }
        client
            .batch_execute("INSERT INTO \"cvrflushq_0/cvr\".instances (\"clientGroupID\", \"version\", \"lastActive\") VALUES ('cg1', '00', now());")
            .await
            .unwrap();

        let full_sql = get_flush_queries_full_sql("cvrflushq_0/cvr", &[full_write()]).unwrap();
        client.batch_execute(&full_sql).await.unwrap();

        let row = client
            .query_one(
                "SELECT \"queryName\", \"patchVersion\" FROM \"cvrflushq_0/cvr\".queries WHERE \"clientGroupID\" = 'cg1' AND \"queryHash\" = 'h1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "myQuery");
        assert_eq!(row.get::<_, String>(1), "01");

        // Now a partial update that only touches patchVersion — queryName
        // should be left alone.
        let partial = QueryPartialWrite {
            client_group_id: "cg1".into(),
            query_hash: "h1".into(),
            patch_version: Some("02".into()),
            ..Default::default()
        };
        let partial_sql = get_flush_queries_partial_sql("cvrflushq_0/cvr", &[partial]).unwrap();
        client.batch_execute(&partial_sql).await.unwrap();

        let row = client
            .query_one(
                "SELECT \"queryName\", \"patchVersion\" FROM \"cvrflushq_0/cvr\".queries WHERE \"clientGroupID\" = 'cg1' AND \"queryHash\" = 'h1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            row.get::<_, String>(0),
            "myQuery",
            "queryName should be untouched by the partial update"
        );
        assert_eq!(
            row.get::<_, String>(1),
            "02",
            "patchVersion should have been updated"
        );

        client
            .batch_execute("DROP SCHEMA \"cvrflushq_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }
}
