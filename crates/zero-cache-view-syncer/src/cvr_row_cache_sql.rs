//! Port of the SQL-generation half of `row-record-cache.ts`'s
//! `RowRecordCache.executeRowUpdates` — the CVRStore write-path piece
//! flagged as still fully unstarted after `cvr_flush_sql.rs`/
//! `cvr_store_pg::flush_cvr` closed out the metadata (instances/queries/
//! desires) side of the write path.
//!
//! Scope: SQL text generation only, same established pattern as every
//! other module in this thread. NOT ported: `RowRecordCache` itself (the
//! in-memory row cache, deferred-flush threshold logic, `getRowRecords`,
//! `apply`'s bookkeeping) — this module only covers turning a batch of row
//! updates into the SQL statements `executeRowUpdates` issues, not the
//! stateful cache wrapping it.

use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_types::sql::{id, lit};

use crate::cvr_types::{RowId, RowRecord};

/// One row update: `Some(record)` to put/upsert, `None` to delete. Port of
/// `RowRecord | null` as the `Map<RowID, RowRecord | null>` value type in
/// `executeRowUpdates`.
pub type RowUpdate = (RowId, Option<RowRecord>);

fn row_key_json(row_key: &std::collections::BTreeMap<String, JsonValue>) -> JsonValue {
    JsonValue::Object(
        row_key
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    )
}

fn ref_counts_json(ref_counts: &Option<std::collections::BTreeMap<String, i64>>) -> JsonValue {
    match ref_counts {
        None => JsonValue::Null,
        Some(counts) => JsonValue::Object(
            counts
                .iter()
                .map(|(k, v)| (k.clone(), JsonValue::Number(*v as f64)))
                .collect(),
        ),
    }
}

/// Port of the `DELETE FROM ... WHERE clientGroupID = ... AND schema = ...
/// AND table = ... AND rowKey = ...` template for a `null` row update.
fn get_delete_row_sql(cvr_schema: &str, cvr_id: &str, row_id: &RowId) -> String {
    format!(
        "DELETE FROM {}.rows WHERE \"clientGroupID\" = {} AND \"schema\" = {} AND \"table\" = {} AND \"rowKey\" = {}::jsonb",
        id(cvr_schema),
        lit(cvr_id),
        lit(&row_id.schema),
        lit(&row_id.table),
        lit(&row_key_json(&row_id.row_key).stringify()),
    )
}

/// Port of the bulk `INSERT INTO rows ... FROM json_to_recordset(...) ...
/// ON CONFLICT DO UPDATE` template for the non-null row updates in one
/// batch.
fn get_upsert_rows_sql(cvr_schema: &str, cvr_id: &str, records: &[&RowRecord]) -> String {
    let json_rows: Vec<JsonValue> = records
        .iter()
        .map(|r| {
            JsonValue::Object(vec![
                (
                    "clientGroupID".into(),
                    JsonValue::String(cvr_id.to_string()),
                ),
                ("schema".into(), JsonValue::String(r.id.schema.clone())),
                ("table".into(), JsonValue::String(r.id.table.clone())),
                ("rowKey".into(), row_key_json(&r.id.row_key)),
                (
                    "rowVersion".into(),
                    JsonValue::String(r.row_version.clone()),
                ),
                (
                    "patchVersion".into(),
                    JsonValue::String(version_string(&r.base)),
                ),
                ("refCounts".into(), ref_counts_json(&r.ref_counts)),
            ])
        })
        .collect();
    let rows_json = lit(&JsonValue::Array(json_rows).stringify());
    let schema = id(cvr_schema);

    format!(
        "INSERT INTO {schema}.rows (\"clientGroupID\",\"schema\",\"table\",\"rowKey\",\"rowVersion\",\"patchVersion\",\"refCounts\") \
         SELECT \"clientGroupID\",\"schema\",\"table\",\"rowKey\",\"rowVersion\",\"patchVersion\",\"refCounts\" \
         FROM json_to_recordset({rows_json}::json) AS x(\"clientGroupID\" TEXT,\"schema\" TEXT,\"table\" TEXT,\"rowKey\" JSONB,\"rowVersion\" TEXT,\"patchVersion\" TEXT,\"refCounts\" JSONB) \
         ON CONFLICT (\"clientGroupID\",\"schema\",\"table\",\"rowKey\") DO UPDATE SET \
         \"rowVersion\" = excluded.\"rowVersion\",\"patchVersion\" = excluded.\"patchVersion\",\"refCounts\" = excluded.\"refCounts\""
    )
}

/// `patchVersion` as stored in SQL is the version's cookie-string form.
/// Panics on an unencodable config-version — mirrors upstream's
/// `versionString` (`assert`-free, but `version_to_cookie` here can only
/// fail on a config version that doesn't fit `LexiVersion`'s range, which
/// in practice never happens for the small counters this port produces).
fn version_string(base: &crate::cvr_types::CvrRecordBase) -> String {
    crate::cvr_version::version_to_cookie(&base.patch_version)
        .expect("patch_version always encodes")
}

/// Port of `executeRowUpdates`'s statement-generation (minus the
/// `allow-defer`/`force` mode gating — that's the stateful cache's
/// decision, made by the caller before calling this function). Always
/// includes the `rowsVersion` upsert first (matching upstream unshifting it
/// onto `pending`), then one `DELETE` per null update, then (if any
/// non-null updates exist) one bulk upsert `INSERT` for all of them.
pub fn get_row_updates_sql(
    cvr_schema: &str,
    cvr_id: &str,
    version: &str,
    row_updates: &[RowUpdate],
) -> Vec<String> {
    let mut statements = vec![get_upsert_rows_version_sql(cvr_schema, cvr_id, version)];

    for (row_id, record) in row_updates {
        if record.is_none() {
            statements.push(get_delete_row_sql(cvr_schema, cvr_id, row_id));
        }
    }

    let puts: Vec<&RowRecord> = row_updates.iter().filter_map(|(_, r)| r.as_ref()).collect();
    if !puts.is_empty() {
        statements.push(get_upsert_rows_sql(cvr_schema, cvr_id, &puts));
    }

    statements
}

/// Port of the `INSERT INTO rowsVersion ... ON CONFLICT ("clientGroupID")
/// DO UPDATE SET ...` template `executeRowUpdates` always issues first.
fn get_upsert_rows_version_sql(cvr_schema: &str, cvr_id: &str, version: &str) -> String {
    format!(
        "INSERT INTO {}.\"rowsVersion\" (\"clientGroupID\",\"version\") VALUES ({},{}) \
         ON CONFLICT (\"clientGroupID\") DO UPDATE SET \"version\" = excluded.\"version\"",
        id(cvr_schema),
        lit(cvr_id),
        lit(version),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_types::CvrRecordBase;
    use crate::cvr_version::empty_cvr_version;
    use std::collections::BTreeMap;

    fn row_id(table: &str, id_val: &str) -> RowId {
        let mut key = BTreeMap::new();
        key.insert("id".to_string(), JsonValue::String(id_val.to_string()));
        RowId {
            schema: "public".into(),
            table: table.into(),
            row_key: key,
        }
    }

    fn row_record(table: &str, id_val: &str) -> RowRecord {
        RowRecord {
            base: CvrRecordBase {
                patch_version: empty_cvr_version(),
            },
            id: row_id(table, id_val),
            row_version: "v1".into(),
            ref_counts: Some(BTreeMap::from([("h1".to_string(), 1i64)])),
        }
    }

    #[test]
    fn always_includes_rows_version_upsert_first() {
        let sql = get_row_updates_sql("app_0/cvr", "cg1", "01", &[]);
        assert_eq!(sql.len(), 1);
        assert!(sql[0].starts_with("INSERT INTO \"app_0/cvr\".\"rowsVersion\""));
        assert!(sql[0].contains("'01'"));
    }

    #[test]
    fn null_update_produces_a_delete_statement() {
        let sql = get_row_updates_sql("app_0/cvr", "cg1", "01", &[(row_id("issues", "1"), None)]);
        assert_eq!(sql.len(), 2);
        assert!(sql[1].starts_with("DELETE FROM \"app_0/cvr\".rows"));
        assert!(sql[1].contains("\"table\" = 'issues'"));
    }

    #[test]
    fn non_null_updates_produce_one_bulk_upsert() {
        let sql = get_row_updates_sql(
            "app_0/cvr",
            "cg1",
            "01",
            &[
                (row_id("issues", "1"), Some(row_record("issues", "1"))),
                (row_id("issues", "2"), Some(row_record("issues", "2"))),
            ],
        );
        assert_eq!(
            sql.len(),
            2,
            "rowsVersion upsert + ONE bulk INSERT for both puts"
        );
        assert!(sql[1].starts_with("INSERT INTO \"app_0/cvr\".rows"));
        assert!(sql[1].contains(
            "ON CONFLICT (\"clientGroupID\",\"schema\",\"table\",\"rowKey\") DO UPDATE SET"
        ));
    }

    #[test]
    fn mixed_puts_and_deletes() {
        let sql = get_row_updates_sql(
            "app_0/cvr",
            "cg1",
            "01",
            &[
                (row_id("issues", "1"), None),
                (row_id("issues", "2"), Some(row_record("issues", "2"))),
            ],
        );
        // rowsVersion upsert, 1 delete, 1 bulk upsert.
        assert_eq!(sql.len(), 3);
        assert!(sql[1].starts_with("DELETE FROM"));
        assert!(sql[2].starts_with("INSERT INTO \"app_0/cvr\".rows"));
    }

    /// Live verification: the generated statements actually run against
    /// real Postgres — a bulk row upsert, then a mixed batch (one more put,
    /// one delete of the first row) — and the final row set matches.
    #[tokio::test]
    async fn row_updates_sql_actually_runs_against_real_postgres() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = zero_cache_types::shards::ShardId {
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

        for stmt in get_row_updates_sql(
            "cvrrows_0/cvr",
            "cg1",
            "01",
            &[
                (row_id("issues", "1"), Some(row_record("issues", "1"))),
                (row_id("issues", "2"), Some(row_record("issues", "2"))),
            ],
        ) {
            client.batch_execute(&stmt).await.unwrap();
        }

        let count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM \"cvrrows_0/cvr\".rows WHERE \"clientGroupID\" = 'cg1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 2);

        // Delete row "1", upsert row "2" again (ON CONFLICT path).
        for stmt in get_row_updates_sql(
            "cvrrows_0/cvr",
            "cg1",
            "02",
            &[
                (row_id("issues", "1"), None),
                (row_id("issues", "2"), Some(row_record("issues", "2"))),
            ],
        ) {
            client.batch_execute(&stmt).await.unwrap();
        }

        let rows = client
            .query(
                "SELECT \"rowKey\" FROM \"cvrrows_0/cvr\".rows WHERE \"clientGroupID\" = 'cg1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "row 1 should have been deleted, row 2 should remain (upserted, not duplicated)"
        );

        let rows_version: String = client
            .query_one("SELECT \"version\" FROM \"cvrrows_0/cvr\".\"rowsVersion\" WHERE \"clientGroupID\" = 'cg1'", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            rows_version, "02",
            "rowsVersion should reflect the latest batch's version"
        );

        client
            .batch_execute("DROP SCHEMA \"cvrrows_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }
}
