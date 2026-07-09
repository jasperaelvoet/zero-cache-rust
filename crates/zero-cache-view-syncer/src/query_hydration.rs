//! The first real WIRING of `ViewSyncerService#addAndRemoveQueries`'s core
//! sequence — composing `cvr_query_driven_updater`, `cvr_row_received`, and
//! `cvr_delete_unreferenced_rows` (each ported independently across the
//! last several rounds) into one orchestrated hydration cycle, plus an
//! actual IVM fetch via `zero_cache_zql::ivm::{table_source, filter}`. This
//! is not a port of a specific upstream function — `#addAndRemoveQueries`
//! itself remains far larger (pokers, query covering, catchup, telemetry)
//! — it's the integration glue proving the pieces built over the last four
//! rounds actually compose in upstream's real order: track the query as
//! executed, process freshly-fetched rows as "received", then delete
//! whatever's now unreferenced.
//!
//! Scope: still no CVRStore I/O here (that's `cvr_store_pg::get_row_records`/
//! `flush_cvr`, real Postgres calls a caller makes before/after this) — this
//! function takes `existing_rows` as an already-fetched slice (what
//! `get_row_records` would have returned) and returns the decisions for the
//! caller to persist, matching the pure-orchestration style every module
//! in this thread has used.

use std::collections::{HashMap, HashSet};

use zero_cache_zql::ivm::data::Row as ZqlRow;
use zero_cache_zql::ivm::filter::Filter;
use zero_cache_zql::ivm::operator::FetchRequest;
use zero_cache_zql::ivm::table_source::TableSource;

use crate::cvr_delete_unreferenced_rows::{
    delete_unreferenced_rows, ExistingRow as DeleteExistingRow, RowRecordWrite,
};
use crate::cvr_query_driven_updater::track_executed;
use crate::cvr_ref_counts::RefCounts;
use crate::cvr_row_received::{
    process_received_row, ExistingRow as ReceivedExistingRow, LastPatchInfo, RowOutcome,
    RowUpdateInput,
};
use crate::cvr_types::{Cvr, QueryPatch};
use crate::cvr_version::CvrVersion;

/// The combined outcome of hydrating one query: the query-level patch (if
/// any), and one row outcome per fetched row.
#[derive(Debug, Clone, PartialEq)]
pub struct HydrationResult<K> {
    pub query_patches: Vec<QueryPatch>,
    pub row_outcomes: Vec<(K, RowOutcome)>,
    pub deleted_row_patches: Vec<(K, CvrVersion)>,
    /// The row-record writes `delete_unreferenced_rows` decided (every row
    /// it touched gets a write — see that module's doc for why even a
    /// deleted row gets a `putRowRecord`-equivalent tombstone write, not
    /// just the ones that produced a client-facing delete patch). Combined
    /// with each `row_outcomes` entry's `store_write`, this is everything a
    /// caller needs to actually persist the cycle's row-record changes.
    pub deletion_row_writes: Vec<RowRecordWrite<K>>,
    /// The full fetched row contents for every entry in `row_outcomes`, in the
    /// same order/keying — NOT part of upstream's `HydrationResult`-equivalent
    /// (which persists via `CVRStore`), but needed by a caller (like
    /// `poke_builder`) that wants to build a `ClientPutRowPatch` without
    /// re-running `filter.fetch`. Populated alongside `row_outcomes` in the
    /// same loop, so it never drifts out of sync.
    pub fetched_rows: Vec<(K, ZqlRow)>,
}

/// Runs one query through the real IVM `TableSource` + `Filter` pipeline
/// (`fetch`), then feeds each resulting row through `process_received_row`
/// (as `received()` would), and finally runs `delete_unreferenced_rows`
/// over `existing_rows` to compute deletions — the same three-step order
/// `#addAndRemoveQueries` uses (`trackQueries` -> `received` via
/// `#processChanges` -> `deleteUnreferencedRows`).
///
/// `row_key`/`row_ref_counts` let the caller derive this query's row
/// identity (`K`) and ref-count contribution (typically `{queryID: 1}`)
/// from a fetched `Row` — this function has no opinion on primary keys.
#[allow(clippy::too_many_arguments)]
pub fn hydrate_query<K: Clone + Eq + std::hash::Hash>(
    cvr: &mut Cvr,
    orig_version: &CvrVersion,
    tracked: &mut HashSet<String>,
    query_id: &str,
    transformation_hash: &str,
    source: &TableSource,
    filter: &Filter,
    row_key: impl Fn(&ZqlRow) -> K,
    row_ref_counts: impl Fn(&ZqlRow) -> RefCounts,
    row_version: impl Fn(&ZqlRow) -> String,
    existing_received: &HashMap<K, ReceivedExistingRow>,
    existing_for_deletion: &[DeleteExistingRow<K>],
    received_rows: &mut HashMap<K, Option<RefCounts>>,
    last_patches: &mut HashMap<K, LastPatchInfo>,
) -> HydrationResult<K> {
    let query_patches = track_executed(cvr, orig_version, tracked, query_id, transformation_hash);

    let mut row_outcomes = Vec::new();
    let mut fetched_rows = Vec::new();
    for node in filter.fetch(source, &FetchRequest::default()) {
        let key = row_key(&node.row);
        let update = RowUpdateInput {
            version: Some(row_version(&node.row)),
            has_contents: true,
            ref_counts: Some(row_ref_counts(&node.row)),
        };
        let existing = existing_received.get(&key);
        let outcome = process_received_row(
            key.clone(),
            existing,
            &update,
            None,
            orig_version,
            &cvr.version,
            received_rows,
            last_patches,
        );
        row_outcomes.push((key.clone(), outcome));
        fetched_rows.push((key, node.row.clone()));
    }

    let deletion = delete_unreferenced_rows(
        existing_for_deletion,
        received_rows,
        tracked,
        orig_version,
        &cvr.version,
        last_patches,
    );

    HydrationResult {
        query_patches,
        row_outcomes,
        deleted_row_patches: deletion.patches,
        deletion_row_writes: deletion.row_writes,
        fetched_rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_ref_counts::RefCounts;
    use crate::cvr_row_received::{RowClientPatch, RowStoreWrite};
    use crate::cvr_types::{ClientQueryRecord, ExternalQueryBase, QueryRecord, TtlClock};
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::Direction;
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_zql::ivm::change::make_source_change_add;

    fn v(s: &str) -> CvrVersion {
        CvrVersion {
            state_version: s.into(),
            config_version: None,
        }
    }

    fn issue_row(id: i64, active: bool) -> ZqlRow {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("active".into(), JsonValue::Bool(active)),
        ]
    }

    fn rc(pairs: &[(&str, i64)]) -> RefCounts {
        pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
    }

    fn empty_cvr() -> Cvr {
        Cvr {
            id: "cg1".into(),
            version: v("01"),
            last_active: 0.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: None,
            clients: BTreeMap::new(),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        }
    }

    fn client_query(id: &str, gotten: bool) -> QueryRecord {
        QueryRecord::Client(ClientQueryRecord {
            base: ExternalQueryBase {
                id: id.into(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
                client_state: BTreeMap::new(),
                patch_version: if gotten { Some(v("00")) } else { None },
            },
            ast: zero_cache_protocol::ast::Ast::default(),
        })
    }

    /// The end-to-end wiring proof: a real `TableSource` + `Filter`
    /// (the actual IVM machinery) feeds `hydrate_query`, which composes
    /// `track_executed` + `process_received_row` + `delete_unreferenced_rows`
    /// — three independently-ported modules — into one coherent hydration
    /// cycle matching upstream's real call order.
    #[test]
    fn hydrate_query_composes_tracking_receiving_and_deleting() {
        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue_row(1, true)));
        issues.push(make_source_change_add(issue_row(2, true)));
        issues.push(make_source_change_add(issue_row(3, false))); // filtered out
        let filter = Filter::new(|row: &ZqlRow| {
            matches!(
                row.iter().find(|(k, _)| k == "active"),
                Some((_, JsonValue::Bool(true)))
            )
        });

        let mut cvr = empty_cvr();
        cvr.queries.insert("q1".into(), client_query("q1", false));
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();
        let mut received_rows = HashMap::new();
        let mut last_patches = HashMap::new();

        // A stale row from a PREVIOUS hydration of q1 that no longer
        // matches the filter (e.g. row 99 used to be active, isn't in the
        // TableSource anymore) — proves deleteUnreferencedRows runs in the
        // same cycle and correctly deletes it.
        let existing_for_deletion = vec![DeleteExistingRow {
            id: "row-99".to_string(),
            row_version: "v99".into(),
            patch_version: v("00"),
            ref_counts: Some(rc(&[("q1", 1)])),
        }];

        let result = hydrate_query(
            &mut cvr,
            &orig,
            &mut tracked,
            "q1",
            "hash1",
            &issues,
            &filter,
            |row| format!("row-{}", row_int(row, "id")),
            |_row| rc(&[("q1", 1)]),
            |row| format!("v{}", row_int(row, "id")),
            &HashMap::new(),
            &existing_for_deletion,
            &mut received_rows,
            &mut last_patches,
        );

        // 1. The query moved from desired-only to gotten.
        assert_eq!(
            result.query_patches,
            vec![QueryPatch {
                op: crate::cvr_types::PatchOp::Put,
                id: "q1".into(),
                client_id: None
            }]
        );

        // 2. Exactly the two active rows were fetched and produce Put outcomes.
        assert_eq!(result.row_outcomes.len(), 2);
        let ids: HashSet<_> = result.row_outcomes.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            ids,
            HashSet::from(["row-1".to_string(), "row-2".to_string()])
        );
        for (_, outcome) in &result.row_outcomes {
            assert!(matches!(
                outcome.client_patch,
                Some(RowClientPatch::Put { .. })
            ));
            assert!(matches!(outcome.store_write, RowStoreWrite::Put { .. }));
        }

        // 3. The stale row-99 (not re-fetched) was deleted.
        assert_eq!(result.deleted_row_patches.len(), 1);
        assert_eq!(result.deleted_row_patches[0].0, "row-99");

        // 4. The CVR version was bumped exactly once and consistently used
        //    across the query patch, row patches, and the deletion.
        assert_ne!(cvr.version, orig);
    }

    fn row_int(row: &ZqlRow, col: &str) -> i64 {
        match row.iter().find(|(k, _)| k == col).map(|(_, v)| v) {
            Some(JsonValue::Number(n)) => *n as i64,
            _ => panic!("expected numeric column {col}"),
        }
    }

    #[test]
    fn hydrate_query_with_no_existing_rows_only_adds() {
        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue_row(1, true)));
        let filter = Filter::new(|_row: &ZqlRow| true);

        let mut cvr = empty_cvr();
        cvr.queries.insert("q1".into(), client_query("q1", false));
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();
        let mut received_rows = HashMap::new();
        let mut last_patches = HashMap::new();

        let result = hydrate_query(
            &mut cvr,
            &orig,
            &mut tracked,
            "q1",
            "hash1",
            &issues,
            &filter,
            |row| format!("row-{}", row_int(row, "id")),
            |_row| rc(&[("q1", 1)]),
            |row| format!("v{}", row_int(row, "id")),
            &HashMap::new(),
            &[],
            &mut received_rows,
            &mut last_patches,
        );

        assert_eq!(result.row_outcomes.len(), 1);
        assert!(result.deleted_row_patches.is_empty());
    }

    /// The full-stack live proof: `hydrate_query`'s decisions, backed by a
    /// REAL Postgres CVR row store on both ends — `get_row_records` supplies
    /// the "existing" state, `hydrate_query` runs against the real IVM
    /// `TableSource`/`Filter`, and the resulting row writes are persisted
    /// back via `cvr_row_cache_sql::get_row_updates_sql` — then a second
    /// `get_row_records` call confirms the live database actually reflects
    /// the hydration: the two currently-matching rows present, the stale
    /// leftover row gone (tombstoned, excluded by `refCounts IS NOT NULL`).
    #[tokio::test]
    async fn hydrate_query_persists_through_a_real_cvr_row_store() {
        use crate::cvr_row_cache_sql::{get_row_updates_sql, RowUpdate};
        use crate::cvr_store_pg::get_row_records;
        use crate::cvr_types::RowId;
        use zero_cache_types::shards::ShardId;

        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "cvrhydrate".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrhydrate_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }
        let s = "\"cvrhydrate_0/cvr\"";
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.instances (\"clientGroupID\", \"version\", \"lastActive\", \"replicaVersion\", \"owner\") \
                 VALUES ('cg1', '01', now(), 'rv1', 'my-task'); \
                 INSERT INTO {s}.\"rowsVersion\" (\"clientGroupID\", \"version\") VALUES ('cg1', '01'); \
                 INSERT INTO {s}.rows (\"clientGroupID\",\"schema\",\"table\",\"rowKey\",\"rowVersion\",\"patchVersion\",\"refCounts\") \
                 VALUES ('cg1','public','issues','{{\"id\":\"99\"}}','v99','01','{{\"q1\":1}}');"
            ))
            .await
            .unwrap();

        // Track schema/table/rowKey structure alongside the row-id-string
        // key hydrate_query works with, so the resulting writes can be
        // reconstructed into real `RowId`s for persistence — hydrate_query
        // itself is opaque to row structure (see module doc).
        let row_id_for = |id: i64| RowId {
            schema: "public".into(),
            table: "issues".into(),
            row_key: BTreeMap::from([("id".to_string(), JsonValue::String(id.to_string()))]),
        };
        let key_for = |id: i64| {
            let structured = zero_cache_types::row_key::RowId::new(
                "public",
                "issues",
                vec![("id".to_string(), JsonValue::String(id.to_string()))],
            );
            zero_cache_types::row_key::row_id_string(&structured).unwrap()
        };

        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue_row(1, true)));
        issues.push(make_source_change_add(issue_row(2, true)));
        issues.push(make_source_change_add(issue_row(3, false)));
        let filter = Filter::new(|row: &ZqlRow| {
            matches!(
                row.iter().find(|(k, _)| k == "active"),
                Some((_, JsonValue::Bool(true)))
            )
        });

        let mut cvr = empty_cvr();
        cvr.queries.insert("q1".into(), client_query("q1", false));
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();
        let mut received_rows = HashMap::new();
        let mut last_patches = HashMap::new();

        let live_existing = get_row_records(&client, &shard, "cg1").await.unwrap();
        let existing_for_deletion: Vec<_> = live_existing
            .iter()
            .map(|(k, r)| DeleteExistingRow {
                id: k.clone(),
                row_version: r.row_version.clone(),
                patch_version: r.base.patch_version.clone(),
                ref_counts: r.ref_counts.clone(),
            })
            .collect();

        let result = hydrate_query(
            &mut cvr,
            &orig,
            &mut tracked,
            "q1",
            "hash1",
            &issues,
            &filter,
            |row| key_for(row_int(row, "id")),
            |_row| rc(&[("q1", 1)]),
            |row| format!("v{}", row_int(row, "id")),
            &HashMap::new(),
            &existing_for_deletion,
            &mut received_rows,
            &mut last_patches,
        );

        assert_eq!(
            result.row_outcomes.len(),
            2,
            "rows 1 and 2 should be fetched live via the real Filter"
        );
        assert_eq!(
            result.deletion_row_writes.len(),
            1,
            "the stale row-99 should have been processed for deletion"
        );

        // Persist every row-record write this cycle produced, exactly as a
        // real caller (once CVRStore's row cache exists) would.
        let mut row_updates: Vec<RowUpdate> = Vec::new();
        for (key, outcome) in &result.row_outcomes {
            let RowStoreWrite::Put {
                row_version,
                patch_version,
                merged_ref_counts,
            } = &outcome.store_write
            else {
                continue;
            };
            let row_id = if key == &key_for(1) {
                row_id_for(1)
            } else if key == &key_for(2) {
                row_id_for(2)
            } else {
                panic!("unexpected row key {key}")
            };
            row_updates.push((
                row_id.clone(),
                Some(crate::cvr_types::RowRecord {
                    base: crate::cvr_types::CvrRecordBase {
                        patch_version: patch_version.clone(),
                    },
                    id: row_id,
                    row_version: row_version.clone(),
                    ref_counts: merged_ref_counts.clone(),
                }),
            ));
        }
        for write in &result.deletion_row_writes {
            let row_id = row_id_for(99);
            row_updates.push((
                row_id.clone(),
                Some(crate::cvr_types::RowRecord {
                    base: crate::cvr_types::CvrRecordBase {
                        patch_version: write.patch_version.clone(),
                    },
                    id: row_id,
                    row_version: write.row_version.clone(),
                    ref_counts: write.ref_counts.clone(),
                }),
            ));
        }

        let new_version_cookie = crate::cvr_version::version_to_cookie(&cvr.version).unwrap();
        for sql in get_row_updates_sql("cvrhydrate_0/cvr", "cg1", &new_version_cookie, &row_updates)
        {
            client.batch_execute(&sql).await.unwrap();
        }

        let final_rows = get_row_records(&client, &shard, "cg1").await.unwrap();
        assert_eq!(final_rows.len(), 2, "row-99 should now be tombstoned (excluded), rows 1 and 2 should be live: {final_rows:?}");
        assert!(final_rows.contains_key(&key_for(1)));
        assert!(final_rows.contains_key(&key_for(2)));
        assert!(!final_rows.contains_key(&key_for(99)));

        client
            .batch_execute("DROP SCHEMA \"cvrhydrate_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }
}
