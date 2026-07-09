//! Maps a change-stream commit to the set of live queries it invalidates.
//!
//! When the replication loop commits a transaction, the change-log records
//! which tables that transaction touched (see
//! `zero-cache-sqlite::change_log`). A live query must be re-hydrated iff the
//! commit touched any table the query reads — its
//! [`referenced_tables`](zero_cache_protocol::ast::referenced_tables) read-set
//! (primary table + recursive `related` hops + correlated-subquery
//! `where`-conditions).
//!
//! Upstream, `zero-cache` tracks this structurally through the IVM operator
//! graph (each operator subscribes to its source table's changes); this port
//! has no live IVM pipeline yet, so this is the equivalent static matcher over
//! the tracked query ASTs — the pure decision that sits between a fanned-out
//! `CommitNotification` and the `CVRQueryDrivenUpdater` re-execution it would
//! drive. Table names are matched bare (unqualified), matching how the
//! change-log keys changes by `table`.

use std::collections::BTreeSet;

use zero_cache_protocol::ast::{referenced_tables, Ast};

/// Returns the hashes of every query whose read-set intersects
/// `changed_tables`, i.e. the queries a commit touching those tables
/// invalidates. Deterministic (sorted) output.
///
/// `queries` pairs each tracked query's hash (its stable identity) with its
/// AST. A query with no overlap is left untouched — it need not be
/// re-hydrated for this commit.
pub fn invalidated_query_hashes<'a, I>(changed_tables: &BTreeSet<String>, queries: I) -> Vec<String>
where
    I: IntoIterator<Item = (&'a str, &'a Ast)>,
{
    let mut out: BTreeSet<String> = BTreeSet::new();
    for (hash, ast) in queries {
        if referenced_tables(ast)
            .iter()
            .any(|t| changed_tables.contains(t))
        {
            out.insert(hash.to_string());
        }
    }
    out.into_iter().collect()
}

/// Of the `invalidated` query hashes, the ones that actually need
/// re-hydration: exactly those the CVR currently holds as **"got"**
/// (`got_query_hashes`).
///
/// This is the decision at the invalidation→re-hydration boundary. A query
/// that was invalidated but is NOT currently "got" needs no re-run here — if
/// it is merely *desired* (not yet hydrated) it will hydrate fresh on its own
/// path, and if the client doesn't track it at all the commit is irrelevant to
/// it. Only a query whose results the client already has AND whose underlying
/// tables changed must be re-executed so the row diff can be poked out.
/// Deterministic (sorted) output.
pub fn queries_to_reexecute(
    invalidated: &[String],
    got_query_hashes: &BTreeSet<String>,
) -> Vec<String> {
    let mut out: BTreeSet<String> = BTreeSet::new();
    for hash in invalidated {
        if got_query_hashes.contains(hash) {
            out.insert(hash.clone());
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::{Condition, CorrelatedSubquery, Correlation, ExistsOp};

    fn issue_with_comments() -> Ast {
        // issue -> related comments
        Ast {
            table: "issue".into(),
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast::table("comments")),
                system: None,
                hidden: None,
            }]),
            ..Default::default()
        }
    }

    fn user_with_exists_org() -> Ast {
        // user WHERE EXISTS(org)
        Ast {
            table: "user".into(),
            where_: Some(Condition::CorrelatedSubquery {
                related: CorrelatedSubquery {
                    correlation: Correlation {
                        parent_field: vec!["orgID".into()],
                        child_field: vec!["id".into()],
                    },
                    subquery: Box::new(Ast::table("org")),
                    system: None,
                    hidden: None,
                },
                op: ExistsOp::Exists,
                flip: None,
                scalar: None,
                plan_id: None,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn commit_on_primary_table_invalidates_that_query() {
        let q1 = issue_with_comments();
        let q2 = user_with_exists_org();
        let changed = BTreeSet::from(["issue".to_string()]);
        let hits = invalidated_query_hashes(&changed, [("h1", &q1), ("h2", &q2)]);
        assert_eq!(hits, vec!["h1".to_string()]);
    }

    #[test]
    fn commit_on_a_related_hop_table_invalidates_the_parent_query() {
        // A change to `comments` must invalidate the issue query even though
        // `comments` is only reached via a related hop.
        let q1 = issue_with_comments();
        let changed = BTreeSet::from(["comments".to_string()]);
        let hits = invalidated_query_hashes(&changed, [("h1", &q1)]);
        assert_eq!(hits, vec!["h1".to_string()]);
    }

    #[test]
    fn commit_on_a_correlated_where_table_invalidates_the_query() {
        // A change to `org` must invalidate `user WHERE EXISTS(org)`.
        let q2 = user_with_exists_org();
        let changed = BTreeSet::from(["org".to_string()]);
        let hits = invalidated_query_hashes(&changed, [("h2", &q2)]);
        assert_eq!(hits, vec!["h2".to_string()]);
    }

    #[test]
    fn commit_on_an_unreferenced_table_invalidates_nothing() {
        let q1 = issue_with_comments();
        let q2 = user_with_exists_org();
        let changed = BTreeSet::from(["unrelated".to_string()]);
        let hits = invalidated_query_hashes(&changed, [("h1", &q1), ("h2", &q2)]);
        assert!(hits.is_empty());
    }

    #[test]
    fn a_multi_table_commit_invalidates_every_overlapping_query() {
        let q1 = issue_with_comments();
        let q2 = user_with_exists_org();
        // One commit touching both `comments` (q1's hop) and `user` (q2's
        // primary) invalidates both, sorted.
        let changed = BTreeSet::from(["comments".to_string(), "user".to_string()]);
        let hits = invalidated_query_hashes(&changed, [("h1", &q1), ("h2", &q2)]);
        assert_eq!(hits, vec!["h1".to_string(), "h2".to_string()]);
    }

    #[test]
    fn reexecute_only_the_got_queries_among_the_invalidated() {
        // h1 and h2 were invalidated; the CVR currently has h1 "got" and h3
        // "got" (h3 wasn't invalidated). Only h1 needs re-execution: h2 is
        // invalidated but not got (still hydrating / not tracked), h3 is got
        // but not invalidated.
        let invalidated = vec!["h1".to_string(), "h2".to_string()];
        let got = BTreeSet::from(["h1".to_string(), "h3".to_string()]);
        assert_eq!(
            queries_to_reexecute(&invalidated, &got),
            vec!["h1".to_string()]
        );
    }

    #[test]
    fn reexecute_is_empty_when_no_invalidated_query_is_got() {
        let invalidated = vec!["h2".to_string()];
        let got = BTreeSet::from(["h1".to_string()]);
        assert!(queries_to_reexecute(&invalidated, &got).is_empty());
    }

    #[test]
    fn reexecute_returns_all_when_every_invalidated_query_is_got() {
        let invalidated = vec!["h2".to_string(), "h1".to_string()];
        let got = BTreeSet::from(["h1".to_string(), "h2".to_string(), "h3".to_string()]);
        // Sorted, deduped, only the invalidated ones.
        assert_eq!(
            queries_to_reexecute(&invalidated, &got),
            vec!["h1".to_string(), "h2".to_string()]
        );
    }

    /// End-to-end wiring proof: a commit's changed tables drive the full
    /// invalidation → re-execution → re-hydration → poke path, composing the
    /// real ported pieces (`invalidated_query_hashes`, `queries_to_reexecute`,
    /// the IVM `hydrate_query`, `hydration_to_patches`, `build_poke`). Proves
    /// that "table X changed" turns into an actual client poke carrying the
    /// re-hydrated row for the query that reads X — the connective tissue the
    /// invalidation lane exists to provide.
    #[test]
    fn commit_drives_reexecution_and_pokes_the_rehydrated_row() {
        use crate::cvr_delete_unreferenced_rows::ExistingRow as DeleteExistingRow;
        use crate::cvr_types::{
            ClientQueryRecord, Cvr, ExternalQueryBase, QueryRecord, RowId, TtlClock,
        };
        use crate::cvr_version::CvrVersion;
        use crate::poke_builder::{build_poke, hydration_to_patches};
        use std::collections::{BTreeMap, HashMap, HashSet};
        use zero_cache_protocol::ast::{Ast, Direction};
        use zero_cache_shared::bigint_json::JsonValue;
        use zero_cache_zql::ivm::change::make_source_change_add;
        use zero_cache_zql::ivm::data::Row as ZqlRow;
        use zero_cache_zql::ivm::table_source::TableSource;

        // --- The commit: an upstream transaction touched `issues`. ---
        let changed = BTreeSet::from(["issues".to_string()]);

        // --- The tracked query reads `issues` and is currently "got". ---
        let ast = Ast::table("issues");
        let invalidated = invalidated_query_hashes(&changed, [("hash1", &ast)]);
        assert_eq!(invalidated, vec!["hash1".to_string()]);
        let got = BTreeSet::from(["hash1".to_string()]);
        let to_reexecute = queries_to_reexecute(&invalidated, &got);
        assert_eq!(to_reexecute, vec!["hash1".to_string()]);

        // --- Only because it's in the re-execute set do we re-hydrate it. ---
        assert!(to_reexecute.contains(&"hash1".to_string()));

        fn row_int(row: &ZqlRow, col: &str) -> i64 {
            match row.iter().find(|(k, _)| k == col) {
                Some((_, JsonValue::Number(n))) => *n as i64,
                _ => panic!("missing {col}"),
            }
        }

        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(vec![
            ("id".to_string(), JsonValue::Number(1.0)),
            ("title".to_string(), JsonValue::String("filed".into())),
        ]));
        let filter = zero_cache_zql::ivm::filter::Filter::new(|_row: &ZqlRow| true);

        let mut cvr = Cvr {
            id: "cg1".into(),
            version: CvrVersion {
                state_version: "01".into(),
                config_version: None,
            },
            last_active: 0.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: None,
            clients: BTreeMap::new(),
            queries: BTreeMap::from([(
                "hash1".to_string(),
                QueryRecord::Client(ClientQueryRecord {
                    base: ExternalQueryBase {
                        id: "hash1".into(),
                        transformation_hash: None,
                        transformation_version: None,
                        row_set_signature: None,
                        client_state: BTreeMap::new(),
                        patch_version: None,
                    },
                    ast: ast.clone(),
                }),
            )]),
            client_schema: None,
            profile_id: None,
        };
        let orig = cvr.version.clone();
        let mut tracked = HashSet::new();
        let mut received_rows = HashMap::new();
        let mut last_patches = HashMap::new();

        let result = crate::query_hydration::hydrate_query(
            &mut cvr,
            &orig,
            &mut tracked,
            "hash1",
            "thash1",
            &issues,
            &filter,
            |row| format!("row-{}", row_int(row, "id")),
            |_row| BTreeMap::from([("hash1".to_string(), 1i64)]),
            |row| format!("v{}", row_int(row, "id")),
            &HashMap::new(),
            &[] as &[DeleteExistingRow<String>],
            &mut received_rows,
            &mut last_patches,
        );

        let patches = hydration_to_patches(&result, &cvr.version, |key: &String| RowId {
            schema: "public".into(),
            table: "issues".into(),
            row_key: BTreeMap::from([("id".to_string(), JsonValue::String(key.clone()))]),
        });
        let poke = build_poke("commit-poke", &None, &patches, Some(1.0))
            .unwrap()
            .expect("the invalidated query re-hydrated a row, so a poke exists");

        // The poke carries the re-hydrated `issues` row out to the client.
        let rows = poke.part.rows_patch.expect("rows patch present");
        assert_eq!(rows.len(), 1);
        match &rows[0] {
            zero_cache_protocol::row_patch::RowPatchOp::Put(p) => {
                assert_eq!(p.table_name, "issues");
                assert!(p
                    .value
                    .contains(&("title".to_string(), JsonValue::String("filed".into()))));
            }
            other => panic!("expected a row Put in the poke, got {other:?}"),
        }
    }
}
