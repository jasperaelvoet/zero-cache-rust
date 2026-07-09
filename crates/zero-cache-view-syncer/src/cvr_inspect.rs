//! Pure inspect-query projection for CVR snapshots.
//!
//! This ports the non-SQL semantics of `CVRStore.inspectQueries` from
//! `zero-cache/src/services/view-syncer/cvr-store.ts`: expose each external
//! query's per-client desire state as an inspector row, suppress expired
//! inactive states, count rows whose ref-counts mention the query, and order by
//! `(clientID, queryHash)`.
//!
//! The persisted `desires.deleted` bit is retained on `ClientQueryState`, so
//! deleted-but-not-expired desires are visible to the inspector like upstream.

use zero_cache_protocol::inspect_down::InspectQueryRow;

use crate::cvr_types::{Cvr, QueryRecord, RowRecord, TtlClock};

fn row_count_for_query(rows: &[RowRecord], query_id: &str) -> f64 {
    rows.iter()
        .filter(|row| {
            row.ref_counts
                .as_ref()
                .is_some_and(|counts| counts.contains_key(query_id))
        })
        .count() as f64
}

fn expired(ttl_clock: TtlClock, inactivated_at: Option<TtlClock>, ttl: f64) -> bool {
    let Some(inactivated_at) = inactivated_at else {
        return false;
    };
    ttl >= 0.0 && inactivated_at.as_number() + ttl <= ttl_clock.as_number()
}

/// Projects inspect query rows from an in-memory CVR snapshot.
///
/// `row_records` supplies the row-cache side of `CVRStore.inspectQueries`'s
/// `rowCount` subquery. The optional `client_id` mirrors the upstream
/// per-client filter.
pub fn inspect_queries_from_cvr(
    cvr: &Cvr,
    row_records: &[RowRecord],
    client_id: Option<&str>,
) -> Vec<InspectQueryRow> {
    let mut rows = Vec::new();

    for (query_id, query) in &cvr.queries {
        match query {
            QueryRecord::Client(query) => {
                for (cid, state) in &query.base.client_state {
                    if client_id.is_some_and(|wanted| wanted != cid) {
                        continue;
                    }
                    if expired(cvr.ttl_clock, state.inactivated_at, state.ttl) {
                        continue;
                    }
                    rows.push(InspectQueryRow {
                        client_id: cid.clone(),
                        query_id: query_id.clone(),
                        ast: Some(query.ast.clone()),
                        name: None,
                        args: None,
                        got: query.base.patch_version.is_some(),
                        deleted: state.deleted,
                        ttl: state.ttl,
                        inactivated_at: state.inactivated_at.map(TtlClock::as_number),
                        row_count: row_count_for_query(row_records, query_id),
                        metrics: None,
                    });
                }
            }
            QueryRecord::Custom(query) => {
                for (cid, state) in &query.base.client_state {
                    if client_id.is_some_and(|wanted| wanted != cid) {
                        continue;
                    }
                    if expired(cvr.ttl_clock, state.inactivated_at, state.ttl) {
                        continue;
                    }
                    rows.push(InspectQueryRow {
                        client_id: cid.clone(),
                        query_id: query_id.clone(),
                        ast: None,
                        name: Some(query.name.clone()),
                        args: Some(query.args.clone()),
                        got: query.base.patch_version.is_some(),
                        deleted: state.deleted,
                        ttl: state.ttl,
                        inactivated_at: state.inactivated_at.map(TtlClock::as_number),
                        row_count: row_count_for_query(row_records, query_id),
                        metrics: None,
                    });
                }
            }
            QueryRecord::Internal(_) => {}
        }
    }

    rows.sort_by(|a, b| {
        a.client_id
            .cmp(&b.client_id)
            .then_with(|| a.query_id.cmp(&b.query_id))
    });
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use zero_cache_protocol::ast::Ast;
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_zql::ttl::DEFAULT_TTL_MS;

    use crate::cvr_types::{
        ClientQueryRecord, ClientQueryState, ClientRecord, CustomQueryRecord, Cvr, CvrRecordBase,
        ExternalQueryBase, InternalQueryRecord, RowId, RowRecord,
    };
    use crate::cvr_version::{empty_cvr_version, one_after};

    fn base(id: &str, patch: bool) -> ExternalQueryBase {
        ExternalQueryBase {
            id: id.to_string(),
            transformation_hash: None,
            transformation_version: None,
            row_set_signature: None,
            client_state: BTreeMap::new(),
            patch_version: patch.then(empty_cvr_version),
        }
    }

    fn state(ttl: f64, inactivated_at: Option<f64>) -> ClientQueryState {
        ClientQueryState {
            inactivated_at: inactivated_at.map(TtlClock::from_number),
            ttl,
            deleted: false,
            version: empty_cvr_version(),
        }
    }

    fn deleted_state(ttl: f64, inactivated_at: Option<f64>) -> ClientQueryState {
        ClientQueryState {
            inactivated_at: inactivated_at.map(TtlClock::from_number),
            ttl,
            deleted: true,
            version: empty_cvr_version(),
        }
    }

    fn cvr() -> Cvr {
        Cvr {
            id: "cg1".to_string(),
            version: empty_cvr_version(),
            last_active: 0.0,
            ttl_clock: TtlClock::from_number(1_000.0),
            replica_version: None,
            clients: BTreeMap::from([
                (
                    "client1".to_string(),
                    ClientRecord {
                        id: "client1".to_string(),
                        desired_query_ids: vec!["bar".to_string(), "foo".to_string()],
                    },
                ),
                (
                    "client2".to_string(),
                    ClientRecord {
                        id: "client2".to_string(),
                        desired_query_ids: vec!["bar".to_string(), "baz".to_string()],
                    },
                ),
            ]),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        }
    }

    fn row(id: &str, counts: &[(&str, i64)]) -> RowRecord {
        RowRecord {
            base: CvrRecordBase {
                patch_version: empty_cvr_version(),
            },
            id: RowId {
                schema: "public".to_string(),
                table: "issues".to_string(),
                row_key: BTreeMap::from([("id".to_string(), JsonValue::String(id.to_string()))]),
            },
            row_version: "01".to_string(),
            ref_counts: Some(
                counts
                    .iter()
                    .map(|(query_id, count)| (query_id.to_string(), *count))
                    .collect(),
            ),
        }
    }

    #[test]
    fn projects_external_queries_sorted_with_counts() {
        let mut cvr = cvr();

        let mut foo = ClientQueryRecord {
            base: base("foo", true),
            ast: Ast::table("issues"),
        };
        foo.base
            .client_state
            .insert("client1".to_string(), state(DEFAULT_TTL_MS, None));

        let mut bar = ClientQueryRecord {
            base: base("bar", true),
            ast: Ast::table("users"),
        };
        bar.base
            .client_state
            .insert("client1".to_string(), state(DEFAULT_TTL_MS, None));
        bar.base
            .client_state
            .insert("client2".to_string(), state(DEFAULT_TTL_MS, Some(500.0)));

        let mut baz = CustomQueryRecord {
            base: base("baz", true),
            name: "named".to_string(),
            args: vec![
                JsonValue::String("arg1".to_string()),
                JsonValue::Number(1.0),
                JsonValue::Bool(true),
            ],
        };
        baz.base
            .client_state
            .insert("client2".to_string(), state(7_200.0, None));

        cvr.queries
            .insert("foo".to_string(), QueryRecord::Client(foo));
        cvr.queries
            .insert("bar".to_string(), QueryRecord::Client(bar));
        cvr.queries
            .insert("baz".to_string(), QueryRecord::Custom(baz));

        let rows = inspect_queries_from_cvr(
            &cvr,
            &[
                row("1", &[("foo", 1), ("bar", 1)]),
                row("2", &[("foo", 0), ("bar", 2)]),
                row("3", &[("other", 1)]),
            ],
            None,
        );

        assert_eq!(
            rows.iter()
                .map(|row| (row.client_id.as_str(), row.query_id.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("client1", "bar"),
                ("client1", "foo"),
                ("client2", "bar"),
                ("client2", "baz")
            ]
        );
        assert_eq!(rows[0].ast, Some(Ast::table("users")));
        assert_eq!(rows[0].row_count, 2.0);
        assert_eq!(rows[1].row_count, 2.0);
        assert_eq!(rows[2].inactivated_at, Some(500.0));
        assert_eq!(rows[3].ast, None);
        assert_eq!(rows[3].name.as_deref(), Some("named"));
        assert_eq!(
            rows[3].args,
            Some(vec![
                JsonValue::String("arg1".to_string()),
                JsonValue::Number(1.0),
                JsonValue::Bool(true),
            ])
        );
    }

    #[test]
    fn filters_to_one_client_and_skips_internal_queries() {
        let mut cvr = cvr();

        let mut external = ClientQueryRecord {
            base: base("bar", false),
            ast: Ast::table("users"),
        };
        external
            .base
            .client_state
            .insert("client1".to_string(), state(DEFAULT_TTL_MS, None));
        external
            .base
            .client_state
            .insert("client2".to_string(), state(DEFAULT_TTL_MS, None));

        cvr.queries
            .insert("bar".to_string(), QueryRecord::Client(external));
        cvr.queries.insert(
            "internal".to_string(),
            QueryRecord::Internal(InternalQueryRecord {
                id: "internal".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
                ast: Ast::table("zero.clients"),
            }),
        );

        let rows = inspect_queries_from_cvr(&cvr, &[], Some("client2"));

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].client_id, "client2");
        assert_eq!(rows[0].query_id, "bar");
        assert!(!rows[0].got);
    }

    #[test]
    fn suppresses_expired_inactive_queries() {
        let mut cvr = cvr();
        cvr.ttl_clock = TtlClock::from_number(0.0);

        let mut query = ClientQueryRecord {
            base: base("q", true),
            ast: Ast::table("issues"),
        };
        query
            .base
            .client_state
            .insert("expired".to_string(), state(60_000.0, Some(-120_000.0)));
        query
            .base
            .client_state
            .insert("active".to_string(), state(60_000.0, Some(-30_000.0)));
        query
            .base
            .client_state
            .insert("forever".to_string(), state(-1.0, Some(-120_000.0)));
        cvr.queries
            .insert("q".to_string(), QueryRecord::Client(query));

        let rows = inspect_queries_from_cvr(&cvr, &[], None);

        assert_eq!(
            rows.iter()
                .map(|row| row.client_id.as_str())
                .collect::<Vec<_>>(),
            vec!["active", "forever"]
        );

        cvr.ttl_clock = TtlClock::from_number(-120_000.0);
        let rows = inspect_queries_from_cvr(&cvr, &[], None);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn includes_deleted_but_not_expired_queries() {
        let mut cvr = cvr();
        cvr.ttl_clock = TtlClock::from_number(0.0);

        let mut query = ClientQueryRecord {
            base: base("q", true),
            ast: Ast::table("issues"),
        };
        query.base.client_state.insert(
            "deleted-active".to_string(),
            deleted_state(60_000.0, Some(-10_000.0)),
        );
        query.base.client_state.insert(
            "deleted-expired".to_string(),
            deleted_state(60_000.0, Some(-120_000.0)),
        );
        cvr.queries
            .insert("q".to_string(), QueryRecord::Client(query));

        let rows = inspect_queries_from_cvr(&cvr, &[], None);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].client_id, "deleted-active");
        assert!(rows[0].deleted);
        assert_eq!(rows[0].inactivated_at, Some(-10_000.0));
    }

    #[test]
    fn tombstoned_rows_do_not_contribute_to_count() {
        let mut cvr = cvr();
        let mut query = ClientQueryRecord {
            base: base("q", true),
            ast: Ast::table("issues"),
        };
        query
            .base
            .client_state
            .insert("client1".to_string(), state(DEFAULT_TTL_MS, None));
        cvr.queries
            .insert("q".to_string(), QueryRecord::Client(query));

        let mut tombstone = row("1", &[("q", 1)]);
        tombstone.ref_counts = None;
        let rows = inspect_queries_from_cvr(&cvr, &[tombstone, row("2", &[("q", 0)])], None);

        assert_eq!(rows[0].row_count, 1.0);
    }

    #[test]
    fn got_tracks_external_patch_version() {
        let mut cvr = cvr();
        let mut query = ClientQueryRecord {
            base: base("q", true),
            ast: Ast::table("issues"),
        };
        query.base.patch_version = Some(one_after(&Some(empty_cvr_version())));
        query
            .base
            .client_state
            .insert("client1".to_string(), state(DEFAULT_TTL_MS, None));
        cvr.queries
            .insert("q".to_string(), QueryRecord::Client(query));

        let rows = inspect_queries_from_cvr(&cvr, &[], None);

        assert!(rows[0].got);
        assert!(!rows[0].deleted);
        assert_eq!(rows[0].metrics, None);
    }
}
