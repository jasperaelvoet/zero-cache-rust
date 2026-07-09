//! Builds the wire poke messages (`pokeStart`/`pokePart`/`pokeEnd`) from CVR
//! [`PatchToVersion`]s — the piece that turns `CvrQueryHandler`'s output into
//! the downstream frames a served connection actually sends.
//!
//! This composes with `client_handler_poke`'s already-ported pure decisions
//! (`decide_poke_end`) rather than duplicating them: this module's job is
//! purely the patch -> protocol *shape* conversion (view-syncer's
//! [`Patch`]/[`ClientRowPatch`] -> protocol's [`QueriesPatch`]/[`RowsPatch`]),
//! not the send-timing decisions those functions already own.
//!
//! Scope: query-patch TTL is not tracked on [`crate::cvr_types::QueryPatch`]
//! (it only carries the query id/op/client), so a converted `QueriesPutOp`
//! carries `ttl: None` — a caller that needs to advertise TTL to the client
//! must currently attach it out of band. This mirrors the CVR module
//! boundary used throughout this port: patch *shape*, not every downstream
//! field a full `CVRStore`-backed `ClientHandler` would additionally know.
//!
//! `pokeStart.schemaVersions` is emitted iff the poke carries a `rowsPatch`,
//! matching the wire contract (`poke.ts`: *"always set if the poke contains a
//! `rowsPatch`; may be absent for patches that only update clients and
//! queries"*). The `{min,max}` supported-version values are demo placeholders
//! (`1.0`/`1.0`) here — a real `ClientHandler` sources them from the app's
//! configured schema-version range; the *gating* on rows is faithful, only the
//! values are stubbed, per the same module boundary above.

use std::collections::BTreeMap;

use zero_cache_protocol::poke::{PokeEndBody, PokePartBody, PokeStartBody, SchemaVersions};
use zero_cache_protocol::queries_patch::{
    QueriesDelOp, QueriesPatch, QueriesPatchOp, QueriesPutOp,
};
use zero_cache_protocol::row_patch::{RowDelOp, RowPatchOp, RowPutOp, RowsPatch};
use zero_cache_protocol::version::Version;

use crate::client_patch::{
    ClientDeleteRowPatch, ClientPutRowPatch, ClientRowPatch, Patch, PatchToVersion,
};
use crate::cvr_row_received::RowClientPatch;
use crate::cvr_types::{PatchOp, RowId};
use crate::cvr_version::{
    version_to_cookie, version_to_nullable_cookie, CvrVersion, NullableCvrVersion, VersionError,
};
use crate::query_hydration::HydrationResult;

/// Converts one view-syncer [`Patch`] into its protocol-wire counterpart,
/// accumulating into `queries`/`rows`. A config patch not scoped to a specific
/// client (`client_id: None`) is filed under `""` — the caller decides how to
/// interpret an unscoped patch (typically: broadcast to every connected
/// client of the group).
fn accumulate_patch(
    patch: &Patch,
    queries: &mut BTreeMap<String, QueriesPatch>,
    rows: &mut RowsPatch,
) {
    match patch {
        Patch::Config(qp) => {
            let op = match qp.op {
                PatchOp::Put => QueriesPatchOp::Put(QueriesPutOp {
                    hash: qp.id.clone(),
                    ttl: None,
                }),
                PatchOp::Del => QueriesPatchOp::Del(QueriesDelOp {
                    hash: qp.id.clone(),
                }),
            };
            let key = qp.client_id.clone().unwrap_or_default();
            queries.entry(key).or_default().push(op);
        }
        Patch::Row(ClientRowPatch::Put(p)) => {
            rows.push(RowPatchOp::Put(RowPutOp {
                table_name: p.id.table.clone(),
                value: p.contents.clone(),
            }));
        }
        Patch::Row(ClientRowPatch::Delete(d)) => {
            rows.push(RowPatchOp::Del(RowDelOp {
                table_name: d.id.table.clone(),
                id: d.id.row_key.clone(),
            }));
        }
    }
}

/// Converts a [`HydrationResult`] (the output of
/// [`crate::query_hydration::hydrate_query`]) into the [`PatchToVersion`]s
/// [`build_poke`] consumes — the missing link between running a query against
/// the replica and actually notifying a client of its results.
///
/// * `query_patches` become unscoped (`client_id: None`) config patches, all
///   advancing to `version_after` — the CVR's version once `hydrate_query`
///   returns (a single monotonic bump across the whole hydration cycle, the
///   same convention `CvrQueryHandler` uses for its own patch batches).
/// * Each row outcome's `client_patch` becomes a row patch: `Put` pairs with
///   that row's contents (looked up in `fetched_rows`, populated by
///   `hydrate_query` in the same fetch loop so it can't drift out of sync);
///   `Del` carries just the key. A row outcome with no `client_patch` (no
///   client-visible change this cycle) contributes nothing.
///
/// `row_id` maps this query's row key type `K` to the `RowId` (schema/table/
/// primary-key) a client patch needs — the caller's job, same as
/// `hydrate_query`'s own `row_key`/`row_ref_counts` parameters, since this
/// function has no opinion on primary keys or table names.
pub fn hydration_to_patches<K: Clone + Eq + std::hash::Hash>(
    result: &HydrationResult<K>,
    version_after: &CvrVersion,
    row_id: impl Fn(&K) -> RowId,
) -> Vec<PatchToVersion> {
    let mut out: Vec<PatchToVersion> = result
        .query_patches
        .iter()
        .map(|qp| PatchToVersion {
            patch: Patch::Config(qp.clone()),
            to_version: version_after.clone(),
        })
        .collect();

    for (key, outcome) in &result.row_outcomes {
        let Some(patch) = &outcome.client_patch else {
            continue;
        };
        match patch {
            RowClientPatch::Put { to_version } => {
                let Some((_, row)) = result.fetched_rows.iter().find(|(k, _)| k == key) else {
                    continue; // Should not happen: fetched_rows is populated 1:1 with row_outcomes.
                };
                out.push(PatchToVersion {
                    patch: Patch::Row(ClientRowPatch::Put(ClientPutRowPatch {
                        id: row_id(key),
                        contents: row.clone(),
                    })),
                    to_version: to_version.clone(),
                });
            }
            RowClientPatch::Del { to_version } => {
                out.push(PatchToVersion {
                    patch: Patch::Row(ClientRowPatch::Delete(ClientDeleteRowPatch {
                        id: row_id(key),
                    })),
                    to_version: to_version.clone(),
                });
            }
        }
    }
    out
}

/// The wire messages for one poke: a start, a single part (small patch sets
/// don't need `should_flush_poke_part`'s mid-poke splitting), and an end.
/// Returned as `None` if `patches` is empty (nothing to poke).
#[derive(Debug, Clone, PartialEq)]
pub struct PokeMessages {
    pub start: PokeStartBody,
    pub part: PokePartBody,
    pub end: PokeEndBody,
}

/// Errors converting a CVR version to its wire cookie form.
pub type PokeBuildError = VersionError;

/// Builds the poke messages for a batch of same-poke patches, from
/// `base_version` (the client's current cookie) to the highest `to_version`
/// among `patches`. Returns `None` if `patches` is empty. `poke_id` and
/// `timestamp` are the caller's (this port takes both as explicit parameters,
/// matching its no-ambient-clock/no-ambient-id convention).
pub fn build_poke(
    poke_id: &str,
    base_version: &NullableCvrVersion,
    patches: &[PatchToVersion],
    timestamp: Option<f64>,
) -> Result<Option<PokeMessages>, PokeBuildError> {
    let Some(final_version) = patches
        .iter()
        .map(|p| &p.to_version)
        .max_by_key(|v| version_to_cookie(v).unwrap_or_default())
    else {
        return Ok(None);
    };

    let mut queries: BTreeMap<String, QueriesPatch> = BTreeMap::new();
    let mut rows: RowsPatch = Vec::new();
    for p in patches {
        accumulate_patch(&p.patch, &mut queries, &mut rows);
    }

    let base_cookie = version_to_nullable_cookie(base_version)?;
    let final_cookie: Version = version_to_cookie(final_version)?;

    let has_rows = !rows.is_empty();
    let start = PokeStartBody {
        poke_id: poke_id.to_string(),
        base_cookie,
        schema_versions: has_rows.then_some(SchemaVersions {
            min_supported_version: 1.0,
            max_supported_version: 1.0,
        }),
        timestamp,
    };
    let part = PokePartBody {
        poke_id: poke_id.to_string(),
        last_mutation_id_changes: None,
        desired_queries_patches: (!queries.is_empty()).then_some(queries),
        got_queries_patch: None,
        rows_patch: has_rows.then_some(rows),
        mutations_patch: None,
    };
    let end = PokeEndBody {
        poke_id: poke_id.to_string(),
        cookie: final_cookie,
        cancel: None,
    };

    Ok(Some(PokeMessages { start, part, end }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_patch::{ClientDeleteRowPatch, ClientPutRowPatch};
    use crate::cvr_types::{QueryPatch, RowId};
    use crate::cvr_version::{empty_cvr_version, CvrVersion};
    use zero_cache_shared::bigint_json::JsonValue;

    fn version(state: &str) -> CvrVersion {
        CvrVersion {
            state_version: state.into(),
            config_version: None,
        }
    }

    #[test]
    fn no_patches_yields_no_poke() {
        assert_eq!(build_poke("p1", &None, &[], None).unwrap(), None);
    }

    /// Full-stack proof: real IVM (`TableSource`+`Filter`) -> `hydrate_query`
    /// -> `hydration_to_patches` -> `build_poke` -> real wire poke messages
    /// with actual row contents, closing the loop this round set out to close.
    #[test]
    fn hydration_to_patches_then_build_poke_produces_real_row_contents() {
        use crate::cvr_delete_unreferenced_rows::ExistingRow as DeleteExistingRow;
        use crate::cvr_types::{ClientQueryRecord, ExternalQueryBase, QueryRecord};
        use std::collections::{HashMap, HashSet};
        use zero_cache_protocol::ast::Direction;
        use zero_cache_zql::ivm::change::make_source_change_add;
        use zero_cache_zql::ivm::data::Row as ZqlRow;
        use zero_cache_zql::ivm::table_source::TableSource;

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
            ("title".to_string(), JsonValue::String("hello".into())),
        ]));
        let filter = zero_cache_zql::ivm::filter::Filter::new(|_row: &ZqlRow| true);

        let mut cvr = crate::cvr_types::Cvr {
            id: "cg1".into(),
            version: version("01"),
            last_active: 0.0,
            ttl_clock: crate::cvr_types::TtlClock::from_number(0.0),
            replica_version: None,
            clients: BTreeMap::new(),
            queries: BTreeMap::from([(
                "q1".to_string(),
                QueryRecord::Client(ClientQueryRecord {
                    base: ExternalQueryBase {
                        id: "q1".into(),
                        transformation_hash: None,
                        transformation_version: None,
                        row_set_signature: None,
                        client_state: BTreeMap::new(),
                        patch_version: None,
                    },
                    ast: zero_cache_protocol::ast::Ast::default(),
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
            "q1",
            "hash1",
            &issues,
            &filter,
            |row| format!("row-{}", row_int(row, "id")),
            |_row| BTreeMap::from([("q1".to_string(), 1i64)]),
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
        // One config patch (query newly gotten) + one row put.
        assert_eq!(patches.len(), 2, "{patches:?}");

        let poke = build_poke("p1", &None, &patches, Some(1.0))
            .unwrap()
            .unwrap();
        assert!(
            poke.start.schema_versions.is_some(),
            "row data present -> schema_versions set"
        );
        let rows = poke.part.rows_patch.expect("rows_patch present");
        assert_eq!(rows.len(), 1);
        match &rows[0] {
            RowPatchOp::Put(p) => {
                assert_eq!(p.table_name, "issues");
                // The REAL fetched row contents made it all the way to the wire.
                assert!(p
                    .value
                    .contains(&("title".to_string(), JsonValue::String("hello".into()))));
            }
            other => panic!("expected Put, got {other:?}"),
        }
        let queries = poke
            .part
            .desired_queries_patches
            .expect("queries patch present");
        assert!(queries.contains_key(""), "unscoped query patch");
    }

    #[test]
    fn config_put_and_del_map_to_queries_patch_by_client() {
        let patches = vec![
            PatchToVersion {
                patch: Patch::Config(QueryPatch {
                    op: PatchOp::Put,
                    id: "hash1".into(),
                    client_id: Some("c1".into()),
                }),
                to_version: version("01"),
            },
            PatchToVersion {
                patch: Patch::Config(QueryPatch {
                    op: PatchOp::Del,
                    id: "hash2".into(),
                    client_id: Some("c1".into()),
                }),
                to_version: version("02"),
            },
        ];
        let poke = build_poke("p1", &None, &patches, Some(123.0))
            .unwrap()
            .unwrap();
        assert_eq!(poke.start.base_cookie, None);
        assert_eq!(
            poke.start.schema_versions, None,
            "no row patches -> no schema_versions"
        );
        assert_eq!(poke.end.cookie, "02");

        let per_client = poke.part.desired_queries_patches.unwrap();
        let c1 = &per_client["c1"];
        assert_eq!(c1.len(), 2);
        assert_eq!(
            c1[0],
            QueriesPatchOp::Put(QueriesPutOp {
                hash: "hash1".into(),
                ttl: None
            })
        );
        assert_eq!(
            c1[1],
            QueriesPatchOp::Del(QueriesDelOp {
                hash: "hash2".into()
            })
        );
    }

    #[test]
    fn row_patches_map_to_rows_patch_and_set_schema_versions() {
        let row_id = RowId {
            schema: "public".into(),
            table: "issue".into(),
            row_key: BTreeMap::from([("id".to_string(), JsonValue::Number(1.0))]),
        };
        let patches = vec![PatchToVersion {
            patch: Patch::Row(ClientRowPatch::Put(ClientPutRowPatch {
                id: row_id.clone(),
                contents: vec![
                    ("id".to_string(), JsonValue::Number(1.0)),
                    ("title".to_string(), JsonValue::String("hi".into())),
                ],
            })),
            to_version: version("01"),
        }];
        let poke = build_poke("p1", &Some(empty_cvr_version()), &patches, None)
            .unwrap()
            .unwrap();
        assert_eq!(poke.start.base_cookie, Some("00".into()));
        assert!(poke.start.schema_versions.is_some());
        let rows = poke.part.rows_patch.unwrap();
        assert_eq!(rows.len(), 1);
        match &rows[0] {
            RowPatchOp::Put(p) => {
                assert_eq!(p.table_name, "issue");
                assert_eq!(p.value.len(), 2);
            }
            other => panic!("expected Put, got {other:?}"),
        }
    }

    #[test]
    fn row_delete_maps_to_del_op() {
        let row_id = RowId {
            schema: "public".into(),
            table: "issue".into(),
            row_key: BTreeMap::from([("id".to_string(), JsonValue::Number(2.0))]),
        };
        let patches = vec![PatchToVersion {
            patch: Patch::Row(ClientRowPatch::Delete(ClientDeleteRowPatch { id: row_id })),
            to_version: version("01"),
        }];
        let poke = build_poke("p1", &None, &patches, None).unwrap().unwrap();
        let rows = poke.part.rows_patch.unwrap();
        match &rows[0] {
            RowPatchOp::Del(d) => {
                assert_eq!(d.table_name, "issue");
                assert_eq!(d.id.get("id"), Some(&JsonValue::Number(2.0)));
            }
            other => panic!("expected Del, got {other:?}"),
        }
    }

    #[test]
    fn final_version_is_the_max_to_version_across_patches() {
        let patches = vec![
            PatchToVersion {
                patch: Patch::Config(QueryPatch {
                    op: PatchOp::Put,
                    id: "h1".into(),
                    client_id: None,
                }),
                to_version: version("01"),
            },
            PatchToVersion {
                patch: Patch::Config(QueryPatch {
                    op: PatchOp::Put,
                    id: "h2".into(),
                    client_id: None,
                }),
                to_version: version("03"),
            },
            PatchToVersion {
                patch: Patch::Config(QueryPatch {
                    op: PatchOp::Put,
                    id: "h3".into(),
                    client_id: None,
                }),
                to_version: version("02"),
            },
        ];
        let poke = build_poke("p1", &None, &patches, None).unwrap().unwrap();
        assert_eq!(poke.end.cookie, "03");
        // Unscoped (client_id: None) patches are filed under "".
        assert!(poke.part.desired_queries_patches.unwrap().contains_key(""));
    }
}
