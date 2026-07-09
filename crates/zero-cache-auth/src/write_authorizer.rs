//! Partial port of `src/auth/write-authorizer.ts`'s `WriteAuthorizerImpl`.
//!
//! Scope for this slice: [`validate_table_names`], [`normalize_ops`], and
//! now [`can_pre_mutation`]/[`can_post_mutation`] — see [`crate::policy`]'s
//! module doc for the scope deviation those two rely on (row-lookup +
//! predicate evaluation standing in for a live ZQL query pipeline).
//!
//! NOT ported: `#getPreMutationRow`'s live SQLite query and
//! `#timedCanDo`'s latency logging — both injected/omitted the same way as
//! elsewhere in this crate (see `normalize_ops`'s `row_exists` closure for
//! the established pattern).

use std::collections::HashSet;

use zero_cache_mutagen::crud_ops::{CrudOp, DeleteOp, InsertOp, UpdateOp, UpsertOp};
use zero_cache_zql::ivm::data::Row;

use crate::policy::{can_do, Action, Phase, TablePermissions};

/// A CRUD op with `Upsert` resolved away — port of `Exclude<CRUDOp,
/// UpsertOp>`, the return type of `normalizeOps`.
#[derive(Debug, Clone, PartialEq)]
pub enum NormalizedCrudOp {
    Insert(InsertOp),
    Update(UpdateOp),
    Delete(DeleteOp),
}

/// Error thrown by [`validate_table_names`]. Port of the `throw new
/// Error(...)` in `validateTableNames`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Table '{0}' is not a valid table.")]
pub struct InvalidTableName(pub String);

/// Validates that every op's `table_name` exists in `known_tables`. Port of
/// `validateTableNames` — `known_tables` stands in for
/// `WriteAuthorizerImpl.#tableSpecs`'s key set (computed from the live
/// replica's schema upstream; supplied by the caller here since this crate
/// has no replica access of its own).
pub fn validate_table_names(
    ops: &[CrudOp],
    known_tables: &HashSet<String>,
) -> Result<(), InvalidTableName> {
    for op in ops {
        let table_name = match op {
            CrudOp::Insert(o) => &o.table_name,
            CrudOp::Upsert(o) => &o.table_name,
            CrudOp::Update(o) => &o.table_name,
            CrudOp::Delete(o) => &o.table_name,
        };
        if !known_tables.contains(table_name) {
            return Err(InvalidTableName(table_name.clone()));
        }
    }
    Ok(())
}

/// Resolves every `Upsert` op into an `Insert` (if no row with that primary
/// key currently exists) or an `Update` (if one does), passing all other
/// ops through unchanged. Port of `normalizeOps`; `row_exists` stands in
/// for `#getPreMutationRow` (a live `TableSource` lookup upstream) since
/// this crate has no replica access of its own — callers wire it to
/// `zero_cache_zql::ivm::table_source::TableSource::find_by_key` (or
/// equivalent) with the op's primary key.
pub fn normalize_ops(
    ops: Vec<CrudOp>,
    mut row_exists: impl FnMut(&UpsertOp) -> bool,
) -> Vec<NormalizedCrudOp> {
    ops.into_iter()
        .map(|op| match op {
            CrudOp::Upsert(upsert) => {
                if row_exists(&upsert) {
                    NormalizedCrudOp::Update(UpdateOp {
                        table_name: upsert.table_name,
                        primary_key: upsert.primary_key,
                        value: upsert.value,
                    })
                } else {
                    NormalizedCrudOp::Insert(InsertOp {
                        table_name: upsert.table_name,
                        primary_key: upsert.primary_key,
                        value: upsert.value,
                    })
                }
            }
            CrudOp::Insert(o) => NormalizedCrudOp::Insert(o),
            CrudOp::Update(o) => NormalizedCrudOp::Update(o),
            CrudOp::Delete(o) => NormalizedCrudOp::Delete(o),
        })
        .collect()
}

fn changed_columns(row: &Row) -> Vec<String> {
    row.iter().map(|(k, _)| k.clone()).collect()
}

/// Port of `canPreMutation`: `Insert` runs no pre-mutation check; `Update`/
/// `Delete` must pass their table's pre-mutation policy against the row
/// that currently exists (looked up via `existing_row`, standing in for
/// `#getPreMutationRow`'s live SQLite query). Returns `false` (deny) if the
/// referenced row can't be found — matching the effect of upstream's
/// `#requirePreMutationRow` assertion failing, without actually panicking.
pub fn can_pre_mutation(
    ops: &[NormalizedCrudOp],
    tables: &TablePermissions,
    mut existing_row: impl FnMut(
        &str,
        &[(String, zero_cache_shared::bigint_json::JsonValue)],
    ) -> Option<Row>,
) -> bool {
    for op in ops {
        match op {
            NormalizedCrudOp::Insert(_) => {}
            NormalizedCrudOp::Update(u) => {
                let key: Vec<_> = u
                    .value
                    .iter()
                    .filter(|(k, _)| u.primary_key.contains(k))
                    .cloned()
                    .collect();
                let Some(row) = existing_row(&u.table_name, &key) else {
                    return false;
                };
                if !can_do(
                    tables.get(&u.table_name),
                    Action::Update,
                    Phase::PreMutation,
                    &row,
                    &changed_columns(&u.value),
                ) {
                    return false;
                }
            }
            NormalizedCrudOp::Delete(d) => {
                let key: Vec<_> = d
                    .value
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let Some(row) = existing_row(&d.table_name, &key) else {
                    return false;
                };
                if !can_do(
                    tables.get(&d.table_name),
                    Action::Delete,
                    Phase::PreMutation,
                    &row,
                    &[],
                ) {
                    return false;
                }
            }
        }
    }
    true
}

/// Port of `canPostMutation`'s permission-check half (not the
/// speculative-apply/rollback transaction it wraps that logic in
/// upstream — see module doc): `Insert`/`Update` must pass their table's
/// post-mutation policy against the resulting row; `Delete` runs no
/// post-mutation check. For `Insert` the resulting row is just `op.value`;
/// for `Update`, `resulting_row` lets the caller supply the merged
/// old+new row (upstream pushes the edit through a real `TableSource` to
/// get this — see `zero-cache-sqlite::ivm_bridge` for how this port does
/// that in the replication path).
pub fn can_post_mutation(
    ops: &[NormalizedCrudOp],
    tables: &TablePermissions,
    mut resulting_row: impl FnMut(&NormalizedCrudOp) -> Row,
) -> bool {
    for op in ops {
        match op {
            NormalizedCrudOp::Insert(i) => {
                let row = resulting_row(op);
                if !can_do(
                    tables.get(&i.table_name),
                    Action::Insert,
                    Phase::PostMutation,
                    &row,
                    &[],
                ) {
                    return false;
                }
            }
            NormalizedCrudOp::Update(u) => {
                let row = resulting_row(op);
                if !can_do(
                    tables.get(&u.table_name),
                    Action::Update,
                    Phase::PostMutation,
                    &row,
                    &changed_columns(&u.value),
                ) {
                    return false;
                }
            }
            NormalizedCrudOp::Delete(_) => {}
        }
    }
    true
}

/// WIRING: port of `mutagen.ts#processMutationWithTx`'s authorization call
/// sequence — `validateTableNames` -> `normalizeOps` -> `canPreMutation` +
/// `canPostMutation` (upstream runs the latter two concurrently via
/// `Promise.all`; both must pass — `canPre && canPost`) — composing four
/// previously-separately-tested functions that had never been chained
/// together anywhere in this port. The result is exactly the `authorized:
/// bool` that `zero_cache_mutagen::orchestration::plan_mutation_sql` takes
/// but that, until now, had nothing in this port computing it for real —
/// this had been flagged as "the single most consequential correctness
/// gap in the port" in an earlier round (nothing stopped an unauthorized
/// write if the pipeline were connected). Still not a full live wire-up
/// (that needs a real replica for `row_exists`/`existing_row`/
/// `resulting_row` — this crate has none of its own, consistent with
/// every other module here taking such lookups as closures), but the
/// actual DECISION SEQUENCE itself is now provably correct and composed,
/// not just individually-tested pieces sitting unconnected.
pub fn authorize_mutation(
    ops: Vec<CrudOp>,
    known_tables: &HashSet<String>,
    tables: &TablePermissions,
    row_exists: impl FnMut(&UpsertOp) -> bool,
    existing_row: impl FnMut(
        &str,
        &[(String, zero_cache_shared::bigint_json::JsonValue)],
    ) -> Option<Row>,
    resulting_row: impl FnMut(&NormalizedCrudOp) -> Row,
) -> Result<bool, InvalidTableName> {
    validate_table_names(&ops, known_tables)?;
    let normalized = normalize_ops(ops, row_exists);
    let can_pre = can_pre_mutation(&normalized, tables, existing_row);
    let can_post = can_post_mutation(&normalized, tables, resulting_row);
    Ok(can_pre && can_post)
}

/// WIRING: the real call site `authorize_mutation` feeds into — port of
/// `processMutationWithTx`'s full non-error-mode sequence: authorize, then
/// (only if authorized) build the SQL task list via
/// `zero_cache_mutagen::orchestration::plan_mutation_sql`. This crate
/// already depends on `zero-cache-mutagen` (for `CrudOp`), so this is
/// where the two previously-separate pieces (write authorization here,
/// SQL planning there) actually get composed end to end — the reverse
/// dependency (`zero-cache-mutagen` calling into `zero-cache-auth`) would
/// cycle, so this composition necessarily lives on the auth side. Still
/// NOT executing the SQL against a live transaction (a caller does that
/// with the returned statement list) — that's real I/O this crate has no
/// connection to drive.
#[allow(clippy::too_many_arguments)]
pub fn authorize_and_plan_mutation(
    ops: Vec<CrudOp>,
    error_mode: bool,
    known_tables: &HashSet<String>,
    tables: &TablePermissions,
    row_exists: impl FnMut(&UpsertOp) -> bool,
    existing_row: impl FnMut(
        &str,
        &[(String, zero_cache_shared::bigint_json::JsonValue)],
    ) -> Option<Row>,
    resulting_row: impl FnMut(&NormalizedCrudOp) -> Row,
) -> Result<Vec<String>, InvalidTableName> {
    if error_mode {
        return Ok(Vec::new());
    }
    let authorized = authorize_mutation(
        ops.clone(),
        known_tables,
        tables,
        row_exists,
        existing_row,
        resulting_row,
    )?;
    Ok(zero_cache_mutagen::orchestration::plan_mutation_sql(
        &ops, error_mode, authorized,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert_op(table: &str) -> CrudOp {
        CrudOp::Insert(InsertOp {
            table_name: table.into(),
            primary_key: vec!["id".into()],
            value: vec![],
        })
    }

    fn upsert_op(table: &str) -> UpsertOp {
        UpsertOp {
            table_name: table.into(),
            primary_key: vec!["id".into()],
            value: vec![],
        }
    }

    #[test]
    fn validate_table_names_ok_when_all_known() {
        let known: HashSet<String> = ["t1".to_string(), "t2".to_string()].into_iter().collect();
        let ops = vec![insert_op("t1")];
        assert_eq!(validate_table_names(&ops, &known), Ok(()));
    }

    #[test]
    fn validate_table_names_errors_on_unknown_table() {
        let known: HashSet<String> = ["t1".to_string()].into_iter().collect();
        let ops = vec![insert_op("nope")];
        assert_eq!(
            validate_table_names(&ops, &known),
            Err(InvalidTableName("nope".to_string()))
        );
    }

    #[test]
    fn normalize_ops_upsert_becomes_insert_when_row_missing() {
        let ops = vec![CrudOp::Upsert(upsert_op("t"))];
        let normalized = normalize_ops(ops, |_| false);
        assert_eq!(
            normalized,
            vec![NormalizedCrudOp::Insert(InsertOp {
                table_name: "t".into(),
                primary_key: vec!["id".into()],
                value: vec![]
            })]
        );
    }

    #[test]
    fn normalize_ops_upsert_becomes_update_when_row_exists() {
        let ops = vec![CrudOp::Upsert(upsert_op("t"))];
        let normalized = normalize_ops(ops, |_| true);
        assert_eq!(
            normalized,
            vec![NormalizedCrudOp::Update(UpdateOp {
                table_name: "t".into(),
                primary_key: vec!["id".into()],
                value: vec![]
            })]
        );
    }

    #[test]
    fn normalize_ops_passes_through_non_upsert_ops() {
        let ops = vec![insert_op("t")];
        let normalized = normalize_ops(ops, |_| {
            panic!("row_exists should not be called for non-upsert ops")
        });
        assert_eq!(
            normalized,
            vec![NormalizedCrudOp::Insert(InsertOp {
                table_name: "t".into(),
                primary_key: vec!["id".into()],
                value: vec![]
            })]
        );
    }

    #[test]
    fn normalize_ops_checks_each_upsert_independently() {
        let ops = vec![
            CrudOp::Upsert(upsert_op("exists")),
            CrudOp::Upsert(upsert_op("missing")),
        ];
        let normalized = normalize_ops(ops, |op| op.table_name == "exists");
        assert!(matches!(normalized[0], NormalizedCrudOp::Update(_)));
        assert!(matches!(normalized[1], NormalizedCrudOp::Insert(_)));
    }

    use crate::policy::{
        AssetPermissions, PermissionsConfig, TablePermissionsEntry, UpdatePolicies,
    };
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::{ColumnReference, LiteralValue, SimpleOperator, ValuePosition};
    use zero_cache_shared::bigint_json::JsonValue;

    fn owner_is(name: &str) -> zero_cache_protocol::ast::Condition {
        zero_cache_protocol::ast::Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: "owner".into(),
            }),
            right: ValuePosition::Literal(LiteralValue::String(name.into())),
        }
    }

    fn tables_with_update_policy(
        table: &str,
        pre: Option<Vec<zero_cache_protocol::ast::Condition>>,
    ) -> TablePermissions {
        let mut tables = TablePermissions::new();
        tables.insert(
            table.into(),
            TablePermissionsEntry {
                row: Some(AssetPermissions {
                    update: UpdatePolicies {
                        pre_mutation: pre,
                        ..Default::default()
                    },
                    ..Default::default()
                }),
                cell: None,
            },
        );
        tables
    }

    fn row_with_owner(owner: &str) -> Row {
        vec![
            ("owner".into(), JsonValue::String(owner.into())),
            ("id".into(), JsonValue::Number(1.0)),
        ]
    }

    #[test]
    fn can_pre_mutation_insert_always_allowed() {
        let tables = TablePermissions::new();
        let ops = vec![NormalizedCrudOp::Insert(InsertOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value: vec![],
        })];
        assert!(can_pre_mutation(&ops, &tables, |_, _| None));
    }

    #[test]
    fn can_pre_mutation_update_checks_existing_row_against_policy() {
        let tables = tables_with_update_policy("t", Some(vec![owner_is("alice")]));
        let ops = vec![NormalizedCrudOp::Update(UpdateOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value: vec![("id".into(), JsonValue::Number(1.0))],
        })];
        assert!(can_pre_mutation(&ops, &tables, |_, _| Some(
            row_with_owner("alice")
        )));
        assert!(!can_pre_mutation(&ops, &tables, |_, _| Some(
            row_with_owner("bob")
        )));
    }

    #[test]
    fn can_pre_mutation_denies_when_row_not_found() {
        let tables = tables_with_update_policy("t", Some(vec![owner_is("alice")]));
        let ops = vec![NormalizedCrudOp::Update(UpdateOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value: vec![("id".into(), JsonValue::Number(1.0))],
        })];
        assert!(!can_pre_mutation(&ops, &tables, |_, _| None));
    }

    #[test]
    fn can_post_mutation_insert_checks_new_row() {
        let mut tables = TablePermissions::new();
        tables.insert(
            "t".into(),
            TablePermissionsEntry {
                row: Some(AssetPermissions {
                    insert: Some(vec![owner_is("alice")]),
                    ..Default::default()
                }),
                cell: None,
            },
        );
        let ops = vec![NormalizedCrudOp::Insert(InsertOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value: row_with_owner("alice"),
        })];
        assert!(can_post_mutation(&ops, &tables, |op| match op {
            NormalizedCrudOp::Insert(i) => i.value.clone(),
            _ => panic!(),
        }));
    }

    #[test]
    fn can_post_mutation_delete_always_allowed() {
        let tables = TablePermissions::new();
        let mut value = BTreeMap::new();
        value.insert("id".to_string(), JsonValue::Number(1.0));
        let ops = vec![NormalizedCrudOp::Delete(DeleteOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value,
        })];
        assert!(can_post_mutation(&ops, &tables, |_| panic!(
            "resulting_row should not be called for delete"
        )));
    }

    #[test]
    fn permissions_config_smoke() {
        // Just exercises the type is constructible/usable end-to-end.
        let config = PermissionsConfig {
            tables: Some(tables_with_update_policy("t", None)),
        };
        assert!(config.tables.is_some());
    }

    fn known(tables: &[&str]) -> HashSet<String> {
        tables.iter().map(|t| t.to_string()).collect()
    }

    #[test]
    fn authorize_mutation_denies_an_unknown_table_before_checking_permissions() {
        let ops = vec![insert_op("nope")];
        let err = authorize_mutation(
            ops,
            &known(&["t"]),
            &TablePermissions::new(),
            |_| false,
            |_, _| None,
            |_| vec![],
        )
        .unwrap_err();
        assert_eq!(err, InvalidTableName("nope".to_string()));
    }

    #[test]
    fn authorize_mutation_denies_by_default_with_no_configured_permissions() {
        // Insert has no pre-mutation check, but WILL be checked post-mutation
        // — default-deny with no permissions entry at all means the whole
        // mutation is denied even though nothing "explicitly" said no.
        let ops = vec![insert_op("t")];
        let authorized = authorize_mutation(
            ops,
            &known(&["t"]),
            &TablePermissions::new(),
            |_| false,
            |_, _| None,
            |op| match op {
                NormalizedCrudOp::Insert(i) => i.value.clone(),
                _ => panic!(),
            },
        )
        .unwrap();
        assert!(
            !authorized,
            "no permissions configured for the table at all must default-deny"
        );
    }

    #[test]
    fn authorize_mutation_allows_when_both_pre_and_post_checks_pass() {
        let mut tables = TablePermissions::new();
        tables.insert(
            "t".into(),
            TablePermissionsEntry {
                row: Some(AssetPermissions {
                    update: UpdatePolicies {
                        pre_mutation: Some(vec![owner_is("alice")]),
                        post_mutation: Some(vec![owner_is("alice")]),
                    },
                    ..Default::default()
                }),
                cell: None,
            },
        );
        let ops = vec![CrudOp::Update(UpdateOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value: row_with_owner("alice"),
        })];
        let authorized = authorize_mutation(
            ops,
            &known(&["t"]),
            &tables,
            |_| false,
            |_, _| Some(row_with_owner("alice")),
            |_| row_with_owner("alice"),
        )
        .unwrap();
        assert!(authorized);
    }

    #[test]
    fn authorize_mutation_denies_when_only_the_pre_check_would_fail() {
        // canPre && canPost — a single failing phase denies the whole
        // mutation, matching upstream's `Promise.all` + `&&`.
        let mut tables = TablePermissions::new();
        tables.insert(
            "t".into(),
            TablePermissionsEntry {
                row: Some(AssetPermissions {
                    update: UpdatePolicies {
                        pre_mutation: Some(vec![owner_is("alice")]),
                        post_mutation: Some(vec![owner_is("bob")]),
                    },
                    ..Default::default()
                }),
                cell: None,
            },
        );
        let ops = vec![CrudOp::Update(UpdateOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value: row_with_owner("bob"),
        })];
        // Existing row owned by "bob" -> fails the pre-mutation "alice" check.
        let authorized = authorize_mutation(
            ops,
            &known(&["t"]),
            &tables,
            |_| false,
            |_, _| Some(row_with_owner("bob")),
            |_| row_with_owner("bob"),
        )
        .unwrap();
        assert!(!authorized);
    }

    #[test]
    fn authorize_and_plan_mutation_returns_no_sql_in_error_mode() {
        let ops = vec![insert_op("t")];
        let sql = authorize_and_plan_mutation(
            ops,
            true,
            &known(&["t"]),
            &TablePermissions::new(),
            |_| false,
            |_, _| None,
            |_| vec![],
        )
        .unwrap();
        assert!(
            sql.is_empty(),
            "error mode must never run mutation SQL, regardless of authorization"
        );
    }

    #[test]
    fn authorize_and_plan_mutation_returns_no_sql_when_unauthorized() {
        // No permissions configured at all -> default-deny -> no SQL, even
        // though error_mode is false and the op itself is well-formed.
        let ops = vec![insert_op("t")];
        let sql = authorize_and_plan_mutation(
            ops,
            false,
            &known(&["t"]),
            &TablePermissions::new(),
            |_| false,
            |_, _| None,
            |op| match op {
                NormalizedCrudOp::Insert(i) => i.value.clone(),
                _ => panic!(),
            },
        )
        .unwrap();
        assert!(sql.is_empty());
    }

    #[test]
    fn authorize_and_plan_mutation_returns_real_sql_when_authorized() {
        let mut tables = TablePermissions::new();
        tables.insert(
            "t".into(),
            TablePermissionsEntry {
                row: Some(AssetPermissions {
                    insert: Some(vec![owner_is("alice")]),
                    ..Default::default()
                }),
                cell: None,
            },
        );
        let ops = vec![CrudOp::Insert(InsertOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value: row_with_owner("alice"),
        })];
        let sql = authorize_and_plan_mutation(
            ops,
            false,
            &known(&["t"]),
            &tables,
            |_| false,
            |_, _| None,
            |op| match op {
                NormalizedCrudOp::Insert(i) => i.value.clone(),
                _ => panic!(),
            },
        )
        .unwrap();
        assert_eq!(sql.len(), 1);
        assert!(
            sql[0].contains("INSERT INTO"),
            "expected real generated SQL, got: {}",
            sql[0]
        );
    }

    #[test]
    fn authorize_and_plan_mutation_propagates_an_unknown_table_error() {
        let ops = vec![insert_op("nope")];
        let err = authorize_and_plan_mutation(
            ops,
            false,
            &known(&["t"]),
            &TablePermissions::new(),
            |_| false,
            |_, _| None,
            |_| vec![],
        )
        .unwrap_err();
        assert_eq!(err, InvalidTableName("nope".to_string()));
    }
}
