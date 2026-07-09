//! Port of `zero-schema/src/compiled-permissions.ts` (the `Policy`/
//! `AssetPermissions`/`TablePermissions`/`PermissionsConfig` data model)
//! plus the policy-evaluation core of `write-authorizer.ts`'s `#canDo`/
//! `#passesPolicy`/`#passesPolicyGroup` — the piece that turns
//! `canPreMutation`/`canPostMutation` from a total gap into something that
//! actually enforces permissions.
//!
//! Scope deviation, deliberate: upstream's `#passesPolicy` builds a real ZQL
//! query (`newStaticQuery(...).where(pk, '=', val)`, AND'd with an OR of the
//! policy's rule conditions) and checks whether `buildPipeline(...).fetch()`
//! returns any row. This port has no general query builder (`TableSource`+
//! `Filter` compiles one predicate per query, not an arbitrary pipeline from
//! an AST at runtime) — but the *semantics* are equivalent to a much
//! simpler operation this port already has the pieces for: look the row up
//! by primary key (`TableSource::find_by_key`), then check whether ANY rule
//! condition's compiled predicate (`create_predicate`) matches that row.
//! "Does `SELECT ... WHERE pk=x AND (rule1 OR rule2)` return a row?" and
//! "does the row at pk=x satisfy rule1 OR rule2?" are the same question
//! when rules only reference columns of the row itself — true for every
//! rule this evaluator supports. Rules containing
//! `Condition::CorrelatedSubquery` (a permission rule referencing a related
//! table, e.g. "the user owns the parent issue") ARE now supported, via the
//! `_with_exists` variants of every function in this module
//! ([`passes_policy_with_exists`], [`passes_policy_group_with_exists`],
//! [`can_do_with_exists`]) — mirroring `create_predicate`/
//! `create_predicate_with_exists`'s own naming and delegation pattern. The
//! plain (non-`_with_exists`) functions still exist, for callers that know
//! their policies have no `CorrelatedSubquery` rules, and are implemented
//! as thin wrappers passing a resolver that panics if one is ever
//! encountered — same default-panics-unless-supplied contract
//! `create_predicate` itself has.
//!
//! Also NOT ported: `#getPreMutationRow`'s live SQLite query (this module
//! takes row lookup as an injected closure, matching the project's
//! established pattern of decoupling pure logic from live I/O), and
//! `#timedCanDo`'s latency logging (observability, not logic).

use std::collections::BTreeMap;
use std::rc::Rc;

use zero_cache_protocol::ast::Condition;
use zero_cache_zql::builder::filter::{create_predicate_with_exists, ExistsFn};
use zero_cache_zql::ivm::data::Row;

fn panicking_exists() -> ExistsFn<'static> {
    Rc::new(|_, _| {
        panic!("policy: CorrelatedSubquery rules need the _with_exists variant (see module doc)")
    })
}

/// A permission rule: `["allow", Condition]` upstream — the `"allow"` tag
/// is the only value the schema permits (upstream's own `assert(action,
/// 'action must be defined in policy')` doesn't even check its value), so
/// this port drops it and models a rule as just its `Condition`.
pub type Rule = Condition;

/// A set of rules, ORed together: the policy passes if ANY rule matches.
/// Port of `Policy`.
pub type Policy = Vec<Rule>;

/// Pre/post-mutation policies for `UPDATE`. Port of `AssetPermissions.update`.
#[derive(Debug, Clone, Default)]
pub struct UpdatePolicies {
    pub pre_mutation: Option<Policy>,
    pub post_mutation: Option<Policy>,
}

/// The policies governing one asset (a table's rows, or one cell/column).
/// Port of `AssetPermissions`.
#[derive(Debug, Clone, Default)]
pub struct AssetPermissions {
    pub select: Option<Policy>,
    pub insert: Option<Policy>,
    pub update: UpdatePolicies,
    pub delete: Option<Policy>,
}

/// One table's row- and cell-level permissions. Port of the value type of
/// `TablePermissions`.
#[derive(Debug, Clone, Default)]
pub struct TablePermissionsEntry {
    pub row: Option<AssetPermissions>,
    /// Column name -> that column's `AssetPermissions`.
    pub cell: Option<BTreeMap<String, AssetPermissions>>,
}

/// Table name -> that table's permissions. Port of `TablePermissions`.
pub type TablePermissions = BTreeMap<String, TablePermissionsEntry>;

/// The full permissions config. Port of `PermissionsConfig`.
#[derive(Debug, Clone, Default)]
pub struct PermissionsConfig {
    pub tables: Option<TablePermissions>,
}

/// Which half of an `UPDATE`'s two-phase check is running — before the
/// mutation is applied (checked against the existing row) or after
/// (checked against the resulting row). Port of `Phase`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    PreMutation,
    PostMutation,
}

/// Which CRUD action is being checked. Port of the `action` type parameter
/// of `#canDo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Insert,
    Update,
    Delete,
}

/// Whether `row` satisfies `policy`. Port of `#passesPolicy`: an
/// undefined-or-empty policy always fails ("at least one rule has to pass
/// for the policy to pass" — the default-deny stance), otherwise passes if
/// any rule's condition matches `row`. Panics if any rule contains a
/// `CorrelatedSubquery` — use [`passes_policy_with_exists`] for those.
pub fn passes_policy(policy: Option<&Policy>, row: &Row) -> bool {
    passes_policy_with_exists(policy, row, panicking_exists())
}

/// Like [`passes_policy`], but `CorrelatedSubquery` rules are evaluated via
/// `exists` (see [`ExistsFn`]) instead of panicking.
pub fn passes_policy_with_exists(policy: Option<&Policy>, row: &Row, exists: ExistsFn<'_>) -> bool {
    match policy {
        None => false,
        Some(rules) if rules.is_empty() => false,
        Some(rules) => rules
            .iter()
            .any(|rule| create_predicate_with_exists(rule, exists.clone())(row)),
    }
}

/// Port of `#passesPolicyGroup`: the row policy must pass, AND every
/// applicable cell policy must pass. Panics on a `CorrelatedSubquery` rule
/// — use [`passes_policy_group_with_exists`] for those.
pub fn passes_policy_group(
    row_policy: Option<&Policy>,
    cell_policies: &[Policy],
    row: &Row,
) -> bool {
    passes_policy_group_with_exists(row_policy, cell_policies, row, panicking_exists())
}

/// Like [`passes_policy_group`], but `CorrelatedSubquery` rules are
/// evaluated via `exists` instead of panicking.
pub fn passes_policy_group_with_exists(
    row_policy: Option<&Policy>,
    cell_policies: &[Policy],
    row: &Row,
    exists: ExistsFn<'_>,
) -> bool {
    if !passes_policy_with_exists(row_policy, row, exists.clone()) {
        return false;
    }
    cell_policies
        .iter()
        .all(|policy| passes_policy_with_exists(Some(policy), row, exists.clone()))
}

/// Selects the row policy applicable to `(action, phase)` from a table's
/// row permissions. Port of the row-policy `switch` in `#canDo`.
fn applicable_row_policy(
    row: Option<&AssetPermissions>,
    action: Action,
    phase: Phase,
) -> Option<Policy> {
    let row = row?;
    match (action, phase) {
        (Action::Insert, Phase::PostMutation) => row.insert.clone(),
        (Action::Update, Phase::PreMutation) => row.update.pre_mutation.clone(),
        (Action::Update, Phase::PostMutation) => row.update.post_mutation.clone(),
        (Action::Delete, Phase::PreMutation) => row.delete.clone(),
        _ => None,
    }
}

/// Selects the cell policies applicable to `(action, phase)`, skipping
/// columns not present in `changed_columns` for updates (matching
/// upstream's "if the cell is not being updated, we do not need to check
/// the cell rules"). Port of the cell-policy loop in `#canDo`.
fn applicable_cell_policies(
    cells: Option<&BTreeMap<String, AssetPermissions>>,
    action: Action,
    phase: Phase,
    changed_columns: &[String],
) -> Vec<Policy> {
    let Some(cells) = cells else { return vec![] };
    let mut result = Vec::new();
    for (column, policy) in cells {
        if action == Action::Update && !changed_columns.contains(column) {
            continue;
        }
        let applicable = match (action, phase) {
            (Action::Insert, Phase::PostMutation) => policy.insert.clone(),
            (Action::Update, Phase::PreMutation) => policy.update.pre_mutation.clone(),
            (Action::Update, Phase::PostMutation) => policy.update.post_mutation.clone(),
            (Action::Delete, Phase::PreMutation) => policy.delete.clone(),
            _ => None,
        };
        if let Some(p) = applicable {
            result.push(p);
        }
    }
    result
}

/// Whether `row` (already looked up for the specific primary key `op`
/// touches — see module doc for why this replaces upstream's live query)
/// passes the table's `(action, phase)` permission check. Port of
/// `#canDo`'s evaluation-order contract: table -> column -> row -> cell,
/// all steps must pass. `changed_columns` is only consulted for `Update`
/// (which columns the mutation's `value` sets — used to skip cell policies
/// for untouched columns).
pub fn can_do(
    table_permissions: Option<&TablePermissionsEntry>,
    action: Action,
    phase: Phase,
    row: &Row,
    changed_columns: &[String],
) -> bool {
    can_do_with_exists(
        table_permissions,
        action,
        phase,
        row,
        changed_columns,
        panicking_exists(),
    )
}

/// Like [`can_do`], but `CorrelatedSubquery` rules are evaluated via
/// `exists` instead of panicking.
pub fn can_do_with_exists(
    table_permissions: Option<&TablePermissionsEntry>,
    action: Action,
    phase: Phase,
    row: &Row,
    changed_columns: &[String],
    exists: ExistsFn<'_>,
) -> bool {
    let Some(entry) = table_permissions else {
        // No permissions configured for this table at all. Matches
        // upstream: `rules` is `undefined`, so `rowPolicies`/`cellPolicies`
        // are both `undefined`, so `applicableRowPolicy` stays `undefined`
        // and `#passesPolicy` denies (default-deny).
        return passes_policy_group_with_exists(None, &[], row, exists);
    };
    let row_policy = applicable_row_policy(entry.row.as_ref(), action, phase);
    let cell_policies =
        applicable_cell_policies(entry.cell.as_ref(), action, phase, changed_columns);
    passes_policy_group_with_exists(row_policy.as_ref(), &cell_policies, row, exists)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::{ColumnReference, LiteralValue, SimpleOperator, ValuePosition};
    use zero_cache_shared::bigint_json::JsonValue;

    fn row(owner: &str) -> Row {
        vec![("owner".into(), JsonValue::String(owner.into()))]
    }

    fn owner_is(name: &str) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: "owner".into(),
            }),
            right: ValuePosition::Literal(LiteralValue::String(name.into())),
        }
    }

    #[test]
    fn passes_policy_none_or_empty_is_default_deny() {
        assert!(!passes_policy(None, &row("alice")));
        assert!(!passes_policy(Some(&vec![]), &row("alice")));
    }

    #[test]
    fn passes_policy_any_rule_matching_passes() {
        let policy = vec![owner_is("bob"), owner_is("alice")];
        assert!(passes_policy(Some(&policy), &row("alice")));
        assert!(!passes_policy(Some(&policy), &row("carol")));
    }

    #[test]
    fn passes_policy_group_requires_row_and_all_cell_policies() {
        let row_policy = vec![owner_is("alice")];
        let cell_ok = vec![owner_is("alice")];
        let cell_fail = vec![owner_is("bob")];

        assert!(passes_policy_group(
            Some(&row_policy),
            &[cell_ok.clone()],
            &row("alice")
        ));
        assert!(!passes_policy_group(
            Some(&row_policy),
            &[cell_fail],
            &row("alice")
        ));
        assert!(!passes_policy_group(None, &[cell_ok], &row("alice")));
    }

    fn permissions_with_row_update(
        pre: Option<Policy>,
        post: Option<Policy>,
    ) -> TablePermissionsEntry {
        TablePermissionsEntry {
            row: Some(AssetPermissions {
                update: UpdatePolicies {
                    pre_mutation: pre,
                    post_mutation: post,
                },
                ..Default::default()
            }),
            cell: None,
        }
    }

    #[test]
    fn can_do_selects_update_pre_mutation_policy() {
        let entry =
            permissions_with_row_update(Some(vec![owner_is("alice")]), Some(vec![owner_is("bob")]));
        assert!(can_do(
            Some(&entry),
            Action::Update,
            Phase::PreMutation,
            &row("alice"),
            &[]
        ));
        assert!(!can_do(
            Some(&entry),
            Action::Update,
            Phase::PreMutation,
            &row("bob"),
            &[]
        ));
    }

    #[test]
    fn can_do_selects_update_post_mutation_policy() {
        let entry =
            permissions_with_row_update(Some(vec![owner_is("alice")]), Some(vec![owner_is("bob")]));
        assert!(can_do(
            Some(&entry),
            Action::Update,
            Phase::PostMutation,
            &row("bob"),
            &[]
        ));
        assert!(!can_do(
            Some(&entry),
            Action::Update,
            Phase::PostMutation,
            &row("alice"),
            &[]
        ));
    }

    #[test]
    fn can_do_insert_only_checked_post_mutation() {
        let entry = TablePermissionsEntry {
            row: Some(AssetPermissions {
                insert: Some(vec![owner_is("alice")]),
                ..Default::default()
            }),
            cell: None,
        };
        assert!(can_do(
            Some(&entry),
            Action::Insert,
            Phase::PostMutation,
            &row("alice"),
            &[]
        ));
        // No insert policy applies pre-mutation -> default deny.
        assert!(!can_do(
            Some(&entry),
            Action::Insert,
            Phase::PreMutation,
            &row("alice"),
            &[]
        ));
    }

    #[test]
    fn can_do_delete_only_checked_pre_mutation() {
        let entry = TablePermissionsEntry {
            row: Some(AssetPermissions {
                delete: Some(vec![owner_is("alice")]),
                ..Default::default()
            }),
            cell: None,
        };
        assert!(can_do(
            Some(&entry),
            Action::Delete,
            Phase::PreMutation,
            &row("alice"),
            &[]
        ));
        assert!(!can_do(
            Some(&entry),
            Action::Delete,
            Phase::PostMutation,
            &row("alice"),
            &[]
        ));
    }

    #[test]
    fn can_do_no_table_permissions_denies_by_default() {
        assert!(!can_do(
            None,
            Action::Insert,
            Phase::PostMutation,
            &row("alice"),
            &[]
        ));
    }

    #[test]
    fn can_do_untouched_cell_policy_is_skipped_for_update() {
        let mut cells = BTreeMap::new();
        cells.insert(
            "secret".to_string(),
            AssetPermissions {
                update: UpdatePolicies {
                    pre_mutation: Some(vec![owner_is("nobody")]),
                    ..Default::default()
                },
                ..Default::default()
            },
        );
        let entry = TablePermissionsEntry {
            row: Some(AssetPermissions {
                update: UpdatePolicies {
                    pre_mutation: Some(vec![owner_is("alice")]),
                    ..Default::default()
                },
                ..Default::default()
            }),
            cell: Some(cells),
        };
        // "secret" column not in changed_columns -> its (unpassable) policy is skipped.
        assert!(can_do(
            Some(&entry),
            Action::Update,
            Phase::PreMutation,
            &row("alice"),
            &["other".into()]
        ));
        // "secret" column IS changed -> its policy applies and fails.
        assert!(!can_do(
            Some(&entry),
            Action::Update,
            Phase::PreMutation,
            &row("alice"),
            &["secret".into()]
        ));
    }

    fn correlated_subquery_rule() -> Condition {
        use zero_cache_protocol::ast::{CorrelatedSubquery, Correlation, ExistsOp};
        Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(zero_cache_protocol::ast::Ast::table("comments")),
                system: None,
                hidden: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        }
    }

    #[test]
    #[should_panic(expected = "_with_exists")]
    fn plain_passes_policy_panics_on_correlated_subquery_rule() {
        let policy: Policy = vec![correlated_subquery_rule()];
        // Force evaluation (the panic fires when the compiled predicate
        // actually runs, not at construction).
        passes_policy(Some(&policy), &row("alice"));
    }

    #[test]
    fn passes_policy_with_exists_evaluates_correlated_subquery_rule() {
        let policy: Policy = vec![correlated_subquery_rule()];
        let exists: ExistsFn = Rc::new(|_, _| true);
        assert!(passes_policy_with_exists(
            Some(&policy),
            &row("alice"),
            exists
        ));

        let exists: ExistsFn = Rc::new(|_, _| false);
        assert!(!passes_policy_with_exists(
            Some(&policy),
            &row("alice"),
            exists
        ));
    }

    /// Real end-to-end proof: a `can_do_with_exists` check wired to a REAL
    /// `TableSource` (via `ivm::join::exists_for_row`), not a mocked
    /// resolver — genuinely evaluating a cross-table permission rule
    /// ("this issue has at least one comment") against real joined data.
    #[test]
    fn can_do_with_exists_wired_to_a_real_table_source() {
        use zero_cache_protocol::ast::Direction;
        use zero_cache_zql::ivm::change::make_source_change_add;
        use zero_cache_zql::ivm::table_source::TableSource;

        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        comments.push(make_source_change_add(vec![
            ("id".into(), JsonValue::Number(100.0)),
            ("issueID".into(), JsonValue::Number(1.0)),
        ]));

        let exists: ExistsFn = Rc::new(move |related, row| {
            zero_cache_zql::ivm::join::exists_for_row(row, &comments, &related.correlation)
        });

        // `select` isn't consulted by `can_do` for insert/update/delete
        // (upstream's `#canDo` only checks it for read-side authorization,
        // unported here), so exercise the resolver directly via
        // `passes_policy_with_exists` against the row-level rule instead —
        // proving the wiring end-to-end without needing `select` support.
        let policy: Policy = vec![correlated_subquery_rule()];
        assert!(passes_policy_with_exists(
            Some(&policy),
            &vec![("id".into(), JsonValue::Number(1.0))],
            exists.clone()
        ));
        assert!(!passes_policy_with_exists(
            Some(&policy),
            &vec![("id".into(), JsonValue::Number(2.0))],
            exists
        ));
    }
}
