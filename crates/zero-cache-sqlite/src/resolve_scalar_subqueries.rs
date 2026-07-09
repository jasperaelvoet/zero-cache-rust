//! Port of `zqlite/src/resolve-scalar-subqueries.ts`. Found via a
//! directory-coverage scan of `zqlite/src` (previously zero representation
//! in this table). Rewrites "simple" scalar subqueries — a correlated
//! subquery whose `scalar` flag is set, and whose subquery table has a
//! unique index fully constrained by literal-equality `WHERE` clauses
//! joined only by `AND` — into a plain literal comparison, by actually
//! executing the subquery (via an injected [`ScalarExecutor`] closure, this
//! port's established pattern for a live dependency — here, the real
//! SQLite query execution — that isn't wired to this pure logic layer).
//! Non-simple scalar subqueries are left untouched, matching upstream
//! (deferred to the client's own EXISTS rewrite).

use std::collections::BTreeMap;

use zero_cache_protocol::ast::{
    Ast, ColumnReference, Condition, CorrelatedSubquery, ExistsOp, LiteralValue, SimpleOperator,
    ValuePosition,
};

/// The subset of a table's spec this module reads. Port of
/// `TableSpecWithUniqueKeys`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSpecWithUniqueKeys {
    pub unique_keys: Vec<Vec<String>>,
}

/// Port of `CompanionSubquery`: a resolved scalar subquery's own AST (whose
/// rows still need to be synced to the client so its EXISTS rewrite has
/// something to match against) plus what was resolved from it.
#[derive(Debug, Clone, PartialEq)]
pub struct CompanionSubquery {
    pub ast: Ast,
    pub child_field: String,
    pub resolved_value: ResolvedValue,
}

/// Port of the `LiteralValue | null | undefined` tri-state
/// `ScalarExecutor`/`CompanionSubquery.resolvedValue` return: `NoMatch`
/// (`undefined` — no row matched), `Null` (a row matched but the field was
/// `NULL`), or `Value` (the resolved literal).
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedValue {
    NoMatch,
    Null,
    Value(LiteralValue),
}

/// Port of `ScalarExecutor`: executes a scalar subquery and returns the
/// resolved value of `child_field` from the (at most one) matching row.
pub trait ScalarExecutor {
    fn execute(&mut self, subquery_ast: &Ast, child_field: &str) -> ResolvedValue;
}

impl<F: FnMut(&Ast, &str) -> ResolvedValue> ScalarExecutor for F {
    fn execute(&mut self, subquery_ast: &Ast, child_field: &str) -> ResolvedValue {
        self(subquery_ast, child_field)
    }
}

/// Port of `resolveSimpleScalarSubqueries`.
pub fn resolve_simple_scalar_subqueries(
    ast: &Ast,
    table_specs: &BTreeMap<String, TableSpecWithUniqueKeys>,
    execute: &mut dyn ScalarExecutor,
) -> (Ast, Vec<CompanionSubquery>) {
    let mut companions = Vec::new();
    let resolved = resolve_ast_recursive(ast, table_specs, execute, &mut companions);
    (resolved, companions)
}

fn resolve_ast_recursive(
    ast: &Ast,
    table_specs: &BTreeMap<String, TableSpecWithUniqueKeys>,
    execute: &mut dyn ScalarExecutor,
    companions: &mut Vec<CompanionSubquery>,
) -> Ast {
    let where_ = ast
        .where_
        .as_ref()
        .map(|w| resolve_condition(w, table_specs, execute, companions));
    let related = ast.related.as_ref().map(|rels| {
        rels.iter()
            .map(|r| CorrelatedSubquery {
                correlation: r.correlation.clone(),
                subquery: Box::new(resolve_ast_recursive(
                    &r.subquery,
                    table_specs,
                    execute,
                    companions,
                )),
                system: r.system,
                hidden: r.hidden,
            })
            .collect()
    });
    Ast {
        where_,
        related,
        ..ast.clone()
    }
}

fn resolve_condition(
    condition: &Condition,
    table_specs: &BTreeMap<String, TableSpecWithUniqueKeys>,
    execute: &mut dyn ScalarExecutor,
    companions: &mut Vec<CompanionSubquery>,
) -> Condition {
    match condition {
        Condition::CorrelatedSubquery {
            scalar: Some(true), ..
        } => resolve_scalar_subquery(condition, table_specs, execute, companions),
        Condition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
            plan_id,
        } => {
            let resolved_subquery =
                resolve_ast_recursive(&related.subquery, table_specs, execute, companions);
            Condition::CorrelatedSubquery {
                related: CorrelatedSubquery {
                    correlation: related.correlation.clone(),
                    subquery: Box::new(resolved_subquery),
                    system: related.system,
                    hidden: related.hidden,
                },
                op: *op,
                flip: *flip,
                scalar: *scalar,
                plan_id: *plan_id,
            }
        }
        Condition::And { conditions } => Condition::And {
            conditions: conditions
                .iter()
                .map(|c| resolve_condition(c, table_specs, execute, companions))
                .collect(),
        },
        Condition::Or { conditions } => Condition::Or {
            conditions: conditions
                .iter()
                .map(|c| resolve_condition(c, table_specs, execute, companions))
                .collect(),
        },
        Condition::Simple { .. } => condition.clone(),
    }
}

fn resolve_scalar_subquery(
    condition: &Condition,
    table_specs: &BTreeMap<String, TableSpecWithUniqueKeys>,
    execute: &mut dyn ScalarExecutor,
    companions: &mut Vec<CompanionSubquery>,
) -> Condition {
    let Condition::CorrelatedSubquery {
        related,
        op,
        flip,
        scalar,
        plan_id,
    } = condition
    else {
        unreachable!("resolve_scalar_subquery called on a non-CorrelatedSubquery condition")
    };
    let parent_field = related.correlation.parent_field[0].clone();
    let child_field = related.correlation.child_field[0].clone();

    // Recursively resolve any scalar subqueries nested in the subquery's
    // own WHERE (and related) before evaluating this one.
    let subquery = resolve_ast_recursive(&related.subquery, table_specs, execute, companions);

    if !is_simple_subquery(&subquery, table_specs) {
        return Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: related.correlation.clone(),
                subquery: Box::new(subquery),
                system: related.system,
                hidden: related.hidden,
            },
            op: *op,
            flip: *flip,
            scalar: *scalar,
            plan_id: *plan_id,
        };
    }

    let value = execute.execute(&subquery, &child_field);

    // Record the companion subquery AST so its rows are synced to the
    // client — the client rewrites scalar subqueries to EXISTS and needs
    // those rows.
    companions.push(CompanionSubquery {
        ast: subquery,
        child_field: child_field.clone(),
        resolved_value: value.clone(),
    });

    match value {
        ResolvedValue::NoMatch | ResolvedValue::Null => always_false(),
        ResolvedValue::Value(v) => {
            let cmp_op = if *op == ExistsOp::Exists {
                SimpleOperator::Eq
            } else {
                SimpleOperator::IsNot
            };
            Condition::Simple {
                op: cmp_op,
                left: ValuePosition::Column(ColumnReference { name: parent_field }),
                right: ValuePosition::Literal(v),
            }
        }
    }
}

fn always_false() -> Condition {
    Condition::Simple {
        op: SimpleOperator::Eq,
        left: ValuePosition::Literal(LiteralValue::Number(1.0)),
        right: ValuePosition::Literal(LiteralValue::Number(0.0)),
    }
}

/// Port of `isSimpleSubquery`: true when all columns of at least one
/// unique index on the subquery's table are equality-constrained by
/// literal values in its `WHERE` clause (`AND`-only).
pub fn is_simple_subquery(
    subquery: &Ast,
    table_specs: &BTreeMap<String, TableSpecWithUniqueKeys>,
) -> bool {
    let Some(spec) = table_specs.get(&subquery.table) else {
        return false;
    };
    let Some(where_) = &subquery.where_ else {
        return false;
    };
    let constraints = extract_literal_equality_constraints(where_);
    if constraints.is_empty() {
        return false;
    }
    spec.unique_keys
        .iter()
        .any(|key| key.iter().all(|col| constraints.contains_key(col)))
}

/// Port of `extractLiteralEqualityConstraints`.
pub fn extract_literal_equality_constraints(
    condition: &Condition,
) -> BTreeMap<String, LiteralValue> {
    let mut constraints = BTreeMap::new();
    collect_constraints(condition, &mut constraints);
    constraints
}

/// Port of `collectConstraints`: only follows `AND`, matching upstream
/// (an `OR`'d equality doesn't guarantee the constraint actually holds).
fn collect_constraints(condition: &Condition, constraints: &mut BTreeMap<String, LiteralValue>) {
    match condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(col),
            right: ValuePosition::Literal(v),
        } => {
            constraints.insert(col.name.clone(), v.clone());
        }
        Condition::And { conditions } => {
            for c in conditions {
                collect_constraints(c, constraints);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eq_col_lit(col: &str, value: LiteralValue) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference { name: col.into() }),
            right: ValuePosition::Literal(value),
        }
    }

    fn spec(unique_keys: Vec<Vec<&str>>) -> TableSpecWithUniqueKeys {
        TableSpecWithUniqueKeys {
            unique_keys: unique_keys
                .into_iter()
                .map(|k| k.into_iter().map(String::from).collect())
                .collect(),
        }
    }

    #[test]
    fn extract_literal_equality_constraints_follows_and_not_or() {
        let cond = Condition::And {
            conditions: vec![
                eq_col_lit("a", LiteralValue::Number(1.0)),
                eq_col_lit("b", LiteralValue::String("x".into())),
            ],
        };
        let constraints = extract_literal_equality_constraints(&cond);
        assert_eq!(constraints.len(), 2);

        let or_cond = Condition::Or {
            conditions: vec![eq_col_lit("a", LiteralValue::Number(1.0))],
        };
        assert!(
            extract_literal_equality_constraints(&or_cond).is_empty(),
            "OR must not contribute constraints"
        );
    }

    #[test]
    fn is_simple_subquery_true_when_a_full_unique_key_is_constrained() {
        let mut table_specs = BTreeMap::new();
        table_specs.insert("comment".to_string(), spec(vec![vec!["issueID", "id"]]));
        let subquery = Ast {
            table: "comment".into(),
            where_: Some(Condition::And {
                conditions: vec![
                    eq_col_lit("issueID", LiteralValue::String("i1".into())),
                    eq_col_lit("id", LiteralValue::String("c1".into())),
                ],
            }),
            ..Default::default()
        };
        assert!(is_simple_subquery(&subquery, &table_specs));
    }

    #[test]
    fn is_simple_subquery_false_when_no_unique_key_is_fully_constrained() {
        let mut table_specs = BTreeMap::new();
        table_specs.insert("comment".to_string(), spec(vec![vec!["issueID", "id"]]));
        let subquery = Ast {
            table: "comment".into(),
            where_: Some(eq_col_lit("issueID", LiteralValue::String("i1".into()))),
            ..Default::default()
        };
        assert!(
            !is_simple_subquery(&subquery, &table_specs),
            "only one of the two unique-key columns is constrained"
        );
    }

    #[test]
    fn is_simple_subquery_false_for_an_unknown_table_or_missing_where() {
        let table_specs = BTreeMap::new();
        let subquery = Ast {
            table: "unknown".into(),
            where_: None,
            ..Default::default()
        };
        assert!(!is_simple_subquery(&subquery, &table_specs));
    }

    fn scalar_exists_condition() -> Condition {
        Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: zero_cache_protocol::ast::Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast {
                    table: "comment".into(),
                    where_: Some(eq_col_lit("issueID", LiteralValue::String("i1".into()))),
                    ..Default::default()
                }),
                system: None,
                hidden: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: Some(true),
            plan_id: None,
        }
    }

    #[test]
    fn resolves_a_simple_scalar_exists_subquery_to_a_literal_eq() {
        let mut table_specs = BTreeMap::new();
        table_specs.insert("comment".to_string(), spec(vec![vec!["issueID"]]));
        let ast = Ast {
            table: "issue".into(),
            where_: Some(scalar_exists_condition()),
            ..Default::default()
        };

        let mut execute = |_subquery: &Ast, _child_field: &str| {
            ResolvedValue::Value(LiteralValue::String("c1".into()))
        };
        let (resolved, companions) =
            resolve_simple_scalar_subqueries(&ast, &table_specs, &mut execute);

        assert_eq!(
            resolved.where_,
            Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference { name: "id".into() }),
                right: ValuePosition::Literal(LiteralValue::String("c1".into()))
            })
        );
        assert_eq!(companions.len(), 1);
        assert_eq!(
            companions[0].resolved_value,
            ResolvedValue::Value(LiteralValue::String("c1".into()))
        );
    }

    #[test]
    fn resolves_a_no_match_scalar_subquery_to_always_false() {
        let mut table_specs = BTreeMap::new();
        table_specs.insert("comment".to_string(), spec(vec![vec!["issueID"]]));
        let ast = Ast {
            table: "issue".into(),
            where_: Some(scalar_exists_condition()),
            ..Default::default()
        };

        let mut execute = |_subquery: &Ast, _child_field: &str| ResolvedValue::NoMatch;
        let (resolved, _) = resolve_simple_scalar_subqueries(&ast, &table_specs, &mut execute);

        assert_eq!(resolved.where_, Some(always_false()));
    }

    #[test]
    fn resolves_not_exists_to_is_not() {
        let mut table_specs = BTreeMap::new();
        table_specs.insert("comment".to_string(), spec(vec![vec!["issueID"]]));
        let mut cond = scalar_exists_condition();
        if let Condition::CorrelatedSubquery { op, .. } = &mut cond {
            *op = ExistsOp::NotExists;
        }
        let ast = Ast {
            table: "issue".into(),
            where_: Some(cond),
            ..Default::default()
        };

        let mut execute =
            |_subquery: &Ast, _child_field: &str| ResolvedValue::Value(LiteralValue::Number(5.0));
        let (resolved, _) = resolve_simple_scalar_subqueries(&ast, &table_specs, &mut execute);

        assert_eq!(
            resolved.where_,
            Some(Condition::Simple {
                op: SimpleOperator::IsNot,
                left: ValuePosition::Column(ColumnReference { name: "id".into() }),
                right: ValuePosition::Literal(LiteralValue::Number(5.0))
            })
        );
    }

    #[test]
    fn leaves_a_non_simple_scalar_subquery_untouched_and_records_no_companion() {
        let mut table_specs = BTreeMap::new();
        table_specs.insert("comment".to_string(), spec(vec![vec!["issueID", "id"]])); // needs BOTH columns
        let ast = Ast {
            table: "issue".into(),
            where_: Some(scalar_exists_condition()),
            ..Default::default()
        }; // only constrains issueID
        let mut execute =
            |_subquery: &Ast, _child_field: &str| panic!("must not execute a non-simple subquery");
        let (resolved, companions) =
            resolve_simple_scalar_subqueries(&ast, &table_specs, &mut execute);

        assert!(companions.is_empty());
        let Some(Condition::CorrelatedSubquery { scalar, .. }) = resolved.where_ else {
            panic!("expected the CorrelatedSubquery to survive untouched")
        };
        assert_eq!(scalar, Some(true));
    }
}
