//! Port of `zql/src/builder/filter.ts`'s `createPredicate` — compiles an
//! AST [`Condition`] tree into an executable `Fn(&Row) -> bool`. This is
//! the piece that turns a query's arbitrary WHERE clause (or a permission
//! policy's `Condition`, once ported) into the predicate
//! [`crate::ivm::filter::Filter`] already knows how to run — closing the
//! gap the prior round's `ivm` slice deliberately left open ("only one
//! hand-wired predicate per query").
//!
//! Scope: [`create_predicate`] panics on `Condition::CorrelatedSubquery`
//! (join-aware filtering, e.g. `EXISTS`), matching upstream's
//! `NoSubqueryCondition` type constraint (callers there are statically
//! prevented from passing one in). [`create_predicate_with_exists`] is the
//! richer entry point added once `ivm::join` existed: it takes an `exists`
//! callback the caller wires to a real EXISTS check (e.g.
//! `ivm::join::exists_for_row` against a child `TableSource`) instead of
//! panicking — this is how a permission policy's `CorrelatedSubquery` rule
//! (e.g. "the user owns the parent issue") or a query's `EXISTS` subquery
//! condition can actually be evaluated. `ValuePosition::Parameter` still
//! always panics in both — upstream asserts `left.type !== 'static'`/
//! `right.type !== 'static'` on the premise that `bindStaticParameters`
//! (query-parameter substitution, not ported) already resolved them.

use std::rc::Rc;
use zero_cache_protocol::ast::{
    Condition, CorrelatedSubquery, ExistsOp, LiteralValue, SimpleOperator, ValuePosition,
};
use zero_cache_shared::bigint_json::JsonValue;

use crate::builder::like::get_like_predicate;
use crate::ivm::data::{compare_values, Row, Value};

fn get<'a>(row: &'a Row, field: &str) -> &'a Value {
    static NULL: Value = Value::Null;
    row.iter()
        .find(|(k, _)| k == field)
        .map(|(_, v)| v)
        .unwrap_or(&NULL)
}

fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::String(s) => Value::String(s.clone()),
        LiteralValue::Number(n) => Value::Number(*n),
        LiteralValue::Bool(b) => Value::Bool(*b),
        LiteralValue::Null => Value::Null,
        LiteralValue::Array(items) => Value::Array(items.iter().map(literal_to_value).collect()),
    }
}

/// Resolves a `ValuePosition` to either a fixed literal value or a
/// row-column accessor, at predicate-construction time (not per-row) —
/// matches upstream's `left.type === 'literal'` fast path that pre-computes
/// a constant result instead of re-reading a literal on every row.
enum Resolved {
    Literal(Value),
    Column(String),
}

fn resolve(pos: &ValuePosition) -> Resolved {
    match pos {
        ValuePosition::Literal(lit) => Resolved::Literal(literal_to_value(lit)),
        ValuePosition::Column(col) => Resolved::Column(col.name.clone()),
        ValuePosition::Parameter(_) => {
            panic!("createPredicate: static values should be resolved before creating predicates")
        }
    }
}

/// A shareable EXISTS resolver for `Condition::CorrelatedSubquery`: given
/// the correlated-subquery node and the current row, returns whether a
/// matching related row exists. `Rc` (not `Box`) so the same resolver can
/// be cloned into every recursive branch of [`create_predicate_with_exists`]
/// without re-wrapping.
pub type ExistsFn<'a> = Rc<dyn Fn(&CorrelatedSubquery, &Row) -> bool + 'a>;

/// Port of `createPredicate`. Compiles `condition` into a boxed row
/// predicate; panics if `condition` contains a `CorrelatedSubquery` — use
/// [`create_predicate_with_exists`] when the condition might have one.
pub fn create_predicate(condition: &Condition) -> Box<dyn Fn(&Row) -> bool> {
    let exists: ExistsFn = Rc::new(|_, _| {
        panic!("createPredicate: CorrelatedSubquery conditions need create_predicate_with_exists (see module doc)")
    });
    create_predicate_with_exists(condition, exists)
}

/// Like [`create_predicate`], but `Condition::CorrelatedSubquery` is
/// evaluated by calling `exists(related, row)` instead of panicking —
/// `Ok`/`true` from that callback means the EXISTS check passed for `row`.
/// `flip`/`scalar` are not consulted (this crate doesn't have a use for
/// them yet, matching `ivm::join::exists_for_row`'s own scope note).
pub fn create_predicate_with_exists<'a>(
    condition: &Condition,
    exists: ExistsFn<'a>,
) -> Box<dyn Fn(&Row) -> bool + 'a> {
    match condition {
        Condition::And { conditions } => {
            let predicates: Vec<_> = conditions
                .iter()
                .map(|c| create_predicate_with_exists(c, exists.clone()))
                .collect();
            Box::new(move |row| predicates.iter().all(|p| p(row)))
        }
        Condition::Or { conditions } => {
            let predicates: Vec<_> = conditions
                .iter()
                .map(|c| create_predicate_with_exists(c, exists.clone()))
                .collect();
            Box::new(move |row| predicates.iter().any(|p| p(row)))
        }
        Condition::CorrelatedSubquery { related, op, .. } => {
            let related = related.clone();
            let op = *op;
            Box::new(move |row: &Row| {
                let matched = exists(&related, row);
                match op {
                    ExistsOp::Exists => matched,
                    ExistsOp::NotExists => !matched,
                }
            })
        }
        Condition::Simple { op, left, right } => {
            create_simple_predicate(*op, resolve(left), resolve(right))
        }
    }
}

/// The right-hand side of a `SimpleCondition` is always a bound value
/// (literal), never a column — upstream's `impl = createPredicateImpl(
/// right.value, ...)` reads `right.value` directly once, outside any
/// per-row closure, which only type-checks if `right` is already a plain
/// value. A `Column` on the right (`literal OP column` or `column OP
/// column`) is not a shape this AST's `SimpleCondition` produces; panic
/// rather than silently mishandle it.
fn resolved_literal(resolved: &Resolved, side: &str) -> Value {
    match resolved {
        Resolved::Literal(v) => v.clone(),
        Resolved::Column(_) => panic!(
            "createPredicate: {side}-hand side of a comparison must be a literal, not a column"
        ),
    }
}

fn create_simple_predicate(
    op: SimpleOperator,
    left: Resolved,
    right: Resolved,
) -> Box<dyn Fn(&Row) -> bool> {
    if matches!(op, SimpleOperator::Is | SimpleOperator::IsNot) {
        let rhs_value = resolved_literal(&right, "right");
        return match left {
            Resolved::Literal(lv) => {
                let result = eval_is(op, &lv, &rhs_value);
                Box::new(move |_row| result)
            }
            Resolved::Column(name) => {
                Box::new(move |row: &Row| eval_is(op, get(row, &name), &rhs_value))
            }
        };
    }

    let rhs_value = resolved_literal(&right, "right");

    // Port of `if (right.value === null || right.value === undefined) return
    // () => false` — a non-IS comparison against NULL is never true.
    if matches!(rhs_value, Value::Null) {
        return Box::new(|_row| false);
    }

    match left {
        Resolved::Literal(Value::Null) => Box::new(|_row| false),
        Resolved::Literal(lv) => {
            let result = eval_non_null_op(op, &lv, &rhs_value);
            Box::new(move |_row| result)
        }
        Resolved::Column(name) => Box::new(move |row: &Row| {
            let lhs = get(row, &name);
            if matches!(lhs, Value::Null) {
                return false;
            }
            eval_non_null_op(op, lhs, &rhs_value)
        }),
    }
}

fn eval_is(op: SimpleOperator, lhs: &Value, rhs: &Value) -> bool {
    let eq = lhs == rhs;
    match op {
        SimpleOperator::Is => eq,
        SimpleOperator::IsNot => !eq,
        _ => unreachable!(),
    }
}

fn eval_non_null_op(op: SimpleOperator, lhs: &Value, rhs: &Value) -> bool {
    use SimpleOperator::*;
    match op {
        Eq => lhs == rhs,
        Ne => lhs != rhs,
        Lt => compare_values(lhs, rhs).is_lt(),
        Le => compare_values(lhs, rhs).is_le(),
        Gt => compare_values(lhs, rhs).is_gt(),
        Ge => compare_values(lhs, rhs).is_ge(),
        Like => like_matches(lhs, rhs, false),
        NotLike => !like_matches(lhs, rhs, false),
        ILike => like_matches(lhs, rhs, true),
        NotILike => !like_matches(lhs, rhs, true),
        In => in_set(lhs, rhs),
        NotIn => !in_set(lhs, rhs),
        Is | IsNot => unreachable!("handled by eval_is"),
    }
}

fn like_matches(lhs: &Value, rhs: &Value, case_insensitive: bool) -> bool {
    let JsonValue::String(pattern) = rhs else {
        panic!("LIKE: expected rhs to be a string")
    };
    let JsonValue::String(s) = lhs else {
        panic!("LIKE: expected lhs to be a string")
    };
    get_like_predicate(pattern, case_insensitive).matches(s)
}

fn in_set(lhs: &Value, rhs: &Value) -> bool {
    let JsonValue::Array(items) = rhs else {
        panic!("IN: expected rhs to be an array")
    };
    items.iter().any(|item| item == lhs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::ColumnReference;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }
    fn col(name: &str) -> ValuePosition {
        ValuePosition::Column(ColumnReference { name: name.into() })
    }
    fn lit_num(v: f64) -> ValuePosition {
        ValuePosition::Literal(LiteralValue::Number(v))
    }
    fn lit_str(v: &str) -> ValuePosition {
        ValuePosition::Literal(LiteralValue::String(v.into()))
    }
    fn lit_null() -> ValuePosition {
        ValuePosition::Literal(LiteralValue::Null)
    }
    fn simple(op: SimpleOperator, left: ValuePosition, right: ValuePosition) -> Condition {
        Condition::Simple { op, left, right }
    }

    #[test]
    fn eq_matches_column_to_literal() {
        let cond = simple(SimpleOperator::Eq, col("a"), lit_num(1.0));
        let p = create_predicate(&cond);
        assert!(p(&row(&[("a", Value::Number(1.0))])));
        assert!(!p(&row(&[("a", Value::Number(2.0))])));
    }

    #[test]
    fn non_is_comparison_against_null_rhs_is_always_false() {
        let cond = simple(SimpleOperator::Eq, col("a"), lit_null());
        let p = create_predicate(&cond);
        assert!(!p(&row(&[("a", Value::Null)])));
    }

    #[test]
    fn non_is_comparison_with_null_lhs_row_value_is_false() {
        let cond = simple(SimpleOperator::Eq, col("a"), lit_num(1.0));
        let p = create_predicate(&cond);
        assert!(!p(&row(&[]))); // missing column -> null
    }

    #[test]
    fn is_null_matches_missing_column() {
        let cond = simple(SimpleOperator::Is, col("a"), lit_null());
        let p = create_predicate(&cond);
        assert!(p(&row(&[])));
        assert!(!p(&row(&[("a", Value::Number(1.0))])));
    }

    #[test]
    fn is_not_null_is_inverse() {
        let cond = simple(SimpleOperator::IsNot, col("a"), lit_null());
        let p = create_predicate(&cond);
        assert!(!p(&row(&[])));
        assert!(p(&row(&[("a", Value::Number(1.0))])));
    }

    #[test]
    fn and_requires_all() {
        let cond = Condition::And {
            conditions: vec![
                simple(SimpleOperator::Eq, col("a"), lit_num(1.0)),
                simple(SimpleOperator::Eq, col("b"), lit_num(2.0)),
            ],
        };
        let p = create_predicate(&cond);
        assert!(p(&row(&[
            ("a", Value::Number(1.0)),
            ("b", Value::Number(2.0))
        ])));
        assert!(!p(&row(&[
            ("a", Value::Number(1.0)),
            ("b", Value::Number(3.0))
        ])));
    }

    #[test]
    fn or_requires_any() {
        let cond = Condition::Or {
            conditions: vec![
                simple(SimpleOperator::Eq, col("a"), lit_num(1.0)),
                simple(SimpleOperator::Eq, col("a"), lit_num(2.0)),
            ],
        };
        let p = create_predicate(&cond);
        assert!(p(&row(&[("a", Value::Number(1.0))])));
        assert!(p(&row(&[("a", Value::Number(2.0))])));
        assert!(!p(&row(&[("a", Value::Number(3.0))])));
    }

    #[test]
    fn lt_uses_compare_values_not_raw_ordering() {
        let cond = simple(SimpleOperator::Lt, col("a"), lit_str("b"));
        let p = create_predicate(&cond);
        assert!(p(&row(&[("a", Value::String("a".into()))])));
        assert!(!p(&row(&[("a", Value::String("c".into()))])));
    }

    #[test]
    fn like_with_wildcard() {
        let cond = simple(SimpleOperator::Like, col("name"), lit_str("f%"));
        let p = create_predicate(&cond);
        assert!(p(&row(&[("name", Value::String("foo".into()))])));
        assert!(!p(&row(&[("name", Value::String("bar".into()))])));
    }

    #[test]
    fn ilike_is_case_insensitive() {
        let cond = simple(SimpleOperator::ILike, col("name"), lit_str("FOO"));
        let p = create_predicate(&cond);
        assert!(p(&row(&[("name", Value::String("foo".into()))])));
    }

    #[test]
    fn in_matches_membership() {
        let cond = simple(
            SimpleOperator::In,
            col("a"),
            ValuePosition::Literal(LiteralValue::Array(vec![
                LiteralValue::Number(1.0),
                LiteralValue::Number(2.0),
            ])),
        );
        let p = create_predicate(&cond);
        assert!(p(&row(&[("a", Value::Number(1.0))])));
        assert!(!p(&row(&[("a", Value::Number(3.0))])));
    }

    #[test]
    fn not_in_is_inverse() {
        let cond = simple(
            SimpleOperator::NotIn,
            col("a"),
            ValuePosition::Literal(LiteralValue::Array(vec![LiteralValue::Number(1.0)])),
        );
        let p = create_predicate(&cond);
        assert!(!p(&row(&[("a", Value::Number(1.0))])));
        assert!(p(&row(&[("a", Value::Number(2.0))])));
    }

    #[test]
    #[should_panic(expected = "CorrelatedSubquery")]
    fn correlated_subquery_panics() {
        use zero_cache_protocol::ast::{CorrelatedSubquery, Correlation, ExistsOp};
        let cond = Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec![],
                    child_field: vec![],
                },
                subquery: Box::new(zero_cache_protocol::ast::Ast::default()),
                system: None,
                hidden: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        };
        // The panic fires when the compiled predicate is actually run
        // against a row (the CorrelatedSubquery branch defers to the
        // `exists` resolver lazily, same as every other branch defers
        // column lookups to call time) — not at `create_predicate`
        // construction time.
        let p = create_predicate(&cond);
        p(&row(&[]));
    }

    fn correlated_subquery_cond(op: ExistsOp) -> Condition {
        use zero_cache_protocol::ast::Correlation;
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
            op,
            flip: None,
            scalar: None,
            plan_id: None,
        }
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn create_predicate_with_exists_calls_the_resolver_for_exists() {
        let cond = correlated_subquery_cond(ExistsOp::Exists);
        let calls = std::cell::RefCell::new(Vec::new());
        let exists: Rc<dyn Fn(&CorrelatedSubquery, &Row) -> bool> = Rc::new(|related, row| {
            calls
                .borrow_mut()
                .push((related.correlation.child_field.clone(), row.clone()));
            true
        });
        let p = create_predicate_with_exists(&cond, exists);
        let r = row(&[("id", Value::Number(1.0))]);
        assert!(p(&r));
        assert_eq!(calls.borrow().len(), 1);
        assert_eq!(calls.borrow()[0].0, vec!["issueID".to_string()]);
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn create_predicate_with_exists_not_exists_inverts_the_resolver() {
        let cond = correlated_subquery_cond(ExistsOp::NotExists);
        let exists: Rc<dyn Fn(&CorrelatedSubquery, &Row) -> bool> = Rc::new(|_, _| true);
        let p = create_predicate_with_exists(&cond, exists);
        assert!(!p(&row(&[])));
    }

    #[test]
    #[allow(clippy::type_complexity)]
    fn create_predicate_with_exists_composes_with_and() {
        let cond = Condition::And {
            conditions: vec![
                simple(SimpleOperator::Eq, col("a"), lit_num(1.0)),
                correlated_subquery_cond(ExistsOp::Exists),
            ],
        };
        let exists: Rc<dyn Fn(&CorrelatedSubquery, &Row) -> bool> = Rc::new(|_, _| false);
        let p = create_predicate_with_exists(&cond, exists);
        assert!(
            !p(&row(&[("a", Value::Number(1.0))])),
            "AND should short-circuit false when the subquery check fails"
        );
    }

    /// Real integration: `create_predicate_with_exists` wired to
    /// `ivm::join::exists_for_row` against an actual `TableSource` — not a
    /// mocked resolver, proving the whole path (AST Condition -> compiled
    /// predicate -> real EXISTS check against real joined data) works.
    #[test]
    #[allow(clippy::type_complexity)]
    fn create_predicate_with_exists_wired_to_a_real_table_source() {
        use crate::ivm::change::make_source_change_add;
        use crate::ivm::table_source::TableSource;
        use zero_cache_protocol::ast::Direction;

        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        comments.push(make_source_change_add(vec![
            ("id".into(), Value::Number(100.0)),
            ("issueID".into(), Value::Number(1.0)),
        ]));

        let cond = correlated_subquery_cond(ExistsOp::Exists);
        let exists: Rc<dyn Fn(&CorrelatedSubquery, &Row) -> bool> = Rc::new(move |related, row| {
            crate::ivm::join::exists_for_row(row, &comments, &related.correlation)
        });
        let p = create_predicate_with_exists(&cond, exists);

        assert!(
            p(&row(&[("id", Value::Number(1.0))])),
            "issue 1 has a matching comment"
        );
        assert!(
            !p(&row(&[("id", Value::Number(2.0))])),
            "issue 2 has no matching comment"
        );
    }
}
