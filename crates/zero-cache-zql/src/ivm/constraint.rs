//! Port of `zql/src/ivm/constraint.ts`.
//!
//! A `Constraint` is a set of column=value equalities (an implicit AND) —
//! how `Source::connect`/`fetch` request rows matching a key, and how a
//! query's WHERE clause is checked for "is this actually a primary-key
//! lookup" to enable fast paths. This is pure logic with no dependency on
//! the not-yet-ported `Node`/`Stream`/`Operator` machinery, so it can be
//! ported ahead of the yield-stream architectural decision (see
//! `ivm::data`'s module doc) — it's needed by both `Filter` and `Source`
//! once those land.
//!
//! Not ported: `SetOfConstraint` (a testing-only helper, gated behind
//! `assertTesting()` upstream and unused outside `*.test.ts` files).

use zero_cache_protocol::ast::{Condition, LiteralValue, SimpleOperator, ValuePosition};

use crate::ivm::data::{values_equal, Row, Value};

/// A set of column=value equalities, implicitly ANDed. Port of
/// `Constraint` (a `{[key: string]: Value}` object) — kept as an ordered
/// `Vec` of pairs since Rust has no ambient-order-preserving map primitive
/// this crate already depends on and constraints are always small.
pub type Constraint = Vec<(String, Value)>;

/// A table's primary key: an ordered list of column names. Port of
/// `PrimaryKey`.
pub type PrimaryKey = Vec<String>;

fn get(row: &Row, field: &str) -> Value {
    row.iter()
        .find(|(k, _)| k == field)
        .map(|(_, v)| v.clone())
        .unwrap_or(Value::Null)
}

fn get_constraint<'a>(c: &'a Constraint, key: &str) -> Option<&'a Value> {
    c.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

/// Whether `row` satisfies every equality in `constraint`. Port of
/// `constraintMatchesRow`.
pub fn constraint_matches_row(constraint: &Constraint, row: &Row) -> bool {
    constraint
        .iter()
        .all(|(key, value)| values_equal(&get(row, key), value))
}

/// Constraints are compatible if they agree on every key they have in
/// common (having no keys in common is trivially compatible). Port of
/// `constraintsAreCompatible`.
pub fn constraints_are_compatible(left: &Constraint, right: &Constraint) -> bool {
    left.iter()
        .all(|(key, left_value)| match get_constraint(right, key) {
            Some(right_value) => values_equal(left_value, right_value),
            None => true,
        })
}

/// Whether `key` (a set of column names, order-independent) is exactly the
/// table's primary key. Port of `keyMatchesPrimaryKey`.
pub fn key_matches_primary_key<'a>(
    key: impl IntoIterator<Item = &'a str>,
    primary: &PrimaryKey,
) -> bool {
    let mut key: Vec<&str> = key.into_iter().collect();
    if key.len() != primary.len() {
        return false;
    }
    key.sort_unstable();
    let mut sorted_primary: Vec<&str> = primary.iter().map(String::as_str).collect();
    sorted_primary.sort_unstable();
    key == sorted_primary
}

/// Whether `constraint`'s keys are exactly the table's primary key. Port of
/// `constraintMatchesPrimaryKey`.
pub fn constraint_matches_primary_key(constraint: &Constraint, primary: &PrimaryKey) -> bool {
    key_matches_primary_key(constraint.iter().map(|(k, _)| k.as_str()), primary)
}

/// Pulls top-level `AND` components out of a condition tree — the resulting
/// list matches a superset of what the original condition would match (an
/// `OR` at the top level, or below an unpullable node, blocks further
/// pulling). Port of `pullSimpleAndComponents`. Returns simple conditions
/// only (`op`+`left`+`right`), matching the upstream `SimpleCondition[]`
/// return type.
pub fn pull_simple_and_components(
    condition: &Condition,
) -> Vec<(SimpleOperator, ValuePosition, ValuePosition)> {
    match condition {
        Condition::And { conditions } => conditions
            .iter()
            .flat_map(pull_simple_and_components)
            .collect(),
        Condition::Simple { op, left, right } => vec![(*op, left.clone(), right.clone())],
        Condition::Or { conditions } if conditions.len() == 1 => {
            pull_simple_and_components(&conditions[0])
        }
        _ => vec![],
    }
}

/// If `condition` amounts to a full primary-key equality lookup (all
/// primary-key columns pinned by top-level `AND`ed `=` comparisons against
/// literals, and nothing else), returns the resulting `Constraint`.
/// Otherwise `None`. Port of `primaryKeyConstraintFromFilters`.
pub fn primary_key_constraint_from_filters(
    condition: Option<&Condition>,
    primary: &PrimaryKey,
) -> Option<Constraint> {
    let condition = condition?;
    let conditions = pull_simple_and_components(condition);
    if conditions.is_empty() {
        return None;
    }

    let mut result: Constraint = Vec::new();
    for (op, left, right) in conditions {
        if op != SimpleOperator::Eq {
            continue;
        }
        if let Some((name, value)) = extract_column(&left, &right) {
            if primary.contains(&name) {
                result.push((name, value));
            }
        }
    }

    if result.len() != primary.len() {
        return None;
    }
    Some(result)
}

/// Port of `extractColumn`: given a simple condition's `left`/`right`, if
/// `left` is a column reference, returns its name and the (required)
/// literal `right` value. Panics if `left` is a column but `right` isn't a
/// literal — port of the upstream `assert`.
fn extract_column(left: &ValuePosition, right: &ValuePosition) -> Option<(String, Value)> {
    let ValuePosition::Column(col) = left else {
        return None;
    };
    let ValuePosition::Literal(lit) = right else {
        panic!("extractColumn: expected right side to be literal, got {right:?}");
    };
    Some((col.name.clone(), literal_to_value(lit)))
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

    fn n(v: f64) -> Value {
        Value::Number(v)
    }

    #[test]
    fn constraint_matches_row_all_keys_must_match() {
        let c: Constraint = vec![("a".into(), n(1.0)), ("b".into(), n(2.0))];
        assert!(constraint_matches_row(
            &c,
            &row(&[("a", n(1.0)), ("b", n(2.0))])
        ));
        assert!(!constraint_matches_row(
            &c,
            &row(&[("a", n(1.0)), ("b", n(3.0))])
        ));
        assert!(!constraint_matches_row(&c, &row(&[("a", n(1.0))]))); // missing b -> null -> unequal
    }

    #[test]
    fn empty_constraint_matches_everything() {
        let c: Constraint = vec![];
        assert!(constraint_matches_row(&c, &row(&[])));
    }

    #[test]
    fn constraints_are_compatible_no_shared_keys() {
        let left: Constraint = vec![("a".into(), n(1.0))];
        let right: Constraint = vec![("b".into(), n(2.0))];
        assert!(constraints_are_compatible(&left, &right));
    }

    #[test]
    fn constraints_are_compatible_shared_key_equal_values() {
        let left: Constraint = vec![("a".into(), n(1.0))];
        let right: Constraint = vec![("a".into(), n(1.0))];
        assert!(constraints_are_compatible(&left, &right));
    }

    #[test]
    fn constraints_incompatible_shared_key_different_values() {
        let left: Constraint = vec![("a".into(), n(1.0))];
        let right: Constraint = vec![("a".into(), n(2.0))];
        assert!(!constraints_are_compatible(&left, &right));
    }

    #[test]
    fn key_matches_primary_key_order_independent() {
        let pk: PrimaryKey = vec!["a".into(), "b".into()];
        assert!(key_matches_primary_key(["b", "a"], &pk));
        assert!(!key_matches_primary_key(["a"], &pk));
        assert!(!key_matches_primary_key(["a", "b", "c"], &pk));
    }

    #[test]
    fn constraint_matches_primary_key_checks_keys_only() {
        let pk: PrimaryKey = vec!["id".into()];
        let c: Constraint = vec![("id".into(), n(1.0))];
        assert!(constraint_matches_primary_key(&c, &pk));
        let wrong: Constraint = vec![("other".into(), n(1.0))];
        assert!(!constraint_matches_primary_key(&wrong, &pk));
    }

    fn col(name: &str) -> ValuePosition {
        ValuePosition::Column(ColumnReference { name: name.into() })
    }
    fn lit_num(v: f64) -> ValuePosition {
        ValuePosition::Literal(LiteralValue::Number(v))
    }
    fn simple_eq(field: &str, v: f64) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: col(field),
            right: lit_num(v),
        }
    }

    #[test]
    fn pull_simple_and_components_flattens_and() {
        let cond = Condition::And {
            conditions: vec![simple_eq("a", 1.0), simple_eq("b", 2.0)],
        };
        let pulled = pull_simple_and_components(&cond);
        assert_eq!(pulled.len(), 2);
    }

    #[test]
    fn pull_simple_and_components_stops_at_or() {
        let cond = Condition::Or {
            conditions: vec![simple_eq("a", 1.0), simple_eq("b", 2.0)],
        };
        assert_eq!(pull_simple_and_components(&cond), vec![]);
    }

    #[test]
    fn pull_simple_and_components_unwraps_single_or() {
        let cond = Condition::Or {
            conditions: vec![simple_eq("a", 1.0)],
        };
        assert_eq!(pull_simple_and_components(&cond).len(), 1);
    }

    #[test]
    fn primary_key_constraint_from_full_equality_match() {
        let pk: PrimaryKey = vec!["a".into(), "b".into()];
        let cond = Condition::And {
            conditions: vec![simple_eq("a", 1.0), simple_eq("b", 2.0)],
        };
        let c = primary_key_constraint_from_filters(Some(&cond), &pk).unwrap();
        assert_eq!(
            c,
            vec![("a".to_string(), n(1.0)), ("b".to_string(), n(2.0))]
        );
    }

    #[test]
    fn primary_key_constraint_none_when_partial() {
        let pk: PrimaryKey = vec!["a".into(), "b".into()];
        let cond = simple_eq("a", 1.0);
        assert_eq!(primary_key_constraint_from_filters(Some(&cond), &pk), None);
    }

    #[test]
    fn primary_key_constraint_none_when_condition_missing() {
        let pk: PrimaryKey = vec!["a".into()];
        assert_eq!(primary_key_constraint_from_filters(None, &pk), None);
    }

    #[test]
    fn primary_key_constraint_ignores_non_pk_equalities() {
        let pk: PrimaryKey = vec!["a".into()];
        let cond = Condition::And {
            conditions: vec![simple_eq("a", 1.0), simple_eq("other", 9.0)],
        };
        let c = primary_key_constraint_from_filters(Some(&cond), &pk).unwrap();
        assert_eq!(c, vec![("a".to_string(), n(1.0))]);
    }

    #[test]
    #[should_panic(expected = "expected right side to be literal")]
    fn extract_column_panics_if_right_not_literal() {
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: col("a"),
            right: col("b"),
        };
        primary_key_constraint_from_filters(
            Some(&Condition::And {
                conditions: vec![cond],
            }),
            &vec!["a".into()],
        );
    }
}
