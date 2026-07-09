//! Port of `zql/src/ivm/data.ts`.
//!
//! The row/value vocabulary shared by every IVM pipeline operator:
//! `Value` comparison (`compareValues`), row comparators built from an
//! `Ordering` (`makeComparator`), and value equality for joins
//! (`valuesEqual`). This is the pure, generator-free foundation the rest of
//! `ivm` builds on.
//!
//! Scope note: this is the first slice of `zql/src/ivm` — it does not yet
//! include `Node`/`Stream`/`Operator`/`Source` (the lazy-generator-driven
//! pipeline machinery). Those model JS generator `'yield'` cooperative
//! scheduling, which has no direct Rust equivalent and needs a deliberate
//! design decision (e.g. an explicit iterator/coroutine shape) before
//! porting — tracked as the next increment in `PORTING.md`.

use zero_cache_protocol::ast::{Direction, Ordering};
use zero_cache_shared::bigint_json::JsonValue;

/// A single cell value flowing through IVM. Reuses [`JsonValue`] (the same
/// value vocabulary as `change-source::data::Row`) rather than a narrower
/// type: upstream's `Value` is `JSON | undefined`, and `JsonValue` already
/// models everything JSON-representable; `undefined` collapses to
/// `JsonValue::Null` via [`normalize_undefined`], matching upstream's
/// `?? null` treatment of the two as equivalent.
pub type Value = JsonValue;

/// A row: column name -> value. Same shape as `change-source::data::Row`,
/// defined locally to keep `zero-cache-zql` from depending on
/// `zero-cache-change-source` for a type alias.
pub type Row = Vec<(String, Value)>;

/// Looks up a column's value in `row`, treating a missing column the same as
/// an explicit `null` (mirrors how a real `Row` object with an optional
/// field looks under JS's `row[field]` access).
fn get(row: &Row, field: &str) -> Value {
    row.iter()
        .find(|(k, _)| k == field)
        .map(|(_, v)| v.clone())
        .unwrap_or(Value::Null)
}

/// Compares two values. Requires `a` and `b` to be the same "kind" (both
/// numeric, both string, both bool) once null is excluded — mixed-type
/// comparison is a logic error upstream deliberately surfaces by panicking,
/// so this does too. Port of `compareValues`.
///
/// `null` sorts before every other value and is equal to itself (unlike
/// [`values_equal`], which treats null as unequal to itself — the two
/// functions have deliberately different null semantics upstream, for
/// ordering vs. join-key equality respectively).
pub fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering as Ord;
    use JsonValue::*;

    match (a, b) {
        (Null, Null) => Ord::Equal,
        (Null, _) => Ord::Less,
        (_, Null) => Ord::Greater,
        (String(x), String(y)) => x.as_str().cmp(y.as_str()),
        (Number(x), Number(y)) => x.partial_cmp(y).unwrap_or(Ord::Equal),
        (Bool(x), Bool(y)) => x.cmp(y),
        _ => panic!("Cannot compare values of different types: {a:?} and {b:?}"),
    }
}

/// A row comparator built from a query's `Ordering`. Port of
/// `makeComparator`.
pub fn make_comparator(
    order: &Ordering,
    reverse: bool,
) -> impl Fn(&Row, &Row) -> std::cmp::Ordering + '_ {
    move |a, b| {
        for (field, direction) in order {
            let comp = compare_values(&get(a, field), &get(b, field));
            if comp != std::cmp::Ordering::Equal {
                let result = if *direction == Direction::Asc {
                    comp
                } else {
                    comp.reverse()
                };
                return if reverse { result.reverse() } else { result };
            }
        }
        std::cmp::Ordering::Equal
    }
}

/// Whether two values are equal for join-key purposes. Unlike
/// [`compare_values`], `null` is unequal to itself here — required for joins
/// to behave like SQL (`NULL != NULL`). Port of `valuesEqual`.
pub fn values_equal(a: &Value, b: &Value) -> bool {
    if matches!(a, JsonValue::Null) || matches!(b, JsonValue::Null) {
        return false;
    }
    a == b
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Value {
        Value::String(v.to_string())
    }
    fn n(v: f64) -> Value {
        Value::Number(v)
    }
    fn b(v: bool) -> Value {
        Value::Bool(v)
    }

    #[test]
    fn null_is_equal_to_null_and_less_than_everything() {
        assert_eq!(
            compare_values(&Value::Null, &Value::Null),
            std::cmp::Ordering::Equal
        );
        for v in [s("x"), n(0.0), b(false)] {
            assert_eq!(compare_values(&Value::Null, &v), std::cmp::Ordering::Less);
            assert_eq!(
                compare_values(&v, &Value::Null),
                std::cmp::Ordering::Greater
            );
        }
    }

    #[test]
    fn boolean_ordering_false_lt_true() {
        assert_eq!(
            compare_values(&b(false), &b(true)),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_values(&b(true), &b(true)),
            std::cmp::Ordering::Equal
        );
    }

    #[test]
    fn numeric_ordering() {
        assert_eq!(compare_values(&n(1.0), &n(2.0)), std::cmp::Ordering::Less);
        assert_eq!(
            compare_values(&n(2.0), &n(1.0)),
            std::cmp::Ordering::Greater
        );
        assert_eq!(compare_values(&n(1.0), &n(1.0)), std::cmp::Ordering::Equal);
    }

    #[test]
    fn string_ordering_is_byte_order() {
        assert_eq!(compare_values(&s("a"), &s("b")), std::cmp::Ordering::Less);
    }

    #[test]
    #[should_panic(expected = "Cannot compare values of different types")]
    fn mixed_type_comparison_panics() {
        compare_values(&s("x"), &n(1.0));
    }

    #[test]
    fn make_comparator_orders_by_first_differing_field() {
        let order: Ordering = vec![("a".into(), Direction::Asc), ("b".into(), Direction::Desc)];
        let cmp = make_comparator(&order, false);
        let r1: Row = vec![("a".into(), n(1.0)), ("b".into(), n(1.0))];
        let r2: Row = vec![("a".into(), n(1.0)), ("b".into(), n(2.0))];
        // a ties, b differs: desc means higher b sorts first (Less).
        assert_eq!(cmp(&r1, &r2), std::cmp::Ordering::Greater);
        assert_eq!(cmp(&r2, &r1), std::cmp::Ordering::Less);
    }

    #[test]
    fn make_comparator_reverse_flips_everything() {
        let order: Ordering = vec![("a".into(), Direction::Asc)];
        let cmp = make_comparator(&order, true);
        let r1: Row = vec![("a".into(), n(1.0))];
        let r2: Row = vec![("a".into(), n(2.0))];
        assert_eq!(cmp(&r1, &r2), std::cmp::Ordering::Greater);
    }

    #[test]
    fn missing_field_treated_as_null() {
        let order: Ordering = vec![("missing".into(), Direction::Asc)];
        let cmp = make_comparator(&order, false);
        let r1: Row = vec![];
        let r2: Row = vec![("missing".into(), n(1.0))];
        assert_eq!(cmp(&r1, &r2), std::cmp::Ordering::Less);
    }

    #[test]
    fn values_equal_treats_null_as_unequal_to_itself() {
        assert!(!values_equal(&Value::Null, &Value::Null));
        assert!(!values_equal(&Value::Null, &n(1.0)));
        assert!(values_equal(&n(1.0), &n(1.0)));
        assert!(!values_equal(&n(1.0), &n(2.0)));
    }
}
