//! Port of `zero-cache/src/types/row-key.ts`.
//!
//! Normalized, column-order-agnostic representations of row keys and row ids,
//! used as map keys and (via [`row_id_hash`]) as compact CVR identifiers.

use thiserror::Error;
use zero_cache_shared::bigint_json::{stringify, JsonValue};
use zero_cache_shared::hash::h128;

/// A row key: column name -> value. Order-independent; callers may build it in
/// any order, and normalization sorts by column name.
///
/// In the TS source this is `Record<string, JSONValue>`; here it is an ordered
/// list of `(column, value)` pairs so insertion order is observable (matching
/// JavaScript object semantics) prior to normalization.
pub type RowKey = Vec<(String, JsonValue)>;

/// A fully-qualified row identifier. Port of `RowID`.
#[derive(Debug, Clone, PartialEq)]
pub struct RowId {
    pub schema: String,
    pub table: String,
    pub row_key: RowKey,
}

impl RowId {
    pub fn new(schema: impl Into<String>, table: impl Into<String>, row_key: RowKey) -> Self {
        RowId {
            schema: schema.into(),
            table: table.into(),
            row_key,
        }
    }
}

/// Errors from row-key normalization.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RowKeyError {
    #[error("empty row key")]
    Empty,
}

/// Returns the entries of `row_key` in sorted (normalized) column order.
///
/// Port of `normalizedKeyOrder`. The TS version returns the input unchanged
/// when already sorted (an allocation optimization); here we always return a
/// sorted copy, which is observationally equivalent. Errors on an empty key.
pub fn normalized_key_order(row_key: &[(String, JsonValue)]) -> Result<RowKey, RowKeyError> {
    if row_key.is_empty() {
        return Err(RowKeyError::Empty);
    }
    let mut entries = row_key.to_vec();
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));
    Ok(entries)
}

/// The `[k0, v0, k1, v1, ...]` tuple list of a normalized row key.
fn tuples(key: &RowKey) -> Result<Vec<JsonValue>, RowKeyError> {
    let ordered = normalized_key_order(key)?;
    let mut out = Vec::with_capacity(ordered.len() * 2);
    for (k, v) in ordered {
        out.push(JsonValue::String(k));
        out.push(v);
    }
    Ok(out)
}

/// A normalized string representation of a row key, suitable as a map key.
/// Port of `rowKeyString`.
pub fn row_key_string(key: &RowKey) -> Result<String, RowKeyError> {
    Ok(stringify(&JsonValue::Array(tuples(key)?)))
}

/// A normalized string representation of a [`RowId`], suitable as a map key.
/// Port of `rowIDString`.
pub fn row_id_string(id: &RowId) -> Result<String, RowKeyError> {
    let mut arr = Vec::with_capacity(2 + id.row_key.len() * 2);
    arr.push(JsonValue::String(id.schema.clone()));
    arr.push(JsonValue::String(id.table.clone()));
    arr.extend(tuples(&id.row_key)?);
    Ok(stringify(&JsonValue::Array(arr)))
}

/// A 128-bit, column-order-agnostic hash of a [`RowId`], base36-encoded (max 25
/// characters). Port of `rowIDHash`.
pub fn row_id_hash(id: &RowId) -> Result<String, RowKeyError> {
    let s = row_id_string(id)?;
    Ok(u128_to_base36(h128(&s)))
}

/// Encodes a `u128` in base36 with no leading zeros, matching
/// `BigInt.prototype.toString(36)`.
fn u128_to_base36(mut n: u128) -> String {
    if n == 0 {
        return "0".to_string();
    }
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = Vec::new();
    while n > 0 {
        buf.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;

    fn s(v: &str) -> JsonValue {
        JsonValue::String(v.to_string())
    }
    fn arr(v: &[&str]) -> JsonValue {
        JsonValue::Array(v.iter().map(|x| s(x)).collect())
    }
    fn num(n: i64) -> JsonValue {
        JsonValue::Number(n as f64)
    }
    fn key(pairs: &[(&str, JsonValue)]) -> RowKey {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    struct Case {
        schema: &'static str,
        table: &'static str,
        keys: Vec<RowKey>,
        row_key_string: &'static str,
        row_id_string: &'static str,
        row_id_hash: &'static str,
    }

    #[test]
    fn row_key_cases() {
        let cases = vec![
            Case {
                schema: "public",
                table: "issue",
                keys: vec![key(&[("foo", s("bar"))]), key(&[("foo", s("bar"))])],
                row_key_string: r#"["foo","bar"]"#,
                row_id_string: r#"["public","issue","foo","bar"]"#,
                row_id_hash: "ciol231ukcwkot147odcn45m0",
            },
            Case {
                schema: "public",
                table: "clients",
                keys: vec![key(&[("foo", s("bar"))]), key(&[("foo", s("bar"))])],
                row_key_string: r#"["foo","bar"]"#,
                row_id_string: r#"["public","clients","foo","bar"]"#,
                row_id_hash: "64611vx2jblwgdkqghzcfnbhm",
            },
            Case {
                schema: "zero",
                table: "clients",
                keys: vec![key(&[("foo", s("bar"))]), key(&[("foo", s("bar"))])],
                row_key_string: r#"["foo","bar"]"#,
                row_id_string: r#"["zero","clients","foo","bar"]"#,
                row_id_hash: "d5ylu9yny0atlxwv84ckob3iq",
            },
            Case {
                schema: "clients",
                table: "zero",
                keys: vec![key(&[("foo", s("bar"))]), key(&[("foo", s("bar"))])],
                row_key_string: r#"["foo","bar"]"#,
                row_id_string: r#"["clients","zero","foo","bar"]"#,
                row_id_hash: "46fn166ycpx29z47xjh8mcqxp",
            },
            Case {
                schema: "public",
                table: "issues",
                keys: vec![
                    key(&[("foo", arr(&["bar"]))]),
                    key(&[("foo", arr(&["bar"]))]),
                ],
                row_key_string: r#"["foo",["bar"]]"#,
                row_id_string: r#"["public","issues","foo",["bar"]]"#,
                row_id_hash: "9q3o77bjorgu22uheyyr3yyh2",
            },
            Case {
                schema: "public",
                table: "issue",
                keys: vec![key(&[("foo", num(1))]), key(&[("foo", num(1))])],
                row_key_string: r#"["foo",1]"#,
                row_id_string: r#"["public","issue","foo",1]"#,
                row_id_hash: "cy4p72xet3a20cgyrdj1c81ak",
            },
            Case {
                schema: "public",
                table: "issue",
                keys: vec![key(&[("foo", s("1"))]), key(&[("foo", s("1"))])],
                row_key_string: r#"["foo","1"]"#,
                row_id_string: r#"["public","issue","foo","1"]"#,
                row_id_hash: "5ejr02sz9n3l7zpt82rr8mh7c",
            },
            Case {
                schema: "public",
                table: "issue",
                keys: vec![
                    key(&[("foo", s("bar")), ("bar", arr(&["foo"]))]),
                    key(&[("bar", arr(&["foo"])), ("foo", s("bar"))]),
                ],
                row_key_string: r#"["bar",["foo"],"foo","bar"]"#,
                row_id_string: r#"["public","issue","bar",["foo"],"foo","bar"]"#,
                row_id_hash: "5h887x9fpyacg9dsk8ld9w6qf",
            },
            Case {
                schema: "public",
                table: "issue",
                keys: vec![
                    key(&[("foo", s("bar")), ("bar", arr(&["foo"])), ("baz", num(2))]),
                    key(&[("baz", num(2)), ("foo", s("bar")), ("bar", arr(&["foo"]))]),
                    key(&[("bar", arr(&["foo"])), ("foo", s("bar")), ("baz", num(2))]),
                ],
                row_key_string: r#"["bar",["foo"],"baz",2,"foo","bar"]"#,
                row_id_string: r#"["public","issue","bar",["foo"],"baz",2,"foo","bar"]"#,
                row_id_hash: "3qflvcrevxjynhsqs07r27cik",
            },
            Case {
                schema: "public",
                table: "issue",
                keys: vec![key(&[("id", s("HhCx1Vi3js"))])],
                row_key_string: r#"["id","HhCx1Vi3js"]"#,
                row_id_string: r#"["public","issue","id","HhCx1Vi3js"]"#,
                row_id_hash: "6si0q0rmq27la39k5mhtl9420",
            },
        ];

        for c in &cases {
            for k in &c.keys {
                assert_eq!(row_key_string(k).unwrap(), c.row_key_string);
                let id = RowId::new(c.schema, c.table, k.clone());
                assert_eq!(row_id_string(&id).unwrap(), c.row_id_string);
                assert_eq!(row_id_hash(&id).unwrap(), c.row_id_hash);
            }
        }
    }

    #[test]
    fn normalized_key_order_behavior() {
        let sorted = key(&[("a", num(3)), ("b", num(2)), ("c", num(1))]);
        let not_sorted = vec![
            key(&[("a", num(3)), ("c", num(1)), ("b", num(2))]),
            key(&[("b", num(2)), ("a", num(3)), ("c", num(1))]),
            key(&[("b", num(2)), ("c", num(1)), ("a", num(3))]),
            key(&[("c", num(1)), ("b", num(2)), ("a", num(3))]),
            key(&[("c", num(1)), ("a", num(3)), ("b", num(2))]),
        ];

        let keys_of = |k: &RowKey| -> Vec<String> {
            normalized_key_order(k)
                .unwrap()
                .into_iter()
                .map(|(k, _)| k)
                .collect()
        };

        assert_eq!(keys_of(&sorted), vec!["a", "b", "c"]);
        for ns in &not_sorted {
            assert_eq!(keys_of(ns), vec!["a", "b", "c"]);
        }

        assert_eq!(normalized_key_order(&[]), Err(RowKeyError::Empty));
    }

    // Guard: base36 of a BigInt matches our u128 encoder for a known value.
    #[test]
    fn base36_matches_bigint() {
        let n: u128 = 0xdead_beef_1234_5678_9abc_def0_1122_3344;
        let expected = BigInt::from(n).to_str_radix(36);
        assert_eq!(u128_to_base36(n), expected);
    }
}
