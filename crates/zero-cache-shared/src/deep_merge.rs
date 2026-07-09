//! Port of `packages/shared/src/deep-merge.ts`, operating on
//! [`crate::bigint_json::JsonValue`].
//!
//! Deep-merges two JSON objects: keys from `b` override keys from `a`;
//! when both sides' value at a key are non-leaf (by default, "non-leaf" means
//! `Object` or `Array` — matching the JS source's `isPlainObject`, which is
//! `typeof v === 'object' && v !== null` and is therefore also true for
//! arrays), they are merged recursively.
//!
//! A JS quirk carries over faithfully: because arrays are not leaves by
//! default, merging two arrays does *not* concatenate or replace them — it
//! merges by index as if the arrays were objects keyed `"0"`, `"1"`, ... ,
//! which is what plain `for...in` iteration over a JS array produces. The
//! result is a JSON *object* with numeric string keys, not an array. Pass a
//! custom `is_leaf` that treats arrays as leaves to get array-replacement
//! semantics instead.

use crate::bigint_json::JsonValue;

/// Default leaf predicate: anything that is not an `Object` or `Array`. Port
/// of the inline default `v => !isPlainObject(v)`.
pub fn is_plain_object_like(v: &JsonValue) -> bool {
    matches!(v, JsonValue::Object(_) | JsonValue::Array(_))
}

/// Views `v` as an ordered `(key, value)` list if it is an `Object` or
/// `Array` (arrays are indexed as `"0"`, `"1"`, ...), else `None`.
fn as_entries(v: &JsonValue) -> Option<Vec<(String, JsonValue)>> {
    match v {
        JsonValue::Object(entries) => Some(entries.clone()),
        JsonValue::Array(items) => Some(
            items
                .iter()
                .enumerate()
                .map(|(i, v)| (i.to_string(), v.clone()))
                .collect(),
        ),
        _ => None,
    }
}

/// Deep-merges JSON object `a` and `b`, with `b`'s keys taking precedence.
/// `is_leaf` decides whether a value should be treated as opaque (not
/// recursed into) rather than merged; defaults to
/// [`is_plain_object_like`]'s negation via [`deep_merge`].
pub fn deep_merge_with(
    a: &[(String, JsonValue)],
    b: &[(String, JsonValue)],
    is_leaf: &impl Fn(&JsonValue) -> bool,
) -> Vec<(String, JsonValue)> {
    let mut result: Vec<(String, JsonValue)> = a.to_vec();

    for (key, b_val) in b {
        let a_val = a.iter().find(|(k, _)| k == key).map(|(_, v)| v);
        let merged = match a_val {
            Some(a_val) if !is_leaf(a_val) && !is_leaf(b_val) => {
                match (as_entries(a_val), as_entries(b_val)) {
                    (Some(ae), Some(be)) => JsonValue::Object(deep_merge_with(&ae, &be, is_leaf)),
                    _ => b_val.clone(),
                }
            }
            _ => b_val.clone(),
        };

        if let Some(slot) = result.iter_mut().find(|(k, _)| k == key) {
            slot.1 = merged;
        } else {
            result.push((key.clone(), merged));
        }
    }

    result
}

/// [`deep_merge_with`] using the default leaf predicate ([`is_plain_object_like`]
/// negated). Port of `deepMerge` with no `isLeaf` argument.
pub fn deep_merge(
    a: &[(String, JsonValue)],
    b: &[(String, JsonValue)],
) -> Vec<(String, JsonValue)> {
    deep_merge_with(a, b, &|v: &JsonValue| !is_plain_object_like(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(v: f64) -> JsonValue {
        JsonValue::Number(v)
    }
    fn s(v: &str) -> JsonValue {
        JsonValue::String(v.to_string())
    }
    fn obj(pairs: Vec<(&str, JsonValue)>) -> Vec<(String, JsonValue)> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    fn get<'a>(entries: &'a [(String, JsonValue)], key: &str) -> Option<&'a JsonValue> {
        entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    #[test]
    fn shallow_properties() {
        let a = obj(vec![("x", n(1.0)), ("y", n(2.0))]);
        let b = obj(vec![("y", n(3.0)), ("z", n(4.0))]);
        let result = deep_merge(&a, &b);
        assert_eq!(get(&result, "x"), Some(&n(1.0)));
        assert_eq!(get(&result, "y"), Some(&n(3.0)));
        assert_eq!(get(&result, "z"), Some(&n(4.0)));
    }

    #[test]
    fn nested_objects() {
        let a = obj(vec![
            (
                "user",
                JsonValue::Object(obj(vec![("name", s("Alice")), ("age", n(30.0))])),
            ),
            (
                "settings",
                JsonValue::Object(obj(vec![("theme", s("dark"))])),
            ),
        ]);
        let b = obj(vec![(
            "user",
            JsonValue::Object(obj(vec![
                ("age", n(31.0)),
                ("email", s("alice@example.com")),
            ])),
        )]);
        let result = deep_merge(&a, &b);
        let user = get(&result, "user").unwrap();
        if let JsonValue::Object(entries) = user {
            assert_eq!(get(entries, "name"), Some(&s("Alice")));
            assert_eq!(get(entries, "age"), Some(&n(31.0)));
            assert_eq!(get(entries, "email"), Some(&s("alice@example.com")));
        } else {
            panic!("expected object");
        }
        assert_eq!(get(&result, "settings"), get(&a, "settings"));
    }

    #[test]
    fn does_not_mutate_inputs() {
        let a = obj(vec![(
            "nested",
            JsonValue::Object(obj(vec![("value", n(1.0))])),
        )]);
        let b = obj(vec![(
            "nested",
            JsonValue::Object(obj(vec![("other", n(2.0))])),
        )]);
        let a_before = a.clone();
        let b_before = b.clone();
        deep_merge(&a, &b);
        assert_eq!(a, a_before);
        assert_eq!(b, b_before);
    }

    #[test]
    fn arrays_merged_by_index_by_default() {
        let a = obj(vec![(
            "arr",
            JsonValue::Array(vec![n(1.0), n(2.0), n(3.0)]),
        )]);
        let b = obj(vec![("arr", JsonValue::Array(vec![n(4.0), n(5.0)]))]);
        let result = deep_merge(&a, &b);
        let arr = get(&result, "arr").unwrap();
        if let JsonValue::Object(entries) = arr {
            assert_eq!(get(entries, "0"), Some(&n(4.0)));
            assert_eq!(get(entries, "1"), Some(&n(5.0)));
            assert_eq!(get(entries, "2"), Some(&n(3.0)));
        } else {
            panic!("expected object (JS array-as-object quirk)");
        }
    }

    #[test]
    fn arrays_replaced_with_custom_is_leaf() {
        let a = obj(vec![(
            "arr",
            JsonValue::Array(vec![n(1.0), n(2.0), n(3.0)]),
        )]);
        let b = obj(vec![("arr", JsonValue::Array(vec![n(4.0), n(5.0)]))]);
        let is_leaf = |v: &JsonValue| !matches!(v, JsonValue::Object(_));
        let result = deep_merge_with(&a, &b, &is_leaf);
        assert_eq!(
            get(&result, "arr"),
            Some(&JsonValue::Array(vec![n(4.0), n(5.0)]))
        );
    }

    #[test]
    fn handles_empty_objects() {
        assert_eq!(
            deep_merge(&[], &obj(vec![("a", n(1.0))])),
            obj(vec![("a", n(1.0))])
        );
        assert_eq!(
            deep_merge(&obj(vec![("a", n(1.0))]), &[]),
            obj(vec![("a", n(1.0))])
        );
        assert_eq!(deep_merge(&[], &[]), vec![]);
    }

    #[test]
    fn deeply_nested() {
        let a = obj(vec![(
            "a",
            JsonValue::Object(obj(vec![(
                "b",
                JsonValue::Object(obj(vec![(
                    "c",
                    JsonValue::Object(obj(vec![("d", n(1.0))])),
                )])),
            )])),
        )]);
        let b = obj(vec![(
            "a",
            JsonValue::Object(obj(vec![(
                "b",
                JsonValue::Object(obj(vec![(
                    "c",
                    JsonValue::Object(obj(vec![("e", n(2.0))])),
                )])),
            )])),
        )]);
        let result = deep_merge(&a, &b);
        let JsonValue::Object(a_) = get(&result, "a").unwrap() else {
            panic!()
        };
        let JsonValue::Object(b_) = get(a_, "b").unwrap() else {
            panic!()
        };
        let JsonValue::Object(c_) = get(b_, "c").unwrap() else {
            panic!()
        };
        assert_eq!(get(c_, "d"), Some(&n(1.0)));
        assert_eq!(get(c_, "e"), Some(&n(2.0)));
    }
}
