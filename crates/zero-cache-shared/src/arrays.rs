//! Port of `packages/shared/src/arrays.ts`.

use std::collections::HashMap;
use std::hash::Hash;

/// Returns the elements of `arr` that are `Some`, dropping `None`s. Port of
/// `defined`.
///
/// The TS version avoids copying when nothing needs filtering; Rust's
/// `into_iter().flatten()` already avoids unnecessary allocation churn, so
/// this is a straightforward filter-map.
pub fn defined<T>(arr: Vec<Option<T>>) -> Vec<T> {
    arr.into_iter().flatten().collect()
}

/// Element-wise equality of two slices. Port of `areEqual`.
pub fn are_equal<T: PartialEq>(a: &[T], b: &[T]) -> bool {
    a == b
}

/// Pairs up two equal-length slices. Port of `zip`. Panics if lengths differ
/// (matching the TS `assert`).
pub fn zip<T1: Clone, T2: Clone>(a1: &[T1], a2: &[T2]) -> Vec<(T1, T2)> {
    assert!(a1.len() == a2.len(), "zip: arrays must have equal length");
    a1.iter().cloned().zip(a2.iter().cloned()).collect()
}

/// The last element, if any. Port of `last`.
pub fn last<T: Clone>(arr: &[T]) -> Option<T> {
    arr.last().cloned()
}

/// Groups elements by a computed key, preserving encounter order within each
/// group and insertion order of first-seen keys. Port of `groupBy`.
pub fn group_by<T: Clone, K: Eq + Hash + Clone>(
    arr: &[T],
    key_fn: impl Fn(&T) -> K,
) -> Vec<(K, Vec<T>)> {
    let mut order: Vec<K> = Vec::new();
    let mut groups: HashMap<K, Vec<T>> = HashMap::new();
    for el in arr {
        let key = key_fn(el);
        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        groups.entry(key).or_default().push(el.clone());
    }
    order
        .into_iter()
        .map(|k| {
            let v = groups.remove(&k).unwrap();
            (k, v)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defined_cases() {
        let cases: Vec<(Vec<Option<i32>>, Vec<i32>)> = vec![
            (vec![], vec![]),
            (vec![None], vec![]),
            (vec![None, None], vec![]),
            (vec![Some(0), None], vec![0]),
            (vec![None, Some(0)], vec![0]),
            (vec![None, Some(0), None], vec![0]),
            (vec![None, Some(0), Some(1)], vec![0, 1]),
            (vec![Some(0), None, Some(1)], vec![0, 1]),
            (vec![Some(0), None, Some(0), Some(1)], vec![0, 0, 1]),
            (
                vec![
                    Some(2),
                    Some(1),
                    Some(0),
                    None,
                    Some(0),
                    None,
                    Some(1),
                    None,
                    Some(2),
                ],
                vec![2, 1, 0, 0, 1, 2],
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(defined(input), expected);
        }
    }

    #[test]
    fn zip_cases() {
        assert_eq!(zip::<i32, i32>(&[], &[]), vec![]);
        assert_eq!(
            zip(&[1, 2, 3], &["a", "b", "c"]),
            vec![(1, "a"), (2, "b"), (3, "c")]
        );
        assert_eq!(zip(&[1, 1, 1], &[2, 2, 2]), vec![(1, 2), (1, 2), (1, 2)]);
    }

    #[test]
    #[should_panic]
    fn zip_panics_on_unequal_length() {
        zip(&[1, 2], &[1]);
    }

    #[test]
    fn group_by_cases() {
        let empty: Vec<i32> = vec![];
        assert_eq!(group_by(&empty, |x| *x), vec![]);

        let input = [1, 2, 1, 3, 2, 1];
        let result = group_by(&input, |x| *x);
        assert_eq!(result.len(), 3);
        let find = |k: i32| {
            result
                .iter()
                .find(|(key, _)| *key == k)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(find(1), Some(vec![1, 1, 1]));
        assert_eq!(find(2), Some(vec![2, 2]));
        assert_eq!(find(3), Some(vec![3]));

        let input = [1, 2, 3, 4, 5, 6];
        let result = group_by(&input, |x| if x % 2 == 0 { "even" } else { "odd" });
        let find = |k: &str| {
            result
                .iter()
                .find(|(key, _)| *key == k)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(find("even"), Some(vec![2, 4, 6]));
        assert_eq!(find("odd"), Some(vec![1, 3, 5]));
    }

    #[test]
    fn group_by_different_key_types() {
        let input = ["a", "bb", "ccc", "d", "ee"];
        let result = group_by(&input, |x| x.len());
        let find = |k: usize| {
            result
                .iter()
                .find(|(key, _)| *key == k)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(find(1), Some(vec!["a", "d"]));
        assert_eq!(find(2), Some(vec!["bb", "ee"]));
        assert_eq!(find(3), Some(vec!["ccc"]));
    }
}
