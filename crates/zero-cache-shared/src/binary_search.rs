//! Port of `packages/shared/src/binary-search.ts`.

/// Returns the index of the first element in `0..high` for which `compare`
/// returns `<= 0` (i.e. the first index whose "target minus element" is
/// non-positive) — the standard lower-bound binary search.
///
/// Typical usage: `compare(i) = needle - haystack[i]`.
pub fn binary_search(high: usize, compare: impl Fn(usize) -> f64) -> usize {
    let mut low = 0usize;
    let mut high = high;
    while low < high {
        let mid = low + (high - low) / 2;
        let i = compare(mid);
        if i == 0.0 {
            return mid;
        }
        if i > 0.0 {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_search_cases() {
        let t = |needle: f64, haystack: &[f64], expected: usize| {
            assert_eq!(
                binary_search(haystack.len(), |i| needle - haystack[i]),
                expected,
                "needle={needle} haystack={haystack:?}"
            );
        };

        t(0.0, &[], 0);

        t(-1.0, &[0.0], 0);
        t(0.0, &[0.0], 0);
        t(1.0, &[0.0], 1);

        t(-1.0, &[0.0, 1.0], 0);
        t(0.0, &[0.0, 1.0], 0);
        t(0.5, &[0.0, 1.0], 1);
        t(1.0, &[0.0, 1.0], 1);
        t(2.0, &[0.0, 1.0], 2);

        t(-1.0, &[0.0, 1.0, 2.0], 0);
        t(0.0, &[0.0, 1.0, 2.0], 0);
        t(0.5, &[0.0, 1.0, 2.0], 1);
        t(1.0, &[0.0, 1.0, 2.0], 1);
        t(1.5, &[0.0, 1.0, 2.0], 2);
        t(2.0, &[0.0, 1.0, 2.0], 2);
        t(3.0, &[0.0, 1.0, 2.0], 3);

        t(-1.0, &[0.0, 1.0, 2.0, 3.0], 0);
        t(0.0, &[0.0, 1.0, 2.0, 3.0], 0);
        t(0.5, &[0.0, 1.0, 2.0, 3.0], 1);
        t(1.0, &[0.0, 1.0, 2.0, 3.0], 1);
        t(1.5, &[0.0, 1.0, 2.0, 3.0], 2);
        t(2.0, &[0.0, 1.0, 2.0, 3.0], 2);
        t(2.5, &[0.0, 1.0, 2.0, 3.0], 3);
        t(3.0, &[0.0, 1.0, 2.0, 3.0], 3);
        t(4.0, &[0.0, 1.0, 2.0, 3.0], 4);
    }
}
