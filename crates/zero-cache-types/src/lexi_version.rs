//! Port of `zero-cache/src/types/lexi-version.ts`.
//!
//! A [`LexiVersion`] is a lexicographically sortable representation of numbers
//! from 0 to `Number.MAX_SAFE_INTEGER` (the safe range of Version values used
//! in Zero).
//!
//! The Version is first encoded in base36, and then prepended by a single
//! base36 character representing the length (of the base36 version) minus 1.
//! This encoding can encode numbers up to 185 bits, with the maximum encoded
//! number being `"z".repeat(37)`, or 36^36-1 (approximately 1.06e+56).
//!
//! Examples:
//! * 0 => "00"
//! * 10 => "0a"
//! * 35 => "0z"
//! * 36 => "110"
//! * 46655 => "2zzz"
//! * 2^64 => "c3w5e11264sgsg"
//!
//! Note that when using the `number` type (the [`Version::Num`] variant), the
//! functions error if attempting to encode a Version larger than
//! `Number.MAX_SAFE_INTEGER`. For large numbers, use the `bigint` variant.

use num_bigint::BigInt;
use num_traits::Signed;
use thiserror::Error;

/// A lexicographically sortable version string. In the TypeScript source this
/// is a bare `string` alias; here we keep it as [`String`] but name the type
/// for clarity at call sites.
pub type LexiVersion = String;

/// `Number.MAX_SAFE_INTEGER` from JavaScript: `2^53 - 1`.
pub const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// The input to [`version_to_lexi`], mirroring the TypeScript `number | bigint`
/// union. `Num` carries JavaScript-`number` semantics (including the
/// safe-integer assertion); `Big` carries arbitrary-precision `bigint`
/// semantics.
#[derive(Debug, Clone, PartialEq)]
pub enum Version {
    Num(f64),
    Big(BigInt),
}

impl From<f64> for Version {
    fn from(v: f64) -> Self {
        Version::Num(v)
    }
}

impl From<i32> for Version {
    fn from(v: i32) -> Self {
        Version::Num(v as f64)
    }
}

impl From<i64> for Version {
    fn from(v: i64) -> Self {
        Version::Num(v as f64)
    }
}

impl From<u64> for Version {
    fn from(v: u64) -> Self {
        Version::Num(v as f64)
    }
}

impl From<BigInt> for Version {
    fn from(v: BigInt) -> Self {
        Version::Big(v)
    }
}

/// Errors produced by the LexiVersion codec. In the TypeScript source these are
/// `assert` failures that throw; here they surface as `Result::Err`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LexiError {
    #[error("Negative versions are not supported")]
    Negative,
    #[error("Invalid or unsafe version {0}")]
    UnsafeNumber(String),
    #[error("Value is too large to be encoded as a LexiVersion: {0}")]
    TooLarge(String),
    #[error("LexiVersion must have at least 2 characters, got {0}")]
    TooShort(usize),
    #[error("Invalid LexiVersion: {0}")]
    Invalid(String),
}

/// Encodes a version number into a [`LexiVersion`].
///
/// Port of `versionToLexi`. Accepts anything convertible into [`Version`], so
/// `version_to_lexi(36)` and `version_to_lexi(some_bigint)` both work.
pub fn version_to_lexi(v: impl Into<Version>) -> Result<LexiVersion, LexiError> {
    let big: BigInt = match v.into() {
        Version::Num(n) => {
            if n < 0.0 {
                return Err(LexiError::Negative);
            }
            // typeof v === 'bigint' || (v <= MAX_SAFE_INTEGER && Number.isInteger(v))
            if !(n <= MAX_SAFE_INTEGER && n.fract() == 0.0 && n.is_finite()) {
                return Err(LexiError::UnsafeNumber(js_number_to_string(n)));
            }
            // Safe: n is a non-negative integer <= 2^53 - 1.
            BigInt::from(n as u64)
        }
        Version::Big(b) => {
            if b.is_negative() {
                return Err(LexiError::Negative);
            }
            b
        }
    };

    let base36_version = big.to_str_radix(36);
    let length = BigInt::from(base36_version.len() - 1).to_str_radix(36);
    if length.len() != 1 {
        return Err(LexiError::TooLarge(big.to_string()));
    }
    Ok(format!("{length}{base36_version}"))
}

/// Decodes a [`LexiVersion`] back into its numeric value.
///
/// Port of `versionFromLexi`. Returns a [`BigInt`] to preserve the full range.
pub fn version_from_lexi(lexi_version: &str) -> Result<BigInt, LexiError> {
    if lexi_version.len() < 2 {
        return Err(LexiError::TooShort(lexi_version.len()));
    }
    let length = &lexi_version[0..1];
    let base36_version = &lexi_version[1..];

    // parseInt(length, 36): a single base36 digit.
    let expected_len =
        parse_base36_digit(length).ok_or_else(|| LexiError::Invalid(lexi_version.to_string()))?;
    if base36_version.len() != expected_len + 1 {
        return Err(LexiError::Invalid(lexi_version.to_string()));
    }
    BigInt::parse_bytes(base36_version.as_bytes(), 36)
        .ok_or_else(|| LexiError::Invalid(lexi_version.to_string()))
}

/// Returns the lexicographically greatest of the given versions.
///
/// Port of `max`. Panics if `versions` is empty (mirroring the `AtLeastOne`
/// non-empty-tuple type in the TypeScript source).
pub fn max<'a>(versions: &'a [&'a str]) -> &'a str {
    assert!(!versions.is_empty(), "max requires at least one version");
    let mut winner = versions[0];
    for &b in &versions[1..] {
        winner = if winner > b { winner } else { b };
    }
    winner
}

/// Returns the lexicographically least of the given versions.
///
/// Port of `min`. Panics if `versions` is empty.
pub fn min<'a>(versions: &'a [&'a str]) -> &'a str {
    assert!(!versions.is_empty(), "min requires at least one version");
    let mut winner = versions[0];
    for &b in &versions[1..] {
        winner = if winner < b { winner } else { b };
    }
    winner
}

/// Parses a single base36 digit ('0'..='9', 'a'..='z', case-insensitive) into
/// its numeric value, mirroring `parseInt(char, 36)` for a one-character input.
fn parse_base36_digit(s: &str) -> Option<usize> {
    let c = s.chars().next()?;
    c.to_digit(36).map(|d| d as usize)
}

/// Renders an `f64` the way `JavaScript`'s `String(n)` / template literal would
/// for the integer-ish values that reach here, used only for error messages.
fn js_number_to_string(n: f64) -> String {
    if n.fract() == 0.0 && n.is_finite() && n.abs() < 1e21 {
        format!("{}", n as i128)
    } else {
        format!("{n}")
    }
}

/// Convenience: `versionToLexi` for a plain [`BigInt`], avoiding the `Into`
/// dance when the caller already holds one.
pub fn version_to_lexi_big(v: &BigInt) -> Result<LexiVersion, LexiError> {
    version_to_lexi(v.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;

    fn big_pow(base: u32, exp: u32) -> BigInt {
        num_traits::pow(BigInt::from(base), exp as usize)
    }

    #[test]
    fn lexi_version_encoding() {
        // (input, expected). Inputs are either a small number or a BigInt.
        let num_cases: &[(u64, &str)] = &[
            (0, "00"),
            (10, "0a"),
            (35, "0z"),
            (36, "110"),
            (46655, "2zzz"),
            (1u64 << 32, "61z141z4"),                // 2^32
            (9_007_199_254_740_991, "a2gosa7pa2gv"), // MAX_SAFE_INTEGER
        ];
        for &(num, lexi) in num_cases {
            assert_eq!(version_to_lexi(num as i64).unwrap(), lexi, "encode {num}");
            assert_eq!(
                version_from_lexi(lexi).unwrap().to_string(),
                num.to_string(),
                "decode {lexi}"
            );
        }

        let big_cases: Vec<(BigInt, String)> = vec![
            (big_pow(2, 64), "c3w5e11264sgsg".to_string()),
            (big_pow(2, 75), "e65gym2kbgwjf668".to_string()),
            (big_pow(2, 128), "of5lxx1zz5pnorynqglhzmsp34".to_string()),
            (
                big_pow(2, 160),
                "utwj4yidkw7a8pn4g709kzmfoaol3x8g".to_string(),
            ),
            (
                big_pow(2, 186),
                "zx6sp2h09v22524mnljo7dsm6cz9iehtq4xds".to_string(),
            ),
            (big_pow(36, 36) - 1, "z".repeat(37)),
        ];
        for (num, lexi) in big_cases {
            assert_eq!(version_to_lexi(num.clone()).unwrap(), lexi, "encode {num}");
            assert_eq!(
                version_from_lexi(&lexi).unwrap().to_string(),
                num.to_string(),
                "decode {lexi}"
            );
        }
    }

    #[test]
    fn min_max() {
        assert_eq!(min(&["01"]), "01");
        assert_eq!(max(&["01"]), "01");

        assert_eq!(min(&["01", "02"]), "01");
        assert_eq!(max(&["01", "02"]), "02");

        assert_eq!(min(&["01", "03", "02"]), "01");
        assert_eq!(min(&["02", "03", "01"]), "01");
        assert_eq!(max(&["01", "03", "02"]), "03");
        assert_eq!(max(&["02", "01", "03"]), "03");

        assert_eq!(min(&["04", "01", "03", "02"]), "01");
        assert_eq!(max(&["04", "01", "03", "02"]), "04");
        assert_eq!(min(&["04", "01", "03", "02", "00"]), "00");
        assert_eq!(max(&["04", "01", "03", "02", "05"]), "05");

        let array = ["02", "04", "01", "03", "02"];
        assert_eq!(min(&array), "01");
        assert_eq!(max(&array), "04");
    }

    #[test]
    fn lexi_version_sorting() {
        let v = |n: u64| version_to_lexi(n as i64).unwrap();

        assert_eq!(v(35).cmp(&v(36)), std::cmp::Ordering::Less);
        assert_eq!(v(36).cmp(&v(35)), std::cmp::Ordering::Greater);
        assert_eq!(min(&[&v(36), &v(35), &v(37)]), v(35));
        assert_eq!(max(&[&v(34), &v(36), &v(35)]), v(36));

        assert_eq!(v(1000).cmp(&v(9)), std::cmp::Ordering::Greater);
        assert_eq!(min(&[&v(1000), &v(9)]), v(9));
        assert_eq!(max(&[&v(1000), &v(9)]), v(1000));

        assert_eq!(v(89).cmp(&v(1234)), std::cmp::Ordering::Less);
        assert_eq!(min(&[&v(89), &v(1234)]), v(89));
        assert_eq!(max(&[&v(89), &v(1234)]), v(1234));

        assert_eq!(v(238).cmp(&v(238)), std::cmp::Ordering::Equal);

        // Deterministic fuzz test (xorshift64* instead of Math.random).
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            let r = state.wrapping_mul(0x2545F4914F6CDD1D);
            // Map into [0, MAX_SAFE_INTEGER].
            r % 9_007_199_254_740_992
        };
        for _ in 0..50 {
            let v1 = next();
            let v2 = next();
            let lexi_v1 = version_to_lexi(v1 as i64).unwrap();
            let lexi_v2 = version_to_lexi(v2 as i64).unwrap();

            assert_eq!(v1.cmp(&v2), lexi_v1.cmp(&lexi_v2), "cmp {v1} {v2}");
            let lo = version_to_lexi(v1.min(v2) as i64).unwrap();
            let hi = version_to_lexi(v1.max(v2) as i64).unwrap();
            assert_eq!(lo, min(&[&lexi_v1, &lexi_v2]));
            assert_eq!(lo, min(&[&lexi_v2, &lexi_v1]));
            assert_eq!(hi, max(&[&lexi_v1, &lexi_v2]));
            assert_eq!(hi, max(&[&lexi_v2, &lexi_v1]));
        }
    }

    #[test]
    fn lexi_version_encode_sanity_checks() {
        assert!(version_to_lexi(-1i64).is_err()); // negative
        assert!(version_to_lexi(0.5f64).is_err()); // decimal
        assert!(version_to_lexi(MAX_SAFE_INTEGER * 2.0).is_err()); // not safe
        assert!(version_to_lexi(big_pow(2, 187)).is_err()); // too large
    }

    #[test]
    fn lexi_version_decode_sanity_checks() {
        assert!(version_from_lexi("not a ! number").is_err());
        assert!(version_from_lexi("0").is_err()); // too short
        assert!(version_from_lexi("20").is_err()); // length too long
        assert!(version_from_lexi("3cis1k").is_err()); // length too short
    }
}
