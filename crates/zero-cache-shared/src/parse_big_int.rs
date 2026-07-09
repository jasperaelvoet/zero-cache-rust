//! Port of `packages/shared/src/parse-big-int.ts`.

use num_bigint::BigInt;

/// Parses `val` as a base-`radix` integer into a [`BigInt`].
///
/// Returns `None` if any character is not a valid digit in `radix` (the TS
/// version relies on `parseInt` returning `NaN`, which here we surface as
/// `None`). Mirrors the digit-by-digit accumulation of the original.
pub fn parse_big_int(val: &str, radix: u32) -> Option<BigInt> {
    let base = BigInt::from(radix);
    let mut result = BigInt::from(0);
    for ch in val.chars() {
        let digit = ch.to_digit(radix)?;
        result *= &base;
        result += BigInt::from(digit);
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_base36() {
        assert_eq!(parse_big_int("zz", 36).unwrap(), BigInt::from(35 * 36 + 35));
        assert_eq!(parse_big_int("10", 36).unwrap(), BigInt::from(36));
        assert_eq!(parse_big_int("0", 10).unwrap(), BigInt::from(0));
    }

    #[test]
    fn rejects_invalid_digits() {
        assert!(parse_big_int("1!2", 10).is_none());
    }
}
