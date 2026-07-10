//! Port of `packages/shared/src/float-to-ordered-string.ts`.
//!
//! Encodes an `f64` as a 13-character base36 string such that lexicographic
//! string ordering matches numeric ordering (including `NaN`/`Infinity`/`-0`
//! per IEEE-754 total-order-like placement). Used for keys in ordered indexes
//! (e.g. SQLite) where only string comparison is available.

/// Encodes `n` as an order-preserving 13-character base36 string. Port of
/// `encodeFloat64AsString`.
pub fn encode_float64_as_string(n: f64) -> String {
    let bits = n.to_bits();
    let mut high = (bits >> 32) as u32;
    let mut low = bits as u32;

    // Flip the sign bit so positive numbers sort before negative. For
    // negatives, flip all bits so larger-magnitude values sort smaller.
    if n < 0.0 || n.is_sign_negative() && n == 0.0 {
        high ^= 0xffff_ffff;
        low ^= 0xffff_ffff;
    } else {
        high ^= 1 << 31;
    }

    let combined = ((high as u64) << 32) | (low as u64);
    format!("{combined:0>13}", combined = to_base36(combined))
}

/// Decodes a string produced by [`encode_float64_as_string`] back to its
/// original `f64`. Port of `decodeFloat64AsString`. Panics if `s` is not
/// exactly 13 characters (matching the TS `assert`).
pub fn decode_float64_as_string(s: &str) -> f64 {
    assert!(s.len() == 13, "Invalid encoded float64: {s}");
    let combined = from_base36(s);
    let mut high = (combined >> 32) as u32;
    let low = combined as u32;
    let sign = high >> 31;

    let low = if sign != 0 {
        high ^= 1 << 31;
        low
    } else {
        high ^= 0xffff_ffff;
        low ^ 0xffff_ffff
    };

    let bits = ((high as u64) << 32) | (low as u64);
    f64::from_bits(bits)
}

fn to_base36(mut n: u64) -> String {
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

fn from_base36(s: &str) -> u64 {
    let mut result: u64 = 0;
    for c in s.chars() {
        result = result * 36 + c.to_digit(36).unwrap() as u64;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const CASES: &[(f64, &str)] = &[
        (-0.0, "1y2p0ij32e8e7"),
        (0.0, "1y2p0ij32e8e8"),
        (1.0, "2x2t6dniqybcw"),
        (2.0, "2x41irsmllclc"),
        (3.0, "2x4noyv6iwv7k"),
        (4.0, "2x59v5xqg8dts"),
        (-1.0, "0z2kunendu5fj"),
        (-2.0, "0z1ci99jj7473"),
        (-3.0, "0z0qc26zlvlkv"),
        (-4.0, "0z045v4fok2yn"),
        (std::f64::consts::PI, "2x4qtzjh93rx4"),
        (f64::NAN, "3w4rutzm7gy68"),
        (f64::INFINITY, "3w45omx2a5fk0"),
        (f64::NEG_INFINITY, "0018ce53un18f"),
        (9_007_199_254_740_991.0, "2yw3f766uv4sf"), // MAX_SAFE_INTEGER
        (-9_007_199_254_740_991.0, "0x9altvz9xc00"), // MIN_SAFE_INTEGER
        (5e-324, "1y2p0ij32e8e9"),                  // MIN_VALUE (smallest positive)
        (1.7976931348623157e308, "3w45omx2a5fjz"),  // MAX_VALUE
    ];

    #[test]
    fn encode_matches_known_vectors() {
        for &(n, expected) in CASES {
            assert_eq!(encode_float64_as_string(n), expected, "encode {n}");
        }
    }

    #[test]
    fn decode_matches_known_vectors() {
        for &(n, s) in CASES {
            let decoded = decode_float64_as_string(s);
            if n.is_nan() {
                assert!(decoded.is_nan(), "decode {s}");
            } else {
                assert_eq!(decoded.to_bits(), n.to_bits(), "decode {s}");
            }
        }
    }

    #[test]
    fn ordering_is_preserved() {
        // Pairwise: string order must match numeric order for non-NaN values.
        let mut vals: Vec<f64> = CASES
            .iter()
            .map(|&(n, _)| n)
            .filter(|n| !n.is_nan())
            .collect();
        vals.sort_by(|a, b| a.total_cmp(b));
        let encoded: Vec<String> = vals.iter().map(|&n| encode_float64_as_string(n)).collect();
        let mut sorted_encoded = encoded.clone();
        sorted_encoded.sort();
        assert_eq!(encoded, sorted_encoded);
    }

    #[test]
    #[should_panic(expected = "Invalid encoded float64")]
    fn decode_rejects_wrong_length() {
        decode_float64_as_string("short");
    }

    #[test]
    fn round_trips_arbitrary_values() {
        let samples: &[f64] = &[
            0.1,
            -0.1,
            123456.789,
            -123456.789,
            1e100,
            -1e100,
            1e-100,
            f64::MIN,
            f64::MAX,
            f64::MIN_POSITIVE,
        ];
        for &n in samples {
            let s = encode_float64_as_string(n);
            assert_eq!(s.len(), 13);
            assert_eq!(decode_float64_as_string(&s).to_bits(), n.to_bits(), "{n}");
        }
    }
}
