//! Port of `zero-cache/src/services/change-source/pg/lsn.ts`.
//!
//! Parsing and conversion for the Postgres `pg_lsn` type — the 64-bit "Log
//! Sequence Number" used as the monotonic progress marker for logical
//! replication. In the wire format it is two hex numbers (up to 8 digits each)
//! separated by a slash; it is converted to a [`LexiVersion`] and used as the
//! DB-agnostic version throughout the sync replica.
//!
//! [`LexiVersion`]: crate::lexi_version::LexiVersion

use num_bigint::BigInt;
use thiserror::Error;

use crate::lexi_version::Version;
use crate::state_version::{major_version_to_string, state_version_from_string};

/// A Postgres LSN string, e.g. `"16/B374D848"`.
pub type Lsn = String;

/// Errors from LSN parsing.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum LsnError {
    #[error("Malformed LSN: \"{0}\"")]
    Malformed(String),
}

/// Parses an LSN into its 64-bit value. Port of `toBigInt`.
pub fn to_bigint(lsn: &str) -> Result<BigInt, LsnError> {
    let parts: Vec<&str> = lsn.split('/').collect();
    if parts.len() != 2 {
        return Err(LsnError::Malformed(lsn.to_string()));
    }
    let high = BigInt::parse_bytes(parts[0].as_bytes(), 16)
        .ok_or_else(|| LsnError::Malformed(lsn.to_string()))?;
    let low = BigInt::parse_bytes(parts[1].as_bytes(), 16)
        .ok_or_else(|| LsnError::Malformed(lsn.to_string()))?;
    Ok((high << 32u32) + low)
}

/// Converts an LSN to its (major) state-version string. Port of
/// `toStateVersionString`.
pub fn to_state_version_string(lsn: &str) -> Result<String, LsnError> {
    let val = to_bigint(lsn)?;
    // `major_version_to_string` only fails on values too large to encode, which
    // cannot happen for a 64-bit LSN.
    Ok(major_version_to_string(Version::Big(val)).expect("64-bit LSN always encodes"))
}

/// Extracts the LSN from a state-version string (tracked by the `major`
/// component). Port of `fromStateVersionString`.
pub fn from_state_version_string(ver: &str) -> Result<Lsn, LsnError> {
    let sv = state_version_from_string(ver).map_err(|_| LsnError::Malformed(ver.to_string()))?;
    Ok(from_bigint(&sv.major))
}

/// Formats a 64-bit value as an LSN string (uppercase hex). Port of `fromBigInt`.
pub fn from_bigint(val: &BigInt) -> Lsn {
    let high = val >> 32u32;
    let low = val & BigInt::from(0xffff_ffffu64);
    format!(
        "{}/{}",
        high.to_str_radix(16).to_uppercase(),
        low.to_str_radix(16).to_uppercase()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn big_pow(base: u32, exp: u32) -> BigInt {
        num_traits::pow(BigInt::from(base), exp as usize)
    }

    #[test]
    fn lsn_conversions() {
        // (lsn, lexiVersion, value)
        let cases: Vec<(&str, &str, BigInt)> = vec![
            ("0/0", "00", BigInt::from(0)),
            ("0/0", "00.01", BigInt::from(0)),
            ("0/A", "0a", BigInt::from(10)),
            ("16/B374D848", "718sh0nk8", BigInt::from(97_500_059_720i64)),
            (
                "16/B374D848",
                "718sh0nk8.123",
                BigInt::from(97_500_059_720i64),
            ),
            ("FFFFFFFF/FFFFFFFF", "c3w5e11264sgsf", big_pow(2, 64) - 1),
        ];

        for (lsn, str, ver) in cases {
            let major = if let Some(dot) = str.find('.') {
                &str[..dot]
            } else {
                str
            };
            assert_eq!(
                to_state_version_string(lsn).unwrap(),
                major,
                "toStateVersion {lsn}"
            );
            assert_eq!(to_bigint(lsn).unwrap(), ver, "toBigInt {lsn}");
            assert_eq!(from_bigint(&ver), lsn, "fromBigInt {ver}");
            assert_eq!(
                from_state_version_string(str).unwrap(),
                lsn,
                "fromStateVersion {str}"
            );
        }
    }

    #[test]
    fn malformed_lsn() {
        assert_eq!(to_bigint("nope"), Err(LsnError::Malformed("nope".into())));
        assert!(to_bigint("1/2/3").is_err());
    }
}
