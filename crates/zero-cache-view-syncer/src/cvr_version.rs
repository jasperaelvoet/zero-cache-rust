//! Port of the `CVRVersion` portion of
//! `zero-cache/src/services/view-syncer/schema/types.ts`.
//!
//! A CVR (Client View Record) version identifies a point in the CVR's history:
//! the upstream `stateVersion` it is consistent with, plus a `configVersion`
//! sub-counter for configuration-only changes (query set / transformation
//! changes) that don't advance the state version. Versions serialize to
//! "cookie" strings sent to clients.

use thiserror::Error;
use zero_cache_types::lexi_version::{version_from_lexi, version_to_lexi};
use zero_cache_types::state_version::state_version_from_string;

/// A CVR version: the upstream state version plus an optional config
/// sub-version. Port of `CVRVersion`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CvrVersion {
    /// LexiVersion string.
    pub state_version: String,
    pub config_version: Option<i64>,
}

/// `CVRVersion | null`. Port of `NullableCVRVersion`.
pub type NullableCvrVersion = Option<CvrVersion>;

/// The version of a freshly-initialized, empty CVR. Port of
/// `EMPTY_CVR_VERSION`.
pub fn empty_cvr_version() -> CvrVersion {
    CvrVersion {
        state_version: major_version_to_string_0(),
        config_version: None,
    }
}

fn major_version_to_string_0() -> String {
    version_to_lexi(0i64).expect("0 always encodes")
}

/// Returns the version immediately following `v` (bumping `configVersion`, or
/// starting a fresh CVR at state version 0 if `v` is `None`). Port of
/// `oneAfter`.
pub fn one_after(v: &NullableCvrVersion) -> CvrVersion {
    match v {
        None => CvrVersion {
            state_version: major_version_to_string_0(),
            config_version: None,
        },
        Some(v) => CvrVersion {
            state_version: v.state_version.clone(),
            config_version: Some(v.config_version.unwrap_or(0) + 1),
        },
    }
}

/// Compares two (possibly absent) versions: `None < Some`, then by
/// `stateVersion` (lexicographic), then by `configVersion`. Port of
/// `cmpVersions`.
pub fn cmp_versions(a: &NullableCvrVersion, b: &NullableCvrVersion) -> i64 {
    match (a, b) {
        (None, None) => 0,
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (Some(a), Some(b)) => {
            if a.state_version < b.state_version {
                -1
            } else if a.state_version > b.state_version {
                1
            } else {
                a.config_version.unwrap_or(0) - b.config_version.unwrap_or(0)
            }
        }
    }
}

/// Returns the greater of `a` and `b` (`b` defaulting to `a` if absent). Port
/// of `maxVersion`.
pub fn max_version(a: &CvrVersion, b: Option<&CvrVersion>) -> CvrVersion {
    match b {
        None => a.clone(),
        Some(b) => {
            if cmp_versions(&Some(b.clone()), &Some(a.clone())) > 0 {
                b.clone()
            } else {
                a.clone()
            }
        }
    }
}

/// Errors from cookie/version conversion.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum VersionError {
    #[error("Invalid cookie: {0}")]
    InvalidCookie(String),
    #[error("minorVersion {0} exceeds max safe integer")]
    ConfigVersionOverflow(String),
}

/// Serializes a version to its "cookie" string. The `:` separator is chosen to
/// sort lexicographically after `/` (the storage key path separator), so that
/// e.g. `"01/row-hash"` sorts before `"01:01/row-hash"`. Port of
/// `versionString`/`versionToCookie`.
pub fn version_to_cookie(v: &CvrVersion) -> Result<String, VersionError> {
    match v.config_version {
        None | Some(0) => Ok(v.state_version.clone()),
        Some(cv) => {
            let lexi =
                version_to_lexi(cv).map_err(|e| VersionError::InvalidCookie(e.to_string()))?;
            Ok(format!("{}:{lexi}", v.state_version))
        }
    }
}

/// [`version_to_cookie`] over a [`NullableCvrVersion`]. Port of
/// `versionToNullableCookie`.
pub fn version_to_nullable_cookie(v: &NullableCvrVersion) -> Result<Option<String>, VersionError> {
    match v {
        None => Ok(None),
        Some(v) => version_to_cookie(v).map(Some),
    }
}

/// Parses a cookie string into a version. Port of `versionFromString`.
pub fn version_from_string(str: &str) -> Result<CvrVersion, VersionError> {
    let parts: Vec<&str> = str.split(':').collect();
    match parts.len() {
        1 => {
            let state_version = parts[0].to_string();
            // Purely for validation, matching the TS source.
            state_version_from_string(&state_version)
                .map_err(|_| VersionError::InvalidCookie(str.to_string()))?;
            Ok(CvrVersion {
                state_version,
                config_version: None,
            })
        }
        2 => {
            let state_version = parts[0].to_string();
            let config_version = version_from_lexi(parts[1])
                .map_err(|_| VersionError::InvalidCookie(str.to_string()))?;
            if config_version > num_bigint::BigInt::from(9_007_199_254_740_991i64) {
                return Err(VersionError::ConfigVersionOverflow(parts[1].to_string()));
            }
            // Fits: checked above.
            let cv: i64 = config_version.to_string().parse().unwrap();
            Ok(CvrVersion {
                state_version,
                config_version: Some(cv),
            })
        }
        _ => Err(VersionError::InvalidCookie(str.to_string())),
    }
}

/// [`version_from_string`] over an optional cookie. Port of `cookieToVersion`.
pub fn cookie_to_version(cookie: Option<&str>) -> Result<NullableCvrVersion, VersionError> {
    match cookie {
        None => Ok(None),
        Some(c) => version_from_string(c).map(Some),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(state: &str) -> CvrVersion {
        CvrVersion {
            state_version: state.into(),
            config_version: None,
        }
    }
    fn vc(state: &str, config: i64) -> CvrVersion {
        CvrVersion {
            state_version: state.into(),
            config_version: Some(config),
        }
    }

    #[test]
    fn version_comparison() {
        assert!(cmp_versions(&Some(vc("02", 1)), &Some(vc("01", 2))) > 0);
        assert!(cmp_versions(&Some(vc("01", 2)), &Some(vc("02", 1))) < 0);
        assert!(cmp_versions(&Some(vc("02", 1)), &Some(vc("02", 2))) < 0);
        assert!(cmp_versions(&Some(vc("02", 2)), &Some(vc("02", 1))) > 0);
        assert!(cmp_versions(&Some(v("02")), &Some(vc("02", 1))) < 0);
        assert!(cmp_versions(&Some(vc("02", 1)), &Some(v("02"))) > 0);
        assert_eq!(cmp_versions(&Some(vc("02", 2)), &Some(vc("02", 2))), 0);
        assert_eq!(cmp_versions(&None, &None), 0);
        assert!(cmp_versions(&None, &Some(v("00"))) < 0);
        assert!(cmp_versions(&Some(v("00")), &None) > 0);
    }

    #[test]
    fn cookie_ordering_matches_cmp_versions() {
        // `build_poke` selects the final version via `max_by_key(cookie)`
        // (lexicographic string order), which is only correct because
        // `version_to_cookie` is order-consistent with `cmp_versions`. Pin that
        // load-bearing invariant, including the LexiVersion length-prefix case
        // (config 9 < 10, where naive "9" > "10" would break it).
        let ascending = [
            v("01"),     // config 0
            vc("01", 1), // config bump, same state
            vc("01", 9),
            vc("01", 10), // 10 must sort after 9 (LexiVersion)
            v("02"),      // higher state version dominates config
            vc("02", 1),
        ];
        for pair in ascending.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            assert!(
                cmp_versions(&Some(a.clone()), &Some(b.clone())) < 0,
                "cmp_versions should order {a:?} before {b:?}"
            );
            assert!(
                version_to_cookie(a).unwrap() < version_to_cookie(b).unwrap(),
                "cookie order must match: {:?} < {:?}",
                version_to_cookie(a),
                version_to_cookie(b),
            );
        }
    }

    #[test]
    fn cookie_version_round_trip() {
        let cases: Vec<(Option<&str>, NullableCvrVersion)> = vec![
            (None, None),
            (Some("00"), Some(v("00"))),
            (Some("2abc"), Some(v("2abc"))),
            (Some("00:01"), Some(vc("00", 1))),
            (Some("100:0a"), Some(vc("100", 10))),
            (Some("a128adk2f9s:110"), Some(vc("a128adk2f9s", 36))),
        ];
        for (cookie, version) in cases {
            assert_eq!(cookie_to_version(cookie).unwrap(), version, "{cookie:?}");
            assert_eq!(
                version_to_nullable_cookie(&version).unwrap(),
                cookie.map(|s| s.to_string()),
                "{version:?}"
            );
        }
    }

    #[test]
    fn invalid_cookies() {
        assert!(cookie_to_version(Some("foo-bar")).is_err());
        assert!(cookie_to_version(Some("1:2:3")).is_err());
        assert!(cookie_to_version(Some("110:93jlxpt2ps")).is_err());
    }

    #[test]
    fn one_after_cases() {
        assert_eq!(one_after(&Some(v("00"))), vc("00", 1));
        assert_eq!(one_after(&Some(v("2abc"))), vc("2abc", 1));
        assert_eq!(one_after(&Some(vc("00", 1))), vc("00", 2));
        assert_eq!(one_after(&Some(vc("100", 10))), vc("100", 11));
        assert_eq!(
            one_after(&Some(vc("a128adk2f9s", 36))),
            vc("a128adk2f9s", 37)
        );
    }
}
