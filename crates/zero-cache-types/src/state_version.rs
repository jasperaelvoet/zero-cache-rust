//! Port of `zero-cache/src/types/state-version.ts`.
//!
//! Identifies the version of the data on the replica, corresponding to the
//! stream of changes produced by the change-source and change-streamer.
//!
//! The `major` version directly tracks the watermark of the replication stream
//! (e.g. the Postgres LSN). The optional `minor` version is used for auxiliary
//! state changes, such as writes from pending backfills.
//!
//! StateVersions are persisted and compared as lexicographically ordered
//! strings, using the [`crate::lexi_version`] format for major and minor,
//! separated by a dot. With no minor version, only the major LexiVersion is
//! emitted.

use num_bigint::BigInt;

use crate::lexi_version::{version_from_lexi, version_to_lexi, LexiError, Version};

/// A parsed state version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateVersion {
    pub major: BigInt,
    pub minor: Option<BigInt>,
}

impl StateVersion {
    /// Constructs a major-only state version.
    pub fn major(major: impl Into<BigInt>) -> Self {
        StateVersion {
            major: major.into(),
            minor: None,
        }
    }

    /// Constructs a major+minor state version.
    pub fn with_minor(major: impl Into<BigInt>, minor: impl Into<BigInt>) -> Self {
        StateVersion {
            major: major.into(),
            minor: Some(minor.into()),
        }
    }
}

/// Parses a state version string. Port of `stateVersionFromString`.
pub fn state_version_from_string(ver: &str) -> Result<StateVersion, LexiError> {
    if !ver.contains('.') {
        return Ok(StateVersion {
            major: version_from_lexi(ver)?,
            minor: None,
        });
    }
    let parts: Vec<&str> = ver.split('.').collect();
    if parts.len() != 2 {
        return Err(LexiError::Invalid(format!("Invalid stateVersion {ver}")));
    }
    Ok(StateVersion {
        major: version_from_lexi(parts[0])?,
        minor: Some(version_from_lexi(parts[1])?),
    })
}

/// Serializes a state version. Port of `stateVersionToString`.
pub fn state_version_to_string(ver: &StateVersion) -> Result<String, LexiError> {
    match &ver.minor {
        None => version_to_lexi(Version::Big(ver.major.clone())),
        Some(minor) => Ok(format!(
            "{}.{}",
            version_to_lexi(Version::Big(ver.major.clone()))?,
            version_to_lexi(Version::Big(minor.clone()))?
        )),
    }
}

/// Extracts just the major version from a state version string. Port of
/// `majorVersionFromString`.
pub fn major_version_from_string(ver: &str) -> Result<BigInt, LexiError> {
    if !ver.contains('.') {
        return version_from_lexi(ver);
    }
    Ok(state_version_from_string(ver)?.major)
}

/// Serializes a major version. Port of `majorVersionToString`.
pub fn major_version_to_string(major: impl Into<Version>) -> Result<String, LexiError> {
    version_to_lexi(major)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_version_roundtrip() {
        let cases: Vec<(&str, StateVersion)> = vec![
            ("00", StateVersion::major(0)),
            ("01", StateVersion::major(1)),
            ("123", StateVersion::major(75)),
            ("01.00", StateVersion::with_minor(1, 0)),
            ("01.123", StateVersion::with_minor(1, 75)),
        ];

        for (str, ver) in &cases {
            assert_eq!(&state_version_from_string(str).unwrap(), ver);
            assert_eq!(&state_version_to_string(ver).unwrap(), str);
            assert_eq!(major_version_from_string(str).unwrap(), ver.major);

            match &ver.minor {
                None => assert_eq!(
                    &major_version_to_string(Version::Big(ver.major.clone())).unwrap(),
                    str
                ),
                Some(_) => {
                    let prefix =
                        major_version_to_string(Version::Big(ver.major.clone())).unwrap() + ".";
                    assert!(str.starts_with(&prefix));
                }
            }
        }
    }

    #[test]
    fn sorting() {
        let vers = [
            StateVersion::major(75),
            StateVersion::with_minor(75, 1),
            StateVersion::with_minor(23, 100),
            StateVersion::with_minor(12, 1001),
            StateVersion::with_minor(23, 101),
            StateVersion::with_minor(12, 1),
            StateVersion::with_minor(12, 0),
            StateVersion::major(12),
        ];

        let mut strs: Vec<String> = vers
            .iter()
            .map(|v| state_version_to_string(v).unwrap())
            .collect();
        strs.sort();
        let parsed: Vec<StateVersion> = strs
            .iter()
            .map(|s| state_version_from_string(s).unwrap())
            .collect();

        assert_eq!(
            parsed,
            vec![
                StateVersion::major(12),
                StateVersion::with_minor(12, 0),
                StateVersion::with_minor(12, 1),
                StateVersion::with_minor(12, 1001),
                StateVersion::with_minor(23, 100),
                StateVersion::with_minor(23, 101),
                StateVersion::major(75),
                StateVersion::with_minor(75, 1),
            ]
        );
    }
}
