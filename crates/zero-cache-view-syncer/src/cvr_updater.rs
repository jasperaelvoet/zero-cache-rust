//! Port of the version-transition invariants from `CVRUpdater` in
//! `zero-cache/src/services/view-syncer/cvr.ts`.
//!
//! `CVRUpdater` itself is a stateful class tightly coupled to `CVRStore` (a
//! Postgres-backed store not yet ported); this module extracts its pure
//! version-bumping logic — `_setVersion`/`_ensureNewVersion` — as free
//! functions so the core CVR invariant ("version strictly increases, bumping
//! is idempotent within a flush cycle") is portable and testable in isolation.

use crate::cvr_version::{cmp_versions, one_after, CvrVersion};

/// Sets `current` to `new_version`, enforcing that it strictly increases.
/// Port of `CVRUpdater._setVersion`. Panics if `new_version` does not exceed
/// `current` (matching the TS `assert`).
pub fn set_version(current: &mut CvrVersion, new_version: CvrVersion) {
    assert!(
        cmp_versions(&Some(current.clone()), &Some(new_version.clone())) < 0,
        "Expected new version to be greater than current version"
    );
    *current = new_version;
}

/// Ensures `current` has a higher version than `orig`, bumping it via
/// [`one_after`] if they are still equal. Idempotent: repeated calls without an
/// intervening version bump elsewhere return the same version. Port of
/// `CVRUpdater._ensureNewVersion`.
pub fn ensure_new_version(orig: &CvrVersion, current: &mut CvrVersion) -> CvrVersion {
    if cmp_versions(&Some(orig.clone()), &Some(current.clone())) == 0 {
        set_version(current, one_after(&Some(current.clone())));
    }
    current.clone()
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
    fn set_version_advances() {
        let mut current = v("01");
        set_version(&mut current, v("02"));
        assert_eq!(current, v("02"));
    }

    #[test]
    #[should_panic(expected = "Expected new version to be greater")]
    fn set_version_rejects_non_increasing() {
        let mut current = v("02");
        set_version(&mut current, v("01"));
    }

    #[test]
    #[should_panic(expected = "Expected new version to be greater")]
    fn set_version_rejects_equal() {
        let mut current = v("02");
        set_version(&mut current, v("02"));
    }

    #[test]
    fn ensure_new_version_bumps_when_unchanged() {
        let orig = v("01");
        let mut current = v("01");
        let result = ensure_new_version(&orig, &mut current);
        assert_eq!(result, vc("01", 1));
        assert_eq!(current, vc("01", 1));
    }

    #[test]
    fn ensure_new_version_is_idempotent() {
        let orig = v("01");
        let mut current = v("01");
        let first = ensure_new_version(&orig, &mut current);
        let second = ensure_new_version(&orig, &mut current);
        assert_eq!(first, second);
    }

    #[test]
    fn ensure_new_version_noop_when_already_bumped() {
        let orig = v("01");
        let mut current = v("02"); // already advanced by other means
        let result = ensure_new_version(&orig, &mut current);
        assert_eq!(result, v("02"));
    }
}
