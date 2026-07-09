//! Port of `zero-config.ts`'s `isAdminPasswordValid` — the admin-password
//! check gating access to zero-cache's admin endpoints.
//!
//! `lc.warn?.(...)`/`lc.debug?.(...)` calls are left to the caller (this
//! port has no `LogContext`, matching `ttl.rs`'s established pattern): the
//! [`Outcome`] returned here tells the caller which message to log, if any,
//! rather than logging itself. Upstream's own `warnOnce` (a module-level
//! `hasWarned` flag ensuring the "no admin password set" warning fires only
//! once per process) is threaded through as a `warned_once: &mut bool`
//! parameter instead of a hidden `static mut`, matching this port's
//! determinism convention of taking ambient/mutable process state
//! explicitly rather than reading it implicitly.
//!
//! `timingSafeEqual` (Node's `crypto.timingSafeEqual`) has no equivalent
//! dependency in this port yet, so the constant-time byte comparison is
//! hand-rolled (`ct_eq`): XOR every byte pair and OR the results together,
//! never early-returning on a mismatch, matching the same threat model
//! (prevents an attacker from timing byte-by-byte comparison to guess the
//! password) as the length-mismatch dummy-comparison branch below.

/// What the caller should do/log after a password check. Port of the
/// `lc.warn?.(...)`/`lc.debug?.(...)` side effects, deferred to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Valid: no admin password configured, but development mode allows it
    /// through. Caller should warn (only once per process — see
    /// `warned_once`).
    ValidDevelopmentModeNoPassword,
    /// Valid: password matched.
    ValidPasswordAccepted,
    /// Invalid: no admin password is configured at all (and not in
    /// development mode, or a password was supplied anyway).
    InvalidNoAdminPasswordConfigured,
    /// Invalid: a password was supplied but didn't match.
    InvalidPasswordMismatch,
}

impl Outcome {
    pub fn is_valid(self) -> bool {
        matches!(
            self,
            Outcome::ValidDevelopmentModeNoPassword | Outcome::ValidPasswordAccepted
        )
    }
}

/// Port of `isAdminPasswordValid`. `warned_once` is upstream's module-level
/// `hasWarned` (see module doc) — pass the same `&mut bool` across calls
/// within a process to reproduce the "warn only once" behavior; a fresh
/// `false` each call reproduces "warn every time" (`resetWarnOnceState`'s
/// effect, useful in tests).
pub fn is_admin_password_valid(
    admin_password: Option<&str>,
    password: Option<&str>,
    is_development_mode: bool,
    warned_once: &mut bool,
) -> Outcome {
    if password.is_none() && admin_password.is_none() && is_development_mode {
        if !*warned_once {
            *warned_once = true;
        }
        return Outcome::ValidDevelopmentModeNoPassword;
    }

    let Some(admin_password) = admin_password else {
        return Outcome::InvalidNoAdminPasswordConfigured;
    };

    let password = password.unwrap_or("");
    if !ct_eq(password.as_bytes(), admin_password.as_bytes()) {
        return Outcome::InvalidPasswordMismatch;
    }

    Outcome::ValidPasswordAccepted
}

/// Constant-time byte-slice comparison. Port of the `timingSafeEqual`
/// usage's semantics (see module doc) — always compares every byte,
/// regardless of length or an early mismatch, so equal-length comparisons
/// take the same time whether or not they match. A length mismatch is
/// reported (there's no way to hide the length itself without padding,
/// same limitation upstream has — its length-mismatch branch runs a dummy
/// `timingSafeEqual(configBuffer, configBuffer)` purely to burn equivalent
/// time, then returns `false`; that dummy comparison isn't needed here
/// since this fn already always walks the longer buffer).
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let len_eq = a.len() == b.len();
    let n = a.len().max(b.len());
    let mut diff: u8 = if len_eq { 0 } else { 1 };
    for i in 0..n {
        let av = a.get(i).copied().unwrap_or(0);
        let bv = b.get(i).copied().unwrap_or(0);
        diff |= av ^ bv;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_password_configured_in_development_mode_is_valid() {
        let mut warned = false;
        let outcome = is_admin_password_valid(None, None, true, &mut warned);
        assert_eq!(outcome, Outcome::ValidDevelopmentModeNoPassword);
        assert!(outcome.is_valid());
        assert!(warned, "should have flagged the caller to warn");
    }

    #[test]
    fn no_password_configured_outside_development_mode_is_invalid() {
        let mut warned = false;
        let outcome = is_admin_password_valid(None, None, false, &mut warned);
        assert_eq!(outcome, Outcome::InvalidNoAdminPasswordConfigured);
        assert!(!outcome.is_valid());
    }

    #[test]
    fn supplying_a_password_when_none_is_configured_is_invalid_even_in_development() {
        // Matches upstream: the dev-mode bypass only applies when BOTH
        // `password` and `config.adminPassword` are absent.
        let mut warned = false;
        let outcome = is_admin_password_valid(None, Some("guess"), true, &mut warned);
        assert_eq!(outcome, Outcome::InvalidNoAdminPasswordConfigured);
    }

    #[test]
    fn matching_password_is_valid() {
        let mut warned = false;
        let outcome = is_admin_password_valid(Some("secret"), Some("secret"), false, &mut warned);
        assert_eq!(outcome, Outcome::ValidPasswordAccepted);
        assert!(outcome.is_valid());
    }

    #[test]
    fn mismatched_password_is_invalid() {
        let mut warned = false;
        let outcome = is_admin_password_valid(Some("secret"), Some("wrong"), false, &mut warned);
        assert_eq!(outcome, Outcome::InvalidPasswordMismatch);
    }

    #[test]
    fn missing_password_against_a_configured_one_is_invalid() {
        let mut warned = false;
        let outcome = is_admin_password_valid(Some("secret"), None, false, &mut warned);
        assert_eq!(outcome, Outcome::InvalidPasswordMismatch);
    }

    #[test]
    fn different_length_passwords_are_invalid() {
        let mut warned = false;
        let outcome = is_admin_password_valid(Some("secret"), Some("s"), false, &mut warned);
        assert_eq!(outcome, Outcome::InvalidPasswordMismatch);
    }

    #[test]
    fn ct_eq_matches_naive_equality_for_various_inputs() {
        assert!(ct_eq(b"", b""));
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
        assert!(!ct_eq(b"ab", b"abc"));
    }

    #[test]
    fn warned_once_flag_is_caller_controlled_not_reset_automatically() {
        // Reproduces `warnOnce`'s "only warn the first time" behavior when
        // the SAME flag is threaded across calls...
        let mut warned = false;
        is_admin_password_valid(None, None, true, &mut warned);
        assert!(warned);
        // ...and `resetWarnOnceState`'s effect when the caller passes a
        // fresh `false` instead.
        let mut fresh = false;
        let outcome = is_admin_password_valid(None, None, true, &mut fresh);
        assert_eq!(outcome, Outcome::ValidDevelopmentModeNoPassword);
    }
}
