//! Port of the pure decision inside `ClientHandler#startPoke`
//! (view-syncer/client-handler.ts) — a `ViewSyncerService`-adjacent slice,
//! alongside `view_syncer_lifecycle.rs`/`query_set_sync.rs`/
//! `cvr_query_driven_updater.rs`/`cvr_row_received.rs`/
//! `cvr_delete_unreferenced_rows.rs`/`query_hydration.rs`. `ClientHandler`
//! itself is a per-connection object wrapping a real `Subscription<Downstream>`
//! (the WebSocket downstream) with poke-transaction bookkeeping and metrics
//! — this module ports the one genuinely pure question `startPoke` answers
//! before doing any of that real work: does this client actually need a
//! poke sent at all, or is it already caught up?
//!
//! Also ports three more pure decisions from inside the `PokeHandler`
//! `startPoke` constructs (`addPatch`/`end`'s bodies): whether a given
//! patch is stale and should be skipped (`addPatch`'s
//! `cmpVersions(toVersion, baseVersion) <= 0` guard), whether the
//! in-progress poke-part body should flush (the `PART_COUNT_FLUSH_THRESHOLD`
//! check), and `end`'s "was anything actually sent" no-op detection
//! (distinct from `should_send_poke` above — this one runs after the fact,
//! checking whether `pokeStarted` ever became true).
//!
//! Scope: NOT ported — `ClientHandler` itself (owns a live
//! `Subscription<Downstream>`, `#pokeTime`/`#pokeTransactions`/`#pokedRows`
//! metrics), the actual `PokeHandler` construction and its
//! `pokeStart`/`pokePart`/`pokeEnd` message SENDING over that live
//! downstream (this module only ports the decisions of when/whether to
//! send, not the sending), the per-patch-type body-shape assembly
//! (`desiredQueriesPatches`/`gotQueriesPatch`/`lastMutationIDChanges`/
//! `mutationsPatch`/`rowsPatch` — needs the full `PokePartBody` wire type
//! and `makeRowPatch`, a larger separate slice), and `startPoke`
//! (client-handler.ts's module-level function, not the method) — the
//! `Promise.allSettled`-based multi-client fan-out combinator, which needs
//! a real async downstream per client this port doesn't have yet.

use crate::cvr_version::{cmp_versions, CvrVersion, NullableCvrVersion};

/// Port of `startPoke`'s early-return decision:
///
/// ```text
/// const forceInitialPoke = !this.#everPoked;
/// const cmp = cmpVersions(this.#baseVersion, tentativeVersion);
/// if (cmp > 0 || (cmp === 0 && !forceInitialPoke)) {
///   return NOOP; // already caught up, don't poke
/// }
/// ```
///
/// Returns `true` if a real poke should be sent, `false` if the client is
/// already caught up and `startPoke` should return the upstream `NOOP`
/// handler instead. A client that has NEVER been poked always gets a poke
/// even if its base version already matches (`forceInitialPoke` — so it
/// learns its "got queries" state has been reconciled with the server),
/// matching upstream's documented behavior exactly.
pub fn should_send_poke(
    base_version: &NullableCvrVersion,
    tentative_version: &CvrVersion,
    ever_poked: bool,
) -> bool {
    let force_initial_poke = !ever_poked;
    let cmp = cmp_versions(base_version, &Some(tentative_version.clone()));
    !(cmp > 0 || (cmp == 0 && !force_initial_poke))
}

/// The threshold at which `addPatch` flushes the in-progress poke-part
/// body. Port of `PART_COUNT_FLUSH_THRESHOLD`.
pub const PART_COUNT_FLUSH_THRESHOLD: u32 = 100;

/// Port of `addPatch`'s staleness guard: `if (cmpVersions(toVersion,
/// this.#baseVersion) <= 0) return;` — a patch whose version is not newer
/// than what the client already has is stale (would have already been
/// caught up by a prior poke) and should be skipped entirely, never even
/// touching the in-progress poke-part body.
pub fn should_include_patch(to_version: &CvrVersion, base_version: &NullableCvrVersion) -> bool {
    cmp_versions(&Some(to_version.clone()), base_version) > 0
}

/// Port of the `if (++partCount >= PART_COUNT_FLUSH_THRESHOLD)` check —
/// `part_count` is the count AFTER incrementing for the patch just added,
/// matching upstream's pre-increment-then-compare.
pub fn should_flush_poke_part(part_count_after_increment: u32) -> bool {
    part_count_after_increment >= PART_COUNT_FLUSH_THRESHOLD
}

/// What `PokeHandler#end` should do, given whether any `pokeStart`/
/// `pokePart` was already sent this poke cycle. Port of `end`'s branching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PokeEndAction {
    /// Nothing changed and nothing was sent — `end` should return
    /// immediately without sending `pokeStart`/`pokeEnd` at all.
    Noop,
    /// No patches were added this cycle, but the client's version still
    /// needs to advance (or this is a forced initial poke) — send
    /// `pokeStart` before continuing on to flush + `pokeEnd`.
    SendPokeStartFirst,
    /// Patches were already sent (`pokeStarted` is true) — proceed
    /// directly to flush + `pokeEnd`.
    ProceedToEnd,
}

/// Port of `end`'s version-consistency invariant violation: if patches
/// were already sent this cycle, `finalVersion` MUST be strictly greater
/// than the client's base version — this is a sanity check on a bug
/// elsewhere in the CVR/poke pipeline, not a normal runtime condition.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Patches were sent but finalVersion is not greater than baseVersion")]
pub struct InvalidPokeEndVersion;

/// Port of `PokeHandler#end`'s branching logic (minus the actual
/// `pokeStart`/`flushBody`/`pokeEnd` message sends and `#baseVersion`/
/// `#everPoked` mutation, which need a live downstream — see module doc).
pub fn decide_poke_end(
    poke_started: bool,
    base_version: &NullableCvrVersion,
    final_version: &CvrVersion,
    force_initial_poke: bool,
) -> Result<PokeEndAction, InvalidPokeEndVersion> {
    if !poke_started {
        let cmp = cmp_versions(base_version, &Some(final_version.clone()));
        if cmp == 0 && !force_initial_poke {
            return Ok(PokeEndAction::Noop);
        }
        return Ok(PokeEndAction::SendPokeStartFirst);
    }

    if cmp_versions(base_version, &Some(final_version.clone())) >= 0 {
        return Err(InvalidPokeEndVersion);
    }
    Ok(PokeEndAction::ProceedToEnd)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> CvrVersion {
        CvrVersion {
            state_version: s.into(),
            config_version: None,
        }
    }

    #[test]
    fn behind_client_gets_a_poke() {
        assert!(should_send_poke(&Some(v("01")), &v("02"), true));
    }

    #[test]
    fn already_caught_up_client_gets_no_poke_after_first_poke() {
        assert!(!should_send_poke(&Some(v("02")), &v("02"), true));
    }

    #[test]
    fn never_poked_client_gets_a_poke_even_when_already_caught_up() {
        assert!(should_send_poke(&Some(v("02")), &v("02"), false));
    }

    #[test]
    fn client_ahead_of_tentative_version_gets_no_poke() {
        // Shouldn't normally happen, but matches upstream's `cmp > 0` guard
        // regardless of ever_poked.
        assert!(!should_send_poke(&Some(v("03")), &v("02"), false));
    }

    #[test]
    fn brand_new_client_with_no_base_version_gets_a_poke() {
        assert!(should_send_poke(&None, &v("01"), false));
        assert!(should_send_poke(&None, &v("01"), true));
    }

    #[test]
    fn should_include_patch_skips_stale_patches() {
        assert!(
            !should_include_patch(&v("01"), &Some(v("01"))),
            "toVersion == baseVersion is stale"
        );
        assert!(
            !should_include_patch(&v("01"), &Some(v("02"))),
            "toVersion < baseVersion is stale"
        );
    }

    #[test]
    fn should_include_patch_includes_newer_patches() {
        assert!(should_include_patch(&v("02"), &Some(v("01"))));
    }

    #[test]
    fn should_include_patch_includes_everything_when_no_base_version() {
        assert!(should_include_patch(&v("01"), &None));
    }

    #[test]
    fn should_flush_poke_part_triggers_at_threshold() {
        assert!(!should_flush_poke_part(PART_COUNT_FLUSH_THRESHOLD - 1));
        assert!(should_flush_poke_part(PART_COUNT_FLUSH_THRESHOLD));
        assert!(should_flush_poke_part(PART_COUNT_FLUSH_THRESHOLD + 1));
    }

    #[test]
    fn decide_poke_end_noop_when_nothing_started_and_nothing_changed() {
        let action = decide_poke_end(false, &Some(v("02")), &v("02"), false).unwrap();
        assert_eq!(action, PokeEndAction::Noop);
    }

    #[test]
    fn decide_poke_end_sends_start_first_when_forced_even_if_unchanged() {
        let action = decide_poke_end(false, &Some(v("02")), &v("02"), true).unwrap();
        assert_eq!(action, PokeEndAction::SendPokeStartFirst);
    }

    #[test]
    fn decide_poke_end_sends_start_first_when_version_actually_advanced() {
        let action = decide_poke_end(false, &Some(v("01")), &v("02"), false).unwrap();
        assert_eq!(action, PokeEndAction::SendPokeStartFirst);
    }

    #[test]
    fn decide_poke_end_proceeds_when_already_started_and_version_advanced() {
        let action = decide_poke_end(true, &Some(v("01")), &v("02"), false).unwrap();
        assert_eq!(action, PokeEndAction::ProceedToEnd);
    }

    #[test]
    fn decide_poke_end_errors_on_the_sanity_check_violation() {
        let err = decide_poke_end(true, &Some(v("02")), &v("02"), false).unwrap_err();
        assert_eq!(err, InvalidPokeEndVersion);
    }
}
