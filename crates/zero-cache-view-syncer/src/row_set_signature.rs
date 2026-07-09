//! Port of `zero-cache/src/services/view-syncer/row-set-signature.ts`.
//!
//! A query's "row set signature" is the XOR of a per-row unit hash over all
//! rows in its result. The unit hash covers schema + table + row key, so the
//! same row key in a different table hashes differently. Signatures are stored
//! as hex; the empty/absent signature is the identity `0`.

use zero_cache_shared::hash::h64;
use zero_cache_types::row_key::{row_id_string, RowId, RowKeyError};

/// The unit hash XOR'd into a query's row-set signature for `id`. Port of
/// `rowIDSignatureUnit`.
pub fn row_id_signature_unit(id: &RowId) -> Result<u64, RowKeyError> {
    Ok(h64(&row_id_string(id)?))
}

/// Parses a hex-encoded signature. Empty/absent is the identity `0`. Port of
/// `parseSignature`.
pub fn parse_signature(hex: Option<&str>) -> u64 {
    match hex {
        None | Some("") => 0,
        Some(h) => u64::from_str_radix(h, 16).unwrap_or(0),
    }
}

/// Serializes a signature to lowercase hex (`0` -> `"0"`). Port of
/// `formatSignature`.
pub fn format_signature(sig: u64) -> String {
    format!("{sig:x}")
}

/// WIRING: port of the row-set-signature drift check inline in
/// `ViewSyncerService`'s hydration path (view-syncer.ts, ~line 1598) —
/// composes [`parse_signature`] (ported, but never consumed anywhere in
/// this port until now) with the actual comparison decision. Compares the
/// signature stored in the CVR for a query (`stored_sig_hex`, `None` if
/// absent) against the just-computed candidate signature from a fresh
/// hydration pass (`candidate_sig` — what `PipelineDriver::rowSetSignature`
/// would return). A mismatch means the query re-executed to a different
/// row set at the same DB state — only possible for a query using the
/// `Cap`/`LIMIT` operator, which can non-deterministically pick a
/// different N-row subset — and the caller should drop the query's
/// pipeline for full re-execution (the actual `removeQuery`/pipeline
/// mutation is the caller's job, not modeled here).
///
/// Absent stored signatures are NOT drift (`Some(false)` is never
/// returned for that case — legacy queries from before this feature
/// existed, or a query that hasn't completed a hydration cycle since,
/// have nothing to compare against): returns `None` to distinguish "no
/// signature to compare" from "compared and matched", exactly matching
/// upstream's `storedSigHex !== undefined && storedSigHex !== null` guard
/// which skips the whole check rather than treating absence as either
/// outcome.
pub fn detect_row_set_signature_drift(
    stored_sig_hex: Option<&str>,
    candidate_sig: u64,
) -> Option<bool> {
    let stored_sig_hex = stored_sig_hex?;
    let prior_sig = parse_signature(Some(stored_sig_hex));
    Some(prior_sig != candidate_sig)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_shared::bigint_json::JsonValue;

    fn row_id(table: &str, id: &str) -> RowId {
        RowId::new(
            "public",
            table,
            vec![("id".to_string(), JsonValue::String(id.to_string()))],
        )
    }

    #[test]
    fn format_signature_hex() {
        assert_eq!(format_signature(0), "0");
        assert_eq!(format_signature(0xabcd), "abcd");
    }

    #[test]
    fn parse_round_trips() {
        for v in [0u64, 1, 0xdeadbeef, 0xffffffffffffffff] {
            assert_eq!(parse_signature(Some(&format_signature(v))), v);
        }
    }

    #[test]
    fn parse_empty_is_zero() {
        assert_eq!(parse_signature(None), 0);
        assert_eq!(parse_signature(Some("")), 0);
    }

    #[test]
    fn signature_unit_stable_and_distinct() {
        let a = row_id_signature_unit(&row_id("issues", "1")).unwrap();
        let a2 = row_id_signature_unit(&row_id("issues", "1")).unwrap();
        let b = row_id_signature_unit(&row_id("issues", "2")).unwrap();
        let c = row_id_signature_unit(&row_id("users", "1")).unwrap();
        assert_eq!(a, a2);
        assert_ne!(a, b);
        // Same row key, different table -> different hash.
        assert_ne!(a, c);
    }

    #[test]
    fn drift_is_none_when_nothing_stored_to_compare_against() {
        assert_eq!(detect_row_set_signature_drift(None, 42), None);
    }

    #[test]
    fn drift_is_false_when_signatures_match() {
        let hex = format_signature(42);
        assert_eq!(detect_row_set_signature_drift(Some(&hex), 42), Some(false));
    }

    #[test]
    fn drift_is_true_when_signatures_differ() {
        let hex = format_signature(42);
        assert_eq!(detect_row_set_signature_drift(Some(&hex), 99), Some(true));
    }

    #[test]
    fn empty_stored_signature_is_treated_as_zero_not_absent() {
        // Matches upstream's `!== undefined && !== null` guard: an empty
        // string still triggers a comparison (against 0), it's only a
        // genuinely absent (None) signature that's skipped.
        assert_eq!(detect_row_set_signature_drift(Some(""), 0), Some(false));
        assert_eq!(detect_row_set_signature_drift(Some(""), 7), Some(true));
    }
}
