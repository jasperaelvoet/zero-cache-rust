//! Port of `zero-protocol/src/poke.ts`.
//!
//! Pokes use a multi-part format: entity data can be multiple megabytes, so
//! it's split across `poke-start` (announces the version range being
//! updated to), zero or more `poke-part` messages (patch fragments, merged
//! in receipt order), and `poke-end` (finalizes, or cancels, the poke). All
//! messages for one poke share a `poke_id`; pokes for different ids are
//! never interleaved.

use std::collections::BTreeMap;

use crate::mutations_patch::MutationsPatch;
use crate::queries_patch::QueriesPatch;
use crate::row_patch::RowsPatch;
use crate::version::{NullableVersion, Version};

/// The schema-version range a poke's data is valid for. Port of the inline
/// `schemaVersions` object in `pokeStartBodySchema`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SchemaVersions {
    pub min_supported_version: f64,
    pub max_supported_version: f64,
}

/// Port of `pokeStartBodySchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct PokeStartBody {
    pub poke_id: String,
    /// Always a `Version`, except a client's very first poke, which updates
    /// from the null initial cookie.
    pub base_cookie: NullableVersion,
    /// Set whenever this poke will contain a `rows_patch` (absent for
    /// patches that only update clients/queries).
    pub schema_versions: Option<SchemaVersions>,
    pub timestamp: Option<f64>,
}

/// Port of `pokePartBodySchema`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PokePartBody {
    pub poke_id: String,
    /// Changes to last-mutation-id, by client id.
    pub last_mutation_id_changes: Option<BTreeMap<String, f64>>,
    /// Patches to the desired query sets, by client id.
    pub desired_queries_patches: Option<BTreeMap<String, QueriesPatch>>,
    /// Patches to the set of queries whose entities are sync'd in
    /// `rows_patch`.
    pub got_queries_patch: Option<QueriesPatch>,
    pub rows_patch: Option<RowsPatch>,
    pub mutations_patch: Option<MutationsPatch>,
}

/// Port of `pokeEndBodySchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct PokeEndBody {
    pub poke_id: String,
    /// Ignored (and may be a placeholder) if `cancel` is `true`.
    pub cookie: Version,
    /// If `true`, discard this `poke_id`'s accumulated patch without
    /// applying it.
    pub cancel: Option<bool>,
}

/// Port of `Downstream`'s three poke message variants
/// (`PokeStartMessage`/`PokePartMessage`/`PokeEndMessage`), collapsed into
/// one enum here rather than three separate tuple-tagged types, since Rust
/// enums already carry their own discriminant.
#[derive(Debug, Clone, PartialEq)]
pub enum PokeMessage {
    Start(PokeStartBody),
    Part(PokePartBody),
    End(PokeEndBody),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poke_part_body_default_has_no_patches() {
        let body = PokePartBody {
            poke_id: "p1".into(),
            ..Default::default()
        };
        assert!(body.rows_patch.is_none());
        assert!(body.mutations_patch.is_none());
    }

    #[test]
    fn poke_start_body_allows_null_base_cookie_for_first_poke() {
        let body = PokeStartBody {
            poke_id: "p1".into(),
            base_cookie: None,
            schema_versions: None,
            timestamp: None,
        };
        assert_eq!(body.base_cookie, None);
    }

    #[test]
    fn poke_end_body_cancel_defaults_absent() {
        let body = PokeEndBody {
            poke_id: "p1".into(),
            cookie: "01".into(),
            cancel: None,
        };
        assert_eq!(body.cancel, None);
    }
}
