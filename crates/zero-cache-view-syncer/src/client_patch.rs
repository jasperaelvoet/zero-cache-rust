//! Port of the `Patch`/`PatchToVersion` data model from
//! `zero-cache/src/services/view-syncer/client-handler.ts`.
//!
//! These are the patches sent to a client over a poke: either a config patch
//! (query put/del, reusing [`crate::cvr_types::QueryPatch`]) or a row patch
//! carrying the row's actual contents (distinct from
//! [`crate::cvr_types::RowPatch`], which is the *persisted* CVR-internal patch
//! shape that only tracks `row_version`, not contents). Pure data model — the
//! upstream file's test exercises the stateful `ClientHandler`, not these
//! shapes in isolation, so tests here are constructional.

use zero_cache_shared::bigint_json::JsonValue;

use crate::cvr_types::{QueryPatch, RowId};
use crate::cvr_version::CvrVersion;

/// A row put patch sent to a client, carrying the row's full contents. Port
/// of `PutRowPatch` (client-handler.ts).
#[derive(Debug, Clone, PartialEq)]
pub struct ClientPutRowPatch {
    pub id: RowId,
    pub contents: Vec<(String, JsonValue)>,
}

/// A row delete patch sent to a client. Port of `DeleteRowPatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientDeleteRowPatch {
    pub id: RowId,
}

/// A row patch sent to a client: put (with contents) or delete. Port of
/// `RowPatch` (client-handler.ts; distinct from [`crate::cvr_types::RowPatch`]).
#[derive(Debug, Clone, PartialEq)]
pub enum ClientRowPatch {
    Put(ClientPutRowPatch),
    Delete(ClientDeleteRowPatch),
}

/// A config (query add/remove) patch. Port of `ConfigPatch` (an alias for the
/// del/put query patch union).
pub type ConfigPatch = QueryPatch;

/// A patch sent to a client: either a config change or a row change. Port of
/// `Patch`.
#[derive(Debug, Clone, PartialEq)]
pub enum Patch {
    Config(ConfigPatch),
    Row(ClientRowPatch),
}

/// A patch paired with the CVR version it advances the client to. Port of
/// `PatchToVersion`.
#[derive(Debug, Clone, PartialEq)]
pub struct PatchToVersion {
    pub patch: Patch,
    pub to_version: CvrVersion,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_types::PatchOp;
    use crate::cvr_version::empty_cvr_version;
    use std::collections::BTreeMap;

    fn row_id(table: &str) -> RowId {
        RowId {
            schema: "public".into(),
            table: table.into(),
            row_key: BTreeMap::from([("id".to_string(), JsonValue::String("1".into()))]),
        }
    }

    #[test]
    fn constructs_row_put_patch_with_contents() {
        let patch = Patch::Row(ClientRowPatch::Put(ClientPutRowPatch {
            id: row_id("issues"),
            contents: vec![("title".to_string(), JsonValue::String("bug".into()))],
        }));
        let pv = PatchToVersion {
            patch,
            to_version: empty_cvr_version(),
        };
        match pv.patch {
            Patch::Row(ClientRowPatch::Put(p)) => assert_eq!(p.contents.len(), 1),
            _ => panic!("expected a row put patch"),
        }
    }

    #[test]
    fn constructs_row_delete_patch_without_contents() {
        let patch = Patch::Row(ClientRowPatch::Delete(ClientDeleteRowPatch {
            id: row_id("issues"),
        }));
        assert!(matches!(patch, Patch::Row(ClientRowPatch::Delete(_))));
    }

    #[test]
    fn constructs_config_patch() {
        let patch = Patch::Config(QueryPatch {
            op: PatchOp::Del,
            id: "q1".into(),
            client_id: Some("c1".into()),
        });
        match patch {
            Patch::Config(qp) => {
                assert_eq!(qp.op, PatchOp::Del);
                assert_eq!(qp.client_id, Some("c1".to_string()));
            }
            _ => panic!("expected a config patch"),
        }
    }
}
