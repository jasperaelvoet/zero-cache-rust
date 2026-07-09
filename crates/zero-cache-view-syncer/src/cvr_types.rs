//! Port of the record/patch data model in
//! `zero-cache/src/services/view-syncer/schema/types.ts` (the portion beyond
//! [`crate::cvr_version`]).
//!
//! These are the CVR's persisted record shapes: per-client state, per-query
//! records (client/custom/internal), row records, and the put/del patches used
//! to communicate incremental CVR changes to clients. Pure data model — the
//! upstream file has no dedicated test beyond the version logic already ported
//! in `cvr_version.rs`.

use std::collections::BTreeMap;

use zero_cache_protocol::ast::Ast;
use zero_cache_shared::bigint_json::JsonValue;

use crate::cvr_version::CvrVersion;

/// A monotonic clock value used for query TTL bookkeeping (branded `number` in
/// the TS source; a plain newtype here since Rust doesn't need the phantom-tag
/// trick to prevent mixing with other numbers at the type level... but we keep
/// the wrapper for parity and to prevent accidental arithmetic with unrelated
/// values). Port of `TTLClock`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct TtlClock(pub f64);

impl TtlClock {
    pub fn as_number(self) -> f64 {
        self.0
    }
    pub fn from_number(n: f64) -> Self {
        TtlClock(n)
    }
}

/// A CVR ID. Port of `CvrID`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CvrId {
    pub id: String,
}

/// Fields common to all CVR records: the version at which the record was last
/// patched in. Port of the internal `cvrRecordSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct CvrRecordBase {
    pub patch_version: CvrVersion,
}

/// A client's desired-query state. Port of `ClientRecord`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientRecord {
    pub id: String,
    pub desired_query_ids: Vec<String>,
}

/// Per-client state for a query (TTL/activation bookkeeping). Port of the
/// internal `clientStateSchema`, plus the persisted `desires.deleted` bit that
/// the inspect path needs when projecting DB-loaded desired-query rows.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQueryState {
    /// When the query was last inactivated; `None` means active.
    pub inactivated_at: Option<TtlClock>,
    /// Time-to-live in milliseconds; negative means "forever".
    pub ttl: f64,
    /// Whether the persisted desire row is a tombstone.
    pub deleted: bool,
    pub version: CvrVersion,
}

/// Fields shared by client- and custom-query records (as opposed to internal
/// queries). Port of the internal `externalQueryRecordSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct ExternalQueryBase {
    pub id: String,
    pub transformation_hash: Option<String>,
    pub transformation_version: Option<CvrVersion>,
    pub row_set_signature: Option<String>,
    /// Per-client-ID state.
    pub client_state: BTreeMap<String, ClientQueryState>,
    /// Absent if the query has not yet reached the "gotten" state.
    pub patch_version: Option<CvrVersion>,
}

/// A query supplied directly by a client. Port of `ClientQueryRecord`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientQueryRecord {
    pub base: ExternalQueryBase,
    pub ast: Ast,
}

/// A named custom query with arguments. Port of `CustomQueryRecord`.
#[derive(Debug, Clone, PartialEq)]
pub struct CustomQueryRecord {
    pub base: ExternalQueryBase,
    pub name: String,
    pub args: Vec<JsonValue>,
}

/// An internally-tracked query (e.g. `lastMutationID`s). Port of
/// `InternalQueryRecord`.
#[derive(Debug, Clone, PartialEq)]
pub struct InternalQueryRecord {
    pub id: String,
    pub transformation_hash: Option<String>,
    pub transformation_version: Option<CvrVersion>,
    pub row_set_signature: Option<String>,
    pub ast: Ast,
}

/// A query record: client-, custom-, or internal-sourced. Port of `QueryRecord`.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryRecord {
    Client(ClientQueryRecord),
    Custom(CustomQueryRecord),
    Internal(InternalQueryRecord),
}

/// Identifies a specific row. Port of `RowID`.
#[derive(Debug, Clone, PartialEq)]
pub struct RowId {
    pub schema: String,
    pub table: String,
    pub row_key: BTreeMap<String, JsonValue>,
}

/// A tracked row and its query reference counts. Port of `RowRecord`. `None`
/// ref-counts (vs. an empty map) denotes a tombstone (removed from the view).
#[derive(Debug, Clone, PartialEq)]
pub struct RowRecord {
    pub base: CvrRecordBase,
    pub id: RowId,
    /// `_0_version` of the row.
    pub row_version: String,
    /// Query hash -> reference count, or `None` for a tombstoned row.
    pub ref_counts: Option<BTreeMap<String, i64>>,
}

/// The put/del discriminant for patches. Port of `patchSchema`'s `op`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatchOp {
    Put,
    Del,
}

/// A row put patch. Port of `PutRowPatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct PutRowPatch {
    pub id: RowId,
    /// `_0_version` of the row.
    pub row_version: String,
}

/// A row delete patch. Port of `DelRowPatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct DelRowPatch {
    pub id: RowId,
}

/// A row patch: put or delete. Port of `RowPatch`.
#[derive(Debug, Clone, PartialEq)]
pub enum RowPatch {
    Put(PutRowPatch),
    Del(DelRowPatch),
}

/// A query add/remove patch. `client_id` is set for "desired" patches, absent
/// for "got" patches. Port of `QueryPatch` (and `MetadataPatch`, which is an
/// alias of it).
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPatch {
    pub op: PatchOp,
    pub id: String,
    pub client_id: Option<String>,
}

/// The full CVR (Client View Record) state. Port of `CVR` (mutable) /
/// `CVRSnapshot` (readonly view — modeled the same here since Rust borrowing
/// already gives read-only access via `&CVR`).
#[derive(Debug, Clone, PartialEq)]
pub struct Cvr {
    pub id: String,
    pub version: CvrVersion,
    pub last_active: f64,
    pub ttl_clock: TtlClock,
    pub replica_version: Option<String>,
    pub clients: BTreeMap<String, ClientRecord>,
    pub queries: BTreeMap<String, QueryRecord>,
    /// Opaque client schema (not yet ported); `None` if absent.
    pub client_schema: Option<JsonValue>,
    pub profile_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_version::empty_cvr_version;

    #[test]
    fn constructs_client_record() {
        let c = ClientRecord {
            id: "client-1".into(),
            desired_query_ids: vec!["q1".into(), "q2".into()],
        };
        assert_eq!(c.desired_query_ids.len(), 2);
    }

    #[test]
    fn constructs_row_record_with_tombstone() {
        let row = RowRecord {
            base: CvrRecordBase {
                patch_version: empty_cvr_version(),
            },
            id: RowId {
                schema: "public".into(),
                table: "issues".into(),
                row_key: BTreeMap::from([("id".to_string(), JsonValue::String("1".into()))]),
            },
            row_version: "01".into(),
            ref_counts: None, // tombstone
        };
        assert!(row.ref_counts.is_none());
    }

    #[test]
    fn constructs_query_patch_variants() {
        let desired = QueryPatch {
            op: PatchOp::Put,
            id: "q1".into(),
            client_id: Some("client-1".into()),
        };
        let got = QueryPatch {
            op: PatchOp::Put,
            id: "q1".into(),
            client_id: None,
        };
        assert!(desired.client_id.is_some());
        assert!(got.client_id.is_none());
    }

    #[test]
    fn ttl_clock_round_trips() {
        let t = TtlClock::from_number(1234.5);
        assert_eq!(t.as_number(), 1234.5);
    }
}
