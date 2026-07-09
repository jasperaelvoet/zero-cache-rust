//! Port of `zero-protocol/src/row-patch.ts`.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::JsonValue;

/// A row: column name -> value. Same shape as the `Row` alias in
/// `zero-cache-change-source`/`zero-cache-zql` (all three crates define
/// their own to avoid a protocol crate depending on either).
pub type Row = Vec<(String, JsonValue)>;

/// A primary-key value: `string | number | boolean` (never JSON objects/
/// arrays/null — primary keys must be comparable). Port of
/// `PrimaryKeyValue`. Modeled as a restricted `JsonValue` rather than a
/// dedicated enum to reuse the existing value vocabulary; callers should
/// not construct `Array`/`Object`/`Null`/`BigInt` variants here.
pub type PrimaryKeyValue = JsonValue;

/// A primary key's values, by column name. Port of
/// `PrimaryKeyValueRecord`.
pub type PrimaryKeyValueRecord = BTreeMap<String, PrimaryKeyValue>;

/// Port of `rowPatchOpSchema`'s `put` operation: insert/replace a full row.
#[derive(Debug, Clone, PartialEq)]
pub struct RowPutOp {
    pub table_name: String,
    pub value: Row,
}

/// Port of `rowPatchOpSchema`'s `update` operation: merge JSON fields into
/// an existing row (by primary key), optionally constrained to a column
/// subset.
#[derive(Debug, Clone, PartialEq)]
pub struct RowUpdateOp {
    pub table_name: String,
    pub id: PrimaryKeyValueRecord,
    pub merge: Option<Vec<(String, JsonValue)>>,
    pub constrain: Option<Vec<String>>,
}

/// Port of `rowPatchOpSchema`'s `del` operation.
#[derive(Debug, Clone, PartialEq)]
pub struct RowDelOp {
    pub table_name: String,
    pub id: PrimaryKeyValueRecord,
}

/// Port of `rowPatchOpSchema`'s `clear` operation: discard all synced rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowClearOp;

/// Port of `RowPatchOp` (one element of `rowsPatchSchema`).
#[derive(Debug, Clone, PartialEq)]
pub enum RowPatchOp {
    Put(RowPutOp),
    Update(RowUpdateOp),
    Del(RowDelOp),
    Clear(RowClearOp),
}

/// Port of `rowsPatchSchema` (`v.array(rowPatchOpSchema)`).
pub type RowsPatch = Vec<RowPatchOp>;
