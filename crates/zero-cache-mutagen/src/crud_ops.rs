//! Port of the CRUD mutation-op types from `zero-protocol/src/mutation.ts`.
//!
//! These model the `ops` array a client's CRUD mutation carries — deferred
//! out of `zero-cache-protocol` because they're consumed exclusively by
//! this crate's `mutagen.ts` port, not the downstream sync protocol
//! (`zero-cache-protocol::mutations_patch` only needs the mutation
//! *result*, not the *request* shape).

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::JsonValue;

/// A row: column name -> value. Same shape as the `Row` alias in every
/// other crate that needs one (see e.g. `zero-cache-protocol::row_patch`'s
/// module doc for why each crate defines its own).
pub type Row = Vec<(String, JsonValue)>;

/// A table's primary key: an ordered, non-empty list of column names. Port
/// of `PrimaryKey`.
pub type PrimaryKey = Vec<String>;

/// A primary key's values, by column name. Port of
/// `PrimaryKeyValueRecord`.
pub type PrimaryKeyValueRecord = BTreeMap<String, JsonValue>;

/// Insert a new row; fails if a row with the same primary key already
/// exists. Port of `InsertOp`.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertOp {
    pub table_name: String,
    pub primary_key: PrimaryKey,
    pub value: Row,
}

/// Insert a new row, or replace it if one with the same primary key already
/// exists. Port of `UpsertOp`.
#[derive(Debug, Clone, PartialEq)]
pub struct UpsertOp {
    pub table_name: String,
    pub primary_key: PrimaryKey,
    pub value: Row,
}

/// Updates an existing row (identified by `value`'s primary-key fields);
/// does nothing if no such row exists. `value` is a partial row containing
/// at least the primary-key fields. Port of `UpdateOp`.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateOp {
    pub table_name: String,
    pub primary_key: PrimaryKey,
    pub value: Row,
}

/// Deletes an existing row by primary key. Port of `DeleteOp`.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteOp {
    pub table_name: String,
    pub primary_key: PrimaryKey,
    pub value: PrimaryKeyValueRecord,
}

/// Port of `CRUDOp` (`insertOpSchema | upsertOpSchema | updateOpSchema |
/// deleteOpSchema`).
#[derive(Debug, Clone, PartialEq)]
pub enum CrudOp {
    Insert(InsertOp),
    Upsert(UpsertOp),
    Update(UpdateOp),
    Delete(DeleteOp),
}

/// Port of `CRUDMutationArg` (`{ops: CRUDOp[]}`).
#[derive(Debug, Clone, PartialEq)]
pub struct CrudMutationArg {
    pub ops: Vec<CrudOp>,
}
