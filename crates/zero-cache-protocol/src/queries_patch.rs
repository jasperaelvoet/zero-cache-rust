//! Port of `zero-protocol/src/queries-patch.ts`.

use zero_cache_shared::bigint_json::JsonValue;

use crate::ast::Ast;

/// Port of `putOpSchema` (the downstream `queriesPatchSchema` variant —
/// server->client only needs `hash`/`ttl`).
#[derive(Debug, Clone, PartialEq)]
pub struct QueriesPutOp {
    pub hash: String,
    pub ttl: Option<f64>,
}

/// Port of `upPutOpSchema` (the upstream `upQueriesPatchSchema` variant —
/// client->server additionally carries the query definition, either as an
/// AST for client queries or a name+args for custom queries; all fields
/// optional during the transitional period upstream notes).
#[derive(Debug, Clone, PartialEq)]
pub struct UpQueriesPutOp {
    pub hash: String,
    pub ttl: Option<f64>,
    pub ast: Option<Ast>,
    pub name: Option<String>,
    pub args: Option<Vec<JsonValue>>,
}

/// Port of `delOpSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct QueriesDelOp {
    pub hash: String,
}

/// Port of `clearOpSchema`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueriesClearOp;

/// Port of `QueriesPatchOp` (downstream direction).
#[derive(Debug, Clone, PartialEq)]
pub enum QueriesPatchOp {
    Put(QueriesPutOp),
    Del(QueriesDelOp),
    Clear(QueriesClearOp),
}

/// Port of `UpQueriesPatchOp` (upstream direction).
#[derive(Debug, Clone, PartialEq)]
pub enum UpQueriesPatchOp {
    Put(UpQueriesPutOp),
    Del(QueriesDelOp),
    Clear(QueriesClearOp),
}

/// Port of `queriesPatchSchema`.
pub type QueriesPatch = Vec<QueriesPatchOp>;

/// Port of `upQueriesPatchSchema`.
pub type UpQueriesPatch = Vec<UpQueriesPatchOp>;
