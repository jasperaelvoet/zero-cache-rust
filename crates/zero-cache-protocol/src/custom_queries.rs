//! Port of `zero-protocol/src/custom-queries.ts`'s pure data model: the
//! `/transform` request/response shapes exchanged between zero-cache and a
//! user's API server. `valita` schemas are not ported (no schema-validation
//! library in this port, per established convention) — only the resulting
//! Rust types, matching how `ast.rs`/`poke.rs`/etc. handle their upstream
//! `valita` counterparts.

use zero_cache_shared::bigint_json::JsonValue;

use crate::ast::Ast;

/// One query in a `/transform` request. Port of `transformRequestBodySchema`'s
/// element shape.
#[derive(Debug, Clone, PartialEq)]
pub struct TransformRequestQuery {
    pub id: String,
    pub name: String,
    pub args: Vec<JsonValue>,
}

/// Port of `TransformRequestBody`.
pub type TransformRequestBody = Vec<TransformRequestQuery>;

/// A successfully transformed query. Port of `transformedQuerySchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct TransformedQuery {
    pub id: String,
    pub name: String,
    pub ast: Ast,
}

/// Why a single query failed to transform. Port of the `error` discriminant
/// shared by `appErroredQuerySchema`/`parseErroredQuerySchema`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErroredQueryKind {
    /// The user's API server rejected the query (`error: 'app'`).
    App,
    /// zero-cache couldn't parse the server's response for this query
    /// (`error: 'parse'`).
    Parse,
}

/// A query that failed to transform. Port of `ErroredQuery`.
///
/// `message` is `Option` for `App` (matching upstream's "optional for
/// backwards compatibility" comment) but always present for `Parse`; this is
/// not encoded in the type since both variants share one struct, matching
/// how the `valita` union is a single flat shape with a discriminant.
#[derive(Debug, Clone, PartialEq)]
pub struct ErroredQuery {
    pub error: ErroredQueryKind,
    pub id: String,
    pub name: String,
    pub message: Option<String>,
    pub details: Option<JsonValue>,
}

/// A single transform result: success or per-query failure. Port of
/// `queryResultSchema`/one element of `transformResponseBodySchema`.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    Transformed(TransformedQuery),
    Errored(ErroredQuery),
}

/// Port of `TransformResponseBody`/`QueryResponseBody` (the two are the same
/// shape, `queryResultSchema` reuses `transformedQuerySchema`/
/// `erroredQuerySchema`).
pub type TransformResponseBody = Vec<QueryResult>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructs_transform_request_query() {
        let q = TransformRequestQuery {
            id: "q1".into(),
            name: "myQuery".into(),
            args: vec![JsonValue::Number(1.0)],
        };
        assert_eq!(q.id, "q1");
        assert_eq!(q.args.len(), 1);
    }

    #[test]
    fn query_result_distinguishes_success_and_error() {
        let ok = QueryResult::Transformed(TransformedQuery {
            id: "q1".into(),
            name: "n".into(),
            ast: Ast::table("t"),
        });
        let err = QueryResult::Errored(ErroredQuery {
            error: ErroredQueryKind::App,
            id: "q2".into(),
            name: "n2".into(),
            message: Some("nope".into()),
            details: None,
        });
        assert!(matches!(ok, QueryResult::Transformed(_)));
        assert!(matches!(err, QueryResult::Errored(_)));
    }
}
