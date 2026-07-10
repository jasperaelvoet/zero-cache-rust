//! Deserializes a [`JsonValue`] (as returned by a real `/query` HTTP
//! response body) into [`QueryResponse`] — the counterpart to `ast_json`'s
//! `ast_from_json` for `query-server.ts`'s `queryResponseSchema`. This is
//! the piece that was missing to actually wire `zero-cache-mutagen::
//! api_fetch::fetch_from_api_server`'s generic `serde_json::Value` result
//! into `view-syncer::transform_query_response::shape_transform_response`,
//! closing the custom-queries HTTP/wire gap.
//!
//! Scope: parses `querySuccessSchema` and `transformFailedBodySchema`.
//! `transformResponseMessageSchema`'s legacy tuple-message shapes
//! (`['transformed', ...]` / `['transformFailed', ...]`, kept upstream
//! "for backwards compatibility") are NOT ported — nothing in this port's
//! HTTP client is a legacy caller, and `transform_query_response`'s doc
//! already notes `#requestTransform`'s legacy-tuple handling is a
//! wire-shape detail for that call site, not this type's own data model.

use zero_cache_shared::bigint_json::JsonValue;

use crate::ast_json::{ast_from_json, AstJsonError};
use crate::custom_queries::{ErroredQuery, ErroredQueryKind, QueryResult, TransformedQuery};
use crate::error::{TransformFailedBody, TransformFailedReason};
use crate::error_reason::ErrorReason;
use crate::query_server::{QueryResponse, QuerySuccess};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid /query response JSON: {0}")]
pub struct QueryResponseJsonError(pub String);

impl From<AstJsonError> for QueryResponseJsonError {
    fn from(e: AstJsonError) -> Self {
        QueryResponseJsonError(e.0)
    }
}

fn err(msg: impl Into<String>) -> QueryResponseJsonError {
    QueryResponseJsonError(msg.into())
}

fn as_object(v: &JsonValue) -> Result<&Vec<(String, JsonValue)>, QueryResponseJsonError> {
    match v {
        JsonValue::Object(entries) => Ok(entries),
        other => Err(err(format!("expected object, got {other:?}"))),
    }
}

fn field<'a>(obj: &'a [(String, JsonValue)], key: &str) -> Option<&'a JsonValue> {
    obj.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

fn required<'a>(
    obj: &'a [(String, JsonValue)],
    key: &str,
) -> Result<&'a JsonValue, QueryResponseJsonError> {
    field(obj, key).ok_or_else(|| err(format!("missing field {key:?}")))
}

fn as_str(v: &JsonValue) -> Result<&str, QueryResponseJsonError> {
    match v {
        JsonValue::String(s) => Ok(s.as_str()),
        other => Err(err(format!("expected string, got {other:?}"))),
    }
}

fn as_f64(v: &JsonValue) -> Result<f64, QueryResponseJsonError> {
    match v {
        JsonValue::Number(n) => Ok(*n),
        other => Err(err(format!("expected number, got {other:?}"))),
    }
}

fn as_array(v: &JsonValue) -> Result<&Vec<JsonValue>, QueryResponseJsonError> {
    match v {
        JsonValue::Array(items) => Ok(items),
        other => Err(err(format!("expected array, got {other:?}"))),
    }
}

fn string_at(
    obj: &[(String, JsonValue)],
    key: &str,
) -> Result<Option<String>, QueryResponseJsonError> {
    field(obj, key)
        .map(as_str)
        .transpose()
        .map(|o| o.map(str::to_string))
}

/// Port of `queryResultSchema`, discriminated by presence of the `error`
/// field (an `erroredQuerySchema` has one, a `transformedQuerySchema`
/// doesn't).
fn query_result_from_json(v: &JsonValue) -> Result<QueryResult, QueryResponseJsonError> {
    let obj = as_object(v)?;
    match field(obj, "error") {
        None => Ok(QueryResult::Transformed(TransformedQuery {
            id: as_str(required(obj, "id")?)?.to_string(),
            name: as_str(required(obj, "name")?)?.to_string(),
            ast: ast_from_json(required(obj, "ast")?)?,
        })),
        Some(error_kind) => {
            let kind = match as_str(error_kind)? {
                "app" => ErroredQueryKind::App,
                "parse" => ErroredQueryKind::Parse,
                other => return Err(err(format!("unknown erroredQuery error kind {other:?}"))),
            };
            Ok(QueryResult::Errored(ErroredQuery {
                error: kind,
                id: as_str(required(obj, "id")?)?.to_string(),
                name: as_str(required(obj, "name")?)?.to_string(),
                message: string_at(obj, "message")?,
                details: field(obj, "details").cloned(),
            }))
        }
    }
}

/// Port of `queryResponseBodySchema`.
fn query_response_body_from_json(
    v: &JsonValue,
) -> Result<Vec<QueryResult>, QueryResponseJsonError> {
    as_array(v)?.iter().map(query_result_from_json).collect()
}

/// Port of `querySuccessSchema`.
fn query_success_from_json(
    obj: &[(String, JsonValue)],
) -> Result<QuerySuccess, QueryResponseJsonError> {
    let user_id = match field(obj, "userID") {
        None => None,
        Some(JsonValue::Null) => Some(None),
        Some(v) => Some(Some(as_str(v)?.to_string())),
    };
    Ok(QuerySuccess {
        user_id,
        queries: query_response_body_from_json(required(obj, "queries")?)?,
    })
}

/// Port of `transformFailedBodySchema`'s origin-discriminated union.
fn transform_failed_body_from_json(
    obj: &[(String, JsonValue)],
) -> Result<TransformFailedBody, QueryResponseJsonError> {
    let origin = as_str(required(obj, "origin")?)?;
    let reason_str = as_str(required(obj, "reason")?)?;
    let reason = match origin {
        "server" => {
            let r = ErrorReason::from_str(reason_str)
                .ok_or_else(|| err(format!("unknown server reason {reason_str:?}")))?;
            TransformFailedReason::Server(r)
        }
        "zeroCache" if reason_str == "http" => TransformFailedReason::ZeroCacheHttp {
            status: as_f64(required(obj, "status")?)?,
            body_preview: string_at(obj, "bodyPreview")?,
        },
        "zeroCache" => {
            let r = ErrorReason::from_str(reason_str)
                .ok_or_else(|| err(format!("unknown zeroCache reason {reason_str:?}")))?;
            TransformFailedReason::ZeroCacheOther(r)
        }
        other => return Err(err(format!("unknown TransformFailedBody origin {other:?}"))),
    };
    let query_ids = as_array(required(obj, "queryIDs")?)?
        .iter()
        .map(as_str)
        .map(|r| r.map(str::to_string))
        .collect::<Result<_, _>>()?;
    Ok(TransformFailedBody {
        reason,
        query_ids,
        message: as_str(required(obj, "message")?)?.to_string(),
        details: field(obj, "details").cloned(),
    })
}

/// Port of `queryResponseSchema` (minus the legacy tuple-message
/// variant — see module doc). The main entry point of this module.
pub fn query_response_from_json(v: &JsonValue) -> Result<QueryResponse, QueryResponseJsonError> {
    let obj = as_object(v)?;
    match field(obj, "kind") {
        Some(JsonValue::String(kind)) if kind == "QueryResponse" => {
            Ok(QueryResponse::Success(query_success_from_json(obj)?))
        }
        Some(JsonValue::String(kind)) if kind == "TransformFailed" => {
            Ok(QueryResponse::Failed(transform_failed_body_from_json(obj)?))
        }
        Some(JsonValue::String(other)) => Err(err(format!("unknown queryResponse kind {other:?}"))),
        _ => Err(err("missing or invalid \"kind\" field")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_shared::bigint_json::parse as parse_json;

    #[test]
    fn parses_a_success_response_with_transformed_and_errored_queries() {
        let json = parse_json(
            r#"{"kind":"QueryResponse","userID":"u1","queries":[
                {"id":"q1","name":"n1","ast":{"table":"issues"}},
                {"error":"app","id":"q2","name":"n2","message":"denied"}
            ]}"#,
        )
        .unwrap();
        let response = query_response_from_json(&json).unwrap();
        match response {
            QueryResponse::Success(success) => {
                assert_eq!(success.user_id, Some(Some("u1".to_string())));
                assert_eq!(success.queries.len(), 2);
                assert!(matches!(success.queries[0], QueryResult::Transformed(_)));
                assert!(matches!(success.queries[1], QueryResult::Errored(_)));
            }
            _ => panic!("expected Success"),
        }
    }

    #[test]
    fn success_response_treats_absent_and_null_user_id_differently() {
        let absent = query_response_from_json(
            &parse_json(r#"{"kind":"QueryResponse","queries":[]}"#).unwrap(),
        )
        .unwrap();
        let null = query_response_from_json(
            &parse_json(r#"{"kind":"QueryResponse","userID":null,"queries":[]}"#).unwrap(),
        )
        .unwrap();
        match (absent, null) {
            (QueryResponse::Success(a), QueryResponse::Success(b)) => {
                assert_eq!(a.user_id, None);
                assert_eq!(b.user_id, Some(None));
            }
            _ => panic!("expected Success"),
        }
    }

    #[test]
    fn parses_a_zero_cache_http_transform_failed_body() {
        let json = parse_json(
            r#"{"kind":"TransformFailed","origin":"zeroCache","reason":"http","status":502,"message":"bad gateway","queryIDs":["q1"]}"#,
        )
        .unwrap();
        let response = query_response_from_json(&json).unwrap();
        match response {
            QueryResponse::Failed(body) => {
                assert_eq!(body.query_ids, vec!["q1".to_string()]);
                assert!(
                    matches!(body.reason, TransformFailedReason::ZeroCacheHttp { status, .. } if status == 502.0)
                );
            }
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn parses_a_server_origin_transform_failed_body() {
        let json = parse_json(r#"{"kind":"TransformFailed","origin":"server","reason":"database","message":"boom","queryIDs":[]}"#).unwrap();
        let response = query_response_from_json(&json).unwrap();
        match response {
            QueryResponse::Failed(body) => {
                assert!(matches!(
                    body.reason,
                    TransformFailedReason::Server(ErrorReason::Database)
                ));
            }
            _ => panic!("expected Failed"),
        }
    }

    #[test]
    fn unknown_kind_errors() {
        assert!(query_response_from_json(&parse_json(r#"{"kind":"bogus"}"#).unwrap()).is_err());
    }

    #[test]
    fn malformed_ast_inside_a_transformed_query_propagates_as_an_error() {
        let json =
            parse_json(r#"{"kind":"QueryResponse","queries":[{"id":"q1","name":"n1","ast":{}}]}"#)
                .unwrap();
        assert!(
            query_response_from_json(&json).is_err(),
            "AST missing required `table` field should fail"
        );
    }
}
