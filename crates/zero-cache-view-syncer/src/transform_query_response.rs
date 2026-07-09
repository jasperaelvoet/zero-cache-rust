//! Port of `custom-queries/transform-query.ts`'s `CustomQueryTransformer#transform`
//! response-shaping logic — split from the HTTP call itself, matching this
//! port's established boundary (see `mutagen::orchestration`,
//! `write_authorizer`): the actual `fetchFromAPIServer` request/response
//! round trip is NOT ported here (needs an HTTP client + the `/query` wire
//! framing — a separate, connection-coupled slice); this module ports the
//! pure pieces around it — splitting queries into cached/uncached via
//! `zero_cache_shared::timed_cache::TimedCache`, and shaping a raw
//! [`QueryResponse`] into the caller-facing result — that are directly
//! testable without a live server.
//!
//! `validate()` (the empty-request auth-maintenance path) is NOT ported:
//! it's a thin wrapper around `#requestTransform` with no decision logic of
//! its own beyond "always hit the server", nothing to extract.

use zero_cache_protocol::custom_queries::{ErroredQuery, QueryResult, TransformRequestQuery};
use zero_cache_protocol::error::TransformFailedBody;
use zero_cache_protocol::query_hash::hash_of_ast;
use zero_cache_protocol::query_server::{QueryResponse, QuerySuccess};
use zero_cache_shared::timed_cache::TimedCache;

use crate::cvr_types::CustomQueryRecord;

/// A successfully transformed, hashed query result. Port of
/// `TransformedAndHashed` (`view-syncer/auth/read-authorizer.ts`) — the one
/// piece of that not-yet-ported type this module needs.
#[derive(Debug, Clone, PartialEq)]
pub struct TransformedAndHashed {
    pub id: String,
    pub transformed_ast: zero_cache_protocol::ast::Ast,
    pub transformation_hash: String,
}

/// A single query's outcome after (possibly cached) transformation. Port of
/// one element of `HashedTransformResponse.result`.
#[derive(Debug, Clone, PartialEq)]
pub enum TransformedOrErrored {
    Ok(TransformedAndHashed),
    Errored(ErroredQuery),
}

/// Port of `HashedTransformResponse`.
#[derive(Debug, Clone, PartialEq)]
pub enum HashedTransformResponse {
    Failed(TransformFailedBody),
    /// `cached == true` iff every query was served from the cache (so no
    /// request was actually made) — matches upstream's `cached: true`
    /// variant that additionally omits `validation` (nothing to report).
    Success {
        result: Vec<TransformedOrErrored>,
        cached: bool,
    },
}

/// Splits `queries` into cache hits and the request that still needs to be
/// sent for the rest. Port of the first half of `transform()`'s loop
/// (`getCacheKey` + `#cache.get`). `cache_key` is a caller-supplied fn since
/// this crate doesn't build a `ConnectionContext` here — callers use
/// `transform_query_cache_key::get_cache_key`.
pub fn split_cached_and_uncached<'a>(
    queries: impl Iterator<Item = &'a CustomQueryRecord>,
    cache: &mut TimedCache<String, TransformedAndHashed>,
    mut cache_key: impl FnMut(&str) -> String,
    now: i64,
) -> (Vec<TransformedAndHashed>, Vec<TransformRequestQuery>) {
    let mut cached_responses = Vec::new();
    let mut request = Vec::new();
    for query in queries {
        let key = cache_key(&query.base.id);
        if let Some(hit) = cache.get(&key, now) {
            cached_responses.push(hit.clone());
        } else {
            request.push(TransformRequestQuery {
                id: query.base.id.clone(),
                name: query.name.clone(),
                args: query.args.clone(),
            });
        }
    }
    (cached_responses, request)
}

/// Port of the second half of `transform()`: given the raw [`QueryResponse`]
/// from an actual `/transform` request (empty `request` short-circuits
/// before ever reaching this — matches upstream's `request.length === 0`
/// early return, modeled by callers simply not calling this when the
/// uncached list from [`split_cached_and_uncached`] is empty), maps each
/// result to [`TransformedAndHashed`]/[`ErroredQuery`] (computing the
/// transformation hash via `hashOfAST`) and populates `cache` for every
/// success (errors are deliberately not cached, matching upstream's comment
/// that the caller may want to retry a transient failure).
pub fn shape_transform_response(
    response: QueryResponse,
    cached_responses: Vec<TransformedAndHashed>,
    cache: &mut TimedCache<String, TransformedAndHashed>,
    mut cache_key: impl FnMut(&str) -> String,
    now: i64,
) -> HashedTransformResponse {
    let QuerySuccess { queries, .. } = match response {
        QueryResponse::Failed(body) => return HashedTransformResponse::Failed(body),
        QueryResponse::Success(success) => success,
    };

    let new_responses: Vec<TransformedOrErrored> = queries
        .into_iter()
        .map(|result| match result {
            QueryResult::Errored(e) => TransformedOrErrored::Errored(e),
            QueryResult::Transformed(t) => {
                let hash = hash_of_ast(&t.ast);
                TransformedOrErrored::Ok(TransformedAndHashed {
                    id: t.id,
                    transformed_ast: t.ast,
                    transformation_hash: hash,
                })
            }
        })
        .collect();

    for transformed in &new_responses {
        if let TransformedOrErrored::Ok(t) = transformed {
            let key = cache_key(&t.id);
            cache.set(key, t.clone(), now);
        }
    }

    let mut result: Vec<TransformedOrErrored> = new_responses;
    result.extend(cached_responses.into_iter().map(TransformedOrErrored::Ok));

    HashedTransformResponse::Success {
        result,
        cached: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::Ast;
    use zero_cache_protocol::custom_queries::{ErroredQueryKind, TransformedQuery};
    use zero_cache_protocol::error::TransformFailedReason;
    use zero_cache_shared::bigint_json::JsonValue;

    fn query(id: &str, name: &str) -> CustomQueryRecord {
        CustomQueryRecord {
            base: crate::cvr_types::ExternalQueryBase {
                id: id.to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
                client_state: Default::default(),
                patch_version: None,
            },
            name: name.to_string(),
            args: vec![JsonValue::Number(1.0)],
        }
    }

    #[test]
    fn split_returns_all_queries_uncached_when_cache_is_empty() {
        let mut cache: TimedCache<String, TransformedAndHashed> = TimedCache::new(5000);
        let queries = vec![query("q1", "n1"), query("q2", "n2")];
        let (cached, request) =
            split_cached_and_uncached(queries.iter(), &mut cache, |id| id.to_string(), 0);
        assert!(cached.is_empty());
        assert_eq!(request.len(), 2);
        assert_eq!(request[0].id, "q1");
    }

    #[test]
    fn split_pulls_cache_hits_out_of_the_request() {
        let mut cache: TimedCache<String, TransformedAndHashed> = TimedCache::new(5000);
        let hit = TransformedAndHashed {
            id: "q1".into(),
            transformed_ast: Ast::table("t"),
            transformation_hash: "h1".into(),
        };
        cache.set("q1".to_string(), hit.clone(), 0);
        let queries = vec![query("q1", "n1"), query("q2", "n2")];
        let (cached, request) =
            split_cached_and_uncached(queries.iter(), &mut cache, |id| id.to_string(), 0);
        assert_eq!(cached, vec![hit]);
        assert_eq!(request.len(), 1);
        assert_eq!(request[0].id, "q2");
    }

    #[test]
    fn shape_response_maps_success_and_computes_hash() {
        let mut cache: TimedCache<String, TransformedAndHashed> = TimedCache::new(5000);
        let ast = Ast::table("issues");
        let response = QueryResponse::Success(QuerySuccess {
            user_id: None,
            queries: vec![QueryResult::Transformed(TransformedQuery {
                id: "q1".into(),
                name: "n1".into(),
                ast: ast.clone(),
            })],
        });
        let shaped = shape_transform_response(response, vec![], &mut cache, |id| id.to_string(), 0);
        match shaped {
            HashedTransformResponse::Success { result, cached } => {
                assert!(!cached);
                assert_eq!(result.len(), 1);
                match &result[0] {
                    TransformedOrErrored::Ok(t) => {
                        assert_eq!(t.id, "q1");
                        assert_eq!(t.transformation_hash, hash_of_ast(&ast));
                    }
                    _ => panic!("expected Ok"),
                }
            }
            _ => panic!("expected Success"),
        }
        // populated the cache for the successful query
        assert!(cache.get(&"q1".to_string(), 0).is_some());
    }

    #[test]
    fn shape_response_does_not_cache_errored_queries() {
        let mut cache: TimedCache<String, TransformedAndHashed> = TimedCache::new(5000);
        let response = QueryResponse::Success(QuerySuccess {
            user_id: None,
            queries: vec![QueryResult::Errored(ErroredQuery {
                error: ErroredQueryKind::App,
                id: "q1".into(),
                name: "n1".into(),
                message: Some("nope".into()),
                details: None,
            })],
        });
        let shaped = shape_transform_response(response, vec![], &mut cache, |id| id.to_string(), 0);
        match shaped {
            HashedTransformResponse::Success { result, .. } => {
                assert!(matches!(result[0], TransformedOrErrored::Errored(_)));
            }
            _ => panic!("expected Success"),
        }
        assert!(
            cache.get(&"q1".to_string(), 0).is_none(),
            "error responses must not be cached"
        );
    }

    #[test]
    fn shape_response_merges_new_and_cached_results() {
        let mut cache: TimedCache<String, TransformedAndHashed> = TimedCache::new(5000);
        let cached_hit = TransformedAndHashed {
            id: "cached1".into(),
            transformed_ast: Ast::table("t"),
            transformation_hash: "h".into(),
        };
        let response = QueryResponse::Success(QuerySuccess {
            user_id: None,
            queries: vec![QueryResult::Transformed(TransformedQuery {
                id: "new1".into(),
                name: "n".into(),
                ast: Ast::table("t2"),
            })],
        });
        let shaped = shape_transform_response(
            response,
            vec![cached_hit.clone()],
            &mut cache,
            |id| id.to_string(),
            0,
        );
        match shaped {
            HashedTransformResponse::Success { result, .. } => {
                let ids: Vec<&str> = result
                    .iter()
                    .map(|r| match r {
                        TransformedOrErrored::Ok(t) => t.id.as_str(),
                        TransformedOrErrored::Errored(e) => e.id.as_str(),
                    })
                    .collect();
                assert_eq!(ids, vec!["new1", "cached1"]);
            }
            _ => panic!("expected Success"),
        }
    }

    #[test]
    fn shape_response_propagates_failure_untouched() {
        let mut cache: TimedCache<String, TransformedAndHashed> = TimedCache::new(5000);
        let failure = TransformFailedBody {
            reason: TransformFailedReason::ZeroCacheOther(
                zero_cache_protocol::error_reason::ErrorReason::Internal,
            ),
            query_ids: vec!["q1".into()],
            message: "boom".into(),
            details: None,
        };
        let shaped = shape_transform_response(
            QueryResponse::Failed(failure.clone()),
            vec![],
            &mut cache,
            |id| id.to_string(),
            0,
        );
        assert_eq!(shaped, HashedTransformResponse::Failed(failure));
    }
}
