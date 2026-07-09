//! Port of `zero-protocol/src/query-server.ts`'s pure data model: the
//! top-level `/query` response envelope (success-with-optional-userID, or a
//! `TransformFailedBody` failure). See `custom_queries.rs` for the per-query
//! result shapes this wraps.

use crate::custom_queries::TransformResponseBody as QueryResponseBodyInner;
use crate::error::TransformFailedBody;

/// Port of `QueryResponseBody` (`queryResultSchema[]`, same shape as
/// `custom_queries::TransformResponseBody`).
pub type QueryResponseBody = QueryResponseBodyInner;

/// The successful `/query` response envelope. Port of `QuerySuccess`.
#[derive(Debug, Clone, PartialEq)]
pub struct QuerySuccess {
    /// `None` = the field was absent (server didn't validate auth);
    /// `Some(None)` = the server explicitly validated and found no user
    /// (`userID: null`); `Some(Some(id))` = server-validated user.
    pub user_id: Option<Option<String>>,
    pub queries: QueryResponseBody,
}

/// Port of `QueryResponse` (`querySuccessSchema | transformFailedBodySchema
/// | transformResponseMessageSchema`). The legacy tuple-message backwards-
/// compatibility variant (`['transformed', ...] | ['transformFailed', ...]`)
/// is handled at the `transform_query_response` call site instead of here,
/// since it's a wire-shape detail of `#requestTransform`, not part of this
/// type's own data model.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResponse {
    Success(QuerySuccess),
    Failed(TransformFailedBody),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_success_distinguishes_absent_vs_null_vs_present_user_id() {
        let absent = QuerySuccess {
            user_id: None,
            queries: vec![],
        };
        let null = QuerySuccess {
            user_id: Some(None),
            queries: vec![],
        };
        let present = QuerySuccess {
            user_id: Some(Some("u1".into())),
            queries: vec![],
        };
        assert_eq!(absent.user_id, None);
        assert_eq!(null.user_id, Some(None));
        assert_eq!(present.user_id, Some(Some("u1".to_string())));
    }
}
