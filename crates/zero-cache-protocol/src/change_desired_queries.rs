//! Port of `zero-protocol/src/change-desired-queries.ts`.

use crate::queries_patch::UpQueriesPatch;

/// Port of `ChangeDesiredQueriesBody`.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeDesiredQueriesBody {
    pub desired_queries_patch: UpQueriesPatch,
    /// W3C traceparent header for distributed tracing.
    pub traceparent: Option<String>,
}
