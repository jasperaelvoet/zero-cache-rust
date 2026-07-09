//! Port of `zero-protocol/src/pull.ts`.
//!
//! Pull is currently used by upstream for mutation recovery: the request asks
//! for changes since a cookie, and the response carries the latest cookie plus
//! `lastMutationIDChanges`. The row patch is intentionally absent upstream
//! ("save work by not computing the patch"), so this module is just the pure
//! request/response data model and its small JSON codec lives in
//! [`crate::pull_json`].

use std::collections::BTreeMap;

use crate::version::{NullableVersion, Version};

/// Port of `pullRequestBodySchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct PullRequestBody {
    pub client_group_id: String,
    pub cookie: NullableVersion,
    pub request_id: String,
}

/// Port of `pullResponseBodySchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct PullResponseBody {
    pub cookie: Version,
    /// Matches the request ID that initiated this response.
    pub request_id: String,
    pub last_mutation_id_changes: BTreeMap<String, f64>,
}
