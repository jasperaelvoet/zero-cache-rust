//! Port of `zero-cache/src/services/change-source/protocol/current/control.ts`.
//!
//! Control-plane messages communicate non-content signals between a
//! ChangeSource and ChangeStreamer; they are not forwarded to ChangeStreamer
//! subscribers.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::JsonValue;

/// Indicates that replication cannot continue and the replica must be resynced
/// from scratch. Port of `resetRequiredSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct ResetRequired {
    pub message: Option<String>,
    /// Published in the `errorDetails` field of a replication ERROR event.
    pub error_details: Option<BTreeMap<String, JsonValue>>,
}
