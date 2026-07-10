//! Port of `zero-protocol/src/inspect-up.ts`.
//!
//! This is the upstream (client -> server) inspector request shape. The
//! downstream inspector response shapes are larger and stateful-result-driven;
//! this module intentionally covers the request body needed to decode and
//! route the `["inspect", ...]` upstream tag.

use zero_cache_shared::bigint_json::JsonValue;

use crate::ast::Ast;

#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeQueryOptions {
    pub vended_rows: Option<bool>,
    pub synced_rows: Option<bool>,
    pub join_plans: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum InspectUpBody {
    Queries {
        id: String,
        client_id: Option<String>,
    },
    Metrics {
        id: String,
    },
    Version {
        id: String,
    },
    Authenticate {
        id: String,
        value: String,
    },
    AnalyzeQuery {
        id: String,
        /// Deprecated upstream field; retained for wire compatibility.
        value: Option<Ast>,
        options: Option<AnalyzeQueryOptions>,
        ast: Option<Ast>,
        name: Option<String>,
        args: Option<Vec<JsonValue>>,
    },
}

impl InspectUpBody {
    pub fn id(&self) -> &str {
        match self {
            InspectUpBody::Queries { id, .. }
            | InspectUpBody::Metrics { id }
            | InspectUpBody::Version { id }
            | InspectUpBody::Authenticate { id, .. }
            | InspectUpBody::AnalyzeQuery { id, .. } => id,
        }
    }
}
