//! Port of `zero-protocol/src/inspect-down.ts`.
//!
//! This is the downstream (server -> client) inspector response shape. The
//! live handler that fills these responses needs CVR/InspectorDelegate state;
//! this module covers the protocol data model and wire encoder.

use zero_cache_shared::bigint_json::JsonValue;

use crate::analyze_query_result::AnalyzeQueryResult;
use crate::ast::Ast;
use crate::row_patch::Row;

/// Port of `ServerMetrics`: each metric is a TDigest JSON array
/// (`[compression, mean0, weight0, ...]`).
#[derive(Debug, Clone, PartialEq)]
pub struct ServerMetrics {
    pub query_materialization_server: Vec<f64>,
    pub query_update_server: Vec<f64>,
}

/// Port of `InspectQueryRow`.
#[derive(Debug, Clone, PartialEq)]
pub struct InspectQueryRow {
    pub client_id: String,
    pub query_id: String,
    pub ast: Option<Ast>,
    pub name: Option<String>,
    pub args: Option<Vec<JsonValue>>,
    pub got: bool,
    pub deleted: bool,
    pub ttl: f64,
    pub inactivated_at: Option<f64>,
    pub row_count: f64,
    /// Optional nullable metrics. Kept as raw JSON because current clients
    /// expect `QueryServerMetrics`, while older protocol compatibility can
    /// send a legacy `ServerMetrics`-shaped object in the same field.
    pub metrics: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum InspectDownBody {
    Queries {
        id: String,
        value: Vec<InspectQueryRow>,
    },
    Metrics {
        id: String,
        value: ServerMetrics,
    },
    Version {
        id: String,
        value: String,
    },
    Authenticated {
        id: String,
        value: bool,
    },
    AnalyzeQuery {
        id: String,
        value: AnalyzeQueryResult,
    },
    Error {
        id: String,
        value: String,
    },
}

impl InspectDownBody {
    pub fn id(&self) -> &str {
        match self {
            InspectDownBody::Queries { id, .. }
            | InspectDownBody::Metrics { id, .. }
            | InspectDownBody::Version { id, .. }
            | InspectDownBody::Authenticated { id, .. }
            | InspectDownBody::AnalyzeQuery { id, .. }
            | InspectDownBody::Error { id, .. } => id,
        }
    }
}

/// Convenience for callers that have a row object as protocol key/value pairs.
pub fn row_json(row: &Row) -> JsonValue {
    JsonValue::Object(row.clone())
}
