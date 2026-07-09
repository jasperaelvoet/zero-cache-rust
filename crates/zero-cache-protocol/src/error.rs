//! Partial port of `zero-protocol/src/error.ts`.
//!
//! The `valita` schemas for the full discriminated `ErrorBody` union are not
//! ported yet; this provides the `ProtocolError` type and a basic `ErrorBody`
//! sufficient for the error-handling code paths ported so far.

use std::error::Error;
use std::fmt;

use std::collections::BTreeMap;

use crate::error_kind::ErrorKind;
use crate::error_origin::ErrorOrigin;
use crate::error_reason::ErrorReason;
use crate::mutation_id::MutationId;
use zero_cache_shared::bigint_json::JsonValue;

/// A protocol error body (basic form). Port of the common fields of
/// `ErrorBody`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorBody {
    pub kind: ErrorKind,
    pub message: String,
    /// Optional for backwards compatibility.
    pub origin: Option<ErrorOrigin>,
}

impl ErrorBody {
    pub fn new(kind: ErrorKind, message: impl Into<String>, origin: Option<ErrorOrigin>) -> Self {
        ErrorBody {
            kind,
            message: message.into(),
            origin,
        }
    }
}

/// An error carrying an [`ErrorBody`]. Port of the `ProtocolError` class (which
/// extends `Error` with `message = errorBody.message`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError {
    pub error_body: ErrorBody,
}

impl ProtocolError {
    pub fn new(error_body: ErrorBody) -> Self {
        ProtocolError { error_body }
    }

    /// The error message (equal to `errorBody.message`).
    pub fn message(&self) -> &str {
        &self.error_body.message
    }

    /// The error kind (`errorBody.kind`). Port of the `kind` getter.
    pub fn kind(&self) -> ErrorKind {
        self.error_body.kind
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.error_body.message)
    }
}

impl Error for ProtocolError {}

/// Port of `TransformFailedBody` (`error.ts`'s `transformFailedBodySchema`
/// union) — the failure shape `CustomQueryTransformer#requestTransform`
/// returns when a `/query` transform request fails, distinguished by which
/// side produced the failure (`origin`) and why (`reason`).
#[derive(Debug, Clone, PartialEq)]
pub struct TransformFailedBody {
    pub reason: TransformFailedReason,
    /// The queryIDs of the queries that failed to transform.
    pub query_ids: Vec<String>,
    pub message: String,
    pub details: Option<JsonValue>,
}

/// The origin-discriminated `reason` (+ origin-specific extra fields) half
/// of `transformFailedBodySchema`'s union.
#[derive(Debug, Clone, PartialEq)]
pub enum TransformFailedReason {
    /// `origin: Server`, `reason: database | parse | internal`.
    Server(ErrorReason),
    /// `origin: ZeroCache`, `reason: http`.
    ZeroCacheHttp {
        status: f64,
        body_preview: Option<String>,
    },
    /// `origin: ZeroCache`, `reason: timeout | parse | internal`.
    ZeroCacheOther(ErrorReason),
}

impl TransformFailedReason {
    pub fn origin(&self) -> ErrorOrigin {
        match self {
            TransformFailedReason::Server(_) => ErrorOrigin::Server,
            TransformFailedReason::ZeroCacheHttp { .. }
            | TransformFailedReason::ZeroCacheOther(_) => ErrorOrigin::ZeroCache,
        }
    }
}

/// Port of `PushFailedBody` (`error.ts`'s `pushFailedBodySchema` union) — the
/// non-deprecated `['error', {...}]` shape a failed push surfaces (superseding
/// the deprecated top-level `pushErrorSchema` variants, which this port
/// deliberately does not model). Its `kind` is always
/// [`ErrorKind::PushFailed`]; failures are distinguished by which side produced
/// them (`origin`) and why (`reason`), mirroring [`TransformFailedBody`].
#[derive(Debug, Clone, PartialEq)]
pub struct PushFailedBody {
    pub reason: PushFailedReason,
    /// The mutationIDs of the mutations that failed to process — may be a
    /// subset of the mutationIDs in the request.
    pub mutation_ids: Vec<MutationId>,
    pub message: String,
    pub details: Option<JsonValue>,
}

/// The origin-discriminated `reason` (+ origin-specific extra fields) half of
/// `pushFailedBodySchema`'s union.
#[derive(Debug, Clone, PartialEq)]
pub enum PushFailedReason {
    /// `origin: Server`, `reason: database | parse | outOfOrderMutation |
    /// unsupportedPushVersion | internal`.
    Server(ErrorReason),
    /// `origin: ZeroCache`, `reason: http`.
    ZeroCacheHttp {
        status: f64,
        body_preview: Option<String>,
    },
    /// `origin: ZeroCache`, `reason: timeout | parse | internal`.
    ZeroCacheOther(ErrorReason),
}

impl PushFailedReason {
    pub fn origin(&self) -> ErrorOrigin {
        match self {
            PushFailedReason::Server(_) => ErrorOrigin::Server,
            PushFailedReason::ZeroCacheHttp { .. } | PushFailedReason::ZeroCacheOther(_) => {
                ErrorOrigin::ZeroCache
            }
        }
    }
}

impl PushFailedBody {
    /// The error kind for a push failure — always [`ErrorKind::PushFailed`]
    /// (`pushFailedErrorKindSchema` is a literal).
    pub fn kind(&self) -> ErrorKind {
        ErrorKind::PushFailed
    }

    /// The `origin` of the failure, derived from the `reason` variant.
    pub fn origin(&self) -> ErrorOrigin {
        self.reason.origin()
    }
}

/// Port of `backoffBodySchema` — the error shape the server sends to tell a
/// client to back off and reconnect (`Rebalance`/`Rehome`/`ServerOverloaded`).
/// The optional `min`/`max` backoff bounds and `reconnect_params` (query
/// parameters to attach to *only* the immediately-following reconnect) let the
/// server steer the client's reconnect. `origin`, when present, is always
/// [`ErrorOrigin::ZeroCache`].
#[derive(Debug, Clone, PartialEq)]
pub struct BackoffBody {
    pub kind: ErrorKind,
    pub message: String,
    pub min_backoff_ms: Option<f64>,
    pub max_backoff_ms: Option<f64>,
    pub reconnect_params: Option<BTreeMap<String, String>>,
    pub origin: Option<ErrorOrigin>,
}

impl BackoffBody {
    /// Whether `kind` is one of the three backoff error kinds this body is
    /// valid for (`backoffErrorKindSchema`'s literal union).
    pub fn is_backoff_kind(kind: ErrorKind) -> bool {
        matches!(
            kind,
            ErrorKind::Rebalance | ErrorKind::Rehome | ErrorKind::ServerOverloaded
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carries_body() {
        let e = ProtocolError::new(ErrorBody::new(
            ErrorKind::Internal,
            "boom",
            Some(ErrorOrigin::ZeroCache),
        ));
        assert_eq!(e.message(), "boom");
        assert_eq!(e.kind(), ErrorKind::Internal);
        assert_eq!(e.to_string(), "boom");
    }

    #[test]
    fn transform_failed_reason_origins() {
        assert_eq!(
            TransformFailedReason::Server(ErrorReason::Database).origin(),
            ErrorOrigin::Server
        );
        assert_eq!(
            TransformFailedReason::ZeroCacheHttp {
                status: 500.0,
                body_preview: None
            }
            .origin(),
            ErrorOrigin::ZeroCache
        );
        assert_eq!(
            TransformFailedReason::ZeroCacheOther(ErrorReason::Timeout).origin(),
            ErrorOrigin::ZeroCache
        );
    }

    #[test]
    fn transform_failed_body_carries_query_ids() {
        let body = TransformFailedBody {
            reason: TransformFailedReason::ZeroCacheOther(ErrorReason::Internal),
            query_ids: vec!["q1".to_string(), "q2".to_string()],
            message: "boom".to_string(),
            details: None,
        };
        assert_eq!(body.reason.origin(), ErrorOrigin::ZeroCache);
        assert_eq!(body.query_ids.len(), 2);
    }

    #[test]
    fn push_failed_reason_origins() {
        assert_eq!(
            PushFailedReason::Server(ErrorReason::OutOfOrderMutation).origin(),
            ErrorOrigin::Server
        );
        assert_eq!(
            PushFailedReason::ZeroCacheHttp {
                status: 503.0,
                body_preview: Some("upstream down".into()),
            }
            .origin(),
            ErrorOrigin::ZeroCache
        );
        assert_eq!(
            PushFailedReason::ZeroCacheOther(ErrorReason::Timeout).origin(),
            ErrorOrigin::ZeroCache
        );
    }

    #[test]
    fn backoff_kind_predicate_matches_only_backoff_kinds() {
        assert!(BackoffBody::is_backoff_kind(ErrorKind::Rebalance));
        assert!(BackoffBody::is_backoff_kind(ErrorKind::Rehome));
        assert!(BackoffBody::is_backoff_kind(ErrorKind::ServerOverloaded));
        assert!(!BackoffBody::is_backoff_kind(ErrorKind::Internal));
        assert!(!BackoffBody::is_backoff_kind(ErrorKind::PushFailed));
    }

    #[test]
    fn backoff_body_carries_bounds_and_reconnect_params() {
        let body = BackoffBody {
            kind: ErrorKind::ServerOverloaded,
            message: "slow down".to_string(),
            min_backoff_ms: Some(100.0),
            max_backoff_ms: Some(5000.0),
            reconnect_params: Some(BTreeMap::from([("shard".to_string(), "3".to_string())])),
            origin: Some(ErrorOrigin::ZeroCache),
        };
        assert_eq!(body.min_backoff_ms, Some(100.0));
        assert_eq!(
            body.reconnect_params.as_ref().unwrap().get("shard"),
            Some(&"3".to_string())
        );
        assert_eq!(body.origin, Some(ErrorOrigin::ZeroCache));
    }

    #[test]
    fn push_failed_body_carries_kind_origin_and_mutation_ids() {
        let body = PushFailedBody {
            reason: PushFailedReason::Server(ErrorReason::Database),
            mutation_ids: vec![
                MutationId {
                    id: 1.0,
                    client_id: "c1".into(),
                },
                MutationId {
                    id: 2.0,
                    client_id: "c1".into(),
                },
            ],
            message: "insert failed".to_string(),
            details: None,
        };
        assert_eq!(body.kind(), ErrorKind::PushFailed);
        assert_eq!(body.origin(), ErrorOrigin::Server);
        assert_eq!(body.mutation_ids.len(), 2);
    }
}
