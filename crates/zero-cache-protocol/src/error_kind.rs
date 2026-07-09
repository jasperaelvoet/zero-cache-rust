//! Port of `zero-protocol/src/error-kind-enum.ts`.
//!
//! Note: metric names depend on these string values, so changes here likely
//! require corresponding dashboard changes upstream.

use std::fmt;

/// The kind of a protocol error. Port of `ErrorKind`. Each variant serializes
/// to the identical PascalCase string used in the TS source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// App rejected the client's auth token (deprecated).
    AuthInvalidated,
    /// zero-cache no longer has CVR state for the client.
    ClientNotFound,
    /// Handshake metadata is invalid or incomplete.
    InvalidConnectionRequest,
    /// Client's base cookie is ahead of the replica snapshot.
    InvalidConnectionRequestBaseCookie,
    /// Client's last mutation ID is ahead of the replica.
    InvalidConnectionRequestLastMutationID,
    /// The server deleted the client.
    InvalidConnectionRequestClientDeleted,
    /// Upstream message failed schema validation or JSON parsing.
    InvalidMessage,
    /// Push payload could not be applied.
    InvalidPush,
    /// Push failed during processing.
    PushFailed,
    /// CRUD mutator failure (deprecated).
    MutationFailed,
    /// CRUD mutator rate limit (deprecated).
    MutationRateLimited,
    /// Cache is rebalancing ownership.
    Rebalance,
    /// Replica ownership moved.
    Rehome,
    /// Transform failed during processing.
    TransformFailed,
    /// Unauthorized client request.
    Unauthorized,
    /// Client requested unsupported protocol version.
    VersionNotSupported,
    /// Client schema hash or version outside the zero-cache window.
    SchemaVersionNotSupported,
    /// zero-cache is overloaded.
    ServerOverloaded,
    /// Unhandled zero-cache exception.
    Internal,
}

impl ErrorKind {
    /// The wire string for this kind (identical to the variant name).
    pub fn as_str(self) -> &'static str {
        use ErrorKind::*;
        match self {
            AuthInvalidated => "AuthInvalidated",
            ClientNotFound => "ClientNotFound",
            InvalidConnectionRequest => "InvalidConnectionRequest",
            InvalidConnectionRequestBaseCookie => "InvalidConnectionRequestBaseCookie",
            InvalidConnectionRequestLastMutationID => "InvalidConnectionRequestLastMutationID",
            InvalidConnectionRequestClientDeleted => "InvalidConnectionRequestClientDeleted",
            InvalidMessage => "InvalidMessage",
            InvalidPush => "InvalidPush",
            PushFailed => "PushFailed",
            MutationFailed => "MutationFailed",
            MutationRateLimited => "MutationRateLimited",
            Rebalance => "Rebalance",
            Rehome => "Rehome",
            TransformFailed => "TransformFailed",
            Unauthorized => "Unauthorized",
            VersionNotSupported => "VersionNotSupported",
            SchemaVersionNotSupported => "SchemaVersionNotSupported",
            ServerOverloaded => "ServerOverloaded",
            Internal => "Internal",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_strings() {
        assert_eq!(ErrorKind::Internal.as_str(), "Internal");
        assert_eq!(ErrorKind::AuthInvalidated.as_str(), "AuthInvalidated");
        assert_eq!(
            ErrorKind::InvalidConnectionRequestLastMutationID.as_str(),
            "InvalidConnectionRequestLastMutationID"
        );
    }
}
