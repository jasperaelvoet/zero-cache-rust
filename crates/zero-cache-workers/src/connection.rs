//! Port of the pure decision logic in `zero-cache/src/workers/connection.ts`
//! — the per-connection message-dispatch class that (despite living under
//! `src/workers`) is NOT a Node `Worker` thread: it runs on the same
//! event loop as everything else, dispatching already-parsed `Upstream`
//! messages to a `MessageHandler` and turning the `HandlerResult` back
//! into WebSocket sends/closes. There is no worker-thread/process
//! topology to decide here — that was a mischaracterization in an earlier
//! round's scope note; `Connection` is ordinary per-connection object
//! state, structurally close to what `zero-cache-server::ws_connection`
//! already handles at the transport layer.
//!
//! Scope: ports the PURE decision functions — protocol-version gating
//! (`init`'s version check), `HandlerResult`/`StreamResult`'s action
//! classification (`#handleMessageResult`), transient-socket-error
//! classification (`hasTransientSocketCode`/`isTransientSocketMessage`),
//! and `findProtocolError`'s cause-chain walk. NOT ported: the actual
//! `Connection` class (owns a live `ws`/`LogContext`/timers, wires into
//! `#proxyInbound`/`#proxyOutbound` stream piping) — that needs a real
//! `MessageHandler`/`Source<Downstream>` (view-syncer/pusher dispatch, not
//! yet unified behind one trait in this port) to be meaningful, and
//! `zero-cache-server::ws_connection` already covers the raw
//! accept/send/decode transport half. This module is the connection-
//! independent decision core a future `Connection` struct would call into.

use zero_cache_protocol::error::ErrorBody;
use zero_cache_protocol::error_kind::ErrorKind;
use zero_cache_protocol::error_origin::ErrorOrigin;
use zero_cache_protocol::protocol_version::{MIN_SERVER_SUPPORTED_SYNC_PROTOCOL, PROTOCOL_VERSION};

/// Port of `Connection#init`'s protocol-version check. `Ok(())` means the
/// connection may proceed (upstream sends the `connected` message on this
/// path); `Err(body)` means it must be closed with a `VersionNotSupported`
/// error.
pub fn check_protocol_version(client_protocol_version: i64) -> Result<(), ErrorBody> {
    if !(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL..=PROTOCOL_VERSION).contains(&client_protocol_version) {
        let who = if client_protocol_version > PROTOCOL_VERSION {
            "server"
        } else {
            "client"
        };
        return Err(ErrorBody::new(
            ErrorKind::VersionNotSupported,
            format!(
                "server is at sync protocol v{PROTOCOL_VERSION} and does not support v{client_protocol_version}. The {who} must be updated to a newer release."
            ),
            Some(ErrorOrigin::ZeroCache),
        ));
    }
    Ok(())
}

/// Which downstream source a `StreamResult` is for. Port of
/// `StreamResult['source']`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSource {
    ViewSyncer,
    Pusher,
}

/// Port of `HandlerResult`, with `stream`/`source` split out (this crate
/// doesn't have a concrete `Source<Downstream>` type to carry — see module
/// doc) — callers pattern-match on `Stream(StreamSource)` and attach their
/// own stream handle.
#[derive(Debug, Clone, PartialEq)]
pub enum HandlerResult {
    Ok,
    Fatal(ErrorBody),
    Transient(Vec<ErrorBody>),
    Stream(StreamSource),
}

/// The action a `Connection` should take after receiving one
/// `HandlerResult`. Port of `#handleMessageResult`'s dispatch, minus the
/// actual I/O (`#closeWithError`/stream assignment/`sendError`) — this is
/// what upstream's `switch` decides, factored out so it's testable without
/// a live connection.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionAction {
    /// No action needed (`'ok'`).
    None,
    /// Close the connection with this error (`'fatal'`).
    CloseWithError(ErrorBody),
    /// Attach an outbound stream for the given source and start proxying
    /// it (`'stream'`) — the caller is responsible for the
    /// already-set-for-this-connection assertion upstream makes (this
    /// module has no per-connection state to check it against).
    AttachStream(StreamSource),
    /// Send each error to the client without closing (`'transient'`).
    SendErrors(Vec<ErrorBody>),
}

/// Port of `#handleMessageResult`'s classification.
pub fn classify_handler_result(result: HandlerResult) -> ConnectionAction {
    match result {
        HandlerResult::Ok => ConnectionAction::None,
        HandlerResult::Fatal(body) => ConnectionAction::CloseWithError(body),
        HandlerResult::Stream(source) => ConnectionAction::AttachStream(source),
        HandlerResult::Transient(errors) => ConnectionAction::SendErrors(errors),
    }
}

/// System error codes that indicate transient socket conditions. Port of
/// `TRANSIENT_SOCKET_ERROR_CODES`.
const TRANSIENT_SOCKET_ERROR_CODES: [&str; 3] = ["EPIPE", "ECONNRESET", "ECANCELED"];

/// Port of `hasTransientSocketCode` (minus the "does this object even have
/// a `code` property" check, which a caller does before extracting the
/// code string — this crate has no ambient `unknown`-typed error object to
/// probe).
pub fn has_transient_socket_code(code: &str) -> bool {
    TRANSIENT_SOCKET_ERROR_CODES
        .iter()
        .any(|c| c.eq_ignore_ascii_case(code))
}

/// Error messages that indicate transient socket conditions without a
/// standard error code. Port of `TRANSIENT_SOCKET_MESSAGE_PATTERNS`.
const TRANSIENT_SOCKET_MESSAGE_PATTERNS: [&str; 1] =
    ["socket was closed while data was being compressed"];

/// Port of `isTransientSocketMessage`.
pub fn is_transient_socket_message(message: &str) -> bool {
    let lower = message.to_lowercase();
    TRANSIENT_SOCKET_MESSAGE_PATTERNS
        .iter()
        .any(|pattern| lower.contains(pattern))
}

/// Port of `findProtocolError`: walks an error's `source()` chain looking
/// for a `zero_cache_protocol::error::ProtocolError`. Rust's
/// `std::error::Error::source()` chain is the structural analog of JS's
/// `Error.cause` chain upstream walks.
pub fn find_protocol_error<'a>(
    error: &'a (dyn std::error::Error + 'static),
) -> Option<&'a zero_cache_protocol::error::ProtocolError> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(err) = current {
        if let Some(protocol_error) =
            err.downcast_ref::<zero_cache_protocol::error::ProtocolError>()
        {
            return Some(protocol_error);
        }
        current = err.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::error::ProtocolError;

    #[test]
    fn protocol_version_within_range_is_ok() {
        assert!(check_protocol_version(PROTOCOL_VERSION).is_ok());
        assert!(check_protocol_version(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL).is_ok());
    }

    #[test]
    fn protocol_version_too_new_errors_blaming_server() {
        let err = check_protocol_version(PROTOCOL_VERSION + 1).unwrap_err();
        assert_eq!(err.kind, ErrorKind::VersionNotSupported);
        assert!(
            err.message.contains("server must be updated"),
            "{}",
            err.message
        );
    }

    #[test]
    fn protocol_version_too_old_errors_blaming_client() {
        let err = check_protocol_version(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL - 1).unwrap_err();
        assert_eq!(err.kind, ErrorKind::VersionNotSupported);
        assert!(
            err.message.contains("client must be updated"),
            "{}",
            err.message
        );
    }

    #[test]
    fn classify_ok_is_none_action() {
        assert_eq!(
            classify_handler_result(HandlerResult::Ok),
            ConnectionAction::None
        );
    }

    #[test]
    fn classify_fatal_closes_with_error() {
        let body = ErrorBody::new(ErrorKind::Internal, "boom", None);
        assert_eq!(
            classify_handler_result(HandlerResult::Fatal(body.clone())),
            ConnectionAction::CloseWithError(body)
        );
    }

    #[test]
    fn classify_stream_attaches_stream() {
        assert_eq!(
            classify_handler_result(HandlerResult::Stream(StreamSource::Pusher)),
            ConnectionAction::AttachStream(StreamSource::Pusher)
        );
    }

    #[test]
    fn classify_transient_sends_errors_without_closing() {
        let bodies = vec![
            ErrorBody::new(ErrorKind::Internal, "a", None),
            ErrorBody::new(ErrorKind::Internal, "b", None),
        ];
        assert_eq!(
            classify_handler_result(HandlerResult::Transient(bodies.clone())),
            ConnectionAction::SendErrors(bodies)
        );
    }

    #[test]
    fn transient_socket_codes_are_case_insensitive() {
        assert!(has_transient_socket_code("epipe"));
        assert!(has_transient_socket_code("ECONNRESET"));
        assert!(has_transient_socket_code("ECanceled"));
        assert!(!has_transient_socket_code("ENOENT"));
    }

    #[test]
    fn transient_socket_message_pattern_matches_substring_case_insensitively() {
        assert!(is_transient_socket_message(
            "Socket Was Closed While Data Was Being Compressed, sorry"
        ));
        assert!(!is_transient_socket_message("some other error"));
    }

    #[derive(Debug)]
    struct WrapperError {
        source: ProtocolError,
    }
    impl std::fmt::Display for WrapperError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "wrapped")
        }
    }
    impl std::error::Error for WrapperError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.source)
        }
    }

    #[test]
    fn find_protocol_error_finds_it_directly() {
        let err = ProtocolError::new(ErrorBody::new(ErrorKind::Internal, "boom", None));
        let found = find_protocol_error(&err).unwrap();
        assert_eq!(found.message(), "boom");
    }

    #[test]
    fn find_protocol_error_walks_source_chain() {
        let inner = ProtocolError::new(ErrorBody::new(ErrorKind::Internal, "root cause", None));
        let wrapper = WrapperError { source: inner };
        let found = find_protocol_error(&wrapper).unwrap();
        assert_eq!(found.message(), "root cause");
    }

    #[test]
    fn find_protocol_error_returns_none_when_absent() {
        #[derive(Debug)]
        struct PlainError;
        impl std::fmt::Display for PlainError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "plain")
            }
        }
        impl std::error::Error for PlainError {}

        let err = PlainError;
        assert!(find_protocol_error(&err).is_none());
    }
}
