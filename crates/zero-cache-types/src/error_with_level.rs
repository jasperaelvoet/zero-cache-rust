//! Port of `zero-cache/src/types/error-with-level.ts`.
//!
//! Associates a log level with protocol errors and classifies arbitrary errors
//! into a [`LogLevel`] for logging.

use zero_cache_protocol::error::{ErrorBody, ProtocolError};
use zero_cache_protocol::{ErrorKind, ErrorOrigin};

/// Log levels, mirroring `LogLevel` from `@rocicorp/logger`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// A [`ProtocolError`] annotated with an explicit log level. Port of
/// `ProtocolErrorWithLevel` (which extends `ProtocolError`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolErrorWithLevel {
    pub error_body: ErrorBody,
    pub log_level: LogLevel,
}

impl ProtocolErrorWithLevel {
    pub fn new(error_body: ErrorBody, log_level: LogLevel) -> Self {
        ProtocolErrorWithLevel {
            error_body,
            log_level,
        }
    }

    /// The error message (equal to `errorBody.message`).
    pub fn message(&self) -> &str {
        &self.error_body.message
    }
}

/// An arbitrary error value, modeling the TypeScript `unknown` accepted by
/// [`get_log_level`] and [`wrap_with_protocol_error`]. Rust lacks an untyped
/// error, so the three cases the TS code distinguishes are made explicit.
pub enum ErrorValue {
    /// A `ProtocolErrorWithLevel`.
    WithLevel(ProtocolErrorWithLevel),
    /// A plain `ProtocolError`.
    Protocol(ProtocolError),
    /// Any other error, carrying its message (as `getErrorMessage` would yield).
    Other(String),
}

/// Returns the log level to use for `error`. Port of `getLogLevel`:
/// - `ProtocolErrorWithLevel` -> its explicit level,
/// - any other `ProtocolError` -> `warn`,
/// - anything else -> `error`.
pub fn get_log_level(error: &ErrorValue) -> LogLevel {
    match error {
        ErrorValue::WithLevel(e) => e.log_level,
        ErrorValue::Protocol(_) => LogLevel::Warn,
        ErrorValue::Other(_) => LogLevel::Error,
    }
}

/// Wraps `error` as a [`ProtocolError`], passing existing protocol errors
/// through. Port of `wrapWithProtocolError`.
pub fn wrap_with_protocol_error(error: ErrorValue) -> ProtocolError {
    match error {
        ErrorValue::Protocol(e) => e,
        ErrorValue::WithLevel(e) => ProtocolError::new(e.error_body),
        ErrorValue::Other(message) => ProtocolError::new(ErrorBody::new(
            ErrorKind::Internal,
            message,
            Some(ErrorOrigin::ZeroCache),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_error_with_specified_log_level() {
        let error = ProtocolErrorWithLevel::new(
            ErrorBody::new(
                ErrorKind::Internal,
                "test message",
                Some(ErrorOrigin::ZeroCache),
            ),
            LogLevel::Warn,
        );
        assert_eq!(error.message(), "test message");
        assert_eq!(error.log_level, LogLevel::Warn);
    }

    #[test]
    fn returns_explicit_level_from_with_level() {
        let error = ProtocolErrorWithLevel::new(
            ErrorBody::new(
                ErrorKind::Internal,
                "explicit",
                Some(ErrorOrigin::ZeroCache),
            ),
            LogLevel::Info,
        );
        assert_eq!(get_log_level(&ErrorValue::WithLevel(error)), LogLevel::Info);
    }

    #[test]
    fn returns_warn_for_protocol_error() {
        let error = ProtocolError::new(ErrorBody::new(
            ErrorKind::Internal,
            "protocol",
            Some(ErrorOrigin::Server),
        ));
        assert_eq!(get_log_level(&ErrorValue::Protocol(error)), LogLevel::Warn);
    }

    #[test]
    fn defaults_to_error_for_other_values() {
        assert_eq!(
            get_log_level(&ErrorValue::Other("boom".to_string())),
            LogLevel::Error
        );
    }
}
