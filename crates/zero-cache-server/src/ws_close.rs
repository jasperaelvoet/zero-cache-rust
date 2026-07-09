//! Port of the pure half of `zero-cache/src/types/ws.ts`'s `closeWithError`
//! — computing the WebSocket close code + reason a caller sends, closing
//! (partially) the previously-`⬜`-marked `ws`/`websocket_handoff` gap in
//! `zero-cache-types`'s module backlog (that table having gone stale —
//! `url_params`/`processes` were also marked `⬜` there despite being
//! ported rounds ago as `zero-cache-workers::url_params`/`worker_message`;
//! corrected in PORTING.md alongside this addition).
//!
//! Scope: `closeWithError` itself calls `ws.close(code, reason)` on a live
//! `WebSocket` — this module ports just the code/reason COMPUTATION
//! (`elide`-truncated error message, using the already-ported
//! `zero_cache_types::strings::elide`), leaving the actual
//! `tokio-tungstenite` close call to a caller (`zero-cache-server::
//! ws_connection` already owns real socket I/O). `sendPingsForLiveness`/
//! `expectPingsForLiveness` (heartbeat timers wired to a live socket's
//! ping/pong/message events) are NOT ported — genuinely stateful,
//! real-timer-plus-live-socket logic with no pure core to extract, unlike
//! `closeWithError`.

use zero_cache_types::strings::elide;

/// WebSocket close codes. Port of `PROTOCOL_ERROR`/`INTERNAL_ERROR`.
/// See <https://github.com/Luka967/websocket-close-codes>.
pub const PROTOCOL_ERROR: u16 = 1002;
pub const INTERNAL_ERROR: u16 = 1011;

/// The close code + reason to send. Port of `closeWithError`'s computed
/// arguments to `ws.close(code, reason)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseWithError {
    pub code: u16,
    pub reason: String,
}

/// Port of `closeWithError`'s code/reason computation: the close code
/// defaults to [`INTERNAL_ERROR`], and the reason is the stringified error
/// truncated to fit the WebSocket close-reason limit (close messages must
/// be `<= 123` bytes per the WebSocket spec/MDN).
pub fn close_with_error(err_message: &str, code: Option<u16>) -> CloseWithError {
    CloseWithError {
        code: code.unwrap_or(INTERNAL_ERROR),
        reason: elide(err_message, 123),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_internal_error_code() {
        let result = close_with_error("boom", None);
        assert_eq!(result.code, INTERNAL_ERROR);
    }

    #[test]
    fn honors_an_explicit_code() {
        let result = close_with_error("boom", Some(PROTOCOL_ERROR));
        assert_eq!(result.code, PROTOCOL_ERROR);
    }

    #[test]
    fn short_message_passes_through_unelided() {
        let result = close_with_error("boom", None);
        assert_eq!(result.reason, "boom");
    }

    #[test]
    fn long_message_gets_elided_to_fit_the_123_byte_close_reason_limit() {
        let long_message = "x".repeat(500);
        let result = close_with_error(&long_message, None);
        assert!(
            result.reason.len() <= 123,
            "reason must fit the WebSocket close-reason byte limit, got {} bytes",
            result.reason.len()
        );
    }
}
