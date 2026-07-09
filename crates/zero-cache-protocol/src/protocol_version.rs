//! Port of `zero-protocol/src/protocol-version.ts`.

/// The current sync/AST wire-protocol version. Port of `PROTOCOL_VERSION`.
/// Kept in sync with upstream's history comment (see that file) — bump
/// this alongside upstream, don't re-derive independently.
pub const PROTOCOL_VERSION: i64 = 51;

/// The minimum server-supported sync protocol version. Port of
/// `MIN_SERVER_SUPPORTED_SYNC_PROTOCOL`.
pub const MIN_SERVER_SUPPORTED_SYNC_PROTOCOL: i64 = 30;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_supported_is_below_current() {
        assert!(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL < PROTOCOL_VERSION);
    }
}
