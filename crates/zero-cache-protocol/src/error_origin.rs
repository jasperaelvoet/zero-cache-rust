//! Port of `zero-protocol/src/error-origin-enum.ts`.

use std::fmt;

/// The origin of a protocol error. Port of `ErrorOrigin`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorOrigin {
    Client,
    Server,
    ZeroCache,
}

impl ErrorOrigin {
    /// The wire string for this origin.
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorOrigin::Client => "client",
            ErrorOrigin::Server => "server",
            ErrorOrigin::ZeroCache => "zeroCache",
        }
    }
}

impl fmt::Display for ErrorOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_strings() {
        assert_eq!(ErrorOrigin::Client.as_str(), "client");
        assert_eq!(ErrorOrigin::Server.as_str(), "server");
        assert_eq!(ErrorOrigin::ZeroCache.as_str(), "zeroCache");
    }
}
