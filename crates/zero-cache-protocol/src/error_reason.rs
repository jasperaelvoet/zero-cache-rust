//! Port of `zero-protocol/src/error-reason-enum.ts`/`error-reason.ts`.

/// The reason an API-server/mutation error occurred. Port of `ErrorReason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorReason {
    Database,
    Parse,
    OutOfOrderMutation,
    UnsupportedPushVersion,
    Internal,
    Http,
    Timeout,
}

impl ErrorReason {
    /// The wire string for this reason.
    pub fn as_str(self) -> &'static str {
        use ErrorReason::*;
        match self {
            Database => "database",
            Parse => "parse",
            OutOfOrderMutation => "oooMutation",
            UnsupportedPushVersion => "unsupportedPushVersion",
            Internal => "internal",
            Http => "http",
            Timeout => "timeout",
        }
    }

    pub fn from_str(s: &str) -> Option<ErrorReason> {
        use ErrorReason::*;
        Some(match s {
            "database" => Database,
            "parse" => Parse,
            "oooMutation" => OutOfOrderMutation,
            "unsupportedPushVersion" => UnsupportedPushVersion,
            "internal" => Internal,
            "http" => Http,
            "timeout" => Timeout,
            _ => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_strings() {
        for reason in [
            ErrorReason::Database,
            ErrorReason::Parse,
            ErrorReason::OutOfOrderMutation,
            ErrorReason::UnsupportedPushVersion,
            ErrorReason::Internal,
            ErrorReason::Http,
            ErrorReason::Timeout,
        ] {
            assert_eq!(ErrorReason::from_str(reason.as_str()), Some(reason));
        }
        assert_eq!(ErrorReason::from_str("bogus"), None);
    }
}
