//! Port of `services/change-streamer/error-type-enum.ts` and
//! `change-streamer.ts`'s `errorTypeToReadableName`. Found while sweeping
//! `change-streamer/*` for real gaps — small but genuinely unported (this
//! crate's `broadcast.rs`/`change_streamer_forwarder.rs`/`notifier.rs`
//! already cover the rest of what's tractable in this directory; several
//! other files (`change-streamer.ts`'s interfaces, `snapshot.ts`,
//! `backup-monitor.ts`) are pure `valita` schema/interface declarations
//! with no logic, correctly out of scope).

/// Port of `error-type-enum.ts`'s numeric constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    Unknown,
    WrongReplicaVersion,
    WatermarkTooOld,
}

/// Port of `errorTypeToReadableName`.
pub fn error_type_to_readable_name(val: ErrorType) -> &'static str {
    match val {
        ErrorType::WrongReplicaVersion => "WrongReplicaVersion",
        ErrorType::WatermarkTooOld => "WatermarkTooOld",
        ErrorType::Unknown => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_every_variant_to_its_readable_name() {
        assert_eq!(
            error_type_to_readable_name(ErrorType::WrongReplicaVersion),
            "WrongReplicaVersion"
        );
        assert_eq!(
            error_type_to_readable_name(ErrorType::WatermarkTooOld),
            "WatermarkTooOld"
        );
        assert_eq!(error_type_to_readable_name(ErrorType::Unknown), "Unknown");
    }
}
