//! Port of `zero-cache/src/services/change-source/protocol/current/status.ts`.
//!
//! Status messages exchanged between `zero-cache` and the change-source for
//! ack/lag-reporting handshakes.

use crate::downstream::Commit;
use crate::report_schema::ChangeSourceReport;

/// A downstream status payload. Port of `DownstreamStatus`. `ack` defaults to
/// `true` (matching the TS schema's `.optional(() => true)`).
#[derive(Debug, Clone, PartialEq)]
pub struct DownstreamStatus {
    pub ack: bool,
    /// Lag report for end-to-end latency metrics. Port of the optional
    /// `lagReport` (`changeSourceReportSchema`).
    pub lag_report: Option<ChangeSourceReport>,
}

impl Default for DownstreamStatus {
    fn default() -> Self {
        DownstreamStatus {
            ack: true,
            lag_report: None,
        }
    }
}

/// A `["status", DownstreamStatus, {watermark}]` envelope. Port of
/// `downstreamStatusMessageSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct DownstreamStatusMessage {
    pub status: DownstreamStatus,
    pub watermark: String,
}

/// The payload of an upstream status message: either an echoed
/// [`DownstreamStatus`] (acking) or a [`Commit`] (acknowledging a completed
/// transaction). Port of the `v.union(downstreamStatusSchema, commitSchema)`
/// in `upstreamStatusMessageSchema`.
#[derive(Debug, Clone, PartialEq)]
pub enum UpstreamStatusPayload {
    Status(DownstreamStatus),
    Commit(Commit),
}

/// A `["status", payload, {watermark}]` envelope sent upstream. Port of
/// `upstreamStatusMessageSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct UpstreamStatusMessage {
    pub payload: UpstreamStatusPayload,
    pub watermark: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downstream_status_defaults_ack_true() {
        assert!(DownstreamStatus::default().ack);
    }

    #[test]
    fn upstream_status_can_carry_commit_or_status() {
        let commit_msg = UpstreamStatusMessage {
            payload: UpstreamStatusPayload::Commit(Commit {
                watermark: "01".into(),
            }),
            watermark: "01".into(),
        };
        assert!(matches!(
            commit_msg.payload,
            UpstreamStatusPayload::Commit(_)
        ));

        let status_msg = UpstreamStatusMessage {
            payload: UpstreamStatusPayload::Status(DownstreamStatus::default()),
            watermark: "02".into(),
        };
        assert!(matches!(
            status_msg.payload,
            UpstreamStatusPayload::Status(_)
        ));
    }
}
