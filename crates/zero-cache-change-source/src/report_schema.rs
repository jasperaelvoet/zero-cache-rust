//! Port of `zero-cache/src/services/replicator/reporter/report-schema.ts`.
//!
//! The lag-report payloads carried in change-source status handshakes, used
//! for end-to-end latency metrics. All fields are millisecond timestamps
//! (`f64`, matching the TS `v.number()`s). `ChangeSourceReport` is the shape
//! [`crate::status::DownstreamStatus`]'s `lag_report` carries.

/// Port of `changeSourceTimingsSchema`: the timing marks for one change as it
/// moves through the change-source.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeSourceTimings {
    pub send_time_ms: f64,
    pub commit_time_ms: f64,
    pub receive_time_ms: f64,
}

/// Port of `changeSourceReportSchema`: the last observed timings plus the next
/// scheduled send time.
#[derive(Debug, Clone, PartialEq)]
pub struct ChangeSourceReport {
    pub last_timings: ChangeSourceTimings,
    pub next_send_time_ms: f64,
}

/// Port of `replicationTimingsSchema` (`changeSourceTimingsSchema` extended
/// with the replicate mark).
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicationTimings {
    pub send_time_ms: f64,
    pub commit_time_ms: f64,
    pub receive_time_ms: f64,
    pub replicate_time_ms: f64,
}

/// Port of `replicationReportSchema`: the last replication timings (optional,
/// unlike the change-source report's required `last_timings`) plus the next
/// scheduled send time.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicationReport {
    pub last_timings: Option<ReplicationTimings>,
    pub next_send_time_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn change_source_report_holds_last_timings_and_next_send() {
        let report = ChangeSourceReport {
            last_timings: ChangeSourceTimings {
                send_time_ms: 1.0,
                commit_time_ms: 2.0,
                receive_time_ms: 3.0,
            },
            next_send_time_ms: 4.0,
        };
        assert_eq!(report.last_timings.commit_time_ms, 2.0);
        assert_eq!(report.next_send_time_ms, 4.0);
    }

    #[test]
    fn replication_report_last_timings_is_optional() {
        let report = ReplicationReport {
            last_timings: None,
            next_send_time_ms: 9.0,
        };
        assert_eq!(report.last_timings, None);
        assert_eq!(report.next_send_time_ms, 9.0);
    }
}
