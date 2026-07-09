//! Port of `zero-cache/src/db/warmup.ts`'s pure decision logic —
//! `warmupConnections` pre-warms a Postgres connection pool by firing a
//! batch of throwaway queries, then pings again to measure average
//! latency and decides whether that latency is concerning enough to warn
//! about. The actual queries (`db\`SELECT 1\`.simple().execute()`,
//! `performance.now()` timing) are real I/O/timing this port doesn't
//! drive here — a caller measures its own ping times (e.g. via
//! `tokio-postgres`) and passes them in, matching this port's convention
//! of taking ambient/IO-derived values as explicit parameters.

/// Port of `Math.min(db.options.max, MAX_WARMUP_CONNECTIONS)`.
pub const MAX_WARMUP_CONNECTIONS: u32 = 5;

pub fn warmup_connection_count(configured_max: u32) -> u32 {
    configured_max.min(MAX_WARMUP_CONNECTIONS)
}

/// The log level `warmupConnections` would use for its ping-time report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmupLogLevel {
    Info,
    /// Port of the `average >= 10` branch, which also logs the extra
    /// "ideal db ping time is < 5 ms" line.
    Warn,
}

/// Port of the average-ping-time + log-level decision at the end of
/// `warmupConnections`. Returns `None` if `ping_times_ms` is empty (`0/0`
/// would be `NaN` in JS, which is `< 10` so upstream would actually still
/// log at `info` with a "NaN ms" message — this port treats the
/// genuinely-nonsensical empty-input case as an explicit `None` instead of
/// reproducing that NaN quirk, since no real caller passes an empty ping
/// list).
pub fn warmup_ping_report(ping_times_ms: &[f64]) -> Option<(f64, WarmupLogLevel)> {
    if ping_times_ms.is_empty() {
        return None;
    }
    let average = ping_times_ms.iter().sum::<f64>() / ping_times_ms.len() as f64;
    let level = if average >= 10.0 {
        WarmupLogLevel::Warn
    } else {
        WarmupLogLevel::Info
    };
    Some((average, level))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warmup_connection_count_is_clamped_to_the_max() {
        assert_eq!(warmup_connection_count(20), 5);
        assert_eq!(warmup_connection_count(3), 3);
        assert_eq!(warmup_connection_count(5), 5);
    }

    #[test]
    fn ping_report_averages_and_uses_info_below_10ms() {
        let (avg, level) = warmup_ping_report(&[2.0, 4.0, 6.0]).unwrap();
        assert_eq!(avg, 4.0);
        assert_eq!(level, WarmupLogLevel::Info);
    }

    #[test]
    fn ping_report_warns_at_or_above_10ms() {
        let (avg, level) = warmup_ping_report(&[10.0]).unwrap();
        assert_eq!(avg, 10.0);
        assert_eq!(level, WarmupLogLevel::Warn);

        let (avg, level) = warmup_ping_report(&[9.999]).unwrap();
        assert_eq!(avg, 9.999);
        assert_eq!(level, WarmupLogLevel::Info);
    }

    #[test]
    fn ping_report_is_none_for_an_empty_list() {
        assert_eq!(warmup_ping_report(&[]), None);
    }
}
