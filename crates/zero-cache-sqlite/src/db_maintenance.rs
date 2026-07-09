//! Port of the pure decision logic inside `zqlite/src/db.ts`'s `Database`
//! class — `mb` (byte-count formatting) and the `compact` method's
//! actual decision of whether/how to proceed, extracted as a standalone
//! function taking the pragma-read values it needs as explicit parameters
//! rather than reading them off a live `Database` itself.
//!
//! Scope: NOT ported — `Database`/`Statement`/`LoggingIterableIterator`
//! themselves (thin tracing/slow-query-logging wrappers around
//! `@rocicorp/zero-sqlite3`, all `performance.now()`/`LogContext`/OTel-span
//! side effects, no decision logic beyond what's extracted here) and the
//! actual pragma-driving side of `compact` (issuing `PRAGMA freelist_count`/
//! `auto_vacuum`/`incremental_vacuum` against a live connection) — this
//! port's `StatementRunner::pragma` already exists to issue those, a caller
//! just needs to combine it with [`decide_compaction`] below.

/// Port of the `AUTO_VACUUM_INCREMENTAL` SQLite pragma value (see
/// <https://www.sqlite.org/pragma.html#pragma_auto_vacuum>).
pub const AUTO_VACUUM_INCREMENTAL: i64 = 2;

/// Port of `mb`: formats a byte count as a `"X.XX"` megabyte string.
pub fn mb(bytes: f64) -> String {
    format!("{:.2}", bytes / (1024.0 * 1024.0))
}

/// Port of the decision `Database::compact` makes before actually issuing
/// `PRAGMA incremental_vacuum` — whether there's enough freeable space to
/// bother, and whether the database is even in a mode that supports
/// incremental vacuuming at all.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CompactionDecision {
    /// Not enough freeable space to bother (`freeable_bytes` for the log
    /// message upstream emits at debug level).
    NotEnoughFreeableSpace { freeable_bytes: f64 },
    /// Enough freeable space, but `auto_vacuum` isn't in `INCREMENTAL`
    /// mode, so `incremental_vacuum` wouldn't do anything (upstream warns).
    WrongAutoVacuumMode {
        freeable_bytes: f64,
        auto_vacuum_mode: i64,
    },
    /// Go ahead and run `PRAGMA incremental_vacuum`.
    Proceed,
}

/// Port of `compact`'s decision (not its pragma-issuing side): given the
/// freelist page count (from `PRAGMA freelist_count`), the page size (from
/// `PRAGMA page_size`, captured once at `Database` construction), the
/// caller's `freeable_bytes_threshold`, and the current `auto_vacuum` mode
/// (from `PRAGMA auto_vacuum`), decides whether compaction should proceed.
pub fn decide_compaction(
    freelist_count: i64,
    page_size: i64,
    freeable_bytes_threshold: f64,
    auto_vacuum_mode: i64,
) -> CompactionDecision {
    let freeable_bytes = (freelist_count * page_size) as f64;
    if freeable_bytes < freeable_bytes_threshold {
        return CompactionDecision::NotEnoughFreeableSpace { freeable_bytes };
    }
    if auto_vacuum_mode != AUTO_VACUUM_INCREMENTAL {
        return CompactionDecision::WrongAutoVacuumMode {
            freeable_bytes,
            auto_vacuum_mode,
        };
    }
    CompactionDecision::Proceed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mb_formats_to_two_decimal_places() {
        assert_eq!(mb(1024.0 * 1024.0), "1.00");
        assert_eq!(mb(1536.0 * 1024.0), "1.50");
        assert_eq!(mb(0.0), "0.00");
    }

    #[test]
    fn decide_compaction_skips_when_not_enough_freeable_space() {
        let decision = decide_compaction(1, 4096, 1_000_000.0, AUTO_VACUUM_INCREMENTAL);
        assert_eq!(
            decision,
            CompactionDecision::NotEnoughFreeableSpace {
                freeable_bytes: 4096.0
            }
        );
    }

    #[test]
    fn decide_compaction_warns_on_wrong_auto_vacuum_mode() {
        let decision = decide_compaction(1000, 4096, 100.0, 0);
        assert_eq!(
            decision,
            CompactionDecision::WrongAutoVacuumMode {
                freeable_bytes: 1000.0 * 4096.0,
                auto_vacuum_mode: 0
            }
        );
    }

    #[test]
    fn decide_compaction_proceeds_when_eligible() {
        let decision = decide_compaction(1000, 4096, 100.0, AUTO_VACUUM_INCREMENTAL);
        assert_eq!(decision, CompactionDecision::Proceed);
    }
}
