//! Structured, levelled logging — the port of zero-cache's `ZERO_LOG_*`
//! behaviour (`@rocicorp/logger`'s console sink).
//!
//! Honors:
//!   * `ZERO_LOG_LEVEL`  — `debug`|`info`|`warn`|`error` (filters output).
//!   * `ZERO_LOG_FORMAT` — `text` (human) or `json` (one object per line, the
//!     shape zero emits: `{"level","pid","worker","workerIndex","message"}`,
//!     for ECS/CloudWatch-style aggregation).
//!
//! Initialize once at startup with [`init`]; then use [`info!`]/[`warn!`]/
//! [`error!`]/[`debug!`]. Output goes to stderr (matching zero + 12-factor).

use std::sync::OnceLock;

/// Severity, ordered most-severe (`Error`) to least (`Debug`). A configured
/// level shows itself and everything more severe.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Level {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
}

impl Level {
    pub fn parse(s: &str) -> Level {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" => Level::Error,
            "warn" | "warning" => Level::Warn,
            "debug" => Level::Debug,
            _ => Level::Info,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    Text,
    Json,
}

struct Config {
    level: Level,
    format: Format,
    worker: String,
    worker_index: u32,
    pid: u32,
}

static CONFIG: OnceLock<Config> = OnceLock::new();

/// Installs the process-wide logger. Idempotent (first call wins). `worker` is
/// the role name that appears in logs (e.g. `replicator`, `view-syncer`).
pub fn init(level: &str, format: &str, worker: &str) {
    let json = format.trim().eq_ignore_ascii_case("json");
    let _ = CONFIG.set(Config {
        level: Level::parse(level),
        format: if json { Format::Json } else { Format::Text },
        worker: worker.to_string(),
        worker_index: 0, // single-process port; upstream uses this per sync worker
        pid: std::process::id(),
    });
}

/// Whether a message at `level` would be emitted under the current config.
/// (Cheap gate so callers can skip building expensive messages.)
pub fn enabled(level: Level) -> bool {
    let cfg = CONFIG.get();
    let configured = cfg.map(|c| c.level).unwrap_or(Level::Info);
    level <= configured
}

/// Emits one log message at `level` (no-op if filtered). Matches
/// `@rocicorp/logger`'s zero-cache sinks:
///   * `text`: `<local-ISO-timestamp> pid=<pid> worker=<worker> workerIndex=<n>
///     <message>`, ANSI-colored (gray/yellow/red for debug/warn/error).
///   * `json`: `{"level":"INFO","pid":…,"worker":…,"workerIndex":…,"message":…}`
///     (level upper-cased, no timestamp — the log aggregator stamps it).
pub fn log(level: Level, message: &str) {
    log_fields(level, message, &[]);
}

/// Like [`log`] but with structured key/value fields — spread into the JSON
/// object (like upstream's trailing-object arg), and appended as `k=v` in text.
pub fn log_fields(level: Level, message: &str, fields: &[(&str, String)]) {
    let Some(cfg) = CONFIG.get() else {
        if level <= Level::Warn {
            eprintln!("{}: {message}", level.label());
        }
        return;
    };
    if level > cfg.level {
        return;
    }
    match cfg.format {
        Format::Text => {
            let mut line = format!(
                "{} pid={} worker={} workerIndex={}",
                utc_iso_now(),
                cfg.pid,
                cfg.worker,
                cfg.worker_index,
            );
            for (k, v) in fields {
                line.push_str(&format!(" {k}={v}"));
            }
            if !message.is_empty() {
                line.push(' ');
                line.push_str(message);
            }
            eprintln!("{}", colorize(level, &line));
        }
        Format::Json => {
            let mut obj = format!(
                "{{\"level\":\"{}\",\"pid\":{},\"worker\":{},\"workerIndex\":{}",
                level.label(),
                cfg.pid,
                json_string(&cfg.worker),
                cfg.worker_index,
            );
            for (k, v) in fields {
                obj.push_str(&format!(",{}:{}", json_string(k), json_string(v)));
            }
            if !message.is_empty() {
                obj.push_str(&format!(",\"message\":{}", json_string(message)));
            }
            obj.push('}');
            eprintln!("{obj}");
        }
    }
}

/// ANSI-colorizes a whole text line by level (debug=gray, warn=yellow,
/// red=error; info uncolored), matching upstream's `styleText`. Skipped when
/// stderr isn't a terminal so log files/aggregators stay clean.
fn colorize(level: Level, line: &str) -> String {
    if !std::io::IsTerminal::is_terminal(&std::io::stderr()) {
        return line.to_string();
    }
    let code = match level {
        Level::Debug => "90", // gray
        Level::Warn => "33",  // yellow
        Level::Error => "31", // red
        Level::Info => return line.to_string(),
    };
    format!("\x1b[{code}m{line}\x1b[0m")
}

/// Current UTC time as an ISO-8601 string with milliseconds, e.g.
/// `2026-07-09T14:30:45.123Z`. (Upstream renders local time; a containerized
/// server runs UTC, and the aggregator timestamps anyway.)
fn utc_iso_now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let ms = now.subsec_millis();
    let (h, m, s) = ((secs / 3600) % 24, (secs / 60) % 60, secs % 60);
    let (y, mo, d) = civil_from_days((secs / 86400) as i64);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}.{ms:03}Z")
}

/// Days-since-epoch → (year, month, day). Howard Hinnant's `civil_from_days`.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Observability thresholds (`ZERO_LOG_SLOW_*`, `ZERO_LOG_IVM_SAMPLING`,
/// `ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG`), applied by the sync path.
pub struct Observability {
    pub slow_hydrate_ms: u64,
    pub slow_row_threshold: u64,
    pub ivm_sampling: u64,
    pub replication_reports_at_debug: bool,
}

static OBSERVABILITY: OnceLock<Observability> = OnceLock::new();

/// Installs the observability thresholds (call once at startup).
pub fn init_observability(o: Observability) {
    let _ = OBSERVABILITY.set(o);
}

fn observability() -> &'static Observability {
    static DEFAULT: Observability = Observability {
        slow_hydrate_ms: 100,
        slow_row_threshold: 3000,
        ivm_sampling: 5000,
        replication_reports_at_debug: false,
    };
    OBSERVABILITY.get().unwrap_or(&DEFAULT)
}

/// Logs a query hydration that exceeded `ZERO_LOG_SLOW_HYDRATE_THRESHOLD` ms or
/// `ZERO_LOG_SLOW_ROW_THRESHOLD` rows (upstream's slow-query observability).
pub fn maybe_log_slow_hydrate(query_id: &str, elapsed_ms: u64, rows: usize) {
    let o = observability();
    let slow_time = o.slow_hydrate_ms > 0 && elapsed_ms >= o.slow_hydrate_ms;
    let many_rows = o.slow_row_threshold > 0 && rows as u64 >= o.slow_row_threshold;
    if slow_time || many_rows {
        log_fields(
            Level::Info,
            "slow query",
            &[
                ("queryID", query_id.to_string()),
                ("elapsedMs", elapsed_ms.to_string()),
                ("rows", rows.to_string()),
            ],
        );
    }
}

/// The level at which routine replication-progress reports are logged: `Debug`
/// when `ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG` is set, else `Info`.
pub fn replication_report_level() -> Level {
    if observability().replication_reports_at_debug {
        Level::Debug
    } else {
        Level::Info
    }
}

/// Minimal JSON string escaping (quotes, backslashes, control chars).
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[macro_export]
macro_rules! log_at {
    ($level:expr, $($arg:tt)*) => {
        if $crate::logging::enabled($level) {
            $crate::logging::log($level, &format!($($arg)*));
        }
    };
}
#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => { $crate::log_at!($crate::logging::Level::Error, $($arg)*) };
}
#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => { $crate::log_at!($crate::logging::Level::Warn, $($arg)*) };
}
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => { $crate::log_at!($crate::logging::Level::Info, $($arg)*) };
}
#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => { $crate::log_at!($crate::logging::Level::Debug, $($arg)*) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parse_and_ordering() {
        assert_eq!(Level::parse("DEBUG"), Level::Debug);
        assert_eq!(Level::parse("warning"), Level::Warn);
        assert_eq!(Level::parse("nonsense"), Level::Info);
        // More severe sorts lower; Info shows Error/Warn/Info, hides Debug.
        assert!(Level::Error < Level::Info);
        assert!(Level::Debug > Level::Info);
    }

    #[test]
    fn json_string_escapes() {
        assert_eq!(json_string("a\"b\\c\n"), "\"a\\\"b\\\\c\\n\"");
    }

    #[test]
    fn civil_from_days_matches_known_dates() {
        // 1970-01-01 is day 0; 2000-01-01 is day 10957; 2026-07-09 is day 20643.
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(10957), (2000, 1, 1));
        assert_eq!(civil_from_days(20643), (2026, 7, 9));
    }

    #[test]
    fn utc_iso_now_is_well_formed() {
        let s = utc_iso_now();
        // YYYY-MM-DDTHH:MM:SS.mmmZ
        assert_eq!(s.len(), 24, "{s}");
        assert!(s.ends_with('Z') && s.contains('T') && s.contains('.'), "{s}");
    }
}
