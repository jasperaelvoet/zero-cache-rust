//! Partial port of `zero-cache/src/types/pg.ts`.
//!
//! The pure time-conversion helpers are ported here, including
//! [`date_to_utc_midnight`] (`dateToUTCMidnight`), which uses exact
//! proleptic-Gregorian civil-date math (Howard Hinnant's `days_from_civil`).
//! `timestampToFpMillis` (which needs microsecond-precision `PreciseDate`
//! semantics plus expanded-year/BC/timezone-offset handling) and the Postgres
//! client / error helpers (which need the `postgres` driver) are deferred until
//! their dependencies land.

use std::sync::OnceLock;

use regex::Regex;
use thiserror::Error;

const MILLISECONDS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

/// Errors from the time helpers. Each `Display` matches the corresponding
/// JavaScript `Error` message so callers/tests can assert on the text.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct PgTimeError(pub String);

fn err(msg: impl Into<String>) -> PgTimeError {
    PgTimeError(msg.into())
}

/// Formats a count of milliseconds-since-midnight as a Postgres `time` string
/// (`HH:MM:SS.mmm+00`). Port of `millisecondsToPostgresTime`.
///
/// Accepts a floating-point count (floored to an integer), erroring if negative
/// or `>= 24h`.
pub fn milliseconds_to_postgres_time(milliseconds: f64) -> Result<String, PgTimeError> {
    if milliseconds < 0.0 {
        return Err(err("Milliseconds cannot be negative"));
    }
    if milliseconds >= MILLISECONDS_PER_DAY as f64 {
        return Err(err(format!(
            "Milliseconds cannot exceed 24 hours ({MILLISECONDS_PER_DAY}ms)"
        )));
    }
    let milliseconds = milliseconds.floor() as i64;

    let total_seconds = milliseconds / 1000;
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;
    let ms = milliseconds % 1000;

    Ok(format!("{hours:02}:{minutes:02}:{seconds:02}.{ms:03}+00"))
}

fn time_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(?:([+-])(\d{1,2})(?::(\d{2}))?)?$")
            .unwrap()
    })
}

/// Parses a Postgres `time` string into milliseconds-since-midnight. Port of
/// `postgresTimeToMilliseconds`.
pub fn postgres_time_to_milliseconds(time_string: &str) -> Result<i64, PgTimeError> {
    if time_string.is_empty() {
        return Err(err("Invalid time string: must be a non-empty string"));
    }

    let caps = time_regex().captures(time_string).ok_or_else(|| {
        err(format!(
            "Invalid time format: \"{time_string}\". Expected HH:MM:SS[.mmm][+|-HH[:MM]]"
        ))
    })?;

    let hours: i64 = caps[1].parse().unwrap();
    let minutes: i64 = caps[2].parse().unwrap();
    let seconds: i64 = caps[3].parse().unwrap();

    // Optional fractional seconds: pad to 6 microsecond digits, keep the first
    // 3 (milliseconds), truncating the rest.
    let mut milliseconds = 0i64;
    if let Some(frac) = caps.get(4) {
        let mut micros = frac.as_str().to_string();
        while micros.len() < 6 {
            micros.push('0');
        }
        milliseconds = micros[..3].parse().unwrap();
    }

    if !(0..=24).contains(&hours) {
        return Err(err(format!(
            "Invalid hours: {hours}. Must be between 0 and 24 (24 means end of day)"
        )));
    }
    if !(0..60).contains(&minutes) {
        return Err(err(format!(
            "Invalid minutes: {minutes}. Must be between 0 and 59"
        )));
    }
    if !(0..60).contains(&seconds) {
        return Err(err(format!(
            "Invalid seconds: {seconds}. Must be between 0 and 59"
        )));
    }
    // milliseconds is 0..=999 by construction.

    if hours == 24 && (minutes != 0 || seconds != 0 || milliseconds != 0) {
        return Err(err(
            "Invalid time: when hours is 24, minutes, seconds, and milliseconds must be 0",
        ));
    }

    let mut total_ms = hours * 3_600_000 + minutes * 60_000 + seconds * 1000 + milliseconds;

    if let Some(sign) = caps.get(5) {
        let sign = if sign.as_str() == "+" { 1 } else { -1 };
        let tz_hours: i64 = caps[6].parse().unwrap();
        let tz_minutes: i64 = caps.get(7).map_or(0, |m| m.as_str().parse().unwrap());
        let offset_ms = sign * (tz_hours * 3_600_000 + tz_minutes * 60_000);
        total_ms -= offset_ms;
    }

    if !(0..=MILLISECONDS_PER_DAY).contains(&total_ms) {
        return Ok(
            ((total_ms % MILLISECONDS_PER_DAY) + MILLISECONDS_PER_DAY) % MILLISECONDS_PER_DAY
        );
    }
    Ok(total_ms)
}

/// Days from the civil date `y-m-d` (proleptic Gregorian) to `1970-01-01`.
/// Howard Hinnant's `days_from_civil` — exact for any date, negative before
/// the epoch. `m` is 1..=12, `d` is 1..=31.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146097 + doe - 719468
}

/// Port of `dateToUTCMidnight`: converts a Postgres `date` value to
/// floating-point milliseconds since the Unix epoch at UTC midnight.
///
/// `infinity`/`-infinity` map to `+∞`/`-∞`. Otherwise the canonical Postgres
/// AD `date` text `YYYY-MM-DD` (the only form a `date` column emits) is
/// converted via exact civil-date math. Any other input — including BC dates,
/// which Postgres renders with a trailing ` BC` — yields `NaN`, matching
/// upstream's `new Date(invalid)` → `Date.UTC(NaN, …)` → `NaN`.
pub fn date_to_utc_midnight(date: &str) -> f64 {
    match date {
        "infinity" => return f64::INFINITY,
        "-infinity" => return f64::NEG_INFINITY,
        _ => {}
    }

    // Strict `YYYY-MM-DD`: exactly three dash-separated integer parts, no
    // trailing content (so a ` BC` suffix fails to parse, as upstream).
    let mut parts = date.split('-');
    let (Some(y), Some(m), Some(d), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return f64::NAN;
    };
    let (Ok(y), Ok(m), Ok(d)) = (y.parse::<i64>(), m.parse::<i64>(), d.parse::<i64>()) else {
        return f64::NAN;
    };
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return f64::NAN;
    }

    (days_from_civil(y, m, d) * MILLISECONDS_PER_DAY) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(hours: i64, m: i64, s: i64, ms: i64) -> f64 {
        (hours * 3_600_000 + m * 60_000 + s * 1000 + ms) as f64
    }

    #[test]
    fn ms_to_pg_time_valid() {
        let cases: &[(f64, &str)] = &[
            (0.0, "00:00:00.000+00"),
            (1000.0, "00:00:01.000+00"),
            (60000.0, "00:01:00.000+00"),
            (3600000.0, "01:00:00.000+00"),
            (1.0, "00:00:00.001+00"),
            (123.0, "00:00:00.123+00"),
            (999.0, "00:00:00.999+00"),
            (h(12, 34, 56, 789), "12:34:56.789+00"),
            (86_399_999.0, "23:59:59.999+00"),
            (h(1, 2, 3, 4), "01:02:03.004+00"),
            (1010.0, "00:00:01.010+00"),
            (1100.0, "00:00:01.100+00"),
            (100000.0, "00:01:40.000+00"),
            (1000000.0, "00:16:40.000+00"),
            // floating point floors down
            (0.1, "00:00:00.000+00"),
            (999.9, "00:00:00.999+00"),
            (1000.1, "00:00:01.000+00"),
        ];
        for &(input, expected) in cases {
            assert_eq!(
                milliseconds_to_postgres_time(input).unwrap(),
                expected,
                "{input}"
            );
            assert_eq!(milliseconds_to_postgres_time(input).unwrap().len(), 15);
        }
    }

    #[test]
    fn ms_to_pg_time_errors() {
        assert_eq!(
            milliseconds_to_postgres_time(-1.0),
            Err(err("Milliseconds cannot be negative"))
        );
        assert_eq!(
            milliseconds_to_postgres_time(86_400_000.0),
            Err(err("Milliseconds cannot exceed 24 hours (86400000ms)"))
        );
        assert!(milliseconds_to_postgres_time(100_000_000.0).is_err());
    }

    #[test]
    fn pg_time_to_ms_valid() {
        let cases: &[(&str, i64)] = &[
            ("00:00:00", 0),
            ("00:00:01", 1000),
            ("00:01:00", 60000),
            ("01:00:00", 3600000),
            ("12:00:00", 43200000),
            ("12:34:56", 45296000),
            ("23:59:59", 86399000),
            ("9:30:45", 34245000),
            ("09:30:45", 34245000),
            // milliseconds
            ("00:00:00.001", 1),
            ("00:00:00.999", 999),
            ("12:34:56.789", 45296789),
            ("23:59:59.999", 86399999),
            ("01:02:03.456", 3723456),
            // padding
            ("12:34:56.7", 45296700),
            ("12:34:56.78", 45296780),
            ("00:00:01.5", 1500),
            ("00:00:01.05", 1050),
            ("00:00:01.005", 1005),
            // microsecond truncation
            ("12:34:56.7891", 45296789),
            ("12:34:56.789123", 45296789),
            ("00:00:00.1239", 123),
            ("00:00:00.000001", 0),
            ("23:59:59.999999", 86399999),
            // 24:00:00 edge cases
            ("24:00:00", 86400000),
            ("24:00:00.000", 86400000),
            ("24:00:00+00", 86400000),
            ("24:00:00+02", 79200000),
            ("24:00:00-05", 18000000),
        ];
        for &(input, expected) in cases {
            assert_eq!(
                postgres_time_to_milliseconds(input),
                Ok(expected),
                "{input}"
            );
        }
    }

    #[test]
    fn pg_time_to_ms_errors() {
        assert_eq!(
            postgres_time_to_milliseconds(""),
            Err(err("Invalid time string: must be a non-empty string"))
        );
        for bad in [
            "123456",
            "12:34",
            "12:34:56:78",
            "AB:34:56",
            "12:AB:56",
            "12:34:AB",
            "12:34:56.ABC",
            "12-34-56",
            "12 34 56",
            "12:34:56,789",
            "12:3:56",
            "12:34:5",
            "123:34:56",
            "-12:34:56",
        ] {
            let e = postgres_time_to_milliseconds(bad).unwrap_err();
            assert!(e.0.contains("Invalid time format"), "{bad}: {e:?}");
        }

        assert!(postgres_time_to_milliseconds("24:01:00")
            .unwrap_err()
            .0
            .contains("when hours is 24"));
        assert!(postgres_time_to_milliseconds("24:00:01")
            .unwrap_err()
            .0
            .contains("when hours is 24"));
        assert!(postgres_time_to_milliseconds("25:00:00")
            .unwrap_err()
            .0
            .contains("Invalid hours: 25"));
        assert!(postgres_time_to_milliseconds("99:00:00")
            .unwrap_err()
            .0
            .contains("Invalid hours: 99"));
    }

    #[test]
    fn date_to_utc_midnight_epoch_and_known_dates() {
        assert_eq!(date_to_utc_midnight("1970-01-01"), 0.0);
        assert_eq!(date_to_utc_midnight("1970-01-02"), 86_400_000.0);
        // 2000-01-01T00:00:00Z in Unix ms.
        assert_eq!(date_to_utc_midnight("2000-01-01"), 946_684_800_000.0);
        // Pre-epoch date is negative.
        assert_eq!(date_to_utc_midnight("1969-12-31"), -86_400_000.0);
    }

    #[test]
    fn date_to_utc_midnight_infinities_and_invalid() {
        assert_eq!(date_to_utc_midnight("infinity"), f64::INFINITY);
        assert_eq!(date_to_utc_midnight("-infinity"), f64::NEG_INFINITY);
        assert!(date_to_utc_midnight("not-a-date").is_nan());
        assert!(date_to_utc_midnight("2000-13-01").is_nan()); // bad month
        assert!(date_to_utc_midnight("0044-01-01 BC").is_nan()); // BC unsupported, like upstream
    }
}
