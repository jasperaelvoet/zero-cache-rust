//! Port of `packages/zql/src/query/ttl.ts`.
//!
//! Time-to-live for query expiration: `forever` (never expires), `none`
//! (expires immediately), a plain millisecond count, or a `"<number><unit>"`
//! string (`s`/`m`/`h`/`d`/`y`).

/// A parsed TTL value, mirroring the TS `TTL` union.
#[derive(Debug, Clone, PartialEq)]
pub enum Ttl {
    Forever,
    None,
    Millis(f64),
    /// A `"<number><unit>"` string, e.g. `"1.5h"`.
    Duration(String),
}

impl Ttl {
    pub fn duration(value: f64, unit: char) -> Self {
        Ttl::Duration(format!("{value}{unit}"))
    }
}

pub const DEFAULT_TTL_MS: f64 = 1000.0 * 60.0 * 5.0;
pub const DEFAULT_PRELOAD_TTL_MS: f64 = 0.0;
pub const MAX_TTL_MS: f64 = 1000.0 * 60.0 * 10.0;

/// The preload default TTL: `none`.
pub fn default_preload_ttl() -> Ttl {
    Ttl::None
}

/// The default TTL: `"5m"`.
pub fn default_ttl() -> Ttl {
    Ttl::Duration("5m".to_string())
}

/// The max TTL: `"10m"`.
pub fn max_ttl() -> Ttl {
    Ttl::Duration("10m".to_string())
}

fn multiplier(unit: char) -> Option<f64> {
    Some(match unit {
        's' => 1000.0,
        'm' => 60.0 * 1000.0,
        'h' => 60.0 * 60.0 * 1000.0,
        'd' => 24.0 * 60.0 * 60.0 * 1000.0,
        'y' => 365.0 * 24.0 * 60.0 * 60.0 * 1000.0,
        _ => return None,
    })
}

/// Parses a [`Ttl`] into milliseconds; `-1` means "forever". Port of
/// `parseTTL`.
pub fn parse_ttl(ttl: &Ttl) -> f64 {
    match ttl {
        Ttl::Millis(n) => {
            if n.is_nan() {
                0.0
            } else if !n.is_finite() || *n < 0.0 {
                -1.0
            } else {
                *n
            }
        }
        Ttl::None => 0.0,
        Ttl::Forever => -1.0,
        Ttl::Duration(s) => {
            let unit = s.chars().last().unwrap();
            let multi = multiplier(unit).unwrap_or(f64::NAN);
            let num: f64 = s[..s.len() - unit.len_utf8()].parse().unwrap_or(f64::NAN);
            num * multi
        }
    }
}

/// Compares two TTLs by their parsed millisecond value; `forever` sorts as
/// greatest. Port of `compareTTL`.
pub fn compare_ttl(a: &Ttl, b: &Ttl) -> i64 {
    let ap = parse_ttl(a);
    let bp = parse_ttl(b);
    if ap == -1.0 && bp != -1.0 {
        return 1;
    }
    if ap != -1.0 && bp == -1.0 {
        return -1;
    }
    (ap - bp) as i64
}

/// Normalizes a millisecond TTL to its shortest string representation (if
/// shorter than the plain number), or passes strings through unchanged. Port
/// of `normalizeTTL`.
pub fn normalize_ttl(ttl: &Ttl) -> Ttl {
    let ms = match ttl {
        Ttl::Duration(_) => return ttl.clone(),
        Ttl::Forever => return Ttl::Forever,
        Ttl::None => return Ttl::None,
        Ttl::Millis(n) => *n,
    };

    if ms < 0.0 {
        return Ttl::Forever;
    }
    if ms == 0.0 {
        return Ttl::None;
    }

    let number_str = js_number_string(ms);
    let mut shortest = number_str.clone();
    for unit in ['y', 'd', 'h', 'm', 's'] {
        let multi = multiplier(unit).unwrap();
        let value = ms / multi;
        let candidate = format!("{}{unit}", js_number_string(value));
        if candidate.len() < shortest.len() {
            shortest = candidate;
        }
    }

    if shortest.len() < number_str.len() {
        Ttl::Duration(shortest)
    } else {
        Ttl::Millis(ms)
    }
}

/// Clamps a TTL to at most [`MAX_TTL_MS`] (treating `forever`/negative and
/// anything above the max as the max). Port of `clampTTL`. Returns
/// `(clamped_ms, was_clamped)` — the TS `LogContext.warn` call is left to the
/// caller (`was_clamped` tells them whether to warn).
pub fn clamp_ttl(ttl: &Ttl) -> (f64, bool) {
    let parsed = parse_ttl(ttl);
    if parsed == -1.0 || parsed > MAX_TTL_MS {
        (parse_ttl(&max_ttl()), true)
    } else {
        (parsed, false)
    }
}

/// Renders an `f64` the way JS's `String(n)` / template literal would, for the
/// shortest-representation comparisons in [`normalize_ttl`].
fn js_number_string(n: f64) -> String {
    if n.fract() == 0.0 && n.is_finite() && n.abs() < 1e21 {
        format!("{}", n as i128)
    } else {
        format!("{n}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dur(s: &str) -> Ttl {
        Ttl::Duration(s.to_string())
    }

    #[test]
    fn parse_ttl_cases() {
        let cases: Vec<(Ttl, f64)> = vec![
            (Ttl::None, 0.0),
            (Ttl::Forever, -1.0),
            (Ttl::Millis(0.0), 0.0),
            (Ttl::Millis(-0.0), -0.0),
            (Ttl::Millis(f64::INFINITY), -1.0),
            (Ttl::Millis(f64::NEG_INFINITY), -1.0),
            (Ttl::Millis(f64::NAN), 0.0),
            (Ttl::Millis(-0.5), -1.0),
            (Ttl::Millis(1.0), 1.0),
            (dur("1s"), 1000.0),
            (dur("1m"), 60.0 * 1000.0),
            (dur("1h"), 60.0 * 60.0 * 1000.0),
            (dur("1d"), 24.0 * 60.0 * 60.0 * 1000.0),
            (dur("1y"), 365.0 * 24.0 * 60.0 * 60.0 * 1000.0),
            (dur("1.5s"), 1500.0),
            (dur("1.5m"), 1.5 * 60.0 * 1000.0),
            (dur("1.5h"), 1.5 * 60.0 * 60.0 * 1000.0),
            (dur("1.5d"), 1.5 * 24.0 * 60.0 * 60.0 * 1000.0),
            (dur("1.5y"), 1.5 * 365.0 * 24.0 * 60.0 * 60.0 * 1000.0),
        ];
        for (ttl, expected) in cases {
            assert_eq!(parse_ttl(&ttl), expected, "{ttl:?}");
        }
    }

    #[test]
    fn compare_ttl_cases() {
        let cases: Vec<(Ttl, Ttl, i64)> = vec![
            (Ttl::None, Ttl::None, 0),
            (Ttl::None, Ttl::Forever, -1),
            (Ttl::None, Ttl::Millis(0.0), 0),
            (Ttl::Forever, Ttl::Forever, 0),
            (Ttl::Millis(1.0), Ttl::Millis(2.0), -1),
            (Ttl::Millis(1000.0), dur("1s"), 0),
            (dur("1s"), dur("1m"), -59 * 1000),
        ];
        for (a, b, expected) in cases {
            assert_eq!(compare_ttl(&a, &a), 0);
            assert_eq!(compare_ttl(&b, &b), 0);
            assert_eq!(compare_ttl(&a, &b), expected, "{a:?} vs {b:?}");
            let neg = if expected == 0 { 0 } else { -expected };
            assert_eq!(compare_ttl(&b, &a), neg);
        }
    }

    #[test]
    fn normalize_ttl_cases() {
        let cases: Vec<(Ttl, Ttl)> = vec![
            (Ttl::None, Ttl::None),
            (Ttl::Forever, Ttl::Forever),
            (Ttl::Millis(0.0), Ttl::None),
            (Ttl::Millis(-1.0), Ttl::Forever),
            (Ttl::Millis(1.0), Ttl::Millis(1.0)),
            (Ttl::Millis(1000.0), dur("1s")),
            (Ttl::Millis(60.0 * 1000.0), dur("1m")),
            (Ttl::Millis(60.0 * 60.0 * 1000.0), dur("1h")),
            (Ttl::Millis(24.0 * 60.0 * 60.0 * 1000.0), dur("1d")),
            (Ttl::Millis(365.0 * 24.0 * 60.0 * 60.0 * 1000.0), dur("1y")),
            (Ttl::Millis(1500.0), Ttl::Millis(1500.0)),
            (Ttl::Millis(1.5 * 60.0 * 1000.0), dur("90s")),
            (Ttl::Millis(1.5 * 60.0 * 60.0 * 1000.0), dur("90m")),
            (Ttl::Millis(1.5 * 24.0 * 60.0 * 60.0 * 1000.0), dur("36h")),
            (
                Ttl::Millis(1.5 * 365.0 * 24.0 * 60.0 * 60.0 * 1000.0),
                dur("1.5y"),
            ),
            (
                Ttl::Millis(1.25 * 365.0 * 24.0 * 60.0 * 60.0 * 1000.0),
                dur("1.25y"),
            ),
        ];
        for (ttl, expected) in cases {
            assert_eq!(normalize_ttl(&ttl), expected, "{ttl:?}");
        }
    }

    #[test]
    fn clamp_ttl_cases() {
        let cases: Vec<(Ttl, f64, bool)> = vec![
            (Ttl::None, 0.0, false),
            (Ttl::Forever, 10.0 * 60.0 * 1000.0, true),
            (Ttl::Millis(0.0), 0.0, false),
            (Ttl::Millis(-1.0), 10.0 * 60.0 * 1000.0, true),
            (Ttl::Millis(1.0), 1.0, false),
            (Ttl::Millis(1000.0), 1000.0, false),
            (
                Ttl::Millis(10.0 * 60.0 * 1000.0),
                10.0 * 60.0 * 1000.0,
                false,
            ),
            (
                Ttl::Millis(10.0 * 60.0 * 1000.0 + 1.0),
                10.0 * 60.0 * 1000.0,
                true,
            ),
            (dur("1h"), 10.0 * 60.0 * 1000.0, true),
            (dur("1m"), 60.0 * 1000.0, false),
        ];
        for (ttl, expected, expect_clamped) in cases {
            let (got, clamped) = clamp_ttl(&ttl);
            assert_eq!(got, expected, "{ttl:?}");
            assert_eq!(clamped, expect_clamped, "{ttl:?}");
        }
    }
}
