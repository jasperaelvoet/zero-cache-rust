//! Text-format `COPY ... TO STDOUT` support for initial sync — the port of
//! upstream `initial-sync.ts`'s `copyTextTo`/`ZERO_INITIAL_SYNC_TEXT_COPY`
//! path. Two halves:
//!
//! 1. [`TextCopyParser`]: a streaming parser for PostgreSQL's default text
//!    COPY format (TSV): rows are `\n`-terminated lines, fields are
//!    `\t`-separated, `\N` is SQL NULL, and special characters are
//!    backslash-escaped (`\t` `\n` `\\` `\b` `\f` `\r` `\v`).
//! 2. [`make_text_decoder`]: per-column text→[`LiteValue`] converters that
//!    mirror the *binary* decoders in `zero_cache_types::pg_copy_binary`
//!    exactly — a replica built with `text_copy: true` must be byte-for-byte
//!    identical to one built via binary COPY, and both must agree with the
//!    pgoutput streaming path's text conversion (timestamps → epoch millis,
//!    bools → 0/1, int8 → BigInt, arrays → JSON text, etc.).
//!
//! The timestamp/date text parsing is the same algorithm as the replication
//! stream's `pg_to_change::pg_timestamp_to_epoch_millis` (upstream
//! `timestampToFpMillis`); it is duplicated here because that function is
//! private to `zero-cache-change-source` and this increment is scoped to the
//! `zero-cache-sqlite` crate.

use num_bigint::BigInt;
use zero_cache_shared::bigint_json::{stringify, JsonValue};
use zero_cache_types::lite::LiteValue;
use zero_cache_types::pg_copy_binary::{has_binary_decoder, BinaryColumnSpec};
use zero_cache_types::pg_types;
use zero_cache_types::specs::PgTypeClass;

const MS_PER_DAY: f64 = 86_400_000.0;

/// A function decoding one unescaped text COPY field into a [`LiteValue`].
pub type TextDecoder = Box<dyn Fn(&str) -> LiteValue + Send + Sync>;

/// Streaming parser for the text COPY format. Feed raw `CopyData` chunks to
/// [`parse`](Self::parse); each call returns the rows completed by that chunk
/// (fields already unescaped, `None` = SQL NULL). Rows may span chunk
/// boundaries; leftover bytes are buffered.
#[derive(Default)]
pub struct TextCopyParser {
    buffer: Vec<u8>,
}

impl TextCopyParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses the rows completed by `chunk`.
    pub fn parse(&mut self, chunk: &[u8]) -> Vec<Vec<Option<String>>> {
        self.buffer.extend_from_slice(chunk);
        let mut rows = Vec::new();
        let mut start = 0usize;
        while let Some(nl) = self.buffer[start..].iter().position(|&b| b == b'\n') {
            let line = &self.buffer[start..start + nl];
            start += nl + 1;
            // The end-of-data marker line (`\.`) is not part of the COPY-OUT
            // wire protocol, but tolerate it defensively (psql emits it).
            if line == b"\\." {
                continue;
            }
            rows.push(parse_line(line));
        }
        self.buffer.drain(..start);
        rows
    }

    /// Bytes buffered but not yet terminated by a newline (should be zero at
    /// end of stream).
    pub fn pending_bytes(&self) -> usize {
        self.buffer.len()
    }
}

/// Splits one line into unescaped fields. Escaped tabs arrive as the two-byte
/// sequence `\t`, so a raw `0x09` byte is always a field delimiter.
fn parse_line(line: &[u8]) -> Vec<Option<String>> {
    line.split(|&b| b == b'\t')
        .map(|field| {
            if field == b"\\N" {
                None
            } else {
                Some(unescape_field(&String::from_utf8_lossy(field)))
            }
        })
        .collect()
}

/// Undoes COPY text-format backslash escaping. `COPY TO` only ever emits the
/// named single-character escapes; any other backslashed character represents
/// itself (per the COPY spec).
fn unescape_field(s: &str) -> String {
    if !s.contains('\\') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('b') => out.push('\u{0008}'),
            Some('f') => out.push('\u{000C}'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('t') => out.push('\t'),
            Some('v') => out.push('\u{000B}'),
            Some(other) => out.push(other), // includes `\\`
            None => out.push('\\'),
        }
    }
    out
}

/// Builds the text-format counterpart of
/// [`zero_cache_types::pg_copy_binary::make_binary_decoder`]: given the same
/// column spec, the returned decoder produces the same [`LiteValue`] the
/// binary decoder would for the same upstream value.
pub fn make_text_decoder(spec: &BinaryColumnSpec) -> TextDecoder {
    // Arrays: parse the `{...}` literal and re-emit as JSON text (matching
    // `decode_array`'s output for the binary path).
    if spec.elem_pg_type_class.is_some() {
        let elem_oid = pg_types::array_element_type(spec.type_oid);
        return Box::new(move |text| match parse_pg_array(text) {
            Ok(elements) => {
                let arr: Vec<JsonValue> = elements
                    .into_iter()
                    .map(|el| match el {
                        None => JsonValue::Null,
                        Some(s) => match elem_oid {
                            Some(oid) => element_text_to_json(oid, &s),
                            None => JsonValue::String(s),
                        },
                    })
                    .collect();
                LiteValue::Text(stringify(&JsonValue::Array(arr)))
            }
            Err(()) => LiteValue::Text(text.to_string()),
        });
    }
    // Enums (and any type without a native binary decoder) pass through as
    // text, matching the binary path's `::text` cast + `text_cast_decoder`.
    if spec.pg_type_class == Some(PgTypeClass::Enum) || !has_binary_decoder(spec) {
        return Box::new(|text| LiteValue::Text(text.to_string()));
    }

    match spec.type_oid {
        pg_types::BOOL => Box::new(|t| LiteValue::Number(if t == "t" { 1.0 } else { 0.0 })),
        pg_types::INT2 | pg_types::INT4 => Box::new(|t| match t.parse::<f64>() {
            Ok(n) => LiteValue::Number(n),
            Err(_) => LiteValue::Text(t.to_string()),
        }),
        // The binary decoder always yields Big for int8, even in-range values.
        pg_types::INT8 => Box::new(|t| match t.parse::<i64>() {
            Ok(n) => LiteValue::Big(BigInt::from(n)),
            Err(_) => LiteValue::Text(t.to_string()),
        }),
        // f64 parsing accepts Postgres's `NaN` / `Infinity` / `-Infinity`.
        pg_types::FLOAT4 | pg_types::FLOAT8 | pg_types::NUMERIC => {
            Box::new(|t| match t.parse::<f64>() {
                Ok(n) => LiteValue::Number(n),
                Err(_) => LiteValue::Text(t.to_string()),
            })
        }
        pg_types::BYTEA => Box::new(|t| LiteValue::Blob(decode_bytea_text(t))),
        pg_types::TIMESTAMP | pg_types::TIMESTAMPTZ | pg_types::DATE => {
            Box::new(|t| match pg_timestamp_to_epoch_millis(t) {
                Some(ms) => LiteValue::Number(ms),
                None => LiteValue::Text(t.to_string()),
            })
        }
        pg_types::TIME => Box::new(|t| match parse_time_millis(t) {
            Some(ms) => LiteValue::Number(ms.trunc()),
            None => LiteValue::Text(t.to_string()),
        }),
        pg_types::TIMETZ => Box::new(|t| match parse_timetz_millis(t) {
            Some(ms) => LiteValue::Number(ms),
            None => LiteValue::Text(t.to_string()),
        }),
        // TEXT | VARCHAR | BPCHAR | CHAR | JSON | JSONB | UUID and anything
        // else with a binary decoder that is textual on the wire.
        _ => Box::new(|t| LiteValue::Text(t.to_string())),
    }
}

/// Array-element conversion matching `pg_copy_binary`'s `decode_element` (the
/// binary path), which differs from top-level columns: bools stay JSON bools
/// and in-range int8 values stay JSON numbers.
fn element_text_to_json(elem_oid: i64, text: &str) -> JsonValue {
    match elem_oid {
        pg_types::BOOL => JsonValue::Bool(text == "t"),
        pg_types::INT2
        | pg_types::INT4
        | pg_types::FLOAT4
        | pg_types::FLOAT8
        | pg_types::NUMERIC => match text.parse::<f64>() {
            Ok(n) => JsonValue::Number(n),
            Err(_) => JsonValue::String(text.to_string()),
        },
        pg_types::INT8 => match text.parse::<i64>() {
            Ok(v) if (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&v) => {
                JsonValue::Number(v as f64)
            }
            Ok(v) => JsonValue::BigInt(BigInt::from(v)),
            Err(_) => JsonValue::String(text.to_string()),
        },
        pg_types::TIMESTAMP | pg_types::TIMESTAMPTZ | pg_types::DATE => {
            match pg_timestamp_to_epoch_millis(text) {
                Some(ms) => JsonValue::Number(ms),
                None => JsonValue::String(text.to_string()),
            }
        }
        pg_types::TIME => match parse_time_millis(text) {
            Some(ms) => JsonValue::Number(ms.trunc()),
            None => JsonValue::String(text.to_string()),
        },
        pg_types::TIMETZ => match parse_timetz_millis(text) {
            Some(ms) => JsonValue::Number(ms),
            None => JsonValue::String(text.to_string()),
        },
        _ => JsonValue::String(text.to_string()),
    }
}

/// Decodes Postgres's text bytea representation (hex `\x...` since PG 9.0).
/// Anything else falls back to the raw bytes of the string.
fn decode_bytea_text(text: &str) -> Vec<u8> {
    if let Some(hex) = text.strip_prefix("\\x") {
        if hex.len() % 2 == 0 {
            let mut out = Vec::with_capacity(hex.len() / 2);
            for i in (0..hex.len()).step_by(2) {
                match u8::from_str_radix(&hex[i..i + 2], 16) {
                    Ok(b) => out.push(b),
                    Err(_) => return text.as_bytes().to_vec(),
                }
            }
            return out;
        }
    }
    text.as_bytes().to_vec()
}

/// Parses a Postgres date/timestamp text value to epoch milliseconds. Same
/// algorithm as the replication stream's conversion (upstream
/// `timestampToFpMillis`): accepts `YYYY-MM-DD`,
/// `YYYY-MM-DD HH:MM:SS[.frac]`, an optional trailing `[+-]HH[:MM]` offset,
/// and the `infinity`/`-infinity` specials. No offset means UTC.
fn pg_timestamp_to_epoch_millis(text: &str) -> Option<f64> {
    let t = text.trim();
    if t == "infinity" {
        return Some(f64::INFINITY);
    }
    if t == "-infinity" {
        return Some(f64::NEG_INFINITY);
    }
    let (date_part, time_part) = match t.split_once([' ', 'T']) {
        Some((d, r)) => (d, Some(r)),
        None => (t, None),
    };
    let mut dp = date_part.split('-');
    let year: i64 = dp.next()?.parse().ok()?;
    let month: i64 = dp.next()?.parse().ok()?;
    let day: i64 = dp.next()?.parse().ok()?;
    if dp.next().is_some() {
        return None;
    }
    let mut millis = days_from_civil(year, month, day) as f64 * MS_PER_DAY;

    if let Some(time_part) = time_part {
        let (time_str, tz) = split_tz_offset(time_part);
        let time_str = time_str.strip_suffix('Z').unwrap_or(time_str);
        millis += parse_time_millis(time_str)?;
        if let Some(tz) = tz {
            millis -= parse_tz_offset_millis(tz)?;
        }
    }
    Some(millis)
}

/// Splits a trailing timezone offset (`+HH[:MM]` / `-HH[:MM]`) off a time
/// string. The time itself contains no `+`/`-`.
fn split_tz_offset(time_part: &str) -> (&str, Option<&str>) {
    match time_part.rfind(['+', '-']) {
        Some(i) if i > 0 => (&time_part[..i], Some(&time_part[i..])),
        _ => (time_part, None),
    }
}

/// `[+-]HH[:MM[:SS]]` → signed offset in milliseconds east of UTC.
fn parse_tz_offset_millis(tz: &str) -> Option<f64> {
    let positive = tz.starts_with('+');
    let body = &tz[1..];
    let mut parts = body.split(':');
    let hh: f64 = parts.next()?.parse().ok()?;
    let mm: f64 = parts.next().unwrap_or("0").parse().ok()?;
    let ss: f64 = parts.next().unwrap_or("0").parse().ok()?;
    let ms = (hh.abs() * 3600.0 + mm * 60.0 + ss) * 1000.0;
    Some(if positive { ms } else { -ms })
}

/// `HH:MM:SS[.frac]` → milliseconds since midnight (fractional).
fn parse_time_millis(time: &str) -> Option<f64> {
    let mut ts = time.split(':');
    let h: f64 = ts.next()?.parse().ok()?;
    let m: f64 = ts.next()?.parse().ok()?;
    let s: f64 = ts.next().unwrap_or("0").parse().ok()?;
    if ts.next().is_some() {
        return None;
    }
    Some((h * 3600.0 + m * 60.0 + s) * 1000.0)
}

/// `HH:MM:SS[.frac][+-]HH[:MM]` → UTC millis normalized to `[0, MS_PER_DAY)`,
/// matching `pg_copy_binary::decode_time_tz`.
fn parse_timetz_millis(text: &str) -> Option<f64> {
    let (time_str, tz) = split_tz_offset(text);
    let mut ms = parse_time_millis(time_str)?;
    if let Some(tz) = tz {
        ms -= parse_tz_offset_millis(tz)?;
    }
    let mut ms = ms.trunc();
    if !(0.0..MS_PER_DAY).contains(&ms) {
        ms = ((ms % MS_PER_DAY) + MS_PER_DAY) % MS_PER_DAY;
    }
    Some(ms)
}

/// Days since the Unix epoch for a proleptic-Gregorian date (Howard Hinnant's
/// `days_from_civil`), same as the replication stream's converter.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Parses a one-dimensional Postgres array text literal (`{1,2,3}`,
/// `{"a","b,c"}`, `{NULL,1}`) into its elements. Same algorithm as the
/// replication stream's array parsing.
fn parse_pg_array(text: &str) -> Result<Vec<Option<String>>, ()> {
    let inner = text
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or(())?;
    if inner.is_empty() {
        return Ok(vec![]);
    }
    let chars: Vec<char> = inner.chars().collect();
    let mut elements = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '"' {
            i += 1;
            let mut s = String::new();
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    i += 1;
                    if i >= chars.len() {
                        return Err(());
                    }
                }
                s.push(chars[i]);
                i += 1;
            }
            if i >= chars.len() {
                return Err(()); // unterminated quoted element
            }
            i += 1; // closing quote
            elements.push(Some(s));
        } else {
            let start = i;
            while i < chars.len() && chars[i] != ',' {
                i += 1;
            }
            let raw: String = chars[start..i].iter().collect();
            elements.push(if raw == "NULL" { None } else { Some(raw) });
        }
        if i < chars.len() {
            if chars[i] != ',' {
                return Err(());
            }
            i += 1;
        }
    }
    Ok(elements)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(type_oid: i64, data_type: &str) -> BinaryColumnSpec {
        BinaryColumnSpec {
            type_oid,
            data_type: data_type.to_string(),
            pg_type_class: None,
            elem_pg_type_class: None,
        }
    }

    #[test]
    fn parser_splits_rows_and_fields_and_recognizes_null() {
        let mut p = TextCopyParser::new();
        let rows = p.parse(b"1\tone\n2\t\\N\n");
        assert_eq!(
            rows,
            vec![
                vec![Some("1".to_string()), Some("one".to_string())],
                vec![Some("2".to_string()), None],
            ]
        );
        assert_eq!(p.pending_bytes(), 0);
    }

    #[test]
    fn parser_buffers_rows_spanning_chunks() {
        let mut p = TextCopyParser::new();
        assert!(p.parse(b"10\tab").is_empty());
        assert!(p.pending_bytes() > 0);
        let rows = p.parse(b"c\n20\txy\n");
        assert_eq!(
            rows,
            vec![
                vec![Some("10".to_string()), Some("abc".to_string())],
                vec![Some("20".to_string()), Some("xy".to_string())],
            ]
        );
        assert_eq!(p.pending_bytes(), 0);
    }

    #[test]
    fn parser_unescapes_all_copy_escapes() {
        let mut p = TextCopyParser::new();
        let rows = p.parse(b"a\\tb\\nc\\\\d\\be\\ff\\rg\\vh\n");
        assert_eq!(
            rows,
            vec![vec![Some(
                "a\tb\nc\\d\u{0008}e\u{000C}f\rg\u{000B}h".to_string()
            )]]
        );
    }

    #[test]
    fn parser_ignores_the_end_of_data_marker() {
        let mut p = TextCopyParser::new();
        let rows = p.parse(b"1\n\\.\n");
        assert_eq!(rows, vec![vec![Some("1".to_string())]]);
    }

    #[test]
    fn escaped_backslash_n_is_a_literal_not_null() {
        // The two-character field `\N` (escaped as `\\N`) is the string "\N",
        // while the bare `\N` marker is SQL NULL.
        let mut p = TextCopyParser::new();
        let rows = p.parse(b"\\\\N\n\\N\n");
        assert_eq!(rows, vec![vec![Some("\\N".to_string())], vec![None]]);
    }

    #[test]
    fn scalar_decoders_match_the_binary_decoders_output_shapes() {
        let dec = |oid, ty: &str, text: &str| make_text_decoder(&spec(oid, ty))(text);
        assert_eq!(dec(pg_types::BOOL, "bool", "t"), LiteValue::Number(1.0));
        assert_eq!(dec(pg_types::BOOL, "bool", "f"), LiteValue::Number(0.0));
        assert_eq!(
            dec(pg_types::INT4, "int4", "-123"),
            LiteValue::Number(-123.0)
        );
        assert_eq!(
            dec(pg_types::INT8, "int8", "42"),
            LiteValue::Big(BigInt::from(42)),
            "int8 is always Big, matching the binary decoder"
        );
        assert_eq!(
            dec(pg_types::INT8, "int8", "9007199254740993"),
            LiteValue::Big(BigInt::from(9_007_199_254_740_993i64))
        );
        assert_eq!(
            dec(pg_types::FLOAT8, "float8", "1.5"),
            LiteValue::Number(1.5)
        );
        assert_eq!(
            dec(pg_types::FLOAT8, "float8", "Infinity"),
            LiteValue::Number(f64::INFINITY)
        );
        assert_eq!(
            dec(pg_types::NUMERIC, "numeric", "12.34"),
            LiteValue::Number(12.34)
        );
        assert_eq!(
            dec(pg_types::TEXT, "text", "hello"),
            LiteValue::Text("hello".into())
        );
        assert_eq!(
            dec(pg_types::JSONB, "jsonb", "{\"k\": 1}"),
            LiteValue::Text("{\"k\": 1}".into())
        );
        assert_eq!(
            dec(pg_types::BYTEA, "bytea", "\\x666f6f"),
            LiteValue::Blob(b"foo".to_vec())
        );
    }

    #[test]
    fn timestamp_decoders_produce_epoch_millis_like_the_binary_path() {
        let dec = |oid, ty: &str, text: &str| make_text_decoder(&spec(oid, ty))(text);
        // 2024-03-15T12:00:00Z = 1710504000000 ms.
        assert_eq!(
            dec(
                pg_types::TIMESTAMPTZ,
                "timestamptz",
                "2024-03-15 12:00:00+00"
            ),
            LiteValue::Number(1_710_504_000_000.0)
        );
        // A +02 offset is two hours ahead of UTC.
        assert_eq!(
            dec(
                pg_types::TIMESTAMPTZ,
                "timestamptz",
                "2024-03-15 14:00:00+02"
            ),
            LiteValue::Number(1_710_504_000_000.0)
        );
        assert_eq!(
            dec(pg_types::TIMESTAMP, "timestamp", "2024-03-15 12:00:00.5"),
            LiteValue::Number(1_710_504_000_500.0)
        );
        assert_eq!(
            dec(pg_types::DATE, "date", "2024-03-15"),
            LiteValue::Number(1_710_460_800_000.0)
        );
        assert_eq!(
            dec(pg_types::DATE, "date", "infinity"),
            LiteValue::Number(f64::INFINITY)
        );
        assert_eq!(
            dec(pg_types::TIME, "time", "01:02:03.25"),
            LiteValue::Number(3_723_250.0)
        );
        // 01:00+02 normalizes to 23:00 the previous day.
        assert_eq!(
            dec(pg_types::TIMETZ, "timetz", "01:00:00+02"),
            LiteValue::Number(82_800_000.0)
        );
    }

    #[test]
    fn array_decoder_emits_json_text_like_decode_array() {
        let mut s = spec(pg_types::INT4_ARRAY, "int4[]");
        s.elem_pg_type_class = Some(PgTypeClass::Base);
        let dec = make_text_decoder(&s);
        assert_eq!(dec("{1,NULL,3}"), LiteValue::Text("[1,null,3]".into()));
        assert_eq!(dec("{}"), LiteValue::Text("[]".into()));

        let mut s = spec(pg_types::TEXT_ARRAY, "text[]");
        s.elem_pg_type_class = Some(PgTypeClass::Base);
        let dec = make_text_decoder(&s);
        assert_eq!(
            dec("{\"a,b\",c,NULL}"),
            LiteValue::Text("[\"a,b\",\"c\",null]".into())
        );

        let mut s = spec(pg_types::BOOL_ARRAY, "bool[]");
        s.elem_pg_type_class = Some(PgTypeClass::Base);
        let dec = make_text_decoder(&s);
        assert_eq!(
            dec("{t,f}"),
            LiteValue::Text("[true,false]".into()),
            "array elements keep JSON booleans, matching decode_element"
        );
    }

    #[test]
    fn enum_and_unknown_types_pass_through_as_text() {
        let mut s = spec(999_999, "mood");
        s.pg_type_class = Some(PgTypeClass::Enum);
        assert_eq!(
            make_text_decoder(&s)("happy"),
            LiteValue::Text("happy".into())
        );
        let s = spec(999_999, "some_custom");
        assert_eq!(make_text_decoder(&s)("v"), LiteValue::Text("v".into()));
    }
}
