//! Port of `packages/shared/src/bigint-json.ts`.
//!
//! A JSON value model that admits `bigint`, plus [`stringify`] matching the
//! `json-custom-numbers` output used by zero-cache: compact (no whitespace when
//! no indent), bigints emitted as bare integer tokens (not quoted strings),
//! object keys serialized in insertion order. [`parse`] is the inverse:
//! integers outside `Number.MAX_SAFE_INTEGER`/`MIN_SAFE_INTEGER` (and without
//! a decimal point or exponent) become [`JsonValue::BigInt`].

use num_bigint::BigInt;

/// A JSON value, extended to include `bigint` (as in the TS `JSONValue`).
/// Objects preserve insertion order, matching JavaScript object semantics.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(f64),
    BigInt(BigInt),
    String(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    /// Serializes to a compact JSON string (no indentation), matching
    /// `stringify(obj)` with no replacer/indent.
    pub fn stringify(&self) -> String {
        let mut out = String::new();
        self.write(&mut out);
        out
    }

    fn write(&self, out: &mut String) {
        match self {
            JsonValue::Null => out.push_str("null"),
            JsonValue::Bool(true) => out.push_str("true"),
            JsonValue::Bool(false) => out.push_str("false"),
            JsonValue::Number(n) => out.push_str(&format_number(*n)),
            // customSerializer returns `v.toString()` for bigint, which
            // json-custom-numbers inserts as a bare numeric token.
            JsonValue::BigInt(b) => out.push_str(&b.to_string()),
            JsonValue::String(s) => write_json_string(s, out),
            JsonValue::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write(out);
                }
                out.push(']');
            }
            JsonValue::Object(entries) => {
                out.push('{');
                let mut first = true;
                for (k, val) in entries {
                    // JSON.stringify omits properties whose value is undefined;
                    // there is no undefined here, so every entry is emitted.
                    if !first {
                        out.push(',');
                    }
                    first = false;
                    write_json_string(k, out);
                    out.push(':');
                    val.write(out);
                }
                out.push('}');
            }
        }
    }
}

/// Convenience wrapper mirroring the free `stringify` function.
pub fn stringify(value: &JsonValue) -> String {
    value.stringify()
}

/// Appends `value`'s JSON encoding to `out` (no intermediate `String`
/// allocation). Byte-identical to `stringify(value)`; the append form lets hot
/// serializers (e.g. poke bodies over 1000-row `rowsPatch`es) build one buffer
/// instead of allocating and joining a `String` per element.
pub fn write_value(value: &JsonValue, out: &mut String) {
    value.write(out);
}

/// Appends `s` as a JSON string literal (quoted + escaped) to `out`, reusing
/// the same escaping as `stringify`. Avoids the `JsonValue::String(s.to_owned())`
/// + `stringify` round-trip when serializing many string fields.
pub fn write_string(s: &str, out: &mut String) {
    write_json_string(s, out);
}

/// Appends `entries` as a JSON object (`{"k":v,...}`) to `out`, byte-identical
/// to `write_value(&JsonValue::Object(entries.to_vec()), out)` but WITHOUT
/// cloning the entries. Lets a row (`&[(String, JsonValue)]`) be serialized in
/// place rather than cloned into an owned `JsonValue::Object` first.
pub fn write_object(entries: &[(String, JsonValue)], out: &mut String) {
    out.push('{');
    let mut first = true;
    for (k, val) in entries {
        if !first {
            out.push(',');
        }
        first = false;
        write_json_string(k, out);
        out.push(':');
        val.write(out);
    }
    out.push('}');
}

/// Error from [`parse`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError(pub String);

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for ParseError {}

/// Parses a JSON string into a [`JsonValue`]. Port of `bigint-json.ts`'s
/// `parse`: integers outside the safe-integer range (and without a decimal
/// point or exponent) become [`JsonValue::BigInt`]; everything else that is
/// numeric becomes [`JsonValue::Number`].
pub fn parse(s: &str) -> Result<JsonValue, ParseError> {
    let mut p = Parser {
        chars: s.chars().collect(),
        pos: 0,
    };
    p.skip_ws();
    let value = p.parse_value()?;
    p.skip_ws();
    if p.pos != p.chars.len() {
        return Err(ParseError(format!(
            "Unexpected trailing input at {}",
            p.pos
        )));
    }
    Ok(value)
}

/// `Number.MAX_SAFE_INTEGER` / `MIN_SAFE_INTEGER`.
const MAX_SAFE: f64 = 9_007_199_254_740_991.0;
const MIN_SAFE: f64 = -9_007_199_254_740_991.0;

struct Parser {
    chars: Vec<char>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.pos += 1;
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue, ParseError> {
        match self.peek() {
            Some('{') => self.parse_object(),
            Some('[') => self.parse_array(),
            Some('"') => Ok(JsonValue::String(self.parse_string()?)),
            Some('t') | Some('f') => self.parse_bool(),
            Some('n') => self.parse_null(),
            Some(c) if c == '-' || c.is_ascii_digit() => self.parse_number(),
            other => Err(ParseError(format!(
                "Unexpected token {other:?} at {}",
                self.pos
            ))),
        }
    }

    fn expect(&mut self, c: char) -> Result<(), ParseError> {
        if self.peek() == Some(c) {
            self.pos += 1;
            Ok(())
        } else {
            Err(ParseError(format!("Expected '{c}' at {}", self.pos)))
        }
    }

    fn parse_object(&mut self) -> Result<JsonValue, ParseError> {
        self.expect('{')?;
        let mut entries = Vec::new();
        self.skip_ws();
        if self.peek() == Some('}') {
            self.pos += 1;
            return Ok(JsonValue::Object(entries));
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect(':')?;
            self.skip_ws();
            let value = self.parse_value()?;
            entries.push((key, value));
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos += 1;
                }
                Some('}') => {
                    self.pos += 1;
                    break;
                }
                other => {
                    return Err(ParseError(format!(
                        "Expected ',' or '}}' at {}, got {other:?}",
                        self.pos
                    )))
                }
            }
        }
        Ok(JsonValue::Object(entries))
    }

    fn parse_array(&mut self) -> Result<JsonValue, ParseError> {
        self.expect('[')?;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(']') {
            self.pos += 1;
            return Ok(JsonValue::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.parse_value()?);
            self.skip_ws();
            match self.peek() {
                Some(',') => {
                    self.pos += 1;
                }
                Some(']') => {
                    self.pos += 1;
                    break;
                }
                other => {
                    return Err(ParseError(format!(
                        "Expected ',' or ']' at {}, got {other:?}",
                        self.pos
                    )))
                }
            }
        }
        Ok(JsonValue::Array(items))
    }

    fn parse_string(&mut self) -> Result<String, ParseError> {
        self.expect('"')?;
        let mut out = String::new();
        loop {
            match self.peek() {
                None => return Err(ParseError("Unterminated string".into())),
                Some('"') => {
                    self.pos += 1;
                    break;
                }
                Some('\\') => {
                    self.pos += 1;
                    match self.peek() {
                        Some('"') => out.push('"'),
                        Some('\\') => out.push('\\'),
                        Some('/') => out.push('/'),
                        Some('b') => out.push('\u{08}'),
                        Some('f') => out.push('\u{0C}'),
                        Some('n') => out.push('\n'),
                        Some('r') => out.push('\r'),
                        Some('t') => out.push('\t'),
                        Some('u') => {
                            let mut code = 0u32;
                            for _ in 0..4 {
                                self.pos += 1;
                                let c = self.peek().ok_or_else(|| {
                                    ParseError("Unterminated unicode escape".into())
                                })?;
                                code = code * 16
                                    + c.to_digit(16).ok_or_else(|| {
                                        ParseError(format!("Invalid hex digit {c:?}"))
                                    })?;
                            }
                            out.push(char::from_u32(code).unwrap_or('\u{FFFD}'));
                        }
                        other => return Err(ParseError(format!("Invalid escape {other:?}"))),
                    }
                    self.pos += 1;
                }
                Some(c) => {
                    out.push(c);
                    self.pos += 1;
                }
            }
        }
        Ok(out)
    }

    fn parse_bool(&mut self) -> Result<JsonValue, ParseError> {
        if self.matches("true") {
            Ok(JsonValue::Bool(true))
        } else if self.matches("false") {
            Ok(JsonValue::Bool(false))
        } else {
            Err(ParseError(format!("Invalid literal at {}", self.pos)))
        }
    }

    fn parse_null(&mut self) -> Result<JsonValue, ParseError> {
        if self.matches("null") {
            Ok(JsonValue::Null)
        } else {
            Err(ParseError(format!("Invalid literal at {}", self.pos)))
        }
    }

    fn matches(&mut self, word: &str) -> bool {
        let end = self.pos + word.len();
        if end <= self.chars.len() && self.chars[self.pos..end].iter().collect::<String>() == word {
            self.pos = end;
            true
        } else {
            false
        }
    }

    fn parse_number(&mut self) -> Result<JsonValue, ParseError> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.pos += 1;
        }
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
        let mut has_fraction_or_exp = false;
        if self.peek() == Some('.') {
            has_fraction_or_exp = true;
            self.pos += 1;
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some('e' | 'E')) {
            has_fraction_or_exp = true;
            self.pos += 1;
            if matches!(self.peek(), Some('+' | '-')) {
                self.pos += 1;
            }
            while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        let token: String = self.chars[start..self.pos].iter().collect();
        let n: f64 = token
            .parse()
            .map_err(|_| ParseError(format!("Invalid number {token}")))?;

        // bigint-json number rule.
        if (MIN_SAFE..=MAX_SAFE).contains(&n) || has_fraction_or_exp {
            Ok(JsonValue::Number(n))
        } else {
            match token.parse::<num_bigint::BigInt>() {
                Ok(b) => Ok(JsonValue::BigInt(b)),
                Err(_) => Ok(JsonValue::Number(n)),
            }
        }
    }
}

/// Formats an `f64` the way `JSON.stringify` renders a JS number: integers with
/// no fractional part, otherwise the shortest round-tripping decimal. `NaN` and
/// infinities become `null` (as `JSON.stringify` does).
fn format_number(n: f64) -> String {
    if !n.is_finite() {
        return "null".to_string();
    }
    if n == 0.0 {
        return "0".to_string();
    }
    if n.fract() == 0.0 && n.abs() < 1e21 {
        return format!("{}", n as i128);
    }
    // ECMAScript `Number::toString` switches to exponential notation when the
    // base-10 exponent of the leading significant digit is >= 21 or <= -7;
    // otherwise the shortest round-tripping decimal already matches JS. Both
    // Rust and V8 emit the shortest round-tripping digit string, so only the
    // *shape* (decimal vs `e+`/`e-`) differs — derive the exponent from Rust's
    // `{:e}` form and reshape only when JS would.
    let sci = format!("{n:e}"); // e.g. "1.5e300", "-1e-7", "1e21"
    let (mantissa, exp_str) = sci.rsplit_once('e').expect("{:e} always contains 'e'");
    let exp: i32 = exp_str.parse().expect("{:e} exponent is an integer");
    if exp >= 21 || exp <= -7 {
        let sign = if exp >= 0 { '+' } else { '-' };
        format!("{mantissa}e{sign}{}", exp.abs())
    } else {
        // -6 <= exponent <= 20: JS renders plain decimal, which Rust matches.
        format!("{n}")
    }
}

/// Writes `s` as a JSON string literal, escaping per the JSON spec exactly as
/// `JSON.stringify` does (quotes, backslash, and control characters).
fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stringify_primitives() {
        assert_eq!(JsonValue::Null.stringify(), "null");
        assert_eq!(JsonValue::Bool(true).stringify(), "true");
        assert_eq!(JsonValue::Number(1.0).stringify(), "1");
        assert_eq!(JsonValue::Number(1.5).stringify(), "1.5");
        assert_eq!(JsonValue::String("ab\"c".into()).stringify(), "\"ab\\\"c\"");
    }

    #[test]
    fn format_number_matches_ecmascript_tostring() {
        let f = |n: f64| JsonValue::Number(n).stringify();
        // Exponential thresholds (JS: exponent >= 21 or <= -7).
        assert_eq!(f(1e-7), "1e-7");
        assert_eq!(f(5e-7), "5e-7");
        assert_eq!(f(1e21), "1e+21");
        assert_eq!(f(1.5e300), "1.5e+300");
        assert_eq!(f(-1e-7), "-1e-7");
        assert_eq!(f(1.23e21), "1.23e+21");
        // Just inside the decimal range — plain decimal, matching JS.
        assert_eq!(f(1e-6), "0.000001");
        assert_eq!(f(1e20), "100000000000000000000");
        assert_eq!(f(0.5), "0.5");
        assert_eq!(f(12.5), "12.5");
        assert_eq!(f(100.0), "100");
    }

    #[test]
    fn stringify_bigint_is_bare_token() {
        let v = JsonValue::Array(vec![
            JsonValue::String("n".into()),
            JsonValue::BigInt(BigInt::from(9_007_199_254_740_993i64)),
        ]);
        assert_eq!(v.stringify(), "[\"n\",9007199254740993]");
    }

    #[test]
    fn parse_roundtrip_and_bigint_rule() {
        assert_eq!(parse("null").unwrap(), JsonValue::Null);
        assert_eq!(parse("true").unwrap(), JsonValue::Bool(true));
        assert_eq!(parse("  1.5 ").unwrap(), JsonValue::Number(1.5));
        assert_eq!(
            parse(r#"["a","b"]"#).unwrap(),
            JsonValue::Array(vec![
                JsonValue::String("a".into()),
                JsonValue::String("b".into())
            ])
        );
        assert_eq!(
            parse(r#"{"foo":"bar"}"#).unwrap(),
            JsonValue::Object(vec![("foo".into(), JsonValue::String("bar".into()))])
        );
        // A large integer without a decimal point becomes a bigint.
        assert_eq!(
            parse("9007199254740993").unwrap(),
            JsonValue::BigInt(BigInt::from(9_007_199_254_740_993i64))
        );
        // A small integer stays a number.
        assert_eq!(parse("42").unwrap(), JsonValue::Number(42.0));
        // Round-trips through stringify.
        let v = parse(r#"{"a":[1,2],"b":"x\"y"}"#).unwrap();
        assert_eq!(v.stringify(), r#"{"a":[1,2],"b":"x\"y"}"#);
        assert!(parse("{bad}").is_err());
    }

    #[test]
    fn stringify_nested() {
        let v = JsonValue::Array(vec![
            JsonValue::String("foo".into()),
            JsonValue::Array(vec![JsonValue::String("bar".into())]),
        ]);
        assert_eq!(v.stringify(), "[\"foo\",[\"bar\"]]");
    }
}
