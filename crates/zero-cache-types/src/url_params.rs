//! Port of `zero-cache/src/types/url-params.ts`'s `URLParams`.
//!
//! A typed accessor over a URL's query string, used to read the sync
//! connection handshake parameters. Mirrors `URLSearchParams.get` semantics:
//! an absent OR empty-string value is treated as missing, and `get`ting the
//! first value for a repeated key. Percent-decoding and `+`→space are handled
//! by `form_urlencoded` (the same decoding `URLSearchParams` performs).

/// Error from a required/typed lookup. `Display` matches the corresponding
/// upstream `throw new Error(...)` text so callers/tests can assert on it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct UrlParamError(pub String);

/// Port of `URLParams`. Holds the decoded query pairs plus the original URL
/// (for error messages), matching upstream's `readonly url`.
#[derive(Debug, Clone)]
pub struct UrlParams {
    url: String,
    pairs: Vec<(String, String)>,
}

impl UrlParams {
    /// Port of `new URLParams(url)`: parses `url`'s query string (the part
    /// after the first `?` and before any `#`).
    pub fn new(url: impl Into<String>) -> Self {
        let url = url.into();
        let query = url
            .split_once('?')
            .map(|(_, rest)| rest.split('#').next().unwrap_or(rest))
            .unwrap_or("");
        let pairs = form_urlencoded::parse(query.as_bytes())
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        UrlParams { url, pairs }
    }

    /// The first value for `name`, or `None` if it is absent or empty —
    /// `URLSearchParams.get` with upstream's `'' || null` collapse.
    fn first(&self, name: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
            .filter(|v| !v.is_empty())
    }

    /// Port of `get(name, true)`: the value, erroring if absent/empty.
    pub fn get_required(&self, name: &str) -> Result<String, UrlParamError> {
        self.first(name)
            .map(str::to_string)
            .ok_or_else(|| UrlParamError(format!("invalid querystring - missing {name}")))
    }

    /// Port of `get(name, false)`: the value or `None` (never errors).
    pub fn get_optional(&self, name: &str) -> Option<String> {
        self.first(name).map(str::to_string)
    }

    /// Port of `getInteger(name, true)`.
    pub fn get_integer_required(&self, name: &str) -> Result<i64, UrlParamError> {
        let value = self.get_required(name)?;
        self.parse_int(name, &value)
    }

    /// Port of `getInteger(name, false)`: `None` if absent/empty, else the
    /// parsed integer (erroring only on a present-but-unparseable value).
    pub fn get_integer_optional(&self, name: &str) -> Result<Option<i64>, UrlParamError> {
        match self.get_optional(name) {
            None => Ok(None),
            Some(value) => self.parse_int(name, &value).map(Some),
        }
    }

    /// Port of `getBoolean(name)`: `false` if absent/empty, else `value ==
    /// "true"`.
    pub fn get_boolean(&self, name: &str) -> bool {
        matches!(self.first(name), Some("true"))
    }

    /// Replicates JS `parseInt(value)` (base 10) closely enough for query
    /// params: skips leading ASCII whitespace, takes an optional sign and the
    /// leading run of digits, ignoring any trailing non-digits. Errors (like
    /// upstream's `isNaN` check) when no digits lead the value.
    fn parse_int(&self, name: &str, value: &str) -> Result<i64, UrlParamError> {
        let trimmed = value.trim_start_matches([' ', '\t', '\n', '\r', '\u{0c}']);
        let mut chars = trimmed.chars();
        let mut lead = String::new();
        match chars.clone().next() {
            Some(sign @ ('+' | '-')) => {
                lead.push(sign);
                chars.next();
            }
            _ => {}
        }
        for c in chars {
            if c.is_ascii_digit() {
                lead.push(c);
            } else {
                break;
            }
        }
        lead.parse::<i64>().map_err(|_| {
            UrlParamError(format!(
                "invalid querystring parameter {name}, got: {value}, url: {}",
                self.url
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_required_errors_when_absent_or_empty() {
        let p = UrlParams::new("ws://h/sync?a=1&empty=");
        assert_eq!(p.get_required("a").unwrap(), "1");
        assert_eq!(
            p.get_required("missing").unwrap_err(),
            UrlParamError("invalid querystring - missing missing".into())
        );
        // empty string is treated as missing.
        assert!(p.get_required("empty").is_err());
    }

    #[test]
    fn get_optional_is_none_for_absent_or_empty() {
        let p = UrlParams::new("ws://h/sync?a=1&empty=");
        assert_eq!(p.get_optional("a"), Some("1".to_string()));
        assert_eq!(p.get_optional("empty"), None);
        assert_eq!(p.get_optional("missing"), None);
    }

    #[test]
    fn integer_parsing_matches_parse_int() {
        let p = UrlParams::new("ws://h/sync?n=42&partial=12abc&neg=-7&bad=abc");
        assert_eq!(p.get_integer_required("n").unwrap(), 42);
        // parseInt-style: leading digits win.
        assert_eq!(p.get_integer_required("partial").unwrap(), 12);
        assert_eq!(p.get_integer_required("neg").unwrap(), -7);
        assert!(p.get_integer_required("bad").is_err());
        assert_eq!(p.get_integer_optional("missing").unwrap(), None);
    }

    #[test]
    fn boolean_is_true_only_for_literal_true() {
        let p = UrlParams::new("ws://h/sync?yes=true&no=false&weird=1");
        assert!(p.get_boolean("yes"));
        assert!(!p.get_boolean("no"));
        assert!(!p.get_boolean("weird"));
        assert!(!p.get_boolean("missing"));
    }

    #[test]
    fn decodes_percent_and_plus_like_urlsearchparams() {
        let p = UrlParams::new("ws://h/sync?name=a%20b+c&hash=x#frag=ignored");
        assert_eq!(p.get_required("name").unwrap(), "a b c");
        // the fragment is not part of the query.
        assert_eq!(p.get_required("hash").unwrap(), "x");
    }

    #[test]
    fn first_value_wins_for_repeated_keys() {
        let p = UrlParams::new("ws://h/sync?k=1&k=2");
        assert_eq!(p.get_required("k").unwrap(), "1");
    }
}
