//! Port of `zero-cache/src/types/url-params.ts`'s `URLParams` — the
//! typed query-string accessor `getConnectParams` (`connect_params.rs`)
//! uses to pull `clientID`/`ts`/`lmid`/etc. out of a connect URL.
//!
//! Scope deviation: upstream wraps a real `URL` (`this.url.searchParams`).
//! This crate has no URL-parsing dependency, so `UrlParams` wraps an
//! already-parsed `&[(String, String)]` of query pairs instead (a caller
//! parses the URL's query string however it likes — e.g. `url::Url` if
//! this port ever adds that dependency for the real HTTP server — and
//! hands the pairs in). The accessor semantics (missing/empty value ==
//! absent, `required` throws vs. returns `None`) are ported exactly.

use std::fmt;

/// Port of the `Error` thrown by `URLParams.get`/`getInteger`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub struct UrlParamsError(pub String);

impl fmt::Display for UrlParamsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Port of `URLParams`.
pub struct UrlParams<'a> {
    pairs: &'a [(String, String)],
}

impl<'a> UrlParams<'a> {
    pub fn new(pairs: &'a [(String, String)]) -> Self {
        UrlParams { pairs }
    }

    fn raw_get(&self, name: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// Port of `URLParams.get`. `required = true` errors when the param is
    /// missing or empty (`""`); `required = false` returns `Ok(None)`.
    pub fn get(&self, name: &str, required: bool) -> Result<Option<String>, UrlParamsError> {
        match self.raw_get(name) {
            Some(v) if !v.is_empty() => Ok(Some(v.to_string())),
            _ => {
                if required {
                    Err(UrlParamsError(format!(
                        "invalid querystring - missing {name}"
                    )))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Port of `URLParams.get(name, true)`'s non-nullable overload — for
    /// callers that already know the param is required.
    pub fn get_required(&self, name: &str) -> Result<String, UrlParamsError> {
        Ok(self
            .get(name, true)?
            .expect("required get() always returns Some on success"))
    }

    /// Port of `URLParams.getInteger`.
    pub fn get_integer(&self, name: &str, required: bool) -> Result<Option<i64>, UrlParamsError> {
        let Some(value) = self.get(name, required)? else {
            return Ok(None);
        };
        value.trim().parse::<i64>().map(Some).map_err(|_| {
            UrlParamsError(format!(
                "invalid querystring parameter {name}, got: {value}"
            ))
        })
    }

    pub fn get_integer_required(&self, name: &str) -> Result<i64, UrlParamsError> {
        Ok(self
            .get_integer(name, true)?
            .expect("required getInteger() always returns Some on success"))
    }

    /// Port of `URLParams.getBoolean` — absent means `false`, any value
    /// other than the literal string `"true"` also means `false` (matching
    /// upstream's `value === 'true'`, not a general bool parse).
    pub fn get_boolean(&self, name: &str) -> bool {
        matches!(self.get(name, false), Ok(Some(v)) if v == "true")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn get_required_returns_value() {
        let pairs = params(&[("clientID", "c1")]);
        let p = UrlParams::new(&pairs);
        assert_eq!(p.get_required("clientID").unwrap(), "c1");
    }

    #[test]
    fn get_required_missing_errors() {
        let pairs = params(&[]);
        let p = UrlParams::new(&pairs);
        let err = p.get_required("clientID").unwrap_err();
        assert_eq!(err.0, "invalid querystring - missing clientID");
    }

    #[test]
    fn get_required_empty_value_errors_same_as_missing() {
        let pairs = params(&[("clientID", "")]);
        let p = UrlParams::new(&pairs);
        assert!(p.get_required("clientID").is_err());
    }

    #[test]
    fn get_optional_missing_returns_none() {
        let pairs = params(&[]);
        let p = UrlParams::new(&pairs);
        assert_eq!(p.get("profileID", false).unwrap(), None);
    }

    #[test]
    fn get_integer_parses_valid_int() {
        let pairs = params(&[("ts", "12345")]);
        let p = UrlParams::new(&pairs);
        assert_eq!(p.get_integer_required("ts").unwrap(), 12345);
    }

    #[test]
    fn get_integer_invalid_errors() {
        let pairs = params(&[("ts", "not-a-number")]);
        let p = UrlParams::new(&pairs);
        assert!(p.get_integer("ts", true).is_err());
    }

    #[test]
    fn get_integer_optional_missing_returns_none() {
        let pairs = params(&[]);
        let p = UrlParams::new(&pairs);
        assert_eq!(p.get_integer("lmid", false).unwrap(), None);
    }

    #[test]
    fn get_boolean_true_string_is_true() {
        let pairs = params(&[("debugPerf", "true")]);
        let p = UrlParams::new(&pairs);
        assert!(p.get_boolean("debugPerf"));
    }

    #[test]
    fn get_boolean_missing_is_false() {
        let pairs = params(&[]);
        let p = UrlParams::new(&pairs);
        assert!(!p.get_boolean("debugPerf"));
    }

    #[test]
    fn get_boolean_non_true_value_is_false() {
        let pairs = params(&[("debugPerf", "yes")]);
        let p = UrlParams::new(&pairs);
        assert!(!p.get_boolean("debugPerf"));
    }
}
