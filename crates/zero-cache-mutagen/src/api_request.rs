//! Port of the pure request-construction logic in `custom/fetch.ts`'s
//! `fetchFromAPIServer` — the piece that builds the actual HTTP request
//! `pusher_batch`'s batched pushes (and, eventually, custom query
//! transforms) get forwarded to a user's API server as. This is NOT the
//! HTTP call itself (no HTTP client dependency exists in this port yet —
//! see module doc on scope) but the request-shape logic around it:
//! exponential-backoff-with-jitter delay calculation, header assembly, and
//! reserved-query-param validation + URL construction.
//!
//! Scope: [`get_backoff_delay_ms`], [`build_request_headers`], and
//! [`build_final_url`] are ported. NOT ported: the actual `fetch()` call
//! and retry loop (`MAX_ATTEMPTS` attempts, `fetch failed`/502/504
//! retry-worthiness classification), `urlMatch`/`compileUrlPattern`
//! (needs a URL-pattern-matching library this port doesn't depend on),
//! response parsing (`apiErrorFromResult`/`errorBodySchema` validation),
//! and all the OpenTelemetry metrics recording (`apiRequestMetricAttrs`
//! etc. — observability, not request-construction logic). `jitter` is
//! taken as a parameter to `get_backoff_delay_ms` (a value the caller
//! already computed, e.g. via a seeded RNG) rather than calling
//! `Math.random()` internally, matching this port's established
//! determinism convention for anything upstream seeds with `Math.random`.

/// Port of `getBackoffDelayMs`: `min(1000, 100 * 2^(attempt-1) + jitter)`.
/// `attempt` is 1-based (matching upstream's "assumes the first retry is
/// attempt 1"). `jitter_ms` stands in for upstream's inline
/// `Math.random() * 100` — pass a value in `[0, 100)` for equivalent
/// behavior, or a fixed value for deterministic tests.
pub fn get_backoff_delay_ms(attempt: u32, jitter_ms: f64) -> f64 {
    let base = 100.0 * 2f64.powi(attempt as i32 - 1);
    (base + jitter_ms).min(1000.0)
}

/// Port of the header-assembly block in `fetchFromAPIServer`: builds the
/// ordered header list for the outgoing request. Order matters for
/// matching upstream's `Object.assign` overwrite semantics (a
/// `request_headers` entry overrides a same-named `custom_headers` entry,
/// matching `Object.assign(headers, customHeaders, requestHeaders)`), not
/// for wire correctness (HTTP headers are unordered) — kept as a `Vec`
/// rather than a `HashMap` so tests can assert the exact override
/// behavior. `propagation.inject` (OpenTelemetry trace-context injection)
/// is not ported — this crate has no tracing integration.
#[derive(Debug, Clone, Default)]
pub struct HeaderOptions<'a> {
    pub api_key: Option<&'a str>,
    pub custom_headers: &'a [(String, String)],
    pub request_headers: &'a [(String, String)],
    pub auth_raw: Option<&'a str>,
    pub cookie: Option<&'a str>,
    pub origin: Option<&'a str>,
}

pub fn build_request_headers(opts: &HeaderOptions) -> Vec<(String, String)> {
    let mut headers: Vec<(String, String)> =
        vec![("Content-Type".to_string(), "application/json".to_string())];

    if let Some(api_key) = opts.api_key {
        headers.push(("X-Api-Key".to_string(), api_key.to_string()));
    }

    // Object.assign(headers, customHeaders, requestHeaders): later entries
    // (by key) overwrite earlier ones; iteration order otherwise preserved.
    for (k, v) in opts
        .custom_headers
        .iter()
        .chain(opts.request_headers.iter())
    {
        if let Some(existing) = headers.iter_mut().find(|(hk, _)| hk == k) {
            existing.1 = v.clone();
        } else {
            headers.push((k.clone(), v.clone()));
        }
    }

    if let Some(auth) = opts.auth_raw {
        headers.push(("Authorization".to_string(), format!("Bearer {auth}")));
    }
    if let Some(cookie) = opts.cookie {
        headers.push(("Cookie".to_string(), cookie.to_string()));
    }
    if let Some(origin) = opts.origin {
        headers.push(("Origin".to_string(), origin.to_string()));
    }

    headers
}

/// Query param names the push URL may not itself contain — `zero-cache`
/// appends these itself. Port of `reservedParams`.
pub const RESERVED_PARAMS: &[&str] = &["schema", "appID"];

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("The push URL cannot contain the reserved query param {0:?}")]
pub struct ReservedParamError(pub String);

/// Port of the reserved-param check + `schema`/`appID` query-param
/// append in `fetchFromAPIServer`. `url` must already have had its
/// `allowedUrlPatterns` check performed by the caller (`urlMatch`, not
/// ported — see module doc). Returns the final URL with `schema`/`appID`
/// appended, or an error if `url` already has a reserved param.
///
/// Minimal URL/query manipulation (no `url`-crate dependency): splits on
/// the first `?`, parses `&`-separated `key=value` pairs without
/// percent-decoding keys (matching how `reservedParams` are checked
/// case-sensitively against literal param names, which are never
/// percent-encoded in practice), and appends the two new params.
pub fn build_final_url(
    url: &str,
    schema: &str,
    app_id: &str,
) -> Result<String, ReservedParamError> {
    let (base, existing_query) = match url.split_once('?') {
        Some((b, q)) => (b, q),
        None => (url, ""),
    };

    for pair in existing_query.split('&').filter(|s| !s.is_empty()) {
        let key = pair.split('=').next().unwrap_or("");
        if RESERVED_PARAMS.contains(&key) {
            return Err(ReservedParamError(key.to_string()));
        }
    }

    let mut query = existing_query.to_string();
    for (k, v) in [("schema", schema), ("appID", app_id)] {
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str(&urlencode(k));
        query.push('=');
        query.push_str(&urlencode(v));
    }

    Ok(format!("{base}?{query}"))
}

/// Minimal `application/x-www-form-urlencoded`-style percent-encoding for
/// query values — just enough for typical schema/appID identifiers
/// (`[a-z0-9_]+`, per `zero_cache_types::shards`' own app-id validation)
/// plus common URL-unsafe characters, not a full RFC 3986 encoder.
fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_delay_grows_exponentially() {
        assert_eq!(get_backoff_delay_ms(1, 0.0), 100.0);
        assert_eq!(get_backoff_delay_ms(2, 0.0), 200.0);
        assert_eq!(get_backoff_delay_ms(3, 0.0), 400.0);
    }

    #[test]
    fn backoff_delay_includes_jitter() {
        assert_eq!(get_backoff_delay_ms(1, 50.0), 150.0);
    }

    #[test]
    fn backoff_delay_caps_at_1000() {
        assert_eq!(get_backoff_delay_ms(10, 99.0), 1000.0);
    }

    #[test]
    fn headers_include_content_type_by_default() {
        let headers = build_request_headers(&HeaderOptions::default());
        assert_eq!(
            headers,
            vec![("Content-Type".to_string(), "application/json".to_string())]
        );
    }

    #[test]
    fn headers_include_api_key_when_present() {
        let headers = build_request_headers(&HeaderOptions {
            api_key: Some("k1"),
            ..Default::default()
        });
        assert!(headers.contains(&("X-Api-Key".to_string(), "k1".to_string())));
    }

    #[test]
    fn request_headers_override_custom_headers_with_same_key() {
        let custom = vec![("X-Foo".to_string(), "custom".to_string())];
        let request = vec![("X-Foo".to_string(), "request".to_string())];
        let headers = build_request_headers(&HeaderOptions {
            custom_headers: &custom,
            request_headers: &request,
            ..Default::default()
        });
        let foo: Vec<_> = headers.iter().filter(|(k, _)| k == "X-Foo").collect();
        assert_eq!(foo.len(), 1, "should not duplicate the key");
        assert_eq!(foo[0].1, "request");
    }

    #[test]
    fn headers_include_auth_cookie_origin_when_present() {
        let headers = build_request_headers(&HeaderOptions {
            auth_raw: Some("tok"),
            cookie: Some("c=1"),
            origin: Some("https://x"),
            ..Default::default()
        });
        assert!(headers.contains(&("Authorization".to_string(), "Bearer tok".to_string())));
        assert!(headers.contains(&("Cookie".to_string(), "c=1".to_string())));
        assert!(headers.contains(&("Origin".to_string(), "https://x".to_string())));
    }

    #[test]
    fn build_final_url_appends_schema_and_app_id() {
        let url = build_final_url("https://api.example.com/push", "myapp_0", "myapp").unwrap();
        assert_eq!(
            url,
            "https://api.example.com/push?schema=myapp_0&appID=myapp"
        );
    }

    #[test]
    fn build_final_url_preserves_existing_query_params() {
        let url = build_final_url("https://api.example.com/push?foo=bar", "s", "a").unwrap();
        assert_eq!(url, "https://api.example.com/push?foo=bar&schema=s&appID=a");
    }

    #[test]
    fn build_final_url_rejects_reserved_schema_param() {
        let err = build_final_url("https://api.example.com/push?schema=x", "s", "a").unwrap_err();
        assert_eq!(err, ReservedParamError("schema".to_string()));
    }

    #[test]
    fn build_final_url_rejects_reserved_app_id_param() {
        let err = build_final_url("https://api.example.com/push?appID=x", "s", "a").unwrap_err();
        assert_eq!(err, ReservedParamError("appID".to_string()));
    }

    #[test]
    fn build_final_url_percent_encodes_values() {
        let url = build_final_url("https://api.example.com/push", "my app", "a").unwrap();
        assert_eq!(url, "https://api.example.com/push?schema=my%20app&appID=a");
    }
}
