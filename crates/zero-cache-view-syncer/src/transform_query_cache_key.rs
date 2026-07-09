//! Port of `custom-queries/transform-query.ts`'s `getCacheKey`/
//! `normalizedHeaders` — the cache-key computation
//! `CustomQueryTransformer` uses to key its `TimedCache` (now ported as
//! `zero_cache_shared::timed_cache::TimedCache`) of transformed custom-
//! query results. Unblocked by `connection_context_manager.rs`'s
//! `ConnectionFetchContext` addition last round.
//!
//! Scope: ports the pure key-computation only — `CustomQueryTransformer`
//! itself (the class that calls the user's API server, wraps this in a
//! `TimedCache`, and handles the transform-request/response protocol)
//! is NOT ported; that needs `TransformRequestBody`/`QueryResponseBody`
//! wire types and the request/response flow, a separate, larger slice.
//! `token` is taken as an explicit `Option<&str>` parameter rather than
//! read off `ctx.auth?.raw` — this port's `ConnectionContext` deliberately
//! doesn't carry `auth` yet (a separate, unported piece), matching the
//! same "carry what's needed as a parameter" pattern used throughout this
//! module's neighbors.

use zero_cache_shared::bigint_json::JsonValue;

use crate::connection_context_manager::{ConnectionFetchContext, UserState};

/// Port of `normalizedHeaders`: sorts a header map by key (this port's
/// `BTreeMap<String, String>` is already key-sorted by construction, so
/// this is really just "stringify the sorted entries, or `None` if
/// empty/absent" — `sortedEntries` itself needs no separate port here)
/// and JSON-encodes it as an array of `[key, value]` pairs, matching
/// upstream's `JSON.stringify(sortedEntries(headers))`.
pub fn normalized_headers(
    headers: &Option<std::collections::BTreeMap<String, String>>,
) -> Option<String> {
    let headers = headers.as_ref()?;
    if headers.is_empty() {
        return None;
    }
    let entries: Vec<JsonValue> = headers
        .iter()
        .map(|(k, v)| {
            JsonValue::Array(vec![
                JsonValue::String(k.clone()),
                JsonValue::String(v.clone()),
            ])
        })
        .collect();
    Some(JsonValue::Array(entries).stringify())
}

fn user_json(user: &UserState) -> JsonValue {
    JsonValue::Object(vec![(
        "id".to_string(),
        user.id
            .clone()
            .map(JsonValue::String)
            .unwrap_or(JsonValue::Null),
    )])
}

fn opt_string_json(v: &Option<String>) -> JsonValue {
    v.clone().map(JsonValue::String).unwrap_or(JsonValue::Null)
}

fn opt_str_json(v: Option<&str>) -> JsonValue {
    v.map(|s| JsonValue::String(s.to_string()))
        .unwrap_or(JsonValue::Null)
}

/// Port of `getCacheKey`. Field order matches upstream's object literal
/// exactly (`queryID`, `token`, `cookie`, `origin`, `userID`, `url`,
/// `customHeaders`, `requestHeaders`) since `JSON.stringify`/this port's
/// `JsonValue::Object` both preserve insertion order — two calls with the
/// same logical inputs always produce the same string, which is the
/// actual property the cache needs.
pub fn get_cache_key(
    query_id: &str,
    token: Option<&str>,
    user: &UserState,
    query_context: &ConnectionFetchContext,
) -> String {
    let opts = &query_context.header_options;
    JsonValue::Object(vec![
        (
            "queryID".to_string(),
            JsonValue::String(query_id.to_string()),
        ),
        ("token".to_string(), opt_str_json(token)),
        ("cookie".to_string(), opt_string_json(&opts.cookie)),
        ("origin".to_string(), opt_string_json(&opts.origin)),
        ("userID".to_string(), user_json(user)),
        ("url".to_string(), opt_string_json(&query_context.url)),
        (
            "customHeaders".to_string(),
            normalized_headers(&opts.custom_headers)
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
        ),
        (
            "requestHeaders".to_string(),
            normalized_headers(&opts.request_headers)
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
        ),
    ])
    .stringify()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection_context_manager::HeaderOptions;
    use std::collections::BTreeMap;

    fn user(id: Option<&str>) -> UserState {
        UserState {
            id: id.map(String::from),
        }
    }

    #[test]
    fn normalized_headers_none_for_absent_or_empty() {
        assert_eq!(normalized_headers(&None), None);
        assert_eq!(normalized_headers(&Some(BTreeMap::new())), None);
    }

    #[test]
    fn normalized_headers_sorts_and_encodes_as_pairs() {
        let mut headers = BTreeMap::new();
        headers.insert("z-header".to_string(), "z".to_string());
        headers.insert("a-header".to_string(), "a".to_string());
        assert_eq!(
            normalized_headers(&Some(headers)).unwrap(),
            r#"[["a-header","a"],["z-header","z"]]"#
        );
    }

    #[test]
    fn get_cache_key_includes_query_id_and_user() {
        let ctx = ConnectionFetchContext::default();
        let key = get_cache_key("q1", None, &user(Some("u1")), &ctx);
        assert!(key.contains(r#""queryID":"q1""#));
        assert!(key.contains(r#""userID":{"id":"u1"}"#));
    }

    #[test]
    fn get_cache_key_uses_null_for_absent_fields() {
        let ctx = ConnectionFetchContext::default();
        let key = get_cache_key("q1", None, &user(None), &ctx);
        assert!(key.contains(r#""token":null"#));
        assert!(key.contains(r#""cookie":null"#));
        assert!(key.contains(r#""userID":{"id":null}"#));
        assert!(key.contains(r#""url":null"#));
    }

    #[test]
    fn get_cache_key_is_stable_for_identical_inputs() {
        let ctx = ConnectionFetchContext {
            url: Some("https://api.example".into()),
            header_options: HeaderOptions {
                cookie: Some("s=1".into()),
                ..Default::default()
            },
            ..Default::default()
        };
        let key1 = get_cache_key("q1", Some("tok"), &user(Some("u1")), &ctx);
        let key2 = get_cache_key("q1", Some("tok"), &user(Some("u1")), &ctx);
        assert_eq!(
            key1, key2,
            "identical logical inputs must produce identical cache keys"
        );
    }

    #[test]
    fn get_cache_key_differs_when_headers_differ() {
        let mut headers1 = BTreeMap::new();
        headers1.insert("x".to_string(), "1".to_string());
        let mut headers2 = BTreeMap::new();
        headers2.insert("x".to_string(), "2".to_string());

        let ctx1 = ConnectionFetchContext {
            header_options: HeaderOptions {
                custom_headers: Some(headers1),
                ..Default::default()
            },
            ..Default::default()
        };
        let ctx2 = ConnectionFetchContext {
            header_options: HeaderOptions {
                custom_headers: Some(headers2),
                ..Default::default()
            },
            ..Default::default()
        };

        assert_ne!(
            get_cache_key("q1", None, &user(Some("u1")), &ctx1),
            get_cache_key("q1", None, &user(Some("u1")), &ctx2)
        );
    }
}
