//! Port of the pure pieces of `observability/events.ts`'s `initEventSink`/
//! `publishFn` CloudEvent-publishing path — the retry-backoff timing and
//! the `extensionOverridesEnv` JSON validation. This closes out
//! `events.ts`'s pure surface (previously flagged as a low-value, unattempted
//! candidate across several rounds): everything else in the file is real
//! I/O (gzip, HTTP `CloudEvent` emit via the `cloudevents` npm package) or
//! trivial object-literal construction with no decision logic of its own
//! (`createCloudEvent`'s `{...overrides}` spread is a plain last-writer-
//! wins merge, not modeled separately here — see [`apply_extension_overrides`]
//! for the one place that spread actually needed porting: applying the
//! validated overrides onto a base attribute list).
//!
//! Not ported: `initEventSink`/`publishEvent`/`publishCriticalEvent`
//! themselves (real I/O + process-global `publishFn` mutable state), the
//! `base64gzip` encoding step, and the actual CloudEvent HTTP transport —
//! all need the `cloudevents` npm package's real equivalent, which this
//! port doesn't depend on, and nothing in this port produces a `ZeroEvent`
//! to publish yet anyway (no `zero-events` crate exists).

use crate::bigint_json::JsonValue;

/// Port of `MAX_PUBLISH_ATTEMPTS`.
pub const MAX_PUBLISH_ATTEMPTS: u32 = 6;
/// Port of `INITIAL_PUBLISH_BACKOFF_MS`.
pub const INITIAL_PUBLISH_BACKOFF_MS: f64 = 500.0;

/// Port of the retry loop's backoff formula (`INITIAL_PUBLISH_BACKOFF_MS *
/// 2 ** (i - 1)`, for `i` from `1` up to `MAX_PUBLISH_ATTEMPTS - 1` — the
/// loop's first iteration, `i === 0`, never sleeps). `attempt` is 1-based,
/// matching every other backoff port in this crate/workspace
/// (`api_request::get_backoff_delay_ms`'s convention) — unlike that
/// sibling function, upstream applies NO jitter here and NO cap, so
/// neither is added.
pub fn publish_backoff_delay_ms(attempt: u32) -> f64 {
    INITIAL_PUBLISH_BACKOFF_MS * 2f64.powi(attempt as i32 - 1)
}

/// An attribute value in an event/extensions record. Port of
/// `attributeValueSchema` (`string | number | boolean`).
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    String(String),
    Number(f64),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExtensionOverridesError {
    #[error("expected a JSON object")]
    NotAnObject,
    #[error("expected an \"extensions\" object field")]
    MissingExtensions,
    #[error("extension {0:?} must be a string, number, or boolean")]
    InvalidAttributeValue(String),
}

/// Port of `v.parse(JSON.parse(strVal), extensionsObjectSchema)`'s
/// validation half (JSON *parsing* itself is the caller's job via
/// `zero_cache_shared::bigint_json::parse`, matching this crate's
/// established split between generic JSON parsing and schema validation
/// seen elsewhere in this port, e.g. `ast_json.rs`). Validates that `v` is
/// `{"extensions": {...}}` where every value in `extensions` is a string,
/// number, or boolean, returning the validated `(key, value)` pairs in
/// their original JSON order (matching upstream's `PartialEvent` — a
/// plain `Record`, order-preserving under `JSON.parse`/object iteration).
pub fn parse_extension_overrides(
    v: &JsonValue,
) -> Result<Vec<(String, AttributeValue)>, ExtensionOverridesError> {
    let JsonValue::Object(fields) = v else {
        return Err(ExtensionOverridesError::NotAnObject);
    };
    let Some((_, extensions)) = fields.iter().find(|(k, _)| k == "extensions") else {
        return Err(ExtensionOverridesError::MissingExtensions);
    };
    let JsonValue::Object(entries) = extensions else {
        return Err(ExtensionOverridesError::MissingExtensions);
    };
    entries
        .iter()
        .map(|(k, v)| {
            let value = match v {
                JsonValue::String(s) => AttributeValue::String(s.clone()),
                JsonValue::Number(n) => AttributeValue::Number(*n),
                JsonValue::Bool(b) => AttributeValue::Bool(*b),
                _ => return Err(ExtensionOverridesError::InvalidAttributeValue(k.clone())),
            };
            Ok((k.clone(), value))
        })
        .collect()
}

/// Port of `createCloudEvent`'s `{...overrides}` spread onto the base
/// CloudEvent attributes: applies `overrides` on top of `base`, with a
/// same-key override replacing (not merging into) the base entry, and any
/// new key appended — plain `Object.assign` semantics, matching the same
/// override-list pattern `zero_cache_mutagen::api_request::build_request_headers`
/// already established for an analogous "later entries win" merge.
pub fn apply_extension_overrides(
    base: &[(String, AttributeValue)],
    overrides: &[(String, AttributeValue)],
) -> Vec<(String, AttributeValue)> {
    let mut result = base.to_vec();
    for (k, v) in overrides {
        if let Some(existing) = result.iter_mut().find(|(ek, _)| ek == k) {
            existing.1 = v.clone();
        } else {
            result.push((k.clone(), v.clone()));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bigint_json::parse as parse_json;

    #[test]
    fn backoff_delay_doubles_each_attempt_with_no_jitter_or_cap() {
        assert_eq!(publish_backoff_delay_ms(1), 500.0);
        assert_eq!(publish_backoff_delay_ms(2), 1000.0);
        assert_eq!(publish_backoff_delay_ms(3), 2000.0);
        assert_eq!(publish_backoff_delay_ms(5), 8000.0);
    }

    #[test]
    fn parses_valid_extension_overrides() {
        let v = parse_json(r#"{"extensions":{"region":"us-east","attempt":3,"critical":true}}"#)
            .unwrap();
        let parsed = parse_extension_overrides(&v).unwrap();
        assert_eq!(
            parsed,
            vec![
                (
                    "region".to_string(),
                    AttributeValue::String("us-east".to_string())
                ),
                ("attempt".to_string(), AttributeValue::Number(3.0)),
                ("critical".to_string(), AttributeValue::Bool(true))
            ]
        );
    }

    #[test]
    fn rejects_a_non_object_value() {
        let v = parse_json(r#"[1,2,3]"#).unwrap();
        assert_eq!(
            parse_extension_overrides(&v),
            Err(ExtensionOverridesError::NotAnObject)
        );
    }

    #[test]
    fn rejects_a_missing_extensions_field() {
        let v = parse_json(r#"{"other":1}"#).unwrap();
        assert_eq!(
            parse_extension_overrides(&v),
            Err(ExtensionOverridesError::MissingExtensions)
        );
    }

    #[test]
    fn rejects_an_invalid_attribute_value_type() {
        let v = parse_json(r#"{"extensions":{"bad":[1,2]}}"#).unwrap();
        assert_eq!(
            parse_extension_overrides(&v),
            Err(ExtensionOverridesError::InvalidAttributeValue(
                "bad".to_string()
            ))
        );
    }

    #[test]
    fn apply_overrides_replaces_matching_keys_and_appends_new_ones() {
        let base = vec![
            ("id".to_string(), AttributeValue::String("evt1".to_string())),
            (
                "source".to_string(),
                AttributeValue::String("taskA".to_string()),
            ),
        ];
        let overrides = vec![
            (
                "source".to_string(),
                AttributeValue::String("taskB".to_string()),
            ),
            (
                "region".to_string(),
                AttributeValue::String("us-east".to_string()),
            ),
        ];
        let result = apply_extension_overrides(&base, &overrides);
        assert_eq!(
            result,
            vec![
                ("id".to_string(), AttributeValue::String("evt1".to_string())),
                (
                    "source".to_string(),
                    AttributeValue::String("taskB".to_string())
                ),
                (
                    "region".to_string(),
                    AttributeValue::String("us-east".to_string())
                ),
            ]
        );
    }
}
