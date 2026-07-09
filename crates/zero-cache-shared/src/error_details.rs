//! Port of `zero-cache/src/observability/events.ts`'s `makeErrorDetails` —
//! the pure error->JSON-object mapping used to attach structured error
//! detail to a published `ZeroEvent`.
//!
//! First (and, on inspection, only pure-logic) piece of the previously
//! entirely-unmapped `zero-cache/src/observability` directory
//! (`events.ts`/`metrics.ts`, ~370 lines outside tests). `metrics.ts` is
//! pure OpenTelemetry counter/gauge/histogram factory boilerplate — no
//! decision logic, and this port has no OTel dependency to hang it on, so
//! it's deliberately not ported (same call as skipping upstream files that
//! are pure library-wiring, e.g. `zero-config.ts`'s CLI declarations).
//! `events.ts`'s own `initEventSink`/`publishEvent`/etc. are real I/O
//! (gzip + HTTP CloudEvent publishing via the `cloudevents` npm package,
//! itself needing a `NormalizedZeroConfig` + retry loop) — out of scope
//! for the same reason `fetchFromAPIServer`'s actual HTTP call was scoped
//! separately from its pure request-construction logic elsewhere in this
//! port; nothing currently produces a `ZeroEvent` to publish anyway (no
//! `zero-events` crate exists), so there's no live caller yet.
//!
//! Scope deviation from the JS original, which operates on `unknown`
//! (anything can be `throw`n, and JS's duck-typed `Error` exposes
//! `.name`/`.message`/`.stack`/arbitrary enumerable own-properties on
//! subclasses): this port takes a `&(dyn std::error::Error + 'static)`
//! instead, which only reliably offers a `Display` message and an optional
//! `.source()` cause chain — Rust has no `.stack`/arbitrary-property
//! equivalent to walk. So [`error_details`] emits `message` (via
//! `to_string()`) and a recursive `cause` (via `.source()`), and
//! deliberately omits `name`/`stack`/extra fields rather than fabricating
//! them.

use crate::bigint_json::JsonValue;

/// Port of `makeErrorDetails`. Port of the `err.cause` recursion (`err.cause
/// ? makeErrorDetails(err.cause) : undefined`) via `std::error::Error::source()`.
pub fn error_details(e: &(dyn std::error::Error + 'static)) -> JsonValue {
    let mut fields = vec![("message".to_string(), JsonValue::String(e.to_string()))];
    if let Some(cause) = e.source() {
        fields.push(("cause".to_string(), error_details(cause)));
    }
    JsonValue::Object(fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt;

    #[derive(Debug)]
    struct Leaf;
    impl fmt::Display for Leaf {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "leaf failure")
        }
    }
    impl std::error::Error for Leaf {}

    #[derive(Debug)]
    struct Wrapper(Leaf);
    impl fmt::Display for Wrapper {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "wrapper failure")
        }
    }
    impl std::error::Error for Wrapper {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn error_with_no_cause_has_only_a_message() {
        let details = error_details(&Leaf);
        assert_eq!(
            details,
            JsonValue::Object(vec![(
                "message".to_string(),
                JsonValue::String("leaf failure".to_string())
            )])
        );
    }

    #[test]
    fn error_with_a_cause_recurses() {
        let details = error_details(&Wrapper(Leaf));
        match details {
            JsonValue::Object(fields) => {
                assert_eq!(
                    fields[0],
                    (
                        "message".to_string(),
                        JsonValue::String("wrapper failure".to_string())
                    )
                );
                let (cause_key, cause_value) = &fields[1];
                assert_eq!(cause_key, "cause");
                assert_eq!(
                    *cause_value,
                    JsonValue::Object(vec![(
                        "message".to_string(),
                        JsonValue::String("leaf failure".to_string())
                    )])
                );
            }
            _ => panic!("expected an object"),
        }
    }
}
