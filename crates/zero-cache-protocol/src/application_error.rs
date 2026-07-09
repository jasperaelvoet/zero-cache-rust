//! Port of `zero-protocol/src/application-error.ts`.
//!
//! `ApplicationError` is the error type application code throws to surface
//! structured, JSON-serializable metadata (`details`) back to the client —
//! it maps onto transform and push app-level failures. Upstream carries a
//! generic `T extends ReadonlyJSONValue | undefined` for `details`; this port
//! models `details` as an `Option<JsonValue>` (the JSON value type the rest of
//! the protocol crate uses), since Rust has no equivalent of the TS const type
//! parameter and every wire consumer treats `details` as opaque JSON anyway.
//!
//! Upstream's `isApplicationError` (a JS `instanceof` guard) has no faithful
//! Rust analogue — a caller with an `ApplicationError` value already has the
//! type — so it is intentionally not ported; [`wrap_message`] provides the
//! useful half of `wrapWithApplicationError` (turn an arbitrary error's message
//! into an `ApplicationError`).

use zero_cache_shared::bigint_json::JsonValue;

/// Port of `ApplicationError`. Carries a human-readable `message` and optional
/// JSON `details`; its `kind()` is always `"Application"`, matching the
/// discriminant the wire error envelope keys off.
#[derive(Debug, Clone, PartialEq)]
pub struct ApplicationError {
    message: String,
    details: Option<JsonValue>,
}

impl ApplicationError {
    /// Port of `new ApplicationError(message)` with no `details`.
    pub fn new(message: impl Into<String>) -> Self {
        ApplicationError {
            message: message.into(),
            details: None,
        }
    }

    /// Port of `new ApplicationError(message, {details})`.
    pub fn with_details(message: impl Into<String>, details: JsonValue) -> Self {
        ApplicationError {
            message: message.into(),
            details: Some(details),
        }
    }

    /// Port of the `details` getter.
    pub fn details(&self) -> Option<&JsonValue> {
        self.details.as_ref()
    }

    /// Port of the `message` (from `Error`).
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Port of the `kind` getter — always `"Application"`.
    pub fn kind(&self) -> &'static str {
        "Application"
    }

    /// The useful half of `wrapWithApplicationError`: wrap any error's message
    /// into an `ApplicationError` (with no details). Upstream additionally
    /// extracts a `.details` property off the source error and short-circuits
    /// when the source is already an `ApplicationError` — both JS-object
    /// behaviors with no faithful Rust analogue, so this only carries the
    /// message forward.
    pub fn wrap_message(error: &(impl std::fmt::Display + ?Sized)) -> Self {
        ApplicationError::new(error.to_string())
    }
}

impl std::fmt::Display for ApplicationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ApplicationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_is_always_application() {
        assert_eq!(ApplicationError::new("boom").kind(), "Application");
    }

    #[test]
    fn new_has_no_details_and_carries_the_message() {
        let e = ApplicationError::new("boom");
        assert_eq!(e.message(), "boom");
        assert_eq!(e.details(), None);
        assert_eq!(e.to_string(), "boom");
    }

    #[test]
    fn with_details_carries_json_details() {
        let details = JsonValue::Object(vec![("code".into(), JsonValue::Number(42.0))]);
        let e = ApplicationError::with_details("nope", details.clone());
        assert_eq!(e.message(), "nope");
        assert_eq!(e.details(), Some(&details));
    }

    #[test]
    fn wrap_message_carries_the_source_message_without_details() {
        let source = ApplicationError::with_details("inner", JsonValue::String("d".into()));
        let wrapped = ApplicationError::wrap_message(&source);
        assert_eq!(wrapped.message(), "inner");
        // wrap_message only forwards the message, not details (see doc).
        assert_eq!(wrapped.details(), None);
    }

    #[test]
    fn is_usable_as_a_std_error() {
        let e = ApplicationError::new("boom");
        let dyn_err: &dyn std::error::Error = &e;
        assert_eq!(dyn_err.to_string(), "boom");
    }
}
