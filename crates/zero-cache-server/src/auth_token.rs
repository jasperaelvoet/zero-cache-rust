//! JWT (HS256) validation for `ZERO_AUTH_SECRET`.
//!
//! Zero clients authenticate with a bearer JWT (carried in the connection's
//! `Sec-WebSocket-Protocol` payload or `initConnection`). When the server is
//! configured with an auth secret, every connection's token must be a valid
//! HS256 JWT signed with that secret and unexpired. This is a focused,
//! dependency-light implementation (HMAC-SHA256 + base64url + a claims check) —
//! no external JWT crate.

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum AuthError {
    #[error("malformed token")]
    Malformed,
    #[error("unsupported alg (only HS256)")]
    UnsupportedAlg,
    #[error("bad signature")]
    BadSignature,
    #[error("token expired")]
    Expired,
    #[error("missing token")]
    Missing,
    #[error("claim mismatch ({0})")]
    BadClaim(&'static str),
}

/// Validated claims of interest.
#[derive(Debug, Clone, PartialEq)]
pub struct Claims {
    /// `sub` — the authenticated user id (empty if absent).
    pub sub: String,
}

fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s.as_bytes())
        .ok()
}

/// Validates an HS256 JWT against `secret` at time `now_unix` (seconds).
///
/// Checks: three segments, header `alg == HS256`, HMAC-SHA256 signature over
/// `header.payload`, and `exp` (if present) is in the future. Returns the
/// token's [`Claims`] on success.
pub fn validate_jwt(
    token: &str,
    secret: &[u8],
    now_unix: u64,
    expected_iss: Option<&str>,
    expected_aud: Option<&str>,
) -> Result<Claims, AuthError> {
    let mut parts = token.split('.');
    let (header_b64, payload_b64, sig_b64) =
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(h), Some(p), Some(s), None) => (h, p, s),
            _ => return Err(AuthError::Malformed),
        };

    // Header: require alg HS256.
    let header = b64url_decode(header_b64).ok_or(AuthError::Malformed)?;
    let header = zero_cache_shared::bigint_json::parse(
        std::str::from_utf8(&header).map_err(|_| AuthError::Malformed)?,
    )
    .map_err(|_| AuthError::Malformed)?;
    let alg_ok = matches!(
        object_get(&header, "alg"),
        Some(zero_cache_shared::bigint_json::JsonValue::String(a)) if a == "HS256"
    );
    if !alg_ok {
        return Err(AuthError::UnsupportedAlg);
    }

    // Signature: HMAC-SHA256(header_b64 . payload_b64).
    let signing_input = format!("{header_b64}.{payload_b64}");
    let expected = b64url_decode(sig_b64).ok_or(AuthError::Malformed)?;
    let mut mac = <Hmac<Sha256>>::new_from_slice(secret).map_err(|_| AuthError::BadSignature)?;
    mac.update(signing_input.as_bytes());
    mac.verify_slice(&expected)
        .map_err(|_| AuthError::BadSignature)?;

    // Claims: exp (if present) must be in the future; capture sub.
    let payload = b64url_decode(payload_b64).ok_or(AuthError::Malformed)?;
    let payload = zero_cache_shared::bigint_json::parse(
        std::str::from_utf8(&payload).map_err(|_| AuthError::Malformed)?,
    )
    .map_err(|_| AuthError::Malformed)?;
    if let Some(zero_cache_shared::bigint_json::JsonValue::Number(exp)) =
        object_get(&payload, "exp")
    {
        if (*exp as u64) < now_unix {
            return Err(AuthError::Expired);
        }
    }
    // Issuer / audience, if the server requires them.
    if let Some(want) = expected_iss {
        let ok = matches!(
            object_get(&payload, "iss"),
            Some(zero_cache_shared::bigint_json::JsonValue::String(iss)) if iss == want
        );
        if !ok {
            return Err(AuthError::BadClaim("iss"));
        }
    }
    if let Some(want) = expected_aud {
        // `aud` may be a string or an array of strings.
        let ok = match object_get(&payload, "aud") {
            Some(zero_cache_shared::bigint_json::JsonValue::String(a)) => a == want,
            Some(zero_cache_shared::bigint_json::JsonValue::Array(items)) => items.iter().any(|v| {
                matches!(v, zero_cache_shared::bigint_json::JsonValue::String(a) if a == want)
            }),
            _ => false,
        };
        if !ok {
            return Err(AuthError::BadClaim("aud"));
        }
    }

    let sub = match object_get(&payload, "sub") {
        Some(zero_cache_shared::bigint_json::JsonValue::String(s)) => s.clone(),
        _ => String::new(),
    };
    Ok(Claims { sub })
}

fn object_get<'a>(
    v: &'a zero_cache_shared::bigint_json::JsonValue,
    key: &str,
) -> Option<&'a zero_cache_shared::bigint_json::JsonValue> {
    match v {
        zero_cache_shared::bigint_json::JsonValue::Object(fields) => {
            fields.iter().find(|(k, _)| k == key).map(|(_, val)| val)
        }
        _ => None,
    }
}

/// Extracts the auth token from a connection's decoded `Sec-WebSocket-Protocol`
/// payload (`{"initConnectionMessage":…,"authToken":"…"}`). Returns `None` if
/// absent.
pub fn token_from_sec_payload(payload: &str) -> Option<String> {
    let json = zero_cache_shared::bigint_json::parse(payload).ok()?;
    match object_get(&json, "authToken") {
        Some(zero_cache_shared::bigint_json::JsonValue::String(t)) if !t.is_empty() => {
            Some(t.clone())
        }
        _ => None,
    }
}

/// The connection-admission auth gate: `true` if the connection may proceed.
/// When `secret` is `None` auth is disabled (always allowed); otherwise the
/// connection's `Sec-WebSocket-Protocol` payload must carry a valid, unexpired
/// HS256 token signed with `secret`.
pub fn authorize_connection(
    sec_payload: Option<&str>,
    secret: Option<&[u8]>,
    now_unix: u64,
    issuer: Option<&str>,
    audience: Option<&str>,
) -> bool {
    match secret {
        None => true,
        Some(secret) => sec_payload
            .and_then(token_from_sec_payload)
            .and_then(|t| validate_jwt(&t, secret, now_unix, issuer, audience).ok())
            .is_some(),
    }
}

/// Current unix time in seconds (for `exp` checks).
pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mints an HS256 JWT for tests.
    fn mint(secret: &[u8], payload_json: &str) -> String {
        let b64 = |b: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b);
        let header = b64(br#"{"alg":"HS256","typ":"JWT"}"#);
        let payload = b64(payload_json.as_bytes());
        let signing_input = format!("{header}.{payload}");
        let mut mac = <Hmac<Sha256>>::new_from_slice(secret).unwrap();
        mac.update(signing_input.as_bytes());
        let sig = b64(&mac.finalize().into_bytes());
        format!("{signing_input}.{sig}")
    }

    #[test]
    fn valid_token_passes_and_returns_sub() {
        let secret = b"topsecret";
        let token = mint(secret, r#"{"sub":"user-42","exp":9999999999}"#);
        let claims = validate_jwt(&token, secret, 1_000, None, None).unwrap();
        assert_eq!(claims.sub, "user-42");
    }

    #[test]
    fn wrong_secret_fails_signature() {
        let token = mint(b"topsecret", r#"{"sub":"u","exp":9999999999}"#);
        assert_eq!(
            validate_jwt(&token, b"different", 1_000, None, None),
            Err(AuthError::BadSignature)
        );
    }

    #[test]
    fn expired_token_is_rejected() {
        let secret = b"s";
        let token = mint(secret, r#"{"sub":"u","exp":500}"#);
        assert_eq!(validate_jwt(&token, secret, 1_000, None, None), Err(AuthError::Expired));
    }

    #[test]
    fn token_without_exp_is_allowed() {
        let secret = b"s";
        let token = mint(secret, r#"{"sub":"u"}"#);
        assert!(validate_jwt(&token, secret, 1_000, None, None).is_ok());
    }

    #[test]
    fn malformed_token_is_rejected() {
        assert_eq!(validate_jwt("not.a", b"s", 0, None, None), Err(AuthError::Malformed));
        assert_eq!(validate_jwt("a.b.c.d", b"s", 0, None, None), Err(AuthError::Malformed));
    }

    #[test]
    fn tampered_payload_fails_signature() {
        let secret = b"s";
        let token = mint(secret, r#"{"sub":"u","exp":9999999999}"#);
        // Swap the payload segment for a different one; signature no longer matches.
        let mut parts: Vec<&str> = token.split('.').collect();
        let evil = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"sub":"admin","exp":9999999999}"#);
        parts[1] = &evil;
        let tampered = parts.join(".");
        assert_eq!(
            validate_jwt(&tampered, secret, 1_000, None, None),
            Err(AuthError::BadSignature)
        );
    }

    #[test]
    fn extracts_token_from_sec_payload() {
        let payload = r#"{"initConnectionMessage":["initConnection",{}],"authToken":"abc.def.ghi"}"#;
        assert_eq!(token_from_sec_payload(payload), Some("abc.def.ghi".to_string()));
        assert_eq!(token_from_sec_payload(r#"{"authToken":""}"#), None);
        assert_eq!(token_from_sec_payload(r#"{}"#), None);
    }

    #[test]
    fn issuer_and_audience_claims_are_enforced_when_expected() {
        let secret = b"s";
        let token = mint(
            secret,
            r#"{"sub":"u","exp":9999999999,"iss":"https://issuer","aud":["api","web"]}"#,
        );
        // Matching iss + aud (aud is an array containing "api").
        assert!(validate_jwt(&token, secret, 1_000, Some("https://issuer"), Some("api")).is_ok());
        // Wrong issuer.
        assert_eq!(
            validate_jwt(&token, secret, 1_000, Some("https://evil"), None),
            Err(AuthError::BadClaim("iss"))
        );
        // Audience not in the array.
        assert_eq!(
            validate_jwt(&token, secret, 1_000, None, Some("mobile")),
            Err(AuthError::BadClaim("aud"))
        );
        // Not required -> ignored.
        assert!(validate_jwt(&token, secret, 1_000, None, None).is_ok());
    }

    #[test]
    fn connection_gate_enforces_auth_only_when_configured() {
        let secret = b"gate-secret";
        let token = mint(secret, r#"{"sub":"u","exp":9999999999}"#);
        let good_payload = format!(r#"{{"authToken":"{token}"}}"#);
        let bad_payload = r#"{"authToken":"garbage"}"#;

        // No secret configured -> always allowed (even with no payload).
        assert!(authorize_connection(None, None, 1_000, None, None));
        // Secret configured -> valid token allowed, invalid/missing rejected.
        assert!(authorize_connection(Some(&good_payload), Some(secret), 1_000, None, None));
        assert!(!authorize_connection(Some(bad_payload), Some(secret), 1_000, None, None));
        assert!(!authorize_connection(None, Some(secret), 1_000, None, None));
    }
}
