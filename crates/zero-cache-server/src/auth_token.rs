//! JWT validation for `ZERO_AUTH_SECRET` / `ZERO_AUTH_JWK` / `ZERO_AUTH_JWKS_URL`.
//!
//! Zero clients authenticate with a bearer JWT (carried in the connection's
//! `Sec-WebSocket-Protocol` payload or `initConnection`). When the server is
//! configured with an auth key source, every connection's token must be a
//! valid, unexpired JWT verifiable against that source.
//!
//! This mirrors upstream `mono-src/packages/zero-cache/src/auth/jwt.ts`
//! `verifyToken`, which resolves a key from one of three sources in priority
//! order **jwk > secret > jwksUrl** and hands it to `jose.jwtVerify`:
//!
//! - [`TokenVerifier::Secret`] — a symmetric HMAC secret (HS256/384/512).
//! - [`TokenVerifier::StaticJwk`] — a single static JWK (asymmetric
//!   RS/ES/PS/EdDSA, matching the token header `alg`).
//! - [`TokenVerifier::JwksUrl`] — a remote JWKS document, fetched and cached,
//!   with the signing key selected by the token header `kid`
//!   (`jose.createRemoteJWKSet`).
//!
//! Signature verification is delegated to the `jsonwebtoken` crate (which
//! supports the full alg matrix). Claim validation (`exp`/`nbf`/`iss`/`aud`/
//! `sub`) is performed here against an injectable `now` so the semantics match
//! `jose` exactly and stay deterministically testable — `jsonwebtoken`'s own
//! `exp`/`nbf` checks read the real system clock, which is why they are
//! disabled and re-implemented below. The full payload is preserved verbatim
//! (via `bigint_json`) for compiled-permission `authData` binding.

use base64::Engine;
use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use std::sync::RwLock;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum AuthError {
    #[error("malformed token")]
    Malformed,
    #[error("unsupported alg")]
    UnsupportedAlg,
    #[error("bad signature")]
    BadSignature,
    #[error("token expired")]
    Expired,
    #[error("token not yet valid")]
    NotYetValid,
    #[error("missing token")]
    Missing,
    #[error("claim mismatch ({0})")]
    BadClaim(&'static str),
    #[error("key error: {0}")]
    KeyError(String),
    #[error("jwks fetch failed: {0}")]
    JwksFetch(String),
}

/// Validated claims of interest.
#[derive(Debug, Clone, PartialEq)]
pub struct Claims {
    /// `sub` — the authenticated user id (empty if absent).
    pub sub: String,
    /// The verified JWT payload, retained verbatim for compiled permission
    /// rules whose static `authData` references need arbitrary claims (not
    /// merely `sub`).
    pub decoded: zero_cache_shared::bigint_json::JsonValue,
}

fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(s.as_bytes())
        .ok()
}

/// A resolved token-verification key source, matching upstream's
/// `LegacyJWTAuthConfig` resolution (`jwk > secret > jwksUrl`).
pub enum TokenVerifier {
    /// No auth configured — every connection is admitted.
    Disabled,
    /// Symmetric HMAC secret (`ZERO_AUTH_SECRET`).
    Secret(Vec<u8>),
    /// A single static JWK (`ZERO_AUTH_JWK`).
    StaticJwk(Box<Jwk>),
    /// A remote JWKS document (`ZERO_AUTH_JWKS_URL`).
    JwksUrl(JwksSource),
}

/// A remote JWKS endpoint with its fetched-key cache. Mirrors
/// `jose.createRemoteJWKSet`: the set is fetched lazily and cached; an unknown
/// `kid` triggers a refetch.
pub struct JwksSource {
    url: String,
    client: reqwest::Client,
    cache: RwLock<Option<JwkSet>>,
    /// Timestamp of the last successful (re)fetch, used to rate-limit refetches
    /// on unknown `kid` so a flood of bad-`kid` tokens cannot trigger a fetch
    /// storm. Mirrors the cooldown `jose.createRemoteJWKSet` applies.
    last_refetch: RwLock<Option<std::time::Instant>>,
}

/// Cooldown between remote-JWKS refetches triggered by an unknown `kid`.
const JWKS_REFETCH_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(30);

impl TokenVerifier {
    /// Resolves the configured key source. Empty strings are treated as unset.
    ///
    /// Upstream installs legacy-JWT validation only when **exactly one** of
    /// `jwk`/`secret`/`jwksUrl` is configured (`syncer.ts`: `tokenOptions.length
    /// === 1`). When zero or more than one source is configured, validation is
    /// skipped entirely — so with multiple sources we return [`Disabled`] rather
    /// than silently picking a "priority" winner.
    ///
    /// [`Disabled`]: TokenVerifier::Disabled
    pub fn from_config(
        secret: Option<&str>,
        jwk: Option<&str>,
        jwks_url: Option<&str>,
    ) -> Result<Self, AuthError> {
        let jwk = jwk.filter(|s| !s.is_empty());
        let secret = secret.filter(|s| !s.is_empty());
        let jwks_url = jwks_url.filter(|s| !s.is_empty());

        let configured = jwk.is_some() as u8 + secret.is_some() as u8 + jwks_url.is_some() as u8;
        if configured != 1 {
            // Zero sources -> auth disabled; multiple sources -> upstream skips
            // legacy-JWT validation, so the connection is admitted unverified.
            return Ok(TokenVerifier::Disabled);
        }

        if let Some(jwk) = jwk {
            let parsed: Jwk =
                serde_json::from_str(jwk).map_err(|e| AuthError::KeyError(e.to_string()))?;
            return Ok(TokenVerifier::StaticJwk(Box::new(parsed)));
        }
        if let Some(secret) = secret {
            return Ok(TokenVerifier::Secret(secret.as_bytes().to_vec()));
        }
        if let Some(url) = jwks_url {
            return Ok(TokenVerifier::JwksUrl(JwksSource {
                url: url.to_string(),
                client: reqwest::Client::new(),
                cache: RwLock::new(None),
                last_refetch: RwLock::new(None),
            }));
        }
        Ok(TokenVerifier::Disabled)
    }

    /// `true` unless auth is disabled (no key source configured).
    pub fn is_enabled(&self) -> bool {
        !matches!(self, TokenVerifier::Disabled)
    }

    /// Verifies `token`, resolving a remote JWKS over the network if needed.
    /// The future is `Send` (no lock guard is held across `.await`).
    pub async fn verify(
        &self,
        token: &str,
        now_unix: u64,
        expected_iss: Option<&str>,
        expected_aud: Option<&str>,
        expected_sub: Option<&str>,
    ) -> Result<Claims, AuthError> {
        match self {
            TokenVerifier::Disabled => Err(AuthError::Missing),
            TokenVerifier::Secret(secret) => verify_with_secret(
                secret,
                token,
                now_unix,
                expected_iss,
                expected_aud,
                expected_sub,
            ),
            TokenVerifier::StaticJwk(jwk) => verify_with_jwk(
                jwk,
                token,
                now_unix,
                expected_iss,
                expected_aud,
                expected_sub,
            ),
            TokenVerifier::JwksUrl(source) => {
                verify_with_jwks(
                    source,
                    token,
                    now_unix,
                    expected_iss,
                    expected_aud,
                    expected_sub,
                )
                .await
            }
        }
    }

    /// Synchronous verification for the `updateAuth` revalidation path. A
    /// remote JWKS uses only the key set already cached at connect time (no
    /// refetch), keeping this call non-async and `Send`.
    pub fn verify_sync(
        &self,
        token: &str,
        now_unix: u64,
        expected_iss: Option<&str>,
        expected_aud: Option<&str>,
        expected_sub: Option<&str>,
    ) -> Result<Claims, AuthError> {
        match self {
            TokenVerifier::Disabled => Err(AuthError::Missing),
            TokenVerifier::Secret(secret) => verify_with_secret(
                secret,
                token,
                now_unix,
                expected_iss,
                expected_aud,
                expected_sub,
            ),
            TokenVerifier::StaticJwk(jwk) => verify_with_jwk(
                jwk,
                token,
                now_unix,
                expected_iss,
                expected_aud,
                expected_sub,
            ),
            TokenVerifier::JwksUrl(source) => {
                let header = decode_header(token).map_err(|_| AuthError::Malformed)?;
                let kid = header.kid.as_deref();

                // Prefer the already-cached key set.
                if let Some(set) = source.cache.read().unwrap().clone() {
                    if let Some(jwk) = select_jwk(&set, kid) {
                        return verify_with_jwk(
                            jwk,
                            token,
                            now_unix,
                            expected_iss,
                            expected_aud,
                            expected_sub,
                        );
                    }
                }

                // Unknown `kid` (or empty cache): refetch the JWKS, matching
                // jose's `createRemoteJWKSet`, which auto-fetches on an unknown
                // key id. `refetch_sync` rate-limits so a flood of bad-`kid`
                // tokens cannot storm the endpoint.
                let set = source.refetch_sync()?;
                let jwk = select_jwk(&set, kid)
                    .ok_or_else(|| AuthError::KeyError("no matching jwk for kid".into()))?;
                verify_with_jwk(
                    jwk,
                    token,
                    now_unix,
                    expected_iss,
                    expected_aud,
                    expected_sub,
                )
            }
        }
    }

    /// The connection-admission auth gate: `true` if the connection may
    /// proceed. When auth is disabled every connection is allowed; otherwise
    /// the connection's `Sec-WebSocket-Protocol` payload must carry a token
    /// that verifies against the configured key source.
    pub async fn authorize(
        &self,
        sec_payload: Option<&str>,
        now_unix: u64,
        issuer: Option<&str>,
        audience: Option<&str>,
        subject: Option<&str>,
    ) -> bool {
        if !self.is_enabled() {
            return true;
        }
        match sec_payload.and_then(token_from_sec_payload) {
            Some(token) => self
                .verify(&token, now_unix, issuer, audience, subject)
                .await
                .is_ok(),
            None => false,
        }
    }
}

/// Splits a compact JWT into its three segments, rejecting any other shape.
fn split_segments(token: &str) -> Result<(&str, &str, &str), AuthError> {
    let mut parts = token.split('.');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(h), Some(p), Some(s), None) => Ok((h, p, s)),
        _ => Err(AuthError::Malformed),
    }
}

/// Maps a `jsonwebtoken` verification failure onto our error taxonomy.
fn map_jwt_error(err: &jsonwebtoken::errors::Error) -> AuthError {
    use jsonwebtoken::errors::ErrorKind;
    match err.kind() {
        ErrorKind::InvalidSignature => AuthError::BadSignature,
        ErrorKind::InvalidAlgorithm | ErrorKind::InvalidAlgorithmName => AuthError::UnsupportedAlg,
        ErrorKind::InvalidRsaKey(_) | ErrorKind::InvalidEcdsaKey | ErrorKind::InvalidKeyFormat => {
            AuthError::KeyError(err.to_string())
        }
        _ => AuthError::Malformed,
    }
}

/// Verifies the token's signature with `key`/`alg` (delegated to
/// `jsonwebtoken`, with its clock-dependent claim checks disabled), then runs
/// the injectable-`now` claim validation and returns the preserved payload.
fn verify_signature_and_claims(
    token: &str,
    key: &DecodingKey,
    alg: Algorithm,
    now_unix: u64,
    expected_iss: Option<&str>,
    expected_aud: Option<&str>,
    expected_sub: Option<&str>,
) -> Result<Claims, AuthError> {
    let (_, payload_b64, _) = split_segments(token)?;

    // Signature only: `exp`/`nbf`/`aud`/`iss` are re-checked below against the
    // injected `now` so semantics match jose and stay deterministic.
    let mut validation = Validation::new(alg);
    validation.required_spec_claims.clear();
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.validate_aud = false;
    decode::<serde_json::Value>(token, key, &validation).map_err(|e| map_jwt_error(&e))?;

    let payload = b64url_decode(payload_b64).ok_or(AuthError::Malformed)?;
    let payload = zero_cache_shared::bigint_json::parse(
        std::str::from_utf8(&payload).map_err(|_| AuthError::Malformed)?,
    )
    .map_err(|_| AuthError::Malformed)?;
    check_claims(payload, now_unix, expected_iss, expected_aud, expected_sub)
}

/// Validates the standard claims against an injected `now` and returns the
/// [`Claims`]. Semantics match jose's `jwtVerify`:
/// `exp` inclusive (`now >= exp` is expired), `nbf` (`nbf > now` is not-yet-
/// valid), and required `iss`/`aud`/`sub`.
fn check_claims(
    payload: zero_cache_shared::bigint_json::JsonValue,
    now_unix: u64,
    expected_iss: Option<&str>,
    expected_aud: Option<&str>,
    expected_sub: Option<&str>,
) -> Result<Claims, AuthError> {
    use zero_cache_shared::bigint_json::JsonValue;
    if !matches!(&payload, JsonValue::Object(_)) {
        return Err(AuthError::Malformed);
    }
    // `exp`: reject once the current time has reached it (jose uses `>=`).
    if let Some(JsonValue::Number(exp)) = object_get(&payload, "exp") {
        if !exp.is_finite() || *exp < 0.0 {
            return Err(AuthError::BadClaim("exp"));
        }
        if now_unix >= *exp as u64 {
            return Err(AuthError::Expired);
        }
    }
    // `nbf`: reject a token whose validity has not begun.
    if let Some(JsonValue::Number(nbf)) = object_get(&payload, "nbf") {
        if !nbf.is_finite() || *nbf < 0.0 {
            return Err(AuthError::BadClaim("nbf"));
        }
        if (*nbf as u64) > now_unix {
            return Err(AuthError::NotYetValid);
        }
    }
    if let Some(want) = expected_iss {
        let ok = matches!(
            object_get(&payload, "iss"),
            Some(JsonValue::String(iss)) if iss == want
        );
        if !ok {
            return Err(AuthError::BadClaim("iss"));
        }
    }
    if let Some(want) = expected_aud {
        // `aud` may be a string or an array of strings.
        let ok = match object_get(&payload, "aud") {
            Some(JsonValue::String(a)) => a == want,
            Some(JsonValue::Array(items)) => items
                .iter()
                .any(|v| matches!(v, JsonValue::String(a) if a == want)),
            _ => false,
        };
        if !ok {
            return Err(AuthError::BadClaim("aud"));
        }
    }

    let sub = match object_get(&payload, "sub") {
        Some(JsonValue::String(s)) => s.clone(),
        _ => String::new(),
    };
    // Subject: upstream passes the connecting `userID` as jose's `subject`, so a
    // token whose `sub` does not match the user presenting it is rejected. An
    // empty expected subject (anonymous connect) imposes no constraint.
    if let Some(want) = expected_sub {
        if !want.is_empty() && sub != want {
            return Err(AuthError::BadClaim("sub"));
        }
    }
    Ok(Claims {
        sub,
        decoded: payload,
    })
}

fn verify_with_secret(
    secret: &[u8],
    token: &str,
    now_unix: u64,
    expected_iss: Option<&str>,
    expected_aud: Option<&str>,
    expected_sub: Option<&str>,
) -> Result<Claims, AuthError> {
    let header = decode_header(token).map_err(|_| AuthError::Malformed)?;
    // A symmetric secret only signs HMAC algorithms (jose rejects otherwise).
    if !matches!(
        header.alg,
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
    ) {
        return Err(AuthError::UnsupportedAlg);
    }
    verify_signature_and_claims(
        token,
        &DecodingKey::from_secret(secret),
        header.alg,
        now_unix,
        expected_iss,
        expected_aud,
        expected_sub,
    )
}

fn verify_with_jwk(
    jwk: &Jwk,
    token: &str,
    now_unix: u64,
    expected_iss: Option<&str>,
    expected_aud: Option<&str>,
    expected_sub: Option<&str>,
) -> Result<Claims, AuthError> {
    let header = decode_header(token).map_err(|_| AuthError::Malformed)?;
    let key = DecodingKey::from_jwk(jwk).map_err(|e| AuthError::KeyError(e.to_string()))?;
    // The token header declares the alg; jose verifies the signature against
    // the key material regardless. A mismatched alg fails signature check.
    verify_signature_and_claims(
        token,
        &key,
        header.alg,
        now_unix,
        expected_iss,
        expected_aud,
        expected_sub,
    )
}

/// Selects the JWK whose `kid` matches the token header.
///
/// When the token carries a `kid`, only a key with that exact id is eligible —
/// jose throws "no applicable key" rather than falling back to the sole key, so
/// a `kid` that matches nothing yields `None` even for a single-key set. When
/// the token has no `kid`, the sole key of a single-key set is used (jose's
/// behavior); an ambiguous multi-key set yields `None`.
fn select_jwk<'a>(set: &'a JwkSet, kid: Option<&str>) -> Option<&'a Jwk> {
    match kid {
        Some(kid) => set
            .keys
            .iter()
            .find(|k| k.common.key_id.as_deref() == Some(kid)),
        None => {
            if set.keys.len() == 1 {
                set.keys.first()
            } else {
                None
            }
        }
    }
}

impl JwksSource {
    /// Synchronously refetches the remote JWKS, rate-limited by
    /// [`JWKS_REFETCH_COOLDOWN`]. Within the cooldown the cached set is reused
    /// (so an unknown `kid` fails with "no matching key" instead of hammering
    /// the endpoint).
    ///
    /// The fetch runs on a dedicated OS thread with its own single-threaded
    /// runtime, so this stays a synchronous, `Send` call usable from the
    /// `updateAuth` revalidation path and safe to invoke from within an async
    /// runtime.
    fn refetch_sync(&self) -> Result<JwkSet, AuthError> {
        if let Some(last) = *self.last_refetch.read().unwrap() {
            if last.elapsed() < JWKS_REFETCH_COOLDOWN {
                return self
                    .cache
                    .read()
                    .unwrap()
                    .clone()
                    .ok_or_else(|| AuthError::KeyError("no matching jwk for kid".into()));
            }
        }

        let url = self.url.clone();
        let fetched = std::thread::spawn(move || -> Result<JwkSet, String> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            rt.block_on(async {
                reqwest::Client::new()
                    .get(&url)
                    .send()
                    .await
                    .map_err(|e| e.to_string())?
                    .json::<JwkSet>()
                    .await
                    .map_err(|e| e.to_string())
            })
        })
        .join()
        .map_err(|_| AuthError::JwksFetch("jwks refetch thread panicked".into()))?
        .map_err(AuthError::JwksFetch)?;

        *self.cache.write().unwrap() = Some(fetched.clone());
        *self.last_refetch.write().unwrap() = Some(std::time::Instant::now());
        Ok(fetched)
    }
}

async fn verify_with_jwks(
    source: &JwksSource,
    token: &str,
    now_unix: u64,
    expected_iss: Option<&str>,
    expected_aud: Option<&str>,
    expected_sub: Option<&str>,
) -> Result<Claims, AuthError> {
    let header = decode_header(token).map_err(|_| AuthError::Malformed)?;
    let kid = header.kid.as_deref();

    // Try the cached set first (clone out so no guard is held across the fetch).
    let cached = source.cache.read().unwrap().clone();
    if let Some(set) = &cached {
        if let Some(jwk) = select_jwk(set, kid) {
            return verify_with_jwk(
                jwk,
                token,
                now_unix,
                expected_iss,
                expected_aud,
                expected_sub,
            );
        }
    }

    // Unknown kid (or empty cache): refetch, matching jose's
    // `createRemoteJWKSet` behavior.
    let fetched = source
        .client
        .get(&source.url)
        .send()
        .await
        .map_err(|e| AuthError::JwksFetch(e.to_string()))?
        .json::<JwkSet>()
        .await
        .map_err(|e| AuthError::JwksFetch(e.to_string()))?;

    let result = match select_jwk(&fetched, kid) {
        Some(jwk) => verify_with_jwk(
            jwk,
            token,
            now_unix,
            expected_iss,
            expected_aud,
            expected_sub,
        ),
        None => Err(AuthError::KeyError("no matching jwk for kid".into())),
    };
    *source.cache.write().unwrap() = Some(fetched);
    *source.last_refetch.write().unwrap() = Some(std::time::Instant::now());
    result
}

/// Validates an HS256/384/512 JWT against `secret` at time `now_unix`.
///
/// Retained for the symmetric-secret path (`ZERO_AUTH_SECRET`) and its callers;
/// equivalent to `TokenVerifier::Secret(secret).verify_sync(...)`.
pub fn validate_jwt(
    token: &str,
    secret: &[u8],
    now_unix: u64,
    expected_iss: Option<&str>,
    expected_aud: Option<&str>,
    expected_sub: Option<&str>,
) -> Result<Claims, AuthError> {
    verify_with_secret(
        secret,
        token,
        now_unix,
        expected_iss,
        expected_aud,
        expected_sub,
    )
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

/// The connection-admission auth gate (symmetric-secret form): `true` if the
/// connection may proceed. When `secret` is `None` auth is disabled (always
/// allowed); otherwise the connection's `Sec-WebSocket-Protocol` payload must
/// carry a valid, unexpired HS token signed with `secret`.
#[allow(clippy::too_many_arguments)]
pub fn authorize_connection(
    sec_payload: Option<&str>,
    secret: Option<&[u8]>,
    now_unix: u64,
    issuer: Option<&str>,
    audience: Option<&str>,
    subject: Option<&str>,
) -> bool {
    match secret {
        None => true,
        Some(secret) => sec_payload
            .and_then(token_from_sec_payload)
            .and_then(|t| validate_jwt(&t, secret, now_unix, issuer, audience, subject).ok())
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
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

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
        let claims = validate_jwt(&token, secret, 1_000, None, None, None).unwrap();
        assert_eq!(claims.sub, "user-42");
        assert!(matches!(
            object_get(&claims.decoded, "sub"),
            Some(zero_cache_shared::bigint_json::JsonValue::String(sub)) if sub == "user-42"
        ));
    }

    #[test]
    fn wrong_secret_fails_signature() {
        let token = mint(b"topsecret", r#"{"sub":"u","exp":9999999999}"#);
        assert_eq!(
            validate_jwt(&token, b"different", 1_000, None, None, None),
            Err(AuthError::BadSignature)
        );
    }

    #[test]
    fn expired_token_is_rejected() {
        let secret = b"s";
        let token = mint(secret, r#"{"sub":"u","exp":500}"#);
        assert_eq!(
            validate_jwt(&token, secret, 1_000, None, None, None),
            Err(AuthError::Expired)
        );
    }

    #[test]
    fn token_without_exp_is_allowed() {
        let secret = b"s";
        let token = mint(secret, r#"{"sub":"u"}"#);
        assert!(validate_jwt(&token, secret, 1_000, None, None, None).is_ok());
    }

    #[test]
    fn malformed_token_is_rejected() {
        assert_eq!(
            validate_jwt("not.a", b"s", 0, None, None, None),
            Err(AuthError::Malformed)
        );
        assert_eq!(
            validate_jwt("a.b.c.d", b"s", 0, None, None, None),
            Err(AuthError::Malformed)
        );
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
            validate_jwt(&tampered, secret, 1_000, None, None, None),
            Err(AuthError::BadSignature)
        );
    }

    #[test]
    fn extracts_token_from_sec_payload() {
        let payload =
            r#"{"initConnectionMessage":["initConnection",{}],"authToken":"abc.def.ghi"}"#;
        assert_eq!(
            token_from_sec_payload(payload),
            Some("abc.def.ghi".to_string())
        );
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
        assert!(validate_jwt(
            &token,
            secret,
            1_000,
            Some("https://issuer"),
            Some("api"),
            None
        )
        .is_ok());
        // Wrong issuer.
        assert_eq!(
            validate_jwt(&token, secret, 1_000, Some("https://evil"), None, None),
            Err(AuthError::BadClaim("iss"))
        );
        // Audience not in the array.
        assert_eq!(
            validate_jwt(&token, secret, 1_000, None, Some("mobile"), None),
            Err(AuthError::BadClaim("aud"))
        );
        // Not required -> ignored.
        assert!(validate_jwt(&token, secret, 1_000, None, None, None).is_ok());
    }

    #[test]
    fn connection_gate_enforces_auth_only_when_configured() {
        let secret = b"gate-secret";
        let token = mint(secret, r#"{"sub":"u","exp":9999999999}"#);
        let good_payload = format!(r#"{{"authToken":"{token}"}}"#);
        let bad_payload = r#"{"authToken":"garbage"}"#;

        // No secret configured -> always allowed (even with no payload).
        assert!(authorize_connection(None, None, 1_000, None, None, None));
        // Secret configured -> valid token allowed, invalid/missing rejected.
        assert!(authorize_connection(
            Some(&good_payload),
            Some(secret),
            1_000,
            None,
            None,
            None
        ));
        assert!(!authorize_connection(
            Some(bad_payload),
            Some(secret),
            1_000,
            None,
            None,
            None
        ));
        assert!(!authorize_connection(
            None,
            Some(secret),
            1_000,
            None,
            None,
            None
        ));
    }

    #[test]
    fn not_yet_valid_token_is_rejected() {
        let secret = b"s";
        let token = mint(secret, r#"{"sub":"u","nbf":2000,"exp":9999999999}"#);
        assert_eq!(
            validate_jwt(&token, secret, 1_000, None, None, None),
            Err(AuthError::NotYetValid)
        );
        // Once `now` reaches `nbf`, the token is valid.
        assert!(validate_jwt(&token, secret, 2_000, None, None, None).is_ok());
    }

    #[test]
    fn exp_boundary_is_inclusive() {
        let secret = b"s";
        let token = mint(secret, r#"{"sub":"u","exp":1000}"#);
        // Invalid *at* exp (jose semantics), and after.
        assert_eq!(
            validate_jwt(&token, secret, 1_000, None, None, None),
            Err(AuthError::Expired)
        );
        // Still valid just before exp.
        assert!(validate_jwt(&token, secret, 999, None, None, None).is_ok());
    }

    #[test]
    fn subject_must_match_connecting_user() {
        let secret = b"s";
        let token = mint(secret, r#"{"sub":"user-42","exp":9999999999}"#);
        // Matching subject.
        assert!(validate_jwt(&token, secret, 1_000, None, None, Some("user-42")).is_ok());
        // Mismatched subject is rejected even though the signature is valid.
        assert_eq!(
            validate_jwt(&token, secret, 1_000, None, None, Some("someone-else")),
            Err(AuthError::BadClaim("sub"))
        );
        // Empty/absent expected subject imposes no constraint.
        assert!(validate_jwt(&token, secret, 1_000, None, None, Some("")).is_ok());
        assert!(validate_jwt(&token, secret, 1_000, None, None, None).is_ok());
    }

    // ---- Asymmetric (JWK / JWKS) verification ----------------------------

    // Fixed test RSA-2048 keypair (private PEM signs; public JWK verifies).
    const RSA_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDIY7FO8YWMYc0J\n\
pN8ezPgWiR4MVYK1QMagHzm6eNJHXEcsgUKvqA3xPiu9QaPnWavjXEbtQbjRAqGO\n\
mUvTK/ggotncbXstgU8YYuwFv35Wd20j3XLG8g9gv+veQf0zN2MEVtRctbd7ayRU\n\
W/91TjUx2UMebX6flleGDPOovaBR+Wf004Hhq5Bz9+D5XaKhbacCcSjSopC5UbTC\n\
zqq0uYixuJkXlgZIeBImPnqG7TYsAs7Fq/WuKY9HahKYhgq7/yRgQraRp3PlJ5Oh\n\
d/3PXqVkEYqRKa8zuaVjMr13alCCuA8eiquHCP2QBO92HTBDDcmVDo4Qly3XCU0/\n\
upQ0bKmVAgMBAAECggEBAJesxbcwHlp8aFqlXXCNyjYcgQ7q5m1U40Ktf9+Btf/n\n\
e8PW7ufP9wWjfi3Y8juZZv5HiTPp6/7f0/MAWyEyhbQGL4qln3d5Cao2rdlH8VN3\n\
P7fG1Sp6a2NawShQoFrU0HCnXEP0EzYqiawEml5q3N6nSSNN02rtu7+uK/uL1D5W\n\
hPPA2HWi/x1WVR8JyAD3aS1I14vcCGQbZOc6230vD4cgW4x1u8P7MTbPmbGnHWPH\n\
9V3gqLHsMp/wVsB2TGBfkWRs0GcqJcAwqOtx5x+VO2zrB8Di7YzATN/T8Qjnz/PU\n\
HGnPvzxmNoL0orFdswa1qmvdAUlyitfCyBAfNYqJwz0CgYEA/Tp1Pp18/OC8FJYe\n\
xG2VabXxQvtI0vLKCwFSOrE7oMtHHzaa5ayro0F3w36n5AWC+gvDV64OtSczYpBW\n\
qZsGZo/aguGAQvJSLo78/K9tnjoOFJ/z6ODfWm0w17joKKb8A8OQUed4DwcI4B3n\n\
1DHjpLdeGNIAZ8AVaSlauAGsx8MCgYEAypUuRYt/C2LG3QDhvd3OrlOQiXmspye2\n\
qDL+SLLcvd1fCH4UGWPgbh6ZNGbOh7L2oD9HP01ske0Ldm2xJyjY5pWOxj5m1uQy\n\
DPuCxLcnyKFanhP1i/JY2jE7vXRvygw26SUhL0Si3v7B/iYcXoGIGwBCU416NRn2\n\
nzZWv3rYC8cCgYEAkXfTgnTWKC6x3OGgKxcIjgGG5wOTghsXFdtccXr+1g/we23S\n\
7b2Tm+Uv9436xHKmGx5GyUekC0zJqAViw2va8XASBr2kANFThIt/qWjdf9e53v9E\n\
DrOfm0K+nC4Mr829WCwv690ciwVvg8+qLau7KhRsabW5peAibJblFm9f4iECgYEA\n\
vyelUdofNw8ttrxuRkpWDAiuCgrV76R5ppz3dIHR6RZJ5imRraOg0kftKJUZrNIi\n\
BXOwNvtHxyp19nnq/5h7kpjs8ANR5tPMppNtAVISKC6Y4zDSMgur67cpN8v28CA2\n\
cCio94E8bk7Vnos3mbWASHomG9ETz6eAHxuXH3c7BWECgYAD4PLxqbO0SHGbcvEY\n\
m1ZF4UkofDWqOzR4Vichw5t1sBENWs0v1gdXvwu+uqek5xFaXr9oK4z2QbMTVork\n\
X39QTozw/ytmBpSpeTEDyvnLvobOCn5ygUnp81VB1xX4CxWonKEsQN2e8vaL5ZUX\n\
KDcr9uf+cYknm9fVEsfIYYFjHg==\n\
-----END PRIVATE KEY-----\n";

    const RSA_PUB_JWK: &str = r#"{"kty":"RSA","n":"yGOxTvGFjGHNCaTfHsz4FokeDFWCtUDGoB85unjSR1xHLIFCr6gN8T4rvUGj51mr41xG7UG40QKhjplL0yv4IKLZ3G17LYFPGGLsBb9-VndtI91yxvIPYL_r3kH9MzdjBFbUXLW3e2skVFv_dU41MdlDHm1-n5ZXhgzzqL2gUfln9NOB4auQc_fg-V2ioW2nAnEo0qKQuVG0ws6qtLmIsbiZF5YGSHgSJj56hu02LALOxav1rimPR2oSmIYKu_8kYEK2kadz5SeToXf9z16lZBGKkSmvM7mlYzK9d2pQgrgPHoqrhwj9kATvdh0wQw3JlQ6OEJct1wlNP7qUNGyplQ","e":"AQAB","alg":"RS256","use":"sig","kid":"rsa-test-1"}"#;

    // Fixed test EC P-256 keypair (PKCS#8 private PEM signs; public JWK verifies).
    const EC_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgapZwj3mKq+vNgrTR\n\
lS5cSl9GFfb2yezehNNBELKfLDihRANCAAQp6cWe4ykqUhDwufD3q/8CVjudh4Z0\n\
h8Mq9VYeANCVJqrx3m6CiOHziQvCk/XJrrXehvikhtmEpL6HtMVbfmbv\n\
-----END PRIVATE KEY-----\n";

    const EC_PUB_JWK: &str = r#"{"kty":"EC","crv":"P-256","x":"KenFnuMpKlIQ8Lnw96v_AlY7nYeGdIfDKvVWHgDQlSY","y":"qvHeboKI4fOJC8KT9cmutd6G-KSG2YSkvoe0xVt-Zu8","alg":"ES256","use":"sig","kid":"ec-test-1"}"#;

    /// Mints an asymmetric JWT with `jsonwebtoken` (private key), embedding an
    /// optional `kid` in the header.
    fn mint_asymmetric(
        alg: Algorithm,
        key: &jsonwebtoken::EncodingKey,
        kid: Option<&str>,
        payload_json: &str,
    ) -> String {
        let mut header = jsonwebtoken::Header::new(alg);
        header.kid = kid.map(|k| k.to_string());
        let claims: serde_json::Value = serde_json::from_str(payload_json).unwrap();
        jsonwebtoken::encode(&header, &claims, key).unwrap()
    }

    fn rsa_encoding_key() -> jsonwebtoken::EncodingKey {
        jsonwebtoken::EncodingKey::from_rsa_pem(RSA_PRIV_PEM.as_bytes()).unwrap()
    }

    fn ec_encoding_key() -> jsonwebtoken::EncodingKey {
        jsonwebtoken::EncodingKey::from_ec_pem(EC_PRIV_PEM.as_bytes()).unwrap()
    }

    #[test]
    fn rs256_verifies_with_static_jwk() {
        let verifier =
            TokenVerifier::from_config(None, Some(RSA_PUB_JWK), None).expect("valid jwk config");
        assert!(verifier.is_enabled());
        let token = mint_asymmetric(
            Algorithm::RS256,
            &rsa_encoding_key(),
            Some("rsa-test-1"),
            r#"{"sub":"rsa-user","exp":9999999999}"#,
        );
        let claims = verifier
            .verify_sync(&token, 1_000, None, None, None)
            .expect("rs256 token verifies");
        assert_eq!(claims.sub, "rsa-user");
    }

    #[test]
    fn es256_verifies_with_static_jwk() {
        let verifier =
            TokenVerifier::from_config(None, Some(EC_PUB_JWK), None).expect("valid jwk config");
        let token = mint_asymmetric(
            Algorithm::ES256,
            &ec_encoding_key(),
            Some("ec-test-1"),
            r#"{"sub":"ec-user","exp":9999999999}"#,
        );
        let claims = verifier
            .verify_sync(&token, 1_000, None, None, None)
            .expect("es256 token verifies");
        assert_eq!(claims.sub, "ec-user");
    }

    #[test]
    fn wrong_key_is_rejected_through_jwk_path() {
        // Token signed with the EC key but verified against the RSA JWK.
        let verifier = TokenVerifier::from_config(None, Some(RSA_PUB_JWK), None).unwrap();
        let token = mint_asymmetric(
            Algorithm::ES256,
            &ec_encoding_key(),
            Some("ec-test-1"),
            r#"{"sub":"u","exp":9999999999}"#,
        );
        // Alg mismatch / bad signature — either way, rejected (not Ok).
        assert!(verifier
            .verify_sync(&token, 1_000, None, None, None)
            .is_err());
    }

    #[test]
    fn claim_checks_apply_on_the_jwk_path() {
        let verifier = TokenVerifier::from_config(None, Some(RSA_PUB_JWK), None).unwrap();
        let key = rsa_encoding_key();
        let mint =
            |payload: &str| mint_asymmetric(Algorithm::RS256, &key, Some("rsa-test-1"), payload);

        // Expired.
        assert_eq!(
            verifier.verify_sync(&mint(r#"{"sub":"u","exp":500}"#), 1_000, None, None, None),
            Err(AuthError::Expired)
        );
        // Not yet valid.
        assert_eq!(
            verifier.verify_sync(
                &mint(r#"{"sub":"u","nbf":2000,"exp":9999999999}"#),
                1_000,
                None,
                None,
                None
            ),
            Err(AuthError::NotYetValid)
        );
        // Issuer mismatch.
        assert_eq!(
            verifier.verify_sync(
                &mint(r#"{"sub":"u","exp":9999999999,"iss":"a"}"#),
                1_000,
                Some("b"),
                None,
                None
            ),
            Err(AuthError::BadClaim("iss"))
        );
        // Audience mismatch.
        assert_eq!(
            verifier.verify_sync(
                &mint(r#"{"sub":"u","exp":9999999999,"aud":"api"}"#),
                1_000,
                None,
                Some("web"),
                None
            ),
            Err(AuthError::BadClaim("aud"))
        );
        // Subject mismatch.
        assert_eq!(
            verifier.verify_sync(
                &mint(r#"{"sub":"u","exp":9999999999}"#),
                1_000,
                None,
                None,
                Some("other")
            ),
            Err(AuthError::BadClaim("sub"))
        );
    }

    #[test]
    fn jwks_selects_key_by_kid() {
        // A JWKS with two keys; the token's `kid` picks the right one.
        let jwks_json = format!(r#"{{"keys":[{RSA_PUB_JWK},{EC_PUB_JWK}]}}"#);
        let set: JwkSet = serde_json::from_str(&jwks_json).unwrap();

        // Populate a JwksUrl verifier's cache directly (no network).
        // `last_refetch` set to "just now" keeps refetch_sync inside its
        // cooldown, so the unknown-kid case below reuses the cached set (no
        // network) and fails with a no-matching-key error.
        let source = JwksSource {
            url: "http://example.invalid/jwks".into(),
            client: reqwest::Client::new(),
            cache: RwLock::new(Some(set)),
            last_refetch: RwLock::new(Some(std::time::Instant::now())),
        };
        let verifier = TokenVerifier::JwksUrl(source);

        // ES256 token with kid "ec-test-1" -> must select the EC key.
        let ec_token = mint_asymmetric(
            Algorithm::ES256,
            &ec_encoding_key(),
            Some("ec-test-1"),
            r#"{"sub":"ec-user","exp":9999999999}"#,
        );
        let claims = verifier
            .verify_sync(&ec_token, 1_000, None, None, None)
            .expect("kid selects EC key");
        assert_eq!(claims.sub, "ec-user");

        // RS256 token with kid "rsa-test-1" -> must select the RSA key.
        let rsa_token = mint_asymmetric(
            Algorithm::RS256,
            &rsa_encoding_key(),
            Some("rsa-test-1"),
            r#"{"sub":"rsa-user","exp":9999999999}"#,
        );
        let claims = verifier
            .verify_sync(&rsa_token, 1_000, None, None, None)
            .expect("kid selects RSA key");
        assert_eq!(claims.sub, "rsa-user");

        // Unknown kid with a multi-key set and no refetch -> no matching key.
        let unknown = mint_asymmetric(
            Algorithm::RS256,
            &rsa_encoding_key(),
            Some("does-not-exist"),
            r#"{"sub":"u","exp":9999999999}"#,
        );
        assert!(matches!(
            verifier.verify_sync(&unknown, 1_000, None, None, None),
            Err(AuthError::KeyError(_))
        ));
    }

    #[test]
    fn config_validates_only_with_exactly_one_source() {
        // M3: upstream installs legacy-JWT validation only when EXACTLY ONE of
        // jwk/secret/jwksUrl is configured. Multiple sources -> validation is
        // skipped entirely (Disabled), not silently resolved by "priority".
        assert!(matches!(
            TokenVerifier::from_config(Some("s"), Some(RSA_PUB_JWK), Some("http://x")).unwrap(),
            TokenVerifier::Disabled
        ));
        assert!(matches!(
            TokenVerifier::from_config(Some("s"), None, Some("http://x")).unwrap(),
            TokenVerifier::Disabled
        ));
        assert!(matches!(
            TokenVerifier::from_config(Some("s"), Some(RSA_PUB_JWK), None).unwrap(),
            TokenVerifier::Disabled
        ));

        // Exactly one source -> that verifier is used.
        assert!(matches!(
            TokenVerifier::from_config(None, Some(RSA_PUB_JWK), None).unwrap(),
            TokenVerifier::StaticJwk(_)
        ));
        assert!(matches!(
            TokenVerifier::from_config(Some("s"), None, None).unwrap(),
            TokenVerifier::Secret(_)
        ));
        assert!(matches!(
            TokenVerifier::from_config(None, None, Some("http://x")).unwrap(),
            TokenVerifier::JwksUrl(_)
        ));

        // Nothing configured -> disabled.
        assert!(matches!(
            TokenVerifier::from_config(None, None, None).unwrap(),
            TokenVerifier::Disabled
        ));
        // Empty strings are treated as unset (so all-empty -> disabled, and one
        // real source alongside empty strings still counts as exactly one).
        assert!(matches!(
            TokenVerifier::from_config(Some(""), Some(""), Some("")).unwrap(),
            TokenVerifier::Disabled
        ));
        assert!(matches!(
            TokenVerifier::from_config(Some("s"), Some(""), Some("")).unwrap(),
            TokenVerifier::Secret(_)
        ));
    }

    #[test]
    fn kid_mismatch_does_not_fall_back_to_sole_key() {
        // L3: a token carrying a `kid` that matches no key is rejected even for
        // a single-key set — jose throws "no applicable key" rather than using
        // the only key on hand.
        let set: JwkSet = serde_json::from_str(&format!(r#"{{"keys":[{RSA_PUB_JWK}]}}"#)).unwrap();
        let source = JwksSource {
            url: "http://example.invalid/jwks".into(),
            client: reqwest::Client::new(),
            cache: RwLock::new(Some(set)),
            // In-cooldown: no network refetch on the unknown-kid miss.
            last_refetch: RwLock::new(Some(std::time::Instant::now())),
        };
        let verifier = TokenVerifier::JwksUrl(source);

        // Token signed by the sole (RSA) key but declaring a non-matching kid.
        let token = mint_asymmetric(
            Algorithm::RS256,
            &rsa_encoding_key(),
            Some("some-other-kid"),
            r#"{"sub":"u","exp":9999999999}"#,
        );
        assert!(matches!(
            verifier.verify_sync(&token, 1_000, None, None, None),
            Err(AuthError::KeyError(_))
        ));

        // Sanity: with the matching kid, the same single-key set verifies.
        let ok_token = mint_asymmetric(
            Algorithm::RS256,
            &rsa_encoding_key(),
            Some("rsa-test-1"),
            r#"{"sub":"u","exp":9999999999}"#,
        );
        assert!(verifier
            .verify_sync(&ok_token, 1_000, None, None, None)
            .is_ok());
    }

    #[test]
    fn no_kid_still_uses_the_sole_key() {
        // L3 boundary: when the token has no `kid`, jose still uses the sole key
        // of a single-key set. Only a present-but-unmatched kid is rejected.
        let jwk: Jwk = serde_json::from_str(RSA_PUB_JWK).unwrap();
        let set = JwkSet { keys: vec![jwk] };
        assert!(select_jwk(&set, None).is_some());
        assert!(select_jwk(&set, Some("rsa-test-1")).is_some());
        assert!(select_jwk(&set, Some("nope")).is_none());
    }
}
