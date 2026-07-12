//! TLS support for upstream Postgres connections.
//!
//! Upstream zero-cache hands its connection strings to `postgres.js`, which
//! implements libpq's `sslmode` semantics: `require` means "encrypt the
//! connection" but does NOT verify the server certificate — verification is
//! opt-in via `verify-ca`/`verify-full`. This port matches that: managed
//! Postgres (e.g. AWS RDS with `rds.force_ssl=1`) hands out certificates
//! signed by a private provider CA that a stock root store cannot verify, so
//! `sslmode=require` with certificate verification enabled would refuse every
//! RDS connection unless the provider CA bundle were separately provisioned.
//!
//! Supported modes are the ones `tokio-postgres` itself parses: `disable`,
//! `prefer` (its default), and `require`. `verify-ca`/`verify-full` are
//! rejected at config-parse time by `tokio-postgres` with an "invalid
//! sslmode" error rather than silently downgraded to unverified TLS.

use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};

/// A certificate verifier that accepts any server certificate — libpq
/// `sslmode=require` semantics (encryption without authentication of the
/// peer). Signature checks still run against the negotiated scheme list so
/// the handshake itself is well-formed TLS.
#[derive(Debug)]
struct AcceptAnyServerCert(Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// A rustls client config with libpq-`require` trust semantics (see module
/// docs). One shared config is enough — it is connection-independent.
pub fn client_config() -> ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("ring provider supports the default TLS protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert(provider)))
        .with_no_client_auth()
}

/// The TLS connector for ordinary `tokio-postgres` connections.
/// `tokio-postgres` consults the conn string's `sslmode` itself: with
/// `disable` this connector is never invoked, with `prefer`/`require` it is
/// used when the server agrees to TLS.
pub fn make_tls_connect() -> tokio_postgres_rustls::MakeRustlsConnect {
    tokio_postgres_rustls::MakeRustlsConnect::new(client_config())
}

/// `sslmode` for the raw replication-protocol connection, which bypasses
/// `tokio-postgres` and negotiates TLS itself ([`crate::replication_conn`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgSslMode {
    /// Never negotiate TLS.
    Disable,
    /// Ask for TLS; fall back to plaintext if the server declines (libpq's
    /// default).
    Prefer,
    /// Require TLS; fail if the server declines.
    Require,
}

impl PgSslMode {
    /// Extracts the `sslmode` from a libpq keyword or URL conn string via
    /// `tokio-postgres`'s own parser, so the raw replication connection always
    /// agrees with the ordinary connections opened from the same string.
    /// Unparseable strings and unknown future modes fall back to the *more*
    /// encrypted interpretation rather than silently downgrading.
    pub fn from_conn_str(conn_str: &str) -> PgSslMode {
        match conn_str.parse::<tokio_postgres::Config>() {
            Ok(cfg) => match cfg.get_ssl_mode() {
                tokio_postgres::config::SslMode::Disable => PgSslMode::Disable,
                tokio_postgres::config::SslMode::Prefer => PgSslMode::Prefer,
                tokio_postgres::config::SslMode::Require => PgSslMode::Require,
                _ => PgSslMode::Require,
            },
            Err(_) => PgSslMode::Prefer,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssl_mode_parses_url_and_keyword_forms() {
        assert_eq!(
            PgSslMode::from_conn_str("postgresql://u:p@h:5432/db?sslmode=require"),
            PgSslMode::Require
        );
        assert_eq!(
            PgSslMode::from_conn_str("postgres://u@h/db?sslmode=disable"),
            PgSslMode::Disable
        );
        assert_eq!(
            PgSslMode::from_conn_str("host=localhost port=5432 sslmode=require"),
            PgSslMode::Require
        );
        assert_eq!(
            PgSslMode::from_conn_str("host=localhost sslmode=prefer"),
            PgSslMode::Prefer
        );
    }

    #[test]
    fn ssl_mode_defaults_to_prefer_like_libpq() {
        assert_eq!(
            PgSslMode::from_conn_str("host=localhost port=5432 user=postgres"),
            PgSslMode::Prefer
        );
        assert_eq!(
            PgSslMode::from_conn_str("postgresql://u@h/db"),
            PgSslMode::Prefer
        );
        // Unparseable input: fall back to Prefer rather than panicking — the
        // ordinary connection from the same string will surface the parse
        // error with proper context.
        assert_eq!(PgSslMode::from_conn_str("::::"), PgSslMode::Prefer);
    }

    #[test]
    fn verify_ca_and_verify_full_are_rejected_not_downgraded() {
        // tokio-postgres refuses to parse these modes, so a config demanding
        // certificate verification can never silently connect unverified.
        assert!("host=h sslmode=verify-full"
            .parse::<tokio_postgres::Config>()
            .is_err());
        assert!("host=h sslmode=verify-ca"
            .parse::<tokio_postgres::Config>()
            .is_err());
    }

    #[test]
    fn client_config_builds() {
        // The dangerous-verifier construction is all compile-time-checked
        // except the protocol-version expect; exercise it once.
        let _ = client_config();
    }
}
