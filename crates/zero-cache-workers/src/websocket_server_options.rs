//! Port of `syncer.ts`'s `getWebSocketServerOptions` — a second
//! self-contained pure slice of `syncer.ts` (694 lines, the actual
//! `ViewSyncerService`-hosting worker, still otherwise unported). Maps the
//! subset of `ZeroConfig` that controls WebSocket server behavior into the
//! `ws` library's `ServerOptions` shape.
//!
//! Scope deviation: upstream's `perMessageDeflate` can be `true` or an
//! arbitrary `PerMessageDeflateOptions` object parsed from JSON (the `ws`
//! library's compression tuning knobs — window bits, memory level,
//! threshold, etc.). This port doesn't have a `tokio-tungstenite`-side
//! equivalent options struct wired up yet (`zero-cache-server::ws_connection`
//! doesn't expose compression tuning), so `compression_options` is kept as
//! the parsed-but-untyped `JsonValue` rather than a strongly-typed options
//! struct — this function's real job (validate the config, produce a
//! `PerMessageDeflate` decision) doesn't need more than that yet.

use zero_cache_shared::bigint_json::{parse as parse_json, JsonValue, ParseError};

/// Port of `perMessageDeflate`'s two possible values: enabled with default
/// tuning, or enabled with the specific options object the operator
/// configured via `ZERO_WEBSOCKET_COMPRESSION_OPTIONS`.
#[derive(Debug, Clone, PartialEq)]
pub enum PerMessageDeflate {
    Disabled,
    Default,
    Options(JsonValue),
}

/// Port of the fields of `ServerOptions` this function populates.
#[derive(Debug, Clone, PartialEq)]
pub struct WebSocketServerOptions {
    pub no_server: bool,
    pub max_payload: Option<u64>,
    pub per_message_deflate: PerMessageDeflate,
}

/// The subset of `ZeroConfig` this function reads. Port of the
/// `websocket*` fields of `ZeroConfig`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WebSocketConfig {
    pub websocket_max_payload_bytes: Option<u64>,
    pub websocket_compression: bool,
    pub websocket_compression_options: Option<String>,
}

/// Port of `getWebSocketServerOptions`'s failure mode: malformed
/// `ZERO_WEBSOCKET_COMPRESSION_OPTIONS` JSON.
#[derive(Debug, Clone, thiserror::Error)]
#[error("Failed to parse ZERO_WEBSOCKET_COMPRESSION_OPTIONS: {0}. Expected valid JSON.")]
pub struct WebSocketCompressionOptionsError(#[source] ParseError);

/// Port of `getWebSocketServerOptions`.
pub fn get_websocket_server_options(
    config: &WebSocketConfig,
) -> Result<WebSocketServerOptions, WebSocketCompressionOptionsError> {
    let per_message_deflate = if !config.websocket_compression {
        PerMessageDeflate::Disabled
    } else {
        match &config.websocket_compression_options {
            None => PerMessageDeflate::Default,
            Some(raw) => PerMessageDeflate::Options(
                parse_json(raw).map_err(WebSocketCompressionOptionsError)?,
            ),
        }
    };

    Ok(WebSocketServerOptions {
        no_server: true,
        max_payload: config.websocket_max_payload_bytes,
        per_message_deflate,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_server_and_max_payload_always_set() {
        let config = WebSocketConfig {
            websocket_max_payload_bytes: Some(1024),
            ..Default::default()
        };
        let opts = get_websocket_server_options(&config).unwrap();
        assert!(opts.no_server);
        assert_eq!(opts.max_payload, Some(1024));
    }

    #[test]
    fn compression_disabled_by_default() {
        let config = WebSocketConfig::default();
        let opts = get_websocket_server_options(&config).unwrap();
        assert_eq!(opts.per_message_deflate, PerMessageDeflate::Disabled);
    }

    #[test]
    fn compression_enabled_without_options_uses_default_tuning() {
        let config = WebSocketConfig {
            websocket_compression: true,
            ..Default::default()
        };
        let opts = get_websocket_server_options(&config).unwrap();
        assert_eq!(opts.per_message_deflate, PerMessageDeflate::Default);
    }

    #[test]
    fn compression_enabled_with_valid_options_parses_them() {
        let config = WebSocketConfig {
            websocket_compression: true,
            websocket_compression_options: Some(r#"{"threshold":100}"#.to_string()),
            ..Default::default()
        };
        let opts = get_websocket_server_options(&config).unwrap();
        match opts.per_message_deflate {
            PerMessageDeflate::Options(JsonValue::Object(fields)) => {
                assert_eq!(
                    fields,
                    vec![("threshold".to_string(), JsonValue::Number(100.0))]
                );
            }
            other => panic!("expected parsed Options, got {other:?}"),
        }
    }

    #[test]
    fn compression_enabled_with_invalid_json_errors() {
        let config = WebSocketConfig {
            websocket_compression: true,
            websocket_compression_options: Some("not json".to_string()),
            ..Default::default()
        };
        let err = get_websocket_server_options(&config).unwrap_err();
        assert!(err
            .to_string()
            .contains("ZERO_WEBSOCKET_COMPRESSION_OPTIONS"));
    }

    #[test]
    fn compression_options_ignored_when_compression_disabled() {
        // Matches upstream: the options string is only read inside the
        // `if (config.websocketCompression)` branch, so bad JSON there is
        // never even parsed if compression itself is off.
        let config = WebSocketConfig {
            websocket_compression: false,
            websocket_compression_options: Some("not json".to_string()),
            ..Default::default()
        };
        let opts = get_websocket_server_options(&config).unwrap();
        assert_eq!(opts.per_message_deflate, PerMessageDeflate::Disabled);
    }
}
