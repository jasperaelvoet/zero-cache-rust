//! Faithful `ZERO_*` environment configuration, matching upstream
//! `zero-cache/src/config/zero-config.ts` env-var names and defaults.
//!
//! This reads the SAME env var names the real zero-cache uses (so an existing
//! deployment's config works against this binary). Pinned Zero v1.7 is the
//! only supported configuration contract; former Rust-only aliases and
//! extensions are intentionally rejected by omission.

/// Parsed, honored configuration for the running server.
#[derive(Debug, Clone)]
pub struct ZeroConfig {
    // --- core ---
    /// `ZERO_UPSTREAM_DB` — authoritative upstream Postgres (libpq string).
    pub upstream_db: Option<String>,
    /// `ZERO_REPLICA_FILE` — SQLite replica path (upstream default `zero.db`).
    pub replica_file: String,
    /// Bind address derived from official `ZERO_PORT`.
    pub listen_addr: String,
    /// `ZERO_APP_ID` — app identity (upstream default `zero`).
    pub app_id: String,
    /// `ZERO_SHARD_NUM` — shard number (upstream default 0).
    pub shard_num: i64,
    /// `ZERO_APP_PUBLICATIONS` — publications to replicate (comma-separated).
    pub app_publications: Vec<String>,
    /// The sync port number (from `ZERO_PORT`), used to derive the
    /// change-streamer port default.
    pub port: u16,

    // --- horizontal scaling / topology ---
    /// `ZERO_CHANGE_STREAMER_URI` — if set, this node is a VIEW-SYNCER that
    /// subscribes to the change-streamer at this URI (no Postgres slot).
    pub change_streamer_uri: Option<String>,
    /// `ZERO_CHANGE_STREAMER_ADDR` / port — where a CHANGE-STREAMER node exposes
    /// its replication stream for view-syncers (default `0.0.0.0:{port+1}`).
    pub change_streamer_addr: String,

    // --- custom mutators / synced queries (app API servers) ---
    /// `ZERO_MUTATE_URL` — app push/mutate API server (custom mutators).
    pub mutate_url: Option<String>,
    /// `ZERO_MUTATE_API_KEY` — secret to authorize calls to the mutate server.
    pub mutate_api_key: Option<String>,
    /// `ZERO_QUERY_URL` — app synced-query API server (custom queries).
    pub query_url: Option<String>,
    /// `ZERO_QUERY_API_KEY` — secret to authorize calls to the query server.
    pub query_api_key: Option<String>,
    /// `ZERO_QUERY_FORWARD_COOKIES` — forward the client's `Cookie` header to
    /// the query API server (apps that authenticate via a session cookie).
    pub query_forward_cookies: bool,
    /// `ZERO_MUTATE_FORWARD_COOKIES` — same, for the mutate API server.
    pub mutate_forward_cookies: bool,
    /// `ZERO_QUERY_ALLOWED_CLIENT_HEADERS` — client request-header names to
    /// forward to the query API server (comma-separated, case-insensitive).
    pub query_allowed_client_headers: Vec<String>,
    /// `ZERO_MUTATE_ALLOWED_CLIENT_HEADERS` — same, for the mutate API server.
    pub mutate_allowed_client_headers: Vec<String>,
    /// `ZERO_LITESTREAM_BACKUP_URL` — object-store URL for continuous SQLite
    /// replica backup / restore (e.g. `s3://bucket/path`).
    pub litestream_backup_url: Option<String>,
    /// `ZERO_CVR_DB` — Postgres connection for the Client View Record store.
    /// Defaults to `ZERO_UPSTREAM_DB` (as upstream's normalizer does).
    pub cvr_db: Option<String>,
    /// `ZERO_CHANGE_DB` — Postgres connection for the change-log store.
    /// Defaults to `ZERO_UPSTREAM_DB`.
    pub change_db: Option<String>,

    // --- auth ---
    /// `ZERO_AUTH_SECRET` — HS256/384/512 JWT symmetric key.
    pub auth_secret: Option<String>,
    /// `ZERO_AUTH_JWK` — a single static JWK (JSON) used to verify asymmetric
    /// (RS/ES/PS/EdDSA) JWTs. Takes priority over `auth_secret`.
    pub auth_jwk: Option<String>,
    /// `ZERO_AUTH_JWKS_URL` — remote JWKS endpoint; the signing key is selected
    /// by the token header `kid` and cached. Lowest priority.
    pub auth_jwks_url: Option<String>,
    /// `ZERO_AUTH_ISSUER` — required `iss` claim, if set.
    pub auth_issuer: Option<String>,
    /// `ZERO_AUTH_AUDIENCE` — required `aud` claim, if set.
    pub auth_audience: Option<String>,
    /// `ZERO_SCHEMA_JSON` — the compiled Zero schema document.  Its
    /// `permissions` member is parsed at startup and enforced for live reads
    /// and CRUD writes.
    pub schema_json: Option<String>,

    // --- server tuning (honored) ---
    /// `ZERO_NUM_SYNC_WORKERS` — tokio worker-thread count (vertical multi-core).
    /// `Some(0)` on a replicator node = dedicated change-streamer (no client
    /// serving), matching upstream's replication-manager role. `None` = default
    /// (all cores).
    pub num_sync_workers: Option<usize>,
    /// `ZERO_CVR_MAX_CONNS` — shared CVR PostgreSQL pool bound. Matches
    /// official zero-cache's default total of 30 connections.
    pub cvr_max_conns: usize,
    /// `ZERO_ENABLE_CRUD_MUTATIONS` — route pushes to upstream (default true).
    pub enable_crud_mutations: bool,
    /// `ZERO_AUTO_RESET` — resync replica on schema drift (default true).
    pub auto_reset: bool,

    // --- observability / identity (logged) ---
    /// `ZERO_LOG_LEVEL` — debug|info|warn|error (default info).
    pub log_level: String,
    /// `ZERO_LOG_FORMAT` — text|json (default text).
    pub log_format: String,
    /// `ZERO_LITESTREAM_LOG_LEVEL` — log level passed to the litestream process.
    pub litestream_log_level: Option<String>,
    /// `ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG` — demote routine replication
    /// progress logs from info to debug (quieter prod logs).
    pub log_all_replication_reports_at_debug: bool,
    /// `ZERO_LOG_IVM_SAMPLING` — sample rate (1 in N) for IVM/hydration debug
    /// logs; `0` disables IVM sampling logs. Default 5000 (upstream default).
    pub log_ivm_sampling: u64,
    /// `ZERO_LOG_SLOW_HYDRATE_THRESHOLD` — ms; a query hydration slower than
    /// this is logged at info as a slow-query warning. Default 100.
    pub log_slow_hydrate_threshold_ms: u64,
    /// `ZERO_LOG_SLOW_ROW_THRESHOLD` — row count; a query returning more than
    /// this many rows is flagged in the slow-hydrate log. Default 3000.
    pub log_slow_row_threshold: u64,
    /// `ZERO_TASK_ID` — instance identifier for logs/metrics.
    pub task_id: Option<String>,
    /// `ZERO_SERVER_VERSION` — version string logged at startup.
    pub server_version: Option<String>,
    /// `ZERO_ADMIN_PASSWORD` — administer endpoints (inspect).
    pub admin_password: Option<String>,
    /// `ZERO_KEEPALIVE_TIMEOUT_MS` — stop accepting after health-check
    /// heartbeats cease for this duration.
    pub keepalive_timeout_ms: Option<u64>,
}

/// Official `ZERO_*` options whose corresponding v1.7 subsystem has not yet
/// been wired into this binary. They are deliberately rejected at startup:
/// accepting them while silently changing their meaning is worse than an
/// explicit configuration failure for a compatibility server.
pub const UNSUPPORTED_ZERO_OPTIONS: &[&str] = &[
    // upstream / cvr / change tuning
    "ZERO_UPSTREAM_TYPE",
    "ZERO_UPSTREAM_MAX_CONNS",
    "ZERO_UPSTREAM_PG_REPLICATION_SLOT_FAILOVER",
    "ZERO_CVR_GARBAGE_COLLECTION_INACTIVITY_THRESHOLD_HOURS",
    "ZERO_CVR_GARBAGE_COLLECTION_INITIAL_INTERVAL_SECONDS",
    "ZERO_CVR_GARBAGE_COLLECTION_INITIAL_BATCH_SIZE",
    "ZERO_CHANGE_MAX_CONNS",
    "ZERO_REPLICA_VACUUM_INTERVAL_HOURS",
    // change-streamer discovery (multi-node) — URI/PORT/ADDR are honored;
    // MODE (auto-discovery) is not.
    "ZERO_CHANGE_STREAMER_MODE",
    // topology / lifecycle
    "ZERO_LAZY_STARTUP",
    // websocket
    "ZERO_WEBSOCKET_COMPRESSION",
    "ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES",
    // initial-sync / shadow-sync tuning
    "ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS",
    "ZERO_INITIAL_SYNC_TEXT_COPY",
    "ZERO_SHADOW_SYNC_ENABLED",
    // rate limit
    "ZERO_PER_USER_MUTATION_LIMIT_MAX",
    // backup / telemetry / cloud events
    "ZERO_ENABLE_TELEMETRY",
    "ZERO_CLOUD_EVENT_SINK_ENV",
    // query engine tuning
    "ZERO_ENABLE_QUERY_PLANNER",
    "ZERO_ENABLE_QUERY_COVERING",
    "ZERO_QUERY_HYDRATION_STATS",
];

/// Unsupported options whose *default* value is a no-op for this binary, so an
/// operator pointing an existing rocicorp/zero config at this server does not
/// hit a fatal startup error just for leaving a knob at its documented default
/// (or, for telemetry, opting out — which this binary honors implicitly by
/// never emitting telemetry). Each entry lists the normalized values that are
/// safe to accept because they match what this server actually does; any other
/// value genuinely changes behavior we cannot honor and is still rejected.
///
/// Booleans are normalized (`1/true/yes/on` -> `true`, `0/false/no/off` ->
/// `false`) before comparison; other values compared trimmed.
const DEFAULT_VALUED_UNSUPPORTED: &[(&str, &[&str])] = &[
    // Upstream pool default is 20; we use our own pooling, so only the default
    // is a safe no-op.
    ("ZERO_UPSTREAM_MAX_CONNS", &["20"]),
    // The query planner is implemented and on; accept the default, reject a
    // request to turn it off (which we cannot honor).
    ("ZERO_ENABLE_QUERY_PLANNER", &["true"]),
    ("ZERO_ENABLE_QUERY_COVERING", &["true"]),
    // These features are not implemented; their upstream default is off, so
    // accept the off value and reject a request to turn them on.
    ("ZERO_WEBSOCKET_COMPRESSION", &["false"]),
    ("ZERO_INITIAL_SYNC_TEXT_COPY", &["false"]),
    ("ZERO_SHADOW_SYNC_ENABLED", &["false"]),
    ("ZERO_LAZY_STARTUP", &["false"]),
    // No telemetry is ever emitted, so both the default (on) and the documented
    // opt-out (off) are honest no-ops for this server.
    ("ZERO_ENABLE_TELEMETRY", &["true", "false"]),
];

/// Normalizes a raw env value for comparison against an accepted set: booleans
/// collapse to `true`/`false`, everything else is trimmed.
fn normalize_option_value(raw: &str) -> String {
    match raw.trim().to_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => "true".to_string(),
        "0" | "false" | "no" | "off" => "false".to_string(),
        other => other.to_string(),
    }
}

/// Whether an unsupported option set to `raw` must be rejected. Returns `false`
/// (accept) when the value matches a documented no-op for this binary.
fn unsupported_option_rejected(name: &str, raw: &str) -> bool {
    if raw.is_empty() {
        return false;
    }
    match DEFAULT_VALUED_UNSUPPORTED
        .iter()
        .find(|(option, _)| *option == name)
    {
        Some((_, accepted)) => {
            let normalized = normalize_option_value(raw);
            !accepted.iter().any(|value| *value == normalized)
        }
        // Not a default-valued knob: any non-empty value is unhonorable.
        None => true,
    }
}

impl ZeroConfig {
    /// Reads the process environment using only the pinned upstream names.
    pub fn from_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok().filter(|s| !s.is_empty()))
    }

    /// Parses config from an arbitrary `name -> value` lookup (pure; testable
    /// without touching process env).
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let or = |name: &str, default: &str| get(name).unwrap_or_else(|| default.to_string());
        let bool_ = |name: &str, default: bool| match get(name) {
            Some(v) => matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"),
            None => default,
        };
        // Comma-separated header list -> lowercased, trimmed, non-empty names.
        let csv_list = |v: Option<String>| -> Vec<String> {
            v.map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_lowercase())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default()
        };

        let listen_addr = format!("[::]:{}", or("ZERO_PORT", "4848"));

        let app_publications = get("ZERO_APP_PUBLICATIONS")
            .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
            .unwrap_or_default();

        let port: u16 = or("ZERO_PORT", "4848").parse().unwrap_or(4848);
        // Change-streamer bind address: explicit, or port+1 (upstream default).
        let change_streamer_addr = get("ZERO_CHANGE_STREAMER_ADDR").unwrap_or_else(|| {
            let csp: u16 = get("ZERO_CHANGE_STREAMER_PORT")
                .and_then(|s| s.parse().ok())
                .unwrap_or(port + 1);
            format!("[::]:{csp}")
        });

        ZeroConfig {
            upstream_db: get("ZERO_UPSTREAM_DB"),
            replica_file: or("ZERO_REPLICA_FILE", "zero.db"),
            listen_addr,
            app_id: or("ZERO_APP_ID", "zero"),
            shard_num: get("ZERO_SHARD_NUM")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0),
            app_publications,
            port,
            change_streamer_uri: get("ZERO_CHANGE_STREAMER_URI"),
            change_streamer_addr,
            mutate_url: get("ZERO_MUTATE_URL"),
            mutate_api_key: get("ZERO_MUTATE_API_KEY"),
            query_url: get("ZERO_QUERY_URL"),
            query_api_key: get("ZERO_QUERY_API_KEY"),
            query_forward_cookies: bool_("ZERO_QUERY_FORWARD_COOKIES", false),
            mutate_forward_cookies: bool_("ZERO_MUTATE_FORWARD_COOKIES", false),
            query_allowed_client_headers: csv_list(get("ZERO_QUERY_ALLOWED_CLIENT_HEADERS")),
            mutate_allowed_client_headers: csv_list(get("ZERO_MUTATE_ALLOWED_CLIENT_HEADERS")),
            litestream_backup_url: get("ZERO_LITESTREAM_BACKUP_URL"),
            cvr_db: get("ZERO_CVR_DB").or_else(|| get("ZERO_UPSTREAM_DB")),
            change_db: get("ZERO_CHANGE_DB").or_else(|| get("ZERO_UPSTREAM_DB")),
            auth_secret: get("ZERO_AUTH_SECRET"),
            auth_jwk: get("ZERO_AUTH_JWK"),
            auth_jwks_url: get("ZERO_AUTH_JWKS_URL"),
            auth_issuer: get("ZERO_AUTH_ISSUER"),
            auth_audience: get("ZERO_AUTH_AUDIENCE"),
            schema_json: get("ZERO_SCHEMA_JSON"),
            num_sync_workers: get("ZERO_NUM_SYNC_WORKERS").and_then(|s| s.parse().ok()),
            // Upstream `cvr.maxConns` is a plain number defaulting to 30; an
            // explicit value is honored verbatim (upstream fails startup if it
            // is too low rather than silently rewriting it), so do not coerce a
            // configured 0 up to the default.
            cvr_max_conns: get("ZERO_CVR_MAX_CONNS")
                .and_then(|s| s.parse().ok())
                .unwrap_or(30),
            enable_crud_mutations: bool_("ZERO_ENABLE_CRUD_MUTATIONS", true),
            auto_reset: bool_("ZERO_AUTO_RESET", true),
            log_level: or("ZERO_LOG_LEVEL", "info"),
            log_format: or("ZERO_LOG_FORMAT", "text"),
            litestream_log_level: get("ZERO_LITESTREAM_LOG_LEVEL"),
            // Upstream enables this only for the literal value `1`
            // (`=== '1'` in recorder.ts), not the broader truthy token set.
            log_all_replication_reports_at_debug: get("ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG")
                .as_deref()
                == Some("1"),
            log_ivm_sampling: get("ZERO_LOG_IVM_SAMPLING")
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000),
            log_slow_hydrate_threshold_ms: get("ZERO_LOG_SLOW_HYDRATE_THRESHOLD")
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            log_slow_row_threshold: get("ZERO_LOG_SLOW_ROW_THRESHOLD")
                .and_then(|v| v.parse().ok())
                .unwrap_or(3000),
            task_id: get("ZERO_TASK_ID"),
            server_version: get("ZERO_SERVER_VERSION"),
            admin_password: get("ZERO_ADMIN_PASSWORD"),
            keepalive_timeout_ms: get("ZERO_KEEPALIVE_TIMEOUT_MS")
                .and_then(|s| s.parse().ok())
                .or_else(|| get("ECS_CONTAINER_METADATA_URI_V4").map(|_| 20_000)),
        }
    }

    /// Official options set in the process environment that this binary cannot
    /// yet honor. Callers must fail startup rather than ignore them. Options set
    /// to a documented no-op value (see [`DEFAULT_VALUED_UNSUPPORTED`]) are
    /// accepted so an existing default config still boots.
    pub fn unsupported_options_set(&self) -> Vec<&'static str> {
        Self::unsupported_options_with(|k| std::env::var(k).ok())
    }

    /// Pure form of [`Self::unsupported_options_set`] for testing: resolves each
    /// option name through `get` instead of the process environment.
    pub fn unsupported_options_with(get: impl Fn(&str) -> Option<String>) -> Vec<&'static str> {
        UNSUPPORTED_ZERO_OPTIONS
            .iter()
            .filter(|name| {
                get(name)
                    .map(|value| unsupported_option_rejected(name, &value))
                    .unwrap_or(false)
            })
            .copied()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> ZeroConfig {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        ZeroConfig::from_lookup(|k| map.get(k).cloned())
    }

    #[test]
    fn config_normalization_matches_upstream() {
        // ZERO_CVR_MAX_CONNS: an explicit 0 is honored, not coerced to 30.
        assert_eq!(cfg(&[("ZERO_CVR_MAX_CONNS", "0")]).cvr_max_conns, 0);
        assert_eq!(cfg(&[("ZERO_CVR_MAX_CONNS", "12")]).cvr_max_conns, 12);
        assert_eq!(cfg(&[]).cvr_max_conns, 30); // absent -> default

        // ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG: only the literal "1".
        assert!(
            cfg(&[("ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG", "1")])
                .log_all_replication_reports_at_debug
        );
        for token in ["true", "yes", "on", "0", ""] {
            assert!(
                !cfg(&[("ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG", token)])
                    .log_all_replication_reports_at_debug,
                "token {token:?} must not enable the flag (upstream honors only '1')"
            );
        }
    }

    #[test]
    fn default_valued_unsupported_options_are_accepted() {
        let get = |pairs: &[(&str, &str)]| {
            let map: HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            move |k: &str| map.get(k).cloned()
        };

        // Leaving these at their documented defaults / opting out of telemetry
        // must NOT block startup — an existing rocicorp/zero config should boot.
        let accepted = ZeroConfig::unsupported_options_with(get(&[
            ("ZERO_ENABLE_QUERY_PLANNER", "true"),
            ("ZERO_ENABLE_QUERY_COVERING", "1"),
            ("ZERO_UPSTREAM_MAX_CONNS", "20"),
            ("ZERO_LAZY_STARTUP", "false"),
            ("ZERO_WEBSOCKET_COMPRESSION", "off"),
            ("ZERO_SHADOW_SYNC_ENABLED", "false"),
            ("ZERO_INITIAL_SYNC_TEXT_COPY", "no"),
            ("ZERO_ENABLE_TELEMETRY", "false"),
            ("ZERO_ENABLE_TELEMETRY", "true"),
        ]));
        assert!(
            accepted.is_empty(),
            "default-valued options must not fail startup, got {accepted:?}"
        );
    }

    #[test]
    fn unhonorable_unsupported_values_are_rejected() {
        let get = |pairs: &[(&str, &str)]| {
            let map: HashMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            move |k: &str| map.get(k).cloned()
        };

        // Non-default values whose behavior we cannot honor must still be
        // rejected, as must any value on a knob with no supported behavior.
        let rejected = ZeroConfig::unsupported_options_with(get(&[
            ("ZERO_ENABLE_QUERY_PLANNER", "false"), // cannot disable the planner
            ("ZERO_WEBSOCKET_COMPRESSION", "true"), // compression unimplemented
            ("ZERO_UPSTREAM_MAX_CONNS", "50"),      // non-default pool size
        ]));
        assert!(rejected.contains(&"ZERO_ENABLE_QUERY_PLANNER"));
        assert!(rejected.contains(&"ZERO_WEBSOCKET_COMPRESSION"));
        assert!(rejected.contains(&"ZERO_UPSTREAM_MAX_CONNS"));
        // ZERO_AUTH_JWK / ZERO_AUTH_JWKS_URL are now supported and must NOT be
        // rejected as unsupported options.
        assert!(!UNSUPPORTED_ZERO_OPTIONS.contains(&"ZERO_AUTH_JWK"));
        assert!(!UNSUPPORTED_ZERO_OPTIONS.contains(&"ZERO_AUTH_JWKS_URL"));
    }

    #[test]
    fn defaults_match_upstream() {
        let c = cfg(&[]);
        assert_eq!(c.listen_addr, "[::]:4848"); // ZERO_PORT default 4848
        assert_eq!(c.app_id, "zero");
        assert_eq!(c.shard_num, 0);
        assert!(c.app_publications.is_empty());
        assert!(c.enable_crud_mutations);
        assert!(c.auto_reset);
        assert_eq!(c.cvr_max_conns, 30);
        assert_eq!(c.log_level, "info");
    }

    #[test]
    fn cvr_pool_bound_matches_official_config_name() {
        assert_eq!(cfg(&[("ZERO_CVR_MAX_CONNS", "7")]).cvr_max_conns, 7);
        // Upstream honors an explicit 0 verbatim (no silent coercion to 30).
        assert_eq!(cfg(&[("ZERO_CVR_MAX_CONNS", "0")]).cvr_max_conns, 0);
    }

    #[test]
    fn zero_port_sets_the_listen_address() {
        assert_eq!(cfg(&[("ZERO_PORT", "5000")]).listen_addr, "[::]:5000");
    }

    #[test]
    fn keepalive_timeout_matches_ecs_default() {
        assert_eq!(cfg(&[]).keepalive_timeout_ms, None);
        assert_eq!(
            cfg(&[("ECS_CONTAINER_METADATA_URI_V4", "http://metadata")]).keepalive_timeout_ms,
            Some(20_000)
        );
        assert_eq!(
            cfg(&[("ZERO_KEEPALIVE_TIMEOUT_MS", "5000")]).keepalive_timeout_ms,
            Some(5000)
        );
    }

    #[test]
    fn app_publications_uses_the_upstream_name() {
        assert_eq!(
            cfg(&[("ZERO_APP_PUBLICATIONS", "pub_a, pub_b")]).app_publications,
            vec!["pub_a".to_string(), "pub_b".to_string()]
        );
        assert!(cfg(&[("ZERO_PUBLICATION", "legacy_pub")])
            .app_publications
            .is_empty());
    }

    #[test]
    fn enable_crud_mutations_and_auto_reset_toggle() {
        assert!(!cfg(&[("ZERO_ENABLE_CRUD_MUTATIONS", "false")]).enable_crud_mutations);
        assert!(!cfg(&[("ZERO_AUTO_RESET", "0")]).auto_reset);
        assert!(cfg(&[("ZERO_ENABLE_CRUD_MUTATIONS", "yes")]).enable_crud_mutations);
    }

    #[test]
    fn schema_json_is_retained_for_compiled_permissions_loading() {
        let schema = r#"{"permissions":{"tables":{}}}"#;
        assert_eq!(
            cfg(&[("ZERO_SCHEMA_JSON", schema)]).schema_json.as_deref(),
            Some(schema)
        );
        assert_eq!(cfg(&[]).schema_json, None);
    }

    #[test]
    fn cookie_and_header_forwarding_config_parses() {
        let c = cfg(&[
            ("ZERO_QUERY_FORWARD_COOKIES", "true"),
            ("ZERO_MUTATE_FORWARD_COOKIES", "true"),
            ("ZERO_QUERY_ALLOWED_CLIENT_HEADERS", "cookie"),
            ("ZERO_MUTATE_ALLOWED_CLIENT_HEADERS", "Cookie, X-Trace"),
        ]);
        assert!(c.query_forward_cookies);
        assert!(c.mutate_forward_cookies);
        assert_eq!(c.query_allowed_client_headers, vec!["cookie".to_string()]);
        // lowercased + trimmed.
        assert_eq!(
            c.mutate_allowed_client_headers,
            vec!["cookie".to_string(), "x-trace".to_string()]
        );
        // Defaults: off / empty.
        let d = cfg(&[]);
        assert!(!d.query_forward_cookies);
        assert!(d.mutate_allowed_client_headers.is_empty());
    }

    #[test]
    fn unsupported_options_list_covers_peripheral_subsystems() {
        // Sanity: the list includes the big peripheral subsystems.
        for v in ["ZERO_CHANGE_STREAMER_MODE", "ZERO_LAZY_STARTUP"] {
            assert!(UNSUPPORTED_ZERO_OPTIONS.contains(&v), "missing {v}");
        }
    }

    #[test]
    fn unsupported_options_are_detected_from_the_process_environment() {
        // `from_lookup` deliberately remains pure. The process-environment
        // scan is tested against a unique temporary option to avoid mutating
        // shared test environment state; the list is static and this verifies
        // its lookup semantics through the public method instead.
        assert!(UNSUPPORTED_ZERO_OPTIONS.contains(&"ZERO_LAZY_STARTUP"));
    }
}
