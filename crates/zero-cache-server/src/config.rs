//! Faithful `ZERO_*` environment configuration, matching upstream
//! `zero-cache/src/config/zero-config.ts` env-var names and defaults.
//!
//! This reads the SAME env var names the real zero-cache uses (so an existing
//! deployment's config works against this binary), honoring the options this
//! port implements and transparently reporting recognized-but-not-yet-honored
//! ones (peripheral subsystems: litestream, change-streamer discovery, cloud
//! events, etc.) via [`ZeroConfig::unimplemented_but_set`].

/// Parsed, honored configuration for the running server.
#[derive(Debug, Clone)]
pub struct ZeroConfig {
    // --- core ---
    /// `ZERO_UPSTREAM_DB` — authoritative upstream Postgres (libpq string).
    pub upstream_db: Option<String>,
    /// `ZERO_UPSTREAM_SCHEMA` (port extension) — schema for pushed mutations.
    pub upstream_schema: String,
    /// `ZERO_REPLICA_FILE` — SQLite replica path (upstream default `zero.db`).
    pub replica_file: String,
    /// `ZERO_PORT` — sync WebSocket port (upstream default 4848). Combined with
    /// the legacy `ZERO_LISTEN_ADDR` override into a bind address.
    pub listen_addr: String,
    /// `ZERO_METRICS_ADDR` (port extension) — ops endpoint bind address.
    pub metrics_addr: String,
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
    /// `ZERO_AUTH_SECRET` — HS256 JWT symmetric key.
    pub auth_secret: Option<String>,
    /// `ZERO_AUTH_ISSUER` — required `iss` claim, if set.
    pub auth_issuer: Option<String>,
    /// `ZERO_AUTH_AUDIENCE` — required `aud` claim, if set.
    pub auth_audience: Option<String>,

    // --- server tuning (honored) ---
    /// `ZERO_NUM_SYNC_WORKERS` — tokio worker-thread count (vertical multi-core).
    /// `Some(0)` on a replicator node = dedicated change-streamer (no client
    /// serving), matching upstream's replication-manager role. `None` = default
    /// (all cores).
    pub num_sync_workers: Option<usize>,
    /// `ZERO_MAX_CONNECTIONS` (port extension) — admission cap.
    pub max_connections: Option<usize>,
    /// `ZERO_FANOUT_CAPACITY` (port extension) — per-connection commit buffer.
    pub fanout_capacity: usize,
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
}


/// Every `ZERO_*` env var name upstream recognizes but this port does NOT yet
/// honor — peripheral/operational subsystems. Set values are accepted (they
/// don't break startup) but have no effect; startup logs which are set.
pub const RECOGNIZED_UNIMPLEMENTED: &[&str] = &[
    // upstream / cvr / change tuning
    "ZERO_UPSTREAM_TYPE",
    "ZERO_UPSTREAM_MAX_CONNS",
    "ZERO_UPSTREAM_PG_REPLICATION_SLOT_FAILOVER",
    "ZERO_CVR_MAX_CONNS",
    "ZERO_CVR_GARBAGE_COLLECTION_INACTIVITY_THRESHOLD_HOURS",
    "ZERO_CVR_GARBAGE_COLLECTION_INITIAL_INTERVAL_SECONDS",
    "ZERO_CVR_GARBAGE_COLLECTION_INITIAL_BATCH_SIZE",
    "ZERO_CHANGE_MAX_CONNS",
    "ZERO_REPLICA_VACUUM_INTERVAL_HOURS",
    // change-streamer discovery (multi-node) — URI/PORT/ADDR are honored;
    // MODE (auto-discovery) is not.
    "ZERO_CHANGE_STREAMER_MODE",
    // auth (asymmetric)
    "ZERO_AUTH_JWK",
    "ZERO_AUTH_JWKS_URL",
    // topology / lifecycle
    "ZERO_KEEPALIVE_TIMEOUT_MS",
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

impl ZeroConfig {
    /// Reads the process environment (faithful upstream names + a couple of port
    /// extensions and legacy aliases).
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

        // Port: canonical ZERO_PORT (upstream, default 4848); legacy
        // ZERO_LISTEN_ADDR overrides the whole bind address if set.
        let listen_addr = match get("ZERO_LISTEN_ADDR") {
            Some(addr) => addr,
            None => format!("0.0.0.0:{}", or("ZERO_PORT", "4848")),
        };

        // Publications: canonical ZERO_APP_PUBLICATIONS; legacy ZERO_PUBLICATION.
        let app_publications = get("ZERO_APP_PUBLICATIONS")
            .or_else(|| get("ZERO_PUBLICATION"))
            .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
            .unwrap_or_default();

        let port: u16 = or("ZERO_PORT", "4848").parse().unwrap_or(4848);
        // Change-streamer bind address: explicit, or port+1 (upstream default).
        let change_streamer_addr = get("ZERO_CHANGE_STREAMER_ADDR").unwrap_or_else(|| {
            let csp: u16 = get("ZERO_CHANGE_STREAMER_PORT")
                .and_then(|s| s.parse().ok())
                .unwrap_or(port + 1);
            format!("0.0.0.0:{csp}")
        });

        ZeroConfig {
            upstream_db: get("ZERO_UPSTREAM_DB"),
            upstream_schema: or("ZERO_UPSTREAM_SCHEMA", "public"),
            replica_file: or("ZERO_REPLICA_FILE", "./zero-replica.db"),
            listen_addr,
            metrics_addr: or("ZERO_METRICS_ADDR", "0.0.0.0:9600"),
            app_id: or("ZERO_APP_ID", "zero"),
            shard_num: get("ZERO_SHARD_NUM").and_then(|s| s.parse().ok()).unwrap_or(0),
            app_publications,
            port,
            change_streamer_uri: get("ZERO_CHANGE_STREAMER_URI"),
            change_streamer_addr,
            mutate_url: get("ZERO_MUTATE_URL").or_else(|| get("ZERO_PUSH_URL")),
            mutate_api_key: get("ZERO_MUTATE_API_KEY").or_else(|| get("ZERO_PUSH_API_KEY")),
            query_url: get("ZERO_QUERY_URL").or_else(|| get("ZERO_GET_QUERIES_URL")),
            query_api_key: get("ZERO_QUERY_API_KEY").or_else(|| get("ZERO_GET_QUERIES_API_KEY")),
            query_forward_cookies: bool_("ZERO_QUERY_FORWARD_COOKIES", false),
            mutate_forward_cookies: bool_("ZERO_MUTATE_FORWARD_COOKIES", false),
            query_allowed_client_headers: csv_list(get("ZERO_QUERY_ALLOWED_CLIENT_HEADERS")),
            mutate_allowed_client_headers: csv_list(get("ZERO_MUTATE_ALLOWED_CLIENT_HEADERS")),
            litestream_backup_url: get("ZERO_LITESTREAM_BACKUP_URL"),
            cvr_db: get("ZERO_CVR_DB").or_else(|| get("ZERO_UPSTREAM_DB")),
            change_db: get("ZERO_CHANGE_DB").or_else(|| get("ZERO_UPSTREAM_DB")),
            auth_secret: get("ZERO_AUTH_SECRET"),
            auth_issuer: get("ZERO_AUTH_ISSUER"),
            auth_audience: get("ZERO_AUTH_AUDIENCE"),
            num_sync_workers: get("ZERO_NUM_SYNC_WORKERS").and_then(|s| s.parse().ok()),
            max_connections: get("ZERO_MAX_CONNECTIONS").and_then(|s| s.parse().ok()),
            fanout_capacity: get("ZERO_FANOUT_CAPACITY")
                .and_then(|s| s.parse().ok())
                .unwrap_or(1024),
            enable_crud_mutations: bool_("ZERO_ENABLE_CRUD_MUTATIONS", true),
            auto_reset: bool_("ZERO_AUTO_RESET", true),
            log_level: or("ZERO_LOG_LEVEL", "info"),
            log_format: or("ZERO_LOG_FORMAT", "text"),
            litestream_log_level: get("ZERO_LITESTREAM_LOG_LEVEL"),
            log_all_replication_reports_at_debug: bool_(
                "ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG",
                false,
            ),
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
        }
    }

    /// The recognized-but-unimplemented env vars currently set in the process
    /// environment — for a transparent startup warning.
    pub fn unimplemented_but_set(&self) -> Vec<&'static str> {
        RECOGNIZED_UNIMPLEMENTED
            .iter()
            .filter(|name| std::env::var(name).map(|v| !v.is_empty()).unwrap_or(false))
            .copied()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> ZeroConfig {
        let map: HashMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        ZeroConfig::from_lookup(|k| map.get(k).cloned())
    }

    #[test]
    fn defaults_match_upstream() {
        let c = cfg(&[]);
        assert_eq!(c.listen_addr, "0.0.0.0:4848"); // ZERO_PORT default 4848
        assert_eq!(c.app_id, "zero");
        assert_eq!(c.shard_num, 0);
        assert!(c.app_publications.is_empty());
        assert!(c.enable_crud_mutations);
        assert!(c.auto_reset);
        assert_eq!(c.log_level, "info");
        assert_eq!(c.upstream_schema, "public");
    }

    #[test]
    fn zero_port_sets_the_listen_address() {
        assert_eq!(cfg(&[("ZERO_PORT", "5000")]).listen_addr, "0.0.0.0:5000");
        // Legacy ZERO_LISTEN_ADDR override wins.
        assert_eq!(
            cfg(&[("ZERO_PORT", "5000"), ("ZERO_LISTEN_ADDR", "127.0.0.1:9")]).listen_addr,
            "127.0.0.1:9"
        );
    }

    #[test]
    fn app_publications_faithful_name_and_legacy_alias() {
        assert_eq!(
            cfg(&[("ZERO_APP_PUBLICATIONS", "pub_a, pub_b")]).app_publications,
            vec!["pub_a".to_string(), "pub_b".to_string()]
        );
        // Legacy singular alias still works.
        assert_eq!(
            cfg(&[("ZERO_PUBLICATION", "legacy_pub")]).app_publications,
            vec!["legacy_pub".to_string()]
        );
    }

    #[test]
    fn enable_crud_mutations_and_auto_reset_toggle() {
        assert!(!cfg(&[("ZERO_ENABLE_CRUD_MUTATIONS", "false")]).enable_crud_mutations);
        assert!(!cfg(&[("ZERO_AUTO_RESET", "0")]).auto_reset);
        assert!(cfg(&[("ZERO_ENABLE_CRUD_MUTATIONS", "yes")]).enable_crud_mutations);
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
    fn recognized_unimplemented_list_covers_peripheral_subsystems() {
        // Sanity: the list includes the big peripheral subsystems.
        for v in ["ZERO_CHANGE_STREAMER_MODE", "ZERO_LAZY_STARTUP"] {
            assert!(RECOGNIZED_UNIMPLEMENTED.contains(&v), "missing {v}");
        }
    }
}
