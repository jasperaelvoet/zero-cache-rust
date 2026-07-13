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

    // --- upstream tuning ---
    /// `ZERO_UPSTREAM_TYPE` — upstream kind (`pg` | `custom`; upstream default
    /// `pg`). `custom` (an HTTP change-source endpoint) is an unreleased,
    /// hidden upstream feature and is rejected at startup.
    pub upstream_type: String,
    /// `ZERO_UPSTREAM_MAX_CONNS` — bound on upstream connections used for
    /// committing mutations (upstream default 20; excludes the replication
    /// stream connection). Enforced by the shared upstream mutation limiter.
    pub upstream_max_conns: usize,
    /// `ZERO_UPSTREAM_MAX_CONNS_PER_WORKER` — hidden upstream option (main
    /// thread → sync worker plumbing). In this single-process server an
    /// explicit value simply overrides the shared limiter bound.
    pub upstream_max_conns_per_worker: Option<usize>,
    /// `ZERO_UPSTREAM_PG_REPLICATION_SLOT_FAILOVER` — create the replication
    /// slot with `(FAILOVER)` on Postgres 17+ (no-op below 17, as upstream).
    pub pg_replication_slot_failover: bool,

    // --- CVR garbage collection ---
    /// `ZERO_CVR_GARBAGE_COLLECTION_INACTIVITY_THRESHOLD_HOURS` (default 48).
    pub cvr_gc_inactivity_threshold_hours: f64,
    /// `ZERO_CVR_GARBAGE_COLLECTION_INITIAL_INTERVAL_SECONDS` (default 60).
    pub cvr_gc_initial_interval_seconds: f64,
    /// `ZERO_CVR_GARBAGE_COLLECTION_INITIAL_BATCH_SIZE` (default 25; 0
    /// disables CVR GC, as upstream).
    pub cvr_gc_initial_batch_size: u64,
    /// `ZERO_CVR_MAX_CONNS_PER_WORKER` — hidden upstream option; an explicit
    /// value overrides the CVR pool bound in this single-process server.
    pub cvr_max_conns_per_worker: Option<usize>,

    // --- change db tuning ---
    /// `ZERO_CHANGE_MAX_CONNS` — bound on change-db connections (upstream
    /// default 5). This server keeps the change-log in the SQLite replica and
    /// opens no separate change-db connections, so the bound is enforced
    /// vacuously; it is parsed and validated for config parity.
    pub change_max_conns: usize,
    /// `ZERO_CHANGE_STATEMENT_TIMEOUT_MS` — hidden upstream option (default
    /// 20000): fail change-log transactions when a statement stalls. Applied
    /// as the busy/statement budget for durable change-log writes.
    pub change_statement_timeout_ms: u64,
    /// `ZERO_CHANGE_LOG_BATCH_SIZE` — hidden upstream option (default 2000,
    /// must be an integer >= 1): max change-log rows per multi-row insert.
    pub change_log_batch_size: u64,

    // --- replica maintenance ---
    /// `ZERO_REPLICA_VACUUM_INTERVAL_HOURS` — VACUUM at startup when this many
    /// hours elapsed since the last sync/upgrade/vacuum event (tracked in
    /// `_zero.runtimeEvents`, as upstream). Unset = never VACUUM.
    pub replica_vacuum_interval_hours: Option<f64>,

    // --- query engine ---
    /// `ZERO_QUERY_HYDRATION_STATS` — track and log rows considered by slow
    /// hydrations (upstream `runtimeDebugFlags.trackRowCountsVended`).
    pub query_hydration_stats: bool,
    /// `ZERO_ENABLE_QUERY_PLANNER` — enable the ZQL query planner (default
    /// true). When false, plans skip join-strategy optimization.
    pub enable_query_planner: bool,
    /// `ZERO_ENABLE_QUERY_COVERING` — shadow-mode query-covering detection
    /// during hydration (default true).
    pub enable_query_covering: bool,
    /// `ZERO_YIELD_THRESHOLD_MS` — max time spent in IVM work before yielding
    /// to the executor (default 10).
    pub yield_threshold_ms: u64,

    // --- change-streamer extras ---
    /// `ZERO_CHANGE_STREAMER_MODE` — `dedicated` (default) or `discover`
    /// (view-syncers look up the change-streamer address registered by the
    /// replication-manager). Ignored when `ZERO_CHANGE_STREAMER_URI` is set.
    pub change_streamer_mode: String,
    /// `ZERO_CHANGE_STREAMER_PROTOCOL` — deprecated (`ws` | `wss`, default
    /// `ws`); used only with the deprecated address-based discovery.
    pub change_streamer_protocol: String,
    /// `ZERO_CHANGE_STREAMER_DISCOVERY_INTERFACE_PREFERENCES` — hidden
    /// upstream option: interface-name prefixes preferred when picking the
    /// externally reachable IP to register for discovery.
    pub discovery_interface_preferences: Vec<String>,
    /// `ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS` — delay before the
    /// change-streamer takes over the replication stream (default 15000),
    /// canceled early by an incoming change-stream request.
    pub change_streamer_startup_delay_ms: u64,
    /// `ZERO_CHANGE_STREAMER_BACK_PRESSURE_LIMIT_HEAP_PROPORTION` — upstream
    /// default 0.04. This server's fan-out applies back pressure through its
    /// bounded commit queue; the proportion scales that bound.
    pub back_pressure_limit_heap_proportion: f64,
    /// `ZERO_CHANGE_STREAMER_FLOW_CONTROL_CONSENSUS_PADDING_SECONDS` —
    /// upstream default 1; grace period granted to laggard subscribers after
    /// a majority acks a flow-control check. Negative disables early release.
    pub flow_control_consensus_padding_seconds: f64,

    // --- rate limiting ---
    /// `ZERO_PER_USER_MUTATION_LIMIT_MAX` — max mutations per user per sliding
    /// window; unset = unlimited (upstream default).
    pub per_user_mutation_limit_max: Option<u64>,
    /// `ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS` — sliding window (default
    /// 60000).
    pub per_user_mutation_limit_window_ms: u64,

    // --- replication lag reporting ---
    /// `ZERO_REPLICATION_LAG_REPORT_INTERVAL_MS` — min interval between
    /// replication-lag reports (default 30000; <= 0 disables).
    pub replication_lag_report_interval_ms: i64,

    // --- websocket ---
    /// `ZERO_WEBSOCKET_COMPRESSION` — permessage-deflate (upstream default
    /// false).
    pub websocket_compression: bool,
    /// `ZERO_WEBSOCKET_COMPRESSION_OPTIONS` — JSON tuning for compression
    /// (validated at startup when compression is enabled).
    pub websocket_compression_options: Option<String>,
    /// `ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES` — max incoming WS message size
    /// (default 10 MiB), rejected before parsing.
    pub websocket_max_payload_bytes: u64,

    // --- initial sync ---
    /// `ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS` — parallel table-copy
    /// connections during initial sync (default 5; effective count is
    /// min(workers, tables), as upstream).
    pub initial_sync_table_copy_workers: usize,
    /// `ZERO_INITIAL_SYNC_PROFILE_COPY` — hidden upstream option: profile the
    /// copy phase. This server logs per-table copy timings when set.
    pub initial_sync_profile_copy: bool,
    /// `ZERO_INITIAL_SYNC_TEXT_COPY` — use text-format COPY instead of binary
    /// (default false).
    pub initial_sync_text_copy: bool,

    // --- shadow sync (canary) ---
    /// `ZERO_SHADOW_SYNC_ENABLED` — periodic canary initial-sync into a
    /// throwaway replica (default false; change-streamer node only).
    pub shadow_sync_enabled: bool,
    /// `ZERO_SHADOW_SYNC_INTERVAL_HOURS` — canary interval (default 12); the
    /// first run is jittered into [2/3, 1) of the interval, as upstream.
    pub shadow_sync_interval_hours: f64,
    /// `ZERO_SHADOW_SYNC_SAMPLE_RATE` — BERNOULLI sample rate (default 0.1;
    /// >= 1 copies all rows).
    pub shadow_sync_sample_rate: f64,
    /// `ZERO_SHADOW_SYNC_MAX_ROWS_PER_TABLE` — per-table row cap (default
    /// 10000).
    pub shadow_sync_max_rows_per_table: u64,

    // --- lifecycle / misc ---
    /// `ZERO_LAZY_STARTUP` — defer replication until the first request
    /// (single-node only, as upstream).
    pub lazy_startup: bool,
    /// `ZERO_STORAGE_DB_TMP_DIR` — tmp directory for operator storage /
    /// scratch SQLite files; unset = the OS tmp dir.
    pub storage_db_tmp_dir: Option<String>,
    /// `ZERO_ENABLE_TELEMETRY` / `DO_NOT_TRACK` — telemetry opt-out contract.
    /// This server never phones home; the flag gates only local anonymous
    /// usage counters exposed on the metrics endpoint.
    pub enable_telemetry: bool,
    /// `ZERO_CLOUD_EVENT_SINK_ENV` — NAME of an env var holding a CloudEvents
    /// sink URI (knative K_SINK binding shape). Lifecycle ZeroEvents are
    /// POSTed there when configured.
    pub cloud_event_sink_env: Option<String>,
    /// `ZERO_CLOUD_EVENT_EXTENSION_OVERRIDES_ENV` — NAME of an env var holding
    /// a JSON `{"extensions": {...}}` object merged onto outbound CloudEvents.
    pub cloud_event_extension_overrides_env: Option<String>,

    // --- API server request-header forwarding ---
    /// `ZERO_QUERY_ALLOWED_REQUEST_HEADERS` — connection-request headers
    /// (e.g. proxy-injected) forwarded to the query API server.
    pub query_allowed_request_headers: Vec<String>,
    /// `ZERO_MUTATE_ALLOWED_REQUEST_HEADERS` — same, for the mutate server.
    pub mutate_allowed_request_headers: Vec<String>,

    // --- periodic auth work ---
    /// `ZERO_AUTH_REVALIDATE_INTERVAL_SECONDS` — interval between periodic
    /// /query auth revalidation for validated connections (default 300).
    pub auth_revalidate_interval_seconds: u64,
    /// `ZERO_AUTH_RETRANSFORM_INTERVAL_SECONDS` — interval between periodic
    /// shared /query retransform work per client group (default 300).
    pub auth_retransform_interval_seconds: u64,

    // --- litestream (consumed by the spawned litestream process and the
    //     replica checkpoint/backup plumbing) ---
    /// `ZERO_LITESTREAM_EXECUTABLE` — path to the rocicorp-fork litestream
    /// binary (unset = `litestream` on PATH, as the container image installs).
    pub litestream_executable: Option<String>,
    /// `ZERO_LITESTREAM_EXECUTABLE_V5` — v0.5.x litestream used when
    /// restore-using-v5 is set.
    pub litestream_executable_v5: Option<String>,
    /// `ZERO_LITESTREAM_RESTORE_USING_V5` (default false).
    pub litestream_restore_using_v5: bool,
    /// `ZERO_LITESTREAM_BACKUP_USING_V5` (default false; requires
    /// restore-using-v5, as upstream).
    pub litestream_backup_using_v5: bool,
    /// `ZERO_LITESTREAM_CONFIG_PATH` — litestream yaml config (upstream
    /// default `./src/services/litestream/config.yml`).
    pub litestream_config_path: String,
    /// `ZERO_LITESTREAM_ENDPOINT` — S3-compatible endpoint override.
    pub litestream_endpoint: Option<String>,
    /// `ZERO_LITESTREAM_REGION` — AWS region for the backup bucket.
    pub litestream_region: Option<String>,
    /// `ZERO_LITESTREAM_PORT` — litestream metrics port (default port+2).
    pub litestream_port: u16,
    /// `ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB` (default 40).
    pub litestream_checkpoint_threshold_mb: u64,
    /// `ZERO_LITESTREAM_MIN_CHECKPOINT_PAGE_COUNT` — default
    /// checkpointThresholdMB * 250 (4KB pages), as upstream.
    pub litestream_min_checkpoint_page_count: u64,
    /// `ZERO_LITESTREAM_MAX_CHECKPOINT_PAGE_COUNT` — default min * 10; 0
    /// disables RESTART checkpoints.
    pub litestream_max_checkpoint_page_count: u64,
    /// `ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES` (default 15).
    pub litestream_incremental_backup_interval_minutes: u64,
    /// `ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_HOURS` (default 12).
    pub litestream_snapshot_backup_interval_hours: u64,
    /// `ZERO_LITESTREAM_RESTORE_PARALLELISM` (default 48).
    pub litestream_restore_parallelism: u64,
    /// `ZERO_LITESTREAM_MULTIPART_CONCURRENCY` (default 48).
    pub litestream_multipart_concurrency: u64,
    /// `ZERO_LITESTREAM_MULTIPART_SIZE` (default 16 MiB).
    pub litestream_multipart_size: u64,
    /// `ZERO_LITESTREAM_VFS_EXTENSION_PATH` (upstream default
    /// `/usr/local/lib/litestream-vfs.so`).
    pub litestream_vfs_extension_path: String,
    /// `ZERO_LITESTREAM_VFS_PROBE_INTERVAL_MS` (default 30000).
    pub litestream_vfs_probe_interval_ms: u64,
    /// `ZERO_LITESTREAM_VFS_PROBE_TIMEOUT_MS` (default 30000).
    pub litestream_vfs_probe_timeout_ms: u64,
    /// `ZERO_LITESTREAM_VFS_LOG_FILE` — optional VFS extension log path.
    pub litestream_vfs_log_file: Option<String>,

    /// Fatal parse-time errors accumulated while reading the environment
    /// (invalid boolean/number tokens, bad `ZERO_APP_ID`, out-of-union log
    /// levels/formats). Upstream `parseOptions` throws on the first such value;
    /// this port collects them and surfaces them through
    /// [`Self::startup_errors`] so startup fails exactly as upstream would.
    pub parse_errors: Vec<String>,
}

/// Fatal configuration errors, matching upstream's parse-time asserts. Every
/// official `ZERO_*` option is now parsed and honored; what remains fatal is
/// what upstream itself rejects (removed options, invalid values, conflicting
/// combinations) plus `upstream.type=custom`, which upstream marks hidden /
/// unreleased ("TODO: Unhide when ready to officially support").
fn config_errors(cfg: &ZeroConfig, get: &impl Fn(&str) -> Option<String>) -> Vec<String> {
    // Parse-time errors (invalid bool/number tokens, bad app id, out-of-union
    // log level/format) come first: upstream's `parseOptions` throws on these
    // before any cross-field assert runs.
    let mut errors = cfg.parse_errors.clone();

    // Upstream's shardOptions.id assert fires whenever the option is set.
    if get("ZERO_SHARD_ID").is_some() {
        errors.push("ZERO_SHARD_ID is no longer an option. Please use ZERO_APP_ID instead.".into());
    }

    match cfg.upstream_type.as_str() {
        "pg" => {}
        "custom" => errors.push(
            "ZERO_UPSTREAM_TYPE=custom (HTTP change-source endpoints) is an unreleased \
             upstream feature and is not supported by this server; use the default \"pg\""
                .into(),
        ),
        other => errors.push(format!(
            "invalid ZERO_UPSTREAM_TYPE {other:?} (expected \"pg\" or \"custom\")"
        )),
    }

    if cfg.change_log_batch_size < 1 {
        errors.push("change.logBatchSize must be an integer >= 1".into());
    }

    if !matches!(cfg.change_streamer_mode.as_str(), "dedicated" | "discover") {
        errors.push(format!(
            "invalid ZERO_CHANGE_STREAMER_MODE {:?} (expected \"dedicated\" or \"discover\")",
            cfg.change_streamer_mode
        ));
    }
    if !matches!(cfg.change_streamer_protocol.as_str(), "ws" | "wss") {
        errors.push(format!(
            "invalid ZERO_CHANGE_STREAMER_PROTOCOL {:?} (expected \"ws\" or \"wss\")",
            cfg.change_streamer_protocol
        ));
    }

    // Upstream: "Only one of jwk, jwksUrl and secret may be set."
    let auth_sources = [&cfg.auth_jwk, &cfg.auth_jwks_url, &cfg.auth_secret]
        .iter()
        .filter(|v| v.is_some())
        .count();
    if auth_sources > 1 {
        errors.push("Only one of jwk, jwksUrl and secret may be set.".into());
    }

    // Upstream validates websocket compression options JSON at startup.
    if cfg.websocket_compression {
        let ws_config = zero_cache_workers::websocket_server_options::WebSocketConfig {
            websocket_max_payload_bytes: Some(cfg.websocket_max_payload_bytes),
            websocket_compression: true,
            websocket_compression_options: cfg.websocket_compression_options.clone(),
        };
        if let Err(e) =
            zero_cache_workers::websocket_server_options::get_websocket_server_options(&ws_config)
        {
            errors.push(format!("invalid ZERO_WEBSOCKET_COMPRESSION_OPTIONS: {e}"));
        }
    }

    // Upstream main.ts fail-fast checks: pool bounds "must allow for at least
    // one connection per sync worker". numSyncers defaults to
    // max(1, availableParallelism() - 1); numSyncWorkers=0 is the
    // replication-manager config, which skips both checks.
    let num_syncers = cfg.resolved_num_syncers();
    if num_syncers > 0 {
        if cfg.enable_crud_mutations && cfg.upstream_max_conns < num_syncers {
            errors.push(format!(
                "Insufficient upstream connections (ZERO_UPSTREAM_MAX_CONNS={}) for {} sync \
                 workers: need at least one connection per sync worker",
                cfg.upstream_max_conns, num_syncers
            ));
        }
        if cfg.cvr_max_conns < num_syncers {
            errors.push(format!(
                "Insufficient cvr connections (ZERO_CVR_MAX_CONNS={}) for {} sync workers: \
                 need at least one connection per sync worker",
                cfg.cvr_max_conns, num_syncers
            ));
        }
    }

    // Upstream: backup-using-v5 "requires ZERO_LITESTREAM_RESTORE_USING_V5".
    if cfg.litestream_backup_using_v5 && !cfg.litestream_restore_using_v5 {
        errors.push(
            "ZERO_LITESTREAM_BACKUP_USING_V5 requires ZERO_LITESTREAM_RESTORE_USING_V5".into(),
        );
    }

    errors
}

/// Deprecation warnings matching upstream's flag-resolution warnings: emitted
/// once at startup for each deprecated option that is actually set.
fn config_deprecations(get: &impl Fn(&str) -> Option<String>) -> Vec<String> {
    const DEPRECATED: &[(&str, &str)] = &[
        ("ZERO_PUSH_URL", "ZERO_MUTATE_URL"),
        ("ZERO_PUSH_API_KEY", "ZERO_MUTATE_API_KEY"),
        ("ZERO_PUSH_FORWARD_COOKIES", "ZERO_MUTATE_FORWARD_COOKIES"),
        (
            "ZERO_PUSH_ALLOWED_CLIENT_HEADERS",
            "ZERO_MUTATE_ALLOWED_CLIENT_HEADERS",
        ),
        (
            "ZERO_PUSH_ALLOWED_REQUEST_HEADERS",
            "ZERO_MUTATE_ALLOWED_REQUEST_HEADERS",
        ),
        ("ZERO_GET_QUERIES_URL", "ZERO_QUERY_URL"),
        ("ZERO_GET_QUERIES_API_KEY", "ZERO_QUERY_API_KEY"),
        (
            "ZERO_GET_QUERIES_FORWARD_COOKIES",
            "ZERO_QUERY_FORWARD_COOKIES",
        ),
        (
            "ZERO_GET_QUERIES_ALLOWED_CLIENT_HEADERS",
            "ZERO_QUERY_ALLOWED_CLIENT_HEADERS",
        ),
        (
            "ZERO_GET_QUERIES_ALLOWED_REQUEST_HEADERS",
            "ZERO_QUERY_ALLOWED_REQUEST_HEADERS",
        ),
        (
            "ZERO_CHANGE_STREAMER_ADDRESS",
            "ZERO_CHANGE_STREAMER_URI (on view-syncers)",
        ),
        (
            "ZERO_CHANGE_STREAMER_PROTOCOL",
            "ZERO_CHANGE_STREAMER_URI (on view-syncers)",
        ),
    ];
    let mut warnings: Vec<String> = DEPRECATED
        .iter()
        .filter(|(name, _)| get(name).is_some())
        .map(|(name, replacement)| format!("{name} is deprecated; use {replacement} instead"))
        .collect();
    if get("ZERO_TARGET_CLIENT_ROW_COUNT").is_some() {
        warnings.push(
            "ZERO_TARGET_CLIENT_ROW_COUNT is no longer used and will be removed in a \
             future version (TTL-based expiration manages client cache size)"
                .into(),
        );
    }
    warnings
}

/// Parses a boolean exactly like upstream `parseBoolean` (options.ts): only
/// `true`/`1` → true and `false`/`0` → false; anything else is a fatal error
/// (upstream throws `TypeError`). An unset value takes `default`.
fn parse_bool(
    name: &str,
    val: Option<String>,
    default: bool,
    errs: &std::cell::RefCell<Vec<String>>,
) -> bool {
    match val {
        Some(v) => match v.to_lowercase().as_str() {
            "true" | "1" => true,
            "false" | "0" => false,
            _ => {
                errs.borrow_mut()
                    .push(format!("Invalid input for {name}: \"{v}\""));
                default
            }
        },
        None => default,
    }
}

/// Parses a numeric option, erroring on unparseable input like upstream's
/// `Number(input)` + `Number.isNaN` throw (options.ts). An unset value takes
/// `default`.
fn parse_num<T: std::str::FromStr>(
    name: &str,
    val: Option<String>,
    default: T,
    errs: &std::cell::RefCell<Vec<String>>,
) -> T {
    match val {
        Some(v) => match v.parse::<T>() {
            Ok(n) => n,
            Err(_) => {
                errs.borrow_mut()
                    .push(format!("Invalid input for {name}: \"{v}\""));
                default
            }
        },
        None => default,
    }
}

/// Parses an optional numeric option: unset → `None`; a present but
/// unparseable value is a fatal error (upstream throws) and yields `None`.
fn parse_opt_num<T: std::str::FromStr>(
    name: &str,
    val: Option<String>,
    errs: &std::cell::RefCell<Vec<String>>,
) -> Option<T> {
    match val {
        Some(v) => match v.parse::<T>() {
            Ok(n) => Some(n),
            Err(_) => {
                errs.borrow_mut()
                    .push(format!("Invalid input for {name}: \"{v}\""));
                None
            }
        },
        None => None,
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
        // Accumulates fatal parse-time errors (invalid bool/number tokens, bad
        // app id, out-of-union log level/format). Upstream `parseOptions`
        // throws on the first such value; we collect them and fail startup via
        // `startup_errors` so behavior matches while all issues are reported.
        let errs = std::cell::RefCell::new(Vec::<String>::new());

        let or = |name: &str, default: &str| get(name).unwrap_or_else(|| default.to_string());
        let bool_ = |name: &str, default: bool| parse_bool(name, get(name), default, &errs);
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

        let u64_ = |name: &str, default: u64| -> u64 { parse_num(name, get(name), default, &errs) };
        let f64_ = |name: &str, default: f64| -> f64 { parse_num(name, get(name), default, &errs) };
        // Upstream deprecated aliases: `mutate.*` supersedes `push.*` and
        // `query.*` supersedes `getQueries.*`; the new name wins when both are
        // set (upstream's flag resolution order).
        let aliased = |primary: &str, deprecated: &str| get(primary).or_else(|| get(deprecated));

        let app_publications = get("ZERO_APP_PUBLICATIONS")
            .map(|s| s.split(',').map(|p| p.trim().to_string()).collect())
            .unwrap_or_default();

        let port: u16 = parse_num("ZERO_PORT", get("ZERO_PORT"), 4848, &errs);
        let listen_addr = format!("[::]:{port}");
        // Change-streamer bind address: explicit, or port+1 (upstream default).
        let change_streamer_addr = get("ZERO_CHANGE_STREAMER_ADDR").unwrap_or_else(|| {
            let csp: u16 = parse_num(
                "ZERO_CHANGE_STREAMER_PORT",
                get("ZERO_CHANGE_STREAMER_PORT"),
                port + 1,
                &errs,
            );
            format!("[::]:{csp}")
        });

        // Upstream `appOptions.id` asserts `/^[a-z0-9_]+$/` (types/shards.ts).
        let app_id = or("ZERO_APP_ID", "zero");
        if app_id.is_empty()
            || !app_id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        {
            errs.borrow_mut().push(
                "The App ID may only consist of lower-case letters, numbers, and the \
                 underscore character"
                    .into(),
            );
        }

        let mut cfg = ZeroConfig {
            upstream_db: get("ZERO_UPSTREAM_DB"),
            replica_file: or("ZERO_REPLICA_FILE", "zero.db"),
            listen_addr,
            app_id,
            shard_num: parse_num("ZERO_SHARD_NUM", get("ZERO_SHARD_NUM"), 0, &errs),
            app_publications,
            port,
            change_streamer_uri: get("ZERO_CHANGE_STREAMER_URI"),
            change_streamer_addr,
            mutate_url: aliased("ZERO_MUTATE_URL", "ZERO_PUSH_URL"),
            mutate_api_key: aliased("ZERO_MUTATE_API_KEY", "ZERO_PUSH_API_KEY"),
            query_url: aliased("ZERO_QUERY_URL", "ZERO_GET_QUERIES_URL"),
            query_api_key: aliased("ZERO_QUERY_API_KEY", "ZERO_GET_QUERIES_API_KEY"),
            query_forward_cookies: if get("ZERO_QUERY_FORWARD_COOKIES").is_some() {
                bool_("ZERO_QUERY_FORWARD_COOKIES", false)
            } else {
                bool_("ZERO_GET_QUERIES_FORWARD_COOKIES", false)
            },
            mutate_forward_cookies: if get("ZERO_MUTATE_FORWARD_COOKIES").is_some() {
                bool_("ZERO_MUTATE_FORWARD_COOKIES", false)
            } else {
                bool_("ZERO_PUSH_FORWARD_COOKIES", false)
            },
            query_allowed_client_headers: csv_list(aliased(
                "ZERO_QUERY_ALLOWED_CLIENT_HEADERS",
                "ZERO_GET_QUERIES_ALLOWED_CLIENT_HEADERS",
            )),
            mutate_allowed_client_headers: csv_list(aliased(
                "ZERO_MUTATE_ALLOWED_CLIENT_HEADERS",
                "ZERO_PUSH_ALLOWED_CLIENT_HEADERS",
            )),
            litestream_backup_url: get("ZERO_LITESTREAM_BACKUP_URL"),
            cvr_db: get("ZERO_CVR_DB").or_else(|| get("ZERO_UPSTREAM_DB")),
            change_db: get("ZERO_CHANGE_DB").or_else(|| get("ZERO_UPSTREAM_DB")),
            auth_secret: get("ZERO_AUTH_SECRET"),
            auth_jwk: get("ZERO_AUTH_JWK"),
            auth_jwks_url: get("ZERO_AUTH_JWKS_URL"),
            auth_issuer: get("ZERO_AUTH_ISSUER"),
            auth_audience: get("ZERO_AUTH_AUDIENCE"),
            schema_json: get("ZERO_SCHEMA_JSON"),
            num_sync_workers: parse_opt_num(
                "ZERO_NUM_SYNC_WORKERS",
                get("ZERO_NUM_SYNC_WORKERS"),
                &errs,
            ),
            // Upstream `cvr.maxConns` is a plain number defaulting to 30; an
            // explicit value is honored verbatim (upstream fails startup if it
            // is too low rather than silently rewriting it), so do not coerce a
            // configured 0 up to the default.
            cvr_max_conns: parse_num("ZERO_CVR_MAX_CONNS", get("ZERO_CVR_MAX_CONNS"), 30, &errs),
            enable_crud_mutations: bool_("ZERO_ENABLE_CRUD_MUTATIONS", true),
            auto_reset: bool_("ZERO_AUTO_RESET", true),
            // Upstream log options are literal unions: level debug|info|warn|
            // error, format text|json.
            log_level: {
                let v = or("ZERO_LOG_LEVEL", "info");
                if !matches!(v.as_str(), "debug" | "info" | "warn" | "error") {
                    errs.borrow_mut()
                        .push(format!("Invalid input for ZERO_LOG_LEVEL: \"{v}\""));
                }
                v
            },
            log_format: {
                let v = or("ZERO_LOG_FORMAT", "text");
                if !matches!(v.as_str(), "text" | "json") {
                    errs.borrow_mut()
                        .push(format!("Invalid input for ZERO_LOG_FORMAT: \"{v}\""));
                }
                v
            },
            // Upstream: literalUnion('debug','info','warn','error').default('warn').
            litestream_log_level: {
                let v = or("ZERO_LITESTREAM_LOG_LEVEL", "warn");
                if !matches!(v.as_str(), "debug" | "info" | "warn" | "error") {
                    errs.borrow_mut().push(format!(
                        "Invalid input for ZERO_LITESTREAM_LOG_LEVEL: \"{v}\""
                    ));
                }
                Some(v)
            },
            // Upstream enables this only for the literal value `1`
            // (`=== '1'` in recorder.ts), not the broader truthy token set.
            log_all_replication_reports_at_debug: get("ZERO_LOG_ALL_REPLICATION_REPORTS_AT_DEBUG")
                .as_deref()
                == Some("1"),
            log_ivm_sampling: u64_("ZERO_LOG_IVM_SAMPLING", 5000),
            log_slow_hydrate_threshold_ms: u64_("ZERO_LOG_SLOW_HYDRATE_THRESHOLD", 100),
            log_slow_row_threshold: u64_("ZERO_LOG_SLOW_ROW_THRESHOLD", 3000),
            task_id: get("ZERO_TASK_ID"),
            server_version: get("ZERO_SERVER_VERSION"),
            admin_password: get("ZERO_ADMIN_PASSWORD"),
            keepalive_timeout_ms: parse_opt_num(
                "ZERO_KEEPALIVE_TIMEOUT_MS",
                get("ZERO_KEEPALIVE_TIMEOUT_MS"),
                &errs,
            )
            .or_else(|| get("ECS_CONTAINER_METADATA_URI_V4").map(|_| 20_000)),

            upstream_type: or("ZERO_UPSTREAM_TYPE", "pg"),
            upstream_max_conns: u64_("ZERO_UPSTREAM_MAX_CONNS", 20) as usize,
            upstream_max_conns_per_worker: parse_opt_num(
                "ZERO_UPSTREAM_MAX_CONNS_PER_WORKER",
                get("ZERO_UPSTREAM_MAX_CONNS_PER_WORKER"),
                &errs,
            ),
            pg_replication_slot_failover: bool_(
                "ZERO_UPSTREAM_PG_REPLICATION_SLOT_FAILOVER",
                false,
            ),

            cvr_gc_inactivity_threshold_hours: f64_(
                "ZERO_CVR_GARBAGE_COLLECTION_INACTIVITY_THRESHOLD_HOURS",
                48.0,
            ),
            cvr_gc_initial_interval_seconds: f64_(
                "ZERO_CVR_GARBAGE_COLLECTION_INITIAL_INTERVAL_SECONDS",
                60.0,
            ),
            cvr_gc_initial_batch_size: u64_("ZERO_CVR_GARBAGE_COLLECTION_INITIAL_BATCH_SIZE", 25),
            cvr_max_conns_per_worker: parse_opt_num(
                "ZERO_CVR_MAX_CONNS_PER_WORKER",
                get("ZERO_CVR_MAX_CONNS_PER_WORKER"),
                &errs,
            ),

            change_max_conns: u64_("ZERO_CHANGE_MAX_CONNS", 5) as usize,
            change_statement_timeout_ms: u64_("ZERO_CHANGE_STATEMENT_TIMEOUT_MS", 20_000),
            change_log_batch_size: u64_("ZERO_CHANGE_LOG_BATCH_SIZE", 2_000),

            replica_vacuum_interval_hours: parse_opt_num(
                "ZERO_REPLICA_VACUUM_INTERVAL_HOURS",
                get("ZERO_REPLICA_VACUUM_INTERVAL_HOURS"),
                &errs,
            ),

            query_hydration_stats: bool_("ZERO_QUERY_HYDRATION_STATS", false),
            enable_query_planner: bool_("ZERO_ENABLE_QUERY_PLANNER", true),
            enable_query_covering: bool_("ZERO_ENABLE_QUERY_COVERING", true),
            yield_threshold_ms: u64_("ZERO_YIELD_THRESHOLD_MS", 10),

            change_streamer_mode: or("ZERO_CHANGE_STREAMER_MODE", "dedicated"),
            change_streamer_protocol: or("ZERO_CHANGE_STREAMER_PROTOCOL", "ws"),
            discovery_interface_preferences: {
                let prefs = csv_list(get("ZERO_CHANGE_STREAMER_DISCOVERY_INTERFACE_PREFERENCES"));
                if prefs.is_empty() {
                    // Upstream DEFAULT_PREFERRED_PREFIXES (config/network.ts):
                    // linux ethernet + macOS interface prefixes, so VPN/tunnel
                    // interfaces are not selected for discovery registration.
                    vec!["eth".to_string(), "en".to_string()]
                } else {
                    prefs
                }
            },
            change_streamer_startup_delay_ms: u64_("ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS", 15_000),
            back_pressure_limit_heap_proportion: f64_(
                "ZERO_CHANGE_STREAMER_BACK_PRESSURE_LIMIT_HEAP_PROPORTION",
                0.04,
            ),
            flow_control_consensus_padding_seconds: f64_(
                "ZERO_CHANGE_STREAMER_FLOW_CONTROL_CONSENSUS_PADDING_SECONDS",
                1.0,
            ),

            per_user_mutation_limit_max: parse_opt_num(
                "ZERO_PER_USER_MUTATION_LIMIT_MAX",
                get("ZERO_PER_USER_MUTATION_LIMIT_MAX"),
                &errs,
            ),
            per_user_mutation_limit_window_ms: u64_(
                "ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS",
                60_000,
            ),

            replication_lag_report_interval_ms: parse_num(
                "ZERO_REPLICATION_LAG_REPORT_INTERVAL_MS",
                get("ZERO_REPLICATION_LAG_REPORT_INTERVAL_MS"),
                30_000,
                &errs,
            ),

            websocket_compression: bool_("ZERO_WEBSOCKET_COMPRESSION", false),
            websocket_compression_options: get("ZERO_WEBSOCKET_COMPRESSION_OPTIONS"),
            websocket_max_payload_bytes: u64_("ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES", 10 * 1024 * 1024),

            initial_sync_table_copy_workers: u64_("ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS", 5)
                as usize,
            initial_sync_profile_copy: bool_("ZERO_INITIAL_SYNC_PROFILE_COPY", false),
            initial_sync_text_copy: bool_("ZERO_INITIAL_SYNC_TEXT_COPY", false),

            shadow_sync_enabled: bool_("ZERO_SHADOW_SYNC_ENABLED", false),
            shadow_sync_interval_hours: f64_("ZERO_SHADOW_SYNC_INTERVAL_HOURS", 12.0),
            shadow_sync_sample_rate: f64_("ZERO_SHADOW_SYNC_SAMPLE_RATE", 0.1),
            shadow_sync_max_rows_per_table: u64_("ZERO_SHADOW_SYNC_MAX_ROWS_PER_TABLE", 10_000),

            lazy_startup: bool_("ZERO_LAZY_STARTUP", false),
            storage_db_tmp_dir: get("ZERO_STORAGE_DB_TMP_DIR"),
            // Upstream: telemetry is on by default; ZERO_ENABLE_TELEMETRY=false
            // or the standard DO_NOT_TRACK env var opts out.
            enable_telemetry: bool_("ZERO_ENABLE_TELEMETRY", true) && get("DO_NOT_TRACK").is_none(),
            cloud_event_sink_env: get("ZERO_CLOUD_EVENT_SINK_ENV"),
            cloud_event_extension_overrides_env: get("ZERO_CLOUD_EVENT_EXTENSION_OVERRIDES_ENV"),

            query_allowed_request_headers: csv_list(aliased(
                "ZERO_QUERY_ALLOWED_REQUEST_HEADERS",
                "ZERO_GET_QUERIES_ALLOWED_REQUEST_HEADERS",
            )),
            mutate_allowed_request_headers: csv_list(aliased(
                "ZERO_MUTATE_ALLOWED_REQUEST_HEADERS",
                "ZERO_PUSH_ALLOWED_REQUEST_HEADERS",
            )),

            auth_revalidate_interval_seconds: u64_("ZERO_AUTH_REVALIDATE_INTERVAL_SECONDS", 300),
            auth_retransform_interval_seconds: u64_("ZERO_AUTH_RETRANSFORM_INTERVAL_SECONDS", 300),

            litestream_executable: get("ZERO_LITESTREAM_EXECUTABLE"),
            litestream_executable_v5: get("ZERO_LITESTREAM_EXECUTABLE_V5"),
            litestream_restore_using_v5: bool_("ZERO_LITESTREAM_RESTORE_USING_V5", false),
            litestream_backup_using_v5: bool_("ZERO_LITESTREAM_BACKUP_USING_V5", false),
            litestream_config_path: or(
                "ZERO_LITESTREAM_CONFIG_PATH",
                "./src/services/litestream/config.yml",
            ),
            litestream_endpoint: get("ZERO_LITESTREAM_ENDPOINT"),
            litestream_region: get("ZERO_LITESTREAM_REGION"),
            litestream_port: parse_num(
                "ZERO_LITESTREAM_PORT",
                get("ZERO_LITESTREAM_PORT"),
                port + 2,
                &errs,
            ),
            litestream_checkpoint_threshold_mb: u64_("ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB", 40),
            // Upstream defaults: min = thresholdMB * 250 (4KB pages), max =
            // min * 10 (0 disables RESTART checkpoints).
            litestream_min_checkpoint_page_count: {
                let threshold = u64_("ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB", 40);
                u64_("ZERO_LITESTREAM_MIN_CHECKPOINT_PAGE_COUNT", threshold * 250)
            },
            litestream_max_checkpoint_page_count: {
                let threshold = u64_("ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB", 40);
                let min = u64_("ZERO_LITESTREAM_MIN_CHECKPOINT_PAGE_COUNT", threshold * 250);
                u64_("ZERO_LITESTREAM_MAX_CHECKPOINT_PAGE_COUNT", min * 10)
            },
            litestream_incremental_backup_interval_minutes: u64_(
                "ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES",
                15,
            ),
            litestream_snapshot_backup_interval_hours: u64_(
                "ZERO_LITESTREAM_SNAPSHOT_BACKUP_INTERVAL_HOURS",
                12,
            ),
            litestream_restore_parallelism: u64_("ZERO_LITESTREAM_RESTORE_PARALLELISM", 48),
            litestream_multipart_concurrency: u64_("ZERO_LITESTREAM_MULTIPART_CONCURRENCY", 48),
            litestream_multipart_size: u64_("ZERO_LITESTREAM_MULTIPART_SIZE", 16 * 1024 * 1024),
            litestream_vfs_extension_path: or(
                "ZERO_LITESTREAM_VFS_EXTENSION_PATH",
                "/usr/local/lib/litestream-vfs.so",
            ),
            litestream_vfs_probe_interval_ms: u64_("ZERO_LITESTREAM_VFS_PROBE_INTERVAL_MS", 30_000),
            litestream_vfs_probe_timeout_ms: u64_("ZERO_LITESTREAM_VFS_PROBE_TIMEOUT_MS", 30_000),
            litestream_vfs_log_file: get("ZERO_LITESTREAM_VFS_LOG_FILE"),

            parse_errors: Vec::new(),
        };
        // Collect after every field closure has run — the parse helpers push
        // into `errs` during struct construction above.
        cfg.parse_errors = errs.borrow().clone();
        cfg
    }

    /// The resolved sync-worker count for pool-division math: the configured
    /// `ZERO_NUM_SYNC_WORKERS`, defaulting to `max(1, availableParallelism-1)`
    /// as upstream's normalizer does. `Some(0)` (the replication-manager
    /// config) resolves to 0.
    pub fn resolved_num_syncers(&self) -> usize {
        match self.num_sync_workers {
            Some(n) => n,
            None => std::cmp::max(
                1,
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(2)
                    - 1,
            ),
        }
    }

    /// The effective CVR pool bound: the hidden per-worker override when set
    /// (upstream's main-thread → worker plumbing), else the total. This is a
    /// single-process server, so the total is not divided further.
    pub fn effective_cvr_max_conns(&self) -> usize {
        self.cvr_max_conns_per_worker.unwrap_or(self.cvr_max_conns)
    }

    /// The effective bound on concurrently-open upstream mutation connections.
    pub fn effective_upstream_max_conns(&self) -> usize {
        self.upstream_max_conns_per_worker
            .unwrap_or(self.upstream_max_conns)
    }

    /// Fatal configuration errors (upstream parse-time asserts + startup
    /// checks), resolved against the process environment.
    pub fn startup_errors(&self) -> Vec<String> {
        self.startup_errors_with(|k| std::env::var(k).ok().filter(|s| !s.is_empty()))
    }

    /// Pure form of [`Self::startup_errors`] for testing.
    pub fn startup_errors_with(&self, get: impl Fn(&str) -> Option<String>) -> Vec<String> {
        config_errors(self, &get)
    }

    /// Deprecation warnings for set deprecated options, resolved against the
    /// process environment. Logged once at startup.
    pub fn deprecation_warnings() -> Vec<String> {
        Self::deprecation_warnings_with(|k| std::env::var(k).ok().filter(|s| !s.is_empty()))
    }

    /// Pure form of [`Self::deprecation_warnings`] for testing.
    pub fn deprecation_warnings_with(get: impl Fn(&str) -> Option<String>) -> Vec<String> {
        config_deprecations(&get)
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

    fn get_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    #[test]
    fn every_official_option_is_accepted_at_any_value() {
        // A full production rocicorp/zero config — every option set, many at
        // non-default values — must produce zero startup errors.
        let env = [
            ("ZERO_UPSTREAM_TYPE", "pg"),
            ("ZERO_UPSTREAM_MAX_CONNS", "4"),
            ("ZERO_UPSTREAM_PG_REPLICATION_SLOT_FAILOVER", "true"),
            (
                "ZERO_CVR_GARBAGE_COLLECTION_INACTIVITY_THRESHOLD_HOURS",
                "24",
            ),
            ("ZERO_CVR_GARBAGE_COLLECTION_INITIAL_INTERVAL_SECONDS", "30"),
            ("ZERO_CVR_GARBAGE_COLLECTION_INITIAL_BATCH_SIZE", "50"),
            ("ZERO_CHANGE_MAX_CONNS", "2"),
            ("ZERO_CHANGE_STATEMENT_TIMEOUT_MS", "10000"),
            ("ZERO_CHANGE_LOG_BATCH_SIZE", "500"),
            ("ZERO_CHANGE_STREAMER_MODE", "discover"),
            ("ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS", "5000"),
            ("ZERO_REPLICA_VACUUM_INTERVAL_HOURS", "168"),
            ("ZERO_QUERY_HYDRATION_STATS", "true"),
            ("ZERO_ENABLE_QUERY_PLANNER", "false"),
            ("ZERO_ENABLE_QUERY_COVERING", "false"),
            ("ZERO_YIELD_THRESHOLD_MS", "5"),
            ("ZERO_PER_USER_MUTATION_LIMIT_MAX", "100"),
            ("ZERO_PER_USER_MUTATION_LIMIT_WINDOW_MS", "30000"),
            ("ZERO_REPLICATION_LAG_REPORT_INTERVAL_MS", "10000"),
            ("ZERO_WEBSOCKET_COMPRESSION", "true"),
            ("ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES", "5242880"),
            ("ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS", "3"),
            ("ZERO_INITIAL_SYNC_TEXT_COPY", "true"),
            ("ZERO_SHADOW_SYNC_ENABLED", "true"),
            ("ZERO_SHADOW_SYNC_INTERVAL_HOURS", "6"),
            ("ZERO_SHADOW_SYNC_SAMPLE_RATE", "0.5"),
            ("ZERO_SHADOW_SYNC_MAX_ROWS_PER_TABLE", "5000"),
            ("ZERO_LAZY_STARTUP", "true"),
            ("ZERO_STORAGE_DB_TMP_DIR", "/tmp/zero-storage"),
            ("ZERO_ENABLE_TELEMETRY", "false"),
            ("ZERO_CLOUD_EVENT_SINK_ENV", "K_SINK"),
            ("ZERO_AUTH_REVALIDATE_INTERVAL_SECONDS", "60"),
            ("ZERO_AUTH_RETRANSFORM_INTERVAL_SECONDS", "120"),
            ("ZERO_NUM_SYNC_WORKERS", "2"),
        ];
        let c = ZeroConfig::from_lookup(get_of(&env));
        let errors = c.startup_errors_with(get_of(&env));
        assert!(
            errors.is_empty(),
            "a full official config must not fail startup, got {errors:?}"
        );

        // Spot-check the parsed values.
        assert_eq!(c.upstream_max_conns, 4);
        assert!(c.pg_replication_slot_failover);
        assert_eq!(c.cvr_gc_inactivity_threshold_hours, 24.0);
        assert_eq!(c.cvr_gc_initial_batch_size, 50);
        assert_eq!(c.change_log_batch_size, 500);
        assert_eq!(c.change_streamer_mode, "discover");
        assert_eq!(c.replica_vacuum_interval_hours, Some(168.0));
        assert!(!c.enable_query_planner);
        assert_eq!(c.yield_threshold_ms, 5);
        assert_eq!(c.per_user_mutation_limit_max, Some(100));
        assert!(c.websocket_compression);
        assert_eq!(c.websocket_max_payload_bytes, 5_242_880);
        assert_eq!(c.initial_sync_table_copy_workers, 3);
        assert!(c.initial_sync_text_copy);
        assert!(c.shadow_sync_enabled);
        assert!(c.lazy_startup);
        assert!(!c.enable_telemetry);
        assert_eq!(c.auth_revalidate_interval_seconds, 60);
    }

    #[test]
    fn config_defaults_match_upstream_for_new_options() {
        let c = cfg(&[]);
        assert_eq!(c.upstream_type, "pg");
        assert_eq!(c.upstream_max_conns, 20);
        assert!(!c.pg_replication_slot_failover);
        assert_eq!(c.cvr_gc_inactivity_threshold_hours, 48.0);
        assert_eq!(c.cvr_gc_initial_interval_seconds, 60.0);
        assert_eq!(c.cvr_gc_initial_batch_size, 25);
        assert_eq!(c.change_max_conns, 5);
        assert_eq!(c.change_statement_timeout_ms, 20_000);
        assert_eq!(c.change_log_batch_size, 2_000);
        assert_eq!(c.replica_vacuum_interval_hours, None);
        assert!(!c.query_hydration_stats);
        assert!(c.enable_query_planner);
        assert!(c.enable_query_covering);
        assert_eq!(c.yield_threshold_ms, 10);
        assert_eq!(c.change_streamer_mode, "dedicated");
        assert_eq!(c.change_streamer_protocol, "ws");
        assert_eq!(
            c.discovery_interface_preferences,
            vec!["eth".to_string(), "en".to_string()]
        );
        assert_eq!(c.change_streamer_startup_delay_ms, 15_000);
        assert_eq!(c.back_pressure_limit_heap_proportion, 0.04);
        assert_eq!(c.flow_control_consensus_padding_seconds, 1.0);
        assert_eq!(c.per_user_mutation_limit_max, None);
        assert_eq!(c.per_user_mutation_limit_window_ms, 60_000);
        assert_eq!(c.replication_lag_report_interval_ms, 30_000);
        assert!(!c.websocket_compression);
        assert_eq!(c.websocket_max_payload_bytes, 10 * 1024 * 1024);
        assert_eq!(c.initial_sync_table_copy_workers, 5);
        assert!(!c.initial_sync_text_copy);
        assert!(!c.shadow_sync_enabled);
        assert_eq!(c.shadow_sync_interval_hours, 12.0);
        assert_eq!(c.shadow_sync_sample_rate, 0.1);
        assert_eq!(c.shadow_sync_max_rows_per_table, 10_000);
        assert!(!c.lazy_startup);
        assert!(c.enable_telemetry);
        assert_eq!(c.auth_revalidate_interval_seconds, 300);
        assert_eq!(c.auth_retransform_interval_seconds, 300);
        // Litestream defaults, including the derived checkpoint page counts.
        assert_eq!(c.litestream_checkpoint_threshold_mb, 40);
        assert_eq!(c.litestream_min_checkpoint_page_count, 40 * 250);
        assert_eq!(c.litestream_max_checkpoint_page_count, 40 * 250 * 10);
        assert_eq!(c.litestream_port, 4848 + 2);
        assert_eq!(c.litestream_incremental_backup_interval_minutes, 15);
        assert_eq!(c.litestream_snapshot_backup_interval_hours, 12);
        assert_eq!(c.litestream_restore_parallelism, 48);
    }

    #[test]
    fn removed_and_invalid_options_are_fatal() {
        // ZERO_SHARD_ID: upstream's assert fires whenever it is set.
        let env = [("ZERO_SHARD_ID", "0")];
        let errors = cfg(&[]).startup_errors_with(get_of(&env));
        assert!(errors.iter().any(|e| e.contains("ZERO_SHARD_ID")));

        // upstream type: custom is unreleased; garbage is invalid.
        let c = cfg(&[("ZERO_UPSTREAM_TYPE", "custom")]);
        assert!(c
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("custom")));
        let c = cfg(&[("ZERO_UPSTREAM_TYPE", "mysql")]);
        assert!(!c.startup_errors_with(get_of(&[])).is_empty());

        // change.logBatchSize must be >= 1 (upstream assert).
        let c = cfg(&[("ZERO_CHANGE_LOG_BATCH_SIZE", "0")]);
        assert!(c
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("logBatchSize")));

        // Only one of jwk / jwksUrl / secret (upstream assert).
        let c = cfg(&[("ZERO_AUTH_SECRET", "s"), ("ZERO_AUTH_JWK", "{}")]);
        assert!(c
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("Only one of")));

        // Insufficient pool bounds for the sync-worker count (upstream check).
        let c = cfg(&[
            ("ZERO_NUM_SYNC_WORKERS", "8"),
            ("ZERO_UPSTREAM_MAX_CONNS", "4"),
        ]);
        assert!(c
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("Insufficient upstream connections")));
        // …but not when CRUD mutations are disabled (upstream gates the check).
        let c = cfg(&[
            ("ZERO_NUM_SYNC_WORKERS", "8"),
            ("ZERO_UPSTREAM_MAX_CONNS", "4"),
            ("ZERO_ENABLE_CRUD_MUTATIONS", "false"),
        ]);
        assert!(!c
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("Insufficient upstream connections")));
        // numSyncWorkers=0 (replication-manager) skips both pool checks.
        let c = cfg(&[("ZERO_NUM_SYNC_WORKERS", "0"), ("ZERO_CVR_MAX_CONNS", "0")]);
        assert!(!c
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("Insufficient")));

        // backup-using-v5 requires restore-using-v5 (upstream requirement).
        let c = cfg(&[("ZERO_LITESTREAM_BACKUP_USING_V5", "true")]);
        assert!(c
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("RESTORE_USING_V5")));

        // Invalid websocket compression options JSON is fatal when
        // compression is enabled (upstream parse-time error)…
        let c = cfg(&[
            ("ZERO_WEBSOCKET_COMPRESSION", "true"),
            ("ZERO_WEBSOCKET_COMPRESSION_OPTIONS", "{not json"),
        ]);
        assert!(!c.startup_errors_with(get_of(&[])).is_empty());
        // …and ignored when compression is disabled (upstream only parses the
        // options when compression is on).
        let c = cfg(&[("ZERO_WEBSOCKET_COMPRESSION_OPTIONS", "{not json")]);
        assert!(c.startup_errors_with(get_of(&[])).is_empty());
    }

    #[test]
    fn deprecated_aliases_resolve_and_warn() {
        // push.* / getQueries.* fall back into mutate.* / query.*…
        let c = cfg(&[
            ("ZERO_PUSH_URL", "https://push.example/m"),
            ("ZERO_GET_QUERIES_URL", "https://gq.example/q"),
            ("ZERO_PUSH_FORWARD_COOKIES", "true"),
            ("ZERO_GET_QUERIES_ALLOWED_CLIENT_HEADERS", "X-A"),
        ]);
        assert_eq!(c.mutate_url.as_deref(), Some("https://push.example/m"));
        assert_eq!(c.query_url.as_deref(), Some("https://gq.example/q"));
        assert!(c.mutate_forward_cookies);
        assert_eq!(c.query_allowed_client_headers, vec!["x-a".to_string()]);

        // …the new name wins when both are set…
        let c = cfg(&[
            ("ZERO_PUSH_URL", "https://old.example"),
            ("ZERO_MUTATE_URL", "https://new.example"),
        ]);
        assert_eq!(c.mutate_url.as_deref(), Some("https://new.example"));

        // …and each set deprecated option produces one warning.
        let env = [
            ("ZERO_PUSH_URL", "https://old.example"),
            ("ZERO_CHANGE_STREAMER_PROTOCOL", "wss"),
            ("ZERO_TARGET_CLIENT_ROW_COUNT", "20000"),
        ];
        let warnings = ZeroConfig::deprecation_warnings_with(get_of(&env));
        assert!(warnings.iter().any(|w| w.contains("ZERO_PUSH_URL")));
        assert!(warnings
            .iter()
            .any(|w| w.contains("ZERO_CHANGE_STREAMER_PROTOCOL")));
        assert!(warnings
            .iter()
            .any(|w| w.contains("ZERO_TARGET_CLIENT_ROW_COUNT")));
        assert!(ZeroConfig::deprecation_warnings_with(get_of(&[])).is_empty());
    }

    #[test]
    fn request_header_forwarding_lists_parse_with_aliases() {
        let c = cfg(&[
            (
                "ZERO_QUERY_ALLOWED_REQUEST_HEADERS",
                "X-Forwarded-For, CF-Ray",
            ),
            ("ZERO_PUSH_ALLOWED_REQUEST_HEADERS", "x-trace"),
        ]);
        assert_eq!(
            c.query_allowed_request_headers,
            vec!["x-forwarded-for".to_string(), "cf-ray".to_string()]
        );
        assert_eq!(
            c.mutate_allowed_request_headers,
            vec!["x-trace".to_string()]
        );
    }

    #[test]
    fn telemetry_honors_do_not_track() {
        assert!(cfg(&[]).enable_telemetry);
        assert!(!cfg(&[("ZERO_ENABLE_TELEMETRY", "false")]).enable_telemetry);
        assert!(!cfg(&[("DO_NOT_TRACK", "1")]).enable_telemetry);
    }

    #[test]
    fn per_worker_overrides_take_precedence() {
        let c = cfg(&[
            ("ZERO_CVR_MAX_CONNS", "30"),
            ("ZERO_CVR_MAX_CONNS_PER_WORKER", "6"),
            ("ZERO_UPSTREAM_MAX_CONNS", "20"),
            ("ZERO_UPSTREAM_MAX_CONNS_PER_WORKER", "5"),
        ]);
        assert_eq!(c.effective_cvr_max_conns(), 6);
        assert_eq!(c.effective_upstream_max_conns(), 5);
        let c = cfg(&[("ZERO_CVR_MAX_CONNS", "12")]);
        assert_eq!(c.effective_cvr_max_conns(), 12);
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
        assert!(cfg(&[("ZERO_ENABLE_CRUD_MUTATIONS", "1")]).enable_crud_mutations);
        // "yes"/"on" are NOT valid booleans (upstream parseBoolean throws).
        let c = cfg(&[("ZERO_ENABLE_CRUD_MUTATIONS", "yes")]);
        assert!(c
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("ZERO_ENABLE_CRUD_MUTATIONS")));
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
    fn formerly_rejected_options_are_now_honored_not_fatal() {
        // These once forced a startup failure ("cannot be ignored"); every one
        // is now a real, parsed option, so setting it must NOT be an error.
        for (name, value) in [
            ("ZERO_CHANGE_STREAMER_MODE", "discover"),
            ("ZERO_LAZY_STARTUP", "true"),
            ("ZERO_SHADOW_SYNC_ENABLED", "true"),
            ("ZERO_INITIAL_SYNC_TEXT_COPY", "true"),
            ("ZERO_ENABLE_QUERY_PLANNER", "false"),
            ("ZERO_WEBSOCKET_COMPRESSION", "true"),
            ("ZERO_UPSTREAM_MAX_CONNS", "50"),
            ("ZERO_REPLICA_VACUUM_INTERVAL_HOURS", "168"),
            ("ZERO_ENABLE_TELEMETRY", "false"),
        ] {
            let c = cfg(&[(name, value)]);
            assert!(
                c.startup_errors_with(get_of(&[(name, value)])).is_empty(),
                "{name}={value} must not be a fatal config error"
            );
        }
    }

    #[test]
    fn invalid_boolean_tokens_are_fatal() {
        // Upstream parseBoolean accepts only true/1/false/0; everything else
        // throws. The port must fail startup, not silently coerce to false.
        for bad in ["yes", "on", "no", "off", "enabled", "2", "truthy", ""] {
            let c = cfg(&[("ZERO_AUTO_RESET", bad)]);
            assert!(
                c.startup_errors_with(get_of(&[]))
                    .iter()
                    .any(|e| e.contains("ZERO_AUTO_RESET")),
                "ZERO_AUTO_RESET={bad:?} must be a fatal config error"
            );
        }
        // The valid tokens parse cleanly with no error.
        for (val, expected) in [("true", true), ("1", true), ("false", false), ("0", false)] {
            let c = cfg(&[("ZERO_AUTO_RESET", val)]);
            assert!(c.startup_errors_with(get_of(&[])).is_empty());
            assert_eq!(c.auto_reset, expected);
        }
    }

    #[test]
    fn invalid_numbers_are_fatal() {
        // Unparseable numerics fail startup (upstream Number()+throw), rather
        // than silently falling back to the default.
        for (name, bad) in [
            ("ZERO_UPSTREAM_MAX_CONNS", "abc"),
            ("ZERO_PORT", "not-a-port"),
            ("ZERO_YIELD_THRESHOLD_MS", "ten"),
            ("ZERO_KEEPALIVE_TIMEOUT_MS", "soon"),
            ("ZERO_SHADOW_SYNC_SAMPLE_RATE", "half"),
            ("ZERO_SHARD_NUM", "x"),
            ("ZERO_PER_USER_MUTATION_LIMIT_MAX", "lots"),
        ] {
            let c = cfg(&[(name, bad)]);
            assert!(
                c.startup_errors_with(get_of(&[]))
                    .iter()
                    .any(|e| e.contains(name)),
                "{name}={bad:?} must be a fatal config error"
            );
        }
        // Float-typed options keep their float value (v.number() upstream).
        assert_eq!(
            cfg(&[("ZERO_SHADOW_SYNC_SAMPLE_RATE", "0.25")]).shadow_sync_sample_rate,
            0.25
        );
    }

    #[test]
    fn invalid_app_id_is_fatal() {
        for bad in ["MyApp", "app-1", "app.name", "app id", ""] {
            let c = cfg(&[("ZERO_APP_ID", bad)]);
            assert!(
                c.startup_errors_with(get_of(&[]))
                    .iter()
                    .any(|e| e.contains("App ID")),
                "ZERO_APP_ID={bad:?} must be a fatal config error"
            );
        }
        // Valid ids (lower-case, digits, underscore) are accepted.
        for good in ["zero", "my_app", "app_2", "z0"] {
            let c = cfg(&[("ZERO_APP_ID", good)]);
            assert!(c.startup_errors_with(get_of(&[])).is_empty());
            assert_eq!(c.app_id, good);
        }
    }

    #[test]
    fn litestream_log_level_defaults_warn_and_validates_union() {
        // Upstream default is 'warn' (not unset).
        assert_eq!(cfg(&[]).litestream_log_level.as_deref(), Some("warn"));
        // Valid union members pass through.
        assert_eq!(
            cfg(&[("ZERO_LITESTREAM_LOG_LEVEL", "debug")])
                .litestream_log_level
                .as_deref(),
            Some("debug")
        );
        // Out-of-union values fail startup.
        let c = cfg(&[("ZERO_LITESTREAM_LOG_LEVEL", "verbose")]);
        assert!(c
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("ZERO_LITESTREAM_LOG_LEVEL")));
    }

    #[test]
    fn log_level_and_format_validate_union() {
        assert!(cfg(&[("ZERO_LOG_LEVEL", "verbose")])
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("ZERO_LOG_LEVEL")));
        assert!(cfg(&[("ZERO_LOG_FORMAT", "xml")])
            .startup_errors_with(get_of(&[]))
            .iter()
            .any(|e| e.contains("ZERO_LOG_FORMAT")));
        // Valid combinations are clean.
        let c = cfg(&[("ZERO_LOG_LEVEL", "debug"), ("ZERO_LOG_FORMAT", "json")]);
        assert!(c.startup_errors_with(get_of(&[])).is_empty());
    }
}
