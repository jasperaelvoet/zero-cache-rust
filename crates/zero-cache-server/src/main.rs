//! `zero-cache-server` binary entry point.
//!
//! Configuration is read from `ZERO_*` environment variables matching upstream
//! zero-cache (see `config::ZeroConfig`). Two modes, chosen by whether
//! `ZERO_UPSTREAM_DB` is set:
//!
//! * **Synced mode** (`ZERO_UPSTREAM_DB` set): spawn the replicator (initial
//!   sync of upstream Postgres into a durable WAL replica, then ongoing apply +
//!   fan-out), wait for readiness, then serve each connection from the shared
//!   replica with live pokes; pushes route to upstream Postgres.
//! * **Standalone mode** (no upstream): protocol only (handshake / ping).

// Global allocator: jemalloc. The deploy image is Alpine/musl, and musl's
// built-in allocator is simple, slow under multi-threaded contention, and
// conservative about returning freed memory. After the initial Postgres→SQLite
// sync allocates large transient buffers across the thread pool, an
// otherwise-idle node would keep a big resident heap. jemalloc bounds
// fragmentation, materially improves multi-threaded allocation throughput on
// musl (the hot hydration path), and its decay settings return idle pages to
// the OS (configured via `_RJEM_MALLOC_CONF` in the Dockerfile). Excluded only
// on MSVC, where jemalloc-sys does not build.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;

use zero_cache_server::bootstrap::{bind, run_synced_server, ServerConfig};
use zero_cache_server::config::ZeroConfig;
use zero_cache_server::replicator_service::{spawn_replicator_thread, ReplicatorConfig};
use zero_cache_server::service_lifecycle::{
    exit_process_when_thread_dies, wait_for_ready, ReadyWait,
};
use zero_cache_server::sync_service::SyncService;

use zero_cache_server::{error, info, warn};

/// Upstream lifecycle (`run-worker.ts` + `ProcessManager`): startup blocks
/// until the service thread signals ready. A thread that dies first aborts
/// the process with its error logged — previously that error was silently
/// dropped and the server waited forever without ever serving HTTP, so a
/// failed deploy showed up only as an endless 503 at the load balancer.
async fn wait_until_ready<T, E: std::fmt::Display>(
    name: &str,
    ready: &AtomicBool,
    shutdown: &AtomicBool,
    handle: std::thread::JoinHandle<Result<T, E>>,
) -> std::thread::JoinHandle<Result<T, E>> {
    match wait_for_ready(name, ready, shutdown, handle).await {
        ReadyWait::Ready(handle) => handle,
        ReadyWait::Shutdown => {
            info!("shutdown requested before {name} was ready; exiting");
            std::process::exit(0);
        }
        ReadyWait::Died(message) => {
            error!("{message}");
            std::process::exit(-1);
        }
    }
}

/// Raise the soft open-file-descriptor limit (`RLIMIT_NOFILE`) to the hard cap.
/// The view-syncer opens several SQLite (wal2) fds per client-group pipeline;
/// on macOS the default soft limit is 256, which a busy local/dev run can
/// exhaust — surfacing as `SQLITE_CANTOPEN` ("unable to open database file").
/// This is a safety net (the real bound is the shared-connection pipeline) and
/// is harmless in production, where Linux hard limits are high. Best-effort:
/// any failure is logged, never fatal.
#[cfg(unix)]
fn raise_fd_limit() {
    // SAFETY: plain libc get/setrlimit over a stack-local `rlimit`.
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            return;
        }
        let current = lim.rlim_cur;
        // Darwin's hard cap is often RLIM_INFINITY, but the kernel rejects a
        // soft limit above OPEN_MAX — clamp so setrlimit doesn't EINVAL.
        #[cfg(target_os = "macos")]
        let target = {
            const OPEN_MAX: libc::rlim_t = 24 * 1024;
            if lim.rlim_max == libc::RLIM_INFINITY || lim.rlim_max > OPEN_MAX {
                OPEN_MAX
            } else {
                lim.rlim_max
            }
        };
        #[cfg(not(target_os = "macos"))]
        let target = lim.rlim_max;
        if target <= current {
            return;
        }
        lim.rlim_cur = target;
        if libc::setrlimit(libc::RLIMIT_NOFILE, &lim) == 0 {
            info!("raised open-file limit {current} -> {target}");
        } else {
            warn!("could not raise open-file limit (currently {current})");
        }
    }
}

#[cfg(not(unix))]
fn raise_fd_limit() {}

fn main() -> std::io::Result<()> {
    let cfg = ZeroConfig::from_env();
    // Structured, levelled logging (ZERO_LOG_LEVEL / ZERO_LOG_FORMAT). The
    // worker role names the JSON logs; determined from the node's mode.
    let worker = if cfg.change_streamer_uri.is_some() {
        "view-syncer"
    } else if cfg.upstream_db.is_some() {
        "replicator"
    } else {
        "standalone"
    };
    zero_cache_server::logging::init(&cfg.log_level, &cfg.log_format, worker);
    // Give the process fd headroom before the replication/view-syncer stack
    // starts opening SQLite connections (safety net for the shared-connection
    // pipeline; see raise_fd_limit).
    raise_fd_limit();
    zero_cache_server::logging::init_observability(zero_cache_server::logging::Observability {
        slow_hydrate_ms: cfg.log_slow_hydrate_threshold_ms,
        slow_row_threshold: cfg.log_slow_row_threshold,
        ivm_sampling: cfg.log_ivm_sampling,
        replication_reports_at_debug: cfg.log_all_replication_reports_at_debug,
    });
    zero_cache_server::litestream::configure(cfg.litestream_log_level.clone());
    // ZERO_NUM_SYNC_WORKERS>0 sets the tokio worker-thread count (vertical
    // multi-core); default is all cores.
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(n) = cfg.num_sync_workers {
        if n > 0 {
            builder.worker_threads(n);
            info!("{n} sync worker thread(s)");
        }
    }
    let rt = builder.build()?;
    rt.block_on(async_main(cfg))
}

/// Official replication-path options extracted from the parsed config.
fn replicator_tuning(cfg: &ZeroConfig) -> zero_cache_server::replicator_service::ReplicatorTuning {
    zero_cache_server::replicator_service::ReplicatorTuning {
        pg_replication_slot_failover: cfg.pg_replication_slot_failover,
        replica_vacuum_interval_hours: cfg.replica_vacuum_interval_hours,
        initial_sync_table_copy_workers: cfg.initial_sync_table_copy_workers,
        initial_sync_text_copy: cfg.initial_sync_text_copy,
        initial_sync_profile_copy: cfg.initial_sync_profile_copy,
        replication_lag_report_interval_ms: cfg.replication_lag_report_interval_ms,
        change_streamer_startup_delay_ms: cfg.change_streamer_startup_delay_ms,
        // Dedicated change-streamer nodes serve remote view-syncers; the
        // takeover grace period applies there. Single-node deployments have
        // an in-process subscriber, equivalent to upstream's immediate
        // request-cancel.
        apply_startup_delay: cfg.num_sync_workers == Some(0),
    }
}

async fn async_main(mut cfg: ZeroConfig) -> std::io::Result<()> {
    const FANOUT_CAPACITY: usize = 1024;
    // Compile permissions once at startup. A set but malformed schema is a
    // configuration error, not an excuse to start an unrestricted server.
    let compiled_permissions = match cfg.schema_json.as_deref() {
        Some(schema_json) => {
            let permissions =
                zero_cache_auth::compiled_permissions::parse_compiled_permissions_json(schema_json)
                    .map_err(|error| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("invalid ZERO_SCHEMA_JSON permissions: {error}"),
                        )
                    })?;
            info!("compiled permissions loaded from ZERO_SCHEMA_JSON");
            Some(Arc::new(permissions))
        }
        None => None,
    };

    if let Some(v) = &cfg.server_version {
        info!("version {v}");
    }
    // Upstream parse-time asserts + startup checks (removed options, invalid
    // values, conflicting combinations, insufficient pool bounds).
    let config_errors = cfg.startup_errors();
    if !config_errors.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            config_errors.join("; "),
        ));
    }
    for warning in zero_cache_server::config::ZeroConfig::deprecation_warnings() {
        warn!("{warning}");
    }

    // Task identity: explicit ZERO_TASK_ID, else the ECS TaskARN suffix, else
    // a random id (upstream getTaskID + normalize defaultTaskID).
    let task_id = match cfg.task_id.clone() {
        Some(t) => t,
        None => zero_cache_server::task_identity::resolve_task_id().await,
    };
    cfg.task_id = Some(task_id.clone());
    info!("task {task_id}");

    // Telemetry contract: upstream phones anonymous usage metrics home to
    // Rocicorp when enabled. This independent server NEVER phones home —
    // polluting the official fleet's anonymous dataset would be worse than
    // useless — so "enabled" means the same zero.* usage counters are kept
    // locally on the metrics endpoint, and "disabled" (or DO_NOT_TRACK)
    // matches upstream's opt-out exactly.
    if cfg.enable_telemetry {
        info!("telemetry: local-only anonymous usage counters (this server never phones home)");
    } else {
        info!("telemetry: disabled");
    }

    // CloudEvents sink (knative K_SINK-style env indirection). A configured
    // sink var that is missing from the environment is a startup error, as
    // upstream's `must()`.
    if let Err(e) = zero_cache_server::zero_events::init(
        cfg.cloud_event_sink_env.as_deref(),
        cfg.cloud_event_extension_overrides_env.as_deref(),
        &task_id,
    ) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            e.to_string(),
        ));
    }

    // Query-engine flags consulted from the hydration/advancement paths.
    zero_cache_server::query_engine_options::init(
        zero_cache_server::query_engine_options::QueryEngineOptions {
            enable_planner: cfg.enable_query_planner,
            enable_covering: cfg.enable_query_covering,
            hydration_stats: cfg.query_hydration_stats,
            yield_threshold_ms: cfg.yield_threshold_ms,
            auth_revalidate_interval_seconds: cfg.auth_revalidate_interval_seconds,
            auth_retransform_interval_seconds: cfg.auth_retransform_interval_seconds,
        },
    );

    // ZERO_PER_USER_MUTATION_LIMIT_*: per-client-group sliding-window CRUD
    // rate limiting (unset max = unlimited, upstream default).
    zero_cache_server::mutation_rate_limit::init(
        cfg.per_user_mutation_limit_max,
        cfg.per_user_mutation_limit_window_ms,
    );
    // ZERO_UPSTREAM_MAX_CONNS: bound on concurrently-open upstream mutation
    // connections (the hidden per-worker override wins when set).
    zero_cache_server::upstream_conn_limit::init(cfg.effective_upstream_max_conns());

    if cfg.websocket_compression {
        // Options are parsed and validated (startup_errors); tungstenite does
        // not implement RFC 7692 permessage-deflate, and the extension is
        // negotiated — a server that declines it interoperates identically,
        // just without the bandwidth savings. Say so rather than fail.
        warn!(
            "ZERO_WEBSOCKET_COMPRESSION enabled: this build does not negotiate \
             permessage-deflate; connections proceed uncompressed"
        );
    }

    let server_config = ServerConfig {
        listen_addr: cfg.listen_addr.clone(),
        fanout_capacity: FANOUT_CAPACITY,
    };
    let service = Arc::new(SyncService::new(FANOUT_CAPACITY));
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let ready = Arc::new(AtomicBool::new(false));

    // Node role: view-syncer (subscribes to a change-streamer), replicator/
    // change-streamer (owns the PG slot + serves other view-syncers), or
    // standalone (in-memory, protocol only).
    let mut change_streamer_shutdown: Option<oneshot::Sender<()>> = None;
    let mut litestream_child: Option<std::process::Child> = None;
    // Provision the shared CVR schema before selecting the node role. A
    // view-syncer also loads and flushes CVRs, so restricting this to the
    // replicator branch leaves fresh view-syncer deployments unable to start.
    if let Some(cvr_db) = cfg.cvr_db.clone() {
        let shard = zero_cache_types::shards::ShardId {
            app_id: cfg.app_id.clone(),
            shard_num: cfg.shard_num,
        };
        match zero_cache_server::cvr_provision::provision_cvr_schema(&cvr_db, &shard).await {
            Ok(true) => info!("CVR schema provisioned in ZERO_CVR_DB"),
            Ok(false) => info!("CVR schema already present in ZERO_CVR_DB"),
            Err(e) => warn!("CVR DB provisioning failed: {e}"),
        }
    }
    // ZERO_CHANGE_STREAMER_MODE=discover: resolve the change-streamer URI
    // from the address the replication-manager registered in the change DB
    // ("ChangeDB as DNS"). An explicit URI always wins; `dedicated` (default)
    // means this node runs its own change-streamer when it has an upstream.
    if cfg.change_streamer_uri.is_none() && cfg.change_streamer_mode == "discover" {
        let change_db = cfg.change_db.clone().or_else(|| cfg.upstream_db.clone());
        if let Some(change_db) = change_db {
            let shard = zero_cache_types::shards::ShardId {
                app_id: cfg.app_id.clone(),
                shard_num: cfg.shard_num,
            };
            let mut delay = std::time::Duration::from_millis(250);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                match zero_cache_server::change_streamer_discovery::discover_address(
                    &change_db, &shard,
                )
                .await
                {
                    Ok(Some(address)) => {
                        let uri =
                            zero_cache_server::change_streamer_discovery::discovered_url(&address);
                        info!("discovered change-streamer at {uri}");
                        cfg.change_streamer_uri = Some(uri);
                        break;
                    }
                    Ok(None) => warn!("no change-streamer is running (nothing registered)"),
                    Err(e) => warn!("change-streamer discovery failed: {e}"),
                }
                if std::time::Instant::now() >= deadline {
                    return Err(std::io::Error::other(
                        "no change-streamer is running (discovery timed out)",
                    ));
                }
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, std::time::Duration::from_secs(5));
            }
        }
    }

    let synced = if let Some(uri) = cfg.change_streamer_uri.clone() {
        // VIEW-SYNCER: no Postgres slot; bootstrap + follow the change-streamer.
        info!("VIEW-SYNCER mode — subscribing to change-streamer {uri}");
        let handle = zero_cache_server::view_syncer_client::spawn_view_syncer_thread(
            uri,
            cfg.replica_file.clone(),
            service.clone(),
            shutdown_flag.clone(),
            Some(ready.clone()),
        );
        let handle = wait_until_ready("view-syncer", &ready, &shutdown_flag, handle).await;
        exit_process_when_thread_dies("view-syncer", shutdown_flag.clone(), handle);
        info!("replica bootstrapped; serving from {}", cfg.replica_file);
        true
    } else if let Some(upstream) = cfg.upstream_db.clone() {
        if cfg.lazy_startup && cfg.num_sync_workers != Some(0) {
            // LAZY STARTUP (single-node): defer the whole replication stack —
            // litestream restore, initial sync, replication stream, backup,
            // change-streamer — until the first sync request fires the
            // trigger (health/admin routes do not trigger, as upstream's
            // dispatcher). The listener binds immediately below.
            info!("lazy startup — replication deferred until the first sync request");
            let trigger = zero_cache_server::lazy_start::arm(ready.clone());
            let lazy_cfg = cfg.clone();
            let lazy_service = service.clone();
            let lazy_shutdown = shutdown_flag.clone();
            let lazy_ready = ready.clone();
            tokio::spawn(async move {
                trigger.triggered().await;
                if let Some(backup) = lazy_cfg.litestream_backup_url.clone() {
                    let replica = lazy_cfg.replica_file.clone();
                    let restored = tokio::task::spawn_blocking(move || {
                        zero_cache_server::litestream::restore(&replica, &backup)
                    })
                    .await
                    .unwrap_or(false);
                    if restored {
                        info!("litestream — replica present ({})", lazy_cfg.replica_file);
                    }
                }
                let mut repl_cfg = ReplicatorConfig::from_upstream(
                    &upstream,
                    lazy_cfg.replica_file.clone(),
                    lazy_cfg.app_id.clone(),
                    lazy_cfg.shard_num,
                    lazy_cfg.app_publications.clone(),
                );
                repl_cfg.tuning = replicator_tuning(&lazy_cfg);
                info!("REPLICATOR mode — starting replicator (initial sync)…");
                let handle = spawn_replicator_thread(
                    repl_cfg,
                    lazy_service.clone(),
                    lazy_shutdown.clone(),
                    Some(lazy_ready.clone()),
                );
                let handle =
                    wait_until_ready("replicator", &lazy_ready, &lazy_shutdown, handle).await;
                exit_process_when_thread_dies("replicator", lazy_shutdown.clone(), handle);
                info!(
                    "initial sync complete; serving from {}",
                    lazy_cfg.replica_file
                );
                if let Some(backup) = lazy_cfg.litestream_backup_url.clone() {
                    // The child is intentionally left to die with the process
                    // (containers reap it); lazy mode has no eager shutdown
                    // hook to thread it through.
                    match zero_cache_server::litestream::spawn_replicate(
                        &lazy_cfg.replica_file,
                        &backup,
                    ) {
                        Ok(_child) => info!("litestream — backing up to {backup}"),
                        Err(e) => warn!("litestream — could not start backup: {e}"),
                    }
                }
                if let Ok(cs_listener) =
                    tokio::net::TcpListener::bind(&lazy_cfg.change_streamer_addr).await
                {
                    let (cs_tx, cs_rx) = oneshot::channel();
                    let flag = lazy_shutdown.clone();
                    tokio::spawn(async move {
                        while !flag.load(Ordering::SeqCst) {
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        }
                        let _ = cs_tx.send(());
                    });
                    tokio::spawn(
                        zero_cache_server::change_streamer_server::run_change_streamer(
                            cs_listener,
                            lazy_service.clone(),
                            lazy_cfg.replica_file.clone(),
                            lazy_cfg.litestream_backup_url.clone(),
                            lazy_cfg.keepalive_timeout_ms,
                            cs_rx,
                        ),
                    );
                    info!(
                        "change-streamer serving view-syncers on {}",
                        lazy_cfg.change_streamer_addr
                    );
                }
            });
            true
        } else {
            // REPLICATOR / CHANGE-STREAMER: own the slot, serve view-syncers.
            // Litestream restore: warm the replica from the object-store backup
            // before initial sync (skips a cold re-sync if a backup exists).
            if let Some(backup) = cfg.litestream_backup_url.clone() {
                info!("litestream — attempting restore from {backup}");
                if zero_cache_server::litestream::restore(&cfg.replica_file, &backup) {
                    info!("litestream — replica present ({})", cfg.replica_file);
                } else {
                    info!("litestream — no backup restored; will initial-sync");
                }
            }
            let mut repl_cfg = ReplicatorConfig::from_upstream(
                &upstream,
                cfg.replica_file.clone(),
                cfg.app_id.clone(),
                cfg.shard_num,
                cfg.app_publications.clone(),
            );
            repl_cfg.tuning = replicator_tuning(&cfg);
            info!("REPLICATOR mode — starting replicator (initial sync)…");
            let handle = spawn_replicator_thread(
                repl_cfg,
                service.clone(),
                shutdown_flag.clone(),
                Some(ready.clone()),
            );
            let handle = wait_until_ready("replicator", &ready, &shutdown_flag, handle).await;
            exit_process_when_thread_dies("replicator", shutdown_flag.clone(), handle);
            info!("initial sync complete; serving from {}", cfg.replica_file);
            // Litestream: start continuous backup of the live replica to the
            // object store (disaster recovery). Killed on shutdown.
            if let Some(backup) = cfg.litestream_backup_url.clone() {
                match zero_cache_server::litestream::spawn_replicate(&cfg.replica_file, &backup) {
                    Ok(child) => {
                        litestream_child = Some(child);
                        info!("litestream — backing up to {backup}");
                    }
                    Err(e) => warn!("litestream — could not start backup: {e}"),
                }
            }
            // Expose the change-streamer so view-syncer nodes can subscribe.
            if let Ok(cs_listener) = tokio::net::TcpListener::bind(&cfg.change_streamer_addr).await
            {
                let (cs_tx, cs_rx) = oneshot::channel();
                change_streamer_shutdown = Some(cs_tx);
                let svc = service.clone();
                let replica = cfg.replica_file.clone();
                tokio::spawn(
                    zero_cache_server::change_streamer_server::run_change_streamer(
                        cs_listener,
                        svc,
                        replica,
                        cfg.litestream_backup_url.clone(),
                        cfg.keepalive_timeout_ms,
                        cs_rx,
                    ),
                );
                info!(
                    "change-streamer serving view-syncers on {}",
                    cfg.change_streamer_addr
                );
                // Register this node's externally-reachable address in the change
                // DB so discover-mode view-syncers can find it (upstream
                // assumeOwnership writes owner + ownerAddress).
                let change_db = cfg.change_db.clone().or_else(|| cfg.upstream_db.clone());
                if let Some(change_db) = change_db {
                    let shard = zero_cache_types::shards::ShardId {
                        app_id: cfg.app_id.clone(),
                        shard_num: cfg.shard_num,
                    };
                    let cs_port = cfg
                        .change_streamer_addr
                        .rsplit(':')
                        .next()
                        .unwrap_or("4849")
                        .to_string();
                    let host_ip = zero_cache_server::change_streamer_discovery::pick_host_ip(
                        &cfg.discovery_interface_preferences,
                    )
                    .unwrap_or_else(|| "127.0.0.1".to_string());
                    let owner_address =
                        zero_cache_server::change_streamer_discovery::address_with_protocol(
                            &cfg.change_streamer_protocol,
                            &format!("{host_ip}:{cs_port}"),
                        );
                    let owner = task_id.clone();
                    tokio::spawn(async move {
                        match zero_cache_server::change_streamer_discovery::register_owner(
                            &change_db,
                            &shard,
                            &owner,
                            &owner_address,
                        )
                        .await
                        {
                            Ok(()) => info!("registered change-streamer address {owner_address}"),
                            Err(e) => warn!("change-streamer address registration failed: {e}"),
                        }
                    });
                }
            } else {
                warn!(
                    "could not bind change-streamer on {}",
                    cfg.change_streamer_addr
                );
            }
            true
        }
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Zero v1.7 requires ZERO_UPSTREAM_DB or ZERO_CHANGE_STREAMER_URI",
        ));
    };

    // CVR garbage collection (upstream's reaper worker): runs on the node
    // that owns the change-streamer (single-node / replication-manager), so a
    // fleet doesn't run one purger per view-syncer. Batch size 0 disables.
    if synced && cfg.change_streamer_uri.is_none() {
        if let Some(cvr_db) = cfg.cvr_db.clone() {
            let gc = zero_cache_server::cvr_purger::CvrPurgerConfig::from_options(
                cfg.cvr_gc_inactivity_threshold_hours,
                cfg.cvr_gc_initial_interval_seconds,
                cfg.cvr_gc_initial_batch_size,
            );
            let shard = zero_cache_types::shards::ShardId {
                app_id: cfg.app_id.clone(),
                shard_num: cfg.shard_num,
            };
            tokio::spawn(zero_cache_server::cvr_purger::run_cvr_purger(
                cvr_db,
                shard,
                gc,
                shutdown_flag.clone(),
            ));
            info!(
                "CVR GC scheduled (threshold {}h, interval {}s, batch {})",
                cfg.cvr_gc_inactivity_threshold_hours,
                cfg.cvr_gc_initial_interval_seconds,
                cfg.cvr_gc_initial_batch_size
            );
        }
    }

    // ZERO_SHADOW_SYNC_ENABLED: periodic canary initial-sync into a throwaway
    // replica, on the change-streamer node only (upstream gates on
    // runChangeStreamer). A failure logs but never crashes.
    if cfg.shadow_sync_enabled && synced && cfg.change_streamer_uri.is_none() {
        if let Some(upstream) = cfg.upstream_db.clone() {
            let shadow_cfg = zero_cache_server::shadow_sync_canary::ShadowSyncConfig {
                interval_hours: cfg.shadow_sync_interval_hours,
                sample_rate: cfg.shadow_sync_sample_rate,
                max_rows_per_table: cfg.shadow_sync_max_rows_per_table,
                storage_tmp_dir: cfg.storage_db_tmp_dir.clone(),
            };
            // Per-process jitter (the runtime forbids ambient entropy): derive
            // a stable [0,1) from the task id so a fleet doesn't canary in
            // lockstep, without Math.random.
            let jitter = {
                let sum: u32 = task_id.bytes().map(|b| b as u32).sum();
                (sum % 1000) as f64 / 1000.0
            };
            // The canary builds a throwaway SQLite replica; its writer handle
            // is !Send, so — like the main replicator — it runs on a
            // dedicated OS thread with its own current-thread runtime rather
            // than on the shared multi-threaded runtime.
            let publications = cfg.app_publications.clone();
            let shadow_shutdown = shutdown_flag.clone();
            std::thread::Builder::new()
                .name("shadow-sync-canary".into())
                .spawn(move || {
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            eprintln!("shadow-sync canary runtime failed: {e}");
                            return;
                        }
                    };
                    rt.block_on(zero_cache_server::shadow_sync_canary::run_shadow_sync(
                        upstream,
                        publications,
                        shadow_cfg,
                        jitter,
                        shadow_shutdown,
                    ));
                })
                .ok();
            info!(
                "shadow-sync canary enabled (every {}h, sample {}, cap {})",
                cfg.shadow_sync_interval_hours,
                cfg.shadow_sync_sample_rate,
                cfg.shadow_sync_max_rows_per_table
            );
        }
    }

    match &cfg.auth_secret {
        Some(_) => info!("auth ENABLED (HS256 JWT required)"),
        None => info!("auth DISABLED (set ZERO_AUTH_SECRET to require JWTs)"),
    }
    info!("log level {} format {}", cfg.log_level, cfg.log_format);

    // Upstream's runner starts listening only after its workers signal ready
    // (`run-worker.ts`: dispatcher listens after `allWorkersReady()`). Binding
    // any earlier queues health checks in the accept backlog with no response
    // for the whole of initial sync; refused connections are the readiness
    // signal load balancers expect.
    let listener = bind(&server_config).await?;
    let addr = listener.local_addr()?;
    info!("listening on {addr}");

    // Shut everything down on Ctrl-C.
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_signal = shutdown_flag.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_signal.store(true, Ordering::SeqCst);
        let _ = shutdown_tx.send(());
        if let Some(cs) = change_streamer_shutdown {
            let _ = cs.send(());
        }
    });

    // A replicator node with NUM_SYNC_WORKERS=0 is a DEDICATED change-streamer:
    // it owns the slot + streams to view-syncers, but does not serve clients.
    let dedicated_change_streamer =
        synced && cfg.change_streamer_uri.is_none() && cfg.num_sync_workers == Some(0);

    let accepted = if dedicated_change_streamer {
        info!(
            "dedicated change-streamer (ZERO_NUM_SYNC_WORKERS=0) — \
             streaming to view-syncers, not serving clients"
        );
        // Health/admin routes stay up on the public port (upstream's runner
        // serves them on replication-manager nodes too); sync is refused.
        zero_cache_server::bootstrap::run_health_endpoint_server(
            listener,
            shutdown_rx,
            zero_cache_server::public_http::PublicEndpointConfig::new(
                cfg.admin_password.clone(),
                std::env::var("NODE_ENV").as_deref() == Ok("development"),
                cfg.keepalive_timeout_ms,
            ),
            cfg.replica_file.clone(),
        )
        .await;
        0
    } else {
        // CRUD pushes route to upstream Postgres (unless disabled); custom
        // mutators route to ZERO_MUTATE_URL; custom queries to ZERO_QUERY_URL.
        let upstream_push = if cfg.enable_crud_mutations {
            cfg.upstream_db.clone().map(|conn| (conn, "public".into()))
        } else {
            None
        };
        // The `schema` query param sent to the mutate/query API servers is the
        // ZERO SHARD schema (`<appID>_<shardNum>`, where zero's clients/mutations
        // bookkeeping tables live) — NOT the upstream DATA schema (`public`).
        // The mutate server's PushProcessor uses it as `upstreamSchema` to record
        // `lastMutationID` into `<schema>.clients`; sending `public` makes it
        // write to a nonexistent `public.clients` and every mutation fails.
        // (Upstream: `custom/fetch.ts` `params.append('schema', upstreamSchema(shard))`.)
        let shard_schema =
            zero_cache_types::shards::upstream_schema(&zero_cache_types::shards::ShardId {
                app_id: cfg.app_id.clone(),
                shard_num: cfg.shard_num,
            })
            .unwrap_or_else(|_| "public".into());
        let deps = zero_cache_server::bootstrap::HandlerDeps {
            public_endpoint: Some(zero_cache_server::public_http::PublicEndpointConfig::new(
                cfg.admin_password.clone(),
                std::env::var("NODE_ENV").as_deref() == Ok("development"),
                cfg.keepalive_timeout_ms,
            )),
            cvr: cfg.cvr_db.clone().map(|connection_string| {
                zero_cache_server::bootstrap::CvrRuntimeConfig {
                    connection_string,
                    max_connections: cfg.cvr_max_conns,
                    shard: zero_cache_types::shards::ShardId {
                        app_id: cfg.app_id.clone(),
                        shard_num: cfg.shard_num,
                    },
                    task_id: cfg
                        .task_id
                        .clone()
                        .unwrap_or_else(|| format!("zero-{}", std::process::id())),
                }
            }),
            permissions: compiled_permissions.clone(),
            upstream_push,
            mutate_api: cfg.mutate_url.clone().map(|url| {
                (
                    url,
                    cfg.mutate_api_key.clone(),
                    shard_schema.clone(),
                    cfg.app_id.clone(),
                )
            }),
            query_api: cfg.query_url.clone().map(|url| {
                (
                    url,
                    cfg.query_api_key.clone(),
                    shard_schema.clone(),
                    cfg.app_id.clone(),
                )
            }),
            query_forward_cookies: cfg.query_forward_cookies,
            mutate_forward_cookies: cfg.mutate_forward_cookies,
            query_allowed_client_headers: cfg.query_allowed_client_headers.clone(),
            query_allowed_request_headers: cfg.query_allowed_request_headers.clone(),
            mutate_allowed_client_headers: cfg.mutate_allowed_client_headers.clone(),
            mutate_allowed_request_headers: cfg.mutate_allowed_request_headers.clone(),
            // The binary keeps the env-variable behavior (`ZERO_GROUP_OWNERSHIP`).
            group_ownership: None,
        };
        if cfg.query_forward_cookies || cfg.mutate_forward_cookies {
            info!(
                "forwarding session cookies to app API servers (query={}, mutate={})",
                cfg.query_forward_cookies, cfg.mutate_forward_cookies
            );
        }
        if deps.mutate_api.is_some() {
            info!("custom mutators -> {}", cfg.mutate_url.as_deref().unwrap());
        }
        if deps.query_api.is_some() {
            info!("custom queries -> {}", cfg.query_url.as_deref().unwrap());
        }
        run_synced_server(
            listener,
            service,
            shutdown_rx,
            cfg.replica_file.clone(),
            deps,
        )
        .await
    };
    // Stop the litestream backup process (best-effort; it flushes on SIGTERM).
    if let Some(mut child) = litestream_child {
        let _ = child.kill();
        let _ = child.wait();
        info!("litestream backup stopped");
    }
    info!("stopped after {accepted} connection(s)");
    Ok(())
}
