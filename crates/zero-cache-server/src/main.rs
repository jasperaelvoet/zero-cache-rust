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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::oneshot;

use zero_cache_server::bootstrap::{
    bind, live_handler_with_permissions, run_server, run_synced_server, ServerConfig,
};
use zero_cache_server::config::ZeroConfig;
use zero_cache_server::replicator_service::{spawn_replicator_thread, ReplicatorConfig};
use zero_cache_server::sync_service::SyncService;
use zero_cache_sqlite::StatementRunner;

use zero_cache_server::{error, info, warn};

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

async fn async_main(cfg: ZeroConfig) -> std::io::Result<()> {
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
    if let Some(t) = &cfg.task_id {
        info!("task {t}");
    }
    // Transparency: warn about recognized-but-unimplemented settings that are set.
    let ignored = cfg.unimplemented_but_set();
    if !ignored.is_empty() {
        warn!(
            "these set env vars are recognized but not yet honored: {}",
            ignored.join(", ")
        );
    }

    let server_config = ServerConfig {
        listen_addr: cfg.listen_addr.clone(),
        fanout_capacity: cfg.fanout_capacity,
    };
    let listener = bind(&server_config).await?;
    let addr = listener.local_addr()?;

    // Explicit metrics backend so the ops endpoint can render it.
    let metrics_backend = Arc::new(zero_cache_services::metrics::InMemoryBackend::new());
    let metrics = Arc::new(zero_cache_services::metrics::Metrics::new(
        metrics_backend.clone(),
    ));
    let service = Arc::new(SyncService::with_metrics(cfg.fanout_capacity, metrics));
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
    let synced = if let Some(uri) = cfg.change_streamer_uri.clone() {
        // VIEW-SYNCER: no Postgres slot; bootstrap + follow the change-streamer.
        info!("VIEW-SYNCER mode — subscribing to change-streamer {uri}");
        zero_cache_server::view_syncer_client::spawn_view_syncer_thread(
            uri,
            cfg.replica_file.clone(),
            service.clone(),
            shutdown_flag.clone(),
            Some(ready.clone()),
        );
        while !ready.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        info!("replica bootstrapped; serving from {}", cfg.replica_file);
        true
    } else if let Some(upstream) = cfg.upstream_db.clone() {
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
        let repl_cfg = ReplicatorConfig::from_upstream(
            &upstream,
            cfg.replica_file.clone(),
            cfg.app_id.clone(),
            cfg.shard_num,
            cfg.app_publications.clone(),
        );
        info!("REPLICATOR mode — starting replicator (initial sync)…");
        spawn_replicator_thread(
            repl_cfg,
            service.clone(),
            shutdown_flag.clone(),
            Some(ready.clone()),
        );
        while !ready.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
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
        if let Ok(cs_listener) = tokio::net::TcpListener::bind(&cfg.change_streamer_addr).await {
            let (cs_tx, cs_rx) = oneshot::channel();
            change_streamer_shutdown = Some(cs_tx);
            let svc = service.clone();
            let replica = cfg.replica_file.clone();
            tokio::spawn(
                zero_cache_server::change_streamer_server::run_change_streamer(
                    cs_listener,
                    svc,
                    replica,
                    cs_rx,
                ),
            );
            info!(
                "change-streamer serving view-syncers on {}",
                cfg.change_streamer_addr
            );
        } else {
            warn!(
                "could not bind change-streamer on {}",
                cfg.change_streamer_addr
            );
        }
        true
    } else {
        info!("no ZERO_UPSTREAM_DB — standalone (in-memory) mode");
        ready.store(true, Ordering::SeqCst);
        false
    };

    // Ops endpoint: Prometheus /metrics + /healthz + /readyz.
    let (metrics_shutdown_tx, metrics_shutdown_rx) = oneshot::channel();
    {
        let backend = metrics_backend.clone();
        let ready = ready.clone();
        let metrics_addr = cfg.metrics_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = zero_cache_server::metrics_server::run_metrics_server(
                &metrics_addr,
                backend,
                ready,
                metrics_shutdown_rx,
            )
            .await
            {
                error!("metrics endpoint error: {e}");
            }
        });
    }
    info!(
        "ops endpoint on {} (/metrics /healthz /readyz)",
        cfg.metrics_addr
    );

    match &cfg.auth_secret {
        Some(_) => info!("auth ENABLED (HS256 JWT required)"),
        None => info!("auth DISABLED (set ZERO_AUTH_SECRET to require JWTs)"),
    }
    if let Some(max) = cfg.max_connections {
        info!("max connections = {max}");
    }
    info!("log level {} format {}", cfg.log_level, cfg.log_format);

    info!("listening on {addr}");

    // Shut everything down on Ctrl-C.
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_signal = shutdown_flag.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown_signal.store(true, Ordering::SeqCst);
        let _ = shutdown_tx.send(());
        let _ = metrics_shutdown_tx.send(());
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
        let _ = shutdown_rx.await; // run until shutdown; replicator + streamer keep going
        0
    } else if synced {
        // CRUD pushes route to upstream Postgres (unless disabled); custom
        // mutators route to ZERO_MUTATE_URL; custom queries to ZERO_QUERY_URL.
        let upstream_push = if cfg.enable_crud_mutations {
            cfg.upstream_db
                .clone()
                .map(|conn| (conn, cfg.upstream_schema.clone()))
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
            .unwrap_or_else(|_| cfg.upstream_schema.clone());
        let deps = zero_cache_server::bootstrap::HandlerDeps {
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
            mutate_allowed_client_headers: cfg.mutate_allowed_client_headers.clone(),
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
    } else {
        let permissions = compiled_permissions.clone();
        run_server(listener, service, shutdown_rx, move |id| {
            let db = StatementRunner::open_in_memory().expect("open replica");
            live_handler_with_permissions(id, db, permissions.as_deref().cloned())
        })
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
