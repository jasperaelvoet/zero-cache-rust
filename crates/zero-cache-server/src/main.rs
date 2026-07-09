//! `zero-cache-server` binary entry point — the thin outer shell that
//! instantiates the shared [`SyncService`] and runs the WebSocket accept loop
//! against a real listener until Ctrl-C.
//!
//! Configuration is read from the environment:
//!   * `ZERO_LISTEN_ADDR` — `host:port` to bind (default `0.0.0.0:4848`);
//!   * `ZERO_FANOUT_CAPACITY` — per-connection commit buffer depth (default 1024).
//!
//! This wires the tested orchestration parts into a running process. The
//! per-connection handler here is a minimal keepalive-only stand-in; a full
//! deployment supplies a handler backed by the live view-syncer/CVR machinery
//! and spawns the supervised replicator loop (which calls
//! `SyncService::publish_commit`) alongside this accept loop, sharing the same
//! `SyncService` handle.

use std::sync::Arc;

use tokio::sync::oneshot;

use zero_cache_server::bootstrap::{bind, live_handler, run_server, ServerConfig};
use zero_cache_server::sync_service::SyncService;
use zero_cache_sqlite::StatementRunner;

fn config_from_env() -> ServerConfig {
    let default = ServerConfig::default();
    ServerConfig {
        listen_addr: std::env::var("ZERO_LISTEN_ADDR").unwrap_or(default.listen_addr),
        fanout_capacity: std::env::var("ZERO_FANOUT_CAPACITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default.fanout_capacity),
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let config = config_from_env();
    let listener = bind(&config).await?;
    let addr = listener.local_addr()?;
    eprintln!("zero-cache-server listening on {addr}");

    let service = Arc::new(SyncService::new(config.fanout_capacity));

    // Shut the accept loop down on Ctrl-C.
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(());
    });

    let accepted = run_server(listener, service, shutdown_rx, |id| {
        // The LIVE view-syncer handler, backed by a per-connection replica.
        // (A production deployment shares a read replica seeded by initial
        // sync; this opens a fresh in-memory replica per connection.)
        let db = StatementRunner::open_in_memory().expect("open replica");
        live_handler(id, db)
    })
    .await;
    eprintln!("zero-cache-server stopped after {accepted} connection(s)");
    Ok(())
}
