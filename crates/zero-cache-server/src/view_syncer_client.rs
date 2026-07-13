//! The view-syncer subscriber — a node that does NOT own the Postgres slot.
//!
//! It connects to a change-streamer ([`crate::change_streamer_server`]),
//! bootstraps its local replica from the snapshot, then applies every streamed
//! commit to that replica and publishes to its own [`SyncService`] fan-out so
//! its connected clients get live pokes. Many such nodes share the single
//! upstream replication slot the change-streamer owns — this is horizontal
//! scaling.
//!
//! Like the replicator, the SQLite writer is `!Sync`, so this runs on a
//! dedicated OS thread with a current-thread runtime (see
//! [`spawn_view_syncer_thread`]).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures_util::StreamExt;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use zero_cache_sqlite::streamed_apply::apply_streamed_commit;
use zero_cache_sqlite::StatementRunner;

use crate::change_streamer_wire::{decode_official_message, OfficialMessage};
use crate::sync_service::SyncService;

#[derive(Debug, thiserror::Error)]
pub enum ViewSyncerError {
    #[error("change-streamer connection failed: {0}")]
    Connect(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("replica error: {0}")]
    Replica(String),
}

type ChangeStream = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

/// One bootstrap attempt: reserve a snapshot on the change-streamer, restore
/// it if the local replica is missing or incompatible, and open the change
/// stream from the replica's watermark.
async fn bootstrap_replica(
    streamer_url: &str,
    replica_path: &str,
) -> Result<(StatementRunner, ChangeStream, String), ViewSyncerError> {
    let snapshot = reserve_snapshot(streamer_url).await?;
    let compatible = StatementRunner::open_file_readonly(replica_path)
        .ok()
        .and_then(|db| {
            zero_cache_sqlite::replication_state::get_subscription_state_and_context(&db).ok()
        })
        .is_some_and(|state| {
            state.replica_version == snapshot.replica_version
                && state.watermark >= snapshot.min_watermark
        });
    if !compatible {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{replica_path}{suffix}"));
        }
        restore_snapshot(&snapshot.backup_url, replica_path).await?;
    }
    let db = StatementRunner::open_file(replica_path)
        .map_err(|e| ViewSyncerError::Replica(e.to_string()))?;
    let state = zero_cache_sqlite::replication_state::get_subscription_state_and_context(&db)
        .map_err(|error| ViewSyncerError::Replica(error.to_string()))?;
    let watermark = state.watermark;
    let streamer_url = official_changes_url(streamer_url, &state.replica_version, &watermark);
    let req = streamer_url
        .into_client_request()
        .map_err(|e| ViewSyncerError::Connect(e.to_string()))?;
    let (ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .map_err(|e| ViewSyncerError::Connect(e.to_string()))?;
    let (_sink, stream) = ws.split();
    Ok((db, stream, watermark))
}

/// Connects to the change-streamer at `streamer_url`, bootstraps the replica at
/// `replica_path` from its snapshot, sets `ready`, then applies streamed commits
/// (publishing each to `service`'s fan-out) until `shutdown`.
pub async fn run_view_syncer(
    streamer_url: String,
    replica_path: String,
    service: Arc<SyncService>,
    shutdown: Arc<AtomicBool>,
    ready: Option<Arc<AtomicBool>>,
) -> Result<(), ViewSyncerError> {
    // Bootstrap retries until it succeeds or shutdown: a change-streamer that
    // is not up yet is the normal case during a rolling deploy, and a
    // permanently-failed bootstrap would never flip readiness (upstream
    // recovers from this by crash-looping the whole process).
    let (db, mut stream, watermark) = {
        let mut attempt: u32 = 0;
        loop {
            match bootstrap_replica(&streamer_url, &replica_path).await {
                Ok(bootstrapped) => break bootstrapped,
                Err(e) => {
                    if shutdown.load(Ordering::SeqCst) {
                        return Err(e);
                    }
                    attempt += 1;
                    crate::warn!(
                        "view-syncer bootstrap attempt {attempt} failed: {e}; retrying in 3s…"
                    );
                    // Back off 3s between attempts, waking early on shutdown so
                    // a stopping process doesn't sit out the rest of the backoff.
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
                    while std::time::Instant::now() < deadline {
                        if shutdown.load(Ordering::SeqCst) {
                            return Err(e);
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }
    };
    crate::info!("serving restored replica and subscribing from watermark {watermark}");
    if let Some(ready) = &ready {
        ready.store(true, Ordering::SeqCst);
    }

    // --- Live: apply each streamed commit + publish to the local fan-out. ---
    let mut pending = Vec::new();
    while !shutdown.load(Ordering::SeqCst) {
        let text = match tokio::time::timeout(std::time::Duration::from_millis(500), stream.next())
            .await
        {
            Ok(Some(Ok(Message::Text(t)))) => t.to_string(),
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => break,
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => return Err(ViewSyncerError::Protocol(e.to_string())),
            Err(_) => continue, // idle tick — re-check shutdown
        };
        match decode_official_message(&text)
            .map_err(|error| ViewSyncerError::Protocol(error.to_string()))?
        {
            OfficialMessage::Status => {}
            OfficialMessage::Begin => pending.clear(),
            OfficialMessage::Data(change) => pending.push(change),
            // `truncate` is APPLIED, not resynced: buffer a `Truncate` per named
            // relation so it clears those tables' rows on `commit` (H6(b)).
            OfficialMessage::Truncate { tables } => {
                for table in tables {
                    pending.push(
                        zero_cache_sqlite::streamed_apply::StreamedChange::Truncate { table },
                    );
                }
            }
            // `rollback` discards the in-flight transaction's buffered changes;
            // the aborted commit never touches the replica. No resync.
            OfficialMessage::Rollback => pending.clear(),
            // Inline DDL is not applied in place (H5): tear the subscriber down
            // with a protocol error so it re-bootstraps from a fresh snapshot
            // (which carries the new schema). Documented resync, not a silent
            // drop.
            OfficialMessage::SchemaChange { tag } => {
                return Err(ViewSyncerError::Protocol(format!(
                    "inline schema change ({tag}) requires a resync"
                )))
            }
            OfficialMessage::Commit { watermark } => {
                let n = pending.len();
                apply_streamed_commit(&db, &watermark, &pending)
                    .map_err(|e| ViewSyncerError::Replica(e.to_string()))?;
                service.publish_commit(watermark, false, n as i64);
                pending.clear();
            }
            OfficialMessage::Error(message) => return Err(ViewSyncerError::Protocol(message)),
        }
    }
    Ok(())
}

fn official_changes_url(base: &str, replica_version: &str, watermark: &str) -> String {
    if base.contains("/replication/v") {
        return base.to_string();
    }
    format!(
        "{}/replication/v6/changes?id=zero-view-syncer&taskID={}&mode=serving&replicaVersion={}&watermark={}&initial=true",
        base.trim_end_matches('/'),
        std::process::id(),
        replica_version,
        watermark,
    )
}

struct SnapshotStatus {
    backup_url: String,
    replica_version: String,
    min_watermark: String,
}

async fn reserve_snapshot(base: &str) -> Result<SnapshotStatus, ViewSyncerError> {
    let url = format!(
        "{}/replication/v6/snapshot?taskID={}",
        base.trim_end_matches('/'),
        std::process::id()
    );
    let (mut ws, _) = tokio_tungstenite::connect_async(url)
        .await
        .map_err(|error| ViewSyncerError::Connect(error.to_string()))?;
    let status = next_text(&mut ws).await?;
    let value: serde_json::Value = serde_json::from_str(&status)
        .map_err(|error| ViewSyncerError::Protocol(error.to_string()))?;
    if value.get(0).and_then(serde_json::Value::as_str) == Some("error") {
        return Err(ViewSyncerError::Protocol(status));
    }
    let field = |name: &str| {
        value
            .get(1)
            .and_then(|value| value.get(name))
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| ViewSyncerError::Protocol(format!("snapshot status missing {name}")))
    };
    Ok(SnapshotStatus {
        backup_url: field("backupURL")?,
        replica_version: field("replicaVersion")?,
        min_watermark: field("minWatermark")?,
    })
}

async fn restore_snapshot(backup_url: &str, replica_path: &str) -> Result<(), ViewSyncerError> {
    let backup_url = backup_url.to_string();
    let replica_path = replica_path.to_string();
    let restored =
        tokio::task::spawn_blocking(move || crate::litestream::restore(&replica_path, &backup_url))
            .await
            .map_err(|error| ViewSyncerError::Replica(error.to_string()))?;
    if !restored {
        return Err(ViewSyncerError::Replica(
            "litestream snapshot restore failed".into(),
        ));
    }
    Ok(())
}

async fn next_text<S>(stream: &mut S) -> Result<String, ViewSyncerError>
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        match stream.next().await {
            Some(Ok(Message::Text(t))) => return Ok(t.to_string()),
            Some(Ok(Message::Binary(_))) => {
                return Err(ViewSyncerError::Protocol(
                    "expected text, got binary".into(),
                ))
            }
            Some(Ok(_)) => continue,
            _ => return Err(ViewSyncerError::Protocol("stream ended".into())),
        }
    }
}

/// Runs [`run_view_syncer`] on a dedicated OS thread with its own current-thread
/// runtime (the SQLite writer is `!Sync`).
pub fn spawn_view_syncer_thread(
    streamer_url: String,
    replica_path: String,
    service: Arc<SyncService>,
    shutdown: Arc<AtomicBool>,
    ready: Option<Arc<AtomicBool>>,
) -> std::thread::JoinHandle<Result<(), ViewSyncerError>> {
    std::thread::Builder::new()
        .name("zero-view-syncer".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| ViewSyncerError::Replica(e.to_string()))?;
            rt.block_on(run_view_syncer(
                streamer_url,
                replica_path,
                service,
                shutdown,
                ready,
            ))
        })
        .expect("spawn view-syncer thread")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A change-streamer that is not up yet (the normal case during a rolling
    /// deploy) must not kill the view-syncer: bootstrap retries until shutdown
    /// instead of dying on the first failed connect, which previously left the
    /// server waiting on readiness forever.
    #[tokio::test]
    async fn bootstrap_retries_until_shutdown_when_streamer_is_unreachable() {
        // Reserve a port with nothing listening on it.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let shutdown = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(false));
        let handle = spawn_view_syncer_thread(
            format!("ws://{addr}"),
            "/nonexistent/replica.db".into(),
            Arc::new(SyncService::new(4)),
            shutdown.clone(),
            Some(ready.clone()),
        );

        // The first connect fails immediately (connection refused on a closed
        // local port); the thread must survive it and sit in its retry backoff.
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert!(
            !handle.is_finished(),
            "a failed bootstrap attempt must retry, not kill the view-syncer"
        );
        assert!(!ready.load(Ordering::SeqCst));

        // The next attempt (after the 3s backoff) observes shutdown and stops.
        shutdown.store(true, Ordering::SeqCst);
        let result = tokio::task::spawn_blocking(move || handle.join().unwrap())
            .await
            .unwrap();
        assert!(
            result.is_err(),
            "shutdown during bootstrap surfaces the last error"
        );
    }
}
