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

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use zero_cache_sqlite::streamed_apply::apply_streamed_commit;
use zero_cache_sqlite::StatementRunner;

use crate::change_streamer_wire::{decode_streamer_message, encode_subscribe, StreamerMessage};
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
    let req = streamer_url
        .into_client_request()
        .map_err(|e| ViewSyncerError::Connect(e.to_string()))?;
    let (ws, _) = tokio_tungstenite::connect_async(req)
        .await
        .map_err(|e| ViewSyncerError::Connect(e.to_string()))?;
    let (mut sink, mut stream) = ws.split();

    // Fresh bootstrap each start (since = ""): request the full snapshot.
    sink.send(Message::text(encode_subscribe("")))
        .await
        .map_err(|e| ViewSyncerError::Connect(e.to_string()))?;

    // --- Snapshot: header, then `bytes` of binary, then snapshotEnd. ---
    let header = next_text(&mut stream).await?;
    let (snap_watermark, expected) = match decode_streamer_message(&header)
        .map_err(|e| ViewSyncerError::Protocol(e.to_string()))?
    {
        StreamerMessage::SnapshotHeader { watermark, bytes } => (watermark, bytes),
        other => return Err(ViewSyncerError::Protocol(format!("expected snapshot, got {other:?}"))),
    };
    let mut buf: Vec<u8> = Vec::with_capacity(expected);
    while buf.len() < expected {
        match stream.next().await {
            Some(Ok(Message::Binary(b))) => buf.extend_from_slice(&b),
            Some(Ok(Message::Text(t))) => {
                // A stray text frame before all bytes arrived is a protocol error.
                return Err(ViewSyncerError::Protocol(format!(
                    "unexpected text during snapshot: {t}"
                )));
            }
            Some(Ok(_)) => {}
            _ => return Err(ViewSyncerError::Protocol("snapshot truncated".into())),
        }
    }
    // Expect snapshotEnd.
    let end = next_text(&mut stream).await?;
    if !matches!(
        decode_streamer_message(&end),
        Ok(StreamerMessage::SnapshotEnd)
    ) {
        return Err(ViewSyncerError::Protocol("expected snapshotEnd".into()));
    }

    // Write the snapshot as the local replica (clear stale WAL sidecars first).
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{replica_path}{suffix}"));
    }
    std::fs::write(&replica_path, &buf).map_err(|e| ViewSyncerError::Replica(e.to_string()))?;
    let db = StatementRunner::open_file(&replica_path)
        .map_err(|e| ViewSyncerError::Replica(e.to_string()))?;
    crate::info!(
        "bootstrapped replica from snapshot at watermark {snap_watermark} ({} bytes)",
        buf.len()
    );
    if let Some(ready) = &ready {
        ready.store(true, Ordering::SeqCst);
    }

    // --- Live: apply each streamed commit + publish to the local fan-out. ---
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
        if let Ok(StreamerMessage::Commit { watermark, changes }) = decode_streamer_message(&text) {
            let n = changes.len();
            apply_streamed_commit(&db, &watermark, &changes)
                .map_err(|e| ViewSyncerError::Replica(e.to_string()))?;
            // Notify local clients (they re-hydrate + poke).
            service.publish_commit(watermark, false, n as i64);
        }
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
                return Err(ViewSyncerError::Protocol("expected text, got binary".into()))
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
