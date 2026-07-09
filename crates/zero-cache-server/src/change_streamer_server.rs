//! The change-streamer network service — the "one sync service" of a
//! horizontally-scaled deployment.
//!
//! It runs on the replication-owning node and serves VIEW-SYNCER nodes over
//! WebSocket: on connect it sends a consistent snapshot of the replica, then
//! streams every subsequent commit's row changes. View-syncers apply these to
//! their own replicas ([`zero_cache_sqlite::streamed_apply`]) and serve clients
//! — so many view-syncers share ONE upstream Postgres replication slot (the one
//! this node owns).
//!
//! Wire format: [`crate::change_streamer_wire`]. Live commits are driven by the
//! node's [`SyncService`] fan-out (the replicator publishes to it), but the
//! actual changes are always read from the durable change-log, so a lagging or
//! reconnecting subscriber never misses data.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::oneshot;

use zero_cache_sqlite::change_fanout::FanoutEvent;
use zero_cache_sqlite::change_log::ChangeLog;
use zero_cache_sqlite::streamed_apply::StreamedChange;
use zero_cache_sqlite::subscriber_catchup::{parse_row_key, resolve_catchup, ResolvedChange};
use zero_cache_sqlite::StatementRunner;

use crate::change_streamer_wire::{
    decode_subscribe, encode_commit, encode_snapshot_end, encode_snapshot_header,
};
use crate::sync_service::SyncService;
use crate::ws_connection::WsConnection;

/// Reads change-log entries strictly after `since` and converts them to
/// [`StreamedChange`]s + the max watermark, or `None` if nothing is new.
pub fn changes_since(db: &StatementRunner, since: &str) -> Option<(String, Vec<StreamedChange>)> {
    let entries = ChangeLog::new(db).read_since(since).ok()?;
    if entries.is_empty() {
        return None;
    }
    let watermark = entries
        .iter()
        .map(|e| e.state_version.clone())
        .max()
        .unwrap_or_default();
    let resolved = resolve_catchup(db, &entries).ok()?;
    let mut changes = Vec::with_capacity(entries.len());
    for (entry, change) in entries.iter().zip(resolved) {
        match change {
            ResolvedChange::Set { table, row } => changes.push(StreamedChange::Set {
                table,
                row_key: parse_row_key(&entry.row_key).unwrap_or_default(),
                row,
            }),
            ResolvedChange::Delete { table, key } => {
                changes.push(StreamedChange::Del { table, row_key: key })
            }
        }
    }
    Some((watermark, changes))
}

/// Produces a consistent SQLite snapshot of `replica_path` (via `VACUUM INTO` a
/// temp file), returning `(bytes, watermark)`. The temp file is removed.
fn snapshot_replica(replica_path: &str) -> Result<(Vec<u8>, String), String> {
    let tmp = format!(
        "{replica_path}.snap.{}.{}",
        std::process::id(),
        // a per-call suffix so concurrent subscribers don't collide
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let _ = std::fs::remove_file(&tmp);
    {
        // A read connection is enough for VACUUM INTO (it only reads the source).
        let db = StatementRunner::open_file_readonly(replica_path).map_err(|e| e.to_string())?;
        db.exec(&format!("VACUUM INTO '{}'", tmp.replace('\'', "''")))
            .map_err(|e| e.to_string())?;
    }
    let watermark = {
        let snap = StatementRunner::open_file_readonly(&tmp).map_err(|e| e.to_string())?;
        zero_cache_sqlite::replication_state::get_replication_state(&snap)
            .map(|s| s.state_version)
            .unwrap_or_default()
    };
    let bytes = std::fs::read(&tmp).map_err(|e| e.to_string())?;
    let _ = std::fs::remove_file(&tmp);
    Ok((bytes, watermark))
}

/// Serves one view-syncer connection: read its `subscribe`, send a snapshot,
/// then stream commits until it disconnects or shutdown.
async fn serve_subscriber(
    mut conn: WsConnection,
    replica_path: String,
    service: Arc<SyncService>,
) {
    // The subscriber tells us where it's resuming from (empty = fresh).
    let Ok(Some(sub_text)) = conn.recv_text().await else { return };
    let _since = decode_subscribe(&sub_text).unwrap_or_default();

    // Subscribe to the fan-out BEFORE snapshotting so no commit is missed
    // between the snapshot and going live (we always re-read the durable log).
    let mut fanout = service.subscribe();

    // Send a consistent snapshot + its watermark.
    let (bytes, watermark) = match tokio::task::spawn_blocking({
        let p = replica_path.clone();
        move || snapshot_replica(&p)
    })
    .await
    {
        Ok(Ok(v)) => v,
        _ => return,
    };
    let chunk = 64 * 1024;
    if conn
        .send_json(&encode_snapshot_header(&watermark, bytes.len()))
        .await
        .is_err()
    {
        return;
    }
    for part in bytes.chunks(chunk) {
        if conn.send_binary(part.to_vec()).await.is_err() {
            return;
        }
    }
    if conn.send_json(&encode_snapshot_end()).await.is_err() {
        return;
    }

    // A dedicated read connection for streaming change-log reads.
    let Ok(reader) = StatementRunner::open_file_readonly(&replica_path) else { return };
    let mut last = watermark;

    // Immediate catch-up (commits since the snapshot), then live.
    if let Some((w, changes)) = changes_since(&reader, &last) {
        if conn.send_json(&encode_commit(&w, &changes)).await.is_err() {
            return;
        }
        last = w;
    }
    loop {
        match fanout.recv().await {
            FanoutEvent::Commit(_) => {
                if let Some((w, changes)) = changes_since(&reader, &last) {
                    if conn.send_json(&encode_commit(&w, &changes)).await.is_err() {
                        return;
                    }
                    last = w;
                }
            }
            FanoutEvent::Lagged { .. } => {
                // Re-read from the durable log covers the gap on the next commit.
            }
            FanoutEvent::Closed => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_sqlite::change_log::{CREATE_CHANGELOG_SCHEMA, RowKey};
    use zero_cache_sqlite::replication_state::{update_replication_watermark, CREATE_REPLICATION_STATE_SCHEMA};
    use zero_cache_sqlite::Value;

    fn tmp(name: &str) -> String {
        std::env::temp_dir()
            .join(format!("zc_hscale_{name}_{}.db", std::process::id()))
            .to_string_lossy()
            .into_owned()
    }
    fn rm(path: &str) {
        for s in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{s}"));
        }
    }
    fn rk(id: i64) -> RowKey {
        vec![("id".to_string(), JsonValue::Number(id as f64))]
    }

    /// Seeds a source replica (writer) with the metadata schema + a user table.
    fn make_source(path: &str) -> StatementRunner {
        rm(path);
        let db = StatementRunner::open_file(path).unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_REPLICATION_STATE_SCHEMA).unwrap();
        db.exec(r#"INSERT INTO "_zero.replicationState" (stateVersion, writeTimeMs) VALUES ('01', 0)"#)
            .unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT, \"_0_version\" TEXT)")
            .unwrap();
        // Initial row present in the snapshot (not via change-log).
        db.run(
            "INSERT INTO issue (id, title, \"_0_version\") VALUES (1, 'snapshot row', '01')",
            &[],
        )
        .unwrap();
        db
    }

    /// End-to-end horizontal scaling: a change-streamer serves a snapshot to a
    /// view-syncer, which bootstraps its own replica AND then receives a live
    /// commit streamed from the source — the view-syncer's replica converges and
    /// its own fan-out fires (so its clients would poke). No second Postgres slot.
    #[tokio::test]
    async fn view_syncer_bootstraps_from_snapshot_and_applies_a_live_commit() {
        let src_path = tmp("src");
        let vs_path = tmp("vs");
        rm(&vs_path);
        let source = make_source(&src_path);
        let src_service = Arc::new(SyncService::new(64));

        // Start the change-streamer.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (cs_tx, cs_rx) = oneshot::channel();
        let cs = tokio::spawn(run_change_streamer(
            listener,
            src_service.clone(),
            src_path.clone(),
            cs_rx,
        ));

        // Start the view-syncer subscriber.
        let vs_service = Arc::new(SyncService::new(64));
        let mut vs_sub = vs_service.subscribe();
        let shutdown = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(false));
        let vs = crate::view_syncer_client::spawn_view_syncer_thread(
            format!("ws://{addr}/replication"),
            vs_path.clone(),
            vs_service.clone(),
            shutdown.clone(),
            Some(ready.clone()),
        );

        // Wait for bootstrap.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while !ready.load(Ordering::SeqCst) {
            assert!(tokio::time::Instant::now() < deadline, "view-syncer did not bootstrap");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // The snapshot row is in the view-syncer's replica.
        {
            let r = StatementRunner::open_file_readonly(&vs_path).unwrap();
            let rows = r.query_uncached("SELECT id FROM issue ORDER BY id", &[]).unwrap();
            assert_eq!(rows.len(), 1, "snapshot row bootstrapped");
        }

        // A live commit on the source: insert row 2, log it, bump watermark,
        // publish to the source fan-out (as the replicator would).
        source
            .run(
                "INSERT INTO issue (id, title, \"_0_version\") VALUES (2, 'live row', '02')",
                &[],
            )
            .unwrap();
        zero_cache_sqlite::change_log::ChangeLog::new(&source)
            .log_set_op("02", 0, "issue", &rk(2), None)
            .unwrap();
        update_replication_watermark(&source, "02").unwrap();
        src_service.publish_commit("02", false, 1);

        // The view-syncer's own fan-out fires (its clients would re-hydrate).
        let got = tokio::time::timeout(Duration::from_secs(10), vs_sub.recv()).await;
        assert!(
            matches!(got, Ok(zero_cache_sqlite::change_fanout::FanoutEvent::Commit(_))),
            "view-syncer republished the streamed commit to its clients"
        );

        // And row 2 converged into the view-syncer's replica.
        let mut converged = false;
        for _ in 0..40 {
            let r = StatementRunner::open_file_readonly(&vs_path).unwrap();
            let n = r.query_uncached("SELECT id FROM issue", &[]).unwrap().len();
            if n == 2 {
                converged = true;
                break;
            }
            drop(r);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(converged, "the streamed live commit converged into the view-syncer replica");

        // Teardown.
        shutdown.store(true, Ordering::SeqCst);
        let _ = cs_tx.send(());
        let _ = tokio::task::spawn_blocking(move || vs.join()).await;
        cs.abort();
        drop(source);
        rm(&src_path);
        rm(&vs_path);
    }
}

/// Runs the change-streamer accept loop on `listener` until `shutdown`.
pub async fn run_change_streamer(
    listener: TcpListener,
    service: Arc<SyncService>,
    replica_path: String,
    shutdown: oneshot::Receiver<()>,
) {
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            accepted = listener.accept() => {
                let Ok((tcp, _)) = accepted else { return };
                let service = service.clone();
                let replica_path = replica_path.clone();
                tokio::spawn(async move {
                    if let Ok(conn) = WsConnection::accept(tcp).await {
                        serve_subscriber(conn, replica_path, service).await;
                    }
                });
            }
        }
    }
}
