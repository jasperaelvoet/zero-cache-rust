//! The change-streamer network service — the "one sync service" of a
//! horizontally-scaled deployment.
//!
//! It runs on the replication-owning node and serves VIEW-SYNCER nodes over
//! the official `/replication/v6/snapshot` and `/replication/v6/changes`
//! WebSockets. View-syncers restore the advertised Litestream snapshot, then
//! receive every subsequent commit's row changes. View-syncers apply these to
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

use crate::change_streamer_wire::{encode_official_status, encode_official_transaction};
use crate::sync_service::SyncService;
use crate::ws_connection::WsConnection;

const CHANGE_STREAMER_PROTOCOL_VERSION: u32 = 6;

enum ChangeRequest {
    Changes(tokio::net::TcpStream),
    Snapshot(tokio::net::TcpStream),
    Handled,
}

async fn dispatch_request(
    tcp: tokio::net::TcpStream,
    keepalive: &crate::public_http::PublicEndpointConfig,
) -> ChangeRequest {
    let request = match crate::http_dispatch::peek_request(&tcp).await {
        Ok(request) => request,
        Err(error) => {
            crate::http_dispatch::send_response(
                tcp,
                crate::http_dispatch::HttpResponse::text("400 Bad Request", error.to_string()),
            )
            .await;
            return ChangeRequest::Handled;
        }
    };
    if !request.is_websocket_upgrade() {
        let response = match (request.method.as_str(), request.path.as_str()) {
            ("GET" | "HEAD", "/") => crate::http_dispatch::HttpResponse::text("200 OK", "OK"),
            ("GET" | "HEAD", "/keepalive") => {
                keepalive.record_keepalive();
                crate::http_dispatch::HttpResponse::text("200 OK", "OK")
            }
            _ => crate::http_dispatch::HttpResponse::text("404 Not Found", "Not Found"),
        };
        crate::http_dispatch::send_response(tcp, response.for_method(&request.method)).await;
        return ChangeRequest::Handled;
    }
    let Some((version, action)) = parse_replication_path(&request.path) else {
        crate::http_dispatch::send_response(
            tcp,
            crate::http_dispatch::HttpResponse::text(
                "400 Bad Request",
                format!("invalid path: {}", request.path),
            ),
        )
        .await;
        return ChangeRequest::Handled;
    };
    if !(1..=CHANGE_STREAMER_PROTOCOL_VERSION).contains(&version) {
        crate::http_dispatch::send_response(
            tcp,
            crate::http_dispatch::HttpResponse::text(
                "400 Bad Request",
                format!(
                    "Cannot service client at protocol v{version}. Supported protocols: [v1 ... v{CHANGE_STREAMER_PROTOCOL_VERSION}]"
                ),
            ),
        )
        .await;
        return ChangeRequest::Handled;
    }
    match action {
        "changes"
            if ["id", "replicaVersion", "watermark", "initial"]
                .iter()
                .all(|name| crate::ws_connection::query_param(&request.target, name).is_some()) =>
        {
            ChangeRequest::Changes(tcp)
        }
        "snapshot"
            if crate::ws_connection::query_param(&request.target, "taskID")
                .is_some_and(|task_id| !task_id.is_empty()) =>
        {
            ChangeRequest::Snapshot(tcp)
        }
        "changes" | "snapshot" => {
            crate::http_dispatch::send_response(
                tcp,
                crate::http_dispatch::HttpResponse::text(
                    "400 Bad Request",
                    "missing required replication query parameters",
                ),
            )
            .await;
            ChangeRequest::Handled
        }
        _ => ChangeRequest::Handled,
    }
}

fn parse_replication_path(path: &str) -> Option<(u32, &str)> {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    let ["replication", version, action] = segments.as_slice() else {
        return None;
    };
    Some((version.strip_prefix('v')?.parse().ok()?, *action))
}

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
            ResolvedChange::Delete { table, key } => changes.push(StreamedChange::Del {
                table,
                row_key: key,
            }),
        }
    }
    Some((watermark, changes))
}

/// Produces a consistent SQLite snapshot of `replica_path` (via `VACUUM INTO` a
/// temp file), returning `(bytes, watermark)`. The temp file is removed.
#[cfg(test)]
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
async fn serve_subscriber(mut conn: WsConnection, replica_path: String, service: Arc<SyncService>) {
    let since = conn
        .request_uri
        .as_deref()
        .and_then(|uri| crate::ws_connection::query_param(uri, "watermark"))
        .unwrap_or_default();

    // Subscribe to the fan-out BEFORE snapshotting so no commit is missed
    // between the snapshot and going live (we always re-read the durable log).
    let mut fanout = service.subscribe();

    if conn.send_json(&encode_official_status()).await.is_err() {
        return;
    }

    // A dedicated read connection for streaming change-log reads.
    let Ok(reader) = StatementRunner::open_file_readonly(&replica_path) else {
        return;
    };
    let mut last = since;

    // Immediate catch-up (commits since the snapshot), then live.
    if let Some((w, changes)) = changes_since(&reader, &last) {
        for message in encode_official_transaction(&w, &changes) {
            if conn.send_json(&message).await.is_err() {
                return;
            }
        }
        last = w;
    }
    loop {
        match fanout.recv().await {
            FanoutEvent::Commit(_) => {
                if let Some((w, changes)) = changes_since(&reader, &last) {
                    for message in encode_official_transaction(&w, &changes) {
                        if conn.send_json(&message).await.is_err() {
                            return;
                        }
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

/// Runs the change-streamer accept loop on `listener` until `shutdown`.
pub async fn run_change_streamer(
    listener: TcpListener,
    service: Arc<SyncService>,
    replica_path: String,
    backup_url: Option<String>,
    keepalive_timeout_ms: Option<u64>,
    shutdown: oneshot::Receiver<()>,
) {
    let keepalive =
        crate::public_http::PublicEndpointConfig::new(None, false, keepalive_timeout_ms);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)), if keepalive.keepalive_timeout_ms.is_some() => {
                if keepalive.keepalive_expired() {
                    crate::info!("keepalive timeout elapsed; stopping change-streamer listener");
                    return;
                }
            }
            accepted = listener.accept() => {
                let Ok((tcp, _)) = accepted else { return };
                let service = service.clone();
                let replica_path = replica_path.clone();
                let backup_url = backup_url.clone();
                let keepalive = keepalive.clone();
                tokio::spawn(async move {
                    match dispatch_request(tcp, &keepalive).await {
                        ChangeRequest::Changes(tcp) => {
                            if let Ok(conn) = WsConnection::accept(tcp).await {
                                serve_subscriber(conn, replica_path, service).await;
                            }
                        }
                        ChangeRequest::Snapshot(tcp) => {
                            if let Ok(mut conn) = WsConnection::accept(tcp).await {
                                let state = StatementRunner::open_file_readonly(&replica_path)
                                    .ok()
                                    .and_then(|db| zero_cache_sqlite::replication_state::get_subscription_state_and_context(&db).ok());
                                let replica_version = state.as_ref().map(|state| state.replica_version.clone()).unwrap_or_default();
                                let watermark = state.as_ref().map(|state| state.watermark.clone()).unwrap_or_default();
                                match backup_url {
                                    Some(backup_url) => {
                                        let status = serde_json::json!(["status", {
                                            "tag": "status",
                                            "backupURL": backup_url,
                                            "replicaVersion": replica_version,
                                            "minWatermark": watermark,
                                        }]);
                                        let _ = conn.send_json(&status.to_string()).await;
                                    }
                                    None => {
                                        let _ = conn.send_json(r#"["error",{"type":0,"message":"replication-manager is not configured with ZERO_LITESTREAM_BACKUP_URL"}]"#).await;
                                    }
                                }
                            }
                        }
                        ChangeRequest::Handled => {}
                    }
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_sqlite::change_log::{RowKey, CREATE_CHANGELOG_SCHEMA};
    use zero_cache_sqlite::replication_state::{
        update_replication_watermark, CREATE_REPLICATION_STATE_SCHEMA,
    };

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

    #[test]
    fn replication_paths_match_upstream_v6_surface() {
        assert_eq!(
            parse_replication_path("/replication/v6/changes"),
            Some((6, "changes"))
        );
        assert_eq!(
            parse_replication_path("/replication/v6/snapshot"),
            Some((6, "snapshot"))
        );
        assert_eq!(parse_replication_path("/replication"), None);
    }

    /// Seeds a source replica (writer) with the metadata schema + a user table.
    fn make_source(path: &str) -> StatementRunner {
        rm(path);
        let db = StatementRunner::open_file(path).unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_REPLICATION_STATE_SCHEMA).unwrap();
        db.exec(
            r#"INSERT INTO "_zero.replicationState" (stateVersion, writeTimeMs) VALUES ('01', 0)"#,
        )
        .unwrap();
        db.exec(
            r#"INSERT INTO "_zero.replicationConfig" (replicaVersion, publications, initialSyncContext) VALUES ('01', '[]', '{}')"#,
        )
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
        let (snapshot, _) = snapshot_replica(&src_path).unwrap();
        std::fs::write(&vs_path, snapshot).unwrap();
        let src_service = Arc::new(SyncService::new(64));

        // Start the change-streamer.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (cs_tx, cs_rx) = oneshot::channel();
        let cs = tokio::spawn(run_change_streamer(
            listener,
            src_service.clone(),
            src_path.clone(),
            Some("file:///unused-test-backup".into()),
            None,
            cs_rx,
        ));

        // Start the view-syncer subscriber.
        let vs_service = Arc::new(SyncService::new(64));
        let mut vs_sub = vs_service.subscribe();
        let shutdown = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(false));
        let vs = crate::view_syncer_client::spawn_view_syncer_thread(
            format!("ws://{addr}"),
            vs_path.clone(),
            vs_service.clone(),
            shutdown.clone(),
            Some(ready.clone()),
        );

        // Wait for bootstrap.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while !ready.load(Ordering::SeqCst) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "view-syncer did not bootstrap"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // The snapshot row is in the view-syncer's replica.
        {
            let r = StatementRunner::open_file_readonly(&vs_path).unwrap();
            let rows = r
                .query_uncached("SELECT id FROM issue ORDER BY id", &[])
                .unwrap();
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
            matches!(
                got,
                Ok(zero_cache_sqlite::change_fanout::FanoutEvent::Commit(_))
            ),
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
        assert!(
            converged,
            "the streamed live commit converged into the view-syncer replica"
        );

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
