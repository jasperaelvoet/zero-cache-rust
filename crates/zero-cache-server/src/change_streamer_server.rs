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
use zero_cache_sqlite::subscriber_catchup::{
    parse_row_key, resolve_catchup_typed, TypedResolvedChange,
};
use zero_cache_sqlite::StatementRunner;

use crate::change_streamer_wire::{
    encode_official_status, encode_official_transaction, WireChange,
};
use crate::sync_service::SyncService;
use crate::ws_connection::WsConnection;

const CHANGE_STREAMER_PROTOCOL_VERSION: u32 = 6;

/// `ErrorType` enum from
/// `mono-src/.../change-streamer/error-type-enum.ts`, sent as the `type` of a
/// `["error",{type,message}]` subscription error.
mod error_type {
    pub const WRONG_REPLICA_VERSION: u32 = 1;
    pub const WATERMARK_TOO_OLD: u32 = 2;
}

/// The `ReplicatorMode` of a subscriber (`change-streamer.ts` `SubscriberContext.mode`).
/// `serving` subscribers back user-facing view-syncers; `backup` subscribers
/// are the replication-manager-local backup replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SubscriberMode {
    Serving,
    Backup,
}

impl SubscriberMode {
    /// Upstream `getSubscriberContext`: `mode === 'backup' ? 'backup' : 'serving'`.
    fn from_param(value: Option<String>) -> Self {
        match value.as_deref() {
            Some("backup") => Self::Backup,
            _ => Self::Serving,
        }
    }
}

/// Builds a typed `["error",{type,message}]` subscription error, sent as the
/// first (and only) message of an invalid subscription — matching
/// `change-streamer-service.ts`/`storer.ts` `subscriber.close(errorType, msg)`.
fn encode_subscription_error(error_type: u32, message: &str) -> String {
    serde_json::json!(["error", {"type": error_type, "message": message}]).to_string()
}

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
        // `initial` is intentionally NOT required: upstream `getSubscriberContext`
        // reads it via `getBoolean('initial')`, defaulting to `false` when absent
        // (L9). Only `id`/`replicaVersion`/`watermark` are mandatory.
        "changes"
            if ["id", "replicaVersion", "watermark"]
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
/// [`WireChange`]s + the max watermark, or `None` if nothing is new.
///
/// Op mapping (change-log ops → `dataChangeSchema`):
/// * `d` → `delete`;
/// * `t` → `truncate` (previously this errored inside `resolve_catchup`, which
///   silently dropped the entire commit — H6(b));
/// * `s` → `update` with the full `new` row and a null `key`. The change-log
///   coalesces to one latest op per row and cannot distinguish an insert from
///   an update (H6(a)), so the wire-safe `update` superset is emitted; genuine
///   `insert` tagging awaits the out-of-scope durable-CDC store.
pub fn changes_since(db: &StatementRunner, since: &str) -> Option<(String, Vec<WireChange>)> {
    let entries = ChangeLog::new(db).read_since(since).ok()?;
    if entries.is_empty() {
        return None;
    }
    let watermark = entries
        .iter()
        .map(|e| e.state_version.clone())
        .max()
        .unwrap_or_default();
    let mut changes = Vec::with_capacity(entries.len());
    for entry in &entries {
        if entry.op == zero_cache_sqlite::change_log::TRUNCATE_OP {
            changes.push(WireChange::Truncate {
                table: entry.table.clone(),
            });
            continue;
        }
        // `s`/`d` resolve to concrete row diffs (a `d` carries just the key; an
        // `s` reads back the current full row). The TYPED resolver restores each
        // `Set` column to its declared ZQL type (booleans → true/false, JSON →
        // parsed) so those survive the multi-node wire (L4). `resolve_catchup_typed`
        // errors on any op it doesn't model, so resolve per-entry to keep the
        // truncate handling above independent.
        let resolved = resolve_catchup_typed(db, std::slice::from_ref(entry)).ok()?;
        match resolved.into_iter().next()? {
            TypedResolvedChange::Set { table, row } => changes.push(WireChange::Update {
                table,
                row_key: parse_row_key(&entry.row_key).unwrap_or_default(),
                key: None,
                row,
            }),
            TypedResolvedChange::Delete { table, key } => changes.push(WireChange::Delete {
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

/// Validates a subscriber's `replicaVersion` and `watermark` against this
/// change-streamer's replica (M12). Returns the JSON of a typed
/// `["error",{type,…}]` to reject the subscription, or `None` if it is valid.
///
/// * `WrongReplicaVersion` — the subscriber's replica derives from a different
///   initial snapshot; its changes cannot apply, so it must re-restore
///   (`change-streamer-service.ts` `subscribe`).
/// * `WatermarkTooOld` — the requested watermark is older than the earliest
///   change still retained in the durable log, so catch-up is impossible
///   (`storer.ts` catch-up).
fn validate_subscription(
    reader: &StatementRunner,
    requested_replica_version: &str,
    watermark: &str,
    mode: SubscriberMode,
) -> Option<String> {
    // If we can't read our own subscription state we can't validate; don't
    // reject (avoid false negatives during startup races).
    let state =
        zero_cache_sqlite::replication_state::get_subscription_state_and_context(reader).ok()?;

    if !requested_replica_version.is_empty() && requested_replica_version != state.replica_version {
        return Some(encode_subscription_error(
            error_type::WRONG_REPLICA_VERSION,
            &format!(
                "current replica version is {} (requested {requested_replica_version})",
                state.replica_version
            ),
        ));
    }

    // A watermark equal to the replicaVersion is the "from the initial
    // snapshot" boundary and is always valid. Otherwise, if it predates the
    // earliest retained change-log entry, catch-up is impossible.
    if !watermark.is_empty() && watermark != state.replica_version {
        if let Ok(entries) = ChangeLog::new(reader).read_since("") {
            if let Some(earliest) = entries.first().map(|e| e.state_version.as_str()) {
                if watermark < earliest {
                    // TODO(L10): upstream distinguishes `backup` subscribers here
                    // — a backup replica that is behind the change DB triggers an
                    // `AutoResetSignal` (resetting the change-streamer) rather than
                    // a subscriber-facing `WatermarkTooOld`, and the full
                    // reservation lifecycle (snapshot reserve → endReservation)
                    // gates cleanup. Until that lifecycle is ported, both modes
                    // get the typed error; `mode` is threaded so the branch is
                    // ready to diverge.
                    let _ = mode;
                    return Some(encode_subscription_error(
                        error_type::WATERMARK_TOO_OLD,
                        &format!(
                            "earliest supported watermark is {earliest} (requested {watermark})"
                        ),
                    ));
                }
            }
        }
    }

    None
}

/// Serves one view-syncer connection: read its `subscribe`, send a snapshot,
/// then stream commits until it disconnects or shutdown.
async fn serve_subscriber(mut conn: WsConnection, replica_path: String, service: Arc<SyncService>) {
    let param = |name: &str| {
        conn.request_uri
            .as_deref()
            .and_then(|uri| crate::ws_connection::query_param(uri, name))
    };
    let since = param("watermark").unwrap_or_default();
    let requested_replica_version = param("replicaVersion").unwrap_or_default();
    // L10: `mode=backup` vs `serving`. Plumbed through so the (WatermarkTooOld)
    // catch-up path can gate on it the way upstream does.
    let mode = SubscriberMode::from_param(param("mode"));

    // Subscribe to the fan-out BEFORE snapshotting so no commit is missed
    // between the snapshot and going live (we always re-read the durable log).
    let mut fanout = service.subscribe();

    // A dedicated read connection for streaming change-log reads.
    let Ok(reader) = StatementRunner::open_file_readonly(&replica_path) else {
        return;
    };

    // M12: validate the subscriber's replica version and watermark BEFORE the
    // status message. An invalid subscription's first (and only) message is a
    // typed `["error",{type,…}]`, mirroring `change-streamer-service.ts`
    // (WrongReplicaVersion) and `storer.ts` catch-up (WatermarkTooOld).
    if let Some(error) = validate_subscription(&reader, &requested_replica_version, &since, mode) {
        let _ = conn.send_json(&error).await;
        return;
    }

    if conn.send_json(&encode_official_status()).await.is_err() {
        return;
    }

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

    /// H6(b): a change-log `t` (truncate) op is carried as a
    /// [`WireChange::Truncate`] instead of erroring inside `resolve_catchup`
    /// and dropping the whole commit.
    #[test]
    fn changes_since_carries_truncate() {
        let path = tmp("truncate_src");
        let src = make_source(&path);
        ChangeLog::new(&src).log_truncate_op("02", "issue").unwrap();

        let (watermark, changes) = changes_since(&src, "01").expect("a truncate change");
        assert_eq!(watermark, "02");
        assert_eq!(
            changes,
            vec![WireChange::Truncate {
                table: "issue".into()
            }]
        );
        drop(src);
        rm(&path);
    }

    /// M12: a subscriber on a different replica version is rejected with a typed
    /// `WrongReplicaVersion` error; a subscriber requesting a watermark older
    /// than the earliest retained change gets `WatermarkTooOld`; a compatible
    /// subscriber is accepted.
    #[test]
    fn validate_subscription_enforces_replica_version_and_watermark() {
        let path = tmp("validate_src");
        let src = make_source(&path); // replicaVersion "01", watermark "01"

        // Wrong replica version → type 1.
        let err = validate_subscription(&src, "99", "99", SubscriberMode::Serving)
            .expect("wrong replica version rejected");
        assert!(err.contains("\"type\":1"), "{err}");

        // Compatible (watermark == replicaVersion boundary) → accepted.
        assert!(validate_subscription(&src, "01", "01", SubscriberMode::Serving).is_none());

        // A retained change at "05" makes an older requested watermark "02"
        // (on the correct replica version) too old → type 2.
        ChangeLog::new(&src)
            .log_set_op("05", 0, "issue", &rk(2), None)
            .unwrap();
        let err = validate_subscription(&src, "01", "02", SubscriberMode::Serving)
            .expect("stale watermark rejected");
        assert!(err.contains("\"type\":2"), "{err}");
        assert!(err.contains("earliest supported watermark"), "{err}");

        drop(src);
        rm(&path);
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
