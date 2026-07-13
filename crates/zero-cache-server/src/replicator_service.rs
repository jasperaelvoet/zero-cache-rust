//! The in-process replicator service — the piece that makes the running server
//! serve REAL data: connect to upstream Postgres, build/maintain a durable
//! SQLite replica, and fan each committed change out to connected clients.
//!
//! It assembles modules that were individually built and tested:
//!   * `run_full_initial_sync` — snapshot-copy the published schema+data into the
//!     writer replica and create the replication slot;
//!   * `drive_apply_loop` — stream ongoing changes and apply them to the replica,
//!     calling [`SyncService::publish_commit`] after each committed transaction;
//!   * `ReplicatorSupervisor` / `decide_next_action` — reconnect after a clean
//!     stream end (resuming from the slot's confirmed LSN) or resync on schema
//!     drift; stop on shutdown.
//!
//! One writer connection (this service) owns the replica file in WAL mode; each
//! view-syncer connection opens its own read-only connection to the same file
//! (see `bootstrap::live_handler`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use zero_cache_change_source::pg_connection;
use zero_cache_change_source::published_schema::get_publication_info;
use zero_cache_change_source::replication_conn::ReplicationConn;
use zero_cache_sqlite::initial_sync::{
    reset_replica_for_resync, run_full_initial_sync, InitialSyncParams,
};
use zero_cache_sqlite::replication_apply::ReplicationApplier;
use zero_cache_sqlite::replication_supervisor::{ReplicatorSupervisor, SupervisorDecision};
use zero_cache_sqlite::StatementRunner;
use zero_cache_types::shards::ShardConfig;

use crate::sync_service::SyncService;

/// Upstream + shard connection parameters for [`run_replicator`].
#[derive(Debug, Clone)]
pub struct ReplicatorConfig {
    /// libpq-style connection string for ordinary query/copy connections.
    pub conn_str: String,
    /// Host/port/user/dbname for the raw replication-protocol connection.
    pub host: String,
    pub port: u16,
    pub user: String,
    pub dbname: String,
    /// Upstream password (for md5/SCRAM/cleartext replication-protocol auth).
    pub password: Option<String>,
    /// Path to the durable SQLite replica file (WAL).
    pub replica_path: String,
    /// Shard identity (drives publication/slot names).
    pub app_id: String,
    pub shard_num: i64,
    /// Upstream publications the shard replicates (empty → shard defaults).
    pub publications: Vec<String>,
    /// Official tuning options honored on the replication path.
    pub tuning: ReplicatorTuning,
}

/// Official replication-path options (defaults match upstream's).
#[derive(Debug, Clone)]
pub struct ReplicatorTuning {
    /// `ZERO_UPSTREAM_PG_REPLICATION_SLOT_FAILOVER` (PG 17+ failover slots).
    pub pg_replication_slot_failover: bool,
    /// `ZERO_REPLICA_VACUUM_INTERVAL_HOURS` — startup VACUUM cadence.
    pub replica_vacuum_interval_hours: Option<f64>,
    /// `ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS` (default 5).
    pub initial_sync_table_copy_workers: usize,
    /// `ZERO_INITIAL_SYNC_TEXT_COPY` (default false).
    pub initial_sync_text_copy: bool,
    /// `ZERO_INITIAL_SYNC_PROFILE_COPY` — log per-table copy timings.
    pub initial_sync_profile_copy: bool,
    /// `ZERO_REPLICATION_LAG_REPORT_INTERVAL_MS` (default 30000; <= 0 off).
    pub replication_lag_report_interval_ms: i64,
    /// `ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS` (default 15000) — grace period
    /// before taking over the replication stream, applied on dedicated
    /// change-streamer nodes (`apply_startup_delay`). Upstream cancels the
    /// delay on an incoming change-stream request; a single-node deployment's
    /// in-process subscriber is equivalent to an immediate cancel, so the
    /// delay applies only to dedicated nodes serving remote view-syncers.
    pub change_streamer_startup_delay_ms: u64,
    /// Whether this node applies the startup delay (dedicated streamer).
    pub apply_startup_delay: bool,
}

impl Default for ReplicatorTuning {
    fn default() -> Self {
        ReplicatorTuning {
            pg_replication_slot_failover: false,
            replica_vacuum_interval_hours: None,
            initial_sync_table_copy_workers: 5,
            initial_sync_text_copy: false,
            initial_sync_profile_copy: false,
            replication_lag_report_interval_ms: 30_000,
            change_streamer_startup_delay_ms: 15_000,
            apply_startup_delay: false,
        }
    }
}

impl ReplicatorConfig {
    fn slot_name(&self) -> String {
        // Mirrors the shard's slot naming: `zero_slot_<app>_<shard>`.
        format!("zero_slot_{}_{}", self.app_id, self.shard_num)
    }

    /// Builds a config from a `ZERO_UPSTREAM_DB` connection string, deriving the
    /// raw-replication connection parts (`host`/`port`/`user`/`dbname`/
    /// `password`). Accepts BOTH libpq keyword form (`host=… port=… user=…`) AND
    /// URL form (`postgres://user:pass@host:port/dbname?…`) — `tokio-postgres`
    /// parses either for the ordinary connections, so the hand-rolled
    /// replication connection must too, or it silently falls back to
    /// `localhost:5432` and gets connection-refused. Unspecified parts default
    /// to libpq-ish defaults (`localhost`/`5432`/`postgres`/`postgres`).
    pub fn from_upstream(
        conn_str: &str,
        replica_path: String,
        app_id: String,
        shard_num: i64,
        publications: Vec<String>,
    ) -> Self {
        let p = parse_conn_parts(conn_str);
        ReplicatorConfig {
            conn_str: conn_str.to_string(),
            host: p.host.unwrap_or_else(|| "localhost".to_string()),
            port: p.port.unwrap_or(5432),
            user: p.user.unwrap_or_else(|| "postgres".to_string()),
            dbname: p.dbname.unwrap_or_else(|| "postgres".to_string()),
            password: p.password,
            replica_path,
            app_id,
            shard_num,
            publications,
            tuning: ReplicatorTuning::default(),
        }
    }
}

/// The connection parts the raw replication connection needs.
#[derive(Debug, Default, PartialEq, Eq)]
struct ConnParts {
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    dbname: Option<String>,
    password: Option<String>,
}

/// Parses either a `postgres://`/`postgresql://` URL or a libpq keyword string.
fn parse_conn_parts(s: &str) -> ConnParts {
    let s = s.trim();
    if let Some(rest) = s
        .strip_prefix("postgresql://")
        .or_else(|| s.strip_prefix("postgres://"))
    {
        parse_url_parts(rest)
    } else {
        let kv = parse_libpq(s);
        ConnParts {
            host: kv.get("host").cloned(),
            port: kv.get("port").and_then(|v| v.parse().ok()),
            user: kv.get("user").cloned(),
            dbname: kv.get("dbname").cloned(),
            password: kv.get("password").cloned(),
        }
    }
}

/// Parses the part of a Postgres URL after the scheme:
/// `[user[:password]@]host[:port][/dbname][?k=v&…]`. libpq also allows
/// `host`/`port`/`user`/`dbname`/`password` as query params, which override.
fn parse_url_parts(rest: &str) -> ConnParts {
    let mut out = ConnParts::default();

    // Split off the query string.
    let (authority_path, query) = match rest.split_once('?') {
        Some((a, q)) => (a, Some(q)),
        None => (rest, None),
    };
    // Split authority from the path (dbname).
    let (authority, path) = match authority_path.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (authority_path, None),
    };
    if let Some(db) = path.filter(|p| !p.is_empty()) {
        out.dbname = Some(pct_decode(db));
    }
    // userinfo@hostport
    let (userinfo, hostport) = match authority.rsplit_once('@') {
        Some((u, h)) => (Some(u), h),
        None => (None, authority),
    };
    if let Some(ui) = userinfo {
        match ui.split_once(':') {
            Some((u, pw)) => {
                if !u.is_empty() {
                    out.user = Some(pct_decode(u));
                }
                out.password = Some(pct_decode(pw));
            }
            None if !ui.is_empty() => out.user = Some(pct_decode(ui)),
            None => {}
        }
    }
    // host[:port] — rsplit so IPv4/host:port works (IPv6 in brackets is rare
    // here; the query-param form handles it if needed).
    if !hostport.is_empty() {
        match hostport.rsplit_once(':') {
            Some((h, p)) => {
                if !h.is_empty() {
                    out.host = Some(pct_decode(h));
                }
                out.port = p.parse().ok();
            }
            None => out.host = Some(pct_decode(hostport)),
        }
    }
    // Query-param overrides (libpq style).
    if let Some(q) = query {
        for pair in q.split('&') {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let v = pct_decode(v);
            match k {
                "host" if !v.is_empty() => out.host = Some(v),
                "port" => out.port = v.parse().ok().or(out.port),
                "user" if !v.is_empty() => out.user = Some(v),
                "dbname" | "database" if !v.is_empty() => out.dbname = Some(v),
                "password" if !v.is_empty() => out.password = Some(v),
                _ => {}
            }
        }
    }
    out
}

/// Minimal `%XX` percent-decoding for URL connection-string components.
fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(byte) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(byte);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Parses a libpq space-separated `key=value` connection string into a map.
/// (Deliberately simple: no quoting/escaping — sufficient for the `host=… port=…
/// user=… password=… dbname=…` form the deployment uses.)
fn parse_libpq(s: &str) -> std::collections::HashMap<String, String> {
    s.split_whitespace()
        .filter_map(|tok| tok.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub enum ReplicatorError {
    #[error("initial sync failed: {0}")]
    InitialSync(String),
    #[error("replication stream error: {0}")]
    Replication(String),
    #[error("apply error: {0}")]
    Apply(String),
    #[error("replica db error: {0}")]
    Db(String),
}

/// Makes an initial-sync attempt start from a genuinely fresh state.
///
/// A replication slot cannot be reused for initial sync because only creating
/// it exports the snapshot used by the bulk copy. Likewise, the existing
/// SQLite replica belongs to that old snapshot. Clean both before the first
/// attempt (and every retry), instead of discovering the stale slot via a
/// failed `CREATE_REPLICATION_SLOT` and waiting for the retry backoff.
async fn prepare_initial_sync_attempt(
    cfg: &ReplicatorConfig,
    db: &StatementRunner,
    slot: &str,
) -> Result<(), ReplicatorError> {
    reset_replica_for_resync(db).map_err(|e| ReplicatorError::Db(e.to_string()))?;

    let admin = pg_connection::connect(&cfg.conn_str)
        .await
        .map_err(|e| ReplicatorError::InitialSync(e.to_string()))?;
    admin
        .query_opt(
            "SELECT pg_drop_replication_slot($1) WHERE EXISTS (\
             SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot],
        )
        .await
        .map_err(|e| {
            ReplicatorError::InitialSync(format!("dropping stale replication slot: {e}"))
        })?;
    Ok(())
}

/// Runs the replicator until `shutdown` is set. Returns the accumulated
/// lifecycle counters (`total_commits`, `reconnects`, `resyncs`).
///
/// After the initial sync completes, `ready` (if provided) is set to `true` so
/// a readiness probe can flip — clients should only be served once the replica
/// exists. The `service`'s fan-out receives a `publish_commit` for every applied
/// transaction, which connected clients turn into pokes.
pub async fn run_replicator(
    cfg: ReplicatorConfig,
    service: Arc<SyncService>,
    shutdown: Arc<AtomicBool>,
    ready: Option<Arc<AtomicBool>>,
) -> Result<ReplicatorSupervisor, ReplicatorError> {
    // Writer replica (WAL) — created/populated by initial sync.
    let db = StatementRunner::open_file(&cfg.replica_path)
        .map_err(|e| ReplicatorError::Db(e.to_string()))?;

    // M11: apply the writer/serving replica's runtime pragmas, matching
    // upstream `replicator.ts`'s `getPragmaConfig`/`applyPragmas`
    // (`busy_timeout=30000`, `analysis_limit=1000`) instead of the bare
    // `busy_timeout=5000` that `open_file` leaves behind. The serving replica
    // uses `ReplicaFileMode::Serving` (no `wal_autocheckpoint` override — that
    // is set from the litestream threshold below), then runs `PRAGMA optimize`
    // so the planner has the stat refresh upstream relies on for hydration.
    {
        use zero_cache_sqlite::replicator_setup::{
            apply_pragmas, get_pragma_config, ReplicaFileMode,
        };
        let pragmas = get_pragma_config(ReplicaFileMode::Serving);
        if let Err(e) = apply_pragmas(&db, &pragmas) {
            crate::warn!("could not apply replica pragmas: {e}");
        }
        if let Err(e) = db.pragma("optimize = 0x10002") {
            crate::warn!("could not run PRAGMA optimize on the replica: {e}");
        }
    }

    // ZERO_LITESTREAM_MIN_CHECKPOINT_PAGE_COUNT (default thresholdMB * 250,
    // 4KB pages): upstream sets SQLite's auto-checkpoint page count so WAL
    // segments roll at the size litestream backs up. Applied whenever a
    // litestream backup is in play; harmless otherwise.
    {
        let threshold_mb: u64 = std::env::var("ZERO_LITESTREAM_CHECKPOINT_THRESHOLD_MB")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(40);
        let min_pages: u64 = std::env::var("ZERO_LITESTREAM_MIN_CHECKPOINT_PAGE_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(threshold_mb * 250);
        if let Err(e) = db.exec(&format!("PRAGMA wal_autocheckpoint = {min_pages}")) {
            crate::warn!("could not set wal_autocheckpoint: {e}");
        }
    }

    // ZERO_REPLICA_VACUUM_INTERVAL_HOURS: VACUUM at startup when the interval
    // has elapsed since the last sync/upgrade/vacuum runtime event (upstream
    // replicator.ts prepare()). Runs against the pre-existing replica before
    // any resync decision; the completion is recorded as a 'vacuum' event.
    if let Some(hours) = cfg.tuning.replica_vacuum_interval_hours {
        match zero_cache_sqlite::runtime_events::maybe_vacuum_at_startup(&db, hours) {
            Ok(true) => crate::info!("replica VACUUM completed (interval {hours}h elapsed)"),
            Ok(false) => {}
            Err(e) => crate::warn!("replica VACUUM check failed: {e}"),
        }
    }

    let params = InitialSyncParams {
        conn_str: cfg.conn_str.clone(),
        host: cfg.host.clone(),
        port: cfg.port,
        user: cfg.user.clone(),
        dbname: cfg.dbname.clone(),
        password: cfg.password.clone(),
        slot_name: cfg.slot_name(),
        pg_replication_slot_failover: cfg.tuning.pg_replication_slot_failover,
        // ZERO_INITIAL_SYNC_TABLE_COPY_WORKERS / _TEXT_COPY / _PROFILE_COPY.
        copy_options: zero_cache_sqlite::initial_sync::InitialSyncOptions {
            table_copy_workers: cfg.tuning.initial_sync_table_copy_workers,
            text_copy: cfg.tuning.initial_sync_text_copy,
            profile_copy: cfg.tuning.initial_sync_profile_copy,
        },
    };
    let requested = ShardConfig {
        app_id: cfg.app_id.clone(),
        shard_num: cfg.shard_num,
        publications: cfg.publications.clone(),
    };

    // ZERO_CHANGE_STREAMER_STARTUP_DELAY_MS: on a dedicated change-streamer,
    // wait before taking over the replication stream (dropping/re-creating
    // the previous manager's slot) so load balancers can register the task.
    if cfg.tuning.apply_startup_delay && cfg.tuning.change_streamer_startup_delay_ms > 0 {
        let delay = cfg.tuning.change_streamer_startup_delay_ms;
        crate::info!("waiting {delay}ms before replication takeover (startup delay)");
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(delay);
        while std::time::Instant::now() < deadline && !shutdown.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    // Snapshot-copy the schema+data and create the slot. Every attempt starts
    // by clearing the old replica and slot: CREATE_REPLICATION_SLOT must create
    // a new slot to export the snapshot used by the copy. Retry until it
    // succeeds or shutdown — the upstream (or its publication) may not be ready
    // yet, and a permanently-failed sync would never flip readiness.
    let slot = cfg.slot_name();
    let (_result, publications, slot_info) = {
        let mut attempt: u32 = 0;
        loop {
            let sync = async {
                prepare_initial_sync_attempt(&cfg, &db, &slot).await?;
                run_full_initial_sync(&params, &db, &requested)
                    .await
                    .map_err(|e| ReplicatorError::InitialSync(e.to_string()))
            }
            .await;
            match sync {
                Ok(v) => break v,
                Err(e) => {
                    if shutdown.load(Ordering::SeqCst) {
                        return Err(ReplicatorError::InitialSync(e.to_string()));
                    }
                    attempt += 1;
                    crate::warn!("initial sync attempt {attempt} failed: {e}; retrying in 3s…");
                    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                }
            }
        }
    };
    if let Some(ready) = &ready {
        ready.store(true, Ordering::SeqCst);
    }

    // Published table specs (for schema-drift detection during streaming).
    let query_conn = pg_connection::connect(&cfg.conn_str)
        .await
        .map_err(|e| ReplicatorError::InitialSync(e.to_string()))?;
    let pub_refs: Vec<&str> = publications.iter().map(|s| s.as_str()).collect();
    let (mut specs, _indexes) = get_publication_info(&query_conn, &pub_refs)
        .await
        .map_err(|e| ReplicatorError::InitialSync(e.to_string()))?;

    // ZERO_REPLICATION_LAG_REPORT_INTERVAL_MS: emit a WAL message on an
    // interval and observe its round-trip through the stream below to measure
    // replication lag. `<= 0` disables it. pg_logical_emit_message's `flush`
    // arg (PG 17+) is used when the server is new enough.
    let lag_prefix = crate::lag_reporter::message_prefix(&cfg.app_id, cfg.shard_num);
    let lag_state = std::sync::Arc::new(crate::lag_reporter::LagState::default());
    if cfg.tuning.replication_lag_report_interval_ms > 0 {
        let pg17_plus = pg_connection::check_upstream_config(&query_conn)
            .await
            .map(|v| v >= pg_connection::PG_17)
            .unwrap_or(false);
        tokio::spawn(crate::lag_reporter::run_lag_reporter(
            cfg.conn_str.clone(),
            lag_prefix.clone(),
            cfg.tuning.replication_lag_report_interval_ms,
            pg17_plus,
            lag_state.clone(),
            shutdown.clone(),
        ));
        crate::info!(
            "replication lag reporting every {}ms (prefix {lag_prefix})",
            cfg.tuning.replication_lag_report_interval_ms
        );
    }

    // H5 (interim safeguard): a background schema-hash poll. DDL is not
    // replicated inline (no EVENT TRIGGER port yet), and drift is otherwise
    // only noticed from a streamed `Relation` message — so a DDL with NO
    // following DML would leave the replica silently stale. This poll compares
    // the live published-schema fingerprint against the last-synced one on an
    // interval and flips `schema_change` so `drive_apply_loop` stops and the
    // supervisor resyncs. Modest by design; `ZERO_SCHEMA_POLL_INTERVAL_MS`
    // (default 30000; 0 disables) tunes it. TODO: replace with real
    // event-trigger DDL replication (`change-source/pg/schema/ddl.ts`).
    let schema_change = Arc::new(AtomicBool::new(false));
    let schema_baseline = Arc::new(std::sync::Mutex::new(schema_fingerprint(&specs)));
    let schema_poll_interval_ms: u64 = std::env::var("ZERO_SCHEMA_POLL_INTERVAL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30_000);
    if schema_poll_interval_ms > 0 {
        tokio::spawn(run_schema_poll(
            cfg.conn_str.clone(),
            publications.clone(),
            schema_poll_interval_ms,
            schema_change.clone(),
            schema_baseline.clone(),
            shutdown.clone(),
        ));
        crate::info!("schema-hash poll every {schema_poll_interval_ms}ms (DDL safeguard)");
    }

    let pubs_joined = publications.join(",");
    let mut applier =
        ReplicationApplier::new(&db).map_err(|e| ReplicatorError::Apply(e.to_string()))?;
    // Enable inline DDL replication (H5): the event triggers emit schema changes
    // on the `{app}/{shard}/ddl` logical-message prefix; teach the applier that
    // prefix so it decodes and applies them incrementally instead of falling back
    // to the schema-hash-poll resync.
    applier.set_shard(&cfg.app_id, cfg.shard_num);
    let mut sup = ReplicatorSupervisor::new();
    let mut resume_lsn = slot_info.consistent_point.clone();
    // Consecutive (re)connect failures, for exponential backoff. Reset to 0 on
    // every successful subscribe.
    let mut reconnect_attempt: u32 = 0;

    while !shutdown.load(Ordering::SeqCst) {
        // (Re)subscribe from the resume LSN. A transient upstream disconnect
        // (PG restart / RDS failover / network blip / wal_sender_timeout) is a
        // RECONNECT signal, not a fatal condition — crashing the process here
        // restarted the pod and re-ran a full initial sync on every hiccup.
        // Retry with backoff, gated on shutdown, mirroring the initial-sync loop.
        let conn = match ReplicationConn::connect(
            &cfg.host,
            cfg.port,
            &cfg.user,
            &cfg.dbname,
            cfg.password.as_deref(),
            zero_cache_change_source::pg_tls::PgSslMode::from_conn_str(&cfg.conn_str),
        )
        .await
        {
            Ok(conn) => conn,
            Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
                    break;
                }
                reconnect_attempt += 1;
                let delay = reconnect_backoff(reconnect_attempt);
                crate::warn!(
                    "replication connect failed (attempt {reconnect_attempt}): {e}; \
                     retrying in {}ms…",
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        let mut stream = match conn
            .start_replication(&slot, &pubs_joined, &resume_lsn)
            .await
        {
            Ok(stream) => stream,
            Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
                    break;
                }
                reconnect_attempt += 1;
                let delay = reconnect_backoff(reconnect_attempt);
                crate::warn!(
                    "start_replication failed (attempt {reconnect_attempt}): {e}; \
                     retrying in {}ms…",
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                continue;
            }
        };
        // Subscribed successfully — reset the backoff.
        reconnect_attempt = 0;

        // Apply commits; publish each to the fan-out; stop when shutdown flips.
        let shutdown_inner = shutdown.clone();
        let service_inner = service.clone();
        let lag_state_inner = lag_state.clone();
        let lag_prefix_inner = lag_prefix.clone();
        let outcome = zero_cache_sqlite::replication_apply::drive_apply_loop_with_message_observer(
            &mut stream,
            &mut applier,
            &specs,
            move |commit| {
                service_inner.publish_commit(
                    commit.watermark.clone(),
                    commit.schema_changed,
                    commit.num_change_log_entries,
                );
                shutdown_inner.load(Ordering::SeqCst)
            },
            move |message, receive_ms| {
                if let zero_cache_change_source::pgoutput::PgoutputMessage::Message {
                    prefix,
                    content,
                    ..
                } = message
                {
                    if let Some(lag) = crate::lag_reporter::observe_message(
                        &lag_state_inner,
                        &lag_prefix_inner,
                        prefix,
                        content,
                        receive_ms,
                    ) {
                        crate::info!("replication lag: {lag}ms (total)");
                    }
                }
            },
            Some(&*schema_change),
        )
        .await;
        drop(stream);

        // A mid-stream apply/replication error (the ordinary disconnect: TCP RST,
        // wal_sender_timeout, PG restart) is a reconnect signal, not fatal. The
        // supervised reconnect path used to be unreachable because it only ran on
        // a graceful CopyDone, which Postgres essentially never sends.
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(e) => {
                if shutdown.load(Ordering::SeqCst) {
                    break;
                }
                // The interrupted transaction is re-delivered from its start when
                // the resumed stream replays from confirmed_flush_lsn, so drop it
                // and leave the applier at a clean transaction boundary (else the
                // next Begin errors AlreadyInTransaction).
                applier.rollback().ok();
                resume_lsn = confirmed_lsn(&query_conn, &slot)
                    .await
                    .unwrap_or(resume_lsn);
                reconnect_attempt += 1;
                let delay = reconnect_backoff(reconnect_attempt);
                crate::warn!(
                    "replication stream error (attempt {reconnect_attempt}, will reconnect): \
                     {e}; retrying in {}ms…",
                    delay.as_millis()
                );
                tokio::time::sleep(delay).await;
                continue;
            }
        };

        match sup.record(&outcome, shutdown.load(Ordering::SeqCst)) {
            SupervisorDecision::Stop => break,
            SupervisorDecision::Reconnect { .. } => {
                // A clean stream-end (CopyDone). Roll back any transaction left
                // open at the boundary so the resumed stream's replay from
                // confirmed_flush_lsn re-applies it from a clean state.
                applier.rollback().ok();
                // Resume from the slot's confirmed position.
                resume_lsn = confirmed_lsn(&query_conn, &slot)
                    .await
                    .unwrap_or(resume_lsn);
            }
            SupervisorDecision::Resync { .. } => {
                // Schema drifted: the replica is stale. Roll back the interrupted
                // txn, discard the replica, drop the slot, and re-run initial
                // sync from a fresh snapshot.
                applier.rollback().ok();
                reset_replica_for_resync(&db).map_err(|e| ReplicatorError::Db(e.to_string()))?;
                let _ = query_conn
                    .batch_execute(&format!(
                        "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
                         (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}')"
                    ))
                    .await;
                let (_r, new_pubs, new_slot) = run_full_initial_sync(&params, &db, &requested)
                    .await
                    .map_err(|e| ReplicatorError::InitialSync(e.to_string()))?;
                let new_pub_refs: Vec<&str> = new_pubs.iter().map(|s| s.as_str()).collect();
                let (new_specs, _i) = get_publication_info(&query_conn, &new_pub_refs)
                    .await
                    .map_err(|e| ReplicatorError::InitialSync(e.to_string()))?;
                specs = new_specs;
                resume_lsn = new_slot.consistent_point.clone();
                applier = ReplicationApplier::new(&db)
                    .map_err(|e| ReplicatorError::Apply(e.to_string()))?;
                applier.set_shard(&cfg.app_id, cfg.shard_num);
                // H5: the replica now matches the new schema — update the poll
                // baseline and clear the signal so the poll stops re-firing.
                *schema_baseline.lock().unwrap() = schema_fingerprint(&specs);
                schema_change.store(false, Ordering::SeqCst);
            }
        }
    }

    Ok(sup)
}

/// Spawns [`run_replicator`] on a **dedicated OS thread with its own
/// current-thread Tokio runtime**. The replicator owns a single-threaded SQLite
/// writer (a `!Sync` handle held across awaits), so it cannot be
/// `tokio::spawn`ed onto the shared multi-threaded runtime; a dedicated thread
/// is both required and the natural fit (it is a single-writer loop). The
/// returned `JoinHandle` yields the lifecycle counters when the loop stops
/// (after `shutdown` is set).
pub fn spawn_replicator_thread(
    cfg: ReplicatorConfig,
    service: Arc<SyncService>,
    shutdown: Arc<AtomicBool>,
    ready: Option<Arc<AtomicBool>>,
) -> std::thread::JoinHandle<Result<ReplicatorSupervisor, ReplicatorError>> {
    std::thread::Builder::new()
        .name("zero-replicator".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| ReplicatorError::Db(e.to_string()))?;
            rt.block_on(run_replicator(cfg, service, shutdown, ready))
        })
        .expect("spawn replicator thread")
}

/// Capped exponential backoff between replication (re)connect attempts, so a
/// still-unavailable upstream is retried without hammering it (or hot-spinning).
/// ~500ms, 1s, 2s, … capped at 30s.
fn reconnect_backoff(attempt: u32) -> std::time::Duration {
    let shift = attempt.saturating_sub(1).min(6);
    let ms = 500u64.saturating_mul(1u64 << shift).min(30_000);
    std::time::Duration::from_millis(ms)
}

/// A stable fingerprint of the published table specs — the H5 schema-hash
/// poll's comparison value. Derived from the specs' `Debug` form (deterministic:
/// tables are query-ordered and columns are `pos`-ordered), which captures table
/// names, columns, types and keys without needing `Hash` on the spec types.
fn schema_fingerprint(specs: &[zero_cache_types::specs::PublishedTableSpec]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{specs:?}").hash(&mut hasher);
    hasher.finish()
}

/// H5 interim safeguard: periodically re-introspect the published schema and,
/// if its fingerprint diverges from the last-synced `baseline`, flip
/// `schema_change` so the apply loop stops and the supervisor resyncs. This
/// catches a DDL that ships no following DML (hence no pgoutput `Relation`
/// message). Runs its own upstream connection (reconnecting on error) until
/// `shutdown`. Intentionally best-effort: transient introspection errors are
/// logged and retried, never fatal.
async fn run_schema_poll(
    conn_str: String,
    publications: Vec<String>,
    interval_ms: u64,
    schema_change: Arc<AtomicBool>,
    baseline: Arc<std::sync::Mutex<u64>>,
    shutdown: Arc<AtomicBool>,
) {
    let interval = std::time::Duration::from_millis(interval_ms);
    while !shutdown.load(Ordering::SeqCst) {
        tokio::time::sleep(interval).await;
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let conn = match pg_connection::connect(&conn_str).await {
            Ok(c) => c,
            Err(e) => {
                crate::warn!("schema poll: could not connect upstream: {e}");
                continue;
            }
        };
        let pub_refs: Vec<&str> = publications.iter().map(|s| s.as_str()).collect();
        match get_publication_info(&conn, &pub_refs).await {
            Ok((specs, _indexes)) => {
                let fp = schema_fingerprint(&specs);
                if fp != *baseline.lock().unwrap() {
                    crate::info!("schema poll: upstream schema changed — triggering resync");
                    schema_change.store(true, Ordering::SeqCst);
                }
            }
            Err(e) => crate::warn!("schema poll: introspection failed: {e}"),
        }
    }
}

/// The slot's current `confirmed_flush_lsn` (where a resubscribe resumes).
async fn confirmed_lsn(pg: &tokio_postgres::Client, slot: &str) -> Option<String> {
    let row = pg
        .query_opt(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .ok()??;
    row.get::<_, Option<String>>(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use zero_cache_sqlite::change_fanout::FanoutEvent;

    #[test]
    fn reconnect_backoff_is_capped_exponential() {
        // First retry ~500ms, doubling, capped at 30s — bounded so a still-down
        // upstream is retried without hot-spinning or hammering.
        assert_eq!(reconnect_backoff(1), Duration::from_millis(500));
        assert_eq!(reconnect_backoff(2), Duration::from_millis(1000));
        assert_eq!(reconnect_backoff(3), Duration::from_millis(2000));
        assert_eq!(reconnect_backoff(7), Duration::from_millis(30_000));
        // Never exceeds the 30s cap, however many attempts.
        assert_eq!(reconnect_backoff(50), Duration::from_millis(30_000));
        // Monotonic non-decreasing.
        for a in 1..20 {
            assert!(reconnect_backoff(a) <= reconnect_backoff(a + 1));
        }
    }

    #[test]
    fn from_upstream_parses_a_url_connection_string() {
        // hunting-game's form: a postgres URL, not libpq keywords. The raw
        // replication connection must get the real host/port (not localhost).
        let c = ReplicatorConfig::from_upstream(
            "postgresql://hunting_game:secret@db.internal:5433/hunting_game",
            "/tmp/r.db".into(),
            "app".into(),
            0,
            vec![],
        );
        assert_eq!(c.host, "db.internal");
        assert_eq!(c.port, 5433);
        assert_eq!(c.user, "hunting_game");
        assert_eq!(c.dbname, "hunting_game");
        assert_eq!(c.password.as_deref(), Some("secret"));
    }

    #[test]
    fn from_upstream_still_parses_libpq_keywords() {
        let c = ReplicatorConfig::from_upstream(
            "host=pg port=6000 user=u password=p dbname=d",
            "/tmp/r.db".into(),
            "app".into(),
            0,
            vec![],
        );
        assert_eq!(
            (c.host.as_str(), c.port, c.user.as_str(), c.dbname.as_str()),
            ("pg", 6000, "u", "d")
        );
        assert_eq!(c.password.as_deref(), Some("p"));
    }

    #[test]
    fn url_parsing_handles_defaults_percent_encoding_and_query_overrides() {
        // No port/db in the URL → libpq defaults; percent-encoded password;
        // sslmode query param ignored.
        let p = parse_conn_parts("postgres://user%40x:p%40ss@host/?sslmode=require");
        assert_eq!(p.host.as_deref(), Some("host"));
        assert_eq!(p.user.as_deref(), Some("user@x"));
        assert_eq!(p.password.as_deref(), Some("p@ss"));
        assert_eq!(p.port, None); // → from_upstream applies 5432
                                  // Query-param host override (libpq style, e.g. unix socket dir).
        let q = parse_conn_parts("postgresql://u@ignored/db?host=/var/run&port=5555");
        assert_eq!(q.host.as_deref(), Some("/var/run"));
        assert_eq!(q.port, Some(5555));
        assert_eq!(q.dbname.as_deref(), Some("db"));
    }

    #[test]
    fn schema_fingerprint_changes_when_a_table_is_added_or_renamed() {
        use std::collections::BTreeMap;
        use zero_cache_types::specs::PublishedTableSpec;
        let spec = |name: &str| PublishedTableSpec {
            name: name.into(),
            schema: "public".into(),
            oid: 1,
            schema_oid: None,
            columns: vec![],
            primary_key: Some(vec!["id".into()]),
            replica_identity: None,
            publications: BTreeMap::new(),
        };
        let a = vec![spec("issue")];
        let a2 = vec![spec("issue")];
        let b = vec![spec("issue"), spec("comment")]; // added table
        let c = vec![spec("issues")]; // renamed table
        assert_eq!(schema_fingerprint(&a), schema_fingerprint(&a2), "stable");
        assert_ne!(
            schema_fingerprint(&a),
            schema_fingerprint(&b),
            "add detected"
        );
        assert_ne!(
            schema_fingerprint(&a),
            schema_fingerprint(&c),
            "rename detected"
        );
    }

    fn conn_str() -> String {
        std::env::var("ZERO_TEST_PG")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into())
    }
    fn host_port() -> (String, u16) {
        let url = std::env::var("ZERO_TEST_PG_TCP").unwrap_or_else(|_| "localhost:54329".into());
        let (h, p) = url.split_once(':').unwrap();
        (h.to_string(), p.parse().unwrap())
    }

    /// Live end-to-end: `run_replicator` initial-syncs a real table, streams an
    /// upstream INSERT into the replica, and fans the commit out to a subscriber
    /// (what a connected client turns into a poke). Verifies the running-server
    /// data path is assembled correctly. Skips without a test Postgres.
    #[tokio::test]
    async fn live_replicator_syncs_and_fans_out_a_commit() {
        let Ok(pg) = pg_connection::connect(&conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        let app = "replsvc";
        let slot = format!("zero_slot_{app}_0");
        // Clean any prior run.
        pg.batch_execute(&zero_cache_change_source::shard_schema::drop_shard(app, 0))
            .await
            .ok();
        pg.batch_execute(&format!(r#"DROP SCHEMA IF EXISTS "{app}" CASCADE;"#))
            .await
            .ok();
        pg.batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{slot}') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot}');"
        ))
        .await
        .ok();
        pg.batch_execute(
            "DROP TABLE IF EXISTS repl_svc_test CASCADE; \
             CREATE TABLE repl_svc_test(id int primary key, label text not null); \
             INSERT INTO repl_svc_test(id, label) VALUES (1, 'a'); \
             DROP PUBLICATION IF EXISTS repl_svc_pub; \
             CREATE PUBLICATION repl_svc_pub FOR TABLE repl_svc_test;",
        )
        .await
        .unwrap();

        let replica_path = std::env::temp_dir()
            .join(format!("zc_repl_svc_{}.db", std::process::id()))
            .to_string_lossy()
            .into_owned();
        for s in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{replica_path}{s}"));
        }

        let (host, port) = host_port();
        let cfg = ReplicatorConfig {
            conn_str: conn_str(),
            host,
            port,
            user: "postgres".into(),
            dbname: "postgres".into(),
            password: None,
            replica_path: replica_path.clone(),
            app_id: app.into(),
            shard_num: 0,
            publications: vec!["repl_svc_pub".into()],
            tuning: ReplicatorTuning::default(),
        };

        let service = Arc::new(SyncService::new(64));
        let mut subscriber = service.subscribe();
        let shutdown = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(AtomicBool::new(false));

        let handle =
            spawn_replicator_thread(cfg, service.clone(), shutdown.clone(), Some(ready.clone()));

        // Wait for initial sync to complete.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while !ready.load(Ordering::SeqCst) {
            if tokio::time::Instant::now() > deadline {
                shutdown.store(true, Ordering::SeqCst);
                panic!("initial sync did not become ready in time");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // An upstream INSERT after streaming begins -> a fanned-out commit.
        pg.batch_execute("INSERT INTO repl_svc_test(id, label) VALUES (2, 'b')")
            .await
            .unwrap();

        // The subscriber observes the commit.
        let got = tokio::time::timeout(Duration::from_secs(15), subscriber.recv()).await;

        // Signal shutdown, then nudge the stream with one more commit so the
        // apply loop wakes, sees `shutdown`, and stops promptly.
        shutdown.store(true, Ordering::SeqCst);
        pg.batch_execute("INSERT INTO repl_svc_test(id, label) VALUES (3, 'c')")
            .await
            .ok();

        match got {
            Ok(FanoutEvent::Commit(note)) => {
                assert!(!note.watermark.is_empty(), "commit carried a watermark");
            }
            other => {
                let _ = tokio::task::spawn_blocking(move || handle.join()).await;
                panic!("expected a fanned-out Commit, got {other:?}");
            }
        }

        // The replica file has the streamed row.
        let replica = StatementRunner::open_file_readonly(&replica_path).unwrap();
        let rows = replica
            .query_uncached(
                "SELECT id FROM repl_svc_test WHERE id IN (1,2) ORDER BY id",
                &[],
            )
            .unwrap();
        assert_eq!(
            rows.len(),
            2,
            "initial + streamed row present in the replica"
        );
        drop(replica);

        // Join the replicator thread (bounded).
        let _ = tokio::task::spawn_blocking(move || handle.join()).await;

        // Teardown.
        for _ in 0..20 {
            if pg
                .query(&format!("SELECT pg_drop_replication_slot('{slot}')"), &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        pg.batch_execute(&zero_cache_change_source::shard_schema::drop_shard(app, 0))
            .await
            .ok();
        pg.batch_execute(&format!(r#"DROP SCHEMA IF EXISTS "{app}" CASCADE;"#))
            .await
            .ok();
        pg.batch_execute(
            "DROP PUBLICATION IF EXISTS repl_svc_pub; DROP TABLE IF EXISTS repl_svc_test;",
        )
        .await
        .ok();
        for s in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{replica_path}{s}"));
        }
    }
}
