//! The top-level sync-service orchestrator: the process-level object that owns
//! the fan-out hub and joins the replicator side to the per-connection
//! view-syncer side.
//!
//! This is the shell a `main` builds once and shares:
//!   * the REPLICATOR half (the supervised apply loop —
//!     `zero-cache-sqlite::replication_apply` + `replication_supervisor`) calls
//!     [`SyncService::publish_commit`] after each committed transaction, fanning
//!     the commit watermark out to every live connection;
//!   * each CLIENT CONNECTION (a served WebSocket — `serve_connection`) holds a
//!     [`fanout`](SyncService::fanout) subscription and, on each delivered
//!     commit, calls [`SyncService::poke_for_commit`] to turn "a commit landed"
//!     into the concrete poke that connection must send its client.
//!
//! The commit→poke computation is [`crate::commit_dispatch::pokes_for_commit`]
//! (change-log → changed tables → invalidation → re-execute-got → build poke);
//! this type wires that to the live fan-out so the replicator and the
//! connections are driven by the SAME commit stream. Re-hydration stays a
//! caller-supplied closure (the live IVM pipeline), exactly as in
//! `commit_dispatch`.

use zero_cache_sqlite::change_fanout::{ChangeFanout, CommitNotification, FanoutSubscriber};
use zero_cache_sqlite::StatementRunner;
use zero_cache_view_syncer::client_patch::PatchToVersion;
use zero_cache_view_syncer::cvr_version::NullableCvrVersion;
use zero_cache_view_syncer::poke_builder::PokeMessages;

use std::sync::Arc;
use std::time::Instant;
use zero_cache_services::metrics::{Category, Counter, InMemoryBackend, LatencyHistogram, Metrics};

use crate::commit_dispatch::{pokes_for_commit, CommitDispatchError, TrackedQuery};

/// The shared sync-service hub. Cheap to construct; the fan-out inside is the
/// single rendezvous point between the replicator and all connections.
pub struct SyncService {
    fanout: ChangeFanout,
    metrics: Arc<Metrics>,
    /// `zero.replication.commit` — total transactions the replicator has
    /// fanned out through this service.
    commit_counter: Counter,
    /// `zero.sync.poke-time` — wall time to compute a client poke for a commit.
    poke_time: LatencyHistogram,
}

impl SyncService {
    /// Creates the hub with an in-memory metrics backend (a real deployment
    /// uses [`SyncService::with_metrics`] to supply an OTel-forwarding one).
    /// `fanout_capacity` bounds each connection's in-flight commit buffer
    /// before it must re-catch-up from the change-log.
    pub fn new(fanout_capacity: usize) -> Self {
        Self::with_metrics(
            fanout_capacity,
            Arc::new(Metrics::new(Arc::new(InMemoryBackend::new()))),
        )
    }

    /// Creates the hub with a caller-supplied [`Metrics`] registry — the seam a
    /// process uses to route instrumentation to a live OTel backend.
    pub fn with_metrics(fanout_capacity: usize, metrics: Arc<Metrics>) -> Self {
        let commit_counter = metrics.get_or_create_counter(Category::Replication, "commit");
        let poke_time = metrics.get_or_create_latency_histogram(Category::Sync, "poke-time");
        SyncService {
            fanout: ChangeFanout::new(fanout_capacity),
            metrics,
            commit_counter,
            poke_time,
        }
    }

    /// The service's metrics registry (for wiring further instruments).
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }

    /// The fan-out hub. A connection calls `.subscribe()` on it (after catching
    /// up to the current watermark) to follow the live commit stream.
    pub fn fanout(&self) -> &ChangeFanout {
        &self.fanout
    }

    /// Registers a connection subscriber — sugar for `self.fanout().subscribe()`.
    pub fn subscribe(&self) -> FanoutSubscriber {
        self.fanout.subscribe()
    }

    /// Replicator side: announce a committed transaction at `watermark` to every
    /// live connection. Returns how many connections it reached (0 is fine — the
    /// change-log is the durable record). `num_change_log_entries` and
    /// `schema_changed` are carried through for observability / drift signalling.
    pub fn publish_commit(
        &self,
        watermark: impl Into<String>,
        schema_changed: bool,
        num_change_log_entries: i64,
    ) -> usize {
        // Instrument: every fanned-out commit increments zero.replication.commit.
        self.commit_counter.add(1.0);
        self.fanout.publish(CommitNotification {
            watermark: watermark.into(),
            schema_changed,
            num_change_log_entries,
        })
    }

    /// Connection side: given a commit `notification` this connection just
    /// received, and the connection's `tracked` queries + the `since_watermark`
    /// it last poked to, compute the poke to send (or `None` if the commit
    /// invalidates nothing this client holds). Delegates the actual
    /// change-log→invalidation→poke work to
    /// [`crate::commit_dispatch::pokes_for_commit`]; the notification's
    /// `watermark` is the new high-water this poke advances the client toward.
    #[allow(clippy::too_many_arguments)]
    pub fn poke_for_commit<F>(
        &self,
        db: &StatementRunner,
        notification: &CommitNotification,
        tracked: &[TrackedQuery],
        since_watermark: &str,
        base_version: &NullableCvrVersion,
        timestamp: Option<f64>,
        rehydrate: F,
    ) -> Result<Option<PokeMessages>, CommitDispatchError>
    where
        F: FnMut(&str) -> Vec<PatchToVersion>,
    {
        // The poke id is derived from the commit watermark so a client can
        // correlate the poke with the commit that produced it.
        let poke_id = format!("poke-{}", notification.watermark);
        let start = Instant::now();
        let result = pokes_for_commit(
            db,
            since_watermark,
            tracked,
            &poke_id,
            base_version,
            timestamp,
            rehydrate,
        );
        // Instrument: record poke-computation latency (zero.sync.poke-time).
        self.poke_time
            .record_ms(start.elapsed().as_secs_f64() * 1000.0);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::Ast;
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_sqlite::change_fanout::FanoutEvent;
    use zero_cache_sqlite::change_log::{ChangeLog, RowKey, CREATE_CHANGELOG_SCHEMA};
    use zero_cache_view_syncer::client_patch::{ClientPutRowPatch, ClientRowPatch, Patch};
    use zero_cache_view_syncer::cvr_types::RowId;
    use zero_cache_view_syncer::cvr_version::CvrVersion;

    fn db_with_changelog() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db
    }

    fn rk(id: i64) -> RowKey {
        vec![("id".to_string(), JsonValue::Number(id as f64))]
    }

    fn put_patch() -> PatchToVersion {
        PatchToVersion {
            patch: Patch::Row(ClientRowPatch::Put(ClientPutRowPatch {
                id: RowId {
                    schema: "public".into(),
                    table: "issues".into(),
                    row_key: BTreeMap::from([("id".to_string(), JsonValue::String("1".into()))]),
                },
                contents: vec![("title".to_string(), JsonValue::String("filed".into()))],
            })),
            to_version: CvrVersion {
                state_version: "02".into(),
                config_version: None,
            },
        }
    }

    /// The whole assembled top-level flow, in one process: the replicator
    /// records a commit's row change in the change-log and `publish_commit`s
    /// it; a connection subscriber receives the notification off the fan-out
    /// and turns it into a client poke carrying the re-hydrated row — proving
    /// the replicator and the view-syncer connection are driven by the same
    /// commit stream through the shared service.
    #[tokio::test]
    async fn replicator_commit_flows_through_service_to_a_connection_poke() {
        let service = SyncService::new(16);
        let db = db_with_changelog();

        // A connection subscribes (having caught up to watermark "01").
        let mut conn = service.subscribe();
        let tracked = vec![TrackedQuery {
            hash: "h1".into(),
            ast: Ast::table("issues"),
            got: true,
        }];

        // Replicator side: a commit at "02" changed `issues`; record it in the
        // change-log, then fan the commit out.
        ChangeLog::new(&db)
            .log_set_op("02", 0, "issues", &rk(1), None)
            .unwrap();
        let delivered = service.publish_commit("02", false, 1);
        assert_eq!(
            delivered, 1,
            "one subscribed connection received the commit"
        );

        // Connection side: receive the fan-out event and compute the poke.
        let FanoutEvent::Commit(note) = conn.recv().await else {
            panic!("expected a Commit event");
        };
        assert_eq!(note.watermark, "02");

        let poke = service
            .poke_for_commit(&db, &note, &tracked, "01", &None, Some(1.0), |hash| {
                assert_eq!(hash, "h1");
                vec![put_patch()]
            })
            .unwrap()
            .expect("the commit invalidated a got query, so a poke is produced");

        // The poke id ties back to the commit, and it carries the row.
        assert_eq!(poke.start.poke_id, "poke-02");
        let rows = poke.part.rows_patch.expect("rows patch present");
        assert_eq!(rows.len(), 1);
        assert_eq!(poke.end.cookie, "02");
    }

    #[tokio::test]
    async fn publish_commit_increments_the_replication_commit_metric() {
        // A caller-supplied metrics registry (the OTel-backend seam) is wired
        // through SyncService and incremented on every fanned-out commit.
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Arc::new(Metrics::new(backend.clone()));
        let service = SyncService::with_metrics(16, metrics);

        service.publish_commit("01", false, 1);
        service.publish_commit("02", false, 2);
        service.publish_commit("03", true, 0);

        assert_eq!(
            backend.counter_value("zero.replication.commit"),
            3.0,
            "each fanned-out commit is counted under the OTel metric name"
        );
    }

    #[tokio::test]
    async fn poke_for_commit_records_a_poke_time_latency_observation() {
        let backend = Arc::new(InMemoryBackend::new());
        let metrics = Arc::new(Metrics::new(backend.clone()));
        let service = SyncService::with_metrics(16, metrics);
        let db = db_with_changelog();

        // A commit touching a got query's table -> a poke is computed (and timed).
        ChangeLog::new(&db)
            .log_set_op("02", 0, "issues", &rk(1), None)
            .unwrap();
        let note = CommitNotification {
            watermark: "02".into(),
            schema_changed: false,
            num_change_log_entries: 1,
        };
        let tracked = vec![TrackedQuery {
            hash: "h1".into(),
            ast: Ast::table("issues"),
            got: true,
        }];
        service
            .poke_for_commit(&db, &note, &tracked, "01", &None, Some(1.0), |_| {
                vec![put_patch()]
            })
            .unwrap();

        // One latency observation was recorded, and it renders as a histogram.
        assert_eq!(
            backend.observations("zero.sync.poke-time").len(),
            1,
            "poke computation was timed"
        );
        let text = backend.render_prometheus();
        assert!(
            text.contains("zero_sync_poke_time_count 1\n"),
            "histogram exported to Prometheus:\n{text}"
        );
    }

    #[tokio::test]
    async fn commit_unrelated_to_a_connection_yields_no_poke() {
        let service = SyncService::new(16);
        let db = db_with_changelog();
        let mut conn = service.subscribe();
        let tracked = vec![TrackedQuery {
            hash: "h1".into(),
            ast: Ast::table("issues"),
            got: true,
        }];

        ChangeLog::new(&db)
            .log_set_op("02", 0, "unrelated", &rk(1), None)
            .unwrap();
        service.publish_commit("02", false, 1);

        let FanoutEvent::Commit(note) = conn.recv().await else {
            panic!("expected a Commit event");
        };
        let poke = service
            .poke_for_commit(&db, &note, &tracked, "01", &None, Some(1.0), |_| {
                panic!("re-hydration must not run for an unrelated commit")
            })
            .unwrap();
        assert!(poke.is_none());
    }
}
