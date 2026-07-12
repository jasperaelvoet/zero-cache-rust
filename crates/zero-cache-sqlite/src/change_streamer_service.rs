//! `ChangeStreamerService` — the running-service object that ties the ongoing
//! replication loop to the live subscriber fan-out.
//!
//! This is the composition upstream calls `ChangeStreamerService`: one process
//! consumes the single Postgres replication stream, applies each transaction to
//! the SQLite replica + durable change-log, advances the slot via feedback, and
//! **fans each commit out to every subscribed view-syncer**. Subscribers never
//! touch Postgres — they catch up from the change-log (`ChangeLog::read_since`)
//! and then follow this service's [`ChangeFanout`].
//!
//! It composes [`crate::change_stream_loop::run_change_stream`] (read → apply →
//! feedback) with [`crate::change_fanout::ChangeFanout`] (fan-out): the loop's
//! `on_commit` hook publishes each [`CommitResult`] to the hub. No new protocol
//! or transaction logic lives here — it is the wiring that makes the pieces a
//! service.

use zero_cache_change_source::replication_conn::ReplicationStream;

use crate::change_dispatcher::CommitResult;
use crate::change_fanout::{ChangeFanout, FanoutSubscriber};
use crate::change_stream_loop::{run_change_stream, ChangeStreamError, LoopControl};
use crate::replication_apply::ReplicationApplier;

/// The running change-streamer service: owns the fan-out hub subscribers
/// register with, and drives the replication stream into it.
pub struct ChangeStreamerService {
    fanout: ChangeFanout,
}

impl ChangeStreamerService {
    /// Creates a service whose fan-out buffers up to `fanout_capacity` commits
    /// per subscriber before a slow one is told to re-catch-up.
    pub fn new(fanout_capacity: usize) -> Self {
        ChangeStreamerService {
            fanout: ChangeFanout::new(fanout_capacity),
        }
    }

    /// Registers a new live subscriber. As with [`ChangeFanout::subscribe`], it
    /// only receives commits published after this call — the caller catches up
    /// from the change-log up to the current watermark first, then follows.
    pub fn subscribe(&self) -> FanoutSubscriber {
        self.fanout.subscribe()
    }

    /// Number of live subscribers currently attached.
    pub fn subscriber_count(&self) -> usize {
        self.fanout.subscriber_count()
    }

    /// Runs the service: consumes `stream`, applies each transaction via
    /// `applier`, flushes slot feedback, and fans every commit out to
    /// subscribers. `should_continue` is called after each commit is published;
    /// return [`LoopControl::Stop`] to end the service cleanly (e.g. on
    /// shutdown or a target watermark). Runs until the stream ends, an error
    /// occurs, or `should_continue` stops it.
    pub async fn run<F>(
        &self,
        stream: &mut ReplicationStream,
        applier: &mut ReplicationApplier<'_>,
        timestamp_micros: i64,
        mut should_continue: F,
    ) -> Result<(), ChangeStreamError>
    where
        F: FnMut(&CommitResult) -> LoopControl,
    {
        let fanout = &self.fanout;
        run_change_stream(stream, applier, timestamp_micros, |commit| {
            // Fan the commit out to all subscribers, then consult the caller's
            // stop condition.
            fanout.publish(commit.into());
            should_continue(commit)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_fanout::{CommitNotification, FanoutEvent};
    use crate::initial_sync::{run_initial_sync_introspected, SlotInfo};
    use crate::{StatementRunner, Value};
    use zero_cache_change_source::pg_connection;
    use zero_cache_change_source::replication_conn::{PgSslMode, ReplicationConn};

    fn conn_str() -> String {
        std::env::var("ZERO_TEST_PG")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into())
    }
    fn host_port() -> (String, u16) {
        let url = std::env::var("ZERO_TEST_PG_TCP").unwrap_or_else(|_| "localhost:54329".into());
        let (h, p) = url.split_once(':').unwrap();
        (h.to_string(), p.parse().unwrap())
    }
    fn ids(db: &StatementRunner) -> Vec<i64> {
        db.query_uncached("SELECT id FROM css_test ORDER BY id", &[])
            .unwrap()
            .iter()
            .map(|r| match r[0].1 {
                Value::Integer(n) => n,
                ref v => panic!("unexpected {v:?}"),
            })
            .collect()
    }

    /// Live: two subscribers attach, the service consumes two upstream
    /// transactions (applying to the replica AND fanning out), and both
    /// subscribers receive both commit notifications.
    #[tokio::test]
    async fn live_service_applies_and_fans_out_to_subscribers() {
        let Ok(pg) = pg_connection::connect(&conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS css_test CASCADE; \
             CREATE TABLE css_test(id int primary key); \
             DROP PUBLICATION IF EXISTS css_pub; \
             CREATE PUBLICATION css_pub FOR TABLE css_test;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('css_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'css_slot');",
        )
        .await
        .ok();

        let (host, port) = host_port();
        let mut create_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let slot = create_conn
            .create_logical_replication_slot("css_slot")
            .await
            .unwrap();

        let copy_conn = pg_connection::connect(&conn_str()).await.unwrap();
        let db = StatementRunner::open_in_memory().unwrap();
        run_initial_sync_introspected(
            &copy_conn,
            &db,
            &SlotInfo {
                consistent_point: slot.consistent_point.clone(),
                snapshot_name: slot.snapshot_name.clone(),
            },
            &["css_pub".to_string()],
        )
        .await
        .unwrap();
        drop(create_conn);

        let stream_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let mut stream = stream_conn
            .start_replication("css_slot", "css_pub", &slot.consistent_point)
            .await
            .unwrap();

        let service = ChangeStreamerService::new(64);
        let mut sub_a = service.subscribe();
        let mut sub_b = service.subscribe();
        assert_eq!(service.subscriber_count(), 2);

        // Two upstream transactions AFTER subscribers attached.
        pg.batch_execute("INSERT INTO css_test(id) VALUES (1)")
            .await
            .unwrap();
        pg.batch_execute("INSERT INTO css_test(id) VALUES (2)")
            .await
            .unwrap();

        let mut applier = ReplicationApplier::new(&db).unwrap();
        let mut published = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            service.run(&mut stream, &mut applier, 0, |commit| {
                published.push(commit.watermark.clone());
                if ids(&db) == vec![1, 2] {
                    LoopControl::Stop
                } else {
                    LoopControl::Continue
                }
            }),
        )
        .await
        .expect("service timed out")
        .unwrap();

        assert_eq!(ids(&db), vec![1, 2], "replica updated by the service");
        assert!(published.len() >= 2);

        // Both subscribers received both commit notifications (order preserved).
        for sub in [&mut sub_a, &mut sub_b] {
            let mut got: Vec<CommitNotification> = Vec::new();
            while let Some(ev) = sub.try_recv() {
                match ev {
                    FanoutEvent::Commit(c) => got.push(c),
                    other => panic!("unexpected fanout event {other:?}"),
                }
            }
            assert!(
                got.len() >= 2,
                "subscriber received both commits, got {}",
                got.len()
            );
            // The published watermarks match what the loop reported.
            let watermarks: Vec<String> = got.iter().map(|c| c.watermark.clone()).collect();
            assert_eq!(&watermarks[..2], &published[..2]);
        }

        drop(stream);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('css_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION css_pub; DROP TABLE css_test;")
            .await
            .unwrap();
    }
}
