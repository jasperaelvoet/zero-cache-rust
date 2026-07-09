//! The change-streamer read/apply/feedback service loop — the orchestration
//! that ties the three ongoing-replication primitives into one running service:
//!   * read: `ReplicationStream::next_event` (raw `START_REPLICATION` frames),
//!   * apply: `ReplicationApplier::apply_message` (persist changes to the SQLite
//!     replica + change-log), and
//!   * feedback: `ReplicationStream::send_standby_status_update` (advance the
//!     slot's `confirmed_flush_lsn` so upstream WAL is released).
//!
//! This is the core of what upstream's `ChangeStreamerService` runs: consume
//! the logical-replication stream forever, applying each transaction and
//! flushing feedback so the slot tracks the replica's progress. It handles
//! server keepalives (replying when `reply_requested`) and tracks the high-water
//! LSN across data/keepalive frames.
//!
//! Scope: this is the single-consumer apply+feedback loop. The durable
//! change-log *fan-out* to multiple view-syncer subscribers (`Storer` +
//! `Subscriber` catchup) sits above this — those subscribe to the change-log
//! this loop writes via the `ReplicationApplier`/`ChangeDispatcher`, rather than
//! re-reading the Postgres stream. The pure back-pressure/watermark primitives
//! for that fan-out are already ported (`zero-cache-services::
//! subscriber_backpressure`).

use zero_cache_change_source::replication_conn::{
    ReplicationError, ReplicationEvent, ReplicationStream,
};

use crate::change_dispatcher::CommitResult;
use crate::replication_apply::{ApplyError, ReplicationApplier};

#[derive(Debug, thiserror::Error)]
pub enum ChangeStreamError {
    #[error(transparent)]
    Replication(#[from] ReplicationError),
    #[error(transparent)]
    Apply(#[from] ApplyError),
}

/// What to do after applying a committed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopControl {
    /// Keep consuming the stream.
    Continue,
    /// Stop the loop cleanly (e.g. the caller reached a target watermark or is
    /// shutting down).
    Stop,
}

/// Runs the change-stream service loop until the stream ends, an error occurs,
/// or `on_commit` returns [`LoopControl::Stop`].
///
/// For each frame: the high-water LSN is advanced; data messages are applied
/// via `applier`; when a transaction commits, feedback is flushed up to the
/// high-water LSN (releasing upstream WAL) and `on_commit` is invoked with the
/// [`CommitResult`]. Server keepalives with `reply_requested` are answered with
/// feedback too. `timestamp_micros` is passed through to feedback (pass `0` to
/// let the server use its own clock; a caller with a real clock can supply
/// `ReplicationStream::pg_timestamp_from_unix_micros(...)`).
pub async fn run_change_stream<F>(
    stream: &mut ReplicationStream,
    applier: &mut ReplicationApplier<'_>,
    timestamp_micros: i64,
    mut on_commit: F,
) -> Result<(), ChangeStreamError>
where
    F: FnMut(&CommitResult) -> LoopControl,
{
    let mut high_water: u64 = 0;
    loop {
        let Some(event) = stream.next_event().await? else {
            break; // clean CopyDone / stream end
        };
        match event {
            ReplicationEvent::Data {
                end_lsn, message, ..
            } => {
                high_water = high_water.max(end_lsn);
                if let Some(commit) = applier.apply_message(&message)? {
                    // Flush feedback up to the committed high-water LSN.
                    stream
                        .send_standby_status_update(
                            high_water,
                            high_water,
                            high_water,
                            timestamp_micros,
                            false,
                        )
                        .await?;
                    if on_commit(&commit) == LoopControl::Stop {
                        break;
                    }
                }
            }
            ReplicationEvent::Keepalive {
                end_lsn,
                reply_requested,
            } => {
                high_water = high_water.max(end_lsn);
                if reply_requested {
                    stream
                        .send_standby_status_update(
                            high_water,
                            high_water,
                            high_water,
                            timestamp_micros,
                            false,
                        )
                        .await?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::initial_sync::{run_initial_sync_introspected, SlotInfo};
    use crate::{StatementRunner, Value};
    use zero_cache_change_source::pg_connection;
    use zero_cache_change_source::replication_conn::ReplicationConn;

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
        db.query_uncached("SELECT id FROM csl_test ORDER BY id", &[])
            .unwrap()
            .iter()
            .map(|r| match r[0].1 {
                Value::Integer(n) => n,
                ref v => panic!("unexpected {v:?}"),
            })
            .collect()
    }

    /// Live: run the full service loop over two upstream transactions, stopping
    /// after the second commit, and confirm the replica converged and the slot
    /// advanced (feedback was flushed by the loop itself).
    #[tokio::test]
    async fn live_loop_applies_transactions_and_flushes_feedback() {
        let Ok(pg) = pg_connection::connect(&conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS csl_test CASCADE; \
             CREATE TABLE csl_test(id int primary key); \
             DROP PUBLICATION IF EXISTS csl_pub; \
             CREATE PUBLICATION csl_pub FOR TABLE csl_test;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('csl_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'csl_slot');",
        )
        .await
        .ok();

        let (host, port) = host_port();
        let mut create_conn = ReplicationConn::connect(&host, port, "postgres", "postgres", None)
            .await
            .unwrap();
        let slot = create_conn
            .create_logical_replication_slot("csl_slot")
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
            &["csl_pub".to_string()],
        )
        .await
        .unwrap();
        drop(create_conn);

        let stream_conn = ReplicationConn::connect(&host, port, "postgres", "postgres", None)
            .await
            .unwrap();
        let mut stream = stream_conn
            .start_replication("csl_slot", "csl_pub", &slot.consistent_point)
            .await
            .unwrap();

        // Two separate upstream transactions.
        pg.batch_execute("INSERT INTO csl_test(id) VALUES (1)")
            .await
            .unwrap();
        pg.batch_execute("INSERT INTO csl_test(id) VALUES (2)")
            .await
            .unwrap();

        let mut applier = ReplicationApplier::new(&db).unwrap();
        let mut commits = 0;
        let mut last_watermark = String::new();
        // Stop once we've applied both rows.
        let run = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            run_change_stream(&mut stream, &mut applier, 0, |commit| {
                commits += 1;
                last_watermark = commit.watermark.clone();
                if ids(&db) == vec![1, 2] {
                    LoopControl::Stop
                } else {
                    LoopControl::Continue
                }
            }),
        )
        .await
        .expect("loop timed out");
        run.unwrap();

        assert!(commits >= 2, "applied both transactions, got {commits}");
        assert_eq!(ids(&db), vec![1, 2]);
        assert!(
            !last_watermark.is_empty(),
            "a commit watermark was recorded"
        );

        // The loop flushed feedback, so the slot advanced past 0/0.
        let advanced = pg
            .query_one(
                "SELECT confirmed_flush_lsn > '0/0'::pg_lsn AS a \
                 FROM pg_replication_slots WHERE slot_name = 'csl_slot'",
                &[],
            )
            .await
            .unwrap();
        assert!(
            advanced.get::<_, bool>("a"),
            "slot advanced via loop feedback"
        );

        drop(stream);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('csl_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION csl_pub; DROP TABLE csl_test;")
            .await
            .unwrap();
    }

    /// Live: wires `run_change_stream`'s `on_commit` seam into a
    /// [`ChangeFanout`], proving the replication loop drives subscriber
    /// fan-out end-to-end. `CommitResult` (what the loop yields) and
    /// `CommitNotification` (what the fanout publishes) have matching fields,
    /// so the `on_commit` closure translates one to the other and publishes;
    /// a subscriber then receives the `Commit` event with the same watermark
    /// the loop recorded. This connects two independently-ported pieces (the
    /// change-stream loop and the fan-out hub) that nothing previously
    /// exercised together — the "replication commit → view-syncer subscriber"
    /// hand-off.
    #[tokio::test]
    async fn live_loop_fans_out_commits_to_a_subscriber() {
        use crate::change_fanout::{ChangeFanout, CommitNotification, FanoutEvent};

        let Ok(pg) = pg_connection::connect(&conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS cslfan_test CASCADE; \
             CREATE TABLE cslfan_test(id int primary key); \
             DROP PUBLICATION IF EXISTS cslfan_pub; \
             CREATE PUBLICATION cslfan_pub FOR TABLE cslfan_test;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('cslfan_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'cslfan_slot');",
        )
        .await
        .ok();

        let (host, port) = host_port();
        let mut create_conn = ReplicationConn::connect(&host, port, "postgres", "postgres", None)
            .await
            .unwrap();
        let slot = create_conn
            .create_logical_replication_slot("cslfan_slot")
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
            &["cslfan_pub".to_string()],
        )
        .await
        .unwrap();
        drop(create_conn);

        let stream_conn = ReplicationConn::connect(&host, port, "postgres", "postgres", None)
            .await
            .unwrap();
        let mut stream = stream_conn
            .start_replication("cslfan_slot", "cslfan_pub", &slot.consistent_point)
            .await
            .unwrap();

        pg.batch_execute("INSERT INTO cslfan_test(id) VALUES (1)")
            .await
            .unwrap();

        // A fan-out hub with one live subscriber.
        let fanout = ChangeFanout::new(16);
        let mut subscriber = fanout.subscribe();

        let mut applier = ReplicationApplier::new(&db).unwrap();
        let mut loop_watermark = String::new();
        let run = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            run_change_stream(&mut stream, &mut applier, 0, |commit| {
                loop_watermark = commit.watermark.clone();
                // The on_commit seam publishes to the fan-out hub.
                fanout.publish(CommitNotification {
                    watermark: commit.watermark.clone(),
                    schema_changed: commit.schema_changed,
                    num_change_log_entries: commit.num_change_log_entries,
                });
                LoopControl::Stop
            }),
        )
        .await
        .expect("loop timed out");
        run.unwrap();

        let replica_ids: Vec<i64> = db
            .query_uncached("SELECT id FROM cslfan_test ORDER BY id", &[])
            .unwrap()
            .iter()
            .map(|r| match r[0].1 {
                Value::Integer(n) => n,
                ref v => panic!("unexpected {v:?}"),
            })
            .collect();
        assert_eq!(replica_ids, vec![1]);
        assert!(!loop_watermark.is_empty());

        // The subscriber receives exactly the commit the loop fanned out.
        match subscriber.try_recv() {
            Some(FanoutEvent::Commit(note)) => {
                assert_eq!(
                    note.watermark, loop_watermark,
                    "subscriber sees the same watermark the loop committed"
                );
                assert_eq!(note.num_change_log_entries, 1, "one inserted row");
            }
            other => panic!("expected a fanned-out Commit event, got {other:?}"),
        }

        drop(stream);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('cslfan_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION cslfan_pub; DROP TABLE cslfan_test;")
            .await
            .unwrap();
    }
}
