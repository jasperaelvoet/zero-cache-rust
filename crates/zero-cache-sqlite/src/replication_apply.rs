//! The ongoing-replication apply loop — the change-application half of the
//! pipeline that consumes the logical-replication stream *after* initial sync
//! and keeps the SQLite replica up to date.
//!
//! This ties together three pieces built in prior rounds that had no consumer
//! wiring them into a running apply loop:
//!   * `zero-cache-change-source`'s `ReplicationStream` (raw `START_REPLICATION`
//!     frames) + `RelationTracker::translate` (pgoutput → `Change`), and
//!   * this crate's `ChangeDispatcher` (applies a `Change` to the replica
//!     inside a transaction and records change-log entries under a watermark).
//!
//! Transaction/version handling mirrors upstream's change-processor: a pgoutput
//! `Begin` carries the transaction's `final_lsn` (the eventual commit LSN), so
//! the dispatcher transaction is opened at that version up front (change-log
//! entries are stamped with it as they are applied); the matching `Commit`
//! closes the transaction at the same watermark. The LSN → replica version
//! conversion reuses `lsn::from_bigint` + `to_state_version_string`, the same
//! path initial sync uses for the slot's consistent point, so streamed
//! watermarks are continuous with the initial-sync watermark.
//!
//! Scope: this is the apply loop only — it does NOT own the network read side
//! (the caller pumps `ReplicationStream::next_event` and hands the decoded
//! pgoutput messages here) nor the change-streamer's fan-out to subscribers /
//! durable change-log service (`ChangeStreamerImpl`/`Storer`), which sit above
//! this. Keepalive/standby-status replies are also the caller's concern.

use num_bigint::BigInt;

use zero_cache_change_source::pg_schema_diff::relation_message_drift;
use zero_cache_change_source::pg_to_change::{DdlOutcome, RelationTracker, TranslateError};
use zero_cache_change_source::pgoutput::{self, DecodeError, PgoutputMessage};
use zero_cache_change_source::replication_conn::{
    ReplicationError, ReplicationEvent, ReplicationStream,
};
use zero_cache_types::specs::PublishedTableSpec;

use crate::change_dispatcher::{ChangeDispatcher, CommitResult, DispatchError};
use crate::{DbError, StatementRunner};

#[derive(Debug, thiserror::Error)]
pub enum ApplyError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    Dispatch(#[from] DispatchError),
    #[error(transparent)]
    Translate(#[from] TranslateError),
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error(transparent)]
    Replication(#[from] ReplicationError),
}

/// Converts a pgoutput LSN (a raw `u64`) to the replica version string, via the
/// `X/Y` hex form (`from_bigint`) and `to_state_version_string` — the same
/// conversion initial sync applies to the slot's `consistent_point`.
fn version_from_lsn(lsn: u64) -> Result<String, ApplyError> {
    let hex = zero_cache_types::lsn::from_bigint(&BigInt::from(lsn));
    zero_cache_types::lsn::to_state_version_string(&hex)
        .map_err(|e| ApplyError::Db(DbError(format!("bad replication LSN {lsn}: {e}"))))
}

/// Drives a [`ChangeDispatcher`] from a stream of decoded pgoutput messages,
/// keeping a [`RelationTracker`] for relation metadata across the stream.
///
/// Feed each message (in stream order) to [`apply_message`](Self::apply_message).
/// A `Begin` opens a transaction; data messages apply within it; a `Commit`
/// closes it and returns the [`CommitResult`] (watermark + change-log stats).
pub struct ReplicationApplier<'a> {
    dispatcher: ChangeDispatcher<'a>,
    relations: RelationTracker,
    /// Set when a streamed DDL message describes a schema change the port can't
    /// apply incrementally (e.g. one needing a backfill). The drive loop drains
    /// this and reports it as [`ApplyLoopOutcome::drift`] so the supervisor
    /// resyncs. See [`RelationTracker::ddl_outcome`].
    resync_signal: Option<String>,
}

impl<'a> ReplicationApplier<'a> {
    pub fn new(db: &'a StatementRunner) -> Result<Self, DbError> {
        Ok(ReplicationApplier {
            dispatcher: ChangeDispatcher::new(db)?,
            relations: RelationTracker::new(),
            resync_signal: None,
        })
    }

    /// Enables inline DDL replication for the shard `{app_id}/{shard_num}`:
    /// captured `{app_id}/{shard_num}/ddl` logical messages (emitted by the
    /// event triggers) are decoded into schema changes and applied inline,
    /// instead of only being detected out-of-band by the schema-hash poll.
    /// Threaded in from the shard config where pgoutput is consumed.
    ///
    /// NOTE: production wiring (passing the shard's `app_id`/`shard_num` here
    /// from `replicator_service`) is the remaining follow-up; until then DDL
    /// still falls back to the resync-on-drift path. TODO.
    pub fn set_shard(&mut self, app_id: &str, shard_num: i64) {
        self.relations.set_ddl_prefix(app_id, shard_num);
    }

    /// Drains a pending resync signal raised while applying a streamed DDL
    /// message (see [`Self::set_shard`]). The drive loop calls this after each
    /// applied message and, if `Some`, stops with a drift/resync reason.
    pub fn take_resync_signal(&mut self) -> Option<String> {
        self.resync_signal.take()
    }

    /// Whether a replication transaction is currently open.
    pub fn in_transaction(&self) -> bool {
        self.dispatcher.in_transaction()
    }

    /// Applies one decoded pgoutput message. Returns `Some(CommitResult)` when
    /// the message was the `Commit` that closed a transaction, else `None`.
    ///
    /// * `Begin { final_lsn }` → open the dispatcher transaction at the
    ///   commit version derived from `final_lsn`.
    /// * `Relation` → recorded in the tracker (no replica write).
    /// * `Insert`/`Update`/`Delete`/`Truncate` → translated to a `Change` and
    ///   applied within the open transaction.
    /// * `Commit { commit_lsn, .. }` → commit at the watermark derived from
    ///   `commit_lsn`.
    /// * `Unsupported` → ignored (matching `translate`).
    pub fn apply_message(
        &mut self,
        msg: &PgoutputMessage,
    ) -> Result<Option<CommitResult>, ApplyError> {
        match msg {
            PgoutputMessage::Begin { final_lsn, .. } => {
                let version = version_from_lsn(*final_lsn)?;
                self.dispatcher.begin(&version)?;
                Ok(None)
            }
            PgoutputMessage::Commit { commit_lsn, .. } => {
                let watermark = version_from_lsn(*commit_lsn)?;
                let result = self.dispatcher.commit(&watermark)?;
                Ok(Some(result))
            }
            other => {
                // A captured `{app}/{shard}/ddl` logical message (when a shard
                // is configured) decodes into inline schema changes; anything
                // else falls through to the normal data path.
                if let Some(outcome) = self.relations.ddl_outcome(other)? {
                    match outcome {
                        DdlOutcome::Changes(changes) => {
                            for change in &changes {
                                self.dispatcher.apply(change)?;
                            }
                        }
                        DdlOutcome::Resync(reason) => {
                            // Leave the open transaction for the drive loop to
                            // roll back; it drains this and reports drift.
                            self.resync_signal = Some(reason);
                        }
                    }
                    return Ok(None);
                }
                // Relation updates the tracker's cache and yields no Change;
                // data messages yield a Change to apply.
                if let Some(change) = self.relations.translate(other)? {
                    self.dispatcher.apply(&change)?;
                }
                Ok(None)
            }
        }
    }

    /// Decodes one raw pgoutput frame (`XLogData` payload) and applies it,
    /// closing the last seam between the wire bytes and the replica: the
    /// network read side hands raw pgoutput bytes, and this decodes them via
    /// [`pgoutput::decode`] before dispatching through
    /// [`apply_message`](Self::apply_message). Returns `Some(CommitResult)`
    /// when the frame was the `Commit` that closed a transaction.
    pub fn apply_frame(&mut self, data: &[u8]) -> Result<Option<CommitResult>, ApplyError> {
        let msg = pgoutput::decode(data)?;
        self.apply_message(&msg)
    }

    /// Aborts any open transaction (e.g. on a stream error before `Commit`).
    pub fn rollback(&mut self) -> Result<(), ApplyError> {
        self.dispatcher.rollback()?;
        Ok(())
    }
}

/// The result of a [`drive_apply_loop`] run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyLoopOutcome {
    /// The number of transactions committed to the replica this run.
    pub commits: usize,
    /// `Some(reason)` if the loop stopped because a streamed `Relation` message
    /// drifted from the published `specs` (an upstream schema change) — the
    /// caller should trigger a re-sync. `None` if the loop stopped for any
    /// other reason (stream ended or `should_stop`).
    pub drift: Option<String>,
}

/// Drives the ongoing-replication apply loop against a live logical-replication
/// [`ReplicationStream`]: it pumps decoded events into `applier`, and — this is
/// the piece a real service adds over [`ReplicationApplier`] alone — after each
/// COMMITTED transaction it sends a standby status update flushing up to that
/// commit's WAL end LSN, so Postgres can advance the replication slot. Without
/// that feedback the upstream WAL would accumulate unboundedly.
///
/// The flush LSN only ever advances to a committed transaction's `end_lsn`
/// (durable in the SQLite replica), never mid-transaction, so the slot never
/// moves past data the replica hasn't durably applied. A keepalive that
/// requests a reply is answered with the received position as `write` and the
/// last committed position as `flush`/`apply`.
///
/// Runs until the stream ends (`next_event` yields `None`), `should_stop`
/// returns `true` after a commit, or a streamed `Relation` message drifts from
/// `specs` (schema change upstream) — the drift case is reported in the
/// returned [`ApplyLoopOutcome::drift`] so a service can trigger a re-sync
/// instead of applying data against a stale schema. `specs` is the published
/// table schema the replica was built from (pass `&[]` to skip drift checking).
pub async fn drive_apply_loop<F>(
    stream: &mut ReplicationStream,
    applier: &mut ReplicationApplier<'_>,
    specs: &[PublishedTableSpec],
    should_stop: F,
) -> Result<ApplyLoopOutcome, ApplyError>
where
    F: FnMut(&CommitResult) -> bool,
{
    drive_apply_loop_with_message_observer(stream, applier, specs, should_stop, |_, _| {}, None)
        .await
}

/// Unix-epoch milliseconds, for stamping WAL-message receipt in the lag path.
fn unix_millis_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

/// [`drive_apply_loop`] plus a `message_observer` invoked for every pgoutput
/// Logical Decoding Message (`pg_logical_emit_message`) that flows through the
/// stream, with `(message, receive_time_unix_ms)`. Upstream's replication-lag
/// reporter round-trips a WAL message and measures the delay between emit and
/// this receipt; the observer is where that arrival is timestamped. The
/// message itself is otherwise a no-op for replication (it carries no row
/// data), matching `apply_message`'s handling.
///
/// H5 (interim safeguard): `schema_change_signal`, when `Some`, is polled on
/// every stream event (both data and keepalive). If it is set, the loop stops
/// with `drift = Some(..)` so the supervisor resyncs — this is the escape hatch
/// for a DDL that ships NO following DML (and therefore no new pgoutput
/// `Relation` message to trip `relation_message_drift`). A background
/// schema-hash poll in the replicator service sets the flag; checking it on
/// keepalives is what lets an otherwise-idle stream notice the change. This is
/// NOT a substitute for real EVENT TRIGGER DDL replication (commit-ordering of
/// the DDL is still lost); TODO: port `change-source/pg/schema/ddl.ts`'s event
/// triggers so DDL streams inline as change messages.
pub async fn drive_apply_loop_with_message_observer<F, M>(
    stream: &mut ReplicationStream,
    applier: &mut ReplicationApplier<'_>,
    specs: &[PublishedTableSpec],
    mut should_stop: F,
    mut message_observer: M,
    schema_change_signal: Option<&std::sync::atomic::AtomicBool>,
) -> Result<ApplyLoopOutcome, ApplyError>
where
    F: FnMut(&CommitResult) -> bool,
    M: FnMut(&PgoutputMessage, i64),
{
    use std::sync::atomic::Ordering;

    let mut commits = 0usize;
    let mut flush_lsn = 0u64;
    let mut drift = None;

    // H5: a DDL with no following DML emits no Relation message; the background
    // schema-hash poll flips this flag so an idle stream still resyncs.
    let schema_poll_tripped = |drift: &mut Option<String>| -> bool {
        if schema_change_signal.is_some_and(|s| s.load(Ordering::SeqCst)) {
            *drift = Some(
                "schema-hash poll detected an upstream schema change with no \
                 accompanying Relation message (DML-less DDL)"
                    .to_string(),
            );
            true
        } else {
            false
        }
    };

    while let Some(event) = stream.next_event().await? {
        if schema_poll_tripped(&mut drift) {
            break;
        }
        match event {
            ReplicationEvent::Data {
                end_lsn, message, ..
            } => {
                // A drifted relation means the replica's schema is stale; stop
                // and let the caller re-sync rather than misapply the data.
                if let Some(reason) = relation_message_drift(&message, specs) {
                    drift = Some(reason);
                    break;
                }
                if matches!(message, PgoutputMessage::Message { .. }) {
                    message_observer(&message, unix_millis_now());
                }
                let commit = applier.apply_message(&message)?;
                // A streamed DDL that can't be applied incrementally raises a
                // resync signal; stop and let the caller rebuild the replica
                // (the same escape hatch as relation drift).
                if let Some(reason) = applier.take_resync_signal() {
                    drift = Some(reason);
                    break;
                }
                if let Some(commit) = commit {
                    commits += 1;
                    flush_lsn = flush_lsn.max(end_lsn);
                    // Acknowledge durability up to this commit so the slot advances.
                    stream
                        .send_standby_status_update(flush_lsn, flush_lsn, flush_lsn, 0, false)
                        .await?;
                    if should_stop(&commit) {
                        break;
                    }
                }
            }
            ReplicationEvent::Keepalive {
                end_lsn,
                reply_requested,
            } => {
                if reply_requested {
                    // Received up to `end_lsn`; durably applied up to `flush_lsn`.
                    stream
                        .send_standby_status_update(end_lsn, flush_lsn, flush_lsn, 0, false)
                        .await?;
                }
            }
        }
    }

    Ok(ApplyLoopOutcome { commits, drift })
}

#[cfg(test)]
mod hermetic_tests {
    //! End-to-end apply-loop coverage that needs NO live Postgres: synthetic
    //! pgoutput messages/frames are driven through the applier into an
    //! in-memory SQLite replica. (The live-Postgres integration lives in the
    //! sibling `tests` module.)
    use super::*;
    use crate::change_dispatcher::ChangeDispatcher;
    use crate::change_log::CREATE_CHANGELOG_SCHEMA;
    use crate::column_metadata::CREATE_COLUMN_METADATA_TABLE;
    use crate::table_metadata::CREATE_TABLE_METADATA_TABLE;
    use zero_cache_change_source::data::{Change, TableCreate};
    use zero_cache_change_source::pgoutput::{RelationColumn, ReplicaIdentity, TupleColumn};
    use zero_cache_types::specs::{ColumnSpec, TableSpec};

    /// LSN 0x100_0000; any valid LSN works — begin/commit reuse it so the
    /// transaction opens and closes at the same watermark.
    const LSN: u64 = 0x0100_0000;

    fn setup_db_with_issues_table() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_TABLE_METADATA_TABLE).unwrap();
        db.exec(CREATE_COLUMN_METADATA_TABLE).unwrap();
        // Create the `issues(id text pk)` table the streamed INSERT targets,
        // the way ongoing replication assumes it already exists post-initial-sync.
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        dispatcher.begin("01").unwrap();
        dispatcher
            .apply(&Change::CreateTable(TableCreate {
                spec: TableSpec {
                    name: "issues".into(),
                    schema: "public".into(),
                    columns: vec![("id".into(), ColumnSpec::new("text", 1))],
                    primary_key: Some(vec!["id".into()]),
                },
                metadata: None,
                backfill: None,
            }))
            .unwrap();
        dispatcher.commit("01").unwrap();
        db
    }

    /// Like [`setup_db_with_issues_table`] but a two-column `issues(id text pk,
    /// title text)` table, for exercising a value `Update`.
    fn setup_db_with_titled_issues_table() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_TABLE_METADATA_TABLE).unwrap();
        db.exec(CREATE_COLUMN_METADATA_TABLE).unwrap();
        let mut dispatcher = ChangeDispatcher::new(&db).unwrap();
        dispatcher.begin("01").unwrap();
        dispatcher
            .apply(&Change::CreateTable(TableCreate {
                spec: TableSpec {
                    name: "issues".into(),
                    schema: "public".into(),
                    columns: vec![
                        ("id".into(), ColumnSpec::new("text", 1)),
                        ("title".into(), ColumnSpec::new("text", 2)),
                    ],
                    primary_key: Some(vec!["id".into()]),
                },
                metadata: None,
                backfill: None,
            }))
            .unwrap();
        dispatcher.commit("01").unwrap();
        db
    }

    #[test]
    fn streamed_primary_key_changing_update_moves_the_row_end_to_end() {
        // A pgoutput Update that changes the key carries the OLD key tuple
        // (replica identity default sends a key-only `old`). The apply path
        // must relocate the row from the old key to the new one.
        let db = setup_db_with_issues_table();
        let mut applier = ReplicationApplier::new(&db).unwrap();
        let relation = PgoutputMessage::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "issues".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![RelationColumn {
                is_key: true,
                name: "id".into(),
                type_oid: 25,
                atttypmod: -1,
            }],
        };

        apply_txn_with_relation(
            &mut applier,
            &relation,
            &[PgoutputMessage::Insert {
                relation_id: 1,
                new: vec![TupleColumn::Text("a".into())],
            }],
        );
        apply_txn_with_relation(
            &mut applier,
            &relation,
            &[PgoutputMessage::Update {
                relation_id: 1,
                old: Some(vec![TupleColumn::Text("a".into())]),
                old_is_key_only: true,
                new: vec![TupleColumn::Text("b".into())],
            }],
        );

        let rows = db.query_uncached("SELECT id FROM issues", &[]).unwrap();
        assert_eq!(rows.len(), 1, "still exactly one row");
        assert_eq!(
            rows[0][0].1,
            crate::Value::Text("b".into()),
            "the row was relocated from key 'a' to key 'b'"
        );
    }

    /// Like [`apply_txn`] but with a caller-supplied `Relation` message (for
    /// tables whose relation shape differs from the default single-`id` one).
    fn apply_txn_with_relation(
        applier: &mut ReplicationApplier,
        relation: &PgoutputMessage,
        data: &[PgoutputMessage],
    ) {
        applier
            .apply_message(&PgoutputMessage::Begin {
                final_lsn: LSN,
                commit_timestamp: 0,
                xid: 1,
            })
            .unwrap();
        applier.apply_message(relation).unwrap();
        for msg in data {
            applier.apply_message(msg).unwrap();
        }
        applier
            .apply_message(&PgoutputMessage::Commit {
                commit_lsn: LSN,
                end_lsn: LSN,
                commit_timestamp: 0,
            })
            .unwrap();
    }

    #[test]
    fn streamed_update_changes_the_row_value_end_to_end() {
        let db = setup_db_with_titled_issues_table();
        let mut applier = ReplicationApplier::new(&db).unwrap();

        let titled_relation = PgoutputMessage::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "issues".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                RelationColumn {
                    is_key: true,
                    name: "id".into(),
                    type_oid: 25,
                    atttypmod: -1,
                },
                RelationColumn {
                    is_key: false,
                    name: "title".into(),
                    type_oid: 25,
                    atttypmod: -1,
                },
            ],
        };

        // Insert (id=a, title=old).
        applier
            .apply_message(&PgoutputMessage::Begin {
                final_lsn: LSN,
                commit_timestamp: 0,
                xid: 1,
            })
            .unwrap();
        applier.apply_message(&titled_relation).unwrap();
        applier
            .apply_message(&PgoutputMessage::Insert {
                relation_id: 1,
                new: vec![
                    TupleColumn::Text("a".into()),
                    TupleColumn::Text("old".into()),
                ],
            })
            .unwrap();
        applier
            .apply_message(&PgoutputMessage::Commit {
                commit_lsn: LSN,
                end_lsn: LSN,
                commit_timestamp: 0,
            })
            .unwrap();

        // Update the title for key id=a (key unchanged, replica identity default).
        applier
            .apply_message(&PgoutputMessage::Begin {
                final_lsn: LSN,
                commit_timestamp: 0,
                xid: 2,
            })
            .unwrap();
        applier.apply_message(&titled_relation).unwrap();
        applier
            .apply_message(&PgoutputMessage::Update {
                relation_id: 1,
                old: None,
                old_is_key_only: false,
                new: vec![
                    TupleColumn::Text("a".into()),
                    TupleColumn::Text("new".into()),
                ],
            })
            .unwrap();
        applier
            .apply_message(&PgoutputMessage::Commit {
                commit_lsn: LSN,
                end_lsn: LSN,
                commit_timestamp: 0,
            })
            .unwrap();

        let rows = db
            .query_uncached("SELECT title FROM issues WHERE id = 'a'", &[])
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].1, crate::Value::Text("new".into()));
    }

    #[test]
    fn streamed_insert_lands_in_the_replica_end_to_end() {
        let db = setup_db_with_issues_table();
        let mut applier = ReplicationApplier::new(&db).unwrap();

        assert!(applier
            .apply_message(&PgoutputMessage::Begin {
                final_lsn: LSN,
                commit_timestamp: 0,
                xid: 1,
            })
            .unwrap()
            .is_none());
        assert!(applier.in_transaction());
        applier
            .apply_message(&PgoutputMessage::Relation {
                relation_id: 1,
                namespace: "public".into(),
                name: "issues".into(),
                replica_identity: ReplicaIdentity::Default,
                columns: vec![RelationColumn {
                    is_key: true,
                    name: "id".into(),
                    type_oid: 25,
                    atttypmod: -1,
                }],
            })
            .unwrap();
        applier
            .apply_message(&PgoutputMessage::Insert {
                relation_id: 1,
                new: vec![TupleColumn::Text("a".into())],
            })
            .unwrap();
        let commit = applier
            .apply_message(&PgoutputMessage::Commit {
                commit_lsn: LSN,
                end_lsn: LSN,
                commit_timestamp: 0,
            })
            .unwrap()
            .expect("commit closes the transaction and returns a result");
        assert!(!applier.in_transaction());
        assert_eq!(commit.num_change_log_entries, 1);

        let rows = db.query_uncached("SELECT id FROM issues", &[]).unwrap();
        assert_eq!(rows.len(), 1, "the streamed row was applied to the replica");
    }

    /// Applies a `Relation` then the given data messages inside one
    /// Begin/Commit transaction, returning the commit result.
    fn apply_txn(applier: &mut ReplicationApplier, data: &[PgoutputMessage]) -> CommitResult {
        applier
            .apply_message(&PgoutputMessage::Begin {
                final_lsn: LSN,
                commit_timestamp: 0,
                xid: 1,
            })
            .unwrap();
        applier
            .apply_message(&PgoutputMessage::Relation {
                relation_id: 1,
                namespace: "public".into(),
                name: "issues".into(),
                replica_identity: ReplicaIdentity::Default,
                columns: vec![RelationColumn {
                    is_key: true,
                    name: "id".into(),
                    type_oid: 25,
                    atttypmod: -1,
                }],
            })
            .unwrap();
        for msg in data {
            applier.apply_message(msg).unwrap();
        }
        applier
            .apply_message(&PgoutputMessage::Commit {
                commit_lsn: LSN,
                end_lsn: LSN,
                commit_timestamp: 0,
            })
            .unwrap()
            .expect("commit returns a result")
    }

    #[test]
    fn streamed_delete_removes_the_row_end_to_end() {
        let db = setup_db_with_issues_table();
        let mut applier = ReplicationApplier::new(&db).unwrap();

        // Insert then delete the same key across two transactions.
        apply_txn(
            &mut applier,
            &[PgoutputMessage::Insert {
                relation_id: 1,
                new: vec![TupleColumn::Text("a".into())],
            }],
        );
        assert_eq!(
            db.query_uncached("SELECT id FROM issues", &[])
                .unwrap()
                .len(),
            1
        );

        apply_txn(
            &mut applier,
            &[PgoutputMessage::Delete {
                relation_id: 1,
                key: vec![TupleColumn::Text("a".into())],
                is_key_only: true,
            }],
        );
        assert_eq!(
            db.query_uncached("SELECT id FROM issues", &[])
                .unwrap()
                .len(),
            0,
            "the streamed delete removed the row from the replica"
        );
    }

    #[test]
    fn multi_row_transaction_applies_all_rows_and_counts_changelog_entries() {
        let db = setup_db_with_issues_table();
        let mut applier = ReplicationApplier::new(&db).unwrap();

        let commit = apply_txn(
            &mut applier,
            &[
                PgoutputMessage::Insert {
                    relation_id: 1,
                    new: vec![TupleColumn::Text("a".into())],
                },
                PgoutputMessage::Insert {
                    relation_id: 1,
                    new: vec![TupleColumn::Text("b".into())],
                },
                PgoutputMessage::Insert {
                    relation_id: 1,
                    new: vec![TupleColumn::Text("c".into())],
                },
            ],
        );
        assert_eq!(
            db.query_uncached("SELECT id FROM issues", &[])
                .unwrap()
                .len(),
            3,
            "all three streamed rows landed in one transaction"
        );
        assert_eq!(
            commit.num_change_log_entries, 3,
            "one change-log entry per row applied in the transaction"
        );
    }

    #[test]
    fn commit_result_watermark_derives_from_the_commit_lsn() {
        let db = setup_db_with_issues_table();
        let mut applier = ReplicationApplier::new(&db).unwrap();
        let commit = apply_txn(
            &mut applier,
            &[PgoutputMessage::Insert {
                relation_id: 1,
                new: vec![TupleColumn::Text("a".into())],
            }],
        );
        // The commit watermark is the commit LSN run through the same
        // LSN→version conversion initial sync uses.
        assert_eq!(commit.watermark, super::version_from_lsn(LSN).unwrap());
    }

    #[test]
    fn rollback_discards_uncommitted_changes_end_to_end() {
        let db = setup_db_with_issues_table();
        let mut applier = ReplicationApplier::new(&db).unwrap();

        applier
            .apply_message(&PgoutputMessage::Begin {
                final_lsn: LSN,
                commit_timestamp: 0,
                xid: 1,
            })
            .unwrap();
        applier
            .apply_message(&PgoutputMessage::Relation {
                relation_id: 1,
                namespace: "public".into(),
                name: "issues".into(),
                replica_identity: ReplicaIdentity::Default,
                columns: vec![RelationColumn {
                    is_key: true,
                    name: "id".into(),
                    type_oid: 25,
                    atttypmod: -1,
                }],
            })
            .unwrap();
        applier
            .apply_message(&PgoutputMessage::Insert {
                relation_id: 1,
                new: vec![TupleColumn::Text("a".into())],
            })
            .unwrap();
        // Abort before commit (e.g. stream error).
        applier.rollback().unwrap();

        assert!(!applier.in_transaction());
        assert_eq!(
            db.query_uncached("SELECT id FROM issues", &[])
                .unwrap()
                .len(),
            0,
            "an uncommitted, rolled-back transaction leaves the replica unchanged"
        );
    }

    #[test]
    fn streamed_truncate_empties_the_table_end_to_end() {
        let db = setup_db_with_issues_table();
        let mut applier = ReplicationApplier::new(&db).unwrap();

        apply_txn(
            &mut applier,
            &[PgoutputMessage::Insert {
                relation_id: 1,
                new: vec![TupleColumn::Text("a".into())],
            }],
        );
        assert_eq!(
            db.query_uncached("SELECT id FROM issues", &[])
                .unwrap()
                .len(),
            1
        );

        apply_txn(
            &mut applier,
            &[PgoutputMessage::Truncate {
                relation_ids: vec![1],
                cascade: false,
                restart_identity: false,
            }],
        );
        assert_eq!(
            db.query_uncached("SELECT id FROM issues", &[])
                .unwrap()
                .len(),
            0,
            "the streamed truncate emptied the replica table"
        );
    }

    #[test]
    fn insert_without_a_prior_relation_message_errors() {
        let db = setup_db_with_issues_table();
        let mut applier = ReplicationApplier::new(&db).unwrap();
        applier
            .apply_message(&PgoutputMessage::Begin {
                final_lsn: LSN,
                commit_timestamp: 0,
                xid: 1,
            })
            .unwrap();
        // No Relation message cached for relation_id 7 → translate must fail.
        let err = applier
            .apply_message(&PgoutputMessage::Insert {
                relation_id: 7,
                new: vec![TupleColumn::Text("a".into())],
            })
            .unwrap_err();
        assert!(
            matches!(err, ApplyError::Translate(_)),
            "an Insert referencing an unknown relation surfaces a Translate error, got {err:?}"
        );
        applier.rollback().unwrap();
    }

    #[test]
    fn apply_frame_decodes_raw_bytes_and_dispatches() {
        let db = setup_db_with_issues_table();
        let mut applier = ReplicationApplier::new(&db).unwrap();
        // A raw pgoutput `Begin` frame: 'B' + final_lsn(u64) + timestamp(i64) + xid(i32).
        let mut frame = vec![b'B'];
        frame.extend_from_slice(&LSN.to_be_bytes());
        frame.extend_from_slice(&0i64.to_be_bytes());
        frame.extend_from_slice(&1i32.to_be_bytes());
        assert!(applier.apply_frame(&frame).unwrap().is_none());
        assert!(
            applier.in_transaction(),
            "apply_frame decoded the Begin frame and opened the transaction"
        );
        applier.rollback().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::initial_sync::{run_initial_sync_introspected, SlotInfo};
    use crate::{StatementRunner, Value};
    use zero_cache_change_source::pg_connection;
    use zero_cache_change_source::replication_conn::{
        PgSslMode, ReplicationConn, ReplicationEvent,
    };

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
        db.query_uncached("SELECT id FROM rep_apply_test ORDER BY id", &[])
            .unwrap()
            .iter()
            .map(|r| match r[0].1 {
                Value::Integer(n) => n,
                ref v => panic!("unexpected {v:?}"),
            })
            .collect()
    }

    /// Full ongoing pipeline: initial-sync a table at the slot snapshot, then
    /// stream a live INSERT + UPDATE + DELETE through `ReplicationApplier` and
    /// confirm the SQLite replica tracks upstream.
    #[tokio::test]
    async fn live_streams_insert_update_delete_into_replica() {
        let Ok(pg) = pg_connection::connect(&conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS rep_apply_test CASCADE; \
             CREATE TABLE rep_apply_test(id int primary key, name text); \
             INSERT INTO rep_apply_test(id, name) VALUES (1, 'one'); \
             DROP PUBLICATION IF EXISTS rep_apply_pub; \
             CREATE PUBLICATION rep_apply_pub FOR TABLE rep_apply_test;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('rep_apply_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'rep_apply_slot');",
        )
        .await
        .ok();

        let (host, port) = host_port();
        let mut create_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let slot = create_conn
            .create_logical_replication_slot("rep_apply_slot", false)
            .await
            .unwrap();

        // Initial sync at the snapshot (copies id=1).
        let copy_conn = pg_connection::connect(&conn_str()).await.unwrap();
        let db = StatementRunner::open_in_memory().unwrap();
        run_initial_sync_introspected(
            &copy_conn,
            &db,
            &SlotInfo {
                consistent_point: slot.consistent_point.clone(),
                snapshot_name: slot.snapshot_name.clone(),
            },
            &["rep_apply_pub".to_string()],
            None,
            &Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            ids(&db),
            vec![1],
            "initial sync copied the pre-existing row"
        );
        drop(create_conn); // snapshot no longer needed

        // Start streaming from the slot's consistent point.
        let stream_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let mut stream = stream_conn
            .start_replication("rep_apply_slot", "rep_apply_pub", &slot.consistent_point)
            .await
            .unwrap();

        // Mutate upstream: insert 2, update 1, delete... then insert 3.
        pg.batch_execute(
            "INSERT INTO rep_apply_test(id, name) VALUES (2, 'two'); \
             UPDATE rep_apply_test SET name = 'ONE' WHERE id = 1; \
             INSERT INTO rep_apply_test(id, name) VALUES (3, 'three'); \
             DELETE FROM rep_apply_test WHERE id = 2;",
        )
        .await
        .unwrap();

        // Apply streamed transactions until the replica shows the final state.
        let mut applier = ReplicationApplier::new(&db).unwrap();
        let mut commits = 0;
        for _ in 0..200 {
            let event =
                tokio::time::timeout(std::time::Duration::from_secs(5), stream.next_event())
                    .await
                    .expect("timed out")
                    .unwrap();
            let Some(event) = event else { break };
            if let ReplicationEvent::Data { message, .. } = event {
                if applier.apply_message(&message).unwrap().is_some() {
                    commits += 1;
                    // After the transaction(s), ids should be [1, 3].
                    if ids(&db) == vec![1, 3] {
                        break;
                    }
                }
            }
        }
        assert!(
            commits >= 1,
            "at least one streamed transaction was committed"
        );
        assert_eq!(
            ids(&db),
            vec![1, 3],
            "replica reflects insert 2/3, delete 2"
        );
        let name = db
            .query_uncached("SELECT name FROM rep_apply_test WHERE id = 1", &[])
            .unwrap();
        assert_eq!(
            name[0][0].1,
            Value::Text("ONE".into()),
            "update to id=1 applied"
        );

        drop(stream);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('rep_apply_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION rep_apply_pub; DROP TABLE rep_apply_test;")
            .await
            .unwrap();
    }

    /// The reusable [`drive_apply_loop`] applies a streamed transaction AND
    /// advances the replication slot: after driving one commit, the row is in
    /// the replica and the slot's `confirmed_flush_lsn` has moved off the
    /// initial `0/0` (proving the standby status update was sent).
    #[tokio::test]
    async fn live_drive_apply_loop_advances_the_slot() {
        let Ok(pg) = pg_connection::connect(&conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS rep_drive_test CASCADE; \
             CREATE TABLE rep_drive_test(id int primary key, name text); \
             DROP PUBLICATION IF EXISTS rep_drive_pub; \
             CREATE PUBLICATION rep_drive_pub FOR TABLE rep_drive_test;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('rep_drive_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'rep_drive_slot');",
        )
        .await
        .ok();

        let (host, port) = host_port();
        let mut create_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let slot = create_conn
            .create_logical_replication_slot("rep_drive_slot", false)
            .await
            .unwrap();

        // Empty initial sync (table is empty), then stream.
        let copy_conn = pg_connection::connect(&conn_str()).await.unwrap();
        let db = StatementRunner::open_in_memory().unwrap();
        run_initial_sync_introspected(
            &copy_conn,
            &db,
            &SlotInfo {
                consistent_point: slot.consistent_point.clone(),
                snapshot_name: slot.snapshot_name.clone(),
            },
            &["rep_drive_pub".to_string()],
            None,
            &Default::default(),
        )
        .await
        .unwrap();
        drop(create_conn);

        let stream_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let mut stream = stream_conn
            .start_replication("rep_drive_slot", "rep_drive_pub", &slot.consistent_point)
            .await
            .unwrap();

        pg.batch_execute("INSERT INTO rep_drive_test(id, name) VALUES (7, 'seven')")
            .await
            .unwrap();

        // Drive exactly one committed transaction, then stop.
        let mut applier = ReplicationApplier::new(&db).unwrap();
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            drive_apply_loop(&mut stream, &mut applier, &[], |_commit| true),
        )
        .await
        .expect("drive loop timed out")
        .unwrap();
        assert_eq!(outcome.commits, 1, "drove one committed transaction");
        assert_eq!(outcome.drift, None);

        let rows = db
            .query_uncached("SELECT id FROM rep_drive_test", &[])
            .unwrap();
        assert_eq!(rows.len(), 1, "streamed row landed via the drive loop");

        // The slot's confirmed_flush_lsn advanced off the initial 0/0 — proof
        // the standby status update was sent by the drive loop.
        let confirmed = pg
            .query(
                "SELECT confirmed_flush_lsn > '0/0'::pg_lsn \
                 FROM pg_replication_slots WHERE slot_name = 'rep_drive_slot'",
                &[],
            )
            .await
            .unwrap();
        let advanced: bool = confirmed[0].get(0);
        assert!(advanced, "drive loop advanced the replication slot");

        drop(stream);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('rep_drive_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION rep_drive_pub; DROP TABLE rep_drive_test;")
            .await
            .unwrap();
    }

    /// [`drive_apply_loop`] continues across MULTIPLE committed transactions
    /// until `should_stop` fires, applying each and advancing the slot
    /// cumulatively — a distinct behavior from the single-commit stop.
    #[tokio::test]
    async fn live_drive_apply_loop_across_multiple_transactions() {
        let Ok(pg) = pg_connection::connect(&conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS rep_multi_test CASCADE; \
             CREATE TABLE rep_multi_test(id int primary key); \
             DROP PUBLICATION IF EXISTS rep_multi_pub; \
             CREATE PUBLICATION rep_multi_pub FOR TABLE rep_multi_test;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('rep_multi_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'rep_multi_slot');",
        )
        .await
        .ok();

        let (host, port) = host_port();
        let mut create_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let slot = create_conn
            .create_logical_replication_slot("rep_multi_slot", false)
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
            &["rep_multi_pub".to_string()],
            None,
            &Default::default(),
        )
        .await
        .unwrap();
        drop(create_conn);
        let stream_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let mut stream = stream_conn
            .start_replication("rep_multi_slot", "rep_multi_pub", &slot.consistent_point)
            .await
            .unwrap();

        // Two SEPARATE transactions (semicolon-separated auto-commit statements
        // are each their own transaction).
        pg.batch_execute("INSERT INTO rep_multi_test(id) VALUES (10);")
            .await
            .unwrap();
        pg.batch_execute("INSERT INTO rep_multi_test(id) VALUES (20);")
            .await
            .unwrap();

        // Drive until BOTH commits have been applied.
        let mut applier = ReplicationApplier::new(&db).unwrap();
        let mut seen = 0usize;
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            drive_apply_loop(&mut stream, &mut applier, &[], |_commit| {
                seen += 1;
                seen >= 2 // stop after the second commit
            }),
        )
        .await
        .expect("drive loop timed out")
        .unwrap();
        assert_eq!(outcome.commits, 2, "drove two separate transactions");

        let rows = db
            .query_uncached("SELECT id FROM rep_multi_test ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2, "both streamed rows landed");

        drop(stream);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('rep_multi_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION rep_multi_pub; DROP TABLE rep_multi_test;")
            .await
            .unwrap();
    }

    /// End-to-end SUPERVISED RECONNECT against LIVE Postgres: models the
    /// replicator service loop cycling across a transient disconnect. Round 1
    /// streams one transaction and stops; the supervisor
    /// (`decide_next_action`, no drift, service not shutting down) returns
    /// `Reconnect`, so the service re-subscribes from the slot's confirmed
    /// position. Round 2 then streams a SECOND transaction committed while
    /// disconnected — proving the resume picks up exactly where the slot left
    /// off (no gap, no re-delivery of round 1's row). A final shutdown flag
    /// yields the terminal `Stop`.
    #[tokio::test]
    async fn live_supervised_reconnect_resumes_from_slot() {
        use crate::replication_supervisor::{decide_next_action, SupervisorDecision};

        let Ok(pg) = pg_connection::connect(&conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS rep_recon_test CASCADE; \
             CREATE TABLE rep_recon_test(id int primary key); \
             DROP PUBLICATION IF EXISTS rep_recon_pub; \
             CREATE PUBLICATION rep_recon_pub FOR TABLE rep_recon_test;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('rep_recon_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'rep_recon_slot');",
        )
        .await
        .ok();

        let (host, port) = host_port();
        let mut create_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let slot = create_conn
            .create_logical_replication_slot("rep_recon_slot", false)
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
            &["rep_recon_pub".to_string()],
            None,
            &Default::default(),
        )
        .await
        .unwrap();
        drop(create_conn);

        let mut applier = ReplicationApplier::new(&db).unwrap();

        // The slot's current confirmed position — where a (re)subscribe resumes.
        async fn confirmed_lsn(pg: &tokio_postgres::Client) -> String {
            let row = pg
                .query_one(
                    "SELECT confirmed_flush_lsn::text \
                     FROM pg_replication_slots WHERE slot_name = 'rep_recon_slot'",
                    &[],
                )
                .await
                .unwrap();
            row.get::<_, String>(0)
        }

        // ---- Round 1: subscribe, stream one txn, then "disconnect". ----
        let stream_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let mut stream = stream_conn
            .start_replication("rep_recon_slot", "rep_recon_pub", &slot.consistent_point)
            .await
            .unwrap();
        pg.batch_execute("INSERT INTO rep_recon_test(id) VALUES (1)")
            .await
            .unwrap();
        let outcome1 = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            drive_apply_loop(&mut stream, &mut applier, &[], |_c| true),
        )
        .await
        .expect("round 1 timed out")
        .unwrap();
        assert_eq!(outcome1.commits, 1);
        // Service is NOT shutting down -> supervisor says reconnect.
        assert_eq!(
            decide_next_action(&outcome1, false),
            SupervisorDecision::Reconnect { applied_commits: 1 }
        );
        drop(stream); // simulate the transient disconnect

        let resume_lsn = confirmed_lsn(&pg).await;

        // A transaction committed WHILE disconnected must still be picked up.
        pg.batch_execute("INSERT INTO rep_recon_test(id) VALUES (2)")
            .await
            .unwrap();

        // ---- Round 2: re-subscribe from the confirmed LSN and resume. ----
        let stream_conn2 =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let mut stream2 = stream_conn2
            .start_replication("rep_recon_slot", "rep_recon_pub", &resume_lsn)
            .await
            .unwrap();
        let outcome2 = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            drive_apply_loop(&mut stream2, &mut applier, &[], |_c| true),
        )
        .await
        .expect("round 2 timed out")
        .unwrap();
        assert_eq!(outcome2.commits, 1, "resumed and drove the second txn");
        // Now the service IS shutting down -> terminal stop.
        assert_eq!(
            decide_next_action(&outcome2, true),
            SupervisorDecision::Stop
        );

        // Both rows present exactly once — no gap, no double-apply.
        let rows = db
            .query_uncached("SELECT id FROM rep_recon_test ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 2, "both rows landed across the reconnect");

        drop(stream2);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('rep_recon_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION rep_recon_pub; DROP TABLE rep_recon_test;")
            .await
            .unwrap();
    }

    /// End-to-end schema-drift detection against LIVE Postgres: after streaming
    /// begins, an upstream `ALTER TABLE ... ADD COLUMN` changes the schema. The
    /// next change ships a new `Relation` message reflecting the added column,
    /// and `drive_apply_loop` (given the ORIGINAL published specs) stops with
    /// `drift = Some(..)` instead of misapplying data against the stale schema.
    #[tokio::test]
    async fn live_drive_apply_loop_detects_schema_drift() {
        let Ok(pg) = pg_connection::connect(&conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS rep_drift_test CASCADE; \
             CREATE TABLE rep_drift_test(id int primary key, name text); \
             DROP PUBLICATION IF EXISTS rep_drift_pub; \
             CREATE PUBLICATION rep_drift_pub FOR TABLE rep_drift_test;",
        )
        .await
        .unwrap();
        pg.batch_execute(
            "SELECT pg_drop_replication_slot('rep_drift_slot') WHERE EXISTS \
             (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'rep_drift_slot');",
        )
        .await
        .ok();

        let (host, port) = host_port();
        let mut create_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let slot = create_conn
            .create_logical_replication_slot("rep_drift_slot", false)
            .await
            .unwrap();

        // The ORIGINAL published specs (2 columns) — what the replica was built
        // from; `drive_apply_loop` diffs streamed relations against these.
        let copy_conn = pg_connection::connect(&conn_str()).await.unwrap();
        let (specs, _indexes) = zero_cache_change_source::published_schema::get_publication_info(
            &copy_conn,
            &["rep_drift_pub"],
        )
        .await
        .unwrap();
        let db = StatementRunner::open_in_memory().unwrap();
        run_initial_sync_introspected(
            &copy_conn,
            &db,
            &SlotInfo {
                consistent_point: slot.consistent_point.clone(),
                snapshot_name: slot.snapshot_name.clone(),
            },
            &["rep_drift_pub".to_string()],
            None,
            &Default::default(),
        )
        .await
        .unwrap();
        drop(create_conn);

        let stream_conn =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let mut stream = stream_conn
            .start_replication("rep_drift_slot", "rep_drift_pub", &slot.consistent_point)
            .await
            .unwrap();

        // Change the schema upstream, then make a change so a new Relation
        // message (with the added column) is streamed.
        pg.batch_execute(
            "ALTER TABLE rep_drift_test ADD COLUMN extra int; \
             INSERT INTO rep_drift_test(id, name, extra) VALUES (1, 'x', 9);",
        )
        .await
        .unwrap();

        let mut applier = ReplicationApplier::new(&db).unwrap();
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            drive_apply_loop(&mut stream, &mut applier, &specs, |_commit| false),
        )
        .await
        .expect("drive loop timed out")
        .unwrap();
        assert!(
            outcome.drift.is_some(),
            "the added column was detected as schema drift, got {outcome:?}"
        );

        drop(stream);
        for _ in 0..20 {
            if pg
                .query("SELECT pg_drop_replication_slot('rep_drift_slot')", &[])
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION rep_drift_pub; DROP TABLE rep_drift_test;")
            .await
            .unwrap();
    }

    /// The ASSEMBLED supervised replication service, run live end-to-end
    /// across THREE cycles driven by a single [`ReplicatorSupervisor`]:
    /// cycle 1 applies a txn and the stream drops (supervisor → `Reconnect`,
    /// resume from the slot's confirmed LSN); cycle 2 hits an upstream schema
    /// change (supervisor → `Resync`, which really runs
    /// `reset_replica_for_resync` + `run_initial_sync_introspected` from a
    /// fresh slot); cycle 3 applies a txn against the NEW schema and the
    /// service is asked to shut down (supervisor → `Stop`). This is the
    /// proven pieces (`drive_apply_loop`, `ReplicatorSupervisor`, the
    /// reconnect-resume and resync-rebuild paths) assembled into one running
    /// loop against real Postgres — the service-loop assembly, exercised.
    #[tokio::test]
    async fn live_assembled_supervised_service_runs_through_reconnect_and_resync() {
        use crate::initial_sync::reset_replica_for_resync;
        use crate::replication_supervisor::{ReplicatorSupervisor, SupervisorDecision};

        let Ok(pg) = pg_connection::connect(&conn_str()).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        pg.batch_execute(
            "DROP TABLE IF EXISTS rep_svc_test CASCADE; \
             CREATE TABLE rep_svc_test(id int primary key, name text); \
             DROP PUBLICATION IF EXISTS rep_svc_pub; \
             CREATE PUBLICATION rep_svc_pub FOR TABLE rep_svc_test;",
        )
        .await
        .unwrap();
        let drop_slot = |n: &str| {
            format!(
                "SELECT pg_drop_replication_slot('{n}') WHERE EXISTS \
                 (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{n}');"
            )
        };
        pg.batch_execute(&drop_slot("rep_svc_slot1")).await.ok();
        pg.batch_execute(&drop_slot("rep_svc_slot2")).await.ok();

        let (host, port) = host_port();
        let subscribe = |slot: &'static str, from: String| {
            let host = host.clone();
            async move {
                let conn = ReplicationConn::connect(
                    &host,
                    port,
                    "postgres",
                    "postgres",
                    None,
                    PgSslMode::Prefer,
                )
                .await
                .unwrap();
                conn.start_replication(slot, "rep_svc_pub", &from)
                    .await
                    .unwrap()
            }
        };
        async fn confirmed(pg: &tokio_postgres::Client, slot: &str) -> String {
            pg.query_one(
                &format!(
                    "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                     WHERE slot_name = '{slot}'"
                ),
                &[],
            )
            .await
            .unwrap()
            .get::<_, String>(0)
        }

        // Initial sync from the original (id, name) schema.
        let mut create1 =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let slot1 = create1
            .create_logical_replication_slot("rep_svc_slot1", false)
            .await
            .unwrap();
        let copy1 = pg_connection::connect(&conn_str()).await.unwrap();
        let (mut specs, _) = zero_cache_change_source::published_schema::get_publication_info(
            &copy1,
            &["rep_svc_pub"],
        )
        .await
        .unwrap();
        let db = StatementRunner::open_in_memory().unwrap();
        run_initial_sync_introspected(
            &copy1,
            &db,
            &SlotInfo {
                consistent_point: slot1.consistent_point.clone(),
                snapshot_name: slot1.snapshot_name.clone(),
            },
            &["rep_svc_pub".to_string()],
            None,
            &Default::default(),
        )
        .await
        .unwrap();
        drop(create1);

        let mut applier = ReplicationApplier::new(&db).unwrap();
        let mut sup = ReplicatorSupervisor::new();

        // ---- Cycle 1: apply one txn, stream drops -> Reconnect. ----
        let mut stream = subscribe("rep_svc_slot1", slot1.consistent_point.clone()).await;
        pg.batch_execute("INSERT INTO rep_svc_test(id, name) VALUES (1, 'a')")
            .await
            .unwrap();
        let o1 = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            drive_apply_loop(&mut stream, &mut applier, &specs, |_c| true),
        )
        .await
        .expect("cycle 1 timed out")
        .unwrap();
        assert_eq!(
            sup.record(&o1, false),
            SupervisorDecision::Reconnect { applied_commits: 1 }
        );
        drop(stream);

        // ---- Cycle 2: upstream schema drifts -> Resync. ----
        let resume = confirmed(&pg, "rep_svc_slot1").await;
        pg.batch_execute(
            "ALTER TABLE rep_svc_test ADD COLUMN priority int; \
             INSERT INTO rep_svc_test(id, name, priority) VALUES (2, 'b', 5);",
        )
        .await
        .unwrap();
        let mut stream = subscribe("rep_svc_slot1", resume).await;
        let o2 = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            drive_apply_loop(&mut stream, &mut applier, &specs, |_c| false),
        )
        .await
        .expect("cycle 2 timed out")
        .unwrap();
        let decision2 = sup.record(&o2, false);
        assert!(
            matches!(decision2, SupervisorDecision::Resync { .. }),
            "schema change drove a resync, got {decision2:?}"
        );
        // Drift breaks the loop mid-transaction (the `Begin` opened a txn before
        // the drifted `Relation`); roll it back so the shared replica connection
        // isn't left inside a transaction before the resync rebuild.
        applier.rollback().ok();
        drop(stream);

        // ---- Execute the Resync: reset the replica + re-sync from a fresh
        //      slot at the NEW schema. ----
        pg.batch_execute(&drop_slot("rep_svc_slot1")).await.ok();
        reset_replica_for_resync(&db).unwrap();
        let mut create2 =
            ReplicationConn::connect(&host, port, "postgres", "postgres", None, PgSslMode::Prefer)
                .await
                .unwrap();
        let slot2 = create2
            .create_logical_replication_slot("rep_svc_slot2", false)
            .await
            .unwrap();
        let copy2 = pg_connection::connect(&conn_str()).await.unwrap();
        let (new_specs, _) = zero_cache_change_source::published_schema::get_publication_info(
            &copy2,
            &["rep_svc_pub"],
        )
        .await
        .unwrap();
        specs = new_specs;
        run_initial_sync_introspected(
            &copy2,
            &db,
            &SlotInfo {
                consistent_point: slot2.consistent_point.clone(),
                snapshot_name: slot2.snapshot_name.clone(),
            },
            &["rep_svc_pub".to_string()],
            None,
            &Default::default(),
        )
        .await
        .unwrap();
        drop(create2);
        // A fresh applier for the rebuilt replica.
        let mut applier = ReplicationApplier::new(&db).unwrap();

        // ---- Cycle 3: apply one txn on the NEW schema, then shut down -> Stop. ----
        let mut stream = subscribe("rep_svc_slot2", slot2.consistent_point.clone()).await;
        pg.batch_execute("INSERT INTO rep_svc_test(id, name, priority) VALUES (3, 'c', 9)")
            .await
            .unwrap();
        let o3 = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            drive_apply_loop(&mut stream, &mut applier, &specs, |_c| true),
        )
        .await
        .expect("cycle 3 timed out")
        .unwrap();
        assert_eq!(sup.record(&o3, true), SupervisorDecision::Stop);
        drop(stream);

        // The service's lifecycle bookkeeping across all three cycles.
        assert_eq!(sup.reconnects, 1);
        assert_eq!(sup.resyncs, 1);

        // The replica reflects the NEW schema with every row (1 & 2 re-copied at
        // the resync snapshot, 3 streamed after) and the added `priority` column.
        let rows = db
            .query_uncached("SELECT id, priority FROM rep_svc_test ORDER BY id", &[])
            .unwrap();
        assert_eq!(rows.len(), 3, "all rows present post-resync-and-resume");
        // Row 2's priority (5) and row 3's (9) survived; row 1 predates the column.
        let p2 = rows.iter().find(|r| matches!(r[0].1, Value::Integer(2)));
        assert!(
            matches!(p2.map(|r| &r[1].1), Some(Value::Integer(5))),
            "row 2 has priority 5 after the resync"
        );

        // The WHERE EXISTS form succeeds once the slot is gone and only errors
        // while the walsender still holds it, so retrying it terminates as soon
        // as the slot is released (a plain drop after it would always error).
        for _ in 0..20 {
            if pg.batch_execute(&drop_slot("rep_svc_slot2")).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        pg.batch_execute("DROP PUBLICATION rep_svc_pub; DROP TABLE rep_svc_test;")
            .await
            .unwrap();
    }

    #[test]
    fn version_from_lsn_matches_initial_sync_path() {
        // 0/16B2D8 -> the same version to_state_version_string yields for the
        // "0/16B2D8" consistent-point string form.
        let lsn: u64 = 0x16B2D8;
        let via_u64 = version_from_lsn(lsn).unwrap();
        let via_str = zero_cache_types::lsn::to_state_version_string("0/16B2D8").unwrap();
        assert_eq!(via_u64, via_str);
    }
}
