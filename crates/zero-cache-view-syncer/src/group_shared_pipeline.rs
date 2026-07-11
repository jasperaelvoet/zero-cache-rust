//! One client group's shared query pipeline — the concrete owner of the shared
//! [`PipelineDriver`] and its cross-client query ref-count
//! ([`GroupQuerySet`]).
//!
//! This is the vehicle upstream calls `#pipelines` guarded by `#lock`
//! (`mono-src/packages/zero-cache/src/services/view-syncer/view-syncer.ts`): one
//! driver per client group, its mutations serialized by a lock, its queries
//! ref-counted across the group's connections. Because every `PipelineDriver`
//! call site in the server is SYNCHRONOUS
//! (`crates/zero-cache-server/src/live_connection.rs` — `hydrate_put`,
//! `rehydrate_tracked`, `apply_and_poke*`), the lock here is a plain
//! `std::sync::Mutex` and driver calls never `.await` while it is held — the
//! Rust equivalent of upstream's synchronous pipeline under an async `Lock`.
//!
//! (The thread-confined [`crate::group_pipeline::GroupHandle`] is the vehicle
//! for the future `!Send`, graph-owning driver in redesign Phase C; while the
//! driver is `Send`, this in-process `Mutex` shares it across a group's
//! connection tasks without a dedicated thread.)
//!
//! Dead code until the bootstrap wiring (redesign §6 B4) routes a group's
//! connections here; kept standalone and unit-tested so the shared-driver
//! ref-count semantics are pinned first.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use zero_cache_protocol::ast::Ast;

use crate::group_pipeline::PipelineDriverBuilder;
use crate::group_query_set::{GroupQuerySet, QueryTransition};
use crate::pipeline_driver::{PipelineDriver, PipelineError, PipelineRowChange};

/// An ordered log of the group's pipeline advances, so every connection sees
/// every advance exactly once and in order even though a single shared
/// snapshotter can only leapfrog to head once per commit.
///
/// The FIRST connection to process a commit advances the driver and appends the
/// resulting changes as a log entry; a later connection processing the same
/// commit finds the driver already at head (its own advance is empty) but reads
/// the SAME appended entry from its cursor. This is the Rust analogue of
/// upstream advancing the group pipeline once and fanning the result out to
/// every `ClientHandler` (`view-syncer.ts`). Entries below the slowest cursor
/// are trimmed.
#[derive(Default)]
struct AdvanceLog {
    /// `(sequence, changes)` for each non-empty advance, oldest first.
    entries: VecDeque<(u64, std::sync::Arc<Vec<PipelineRowChange>>)>,
    /// Sequence number the next appended entry will get.
    next_seq: u64,
    /// Each connection's next-unread sequence, keyed by `client_id`.
    cursors: HashMap<String, u64>,
}

impl AdvanceLog {
    /// Registers a connection joining at the current head — it will only read
    /// advances appended after this point (its hydration already reflects
    /// everything up to `next_seq`). Idempotent: an existing cursor is kept.
    fn register(&mut self, client_id: &str) {
        self.cursors
            .entry(client_id.to_string())
            .or_insert(self.next_seq);
    }

    /// Appends a non-empty advance's changes.
    fn append(&mut self, changes: std::sync::Arc<Vec<PipelineRowChange>>) {
        self.entries.push_back((self.next_seq, changes));
        self.next_seq += 1;
    }

    /// Returns everything `client_id` has not yet read (in order), advancing its
    /// cursor to head. A connection with no cursor is registered at head first
    /// (so it reads nothing until the next advance).
    fn drain_for(&mut self, client_id: &str) -> Vec<PipelineRowChange> {
        let cursor = *self
            .cursors
            .entry(client_id.to_string())
            .or_insert(self.next_seq);
        let mut out = Vec::new();
        for (seq, changes) in &self.entries {
            if *seq >= cursor {
                out.extend(changes.iter().cloned());
            }
        }
        self.cursors.insert(client_id.to_string(), self.next_seq);
        self.trim();
        out
    }

    /// Drops entries every cursor has passed.
    fn trim(&mut self) {
        let min_cursor = self
            .cursors
            .values()
            .copied()
            .min()
            .unwrap_or(self.next_seq);
        while let Some((seq, _)) = self.entries.front() {
            if *seq < min_cursor {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }

    fn forget(&mut self, client_id: &str) {
        self.cursors.remove(client_id);
        self.trim();
    }
}

/// The shared driver + query ref-count for one client group. Every connection
/// in the group calls through the same instance; all methods are synchronous so
/// they drop into the existing sync handler call sites unchanged.
pub struct SharedGroupPipeline {
    driver: Mutex<PipelineDriver>,
    query_set: Mutex<GroupQuerySet>,
    advance_log: Mutex<AdvanceLog>,
}

impl SharedGroupPipeline {
    pub fn new(builder: PipelineDriverBuilder) -> Result<Self, PipelineError> {
        Ok(Self {
            driver: Mutex::new(builder.build()?),
            query_set: Mutex::new(GroupQuerySet::new()),
            advance_log: Mutex::new(AdvanceLog::default()),
        })
    }

    fn driver(&self) -> std::sync::MutexGuard<'_, PipelineDriver> {
        self.driver
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn query_set(&self) -> std::sync::MutexGuard<'_, GroupQuerySet> {
        self.query_set
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn advance_log(&self) -> std::sync::MutexGuard<'_, AdvanceLog> {
        self.advance_log
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// `client_id` desires `query_id`. Ref-counted across the group: the query
    /// is hydrated on the shared driver only for the FIRST desirer; a later
    /// connection desiring the same query is seeded from the driver's existing
    /// result rows (no re-hydration). Either way the caller receives the `Add`
    /// changes to seed THIS connection's CVR/poke.
    pub fn desire(
        &self,
        client_id: &str,
        query_id: &str,
        ast: Ast,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        // Register this connection in the advance log at the current head, so it
        // reads only advances that happen after it hydrates.
        self.advance_log().register(client_id);
        let transition = self.query_set().add_desire(client_id, query_id);
        let mut driver = self.driver();
        match transition {
            QueryTransition::Hydrate => driver.add_query(query_id, ast),
            // Already active for another connection: seed from existing rows
            // rather than re-adding (which would be a DuplicateQuery).
            QueryTransition::Unchanged => Ok(driver.current_query_rows(query_id)),
            QueryTransition::Remove => unreachable!("add_desire never removes"),
        }
    }

    /// `client_id` no longer desires `query_id`. The query is removed from the
    /// shared driver only when the LAST desirer drops it; the returned `Remove`
    /// changes are non-empty only in that case.
    pub fn undesire(&self, client_id: &str, query_id: &str) -> Vec<PipelineRowChange> {
        let transition = self.query_set().remove_desire(client_id, query_id);
        match transition {
            QueryTransition::Remove => self.driver().remove_query(query_id),
            _ => Vec::new(),
        }
    }

    /// Drops every query `client_id` desired (a disconnect), removing from the
    /// shared driver only those it was the sole desirer of.
    pub fn remove_client(&self, client_id: &str) -> Vec<PipelineRowChange> {
        let removed = self.query_set().remove_client(client_id);
        self.advance_log().forget(client_id);
        let mut driver = self.driver();
        removed
            .iter()
            .flat_map(|query_id| driver.remove_query(query_id))
            .collect()
    }

    /// Advances the shared snapshotter to head once and appends any resulting
    /// changes to the advance log; returns nothing. Used where a connection must
    /// bring the shared snapshot current WITHOUT consuming its own advance cursor
    /// (e.g. before an initial query fetch): the changes are still logged so the
    /// connection — and every other connection in the group — reads them on its
    /// next [`poll_advance`].
    pub fn advance_to_head(&self) -> Result<(), PipelineError> {
        let changes = self.driver().advance()?;
        if !changes.is_empty() {
            self.advance_log().append(std::sync::Arc::new(changes));
        }
        Ok(())
    }

    /// The group-shared advance for one connection: brings the snapshotter to
    /// head (appending any new changes to the log), then returns everything this
    /// connection has not yet read, in order. Concurrent connections processing
    /// the same commit each receive the same logged changes exactly once — the
    /// fan-out that makes a shared driver correct for a multi-connection group.
    pub fn poll_advance(&self, client_id: &str) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let changes = self.driver().advance()?;
        let mut log = self.advance_log();
        if !changes.is_empty() {
            log.append(std::sync::Arc::new(changes));
        }
        Ok(log.drain_for(client_id))
    }

    /// Advances the shared pipeline to the replica head once, returning the
    /// changes across all active queries. This is the single-owner path (one
    /// connection per group); [`poll_advance`] is the multi-connection fan-out.
    pub fn advance(&self) -> Result<Vec<PipelineRowChange>, PipelineError> {
        self.driver().advance()
    }

    pub fn version(&self) -> Result<String, PipelineError> {
        Ok(self.driver().version()?.to_string())
    }

    pub fn row_set_signature(&self, query_id: &str) -> Option<u64> {
        self.driver().row_set_signature(query_id)
    }

    /// Whether `ast` would hydrate via the direct-incremental graph (so the
    /// caller could register pre-fetched rows). Forwards to the driver.
    pub fn uses_prehydrated_rows(&self, ast: &Ast) -> bool {
        self.driver().uses_prehydrated_rows(ast)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicU64, Ordering};

    use zero_cache_protocol::ast::{Ast, Direction};
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_sqlite::change_log::CREATE_CHANGELOG_SCHEMA;
    use zero_cache_sqlite::replication_state::init_replication_state;
    use zero_cache_sqlite::snapshotter::SnapshotTableSpec;
    use zero_cache_sqlite::StatementRunner;

    use crate::pipeline_driver::PipelineRowChangeKind;

    fn path() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir()
            .join(format!(
                "zero-shared-pipeline-{}-{}.db",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed),
            ))
            .to_string_lossy()
            .into_owned()
    }

    fn builder(db_file: &str) -> PipelineDriverBuilder {
        PipelineDriverBuilder {
            db_file: db_file.to_string(),
            app_id: "zero".into(),
            page_cache_size_kib: None,
            table_specs: BTreeMap::from([(
                "issue".into(),
                SnapshotTableSpec {
                    name: "issue".into(),
                    columns: vec!["id".into(), "_0_version".into()],
                    column_types: BTreeMap::new(),
                    primary_key: vec!["id".into()],
                    unique_keys: vec![],
                    min_row_version: Some("00".into()),
                },
            )]),
            all_table_names: BTreeSet::from(["issue".into()]),
        }
    }

    fn issue_query() -> Ast {
        Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        }
    }

    fn fresh_replica() -> String {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, _0_version TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (1, '00')", &[])
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (2, '00')", &[])
            .unwrap();
        drop(writer);
        path
    }

    /// Two connections in one group desire the SAME query: it is hydrated once
    /// (first connection) and the second is seeded from the existing rows — a
    /// shared pipeline never double-adds. The query is removed only when the
    /// last connection drops it.
    #[test]
    fn shared_query_hydrates_once_and_removes_on_last_drop() {
        let path = fresh_replica();
        let shared = SharedGroupPipeline::new(builder(&path)).unwrap();

        // First connection hydrates the query: 2 rows.
        let first = shared.desire("c1", "q", issue_query()).unwrap();
        assert_eq!(first.len(), 2);
        assert!(first.iter().all(|c| c.kind == PipelineRowChangeKind::Add));

        // Second connection desiring the same query gets the SAME rows without
        // a re-hydration (a re-add would be a DuplicateQuery error).
        let second = shared.desire("c2", "q", issue_query()).unwrap();
        assert_eq!(second.len(), 2);
        assert!(second.iter().all(|c| c.kind == PipelineRowChangeKind::Add));

        // One connection dropping the query does NOT remove it (c2 still wants).
        assert!(shared.undesire("c1", "q").is_empty());
        assert!(shared.row_set_signature("q").is_some());

        // The last connection dropping it removes it: 2 Remove changes.
        let removed = shared.undesire("c2", "q");
        assert_eq!(removed.len(), 2);
        assert!(removed
            .iter()
            .all(|c| c.kind == PipelineRowChangeKind::Remove));

        let _ = std::fs::remove_file(path);
    }

    /// A disconnect drops only the queries the client solely desired.
    #[test]
    fn remove_client_removes_only_solely_desired_queries() {
        let path = fresh_replica();
        let shared = SharedGroupPipeline::new(builder(&path)).unwrap();
        shared.desire("c1", "q", issue_query()).unwrap();
        shared.desire("c2", "q", issue_query()).unwrap();

        // c1 disconnects but c2 still desires q, so nothing is removed.
        assert!(shared.remove_client("c1").is_empty());
        assert!(shared.row_set_signature("q").is_some());

        // c2 disconnects: q loses its last desirer and is removed (2 rows).
        let removed = shared.remove_client("c2");
        assert_eq!(removed.len(), 2);

        let _ = std::fs::remove_file(path);
    }

    /// The fan-out invariant: two connections sharing a group both observe the
    /// SAME commit's changes exactly once, even though the single shared
    /// snapshotter leapfrogs to head only once. The first `poll_advance`
    /// advances the driver and logs the change; the second finds the driver
    /// already at head but reads the same logged change from its own cursor.
    #[test]
    fn poll_advance_fans_one_commit_out_to_every_connection() {
        use zero_cache_sqlite::change_log::ChangeLog;
        use zero_cache_sqlite::replication_state::update_replication_watermark;

        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, _0_version TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (1, '00')", &[])
            .unwrap();

        let shared = SharedGroupPipeline::new(builder(&path)).unwrap();
        // Two connections in the same group desire the same query; it hydrates
        // once and both are registered in the advance log at head.
        assert_eq!(shared.desire("c1", "q", issue_query()).unwrap().len(), 1);
        assert_eq!(shared.desire("c2", "q", issue_query()).unwrap().len(), 1);

        // One commit: update the row.
        writer
            .run("UPDATE issue SET _0_version='01' WHERE id=1", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "01",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(1.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "01").unwrap();

        // c1 processes the commit first: it advances the driver and gets the
        // Edit.
        let c1 = shared.poll_advance("c1").unwrap();
        assert_eq!(c1.len(), 1);
        assert_eq!(c1[0].kind, PipelineRowChangeKind::Edit);

        // c2 processes the SAME commit: the driver is already at head (its own
        // advance is empty) but it reads the identical logged change.
        let c2 = shared.poll_advance("c2").unwrap();
        assert_eq!(c2.len(), 1);
        assert_eq!(c2[0].kind, PipelineRowChangeKind::Edit);
        assert_eq!(c1, c2, "both connections observe the same change");

        // Neither sees the change again on a subsequent poll with no new commit.
        assert!(shared.poll_advance("c1").unwrap().is_empty());
        assert!(shared.poll_advance("c2").unwrap().is_empty());

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// A connection that joins AFTER a commit does not replay changes already
    /// baked into its hydration — its cursor starts at head.
    #[test]
    fn late_joining_connection_does_not_replay_pre_hydration_changes() {
        use zero_cache_sqlite::change_log::ChangeLog;
        use zero_cache_sqlite::replication_state::update_replication_watermark;

        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, _0_version TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (1, '00')", &[])
            .unwrap();

        let shared = SharedGroupPipeline::new(builder(&path)).unwrap();
        shared.desire("c1", "q", issue_query()).unwrap();

        // Commit, then c1 consumes it (advances the driver + logs the change).
        writer
            .run("UPDATE issue SET _0_version='01' WHERE id=1", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "01",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(1.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "01").unwrap();
        assert_eq!(shared.poll_advance("c1").unwrap().len(), 1);

        // c2 joins now: its hydration already reflects version 01, so its first
        // poll (no new commit) must return nothing — it does not replay the
        // pre-hydration change.
        shared.desire("c2", "q", issue_query()).unwrap();
        assert!(shared.poll_advance("c2").unwrap().is_empty());

        drop(writer);
        let _ = std::fs::remove_file(path);
    }
}
