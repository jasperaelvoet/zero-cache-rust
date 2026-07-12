//! One client group's shared query pipeline over the PERSISTENT push-graph
//! driver (group-loop plan increment 7) — the [`GroupHandle`]-backed successor
//! to [`crate::group_shared_pipeline::SharedGroupPipeline`].
//!
//! Where `SharedGroupPipeline` shares a `Send` `PipelineDriver` behind a
//! `Mutex` (whose complex-query advance re-fetches, O(result)), this facade
//! routes every driver call to the group's dedicated OS thread hosting the
//! `!Send` [`crate::graph_pipeline_driver::GraphPipelineDriver`], so direct AND
//! complex queries advance by push, O(change). The cross-client query
//! ref-count ([`GroupQuerySet`]) and the advance fan-out log ([`AdvanceLog`])
//! keep exactly the semantics `SharedGroupPipeline` pinned; only the driver
//! (and the thread bridge) changed.
//!
//! The method surface is synchronous — the identical drop-in surface the
//! server's `QueryPipeline::Shared` call sites use — bridged to the pipeline
//! thread via [`GroupHandle`]'s blocking calls. This is deadlock-free: replies
//! are produced by the group's dedicated OS thread, which never blocks on the
//! caller's runtime (see `group_pipeline`'s module doc). The only production
//! caller on the flag-on path is the per-group processor loop, so a briefly
//! blocked worker serializes exactly the work the group loop already
//! serializes by design.

use std::sync::Mutex;

use zero_cache_protocol::ast::Ast;

use crate::group_pipeline::{GroupHandle, GroupPipelineError, PipelineDriverBuilder};
use crate::group_query_set::{GroupQuerySet, QueryTransition};
use crate::group_shared_pipeline::AdvanceLog;
use crate::pipeline_driver::{PipelineError, PipelineRowChange};

fn bridge(error: GroupPipelineError) -> PipelineError {
    match error {
        GroupPipelineError::Pipeline(error) => error,
        GroupPipelineError::Closed => PipelineError::Thread("closed".into()),
    }
}

/// The thread-hosted graph driver + query ref-count for one client group.
/// Every connection in the group calls through the same instance; all methods
/// are synchronous so they drop into the existing `QueryPipeline::Shared` call
/// sites unchanged.
pub struct GroupGraphPipeline {
    handle: GroupHandle,
    query_set: Mutex<GroupQuerySet>,
    advance_log: Mutex<AdvanceLog>,
}

impl GroupGraphPipeline {
    pub fn new(builder: PipelineDriverBuilder) -> Result<Self, PipelineError> {
        let handle = GroupHandle::spawn(builder)
            .map_err(|error| PipelineError::Thread(error.to_string()))?;
        Ok(Self {
            handle,
            query_set: Mutex::new(GroupQuerySet::new()),
            advance_log: Mutex::new(AdvanceLog::default()),
        })
    }

    /// A clone of the underlying thread handle (async surface), for callers
    /// that want to await instead of block.
    pub fn handle(&self) -> GroupHandle {
        self.handle.clone()
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
    /// is hydrated on the thread-hosted graph driver only for the FIRST
    /// desirer; a later connection desiring the same query is seeded from the
    /// driver's existing result rows (no re-hydration).
    pub fn desire(
        &self,
        client_id: &str,
        query_id: &str,
        ast: Ast,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        self.advance_log().register(client_id);
        let transition = self.query_set().add_desire(client_id, query_id);
        match transition {
            QueryTransition::Hydrate => self
                .handle
                .add_query_blocking(query_id, ast)
                .map_err(bridge),
            // Already active for another connection: seed from existing rows
            // rather than re-adding (which would be a DuplicateQuery).
            QueryTransition::Unchanged => self
                .handle
                .current_query_rows_blocking(query_id)
                .map_err(bridge),
            QueryTransition::Remove => unreachable!("add_desire never removes"),
        }
    }

    /// [`Self::desire`] with rows the caller ALREADY fetched. The graph driver
    /// always hydrates through its own persistent graph (its `register_query`
    /// ignores the rows and is exactly `add_query`), so this exists purely for
    /// surface parity — `uses_prehydrated_rows` returns `false`, so the server
    /// never routes here for this driver.
    pub fn register_desire(
        &self,
        client_id: &str,
        query_id: &str,
        ast: Ast,
        rows: Vec<zero_cache_zql::ivm::data::Row>,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        self.advance_log().register(client_id);
        let transition = self.query_set().add_desire(client_id, query_id);
        match transition {
            QueryTransition::Hydrate => self
                .handle
                .register_query_blocking(query_id, ast, rows)
                .map_err(bridge),
            QueryTransition::Unchanged => self
                .handle
                .current_query_rows_blocking(query_id)
                .map_err(bridge),
            QueryTransition::Remove => unreachable!("add_desire never removes"),
        }
    }

    /// `client_id` no longer desires `query_id`. The query is removed from the
    /// driver only when the LAST desirer drops it; the returned `Remove`
    /// changes are non-empty only in that case.
    pub fn undesire(&self, client_id: &str, query_id: &str) -> Vec<PipelineRowChange> {
        let transition = self.query_set().remove_desire(client_id, query_id);
        match transition {
            QueryTransition::Remove => self
                .handle
                .remove_query_blocking(query_id)
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }

    /// Drops every query `client_id` desired (a disconnect), removing from the
    /// driver only those it was the sole desirer of.
    pub fn remove_client(&self, client_id: &str) -> Vec<PipelineRowChange> {
        let removed = self.query_set().remove_client(client_id);
        self.advance_log().forget(client_id);
        let mut changes = Vec::new();
        for query_id in removed {
            changes.extend(
                self.handle
                    .remove_query_blocking(&query_id)
                    .unwrap_or_default(),
            );
        }
        changes
    }

    /// Advances the shared graph driver to head once, appending any changes to
    /// the log, then skips `client_id`'s cursor PAST them (it does not receive
    /// them). Same semantics as `SharedGroupPipeline::advance_to_head`.
    pub fn advance_to_head(&self, client_id: &str) -> Result<(), PipelineError> {
        let changes = self.handle.advance_blocking().map_err(bridge)?;
        let mut log = self.advance_log();
        if !changes.is_empty() {
            log.append(std::sync::Arc::new(changes));
        }
        let _ = log.drain_for(client_id);
        Ok(())
    }

    /// The group-shared advance for one connection: brings the driver to head
    /// (appending any new changes to the log), then returns everything this
    /// connection has not yet read, filtered to the queries it desires. Same
    /// fan-out semantics as `SharedGroupPipeline::poll_advance`.
    pub fn poll_advance(&self, client_id: &str) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let unread = {
            let changes = self.handle.advance_blocking().map_err(bridge)?;
            let mut log = self.advance_log();
            if !changes.is_empty() {
                log.append(std::sync::Arc::new(changes));
            }
            log.drain_for(client_id)
        };
        let query_set = self.query_set();
        Ok(unread
            .into_iter()
            .filter(|change| query_set.client_desires(client_id, &change.query_id))
            .collect())
    }

    /// Advances the pipeline to the replica head once, returning the changes
    /// across all active queries — the single-owner path the group processor
    /// loop drives (it fans the result out itself).
    pub fn advance(&self) -> Result<Vec<PipelineRowChange>, PipelineError> {
        self.handle.advance_blocking().map_err(bridge)
    }

    /// Fully drops `query_id` from the driver AND its group ref-count so the
    /// next `desire`/`register_desire` re-hydrates it from scratch (the group
    /// loop's transformation-hash guard). Returns the `Remove` changes.
    pub fn reset_query(&self, query_id: &str) -> Vec<PipelineRowChange> {
        self.query_set().clear_query(query_id);
        self.handle
            .remove_query_blocking(query_id)
            .unwrap_or_default()
    }

    pub fn version(&self) -> Result<String, PipelineError> {
        self.handle.version_blocking().map_err(bridge)
    }

    pub fn row_set_signature(&self, query_id: &str) -> Option<u64> {
        self.handle
            .row_set_signature_blocking(query_id)
            .unwrap_or(None)
    }

    /// The graph driver always hydrates through its persistent replica-backed
    /// graph, so the prehydration fast path never applies (always `false`).
    /// Forwarded to the driver for surface fidelity.
    pub fn uses_prehydrated_rows(&self, ast: &Ast) -> bool {
        self.handle
            .uses_prehydrated_rows_blocking(ast.clone())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicU64, Ordering};

    use zero_cache_protocol::ast::{Ast, Direction};
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_sqlite::change_log::{ChangeLog, CREATE_CHANGELOG_SCHEMA};
    use zero_cache_sqlite::replication_state::{
        init_replication_state, update_replication_watermark,
    };
    use zero_cache_sqlite::snapshotter::SnapshotTableSpec;
    use zero_cache_sqlite::StatementRunner;

    use crate::pipeline_driver::PipelineRowChangeKind;

    fn path() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir()
            .join(format!(
                "zero-graph-shared-pipeline-{}-{}.db",
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

    /// The ref-count semantics `SharedGroupPipeline` pinned hold identically
    /// on the graph-backed facade: hydrate once, seed the 2nd desirer from the
    /// active query's rows, remove only on the last drop.
    #[test]
    fn shared_query_hydrates_once_and_removes_on_last_drop() {
        let path = fresh_replica();
        let shared = GroupGraphPipeline::new(builder(&path)).unwrap();

        let first = shared.desire("c1", "q", issue_query()).unwrap();
        assert_eq!(first.len(), 2);
        assert!(first.iter().all(|c| c.kind == PipelineRowChangeKind::Add));

        let second = shared.desire("c2", "q", issue_query()).unwrap();
        assert_eq!(second.len(), 2);
        assert!(second.iter().all(|c| c.kind == PipelineRowChangeKind::Add));

        assert!(shared.undesire("c1", "q").is_empty());
        assert!(shared.row_set_signature("q").is_some());

        let removed = shared.undesire("c2", "q");
        assert_eq!(removed.len(), 2);
        assert!(removed
            .iter()
            .all(|c| c.kind == PipelineRowChangeKind::Remove));

        let _ = std::fs::remove_file(path);
    }

    /// One commit fans out to every connection exactly once through the
    /// blocking bridge — the `poll_advance` invariant on the graph driver.
    #[test]
    fn poll_advance_fans_one_commit_out_to_every_connection() {
        let path = fresh_replica();
        let shared = GroupGraphPipeline::new(builder(&path)).unwrap();
        assert_eq!(shared.desire("c1", "q", issue_query()).unwrap().len(), 2);
        assert_eq!(shared.desire("c2", "q", issue_query()).unwrap().len(), 2);

        let writer = StatementRunner::open_file(&path).unwrap();
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

        let c1 = shared.poll_advance("c1").unwrap();
        assert_eq!(c1.len(), 1);
        assert_eq!(c1[0].kind, PipelineRowChangeKind::Edit);
        let c2 = shared.poll_advance("c2").unwrap();
        assert_eq!(c1, c2, "both connections observe the same change");
        assert!(shared.poll_advance("c1").unwrap().is_empty());
        assert!(shared.poll_advance("c2").unwrap().is_empty());

        drop(writer);
        let _ = std::fs::remove_file(path);
    }
}
