//! Pins one client group's [`PipelineDriver`] to a dedicated OS thread behind a
//! command channel — the Rust analogue of upstream's per-group
//! `ViewSyncerService` owning ONE `PipelineDriver` whose mutations are
//! serialized by `#lock` (`mono-src/packages/zero-cache/src/services/runner.ts`
//! and `view-syncer.ts`). Every connection in a group sends commands to the same
//! thread, which serves them FIFO, so the driver is never touched concurrently
//! and only `Send` values (the [`PipelineRowChange`] boundary type and reply
//! payloads) cross the thread boundary.
//!
//! Since group-loop-plan increment 7 the thread hosts the graph-owning,
//! `!Send` [`GraphPipelineDriver`]: every query in the group advances by PUSH
//! through its persistent operator graph (O(change)), and the channel is what
//! keeps the `Rc` graphs thread-confined — the whole point of routing through
//! commands rather than sharing the driver.
//!
//! Callers come in two flavors: async connection/loop tasks await the reply
//! (`oneshot`), while the synchronous [`crate::group_graph_pipeline`] facade
//! blocks on a `std::sync::mpsc` reply. The blocking bridge is deadlock-free by
//! construction: the reply is produced by this dedicated OS thread, which never
//! blocks on (or even knows about) the caller's async runtime.

use std::collections::{BTreeMap, BTreeSet};

use tokio::sync::{mpsc, oneshot};

use zero_cache_protocol::ast::Ast;
use zero_cache_sqlite::snapshotter::SnapshotTableSpec;
use zero_cache_zql::ivm::data::Row;

use crate::graph_pipeline_driver::GraphPipelineDriver;
use crate::pipeline_driver::{PipelineDriver, PipelineError, PipelineRowChange};

/// The `Send` construction inputs for a group's [`PipelineDriver`]. Carried to
/// the worker thread so the (soon `!Send`) driver is built *inside* the thread
/// it lives on, never crossing a thread boundary.
pub struct PipelineDriverBuilder {
    pub db_file: String,
    pub app_id: String,
    pub page_cache_size_kib: Option<usize>,
    pub table_specs: BTreeMap<String, SnapshotTableSpec>,
    pub all_table_names: BTreeSet<String>,
}

impl PipelineDriverBuilder {
    pub(crate) fn build(self) -> Result<PipelineDriver, PipelineError> {
        PipelineDriver::new(
            self.db_file,
            self.app_id,
            self.page_cache_size_kib,
            self.table_specs,
            self.all_table_names,
        )
    }

    /// Builds the persistent push-graph driver (increment 7's thread-hosted
    /// driver). Called ONLY on the pipeline thread — the result is `!Send`.
    pub(crate) fn build_graph(self) -> Result<GraphPipelineDriver, PipelineError> {
        GraphPipelineDriver::new(
            self.db_file,
            self.app_id,
            self.page_cache_size_kib,
            self.table_specs,
            self.all_table_names,
        )
    }
}

/// A reply channel for one command: async callers await a `oneshot`; the
/// synchronous [`crate::group_graph_pipeline`] facade blocks on a bounded std
/// channel (capacity 1, so the pipeline thread's send never blocks either).
enum ReplyTo<T> {
    Async(oneshot::Sender<T>),
    Sync(std::sync::mpsc::SyncSender<T>),
}

impl<T> ReplyTo<T> {
    fn send(self, value: T) {
        match self {
            ReplyTo::Async(tx) => {
                let _ = tx.send(value);
            }
            ReplyTo::Sync(tx) => {
                let _ = tx.send(value);
            }
        }
    }
}

/// A command sent to the group's pipeline thread. Each carries a one-shot reply
/// channel; the async caller `await`s it. Mirrors the driver's public surface
/// (`crates/zero-cache-view-syncer/src/pipeline_driver.rs`).
enum PipelineCommand {
    AddQuery {
        query_id: String,
        ast: Box<Ast>,
        reply: ReplyTo<Result<Vec<PipelineRowChange>, PipelineError>>,
    },
    RegisterQuery {
        query_id: String,
        ast: Box<Ast>,
        rows: Vec<Row>,
        reply: ReplyTo<Result<Vec<PipelineRowChange>, PipelineError>>,
    },
    RemoveQuery {
        query_id: String,
        reply: ReplyTo<Vec<PipelineRowChange>>,
    },
    Advance {
        reply: ReplyTo<Result<Vec<PipelineRowChange>, PipelineError>>,
    },
    UsesPrehydratedRows {
        ast: Box<Ast>,
        reply: ReplyTo<bool>,
    },
    Version {
        reply: ReplyTo<Result<String, PipelineError>>,
    },
    RowSetSignature {
        query_id: String,
        reply: ReplyTo<Option<u64>>,
    },
    /// The current result rows of an active query as `Add` changes — the
    /// seed for a group's 2nd+ desirer of an already-hydrated query.
    CurrentQueryRows {
        query_id: String,
        reply: ReplyTo<Vec<PipelineRowChange>>,
    },
}

/// Error surfaced when the pipeline thread is gone (dropped/panicked) or the
/// driver failed to build. Distinct from [`PipelineError`] so callers can tell
/// "the pipeline is dead" from a query-level error.
#[derive(Debug, thiserror::Error)]
pub enum GroupPipelineError {
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error("group pipeline thread is closed")]
    Closed,
}

/// A `Send + Sync + Clone` handle to a group's thread-confined
/// [`PipelineDriver`]. This is the only thing connection tasks hold; cloning it
/// is cheap (an [`mpsc::UnboundedSender`] clone). Every method routes a command
/// to the pipeline thread and awaits its one-shot reply.
#[derive(Clone)]
pub struct GroupHandle {
    tx: mpsc::UnboundedSender<PipelineCommand>,
}

impl GroupHandle {
    /// Spawns the dedicated pipeline thread and returns a handle to it. The
    /// driver is built on the thread from `builder`; if that fails the thread
    /// exits and the first command returns [`GroupPipelineError::Closed`]. The
    /// spawned thread ends when the last [`GroupHandle`] is dropped (the command
    /// channel closes) or the process exits.
    pub fn spawn(builder: PipelineDriverBuilder) -> std::io::Result<Self> {
        let (tx, rx) = mpsc::unbounded_channel::<PipelineCommand>();
        std::thread::Builder::new()
            .name("zero-group-ivm".into())
            .spawn(move || run_pipeline_thread(builder, rx))?;
        Ok(Self { tx })
    }

    async fn call<T>(
        &self,
        make: impl FnOnce(ReplyTo<T>) -> PipelineCommand,
    ) -> Result<T, GroupPipelineError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(make(ReplyTo::Async(reply)))
            .map_err(|_| GroupPipelineError::Closed)?;
        rx.await.map_err(|_| GroupPipelineError::Closed)
    }

    /// Synchronous counterpart of [`Self::call`], blocking the calling thread on
    /// a bounded std channel until the pipeline thread replies. Deadlock-free:
    /// the reply always comes from the dedicated OS thread, which never blocks
    /// on the caller (see the module doc). Used by the sync
    /// [`crate::group_graph_pipeline::GroupGraphPipeline`] facade whose call
    /// sites (the group processor loop's transition core) are synchronous.
    fn call_blocking<T>(
        &self,
        make: impl FnOnce(ReplyTo<T>) -> PipelineCommand,
    ) -> Result<T, GroupPipelineError> {
        let (reply, rx) = std::sync::mpsc::sync_channel(1);
        self.tx
            .send(make(ReplyTo::Sync(reply)))
            .map_err(|_| GroupPipelineError::Closed)?;
        rx.recv().map_err(|_| GroupPipelineError::Closed)
    }

    pub async fn add_query(
        &self,
        query_id: impl Into<String>,
        ast: Ast,
    ) -> Result<Vec<PipelineRowChange>, GroupPipelineError> {
        let query_id = query_id.into();
        self.call(|reply| PipelineCommand::AddQuery {
            query_id,
            ast: Box::new(ast),
            reply,
        })
        .await?
        .map_err(GroupPipelineError::Pipeline)
    }

    pub async fn register_query(
        &self,
        query_id: impl Into<String>,
        ast: Ast,
        rows: Vec<Row>,
    ) -> Result<Vec<PipelineRowChange>, GroupPipelineError> {
        let query_id = query_id.into();
        self.call(|reply| PipelineCommand::RegisterQuery {
            query_id,
            ast: Box::new(ast),
            rows,
            reply,
        })
        .await?
        .map_err(GroupPipelineError::Pipeline)
    }

    pub async fn remove_query(
        &self,
        query_id: impl Into<String>,
    ) -> Result<Vec<PipelineRowChange>, GroupPipelineError> {
        let query_id = query_id.into();
        self.call(|reply| PipelineCommand::RemoveQuery { query_id, reply })
            .await
    }

    pub async fn advance(&self) -> Result<Vec<PipelineRowChange>, GroupPipelineError> {
        self.call(|reply| PipelineCommand::Advance { reply })
            .await?
            .map_err(GroupPipelineError::Pipeline)
    }

    pub async fn uses_prehydrated_rows(&self, ast: Ast) -> Result<bool, GroupPipelineError> {
        self.call(|reply| PipelineCommand::UsesPrehydratedRows {
            ast: Box::new(ast),
            reply,
        })
        .await
    }

    pub async fn version(&self) -> Result<String, GroupPipelineError> {
        self.call(|reply| PipelineCommand::Version { reply })
            .await?
            .map_err(GroupPipelineError::Pipeline)
    }

    pub async fn row_set_signature(
        &self,
        query_id: impl Into<String>,
    ) -> Result<Option<u64>, GroupPipelineError> {
        let query_id = query_id.into();
        self.call(|reply| PipelineCommand::RowSetSignature { query_id, reply })
            .await
    }

    /// The current result rows of an active query as `Add` changes.
    pub async fn current_query_rows(
        &self,
        query_id: impl Into<String>,
    ) -> Result<Vec<PipelineRowChange>, GroupPipelineError> {
        let query_id = query_id.into();
        self.call(|reply| PipelineCommand::CurrentQueryRows { query_id, reply })
            .await
    }

    // --- Blocking bridge (see `call_blocking`) -----------------------------

    pub fn add_query_blocking(
        &self,
        query_id: impl Into<String>,
        ast: Ast,
    ) -> Result<Vec<PipelineRowChange>, GroupPipelineError> {
        let query_id = query_id.into();
        self.call_blocking(|reply| PipelineCommand::AddQuery {
            query_id,
            ast: Box::new(ast),
            reply,
        })?
        .map_err(GroupPipelineError::Pipeline)
    }

    pub fn register_query_blocking(
        &self,
        query_id: impl Into<String>,
        ast: Ast,
        rows: Vec<Row>,
    ) -> Result<Vec<PipelineRowChange>, GroupPipelineError> {
        let query_id = query_id.into();
        self.call_blocking(|reply| PipelineCommand::RegisterQuery {
            query_id,
            ast: Box::new(ast),
            rows,
            reply,
        })?
        .map_err(GroupPipelineError::Pipeline)
    }

    pub fn remove_query_blocking(
        &self,
        query_id: impl Into<String>,
    ) -> Result<Vec<PipelineRowChange>, GroupPipelineError> {
        let query_id = query_id.into();
        self.call_blocking(|reply| PipelineCommand::RemoveQuery { query_id, reply })
    }

    pub fn advance_blocking(&self) -> Result<Vec<PipelineRowChange>, GroupPipelineError> {
        self.call_blocking(|reply| PipelineCommand::Advance { reply })?
            .map_err(GroupPipelineError::Pipeline)
    }

    pub fn uses_prehydrated_rows_blocking(&self, ast: Ast) -> Result<bool, GroupPipelineError> {
        self.call_blocking(|reply| PipelineCommand::UsesPrehydratedRows {
            ast: Box::new(ast),
            reply,
        })
    }

    pub fn version_blocking(&self) -> Result<String, GroupPipelineError> {
        self.call_blocking(|reply| PipelineCommand::Version { reply })?
            .map_err(GroupPipelineError::Pipeline)
    }

    pub fn row_set_signature_blocking(
        &self,
        query_id: impl Into<String>,
    ) -> Result<Option<u64>, GroupPipelineError> {
        let query_id = query_id.into();
        self.call_blocking(|reply| PipelineCommand::RowSetSignature { query_id, reply })
    }

    pub fn current_query_rows_blocking(
        &self,
        query_id: impl Into<String>,
    ) -> Result<Vec<PipelineRowChange>, GroupPipelineError> {
        let query_id = query_id.into();
        self.call_blocking(|reply| PipelineCommand::CurrentQueryRows { query_id, reply })
    }
}

/// The pipeline thread body: build the driver, then serve commands FIFO until
/// the channel closes (all handles dropped). A dropped reply receiver (caller
/// cancelled) is ignored. `blocking_recv` is valid here because this runs on a
/// plain OS thread with no async runtime.
fn run_pipeline_thread(
    builder: PipelineDriverBuilder,
    mut rx: mpsc::UnboundedReceiver<PipelineCommand>,
) {
    let mut driver = match builder.build_graph() {
        Ok(driver) => driver,
        Err(_) => {
            // Drain and fail every command: dropping each reply sender makes the
            // awaiting caller observe `Closed`.
            while rx.blocking_recv().is_some() {}
            return;
        }
    };

    while let Some(command) = rx.blocking_recv() {
        match command {
            PipelineCommand::AddQuery {
                query_id,
                ast,
                reply,
            } => {
                reply.send(driver.add_query(query_id, *ast));
            }
            PipelineCommand::RegisterQuery {
                query_id,
                ast,
                rows,
                reply,
            } => {
                reply.send(driver.register_query(query_id, *ast, rows));
            }
            PipelineCommand::RemoveQuery { query_id, reply } => {
                reply.send(driver.remove_query(&query_id));
            }
            PipelineCommand::Advance { reply } => {
                reply.send(driver.advance());
            }
            PipelineCommand::UsesPrehydratedRows { ast, reply } => {
                reply.send(driver.uses_prehydrated_rows(&ast));
            }
            PipelineCommand::Version { reply } => {
                reply.send(driver.version().map(|version| version.to_string()));
            }
            PipelineCommand::RowSetSignature { query_id, reply } => {
                reply.send(driver.row_set_signature(&query_id));
            }
            PipelineCommand::CurrentQueryRows { query_id, reply } => {
                reply.send(driver.current_query_rows(&query_id));
            }
        }
    }

    // All handles dropped: tear the driver's snapshots down cleanly.
    let _ = driver.destroy();
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::{Ast, Direction};
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_sqlite::change_log::{ChangeLog, CREATE_CHANGELOG_SCHEMA};
    use zero_cache_sqlite::replication_state::{
        init_replication_state, update_replication_watermark,
    };
    use zero_cache_sqlite::StatementRunner;

    use crate::pipeline_driver::PipelineRowChangeKind;

    fn path() -> String {
        // A process-unique counter guarantees distinct replica files even when
        // parallel tests start within the same timer tick (macOS clock
        // resolution can collide on `as_nanos()` alone).
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir()
            .join(format!(
                "zero-group-pipeline-{}-{}.db",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed),
            ))
            .to_string_lossy()
            .into_owned()
    }

    fn issue_specs() -> BTreeMap<String, SnapshotTableSpec> {
        BTreeMap::from([(
            "issue".into(),
            SnapshotTableSpec {
                name: "issue".into(),
                columns: vec!["id".into(), "active".into(), "_0_version".into()],
                column_types: BTreeMap::new(),
                primary_key: vec!["id".into()],
                unique_keys: vec![],
                min_row_version: Some("00".into()),
            },
        )])
    }

    fn issue_query() -> Ast {
        Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        }
    }

    fn builder(db_file: &str) -> PipelineDriverBuilder {
        PipelineDriverBuilder {
            db_file: db_file.to_string(),
            app_id: "zero".into(),
            page_cache_size_kib: None,
            table_specs: issue_specs(),
            all_table_names: BTreeSet::from(["issue".into()]),
        }
    }

    /// The handle forwards hydrate + advance to the thread-confined driver and
    /// returns byte-identical `PipelineRowChange`s to calling the driver
    /// directly (compare against `pipeline_driver.rs`'s
    /// `persistent_pipeline_hydrates_once_then_advances_from_snapshot_diff`).
    #[tokio::test]
    async fn group_handle_forwards_hydrate_and_advance() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (1, 1, '00')", &[])
            .unwrap();

        let handle = GroupHandle::spawn(builder(&path)).unwrap();

        let initial = handle.add_query("q", issue_query()).await.unwrap();
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].kind, PipelineRowChangeKind::Add);
        let initial_signature = handle.row_set_signature("q").await.unwrap().unwrap();
        assert_ne!(initial_signature, 0);
        assert_eq!(handle.version().await.unwrap(), "00");

        // Advance to a commit that removes the row from the result.
        writer
            .run("UPDATE issue SET active=0, _0_version='01' WHERE id=1", &[])
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

        // The query has no filter, so an update to the row is an Edit (it stays
        // in the result set), advanced from the snapshot diff via the handle.
        let changes = handle.advance().await.unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].kind, PipelineRowChangeKind::Edit);
        assert_eq!(handle.version().await.unwrap(), "01");

        // Removing the query emits a Remove for the row it still holds, and
        // clears its signature.
        let removed = handle.remove_query("q").await.unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].kind, PipelineRowChangeKind::Remove);
        assert_eq!(handle.row_set_signature("q").await.unwrap(), None);

        drop(handle);
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// `uses_prehydrated_rows` forwards to the driver. The thread now hosts the
    /// persistent push-graph driver, which ALWAYS hydrates through its own
    /// replica-backed graph — the prehydration fast path never applies.
    #[tokio::test]
    async fn group_handle_reports_prehydrated_eligibility() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();

        let handle = GroupHandle::spawn(builder(&path)).unwrap();
        assert!(!handle.uses_prehydrated_rows(issue_query()).await.unwrap());

        drop(handle);
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// A COMPLEX query (`issue` where exists a matching `label`) hydrates and
    /// then advances by PUSH through the thread-hosted graph driver: a commit
    /// inserting a label makes the parent issue enter the result across the
    /// handle — the increment-7 end-to-end shape (child-table change → join →
    /// root Add) on the group thread.
    #[tokio::test]
    async fn group_handle_advances_where_exists_query_across_a_commit() {
        use zero_cache_protocol::ast::{CorrelatedSubquery, Correlation, ExistsOp};

        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .exec("CREATE TABLE label (id INTEGER PRIMARY KEY, issue_id INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (1, 1, '00')", &[])
            .unwrap();

        let mut table_specs = issue_specs();
        table_specs.insert(
            "label".into(),
            SnapshotTableSpec {
                name: "label".into(),
                columns: vec!["id".into(), "issue_id".into(), "_0_version".into()],
                column_types: BTreeMap::new(),
                primary_key: vec!["id".into()],
                unique_keys: vec![],
                min_row_version: Some("00".into()),
            },
        );
        let builder = PipelineDriverBuilder {
            db_file: path.clone(),
            app_id: "zero".into(),
            page_cache_size_kib: None,
            table_specs,
            all_table_names: BTreeSet::from(["issue".into(), "label".into()]),
        };

        let ast = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            where_: Some(zero_cache_protocol::ast::Condition::CorrelatedSubquery {
                op: ExistsOp::Exists,
                related: CorrelatedSubquery {
                    correlation: Correlation {
                        parent_field: vec!["id".into()],
                        child_field: vec!["issue_id".into()],
                    },
                    subquery: Box::new(Ast {
                        table: "label".into(),
                        alias: Some("zsubq_labels".into()),
                        order_by: Some(vec![("id".into(), Direction::Asc)]),
                        ..Default::default()
                    }),
                    system: None,
                    hidden: None,
                },
                flip: None,
                scalar: None,
                plan_id: None,
            }),
            ..Default::default()
        };

        let handle = GroupHandle::spawn(builder).unwrap();
        // No label yet: the whereExists query hydrates empty.
        let initial = handle.add_query("q", ast).await.unwrap();
        assert!(initial.is_empty(), "no label yet: {initial:?}");

        // One commit inserts a matching label: the issue enters the result via
        // PUSH through the persistent graph, across the handle.
        writer
            .run("INSERT INTO label VALUES (7, 1, '01')", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "01",
                0,
                "label",
                &vec![("id".into(), JsonValue::Number(7.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "01").unwrap();

        let changes = handle.advance().await.unwrap();
        assert!(
            changes
                .iter()
                .any(|change| change.table == "issue" && change.kind == PipelineRowChangeKind::Add),
            "issue enters the whereExists result: {changes:?}"
        );
        assert_eq!(handle.version().await.unwrap(), "01");

        drop(handle);
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// A builder that cannot open its replica leaves the thread dead; the first
    /// command observes `Closed` rather than hanging.
    #[tokio::test]
    async fn group_handle_reports_closed_when_driver_fails_to_build() {
        let handle = GroupHandle::spawn(builder("/nonexistent/dir/replica.db")).unwrap();
        let result = handle.version().await;
        assert!(matches!(result, Err(GroupPipelineError::Closed)));
    }
}
