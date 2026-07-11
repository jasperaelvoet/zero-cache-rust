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

use std::sync::Mutex;

use zero_cache_protocol::ast::Ast;

use crate::group_pipeline::PipelineDriverBuilder;
use crate::group_query_set::{GroupQuerySet, QueryTransition};
use crate::pipeline_driver::{PipelineDriver, PipelineError, PipelineRowChange};

/// The shared driver + query ref-count for one client group. Every connection
/// in the group calls through the same instance; all methods are synchronous so
/// they drop into the existing sync handler call sites unchanged.
pub struct SharedGroupPipeline {
    driver: Mutex<PipelineDriver>,
    query_set: Mutex<GroupQuerySet>,
}

impl SharedGroupPipeline {
    pub fn new(builder: PipelineDriverBuilder) -> Result<Self, PipelineError> {
        Ok(Self {
            driver: Mutex::new(builder.build()?),
            query_set: Mutex::new(GroupQuerySet::new()),
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
        let mut driver = self.driver();
        removed
            .iter()
            .flat_map(|query_id| driver.remove_query(query_id))
            .collect()
    }

    /// Advances the shared pipeline to the replica head once, returning the
    /// changes across all active queries. (Fanning one group advance out to
    /// every connection's poke is the remaining B4 wiring; a single-connection
    /// group — the conformance and bench shape — advances correctly here.)
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
}
