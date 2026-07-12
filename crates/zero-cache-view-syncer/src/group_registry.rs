//! Process-wide registry of per-client-group services — the Rust port of
//! upstream's `ServiceRunner` (`mono-src/packages/zero-cache/src/services/
//! runner.ts`), whose `#instances: Map<clientGroupID, ViewSyncer>` memoizes one
//! service per group and reaps it when the group empties.
//!
//! Each [`GroupService`] owns exactly one group's pipeline (via [`GroupHandle`])
//! — one Snapshotter, one operator graph, one CVR — shared by every connection
//! in that group, replacing today's per-WebSocket `PipelineDriver`. Connections
//! resolve their group with [`ClientGroupRegistry::get_or_create`]; the service
//! stays alive while any connection holds an `Arc<GroupService>` and is dropped
//! (its pipeline thread joined) when the last connection disconnects.
//!
//! This is the skeleton: the [`GroupService`] owns the pipeline now; the
//! group-shared CVR state and per-connection client map fold in with the
//! bootstrap wiring (redesign §6 B4), where the moved `cvr_transition_lock`
//! becomes the service's internal lock.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use zero_cache_sqlite::snapshotter::SnapshotTableSpec;

use crate::group_graph_pipeline::GroupGraphPipeline;
use crate::group_pipeline::PipelineDriverBuilder;

/// The replica-wide inputs needed to build any group's pipeline. These are
/// identical across every group in a process (they describe the shared replica),
/// so they are computed once and cloned per group.
#[derive(Clone)]
pub struct GroupBuilderDeps {
    pub db_file: String,
    pub app_id: String,
    pub page_cache_size_kib: Option<usize>,
    pub table_specs: BTreeMap<String, SnapshotTableSpec>,
    pub all_table_names: BTreeSet<String>,
}

impl GroupBuilderDeps {
    fn pipeline_builder(&self) -> PipelineDriverBuilder {
        PipelineDriverBuilder {
            db_file: self.db_file.clone(),
            app_id: self.app_id.clone(),
            page_cache_size_kib: self.page_cache_size_kib,
            table_specs: self.table_specs.clone(),
            all_table_names: self.all_table_names.clone(),
        }
    }
}

/// One client group's shared service — the port of upstream's
/// `ViewSyncerService` (`view-syncer.ts`): it owns the single pipeline for the
/// group. Held by an `Arc` shared across all of the group's connections.
pub struct GroupService {
    pub group_id: String,
    /// The group's shared query pipeline: ONE persistent push-graph driver
    /// (hosted on the group's dedicated OS thread, see
    /// [`crate::group_pipeline::GroupHandle`]) + one cross-client query
    /// ref-count, shared by every connection in the group. Every connection
    /// routes desire/undesire/advance through this one instance.
    pub pipeline: GroupGraphPipeline,
    /// The group's shared in-memory CVR (redesign §6 C2): one CVR per group,
    /// checked out/in by transitions under the group's transition lock. Starts
    /// empty; the first transition seeds it (from the durable store when
    /// configured). Dies with the service, i.e. with the group's last
    /// connection.
    pub cvr_cell: Arc<crate::group_cvr::GroupCvrCell>,
    /// The group's shared connect-time durable-CVR load. Under a burst of
    /// connections joining a NEW group (before the processor loop has populated
    /// [`cvr_cell`]), only the FIRST connection loads the durable CVR from
    /// Postgres; the rest await this `OnceCell` and clone the result — turning
    /// 300 concurrent per-connection CVR loads into one per group. Once the loop
    /// checks live state into `cvr_cell`, later connections read that instead and
    /// never consult this. The load closure is supplied by the server (it owns
    /// the CVR pool), so this stores only the loaded value.
    pub connect_cvr: tokio::sync::OnceCell<GroupConnectSeed>,
}

/// The durable CVR + row records loaded once at a group's connect time, shared
/// across the connections that join before the processor loop takes over.
#[derive(Clone)]
pub struct GroupConnectSeed {
    pub cvr: crate::cvr_types::Cvr,
    pub row_records: std::sync::Arc<Vec<crate::cvr_types::RowRecord>>,
}

impl GroupService {
    /// Starts a fresh service for `group_id`, building its shared pipeline over
    /// the replica. Fails only if the driver cannot open the replica.
    fn start(group_id: &str, deps: &GroupBuilderDeps) -> Result<Arc<Self>, GroupStartError> {
        let pipeline = GroupGraphPipeline::new(deps.pipeline_builder())?;
        Ok(Arc::new(Self {
            group_id: group_id.to_string(),
            pipeline,
            cvr_cell: Arc::new(crate::group_cvr::GroupCvrCell::default()),
            connect_cvr: tokio::sync::OnceCell::new(),
        }))
    }
}

/// Failure to start a group service — currently only a pipeline build error
/// (the replica could not be opened for the group's driver).
#[derive(Debug, thiserror::Error)]
pub enum GroupStartError {
    #[error(transparent)]
    Pipeline(#[from] crate::pipeline_driver::PipelineError),
}

/// Process-wide `clientGroupID -> GroupService` registry. Ported from
/// `ServiceRunner` (`runner.ts:8`): weak-referenced so a service is dropped when
/// its last connection releases it, and reaped from the map on the next lookup.
pub struct ClientGroupRegistry {
    inner: Mutex<BTreeMap<String, Weak<GroupService>>>,
    deps: GroupBuilderDeps,
}

impl ClientGroupRegistry {
    pub fn new(deps: GroupBuilderDeps) -> Self {
        Self {
            inner: Mutex::new(BTreeMap::new()),
            deps,
        }
    }

    /// Resolves the service for `group_id`, creating it if no live one exists —
    /// the port of `ServiceRunner.getService` (`runner.ts:28`). Concurrent
    /// callers for the same new group race to insert; the loser adopts the
    /// winner's service, so a group is never served by two pipelines.
    pub fn get_or_create(&self, group_id: &str) -> Result<Arc<GroupService>, GroupStartError> {
        {
            // Fast path: a live service already exists.
            let map = self.lock();
            if let Some(existing) = map.get(group_id).and_then(Weak::upgrade) {
                return Ok(existing);
            }
        }
        // Build outside the map lock (thread spawn); then insert under the lock,
        // re-checking so a concurrent creator wins and we drop our spare.
        let candidate = GroupService::start(group_id, &self.deps)?;
        let mut map = self.lock();
        if let Some(existing) = map.get(group_id).and_then(Weak::upgrade) {
            return Ok(existing);
        }
        map.insert(group_id.to_string(), Arc::downgrade(&candidate));
        // Reap any entries whose services have since been dropped.
        map.retain(|_, weak| weak.strong_count() > 0);
        Ok(candidate)
    }

    /// Number of live (upgradeable) services. Test/observability helper.
    pub fn live_len(&self) -> usize {
        self.lock()
            .values()
            .filter(|weak| weak.strong_count() > 0)
            .count()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Weak<GroupService>>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_sqlite::change_log::CREATE_CHANGELOG_SCHEMA;
    use zero_cache_sqlite::replication_state::init_replication_state;
    use zero_cache_sqlite::StatementRunner;

    fn path() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir()
            .join(format!(
                "zero-group-registry-{}-{}.db",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed),
            ))
            .to_string_lossy()
            .into_owned()
    }

    /// Creates a minimal replica file the pipeline thread can open a snapshot
    /// over, and returns registry deps pointing at it.
    fn deps_over_fresh_replica() -> (String, GroupBuilderDeps) {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, _0_version TEXT)")
            .unwrap();
        drop(writer);
        let deps = GroupBuilderDeps {
            db_file: path.clone(),
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
        };
        (path, deps)
    }

    #[test]
    fn same_group_id_shares_one_service() {
        let (path, deps) = deps_over_fresh_replica();
        let registry = ClientGroupRegistry::new(deps);

        let a = registry.get_or_create("g1").unwrap();
        let b = registry.get_or_create("g1").unwrap();
        assert!(Arc::ptr_eq(&a, &b), "same group id must share one service");
        assert_eq!(registry.live_len(), 1);

        let c = registry.get_or_create("g2").unwrap();
        assert!(
            !Arc::ptr_eq(&a, &c),
            "distinct groups get distinct services"
        );
        assert_eq!(registry.live_len(), 2);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn service_is_reaped_after_last_connection_drops() {
        let (path, deps) = deps_over_fresh_replica();
        let registry = ClientGroupRegistry::new(deps);

        let first = registry.get_or_create("g1").unwrap();
        let first_ptr = Arc::as_ptr(&first);
        drop(first);
        // The service (and its pipeline thread) is now dropped; a re-lookup
        // must build a FRESH service, not resurrect the dead Weak.
        let second = registry.get_or_create("g1").unwrap();
        assert_ne!(
            first_ptr,
            Arc::as_ptr(&second),
            "a group must be rebuilt after its last connection drops"
        );
        assert_eq!(registry.live_len(), 1);

        let _ = std::fs::remove_file(path);
    }

    /// The service's shared pipeline is usable end-to-end through the registry.
    #[test]
    fn resolved_service_pipeline_answers() {
        let (path, deps) = deps_over_fresh_replica();
        let registry = ClientGroupRegistry::new(deps);
        let service = registry.get_or_create("g1").unwrap();
        assert_eq!(service.pipeline.version().unwrap(), "00");
        let _ = std::fs::remove_file(path);
    }
}
