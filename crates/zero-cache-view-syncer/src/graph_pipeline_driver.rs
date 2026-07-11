//! Persistent per-query push graphs (piece 2, redesign §6 / group-loop-plan
//! increment 6).
//!
//! [`GraphPipelineDriver`] is the push-incremental successor to
//! [`crate::pipeline_driver::PipelineDriver`]. Where `PipelineDriver::advance`
//! re-derives every complex query by re-fetching a *transient* graph
//! (`hydrate_via_graph`, O(result)), this driver holds ONE persistent operator
//! graph per query — built once at hydration, with a
//! [`Collector`](zero_cache_zql::ivm::collector::Collector) attached at its
//! root — and advances it by PUSHING the individual [`SourceChange`]s of each
//! commit through that graph. That is true O(change) incremental advance,
//! upstream's `PipelineDriver.#advance` (`pipeline-driver.ts:983-1030`).
//!
//! It is deliberately `!Send` (each query owns `Rc` graph nodes), so — unlike
//! `PipelineDriver` — it cannot sit behind a `Mutex`; increment 7 hosts it on
//! the [`crate::group_pipeline::GroupHandle`] thread. THIS increment only makes
//! it standalone and oracle-gated (see the test module): for every
//! `pipeline_driver` fixture, a commit stream pushed through this driver
//! produces the same materialized row set as a fresh
//! `PipelineDriver::add_query` re-derivation (the oracle).
//!
//! ## How the sources stay snapshot-fresh (the chosen option)
//!
//! Each query owns its own persistent [`SqliteSource`]s (one per
//! `(table, ordering)` its graph references), each holding an independent
//! `BEGIN CONCURRENT` snapshot handle over the replica file. Between commits a
//! source holds the *previous* snapshot; on [`advance`](Self::advance) the
//! driver
//!   1. pushes ALL of the commit's [`SourceChange`]s into the sources (a
//!      mid-push `fetch` sees prev-snapshot + every in-flight change so far via
//!      the source's ACCUMULATING pending-change overlay — increments 5 + 6b),
//!      then
//!   2. AFTER all pushes, `set_db`s every source to a fresh snapshot at replica
//!      head (upstream's "set DBs only after all pushes",
//!      `pipeline-driver.ts:1039-1046`), which also clears the overlay.
//!
//! This is the "simpler-but-correct alternative" the plan sanctions: it keeps
//! the driver entirely within `zero-cache-view-syncer` (sharing the
//! `Snapshotter`'s own `BEGIN CONCURRENT` connection across sources would need
//! `Snapshot`'s db to become `Rc`-shareable in `zero-cache-sqlite`, which is
//! out of scope here). Per-query (rather than driver-shared) sources also make
//! [`remove_query`](Self::remove_query) trivially clean: dropping the query
//! drops its whole graph, with no sibling push edges to splice out.
//!
//! ## Multi-change commits (increment 6b)
//!
//! The port's [`SqliteSource`] is READ-ONLY — `push` never writes SQLite (the
//! replicator owns writes) — but its overlay now ACCUMULATES every change of a
//! commit, so a mid-push `fetch` sees prev-snapshot + all prior same-commit
//! changes, the exact effect upstream's zqlite `TableSource.push` gets by
//! writing each change into its prev-snapshot connection
//! (`table-source.ts:#writeChange`). This is what makes a commit that mutates
//! several rows feeding the SAME join parent's relationship (e.g. deleting two
//! of a parent's children at once, or deleting one child and adding another to
//! the same parent) advance correctly by push: [`push_advance_query`] pushes
//! ALL of the commit's [`SourceChange`]s into the sources — each accumulating —
//! before draining the [`Collector`] and before `set_db`, so every join re-fetch
//! sees the fully consistent post-change child set. The oracle streams below
//! therefore include multi-row commits through joins/exists/take, each asserted
//! equal to a fresh re-derivation on the PUSH path.
//!
//! The one shape still re-derived (not pushed) is OR-of-correlated
//! (`... OR EXISTS(...)`) — see [`ast_needs_rebuild_advance`] — because of the
//! zql `FanOut`/`FanIn` limitation, not the overlay.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use zero_cache_protocol::ast::{referenced_tables, Ast, Condition, Ordering};
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_sqlite::snapshotter::{
    SnapshotChange, SnapshotError, SnapshotTableSpec, Snapshotter,
};
use zero_cache_sqlite::{SqliteSource, StatementRunner};
use zero_cache_zql::builder::pipeline::{build_pipeline, BuildDelegate};
use zero_cache_zql::ivm::change::SourceChange;
use zero_cache_zql::ivm::collector::{Collector, CollectorChangeKind, CollectorRowChange};
use zero_cache_zql::ivm::data::Row;
use zero_cache_zql::ivm::memory_storage::MemoryStorage;
use zero_cache_zql::ivm::operator::{FetchRequest, Input, Node, Storage};

use zero_cache_types::pg_data_type::ValueType as ColumnValueType;

use crate::pipeline_driver::{
    additions, clamp_row_version, diff_rows, get, graph_child_hops, insert_row,
    materialized_key_for, referenced_sources, signature_for_rows, source_key, source_ordering,
    sql_row_to_zql, to_source_column_types, MaterializedRow, PipelineError, PipelineRowChange,
    PipelineRowChangeKind,
};
use crate::row_set_signature::row_id_signature_unit;

/// One active query's PERSISTENT push graph and materialized result.
struct GraphQuery {
    ast: Ast,
    /// The pipeline root; a [`Collector`] is attached as its output. Kept alive
    /// (with `sources`) so the whole `Rc` graph persists between advances.
    root: Rc<dyn Input>,
    collector: Rc<Collector>,
    /// This query's own sources, `(table, source)`. A commit's changes for
    /// `table` are pushed into every source whose table matches (a query can
    /// hold >1 source for a table when the root and a child order it
    /// differently). Not shared with other queries — see the module doc.
    sources: Vec<(String, Rc<SqliteSource>)>,
    referenced_tables: BTreeSet<String>,
    /// This query cannot be maintained by push and is re-derived (rebuilt) on
    /// every relevant advance — see [`ast_needs_rebuild_advance`]. Today that is
    /// only OR-of-correlated (`... OR EXISTS(...)`): a child-table change enters
    /// one `FanOut` branch's join directly, bypassing the `FanOut`, so the
    /// `FanIn` collapse protocol never fires (a zql fan-in/out limitation, out
    /// of scope here). Such queries fall back to the O(result) re-fetch the
    /// current `PipelineDriver` already uses for all complex queries.
    rebuild_only: bool,
    /// The current materialized rows (keyed by [`materialized_key_for`]), kept
    /// so [`remove_query`](GraphPipelineDriver::remove_query) /
    /// [`current_query_rows`](GraphPipelineDriver::current_query_rows) can read
    /// the set back and so a [`CollectorChangeKind::Remove`]/`Edit` can recover
    /// the prior row body the collector omits.
    rows: BTreeMap<String, MaterializedRow>,
}

/// The pieces a freshly built query graph yields.
struct BuiltGraph {
    root: Rc<dyn Input>,
    collector: Rc<Collector>,
    sources: Vec<(String, Rc<SqliteSource>)>,
    rows: BTreeMap<String, MaterializedRow>,
}

/// Persistent push-graph pipeline owner for one Zero client group. `!Send`
/// (holds `Rc` graphs). Same public surface as
/// [`crate::pipeline_driver::PipelineDriver`] where the oracle test and the
/// (later) `GroupHandle` command enum need it.
pub struct GraphPipelineDriver {
    snapshotter: Snapshotter,
    db_file: String,
    page_cache_size_kib: Option<usize>,
    table_specs: BTreeMap<String, SnapshotTableSpec>,
    all_table_names: BTreeSet<String>,
    queries: BTreeMap<String, GraphQuery>,
    row_set_signatures: BTreeMap<String, u64>,
}

impl GraphPipelineDriver {
    pub fn new(
        db_file: impl Into<String>,
        app_id: impl Into<String>,
        page_cache_size_kib: Option<usize>,
        table_specs: BTreeMap<String, SnapshotTableSpec>,
        all_table_names: BTreeSet<String>,
    ) -> Result<Self, PipelineError> {
        let db_file = db_file.into();
        let mut snapshotter = Snapshotter::new(db_file.clone(), app_id, page_cache_size_kib);
        snapshotter.init()?;
        Ok(Self {
            snapshotter,
            db_file,
            page_cache_size_kib,
            table_specs,
            all_table_names,
            queries: BTreeMap::new(),
            row_set_signatures: BTreeMap::new(),
        })
    }

    pub fn version(&self) -> Result<&str, PipelineError> {
        Ok(&self.snapshotter.current()?.version)
    }

    pub fn row_set_signature(&self, query_id: &str) -> Option<u64> {
        self.row_set_signatures.get(query_id).copied()
    }

    /// Builds a fresh, replica-backed [`SqliteSource`] for `table`, opening a
    /// new `BEGIN CONCURRENT` snapshot at replica head — byte-identical to
    /// `PipelineDriver::build_graph_source`, so the two drivers hydrate the
    /// same subset for a bounded+ordered query.
    fn build_graph_source(
        &self,
        table: &str,
        order_by: Option<&Ordering>,
    ) -> Result<Rc<SqliteSource>, PipelineError> {
        let spec = self
            .table_specs
            .get(table)
            .ok_or_else(|| PipelineError::UnknownTable(table.to_string()))?;
        let db = StatementRunner::open_snapshot(&self.db_file, self.page_cache_size_kib)?;
        let ordering: Ordering = source_ordering(order_by, &spec.primary_key);
        Ok(Rc::new(SqliteSource::with_column_types(
            db,
            spec.name.clone(),
            spec.primary_key.clone(),
            ordering,
            spec.columns.clone(),
            to_source_column_types(&spec.column_types, &spec.columns),
        )))
    }

    /// Builds a query's PERSISTENT graph: one [`SqliteSource`] per referenced
    /// `(table, ordering)`, `build_pipeline` over them, an initial `root.fetch`
    /// (which also primes `Take`/`Exists` operator storage) flattened into the
    /// materialized rows exactly as `PipelineDriver::insert_graph_nodes`, and a
    /// [`Collector`] attached at the root for subsequent pushes.
    fn build_query_graph(&self, ast: &Ast) -> Result<BuiltGraph, PipelineError> {
        let mut source_map: HashMap<String, Rc<SqliteSource>> = HashMap::new();
        let mut sources: Vec<(String, Rc<SqliteSource>)> = Vec::new();
        for (table, order_by) in referenced_sources(ast) {
            let key = source_key(&table, order_by.as_ref());
            if let Entry::Vacant(entry) = source_map.entry(key) {
                let src = self.build_graph_source(&table, order_by.as_ref())?;
                entry.insert(src.clone());
                sources.push((table, src));
            }
        }

        let get_source = |table: &str, order_by: Option<&Ordering>| -> Rc<dyn Input> {
            source_map
                .get(&source_key(table, order_by))
                .cloned()
                .unwrap_or_else(|| panic!("graph source for `{table}` not pre-built"))
                as Rc<dyn Input>
        };
        let create_storage = |_name: &str| -> Rc<dyn Storage> { Rc::new(MemoryStorage::default()) };
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let root = build_pipeline(ast, &delegate);

        // Drain the initial rows (also priming Take/Exists storage), THEN attach
        // the sink — a fetch never pushes, so the collector stays empty here.
        let roots: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        let mut rows = BTreeMap::new();
        self.flatten_nodes(ast, &roots, &mut rows)?;

        let collector = Collector::new(root.get_schema());
        root.set_output(collector.clone());

        Ok(BuiltGraph {
            root,
            collector,
            sources,
            rows,
        })
    }

    /// Flattens hydrated graph nodes into `output` keyed by `materialized_key` —
    /// the exact recursion of `PipelineDriver::insert_graph_nodes` (root rows
    /// plus each `related`/`EXISTS` relationship child, `_0_version` clamped to
    /// the table's `min_row_version`), so graph hydration is byte-identical to
    /// the oracle's.
    fn flatten_nodes(
        &self,
        ast: &Ast,
        nodes: &[Node],
        output: &mut BTreeMap<String, MaterializedRow>,
    ) -> Result<(), PipelineError> {
        let min_row_version = self
            .table_specs
            .get(&ast.table)
            .and_then(|spec| spec.min_row_version.clone());
        for node in nodes {
            let row = clamp_row_version(node.row.clone(), min_row_version.as_deref());
            insert_row(&ast.table, row, &self.table_specs, output)?;
        }
        for (subquery, alias) in graph_child_hops(ast) {
            let children: Vec<Node> = nodes
                .iter()
                .flat_map(|node| node.relationships.get(&alias).cloned().unwrap_or_default())
                .collect();
            self.flatten_nodes(subquery, &children, output)?;
        }
        Ok(())
    }

    pub fn add_query(
        &mut self,
        query_id: impl Into<String>,
        ast: Ast,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let query_id = query_id.into();
        if self.queries.contains_key(&query_id) {
            return Err(PipelineError::DuplicateQuery(query_id));
        }
        let built = self.build_query_graph(&ast)?;
        self.row_set_signatures
            .insert(query_id.clone(), signature_for_rows(built.rows.values())?);
        let changes = additions(&query_id, &built.rows);
        self.queries.insert(
            query_id,
            GraphQuery {
                referenced_tables: referenced_tables(&ast),
                rebuild_only: ast_needs_rebuild_advance(&ast),
                ast,
                root: built.root,
                collector: built.collector,
                sources: built.sources,
                rows: built.rows,
            },
        );
        Ok(changes)
    }

    /// The graph driver ALWAYS hydrates through the replica-backed graph, so
    /// the prehydration fast path (reusing a caller's root-table fetch) never
    /// applies. Reported for surface parity with `PipelineDriver`.
    pub fn uses_prehydrated_rows(&self, _ast: &Ast) -> bool {
        false
    }

    /// Surface-parity alias: the graph hydrates from the replica, so the
    /// caller-supplied `rows` are ignored and this is exactly
    /// [`add_query`](Self::add_query). (`uses_prehydrated_rows` returns `false`,
    /// so the loop never routes here for this driver.)
    pub fn register_query(
        &mut self,
        query_id: impl Into<String>,
        ast: Ast,
        _rows: Vec<Row>,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        self.add_query(query_id, ast)
    }

    pub fn remove_query(&mut self, query_id: &str) -> Vec<PipelineRowChange> {
        self.row_set_signatures.remove(query_id);
        self.queries
            .remove(query_id)
            .map(|query| {
                // Dropping `query` drops the whole `Rc` graph (sources included;
                // they are per-query, so nothing else references them and no
                // sibling push edge survives).
                query
                    .rows
                    .into_values()
                    .map(|entry| PipelineRowChange {
                        query_id: query_id.to_string(),
                        table: entry.table,
                        kind: PipelineRowChangeKind::Remove,
                        row: entry.row,
                        old_row: None,
                        row_key: entry.row_key,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The current result rows of an active query as `Add` changes (see
    /// `PipelineDriver::current_query_rows`).
    pub fn current_query_rows(&self, query_id: &str) -> Vec<PipelineRowChange> {
        self.queries
            .get(query_id)
            .map(|query| additions(query_id, &query.rows))
            .unwrap_or_default()
    }

    /// Advances every affected query by PUSHING the commit's changes through its
    /// persistent graph. Port of `PipelineDriver.#advance`
    /// (`pipeline-driver.ts:983-1030`): the snapshot diff's
    /// [`SnapshotChange`]s become [`SourceChange`]s (prev↔next paired by PK →
    /// `Edit`; unpaired prev → `Remove`; bare next → `Add`), each pushed into
    /// the query's sources; then every source is swapped to the new head.
    pub fn advance(&mut self) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let diff = match self
            .snapshotter
            .advance(&self.table_specs, &self.all_table_names)
        {
            Ok(diff) => diff,
            // A reset (schema/truncate/permissions/timeout) invalidates every
            // persistent graph — rebuild them all at the new head (redesign
            // §5.3). The leapfrog already moved `current` to head.
            Err(SnapshotError::Reset { .. }) => return self.rebuild_all_after_reset(),
            Err(e) => return Err(e.into()),
        };

        let changed_tables: BTreeSet<String> =
            diff.rows.iter().map(|row| row.table.clone()).collect();

        // The SourceChanges for each diff entry are query-independent — derive
        // them once.
        let prepared: Vec<(String, Vec<SourceChange>)> = diff
            .rows
            .iter()
            .map(|change| {
                (
                    change.table.clone(),
                    snapshot_change_to_source_changes(change, self.table_specs.get(&change.table)),
                )
            })
            .collect();

        let ids: Vec<String> = self
            .queries
            .iter()
            .filter(|(_, query)| !query.referenced_tables.is_disjoint(&changed_tables))
            .map(|(id, _)| id.clone())
            .collect();

        let mut changes = Vec::new();
        for id in ids {
            let push_count: usize = {
                let query = &self.queries[&id];
                prepared
                    .iter()
                    .filter(|(table, _)| query.referenced_tables.contains(table))
                    .map(|(_, source_changes)| source_changes.len())
                    .sum()
            };
            // Rebuild when the query is not push-maintainable (OR-of-correlated,
            // see `rebuild_only`) OR when the runaway-push guard (redesign §5.3,
            // first cut) trips: a commit pushing far more changes into a query
            // than its current size is re-derived instead — same result,
            // bounded cost. The floor is generous so ordinary commits always
            // take the O(change) push path.
            let query = &self.queries[&id];
            let query_changes =
                if query.rebuild_only || push_count > runaway_floor(query.rows.len()) {
                    self.rebuild_query(&id)?
                } else {
                    self.push_advance_query(&id, &prepared)?
                };
            changes.extend(query_changes);
        }

        self.apply_signature_changes(&changes)?;
        Ok(changes)
    }

    /// Pushes ALL of the commit's changes through one query's persistent graph
    /// — each accumulating in its source's overlay so a later same-commit
    /// fetch (e.g. a join re-deriving a parent's children) sees every prior
    /// change — THEN swaps every source to the new head (which clears the
    /// overlays), and finally drains + maps its collector. The push-all-first
    /// ordering is what makes multi-row commits through a join correct
    /// (increment 6b).
    fn push_advance_query(
        &mut self,
        id: &str,
        prepared: &[(String, Vec<SourceChange>)],
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        {
            let query = self.queries.get(id).expect("selected query exists");
            for (table, source_changes) in prepared {
                if !query.referenced_tables.contains(table) {
                    continue;
                }
                for (source_table, source) in &query.sources {
                    if source_table == table {
                        for change in source_changes {
                            source.push(change.clone());
                        }
                    }
                }
            }
        }
        // Upstream ordering: set every source's DB to the new head only AFTER
        // all pushes for the commit have flowed through the graph.
        {
            let query = self.queries.get(id).expect("selected query exists");
            for (_, source) in &query.sources {
                source.set_db(StatementRunner::open_snapshot(
                    &self.db_file,
                    self.page_cache_size_kib,
                )?);
            }
        }
        let collected = self
            .queries
            .get(id)
            .expect("selected query exists")
            .collector
            .take();
        Ok(self.map_collected(id, collected))
    }

    /// Maps drained [`CollectorRowChange`]s to [`PipelineRowChange`]s, applying
    /// the `_0_version` `min_row_version` clamp and updating the query's
    /// materialized `rows` (so a later `Remove`/`Edit` recovers the prior row
    /// body the collector omits, and `current_query_rows`/`remove_query` stay
    /// accurate).
    fn map_collected(
        &mut self,
        id: &str,
        collected: Vec<CollectorRowChange>,
    ) -> Vec<PipelineRowChange> {
        let specs = &self.table_specs;
        let query = self.queries.get_mut(id).expect("selected query exists");
        let mut out = Vec::with_capacity(collected.len());
        for change in collected {
            let min = specs
                .get(&change.table)
                .and_then(|spec| spec.min_row_version.as_deref());
            let row_key: BTreeMap<String, JsonValue> = change.row_key.iter().cloned().collect();
            let key = materialized_key_for(&change.table, &row_key);
            match change.kind {
                CollectorChangeKind::Add => {
                    let row =
                        clamp_row_version(change.row.expect("collector Add carries a row"), min);
                    query.rows.insert(
                        key,
                        MaterializedRow {
                            table: change.table.clone(),
                            row: row.clone(),
                            row_key: row_key.clone(),
                        },
                    );
                    out.push(PipelineRowChange {
                        query_id: id.to_string(),
                        table: change.table,
                        kind: PipelineRowChangeKind::Add,
                        row,
                        old_row: None,
                        row_key,
                    });
                }
                CollectorChangeKind::Edit => {
                    let row =
                        clamp_row_version(change.row.expect("collector Edit carries a row"), min);
                    let old_row = query.rows.get(&key).map(|entry| entry.row.clone());
                    query.rows.insert(
                        key,
                        MaterializedRow {
                            table: change.table.clone(),
                            row: row.clone(),
                            row_key: row_key.clone(),
                        },
                    );
                    out.push(PipelineRowChange {
                        query_id: id.to_string(),
                        table: change.table,
                        kind: PipelineRowChangeKind::Edit,
                        row,
                        old_row,
                        row_key,
                    });
                }
                CollectorChangeKind::Remove => {
                    let existing = query.rows.remove(&key);
                    let row = existing
                        .map(|entry| entry.row)
                        .unwrap_or_else(|| change.row_key.clone());
                    out.push(PipelineRowChange {
                        query_id: id.to_string(),
                        table: change.table,
                        kind: PipelineRowChangeKind::Remove,
                        row,
                        old_row: None,
                        row_key,
                    });
                }
            }
        }
        out
    }

    /// Rebuilds one query's persistent graph at replica head and diffs its old
    /// vs new materialized rows — the runaway-guard fallback (and the per-query
    /// unit of the reset rebuild).
    fn rebuild_query(&mut self, id: &str) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let ast = self
            .queries
            .get(id)
            .expect("selected query exists")
            .ast
            .clone();
        let built = self.build_query_graph(&ast)?;
        let old_rows = std::mem::take(&mut self.queries.get_mut(id).expect("exists").rows);
        let changes = diff_rows(id, &old_rows, &built.rows);
        let query = self.queries.get_mut(id).expect("exists");
        query.root = built.root;
        query.collector = built.collector;
        query.sources = built.sources;
        query.rows = built.rows;
        Ok(changes)
    }

    /// Rebuilds every query after a `SnapshotError::Reset` (redesign §5.3).
    fn rebuild_all_after_reset(&mut self) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let ids: Vec<String> = self.queries.keys().cloned().collect();
        let mut changes = Vec::new();
        for id in ids {
            changes.extend(self.rebuild_query(&id)?);
        }
        self.apply_signature_changes(&changes)?;
        Ok(changes)
    }

    /// XORs each Add/Remove's per-row unit into its query's running row-set
    /// signature — identical bookkeeping to `PipelineDriver::apply_signature_
    /// changes`.
    fn apply_signature_changes(
        &mut self,
        changes: &[PipelineRowChange],
    ) -> Result<(), PipelineError> {
        for change in changes {
            if matches!(
                change.kind,
                PipelineRowChangeKind::Add | PipelineRowChangeKind::Remove
            ) {
                let unit = row_id_signature_unit(&zero_cache_types::row_key::RowId::new(
                    "public",
                    change.table.clone(),
                    change
                        .row_key
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                ))?;
                *self
                    .row_set_signatures
                    .entry(change.query_id.clone())
                    .or_default() ^= unit;
            }
        }
        Ok(())
    }

    pub fn destroy(self) -> Result<(), PipelineError> {
        self.snapshotter.destroy()?;
        Ok(())
    }
}

/// Whether `ast` must advance by re-derivation rather than push. True iff a
/// correlated subquery sits beneath an `Or` anywhere in the query (the root
/// `where`, or recursively in any `related`/`whereExists` subquery): the zql
/// `FanOut`/`FanIn` push protocol does not coordinate a child-table change that
/// enters one OR branch's join directly (bypassing the fan-out), so those
/// queries fall back to the O(result) re-fetch — the same path today's
/// `PipelineDriver` uses for every complex query.
fn ast_needs_rebuild_advance(ast: &Ast) -> bool {
    if let Some(condition) = &ast.where_ {
        if condition_has_correlated_under_or(condition) {
            return true;
        }
        for subquery in correlated_subquery_asts(condition) {
            if ast_needs_rebuild_advance(subquery) {
                return true;
            }
        }
    }
    for csq in ast.related.iter().flatten() {
        if ast_needs_rebuild_advance(&csq.subquery) {
            return true;
        }
    }
    false
}

/// Whether any `CorrelatedSubquery` appears beneath an `Or` in `condition`.
/// Mirrors `builder::pipeline`'s private `condition_has_correlated_under_or`.
fn condition_has_correlated_under_or(condition: &Condition) -> bool {
    fn has_correlated(condition: &Condition) -> bool {
        match condition {
            Condition::Simple { .. } => false,
            Condition::CorrelatedSubquery { .. } => true,
            Condition::And { conditions } | Condition::Or { conditions } => {
                conditions.iter().any(has_correlated)
            }
        }
    }
    match condition {
        Condition::Simple { .. } | Condition::CorrelatedSubquery { .. } => false,
        Condition::Or { conditions } => conditions.iter().any(has_correlated),
        Condition::And { conditions } => conditions.iter().any(condition_has_correlated_under_or),
    }
}

/// Every correlated-subquery AST reachable from `condition` (under `And`/`Or`).
fn correlated_subquery_asts(condition: &Condition) -> Vec<&Ast> {
    fn collect<'a>(condition: &'a Condition, out: &mut Vec<&'a Ast>) {
        match condition {
            Condition::Simple { .. } => {}
            Condition::CorrelatedSubquery { related, .. } => out.push(&related.subquery),
            Condition::And { conditions } | Condition::Or { conditions } => {
                for c in conditions {
                    collect(c, out);
                }
            }
        }
    }
    let mut out = Vec::new();
    collect(condition, &mut out);
    out
}

/// First-cut runaway floor: rebuild-via-refetch only kicks in when a single
/// commit would push more than this many changes into a query. Generous
/// (never triggered by ordinary commits) so the O(change) push path is the
/// rule and rebuild the rare exception.
fn runaway_floor(current_rows: usize) -> usize {
    current_rows.saturating_mul(4).max(256)
}

/// Maps one [`SnapshotChange`] to the [`SourceChange`]s to push, per upstream
/// `PipelineDriver.#advance` (`pipeline-driver.ts:990-1024`): each `prev` whose
/// primary key matches `next` pairs into a single `Edit`; any other `prev` is a
/// `Remove` (a unique-key conflict row); a `next` with no PK-matching `prev` is
/// an `Add`. Raw replica values are converted with the table's declared column
/// types and `_0_version` clamp (`sql_row_to_zql`), so pushed rows match what a
/// source `fetch` produces.
fn snapshot_change_to_source_changes(
    change: &SnapshotChange,
    spec: Option<&SnapshotTableSpec>,
) -> Vec<SourceChange> {
    let empty_types: BTreeMap<String, ColumnValueType> = BTreeMap::new();
    let column_types = spec.map(|spec| &spec.column_types).unwrap_or(&empty_types);
    let min = spec.and_then(|spec| spec.min_row_version.as_deref());
    let primary_key: &[String] = spec.map(|spec| spec.primary_key.as_slice()).unwrap_or(&[]);

    let next: Option<Row> = change
        .next_value
        .as_ref()
        .map(|row| sql_row_to_zql(row.clone(), column_types, min));

    let mut out = Vec::new();
    let mut edit_old: Option<Row> = None;
    for prev in &change.prev_values {
        let prev_zql = sql_row_to_zql(prev.clone(), column_types, min);
        match &next {
            Some(next_row) if pk_equal(&prev_zql, next_row, primary_key) => {
                edit_old = Some(prev_zql);
            }
            _ => out.push(SourceChange::Remove(prev_zql)),
        }
    }
    if let Some(next_row) = next {
        match edit_old {
            Some(old_row) => out.push(SourceChange::Edit {
                row: next_row,
                old_row,
            }),
            None => out.push(SourceChange::Add(next_row)),
        }
    }
    out
}

/// Whether two rows agree on every `primary_key` column.
fn pk_equal(a: &Row, b: &Row, primary_key: &[String]) -> bool {
    primary_key
        .iter()
        .all(|column| get(a, column) == get(b, column))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::{
        Bound, ColumnReference, Condition, CorrelatedSubquery, Correlation, Direction, ExistsOp,
        LiteralValue, SimpleOperator, ValuePosition,
    };
    use zero_cache_sqlite::change_log::{ChangeLog, CREATE_CHANGELOG_SCHEMA};
    use zero_cache_sqlite::replication_state::{
        init_replication_state, update_replication_watermark,
    };
    use zero_cache_sqlite::StatementRunner;

    use crate::pipeline_driver::PipelineDriver;

    // ---- Oracle gate scaffolding ----
    //
    // For every fixture, a commit stream pushed through `GraphPipelineDriver`
    // (applied to a running row set per query) must equal a FRESH
    // `PipelineDriver::add_query` re-derivation at the same replica head — the
    // oracle (its hydration is `hydrate_via_graph`, the existing test oracle).
    // Rows are compared as `(table, row_key) -> field map` sets, so both row
    // identity AND every field value (booleans as `true`/`false`, clamped
    // versions, …) are checked, order-insensitively.

    /// A materialized row set: `materialized_key` -> (table, field map).
    type RowSet = BTreeMap<String, (String, BTreeMap<String, JsonValue>)>;

    fn fields(row: &Row) -> BTreeMap<String, JsonValue> {
        row.iter().cloned().collect()
    }

    fn apply_changes(set: &mut RowSet, changes: &[PipelineRowChange]) {
        for change in changes {
            let key = materialized_key_for(&change.table, &change.row_key);
            match change.kind {
                PipelineRowChangeKind::Add | PipelineRowChangeKind::Edit => {
                    set.insert(key, (change.table.clone(), fields(&change.row)));
                }
                PipelineRowChangeKind::Remove => {
                    set.remove(&key);
                }
            }
        }
    }

    fn from_changes(changes: &[PipelineRowChange]) -> RowSet {
        let mut set = RowSet::new();
        apply_changes(&mut set, changes);
        set
    }

    /// The oracle's full row set for `ast` at the CURRENT replica head: a fresh
    /// `PipelineDriver` (opens a snapshot at head), `add_query` (hydrates via
    /// the transient graph), all `Add`s applied to an empty set.
    fn oracle_set(
        path: &str,
        specs: &BTreeMap<String, SnapshotTableSpec>,
        tables: &BTreeSet<String>,
        ast: &Ast,
    ) -> RowSet {
        let mut driver =
            PipelineDriver::new(path, "zero", None, specs.clone(), tables.clone()).unwrap();
        let changes = driver.add_query("q", ast.clone()).unwrap();
        let set = from_changes(&changes);
        driver.destroy().unwrap();
        set
    }

    fn path() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir()
            .join(format!(
                "zero-graph-driver-{}-{}-{}.db",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                COUNTER.fetch_add(1, Ordering::Relaxed),
            ))
            .to_string_lossy()
            .into_owned()
    }

    fn log_set(writer: &StatementRunner, version: &str, table: &str, id: f64) {
        ChangeLog::new(writer)
            .log_set_op(
                version,
                0,
                table,
                &vec![("id".into(), JsonValue::Number(id))],
                None,
            )
            .unwrap();
    }

    fn log_del(writer: &StatementRunner, version: &str, table: &str, id: f64) {
        ChangeLog::new(writer)
            .log_delete_op(
                version,
                0,
                table,
                &vec![("id".into(), JsonValue::Number(id))],
            )
            .unwrap();
    }

    /// One commit: run `sql`, log the single changelog op, bump the watermark.
    fn commit(writer: &StatementRunner, version: &str, sql: &str, op: ChangeLogOp) {
        writer.run(sql, &[]).unwrap();
        match op {
            ChangeLogOp::Set(table, id) => log_set(writer, version, table, id),
            ChangeLogOp::Del(table, id) => log_del(writer, version, table, id),
        }
        update_replication_watermark(writer, version).unwrap();
    }

    enum ChangeLogOp<'a> {
        Set(&'a str, f64),
        Del(&'a str, f64),
    }

    /// Drives `asts` through BOTH the graph driver and the oracle over a commit
    /// `stream`: asserts hydration equality, then after EACH commit asserts the
    /// graph driver's running set equals the oracle's fresh re-derivation.
    fn run_oracle_gate(
        path: &str,
        writer: &StatementRunner,
        specs: BTreeMap<String, SnapshotTableSpec>,
        tables: BTreeSet<String>,
        asts: &[Ast],
        stream: &[(&str, &str, ChangeLogOp)],
    ) {
        let mut driver =
            GraphPipelineDriver::new(path, "zero", None, specs.clone(), tables.clone()).unwrap();
        let mut running: Vec<RowSet> = Vec::new();
        for (i, ast) in asts.iter().enumerate() {
            let initial = driver.add_query(format!("q{i}"), ast.clone()).unwrap();
            let set = from_changes(&initial);
            assert_eq!(
                set,
                oracle_set(path, &specs, &tables, ast),
                "hydration mismatch for q{i}: {ast:?}"
            );
            running.push(set);
        }

        for (version, sql, op) in stream {
            commit(writer, version, sql, matched(op));
            let changes = driver.advance().unwrap();
            for (i, ast) in asts.iter().enumerate() {
                let query_id = format!("q{i}");
                let owned: Vec<PipelineRowChange> = changes
                    .iter()
                    .filter(|c| c.query_id == query_id)
                    .cloned()
                    .collect();
                apply_changes(&mut running[i], &owned);
                assert_eq!(
                    running[i],
                    oracle_set(path, &specs, &tables, ast),
                    "post-commit @{version} mismatch for q{i} ({sql}): {ast:?}"
                );
            }
        }

        driver.destroy().unwrap();
    }

    /// Re-borrows a `ChangeLogOp` (the stream holds them by ref so the same
    /// list shape can be reused).
    fn matched<'a>(op: &ChangeLogOp<'a>) -> ChangeLogOp<'a> {
        match op {
            ChangeLogOp::Set(t, id) => ChangeLogOp::Set(t, *id),
            ChangeLogOp::Del(t, id) => ChangeLogOp::Del(t, *id),
        }
    }

    /// One commit that runs SEVERAL statements and logs SEVERAL changelog ops,
    /// all under a single version + one watermark bump. This is the multi-row
    /// commit shape increment 6b must advance correctly by push: the snapshotter
    /// diffs all the logged rows together, the driver derives one `SourceChange`
    /// per row, and `push_advance_query` accumulates them all before `set_db`.
    fn commit_many(writer: &StatementRunner, version: &str, steps: &[(&str, ChangeLogOp)]) {
        // The change-log PK is ("stateVersion","pos"), so each op in one commit
        // needs a DISTINCT pos — otherwise the second changelog row collides
        // with the first and the snapshotter only ever sees one change.
        for (pos, (sql, op)) in steps.iter().enumerate() {
            writer.run(sql, &[]).unwrap();
            let log = ChangeLog::new(writer);
            match op {
                ChangeLogOp::Set(table, id) => {
                    log.log_set_op(
                        version,
                        pos as i64,
                        table,
                        &vec![("id".into(), JsonValue::Number(*id))],
                        None,
                    )
                    .unwrap();
                }
                ChangeLogOp::Del(table, id) => {
                    log.log_delete_op(
                        version,
                        pos as i64,
                        table,
                        &vec![("id".into(), JsonValue::Number(*id))],
                    )
                    .unwrap();
                }
            }
        }
        update_replication_watermark(writer, version).unwrap();
    }

    /// Like [`run_oracle_gate`] but every commit can mutate MULTIPLE rows (a list
    /// of statement+op steps applied under one version). After each such
    /// multi-row commit the graph driver's running set MUST still equal the
    /// oracle's fresh re-derivation — the proof that the accumulating overlay
    /// advances multi-row-through-a-join commits by PUSH (these shapes are not
    /// `rebuild_only`, and the runaway floor is far above these push counts, so
    /// `advance` takes `push_advance_query`).
    fn run_oracle_gate_multi(
        path: &str,
        writer: &StatementRunner,
        specs: BTreeMap<String, SnapshotTableSpec>,
        tables: BTreeSet<String>,
        asts: &[Ast],
        stream: &[(&str, Vec<(&str, ChangeLogOp)>)],
    ) {
        let mut driver =
            GraphPipelineDriver::new(path, "zero", None, specs.clone(), tables.clone()).unwrap();
        let mut running: Vec<RowSet> = Vec::new();
        for (i, ast) in asts.iter().enumerate() {
            let initial = driver.add_query(format!("q{i}"), ast.clone()).unwrap();
            let set = from_changes(&initial);
            assert_eq!(
                set,
                oracle_set(path, &specs, &tables, ast),
                "hydration mismatch for q{i}: {ast:?}"
            );
            running.push(set);
        }

        for (version, steps) in stream {
            commit_many(writer, version, steps);
            let changes = driver.advance().unwrap();
            for (i, ast) in asts.iter().enumerate() {
                let query_id = format!("q{i}");
                let owned: Vec<PipelineRowChange> = changes
                    .iter()
                    .filter(|c| c.query_id == query_id)
                    .cloned()
                    .collect();
                apply_changes(&mut running[i], &owned);
                assert_eq!(
                    running[i],
                    oracle_set(path, &specs, &tables, ast),
                    "post-multi-commit @{version} mismatch for q{i}: {ast:?}"
                );
            }
        }

        driver.destroy().unwrap();
    }

    // ---- specs / query shapes ----

    fn issue_spec() -> (String, SnapshotTableSpec) {
        (
            "issue".into(),
            SnapshotTableSpec {
                name: "issue".into(),
                columns: vec!["id".into(), "active".into(), "_0_version".into()],
                column_types: BTreeMap::from([("active".into(), ColumnValueType::Boolean)]),
                primary_key: vec!["id".into()],
                unique_keys: vec![],
                min_row_version: Some("00".into()),
            },
        )
    }

    fn comment_spec() -> (String, SnapshotTableSpec) {
        (
            "comment".into(),
            SnapshotTableSpec {
                name: "comment".into(),
                columns: vec![
                    "id".into(),
                    "issueID".into(),
                    "body".into(),
                    "_0_version".into(),
                ],
                column_types: BTreeMap::new(),
                primary_key: vec!["id".into()],
                unique_keys: vec![],
                min_row_version: Some("00".into()),
            },
        )
    }

    fn active_eq_1() -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: "active".into(),
            }),
            right: ValuePosition::Literal(LiteralValue::Bool(true)),
        }
    }

    fn comments_rel() -> CorrelatedSubquery {
        CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: "comment".into(),
                alias: Some("comments".into()),
                ..Default::default()
            }),
            system: None,
            hidden: None,
        }
    }

    fn exists_cond(op: ExistsOp) -> Condition {
        Condition::CorrelatedSubquery {
            related: comments_rel(),
            op,
            flip: None,
            scalar: None,
            plan_id: None,
        }
    }

    // ---- Fixture 1: single-table filter, all-rows, version clamp ----

    #[test]
    fn oracle_gate_single_table_filter_all_rows_and_clamp() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run(
                "INSERT INTO issue VALUES (1,1,'00'),(2,0,'00'),(3,1,'00')",
                &[],
            )
            .unwrap();

        let specs = BTreeMap::from([issue_spec()]);
        let tables = BTreeSet::from(["issue".to_string()]);

        // (a) active=1 filter, (b) all rows, (c) all rows with a raised
        // min_row_version (clamp) — a separate specs clone so it doesn't
        // perturb the others.
        let filtered = Ast {
            table: "issue".into(),
            where_: Some(active_eq_1()),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let all_rows = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };

        let stream: Vec<(&str, &str, ChangeLogOp)> = vec![
            // root add: a new active row enters the filter and the all-rows set.
            (
                "01",
                "INSERT INTO issue VALUES (4,1,'01')",
                ChangeLogOp::Set("issue", 4.0),
            ),
            // root update leaving the filter (active 1 -> 0).
            (
                "02",
                "UPDATE issue SET active=0, _0_version='02' WHERE id=1",
                ChangeLogOp::Set("issue", 1.0),
            ),
            // root update entering the filter (active 0 -> 1).
            (
                "03",
                "UPDATE issue SET active=1, _0_version='03' WHERE id=2",
                ChangeLogOp::Set("issue", 2.0),
            ),
            // root delete.
            (
                "04",
                "DELETE FROM issue WHERE id=3",
                ChangeLogOp::Del("issue", 3.0),
            ),
            // root edit staying in the filter (version bump only).
            (
                "05",
                "UPDATE issue SET _0_version='05' WHERE id=4",
                ChangeLogOp::Set("issue", 4.0),
            ),
        ];

        run_oracle_gate(
            &path,
            &writer,
            specs,
            tables,
            &[filtered, all_rows],
            &stream,
        );

        // (c) version clamp: same replica, raised min_row_version. Seeded
        // versions ("00".."05") straddle a raised minimum "03", so hydration
        // AND every advance must clamp up identically on both paths.
        let mut clamped_specs = BTreeMap::from([issue_spec()]);
        clamped_specs.get_mut("issue").unwrap().min_row_version = Some("03".into());
        let clamp_ast = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        // A fresh graph driver over the (now-mutated) replica; assert hydration
        // parity under the clamp against the oracle.
        let set = {
            let mut d = GraphPipelineDriver::new(
                &path,
                "zero",
                None,
                clamped_specs.clone(),
                BTreeSet::from(["issue".to_string()]),
            )
            .unwrap();
            let ch = d.add_query("q", clamp_ast.clone()).unwrap();
            let s = from_changes(&ch);
            d.destroy().unwrap();
            s
        };
        assert_eq!(
            set,
            oracle_set(
                &path,
                &clamped_specs,
                &BTreeSet::from(["issue".to_string()]),
                &clamp_ast
            ),
            "version-clamp hydration parity"
        );

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    // ---- Fixture 2: related join, whereExists EXISTS / NOT EXISTS ----

    fn setup_issue_comment(path: &str) -> StatementRunner {
        let writer = StatementRunner::open_file(path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .exec(
                "CREATE TABLE comment (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT, _0_version TEXT)",
            )
            .unwrap();
        // issue1 active {c10}; issue2 inactive {}; issue3 active {c12}.
        writer
            .run(
                "INSERT INTO issue VALUES (1,1,'00'),(2,0,'00'),(3,1,'00')",
                &[],
            )
            .unwrap();
        writer
            .run(
                "INSERT INTO comment VALUES (10,1,'a','00'),(12,3,'c','00')",
                &[],
            )
            .unwrap();
        writer
    }

    #[test]
    fn oracle_gate_related_and_where_exists() {
        let path = path();
        let writer = setup_issue_comment(&path);
        let specs = BTreeMap::from([issue_spec(), comment_spec()]);
        let tables = BTreeSet::from(["issue".to_string(), "comment".to_string()]);

        let related = Ast {
            table: "issue".into(),
            related: Some(vec![comments_rel()]),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let exists = Ast {
            table: "issue".into(),
            where_: Some(exists_cond(ExistsOp::Exists)),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let not_exists = Ast {
            table: "issue".into(),
            where_: Some(exists_cond(ExistsOp::NotExists)),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let filtered_exists = Ast {
            table: "issue".into(),
            where_: Some(Condition::And {
                conditions: vec![active_eq_1(), exists_cond(ExistsOp::Exists)],
            }),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };

        let stream: Vec<(&str, &str, ChangeLogOp)> = vec![
            // child add flips issue2 INTO EXISTS (out of NOT EXISTS); related
            // gains a child; filtered_exists still excludes it (inactive).
            (
                "01",
                "INSERT INTO comment VALUES (13,2,'x','01')",
                ChangeLogOp::Set("comment", 13.0),
            ),
            // related-child non-key edit (body) — changes the related child row.
            (
                "02",
                "UPDATE comment SET body='y', _0_version='02' WHERE id=13",
                ChangeLogOp::Set("comment", 13.0),
            ),
            // child add to issue1 (already EXISTS: size 1 -> 2, no flip) —
            // related gains a second child; exists forwards the child.
            (
                "03",
                "INSERT INTO comment VALUES (14,1,'d','03')",
                ChangeLogOp::Set("comment", 14.0),
            ),
            // child delete on issue1 (size 2 -> 1, no flip) — related loses one.
            (
                "04",
                "DELETE FROM comment WHERE id=10",
                ChangeLogOp::Del("comment", 10.0),
            ),
            // child delete flips issue2 OUT of EXISTS (its only child 13 gone).
            (
                "05",
                "DELETE FROM comment WHERE id=13",
                ChangeLogOp::Del("comment", 13.0),
            ),
            // parent add: a new issue with no comments — in NOT EXISTS, related
            // with empty children, out of EXISTS.
            (
                "06",
                "INSERT INTO issue VALUES (4,1,'06')",
                ChangeLogOp::Set("issue", 4.0),
            ),
            // parent non-key edit (active) — for the exists/related shapes it is
            // a plain row edit; for filtered_exists issue3 is active + has c12.
            (
                "07",
                "UPDATE issue SET active=0, _0_version='07' WHERE id=3",
                ChangeLogOp::Set("issue", 3.0),
            ),
            // parent delete.
            (
                "08",
                "DELETE FROM issue WHERE id=4",
                ChangeLogOp::Del("issue", 4.0),
            ),
        ];

        run_oracle_gate(
            &path,
            &writer,
            specs,
            tables,
            &[related, exists, not_exists, filtered_exists],
            &stream,
        );

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    // ---- Fixture 3: OR-of-exists (FanOut -> branches -> FanIn) ----

    #[test]
    fn oracle_gate_or_of_exists() {
        let path = path();
        let writer = setup_issue_comment(&path);
        let specs = BTreeMap::from([issue_spec(), comment_spec()]);
        let tables = BTreeSet::from(["issue".to_string(), "comment".to_string()]);

        // active = true OR EXISTS(comments).
        let or_exists = Ast {
            table: "issue".into(),
            where_: Some(Condition::Or {
                conditions: vec![active_eq_1(), exists_cond(ExistsOp::Exists)],
            }),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };

        let stream: Vec<(&str, &str, ChangeLogOp)> = vec![
            // child add: issue2 (inactive) now qualifies via the EXISTS arm.
            (
                "01",
                "INSERT INTO comment VALUES (13,2,'x','01')",
                ChangeLogOp::Set("comment", 13.0),
            ),
            // child delete: issue2 drops out again (inactive, no comments).
            (
                "02",
                "DELETE FROM comment WHERE id=13",
                ChangeLogOp::Del("comment", 13.0),
            ),
            // parent edit: issue1 active -> 0 but still has c10 (EXISTS arm keeps it).
            (
                "03",
                "UPDATE issue SET active=0, _0_version='03' WHERE id=1",
                ChangeLogOp::Set("issue", 1.0),
            ),
            // now delete issue1's only comment: neither arm holds -> drops out.
            (
                "04",
                "DELETE FROM comment WHERE id=10",
                ChangeLogOp::Del("comment", 10.0),
            ),
            // parent add: a new active issue qualifies via the active arm.
            (
                "05",
                "INSERT INTO issue VALUES (5,1,'05')",
                ChangeLogOp::Set("issue", 5.0),
            ),
        ];

        run_oracle_gate(&path, &writer, specs, tables, &[or_exists], &stream);

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    // ---- Fixture 4: bounded + ordered take (insert evicts the bound) ----

    #[test]
    fn oracle_gate_bounded_ordered_take() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run(
                "INSERT INTO issue VALUES (1,1,'00'),(2,1,'00'),(3,1,'00'),(4,1,'00'),(5,1,'00')",
                &[],
            )
            .unwrap();

        let specs = BTreeMap::from([issue_spec()]);
        let tables = BTreeSet::from(["issue".to_string()]);

        // limit 2, ordered DESC by id -> window {5,4}. Only inserts/deletes and
        // non-sort-column edits (the read-only source does not split a
        // sort-key-changing edit — see the module doc), so `id` (the sort key)
        // is never edited.
        let bounded_desc = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Desc)]),
            limit: Some(2.0),
            ..Default::default()
        };
        // start (exclusive id=5) + limit 2 ASC -> window {6,7} after inserts.
        let bounded_start = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            start: Some(Bound {
                row: JsonValue::Object(vec![("id".into(), JsonValue::Number(2.0))]),
                exclusive: true,
            }),
            limit: Some(2.0),
            ..Default::default()
        };

        let stream: Vec<(&str, &str, ChangeLogOp)> = vec![
            // insert id=6: sorts to the top of the DESC window -> evicts id=4.
            (
                "01",
                "INSERT INTO issue VALUES (6,1,'01')",
                ChangeLogOp::Set("issue", 6.0),
            ),
            // insert id=7: evicts again.
            (
                "02",
                "INSERT INTO issue VALUES (7,1,'02')",
                ChangeLogOp::Set("issue", 7.0),
            ),
            // delete id=7 (top of window): the window refills from below.
            (
                "03",
                "DELETE FROM issue WHERE id=7",
                ChangeLogOp::Del("issue", 7.0),
            ),
            // non-sort-column edit on an in-window row.
            (
                "04",
                "UPDATE issue SET active=0, _0_version='04' WHERE id=6",
                ChangeLogOp::Set("issue", 6.0),
            ),
            // delete a row below the window (no effect on the DESC top-2).
            (
                "05",
                "DELETE FROM issue WHERE id=1",
                ChangeLogOp::Del("issue", 1.0),
            ),
        ];

        run_oracle_gate(
            &path,
            &writer,
            specs,
            tables,
            &[bounded_desc, bounded_start],
            &stream,
        );

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    // ---- Fixture 5: partitioned take (related child with a per-parent limit) ----

    #[test]
    fn oracle_gate_partitioned_take() {
        let path = path();
        let writer = setup_issue_comment(&path);
        // Give issue1 a second comment so a per-parent limit actually bounds it.
        writer
            .run("INSERT INTO comment VALUES (11,1,'b','00')", &[])
            .unwrap();
        let specs = BTreeMap::from([issue_spec(), comment_spec()]);
        let tables = BTreeSet::from(["issue".to_string(), "comment".to_string()]);

        // issue related (comments LIMIT 1 per issue, ordered by comment id).
        let partitioned = Ast {
            table: "issue".into(),
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast {
                    table: "comment".into(),
                    alias: Some("comments".into()),
                    order_by: Some(vec![("id".into(), Direction::Asc)]),
                    limit: Some(1.0),
                    ..Default::default()
                }),
                system: None,
                hidden: None,
            }]),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };

        let stream: Vec<(&str, &str, ChangeLogOp)> = vec![
            // add a comment to issue2 (had none) -> its single related child.
            (
                "01",
                "INSERT INTO comment VALUES (20,2,'z','01')",
                ChangeLogOp::Set("comment", 20.0),
            ),
            // delete issue3's only comment -> issue3's related child goes empty.
            (
                "02",
                "DELETE FROM comment WHERE id=12",
                ChangeLogOp::Del("comment", 12.0),
            ),
        ];

        run_oracle_gate(&path, &writer, specs, tables, &[partitioned], &stream);

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    // ---- Fixture 6: MULTI-ROW commits through related / EXISTS / NOT EXISTS ----
    //
    // The increment-6b proof: a SINGLE commit that mutates several rows feeding
    // the SAME complex query must advance correctly on the PUSH path (not
    // rebuild). Each commit below logs >1 changelog op under one version.

    /// A replica with issue1 holding TWO comments (so a commit can delete both
    /// of one parent's children at once).
    fn setup_issue_comment_two_children(path: &str) -> StatementRunner {
        let writer = StatementRunner::open_file(path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .exec(
                "CREATE TABLE comment (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT, _0_version TEXT)",
            )
            .unwrap();
        // issue1 active {c10,c11}; issue2 inactive {}; issue3 active {c12}.
        writer
            .run(
                "INSERT INTO issue VALUES (1,1,'00'),(2,0,'00'),(3,1,'00')",
                &[],
            )
            .unwrap();
        writer
            .run(
                "INSERT INTO comment VALUES (10,1,'a','00'),(11,1,'b','00'),(12,3,'c','00')",
                &[],
            )
            .unwrap();
        writer
    }

    #[test]
    fn oracle_gate_multi_change_related_and_exists() {
        let path = path();
        let writer = setup_issue_comment_two_children(&path);
        let specs = BTreeMap::from([issue_spec(), comment_spec()]);
        let tables = BTreeSet::from(["issue".to_string(), "comment".to_string()]);

        let related = Ast {
            table: "issue".into(),
            related: Some(vec![comments_rel()]),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let exists = Ast {
            table: "issue".into(),
            where_: Some(exists_cond(ExistsOp::Exists)),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let not_exists = Ast {
            table: "issue".into(),
            where_: Some(exists_cond(ExistsOp::NotExists)),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };

        let stream: Vec<(&str, Vec<(&str, ChangeLogOp)>)> = vec![
            // (1) DELETE BOTH of issue1's children in one commit. related loses
            // both; EXISTS flips issue1 OUT; NOT EXISTS flips it IN. The second
            // push's join re-fetch of issue1's children must see {} (both removes
            // accumulated), not {c11}.
            (
                "01",
                vec![
                    (
                        "DELETE FROM comment WHERE id=10",
                        ChangeLogOp::Del("comment", 10.0),
                    ),
                    (
                        "DELETE FROM comment WHERE id=11",
                        ChangeLogOp::Del("comment", 11.0),
                    ),
                ],
            ),
            // (2) DELETE issue3's only child AND ADD a replacement for issue3 in
            // one commit (the canonical "delete c1, add c2 of the same parent"
            // case). EXISTS/NOT EXISTS unchanged for issue3; related swaps c12->c14.
            (
                "02",
                vec![
                    (
                        "DELETE FROM comment WHERE id=12",
                        ChangeLogOp::Del("comment", 12.0),
                    ),
                    (
                        "INSERT INTO comment VALUES (14,3,'d','02')",
                        ChangeLogOp::Set("comment", 14.0),
                    ),
                ],
            ),
            // (3) ADD TWO comments to issue2 (had none) in one commit — several
            // rows feeding one whereExists flip issue2 INTO EXISTS / OUT of NOT
            // EXISTS.
            (
                "03",
                vec![
                    (
                        "INSERT INTO comment VALUES (20,2,'e','03')",
                        ChangeLogOp::Set("comment", 20.0),
                    ),
                    (
                        "INSERT INTO comment VALUES (21,2,'f','03')",
                        ChangeLogOp::Set("comment", 21.0),
                    ),
                ],
            ),
            // (4) Multi-row PARENT commit: add issue4 (no comments) AND delete
            // issue3 in one commit. related gains issue4 (empty) and drops issue3
            // + its child; EXISTS drops issue3; NOT EXISTS gains issue4.
            (
                "04",
                vec![
                    (
                        "INSERT INTO issue VALUES (4,1,'04')",
                        ChangeLogOp::Set("issue", 4.0),
                    ),
                    (
                        "DELETE FROM issue WHERE id=3",
                        ChangeLogOp::Del("issue", 3.0),
                    ),
                ],
            ),
        ];

        run_oracle_gate_multi(
            &path,
            &writer,
            specs,
            tables,
            &[related, exists, not_exists],
            &stream,
        );

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn oracle_gate_multi_change_bounded_take() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run(
                "INSERT INTO issue VALUES (1,1,'00'),(2,1,'00'),(3,1,'00'),(4,1,'00'),(5,1,'00')",
                &[],
            )
            .unwrap();

        let specs = BTreeMap::from([issue_spec()]);
        let tables = BTreeSet::from(["issue".to_string()]);

        // limit 2, DESC by id -> window {5,4}.
        let bounded_desc = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Desc)]),
            limit: Some(2.0),
            ..Default::default()
        };

        let stream: Vec<(&str, Vec<(&str, ChangeLogOp)>)> = vec![
            // (1) INSERT id=6 AND id=7 in one commit: both sort above the DESC
            // window -> two evictions in one commit, window {5,4} -> {7,6}.
            (
                "01",
                vec![
                    (
                        "INSERT INTO issue VALUES (6,1,'01')",
                        ChangeLogOp::Set("issue", 6.0),
                    ),
                    (
                        "INSERT INTO issue VALUES (7,1,'01')",
                        ChangeLogOp::Set("issue", 7.0),
                    ),
                ],
            ),
            // (2) DELETE an in-window row (id=6) AND its refill candidate (id=4,
            // below the window) in one commit. Evicting 6 must refill from the
            // accumulated-consistent source that ALSO reflects the delete of 4,
            // so the window skips 4. window {7,6} -> {7,5}.
            (
                "02",
                vec![
                    (
                        "DELETE FROM issue WHERE id=6",
                        ChangeLogOp::Del("issue", 6.0),
                    ),
                    (
                        "DELETE FROM issue WHERE id=4",
                        ChangeLogOp::Del("issue", 4.0),
                    ),
                ],
            ),
            // (3) DELETE both remaining window rows (id=7, id=5) in one commit;
            // the window refills twice from below -> {3,2}.
            (
                "03",
                vec![
                    (
                        "DELETE FROM issue WHERE id=7",
                        ChangeLogOp::Del("issue", 7.0),
                    ),
                    (
                        "DELETE FROM issue WHERE id=5",
                        ChangeLogOp::Del("issue", 5.0),
                    ),
                ],
            ),
        ];

        run_oracle_gate_multi(&path, &writer, specs, tables, &[bounded_desc], &stream);

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// OR-of-correlated (`active OR EXISTS(comments)`) stays `rebuild_only` (the
    /// FanOut/FanIn limitation), but must STILL match the oracle across multi-row
    /// commits — here via the re-derivation path.
    #[test]
    fn oracle_gate_multi_change_or_of_exists_rebuilds() {
        let path = path();
        let writer = setup_issue_comment_two_children(&path);
        let specs = BTreeMap::from([issue_spec(), comment_spec()]);
        let tables = BTreeSet::from(["issue".to_string(), "comment".to_string()]);

        let or_exists = Ast {
            table: "issue".into(),
            where_: Some(Condition::Or {
                conditions: vec![active_eq_1(), exists_cond(ExistsOp::Exists)],
            }),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        assert!(
            ast_needs_rebuild_advance(&or_exists),
            "OR-of-correlated must be rebuild_only"
        );

        let stream: Vec<(&str, Vec<(&str, ChangeLogOp)>)> = vec![
            // Delete both of issue1's comments AND flip issue1 inactive in one
            // commit: issue1 held only via the active arm + the EXISTS arm; both
            // gone -> issue1 drops out.
            (
                "01",
                vec![
                    (
                        "DELETE FROM comment WHERE id=10",
                        ChangeLogOp::Del("comment", 10.0),
                    ),
                    (
                        "DELETE FROM comment WHERE id=11",
                        ChangeLogOp::Del("comment", 11.0),
                    ),
                    (
                        "UPDATE issue SET active=0, _0_version='01' WHERE id=1",
                        ChangeLogOp::Set("issue", 1.0),
                    ),
                ],
            ),
            // Add a comment to inactive issue2 AND add a new active issue5 in one
            // commit: issue2 qualifies via EXISTS, issue5 via active.
            (
                "02",
                vec![
                    (
                        "INSERT INTO comment VALUES (22,2,'g','02')",
                        ChangeLogOp::Set("comment", 22.0),
                    ),
                    (
                        "INSERT INTO issue VALUES (5,1,'02')",
                        ChangeLogOp::Set("issue", 5.0),
                    ),
                ],
            ),
        ];

        run_oracle_gate_multi(&path, &writer, specs, tables, &[or_exists], &stream);

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// The push-vs-rebuild classification: related / plain EXISTS / AND-filtered
    /// EXISTS all push-advance; only OR-of-correlated is `rebuild_only`.
    #[test]
    fn rebuild_classification_only_flags_or_of_correlated() {
        let related = Ast {
            table: "issue".into(),
            related: Some(vec![comments_rel()]),
            ..Default::default()
        };
        let exists = Ast {
            table: "issue".into(),
            where_: Some(exists_cond(ExistsOp::Exists)),
            ..Default::default()
        };
        let filtered = Ast {
            table: "issue".into(),
            where_: Some(Condition::And {
                conditions: vec![active_eq_1(), exists_cond(ExistsOp::Exists)],
            }),
            ..Default::default()
        };
        assert!(!ast_needs_rebuild_advance(&related), "related pushes");
        assert!(!ast_needs_rebuild_advance(&exists), "EXISTS pushes");
        assert!(
            !ast_needs_rebuild_advance(&filtered),
            "AND-filtered EXISTS pushes"
        );

        let or_exists = Ast {
            table: "issue".into(),
            where_: Some(Condition::Or {
                conditions: vec![active_eq_1(), exists_cond(ExistsOp::Exists)],
            }),
            ..Default::default()
        };
        assert!(
            ast_needs_rebuild_advance(&or_exists),
            "OR-of-correlated rebuilds"
        );
    }

    // ---- Public surface: remove_query / current_query_rows / signatures ----

    #[test]
    fn remove_query_emits_removes_and_clears_signature() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (1,1,'00'),(2,1,'00')", &[])
            .unwrap();

        let mut driver = GraphPipelineDriver::new(
            &path,
            "zero",
            None,
            BTreeMap::from([issue_spec()]),
            BTreeSet::from(["issue".to_string()]),
        )
        .unwrap();
        let ast = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let initial = driver.add_query("q", ast).unwrap();
        assert_eq!(initial.len(), 2);
        assert_ne!(driver.row_set_signature("q").unwrap(), 0);
        // current_query_rows echoes the active set as Adds.
        assert_eq!(driver.current_query_rows("q").len(), 2);

        let removed = driver.remove_query("q");
        assert_eq!(removed.len(), 2);
        assert!(removed
            .iter()
            .all(|c| c.kind == PipelineRowChangeKind::Remove));
        assert_eq!(driver.row_set_signature("q"), None);
        assert!(driver.current_query_rows("q").is_empty());

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// A duplicate `add_query` is rejected, and `version` tracks the head.
    #[test]
    fn duplicate_query_rejected_and_version_tracks_head() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (1,1,'00')", &[])
            .unwrap();

        let mut driver = GraphPipelineDriver::new(
            &path,
            "zero",
            None,
            BTreeMap::from([issue_spec()]),
            BTreeSet::from(["issue".to_string()]),
        )
        .unwrap();
        let ast = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        driver.add_query("q", ast.clone()).unwrap();
        assert!(matches!(
            driver.add_query("q", ast),
            Err(PipelineError::DuplicateQuery(_))
        ));
        assert_eq!(driver.version().unwrap(), "00");

        commit(
            &writer,
            "01",
            "UPDATE issue SET _0_version='01' WHERE id=1",
            ChangeLogOp::Set("issue", 1.0),
        );
        driver.advance().unwrap();
        assert_eq!(driver.version().unwrap(), "01");

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }
}
