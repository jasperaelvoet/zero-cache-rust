//! Persistent client-group query pipelines driven by Zero snapshot diffs.
//!
//! In-memory `Vec<Row>` sources are hydrated LAZILY — only for tables a
//! COMPLEX (non-direct) query must re-materialize. Direct-incremental queries
//! hydrate through a transient replica-backed operator graph and advance from
//! the snapshot diff, never loading their table into memory.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use zero_cache_protocol::ast::{
    referenced_tables, Ast, Bound, Condition, CorrelatedSubquery, Direction, ExistsOp, Ordering,
};
use zero_cache_shared::bigint_json::{stringify, JsonValue};
use zero_cache_sqlite::snapshotter::{
    SnapshotChange, SnapshotError, SnapshotTableSpec, Snapshotter,
};
use zero_cache_sqlite::{SqliteSource, StatementRunner, Value as SqlValue};
use zero_cache_zql::builder::filter::{create_predicate_with_exists, ExistsFn};
use zero_cache_zql::builder::pipeline::{build_pipeline, BuildDelegate};
use zero_cache_zql::ivm::change::{make_source_change_add, make_source_change_remove};
use zero_cache_zql::ivm::data::{make_comparator, Row};
use zero_cache_zql::ivm::operator::{FetchRequest, Input, Node};
use zero_cache_zql::ivm::table_source::TableSource;

use crate::row_set_signature::row_id_signature_unit;

use zero_cache_types::pg_data_type::ValueType as ColumnValueType;
use zero_cache_types::pg_to_lite::ZERO_VERSION_COLUMN_NAME;

/// Environment escape hatch. The replica-backed graph hydration path
/// (redesign §7 increment 4) is now the DEFAULT for direct-incremental
/// queries. Setting `ZERO_IVM_GRAPH=0` (or `off`) forces the legacy
/// `materialize_query` path everywhere — kept only as a fallback.
const IVM_GRAPH_ENV: &str = "ZERO_IVM_GRAPH";

/// Per-column declared ZQL value types for one table, as carried on
/// [`SnapshotTableSpec::column_types`].
type ColumnTypes = BTreeMap<String, ColumnValueType>;

/// Empty type map used where a table's declared types are unavailable; every
/// column then falls back to the generic (string/number) conversion.
fn empty_column_types() -> ColumnTypes {
    BTreeMap::new()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineRowChangeKind {
    Add,
    Remove,
    Edit,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PipelineRowChange {
    pub query_id: String,
    pub table: String,
    pub kind: PipelineRowChangeKind,
    pub row: Row,
    pub old_row: Option<Row>,
    pub row_key: BTreeMap<String, JsonValue>,
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error(transparent)]
    Db(#[from] zero_cache_sqlite::DbError),
    #[error("unknown table {0}")]
    UnknownTable(String),
    #[error("query {0} is already active")]
    DuplicateQuery(String),
    #[error(transparent)]
    RowKey(#[from] zero_cache_types::row_key::RowKeyError),
}

#[derive(Clone)]
struct MaterializedRow {
    table: String,
    row: Row,
    row_key: BTreeMap<String, JsonValue>,
}

/// One active query's durable pipeline state.
///
/// NOTE (redesign §5.1, increment 4): the operator graph (`Rc<dyn Input>` +
/// `SqliteSource`) is deliberately NOT stored here. It is `!Send`, and
/// `PipelineDriver` must stay `Send` for the current per-connection server
/// ownership — the dedicated-thread, graph-owning per-group driver is redesign
/// increment 9. So the graph is built transiently in
/// [`PipelineDriver::hydrate_via_graph`], drained by `fetch` for its
/// hydration rows, and dropped. `advance` then runs on
/// `apply_direct_changes` (direct queries, from the snapshot diff) or
/// `materialize_query` (complex queries) using these durable `rows`.
struct Pipeline {
    ast: Ast,
    rows: BTreeMap<String, MaterializedRow>,
    referenced_tables: BTreeSet<String>,
}

/// Persistent pipeline owner for one Zero client group.
pub struct PipelineDriver {
    snapshotter: Snapshotter,
    db_file: String,
    page_cache_size_kib: Option<usize>,
    table_specs: BTreeMap<String, SnapshotTableSpec>,
    all_table_names: BTreeSet<String>,
    sources: HashMap<String, TableSource>,
    pipelines: BTreeMap<String, Pipeline>,
    row_set_signatures: BTreeMap<String, u64>,
    /// When true (the default), direct-incremental queries hydrate via
    /// `build_pipeline` + fetch (redesign §5.1) and never load their table into
    /// the in-memory `sources`. Set `ZERO_IVM_GRAPH=0` to force the legacy
    /// `materialize_query` path everywhere.
    graph_enabled: bool,
}

impl PipelineDriver {
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
        let graph_enabled = std::env::var(IVM_GRAPH_ENV)
            .map(|value| {
                let value = value.to_ascii_lowercase();
                value != "0" && value != "off" && value != "false"
            })
            .unwrap_or(true);
        let driver = Self {
            snapshotter,
            db_file,
            page_cache_size_kib,
            table_specs,
            all_table_names,
            sources: HashMap::new(),
            pipelines: BTreeMap::new(),
            row_set_signatures: BTreeMap::new(),
            graph_enabled,
        };
        Ok(driver)
    }

    /// Loads a table's full in-memory `Vec<Row>` [`TableSource`] on demand,
    /// caching it in `self.sources`. A no-op if already loaded. This is ONLY
    /// called for tables referenced by a COMPLEX (non-direct) query, which
    /// re-materializes via [`materialize_query`] and so needs the in-memory
    /// scan. Direct-incremental queries never trigger this (they hydrate via
    /// the graph and advance from the snapshot diff).
    fn ensure_source(&mut self, table: &str) -> Result<(), PipelineError> {
        if self.sources.contains_key(table) {
            return Ok(());
        }
        let spec = self
            .table_specs
            .get(table)
            .ok_or_else(|| PipelineError::UnknownTable(table.to_string()))?;
        let ordering = spec
            .primary_key
            .iter()
            .map(|column| (column.clone(), Direction::Asc))
            .collect();
        let mut source = TableSource::new(&spec.name, spec.primary_key.clone(), ordering);
        let columns = spec
            .columns
            .iter()
            .map(|column| quote(column))
            .collect::<Vec<_>>();
        let db = self.snapshotter.current()?.db();
        for row in db.all(
            &format!("SELECT {} FROM {}", columns.join(","), quote(&spec.name)),
            &[],
        )? {
            source.push(make_source_change_add(sql_row_to_zql(
                row,
                &spec.column_types,
                spec.min_row_version.as_deref(),
            )));
        }
        self.sources.insert(spec.name.clone(), source);
        Ok(())
    }

    /// Builds a fresh, replica-backed [`SqliteSource`] for `table`, opening a
    /// new `BEGIN CONCURRENT` snapshot handle over the same replica file the
    /// driver reads. Not memoized on `self` — see [`Pipeline`]'s note for why
    /// the graph must stay transient (`!Send`) this increment.
    ///
    /// The source is ordered by `order_by ++ primary_key` (falling back to just
    /// the PK when `order_by` is `None`), computed by [`source_ordering`] —
    /// byte-identical to [`completed_ordering`], the total order
    /// [`materialize_query`] sorts by. This makes the graph's `Skip`/`Take`
    /// select the SAME bounded subset as `materialize_query` for a
    /// bounded+ordered query. Only the ROOT table's source carries a query
    /// `order_by`; child/related sources are always built with `None` (PK-only)
    /// — see [`hydrate_via_graph`].
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

    /// Hydrates `ast` through a freshly built, replica-backed operator graph
    /// (redesign §5.1): `build_pipeline` over transiently built `SqliteSource`s
    /// (one per referenced table), drained by `root.fetch(..)`. The whole graph
    /// is dropped when this returns (see [`Pipeline`]'s note); only the
    /// materialized rows escape.
    ///
    /// Output is keyed by `materialized_key` — byte-identical in shape to
    /// [`materialize_query`] — so `add_query` derives both the returned
    /// hydration changes and the durable `pipeline.rows` (that `advance`
    /// re-derives) from the same map. Each fetched row's `_0_version` is clamped
    /// up to its table's `spec.min_row_version`, matching [`sql_row_to_zql`]'s
    /// clamp on the `materialize_query` path.
    ///
    /// For COMPLEX queries (`related` joins / `whereExists`) the fetched root
    /// nodes carry their joined children under `Node.relationships[alias]`; this
    /// walks those relationships and inserts the child rows too — mirroring
    /// [`materialize_query`]'s [`materialize_related`] recursion (which inserts
    /// `related` children and `EXISTS` — but not `NOT EXISTS` — subquery
    /// children). The graph naturally produces the same set: an `EXISTS`
    /// parent's relationship holds all matching children, while a surviving `NOT
    /// EXISTS` parent's relationship is empty.
    fn hydrate_via_graph(
        &self,
        ast: &Ast,
    ) -> Result<BTreeMap<String, MaterializedRow>, PipelineError> {
        // Build a source for EVERY (table, ordering) pair `build_pipeline`
        // will request up front (the root plus, recursively, every
        // `related`/`whereExists` subquery) so the delegate closure — which
        // can't itself surface an open error — only looks them up. Each
        // subquery's source carries that subquery's OWN `order_by` (the
        // analogue of upstream `getSource(table).connect(sort)`), so a child
        // `Skip`/`Take` selects the same subset the query semantics demand —
        // including a child subquery pairing `order_by` with a bound.
        let mut tables: HashMap<String, Rc<SqliteSource>> = HashMap::new();
        for (table, order_by) in referenced_sources(ast) {
            let key = source_key(&table, order_by.as_ref());
            if let std::collections::hash_map::Entry::Vacant(entry) = tables.entry(key) {
                entry.insert(self.build_graph_source(&table, order_by.as_ref())?);
            }
        }
        let get_source = |table: &str, order_by: Option<&Ordering>| -> Rc<dyn Input> {
            tables
                .get(&source_key(table, order_by))
                .cloned()
                .unwrap_or_else(|| panic!("graph source for `{table}` not pre-built"))
                as Rc<dyn Input>
        };
        let create_storage = |_name: &str| -> Rc<dyn zero_cache_zql::ivm::operator::Storage> {
            Rc::new(zero_cache_zql::ivm::memory_storage::MemoryStorage::default())
        };
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let root = build_pipeline(ast, &delegate);

        let roots: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        let mut output = BTreeMap::new();
        self.insert_graph_nodes(ast, &roots, &mut output)?;
        Ok(output)
    }

    /// Inserts every row a hydrated graph produced for `ast` into `output`,
    /// keyed by `materialized_key`: the `ast.table` root rows plus, recursively,
    /// the child rows each node carries under `Node.relationships[alias]` for
    /// this query's `related` hops and `EXISTS` `whereExists` conditions. The
    /// set of hops walked (and their alias names) mirrors exactly what
    /// `build_pipeline` wired as `JoinInput`s, so the traversal reads back every
    /// relationship the graph populated — and only those. Each row's
    /// `_0_version` is clamped up to its own table's `min_row_version`.
    fn insert_graph_nodes(
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
            self.insert_graph_nodes(subquery, &children, output)?;
        }
        Ok(())
    }

    pub fn version(&self) -> Result<&str, PipelineError> {
        Ok(&self.snapshotter.current()?.version)
    }

    pub fn add_query(
        &mut self,
        query_id: impl Into<String>,
        ast: Ast,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let query_id = query_id.into();
        if self.pipelines.contains_key(&query_id) {
            return Err(PipelineError::DuplicateQuery(query_id));
        }
        // Graph-eligible queries (direct-incremental AND complex `related`/
        // `whereExists` — see [`is_graph_eligible`]) hydrate through the
        // transient replica-backed graph and NEVER load their tables into the
        // in-memory `sources`: `advance` re-derives them the same way (direct
        // ones from the snapshot diff via `apply_direct_changes`, complex ones
        // by re-fetching the graph). Only NON-eligible shapes (an OR of a
        // correlated subquery) fall back to `materialize_query`, which must load
        // (`ensure_source`) each referenced table.
        let rows = if self.graph_enabled && is_graph_eligible(&ast) {
            self.hydrate_via_graph(&ast)?
        } else {
            for table in referenced_tables(&ast) {
                self.ensure_source(&table)?;
            }
            materialize_query(&ast, &self.sources, &self.table_specs)?
        };
        self.row_set_signatures
            .insert(query_id.clone(), signature_for_rows(rows.values())?);
        let changes = additions(&query_id, &rows);

        self.pipelines.insert(
            query_id,
            Pipeline {
                referenced_tables: referenced_tables(&ast),
                ast,
                rows,
            },
        );
        Ok(changes)
    }

    /// Whether `ast` can be registered with PRE-COMPUTED rows through
    /// [`register_query`] (reusing the caller's single root-table hydration
    /// fetch) instead of re-fetching. This is a STRICT SUBSET of
    /// [`is_graph_eligible`]: only DIRECT-incremental queries qualify, because
    /// the prehydration fast path supplies just the root table's rows — a
    /// complex `related`/`whereExists` query also needs its joined child rows,
    /// which only the full graph hydration (`add_query` → `hydrate_via_graph`)
    /// produces. So complex graph-eligible queries deliberately report `false`
    /// here and hydrate through `add_query`.
    pub fn uses_prehydrated_rows(&self, ast: &Ast) -> bool {
        self.graph_enabled && is_direct_incremental_query(ast)
    }

    /// Registers a direct-incremental query with rows already fetched by the
    /// caller (the `live_hydration` fetch that also builds the poke), so the
    /// pipeline does NOT re-fetch or open a second snapshot connection. The
    /// supplied `rows` are the typed ZQL rows the replica read produced; this
    /// applies the SAME `_0_version` clamp and byte-identical `materialized_key`
    /// keying as [`hydrate_via_graph`], so subsequent [`advance`] /
    /// [`apply_direct_changes`] diffs are indistinguishable from the graph path.
    ///
    /// Only valid for [`uses_prehydrated_rows`] ASTs; complex queries and the
    /// `ZERO_IVM_GRAPH=0` legacy path must use [`add_query`].
    pub fn register_query(
        &mut self,
        query_id: impl Into<String>,
        ast: Ast,
        rows: Vec<Row>,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let query_id = query_id.into();
        if self.pipelines.contains_key(&query_id) {
            return Err(PipelineError::DuplicateQuery(query_id));
        }
        let min_row_version = self
            .table_specs
            .get(&ast.table)
            .and_then(|spec| spec.min_row_version.clone());
        let mut materialized = BTreeMap::new();
        for row in rows {
            let row = clamp_row_version(row, min_row_version.as_deref());
            insert_row(&ast.table, row, &self.table_specs, &mut materialized)?;
        }
        self.row_set_signatures
            .insert(query_id.clone(), signature_for_rows(materialized.values())?);
        let changes = additions(&query_id, &materialized);
        self.pipelines.insert(
            query_id,
            Pipeline {
                referenced_tables: referenced_tables(&ast),
                ast,
                rows: materialized,
            },
        );
        Ok(changes)
    }

    pub fn remove_query(&mut self, query_id: &str) -> Vec<PipelineRowChange> {
        self.row_set_signatures.remove(query_id);
        self.pipelines
            .remove(query_id)
            .map(|pipeline| {
                pipeline
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

    pub fn advance(&mut self) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let diff = self
            .snapshotter
            .advance(&self.table_specs, &self.all_table_names)?;
        let changed_tables: BTreeSet<_> = diff.rows.iter().map(|row| row.table.clone()).collect();
        // Only tables with an in-memory source loaded (i.e. referenced by a
        // COMPLEX query that re-materializes) need their `Vec<Row>` advanced.
        // Direct-only tables have no source and are skipped — their pipelines
        // advance from the snapshot diff via `apply_direct_changes`.
        for change in &diff.rows {
            if self.sources.contains_key(&change.table) {
                apply_snapshot_change(&mut self.sources, change, &self.table_specs)?;
            }
        }

        let ids: Vec<_> = self
            .pipelines
            .iter()
            .filter(|(_, pipeline)| !pipeline.referenced_tables.is_disjoint(&changed_tables))
            .map(|(id, _)| id.clone())
            .collect();
        let mut changes = Vec::new();
        for id in ids {
            // A direct-incremental query advances from the snapshot diff without
            // any full re-read.
            if is_direct_incremental_query(&self.pipelines[&id].ast) {
                let pipeline = self
                    .pipelines
                    .get_mut(&id)
                    .expect("selected pipeline exists");
                changes.extend(apply_direct_changes(
                    &id,
                    pipeline,
                    &diff.rows,
                    &self.table_specs,
                )?);
                continue;
            }

            // A COMPLEX query re-derives its full result. When the graph handled
            // its hydration (`is_graph_eligible`), it must re-derive via the SAME
            // graph — `materialize_query`'s in-memory sources were never loaded
            // for it. The `ast` is cloned so the shared `&self` graph fetch does
            // not overlap the `&mut` pipeline borrow (the graph is `!Send`, so it
            // cannot be stored on the pipeline — see [`Pipeline`]'s note).
            let ast = self.pipelines[&id].ast.clone();
            let next = if self.graph_enabled && is_graph_eligible(&ast) {
                self.hydrate_via_graph(&ast)?
            } else {
                materialize_query(&ast, &self.sources, &self.table_specs)?
            };
            let pipeline = self
                .pipelines
                .get_mut(&id)
                .expect("selected pipeline exists");
            changes.extend(diff_rows(&id, &pipeline.rows, &next));
            pipeline.rows = next;
        }
        self.apply_signature_changes(&changes)?;
        Ok(changes)
    }

    pub fn row_set_signature(&self, query_id: &str) -> Option<u64> {
        self.row_set_signatures.get(query_id).copied()
    }

    /// The current result rows of an ALREADY-active query, as `Add` changes —
    /// the same shape [`add_query`] returns on first hydration. Used when a
    /// second connection in a client group desires a query the shared pipeline
    /// already hydrated for another connection: the joining connection seeds its
    /// CVR from these rows WITHOUT re-adding the query (which would be a
    /// `DuplicateQuery`). Empty if the query is not active. Mirrors upstream,
    /// where a late client reads the group pipeline's existing output rather
    /// than re-hydrating (`view-syncer.ts` / `pipeline-driver.ts`).
    pub fn current_query_rows(&self, query_id: &str) -> Vec<PipelineRowChange> {
        self.pipelines
            .get(query_id)
            .map(|pipeline| additions(query_id, &pipeline.rows))
            .unwrap_or_default()
    }

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

fn apply_snapshot_change(
    sources: &mut HashMap<String, TableSource>,
    change: &SnapshotChange,
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<(), PipelineError> {
    let source = sources
        .get_mut(&change.table)
        .ok_or_else(|| PipelineError::UnknownTable(change.table.clone()))?;
    let fallback = empty_column_types();
    let spec = specs.get(&change.table);
    let column_types = spec.map(|spec| &spec.column_types).unwrap_or(&fallback);
    let min_row_version = spec.and_then(|spec| spec.min_row_version.as_deref());
    for previous in &change.prev_values {
        source.push(make_source_change_remove(sql_row_to_zql(
            previous.clone(),
            column_types,
            min_row_version,
        )));
    }
    if let Some(next) = &change.next_value {
        source.push(make_source_change_add(sql_row_to_zql(
            next.clone(),
            column_types,
            min_row_version,
        )));
    }
    Ok(())
}

/// Whether `add_query`/`advance` route `ast` through the replica-backed
/// operator graph (`build_pipeline` + fetch) instead of the legacy O(table)
/// `materialize_query`. This is the SINGLE predicate both call sites consult,
/// so a query hydrated via the graph is always advanced via the graph too
/// (otherwise `advance` would fall to `materialize_query`, whose in-memory
/// sources the graph path never loaded — an `UnknownTable` error).
///
/// Eligible = [`graph_can_build`] (every shape `build_pipeline` assembles:
/// single table, `where` filter, `related` joins, `whereExists`
/// `EXISTS`/`NOT EXISTS`, OR-of-correlated at any nesting depth via
/// `FanOut`/`FanIn`, `start`/`limit`). Bounded+ordered queries are eligible at
/// EVERY level: each subquery's `SqliteSource` is built ordered by its own
/// `order_by ++ pk` (see [`build_graph_source`] / [`referenced_sources`]), so
/// the graph's `Skip`/`Take` select the SAME subset `materialize_query` does.
fn is_graph_eligible(ast: &Ast) -> bool {
    graph_can_build(ast)
}

/// Whether `build_pipeline` can assemble an operator graph for `ast`. Every
/// AST shape the wire protocol can express now builds — plain filters, nested
/// `related`, `AND`-composed `EXISTS`, OR-of-correlated (fan-out/fan-in, at
/// any nesting), `start`/`limit` at root or child level. Kept as a recursive
/// walk so a future genuinely-unbuildable shape has a single place to gate
/// (a false positive would crash the server).
fn graph_can_build(ast: &Ast) -> bool {
    ast.related
        .iter()
        .flatten()
        .all(|csq| graph_can_build(&csq.subquery))
        && ast
            .where_
            .as_ref()
            .map(|c| correlated_subquery_asts(c).all(graph_can_build))
            .unwrap_or(true)
}

/// Every `(table, order_by)` pair `build_pipeline` will request a source for:
/// the root query plus, recursively, every `related` hop and every correlated
/// (`whereExists`) subquery. The same table can appear more than once with
/// different orderings — each gets its own pre-built source.
fn referenced_sources(ast: &Ast) -> Vec<(String, Option<Ordering>)> {
    fn walk(ast: &Ast, out: &mut Vec<(String, Option<Ordering>)>) {
        out.push((ast.table.clone(), ast.order_by.clone()));
        for csq in ast.related.iter().flatten() {
            walk(&csq.subquery, out);
        }
        if let Some(condition) = &ast.where_ {
            for subquery in correlated_subquery_asts(condition) {
                walk(subquery, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(ast, &mut out);
    out
}

/// Lookup key for a pre-built graph source: the table plus the ordering its
/// source was built with (mirroring [`BuildDelegate::get_source`]'s two
/// parameters).
fn source_key(table: &str, order_by: Option<&Ordering>) -> String {
    format!("{table}|{order_by:?}")
}

/// Every correlated-subquery AST reachable from `condition` (any `EXISTS`/`NOT
/// EXISTS`, under `AND` or `OR`), used to recurse [`graph_can_build`] into
/// `whereExists` subqueries.
fn correlated_subquery_asts(condition: &Condition) -> impl Iterator<Item = &Ast> {
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
    out.into_iter()
}

/// The `related` and `EXISTS` `whereExists` hops of `ast`, each paired with the
/// `Node.relationships` alias name `build_pipeline` populated it under. Order
/// and alias assignment mirror `build_pipeline` exactly: `related` hops deduped
/// by alias (last-one-wins, first-seen order) then keyed by their enumerated
/// position; `whereExists` hops keyed by their position among ALL gathered
/// correlated conditions (`EXISTS` and `NOT EXISTS`), then filtered to `EXISTS`
/// only — matching `materialize_query`, which inserts `EXISTS` subquery
/// children but not `NOT EXISTS`.
fn graph_child_hops(ast: &Ast) -> Vec<(&Ast, String)> {
    let mut hops: Vec<(&Ast, String)> = Vec::new();

    // `related`: dedupe by alias (last-one-wins, first-seen order), then name
    // each by its enumerated position — exactly `build_pipeline`'s two loops.
    let mut seen: Vec<String> = Vec::new();
    let mut chosen: Vec<&CorrelatedSubquery> = Vec::new();
    for csq in ast.related.iter().flatten() {
        let alias = graph_relationship_name(csq, seen.len());
        if let Some(pos) = seen.iter().position(|a| *a == alias) {
            chosen[pos] = csq;
        } else {
            seen.push(alias);
            chosen.push(csq);
        }
    }
    for (index, csq) in chosen.into_iter().enumerate() {
        hops.push((csq.subquery.as_ref(), graph_relationship_name(csq, index)));
    }

    // `whereExists`: enumerate over ALL gathered correlated conditions so the
    // alias index matches `build_pipeline`, then keep only `EXISTS` (whose
    // children `materialize_query` also inserts).
    if let Some(condition) = &ast.where_ {
        for (index, (csq, op)) in gather_exists_conditions(condition).into_iter().enumerate() {
            if op == ExistsOp::Exists {
                hops.push((csq.subquery.as_ref(), graph_relationship_name(csq, index)));
            }
        }
    }
    hops
}

/// The `Node.relationships` key `build_pipeline` uses for a correlated
/// subquery: its subquery `alias`, or a stable position-keyed name when the
/// AST carries none. Mirrors `build_pipeline`'s `relationship_name`.
fn graph_relationship_name(csq: &CorrelatedSubquery, index: usize) -> String {
    csq.subquery
        .alias
        .clone()
        .unwrap_or_else(|| format!("zsubq_{index}"))
}

/// Every correlated-subquery condition in `condition` with its `EXISTS`/`NOT
/// EXISTS` op, in the same traversal order as `build_pipeline`'s
/// `gather_exists_conditions`.
fn gather_exists_conditions(condition: &Condition) -> Vec<(&CorrelatedSubquery, ExistsOp)> {
    fn gather<'a>(condition: &'a Condition, out: &mut Vec<(&'a CorrelatedSubquery, ExistsOp)>) {
        match condition {
            Condition::CorrelatedSubquery { related, op, .. } => out.push((related, *op)),
            Condition::And { conditions } | Condition::Or { conditions } => {
                for c in conditions {
                    gather(c, out);
                }
            }
            Condition::Simple { .. } => {}
        }
    }
    let mut out = Vec::new();
    gather(condition, &mut out);
    out
}

/// Maps a table spec's declared ZQL column value-types (`pg_data_type`
/// [`ColumnValueType`]) into the `query_builder::ColumnType` the
/// [`SqliteSource`] read path uses, so graph fetches restore booleans/JSON
/// identically to `sql_row_to_zql`. Columns without a declared type fall back
/// to a generic optional `String`, matching [`SqliteSource::new`].
fn to_source_column_types(
    column_types: &ColumnTypes,
    columns: &[String],
) -> BTreeMap<String, zero_cache_sqlite::query_builder::ColumnType> {
    use zero_cache_protocol::client_schema::ValueType as ClientValueType;
    use zero_cache_sqlite::query_builder::ColumnType;
    columns
        .iter()
        .map(|column| {
            let value_type = match column_types.get(column) {
                Some(ColumnValueType::Boolean) => ClientValueType::Boolean,
                Some(ColumnValueType::Json) => ClientValueType::Json,
                Some(ColumnValueType::Number) => ClientValueType::Number,
                Some(ColumnValueType::Null) => ClientValueType::Null,
                Some(ColumnValueType::String) | None => ClientValueType::String,
            };
            (
                column.clone(),
                ColumnType {
                    value_type,
                    optional: true,
                },
            )
        })
        .collect()
}

fn is_direct_incremental_query(ast: &Ast) -> bool {
    ast.limit.is_none()
        && ast.start.is_none()
        && ast.related.as_ref().is_none_or(Vec::is_empty)
        && ast
            .where_
            .as_ref()
            .is_none_or(|condition| !has_correlated_subquery(condition))
}

fn has_correlated_subquery(condition: &Condition) -> bool {
    match condition {
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(has_correlated_subquery)
        }
        Condition::CorrelatedSubquery { .. } => true,
        _ => false,
    }
}

fn apply_direct_changes(
    query_id: &str,
    pipeline: &mut Pipeline,
    changes: &[SnapshotChange],
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<Vec<PipelineRowChange>, PipelineError> {
    let relevant = changes
        .iter()
        .filter(|change| change.table == pipeline.ast.table);
    let mut previous = BTreeMap::new();
    let mut affected_keys = BTreeSet::new();
    let fallback = empty_column_types();
    let spec = specs.get(&pipeline.ast.table);
    let column_types = spec.map(|spec| &spec.column_types).unwrap_or(&fallback);
    let min_row_version = spec.and_then(|spec| spec.min_row_version.as_deref());

    for change in relevant {
        for row in &change.prev_values {
            let row = sql_row_to_zql(row.clone(), column_types, min_row_version);
            let entry = materialized_row(&change.table, row, specs)?;
            let key = materialized_key(&entry);
            affected_keys.insert(key.clone());
            if let Some(old) = pipeline.rows.remove(&key) {
                previous.entry(key).or_insert(old);
            }
        }

        if let Some(row) = &change.next_value {
            let row = sql_row_to_zql(row.clone(), column_types, min_row_version);
            let entry = materialized_row(&change.table, row, specs)?;
            let key = materialized_key(&entry);
            affected_keys.insert(key.clone());
            if direct_row_matches(&pipeline.ast, &entry.row) {
                pipeline.rows.insert(key, entry);
            }
        }
    }

    let next = affected_keys
        .into_iter()
        .filter_map(|key| pipeline.rows.get(&key).cloned().map(|row| (key, row)))
        .collect();
    Ok(diff_rows(query_id, &previous, &next))
}

fn direct_row_matches(ast: &Ast, row: &Row) -> bool {
    let Some(condition) = &ast.where_ else {
        return true;
    };
    let unreachable_exists: ExistsFn<'_> = std::rc::Rc::new(|_, _| {
        unreachable!("direct incremental queries cannot contain correlated subqueries")
    });
    create_predicate_with_exists(condition, unreachable_exists)(row)
}

fn materialize_query(
    ast: &Ast,
    sources: &HashMap<String, TableSource>,
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<BTreeMap<String, MaterializedRow>, PipelineError> {
    let mut output = BTreeMap::new();
    let roots = matching_rows(ast, None, sources, specs)?;
    for row in &roots {
        insert_row(&ast.table, row.clone(), specs, &mut output)?;
    }
    for relation in ast.related.iter().flatten() {
        materialize_related(&roots, relation, sources, specs, &mut output)?;
    }
    if let Some(condition) = &ast.where_ {
        for relation in correlated_subqueries(condition) {
            materialize_related(&roots, &relation, sources, specs, &mut output)?;
        }
    }
    Ok(output)
}

fn matching_rows(
    ast: &Ast,
    correlation: Option<(&Row, &CorrelatedSubquery)>,
    sources: &HashMap<String, TableSource>,
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<Vec<Row>, PipelineError> {
    let source = sources
        .get(&ast.table)
        .ok_or_else(|| PipelineError::UnknownTable(ast.table.clone()))?;
    let mut rows: Vec<_> = source
        .fetch(&FetchRequest::default())
        .map(|node| node.row)
        .collect();
    if let Some((parent, relation)) = correlation {
        let values: Vec<_> = relation
            .correlation
            .parent_field
            .iter()
            .map(|field| get(parent, field))
            .collect();
        rows.retain(|child| {
            relation
                .correlation
                .child_field
                .iter()
                .zip(values.iter())
                .all(|(field, value)| *value != JsonValue::Null && get(child, field) == *value)
        });
    }
    if let Some(condition) = &ast.where_ {
        let exists: ExistsFn<'_> = std::rc::Rc::new(|relation, parent| {
            matching_rows(&relation.subquery, Some((parent, relation)), sources, specs)
                .map(|rows| !rows.is_empty())
                .unwrap_or(false)
        });
        let predicate = create_predicate_with_exists(condition, exists);
        rows.retain(|row| predicate(row));
    }
    let ordering = completed_ordering(ast, specs)?;
    rows.sort_by(make_comparator(&ordering, false));
    if let Some(start) = &ast.start {
        rows = apply_start(rows, start, &ordering);
    }
    if let Some(limit) = ast.limit {
        rows.truncate(limit.max(0.0) as usize);
    }
    Ok(rows)
}

fn materialize_related(
    parents: &[Row],
    relation: &CorrelatedSubquery,
    sources: &HashMap<String, TableSource>,
    specs: &BTreeMap<String, SnapshotTableSpec>,
    output: &mut BTreeMap<String, MaterializedRow>,
) -> Result<(), PipelineError> {
    for parent in parents {
        let children = matching_rows(&relation.subquery, Some((parent, relation)), sources, specs)?;
        for child in &children {
            insert_row(&relation.subquery.table, child.clone(), specs, output)?;
        }
        for nested in relation.subquery.related.iter().flatten() {
            materialize_related(&children, nested, sources, specs, output)?;
        }
        if let Some(condition) = &relation.subquery.where_ {
            for nested in correlated_subqueries(condition) {
                materialize_related(&children, &nested, sources, specs, output)?;
            }
        }
    }
    Ok(())
}

fn completed_ordering(
    ast: &Ast,
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<Ordering, PipelineError> {
    let spec = specs
        .get(&ast.table)
        .ok_or_else(|| PipelineError::UnknownTable(ast.table.clone()))?;
    Ok(source_ordering(ast.order_by.as_ref(), &spec.primary_key))
}

/// The total order `order_by ++ primary_key`: the query's `order_by` (empty if
/// `None`) followed by every PK column not already named in it (appended
/// `Asc`). This is the ordering [`materialize_query`] sorts by
/// ([`completed_ordering`]) AND the one [`build_graph_source`] gives the root
/// `SqliteSource`, so the graph's `Skip`/`Take` select the SAME bounded subset.
fn source_ordering(order_by: Option<&Ordering>, primary_key: &[String]) -> Ordering {
    let mut ordering = order_by.cloned().unwrap_or_default();
    for key in primary_key {
        if !ordering.iter().any(|(column, _)| column == key) {
            ordering.push((key.clone(), Direction::Asc));
        }
    }
    ordering
}

fn apply_start(rows: Vec<Row>, start: &Bound, ordering: &Ordering) -> Vec<Row> {
    let JsonValue::Object(bound) = &start.row else {
        return rows;
    };
    let compare = make_comparator(ordering, false);
    rows.into_iter()
        .filter(|row| {
            let result = compare(row, bound);
            result.is_gt() || (!start.exclusive && result.is_eq())
        })
        .collect()
}

fn correlated_subqueries(condition: &Condition) -> Vec<CorrelatedSubquery> {
    match condition {
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().flat_map(correlated_subqueries).collect()
        }
        Condition::CorrelatedSubquery { related, op, .. } if *op == ExistsOp::Exists => {
            vec![related.clone()]
        }
        _ => Vec::new(),
    }
}

fn insert_row(
    table: &str,
    row: Row,
    specs: &BTreeMap<String, SnapshotTableSpec>,
    output: &mut BTreeMap<String, MaterializedRow>,
) -> Result<(), PipelineError> {
    let entry = materialized_row(table, row, specs)?;
    output.insert(materialized_key(&entry), entry);
    Ok(())
}

fn materialized_row(
    table: &str,
    row: Row,
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<MaterializedRow, PipelineError> {
    let spec = specs
        .get(table)
        .ok_or_else(|| PipelineError::UnknownTable(table.to_string()))?;
    let values: BTreeMap<_, _> = spec
        .primary_key
        .iter()
        .map(|column| (column.clone(), get(&row, column)))
        .collect();
    Ok(MaterializedRow {
        table: table.to_string(),
        row,
        row_key: values,
    })
}

fn materialized_key(entry: &MaterializedRow) -> String {
    format!(
        "{}:{}",
        entry.table,
        stringify(&JsonValue::Object(
            entry.row_key.clone().into_iter().collect()
        ))
    )
}

fn additions(query_id: &str, rows: &BTreeMap<String, MaterializedRow>) -> Vec<PipelineRowChange> {
    rows.values()
        .map(|entry| PipelineRowChange {
            query_id: query_id.to_string(),
            table: entry.table.clone(),
            kind: PipelineRowChangeKind::Add,
            row: entry.row.clone(),
            old_row: None,
            row_key: entry.row_key.clone(),
        })
        .collect()
}

fn signature_for_rows<'a>(
    rows: impl Iterator<Item = &'a MaterializedRow>,
) -> Result<u64, PipelineError> {
    let mut signature = 0;
    for row in rows {
        signature ^= row_id_signature_unit(&zero_cache_types::row_key::RowId::new(
            "public",
            row.table.clone(),
            row.row_key
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        ))?;
    }
    Ok(signature)
}

fn diff_rows(
    query_id: &str,
    previous: &BTreeMap<String, MaterializedRow>,
    next: &BTreeMap<String, MaterializedRow>,
) -> Vec<PipelineRowChange> {
    let keys: BTreeSet<_> = previous.keys().chain(next.keys()).cloned().collect();
    keys.into_iter()
        .filter_map(|key| match (previous.get(&key), next.get(&key)) {
            (None, Some(row)) => Some(PipelineRowChange {
                query_id: query_id.into(),
                table: row.table.clone(),
                kind: PipelineRowChangeKind::Add,
                row: row.row.clone(),
                old_row: None,
                row_key: row.row_key.clone(),
            }),
            (Some(row), None) => Some(PipelineRowChange {
                query_id: query_id.into(),
                table: row.table.clone(),
                kind: PipelineRowChangeKind::Remove,
                row: row.row.clone(),
                old_row: None,
                row_key: row.row_key.clone(),
            }),
            (Some(old), Some(new)) if old.row != new.row => Some(PipelineRowChange {
                query_id: query_id.into(),
                table: new.table.clone(),
                kind: PipelineRowChangeKind::Edit,
                row: new.row.clone(),
                old_row: Some(old.row.clone()),
                row_key: new.row_key.clone(),
            }),
            _ => None,
        })
        .collect()
}

fn get(row: &Row, field: &str) -> JsonValue {
    row.iter()
        .find(|(column, _)| column == field)
        .map(|(_, value)| value.clone())
        .unwrap_or(JsonValue::Null)
}

/// Converts a raw SQLite replica row into a ZQL row, restoring each column's
/// declared ZQL value type (`fromSQLiteTypes` upstream). Without the type map,
/// booleans stored as SQLite `0`/`1` would ship as numbers and JSON columns as
/// raw text — diverging from official Zero, which emits `true`/`false` and
/// parsed JSON on both the hydration and the incremental (poke) paths.
/// Unknown columns default to a generic (string/number) conversion.
///
/// The row's `_0_version` is clamped up to `min_row_version` (upstream
/// `Streamer.#streamNodes`): a row whose stored version predates a table's
/// minimum (e.g. just after the table was re-added/backfilled) must be emitted
/// at the minimum, or the client's CVR row versions diverge from the reference
/// server. Versions are lexi-encoded, so a plain string comparison suffices.
fn sql_row_to_zql(
    row: Vec<(String, SqlValue)>,
    column_types: &ColumnTypes,
    min_row_version: Option<&str>,
) -> Row {
    row.into_iter()
        .map(|(column, value)| {
            if column == ZERO_VERSION_COLUMN_NAME {
                if let (SqlValue::Text(version), Some(min)) = (&value, min_row_version) {
                    if version.as_str() < min {
                        return (column, JsonValue::String(min.to_string()));
                    }
                }
            }
            let value_type = column_types.get(&column).copied();
            let value = sql_value_to_zql(value, value_type);
            (column, value)
        })
        .collect()
}

/// Clamps a ZQL row's `_0_version` up to `min_row_version`, matching the same
/// clamp [`sql_row_to_zql`] applies on the `materialize_query` path (upstream
/// `Streamer.#streamNodes`). The replica-backed graph fetch restores column
/// value-types but does NOT know a table's `minRowVersion`, so the driver
/// applies the clamp to every row it drains from the graph — keeping graph
/// hydration byte-identical to `materialize_query`. Versions are lexi-encoded,
/// so a plain string comparison suffices.
fn clamp_row_version(row: Row, min_row_version: Option<&str>) -> Row {
    let Some(min) = min_row_version else {
        return row;
    };
    row.into_iter()
        .map(|(column, value)| {
            if column == ZERO_VERSION_COLUMN_NAME {
                if let JsonValue::String(version) = &value {
                    if version.as_str() < min {
                        return (column, JsonValue::String(min.to_string()));
                    }
                }
            }
            (column, value)
        })
        .collect()
}

fn sql_value_to_zql(value: SqlValue, value_type: Option<ColumnValueType>) -> JsonValue {
    match value_type {
        Some(ColumnValueType::Boolean) => match value {
            SqlValue::Null => JsonValue::Null,
            SqlValue::Integer(value) => JsonValue::Bool(value != 0),
            SqlValue::Real(value) => JsonValue::Bool(value != 0.0),
            SqlValue::Text(value) => JsonValue::Bool(matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "t" | "true"
            )),
            SqlValue::Blob(value) => JsonValue::Bool(!value.is_empty() && value != b"0"),
        },
        Some(ColumnValueType::Json) => match value {
            SqlValue::Null => JsonValue::Null,
            SqlValue::Text(value) => {
                zero_cache_shared::bigint_json::parse(&value).unwrap_or(JsonValue::String(value))
            }
            other => generic_sql_value_to_zql(other),
        },
        _ => generic_sql_value_to_zql(value),
    }
}

fn generic_sql_value_to_zql(value: SqlValue) -> JsonValue {
    match value {
        SqlValue::Null => JsonValue::Null,
        SqlValue::Integer(value) => JsonValue::Number(value as f64),
        SqlValue::Real(value) => JsonValue::Number(value),
        SqlValue::Text(value) => JsonValue::String(value),
        SqlValue::Blob(value) => JsonValue::String(String::from_utf8_lossy(&value).into()),
    }
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::{ColumnReference, LiteralValue, SimpleOperator, ValuePosition};
    use zero_cache_sqlite::change_log::{ChangeLog, CREATE_CHANGELOG_SCHEMA};
    use zero_cache_sqlite::replication_state::{
        init_replication_state, update_replication_watermark,
    };
    use zero_cache_sqlite::StatementRunner;

    fn path() -> String {
        // A process-wide atomic counter guarantees uniqueness even when two
        // tests call `path()` within the same nanosecond on different threads
        // (a plain timestamp can collide under parallel `cargo test`, letting
        // two tests stomp the same replica file).
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir()
            .join(format!(
                "zero-pipeline-{}-{}-{}.db",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                seq
            ))
            .to_string_lossy()
            .into_owned()
    }

    fn query() -> Ast {
        Ast {
            table: "issue".into(),
            where_: Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "active".into(),
                }),
                right: ValuePosition::Literal(LiteralValue::Number(1.0)),
            }),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        }
    }

    fn specs() -> BTreeMap<String, SnapshotTableSpec> {
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

    #[test]
    fn persistent_pipeline_hydrates_once_then_advances_from_snapshot_diff() {
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

        let mut driver = PipelineDriver::new(
            &path,
            "zero",
            None,
            specs(),
            BTreeSet::from(["issue".into()]),
        )
        .unwrap();
        let initial = driver.add_query("q", query()).unwrap();
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].kind, PipelineRowChangeKind::Add);
        let initial_signature = driver.row_set_signature("q").unwrap();
        assert_ne!(initial_signature, 0);

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

        let changes = driver.advance().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].kind, PipelineRowChangeKind::Remove);
        assert_eq!(driver.row_set_signature("q"), Some(0));
        assert_eq!(driver.version().unwrap(), "01");

        writer
            .run("UPDATE issue SET active=1, _0_version='02' WHERE id=1", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "02",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(1.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "02").unwrap();
        let changes = driver.advance().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].kind, PipelineRowChangeKind::Add);
        assert_eq!(driver.row_set_signature("q"), Some(initial_signature));

        writer
            .run("UPDATE issue SET _0_version='03' WHERE id=1", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "03",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(1.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "03").unwrap();
        let changes = driver.advance().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].kind, PipelineRowChangeKind::Edit);
        assert_eq!(driver.row_set_signature("q"), Some(initial_signature));
        assert_eq!(driver.version().unwrap(), "03");

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    fn typed_specs() -> BTreeMap<String, SnapshotTableSpec> {
        BTreeMap::from([(
            "issue".into(),
            SnapshotTableSpec {
                name: "issue".into(),
                columns: vec!["id".into(), "active".into(), "_0_version".into()],
                column_types: BTreeMap::from([("active".into(), ColumnValueType::Boolean)]),
                primary_key: vec!["id".into()],
                unique_keys: vec![],
                min_row_version: Some("00".into()),
            },
        )])
    }

    /// A row whose stored `_0_version` predates the table's `minRowVersion`
    /// must be emitted at the minimum, matching upstream `Streamer.#streamNodes`
    /// — otherwise the client's CVR row versions diverge from the reference.
    #[test]
    fn emitted_rows_clamp_version_up_to_min_row_version() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "05", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        // Stored version "01" is below the table's minimum "05".
        writer
            .run("INSERT INTO issue VALUES (1, 1, '01')", &[])
            .unwrap();

        let mut specs = typed_specs();
        specs.get_mut("issue").unwrap().min_row_version = Some("05".into());

        let query = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let mut driver =
            PipelineDriver::new(&path, "zero", None, specs, BTreeSet::from(["issue".into()]))
                .unwrap();
        let initial = driver.add_query("q", query).unwrap();
        assert_eq!(initial.len(), 1);
        assert_eq!(
            get(&initial[0].row, "_0_version"),
            JsonValue::String("05".into()),
            "version below minRowVersion must be clamped up"
        );

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// Equivalence proof (redesign §8.1, increment 3): for a single-table
    /// filtered query, the `build_pipeline` + fetch graph hydration must yield
    /// the SAME `PipelineRowChange` set (order-insensitive, keyed by row key)
    /// as `materialize_query`. This is the correctness gate before a later
    /// increment flips the graph on by default.
    #[test]
    fn graph_hydration_matches_materialize_query_for_single_table_filter() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        // Two matching (active=1) and one non-matching row so the WHERE filter
        // is actually exercised.
        writer
            .run(
                "INSERT INTO issue VALUES (1, 1, '00'), (2, 0, '00'), (3, 1, '00')",
                &[],
            )
            .unwrap();

        // Reference: force the legacy (`materialize_query`) hydration path.
        let mut baseline = PipelineDriver::new(
            &path,
            "zero",
            None,
            specs(),
            BTreeSet::from(["issue".into()]),
        )
        .unwrap();
        baseline.graph_enabled = false;
        let expected = baseline.add_query("q", query()).unwrap();
        // Legacy path loads the in-memory source; graph path must not.
        assert!(baseline.sources.contains_key("issue"));

        // Candidate: the DEFAULT graph path on the SAME committed replica.
        let mut graph = PipelineDriver::new(
            &path,
            "zero",
            None,
            specs(),
            BTreeSet::from(["issue".into()]),
        )
        .unwrap();
        assert!(graph.graph_enabled, "graph must be the default");
        let actual = graph.add_query("q", query()).unwrap();

        assert_eq!(
            sorted_by_key(expected),
            sorted_by_key(actual),
            "graph hydration must equal materialize_query for a single-table filter"
        );
        // The direct query hydrated via the graph must NOT have loaded the
        // in-memory full-table source.
        assert!(
            !graph.sources.contains_key("issue"),
            "direct query must not load the in-memory source"
        );

        baseline.destroy().unwrap();
        graph.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// Runs `ast` through BOTH hydration paths on the SAME committed replica
    /// (`path`) and asserts they emit an identical `PipelineRowChange` set
    /// (order-insensitive). Returns the graph-path changes for further checks.
    fn assert_graph_matches_materialize(
        path: &str,
        specs: BTreeMap<String, SnapshotTableSpec>,
        tables: BTreeSet<String>,
        ast: Ast,
    ) -> Vec<PipelineRowChange> {
        let mut baseline =
            PipelineDriver::new(path, "zero", None, specs.clone(), tables.clone()).unwrap();
        baseline.graph_enabled = false;
        let expected = baseline.add_query("q", ast.clone()).unwrap();

        let mut graph = PipelineDriver::new(path, "zero", None, specs, tables).unwrap();
        assert!(graph.graph_enabled, "graph must be the default");
        let actual = graph.add_query("q", ast).unwrap();
        assert!(
            !graph.sources.contains_key("issue"),
            "direct query must not load the in-memory source"
        );

        assert_eq!(
            sorted_by_key(expected),
            sorted_by_key(actual.clone()),
            "graph hydration must equal materialize_query"
        );

        baseline.destroy().unwrap();
        graph.destroy().unwrap();
        actual
    }

    /// Equivalence over the all-rows (no WHERE) fixture.
    #[test]
    fn graph_hydration_matches_materialize_query_for_all_rows() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run(
                "INSERT INTO issue VALUES (1, 1, '00'), (2, 0, '00'), (3, 1, '00')",
                &[],
            )
            .unwrap();

        let all_rows = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let changes = assert_graph_matches_materialize(
            &path,
            specs(),
            BTreeSet::from(["issue".into()]),
            all_rows,
        );
        assert_eq!(changes.len(), 3, "all three rows hydrated");

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// Equivalence over a fixture where the stored `_0_version` predates
    /// `minRowVersion`: the graph path must apply the SAME clamp as
    /// `materialize_query`, so both emit the row at the minimum version.
    #[test]
    fn graph_hydration_matches_materialize_query_with_version_clamp() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "05", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        // Stored versions "01"/"02" are below the table's minimum "05".
        writer
            .run("INSERT INTO issue VALUES (1, 1, '01'), (2, 1, '02')", &[])
            .unwrap();

        let mut specs = typed_specs();
        specs.get_mut("issue").unwrap().min_row_version = Some("05".into());

        let query = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let changes =
            assert_graph_matches_materialize(&path, specs, BTreeSet::from(["issue".into()]), query);
        for change in &changes {
            assert_eq!(
                get(&change.row, "_0_version"),
                JsonValue::String("05".into()),
                "graph path must clamp version up to minRowVersion"
            );
        }

        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// Orders a change set by `(table, row_key)` so two hydrations can be
    /// compared independent of emission order.
    fn sorted_by_key(mut changes: Vec<PipelineRowChange>) -> Vec<PipelineRowChange> {
        changes.sort_by_key(|change| {
            (
                change.table.clone(),
                stringify(&JsonValue::Object(
                    change.row_key.clone().into_iter().collect(),
                )),
            )
        });
        changes
    }

    /// Regression for the differential-conformance bug where a boolean column
    /// updated live shipped `1` instead of `true`: the incremental (poke) path
    /// must restore the declared `Boolean` value type just like hydration does.
    #[test]
    fn incremental_advance_restores_boolean_value_type() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (1, 0, '00')", &[])
            .unwrap();

        // Query all rows (no `active` predicate) so the live update produces an
        // Edit carrying the row's `active` value on the wire.
        let query = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };

        let mut driver = PipelineDriver::new(
            &path,
            "zero",
            None,
            typed_specs(),
            BTreeSet::from(["issue".into()]),
        )
        .unwrap();
        let initial = driver.add_query("q", query).unwrap();
        assert_eq!(initial.len(), 1);
        // Hydration path already restores the boolean.
        assert_eq!(get(&initial[0].row, "active"), JsonValue::Bool(false));

        writer
            .run("UPDATE issue SET active=1, _0_version='01' WHERE id=1", &[])
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

        let changes = driver.advance().unwrap();
        assert_eq!(changes.len(), 1);
        // The incremental path must ship `true`, not `1`.
        assert_eq!(get(&changes[0].row, "active"), JsonValue::Bool(true));

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    // ---- Complex-query graph/materialize equivalence (oracle tests) ----
    //
    // The correctness gate for routing COMPLEX queries (`related` joins /
    // `whereExists`) through the replica-backed graph (`hydrate_via_graph`)
    // instead of the O(table) `materialize_query`. For representative complex
    // shapes we assert the two paths produce a byte-identical row set — both on
    // first hydration AND after a sequence of child-row mutations.

    use zero_cache_protocol::ast::Correlation;

    fn parent_child_specs() -> BTreeMap<String, SnapshotTableSpec> {
        BTreeMap::from([
            (
                "issue".into(),
                SnapshotTableSpec {
                    name: "issue".into(),
                    columns: vec!["id".into(), "active".into(), "_0_version".into()],
                    column_types: BTreeMap::new(),
                    primary_key: vec!["id".into()],
                    unique_keys: vec![],
                    min_row_version: Some("00".into()),
                },
            ),
            (
                "comment".into(),
                SnapshotTableSpec {
                    name: "comment".into(),
                    columns: vec!["id".into(), "issueID".into(), "_0_version".into()],
                    column_types: BTreeMap::new(),
                    primary_key: vec!["id".into()],
                    unique_keys: vec![],
                    min_row_version: Some("00".into()),
                },
            ),
        ])
    }

    /// Correlation `issue.id = comment.issueID`, aliased `comments`.
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

    fn active_eq_1() -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: "active".into(),
            }),
            right: ValuePosition::Literal(LiteralValue::Number(1.0)),
        }
    }

    /// The representative COMPLEX shapes the switch must preserve:
    /// (a) `related` child, (b) `whereExists` EXISTS, (c) `whereExists` NOT
    /// EXISTS, (d) a filtered EXISTS (`active = 1 AND EXISTS(comments)`).
    fn complex_asts() -> Vec<Ast> {
        vec![
            Ast {
                table: "issue".into(),
                related: Some(vec![comments_rel()]),
                order_by: Some(vec![("id".into(), Direction::Asc)]),
                ..Default::default()
            },
            Ast {
                table: "issue".into(),
                where_: Some(exists_cond(ExistsOp::Exists)),
                order_by: Some(vec![("id".into(), Direction::Asc)]),
                ..Default::default()
            },
            Ast {
                table: "issue".into(),
                where_: Some(exists_cond(ExistsOp::NotExists)),
                order_by: Some(vec![("id".into(), Direction::Asc)]),
                ..Default::default()
            },
            Ast {
                table: "issue".into(),
                where_: Some(Condition::And {
                    conditions: vec![active_eq_1(), exists_cond(ExistsOp::Exists)],
                }),
                order_by: Some(vec![("id".into(), Direction::Asc)]),
                ..Default::default()
            },
        ]
    }

    fn setup_parent_child(path: &str) -> StatementRunner {
        let writer = StatementRunner::open_file(path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .exec("CREATE TABLE comment (id INTEGER PRIMARY KEY, issueID INTEGER, _0_version TEXT)")
            .unwrap();
        // Issues: 1 (active), 2 (inactive), 3 (active).
        writer
            .run(
                "INSERT INTO issue VALUES (1, 1, '00'), (2, 0, '00'), (3, 1, '00')",
                &[],
            )
            .unwrap();
        // Comments: issue 1 has two, issue 3 has one, issue 2 has none.
        writer
            .run(
                "INSERT INTO comment VALUES (10, 1, '00'), (11, 1, '00'), (12, 3, '00')",
                &[],
            )
            .unwrap();
        writer
    }

    fn parent_child_driver(path: &str) -> PipelineDriver {
        let mut driver = PipelineDriver::new(
            path,
            "zero",
            None,
            parent_child_specs(),
            BTreeSet::from(["issue".into(), "comment".into()]),
        )
        .unwrap();
        // Load the in-memory sources the `materialize_query` oracle side reads.
        driver.ensure_source("issue").unwrap();
        driver.ensure_source("comment").unwrap();
        driver
    }

    /// Asserts `hydrate_via_graph(ast)` produces the SAME row set as
    /// `materialize_query(ast, driver.sources, driver.table_specs)` — the
    /// correctness gate. Compared as sorted `Add` change sets so both row bodies
    /// AND keys are checked, order-insensitively.
    fn assert_oracle_eq(driver: &PipelineDriver, ast: &Ast) {
        let graph = driver.hydrate_via_graph(ast).unwrap();
        let materialized = materialize_query(ast, &driver.sources, &driver.table_specs).unwrap();
        assert_eq!(
            sorted_by_key(additions("q", &materialized)),
            sorted_by_key(additions("q", &graph)),
            "graph hydration must equal materialize_query for ast: {ast:?}"
        );
    }

    #[test]
    fn graph_hydration_matches_materialize_for_complex_queries() {
        let path = path();
        let writer = setup_parent_child(&path);
        let driver = parent_child_driver(&path);

        for ast in &complex_asts() {
            assert!(
                is_graph_eligible(ast),
                "complex ast must be graph-eligible: {ast:?}"
            );
            assert_oracle_eq(&driver, ast);
        }

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn graph_and_materialize_stay_equal_across_child_mutations() {
        let path = path();
        let writer = setup_parent_child(&path);
        let mut driver = parent_child_driver(&path);
        let asts = complex_asts();

        let assert_all = |driver: &PipelineDriver| {
            for ast in &asts {
                assert_oracle_eq(driver, ast);
            }
        };

        // Baseline (before any mutation).
        assert_all(&driver);

        // INSERT: give issue 2 its first comment (flips its EXISTS membership).
        writer
            .run("INSERT INTO comment VALUES (13, 2, '01')", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "01",
                0,
                "comment",
                &vec![("id".into(), JsonValue::Number(13.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "01").unwrap();
        // `advance` moves the snapshot to head AND updates the in-memory sources
        // the materialize oracle reads, so both paths see the same committed
        // state on the next comparison.
        driver.advance().unwrap();
        assert_all(&driver);

        // UPDATE: move comment 13 from issue 2 to issue 3.
        writer
            .run(
                "UPDATE comment SET issueID=3, _0_version='02' WHERE id=13",
                &[],
            )
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "02",
                0,
                "comment",
                &vec![("id".into(), JsonValue::Number(13.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "02").unwrap();
        driver.advance().unwrap();
        assert_all(&driver);

        // DELETE: remove issue 3's original comment 12.
        writer.run("DELETE FROM comment WHERE id=12", &[]).unwrap();
        ChangeLog::new(&writer)
            .log_delete_op(
                "03",
                0,
                "comment",
                &vec![("id".into(), JsonValue::Number(12.0))],
            )
            .unwrap();
        update_replication_watermark(&writer, "03").unwrap();
        driver.advance().unwrap();
        assert_all(&driver);

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// A hydrate → advance → advance sequence for a COMPLEX query registered via
    /// `add_query` (which now hydrates it through the graph, loading NO in-memory
    /// source) must stay correct across child mutations — proving the graph
    /// hydration and graph re-advance agree end to end.
    #[test]
    fn complex_query_hydrate_then_advance_stays_correct() {
        let path = path();
        let writer = setup_parent_child(&path);
        let mut driver = PipelineDriver::new(
            &path,
            "zero",
            None,
            parent_child_specs(),
            BTreeSet::from(["issue".into(), "comment".into()]),
        )
        .unwrap();
        assert!(driver.graph_enabled);

        // `whereExists` EXISTS: issues 1 and 3 have comments.
        let ast = Ast {
            table: "issue".into(),
            where_: Some(exists_cond(ExistsOp::Exists)),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let initial = driver.add_query("q", ast).unwrap();
        // Complex graph hydration must NOT have loaded the in-memory sources.
        assert!(!driver.sources.contains_key("issue"));
        assert!(!driver.sources.contains_key("comment"));
        // Hydration set: issue rows 1 and 3, plus their comment children 10, 11,
        // 12 (EXISTS includes the matched children, like `materialize_query`).
        let issue_adds = initial
            .iter()
            .filter(|c| c.table == "issue" && c.kind == PipelineRowChangeKind::Add)
            .count();
        assert_eq!(issue_adds, 2, "issues 1 and 3 satisfy EXISTS");

        // Give issue 2 a comment -> issue 2 now satisfies EXISTS (a new Add).
        writer
            .run("INSERT INTO comment VALUES (13, 2, '01')", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "01",
                0,
                "comment",
                &vec![("id".into(), JsonValue::Number(13.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "01").unwrap();
        let changes = driver.advance().unwrap();
        assert!(
            changes.iter().any(|c| c.table == "issue"
                && c.kind == PipelineRowChangeKind::Add
                && get(&c.row, "id") == JsonValue::Number(2.0)),
            "issue 2 must be added once it satisfies EXISTS"
        );

        // Remove issue 1's comments -> issue 1 no longer satisfies EXISTS.
        writer
            .run("DELETE FROM comment WHERE id IN (10, 11)", &[])
            .unwrap();
        for id in [10.0, 11.0] {
            ChangeLog::new(&writer)
                .log_delete_op(
                    "02",
                    0,
                    "comment",
                    &vec![("id".into(), JsonValue::Number(id))],
                )
                .unwrap();
        }
        update_replication_watermark(&writer, "02").unwrap();
        let changes = driver.advance().unwrap();
        assert!(
            changes.iter().any(|c| c.table == "issue"
                && c.kind == PipelineRowChangeKind::Remove
                && get(&c.row, "id") == JsonValue::Number(1.0)),
            "issue 1 must be removed once it no longer satisfies EXISTS"
        );

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    // ---- Bounded + ordered graph/materialize equivalence (oracle tests) ----
    //
    // The correctness gate for routing a `limit`/`start` query that pairs a
    // bound with an explicit `order_by` through the graph. The root
    // `SqliteSource` is now ordered by `order_by ++ pk` (matching
    // `materialize_query`'s total order), so the graph's `Skip`/`Take` must
    // select the IDENTICAL subset. For each representative shape (ascending and
    // descending order, with and without a `where` filter, `limit` and/or
    // `start`) we assert `hydrate_via_graph(ast)` equals
    // `materialize_query(ast, driver.sources, driver.table_specs)` — both on
    // first hydration AND after a mutation + advance.

    fn setup_issue_ordered(path: &str) -> StatementRunner {
        let writer = StatementRunner::open_file(path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        // A mix of active/inactive rows so ordering by `active` (with an `id`
        // tie-break) actually differs from PK order, exercising the non-PK sort.
        writer
            .run(
                "INSERT INTO issue VALUES (1,1,'00'),(2,0,'00'),(3,1,'00'),(4,0,'00'),(5,1,'00')",
                &[],
            )
            .unwrap();
        writer
    }

    fn ordered_driver(path: &str) -> PipelineDriver {
        let mut driver = PipelineDriver::new(
            path,
            "zero",
            None,
            specs(),
            BTreeSet::from(["issue".into()]),
        )
        .unwrap();
        // Load the in-memory source the `materialize_query` oracle side reads.
        driver.ensure_source("issue").unwrap();
        driver
    }

    fn id_start(id: f64, exclusive: bool) -> Bound {
        Bound {
            row: JsonValue::Object(vec![("id".into(), JsonValue::Number(id))]),
            exclusive,
        }
    }

    /// Representative bounded+ordered shapes the switch must preserve. Each
    /// pairs a `limit` and/or `start` with an explicit `order_by` — the exact
    /// shape the removed `has_order_sensitive_bound` used to exclude.
    fn bounded_ordered_asts() -> Vec<Ast> {
        vec![
            // limit + order by a non-PK column ASC (id tie-break).
            Ast {
                table: "issue".into(),
                order_by: Some(vec![
                    ("active".into(), Direction::Asc),
                    ("id".into(), Direction::Asc),
                ]),
                limit: Some(2.0),
                ..Default::default()
            },
            // limit + order by a non-PK column DESC (id tie-break).
            Ast {
                table: "issue".into(),
                order_by: Some(vec![
                    ("active".into(), Direction::Desc),
                    ("id".into(), Direction::Asc),
                ]),
                limit: Some(3.0),
                ..Default::default()
            },
            // limit + order DESC + a `where` filter.
            Ast {
                table: "issue".into(),
                where_: Some(active_eq_1()),
                order_by: Some(vec![("id".into(), Direction::Desc)]),
                limit: Some(2.0),
                ..Default::default()
            },
            // start (exclusive) + order ASC, no limit.
            Ast {
                table: "issue".into(),
                order_by: Some(vec![("id".into(), Direction::Asc)]),
                start: Some(id_start(2.0, true)),
                ..Default::default()
            },
            // start (inclusive) + limit + order DESC.
            Ast {
                table: "issue".into(),
                order_by: Some(vec![("id".into(), Direction::Desc)]),
                start: Some(id_start(4.0, false)),
                limit: Some(2.0),
                ..Default::default()
            },
            // start + limit + order DESC + a `where` filter.
            Ast {
                table: "issue".into(),
                where_: Some(active_eq_1()),
                order_by: Some(vec![("id".into(), Direction::Asc)]),
                start: Some(id_start(1.0, false)),
                limit: Some(2.0),
                ..Default::default()
            },
        ]
    }

    #[test]
    fn graph_matches_materialize_for_bounded_ordered_queries() {
        let path = path();
        let writer = setup_issue_ordered(&path);
        let driver = ordered_driver(&path);

        for ast in &bounded_ordered_asts() {
            assert!(
                is_graph_eligible(ast),
                "bounded+ordered ast must now be graph-eligible: {ast:?}"
            );
            assert_oracle_eq(&driver, ast);
        }

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn graph_matches_materialize_for_bounded_ordered_after_mutation() {
        let path = path();
        let writer = setup_issue_ordered(&path);
        let mut driver = ordered_driver(&path);
        let asts = bounded_ordered_asts();

        // Baseline (before any mutation).
        for ast in &asts {
            assert_oracle_eq(&driver, ast);
        }

        // Flip issue 2 to active AND insert a new active row 6 — both shift which
        // rows fall inside a bounded+ordered window.
        writer
            .run("UPDATE issue SET active=1, _0_version='01' WHERE id=2", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "01",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(2.0))],
                None,
            )
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (6,1,'01')", &[])
            .unwrap();
        // Distinct `pos`: the changelog PK is `(stateVersion, pos)`, so two
        // changes at the same version must use different positions or the later
        // `INSERT OR REPLACE` clobbers the earlier one.
        ChangeLog::new(&writer)
            .log_set_op(
                "01",
                1,
                "issue",
                &vec![("id".into(), JsonValue::Number(6.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "01").unwrap();
        // `advance` moves the snapshot to head AND updates the in-memory source
        // the materialize oracle reads, so both paths see the same committed
        // state on the next comparison.
        driver.advance().unwrap();

        for ast in &asts {
            assert_oracle_eq(&driver, ast);
        }

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }
}
