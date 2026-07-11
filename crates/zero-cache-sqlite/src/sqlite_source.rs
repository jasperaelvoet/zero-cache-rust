//! `SqliteSource` — a replica-backed operator `Source`, port of the
//! `Input`/`push` half of `zqlite/src/table-source.ts`.
//!
//! Where [`crate::sqlite_table_source::SqliteTableSource`] is a borrowing,
//! read-only helper (`fetch`), `SqliteSource` is the real operator-graph
//! `Source`: it OWNS a swappable SQLite handle (`RefCell<StatementRunner>`,
//! matching upstream's `table.setDB` leapfrog on advance), implements
//! [`Input`] (so it can sit at the bottom of an operator pipeline and be
//! wired to downstream [`Output`]s via `set_output`), and fans a
//! [`SourceChange`] out to every connected output as an operator-level
//! [`Change`] (upstream `Source.push` / the driver's `#push`).
//!
//! Deliberate scope, per the query-pipeline-redesign spec (§3.2/§3.3):
//! - **Reads are committed-state only.** `push` does NOT write SQLite — the
//!   replicator owns writes; the source reads already-committed replica
//!   state. `push` only elaborates the change and fans it downstream.
//! - **Accumulating pending-change overlay (increments 5 + 6b).** Upstream sets
//!   the DBs of every source to the new snapshot only AFTER all pushes for a
//!   commit have flowed through the graph (`pipeline-driver.ts:1039-1046`). So a
//!   fetch issued *during* a push must see prev-snapshot state overlaid with the
//!   in-flight changes — otherwise a `Join` re-deriving a parent's children
//!   mid-child-push would miss a just-added (or still-see a just-removed) row.
//!
//!   Increment 5 held a SINGLE in-flight change and cleared it after the
//!   fan-out, which was exact only for commits touching one row per table. But
//!   upstream's zqlite `TableSource.push` WRITES each change into its
//!   prev-snapshot connection (`table-source.ts:#writeChange`), so several
//!   changes to the same table in one commit ACCUMULATE and later same-commit
//!   fetches see every prior one. Increment 6b ports that: [`Self::overlay`] is
//!   a `Vec<SourceChange>` that [`Self::push`] APPENDS to (then fans the
//!   operator change out, WITHOUT clearing). While the overlay is non-empty,
//!   [`Input::fetch`] splices ALL accumulated changes onto the committed rows
//!   (ports `generateWithOverlay`/`generateWithOverlayInner`,
//!   `memory-source.ts:720-901`, into [`apply_overlay`]): every add injected at
//!   its sorted position, every removed row suppressed, an edit both — all
//!   filtered to the fetch's `start`/`constraint`/`multi_constraints`, applied
//!   in push order so an add-then-remove (or vice versa) of the same PK nets to
//!   what the committed head will show. The overlay is cleared only by
//!   [`Self::set_db`] (the driver's leapfrog to the new head, where these
//!   changes are now committed) or [`Self::clear_overlay`]. The committed
//!   replica is never written.
//! - The read path itself (constraint / multi-constraint / order / reverse /
//!   `start` cursor + declared value-type restoration) is reused verbatim
//!   from `SqliteTableSource` rather than duplicated.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use zero_cache_protocol::ast::Ordering;
use zero_cache_zql::ivm::change::SourceChange;
use zero_cache_zql::ivm::constraint::{constraint_matches_row, PrimaryKey};
use zero_cache_zql::ivm::data::{make_comparator, Row};
use zero_cache_zql::ivm::operator::{
    Change, FetchRequest, Input, InputBase, Node, Output, SourceSchema, Stream,
};

use crate::query_builder::ColumnType;
use crate::sqlite_table_source::SqliteTableSource;
use crate::StatementRunner;

/// A real, replica-backed operator source for one table. Port of
/// `zqlite::TableSource` (the `Input` + `push` + `setDB` surface).
pub struct SqliteSource {
    /// Swapped on advance via [`Self::set_db`] (upstream `table.setDB`).
    db: RefCell<StatementRunner>,
    schema: SourceSchema,
    columns: Vec<String>,
    column_types: BTreeMap<String, ColumnType>,
    /// Downstream consumers registered through [`Input::set_output`]; a single
    /// `push` fans to all of them.
    outputs: RefCell<Vec<Rc<dyn Output>>>,
    /// The commit's in-flight changes, ACCUMULATED across every [`Self::push`]
    /// since the last [`Self::set_db`]/[`Self::clear_overlay`], overlaid onto
    /// every [`Input::fetch`] so a mid-push fetch sees prev-snapshot + all
    /// pending changes so far (upstream writes each into the prev connection —
    /// see the module doc). Empty between commits.
    overlay: RefCell<Vec<SourceChange>>,
}

impl SqliteSource {
    /// Constructs a source with generic (all-`String`, optional) column types
    /// — mirrors [`SqliteTableSource::new`]. Callers that know real per-column
    /// types/optionality should use [`Self::with_column_types`].
    pub fn new(
        db: StatementRunner,
        table_name: impl Into<String>,
        primary_key: PrimaryKey,
        sort: Ordering,
        columns: Vec<String>,
    ) -> Self {
        let column_types = generic_column_types(&columns);
        Self::with_column_types(db, table_name, primary_key, sort, columns, column_types)
    }

    /// Constructs a source with explicit per-column types/optionality —
    /// mirrors [`SqliteTableSource::with_column_types`]. Needed for
    /// type-directed value serialization (booleans coerced to 0/1, JSON
    /// columns stringified) and nullable-aware cursor comparisons.
    pub fn with_column_types(
        db: StatementRunner,
        table_name: impl Into<String>,
        primary_key: PrimaryKey,
        sort: Ordering,
        columns: Vec<String>,
        column_types: BTreeMap<String, ColumnType>,
    ) -> Self {
        SqliteSource {
            db: RefCell::new(db),
            schema: SourceSchema {
                table_name: table_name.into(),
                primary_key,
                sort,
                relationships: BTreeMap::new(),
            },
            columns,
            column_types,
            outputs: RefCell::new(Vec::new()),
            overlay: RefCell::new(Vec::new()),
        }
    }

    /// The schema of the rows this source returns.
    pub fn schema(&self) -> &SourceSchema {
        &self.schema
    }

    /// Swaps the backing SQLite handle and clears the accumulated overlay. Port
    /// of upstream `table.setDB`, called by the driver on every source AFTER all
    /// of a commit's pushes have flowed through the graph so subsequent
    /// `fetch`es see head state (`pipeline-driver.ts:1044`). The just-swapped
    /// head already reflects the overlay's changes, so they are dropped here.
    pub fn set_db(&self, db: StatementRunner) {
        *self.db.borrow_mut() = db;
        self.overlay.borrow_mut().clear();
        // A `BEGIN CONCURRENT` connection defers its read snapshot to the first
        // read. Establish it NOW so the source freezes at the CURRENT head (the
        // intended "previous" state for the next commit); otherwise the first
        // read would happen during the NEXT commit's push, after the writer has
        // already advanced head past prev — so a later same-table change would
        // be seen twice (committed head + overlay).
        self.establish_snapshot();
    }

    /// Touches the backing table so the `BEGIN CONCURRENT` read snapshot is
    /// established immediately (SQLite defers it to the first read).
    fn establish_snapshot(&self) {
        let table = self.schema.table_name.replace('"', "\"\"");
        let _ = self
            .db
            .borrow()
            .get(&format!("SELECT 1 FROM \"{table}\" LIMIT 1"), &[]);
    }

    /// Discards the accumulated overlay without swapping the DB — the
    /// commit-boundary reset [`Self::set_db`] also performs, exposed for callers
    /// (and tests) that need to end the in-flight commit without a new snapshot.
    pub fn clear_overlay(&self) {
        self.overlay.borrow_mut().clear();
    }

    /// Elaborates a row-level [`SourceChange`] into an operator-level
    /// [`Change`] and fans it out to every connected [`Output`]. Port of
    /// `Source.push` / the driver's `#push` (`pipeline-driver.ts:1006`).
    ///
    /// Does NOT write SQLite — the replicator owns writes; this source only
    /// reads committed state (see the module doc). Add/Remove/Edit map exactly
    /// as the in-memory `TableSource::push` maps them; row identity by primary
    /// key is the writer's responsibility, so no membership assertion is made
    /// here (unlike the authoritative in-memory `TableSource`).
    pub fn push(&self, change: SourceChange) {
        // Append this change to the accumulating overlay BEFORE fanning out so
        // any fetch a downstream issues while processing this push (e.g. a Join
        // re-deriving a parent's children) sees prev-snapshot + every change of
        // this commit so far, INCLUDING this one. Upstream writes each change
        // into its prev-snapshot connection before pushing it
        // (`table-source.ts:#writeChange`), so the accumulation persists across
        // the whole commit and is cleared only by `set_db`/`clear_overlay`.
        self.overlay.borrow_mut().push(change.clone());
        let op_change = source_change_to_change(change);
        // Clone the output handles out before dispatching so a downstream that
        // re-enters (e.g. registers another output) can't invalidate the
        // borrow mid-iteration — matches upstream cloning `#output`s.
        let outputs: Vec<Rc<dyn Output>> = self.outputs.borrow().clone();
        for output in outputs {
            output.push(op_change.clone(), self);
        }
    }

    /// Removes a single registered output by `Rc` identity — port of the
    /// per-connection teardown a `SourceInput.destroy` performs upstream
    /// (`memory-source.ts:205`, splicing just that connection out). Unlike
    /// [`InputBase::destroy`], which clears ALL outputs, this lets one query's
    /// push edge be dropped from a source SHARED by sibling queries without
    /// severing the others'.
    pub fn remove_output(&self, output: &Rc<dyn Output>) {
        self.outputs.borrow_mut().retain(|o| !Rc::ptr_eq(o, output));
    }
}

/// Looks up a column's value in `row` (`None` if absent).
fn row_get<'a>(row: &'a Row, col: &str) -> Option<&'a zero_cache_zql::ivm::data::Value> {
    row.iter().find(|(k, _)| k == col).map(|(_, v)| v)
}

/// Splices ALL accumulated pending `overlay` changes into the already-committed,
/// sorted, constraint-filtered `nodes` a fetch produced. Port of
/// `generateWithOverlay` + `generateWithOverlayInner` (`memory-source.ts`),
/// generalized to a whole commit's worth of changes (increment 6b): each change
/// is filtered to the fetch's `constraint`/`multi_constraints`/`start`
/// (upstream `computeOverlays`), then applied IN PUSH ORDER to a running,
/// sorted working set — every add injected at its sorted position (replacing an
/// existing same-identity row), every removed row suppressed by identity. Row
/// identity is the primary key (upstream writes by PK), falling back to full
/// comparator equality when no primary key is declared. Applying in order makes
/// an add-then-remove (or remove-then-add) of the same PK net to exactly what
/// the committed head will show.
fn apply_overlay(
    nodes: Vec<Node>,
    overlay: &[SourceChange],
    req: &FetchRequest,
    sort: &Ordering,
    primary_key: &[String],
) -> Vec<Node> {
    use std::cmp::Ordering as Ord;

    if overlay.is_empty() {
        return nodes;
    }

    let compare = make_comparator(sort, req.reverse);
    // Row identity for suppression/replacement: the primary key (matching
    // upstream's write-by-PK), falling back to comparator equality.
    let same = |a: &Row, b: &Row| -> bool {
        if primary_key.is_empty() {
            compare(a, b) == Ord::Equal
        } else {
            primary_key.iter().all(|k| row_get(a, k) == row_get(b, k))
        }
    };

    // The running working set, kept in the fetch's sorted order.
    let mut work: Vec<Row> = nodes.into_iter().map(|n| n.row).collect();

    for change in overlay {
        // The row this change adds and the row it removes (an edit does both),
        // per `computeOverlays`'s ADD/REMOVE/EDIT cases.
        let (mut add, mut remove): (Option<Row>, Option<Row>) = match change {
            SourceChange::Add(row) => (Some(row.clone()), None),
            SourceChange::Remove(row) => (None, Some(row.clone())),
            SourceChange::Edit { row, old_row } => (Some(row.clone()), Some(old_row.clone())),
        };

        // Drop an overlay row that lies before the fetch's `start`
        // (`overlaysForStartAt`).
        if let Some(start) = &req.start {
            let before_start = |row: &Row| compare(row, &start.row) == Ord::Less;
            if add.as_ref().is_some_and(before_start) {
                add = None;
            }
            if remove.as_ref().is_some_and(before_start) {
                remove = None;
            }
        }

        // Drop an overlay row that does not match the fetch constraint
        // (`overlaysForConstraint`).
        if let Some(constraint) = &req.constraint {
            let matches = |row: &Row| constraint_matches_row(constraint, row);
            if add.as_ref().is_some_and(|r| !matches(r)) {
                add = None;
            }
            if remove.as_ref().is_some_and(|r| !matches(r)) {
                remove = None;
            }
        }

        // Drop an overlay row that does not match every non-empty
        // multi-constraint batch (`applyMultiConstraintsToOverlays`).
        for batch in &req.multi_constraints {
            if batch.is_empty() {
                continue;
            }
            let matches_any = |row: &Row| batch.iter().any(|c| constraint_matches_row(c, row));
            if add.as_ref().is_some_and(|r| !matches_any(r)) {
                add = None;
            }
            if remove.as_ref().is_some_and(|r| !matches_any(r)) {
                remove = None;
            }
        }

        // Suppress the removed row (if present) before injecting the add, so an
        // edit that both removes and re-adds the same identity lands once.
        if let Some(r) = &remove {
            if let Some(pos) = work.iter().position(|w| same(w, r)) {
                work.remove(pos);
            }
        }
        if let Some(a) = add {
            // Replace any existing same-identity row, then insert at the sorted
            // position (upstream's write-then-read from the prev connection).
            if let Some(pos) = work.iter().position(|w| same(w, &a)) {
                work.remove(pos);
            }
            let at = work.partition_point(|w| compare(w, &a) == Ord::Less);
            work.insert(at, a);
        }
    }

    work.into_iter().map(Node::new).collect()
}

/// Elaborates a `SourceChange` into an operator `Change` — the same mapping
/// [`zero_cache_zql::ivm::table_source::TableSource::push`] produces.
fn source_change_to_change(change: SourceChange) -> Change {
    match change {
        SourceChange::Add(row) => Change::Add(Node::new(row)),
        SourceChange::Remove(row) => Change::Remove(Node::new(row)),
        SourceChange::Edit { row, old_row } => Change::Edit {
            node: Node::new(row),
            old_node: Node::new(old_row),
        },
    }
}

/// Same generic column-type map as [`SqliteTableSource::new`] builds: every
/// column typed `String`/optional.
fn generic_column_types(columns: &[String]) -> BTreeMap<String, ColumnType> {
    columns
        .iter()
        .map(|c| {
            (
                c.clone(),
                ColumnType {
                    value_type: zero_cache_protocol::client_schema::ValueType::String,
                    optional: true,
                },
            )
        })
        .collect()
}

impl InputBase for SqliteSource {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&self) {
        // A source has no upstream to cascade to; drop registered outputs so a
        // rebuilt pipeline doesn't fan to stale consumers, and discard any
        // in-flight overlay.
        self.outputs.borrow_mut().clear();
        self.overlay.borrow_mut().clear();
    }
}

impl Input for SqliteSource {
    fn set_output(&self, output: Rc<dyn Output>) {
        self.outputs.borrow_mut().push(output);
    }

    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node> {
        // Reuse the exact SQL-pushdown read path from `SqliteTableSource`.
        // `Input::fetch` returns no `Result`, so on a DB error we log and
        // yield an empty stream rather than panic (spec §3.2).
        let db = self.db.borrow();
        let reader = SqliteTableSource::with_column_types(
            &db,
            self.schema.table_name.clone(),
            self.schema.primary_key.clone(),
            self.schema.sort.clone(),
            self.columns.clone(),
            self.column_types.clone(),
        );
        match reader.fetch(req) {
            Ok(nodes) => {
                // Splice in every accumulated in-flight change of the current
                // commit (upstream applies `#overlay` inside `#fetch`).
                let overlay = self.overlay.borrow();
                let out = if overlay.is_empty() {
                    nodes
                } else {
                    apply_overlay(
                        nodes,
                        overlay.as_slice(),
                        req,
                        &self.schema.sort,
                        &self.schema.primary_key,
                    )
                };
                drop(overlay);
                Box::new(out.into_iter())
            }
            Err(e) => {
                eprintln!(
                    "SqliteSource::fetch on table `{}` failed: {e}",
                    self.schema.table_name
                );
                Box::new(std::iter::empty())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell as StdRefCell;
    use std::rc::Weak;
    use zero_cache_protocol::ast::Direction;
    use zero_cache_zql::ivm::change::{
        make_source_change_add, make_source_change_edit, make_source_change_remove,
    };
    use zero_cache_zql::ivm::data::Value;

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT, active INTEGER)")
            .unwrap();
        db.exec("INSERT INTO issues (id, title, active) VALUES ('1', 'a', 1), ('2', 'b', 0), ('3', 'c', 1)").unwrap();
        db
    }

    fn source(db: StatementRunner) -> SqliteSource {
        SqliteSource::new(
            db,
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
            vec!["id".into(), "title".into(), "active".into()],
        )
    }

    fn ids(nodes: &[Node]) -> Vec<String> {
        nodes
            .iter()
            .map(|n| match &n.row[0].1 {
                Value::String(s) => s.clone(),
                other => panic!("expected string id, got {other:?}"),
            })
            .collect()
    }

    /// Records pushes it receives, matching upstream `table-source.test.ts`'s
    /// `SpyOutput`/`Catch`.
    struct SpyOutput {
        changes: StdRefCell<Vec<Change>>,
    }

    impl SpyOutput {
        fn new() -> Rc<Self> {
            Rc::new(SpyOutput {
                changes: StdRefCell::new(Vec::new()),
            })
        }
    }

    impl Output for SpyOutput {
        fn push(&self, change: Change, _pusher: &dyn InputBase) {
            self.changes.borrow_mut().push(change);
        }
    }

    #[test]
    fn fetch_matches_the_committed_replica_fixture() {
        let s = source(setup());
        let nodes: Vec<Node> = s.fetch(&FetchRequest::default()).collect();
        assert_eq!(ids(&nodes), vec!["1", "2", "3"]);
    }

    #[test]
    fn fetch_applies_constraint_via_sql_pushdown() {
        let s = source(setup());
        let req = FetchRequest {
            constraint: Some(vec![("active".into(), Value::Number(1.0))]),
            ..Default::default()
        };
        let nodes: Vec<Node> = s.fetch(&req).collect();
        assert_eq!(ids(&nodes), vec!["1", "3"]);
    }

    #[test]
    fn fetch_after_set_db_sees_the_new_handle() {
        let s = source(setup());
        assert_eq!(s.fetch(&FetchRequest::default()).count(), 3);

        // Swap in a fresh replica with different committed state.
        let db2 = StatementRunner::open_in_memory().unwrap();
        db2.exec("CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT, active INTEGER)")
            .unwrap();
        db2.exec("INSERT INTO issues (id, title, active) VALUES ('9', 'z', 1)")
            .unwrap();
        s.set_db(db2);

        let nodes: Vec<Node> = s.fetch(&FetchRequest::default()).collect();
        assert_eq!(ids(&nodes), vec!["9"]);
    }

    #[test]
    fn push_fans_add_remove_edit_to_connected_outputs() {
        let s = source(setup());
        let spy = SpyOutput::new();
        s.set_output(spy.clone());

        s.push(make_source_change_add(vec![(
            "id".into(),
            Value::String("4".into()),
        )]));
        s.push(make_source_change_remove(vec![(
            "id".into(),
            Value::String("2".into()),
        )]));
        s.push(make_source_change_edit(
            vec![
                ("id".into(), Value::String("1".into())),
                ("title".into(), Value::String("A".into())),
            ],
            vec![
                ("id".into(), Value::String("1".into())),
                ("title".into(), Value::String("a".into())),
            ],
        ));

        let changes = spy.changes.borrow();
        assert_eq!(changes.len(), 3);
        assert_eq!(
            changes[0],
            Change::Add(Node::new(vec![("id".into(), Value::String("4".into()))]))
        );
        assert_eq!(
            changes[1],
            Change::Remove(Node::new(vec![("id".into(), Value::String("2".into()))]))
        );
        assert!(matches!(changes[2], Change::Edit { .. }));
    }

    #[test]
    fn push_fans_to_every_connected_output() {
        let s = source(setup());
        let a = SpyOutput::new();
        let b = SpyOutput::new();
        s.set_output(a.clone());
        s.set_output(b.clone());

        s.push(make_source_change_add(vec![(
            "id".into(),
            Value::String("4".into()),
        )]));

        assert_eq!(a.changes.borrow().len(), 1);
        assert_eq!(b.changes.borrow().len(), 1);
    }

    #[test]
    fn push_does_not_write_sqlite() {
        // The replicator owns writes; push must not mutate the replica.
        let s = source(setup());
        let spy = SpyOutput::new();
        s.set_output(spy);
        s.push(make_source_change_add(vec![(
            "id".into(),
            Value::String("4".into()),
        )]));
        // End the in-flight commit; the committed replica still has exactly the
        // three seeded rows (push never wrote SQLite).
        s.clear_overlay();
        assert_eq!(s.fetch(&FetchRequest::default()).count(), 3);
    }

    #[test]
    fn destroy_drops_outputs() {
        let s = source(setup());
        let spy = SpyOutput::new();
        s.set_output(spy.clone());
        s.destroy();
        s.push(make_source_change_add(vec![(
            "id".into(),
            Value::String("4".into()),
        )]));
        assert_eq!(spy.changes.borrow().len(), 0, "no output after destroy");
    }

    // ---- remove_output (increment 5) ----

    #[test]
    fn remove_output_removes_only_the_named_output_by_identity() {
        // A source SHARED by three sibling queries: removing one query's edge
        // must leave the others' intact (unlike `destroy`, which clears all).
        let s = source(setup());
        let a = SpyOutput::new();
        let b = SpyOutput::new();
        let c = SpyOutput::new();
        s.set_output(a.clone());
        s.set_output(b.clone());
        s.set_output(c.clone());

        let b_erased: Rc<dyn Output> = b.clone();
        s.remove_output(&b_erased);

        s.push(make_source_change_add(vec![(
            "id".into(),
            Value::String("4".into()),
        )]));

        assert_eq!(a.changes.borrow().len(), 1, "A still wired");
        assert_eq!(b.changes.borrow().len(), 0, "B removed by identity");
        assert_eq!(c.changes.borrow().len(), 1, "C (sibling) untouched");
    }

    // ---- pending-change overlay (increment 5) ----

    /// An `Output` that re-fetches its source when pushed, recording the ids it
    /// sees — so a test can observe the overlay a mid-push fetch is subject to.
    struct FetchingSpy {
        source: Weak<SqliteSource>,
        req: FetchRequest,
        seen: StdRefCell<Vec<Vec<String>>>,
    }
    impl FetchingSpy {
        fn new(source: &Rc<SqliteSource>, req: FetchRequest) -> Rc<Self> {
            Rc::new(FetchingSpy {
                source: Rc::downgrade(source),
                req,
                seen: StdRefCell::new(Vec::new()),
            })
        }
    }
    impl Output for FetchingSpy {
        fn push(&self, _change: Change, _pusher: &dyn InputBase) {
            if let Some(src) = self.source.upgrade() {
                let nodes: Vec<Node> = src.fetch(&self.req).collect();
                self.seen.borrow_mut().push(ids(&nodes));
            }
        }
    }

    fn rc_source() -> Rc<SqliteSource> {
        Rc::new(source(setup()))
    }

    #[test]
    fn overlay_add_is_visible_to_a_mid_push_fetch_then_gone() {
        let s = rc_source();
        let spy = FetchingSpy::new(&s, FetchRequest::default());
        s.set_output(spy.clone() as Rc<dyn Output>);

        // id '4' sorts after the seeded 1,2,3 -> injected at the end.
        s.push(make_source_change_add(vec![(
            "id".into(),
            Value::String("4".into()),
        )]));

        assert_eq!(
            *spy.seen.borrow(),
            vec![vec![
                "1".to_string(),
                "2".to_string(),
                "3".to_string(),
                "4".to_string()
            ]],
            "the mid-push fetch saw the overlaid add"
        );
        // Ending the commit clears the overlay; nothing was written, so the
        // committed state is unchanged.
        s.clear_overlay();
        let after: Vec<Node> = s.fetch(&FetchRequest::default()).collect();
        assert_eq!(ids(&after), vec!["1", "2", "3"]);
    }

    #[test]
    fn overlay_remove_is_suppressed_in_a_mid_push_fetch() {
        let s = rc_source();
        let spy = FetchingSpy::new(&s, FetchRequest::default());
        s.set_output(spy.clone() as Rc<dyn Output>);

        s.push(make_source_change_remove(vec![(
            "id".into(),
            Value::String("2".into()),
        )]));

        assert_eq!(
            *spy.seen.borrow(),
            vec![vec!["1".to_string(), "3".to_string()]],
            "the removed row was suppressed mid-push"
        );
        s.clear_overlay();
        let after: Vec<Node> = s.fetch(&FetchRequest::default()).collect();
        assert_eq!(ids(&after), vec!["1", "2", "3"], "remove not committed");
    }

    #[test]
    fn overlay_add_is_filtered_by_the_fetch_constraint() {
        let s = rc_source();
        // Fetch only inactive rows (active = 0): seeded that is just id '2'.
        let inactive_req = FetchRequest {
            constraint: Some(vec![("active".into(), Value::Number(0.0))]),
            ..Default::default()
        };
        let spy = FetchingSpy::new(&s, inactive_req);
        s.set_output(spy.clone() as Rc<dyn Output>);

        // Add an ACTIVE row; the inactive-only fetch must not see it.
        s.push(make_source_change_add(vec![
            ("id".into(), Value::String("4".into())),
            ("title".into(), Value::String("d".into())),
            ("active".into(), Value::Number(1.0)),
        ]));

        assert_eq!(
            *spy.seen.borrow(),
            vec![vec!["2".to_string()]],
            "the active add did not match the inactive-only constraint"
        );
    }

    // ---- accumulating overlay across a multi-change commit (increment 6b) ----

    #[test]
    fn overlay_accumulates_multiple_changes_across_pushes() {
        let s = rc_source();
        let spy = FetchingSpy::new(&s, FetchRequest::default());
        s.set_output(spy.clone() as Rc<dyn Output>);

        // Two changes in ONE commit: remove '2' then add '4'. With increment 5's
        // single-slot overlay the second push's fetch would still see '2' (its
        // remove was cleared after the first push); accumulation keeps BOTH the
        // remove and the add applied for the second push's fetch.
        s.push(make_source_change_remove(vec![(
            "id".into(),
            Value::String("2".into()),
        )]));
        s.push(make_source_change_add(vec![(
            "id".into(),
            Value::String("4".into()),
        )]));

        assert_eq!(
            *spy.seen.borrow(),
            vec![
                vec!["1".to_string(), "3".to_string()],
                vec!["1".to_string(), "3".to_string(), "4".to_string()],
            ],
            "the second push's fetch saw the accumulated remove AND add"
        );

        // Ending the commit clears the overlay; nothing was written.
        s.clear_overlay();
        let after: Vec<Node> = s.fetch(&FetchRequest::default()).collect();
        assert_eq!(ids(&after), vec!["1", "2", "3"]);
    }

    #[test]
    fn overlay_add_then_remove_same_pk_nets_to_committed() {
        let s = rc_source();
        let spy = FetchingSpy::new(&s, FetchRequest::default());
        s.set_output(spy.clone() as Rc<dyn Output>);

        // Add '4' then remove '4' within one commit: the net must be the
        // committed rows, exactly what the new head will show.
        s.push(make_source_change_add(vec![(
            "id".into(),
            Value::String("4".into()),
        )]));
        s.push(make_source_change_remove(vec![(
            "id".into(),
            Value::String("4".into()),
        )]));

        assert_eq!(
            *spy.seen.borrow(),
            vec![
                vec![
                    "1".to_string(),
                    "2".to_string(),
                    "3".to_string(),
                    "4".to_string()
                ],
                vec!["1".to_string(), "2".to_string(), "3".to_string()],
            ],
            "add then remove of the same pk nets to the committed rows"
        );
    }

    // ---- Join over shared SqliteSources with the overlay (increment 5) ----

    fn comment_setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE comments (id TEXT PRIMARY KEY, issueID TEXT)")
            .unwrap();
        db
    }

    fn issue_only_setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT, active INTEGER)")
            .unwrap();
        db.exec("INSERT INTO issues (id, title, active) VALUES ('i1', 'a', 1)")
            .unwrap();
        db
    }

    fn comment_source(db: StatementRunner) -> Rc<SqliteSource> {
        Rc::new(SqliteSource::new(
            db,
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
            vec!["id".into(), "issueID".into()],
        ))
    }

    fn issue_source_rc(db: StatementRunner) -> Rc<SqliteSource> {
        Rc::new(source(db))
    }

    fn str_val(row: &Node, col: &str) -> String {
        match &row.row.iter().find(|(k, _)| k == col).unwrap().1 {
            Value::String(s) => s.clone(),
            other => panic!("expected string, got {other:?}"),
        }
    }

    /// A child add pushed through the (never-written) comment `SqliteSource`
    /// makes the join re-derive its parent's relationship OFF THE OVERLAY: the
    /// emitted `Change::Child` carries the parent with the just-added comment,
    /// even though the comment table stays empty.
    #[test]
    fn join_child_push_sees_overlay_on_the_shared_child_source() {
        use zero_cache_zql::ivm::join_input::JoinInput;

        let issues = issue_source_rc(issue_only_setup());
        let comments = comment_source(comment_setup());

        let join = JoinInput::new(
            issues.clone() as Rc<dyn Input>,
            comments.clone() as Rc<dyn Input>,
            vec!["id".into()],
            vec!["issueID".into()],
            "comments",
        );
        let spy = SpyOutput::new();
        join.set_output(spy.clone());

        // Push a comment for i1 through the comment source. The source does NOT
        // write SQLite, but installs the overlay while fanning the change to the
        // join's child edge -> the join's re-fetch of i1's comments sees c1.
        comments.push(make_source_change_add(vec![
            ("id".into(), Value::String("c1".into())),
            ("issueID".into(), Value::String("i1".into())),
        ]));

        let changes = spy.changes.borrow();
        assert_eq!(changes.len(), 1);
        let Change::Child { node, child } = &changes[0] else {
            panic!("expected Change::Child, got {:?}", changes[0]);
        };
        assert_eq!(str_val(node, "id"), "i1");
        assert_eq!(child.relationship_name, "comments");
        // The parent's relationship reflects the overlaid (uncommitted) comment.
        let rel = &node.relationships["comments"];
        assert_eq!(rel.len(), 1, "overlay surfaced the pending comment");
        assert_eq!(str_val(&rel[0], "id"), "c1");
        drop(changes);

        // The comment table was never written; ending the commit clears the
        // overlay and a fresh fetch is still empty.
        comments.clear_overlay();
        assert_eq!(comments.fetch(&FetchRequest::default()).count(), 0);
    }

    /// Two sibling joins share ONE parent `issue` source. A parent push fans to
    /// both (each re-derives its own relationship) — the shared-source push
    /// edges coexist, the case `remove_output` (not `destroy`) protects.
    #[test]
    fn sibling_joins_sharing_a_parent_source_both_receive_a_parent_push() {
        use zero_cache_zql::ivm::join_input::JoinInput;

        // Shared issue source starts EMPTY so the parent add is a fresh push.
        let issue_db = StatementRunner::open_in_memory().unwrap();
        issue_db
            .exec("CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT, active INTEGER)")
            .unwrap();
        let issues = issue_source_rc(issue_db);

        // Two children: comments (c1 -> i1) and a second comments-shaped table.
        let comments_a_db = comment_setup();
        comments_a_db
            .exec("INSERT INTO comments (id, issueID) VALUES ('c1', 'i1')")
            .unwrap();
        let comments_a = comment_source(comments_a_db);

        let comments_b_db = comment_setup();
        comments_b_db
            .exec("INSERT INTO comments (id, issueID) VALUES ('c2', 'i1')")
            .unwrap();
        let comments_b = comment_source(comments_b_db);

        let join_a = JoinInput::new(
            issues.clone() as Rc<dyn Input>,
            comments_a.clone() as Rc<dyn Input>,
            vec!["id".into()],
            vec!["issueID".into()],
            "comments",
        );
        let join_b = JoinInput::new(
            issues.clone() as Rc<dyn Input>,
            comments_b.clone() as Rc<dyn Input>,
            vec!["id".into()],
            vec!["issueID".into()],
            "comments",
        );
        let spy_a = SpyOutput::new();
        let spy_b = SpyOutput::new();
        join_a.set_output(spy_a.clone());
        join_b.set_output(spy_b.clone());

        issues.push(make_source_change_add(vec![
            ("id".into(), Value::String("i1".into())),
            ("title".into(), Value::String("t".into())),
            ("active".into(), Value::Number(1.0)),
        ]));

        // Both sibling joins re-derived their own relationship for the new parent.
        let a = spy_a.changes.borrow();
        let b = spy_b.changes.borrow();
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
        let Change::Add(node_a) = &a[0] else {
            panic!("expected Add");
        };
        let Change::Add(node_b) = &b[0] else {
            panic!("expected Add");
        };
        assert_eq!(str_val(&node_a.relationships["comments"][0], "id"), "c1");
        assert_eq!(str_val(&node_b.relationships["comments"][0], "id"), "c2");
    }
}
