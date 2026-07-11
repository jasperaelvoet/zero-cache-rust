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
//! - **No overlay (yet).** Upstream's `generateWithOverlay` interleaves a
//!   mid-transaction pending push with the committed stream so a `Join`
//!   fetch issued mid-push sees the not-yet-committed row. Deferred until a
//!   ported push test actually needs it (§3.3, phase 2).
//! - The read path itself (constraint / multi-constraint / order / reverse /
//!   `start` cursor + declared value-type restoration) is reused verbatim
//!   from `SqliteTableSource` rather than duplicated.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use zero_cache_protocol::ast::Ordering;
use zero_cache_zql::ivm::change::SourceChange;
use zero_cache_zql::ivm::constraint::PrimaryKey;
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
        }
    }

    /// The schema of the rows this source returns.
    pub fn schema(&self) -> &SourceSchema {
        &self.schema
    }

    /// Swaps the backing SQLite handle. Port of upstream `table.setDB`, called
    /// by the driver on every source after an advance so subsequent `fetch`es
    /// see head state (`pipeline-driver.ts:1044`).
    pub fn set_db(&self, db: StatementRunner) {
        *self.db.borrow_mut() = db;
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
        let change = source_change_to_change(change);
        // Clone the output handles out before dispatching so a downstream that
        // re-enters (e.g. registers another output) can't invalidate the
        // borrow mid-iteration — matches upstream cloning `#output`s.
        let outputs: Vec<Rc<dyn Output>> = self.outputs.borrow().clone();
        for output in outputs {
            output.push(change.clone(), self);
        }
    }
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
        // A source has no upstream to cascade to; just drop registered
        // outputs so a rebuilt pipeline doesn't fan to stale consumers.
        self.outputs.borrow_mut().clear();
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
            Ok(nodes) => Box::new(nodes.into_iter()),
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
        // The committed replica still has exactly the three seeded rows.
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
}
