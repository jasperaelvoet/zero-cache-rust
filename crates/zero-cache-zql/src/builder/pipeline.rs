//! Port of `zql/src/builder/builder.ts`'s `buildPipeline`, restricted to the
//! single-table + optional `where_` `Filter` slice (redesign §5.1,
//! increment 3). Join / limit / start / exists wiring lands in later
//! increments; this build only instantiates a source and, when the query has
//! a `WHERE`, wraps it in a [`GraphFilter`].
//!
//! **Source-agnostic by construction.** `zero-cache-zql` sits *below*
//! `zero-cache-sqlite` in the crate graph, so it cannot name `SqliteSource`.
//! The delegate therefore hands back sources as `Rc<dyn Input>` — the driver
//! (which depends on both crates) is what actually constructs the concrete
//! replica-backed `SqliteSource` and erases it to `dyn Input` behind
//! [`BuildDelegate::get_source`].

use std::rc::Rc;

use zero_cache_protocol::ast::{Ast, Bound as AstBound};

use crate::builder::filter::create_predicate;
use crate::ivm::data::Row;
use crate::ivm::filter::GraphFilter;
use crate::ivm::operator::{Input, Storage};
use crate::ivm::skip::{Bound as SkipBound, Skip};
use crate::ivm::take::Take;
use zero_cache_shared::bigint_json::JsonValue;

/// The build-time environment `build_pipeline` consults, mirroring upstream's
/// `BuilderDelegate` (`builder.ts`).
pub struct BuildDelegate<'d> {
    /// Returns the (memoized, per-table) source for `table`, already erased to
    /// `Rc<dyn Input>` so this crate stays source-agnostic. Port of upstream
    /// `#getSource` (`pipeline-driver.ts:1054`).
    pub get_source: &'d dyn Fn(&str) -> Rc<dyn Input>,
    /// Returns a fresh per-operator [`Storage`] namespaced by `name`. Port of
    /// upstream `createStorage` (`builder.ts`) — needed once `Take` (and later
    /// `Exists`) maintain durable state.
    pub create_storage: &'d dyn Fn(&str) -> Rc<dyn Storage>,
}

/// Converts an AST [`AstBound`] (whose `row` is a JSON object) into the ivm
/// [`SkipBound`] a [`Skip`] expects.
fn ast_bound_to_skip_bound(bound: &AstBound) -> SkipBound {
    let row: Row = match &bound.row {
        JsonValue::Object(entries) => entries.clone(),
        _ => Vec::new(),
    };
    SkipBound {
        row,
        exclusive: bound.exclusive,
    }
}

/// Builds an operator pipeline for `ast`, returning its root [`Input`]. This
/// increment covers only a single table with an optional `where_` `Filter`;
/// the returned root's `set_output` must still be pointed at a downstream
/// (e.g. the driver's `Collector`) before it is pushed to.
///
/// Panics (via [`create_predicate`]) if `ast.where_` contains a
/// `CorrelatedSubquery` — callers must gate the graph path to queries without
/// one until `Exists` is ported (the driver does this via its eligibility
/// check).
pub fn build_pipeline(ast: &Ast, delegate: &BuildDelegate) -> Rc<dyn Input> {
    // Operator order mirrors upstream `buildPipelineInternal` (`builder.ts`):
    // source → skip (`start`) → where filter → take (`limit`).
    let mut end: Rc<dyn Input> = (delegate.get_source)(&ast.table);

    if let Some(bound) = &ast.start {
        end = Skip::new(end, ast_bound_to_skip_bound(bound)) as Rc<dyn Input>;
    }

    if let Some(condition) = &ast.where_ {
        let predicate = create_predicate(condition);
        end = GraphFilter::new(end, predicate) as Rc<dyn Input>;
    }

    if let Some(limit) = ast.limit {
        let storage = (delegate.create_storage)("take");
        end = Take::new(end, storage, limit as usize, None) as Rc<dyn Input>;
    }

    end
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::make_source_change_add;
    use crate::ivm::data::Row;
    use crate::ivm::memory_storage::MemoryStorage;
    use crate::ivm::operator::{FetchRequest, InputBase, Node, Output, SourceSchema, Stream};
    use crate::ivm::test_input::TestSource;
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::{
        Bound, ColumnReference, Condition, Direction, LiteralValue, SimpleOperator, ValuePosition,
    };
    use zero_cache_shared::bigint_json::JsonValue;

    fn make_storage(_name: &str) -> Rc<dyn Storage> {
        Rc::new(MemoryStorage::default())
    }

    fn row(id: i64, active: bool) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("active".into(), JsonValue::Bool(active)),
        ]
    }

    /// In-memory `Input` returning a fixed row set — the source-agnostic test
    /// double the delegate hands back as `Rc<dyn Input>`.
    struct VecInput {
        rows: Vec<Row>,
    }
    impl VecInput {
        fn new(rows: Vec<Row>) -> Rc<Self> {
            Rc::new(VecInput { rows })
        }
    }
    impl InputBase for VecInput {
        fn get_schema(&self) -> SourceSchema {
            SourceSchema {
                table_name: "issue".into(),
                primary_key: vec!["id".into()],
                sort: vec![("id".into(), Direction::Asc)],
                relationships: BTreeMap::new(),
            }
        }
        fn destroy(&self) {}
    }
    impl Input for VecInput {
        fn set_output(&self, _output: Rc<dyn Output>) {}
        fn fetch<'a>(&'a self, _req: &FetchRequest) -> Stream<'a, Node> {
            Box::new(self.rows.iter().cloned().map(Node::new))
        }
    }

    fn where_active() -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: "active".into(),
            }),
            right: ValuePosition::Literal(LiteralValue::Bool(true)),
        }
    }

    #[test]
    fn builds_bare_source_when_no_where() {
        let input = VecInput::new(vec![row(1, true), row(2, false)]);
        let source_slot: RefCell<Option<Rc<dyn Input>>> = RefCell::new(Some(input.clone()));
        let get_source = |_t: &str| source_slot.borrow().clone().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let rows: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        assert_eq!(
            rows,
            vec![Node::new(row(1, true)), Node::new(row(2, false))]
        );
    }

    #[test]
    fn wraps_source_in_filter_when_where_present() {
        let input = VecInput::new(vec![row(1, true), row(2, false), row(3, true)]);
        let source_slot: RefCell<Option<Rc<dyn Input>>> = RefCell::new(Some(input.clone()));
        let get_source = |_t: &str| source_slot.borrow().clone().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            where_: Some(where_active()),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let rows: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        assert_eq!(rows, vec![Node::new(row(1, true)), Node::new(row(3, true))]);
    }

    fn issue_row(id: i64) -> Row {
        vec![("id".into(), JsonValue::Number(id as f64))]
    }

    fn seeded_source(ids: &[i64]) -> Rc<TestSource> {
        let source = TestSource::new(
            "issue",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        for id in ids {
            source.push_change(make_source_change_add(issue_row(*id)));
        }
        source
    }

    #[test]
    fn wraps_source_in_skip_when_start_present() {
        let source = seeded_source(&[1, 2, 3, 4]);
        let source_slot: RefCell<Option<Rc<dyn Input>>> =
            RefCell::new(Some(source.clone() as Rc<dyn Input>));
        let get_source = |_t: &str| source_slot.borrow().clone().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            start: Some(Bound {
                row: JsonValue::Object(vec![("id".into(), JsonValue::Number(2.0))]),
                exclusive: true,
            }),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let rows: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        // Exclusive start at id=2 drops ids 1 and 2.
        assert_eq!(rows, vec![Node::new(issue_row(3)), Node::new(issue_row(4))]);
    }

    #[test]
    fn wraps_source_in_take_when_limit_present() {
        let source = seeded_source(&[1, 2, 3, 4, 5]);
        let source_slot: RefCell<Option<Rc<dyn Input>>> =
            RefCell::new(Some(source.clone() as Rc<dyn Input>));
        let get_source = |_t: &str| source_slot.borrow().clone().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            limit: Some(2.0),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let rows: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        assert_eq!(rows, vec![Node::new(issue_row(1)), Node::new(issue_row(2))]);
    }

    #[test]
    fn combines_start_and_limit() {
        let source = seeded_source(&[1, 2, 3, 4, 5]);
        let source_slot: RefCell<Option<Rc<dyn Input>>> =
            RefCell::new(Some(source.clone() as Rc<dyn Input>));
        let get_source = |_t: &str| source_slot.borrow().clone().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            start: Some(Bound {
                row: JsonValue::Object(vec![("id".into(), JsonValue::Number(2.0))]),
                exclusive: true,
            }),
            limit: Some(2.0),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let rows: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        // Skip past id=2, then take 2 -> ids 3 and 4.
        assert_eq!(rows, vec![Node::new(issue_row(3)), Node::new(issue_row(4))]);
    }
}
