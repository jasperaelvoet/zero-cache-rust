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

use zero_cache_protocol::ast::Ast;

use crate::builder::filter::create_predicate;
use crate::ivm::filter::GraphFilter;
use crate::ivm::operator::Input;

/// The build-time environment `build_pipeline` consults, mirroring upstream's
/// `BuilderDelegate` (`builder.ts`). Only `get_source` is needed this
/// increment; `create_storage` (for `Take`/`Exists` state) is deferred until
/// an operator that needs [`crate::ivm::operator::Storage`] is ported.
pub struct BuildDelegate<'d> {
    /// Returns the (memoized, per-table) source for `table`, already erased to
    /// `Rc<dyn Input>` so this crate stays source-agnostic. Port of upstream
    /// `#getSource` (`pipeline-driver.ts:1054`).
    pub get_source: &'d dyn Fn(&str) -> Rc<dyn Input>,
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
    let source = (delegate.get_source)(&ast.table);
    match &ast.where_ {
        Some(condition) => {
            let predicate = create_predicate(condition);
            GraphFilter::new(source, predicate) as Rc<dyn Input>
        }
        None => source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::data::Row;
    use crate::ivm::operator::{FetchRequest, InputBase, Node, Output, SourceSchema, Stream};
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::{
        ColumnReference, Condition, Direction, LiteralValue, SimpleOperator, ValuePosition,
    };
    use zero_cache_shared::bigint_json::JsonValue;

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
        let delegate = BuildDelegate {
            get_source: &get_source,
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
        let delegate = BuildDelegate {
            get_source: &get_source,
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
}
