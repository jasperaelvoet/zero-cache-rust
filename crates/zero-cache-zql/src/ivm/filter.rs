//! Port of `zql/src/ivm/filter.ts` + the change-splitting core of
//! `zql/src/ivm/filter-push.ts`.
//!
//! Upstream's `Filter` is a stateless operator sitting between an `Input`
//! and an `Output`, participating in a separate `FilterOperator` sub-
//! protocol (`beginFilter`/`endFilter`/`filter(node)`) that composes with
//! `fan-out`/`fan-in` for multi-condition queries. This v1 port keeps only
//! the single-predicate behavior needed for one WHERE clause on one table —
//! see `ivm::operator`'s module doc for the broader scope deviation
//! (concrete composition instead of a trait-object operator graph).
//!
//! The one piece of real logic here — and the reason `Filter` isn't just
//! `Iterator::filter` — is edit-splitting: `filterPush`'s documented
//! contract (see `change.ts`'s `EditChange` doc comment) is that an `Edit`
//! whose presence-under-the-predicate changes must be turned into a
//! `Remove` (matched -> unmatched) or an `Add` (unmatched -> matched), not
//! passed through as an `Edit` a downstream consumer would misinterpret as
//! "this row was already present".

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::data::Row;
use crate::ivm::operator::{
    Change, FetchRequest, Input, InputBase, Node, Operator, Output, SourceSchema, Stream,
    ThrowOutput,
};
use crate::ivm::table_source::TableSource;

/// Filters a `TableSource`'s changes and fetches through `predicate`. Port
/// of `Filter`, restricted to wrapping a `TableSource` directly (v1 scope —
/// see module doc).
pub struct Filter<'a> {
    predicate: Box<dyn Fn(&Row) -> bool + 'a>,
}

impl<'a> Filter<'a> {
    pub fn new(predicate: impl Fn(&Row) -> bool + 'a) -> Self {
        Filter {
            predicate: Box::new(predicate),
        }
    }

    /// Filters a source's fetch stream. Port of `Filter.fetch` (inherited
    /// from wrapping `FilterInput`/pass-through fetch).
    pub fn fetch<'s>(&'s self, source: &'s TableSource, req: &FetchRequest) -> Stream<'s, Node> {
        let predicate = &self.predicate;
        Box::new(source.fetch(req).filter(move |node| predicate(&node.row)))
    }

    /// Translates a source-level `Change` through the predicate. Port of
    /// `filterPush`'s change-splitting logic (see module doc): an `Add`/
    /// `Remove` passes through only if its row matches; an `Edit` is
    /// reclassified based on whether the predicate's verdict changed.
    /// Returns `None` if the change is entirely invisible to this filter
    /// (neither old nor new row matched).
    pub fn push(&self, change: Change) -> Option<Change> {
        filter_push(change, &self.predicate)
    }
}

/// The edit-splitting core shared by [`Filter::push`] and [`GraphFilter`].
/// Port of `filterPush` (`filter-push.ts`): an `Add`/`Remove`/`Child` passes
/// through only if its row matches the predicate; an `Edit` is reclassified
/// (`Add`/`Remove`/`Edit`/drop) based on whether the predicate verdict
/// changed. Returns `None` when the change is entirely invisible to the
/// filter.
pub(crate) fn filter_push(change: Change, predicate: &dyn Fn(&Row) -> bool) -> Option<Change> {
    match change {
        Change::Add(node) => predicate(&node.row).then_some(Change::Add(node)),
        Change::Remove(node) => predicate(&node.row).then_some(Change::Remove(node)),
        Change::Child { node, child } => {
            predicate(&node.row).then_some(Change::Child { node, child })
        }
        Change::Edit { node, old_node } => {
            let new_matches = predicate(&node.row);
            let old_matches = predicate(&old_node.row);
            match (old_matches, new_matches) {
                (true, true) => Some(Change::Edit { node, old_node }),
                (true, false) => Some(Change::Remove(old_node)),
                (false, true) => Some(Change::Add(node)),
                (false, false) => None,
            }
        }
    }
}

/// Graph-capable `Filter`: a real [`Operator`] wired into the push-based
/// operator graph via `Rc<dyn Input>` upstream and `Rc<dyn Output>`
/// downstream. Port of upstream's `Filter` operator's standard `push`/`fetch`
/// role (`filter.ts`) — the `FilterOperator` OR sub-protocol
/// (`beginFilter`/`endFilter`/`filter`) is deferred with FanOut/FanIn
/// (redesign Section 4.4).
///
/// This is the additive graph API; the concrete [`Filter`] above (with its
/// `TableSource`-taking `fetch`/`push`) stays intact for existing callers.
pub struct GraphFilter {
    input: Rc<dyn Input>,
    predicate: Box<dyn Fn(&Row) -> bool>,
    output: RefCell<Rc<dyn Output>>,
}

impl GraphFilter {
    /// Builds a filter over `input`, wiring `input.set_output(self)` so source
    /// pushes flow through the filter — matching [`crate::ivm::skip::Skip::new`]/
    /// [`crate::ivm::take::Take::new`] and upstream's `input.setOutput(this)`
    /// (`filter.ts`). Its own downstream output starts as [`ThrowOutput`] until
    /// [`Input::set_output`] wires the real one (upstream sets
    /// `#output = throwFilterOutput` initially).
    pub fn new(input: Rc<dyn Input>, predicate: impl Fn(&Row) -> bool + 'static) -> Rc<Self> {
        let filter = Rc::new(GraphFilter {
            input,
            predicate: Box::new(predicate),
            output: RefCell::new(Rc::new(ThrowOutput)),
        });
        filter.input.set_output(filter.clone());
        filter
    }
}

impl InputBase for GraphFilter {
    fn get_schema(&self) -> SourceSchema {
        self.input.get_schema()
    }

    fn destroy(&self) {
        self.input.destroy();
    }
}

impl Input for GraphFilter {
    fn set_output(&self, output: Rc<dyn Output>) {
        *self.output.borrow_mut() = output;
    }

    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node> {
        let predicate = &self.predicate;
        Box::new(
            self.input
                .fetch(req)
                .filter(move |node| predicate(&node.row)),
        )
    }
}

impl Output for GraphFilter {
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        if let Some(change) = filter_push(change, &self.predicate) {
            self.output.borrow().push(change, self);
        }
    }
}

impl Operator for GraphFilter {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::{make_source_change_add, make_source_change_edit};
    use zero_cache_protocol::ast::Direction;
    use zero_cache_shared::bigint_json::JsonValue;

    fn row(id: i64, active: bool) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("active".into(), JsonValue::Bool(active)),
        ]
    }

    fn is_active(row: &Row) -> bool {
        row.iter()
            .any(|(k, v)| k == "active" && *v == JsonValue::Bool(true))
    }

    #[test]
    fn fetch_only_returns_matching_rows() {
        let mut s = TableSource::new("t", vec!["id".into()], vec![("id".into(), Direction::Asc)]);
        s.push(make_source_change_add(row(1, true)));
        s.push(make_source_change_add(row(2, false)));
        let f = Filter::new(is_active);
        let rows: Vec<Node> = f.fetch(&s, &FetchRequest::default()).collect();
        assert_eq!(rows, vec![Node::new(row(1, true))]);
    }

    #[test]
    fn push_add_matching_passes_through() {
        let f = Filter::new(is_active);
        let change = Change::Add(Node::new(row(1, true)));
        assert_eq!(f.push(change.clone()), Some(change));
    }

    #[test]
    fn push_add_nonmatching_is_dropped() {
        let f = Filter::new(is_active);
        assert_eq!(f.push(Change::Add(Node::new(row(1, false)))), None);
    }

    #[test]
    fn push_child_change_tracks_parent_filter_membership() {
        let f = Filter::new(is_active);
        let child = Change::Add(Node::new(row(9, true)));
        let matching = crate::ivm::operator::make_child_change(
            Node::new(row(1, true)),
            "children",
            child.clone(),
        );
        assert_eq!(f.push(matching.clone()), Some(matching));

        let non_matching =
            crate::ivm::operator::make_child_change(Node::new(row(2, false)), "children", child);
        assert_eq!(f.push(non_matching), None);
    }

    #[test]
    fn push_edit_still_matching_stays_edit() {
        let f = Filter::new(is_active);
        let change = Change::Edit {
            node: Node::new(row(1, true)),
            old_node: Node::new(row(1, true)),
        };
        assert_eq!(f.push(change.clone()), Some(change));
    }

    #[test]
    fn push_edit_leaving_match_becomes_remove() {
        let f = Filter::new(is_active);
        let change = Change::Edit {
            node: Node::new(row(1, false)),
            old_node: Node::new(row(1, true)),
        };
        assert_eq!(
            f.push(change),
            Some(Change::Remove(Node::new(row(1, true))))
        );
    }

    #[test]
    fn push_edit_entering_match_becomes_add() {
        let f = Filter::new(is_active);
        let change = Change::Edit {
            node: Node::new(row(1, true)),
            old_node: Node::new(row(1, false)),
        };
        assert_eq!(f.push(change), Some(Change::Add(Node::new(row(1, true)))));
    }

    #[test]
    fn push_edit_never_matching_is_dropped() {
        let f = Filter::new(is_active);
        let change = Change::Edit {
            node: Node::new(row(1, false)),
            old_node: Node::new(row(1, false)),
        };
        assert_eq!(f.push(change), None);
    }

    // ---- graph-capable GraphFilter (Operator) tests ----

    use crate::ivm::operator::SourceSchema;
    use std::cell::RefCell as StdRefCell;
    use std::collections::BTreeMap;

    /// Minimal in-memory `Input` for exercising the operator graph: yields a
    /// fixed set of rows (unfiltered) as leaf `Node`s.
    struct VecInput {
        rows: Vec<Row>,
        destroyed: StdRefCell<bool>,
    }
    impl VecInput {
        fn new(rows: Vec<Row>) -> Rc<Self> {
            Rc::new(VecInput {
                rows,
                destroyed: StdRefCell::new(false),
            })
        }
    }
    impl InputBase for VecInput {
        fn get_schema(&self) -> SourceSchema {
            SourceSchema {
                table_name: "t".into(),
                primary_key: vec!["id".into()],
                sort: vec![("id".into(), Direction::Asc)],
                relationships: BTreeMap::new(),
            }
        }
        fn destroy(&self) {
            *self.destroyed.borrow_mut() = true;
        }
    }
    impl Input for VecInput {
        fn set_output(&self, _output: Rc<dyn Output>) {}
        fn fetch<'a>(&'a self, _req: &FetchRequest) -> Stream<'a, Node> {
            Box::new(self.rows.iter().cloned().map(Node::new))
        }
    }

    struct SpyOutput {
        received: StdRefCell<Vec<Change>>,
    }
    impl SpyOutput {
        fn new() -> Rc<Self> {
            Rc::new(SpyOutput {
                received: StdRefCell::new(Vec::new()),
            })
        }
    }
    impl Output for SpyOutput {
        fn push(&self, change: Change, _pusher: &dyn InputBase) {
            self.received.borrow_mut().push(change);
        }
    }

    #[test]
    fn graph_filter_fetch_filters_upstream_stream() {
        let input = VecInput::new(vec![row(1, true), row(2, false), row(3, true)]);
        let f = GraphFilter::new(input, is_active);
        let rows: Vec<Node> = f.fetch(&FetchRequest::default()).collect();
        assert_eq!(rows, vec![Node::new(row(1, true)), Node::new(row(3, true))]);
    }

    #[test]
    fn graph_filter_get_schema_delegates_to_input() {
        let input = VecInput::new(vec![]);
        let f = GraphFilter::new(input, is_active);
        assert_eq!(f.get_schema().table_name, "t");
    }

    #[test]
    fn graph_filter_push_forwards_matching_and_splits_edits() {
        let input = VecInput::new(vec![]);
        let f = GraphFilter::new(input, is_active);
        let spy = SpyOutput::new();
        f.set_output(spy.clone());

        // matching Add passes through
        f.push(Change::Add(Node::new(row(1, true))), &*f);
        // non-matching Add is dropped
        f.push(Change::Add(Node::new(row(2, false))), &*f);
        // Edit leaving the predicate becomes a Remove of the old row
        f.push(
            Change::Edit {
                node: Node::new(row(1, false)),
                old_node: Node::new(row(1, true)),
            },
            &*f,
        );

        let received = spy.received.borrow();
        assert_eq!(
            *received,
            vec![
                Change::Add(Node::new(row(1, true))),
                Change::Remove(Node::new(row(1, true))),
            ]
        );
    }

    #[test]
    fn graph_filter_destroy_cascades_to_input() {
        let input = VecInput::new(vec![]);
        let f = GraphFilter::new(input.clone(), is_active);
        f.destroy();
        assert!(*input.destroyed.borrow());
    }

    #[test]
    #[should_panic(expected = "Output not set")]
    fn graph_filter_push_without_output_panics() {
        let input = VecInput::new(vec![]);
        let f = GraphFilter::new(input, is_active);
        f.push(Change::Add(Node::new(row(1, true))), &*f);
    }

    #[test]
    fn end_to_end_source_push_through_filter() {
        let mut s = TableSource::new("t", vec!["id".into()], vec![("id".into(), Direction::Asc)]);
        let f = Filter::new(is_active);

        let source_change = s.push(make_source_change_add(row(1, true)));
        assert_eq!(
            f.push(source_change),
            Some(Change::Add(Node::new(row(1, true))))
        );

        let source_change = s.push(make_source_change_edit(row(1, false), row(1, true)));
        assert_eq!(
            f.push(source_change),
            Some(Change::Remove(Node::new(row(1, true))))
        );
    }
}
