//! Port of the pure data shapes from `zql/src/ivm/operator.ts`,
//! `zql/src/ivm/data.ts`'s `Node`, and the operator-level `Change` from
//! `zql/src/ivm/change.ts`.
//!
//! **Scope deviation, RESOLVED:** upstream wires operators into a graph via
//! `Input.setOutput(output: Output)`/`Output.push()` — a mutable observer
//! graph where each node holds a trait-object reference to its downstream
//! neighbor. For a single-table filtered query (`TableSource` wrapped by
//! `Filter`) this port modeled v1 as concrete composition instead — an
//! explicit `push`/`fetch` call chain, deferring the graph shape "until
//! `Join` needs a real multi-consumer fan-out" (this doc's own words, for
//! several rounds). `Join` now exists (`crate::ivm::join::Join`) and DOES
//! need real fan-out (one child-table change can affect a join feeding
//! multiple downstream consumers — e.g. a query result AND a permission
//! check watching the same join), so the decision is made: the [`Output`]
//! trait below, held as `Rc<dyn Output>` (not `Rc<RefCell<dyn Output>>` —
//! see that trait's doc for why the `RefCell` moved inside implementors
//! instead of wrapping the trait object). `TableSource`/`Filter` keep their
//! existing direct `push`-returns-`Change` API unchanged (nothing before
//! `Join` needed the graph, so nothing before it was forced to adopt it);
//! `Join` is the first operator that actually registers downstream
//! `Output`s and fans a single incoming change out to all of them.
//!
//! **`Stream`/`'yield'`, resolved:** upstream's `fetch()` returns
//! `Stream<Node | 'yield'>`, a JS generator that can yield the literal
//! string `'yield'` mid-iteration purely so a long-running fetch doesn't
//! block the event loop (a single-threaded-runtime concern). Rust iterators
//! are already lazy/pull-based — the caller controls pacing simply by not
//! calling `.next()` — so there is no `'yield'` signal to carry; `Stream<T>`
//! here is just `Box<dyn Iterator<Item = T> + 'a>`.
//!
//! `relationships` on `Node` is now populated (see `ivm::join`), with one
//! deliberate simplification: upstream generates each relationship
//! LAZILY via a `() -> Stream<Node | 'yield'>` thunk (a join's children
//! are only actually fetched if a caller reads that key). This port
//! stores relationships EAGERLY as a materialized `Vec<Node>` instead —
//! there's no thunk/generator machinery to defer through once `Stream` is
//! just `Box<dyn Iterator>` (see above), and every relationship this port
//! populates today comes from an already-fetched, already-in-memory
//! `TableSource` fetch, so laziness buys nothing yet. Revisit if a
//! relationship ever gets expensive enough to want to skip computing it
//! for an unread key.
//!
//! Not ported in this slice: `Child` changes (joins — see `ivm::join`'s
//! module doc for exactly how far incremental join maintenance goes),
//! `Storage`, `SourceSchema.columns`/`system`/`isHidden` (not needed until
//! a real column-typed source or permissions system exists).

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use zero_cache_protocol::ast::Ordering;

use crate::ivm::constraint::{Constraint, PrimaryKey};
use crate::ivm::data::{make_comparator, Row};
use zero_cache_shared::bigint_json::JsonValue;

/// A lazily-produced sequence, standing in for upstream's `Stream<T>`. See
/// the module doc for why this needs no `'yield'` variant.
pub type Stream<'a, T> = Box<dyn Iterator<Item = T> + 'a>;

/// A row flowing through the pipeline, plus its joined relationships. Port
/// of `Node` — see the module doc for the eager-vs-lazy `relationships`
/// simplification.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Node {
    pub row: Row,
    pub relationships: HashMap<String, Vec<Node>>,
}

impl Node {
    /// Constructs a leaf `Node` with no relationships — the common case
    /// for a bare `TableSource`/`Filter` fetch that isn't part of a join.
    pub fn new(row: Row) -> Self {
        Node {
            row,
            relationships: HashMap::new(),
        }
    }
}

/// A change to one named descendant relationship of an otherwise unchanged
/// parent node.
#[derive(Debug, Clone, PartialEq)]
pub struct ChildData {
    pub relationship_name: String,
    pub change: Box<Change>,
}

/// The complete operator-level change vocabulary.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    Add(Node),
    Remove(Node),
    Child { node: Node, child: ChildData },
    Edit { node: Node, old_node: Node },
}

pub fn make_child_change(
    node: Node,
    relationship_name: impl Into<String>,
    change: Change,
) -> Change {
    Change::Child {
        node,
        child: ChildData {
            relationship_name: relationship_name.into(),
            change: Box::new(change),
        },
    }
}

/// The read/lifecycle half of an operator, matching upstream `InputBase`
/// (`operator.ts:14`). An `Input` also exposes `set_output`/`fetch`.
pub trait InputBase {
    /// The schema of the data this input returns.
    fn get_schema(&self) -> SourceSchema;
    /// Completely destroy the input, cascading `destroy` to its upstreams so
    /// a whole pipeline is torn down. Port of `InputBase.destroy`
    /// (`operator.ts:19`).
    fn destroy(&self);
}

/// Input to an operator. `Rc<dyn Input>` is the upstream-edge type in the
/// push-based graph. Port of `Input` (`operator.ts:26`).
pub trait Input: InputBase {
    /// Tell the input where to send its output (`operator.ts:28`).
    fn set_output(&self, output: Rc<dyn Output>);
    /// Fetch data, returning nodes sorted per
    /// [`SourceSchema::compare_rows`]. There is no `'yield'` variant — Rust
    /// iterators are pull-based, so pacing is the caller not calling
    /// `.next()` (see the module doc). Port of `Input.fetch`
    /// (`operator.ts:43`).
    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node>;
}

/// A downstream consumer of operator `Change`s in the push-based graph.
/// Port of upstream's `Output` interface. Held as `Rc<dyn Output>`, NOT
/// `Rc<RefCell<dyn Output>>` — the `RefCell` (or other interior mutability)
/// lives inside each concrete implementor around whatever state it needs to
/// mutate on `push`, rather than wrapping the whole trait object. This
/// keeps `Rc<dyn Output>` clonable and shareable across multiple
/// registrations without every caller needing to borrow_mut the outer
/// cell just to call a method that takes `&self`.
pub trait Output {
    /// Push an incremental change downstream. `pusher` identifies the calling
    /// input so a downstream that fans back in (fan-in) can attribute the
    /// source branch — matching upstream's `push(change, pusher)`
    /// (`operator.ts:104`).
    fn push(&self, change: Change, pusher: &dyn InputBase);
}

/// Operators are both an [`Input`] and an [`Output`]: each is the input to the
/// next operator in the chain and the output of the previous. Port of
/// `Operator` (`operator.ts:126`).
pub trait Operator: Input + Output {}

/// An [`Output`] that panics if pushed to. Used as the initial value for an
/// operator's output before [`Input::set_output`] wires it up. Port of
/// `throwOutput` (`operator.ts:114`).
pub struct ThrowOutput;

impl Output for ThrowOutput {
    fn push(&self, _change: Change, _pusher: &dyn InputBase) {
        unreachable!("Output not set")
    }
}

/// Error surface shared by in-memory and SQLite-backed operator state.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct StorageError(pub String);

/// Per-operator state storage. Implementations namespace instances by client
/// group and operator, matching the v1.7 `Storage` contract.
pub trait Storage {
    fn set(&self, key: &str, value: JsonValue) -> Result<(), StorageError>;
    fn get(&self, key: &str, default: Option<JsonValue>)
        -> Result<Option<JsonValue>, StorageError>;
    fn scan(&self, prefix: Option<&str>) -> Result<Vec<(String, JsonValue)>, StorageError>;
    fn del(&self, key: &str) -> Result<(), StorageError>;
}

/// Information about the rows an operator produces. Port of `SourceSchema`,
/// trimmed to what a single-table filtered query needs — see module doc.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceSchema {
    pub table_name: String,
    pub primary_key: PrimaryKey,
    pub sort: Ordering,
    /// Named child relationships an operator (Join/Exists) can attach — the
    /// schema of the rows under each `Node.relationships` key. Empty for a
    /// bare source. Port of `SourceSchema.relationships`
    /// (`schema.ts:12`). (`Node.relationships` itself is unchanged this
    /// increment.)
    pub relationships: BTreeMap<String, Box<SourceSchema>>,
}

impl SourceSchema {
    /// Orders two rows per this schema's `sort`. Port of
    /// `SourceSchema.compareRows` (`schema.ts:23`), built from the same
    /// [`make_comparator`] the sources already use.
    pub fn compare_rows(&self, a: &Row, b: &Row) -> std::cmp::Ordering {
        make_comparator(&self.sort, false)(a, b)
    }
}

/// Where a bounded fetch should start. Port of `Start`.
#[derive(Debug, Clone, PartialEq)]
pub struct Start {
    pub row: Row,
    pub basis: StartBasis,
}

/// Port of `Start.basis`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartBasis {
    At,
    After,
}

/// One non-empty batched `IN` clause. Every constraint in the batch has the
/// same column shape. Multiple batches on a request are ANDed together.
pub type MultiConstraint = Vec<Constraint>;

/// A fetch request matching the v1.7 operator contract.
#[derive(Debug, Clone, Default)]
pub struct FetchRequest {
    pub constraint: Option<Constraint>,
    pub multi_constraints: Vec<MultiConstraint>,
    pub start: Option<Start>,
    pub reverse: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::Direction;
    use zero_cache_shared::bigint_json::JsonValue;

    fn schema(sort: Ordering) -> SourceSchema {
        SourceSchema {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            sort,
            relationships: BTreeMap::new(),
        }
    }

    fn row(id: i64) -> Row {
        vec![("id".into(), JsonValue::Number(id as f64))]
    }

    #[test]
    fn source_schema_defaults_to_no_relationships() {
        assert!(schema(vec![("id".into(), Direction::Asc)])
            .relationships
            .is_empty());
    }

    #[test]
    fn compare_rows_orders_ascending_by_sort() {
        let s = schema(vec![("id".into(), Direction::Asc)]);
        assert_eq!(s.compare_rows(&row(1), &row(2)), std::cmp::Ordering::Less);
        assert_eq!(
            s.compare_rows(&row(2), &row(1)),
            std::cmp::Ordering::Greater
        );
        assert_eq!(s.compare_rows(&row(1), &row(1)), std::cmp::Ordering::Equal);
    }

    #[test]
    fn compare_rows_honors_descending_sort() {
        let s = schema(vec![("id".into(), Direction::Desc)]);
        assert_eq!(
            s.compare_rows(&row(1), &row(2)),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    #[should_panic(expected = "Output not set")]
    fn throw_output_panics_on_push() {
        ThrowOutput.push(Change::Add(Node::new(row(1))), &UnitInput);
    }

    /// Minimal `InputBase` to satisfy `Output::push`'s `pusher` argument in
    /// the panic test above.
    struct UnitInput;
    impl InputBase for UnitInput {
        fn get_schema(&self) -> SourceSchema {
            schema(vec![("id".into(), Direction::Asc)])
        }
        fn destroy(&self) {}
    }

    #[test]
    fn node_equality_is_by_row() {
        let row: Row = vec![("id".into(), JsonValue::Number(1.0))];
        assert_eq!(Node::new(row.clone()), Node::new(row));
    }

    #[test]
    fn fetch_request_default_has_no_constraint_or_start() {
        let req = FetchRequest::default();
        assert!(req.constraint.is_none());
        assert!(req.multi_constraints.is_empty());
        assert!(req.start.is_none());
        assert!(!req.reverse);
    }
}
