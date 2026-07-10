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

use std::collections::HashMap;

use zero_cache_protocol::ast::Ordering;

use crate::ivm::constraint::{Constraint, PrimaryKey};
use crate::ivm::data::Row;
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

/// A downstream consumer of operator `Change`s in the push-based graph.
/// Port of upstream's `Output` interface. Held as `Rc<dyn Output>`, NOT
/// `Rc<RefCell<dyn Output>>` — the `RefCell` (or other interior mutability)
/// lives inside each concrete implementor around whatever state it needs to
/// mutate on `push`, rather than wrapping the whole trait object. This
/// keeps `Rc<dyn Output>` clonable and shareable across multiple
/// registrations without every caller needing to borrow_mut the outer
/// cell just to call a method that takes `&self`.
pub trait Output {
    fn push(&self, change: Change);
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
    use zero_cache_shared::bigint_json::JsonValue;

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
