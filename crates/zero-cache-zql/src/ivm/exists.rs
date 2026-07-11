//! Port of `zql/src/ivm/exists.ts` — the `Exists` operator, which filters a
//! parent stream by whether a named child relationship is non-empty
//! (`EXISTS`) or empty (`NOT EXISTS`).
//!
//! **Model, and how it maps to upstream.** Upstream's `Exists` is a
//! `FilterOperator` (the `beginFilter`/`endFilter`/`filter` sub-protocol used
//! by `FanOut`/`FanIn`) that keeps an in-memory per-fetch-pass size *cache*
//! (`Map<string, boolean>`, `exists.ts:27`) keyed by the parent's
//! `parentJoinKey` values. This port doesn't yet have the `FilterOperator`
//! sub-protocol (deferred with FanOut/FanIn — redesign §4.4), so `Exists` here
//! is a plain [`Operator`] (`Input` + `Output`) wired into the same push-based
//! graph as [`crate::ivm::filter::GraphFilter`]: an upstream `Rc<dyn Input>`
//! (in the real graph, a `Join` that populates
//! `Node.relationships[relationship_name]`), an `Rc<dyn Output>` downstream,
//! and per-parent state in [`Storage`] instead of the per-pass in-memory
//! cache. The push semantics are ported directly from `exists.ts`'s `#push`
//! (`exists.ts:109`): `Add`/`Remove`/`Edit` and non-watched child changes
//! cannot change relationship emptiness, so they pass through the exists
//! filter unchanged (`#pushWithFilter`, `exists.ts:235`); a child `Add`/
//! `Remove` on the watched relationship *can* flip emptiness, and when it
//! crosses the `0↔1` boundary the child change is converted into an `Add` or
//! `Remove` of the *parent* node downstream (`exists.ts:139-200`), inverted
//! for `NOT EXISTS`.
//!
//! **Where the "size" comes from.** In the graph, a child change arrives as a
//! `Change::Child` whose `node` is the parent with its relationship *already*
//! reflecting the change (the upstream `Join` re-derived it — see
//! `join.rs`'s `push_child_change`). So the post-change size is exactly
//! `node.relationships[relationship_name].len()`, matching upstream's
//! `#fetchSize` reading `node.relationships[...]` after the child source has
//! applied the change (`exists.ts:248`). The per-parent count is written
//! through to [`Storage`] (keyed by the `parentJoinKey` values, matching
//! `#getCacheKey`, `exists.ts:224`) so a later `fetch` can read it back.
//!
//! **Scope note / deviation.** Upstream's `#noSizeReuse`/`#inPush` machinery
//! exists purely to make the *fetch-pass* size cache safe to reuse across
//! sibling rows and to disable that reuse mid-push (`exists.ts:39,61,82`).
//! Because this port recomputes size directly from each node's (already
//! consistent) `relationships` rather than reusing a cached size across rows,
//! that machinery isn't needed and isn't ported. The `0↔1` transition
//! emission — the actual behavioral spec — is ported faithfully, including
//! upstream's relationship reconstruction on the emitted parent change
//! (emptying the relationship for a `NOT EXISTS` add-to-1, and re-including
//! the removed child for an `EXISTS` remove-to-0, `exists.ts:150,183`).

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::data::{Row, Value};
use crate::ivm::operator::{
    Change, FetchRequest, Input, InputBase, Node, Operator, Output, SourceSchema, Storage, Stream,
    ThrowOutput,
};
use zero_cache_shared::bigint_json::JsonValue;

/// Which flavor of existence filter this operator applies. Port of the
/// `'EXISTS' | 'NOT EXISTS'` constructor argument (`exists.ts:45`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistsType {
    Exists,
    NotExists,
}

fn get(row: &Row, field: &str) -> Value {
    row.iter()
        .find(|(k, _)| k == field)
        .map(|(_, v)| v.clone())
        .unwrap_or(Value::Null)
}

/// The primary node a change is "about" — the parent node for `Add`/`Remove`/
/// `Edit`/`Child`. Its `relationships[relationship_name]` is what the exists
/// filter inspects.
fn primary_node(change: &Change) -> &Node {
    match change {
        Change::Add(node) | Change::Remove(node) => node,
        Change::Edit { node, .. } => node,
        Change::Child { node, .. } => node,
    }
}

/// The `Exists`/`NOT EXISTS` operator. Filters/reshapes a parent stream by
/// whether the parent's `relationship_name` relationship is non-empty.
pub struct Exists {
    input: Rc<dyn Input>,
    storage: Rc<dyn Storage>,
    relationship_name: String,
    parent_join_key: Vec<String>,
    not: bool,
    output: RefCell<Rc<dyn Output>>,
}

impl Exists {
    /// Builds an `Exists` over `input`. `relationship_name` names the child
    /// relationship (populated upstream by a `Join`), `parent_join_key` is the
    /// set of parent columns the per-parent count is keyed by (upstream's
    /// `parentJoinKey`), and `exists_type` selects `EXISTS` vs `NOT EXISTS`.
    /// The output starts as [`ThrowOutput`] until [`Input::set_output`] wires
    /// the real downstream (mirroring `GraphFilter::new`).
    pub fn new(
        input: Rc<dyn Input>,
        storage: Rc<dyn Storage>,
        relationship_name: impl Into<String>,
        parent_join_key: Vec<String>,
        exists_type: ExistsType,
    ) -> Rc<Self> {
        Rc::new(Exists {
            input,
            storage,
            relationship_name: relationship_name.into(),
            parent_join_key,
            not: exists_type == ExistsType::NotExists,
            output: RefCell::new(Rc::new(ThrowOutput)),
        })
    }

    /// The `Storage` key for `node`'s parent join value — port of
    /// `#getCacheKey` (`exists.ts:224`): the JSON encoding of the
    /// `parent_join_key` column values, in key order.
    fn cache_key(&self, node: &Node) -> String {
        let values: Vec<Value> = self
            .parent_join_key
            .iter()
            .map(|k| get(&node.row, k))
            .collect();
        JsonValue::Array(values).stringify()
    }

    /// The current size of `node`'s watched relationship. In the graph the
    /// relationship already reflects any in-flight child change (the upstream
    /// `Join` re-derived it), so this is the post-change size — matching
    /// upstream's `#fetchSize` (`exists.ts:248`).
    fn relationship_size(&self, node: &Node) -> usize {
        node.relationships
            .get(&self.relationship_name)
            .map(|children| children.len())
            .unwrap_or(0)
    }

    /// Writes the per-parent count through to `Storage` (upstream caches it in
    /// `#cache`; this port persists it so `fetch` can read it back).
    fn store_size(&self, node: &Node, size: usize) {
        let key = self.cache_key(node);
        let _ = self.storage.set(&key, JsonValue::Number(size as f64));
    }

    /// The stored per-parent count for `node`, if any. Not consulted during
    /// `push` (size is recomputed from the node's relationship, matching
    /// upstream's mid-push cache bypass), but exposed so a `fetch` — or a
    /// test — can observe the maintained count.
    pub fn cached_size(&self, node: &Node) -> Option<usize> {
        match self.storage.get(&self.cache_key(node), None) {
            Ok(Some(JsonValue::Number(n))) => Some(n as usize),
            _ => None,
        }
    }

    /// Applies the exists/not-exists verdict to a raw `exists` boolean. Port
    /// of `#filter`'s `this.#not ? !exists : exists` (`exists.ts:221`).
    fn passes(&self, exists: bool) -> bool {
        if self.not {
            !exists
        } else {
            exists
        }
    }

    /// A copy of `node` with its watched relationship replaced by `children`.
    /// Port of upstream's `{...relationships, [relationshipName]: () => ...}`
    /// reconstruction on the emitted parent change (`exists.ts:150,183`).
    fn node_with_relationship(&self, node: &Node, children: Vec<Node>) -> Node {
        let mut reshaped = node.clone();
        reshaped
            .relationships
            .insert(self.relationship_name.clone(), children);
        reshaped
    }

    /// Forwards `change` downstream iff its node passes the exists filter.
    /// Port of `#pushWithFilter` (`exists.ts:235`); `exists_override` mirrors
    /// the optional `exists` argument upstream passes when it already computed
    /// the size.
    fn push_with_filter(&self, change: Change, exists_override: Option<bool>) {
        let node = primary_node(&change);
        let size = self.relationship_size(node);
        self.store_size(node, size);
        let exists = exists_override.unwrap_or(size > 0);
        if self.passes(exists) {
            let output = self.output.borrow().clone();
            output.push(change, self);
        }
    }
}

impl InputBase for Exists {
    fn get_schema(&self) -> SourceSchema {
        self.input.get_schema()
    }

    fn destroy(&self) {
        self.input.destroy();
    }
}

impl Input for Exists {
    fn set_output(&self, output: Rc<dyn Output>) {
        *self.output.borrow_mut() = output;
    }

    /// Passes through only the parent nodes whose relationship passes the
    /// exists filter, writing each parent's count through to `Storage`. Port
    /// of `Exists.filter` (`exists.ts:80`) in the graph's pull direction.
    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node> {
        Box::new(self.input.fetch(req).filter(move |node| {
            let size = self.relationship_size(node);
            self.store_size(node, size);
            self.passes(size > 0)
        }))
    }
}

impl Output for Exists {
    /// Port of `Exists.push` (`exists.ts:109`).
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        // Only an add/remove child change on the *watched* relationship can
        // flip its emptiness; everything else just runs through the filter.
        if let Change::Child { node, child } = &change {
            if child.relationship_name == self.relationship_name {
                match &*child.change {
                    Change::Add(_) => {
                        let size = self.relationship_size(node);
                        self.store_size(node, size);
                        let output = self.output.borrow().clone();
                        if size == 1 {
                            // Transition 0 -> 1.
                            if self.not {
                                // Was present under NOT EXISTS (empty), now
                                // absent. The just-added child was never
                                // emitted downstream, so exclude it from the
                                // removed parent's relationship.
                                output.push(
                                    Change::Remove(self.node_with_relationship(node, vec![])),
                                    self,
                                );
                            } else {
                                output.push(Change::Add(node.clone()), self);
                            }
                        } else {
                            // size > 1: no emptiness flip; forward as-is.
                            self.push_with_filter(change, Some(size > 0));
                        }
                        return;
                    }
                    Change::Remove(removed_child) => {
                        let size = self.relationship_size(node);
                        self.store_size(node, size);
                        let output = self.output.borrow().clone();
                        if size == 0 {
                            // Transition 1 -> 0.
                            if self.not {
                                output.push(Change::Add(node.clone()), self);
                            } else {
                                // The removed child was never separately
                                // emitted; re-include it in the removed
                                // parent's relationship.
                                output.push(
                                    Change::Remove(
                                        self.node_with_relationship(
                                            node,
                                            vec![removed_child.clone()],
                                        ),
                                    ),
                                    self,
                                );
                            }
                        } else {
                            self.push_with_filter(change, Some(size > 0));
                        }
                        return;
                    }
                    // Edit / nested Child changes can't change size.
                    _ => {}
                }
            }
        }
        self.push_with_filter(change, None);
    }
}

impl Operator for Exists {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::memory_storage::MemoryStorage;
    use crate::ivm::operator::make_child_change;
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::Direction;
    use zero_cache_shared::bigint_json::JsonValue;

    fn issue(id: i64) -> Row {
        vec![("id".into(), JsonValue::Number(id as f64))]
    }
    fn comment(id: i64, issue_id: i64) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("issueID".into(), JsonValue::Number(issue_id as f64)),
        ]
    }

    /// A parent (issue) node with its `comments` relationship populated.
    fn parent(id: i64, children: &[Row]) -> Node {
        let mut node = Node::new(issue(id));
        node.relationships.insert(
            "comments".into(),
            children.iter().cloned().map(Node::new).collect(),
        );
        node
    }

    fn schema() -> SourceSchema {
        SourceSchema {
            table_name: "issue".into(),
            primary_key: vec!["id".into()],
            sort: vec![("id".into(), Direction::Asc)],
            relationships: BTreeMap::new(),
        }
    }

    /// Minimal `Input` returning a fixed set of parent nodes.
    struct StubInput {
        nodes: Vec<Node>,
        destroyed: RefCell<bool>,
    }
    impl StubInput {
        fn new(nodes: Vec<Node>) -> Rc<Self> {
            Rc::new(StubInput {
                nodes,
                destroyed: RefCell::new(false),
            })
        }
    }
    impl InputBase for StubInput {
        fn get_schema(&self) -> SourceSchema {
            schema()
        }
        fn destroy(&self) {
            *self.destroyed.borrow_mut() = true;
        }
    }
    impl Input for StubInput {
        fn set_output(&self, _output: Rc<dyn Output>) {}
        fn fetch<'a>(&'a self, _req: &FetchRequest) -> Stream<'a, Node> {
            Box::new(self.nodes.clone().into_iter())
        }
    }

    struct SpyOutput {
        received: RefCell<Vec<Change>>,
    }
    impl SpyOutput {
        fn new() -> Rc<Self> {
            Rc::new(SpyOutput {
                received: RefCell::new(Vec::new()),
            })
        }
    }
    impl Output for SpyOutput {
        fn push(&self, change: Change, _pusher: &dyn InputBase) {
            self.received.borrow_mut().push(change);
        }
    }

    fn exists_op(input: Rc<dyn Input>, ty: ExistsType) -> (Rc<Exists>, Rc<SpyOutput>) {
        let storage = Rc::new(MemoryStorage::default());
        let op = Exists::new(input, storage, "comments", vec!["id".into()], ty);
        let spy = SpyOutput::new();
        op.set_output(spy.clone());
        (op, spy)
    }

    fn child_add(node: Node, child: Row) -> Change {
        make_child_change(node, "comments", Change::Add(Node::new(child)))
    }
    fn child_remove(node: Node, child: Row) -> Change {
        make_child_change(node, "comments", Change::Remove(Node::new(child)))
    }

    // ---- EXISTS: child add / remove 0<->1 transitions ----

    #[test]
    fn exists_child_add_zero_to_one_emits_parent_add() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::Exists);
        // Post-add relationship has exactly one child => transition 0 -> 1.
        let node = parent(1, &[comment(10, 1)]);
        op.push(child_add(node.clone(), comment(10, 1)), &*op);
        assert_eq!(*spy.received.borrow(), vec![Change::Add(node)]);
    }

    #[test]
    fn exists_child_add_one_to_two_forwards_child_change() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::Exists);
        // Post-add size is 2 => no emptiness flip; the child change is
        // forwarded (parent still exists).
        let node = parent(1, &[comment(10, 1), comment(11, 1)]);
        let change = child_add(node.clone(), comment(11, 1));
        op.push(change.clone(), &*op);
        assert_eq!(*spy.received.borrow(), vec![change]);
    }

    #[test]
    fn exists_child_remove_one_to_zero_emits_parent_remove_with_child() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::Exists);
        // Post-remove size is 0 => transition 1 -> 0. The removed child is
        // re-included in the removed parent's relationship.
        let node = parent(1, &[]);
        op.push(child_remove(node.clone(), comment(10, 1)), &*op);
        assert_eq!(
            *spy.received.borrow(),
            vec![Change::Remove(parent(1, &[comment(10, 1)]))]
        );
    }

    #[test]
    fn exists_child_remove_two_to_one_forwards_child_change() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::Exists);
        // Post-remove size is 1 => still exists; forward the child change.
        let node = parent(1, &[comment(11, 1)]);
        let change = child_remove(node.clone(), comment(10, 1));
        op.push(change.clone(), &*op);
        assert_eq!(*spy.received.borrow(), vec![change]);
    }

    // ---- NOT EXISTS inversions ----

    #[test]
    fn not_exists_child_add_zero_to_one_emits_parent_remove_emptied() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::NotExists);
        // Under NOT EXISTS the parent was present while empty; the add makes
        // it non-empty => remove the parent, with the relationship emptied
        // (the added child was never emitted downstream).
        let node = parent(1, &[comment(10, 1)]);
        op.push(child_add(node, comment(10, 1)), &*op);
        assert_eq!(*spy.received.borrow(), vec![Change::Remove(parent(1, &[]))]);
    }

    #[test]
    fn not_exists_child_remove_one_to_zero_emits_parent_add() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::NotExists);
        // Removing the last child makes the parent empty => present under
        // NOT EXISTS => add the parent.
        let node = parent(1, &[]);
        op.push(child_remove(node.clone(), comment(10, 1)), &*op);
        assert_eq!(*spy.received.borrow(), vec![Change::Add(node)]);
    }

    #[test]
    fn not_exists_child_add_one_to_two_forwards_nothing() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::NotExists);
        // Size stays >= 1 => parent is absent under NOT EXISTS both before and
        // after; nothing is forwarded.
        let node = parent(1, &[comment(10, 1), comment(11, 1)]);
        op.push(child_add(node, comment(11, 1)), &*op);
        assert!(spy.received.borrow().is_empty());
    }

    // ---- Add / Remove / Edit parent changes: pushWithFilter ----

    #[test]
    fn exists_parent_add_with_children_passes_through() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::Exists);
        let node = parent(1, &[comment(10, 1)]);
        op.push(Change::Add(node.clone()), &*op);
        assert_eq!(*spy.received.borrow(), vec![Change::Add(node)]);
    }

    #[test]
    fn exists_parent_add_without_children_is_dropped() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::Exists);
        op.push(Change::Add(parent(1, &[])), &*op);
        assert!(spy.received.borrow().is_empty());
    }

    #[test]
    fn not_exists_parent_add_without_children_passes_through() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::NotExists);
        let node = parent(1, &[]);
        op.push(Change::Add(node.clone()), &*op);
        assert_eq!(*spy.received.borrow(), vec![Change::Add(node)]);
    }

    #[test]
    fn not_exists_parent_add_with_children_is_dropped() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::NotExists);
        op.push(Change::Add(parent(1, &[comment(10, 1)])), &*op);
        assert!(spy.received.borrow().is_empty());
    }

    #[test]
    fn non_watched_child_change_uses_push_with_filter() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::Exists);
        // A child change on a DIFFERENT relationship: existence of `comments`
        // is unchanged, so it just runs through the filter (parent has a
        // comment => passes).
        let node = parent(1, &[comment(10, 1)]);
        let change =
            make_child_change(node.clone(), "reactions", Change::Add(Node::new(issue(99))));
        op.push(change.clone(), &*op);
        assert_eq!(*spy.received.borrow(), vec![change]);
    }

    #[test]
    fn watched_child_edit_uses_push_with_filter() {
        let (op, spy) = exists_op(StubInput::new(vec![]), ExistsType::Exists);
        // A watched-relationship *edit* can't change size => pushWithFilter.
        let node = parent(1, &[comment(10, 1)]);
        let change = make_child_change(
            node.clone(),
            "comments",
            Change::Edit {
                node: Node::new(comment(10, 1)),
                old_node: Node::new(comment(10, 1)),
            },
        );
        op.push(change.clone(), &*op);
        assert_eq!(*spy.received.borrow(), vec![change]);
    }

    // ---- Storage-backed count ----

    #[test]
    fn push_writes_per_parent_count_to_storage() {
        let storage = Rc::new(MemoryStorage::default());
        let op = Exists::new(
            StubInput::new(vec![]),
            storage.clone(),
            "comments",
            vec!["id".into()],
            ExistsType::Exists,
        );
        let spy = SpyOutput::new();
        op.set_output(spy);

        let node = parent(1, &[comment(10, 1), comment(11, 1)]);
        op.push(child_add(node.clone(), comment(11, 1)), &*op);
        assert_eq!(op.cached_size(&node), Some(2));
    }

    // ---- fetch ----

    #[test]
    fn exists_fetch_returns_only_parents_with_children() {
        let input = StubInput::new(vec![parent(1, &[comment(10, 1)]), parent(2, &[])]);
        let (op, _spy) = exists_op(input, ExistsType::Exists);
        let rows: Vec<Node> = op.fetch(&FetchRequest::default()).collect();
        assert_eq!(rows, vec![parent(1, &[comment(10, 1)])]);
    }

    #[test]
    fn not_exists_fetch_returns_only_empty_parents() {
        let input = StubInput::new(vec![parent(1, &[comment(10, 1)]), parent(2, &[])]);
        let (op, _spy) = exists_op(input, ExistsType::NotExists);
        let rows: Vec<Node> = op.fetch(&FetchRequest::default()).collect();
        assert_eq!(rows, vec![parent(2, &[])]);
    }

    #[test]
    fn fetch_writes_counts_to_storage() {
        let input = StubInput::new(vec![parent(1, &[comment(10, 1), comment(11, 1)])]);
        let (op, _spy) = exists_op(input, ExistsType::Exists);
        let _: Vec<Node> = op.fetch(&FetchRequest::default()).collect();
        assert_eq!(op.cached_size(&parent(1, &[])), Some(2));
    }

    // ---- lifecycle ----

    #[test]
    fn get_schema_delegates_to_input() {
        let (op, _spy) = exists_op(StubInput::new(vec![]), ExistsType::Exists);
        assert_eq!(op.get_schema().table_name, "issue");
    }

    #[test]
    fn destroy_cascades_to_input() {
        let input = StubInput::new(vec![]);
        let storage = Rc::new(MemoryStorage::default());
        let op = Exists::new(
            input.clone(),
            storage,
            "comments",
            vec!["id".into()],
            ExistsType::Exists,
        );
        op.destroy();
        assert!(*input.destroyed.borrow());
    }

    #[test]
    #[should_panic(expected = "Output not set")]
    fn push_without_output_panics() {
        let storage = Rc::new(MemoryStorage::default());
        let op = Exists::new(
            StubInput::new(vec![]),
            storage,
            "comments",
            vec!["id".into()],
            ExistsType::Exists,
        );
        op.push(Change::Add(parent(1, &[comment(10, 1)])), &*op);
    }
}
