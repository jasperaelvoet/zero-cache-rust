//! Port of `zql/src/ivm/take.ts`.
//!
//! `Take` implements `limit` queries: it keeps the first `limit` nodes of its
//! input (per the input's comparator) and maintains a durable *bound* — the
//! last accepted row — in per-op [`Storage`], so incoming pushes can be
//! accepted or rejected without re-scanning. It maintains the invariant that
//! its output size is always `<= limit`, even mid-push (a row entering at the
//! window pushes the old bound out via a `Remove` emitted *before* the `Add`).
//!
//! Supports both the global limit (`partition_key: None`, a root query's
//! `limit`) and the PARTITIONED limit (`partition_key: Some(childField)`, a
//! correlated subquery's per-parent `limit`, e.g.
//! `issues.related(comments.limit(n))`): take state is keyed per partition
//! value, a constrained fetch bounds by its own partition's state, and an
//! unconstrained fetch over a partitioned take walks the input up to the
//! global `maxBound`, admitting each row iff its partition's bound does. The
//! full add/remove/edit/child push transition table and `maxBound`
//! bookkeeping are ported faithfully from `take.ts`.

use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::rc::Rc;

use zero_cache_shared::bigint_json::JsonValue;

use crate::ivm::constraint::{Constraint, PrimaryKey};
use crate::ivm::data::Row;
use crate::ivm::operator::{
    Change, FetchRequest, Input, InputBase, Node, Operator, Output, SourceSchema, Start,
    StartBasis, Storage, Stream, ThrowOutput,
};

const MAX_BOUND_KEY: &str = "maxBound";

/// The persisted per-partition take state. Port of `TakeState`.
#[derive(Debug, Clone, PartialEq)]
struct TakeState {
    size: usize,
    bound: Option<Row>,
}

/// A partition key: the field(s) rows are counted by. Port of `PartitionKey`.
pub type PartitionKey = PrimaryKey;

/// Implements `limit` by keeping the first `limit` input nodes.
pub struct Take {
    input: Rc<dyn Input>,
    storage: Rc<dyn Storage>,
    limit: usize,
    partition_key: Option<PartitionKey>,
    schema: SourceSchema,
    /// Fetch overlay needed for some split-push cases: while a `Remove` of the
    /// old bound is being pushed downstream (before the corresponding `Add`),
    /// the row entering the window is hidden from `fetch` so the momentary
    /// state stays consistent. Port of `#rowHiddenFromFetch`.
    row_hidden_from_fetch: RefCell<Option<Row>>,
    output: RefCell<Rc<dyn Output>>,
}

impl Take {
    /// Builds a `Take` over `input`. Mirrors upstream's constructor: asserts a
    /// sorted input and wires `input.setOutput(this)`.
    pub fn new(
        input: Rc<dyn Input>,
        storage: Rc<dyn Storage>,
        limit: usize,
        partition_key: Option<PartitionKey>,
    ) -> Rc<Self> {
        let schema = input.get_schema();
        assert!(!schema.sort.is_empty(), "Take requires sorted input");
        let take = Rc::new(Take {
            input,
            storage,
            limit,
            partition_key,
            schema,
            row_hidden_from_fetch: RefCell::new(None),
            output: RefCell::new(Rc::new(ThrowOutput)),
        });
        take.input.set_output(take.clone());
        take
    }

    fn cmp(&self, a: &Row, b: &Row) -> CmpOrdering {
        self.schema.compare_rows(a, b)
    }

    // ---- storage helpers ----

    fn get_take_state(&self, key: &str) -> Option<TakeState> {
        match self.storage.get(key, None).expect("storage get") {
            Some(JsonValue::Object(fields)) => {
                let size = fields
                    .iter()
                    .find(|(k, _)| k == "size")
                    .and_then(|(_, v)| match v {
                        JsonValue::Number(n) => Some(*n as usize),
                        _ => None,
                    })
                    .expect("take state size");
                let bound = fields
                    .iter()
                    .find(|(k, _)| k == "bound")
                    .and_then(|(_, v)| match v {
                        JsonValue::Object(row) => Some(row.clone()),
                        _ => None,
                    });
                Some(TakeState { size, bound })
            }
            _ => None,
        }
    }

    fn get_max_bound(&self) -> Option<Row> {
        match self.storage.get(MAX_BOUND_KEY, None).expect("storage get") {
            Some(JsonValue::Object(row)) => Some(row),
            _ => None,
        }
    }

    /// Port of `#setTakeState`: persists the state and advances `maxBound` if
    /// the new bound sorts past it.
    fn set_take_state(&self, key: &str, size: usize, bound: Option<Row>, max_bound: Option<Row>) {
        let bound_json = match &bound {
            Some(row) => JsonValue::Object(row.clone()),
            None => JsonValue::Null,
        };
        let state = JsonValue::Object(vec![
            ("size".into(), JsonValue::Number(size as f64)),
            ("bound".into(), bound_json),
        ]);
        self.storage.set(key, state).expect("storage set");
        if let Some(b) = &bound {
            if max_bound.is_none()
                || self.cmp(b, max_bound.as_ref().unwrap()) == CmpOrdering::Greater
            {
                self.storage
                    .set(MAX_BOUND_KEY, JsonValue::Object(b.clone()))
                    .expect("storage set");
            }
        }
    }

    /// Port of `#getStateAndConstraint`. With no partition key the derived
    /// `constraint` is always `None`.
    fn get_state_and_constraint(
        &self,
        row: &Row,
    ) -> (Option<TakeState>, String, Option<Row>, Option<Constraint>) {
        let key = get_take_state_key(&self.partition_key, Some(row));
        let take_state = self.get_take_state(&key);
        let (max_bound, constraint) = if take_state.is_some() {
            (self.get_max_bound(), self.partition_constraint(row))
        } else {
            (None, None)
        };
        (take_state, key, max_bound, constraint)
    }

    fn partition_constraint(&self, row: &Row) -> Option<Constraint> {
        self.partition_key.as_ref().map(|pk| {
            pk.iter()
                .map(|k| {
                    (
                        k.clone(),
                        row.iter()
                            .find(|(c, _)| c == k)
                            .map(|(_, v)| v.clone())
                            .unwrap_or(JsonValue::Null),
                    )
                })
                .collect()
        })
    }

    fn fetch_bounded(
        &self,
        bound: &Row,
        basis: StartBasis,
        constraint: &Option<Constraint>,
        reverse: bool,
    ) -> Vec<Node> {
        self.input
            .fetch(&FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis,
                }),
                constraint: constraint.clone(),
                reverse,
                ..Default::default()
            })
            .collect()
    }

    fn push_with_row_hidden_from_fetch(&self, row: Row, change: Change) {
        *self.row_hidden_from_fetch.borrow_mut() = Some(row);
        self.output.borrow().push(change, self);
        *self.row_hidden_from_fetch.borrow_mut() = None;
    }

    fn output_push(&self, change: Change) {
        self.output.borrow().push(change, self);
    }

    // ---- fetch ----

    fn initial_fetch(&self, req: &FetchRequest) -> Vec<Node> {
        assert!(req.start.is_none(), "Start should be undefined");
        assert!(!req.reverse, "Reverse should be false");
        if self.limit == 0 {
            return Vec::new();
        }
        assert!(
            constraint_matches_partition_key(req.constraint.as_ref(), &self.partition_key),
            "Constraint should match partition key"
        );
        let key = get_take_state_key(&self.partition_key, req.constraint.as_deref());
        assert!(
            self.get_take_state(&key).is_none(),
            "Take state should be undefined"
        );

        let mut size = 0usize;
        let mut bound: Option<Row> = None;
        let mut out = Vec::new();
        for node in self.input.fetch(req) {
            bound = Some(node.row.clone());
            out.push(node);
            size += 1;
            if size == self.limit {
                break;
            }
        }
        self.set_take_state(&key, size, bound, self.get_max_bound());
        out
    }

    // ---- push transitions ----

    fn push_add(
        &self,
        node: Node,
        ts: TakeState,
        key: String,
        max_bound: Option<Row>,
        constraint: Option<Constraint>,
    ) {
        let row = node.row.clone();
        if ts.size < self.limit {
            let new_bound = match &ts.bound {
                None => row.clone(),
                Some(b) if self.cmp(b, &row) == CmpOrdering::Less => row.clone(),
                Some(b) => b.clone(),
            };
            self.set_take_state(&key, ts.size + 1, Some(new_bound), max_bound);
            self.output_push(Change::Add(node));
            return;
        }
        // size == limit
        let Some(bound) = ts.bound.clone() else {
            return;
        };
        if self.cmp(&row, &bound) != CmpOrdering::Less {
            // added row >= bound
            return;
        }
        // added row < bound: evict the current bound node.
        let (before_bound_node, bound_node) = if self.limit == 1 {
            let bn = self
                .fetch_bounded(&bound, StartBasis::At, &constraint, false)
                .into_iter()
                .next();
            (None, bn)
        } else {
            let mut it = self
                .fetch_bounded(&bound, StartBasis::At, &constraint, true)
                .into_iter();
            let bn = it.next();
            let before = it.next();
            (before, bn)
        };
        let bound_node = bound_node.expect("Take: boundNode must be found during fetch");
        let remove_change = Change::Remove(bound_node);
        let new_bound = match &before_bound_node {
            None => row.clone(),
            Some(bb) if self.cmp(&row, &bb.row) == CmpOrdering::Greater => row.clone(),
            Some(bb) => bb.row.clone(),
        };
        // Remove before add to keep the output size <= limit.
        self.set_take_state(&key, ts.size, Some(new_bound), max_bound);
        self.push_with_row_hidden_from_fetch(row, remove_change);
        self.output_push(Change::Add(node));
    }

    fn push_remove(
        &self,
        node: Node,
        ts: TakeState,
        key: String,
        max_bound: Option<Row>,
        constraint: Option<Constraint>,
    ) {
        let Some(bound) = ts.bound.clone() else {
            // change is after bound
            return;
        };
        if self.cmp(&node.row, &bound) == CmpOrdering::Greater {
            // change is after bound
            return;
        }
        let before_bound_node = self
            .fetch_bounded(&bound, StartBasis::After, &constraint, true)
            .into_iter()
            .next();
        let mut new_bound: Option<(Node, bool)> = None;
        if let Some(bn) = before_bound_node {
            let push = self.cmp(&bn.row, &bound) == CmpOrdering::Greater;
            new_bound = Some((bn, push));
        }
        if !matches!(&new_bound, Some((_, true))) {
            for candidate in self.fetch_bounded(&bound, StartBasis::At, &constraint, false) {
                let push = self.cmp(&candidate.row, &bound) == CmpOrdering::Greater;
                new_bound = Some((candidate, push));
                if push {
                    break;
                }
            }
        }

        match new_bound {
            Some((nb, true)) => {
                self.output_push(Change::Remove(node));
                self.set_take_state(&key, ts.size, Some(nb.row.clone()), max_bound);
                self.output_push(Change::Add(nb));
            }
            other => {
                let new_bound_row = other.map(|(n, _)| n.row);
                self.set_take_state(&key, ts.size - 1, new_bound_row, max_bound);
                self.output_push(Change::Remove(node));
            }
        }
    }

    fn push_edit(&self, change: Change) {
        let (node, old_node) = match &change {
            Change::Edit { node, old_node } => (node.clone(), old_node.clone()),
            _ => unreachable!("push_edit called with non-edit change"),
        };

        // Upstream asserts an edit never moves a row across partitions (the
        // source splits such an edit into remove+add before it gets here).
        if let Some(pk) = &self.partition_key {
            let same_partition = pk.iter().all(|key| {
                let value_of = |row: &Row| {
                    row.iter()
                        .find(|(k, _)| k == key)
                        .map(|(_, v)| v.clone())
                        .unwrap_or(JsonValue::Null)
                };
                crate::ivm::data::compare_values(&value_of(&old_node.row), &value_of(&node.row))
                    == CmpOrdering::Equal
            });
            assert!(same_partition, "Unexpected change of partition key");
        }

        let (take_state, key, max_bound, constraint) = self.get_state_and_constraint(&old_node.row);
        let Some(ts) = take_state else {
            return;
        };
        let bound = ts.bound.clone().expect("Bound should be set");
        let old_cmp = self.cmp(&old_node.row, &bound);
        let new_cmp = self.cmp(&node.row, &bound);

        if old_cmp == CmpOrdering::Equal {
            // The bound's row was changed.
            if new_cmp == CmpOrdering::Equal {
                // Keeping the bound; forward the edit unchanged.
                self.output_push(change);
                return;
            }
            if new_cmp == CmpOrdering::Less {
                if self.limit == 1 {
                    self.set_take_state(&key, ts.size, Some(node.row.clone()), max_bound);
                    self.output_push(change);
                    return;
                }
                // New row stays in the window but may no longer be the bound;
                // find the row before the old bound.
                let before_bound_node = self
                    .fetch_bounded(&bound, StartBasis::After, &constraint, true)
                    .into_iter()
                    .next()
                    .expect("Take: beforeBoundNode must be found during fetch");
                self.set_take_state(&key, ts.size, Some(before_bound_node.row), max_bound);
                self.output_push(change);
                return;
            }
            // new_cmp > 0: find the first row at the old bound — the new bound.
            let new_bound_node = self
                .fetch_bounded(&bound, StartBasis::At, &constraint, false)
                .into_iter()
                .next()
                .expect("Take: newBoundNode must be found during fetch");
            if self.cmp(&new_bound_node.row, &node.row) == CmpOrdering::Equal {
                // The next row is the new row — replace the bound, keep the edit.
                self.set_take_state(&key, ts.size, Some(node.row.clone()), max_bound);
                self.output_push(change);
                return;
            }
            // New row now outside the window: remove old, add the new bound row.
            self.set_take_state(&key, ts.size, Some(new_bound_node.row.clone()), max_bound);
            self.push_with_row_hidden_from_fetch(
                new_bound_node.row.clone(),
                Change::Remove(old_node),
            );
            self.output_push(Change::Add(new_bound_node));
            return;
        }

        if old_cmp == CmpOrdering::Greater {
            assert!(
                new_cmp != CmpOrdering::Equal,
                "Invalid state. Row has duplicate primary key"
            );
            if new_cmp == CmpOrdering::Greater {
                // Both old and new outside the window.
                return;
            }
            // old outside, new inside: push the old bound out.
            let mut it = self
                .fetch_bounded(&bound, StartBasis::At, &constraint, true)
                .into_iter();
            let old_bound_node = it
                .next()
                .expect("Take: oldBoundNode must be found during fetch");
            let new_bound_node = it
                .next()
                .expect("Take: newBoundNode must be found during fetch");
            self.set_take_state(&key, ts.size, Some(new_bound_node.row), max_bound);
            self.push_with_row_hidden_from_fetch(node.row.clone(), Change::Remove(old_bound_node));
            self.output_push(Change::Add(node));
            return;
        }

        // old_cmp < 0
        assert!(
            new_cmp != CmpOrdering::Equal,
            "Invalid state. Row has duplicate primary key"
        );
        if new_cmp == CmpOrdering::Less {
            // Both old and new inside the window.
            self.output_push(change);
            return;
        }
        // old inside, new larger than old bound: the row after the bound or the
        // new row becomes the new bound.
        let after_bound_node = self
            .fetch_bounded(&bound, StartBasis::After, &constraint, false)
            .into_iter()
            .next()
            .expect("Take: afterBoundNode must be found during fetch");
        if self.cmp(&after_bound_node.row, &node.row) == CmpOrdering::Equal {
            self.set_take_state(&key, ts.size, Some(node.row.clone()), max_bound);
            self.output_push(change);
            return;
        }
        self.output_push(Change::Remove(old_node));
        self.set_take_state(&key, ts.size, Some(after_bound_node.row.clone()), max_bound);
        self.output_push(Change::Add(after_bound_node));
    }
}

impl InputBase for Take {
    fn get_schema(&self) -> SourceSchema {
        self.input.get_schema()
    }
    fn destroy(&self) {
        self.input.destroy();
    }
}

impl Input for Take {
    fn set_output(&self, output: Rc<dyn Output>) {
        *self.output.borrow_mut() = output;
    }

    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node> {
        if self.partition_key.is_none()
            || req
                .constraint
                .as_ref()
                .is_some_and(|c| constraint_matches_partition_key(Some(c), &self.partition_key))
        {
            let key = get_take_state_key(&self.partition_key, req.constraint.as_deref());
            let take_state = match self.get_take_state(&key) {
                None => return Box::new(self.initial_fetch(req).into_iter()),
                Some(ts) => ts,
            };
            let Some(bound) = take_state.bound else {
                return Box::new(std::iter::empty());
            };
            let hidden = self.row_hidden_from_fetch.borrow().clone();
            let mut out = Vec::new();
            for node in self.input.fetch(req) {
                if self.cmp(&bound, &node.row) == CmpOrdering::Less {
                    break;
                }
                if let Some(h) = &hidden {
                    if self.cmp(h, &node.row) == CmpOrdering::Equal {
                        continue;
                    }
                }
                out.push(node);
            }
            return Box::new(out.into_iter());
        }
        // There is a partition key, but the fetch is not constrained (or is
        // constrained on a different key), so no single take state bounds the
        // scan. Walk the input up to the global max bound, admitting each row
        // iff its own partition's take state does — upstream `fetch`'s second
        // branch (nested sub-query fetches, take.ts).
        let Some(max_bound) = self.get_max_bound() else {
            return Box::new(std::iter::empty());
        };
        let mut out = Vec::new();
        for node in self.input.fetch(req) {
            if self.cmp(&node.row, &max_bound) == CmpOrdering::Greater {
                break;
            }
            let key = get_take_state_key(&self.partition_key, Some(&node.row));
            if let Some(ts) = self.get_take_state(&key) {
                if ts
                    .bound
                    .as_ref()
                    .is_some_and(|b| self.cmp(b, &node.row) != CmpOrdering::Less)
                {
                    out.push(node);
                }
            }
        }
        Box::new(out.into_iter())
    }
}

impl Output for Take {
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        if let Change::Edit { .. } = &change {
            self.push_edit(change);
            return;
        }

        let node_row = match &change {
            Change::Add(n) | Change::Remove(n) => n.row.clone(),
            Change::Child { node, .. } => node.row.clone(),
            Change::Edit { .. } => unreachable!(),
        };
        let (take_state, key, max_bound, constraint) = self.get_state_and_constraint(&node_row);
        let Some(ts) = take_state else {
            return;
        };

        match change {
            Change::Add(node) => self.push_add(node, ts, key, max_bound, constraint),
            Change::Remove(node) => self.push_remove(node, ts, key, max_bound, constraint),
            Change::Child { .. } => {
                // Forward a 'child' change only if its row is within the window.
                if let Some(b) = &ts.bound {
                    if self.cmp(&node_row, b) != CmpOrdering::Greater {
                        self.output_push(change);
                    }
                }
            }
            Change::Edit { .. } => unreachable!(),
        }
    }
}

impl Operator for Take {}

/// Port of `getTakeStateKey`. With no partition key this is the constant
/// `["take"]`, matching upstream's storage key.
fn get_take_state_key(
    partition_key: &Option<PartitionKey>,
    row_or_constraint: Option<&[(String, JsonValue)]>,
) -> String {
    let mut values: Vec<JsonValue> = vec![JsonValue::String("take".into())];
    if let (Some(pk), Some(rc)) = (partition_key, row_or_constraint) {
        for key in pk {
            let v = rc
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap_or(JsonValue::Null);
            values.push(v);
        }
    }
    JsonValue::Array(values).stringify()
}

/// Port of `constraintMatchesPartitionKey`.
fn constraint_matches_partition_key(
    constraint: Option<&Constraint>,
    partition_key: &Option<PartitionKey>,
) -> bool {
    match (constraint, partition_key) {
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
        (Some(c), Some(pk)) => {
            if pk.len() != c.len() {
                return false;
            }
            pk.iter().all(|k| c.iter().any(|(ck, _)| ck == k))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::{
        make_source_change_add, make_source_change_edit, make_source_change_remove,
    };
    use crate::ivm::memory_storage::MemoryStorage;
    use crate::ivm::test_input::{SpyOutput, TestSource};
    use zero_cache_protocol::ast::Direction;

    fn table() -> Rc<TestSource> {
        TestSource::new(
            "testTable",
            vec!["id".into()],
            vec![
                ("created".into(), Direction::Asc),
                ("id".into(), Direction::Asc),
            ],
        )
    }

    fn row(id: &str, created: i64) -> Row {
        vec![
            ("id".into(), JsonValue::String(id.into())),
            ("created".into(), JsonValue::Number(created as f64)),
        ]
    }

    struct Harness {
        source: Rc<TestSource>,
        take: Rc<Take>,
        spy: Rc<SpyOutput>,
    }

    fn setup(source_rows: &[(&str, i64)], limit: usize) -> Harness {
        let source = table();
        for (id, created) in source_rows {
            source.push_change(make_source_change_add(row(id, *created)));
        }
        let storage = Rc::new(MemoryStorage::default());
        let take = Take::new(source.clone(), storage, limit, None);
        // Hydrate the take state via an initial fetch (as the pipeline driver
        // does before applying live pushes).
        let _ = take.fetch(&FetchRequest::default()).count();
        let spy = SpyOutput::new();
        take.set_output(spy.clone());
        Harness { source, take, spy }
    }

    impl Harness {
        fn push(&self, sc: crate::ivm::change::SourceChange) {
            let change = self.source.push_change(sc);
            self.take.push(change, &*self.source);
        }
        fn pushes(&self) -> Vec<Change> {
            self.spy.received.borrow().clone()
        }
        fn take_state(&self) -> Option<TakeState> {
            self.take.get_take_state(&get_take_state_key(&None, None))
        }
        fn data(&self) -> Vec<Row> {
            self.take
                .fetch(&FetchRequest::default())
                .map(|n| n.row)
                .collect()
        }
    }

    fn ids(rows: &[Row]) -> Vec<String> {
        rows.iter()
            .map(
                |r| match r.iter().find(|(k, _)| k == "id").unwrap().1.clone() {
                    JsonValue::String(s) => s,
                    _ => panic!(),
                },
            )
            .collect()
    }

    // ---- add ----

    #[test]
    fn add_limit_zero_emits_nothing() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300)], 0);
        h.push(make_source_change_add(row("i4", 50)));
        assert_eq!(h.pushes(), vec![]);
        assert_eq!(h.take_state(), None);
    }

    #[test]
    fn add_less_than_limit_at_start() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300)], 5);
        h.push(make_source_change_add(row("i4", 50)));
        assert_eq!(h.pushes(), vec![Change::Add(Node::new(row("i4", 50)))]);
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 4,
                bound: Some(row("i3", 300))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i4", "i1", "i2", "i3"]);
    }

    #[test]
    fn add_less_than_limit_at_end() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300)], 5);
        h.push(make_source_change_add(row("i4", 350)));
        assert_eq!(h.pushes(), vec![Change::Add(Node::new(row("i4", 350)))]);
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 4,
                bound: Some(row("i4", 350))
            })
        );
    }

    #[test]
    fn add_at_limit_after_bound_rejected() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        h.push(make_source_change_add(row("i5", 350)));
        assert_eq!(h.pushes(), vec![]);
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i3", 300))
            })
        );
    }

    #[test]
    fn add_at_limit_at_start_evicts_bound() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        h.push(make_source_change_add(row("i5", 50)));
        assert_eq!(
            h.pushes(),
            vec![
                Change::Remove(Node::new(row("i3", 300))),
                Change::Add(Node::new(row("i5", 50))),
            ]
        );
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i2", 200))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i5", "i1", "i2"]);
    }

    #[test]
    fn add_at_limit_at_start_limit_one() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300)], 1);
        h.push(make_source_change_add(row("i5", 50)));
        assert_eq!(
            h.pushes(),
            vec![
                Change::Remove(Node::new(row("i1", 100))),
                Change::Add(Node::new(row("i5", 50))),
            ]
        );
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 1,
                bound: Some(row("i5", 50))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i5"]);
    }

    #[test]
    fn add_at_limit_in_middle_evicts_bound() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        h.push(make_source_change_add(row("i5", 250)));
        assert_eq!(
            h.pushes(),
            vec![
                Change::Remove(Node::new(row("i3", 300))),
                Change::Add(Node::new(row("i5", 250))),
            ]
        );
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i5", 250))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i1", "i2", "i5"]);
    }

    // ---- remove ----

    #[test]
    fn remove_less_than_limit_at_start() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300)], 5);
        h.push(make_source_change_remove(row("i1", 100)));
        assert_eq!(h.pushes(), vec![Change::Remove(Node::new(row("i1", 100)))]);
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 2,
                bound: Some(row("i3", 300))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i2", "i3"]);
    }

    #[test]
    fn remove_after_bound_rejected() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        h.push(make_source_change_remove(row("i4", 400)));
        assert_eq!(h.pushes(), vec![]);
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i3", 300))
            })
        );
    }

    #[test]
    fn remove_at_limit_at_start_with_row_after() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        h.push(make_source_change_remove(row("i1", 100)));
        assert_eq!(
            h.pushes(),
            vec![
                Change::Remove(Node::new(row("i1", 100))),
                Change::Add(Node::new(row("i4", 400))),
            ]
        );
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i4", 400))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i2", "i3", "i4"]);
    }

    #[test]
    fn remove_at_limit_at_end_no_row_after() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300)], 3);
        h.push(make_source_change_remove(row("i3", 300)));
        assert_eq!(h.pushes(), vec![Change::Remove(Node::new(row("i3", 300)))]);
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 2,
                bound: Some(row("i2", 200))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i1", "i2"]);
    }

    #[test]
    fn remove_at_limit_at_end_with_row_after() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        h.push(make_source_change_remove(row("i3", 300)));
        assert_eq!(
            h.pushes(),
            vec![
                Change::Remove(Node::new(row("i3", 300))),
                Change::Add(Node::new(row("i4", 400))),
            ]
        );
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i4", 400))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i1", "i2", "i4"]);
    }

    // ---- child ----

    #[test]
    fn child_within_window_is_forwarded() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        let child = crate::ivm::operator::make_child_change(
            Node::new(row("i2", 200)),
            "comments",
            Change::Add(Node::new(row("c1", 1))),
        );
        h.take.push(child.clone(), &*h.source);
        assert_eq!(h.pushes(), vec![child]);
    }

    #[test]
    fn child_after_bound_is_dropped() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        let child = crate::ivm::operator::make_child_change(
            Node::new(row("i4", 400)),
            "comments",
            Change::Add(Node::new(row("c1", 1))),
        );
        h.take.push(child, &*h.source);
        assert_eq!(h.pushes(), vec![]);
    }

    // ---- edit ----

    #[test]
    fn edit_row_inside_window_forwarded() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        // i1 stays inside the window (created stays < bound i3).
        h.push(make_source_change_edit(row("i1", 150), row("i1", 100)));
        assert_eq!(
            h.pushes(),
            vec![Change::Edit {
                node: Node::new(row("i1", 150)),
                old_node: Node::new(row("i1", 100)),
            }]
        );
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i3", 300))
            })
        );
    }

    #[test]
    fn edit_bound_row_staying_bound_forwards_edit() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        // Edit the bound (i3) keeping it the bound (created still 300 relative
        // ordering; use a non-order field change by keeping created equal is
        // impossible here, so move it slightly but staying the max in-window).
        // i3 -> created 290 keeps it > i2(200) and < i4(400): still the bound.
        h.push(make_source_change_edit(row("i3", 290), row("i3", 300)));
        assert_eq!(
            h.pushes(),
            vec![Change::Edit {
                node: Node::new(row("i3", 290)),
                old_node: Node::new(row("i3", 300)),
            }]
        );
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i3", 290))
            })
        );
    }

    #[test]
    fn edit_bound_row_out_of_window_swaps_in_next() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        // Edit the bound (i3) to created 500 -> now beyond i4(400). i3 leaves
        // the window; i4 becomes the new bound.
        h.push(make_source_change_edit(row("i3", 500), row("i3", 300)));
        assert_eq!(
            h.pushes(),
            vec![
                Change::Remove(Node::new(row("i3", 300))),
                Change::Add(Node::new(row("i4", 400))),
            ]
        );
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i4", 400))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i1", "i2", "i4"]);
    }

    #[test]
    fn edit_row_inside_to_outside_window_swaps() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        // i2 (inside) edited to created 500 (past i4). i2 leaves the window;
        // i4 becomes the new bound.
        h.push(make_source_change_edit(row("i2", 500), row("i2", 200)));
        assert_eq!(
            h.pushes(),
            vec![
                Change::Remove(Node::new(row("i2", 200))),
                Change::Add(Node::new(row("i4", 400))),
            ]
        );
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i4", 400))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i1", "i3", "i4"]);
    }

    #[test]
    fn edit_row_outside_to_inside_window_swaps() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        // i4 (outside) edited to created 50 (before i1). i4 enters the window;
        // the old bound i3 is pushed out.
        h.push(make_source_change_edit(row("i4", 50), row("i4", 400)));
        assert_eq!(
            h.pushes(),
            vec![
                Change::Remove(Node::new(row("i3", 300))),
                Change::Add(Node::new(row("i4", 50))),
            ]
        );
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i2", 200))
            })
        );
        assert_eq!(ids(&h.data()), vec!["i4", "i1", "i2"]);
    }

    #[test]
    fn edit_row_both_outside_window_ignored() {
        let h = setup(&[("i1", 100), ("i2", 200), ("i3", 300), ("i4", 400)], 3);
        h.push(make_source_change_edit(row("i4", 450), row("i4", 400)));
        assert_eq!(h.pushes(), vec![]);
        assert_eq!(
            h.take_state(),
            Some(TakeState {
                size: 3,
                bound: Some(row("i3", 300))
            })
        );
    }
}
