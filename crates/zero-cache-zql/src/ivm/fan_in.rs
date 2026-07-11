//! Port of `zql/src/ivm/fan-in.ts` — the `FanIn` operator that merges the
//! multiple branch streams a [`crate::ivm::fan_out::FanOut`] forked, eliminating
//! duplicates so a row emitted by several OR branches appears exactly once.
//!
//! ```text
//!  issue
//!    |
//! fan-out
//! /      \
//! a      b
//!  \    /
//! fan-in
//!   |
//! ```
//!
//! **How it maps to upstream.** Upstream splits the two directions across two
//! files: `fan-in.ts`'s `FanIn` is a `FilterOperator` participating in the
//! `beginFilter`/`endFilter`/`filter` sub-protocol (it has *no* `fetch` — the
//! fetch-side merge lives in `union-fan-in.ts`'s `mergeFetches`). This port does
//! not have the `FilterOperator` sub-protocol (deferred — see
//! [`crate::ivm::filter`]'s module doc); its operators are plain [`Input`] +
//! [`Output`]. So this one `FanIn` carries BOTH halves:
//!
//! - **pull (`fetch`)** — a k-way merge of the branch fetches, de-duplicating
//!   rows that compare equal under the schema's sort. Port of
//!   `union-fan-in.ts`'s `mergeFetches` (`union-fan-in.ts:224`). This is the
//!   half the driver's fetch-only hydration graph actually drives.
//! - **push** — branch pushes are accumulated, then collapsed to a single
//!   change per row when [`FanOut`] signals it has finished pushing to every
//!   branch. Port of `fan-in.ts`'s `push` + `fanOutDonePushingToAllBranches`
//!   (`fan-in.ts:71,76`), driving the identity-relationship variant of
//!   `pushAccumulatedChanges` (`push-accumulated.ts:87`) — `FanIn` (unlike
//!   `UnionFanIn`) passes `identity` for both the merge and add-empty-
//!   relationship hooks, so the collapse keeps the first-seen change per type
//!   and does not touch relationships.
//!
//! [`FanOut`]: crate::ivm::fan_out::FanOut

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::data::make_comparator;
use crate::ivm::fan_out::FanOut;
use crate::ivm::operator::{
    Change, FetchRequest, Input, InputBase, Node, Operator, Output, SourceSchema, Stream,
    ThrowOutput,
};

/// The four operator-level change types, mirroring upstream `ChangeType`. Used
/// to bucket accumulated pushes by kind when collapsing them in
/// [`push_accumulated_changes`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeTag {
    Add,
    Remove,
    Edit,
    Child,
}

/// The `ChangeTag` of a change — port of reading `change[ChangeIndex.TYPE]`.
pub fn change_tag(change: &Change) -> ChangeTag {
    match change {
        Change::Add(_) => ChangeTag::Add,
        Change::Remove(_) => ChangeTag::Remove,
        Change::Edit { .. } => ChangeTag::Edit,
        Change::Child { .. } => ChangeTag::Child,
    }
}

/// The primary node a change is "about" (its parent node for every variant).
fn primary_node(change: &Change) -> &Node {
    match change {
        Change::Add(node) | Change::Remove(node) => node,
        Change::Edit { node, .. } => node,
        Change::Child { node, .. } => node,
    }
}

/// The `FanIn` operator. Owns the branch inputs (for the fetch-side merge) and
/// accumulates branch pushes until the paired [`FanOut`] reports it has pushed
/// the change to every branch.
pub struct FanIn {
    inputs: Vec<Rc<dyn Input>>,
    schema: SourceSchema,
    output: RefCell<Rc<dyn Output>>,
    accumulated: RefCell<Vec<Change>>,
}

impl FanIn {
    /// Builds a `FanIn` merging `inputs` (the branches), taking its schema from
    /// `fan_out` (the fork point upstream of every branch) — port of
    /// `FanIn`'s constructor (`fan-in.ts:36`). The downstream output starts as
    /// [`ThrowOutput`] until [`Input::set_output`] wires it.
    pub fn new(fan_out: &Rc<FanOut>, inputs: Vec<Rc<dyn Input>>) -> Rc<Self> {
        Rc::new(FanIn {
            inputs,
            schema: fan_out.get_schema(),
            output: RefCell::new(Rc::new(ThrowOutput)),
            accumulated: RefCell::new(Vec::new()),
        })
    }

    /// Called by the paired [`FanOut`] once it has pushed a change to every
    /// branch: collapses the accumulated branch pushes to a single change and
    /// forwards it downstream. Port of `fanOutDonePushingToAllBranches`
    /// (`fan-in.ts:76`).
    pub fn fan_out_done_pushing_to_all_branches(&self, fan_out_type: ChangeTag) {
        let accumulated = std::mem::take(&mut *self.accumulated.borrow_mut());
        if self.inputs.is_empty() {
            debug_assert!(
                accumulated.is_empty(),
                "If there are no inputs then fan-in should not receive any pushes."
            );
            return;
        }
        let output = self.output.borrow().clone();
        push_accumulated_changes(accumulated, &output, self, fan_out_type);
    }
}

impl InputBase for FanIn {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&self) {
        for input in &self.inputs {
            input.destroy();
        }
    }
}

impl Input for FanIn {
    fn set_output(&self, output: Rc<dyn Output>) {
        *self.output.borrow_mut() = output;
    }

    /// Merges the branch fetch streams, de-duplicating rows that compare equal
    /// under the schema's sort — port of `mergeFetches` (`union-fan-in.ts:224`).
    /// Each branch fetch is already sorted, so a k-way merge collapses to a
    /// stable sort of the concatenation followed by a consecutive-equal dedup
    /// (keeping the first occurrence), which yields the identical ordered,
    /// de-duplicated stream.
    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node> {
        let base = make_comparator(&self.schema.sort, false);
        let reverse = req.reverse;
        let compare = move |a: &Node, b: &Node| {
            if reverse {
                base(&b.row, &a.row)
            } else {
                base(&a.row, &b.row)
            }
        };

        let mut merged: Vec<Node> = self
            .inputs
            .iter()
            .flat_map(|input| input.fetch(req))
            .collect();
        merged.sort_by(|a, b| compare(a, b));
        merged.dedup_by(|a, b| compare(a, b) == std::cmp::Ordering::Equal);
        Box::new(merged.into_iter())
    }
}

impl Output for FanIn {
    /// Accumulates a branch's push; the collapse and downstream forward happen
    /// in [`FanIn::fan_out_done_pushing_to_all_branches`]. Port of `FanIn.push`
    /// (`fan-in.ts:71`).
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        self.accumulated.borrow_mut().push(change);
    }
}

impl Operator for FanIn {}

/// Collapses the branch pushes accumulated for one fan-out change down to the
/// single change to forward downstream, then pushes it. Port of
/// `pushAccumulatedChanges` (`push-accumulated.ts:87`) for the `FanIn` (as
/// opposed to `UnionFanIn`) call site, which passes `identity` for both the
/// merge and add-empty-relationship hooks: duplicates of the same type keep the
/// first-seen change and relationships are left untouched.
///
/// The invariants (`push-accumulated.ts:62`): an `add` in yields only `add`s
/// out (one row, so one add); a `remove` in yields only `remove`s; an `edit`
/// can yield `add`/`remove`/`edit` (recombined into one `edit`); a `child` can
/// yield `add`/`remove`/`child` (a preserved `child` takes precedence).
fn push_accumulated_changes(
    accumulated: Vec<Change>,
    output: &Rc<dyn Output>,
    pusher: &dyn InputBase,
    fan_out_type: ChangeTag,
) {
    if accumulated.is_empty() {
        // No branch forwarded the change (e.g. no filter matched in any fork).
        return;
    }

    // Collapse to at most one change per type, keeping the first seen (the
    // `identity` merge upstream passes for a plain FanIn).
    let mut candidates: Vec<(ChangeTag, Change)> = Vec::new();
    for change in accumulated {
        let tag = change_tag(&change);
        if !candidates.iter().any(|(t, _)| *t == tag) {
            candidates.push((tag, change));
        }
    }
    let take = |tag: ChangeTag| -> Option<Change> {
        candidates
            .iter()
            .find(|(t, _)| *t == tag)
            .map(|(_, c)| c.clone())
    };

    match fan_out_type {
        ChangeTag::Remove => {
            debug_assert!(
                candidates.len() == 1 && candidates[0].0 == ChangeTag::Remove,
                "Fan-in:remove expected all removes"
            );
            if let Some(change) = take(ChangeTag::Remove) {
                output.push(change, pusher);
            }
        }
        ChangeTag::Add => {
            debug_assert!(
                candidates.len() == 1 && candidates[0].0 == ChangeTag::Add,
                "Fan-in:add expected all adds"
            );
            if let Some(change) = take(ChangeTag::Add) {
                output.push(change, pusher);
            }
        }
        ChangeTag::Edit => {
            debug_assert!(
                candidates.iter().all(|(t, _)| matches!(
                    t,
                    ChangeTag::Add | ChangeTag::Remove | ChangeTag::Edit
                )),
                "Fan-in:edit expected all adds, removes, or edits"
            );
            let add = take(ChangeTag::Add);
            let remove = take(ChangeTag::Remove);
            let edit = take(ChangeTag::Edit);
            if let Some(edit) = edit {
                // An `edit` supersedes any `add`/`remove` (it already represents
                // both). With identity relationships nothing further to merge.
                output.push(edit, pusher);
            } else if let (Some(add), Some(remove)) = (&add, &remove) {
                // One branch turned the edit into an add, another into a remove:
                // recombine into a single edit.
                output.push(
                    Change::Edit {
                        node: primary_node(add).clone(),
                        old_node: primary_node(remove).clone(),
                    },
                    pusher,
                );
            } else if let Some(change) = add.or(remove) {
                output.push(change, pusher);
            }
        }
        ChangeTag::Child => {
            debug_assert!(
                candidates.iter().all(|(t, _)| matches!(
                    t,
                    ChangeTag::Add | ChangeTag::Remove | ChangeTag::Child
                )),
                "Fan-in:child expected all adds, removes, or children"
            );
            if let Some(child) = take(ChangeTag::Child) {
                // A preserved child change takes precedence over all others.
                output.push(child, pusher);
            } else if let Some(change) = take(ChangeTag::Add).or_else(|| take(ChangeTag::Remove)) {
                output.push(change, pusher);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::data::Row;
    use crate::ivm::fan_out::FanOut;
    use crate::ivm::filter::GraphFilter;
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::Direction;
    use zero_cache_shared::bigint_json::JsonValue;

    fn row(a: i64, b: bool) -> Row {
        vec![
            ("a".into(), JsonValue::Number(a as f64)),
            ("b".into(), JsonValue::Bool(b)),
        ]
    }

    fn schema() -> SourceSchema {
        SourceSchema {
            table_name: "table".into(),
            primary_key: vec!["a".into(), "b".into()],
            sort: vec![("a".into(), Direction::Asc), ("b".into(), Direction::Asc)],
            relationships: BTreeMap::new(),
        }
    }

    /// In-memory `Input` yielding a fixed, pre-sorted set of leaf nodes.
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
            schema()
        }
        fn destroy(&self) {}
    }
    impl Input for VecInput {
        fn set_output(&self, _output: Rc<dyn Output>) {}
        fn fetch<'a>(&'a self, _req: &FetchRequest) -> Stream<'a, Node> {
            Box::new(self.rows.iter().cloned().map(Node::new))
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

    fn is_a(row: &Row) -> bool {
        matches!(row.iter().find(|(k, _)| k == "a").map(|(_, v)| v), Some(JsonValue::Number(n)) if *n == 1.0)
    }

    // ---- fetch merge + dedup (port of `fan-in fetch`) ----

    #[test]
    fn fan_in_fetch_merges_and_deduplicates() {
        // Rows (a,b): (0,false),(0,true),(1,false),(1,true).
        let all = VecInput::new(vec![
            row(0, false),
            row(0, true),
            row(1, false),
            row(1, true),
        ]);
        let fan_out = FanOut::new(all);

        // filter1: a==1 -> (1,false),(1,true)
        let filter1 = GraphFilter::new(fan_out.clone() as Rc<dyn Input>, is_a) as Rc<dyn Input>;
        // filter2: b==true -> (0,true),(1,true)
        let filter2 = GraphFilter::new(fan_out.clone() as Rc<dyn Input>, |r: &Row| {
            matches!(
                r.iter().find(|(k, _)| k == "b").map(|(_, v)| v),
                Some(JsonValue::Bool(true))
            )
        }) as Rc<dyn Input>;
        // filter3: a==1 && b==false -> (1,false) [duplicates filter1]
        let filter3 = GraphFilter::new(fan_out.clone() as Rc<dyn Input>, |r: &Row| {
            let a = matches!(r.iter().find(|(k, _)| k == "a").map(|(_, v)| v), Some(JsonValue::Number(n)) if *n == 1.0);
            let b = matches!(
                r.iter().find(|(k, _)| k == "b").map(|(_, v)| v),
                Some(JsonValue::Bool(false))
            );
            a && b
        }) as Rc<dyn Input>;

        let fan_in = FanIn::new(&fan_out, vec![filter1, filter2, filter3]);
        let rows: Vec<Node> = fan_in.fetch(&FetchRequest::default()).collect();
        // Union of {(1,false),(1,true)} ∪ {(0,true),(1,true)} ∪ {(1,false)}
        // de-duplicated and sorted: (0,true),(1,false),(1,true).
        assert_eq!(
            rows,
            vec![
                Node::new(row(0, true)),
                Node::new(row(1, false)),
                Node::new(row(1, true)),
            ]
        );
    }

    // ---- push accumulation + collapse (port of "does not duplicate pushes") ----

    #[test]
    fn fan_out_fan_in_pairing_does_not_duplicate_pushes() {
        let source = VecInput::new(vec![]);
        let fan_out = FanOut::new(source);
        let filter1 = GraphFilter::new(fan_out.clone() as Rc<dyn Input>, |_r: &Row| true);
        let filter2 = GraphFilter::new(fan_out.clone() as Rc<dyn Input>, |_r: &Row| true);
        let filter3 = GraphFilter::new(fan_out.clone() as Rc<dyn Input>, |_r: &Row| true);
        // Wire the push graph: fan-out broadcasts to each filter, each filter
        // pushes to fan-in.
        fan_out.set_output(filter1.clone());
        fan_out.set_output(filter2.clone());
        fan_out.set_output(filter3.clone());
        let fan_in = FanIn::new(
            &fan_out,
            vec![
                filter1.clone() as Rc<dyn Input>,
                filter2.clone() as Rc<dyn Input>,
                filter3.clone() as Rc<dyn Input>,
            ],
        );
        filter1.set_output(fan_in.clone());
        filter2.set_output(fan_in.clone());
        filter3.set_output(fan_in.clone());
        fan_out.set_fan_in(&fan_in);
        let spy = SpyOutput::new();
        fan_in.set_output(spy.clone());

        // Three adds, each of which reaches fan-in three times (once per filter)
        // but must be forwarded once.
        for a in [1, 2, 3] {
            fan_out.push(Change::Add(Node::new(row(a, false))), &*fan_out);
        }

        assert_eq!(
            *spy.received.borrow(),
            vec![
                Change::Add(Node::new(row(1, false))),
                Change::Add(Node::new(row(2, false))),
                Change::Add(Node::new(row(3, false))),
            ]
        );
    }

    #[test]
    fn empty_fan_in_forwards_nothing() {
        let source = VecInput::new(vec![]);
        let fan_out = FanOut::new(source);
        let fan_in = FanIn::new(&fan_out, vec![]);
        fan_out.set_fan_in(&fan_in);
        let spy = SpyOutput::new();
        fan_in.set_output(spy.clone());
        // No branches wired; a fan-out push reaches no branch, so fan-in has
        // nothing to forward.
        fan_out.push(Change::Add(Node::new(row(1, false))), &*fan_out);
        assert!(spy.received.borrow().is_empty());
    }
}
