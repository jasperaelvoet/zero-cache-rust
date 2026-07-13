//! Port of `zql/src/ivm/fan-out.ts` — the `FanOut` operator that forks one
//! stream into N branches (the arms of an `OR`), to be merged back together by
//! a paired [`crate::ivm::fan_in::FanIn`].
//!
//! **How it maps to upstream.** Upstream's `FanOut` is a `FilterOperator`
//! (`beginFilter`/`endFilter`/`filter`) whose branch registrations arrive via
//! `setFilterOutput`. This port has no `FilterOperator` sub-protocol (see
//! [`crate::ivm::filter`]'s module doc), so `FanOut` is a plain [`Operator`]:
//!
//! - **pull (`fetch`)** — a pass-through to its upstream input. Each branch
//!   fetches *through* the `FanOut` (holding it as its own `Rc<dyn Input>`), so
//!   the source is re-fetched once per branch — the same re-fetch the
//!   [`crate::ivm::join_input::JoinInput`] already performs per parent. (The
//!   `FilterOperator` sub-protocol upstream avoids this by pushing one shared
//!   node through all branches; the pull model here does not.)
//! - **push** — broadcasts an incoming change to every registered branch output
//!   (accumulated via [`Input::set_output`], mirroring upstream's `#outputs`),
//!   then signals the paired [`FanIn`] that the change was delivered to all
//!   branches so it can collapse and forward the de-duplicated result. Port of
//!   `FanOut.push` (`fan-out.ts:74`).
//!
//! [`FanIn`]: crate::ivm::fan_in::FanIn

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use crate::ivm::fan_in::{change_tag, FanIn};
use crate::ivm::operator::{
    Change, FetchRequest, Input, InputBase, Node, Operator, Output, SourceSchema, Stream,
};

/// The `FanOut` operator. Broadcasts pushes to every branch and delegates
/// fetches straight through to its upstream input.
pub struct FanOut {
    input: Rc<dyn Input>,
    outputs: RefCell<Vec<Rc<dyn Output>>>,
    /// Back-reference to the paired fan-in, held [`Weak`] so the
    /// fan-in → branches → fan-out → fan-in cycle does not leak the transient
    /// graph. Upstream relies on GC; this port breaks the cycle explicitly.
    fan_in: RefCell<Weak<FanIn>>,
}

impl FanOut {
    /// Builds a `FanOut` over `input`. Port of `FanOut`'s constructor
    /// (`fan-out.ts:23`); the upstream `input.setFilterOutput(this)` wiring is
    /// the caller's responsibility here (the push graph is wired separately from
    /// construction — see [`Input::set_output`]).
    pub fn new(input: Rc<dyn Input>) -> Rc<Self> {
        Rc::new(FanOut {
            input,
            outputs: RefCell::new(Vec::new()),
            fan_in: RefCell::new(Weak::new()),
        })
    }

    /// Registers the paired [`FanIn`] so [`FanOut::push`] can notify it once a
    /// change has been broadcast to every branch. Port of `setFanIn`
    /// (`fan-out.ts:28`).
    pub fn set_fan_in(&self, fan_in: &Rc<FanIn>) {
        *self.fan_in.borrow_mut() = Rc::downgrade(fan_in);
    }
}

impl InputBase for FanOut {
    fn get_schema(&self) -> SourceSchema {
        self.input.get_schema()
    }

    fn destroy(&self) {
        // Drop strong back-refs to our downstream outputs before cascading —
        // otherwise the `input.set_output(self)` cycle leaks the transient
        // hydration graph and its source's shared replica handle. See
        // `GraphFilter::destroy` / `Snapshotter::with_current_shared`.
        self.outputs.borrow_mut().clear();
        self.input.destroy();
    }
}

impl Input for FanOut {
    /// Registers one branch as an output. Unlike a single-output operator, this
    /// ACCUMULATES — a `FanOut` fans one input out to many branches. Port of
    /// `setFilterOutput` pushing to `#outputs` (`fan-out.ts:32`).
    fn set_output(&self, output: Rc<dyn Output>) {
        self.outputs.borrow_mut().push(output);
    }

    /// Pass-through fetch: each branch pulls the source through the `FanOut`.
    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node> {
        self.input.fetch(req)
    }
}

impl Output for FanOut {
    /// Broadcasts `change` to every branch, then tells the paired [`FanIn`] the
    /// change reached all branches so it can collapse the accumulated branch
    /// pushes and forward the de-duplicated result. Port of `FanOut.push`
    /// (`fan-out.ts:74`).
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        let outputs = self.outputs.borrow().clone();
        for output in &outputs {
            output.push(change.clone(), self);
        }
        let fan_in = self
            .fan_in
            .borrow()
            .upgrade()
            .expect("fan-out must have a corresponding fan-in set!");
        fan_in.fan_out_done_pushing_to_all_branches(change_tag(&change));
    }
}

impl Operator for FanOut {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::data::Row;
    use crate::ivm::fan_in::FanIn;
    use std::collections::BTreeMap;
    use zero_cache_protocol::ast::Direction;
    use zero_cache_shared::bigint_json::JsonValue;

    fn row(a: i64, b: &str) -> Row {
        vec![
            ("a".into(), JsonValue::Number(a as f64)),
            ("b".into(), JsonValue::String(b.into())),
        ]
    }

    fn schema() -> SourceSchema {
        SourceSchema {
            table_name: "table".into(),
            primary_key: vec!["a".into()],
            sort: vec![("a".into(), Direction::Asc)],
            relationships: BTreeMap::new(),
        }
    }

    struct VecInput {
        rows: Vec<Row>,
        destroyed: RefCell<bool>,
    }
    impl VecInput {
        fn new(rows: Vec<Row>) -> Rc<Self> {
            Rc::new(VecInput {
                rows,
                destroyed: RefCell::new(false),
            })
        }
    }
    impl InputBase for VecInput {
        fn get_schema(&self) -> SourceSchema {
            schema()
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

    /// Port of `fan-out pushes along all paths`: a change is broadcast to every
    /// registered branch output.
    #[test]
    fn fan_out_pushes_along_all_paths() {
        let source = VecInput::new(vec![]);
        let fan_out = FanOut::new(source);
        let catch1 = SpyOutput::new();
        let catch2 = SpyOutput::new();
        let catch3 = SpyOutput::new();
        fan_out.set_output(catch1.clone());
        fan_out.set_output(catch2.clone());
        fan_out.set_output(catch3.clone());
        // Dummy fan-in so the invariant in fan-out push is satisfied.
        let fan_in = FanIn::new(&fan_out, vec![]);
        fan_out.set_fan_in(&fan_in);

        let add = Change::Add(Node::new(row(1, "foo")));
        let edit = Change::Edit {
            node: Node::new(row(1, "bar")),
            old_node: Node::new(row(1, "foo")),
        };
        let remove = Change::Remove(Node::new(row(1, "bar")));
        fan_out.push(add.clone(), &*fan_out);
        fan_out.push(edit.clone(), &*fan_out);
        fan_out.push(remove.clone(), &*fan_out);

        let expected = vec![add, edit, remove];
        assert_eq!(*catch1.received.borrow(), expected);
        assert_eq!(*catch2.received.borrow(), expected);
        assert_eq!(*catch3.received.borrow(), expected);
    }

    #[test]
    fn fetch_passes_through_to_input() {
        let source = VecInput::new(vec![row(1, "foo"), row(2, "bar")]);
        let fan_out = FanOut::new(source);
        let rows: Vec<Node> = fan_out.fetch(&FetchRequest::default()).collect();
        assert_eq!(
            rows,
            vec![Node::new(row(1, "foo")), Node::new(row(2, "bar"))]
        );
    }

    #[test]
    fn destroy_cascades_to_input() {
        let source = VecInput::new(vec![]);
        let fan_out = FanOut::new(source.clone());
        fan_out.destroy();
        assert!(*source.destroyed.borrow());
    }
}
