//! A pull/fetch-shaped join operator, the piece `build_pipeline` needs to
//! assemble `related` hops and `whereExists` (`EXISTS`/`NOT EXISTS`) into a
//! real operator graph — the counterpart of upstream `zql/src/ivm/join.ts`'s
//! `Join` for the *hydration* (pull) direction this port's driver actually
//! drives.
//!
//! **Why not the existing [`crate::ivm::join::Join`]?** That one is a
//! push-only operator built from two concrete `TableSource`s (it reacts to a
//! child `SourceChange` and fans a `Change::Child` out to registered
//! `Output`s — see its module doc). `build_pipeline` composes
//! `Rc<dyn Input>`s and the driver drains the result with a single
//! `root.fetch(..)` (the whole graph is transient and dropped afterwards —
//! see `pipeline_driver.rs`'s `Pipeline` note). So the join `build_pipeline`
//! needs is a pull-shaped [`Input`]: on `fetch`, for each parent node it
//! fetches the correlated child rows and populates
//! `Node.relationships[relationship_name]` — exactly what upstream's
//! `Join.fetch` yields, and exactly what the [`crate::ivm::exists::Exists`]
//! operator sitting on top of it reads. This is the same correlation →
//! child-constraint mapping [`crate::ivm::join::fetch_joined`] already
//! performs, generalized from `TableSource` to any upstream `Rc<dyn Input>`
//! (a recursively-built child pipeline, so nested `related`/`where` on the
//! child are honored).
//!
//! **Push, increment 5.** `JoinInput` is now a real [`Output`] of BOTH its
//! parent and its child, matching upstream `Join`'s constructor wiring
//! (`parent.setOutput({push: c => this.#pushParent(c)})` and
//! `child.setOutput({push: c => this.#pushChild(c)})`, join.ts:98-103). It
//! registers via two adapter outputs ([`ParentOutput`]/[`ChildOutput`], held
//! [`std::rc::Weak`] so the parent/child → adapter → join back-edge does not
//! leak the transient graph) so a single `Output::push` can distinguish which
//! upstream fired:
//!
//! - **Parent `Add`/`Remove`/`Edit`/`Child`** → re-emit downstream with the
//!   child rows fetched for that parent attached (`#pushParent` /
//!   `#processParentNode`, join.ts:129-193). The correlated children are
//!   `self.child.fetch(constraint)`ed — when the child is a mid-push
//!   [`crate::ivm::operator::Source`] with a pending-change overlay, that fetch
//!   already reflects the in-flight change (upstream sets DBs only after all
//!   pushes; this port overlays — see `zero-cache-sqlite`'s `SqliteSource`).
//! - **Child `Add`/`Remove`/`Edit`/`Child`** → `build_join_constraint` from the
//!   child row's `child_key` → `parent_key`, `self.parent.fetch({constraint})`,
//!   and for each matched parent emit a [`Change::Child`] naming this
//!   relationship (`make_child_change`) carrying the original child change —
//!   upstream `#pushChild` / `#pushChildChange` (join.ts:195-250).
//!
//! The pull (`fetch`) direction is unchanged from before, byte-for-byte, so
//! every existing `build_pipeline` fetch shape (related hops / `whereExists`)
//! is preserved.
//!
//! **Scope / deviation.** Not ported: upstream's *self-join* mid-push overlay
//! inside `#processParentNode` (the `#inprogressChildChange` +
//! `generateWithOverlay` in join-utils.ts:252-302), which handles a parent and
//! child that are the SAME source re-fetched mid-child-push. The port's
//! source-level overlay (`SqliteSource`) covers the standard distinct-source
//! case the corpus needs; a genuine self-join would need the join-level overlay
//! layered on top. Also not ported: the `rowEqualsForCompoundKey` asserts on
//! `Edit` (that an edit cannot change the join key) — the port trusts the
//! upstream invariant rather than re-checking.

use std::cell::RefCell;
use std::rc::{Rc, Weak};

use zero_cache_protocol::ast::CompoundKey;

use crate::ivm::constraint::Constraint;
use crate::ivm::data::{Row, Value};
use crate::ivm::operator::{
    make_child_change, Change, FetchRequest, Input, InputBase, Node, Output, SourceSchema, Stream,
    ThrowOutput,
};

fn get(row: &Row, field: &str) -> Value {
    row.iter()
        .find(|(k, _)| k == field)
        .map(|(_, v)| v.clone())
        .unwrap_or(Value::Null)
}

/// The primary node a change is "about" — the (parent- or child-) node for
/// `Add`/`Remove`/`Edit`/`Child`. Its `row` identifies the join key.
fn primary_node(change: &Change) -> &Node {
    match change {
        Change::Add(node) | Change::Remove(node) => node,
        Change::Edit { node, .. } => node,
        Change::Child { node, .. } => node,
    }
}

/// A pull-shaped join: fetches `parent`, and for each parent node fetches the
/// `child` rows correlated to it (via `parent_key`/`child_key`), storing them
/// under `Node.relationships[relationship_name]`. Port of the pull half of
/// `Join` (`join.ts`) — see the module doc for the split from the push-based
/// [`crate::ivm::join::Join`].
pub struct JoinInput {
    parent: Rc<dyn Input>,
    child: Rc<dyn Input>,
    parent_key: CompoundKey,
    child_key: CompoundKey,
    relationship_name: String,
    /// The single downstream consumer, wired by [`Input::set_output`]. Starts
    /// as [`ThrowOutput`] until a downstream (an [`crate::ivm::exists::Exists`],
    /// a [`crate::ivm::collector::Collector`], another `JoinInput`, …) is set.
    output: RefCell<Rc<dyn Output>>,
}

/// Adapter registered as the parent input's [`Output`]; forwards to
/// [`JoinInput::push_parent`]. Held via [`Weak`] to break the
/// parent → output → join back-edge.
struct ParentOutput {
    join: Weak<JoinInput>,
}

impl Output for ParentOutput {
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        if let Some(join) = self.join.upgrade() {
            join.push_parent(change);
        }
    }
}

/// Adapter registered as the child input's [`Output`]; forwards to
/// [`JoinInput::push_child`]. Held via [`Weak`] to break the
/// child → output → join back-edge.
struct ChildOutput {
    join: Weak<JoinInput>,
}

impl Output for ChildOutput {
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        if let Some(join) = self.join.upgrade() {
            join.push_child(change);
        }
    }
}

impl JoinInput {
    pub fn new(
        parent: Rc<dyn Input>,
        child: Rc<dyn Input>,
        parent_key: CompoundKey,
        child_key: CompoundKey,
        relationship_name: impl Into<String>,
    ) -> Rc<Self> {
        let join = Rc::new(JoinInput {
            parent,
            child,
            parent_key,
            child_key,
            relationship_name: relationship_name.into(),
            output: RefCell::new(Rc::new(ThrowOutput)),
        });
        // Register as an `Output` of both inputs so their pushes reach this
        // join (upstream `parent.setOutput`/`child.setOutput`, join.ts:98-103).
        // A fetch-only input (`SourceInput`, `TestSource`, another fetch-only
        // `JoinInput`) simply ignores these; a push-capable one now routes to
        // `push_parent`/`push_child`.
        join.parent.set_output(Rc::new(ParentOutput {
            join: Rc::downgrade(&join),
        }));
        join.child.set_output(Rc::new(ChildOutput {
            join: Rc::downgrade(&join),
        }));
        join
    }

    /// The child-side [`Constraint`] for one parent row: each
    /// `child_key[i]` column must equal the parent row's `parent_key[i]`
    /// value. Port of the correlation-to-filter mapping `Join` applies per
    /// parent row upstream.
    fn child_constraint(&self, parent_row: &Row) -> Constraint {
        self.parent_key
            .iter()
            .zip(&self.child_key)
            .map(|(parent_col, child_col)| (child_col.clone(), get(parent_row, parent_col)))
            .collect()
    }

    /// Port of `buildJoinConstraint(sourceRow, sourceKey, targetKey)`
    /// (join-utils.ts:238): maps the child row's `child_key` values onto the
    /// `parent_key` columns, returning `None` if any value is `null` (a null
    /// foreign key matches no parent — upstream returns `undefined`).
    fn parent_constraint(&self, child_row: &Row) -> Option<Constraint> {
        let mut constraint: Constraint = Vec::with_capacity(self.parent_key.len());
        for (child_col, parent_col) in self.child_key.iter().zip(&self.parent_key) {
            let value = get(child_row, child_col);
            if value == Value::Null {
                return None;
            }
            constraint.push((parent_col.clone(), value));
        }
        Some(constraint)
    }

    /// Port of `#processParentNode` (join.ts:252): returns `parent_node` with
    /// its `relationship_name` relationship populated by fetching the correlated
    /// child rows. Any relationships the parent already carries (nested joins)
    /// are preserved.
    fn process_parent_node(&self, mut parent_node: Node) -> Node {
        let constraint = self.child_constraint(&parent_node.row);
        let children: Vec<Node> = self
            .child
            .fetch(&FetchRequest {
                constraint: Some(constraint),
                ..Default::default()
            })
            .collect();
        parent_node
            .relationships
            .insert(self.relationship_name.clone(), children);
        parent_node
    }

    /// Port of `#pushParent` (join.ts:129): re-emits a parent change downstream
    /// with the child rows attached to each parent node.
    fn push_parent(&self, change: Change) {
        let output = self.output.borrow().clone();
        let processed = match change {
            Change::Add(node) => Change::Add(self.process_parent_node(node)),
            Change::Remove(node) => Change::Remove(self.process_parent_node(node)),
            Change::Child { node, child } => Change::Child {
                node: self.process_parent_node(node),
                child,
            },
            Change::Edit { node, old_node } => Change::Edit {
                node: self.process_parent_node(node),
                old_node: self.process_parent_node(old_node),
            },
        };
        output.push(processed, self);
    }

    /// Port of `#pushChild` / `#pushChildChange` (join.ts:195): finds the
    /// parent rows correlated to the changed child row and emits, for each, a
    /// [`Change::Child`] naming this relationship and carrying the original
    /// child change.
    fn push_child(&self, change: Change) {
        let child_row = primary_node(&change).row.clone();
        let Some(constraint) = self.parent_constraint(&child_row) else {
            return;
        };
        let output = self.output.borrow().clone();
        let parents: Vec<Node> = self
            .parent
            .fetch(&FetchRequest {
                constraint: Some(constraint),
                ..Default::default()
            })
            .collect();
        for parent_node in parents {
            let processed = self.process_parent_node(parent_node);
            let child_change =
                make_child_change(processed, self.relationship_name.clone(), change.clone());
            output.push(child_change, self);
        }
    }
}

impl InputBase for JoinInput {
    fn get_schema(&self) -> SourceSchema {
        // Report the parent schema with the child relationship attached, so a
        // downstream `Exists` (or a schema consumer) can see the relationship
        // this join populates. Port of `Join.getSchema` adding
        // `relationships[relationshipName]` (`join.ts`).
        let mut schema = self.parent.get_schema();
        schema.relationships.insert(
            self.relationship_name.clone(),
            Box::new(self.child.get_schema()),
        );
        schema
    }

    fn destroy(&self) {
        // Drop the strong back-ref to our downstream before cascading. The
        // parent/child → JoinInput back-edges are already `Weak`, but
        // `JoinInput.output` is a strong `Rc` to the downstream operator whose
        // `input` is this JoinInput — a cycle that would otherwise leak the
        // transient hydration graph and the shared replica handle its parent/
        // child sources hold. See `GraphFilter::destroy` /
        // `Snapshotter::with_current_shared`.
        *self.output.borrow_mut() = Rc::new(ThrowOutput);
        self.parent.destroy();
        self.child.destroy();
    }
}

impl Input for JoinInput {
    /// Wires the single downstream consumer. Previously a no-op (the graph was
    /// fetch-only); now that `JoinInput` pushes (see module doc), it stores the
    /// output so `push_parent`/`push_child` can forward — port of
    /// `Join.setOutput` (join.ts:111).
    fn set_output(&self, output: Rc<dyn Output>) {
        *self.output.borrow_mut() = output;
    }

    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node> {
        Box::new(self.parent.fetch(req).map(move |mut parent_node| {
            let constraint = self.child_constraint(&parent_node.row);
            let children: Vec<Node> = self
                .child
                .fetch(&FetchRequest {
                    constraint: Some(constraint),
                    ..Default::default()
                })
                .collect();
            parent_node
                .relationships
                .insert(self.relationship_name.clone(), children);
            parent_node
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::make_source_change_add;
    use crate::ivm::table_source::TableSource;
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

    fn issues(ids: &[i64]) -> Rc<TableSource> {
        let mut s = TableSource::new(
            "issue",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        for id in ids {
            s.push(make_source_change_add(issue(*id)));
        }
        Rc::new(s)
    }
    fn comments(rows: &[(i64, i64)]) -> Rc<TableSource> {
        let mut s = TableSource::new(
            "comment",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        for (id, issue_id) in rows {
            s.push(make_source_change_add(comment(*id, *issue_id)));
        }
        Rc::new(s)
    }

    /// `TableSource` has an inherent `fetch` but does not implement the
    /// `Input` trait; wrap it so it can be a `Rc<dyn Input>` upstream edge.
    struct SourceInput(Rc<TableSource>);
    impl InputBase for SourceInput {
        fn get_schema(&self) -> SourceSchema {
            self.0.schema().clone()
        }
        fn destroy(&self) {}
    }
    impl Input for SourceInput {
        fn set_output(&self, _output: Rc<dyn Output>) {}
        fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node> {
            Box::new(self.0.fetch(req).collect::<Vec<_>>().into_iter())
        }
    }
    fn input(source: Rc<TableSource>) -> Rc<dyn Input> {
        Rc::new(SourceInput(source)) as Rc<dyn Input>
    }

    #[test]
    fn populates_relationship_with_matching_children() {
        let join = JoinInput::new(
            input(issues(&[1, 2])),
            input(comments(&[(10, 1), (11, 1), (12, 2)])),
            vec!["id".into()],
            vec!["issueID".into()],
            "comments",
        );
        let nodes: Vec<Node> = join.fetch(&FetchRequest::default()).collect();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].row, issue(1));
        assert_eq!(nodes[0].relationships["comments"].len(), 2);
        assert_eq!(nodes[1].row, issue(2));
        assert_eq!(nodes[1].relationships["comments"].len(), 1);
    }

    #[test]
    fn parent_with_no_children_gets_empty_relationship() {
        let join = JoinInput::new(
            input(issues(&[1])),
            input(comments(&[(10, 999)])),
            vec!["id".into()],
            vec!["issueID".into()],
            "comments",
        );
        let nodes: Vec<Node> = join.fetch(&FetchRequest::default()).collect();
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].relationships["comments"].is_empty());
    }

    #[test]
    fn get_schema_attaches_child_relationship() {
        let join = JoinInput::new(
            input(issues(&[])),
            input(comments(&[])),
            vec!["id".into()],
            vec!["issueID".into()],
            "comments",
        );
        assert!(join.get_schema().relationships.contains_key("comments"));
    }

    // ---- push (increment 5) ----
    //
    // Ports of `join.push.test.ts`'s one:many scenarios, driven through
    // `TestSource`s (which mutate their backing rows before forwarding, so a
    // fetch during the push sees post-change state — the same net effect the
    // `SqliteSource` overlay produces for the replica-backed sources; those
    // overlay-dependent variants are proven in `zero-cache-sqlite`).

    use crate::ivm::change::{make_source_change_edit, make_source_change_remove};
    use crate::ivm::operator::{make_child_change, Change};
    use crate::ivm::test_input::{SpyOutput, TestSource};

    fn test_source(table: &str) -> Rc<TestSource> {
        TestSource::new(
            table,
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        )
    }

    /// Builds a wired `issue_src → JoinInput(comments) → SpyOutput` graph over the
    /// two given sources.
    fn wired_join(
        issue_src: &Rc<TestSource>,
        comment_src: &Rc<TestSource>,
    ) -> (Rc<JoinInput>, Rc<SpyOutput>) {
        let join = JoinInput::new(
            issue_src.clone() as Rc<dyn Input>,
            comment_src.clone() as Rc<dyn Input>,
            vec!["id".into()],
            vec!["issueID".into()],
            "comments",
        );
        let spy = SpyOutput::new();
        join.set_output(spy.clone());
        (join, spy)
    }

    /// `fetch one child, add parent` — a parent add re-emits an `Add` carrying
    /// the parent's fetched children.
    #[test]
    fn push_parent_add_attaches_children() {
        let issue_src = test_source("issue_src");
        let comment_src = test_source("comment_src");
        comment_src.push_change(make_source_change_add(comment(10, 1)));
        let (_join, spy) = wired_join(&issue_src, &comment_src);

        issue_src.push_and_forward(make_source_change_add(issue(1)));

        let mut expected = Node::new(issue(1));
        expected
            .relationships
            .insert("comments".into(), vec![Node::new(comment(10, 1))]);
        assert_eq!(*spy.received.borrow(), vec![Change::Add(expected)]);
    }

    /// `fetch one child, add wrong parent` — a parent whose join key matches no
    /// child still re-emits, with an empty relationship.
    #[test]
    fn push_parent_add_wrong_parent_empty_relationship() {
        let issue_src = test_source("issue_src");
        let comment_src = test_source("comment_src");
        comment_src.push_change(make_source_change_add(comment(10, 1)));
        let (_join, spy) = wired_join(&issue_src, &comment_src);

        issue_src.push_and_forward(make_source_change_add(issue(2)));

        let mut expected = Node::new(issue(2));
        expected.relationships.insert("comments".into(), vec![]);
        assert_eq!(*spy.received.borrow(), vec![Change::Add(expected)]);
    }

    /// `fetch one parent, one child, remove parent` — a parent remove re-emits
    /// a `Remove` carrying the children still fetched for it.
    #[test]
    fn push_parent_remove_attaches_children() {
        let issue_src = test_source("issue_src");
        let comment_src = test_source("comment_src");
        issue_src.push_change(make_source_change_add(issue(1)));
        comment_src.push_change(make_source_change_add(comment(10, 1)));
        let (_join, spy) = wired_join(&issue_src, &comment_src);

        issue_src.push_and_forward(make_source_change_remove(issue(1)));

        let mut expected = Node::new(issue(1));
        expected
            .relationships
            .insert("comments".into(), vec![Node::new(comment(10, 1))]);
        assert_eq!(*spy.received.borrow(), vec![Change::Remove(expected)]);
    }

    /// `edit issue_src text` — a parent edit re-emits an `Edit` with children on
    /// both nodes.
    #[test]
    fn push_parent_edit_attaches_children_on_both_nodes() {
        let issue_src = test_source("issue_src");
        let comment_src = test_source("comment_src");
        issue_src.push_change(make_source_change_add(vec![
            ("id".into(), JsonValue::Number(1.0)),
            ("text".into(), JsonValue::String("old".into())),
        ]));
        comment_src.push_change(make_source_change_add(comment(10, 1)));
        let (_join, spy) = wired_join(&issue_src, &comment_src);

        let old_row = vec![
            ("id".into(), JsonValue::Number(1.0)),
            ("text".into(), JsonValue::String("old".into())),
        ];
        let new_row = vec![
            ("id".into(), JsonValue::Number(1.0)),
            ("text".into(), JsonValue::String("new".into())),
        ];
        issue_src.push_and_forward(make_source_change_edit(new_row.clone(), old_row.clone()));

        let received = spy.received.borrow();
        assert_eq!(received.len(), 1);
        let Change::Edit { node, old_node } = &received[0] else {
            panic!("expected Edit");
        };
        assert_eq!(node.row, new_row);
        assert_eq!(old_node.row, old_row);
        assert_eq!(
            node.relationships["comments"],
            vec![Node::new(comment(10, 1))]
        );
        assert_eq!(
            old_node.relationships["comments"],
            vec![Node::new(comment(10, 1))]
        );
    }

    /// `fetch one parent, add child` — a child add fans out a `Change::Child`
    /// per matched parent, carrying the child add.
    #[test]
    fn push_child_add_emits_child_change_per_parent() {
        let issue_src = test_source("issue_src");
        let comment_src = test_source("comment_src");
        issue_src.push_change(make_source_change_add(issue(1)));
        let (_join, spy) = wired_join(&issue_src, &comment_src);

        comment_src.push_and_forward(make_source_change_add(comment(10, 1)));

        let mut parent = Node::new(issue(1));
        parent
            .relationships
            .insert("comments".into(), vec![Node::new(comment(10, 1))]);
        assert_eq!(
            *spy.received.borrow(),
            vec![make_child_change(
                parent,
                "comments",
                Change::Add(Node::new(comment(10, 1)))
            )]
        );
    }

    /// `fetch one parent, add wrong child` — a child whose join key matches no
    /// parent pushes nothing.
    #[test]
    fn push_child_add_wrong_child_emits_nothing() {
        let issue_src = test_source("issue_src");
        let comment_src = test_source("comment_src");
        issue_src.push_change(make_source_change_add(issue(1)));
        let (_join, spy) = wired_join(&issue_src, &comment_src);

        comment_src.push_and_forward(make_source_change_add(comment(10, 2)));

        assert!(spy.received.borrow().is_empty());
    }

    /// A child remove fans a `Change::Child` (remove) to the parent, whose
    /// relationship reflects the post-remove child set (here, empty).
    #[test]
    fn push_child_remove_emits_child_change_with_post_remove_relationship() {
        let issue_src = test_source("issue_src");
        let comment_src = test_source("comment_src");
        issue_src.push_change(make_source_change_add(issue(1)));
        comment_src.push_change(make_source_change_add(comment(10, 1)));
        let (_join, spy) = wired_join(&issue_src, &comment_src);

        comment_src.push_and_forward(make_source_change_remove(comment(10, 1)));

        let mut parent = Node::new(issue(1));
        parent.relationships.insert("comments".into(), vec![]);
        assert_eq!(
            *spy.received.borrow(),
            vec![make_child_change(
                parent,
                "comments",
                Change::Remove(Node::new(comment(10, 1)))
            )]
        );
    }

    /// A child add fans one `Change::Child` to EACH matching parent (many:one
    /// side): two parents share one child join key.
    #[test]
    fn push_child_add_fans_to_every_matching_parent() {
        // issue_src.ownerID = user.id; add a user, two issues reference it.
        let user = TestSource::new(
            "user",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let issue_src = TestSource::new(
            "issue_src",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let owner = |id: i64| -> Row { vec![("id".into(), JsonValue::String(format!("u{id}")))] };
        let iss = |id: i64, owner: i64| -> Row {
            vec![
                ("id".into(), JsonValue::String(format!("i{id}"))),
                ("ownerID".into(), JsonValue::String(format!("u{owner}"))),
            ]
        };
        // parent = issue_src, child = user; issue_src.ownerID -> user.id.
        let join = JoinInput::new(
            issue_src.clone() as Rc<dyn Input>,
            user.clone() as Rc<dyn Input>,
            vec!["ownerID".into()],
            vec!["id".into()],
            "owner",
        );
        let spy = SpyOutput::new();
        join.set_output(spy.clone());
        issue_src.push_change(make_source_change_add(iss(1, 1)));
        issue_src.push_change(make_source_change_add(iss(2, 1)));

        user.push_and_forward(make_source_change_add(owner(1)));

        // One Change::Child per parent issue_src that references u1.
        assert_eq!(spy.received.borrow().len(), 2);
        for change in spy.received.borrow().iter() {
            let Change::Child { child, .. } = change else {
                panic!("expected Change::Child");
            };
            assert_eq!(child.relationship_name, "owner");
        }
    }
}
