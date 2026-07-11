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
//! **Scope.** Only the pull (`fetch`) direction is implemented; `set_output`
//! is a no-op and there is no incremental `push` maintenance here — the
//! push-based incremental join lives in [`crate::ivm::join::Join`], and the
//! driver's graph is fetch-only. Parent-side ordering/constraints flow
//! straight through to the parent input; the per-parent child constraint is
//! built fresh for each parent row.

use std::rc::Rc;

use zero_cache_protocol::ast::CompoundKey;

use crate::ivm::constraint::Constraint;
use crate::ivm::data::{Row, Value};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Node, Output, SourceSchema, Stream};

fn get(row: &Row, field: &str) -> Value {
    row.iter()
        .find(|(k, _)| k == field)
        .map(|(_, v)| v.clone())
        .unwrap_or(Value::Null)
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
}

impl JoinInput {
    pub fn new(
        parent: Rc<dyn Input>,
        child: Rc<dyn Input>,
        parent_key: CompoundKey,
        child_key: CompoundKey,
        relationship_name: impl Into<String>,
    ) -> Rc<Self> {
        Rc::new(JoinInput {
            parent,
            child,
            parent_key,
            child_key,
            relationship_name: relationship_name.into(),
        })
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
        self.parent.destroy();
        self.child.destroy();
    }
}

impl Input for JoinInput {
    /// No-op: this operator is only ever driven in the pull (`fetch`)
    /// direction by the driver's transient hydration graph (see module doc).
    fn set_output(&self, _output: Rc<dyn Output>) {}

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
}
