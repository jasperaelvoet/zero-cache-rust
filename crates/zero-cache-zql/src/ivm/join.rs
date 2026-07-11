//! A first slice of `zql/src/ivm/join.ts`/`flipped-join.ts` — enough to run
//! a multi-table query's parent->child correlation, closing (partially)
//! the "IVM joins" gap that's been blocking both multi-table queries and
//! `Condition::CorrelatedSubquery` permission rules.
//!
//! **Scope, deliberate and significant:** upstream's real `Join`/
//! `FlippedJoin` are full `Operator`s participating in the push-based
//! incremental-maintenance graph — they receive `Change`s from a parent
//! and child `Input`, maintain join state in `Storage`, and emit their own
//! `Change`s downstream. That requires the general multi-consumer
//! operator graph this port's `operator.rs` module doc deliberately
//! deferred (`Rc<RefCell<dyn Output>>` or an arena) in favor of concrete
//! `TableSource`+`Filter` composition. This module makes the same call for
//! joins: [`fetch_joined`] is a **read-only, non-incremental** join —
//! given two already-fetchable sources and a `Correlation`, it returns
//! each parent row paired with its matching child rows via a plain
//! `fetch()` call. This answers "what does this multi-table query return
//! right now" (useful for the whole-pipeline-slice's query-registration
//! path and for `create_predicate`'s eventual `CorrelatedSubquery`
//! support). [`reeval_exists_after_child_change`] is a first genuinely
//! INCREMENTAL primitive on top of that: given a child-table
//! `SourceChange` that already landed in `child`, it identifies the ONE
//! parent row the change could have affected and re-evaluates its EXISTS
//! status — the exact question `create_predicate_with_exists`'s
//! `CorrelatedSubquery` evaluation needs kept live as changes stream in,
//! without re-scanning the whole join. Full incremental *row-nesting*
//! maintenance (a child insert re-deriving what a parent's *joined child
//! rows* look like, as `Join`/`FlippedJoin` do upstream) remains out of
//! scope — this port's `Node` has no `relationships` field yet (deferred
//! since the first IVM slice), so there's nowhere to put nested child rows
//! even if this function computed them. EXISTS re-evaluation doesn't need
//! that: it only needs a boolean, which is exactly what `exists_for_row`
//! already computes. That's the real, deliberate scope boundary here —
//! not every join use case is incremental yet, but the one this port
//! actually wired into permission/query evaluation
//! (`create_predicate_with_exists`) now is.

use crate::ivm::change::SourceChange;
use crate::ivm::constraint::Constraint;
use crate::ivm::data::Row;
use crate::ivm::operator::{FetchRequest, Node};
use crate::ivm::table_source::TableSource;
use zero_cache_protocol::ast::Correlation;

fn get<'a>(row: &'a Row, field: &str) -> Option<&'a crate::ivm::data::Value> {
    row.iter().find(|(k, _)| k == field).map(|(_, v)| v)
}

/// Builds the child-side `Constraint` for one parent row: each
/// `correlation.child_field[i]` column must equal the parent row's
/// `correlation.parent_field[i]` value. Port of the correlation-to-filter
/// mapping `Join`/`FlippedJoin` do per parent row upstream.
fn correlation_constraint(parent_row: &Row, correlation: &Correlation) -> Constraint {
    correlation
        .parent_field
        .iter()
        .zip(&correlation.child_field)
        .map(|(parent_col, child_col)| {
            (
                child_col.clone(),
                get(parent_row, parent_col)
                    .cloned()
                    .unwrap_or(crate::ivm::data::Value::Null),
            )
        })
        .collect()
}

/// For each row in `parent`, fetches the `child` rows whose
/// `correlation.child_field` values match the parent row's
/// `correlation.parent_field` values. Port of the read-only "what would
/// this join return" question — see module doc for what's NOT covered
/// (incremental push maintenance).
pub fn fetch_joined(
    parent: &TableSource,
    child: &TableSource,
    correlation: &Correlation,
) -> Vec<(Node, Vec<Node>)> {
    parent
        .fetch(&FetchRequest::default())
        .map(|parent_node| {
            let constraint = correlation_constraint(&parent_node.row, correlation);
            let children: Vec<Node> = child
                .fetch(&FetchRequest {
                    constraint: Some(constraint),
                    ..Default::default()
                })
                .collect();
            (parent_node, children)
        })
        .collect()
}

/// Same join as [`fetch_joined`], but returns real `Node`s with the child
/// rows populated into `Node.relationships[relationship_name]` (see
/// `operator::Node`'s module doc for the eager-vs-lazy simplification)
/// instead of a separate tuple. This is what a caller building an actual
/// `Node` tree for a multi-table query result wants; `fetch_joined` remains
/// for callers that only need the flat pairing.
pub fn fetch_joined_nodes(
    parent: &TableSource,
    child: &TableSource,
    correlation: &Correlation,
    relationship_name: &str,
) -> Vec<Node> {
    fetch_joined(parent, child, correlation)
        .into_iter()
        .map(|(mut parent_node, children)| {
            parent_node
                .relationships
                .insert(relationship_name.to_string(), children);
            parent_node
        })
        .collect()
}

/// Whether `child` has any row matching `parent_row` via `correlation` —
/// the primitive `Condition::CorrelatedSubquery`'s `EXISTS`/`NOT EXISTS`
/// evaluation needs (see `builder::filter::create_predicate_with_exists`).
/// Scope: only checks existence via the correlation itself; a
/// `CorrelatedSubquery`'s own nested `subquery` AST can carry its own
/// WHERE clause (`related.subquery.where_`) — not consulted here, so an
/// EXISTS rule with an additional child-side filter is not yet fully
/// faithful. Fine for the common case (a bare parent-owns-child
/// relationship check); a real remaining gap for filtered EXISTS
/// subqueries.
pub fn exists_for_row(parent_row: &Row, child: &TableSource, correlation: &Correlation) -> bool {
    let constraint = correlation_constraint(parent_row, correlation);
    child
        .fetch(&FetchRequest {
            constraint: Some(constraint),
            ..Default::default()
        })
        .next()
        .is_some()
}

/// Inverse of [`correlation_constraint`]: given a row from the CHILD side
/// (e.g. the row a child `SourceChange` carries), builds the `Constraint`
/// identifying the one parent row it correlates to (parent's primary key
/// columns -> the child row's corresponding `correlation.child_field`
/// values).
fn parent_key_constraint(child_row: &Row, correlation: &Correlation) -> Constraint {
    correlation
        .child_field
        .iter()
        .zip(&correlation.parent_field)
        .map(|(child_col, parent_col)| {
            (
                parent_col.clone(),
                get(child_row, child_col)
                    .cloned()
                    .unwrap_or(crate::ivm::data::Value::Null),
            )
        })
        .collect()
}

/// Given a child-table [`SourceChange`] that has already been applied to
/// `child` (i.e. `child.push` already ran), finds the ONE parent row it
/// could have affected and returns `(parent_row, new_exists_value)` —
/// `new_exists_value` is [`exists_for_row`] re-evaluated for that parent
/// against the now-updated `child`. Returns `None` if no parent row
/// correlates to the changed child row (an orphaned child, or the parent
/// doesn't exist in `parent`'s current row set).
///
/// This is the real incremental piece: instead of re-fetching the whole
/// join to see if anything changed, a caller (e.g. `ivm_bridge` reacting
/// to a live replication stream) can call this once per child change and
/// know immediately whether that ONE parent's EXISTS-based filter/policy
/// result needs to flip.
pub fn reeval_exists_after_child_change(
    child_change: &SourceChange,
    parent: &TableSource,
    child: &TableSource,
    correlation: &Correlation,
) -> Option<(Row, bool)> {
    // Use the child row from whichever side of the change identifies the
    // correlation columns — for a Remove this is the removed row (the only
    // one available); for Add/Edit, the current/new row (correlation
    // columns don't change without also changing the row's identity in
    // practice, so `new` is the right choice for Edit too).
    let child_row = match child_change {
        SourceChange::Add(row) => row,
        SourceChange::Remove(row) => row,
        SourceChange::Edit { row, .. } => row,
    };
    let key = parent_key_constraint(child_row, correlation);
    let parent_row = parent.find_by_key(&key)?.clone();
    let new_exists = exists_for_row(&parent_row, child, correlation);
    Some((parent_row, new_exists))
}

/// The full row-nesting counterpart to [`reeval_exists_after_child_change`]:
/// given a child-table [`SourceChange`] already applied to `child`, finds
/// the ONE parent row it could have affected and returns a `Node` for that
/// parent with `relationships[relationship_name]` re-fetched to reflect the
/// now-updated `child` — i.e. the actual incremental maintenance
/// `Join`/`FlippedJoin` do upstream (re-deriving a parent's *joined child
/// rows*, not just a boolean), now buildable because `Node.relationships`
/// exists (see `operator::Node`'s module doc). Returns `None` for the same
/// reason as `reeval_exists_after_child_change`: an orphaned child, or the
/// parent no longer exists in `parent`'s current row set.
///
/// Scope note: like [`exists_for_row`], this re-fetches the correlated
/// child rows via `child.fetch` rather than incrementally patching the
/// previous `Vec<Node>` in place (e.g. inserting/removing just the one
/// changed child row) — a real further optimization upstream's `Join`
/// makes via `Storage`-backed diffing that this port doesn't attempt yet.
/// Still genuinely incremental at the JOIN level: only the ONE affected
/// parent is touched, not a full re-scan of every parent row.
pub fn reeval_relationship_after_child_change(
    child_change: &SourceChange,
    parent: &TableSource,
    child: &TableSource,
    correlation: &Correlation,
    relationship_name: &str,
) -> Option<Node> {
    let child_row = match child_change {
        SourceChange::Add(row) => row,
        SourceChange::Remove(row) => row,
        SourceChange::Edit { row, .. } => row,
    };
    let key = parent_key_constraint(child_row, correlation);
    let parent_row = parent.find_by_key(&key)?.clone();
    let constraint = correlation_constraint(&parent_row, correlation);
    let children: Vec<Node> = child
        .fetch(&FetchRequest {
            constraint: Some(constraint),
            ..Default::default()
        })
        .collect();
    let mut node = Node::new(parent_row);
    node.relationships
        .insert(relationship_name.to_string(), children);
    Some(node)
}

/// A real `Operator` participating in the push-based graph — the piece
/// `operator.rs`'s module doc named as needing genuine multi-consumer
/// fan-out, now built. Wraps [`reeval_relationship_after_child_change`]:
/// when a child-table change is pushed in via [`Join::push_child_change`],
/// it re-derives the affected parent's joined relationship and fans the
/// resulting [`Change`] out to every registered downstream [`Output`] —
/// e.g. a live query result AND a permission check can both watch the same
/// `Join` without either re-deriving the join themselves.
///
/// Scope: still wraps the same single-affected-parent primitive as
/// `reeval_relationship_after_child_change` (re-fetches the correlated
/// children rather than patching in place — see that function's doc) and
/// only reacts to CHILD-side changes (a parent-side change re-deriving
/// which children now match is a further, separately-scoped increment,
/// consistent with every prior round's honest scope notes on this join
/// primitive). `parent`/`child` are held in `RefCell`s (interior
/// mutability, per `Output`'s doc) so `Join` itself can be shared via
/// `Rc<Join>` while still needing `&mut` access to apply changes.
pub struct Join {
    parent: std::cell::RefCell<TableSource>,
    child: std::cell::RefCell<TableSource>,
    correlation: Correlation,
    relationship_name: String,
    outputs: std::cell::RefCell<Vec<std::rc::Rc<dyn crate::ivm::operator::Output>>>,
}

impl Join {
    pub fn new(
        parent: TableSource,
        child: TableSource,
        correlation: Correlation,
        relationship_name: impl Into<String>,
    ) -> std::rc::Rc<Self> {
        std::rc::Rc::new(Join {
            parent: std::cell::RefCell::new(parent),
            child: std::cell::RefCell::new(child),
            correlation,
            relationship_name: relationship_name.into(),
            outputs: std::cell::RefCell::new(Vec::new()),
        })
    }

    /// Port of `Input.setOutput` — registers a downstream consumer. Called
    /// once per subscriber; `Join` fans every subsequent change out to all
    /// of them, the actual multi-consumer behavior the graph decision was
    /// made for.
    pub fn add_output(&self, output: std::rc::Rc<dyn crate::ivm::operator::Output>) {
        self.outputs.borrow_mut().push(output);
    }

    /// Applies `child_change` to the join's internal child `TableSource`
    /// copy, re-derives the affected parent's relationship via
    /// `reeval_relationship_after_child_change`, and — if a parent was
    /// affected — pushes the resulting `Change::Child` to every
    /// registered output. Returns the pushed `Node` (if any) for callers
    /// that want it directly without a spy `Output`.
    pub fn push_child_change(&self, child_change: &SourceChange) -> Option<Node> {
        {
            let mut child = self.child.borrow_mut();
            child.push(child_change.clone());
        }
        let parent = self.parent.borrow();
        let child = self.child.borrow();
        let node = reeval_relationship_after_child_change(
            child_change,
            &parent,
            &child,
            &self.correlation,
            &self.relationship_name,
        )?;
        drop(parent);
        drop(child);

        let relationship_change = match child_change {
            SourceChange::Add(row) => crate::ivm::operator::Change::Add(Node::new(row.clone())),
            SourceChange::Remove(row) => {
                crate::ivm::operator::Change::Remove(Node::new(row.clone()))
            }
            SourceChange::Edit { row, old_row } => crate::ivm::operator::Change::Edit {
                node: Node::new(row.clone()),
                old_node: Node::new(old_row.clone()),
            },
        };
        let change = crate::ivm::operator::make_child_change(
            node.clone(),
            self.relationship_name.clone(),
            relationship_change,
        );
        for output in self.outputs.borrow().iter() {
            output.push(change.clone(), self);
        }
        Some(node)
    }
}

/// Minimal `InputBase` so a `Join` can identify itself as the `pusher` when
/// fanning a change out to its downstream `Output`s (upstream passes `this`).
/// `get_schema` reports the parent-side schema; `destroy` is a no-op until the
/// full operator-graph teardown lands (Section 2/7).
impl crate::ivm::operator::InputBase for Join {
    fn get_schema(&self) -> crate::ivm::operator::SourceSchema {
        self.parent.borrow().schema().clone()
    }

    fn destroy(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::make_source_change_add;
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

    fn correlation() -> Correlation {
        Correlation {
            parent_field: vec!["id".into()],
            child_field: vec!["issueID".into()],
        }
    }

    #[test]
    fn joins_matching_children_to_each_parent() {
        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue(1)));
        issues.push(make_source_change_add(issue(2)));
        comments.push(make_source_change_add(comment(10, 1)));
        comments.push(make_source_change_add(comment(11, 1)));
        comments.push(make_source_change_add(comment(12, 2)));

        let joined = fetch_joined(&issues, &comments, &correlation());
        assert_eq!(joined.len(), 2);
        assert_eq!(joined[0].0.row, issue(1));
        assert_eq!(joined[0].1.len(), 2);
        assert_eq!(joined[1].0.row, issue(2));
        assert_eq!(joined[1].1.len(), 1);
    }

    #[test]
    fn parent_with_no_matching_children_gets_empty_vec() {
        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue(1)));

        let joined = fetch_joined(&issues, &comments, &correlation());
        assert_eq!(joined.len(), 1);
        assert!(joined[0].1.is_empty());
    }

    #[test]
    fn empty_parent_produces_empty_result() {
        let issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        assert_eq!(fetch_joined(&issues, &comments, &correlation()), vec![]);
    }

    #[test]
    fn compound_correlation_keys() {
        let mut parents = TableSource::new(
            "p",
            vec!["a".into(), "b".into()],
            vec![("a".into(), Direction::Asc)],
        );
        let mut children =
            TableSource::new("c", vec!["id".into()], vec![("id".into(), Direction::Asc)]);
        parents.push(make_source_change_add(vec![
            ("a".into(), JsonValue::Number(1.0)),
            ("b".into(), JsonValue::Number(2.0)),
        ]));
        children.push(make_source_change_add(vec![
            ("id".into(), JsonValue::Number(100.0)),
            ("pa".into(), JsonValue::Number(1.0)),
            ("pb".into(), JsonValue::Number(2.0)),
        ]));
        children.push(make_source_change_add(vec![
            ("id".into(), JsonValue::Number(101.0)),
            ("pa".into(), JsonValue::Number(1.0)),
            ("pb".into(), JsonValue::Number(3.0)), // doesn't match b
        ]));

        let correlation = Correlation {
            parent_field: vec!["a".into(), "b".into()],
            child_field: vec!["pa".into(), "pb".into()],
        };
        let joined = fetch_joined(&parents, &children, &correlation);
        assert_eq!(joined.len(), 1);
        assert_eq!(joined[0].1.len(), 1);
    }

    #[test]
    fn exists_for_row_true_when_child_matches() {
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        comments.push(make_source_change_add(comment(10, 1)));
        assert!(exists_for_row(&issue(1), &comments, &correlation()));
    }

    #[test]
    fn exists_for_row_false_when_no_child_matches() {
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        comments.push(make_source_change_add(comment(10, 2)));
        assert!(!exists_for_row(&issue(1), &comments, &correlation()));
    }

    #[test]
    fn exists_for_row_false_on_empty_child_table() {
        let comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        assert!(!exists_for_row(&issue(1), &comments, &correlation()));
    }

    #[test]
    fn reeval_detects_exists_flipping_false_to_true_on_child_insert() {
        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue(1)));
        assert!(
            !exists_for_row(&issue(1), &comments, &correlation()),
            "sanity: starts with no comments"
        );

        let change = comments.push(make_source_change_add(comment(10, 1)));
        let crate::ivm::operator::Change::Add(node) = change else {
            panic!("expected Add")
        };
        let source_change = make_source_change_add(node.row);

        let (parent_row, new_exists) =
            reeval_exists_after_child_change(&source_change, &issues, &comments, &correlation())
                .unwrap();
        assert_eq!(parent_row, issue(1));
        assert!(new_exists, "issue 1 should now have a matching comment");
    }

    #[test]
    fn reeval_detects_exists_flipping_true_to_false_on_child_remove() {
        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue(1)));
        comments.push(make_source_change_add(comment(10, 1)));
        assert!(
            exists_for_row(&issue(1), &comments, &correlation()),
            "sanity: starts with one comment"
        );

        use crate::ivm::change::make_source_change_remove;
        comments.push(make_source_change_remove(comment(10, 1)));
        let source_change = make_source_change_remove(comment(10, 1));

        let (parent_row, new_exists) =
            reeval_exists_after_child_change(&source_change, &issues, &comments, &correlation())
                .unwrap();
        assert_eq!(parent_row, issue(1));
        assert!(!new_exists, "issue 1's only comment was removed");
    }

    #[test]
    fn reeval_does_not_flip_when_another_child_still_matches() {
        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue(1)));
        comments.push(make_source_change_add(comment(10, 1)));
        comments.push(make_source_change_add(comment(11, 1)));

        use crate::ivm::change::make_source_change_remove;
        comments.push(make_source_change_remove(comment(10, 1)));
        let source_change = make_source_change_remove(comment(10, 1));

        let (_, new_exists) =
            reeval_exists_after_child_change(&source_change, &issues, &comments, &correlation())
                .unwrap();
        assert!(new_exists, "comment 11 still matches issue 1");
    }

    #[test]
    fn reeval_returns_none_for_orphaned_child() {
        let issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        comments.push(make_source_change_add(comment(10, 999))); // no issue 999
        let source_change = make_source_change_add(comment(10, 999));

        assert_eq!(
            reeval_exists_after_child_change(&source_change, &issues, &comments, &correlation()),
            None
        );
    }

    #[test]
    fn fetch_joined_nodes_populates_relationships() {
        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue(1)));
        comments.push(make_source_change_add(comment(10, 1)));
        comments.push(make_source_change_add(comment(11, 1)));

        let nodes = fetch_joined_nodes(&issues, &comments, &correlation(), "comments");
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].row, issue(1));
        let children = &nodes[0].relationships["comments"];
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].row, comment(10, 1));
        assert_eq!(children[1].row, comment(11, 1));
    }

    #[test]
    fn reeval_relationship_adds_new_child_on_insert() {
        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue(1)));

        let change = comments.push(make_source_change_add(comment(10, 1)));
        let crate::ivm::operator::Change::Add(node) = change else {
            panic!("expected Add")
        };
        let source_change = make_source_change_add(node.row);

        let node = reeval_relationship_after_child_change(
            &source_change,
            &issues,
            &comments,
            &correlation(),
            "comments",
        )
        .unwrap();
        assert_eq!(node.row, issue(1));
        assert_eq!(
            node.relationships["comments"],
            vec![Node::new(comment(10, 1))]
        );
    }

    #[test]
    fn reeval_relationship_removes_child_on_remove() {
        let mut issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        issues.push(make_source_change_add(issue(1)));
        comments.push(make_source_change_add(comment(10, 1)));
        comments.push(make_source_change_add(comment(11, 1)));

        use crate::ivm::change::make_source_change_remove;
        comments.push(make_source_change_remove(comment(10, 1)));
        let source_change = make_source_change_remove(comment(10, 1));

        let node = reeval_relationship_after_child_change(
            &source_change,
            &issues,
            &comments,
            &correlation(),
            "comments",
        )
        .unwrap();
        assert_eq!(
            node.relationships["comments"],
            vec![Node::new(comment(11, 1))],
            "only the remaining comment should be in the relationship"
        );
    }

    #[test]
    fn reeval_relationship_returns_none_for_orphaned_child() {
        let issues = TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let mut comments = TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        comments.push(make_source_change_add(comment(10, 999)));
        let source_change = make_source_change_add(comment(10, 999));

        assert_eq!(
            reeval_relationship_after_child_change(
                &source_change,
                &issues,
                &comments,
                &correlation(),
                "comments"
            ),
            None
        );
    }

    /// A spy `Output` that just records every `Change` it's pushed —
    /// standing in for a real downstream consumer (a query result stream,
    /// a permission check) without needing one built.
    struct SpyOutput {
        received: std::cell::RefCell<Vec<crate::ivm::operator::Change>>,
    }
    impl SpyOutput {
        fn new() -> std::rc::Rc<Self> {
            std::rc::Rc::new(SpyOutput {
                received: std::cell::RefCell::new(Vec::new()),
            })
        }
    }
    impl crate::ivm::operator::Output for SpyOutput {
        fn push(
            &self,
            change: crate::ivm::operator::Change,
            _pusher: &dyn crate::ivm::operator::InputBase,
        ) {
            self.received.borrow_mut().push(change);
        }
    }

    fn issues_source() -> TableSource {
        TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        )
    }
    fn comments_source() -> TableSource {
        TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        )
    }

    #[test]
    fn join_operator_pushes_child_change_to_a_single_output() {
        let mut issues = issues_source();
        issues.push(make_source_change_add(issue(1)));
        let comments = comments_source();

        let join = Join::new(issues, comments, correlation(), "comments");
        let spy = SpyOutput::new();
        join.add_output(spy.clone());

        let node = join
            .push_child_change(&make_source_change_add(comment(10, 1)))
            .unwrap();
        assert_eq!(node.row, issue(1));
        assert_eq!(
            node.relationships["comments"],
            vec![Node::new(comment(10, 1))]
        );

        let received = spy.received.borrow();
        assert_eq!(received.len(), 1);
        assert_eq!(
            received[0],
            crate::ivm::operator::make_child_change(
                node,
                "comments",
                crate::ivm::operator::Change::Add(Node::new(comment(10, 1)))
            )
        );
    }

    /// The actual reason for the `Output` graph decision: multiple
    /// downstream consumers registered on the same `Join` all see the same
    /// change from a single child-table push — no consumer re-derives the
    /// join itself.
    #[test]
    fn join_operator_fans_a_single_change_out_to_multiple_outputs() {
        let mut issues = issues_source();
        issues.push(make_source_change_add(issue(1)));
        let comments = comments_source();

        let join = Join::new(issues, comments, correlation(), "comments");
        let spy1 = SpyOutput::new();
        let spy2 = SpyOutput::new();
        join.add_output(spy1.clone());
        join.add_output(spy2.clone());

        join.push_child_change(&make_source_change_add(comment(10, 1)));

        assert_eq!(
            spy1.received.borrow().len(),
            1,
            "first output should have received the fanned-out change"
        );
        assert_eq!(
            spy2.received.borrow().len(),
            1,
            "second output should independently have received the SAME change"
        );
        assert_eq!(*spy1.received.borrow(), *spy2.received.borrow());
    }

    #[test]
    fn join_operator_orphaned_child_pushes_nothing() {
        let issues = issues_source();
        let comments = comments_source();
        let join = Join::new(issues, comments, correlation(), "comments");
        let spy = SpyOutput::new();
        join.add_output(spy.clone());

        let result = join.push_child_change(&make_source_change_add(comment(10, 999)));
        assert_eq!(result, None);
        assert!(spy.received.borrow().is_empty());
    }

    #[test]
    fn join_operator_reflects_child_removal_on_next_push() {
        let mut issues = issues_source();
        issues.push(make_source_change_add(issue(1)));
        let mut comments = comments_source();
        comments.push(make_source_change_add(comment(10, 1)));
        comments.push(make_source_change_add(comment(11, 1)));

        let join = Join::new(issues, comments, correlation(), "comments");
        let spy = SpyOutput::new();
        join.add_output(spy.clone());

        use crate::ivm::change::make_source_change_remove;
        let node = join
            .push_child_change(&make_source_change_remove(comment(10, 1)))
            .unwrap();
        assert_eq!(
            node.relationships["comments"],
            vec![Node::new(comment(11, 1))]
        );
    }
}
