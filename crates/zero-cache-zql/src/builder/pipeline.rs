//! Port of `zql/src/builder/builder.ts`'s `buildPipeline`. Builds the operator
//! graph for a query: source → `start` [`Skip`] → `whereExists`
//! [`JoinInput`]s → `where` [`GraphFilter`]/[`Exists`] → `limit` [`Take`] →
//! `related` [`JoinInput`]s (redesign §5.1, increments 3/6/7). See
//! [`build_pipeline`] for the full assembly order and the documented
//! out-of-scope shapes (OR-of-EXISTS, `FlippedJoin`).
//!
//! **Source-agnostic by construction.** `zero-cache-zql` sits *below*
//! `zero-cache-sqlite` in the crate graph, so it cannot name `SqliteSource`.
//! The delegate therefore hands back sources as `Rc<dyn Input>` — the driver
//! (which depends on both crates) is what actually constructs the concrete
//! replica-backed `SqliteSource` and erases it to `dyn Input` behind
//! [`BuildDelegate::get_source`].

use std::rc::Rc;

use zero_cache_protocol::ast::{Ast, Bound as AstBound, Condition, CorrelatedSubquery, ExistsOp};

use crate::builder::filter::create_predicate;
use crate::ivm::data::Row;
use crate::ivm::exists::{Exists, ExistsType};
use crate::ivm::filter::GraphFilter;
use crate::ivm::join_input::JoinInput;
use crate::ivm::operator::{Input, Storage};
use crate::ivm::skip::{Bound as SkipBound, Skip};
use crate::ivm::take::Take;
use zero_cache_shared::bigint_json::JsonValue;

/// The build-time environment `build_pipeline` consults, mirroring upstream's
/// `BuilderDelegate` (`builder.ts`).
pub struct BuildDelegate<'d> {
    /// Returns the (memoized, per-table) source for `table`, already erased to
    /// `Rc<dyn Input>` so this crate stays source-agnostic. Port of upstream
    /// `#getSource` (`pipeline-driver.ts:1054`).
    pub get_source: &'d dyn Fn(&str) -> Rc<dyn Input>,
    /// Returns a fresh per-operator [`Storage`] namespaced by `name`. Port of
    /// upstream `createStorage` (`builder.ts`) — needed once `Take` (and later
    /// `Exists`) maintain durable state.
    pub create_storage: &'d dyn Fn(&str) -> Rc<dyn Storage>,
}

/// Converts an AST [`AstBound`] (whose `row` is a JSON object) into the ivm
/// [`SkipBound`] a [`Skip`] expects.
fn ast_bound_to_skip_bound(bound: &AstBound) -> SkipBound {
    let row: Row = match &bound.row {
        JsonValue::Object(entries) => entries.clone(),
        _ => Vec::new(),
    };
    SkipBound {
        row,
        exclusive: bound.exclusive,
    }
}

/// Builds an operator pipeline for `ast`, returning its root [`Input`], with
/// the same operator assembly order as upstream `buildPipelineInternal`
/// (`builder.ts`):
///
/// 1. `source` — the (memoized) table source from the delegate.
/// 2. `skip` — the `start` cursor bound.
/// 3. `EXISTS` **joins** — for each `whereExists` correlated-subquery
///    condition, a [`JoinInput`] populating `node.relationships[alias]`
///    (upstream's `applyCorrelatedSubQuery(..., fromCondition=true)` run for
///    every gathered `csqCondition` *before* `applyWhere`).
/// 4. `where` — a [`GraphFilter`] for the non-correlated part of the
///    condition, plus an [`Exists`] operator per correlated-subquery condition
///    reading the relationship its join populated (upstream's `applyWhere` →
///    `applyCorrelatedSubqueryCondition` building an `Exists`).
/// 5. `take` — the `limit`.
/// 6. `related` **joins** — a [`JoinInput`] per `related` hop, the child
///    pipeline built recursively (upstream's `applyCorrelatedSubQuery(...,
///    fromCondition=false)`).
///
/// The returned root's `set_output` must still be pointed at a downstream
/// (e.g. the driver's `Collector`) before it is pushed to.
///
/// **Out of scope (documented, not wired):**
/// - *OR-of-EXISTS* — a `CorrelatedSubquery` beneath a [`Condition::Or`]. That
///   needs `FanOut`/`FanIn` (upstream's `applyOr` fans a subquery branch out
///   and unions it back in), which this port hasn't ported. `build_pipeline`
///   [`panic!`]s on it rather than silently drop the EXISTS check. The driver
///   never reaches here for such a query (its `is_graph_eligible` gates out
///   *any* correlated subquery), so this is only reachable by a direct caller.
/// - *`FlippedJoin`* (child-drives-parent, upstream `flipped-join.ts`) — only
///   built for `condition.flip`, which the query planner assigns. This port
///   has no planner-to-AST wiring (`flip` is always `None`), so no corpus
///   query needs it; skipped.
/// - The upstream `EXISTS_LIMIT`/`PERMISSIONS_EXISTS_LIMIT` cap on the EXISTS
///   child subquery (an optimization: existence only needs one row). The
///   pull-based [`Exists`] here checks the full relationship for non-emptiness,
///   so correctness does not depend on the cap.
///
/// Panics (via [`create_predicate`]) if a *non-correlated* branch contains a
/// `CorrelatedSubquery` in a shape this builder does not decompose (see
/// out-of-scope above).
pub fn build_pipeline(ast: &Ast, delegate: &BuildDelegate) -> Rc<dyn Input> {
    let mut end: Rc<dyn Input> = (delegate.get_source)(&ast.table);

    if let Some(bound) = &ast.start {
        end = Skip::new(end, ast_bound_to_skip_bound(bound)) as Rc<dyn Input>;
    }

    if let Some(condition) = &ast.where_ {
        end = apply_where(end, condition, delegate);
    }

    if let Some(limit) = ast.limit {
        let storage = (delegate.create_storage)("take");
        end = Take::new(end, storage, limit as usize, None) as Rc<dyn Input>;
    }

    if let Some(related) = &ast.related {
        // Dedupe by alias, last-one-wins (upstream `byAlias` map), preserving
        // first-seen order.
        let mut seen: Vec<String> = Vec::new();
        let mut chosen: Vec<&CorrelatedSubquery> = Vec::new();
        for csq in related {
            let alias = relationship_name(csq, seen.len());
            if let Some(pos) = seen.iter().position(|a| *a == alias) {
                chosen[pos] = csq;
            } else {
                seen.push(alias);
                chosen.push(csq);
            }
        }
        for (index, csq) in chosen.into_iter().enumerate() {
            end = apply_related(end, csq, index, delegate);
        }
    }

    end
}

/// The relationship name for a correlated subquery: its subquery `alias`, or a
/// stable generated name keyed by position when the AST carries none.
fn relationship_name(csq: &CorrelatedSubquery, index: usize) -> String {
    csq.subquery
        .alias
        .clone()
        .unwrap_or_else(|| format!("zsubq_{index}"))
}

/// Builds the `related` join for one hop: the child pipeline built recursively
/// (so nested `related`/`where` on the child are honored), then a
/// [`JoinInput`] populating `node.relationships[alias]`.
fn apply_related(
    parent: Rc<dyn Input>,
    csq: &CorrelatedSubquery,
    index: usize,
    delegate: &BuildDelegate,
) -> Rc<dyn Input> {
    let name = relationship_name(csq, index);
    let child = build_pipeline(&csq.subquery, delegate);
    JoinInput::new(
        parent,
        child,
        csq.correlation.parent_field.clone(),
        csq.correlation.child_field.clone(),
        name,
    ) as Rc<dyn Input>
}

/// Applies a `where_` condition, decomposing `EXISTS`/`NOT EXISTS`
/// correlated-subquery conditions into `Join` + `Exists` operators (matching
/// upstream's split of `applyCorrelatedSubQuery` before `applyWhere` and
/// `applyCorrelatedSubqueryCondition` inside it). A condition with no
/// correlated subquery compiles straight to a single [`GraphFilter`].
fn apply_where(
    input: Rc<dyn Input>,
    condition: &Condition,
    delegate: &BuildDelegate,
) -> Rc<dyn Input> {
    if !condition_has_correlated(condition) {
        let predicate = create_predicate(condition);
        return GraphFilter::new(input, predicate) as Rc<dyn Input>;
    }

    assert!(
        !condition_has_correlated_under_or(condition),
        "build_pipeline: OR-of-EXISTS is not supported (needs FanOut/FanIn); \
         see build_pipeline's doc"
    );

    // Every correlated subquery is in AND context. Upstream applies all the
    // EXISTS joins first (populating each relationship), then the where
    // filters — the simple/non-correlated remainder as a Filter and each
    // correlated condition as an Exists reading its relationship.
    let exists_conditions = gather_exists_conditions(condition);

    let mut end = input;
    for (index, (csq, _op)) in exists_conditions.iter().enumerate() {
        let name = relationship_name(csq, index);
        let child = build_pipeline(&csq.subquery, delegate);
        end = JoinInput::new(
            end,
            child,
            csq.correlation.parent_field.clone(),
            csq.correlation.child_field.clone(),
            name,
        ) as Rc<dyn Input>;
    }

    if let Some(remainder) = strip_correlated(condition) {
        let predicate = create_predicate(&remainder);
        end = GraphFilter::new(end, predicate) as Rc<dyn Input>;
    }

    for (index, (csq, op)) in exists_conditions.iter().enumerate() {
        let name = relationship_name(csq, index);
        let storage = (delegate.create_storage)(&format!("exists:{name}"));
        let exists_type = match op {
            ExistsOp::Exists => ExistsType::Exists,
            ExistsOp::NotExists => ExistsType::NotExists,
        };
        end = Exists::new(
            end,
            storage,
            name,
            csq.correlation.parent_field.clone(),
            exists_type,
        ) as Rc<dyn Input>;
    }

    end
}

/// Whether `condition` contains any `CorrelatedSubquery` anywhere.
fn condition_has_correlated(condition: &Condition) -> bool {
    match condition {
        Condition::Simple { .. } => false,
        Condition::CorrelatedSubquery { .. } => true,
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(condition_has_correlated)
        }
    }
}

/// Whether any `CorrelatedSubquery` appears beneath a `Condition::Or` — the
/// case `build_pipeline` cannot decompose into `Join`+`Exists` (see doc).
fn condition_has_correlated_under_or(condition: &Condition) -> bool {
    match condition {
        Condition::Simple { .. } | Condition::CorrelatedSubquery { .. } => false,
        Condition::Or { conditions } => conditions.iter().any(condition_has_correlated),
        Condition::And { conditions } => conditions.iter().any(condition_has_correlated_under_or),
    }
}

/// Collects every `CorrelatedSubquery` condition (with its `EXISTS`/`NOT
/// EXISTS` op), recursing through AND/OR. Port of
/// `gatherCorrelatedSubqueryQueryConditions` (`builder.ts`).
fn gather_exists_conditions(condition: &Condition) -> Vec<(&CorrelatedSubquery, ExistsOp)> {
    let mut out = Vec::new();
    fn gather<'a>(condition: &'a Condition, out: &mut Vec<(&'a CorrelatedSubquery, ExistsOp)>) {
        match condition {
            Condition::CorrelatedSubquery { related, op, .. } => out.push((related, *op)),
            Condition::And { conditions } | Condition::Or { conditions } => {
                for c in conditions {
                    gather(c, out);
                }
            }
            Condition::Simple { .. } => {}
        }
    }
    gather(condition, &mut out);
    out
}

/// Returns `condition` with every `CorrelatedSubquery` leaf removed, so the
/// remainder can compile to a plain predicate ([`create_predicate`]) — the
/// EXISTS checks are handled separately by `Exists` operators. Returns `None`
/// when nothing non-correlated is left (e.g. a bare EXISTS). Only sound when
/// no correlated subquery sits under an OR (the caller asserts this).
fn strip_correlated(condition: &Condition) -> Option<Condition> {
    match condition {
        Condition::Simple { .. } => Some(condition.clone()),
        Condition::CorrelatedSubquery { .. } => None,
        Condition::And { conditions } => {
            let kept: Vec<Condition> = conditions.iter().filter_map(strip_correlated).collect();
            match kept.len() {
                0 => None,
                1 => Some(kept.into_iter().next().unwrap()),
                _ => Some(Condition::And { conditions: kept }),
            }
        }
        Condition::Or { conditions } => {
            // No correlated subquery is under an OR here (caller-asserted), so
            // every branch survives unchanged.
            let kept: Vec<Condition> = conditions.iter().filter_map(strip_correlated).collect();
            Some(Condition::Or { conditions: kept })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::make_source_change_add;
    use crate::ivm::data::Row;
    use crate::ivm::memory_storage::MemoryStorage;
    use crate::ivm::operator::{FetchRequest, InputBase, Node, Output, SourceSchema, Stream};
    use crate::ivm::test_input::TestSource;
    use std::cell::RefCell;
    use std::collections::{BTreeMap, HashMap};
    use zero_cache_protocol::ast::{
        Bound, ColumnReference, Condition, CorrelatedSubquery, Correlation, Direction, ExistsOp,
        LiteralValue, SimpleOperator, ValuePosition,
    };
    use zero_cache_shared::bigint_json::JsonValue;

    fn make_storage(_name: &str) -> Rc<dyn Storage> {
        Rc::new(MemoryStorage::default())
    }

    fn row(id: i64, active: bool) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("active".into(), JsonValue::Bool(active)),
        ]
    }

    /// In-memory `Input` returning a fixed row set — the source-agnostic test
    /// double the delegate hands back as `Rc<dyn Input>`.
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
            SourceSchema {
                table_name: "issue".into(),
                primary_key: vec!["id".into()],
                sort: vec![("id".into(), Direction::Asc)],
                relationships: BTreeMap::new(),
            }
        }
        fn destroy(&self) {}
    }
    impl Input for VecInput {
        fn set_output(&self, _output: Rc<dyn Output>) {}
        fn fetch<'a>(&'a self, _req: &FetchRequest) -> Stream<'a, Node> {
            Box::new(self.rows.iter().cloned().map(Node::new))
        }
    }

    fn where_active() -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: "active".into(),
            }),
            right: ValuePosition::Literal(LiteralValue::Bool(true)),
        }
    }

    #[test]
    fn builds_bare_source_when_no_where() {
        let input = VecInput::new(vec![row(1, true), row(2, false)]);
        let source_slot: RefCell<Option<Rc<dyn Input>>> = RefCell::new(Some(input.clone()));
        let get_source = |_t: &str| source_slot.borrow().clone().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let rows: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        assert_eq!(
            rows,
            vec![Node::new(row(1, true)), Node::new(row(2, false))]
        );
    }

    #[test]
    fn wraps_source_in_filter_when_where_present() {
        let input = VecInput::new(vec![row(1, true), row(2, false), row(3, true)]);
        let source_slot: RefCell<Option<Rc<dyn Input>>> = RefCell::new(Some(input.clone()));
        let get_source = |_t: &str| source_slot.borrow().clone().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            where_: Some(where_active()),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let rows: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        assert_eq!(rows, vec![Node::new(row(1, true)), Node::new(row(3, true))]);
    }

    fn issue_row(id: i64) -> Row {
        vec![("id".into(), JsonValue::Number(id as f64))]
    }

    fn seeded_source(ids: &[i64]) -> Rc<TestSource> {
        let source = TestSource::new(
            "issue",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        for id in ids {
            source.push_change(make_source_change_add(issue_row(*id)));
        }
        source
    }

    #[test]
    fn wraps_source_in_skip_when_start_present() {
        let source = seeded_source(&[1, 2, 3, 4]);
        let source_slot: RefCell<Option<Rc<dyn Input>>> =
            RefCell::new(Some(source.clone() as Rc<dyn Input>));
        let get_source = |_t: &str| source_slot.borrow().clone().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            start: Some(Bound {
                row: JsonValue::Object(vec![("id".into(), JsonValue::Number(2.0))]),
                exclusive: true,
            }),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let rows: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        // Exclusive start at id=2 drops ids 1 and 2.
        assert_eq!(rows, vec![Node::new(issue_row(3)), Node::new(issue_row(4))]);
    }

    #[test]
    fn wraps_source_in_take_when_limit_present() {
        let source = seeded_source(&[1, 2, 3, 4, 5]);
        let source_slot: RefCell<Option<Rc<dyn Input>>> =
            RefCell::new(Some(source.clone() as Rc<dyn Input>));
        let get_source = |_t: &str| source_slot.borrow().clone().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            limit: Some(2.0),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let rows: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        assert_eq!(rows, vec![Node::new(issue_row(1)), Node::new(issue_row(2))]);
    }

    #[test]
    fn combines_start_and_limit() {
        let source = seeded_source(&[1, 2, 3, 4, 5]);
        let source_slot: RefCell<Option<Rc<dyn Input>>> =
            RefCell::new(Some(source.clone() as Rc<dyn Input>));
        let get_source = |_t: &str| source_slot.borrow().clone().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            start: Some(Bound {
                row: JsonValue::Object(vec![("id".into(), JsonValue::Number(2.0))]),
                exclusive: true,
            }),
            limit: Some(2.0),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let rows: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        // Skip past id=2, then take 2 -> ids 3 and 4.
        assert_eq!(rows, vec![Node::new(issue_row(3)), Node::new(issue_row(4))]);
    }

    // ---- Join / Exists wiring (redesign increments 6-7) ----

    fn comment_row(id: i64, issue_id: i64) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("issueID".into(), JsonValue::Number(issue_id as f64)),
        ]
    }

    /// Seeds an `issue` source (ids) and a `comment` source (`(id, issueID)`)
    /// and returns a table→`Rc<dyn Input>` map the delegate looks up by name.
    fn issue_comment_sources(
        issues: &[i64],
        comments: &[(i64, i64)],
    ) -> HashMap<String, Rc<dyn Input>> {
        let issue = TestSource::new(
            "issue",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        for id in issues {
            issue.push_change(make_source_change_add(issue_row(*id)));
        }
        let comment = TestSource::new(
            "comment",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        for (id, issue_id) in comments {
            comment.push_change(make_source_change_add(comment_row(*id, *issue_id)));
        }
        let mut map: HashMap<String, Rc<dyn Input>> = HashMap::new();
        map.insert("issue".into(), issue as Rc<dyn Input>);
        map.insert("comment".into(), comment as Rc<dyn Input>);
        map
    }

    /// A `comment` correlated subquery hop (issue.id = comment.issueID).
    fn comments_subquery(alias: &str) -> CorrelatedSubquery {
        CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: "comment".into(),
                alias: Some(alias.into()),
                ..Default::default()
            }),
            system: None,
            hidden: None,
        }
    }

    fn exists_condition(op: ExistsOp) -> Condition {
        Condition::CorrelatedSubquery {
            related: comments_subquery("comments"),
            op,
            flip: None,
            scalar: None,
            plan_id: None,
        }
    }

    fn ids(nodes: &[Node]) -> Vec<f64> {
        nodes
            .iter()
            .map(|n| match n.row.iter().find(|(k, _)| k == "id").unwrap().1 {
                JsonValue::Number(v) => v,
                _ => panic!("id not a number"),
            })
            .collect()
    }

    #[test]
    fn related_subquery_builds_join_populating_relationship() {
        let map = issue_comment_sources(&[1, 2, 3], &[(10, 1), (11, 1), (12, 3)]);
        let get_source = |t: &str| map.get(t).cloned().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            related: Some(vec![comments_subquery("comments")]),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let nodes: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        assert_eq!(ids(&nodes), vec![1.0, 2.0, 3.0]);
        assert_eq!(nodes[0].relationships["comments"].len(), 2);
        assert!(nodes[1].relationships["comments"].is_empty());
        assert_eq!(nodes[2].relationships["comments"].len(), 1);
        assert_eq!(
            nodes[2].relationships["comments"][0].row,
            comment_row(12, 3)
        );
    }

    #[test]
    fn where_exists_builds_join_feeding_exists_filter() {
        // issues 1 and 3 have comments; issue 2 does not.
        let map = issue_comment_sources(&[1, 2, 3], &[(10, 1), (12, 3)]);
        let get_source = |t: &str| map.get(t).cloned().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            where_: Some(exists_condition(ExistsOp::Exists)),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let nodes: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        assert_eq!(ids(&nodes), vec![1.0, 3.0], "only issues with comments");
    }

    #[test]
    fn where_not_exists_returns_parents_without_children() {
        let map = issue_comment_sources(&[1, 2, 3], &[(10, 1), (12, 3)]);
        let get_source = |t: &str| map.get(t).cloned().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            where_: Some(exists_condition(ExistsOp::NotExists)),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let nodes: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        assert_eq!(ids(&nodes), vec![2.0], "only issues WITHOUT comments");
    }

    #[test]
    fn exists_composes_with_a_simple_and_condition() {
        // where active = true AND EXISTS(comments). Issue 1: active + comments;
        // issue 2: inactive + comments; issue 3: active, no comments.
        let issue = TestSource::new(
            "issue",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        for (id, active) in [(1, true), (2, false), (3, true)] {
            issue.push_change(make_source_change_add(vec![
                ("id".into(), JsonValue::Number(id as f64)),
                ("active".into(), JsonValue::Bool(active)),
            ]));
        }
        let comment = TestSource::new(
            "comment",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        for (id, issue_id) in [(10, 1), (11, 2)] {
            comment.push_change(make_source_change_add(comment_row(id, issue_id)));
        }
        let mut map: HashMap<String, Rc<dyn Input>> = HashMap::new();
        map.insert("issue".into(), issue as Rc<dyn Input>);
        map.insert("comment".into(), comment as Rc<dyn Input>);

        let get_source = |t: &str| map.get(t).cloned().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            where_: Some(Condition::And {
                conditions: vec![where_active(), exists_condition(ExistsOp::Exists)],
            }),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let nodes: Vec<Node> = root.fetch(&FetchRequest::default()).collect();
        // Only issue 1 is both active AND has a comment.
        assert_eq!(ids(&nodes), vec![1.0]);
    }

    #[test]
    #[should_panic(expected = "OR-of-EXISTS is not supported")]
    fn or_of_exists_is_rejected() {
        let map = issue_comment_sources(&[1], &[(10, 1)]);
        let get_source = |t: &str| map.get(t).cloned().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            where_: Some(Condition::Or {
                conditions: vec![where_active(), exists_condition(ExistsOp::Exists)],
            }),
            ..Default::default()
        };
        build_pipeline(&ast, &delegate);
    }
}
