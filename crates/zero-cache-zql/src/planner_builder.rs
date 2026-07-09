//! Port of the small, self-contained pure functions in
//! `zql/src/planner/planner-builder.ts` — the AST-to-plan-graph
//! construction file, the last `zql/src/planner` file with real logic
//! now that the graph nodes themselves (`planner_node.rs`) have real
//! `estimateCost`/`propagateConstraints` behavior to build into.
//!
//! Scope: the whole AST-to-graph construction is now ported —
//! `has_correlated_subquery`/`extract_constraint` (pure), `wireOutput`,
//! `buildPlanGraph`, `processCondition`, `processAnd`, `processOr`, and
//! `processCorrelatedSubquery` (see [`wire_output`] and [`build_plan_graph`]).
//! A `where_` `EXISTS`/`NOT EXISTS` condition — top-level, inside `and`, or
//! inside `or` — builds real [`crate::planner_node::PlannerJoinNode`]s (with the
//! right flippability/type and a `plan_id` stored on the join AND stamped onto
//! the AST condition, which is why `build_plan_graph` takes `&mut Ast`); an
//! `or` with correlated-subquery branches builds the fan-out/fan-in pair.
//!
//! `applyToCondition` (rewriting an AST's `correlatedSubquery` conditions
//! with the planner's chosen `flip` value after planning) IS now ported —
//! see [`apply_to_condition`]. Its one-time design blocker (it keys off
//! `condition[planIdSymbol]`, a `Symbol`-keyed property upstream's
//! `CorrelatedSubqueryCondition` declares at `ast.ts` line 330) has since
//! been resolved: this crate's `Condition::CorrelatedSubquery` variant now
//! carries a `plan_id: Option<i64>` field, so the rewrite can consume it
//! directly.
//!
//! `applyPlansToAST` IS now ported — see [`apply_plans_to_ast`]. It derives the
//! flipped-`plan_id` set from a planned graph's joins (those left in the
//! `flipped` state after `PlannerGraph::plan()`), drives [`apply_to_condition`]
//! over `where_`, and recurses into each `related` subquery's matching
//! `sub_plans` entry. The whole `buildPlanGraph -> plan -> applyPlansToAST`
//! chain is now expressible in Rust; wiring it into the live hydration path
//! (to replace the direct-read + IN-filter shape) is the remaining step.

use std::collections::BTreeSet;

use zero_cache_protocol::ast::Condition;

use crate::planner_graph::PlannerGraph;
use crate::planner_node::{ConnectionCostModel, PlannerFanInNode, PlannerFanOutNode, PlannerNode};

/// Port of `planner-builder.ts`'s `applyToCondition`: rewrites a condition
/// tree with the planner's chosen `flip` decisions after planning. For each
/// `correlatedSubquery` condition, `flip` is set to whether that condition's
/// `plan_id` is in `flipped_ids` (the plan ids the planner decided to flip),
/// and the rewrite recurses into the subquery's own `where_`; `and`/`or`
/// branches recurse into their children; `simple` conditions are returned
/// unchanged.
///
/// This is the piece that finally CONSUMES the `plan_id` field on
/// `Condition::CorrelatedSubquery` (which `ast_json` never reads/writes — it
/// is planner-internal) and turns it into a concrete `flip` on the AST. The
/// caller (`applyPlansToAST`, which derives `flipped_ids` from a planned
/// graph's flipped joins) is not ported yet — it needs the still-unported
/// `Plans`/`PlannerGraph::plan()` output shape — so `flipped_ids` is passed in
/// directly here.
pub fn apply_to_condition(condition: &Condition, flipped_ids: &BTreeSet<i64>) -> Condition {
    match condition {
        Condition::Simple { .. } => condition.clone(),
        Condition::CorrelatedSubquery {
            related,
            op,
            flip: _,
            scalar,
            plan_id,
        } => {
            let should_flip = plan_id.is_some_and(|id| flipped_ids.contains(&id));
            let mut new_related = related.clone();
            new_related.subquery.where_ = related
                .subquery
                .where_
                .as_ref()
                .map(|w| apply_to_condition(w, flipped_ids));
            Condition::CorrelatedSubquery {
                related: new_related,
                op: *op,
                flip: Some(should_flip),
                scalar: *scalar,
                plan_id: *plan_id,
            }
        }
        Condition::And { conditions } => Condition::And {
            conditions: conditions
                .iter()
                .map(|c| apply_to_condition(c, flipped_ids))
                .collect(),
        },
        Condition::Or { conditions } => Condition::Or {
            conditions: conditions
                .iter()
                .map(|c| apply_to_condition(c, flipped_ids))
                .collect(),
        },
    }
}

/// Port of `hasCorrelatedSubquery`: true if `condition` is (or contains,
/// recursively through `and`/`or`) a `correlatedSubquery` condition.
pub fn has_correlated_subquery(condition: &Condition) -> bool {
    match condition {
        Condition::CorrelatedSubquery { .. } => true,
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(has_correlated_subquery)
        }
        Condition::Simple { .. } => false,
    }
}

/// Port of `extractConstraint`: builds a `PlannerConstraint` (an
/// existence-only set of column names) from a correlation's field list.
/// `_table_name` is unused upstream too (the parameter exists but its
/// body never reads it) — kept as a parameter here anyway to match the
/// signature exactly and flag that this is intentional, not an oversight
/// dropped during porting.
pub fn extract_constraint(
    fields: &[String],
    _table_name: &str,
) -> crate::planner_constraint::PlannerConstraint {
    fields.iter().cloned().collect()
}

/// Port of `Plans`: a planned graph plus the recursively-planned graphs for
/// each `related` subquery (keyed by the subquery's alias).
pub struct Plans {
    pub plan: PlannerGraph,
    pub sub_plans: std::collections::BTreeMap<String, Plans>,
}

#[derive(Debug, thiserror::Error)]
pub enum BuildPlanGraphError {
    #[error(transparent)]
    Graph(#[from] crate::planner_graph::PlannerGraphError),
    #[error("Related subquery must have alias")]
    MissingAlias,
}

/// Port of `wireOutput`: links `from`'s downstream output pointer to `to`.
/// Single-output nodes (`connection`/`join`/`fan-in`) set their one output;
/// a `fan-out` appends to its output list; a `terminus` can never have an
/// output (it is the sink).
pub fn wire_output(from: &PlannerNode, to: PlannerNode) {
    match from {
        PlannerNode::Connection(n) => n.borrow_mut().set_output(to),
        PlannerNode::Join(n) => n.borrow_mut().set_output(to),
        PlannerNode::FanIn(n) => n.borrow_mut().set_output(to),
        PlannerNode::FanOut(n) => n.borrow_mut().add_output(to),
        PlannerNode::Terminus(_) => panic!("Terminus nodes cannot have outputs"),
    }
}

/// Port of `buildPlanGraph`. Builds the `source -> connection -> [joins] ->
/// terminus` graph for `ast` and recursively plans each `related` subquery
/// into `sub_plans` (keyed by alias, each planned as a root with the child
/// correlation's fields as base constraints). The query's ordering, filters,
/// root-ness, base constraints, and limit are threaded into the root
/// connection exactly as upstream.
///
/// `where_` `correlatedSubquery` (`EXISTS`/`NOT EXISTS`) conditions build real
/// joins via [`process_correlated_subquery`], assigning each a `plan_id` that
/// is both stored on the [`crate::planner_node::PlannerJoinNode`] and stamped
/// onto the AST condition (`condition[planIdSymbol]` upstream) — which is why
/// this takes `&mut Ast`. EXISTS at the top level, inside `and`, and inside
/// `or` (via [`process_or`]'s fan-out/fan-in) are all supported.
pub fn build_plan_graph(
    ast: &mut zero_cache_protocol::ast::Ast,
    model: &ConnectionCostModel,
    is_root: bool,
    base_constraints: Option<crate::planner_constraint::PlannerConstraint>,
) -> Result<Plans, BuildPlanGraphError> {
    let parent_table = ast.table.clone();
    let mut graph = PlannerGraph::new();
    let connection = graph.add_source(&ast.table, model.clone())?.connect(
        ast.order_by.clone().unwrap_or_default(),
        ast.where_.clone(),
        is_root,
        base_constraints,
        ast.limit,
    );
    graph.connections.push(connection.clone());

    let mut next_plan_id: i64 = 0;
    let end = match ast.where_.as_mut() {
        Some(where_) => process_condition(
            where_,
            connection,
            &mut graph,
            model,
            &parent_table,
            &mut next_plan_id,
        )?,
        None => connection,
    };

    let terminus = PlannerNode::Terminus(std::rc::Rc::new(std::cell::RefCell::new(
        crate::planner_node::PlannerTerminusNode {
            input: Some(end.clone()),
            pinned: true,
        },
    )));
    wire_output(&end, terminus.clone());
    graph.set_terminus(terminus);

    let mut sub_plans = std::collections::BTreeMap::new();
    if let Some(related) = ast.related.as_mut() {
        for csq in related.iter_mut() {
            let alias = csq
                .subquery
                .alias
                .clone()
                .ok_or(BuildPlanGraphError::MissingAlias)?;
            let child_constraints =
                extract_constraint(&csq.correlation.child_field, &csq.subquery.table);
            let sub = build_plan_graph(&mut csq.subquery, model, true, Some(child_constraints))?;
            sub_plans.insert(alias, sub);
        }
    }

    Ok(Plans {
        plan: graph,
        sub_plans,
    })
}

/// Port of `processCondition`: routes a `where_` condition to the right
/// builder. `simple` adds no graph structure (returns `input`); `and`/`or`
/// recurse; `correlatedSubquery` builds a join.
fn process_condition(
    condition: &mut Condition,
    input: PlannerNode,
    graph: &mut PlannerGraph,
    model: &ConnectionCostModel,
    parent_table: &str,
    next_plan_id: &mut i64,
) -> Result<PlannerNode, BuildPlanGraphError> {
    match condition {
        Condition::Simple { .. } => Ok(input),
        Condition::And { conditions } => {
            let mut end = input;
            for sub in conditions.iter_mut() {
                end = process_condition(sub, end, graph, model, parent_table, next_plan_id)?;
            }
            Ok(end)
        }
        Condition::Or { conditions } => {
            process_or(conditions, input, graph, model, parent_table, next_plan_id)
        }
        Condition::CorrelatedSubquery { .. } => {
            process_correlated_subquery(condition, input, graph, model, parent_table, next_plan_id)
        }
    }
}

/// Port of `processCorrelatedSubquery`: builds the child's
/// `source -> connection -> [child joins]` spine, then a [`PlannerJoin`]
/// correlating `input` (parent) with the child end. Assigns a `plan_id`
/// (stored on the join and stamped onto the condition), and derives the
/// join's flippability/initial type from the operator and any manual `flip`:
/// `NOT EXISTS` is never flippable (semi); an explicit `flip` pins the type
/// (not flippable); an absent `flip` lets the planner decide (flippable, semi).
///
/// [`PlannerJoin`]: crate::planner_node::PlannerJoinNode
fn process_correlated_subquery(
    condition: &mut Condition,
    input: PlannerNode,
    graph: &mut PlannerGraph,
    model: &ConnectionCostModel,
    _parent_table: &str,
    next_plan_id: &mut i64,
) -> Result<PlannerNode, BuildPlanGraphError> {
    use zero_cache_protocol::ast::ExistsOp;

    let Condition::CorrelatedSubquery {
        related,
        op,
        flip,
        plan_id,
        ..
    } = condition
    else {
        unreachable!("process_correlated_subquery called on a non-correlated condition");
    };

    let child_table = related.subquery.table.clone();
    let child_connection = {
        let source = if graph.has_source(&child_table) {
            graph.get_source(&child_table)?
        } else {
            graph.add_source(&child_table, model.clone())?
        };
        source.connect(
            related.subquery.order_by.clone().unwrap_or_default(),
            related.subquery.where_.clone(),
            false,
            None, // no base constraints for EXISTS/NOT EXISTS
            if *op == ExistsOp::Exists {
                Some(1.0)
            } else {
                None
            },
        )
    };
    graph.connections.push(child_connection.clone());

    let mut child_end = child_connection;
    if let Some(child_where) = related.subquery.where_.as_mut() {
        child_end = process_condition(
            child_where,
            child_end,
            graph,
            model,
            &child_table,
            next_plan_id,
        )?;
    }

    let parent_constraint = related.correlation.parent_field.clone();
    let child_constraint = related.correlation.child_field.clone();

    let id = *next_plan_id;
    *next_plan_id += 1;
    *plan_id = Some(id);

    let is_not_exists = *op == ExistsOp::NotExists;
    let (flippable, initial_type) = if is_not_exists {
        (false, crate::planner_node::JoinType::Semi)
    } else {
        match *flip {
            Some(true) => (false, crate::planner_node::JoinType::Flipped),
            Some(false) => (false, crate::planner_node::JoinType::Semi),
            None => (true, crate::planner_node::JoinType::Semi),
        }
    };

    let join = PlannerNode::Join(std::rc::Rc::new(std::cell::RefCell::new(
        crate::planner_node::PlannerJoinNode::new_with_plan_id(
            Some(input.clone()),
            Some(child_end.clone()),
            flippable,
            initial_type,
            parent_constraint,
            child_constraint,
            id,
        ),
    )));
    graph.joins.push(join.clone());
    wire_output(&input, join.clone());
    wire_output(&child_end, join.clone());

    Ok(join)
}

/// Port of `processOr`: builds a fan-out/fan-in pair around the branches of a
/// disjunction that contain correlated subqueries. Only such branches become
/// graph structure (a branch of purely simple conditions contributes nothing);
/// if no branch has one, the `or` adds no structure and returns `input`
/// unchanged. Each qualifying branch is processed with the fan-out as its
/// input, then joined under a single fan-in.
///
/// Note: each branch ends up in `fan_out.outputs` twice — once via
/// `process_correlated_subquery`'s `wire_output(&input=fan_out, join)` and once
/// via the explicit `add_output` here. This duplication is faithful to
/// upstream (its `planner-builder.test.ts` documents and asserts it: outputs
/// length `>= 2` for a two-branch `or`), so it is replicated rather than
/// "fixed".
fn process_or(
    conditions: &mut [Condition],
    input: PlannerNode,
    graph: &mut PlannerGraph,
    model: &ConnectionCostModel,
    parent_table: &str,
    next_plan_id: &mut i64,
) -> Result<PlannerNode, BuildPlanGraphError> {
    if !conditions.iter().any(has_correlated_subquery) {
        return Ok(input);
    }

    let fan_out = PlannerNode::FanOut(std::rc::Rc::new(std::cell::RefCell::new(
        PlannerFanOutNode {
            input: Some(input.clone()),
            outputs: Vec::new(),
            is_unlimited: false,
        },
    )));
    graph.fan_outs.push(fan_out.clone());
    wire_output(&input, fan_out.clone());

    let mut branches = Vec::new();
    for sub in conditions.iter_mut() {
        if !has_correlated_subquery(sub) {
            continue;
        }
        let branch = process_condition(
            sub,
            fan_out.clone(),
            graph,
            model,
            parent_table,
            next_plan_id,
        )?;
        if let PlannerNode::FanOut(fo) = &fan_out {
            fo.borrow_mut().add_output(branch.clone());
        }
        branches.push(branch);
    }

    let fan_in = PlannerNode::FanIn(std::rc::Rc::new(std::cell::RefCell::new(
        PlannerFanInNode::new(branches.clone()),
    )));
    graph.fan_ins.push(fan_in.clone());
    for branch in &branches {
        wire_output(branch, fan_in.clone());
    }

    Ok(fan_in)
}

/// Port of `applyPlansToAST`: rewrites `ast` with the planner's decisions from
/// a planned `plans`. Collects the `plan_id`s of every join the planner left
/// in the `flipped` state, rewrites `ast.where_` via [`apply_to_condition`]
/// (setting each correlated-subquery's `flip` from that set), and recurses into
/// each `related` subquery using its matching `sub_plans` entry (keyed by the
/// subquery's alias, which every related subquery must have — matching
/// upstream's `must(alias)`).
///
/// This is the final consumer that ties the planner together: `build_plan_graph`
/// stamps `plan_id`s onto joins and conditions, `PlannerGraph::plan()` decides
/// which joins flip, and this reads those decisions back onto the AST. (Callers
/// that only have a flipped-id set, not a planned graph, can still call
/// [`apply_to_condition`] directly.)
pub fn apply_plans_to_ast(
    ast: &zero_cache_protocol::ast::Ast,
    plans: &Plans,
) -> Result<zero_cache_protocol::ast::Ast, BuildPlanGraphError> {
    let mut flipped_ids = BTreeSet::new();
    for join in &plans.plan.joins {
        if let PlannerNode::Join(node) = join {
            let node = node.borrow();
            if node.join_type() == crate::planner_node::JoinType::Flipped {
                if let Some(id) = node.plan_id() {
                    flipped_ids.insert(id);
                }
            }
        }
    }

    let mut out = ast.clone();
    out.where_ = ast
        .where_
        .as_ref()
        .map(|w| apply_to_condition(w, &flipped_ids));

    if let Some(related) = ast.related.as_ref() {
        let mut new_related = Vec::with_capacity(related.len());
        for csq in related {
            let alias = csq
                .subquery
                .alias
                .as_ref()
                .ok_or(BuildPlanGraphError::MissingAlias)?;
            let mut new_csq = csq.clone();
            if let Some(sub_plan) = plans.sub_plans.get(alias) {
                new_csq.subquery = Box::new(apply_plans_to_ast(&csq.subquery, sub_plan)?);
            }
            new_related.push(new_csq);
        }
        out.related = Some(new_related);
    }

    Ok(out)
}

/// Port of `planRecursively`: plans every `related` sub-plan (depth-first)
/// before planning the root graph, so a parent's cost estimation sees its
/// children already optimized.
fn plan_recursively(plans: &Plans) {
    for sub in plans.sub_plans.values() {
        plan_recursively(sub);
    }
    plans.plan.plan();
}

/// Port of `planQuery`: the planner's public entry point. Builds the plan
/// graph for `ast` (stamping `plan_id`s onto its `where_` correlated-subquery
/// conditions), runs the exhaustive flip-search planner over it and every
/// related sub-plan, then returns a rewritten AST whose correlated-subquery
/// `flip` flags reflect the planner's chosen joins. `ast` is mutated in place
/// with the `plan_id` stamps (matching upstream, which mutates its input);
/// the returned AST is the planned rewrite.
///
/// Fails only for the shapes `build_plan_graph` cannot yet build (a correlated
/// subquery inside `or`; a related subquery without an alias).
pub fn plan_query(
    ast: &mut zero_cache_protocol::ast::Ast,
    model: &ConnectionCostModel,
) -> Result<zero_cache_protocol::ast::Ast, BuildPlanGraphError> {
    let plans = build_plan_graph(ast, model, true, None)?;
    plan_recursively(&plans);
    apply_plans_to_ast(ast, &plans)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner_cost::{CostModelCost, FanoutConfidence, FanoutEst};
    use crate::planner_node::JoinOrConnection;
    use std::rc::Rc;
    use zero_cache_protocol::ast::{
        Ast, ColumnReference, CorrelatedSubquery, Correlation, Direction, LiteralValue,
        SimpleOperator, ValuePosition,
    };

    fn stub_model() -> ConnectionCostModel {
        Rc::new(|_t, _s, _f, _c| CostModelCost {
            startup_cost: 0.0,
            rows: 10.0,
            fanout: Rc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            }),
        })
    }

    fn simple() -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference { name: "a".into() }),
            right: ValuePosition::Literal(LiteralValue::Number(1.0)),
        }
    }

    fn correlated() -> Condition {
        Condition::CorrelatedSubquery {
            related: zero_cache_protocol::ast::CorrelatedSubquery {
                correlation: zero_cache_protocol::ast::Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["parentId".into()],
                },
                subquery: Box::new(zero_cache_protocol::ast::Ast::table("comments")),
                system: None,
                hidden: None,
            },
            op: zero_cache_protocol::ast::ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        }
    }

    #[test]
    fn has_correlated_subquery_is_true_for_a_bare_correlated_condition() {
        assert!(has_correlated_subquery(&correlated()));
    }

    #[test]
    fn has_correlated_subquery_is_false_for_a_simple_condition() {
        assert!(!has_correlated_subquery(&simple()));
    }

    #[test]
    fn has_correlated_subquery_recurses_into_and_or() {
        assert!(has_correlated_subquery(&Condition::And {
            conditions: vec![simple(), correlated()]
        }));
        assert!(has_correlated_subquery(&Condition::Or {
            conditions: vec![Condition::And {
                conditions: vec![simple(), correlated()]
            }]
        }));
        assert!(!has_correlated_subquery(&Condition::And {
            conditions: vec![simple(), simple()]
        }));
    }

    #[test]
    fn extract_constraint_builds_a_set_from_field_names() {
        let fields = vec!["issueID".to_string(), "projectID".to_string()];
        let constraint = extract_constraint(&fields, "issue");
        assert_eq!(
            constraint,
            ["issueID".to_string(), "projectID".to_string()]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn extract_constraint_is_empty_for_no_fields() {
        assert!(extract_constraint(&[], "issue").is_empty());
    }

    fn correlated_with_plan_id(plan_id: Option<i64>, where_: Option<Condition>) -> Condition {
        Condition::CorrelatedSubquery {
            related: zero_cache_protocol::ast::CorrelatedSubquery {
                correlation: zero_cache_protocol::ast::Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["parentId".into()],
                },
                subquery: Box::new(zero_cache_protocol::ast::Ast {
                    where_,
                    ..zero_cache_protocol::ast::Ast::table("comments")
                }),
                system: None,
                hidden: None,
            },
            op: zero_cache_protocol::ast::ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id,
        }
    }

    fn flip_of(condition: &Condition) -> Option<bool> {
        match condition {
            Condition::CorrelatedSubquery { flip, .. } => *flip,
            other => panic!("expected correlatedSubquery, got {other:?}"),
        }
    }

    #[test]
    fn build_plan_graph_builds_a_source_connection_terminus_spine() {
        let mut ast = Ast {
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            limit: Some(5.0),
            ..Ast::table("issue")
        };
        let plans = build_plan_graph(&mut ast, &stub_model(), true, None).unwrap();
        assert!(plans.plan.has_source("issue"));
        assert_eq!(plans.plan.connections.len(), 1);
        assert!(plans.sub_plans.is_empty());
        // The connection is the graph's single node before the terminus.
        assert_eq!(
            plans.plan.connections[0].closest_join_or_source(),
            JoinOrConnection::Connection
        );
        // The terminus was set and points at that connection.
        let terminus = plans.plan.terminus().expect("terminus set");
        assert_eq!(
            terminus.closest_join_or_source(),
            JoinOrConnection::Connection
        );
    }

    #[test]
    fn build_plan_graph_recursively_plans_related_subqueries_by_alias() {
        let comments = Ast {
            alias: Some("comments".into()),
            ..Ast::table("comment")
        };
        let mut ast = Ast {
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueId".into()],
                },
                subquery: Box::new(comments),
                system: None,
                hidden: None,
            }]),
            ..Ast::table("issue")
        };
        let plans = build_plan_graph(&mut ast, &stub_model(), true, None).unwrap();
        assert_eq!(plans.sub_plans.len(), 1);
        let sub = plans.sub_plans.get("comments").expect("aliased sub-plan");
        assert!(sub.plan.has_source("comment"));
        assert_eq!(sub.plan.connections.len(), 1);
    }

    #[test]
    fn build_plan_graph_builds_a_join_for_a_where_exists_and_stamps_plan_id() {
        // where: (simple AND exists(comments))
        let mut ast = Ast {
            where_: Some(Condition::And {
                conditions: vec![simple(), correlated()],
            }),
            ..Ast::table("issue")
        };
        let plans = build_plan_graph(&mut ast, &stub_model(), true, None).unwrap();
        // A join was built correlating the issue connection with the comments
        // connection: two connections (issue + comments) and one join.
        assert_eq!(plans.plan.connections.len(), 2);
        assert_eq!(plans.plan.joins.len(), 1);
        assert!(plans.plan.has_source("comments"));
        // The join carries plan_id 0 and, since flip was unset on an EXISTS,
        // is flippable and starts semi.
        let PlannerNode::Join(join) = &plans.plan.joins[0] else {
            panic!("expected a join node");
        };
        assert_eq!(join.borrow().plan_id(), Some(0));
        assert!(join.borrow().is_flippable());
        // The plan_id was stamped back onto the AST condition.
        let Some(Condition::And { conditions }) = &ast.where_ else {
            panic!("expected and");
        };
        let Condition::CorrelatedSubquery { plan_id, .. } = &conditions[1] else {
            panic!("expected correlatedSubquery");
        };
        assert_eq!(*plan_id, Some(0));
    }

    #[test]
    fn build_plan_graph_builds_fan_structure_for_a_correlated_subquery_inside_or() {
        // where: (exists(comments) OR exists(comments)) — two correlated
        // branches produce a fan-out + fan-in and two joins.
        let mut ast = Ast {
            where_: Some(Condition::Or {
                conditions: vec![correlated(), correlated()],
            }),
            ..Ast::table("issue")
        };
        let plans = build_plan_graph(&mut ast, &stub_model(), true, None).unwrap();
        assert_eq!(plans.plan.fan_outs.len(), 1);
        assert_eq!(plans.plan.fan_ins.len(), 1);
        assert_eq!(plans.plan.joins.len(), 2);
        // Faithful to upstream: each branch is added to fan_out.outputs twice.
        let PlannerNode::FanOut(fo) = &plans.plan.fan_outs[0] else {
            panic!("expected fan-out");
        };
        assert!(fo.borrow().outputs.len() >= 2);
    }

    #[test]
    fn build_plan_graph_adds_no_fan_structure_for_a_simple_only_or() {
        let mut ast = Ast {
            where_: Some(Condition::Or {
                conditions: vec![simple(), simple()],
            }),
            ..Ast::table("issue")
        };
        let plans = build_plan_graph(&mut ast, &stub_model(), true, None).unwrap();
        assert_eq!(plans.plan.fan_outs.len(), 0);
        assert_eq!(plans.plan.fan_ins.len(), 0);
        assert_eq!(plans.plan.joins.len(), 0);
    }

    #[test]
    fn apply_plans_to_ast_sets_flip_true_for_a_flipped_join() {
        // Build a graph with a flippable EXISTS join, then simulate the planner
        // choosing to flip it, and confirm apply_plans_to_ast reflects that on
        // the AST condition's `flip`.
        let mut ast = Ast {
            where_: Some(correlated()),
            ..Ast::table("issue")
        };
        let plans = build_plan_graph(&mut ast, &stub_model(), true, None).unwrap();
        let PlannerNode::Join(join) = &plans.plan.joins[0] else {
            panic!("expected a join");
        };
        join.borrow_mut().flip().unwrap();

        let planned = apply_plans_to_ast(&ast, &plans).unwrap();
        assert_eq!(flip_of(&planned.where_.unwrap()), Some(true));
    }

    #[test]
    fn apply_plans_to_ast_sets_flip_false_when_no_join_is_flipped() {
        let mut ast = Ast {
            where_: Some(correlated()),
            ..Ast::table("issue")
        };
        let plans = build_plan_graph(&mut ast, &stub_model(), true, None).unwrap();
        // Do not flip: the join stays semi.
        let planned = apply_plans_to_ast(&ast, &plans).unwrap();
        assert_eq!(flip_of(&planned.where_.unwrap()), Some(false));
    }

    #[test]
    fn plan_query_runs_the_full_pipeline_and_decides_a_flip() {
        let mut ast = Ast {
            where_: Some(correlated()),
            ..Ast::table("issue")
        };
        let planned = plan_query(&mut ast, &stub_model()).unwrap();
        // The whole build -> plan -> applyPlansToAST chain ran and stamped a
        // concrete flip decision on the output (the input's flip was None).
        assert!(flip_of(&planned.where_.unwrap()).is_some());
        // build_plan_graph stamped plan_id 0 onto the input AST condition.
        let Some(Condition::CorrelatedSubquery { plan_id, .. }) = &ast.where_ else {
            panic!("expected correlatedSubquery");
        };
        assert_eq!(*plan_id, Some(0));
    }

    #[test]
    fn plan_query_handles_a_realistic_nested_query_end_to_end() {
        // A realistic shape composing every ported construction path in one
        // query: a root with a `related` subquery, and a `where_` that ANDs a
        // simple filter with an EXISTS. Exercises build_plan_graph's spine +
        // related sub-planning + processAnd + processCorrelatedSubquery, then
        // plan() over root and sub-plan, then apply_plans_to_ast's recursion.
        let comments = Ast {
            alias: Some("comments".into()),
            ..Ast::table("comment")
        };
        let mut ast = Ast {
            where_: Some(Condition::And {
                conditions: vec![simple(), correlated()],
            }),
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueId".into()],
                },
                subquery: Box::new(comments),
                system: None,
                hidden: None,
            }]),
            ..Ast::table("issue")
        };

        let planned = plan_query(&mut ast, &stub_model()).unwrap();

        // The EXISTS join in the root `where_` got a concrete flip decision...
        let Some(Condition::And { conditions }) = &planned.where_ else {
            panic!("expected and");
        };
        assert!(flip_of(&conditions[1]).is_some());
        // ...the simple branch is untouched...
        assert_eq!(conditions[0], simple());
        // ...and the related subquery is preserved by alias through the rewrite.
        let related = planned.related.as_ref().expect("related preserved");
        assert_eq!(related.len(), 1);
        assert_eq!(related[0].subquery.alias.as_deref(), Some("comments"));
    }

    #[test]
    fn build_plan_graph_errors_on_a_related_subquery_without_alias() {
        let mut ast = Ast {
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueId".into()],
                },
                subquery: Box::new(Ast::table("comment")), // no alias
                system: None,
                hidden: None,
            }]),
            ..Ast::table("issue")
        };
        match build_plan_graph(&mut ast, &stub_model(), true, None) {
            Err(BuildPlanGraphError::MissingAlias) => {}
            other => panic!("expected MissingAlias, got {:?}", other.is_ok()),
        }
    }

    #[test]
    fn apply_to_condition_leaves_simple_conditions_unchanged() {
        let c = simple();
        assert_eq!(apply_to_condition(&c, &BTreeSet::new()), c);
    }

    #[test]
    fn apply_to_condition_sets_flip_true_when_plan_id_is_flipped() {
        let c = correlated_with_plan_id(Some(7), None);
        let flipped = BTreeSet::from([7]);
        assert_eq!(flip_of(&apply_to_condition(&c, &flipped)), Some(true));
    }

    #[test]
    fn apply_to_condition_sets_flip_false_when_plan_id_absent_or_unflipped() {
        // plan_id present but not in the flipped set.
        let c = correlated_with_plan_id(Some(7), None);
        assert_eq!(
            flip_of(&apply_to_condition(&c, &BTreeSet::from([9]))),
            Some(false)
        );
        // no plan_id at all -> never flipped.
        let no_id = correlated_with_plan_id(None, None);
        assert_eq!(
            flip_of(&apply_to_condition(&no_id, &BTreeSet::from([7]))),
            Some(false)
        );
    }

    #[test]
    fn apply_to_condition_recurses_into_subquery_where_and_and_or() {
        // A flipped correlated subquery whose own where_ contains another
        // correlated subquery with a different (also-flipped) plan id.
        let inner = correlated_with_plan_id(Some(2), None);
        let outer = correlated_with_plan_id(Some(1), Some(inner));
        let tree = Condition::And {
            conditions: vec![simple(), outer],
        };
        let flipped = BTreeSet::from([1, 2]);
        let Condition::And { conditions } = apply_to_condition(&tree, &flipped) else {
            panic!("expected and");
        };
        assert_eq!(conditions[0], simple());
        assert_eq!(flip_of(&conditions[1]), Some(true));
        // The nested subquery's where_ was rewritten too.
        let Condition::CorrelatedSubquery { related, .. } = &conditions[1] else {
            panic!("expected correlatedSubquery");
        };
        let nested_where = related.subquery.where_.as_ref().expect("nested where");
        assert_eq!(flip_of(nested_where), Some(true));
    }
}
