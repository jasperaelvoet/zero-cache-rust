//! Port of `PlannerSource` (`planner-source.ts`) and `PlannerGraph`'s
//! bookkeeping half (`planner-graph.ts`) — the source registry and
//! node-collection container the graph-construction functions in
//! `planner_builder.rs` would build into. Tractable now that
//! `PlannerConnectionNode::new` (`planner_node.rs`) already exists to back
//! `PlannerSource::connect`.
//!
//! `PlanState` snapshot/restore (`capturePlanningSnapshot`/
//! `restorePlanningSnapshot`) and the cost/constraint entry points
//! (`propagateConstraints`/`getTotalCost`) needed to drive planning from
//! the graph level are ported below.
//!
//! `PlannerGraph::plan()` itself — the real 2^n exhaustive-search planning
//! algorithm over flippable joins (`planner-graph.ts` lines ~256-382) — is
//! now ported too, along with its `FOFIInfo`/`build_fofi_cache`/
//! `check_and_convert_fofi`/`find_fi_and_joins` support (a BFS from each
//! fan-out's outputs to its paired fan-in, caching the joins found along
//! the way, via the `output` back-links `planner_node.rs` gained for
//! exactly this purpose). This closes out `zql/src/planner`'s pure-logic
//! surface entirely.

use std::collections::BTreeMap;
use std::rc::Rc;

use zero_cache_protocol::ast::{Condition, Ordering};

use crate::planner_constraint::PlannerConstraint;
use crate::planner_node::{
    ConnectionCostModel, FanInType, JoinType, PlannerConnectionNode, PlannerFanOutNode, PlannerNode,
};

/// Port of `PlannerSource`: a thin factory tying a table name to its cost
/// model, producing `PlannerConnection` nodes via `connect`.
pub struct PlannerSource {
    name: String,
    model: ConnectionCostModel,
}

impl PlannerSource {
    pub fn new(name: String, model: ConnectionCostModel) -> Self {
        PlannerSource { name, model }
    }

    /// Port of `PlannerSource#connect`.
    pub fn connect(
        &self,
        sort: Ordering,
        filters: Option<Condition>,
        is_root: bool,
        base_constraints: Option<PlannerConstraint>,
        limit: Option<f64>,
    ) -> PlannerNode {
        PlannerNode::Connection(Rc::new(std::cell::RefCell::new(
            PlannerConnectionNode::new(
                self.name.clone(),
                self.model.clone(),
                sort,
                filters,
                is_root,
                base_constraints,
                limit,
                None,
            ),
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlannerGraphError {
    #[error("Source {0:?} already exists in the graph")]
    SourceAlreadyExists(String),
    #[error("Source {0:?} not found in the graph")]
    SourceNotFound(String),
    #[error("Snapshot shape does not match this graph's current node counts")]
    SnapshotShapeMismatch,
}

/// Port of `PlanState` — a snapshot of every join/fan-out/fan-in/
/// connection's mutable planning state, captured/restored by
/// `PlannerGraph` while backtracking over flip-pattern trials during
/// `plan()`. Snapshots are only valid against the exact graph they were
/// captured from (same node count, same order) — `restore_planning_snapshot`
/// validates this (`#validateSnapshotShape`).
pub struct PlanState {
    connection_limits: Vec<Option<f64>>,
    connection_constraints: Vec<BTreeMap<String, Option<PlannerConstraint>>>,
    join_types: Vec<JoinType>,
    fan_out_unlimited: Vec<bool>,
    fan_in_types: Vec<FanInType>,
}

/// Port of `PlannerGraph`'s bookkeeping half — source registry, node
/// collections (`joins`/`fanOuts`/`fanIns`/`connections`), and the
/// terminus slot. See module doc for what's NOT ported (`plan()` itself).
pub struct PlannerGraph {
    sources: BTreeMap<String, PlannerSource>,
    terminus: Option<PlannerNode>,
    pub joins: Vec<PlannerNode>,
    pub fan_outs: Vec<PlannerNode>,
    pub fan_ins: Vec<PlannerNode>,
    pub connections: Vec<PlannerNode>,
}

impl Default for PlannerGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl PlannerGraph {
    pub fn new() -> Self {
        PlannerGraph {
            sources: BTreeMap::new(),
            terminus: None,
            joins: Vec::new(),
            fan_outs: Vec::new(),
            fan_ins: Vec::new(),
            connections: Vec::new(),
        }
    }

    /// Port of `addSource`. Errors (matching upstream's `assert`) if a
    /// source for `name` is already registered.
    pub fn add_source(
        &mut self,
        name: &str,
        model: ConnectionCostModel,
    ) -> Result<&PlannerSource, PlannerGraphError> {
        if self.sources.contains_key(name) {
            return Err(PlannerGraphError::SourceAlreadyExists(name.to_string()));
        }
        self.sources.insert(
            name.to_string(),
            PlannerSource::new(name.to_string(), model),
        );
        Ok(self.sources.get(name).unwrap())
    }

    /// Port of `getSource`.
    pub fn get_source(&self, name: &str) -> Result<&PlannerSource, PlannerGraphError> {
        self.sources
            .get(name)
            .ok_or_else(|| PlannerGraphError::SourceNotFound(name.to_string()))
    }

    /// Port of `hasSource`.
    pub fn has_source(&self, name: &str) -> bool {
        self.sources.contains_key(name)
    }

    /// Port of `setTerminus`.
    pub fn set_terminus(&mut self, terminus: PlannerNode) {
        self.terminus = Some(terminus);
    }

    pub fn terminus(&self) -> Option<&PlannerNode> {
        self.terminus.as_ref()
    }

    /// Port of `resetPlanningState`: resets every join/fan-out/fan-in/
    /// connection's mutable planning state back to initial values,
    /// leaving graph STRUCTURE (which nodes exist, how they're wired)
    /// unchanged — allows replanning the same query graph with a
    /// different strategy.
    pub fn reset_planning_state(&self) {
        for j in &self.joins {
            let PlannerNode::Join(n) = j else { continue };
            n.borrow_mut().reset();
        }
        for fo in &self.fan_outs {
            let PlannerNode::FanOut(n) = fo else { continue };
            n.borrow_mut().reset();
        }
        for fi in &self.fan_ins {
            let PlannerNode::FanIn(n) = fi else { continue };
            n.borrow_mut().reset();
        }
        for c in &self.connections {
            let PlannerNode::Connection(n) = c else {
                continue;
            };
            n.borrow_mut().reset();
        }
    }

    /// Port of `capturePlanningSnapshot`.
    pub fn capture_planning_snapshot(&self) -> PlanState {
        PlanState {
            connection_limits: self
                .connections
                .iter()
                .map(|c| {
                    let PlannerNode::Connection(n) = c else {
                        unreachable!("graph.connections must only contain Connection nodes")
                    };
                    n.borrow().limit
                })
                .collect(),
            connection_constraints: self
                .connections
                .iter()
                .map(|c| {
                    let PlannerNode::Connection(n) = c else {
                        unreachable!("graph.connections must only contain Connection nodes")
                    };
                    n.borrow().capture_constraints()
                })
                .collect(),
            join_types: self
                .joins
                .iter()
                .map(|j| {
                    let PlannerNode::Join(n) = j else {
                        unreachable!("graph.joins must only contain Join nodes")
                    };
                    n.borrow().join_type()
                })
                .collect(),
            fan_out_unlimited: self
                .fan_outs
                .iter()
                .map(|fo| {
                    let PlannerNode::FanOut(n) = fo else {
                        unreachable!("graph.fan_outs must only contain FanOut nodes")
                    };
                    n.borrow().is_unlimited
                })
                .collect(),
            fan_in_types: self
                .fan_ins
                .iter()
                .map(|fi| {
                    let PlannerNode::FanIn(n) = fi else {
                        unreachable!("graph.fan_ins must only contain FanIn nodes")
                    };
                    n.borrow().fan_in_type
                })
                .collect(),
        }
    }

    /// Port of `restorePlanningSnapshot` (including `#validateSnapshotShape`):
    /// errors if `state` wasn't captured from a graph with the exact same
    /// node counts as `self` currently has, otherwise writes every
    /// captured value back (`#restoreConnections`/`#restoreJoins`/
    /// `#restoreFanNodes`).
    pub fn restore_planning_snapshot(&self, state: &PlanState) -> Result<(), PlannerGraphError> {
        if state.connection_limits.len() != self.connections.len()
            || state.join_types.len() != self.joins.len()
            || state.fan_out_unlimited.len() != self.fan_outs.len()
            || state.fan_in_types.len() != self.fan_ins.len()
        {
            return Err(PlannerGraphError::SnapshotShapeMismatch);
        }
        for (c, (limit, constraints)) in self.connections.iter().zip(
            state
                .connection_limits
                .iter()
                .zip(state.connection_constraints.iter()),
        ) {
            let PlannerNode::Connection(n) = c else {
                unreachable!()
            };
            let mut n = n.borrow_mut();
            n.limit = *limit;
            n.restore_constraints(constraints.clone());
        }
        for (j, join_type) in self.joins.iter().zip(state.join_types.iter()) {
            let PlannerNode::Join(n) = j else {
                unreachable!()
            };
            n.borrow_mut().restore_type(*join_type);
        }
        for (fo, unlimited) in self.fan_outs.iter().zip(state.fan_out_unlimited.iter()) {
            let PlannerNode::FanOut(n) = fo else {
                unreachable!()
            };
            n.borrow_mut().is_unlimited = *unlimited;
        }
        for (fi, fan_in_type) in self.fan_ins.iter().zip(state.fan_in_types.iter()) {
            let PlannerNode::FanIn(n) = fi else {
                unreachable!()
            };
            n.borrow_mut().fan_in_type = *fan_in_type;
        }
        Ok(())
    }

    /// Port of `propagateConstraints`: kicks off constraint propagation
    /// from the terminus. Panics if no terminus is set (matching
    /// upstream's `must(this.#terminus)`).
    pub fn propagate_constraints(&self) {
        self.terminus
            .as_ref()
            .expect("PlannerGraph.terminus must be set")
            .start_propagate_constraints();
    }

    /// Port of `getTotalCost`: the total estimated cost of the current
    /// plan, `startup_cost + cost`. Panics if no terminus is set.
    pub fn get_total_cost(&self) -> f64 {
        let estimate = self
            .terminus
            .as_ref()
            .expect("PlannerGraph.terminus must be set")
            .start_estimate_cost();
        estimate.startup_cost + estimate.cost
    }

    /// Port of `plan`: the main planning algorithm using exhaustive join-
    /// flip enumeration. Enumerates all `2^n` flip patterns for the graph's
    /// flippable joins (each pattern a candidate plan), evaluates each
    /// plan's total cost, and restores the graph to whichever plan scored
    /// lowest. Skips optimization entirely (leaving the graph as-is) if
    /// there are more than [`MAX_FLIPPABLE_JOINS`] flippable joins — 2^n
    /// would be too many plans to evaluate (matches upstream's `lc?.warn?.()`
    /// early-return; this port has no `LogContext` equivalent to warn
    /// through, so it silently no-ops instead, consistent with this port's
    /// established convention for debug-only logging elsewhere).
    pub fn plan(&self) {
        let flippable_joins: Vec<PlannerNode> = self
            .joins
            .iter()
            .filter(|j| {
                let PlannerNode::Join(n) = j else {
                    return false;
                };
                n.borrow().is_flippable()
            })
            .cloned()
            .collect();

        if flippable_joins.len() > MAX_FLIPPABLE_JOINS {
            return;
        }

        let fofi_cache = build_fofi_cache(self);

        let num_patterns: u32 = if flippable_joins.is_empty() {
            0
        } else {
            1u32 << flippable_joins.len()
        };
        let mut best_cost = f64::INFINITY;
        let mut best_plan: Option<PlanState> = None;

        for pattern in 0..num_patterns {
            self.reset_planning_state();

            for (i, j) in flippable_joins.iter().enumerate() {
                if pattern & (1 << i) != 0 {
                    let PlannerNode::Join(n) = j else {
                        unreachable!()
                    };
                    n.borrow_mut()
                        .flip()
                        .expect("flippable_joins only contains flippable joins");
                }
            }

            check_and_convert_fofi(self, &fofi_cache);
            propagate_unlimit_for_flipped_joins(self);
            self.propagate_constraints();

            let total_cost = self.get_total_cost();
            if total_cost < best_cost {
                best_cost = total_cost;
                best_plan = Some(self.capture_planning_snapshot());
            }
        }

        if let Some(best_plan) = best_plan {
            self.restore_planning_snapshot(&best_plan)
                .expect("a snapshot captured from this graph must match its own shape");
            self.propagate_constraints();
        } else {
            assert_eq!(
                num_patterns, 0,
                "no plan was found but flippable joins did exist!"
            );
        }
    }
}

/// Port of `MAX_FLIPPABLE_JOINS`: with `n` flippable joins, `plan()`
/// explores `2^n` candidate plans — 10 joins is ~100-200ms, 12 is
/// ~400ms-1s, so beyond this the search is skipped entirely.
const MAX_FLIPPABLE_JOINS: usize = 9;

/// Port of `FOFIInfo`: cached information about a FanOut→FanIn boundary —
/// which FanIn a FanOut's branches eventually converge on, and which joins
/// sit between them. Computed once per `plan()` call (`build_fofi_cache`)
/// to avoid redundant BFS traversals on every flip-pattern trial.
pub struct FOFIInfo {
    pub fi: Option<PlannerNode>,
    pub joins_between: Vec<PlannerNode>,
}

/// Port of `buildFOFICache`: computes a [`FOFIInfo`] for every fan-out in
/// the graph, in the same order as `graph.fan_outs` (used to zip the two
/// back together in `check_and_convert_fofi`, avoiding the need for a
/// `Rc`-keyed map `PlannerNode`'s `Fn`-holding structs can't support).
pub fn build_fofi_cache(graph: &PlannerGraph) -> Vec<FOFIInfo> {
    graph
        .fan_outs
        .iter()
        .map(|fo| {
            let PlannerNode::FanOut(n) = fo else {
                unreachable!("graph.fan_outs must only contain FanOut nodes")
            };
            find_fi_and_joins(&n.borrow())
        })
        .collect()
}

/// Port of `findFIAndJoins`: BFS from a FanOut's outputs, via `output`
/// links, to find the FanIn its branches converge on and every join
/// encountered along the way. A nested FanOut's outputs are queued too
/// (its own inner region belongs to its own cache entry, but a join
/// between the outer FanOut and its FanIn can still pass through it).
/// Visited-tracking uses `ptr_eq` linear scan (matches this port's
/// established pattern elsewhere for `PlannerNode`, which can't be used as
/// a `HashSet`/`HashMap` key since it wraps non-`Hash` `Rc<dyn Fn>` closures).
fn find_fi_and_joins(fan_out: &PlannerFanOutNode) -> FOFIInfo {
    let mut joins_between = Vec::new();
    let mut fi = None;
    let mut queue: Vec<PlannerNode> = fan_out.outputs.clone();
    let mut visited: Vec<PlannerNode> = Vec::new();
    let mut i = 0;
    while i < queue.len() {
        let node = queue[i].clone();
        i += 1;
        if visited.iter().any(|v| v.ptr_eq(&node)) {
            continue;
        }
        visited.push(node.clone());
        match &node {
            PlannerNode::Join(j) => {
                joins_between.push(node.clone());
                let output = j
                    .borrow()
                    .output
                    .clone()
                    .expect("PlannerJoinNode.output must be set before FOFI search");
                queue.push(output);
            }
            PlannerNode::FanOut(fo) => {
                queue.extend(fo.borrow().outputs.clone());
            }
            PlannerNode::FanIn(_) => {
                fi = Some(node.clone());
            }
            PlannerNode::Connection(_) | PlannerNode::Terminus(_) => {}
        }
    }
    FOFIInfo { fi, joins_between }
}

/// Port of `checkAndConvertFOFI`: for every fan-out whose FO→FI region
/// contains a flipped join, converts that fan-out to UFO and its paired
/// fan-in to UFI (a flipped join's child branch is no longer limited, so
/// the fan-out/fan-in surrounding it can no longer assume only one branch
/// executes per row either). Must run after join flipping and before
/// `propagate_constraints`.
pub fn check_and_convert_fofi(graph: &PlannerGraph, fofi_cache: &[FOFIInfo]) {
    for (fo, info) in graph.fan_outs.iter().zip(fofi_cache.iter()) {
        let has_flipped_join = info.joins_between.iter().any(|j| {
            let PlannerNode::Join(n) = j else {
                unreachable!()
            };
            n.borrow().join_type() == JoinType::Flipped
        });
        if let (Some(fi), true) = (&info.fi, has_flipped_join) {
            let PlannerNode::FanOut(n) = fo else {
                unreachable!()
            };
            n.borrow_mut().convert_to_ufo();
            let PlannerNode::FanIn(fin) = fi else {
                unreachable!()
            };
            fin.borrow_mut().convert_to_ufi();
        }
    }
}

/// Port of `propagateUnlimitForFlippedJoins`: after a trial flip pattern
/// is applied, any join that ended up `Flipped` needs its unlimit
/// propagated down to its child branch (a flipped join's child scan can
/// no longer rely on a LIMIT, since the join direction reversed which
/// side drives the scan). Iterates every join in the graph, not just the
/// ones that were just flipped, matching upstream (idempotent — a
/// `Semi` join is simply skipped).
pub fn propagate_unlimit_for_flipped_joins(graph: &PlannerGraph) {
    for j in &graph.joins {
        let PlannerNode::Join(n) = j else { continue };
        if n.borrow().join_type() == JoinType::Flipped {
            j.propagate_unlimit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner_cost::{CostModelCost, FanoutConfidence, FanoutEst};
    use crate::planner_node::JoinType;
    use std::cell::RefCell;

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

    #[test]
    fn add_source_then_get_source_round_trips() {
        let mut graph = PlannerGraph::new();
        graph.add_source("issue", stub_model()).unwrap();
        assert!(graph.has_source("issue"));
        assert!(graph.get_source("issue").is_ok());
        assert!(!graph.has_source("comment"));
    }

    #[test]
    fn add_source_errors_on_a_duplicate_name() {
        let mut graph = PlannerGraph::new();
        graph.add_source("issue", stub_model()).unwrap();
        let err = graph
            .add_source("issue", stub_model())
            .map(|_| ())
            .unwrap_err();
        assert_eq!(
            err,
            PlannerGraphError::SourceAlreadyExists("issue".to_string())
        );
    }

    #[test]
    fn get_source_errors_when_missing() {
        let graph = PlannerGraph::new();
        let err = graph.get_source("nope").map(|_| ()).unwrap_err();
        assert_eq!(err, PlannerGraphError::SourceNotFound("nope".to_string()));
    }

    #[test]
    fn source_connect_produces_a_real_connection_node() {
        let mut graph = PlannerGraph::new();
        let source = graph.add_source("issue", stub_model()).unwrap();
        let node = source.connect(vec![], None, true, None, None);
        assert_eq!(node.node_type(), crate::planner_node::NodeType::Connection);
    }

    #[test]
    fn set_terminus_and_terminus_round_trip() {
        let mut graph = PlannerGraph::new();
        assert!(graph.terminus().is_none());
        let source = graph.add_source("issue", stub_model()).unwrap();
        let conn = source.connect(vec![], None, true, None, None);
        let terminus = PlannerNode::Terminus(Rc::new(std::cell::RefCell::new(
            crate::planner_node::PlannerTerminusNode {
                input: Some(conn),
                pinned: true,
            },
        )));
        graph.set_terminus(terminus);
        assert!(graph.terminus().is_some());
    }

    #[test]
    fn reset_planning_state_resets_every_collected_node_kind() {
        let mut graph = PlannerGraph::new();
        let source = graph.add_source("issue", stub_model()).unwrap();
        let conn = source.connect(vec![], None, false, None, Some(5.0));
        let PlannerNode::Connection(conn_rc) = &conn else {
            unreachable!()
        };
        conn_rc.borrow_mut().unlimit();
        assert_eq!(conn_rc.borrow().limit, None);
        graph.connections.push(conn);

        let join_rc = Rc::new(std::cell::RefCell::new(
            crate::planner_node::PlannerJoinNode::new(
                None,
                None,
                true,
                JoinType::Semi,
                vec![],
                vec![],
            ),
        ));
        join_rc.borrow_mut().flip().unwrap();
        graph.joins.push(PlannerNode::Join(join_rc.clone()));

        graph.reset_planning_state();

        let PlannerNode::Connection(conn_rc2) = &graph.connections[0] else {
            unreachable!()
        };
        assert_eq!(
            conn_rc2.borrow().limit,
            Some(5.0),
            "connection's limit must be restored to base_limit"
        );
        assert_eq!(
            join_rc.borrow().join_type(),
            JoinType::Semi,
            "join's type must be reset"
        );
    }

    #[test]
    fn capture_and_restore_planning_snapshot_round_trips_every_node_kind() {
        let mut graph = PlannerGraph::new();
        let source = graph.add_source("issue", stub_model()).unwrap();
        let conn = source.connect(vec![], None, false, None, Some(5.0));
        let PlannerNode::Connection(conn_rc) = &conn else {
            unreachable!()
        };
        graph.connections.push(conn.clone());

        let join_rc = Rc::new(std::cell::RefCell::new(
            crate::planner_node::PlannerJoinNode::new(
                None,
                None,
                true,
                JoinType::Semi,
                vec![],
                vec![],
            ),
        ));
        graph.joins.push(PlannerNode::Join(join_rc.clone()));

        let fan_out_rc = Rc::new(std::cell::RefCell::new(
            crate::planner_node::PlannerFanOutNode {
                input: None,
                outputs: vec![],
                is_unlimited: false,
            },
        ));
        graph.fan_outs.push(PlannerNode::FanOut(fan_out_rc.clone()));

        let fan_in_rc = Rc::new(std::cell::RefCell::new(
            crate::planner_node::PlannerFanInNode::new(vec![]),
        ));
        graph.fan_ins.push(PlannerNode::FanIn(fan_in_rc.clone()));

        let snapshot = graph.capture_planning_snapshot();

        conn_rc.borrow_mut().limit = None;
        join_rc.borrow_mut().flip().unwrap();
        fan_out_rc.borrow_mut().convert_to_ufo();
        fan_in_rc.borrow_mut().fan_in_type = crate::planner_node::FanInType::Ufi;

        graph.restore_planning_snapshot(&snapshot).unwrap();

        assert_eq!(conn_rc.borrow().limit, Some(5.0));
        assert_eq!(join_rc.borrow().join_type(), JoinType::Semi);
        assert!(!fan_out_rc.borrow().is_unlimited);
        assert_eq!(
            fan_in_rc.borrow().fan_in_type,
            crate::planner_node::FanInType::Fi
        );
    }

    #[test]
    fn restore_planning_snapshot_errors_on_shape_mismatch() {
        let mut graph = PlannerGraph::new();
        let snapshot = graph.capture_planning_snapshot();
        let source = graph.add_source("issue", stub_model()).unwrap();
        let conn = source.connect(vec![], None, false, None, Some(5.0));
        graph.connections.push(conn);
        let err = graph.restore_planning_snapshot(&snapshot).unwrap_err();
        assert_eq!(err, PlannerGraphError::SnapshotShapeMismatch);
    }

    #[test]
    fn propagate_constraints_and_get_total_cost_drive_from_the_terminus() {
        let mut graph = PlannerGraph::new();
        let source = graph.add_source("issue", stub_model()).unwrap();
        let conn = source.connect(vec![], None, true, None, None);
        let terminus = PlannerNode::Terminus(Rc::new(std::cell::RefCell::new(
            crate::planner_node::PlannerTerminusNode {
                input: Some(conn),
                pinned: true,
            },
        )));
        graph.set_terminus(terminus);

        graph.propagate_constraints();
        let cost = graph.get_total_cost();
        assert_eq!(cost, 0.0, "a bare connection's own `cost` field (distinct from scan_est/rows) is 0 with no startup cost");
    }

    #[test]
    #[should_panic(expected = "PlannerGraph.terminus must be set")]
    fn get_total_cost_panics_without_a_terminus() {
        let graph = PlannerGraph::new();
        graph.get_total_cost();
    }

    #[test]
    fn propagate_unlimit_for_flipped_joins_only_touches_flipped_joins() {
        let mut graph = PlannerGraph::new();
        graph.add_source("issue", stub_model()).unwrap();
        let child_conn =
            graph
                .get_source("issue")
                .unwrap()
                .connect(vec![], None, false, None, Some(3.0));
        let PlannerNode::Connection(child_rc) = &child_conn else {
            unreachable!()
        };

        let semi_join_rc = Rc::new(std::cell::RefCell::new(
            crate::planner_node::PlannerJoinNode::new(
                None,
                Some(child_conn.clone()),
                true,
                JoinType::Semi,
                vec![],
                vec![],
            ),
        ));
        graph.joins.push(PlannerNode::Join(semi_join_rc));

        let flipped_child =
            graph
                .get_source("issue")
                .unwrap()
                .connect(vec![], None, false, None, Some(3.0));
        let PlannerNode::Connection(flipped_child_rc) = &flipped_child else {
            unreachable!()
        };
        let flipped_join_rc = Rc::new(std::cell::RefCell::new(
            crate::planner_node::PlannerJoinNode::new(
                None,
                Some(flipped_child.clone()),
                true,
                JoinType::Semi,
                vec![],
                vec![],
            ),
        ));
        flipped_join_rc.borrow_mut().flip().unwrap();
        graph.joins.push(PlannerNode::Join(flipped_join_rc));

        propagate_unlimit_for_flipped_joins(&graph);

        assert_eq!(
            child_rc.borrow().limit,
            Some(3.0),
            "semi join's child must be untouched"
        );
        assert_eq!(
            flipped_child_rc.borrow().limit,
            None,
            "flipped join's child must have its limit cleared"
        );
    }

    fn model_with_rows(rows: f64) -> ConnectionCostModel {
        Rc::new(move |_t, _s, _f, _c| CostModelCost {
            startup_cost: 0.0,
            rows,
            fanout: Rc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            }),
        })
    }

    #[test]
    fn find_fi_and_joins_bfs_finds_the_paired_fan_in_and_every_join_between() {
        let parent = PlannerNode::Connection(Rc::new(RefCell::new(PlannerConnectionNode::new(
            "issue".into(),
            model_with_rows(10.0),
            vec![],
            None,
            true,
            None,
            None,
            None,
        ))));
        let child1 = PlannerNode::Connection(Rc::new(RefCell::new(PlannerConnectionNode::new(
            "comment".into(),
            model_with_rows(10.0),
            vec![],
            None,
            false,
            None,
            None,
            None,
        ))));
        let child2 = PlannerNode::Connection(Rc::new(RefCell::new(PlannerConnectionNode::new(
            "label".into(),
            model_with_rows(10.0),
            vec![],
            None,
            false,
            None,
            None,
            None,
        ))));

        let join1_rc = Rc::new(RefCell::new(crate::planner_node::PlannerJoinNode::new(
            Some(parent.clone()),
            Some(child1),
            true,
            JoinType::Semi,
            vec![],
            vec![],
        )));
        let join1 = PlannerNode::Join(join1_rc.clone());
        let join2_rc = Rc::new(RefCell::new(crate::planner_node::PlannerJoinNode::new(
            Some(parent.clone()),
            Some(child2),
            true,
            JoinType::Semi,
            vec![],
            vec![],
        )));
        let join2 = PlannerNode::Join(join2_rc.clone());

        let fan_out = PlannerFanOutNode {
            input: Some(parent),
            outputs: vec![join1.clone(), join2.clone()],
            is_unlimited: false,
        };

        let fan_in_rc = Rc::new(RefCell::new(crate::planner_node::PlannerFanInNode::new(
            vec![join1.clone(), join2.clone()],
        )));
        let fan_in = PlannerNode::FanIn(fan_in_rc);
        join1_rc.borrow_mut().set_output(fan_in.clone());
        join2_rc.borrow_mut().set_output(fan_in.clone());

        let info = find_fi_and_joins(&fan_out);
        assert!(info.fi.is_some_and(|fi| fi.ptr_eq(&fan_in)));
        assert_eq!(info.joins_between.len(), 2);
        assert!(info.joins_between.iter().any(|j| j.ptr_eq(&join1)));
        assert!(info.joins_between.iter().any(|j| j.ptr_eq(&join2)));
    }

    #[test]
    fn check_and_convert_fofi_converts_fo_fi_to_ufo_ufi_when_a_join_between_is_flipped() {
        let mut graph = PlannerGraph::new();
        let parent = PlannerNode::Connection(Rc::new(RefCell::new(PlannerConnectionNode::new(
            "issue".into(),
            model_with_rows(10.0),
            vec![],
            None,
            true,
            None,
            None,
            None,
        ))));
        let child = PlannerNode::Connection(Rc::new(RefCell::new(PlannerConnectionNode::new(
            "comment".into(),
            model_with_rows(10.0),
            vec![],
            None,
            false,
            None,
            None,
            None,
        ))));

        let join_rc = Rc::new(RefCell::new(crate::planner_node::PlannerJoinNode::new(
            Some(parent.clone()),
            Some(child),
            true,
            JoinType::Semi,
            vec![],
            vec![],
        )));
        let join = PlannerNode::Join(join_rc.clone());
        graph.joins.push(join.clone());

        let fan_out_rc = Rc::new(RefCell::new(PlannerFanOutNode {
            input: Some(parent),
            outputs: vec![join.clone()],
            is_unlimited: false,
        }));
        let fan_out = PlannerNode::FanOut(fan_out_rc.clone());
        graph.fan_outs.push(fan_out);

        let fan_in_rc = Rc::new(RefCell::new(crate::planner_node::PlannerFanInNode::new(
            vec![join.clone()],
        )));
        let fan_in = PlannerNode::FanIn(fan_in_rc.clone());
        join_rc.borrow_mut().set_output(fan_in);
        graph.fan_ins.push(PlannerNode::FanIn(fan_in_rc.clone()));

        let cache = build_fofi_cache(&graph);
        assert_eq!(cache.len(), 1);

        // Before flipping: FO/FI stay as-is.
        check_and_convert_fofi(&graph, &cache);
        assert!(!fan_out_rc.borrow().is_unlimited);
        assert_eq!(fan_in_rc.borrow().fan_in_type, FanInType::Fi);

        join_rc.borrow_mut().flip().unwrap();
        check_and_convert_fofi(&graph, &cache);
        assert!(
            fan_out_rc.borrow().is_unlimited,
            "FO must convert to UFO once a join between it and its FI is flipped"
        );
        assert_eq!(
            fan_in_rc.borrow().fan_in_type,
            FanInType::Ufi,
            "FI must convert to UFI too"
        );
    }

    #[test]
    fn plan_flips_a_single_join_when_it_lowers_total_cost() {
        let mut graph = PlannerGraph::new();
        let parent = graph
            .add_source("issue", model_with_rows(1000.0))
            .unwrap()
            .connect(vec![], None, true, None, None);

        // Give the child a vastly cheaper model than the parent so the
        // flipped orientation (child drives the scan) wins on cost.
        graph.add_source("comment", model_with_rows(1.0)).unwrap();
        let child = graph
            .get_source("comment")
            .unwrap()
            .connect(vec![], None, false, None, None);

        let join_rc = Rc::new(RefCell::new(crate::planner_node::PlannerJoinNode::new(
            Some(parent.clone()),
            Some(child.clone()),
            true,
            JoinType::Semi,
            vec!["userId".to_string()],
            vec!["id".to_string()],
        )));
        let join = PlannerNode::Join(join_rc.clone());
        graph.joins.push(join.clone());
        graph.connections.push(parent.clone());
        graph.connections.push(child.clone());

        let terminus = PlannerNode::Terminus(Rc::new(RefCell::new(
            crate::planner_node::PlannerTerminusNode {
                input: Some(join),
                pinned: true,
            },
        )));
        graph.set_terminus(terminus);

        graph.plan();

        // Whichever orientation `plan()` settled on must be the one that
        // actually minimizes `get_total_cost()` — this pins the search
        // itself (not a specific formula outcome this test hardcodes).
        let chosen_type = join_rc.borrow().join_type();
        let chosen_cost = graph.get_total_cost();

        graph.reset_planning_state();
        if chosen_type == JoinType::Semi {
            join_rc.borrow_mut().flip().unwrap();
        } else {
            join_rc.borrow_mut().reset();
        }
        graph.propagate_constraints();
        let other_cost = graph.get_total_cost();

        assert!(
            chosen_cost <= other_cost,
            "plan() must select the lower-cost orientation: chose {chosen_cost} over {other_cost}"
        );
    }
}
