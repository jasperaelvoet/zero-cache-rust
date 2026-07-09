//! Resolves the `zql/src/planner` graph's architectural decision — the
//! open question named across several prior rounds as the real blocker to
//! any further planner porting beyond the two pure data-model toeholds
//! (`planner_constraint.rs`, `planner_cost.rs`). This module makes the
//! decision and lands the first compiling skeleton of it; it does NOT
//! port the ~4900 lines of actual node behavior (`PlannerJoin`/
//! `PlannerFanIn`/`PlannerFanOut`/`PlannerConnection`/`PlannerTerminus`'s
//! real `estimateCost`/`propagateConstraints` logic) — that remains a
//! real, substantial future increment this module makes tractable to
//! start.
//!
//! **The decision:** upstream's `PlannerNode` is a closed union of five
//! concrete classes (`PlannerJoin | PlannerConnection | PlannerFanOut |
//! PlannerFanIn | PlannerTerminus`), each holding a reference to its
//! `#input` (single-input nodes) or `#inputs` (fan-in) or `#outputs`
//! (fan-out) neighbor(s), with several methods (`convertToUFO`/`reset` on
//! fan-out, `addOutput`) mutating that structure after construction. This
//! is the same "shared mutable graph with cross-references" shape the IVM
//! operator graph faced (`ivm::operator`'s module doc), resolved there as
//! `Rc<dyn Output>` for a genuinely open-ended/extensible trait. The
//! planner graph is different: it's a FIXED, closed set of five node
//! kinds (matching upstream's own union type, not an extensible trait
//! interface), so this port models it as an enum — `PlannerNode` — over
//! `Rc<RefCell<T>>`-wrapped concrete node structs, one variant per kind.
//! `Rc` for shared ownership (a node can be referenced as another node's
//! `#input`/among fan-out's `#outputs` while also being reachable from the
//! graph root), `RefCell` for the in-place mutation upstream's methods
//! perform (`addOutput`, `convertToUFO`/`reset`, `propagateConstraints`'s
//! debug-logging side effects) — same interior-mutability-inside-the-
//! sharing-wrapper pattern `ivm::table_source`/`ivm::filter` already use
//! for their own state, just applied at the enum-variant level here
//! instead of behind a trait object.
//!
//! Each node struct is currently a minimal skeleton (just enough fields to
//! make the graph SHAPE — parent/child wiring — real and testable); the
//! actual cost-estimation/constraint-propagation algorithms inside each
//! (`estimateCost`, `propagateConstraints`, `propagateUnlimitFromFlippedJoin`)
//! are NOT ported here.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use zero_cache_protocol::ast::{Condition, Ordering};

use crate::planner_constraint::{
    merge_constraints, translate_constraints_for_flipped_join, PlannerConstraint,
};
use crate::planner_cost::{CostEstimate, CostModelCost};

/// Port of `NodeType` (`PlannerNode['kind']`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeType {
    Join,
    Connection,
    FanOut,
    FanIn,
    Terminus,
}

/// Port of `JoinOrConnection`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOrConnection {
    Join,
    Connection,
}

/// Port of `PlannerNode` — see the module doc for why this is a closed
/// enum over `Rc<RefCell<T>>` rather than a trait object.
#[derive(Clone)]
pub enum PlannerNode {
    Join(Rc<RefCell<PlannerJoinNode>>),
    Connection(Rc<RefCell<PlannerConnectionNode>>),
    FanOut(Rc<RefCell<PlannerFanOutNode>>),
    FanIn(Rc<RefCell<PlannerFanInNode>>),
    Terminus(Rc<RefCell<PlannerTerminusNode>>),
}

impl PlannerNode {
    /// Port of the `kind` field every concrete node class carries.
    pub fn node_type(&self) -> NodeType {
        match self {
            PlannerNode::Join(_) => NodeType::Join,
            PlannerNode::Connection(_) => NodeType::Connection,
            PlannerNode::FanOut(_) => NodeType::FanOut,
            PlannerNode::FanIn(_) => NodeType::FanIn,
            PlannerNode::Terminus(_) => NodeType::Terminus,
        }
    }

    /// Port of `closestJoinOrSource`, the first real (not just structural)
    /// behavior ported onto the graph skeleton: walks up through
    /// single-input pass-through nodes (`FanOut`/`Terminus`) until it
    /// reaches a `Join`/`FanIn` (both report `'join'`) or a `Connection`
    /// (`'connection'`) — the nearest node that actually produces/joins
    /// rows, skipping the fan-out/terminus plumbing in between. Panics if
    /// a pass-through node has no input wired yet — a well-formedness
    /// invariant of a fully-built graph (matching upstream's `#input`
    /// being non-optional/required-at-construction for every node that
    /// has one).
    pub fn closest_join_or_source(&self) -> JoinOrConnection {
        match self {
            PlannerNode::Join(_) | PlannerNode::FanIn(_) => JoinOrConnection::Join,
            PlannerNode::Connection(_) => JoinOrConnection::Connection,
            PlannerNode::FanOut(n) => n
                .borrow()
                .input
                .as_ref()
                .expect("PlannerFanOutNode.input must be set")
                .closest_join_or_source(),
            PlannerNode::Terminus(n) => n
                .borrow()
                .input
                .as_ref()
                .expect("PlannerTerminusNode.input must be set")
                .closest_join_or_source(),
        }
    }

    /// Port of `propagateUnlimitFromFlippedJoin`: when a parent join is
    /// flipped, propagates the "remove any limit" signal down through the
    /// graph. `Connection` is the one node that does real work
    /// (`unlimit()`, clearing its own `limit`); every other kind is pure
    /// forwarding — `FanOut`/`Terminus`(no-op)/`Join` forward to their
    /// single neighbor, `FanIn` forwards to ALL of its inputs (upstream's
    /// duck-typed `'propagateUnlimitFromFlippedJoin' in input` check has
    /// no work to do in this port: every `PlannerNode` variant has this
    /// method via the enum, so the "does it support this" check upstream
    /// needs is unconditionally true here).
    pub fn propagate_unlimit_from_flipped_join(&self) {
        match self {
            PlannerNode::Join(n) => n
                .borrow()
                .parent
                .as_ref()
                .expect("PlannerJoinNode.parent must be set")
                .propagate_unlimit_from_flipped_join(),
            PlannerNode::Connection(n) => n.borrow_mut().unlimit(),
            PlannerNode::FanOut(n) => n
                .borrow()
                .input
                .as_ref()
                .expect("PlannerFanOutNode.input must be set")
                .propagate_unlimit_from_flipped_join(),
            PlannerNode::FanIn(n) => {
                for input in &n.borrow().inputs {
                    input.propagate_unlimit_from_flipped_join();
                }
            }
            PlannerNode::Terminus(_) => {}
        }
    }

    /// Reference-identity comparison — port of upstream's `===` checks on
    /// `PlannerNode`s (e.g. `flipIfNeeded`'s `input === this.#child`).
    /// JS object identity maps directly to `Rc::ptr_eq` on the shared
    /// `Rc<RefCell<...>>` each variant wraps.
    pub fn ptr_eq(&self, other: &PlannerNode) -> bool {
        match (self, other) {
            (PlannerNode::Join(a), PlannerNode::Join(b)) => Rc::ptr_eq(a, b),
            (PlannerNode::Connection(a), PlannerNode::Connection(b)) => Rc::ptr_eq(a, b),
            (PlannerNode::FanOut(a), PlannerNode::FanOut(b)) => Rc::ptr_eq(a, b),
            (PlannerNode::FanIn(a), PlannerNode::FanIn(b)) => Rc::ptr_eq(a, b),
            (PlannerNode::Terminus(a), PlannerNode::Terminus(b)) => Rc::ptr_eq(a, b),
            _ => false,
        }
    }

    /// Port of the internal 4-arg `propagateConstraints(branchPattern,
    /// constraint, from, planDebugger)` every non-`Terminus` node
    /// implements (`planDebugger`'s logging side effect is out of scope,
    /// matching every other debug-only omission in this port; `from` is
    /// only used for logging upstream, so it's dropped too). `Terminus`
    /// is never a mid-graph input to another node (it's the graph's sole
    /// root/sink — see the struct doc), so it has no entry here; use
    /// [`Self::start_propagate_constraints`] to kick off propagation from
    /// the root instead, matching `PlannerTerminus`'s distinct public
    /// `propagateConstraints(planDebugger?)` API (no branch-
    /// pattern/constraint parameters — it always seeds with `[]`/`None`).
    pub fn propagate_constraints(
        &self,
        branch_pattern: &[i32],
        constraint: Option<&PlannerConstraint>,
    ) {
        match self {
            PlannerNode::Connection(n) => {
                let key = branch_pattern
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(",");
                n.borrow_mut().constraints.insert(key, constraint.cloned());
            }
            PlannerNode::FanOut(n) => {
                n.borrow()
                    .input
                    .as_ref()
                    .expect("PlannerFanOutNode.input must be set")
                    .propagate_constraints(branch_pattern, constraint);
            }
            PlannerNode::FanIn(n) => {
                let fan_in = n.borrow();
                match fan_in.fan_in_type {
                    FanInType::Fi => {
                        // All inputs get the SAME updated pattern (0
                        // prepended) — they can't contribute differing
                        // constraints since they aren't flipped.
                        let mut updated = Vec::with_capacity(branch_pattern.len() + 1);
                        updated.push(0);
                        updated.extend_from_slice(branch_pattern);
                        for input in &fan_in.inputs {
                            input.propagate_constraints(&updated, constraint);
                        }
                    }
                    FanInType::Ufi => {
                        // Each input gets its OWN unique branch-pattern
                        // prefix (its index), since a UFI does a separate
                        // fetch per branch.
                        for (i, input) in fan_in.inputs.iter().enumerate() {
                            let mut updated = Vec::with_capacity(branch_pattern.len() + 1);
                            updated.push(i as i32);
                            updated.extend_from_slice(branch_pattern);
                            input.propagate_constraints(&updated, constraint);
                        }
                    }
                }
            }
            PlannerNode::Join(n) => {
                let (join_type, parent, child, parent_constraint, child_constraint) = {
                    let j = n.borrow();
                    (
                        j.join_type,
                        j.parent.clone(),
                        j.child.clone(),
                        j.parent_constraint.clone(),
                        j.child_constraint.clone(),
                    )
                };
                let child = child.expect("PlannerJoinNode.child must be set");
                let parent = parent.expect("PlannerJoinNode.parent must be set");
                match join_type {
                    JoinType::Semi => {
                        // A semi-join always has constraints for its
                        // child, defined by the parent/child correlation.
                        let child_constraint_set: PlannerConstraint =
                            child_constraint.iter().cloned().collect();
                        child.propagate_constraints(branch_pattern, Some(&child_constraint_set));
                        // And forwards whatever it received to its parent.
                        parent.propagate_constraints(branch_pattern, constraint);
                    }
                    JoinType::Flipped => {
                        // A flipped join translates constraints from
                        // parent-space to child-space (matches
                        // FlippedJoin.fetch()'s runtime key mapping).
                        let translated = translate_constraints_for_flipped_join(
                            constraint,
                            &parent_constraint,
                            &child_constraint,
                        );
                        child.propagate_constraints(branch_pattern, translated.as_ref());
                        // And sends the merge of what it received plus
                        // its own parent-correlation constraint upward.
                        let parent_constraint_set: PlannerConstraint =
                            parent_constraint.iter().cloned().collect();
                        let merged = merge_constraints(constraint, Some(&parent_constraint_set));
                        parent.propagate_constraints(branch_pattern, merged.as_ref());
                    }
                }
            }
            PlannerNode::Terminus(_) => {
                unreachable!("PlannerTerminus is never a mid-graph input to another node")
            }
        }
    }

    /// Port of `PlannerTerminus#propagateConstraints`: the actual public
    /// entry point that kicks off propagation from the graph's root,
    /// always seeding with an empty branch pattern and no constraint.
    /// Panics if called on a non-`Terminus` node — matching upstream,
    /// where this method only exists on `PlannerTerminus`.
    pub fn start_propagate_constraints(&self) {
        let PlannerNode::Terminus(terminus) = self else {
            panic!("start_propagate_constraints called on a non-Terminus node")
        };
        terminus
            .borrow()
            .input
            .as_ref()
            .expect("PlannerTerminusNode.input must be set")
            .propagate_constraints(&[], None);
    }

    /// Port of `PlannerTerminus#estimateCost`: the public cost-estimation
    /// entry point, always seeding with a downstream selectivity of `1`
    /// and an empty branch pattern. Panics if called on a non-`Terminus`
    /// node, matching upstream (only `PlannerTerminus` exposes this).
    pub fn start_estimate_cost(&self) -> CostEstimate {
        let PlannerNode::Terminus(terminus) = self else {
            panic!("start_estimate_cost called on a non-Terminus node")
        };
        terminus
            .borrow()
            .input
            .as_ref()
            .expect("PlannerTerminusNode.input must be set")
            .estimate_cost(1.0, &[])
    }

    /// Port of `estimateCost`, dispatched across every node kind. This is
    /// the recursive counterpart to `PlannerConnectionNode::estimate_cost`
    /// (the leaf/base case every recursion bottoms out at):
    /// `Connection` calls its own memoized method; `FanOut` is pure
    /// delegation to its input; `FanIn` sums (`FI`) or accumulates (`UFI`)
    /// its inputs' costs (see the two branches below for the exact
    /// aggregation upstream uses — max-of-inputs for `FI` since only one
    /// branch actually executes per row, sum-of-inputs for `UFI` since
    /// every branch does); `Join` is the most involved case, factoring in
    /// child fanout/selectivity and (for flipped joins) IN-list chunking
    /// cost — see inline comments for the exact upstream formulas this
    /// mirrors line for line. `Terminus` has no `estimateCost` of its own
    /// upstream either (only `PlannerNode::start_propagate_constraints`
    /// has a terminus-specific public entry point — `estimateCost`'s
    /// entry point isn't ported here since nothing in this port drives
    /// planning end-to-end yet; a caller estimates cost starting from
    /// whatever node is actually relevant).
    pub fn estimate_cost(
        &self,
        downstream_child_selectivity: f64,
        branch_pattern: &[i32],
    ) -> CostEstimate {
        match self {
            PlannerNode::Connection(n) => n.borrow_mut().estimate_cost(downstream_child_selectivity, branch_pattern),
            PlannerNode::FanOut(n) => n.borrow().input.as_ref().expect("PlannerFanOutNode.input must be set").estimate_cost(downstream_child_selectivity, branch_pattern),
            PlannerNode::FanIn(n) => {
                let fan_in = n.borrow();
                let mut total = CostEstimate {
                    returned_rows: 0.0,
                    cost: 0.0,
                    scan_est: 0.0,
                    startup_cost: 0.0,
                    selectivity: 0.0,
                    limit: None,
                    fanout: Rc::new(|_| panic!("Failed to set fanout model")),
                };
                match fan_in.fan_in_type {
                    FanInType::Fi => {
                        // Normal FanIn: only ONE branch actually executes
                        // per row (they're mutually exclusive OR
                        // branches), so cost/rows take the MAX across
                        // inputs, not a sum.
                        let mut updated = Vec::with_capacity(branch_pattern.len() + 1);
                        updated.push(0);
                        updated.extend_from_slice(branch_pattern);
                        let mut no_match_prob = 1.0;
                        for input in &fan_in.inputs {
                            let cost = input.estimate_cost(downstream_child_selectivity, &updated);
                            total.fanout = cost.fanout.clone();
                            total.returned_rows = total.returned_rows.max(cost.returned_rows);
                            total.cost = total.cost.max(cost.cost);
                            total.startup_cost = total.startup_cost.max(cost.startup_cost);
                            total.scan_est = total.scan_est.max(cost.scan_est);
                            // P(A OR B) = 1 - (1-A)(1-B), assuming independence.
                            no_match_prob *= 1.0 - cost.selectivity;
                            assert!(total.limit.is_none() || cost.limit == total.limit, "All FanIn inputs should have the same limit");
                            total.limit = cost.limit;
                        }
                        total.selectivity = 1.0 - no_match_prob;
                    }
                    FanInType::Ufi => {
                        // Union FanIn: every branch DOES execute (a
                        // separate fetch per branch), so cost/rows SUM
                        // across inputs.
                        let mut no_match_prob = 1.0;
                        for (i, input) in fan_in.inputs.iter().enumerate() {
                            let mut updated = Vec::with_capacity(branch_pattern.len() + 1);
                            updated.push(i as i32);
                            updated.extend_from_slice(branch_pattern);
                            let cost = input.estimate_cost(downstream_child_selectivity, &updated);
                            total.fanout = cost.fanout.clone();
                            total.returned_rows += cost.returned_rows;
                            total.cost += cost.cost;
                            total.scan_est += cost.scan_est;
                            total.startup_cost += cost.startup_cost;
                            no_match_prob *= 1.0 - cost.selectivity;
                            assert!(total.limit.is_none() || cost.limit == total.limit, "All FanIn inputs should have the same limit");
                            total.limit = cost.limit;
                        }
                        total.selectivity = 1.0 - no_match_prob;
                    }
                }
                total
            }
            PlannerNode::Join(n) => {
                let (join_type, parent, child, child_constraint) = {
                    let j = n.borrow();
                    (j.join_type, j.parent.clone(), j.child.clone(), j.child_constraint.clone())
                };
                let parent = parent.expect("PlannerJoinNode.parent must be set");
                let child = child.expect("PlannerJoinNode.child must be set");

                // downstreamChildSelectivity accumulates up a PARENT
                // chain, not up child chains (child chains are
                // independent sub-graphs) — pass 1.0 when estimating the
                // child's own cost.
                let child_cost = child.estimate_cost(1.0, branch_pattern);

                let fanout_est = (child_cost.fanout)(&child_constraint);
                // How many child rows match a parent row, on average —
                // e.g. an issue with 10 comments is more likely to be hit
                // than one with 1. Collapses to 0 if the index is all
                // nulls (no parent matches any child).
                let scaled_child_selectivity = 1.0 - (1.0 - child_cost.selectivity).powf(fanout_est.fanout);

                // Selectivity flows UP the graph from child to parent so
                // consecutive ANDed EXISTS checks compound correctly. A
                // flipped join already accounts for child fanout via its
                // own returnedRows, so it passes 1.0 * downstream instead
                // of scaling by child selectivity again.
                let parent_selectivity_arg = if join_type == JoinType::Flipped { downstream_child_selectivity } else { scaled_child_selectivity * downstream_child_selectivity };
                let parent_cost = parent.estimate_cost(parent_selectivity_arg, branch_pattern);

                match join_type {
                    JoinType::Semi => CostEstimate {
                        startup_cost: parent_cost.startup_cost,
                        scan_est: match parent_cost.limit {
                            None => parent_cost.returned_rows,
                            Some(limit) => {
                                if downstream_child_selectivity == 0.0 {
                                    0.0
                                } else {
                                    parent_cost.returned_rows.min(limit / downstream_child_selectivity)
                                }
                            }
                        },
                        cost: parent_cost.cost + parent_cost.scan_est * (child_cost.startup_cost + child_cost.cost + child_cost.scan_est),
                        returned_rows: parent_cost.returned_rows * child_cost.selectivity,
                        selectivity: child_cost.selectivity * parent_cost.selectivity,
                        limit: parent_cost.limit,
                        fanout: parent_cost.fanout,
                    },
                    JoinType::Flipped => CostEstimate {
                        startup_cost: child_cost.startup_cost,
                        scan_est: match parent_cost.limit {
                            None => parent_cost.returned_rows * child_cost.returned_rows,
                            Some(limit) => {
                                if downstream_child_selectivity == 0.0 {
                                    0.0
                                } else {
                                    (parent_cost.returned_rows * child_cost.returned_rows).min(limit / downstream_child_selectivity)
                                }
                            }
                        },
                        // FlippedJoin batches child->parent lookups into
                        // chunks of MULTI_CONSTRAINT_CHUNK_SIZE, issuing
                        // one IN-list query per chunk — parent.startupCost
                        // is paid once per chunk, not once per child row;
                        // the per-seek work still scales with child rows.
                        cost: child_cost.cost + (child_cost.scan_est / MULTI_CONSTRAINT_CHUNK_SIZE).ceil() * parent_cost.startup_cost + child_cost.scan_est * (parent_cost.cost + parent_cost.scan_est),
                        returned_rows: parent_cost.returned_rows * child_cost.returned_rows,
                        selectivity: parent_cost.selectivity * child_cost.selectivity,
                        limit: parent_cost.limit,
                        fanout: parent_cost.fanout,
                    },
                }
            }
            PlannerNode::Terminus(_) => panic!("estimate_cost has no PlannerTerminus-specific entry point in this port; call it on the relevant node directly"),
        }
    }
}

/// Port of `getMultiConstraintChunkSize()`'s default (`MULTI_CONSTRAINT_CHUNK_SIZE`
/// in `zql/src/ivm/flipped-join.ts`) — how many child rows a flipped
/// join's IN-list batches together per query. Upstream allows overriding
/// this for tests via `setMultiConstraintChunkSizeForTest`; not modeled as
/// mutable ambient state here (this port's convention is to take such
/// values as explicit parameters rather than mutable globals) — if a
/// future caller needs to vary it, `estimate_cost`'s flipped-join cost
/// formula is the one place it's used and can take it as a parameter then.
const MULTI_CONSTRAINT_CHUNK_SIZE: f64 = 256.0;

/// Port of `PlannerJoin`'s `'semi' | 'flipped'` type union.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Semi,
    Flipped,
}

/// Port of `UnflippableJoinError` — thrown by `flip()` on a
/// `!flippable` join (e.g. a `NOT EXISTS` correlated subquery, which
/// can't be reordered to scan the child table first).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("Cannot flip a non-flippable join (e.g., NOT EXISTS)")]
pub struct UnflippableJoinError;

/// Minimal skeleton of `PlannerJoin` — fields enough to represent the
/// graph shape (a join has a parent input and a child input) PLUS the
/// real `semi`/`flipped` mutable state machine (`flip`/`flip_if_needed`/
/// `reset`/`is_flippable`/`propagate_unlimit`). `estimateCost`/
/// `propagateConstraints` bodies are NOT ported.
pub struct PlannerJoinNode {
    pub parent: Option<PlannerNode>,
    pub child: Option<PlannerNode>,
    join_type: JoinType,
    initial_type: JoinType,
    flippable: bool,
    /// Port of `#parentConstraint`/`#childConstraint`. Stored as ordered
    /// `Vec<String>` (NOT `PlannerConstraint`/`BTreeSet`) for the same
    /// reason `translate_constraints_for_flipped_join`'s `parent_keys`/
    /// `child_keys` are — the correlation between parent and child fields
    /// is genuinely positional (field N on the parent side correlates
    /// with field N on the child side), which a `BTreeSet` would silently
    /// scramble by alphabetizing.
    parent_constraint: Vec<String>,
    child_constraint: Vec<String>,
    /// Port of `#output`: the node this join feeds INTO — the opposite
    /// direction from `parent`/`child` (this join's two INPUTS). Needed by
    /// `findFIAndJoins`'s fan-out-to-fan-in BFS, which walks forward
    /// through the graph via `output` links, not `parent`/`child`.
    pub output: Option<PlannerNode>,
    /// Port of `PlannerJoin`'s `planId`: the id linking this join to the
    /// `where_` `correlatedSubquery` condition that produced it (assigned by
    /// `buildPlanGraph`'s `getPlanId()` and also stamped onto the condition).
    /// `applyPlansToAST` collects the `planId`s of joins the planner flipped,
    /// and `apply_to_condition` sets each condition's `flip` from that set.
    /// `None` for joins not built from a planned correlated subquery (e.g. the
    /// graph-shape-only joins in existing tests).
    plan_id: Option<i64>,
}

impl PlannerJoinNode {
    pub fn new(
        parent: Option<PlannerNode>,
        child: Option<PlannerNode>,
        flippable: bool,
        initial_type: JoinType,
        parent_constraint: Vec<String>,
        child_constraint: Vec<String>,
    ) -> Self {
        PlannerJoinNode {
            parent,
            child,
            join_type: initial_type,
            initial_type,
            flippable,
            parent_constraint,
            child_constraint,
            output: None,
            plan_id: None,
        }
    }

    /// Like [`Self::new`] but records the `plan_id` linking this join to its
    /// originating `where_` correlated-subquery condition — the constructor
    /// `buildPlanGraph`/`processCorrelatedSubquery` use.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_plan_id(
        parent: Option<PlannerNode>,
        child: Option<PlannerNode>,
        flippable: bool,
        initial_type: JoinType,
        parent_constraint: Vec<String>,
        child_constraint: Vec<String>,
        plan_id: i64,
    ) -> Self {
        let mut node = PlannerJoinNode::new(
            parent,
            child,
            flippable,
            initial_type,
            parent_constraint,
            child_constraint,
        );
        node.plan_id = Some(plan_id);
        node
    }

    /// Port of `PlannerJoin`'s `planId` accessor.
    pub fn plan_id(&self) -> Option<i64> {
        self.plan_id
    }

    /// Port of the `type` getter.
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    /// Port of `setOutput`.
    pub fn set_output(&mut self, node: PlannerNode) {
        self.output = Some(node);
    }

    /// Port of `isFlippable`.
    pub fn is_flippable(&self) -> bool {
        self.flippable
    }

    /// Port of `flip`. Panics if the join isn't currently `semi` (matching
    /// upstream's `assert`, a real invariant violation not a recoverable
    /// error); returns [`UnflippableJoinError`] if the join isn't
    /// flippable (matching upstream's `throw`, a recoverable/expected
    /// outcome the caller decides how to handle).
    pub fn flip(&mut self) -> Result<(), UnflippableJoinError> {
        assert_eq!(self.join_type, JoinType::Semi, "Can only flip a semi-join");
        if !self.flippable {
            return Err(UnflippableJoinError);
        }
        self.join_type = JoinType::Flipped;
        Ok(())
    }

    /// Port of `reset`.
    pub fn reset(&mut self) {
        self.join_type = self.initial_type;
    }

    /// Sets `join_type` directly, bypassing the `semi`-only precondition
    /// `flip()` enforces. Used by `PlannerGraph::restore_planning_snapshot`
    /// to restore an arbitrary captured state (matching upstream, which
    /// restores `PlanState` by writing `#type` directly rather than
    /// replaying `flip()`/`reset()` calls).
    pub fn restore_type(&mut self, join_type: JoinType) {
        self.join_type = join_type;
    }
}

impl PlannerNode {
    /// Port of `flipIfNeeded`: flips the join if `input` is its child
    /// (the join is being reconsidered from the child's side), or asserts
    /// `input` is the parent otherwise (matching upstream's assert — any
    /// other input is a genuine caller bug, not a recoverable case).
    /// Panics if called on a non-`Join` node (upstream's method doesn't
    /// exist on other node types at all — calling it on the wrong variant
    /// is a caller bug this port surfaces immediately rather than
    /// silently no-op-ing).
    pub fn flip_if_needed(&self, input: &PlannerNode) -> Result<(), UnflippableJoinError> {
        let PlannerNode::Join(join) = self else {
            panic!("flip_if_needed called on a non-Join node")
        };
        let is_child = join
            .borrow()
            .child
            .as_ref()
            .is_some_and(|c| c.ptr_eq(input));
        if is_child {
            join.borrow_mut().flip()
        } else {
            let is_parent = join
                .borrow()
                .parent
                .as_ref()
                .is_some_and(|p| p.ptr_eq(input));
            assert!(is_parent, "Can only flip a join from one of its inputs");
            Ok(())
        }
    }

    /// Port of `PlannerJoin::propagateUnlimit`. Panics if called on a
    /// non-`Join` node or a join that isn't currently flipped (both match
    /// upstream's assert/method-doesn't-exist-elsewhere semantics).
    pub fn propagate_unlimit(&self) {
        let PlannerNode::Join(join) = self else {
            panic!("propagate_unlimit called on a non-Join node")
        };
        assert_eq!(
            join.borrow().join_type,
            JoinType::Flipped,
            "Can only unlimit a flipped join"
        );
        join.borrow()
            .child
            .as_ref()
            .expect("PlannerJoinNode.child must be set")
            .propagate_unlimit_from_flipped_join();
    }
}

/// Minimal skeleton of `PlannerConnection` — the leaf query-source node.
pub struct PlannerConnectionNode {
    pub table: String,
    pub name: String,
    sort: Ordering,
    filters: Option<Condition>,
    model: ConnectionCostModel,
    base_constraints: Option<PlannerConstraint>,
    pub limit: Option<f64>,
    /// Port of `#baseLimit`: the original limit from the query structure,
    /// never mutated after construction — what [`Self::reset`] restores
    /// `limit` to.
    base_limit: Option<f64>,
    /// Port of `#constraints`: the per-branch-pattern constraint set this
    /// connection has been told to filter by, keyed by
    /// `branchPattern.join(',')`.
    pub constraints: BTreeMap<String, Option<PlannerConstraint>>,
    is_root: bool,
    /// Port of `#cachedConstraintCosts`.
    cached_constraint_costs: BTreeMap<String, CostEstimate>,
    /// Port of `selectivity` — computed once at construction (see `new`),
    /// never recomputed afterward.
    selectivity: f64,
    /// Port of `#output`: the node this connection feeds INTO. See
    /// `PlannerJoinNode::output`'s doc for why this is a distinct
    /// direction from anything else on this struct.
    pub output: Option<PlannerNode>,
}

/// Port of `ConnectionCostModel` (`(tableName, sort, filters, constraint)
/// => CostModelCost`) — the injected cost source
/// `PlannerConnection.estimateCost` calls. This port has no live
/// implementation of it yet (needs `zqlite/sqlite-cost-model.ts`'s
/// `createSQLiteCostModel`, itself blocked on `rusqlite` not exposing
/// SQLite's `scanStatus` API — confirmed by inspecting the vendored
/// `rusqlite` source directly: it knows the `SQLITE_DBCONFIG_STMT_SCANSTATUS`
/// flag exists but has no wrapper for `sqlite3_stmt_scanstatus_v2` itself).
/// Taking it as an injected closure — this port's established pattern for
/// a not-yet-built live dependency — is what unblocks `estimate_cost`
/// being real/tested today regardless.
pub type ConnectionCostModel =
    Rc<dyn Fn(&str, &Ordering, Option<&Condition>, Option<&PlannerConstraint>) -> CostModelCost>;

impl PlannerConnectionNode {
    /// Port of the constructor, incl. the one-time `selectivity`
    /// computation for EXISTS child connections (`limit` set AND
    /// `filters` present): calls `model` twice (with and without
    /// `filters`) and takes the ratio of rows, defaulting to `1.0` for
    /// root connections or connections without filters — matching
    /// upstream exactly, including calling the model an extra two times
    /// purely to determine this ratio.
    pub fn new(
        table: String,
        model: ConnectionCostModel,
        sort: Ordering,
        filters: Option<Condition>,
        is_root: bool,
        base_constraints: Option<PlannerConstraint>,
        limit: Option<f64>,
        name: Option<String>,
    ) -> Self {
        let name = name.unwrap_or_else(|| table.clone());
        let selectivity = if limit.is_some() && filters.is_some() {
            let with_filters = model(&table, &sort, filters.as_ref(), None);
            let without_filters = model(&table, &sort, None, None);
            if without_filters.rows > 0.0 {
                with_filters.rows / without_filters.rows
            } else {
                1.0
            }
        } else {
            1.0
        };
        PlannerConnectionNode {
            table,
            name,
            sort,
            filters,
            model,
            base_constraints,
            limit,
            base_limit: limit,
            constraints: BTreeMap::new(),
            is_root,
            cached_constraint_costs: BTreeMap::new(),
            selectivity,
            output: None,
        }
    }

    /// Port of `setOutput`.
    pub fn set_output(&mut self, node: PlannerNode) {
        self.output = Some(node);
    }

    /// Port of `unlimit`: clears this connection's limit — UNLESS it's a
    /// root connection, which can never be unlimited (matching upstream's
    /// `if (this.#isRoot) { return; }` guard). The one piece of real
    /// per-node work `propagateUnlimitFromFlippedJoin` performs; every
    /// other node kind is pure forwarding.
    pub fn unlimit(&mut self) {
        if self.is_root {
            return;
        }
        self.limit = None;
    }

    /// Port of `estimateCost`: memoized per branch-pattern. Merges this
    /// connection's `base_constraints` (from parent correlation) with
    /// whatever `propagate_constraints` stored for this branch pattern,
    /// calls the injected cost `model`, and derives `scan_est` — the full
    /// row count if unlimited, else `min(rows, limit /
    /// downstream_child_selectivity)` (accounting for how many parent
    /// rows this connection will actually be asked to serve).
    pub fn estimate_cost(
        &mut self,
        downstream_child_selectivity: f64,
        branch_pattern: &[i32],
    ) -> CostEstimate {
        let key = branch_pattern
            .iter()
            .map(i32::to_string)
            .collect::<Vec<_>>()
            .join(",");
        if let Some(cached) = self.cached_constraint_costs.get(&key) {
            return cached.clone();
        }
        let constraint = self.constraints.get(&key).cloned().flatten();
        let merged = merge_constraints(self.base_constraints.as_ref(), constraint.as_ref());
        let CostModelCost {
            startup_cost,
            fanout,
            rows,
        } = (self.model)(
            &self.table,
            &self.sort,
            self.filters.as_ref(),
            merged.as_ref(),
        );
        let scan_est = match self.limit {
            None => rows,
            Some(limit) => rows.min(limit / downstream_child_selectivity),
        };
        let cost = CostEstimate {
            startup_cost,
            scan_est,
            cost: 0.0,
            returned_rows: rows,
            selectivity: self.selectivity,
            limit: self.limit,
            fanout,
        };
        self.cached_constraint_costs.insert(key, cost.clone());
        cost
    }

    /// Port of `reset`: clears accumulated constraints, restores `limit`
    /// to `base_limit`, and clears the cost cache — used by
    /// `PlannerGraph::reset_planning_state` to replan the same graph with
    /// a different strategy.
    pub fn reset(&mut self) {
        self.constraints.clear();
        self.limit = self.base_limit;
        self.cached_constraint_costs.clear();
    }

    /// Port of `captureConstraints`: a snapshot of the per-branch
    /// constraint map, used by `PlannerGraph`'s plan-search backtracking
    /// to save/restore state between flip-pattern trials.
    pub fn capture_constraints(&self) -> BTreeMap<String, Option<PlannerConstraint>> {
        self.constraints.clone()
    }

    /// Port of `restoreConstraints`: replaces the constraint map wholesale
    /// and invalidates the cost cache (constraints changed).
    pub fn restore_constraints(
        &mut self,
        constraints: BTreeMap<String, Option<PlannerConstraint>>,
    ) {
        self.constraints = constraints;
        self.cached_constraint_costs.clear();
    }
}

/// Minimal skeleton of `PlannerFanOut` — one input, multiple outputs, with
/// upstream's `FO`/`UFO` (unlimited-fan-out) toggle state.
pub struct PlannerFanOutNode {
    pub input: Option<PlannerNode>,
    pub outputs: Vec<PlannerNode>,
    pub is_unlimited: bool,
}

impl PlannerFanOutNode {
    /// Port of `addOutput`.
    pub fn add_output(&mut self, node: PlannerNode) {
        self.outputs.push(node);
    }

    /// Port of `convertToUFO`.
    pub fn convert_to_ufo(&mut self) {
        self.is_unlimited = true;
    }

    /// Port of `reset`.
    pub fn reset(&mut self) {
        self.is_unlimited = false;
    }
}

/// Port of `PlannerFanIn`'s `'FI' | 'UFI'` type union — see that class's
/// doc comment for the FI-vs-UFI cost distinction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanInType {
    Fi,
    Ufi,
}

/// Minimal skeleton of `PlannerFanIn` — multiple inputs, one output.
pub struct PlannerFanInNode {
    pub inputs: Vec<PlannerNode>,
    pub output: Option<PlannerNode>,
    pub fan_in_type: FanInType,
}

impl PlannerFanInNode {
    pub fn new(inputs: Vec<PlannerNode>) -> Self {
        PlannerFanInNode {
            inputs,
            output: None,
            fan_in_type: FanInType::Fi,
        }
    }

    /// Port of `setOutput`.
    pub fn set_output(&mut self, node: PlannerNode) {
        self.output = Some(node);
    }

    /// Port of `convertToUFI`.
    pub fn convert_to_ufi(&mut self) {
        self.fan_in_type = FanInType::Ufi;
    }

    /// Port of `reset`.
    pub fn reset(&mut self) {
        self.fan_in_type = FanInType::Fi;
    }
}

/// Minimal skeleton of `PlannerTerminus` — the graph's single root/sink.
pub struct PlannerTerminusNode {
    pub input: Option<PlannerNode>,
    pub pinned: bool,
}

/// A cached cost estimate slot, matching the pattern every node's
/// `estimateCost` would populate (not wired to any real computation yet —
/// exists to prove `CostEstimate` from `planner_cost.rs` is usable as
/// node-held state, not just a return value).
pub struct CachedCostEstimate {
    pub estimate: Option<CostEstimate>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner_cost::{FanoutConfidence, FanoutEst};

    fn terminus(input: Option<PlannerNode>) -> PlannerNode {
        PlannerNode::Terminus(Rc::new(RefCell::new(PlannerTerminusNode {
            input,
            pinned: true,
        })))
    }

    fn connection(name: &str) -> PlannerNode {
        connection_with_limit(name, None)
    }

    fn stub_cost_model() -> ConnectionCostModel {
        Rc::new(|_table, _sort, _filters, _constraint| CostModelCost {
            startup_cost: 0.0,
            rows: 100.0,
            fanout: Rc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            }),
        })
    }

    fn connection_with_limit(name: &str, limit: Option<f64>) -> PlannerNode {
        connection_full(name, limit, false, stub_cost_model())
    }

    fn connection_full(
        name: &str,
        limit: Option<f64>,
        is_root: bool,
        model: ConnectionCostModel,
    ) -> PlannerNode {
        PlannerNode::Connection(Rc::new(RefCell::new(PlannerConnectionNode::new(
            name.to_string(),
            model,
            vec![],
            None,
            is_root,
            None,
            limit,
            None,
        ))))
    }

    #[test]
    fn node_type_matches_the_variant() {
        assert_eq!(terminus(None).node_type(), NodeType::Terminus);
        assert_eq!(connection("issues").node_type(), NodeType::Connection);
    }

    fn fan_out(input: Option<PlannerNode>) -> PlannerNode {
        PlannerNode::FanOut(Rc::new(RefCell::new(PlannerFanOutNode {
            input,
            outputs: vec![],
            is_unlimited: false,
        })))
    }

    fn join_node() -> PlannerNode {
        PlannerNode::Join(Rc::new(RefCell::new(PlannerJoinNode::new(
            None,
            None,
            true,
            JoinType::Semi,
            vec![],
            vec![],
        ))))
    }

    fn fan_in_node() -> PlannerNode {
        PlannerNode::FanIn(Rc::new(RefCell::new(PlannerFanInNode::new(vec![]))))
    }

    #[test]
    fn closest_join_or_source_is_direct_for_join_fan_in_and_connection() {
        assert_eq!(join_node().closest_join_or_source(), JoinOrConnection::Join);
        assert_eq!(
            fan_in_node().closest_join_or_source(),
            JoinOrConnection::Join
        );
        assert_eq!(
            connection("issues").closest_join_or_source(),
            JoinOrConnection::Connection
        );
    }

    #[test]
    fn closest_join_or_source_skips_through_fan_out_and_terminus() {
        let root = terminus(Some(fan_out(Some(connection("issues")))));
        assert_eq!(root.closest_join_or_source(), JoinOrConnection::Connection);

        let root = terminus(Some(fan_out(Some(join_node()))));
        assert_eq!(root.closest_join_or_source(), JoinOrConnection::Join);
    }

    #[test]
    #[should_panic(expected = "PlannerTerminusNode.input must be set")]
    fn closest_join_or_source_panics_on_a_malformed_graph() {
        terminus(None).closest_join_or_source();
    }

    #[test]
    fn a_terminus_can_reference_a_connection_as_its_input() {
        let conn = connection("issues");
        let root = terminus(Some(conn));
        let PlannerNode::Terminus(t) = &root else {
            panic!("expected Terminus")
        };
        let input = t.borrow().input.clone().unwrap();
        assert_eq!(input.node_type(), NodeType::Connection);
    }

    #[test]
    fn fan_out_add_output_and_ufo_toggle_mutate_in_place() {
        let fan_out = Rc::new(RefCell::new(PlannerFanOutNode {
            input: None,
            outputs: vec![],
            is_unlimited: false,
        }));
        let node = PlannerNode::FanOut(fan_out.clone());
        assert_eq!(node.node_type(), NodeType::FanOut);

        fan_out.borrow_mut().add_output(terminus(None));
        fan_out.borrow_mut().add_output(terminus(None));
        assert_eq!(fan_out.borrow().outputs.len(), 2);

        assert!(!fan_out.borrow().is_unlimited);
        fan_out.borrow_mut().convert_to_ufo();
        assert!(fan_out.borrow().is_unlimited);
        fan_out.borrow_mut().reset();
        assert!(!fan_out.borrow().is_unlimited);
    }

    #[test]
    fn a_shared_rc_node_is_visible_through_every_reference_to_it() {
        // Proves the Rc<RefCell<...>> sharing actually shares: a fan-out
        // referenced from two different "parent" slots sees the same
        // mutation from either.
        let fan_out = Rc::new(RefCell::new(PlannerFanOutNode {
            input: None,
            outputs: vec![],
            is_unlimited: false,
        }));
        let ref_a = PlannerNode::FanOut(fan_out.clone());
        let ref_b = PlannerNode::FanOut(fan_out.clone());

        let PlannerNode::FanOut(fo) = &ref_a else {
            panic!()
        };
        fo.borrow_mut().convert_to_ufo();

        let PlannerNode::FanOut(fo_b) = &ref_b else {
            panic!()
        };
        assert!(
            fo_b.borrow().is_unlimited,
            "mutation through ref_a must be visible through ref_b"
        );
    }

    #[test]
    fn cached_cost_estimate_holds_a_real_cost_estimate() {
        let mut slot = CachedCostEstimate { estimate: None };
        slot.estimate = Some(CostEstimate {
            startup_cost: 1.0,
            scan_est: 2.0,
            cost: 3.0,
            returned_rows: 4.0,
            selectivity: 1.0,
            limit: None,
            fanout: Rc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            }),
        });
        assert!(slot.estimate.is_some());
    }

    #[test]
    fn fan_in_set_output_mutates_in_place() {
        let fan_in = Rc::new(RefCell::new(PlannerFanInNode::new(vec![])));
        assert!(fan_in.borrow().output.is_none());
        fan_in.borrow_mut().set_output(terminus(None));
        assert!(fan_in.borrow().output.is_some());
    }

    #[test]
    fn join_set_output_mutates_in_place() {
        let join = Rc::new(RefCell::new(PlannerJoinNode::new(
            None,
            None,
            true,
            JoinType::Semi,
            vec![],
            vec![],
        )));
        assert!(join.borrow().output.is_none());
        join.borrow_mut().set_output(terminus(None));
        assert!(join.borrow().output.is_some());
    }

    #[test]
    fn connection_set_output_mutates_in_place() {
        let PlannerNode::Connection(conn) = connection("issues") else {
            unreachable!()
        };
        assert!(conn.borrow().output.is_none());
        conn.borrow_mut().set_output(terminus(None));
        assert!(conn.borrow().output.is_some());
    }

    #[test]
    fn propagate_unlimit_clears_a_connections_limit() {
        let conn_rc = match connection_with_limit("issues", Some(10.0)) {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let node = PlannerNode::Connection(conn_rc.clone());
        assert_eq!(conn_rc.borrow().limit, Some(10.0));
        node.propagate_unlimit_from_flipped_join();
        assert_eq!(conn_rc.borrow().limit, None);
    }

    #[test]
    fn unlimit_is_a_no_op_on_a_root_connection() {
        let conn_rc = match connection_full("issues", Some(10.0), true, stub_cost_model()) {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        conn_rc.borrow_mut().unlimit();
        assert_eq!(
            conn_rc.borrow().limit,
            Some(10.0),
            "a root connection's limit must never be cleared"
        );
    }

    #[test]
    fn connection_reset_restores_base_limit_and_clears_constraints_and_cache() {
        let conn_rc = match connection_full("issues", Some(10.0), false, stub_cost_model()) {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        PlannerNode::Connection(conn_rc.clone()).propagate_constraints(&[0], Some(&cset(&["id"])));
        conn_rc.borrow_mut().unlimit();
        conn_rc.borrow_mut().estimate_cost(1.0, &[0]);
        assert!(!conn_rc.borrow().constraints.is_empty());
        assert_eq!(conn_rc.borrow().limit, None);

        conn_rc.borrow_mut().reset();
        assert_eq!(
            conn_rc.borrow().limit,
            Some(10.0),
            "reset must restore base_limit"
        );
        assert!(
            conn_rc.borrow().constraints.is_empty(),
            "reset must clear accumulated constraints"
        );
        assert!(
            conn_rc.borrow().cached_constraint_costs.is_empty(),
            "reset must clear the cost cache"
        );
    }

    #[test]
    fn fan_in_reset_restores_fi_type() {
        let fan_in_rc = Rc::new(RefCell::new(PlannerFanInNode::new(vec![])));
        fan_in_rc.borrow_mut().convert_to_ufi();
        assert_eq!(fan_in_rc.borrow().fan_in_type, FanInType::Ufi);
        fan_in_rc.borrow_mut().reset();
        assert_eq!(fan_in_rc.borrow().fan_in_type, FanInType::Fi);
    }

    #[test]
    fn estimate_cost_calls_the_model_and_derives_scan_est_from_rows_and_limit() {
        let model: ConnectionCostModel = Rc::new(|_t, _s, _f, _c| CostModelCost {
            startup_cost: 5.0,
            rows: 1000.0,
            fanout: Rc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            }),
        });
        let conn_rc = match connection_full("issues", Some(50.0), false, model) {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let cost = conn_rc.borrow_mut().estimate_cost(2.0, &[]);
        assert_eq!(cost.startup_cost, 5.0);
        assert_eq!(cost.returned_rows, 1000.0);
        // scan_est = min(rows, limit / downstream_child_selectivity) = min(1000, 50/2) = 25.
        assert_eq!(cost.scan_est, 25.0);
    }

    #[test]
    fn estimate_cost_uses_the_full_row_count_when_unlimited() {
        let model: ConnectionCostModel = Rc::new(|_t, _s, _f, _c| CostModelCost {
            startup_cost: 0.0,
            rows: 42.0,
            fanout: Rc::new(|_| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::None,
            }),
        });
        let conn_rc = match connection_full("issues", None, false, model) {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let cost = conn_rc.borrow_mut().estimate_cost(1.0, &[]);
        assert_eq!(cost.scan_est, 42.0);
    }

    #[test]
    fn estimate_cost_is_memoized_per_branch_pattern() {
        let call_count = Rc::new(RefCell::new(0));
        let call_count_clone = call_count.clone();
        let model: ConnectionCostModel = Rc::new(move |_t, _s, _f, _c| {
            *call_count_clone.borrow_mut() += 1;
            CostModelCost {
                startup_cost: 0.0,
                rows: 10.0,
                fanout: Rc::new(|_| FanoutEst {
                    fanout: 1.0,
                    confidence: FanoutConfidence::None,
                }),
            }
        });
        let conn_rc = match connection_full("issues", None, false, model) {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        conn_rc.borrow_mut().estimate_cost(1.0, &[0, 1]);
        conn_rc.borrow_mut().estimate_cost(1.0, &[0, 1]);
        assert_eq!(
            *call_count.borrow(),
            1,
            "a second call with the SAME branch pattern must hit the cache, not re-call the model"
        );
        conn_rc.borrow_mut().estimate_cost(1.0, &[0, 2]);
        assert_eq!(
            *call_count.borrow(),
            2,
            "a DIFFERENT branch pattern must not hit the cache"
        );
    }

    #[test]
    fn estimate_cost_merges_base_constraints_with_propagated_ones() {
        let seen_constraint: Rc<RefCell<Option<PlannerConstraint>>> = Rc::new(RefCell::new(None));
        let seen_clone = seen_constraint.clone();
        let model: ConnectionCostModel = Rc::new(move |_t, _s, _f, c| {
            *seen_clone.borrow_mut() = c.cloned();
            CostModelCost {
                startup_cost: 0.0,
                rows: 1.0,
                fanout: Rc::new(|_| FanoutEst {
                    fanout: 1.0,
                    confidence: FanoutConfidence::None,
                }),
            }
        });
        let base: PlannerConstraint = cset(&["id"]);
        let conn_rc = Rc::new(RefCell::new(PlannerConnectionNode::new(
            "issues".to_string(),
            model,
            vec![],
            None,
            false,
            Some(base),
            None,
            None,
        )));
        conn_rc
            .borrow_mut()
            .constraints
            .insert(String::new(), Some(cset(&["status"])));
        conn_rc.borrow_mut().estimate_cost(1.0, &[]);
        assert_eq!(
            seen_constraint.borrow().as_ref(),
            Some(&cset(&["id", "status"]))
        );
    }

    #[test]
    fn propagate_unlimit_forwards_through_fan_out_to_a_connection() {
        let conn_rc = match connection_with_limit("issues", Some(5.0)) {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let root = fan_out(Some(PlannerNode::Connection(conn_rc.clone())));
        root.propagate_unlimit_from_flipped_join();
        assert_eq!(conn_rc.borrow().limit, None);
    }

    #[test]
    fn propagate_unlimit_forwards_to_every_fan_in_input() {
        let a = match connection_with_limit("a", Some(1.0)) {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let b = match connection_with_limit("b", Some(2.0)) {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let fan_in = PlannerNode::FanIn(Rc::new(RefCell::new(PlannerFanInNode::new(vec![
            PlannerNode::Connection(a.clone()),
            PlannerNode::Connection(b.clone()),
        ]))));
        fan_in.propagate_unlimit_from_flipped_join();
        assert_eq!(a.borrow().limit, None);
        assert_eq!(
            b.borrow().limit,
            None,
            "fan-in must propagate to ALL of its inputs, not just the first"
        );
    }

    #[test]
    fn propagate_unlimit_on_terminus_is_a_no_op() {
        // Should simply not panic and not touch anything (no input at all).
        terminus(None).propagate_unlimit_from_flipped_join();
    }

    #[test]
    fn propagate_unlimit_from_a_join_forwards_to_its_parent() {
        let conn_rc = match connection_with_limit("issues", Some(3.0)) {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let join = PlannerNode::Join(Rc::new(RefCell::new(PlannerJoinNode::new(
            Some(PlannerNode::Connection(conn_rc.clone())),
            None,
            true,
            JoinType::Semi,
            vec![],
            vec![],
        ))));
        join.propagate_unlimit_from_flipped_join();
        assert_eq!(conn_rc.borrow().limit, None);
    }

    fn join_with(
        parent: Option<PlannerNode>,
        child: Option<PlannerNode>,
        flippable: bool,
    ) -> Rc<RefCell<PlannerJoinNode>> {
        Rc::new(RefCell::new(PlannerJoinNode::new(
            parent,
            child,
            flippable,
            JoinType::Semi,
            vec![],
            vec![],
        )))
    }

    #[test]
    fn flip_transitions_a_flippable_semi_join_to_flipped() {
        let join = join_with(None, None, true);
        assert_eq!(join.borrow().join_type(), JoinType::Semi);
        join.borrow_mut().flip().unwrap();
        assert_eq!(join.borrow().join_type(), JoinType::Flipped);
    }

    #[test]
    fn flip_errors_on_a_non_flippable_join() {
        let join = join_with(None, None, false);
        let err = join.borrow_mut().flip().unwrap_err();
        assert_eq!(err, UnflippableJoinError);
        assert_eq!(
            join.borrow().join_type(),
            JoinType::Semi,
            "a failed flip must not change the type"
        );
    }

    #[test]
    #[should_panic(expected = "Can only flip a semi-join")]
    fn flip_panics_when_already_flipped() {
        let join = join_with(None, None, true);
        join.borrow_mut().flip().unwrap();
        let _ = join.borrow_mut().flip();
    }

    #[test]
    fn reset_restores_the_initial_type() {
        let join = join_with(None, None, true);
        join.borrow_mut().flip().unwrap();
        assert_eq!(join.borrow().join_type(), JoinType::Flipped);
        join.borrow_mut().reset();
        assert_eq!(join.borrow().join_type(), JoinType::Semi);
    }

    #[test]
    fn is_flippable_reports_the_constructed_flag() {
        assert!(join_with(None, None, true).borrow().is_flippable());
        assert!(!join_with(None, None, false).borrow().is_flippable());
    }

    #[test]
    fn flip_if_needed_flips_when_input_is_the_child_by_identity() {
        let child = terminus(None);
        let join_rc = join_with(None, Some(child.clone()), true);
        let node = PlannerNode::Join(join_rc.clone());
        node.flip_if_needed(&child).unwrap();
        assert_eq!(join_rc.borrow().join_type(), JoinType::Flipped);
    }

    #[test]
    fn flip_if_needed_is_a_no_op_when_input_is_the_parent() {
        let parent = terminus(None);
        let join_rc = join_with(Some(parent.clone()), None, true);
        let node = PlannerNode::Join(join_rc.clone());
        node.flip_if_needed(&parent).unwrap();
        assert_eq!(
            join_rc.borrow().join_type(),
            JoinType::Semi,
            "flipping from the parent side must not flip the join"
        );
    }

    #[test]
    #[should_panic(expected = "Can only flip a join from one of its inputs")]
    fn flip_if_needed_panics_on_an_unrelated_node() {
        let join_rc = join_with(Some(terminus(None)), Some(terminus(None)), true);
        let node = PlannerNode::Join(join_rc);
        let unrelated = terminus(None);
        let _ = node.flip_if_needed(&unrelated);
    }

    #[test]
    fn ptr_eq_distinguishes_distinct_nodes_of_the_same_kind() {
        let a = terminus(None);
        let b = terminus(None);
        let a_again = a.clone();
        assert!(a.ptr_eq(&a_again));
        assert!(!a.ptr_eq(&b));
    }

    fn cset(cols: &[&str]) -> PlannerConstraint {
        cols.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn propagate_constraints_stores_on_a_connection_keyed_by_branch_pattern() {
        let conn_rc = match connection("issues") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let node = PlannerNode::Connection(conn_rc.clone());
        let c = cset(&["id"]);
        node.propagate_constraints(&[0, 1], Some(&c));
        assert_eq!(conn_rc.borrow().constraints.get("0,1"), Some(&Some(c)));
    }

    #[test]
    fn propagate_constraints_forwards_through_fan_out_unchanged() {
        let conn_rc = match connection("issues") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let root = fan_out(Some(PlannerNode::Connection(conn_rc.clone())));
        let c = cset(&["id"]);
        root.propagate_constraints(&[5], Some(&c));
        assert_eq!(conn_rc.borrow().constraints.get("5"), Some(&Some(c)));
    }

    #[test]
    fn propagate_constraints_fi_prepends_zero_for_every_input() {
        let a = match connection("a") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let b = match connection("b") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let fan_in = PlannerNode::FanIn(Rc::new(RefCell::new(PlannerFanInNode::new(vec![
            PlannerNode::Connection(a.clone()),
            PlannerNode::Connection(b.clone()),
        ]))));
        fan_in.propagate_constraints(&[7], None);
        assert!(a.borrow().constraints.contains_key("0,7"));
        assert!(
            b.borrow().constraints.contains_key("0,7"),
            "FI must give every input the SAME updated pattern"
        );
    }

    #[test]
    fn propagate_constraints_ufi_gives_each_input_its_own_index_prefix() {
        let a = match connection("a") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let b = match connection("b") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let fan_in_rc = Rc::new(RefCell::new(PlannerFanInNode::new(vec![
            PlannerNode::Connection(a.clone()),
            PlannerNode::Connection(b.clone()),
        ])));
        fan_in_rc.borrow_mut().convert_to_ufi();
        PlannerNode::FanIn(fan_in_rc).propagate_constraints(&[7], None);
        assert!(a.borrow().constraints.contains_key("0,7"));
        assert!(
            b.borrow().constraints.contains_key("1,7"),
            "UFI must give each input its OWN index prefix"
        );
    }

    #[test]
    fn propagate_constraints_semi_join_sends_child_constraint_down_and_forwards_up() {
        let child_conn = match connection("child") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let parent_conn = match connection("parent") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let join = Rc::new(RefCell::new(PlannerJoinNode::new(
            Some(PlannerNode::Connection(parent_conn.clone())),
            Some(PlannerNode::Connection(child_conn.clone())),
            true,
            JoinType::Semi,
            vec!["issueID".to_string()],
            vec!["id".to_string()],
        )));
        let incoming = cset(&["foo"]);
        PlannerNode::Join(join).propagate_constraints(&[], Some(&incoming));

        // Child always gets the join's OWN childConstraint, not the incoming one.
        assert_eq!(
            child_conn.borrow().constraints.get(""),
            Some(&Some(cset(&["id"])))
        );
        // Parent gets whatever was passed in, forwarded unchanged.
        assert_eq!(
            parent_conn.borrow().constraints.get(""),
            Some(&Some(incoming))
        );
    }

    #[test]
    fn propagate_constraints_flipped_join_translates_down_and_merges_up() {
        let child_conn = match connection("child") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let parent_conn = match connection("parent") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let join_rc = Rc::new(RefCell::new(PlannerJoinNode::new(
            Some(PlannerNode::Connection(parent_conn.clone())),
            Some(PlannerNode::Connection(child_conn.clone())),
            true,
            JoinType::Semi,
            vec!["issueID".to_string(), "projectID".to_string()],
            vec!["id".to_string(), "projectID".to_string()],
        )));
        join_rc.borrow_mut().flip().unwrap();

        let incoming = cset(&["issueID"]);
        PlannerNode::Join(join_rc).propagate_constraints(&[], Some(&incoming));

        // issueID (parent-space) translates to id (child-space) by position.
        assert_eq!(
            child_conn.borrow().constraints.get(""),
            Some(&Some(cset(&["id"])))
        );
        // Parent gets incoming merged with the join's own parentConstraint.
        assert_eq!(
            parent_conn.borrow().constraints.get(""),
            Some(&Some(cset(&["issueID", "projectID"])))
        );
    }

    #[test]
    fn start_propagate_constraints_seeds_from_the_terminus_root() {
        let conn_rc = match connection("issues") {
            PlannerNode::Connection(c) => c,
            _ => unreachable!(),
        };
        let root = terminus(Some(PlannerNode::Connection(conn_rc.clone())));
        root.start_propagate_constraints();
        assert_eq!(conn_rc.borrow().constraints.get(""), Some(&None));
    }

    #[test]
    #[should_panic(expected = "start_propagate_constraints called on a non-Terminus node")]
    fn start_propagate_constraints_panics_on_a_non_terminus_node() {
        connection("issues").start_propagate_constraints();
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

    fn conn_with_rows(name: &str, rows: f64) -> PlannerNode {
        connection_full(name, None, false, model_with_rows(rows))
    }

    #[test]
    fn estimate_cost_fan_out_delegates_unchanged_to_its_input() {
        let root = fan_out(Some(conn_with_rows("issues", 77.0)));
        let cost = root.estimate_cost(1.0, &[]);
        assert_eq!(cost.returned_rows, 77.0);
    }

    #[test]
    fn estimate_cost_fi_fan_in_takes_the_max_across_inputs() {
        let fan_in = PlannerNode::FanIn(Rc::new(RefCell::new(PlannerFanInNode::new(vec![
            conn_with_rows("a", 10.0),
            conn_with_rows("b", 50.0),
        ]))));
        let cost = fan_in.estimate_cost(1.0, &[]);
        assert_eq!(
            cost.returned_rows, 50.0,
            "FI must take the MAX across inputs (mutually exclusive OR branches)"
        );
    }

    #[test]
    fn estimate_cost_ufi_fan_in_sums_across_inputs() {
        let fan_in_rc = Rc::new(RefCell::new(PlannerFanInNode::new(vec![
            conn_with_rows("a", 10.0),
            conn_with_rows("b", 50.0),
        ])));
        fan_in_rc.borrow_mut().convert_to_ufi();
        let cost = PlannerNode::FanIn(fan_in_rc).estimate_cost(1.0, &[]);
        assert_eq!(
            cost.returned_rows, 60.0,
            "UFI must SUM across inputs (every branch actually executes)"
        );
    }

    #[test]
    fn estimate_cost_semi_join_combines_parent_and_child_selectivity() {
        // Both parent/child unfiltered -> selectivity 1.0 each (see
        // PlannerConnectionNode::new: selectivity defaults to 1.0 without
        // a limit+filters combo), so returnedRows = parent.rows * 1.0.
        let parent = conn_with_rows("parent", 100.0);
        let child = conn_with_rows("child", 5.0);
        let join = PlannerNode::Join(Rc::new(RefCell::new(PlannerJoinNode::new(
            Some(parent),
            Some(child),
            true,
            JoinType::Semi,
            vec!["id".to_string()],
            vec!["parentId".to_string()],
        ))));
        let cost = join.estimate_cost(1.0, &[]);
        assert_eq!(
            cost.returned_rows, 100.0,
            "returnedRows = parent.returnedRows * child.selectivity (1.0 here)"
        );
    }

    #[test]
    fn estimate_cost_flipped_join_multiplies_parent_and_child_rows() {
        let parent = conn_with_rows("parent", 100.0);
        let child = conn_with_rows("child", 5.0);
        let join_rc = Rc::new(RefCell::new(PlannerJoinNode::new(
            Some(parent),
            Some(child),
            true,
            JoinType::Semi,
            vec!["id".to_string()],
            vec!["parentId".to_string()],
        )));
        join_rc.borrow_mut().flip().unwrap();
        let cost = PlannerNode::Join(join_rc).estimate_cost(1.0, &[]);
        assert_eq!(
            cost.returned_rows, 500.0,
            "a flipped join's returnedRows = parent.returnedRows * child.returnedRows"
        );
    }

    #[test]
    #[should_panic(expected = "estimate_cost has no PlannerTerminus-specific entry point")]
    fn estimate_cost_panics_on_terminus() {
        terminus(None).estimate_cost(1.0, &[]);
    }
}
