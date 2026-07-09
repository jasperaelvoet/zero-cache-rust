//! Port of `zql/src/planner/planner-node.ts`'s data model: `CostEstimate`
//! (the cost/selectivity/row-count record every planner-graph node's
//! `estimateCost` produces) and `omitFanout`. Second small toehold on the
//! entirely-unstarted `zql/src/planner` subsystem, alongside
//! `planner_constraint.rs` — see that module's doc for the directory-scope
//! scan that found the gap and why the graph itself (the mutually
//! recursive `PlannerJoin`/`PlannerFanIn`/`PlannerFanOut`/
//! `PlannerConnection`/`PlannerTerminus` node types, ~4900 lines) still
//! needs its own architectural decision before real porting can start:
//! this module ports the DATA the graph will eventually produce, not the
//! graph traversal itself, matching the "data model before traversal
//! logic" order this port used for the IVM operator graph too
//! (`ivm::operator`'s `Node`/`Change` types landed before `Filter`/`Join`).
//!
//! `FanoutCostModel` (a `(columns: string[]) => FanoutEst` closure,
//! `planner-connection.ts`) is modeled as a boxed `Fn` — [`FanoutEst`]
//! itself has no logic of its own, just a plain record.

/// Port of `FanoutEst`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FanoutEst {
    pub fanout: f64,
    pub confidence: FanoutConfidence,
}

/// Port of `FanoutEst.confidence`'s `'high' | 'med' | 'none'` union.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutConfidence {
    High,
    Med,
    None,
}

/// Port of `FanoutCostModel`.
pub type FanoutCostModel = std::rc::Rc<dyn Fn(&[String]) -> FanoutEst>;

/// Port of `CostEstimate`. `fanout` is the one field [`omit_fanout`] strips
/// for serialization (a closure isn't serializable/comparable, matching
/// upstream's reason for `omitFanout`'s existence — cost estimates are
/// logged via `PlanDebugger` throughout the graph, and the fanout function
/// itself is meaningless in a log line).
#[derive(Clone)]
pub struct CostEstimate {
    pub startup_cost: f64,
    pub scan_est: f64,
    pub cost: f64,
    pub returned_rows: f64,
    pub selectivity: f64,
    pub limit: Option<f64>,
    pub fanout: FanoutCostModel,
}

/// [`CostEstimate`] minus `fanout`, for logging/serialization. Port of
/// `omitFanout`.
#[derive(Debug, Clone, PartialEq)]
pub struct CostEstimateWithoutFanout {
    pub startup_cost: f64,
    pub scan_est: f64,
    pub cost: f64,
    pub returned_rows: f64,
    pub selectivity: f64,
    pub limit: Option<f64>,
}

/// Port of `omitFanout`.
pub fn omit_fanout(cost: &CostEstimate) -> CostEstimateWithoutFanout {
    CostEstimateWithoutFanout {
        startup_cost: cost.startup_cost,
        scan_est: cost.scan_est,
        cost: cost.cost,
        returned_rows: cost.returned_rows,
        selectivity: cost.selectivity,
        limit: cost.limit,
    }
}

/// Port of `planner-connection.ts`'s `CostModelCost` — the raw
/// row-count/startup-cost/fanout a `ConnectionCostModel` (e.g.
/// `zqlite/sqlite-cost-model.ts`'s `createSQLiteCostModel`) produces for a
/// single connection, before `PlannerConnection.estimateCost` turns it
/// into a full `CostEstimate`.
#[derive(Clone)]
pub struct CostModelCost {
    pub startup_cost: f64,
    pub rows: f64,
    pub fanout: FanoutCostModel,
}

/// Loop information returned by SQLite's `sqlite3_stmt_scanstatus_v2` API.
/// Port of `sqlite-cost-model.ts`'s `ScanstatusLoop`. The extraction of these
/// from a live prepared statement (`getScanstatusLoops`, via `stmt.scanStatus`)
/// is the SQLite-binding-specific half; this is the pure record the cost
/// arithmetic below consumes.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanstatusLoop {
    /// Unique identifier for this loop (`SQLITE_SCANSTAT_SELECTID`).
    pub select_id: i64,
    /// Parent loop id, or 0 for root loops (`SQLITE_SCANSTAT_PARENTID`).
    pub parent_id: i64,
    /// Estimated rows emitted per turn of the parent loop
    /// (`SQLITE_SCANSTAT_EST`).
    pub est: f64,
    /// EXPLAIN text for this loop (`SQLITE_SCANSTAT_EXPLAIN`) — used to tell a
    /// b-tree sort apart from a plain scan.
    pub explain: String,
}

/// Port of `sqlite-cost-model.ts`'s `btreeCost`. B-tree construction is
/// ~O(n·log n); the `/10` reflects that sorting inside SQLite is ~10× faster
/// than pulling rows into the host language and sorting there.
pub fn btree_cost(rows: f64) -> f64 {
    (rows * rows.log2()) / 10.0
}

/// Estimates the cost of a query from its `scanstatus_v2` loops. Port of
/// `sqlite-cost-model.ts`'s `estimateCost`.
///
/// Only top-level ops (`parent_id == 0`) are considered — a ZQL query is
/// single-table when it reaches SQLite (a nested op only appears for
/// `WHERE x IN (:arg)`, negligible for small `:arg`). The FIRST top-level op is
/// the main scan and fixes the output row count; each subsequent top-level op
/// whose `explain` mentions `ORDER BY` adds a [`btree_cost`] sort to the
/// startup cost.
pub fn estimate_cost(scanstats: &[ScanstatusLoop], fanout: FanoutCostModel) -> CostModelCost {
    // Process in execution order (by select_id).
    let mut sorted: Vec<&ScanstatusLoop> = scanstats.iter().collect();
    sorted.sort_by(|a, b| a.select_id.cmp(&b.select_id));

    let top_level: Vec<&ScanstatusLoop> = sorted.into_iter().filter(|s| s.parent_id == 0).collect();

    let mut total_rows = 0.0;
    let mut total_cost = 0.0;
    let mut first_loop = true;
    for op in top_level {
        if first_loop {
            total_rows = op.est;
            first_loop = false;
        } else if op.explain.contains("ORDER BY") {
            total_cost += btree_cost(total_rows);
        }
    }

    CostModelCost {
        rows: total_rows,
        startup_cost: total_cost,
        fanout,
    }
}

/// Removes correlated-subquery conditions from a `where` tree, returning `None`
/// if nothing remains. Port of `sqlite-cost-model.ts`'s
/// `removeCorrelatedSubqueries`: the cost model estimates cost WITHOUT
/// correlated subqueries (they can't ride along in the scanstatus query — this
/// is conservative, the true cost may be higher). An `and`/`or` collapses to
/// its single surviving child, or to `None` if all children were subqueries.
pub fn remove_correlated_subqueries(
    condition: &zero_cache_protocol::ast::Condition,
) -> Option<zero_cache_protocol::ast::Condition> {
    use zero_cache_protocol::ast::Condition;
    match condition {
        Condition::CorrelatedSubquery { .. } => None,
        Condition::Simple { .. } => Some(condition.clone()),
        Condition::And { conditions } => {
            let filtered: Vec<Condition> = conditions
                .iter()
                .filter_map(remove_correlated_subqueries)
                .collect();
            match filtered.len() {
                0 => None,
                1 => Some(filtered.into_iter().next().unwrap()),
                _ => Some(Condition::And {
                    conditions: filtered,
                }),
            }
        }
        Condition::Or { conditions } => {
            let filtered: Vec<Condition> = conditions
                .iter()
                .filter_map(remove_correlated_subqueries)
                .collect();
            match filtered.len() {
                0 => None,
                1 => Some(filtered.into_iter().next().unwrap()),
                _ => Some(Condition::Or {
                    conditions: filtered,
                }),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_cost() -> CostEstimate {
        CostEstimate {
            startup_cost: 1.0,
            scan_est: 2.0,
            cost: 3.0,
            returned_rows: 4.0,
            selectivity: 0.5,
            limit: Some(10.0),
            fanout: std::rc::Rc::new(|_cols| FanoutEst {
                fanout: 1.0,
                confidence: FanoutConfidence::High,
            }),
        }
    }

    #[test]
    fn omit_fanout_drops_the_closure_field_and_keeps_the_rest() {
        let cost = sample_cost();
        let without = omit_fanout(&cost);
        assert_eq!(
            without,
            CostEstimateWithoutFanout {
                startup_cost: 1.0,
                scan_est: 2.0,
                cost: 3.0,
                returned_rows: 4.0,
                selectivity: 0.5,
                limit: Some(10.0)
            }
        );
    }

    #[test]
    fn omit_fanout_preserves_a_none_limit() {
        let mut cost = sample_cost();
        cost.limit = None;
        assert_eq!(omit_fanout(&cost).limit, None);
    }

    #[test]
    fn fanout_cost_model_closure_is_callable() {
        let model: FanoutCostModel = std::rc::Rc::new(|cols: &[String]| FanoutEst {
            fanout: cols.len() as f64,
            confidence: FanoutConfidence::Med,
        });
        let est = model(&["a".to_string(), "b".to_string()]);
        assert_eq!(est.fanout, 2.0);
        assert_eq!(est.confidence, FanoutConfidence::Med);
    }

    fn no_fanout() -> FanoutCostModel {
        std::rc::Rc::new(|_| FanoutEst {
            fanout: 1.0,
            confidence: FanoutConfidence::None,
        })
    }

    fn loop_(select_id: i64, parent_id: i64, est: f64, explain: &str) -> ScanstatusLoop {
        ScanstatusLoop {
            select_id,
            parent_id,
            est,
            explain: explain.into(),
        }
    }

    #[test]
    fn btree_cost_matches_the_n_log2_n_over_10_formula() {
        // 8 * log2(8) / 10 = 8 * 3 / 10 = 2.4
        assert!((btree_cost(8.0) - 2.4).abs() < 1e-9);
        // log2(1) = 0 -> zero sort cost for a single row.
        assert_eq!(btree_cost(1.0), 0.0);
    }

    #[test]
    fn estimate_cost_takes_row_count_from_the_first_top_level_scan() {
        // One top-level scan estimating 100 rows, no sort -> rows=100, cost=0.
        let stats = [loop_(0, 0, 100.0, "SCAN issue")];
        let cost = estimate_cost(&stats, no_fanout());
        assert_eq!(cost.rows, 100.0);
        assert_eq!(cost.startup_cost, 0.0);
    }

    #[test]
    fn estimate_cost_adds_a_btree_sort_for_an_order_by_top_level_op() {
        // A scan (8 rows) plus a top-level ORDER BY op -> startup gains the
        // b-tree sort cost over the scan's row count (8*log2(8)/10 = 2.4).
        let stats = [
            loop_(0, 0, 8.0, "SCAN issue"),
            loop_(1, 0, 8.0, "USE TEMP B-TREE FOR ORDER BY"),
        ];
        let cost = estimate_cost(&stats, no_fanout());
        assert_eq!(cost.rows, 8.0);
        assert!((cost.startup_cost - 2.4).abs() < 1e-9);
    }

    #[test]
    fn estimate_cost_ignores_nested_ops_and_sorts_by_select_id() {
        // A nested op (parent_id != 0) is ignored; a non-ORDER-BY second
        // top-level op adds nothing. Also given out of order to prove sorting.
        let stats = [
            loop_(2, 1, 5.0, "LIST SUBQUERY"),
            loop_(0, 0, 42.0, "SCAN issue"),
            loop_(1, 0, 42.0, "SCAN other"),
        ];
        let cost = estimate_cost(&stats, no_fanout());
        assert_eq!(cost.rows, 42.0, "row count from the first top-level scan");
        assert_eq!(cost.startup_cost, 0.0, "no ORDER BY op -> no sort cost");
    }

    #[test]
    fn remove_correlated_subqueries_strips_the_subquery_and_collapses() {
        use zero_cache_protocol::ast::{
            ColumnReference, Condition, CorrelatedSubquery, Correlation, ExistsOp, LiteralValue,
            SimpleOperator, ValuePosition,
        };
        let simple = || Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference { name: "id".into() }),
            right: ValuePosition::Literal(LiteralValue::Number(1.0)),
        };
        let subq = Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(zero_cache_protocol::ast::Ast::table("comments")),
                system: None,
                hidden: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        };

        // `AND[simple, subquery]` collapses to just `simple`.
        let and = Condition::And {
            conditions: vec![simple(), subq.clone()],
        };
        assert_eq!(remove_correlated_subqueries(&and), Some(simple()));

        // A bare subquery is removed entirely.
        assert_eq!(remove_correlated_subqueries(&subq), None);

        // `AND[subquery, subquery]` -> None (nothing estimable remains).
        let all_subq = Condition::And {
            conditions: vec![subq.clone(), subq],
        };
        assert_eq!(remove_correlated_subqueries(&all_subq), None);
    }
}
