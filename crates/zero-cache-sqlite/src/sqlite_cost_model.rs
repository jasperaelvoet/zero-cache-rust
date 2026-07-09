//! Port of the pure pieces of `zqlite/src/sqlite-cost-model.ts` — a real
//! prerequisite for the planner graph's still-unported `estimateCost`,
//! found by checking this file before hand-rolling cost formulas from
//! scratch (as planned after `propagateConstraints` landed).
//!
//! Scope: `createSQLiteCostModel`'s outer shell (real SQLite
//! `scanStatus`/`SQLITE_SCANSTAT_*` FFI via `@rocicorp/zero-sqlite3`,
//! `buildSelectQuery`, `compileInline`, `db.prepare`) is NOT ported — it
//! needs live SQLite statement introspection this port's `rusqlite`
//! dependency may or may not expose, and `query-builder.ts`'s SQL
//! generation is itself unported. What IS ported: the two genuinely pure
//! functions once a caller has already extracted `ScanstatusLoop`s from a
//! real prepared statement — `estimate_cost` (aggregates loops into a
//! `CostModelCost`) and `btree_cost` (the sort-cost formula) — plus
//! `remove_correlated_subqueries`, a pure `Condition` transform with no
//! SQLite dependency at all.

use zero_cache_protocol::ast::Condition;
use zero_cache_zql::planner_cost::{CostModelCost, FanoutCostModel};

/// Port of `ScanstatusLoop` — one row of SQLite's `scanStatus` output for
/// a prepared statement, already extracted by the (unported) caller.
#[derive(Debug, Clone, PartialEq)]
pub struct ScanstatusLoop {
    pub select_id: i64,
    pub parent_id: i64,
    pub est: f64,
    pub explain: String,
}

/// Port of `btreeCost`: B-Tree construction is ~O(n log n); divided by 10
/// since SQLite's native sort is ~10x faster than sorting after bringing
/// data into JS (the comment upstream gives for the constant — kept
/// verbatim since this port has no reason to believe the ratio differs).
pub fn btree_cost(rows: f64) -> f64 {
    (rows * rows.log2()) / 10.0
}

/// Port of `estimateCost` (the scanstats-aggregation function, not
/// `PlannerConnection.estimateCost`): sorts loops by `selectId`
/// (execution order), takes the first top-level (`parentId == 0`) loop's
/// `est` as the total row count, and adds [`btree_cost`] for every
/// subsequent top-level loop whose `explain` text mentions `ORDER BY`
/// (SQLite's own EXPLAIN output marking a sort step) — matching upstream's
/// "ZQL queries are single-table when hitting SQLite, so only top-level
/// ops matter" comment.
pub fn estimate_cost(scanstats: &[ScanstatusLoop], fanout: FanoutCostModel) -> CostModelCost {
    let mut sorted: Vec<&ScanstatusLoop> = scanstats.iter().collect();
    sorted.sort_by(|a, b| a.select_id.cmp(&b.select_id));

    let top_level_ops: Vec<&&ScanstatusLoop> = sorted.iter().filter(|s| s.parent_id == 0).collect();

    let mut total_rows = 0.0;
    let mut total_cost = 0.0;
    for (i, op) in top_level_ops.iter().enumerate() {
        if i == 0 {
            total_rows = op.est;
        } else if op.explain.contains("ORDER BY") {
            total_cost += btree_cost(total_rows);
        }
    }

    CostModelCost {
        startup_cost: total_cost,
        rows: total_rows,
        fanout,
    }
}

/// Port of `removeCorrelatedSubqueries`: drops `correlatedSubquery`
/// conditions (the cost model can't estimate their cost via `scanStatus`,
/// so upstream conservatively estimates without them — actual cost may be
/// higher). Flattens/collapses `and`/`or` trees the same way
/// `flatten`ing in `filtered.length === 0 -> None` / `=== 1 -> the one
/// child` does, matching upstream's own collapsing (not this port's
/// `normalize_ast::flatten`, a separate function with a similar shape —
/// this one is scoped to exactly this transform).
pub fn remove_correlated_subqueries(condition: &Condition) -> Option<Condition> {
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
                1 => filtered.into_iter().next(),
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
                1 => filtered.into_iter().next(),
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
    use zero_cache_protocol::ast::{ColumnReference, LiteralValue, SimpleOperator, ValuePosition};
    use zero_cache_zql::planner_cost::{FanoutConfidence, FanoutEst};

    fn loop_(select_id: i64, parent_id: i64, est: f64, explain: &str) -> ScanstatusLoop {
        ScanstatusLoop {
            select_id,
            parent_id,
            est,
            explain: explain.to_string(),
        }
    }

    fn no_fanout() -> FanoutCostModel {
        std::rc::Rc::new(|_| FanoutEst {
            fanout: 1.0,
            confidence: FanoutConfidence::None,
        })
    }

    #[test]
    fn btree_cost_matches_the_upstream_formula() {
        assert_eq!(btree_cost(1024.0), (1024.0 * 1024f64.log2()) / 10.0);
    }

    #[test]
    fn estimate_cost_uses_the_first_top_level_loops_est_as_total_rows() {
        let loops = vec![loop_(1, 0, 500.0, "SCAN t")];
        let cost = estimate_cost(&loops, no_fanout());
        assert_eq!(cost.rows, 500.0);
        assert_eq!(cost.startup_cost, 0.0, "no ORDER BY loop -> no sort cost");
    }

    #[test]
    fn estimate_cost_adds_btree_cost_for_a_subsequent_order_by_loop() {
        let loops = vec![
            loop_(1, 0, 500.0, "SCAN t"),
            loop_(2, 0, 500.0, "USE TEMP B-TREE FOR ORDER BY"),
        ];
        let cost = estimate_cost(&loops, no_fanout());
        assert_eq!(cost.rows, 500.0);
        assert_eq!(cost.startup_cost, btree_cost(500.0));
    }

    #[test]
    fn estimate_cost_ignores_non_top_level_loops() {
        let loops = vec![
            loop_(1, 0, 500.0, "SCAN t"),
            loop_(2, 1, 9999.0, "SCAN nested"),
        ];
        let cost = estimate_cost(&loops, no_fanout());
        assert_eq!(
            cost.rows, 500.0,
            "a nested (non-top-level) loop must not override the top-level row count"
        );
    }

    #[test]
    fn estimate_cost_sorts_loops_by_select_id_before_processing() {
        // Passed out of order; select_id=1 (the real scan) must still be
        // treated as "first" even though it's not first in the input slice.
        let loops = vec![
            loop_(2, 0, 9999.0, "USE TEMP B-TREE FOR ORDER BY"),
            loop_(1, 0, 500.0, "SCAN t"),
        ];
        let cost = estimate_cost(&loops, no_fanout());
        assert_eq!(cost.rows, 500.0);
    }

    fn simple(col: &str) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference { name: col.into() }),
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
    fn remove_correlated_subqueries_drops_a_bare_correlated_condition() {
        assert_eq!(remove_correlated_subqueries(&correlated()), None);
    }

    #[test]
    fn remove_correlated_subqueries_leaves_a_simple_condition_untouched() {
        assert_eq!(
            remove_correlated_subqueries(&simple("a")),
            Some(simple("a"))
        );
    }

    #[test]
    fn remove_correlated_subqueries_collapses_an_and_down_to_its_surviving_child() {
        let cond = Condition::And {
            conditions: vec![simple("a"), correlated()],
        };
        assert_eq!(remove_correlated_subqueries(&cond), Some(simple("a")));
    }

    #[test]
    fn remove_correlated_subqueries_drops_an_and_entirely_when_nothing_survives() {
        let cond = Condition::And {
            conditions: vec![correlated(), correlated()],
        };
        assert_eq!(remove_correlated_subqueries(&cond), None);
    }

    #[test]
    fn remove_correlated_subqueries_keeps_a_multi_child_and_as_and() {
        let cond = Condition::And {
            conditions: vec![simple("a"), simple("b"), correlated()],
        };
        assert_eq!(
            remove_correlated_subqueries(&cond),
            Some(Condition::And {
                conditions: vec![simple("a"), simple("b")]
            })
        );
    }
}
