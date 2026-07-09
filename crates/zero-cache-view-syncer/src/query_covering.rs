//! Port of `zero-cache/src/services/view-syncer/query-covering.ts`.
//!
//! Determines whether one query's result set is a subset of another's
//! ("covering"), used for shadow logging / index maintenance. Deliberately
//! conservative: unsupported cases return `false` rather than guessing.

use std::collections::HashMap;

use zero_cache_protocol::ast::{
    normalize_ast, Ast, Condition, CorrelatedSubquery, ExistsOp, LiteralValue, SimpleOperator,
    ValuePosition,
};

/// A currently-running query. Port of `RunningQuery`.
#[derive(Debug, Clone)]
pub struct RunningQuery {
    pub transformed_ast: Ast,
    pub transformation_hash: String,
    pub query_name: Option<String>,
}

/// The result of finding a covering query. Port of `CoveringQuery`.
#[derive(Debug, Clone, PartialEq)]
pub struct CoveringQuery {
    pub query_id: String,
    pub transformation_hash: String,
    pub query_name: Option<String>,
}

/// Returns true when every row producible by `covered` is also produced by
/// `covering`. Port of `isQueryCoveredBy`.
pub fn is_query_covered_by(covered: &Ast, covering: &Ast) -> bool {
    ast_covered_by(&normalize_ast(covered), &normalize_ast(covering))
}

/// Finds a currently-running query that covers `covered_ast`. Port of the free
/// `findCoveringQuery` function.
pub fn find_covering_query(
    covered_query_id: &str,
    covered_ast: &Ast,
    running_queries: &HashMap<String, RunningQuery>,
) -> Option<CoveringQuery> {
    let mut index = QueryCoveringIndex::new();
    for (id, q) in running_queries {
        index.add(id.clone(), q.clone());
    }
    index.find_covering_query(covered_query_id, covered_ast)
}

struct IndexedRunningQuery {
    query: RunningQuery,
    normalized_ast: Ast,
}

/// An index of running queries by "root" (schema/table/alias), for fast
/// covering-query lookups. Port of `QueryCoveringIndex`.
pub struct QueryCoveringIndex {
    by_root: HashMap<String, HashMap<String, IndexedRunningQuery>>,
    query_id_to_root: HashMap<String, String>,
}

impl Default for QueryCoveringIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryCoveringIndex {
    pub fn new() -> Self {
        QueryCoveringIndex {
            by_root: HashMap::new(),
            query_id_to_root: HashMap::new(),
        }
    }

    /// Adds (or replaces) a query. Port of `add`.
    pub fn add(&mut self, query_id: String, query: RunningQuery) {
        self.remove(&query_id);

        let normalized_ast = normalize_ast(&query.transformed_ast);
        let root = root_key(&normalized_ast);
        self.by_root.entry(root.clone()).or_default().insert(
            query_id.clone(),
            IndexedRunningQuery {
                query,
                normalized_ast,
            },
        );
        self.query_id_to_root.insert(query_id, root);
    }

    /// Removes a query. Port of `remove`.
    pub fn remove(&mut self, query_id: &str) {
        let Some(root) = self.query_id_to_root.remove(query_id) else {
            return;
        };
        if let Some(queries) = self.by_root.get_mut(&root) {
            queries.remove(query_id);
            if queries.is_empty() {
                self.by_root.remove(&root);
            }
        }
    }

    /// Finds a covering query for `covered_ast`, excluding `covered_query_id`
    /// itself. Port of `findCoveringQuery`.
    pub fn find_covering_query(
        &self,
        covered_query_id: &str,
        covered_ast: &Ast,
    ) -> Option<CoveringQuery> {
        let normalized_covered = normalize_ast(covered_ast);
        let queries = self.by_root.get(&root_key(&normalized_covered))?;

        for (query_id, indexed) in queries {
            if query_id == covered_query_id {
                continue;
            }
            if ast_covered_by(&normalized_covered, &indexed.normalized_ast) {
                return Some(CoveringQuery {
                    query_id: query_id.clone(),
                    transformation_hash: indexed.query.transformation_hash.clone(),
                    query_name: indexed.query.query_name.clone(),
                });
            }
        }
        None
    }
}

fn root_key(ast: &Ast) -> String {
    format!("{:?}|{:?}|{:?}", ast.schema, ast.table, ast.alias)
}

fn ast_covered_by(covered: &Ast, covering: &Ast) -> bool {
    covered.schema == covering.schema
        && covered.table == covering.table
        && covered.alias == covering.alias
        && condition_implies(covered.where_.as_ref(), covering.where_.as_ref())
        && related_covered_by(covered.related.as_deref(), covering.related.as_deref())
        && bounds_covered_by(covered, covering)
}

fn bounds_covered_by(covered: &Ast, covering: &Ast) -> bool {
    let Some(covering_limit) = covering.limit else {
        return match &covering.start {
            None => true,
            Some(_) => covered.start == covering.start && covered.order_by == covering.order_by,
        };
    };

    match covered.limit {
        None => false,
        Some(covered_limit) if covering_limit < covered_limit => false,
        Some(_) => {
            condition_equivalent(covered.where_.as_ref(), covering.where_.as_ref())
                && covered.start == covering.start
                && covered.order_by == covering.order_by
        }
    }
}

fn related_covered_by(
    covered: Option<&[CorrelatedSubquery]>,
    covering: Option<&[CorrelatedSubquery]>,
) -> bool {
    let covered = match covered {
        None => return true,
        Some(c) if c.is_empty() => return true,
        Some(c) => c,
    };
    let Some(covering) = covering else {
        return false;
    };
    covered.iter().all(|c| {
        covering
            .iter()
            .any(|g| same_related_edge(c, g) && ast_covered_by(&c.subquery, &g.subquery))
    })
}

fn condition_equivalent(a: Option<&Condition>, b: Option<&Condition>) -> bool {
    condition_implies(a, b) && condition_implies(b, a)
}

fn condition_implies(covered: Option<&Condition>, covering: Option<&Condition>) -> bool {
    let Some(covering) = covering else {
        return true;
    };
    let Some(covered) = covered else {
        return false;
    };
    if covered == covering {
        return true;
    }

    if let Condition::Or { conditions } = covered {
        return conditions
            .iter()
            .all(|c| condition_implies(Some(c), Some(covering)));
    }
    if let Condition::Or { conditions } = covering {
        return conditions
            .iter()
            .any(|c| condition_implies(Some(covered), Some(c)));
    }
    if let Condition::And { conditions } = covering {
        return conditions
            .iter()
            .all(|c| condition_implies(Some(covered), Some(c)));
    }
    if let Condition::And { conditions } = covered {
        return conditions
            .iter()
            .any(|c| condition_implies(Some(c), Some(covering)));
    }
    if let (Condition::Simple { .. }, Condition::Simple { .. }) = (covered, covering) {
        return simple_condition_implies(covered, covering);
    }
    if let (Condition::CorrelatedSubquery { .. }, Condition::CorrelatedSubquery { .. }) =
        (covered, covering)
    {
        return correlated_condition_implies(covered, covering);
    }
    false
}

fn correlated_condition_implies(covered: &Condition, covering: &Condition) -> bool {
    let (
        Condition::CorrelatedSubquery {
            related: cr,
            op: co,
            scalar: cs,
            ..
        },
        Condition::CorrelatedSubquery {
            related: gr,
            op: go,
            scalar: gs,
            ..
        },
    ) = (covered, covering)
    else {
        return false;
    };

    if co != go || cs != gs || !same_related_edge(cr, gr) {
        return false;
    }

    if *co == ExistsOp::Exists {
        ast_covered_by(&cr.subquery, &gr.subquery)
    } else {
        ast_covered_by(&gr.subquery, &cr.subquery)
    }
}

fn same_related_edge(a: &CorrelatedSubquery, b: &CorrelatedSubquery) -> bool {
    a.correlation == b.correlation
        && a.hidden == b.hidden
        && a.system == b.system
        && a.subquery.alias == b.subquery.alias
}

fn simple_condition_implies(covered: &Condition, covering: &Condition) -> bool {
    let (
        Condition::Simple {
            op: co,
            left: cl,
            right: cr,
        },
        Condition::Simple {
            op: go,
            left: gl,
            right: gr,
        },
    ) = (covered, covering)
    else {
        return false;
    };
    let (Some((cc, cv)), Some((gc, gv))) = (column_literal(cl, cr), column_literal(gl, gr)) else {
        return false;
    };
    if cc != gc {
        return false;
    }

    if is_equality_op(*co) && is_non_null_scalar(cv) {
        return equality_implies(cv, *go, gv);
    }

    if *co == SimpleOperator::In && *go == SimpleOperator::In {
        if let (LiteralValue::Array(cvs), LiteralValue::Array(gvs)) = (cv, gv) {
            return cvs.iter().all(|v| literal_array_includes(gvs, v));
        }
    }

    if is_numeric_order_op(*co) && is_numeric_order_op(*go) {
        return order_condition_implies(*co, cv, *go, gv);
    }

    false
}

fn equality_implies(
    value: &LiteralValue,
    covering_op: SimpleOperator,
    covering_value: &LiteralValue,
) -> bool {
    use SimpleOperator::*;
    match covering_op {
        Eq | Is => value == covering_value,
        Ne | IsNot => value != covering_value,
        In => {
            matches!(covering_value, LiteralValue::Array(vs) if literal_array_includes(vs, value))
        }
        Lt => num_cmp(value, covering_value, |a, b| a < b),
        Le => num_cmp(value, covering_value, |a, b| a <= b),
        Gt => num_cmp(value, covering_value, |a, b| a > b),
        Ge => num_cmp(value, covering_value, |a, b| a >= b),
        NotIn | Like | NotLike | ILike | NotILike => false,
    }
}

fn num_cmp(a: &LiteralValue, b: &LiteralValue, cmp: impl Fn(f64, f64) -> bool) -> bool {
    match (a, b) {
        (LiteralValue::Number(a), LiteralValue::Number(b)) => cmp(*a, *b),
        _ => false,
    }
}

fn order_condition_implies(
    covered_op: SimpleOperator,
    covered_value: &LiteralValue,
    covering_op: SimpleOperator,
    covering_value: &LiteralValue,
) -> bool {
    let (LiteralValue::Number(cv), LiteralValue::Number(gv)) = (covered_value, covering_value)
    else {
        return false;
    };
    use SimpleOperator::*;
    match covered_op {
        Gt => (covering_op == Gt && cv >= gv) || (covering_op == Ge && cv >= gv),
        Ge => (covering_op == Gt && cv > gv) || (covering_op == Ge && cv >= gv),
        Lt => (covering_op == Lt && cv <= gv) || (covering_op == Le && cv <= gv),
        Le => (covering_op == Lt && cv < gv) || (covering_op == Le && cv <= gv),
        _ => false,
    }
}

fn column_literal<'a>(
    left: &'a ValuePosition,
    right: &'a ValuePosition,
) -> Option<(&'a str, &'a LiteralValue)> {
    match (left, right) {
        (ValuePosition::Column(c), ValuePosition::Literal(v)) => Some((&c.name, v)),
        _ => None,
    }
}

fn is_equality_op(op: SimpleOperator) -> bool {
    matches!(op, SimpleOperator::Eq | SimpleOperator::Is)
}

fn is_numeric_order_op(op: SimpleOperator) -> bool {
    matches!(
        op,
        SimpleOperator::Lt | SimpleOperator::Gt | SimpleOperator::Le | SimpleOperator::Ge
    )
}

fn is_non_null_scalar(v: &LiteralValue) -> bool {
    !matches!(v, LiteralValue::Null | LiteralValue::Array(_))
}

fn literal_array_includes(values: &[LiteralValue], value: &LiteralValue) -> bool {
    values.iter().any(|v| v == value)
}

/// A recorded coverage hit: a hydrated query whose result set turned out to
/// be a subset of another currently-running query's. Port of
/// `QueryCoverageShadowHit`. `_shadow` in the name (matching upstream's
/// naming/comments) refers to this being purely observational logging —
/// coverage isn't actually acted on (e.g. skipping hydration) yet.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryCoverageShadowHit {
    pub covered_query_hash: String,
    pub covered_transformation_hash: String,
    pub covered_query_name: Option<String>,
    pub covering_query_hash: String,
    pub covering_transformation_hash: String,
    pub covering_query_name: Option<String>,
}

/// Port of `ViewSyncerService#findQueryCoverageShadowHit`: looks up
/// whether `ast` (identified by `query_id`/`transformation_hash`/
/// `query_name`) is covered by another currently-indexed running query,
/// and if so, builds the shadow-logging record for it. Pure given an
/// already-built [`QueryCoveringIndex`] — the actual index maintenance
/// (`#syncQueryPipelineSet` adding/removing entries as queries are
/// hydrated/dropped) and the `#logQueryCoverageShadowSummary` LogContext
/// plumbing built on top of this are NOT ported here; both are still part
/// of `ViewSyncerService`'s real remaining stateful-wiring gap.
pub fn find_query_coverage_shadow_hit(
    query_covering_index: &QueryCoveringIndex,
    query_id: &str,
    transformation_hash: &str,
    ast: &Ast,
    query_name: Option<&str>,
) -> Option<QueryCoverageShadowHit> {
    let covering = query_covering_index.find_covering_query(query_id, ast)?;
    Some(QueryCoverageShadowHit {
        covered_query_hash: query_id.to_string(),
        covered_transformation_hash: transformation_hash.to_string(),
        covered_query_name: query_name.map(str::to_string),
        covering_query_hash: covering.query_id,
        covering_transformation_hash: covering.transformation_hash,
        covering_query_name: covering.query_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::{ColumnReference, Correlation, Direction, System};

    fn all_issues() -> Ast {
        Ast {
            table: "issues".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        }
    }

    fn all_comments() -> Ast {
        Ast {
            table: "comments".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        }
    }

    fn where_(condition: Condition) -> Ast {
        Ast {
            where_: Some(condition),
            ..all_issues()
        }
    }

    fn eq_str(column: &str, value: &str) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: column.into(),
            }),
            right: ValuePosition::Literal(LiteralValue::String(value.into())),
        }
    }

    fn gt_num(column: &str, value: f64) -> Condition {
        Condition::Simple {
            op: SimpleOperator::Gt,
            left: ValuePosition::Column(ColumnReference {
                name: column.into(),
            }),
            right: ValuePosition::Literal(LiteralValue::Number(value)),
        }
    }

    fn and_(conditions: Vec<Condition>) -> Condition {
        Condition::And { conditions }
    }
    fn or_(conditions: Vec<Condition>) -> Condition {
        Condition::Or { conditions }
    }

    fn comments_related(subquery: Ast) -> CorrelatedSubquery {
        CorrelatedSubquery {
            system: Some(System::Client),
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                alias: Some("comments".into()),
                ..subquery
            }),
            hidden: None,
        }
    }

    #[test]
    fn same_query_covers_itself() {
        assert!(is_query_covered_by(
            &where_(eq_str("id", "123")),
            &where_(eq_str("id", "123"))
        ));
    }

    #[test]
    fn unfiltered_covers_filtered() {
        assert!(is_query_covered_by(
            &where_(eq_str("id", "123")),
            &all_issues()
        ));
    }

    #[test]
    fn conjunction_covered_by_subset() {
        let covered = where_(and_(vec![
            eq_str("status", "open"),
            eq_str("owner", "alice"),
        ]));
        let covering = where_(eq_str("status", "open"));
        assert!(is_query_covered_by(&covered, &covering));
        assert!(!is_query_covered_by(&covering, &covered));
    }

    #[test]
    fn equality_and_range_implication() {
        let in_cond = Condition::Simple {
            op: SimpleOperator::In,
            left: ValuePosition::Column(ColumnReference { name: "id".into() }),
            right: ValuePosition::Literal(LiteralValue::Array(vec![
                LiteralValue::String("1".into()),
                LiteralValue::String("2".into()),
            ])),
        };
        assert!(is_query_covered_by(
            &where_(eq_str("id", "1")),
            &where_(in_cond)
        ));
        assert!(is_query_covered_by(
            &where_(gt_num("priority", 5.0)),
            &where_(gt_num("priority", 3.0))
        ));
    }

    #[test]
    fn or_coverage_is_conservative() {
        let bug = eq_str("type", "bug");
        let feature = eq_str("type", "feature");
        assert!(is_query_covered_by(
            &where_(bug.clone()),
            &where_(or_(vec![bug.clone(), feature.clone()]))
        ));
        assert!(!is_query_covered_by(
            &where_(or_(vec![bug.clone(), feature])),
            &where_(bug)
        ));
    }

    #[test]
    fn unlimited_covers_limited_and_paged() {
        let covered = Ast {
            limit: Some(10.0),
            start: Some(zero_cache_protocol::ast::Bound {
                row: zero_cache_shared::bigint_json::JsonValue::Object(vec![(
                    "id".into(),
                    zero_cache_shared::bigint_json::JsonValue::String("abc".into()),
                )]),
                exclusive: true,
            }),
            ..where_(eq_str("status", "open"))
        };
        assert!(is_query_covered_by(&covered, &all_issues()));
    }

    #[test]
    fn limited_covering_requires_equivalent_input_and_larger_limit() {
        let covered = Ast {
            limit: Some(10.0),
            ..where_(eq_str("status", "open"))
        };
        let same_input_larger_limit = Ast {
            limit: Some(20.0),
            ..where_(eq_str("status", "open"))
        };
        let broader_input_same_limit = Ast {
            limit: Some(10.0),
            ..all_issues()
        };
        assert!(is_query_covered_by(&covered, &same_input_larger_limit));
        assert!(!is_query_covered_by(&covered, &broader_input_same_limit));
    }

    #[test]
    fn related_query_coverage_is_recursive() {
        let comments_with_text = Ast {
            where_: Some(eq_str("text", "hello")),
            ..all_comments()
        };
        let covered = Ast {
            related: Some(vec![comments_related(comments_with_text)]),
            ..where_(eq_str("status", "open"))
        };
        let covering = Ast {
            related: Some(vec![comments_related(all_comments())]),
            ..all_issues()
        };
        assert!(is_query_covered_by(&covered, &covering));
        assert!(!is_query_covered_by(&covered, &all_issues()));
    }

    #[test]
    fn not_exists_reverses_subquery_implication() {
        let no_comments = where_(Condition::CorrelatedSubquery {
            op: ExistsOp::NotExists,
            related: comments_related(all_comments()),
            flip: None,
            scalar: None,
            plan_id: None,
        });
        let no_hello_comments = where_(Condition::CorrelatedSubquery {
            op: ExistsOp::NotExists,
            related: comments_related(Ast {
                where_: Some(eq_str("text", "hello")),
                ..all_comments()
            }),
            flip: None,
            scalar: None,
            plan_id: None,
        });
        assert!(is_query_covered_by(&no_comments, &no_hello_comments));
        assert!(!is_query_covered_by(&no_hello_comments, &no_comments));
    }

    #[test]
    fn flip_does_not_affect_coverage() {
        let unflipped = where_(Condition::CorrelatedSubquery {
            op: ExistsOp::Exists,
            related: comments_related(all_comments()),
            flip: None,
            scalar: None,
            plan_id: None,
        });
        let flipped = where_(Condition::CorrelatedSubquery {
            op: ExistsOp::Exists,
            related: comments_related(all_comments()),
            flip: Some(true),
            scalar: None,
            plan_id: None,
        });
        assert!(is_query_covered_by(&unflipped, &flipped));
        assert!(is_query_covered_by(&flipped, &unflipped));
    }

    #[test]
    fn find_covering_query_returns_first_active() {
        let mut running = HashMap::new();
        running.insert(
            "query-1".to_string(),
            RunningQuery {
                transformed_ast: all_comments(),
                transformation_hash: "hash-1".into(),
                query_name: None,
            },
        );
        running.insert(
            "query-2".to_string(),
            RunningQuery {
                transformed_ast: all_issues(),
                transformation_hash: "hash-2".into(),
                query_name: Some("allIssues".into()),
            },
        );
        let result = find_covering_query("query-3", &where_(eq_str("id", "123")), &running);
        assert_eq!(
            result,
            Some(CoveringQuery {
                query_id: "query-2".into(),
                transformation_hash: "hash-2".into(),
                query_name: Some("allIssues".into()),
            })
        );
    }

    #[test]
    fn index_only_considers_matching_root() {
        let mut index = QueryCoveringIndex::new();
        index.add(
            "query-1".into(),
            RunningQuery {
                transformed_ast: all_comments(),
                transformation_hash: "hash-1".into(),
                query_name: None,
            },
        );
        assert_eq!(
            index.find_covering_query("query-2", &where_(eq_str("id", "123"))),
            None
        );
    }

    #[test]
    fn index_updates_during_hydration_batch() {
        let mut index = QueryCoveringIndex::new();
        assert_eq!(
            index.find_covering_query("query-2", &where_(eq_str("id", "123"))),
            None
        );

        index.add(
            "query-1".into(),
            RunningQuery {
                transformed_ast: all_issues(),
                transformation_hash: "hash-1".into(),
                query_name: None,
            },
        );
        assert_eq!(
            index.find_covering_query("query-2", &where_(eq_str("id", "123"))),
            Some(CoveringQuery {
                query_id: "query-1".into(),
                transformation_hash: "hash-1".into(),
                query_name: None,
            })
        );
    }

    #[test]
    fn index_replaces_query_when_root_changes() {
        let mut index = QueryCoveringIndex::new();
        index.add(
            "query-1".into(),
            RunningQuery {
                transformed_ast: all_issues(),
                transformation_hash: "issues-hash".into(),
                query_name: None,
            },
        );
        index.add(
            "query-1".into(),
            RunningQuery {
                transformed_ast: all_comments(),
                transformation_hash: "comments-hash".into(),
                query_name: None,
            },
        );
        assert_eq!(
            index.find_covering_query("query-2", &where_(eq_str("id", "123"))),
            None
        );
        assert_eq!(
            index.find_covering_query("query-2", &all_comments()),
            Some(CoveringQuery {
                query_id: "query-1".into(),
                transformation_hash: "comments-hash".into(),
                query_name: None,
            })
        );
    }

    #[test]
    fn shadow_hit_is_none_when_nothing_covers_the_query() {
        let index = QueryCoveringIndex::new();
        assert_eq!(
            find_query_coverage_shadow_hit(&index, "q1", "hash1", &all_issues(), None),
            None
        );
    }

    #[test]
    fn shadow_hit_reports_both_sides_when_a_covering_query_exists() {
        let mut index = QueryCoveringIndex::new();
        index.add(
            "broad".into(),
            RunningQuery {
                transformed_ast: all_issues(),
                transformation_hash: "broad-hash".into(),
                query_name: Some("allIssues".into()),
            },
        );

        let narrow = where_(eq_str("id", "123"));
        let hit = find_query_coverage_shadow_hit(
            &index,
            "narrow",
            "narrow-hash",
            &narrow,
            Some("oneIssue"),
        )
        .unwrap();
        assert_eq!(
            hit,
            QueryCoverageShadowHit {
                covered_query_hash: "narrow".into(),
                covered_transformation_hash: "narrow-hash".into(),
                covered_query_name: Some("oneIssue".into()),
                covering_query_hash: "broad".into(),
                covering_transformation_hash: "broad-hash".into(),
                covering_query_name: Some("allIssues".into()),
            }
        );
    }

    #[test]
    fn shadow_hit_omits_query_names_when_absent() {
        let mut index = QueryCoveringIndex::new();
        index.add(
            "broad".into(),
            RunningQuery {
                transformed_ast: all_issues(),
                transformation_hash: "broad-hash".into(),
                query_name: None,
            },
        );
        let narrow = where_(eq_str("id", "123"));
        let hit =
            find_query_coverage_shadow_hit(&index, "narrow", "narrow-hash", &narrow, None).unwrap();
        assert_eq!(hit.covered_query_name, None);
        assert_eq!(hit.covering_query_name, None);
    }
}
