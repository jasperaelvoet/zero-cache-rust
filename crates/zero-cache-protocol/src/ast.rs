//! Port of the query AST data model from `zero-protocol/src/ast.ts`.
//!
//! This is the abstract syntax tree for ZQL queries, shared by the view-syncer,
//! query-covering, and pipeline layers. This module ports the recursive **type
//! model** and the operator string mappings; `normalizeAST` (structural
//! normalization / canonical ordering) is ported separately on top of these
//! types.
//!
//! `Parameter` (static parameter references) is modeled opaquely for now — its
//! full sub-schema lands with the query-transform layer that consumes it.

use zero_cache_shared::bigint_json::JsonValue;

/// The query system a subquery runs under. Port of `System`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum System {
    Permissions,
    Client,
    Test,
}

/// A simple comparison operator. Port of `SimpleOperator` (the union of
/// equality/order/like/in operators).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimpleOperator {
    Eq,
    Ne,
    Is,
    IsNot,
    Lt,
    Gt,
    Le,
    Ge,
    Like,
    NotLike,
    ILike,
    NotILike,
    In,
    NotIn,
}

impl SimpleOperator {
    /// The SQL/wire string for this operator.
    pub fn as_str(self) -> &'static str {
        use SimpleOperator::*;
        match self {
            Eq => "=",
            Ne => "!=",
            Is => "IS",
            IsNot => "IS NOT",
            Lt => "<",
            Gt => ">",
            Le => "<=",
            Ge => ">=",
            Like => "LIKE",
            NotLike => "NOT LIKE",
            ILike => "ILIKE",
            NotILike => "NOT ILIKE",
            In => "IN",
            NotIn => "NOT IN",
        }
    }

    /// Parses the SQL/wire string.
    pub fn from_str(s: &str) -> Option<SimpleOperator> {
        use SimpleOperator::*;
        Some(match s {
            "=" => Eq,
            "!=" => Ne,
            "IS" => Is,
            "IS NOT" => IsNot,
            "<" => Lt,
            ">" => Gt,
            "<=" => Le,
            ">=" => Ge,
            "LIKE" => Like,
            "NOT LIKE" => NotLike,
            "ILIKE" => ILike,
            "NOT ILIKE" => NotILike,
            "IN" => In,
            "NOT IN" => NotIn,
            _ => return None,
        })
    }
}

/// A scalar or array literal value in a condition. Port of `LiteralValue`.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    /// An array of scalar literals (`string | number | boolean`).
    Array(Vec<LiteralValue>),
}

/// A reference to a column in the current table. Port of `ColumnReference`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnReference {
    pub name: String,
}

/// A static parameter reference (modeled opaquely for now). Port of
/// `Parameter`.
#[derive(Debug, Clone, PartialEq)]
pub struct Parameter {
    /// The raw parameter reference object.
    pub raw: JsonValue,
}

/// One side of a comparison. Port of `ValuePosition`.
#[derive(Debug, Clone, PartialEq)]
pub enum ValuePosition {
    Literal(LiteralValue),
    Column(ColumnReference),
    Parameter(Parameter),
}

/// A `[field, direction]` ordering element. Port of `OrderPart`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Asc => "asc",
            Direction::Desc => "desc",
        }
    }
    pub fn from_str(s: &str) -> Option<Direction> {
        match s {
            "asc" => Some(Direction::Asc),
            "desc" => Some(Direction::Desc),
            _ => None,
        }
    }
}

/// An ordering: a list of `(field, direction)`. Port of `Ordering`.
pub type Ordering = Vec<(String, Direction)>;

/// A cursor bound (`start`). Port of `Bound`. `row` is an opaque JSON object.
#[derive(Debug, Clone, PartialEq)]
pub struct Bound {
    pub row: JsonValue,
    pub exclusive: bool,
}

/// A compound key: a non-empty list of column names. Port of `CompoundKey`.
pub type CompoundKey = Vec<String>;

/// A parent/child field correlation. Port of `Correlation`.
#[derive(Debug, Clone, PartialEq)]
pub struct Correlation {
    pub parent_field: CompoundKey,
    pub child_field: CompoundKey,
}

/// The `EXISTS` / `NOT EXISTS` operator for a correlated-subquery condition.
/// Port of `CorrelatedSubqueryConditionOperator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistsOp {
    Exists,
    NotExists,
}

impl ExistsOp {
    pub fn as_str(self) -> &'static str {
        match self {
            ExistsOp::Exists => "EXISTS",
            ExistsOp::NotExists => "NOT EXISTS",
        }
    }
    pub fn from_str(s: &str) -> Option<ExistsOp> {
        match s {
            "EXISTS" => Some(ExistsOp::Exists),
            "NOT EXISTS" => Some(ExistsOp::NotExists),
            _ => None,
        }
    }
}

/// A correlated subquery (a related query hop). Port of `CorrelatedSubquery`.
#[derive(Debug, Clone, PartialEq)]
pub struct CorrelatedSubquery {
    pub correlation: Correlation,
    pub subquery: Box<Ast>,
    pub system: Option<System>,
    pub hidden: Option<bool>,
}

/// A query condition (a `where` tree). Port of `Condition`.
#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    /// A `simple` comparison: `left op right`.
    Simple {
        op: SimpleOperator,
        left: ValuePosition,
        right: ValuePosition,
    },
    /// A conjunction (`and`).
    And { conditions: Vec<Condition> },
    /// A disjunction (`or`).
    Or { conditions: Vec<Condition> },
    /// A correlated-subquery existence condition.
    CorrelatedSubquery {
        related: CorrelatedSubquery,
        op: ExistsOp,
        flip: Option<bool>,
        scalar: Option<bool>,
        /// Port of `condition[planIdSymbol]`: a plan identifier the query
        /// planner (`zql/src/planner`) stamps onto a correlated-subquery
        /// condition during `applyPlansToAST`/`applyToCondition`, keyed off
        /// a well-known `Symbol` upstream rather than a plain object field
        /// — never part of the wire/JSON AST shape (see `ast_json.rs`,
        /// which never reads or writes this field). `None` until the
        /// planner assigns one; this port has no planner-to-AST wiring
        /// yet (`PlannerGraph::plan()` itself isn't ported), so every
        /// construction site in this codebase sets it to `None`.
        plan_id: Option<i64>,
    },
}

/// A ZQL query abstract syntax tree. Port of `AST`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Ast {
    pub schema: Option<String>,
    pub table: String,
    pub alias: Option<String>,
    pub where_: Option<Condition>,
    pub related: Option<Vec<CorrelatedSubquery>>,
    pub start: Option<Bound>,
    pub limit: Option<f64>,
    pub order_by: Option<Ordering>,
}

impl Ast {
    /// A minimal AST selecting from `table`.
    pub fn table(table: impl Into<String>) -> Self {
        Ast {
            table: table.into(),
            ..Default::default()
        }
    }
}

/// Collects every table a query reads: its own `table` plus, recursively,
/// the tables of all `related` subquery hops AND all correlated-subquery
/// `where`-conditions (`EXISTS`/`NOT EXISTS` and scalar subqueries). Returns
/// a deduplicated, sorted set.
///
/// This is the read-set a change-stream commit is matched against to decide
/// which live queries a transaction invalidates: if a commit touches any
/// table in a query's `referenced_tables`, that query must be re-hydrated.
/// Upstream the pipeline tracks this structurally through the operator graph;
/// this is the equivalent static extraction over the AST (schema-qualified
/// names are keyed by bare table name, matching how the change-log keys
/// changes by `table`).
pub fn referenced_tables(ast: &Ast) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    collect_referenced_tables(ast, &mut out);
    out
}

fn collect_referenced_tables(ast: &Ast, out: &mut std::collections::BTreeSet<String>) {
    out.insert(ast.table.clone());
    if let Some(rels) = &ast.related {
        for r in rels {
            collect_referenced_tables(&r.subquery, out);
        }
    }
    if let Some(cond) = &ast.where_ {
        collect_condition_tables(cond, out);
    }
}

fn collect_condition_tables(cond: &Condition, out: &mut std::collections::BTreeSet<String>) {
    match cond {
        Condition::Simple { .. } => {}
        Condition::And { conditions } | Condition::Or { conditions } => {
            for c in conditions {
                collect_condition_tables(c, out);
            }
        }
        Condition::CorrelatedSubquery { related, .. } => {
            collect_referenced_tables(&related.subquery, out);
        }
    }
}

/// Subquery alias prefix. Port of `SUBQ_PREFIX`.
pub const SUBQ_PREFIX: &str = "zsubq_";

// ---- normalization --------------------------------------------------------

use std::cmp::Ordering as CmpOrdering;

/// Returns a normalized (canonical) copy of `ast`: `where` conditions are
/// flattened and sorted, related subqueries are sorted by alias, and subqueries
/// are normalized recursively. Port of `normalizeAST` (with identity name
/// mapping). Panics if a static parameter reaches condition comparison
/// (parameters must be resolved before normalization), matching the TS source.
pub fn normalize_ast(ast: &Ast) -> Ast {
    Ast {
        schema: ast.schema.clone(),
        table: ast.table.clone(),
        alias: ast.alias.clone(),
        where_: ast.where_.as_ref().and_then(flatten).map(normalize_where),
        related: ast.related.as_ref().map(|rels| {
            let mut mapped: Vec<CorrelatedSubquery> = rels
                .iter()
                .map(|r| CorrelatedSubquery {
                    correlation: r.correlation.clone(),
                    subquery: Box::new(normalize_ast(&r.subquery)),
                    system: r.system,
                    hidden: r.hidden,
                })
                .collect();
            mapped.sort_by(cmp_related);
            mapped
        }),
        start: ast.start.clone(),
        limit: ast.limit,
        order_by: ast.order_by.clone(),
    }
}

/// Recursively normalizes a condition: sorts and/or children and normalizes
/// subqueries. Names are identity-mapped. Port of `transformWhere` under
/// `NORMALIZE_TRANSFORM`.
fn normalize_where(cond: Condition) -> Condition {
    match cond {
        Condition::Simple { .. } => cond,
        Condition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
            plan_id,
        } => Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: related.correlation.clone(),
                subquery: Box::new(normalize_ast(&related.subquery)),
                system: related.system,
                hidden: related.hidden,
            },
            op,
            flip,
            scalar,
            plan_id,
        },
        Condition::And { conditions } => {
            let mut c: Vec<Condition> = conditions.into_iter().map(normalize_where).collect();
            c.sort_by(cmp_condition);
            Condition::And { conditions: c }
        }
        Condition::Or { conditions } => {
            let mut c: Vec<Condition> = conditions.into_iter().map(normalize_where).collect();
            c.sort_by(cmp_condition);
            Condition::Or { conditions: c }
        }
    }
}

/// Flattens nested same-type conjunctions/disjunctions, drops empties, and
/// collapses singletons. Port of `flattened`.
fn flatten(cond: &Condition) -> Option<Condition> {
    let (is_and, conditions) = match cond {
        Condition::Simple { .. } | Condition::CorrelatedSubquery { .. } => {
            return Some(cond.clone())
        }
        Condition::And { conditions } => (true, conditions),
        Condition::Or { conditions } => (false, conditions),
    };

    let mut flat: Vec<Condition> = Vec::new();
    for c in conditions {
        let same_type = matches!(
            (is_and, c),
            (true, Condition::And { .. }) | (false, Condition::Or { .. })
        );
        if same_type {
            let inner = match c {
                Condition::And { conditions } | Condition::Or { conditions } => conditions,
                _ => unreachable!(),
            };
            for ic in inner {
                if let Some(f) = flatten(ic) {
                    flat.push(f);
                }
            }
        } else if let Some(f) = flatten(c) {
            flat.push(f);
        }
    }

    match flat.len() {
        0 => None,
        1 => Some(flat.pop().unwrap()),
        _ => Some(if is_and {
            Condition::And { conditions: flat }
        } else {
            Condition::Or { conditions: flat }
        }),
    }
}

fn cmp_related(a: &CorrelatedSubquery, b: &CorrelatedSubquery) -> CmpOrdering {
    a.subquery
        .alias
        .as_deref()
        .unwrap_or("")
        .cmp(b.subquery.alias.as_deref().unwrap_or(""))
}

/// Ordering: simple < correlatedSubquery < and/or. Port of `cmpCondition`.
fn cmp_condition(a: &Condition, b: &Condition) -> CmpOrdering {
    if let Condition::Simple {
        op: ao,
        left: al,
        right: ar,
    } = a
    {
        return match b {
            Condition::Simple {
                op: bo,
                left: bl,
                right: br,
            } => cmp_value_position(al, bl)
                .then_with(|| ao.as_str().cmp(bo.as_str()))
                .then_with(|| cmp_value_position(ar, br)),
            _ => CmpOrdering::Less,
        };
    }
    if matches!(b, Condition::Simple { .. }) {
        return CmpOrdering::Greater;
    }
    if let Condition::CorrelatedSubquery {
        related: ar,
        op: ao,
        flip: af,
        scalar: asc,
        ..
    } = a
    {
        return match b {
            Condition::CorrelatedSubquery {
                related: br,
                op: bo,
                flip: bf,
                scalar: bsc,
                ..
            } => cmp_related(ar, br)
                .then_with(|| ao.as_str().cmp(bo.as_str()))
                .then_with(|| cmp_optional_bool(*af, *bf))
                .then_with(|| cmp_optional_bool(*asc, *bsc)),
            _ => CmpOrdering::Less,
        };
    }
    if matches!(b, Condition::CorrelatedSubquery { .. }) {
        // Matches the TS source (`return -1`).
        return CmpOrdering::Less;
    }
    // Both are and/or.
    let (a_type, a_conds) = junction_parts(a);
    let (b_type, b_conds) = junction_parts(b);
    let by_type = a_type.cmp(b_type);
    if by_type != CmpOrdering::Equal {
        return by_type;
    }
    for (l, r) in a_conds.iter().zip(b_conds.iter()) {
        let c = cmp_condition(l, r);
        if c != CmpOrdering::Equal {
            return c;
        }
    }
    a_conds.len().cmp(&b_conds.len())
}

fn junction_parts(c: &Condition) -> (&'static str, &Vec<Condition>) {
    match c {
        Condition::And { conditions } => ("and", conditions),
        Condition::Or { conditions } => ("or", conditions),
        _ => unreachable!("junction_parts on non-junction"),
    }
}

fn cmp_optional_bool(a: Option<bool>, b: Option<bool>) -> CmpOrdering {
    // undefined < false < true
    let to_num = |v: Option<bool>| match v {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    };
    to_num(a).cmp(&to_num(b))
}

fn cmp_value_position(a: &ValuePosition, b: &ValuePosition) -> CmpOrdering {
    let type_str = |v: &ValuePosition| match v {
        ValuePosition::Column(_) => "column",
        ValuePosition::Literal(_) => "literal",
        ValuePosition::Parameter(_) => "static",
    };
    let (at, bt) = (type_str(a), type_str(b));
    if at != bt {
        return at.cmp(bt);
    }
    match (a, b) {
        (ValuePosition::Literal(av), ValuePosition::Literal(bv)) => {
            literal_to_string(av).cmp(&literal_to_string(bv))
        }
        (ValuePosition::Column(ac), ValuePosition::Column(bc)) => ac.name.cmp(&bc.name),
        _ => panic!("Static parameters should be resolved before normalization"),
    }
}

/// Renders a literal the way JavaScript's `String()` does, for comparison.
fn literal_to_string(v: &LiteralValue) -> String {
    match v {
        LiteralValue::String(s) => s.clone(),
        LiteralValue::Bool(b) => b.to_string(),
        LiteralValue::Null => "null".to_string(),
        LiteralValue::Number(n) => {
            if n.fract() == 0.0 && n.is_finite() && n.abs() < 1e21 {
                format!("{}", *n as i128)
            } else {
                format!("{n}")
            }
        }
        // String([a, b]) joins with commas.
        LiteralValue::Array(items) => items
            .iter()
            .map(literal_to_string)
            .collect::<Vec<_>>()
            .join(","),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_operator_round_trip() {
        let all = [
            "=",
            "!=",
            "IS",
            "IS NOT",
            "<",
            ">",
            "<=",
            ">=",
            "LIKE",
            "NOT LIKE",
            "ILIKE",
            "NOT ILIKE",
            "IN",
            "NOT IN",
        ];
        for s in all {
            let op = SimpleOperator::from_str(s).unwrap_or_else(|| panic!("parse {s}"));
            assert_eq!(op.as_str(), s);
        }
        assert_eq!(SimpleOperator::from_str("<>"), None);
    }

    #[test]
    fn exists_and_direction_round_trip() {
        assert_eq!(ExistsOp::from_str("EXISTS"), Some(ExistsOp::Exists));
        assert_eq!(ExistsOp::from_str("NOT EXISTS"), Some(ExistsOp::NotExists));
        assert_eq!(ExistsOp::NotExists.as_str(), "NOT EXISTS");
        assert_eq!(Direction::from_str("asc"), Some(Direction::Asc));
        assert_eq!(Direction::Desc.as_str(), "desc");
        assert_eq!(Direction::from_str("ASC"), None);
    }

    #[test]
    fn builds_a_recursive_ast() {
        // issue WHERE (id = 1) with a related `comments` subquery.
        let ast = Ast {
            table: "issue".into(),
            where_: Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference { name: "id".into() }),
                right: ValuePosition::Literal(LiteralValue::Number(1.0)),
            }),
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast::table("comments")),
                system: Some(System::Client),
                hidden: None,
            }]),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        assert_eq!(ast.table, "issue");
        assert_eq!(ast.related.as_ref().unwrap()[0].subquery.table, "comments");
        assert!(matches!(ast.where_, Some(Condition::Simple { .. })));
    }

    // Helpers for normalization tests.
    fn simple(col: &str, op: SimpleOperator, val: &str) -> Condition {
        Condition::Simple {
            op,
            left: ValuePosition::Column(ColumnReference { name: col.into() }),
            right: ValuePosition::Literal(LiteralValue::String(val.into())),
        }
    }

    fn cols(cond: &Condition) -> Vec<(String, String, String)> {
        // Extracts (leftColumn, op, rightLiteral) for each child of an and/or.
        let conds = match cond {
            Condition::And { conditions } | Condition::Or { conditions } => conditions,
            _ => panic!("not a junction"),
        };
        conds
            .iter()
            .map(|c| match c {
                Condition::Simple { op, left, right } => {
                    let l = match left {
                        ValuePosition::Column(c) => c.name.clone(),
                        _ => String::new(),
                    };
                    let r = match right {
                        ValuePosition::Literal(LiteralValue::String(s)) => s.clone(),
                        _ => String::new(),
                    };
                    (l, op.as_str().to_string(), r)
                }
                _ => panic!("not simple"),
            })
            .collect()
    }

    #[test]
    fn conditions_are_sorted() {
        // by left column name
        let ast = Ast {
            table: "table".into(),
            where_: Some(Condition::And {
                conditions: vec![
                    simple("b", SimpleOperator::Eq, "value"),
                    simple("a", SimpleOperator::Eq, "value"),
                ],
            }),
            ..Default::default()
        };
        assert_eq!(
            cols(normalize_ast(&ast).where_.as_ref().unwrap()),
            vec![
                ("a".into(), "=".into(), "value".into()),
                ("b".into(), "=".into(), "value".into())
            ]
        );

        // by right literal value
        let ast = Ast {
            table: "table".into(),
            where_: Some(Condition::And {
                conditions: vec![
                    simple("a", SimpleOperator::Eq, "y"),
                    simple("a", SimpleOperator::Eq, "x"),
                ],
            }),
            ..Default::default()
        };
        let got = cols(normalize_ast(&ast).where_.as_ref().unwrap());
        assert_eq!(got[0].2, "x");
        assert_eq!(got[1].2, "y");

        // by operator ('<' < '>')
        let ast = Ast {
            table: "table".into(),
            where_: Some(Condition::And {
                conditions: vec![
                    simple("a", SimpleOperator::Lt, "x"),
                    simple("a", SimpleOperator::Gt, "y"),
                ],
            }),
            ..Default::default()
        };
        let got = cols(normalize_ast(&ast).where_.as_ref().unwrap());
        assert_eq!(got[0].1, "<");
        assert_eq!(got[1].1, ">");
    }

    #[test]
    fn related_subqueries_are_sorted() {
        let rel = |alias: &str| CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["a".into()],
                child_field: vec!["a".into()],
            },
            subquery: Box::new(Ast {
                table: "table".into(),
                alias: Some(alias.into()),
                ..Default::default()
            }),
            system: Some(System::Client),
            hidden: None,
        };
        let ast = Ast {
            table: "table".into(),
            related: Some(vec![rel("alias2"), rel("alias1")]),
            ..Default::default()
        };
        let normalized = normalize_ast(&ast);
        let aliases: Vec<String> = normalized
            .related
            .unwrap()
            .iter()
            .map(|r| r.subquery.alias.clone().unwrap())
            .collect();
        assert_eq!(aliases, vec!["alias1", "alias2"]);
    }

    #[test]
    fn where_is_flattened() {
        // ((a AND b) AND c) -> (a AND b AND c), singletons collapsed.
        let ast = Ast {
            table: "t".into(),
            where_: Some(Condition::And {
                conditions: vec![
                    Condition::And {
                        conditions: vec![
                            simple("a", SimpleOperator::Eq, "1"),
                            simple("b", SimpleOperator::Eq, "2"),
                        ],
                    },
                    simple("c", SimpleOperator::Eq, "3"),
                ],
            }),
            ..Default::default()
        };
        let w = normalize_ast(&ast).where_.unwrap();
        assert_eq!(
            cols(&w),
            vec![
                ("a".into(), "=".into(), "1".into()),
                ("b".into(), "=".into(), "2".into()),
                ("c".into(), "=".into(), "3".into())
            ]
        );

        // A singleton AND collapses to its child.
        let ast = Ast {
            table: "t".into(),
            where_: Some(Condition::And {
                conditions: vec![simple("only", SimpleOperator::Eq, "1")],
            }),
            ..Default::default()
        };
        assert!(matches!(
            normalize_ast(&ast).where_,
            Some(Condition::Simple { .. })
        ));
    }

    #[test]
    fn where_flatten_and_sort_compose_on_a_nested_unsorted_tree() {
        // ((b AND a) AND c) must normalize to (a AND b AND c): flattening
        // removes the nesting AND sorting canonicalizes the order — the two
        // must compose, not just flatten a pre-sorted input. This is what makes
        // logically-identical queries hash equal regardless of nesting/order.
        let nested = Ast {
            table: "t".into(),
            where_: Some(Condition::And {
                conditions: vec![
                    Condition::And {
                        conditions: vec![
                            simple("b", SimpleOperator::Eq, "2"),
                            simple("a", SimpleOperator::Eq, "1"),
                        ],
                    },
                    simple("c", SimpleOperator::Eq, "3"),
                ],
            }),
            ..Default::default()
        };
        let flat = Ast {
            table: "t".into(),
            where_: Some(Condition::And {
                conditions: vec![
                    simple("c", SimpleOperator::Eq, "3"),
                    simple("a", SimpleOperator::Eq, "1"),
                    simple("b", SimpleOperator::Eq, "2"),
                ],
            }),
            ..Default::default()
        };
        let expected = vec![
            ("a".to_string(), "=".to_string(), "1".to_string()),
            ("b".to_string(), "=".to_string(), "2".to_string()),
            ("c".to_string(), "=".to_string(), "3".to_string()),
        ];
        assert_eq!(cols(&normalize_ast(&nested).where_.unwrap()), expected);
        // ...and a differently-nested-but-equivalent tree normalizes identically.
        assert_eq!(
            normalize_ast(&nested).where_,
            normalize_ast(&flat).where_,
            "nesting/order differences must normalize to the same canonical where"
        );
    }

    #[test]
    fn referenced_tables_collects_primary_related_and_correlated_where() {
        // issue
        //   related: comments -> related: reactions   (two hops deep)
        //   where: EXISTS(labels) AND (id = 1)
        let deep_related = CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: "comments".into(),
                related: Some(vec![CorrelatedSubquery {
                    correlation: Correlation {
                        parent_field: vec!["id".into()],
                        child_field: vec!["commentID".into()],
                    },
                    subquery: Box::new(Ast::table("reactions")),
                    system: None,
                    hidden: None,
                }]),
                ..Default::default()
            }),
            system: None,
            hidden: None,
        };
        let exists_labels = Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast::table("labels")),
                system: None,
                hidden: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        };
        let ast = Ast {
            table: "issue".into(),
            where_: Some(Condition::And {
                conditions: vec![exists_labels, simple("id", SimpleOperator::Eq, "1")],
            }),
            related: Some(vec![deep_related]),
            ..Default::default()
        };

        let tables = referenced_tables(&ast);
        assert_eq!(
            tables,
            std::collections::BTreeSet::from([
                "issue".to_string(),
                "comments".to_string(),
                "reactions".to_string(),
                "labels".to_string(),
            ]),
            "primary + nested related hops + correlated-subquery where tables"
        );
        // A commit touching any of these invalidates the query; one that does
        // not is irrelevant.
        assert!(tables.contains("reactions"));
        assert!(!tables.contains("users"));
    }
}
