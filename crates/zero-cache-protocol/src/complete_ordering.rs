//! Port of `zql/src/query/complete-ordering.ts` — ensures every query's
//! (and every related subquery's) `ORDER BY` includes the table's full
//! primary key as a tiebreaker, so row ordering is always deterministic
//! even when two rows are equal on the query's own `orderBy` fields. This
//! is what IVM/pagination correctness ultimately depends on: without a
//! total order, `LIMIT`/cursor-based pagination can non-deterministically
//! skip or duplicate rows.
//!
//! `getPrimaryKey`'s `must(...)` (throws if the table has no known primary
//! key) is modeled by taking `get_primary_key: impl Fn(&str) -> Vec<String>`
//! as a plain (non-`Option`) closure — this port's callers always have a
//! primary key available by the time they'd call this, matching how other
//! AST-recursion ports (`normalize_ast`) take their lookups as infallible
//! closures too.

use crate::ast::{Ast, Condition, CorrelatedSubquery, Direction, Ordering};

/// Port of `zql/src/query/escape-like.ts`'s `escapeLike`: backslash-escapes
/// SQL `LIKE` wildcard characters (`%`, `_`) in a string that's meant to be
/// matched literally within a `LIKE` pattern. Lives here (rather than its
/// own one-line file, matching upstream's own layout) since it's the same
/// "small query-construction utility" category as the rest of this module,
/// and this port doesn't split single-line modules out on their own.
pub fn escape_like(val: &str) -> String {
    let mut out = String::with_capacity(val.len());
    for c in val.chars() {
        if c == '%' || c == '_' {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Port of `completeOrdering`: recursively appends any primary-key columns
/// missing from `orderBy` (for this AST and every related subquery, incl.
/// ones nested inside `where`'s correlated-subquery conditions).
pub fn complete_ordering(ast: &Ast, get_primary_key: &impl Fn(&str) -> Vec<String>) -> Ast {
    let primary_key = get_primary_key(&ast.table);
    Ast {
        schema: ast.schema.clone(),
        table: ast.table.clone(),
        alias: ast.alias.clone(),
        where_: ast
            .where_
            .as_ref()
            .map(|w| complete_ordering_in_condition(w, get_primary_key)),
        related: ast.related.as_ref().map(|rels| {
            rels.iter()
                .map(|r| CorrelatedSubquery {
                    correlation: r.correlation.clone(),
                    subquery: Box::new(complete_ordering(&r.subquery, get_primary_key)),
                    system: r.system,
                    hidden: r.hidden,
                })
                .collect()
        }),
        start: ast.start.clone(),
        limit: ast.limit,
        order_by: Some(add_primary_keys(&primary_key, ast.order_by.as_ref())),
    }
}

/// Port of `assertOrderingIncludesPK`. Panics (matching upstream's
/// `assert`) if any primary-key field is missing from `ordering`.
pub fn assert_ordering_includes_pk(ordering: &Ordering, pk: &[String]) {
    let ordering_fields: Vec<&str> = ordering.iter().map(|(field, _)| field.as_str()).collect();
    let missing: Vec<&str> = pk
        .iter()
        .map(String::as_str)
        .filter(|f| !ordering_fields.contains(f))
        .collect();
    assert!(
        missing.is_empty(),
        "Ordering must include all primary key fields. Missing: {}.",
        missing.join(", ")
    );
}

fn complete_ordering_in_condition(
    condition: &Condition,
    get_primary_key: &impl Fn(&str) -> Vec<String>,
) -> Condition {
    match condition {
        Condition::Simple { .. } => condition.clone(),
        Condition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
            plan_id,
        } => Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: related.correlation.clone(),
                subquery: Box::new(complete_ordering(&related.subquery, get_primary_key)),
                system: related.system,
                hidden: related.hidden,
            },
            op: *op,
            flip: *flip,
            scalar: *scalar,
            plan_id: *plan_id,
        },
        Condition::And { conditions } => Condition::And {
            conditions: conditions
                .iter()
                .map(|c| complete_ordering_in_condition(c, get_primary_key))
                .collect(),
        },
        Condition::Or { conditions } => Condition::Or {
            conditions: conditions
                .iter()
                .map(|c| complete_ordering_in_condition(c, get_primary_key))
                .collect(),
        },
    }
}

/// Port of `addPrimaryKeys`: appends (in primary-key order) any PK field
/// not already present in `order_by`, ascending. Leaves `order_by`
/// untouched (same `Vec`, no clone-and-return-original distinction needed
/// in Rust) if every PK field is already covered.
fn add_primary_keys(primary_key: &[String], order_by: Option<&Ordering>) -> Ordering {
    let mut result: Ordering = order_by.cloned().unwrap_or_default();
    let existing: std::collections::HashSet<String> =
        result.iter().map(|(f, _)| f.clone()).collect();
    for key in primary_key {
        if !existing.contains(key) {
            result.push((key.clone(), Direction::Asc));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ColumnReference, SimpleOperator, System, ValuePosition};

    fn pk_lookup(pk: &'static [&'static str]) -> impl Fn(&str) -> Vec<String> {
        move |_table| pk.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn appends_missing_primary_key_columns() {
        let ast = Ast {
            table: "issue".into(),
            order_by: Some(vec![("title".into(), Direction::Desc)]),
            ..Default::default()
        };
        let completed = complete_ordering(&ast, &pk_lookup(&["id"]));
        assert_eq!(
            completed.order_by,
            Some(vec![
                ("title".into(), Direction::Desc),
                ("id".into(), Direction::Asc)
            ])
        );
    }

    #[test]
    fn does_not_duplicate_a_pk_column_already_present() {
        let ast = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Desc)]),
            ..Default::default()
        };
        let completed = complete_ordering(&ast, &pk_lookup(&["id"]));
        assert_eq!(
            completed.order_by,
            Some(vec![("id".into(), Direction::Desc)])
        );
    }

    #[test]
    fn adds_a_full_order_by_when_absent() {
        let ast = Ast::table("issue");
        let completed = complete_ordering(&ast, &pk_lookup(&["id", "orgID"]));
        assert_eq!(
            completed.order_by,
            Some(vec![
                ("id".into(), Direction::Asc),
                ("orgID".into(), Direction::Asc)
            ])
        );
    }

    #[test]
    fn recurses_into_related_subqueries() {
        let ast = Ast {
            table: "issue".into(),
            related: Some(vec![CorrelatedSubquery {
                correlation: crate::ast::Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast::table("comment")),
                system: Some(System::Client),
                hidden: None,
            }]),
            ..Default::default()
        };
        let completed = complete_ordering(&ast, &pk_lookup(&["id"]));
        let related_order_by = &completed.related.unwrap()[0].subquery.order_by;
        assert_eq!(*related_order_by, Some(vec![("id".into(), Direction::Asc)]));
    }

    #[test]
    fn recurses_into_correlated_subquery_conditions_inside_where() {
        let ast = Ast {
            table: "issue".into(),
            where_: Some(Condition::CorrelatedSubquery {
                related: CorrelatedSubquery {
                    correlation: crate::ast::Correlation {
                        parent_field: vec!["id".into()],
                        child_field: vec!["issueID".into()],
                    },
                    subquery: Box::new(Ast::table("comment")),
                    system: Some(System::Client),
                    hidden: None,
                },
                op: crate::ast::ExistsOp::Exists,
                flip: None,
                scalar: None,
                plan_id: None,
            }),
            ..Default::default()
        };
        let completed = complete_ordering(&ast, &pk_lookup(&["id"]));
        let Condition::CorrelatedSubquery { related, .. } = completed.where_.unwrap() else {
            panic!("expected CorrelatedSubquery")
        };
        assert_eq!(
            related.subquery.order_by,
            Some(vec![("id".into(), Direction::Asc)])
        );
    }

    #[test]
    fn simple_conditions_in_where_are_left_untouched() {
        let ast = Ast {
            table: "issue".into(),
            where_: Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference { name: "id".into() }),
                right: ValuePosition::Literal(crate::ast::LiteralValue::Number(1.0)),
            }),
            ..Default::default()
        };
        let completed = complete_ordering(&ast, &pk_lookup(&["id"]));
        assert!(matches!(completed.where_, Some(Condition::Simple { .. })));
    }

    #[test]
    fn assert_ordering_includes_pk_passes_when_all_fields_present() {
        let ordering = vec![
            ("a".to_string(), Direction::Asc),
            ("id".to_string(), Direction::Asc),
        ];
        assert_ordering_includes_pk(&ordering, &["id".to_string()]);
    }

    #[test]
    #[should_panic(expected = "Ordering must include all primary key fields. Missing: id.")]
    fn assert_ordering_includes_pk_panics_when_a_field_is_missing() {
        let ordering = vec![("a".to_string(), Direction::Asc)];
        assert_ordering_includes_pk(&ordering, &["id".to_string()]);
    }

    #[test]
    fn escape_like_escapes_percent_and_underscore() {
        assert_eq!(escape_like("50%_off"), r"50\%\_off");
        assert_eq!(escape_like("plain"), "plain");
        assert_eq!(escape_like(""), "");
    }

    #[test]
    fn escape_like_leaves_backslashes_alone_matching_upstream() {
        // Upstream's regex only targets `%`/`_`, not `\` itself — a
        // pre-existing backslash is left as-is (a real, if odd, upstream
        // detail: the resulting pattern isn't round-trip-safe against a
        // literal backslash, but that's upstream's behavior, not a bug to
        // "fix" here).
        assert_eq!(escape_like(r"back\slash"), r"back\slash");
    }
}
