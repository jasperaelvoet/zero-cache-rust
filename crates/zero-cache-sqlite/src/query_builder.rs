//! Port of `zqlite/src/query-builder.ts` (430 lines) — the actual
//! AST-to-parameterized-SQL translation this port's `sqlite_table_source.rs`
//! doesn't do yet (it currently only pushes a bare equality `WHERE` into
//! SQL and does filtering/ordering/start-cursor pagination by reading every
//! row into Rust and processing it there). This module builds the real
//! `WHERE`/`ORDER BY`/cursor-pagination SQL upstream generates; wiring it
//! into `fetch` is a separate follow-up (this module is usable standalone
//! first, verified against a real SQLite connection).
//!
//! Ports upstream's `@databases/sql` tagged-template query builder as a
//! minimal [`SqlFragment`] (SQL text + positional `?` parameter values)
//! with `ident`/`raw`/`param`/`join` helpers — this port's established
//! pattern of replacing a third-party DSL with the minimal Rust equivalent
//! it's actually used for (see `sql_inline.rs` for the sibling
//! `internal/sql.ts` port).
//!
//! `ValuePosition::Parameter`/static parameters surfacing here is a real,
//! recoverable caller error (matching upstream's `throw`, not an `assert`)
//! — represented as [`QueryBuilderError`]. A `Condition::CorrelatedSubquery`
//! (a query's `exists(...)` filter — the server-authoritative authorization
//! pattern real apps rely on) is compiled to a SQL `[NOT] EXISTS (SELECT 1
//! FROM child WHERE <correlation> [AND <subquery where>])` by [`exists_to_sql`],
//! the SQL-pushdown equivalent of upstream's IVM EXISTS join. The correlation's
//! parent field is qualified with the enclosing table (threaded via
//! [`filters_to_sql_with_outer`]) so it escapes the subquery scope.

use std::collections::BTreeMap;

use rusqlite::types::Value;
use zero_cache_protocol::ast::{
    Condition, CorrelatedSubquery, Direction, ExistsOp, LiteralValue, Ordering, SimpleOperator,
    ValuePosition,
};
use zero_cache_zql::ivm::constraint::Constraint;
use zero_cache_zql::ivm::data::Value as IvmValue;
use zero_cache_zql::ivm::operator::{Start, StartBasis};

use zero_cache_protocol::client_schema::ValueType;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueryBuilderError {
    #[error("Static parameters must be replaced before conversion to SQL")]
    UnresolvedParameter,
}

/// A column's declared type, as read by this module. Port of the
/// `SchemaValue` fields `query-builder.ts` actually uses (`type`/`optional`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnType {
    pub value_type: ValueType,
    pub optional: bool,
}

/// A batch of equality constraints sharing the same key shape. Port of
/// `MultiConstraint` (`readonly Constraint[]`).
pub type MultiConstraint = Vec<Constraint>;

/// Port of the `sql` tagged-template's result (`SQLQuery`): SQL text with
/// `?` placeholders, plus the positional parameter values to bind.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlFragment {
    pub text: String,
    pub params: Vec<Value>,
}

impl SqlFragment {
    pub fn raw(text: impl Into<String>) -> Self {
        SqlFragment {
            text: text.into(),
            params: Vec::new(),
        }
    }

    pub fn param(value: Value) -> Self {
        SqlFragment {
            text: "?".to_string(),
            params: vec![value],
        }
    }

    /// Port of `sql.ident`: double-quotes an identifier, doubling any
    /// embedded double quote (matches `escapeSQLiteIdentifier`).
    pub fn ident(name: &str) -> Self {
        SqlFragment::raw(format!("\"{}\"", name.replace('"', "\"\"")))
    }

    /// Port of `sql.join`: concatenates fragments with a literal separator.
    pub fn join(fragments: Vec<SqlFragment>, sep: &str) -> Self {
        let mut text = String::new();
        let mut params = Vec::new();
        for (i, f) in fragments.into_iter().enumerate() {
            if i > 0 {
                text.push_str(sep);
            }
            text.push_str(&f.text);
            params.extend(f.params);
        }
        SqlFragment { text, params }
    }

    /// Concatenates fragments with no separator (used to sandwich raw SQL
    /// keywords/punctuation around a sub-fragment).
    pub fn concat(fragments: Vec<SqlFragment>) -> Self {
        SqlFragment::join(fragments, "")
    }
}

fn wrap(open: &str, inner: SqlFragment, close: &str) -> SqlFragment {
    SqlFragment::concat(vec![SqlFragment::raw(open), inner, SqlFragment::raw(close)])
}

/// Port of `getJsType`: infers a literal's own type when no column type is
/// known (matches JS `typeof`).
fn literal_value_type(v: &LiteralValue) -> ValueType {
    match v {
        LiteralValue::Null => ValueType::Null,
        LiteralValue::String(_) => ValueType::String,
        LiteralValue::Number(_) => ValueType::Number,
        LiteralValue::Bool(_) => ValueType::Boolean,
        LiteralValue::Array(_) => ValueType::Json,
    }
}

fn literal_to_json(v: &LiteralValue) -> zero_cache_shared::bigint_json::JsonValue {
    use zero_cache_shared::bigint_json::JsonValue;
    match v {
        LiteralValue::Null => JsonValue::Null,
        LiteralValue::String(s) => JsonValue::String(s.clone()),
        LiteralValue::Number(n) => JsonValue::Number(*n),
        LiteralValue::Bool(b) => JsonValue::Bool(*b),
        LiteralValue::Array(a) => JsonValue::Array(a.iter().map(literal_to_json).collect()),
    }
}

fn ivm_to_json(v: &IvmValue) -> zero_cache_shared::bigint_json::JsonValue {
    v.clone()
}

fn json_truthy(v: &zero_cache_shared::bigint_json::JsonValue) -> bool {
    use zero_cache_shared::bigint_json::JsonValue;
    match v {
        JsonValue::Null => false,
        JsonValue::Bool(b) => *b,
        JsonValue::Number(n) => *n != 0.0 && !n.is_nan(),
        JsonValue::BigInt(b) => !b.eq(&num_bigint::BigInt::from(0)),
        JsonValue::String(s) => !s.is_empty(),
        JsonValue::Array(_) | JsonValue::Object(_) => true,
    }
}

/// Port of `toSQLiteType`.
fn to_sqlite_value(v: &zero_cache_shared::bigint_json::JsonValue, value_type: ValueType) -> Value {
    use zero_cache_shared::bigint_json::JsonValue;
    match value_type {
        ValueType::Boolean => match v {
            JsonValue::Null => Value::Null,
            other => Value::Integer(if json_truthy(other) { 1 } else { 0 }),
        },
        ValueType::Json => Value::Text(v.stringify()),
        ValueType::Number | ValueType::String | ValueType::Null => match v {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(b) => Value::Integer(if *b { 1 } else { 0 }),
            JsonValue::Number(n) => {
                if n.fract() == 0.0 && n.is_finite() && n.abs() < 9.007_199_254_740_992e15 {
                    Value::Integer(*n as i64)
                } else {
                    Value::Real(*n)
                }
            }
            JsonValue::BigInt(b) => match i64::try_from(b.clone()) {
                Ok(i) => Value::Integer(i),
                Err(_) => Value::Text(b.to_string()),
            },
            JsonValue::String(s) => Value::Text(s.clone()),
            JsonValue::Array(_) | JsonValue::Object(_) => Value::Text(v.stringify()),
        },
    }
}

/// Port of `constraintsToSQL`.
pub fn constraints_to_sql(
    constraint: Option<&Constraint>,
    columns: &BTreeMap<String, ColumnType>,
) -> Vec<SqlFragment> {
    let Some(constraint) = constraint else {
        return Vec::new();
    };
    constraint
        .iter()
        .map(|(key, value)| {
            let value_type = columns[key].value_type;
            SqlFragment::concat(vec![
                SqlFragment::ident(key),
                SqlFragment::raw(" = "),
                SqlFragment::param(to_sqlite_value(&ivm_to_json(value), value_type)),
            ])
        })
        .collect()
}

/// Port of `multiConstraintToSQL`. Panics on shape violations (matching
/// upstream's `assert`s — a caller passing empty/heterogeneous-shaped
/// entries is a real invariant violation, not a recoverable error).
pub fn multi_constraint_to_sql(
    multi_constraint: &MultiConstraint,
    columns: &BTreeMap<String, ColumnType>,
) -> SqlFragment {
    assert!(
        !multi_constraint.is_empty(),
        "multiConstraint must be non-empty"
    );
    let keys: Vec<&String> = multi_constraint[0].iter().map(|(k, _)| k).collect();
    assert!(
        !keys.is_empty(),
        "multiConstraint entries must have at least one key"
    );
    for entry in &multi_constraint[1..] {
        let entry_keys: Vec<&String> = entry.iter().map(|(k, _)| k).collect();
        assert!(
            entry_keys.len() == keys.len() && keys.iter().all(|k| entry_keys.contains(k)),
            "multiConstraint entries must share the same keys"
        );
    }

    if keys.len() == 1 {
        let key = keys[0];
        let value_type = columns[key].value_type;
        let values: Vec<SqlFragment> = multi_constraint
            .iter()
            .map(|entry| {
                let (_, v) = entry.iter().find(|(k, _)| k == key).unwrap();
                SqlFragment::param(to_sqlite_value(&ivm_to_json(v), value_type))
            })
            .collect();
        return SqlFragment::concat(vec![
            SqlFragment::ident(key),
            SqlFragment::raw(" IN ("),
            SqlFragment::join(values, ","),
            SqlFragment::raw(")"),
        ]);
    }

    let col_list = wrap(
        "(",
        SqlFragment::join(keys.iter().map(|k| SqlFragment::ident(k)).collect(), ","),
        ")",
    );
    let rows: Vec<SqlFragment> = multi_constraint
        .iter()
        .map(|entry| {
            let vals: Vec<SqlFragment> = keys
                .iter()
                .map(|k| {
                    let value_type = columns[*k].value_type;
                    let (_, v) = entry.iter().find(|(ek, _)| ek == *k).unwrap();
                    SqlFragment::param(to_sqlite_value(&ivm_to_json(v), value_type))
                })
                .collect();
            wrap("(", SqlFragment::join(vals, ","), ")")
        })
        .collect();

    SqlFragment::concat(vec![
        col_list,
        SqlFragment::raw(" IN (VALUES "),
        SqlFragment::join(rows, ","),
        SqlFragment::raw(")"),
    ])
}

/// Port of `orderByToSQL`.
pub fn order_by_to_sql(order: &Ordering, reverse: bool) -> SqlFragment {
    let parts: Vec<SqlFragment> = order
        .iter()
        .map(|(field, dir)| {
            let effective = if reverse { flip(*dir) } else { *dir };
            SqlFragment::concat(vec![
                SqlFragment::ident(field),
                SqlFragment::raw(" "),
                SqlFragment::raw(direction_str(effective)),
            ])
        })
        .collect();
    SqlFragment::concat(vec![
        SqlFragment::raw("ORDER BY "),
        SqlFragment::join(parts, ", "),
    ])
}

fn flip(d: Direction) -> Direction {
    match d {
        Direction::Asc => Direction::Desc,
        Direction::Desc => Direction::Asc,
    }
}

fn direction_str(d: Direction) -> &'static str {
    match d {
        Direction::Asc => "asc",
        Direction::Desc => "desc",
    }
}

/// Port of `filtersToSQL`. Panics if `filters` contains a
/// `Condition::CorrelatedSubquery` (see module doc) or a `Parameter`/static
/// value reaches [`value_position_to_sql`] (surfaced as
/// [`QueryBuilderError::UnresolvedParameter`] instead, since that IS a
/// recoverable caller error upstream throws for too).
pub fn filters_to_sql(filters: &Condition) -> Result<SqlFragment, QueryBuilderError> {
    filters_to_sql_with_outer(filters, None)
}

/// Like [`filters_to_sql`] but carrying the name of the table the condition is
/// evaluated against (`outer_table`), so a `CorrelatedSubquery` (`EXISTS`) can
/// qualify its correlation's PARENT field with that table — otherwise an
/// unqualified column inside the `EXISTS (SELECT … FROM child …)` subquery
/// would bind to the child scope and silently mis-correlate.
pub fn filters_to_sql_with_outer(
    filters: &Condition,
    outer_table: Option<&str>,
) -> Result<SqlFragment, QueryBuilderError> {
    match filters {
        Condition::Simple { .. } => simple_condition_to_sql(filters),
        Condition::And { conditions } => {
            if conditions.is_empty() {
                return Ok(SqlFragment::raw("TRUE"));
            }
            let parts: Result<Vec<SqlFragment>, _> = conditions
                .iter()
                .map(|c| filters_to_sql_with_outer(c, outer_table))
                .collect();
            Ok(wrap("(", SqlFragment::join(parts?, " AND "), ")"))
        }
        Condition::Or { conditions } => {
            if conditions.is_empty() {
                return Ok(SqlFragment::raw("FALSE"));
            }
            let parts: Result<Vec<SqlFragment>, _> = conditions
                .iter()
                .map(|c| filters_to_sql_with_outer(c, outer_table))
                .collect();
            Ok(wrap("(", SqlFragment::join(parts?, " OR "), ")"))
        }
        Condition::CorrelatedSubquery { related, op, .. } => {
            exists_to_sql(related, *op, outer_table)
        }
    }
}

/// Double-quotes a SQL identifier (doubling embedded quotes), returning the
/// raw string form (for building qualified `"table"."column"` references).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Translates a `CorrelatedSubquery` existence condition into a SQL
/// `[NOT] EXISTS (SELECT 1 FROM child WHERE <correlation> [AND <subquery where>])`.
/// The correlation's child field is qualified with the child table; its parent
/// field with `outer_table` (so it escapes to the enclosing row). The
/// subquery's own `where_` columns stay unqualified — inside the subquery they
/// bind to the child table, which is correct. This is the SQL-pushdown
/// equivalent of upstream's IVM `EXISTS` join for a query's `exists(...)`
/// filter (the server-authoritative authorization pattern real apps rely on).
fn exists_to_sql(
    related: &CorrelatedSubquery,
    op: ExistsOp,
    outer_table: Option<&str>,
) -> Result<SqlFragment, QueryBuilderError> {
    let child_table = &related.subquery.table;
    let corr = &related.correlation;
    if corr.parent_field.len() != corr.child_field.len() || corr.parent_field.is_empty() {
        return Err(QueryBuilderError::UnresolvedParameter);
    }

    let mut clauses: Vec<SqlFragment> = Vec::new();
    for (parent_field, child_field) in corr.parent_field.iter().zip(corr.child_field.iter()) {
        let child_col = format!("{}.{}", quote_ident(child_table), quote_ident(child_field));
        let parent_col = match outer_table {
            Some(t) => format!("{}.{}", quote_ident(t), quote_ident(parent_field)),
            None => quote_ident(parent_field),
        };
        clauses.push(SqlFragment::raw(format!("{child_col} = {parent_col}")));
    }

    // The subquery's own where filters rows of the child table; its columns
    // bind to the child scope, so recurse with the child as the outer table
    // (a nested EXISTS then correlates back to this child correctly).
    if let Some(sub_where) = &related.subquery.where_ {
        clauses.push(wrap(
            "(",
            filters_to_sql_with_outer(sub_where, Some(child_table))?,
            ")",
        ));
    }

    let keyword = match op {
        ExistsOp::Exists => "EXISTS",
        ExistsOp::NotExists => "NOT EXISTS",
    };
    Ok(SqlFragment::concat(vec![
        SqlFragment::raw(format!(
            "{keyword} (SELECT 1 FROM {} WHERE ",
            quote_ident(child_table)
        )),
        SqlFragment::join(clauses, " AND "),
        SqlFragment::raw(")"),
    ]))
}

fn simple_condition_to_sql(filter: &Condition) -> Result<SqlFragment, QueryBuilderError> {
    let Condition::Simple { op, left, right } = filter else {
        unreachable!()
    };
    if matches!(op, SimpleOperator::In | SimpleOperator::NotIn) {
        let ValuePosition::Literal(lit) = right else {
            return Err(QueryBuilderError::UnresolvedParameter);
        };
        let json = literal_to_json(lit);
        return Ok(SqlFragment::concat(vec![
            value_position_to_sql(left)?,
            SqlFragment::raw(format!(" {} (SELECT value FROM json_each(", op.as_str())),
            SqlFragment::param(Value::Text(json.stringify())),
            SqlFragment::raw("))"),
        ]));
    }
    if matches!(
        op,
        SimpleOperator::Like
            | SimpleOperator::NotLike
            | SimpleOperator::ILike
            | SimpleOperator::NotILike
    ) {
        return like_condition_to_sql(*op, left, right);
    }
    Ok(SqlFragment::concat(vec![
        value_position_to_sql(left)?,
        SqlFragment::raw(format!(" {} ", op.as_str())),
        value_position_to_sql(right)?,
    ]))
}

fn like_condition_to_sql(
    op: SimpleOperator,
    left: &ValuePosition,
    right: &ValuePosition,
) -> Result<SqlFragment, QueryBuilderError> {
    let case_insensitive = matches!(op, SimpleOperator::ILike | SimpleOperator::NotILike);
    let negated = matches!(op, SimpleOperator::NotLike | SimpleOperator::NotILike);
    let like_op = if negated { "NOT LIKE" } else { "LIKE" };

    let left_sql = value_position_to_sql(left)?;
    let right_sql = value_position_to_sql(right)?;
    if case_insensitive {
        Ok(SqlFragment::concat(vec![
            SqlFragment::raw("lower("),
            left_sql,
            SqlFragment::raw(format!(") {like_op} lower(")),
            right_sql,
            SqlFragment::raw(") ESCAPE '\\'"),
        ]))
    } else {
        Ok(SqlFragment::concat(vec![
            left_sql,
            SqlFragment::raw(format!(" {like_op} ")),
            right_sql,
            SqlFragment::raw(" ESCAPE '\\'"),
        ]))
    }
}

fn value_position_to_sql(value: &ValuePosition) -> Result<SqlFragment, QueryBuilderError> {
    match value {
        ValuePosition::Column(col) => Ok(SqlFragment::ident(&col.name)),
        ValuePosition::Literal(lit) => Ok(SqlFragment::param(to_sqlite_value(
            &literal_to_json(lit),
            literal_value_type(lit),
        ))),
        ValuePosition::Parameter(_) => Err(QueryBuilderError::UnresolvedParameter),
    }
}

fn nullable_aware_equality(field: &str, value: Value, column_type: ColumnType) -> SqlFragment {
    let op = if column_type.optional { " IS " } else { " = " };
    SqlFragment::concat(vec![
        SqlFragment::ident(field),
        SqlFragment::raw(op),
        SqlFragment::param(value),
    ])
}

fn nullable_aware_range_comparison(
    field: &str,
    value: Value,
    operator: &str,
    column_type: ColumnType,
) -> SqlFragment {
    if value == Value::Null {
        return if operator == ">" {
            SqlFragment::concat(vec![
                SqlFragment::ident(field),
                SqlFragment::raw(" IS NOT NULL"),
            ])
        } else {
            SqlFragment::raw("FALSE")
        };
    }
    let comparison = SqlFragment::concat(vec![
        SqlFragment::ident(field),
        SqlFragment::raw(format!(" {operator} ")),
        SqlFragment::param(value.clone()),
    ]);
    if !column_type.optional {
        return comparison;
    }
    if operator == ">" {
        wrap(
            "(",
            SqlFragment::concat(vec![
                SqlFragment::param(value),
                SqlFragment::raw(" IS NULL OR "),
                comparison,
            ]),
            ")",
        )
    } else {
        wrap(
            "(",
            SqlFragment::concat(vec![
                SqlFragment::ident(field),
                SqlFragment::raw(" IS NULL OR "),
                comparison,
            ]),
            ")",
        )
    }
}

fn sargable_leading_start_bound(
    field: &str,
    value: Value,
    operator: &str,
    column_type: ColumnType,
) -> Option<SqlFragment> {
    if value == Value::Null || column_type.optional {
        return None;
    }
    let inclusive_operator = if operator == ">" { ">=" } else { "<=" };
    Some(SqlFragment::concat(vec![
        SqlFragment::ident(field),
        SqlFragment::raw(format!(" {inclusive_operator} ")),
        SqlFragment::param(value),
    ]))
}

/// Port of `gatherStartConstraints`. `column_types` must have an entry for
/// every field named in `order`. Panics (matching upstream's implicit
/// `Record` lookup) if a field is missing.
pub fn gather_start_constraints(
    start: &Start,
    reverse: bool,
    order: &Ordering,
    column_types: &BTreeMap<String, ColumnType>,
) -> SqlFragment {
    let mut constraints: Vec<SqlFragment> = Vec::new();
    let mut leading_bound: Option<SqlFragment> = None;
    let from = &start.row;

    for i in 0..order.len() {
        let mut group: Vec<SqlFragment> = Vec::new();
        let (i_field, i_direction) = &order[i];
        for (j, (j_field, _)) in order.iter().enumerate().take(i + 1) {
            if j == i {
                let column_type = column_types[i_field];
                let constraint_value =
                    to_sqlite_value(&row_get(from, i_field), column_type.value_type);
                let operator = match (i_direction, reverse) {
                    (Direction::Asc, false) => ">",
                    (Direction::Asc, true) => "<",
                    (Direction::Desc, false) => "<",
                    (Direction::Desc, true) => ">",
                };
                if i == 0 {
                    leading_bound = sargable_leading_start_bound(
                        i_field,
                        constraint_value.clone(),
                        operator,
                        column_type,
                    );
                }
                group.push(nullable_aware_range_comparison(
                    i_field,
                    constraint_value,
                    operator,
                    column_type,
                ));
            } else {
                let column_type = column_types[j_field];
                let value = to_sqlite_value(&row_get(from, j_field), column_type.value_type);
                group.push(nullable_aware_equality(j_field, value, column_type));
            }
        }
        constraints.push(wrap("(", SqlFragment::join(group, " AND "), ")"));
    }

    if start.basis == StartBasis::At {
        let group: Vec<SqlFragment> = order
            .iter()
            .map(|(field, _)| {
                let column_type = column_types[field];
                let value = to_sqlite_value(&row_get(from, field), column_type.value_type);
                nullable_aware_equality(field, value, column_type)
            })
            .collect();
        constraints.push(wrap("(", SqlFragment::join(group, " AND "), ")"));
    }

    let lexicographic_start = wrap("(", SqlFragment::join(constraints, " OR "), ")");
    match leading_bound {
        None => lexicographic_start,
        Some(bound) => wrap(
            "(",
            SqlFragment::concat(vec![bound, SqlFragment::raw(" AND "), lexicographic_start]),
            ")",
        ),
    }
}

fn row_get(
    row: &zero_cache_zql::ivm::data::Row,
    field: &str,
) -> zero_cache_shared::bigint_json::JsonValue {
    row.iter()
        .find(|(k, _)| k == field)
        .map(|(_, v)| v.clone())
        .unwrap_or(zero_cache_shared::bigint_json::JsonValue::Null)
}

/// Port of `buildSelectQuery`. Panics if `start` is set without `order`
/// (matching upstream's `assert`).
///
/// `column_order` is the SELECT list's column order — kept as an explicit
/// ordered slice rather than derived from `columns`' `BTreeMap` iteration
/// (alphabetical), since upstream's `Object.keys(columns)` actually
/// preserves the schema's own declared column order and this port's
/// `BTreeMap<String, ColumnType>` (used everywhere else purely for
/// key->type lookups, where order doesn't matter) would otherwise silently
/// reorder the SELECT list alphabetically instead.
#[allow(clippy::too_many_arguments)]
pub fn build_select_query(
    table_name: &str,
    column_order: &[String],
    columns: &BTreeMap<String, ColumnType>,
    constraint: Option<&Constraint>,
    filters: Option<&Condition>,
    order: Option<&Ordering>,
    reverse: bool,
    start: Option<&Start>,
    multi_constraints: &[MultiConstraint],
) -> Result<SqlFragment, QueryBuilderError> {
    let select_list = SqlFragment::join(
        column_order.iter().map(|c| SqlFragment::ident(c)).collect(),
        ",",
    );
    let mut query = SqlFragment::concat(vec![
        SqlFragment::raw("SELECT "),
        select_list,
        SqlFragment::raw(" FROM "),
        SqlFragment::ident(table_name),
    ]);

    let mut constraints = constraints_to_sql(constraint, columns);

    for mc in multi_constraints {
        if !mc.is_empty() {
            constraints.push(multi_constraint_to_sql(mc, columns));
        }
    }

    if let Some(start) = start {
        let order = order.expect("start requires ordering");
        constraints.push(gather_start_constraints(start, reverse, order, columns));
    }

    if let Some(filters) = filters {
        constraints.push(filters_to_sql_with_outer(filters, Some(table_name))?);
    }

    if !constraints.is_empty() {
        query = SqlFragment::concat(vec![
            query,
            SqlFragment::raw(" WHERE "),
            SqlFragment::join(constraints, " AND "),
        ]);
    }

    if let Some(order) = order {
        if !order.is_empty() {
            return Ok(SqlFragment::concat(vec![
                query,
                SqlFragment::raw(" "),
                order_by_to_sql(order, reverse),
            ]));
        }
    }
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::ColumnReference;

    fn col(name: &str, value_type: ValueType, optional: bool) -> (String, ColumnType) {
        (
            name.to_string(),
            ColumnType {
                value_type,
                optional,
            },
        )
    }

    fn cols(pairs: Vec<(&str, ValueType, bool)>) -> BTreeMap<String, ColumnType> {
        pairs.into_iter().map(|(n, t, o)| col(n, t, o)).collect()
    }

    #[test]
    fn ident_double_quotes_and_escapes_embedded_quotes() {
        assert_eq!(SqlFragment::ident("foo").text, "\"foo\"");
        assert_eq!(SqlFragment::ident("fo\"o").text, "\"fo\"\"o\"");
    }

    #[test]
    fn constraints_to_sql_builds_one_equality_per_key() {
        let columns = cols(vec![("id", ValueType::String, false)]);
        let constraint: Constraint = vec![(
            "id".to_string(),
            zero_cache_shared::bigint_json::JsonValue::String("i1".to_string()),
        )];
        let frags = constraints_to_sql(Some(&constraint), &columns);
        assert_eq!(frags.len(), 1);
        assert_eq!(frags[0].text, "\"id\" = ?");
        assert_eq!(frags[0].params, vec![Value::Text("i1".to_string())]);
    }

    #[test]
    fn multi_constraint_to_sql_single_column_uses_in_list() {
        let columns = cols(vec![("id", ValueType::Number, false)]);
        let mc: MultiConstraint = vec![
            vec![(
                "id".to_string(),
                zero_cache_shared::bigint_json::JsonValue::Number(1.0),
            )],
            vec![(
                "id".to_string(),
                zero_cache_shared::bigint_json::JsonValue::Number(2.0),
            )],
        ];
        let frag = multi_constraint_to_sql(&mc, &columns);
        assert_eq!(frag.text, "\"id\" IN (?,?)");
        assert_eq!(frag.params, vec![Value::Integer(1), Value::Integer(2)]);
    }

    #[test]
    fn multi_constraint_to_sql_compound_uses_values_in() {
        let columns = cols(vec![
            ("a", ValueType::Number, false),
            ("b", ValueType::Number, false),
        ]);
        let mc: MultiConstraint = vec![vec![
            (
                "a".to_string(),
                zero_cache_shared::bigint_json::JsonValue::Number(1.0),
            ),
            (
                "b".to_string(),
                zero_cache_shared::bigint_json::JsonValue::Number(2.0),
            ),
        ]];
        let frag = multi_constraint_to_sql(&mc, &columns);
        assert_eq!(frag.text, "(\"a\",\"b\") IN (VALUES (?,?))");
    }

    #[test]
    #[should_panic(expected = "non-empty")]
    fn multi_constraint_to_sql_panics_on_empty() {
        let columns = BTreeMap::new();
        multi_constraint_to_sql(&vec![], &columns);
    }

    #[test]
    fn order_by_to_sql_reverses_directions() {
        let order: Ordering = vec![
            ("a".to_string(), Direction::Asc),
            ("b".to_string(), Direction::Desc),
        ];
        assert_eq!(
            order_by_to_sql(&order, false).text,
            "ORDER BY \"a\" asc, \"b\" desc"
        );
        assert_eq!(
            order_by_to_sql(&order, true).text,
            "ORDER BY \"a\" desc, \"b\" asc"
        );
    }

    fn simple(op: SimpleOperator, left: ValuePosition, right: ValuePosition) -> Condition {
        Condition::Simple { op, left, right }
    }

    fn col_ref(name: &str) -> ValuePosition {
        ValuePosition::Column(ColumnReference {
            name: name.to_string(),
        })
    }

    fn lit_num(n: f64) -> ValuePosition {
        ValuePosition::Literal(LiteralValue::Number(n))
    }

    #[test]
    fn filters_to_sql_simple_eq() {
        let cond = simple(SimpleOperator::Eq, col_ref("a"), lit_num(1.0));
        let frag = filters_to_sql(&cond).unwrap();
        assert_eq!(frag.text, "\"a\" = ?");
        assert_eq!(frag.params, vec![Value::Integer(1)]);
    }

    #[test]
    fn filters_to_sql_and_or_empty_are_true_false() {
        assert_eq!(
            filters_to_sql(&Condition::And { conditions: vec![] })
                .unwrap()
                .text,
            "TRUE"
        );
        assert_eq!(
            filters_to_sql(&Condition::Or { conditions: vec![] })
                .unwrap()
                .text,
            "FALSE"
        );
    }

    #[test]
    fn filters_to_sql_and_joins_with_and() {
        let cond = Condition::And {
            conditions: vec![
                simple(SimpleOperator::Eq, col_ref("a"), lit_num(1.0)),
                simple(SimpleOperator::Eq, col_ref("b"), lit_num(2.0)),
            ],
        };
        assert_eq!(
            filters_to_sql(&cond).unwrap().text,
            "(\"a\" = ? AND \"b\" = ?)"
        );
    }

    #[test]
    fn filters_to_sql_in_uses_json_each() {
        let cond = simple(
            SimpleOperator::In,
            col_ref("a"),
            ValuePosition::Literal(LiteralValue::Array(vec![
                LiteralValue::Number(1.0),
                LiteralValue::Number(2.0),
            ])),
        );
        let frag = filters_to_sql(&cond).unwrap();
        assert_eq!(frag.text, "\"a\" IN (SELECT value FROM json_each(?))");
        assert_eq!(frag.params, vec![Value::Text("[1,2]".to_string())]);
    }

    #[test]
    fn filters_to_sql_ilike_lowercases_both_sides() {
        let cond = simple(
            SimpleOperator::ILike,
            col_ref("name"),
            ValuePosition::Literal(LiteralValue::String("a%".into())),
        );
        let frag = filters_to_sql(&cond).unwrap();
        assert_eq!(frag.text, "lower(\"name\") LIKE lower(?) ESCAPE '\\'");
    }

    #[test]
    fn filters_to_sql_errors_on_an_unresolved_parameter() {
        let cond = simple(
            SimpleOperator::Eq,
            col_ref("a"),
            ValuePosition::Parameter(zero_cache_protocol::ast::Parameter {
                raw: zero_cache_shared::bigint_json::JsonValue::Null,
            }),
        );
        assert_eq!(
            filters_to_sql(&cond),
            Err(QueryBuilderError::UnresolvedParameter)
        );
    }

    #[test]
    fn filters_to_sql_translates_a_correlated_subquery_to_exists() {
        // A correlated subquery is now compiled to a SQL `EXISTS (...)` with
        // the parent field qualified by the outer table (see `exists_to_sql`).
        let cond = Condition::CorrelatedSubquery {
            related: zero_cache_protocol::ast::CorrelatedSubquery {
                correlation: zero_cache_protocol::ast::Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["parentID".into()],
                },
                subquery: Box::new(zero_cache_protocol::ast::Ast::table("child")),
                system: None,
                hidden: None,
            },
            op: zero_cache_protocol::ast::ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        };
        let frag = filters_to_sql_with_outer(&cond, Some("parent")).unwrap();
        assert_eq!(
            frag.text,
            "EXISTS (SELECT 1 FROM \"child\" WHERE \"child\".\"parentID\" = \"parent\".\"id\")"
        );
    }

    #[test]
    fn build_select_query_combines_everything_and_runs_against_real_sqlite() {
        let db = crate::StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id TEXT PRIMARY KEY, priority INTEGER, title TEXT)")
            .unwrap();
        db.run(
            "INSERT INTO issue (id, priority, title) VALUES (?, ?, ?)",
            &[
                Value::Text("1".into()),
                Value::Integer(2),
                Value::Text("a".into()),
            ],
        )
        .unwrap();
        db.run(
            "INSERT INTO issue (id, priority, title) VALUES (?, ?, ?)",
            &[
                Value::Text("2".into()),
                Value::Integer(1),
                Value::Text("b".into()),
            ],
        )
        .unwrap();
        db.run(
            "INSERT INTO issue (id, priority, title) VALUES (?, ?, ?)",
            &[
                Value::Text("3".into()),
                Value::Integer(3),
                Value::Text("c".into()),
            ],
        )
        .unwrap();

        let columns = cols(vec![
            ("id", ValueType::String, false),
            ("priority", ValueType::Number, false),
            ("title", ValueType::String, false),
        ]);
        let column_order = vec![
            "id".to_string(),
            "priority".to_string(),
            "title".to_string(),
        ];
        let order: Ordering = vec![("priority".to_string(), Direction::Asc)];
        let frag = build_select_query(
            "issue",
            &column_order,
            &columns,
            None,
            None,
            Some(&order),
            false,
            None,
            &[],
        )
        .unwrap();

        let rows = db
            .query_uncached(
                &frag.text,
                &frag
                    .params
                    .iter()
                    .map(json_value_from_rusqlite)
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let ids: Vec<String> = rows
            .iter()
            .map(|r| match &r[0].1 {
                crate::Value::Text(s) => s.clone(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            ids,
            vec!["2", "1", "3"],
            "must be ordered by priority ascending"
        );
    }

    fn json_value_from_rusqlite(v: &Value) -> crate::Value {
        match v {
            Value::Null => crate::Value::Null,
            Value::Integer(i) => crate::Value::Integer(*i),
            Value::Real(r) => crate::Value::Real(*r),
            Value::Text(s) => crate::Value::Text(s.clone()),
            Value::Blob(b) => crate::Value::Blob(b.clone()),
        }
    }

    #[test]
    fn gather_start_constraints_builds_a_sargable_after_cursor() {
        let columns = cols(vec![
            ("priority", ValueType::Number, false),
            ("id", ValueType::String, false),
        ]);
        let order: Ordering = vec![
            ("priority".to_string(), Direction::Asc),
            ("id".to_string(), Direction::Asc),
        ];
        let start = Start {
            row: vec![
                (
                    "priority".to_string(),
                    zero_cache_shared::bigint_json::JsonValue::Number(2.0),
                ),
                (
                    "id".to_string(),
                    zero_cache_shared::bigint_json::JsonValue::String("i1".into()),
                ),
            ],
            basis: StartBasis::After,
        };
        let frag = gather_start_constraints(&start, false, &order, &columns);
        // Sargable leading bound ANDed with the full lexicographic OR-chain.
        assert!(frag.text.starts_with("(\"priority\" >= ? AND ("));
        assert!(frag.text.contains(" OR "));
    }
}

#[cfg(test)]
mod exists_tests {
    use super::*;
    use zero_cache_protocol::ast::{
        Ast, ColumnReference, CorrelatedSubquery, Correlation, ExistsOp,
    };

    /// EXISTS with a subquery WHERE: the correlation qualifies parent (outer)
    /// and child (subquery) tables; the subquery's own filter stays unqualified
    /// (binds to the child scope) and its param is threaded through. This is
    /// hunting-game's `exists(child.where(...))` authorization pattern.
    #[test]
    fn exists_with_subquery_where_qualifies_correlation_and_binds_params() {
        let cond = Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["userId".into()],
                },
                subquery: Box::new(Ast {
                    table: "membership".into(),
                    where_: Some(Condition::Simple {
                        op: SimpleOperator::Eq,
                        left: ValuePosition::Column(ColumnReference {
                            name: "status".into(),
                        }),
                        right: ValuePosition::Literal(LiteralValue::String("active".into())),
                    }),
                    ..Default::default()
                }),
                system: None,
                hidden: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        };
        let frag = filters_to_sql_with_outer(&cond, Some("user")).unwrap();
        assert_eq!(
            frag.text,
            "EXISTS (SELECT 1 FROM \"membership\" WHERE \"membership\".\"userId\" = \"user\".\"id\" AND (\"status\" = ?))"
        );
        assert_eq!(frag.params, vec![Value::Text("active".to_string())]);
    }

    /// NOT EXISTS negates correctly.
    #[test]
    fn not_exists_emits_not_exists() {
        let cond = Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["userId".into()],
                },
                subquery: Box::new(Ast::table("ban")),
                system: None,
                hidden: None,
            },
            op: ExistsOp::NotExists,
            flip: None,
            scalar: None,
            plan_id: None,
        };
        let frag = filters_to_sql_with_outer(&cond, Some("user")).unwrap();
        assert!(frag.text.starts_with("NOT EXISTS ("), "{}", frag.text);
    }
}
