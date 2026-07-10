//! Small live `inspect` analyze-query slice.
//!
//! Upstream analyze-query runs through the full query analyzer/planner stack.
//! This module ports the first useful runtime-backed subset: direct AST queries
//! against this demo server's SQLite replica. It returns the protocol result
//! envelope with generated SQL, read row counts, optional read rows, and SQLite
//! `EXPLAIN QUERY PLAN` output. Related subqueries are analyzed by fetching
//! child rows constrained by their parent result set. `EXISTS`/`NOT EXISTS`
//! correlated-subquery conditions are evaluated against the SQLite replica.
//! Callers may pass custom-query/read-authorizer transformed ASTs; the live
//! inspect handler does that before invoking this module. Table/column/primary
//! key metadata can now be introspected from SQLite for the analyzed AST graph,
//! so inspect analysis is no longer limited to the demo hydration registry.

use std::collections::BTreeMap;
use std::time::Instant;

use zero_cache_protocol::analyze_query_result::AnalyzeQueryResult;
use zero_cache_protocol::ast::{
    Ast, Bound, Condition, CorrelatedSubquery, ExistsOp, LiteralValue, SimpleOperator,
    ValuePosition,
};
use zero_cache_protocol::client_schema::ValueType;
use zero_cache_protocol::complete_ordering::complete_ordering;
use zero_cache_protocol::inspect_up::AnalyzeQueryOptions;
use zero_cache_protocol::query_hash::hash_of_ast;
use zero_cache_protocol::row_patch::Row;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_sqlite::lite_tables::list_tables;
use zero_cache_sqlite::query_builder::{build_select_query, ColumnType};
use zero_cache_sqlite::{DbError, StatementRunner, Value};
use zero_cache_view_syncer::cvr_types::{RowId, RowRecord};
use zero_cache_zql::ivm::data::compare_values;
use zero_cache_zql::ivm::operator::{Start, StartBasis};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzeQueryColumn {
    pub name: String,
    pub value_type: ValueType,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzeQueryTable {
    pub table_name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<AnalyzeQueryColumn>,
}

#[derive(Debug, thiserror::Error)]
pub enum AnalyzeQueryError {
    #[error("analyze-query currently supports AST queries only")]
    MissingAst,
    #[error("analyze-query custom query transforms are not yet ported")]
    CustomQuery,
    #[error("analyze-query encountered an unsupported correlated-subquery condition shape")]
    RelatedQuery,
    #[error("analyze-query related query `{0}` has mismatched parent/child correlation fields")]
    MismatchedCorrelation(String),
    #[error("analyze-query condition uses an unsupported column-vs-column comparison")]
    UnsupportedColumnComparison,
    #[error("analyze-query condition has an unresolved static parameter")]
    UnresolvedParameter,
    #[error("analyze-query table `{0}` is not in this handler's catalog")]
    UnknownTable(String),
    #[error("analyze-query start row must be a JSON object")]
    StartRowNotObject,
    #[error("analyze-query limit must be a finite non-negative integer")]
    InvalidLimit,
    #[error("{0}")]
    Db(#[from] DbError),
    #[error("{0}")]
    QueryBuilder(#[from] zero_cache_sqlite::query_builder::QueryBuilderError),
}

pub fn analyze_catalog_from_sqlite_ast(
    db: &StatementRunner,
    ast: &Ast,
) -> Result<Vec<AnalyzeQueryTable>, AnalyzeQueryError> {
    let tables = list_tables(db)?;
    let needed = tables_needed_by_ast(ast);
    let mut catalog = Vec::new();
    for table_name in needed {
        let Some(table) = tables.iter().find(|table| table.name == table_name) else {
            return Err(AnalyzeQueryError::UnknownTable(table_name));
        };
        catalog.push(AnalyzeQueryTable {
            table_name: table.name.clone(),
            primary_key: table.primary_key.clone().unwrap_or_default(),
            columns: table
                .columns
                .iter()
                .map(|(name, spec)| AnalyzeQueryColumn {
                    name: name.clone(),
                    value_type: sqlite_type_to_value_type(&spec.data_type),
                    optional: !spec.not_null.unwrap_or(false)
                        && !table
                            .primary_key
                            .as_ref()
                            .is_some_and(|pk| pk.iter().any(|key| key == name)),
                })
                .collect(),
        });
    }
    Ok(catalog)
}

#[allow(clippy::too_many_arguments)]
pub fn analyze_sqlite_ast_query(
    db: &StatementRunner,
    catalog: &[AnalyzeQueryTable],
    ast: Option<&Ast>,
    custom_name: Option<&str>,
    options: Option<&AnalyzeQueryOptions>,
    synced_query_id: Option<&str>,
    row_records: &[RowRecord],
    row_bodies: &[(RowId, Row)],
) -> Result<AnalyzeQueryResult, AnalyzeQueryError> {
    if custom_name.is_some() {
        return Err(AnalyzeQueryError::CustomQuery);
    }
    let ast = ast.ok_or(AnalyzeQueryError::MissingAst)?;
    let table = catalog
        .iter()
        .find(|table| table.table_name == ast.table)
        .ok_or_else(|| AnalyzeQueryError::UnknownTable(ast.table.clone()))?;
    let has_correlated_where = ast.where_.as_ref().is_some_and(has_correlated_subquery);

    let ordered_ast = complete_ordering(ast, &|table_name| {
        primary_key_for(catalog, table_name).unwrap_or_default()
    });
    let start = ordered_ast
        .start
        .as_ref()
        .map(start_from_bound)
        .transpose()?;

    let column_order: Vec<String> = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let column_types = column_types(table);

    let started = Instant::now();
    let mut frag = build_select_query(
        &ordered_ast.table,
        &column_order,
        &column_types,
        None,
        if has_correlated_where {
            None
        } else {
            ordered_ast.where_.as_ref()
        },
        ordered_ast.order_by.as_ref(),
        false,
        start.as_ref(),
        &[],
    )?;
    if let Some(limit) = ordered_ast.limit {
        append_limit(&mut frag, limit)?;
    }
    let rows = db.query_uncached(&frag.text, &frag.params)?;
    let mut protocol_rows: Vec<Row> = rows.into_iter().map(row_to_protocol).collect();
    if has_correlated_where {
        if let Some(condition) = &ordered_ast.where_ {
            protocol_rows = filter_rows_with_condition(db, catalog, protocol_rows, condition)?;
        }
    }
    let query = frag.text.clone();
    let read_count = protocol_rows.len() as f64;
    let sqlite_plans = explain_query(db, &query, &frag.params)?;
    let db_scan_count = sqlite_scan_count(&sqlite_plans, read_count);

    let mut rows_by_source = vec![(
        ordered_ast.table.clone(),
        vec![(query.clone(), protocol_rows.clone())],
    )];
    let mut counts_by_source = vec![(ordered_ast.table.clone(), vec![(query.clone(), read_count)])];
    let mut sqlite_plan_entries = vec![(query.clone(), sqlite_plans)];
    let mut db_scans_by_query = vec![(
        ordered_ast.table.clone(),
        db_scan_count
            .map(|count| vec![(query.clone(), count)])
            .unwrap_or_default(),
    )];

    if let Some(related) = &ordered_ast.related {
        for relation in related {
            let child = analyze_related_query(db, catalog, &protocol_rows, relation)?;
            append_related_analysis(
                child,
                &mut rows_by_source,
                &mut counts_by_source,
                &mut db_scans_by_query,
                &mut sqlite_plan_entries,
            );
        }
    }

    let total_read_count: f64 = counts_by_source
        .iter()
        .flat_map(|(_, queries)| queries.iter().map(|(_, count)| *count))
        .sum();
    let elapsed = started.elapsed().as_secs_f64() * 1000.0;

    let ast_hash = hash_of_ast(ast);
    let synced_query_id = synced_query_id.unwrap_or(&ast_hash);
    let synced_rows = synced_rows_for_query(row_records, row_bodies, synced_query_id);
    let synced_row_count = synced_rows.len() as f64;
    let include_rows = options.and_then(|o| o.vended_rows).unwrap_or(false);
    let include_synced_rows = options.and_then(|o| o.synced_rows).unwrap_or(false);

    let warnings = Vec::new();

    Ok(AnalyzeQueryResult {
        warnings,
        synced_rows: include_synced_rows.then_some(vec![(ordered_ast.table.clone(), synced_rows)]),
        synced_row_count,
        start: 0.0,
        end: elapsed,
        elapsed: Some(elapsed),
        after_permissions: None,
        vended_row_counts: Some(counts_by_source.clone()),
        vended_rows: include_rows.then(|| rows_by_source.clone()),
        sqlite_plans: Some(sqlite_plan_entries),
        read_rows: include_rows.then_some(rows_by_source),
        read_row_counts_by_query: Some(counts_by_source),
        read_row_count: Some(total_read_count),
        db_scans_by_query: Some(db_scans_by_query),
        join_plans: None,
    })
}

struct RelatedAnalysis {
    source: String,
    query: String,
    rows: Vec<Row>,
    count: f64,
    scan_count: Option<f64>,
    sqlite_plans: Vec<String>,
    related: Vec<RelatedAnalysis>,
}

fn analyze_related_query(
    db: &StatementRunner,
    catalog: &[AnalyzeQueryTable],
    parent_rows: &[Row],
    related: &CorrelatedSubquery,
) -> Result<RelatedAnalysis, AnalyzeQueryError> {
    let child_ast = related.subquery.as_ref();
    if child_ast
        .where_
        .as_ref()
        .is_some_and(has_correlated_subquery)
    {
        return Err(AnalyzeQueryError::RelatedQuery);
    }
    if related.correlation.parent_field.len() != related.correlation.child_field.len() {
        return Err(AnalyzeQueryError::MismatchedCorrelation(
            child_ast.table.clone(),
        ));
    }
    let child_table = catalog
        .iter()
        .find(|table| table.table_name == child_ast.table)
        .ok_or_else(|| AnalyzeQueryError::UnknownTable(child_ast.table.clone()))?;

    let ordered_child_ast = complete_ordering(child_ast, &|table_name| {
        primary_key_for(catalog, table_name).unwrap_or_default()
    });
    let start = ordered_child_ast
        .start
        .as_ref()
        .map(start_from_bound)
        .transpose()?;
    let column_order: Vec<String> = child_table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let column_types = column_types(child_table);
    let multi_constraints = parent_rows_to_child_constraints(
        parent_rows,
        &related.correlation.parent_field,
        &related.correlation.child_field,
    );

    if multi_constraints.is_empty() {
        let query = empty_related_query(&ordered_child_ast.table, &column_order);
        let sqlite_plans = explain_query(db, &query, &[])?;
        let rows = Vec::new();
        let nested = analyze_nested_related(db, catalog, &rows, &ordered_child_ast)?;
        return Ok(RelatedAnalysis {
            source: ordered_child_ast.table.clone(),
            query,
            rows,
            count: 0.0,
            scan_count: None,
            sqlite_plans,
            related: nested,
        });
    }

    let mut frag = build_select_query(
        &ordered_child_ast.table,
        &column_order,
        &column_types,
        None,
        ordered_child_ast.where_.as_ref(),
        ordered_child_ast.order_by.as_ref(),
        false,
        start.as_ref(),
        &multi_constraints,
    )?;
    if let Some(limit) = ordered_child_ast.limit {
        append_limit(&mut frag, limit)?;
    }
    let rows: Vec<Row> = db
        .query_uncached(&frag.text, &frag.params)?
        .into_iter()
        .map(row_to_protocol)
        .collect();
    let count = rows.len() as f64;
    let sqlite_plans = explain_query(db, &frag.text, &frag.params)?;
    let scan_count = sqlite_scan_count(&sqlite_plans, count);
    let nested = analyze_nested_related(db, catalog, &rows, &ordered_child_ast)?;
    Ok(RelatedAnalysis {
        source: ordered_child_ast.table.clone(),
        query: frag.text,
        rows,
        count,
        scan_count,
        sqlite_plans,
        related: nested,
    })
}

fn analyze_nested_related(
    db: &StatementRunner,
    catalog: &[AnalyzeQueryTable],
    parent_rows: &[Row],
    ast: &Ast,
) -> Result<Vec<RelatedAnalysis>, AnalyzeQueryError> {
    let mut nested = Vec::new();
    if let Some(related) = &ast.related {
        for relation in related {
            nested.push(analyze_related_query(db, catalog, parent_rows, relation)?);
        }
    }
    Ok(nested)
}

#[allow(clippy::type_complexity)]
fn append_related_analysis(
    analysis: RelatedAnalysis,
    rows_by_source: &mut Vec<(String, Vec<(String, Vec<Row>)>)>,
    counts_by_source: &mut Vec<(String, Vec<(String, f64)>)>,
    db_scans_by_query: &mut Vec<(String, Vec<(String, f64)>)>,
    sqlite_plan_entries: &mut Vec<(String, Vec<String>)>,
) {
    let RelatedAnalysis {
        source,
        query,
        rows,
        count,
        scan_count,
        sqlite_plans,
        related,
    } = analysis;
    rows_by_source.push((source.clone(), vec![(query.clone(), rows)]));
    counts_by_source.push((source.clone(), vec![(query.clone(), count)]));
    db_scans_by_query.push((
        source,
        scan_count
            .map(|count| vec![(query.clone(), count)])
            .unwrap_or_default(),
    ));
    sqlite_plan_entries.push((query, sqlite_plans));
    for child in related {
        append_related_analysis(
            child,
            rows_by_source,
            counts_by_source,
            db_scans_by_query,
            sqlite_plan_entries,
        );
    }
}

fn filter_rows_with_condition(
    db: &StatementRunner,
    catalog: &[AnalyzeQueryTable],
    rows: Vec<Row>,
    condition: &Condition,
) -> Result<Vec<Row>, AnalyzeQueryError> {
    let mut filtered = Vec::new();
    for row in rows {
        if condition_matches(db, catalog, &row, condition)? {
            filtered.push(row);
        }
    }
    Ok(filtered)
}

fn condition_matches(
    db: &StatementRunner,
    catalog: &[AnalyzeQueryTable],
    row: &Row,
    condition: &Condition,
) -> Result<bool, AnalyzeQueryError> {
    match condition {
        Condition::Simple { op, left, right } => simple_condition_matches(row, *op, left, right),
        Condition::And { conditions } => {
            for condition in conditions {
                if !condition_matches(db, catalog, row, condition)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Condition::Or { conditions } => {
            for condition in conditions {
                if condition_matches(db, catalog, row, condition)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Condition::CorrelatedSubquery { related, op, .. } => {
            let analysis = analyze_related_query(db, catalog, std::slice::from_ref(row), related)?;
            let matched = analysis.count > 0.0;
            Ok(match op {
                ExistsOp::Exists => matched,
                ExistsOp::NotExists => !matched,
            })
        }
    }
}

fn simple_condition_matches(
    row: &Row,
    op: SimpleOperator,
    left: &ValuePosition,
    right: &ValuePosition,
) -> Result<bool, AnalyzeQueryError> {
    let lhs = value_position(row, left)?;
    let rhs = value_position(row, right)?;
    if !matches!(op, SimpleOperator::Is | SimpleOperator::IsNot)
        && (matches!(&lhs, JsonValue::Null) || matches!(&rhs, JsonValue::Null))
    {
        return Ok(false);
    }
    Ok(match op {
        SimpleOperator::Eq => lhs == rhs,
        SimpleOperator::Ne => lhs != rhs,
        SimpleOperator::Is => lhs == rhs,
        SimpleOperator::IsNot => lhs != rhs,
        SimpleOperator::Lt => compare_values(&lhs, &rhs).is_lt(),
        SimpleOperator::Gt => compare_values(&lhs, &rhs).is_gt(),
        SimpleOperator::Le => compare_values(&lhs, &rhs).is_le(),
        SimpleOperator::Ge => compare_values(&lhs, &rhs).is_ge(),
        SimpleOperator::In => literal_array_contains(&lhs, &rhs)?,
        SimpleOperator::NotIn => !literal_array_contains(&lhs, &rhs)?,
        SimpleOperator::Like
        | SimpleOperator::NotLike
        | SimpleOperator::ILike
        | SimpleOperator::NotILike => return Err(AnalyzeQueryError::UnsupportedColumnComparison),
    })
}

fn value_position(row: &Row, value: &ValuePosition) -> Result<JsonValue, AnalyzeQueryError> {
    match value {
        ValuePosition::Literal(literal) => Ok(literal_to_json(literal)),
        ValuePosition::Column(column) => Ok(row
            .iter()
            .find(|(name, _)| name == &column.name)
            .map(|(_, value)| value.clone())
            .unwrap_or(JsonValue::Null)),
        ValuePosition::Parameter(_) => Err(AnalyzeQueryError::UnresolvedParameter),
    }
}

fn literal_to_json(literal: &LiteralValue) -> JsonValue {
    match literal {
        LiteralValue::String(value) => JsonValue::String(value.clone()),
        LiteralValue::Number(value) => JsonValue::Number(*value),
        LiteralValue::Bool(value) => JsonValue::Bool(*value),
        LiteralValue::Null => JsonValue::Null,
        LiteralValue::Array(values) => {
            JsonValue::Array(values.iter().map(literal_to_json).collect())
        }
    }
}

fn literal_array_contains(lhs: &JsonValue, rhs: &JsonValue) -> Result<bool, AnalyzeQueryError> {
    let JsonValue::Array(values) = rhs else {
        return Err(AnalyzeQueryError::UnsupportedColumnComparison);
    };
    Ok(values.iter().any(|value| value == lhs))
}

fn primary_key_for(catalog: &[AnalyzeQueryTable], table_name: &str) -> Option<Vec<String>> {
    catalog
        .iter()
        .find(|table| table.table_name == table_name)
        .map(|table| {
            table
                .primary_key
                .iter()
                .map(|key| key.to_string())
                .collect()
        })
}

fn column_types(table: &AnalyzeQueryTable) -> BTreeMap<String, ColumnType> {
    table
        .columns
        .iter()
        .map(|column| {
            (
                column.name.clone(),
                ColumnType {
                    value_type: column.value_type,
                    optional: column.optional,
                },
            )
        })
        .collect()
}

fn parent_rows_to_child_constraints(
    parent_rows: &[Row],
    parent_fields: &[String],
    child_fields: &[String],
) -> Vec<zero_cache_sqlite::query_builder::MultiConstraint> {
    let constraints: zero_cache_sqlite::query_builder::MultiConstraint = parent_rows
        .iter()
        .filter_map(|row| {
            let mut constraint = Vec::with_capacity(parent_fields.len());
            for (parent_field, child_field) in parent_fields.iter().zip(child_fields) {
                let (_, value) = row.iter().find(|(field, _)| field == parent_field)?;
                constraint.push((child_field.clone(), value.clone()));
            }
            Some(constraint)
        })
        .collect();
    if constraints.is_empty() {
        Vec::new()
    } else {
        vec![constraints]
    }
}

fn empty_related_query(table_name: &str, column_order: &[String]) -> String {
    let select_list = zero_cache_sqlite::query_builder::SqlFragment::join(
        column_order
            .iter()
            .map(|column| zero_cache_sqlite::query_builder::SqlFragment::ident(column))
            .collect(),
        ",",
    );
    zero_cache_sqlite::query_builder::SqlFragment::concat(vec![
        zero_cache_sqlite::query_builder::SqlFragment::raw("SELECT "),
        select_list,
        zero_cache_sqlite::query_builder::SqlFragment::raw(" FROM "),
        zero_cache_sqlite::query_builder::SqlFragment::ident(table_name),
        zero_cache_sqlite::query_builder::SqlFragment::raw(" WHERE FALSE"),
    ])
    .text
}

fn has_correlated_subquery(condition: &Condition) -> bool {
    match condition {
        Condition::CorrelatedSubquery { .. } => true,
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(has_correlated_subquery)
        }
        Condition::Simple { .. } => false,
    }
}

fn append_limit(
    frag: &mut zero_cache_sqlite::query_builder::SqlFragment,
    limit: f64,
) -> Result<(), AnalyzeQueryError> {
    if !limit.is_finite() || limit < 0.0 || limit.fract() != 0.0 || limit > i64::MAX as f64 {
        return Err(AnalyzeQueryError::InvalidLimit);
    }
    frag.text.push_str(" LIMIT ?");
    frag.params.push(Value::Integer(limit as i64));
    Ok(())
}

fn start_from_bound(bound: &Bound) -> Result<Start, AnalyzeQueryError> {
    let JsonValue::Object(fields) = &bound.row else {
        return Err(AnalyzeQueryError::StartRowNotObject);
    };
    Ok(Start {
        row: fields.clone(),
        basis: if bound.exclusive {
            StartBasis::After
        } else {
            StartBasis::At
        },
    })
}

fn explain_query(
    db: &StatementRunner,
    query: &str,
    params: &[Value],
) -> Result<Vec<String>, DbError> {
    let rows = db.query_uncached(&format!("EXPLAIN QUERY PLAN {query}"), params)?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            row.iter()
                .find(|(col, _)| col == "detail")
                .and_then(|(_, value)| match value {
                    Value::Text(s) => Some(s.clone()),
                    _ => None,
                })
        })
        .collect())
}

fn sqlite_scan_count(plan_details: &[String], read_count: f64) -> Option<f64> {
    plan_details
        .iter()
        .any(|line| line.split_whitespace().any(|word| word == "SCAN"))
        .then_some(read_count)
}

fn row_to_protocol(row: zero_cache_sqlite::Row) -> Row {
    row.into_iter()
        .map(|(column, value)| (column, sqlite_value_to_json(value)))
        .collect()
}

fn sqlite_value_to_json(value: Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Integer(n) => JsonValue::Number(n as f64),
        Value::Real(n) => JsonValue::Number(n),
        Value::Text(s) => JsonValue::String(s),
        Value::Blob(bytes) => JsonValue::String(String::from_utf8_lossy(&bytes).into_owned()),
    }
}

fn tables_needed_by_ast(ast: &Ast) -> Vec<String> {
    let mut tables = Vec::new();
    collect_tables_from_ast(ast, &mut tables);
    tables
}

fn collect_tables_from_ast(ast: &Ast, tables: &mut Vec<String>) {
    if !tables.iter().any(|table| table == &ast.table) {
        tables.push(ast.table.clone());
    }
    if let Some(where_) = &ast.where_ {
        collect_tables_from_condition(where_, tables);
    }
    if let Some(related) = &ast.related {
        for relation in related {
            collect_tables_from_ast(&relation.subquery, tables);
        }
    }
}

fn collect_tables_from_condition(condition: &Condition, tables: &mut Vec<String>) {
    match condition {
        Condition::Simple { .. } => {}
        Condition::And { conditions } | Condition::Or { conditions } => {
            for condition in conditions {
                collect_tables_from_condition(condition, tables);
            }
        }
        Condition::CorrelatedSubquery { related, .. } => {
            collect_tables_from_ast(&related.subquery, tables);
        }
    }
}

fn sqlite_type_to_value_type(data_type: &str) -> ValueType {
    let upper = data_type.to_ascii_uppercase();
    if upper.contains("BOOL") {
        ValueType::Boolean
    } else if upper.contains("INT")
        || upper.contains("REAL")
        || upper.contains("FLOA")
        || upper.contains("DOUB")
        || upper.contains("NUM")
        || upper.contains("DEC")
    {
        ValueType::Number
    } else if upper.contains("JSON") {
        ValueType::Json
    } else {
        ValueType::String
    }
}

fn synced_rows_for_query(
    row_records: &[RowRecord],
    row_bodies: &[(RowId, Row)],
    query_id: &str,
) -> Vec<Row> {
    row_records
        .iter()
        .filter(|record| {
            record
                .ref_counts
                .as_ref()
                .and_then(|counts| counts.get(query_id))
                .is_some_and(|count| *count > 0)
        })
        .filter_map(|record| {
            row_bodies
                .iter()
                .find(|(id, _)| id == &record.id)
                .map(|(_, row)| row.clone())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::{
        Bound, ColumnReference, Condition, CorrelatedSubquery, Correlation, Direction, ExistsOp,
        LiteralValue, SimpleOperator, ValuePosition,
    };

    fn col(name: &str, value_type: ValueType, optional: bool) -> AnalyzeQueryColumn {
        AnalyzeQueryColumn {
            name: name.to_string(),
            value_type,
            optional,
        }
    }

    fn issue_columns() -> Vec<AnalyzeQueryColumn> {
        vec![
            col("id", ValueType::Number, false),
            col("title", ValueType::String, false),
        ]
    }

    fn comment_columns() -> Vec<AnalyzeQueryColumn> {
        vec![
            col("id", ValueType::Number, false),
            col("issueID", ValueType::Number, false),
            col("body", ValueType::String, false),
        ]
    }

    fn reaction_columns() -> Vec<AnalyzeQueryColumn> {
        vec![
            col("id", ValueType::Number, false),
            col("commentID", ValueType::Number, false),
            col("emoji", ValueType::String, false),
        ]
    }

    fn locale_issue_columns() -> Vec<AnalyzeQueryColumn> {
        vec![
            col("tenantID", ValueType::Number, false),
            col("issueID", ValueType::Number, false),
            col("title", ValueType::String, false),
        ]
    }

    fn locale_comment_columns() -> Vec<AnalyzeQueryColumn> {
        vec![
            col("id", ValueType::Number, false),
            col("tenantID", ValueType::Number, false),
            col("issueID", ValueType::Number, false),
            col("body", ValueType::String, false),
        ]
    }

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, title TEXT)")
            .unwrap();
        db.exec("CREATE TABLE comments (id INTEGER PRIMARY KEY, issueID INTEGER, body TEXT)")
            .unwrap();
        db.exec("CREATE TABLE reactions (id INTEGER PRIMARY KEY, commentID INTEGER, emoji TEXT)")
            .unwrap();
        db.exec(
            "INSERT INTO issue (id, title) VALUES (1, 'match me'), (2, 'skip me'), (3, 'later')",
        )
        .unwrap();
        db.exec(
            "INSERT INTO comments (id, issueID, body) VALUES \
             (10, 1, 'first matching child'), \
             (11, 1, 'second matching child'), \
             (12, 2, 'child for skipped parent'), \
             (13, 99, 'orphan child')",
        )
        .unwrap();
        db.exec(
            "INSERT INTO reactions (id, commentID, emoji) VALUES \
             (100, 10, 'thumbs-up'), \
             (101, 11, 'heart'), \
             (102, 12, 'skip-child-reaction'), \
             (103, 999, 'orphan-reaction')",
        )
        .unwrap();
        db.exec(
            "CREATE TABLE locale_issue (tenantID INTEGER, issueID INTEGER, title TEXT, PRIMARY KEY (tenantID, issueID))",
        )
        .unwrap();
        db.exec(
            "CREATE TABLE locale_comment (id INTEGER PRIMARY KEY, tenantID INTEGER, issueID INTEGER, body TEXT)",
        )
        .unwrap();
        db.exec(
            "INSERT INTO locale_issue (tenantID, issueID, title) VALUES \
             (1, 1, 'tenant one issue one'), \
             (1, 2, 'tenant one issue two'), \
             (2, 1, 'tenant two issue one')",
        )
        .unwrap();
        db.exec(
            "INSERT INTO locale_comment (id, tenantID, issueID, body) VALUES \
             (20, 1, 1, 'compound child match'), \
             (21, 1, 2, 'other issue same tenant'), \
             (22, 2, 1, 'same issue different tenant')",
        )
        .unwrap();
        db
    }

    fn issue_catalog() -> Vec<AnalyzeQueryTable> {
        vec![AnalyzeQueryTable {
            table_name: "issue".to_string(),
            primary_key: vec!["id".to_string()],
            columns: issue_columns(),
        }]
    }

    fn issue_and_comments_catalog() -> Vec<AnalyzeQueryTable> {
        vec![
            AnalyzeQueryTable {
                table_name: "issue".to_string(),
                primary_key: vec!["id".to_string()],
                columns: issue_columns(),
            },
            AnalyzeQueryTable {
                table_name: "comments".to_string(),
                primary_key: vec!["id".to_string()],
                columns: comment_columns(),
            },
            AnalyzeQueryTable {
                table_name: "reactions".to_string(),
                primary_key: vec!["id".to_string()],
                columns: reaction_columns(),
            },
        ]
    }

    fn locale_catalog() -> Vec<AnalyzeQueryTable> {
        vec![
            AnalyzeQueryTable {
                table_name: "locale_issue".to_string(),
                primary_key: vec!["tenantID".to_string(), "issueID".to_string()],
                columns: locale_issue_columns(),
            },
            AnalyzeQueryTable {
                table_name: "locale_comment".to_string(),
                primary_key: vec!["id".to_string()],
                columns: locale_comment_columns(),
            },
        ]
    }

    #[test]
    fn analyzes_a_filtered_single_table_query_against_sqlite() {
        let db = setup();
        let ast = Ast {
            table: "issue".to_string(),
            where_: Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "title".to_string(),
                }),
                right: ValuePosition::Literal(LiteralValue::String("match me".to_string())),
            }),
            ..Default::default()
        };

        let result = analyze_sqlite_ast_query(
            &db,
            &issue_catalog(),
            Some(&ast),
            None,
            Some(&AnalyzeQueryOptions {
                vended_rows: Some(true),
                synced_rows: None,
                join_plans: None,
            }),
            None,
            &[],
            &[],
        )
        .unwrap();

        assert_eq!(result.read_row_count, Some(1.0));
        assert_eq!(
            result.read_rows.as_ref().unwrap()[0].1[0].1[0],
            vec![
                ("id".to_string(), JsonValue::Number(1.0)),
                (
                    "title".to_string(),
                    JsonValue::String("match me".to_string())
                ),
            ]
        );
        assert!(result.sqlite_plans.as_ref().unwrap()[0].1[0].contains("issue"));
    }

    #[test]
    fn applies_limit_and_start_cursor_to_sqlite_read() {
        let db = setup();
        let ast = Ast {
            table: "issue".to_string(),
            order_by: Some(vec![("id".to_string(), Direction::Asc)]),
            start: Some(Bound {
                row: JsonValue::Object(vec![("id".to_string(), JsonValue::Number(1.0))]),
                exclusive: true,
            }),
            limit: Some(1.0),
            ..Default::default()
        };

        let result = analyze_sqlite_ast_query(
            &db,
            &issue_catalog(),
            Some(&ast),
            None,
            Some(&AnalyzeQueryOptions {
                vended_rows: Some(true),
                synced_rows: None,
                join_plans: None,
            }),
            None,
            &[],
            &[],
        )
        .unwrap();

        assert_eq!(result.warnings, Vec::<String>::new());
        assert_eq!(result.read_row_count, Some(1.0));
        assert_eq!(
            result.read_rows.as_ref().unwrap()[0].1[0].1[0],
            vec![
                ("id".to_string(), JsonValue::Number(2.0)),
                (
                    "title".to_string(),
                    JsonValue::String("skip me".to_string())
                ),
            ]
        );
        assert!(
            result.sqlite_plans.as_ref().unwrap()[0]
                .0
                .contains("LIMIT ?"),
            "query should be parameterized with a LIMIT"
        );
    }

    #[test]
    fn reports_db_scans_from_sqlite_plan_details() {
        let db = setup();
        let result = analyze_sqlite_ast_query(
            &db,
            &issue_catalog(),
            Some(&Ast::table("issue")),
            None,
            None,
            None,
            &[],
            &[],
        )
        .unwrap();

        let scans = result.db_scans_by_query.as_ref().unwrap();
        assert_eq!(scans.len(), 1);
        assert_eq!(scans[0].0, "issue");
        assert_eq!(scans[0].1.len(), 1);
        assert_eq!(scans[0].1[0].1, 3.0);
        assert!(
            result.sqlite_plans.as_ref().unwrap()[0].1[0].contains("SCAN"),
            "test should exercise the SCAN branch"
        );
    }

    #[test]
    fn indexed_search_plan_does_not_report_db_scan() {
        let db = setup();
        let ast = Ast {
            table: "issue".to_string(),
            where_: Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "id".to_string(),
                }),
                right: ValuePosition::Literal(LiteralValue::Number(1.0)),
            }),
            ..Default::default()
        };

        let result = analyze_sqlite_ast_query(
            &db,
            &issue_catalog(),
            Some(&ast),
            None,
            None,
            None,
            &[],
            &[],
        )
        .unwrap();

        assert!(
            result.sqlite_plans.as_ref().unwrap()[0]
                .1
                .iter()
                .any(|line| line.contains("SEARCH")),
            "test should exercise an indexed SEARCH plan"
        );
        assert!(result.db_scans_by_query.as_ref().unwrap()[0].1.is_empty());
    }

    #[test]
    fn returns_synced_rows_from_matching_row_records_and_bodies() {
        let db = setup();
        let ast = Ast::table("issue");
        let query_id = hash_of_ast(&ast);
        let row_id = RowId {
            schema: "public".to_string(),
            table: "issue".to_string(),
            row_key: std::collections::BTreeMap::from([("id".to_string(), JsonValue::Number(1.0))]),
        };
        let row_records = vec![RowRecord {
            base: zero_cache_view_syncer::cvr_types::CvrRecordBase {
                patch_version: zero_cache_view_syncer::cvr_version::empty_cvr_version(),
            },
            id: row_id.clone(),
            row_version: "v1".to_string(),
            ref_counts: Some(std::collections::BTreeMap::from([(query_id.clone(), 1)])),
        }];
        let row_bodies = vec![(
            row_id,
            vec![
                ("id".to_string(), JsonValue::Number(1.0)),
                (
                    "title".to_string(),
                    JsonValue::String("match me".to_string()),
                ),
            ],
        )];

        let result = analyze_sqlite_ast_query(
            &db,
            &issue_catalog(),
            Some(&ast),
            None,
            Some(&AnalyzeQueryOptions {
                vended_rows: None,
                synced_rows: Some(true),
                join_plans: None,
            }),
            None,
            &row_records,
            &row_bodies,
        )
        .unwrap();

        assert_eq!(result.synced_row_count, 1.0);
        assert_eq!(
            result.synced_rows.as_ref().unwrap()[0].1[0],
            vec![
                ("id".to_string(), JsonValue::Number(1.0)),
                (
                    "title".to_string(),
                    JsonValue::String("match me".to_string())
                ),
            ]
        );
        assert_eq!(result.warnings, Vec::<String>::new());
    }

    #[test]
    fn analyzes_top_level_related_query_against_sqlite() {
        let db = setup();
        let ast = Ast {
            table: "issue".to_string(),
            where_: Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "title".to_string(),
                }),
                right: ValuePosition::Literal(LiteralValue::String("match me".to_string())),
            }),
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".to_string()],
                    child_field: vec!["issueID".to_string()],
                },
                subquery: Box::new(Ast {
                    table: "comments".to_string(),
                    order_by: Some(vec![("id".to_string(), Direction::Asc)]),
                    ..Default::default()
                }),
                system: None,
                hidden: None,
            }]),
            ..Default::default()
        };

        let result = analyze_sqlite_ast_query(
            &db,
            &issue_and_comments_catalog(),
            Some(&ast),
            None,
            Some(&AnalyzeQueryOptions {
                vended_rows: Some(true),
                synced_rows: None,
                join_plans: None,
            }),
            None,
            &[],
            &[],
        )
        .unwrap();

        assert_eq!(result.read_row_count, Some(3.0));
        let counts = result.read_row_counts_by_query.as_ref().unwrap();
        assert_eq!(counts[0].0, "issue");
        assert_eq!(counts[0].1[0].1, 1.0);
        assert_eq!(counts[1].0, "comments");
        assert_eq!(counts[1].1[0].1, 2.0);
        let read_rows = result.read_rows.as_ref().unwrap();
        let child_rows = &read_rows[1].1[0].1;
        assert_eq!(child_rows.len(), 2);
        assert!(format!("{child_rows:?}").contains("first matching child"));
        assert!(format!("{child_rows:?}").contains("second matching child"));
        assert!(!format!("{child_rows:?}").contains("orphan child"));
        assert_eq!(result.sqlite_plans.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn related_query_with_no_parent_rows_does_not_read_all_children() {
        let db = setup();
        let ast = Ast {
            table: "issue".to_string(),
            where_: Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "title".to_string(),
                }),
                right: ValuePosition::Literal(LiteralValue::String("missing".to_string())),
            }),
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".to_string()],
                    child_field: vec!["issueID".to_string()],
                },
                subquery: Box::new(Ast::table("comments")),
                system: None,
                hidden: None,
            }]),
            ..Default::default()
        };

        let result = analyze_sqlite_ast_query(
            &db,
            &issue_and_comments_catalog(),
            Some(&ast),
            None,
            Some(&AnalyzeQueryOptions {
                vended_rows: Some(true),
                synced_rows: None,
                join_plans: None,
            }),
            None,
            &[],
            &[],
        )
        .unwrap();

        assert_eq!(result.read_row_count, Some(0.0));
        let counts = result.read_row_counts_by_query.as_ref().unwrap();
        assert_eq!(counts[0].1[0].1, 0.0);
        assert_eq!(counts[1].1[0].1, 0.0);
        assert!(
            result.sqlite_plans.as_ref().unwrap()[1]
                .0
                .contains("WHERE FALSE"),
            "empty parent sets should compile to an empty child read"
        );
        assert!(result.read_rows.as_ref().unwrap()[1].1[0].1.is_empty());
    }

    #[test]
    fn analyzes_related_query_with_compound_correlation() {
        let db = setup();
        let ast = Ast {
            table: "locale_issue".to_string(),
            where_: Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "title".to_string(),
                }),
                right: ValuePosition::Literal(LiteralValue::String(
                    "tenant one issue one".to_string(),
                )),
            }),
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["tenantID".to_string(), "issueID".to_string()],
                    child_field: vec!["tenantID".to_string(), "issueID".to_string()],
                },
                subquery: Box::new(Ast::table("locale_comment")),
                system: None,
                hidden: None,
            }]),
            ..Default::default()
        };

        let result = analyze_sqlite_ast_query(
            &db,
            &locale_catalog(),
            Some(&ast),
            None,
            Some(&AnalyzeQueryOptions {
                vended_rows: Some(true),
                synced_rows: None,
                join_plans: None,
            }),
            None,
            &[],
            &[],
        )
        .unwrap();

        assert_eq!(result.read_row_count, Some(2.0));
        let child_query = &result.sqlite_plans.as_ref().unwrap()[1].0;
        assert!(
            child_query.contains("(\"tenantID\",\"issueID\") IN (VALUES (?,?))"),
            "compound correlation should use tuple multi-constraints: {child_query}"
        );
        let child_rows = &result.read_rows.as_ref().unwrap()[1].1[0].1;
        assert_eq!(child_rows.len(), 1);
        assert!(format!("{child_rows:?}").contains("compound child match"));
        assert!(!format!("{child_rows:?}").contains("same issue different tenant"));
    }

    #[test]
    fn analyzes_nested_related_queries_against_sqlite() {
        let db = setup();
        let ast = Ast {
            table: "issue".to_string(),
            where_: Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "title".to_string(),
                }),
                right: ValuePosition::Literal(LiteralValue::String("match me".to_string())),
            }),
            related: Some(vec![CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".to_string()],
                    child_field: vec!["issueID".to_string()],
                },
                subquery: Box::new(Ast {
                    table: "comments".to_string(),
                    related: Some(vec![CorrelatedSubquery {
                        correlation: Correlation {
                            parent_field: vec!["id".to_string()],
                            child_field: vec!["commentID".to_string()],
                        },
                        subquery: Box::new(Ast::table("reactions")),
                        system: None,
                        hidden: None,
                    }]),
                    ..Default::default()
                }),
                system: None,
                hidden: None,
            }]),
            ..Default::default()
        };

        let result = analyze_sqlite_ast_query(
            &db,
            &issue_and_comments_catalog(),
            Some(&ast),
            None,
            Some(&AnalyzeQueryOptions {
                vended_rows: Some(true),
                synced_rows: None,
                join_plans: None,
            }),
            None,
            &[],
            &[],
        )
        .unwrap();

        assert_eq!(result.read_row_count, Some(5.0));
        let counts = result.read_row_counts_by_query.as_ref().unwrap();
        assert_eq!(counts[0].0, "issue");
        assert_eq!(counts[0].1[0].1, 1.0);
        assert_eq!(counts[1].0, "comments");
        assert_eq!(counts[1].1[0].1, 2.0);
        assert_eq!(counts[2].0, "reactions");
        assert_eq!(counts[2].1[0].1, 2.0);
        let read_rows = result.read_rows.as_ref().unwrap();
        let reaction_rows = &read_rows[2].1[0].1;
        assert_eq!(reaction_rows.len(), 2);
        assert!(format!("{reaction_rows:?}").contains("thumbs-up"));
        assert!(format!("{reaction_rows:?}").contains("heart"));
        assert!(!format!("{reaction_rows:?}").contains("skip-child-reaction"));
        assert!(!format!("{reaction_rows:?}").contains("orphan-reaction"));
        assert_eq!(result.sqlite_plans.as_ref().unwrap().len(), 3);
    }

    #[test]
    fn analyzes_correlated_subquery_exists_condition() {
        let db = setup();
        let ast = Ast {
            table: "issue".to_string(),
            where_: Some(Condition::CorrelatedSubquery {
                related: CorrelatedSubquery {
                    correlation: Correlation {
                        parent_field: vec!["id".to_string()],
                        child_field: vec!["issueID".to_string()],
                    },
                    subquery: Box::new(Ast::table("comments")),
                    system: None,
                    hidden: None,
                },
                op: ExistsOp::Exists,
                flip: None,
                scalar: None,
                plan_id: None,
            }),
            order_by: Some(vec![("id".to_string(), Direction::Asc)]),
            ..Default::default()
        };

        let result = analyze_sqlite_ast_query(
            &db,
            &issue_and_comments_catalog(),
            Some(&ast),
            None,
            Some(&AnalyzeQueryOptions {
                vended_rows: Some(true),
                synced_rows: None,
                join_plans: None,
            }),
            None,
            &[],
            &[],
        )
        .unwrap();

        assert_eq!(result.read_row_count, Some(2.0));
        let rows = &result.read_rows.as_ref().unwrap()[0].1[0].1;
        assert_eq!(rows.len(), 2);
        assert!(format!("{rows:?}").contains("match me"));
        assert!(format!("{rows:?}").contains("skip me"));
        assert!(!format!("{rows:?}").contains("later"));
    }

    #[test]
    fn analyzes_correlated_subquery_not_exists_condition() {
        let db = setup();
        let ast = Ast {
            table: "issue".to_string(),
            where_: Some(Condition::CorrelatedSubquery {
                related: CorrelatedSubquery {
                    correlation: Correlation {
                        parent_field: vec!["id".to_string()],
                        child_field: vec!["issueID".to_string()],
                    },
                    subquery: Box::new(Ast::table("comments")),
                    system: None,
                    hidden: None,
                },
                op: ExistsOp::NotExists,
                flip: None,
                scalar: None,
                plan_id: None,
            }),
            order_by: Some(vec![("id".to_string(), Direction::Asc)]),
            ..Default::default()
        };

        let result = analyze_sqlite_ast_query(
            &db,
            &issue_and_comments_catalog(),
            Some(&ast),
            None,
            Some(&AnalyzeQueryOptions {
                vended_rows: Some(true),
                synced_rows: None,
                join_plans: None,
            }),
            None,
            &[],
            &[],
        )
        .unwrap();

        assert_eq!(result.read_row_count, Some(1.0));
        let rows = &result.read_rows.as_ref().unwrap()[0].1[0].1;
        assert_eq!(rows.len(), 1);
        assert!(format!("{rows:?}").contains("later"));
    }

    #[test]
    fn rejects_unknown_tables_instead_of_guessing_schema() {
        let db = setup();
        let err = analyze_sqlite_ast_query(
            &db,
            &[],
            Some(&Ast::table("missing")),
            None,
            None,
            None,
            &[],
            &[],
        )
        .unwrap_err();
        assert!(err.to_string().contains("not in this handler's catalog"));
    }
}
