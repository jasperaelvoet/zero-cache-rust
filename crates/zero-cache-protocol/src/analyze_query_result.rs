//! Port of `zero-protocol/src/analyze-query-result.ts`.
//!
//! This models the stable inspector analyze-query result envelope. Planner
//! debug event internals are intentionally raw JSON for now; the planner and
//! its diagnostics are a larger subsystem than this protocol wrapper.

use zero_cache_shared::bigint_json::JsonValue;

use crate::row_patch::Row;

pub type RowCountsByQuery = Vec<(String, f64)>;
pub type RowCountsBySource = Vec<(String, RowCountsByQuery)>;
pub type RowsByQuery = Vec<(String, Vec<Row>)>;
pub type RowsBySource = Vec<(String, RowsByQuery)>;
pub type SqlitePlans = Vec<(String, Vec<String>)>;
pub type PlanDebugEventJson = JsonValue;

#[derive(Debug, Clone, PartialEq)]
pub struct AnalyzeQueryResult {
    pub warnings: Vec<String>,
    pub synced_rows: Option<RowsByQuery>,
    pub synced_row_count: f64,
    pub start: f64,
    /// Deprecated upstream, but still present on the wire for compatibility.
    pub end: f64,
    pub elapsed: Option<f64>,
    pub after_permissions: Option<String>,
    /// Deprecated upstream in favor of `readRowCountsByQuery`.
    pub vended_row_counts: Option<RowCountsBySource>,
    /// Deprecated upstream in favor of `readRows`.
    pub vended_rows: Option<RowsBySource>,
    pub sqlite_plans: Option<SqlitePlans>,
    pub read_rows: Option<RowsBySource>,
    pub read_row_counts_by_query: Option<RowCountsBySource>,
    pub read_row_count: Option<f64>,
    pub db_scans_by_query: Option<RowCountsBySource>,
    pub join_plans: Option<Vec<PlanDebugEventJson>>,
}

fn object(entries: Vec<(String, JsonValue)>) -> JsonValue {
    JsonValue::Object(entries)
}

fn string_array(items: &[String]) -> JsonValue {
    JsonValue::Array(items.iter().cloned().map(JsonValue::String).collect())
}

fn row_json(row: &Row) -> JsonValue {
    JsonValue::Object(row.clone())
}

fn row_counts_by_query_json(value: &RowCountsByQuery) -> JsonValue {
    object(
        value
            .iter()
            .map(|(query, count)| (query.clone(), JsonValue::Number(*count)))
            .collect(),
    )
}

fn row_counts_by_source_json(value: &RowCountsBySource) -> JsonValue {
    object(
        value
            .iter()
            .map(|(source, counts)| (source.clone(), row_counts_by_query_json(counts)))
            .collect(),
    )
}

fn rows_by_query_json(value: &RowsByQuery) -> JsonValue {
    object(
        value
            .iter()
            .map(|(query, rows)| {
                (
                    query.clone(),
                    JsonValue::Array(rows.iter().map(row_json).collect()),
                )
            })
            .collect(),
    )
}

fn rows_by_source_json(value: &RowsBySource) -> JsonValue {
    object(
        value
            .iter()
            .map(|(source, rows)| (source.clone(), rows_by_query_json(rows)))
            .collect(),
    )
}

fn sqlite_plans_json(value: &SqlitePlans) -> JsonValue {
    object(
        value
            .iter()
            .map(|(query, plans)| (query.clone(), string_array(plans)))
            .collect(),
    )
}

pub fn analyze_query_result_json(result: &AnalyzeQueryResult) -> JsonValue {
    let mut fields = vec![
        ("warnings".to_string(), string_array(&result.warnings)),
        (
            "syncedRowCount".to_string(),
            JsonValue::Number(result.synced_row_count),
        ),
        ("start".to_string(), JsonValue::Number(result.start)),
        ("end".to_string(), JsonValue::Number(result.end)),
    ];

    if let Some(value) = &result.synced_rows {
        fields.push(("syncedRows".to_string(), rows_by_query_json(value)));
    }
    if let Some(value) = result.elapsed {
        fields.push(("elapsed".to_string(), JsonValue::Number(value)));
    }
    if let Some(value) = &result.after_permissions {
        fields.push((
            "afterPermissions".to_string(),
            JsonValue::String(value.clone()),
        ));
    }
    if let Some(value) = &result.vended_row_counts {
        fields.push((
            "vendedRowCounts".to_string(),
            row_counts_by_source_json(value),
        ));
    }
    if let Some(value) = &result.vended_rows {
        fields.push(("vendedRows".to_string(), rows_by_source_json(value)));
    }
    if let Some(value) = &result.sqlite_plans {
        fields.push(("sqlitePlans".to_string(), sqlite_plans_json(value)));
    }
    if let Some(value) = &result.read_rows {
        fields.push(("readRows".to_string(), rows_by_source_json(value)));
    }
    if let Some(value) = &result.read_row_counts_by_query {
        fields.push((
            "readRowCountsByQuery".to_string(),
            row_counts_by_source_json(value),
        ));
    }
    if let Some(value) = result.read_row_count {
        fields.push(("readRowCount".to_string(), JsonValue::Number(value)));
    }
    if let Some(value) = &result.db_scans_by_query {
        fields.push((
            "dbScansByQuery".to_string(),
            row_counts_by_source_json(value),
        ));
    }
    if let Some(value) = &result.join_plans {
        fields.push(("joinPlans".to_string(), JsonValue::Array(value.clone())));
    }

    JsonValue::Object(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(fields: &[(&str, JsonValue)]) -> Row {
        fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn minimal_result_serializes_required_fields() {
        let result = AnalyzeQueryResult {
            warnings: vec![],
            synced_rows: None,
            synced_row_count: 0.0,
            start: 1000.0,
            end: 1100.0,
            elapsed: None,
            after_permissions: None,
            vended_row_counts: None,
            vended_rows: None,
            sqlite_plans: None,
            read_rows: None,
            read_row_counts_by_query: None,
            read_row_count: None,
            db_scans_by_query: None,
            join_plans: None,
        };

        assert_eq!(
            analyze_query_result_json(&result).stringify(),
            r#"{"warnings":[],"syncedRowCount":0,"start":1000,"end":1100}"#
        );
    }

    #[test]
    fn complete_result_serializes_optional_fields() {
        let result = AnalyzeQueryResult {
            warnings: vec!["No auth data provided".to_string()],
            synced_rows: Some(vec![(
                "users".to_string(),
                vec![row(&[
                    ("id", JsonValue::Number(1.0)),
                    ("name", JsonValue::String("Alice".to_string())),
                ])],
            )]),
            synced_row_count: 2.0,
            start: 1000.0,
            end: 1150.0,
            elapsed: Some(150.0),
            after_permissions: Some("users.where('id', 1)".to_string()),
            vended_row_counts: Some(vec![(
                "users".to_string(),
                vec![("SELECT * FROM users WHERE id = ?".to_string(), 1.0)],
            )]),
            vended_rows: Some(vec![(
                "users".to_string(),
                vec![(
                    "SELECT * FROM users WHERE id = ?".to_string(),
                    vec![row(&[
                        ("id", JsonValue::Number(1.0)),
                        ("name", JsonValue::String("Alice".to_string())),
                    ])],
                )],
            )]),
            sqlite_plans: Some(vec![(
                "SELECT * FROM users WHERE id = ?".to_string(),
                vec!["SEARCH users USING INDEX idx_users_id (id=?)".to_string()],
            )]),
            read_rows: Some(vec![("users".to_string(), vec![])]),
            read_row_counts_by_query: Some(vec![("users".to_string(), vec![])]),
            read_row_count: Some(1.0),
            db_scans_by_query: Some(vec![("users".to_string(), vec![])]),
            join_plans: Some(vec![JsonValue::Object(vec![(
                "type".to_string(),
                JsonValue::String("attempt-start".to_string()),
            )])]),
        };

        let json = analyze_query_result_json(&result).stringify();
        assert!(json.contains(r#""warnings":["No auth data provided"]"#));
        assert!(json.contains(r#""syncedRows":{"users":[{"id":1,"name":"Alice"}]}"#));
        assert!(json.contains(r#""elapsed":150"#));
        assert!(json.contains(r#""readRowCount":1"#));
        assert!(json.contains(r#""joinPlans":[{"type":"attempt-start"}]"#));
    }
}
