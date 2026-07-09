//! Wire encoder for [`crate::inspect_down`].

use zero_cache_shared::bigint_json::{stringify as json_value_stringify, JsonValue};

use crate::analyze_query_result::analyze_query_result_json;
use crate::ast_json::ast_to_json;
use crate::inspect_down::{InspectDownBody, InspectQueryRow, ServerMetrics};

fn json_string(s: &str) -> String {
    json_value_stringify(&JsonValue::String(s.to_string()))
}

fn tdigest_json(values: &[f64]) -> String {
    let items: Vec<String> = values.iter().map(|v| v.to_string()).collect();
    format!("[{}]", items.join(","))
}

fn server_metrics_json(metrics: &ServerMetrics) -> String {
    format!(
        "{{\"query-materialization-server\":{},\"query-update-server\":{}}}",
        tdigest_json(&metrics.query_materialization_server),
        tdigest_json(&metrics.query_update_server)
    )
}

fn nullable_string_json(s: &Option<String>) -> String {
    match s {
        Some(s) => json_string(s),
        None => "null".to_string(),
    }
}

fn nullable_args_json(args: &Option<Vec<JsonValue>>) -> String {
    match args {
        Some(args) => json_value_stringify(&JsonValue::Array(args.clone())),
        None => "null".to_string(),
    }
}

fn nullable_ast_json(ast: &Option<crate::ast::Ast>) -> String {
    match ast {
        Some(ast) => json_value_stringify(&ast_to_json(ast)),
        None => "null".to_string(),
    }
}

fn inspect_query_row_json(row: &InspectQueryRow) -> String {
    let mut fields = vec![
        format!("\"clientID\":{}", json_string(&row.client_id)),
        format!("\"queryID\":{}", json_string(&row.query_id)),
        format!("\"ast\":{}", nullable_ast_json(&row.ast)),
        format!("\"name\":{}", nullable_string_json(&row.name)),
        format!("\"args\":{}", nullable_args_json(&row.args)),
        format!("\"got\":{}", row.got),
        format!("\"deleted\":{}", row.deleted),
        format!("\"ttl\":{}", row.ttl),
        format!(
            "\"inactivatedAt\":{}",
            row.inactivated_at
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        format!("\"rowCount\":{}", row.row_count),
    ];
    if let Some(metrics) = &row.metrics {
        fields.push(format!("\"metrics\":{}", json_value_stringify(metrics)));
    }
    format!("{{{}}}", fields.join(","))
}

/// Encodes a full `["inspect", body]` downstream inspector response frame.
pub fn inspect_down_message_json(body: &InspectDownBody) -> String {
    let body_json = match body {
        InspectDownBody::Queries { id, value } => {
            let rows: Vec<String> = value.iter().map(inspect_query_row_json).collect();
            format!(
                "{{\"id\":{},\"op\":\"queries\",\"value\":[{}]}}",
                json_string(id),
                rows.join(",")
            )
        }
        InspectDownBody::Metrics { id, value } => format!(
            "{{\"id\":{},\"op\":\"metrics\",\"value\":{}}}",
            json_string(id),
            server_metrics_json(value)
        ),
        InspectDownBody::Version { id, value } => format!(
            "{{\"id\":{},\"op\":\"version\",\"value\":{}}}",
            json_string(id),
            json_string(value)
        ),
        InspectDownBody::Authenticated { id, value } => format!(
            "{{\"id\":{},\"op\":\"authenticated\",\"value\":{}}}",
            json_string(id),
            value
        ),
        InspectDownBody::AnalyzeQuery { id, value } => format!(
            "{{\"id\":{},\"op\":\"analyze-query\",\"value\":{}}}",
            json_string(id),
            json_value_stringify(&analyze_query_result_json(value))
        ),
        InspectDownBody::Error { id, value } => format!(
            "{{\"id\":{},\"op\":\"error\",\"value\":{}}}",
            json_string(id),
            json_string(value)
        ),
    };
    format!("[\"inspect\",{}]", body_json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Ast;
    use zero_cache_shared::bigint_json::parse;

    #[test]
    fn encodes_version_authenticated_and_error() {
        assert_eq!(
            inspect_down_message_json(&InspectDownBody::Version {
                id: "v1".into(),
                value: "1.2.3".into(),
            }),
            r#"["inspect",{"id":"v1","op":"version","value":"1.2.3"}]"#
        );
        assert_eq!(
            inspect_down_message_json(&InspectDownBody::Authenticated {
                id: "a1".into(),
                value: true,
            }),
            r#"["inspect",{"id":"a1","op":"authenticated","value":true}]"#
        );
        assert_eq!(
            inspect_down_message_json(&InspectDownBody::Error {
                id: "e1".into(),
                value: "oops".into(),
            }),
            r#"["inspect",{"id":"e1","op":"error","value":"oops"}]"#
        );
    }

    #[test]
    fn encodes_metrics() {
        let json = inspect_down_message_json(&InspectDownBody::Metrics {
            id: "m1".into(),
            value: ServerMetrics {
                query_materialization_server: vec![1000.0, 2.0, 1.0],
                query_update_server: vec![1000.0],
            },
        });
        assert_eq!(
            json,
            r#"["inspect",{"id":"m1","op":"metrics","value":{"query-materialization-server":[1000,2,1],"query-update-server":[1000]}}]"#
        );
    }

    #[test]
    fn encodes_queries_with_nullable_and_optional_fields() {
        let row = InspectQueryRow {
            client_id: "c1".into(),
            query_id: "q1".into(),
            ast: Some(Ast::table("issue")),
            name: None,
            args: Some(vec![JsonValue::Number(1.0)]),
            got: true,
            deleted: false,
            ttl: 60.0,
            inactivated_at: None,
            row_count: 2.0,
            metrics: Some(parse(r#"{"query-hydration-server-ms":3}"#).unwrap()),
        };
        let json = inspect_down_message_json(&InspectDownBody::Queries {
            id: "i1".into(),
            value: vec![row],
        });
        assert_eq!(
            json,
            r#"["inspect",{"id":"i1","op":"queries","value":[{"clientID":"c1","queryID":"q1","ast":{"table":"issue"},"name":null,"args":[1],"got":true,"deleted":false,"ttl":60,"inactivatedAt":null,"rowCount":2,"metrics":{"query-hydration-server-ms":3}}]}]"#
        );
    }

    #[test]
    fn encodes_analyze_query_as_raw_json() {
        let value = crate::analyze_query_result::AnalyzeQueryResult {
            warnings: vec![],
            synced_rows: None,
            synced_row_count: 0.0,
            start: 1.0,
            end: 2.0,
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
        let json = inspect_down_message_json(&InspectDownBody::AnalyzeQuery {
            id: "aq".into(),
            value,
        });
        assert_eq!(
            json,
            r#"["inspect",{"id":"aq","op":"analyze-query","value":{"warnings":[],"syncedRowCount":0,"start":1,"end":2}}]"#
        );
    }
}
