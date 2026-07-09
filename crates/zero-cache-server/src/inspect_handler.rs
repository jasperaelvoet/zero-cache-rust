//! Pure core of `zero-cache/src/services/view-syncer/inspect-handler.ts`.
//!
//! The upstream handler sends responses through `ClientHandler` and reaches
//! into `CVRStore`, config, permissions, and the query analyzer. This module
//! ports the control flow that is already backed by Rust state: auth gating,
//! authenticate/version/metrics responses, and `queries` rows enriched with
//! `InspectorDelegate` AST and metrics data. The `analyze-query` execution path
//! remains an explicit error until the analyzer stack is ported.

use zero_cache_protocol::inspect_down::InspectDownBody;
use zero_cache_protocol::inspect_up::InspectUpBody;
use zero_cache_view_syncer::cvr_inspect::inspect_queries_from_cvr;
use zero_cache_view_syncer::cvr_types::{Cvr, RowRecord};

use crate::inspector_delegate::{metrics_for_protocol, InspectorDelegate};

/// Handles one inspector request and returns the downstream response body.
///
/// `is_admin_password_valid` corresponds to upstream `isAdminPasswordValid`;
/// `is_development_mode` is passed through to [`InspectorDelegate::is_authenticated`].
pub fn handle_inspect<F>(
    body: InspectUpBody,
    cvr: &Cvr,
    row_records: &[RowRecord],
    inspector_delegate: &mut InspectorDelegate,
    client_group_id: &str,
    protocol_version: u32,
    server_version: &str,
    is_development_mode: bool,
    is_admin_password_valid: F,
) -> InspectDownBody
where
    F: FnOnce(&str) -> bool,
{
    if !matches!(body, InspectUpBody::Authenticate { .. })
        && !inspector_delegate.is_authenticated(client_group_id, is_development_mode)
    {
        return InspectDownBody::Authenticated {
            id: body.id().to_string(),
            value: false,
        };
    }

    match body {
        InspectUpBody::Queries { id, client_id } => {
            let mut rows = inspect_queries_from_cvr(cvr, row_records, client_id.as_deref());
            for row in &mut rows {
                if row.ast.is_none() {
                    row.ast = inspector_delegate.ast_for_query(&row.query_id).cloned();
                }
                row.metrics = metrics_for_protocol(
                    inspector_delegate.metrics_json_for_query(&row.query_id),
                    protocol_version,
                );
            }
            InspectDownBody::Queries { id, value: rows }
        }
        InspectUpBody::Metrics { id } => InspectDownBody::Metrics {
            id,
            value: inspector_delegate.metrics_json(),
        },
        InspectUpBody::Version { id } => InspectDownBody::Version {
            id,
            value: server_version.to_string(),
        },
        InspectUpBody::Authenticate { id, value } => {
            let ok = is_admin_password_valid(&value);
            if ok {
                inspector_delegate.set_authenticated(client_group_id);
            } else {
                inspector_delegate.clear_authenticated(client_group_id);
            }
            InspectDownBody::Authenticated { id, value: ok }
        }
        InspectUpBody::AnalyzeQuery { id, .. } => InspectDownBody::Error {
            id,
            value: "analyze-query is not yet ported".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use zero_cache_protocol::ast::Ast;
    use zero_cache_protocol::inspect_down::InspectQueryRow;
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_view_syncer::cvr_types::{
        ClientQueryState, ClientRecord, CustomQueryRecord, Cvr, CvrRecordBase, ExternalQueryBase,
        RowId,
    };
    use zero_cache_view_syncer::cvr_version::empty_cvr_version;
    use zero_cache_zql::ttl::DEFAULT_TTL_MS;

    use crate::inspector_delegate::ServerMetric;

    fn empty_cvr() -> Cvr {
        Cvr {
            id: "cg1".to_string(),
            version: empty_cvr_version(),
            last_active: 0.0,
            ttl_clock: zero_cache_view_syncer::cvr_types::TtlClock::from_number(0.0),
            replica_version: None,
            clients: BTreeMap::from([(
                "client1".to_string(),
                ClientRecord {
                    id: "client1".to_string(),
                    desired_query_ids: vec!["custom".to_string()],
                },
            )]),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        }
    }

    fn custom_query_cvr() -> Cvr {
        let mut cvr = empty_cvr();
        let mut query = CustomQueryRecord {
            base: ExternalQueryBase {
                id: "custom".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
                client_state: BTreeMap::new(),
                patch_version: Some(empty_cvr_version()),
            },
            name: "named".to_string(),
            args: vec![JsonValue::String("arg".to_string())],
        };
        query.base.client_state.insert(
            "client1".to_string(),
            ClientQueryState {
                inactivated_at: None,
                ttl: DEFAULT_TTL_MS,
                deleted: false,
                version: empty_cvr_version(),
            },
        );
        cvr.queries.insert(
            "custom".to_string(),
            zero_cache_view_syncer::cvr_types::QueryRecord::Custom(query),
        );
        cvr
    }

    fn row_record(query_id: &str) -> RowRecord {
        RowRecord {
            base: CvrRecordBase {
                patch_version: empty_cvr_version(),
            },
            id: RowId {
                schema: "public".to_string(),
                table: "issues".to_string(),
                row_key: BTreeMap::from([("id".to_string(), JsonValue::String("1".to_string()))]),
            },
            row_version: "01".to_string(),
            ref_counts: Some(BTreeMap::from([(query_id.to_string(), 1)])),
        }
    }

    fn assert_authenticated(body: InspectDownBody, expected: bool) {
        assert_eq!(
            body,
            InspectDownBody::Authenticated {
                id: "i1".to_string(),
                value: expected
            }
        );
    }

    #[test]
    fn unauthenticated_non_auth_request_gets_challenge() {
        let cvr = empty_cvr();
        let mut delegate = InspectorDelegate::new();
        let group = "inspect_handler_unauthenticated";
        delegate.clear_authenticated(group);

        let body = handle_inspect(
            InspectUpBody::Metrics {
                id: "i1".to_string(),
            },
            &cvr,
            &[],
            &mut delegate,
            group,
            51,
            "1.2.3",
            false,
            |_| true,
        );

        assert_authenticated(body, false);
    }

    #[test]
    fn authenticate_sets_and_clears_shared_auth_state() {
        let cvr = empty_cvr();
        let mut delegate = InspectorDelegate::new();
        let group = "inspect_handler_authenticate";
        delegate.clear_authenticated(group);

        let ok = handle_inspect(
            InspectUpBody::Authenticate {
                id: "i1".to_string(),
                value: "secret".to_string(),
            },
            &cvr,
            &[],
            &mut delegate,
            group,
            51,
            "1.2.3",
            false,
            |password| password == "secret",
        );
        assert_authenticated(ok, true);
        assert!(delegate.is_authenticated(group, false));

        let denied = handle_inspect(
            InspectUpBody::Authenticate {
                id: "i1".to_string(),
                value: "bad".to_string(),
            },
            &cvr,
            &[],
            &mut delegate,
            group,
            51,
            "1.2.3",
            false,
            |_| false,
        );
        assert_authenticated(denied, false);
        assert!(!delegate.is_authenticated(group, false));
    }

    #[test]
    fn development_mode_allows_version_without_auth() {
        let cvr = empty_cvr();
        let mut delegate = InspectorDelegate::new();

        let body = handle_inspect(
            InspectUpBody::Version {
                id: "v1".to_string(),
            },
            &cvr,
            &[],
            &mut delegate,
            "inspect_handler_dev",
            51,
            "9.8.7",
            true,
            |_| false,
        );

        assert_eq!(
            body,
            InspectDownBody::Version {
                id: "v1".to_string(),
                value: "9.8.7".to_string()
            }
        );
    }

    #[test]
    fn queries_are_projected_and_enriched_with_delegate_ast_and_metrics() {
        let cvr = custom_query_cvr();
        let mut delegate = InspectorDelegate::new();
        let group = "inspect_handler_queries";
        delegate.set_authenticated(group);
        delegate.add_query("custom", Ast::table("server_generated"));
        delegate.add_metric(ServerMetric::QueryMaterializationServer, 12.0, "custom");
        delegate.add_metric(ServerMetric::QueryUpdateServer, 4.0, "custom");

        let body = handle_inspect(
            InspectUpBody::Queries {
                id: "q1".to_string(),
                client_id: None,
            },
            &cvr,
            &[row_record("custom")],
            &mut delegate,
            group,
            51,
            "1.2.3",
            false,
            |_| false,
        );

        let InspectDownBody::Queries { id, value } = body else {
            panic!("expected queries response")
        };
        assert_eq!(id, "q1");
        assert_eq!(value.len(), 1);
        assert_eq!(
            value[0],
            InspectQueryRow {
                client_id: "client1".to_string(),
                query_id: "custom".to_string(),
                ast: Some(Ast::table("server_generated")),
                name: Some("named".to_string()),
                args: Some(vec![JsonValue::String("arg".to_string())]),
                got: true,
                deleted: false,
                ttl: DEFAULT_TTL_MS,
                inactivated_at: None,
                row_count: 1.0,
                metrics: value[0].metrics.clone(),
            }
        );

        let JsonValue::Object(fields) = value[0].metrics.clone().unwrap() else {
            panic!("expected metrics object")
        };
        assert!(fields
            .iter()
            .any(|(k, v)| k == "query-hydration-server-ms" && *v == JsonValue::Number(12.0)));
        assert!(fields.iter().any(|(k, _)| k == "query-update-server"));
    }

    #[test]
    fn queries_use_legacy_metrics_for_old_protocols() {
        let cvr = custom_query_cvr();
        let mut delegate = InspectorDelegate::new();
        let group = "inspect_handler_legacy_metrics";
        delegate.set_authenticated(group);
        delegate.add_metric(ServerMetric::QueryMaterializationServer, 12.0, "custom");

        let body = handle_inspect(
            InspectUpBody::Queries {
                id: "q1".to_string(),
                client_id: Some("client1".to_string()),
            },
            &cvr,
            &[],
            &mut delegate,
            group,
            50,
            "1.2.3",
            false,
            |_| false,
        );

        let InspectDownBody::Queries { value, .. } = body else {
            panic!("expected queries response")
        };
        let JsonValue::Object(fields) = value[0].metrics.clone().unwrap() else {
            panic!("expected metrics object")
        };
        assert!(fields
            .iter()
            .any(|(k, _)| k == "query-materialization-server"));
        assert!(!fields.iter().any(|(k, _)| k == "query-hydration-server-ms"));
    }

    #[test]
    fn metrics_response_uses_delegate_globals() {
        let cvr = empty_cvr();
        let mut delegate = InspectorDelegate::new();
        let group = "inspect_handler_metrics";
        delegate.set_authenticated(group);
        delegate.add_metric(ServerMetric::QueryUpdateServer, 4.0, "q1");

        let body = handle_inspect(
            InspectUpBody::Metrics {
                id: "m1".to_string(),
            },
            &cvr,
            &[],
            &mut delegate,
            group,
            51,
            "1.2.3",
            false,
            |_| false,
        );

        let InspectDownBody::Metrics { id, value } = body else {
            panic!("expected metrics response")
        };
        assert_eq!(id, "m1");
        assert_eq!(value.query_update_server, vec![1000.0, 4.0, 1.0]);
    }

    #[test]
    fn analyze_query_returns_explicit_unported_error_after_auth() {
        let cvr = empty_cvr();
        let mut delegate = InspectorDelegate::new();
        let group = "inspect_handler_analyze";
        delegate.set_authenticated(group);

        let body = handle_inspect(
            InspectUpBody::AnalyzeQuery {
                id: "a1".to_string(),
                value: Some(Ast::table("issues")),
                options: None,
                ast: None,
                name: None,
                args: None,
            },
            &cvr,
            &[],
            &mut delegate,
            group,
            51,
            "1.2.3",
            false,
            |_| false,
        );

        assert_eq!(
            body,
            InspectDownBody::Error {
                id: "a1".to_string(),
                value: "analyze-query is not yet ported".to_string()
            }
        );
    }
}
