//! Port of `zero-cache/src/server/inspector-delegate.ts`.
//!
//! This is the stateful half that complements the protocol-level
//! `inspect_*` modules: it records server-side query metrics, remembers the
//! AST associated with each query id, tracks inspector authentication by
//! client group, and exposes the custom-query transform hook that live
//! `analyze-query` uses.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, OnceLock};

use zero_cache_protocol::ast::Ast;
use zero_cache_protocol::inspect_down::ServerMetrics;
use zero_cache_protocol::query_hash::hash_of_name_and_args;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_shared::tdigest::TDigest;

type ClientGroupId = String;

static AUTHENTICATED_CLIENT_GROUP_IDS: OnceLock<Mutex<BTreeSet<ClientGroupId>>> = OnceLock::new();

fn authenticated_client_group_ids() -> &'static Mutex<BTreeSet<ClientGroupId>> {
    AUTHENTICATED_CLIENT_GROUP_IDS.get_or_init(|| Mutex::new(BTreeSet::new()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerMetric {
    QueryMaterializationServer,
    QueryUpdateServer,
}

#[derive(Default)]
pub struct InspectorDelegate {
    global_materialization: TDigest,
    global_update: TDigest,
    per_query_hydrate_ms: BTreeMap<String, f64>,
    per_query_update_metrics: BTreeMap<String, TDigest>,
    query_id_to_ast: BTreeMap<String, Ast>,
    custom_query_transforms: BTreeMap<String, Ast>,
}

impl InspectorDelegate {
    pub fn new() -> Self {
        Self::default()
    }

    /// Port of `addMetric`. `query_id` is the first variadic metric arg in
    /// the TS `MetricsDelegate` call site.
    pub fn add_metric(&mut self, metric: ServerMetric, value: f64, query_id: &str) {
        match metric {
            ServerMetric::QueryMaterializationServer => {
                // Last hydration value wins per query, matching the TS map.
                self.per_query_hydrate_ms
                    .insert(query_id.to_string(), value);
                self.global_materialization.add(value, 1.0);
            }
            ServerMetric::QueryUpdateServer => {
                self.per_query_update_metrics
                    .entry(query_id.to_string())
                    .or_default()
                    .add(value, 1.0);
                self.global_update.add(value, 1.0);
            }
        }
    }

    /// Port of `getMetricsJSONForQuery`. Returns the protocol JSON object
    /// directly so the inspector response can pass it through unchanged.
    pub fn metrics_json_for_query(&mut self, query_id: &str) -> Option<JsonValue> {
        let hydrate_ms = self.per_query_hydrate_ms.get(query_id).copied();
        let update_metrics = self.per_query_update_metrics.get_mut(query_id);
        if hydrate_ms.is_none() && update_metrics.is_none() {
            return None;
        }

        let update_digest = match update_metrics {
            Some(d) => d.to_json(),
            None => TDigest::default().to_json(),
        };
        let mut fields = Vec::new();
        if let Some(hydrate_ms) = hydrate_ms {
            fields.push((
                "query-hydration-server-ms".to_string(),
                JsonValue::Number(hydrate_ms),
            ));
        }
        fields.push((
            "query-update-server".to_string(),
            JsonValue::Array(update_digest.into_iter().map(JsonValue::Number).collect()),
        ));
        Some(JsonValue::Object(fields))
    }

    /// Port of `getMetricsJSON`.
    pub fn metrics_json(&mut self) -> ServerMetrics {
        ServerMetrics {
            query_materialization_server: self.global_materialization.to_json(),
            query_update_server: self.global_update.to_json(),
        }
    }

    pub fn ast_for_query(&self, query_id: &str) -> Option<&Ast> {
        self.query_id_to_ast.get(query_id)
    }

    pub fn remove_query(&mut self, query_id: &str) {
        self.per_query_hydrate_ms.remove(query_id);
        self.per_query_update_metrics.remove(query_id);
        self.query_id_to_ast.remove(query_id);
    }

    pub fn add_query(&mut self, query_id: impl Into<String>, ast: Ast) {
        self.query_id_to_ast.insert(query_id.into(), ast);
    }

    /// Registers the already-transformed AST for a custom query call. This is
    /// the synchronous delegate hook live inspect can use today; the async
    /// HTTP `CustomQueryTransformer` path can populate the same map when the
    /// production connection handler grows that service boundary.
    pub fn add_custom_query_transform(&mut self, name: &str, args: &[JsonValue], ast: Ast) {
        self.custom_query_transforms
            .insert(hash_of_name_and_args(name, args), ast);
    }

    /// Port-shaped `transformCustomQuery` hook for inspect. Returns an AST
    /// only when a caller has already registered this exact name+args pair.
    pub fn transform_custom_query(&self, name: &str, args: &[JsonValue]) -> Option<&Ast> {
        self.custom_query_transforms
            .get(&hash_of_name_and_args(name, args))
    }

    /// Port of `isAuthenticated`, with `is_development_mode` injected rather
    /// than read from ambient config.
    pub fn is_authenticated(&self, client_group_id: &str, is_development_mode: bool) -> bool {
        is_development_mode
            || authenticated_client_group_ids()
                .lock()
                .unwrap()
                .contains(client_group_id)
    }

    pub fn set_authenticated(&self, client_group_id: impl Into<String>) {
        authenticated_client_group_ids()
            .lock()
            .unwrap()
            .insert(client_group_id.into());
    }

    pub fn clear_authenticated(&self, client_group_id: &str) {
        authenticated_client_group_ids()
            .lock()
            .unwrap()
            .remove(client_group_id);
    }
}

/// Port of `metricsForProtocol` from `inspect-handler.ts`.
///
/// Protocol >= 51 uses the current per-query metrics object unchanged:
/// `query-hydration-server-ms` as a plain number plus `query-update-server` as
/// TDigest JSON. Older clients expect `query-materialization-server` as a
/// TDigest JSON array instead, so the scalar hydration time is wrapped into a
/// one-point digest.
pub fn metrics_for_protocol(
    metrics: Option<JsonValue>,
    protocol_version: u32,
) -> Option<JsonValue> {
    let metrics = metrics?;
    if protocol_version >= 51 {
        return Some(metrics);
    }

    let fields = match metrics {
        JsonValue::Object(fields) => fields,
        other => return Some(other),
    };

    let hydrate_ms = fields
        .iter()
        .find(|(k, _)| k == "query-hydration-server-ms")
        .and_then(|(_, v)| match v {
            JsonValue::Number(n) => Some(*n),
            _ => None,
        });
    let update_server = fields
        .iter()
        .find(|(k, _)| k == "query-update-server")
        .map(|(_, v)| v.clone());

    let mut hydrate_digest = TDigest::default();
    if let Some(hydrate_ms) = hydrate_ms {
        hydrate_digest.add(hydrate_ms, 1.0);
    }

    let mut legacy = vec![(
        "query-materialization-server".to_string(),
        JsonValue::Array(
            hydrate_digest
                .to_json()
                .into_iter()
                .map(JsonValue::Number)
                .collect(),
        ),
    )];
    if let Some(update_server) = update_server {
        legacy.push(("query-update-server".to_string(), update_server));
    }
    Some(JsonValue::Object(legacy))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arr(v: &JsonValue, key: &str) -> Vec<f64> {
        let JsonValue::Object(fields) = v else {
            panic!("expected object")
        };
        let JsonValue::Array(items) = fields.iter().find(|(k, _)| k == key).unwrap().1.clone()
        else {
            panic!("expected array field {key}")
        };
        items
            .into_iter()
            .map(|v| match v {
                JsonValue::Number(n) => n,
                other => panic!("expected number, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn add_metric_accumulates_global_and_per_query_metrics() {
        let mut d = InspectorDelegate::new();
        d.add_query("q1", Ast::table("users"));
        d.add_metric(ServerMetric::QueryMaterializationServer, 5.0, "q1");
        d.add_metric(ServerMetric::QueryMaterializationServer, 15.0, "q1");
        d.add_metric(ServerMetric::QueryUpdateServer, 3.0, "q1");

        let query_metrics = d.metrics_json_for_query("q1").unwrap();
        let JsonValue::Object(fields) = &query_metrics else {
            panic!("expected object")
        };
        assert_eq!(
            fields
                .iter()
                .find(|(k, _)| k == "query-hydration-server-ms")
                .unwrap()
                .1,
            JsonValue::Number(15.0)
        );
        assert_eq!(
            arr(&query_metrics, "query-update-server"),
            vec![1000.0, 3.0, 1.0]
        );

        let global = d.metrics_json();
        assert_eq!(
            global.query_materialization_server,
            vec![1000.0, 5.0, 1.0, 15.0, 1.0]
        );
        assert_eq!(global.query_update_server, vec![1000.0, 3.0, 1.0]);
    }

    #[test]
    fn metrics_json_for_query_returns_none_for_missing_query() {
        let mut d = InspectorDelegate::new();
        assert_eq!(d.metrics_json_for_query("missing"), None);
    }

    #[test]
    fn hydration_only_query_gets_empty_update_digest() {
        let mut d = InspectorDelegate::new();
        d.add_metric(ServerMetric::QueryMaterializationServer, 10.0, "q1");
        let metrics = d.metrics_json_for_query("q1").unwrap();
        assert_eq!(arr(&metrics, "query-update-server"), vec![1000.0]);
    }

    #[test]
    fn add_remove_and_replace_query_ast() {
        let mut d = InspectorDelegate::new();
        d.add_query("q1", Ast::table("first"));
        assert_eq!(d.ast_for_query("q1").unwrap().table, "first");
        d.add_query("q1", Ast::table("second"));
        assert_eq!(d.ast_for_query("q1").unwrap().table, "second");

        d.add_metric(ServerMetric::QueryMaterializationServer, 1.0, "q1");
        d.remove_query("q1");
        assert!(d.ast_for_query("q1").is_none());
        assert_eq!(d.metrics_json_for_query("q1"), None);
    }

    #[test]
    fn custom_query_transform_lookup_is_keyed_by_name_and_args() {
        let mut d = InspectorDelegate::new();
        let args = vec![JsonValue::String("open".to_string())];
        d.add_custom_query_transform("issuesByStatus", &args, Ast::table("issue"));

        assert_eq!(
            d.transform_custom_query("issuesByStatus", &args)
                .unwrap()
                .table,
            "issue"
        );
        assert!(d
            .transform_custom_query("issuesByStatus", &[JsonValue::String("closed".to_string())])
            .is_none());
        assert!(d.transform_custom_query("other", &args).is_none());
    }

    #[test]
    fn global_metrics_include_metrics_for_unknown_queries() {
        let mut d = InspectorDelegate::new();
        d.add_metric(ServerMetric::QueryMaterializationServer, 10.0, "missing");
        assert_eq!(
            d.metrics_json().query_materialization_server,
            vec![1000.0, 10.0, 1.0]
        );
    }

    #[test]
    fn authentication_can_be_development_or_shared_across_instances() {
        let client_group = "inspector_delegate_shared_test_group";
        let d1 = InspectorDelegate::new();
        let d2 = InspectorDelegate::new();
        d1.clear_authenticated(client_group);

        assert!(d1.is_authenticated(client_group, true));
        assert!(!d1.is_authenticated(client_group, false));
        d1.set_authenticated(client_group);
        assert!(d2.is_authenticated(client_group, false));
        d2.clear_authenticated(client_group);
        assert!(!d1.is_authenticated(client_group, false));
    }

    #[test]
    fn metrics_for_protocol_returns_none_and_new_protocol_unchanged() {
        assert_eq!(metrics_for_protocol(None, 50), None);
        let metrics = JsonValue::Object(vec![]);
        assert_eq!(
            metrics_for_protocol(Some(metrics.clone()), 51),
            Some(metrics)
        );
    }

    #[test]
    fn metrics_for_protocol_wraps_hydration_ms_for_old_protocols() {
        let mut update_digest = TDigest::default();
        update_digest.add(5.0, 1.0);
        let update_json = JsonValue::Array(
            update_digest
                .to_json()
                .into_iter()
                .map(JsonValue::Number)
                .collect(),
        );
        let metrics = JsonValue::Object(vec![
            (
                "query-hydration-server-ms".to_string(),
                JsonValue::Number(100.0),
            ),
            ("query-update-server".to_string(), update_json.clone()),
        ]);

        let result = metrics_for_protocol(Some(metrics), 50).unwrap();
        assert!(matches!(result, JsonValue::Object(_)));
        let materialization = arr(&result, "query-materialization-server");
        let mut digest = TDigest::from_json(&materialization).unwrap();
        assert_eq!(digest.count(), 1.0);
        assert_eq!(digest.quantile(0.5), 100.0);
        let expected_update = match update_json {
            JsonValue::Array(items) => items
                .into_iter()
                .map(|v| match v {
                    JsonValue::Number(n) => n,
                    other => panic!("expected number, got {other:?}"),
                })
                .collect::<Vec<_>>(),
            _ => unreachable!(),
        };
        assert_eq!(arr(&result, "query-update-server"), expected_update);
    }

    #[test]
    fn metrics_for_protocol_old_protocol_handles_missing_fields() {
        let only_update = JsonValue::Object(vec![(
            "query-update-server".to_string(),
            JsonValue::Array(vec![JsonValue::Number(1000.0)]),
        )]);
        let result = metrics_for_protocol(Some(only_update), 50).unwrap();
        let mut materialization =
            TDigest::from_json(&arr(&result, "query-materialization-server")).unwrap();
        assert_eq!(materialization.count(), 0.0);
        assert_eq!(arr(&result, "query-update-server"), vec![1000.0]);

        let only_hydrate = JsonValue::Object(vec![(
            "query-hydration-server-ms".to_string(),
            JsonValue::Number(25.0),
        )]);
        let result = metrics_for_protocol(Some(only_hydrate), 1).unwrap();
        let JsonValue::Object(fields) = &result else {
            panic!("expected object")
        };
        assert!(fields.iter().all(|(k, _)| k != "query-update-server"));
        let mut materialization =
            TDigest::from_json(&arr(&result, "query-materialization-server")).unwrap();
        assert_eq!(materialization.count(), 1.0);
        assert_eq!(materialization.quantile(0.5), 25.0);
    }
}
