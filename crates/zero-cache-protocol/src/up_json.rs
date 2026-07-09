//! Wire decoder for upstream (client -> server) messages — the read side the
//! sync-server needs to turn an incoming WebSocket JSON frame into an
//! [`Upstream`] value.
//!
//! Upstream messages are `[tag, body]` JSON tuples (`["ping", {...}]`,
//! `["changeDesiredQueries", {...}]`, ...). This parses that envelope and each
//! ported body (the counterpart to `poke_json`'s downstream *encoder*),
//! operating on this crate's `JsonValue`. The query-carrying bodies
//! (`initConnection`/`changeDesiredQueries`) decode an `UpQueriesPatch` whose
//! `put` ops carry an AST via the existing [`crate::ast_json::ast_from_json`].
//!
//! All upstream tags with ported pure request bodies are decoded here.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::JsonValue;

use crate::ast_json::ast_from_json;
use crate::change_desired_queries::ChangeDesiredQueriesBody;
use crate::client_schema::{ClientSchema, ColumnSchema, TableSchema, ValueType};
use crate::close_connection::CloseConnectionBody;
use crate::connect::InitConnectionBody;
use crate::delete_clients::DeleteClientsBody;
use crate::inspect_up::{AnalyzeQueryOptions, InspectUpBody};
use crate::ping::PingBody;
use crate::push::AckMutationResponsesBody;
use crate::queries_patch::{
    QueriesClearOp, QueriesDelOp, UpQueriesPatch, UpQueriesPatchOp, UpQueriesPutOp,
};
use crate::up::Upstream;
use crate::update_auth::UpdateAuthBody;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("upstream message JSON: {0}")]
pub struct UpJsonError(pub String);

type R<T> = Result<T, UpJsonError>;

fn e<T>(m: impl Into<String>) -> R<T> {
    Err(UpJsonError(m.into()))
}

fn field<'a>(o: &'a JsonValue, k: &str) -> Option<&'a JsonValue> {
    match o {
        JsonValue::Object(es) => es.iter().find(|(n, _)| n == k).map(|(_, v)| v),
        _ => None,
    }
}
fn opt<'a>(o: &'a JsonValue, k: &str) -> Option<&'a JsonValue> {
    match field(o, k) {
        Some(JsonValue::Null) | None => None,
        Some(v) => Some(v),
    }
}
fn as_str(v: &JsonValue) -> R<String> {
    match v {
        JsonValue::String(s) => Ok(s.clone()),
        _ => e(format!("expected string, got {v:?}")),
    }
}
fn as_f64(v: &JsonValue) -> R<f64> {
    match v {
        JsonValue::Number(n) => Ok(*n),
        JsonValue::BigInt(b) => Ok(b.to_string().parse().unwrap_or(0.0)),
        _ => e(format!("expected number, got {v:?}")),
    }
}
fn as_bool(v: &JsonValue) -> R<bool> {
    match v {
        JsonValue::Bool(b) => Ok(*b),
        _ => e(format!("expected boolean, got {v:?}")),
    }
}
fn as_array(v: &JsonValue) -> R<&[JsonValue]> {
    match v {
        JsonValue::Array(a) => Ok(a),
        _ => e(format!("expected array, got {v:?}")),
    }
}
fn as_object(v: &JsonValue) -> R<&[(String, JsonValue)]> {
    match v {
        JsonValue::Object(es) => Ok(es),
        _ => e(format!("expected object, got {v:?}")),
    }
}
fn req<'a>(o: &'a JsonValue, k: &str) -> R<&'a JsonValue> {
    field(o, k).ok_or_else(|| UpJsonError(format!("missing field {k:?}")))
}
fn str_array(v: &JsonValue) -> R<Vec<String>> {
    as_array(v)?.iter().map(as_str).collect()
}
fn str_map(v: &JsonValue) -> R<BTreeMap<String, String>> {
    as_object(v)?
        .iter()
        .map(|(k, val)| Ok((k.clone(), as_str(val)?)))
        .collect()
}

fn mutation_id_from_json(v: &JsonValue) -> R<crate::mutation_id::MutationId> {
    Ok(crate::mutation_id::MutationId {
        id: as_f64(req(v, "id")?)?,
        client_id: as_str(req(v, "clientID")?)?,
    })
}

/// Parses an `UpQueriesPatch` (array of put/del/clear ops).
pub fn up_queries_patch_from_json(v: &JsonValue) -> R<UpQueriesPatch> {
    as_array(v)?
        .iter()
        .map(|op| {
            let tag = as_str(req(op, "op")?)?;
            match tag.as_str() {
                "put" => Ok(UpQueriesPatchOp::Put(UpQueriesPutOp {
                    hash: as_str(req(op, "hash")?)?,
                    ttl: opt(op, "ttl").map(as_f64).transpose()?,
                    ast: opt(op, "ast")
                        .map(|a| ast_from_json(a).map_err(|err| UpJsonError(err.to_string())))
                        .transpose()?,
                    name: opt(op, "name").map(as_str).transpose()?,
                    args: opt(op, "args")
                        .map(|a| as_array(a).map(<[_]>::to_vec))
                        .transpose()?,
                })),
                "del" => Ok(UpQueriesPatchOp::Del(QueriesDelOp {
                    hash: as_str(req(op, "hash")?)?,
                })),
                "clear" => Ok(UpQueriesPatchOp::Clear(QueriesClearOp)),
                other => e(format!("unknown queries-patch op {other:?}")),
            }
        })
        .collect()
}

fn value_type_from_str(s: &str) -> R<ValueType> {
    Ok(match s {
        "string" => ValueType::String,
        "number" => ValueType::Number,
        "boolean" => ValueType::Boolean,
        "null" => ValueType::Null,
        "json" => ValueType::Json,
        other => return e(format!("unknown client-schema value type {other:?}")),
    })
}

/// Parses a `ClientSchema` (`{tables: {name: {columns: {name: {type}}, primaryKey}}}`).
pub fn client_schema_from_json(v: &JsonValue) -> R<ClientSchema> {
    let tables = as_object(req(v, "tables")?)?
        .iter()
        .map(|(tname, tbl)| {
            let columns = as_object(req(tbl, "columns")?)?
                .iter()
                .map(|(cname, col)| {
                    Ok((
                        cname.clone(),
                        ColumnSchema {
                            value_type: value_type_from_str(&as_str(req(col, "type")?)?)?,
                        },
                    ))
                })
                .collect::<R<Vec<_>>>()?;
            let primary_key = str_array(req(tbl, "primaryKey")?)?;
            Ok((
                tname.clone(),
                TableSchema {
                    columns,
                    primary_key,
                },
            ))
        })
        .collect::<R<Vec<_>>>()?;
    Ok(ClientSchema { tables })
}

/// Encodes a `ClientSchema` back to the wire JSON shape
/// (`{tables: {name: {columns: {name: {type}}, primaryKey}}}`) — the inverse
/// of [`client_schema_from_json`], so a received-and-parsed schema can be
/// re-serialized (e.g. to persist it in the CVR as JSONB). Object key order
/// follows the schema's own `Vec` ordering, matching `normalize_client_schema`.
pub fn client_schema_to_json(schema: &ClientSchema) -> JsonValue {
    fn value_type_str(vt: ValueType) -> &'static str {
        match vt {
            ValueType::String => "string",
            ValueType::Number => "number",
            ValueType::Boolean => "boolean",
            ValueType::Null => "null",
            ValueType::Json => "json",
        }
    }
    let tables = schema
        .tables
        .iter()
        .map(|(tname, tbl)| {
            let columns = tbl
                .columns
                .iter()
                .map(|(cname, col)| {
                    (
                        cname.clone(),
                        JsonValue::Object(vec![(
                            "type".into(),
                            JsonValue::String(value_type_str(col.value_type).into()),
                        )]),
                    )
                })
                .collect();
            let primary_key = JsonValue::Array(
                tbl.primary_key
                    .iter()
                    .map(|k| JsonValue::String(k.clone()))
                    .collect(),
            );
            (
                tname.clone(),
                JsonValue::Object(vec![
                    ("columns".into(), JsonValue::Object(columns)),
                    ("primaryKey".into(), primary_key),
                ]),
            )
        })
        .collect();
    JsonValue::Object(vec![("tables".into(), JsonValue::Object(tables))])
}

/// Parses a `deleteClients` body.
pub fn delete_clients_from_json(v: &JsonValue) -> R<DeleteClientsBody> {
    Ok(DeleteClientsBody {
        client_ids: opt(v, "clientIDs").map(str_array).transpose()?,
        client_group_ids: opt(v, "clientGroupIDs").map(str_array).transpose()?,
    })
}

/// Parses a `changeDesiredQueries` body.
pub fn change_desired_queries_from_json(v: &JsonValue) -> R<ChangeDesiredQueriesBody> {
    Ok(ChangeDesiredQueriesBody {
        desired_queries_patch: up_queries_patch_from_json(req(v, "desiredQueriesPatch")?)?,
        traceparent: opt(v, "traceparent").map(as_str).transpose()?,
    })
}

/// Parses an `initConnection` body.
pub fn init_connection_from_json(v: &JsonValue) -> R<InitConnectionBody> {
    Ok(InitConnectionBody {
        desired_queries_patch: up_queries_patch_from_json(req(v, "desiredQueriesPatch")?)?,
        client_schema: opt(v, "clientSchema")
            .map(client_schema_from_json)
            .transpose()?,
        deleted: opt(v, "deleted")
            .map(delete_clients_from_json)
            .transpose()?,
        user_push_url: opt(v, "userPushURL").map(as_str).transpose()?,
        user_push_headers: opt(v, "userPushHeaders").map(str_map).transpose()?,
        user_query_url: opt(v, "userQueryURL").map(as_str).transpose()?,
        user_query_headers: opt(v, "userQueryHeaders").map(str_map).transpose()?,
        active_clients: opt(v, "activeClients").map(str_array).transpose()?,
        traceparent: opt(v, "traceparent").map(as_str).transpose()?,
    })
}

/// Parses an `updateAuth` body.
pub fn update_auth_from_json(v: &JsonValue) -> R<UpdateAuthBody> {
    Ok(UpdateAuthBody {
        auth: as_str(req(v, "auth")?)?,
    })
}

/// Parses an `ackMutationResponses` body.
pub fn ack_mutation_responses_from_json(v: &JsonValue) -> R<AckMutationResponsesBody> {
    Ok(AckMutationResponsesBody {
        mutation_id: mutation_id_from_json(v)?,
    })
}

fn analyze_query_options_from_json(v: &JsonValue) -> R<AnalyzeQueryOptions> {
    Ok(AnalyzeQueryOptions {
        vended_rows: opt(v, "vendedRows").map(as_bool).transpose()?,
        synced_rows: opt(v, "syncedRows").map(as_bool).transpose()?,
        join_plans: opt(v, "joinPlans").map(as_bool).transpose()?,
    })
}

/// Parses an `inspect` body.
pub fn inspect_up_from_json(v: &JsonValue) -> R<InspectUpBody> {
    let id = as_str(req(v, "id")?)?;
    let op = as_str(req(v, "op")?)?;
    Ok(match op.as_str() {
        "queries" => InspectUpBody::Queries {
            id,
            client_id: opt(v, "clientID").map(as_str).transpose()?,
        },
        "metrics" => InspectUpBody::Metrics { id },
        "version" => InspectUpBody::Version { id },
        "authenticate" => InspectUpBody::Authenticate {
            id,
            value: as_str(req(v, "value")?)?,
        },
        "analyze-query" => InspectUpBody::AnalyzeQuery {
            id,
            value: opt(v, "value")
                .map(|a| ast_from_json(a).map_err(|err| UpJsonError(err.to_string())))
                .transpose()?,
            options: opt(v, "options")
                .map(analyze_query_options_from_json)
                .transpose()?,
            ast: opt(v, "ast")
                .map(|a| ast_from_json(a).map_err(|err| UpJsonError(err.to_string())))
                .transpose()?,
            name: opt(v, "name").map(as_str).transpose()?,
            args: opt(v, "args")
                .map(|a| as_array(a).map(<[_]>::to_vec))
                .transpose()?,
        },
        other => return e(format!("unknown inspect op {other:?}")),
    })
}

/// Parses a whole `[tag, body]` upstream message.
pub fn upstream_from_json(v: &JsonValue) -> R<Upstream> {
    let arr = as_array(v)?;
    if arr.len() != 2 {
        return e(format!(
            "upstream message must be a [tag, body] pair, got {} elements",
            arr.len()
        ));
    }
    let tag = as_str(&arr[0])?;
    let body = &arr[1];
    match tag.as_str() {
        "ping" => Ok(Upstream::Ping(PingBody)),
        "closeConnection" => Ok(Upstream::CloseConnection(CloseConnectionBody)),
        "deleteClients" => Ok(Upstream::DeleteClients(delete_clients_from_json(body)?)),
        "changeDesiredQueries" => Ok(Upstream::ChangeDesiredQueries(
            change_desired_queries_from_json(body)?,
        )),
        "initConnection" => Ok(Upstream::InitConnection(init_connection_from_json(body)?)),
        "pull" => Ok(Upstream::Pull(
            crate::pull_json::pull_request_body_from_json(body)
                .map_err(|e| UpJsonError(e.to_string()))?,
        )),
        "push" => Ok(Upstream::Push(
            crate::push_json::push_body_from_json(body).map_err(|e| UpJsonError(e.to_string()))?,
        )),
        "updateAuth" => Ok(Upstream::UpdateAuth(update_auth_from_json(body)?)),
        "ackMutationResponses" => Ok(Upstream::AckMutationResponses(
            ack_mutation_responses_from_json(body)?,
        )),
        "inspect" => Ok(Upstream::Inspect(inspect_up_from_json(body)?)),
        other => Err(UpJsonError(format!(
            "unknown or unported upstream tag {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_shared::bigint_json::parse;

    #[test]
    fn client_schema_json_round_trips() {
        let json = parse(
            r#"{"tables":{"issue":{"columns":{"id":{"type":"string"},"n":{"type":"number"}},"primaryKey":["id"]}}}"#,
        )
        .unwrap();
        let schema = client_schema_from_json(&json).unwrap();
        // encode then re-parse: the schema survives the round trip.
        let encoded = client_schema_to_json(&schema);
        let reparsed = client_schema_from_json(&encoded).unwrap();
        assert_eq!(schema, reparsed);
        // and the encoded JSON matches the original wire shape exactly.
        assert_eq!(encoded, json);
    }

    #[test]
    fn client_schema_to_json_encodes_every_value_type() {
        use crate::client_schema::{ClientSchema, ColumnSchema, TableSchema};
        let schema = ClientSchema {
            tables: vec![(
                "t".into(),
                TableSchema {
                    columns: vec![
                        (
                            "s".into(),
                            ColumnSchema {
                                value_type: ValueType::String,
                            },
                        ),
                        (
                            "n".into(),
                            ColumnSchema {
                                value_type: ValueType::Number,
                            },
                        ),
                        (
                            "b".into(),
                            ColumnSchema {
                                value_type: ValueType::Boolean,
                            },
                        ),
                        (
                            "z".into(),
                            ColumnSchema {
                                value_type: ValueType::Null,
                            },
                        ),
                        (
                            "j".into(),
                            ColumnSchema {
                                value_type: ValueType::Json,
                            },
                        ),
                    ],
                    primary_key: vec!["s".into()],
                },
            )],
        };
        // Round-trips through the parser for every ValueType variant.
        assert_eq!(
            client_schema_from_json(&client_schema_to_json(&schema)).unwrap(),
            schema
        );
    }

    #[test]
    fn parses_ping_and_close() {
        assert_eq!(
            upstream_from_json(&parse(r#"["ping", {}]"#).unwrap()).unwrap(),
            Upstream::Ping(PingBody)
        );
        assert_eq!(
            upstream_from_json(&parse(r#"["closeConnection", {}]"#).unwrap()).unwrap(),
            Upstream::CloseConnection(CloseConnectionBody)
        );
    }

    #[test]
    fn parses_delete_clients() {
        let msg = parse(r#"["deleteClients", {"clientIDs": ["a", "b"], "clientGroupIDs": ["g"]}]"#)
            .unwrap();
        let Upstream::DeleteClients(b) = upstream_from_json(&msg).unwrap() else {
            panic!()
        };
        assert_eq!(b.client_ids, Some(vec!["a".into(), "b".into()]));
        assert_eq!(b.client_group_ids, Some(vec!["g".into()]));
    }

    #[test]
    fn parses_change_desired_queries_with_put_del_clear() {
        let msg = parse(
            r#"["changeDesiredQueries", {"desiredQueriesPatch": [
                {"op": "put", "hash": "h1", "ttl": 1000, "name": "q", "args": [1, "x"]},
                {"op": "del", "hash": "h2"},
                {"op": "clear"}
            ]}]"#,
        )
        .unwrap();
        let Upstream::ChangeDesiredQueries(b) = upstream_from_json(&msg).unwrap() else {
            panic!()
        };
        assert_eq!(b.desired_queries_patch.len(), 3);
        match &b.desired_queries_patch[0] {
            UpQueriesPatchOp::Put(p) => {
                assert_eq!(p.hash, "h1");
                assert_eq!(p.ttl, Some(1000.0));
                assert_eq!(p.name.as_deref(), Some("q"));
                assert_eq!(p.args.as_ref().unwrap().len(), 2);
            }
            other => panic!("expected Put, got {other:?}"),
        }
        assert_eq!(
            b.desired_queries_patch[1],
            UpQueriesPatchOp::Del(QueriesDelOp { hash: "h2".into() })
        );
        assert_eq!(
            b.desired_queries_patch[2],
            UpQueriesPatchOp::Clear(QueriesClearOp)
        );
    }

    #[test]
    fn parses_init_connection_with_schema_and_optional_fields() {
        let msg = parse(
            r#"["initConnection", {
                "desiredQueriesPatch": [{"op": "clear"}],
                "clientSchema": {"tables": {"issue": {
                    "columns": {"id": {"type": "string"}, "n": {"type": "number"}},
                    "primaryKey": ["id"]
                }}},
                "activeClients": ["c1", "c2"],
                "userQueryURL": "https://q"
            }]"#,
        )
        .unwrap();
        let Upstream::InitConnection(b) = upstream_from_json(&msg).unwrap() else {
            panic!()
        };
        assert_eq!(b.desired_queries_patch.len(), 1);
        assert_eq!(b.active_clients, Some(vec!["c1".into(), "c2".into()]));
        assert_eq!(b.user_query_url.as_deref(), Some("https://q"));
        let schema = b.client_schema.unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].0, "issue");
        assert_eq!(schema.tables[0].1.primary_key, vec!["id".to_string()]);
        assert_eq!(schema.tables[0].1.columns.len(), 2);
    }

    #[test]
    fn put_op_decodes_an_ast() {
        let msg = parse(
            r#"["changeDesiredQueries", {"desiredQueriesPatch": [
                {"op": "put", "hash": "h", "ast": {"table": "issue"}}
            ]}]"#,
        )
        .unwrap();
        let Upstream::ChangeDesiredQueries(b) = upstream_from_json(&msg).unwrap() else {
            panic!()
        };
        match &b.desired_queries_patch[0] {
            UpQueriesPatchOp::Put(p) => {
                let ast = p.ast.as_ref().expect("ast decoded");
                assert_eq!(ast.table, "issue");
            }
            other => panic!("expected Put, got {other:?}"),
        }
    }

    #[test]
    fn unknown_tag_and_malformed_envelope_error() {
        // "pull" is decoded now, but an empty body is still missing required
        // fields, so it errors for a schema reason.
        assert!(upstream_from_json(&parse(r#"["pull", {}]"#).unwrap()).is_err());
        // "push" is now decoded, but an empty body is still missing required
        // fields, so it errors for a different (real) reason.
        assert!(upstream_from_json(&parse(r#"["push", {}]"#).unwrap()).is_err());
        assert!(
            upstream_from_json(&parse(r#"["ping"]"#).unwrap()).is_err(),
            "needs [tag, body]"
        );
        assert!(upstream_from_json(&parse(r#"{"not": "an array"}"#).unwrap()).is_err());
    }

    #[test]
    fn parses_a_push_message_with_a_crud_mutation() {
        let msg = parse(
            r#"["push", {
                "clientGroupID": "cg1", "pushVersion": 1, "timestamp": 1, "requestID": "r1",
                "mutations": [
                    {"type": "crud", "id": 1, "clientID": "c1", "timestamp": 1,
                     "args": [{"ops": [{"op":"delete","tableName":"issue","primaryKey":["id"],"value":{"id":"1"}}]}]}
                ]
            }]"#,
        )
        .unwrap();
        let Upstream::Push(body) = upstream_from_json(&msg).unwrap() else {
            panic!("expected Push")
        };
        assert_eq!(body.client_group_id, "cg1");
        assert_eq!(body.mutations.len(), 1);
    }

    #[test]
    fn parses_a_pull_message() {
        let msg = parse(r#"["pull", {"clientGroupID": "cg1", "cookie": null, "requestID": "r1"}]"#)
            .unwrap();
        let Upstream::Pull(body) = upstream_from_json(&msg).unwrap() else {
            panic!("expected Pull")
        };
        assert_eq!(body.client_group_id, "cg1");
        assert_eq!(body.cookie, None);
        assert_eq!(body.request_id, "r1");
    }

    #[test]
    fn parses_update_auth() {
        let msg = parse(r#"["updateAuth", {"auth": "new-token"}]"#).unwrap();
        let Upstream::UpdateAuth(body) = upstream_from_json(&msg).unwrap() else {
            panic!("expected UpdateAuth")
        };
        assert_eq!(body.auth, "new-token");
    }

    #[test]
    fn parses_ack_mutation_responses() {
        let msg = parse(r#"["ackMutationResponses", {"id": 7, "clientID": "c1"}]"#).unwrap();
        let Upstream::AckMutationResponses(body) = upstream_from_json(&msg).unwrap() else {
            panic!("expected AckMutationResponses")
        };
        assert_eq!(body.mutation_id.id, 7.0);
        assert_eq!(body.mutation_id.client_id, "c1");
    }

    #[test]
    fn parses_inspect_queries_metrics_version_and_authenticate() {
        let msg = parse(r#"["inspect", {"op": "queries", "id": "iq", "clientID": "c1"}]"#).unwrap();
        let Upstream::Inspect(InspectUpBody::Queries { id, client_id }) =
            upstream_from_json(&msg).unwrap()
        else {
            panic!("expected inspect queries")
        };
        assert_eq!(id, "iq");
        assert_eq!(client_id.as_deref(), Some("c1"));

        let msg = parse(r#"["inspect", {"op": "metrics", "id": "im"}]"#).unwrap();
        assert!(matches!(
            upstream_from_json(&msg).unwrap(),
            Upstream::Inspect(InspectUpBody::Metrics { .. })
        ));

        let msg = parse(r#"["inspect", {"op": "version", "id": "iv"}]"#).unwrap();
        assert!(matches!(
            upstream_from_json(&msg).unwrap(),
            Upstream::Inspect(InspectUpBody::Version { .. })
        ));

        let msg =
            parse(r#"["inspect", {"op": "authenticate", "id": "ia", "value": "pw"}]"#).unwrap();
        let Upstream::Inspect(InspectUpBody::Authenticate { id, value }) =
            upstream_from_json(&msg).unwrap()
        else {
            panic!("expected authenticate")
        };
        assert_eq!(id, "ia");
        assert_eq!(value, "pw");
    }

    #[test]
    fn parses_inspect_analyze_query_with_ast_name_args_and_options() {
        let msg = parse(
            r#"["inspect", {
                "op": "analyze-query",
                "id": "ia",
                "ast": {"table": "issue"},
                "value": {"table": "legacy"},
                "name": "custom",
                "args": [1, "x"],
                "options": {"vendedRows": true, "syncedRows": false, "joinPlans": true}
            }]"#,
        )
        .unwrap();
        let Upstream::Inspect(InspectUpBody::AnalyzeQuery {
            id,
            value,
            options,
            ast,
            name,
            args,
        }) = upstream_from_json(&msg).unwrap()
        else {
            panic!("expected analyze-query")
        };
        assert_eq!(id, "ia");
        assert_eq!(ast.unwrap().table, "issue");
        assert_eq!(value.unwrap().table, "legacy");
        assert_eq!(name.as_deref(), Some("custom"));
        assert_eq!(args.unwrap().len(), 2);
        let options = options.unwrap();
        assert_eq!(options.vended_rows, Some(true));
        assert_eq!(options.synced_rows, Some(false));
        assert_eq!(options.join_plans, Some(true));
    }

    #[test]
    fn inspect_unknown_op_errors() {
        let msg = parse(r#"["inspect", {"op": "unknown", "id": "i"}]"#).unwrap();
        assert!(upstream_from_json(&msg).is_err());
    }
}
