//! Wire decode (`push` request) and encode (`pushResponse` reply) for
//! [`crate::push`] — closes the "push"/"pull" gap `up_json.rs`'s module doc
//! named as an explicit, real, unported piece (pull and custom-mutator
//! specifics remain out of scope; the CRUD push path is what this closes).

use zero_cache_shared::bigint_json::{stringify as json_value_stringify, JsonValue};

use crate::mutation_id::MutationId;
use crate::mutation_result::MutationResponse;
use crate::push::{CrudMutation, CustomMutation, Mutation, PushBody, PushOk};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("push message JSON: {0}")]
pub struct PushJsonError(pub String);

type R<T> = Result<T, PushJsonError>;

fn err<T>(m: impl Into<String>) -> R<T> {
    Err(PushJsonError(m.into()))
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
fn req<'a>(o: &'a JsonValue, k: &str) -> R<&'a JsonValue> {
    field(o, k).ok_or_else(|| PushJsonError(format!("missing field {k:?}")))
}
fn as_str(v: &JsonValue) -> R<String> {
    match v {
        JsonValue::String(s) => Ok(s.clone()),
        other => err(format!("expected string, got {other:?}")),
    }
}
fn as_f64(v: &JsonValue) -> R<f64> {
    match v {
        JsonValue::Number(n) => Ok(*n),
        JsonValue::BigInt(b) => Ok(b.to_string().parse().unwrap_or(0.0)),
        other => err(format!("expected number, got {other:?}")),
    }
}
fn as_array(v: &JsonValue) -> R<&[JsonValue]> {
    match v {
        JsonValue::Array(a) => Ok(a),
        other => err(format!("expected array, got {other:?}")),
    }
}

/// Parses one element of `pushBodySchema.mutations` — a `crudMutationSchema`
/// or `customMutationSchema` object, discriminated by `type`.
fn mutation_from_json(v: &JsonValue) -> R<Mutation> {
    let ty = as_str(req(v, "type")?)?;
    let id = as_f64(req(v, "id")?)?;
    let client_id = as_str(req(v, "clientID")?)?;
    let timestamp = as_f64(req(v, "timestamp")?)?;
    match ty.as_str() {
        "crud" => {
            // args: [{ops: [...]}] — the ops array itself is left undecoded
            // (see module/push.rs doc); this just unwraps the one-element
            // tuple down to that inner object.
            let args = as_array(req(v, "args")?)?;
            let arg0 = args
                .first()
                .ok_or_else(|| PushJsonError("crud mutation args must have one element".into()))?;
            let ops_json = req(arg0, "ops")?.clone();
            Ok(Mutation::Crud(CrudMutation {
                id,
                client_id,
                ops_json,
                timestamp,
            }))
        }
        "custom" => {
            let name = as_str(req(v, "name")?)?;
            let args = as_array(req(v, "args")?)?.to_vec();
            Ok(Mutation::Custom(CustomMutation {
                id,
                client_id,
                name,
                args,
                timestamp,
            }))
        }
        other => err(format!("unknown mutation type {other:?}")),
    }
}

/// Parses a `push` message's body (the `pushBodySchema` object — NOT
/// including the `["push", ...]` tag envelope, matching how
/// `up_json::upstream_from_json` hands each variant's decoder just the body).
pub fn push_body_from_json(v: &JsonValue) -> R<PushBody> {
    let mutations = as_array(req(v, "mutations")?)?
        .iter()
        .map(mutation_from_json)
        .collect::<R<Vec<_>>>()?;
    Ok(PushBody {
        client_group_id: as_str(req(v, "clientGroupID")?)?,
        mutations,
        push_version: as_f64(req(v, "pushVersion")?)?,
        schema_version: opt(v, "schemaVersion").map(as_f64).transpose()?,
        timestamp: as_f64(req(v, "timestamp")?)?,
        request_id: as_str(req(v, "requestID")?)?,
        traceparent: opt(v, "traceparent").map(as_str).transpose()?,
    })
}

fn mutation_id_json(id: &MutationId) -> String {
    format!(
        "{{\"id\":{},\"clientID\":{}}}",
        id.id,
        json_value_stringify(&JsonValue::String(id.client_id.clone()))
    )
}

/// Encodes one `MutationResponse` (`{id, result}`) — the element shape of
/// `pushOkSchema.mutations`, distinct from `mutations-patch.ts`'s `put`
/// wrapper (`{op, mutation: {id, result}}`) that `poke_json.rs` encodes.
fn mutation_response_json(r: &MutationResponse) -> String {
    use crate::mutation_result::{MutationError, MutationResult};
    let result = match &r.result {
        MutationResult::Ok(ok) => match &ok.data {
            Some(d) => format!("{{\"data\":{}}}", json_value_stringify(d)),
            None => "{}".to_string(),
        },
        MutationResult::Error(MutationError::App(e)) => {
            let message = e
                .message
                .as_ref()
                .map(|m| {
                    format!(
                        ",\"message\":{}",
                        json_value_stringify(&JsonValue::String(m.clone()))
                    )
                })
                .unwrap_or_default();
            let details = e
                .details
                .as_ref()
                .map(|d| format!(",\"details\":{}", json_value_stringify(d)))
                .unwrap_or_default();
            format!("{{\"error\":\"app\"{message}{details}}}")
        }
        MutationResult::Error(MutationError::Zero(e)) => {
            let kind = match e.error {
                crate::mutation_result::ZeroErrorKind::OooMutation => "oooMutation",
                crate::mutation_result::ZeroErrorKind::AlreadyProcessed => "alreadyProcessed",
            };
            let details = e
                .details
                .as_ref()
                .map(|d| format!(",\"details\":{}", json_value_stringify(d)))
                .unwrap_or_default();
            format!("{{\"error\":\"{kind}\"{details}}}")
        }
    };
    format!(
        "{{\"id\":{},\"result\":{}}}",
        mutation_id_json(&r.id),
        result
    )
}

/// Encodes a full `["pushResponse", {"mutations": [...]}]` frame — the
/// `pushOkSchema` reply to a `push` message.
pub fn push_ok_message_json(ok: &PushOk) -> String {
    let mutations: Vec<String> = ok.mutations.iter().map(mutation_response_json).collect();
    format!(
        "[\"pushResponse\",{{\"mutations\":[{}]}}]",
        mutations.join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation_result::{MutationOk, MutationResult};
    use zero_cache_shared::bigint_json::parse;

    #[test]
    fn decodes_a_push_body_with_a_crud_mutation() {
        let json = parse(
            r#"{
              "clientGroupID": "cg1",
              "pushVersion": 1,
              "schemaVersion": 2,
              "timestamp": 100,
              "requestID": "req1",
              "mutations": [
                {"type": "crud", "id": 1, "clientID": "c1", "name": "_zero_crud",
                 "timestamp": 100,
                 "args": [{"ops": [
                   {"op": "insert", "tableName": "issue", "primaryKey": ["id"],
                    "value": {"id": "1", "title": "hi"}}
                 ]}]}
              ]
            }"#,
        )
        .unwrap();
        let body = push_body_from_json(&json).unwrap();
        assert_eq!(body.client_group_id, "cg1");
        assert_eq!(body.push_version, 1.0);
        assert_eq!(body.schema_version, Some(2.0));
        assert_eq!(body.mutations.len(), 1);
        let Mutation::Crud(m) = &body.mutations[0] else {
            panic!("expected Crud")
        };
        assert_eq!(m.id, 1.0);
        assert_eq!(m.client_id, "c1");
        // ops_json carries the raw array, undecoded (mutagen's job).
        assert!(matches!(m.ops_json, JsonValue::Array(_)));
    }

    #[test]
    fn decodes_a_custom_mutation() {
        let json = parse(
            r#"{"clientGroupID":"cg1","pushVersion":1,"timestamp":1,"requestID":"r",
               "mutations":[{"type":"custom","id":2,"clientID":"c2","name":"doThing","timestamp":1,"args":[1,"x"]}]}"#,
        )
        .unwrap();
        let body = push_body_from_json(&json).unwrap();
        let Mutation::Custom(m) = &body.mutations[0] else {
            panic!("expected Custom")
        };
        assert_eq!(m.name, "doThing");
        assert_eq!(m.args.len(), 2);
        assert_eq!(
            body.mutations[0].id(),
            MutationId {
                id: 2.0,
                client_id: "c2".into()
            }
        );
    }

    #[test]
    fn encodes_a_push_ok_response() {
        let ok = PushOk {
            mutations: vec![MutationResponse {
                id: MutationId {
                    id: 1.0,
                    client_id: "c1".into(),
                },
                result: MutationResult::Ok(MutationOk { data: None }),
            }],
        };
        let json = push_ok_message_json(&ok);
        assert_eq!(
            json,
            r#"["pushResponse",{"mutations":[{"id":{"id":1,"clientID":"c1"},"result":{}}]}]"#
        );
    }

    #[test]
    fn missing_required_field_and_unknown_type_error() {
        assert!(push_body_from_json(&parse(r#"{"clientGroupID":"cg1"}"#).unwrap()).is_err());
        let bad = parse(r#"{"clientGroupID":"cg1","pushVersion":1,"timestamp":1,"requestID":"r","mutations":[{"type":"weird","id":1,"clientID":"c","timestamp":1}]}"#).unwrap();
        assert!(push_body_from_json(&bad).is_err());
    }
}
