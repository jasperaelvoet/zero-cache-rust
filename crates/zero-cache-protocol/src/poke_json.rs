//! Hand-rolled JSON serialization for [`crate::poke`]/[`crate::row_patch`],
//! matching the style already established by
//! `zero_cache_shared::bigint_json` (this port has no `serde` dependency
//! anywhere yet — adding one is a real architectural choice deferred to a
//! future round, noted in `PORTING.md`; this module keeps the existing
//! hand-rolled-codec convention instead of introducing a second style).
//!
//! Scope: covers `PokeMessage`/`RowPatchOp`/`QueriesPatchOp`/
//! `lastMutationIDChanges`/`mutationsPatch` (both `Put` and `Del`) — the
//! full `PokePartBody` surface this port's `client_handler.rs` needs.

use zero_cache_shared::bigint_json::{stringify as json_value_stringify, JsonValue};

use crate::mutation_result::{MutationError, MutationResult};
use crate::mutations_patch::{MutationPatchOp, MutationsPatch};
use crate::poke::{PokeEndBody, PokeMessage, PokePartBody, PokeStartBody};
use crate::queries_patch::{QueriesPatch, QueriesPatchOp};
use crate::row_patch::{Row, RowPatchOp};
use crate::version::NullableVersion;

fn json_string(s: &str) -> String {
    // Reuses `JsonValue::String`'s own escaping rather than duplicating it.
    json_value_stringify(&JsonValue::String(s.to_string()))
}

fn json_nullable_version(v: &NullableVersion) -> String {
    match v {
        Some(s) => json_string(s),
        None => "null".to_string(),
    }
}

fn json_row(row: &Row) -> String {
    json_value_stringify(&JsonValue::Object(row.clone()))
}

fn row_patch_op_json(op: &RowPatchOp) -> String {
    match op {
        RowPatchOp::Put(put) => {
            format!(
                "{{\"op\":\"put\",\"tableName\":{},\"value\":{}}}",
                json_string(&put.table_name),
                json_row(&put.value)
            )
        }
        RowPatchOp::Update(update) => {
            let id = json_row(
                &update
                    .id
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            );
            format!(
                "{{\"op\":\"update\",\"tableName\":{},\"id\":{}}}",
                json_string(&update.table_name),
                id
            )
        }
        RowPatchOp::Del(del) => {
            let id = json_row(&del.id.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
            format!(
                "{{\"op\":\"del\",\"tableName\":{},\"id\":{}}}",
                json_string(&del.table_name),
                id
            )
        }
        RowPatchOp::Clear(_) => "{\"op\":\"clear\"}".to_string(),
    }
}

fn queries_patch_op_json(op: &QueriesPatchOp) -> String {
    match op {
        QueriesPatchOp::Put(put) => {
            let ttl = put.ttl.map(|t| format!(",\"ttl\":{t}")).unwrap_or_default();
            format!(
                "{{\"op\":\"put\",\"hash\":{}{}}}",
                json_string(&put.hash),
                ttl
            )
        }
        QueriesPatchOp::Del(del) => {
            format!("{{\"op\":\"del\",\"hash\":{}}}", json_string(&del.hash))
        }
        QueriesPatchOp::Clear(_) => "{\"op\":\"clear\"}".to_string(),
    }
}

fn queries_patch_json(patch: &QueriesPatch) -> String {
    let items: Vec<String> = patch.iter().map(queries_patch_op_json).collect();
    format!("[{}]", items.join(","))
}

fn mutation_result_json(result: &MutationResult) -> String {
    match result {
        MutationResult::Ok(ok) => match &ok.data {
            Some(d) => format!("{{\"data\":{}}}", json_value_stringify(d)),
            None => "{}".to_string(),
        },
        MutationResult::Error(MutationError::App(err)) => {
            let message = err
                .message
                .as_ref()
                .map(|m| format!(",\"message\":{}", json_string(m)))
                .unwrap_or_default();
            let details = err
                .details
                .as_ref()
                .map(|d| format!(",\"details\":{}", json_value_stringify(d)))
                .unwrap_or_default();
            format!("{{\"error\":\"app\"{message}{details}}}")
        }
        MutationResult::Error(MutationError::Zero(err)) => {
            let kind = match err.error {
                crate::mutation_result::ZeroErrorKind::OooMutation => "oooMutation",
                crate::mutation_result::ZeroErrorKind::AlreadyProcessed => "alreadyProcessed",
            };
            let details = err
                .details
                .as_ref()
                .map(|d| format!(",\"details\":{}", json_value_stringify(d)))
                .unwrap_or_default();
            format!("{{\"error\":{}{details}}}", json_string(kind))
        }
    }
}

fn mutation_patch_op_json(op: &MutationPatchOp) -> String {
    match op {
        MutationPatchOp::Del(del) => format!(
            "{{\"op\":\"del\",\"id\":{{\"clientID\":{},\"id\":{}}}}}",
            json_string(&del.id.client_id),
            del.id.id
        ),
        MutationPatchOp::Put(put) => {
            format!(
                "{{\"op\":\"put\",\"mutation\":{{\"id\":{{\"clientID\":{},\"id\":{}}},\"result\":{}}}}}",
                json_string(&put.mutation.id.client_id),
                put.mutation.id.id,
                mutation_result_json(&put.mutation.result)
            )
        }
    }
}

fn mutations_patch_json(patch: &MutationsPatch) -> String {
    let items: Vec<String> = patch.iter().map(mutation_patch_op_json).collect();
    format!("[{}]", items.join(","))
}

fn poke_start_json(body: &PokeStartBody) -> String {
    format!(
        "[\"pokeStart\",{{\"pokeID\":{},\"baseCookie\":{}}}]",
        json_string(&body.poke_id),
        json_nullable_version(&body.base_cookie)
    )
}

fn poke_part_json(body: &PokePartBody) -> String {
    let mut fields = vec![format!("\"pokeID\":{}", json_string(&body.poke_id))];
    if let Some(lmids) = &body.last_mutation_id_changes {
        let items: Vec<String> = lmids
            .iter()
            .map(|(client_id, last_mutation_id)| {
                format!("{}:{last_mutation_id}", json_string(client_id))
            })
            .collect();
        fields.push(format!("\"lastMutationIDChanges\":{{{}}}", items.join(",")));
    }
    if let Some(desired) = &body.desired_queries_patches {
        let items: Vec<String> = desired
            .iter()
            .map(|(client_id, patch)| {
                format!("{}:{}", json_string(client_id), queries_patch_json(patch))
            })
            .collect();
        fields.push(format!("\"desiredQueriesPatches\":{{{}}}", items.join(",")));
    }
    if let Some(got) = &body.got_queries_patch {
        fields.push(format!("\"gotQueriesPatch\":{}", queries_patch_json(got)));
    }
    if let Some(rows_patch) = &body.rows_patch {
        let items: Vec<String> = rows_patch.iter().map(row_patch_op_json).collect();
        fields.push(format!("\"rowsPatch\":[{}]", items.join(",")));
    }
    if let Some(mutations_patch) = &body.mutations_patch {
        fields.push(format!(
            "\"mutationsPatch\":{}",
            mutations_patch_json(mutations_patch)
        ));
    }
    format!("[\"pokePart\",{{{}}}]", fields.join(","))
}

fn poke_end_json(body: &PokeEndBody) -> String {
    let mut fields = vec![
        format!("\"pokeID\":{}", json_string(&body.poke_id)),
        format!("\"cookie\":{}", json_string(&body.cookie)),
    ];
    if let Some(cancel) = body.cancel {
        fields.push(format!("\"cancel\":{cancel}"));
    }
    format!("[\"pokeEnd\",{{{}}}]", fields.join(","))
}

/// Serializes a [`PokeMessage`] to the wire's `[tag, body]` JSON tuple form.
pub fn poke_message_json(msg: &PokeMessage) -> String {
    match msg {
        PokeMessage::Start(body) => poke_start_json(body),
        PokeMessage::Part(body) => poke_part_json(body),
        PokeMessage::End(body) => poke_end_json(body),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row_patch::RowPutOp;

    #[test]
    fn poke_start_json_with_null_base_cookie() {
        let body = PokeStartBody {
            poke_id: "p1".into(),
            base_cookie: None,
            schema_versions: None,
            timestamp: None,
        };
        assert_eq!(
            poke_message_json(&PokeMessage::Start(body)),
            "[\"pokeStart\",{\"pokeID\":\"p1\",\"baseCookie\":null}]"
        );
    }

    #[test]
    fn poke_start_json_with_version_base_cookie() {
        let body = PokeStartBody {
            poke_id: "p1".into(),
            base_cookie: Some("00".into()),
            schema_versions: None,
            timestamp: None,
        };
        assert_eq!(
            poke_message_json(&PokeMessage::Start(body)),
            "[\"pokeStart\",{\"pokeID\":\"p1\",\"baseCookie\":\"00\"}]"
        );
    }

    #[test]
    fn poke_part_json_with_rows_patch_put() {
        let body = PokePartBody {
            poke_id: "p1".into(),
            rows_patch: Some(vec![RowPatchOp::Put(RowPutOp {
                table_name: "issues".into(),
                value: vec![
                    ("id".into(), JsonValue::String("1".into())),
                    ("active".into(), JsonValue::Bool(true)),
                ],
            })]),
            ..Default::default()
        };
        assert_eq!(
            poke_message_json(&PokeMessage::Part(body)),
            "[\"pokePart\",{\"pokeID\":\"p1\",\"rowsPatch\":[{\"op\":\"put\",\"tableName\":\"issues\",\"value\":{\"id\":\"1\",\"active\":true}}]}]"
        );
    }

    #[test]
    fn poke_part_json_with_last_mutation_id_changes() {
        use std::collections::BTreeMap;
        let mut lmids = BTreeMap::new();
        lmids.insert("c1".to_string(), 5.0);
        let body = PokePartBody {
            poke_id: "p1".into(),
            last_mutation_id_changes: Some(lmids),
            ..Default::default()
        };
        assert_eq!(
            poke_message_json(&PokeMessage::Part(body)),
            "[\"pokePart\",{\"pokeID\":\"p1\",\"lastMutationIDChanges\":{\"c1\":5}}]"
        );
    }

    #[test]
    fn poke_part_json_with_mutations_patch_del() {
        use crate::mutation_id::MutationId;
        use crate::mutations_patch::MutationDelOp;
        let body = PokePartBody {
            poke_id: "p1".into(),
            mutations_patch: Some(vec![MutationPatchOp::Del(MutationDelOp {
                id: MutationId {
                    id: 3.0,
                    client_id: "c1".into(),
                },
            })]),
            ..Default::default()
        };
        assert_eq!(poke_message_json(&PokeMessage::Part(body)), "[\"pokePart\",{\"pokeID\":\"p1\",\"mutationsPatch\":[{\"op\":\"del\",\"id\":{\"clientID\":\"c1\",\"id\":3}}]}]");
    }

    #[test]
    fn poke_part_json_with_mutations_patch_put_ok() {
        use crate::mutation_id::MutationId;
        use crate::mutation_result::{MutationOk, MutationResponse, MutationResult};
        use crate::mutations_patch::MutationPutOp;
        let body = PokePartBody {
            poke_id: "p1".into(),
            mutations_patch: Some(vec![MutationPatchOp::Put(MutationPutOp {
                mutation: MutationResponse {
                    id: MutationId {
                        id: 3.0,
                        client_id: "c1".into(),
                    },
                    result: MutationResult::Ok(MutationOk {
                        data: Some(JsonValue::Bool(true)),
                    }),
                },
            })]),
            ..Default::default()
        };
        assert_eq!(poke_message_json(&PokeMessage::Part(body)), "[\"pokePart\",{\"pokeID\":\"p1\",\"mutationsPatch\":[{\"op\":\"put\",\"mutation\":{\"id\":{\"clientID\":\"c1\",\"id\":3},\"result\":{\"data\":true}}}]}]");
    }

    #[test]
    fn poke_part_json_with_mutations_patch_put_app_error() {
        use crate::mutation_id::MutationId;
        use crate::mutation_result::{
            MutationAppError, MutationError, MutationResponse, MutationResult,
        };
        use crate::mutations_patch::MutationPutOp;
        let body = PokePartBody {
            poke_id: "p1".into(),
            mutations_patch: Some(vec![MutationPatchOp::Put(MutationPutOp {
                mutation: MutationResponse {
                    id: MutationId {
                        id: 3.0,
                        client_id: "c1".into(),
                    },
                    result: MutationResult::Error(MutationError::App(MutationAppError {
                        message: Some("oops".into()),
                        details: None,
                    })),
                },
            })]),
            ..Default::default()
        };
        assert_eq!(poke_message_json(&PokeMessage::Part(body)), "[\"pokePart\",{\"pokeID\":\"p1\",\"mutationsPatch\":[{\"op\":\"put\",\"mutation\":{\"id\":{\"clientID\":\"c1\",\"id\":3},\"result\":{\"error\":\"app\",\"message\":\"oops\"}}}]}]");
    }

    #[test]
    fn poke_part_json_with_got_queries_patch() {
        use crate::queries_patch::{QueriesDelOp, QueriesPutOp};
        let body = PokePartBody {
            poke_id: "p1".into(),
            got_queries_patch: Some(vec![
                QueriesPatchOp::Put(QueriesPutOp {
                    hash: "h1".into(),
                    ttl: None,
                }),
                QueriesPatchOp::Del(QueriesDelOp { hash: "h2".into() }),
            ]),
            ..Default::default()
        };
        assert_eq!(poke_message_json(&PokeMessage::Part(body)), "[\"pokePart\",{\"pokeID\":\"p1\",\"gotQueriesPatch\":[{\"op\":\"put\",\"hash\":\"h1\"},{\"op\":\"del\",\"hash\":\"h2\"}]}]");
    }

    #[test]
    fn poke_part_json_with_desired_queries_patches_keyed_by_client() {
        use crate::queries_patch::QueriesPutOp;
        use std::collections::BTreeMap;
        let mut desired = BTreeMap::new();
        desired.insert(
            "c1".to_string(),
            vec![QueriesPatchOp::Put(QueriesPutOp {
                hash: "h1".into(),
                ttl: Some(60.0),
            })],
        );
        let body = PokePartBody {
            poke_id: "p1".into(),
            desired_queries_patches: Some(desired),
            ..Default::default()
        };
        assert_eq!(poke_message_json(&PokeMessage::Part(body)), "[\"pokePart\",{\"pokeID\":\"p1\",\"desiredQueriesPatches\":{\"c1\":[{\"op\":\"put\",\"hash\":\"h1\",\"ttl\":60}]}}]");
    }

    #[test]
    fn poke_part_json_without_rows_patch_omits_field() {
        let body = PokePartBody {
            poke_id: "p1".into(),
            ..Default::default()
        };
        assert_eq!(
            poke_message_json(&PokeMessage::Part(body)),
            "[\"pokePart\",{\"pokeID\":\"p1\"}]"
        );
    }

    #[test]
    fn poke_end_json() {
        let body = PokeEndBody {
            poke_id: "p1".into(),
            cookie: "01".into(),
            cancel: None,
        };
        assert_eq!(
            poke_message_json(&PokeMessage::End(body)),
            "[\"pokeEnd\",{\"pokeID\":\"p1\",\"cookie\":\"01\"}]"
        );
    }

    #[test]
    fn poke_end_json_with_cancel() {
        let body = PokeEndBody {
            poke_id: "p1".into(),
            cookie: "01".into(),
            cancel: Some(true),
        };
        assert_eq!(
            poke_message_json(&PokeMessage::End(body)),
            "[\"pokeEnd\",{\"pokeID\":\"p1\",\"cookie\":\"01\",\"cancel\":true}]"
        );
    }
}
