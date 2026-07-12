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

use zero_cache_shared::bigint_json::{write_object, write_string, write_value};

use crate::mutation_result::{MutationError, MutationResult};
use crate::mutations_patch::{MutationPatchOp, MutationsPatch};
use crate::poke::{PokeEndBody, PokeMessage, PokePartBody, PokeStartBody};
use crate::queries_patch::{QueriesPatch, QueriesPatchOp};
use crate::row_patch::{PrimaryKeyValueRecord, RowPatchOp};
use crate::version::NullableVersion;

// The whole module serializes by APPENDING into one caller-owned `String`
// buffer rather than building an owned `String` (via `format!`) per field/op
// and joining. On a hydration `rowsPatch` of 1000 rows this turns thousands of
// throwaway allocations (and a full clone of every row into an owned
// `JsonValue::Object`) into a single growing buffer — the dominant per-
// connection hydration CPU cost. Byte output is unchanged (the `mod tests`
// assertions and byte-for-byte conformance pin that).

fn write_json_string(s: &str, out: &mut String) {
    // Reuses `JsonValue::String`'s own escaping rather than duplicating it.
    write_string(s, out);
}

fn write_nullable_version(v: &NullableVersion, out: &mut String) {
    match v {
        Some(s) => write_json_string(s, out),
        None => out.push_str("null"),
    }
}

/// Writes an id record (`BTreeMap<String, JsonValue>`) as a JSON object without
/// cloning it into an owned `Row`/`JsonValue::Object` first. Byte-identical to
/// the previous `json_row(&id.iter().map(clone).collect())`: a `BTreeMap`
/// iterates in the same sorted-key order the old `collect()` preserved.
fn write_id_object(id: &PrimaryKeyValueRecord, out: &mut String) {
    out.push('{');
    let mut first = true;
    for (k, v) in id {
        if !first {
            out.push(',');
        }
        first = false;
        write_json_string(k, out);
        out.push(':');
        write_value(v, out);
    }
    out.push('}');
}

fn write_row_patch_op(op: &RowPatchOp, out: &mut String) {
    match op {
        RowPatchOp::Put(put) => {
            out.push_str("{\"op\":\"put\",\"tableName\":");
            write_json_string(&put.table_name, out);
            out.push_str(",\"value\":");
            write_object(&put.value, out);
            out.push('}');
        }
        RowPatchOp::Update(update) => {
            out.push_str("{\"op\":\"update\",\"tableName\":");
            write_json_string(&update.table_name, out);
            out.push_str(",\"id\":");
            write_id_object(&update.id, out);
            out.push('}');
        }
        RowPatchOp::Del(del) => {
            out.push_str("{\"op\":\"del\",\"tableName\":");
            write_json_string(&del.table_name, out);
            out.push_str(",\"id\":");
            write_id_object(&del.id, out);
            out.push('}');
        }
        RowPatchOp::Clear(_) => out.push_str("{\"op\":\"clear\"}"),
    }
}

fn write_queries_patch_op(op: &QueriesPatchOp, out: &mut String) {
    match op {
        QueriesPatchOp::Put(put) => {
            out.push_str("{\"op\":\"put\",\"hash\":");
            write_json_string(&put.hash, out);
            if let Some(t) = put.ttl {
                out.push_str(",\"ttl\":");
                out.push_str(&t.to_string());
            }
            out.push('}');
        }
        QueriesPatchOp::Del(del) => {
            out.push_str("{\"op\":\"del\",\"hash\":");
            write_json_string(&del.hash, out);
            out.push('}');
        }
        QueriesPatchOp::Clear(_) => out.push_str("{\"op\":\"clear\"}"),
    }
}

fn write_queries_patch(patch: &QueriesPatch, out: &mut String) {
    out.push('[');
    for (i, op) in patch.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_queries_patch_op(op, out);
    }
    out.push(']');
}

fn write_mutation_result(result: &MutationResult, out: &mut String) {
    match result {
        MutationResult::Ok(ok) => match &ok.data {
            Some(d) => {
                out.push_str("{\"data\":");
                write_value(d, out);
                out.push('}');
            }
            None => out.push_str("{}"),
        },
        MutationResult::Error(MutationError::App(err)) => {
            out.push_str("{\"error\":\"app\"");
            if let Some(m) = err.message.as_ref() {
                out.push_str(",\"message\":");
                write_json_string(m, out);
            }
            if let Some(d) = err.details.as_ref() {
                out.push_str(",\"details\":");
                write_value(d, out);
            }
            out.push('}');
        }
        MutationResult::Error(MutationError::Zero(err)) => {
            let kind = match err.error {
                crate::mutation_result::ZeroErrorKind::OooMutation => "oooMutation",
                crate::mutation_result::ZeroErrorKind::AlreadyProcessed => "alreadyProcessed",
            };
            out.push_str("{\"error\":");
            write_json_string(kind, out);
            if let Some(d) = err.details.as_ref() {
                out.push_str(",\"details\":");
                write_value(d, out);
            }
            out.push('}');
        }
    }
}

fn write_mutation_patch_op(op: &MutationPatchOp, out: &mut String) {
    match op {
        MutationPatchOp::Del(del) => {
            out.push_str("{\"op\":\"del\",\"id\":{\"clientID\":");
            write_json_string(&del.id.client_id, out);
            out.push_str(",\"id\":");
            out.push_str(&del.id.id.to_string());
            out.push_str("}}");
        }
        MutationPatchOp::Put(put) => {
            out.push_str("{\"op\":\"put\",\"mutation\":{\"id\":{\"clientID\":");
            write_json_string(&put.mutation.id.client_id, out);
            out.push_str(",\"id\":");
            out.push_str(&put.mutation.id.id.to_string());
            out.push_str("},\"result\":");
            write_mutation_result(&put.mutation.result, out);
            out.push_str("}}");
        }
    }
}

fn write_mutations_patch(patch: &MutationsPatch, out: &mut String) {
    out.push('[');
    for (i, op) in patch.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_mutation_patch_op(op, out);
    }
    out.push(']');
}

fn write_poke_start(body: &PokeStartBody, out: &mut String) {
    out.push_str("[\"pokeStart\",{\"pokeID\":");
    write_json_string(&body.poke_id, out);
    out.push_str(",\"baseCookie\":");
    write_nullable_version(&body.base_cookie, out);
    out.push_str("}]");
}

fn write_poke_part(body: &PokePartBody, out: &mut String) {
    out.push_str("[\"pokePart\",{\"pokeID\":");
    write_json_string(&body.poke_id, out);
    if let Some(lmids) = &body.last_mutation_id_changes {
        out.push_str(",\"lastMutationIDChanges\":{");
        for (i, (client_id, last_mutation_id)) in lmids.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_json_string(client_id, out);
            out.push(':');
            out.push_str(&last_mutation_id.to_string());
        }
        out.push('}');
    }
    if let Some(desired) = &body.desired_queries_patches {
        out.push_str(",\"desiredQueriesPatches\":{");
        for (i, (client_id, patch)) in desired.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_json_string(client_id, out);
            out.push(':');
            write_queries_patch(patch, out);
        }
        out.push('}');
    }
    if let Some(got) = &body.got_queries_patch {
        out.push_str(",\"gotQueriesPatch\":");
        write_queries_patch(got, out);
    }
    if let Some(rows_patch) = &body.rows_patch {
        out.push_str(",\"rowsPatch\":[");
        for (i, op) in rows_patch.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            write_row_patch_op(op, out);
        }
        out.push(']');
    }
    if let Some(mutations_patch) = &body.mutations_patch {
        out.push_str(",\"mutationsPatch\":");
        write_mutations_patch(mutations_patch, out);
    }
    out.push_str("}]");
}

fn write_poke_end(body: &PokeEndBody, out: &mut String) {
    out.push_str("[\"pokeEnd\",{\"pokeID\":");
    write_json_string(&body.poke_id, out);
    out.push_str(",\"cookie\":");
    write_json_string(&body.cookie, out);
    if let Some(cancel) = body.cancel {
        out.push_str(",\"cancel\":");
        out.push_str(if cancel { "true" } else { "false" });
    }
    out.push_str("}]");
}

/// Serializes a [`PokeMessage`] to the wire's `[tag, body]` JSON tuple form.
pub fn poke_message_json(msg: &PokeMessage) -> String {
    let mut out = String::new();
    match msg {
        PokeMessage::Start(body) => write_poke_start(body, &mut out),
        PokeMessage::Part(body) => write_poke_part(body, &mut out),
        PokeMessage::End(body) => write_poke_end(body, &mut out),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row_patch::RowPutOp;
    use zero_cache_shared::bigint_json::JsonValue;

    /// Micro-times serialization of a 1000-row `rowsPatch` poke — the shape a
    /// full-table hydration produces. Run with `--ignored --nocapture`.
    #[test]
    #[ignore]
    fn time_poke_part_serialize_1000_rows() {
        let rows: Vec<RowPatchOp> = (0..1000)
            .map(|i| {
                RowPatchOp::Put(RowPutOp {
                    table_name: "issue".into(),
                    value: vec![
                        ("id".into(), JsonValue::String(format!("issue-{i}"))),
                        ("title".into(), JsonValue::String(format!("title of {i}"))),
                        ("owner".into(), JsonValue::String(format!("owner-{i}"))),
                        ("open".into(), JsonValue::Bool(i % 2 == 0)),
                        ("rank".into(), JsonValue::Number(i as f64)),
                        ("_0_version".into(), JsonValue::String("00".into())),
                    ],
                })
            })
            .collect();
        let body = PokePartBody {
            poke_id: "p1".into(),
            rows_patch: Some(rows),
            ..Default::default()
        };
        let msg = PokeMessage::Part(body);
        // Warm up.
        for _ in 0..10 {
            std::hint::black_box(poke_message_json(&msg));
        }
        let iters = 500;
        let start = std::time::Instant::now();
        for _ in 0..iters {
            std::hint::black_box(poke_message_json(&msg));
        }
        let elapsed = start.elapsed();
        let per = elapsed.as_secs_f64() * 1000.0 / iters as f64;
        println!(
            "\npoke_message_json 1000-row rowsPatch x {iters} = {:?} => {per:.3} ms/poke ({:.2} us/row)",
            elapsed,
            per * 1000.0 / 1000.0
        );
    }

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

    /// The PREVIOUS (pre-buffer) serializer, kept verbatim as an oracle. The
    /// buffer-appending `poke_message_json` must produce byte-identical output
    /// to this over a broad corpus — this guards the wire protocol when Docker
    /// conformance can't run (the byte-for-byte scenarios pin the same thing).
    mod reference_impl {
        use crate::mutation_result::{MutationError, MutationResult};
        use crate::mutations_patch::{MutationPatchOp, MutationsPatch};
        use crate::poke::{PokeEndBody, PokeMessage, PokePartBody, PokeStartBody};
        use crate::queries_patch::{QueriesPatch, QueriesPatchOp};
        use crate::row_patch::{Row, RowPatchOp};
        use crate::version::NullableVersion;
        use zero_cache_shared::bigint_json::{stringify as json_value_stringify, JsonValue};

        fn json_string(s: &str) -> String {
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
                RowPatchOp::Put(put) => format!(
                    "{{\"op\":\"put\",\"tableName\":{},\"value\":{}}}",
                    json_string(&put.table_name),
                    json_row(&put.value)
                ),
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
                    let id =
                        json_row(&del.id.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
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
                        crate::mutation_result::ZeroErrorKind::AlreadyProcessed => {
                            "alreadyProcessed"
                        }
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
                MutationPatchOp::Put(put) => format!(
                    "{{\"op\":\"put\",\"mutation\":{{\"id\":{{\"clientID\":{},\"id\":{}}},\"result\":{}}}}}",
                    json_string(&put.mutation.id.client_id),
                    put.mutation.id.id,
                    mutation_result_json(&put.mutation.result)
                ),
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
        pub fn poke_message_json(msg: &PokeMessage) -> String {
            match msg {
                PokeMessage::Start(body) => poke_start_json(body),
                PokeMessage::Part(body) => poke_part_json(body),
                PokeMessage::End(body) => poke_end_json(body),
            }
        }
    }

    #[test]
    fn buffer_serializer_is_byte_identical_to_the_old_format_join_serializer() {
        use crate::mutation_id::MutationId;
        use crate::mutation_result::{
            MutationAppError, MutationError, MutationOk, MutationResponse, MutationResult,
            MutationZeroError, ZeroErrorKind,
        };
        use crate::mutations_patch::{MutationDelOp, MutationPatchOp, MutationPutOp};
        use crate::queries_patch::{QueriesClearOp, QueriesDelOp, QueriesPatchOp, QueriesPutOp};
        use crate::row_patch::{Row, RowClearOp, RowDelOp, RowUpdateOp};
        use std::collections::BTreeMap;

        // Values chosen to exercise every serializer branch AND every escaping /
        // number-format edge: quotes, backslash, control chars, unicode, ints,
        // negatives, fractionals, bools, null, nested arrays/objects, bigint.
        let tricky_strings = [
            "",
            "plain",
            "with \"quotes\"",
            "back\\slash",
            "tab\tnewline\nreturn\r",
            "ctrl\u{0008}\u{000C}",
            "unicode-\u{00e9}\u{1f600}",
            "colon:comma,brace}bracket]",
        ];
        let tricky_values = [
            JsonValue::Null,
            JsonValue::Bool(true),
            JsonValue::Bool(false),
            JsonValue::Number(0.0),
            JsonValue::Number(-1.0),
            JsonValue::Number(42.0),
            JsonValue::Number(3.5),
            JsonValue::Number(-2.25),
            JsonValue::String("nested \"str\"".into()),
            JsonValue::Array(vec![
                JsonValue::Number(1.0),
                JsonValue::String("a,b".into()),
                JsonValue::Null,
            ]),
            JsonValue::Object(vec![
                ("k1".into(), JsonValue::Bool(true)),
                ("k\"2".into(), JsonValue::Number(9.0)),
            ]),
        ];

        let make_row = |seed: usize| -> Row {
            (0..4)
                .map(|f| {
                    (
                        tricky_strings[(seed + f) % tricky_strings.len()].to_string(),
                        tricky_values[(seed * 3 + f) % tricky_values.len()].clone(),
                    )
                })
                .collect()
        };
        let make_id = |seed: usize| -> BTreeMap<String, JsonValue> {
            (0..2)
                .map(|f| {
                    (
                        format!("pk{}", (seed + f) % 3),
                        tricky_values[(seed + f) % tricky_values.len()].clone(),
                    )
                })
                .collect()
        };

        let mut corpus: Vec<PokeMessage> = Vec::new();

        // pokeStart: null and version base cookies.
        for bc in [None, Some("01".to_string()), Some("\"weird\"".to_string())] {
            corpus.push(PokeMessage::Start(PokeStartBody {
                poke_id: "p:\"1\"".into(),
                base_cookie: bc,
                schema_versions: None,
                timestamp: None,
            }));
        }

        // pokeEnd: with and without cancel.
        for cancel in [None, Some(true), Some(false)] {
            corpus.push(PokeMessage::End(PokeEndBody {
                poke_id: "p1".into(),
                cookie: "co\\okie".into(),
                cancel,
            }));
        }

        // pokePart: rows (put/update/del/clear), queries, mutations, lmids, desired.
        for seed in 0..24usize {
            let rows = vec![
                RowPatchOp::Put(RowPutOp {
                    table_name: tricky_strings[seed % tricky_strings.len()].to_string(),
                    value: make_row(seed),
                }),
                RowPatchOp::Update(RowUpdateOp {
                    table_name: "issue".into(),
                    id: make_id(seed),
                    merge: None,
                    constrain: None,
                }),
                RowPatchOp::Del(RowDelOp {
                    table_name: "issue".into(),
                    id: make_id(seed + 1),
                }),
                RowPatchOp::Clear(RowClearOp),
            ];
            let got = vec![
                QueriesPatchOp::Put(QueriesPutOp {
                    hash: tricky_strings[(seed + 2) % tricky_strings.len()].to_string(),
                    ttl: if seed % 2 == 0 { Some(1000.0) } else { None },
                }),
                QueriesPatchOp::Del(QueriesDelOp { hash: "h2".into() }),
                QueriesPatchOp::Clear(QueriesClearOp),
            ];
            let mut lmids = BTreeMap::new();
            lmids.insert("client\"a".to_string(), 5.0);
            lmids.insert("clientb".to_string(), 12.0);
            let mut desired = BTreeMap::new();
            desired.insert("cli:1".to_string(), got.clone());

            let mutations = vec![
                MutationPatchOp::Del(MutationDelOp {
                    id: MutationId {
                        id: 3.0,
                        client_id: "c\"1".into(),
                    },
                }),
                MutationPatchOp::Put(MutationPutOp {
                    mutation: MutationResponse {
                        id: MutationId {
                            id: 7.0,
                            client_id: "c2".into(),
                        },
                        result: match seed % 4 {
                            0 => MutationResult::Ok(MutationOk {
                                data: Some(tricky_values[seed % tricky_values.len()].clone()),
                            }),
                            1 => MutationResult::Ok(MutationOk { data: None }),
                            2 => MutationResult::Error(MutationError::App(MutationAppError {
                                message: Some("boom \"x\"".into()),
                                details: Some(JsonValue::Array(vec![JsonValue::Number(1.0)])),
                            })),
                            _ => MutationResult::Error(MutationError::Zero(MutationZeroError {
                                error: if seed % 8 < 4 {
                                    ZeroErrorKind::OooMutation
                                } else {
                                    ZeroErrorKind::AlreadyProcessed
                                },
                                details: Some(JsonValue::String("d".into())),
                            })),
                        },
                    },
                }),
            ];

            corpus.push(PokeMessage::Part(PokePartBody {
                poke_id: format!("p{seed}"),
                rows_patch: Some(rows),
                got_queries_patch: Some(got),
                desired_queries_patches: Some(desired),
                last_mutation_id_changes: Some(lmids),
                mutations_patch: Some(mutations),
            }));
        }

        for msg in &corpus {
            assert_eq!(
                poke_message_json(msg),
                reference_impl::poke_message_json(msg),
                "buffer serializer diverged from the old serializer for {msg:?}"
            );
        }
    }
}
