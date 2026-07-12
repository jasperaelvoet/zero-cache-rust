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
}
