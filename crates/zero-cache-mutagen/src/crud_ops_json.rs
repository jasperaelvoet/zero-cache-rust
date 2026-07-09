//! Decodes the raw `ops` JSON array a `push` message's CRUD mutation carries
//! (`zero_cache_protocol::push::CrudMutation::ops_json`) into this crate's
//! [`CrudOp`] — the missing link between the wire decode
//! (`up_json`/`push_json`, which deliberately leave this array un-decoded
//! since `CrudOp` lives here, not in `zero-cache-protocol`) and
//! `orchestration::plan_mutation_sql`/`apply_mutation::apply_crud_mutation`,
//! which consume `CrudOp` directly.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::JsonValue;

use crate::crud_ops::{CrudOp, DeleteOp, InsertOp, PrimaryKeyValueRecord, Row, UpdateOp, UpsertOp};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("crud op JSON: {0}")]
pub struct CrudOpJsonError(pub String);

type R<T> = Result<T, CrudOpJsonError>;

fn err<T>(m: impl Into<String>) -> R<T> {
    Err(CrudOpJsonError(m.into()))
}
fn field<'a>(o: &'a JsonValue, k: &str) -> Option<&'a JsonValue> {
    match o {
        JsonValue::Object(es) => es.iter().find(|(n, _)| n == k).map(|(_, v)| v),
        _ => None,
    }
}
fn req<'a>(o: &'a JsonValue, k: &str) -> R<&'a JsonValue> {
    field(o, k).ok_or_else(|| CrudOpJsonError(format!("missing field {k:?}")))
}
fn as_str(v: &JsonValue) -> R<String> {
    match v {
        JsonValue::String(s) => Ok(s.clone()),
        other => err(format!("expected string, got {other:?}")),
    }
}
fn as_array(v: &JsonValue) -> R<&[JsonValue]> {
    match v {
        JsonValue::Array(a) => Ok(a),
        other => err(format!("expected array, got {other:?}")),
    }
}
fn as_object(v: &JsonValue) -> R<&[(String, JsonValue)]> {
    match v {
        JsonValue::Object(es) => Ok(es),
        other => err(format!("expected object, got {other:?}")),
    }
}
fn str_array(v: &JsonValue) -> R<Vec<String>> {
    as_array(v)?.iter().map(as_str).collect()
}
fn row_from_json(v: &JsonValue) -> R<Row> {
    Ok(as_object(v)?.to_vec())
}
fn pk_record_from_json(v: &JsonValue) -> R<PrimaryKeyValueRecord> {
    Ok(as_object(v)?.iter().cloned().collect::<BTreeMap<_, _>>())
}

/// Parses one element of the `ops` array (`insertOpSchema | upsertOpSchema |
/// updateOpSchema | deleteOpSchema`, discriminated by `op`).
pub fn crud_op_from_json(v: &JsonValue) -> R<CrudOp> {
    let op = as_str(req(v, "op")?)?;
    let table_name = as_str(req(v, "tableName")?)?;
    let primary_key = str_array(req(v, "primaryKey")?)?;
    match op.as_str() {
        "insert" => Ok(CrudOp::Insert(InsertOp {
            table_name,
            primary_key,
            value: row_from_json(req(v, "value")?)?,
        })),
        "upsert" => Ok(CrudOp::Upsert(UpsertOp {
            table_name,
            primary_key,
            value: row_from_json(req(v, "value")?)?,
        })),
        "update" => Ok(CrudOp::Update(UpdateOp {
            table_name,
            primary_key,
            value: row_from_json(req(v, "value")?)?,
        })),
        "delete" => Ok(CrudOp::Delete(DeleteOp {
            table_name,
            primary_key,
            value: pk_record_from_json(req(v, "value")?)?,
        })),
        other => err(format!("unknown crud op {other:?}")),
    }
}

/// Parses a whole `ops` array (the raw `CrudMutation::ops_json`).
pub fn crud_ops_from_json(v: &JsonValue) -> R<Vec<CrudOp>> {
    as_array(v)?.iter().map(crud_op_from_json).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_shared::bigint_json::parse;

    #[test]
    fn decodes_insert_upsert_update_delete() {
        let json = parse(
            r#"[
              {"op":"insert","tableName":"issue","primaryKey":["id"],"value":{"id":"1","title":"a"}},
              {"op":"upsert","tableName":"issue","primaryKey":["id"],"value":{"id":"2","title":"b"}},
              {"op":"update","tableName":"issue","primaryKey":["id"],"value":{"id":"1","title":"c"}},
              {"op":"delete","tableName":"issue","primaryKey":["id"],"value":{"id":"2"}}
            ]"#,
        )
        .unwrap();
        let ops = crud_ops_from_json(&json).unwrap();
        assert_eq!(ops.len(), 4);
        assert!(matches!(ops[0], CrudOp::Insert(_)));
        assert!(matches!(ops[1], CrudOp::Upsert(_)));
        assert!(matches!(ops[2], CrudOp::Update(_)));
        assert!(matches!(ops[3], CrudOp::Delete(_)));
        let CrudOp::Insert(i) = &ops[0] else { panic!() };
        assert_eq!(i.table_name, "issue");
        assert_eq!(i.primary_key, vec!["id".to_string()]);
        assert_eq!(
            i.value[0],
            ("id".to_string(), JsonValue::String("1".into()))
        );
    }

    #[test]
    fn unknown_op_errors() {
        let json =
            parse(r#"[{"op":"weird","tableName":"t","primaryKey":["id"],"value":{}}]"#).unwrap();
        assert!(crud_ops_from_json(&json).is_err());
    }
}
