//! Official Zero change-streamer v6 wire messages.

use base64::Engine;
use zero_cache_shared::bigint_json::{parse, JsonValue};
use zero_cache_sqlite::streamed_apply::StreamedChange;
use zero_cache_sqlite::Value;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum WireError {
    #[error("malformed wire message: {0}")]
    Malformed(String),
}

/// Official change-streamer v6 data-plane messages used on
/// `/replication/v6/changes`.
#[derive(Debug, PartialEq)]
pub enum OfficialMessage {
    Status,
    Begin,
    Data(StreamedChange),
    Commit { watermark: String },
    Error(String),
}

pub fn encode_official_status() -> String {
    r#"["status",{"tag":"status"}]"#.to_string()
}

pub fn encode_official_transaction(watermark: &str, changes: &[StreamedChange]) -> Vec<String> {
    let mut messages = Vec::with_capacity(changes.len() + 2);
    messages.push(
        serde_json::json!(["begin", {"tag": "begin"}, {"commitWatermark": watermark}]).to_string(),
    );
    for change in changes {
        let data = match change {
            StreamedChange::Set {
                table,
                row_key,
                row,
            } => {
                let key_columns: Vec<&str> =
                    row_key.iter().map(|(name, _)| name.as_str()).collect();
                let new = row
                    .iter()
                    .map(|(name, value)| (name.clone(), sqlite_value_to_serde(value)))
                    .collect::<serde_json::Map<_, _>>();
                serde_json::json!(["data", {
                    "tag": "update",
                    "relation": {"schema": "public", "name": table, "rowKey": {"columns": key_columns, "type": "default"}},
                    "key": null,
                    "new": new,
                }])
            }
            StreamedChange::Del { table, row_key } => {
                let key_columns: Vec<&str> =
                    row_key.iter().map(|(name, _)| name.as_str()).collect();
                let key = row_key
                    .iter()
                    .map(|(name, value)| (name.clone(), bigint_to_serde(value)))
                    .collect::<serde_json::Map<_, _>>();
                serde_json::json!(["data", {
                    "tag": "delete",
                    "relation": {"schema": "public", "name": table, "rowKey": {"columns": key_columns, "type": "default"}},
                    "key": key,
                }])
            }
        };
        messages.push(data.to_string());
    }
    messages.push(
        serde_json::json!(["commit", {"tag": "commit"}, {"watermark": watermark}]).to_string(),
    );
    messages
}

pub fn decode_official_message(text: &str) -> Result<OfficialMessage, WireError> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|error| WireError::Malformed(error.to_string()))?;
    let tuple = value
        .as_array()
        .ok_or_else(|| WireError::Malformed("not a tuple".into()))?;
    let tag = tuple
        .first()
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    match tag {
        "status" => Ok(OfficialMessage::Status),
        "begin" => Ok(OfficialMessage::Begin),
        "commit" => Ok(OfficialMessage::Commit {
            watermark: tuple
                .get(2)
                .and_then(|value| value.get("watermark"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
        }),
        "error" => Ok(OfficialMessage::Error(
            tuple
                .get(1)
                .and_then(|value| value.get("message"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("change-streamer error")
                .to_string(),
        )),
        "data" => decode_official_data(tuple.get(1).unwrap_or(&serde_json::Value::Null)),
        other => Err(WireError::Malformed(format!(
            "unknown official message {other}"
        ))),
    }
}

fn decode_official_data(data: &serde_json::Value) -> Result<OfficialMessage, WireError> {
    let operation = data
        .get("tag")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let relation = data
        .get("relation")
        .ok_or_else(|| WireError::Malformed("missing relation".into()))?;
    let table = relation
        .get("name")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| WireError::Malformed("missing relation name".into()))?
        .to_string();
    let key_columns: Vec<String> = relation
        .get("rowKey")
        .and_then(|value| value.get("columns"))
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::to_string)
        .collect();
    match operation {
        "insert" | "update" => {
            let new = data
                .get("new")
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| WireError::Malformed("missing new row".into()))?;
            let row: Vec<(String, Value)> = new
                .iter()
                .map(|(name, value)| (name.clone(), serde_to_sqlite_value(value)))
                .collect();
            let row_key = key_columns
                .iter()
                .filter_map(|name| {
                    new.get(name)
                        .map(|value| (name.clone(), serde_to_bigint(value)))
                })
                .collect();
            Ok(OfficialMessage::Data(StreamedChange::Set {
                table,
                row_key,
                row,
            }))
        }
        "delete" => {
            let key = data
                .get("key")
                .and_then(serde_json::Value::as_object)
                .ok_or_else(|| WireError::Malformed("missing delete key".into()))?;
            let row_key = key
                .iter()
                .map(|(name, value)| (name.clone(), serde_to_bigint(value)))
                .collect();
            Ok(OfficialMessage::Data(StreamedChange::Del {
                table,
                row_key,
            }))
        }
        other => Err(WireError::Malformed(format!(
            "unsupported data tag {other}"
        ))),
    }
}

fn sqlite_value_to_serde(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Integer(value) => (*value).into(),
        Value::Real(value) => serde_json::json!(value),
        Value::Text(value) => value.clone().into(),
        Value::Blob(value) => base64::engine::general_purpose::STANDARD
            .encode(value)
            .into(),
    }
}

fn bigint_to_serde(value: &JsonValue) -> serde_json::Value {
    serde_json::from_str(&value.stringify()).unwrap_or(serde_json::Value::Null)
}

fn serde_to_bigint(value: &serde_json::Value) -> JsonValue {
    parse(&value.to_string()).unwrap_or(JsonValue::Null)
}

fn serde_to_sqlite_value(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Integer(i64::from(*value)),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(Value::Integer)
            .or_else(|| value.as_f64().map(Value::Real))
            .unwrap_or(Value::Null),
        serde_json::Value::String(value) => Value::Text(value.clone()),
        other => Value::Text(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn official_transaction_round_trips() {
        let changes = vec![
            StreamedChange::Set {
                table: "issue".into(),
                row_key: vec![("id".into(), JsonValue::Number(1.0))],
                row: vec![
                    ("id".into(), Value::Integer(1)),
                    ("title".into(), Value::Text("hi".into())),
                    ("score".into(), Value::Real(2.5)),
                    ("note".into(), Value::Null),
                ],
            },
            StreamedChange::Del {
                table: "issue".into(),
                row_key: vec![("id".into(), JsonValue::Number(2.0))],
            },
        ];
        let messages = encode_official_transaction("07", &changes);
        assert!(matches!(
            decode_official_message(&messages[0]),
            Ok(OfficialMessage::Begin)
        ));
        assert!(matches!(
            decode_official_message(&messages[1]),
            Ok(OfficialMessage::Data(_))
        ));
        assert!(matches!(
            decode_official_message(&messages[2]),
            Ok(OfficialMessage::Data(_))
        ));
        assert_eq!(
            decode_official_message(&messages[3]).unwrap(),
            OfficialMessage::Commit {
                watermark: "07".into()
            }
        );
    }

    #[test]
    fn malformed_is_rejected() {
        assert!(decode_official_message("not json").is_err());
    }
}
