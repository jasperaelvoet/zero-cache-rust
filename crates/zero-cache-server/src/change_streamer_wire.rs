//! Wire protocol between a change-streamer node and view-syncer nodes.
//!
//! Text frames are JSON tuples `[tag, body]`; the snapshot payload rides as
//! binary frames between a `snapshot` header and `snapshotEnd`. Messages:
//!
//! * view-syncer → streamer: `["subscribe", {"since": "<watermark>"}]`
//! * streamer → view-syncer:
//!   - `["snapshot", {"watermark": "<w>", "bytes": N}]` + N raw bytes (binary
//!     frames) + `["snapshotEnd", {}]` — the initial replica for a fresh node.
//!   - `["commit", {"watermark": "<w>", "changes": [<change>…]}]` — one applied
//!     transaction; each `<change>` is `{"op":"set"|"del","table":…,"key":[…],
//!     "row":[…]}` (row present only for sets). Row/key values are typed so
//!     Integer/Real/Text/Blob/Null survive the round trip.

use base64::Engine;
use zero_cache_shared::bigint_json::{parse, JsonValue};
use zero_cache_sqlite::streamed_apply::StreamedChange;
use zero_cache_sqlite::Value;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum WireError {
    #[error("malformed wire message: {0}")]
    Malformed(String),
}

fn obj_get<'a>(v: &'a JsonValue, key: &str) -> Option<&'a JsonValue> {
    match v {
        JsonValue::Object(fields) => fields.iter().find(|(k, _)| k == key).map(|(_, val)| val),
        _ => None,
    }
}
fn as_str(v: &JsonValue) -> Option<&str> {
    match v {
        JsonValue::String(s) => Some(s),
        _ => None,
    }
}

// ---- typed row/key value <-> JSON --------------------------------------------

/// Encodes a rusqlite [`Value`] as a type-tagged JSON object so the exact SQLite
/// type is preserved across the wire.
fn value_to_json(v: &Value) -> JsonValue {
    let tagged = |k: &str, val: JsonValue| JsonValue::Object(vec![(k.to_string(), val)]);
    match v {
        Value::Integer(n) => tagged("i", JsonValue::Number(*n as f64)),
        Value::Real(f) => tagged("r", JsonValue::Number(*f)),
        Value::Text(s) => tagged("s", JsonValue::String(s.clone())),
        Value::Null => tagged("z", JsonValue::Null),
        Value::Blob(b) => tagged(
            "b",
            JsonValue::String(base64::engine::general_purpose::STANDARD.encode(b)),
        ),
    }
}

fn value_from_json(v: &JsonValue) -> Result<Value, WireError> {
    let bad = || WireError::Malformed("bad tagged value".into());
    if let Some(JsonValue::Number(n)) = obj_get(v, "i") {
        return Ok(Value::Integer(*n as i64));
    }
    if let Some(JsonValue::Number(f)) = obj_get(v, "r") {
        return Ok(Value::Real(*f));
    }
    if let Some(JsonValue::String(s)) = obj_get(v, "s") {
        return Ok(Value::Text(s.clone()));
    }
    if obj_get(v, "z").is_some() {
        return Ok(Value::Null);
    }
    if let Some(JsonValue::String(s)) = obj_get(v, "b") {
        return base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map(Value::Blob)
            .map_err(|_| bad());
    }
    Err(bad())
}

fn cols_to_json(cols: &[(String, Value)]) -> JsonValue {
    JsonValue::Array(
        cols.iter()
            .map(|(c, v)| JsonValue::Array(vec![JsonValue::String(c.clone()), value_to_json(v)]))
            .collect(),
    )
}
fn cols_from_json(v: &JsonValue) -> Result<Vec<(String, Value)>, WireError> {
    let JsonValue::Array(items) = v else {
        return Err(WireError::Malformed("expected array of columns".into()));
    };
    items
        .iter()
        .map(|pair| match pair {
            JsonValue::Array(kv) if kv.len() == 2 => {
                let col = as_str(&kv[0])
                    .ok_or_else(|| WireError::Malformed("column name".into()))?
                    .to_string();
                Ok((col, value_from_json(&kv[1])?))
            }
            _ => Err(WireError::Malformed("column pair".into())),
        })
        .collect()
}

/// The change-log `RowKey` (`Vec<(String, JsonValue)>`) is already JSON-native.
fn rowkey_to_json(key: &[(String, JsonValue)]) -> JsonValue {
    JsonValue::Array(
        key.iter()
            .map(|(c, v)| JsonValue::Array(vec![JsonValue::String(c.clone()), v.clone()]))
            .collect(),
    )
}
fn rowkey_from_json(v: &JsonValue) -> Result<Vec<(String, JsonValue)>, WireError> {
    let JsonValue::Array(items) = v else {
        return Err(WireError::Malformed("expected array key".into()));
    };
    items
        .iter()
        .map(|pair| match pair {
            JsonValue::Array(kv) if kv.len() == 2 => {
                let col = as_str(&kv[0])
                    .ok_or_else(|| WireError::Malformed("key name".into()))?
                    .to_string();
                Ok((col, kv[1].clone()))
            }
            _ => Err(WireError::Malformed("key pair".into())),
        })
        .collect()
}

// ---- messages ----------------------------------------------------------------

pub fn encode_subscribe(since: &str) -> String {
    JsonValue::Array(vec![
        JsonValue::String("subscribe".into()),
        JsonValue::Object(vec![("since".into(), JsonValue::String(since.into()))]),
    ])
    .stringify()
}

pub fn decode_subscribe(text: &str) -> Result<String, WireError> {
    let v = parse(text).map_err(|e| WireError::Malformed(e.to_string()))?;
    let JsonValue::Array(items) = &v else {
        return Err(WireError::Malformed("not a tuple".into()));
    };
    if items.first().and_then(as_str) != Some("subscribe") {
        return Err(WireError::Malformed("not subscribe".into()));
    }
    Ok(obj_get(&items[1], "since")
        .and_then(as_str)
        .unwrap_or("")
        .to_string())
}

pub fn encode_snapshot_header(watermark: &str, bytes: usize) -> String {
    JsonValue::Array(vec![
        JsonValue::String("snapshot".into()),
        JsonValue::Object(vec![
            ("watermark".into(), JsonValue::String(watermark.into())),
            ("bytes".into(), JsonValue::Number(bytes as f64)),
        ]),
    ])
    .stringify()
}

pub fn encode_snapshot_end() -> String {
    r#"["snapshotEnd",{}]"#.to_string()
}

pub fn encode_commit(watermark: &str, changes: &[StreamedChange]) -> String {
    let changes_json: Vec<JsonValue> = changes
        .iter()
        .map(|c| match c {
            StreamedChange::Set {
                table,
                row_key,
                row,
            } => JsonValue::Object(vec![
                ("op".into(), JsonValue::String("set".into())),
                ("table".into(), JsonValue::String(table.clone())),
                ("key".into(), rowkey_to_json(row_key)),
                ("row".into(), cols_to_json(row)),
            ]),
            StreamedChange::Del { table, row_key } => JsonValue::Object(vec![
                ("op".into(), JsonValue::String("del".into())),
                ("table".into(), JsonValue::String(table.clone())),
                ("key".into(), rowkey_to_json(row_key)),
            ]),
        })
        .collect();
    JsonValue::Array(vec![
        JsonValue::String("commit".into()),
        JsonValue::Object(vec![
            ("watermark".into(), JsonValue::String(watermark.into())),
            ("changes".into(), JsonValue::Array(changes_json)),
        ]),
    ])
    .stringify()
}

/// A decoded streamer→view-syncer message.
#[derive(Debug, PartialEq)]
pub enum StreamerMessage {
    SnapshotHeader {
        watermark: String,
        bytes: usize,
    },
    SnapshotEnd,
    Commit {
        watermark: String,
        changes: Vec<StreamedChange>,
    },
}

pub fn decode_streamer_message(text: &str) -> Result<StreamerMessage, WireError> {
    let v = parse(text).map_err(|e| WireError::Malformed(e.to_string()))?;
    let JsonValue::Array(items) = &v else {
        return Err(WireError::Malformed("not a tuple".into()));
    };
    match items.first().and_then(as_str) {
        Some("snapshot") => {
            let watermark = obj_get(&items[1], "watermark")
                .and_then(as_str)
                .unwrap_or("")
                .to_string();
            let bytes = match obj_get(&items[1], "bytes") {
                Some(JsonValue::Number(n)) => *n as usize,
                _ => return Err(WireError::Malformed("bytes".into())),
            };
            Ok(StreamerMessage::SnapshotHeader { watermark, bytes })
        }
        Some("snapshotEnd") => Ok(StreamerMessage::SnapshotEnd),
        Some("commit") => {
            let watermark = obj_get(&items[1], "watermark")
                .and_then(as_str)
                .unwrap_or("")
                .to_string();
            let Some(JsonValue::Array(changes_json)) = obj_get(&items[1], "changes") else {
                return Err(WireError::Malformed("changes".into()));
            };
            let mut changes = Vec::with_capacity(changes_json.len());
            for c in changes_json {
                let op = obj_get(c, "op").and_then(as_str);
                let table = obj_get(c, "table")
                    .and_then(as_str)
                    .ok_or_else(|| WireError::Malformed("table".into()))?
                    .to_string();
                let row_key = rowkey_from_json(
                    obj_get(c, "key").ok_or_else(|| WireError::Malformed("key".into()))?,
                )?;
                match op {
                    Some("set") => {
                        let row = cols_from_json(
                            obj_get(c, "row").ok_or_else(|| WireError::Malformed("row".into()))?,
                        )?;
                        changes.push(StreamedChange::Set {
                            table,
                            row_key,
                            row,
                        });
                    }
                    Some("del") => changes.push(StreamedChange::Del { table, row_key }),
                    _ => return Err(WireError::Malformed("op".into())),
                }
            }
            Ok(StreamerMessage::Commit { watermark, changes })
        }
        _ => Err(WireError::Malformed("unknown tag".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscribe_round_trips() {
        assert_eq!(decode_subscribe(&encode_subscribe("01")).unwrap(), "01");
    }

    #[test]
    fn snapshot_header_round_trips() {
        let m = decode_streamer_message(&encode_snapshot_header("0A", 4096)).unwrap();
        assert_eq!(
            m,
            StreamerMessage::SnapshotHeader {
                watermark: "0A".into(),
                bytes: 4096
            }
        );
        assert_eq!(
            decode_streamer_message(&encode_snapshot_end()).unwrap(),
            StreamerMessage::SnapshotEnd
        );
    }

    #[test]
    fn commit_round_trips_with_typed_values() {
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
        let text = encode_commit("07", &changes);
        let StreamerMessage::Commit {
            watermark,
            changes: got,
        } = decode_streamer_message(&text).unwrap()
        else {
            panic!("expected commit");
        };
        assert_eq!(watermark, "07");
        assert_eq!(
            got, changes,
            "changes survive the round trip with exact types"
        );
    }

    #[test]
    fn malformed_is_rejected() {
        assert!(decode_streamer_message("not json").is_err());
        assert!(decode_subscribe(r#"["nope",{}]"#).is_err());
    }
}
