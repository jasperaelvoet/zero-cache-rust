//! Official Zero change-streamer v6 wire messages.
//!
//! Wire shape mirrors `mono-src/packages/zero-cache/src/services/change-source/
//! protocol/current/{data.ts,downstream.ts}` and `.../change-streamer/
//! change-streamer.ts`:
//!
//! * a transaction is `begin` … `data`* … (`commit` | `rollback`);
//! * a `data` payload is one of the `dataChangeSchema` variants —
//!   `insert` (no `key`), `update` (nullable `key`), `delete` (`key`),
//!   `truncate` — or a `schemaChangeSchema` (DDL) variant
//!   (`create-table`/`add-column`/`drop-column`/`drop-table`/`rename-table`/
//!   `create-index`/`drop-index`/`backfill`/`backfill-completed`).
//!
//! ## What this port drives end-to-end today
//! The port's durable change-log (`_zero.changeLog2`) coalesces to a single
//! latest op per row and records only `s` (set), `d` (delete) and `t`
//! (truncate). It never records inline DDL (schema drift triggers a full
//! resync — see `PORTING.md`/H5) nor `rollback` (rolled-back transactions never
//! reach the log). So:
//!   * `delete` and `truncate` are produced from real ops and encoded here;
//!   * a coalesced `s` op cannot be distinguished as an insert vs. an update
//!     (that history is exactly what the out-of-scope durable-CDC store would
//!     retain), so it is encoded as an `update` carrying the full `new` row and
//!     a null `key` — the wire-safe superset that an official downstream
//!     upserts correctly;
//!   * `insert`, `rollback` and the DDL variants have full encode support and
//!     round-trip through the encoder, but the port has no source that emits
//!     them yet.
//!
//! ## Receive side
//! The port's own view-syncer applies streamed commits through
//! [`zero_cache_sqlite::streamed_apply`]. `insert`/`update`/`delete` decode into
//! [`OfficialMessage::Data`] (a [`StreamedChange`]); `truncate` decodes into
//! [`OfficialMessage::Truncate`] and is APPLIED (the relation's rows are
//! deleted) rather than dropped; `rollback` decodes into
//! [`OfficialMessage::Rollback`] and discards the in-flight transaction's
//! buffered changes. Only inline DDL still forces a resync
//! ([`OfficialMessage::SchemaChange`] → the receiver tears down and
//! re-bootstraps), because applying schema drift in place is out of scope (H5).

use zero_cache_shared::bigint_json::{parse, JsonValue};
use zero_cache_sqlite::change_log::RowKey;
use zero_cache_sqlite::streamed_apply::StreamedChange;

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum WireError {
    #[error("malformed wire message: {0}")]
    Malformed(String),
    /// A wire-valid message this port's receive side cannot apply against its
    /// coalescing replica (`truncate`/`rollback`/DDL). Not silently dropped —
    /// the caller turns it into a protocol error so the subscriber resyncs.
    #[error("unsupported change on receive side: {0}")]
    Unsupported(String),
}

/// A single change destined for the `["data", …]` frame of a streamed
/// transaction. Distinguishes `insert` from `update` (H6(a)) and carries the
/// `truncate` / DDL variants (H6(b)) so nothing is silently dropped.
#[derive(Debug, Clone, PartialEq)]
pub enum WireChange {
    /// `dataChangeSchema` `insert` — a brand-new row; emitted with no `key`.
    /// Row values carry their declared ZQL type ([`JsonValue`]) so booleans/JSON
    /// caught up over the wire restore as `true`/`false`/parsed values (L4).
    Insert {
        table: String,
        row_key: RowKey,
        row: Vec<(String, JsonValue)>,
    },
    /// `dataChangeSchema` `update` — `key` is `None` (serialized `null`) unless
    /// the update changed the row's key (or replica identity is `full`), in
    /// which case it carries the OLD key.
    Update {
        table: String,
        row_key: RowKey,
        key: Option<RowKey>,
        row: Vec<(String, JsonValue)>,
    },
    /// `dataChangeSchema` `delete`.
    Delete { table: String, row_key: RowKey },
    /// `dataChangeSchema` `truncate` — every row of `table` is removed.
    Truncate { table: String },
    /// A `schemaChangeSchema` (DDL) payload carried verbatim. The port has no
    /// source that produces these inline yet, but the variant + encode keeps
    /// the wire union complete so a future source is not silently dropped.
    SchemaChange(serde_json::Value),
}

/// Official change-streamer v6 data-plane messages used on
/// `/replication/v6/changes`.
#[derive(Debug, PartialEq)]
pub enum OfficialMessage {
    Status,
    Begin,
    Data(StreamedChange),
    /// `truncate` — the named relations are cleared. Carried as its own message
    /// (rather than an unsupported error) so a downstream view-syncer applies it
    /// instead of resyncing (H6(b)). Upstream `truncateSchema` allows more than
    /// one relation per frame.
    Truncate {
        tables: Vec<String>,
    },
    /// `rollback` — the in-flight transaction's buffered changes are discarded
    /// (`downstream.ts` `rollback`); no resync required.
    Rollback,
    /// A `schemaChangeSchema` (DDL) payload. The port applies inline DDL only via
    /// a full resync (H5): the receiver surfaces this as a protocol error that
    /// tears the subscriber down so it re-bootstraps. Carries the DDL `tag` for
    /// diagnostics.
    SchemaChange {
        tag: String,
    },
    Commit {
        watermark: String,
    },
    Error(String),
}

pub fn encode_official_status() -> String {
    r#"["status",{"tag":"status"}]"#.to_string()
}

/// A `["rollback", {"tag":"rollback"}]` frame terminating a transaction whose
/// changes must be discarded (`downstream.ts` `rollback`).
pub fn encode_official_rollback() -> String {
    r#"["rollback",{"tag":"rollback"}]"#.to_string()
}

fn relation_json(table: &str, key_columns: &[&str]) -> serde_json::Value {
    serde_json::json!({
        "schema": "public",
        "name": table,
        "rowKey": {"columns": key_columns, "type": "default"},
    })
}

fn row_to_json(row: &[(String, JsonValue)]) -> serde_json::Map<String, serde_json::Value> {
    row.iter()
        .map(|(name, value)| (name.clone(), sqlite_value_to_serde(value)))
        .collect()
}

fn key_to_json(key: &RowKey) -> serde_json::Map<String, serde_json::Value> {
    key.iter()
        .map(|(name, value)| (name.clone(), bigint_to_serde(value)))
        .collect()
}

/// Encodes one [`WireChange`] as its `["data", …]` frame.
pub fn encode_official_change(change: &WireChange) -> serde_json::Value {
    match change {
        WireChange::Insert {
            table,
            row_key,
            row,
        } => {
            let key_columns: Vec<&str> = row_key.iter().map(|(name, _)| name.as_str()).collect();
            // Inserts omit `key` entirely, matching `insertSchema`.
            serde_json::json!(["data", {
                "tag": "insert",
                "relation": relation_json(table, &key_columns),
                "new": row_to_json(row),
            }])
        }
        WireChange::Update {
            table,
            row_key,
            key,
            row,
        } => {
            let key_columns: Vec<&str> = row_key.iter().map(|(name, _)| name.as_str()).collect();
            let key_json = match key {
                Some(k) => serde_json::Value::Object(key_to_json(k)),
                None => serde_json::Value::Null,
            };
            serde_json::json!(["data", {
                "tag": "update",
                "relation": relation_json(table, &key_columns),
                "key": key_json,
                "new": row_to_json(row),
            }])
        }
        WireChange::Delete { table, row_key } => {
            let key_columns: Vec<&str> = row_key.iter().map(|(name, _)| name.as_str()).collect();
            serde_json::json!(["data", {
                "tag": "delete",
                "relation": relation_json(table, &key_columns),
                "key": key_to_json(row_key),
            }])
        }
        WireChange::Truncate { table } => {
            // The coalescing change-log records only the table for a truncate,
            // so `rowKey.columns` is empty (an official downstream keys the
            // truncate by table).
            serde_json::json!(["data", {
                "tag": "truncate",
                "relations": [relation_json(table, &[])],
            }])
        }
        WireChange::SchemaChange(payload) => serde_json::json!(["data", payload]),
    }
}

pub fn encode_official_transaction(watermark: &str, changes: &[WireChange]) -> Vec<String> {
    let mut messages = Vec::with_capacity(changes.len() + 2);
    messages.push(
        serde_json::json!(["begin", {"tag": "begin"}, {"commitWatermark": watermark}]).to_string(),
    );
    for change in changes {
        messages.push(encode_official_change(change).to_string());
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
        // A `rollback` terminates a transaction whose buffered changes must be
        // discarded. The port's view-syncer buffers a whole transaction before
        // applying it on `commit`, so it simply drops the pending buffer — no
        // resync required (`downstream.ts` `rollback`).
        "rollback" => Ok(OfficialMessage::Rollback),
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
    // `truncate` and the DDL variants carry no top-level `relation` (truncate
    // has `relations`, DDL carries `table`/`spec`), so handle them before
    // requiring one.
    match operation {
        // `truncate` is APPLIED (H6(b)): collect every named relation so the
        // receiver can clear each table's rows.
        "truncate" => {
            let tables: Vec<String> = data
                .get("relations")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|relation| {
                    relation
                        .get("name")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .collect();
            if tables.is_empty() {
                return Err(WireError::Malformed("truncate without relations".into()));
            }
            return Ok(OfficialMessage::Truncate { tables });
        }
        // Inline DDL is applied only via a full resync (H5): surface it as a
        // recognized schema-change message; the receiver forces a re-bootstrap.
        "create-table"
        | "add-column"
        | "update-column"
        | "drop-column"
        | "drop-table"
        | "rename-table"
        | "update-table-metadata"
        | "create-index"
        | "drop-index"
        | "backfill"
        | "backfill-completed" => {
            return Ok(OfficialMessage::SchemaChange {
                tag: operation.to_string(),
            });
        }
        _ => {}
    }
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
            let row: Vec<(String, JsonValue)> = new
                .iter()
                .map(|(name, value)| (name.clone(), serde_to_bigint(value)))
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

/// Serializes a full-row value (carried as its declared ZQL type) to its wire
/// JSON form. Because the value is already type-restored (`resolve_catchup_typed`
/// on the send side), a boolean serializes as `true`/`false` and a JSON column
/// as its structured value — not the raw `1`/`0`/text SQLite storage (L4).
fn sqlite_value_to_serde(value: &JsonValue) -> serde_json::Value {
    serde_json::from_str(&value.stringify()).unwrap_or(serde_json::Value::Null)
}

fn bigint_to_serde(value: &JsonValue) -> serde_json::Value {
    serde_json::from_str(&value.stringify()).unwrap_or(serde_json::Value::Null)
}

fn serde_to_bigint(value: &serde_json::Value) -> JsonValue {
    parse(&value.to_string()).unwrap_or(JsonValue::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn official_transaction_round_trips() {
        let changes = vec![
            WireChange::Update {
                table: "issue".into(),
                row_key: vec![("id".into(), JsonValue::Number(1.0))],
                key: None,
                row: vec![
                    ("id".into(), JsonValue::Number(1.0)),
                    ("title".into(), JsonValue::String("hi".into())),
                    ("score".into(), JsonValue::Number(2.5)),
                    ("note".into(), JsonValue::Null),
                ],
            },
            WireChange::Delete {
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

    /// H6(a): an `insert` is tagged `insert` and omits `key`; an `update` is
    /// tagged `update` and carries a (here `null`) `key`.
    #[test]
    fn insert_and_update_are_distinguished_on_the_wire() {
        let insert = encode_official_change(&WireChange::Insert {
            table: "issue".into(),
            row_key: vec![("id".into(), JsonValue::Number(1.0))],
            row: vec![("id".into(), JsonValue::Number(1.0))],
        });
        assert_eq!(insert[0], "data");
        assert_eq!(insert[1]["tag"], "insert");
        // insertSchema has no `key` field.
        assert!(insert[1].get("key").is_none());

        let update = encode_official_change(&WireChange::Update {
            table: "issue".into(),
            row_key: vec![("id".into(), JsonValue::Number(1.0))],
            key: None,
            row: vec![("id".into(), JsonValue::Number(1.0))],
        });
        assert_eq!(update[1]["tag"], "update");
        // updateSchema's `key` is present and nullable.
        assert_eq!(update[1]["key"], serde_json::Value::Null);

        // Both decode as row data on the port's receive side.
        assert!(matches!(
            decode_official_message(&insert.to_string()),
            Ok(OfficialMessage::Data(_))
        ));
        assert!(matches!(
            decode_official_message(&update.to_string()),
            Ok(OfficialMessage::Data(_))
        ));
    }

    /// H6(b): `truncate` is carried as its own `["data",{tag:"truncate",…}]`
    /// frame and now decodes to an APPLIABLE [`OfficialMessage::Truncate`]
    /// naming the relation(s) to clear — no longer dropped or resynced.
    #[test]
    fn truncate_is_carried_and_decodes_to_an_applicable_message() {
        let msg = encode_official_change(&WireChange::Truncate {
            table: "issue".into(),
        });
        assert_eq!(msg[1]["tag"], "truncate");
        assert_eq!(msg[1]["relations"][0]["name"], "issue");
        assert_eq!(
            decode_official_message(&msg.to_string()).unwrap(),
            OfficialMessage::Truncate {
                tables: vec!["issue".into()]
            }
        );
    }

    /// H6(b): `rollback` decodes to [`OfficialMessage::Rollback`] — the receiver
    /// discards its buffered transaction, no resync.
    #[test]
    fn rollback_decodes_to_a_discard_signal() {
        let msg = encode_official_rollback();
        assert_eq!(
            decode_official_message(&msg).unwrap(),
            OfficialMessage::Rollback
        );
    }

    /// A DDL (schema-change) payload decodes to [`OfficialMessage::SchemaChange`]
    /// carrying its tag; the receiver forces a resync (inline DDL apply is out of
    /// scope — H5).
    #[test]
    fn schema_change_decodes_to_a_resync_signal() {
        let payload = serde_json::json!({
            "tag": "create-table",
            "spec": {"schema": "public", "name": "issue", "columns": {}},
        });
        let msg = encode_official_change(&WireChange::SchemaChange(payload));
        assert_eq!(msg[1]["tag"], "create-table");
        assert_eq!(
            decode_official_message(&msg.to_string()).unwrap(),
            OfficialMessage::SchemaChange {
                tag: "create-table".into()
            }
        );
    }

    /// L4: a boolean row value survives the encode → decode round-trip as a
    /// JSON `true` (not the raw `1` SQLite storage), because the send side
    /// type-restores values before they hit the wire.
    #[test]
    fn boolean_row_value_round_trips_as_true() {
        let insert = encode_official_change(&WireChange::Insert {
            table: "issue".into(),
            row_key: vec![("id".into(), JsonValue::Number(1.0))],
            row: vec![
                ("id".into(), JsonValue::Number(1.0)),
                ("active".into(), JsonValue::Bool(true)),
            ],
        });
        // Encoded as a JSON boolean, not 1.
        assert_eq!(insert[1]["new"]["active"], serde_json::Value::Bool(true));
        // And decodes back to a JsonValue::Bool row value.
        match decode_official_message(&insert.to_string()).unwrap() {
            OfficialMessage::Data(StreamedChange::Set { row, .. }) => {
                assert!(row
                    .iter()
                    .any(|(name, value)| name == "active" && *value == JsonValue::Bool(true)));
            }
            other => panic!("expected a Set, got {other:?}"),
        }
    }

    #[test]
    fn malformed_is_rejected() {
        assert!(decode_official_message("not json").is_err());
    }
}
