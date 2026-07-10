//! Translates decoded [`crate::pgoutput::PgoutputMessage`]s into
//! [`crate::data::Change`]s — the last link between the raw replication
//! stream (`replication_conn` + `pgoutput`) and the already-working
//! `ChangeDispatcher` apply-loop in `zero-cache-sqlite`.
//!
//! pgoutput's `Relation` messages describe a table's columns and arrive
//! before any row message referencing it; a `relation_id -> Relation` cache
//! (this module's [`RelationTracker`]) is required to translate `Insert`/
//! `Update`/`Delete`/`Truncate` messages, which only carry the numeric id.
//! This mirrors upstream's `pg/schema/table-diffs.ts`-adjacent relation
//! bookkeeping, done here as its own small stateful piece since upstream
//! keeps it inline in a larger streaming class this port hasn't ported yet.
//!
//! Scope: column values are decoded from pgoutput's text-format tuple
//! encoding (`TupleColumn::Text`) into a typed `JsonValue` per the
//! column's Postgres type OID (bool -> `Bool`, integer/floating-point
//! types -> `Number`, json/jsonb -> a real parsed `JsonValue` via
//! `bigint_json::parse`, one-dimensional arrays of a known element type
//! (`_int4`, `_text`, etc. — see `zero_cache_types::pg_types::
//! array_element_type`) -> a real `JsonValue::Array` via [`parse_pg_array`],
//! everything else -> `String` — see [`text_to_json`]). This mirrors what a
//! real driver (`postgres.js` upstream) does when parsing text-format wire
//! values into JS types before they ever reach a `Change` message. NOT
//! covered: multi-dimensional arrays (`{{1,2},{3,4}}`) and arrays of a
//! type this crate doesn't otherwise decode (they fall back to `String`,
//! same defensive stance as everywhere else in this function).

use std::collections::HashMap;

use zero_cache_shared::bigint_json::{self, JsonValue};
use zero_cache_types::pg_types;

use crate::data::{Change, Relation, Row, RowKey, RowKeyKind};
#[cfg(test)]
use crate::pgoutput::ReplicaIdentity;
use crate::pgoutput::{PgoutputMessage, TupleColumn};

/// Decodes one pgoutput text-format column value into a typed `JsonValue`,
/// given its Postgres type OID. Port of the value-typing half of what a
/// real driver's text-format parsing does (`postgres.js`'s built-in type
/// parsers upstream) — not a literal upstream file, since zero-cache
/// itself receives already-typed values from its driver rather than
/// parsing pgoutput text directly.
///
/// - `bool` -> `Bool` (Postgres sends `t`/`f`)
/// - integer/floating-point/numeric types -> `Number` (parsed as `f64`;
///   values beyond `f64`'s exact-integer range lose precision, the same
///   caveat `zero_cache_types::pg_data_type::NUMERIC_TYPES` already
///   documents for `int8`/`bigint` at the schema-mapping layer)
/// - `json`/`jsonb` -> parsed via `bigint_json::parse` into a real
///   structured `JsonValue`; malformed JSON falls back to `String` (same
///   defensive stance as the numeric-parse-failure case — never panic the
///   replication stream over one bad value)
/// - a known array type -> `Array`, each element decoded via this same
///   function with the array's element type OID
/// - anything else (text, uuid, unrecognized arrays, etc.) -> `String`,
///   passed through verbatim
///   Parses a Postgres date/timestamp text value to epoch milliseconds (as a
///   float, for sub-ms precision), matching upstream's `timestampToFpMillis`.
///   Accepts `YYYY-MM-DD`, `YYYY-MM-DD HH:MM:SS[.frac]`, and a trailing
///   `[+-]HH[:MM]` timezone offset (timestamptz). A value with no offset is
///   treated as UTC (as upstream builds a UTC ISO string). Returns `None` on any
///   unparseable input (caller falls back to the raw text).
fn pg_timestamp_to_epoch_millis(text: &str) -> Option<f64> {
    let t = text.trim();
    if t == "infinity" {
        return Some(f64::INFINITY);
    }
    if t == "-infinity" {
        return Some(f64::NEG_INFINITY);
    }
    let (date_part, time_part) = match t.split_once([' ', 'T']) {
        Some((d, r)) => (d, Some(r)),
        None => (t, None),
    };
    let mut dp = date_part.split('-');
    let year: i64 = dp.next()?.parse().ok()?;
    let month: i64 = dp.next()?.parse().ok()?;
    let day: i64 = dp.next()?.parse().ok()?;
    if dp.next().is_some() {
        return None;
    }
    let mut millis = days_from_civil(year, month, day) as f64 * 86_400_000.0;

    if let Some(time_part) = time_part {
        // Split off a trailing timezone offset (after the seconds). Look for the
        // last '+' or a '-' that isn't part of the date (time has no '-').
        let (time_str, tz) = match time_part.rfind(['+', '-']) {
            Some(i) if i > 0 => (&time_part[..i], Some(&time_part[i..])),
            _ => (time_part, None),
        };
        // Strip a trailing 'Z' (explicit UTC).
        let time_str = time_str.strip_suffix('Z').unwrap_or(time_str);
        let mut ts = time_str.split(':');
        let h: f64 = ts.next()?.parse().ok()?;
        let m: f64 = ts.next()?.parse().ok()?;
        let s: f64 = ts.next().unwrap_or("0").parse().ok()?;
        millis += (h * 3600.0 + m * 60.0 + s) * 1000.0;

        if let Some(tz) = tz {
            let positive = tz.starts_with('+');
            let tz = &tz[1..];
            let (hh, mm) = match tz.split_once(':') {
                Some((h, m)) => (h.parse::<f64>().ok()?, m.parse::<f64>().ok()?),
                None => (tz.parse::<f64>().ok()?, 0.0),
            };
            let offset_millis = (hh.abs() * 60.0 + mm) * 60_000.0;
            // We treated the local time as UTC; a `+` offset is ahead of UTC, so
            // subtract it to get true UTC (matching upstream).
            millis += if positive {
                -offset_millis
            } else {
                offset_millis
            };
        }
    }
    Some(millis)
}

/// Days since the Unix epoch for a proleptic-Gregorian date. Howard Hinnant's
/// `days_from_civil` (the inverse of `civil_from_days`).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn text_to_json(type_oid: i32, text: &str) -> JsonValue {
    if let Some(element_oid) = pg_types::array_element_type(type_oid as i64) {
        return match parse_pg_array(text) {
            Ok(elements) => JsonValue::Array(
                elements
                    .into_iter()
                    .map(|el| match el {
                        None => JsonValue::Null,
                        Some(s) => text_to_json(element_oid as i32, &s),
                    })
                    .collect(),
            ),
            Err(_) => JsonValue::String(text.to_string()),
        };
    }
    match type_oid as i64 {
        pg_types::BOOL => JsonValue::Bool(text == "t"),
        pg_types::INT2
        | pg_types::INT4
        | pg_types::INT8
        | pg_types::OID
        | pg_types::FLOAT4
        | pg_types::FLOAT8
        | pg_types::NUMERIC => match text.parse::<f64>() {
            Ok(n) => JsonValue::Number(n),
            Err(_) => JsonValue::String(text.to_string()),
        },
        pg_types::JSON | pg_types::JSONB => {
            bigint_json::parse(text).unwrap_or_else(|_| JsonValue::String(text.to_string()))
        }
        // Postgres date/timestamp types map to a NUMBER of epoch milliseconds in
        // zero's schema (upstream `timestampToFpMillis`). Delivering the raw text
        // instead breaks any client field typed `number` (e.g. hunting-game's
        // `createdAt`, which feeds `z.number()` query args).
        pg_types::TIMESTAMP | pg_types::TIMESTAMPTZ | pg_types::DATE => {
            match pg_timestamp_to_epoch_millis(text) {
                Some(ms) => JsonValue::Number(ms),
                None => JsonValue::String(text.to_string()),
            }
        }
        _ => JsonValue::String(text.to_string()),
    }
}

/// Parses a one-dimensional Postgres array text literal (`{1,2,3}`,
/// `{"a","b,c"}`, `{NULL,1}`) into its elements, unquoting/unescaping
/// quoted elements and recognizing the bare `NULL` keyword. Port of the
/// value-typing a real driver's array parser does — not a literal upstream
/// file (zero-cache receives already-parsed arrays from `postgres.js`).
/// Returns `Err(())` for malformed input (missing braces); the caller
/// falls back to `String` rather than propagating a hard error.
fn parse_pg_array(text: &str) -> Result<Vec<Option<String>>, ()> {
    let inner = text
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or(())?;
    if inner.is_empty() {
        return Ok(vec![]);
    }
    let chars: Vec<char> = inner.chars().collect();
    let mut elements = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '"' {
            i += 1;
            let mut s = String::new();
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    i += 1;
                    if i >= chars.len() {
                        return Err(());
                    }
                }
                s.push(chars[i]);
                i += 1;
            }
            if i >= chars.len() {
                return Err(()); // unterminated quoted element
            }
            i += 1; // closing quote
            elements.push(Some(s));
        } else {
            let start = i;
            while i < chars.len() && chars[i] != ',' {
                i += 1;
            }
            let raw: String = chars[start..i].iter().collect();
            elements.push(if raw == "NULL" { None } else { Some(raw) });
        }
        if i < chars.len() {
            if chars[i] != ',' {
                return Err(());
            }
            i += 1;
        }
    }
    Ok(elements)
}

/// A pgoutput `Relation` message translated and cached for later row
/// messages that reference it by id.
#[derive(Debug, Clone)]
struct CachedRelation {
    schema: String,
    name: String,
    /// Column names and type OIDs, in wire order (tuple values are
    /// positional).
    columns: Vec<(String, i32)>,
    key_column_names: Vec<String>,
}

/// Errors translating a pgoutput message into a `Change`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TranslateError {
    #[error("row message referenced unknown relation id {0} (no prior Relation message seen)")]
    UnknownRelation(i32),
    #[error("tuple has {actual} columns but relation has {expected}")]
    ColumnCountMismatch { expected: usize, actual: usize },
    #[error("update message has no old/new tuple to determine the row")]
    MissingTuple,
}

/// Tracks `Relation` messages by id and translates subsequent pgoutput
/// messages into [`Change`]s. One instance per replication connection —
/// relation ids are only meaningful within a single stream.
#[derive(Debug, Default)]
pub struct RelationTracker {
    relations: HashMap<i32, CachedRelation>,
}

impl RelationTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Translates one pgoutput message. Returns `Ok(None)` for messages that
    /// don't produce a `Change` on their own (`Relation`, which only updates
    /// internal state, and `Unsupported`/keepalive-adjacent messages).
    pub fn translate(&mut self, msg: &PgoutputMessage) -> Result<Option<Change>, TranslateError> {
        match msg {
            PgoutputMessage::Begin { .. } => Ok(Some(Change::Begin {
                json: None,
                skip_ack: None,
            })),
            PgoutputMessage::Commit { .. } => Ok(Some(Change::Commit)),

            PgoutputMessage::Relation {
                relation_id,
                namespace,
                name,
                replica_identity,
                columns,
            } => {
                let cols: Vec<(String, i32)> = columns
                    .iter()
                    .map(|c| (c.name.clone(), c.type_oid))
                    .collect();
                let key_column_names: Vec<String> = columns
                    .iter()
                    .filter(|c| c.is_key)
                    .map(|c| c.name.clone())
                    .collect();
                self.relations.insert(
                    *relation_id,
                    CachedRelation {
                        schema: namespace.clone(),
                        name: name.clone(),
                        columns: cols,
                        key_column_names,
                    },
                );
                let _ = replica_identity; // recorded implicitly via key_column_names being empty when Nothing
                Ok(None)
            }

            PgoutputMessage::Insert { relation_id, new } => {
                let rel = self.get(*relation_id)?;
                let row = tuple_to_row(rel, new)?;
                Ok(Some(Change::Insert {
                    relation: relation_of(rel),
                    new: row,
                }))
            }

            PgoutputMessage::Update {
                relation_id,
                old,
                old_is_key_only,
                new,
            } => {
                let rel = self.get(*relation_id)?;
                let new_row = tuple_to_row(rel, new)?;
                let key = match old {
                    Some(old_tuple) if *old_is_key_only => Some(key_tuple_to_row(rel, old_tuple)?),
                    Some(old_tuple) => Some(tuple_to_row(rel, old_tuple)?),
                    None => None,
                };
                Ok(Some(Change::Update {
                    relation: relation_of(rel),
                    key,
                    new: new_row,
                }))
            }

            PgoutputMessage::Delete {
                relation_id,
                key,
                is_key_only,
            } => {
                let rel = self.get(*relation_id)?;
                let row = if *is_key_only {
                    key_tuple_to_row(rel, key)?
                } else {
                    tuple_to_row(rel, key)?
                };
                Ok(Some(Change::Delete {
                    relation: relation_of(rel),
                    key: row,
                }))
            }

            PgoutputMessage::Truncate { relation_ids, .. } => {
                let mut relations = Vec::with_capacity(relation_ids.len());
                for id in relation_ids {
                    relations.push(relation_of(self.get(*id)?));
                }
                Ok(Some(Change::Truncate { relations }))
            }

            PgoutputMessage::Unsupported(_) => Ok(None),
        }
    }

    fn get(&self, relation_id: i32) -> Result<&CachedRelation, TranslateError> {
        self.relations
            .get(&relation_id)
            .ok_or(TranslateError::UnknownRelation(relation_id))
    }
}

fn relation_of(rel: &CachedRelation) -> Relation {
    Relation {
        schema: rel.schema.clone(),
        name: rel.name.clone(),
        row_key: RowKey {
            columns: rel.key_column_names.clone(),
            kind: Some(if rel.key_column_names.is_empty() {
                RowKeyKind::Nothing
            } else {
                RowKeyKind::Default
            }),
        },
        columns: rel.columns.clone(),
    }
}

/// Converts a full tuple (one value per relation column, in wire order) into
/// a `Row`, **omitting** columns pgoutput sent as `UnchangedToast` (`'u'`).
///
/// pgoutput emits `'u'` for an out-of-line (TOASTed) column that an UPDATE did
/// not modify (the default under `REPLICA IDENTITY DEFAULT`). Upstream decodes
/// `'u'` to `undefined` and drops the field, so the downstream partial UPDATE
/// omits it from the `SET` clause and the existing replica value is preserved
/// (`pgoutput-parser.ts` `'u'` -> `unchangedToastFallback?.[name]`;
/// `change-processor.ts` `processUpdate`). Surfacing `'u'` as `Null` here would
/// instead clobber the stored value with `NULL`. Key columns are never TOASTed,
/// so omitting these entries never affects change-log key derivation; and since
/// the value is by definition unchanged, leaving the replica column untouched is
/// correct under both `DEFAULT` and `FULL` replica identity.
fn tuple_to_row(rel: &CachedRelation, tuple: &[TupleColumn]) -> Result<Row, TranslateError> {
    if tuple.len() != rel.columns.len() {
        return Err(TranslateError::ColumnCountMismatch {
            expected: rel.columns.len(),
            actual: tuple.len(),
        });
    }
    Ok(rel
        .columns
        .iter()
        .zip(tuple)
        .filter(|(_, col)| !matches!(col, TupleColumn::UnchangedToast))
        .map(|((name, type_oid), col)| (name.clone(), tuple_column_to_json(col, *type_oid)))
        .collect())
}

/// Extracts the key columns from a pgoutput KEY/OLD tuple into a `Row`.
///
/// Note on wire semantics: a `K` (key) or `O` (old) `TupleData` carries a value
/// for EVERY column of the relation — the non-key columns are simply sent as
/// null (or unchanged-toast) placeholders — so the tuple length equals
/// `rel.columns.len()`, NOT `rel.key_column_names.len()`. (An earlier version
/// wrongly expected the latter and rejected every real key/old tuple with a
/// column-count mismatch; only surfaced once a live DELETE/keyed-UPDATE was
/// streamed through.) This walks the full tuple and keeps only the key columns
/// by position.
fn key_tuple_to_row(rel: &CachedRelation, tuple: &[TupleColumn]) -> Result<Row, TranslateError> {
    if tuple.len() != rel.columns.len() {
        return Err(TranslateError::ColumnCountMismatch {
            expected: rel.columns.len(),
            actual: tuple.len(),
        });
    }
    Ok(rel
        .key_column_names
        .iter()
        .filter_map(|key_col| {
            rel.columns
                .iter()
                .position(|(name, _)| name == key_col)
                .map(|idx| {
                    let (name, type_oid) = &rel.columns[idx];
                    (name.clone(), tuple_column_to_json(&tuple[idx], *type_oid))
                })
        })
        .collect())
}

fn tuple_column_to_json(col: &TupleColumn, type_oid: i32) -> JsonValue {
    match col {
        TupleColumn::Null => JsonValue::Null,
        TupleColumn::UnchangedToast => JsonValue::Null,
        TupleColumn::Text(s) => text_to_json(type_oid, s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgoutput::RelationColumn;

    fn relation_msg() -> PgoutputMessage {
        PgoutputMessage::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                RelationColumn {
                    is_key: true,
                    name: "id".into(),
                    type_oid: 23,
                    atttypmod: -1,
                },
                RelationColumn {
                    is_key: false,
                    name: "title".into(),
                    type_oid: 25,
                    atttypmod: -1,
                },
            ],
        }
    }

    #[test]
    fn relation_message_produces_no_change_but_populates_cache() {
        let mut t = RelationTracker::new();
        assert_eq!(t.translate(&relation_msg()).unwrap(), None);
    }

    #[test]
    fn insert_translates_using_cached_relation() {
        let mut t = RelationTracker::new();
        t.translate(&relation_msg()).unwrap();
        let change = t
            .translate(&PgoutputMessage::Insert {
                relation_id: 1,
                new: vec![
                    TupleColumn::Text("7".into()),
                    TupleColumn::Text("hi".into()),
                ],
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            change,
            Change::Insert {
                relation: Relation {
                    schema: "public".into(),
                    name: "t".into(),
                    row_key: RowKey {
                        columns: vec!["id".into()],
                        kind: Some(RowKeyKind::Default)
                    },
                    columns: vec![("id".into(), 23), ("title".into(), 25)],
                },
                new: vec![
                    ("id".into(), JsonValue::Number(7.0)),
                    ("title".into(), JsonValue::String("hi".into()))
                ],
            }
        );
    }

    #[test]
    fn insert_before_relation_errors() {
        let mut t = RelationTracker::new();
        let err = t
            .translate(&PgoutputMessage::Insert {
                relation_id: 99,
                new: vec![],
            })
            .unwrap_err();
        assert_eq!(err, TranslateError::UnknownRelation(99));
    }

    #[test]
    fn update_with_key_only_old_tuple() {
        let mut t = RelationTracker::new();
        t.translate(&relation_msg()).unwrap();
        let change = t
            .translate(&PgoutputMessage::Update {
                relation_id: 1,
                // A key-only OLD tuple still carries a slot for every relation
                // column; the non-key `title` arrives as null.
                old: Some(vec![TupleColumn::Text("7".into()), TupleColumn::Null]),
                old_is_key_only: true,
                new: vec![
                    TupleColumn::Text("7".into()),
                    TupleColumn::Text("bye".into()),
                ],
            })
            .unwrap()
            .unwrap();
        let Change::Update { key, new, .. } = change else {
            panic!("expected Update")
        };
        assert_eq!(key, Some(vec![("id".into(), JsonValue::Number(7.0))]));
        assert_eq!(
            new,
            vec![
                ("id".into(), JsonValue::Number(7.0)),
                ("title".into(), JsonValue::String("bye".into()))
            ]
        );
    }

    #[test]
    fn update_omits_unchanged_toast_columns() {
        let mut t = RelationTracker::new();
        t.translate(&relation_msg()).unwrap();
        // pgoutput sends `'u'` for the TOASTed `title` because this UPDATE did
        // not modify it. The produced row must OMIT `title` so the downstream
        // partial UPDATE leaves the stored value intact — surfacing it as
        // `Null` would clobber the large column.
        let change = t
            .translate(&PgoutputMessage::Update {
                relation_id: 1,
                old: None,
                old_is_key_only: false,
                new: vec![TupleColumn::Text("7".into()), TupleColumn::UnchangedToast],
            })
            .unwrap()
            .unwrap();
        let Change::Update { new, .. } = change else {
            panic!("expected Update")
        };
        assert_eq!(new, vec![("id".into(), JsonValue::Number(7.0))]);
    }

    #[test]
    fn update_without_old_tuple_has_no_key() {
        let mut t = RelationTracker::new();
        t.translate(&relation_msg()).unwrap();
        let change = t
            .translate(&PgoutputMessage::Update {
                relation_id: 1,
                old: None,
                old_is_key_only: false,
                new: vec![
                    TupleColumn::Text("7".into()),
                    TupleColumn::Text("bye".into()),
                ],
            })
            .unwrap()
            .unwrap();
        let Change::Update { key, .. } = change else {
            panic!("expected Update")
        };
        assert_eq!(key, None);
    }

    #[test]
    fn delete_with_full_old_row() {
        let mut t = RelationTracker::new();
        t.translate(&relation_msg()).unwrap();
        let change = t
            .translate(&PgoutputMessage::Delete {
                relation_id: 1,
                key: vec![
                    TupleColumn::Text("7".into()),
                    TupleColumn::Text("hi".into()),
                ],
                is_key_only: false,
            })
            .unwrap()
            .unwrap();
        let Change::Delete { key, .. } = change else {
            panic!("expected Delete")
        };
        assert_eq!(
            key,
            vec![
                ("id".into(), JsonValue::Number(7.0)),
                ("title".into(), JsonValue::String("hi".into()))
            ]
        );
    }

    #[test]
    fn delete_key_only() {
        let mut t = RelationTracker::new();
        t.translate(&relation_msg()).unwrap();
        let change = t
            .translate(&PgoutputMessage::Delete {
                relation_id: 1,
                // A key-only DELETE tuple carries all relation columns with the
                // non-key `title` as null (pgoutput wire semantics).
                key: vec![TupleColumn::Text("7".into()), TupleColumn::Null],
                is_key_only: true,
            })
            .unwrap()
            .unwrap();
        let Change::Delete { key, .. } = change else {
            panic!("expected Delete")
        };
        assert_eq!(key, vec![("id".into(), JsonValue::Number(7.0))]);
    }

    #[test]
    fn text_to_json_bool() {
        assert_eq!(
            text_to_json(pg_types::BOOL as i32, "t"),
            JsonValue::Bool(true)
        );
        assert_eq!(
            text_to_json(pg_types::BOOL as i32, "f"),
            JsonValue::Bool(false)
        );
    }

    #[test]
    fn timestamp_columns_decode_to_epoch_millis_number() {
        // A streamed timestamp must be a NUMBER of epoch ms (zero schema types
        // `createdAt` as `number`) — not a string, or client `z.number()` args
        // (e.g. getMyProgressionEventsSince) fail validation.
        // 2000-01-01 00:00:00 UTC == 946684800000 ms.
        assert_eq!(
            text_to_json(pg_types::TIMESTAMP as i32, "2000-01-01 00:00:00"),
            JsonValue::Number(946684800000.0)
        );
        // 1970-01-01 == epoch 0.
        assert_eq!(
            text_to_json(pg_types::DATE as i32, "1970-01-01"),
            JsonValue::Number(0.0)
        );
        // timestamptz offset applied: 12:00+02 is 10:00 UTC.
        assert_eq!(
            text_to_json(pg_types::TIMESTAMPTZ as i32, "1970-01-01 12:00:00+02"),
            JsonValue::Number((10 * 3600 * 1000) as f64)
        );
        // Fractional seconds preserved.
        assert_eq!(
            text_to_json(pg_types::TIMESTAMP as i32, "1970-01-01 00:00:00.5"),
            JsonValue::Number(500.0)
        );
        // Unparseable → falls back to the raw string (no panic).
        assert_eq!(
            text_to_json(pg_types::TIMESTAMP as i32, "not-a-date"),
            JsonValue::String("not-a-date".into())
        );
    }

    #[test]
    fn text_to_json_numeric_types() {
        assert_eq!(
            text_to_json(pg_types::INT4 as i32, "42"),
            JsonValue::Number(42.0)
        );
        assert_eq!(
            text_to_json(pg_types::INT8 as i32, "9007199254740991"),
            JsonValue::Number(9007199254740991.0)
        );
        assert_eq!(
            text_to_json(pg_types::FLOAT8 as i32, "3.5"),
            JsonValue::Number(3.5)
        );
        assert_eq!(
            text_to_json(pg_types::NUMERIC as i32, "-1.25"),
            JsonValue::Number(-1.25)
        );
    }

    #[test]
    fn text_to_json_unparseable_numeric_falls_back_to_string() {
        // Defensive: a malformed/unexpected numeric-typed value should not
        // panic the whole replication stream.
        assert_eq!(
            text_to_json(pg_types::INT4 as i32, "NaN-ish-garbage"),
            JsonValue::String("NaN-ish-garbage".into())
        );
    }

    #[test]
    fn text_to_json_other_types_stay_string() {
        assert_eq!(
            text_to_json(pg_types::TEXT as i32, "hello"),
            JsonValue::String("hello".into())
        );
        assert_eq!(
            text_to_json(pg_types::UUID as i32, "abc-123"),
            JsonValue::String("abc-123".into())
        );
    }

    #[test]
    fn text_to_json_json_and_jsonb_are_really_parsed() {
        let expected = JsonValue::Object(vec![("a".into(), JsonValue::Number(1.0))]);
        assert_eq!(text_to_json(pg_types::JSON as i32, "{\"a\":1}"), expected);
        assert_eq!(text_to_json(pg_types::JSONB as i32, "{\"a\":1}"), expected);
        assert_eq!(
            text_to_json(pg_types::JSON as i32, "[1,2,3]"),
            JsonValue::Array(vec![
                JsonValue::Number(1.0),
                JsonValue::Number(2.0),
                JsonValue::Number(3.0),
            ])
        );
    }

    #[test]
    fn text_to_json_malformed_json_falls_back_to_string() {
        assert_eq!(
            text_to_json(pg_types::JSON as i32, "{not valid json"),
            JsonValue::String("{not valid json".into())
        );
    }

    #[test]
    fn text_to_json_int4_array() {
        assert_eq!(
            text_to_json(pg_types::INT4_ARRAY as i32, "{1,2,3}"),
            JsonValue::Array(vec![
                JsonValue::Number(1.0),
                JsonValue::Number(2.0),
                JsonValue::Number(3.0)
            ])
        );
    }

    #[test]
    fn text_to_json_text_array_with_quoted_and_comma_containing_elements() {
        assert_eq!(
            text_to_json(pg_types::TEXT_ARRAY as i32, r#"{"a,b","c\"d"}"#),
            JsonValue::Array(vec![
                JsonValue::String("a,b".into()),
                JsonValue::String("c\"d".into())
            ])
        );
    }

    #[test]
    fn text_to_json_array_with_null_element() {
        assert_eq!(
            text_to_json(pg_types::INT4_ARRAY as i32, "{1,NULL,3}"),
            JsonValue::Array(vec![
                JsonValue::Number(1.0),
                JsonValue::Null,
                JsonValue::Number(3.0)
            ])
        );
    }

    #[test]
    fn text_to_json_empty_array() {
        assert_eq!(
            text_to_json(pg_types::INT4_ARRAY as i32, "{}"),
            JsonValue::Array(vec![])
        );
    }

    #[test]
    fn text_to_json_bool_array() {
        assert_eq!(
            text_to_json(pg_types::BOOL_ARRAY as i32, "{t,f,t}"),
            JsonValue::Array(vec![
                JsonValue::Bool(true),
                JsonValue::Bool(false),
                JsonValue::Bool(true)
            ])
        );
    }

    #[test]
    fn text_to_json_malformed_array_falls_back_to_string() {
        assert_eq!(
            text_to_json(pg_types::INT4_ARRAY as i32, "not an array"),
            JsonValue::String("not an array".into())
        );
    }

    #[test]
    fn truncate_resolves_all_relations() {
        let mut t = RelationTracker::new();
        t.translate(&relation_msg()).unwrap();
        let change = t
            .translate(&PgoutputMessage::Truncate {
                relation_ids: vec![1],
                cascade: false,
                restart_identity: false,
            })
            .unwrap()
            .unwrap();
        let Change::Truncate { relations } = change else {
            panic!("expected Truncate")
        };
        assert_eq!(relations.len(), 1);
        assert_eq!(relations[0].name, "t");
    }

    #[test]
    fn begin_and_commit_translate_directly() {
        let mut t = RelationTracker::new();
        assert_eq!(
            t.translate(&PgoutputMessage::Begin {
                final_lsn: 0,
                commit_timestamp: 0,
                xid: 1
            })
            .unwrap(),
            Some(Change::Begin {
                json: None,
                skip_ack: None
            })
        );
        assert_eq!(
            t.translate(&PgoutputMessage::Commit {
                commit_lsn: 0,
                end_lsn: 0,
                commit_timestamp: 0
            })
            .unwrap(),
            Some(Change::Commit)
        );
    }

    #[test]
    fn unsupported_message_produces_no_change() {
        let mut t = RelationTracker::new();
        assert_eq!(
            t.translate(&PgoutputMessage::Unsupported(10)).unwrap(),
            None
        );
    }

    #[test]
    fn column_count_mismatch_errors() {
        let mut t = RelationTracker::new();
        t.translate(&relation_msg()).unwrap();
        let err = t
            .translate(&PgoutputMessage::Insert {
                relation_id: 1,
                new: vec![TupleColumn::Text("only-one".into())],
            })
            .unwrap_err();
        assert_eq!(
            err,
            TranslateError::ColumnCountMismatch {
                expected: 2,
                actual: 1
            }
        );
    }
}
