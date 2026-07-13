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

use std::collections::{BTreeMap, HashMap};

use zero_cache_shared::bigint_json::{self, JsonValue};
use zero_cache_types::pg_to_lite::map_postgres_to_lite_column;
use zero_cache_types::pg_types;
use zero_cache_types::published_schema_json::{published_schema_from_json, to_index_spec};
use zero_cache_types::specs::{PublishedIndexSpec, PublishedTableSpec, TableSpec};

use crate::data::{Change, ColumnDef, Identifier, Relation, Row, RowKey, RowKeyKind, TableCreate};
use crate::pgoutput::{PgoutputMessage, ReplicaIdentity, TupleColumn};

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
    /// The table's declared replica identity, retained so `relation_of` can
    /// distinguish REPLICA IDENTITY FULL (which flags no key columns on the
    /// wire, yet must key rows off the table's primary key) from a genuinely
    /// keyless table.
    replica_identity: ReplicaIdentity,
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

/// The result of translating a captured `{app}/{shard}/ddl` logical message
/// (`pg_logical_emit_message`) into replica schema changes. Port of the return
/// of upstream `change-source.ts`'s `#handleDdlMessage` / `#makeSchemaChanges`,
/// with an explicit resync escape hatch for diffs the port can't yet apply
/// incrementally (upstream would schedule a backfill stream; this port has no
/// inline backfill scheduler on the apply side, so it defers to the existing
/// full-resync drift mechanism instead of silently dropping the change).
#[derive(Debug, Clone, PartialEq)]
pub enum DdlOutcome {
    /// Incremental schema changes to apply, in dependency order (drop indexes,
    /// drop tables, alter tables, create tables, create indexes).
    Changes(Vec<Change>),
    /// The diff can't be represented as incremental `Change`s (e.g. it would
    /// require a column/table backfill). The caller should trigger a full
    /// resync via the existing drift path.
    Resync(String),
}

/// The minimal record of the previous DDL event kept so a message that omits
/// `previousSchema` (pre-v21 upstream trigger functions) can fall back to the
/// last event's schema, and so the "effective tag" can be resolved across
/// nested `ddlStart`/`ddlEnd` events. Port of upstream's
/// `#lastReplicationEvent`.
#[derive(Debug, Clone)]
struct LastDdlEvent {
    event_type: String,
    tag: String,
    schema: JsonValue,
}

/// Tracks `Relation` messages by id and translates subsequent pgoutput
/// messages into [`Change`]s. One instance per replication connection —
/// relation ids are only meaningful within a single stream.
#[derive(Debug, Default)]
pub struct RelationTracker {
    relations: HashMap<i32, CachedRelation>,
    /// When set (via [`RelationTracker::set_ddl_prefix`]), the logical-message
    /// prefix `{app_id}/{shard_num}/ddl` whose payloads are decoded into schema
    /// changes. Left `None` (the default) for callers that only stream data —
    /// DDL messages are then ignored, preserving the prior behavior.
    ddl_prefix: Option<String>,
    /// The previous DDL event, for the `previousSchema`/effective-tag fallbacks.
    last_ddl: Option<LastDdlEvent>,
}

impl RelationTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables inline DDL decoding for the shard `{app_id}/{shard_num}`: a
    /// captured logical message on the `{app_id}/{shard_num}/ddl` prefix (emitted
    /// by the event triggers in [`crate::ddl`]) is then translated into schema
    /// [`Change`]s by [`RelationTracker::ddl_outcome`]. Threaded in from the
    /// shard config; without it, DDL messages are ignored.
    pub fn set_ddl_prefix(&mut self, app_id: &str, shard_num: i64) {
        self.ddl_prefix = Some(format!("{app_id}/{shard_num}/ddl"));
    }

    /// If `msg` is a `{app_id}/{shard_num}/ddl` logical message for the
    /// configured shard, decodes its `previousSchema`→`schema` diff into schema
    /// [`Change`]s (or a resync signal); otherwise returns `Ok(None)` so the
    /// caller falls through to the normal [`RelationTracker::translate`] path.
    ///
    /// Port of `change-source.ts`'s `#handleDdlMessage`: it recognizes the
    /// `ddlStart`/`ddlUpdate`/`schemaSnapshot` event types, resolves the
    /// effective command tag (to decide whether a backfill would be required),
    /// and diffs the two published-schema snapshots via
    /// [`Self::make_schema_changes`]. Malformed/newer payloads and diffs that
    /// would need a backfill map to [`DdlOutcome::Resync`] rather than being
    /// dropped.
    pub fn ddl_outcome(
        &mut self,
        msg: &PgoutputMessage,
    ) -> Result<Option<DdlOutcome>, TranslateError> {
        let PgoutputMessage::Message {
            prefix, content, ..
        } = msg
        else {
            return Ok(None);
        };
        let Some(ddl_prefix) = self.ddl_prefix.clone() else {
            return Ok(None);
        };
        // Upstream also accepts the legacy empty suffix (`{app}/{shard}`); this
        // port only installs the `/ddl` prefix, so match it exactly.
        if *prefix != ddl_prefix {
            return Ok(None);
        }

        let text = match std::str::from_utf8(content) {
            Ok(t) => t,
            Err(_) => {
                return Ok(Some(DdlOutcome::Resync(
                    "ddl message content was not valid UTF-8".into(),
                )))
            }
        };
        let event = match bigint_json::parse(text) {
            Ok(v) => v,
            Err(e) => {
                return Ok(Some(DdlOutcome::Resync(format!(
                    "could not parse ddl message JSON: {e}"
                ))))
            }
        };

        let event_type = json_str(&event, "type").unwrap_or_default();
        match event_type.as_str() {
            "ddlStart" | "ddlUpdate" | "schemaSnapshot" => {}
            // Unknown types are ignored for forwards compatibility (upstream
            // returns no changes).
            _ => return Ok(Some(DdlOutcome::Changes(vec![]))),
        }

        // The current event's schema (always present).
        let Some(schema) = json_field(&event, "schema").cloned() else {
            return Ok(Some(DdlOutcome::Resync(
                "ddl message missing `schema`".into(),
            )));
        };
        let tag = json_field(&event, "event")
            .and_then(|e| json_str(e, "tag"))
            .unwrap_or_else(|| "UNKNOWN".into());

        // previousSchema: present-and-non-null on this event, else (pre-v21) the
        // last event's schema.
        let prev_event = self.last_ddl.take();
        let previous_schema = match json_field(&event, "previousSchema") {
            Some(JsonValue::Null) | None => prev_event.as_ref().map(|e| e.schema.clone()),
            Some(prev) => Some(prev.clone()),
        };

        // Effective tag: from the previous event if it was a ddlStart (nested
        // start->end events), else the current ddlUpdate tag, else UNKNOWN.
        let effective_tag = match prev_event.as_ref() {
            Some(e) if e.event_type == "ddlStart" => e.tag.clone(),
            _ if event_type == "ddlUpdate" => tag.clone(),
            _ => "UNKNOWN".into(),
        };

        // Record this event for the next one's fallbacks.
        self.last_ddl = Some(LastDdlEvent {
            event_type,
            tag,
            schema: schema.clone(),
        });

        let Some(previous_schema) = previous_schema else {
            // No prior schema to diff against (e.g. a no-change ddlStart); the
            // event only establishes context for a following event.
            return Ok(Some(DdlOutcome::Changes(vec![])));
        };

        Ok(Some(Self::make_schema_changes(
            &previous_schema,
            &schema,
            &effective_tag,
        )))
    }

    /// Diffs two `publishedSchema` snapshots into ordered schema [`Change`]s.
    /// Port of `#makeSchemaChanges` + `#getTableChanges`. Returns
    /// [`DdlOutcome::Resync`] for any delta the port can't apply incrementally
    /// (parse failure, or a table/column introduction that upstream would
    /// backfill).
    fn make_schema_changes(prev: &JsonValue, next: &JsonValue, tag: &str) -> DdlOutcome {
        let (prev_tables, prev_indexes) = match published_schema_from_json(prev) {
            Ok(s) => s,
            Err(e) => return DdlOutcome::Resync(format!("could not parse previousSchema: {e}")),
        };
        let (next_tables, next_indexes) = match published_schema_from_json(next) {
            Ok(s) => s,
            Err(e) => return DdlOutcome::Resync(format!("could not parse schema: {e}")),
        };

        // Tables keyed by their stable OID; indexes by `{schema}.{name}`.
        let prev_tbl: BTreeMap<i64, &PublishedTableSpec> =
            prev_tables.iter().map(|t| (t.oid, t)).collect();
        let next_tbl: BTreeMap<i64, &PublishedTableSpec> =
            next_tables.iter().map(|t| (t.oid, t)).collect();
        let index_id = |i: &PublishedIndexSpec| format!("{}.{}", i.schema, i.name);
        let prev_idx: BTreeMap<String, &PublishedIndexSpec> =
            prev_indexes.iter().map(|i| (index_id(i), i)).collect();
        let next_idx: BTreeMap<String, &PublishedIndexSpec> =
            next_indexes.iter().map(|i| (index_id(i), i)).collect();

        let mut changes: Vec<Change> = Vec::new();

        // Indexes present in both but structurally changed are dropped+recreated.
        let mut dropped_idx: Vec<&String> = prev_idx
            .keys()
            .filter(|id| !next_idx.contains_key(*id))
            .collect();
        let mut created_idx: Vec<&String> = next_idx
            .keys()
            .filter(|id| !prev_idx.contains_key(*id))
            .collect();
        for id in prev_idx.keys().filter(|id| next_idx.contains_key(*id)) {
            let (p, n) = (prev_idx[id], next_idx[id]);
            if index_structurally_changed(p, n) {
                dropped_idx.push(id);
                created_idx.push(id);
            }
        }

        // -- drop indexes --
        for id in &dropped_idx {
            let i = prev_idx[*id];
            changes.push(Change::DropIndex {
                id: Identifier {
                    schema: i.schema.clone(),
                    name: i.name.clone(),
                },
            });
        }

        // -- drop tables --
        for (oid, t) in &prev_tbl {
            if !next_tbl.contains_key(oid) {
                changes.push(Change::DropTable {
                    id: Identifier {
                        schema: t.schema.clone(),
                        name: t.name.clone(),
                    },
                });
            }
        }

        // -- alter kept tables (drop / update / add columns, rename) --
        for (oid, next_t) in &next_tbl {
            if let Some(prev_t) = prev_tbl.get(oid) {
                match Self::table_changes(prev_t, next_t, tag) {
                    Ok(mut cs) => changes.append(&mut cs),
                    Err(reason) => return DdlOutcome::Resync(reason),
                }
            }
        }

        // -- create tables --
        for (oid, t) in &next_tbl {
            if !prev_tbl.contains_key(oid) {
                // Only tables introduced by a `CREATE` statement can skip
                // backfill; anything else (ALTER PUBLICATION / COMMENT / MANUAL /
                // UNKNOWN) needs to copy existing rows — which this apply side
                // can't schedule, so resync.
                if !tag.starts_with("CREATE") {
                    return DdlOutcome::Resync(format!(
                        "table {}.{} entered the publication without a CREATE (tag {tag:?}); \
                         a backfill is required",
                        t.schema, t.name
                    ));
                }
                changes.push(Change::CreateTable(TableCreate {
                    spec: table_spec_of(t),
                    // Upstream `getMetadata` (schemaOID/relationOID/rowKey attnums)
                    // is not replicated here; the row key is resolved from the
                    // `<table>_pkey` index that follows as a create-index. TODO:
                    // port table metadata for FULL-identity/no-PK tables.
                    metadata: None,
                    backfill: None,
                }));
            }
        }

        // -- create indexes (after their tables/columns exist) --
        for id in &created_idx {
            changes.push(Change::CreateIndex {
                spec: to_index_spec(next_idx[*id]),
            });
        }

        DdlOutcome::Changes(changes)
    }

    /// Per-table diff (rename + column drop/update/add). Port of
    /// `#getTableChanges`. `Err(reason)` means the change needs a resync.
    fn table_changes(
        prev: &PublishedTableSpec,
        next: &PublishedTableSpec,
        tag: &str,
    ) -> Result<Vec<Change>, String> {
        let mut changes = Vec::new();
        if prev.schema != next.schema || prev.name != next.name {
            changes.push(Change::RenameTable {
                old: Identifier {
                    schema: prev.schema.clone(),
                    name: prev.name.clone(),
                },
                new: Identifier {
                    schema: next.schema.clone(),
                    name: next.name.clone(),
                },
            });
        }
        // NOTE: upstream also emits `update-table-metadata` here; table metadata
        // is intentionally not replicated by this port (see create-table).

        let table = Identifier {
            schema: next.schema.clone(),
            name: next.name.clone(),
        };
        // Columns keyed by their stable attnum (`pos`).
        let prev_cols: BTreeMap<i64, &(String, zero_cache_types::specs::PublishedColumnSpec)> =
            prev.columns.iter().map(|c| (c.1.column.pos, c)).collect();
        let next_cols: BTreeMap<i64, &(String, zero_cache_types::specs::PublishedColumnSpec)> =
            next.columns.iter().map(|c| (c.1.column.pos, c)).collect();

        // -- drop columns --
        for (pos, (name, _)) in &prev_cols {
            if !next_cols.contains_key(pos) {
                changes.push(Change::DropColumn {
                    table: table.clone(),
                    column: name.clone(),
                });
            }
        }

        // -- update columns (name / data type / not-null) --
        for (pos, (next_name, next_spec)) in &next_cols {
            if let Some((prev_name, prev_spec)) = prev_cols.get(pos) {
                if prev_name != next_name
                    || prev_spec.column.data_type != next_spec.column.data_type
                    || prev_spec.column.not_null != next_spec.column.not_null
                {
                    changes.push(Change::UpdateColumn {
                        table: table.clone(),
                        old: ColumnDef {
                            name: (*prev_name).clone(),
                            spec: prev_spec.column.clone(),
                        },
                        new: ColumnDef {
                            name: (*next_name).clone(),
                            spec: next_spec.column.clone(),
                        },
                    });
                }
            }
        }

        // -- add columns --
        // Only `ALTER TABLE`-introduced columns whose default is replicable can
        // skip backfill. Anything else needs to copy existing rows, which this
        // apply side can't schedule inline → resync.
        for (pos, (name, spec)) in &next_cols {
            if prev_cols.contains_key(pos) {
                continue;
            }
            let backfill_required = tag != "ALTER TABLE"
                || map_postgres_to_lite_column(&next.name, name, &spec.column, false).is_err();
            if backfill_required {
                return Err(format!(
                    "column {}.{} needs a backfill (tag {tag:?} / unsupported default)",
                    next.name, name
                ));
            }
            changes.push(Change::AddColumn {
                table: table.clone(),
                column: ColumnDef {
                    name: (*name).clone(),
                    spec: spec.column.clone(),
                },
                table_metadata: None,
                backfill: None,
            });
        }

        Ok(changes)
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
                        replica_identity: *replica_identity,
                    },
                );
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

            // Logical decoding messages (`pg_logical_emit_message`) are
            // informational — upstream's replication-lag reports round-trip
            // them, but they never carry row data, so they produce no Change.
            PgoutputMessage::Message { .. } => Ok(None),

            // Origin ('O') and Type ('Y') are transaction/relation metadata:
            // upstream decodes them (msgOrigin/msgType) but they carry no row
            // data. Values are read via type OIDs, not the Type-name cache, and
            // this port assumes a single replication origin, so both are no-ops.
            PgoutputMessage::Origin { .. } | PgoutputMessage::Type { .. } => Ok(None),

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
    // Under REPLICA IDENTITY `d`(default)/`i`(index) pgoutput flags the key/index
    // columns, so a non-empty `key_column_names` means those cases key off the
    // flagged columns exactly as before. REPLICA IDENTITY `f`(full) flags *no*
    // key columns on the wire, so it lands here with an empty `key_column_names`;
    // upstream's `replicaIdentityColumns` (`schema/published.ts` `case 'f'`)
    // resolves it to the table's primary key. We surface that as
    // `RowKeyKind::Full`, and the change-apply path (`row_apply::get_key`) fills
    // the PK from the replica table spec. A genuinely keyless table (identity
    // `n`, or `d` with no primary key) stays `Nothing`.
    let kind = if !rel.key_column_names.is_empty() {
        RowKeyKind::Default
    } else if rel.replica_identity == ReplicaIdentity::Full {
        RowKeyKind::Full
    } else {
        RowKeyKind::Nothing
    };
    Relation {
        schema: rel.schema.clone(),
        name: rel.name.clone(),
        row_key: RowKey {
            columns: rel.key_column_names.clone(),
            kind: Some(kind),
        },
        columns: rel.columns.clone(),
    }
}

/// Looks up an object field by key in a [`JsonValue::Object`].
fn json_field<'a>(obj: &'a JsonValue, key: &str) -> Option<&'a JsonValue> {
    match obj {
        JsonValue::Object(entries) => entries.iter().find(|(k, _)| k == key).map(|(_, v)| v),
        _ => None,
    }
}

/// Reads a string field by key, or `None` if absent / not a string.
fn json_str(obj: &JsonValue, key: &str) -> Option<String> {
    match json_field(obj, key) {
        Some(JsonValue::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// The plain [`TableSpec`] the `create-table` change carries, built from a
/// published table spec (dropping the OID/publication/replica-identity metadata
/// the `Change` doesn't model).
fn table_spec_of(t: &PublishedTableSpec) -> TableSpec {
    TableSpec {
        name: t.name.clone(),
        schema: t.schema.clone(),
        columns: t
            .columns
            .iter()
            .map(|(name, spec)| (name.clone(), spec.column.clone()))
            .collect(),
        primary_key: t.primary_key.clone(),
    }
}

/// Whether an index kept across a diff (same `{schema}.{name}`) changed
/// structurally (uniqueness, primary-key/replica-identity/immediate flags, or
/// its `(column, direction)` set) rather than merely cosmetically (a table or
/// column rename). Port of `isIndexStructurallyChanged`, comparing by column
/// name+direction rather than by stable attnum — so a bare column rename that
/// keeps the same index looks changed; upstream resolves attnums to avoid that.
/// The consequence is at most a redundant drop+recreate of the index, which is
/// still correct. TODO: resolve column attnums for exact parity.
fn index_structurally_changed(prev: &PublishedIndexSpec, next: &PublishedIndexSpec) -> bool {
    prev.unique != next.unique
        || prev.is_primary_key != next.is_primary_key
        || prev.is_replica_identity != next.is_replica_identity
        || prev.is_immediate != next.is_immediate
        || prev.columns != next.columns
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
        // Binary-format tuple columns ('b') are a latent path: this port never
        // requests binary pgoutput in START_REPLICATION, so pgoutput only ever
        // emits text ('t'). Map the raw bytes lossily so the exhaustive match
        // stays total without inventing a wire semantics we don't exercise.
        TupleColumn::Binary(bytes) => {
            JsonValue::String(String::from_utf8_lossy(bytes).into_owned())
        }
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

    /// A REPLICA IDENTITY FULL relation: pgoutput flags NO column as key, yet
    /// the table has a primary key (`id`) that the change-apply path must key
    /// rows off.
    fn relation_msg_full() -> PgoutputMessage {
        PgoutputMessage::Relation {
            relation_id: 1,
            namespace: "public".into(),
            name: "t".into(),
            replica_identity: ReplicaIdentity::Full,
            columns: vec![
                RelationColumn {
                    is_key: false,
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
    fn full_identity_update_produces_full_row_key_kind() {
        // Under REPLICA IDENTITY FULL pgoutput sends the entire OLD row (not
        // key-only). translate must mark the relation `Full` so the apply path
        // keys off the table's primary key rather than treating it as keyless.
        let mut t = RelationTracker::new();
        t.translate(&relation_msg_full()).unwrap();
        let change = t
            .translate(&PgoutputMessage::Update {
                relation_id: 1,
                old: Some(vec![
                    TupleColumn::Text("7".into()),
                    TupleColumn::Text("old-title".into()),
                ]),
                old_is_key_only: false,
                new: vec![
                    TupleColumn::Text("7".into()),
                    TupleColumn::Text("new-title".into()),
                ],
            })
            .unwrap()
            .unwrap();
        let Change::Update { relation, key, .. } = change else {
            panic!("expected Update")
        };
        assert_eq!(relation.row_key.kind, Some(RowKeyKind::Full));
        // The OLD tuple carries every column (the wire form for FULL); the PK is
        // extracted downstream via `get_key`.
        assert_eq!(
            key,
            Some(vec![
                ("id".into(), JsonValue::Number(7.0)),
                ("title".into(), JsonValue::String("old-title".into())),
            ])
        );
    }

    #[test]
    fn full_identity_delete_produces_full_row_key_kind() {
        let mut t = RelationTracker::new();
        t.translate(&relation_msg_full()).unwrap();
        let change = t
            .translate(&PgoutputMessage::Delete {
                relation_id: 1,
                // FULL sends the whole row, not just the key.
                key: vec![
                    TupleColumn::Text("7".into()),
                    TupleColumn::Text("gone".into()),
                ],
                is_key_only: false,
            })
            .unwrap()
            .unwrap();
        let Change::Delete { relation, key } = change else {
            panic!("expected Delete")
        };
        assert_eq!(relation.row_key.kind, Some(RowKeyKind::Full));
        assert_eq!(
            key,
            vec![
                ("id".into(), JsonValue::Number(7.0)),
                ("title".into(), JsonValue::String("gone".into())),
            ]
        );
    }

    #[test]
    fn keyless_nothing_identity_stays_nothing() {
        // REPLICA IDENTITY NOTHING (no flagged key columns, not FULL) must remain
        // `Nothing`, preserving the existing keyless behavior.
        let mut t = RelationTracker::new();
        t.translate(&PgoutputMessage::Relation {
            relation_id: 2,
            namespace: "public".into(),
            name: "logs".into(),
            replica_identity: ReplicaIdentity::Nothing,
            columns: vec![RelationColumn {
                is_key: false,
                name: "msg".into(),
                type_oid: 25,
                atttypmod: -1,
            }],
        })
        .unwrap();
        let change = t
            .translate(&PgoutputMessage::Insert {
                relation_id: 2,
                new: vec![TupleColumn::Text("hi".into())],
            })
            .unwrap()
            .unwrap();
        let Change::Insert { relation, .. } = change else {
            panic!("expected Insert")
        };
        assert_eq!(relation.row_key.kind, Some(RowKeyKind::Nothing));
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
    fn logical_decoding_message_produces_no_change() {
        let mut t = RelationTracker::new();
        assert_eq!(
            t.translate(&PgoutputMessage::Message {
                transactional: false,
                lsn: 7,
                prefix: "replication-lag".into(),
                content: b"{}".to_vec(),
            })
            .unwrap(),
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

    // ---- DDL apply-side (H5) ----

    /// A `publishedSchema` JSON object with the given table(s)/index(es) spliced
    /// in. Each `table` is `(oid, name, "col:pos:type,...")`; each `index` is
    /// `(name, table, "col:dir,...", unique)`.
    fn schema_json(tables: &[&str], indexes: &[&str]) -> String {
        format!(
            r#"{{"tables":[{}],"indexes":[{}]}}"#,
            tables.join(","),
            indexes.join(",")
        )
    }

    fn table_json(oid: i64, name: &str, cols: &[(&str, i64, &str)]) -> String {
        let cols: Vec<String> = cols
            .iter()
            .map(|(n, pos, ty)| format!(r#""{n}":{{"pos":{pos},"dataType":"{ty}","typeOID":25}}"#))
            .collect();
        format!(
            r#"{{"oid":{oid},"schema":"public","name":"{name}","columns":{{{}}},"publications":{{"p":{{"rowFilter":null}}}}}}"#,
            cols.join(",")
        )
    }

    fn index_json(name: &str, table: &str, col: &str, unique: bool) -> String {
        format!(
            r#"{{"schema":"public","tableName":"{table}","name":"{name}","unique":{unique},"columns":{{"{col}":"ASC"}}}}"#
        )
    }

    /// A `ddlUpdate` logical-message body diffing `prev` → `next` under `tag`.
    fn ddl_message(prev: &str, next: &str, tag: &str) -> PgoutputMessage {
        let body = format!(
            r#"{{"type":"ddlUpdate","version":1,"event":{{"tag":"{tag}"}},"context":{{"query":"x"}},"previousSchema":{prev},"schema":{next}}}"#
        );
        PgoutputMessage::Message {
            transactional: true,
            lsn: 42,
            prefix: "zero/0/ddl".into(),
            content: body.into_bytes(),
        }
    }

    #[test]
    fn ddl_message_ignored_without_a_configured_shard() {
        // With no shard prefix set, a ddl message is not decoded (falls through).
        let mut t = RelationTracker::new();
        let prev = schema_json(&[&table_json(1, "t", &[("id", 1, "int4")])], &[]);
        let next = schema_json(
            &[&table_json(
                1,
                "t",
                &[("id", 1, "int4"), ("extra", 2, "int4")],
            )],
            &[],
        );
        assert_eq!(
            t.ddl_outcome(&ddl_message(&prev, &next, "ALTER TABLE"))
                .unwrap(),
            None
        );
    }

    #[test]
    fn ddl_message_for_a_different_shard_is_ignored() {
        let mut t = RelationTracker::new();
        t.set_ddl_prefix("zero", 1); // messages are emitted on zero/0/ddl
        let prev = schema_json(&[&table_json(1, "t", &[("id", 1, "int4")])], &[]);
        assert_eq!(
            t.ddl_outcome(&ddl_message(&prev, &prev, "ALTER TABLE"))
                .unwrap(),
            None,
            "prefix zero/0/ddl does not match the configured zero/1 shard"
        );
    }

    #[test]
    fn ddl_add_column_maps_to_add_column_change() {
        let mut t = RelationTracker::new();
        t.set_ddl_prefix("zero", 0);
        let prev = schema_json(&[&table_json(1, "t", &[("id", 1, "int4")])], &[]);
        let next = schema_json(
            &[&table_json(
                1,
                "t",
                &[("id", 1, "int4"), ("extra", 2, "int4")],
            )],
            &[],
        );
        let outcome = t
            .ddl_outcome(&ddl_message(&prev, &next, "ALTER TABLE"))
            .unwrap()
            .unwrap();
        let DdlOutcome::Changes(changes) = outcome else {
            panic!("expected changes, got {outcome:?}")
        };
        assert_eq!(changes.len(), 1);
        let Change::AddColumn { table, column, .. } = &changes[0] else {
            panic!("expected add-column, got {:?}", changes[0])
        };
        assert_eq!(table.name, "t");
        assert_eq!(column.name, "extra");
    }

    #[test]
    fn ddl_create_index_maps_to_create_index_change() {
        let mut t = RelationTracker::new();
        t.set_ddl_prefix("zero", 0);
        let table = table_json(1, "t", &[("id", 1, "int4")]);
        let prev = schema_json(&[&table], &[]);
        let next = schema_json(&[&table], &[&index_json("t_id_idx", "t", "id", false)]);
        let outcome = t
            .ddl_outcome(&ddl_message(&prev, &next, "CREATE INDEX"))
            .unwrap()
            .unwrap();
        let DdlOutcome::Changes(changes) = outcome else {
            panic!("expected changes, got {outcome:?}")
        };
        assert_eq!(changes.len(), 1);
        let Change::CreateIndex { spec } = &changes[0] else {
            panic!("expected create-index, got {:?}", changes[0])
        };
        assert_eq!(spec.name, "t_id_idx");
        assert_eq!(spec.table_name, "t");
    }

    #[test]
    fn ddl_drop_table_maps_to_drop_table_change() {
        let mut t = RelationTracker::new();
        t.set_ddl_prefix("zero", 0);
        let prev = schema_json(&[&table_json(1, "t", &[("id", 1, "int4")])], &[]);
        let next = schema_json(&[], &[]);
        let outcome = t
            .ddl_outcome(&ddl_message(&prev, &next, "DROP TABLE"))
            .unwrap()
            .unwrap();
        let DdlOutcome::Changes(changes) = outcome else {
            panic!("expected changes, got {outcome:?}")
        };
        assert_eq!(changes.len(), 1);
        assert!(matches!(&changes[0], Change::DropTable { id } if id.name == "t"));
    }

    #[test]
    fn ddl_create_table_under_create_tag_maps_to_create_table() {
        let mut t = RelationTracker::new();
        t.set_ddl_prefix("zero", 0);
        let prev = schema_json(&[], &[]);
        let next = schema_json(&[&table_json(2, "created", &[("id", 1, "int4")])], &[]);
        let outcome = t
            .ddl_outcome(&ddl_message(&prev, &next, "CREATE TABLE"))
            .unwrap()
            .unwrap();
        let DdlOutcome::Changes(changes) = outcome else {
            panic!("expected changes, got {outcome:?}")
        };
        let Change::CreateTable(create) = &changes[0] else {
            panic!("expected create-table, got {:?}", changes[0])
        };
        assert_eq!(create.spec.name, "created");
        assert!(create.backfill.is_none());
    }

    #[test]
    fn ddl_new_table_without_create_tag_signals_resync() {
        // A table appearing via ALTER PUBLICATION (not CREATE) would need a
        // backfill of existing rows — the port defers to a resync.
        let mut t = RelationTracker::new();
        t.set_ddl_prefix("zero", 0);
        let prev = schema_json(&[], &[]);
        let next = schema_json(&[&table_json(2, "adopted", &[("id", 1, "int4")])], &[]);
        let outcome = t
            .ddl_outcome(&ddl_message(&prev, &next, "ALTER PUBLICATION"))
            .unwrap()
            .unwrap();
        assert!(
            matches!(outcome, DdlOutcome::Resync(_)),
            "expected resync, got {outcome:?}"
        );
    }

    #[test]
    fn ddl_add_column_without_alter_table_tag_signals_resync() {
        // A column newly *published* (not created by ALTER TABLE) may hold
        // arbitrary values in existing rows and must be backfilled → resync.
        let mut t = RelationTracker::new();
        t.set_ddl_prefix("zero", 0);
        let prev = schema_json(&[&table_json(1, "t", &[("id", 1, "int4")])], &[]);
        let next = schema_json(
            &[&table_json(
                1,
                "t",
                &[("id", 1, "int4"), ("extra", 2, "int4")],
            )],
            &[],
        );
        let outcome = t
            .ddl_outcome(&ddl_message(&prev, &next, "ALTER PUBLICATION"))
            .unwrap()
            .unwrap();
        assert!(
            matches!(outcome, DdlOutcome::Resync(_)),
            "expected resync, got {outcome:?}"
        );
    }

    #[test]
    fn ddl_no_diff_yields_no_changes() {
        let mut t = RelationTracker::new();
        t.set_ddl_prefix("zero", 0);
        let schema = schema_json(&[&table_json(1, "t", &[("id", 1, "int4")])], &[]);
        assert_eq!(
            t.ddl_outcome(&ddl_message(&schema, &schema, "ALTER TABLE"))
                .unwrap(),
            Some(DdlOutcome::Changes(vec![]))
        );
    }

    #[test]
    fn ddl_unknown_event_type_is_ignored() {
        let mut t = RelationTracker::new();
        t.set_ddl_prefix("zero", 0);
        let body = br#"{"type":"somethingNew","version":2}"#.to_vec();
        let msg = PgoutputMessage::Message {
            transactional: true,
            lsn: 1,
            prefix: "zero/0/ddl".into(),
            content: body,
        };
        assert_eq!(
            t.ddl_outcome(&msg).unwrap(),
            Some(DdlOutcome::Changes(vec![]))
        );
    }
}
