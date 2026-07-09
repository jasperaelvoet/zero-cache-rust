//! Deserializes the `publishedSchema` JSON produced by
//! `change-source/pg/schema/published.ts`'s introspection query into this
//! port's `PublishedTableSpec` / `PublishedIndexSpec` structs — the counterpart
//! to upstream's `v.parse(result, publishedSchema)` (the valita schema in
//! `db/specs.ts`). This is the piece that lets the live `initial_sync` driver
//! introspect its own table/index specs (via
//! `zero-cache-change-source::published_schema::published_schema_query`) instead
//! of taking them as an input.
//!
//! Operates on this port's [`JsonValue`] (parse the query's JSON text with
//! `zero_cache_shared::bigint_json::parse` first), matching the JSON-model
//! convention of the existing `ast_from_json` deserializer. NOT included: the
//! `replicaIdentityColumns` denormalization (`publishedSchema`'s `.map(...)`),
//! which needs the lite-spec/zql-spec machinery and is a separate step.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::JsonValue;

use crate::specs::{
    ColumnSpec, Direction, IndexSpec, PgTypeClass, PublicationInfo, PublishedColumnSpec,
    PublishedIndexSpec, PublishedTableSpec, ReplicaIdentity,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("published-schema JSON: {0}")]
pub struct ParseSpecError(pub String);

type R<T> = Result<T, ParseSpecError>;

fn err<T>(msg: impl Into<String>) -> R<T> {
    Err(ParseSpecError(msg.into()))
}

fn field<'a>(obj: &'a JsonValue, key: &str) -> Option<&'a JsonValue> {
    match obj {
        JsonValue::Object(entries) => entries.iter().find(|(k, _)| k == key).map(|(_, v)| v),
        _ => None,
    }
}

/// A present-and-non-null field; `None` for absent OR JSON `null` (the query
/// emits `null` for absent optional values, and the valita schemas mark these
/// `.nullable().optional()`).
fn opt_field<'a>(obj: &'a JsonValue, key: &str) -> Option<&'a JsonValue> {
    match field(obj, key) {
        Some(JsonValue::Null) | None => None,
        Some(v) => Some(v),
    }
}

fn as_str(v: &JsonValue) -> R<String> {
    match v {
        JsonValue::String(s) => Ok(s.clone()),
        other => err(format!("expected string, got {other:?}")),
    }
}

fn as_i64(v: &JsonValue) -> R<i64> {
    match v {
        JsonValue::Number(n) => Ok(*n as i64),
        JsonValue::BigInt(b) => i64::try_from(b.clone())
            .map_err(|_| ParseSpecError(format!("bigint {b} out of i64 range"))),
        other => err(format!("expected number, got {other:?}")),
    }
}

fn as_bool(v: &JsonValue) -> R<bool> {
    match v {
        JsonValue::Bool(b) => Ok(*b),
        other => err(format!("expected bool, got {other:?}")),
    }
}

fn as_object(v: &JsonValue) -> R<&[(String, JsonValue)]> {
    match v {
        JsonValue::Object(entries) => Ok(entries),
        other => err(format!("expected object, got {other:?}")),
    }
}

fn as_array(v: &JsonValue) -> R<&[JsonValue]> {
    match v {
        JsonValue::Array(items) => Ok(items),
        other => err(format!("expected array, got {other:?}")),
    }
}

fn req<'a>(obj: &'a JsonValue, key: &str) -> R<&'a JsonValue> {
    field(obj, key).ok_or_else(|| ParseSpecError(format!("missing field {key:?}")))
}

fn parse_column(col: &JsonValue) -> R<PublishedColumnSpec> {
    let pos = as_i64(req(col, "pos")?)?;
    let data_type = as_str(req(col, "dataType")?)?;
    let pg_type_class = opt_field(col, "pgTypeClass")
        .map(|v| as_str(v).and_then(|s| parse_pg_type_class(&s)))
        .transpose()?;
    let elem_pg_type_class = opt_field(col, "elemPgTypeClass")
        .map(|v| as_str(v).and_then(|s| parse_pg_type_class(&s)))
        .transpose()?;
    let character_maximum_length = opt_field(col, "characterMaximumLength")
        .map(as_i64)
        .transpose()?;
    let not_null = opt_field(col, "notNull").map(as_bool).transpose()?;
    let dflt = opt_field(col, "dflt").map(as_str).transpose()?;
    let type_oid = as_i64(req(col, "typeOID")?)?;
    Ok(PublishedColumnSpec {
        column: ColumnSpec {
            pos,
            data_type,
            pg_type_class,
            elem_pg_type_class,
            character_maximum_length,
            not_null,
            dflt,
        },
        type_oid,
    })
}

fn parse_pg_type_class(s: &str) -> R<PgTypeClass> {
    PgTypeClass::from_str(s).ok_or_else(|| ParseSpecError(format!("unknown pgTypeClass {s:?}")))
}

/// Parses one `tables` element into a [`PublishedTableSpec`]. Columns are
/// returned sorted by `pos` (the JSON `columns` object has no guaranteed key
/// order, so this fixes a deterministic column order — also what the lite-table
/// DDL sorts by).
pub fn table_from_json(t: &JsonValue) -> R<PublishedTableSpec> {
    let name = as_str(req(t, "name")?)?;
    let schema = as_str(req(t, "schema")?)?;
    let oid = as_i64(req(t, "oid")?)?;
    let schema_oid = opt_field(t, "schemaOID").map(as_i64).transpose()?;
    let replica_identity = opt_field(t, "replicaIdentity")
        .map(|v| {
            as_str(v).and_then(|s| {
                ReplicaIdentity::from_str(&s)
                    .ok_or_else(|| ParseSpecError(format!("unknown replicaIdentity {s:?}")))
            })
        })
        .transpose()?;

    let mut columns: Vec<(String, PublishedColumnSpec)> = as_object(req(t, "columns")?)?
        .iter()
        .map(|(name, col)| Ok((name.clone(), parse_column(col)?)))
        .collect::<R<_>>()?;
    columns.sort_by_key(|(_, c)| c.column.pos);

    let primary_key = opt_field(t, "primaryKey")
        .map(|v| as_array(v)?.iter().map(as_str).collect::<R<Vec<_>>>())
        .transpose()?
        .filter(|pk| !pk.is_empty());

    let publications: BTreeMap<String, PublicationInfo> = as_object(req(t, "publications")?)?
        .iter()
        .map(|(pubname, info)| {
            let row_filter = opt_field(info, "rowFilter").map(as_str).transpose()?;
            Ok((pubname.clone(), PublicationInfo { row_filter }))
        })
        .collect::<R<_>>()?;

    Ok(PublishedTableSpec {
        name,
        schema,
        oid,
        schema_oid,
        columns,
        primary_key,
        replica_identity,
        publications,
    })
}

/// Parses one `indexes` element into a [`PublishedIndexSpec`]. The `columns`
/// object maps column name -> `"ASC"`/`"DESC"`; order is not meaningful in the
/// JSON but the query emits it already ordered, and this preserves that
/// iteration order.
pub fn index_from_json(i: &JsonValue) -> R<PublishedIndexSpec> {
    let name = as_str(req(i, "name")?)?;
    let table_name = as_str(req(i, "tableName")?)?;
    let schema = as_str(req(i, "schema")?)?;
    let unique = as_bool(req(i, "unique")?)?;
    let is_replica_identity = opt_field(i, "isReplicaIdentity").map(as_bool).transpose()?;
    let is_primary_key = opt_field(i, "isPrimaryKey").map(as_bool).transpose()?;
    let is_immediate = opt_field(i, "isImmediate").map(as_bool).transpose()?;

    let columns: Vec<(String, Direction)> = as_object(req(i, "columns")?)?
        .iter()
        .map(|(col, dir)| {
            let dir = as_str(dir)?;
            let dir = Direction::from_str(&dir)
                .ok_or_else(|| ParseSpecError(format!("unknown index direction {dir:?}")))?;
            Ok((col.clone(), dir))
        })
        .collect::<R<_>>()?;

    Ok(PublishedIndexSpec {
        name,
        table_name,
        schema,
        unique,
        columns,
        is_replica_identity,
        is_primary_key,
        is_immediate,
    })
}

/// Converts a [`PublishedIndexSpec`] to the plain [`IndexSpec`] the DDL applier
/// / initial-sync driver consume (dropping the primary-key/replica-identity
/// flags, matching `IndexSpec`'s narrower shape).
pub fn to_index_spec(p: &PublishedIndexSpec) -> IndexSpec {
    IndexSpec {
        name: p.name.clone(),
        table_name: p.table_name.clone(),
        schema: p.schema.clone(),
        unique: p.unique,
        columns: p.columns.clone(),
    }
}

/// Deserializes a whole `{tables: [...], indexes: [...]}` `publishedSchema`
/// object into its table and index specs.
pub fn published_schema_from_json(
    schema: &JsonValue,
) -> R<(Vec<PublishedTableSpec>, Vec<PublishedIndexSpec>)> {
    let tables = as_array(req(schema, "tables")?)?
        .iter()
        .map(table_from_json)
        .collect::<R<Vec<_>>>()?;
    let indexes = as_array(req(schema, "indexes")?)?
        .iter()
        .map(index_from_json)
        .collect::<R<Vec<_>>>()?;
    Ok((tables, indexes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_shared::bigint_json::parse;

    #[test]
    fn parses_a_published_table_with_columns_sorted_by_pos() {
        // Columns intentionally out of pos order in the JSON.
        let json = parse(
            r#"{
              "oid": 16385,
              "schema": "public",
              "schemaOID": 2200,
              "name": "issue",
              "replicaIdentity": "d",
              "columns": {
                "title": {"pos": 2, "dataType": "text", "pgTypeClass": "b", "elemPgTypeClass": null,
                          "typeOID": 25, "characterMaximumLength": null, "notNull": false, "dflt": null},
                "id": {"pos": 1, "dataType": "int4", "pgTypeClass": "b", "elemPgTypeClass": null,
                       "typeOID": 23, "characterMaximumLength": null, "notNull": true, "dflt": null}
              },
              "primaryKey": ["id"],
              "publications": {"zero_data": {"rowFilter": null}}
            }"#,
        )
        .unwrap();

        let t = table_from_json(&json).unwrap();
        assert_eq!(t.name, "issue");
        assert_eq!(t.oid, 16385);
        assert_eq!(t.schema_oid, Some(2200));
        assert_eq!(t.replica_identity, Some(ReplicaIdentity::Default));
        // Sorted by pos: id (1) before title (2).
        assert_eq!(
            t.columns
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>(),
            ["id", "title"]
        );
        assert_eq!(t.columns[0].1.column.data_type, "int4");
        assert_eq!(t.columns[0].1.type_oid, 23);
        assert_eq!(t.columns[0].1.column.not_null, Some(true));
        assert_eq!(t.columns[0].1.column.pg_type_class, Some(PgTypeClass::Base));
        assert_eq!(t.primary_key, Some(vec!["id".to_string()]));
        assert_eq!(t.publications.get("zero_data").unwrap().row_filter, None);
    }

    #[test]
    fn empty_primary_key_array_becomes_none() {
        let json = parse(
            r#"{"oid": 1, "schema": "public", "name": "t",
               "columns": {"a": {"pos": 1, "dataType": "text", "typeOID": 25}},
               "primaryKey": [], "publications": {}}"#,
        )
        .unwrap();
        let t = table_from_json(&json).unwrap();
        assert_eq!(
            t.primary_key, None,
            "an empty PK array is normalized to None"
        );
    }

    #[test]
    fn row_filter_is_carried_when_present() {
        let json = parse(
            r#"{"oid": 1, "schema": "public", "name": "t",
               "columns": {"a": {"pos": 1, "dataType": "text", "typeOID": 25}},
               "publications": {"p": {"rowFilter": "(a > 5)"}}}"#,
        )
        .unwrap();
        let t = table_from_json(&json).unwrap();
        assert_eq!(
            t.publications.get("p").unwrap().row_filter.as_deref(),
            Some("(a > 5)")
        );
    }

    #[test]
    fn parses_a_published_index() {
        let json = parse(
            r#"{"schema": "public", "tableName": "issue", "name": "issue_pkey",
               "unique": true, "isPrimaryKey": true, "isReplicaIdentity": false,
               "isImmediate": true, "columns": {"id": "ASC"}}"#,
        )
        .unwrap();
        let i = index_from_json(&json).unwrap();
        assert_eq!(i.name, "issue_pkey");
        assert_eq!(i.table_name, "issue");
        assert!(i.unique);
        assert_eq!(i.is_primary_key, Some(true));
        assert_eq!(i.columns, vec![("id".to_string(), Direction::Asc)]);
        // Downcast to the plain IndexSpec used by the DDL applier.
        let plain = to_index_spec(&i);
        assert_eq!(plain.name, "issue_pkey");
        assert_eq!(plain.columns, i.columns);
    }

    #[test]
    fn parses_whole_schema_object() {
        let json = parse(
            r#"{
              "tables": [{"oid": 1, "schema": "public", "name": "t",
                          "columns": {"a": {"pos": 1, "dataType": "text", "typeOID": 25}},
                          "publications": {"p": {"rowFilter": null}}}],
              "indexes": [{"schema": "public", "tableName": "t", "name": "t_a",
                           "unique": false, "columns": {"a": "DESC"}}]
            }"#,
        )
        .unwrap();
        let (tables, indexes) = published_schema_from_json(&json).unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(indexes.len(), 1);
        assert_eq!(tables[0].name, "t");
        assert_eq!(indexes[0].columns, vec![("a".to_string(), Direction::Desc)]);
    }

    #[test]
    fn missing_required_field_errors() {
        let json = parse(r#"{"schema": "public", "name": "t"}"#).unwrap();
        assert!(
            table_from_json(&json).is_err(),
            "missing oid/columns/publications"
        );
    }

    #[test]
    fn empty_schema_yields_empty_vecs() {
        let json = parse(r#"{"tables": [], "indexes": []}"#).unwrap();
        let (tables, indexes) = published_schema_from_json(&json).unwrap();
        assert!(tables.is_empty() && indexes.is_empty());
    }
}
