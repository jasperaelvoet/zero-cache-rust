//! Port of `zero-cache/src/types/lite.ts`.
//!
//! Conversions between Postgres row values and the value types storable in
//! SQLite (via better-sqlite3 in the original), plus the [`LiteTypeString`]
//! encoding that carries upstream type/constraint metadata through SQLite's
//! loose type system.

use num_bigint::BigInt;
use zero_cache_shared::bigint_json::{stringify, JsonValue};

use crate::pg_data_type::{data_type_to_zql_value_type, ValueType};
use crate::specs::LiteTableSpec;

/// A Postgres row value, as it arrives before conversion to a lite value.
/// Mirrors `PostgresValueType` (a `JSONValue` extended with byte arrays).
#[derive(Debug, Clone, PartialEq)]
pub enum PgValue {
    Null,
    Bool(bool),
    Number(f64),
    BigInt(BigInt),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<PgValue>),
    Object(Vec<(String, PgValue)>),
}

/// A JavaScript value type supported by better-sqlite3: `number | bigint |
/// string | null | Uint8Array`. Port of `LiteValueType`.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteValue {
    Null,
    Number(f64),
    Big(BigInt),
    Text(String),
    Blob(Vec<u8>),
}

/// A lite row: column name -> lite value, in column order.
pub type LiteRow = Vec<(String, LiteValue)>;

/// JSON is passed through as an already-stringified string.
pub const JSON_STRINGIFIED: char = 's';
/// JSON is passed through parsed (as objects/arrays), to be stringified here.
pub const JSON_PARSED: char = 'p';

/// How JSON/JSONB values are represented on input. Port of `JSONFormat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonFormat {
    Stringified,
    Parsed,
}

/// The result of [`lite_row`].
#[derive(Debug, Clone, PartialEq)]
pub struct LiteRowResult {
    pub row: LiteRow,
    pub num_cols: usize,
    /// Whether any value required conversion (the TS version avoids copying the
    /// row when `false`).
    pub converted: bool,
}

// ----- LiteTypeString attributes -------------------------------------------

const TEXT_ENUM_ATTRIBUTE: &str = "|TEXT_ENUM";
const NOT_NULL_ATTRIBUTE: &str = "|NOT_NULL";
/// Attribute marking an array column.
pub const TEXT_ARRAY_ATTRIBUTE: &str = "|TEXT_ARRAY";

/// A `LiteTypeString`: the upstream type followed by `|`-prefixed attributes.
pub type LiteTypeString = String;

/// Builds a [`LiteTypeString`]. Port of `liteTypeString`.
///
/// Panics if `upstream_data_type` already contains `|` (an `assert` in the TS
/// source).
pub fn lite_type_string(
    upstream_data_type: &str,
    not_null: bool,
    text_enum: bool,
    text_array: bool,
) -> LiteTypeString {
    assert!(
        !upstream_data_type.contains('|'),
        "Upstream type should not contain |"
    );
    let mut s = upstream_data_type.to_string();
    if not_null {
        s.push_str(NOT_NULL_ATTRIBUTE);
    }
    if text_enum {
        s.push_str(TEXT_ENUM_ATTRIBUTE);
    }
    if text_array {
        s.push_str(TEXT_ARRAY_ATTRIBUTE);
    }
    s
}

/// Extracts the upstream data type (before the first `|`). Port of
/// `upstreamDataType`.
pub fn upstream_data_type(lite_type_string: &str) -> &str {
    match lite_type_string.find('|') {
        Some(delim) if delim > 0 => &lite_type_string[..delim],
        _ => lite_type_string,
    }
}

/// Whether the upstream column is nullable. Port of `nullableUpstream`.
pub fn nullable_upstream(lite_type_string: &str) -> bool {
    !lite_type_string.contains(NOT_NULL_ATTRIBUTE)
}

/// Whether the lite type denotes an enum. Port of `isEnum`.
pub fn is_enum(lite_type_string: &str) -> bool {
    lite_type_string.contains(TEXT_ENUM_ATTRIBUTE)
}

/// Whether the lite type denotes an array. Port of `isArray`.
pub fn is_array(lite_type_string: &str) -> bool {
    lite_type_string.contains(TEXT_ARRAY_ATTRIBUTE) || lite_type_string.contains("[]")
}

/// Errors from [`assert_valid_lite_column_spec`].
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InvalidColumnSpecError {
    #[error("TEXT_ARRAY_ATTRIBUTE and [] must be consistent in dataType: {0}")]
    ArrayAttributeMismatch(String),
    #[error("[] in dataType ({0}) must match elemPgTypeClass presence")]
    ElemTypeClassMismatch(String),
    #[error("Invalid dataType {0}")]
    InvalidDataType(String),
}

/// Validates internal consistency of a lite column spec's `dataType`
/// encoding: the `|TEXT_ARRAY` attribute and a literal `[]` suffix must agree,
/// array-ness must match whether `elem_pg_type_class` is set, and no `[]` may
/// appear after a `|` (i.e. only the legacy pre-attribute position). Port of
/// `assertValidLiteColumnSpec`.
pub fn assert_valid_lite_column_spec(
    spec: &crate::specs::ColumnSpec,
) -> Result<(), InvalidColumnSpecError> {
    let data_type = &spec.data_type;
    let has_attr = data_type.contains(TEXT_ARRAY_ATTRIBUTE);
    let has_brackets = data_type.contains("[]");
    if has_attr != has_brackets {
        return Err(InvalidColumnSpecError::ArrayAttributeMismatch(
            data_type.clone(),
        ));
    }
    if has_brackets != spec.elem_pg_type_class.is_some() {
        return Err(InvalidColumnSpecError::ElemTypeClassMismatch(
            data_type.clone(),
        ));
    }
    // No `[]` after a `|` (only the legacy pre-attribute position is valid).
    // Mirrors `/^.+\|.*\[\]/`: requires a non-empty prefix before the `|`.
    if let Some(bar) = data_type.find('|') {
        if bar > 0 && data_type[bar..].contains("[]") {
            return Err(InvalidColumnSpecError::InvalidDataType(data_type.clone()));
        }
    }
    Ok(())
}

/// Returns the ZQL value type for a [`LiteTypeString`], or `None` if
/// unsupported. Port of `liteTypeToZqlValueType`.
pub fn lite_type_to_zql_value_type(lite_type_string: &str) -> Option<ValueType> {
    data_type_to_zql_value_type(
        &upstream_data_type(lite_type_string).to_lowercase(),
        is_enum(lite_type_string),
        is_array(lite_type_string),
    )
}

// ----- value conversion ----------------------------------------------------

/// Converts a Postgres value to a [`LiteValue`]. Port of `liteValue`.
pub fn lite_value(val: &PgValue, pg_type: &str, json_format: JsonFormat) -> LiteValue {
    match val {
        PgValue::Bytes(b) => return LiteValue::Blob(b.clone()),
        PgValue::Null => return LiteValue::Null,
        _ => {}
    }

    let value_type = lite_type_to_zql_value_type(pg_type);
    if value_type == Some(ValueType::Json) {
        if json_format == JsonFormat::Stringified {
            if let PgValue::String(s) = val {
                // Already a JSON string when not parsed.
                return LiteValue::Text(s.clone());
            }
        }
        return LiteValue::Text(stringify(&pg_to_json(val)));
    }

    // Non-JSON: booleans become 0/1; nested arrays/objects are stringified.
    match to_lite_json(val) {
        j @ (JsonValue::Array(_) | JsonValue::Object(_)) => LiteValue::Text(stringify(&j)),
        JsonValue::String(s) => LiteValue::Text(s),
        JsonValue::Number(n) => LiteValue::Number(n),
        JsonValue::BigInt(b) => LiteValue::Big(b),
        JsonValue::Null => LiteValue::Null,
        // Booleans were converted away by `to_lite_json`.
        JsonValue::Bool(b) => LiteValue::Number(if b { 1.0 } else { 0.0 }),
    }
}

/// Whether [`lite_value`] would return the input unchanged (no copy needed).
/// Encodes the `val !== liteVal` check from `liteRow`.
fn is_passthrough(val: &PgValue, value_type: Option<ValueType>, json_format: JsonFormat) -> bool {
    match val {
        PgValue::Null | PgValue::Bytes(_) => true,
        PgValue::String(_) => {
            !(value_type == Some(ValueType::Json) && json_format == JsonFormat::Parsed)
        }
        PgValue::Number(_) | PgValue::BigInt(_) => value_type != Some(ValueType::Json),
        PgValue::Bool(_) | PgValue::Array(_) | PgValue::Object(_) => false,
    }
}

/// Creates a [`LiteRow`] from `row`. Port of `liteRow`. `converted` reflects
/// whether any value required conversion.
pub fn lite_row(
    row: &[(String, PgValue)],
    table: &LiteTableSpec,
    json_format: JsonFormat,
) -> LiteRowResult {
    let column_type = |col: &str| -> String {
        table
            .column(col)
            .unwrap_or_else(|| panic!("Unknown column {col} in table {}", table.name))
            .data_type
            .clone()
    };

    let mut converted = false;
    for (key, val) in row {
        let vt = lite_type_to_zql_value_type(&column_type(key));
        if !is_passthrough(val, vt, json_format) {
            converted = true;
            break;
        }
    }

    let out: LiteRow = row
        .iter()
        .map(|(key, val)| (key.clone(), lite_value(val, &column_type(key), json_format)))
        .collect();

    LiteRowResult {
        num_cols: row.len(),
        row: out,
        converted,
    }
}

/// Converts a [`PgValue`] to a [`JsonValue`] for the JSON branch (booleans stay
/// boolean). Bytes are not expected here (handled earlier by `lite_value`).
fn pg_to_json(val: &PgValue) -> JsonValue {
    match val {
        PgValue::Null => JsonValue::Null,
        PgValue::Bool(b) => JsonValue::Bool(*b),
        PgValue::Number(n) => JsonValue::Number(*n),
        PgValue::BigInt(b) => JsonValue::BigInt(b.clone()),
        PgValue::String(s) => JsonValue::String(s.clone()),
        PgValue::Bytes(_) => JsonValue::Null,
        PgValue::Array(items) => JsonValue::Array(items.iter().map(pg_to_json).collect()),
        PgValue::Object(entries) => JsonValue::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), pg_to_json(v)))
                .collect(),
        ),
    }
}

/// Port of `toLiteValue`: like [`pg_to_json`] but booleans become `1`/`0`.
fn to_lite_json(val: &PgValue) -> JsonValue {
    match val {
        PgValue::Bool(b) => JsonValue::Number(if *b { 1.0 } else { 0.0 }),
        PgValue::Array(items) => JsonValue::Array(items.iter().map(to_lite_json).collect()),
        PgValue::Object(entries) => JsonValue::Object(
            entries
                .iter()
                .map(|(k, v)| (k.clone(), to_lite_json(v)))
                .collect(),
        ),
        other => pg_to_json(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::specs::ColumnSpec;

    fn col(data_type: &str, pos: i64) -> ColumnSpec {
        ColumnSpec::new(data_type, pos)
    }

    fn table(cols: &[(&str, &str, i64)]) -> LiteTableSpec {
        LiteTableSpec {
            name: "tableName".to_string(),
            primary_key: Some(vec!["foo".to_string()]),
            columns: cols
                .iter()
                .map(|(name, dt, pos)| (name.to_string(), col(dt, *pos)))
                .collect(),
        }
    }

    fn mk_row(pairs: Vec<(&str, PgValue)>) -> Vec<(String, PgValue)> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    fn mk_lite(pairs: Vec<(&str, LiteValue)>) -> LiteRow {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    fn big(n: i64) -> PgValue {
        PgValue::BigInt(BigInt::from(n))
    }

    #[test]
    fn lite_row_no_copy_cases() {
        let t = table(&[("foo", "string", 1)]);
        let r = lite_row(
            &mk_row(vec![("foo", PgValue::String("bar".into()))]),
            &t,
            JsonFormat::Parsed,
        );
        assert!(!r.converted);
        assert_eq!(r.num_cols, 1);
        assert_eq!(r.row, mk_lite(vec![("foo", LiteValue::Text("bar".into()))]));

        let t = table(&[("foo", "string", 1), ("baz", "int", 2)]);
        let r = lite_row(
            &mk_row(vec![
                ("foo", PgValue::String("bar".into())),
                ("baz", big(2)),
            ]),
            &t,
            JsonFormat::Parsed,
        );
        assert!(!r.converted);
        assert_eq!(
            r.row,
            mk_lite(vec![
                ("foo", LiteValue::Text("bar".into())),
                ("baz", LiteValue::Big(BigInt::from(2)))
            ])
        );
    }

    #[test]
    fn lite_row_copy_cases() {
        let t = table(&[("foo", "bool", 1)]);
        let r = lite_row(
            &mk_row(vec![("foo", PgValue::Bool(true))]),
            &t,
            JsonFormat::Parsed,
        );
        assert!(r.converted);
        assert_eq!(r.row, mk_lite(vec![("foo", LiteValue::Number(1.0))]));

        let t = table(&[("foo", "string", 1), ("b", "boolean", 2)]);
        let r = lite_row(
            &mk_row(vec![
                ("foo", PgValue::String("bar".into())),
                ("b", PgValue::Bool(false)),
            ]),
            &t,
            JsonFormat::Parsed,
        );
        assert!(r.converted);
        assert_eq!(
            r.row,
            mk_lite(vec![
                ("foo", LiteValue::Text("bar".into())),
                ("b", LiteValue::Number(0.0))
            ])
        );
    }

    #[test]
    fn lite_row_json_cases() {
        let t = table(&[
            ("foo", "json", 1),
            ("bar", "jsonb", 2),
            ("baz", "json", 3),
            ("boo", "jsonb", 4),
        ]);
        let r = lite_row(
            &mk_row(vec![
                ("foo", PgValue::String("bar".into())),
                ("bar", PgValue::Number(1.0)),
                ("baz", PgValue::Bool(true)),
                (
                    "boo",
                    PgValue::Object(vec![("key".into(), PgValue::String("val".into()))]),
                ),
            ]),
            &t,
            JsonFormat::Parsed,
        );
        assert!(r.converted);
        assert_eq!(
            r.row,
            mk_lite(vec![
                ("foo", LiteValue::Text("\"bar\"".into())),
                ("bar", LiteValue::Text("1".into())),
                ("baz", LiteValue::Text("true".into())),
                ("boo", LiteValue::Text("{\"key\":\"val\"}".into())),
            ])
        );

        // Already-stringified input -> no copy.
        let r = lite_row(
            &mk_row(vec![
                ("foo", PgValue::String("\"bar\"".into())),
                ("bar", PgValue::String("1".into())),
                ("baz", PgValue::String("true".into())),
                ("boo", PgValue::String("{\"key\":\"val\"}".into())),
            ]),
            &t,
            JsonFormat::Stringified,
        );
        assert!(!r.converted);
        assert_eq!(r.num_cols, 4);
    }

    #[test]
    fn lite_value_conversions() {
        let cases: Vec<(&str, PgValue, LiteValue)> = vec![
            ("int", PgValue::Number(1.0), LiteValue::Number(1.0)),
            (
                "string",
                PgValue::String("two".into()),
                LiteValue::Text("two".into()),
            ),
            ("string", PgValue::Null, LiteValue::Null),
            (
                "int",
                big(12313214123432),
                LiteValue::Big(BigInt::from(12313214123432i64)),
            ),
            ("float", PgValue::Number(123.456), LiteValue::Number(123.456)),
            ("bool", PgValue::Bool(true), LiteValue::Number(1.0)),
            ("boolean", PgValue::Bool(false), LiteValue::Number(0.0)),
            (
                "bytea",
                PgValue::Bytes(b"hello world".to_vec()),
                LiteValue::Blob(b"hello world".to_vec()),
            ),
            (
                "json",
                PgValue::Object(vec![(
                    "custom".into(),
                    PgValue::Object(vec![("json".into(), PgValue::String("object".into()))]),
                )]),
                LiteValue::Text("{\"custom\":{\"json\":\"object\"}}".into()),
            ),
            (
                "jsonb",
                PgValue::Array(vec![PgValue::Number(1.0), PgValue::Number(2.0)]),
                LiteValue::Text("[1,2]".into()),
            ),
            (
                "json",
                PgValue::Array(vec![
                    PgValue::String("two".into()),
                    PgValue::String("three".into()),
                ]),
                LiteValue::Text("[\"two\",\"three\"]".into()),
            ),
            (
                "json",
                PgValue::Array(vec![PgValue::Null, PgValue::Null]),
                LiteValue::Text("[null,null]".into()),
            ),
            (
                "int[]",
                PgValue::Array(vec![big(12313214123432), big(12313214123432)]),
                LiteValue::Text("[12313214123432,12313214123432]".into()),
            ),
            (
                "float[]",
                PgValue::Array(vec![PgValue::Number(123.456), PgValue::Number(987.654)]),
                LiteValue::Text("[123.456,987.654]".into()),
            ),
            (
                "bool[]",
                PgValue::Array(vec![PgValue::Bool(true), PgValue::Bool(false)]),
                LiteValue::Text("[true,false]".into()),
            ),
            (
                "json[][]",
                PgValue::Array(vec![
                    PgValue::Array(vec![
                        PgValue::Object(vec![(
                            "custom".into(),
                            PgValue::Object(vec![(
                                "json".into(),
                                PgValue::String("object".into()),
                            )]),
                        )]),
                        PgValue::Object(vec![(
                            "another".into(),
                            PgValue::Object(vec![(
                                "json".into(),
                                PgValue::String("object".into()),
                            )]),
                        )]),
                    ]),
                    PgValue::Array(vec![
                        PgValue::Object(vec![(
                            "custom".into(),
                            PgValue::Object(vec![("foo".into(), PgValue::String("bar".into()))]),
                        )]),
                        PgValue::Object(vec![(
                            "another".into(),
                            PgValue::Object(vec![("boo".into(), PgValue::String("far".into()))]),
                        )]),
                    ]),
                ]),
                LiteValue::Text(
                    "[[{\"custom\":{\"json\":\"object\"}},{\"another\":{\"json\":\"object\"}}],[{\"custom\":{\"foo\":\"bar\"}},{\"another\":{\"boo\":\"far\"}}]]"
                        .into(),
                ),
            ),
        ];

        for (data_type, input, expected) in cases {
            assert_eq!(
                lite_value(&input, data_type, JsonFormat::Parsed),
                expected,
                "liteValue {data_type}"
            );
        }
    }

    #[test]
    fn lite_type_to_zql_value_type_cases() {
        let cases: &[(&str, ValueType)] = &[
            ("int", ValueType::Number),
            ("isbn13", ValueType::String),
            ("macaddr8", ValueType::String),
            ("text", ValueType::String),
            ("float", ValueType::Number),
            ("bool", ValueType::Boolean),
            ("boolean", ValueType::Boolean),
            ("json", ValueType::Json),
            ("int[]|NOT_NULL", ValueType::Json),
            ("float[]", ValueType::Json),
            ("bool[]", ValueType::Json),
            ("json[]", ValueType::Json),
            ("f[]|TEXT_ENUM", ValueType::Json),
            ("f[]|TEXT_ENUM|TEXT_ARRAY", ValueType::Json),
            ("b[]", ValueType::Json),
            ("int|TEXT_ARRAY", ValueType::Json),
            ("float|TEXT_ARRAY", ValueType::Json),
            ("bool|TEXT_ARRAY", ValueType::Json),
            ("json|TEXT_ARRAY", ValueType::Json),
            ("int[]|TEXT_ARRAY", ValueType::Json),
            ("float[]|TEXT_ARRAY", ValueType::Json),
            ("bool[]|TEXT_ARRAY", ValueType::Json),
            ("json[]|TEXT_ARRAY", ValueType::Json),
        ];
        for &(lite_type, expected) in cases {
            assert_eq!(
                lite_type_to_zql_value_type(lite_type),
                Some(expected),
                "{lite_type}"
            );
        }
    }

    #[test]
    fn valid_column_specs_pass() {
        use crate::specs::PgTypeClass;

        assert!(assert_valid_lite_column_spec(&col("text", 1)).is_ok());
        assert!(assert_valid_lite_column_spec(&col("text|NOT_NULL", 1)).is_ok());

        let mut arr = col("text[]", 1);
        arr.elem_pg_type_class = Some(PgTypeClass::Base);
        // Missing |TEXT_ARRAY but has [] -> attribute mismatch (legacy form
        // without the attribute is not valid post-normalization).
        assert!(assert_valid_lite_column_spec(&arr).is_err());

        let mut arr2 = col("text[]|TEXT_ARRAY", 1);
        arr2.elem_pg_type_class = Some(PgTypeClass::Base);
        assert!(assert_valid_lite_column_spec(&arr2).is_ok());
    }

    #[test]
    fn array_attribute_must_match_brackets() {
        // |TEXT_ARRAY without [] is inconsistent.
        let spec = col("text|TEXT_ARRAY", 1);
        assert_eq!(
            assert_valid_lite_column_spec(&spec),
            Err(InvalidColumnSpecError::ArrayAttributeMismatch(
                "text|TEXT_ARRAY".into()
            ))
        );
    }

    #[test]
    fn brackets_must_match_elem_type_class() {
        // [] and |TEXT_ARRAY present but no elemPgTypeClass set.
        let spec = col("text[]|TEXT_ARRAY", 1);
        assert_eq!(
            assert_valid_lite_column_spec(&spec),
            Err(InvalidColumnSpecError::ElemTypeClassMismatch(
                "text[]|TEXT_ARRAY".into()
            ))
        );
    }

    #[test]
    fn rejects_brackets_after_pipe() {
        use crate::specs::PgTypeClass;
        // Both the attribute/bracket and elemPgTypeClass checks pass (both
        // "true"), but the `[]` appears after the `|`, which is invalid.
        let mut spec = col("foo|TEXT_ARRAY[]", 1);
        spec.elem_pg_type_class = Some(PgTypeClass::Base);
        assert_eq!(
            assert_valid_lite_column_spec(&spec),
            Err(InvalidColumnSpecError::InvalidDataType(
                "foo|TEXT_ARRAY[]".into()
            ))
        );
    }
}
