//! Port of `zero-cache/src/types/pg-data-type.ts`.
//!
//! Maps Postgres data type names to the ZQL [`ValueType`] used by clients.
//! Lookups strip any type arguments (e.g. `varchar(32)` -> `varchar`) and are
//! case-insensitive.

/// The ZQL value type of a column. Port of `ValueType` from
/// `zero-protocol/src/client-schema.ts` (defined here until that package is
/// ported).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    Null,
    String,
    Number,
    Boolean,
    Json,
}

/// Postgres numeric type names that map to ZQL `number`. Keys of
/// `pgToZqlNumericTypeMap`.
pub const NUMERIC_TYPES: &[&str] = &[
    "smallint",
    "integer",
    "int",
    "int2",
    "int4",
    "int8",
    "bigint",
    "smallserial",
    "serial",
    "serial2",
    "serial4",
    "serial8",
    "bigserial",
    "decimal",
    "numeric",
    "real",
    "double precision",
    "float",
    "float4",
    "float8",
];

/// Native Postgres string type names. Keys of `pgToZqlNativeStringTypeMap`.
pub const NATIVE_STRING_TYPES: &[&str] = &[
    "bpchar",
    "character",
    "character varying",
    "text",
    "varchar",
];

/// Text-represented Postgres type names (stored/compared as strings). Keys of
/// `pgToZqlTextRepresentedTypeMap`.
pub const TEXT_REPRESENTED_TYPES: &[&str] = &[
    "cidr", "ean13", "inet", "isbn", "isbn13", "ismn", "ismn13", "issn", "issn13", "macaddr",
    "macaddr8", "pg_lsn", "upc", "uuid",
];

/// Postgres date/time type names that map to ZQL `number`.
const DATE_TIME_TYPES: &[&str] = &[
    "date",
    "time",
    "timetz",
    "time with time zone",
    "time without time zone",
    "timestamp",
    "timestamptz",
    "timestamp with time zone",
    "timestamp without time zone",
];

/// Strips type args (e.g. `(32)` in `char(32)`) and lowercases, matching
/// `formatTypeForLookup`.
fn format_type_for_lookup(pg_type: &str) -> String {
    match pg_type.find('(') {
        None => pg_type.to_lowercase(),
        Some(idx) => pg_type[..idx].to_lowercase(),
    }
}

/// Whether `pg_type` maps to ZQL `number`. Port of `isPgNumberType`.
pub fn is_pg_number_type(pg_type: &str) -> bool {
    NUMERIC_TYPES.contains(&format_type_for_lookup(pg_type).as_str())
}

/// Whether `pg_type` is a native Postgres string type. Port of
/// `isPgNativeStringType`.
pub fn is_pg_native_string_type(pg_type: &str) -> bool {
    NATIVE_STRING_TYPES.contains(&format_type_for_lookup(pg_type).as_str())
}

/// Whether `pg_type` is a text-represented type. Port of
/// `isPgTextRepresentedType`.
pub fn is_pg_text_represented_type(pg_type: &str) -> bool {
    TEXT_REPRESENTED_TYPES.contains(&format_type_for_lookup(pg_type).as_str())
}

/// Whether `pg_type` maps to ZQL `string` (native or text-represented). Port of
/// `isPgStringType`.
pub fn is_pg_string_type(pg_type: &str) -> bool {
    let t = format_type_for_lookup(pg_type);
    NATIVE_STRING_TYPES.contains(&t.as_str()) || TEXT_REPRESENTED_TYPES.contains(&t.as_str())
}

/// Maps a Postgres data type to a ZQL [`ValueType`]. Port of
/// `dataTypeToZqlValueType`.
///
/// - Postgres arrays are treated as `json`.
/// - Unknown types that are enums map to `string`.
/// - Otherwise returns `None` for unmapped types.
pub fn data_type_to_zql_value_type(
    pg_type: &str,
    is_enum: bool,
    is_array: bool,
) -> Option<ValueType> {
    if is_array {
        return Some(ValueType::Json);
    }

    let t = format_type_for_lookup(pg_type);
    let value_type = if NUMERIC_TYPES.contains(&t.as_str()) || DATE_TIME_TYPES.contains(&t.as_str())
    {
        Some(ValueType::Number)
    } else if NATIVE_STRING_TYPES.contains(&t.as_str())
        || TEXT_REPRESENTED_TYPES.contains(&t.as_str())
    {
        Some(ValueType::String)
    } else {
        match t.as_str() {
            "bool" | "boolean" => Some(ValueType::Boolean),
            "json" | "jsonb" => Some(ValueType::Json),
            _ => None,
        }
    };

    if value_type.is_none() && is_enum {
        return Some(ValueType::String);
    }
    value_type
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_numeric_types() {
        for &t in NUMERIC_TYPES {
            assert!(is_pg_number_type(t), "{t} numeric");
            assert!(!is_pg_string_type(t), "{t} not string");
        }
    }

    #[test]
    fn identifies_string_types() {
        for &t in NATIVE_STRING_TYPES.iter().chain(TEXT_REPRESENTED_TYPES) {
            assert!(is_pg_string_type(t), "{t} string");
            assert!(!is_pg_number_type(t), "{t} not number");
        }
    }

    #[test]
    fn identifies_native_vs_text_represented() {
        for &t in NATIVE_STRING_TYPES {
            assert!(is_pg_native_string_type(t));
            assert!(!is_pg_text_represented_type(t));
        }
        for &t in TEXT_REPRESENTED_TYPES {
            assert!(is_pg_text_represented_type(t));
            assert!(!is_pg_native_string_type(t));
        }
    }

    #[test]
    fn case_insensitive() {
        assert!(is_pg_string_type("TEXT"));
        assert!(is_pg_native_string_type("TEXT"));
        assert!(is_pg_text_represented_type("ISBN13"));
        assert!(is_pg_number_type("INTEGER"));
    }

    #[test]
    fn maps_types() {
        let cases: &[(&str, ValueType)] = &[
            ("smallint", ValueType::Number),
            ("integer", ValueType::Number),
            ("int", ValueType::Number),
            ("int2", ValueType::Number),
            ("int4", ValueType::Number),
            ("int8", ValueType::Number),
            ("bigint", ValueType::Number),
            ("smallserial", ValueType::Number),
            ("serial", ValueType::Number),
            ("serial2", ValueType::Number),
            ("serial4", ValueType::Number),
            ("serial8", ValueType::Number),
            ("bigserial", ValueType::Number),
            ("decimal", ValueType::Number),
            ("numeric", ValueType::Number),
            ("real", ValueType::Number),
            ("double precision", ValueType::Number),
            ("float", ValueType::Number),
            ("float4", ValueType::Number),
            ("float8", ValueType::Number),
            ("date", ValueType::Number),
            ("time", ValueType::Number),
            ("timetz", ValueType::Number),
            ("timestamp", ValueType::Number),
            ("timestamptz", ValueType::Number),
            ("timestamp with time zone", ValueType::Number),
            ("timestamp without time zone", ValueType::Number),
            ("bpchar", ValueType::String),
            ("character", ValueType::String),
            ("character varying", ValueType::String),
            ("cidr", ValueType::String),
            ("ean13", ValueType::String),
            ("inet", ValueType::String),
            ("isbn", ValueType::String),
            ("isbn13", ValueType::String),
            ("ismn", ValueType::String),
            ("ismn13", ValueType::String),
            ("issn", ValueType::String),
            ("issn13", ValueType::String),
            ("macaddr", ValueType::String),
            ("macaddr8", ValueType::String),
            ("pg_lsn", ValueType::String),
            ("text", ValueType::String),
            ("upc", ValueType::String),
            ("uuid", ValueType::String),
            ("varchar", ValueType::String),
            ("bool", ValueType::Boolean),
            ("boolean", ValueType::Boolean),
            ("json", ValueType::Json),
            ("jsonb", ValueType::Json),
        ];
        for &(pg_type, expected) in cases {
            assert_eq!(
                data_type_to_zql_value_type(pg_type, false, false),
                Some(expected),
                "{pg_type}"
            );
            assert_eq!(
                data_type_to_zql_value_type(&pg_type.to_uppercase(), false, false),
                Some(expected),
                "{pg_type} upper"
            );
        }
    }

    #[test]
    fn enum_types() {
        for t in ["custom_enum_type", "another_enum"] {
            assert_eq!(
                data_type_to_zql_value_type(t, true, false),
                Some(ValueType::String)
            );
            assert_eq!(
                data_type_to_zql_value_type(t, true, true),
                Some(ValueType::Json)
            );
        }
    }

    #[test]
    fn unmapped_types() {
        for t in ["bytea", "unknown_type"] {
            assert_eq!(data_type_to_zql_value_type(t, false, false), None);
        }
    }

    #[test]
    fn value_type_precedence_array_over_known_type_over_enum_fallback() {
        // is_array wins over everything, even a known scalar type or enum.
        assert_eq!(
            data_type_to_zql_value_type("text", false, true),
            Some(ValueType::Json)
        );
        assert_eq!(
            data_type_to_zql_value_type("integer", true, true),
            Some(ValueType::Json)
        );
        // A KNOWN type is NOT overridden by the enum flag — enum is a fallback
        // only for otherwise-unmapped types (matches upstream's
        // `valueType === undefined && isEnum`). A reordering bug that checked
        // enum first would mistype these.
        assert_eq!(
            data_type_to_zql_value_type("integer", true, false),
            Some(ValueType::Number)
        );
        assert_eq!(
            data_type_to_zql_value_type("text", true, false),
            Some(ValueType::String)
        );
        // Enum applies only when the type is unmapped.
        assert_eq!(
            data_type_to_zql_value_type("mystatus", true, false),
            Some(ValueType::String)
        );
        assert_eq!(data_type_to_zql_value_type("mystatus", false, false), None);
    }
}
