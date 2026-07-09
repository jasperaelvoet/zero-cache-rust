//! Port of the pure conversion helpers in
//! `zero-cache/src/services/replicator/schema/column-metadata.ts`.
//!
//! [`ColumnMetadata`] is the structured form of a column's type information;
//! these helpers convert between it, the pipe-delimited [`LiteTypeString`], and
//! Postgres [`ColumnSpec`]s. The SQLite-backed `ColumnMetadataStore` (CRUD) is
//! ported separately in the `zero-cache-sqlite` crate.
//!
//! [`LiteTypeString`]: crate::lite::LiteTypeString

use crate::lite::{is_array, is_enum, lite_type_string, nullable_upstream, upstream_data_type};
use crate::pg_to_lite::{is_array_column, is_enum_column};
use crate::specs::ColumnSpec;

/// Structured column type metadata. Port of `ColumnMetadata`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMetadata {
    pub upstream_type: String,
    pub is_not_null: bool,
    pub is_enum: bool,
    pub is_array: bool,
    pub character_max_length: Option<i64>,
    pub is_backfilling: bool,
}

/// Converts a pipe-delimited [`LiteTypeString`] to [`ColumnMetadata`]. Port of
/// `liteTypeStringToMetadata`.
///
/// [`LiteTypeString`]: crate::lite::LiteTypeString
pub fn lite_type_string_to_metadata(
    lite_type_string: &str,
    character_max_length: Option<i64>,
) -> ColumnMetadata {
    let base_type = upstream_data_type(lite_type_string);
    let is_array_type = is_array(lite_type_string);

    // Reconstruct the full upstream type including array notation. New-style
    // arrays (`text[]`) already carry `[]`; old-style (`int4|NOT_NULL[]`) lose
    // it via `upstream_data_type`, so re-append.
    let full_upstream_type = if is_array_type && !base_type.contains("[]") {
        format!("{base_type}[]")
    } else {
        base_type.to_string()
    };

    ColumnMetadata {
        upstream_type: full_upstream_type,
        is_not_null: !nullable_upstream(lite_type_string),
        is_enum: is_enum(lite_type_string),
        is_array: is_array_type,
        character_max_length,
        is_backfilling: false,
    }
}

/// Converts [`ColumnMetadata`] back to a pipe-delimited lite type string,
/// normalizing to new-style attributes. Port of `metadataToLiteTypeString`.
pub fn metadata_to_lite_type_string(metadata: &ColumnMetadata) -> String {
    lite_type_string(
        &metadata.upstream_type,
        metadata.is_not_null,
        metadata.is_enum,
        metadata.is_array,
    )
}

/// Converts a Postgres [`ColumnSpec`] to [`ColumnMetadata`]. Port of
/// `pgColumnSpecToMetadata`.
pub fn pg_column_spec_to_metadata(spec: &ColumnSpec) -> ColumnMetadata {
    ColumnMetadata {
        upstream_type: spec.data_type.clone(),
        is_not_null: spec.not_null.unwrap_or(false),
        is_enum: is_enum_column(spec),
        is_array: is_array_column(spec),
        character_max_length: spec.character_maximum_length,
        is_backfilling: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::specs::PgTypeClass;

    fn md(
        upstream_type: &str,
        is_not_null: bool,
        is_enum: bool,
        is_array: bool,
        cml: Option<i64>,
    ) -> ColumnMetadata {
        ColumnMetadata {
            upstream_type: upstream_type.into(),
            is_not_null,
            is_enum,
            is_array,
            character_max_length: cml,
            is_backfilling: false,
        }
    }

    #[test]
    fn pipe_to_structured() {
        assert_eq!(
            lite_type_string_to_metadata("int8", None),
            md("int8", false, false, false, None)
        );
        assert_eq!(
            lite_type_string_to_metadata("varchar|NOT_NULL", Some(255)),
            md("varchar", true, false, false, Some(255))
        );
        assert_eq!(
            lite_type_string_to_metadata("user_role|TEXT_ENUM", None),
            md("user_role", false, true, false, None)
        );
        // Old-style arrays.
        assert_eq!(
            lite_type_string_to_metadata("text[]", None),
            md("text[]", false, false, true, None)
        );
        assert_eq!(
            lite_type_string_to_metadata("int4|NOT_NULL[]", None),
            md("int4[]", true, false, true, None)
        );
        // New-style arrays.
        assert_eq!(
            lite_type_string_to_metadata("text[]|TEXT_ARRAY", None),
            md("text[]", false, false, true, None)
        );
        assert_eq!(
            lite_type_string_to_metadata("int4[]|NOT_NULL|TEXT_ARRAY", None),
            md("int4[]", true, false, true, None)
        );
        assert_eq!(
            lite_type_string_to_metadata("user_role[]|TEXT_ENUM|TEXT_ARRAY", None),
            md("user_role[]", false, true, true, None)
        );
    }

    #[test]
    fn round_trip_normalizes_to_new_style() {
        // Simple types are stable.
        for t in ["int8", "text", "varchar"] {
            let m = lite_type_string_to_metadata(t, None);
            assert_eq!(metadata_to_lite_type_string(&m), t);
        }
        // Enum.
        let m = lite_type_string_to_metadata("user_role|TEXT_ENUM", None);
        assert_eq!(metadata_to_lite_type_string(&m), "user_role|TEXT_ENUM");
        // Old-style array normalizes to new-style.
        let m = lite_type_string_to_metadata("int4|NOT_NULL[]", None);
        assert_eq!(
            metadata_to_lite_type_string(&m),
            "int4[]|NOT_NULL|TEXT_ARRAY"
        );
        // New-style array stable.
        let m = lite_type_string_to_metadata("text[]|TEXT_ARRAY", None);
        assert_eq!(metadata_to_lite_type_string(&m), "text[]|TEXT_ARRAY");
        // Array of enum with NOT NULL preserves all attributes.
        let m = lite_type_string_to_metadata("user_role[]|NOT_NULL|TEXT_ENUM|TEXT_ARRAY", None);
        assert_eq!(
            metadata_to_lite_type_string(&m),
            "user_role[]|NOT_NULL|TEXT_ENUM|TEXT_ARRAY"
        );
    }

    #[test]
    fn from_pg_column_spec() {
        let spec = ColumnSpec {
            pos: 1,
            data_type: "my_enum[]".into(),
            pg_type_class: None,
            elem_pg_type_class: Some(PgTypeClass::Enum),
            character_maximum_length: Some(0),
            not_null: Some(true),
            dflt: None,
        };
        assert_eq!(
            pg_column_spec_to_metadata(&spec),
            md("my_enum[]", true, true, true, Some(0))
        );
    }
}
