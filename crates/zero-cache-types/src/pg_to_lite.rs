//! Port of `zero-cache/src/db/pg-to-lite.ts`.
//!
//! Converts Postgres column/table/index specs into their SQLite ("lite")
//! equivalents, including the conservative allowlist for translating column
//! defaults and the appended `_0_version` column.

use std::sync::OnceLock;

use regex::Regex;
use thiserror::Error;

use crate::lite::lite_type_string;
use crate::names::{lite_table_name, TableName};
use crate::specs::{ColumnSpec, IndexSpec, LiteIndexSpec, LiteTableSpec, PgTypeClass, TableSpec};

/// The name of the version column appended to every replicated table.
pub const ZERO_VERSION_COLUMN_NAME: &str = "_0_version";

/// A column default that cannot be safely translated to SQLite (triggers
/// backfill). Port of `UnsupportedColumnDefaultError`.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("{0}")]
pub struct UnsupportedColumnDefaultError(pub String);

/// Whether a Postgres column is an enum (checking the element type class for
/// arrays of enums, else the main type class). Port of `isEnumColumn`.
pub fn is_enum_column(spec: &ColumnSpec) -> bool {
    spec.elem_pg_type_class.or(spec.pg_type_class) == Some(PgTypeClass::Enum)
}

/// Whether a Postgres column is an array (non-null element type class). Port of
/// `isArrayColumn`.
pub fn is_array_column(spec: &ColumnSpec) -> bool {
    spec.elem_pg_type_class.is_some()
}

fn re(cell: &'static OnceLock<Regex>, pat: &str) -> &'static Regex {
    cell.get_or_init(|| Regex::new(pat).unwrap())
}

/// Translates a Postgres column default into a SQLite default, or errors if the
/// expression is not on the safe allowlist. Port of `mapPostgresToLiteDefault`.
pub fn map_postgres_to_lite_default(
    table: &str,
    column: &str,
    default_expression: Option<&str>,
) -> Result<Option<String>, UnsupportedColumnDefaultError> {
    static NUMERIC: OnceLock<Regex> = OnceLock::new();
    static BOOLEAN: OnceLock<Regex> = OnceLock::new();
    static QUOTED_CAST: OnceLock<Regex> = OnceLock::new();
    static ARRAY_CTOR: OnceLock<Regex> = OnceLock::new();
    static ARRAY_LIT: OnceLock<Regex> = OnceLock::new();

    let expr = match default_expression {
        None | Some("") => return Ok(None),
        Some(e) => e,
    };

    if re(&NUMERIC, r"^-?\d+(\.\d+)?$").is_match(expr) {
        return Ok(Some(expr.to_string()));
    }
    if re(&BOOLEAN, r"^(true|false)$").is_match(expr) {
        return Ok(Some(if expr == "true" { "1" } else { "0" }.to_string()));
    }
    if let Some(caps) = re(&QUOTED_CAST, r"^('.*')::(\w+)$").captures(expr) {
        return Ok(Some(caps[1].to_string()));
    }
    if re(&ARRAY_CTOR, r"(?i)^ARRAY\s*\[\s*\]::\w+\[\]$").is_match(expr)
        || re(&ARRAY_LIT, r"^'\{\}'::\w+\[\]$").is_match(expr)
    {
        return Ok(Some("'[]'".to_string()));
    }

    Err(UnsupportedColumnDefaultError(format!(
        "Unsupported default value for {table}.{column}: {expr}"
    )))
}

/// Maps a Postgres column spec to a lite column spec. Port of
/// `mapPostgresToLiteColumn`. NOT NULL is always dropped for the replica;
/// `ignore_default` drops the default (used for CREATE TABLE, where there are no
/// existing rows).
pub fn map_postgres_to_lite_column(
    table: &str,
    column_name: &str,
    spec: &ColumnSpec,
    ignore_default: bool,
) -> Result<ColumnSpec, UnsupportedColumnDefaultError> {
    let lite_type = lite_type_string(
        &spec.data_type,
        spec.not_null.unwrap_or(false),
        is_enum_column(spec),
        is_array_column(spec),
    );
    let dflt = if ignore_default {
        None
    } else {
        map_postgres_to_lite_default(table, column_name, spec.dflt.as_deref())?
    };
    Ok(ColumnSpec {
        pos: spec.pos,
        data_type: lite_type,
        pg_type_class: None,
        elem_pg_type_class: spec.elem_pg_type_class,
        character_maximum_length: None,
        not_null: Some(false),
        dflt,
    })
}

/// The appended `_0_version` column spec. Port of `zeroVersionColumnSpec`.
fn zero_version_column_spec(default_version: Option<&str>) -> ColumnSpec {
    ColumnSpec {
        pos: crate::lexi_version::MAX_SAFE_INTEGER as i64,
        data_type: "text".into(),
        pg_type_class: None,
        elem_pg_type_class: None,
        character_maximum_length: None,
        not_null: Some(false),
        dflt: default_version.map(|v| format!("'{v}'")),
    }
}

/// Maps a Postgres table spec to a lite table spec: renames to the lite table
/// name, drops schema/primary-key, maps each column (ignoring defaults), and
/// appends the `_0_version` column. Port of `mapPostgresToLite`.
pub fn map_postgres_to_lite(
    t: &TableSpec,
    default_version: Option<&str>,
) -> Result<LiteTableSpec, UnsupportedColumnDefaultError> {
    let name = lite_table_name(&TableName {
        schema: &t.schema,
        name: &t.name,
    });
    let mut columns = Vec::with_capacity(t.columns.len() + 1);
    for (col, spec) in &t.columns {
        columns.push((
            col.clone(),
            map_postgres_to_lite_column(&name, col, spec, true)?,
        ));
    }
    columns.push((
        ZERO_VERSION_COLUMN_NAME.to_string(),
        zero_version_column_spec(default_version),
    ));
    Ok(LiteTableSpec {
        name,
        columns,
        primary_key: None,
    })
}

/// Maps a Postgres index spec to a lite index spec (lite table/index names).
/// Port of `mapPostgresToLiteIndex`.
pub fn map_postgres_to_lite_index(index: &IndexSpec) -> LiteIndexSpec {
    LiteIndexSpec {
        table_name: lite_table_name(&TableName {
            schema: &index.schema,
            name: &index.table_name,
        }),
        name: lite_table_name(&TableName {
            schema: &index.schema,
            name: &index.name,
        }),
        unique: index.unique,
        columns: index.columns.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(
        pos: i64,
        data_type: &str,
        not_null: bool,
        dflt: Option<&str>,
        elem: Option<PgTypeClass>,
    ) -> ColumnSpec {
        ColumnSpec {
            pos,
            data_type: data_type.into(),
            pg_type_class: None,
            elem_pg_type_class: elem,
            character_maximum_length: None,
            not_null: Some(not_null),
            dflt: dflt.map(|s| s.into()),
        }
    }

    #[test]
    fn postgres_to_lite_column_cases() {
        // (input, expected lite)
        let cases: Vec<(ColumnSpec, ColumnSpec)> = vec![
            (
                col(3, "int8", true, Some("2147483648"), None),
                col(3, "int8|NOT_NULL", false, Some("2147483648"), None),
            ),
            (
                col(4, "int8", false, Some("'9007199254740992'::bigint"), None),
                col(4, "int8", false, Some("'9007199254740992'"), None),
            ),
            (
                col(5, "text", false, Some("'foo'::string"), None),
                col(5, "text", false, Some("'foo'"), None),
            ),
            (
                col(6, "bool", false, Some("true"), None),
                col(6, "bool", false, Some("1"), None),
            ),
            (
                col(7, "bool", false, Some("false"), None),
                col(7, "bool", false, Some("0"), None),
            ),
            (
                col(8, "int4[]", false, None, Some(PgTypeClass::Base)),
                col(8, "int4[]|TEXT_ARRAY", false, None, Some(PgTypeClass::Base)),
            ),
            (
                col(9, "my_enum[]", false, None, Some(PgTypeClass::Enum)),
                col(
                    9,
                    "my_enum[]|TEXT_ENUM|TEXT_ARRAY",
                    false,
                    None,
                    Some(PgTypeClass::Enum),
                ),
            ),
            (
                col(10, "int4[][]", false, None, Some(PgTypeClass::Base)),
                col(
                    10,
                    "int4[][]|TEXT_ARRAY",
                    false,
                    None,
                    Some(PgTypeClass::Base),
                ),
            ),
            (
                col(11, "my_enum[][]", false, None, Some(PgTypeClass::Enum)),
                col(
                    11,
                    "my_enum[][]|TEXT_ENUM|TEXT_ARRAY",
                    false,
                    None,
                    Some(PgTypeClass::Enum),
                ),
            ),
        ];
        for (pg, lite) in cases {
            assert_eq!(
                map_postgres_to_lite_column("foo", "bar", &pg, false).unwrap(),
                lite
            );
        }
    }

    #[test]
    fn supported_defaults() {
        let cases: &[(&str, &str)] = &[
            ("123", "123"),
            ("0", "0"),
            ("-456", "-456"),
            ("2147483648", "2147483648"),
            ("123.456", "123.456"),
            ("-0.5", "-0.5"),
            ("true", "1"),
            ("false", "0"),
            ("'12345678901234567890'::bigint", "'12345678901234567890'"),
            ("ARRAY[]::text[]", "'[]'"),
            ("'{}'::integer[]", "'[]'"),
        ];
        for &(input, expected) in cases {
            assert_eq!(
                map_postgres_to_lite_default("foo", "bar", Some(input)).unwrap(),
                Some(expected.to_string()),
                "{input}"
            );
        }
        assert_eq!(
            map_postgres_to_lite_default("foo", "bar", None).unwrap(),
            None
        );
    }

    #[test]
    fn unsupported_defaults() {
        for bad in [
            "(id + 2)",
            "generate(id)",
            "now()",
            "current_timestamp",
            "CURRENT_TIMESTAMP",
            "LOCALTIME",
            "CURRENT_USER",
            "ARRAY['a', 'b']::text[]",
            "ARRAY[1,2,3]::integer[]",
            "1::integer",
            "0::smallint",
            "true AND false",
            "NOT true",
            "uuid_generate_v4()",
            "gen_random_uuid()",
            "'foo'",
            "'hello world'",
        ] {
            assert!(
                map_postgres_to_lite_default("foo", "bar", Some(bad)).is_err(),
                "{bad} should be unsupported"
            );
        }
    }

    #[test]
    fn map_table_appends_zero_version() {
        let t = TableSpec {
            name: "issues".into(),
            schema: "public".into(),
            columns: vec![("id".into(), col(1, "int8", true, None, None))],
            primary_key: Some(vec!["id".into()]),
        };
        let lite = map_postgres_to_lite(&t, Some("2b8a")).unwrap();
        assert_eq!(lite.name, "issues");
        assert_eq!(lite.primary_key, None);
        assert_eq!(lite.columns.len(), 2);
        assert_eq!(lite.columns[0].0, "id");
        // id: NOT NULL dropped, default ignored on create.
        assert_eq!(lite.columns[0].1.data_type, "int8|NOT_NULL");
        assert_eq!(lite.columns[0].1.dflt, None);
        // _0_version appended with the default version.
        let (name, zv) = &lite.columns[1];
        assert_eq!(name, "_0_version");
        assert_eq!(zv.data_type, "text");
        assert_eq!(zv.dflt, Some("'2b8a'".to_string()));
    }
}
