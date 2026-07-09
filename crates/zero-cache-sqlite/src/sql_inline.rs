//! Port of `zqlite/src/internal/sql-inline.ts`'s `inlineValue`/
//! `compileInline` value-inlining half (the SQL-string-formatting
//! machinery around `@databases/sql`'s `SQLQuery`/`FormatConfig` is
//! upstream's own driver-integration layer, not ported — this port has no
//! equivalent query-builder abstraction to hook a custom `FormatConfig`
//! into, only the pure value->SQL-literal mapping it configures).
//!
//! WARNING (matching upstream's own, word for word): this must ONLY be
//! used for cost estimation in the SQLite query planner (i.e. by whatever
//! eventually ports `zqlite/src/sqlite-cost-model.ts`, part of the
//! still-entirely-unported `zql/src/planner` subsystem this is a
//! prerequisite for), where SQLite's own planner needs to see literal
//! values to make realistic index/plan decisions. Never use this for
//! actual query execution — every real query in this port already goes
//! through `rusqlite`'s parameter binding (see `StatementRunner`), which
//! is injection-safe; this function deliberately is NOT.

use zero_cache_shared::bigint_json::JsonValue;

/// Escapes a string for inline inclusion in SQLite SQL (single quotes,
/// doubled to escape). Port of `escapeSQLiteString`.
fn escape_sqlite_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Port of `inlineValue`: formats a [`JsonValue`] as a SQLite SQL literal
/// for inline (non-parameterized) inclusion. Numbers are rendered via
/// `JsonValue::stringify`'s own number formatting (matching upstream's
/// `String(value)`, both ultimately following JS's `Number::toString`
/// rules); booleans become SQLite's `1`/`0` (SQLite has no boolean type);
/// arrays and objects both fall back to a JSON-string literal, matching
/// upstream's `JSON.stringify(value)` for both cases (arrays aren't
/// special-cased there either, despite the doc comment's phrasing —
/// ported faithfully, not "fixed").
pub fn inline_value(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::String(s) => escape_sqlite_string(s),
        JsonValue::Number(_) | JsonValue::BigInt(_) => value.stringify(),
        JsonValue::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        JsonValue::Array(_) | JsonValue::Object(_) => escape_sqlite_string(&value.stringify()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_becomes_sql_null() {
        assert_eq!(inline_value(&JsonValue::Null), "NULL");
    }

    #[test]
    fn strings_are_quoted_and_escaped() {
        assert_eq!(
            inline_value(&JsonValue::String("hello".to_string())),
            "'hello'"
        );
        assert_eq!(
            inline_value(&JsonValue::String("it's".to_string())),
            "'it''s'"
        );
    }

    #[test]
    fn numbers_are_rendered_unquoted() {
        assert_eq!(inline_value(&JsonValue::Number(42.0)), "42");
        assert_eq!(inline_value(&JsonValue::Number(3.5)), "3.5");
    }

    #[test]
    fn booleans_become_sqlite_zero_or_one() {
        assert_eq!(inline_value(&JsonValue::Bool(true)), "1");
        assert_eq!(inline_value(&JsonValue::Bool(false)), "0");
    }

    #[test]
    fn arrays_are_json_stringified_and_quoted() {
        let arr = JsonValue::Array(vec![JsonValue::Number(1.0), JsonValue::Number(2.0)]);
        assert_eq!(inline_value(&arr), "'[1,2]'");
    }

    #[test]
    fn objects_are_json_stringified_and_quoted() {
        let obj = JsonValue::Object(vec![("a".to_string(), JsonValue::Number(1.0))]);
        assert_eq!(inline_value(&obj), "'{\"a\":1}'");
    }

    #[test]
    fn a_string_containing_json_special_chars_is_still_just_single_quote_escaped() {
        // Confirms strings take the direct escape path, not the
        // JSON.stringify path arrays/objects use.
        assert_eq!(
            inline_value(&JsonValue::String(r#"a"b"#.to_string())),
            "'a\"b'"
        );
    }
}
