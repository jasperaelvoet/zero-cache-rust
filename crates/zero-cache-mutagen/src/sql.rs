//! Port of the SQL-generating functions in `services/mutagen/mutagen.ts`:
//! `getInsertSQL`/`getUpsertSQL`/`getUpdateSQL`/`getDeleteSQL`.
//!
//! Upstream builds these as `postgres.js` tagged-template queries, which
//! parameterizes every value (safe against injection by construction) and
//! delegates identifier quoting to the driver. This port instead generates
//! fully-inlined SQL text using `zero_cache_types::sql::{id, lit}` — the
//! same approach `zero-cache-sqlite::create` already uses for DDL
//! generation elsewhere in this port — rather than a parameterized-query
//! builder, since there's no live `postgres.js`-equivalent client-side
//! parameter binding layer here yet. Values are escaped via `lit`/numeric
//! formatting, so this remains injection-safe; it's a representation
//! choice (inlined vs. bound), not a safety one.

use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_types::sql::{id, lit};

use crate::crud_ops::{DeleteOp, InsertOp, PrimaryKeyValueRecord, Row, UpdateOp, UpsertOp};

/// Renders one value as a SQL literal. `Array`/`Object` values (e.g. a JSON
/// column) are rendered as a JSON-text string literal — upstream lets the
/// driver's own JS->SQL value coercion handle this; here it's made
/// explicit.
fn value_sql(v: &JsonValue) -> String {
    match v {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::BigInt(b) => b.to_string(),
        JsonValue::String(s) => lit(s),
        JsonValue::Array(_) | JsonValue::Object(_) => lit(&v.stringify()),
    }
}

fn row_columns_sql(row: &Row) -> String {
    format!(
        "({})",
        row.iter().map(|(k, _)| id(k)).collect::<Vec<_>>().join(",")
    )
}

fn row_values_sql(row: &Row) -> String {
    format!(
        "({})",
        row.iter()
            .map(|(_, v)| value_sql(v))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn get<'a>(row: &'a Row, col: &str) -> Option<&'a JsonValue> {
    row.iter().find(|(k, _)| k == col).map(|(_, v)| v)
}

/// Port of `getInsertSQL`.
pub fn get_insert_sql(op: &InsertOp) -> String {
    format!(
        "INSERT INTO {} {} VALUES {}",
        id(&op.table_name),
        row_columns_sql(&op.value),
        row_values_sql(&op.value)
    )
}

/// Port of `getUpsertSQL`.
pub fn get_upsert_sql(op: &UpsertOp) -> String {
    let set_clause = op
        .value
        .iter()
        .map(|(k, v)| format!("{} = {}", id(k), value_sql(v)))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "INSERT INTO {} {} VALUES {} ON CONFLICT ({}) DO UPDATE SET {}",
        id(&op.table_name),
        row_columns_sql(&op.value),
        row_values_sql(&op.value),
        op.primary_key
            .iter()
            .map(|k| id(k))
            .collect::<Vec<_>>()
            .join(","),
        set_clause
    )
}

/// Port of `getUpdateSQL`. Panics if `value` is missing any primary-key
/// column — port of upstream's `v.parse(value[key], primaryKeyValueSchema)`
/// (a required-field assertion, not a soft check).
pub fn get_update_sql(op: &UpdateOp) -> String {
    let set_clause = op
        .value
        .iter()
        .map(|(k, v)| format!("{} = {}", id(k), value_sql(v)))
        .collect::<Vec<_>>()
        .join(",");
    let where_clause = op
        .primary_key
        .iter()
        .map(|k| {
            let v = get(&op.value, k).unwrap_or_else(|| {
                panic!("getUpdateSQL: primary key column {k:?} missing from value")
            });
            format!("{} = {}", id(k), value_sql(v))
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "UPDATE {} SET {} WHERE {}",
        id(&op.table_name),
        set_clause,
        where_clause
    )
}

/// Port of `getDeleteSQL`.
pub fn get_delete_sql(op: &DeleteOp) -> String {
    let where_clause = op
        .primary_key
        .iter()
        .map(|k| format!("{} = {}", id(k), value_sql(pk_value(&op.value, k))))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!("DELETE FROM {} WHERE {}", id(&op.table_name), where_clause)
}

fn pk_value<'a>(record: &'a PrimaryKeyValueRecord, key: &str) -> &'a JsonValue {
    record
        .get(key)
        .unwrap_or_else(|| panic!("getDeleteSQL: primary key column {key:?} missing from value"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, JsonValue)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn insert_sql() {
        let op = InsertOp {
            table_name: "issues".into(),
            primary_key: vec!["id".into()],
            value: row(&[
                ("id", JsonValue::String("a".into())),
                ("title", JsonValue::String("hi".into())),
            ]),
        };
        assert_eq!(
            get_insert_sql(&op),
            "INSERT INTO \"issues\" (\"id\",\"title\") VALUES ('a','hi')"
        );
    }

    #[test]
    fn insert_sql_escapes_quotes_in_identifiers_and_values() {
        let op = InsertOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value: row(&[("id", JsonValue::String("it's".into()))]),
        };
        assert_eq!(
            get_insert_sql(&op),
            "INSERT INTO \"t\" (\"id\") VALUES ('it''s')"
        );
    }

    #[test]
    fn upsert_sql() {
        let op = UpsertOp {
            table_name: "issues".into(),
            primary_key: vec!["id".into()],
            value: row(&[
                ("id", JsonValue::String("a".into())),
                ("title", JsonValue::String("hi".into())),
            ]),
        };
        assert_eq!(
            get_upsert_sql(&op),
            "INSERT INTO \"issues\" (\"id\",\"title\") VALUES ('a','hi') ON CONFLICT (\"id\") DO UPDATE SET \"id\" = 'a',\"title\" = 'hi'"
        );
    }

    #[test]
    fn insert_sql_neutralizes_quote_injection() {
        // This port INLINES values into SQL text (upstream uses parameterized
        // postgres.js queries), so injection-safety rests entirely on `lit`
        // doubling quotes. Verify end-to-end that a malicious value stays
        // trapped inside its string literal and can't terminate it.
        let op = InsertOp {
            table_name: "issues".into(),
            primary_key: vec!["id".into()],
            value: row(&[
                ("id", JsonValue::String("a".into())),
                (
                    "title",
                    JsonValue::String("'); DROP TABLE issues;--".into()),
                ),
            ]),
        };
        let sql = get_insert_sql(&op);
        // The injected leading quote is doubled -> it is a literal quote char
        // inside the string, not a terminator; the DROP is inert text.
        assert_eq!(
            sql,
            "INSERT INTO \"issues\" (\"id\",\"title\") VALUES ('a','''); DROP TABLE issues;--')"
        );
    }

    #[test]
    fn upsert_sql_multi_column_primary_key() {
        // The `ON CONFLICT (pk1,pk2)` comma-joined conflict-target path is
        // distinct from the single-key case and only exercised by a composite
        // primary key.
        let op = UpsertOp {
            table_name: "issues".into(),
            primary_key: vec!["tenant".into(), "id".into()],
            value: row(&[
                ("tenant", JsonValue::String("t1".into())),
                ("id", JsonValue::String("a".into())),
                ("title", JsonValue::String("hi".into())),
            ]),
        };
        assert_eq!(
            get_upsert_sql(&op),
            "INSERT INTO \"issues\" (\"tenant\",\"id\",\"title\") VALUES ('t1','a','hi') ON CONFLICT (\"tenant\",\"id\") DO UPDATE SET \"tenant\" = 't1',\"id\" = 'a',\"title\" = 'hi'"
        );
    }

    #[test]
    fn update_sql() {
        let op = UpdateOp {
            table_name: "issues".into(),
            primary_key: vec!["id".into()],
            value: row(&[
                ("id", JsonValue::String("a".into())),
                ("title", JsonValue::String("new".into())),
            ]),
        };
        assert_eq!(
            get_update_sql(&op),
            "UPDATE \"issues\" SET \"id\" = 'a',\"title\" = 'new' WHERE \"id\" = 'a'"
        );
    }

    #[test]
    fn update_sql_multi_column_primary_key() {
        // The WHERE clause `AND`-joins every primary-key column.
        let op = UpdateOp {
            table_name: "issues".into(),
            primary_key: vec!["tenant".into(), "id".into()],
            value: row(&[
                ("tenant", JsonValue::String("t1".into())),
                ("id", JsonValue::String("a".into())),
                ("title", JsonValue::String("new".into())),
            ]),
        };
        assert_eq!(
            get_update_sql(&op),
            "UPDATE \"issues\" SET \"tenant\" = 't1',\"id\" = 'a',\"title\" = 'new' WHERE \"tenant\" = 't1' AND \"id\" = 'a'"
        );
    }

    #[test]
    #[should_panic(expected = "primary key column")]
    fn update_sql_panics_if_primary_key_missing_from_value() {
        let op = UpdateOp {
            table_name: "issues".into(),
            primary_key: vec!["id".into()],
            value: row(&[("title", JsonValue::String("new".into()))]),
        };
        get_update_sql(&op);
    }

    #[test]
    fn delete_sql() {
        let mut value = PrimaryKeyValueRecord::new();
        value.insert("id".into(), JsonValue::String("a".into()));
        let op = DeleteOp {
            table_name: "issues".into(),
            primary_key: vec!["id".into()],
            value,
        };
        assert_eq!(
            get_delete_sql(&op),
            "DELETE FROM \"issues\" WHERE \"id\" = 'a'"
        );
    }

    #[test]
    fn delete_sql_multi_column_primary_key() {
        let mut value = PrimaryKeyValueRecord::new();
        value.insert("a".into(), JsonValue::Number(1.0));
        value.insert("b".into(), JsonValue::Number(2.0));
        let op = DeleteOp {
            table_name: "t".into(),
            primary_key: vec!["a".into(), "b".into()],
            value,
        };
        assert_eq!(
            get_delete_sql(&op),
            "DELETE FROM \"t\" WHERE \"a\" = 1 AND \"b\" = 2"
        );
    }

    #[test]
    fn null_and_bool_values() {
        let op = InsertOp {
            table_name: "t".into(),
            primary_key: vec!["id".into()],
            value: row(&[
                ("id", JsonValue::Number(1.0)),
                ("active", JsonValue::Bool(true)),
                ("note", JsonValue::Null),
            ]),
        };
        assert_eq!(
            get_insert_sql(&op),
            "INSERT INTO \"t\" (\"id\",\"active\",\"note\") VALUES (1,TRUE,NULL)"
        );
    }
}
