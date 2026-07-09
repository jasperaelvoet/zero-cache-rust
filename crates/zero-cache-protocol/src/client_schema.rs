//! Port of `zero-protocol/src/client-schema.ts`.

use std::collections::BTreeMap;

/// Port of `ValueType` (a client-schema column's declared type).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    String,
    Number,
    Boolean,
    Null,
    Json,
}

/// Port of `ColumnSchema`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub value_type: ValueType,
}

/// Port of `TableSchema`. Uses `Vec<(String, _)>` rather than a `BTreeMap`
/// for `columns` so [`normalize_client_schema`] can model upstream's
/// explicit "sort for hashing" step as a real, observable operation rather
/// than something a sorted-by-construction map would make a no-op.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub columns: Vec<(String, ColumnSchema)>,
    pub primary_key: Vec<String>,
}

/// Port of `ClientSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientSchema {
    pub tables: Vec<(String, TableSchema)>,
}

/// Returns a normalized `ClientSchema` (tables and each table's columns
/// sorted by name, primary key sorted) suitable for hashing. Port of
/// `normalizeClientSchema`. Upstream runtime-asserts a primary key is
/// present (`must(table.primaryKey, ...)`, "new clients always specify a
/// primaryKey") even though its own schema type also requires the field;
/// this port makes `TableSchema::primary_key` non-optional, so the
/// invariant is enforced structurally instead of by a runtime check.
pub fn normalize_client_schema(schema: &ClientSchema) -> ClientSchema {
    let mut tables: Vec<(String, TableSchema)> = schema
        .tables
        .iter()
        .map(|(name, table)| {
            let mut columns = table.columns.clone();
            columns.sort_by(|(a, _), (b, _)| a.cmp(b));
            let mut primary_key = table.primary_key.clone();
            primary_key.sort();
            (
                name.clone(),
                TableSchema {
                    columns,
                    primary_key,
                },
            )
        })
        .collect();
    tables.sort_by(|(a, _), (b, _)| a.cmp(b));
    ClientSchema { tables }
}

/// Convenience: builds a `ClientSchema` from a `BTreeMap`-shaped input
/// (already-unique table names), for callers that don't need to preserve
/// input order. Not part of the upstream API.
pub fn client_schema_from_map(tables: BTreeMap<String, TableSchema>) -> ClientSchema {
    ClientSchema {
        tables: tables.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(t: ValueType) -> ColumnSchema {
        ColumnSchema { value_type: t }
    }

    #[test]
    fn normalize_sorts_tables_columns_and_primary_key() {
        let schema = ClientSchema {
            tables: vec![
                (
                    "b".into(),
                    TableSchema {
                        columns: vec![
                            ("z".into(), col(ValueType::String)),
                            ("a".into(), col(ValueType::Number)),
                        ],
                        primary_key: vec!["z".into(), "a".into()],
                    },
                ),
                (
                    "a".into(),
                    TableSchema {
                        columns: vec![],
                        primary_key: vec![],
                    },
                ),
            ],
        };
        let normalized = normalize_client_schema(&schema);
        assert_eq!(normalized.tables[0].0, "a");
        assert_eq!(normalized.tables[1].0, "b");
        assert_eq!(
            normalized.tables[1]
                .1
                .columns
                .iter()
                .map(|(n, _)| n.clone())
                .collect::<Vec<_>>(),
            vec!["a".to_string(), "z".to_string()]
        );
        assert_eq!(
            normalized.tables[1].1.primary_key,
            vec!["a".to_string(), "z".to_string()]
        );
    }

    #[test]
    fn normalize_is_idempotent() {
        let schema = ClientSchema {
            tables: vec![(
                "t".into(),
                TableSchema {
                    columns: vec![("a".into(), col(ValueType::Boolean))],
                    primary_key: vec!["a".into()],
                },
            )],
        };
        let once = normalize_client_schema(&schema);
        let twice = normalize_client_schema(&once);
        assert_eq!(once, twice);
    }
}
