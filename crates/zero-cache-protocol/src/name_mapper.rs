//! Port of `zero-schema/src/name-mapper.ts` + `zero-types/src/name-mapper.ts`'s
//! `NameMapper` class — translates table/column names between a client's
//! schema-declared names and a table's real Postgres names (a table or
//! column may declare a `serverName` distinct from the name application
//! code uses). Found via a directory-coverage scan of `zero-schema/src`
//! (previously only `compiled-permissions.ts` had any representation in
//! this table) — genuinely real, self-contained logic, not a re-export or
//! authoring-time type, and a real prerequisite for whoever eventually
//! wires ZQL query execution or write authorization against real Postgres
//! column names.
//!
//! Scope deviation: upstream's `TableSchema`/`SchemaValue` (the input this
//! module's `clientToServer`/`serverToClient` build a mapper from) are
//! deeply TS-generic-driven schema-authoring types (`zero-schema/src/
//! table-schema.ts`, mostly re-exports from the NOT-checked-out
//! `zero-types` package) — this port doesn't need that whole type system,
//! only the two fields `createMapperFrom` actually reads: a table's
//! (optional) `serverName` and each column's (optional) `serverName`. So
//! the input here is a minimal `TableNameInfo`/`ColumnNameInfo` pair
//! instead of a full `TableSchema`, carrying exactly what's used.
//! `zero-types/src/name-mapper.ts` itself isn't in the sparse checkout at
//! all (only `zero-cache`/`zql`/`zqlite`/`zero-protocol`/`zero-schema`
//! are) — fetched directly from the upstream GitHub source to port
//! `NameMapper` faithfully rather than guessing its shape.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::JsonValue;

/// Port of `DestNames`.
#[derive(Debug, Clone, PartialEq)]
pub struct DestNames {
    pub table_name: String,
    /// Source column name -> destination column name.
    pub columns: BTreeMap<String, String>,
    pub all_columns_same: bool,
}

/// Port of `NameMapper#getTable`'s failure — thrown as a generic `Error`
/// upstream, given its own type here since this port prefers typed errors
/// over string-matched messages.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown table {0:?}")]
pub struct UnknownTableError(pub String);

/// Port of `NameMapper#columnName`'s failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ColumnNameError {
    #[error("unknown table {0:?}")]
    UnknownTable(String),
    #[error("unknown column {0:?} of {1:?} table")]
    UnknownColumn(String, String),
}

/// Port of `NameMapper`.
#[derive(Debug, Clone, PartialEq)]
pub struct NameMapper {
    tables: BTreeMap<String, DestNames>,
}

impl NameMapper {
    pub fn new(tables: BTreeMap<String, DestNames>) -> Self {
        NameMapper { tables }
    }

    fn get_table(&self, src: &str) -> Result<&DestNames, UnknownTableError> {
        self.tables
            .get(src)
            .ok_or_else(|| UnknownTableError(src.to_string()))
    }

    /// Port of `tableName`.
    pub fn table_name(&self, src: &str) -> Result<&str, UnknownTableError> {
        Ok(&self.get_table(src)?.table_name)
    }

    /// Port of `tableNameIfKnown`.
    pub fn table_name_if_known(&self, src: &str) -> Option<&str> {
        self.tables.get(src).map(|t| t.table_name.as_str())
    }

    /// Port of `columnName`.
    pub fn column_name(&self, table: &str, src: &str) -> Result<&str, ColumnNameError> {
        let dest = self
            .get_table(table)
            .map_err(|e| ColumnNameError::UnknownTable(e.0))?;
        dest.columns
            .get(src)
            .map(String::as_str)
            .ok_or_else(|| ColumnNameError::UnknownColumn(src.to_string(), table.to_string()))
    }

    /// Port of `row`: renames a row's columns according to `table`'s
    /// mapping, matching upstream's "columns with unknown names simply
    /// pass through" behavior (not an error — only `columnName` errors on
    /// an unmapped column).
    pub fn row(
        &self,
        table: &str,
        row: &[(String, JsonValue)],
    ) -> Result<Vec<(String, JsonValue)>, UnknownTableError> {
        let dest = self.get_table(table)?;
        if dest.all_columns_same {
            return Ok(row.to_vec());
        }
        Ok(row
            .iter()
            .map(|(col, value)| {
                (
                    dest.columns
                        .get(col)
                        .cloned()
                        .unwrap_or_else(|| col.clone()),
                    value.clone(),
                )
            })
            .collect())
    }

    /// Port of `columns`: renames a list of column names, same
    /// pass-through-if-unmapped behavior as [`Self::row`].
    pub fn columns(
        &self,
        table: &str,
        cols: Option<&[String]>,
    ) -> Result<Option<Vec<String>>, UnknownTableError> {
        let Some(cols) = cols else { return Ok(None) };
        let dest = self.get_table(table)?;
        if dest.all_columns_same {
            return Ok(Some(cols.to_vec()));
        }
        Ok(Some(
            cols.iter()
                .map(|c| dest.columns.get(c).cloned().unwrap_or_else(|| c.clone()))
                .collect(),
        ))
    }
}

/// The minimal per-column info `createMapperFrom` actually reads from a
/// `SchemaValue` — see module doc on why this replaces the full
/// `TableSchema`/`SchemaValue` type system.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnNameInfo {
    pub name: String,
    pub server_name: Option<String>,
}

/// The minimal per-table info `createMapperFrom` actually reads from a
/// `TableSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableNameInfo {
    pub name: String,
    pub server_name: Option<String>,
    pub columns: Vec<ColumnNameInfo>,
}

/// Which direction a mapper translates. Port of `createMapperFrom`'s
/// `src: 'client' | 'server'` parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MapDirection {
    Client,
    Server,
}

fn create_mapper_from(direction: MapDirection, tables: &[TableNameInfo]) -> NameMapper {
    let mut mapping = BTreeMap::new();
    for table in tables {
        let mut all_columns_same = true;
        let mut names = BTreeMap::new();
        for col in &table.columns {
            if let Some(server_name) = &col.server_name {
                if server_name != &col.name {
                    all_columns_same = false;
                }
            }
            match direction {
                MapDirection::Client => {
                    names.insert(
                        col.name.clone(),
                        col.server_name.clone().unwrap_or_else(|| col.name.clone()),
                    );
                }
                MapDirection::Server => {
                    let server_key = col.server_name.clone().unwrap_or_else(|| col.name.clone());
                    names.insert(server_key, col.name.clone());
                }
            }
        }
        let (src_table_name, dest_table_name) = match direction {
            MapDirection::Client => (
                table.name.clone(),
                table
                    .server_name
                    .clone()
                    .unwrap_or_else(|| table.name.clone()),
            ),
            MapDirection::Server => (
                table
                    .server_name
                    .clone()
                    .unwrap_or_else(|| table.name.clone()),
                table.name.clone(),
            ),
        };
        mapping.insert(
            src_table_name,
            DestNames {
                table_name: dest_table_name,
                columns: names,
                all_columns_same,
            },
        );
    }
    NameMapper::new(mapping)
}

/// Port of `clientToServer`: a mapper from client-declared names to real
/// Postgres names.
pub fn client_to_server(tables: &[TableNameInfo]) -> NameMapper {
    create_mapper_from(MapDirection::Client, tables)
}

/// Port of `serverToClient`: the inverse mapper.
pub fn server_to_client(tables: &[TableNameInfo]) -> NameMapper {
    create_mapper_from(MapDirection::Server, tables)
}

/// Port of `validator`: an identity `NameMapper` whose only purpose is
/// validating that table/column names conform to `tables_to_columns` (an
/// unknown table/column still errors via `table_name`/`column_name`, even
/// though every KNOWN name maps to itself).
pub fn validator(tables_to_columns: &[(String, Vec<String>)]) -> NameMapper {
    let mapping = tables_to_columns
        .iter()
        .map(|(table_name, columns)| {
            (
                table_name.clone(),
                DestNames {
                    table_name: table_name.clone(),
                    columns: columns.iter().map(|c| (c.clone(), c.clone())).collect(),
                    all_columns_same: true,
                },
            )
        })
        .collect();
    NameMapper::new(mapping)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue_table() -> TableNameInfo {
        TableNameInfo {
            name: "issue".to_string(),
            server_name: Some("issues".to_string()),
            columns: vec![
                ColumnNameInfo {
                    name: "id".to_string(),
                    server_name: None,
                },
                ColumnNameInfo {
                    name: "ownerId".to_string(),
                    server_name: Some("owner_id".to_string()),
                },
            ],
        }
    }

    #[test]
    fn client_to_server_maps_table_and_column_names() {
        let mapper = client_to_server(&[issue_table()]);
        assert_eq!(mapper.table_name("issue").unwrap(), "issues");
        assert_eq!(mapper.column_name("issue", "ownerId").unwrap(), "owner_id");
        assert_eq!(mapper.column_name("issue", "id").unwrap(), "id");
    }

    #[test]
    fn server_to_client_is_the_inverse_mapping() {
        let mapper = server_to_client(&[issue_table()]);
        assert_eq!(mapper.table_name("issues").unwrap(), "issue");
        assert_eq!(mapper.column_name("issues", "owner_id").unwrap(), "ownerId");
    }

    #[test]
    fn unknown_table_errors() {
        let mapper = client_to_server(&[issue_table()]);
        assert_eq!(
            mapper.table_name("nope"),
            Err(UnknownTableError("nope".to_string()))
        );
        assert_eq!(mapper.table_name_if_known("nope"), None);
    }

    #[test]
    fn unknown_column_errors_but_row_passes_it_through() {
        let mapper = client_to_server(&[issue_table()]);
        assert!(matches!(
            mapper.column_name("issue", "bogus"),
            Err(ColumnNameError::UnknownColumn(_, _))
        ));

        let row = vec![("bogus".to_string(), JsonValue::Number(1.0))];
        let mapped = mapper.row("issue", &row).unwrap();
        assert_eq!(
            mapped, row,
            "an unmapped column must pass through unchanged, not error"
        );
    }

    #[test]
    fn row_renames_known_columns_and_leaves_unknown_ones() {
        let mapper = client_to_server(&[issue_table()]);
        let row = vec![
            ("id".to_string(), JsonValue::Number(1.0)),
            ("ownerId".to_string(), JsonValue::String("alice".into())),
        ];
        let mapped = mapper.row("issue", &row).unwrap();
        assert_eq!(
            mapped,
            vec![
                ("id".to_string(), JsonValue::Number(1.0)),
                ("owner_id".to_string(), JsonValue::String("alice".into()))
            ]
        );
    }

    #[test]
    fn row_is_a_pure_passthrough_when_all_columns_are_the_same() {
        let table = TableNameInfo {
            name: "t".to_string(),
            server_name: None,
            columns: vec![ColumnNameInfo {
                name: "id".to_string(),
                server_name: None,
            }],
        };
        let mapper = client_to_server(&[table]);
        let row = vec![("id".to_string(), JsonValue::Number(1.0))];
        assert_eq!(mapper.row("t", &row).unwrap(), row);
    }

    #[test]
    fn columns_renames_a_list_and_passes_through_none() {
        let mapper = client_to_server(&[issue_table()]);
        let cols = vec!["id".to_string(), "ownerId".to_string()];
        assert_eq!(
            mapper.columns("issue", Some(&cols)).unwrap(),
            Some(vec!["id".to_string(), "owner_id".to_string()])
        );
        assert_eq!(mapper.columns("issue", None).unwrap(), None);
    }

    #[test]
    fn validator_maps_every_known_name_to_itself_and_rejects_unknown() {
        let v = validator(&[(
            "issue".to_string(),
            vec!["id".to_string(), "ownerId".to_string()],
        )]);
        assert_eq!(v.table_name("issue").unwrap(), "issue");
        assert_eq!(v.column_name("issue", "ownerId").unwrap(), "ownerId");
        assert!(v.table_name("nope").is_err());
        assert!(v.column_name("issue", "bogus").is_err());
    }
}
