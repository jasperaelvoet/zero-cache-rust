//! Port of upstream's `validatePublications` /
//! `change-source/pg/schema/validation.ts`'s `validate` — the guard run between
//! introspecting the published schema (`getPublicationInfo`) and copying it, so
//! initial sync fails cleanly on a schema it cannot replicate rather than
//! emitting a broken `CREATE TABLE`.
//!
//! A table is rejected (upstream throws `UnsupportedTableSchemaError`) when:
//!   - it uses the reserved `_0_version` column name;
//!   - its `REPLICA IDENTITY` is `NOTHING` (nothing to key rows by);
//!   - its `REPLICA IDENTITY` is `INDEX` but no usable replica-identity index
//!     exists (no columns);
//!   - its name has characters outside `[A-Za-z_][A-Za-z0-9_-]*`;
//!   - any mapped (lite) column name has characters outside
//!     `[A-Za-z_][.A-Za-z0-9_-]*`.
//!
//! A table with no primary key and the default replica identity is *not*
//! rejected here (upstream only warns); replication still needs a key, but the
//! rejection for that case is out of scope for this guard.

use zero_cache_types::pg_to_lite::{map_postgres_to_lite, ZERO_VERSION_COLUMN_NAME};
use zero_cache_types::specs::{
    ColumnSpec, PublishedIndexSpec, PublishedTableSpec, ReplicaIdentity, TableSpec,
};

/// The error raised when a published table cannot be replicated. Port of
/// upstream's `UnsupportedTableSchemaError`.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct UnsupportedTableSchemaError(pub String);

impl UnsupportedTableSchemaError {
    fn new(msg: impl Into<String>) -> Self {
        UnsupportedTableSchemaError(msg.into())
    }
}

/// `^[A-Za-z_]+[A-Za-z0-9_-]*$` — the allowed table-name shape. Implemented by
/// hand to avoid pulling in a regex dependency for two fixed patterns.
fn is_valid_table_name(name: &str) -> bool {
    let mut chars = name.chars();
    // At least one leading `[A-Za-z_]`.
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    // The leading run may continue with more `[A-Za-z_]`, and the whole rest
    // may include digits and `-`; since every later-allowed char is a superset
    // of the leading class, a single pass over the remainder suffices.
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// `^[A-Za-z_]+[.A-Za-z0-9_-]*$` — table columns additionally allow `.` after
/// the first character (there is no schema/table delimiter needed once mapped
/// to a SQLite column name).
fn is_valid_column_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// The columns that make up a table's replica identity, mirroring upstream's
/// denormalized `replicaIdentityColumns`. Only the `Index` case is needed for
/// validation (its emptiness is the rejection trigger); it is the set of
/// columns of the table's replica-identity index.
fn replica_identity_index_columns(
    table: &PublishedTableSpec,
    indexes: &[PublishedIndexSpec],
) -> Vec<String> {
    indexes
        .iter()
        .find(|ind| {
            ind.schema == table.schema
                && ind.table_name == table.name
                && ind.is_replica_identity == Some(true)
        })
        .map(|ind| ind.columns.iter().map(|(c, _)| c.clone()).collect())
        .unwrap_or_default()
}

/// Projects a [`PublishedTableSpec`] down to the [`TableSpec`] that
/// `map_postgres_to_lite` consumes, so the column-name check runs against the
/// mapped (lite) names exactly as upstream does.
fn to_table_spec(table: &PublishedTableSpec) -> TableSpec {
    TableSpec {
        name: table.name.clone(),
        schema: table.schema.clone(),
        columns: table
            .columns
            .iter()
            .map(|(name, c)| (name.clone(), c.column.clone()))
            .collect::<Vec<(String, ColumnSpec)>>(),
        primary_key: table.primary_key.clone(),
    }
}

/// Validates a single published table. Port of `validation.ts`'s `validate`.
pub fn validate_table(
    table: &PublishedTableSpec,
    indexes: &[PublishedIndexSpec],
) -> Result<(), UnsupportedTableSchemaError> {
    if table
        .columns
        .iter()
        .any(|(name, _)| name == ZERO_VERSION_COLUMN_NAME)
    {
        return Err(UnsupportedTableSchemaError::new(format!(
            "Table \"{}\" uses reserved column name \"{ZERO_VERSION_COLUMN_NAME}\"",
            table.name
        )));
    }

    // A missing primary key with the default replica identity only warrants a
    // warning upstream (not a rejection); not enforced here.

    if table.replica_identity == Some(ReplicaIdentity::Nothing) {
        return Err(UnsupportedTableSchemaError::new(format!(
            "Table \"{}\" with REPLICA IDENTITY NOTHING cannot be replicated",
            table.name
        )));
    }

    if table.replica_identity == Some(ReplicaIdentity::Index)
        && replica_identity_index_columns(table, indexes).is_empty()
    {
        return Err(UnsupportedTableSchemaError::new(format!(
            "Table \"{}\" is missing its REPLICA IDENTITY INDEX",
            table.name
        )));
    }

    if !is_valid_table_name(&table.name) {
        return Err(UnsupportedTableSchemaError::new(format!(
            "Table \"{}\" has invalid characters.",
            table.name
        )));
    }

    // Column-name check runs against the mapped (lite) column names. If the
    // mapping itself fails (an unsupported column default), that is surfaced as
    // an unsupported-schema error too rather than a later broken CREATE TABLE.
    let mapped = map_postgres_to_lite(&to_table_spec(table), None).map_err(|e| {
        UnsupportedTableSchemaError::new(format!(
            "Table \"{}\" cannot be mapped for replication: {e}",
            table.name
        ))
    })?;
    for (col, _) in &mapped.columns {
        if !is_valid_column_name(col) {
            return Err(UnsupportedTableSchemaError::new(format!(
                "Column \"{col}\" in table \"{}\" has invalid characters.",
                table.name
            )));
        }
    }

    Ok(())
}

/// Validates every published table. Port of `validatePublications` (the
/// per-publication event check upstream also performs is handled at publication
/// setup in this port).
pub fn validate_publications(
    tables: &[PublishedTableSpec],
    indexes: &[PublishedIndexSpec],
) -> Result<(), UnsupportedTableSchemaError> {
    for table in tables {
        validate_table(table, indexes)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_types::specs::{Direction, PublishedColumnSpec};

    fn col(name: &str, pos: i64) -> (String, PublishedColumnSpec) {
        (
            name.to_string(),
            PublishedColumnSpec {
                type_oid: 23,
                column: ColumnSpec::new("int4", pos),
            },
        )
    }

    fn base_table() -> PublishedTableSpec {
        PublishedTableSpec {
            name: "foo".into(),
            schema: "public".into(),
            oid: 0,
            schema_oid: None,
            columns: vec![col("id", 1), col("name", 2)],
            primary_key: Some(vec!["id".into()]),
            replica_identity: Some(ReplicaIdentity::Default),
            publications: Default::default(),
        }
    }

    #[test]
    fn accepts_a_well_formed_table() {
        assert!(validate_publications(&[base_table()], &[]).is_ok());
    }

    #[test]
    fn rejects_replica_identity_nothing() {
        let mut t = base_table();
        t.replica_identity = Some(ReplicaIdentity::Nothing);
        let err = validate_publications(&[t], &[]).unwrap_err();
        assert!(
            err.to_string().contains("REPLICA IDENTITY NOTHING"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_replica_identity_index_without_usable_index() {
        let mut t = base_table();
        t.replica_identity = Some(ReplicaIdentity::Index);
        // No matching replica-identity index supplied.
        let err = validate_publications(&[t], &[]).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing its REPLICA IDENTITY INDEX"),
            "got: {err}"
        );
    }

    #[test]
    fn accepts_replica_identity_index_with_usable_index() {
        let mut t = base_table();
        t.replica_identity = Some(ReplicaIdentity::Index);
        let index = PublishedIndexSpec {
            name: "foo_ukey".into(),
            table_name: "foo".into(),
            schema: "public".into(),
            unique: true,
            columns: vec![("id".into(), Direction::Asc)],
            is_replica_identity: Some(true),
            is_primary_key: Some(false),
            is_immediate: Some(true),
        };
        assert!(validate_publications(&[t], &[index]).is_ok());
    }

    #[test]
    fn rejects_reserved_version_column() {
        let mut t = base_table();
        t.columns.push(col("_0_version", 3));
        let err = validate_publications(&[t], &[]).unwrap_err();
        assert!(
            err.to_string().contains("reserved column name")
                && err.to_string().contains("_0_version"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_invalid_table_name() {
        let mut t = base_table();
        t.name = "has space".into();
        let err = validate_publications(&[t], &[]).unwrap_err();
        assert!(
            err.to_string().contains("has invalid characters"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_invalid_column_name() {
        let mut t = base_table();
        t.columns.push(col("bad col", 3));
        let err = validate_publications(&[t], &[]).unwrap_err();
        assert!(
            err.to_string()
                .contains("Column \"bad col\" in table \"foo\" has invalid characters"),
            "got: {err}"
        );
    }

    #[test]
    fn name_char_rules_match_upstream_patterns() {
        assert!(is_valid_table_name("Foo_bar-1"));
        assert!(!is_valid_table_name("1foo"));
        assert!(!is_valid_table_name("foo.bar"));
        assert!(!is_valid_table_name(""));
        // Columns additionally allow dots (but not as the first character).
        assert!(is_valid_column_name("a.b.c"));
        assert!(!is_valid_column_name(".leading"));
        assert!(is_valid_column_name("_0_version"));
    }
}
