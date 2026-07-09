//! Port of `zero-cache/src/db/specs.ts` (plus the two small pg enums it uses:
//! `postgres-type-class-enum.ts` and `postgres-replica-identity-enum.ts`).
//!
//! Table, column, and index specifications for the replica and for published
//! Postgres schema. The upstream file is pure `valita` schema + type
//! definitions with no dedicated test; the structs are ported directly.
//!
//! These live in the `types` crate (rather than a separate `db` crate) to avoid
//! a crate cycle: `types/lite.ts` depends on `db/specs.ts`.

use std::collections::BTreeMap;

/// Postgres type class (`pg_type.typtype`). Port of `PostgresTypeClass`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgTypeClass {
    Base,
    Composite,
    Domain,
    Enum,
    Pseudo,
    Range,
    Multirange,
}

impl PgTypeClass {
    /// The single-character wire value used by Postgres.
    pub fn as_str(self) -> &'static str {
        match self {
            PgTypeClass::Base => "b",
            PgTypeClass::Composite => "c",
            PgTypeClass::Domain => "d",
            PgTypeClass::Enum => "e",
            PgTypeClass::Pseudo => "p",
            PgTypeClass::Range => "r",
            PgTypeClass::Multirange => "m",
        }
    }

    /// Parses the single-character Postgres value.
    pub fn from_str(s: &str) -> Option<PgTypeClass> {
        Some(match s {
            "b" => PgTypeClass::Base,
            "c" => PgTypeClass::Composite,
            "d" => PgTypeClass::Domain,
            "e" => PgTypeClass::Enum,
            "p" => PgTypeClass::Pseudo,
            "r" => PgTypeClass::Range,
            "m" => PgTypeClass::Multirange,
            _ => return None,
        })
    }
}

/// Postgres replica identity (`pg_class.relreplident`). Port of
/// `PostgresReplicaIdentity`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

impl ReplicaIdentity {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplicaIdentity::Default => "d",
            ReplicaIdentity::Nothing => "n",
            ReplicaIdentity::Full => "f",
            ReplicaIdentity::Index => "i",
        }
    }

    pub fn from_str(s: &str) -> Option<ReplicaIdentity> {
        Some(match s {
            "d" => ReplicaIdentity::Default,
            "n" => ReplicaIdentity::Nothing,
            "f" => ReplicaIdentity::Full,
            "i" => ReplicaIdentity::Index,
            _ => return None,
        })
    }
}

/// A single column's specification. Port of `ColumnSpec`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSpec {
    /// 1-based ordinal position of the column.
    pub pos: i64,
    /// The SQLite/lite data type string (see [`crate::lite::LiteTypeString`]).
    pub data_type: String,
    pub pg_type_class: Option<PgTypeClass>,
    /// For array columns, the element type class; `None` for scalar columns.
    pub elem_pg_type_class: Option<PgTypeClass>,
    pub character_maximum_length: Option<i64>,
    pub not_null: Option<bool>,
    pub dflt: Option<String>,
}

impl ColumnSpec {
    /// Minimal constructor for a scalar column (matches the common case used by
    /// `lite`).
    pub fn new(data_type: impl Into<String>, pos: i64) -> Self {
        ColumnSpec {
            pos,
            data_type: data_type.into(),
            pg_type_class: None,
            elem_pg_type_class: None,
            character_maximum_length: None,
            not_null: None,
            dflt: None,
        }
    }
}

/// A published column spec: a [`ColumnSpec`] plus its Postgres type OID. Port of
/// `publishedColumnSpec`.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishedColumnSpec {
    pub column: ColumnSpec,
    pub type_oid: i64,
}

/// A SQLite ("lite") table specification. Port of `LiteTableSpec`.
///
/// Columns are stored in definition order as `(name, spec)` pairs (JavaScript
/// object insertion order is significant for column ordering).
#[derive(Debug, Clone, PartialEq)]
pub struct LiteTableSpec {
    pub name: String,
    pub columns: Vec<(String, ColumnSpec)>,
    pub primary_key: Option<Vec<String>>,
}

impl LiteTableSpec {
    /// Returns the spec for `col`, or `None` if the column is unknown.
    pub fn column(&self, col: &str) -> Option<&ColumnSpec> {
        self.columns
            .iter()
            .find(|(name, _)| name == col)
            .map(|(_, spec)| spec)
    }
}

/// A Postgres table specification: a [`LiteTableSpec`] plus its schema. Port of
/// `TableSpec`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSpec {
    pub name: String,
    pub schema: String,
    pub columns: Vec<(String, ColumnSpec)>,
    pub primary_key: Option<Vec<String>>,
}

/// The row-filter of a publication over a table. Port of the inline
/// `{rowFilter}` object in `publishedTableSpec`.
#[derive(Debug, Clone, PartialEq)]
pub struct PublicationInfo {
    pub row_filter: Option<String>,
}

/// A published Postgres table spec. Port of `PublishedTableSpec`.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishedTableSpec {
    pub name: String,
    pub schema: String,
    pub oid: i64,
    pub schema_oid: Option<i64>,
    pub columns: Vec<(String, PublishedColumnSpec)>,
    pub primary_key: Option<Vec<String>>,
    pub replica_identity: Option<ReplicaIdentity>,
    /// Publication name -> row-filter info.
    pub publications: BTreeMap<String, PublicationInfo>,
}

/// Index column sort direction. Port of `directionSchema`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Direction::Asc => "ASC",
            Direction::Desc => "DESC",
        }
    }

    pub fn from_str(s: &str) -> Option<Direction> {
        match s {
            "ASC" => Some(Direction::Asc),
            "DESC" => Some(Direction::Desc),
            _ => None,
        }
    }
}

/// A SQLite ("lite") index specification. Port of `LiteIndexSpec`. Columns are
/// kept in order as `(name, direction)` pairs.
#[derive(Debug, Clone, PartialEq)]
pub struct LiteIndexSpec {
    pub name: String,
    pub table_name: String,
    pub unique: bool,
    pub columns: Vec<(String, Direction)>,
}

/// A Postgres index spec: a [`LiteIndexSpec`] plus its schema. Port of
/// `IndexSpec`.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexSpec {
    pub name: String,
    pub table_name: String,
    pub schema: String,
    pub unique: bool,
    pub columns: Vec<(String, Direction)>,
}

/// A published Postgres index spec. Port of `PublishedIndexSpec`.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishedIndexSpec {
    pub name: String,
    pub table_name: String,
    pub schema: String,
    pub unique: bool,
    pub columns: Vec<(String, Direction)>,
    pub is_replica_identity: Option<bool>,
    pub is_primary_key: Option<bool>,
    pub is_immediate: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_type_class_roundtrip() {
        for c in [
            PgTypeClass::Base,
            PgTypeClass::Composite,
            PgTypeClass::Domain,
            PgTypeClass::Enum,
            PgTypeClass::Pseudo,
            PgTypeClass::Range,
            PgTypeClass::Multirange,
        ] {
            assert_eq!(PgTypeClass::from_str(c.as_str()), Some(c));
        }
        assert_eq!(PgTypeClass::Base.as_str(), "b");
        assert_eq!(PgTypeClass::Multirange.as_str(), "m");
        assert_eq!(PgTypeClass::from_str("x"), None);
    }

    #[test]
    fn replica_identity_roundtrip() {
        for r in [
            ReplicaIdentity::Default,
            ReplicaIdentity::Nothing,
            ReplicaIdentity::Full,
            ReplicaIdentity::Index,
        ] {
            assert_eq!(ReplicaIdentity::from_str(r.as_str()), Some(r));
        }
        assert_eq!(ReplicaIdentity::from_str("z"), None);
    }

    #[test]
    fn direction_roundtrip() {
        assert_eq!(Direction::Asc.as_str(), "ASC");
        assert_eq!(Direction::Desc.as_str(), "DESC");
        assert_eq!(Direction::from_str("ASC"), Some(Direction::Asc));
        assert_eq!(Direction::from_str("DESC"), Some(Direction::Desc));
        assert_eq!(Direction::from_str("asc"), None);
    }

    #[test]
    fn lite_table_column_lookup() {
        let t = LiteTableSpec {
            name: "issues".into(),
            columns: vec![
                ("id".into(), ColumnSpec::new("int8", 1)),
                ("title".into(), ColumnSpec::new("text", 2)),
            ],
            primary_key: Some(vec!["id".into()]),
        };
        assert_eq!(t.column("title").unwrap().pos, 2);
        assert!(t.column("missing").is_none());
    }
}
