//! Port of
//! `zero-cache/src/services/change-source/protocol/current/data.ts`.
//!
//! Data-plane messages: changes sent by ChangeSources, forwarded/fanned out by
//! the ChangeStreamerService, and stored in the Change DB for catchup. This is
//! the core `Change` union that drives the entire replication apply-loop.

use std::collections::BTreeMap;

use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_types::specs::{ColumnSpec, IndexSpec, TableSpec};

/// A row: column name -> value.
pub type Row = Vec<(String, JsonValue)>;

/// How a row's key is determined. Port of `rowKeySchema`'s `type` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKeyKind {
    Default,
    Nothing,
    Full,
    Index,
}

/// The columns (and optional qualifier) identifying a row. Port of the inline
/// `rowKeySchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct RowKey {
    pub columns: Vec<String>,
    pub kind: Option<RowKeyKind>,
}

/// A table/relation reference with its row key. Port of `MessageRelation`
/// (`relationSchema`, post `.map()` normalization from the deprecated
/// `keyColumns`/`replicaIdentity` fields — this type models the normalized
/// `rowKey` form plus the full column list `relationDifferent` needs).
///
/// `columns` is the relation's full column list in wire (declaration) order,
/// each `(name, type_oid)` — the shape `pg_schema_diff::relation_different`
/// compares positionally against a `PublishedTableSpec`. It comes straight from
/// the pgoutput `Relation` message's columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Relation {
    pub schema: String,
    pub name: String,
    pub row_key: RowKey,
    pub columns: Vec<(String, i32)>,
}

/// A schema+name identifier. Port of `Identifier`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identifier {
    pub schema: String,
    pub name: String,
}

/// Opaque per-column backfill tracking id. Port of `BackfillID` (a JSON
/// object).
pub type BackfillId = BTreeMap<String, JsonValue>;

/// Table-level configuration for how change messages are handled (not part of
/// the row data itself). Port of `TableMetadata`: required `rowKey`, plus
/// arbitrary additional JSON properties (`.rest(jsonValueSchema)`).
#[derive(Debug, Clone, PartialEq)]
pub struct TableMetadata {
    pub row_key: BTreeMap<String, JsonValue>,
    pub extra: BTreeMap<String, JsonValue>,
}

/// A named column with its spec. Port of the inline `columnSchema`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub spec: ColumnSpec,
}

/// Backfill progress, for display. Port of `DownloadStatus`.
#[derive(Debug, Clone, PartialEq)]
pub struct DownloadStatus {
    pub rows: f64,
    pub total_rows: f64,
    pub total_bytes: Option<f64>,
}

/// The JSON encoding used for `json`/`jsonb` column values within a
/// transaction. Port of `MessageBegin.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonEncoding {
    Parsed,
    Stringified,
}

/// A `create-table` change. Port of `TableCreate`.
#[derive(Debug, Clone, PartialEq)]
pub struct TableCreate {
    pub spec: TableSpec,
    pub metadata: Option<TableMetadata>,
    pub backfill: Option<BTreeMap<String, BackfillId>>,
}

/// The `Change` union: every message the change-source protocol can emit. Port
/// of `Change` (`MessageBegin | DataOrSchemaChange | MessageCommit |
/// MessageRollback`).
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    Begin {
        json: Option<JsonEncoding>,
        skip_ack: Option<bool>,
    },
    Commit,
    Rollback,

    // -- data changes --
    Insert {
        relation: Relation,
        new: Row,
    },
    Update {
        relation: Relation,
        /// Present if the update changed the key, or `replicaIdentity == full`.
        key: Option<Row>,
        new: Row,
    },
    Delete {
        relation: Relation,
        key: Row,
    },
    Truncate {
        relations: Vec<Relation>,
    },
    Backfill {
        relation: Relation,
        columns: Vec<String>,
        watermark: String,
        row_values: Vec<Vec<JsonValue>>,
        status: Option<DownloadStatus>,
    },

    // -- schema changes --
    CreateTable(TableCreate),
    RenameTable {
        old: Identifier,
        new: Identifier,
    },
    UpdateTableMetadata {
        table: Identifier,
        old: TableMetadata,
        new: TableMetadata,
    },
    AddColumn {
        table: Identifier,
        column: ColumnDef,
        table_metadata: Option<TableMetadata>,
        backfill: Option<BackfillId>,
    },
    UpdateColumn {
        table: Identifier,
        old: ColumnDef,
        new: ColumnDef,
    },
    DropColumn {
        table: Identifier,
        column: String,
    },
    DropTable {
        id: Identifier,
    },
    CreateIndex {
        spec: IndexSpec,
    },
    DropIndex {
        id: Identifier,
    },
    BackfillCompleted {
        relation: Relation,
        columns: Vec<String>,
        watermark: String,
        status: Option<DownloadStatus>,
    },
}

impl Change {
    /// The message tag string, matching the TS `tag` discriminant exactly.
    pub fn tag(&self) -> &'static str {
        match self {
            Change::Begin { .. } => "begin",
            Change::Commit => "commit",
            Change::Rollback => "rollback",
            Change::Insert { .. } => "insert",
            Change::Update { .. } => "update",
            Change::Delete { .. } => "delete",
            Change::Truncate { .. } => "truncate",
            Change::Backfill { .. } => "backfill",
            Change::CreateTable(_) => "create-table",
            Change::RenameTable { .. } => "rename-table",
            Change::UpdateTableMetadata { .. } => "update-table-metadata",
            Change::AddColumn { .. } => "add-column",
            Change::UpdateColumn { .. } => "update-column",
            Change::DropColumn { .. } => "drop-column",
            Change::DropTable { .. } => "drop-table",
            Change::CreateIndex { .. } => "create-index",
            Change::DropIndex { .. } => "drop-index",
            Change::BackfillCompleted { .. } => "backfill-completed",
        }
    }
}

/// Schema-change tags, kept in sync with the `Change::tag()` schema variants.
/// Port of `schemaChangeTags`.
pub const SCHEMA_CHANGE_TAGS: &[&str] = &[
    "create-table",
    "rename-table",
    "update-table-metadata",
    "add-column",
    "update-column",
    "drop-column",
    "drop-table",
    "create-index",
    "drop-index",
    "backfill-completed",
];

/// Data-change tags, kept in sync with the `Change::tag()` data variants. Port
/// of `dataChangeTags`.
pub const DATA_CHANGE_TAGS: &[&str] = &["insert", "update", "delete", "truncate", "backfill"];

/// Whether `change` is a schema (DDL) change. Port of `isSchemaChange`.
pub fn is_schema_change(change: &Change) -> bool {
    SCHEMA_CHANGE_TAGS.contains(&change.tag())
}

/// Whether `change` is a data (row) change. Port of `isDataChange`.
pub fn is_data_change(change: &Change) -> bool {
    DATA_CHANGE_TAGS.contains(&change.tag())
}

/// A [`Change`] known to be either a data change or a schema change (i.e. not
/// `begin`/`commit`/`rollback`). Port of the `DataOrSchemaChange` type,
/// enforced at construction via [`TryFrom`] rather than as a separate enum, to
/// avoid duplicating every data/schema variant.
#[derive(Debug, Clone, PartialEq)]
pub struct DataOrSchemaChangeView(pub Change);

impl TryFrom<Change> for DataOrSchemaChangeView {
    type Error = Change;

    fn try_from(change: Change) -> Result<Self, Change> {
        if is_data_change(&change) || is_schema_change(&change) {
            Ok(DataOrSchemaChangeView(change))
        } else {
            Err(change)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_identifier() -> Identifier {
        Identifier {
            schema: "public".into(),
            name: "t".into(),
        }
    }

    fn dummy_relation() -> Relation {
        Relation {
            schema: "public".into(),
            name: "t".into(),
            row_key: RowKey {
                columns: vec!["id".into()],
                kind: None,
            },
            columns: vec![("id".into(), 25)],
        }
    }

    fn change_for_tag(tag: &str) -> Change {
        match tag {
            "begin" => Change::Begin {
                json: None,
                skip_ack: None,
            },
            "commit" => Change::Commit,
            "rollback" => Change::Rollback,
            "insert" => Change::Insert {
                relation: dummy_relation(),
                new: vec![],
            },
            "update" => Change::Update {
                relation: dummy_relation(),
                key: None,
                new: vec![],
            },
            "delete" => Change::Delete {
                relation: dummy_relation(),
                key: vec![],
            },
            "truncate" => Change::Truncate { relations: vec![] },
            "backfill" => Change::Backfill {
                relation: dummy_relation(),
                columns: vec![],
                watermark: "00".into(),
                row_values: vec![],
                status: None,
            },
            "create-table" => Change::CreateTable(TableCreate {
                spec: TableSpec {
                    name: "t".into(),
                    schema: "public".into(),
                    columns: vec![],
                    primary_key: None,
                },
                metadata: None,
                backfill: None,
            }),
            "rename-table" => Change::RenameTable {
                old: dummy_identifier(),
                new: dummy_identifier(),
            },
            "update-table-metadata" => Change::UpdateTableMetadata {
                table: dummy_identifier(),
                old: TableMetadata {
                    row_key: BTreeMap::new(),
                    extra: BTreeMap::new(),
                },
                new: TableMetadata {
                    row_key: BTreeMap::new(),
                    extra: BTreeMap::new(),
                },
            },
            "add-column" => Change::AddColumn {
                table: dummy_identifier(),
                column: ColumnDef {
                    name: "c".into(),
                    spec: ColumnSpec::new("text", 1),
                },
                table_metadata: None,
                backfill: None,
            },
            "update-column" => Change::UpdateColumn {
                table: dummy_identifier(),
                old: ColumnDef {
                    name: "c".into(),
                    spec: ColumnSpec::new("text", 1),
                },
                new: ColumnDef {
                    name: "c".into(),
                    spec: ColumnSpec::new("text", 1),
                },
            },
            "drop-column" => Change::DropColumn {
                table: dummy_identifier(),
                column: "c".into(),
            },
            "drop-table" => Change::DropTable {
                id: dummy_identifier(),
            },
            "create-index" => Change::CreateIndex {
                spec: IndexSpec {
                    name: "idx".into(),
                    table_name: "t".into(),
                    schema: "public".into(),
                    unique: false,
                    columns: vec![],
                },
            },
            "drop-index" => Change::DropIndex {
                id: dummy_identifier(),
            },
            "backfill-completed" => Change::BackfillCompleted {
                relation: dummy_relation(),
                columns: vec![],
                watermark: "00".into(),
                status: None,
            },
            other => panic!("unhandled tag {other}"),
        }
    }

    #[test]
    fn schema_and_data_change_tags() {
        for tag in SCHEMA_CHANGE_TAGS {
            let c = change_for_tag(tag);
            assert!(is_schema_change(&c), "{tag}");
            assert!(!is_data_change(&c), "{tag}");
        }
        for tag in DATA_CHANGE_TAGS {
            let c = change_for_tag(tag);
            assert!(!is_schema_change(&c), "{tag}");
            assert!(is_data_change(&c), "{tag}");
        }
        for tag in ["begin", "commit", "rollback"] {
            let c = change_for_tag(tag);
            assert!(!is_schema_change(&c), "{tag}");
            assert!(!is_data_change(&c), "{tag}");
        }
    }

    #[test]
    fn tag_matches_variant() {
        assert_eq!(Change::Commit.tag(), "commit");
        assert_eq!(
            Change::Insert {
                relation: dummy_relation(),
                new: vec![]
            }
            .tag(),
            "insert"
        );
    }
}
