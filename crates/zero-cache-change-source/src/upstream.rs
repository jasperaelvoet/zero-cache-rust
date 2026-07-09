//! Port of `zero-cache/src/services/change-source/protocol/current/upstream.ts`.

use std::collections::BTreeMap;

use crate::data::{BackfillId, Identifier, TableMetadata};
use crate::status::UpstreamStatusMessage;

/// Messages sent from `zero-cache` to the change-source. Currently only
/// status messages. Port of `ChangeSourceUpstream`.
pub type ChangeSourceUpstream = UpstreamStatusMessage;

/// A table identifier plus its (possibly unknown) metadata, used within a
/// [`BackfillRequest`]. Port of the inline `identifierSchema.extend({metadata})`.
#[derive(Debug, Clone, PartialEq)]
pub struct BackfillTable {
    pub schema: String,
    pub name: String,
    /// `None` if the change-source never specified table metadata.
    pub metadata: Option<TableMetadata>,
}

impl BackfillTable {
    pub fn identifier(&self) -> Identifier {
        Identifier {
            schema: self.schema.clone(),
            name: self.name.clone(),
        }
    }
}

/// Requests that the change-source restart a backfill for the given columns
/// of a table (used to resume backfills interrupted by a dropped session).
/// Port of `BackfillRequest`.
#[derive(Debug, Clone, PartialEq)]
pub struct BackfillRequest {
    pub table: BackfillTable,
    /// Column name -> backfill id.
    pub columns: BTreeMap<String, BackfillId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_table_identifier_roundtrips() {
        let t = BackfillTable {
            schema: "public".into(),
            name: "issues".into(),
            metadata: None,
        };
        let id = t.identifier();
        assert_eq!(id.schema, "public");
        assert_eq!(id.name, "issues");
    }

    #[test]
    fn backfill_request_holds_columns() {
        let req = BackfillRequest {
            table: BackfillTable {
                schema: "public".into(),
                name: "issues".into(),
                metadata: None,
            },
            columns: BTreeMap::from([("new_col".to_string(), BTreeMap::new())]),
        };
        assert!(req.columns.contains_key("new_col"));
    }
}
