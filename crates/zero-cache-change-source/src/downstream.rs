//! Port of
//! `zero-cache/src/services/change-source/protocol/current/downstream.ts`.
//!
//! Downstream messages sent from the ChangeSource/ChangeStreamer to
//! subscribers: the data-plane envelope wrapping [`crate::data::Change`]
//! payloads with their commit watermarks, plus control-plane and status
//! messages.

use crate::control::ResetRequired;
use crate::data::{Change, DataOrSchemaChangeView, JsonEncoding};

/// A `begin` envelope: the begin payload plus the commit watermark it will
/// eventually resolve to. Port of the `begin` tuple schema.
#[derive(Debug, Clone, PartialEq)]
pub struct Begin {
    pub json: Option<JsonEncoding>,
    pub skip_ack: Option<bool>,
    pub commit_watermark: String,
}

/// A `commit` envelope: the commit payload plus its watermark. Port of the
/// `commit` tuple schema.
#[derive(Debug, Clone, PartialEq)]
pub struct Commit {
    pub watermark: String,
}

/// A downstream data/control/status message. Port of `ChangeStreamMessage`
/// (the union of `changeStreamDataSchema`, `changeStreamControlSchema`, and
/// `downstreamStatusMessageSchema`).
#[derive(Debug, Clone, PartialEq)]
pub enum ChangeStreamMessage {
    Begin(Begin),
    /// A data-plane change: either a row data change or a schema change.
    /// (`begin`/`commit`/`rollback` are modeled by their own variants here,
    /// not wrapped as `Data`, since the wire tuple's second element is always
    /// `DataOrSchemaChange` — never a transaction-framing message.)
    Data(DataOrSchemaChangeView),
    Commit(Commit),
    Rollback,
    Control(ResetRequired),
    Status {
        ack: bool,
        lag_report: Option<crate::report_schema::ChangeSourceReport>,
        watermark: String,
    },
}

impl ChangeStreamMessage {
    /// Wraps a data or schema `Change` in a `Data` envelope, or `None` if
    /// `change` is a transaction-framing message (`begin`/`commit`/`rollback`,
    /// which are not valid payloads for the `data` tuple). Convenience over
    /// constructing [`ChangeStreamMessage::Data`] directly.
    pub fn data(change: Change) -> Option<ChangeStreamMessage> {
        DataOrSchemaChangeView::try_from(change)
            .ok()
            .map(ChangeStreamMessage::Data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::Relation;

    fn dummy_relation() -> Relation {
        Relation {
            schema: "public".into(),
            name: "t".into(),
            row_key: crate::data::RowKey {
                columns: vec!["id".into()],
                kind: None,
            },
            columns: vec![("id".into(), 25)],
        }
    }

    #[test]
    fn begin_carries_commit_watermark() {
        let b = Begin {
            json: None,
            skip_ack: None,
            commit_watermark: "01".into(),
        };
        assert_eq!(b.commit_watermark, "01");
    }

    #[test]
    fn data_wraps_row_changes() {
        let change = Change::Insert {
            relation: dummy_relation(),
            new: vec![],
        };
        let msg = ChangeStreamMessage::data(change).unwrap();
        assert!(matches!(msg, ChangeStreamMessage::Data(_)));
    }

    #[test]
    fn data_rejects_transaction_framing_messages() {
        assert_eq!(ChangeStreamMessage::data(Change::Commit), None);
        assert_eq!(
            ChangeStreamMessage::data(Change::Begin {
                json: None,
                skip_ack: None
            }),
            None
        );
        assert_eq!(ChangeStreamMessage::data(Change::Rollback), None);
    }

    #[test]
    fn commit_carries_watermark() {
        let c = Commit {
            watermark: "02".into(),
        };
        assert_eq!(c.watermark, "02");
    }
}
