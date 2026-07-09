//! Port of `zql/src/ivm/change-type.ts` and `zql/src/ivm/source.ts`'s
//! `SourceChange` (the operator-facing `Change` union from `change.ts`,
//! which additionally carries `Node`/relationship data, is deferred along
//! with the rest of the `Node`/`Stream`/`Operator` machinery — see
//! `ivm::data`'s module doc).
//!
//! `SourceChange` is the row-level change vocabulary a `Source` emits
//! before it's been elaborated into a `Node`-bearing `Change` for the
//! operator pipeline: exactly what `zero-cache-sqlite::pipeline`'s apply
//! loop has in hand after decoding a replicated row mutation, making it the
//! natural first thing a future `Source` port would consume.

use crate::ivm::data::Row;

/// The three kinds of row-level change a `Source` can emit. Port of
/// `ChangeType` (`change-type-enum.ts`), restricted to the `Add`/`Remove`/
/// `Edit` values `SourceChange` uses (`Child` only appears on the
/// operator-level `Change`, not yet ported).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeType {
    Add,
    Remove,
    Edit,
}

/// A raw row-level change from a `Source`, before pipeline elaboration.
/// Port of `SourceChange` (`SourceChangeAdd | SourceChangeRemove |
/// SourceChangeEdit`).
#[derive(Debug, Clone, PartialEq)]
pub enum SourceChange {
    Add(Row),
    Remove(Row),
    Edit { row: Row, old_row: Row },
}

impl SourceChange {
    /// The change's tag. Port of accessing `change[0]` (the `ChangeType`
    /// discriminant of the tuple).
    pub fn change_type(&self) -> ChangeType {
        match self {
            SourceChange::Add(_) => ChangeType::Add,
            SourceChange::Remove(_) => ChangeType::Remove,
            SourceChange::Edit { .. } => ChangeType::Edit,
        }
    }
}

/// Port of `makeSourceChangeAdd`.
pub fn make_source_change_add(row: Row) -> SourceChange {
    SourceChange::Add(row)
}

/// Port of `makeSourceChangeRemove`.
pub fn make_source_change_remove(row: Row) -> SourceChange {
    SourceChange::Remove(row)
}

/// Port of `makeSourceChangeEdit`.
pub fn make_source_change_edit(row: Row, old_row: Row) -> SourceChange {
    SourceChange::Edit { row, old_row }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_shared::bigint_json::JsonValue;

    fn row(id: i64) -> Row {
        vec![("id".into(), JsonValue::Number(id as f64))]
    }

    #[test]
    fn factory_functions_produce_expected_variants_and_tags() {
        assert_eq!(make_source_change_add(row(1)), SourceChange::Add(row(1)));
        assert_eq!(
            make_source_change_add(row(1)).change_type(),
            ChangeType::Add
        );

        assert_eq!(
            make_source_change_remove(row(1)),
            SourceChange::Remove(row(1))
        );
        assert_eq!(
            make_source_change_remove(row(1)).change_type(),
            ChangeType::Remove
        );

        let edit = make_source_change_edit(row(2), row(1));
        assert_eq!(
            edit,
            SourceChange::Edit {
                row: row(2),
                old_row: row(1)
            }
        );
        assert_eq!(edit.change_type(), ChangeType::Edit);
    }
}
