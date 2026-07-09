//! Port of `zql/src/ivm/filter.ts` + the change-splitting core of
//! `zql/src/ivm/filter-push.ts`.
//!
//! Upstream's `Filter` is a stateless operator sitting between an `Input`
//! and an `Output`, participating in a separate `FilterOperator` sub-
//! protocol (`beginFilter`/`endFilter`/`filter(node)`) that composes with
//! `fan-out`/`fan-in` for multi-condition queries. This v1 port keeps only
//! the single-predicate behavior needed for one WHERE clause on one table —
//! see `ivm::operator`'s module doc for the broader scope deviation
//! (concrete composition instead of a trait-object operator graph).
//!
//! The one piece of real logic here — and the reason `Filter` isn't just
//! `Iterator::filter` — is edit-splitting: `filterPush`'s documented
//! contract (see `change.ts`'s `EditChange` doc comment) is that an `Edit`
//! whose presence-under-the-predicate changes must be turned into a
//! `Remove` (matched -> unmatched) or an `Add` (unmatched -> matched), not
//! passed through as an `Edit` a downstream consumer would misinterpret as
//! "this row was already present".

use crate::ivm::data::Row;
use crate::ivm::operator::{Change, FetchRequest, Node, Stream};
use crate::ivm::table_source::TableSource;

/// Filters a `TableSource`'s changes and fetches through `predicate`. Port
/// of `Filter`, restricted to wrapping a `TableSource` directly (v1 scope —
/// see module doc).
pub struct Filter<'a> {
    predicate: Box<dyn Fn(&Row) -> bool + 'a>,
}

impl<'a> Filter<'a> {
    pub fn new(predicate: impl Fn(&Row) -> bool + 'a) -> Self {
        Filter {
            predicate: Box::new(predicate),
        }
    }

    /// Filters a source's fetch stream. Port of `Filter.fetch` (inherited
    /// from wrapping `FilterInput`/pass-through fetch).
    pub fn fetch<'s>(&'s self, source: &'s TableSource, req: &FetchRequest) -> Stream<'s, Node> {
        let predicate = &self.predicate;
        Box::new(source.fetch(req).filter(move |node| predicate(&node.row)))
    }

    /// Translates a source-level `Change` through the predicate. Port of
    /// `filterPush`'s change-splitting logic (see module doc): an `Add`/
    /// `Remove` passes through only if its row matches; an `Edit` is
    /// reclassified based on whether the predicate's verdict changed.
    /// Returns `None` if the change is entirely invisible to this filter
    /// (neither old nor new row matched).
    pub fn push(&self, change: Change) -> Option<Change> {
        match change {
            Change::Add(node) => (self.predicate)(&node.row).then_some(Change::Add(node)),
            Change::Remove(node) => (self.predicate)(&node.row).then_some(Change::Remove(node)),
            Change::Edit { node, old_node } => {
                let new_matches = (self.predicate)(&node.row);
                let old_matches = (self.predicate)(&old_node.row);
                match (old_matches, new_matches) {
                    (true, true) => Some(Change::Edit { node, old_node }),
                    (true, false) => Some(Change::Remove(old_node)),
                    (false, true) => Some(Change::Add(node)),
                    (false, false) => None,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::{make_source_change_add, make_source_change_edit};
    use zero_cache_protocol::ast::Direction;
    use zero_cache_shared::bigint_json::JsonValue;

    fn row(id: i64, active: bool) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("active".into(), JsonValue::Bool(active)),
        ]
    }

    fn is_active(row: &Row) -> bool {
        row.iter()
            .any(|(k, v)| k == "active" && *v == JsonValue::Bool(true))
    }

    #[test]
    fn fetch_only_returns_matching_rows() {
        let mut s = TableSource::new("t", vec!["id".into()], vec![("id".into(), Direction::Asc)]);
        s.push(make_source_change_add(row(1, true)));
        s.push(make_source_change_add(row(2, false)));
        let f = Filter::new(is_active);
        let rows: Vec<Node> = f.fetch(&s, &FetchRequest::default()).collect();
        assert_eq!(rows, vec![Node::new(row(1, true))]);
    }

    #[test]
    fn push_add_matching_passes_through() {
        let f = Filter::new(is_active);
        let change = Change::Add(Node::new(row(1, true)));
        assert_eq!(f.push(change.clone()), Some(change));
    }

    #[test]
    fn push_add_nonmatching_is_dropped() {
        let f = Filter::new(is_active);
        assert_eq!(f.push(Change::Add(Node::new(row(1, false)))), None);
    }

    #[test]
    fn push_edit_still_matching_stays_edit() {
        let f = Filter::new(is_active);
        let change = Change::Edit {
            node: Node::new(row(1, true)),
            old_node: Node::new(row(1, true)),
        };
        assert_eq!(f.push(change.clone()), Some(change));
    }

    #[test]
    fn push_edit_leaving_match_becomes_remove() {
        let f = Filter::new(is_active);
        let change = Change::Edit {
            node: Node::new(row(1, false)),
            old_node: Node::new(row(1, true)),
        };
        assert_eq!(
            f.push(change),
            Some(Change::Remove(Node::new(row(1, true))))
        );
    }

    #[test]
    fn push_edit_entering_match_becomes_add() {
        let f = Filter::new(is_active);
        let change = Change::Edit {
            node: Node::new(row(1, true)),
            old_node: Node::new(row(1, false)),
        };
        assert_eq!(f.push(change), Some(Change::Add(Node::new(row(1, true)))));
    }

    #[test]
    fn push_edit_never_matching_is_dropped() {
        let f = Filter::new(is_active);
        let change = Change::Edit {
            node: Node::new(row(1, false)),
            old_node: Node::new(row(1, false)),
        };
        assert_eq!(f.push(change), None);
    }

    #[test]
    fn end_to_end_source_push_through_filter() {
        let mut s = TableSource::new("t", vec!["id".into()], vec![("id".into(), Direction::Asc)]);
        let f = Filter::new(is_active);

        let source_change = s.push(make_source_change_add(row(1, true)));
        assert_eq!(
            f.push(source_change),
            Some(Change::Add(Node::new(row(1, true))))
        );

        let source_change = s.push(make_source_change_edit(row(1, false), row(1, true)));
        assert_eq!(
            f.push(source_change),
            Some(Change::Remove(Node::new(row(1, true))))
        );
    }
}
