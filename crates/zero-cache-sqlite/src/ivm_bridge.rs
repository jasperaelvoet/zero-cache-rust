//! Bridges the replication-derived [`zero_cache_change_source::data::Change`]
//! (what `pipeline.rs`'s apply loop already produces from a live Postgres
//! stream) into [`zero_cache_zql::ivm`]'s `SourceChange`/`TableSource`/
//! `Filter`, so a registered query can receive a live incremental delta —
//! the second half of the whole-pipeline slice, joining up with
//! `pipeline.rs`'s already-proven first half (Postgres -> SQLite).
//!
//! Scope for this increment: `Insert`/`Update`/`Delete`/`Truncate` against a
//! single table matching the `TableSource`'s name (schema qualifier ignored
//! — v1 has no need to disambiguate same-named tables across schemas). All
//! DDL `Change` variants (CreateTable, AddColumn, etc.) are not translated
//! (produce no IVM changes) — those affect the source's schema, not its
//! rows, and this port's `TableSource` has no schema-migration story yet.
//!
//! `Truncate` has no single-`Change` IVM equivalent — a TRUNCATE removes
//! every row in the table, and `ivm::operator::Change` has no "clear all"
//! variant (matching upstream: TRUNCATE isn't in `zql/src/ivm`'s Change
//! union either; `TableSource`s handle it as N individual removes). So
//! [`apply_to_source`] returns `Vec<IvmChange>` rather than
//! `Option<IvmChange>` — always 0 or 1 elements for Insert/Update/Delete,
//! but potentially many for Truncate.
//!
//! Why `Update` needs a `TableSource` lookup, not just the incoming key:
//! Postgres's default replica identity only sends the OLD row's *key*
//! columns (or nothing, if the key didn't change) — never the full old row.
//! But `Filter::push`'s edit-splitting logic (see `ivm::filter`'s module
//! doc) needs the full old row to correctly re-evaluate the predicate
//! against it. The `TableSource` is the authoritative current-state store
//! (that's the point of the local replica), so this bridge looks the
//! current row up by key *before* applying the change, and uses that as the
//! Edit's old row.

use zero_cache_change_source::data::Change as ReplicationChange;
use zero_cache_zql::ivm::change::{
    make_source_change_add, make_source_change_edit, make_source_change_remove,
};
use zero_cache_zql::ivm::constraint::Constraint;
use zero_cache_zql::ivm::data::{Row, Value};
use zero_cache_zql::ivm::operator::{Change as IvmChange, FetchRequest};
use zero_cache_zql::ivm::table_source::TableSource;

fn row_to_constraint(row: &Row, cols: &[String]) -> Constraint {
    cols.iter()
        .map(|c| {
            (
                c.clone(),
                row.iter()
                    .find(|(k, _)| k == c)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Null),
            )
        })
        .collect()
}

/// Applies one replication [`ReplicationChange`] to `source`, returning the
/// resulting IVM [`IvmChange`]s (see module doc for why this is a `Vec` and
/// for overall scope). Empty for changes that don't target this source's
/// table, or that this bridge doesn't translate (DDL).
pub fn apply_to_source(source: &mut TableSource, change: &ReplicationChange) -> Vec<IvmChange> {
    match change {
        ReplicationChange::Insert { relation, new }
            if relation.name == source.schema().table_name =>
        {
            vec![source.push(make_source_change_add(new.clone()))]
        }
        ReplicationChange::Delete { relation, key }
            if relation.name == source.schema().table_name =>
        {
            let full_key: Vec<String> = key.iter().map(|(k, _)| k.clone()).collect();
            let existing = source
                .find_by_key(&row_to_constraint(key, &full_key))
                .cloned()
                .unwrap_or_else(|| key.clone());
            vec![source.push(make_source_change_remove(existing))]
        }
        ReplicationChange::Update { relation, key, new }
            if relation.name == source.schema().table_name =>
        {
            let lookup_row = key.as_ref().unwrap_or(new);
            let lookup_cols: Vec<String> = lookup_row
                .iter()
                .filter(|(k, _)| source.schema().primary_key.contains(k))
                .map(|(k, _)| k.clone())
                .collect();
            let old_row = source
                .find_by_key(&row_to_constraint(lookup_row, &lookup_cols))
                .cloned()
                .unwrap_or_else(|| lookup_row.clone());
            vec![source.push(make_source_change_edit(new.clone(), old_row))]
        }
        ReplicationChange::Truncate { relations }
            if relations
                .iter()
                .any(|r| r.name == source.schema().table_name) =>
        {
            // No single Change models "clear everything" (see module doc) —
            // snapshot the current rows, then remove each one individually.
            let rows: Vec<Row> = source
                .fetch(&FetchRequest::default())
                .map(|node| node.row)
                .collect();
            rows.into_iter()
                .map(|row| source.push(make_source_change_remove(row)))
                .collect()
        }
        _ => vec![],
    }
}

/// Converts an already-applied [`IvmChange`] back into the [`SourceChange`]
/// shape that produced it — `ivm::join::reeval_exists_after_child_change`
/// wants the change description, not just its already-applied result.
fn ivm_change_to_source_change(change: &IvmChange) -> zero_cache_zql::ivm::change::SourceChange {
    use zero_cache_zql::ivm::change::SourceChange;
    match change {
        IvmChange::Add(node) => SourceChange::Add(node.row.clone()),
        IvmChange::Remove(node) => SourceChange::Remove(node.row.clone()),
        IvmChange::Edit { node, old_node } => SourceChange::Edit {
            row: node.row.clone(),
            old_row: old_node.row.clone(),
        },
        IvmChange::Child { .. } => {
            unreachable!("a table source never emits an operator child change")
        }
    }
}

/// Applies a replication change to a CHILD table source (via
/// [`apply_to_source`]) and, for each resulting row change, re-evaluates
/// whether the correlated PARENT row's EXISTS status flipped — wiring
/// `ivm::join::reeval_exists_after_child_change` into the live replication
/// apply loop. This is what lets a real Postgres child-table change (e.g.
/// a new `comments` row) actually keep an EXISTS-based query or permission
/// rule (`create_predicate_with_exists`) live, end to end, rather than
/// only being provable in a unit test against manually-pushed rows.
///
/// Returns one `(parent_row, new_exists_value)` per resulting row change —
/// usually one, but a `Truncate` can affect multiple parents. Entries
/// where the changed child row doesn't correlate to any current parent row
/// are omitted (matching `reeval_exists_after_child_change`'s `None` for
/// an orphaned child).
pub fn apply_to_child_and_reeval_exists(
    child: &mut TableSource,
    replication_change: &ReplicationChange,
    parent: &TableSource,
    correlation: &zero_cache_protocol::ast::Correlation,
) -> Vec<(Row, bool)> {
    apply_to_source(child, replication_change)
        .iter()
        .map(ivm_change_to_source_change)
        .filter_map(|source_change| {
            zero_cache_zql::ivm::join::reeval_exists_after_child_change(
                &source_change,
                parent,
                child,
                correlation,
            )
        })
        .collect()
}

/// The full row-nesting counterpart to [`apply_to_child_and_reeval_exists`]:
/// applies a replication change to a CHILD table source and, for each
/// resulting row change, re-derives the correlated PARENT row's full joined
/// `relationships[relationship_name]` list — wiring
/// `ivm::join::reeval_relationship_after_child_change` into the live
/// replication apply loop. This is what lets a real Postgres child-table
/// change (e.g. a new `comments` row) actually keep a joined query's nested
/// relationship data live end-to-end, not just provable against
/// manually-pushed rows.
///
/// Returns one [`zero_cache_zql::ivm::operator::Node`] per resulting row
/// change (the affected parent, with its relationship re-fetched); entries
/// where the changed child row doesn't correlate to any current parent row
/// are omitted, matching `reeval_relationship_after_child_change`'s `None`.
pub fn apply_to_child_and_reeval_relationship(
    child: &mut TableSource,
    replication_change: &ReplicationChange,
    parent: &TableSource,
    correlation: &zero_cache_protocol::ast::Correlation,
    relationship_name: &str,
) -> Vec<zero_cache_zql::ivm::operator::Node> {
    apply_to_source(child, replication_change)
        .iter()
        .map(ivm_change_to_source_change)
        .filter_map(|source_change| {
            zero_cache_zql::ivm::join::reeval_relationship_after_child_change(
                &source_change,
                parent,
                child,
                correlation,
                relationship_name,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_change_source::data::{Relation, RowKey};
    use zero_cache_protocol::ast::Direction;
    use zero_cache_shared::bigint_json::JsonValue;
    use zero_cache_zql::ivm::filter::Filter;
    use zero_cache_zql::ivm::operator::Node;

    fn relation(name: &str) -> Relation {
        Relation {
            schema: "public".into(),
            name: name.into(),
            row_key: RowKey {
                columns: vec!["id".into()],
                kind: None,
            },
            columns: vec![],
        }
    }

    fn row(id: i64, active: bool) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("active".into(), JsonValue::Bool(active)),
        ]
    }

    fn source() -> TableSource {
        TableSource::new("t", vec!["id".into()], vec![("id".into(), Direction::Asc)])
    }

    #[test]
    fn insert_produces_add_change() {
        let mut s = source();
        let change = apply_to_source(
            &mut s,
            &ReplicationChange::Insert {
                relation: relation("t"),
                new: row(1, true),
            },
        );
        assert_eq!(change, vec![IvmChange::Add(Node::new(row(1, true)))]);
    }

    #[test]
    fn change_for_other_table_is_ignored() {
        let mut s = source();
        let change = apply_to_source(
            &mut s,
            &ReplicationChange::Insert {
                relation: relation("other"),
                new: row(1, true),
            },
        );
        assert_eq!(change, vec![]);
        assert_eq!(s.fetch(&Default::default()).count(), 0);
    }

    #[test]
    fn delete_produces_remove_with_full_row_even_from_key_only() {
        let mut s = source();
        apply_to_source(
            &mut s,
            &ReplicationChange::Insert {
                relation: relation("t"),
                new: row(1, true),
            },
        );
        let key_only: Row = vec![("id".into(), JsonValue::Number(1.0))];
        let change = apply_to_source(
            &mut s,
            &ReplicationChange::Delete {
                relation: relation("t"),
                key: key_only,
            },
        );
        // The bridge looked up the FULL row (with active=true) before deleting,
        // not just the key-only tuple Postgres sent.
        assert_eq!(change, vec![IvmChange::Remove(Node::new(row(1, true)))]);
    }

    #[test]
    fn update_looks_up_full_old_row_for_correct_filter_edit_splitting() {
        let mut s = source();
        apply_to_source(
            &mut s,
            &ReplicationChange::Insert {
                relation: relation("t"),
                new: row(1, true),
            },
        );

        // Postgres default replica identity: old tuple is key-only.
        let key_only_old: Row = vec![("id".into(), JsonValue::Number(1.0))];
        let mut ivm_changes = apply_to_source(
            &mut s,
            &ReplicationChange::Update {
                relation: relation("t"),
                key: Some(key_only_old),
                new: row(1, false),
            },
        );
        assert_eq!(ivm_changes.len(), 1);
        let ivm_change = ivm_changes.remove(0);
        assert_eq!(
            ivm_change,
            IvmChange::Edit {
                node: Node::new(row(1, false)),
                old_node: Node::new(row(1, true))
            }
        );

        // Feed it through a Filter on `active` — this only works correctly
        // because old_node carries the real prior `active: true`, not just
        // the bare key the wire message contained.
        let filter = Filter::new(|r: &Row| {
            r.iter()
                .any(|(k, v)| k == "active" && *v == JsonValue::Bool(true))
        });
        assert_eq!(
            filter.push(ivm_change),
            Some(IvmChange::Remove(Node::new(row(1, true))))
        );
    }

    #[test]
    fn truncate_removes_all_rows_and_produces_one_remove_per_row() {
        let mut s = source();
        apply_to_source(
            &mut s,
            &ReplicationChange::Insert {
                relation: relation("t"),
                new: row(1, true),
            },
        );
        apply_to_source(
            &mut s,
            &ReplicationChange::Insert {
                relation: relation("t"),
                new: row(2, false),
            },
        );

        let changes = apply_to_source(
            &mut s,
            &ReplicationChange::Truncate {
                relations: vec![relation("t")],
            },
        );
        assert_eq!(changes.len(), 2);
        assert!(changes.contains(&IvmChange::Remove(Node::new(row(1, true)))));
        assert!(changes.contains(&IvmChange::Remove(Node::new(row(2, false)))));
        assert_eq!(
            s.fetch(&Default::default()).count(),
            0,
            "TableSource should be empty after truncate"
        );
    }

    #[test]
    fn truncate_for_other_table_is_ignored() {
        let mut s = source();
        apply_to_source(
            &mut s,
            &ReplicationChange::Insert {
                relation: relation("t"),
                new: row(1, true),
            },
        );
        let changes = apply_to_source(
            &mut s,
            &ReplicationChange::Truncate {
                relations: vec![relation("other")],
            },
        );
        assert_eq!(changes, vec![]);
        assert_eq!(
            s.fetch(&Default::default()).count(),
            1,
            "unrelated truncate should not clear this source"
        );
    }

    #[test]
    fn truncate_of_empty_table_produces_no_changes() {
        let mut s = source();
        let changes = apply_to_source(
            &mut s,
            &ReplicationChange::Truncate {
                relations: vec![relation("t")],
            },
        );
        assert_eq!(changes, vec![]);
    }

    #[test]
    fn update_without_key_uses_new_rows_primary_key() {
        let mut s = source();
        apply_to_source(
            &mut s,
            &ReplicationChange::Insert {
                relation: relation("t"),
                new: row(1, true),
            },
        );
        // Key unchanged -> Postgres sends no old tuple at all.
        let ivm_changes = apply_to_source(
            &mut s,
            &ReplicationChange::Update {
                relation: relation("t"),
                key: None,
                new: row(1, false),
            },
        );
        assert_eq!(
            ivm_changes,
            vec![IvmChange::Edit {
                node: Node::new(row(1, false)),
                old_node: Node::new(row(1, true))
            }]
        );
    }

    fn issue(id: i64) -> Row {
        vec![("id".into(), JsonValue::Number(id as f64))]
    }
    fn comment(id: i64, issue_id: i64) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("issueID".into(), JsonValue::Number(issue_id as f64)),
        ]
    }
    fn issues_correlation() -> zero_cache_protocol::ast::Correlation {
        zero_cache_protocol::ast::Correlation {
            parent_field: vec!["id".into()],
            child_field: vec!["issueID".into()],
        }
    }
    fn issues_source() -> TableSource {
        TableSource::new(
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        )
    }
    fn comments_source() -> TableSource {
        TableSource::new(
            "comments",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        )
    }

    #[test]
    fn apply_to_child_and_reeval_exists_detects_a_new_match_via_replication_insert() {
        let mut issues = issues_source();
        let mut comments = comments_source();
        apply_to_source(
            &mut issues,
            &ReplicationChange::Insert {
                relation: relation("issues"),
                new: issue(1),
            },
        );

        let results = apply_to_child_and_reeval_exists(
            &mut comments,
            &ReplicationChange::Insert {
                relation: relation("comments"),
                new: comment(10, 1),
            },
            &issues,
            &issues_correlation(),
        );
        assert_eq!(results, vec![(issue(1), true)]);
    }

    #[test]
    fn apply_to_child_and_reeval_exists_detects_last_match_removed() {
        let mut issues = issues_source();
        let mut comments = comments_source();
        apply_to_source(
            &mut issues,
            &ReplicationChange::Insert {
                relation: relation("issues"),
                new: issue(1),
            },
        );
        apply_to_source(
            &mut comments,
            &ReplicationChange::Insert {
                relation: relation("comments"),
                new: comment(10, 1),
            },
        );

        let results = apply_to_child_and_reeval_exists(
            &mut comments,
            &ReplicationChange::Delete {
                relation: relation("comments"),
                key: vec![("id".into(), JsonValue::Number(10.0))],
            },
            &issues,
            &issues_correlation(),
        );
        assert_eq!(results, vec![(issue(1), false)]);
    }

    #[test]
    fn apply_to_child_and_reeval_exists_ignores_unrelated_table() {
        let issues = issues_source();
        let mut other = TableSource::new(
            "other",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let results = apply_to_child_and_reeval_exists(
            &mut other,
            &ReplicationChange::Insert {
                relation: relation("other"),
                new: row(1, true),
            },
            &issues,
            &issues_correlation(),
        );
        assert_eq!(results, vec![]);
    }

    #[test]
    fn apply_to_child_and_reeval_relationship_adds_new_child_via_replication_insert() {
        let mut issues = issues_source();
        let mut comments = comments_source();
        apply_to_source(
            &mut issues,
            &ReplicationChange::Insert {
                relation: relation("issues"),
                new: issue(1),
            },
        );

        let results = apply_to_child_and_reeval_relationship(
            &mut comments,
            &ReplicationChange::Insert {
                relation: relation("comments"),
                new: comment(10, 1),
            },
            &issues,
            &issues_correlation(),
            "comments",
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row, issue(1));
        assert_eq!(
            results[0].relationships["comments"],
            vec![Node::new(comment(10, 1))]
        );
    }

    #[test]
    fn apply_to_child_and_reeval_relationship_drops_removed_child() {
        let mut issues = issues_source();
        let mut comments = comments_source();
        apply_to_source(
            &mut issues,
            &ReplicationChange::Insert {
                relation: relation("issues"),
                new: issue(1),
            },
        );
        apply_to_source(
            &mut comments,
            &ReplicationChange::Insert {
                relation: relation("comments"),
                new: comment(10, 1),
            },
        );
        apply_to_source(
            &mut comments,
            &ReplicationChange::Insert {
                relation: relation("comments"),
                new: comment(11, 1),
            },
        );

        let results = apply_to_child_and_reeval_relationship(
            &mut comments,
            &ReplicationChange::Delete {
                relation: relation("comments"),
                key: vec![("id".into(), JsonValue::Number(10.0))],
            },
            &issues,
            &issues_correlation(),
            "comments",
        );
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].relationships["comments"],
            vec![Node::new(comment(11, 1))],
            "only the remaining comment should survive"
        );
    }

    #[test]
    fn apply_to_child_and_reeval_relationship_ignores_unrelated_table() {
        let issues = issues_source();
        let mut other = TableSource::new(
            "other",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        );
        let results = apply_to_child_and_reeval_relationship(
            &mut other,
            &ReplicationChange::Insert {
                relation: relation("other"),
                new: row(1, true),
            },
            &issues,
            &issues_correlation(),
            "comments",
        );
        assert_eq!(results, vec![]);
    }
}
