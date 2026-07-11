//! Port of the `Streamer` in `zero-cache/src/services/view-syncer/pipeline-
//! driver.ts` (`#streamChanges`/`#streamNodes`, pipeline-driver.ts:1296-1390):
//! the [`Output`] you attach to a built pipeline's root to turn the operator-
//! level [`Change`]s pushed at it into a flat list of per-row changes.
//!
//! **What it flattens, and how it maps to upstream.** A single [`Change`] can
//! carry a whole subtree of rows (a `Node` plus its relationship children, one
//! per `related` hop / `whereExists` the pipeline wired as a `JoinInput`). The
//! `Collector` walks that subtree exactly as upstream's `Streamer` does:
//!
//! - **`Add`/`Remove`** — emit the primary node's row, then recurse into every
//!   `Node.relationships[name]` child with the SAME op, resolving each child's
//!   table via `schema.relationships[name]` (upstream `#streamNodes`). A
//!   `Remove` carries no row body — only its `row_key` — matching upstream's
//!   `row: op === REMOVE ? undefined : row`.
//! - **`Edit`** — emit ONLY the edited row (no relationships), matching
//!   upstream streaming `{row: change.node.row, relationships: {}}`.
//! - **`Child`** — descend to the named child relationship's schema and
//!   recurse into the child's own [`Change`] (upstream `#streamChanges` on
//!   `child.change` with `schema.relationships[child.relationshipName]`), so a
//!   child add/remove flattens at the child table.
//!
//! **In-crate change record.** The driver's `PipelineRowChange` (with its
//! `_0_version` `min_row_version` clamp and `RowKey` encoding) lives in the
//! `zero-cache-view-syncer` crate, above this one. So the `Collector` emits a
//! source-agnostic [`CollectorRowChange`] the driver maps to `PipelineRowChange`
//! (applying the version clamp) later — this crate stays below the replica.
//!
//! **Scope note.** Upstream skips `schema.system === 'permissions'` subtrees;
//! this port's [`SourceSchema`] carries no `system` field (the port targets
//! server-authoritative apps without compiled `definePermissions` in the graph
//! — see `PORTING.md`), so there is nothing to skip.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::data::Row;
use crate::ivm::operator::{Change, InputBase, Node, Output, SourceSchema};

/// The kind of a flattened row change. Port of the non-`CHILD` `ChangeType`s
/// (`Add`/`Remove`/`Edit`) an emitted `RowChange` can carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectorChangeKind {
    Add,
    Remove,
    Edit,
}

/// One flattened per-row change the [`Collector`] emits. Port of upstream's
/// `RowChange` (`RowAdd`/`RowRemove`/`RowEdit`, pipeline-driver.ts:69-83),
/// trimmed to the crate-level shape (no `queryID`, and the driver applies the
/// `min_row_version` clamp when mapping to `PipelineRowChange`).
#[derive(Debug, Clone, PartialEq)]
pub struct CollectorRowChange {
    /// The table the row belongs to — the `schema.table_name` at the depth this
    /// row was reached (root table for a root row, a child table for a
    /// relationship row).
    pub table: String,
    pub kind: CollectorChangeKind,
    /// The primary-key columns of the row (in primary-key order). Present for
    /// every kind — it identifies a `Remove`d row that carries no body.
    pub row_key: Row,
    /// The full row body — `None` for a `Remove` (upstream sends only the key),
    /// `Some` for `Add`/`Edit`.
    pub row: Option<Row>,
}

/// Accumulates the [`Change`]s pushed at a pipeline root and flattens each into
/// [`CollectorRowChange`]s. Attach with `root.set_output(collector)`.
pub struct Collector {
    /// The root pipeline's schema, carrying the nested `relationships` used to
    /// resolve child tables while flattening.
    schema: SourceSchema,
    collected: RefCell<Vec<CollectorRowChange>>,
}

impl Collector {
    /// Builds a `Collector` for a pipeline whose root reports `schema`
    /// (`root.get_schema()`).
    pub fn new(schema: SourceSchema) -> Rc<Self> {
        Rc::new(Collector {
            schema,
            collected: RefCell::new(Vec::new()),
        })
    }

    /// Drains the flattened changes accumulated so far.
    pub fn take(&self) -> Vec<CollectorRowChange> {
        std::mem::take(&mut self.collected.borrow_mut())
    }

    /// A snapshot of the flattened changes accumulated so far, without draining.
    pub fn changes(&self) -> Vec<CollectorRowChange> {
        self.collected.borrow().clone()
    }

    /// Port of `Streamer.#streamChanges` for a single change: dispatches on the
    /// change kind, recursing into a `Child`'s relationship schema.
    fn stream_change(&self, schema: &SourceSchema, change: &Change) {
        match change {
            Change::Add(node) => self.stream_nodes(schema, CollectorChangeKind::Add, node),
            Change::Remove(node) => self.stream_nodes(schema, CollectorChangeKind::Remove, node),
            Change::Edit { node, .. } => {
                // Upstream streams an edit as just its row, no relationships.
                let row_only = Node::new(node.row.clone());
                self.stream_nodes(schema, CollectorChangeKind::Edit, &row_only);
            }
            Change::Child { child, .. } => {
                if let Some(child_schema) = schema
                    .relationships
                    .get(&child.relationship_name)
                    .map(|b| b.as_ref())
                {
                    self.stream_change(child_schema, &child.change);
                }
            }
        }
    }

    /// Port of `Streamer.#streamNodes`: emits `node`'s row then recurses into
    /// each of its relationships with the same op, resolving each child's schema
    /// (and thus table) via `schema.relationships[name]`.
    fn stream_nodes(&self, schema: &SourceSchema, kind: CollectorChangeKind, node: &Node) {
        let row_key = key_of(&schema.primary_key, &node.row);
        let row = match kind {
            CollectorChangeKind::Remove => None,
            CollectorChangeKind::Add | CollectorChangeKind::Edit => Some(node.row.clone()),
        };
        self.collected.borrow_mut().push(CollectorRowChange {
            table: schema.table_name.clone(),
            kind,
            row_key,
            row,
        });

        // Iterate relationships in a stable (sorted) order — `Node.relationships`
        // is a `HashMap`, so an unsorted walk would be nondeterministic.
        let mut names: Vec<&String> = node.relationships.keys().collect();
        names.sort();
        for name in names {
            let Some(child_schema) = schema.relationships.get(name).map(|b| b.as_ref()) else {
                continue;
            };
            for child in &node.relationships[name] {
                self.stream_nodes(child_schema, kind, child);
            }
        }
    }
}

impl Output for Collector {
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        self.stream_change(&self.schema, &change);
    }
}

/// The primary-key columns of `row`, in `primary_key` order. Port of
/// `getRowKey(primaryKey, row)`.
fn key_of(primary_key: &[String], row: &Row) -> Row {
    primary_key
        .iter()
        .map(|col| {
            let value = row
                .iter()
                .find(|(k, _)| k == col)
                .map(|(_, v)| v.clone())
                .unwrap_or(zero_cache_shared::bigint_json::JsonValue::Null);
            (col.clone(), value)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::pipeline::{build_pipeline, BuildDelegate};
    use crate::ivm::change::{
        make_source_change_add, make_source_change_edit, make_source_change_remove,
    };
    use crate::ivm::data::Row;
    use crate::ivm::exists::{Exists, ExistsType};
    use crate::ivm::join_input::JoinInput;
    use crate::ivm::memory_storage::MemoryStorage;
    use crate::ivm::operator::{
        make_child_change, Change, Input, InputBase, Node, Output, Storage,
    };
    use crate::ivm::test_input::TestSource;
    use std::collections::HashMap;
    use std::rc::Rc;
    use zero_cache_protocol::ast::{
        Ast, ColumnReference, Condition, CorrelatedSubquery, Correlation, Direction, ExistsOp,
        LiteralValue, Ordering, SimpleOperator, ValuePosition,
    };
    use zero_cache_shared::bigint_json::JsonValue;

    // ---- row/source helpers ----

    fn issue(id: i64, active: bool) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("active".into(), JsonValue::Bool(active)),
        ]
    }
    fn comment(id: i64, issue_id: i64) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("issueID".into(), JsonValue::Number(issue_id as f64)),
        ]
    }
    fn id_key(id: i64) -> Row {
        vec![("id".into(), JsonValue::Number(id as f64))]
    }

    fn issue_source() -> Rc<TestSource> {
        TestSource::new(
            "issue",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        )
    }
    fn comment_source() -> Rc<TestSource> {
        TestSource::new(
            "comment",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
        )
    }

    fn make_storage(_name: &str) -> Rc<dyn Storage> {
        Rc::new(MemoryStorage::default())
    }

    fn where_active() -> Condition {
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: "active".into(),
            }),
            right: ValuePosition::Literal(LiteralValue::Bool(true)),
        }
    }

    fn comments_subquery() -> CorrelatedSubquery {
        CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Ast {
                table: "comment".into(),
                alias: Some("comments".into()),
                ..Default::default()
            }),
            system: None,
            hidden: None,
        }
    }

    // ---- filter pipeline: Add / Remove / Edit push through to the Collector ----

    #[test]
    fn built_filter_pipeline_flattens_add_remove_edit() {
        let source = issue_source();
        let map: HashMap<String, Rc<dyn Input>> =
            HashMap::from([("issue".to_string(), source.clone() as Rc<dyn Input>)]);
        let get_source = |t: &str, _o: Option<&Ordering>| map.get(t).cloned().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            where_: Some(where_active()),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let collector = Collector::new(root.get_schema());
        root.set_output(collector.clone());

        // Active add -> forwarded as an Add of the row.
        source.push_and_forward(make_source_change_add(issue(1, true)));
        // Inactive add -> filtered out (predicate fails).
        source.push_and_forward(make_source_change_add(issue(2, false)));
        // Edit that keeps it active -> forwarded as an Edit.
        source.push_and_forward(make_source_change_edit(issue(1, true), issue(1, true)));
        // Remove of the active row -> forwarded as a Remove (key only).
        source.push_and_forward(make_source_change_remove(issue(1, true)));

        assert_eq!(
            collector.take(),
            vec![
                CollectorRowChange {
                    table: "issue".into(),
                    kind: CollectorChangeKind::Add,
                    row_key: id_key(1),
                    row: Some(issue(1, true)),
                },
                CollectorRowChange {
                    table: "issue".into(),
                    kind: CollectorChangeKind::Edit,
                    row_key: id_key(1),
                    row: Some(issue(1, true)),
                },
                CollectorRowChange {
                    table: "issue".into(),
                    kind: CollectorChangeKind::Remove,
                    row_key: id_key(1),
                    row: None,
                },
            ]
        );
    }

    /// A filter `Edit` that leaves the predicate is reclassified to a `Remove`
    /// (by `filter_push`) and reaches the Collector as a `Remove`.
    #[test]
    fn built_filter_pipeline_edit_leaving_predicate_becomes_remove() {
        let source = issue_source();
        let map: HashMap<String, Rc<dyn Input>> =
            HashMap::from([("issue".to_string(), source.clone() as Rc<dyn Input>)]);
        let get_source = |t: &str, _o: Option<&Ordering>| map.get(t).cloned().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            where_: Some(where_active()),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let collector = Collector::new(root.get_schema());
        root.set_output(collector.clone());

        source.push_and_forward(make_source_change_add(issue(1, true)));
        source.push_and_forward(make_source_change_edit(issue(1, false), issue(1, true)));

        assert_eq!(
            collector.take(),
            vec![
                CollectorRowChange {
                    table: "issue".into(),
                    kind: CollectorChangeKind::Add,
                    row_key: id_key(1),
                    row: Some(issue(1, true)),
                },
                CollectorRowChange {
                    table: "issue".into(),
                    kind: CollectorChangeKind::Remove,
                    row_key: id_key(1),
                    row: None,
                },
            ]
        );
    }

    // ---- take pipeline: an add that evicts the bound emits Remove then Add ----

    #[test]
    fn built_take_pipeline_evicts_bound_on_add() {
        let source = issue_source();
        for id in [1, 2, 3] {
            source.push_change(make_source_change_add(issue(id, true)));
        }
        let map: HashMap<String, Rc<dyn Input>> =
            HashMap::from([("issue".to_string(), source.clone() as Rc<dyn Input>)]);
        let get_source = |t: &str, _o: Option<&Ordering>| map.get(t).cloned().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            limit: Some(2.0),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        // Hydrate the take state (as the driver does) before attaching the sink.
        let _ = root.fetch(&Default::default()).count();
        let collector = Collector::new(root.get_schema());
        root.set_output(collector.clone());

        // Adding id=0 (sorts before the window) evicts the bound id=2, then adds
        // id=0 — the Take emits Remove(2) before Add(0) to keep size <= limit.
        source.push_and_forward(make_source_change_add(issue(0, true)));

        assert_eq!(
            collector.take(),
            vec![
                CollectorRowChange {
                    table: "issue".into(),
                    kind: CollectorChangeKind::Remove,
                    row_key: id_key(2),
                    row: None,
                },
                CollectorRowChange {
                    table: "issue".into(),
                    kind: CollectorChangeKind::Add,
                    row_key: id_key(0),
                    row: Some(issue(0, true)),
                },
            ]
        );
    }

    // ---- exists pipeline: a watched child add (0->1) emits a parent Add ----
    //
    // `build_pipeline` assembles `where EXISTS(comments)` as
    // `source → JoinInput → Exists`. The `JoinInput` is fetch-only (its
    // `set_output` is a no-op until the push-capable join of increment 5), so a
    // source push can't yet reach the `Exists`. This test drives the child
    // `Change::Child` into the `Exists` the way that join WILL, over the exact
    // `JoinInput` + `Exists` shape the builder wires, proving the Exists→Collector
    // push edge.

    #[test]
    fn built_exists_pipeline_child_add_emits_parent_add() {
        let issues = issue_source();
        let comments = comment_source();
        let join = JoinInput::new(
            issues as Rc<dyn Input>,
            comments as Rc<dyn Input>,
            vec!["id".into()],
            vec!["issueID".into()],
            "comments",
        );
        let storage: Rc<dyn Storage> = Rc::new(MemoryStorage::default());
        let exists = Exists::new(
            join as Rc<dyn Input>,
            storage,
            "comments",
            vec!["id".into()],
            ExistsType::Exists,
        );
        let collector = Collector::new(exists.get_schema());
        exists.set_output(collector.clone());

        // Post-add the parent's `comments` relationship has one child => a 0->1
        // flip => Exists emits Change::Add(parent). The Collector flattens the
        // parent row and (per schema) its `comments` child row.
        let mut parent = Node::new(issue(1, true));
        parent
            .relationships
            .insert("comments".into(), vec![Node::new(comment(10, 1))]);
        exists.push(
            make_child_change(parent, "comments", Change::Add(Node::new(comment(10, 1)))),
            &*exists,
        );

        assert_eq!(
            collector.take(),
            vec![
                CollectorRowChange {
                    table: "issue".into(),
                    kind: CollectorChangeKind::Add,
                    row_key: id_key(1),
                    row: Some(issue(1, true)),
                },
                CollectorRowChange {
                    table: "comment".into(),
                    kind: CollectorChangeKind::Add,
                    row_key: id_key(10),
                    row: Some(comment(10, 1)),
                },
            ]
        );
    }

    // ---- related hop: a Child change flattens at the child table ----

    #[test]
    fn built_related_pipeline_flattens_child_change() {
        let issues = issue_source();
        let comments = comment_source();
        let map: HashMap<String, Rc<dyn Input>> = HashMap::from([
            ("issue".to_string(), issues as Rc<dyn Input>),
            ("comment".to_string(), comments as Rc<dyn Input>),
        ]);
        let get_source = |t: &str, _o: Option<&Ordering>| map.get(t).cloned().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            related: Some(vec![comments_subquery()]),
            ..Default::default()
        };
        // The related root is a fetch-only `JoinInput`; its schema carries the
        // `comments` relationship the Collector needs to resolve the child table.
        let root = build_pipeline(&ast, &delegate);
        let collector = Collector::new(root.get_schema());

        // A child add on the `comments` relationship of issue 1.
        let child = make_child_change(
            Node::new(issue(1, true)),
            "comments",
            Change::Add(Node::new(comment(10, 1))),
        );
        collector.push(child, &*root);

        // The Child change descends to the `comment` schema and flattens the
        // added comment row — the parent issue row is NOT re-emitted.
        assert_eq!(
            collector.take(),
            vec![CollectorRowChange {
                table: "comment".into(),
                kind: CollectorChangeKind::Add,
                row_key: id_key(10),
                row: Some(comment(10, 1)),
            }]
        );
    }

    // ---- OR-of-exists: fan-out -> fan-in -> Collector ----

    /// `active = true OR EXISTS(comments)` builds `FanOut → (exists branch,
    /// filter branch) → FanIn`. Pushing an active issue flows through the filter
    /// branch, the fan-in collapses the (single) branch push, and the Collector
    /// receives exactly one Add. (The EXISTS branch's push edge from the fan-out
    /// awaits the push-capable join of increment 5.)
    #[test]
    fn built_or_of_exists_pipeline_pushes_through_fan_in() {
        let issues = issue_source();
        let comments = comment_source();
        let source = issues.clone();
        let map: HashMap<String, Rc<dyn Input>> = HashMap::from([
            ("issue".to_string(), issues as Rc<dyn Input>),
            ("comment".to_string(), comments as Rc<dyn Input>),
        ]);
        let get_source = |t: &str, _o: Option<&Ordering>| map.get(t).cloned().unwrap();
        let create_storage = make_storage;
        let delegate = BuildDelegate {
            get_source: &get_source,
            create_storage: &create_storage,
        };
        let ast = Ast {
            table: "issue".into(),
            where_: Some(Condition::Or {
                conditions: vec![
                    where_active(),
                    Condition::CorrelatedSubquery {
                        related: comments_subquery(),
                        op: ExistsOp::Exists,
                        flip: None,
                        scalar: None,
                        plan_id: None,
                    },
                ],
            }),
            ..Default::default()
        };
        let root = build_pipeline(&ast, &delegate);
        let collector = Collector::new(root.get_schema());
        root.set_output(collector.clone());

        // Active issue -> filter branch forwards -> fan-in -> Collector (once).
        source.push_and_forward(make_source_change_add(issue(1, true)));
        // Inactive issue with no comment -> neither branch forwards.
        source.push_and_forward(make_source_change_add(issue(2, false)));

        assert_eq!(
            collector.take(),
            vec![CollectorRowChange {
                table: "issue".into(),
                kind: CollectorChangeKind::Add,
                row_key: id_key(1),
                row: Some(issue(1, true)),
            }]
        );
    }

    // ---- unit: Child change with no matching relationship schema is skipped ----

    #[test]
    fn child_change_for_unknown_relationship_is_skipped() {
        use std::collections::BTreeMap;
        let schema = SourceSchema {
            table_name: "issue".into(),
            primary_key: vec!["id".into()],
            sort: vec![("id".into(), Direction::Asc)],
            relationships: BTreeMap::new(),
        };
        let collector = Collector::new(schema);
        let dummy = issue_source();
        let child = make_child_change(
            Node::new(issue(1, true)),
            "comments",
            Change::Add(Node::new(comment(10, 1))),
        );
        collector.push(child, &*dummy);
        assert!(collector.changes().is_empty());
    }
}
