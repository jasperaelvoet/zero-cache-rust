//! A minimal in-memory `Source`, standing in for `zqlite/src/table-source.ts`
//! (the real SQLite-backed source) for this first IVM slice. Port of the
//! `Source` interface from `zql/src/ivm/source.ts`, restricted to a single
//! connected output (see `ivm::operator`'s module doc for why) and backed by
//! a `Vec<Row>` rather than a live SQLite query.
//!
//! A real `TableSource` reading through `zero-cache-sqlite::StatementRunner`
//! (matching `zqlite/src/table-source.ts`) is the natural next increment;
//! this one exists so `Filter` and the `SourceChange -> Change` push
//! semantics can be built and tested now, independent of that SQL layer.

use crate::ivm::constraint::{constraint_matches_row, Constraint, PrimaryKey};
use crate::ivm::data::{make_comparator, Row, Value};
use crate::ivm::operator::{Change, FetchRequest, Node, SourceSchema, Stream};
use zero_cache_protocol::ast::Ordering;

use crate::ivm::change::SourceChange;

fn get(row: &Row, field: &str) -> Value {
    row.iter()
        .find(|(k, _)| k == field)
        .map(|(_, v)| v.clone())
        .unwrap_or(Value::Null)
}

/// Extracts a row's primary-key values, in primary-key column order — used
/// as the row-identity key for `push`'s Add/Remove/Edit matching (mirroring
/// how upstream's real `TableSource` uses the SQL primary key for row
/// identity rather than JS object identity).
fn pk_values(row: &Row, primary_key: &PrimaryKey) -> Vec<Value> {
    primary_key.iter().map(|col| get(row, col)).collect()
}

/// An in-memory table of rows, playing the `Source` role. Port of `Source`
/// (`connect`/`push`), single-output only.
pub struct TableSource {
    schema: SourceSchema,
    rows: Vec<Row>,
}

impl TableSource {
    pub fn new(table_name: impl Into<String>, primary_key: PrimaryKey, sort: Ordering) -> Self {
        TableSource {
            schema: SourceSchema {
                table_name: table_name.into(),
                primary_key,
                sort,
            },
            rows: Vec::new(),
        }
    }

    pub fn schema(&self) -> &SourceSchema {
        &self.schema
    }

    /// Looks up the current full row matching `key` (a `Constraint` over
    /// some subset of columns, typically just the primary key). Used by
    /// callers that only received a partial "old row" (e.g. a replicated
    /// UPDATE's key-only old tuple) but need the full previous row to
    /// correctly evaluate a filter predicate against it — the `TableSource`
    /// is the authoritative current-state store, unlike the replication
    /// stream which only reports what changed. Not an upstream-named
    /// method; upstream's real `TableSource` reads through SQL instead.
    pub fn find_by_key(&self, key: &Constraint) -> Option<&Row> {
        self.rows.iter().find(|r| constraint_matches_row(key, r))
    }

    /// Returns rows matching `req.constraint` (if any), sorted per
    /// `req.reverse`. Port of `Source.connect(...).fetch(req)`, minus
    /// `start`-based resumption (not needed until pagination is ported).
    pub fn fetch(&self, req: &FetchRequest) -> Stream<'_, Node> {
        let mut matching: Vec<&Row> = match &req.constraint {
            Some(c) => self
                .rows
                .iter()
                .filter(|r| constraint_matches_row(c, r))
                .collect(),
            None => self.rows.iter().collect(),
        };
        let cmp = make_comparator(&self.schema.sort, req.reverse);
        matching.sort_by(|a, b| cmp(a, b));
        Box::new(matching.into_iter().map(|row| Node::new(row.clone())))
    }

    /// Applies a row-level change to the table and returns the resulting
    /// operator-level `Change`. Port of `Source.push` (the row-set-mutation
    /// half of it; fanning the result out to multiple connected outputs is
    /// out of scope for this single-output v1 — see module doc).
    ///
    /// Panics on Add of a row whose primary key already exists, or
    /// Remove/Edit of a row whose primary key doesn't — mirrors upstream's
    /// `Output.push` contract comment ("Only add rows which do not already
    /// exist... only remove rows which do exist"), enforced here rather than
    /// left as caller-trusted, since this source is also the row-set of
    /// record.
    pub fn push(&mut self, change: SourceChange) -> Change {
        match change {
            SourceChange::Add(row) => {
                let key = pk_values(&row, &self.schema.primary_key);
                assert!(
                    !self
                        .rows
                        .iter()
                        .any(|r| pk_values(r, &self.schema.primary_key) == key),
                    "TableSource::push: Add of a row whose primary key already exists"
                );
                self.rows.push(row.clone());
                Change::Add(Node::new(row))
            }
            SourceChange::Remove(row) => {
                let key = pk_values(&row, &self.schema.primary_key);
                let idx = self
                    .rows
                    .iter()
                    .position(|r| pk_values(r, &self.schema.primary_key) == key)
                    .expect("TableSource::push: Remove of a row whose primary key does not exist");
                self.rows.remove(idx);
                Change::Remove(Node::new(row))
            }
            SourceChange::Edit { row, old_row } => {
                let old_key = pk_values(&old_row, &self.schema.primary_key);
                let idx = self
                    .rows
                    .iter()
                    .position(|r| pk_values(r, &self.schema.primary_key) == old_key)
                    .expect("TableSource::push: Edit of a row whose primary key does not exist");
                self.rows[idx] = row.clone();
                Change::Edit {
                    node: Node::new(row),
                    old_node: Node::new(old_row),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::{
        make_source_change_add, make_source_change_edit, make_source_change_remove,
    };
    use zero_cache_protocol::ast::Direction;
    use zero_cache_shared::bigint_json::JsonValue;

    fn row(id: i64, name: &str) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("name".into(), JsonValue::String(name.into())),
        ]
    }

    fn source() -> TableSource {
        TableSource::new("t", vec!["id".into()], vec![("id".into(), Direction::Asc)])
    }

    #[test]
    fn push_add_then_fetch_returns_it() {
        let mut s = source();
        s.push(make_source_change_add(row(1, "a")));
        let rows: Vec<Node> = s.fetch(&FetchRequest::default()).collect();
        assert_eq!(rows, vec![Node::new(row(1, "a"))]);
    }

    #[test]
    fn push_add_returns_add_change() {
        let mut s = source();
        let change = s.push(make_source_change_add(row(1, "a")));
        assert_eq!(change, Change::Add(Node::new(row(1, "a"))));
    }

    #[test]
    #[should_panic(expected = "already exists")]
    fn push_add_duplicate_primary_key_panics() {
        let mut s = source();
        s.push(make_source_change_add(row(1, "a")));
        s.push(make_source_change_add(row(1, "b")));
    }

    #[test]
    fn push_remove_deletes_row() {
        let mut s = source();
        s.push(make_source_change_add(row(1, "a")));
        let change = s.push(make_source_change_remove(row(1, "a")));
        assert_eq!(change, Change::Remove(Node::new(row(1, "a"))));
        assert_eq!(s.fetch(&FetchRequest::default()).count(), 0);
    }

    #[test]
    #[should_panic(expected = "does not exist")]
    fn push_remove_missing_row_panics() {
        let mut s = source();
        s.push(make_source_change_remove(row(1, "a")));
    }

    #[test]
    fn push_edit_replaces_row_by_primary_key() {
        let mut s = source();
        s.push(make_source_change_add(row(1, "a")));
        let change = s.push(make_source_change_edit(row(1, "b"), row(1, "a")));
        assert_eq!(
            change,
            Change::Edit {
                node: Node::new(row(1, "b")),
                old_node: Node::new(row(1, "a"))
            }
        );
        let rows: Vec<Node> = s.fetch(&FetchRequest::default()).collect();
        assert_eq!(rows, vec![Node::new(row(1, "b"))]);
    }

    #[test]
    fn fetch_applies_constraint() {
        let mut s = source();
        s.push(make_source_change_add(row(1, "a")));
        s.push(make_source_change_add(row(2, "b")));
        let req = FetchRequest {
            constraint: Some(vec![("id".into(), JsonValue::Number(2.0))]),
            ..Default::default()
        };
        let rows: Vec<Node> = s.fetch(&req).collect();
        assert_eq!(rows, vec![Node::new(row(2, "b"))]);
    }

    #[test]
    fn fetch_sorts_per_schema_sort_and_honors_reverse() {
        let mut s = source();
        s.push(make_source_change_add(row(2, "b")));
        s.push(make_source_change_add(row(1, "a")));
        let ids: Vec<i64> = s
            .fetch(&FetchRequest::default())
            .map(|n| match get(&n.row, "id") {
                JsonValue::Number(v) => v as i64,
                _ => panic!(),
            })
            .collect();
        assert_eq!(ids, vec![1, 2]);

        let req = FetchRequest {
            reverse: true,
            ..Default::default()
        };
        let ids: Vec<i64> = s
            .fetch(&req)
            .map(|n| match get(&n.row, "id") {
                JsonValue::Number(v) => v as i64,
                _ => panic!(),
            })
            .collect();
        assert_eq!(ids, vec![2, 1]);
    }
}
