//! A test-only in-memory [`Input`] that fully honors `constraint`, `start`
//! and `reverse` fetch semantics — the contract `Skip`/`Take` rely on their
//! upstream `Input` to provide (upstream's `MemorySource.connect(...).fetch`).
//!
//! The existing [`crate::ivm::table_source::TableSource`] deliberately does
//! not implement `start`-based resumption (see its module doc), but `Skip`
//! and `Take` fetch their input with `start`/`reverse` set, so their tests
//! need a source that matches `MemorySource`'s fetch: filter by constraint,
//! sort per `reverse`, then splice to the `start` position
//! (`memory-source.ts`'s `generateWithStart`). This helper provides exactly
//! that, backed by a `RefCell<Vec<Row>>` so it can be shared as an
//! `Rc<dyn Input>` while still accepting `push_change` mutations from a test.

use std::cell::RefCell;
use std::rc::Rc;

use zero_cache_protocol::ast::Ordering;

use crate::ivm::change::SourceChange;
use crate::ivm::constraint::{constraint_matches_row, PrimaryKey};
use crate::ivm::data::{make_comparator, Row, Value};
use crate::ivm::operator::{
    Change, FetchRequest, Input, InputBase, Node, Output, SourceSchema, Start, StartBasis, Stream,
};

fn get(row: &Row, field: &str) -> Value {
    row.iter()
        .find(|(k, _)| k == field)
        .map(|(_, v)| v.clone())
        .unwrap_or(Value::Null)
}

fn pk_values(row: &Row, primary_key: &PrimaryKey) -> Vec<Value> {
    primary_key.iter().map(|col| get(row, col)).collect()
}

pub struct TestSource {
    schema: SourceSchema,
    rows: RefCell<Vec<Row>>,
    output: RefCell<Option<Rc<dyn Output>>>,
}

impl TestSource {
    pub fn new(table_name: impl Into<String>, primary_key: PrimaryKey, sort: Ordering) -> Rc<Self> {
        Rc::new(TestSource {
            schema: SourceSchema {
                table_name: table_name.into(),
                primary_key,
                sort,
                relationships: std::collections::BTreeMap::new(),
            },
            rows: RefCell::new(Vec::new()),
            output: RefCell::new(None),
        })
    }

    /// Applies a row-level change to the backing rows and returns the
    /// resulting operator-level [`Change`], WITHOUT forwarding it downstream
    /// (tests drive the change into the operator under test explicitly). The
    /// rows are mutated first, so a subsequent `fetch` by the operator sees
    /// post-change state — the same net effect as `MemorySource`'s
    /// during-push overlay.
    pub fn push_change(&self, change: SourceChange) -> Change {
        let pk = &self.schema.primary_key;
        let mut rows = self.rows.borrow_mut();
        match change {
            SourceChange::Add(row) => {
                let key = pk_values(&row, pk);
                assert!(
                    !rows.iter().any(|r| pk_values(r, pk) == key),
                    "TestSource: Add of an existing primary key"
                );
                rows.push(row.clone());
                Change::Add(Node::new(row))
            }
            SourceChange::Remove(row) => {
                let key = pk_values(&row, pk);
                let idx = rows
                    .iter()
                    .position(|r| pk_values(r, pk) == key)
                    .expect("TestSource: Remove of a missing primary key");
                rows.remove(idx);
                Change::Remove(Node::new(row))
            }
            SourceChange::Edit { row, old_row } => {
                let key = pk_values(&old_row, pk);
                let idx = rows
                    .iter()
                    .position(|r| pk_values(r, pk) == key)
                    .expect("TestSource: Edit of a missing primary key");
                rows[idx] = row.clone();
                Change::Edit {
                    node: Node::new(row),
                    old_node: Node::new(old_row),
                }
            }
        }
    }
}

impl InputBase for TestSource {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }
    fn destroy(&self) {}
}

impl Input for TestSource {
    fn set_output(&self, output: Rc<dyn Output>) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node> {
        let rows = self.rows.borrow();
        let mut matching: Vec<Row> = rows
            .iter()
            .filter(|r| match &req.constraint {
                Some(c) => constraint_matches_row(c, r),
                None => true,
            })
            .filter(|r| {
                req.multi_constraints.iter().all(|batch| {
                    !batch.is_empty()
                        && batch
                            .iter()
                            .any(|constraint| constraint_matches_row(constraint, r))
                })
            })
            .cloned()
            .collect();

        let cmp = make_comparator(&self.schema.sort, req.reverse);
        matching.sort_by(|a, b| cmp(a, b));

        // Splice to `start` — port of `generateWithStart` (memory-source.ts).
        // The comparator already accounts for `reverse`.
        let start = req.start.clone();
        let out: Vec<Node> = apply_start(matching, start, &cmp)
            .into_iter()
            .map(Node::new)
            .collect();
        Box::new(out.into_iter())
    }
}

fn apply_start(
    rows: Vec<Row>,
    start: Option<Start>,
    cmp: &impl Fn(&Row, &Row) -> std::cmp::Ordering,
) -> Vec<Row> {
    let Some(start) = start else {
        return rows;
    };
    let mut started = false;
    let mut out = Vec::new();
    for row in rows {
        if !started {
            let c = cmp(&row, &start.row);
            match start.basis {
                StartBasis::At => {
                    if c != std::cmp::Ordering::Less {
                        started = true;
                    }
                }
                StartBasis::After => {
                    if c == std::cmp::Ordering::Greater {
                        started = true;
                    }
                }
            }
        }
        if started {
            out.push(row);
        }
    }
    out
}

/// An [`Output`] that records every pushed [`Change`] — the Rust analogue of
/// upstream's `Catch` for asserting an operator's emitted change sequence.
pub struct SpyOutput {
    pub received: RefCell<Vec<Change>>,
}

impl SpyOutput {
    pub fn new() -> Rc<Self> {
        Rc::new(SpyOutput {
            received: RefCell::new(Vec::new()),
        })
    }
}

impl Output for SpyOutput {
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        self.received.borrow_mut().push(change);
    }
}
