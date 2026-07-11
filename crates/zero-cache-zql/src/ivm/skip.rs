//! Port of `zql/src/ivm/skip.ts`.
//!
//! `Skip` sets the start position for a pipeline: no rows before its `bound`
//! (a row plus an `exclusive` flag) are output. It is a stateless
//! [`Operator`] — it holds no per-op [`crate::ivm::operator::Storage`],
//! deciding membership purely from the bound and the input schema's
//! comparator, exactly as upstream `Skip` does.
//!
//! Two pieces of real logic, both faithful to `skip.ts`:
//! - **`fetch`** rewrites the incoming [`FetchRequest`]'s `start` so the
//!   underlying input begins at (the later of) the skip bound and the
//!   requested start — `#getStart`. For a `reverse` fetch, results run from
//!   the requested start back toward the bound, stopping at the first row
//!   before it.
//! - **`push`** gates `Add`/`Remove`/`Child` on the bound and splits `Edit`s
//!   whose across-the-bound presence flips (into an `Add`/`Remove`), which is
//!   exactly [`crate::ivm::filter::filter_push`] against the
//!   "should be present past the bound" predicate.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::data::Row;
use crate::ivm::filter::filter_push;
use crate::ivm::operator::{
    Change, FetchRequest, Input, InputBase, Node, Operator, Output, SourceSchema, Start,
    StartBasis, Stream, ThrowOutput,
};

/// The lower bound a [`Skip`] enforces. Port of `skip.ts`'s `Bound`.
#[derive(Debug, Clone, PartialEq)]
pub struct Bound {
    pub row: Row,
    pub exclusive: bool,
}

/// Sets the pipeline's start position; drops everything before `bound`.
pub struct Skip {
    input: Rc<dyn Input>,
    bound: Bound,
    /// The sort PREFIX the bound row actually names. A client's keyset cursor
    /// may omit the appended primary-key tiebreaker; comparing the missing
    /// field as NULL would lexicographically re-admit the boundary row (NULL
    /// sorts first), turning an exclusive bound inclusive. Every bound
    /// comparison uses only this prefix — mirroring the SQL seek in
    /// `query_builder::gather_start_constraints`.
    bound_sort: zero_cache_protocol::ast::Ordering,
    output: RefCell<Rc<dyn Output>>,
}

impl Skip {
    /// Builds a `Skip` over `input`. Mirrors upstream's constructor: asserts
    /// the input is sorted and wires `input.setOutput(this)` so source pushes
    /// flow through the skip.
    pub fn new(input: Rc<dyn Input>, bound: Bound) -> Rc<Self> {
        let schema = input.get_schema();
        assert!(!schema.sort.is_empty(), "Skip requires sorted input");
        let bound_sort: zero_cache_protocol::ast::Ordering = schema
            .sort
            .iter()
            .take_while(|(field, _)| bound.row.iter().any(|(key, _)| key == field))
            .cloned()
            .collect();
        // A bound row naming not even the leading sort field cannot position
        // anything; keep the full-sort comparison (missing = NULL) as before.
        let bound_sort = if bound_sort.is_empty() {
            schema.sort.clone()
        } else {
            bound_sort
        };
        let skip = Rc::new(Skip {
            input,
            bound,
            bound_sort,
            output: RefCell::new(Rc::new(ThrowOutput)),
        });
        skip.input.set_output(skip.clone());
        skip
    }

    /// Orders `row` against the bound row over the bound's sort prefix.
    fn compare_bound_to(&self, row: &Row) -> std::cmp::Ordering {
        crate::ivm::data::make_comparator(&self.bound_sort, false)(&self.bound.row, row)
    }

    /// Port of `#shouldBePresent`: a row is present past the bound if it sorts
    /// after the bound, or equals it and the bound is inclusive.
    fn should_be_present(&self, row: &Row) -> bool {
        let cmp = self.compare_bound_to(row);
        cmp == std::cmp::Ordering::Less
            || (cmp == std::cmp::Ordering::Equal && !self.bound.exclusive)
    }

    /// Port of `#getStart`: resolves the `start` to forward to the input,
    /// combining the skip bound with the requested `start`.
    fn get_start(&self, req: &FetchRequest) -> GetStart {
        let bound_start = Start {
            row: self.bound.row.clone(),
            basis: if self.bound.exclusive {
                StartBasis::After
            } else {
                StartBasis::At
            },
        };

        let Some(req_start) = &req.start else {
            if req.reverse {
                return GetStart::Use(None);
            }
            return GetStart::Use(Some(bound_start));
        };

        let cmp = self.compare_bound_to(&req_start.row);

        if !req.reverse {
            match cmp {
                std::cmp::Ordering::Greater => GetStart::Use(Some(bound_start)),
                std::cmp::Ordering::Equal => {
                    if self.bound.exclusive || req_start.basis == StartBasis::After {
                        GetStart::Use(Some(Start {
                            row: self.bound.row.clone(),
                            basis: StartBasis::After,
                        }))
                    } else {
                        GetStart::Use(Some(bound_start))
                    }
                }
                std::cmp::Ordering::Less => GetStart::Use(Some(req_start.clone())),
            }
        } else {
            // reverse
            match cmp {
                // bound is after the start, but request is reverse so results
                // must be empty.
                std::cmp::Ordering::Greater => GetStart::Empty,
                std::cmp::Ordering::Equal => {
                    if !self.bound.exclusive && req_start.basis == StartBasis::At {
                        GetStart::Use(Some(bound_start))
                    } else {
                        GetStart::Empty
                    }
                }
                // bound is before the start, return start.
                std::cmp::Ordering::Less => GetStart::Use(Some(req_start.clone())),
            }
        }
    }
}

enum GetStart {
    Empty,
    Use(Option<Start>),
}

impl InputBase for Skip {
    fn get_schema(&self) -> SourceSchema {
        self.input.get_schema()
    }
    fn destroy(&self) {
        self.input.destroy();
    }
}

impl Input for Skip {
    fn set_output(&self, output: Rc<dyn Output>) {
        *self.output.borrow_mut() = output;
    }

    fn fetch<'a>(&'a self, req: &FetchRequest) -> Stream<'a, Node> {
        let start = match self.get_start(req) {
            GetStart::Empty => return Box::new(std::iter::empty()),
            GetStart::Use(start) => start,
        };
        let inner = FetchRequest {
            start,
            ..req.clone()
        };
        let nodes = self.input.fetch(&inner);
        if !req.reverse {
            return nodes;
        }
        // For a reverse fetch, walk from the (upper) start back toward the
        // bound, stopping at the first row that sorts before the bound.
        let bound = self.bound.clone();
        let bound_sort = self.bound_sort.clone();
        Box::new(nodes.take_while(move |node| {
            let cmp = crate::ivm::data::make_comparator(&bound_sort, false)(&bound.row, &node.row);
            cmp == std::cmp::Ordering::Less
                || (cmp == std::cmp::Ordering::Equal && !bound.exclusive)
        }))
    }
}

impl Output for Skip {
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        let predicate = |row: &Row| self.should_be_present(row);
        if let Some(change) = filter_push(change, &predicate) {
            self.output.borrow().push(change, self);
        }
    }
}

impl Operator for Skip {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::change::{make_source_change_add, make_source_change_edit};
    use crate::ivm::test_input::{SpyOutput, TestSource};
    use zero_cache_protocol::ast::Direction;
    use zero_cache_shared::bigint_json::JsonValue;

    fn users_source() -> Rc<TestSource> {
        TestSource::new(
            "users",
            vec!["id".into()],
            vec![
                ("startDate".into(), Direction::Asc),
                ("id".into(), Direction::Asc),
            ],
        )
    }

    fn user(id: i64, name: &str, start_date: &str) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("name".into(), JsonValue::String(name.into())),
            ("startDate".into(), JsonValue::String(start_date.into())),
        ]
    }

    fn bound(start_date: &str, id: i64, exclusive: bool) -> Bound {
        Bound {
            row: vec![
                ("startDate".into(), JsonValue::String(start_date.into())),
                ("id".into(), JsonValue::Number(id as f64)),
            ],
            exclusive,
        }
    }

    fn seed() -> Rc<TestSource> {
        let s = users_source();
        s.push_change(make_source_change_add(user(1, "Aaron", "2019-06-18")));
        s.push_change(make_source_change_add(user(2, "Erik", "2020-08-01")));
        s.push_change(make_source_change_add(user(3, "Greg", "2021-12-07")));
        s.push_change(make_source_change_add(user(4, "Cesar", "2022-12-01")));
        s.push_change(make_source_change_add(user(5, "Alex", "2023-04-01")));
        s.push_change(make_source_change_add(user(6, "Darick", "2023-09-01")));
        s.push_change(make_source_change_add(user(7, "Matt", "2024-06-01")));
        s
    }

    fn ids(nodes: Vec<Node>) -> Vec<i64> {
        nodes
            .into_iter()
            .map(|n| match n.row.iter().find(|(k, _)| k == "id").unwrap().1 {
                JsonValue::Number(v) => v as i64,
                _ => panic!("id not a number"),
            })
            .collect()
    }

    fn fetch_ids(skip_bound: Bound, req: FetchRequest) -> Vec<i64> {
        let source = seed();
        let skip = Skip::new(source.clone(), skip_bound);
        ids(skip.fetch(&req).collect())
    }

    fn start(start_date: &str, id: i64, basis: StartBasis) -> Start {
        Start {
            row: vec![
                ("startDate".into(), JsonValue::String(start_date.into())),
                ("id".into(), JsonValue::Number(id as f64)),
            ],
            basis,
        }
    }

    // ---- fetch (ported from skip.test.ts `suite('fetch')`) ----

    #[test]
    fn fetch_c1_inclusive_bound_before_row() {
        assert_eq!(
            fetch_ids(bound("2023-03-31", 5, false), FetchRequest::default()),
            vec![5, 6, 7]
        );
    }

    #[test]
    fn fetch_c3_inclusive_bound_equal_row() {
        assert_eq!(
            fetch_ids(bound("2023-04-01", 5, false), FetchRequest::default()),
            vec![5, 6, 7]
        );
    }

    #[test]
    fn fetch_c4_exclusive_bound_equal_row_drops_it() {
        assert_eq!(
            fetch_ids(bound("2023-04-01", 5, true), FetchRequest::default()),
            vec![6, 7]
        );
    }

    #[test]
    fn fetch_c7_start_before_bound_inclusive() {
        assert_eq!(
            fetch_ids(
                bound("2023-04-01", 5, false),
                FetchRequest {
                    start: Some(start("2023-03-30", 5, StartBasis::At)),
                    ..Default::default()
                }
            ),
            vec![5, 6, 7]
        );
    }

    #[test]
    fn fetch_c11_start_equals_bound_after() {
        assert_eq!(
            fetch_ids(
                bound("2023-04-01", 5, false),
                FetchRequest {
                    start: Some(start("2023-04-01", 5, StartBasis::After)),
                    ..Default::default()
                }
            ),
            vec![6, 7]
        );
    }

    #[test]
    fn fetch_c12_exclusive_bound_start_equals_bound_at() {
        assert_eq!(
            fetch_ids(
                bound("2023-04-01", 5, true),
                FetchRequest {
                    start: Some(start("2023-04-01", 5, StartBasis::At)),
                    ..Default::default()
                }
            ),
            vec![6, 7]
        );
    }

    #[test]
    fn fetch_c13_start_before_bound_returns_from_bound() {
        assert_eq!(
            fetch_ids(
                bound("2023-04-02", 5, false),
                FetchRequest {
                    start: Some(start("2023-04-01", 5, StartBasis::At)),
                    ..Default::default()
                }
            ),
            vec![6, 7]
        );
    }

    #[test]
    fn fetch_c19_start_past_all_rows_is_empty() {
        assert_eq!(
            fetch_ids(
                bound("2023-04-02", 5, true),
                FetchRequest {
                    start: Some(start("2030-04-02", 5, StartBasis::After)),
                    ..Default::default()
                }
            ),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn fetch_constraint_after_bound() {
        assert_eq!(
            fetch_ids(
                bound("2023-04-01", 5, false),
                FetchRequest {
                    constraint: Some(vec![("id".into(), JsonValue::Number(6.0))]),
                    ..Default::default()
                }
            ),
            vec![6]
        );
    }

    #[test]
    fn fetch_constraint_before_bound_is_empty() {
        assert_eq!(
            fetch_ids(
                bound("2023-04-01", 5, false),
                FetchRequest {
                    constraint: Some(vec![("id".into(), JsonValue::Number(3.0))]),
                    ..Default::default()
                }
            ),
            Vec::<i64>::new()
        );
    }

    // ---- reverse fetch ----

    #[test]
    fn fetch_reverse_inclusive_bound_no_start() {
        assert_eq!(
            fetch_ids(
                bound("2023-03-31", 5, false),
                FetchRequest {
                    reverse: true,
                    ..Default::default()
                }
            ),
            vec![7, 6, 5]
        );
    }

    #[test]
    fn fetch_reverse_exclusive_bound_equal_row_drops_it() {
        assert_eq!(
            fetch_ids(
                bound("2023-04-01", 5, true),
                FetchRequest {
                    reverse: true,
                    ..Default::default()
                }
            ),
            vec![7, 6]
        );
    }

    #[test]
    fn fetch_reverse_inclusive_start_before_bound_is_empty() {
        assert_eq!(
            fetch_ids(
                bound("2023-04-01", 5, false),
                FetchRequest {
                    start: Some(start("2023-03-30", 5, StartBasis::At)),
                    reverse: true,
                    ..Default::default()
                }
            ),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn fetch_reverse_inclusive_bound_start_equal_to_row() {
        assert_eq!(
            fetch_ids(
                bound("2023-04-01", 5, false),
                FetchRequest {
                    start: Some(start("2023-04-01", 5, StartBasis::At)),
                    reverse: true,
                    ..Default::default()
                }
            ),
            vec![5]
        );
    }

    #[test]
    fn fetch_reverse_exclusive_bound_exclusive_start_apart() {
        assert_eq!(
            fetch_ids(
                bound("2020-08-01", 2, true),
                FetchRequest {
                    start: Some(start("2023-06-01", 7, StartBasis::After)),
                    reverse: true,
                    ..Default::default()
                }
            ),
            vec![5, 4, 3]
        );
    }

    // ---- push (ported from skip.test.ts `suite('push')`) ----

    fn push_source() -> Rc<TestSource> {
        TestSource::new(
            "users",
            vec!["id".into()],
            vec![
                ("date".into(), Direction::Asc),
                ("id".into(), Direction::Asc),
            ],
        )
    }

    fn urow(id: i64, date: &str) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("date".into(), JsonValue::String(date.into())),
        ]
    }

    fn urow_x(id: i64, date: &str, x: i64) -> Row {
        vec![
            ("id".into(), JsonValue::Number(id as f64)),
            ("date".into(), JsonValue::String(date.into())),
            ("x".into(), JsonValue::Number(x as f64)),
        ]
    }

    fn pbound(id: i64, date: &str, exclusive: bool) -> Bound {
        Bound {
            row: vec![
                ("id".into(), JsonValue::Number(id as f64)),
                ("date".into(), JsonValue::String(date.into())),
            ],
            exclusive,
        }
    }

    fn run_push(skip_bound: Bound, changes: Vec<crate::ivm::change::SourceChange>) -> Vec<Change> {
        let source = push_source();
        let skip = Skip::new(source.clone(), skip_bound);
        let spy = SpyOutput::new();
        skip.set_output(spy.clone());
        for sc in changes {
            let change = source.push_change(sc);
            skip.push(change, &*source);
        }
        let received = spy.received.borrow().clone();
        received
    }

    #[test]
    fn push_c1_add_before_inclusive_bound_dropped() {
        assert_eq!(
            run_push(
                pbound(1, "2014-01-24", false),
                vec![make_source_change_add(urow(1, "2014-01-23"))]
            ),
            vec![]
        );
    }

    #[test]
    fn push_c3_add_at_inclusive_bound_forwarded() {
        assert_eq!(
            run_push(
                pbound(1, "2014-01-24", false),
                vec![make_source_change_add(urow(1, "2014-01-24"))]
            ),
            vec![Change::Add(Node::new(urow(1, "2014-01-24")))]
        );
    }

    #[test]
    fn push_c5_add_after_bound_forwarded() {
        assert_eq!(
            run_push(
                pbound(1, "2014-01-24", false),
                vec![make_source_change_add(urow(1, "2014-01-25"))]
            ),
            vec![Change::Add(Node::new(urow(1, "2014-01-25")))]
        );
    }

    #[test]
    fn push_c9_add_at_exclusive_bound_dropped() {
        assert_eq!(
            run_push(
                pbound(1, "2014-01-24", true),
                vec![make_source_change_add(urow(1, "2014-01-24"))]
            ),
            vec![]
        );
    }

    #[test]
    fn push_c10_add_at_exclusive_bound_different_id_forwarded() {
        assert_eq!(
            run_push(
                pbound(1, "2014-01-24", true),
                vec![make_source_change_add(urow(2, "2014-01-24"))]
            ),
            vec![Change::Add(Node::new(urow(2, "2014-01-24")))]
        );
    }

    #[test]
    fn push_edit_old_and_new_before_bound_dropped() {
        // Both the seeding Add and the Edit sit before the inclusive bound, so
        // nothing is forwarded downstream.
        assert_eq!(
            run_push(
                pbound(1, "2014-01-24", false),
                vec![
                    make_source_change_add(urow(1, "2014-01-22")),
                    make_source_change_edit(urow(1, "2014-01-23"), urow(1, "2014-01-22")),
                ]
            ),
            vec![]
        );
    }

    #[test]
    fn push_edit_old_and_new_at_inclusive_bound_forwarded() {
        assert_eq!(
            run_push(
                pbound(1, "2014-01-24", false),
                vec![
                    make_source_change_add(urow_x(1, "2014-01-24", 1)),
                    make_source_change_edit(urow_x(1, "2014-01-24", 2), urow_x(1, "2014-01-24", 1)),
                ]
            ),
            vec![
                Change::Add(Node::new(urow_x(1, "2014-01-24", 1))),
                Change::Edit {
                    node: Node::new(urow_x(1, "2014-01-24", 2)),
                    old_node: Node::new(urow_x(1, "2014-01-24", 1)),
                },
            ]
        );
    }

    #[test]
    fn push_edit_old_before_new_after_bound_becomes_add() {
        assert_eq!(
            run_push(
                pbound(1, "2014-01-24", true),
                vec![
                    make_source_change_add(urow(1, "2014-01-23")),
                    make_source_change_edit(urow(1, "2014-01-25"), urow(1, "2014-01-23")),
                ]
            ),
            vec![Change::Add(Node::new(urow(1, "2014-01-25")))]
        );
    }

    #[test]
    fn push_edit_old_after_new_before_bound_becomes_add_then_remove() {
        assert_eq!(
            run_push(
                pbound(1, "2014-01-24", true),
                vec![
                    make_source_change_add(urow(1, "2014-01-25")),
                    make_source_change_edit(urow(1, "2014-01-23"), urow(1, "2014-01-25")),
                ]
            ),
            vec![
                Change::Add(Node::new(urow(1, "2014-01-25"))),
                Change::Remove(Node::new(urow(1, "2014-01-25"))),
            ]
        );
    }
}
