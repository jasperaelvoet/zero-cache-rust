//! A real, SQLite-backed `Source` — port of `zqlite/src/table-source.ts`'s
//! `fetch` path, closing the "current `TableSource` is in-memory" gap
//! flagged across several prior rounds. Unlike
//! `zero_cache_zql::ivm::table_source::TableSource` (an in-memory `Vec`
//! that only knows about rows explicitly pushed into it during a session),
//! this reads directly from a real SQLite replica via `StatementRunner` —
//! so a query can see rows that arrived via initial sync or any prior
//! replication session, not just ones pushed in the current process.
//!
//! Scope deviation, deliberate: upstream's real `#fetch` is a generator
//! that interleaves committed SQLite rows with an in-memory "overlay" of
//! not-yet-committed pushes from the current transaction
//! (`generateWithOverlay`/`generateWithOverlayUnordered`), so a query
//! mid-transaction sees its own pending writes. This port only reads
//! committed state — no overlay — since there is no equivalent to
//! `zero-sqlite3`'s `BEGIN CONCURRENT` transaction-local pending-write
//! tracking here yet. For the whole-pipeline-slice use case (replicating
//! already-committed Postgres transactions into SQLite one at a time via
//! `ChangeDispatcher`, then querying), rows are always fully committed
//! before a query needs to see them, so this gap doesn't block that path;
//! it would block seeing a query's effect on its own just-pushed,
//! not-yet-committed row within one transaction. Also NOT ported:
//! `multiConstraints` batching (join-only, `FetchRequest` doesn't carry
//! one yet) and per-column PG-type-aware value coercion (`fromSQLiteTypes`)
//! — values are mapped generically by SQLite storage class (`INTEGER`/
//! `REAL` -> `Number`, `TEXT` -> `String`, `NULL` -> `Null`, `BLOB` ->
//! lossy UTF-8 `String`) rather than per-column Postgres-type-aware
//! decoding.
//!
//! `fetch` now pushes constraint/ordering/`req.start` cursor pagination
//! down into real SQL via `query_builder::build_select_query`, instead of
//! reading every row and filtering/sorting in Rust memory — the wiring
//! `query_builder.rs`'s module doc flagged as its natural follow-up.

use zero_cache_protocol::ast::{Condition, Ordering};
use zero_cache_protocol::client_schema::ValueType;
use zero_cache_zql::ivm::constraint::PrimaryKey;
use zero_cache_zql::ivm::data::{Row, Value};
use zero_cache_zql::ivm::operator::{FetchRequest, Node, SourceSchema};

use crate::query_builder::{self, ColumnType};
use crate::{DbError, StatementRunner};
use rusqlite::types::Value as SqliteValue;
use std::collections::BTreeMap;

/// A real SQLite-backed source for one table. Port of the read (`fetch`)
/// half of `zqlite::TableSource` — see module doc for scope.
pub struct SqliteTableSource<'a> {
    db: &'a StatementRunner,
    schema: SourceSchema,
    columns: Vec<String>,
    column_types: BTreeMap<String, ColumnType>,
}

fn sqlite_to_value(v: &SqliteValue) -> Value {
    match v {
        SqliteValue::Null => Value::Null,
        SqliteValue::Integer(i) => Value::Number(*i as f64),
        SqliteValue::Real(r) => Value::Number(*r),
        SqliteValue::Text(s) => Value::String(s.clone()),
        SqliteValue::Blob(b) => Value::String(String::from_utf8_lossy(b).into_owned()),
    }
}

/// Builds a generic, all-non-optional `ValueType::Json`-free column-type
/// map from a plain column list — every column typed `String`/non-optional.
/// This is what every existing caller of this port needed until now (no
/// schema-declared types were threaded through); real callers that DO know
/// per-column types/optionality should build their own map and use
/// [`SqliteTableSource::with_column_types`] instead.
fn generic_column_types(columns: &[String]) -> BTreeMap<String, ColumnType> {
    columns
        .iter()
        .map(|c| {
            (
                c.clone(),
                ColumnType {
                    value_type: ValueType::String,
                    optional: true,
                },
            )
        })
        .collect()
}

impl<'a> SqliteTableSource<'a> {
    pub fn new(
        db: &'a StatementRunner,
        table_name: impl Into<String>,
        primary_key: PrimaryKey,
        sort: Ordering,
        columns: Vec<String>,
    ) -> Self {
        let column_types = generic_column_types(&columns);
        SqliteTableSource {
            db,
            schema: SourceSchema {
                table_name: table_name.into(),
                primary_key,
                sort,
            },
            columns,
            column_types,
        }
    }

    /// Like [`Self::new`], but with real per-column types/optionality —
    /// needed for [`query_builder`]'s type-directed value serialization
    /// (e.g. booleans coerced to `0`/`1`, JSON columns stringified) and
    /// nullable-aware cursor-pagination comparisons to behave correctly.
    pub fn with_column_types(
        db: &'a StatementRunner,
        table_name: impl Into<String>,
        primary_key: PrimaryKey,
        sort: Ordering,
        columns: Vec<String>,
        column_types: BTreeMap<String, ColumnType>,
    ) -> Self {
        SqliteTableSource {
            db,
            schema: SourceSchema {
                table_name: table_name.into(),
                primary_key,
                sort,
            },
            columns,
            column_types,
        }
    }

    pub fn schema(&self) -> &SourceSchema {
        &self.schema
    }

    /// Reads matching rows directly from SQLite. Port of `#fetch`'s
    /// committed-rows path (see module doc for the overlay scope
    /// deviation). Constraint/ordering/`req.start` cursor pagination are
    /// pushed down into real SQL via `query_builder::build_select_query`
    /// rather than filtered/sorted in Rust memory. Equivalent to
    /// [`Self::fetch_filtered`] with no `Condition` filter — see that
    /// method's doc for why a caller with an actual query `WHERE` (e.g. an
    /// AST's `where_`) should call it directly instead.
    pub fn fetch(&self, req: &FetchRequest) -> Result<Vec<Node>, DbError> {
        self.fetch_filtered(req, None)
    }

    /// Like [`Self::fetch`], but also pushes an arbitrary `Condition` (e.g. a
    /// client query's AST `where_`) down into the SQL `WHERE` clause via
    /// `query_builder::build_select_query`'s `filters` parameter — which
    /// `fetch` never wired through, so no caller could push a real query
    /// filter into SQL through this type before this method existed. This is
    /// the missing link between the already-ported AST-to-SQL condition
    /// compiler (`query_builder::filters_to_sql`, used elsewhere for
    /// mutation/join constraint building) and an actual replica read: a
    /// client's real query predicate can now be evaluated by SQLite itself
    /// rather than only by the in-memory `Filter` (which has no SQL, and thus
    /// can only ever be a full-table pass-through when the source is SQLite).
    pub fn fetch_filtered(
        &self,
        req: &FetchRequest,
        filters: Option<&Condition>,
    ) -> Result<Vec<Node>, DbError> {
        let order = if self.schema.sort.is_empty() {
            None
        } else {
            Some(&self.schema.sort)
        };
        let frag = query_builder::build_select_query(
            &self.schema.table_name,
            &self.columns,
            &self.column_types,
            req.constraint.as_ref(),
            filters,
            order,
            req.reverse,
            req.start.as_ref(),
            &[],
        )
        .map_err(|e| DbError(e.to_string()))?;

        let rows = self.db.query_uncached(&frag.text, &frag.params)?;
        let result: Vec<Row> = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|(col, v)| (col, sqlite_to_value(&v)))
                    .collect()
            })
            .collect();

        Ok(result.into_iter().map(|row| Node::new(row)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::Direction;

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT, active INTEGER)")
            .unwrap();
        db.exec("INSERT INTO issues (id, title, active) VALUES ('1', 'a', 1), ('2', 'b', 0), ('3', 'c', 1)").unwrap();
        db
    }

    fn source(db: &StatementRunner) -> SqliteTableSource<'_> {
        SqliteTableSource::new(
            db,
            "issues",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
            vec!["id".into(), "title".into(), "active".into()],
        )
    }

    #[test]
    fn fetch_all_rows_sorted() {
        let db = setup();
        let s = source(&db);
        let nodes = s.fetch(&FetchRequest::default()).unwrap();
        let ids: Vec<String> = nodes
            .iter()
            .map(|n| match &n.row[0].1 {
                Value::String(s) => s.clone(),
                other => panic!("expected string id, got {other:?}"),
            })
            .collect();
        assert_eq!(ids, vec!["1", "2", "3"]);
    }

    #[test]
    fn fetch_reverse_order() {
        let db = setup();
        let s = source(&db);
        let req = FetchRequest {
            reverse: true,
            ..Default::default()
        };
        let nodes = s.fetch(&req).unwrap();
        let ids: Vec<String> = nodes
            .iter()
            .map(|n| match &n.row[0].1 {
                Value::String(s) => s.clone(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(ids, vec!["3", "2", "1"]);
    }

    #[test]
    fn fetch_applies_constraint() {
        let db = setup();
        let s = source(&db);
        let req = FetchRequest {
            constraint: Some(vec![("active".into(), Value::Number(1.0))]),
            ..Default::default()
        };
        let nodes = s.fetch(&req).unwrap();
        assert_eq!(nodes.len(), 2);
    }

    /// Proves `fetch_filtered` pushes a real AST `Condition` (a client
    /// query's `where_`) down into the SQL `WHERE` clause — the gap `fetch`
    /// always hardcoded `filters: None` around. `title != 'b'` here mirrors
    /// what an AST-to-SQL query compiler would hand this method from a real
    /// client query.
    #[test]
    fn fetch_filtered_pushes_the_condition_into_sql() {
        use zero_cache_protocol::ast::{
            ColumnReference, Condition, LiteralValue, SimpleOperator, ValuePosition,
        };

        let db = setup();
        let s = source(&db);
        let condition = Condition::Simple {
            op: SimpleOperator::Ne,
            left: ValuePosition::Column(ColumnReference {
                name: "title".into(),
            }),
            right: ValuePosition::Literal(LiteralValue::String("b".into())),
        };
        let nodes = s
            .fetch_filtered(&FetchRequest::default(), Some(&condition))
            .unwrap();
        let ids: Vec<String> = nodes
            .iter()
            .map(|n| match &n.row[0].1 {
                Value::String(s) => s.clone(),
                other => panic!("{other:?}"),
            })
            .collect();
        // Row '2' (title 'b') is excluded by real SQL filtering, not
        // in-memory post-filtering.
        assert_eq!(ids, vec!["1", "3"]);
    }

    /// `fetch_filtered` composes with an existing structural `constraint` —
    /// both narrow the result together.
    #[test]
    fn fetch_filtered_composes_with_a_constraint() {
        use zero_cache_protocol::ast::{
            ColumnReference, Condition, LiteralValue, SimpleOperator, ValuePosition,
        };

        let db = setup();
        let s = source(&db);
        let condition = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column(ColumnReference {
                name: "active".into(),
            }),
            right: ValuePosition::Literal(LiteralValue::Number(1.0)),
        };
        let req = FetchRequest {
            constraint: Some(vec![("id".into(), Value::String("3".into()))]),
            ..Default::default()
        };
        let nodes = s.fetch_filtered(&req, Some(&condition)).unwrap();
        assert_eq!(nodes.len(), 1, "only id=3, which is also active=1");
    }

    #[test]
    fn fetch_maps_null_and_types() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE t (id TEXT PRIMARY KEY, n REAL, s TEXT)")
            .unwrap();
        db.exec("INSERT INTO t (id, n, s) VALUES ('1', 1.5, NULL)")
            .unwrap();
        let s = SqliteTableSource::new(
            &db,
            "t",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
            vec!["id".into(), "n".into(), "s".into()],
        );
        let nodes = s.fetch(&FetchRequest::default()).unwrap();
        assert_eq!(nodes[0].row[1].1, Value::Number(1.5));
        assert_eq!(nodes[0].row[2].1, Value::Null);
    }

    /// Live proof this reads rows the in-memory `TableSource` never could:
    /// data inserted through a completely separate code path (a raw SQL
    /// INSERT, standing in for a real `ChangeDispatcher`-applied
    /// replication write), with no `push` call on this source at all.
    #[test]
    fn sees_rows_never_pushed_through_it() {
        let db = setup();
        db.exec("INSERT INTO issues (id, title, active) VALUES ('4', 'd', 1)")
            .unwrap();
        let s = source(&db);
        let nodes = s.fetch(&FetchRequest::default()).unwrap();
        assert_eq!(
            nodes.len(),
            4,
            "should see the row inserted independently of this Source object"
        );
    }

    /// Proves `req.start` cursor pagination is now pushed down into real
    /// SQL (via `query_builder::gather_start_constraints`), not just
    /// filtering/sorting done in Rust after fetching everything.
    #[test]
    fn fetch_resumes_from_a_start_cursor() {
        let db = setup();
        let s = source(&db);
        let start = zero_cache_zql::ivm::operator::Start {
            row: vec![("id".into(), Value::String("1".into()))],
            basis: zero_cache_zql::ivm::operator::StartBasis::After,
        };
        let req = FetchRequest {
            start: Some(start),
            ..Default::default()
        };
        let nodes = s.fetch(&req).unwrap();
        let ids: Vec<String> = nodes
            .iter()
            .map(|n| match &n.row[0].1 {
                Value::String(s) => s.clone(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(ids, vec!["2", "3"], "must resume strictly after id '1'");
    }

    /// Proves `with_column_types` actually threads real column typing
    /// through to `query_builder` (booleans coerced to 0/1 for the
    /// constraint's bound parameter, not left as a raw JSON-ish value).
    #[test]
    fn fetch_with_real_column_types_coerces_booleans_in_constraints() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec("CREATE TABLE t (id TEXT PRIMARY KEY, active INTEGER)")
            .unwrap();
        db.exec("INSERT INTO t (id, active) VALUES ('1', 1), ('2', 0)")
            .unwrap();
        let mut column_types = std::collections::BTreeMap::new();
        column_types.insert(
            "id".to_string(),
            ColumnType {
                value_type: ValueType::String,
                optional: false,
            },
        );
        column_types.insert(
            "active".to_string(),
            ColumnType {
                value_type: ValueType::Boolean,
                optional: false,
            },
        );
        let s = SqliteTableSource::with_column_types(
            &db,
            "t",
            vec!["id".into()],
            vec![("id".into(), Direction::Asc)],
            vec!["id".into(), "active".into()],
            column_types,
        );
        let req = FetchRequest {
            constraint: Some(vec![("active".into(), Value::Bool(true))]),
            ..Default::default()
        };
        let nodes = s.fetch(&req).unwrap();
        assert_eq!(nodes.len(), 1);
    }
}
