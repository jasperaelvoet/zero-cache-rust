//! Persistent client-group query pipelines driven by Zero snapshot diffs.
//!
//! Sources are hydrated once. Commits advance their in-memory state and emit
//! row deltas; they never trigger SQL re-hydration of every desired query.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use zero_cache_protocol::ast::{
    referenced_tables, Ast, Bound, Condition, CorrelatedSubquery, Direction, ExistsOp, Ordering,
};
use zero_cache_shared::bigint_json::{stringify, JsonValue};
use zero_cache_sqlite::snapshotter::{
    SnapshotChange, SnapshotError, SnapshotTableSpec, Snapshotter,
};
use zero_cache_sqlite::Value as SqlValue;
use zero_cache_zql::builder::filter::{create_predicate_with_exists, ExistsFn};
use zero_cache_zql::ivm::change::{make_source_change_add, make_source_change_remove};
use zero_cache_zql::ivm::data::{make_comparator, Row};
use zero_cache_zql::ivm::operator::FetchRequest;
use zero_cache_zql::ivm::table_source::TableSource;

use crate::row_set_signature::row_id_signature_unit;

use zero_cache_types::pg_data_type::ValueType as ColumnValueType;
use zero_cache_types::pg_to_lite::ZERO_VERSION_COLUMN_NAME;

/// Per-column declared ZQL value types for one table, as carried on
/// [`SnapshotTableSpec::column_types`].
type ColumnTypes = BTreeMap<String, ColumnValueType>;

/// Empty type map used where a table's declared types are unavailable; every
/// column then falls back to the generic (string/number) conversion.
fn empty_column_types() -> ColumnTypes {
    BTreeMap::new()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineRowChangeKind {
    Add,
    Remove,
    Edit,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PipelineRowChange {
    pub query_id: String,
    pub table: String,
    pub kind: PipelineRowChangeKind,
    pub row: Row,
    pub old_row: Option<Row>,
    pub row_key: BTreeMap<String, JsonValue>,
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error(transparent)]
    Db(#[from] zero_cache_sqlite::DbError),
    #[error("unknown table {0}")]
    UnknownTable(String),
    #[error("query {0} is already active")]
    DuplicateQuery(String),
    #[error(transparent)]
    RowKey(#[from] zero_cache_types::row_key::RowKeyError),
}

#[derive(Clone)]
struct MaterializedRow {
    table: String,
    row: Row,
    row_key: BTreeMap<String, JsonValue>,
}

struct Pipeline {
    ast: Ast,
    rows: BTreeMap<String, MaterializedRow>,
    referenced_tables: BTreeSet<String>,
}

/// Persistent pipeline owner for one Zero client group.
pub struct PipelineDriver {
    snapshotter: Snapshotter,
    table_specs: BTreeMap<String, SnapshotTableSpec>,
    all_table_names: BTreeSet<String>,
    sources: HashMap<String, TableSource>,
    pipelines: BTreeMap<String, Pipeline>,
    row_set_signatures: BTreeMap<String, u64>,
}

impl PipelineDriver {
    pub fn new(
        db_file: impl Into<String>,
        app_id: impl Into<String>,
        page_cache_size_kib: Option<usize>,
        table_specs: BTreeMap<String, SnapshotTableSpec>,
        all_table_names: BTreeSet<String>,
    ) -> Result<Self, PipelineError> {
        let mut snapshotter = Snapshotter::new(db_file, app_id, page_cache_size_kib);
        snapshotter.init()?;
        let mut driver = Self {
            snapshotter,
            table_specs,
            all_table_names,
            sources: HashMap::new(),
            pipelines: BTreeMap::new(),
            row_set_signatures: BTreeMap::new(),
        };
        driver.hydrate_sources()?;
        Ok(driver)
    }

    fn hydrate_sources(&mut self) -> Result<(), PipelineError> {
        let db = self.snapshotter.current()?.db();
        for spec in self.table_specs.values() {
            let ordering = spec
                .primary_key
                .iter()
                .map(|column| (column.clone(), Direction::Asc))
                .collect();
            let mut source = TableSource::new(&spec.name, spec.primary_key.clone(), ordering);
            let columns = spec
                .columns
                .iter()
                .map(|column| quote(column))
                .collect::<Vec<_>>();
            for row in db.all(
                &format!("SELECT {} FROM {}", columns.join(","), quote(&spec.name)),
                &[],
            )? {
                source.push(make_source_change_add(sql_row_to_zql(
                    row,
                    &spec.column_types,
                    spec.min_row_version.as_deref(),
                )));
            }
            self.sources.insert(spec.name.clone(), source);
        }
        Ok(())
    }

    pub fn version(&self) -> Result<&str, PipelineError> {
        Ok(&self.snapshotter.current()?.version)
    }

    pub fn add_query(
        &mut self,
        query_id: impl Into<String>,
        ast: Ast,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let query_id = query_id.into();
        if self.pipelines.contains_key(&query_id) {
            return Err(PipelineError::DuplicateQuery(query_id));
        }
        let rows = materialize_query(&ast, &self.sources, &self.table_specs)?;
        let changes = additions(&query_id, &rows);
        self.row_set_signatures
            .insert(query_id.clone(), signature_for_rows(rows.values())?);
        self.pipelines.insert(
            query_id,
            Pipeline {
                referenced_tables: referenced_tables(&ast),
                ast,
                rows,
            },
        );
        Ok(changes)
    }

    pub fn remove_query(&mut self, query_id: &str) -> Vec<PipelineRowChange> {
        self.row_set_signatures.remove(query_id);
        self.pipelines
            .remove(query_id)
            .map(|pipeline| {
                pipeline
                    .rows
                    .into_values()
                    .map(|entry| PipelineRowChange {
                        query_id: query_id.to_string(),
                        table: entry.table,
                        kind: PipelineRowChangeKind::Remove,
                        row: entry.row,
                        old_row: None,
                        row_key: entry.row_key,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn advance(&mut self) -> Result<Vec<PipelineRowChange>, PipelineError> {
        let diff = self
            .snapshotter
            .advance(&self.table_specs, &self.all_table_names)?;
        let changed_tables: BTreeSet<_> = diff.rows.iter().map(|row| row.table.clone()).collect();
        for change in &diff.rows {
            apply_snapshot_change(&mut self.sources, change, &self.table_specs)?;
        }

        let ids: Vec<_> = self
            .pipelines
            .iter()
            .filter(|(_, pipeline)| !pipeline.referenced_tables.is_disjoint(&changed_tables))
            .map(|(id, _)| id.clone())
            .collect();
        let mut changes = Vec::new();
        for id in ids {
            let pipeline = self
                .pipelines
                .get_mut(&id)
                .expect("selected pipeline exists");
            if is_direct_incremental_query(&pipeline.ast) {
                changes.extend(apply_direct_changes(
                    &id,
                    pipeline,
                    &diff.rows,
                    &self.table_specs,
                )?);
            } else {
                let next = materialize_query(&pipeline.ast, &self.sources, &self.table_specs)?;
                changes.extend(diff_rows(&id, &pipeline.rows, &next));
                pipeline.rows = next;
            }
        }
        self.apply_signature_changes(&changes)?;
        Ok(changes)
    }

    pub fn row_set_signature(&self, query_id: &str) -> Option<u64> {
        self.row_set_signatures.get(query_id).copied()
    }

    fn apply_signature_changes(
        &mut self,
        changes: &[PipelineRowChange],
    ) -> Result<(), PipelineError> {
        for change in changes {
            if matches!(
                change.kind,
                PipelineRowChangeKind::Add | PipelineRowChangeKind::Remove
            ) {
                let unit = row_id_signature_unit(&zero_cache_types::row_key::RowId::new(
                    "public",
                    change.table.clone(),
                    change
                        .row_key
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                ))?;
                *self
                    .row_set_signatures
                    .entry(change.query_id.clone())
                    .or_default() ^= unit;
            }
        }
        Ok(())
    }

    pub fn destroy(self) -> Result<(), PipelineError> {
        self.snapshotter.destroy()?;
        Ok(())
    }
}

fn apply_snapshot_change(
    sources: &mut HashMap<String, TableSource>,
    change: &SnapshotChange,
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<(), PipelineError> {
    let source = sources
        .get_mut(&change.table)
        .ok_or_else(|| PipelineError::UnknownTable(change.table.clone()))?;
    let fallback = empty_column_types();
    let spec = specs.get(&change.table);
    let column_types = spec.map(|spec| &spec.column_types).unwrap_or(&fallback);
    let min_row_version = spec.and_then(|spec| spec.min_row_version.as_deref());
    for previous in &change.prev_values {
        source.push(make_source_change_remove(sql_row_to_zql(
            previous.clone(),
            column_types,
            min_row_version,
        )));
    }
    if let Some(next) = &change.next_value {
        source.push(make_source_change_add(sql_row_to_zql(
            next.clone(),
            column_types,
            min_row_version,
        )));
    }
    Ok(())
}

fn is_direct_incremental_query(ast: &Ast) -> bool {
    ast.limit.is_none()
        && ast.start.is_none()
        && ast.related.as_ref().is_none_or(Vec::is_empty)
        && ast
            .where_
            .as_ref()
            .is_none_or(|condition| !has_correlated_subquery(condition))
}

fn has_correlated_subquery(condition: &Condition) -> bool {
    match condition {
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(has_correlated_subquery)
        }
        Condition::CorrelatedSubquery { .. } => true,
        _ => false,
    }
}

fn apply_direct_changes(
    query_id: &str,
    pipeline: &mut Pipeline,
    changes: &[SnapshotChange],
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<Vec<PipelineRowChange>, PipelineError> {
    let relevant = changes
        .iter()
        .filter(|change| change.table == pipeline.ast.table);
    let mut previous = BTreeMap::new();
    let mut affected_keys = BTreeSet::new();
    let fallback = empty_column_types();
    let spec = specs.get(&pipeline.ast.table);
    let column_types = spec.map(|spec| &spec.column_types).unwrap_or(&fallback);
    let min_row_version = spec.and_then(|spec| spec.min_row_version.as_deref());

    for change in relevant {
        for row in &change.prev_values {
            let row = sql_row_to_zql(row.clone(), column_types, min_row_version);
            let entry = materialized_row(&change.table, row, specs)?;
            let key = materialized_key(&entry);
            affected_keys.insert(key.clone());
            if let Some(old) = pipeline.rows.remove(&key) {
                previous.entry(key).or_insert(old);
            }
        }

        if let Some(row) = &change.next_value {
            let row = sql_row_to_zql(row.clone(), column_types, min_row_version);
            let entry = materialized_row(&change.table, row, specs)?;
            let key = materialized_key(&entry);
            affected_keys.insert(key.clone());
            if direct_row_matches(&pipeline.ast, &entry.row) {
                pipeline.rows.insert(key, entry);
            }
        }
    }

    let next = affected_keys
        .into_iter()
        .filter_map(|key| pipeline.rows.get(&key).cloned().map(|row| (key, row)))
        .collect();
    Ok(diff_rows(query_id, &previous, &next))
}

fn direct_row_matches(ast: &Ast, row: &Row) -> bool {
    let Some(condition) = &ast.where_ else {
        return true;
    };
    let unreachable_exists: ExistsFn<'_> = std::rc::Rc::new(|_, _| {
        unreachable!("direct incremental queries cannot contain correlated subqueries")
    });
    create_predicate_with_exists(condition, unreachable_exists)(row)
}

fn materialize_query(
    ast: &Ast,
    sources: &HashMap<String, TableSource>,
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<BTreeMap<String, MaterializedRow>, PipelineError> {
    let mut output = BTreeMap::new();
    let roots = matching_rows(ast, None, sources, specs)?;
    for row in &roots {
        insert_row(&ast.table, row.clone(), specs, &mut output)?;
    }
    for relation in ast.related.iter().flatten() {
        materialize_related(&roots, relation, sources, specs, &mut output)?;
    }
    if let Some(condition) = &ast.where_ {
        for relation in correlated_subqueries(condition) {
            materialize_related(&roots, &relation, sources, specs, &mut output)?;
        }
    }
    Ok(output)
}

fn matching_rows(
    ast: &Ast,
    correlation: Option<(&Row, &CorrelatedSubquery)>,
    sources: &HashMap<String, TableSource>,
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<Vec<Row>, PipelineError> {
    let source = sources
        .get(&ast.table)
        .ok_or_else(|| PipelineError::UnknownTable(ast.table.clone()))?;
    let mut rows: Vec<_> = source
        .fetch(&FetchRequest::default())
        .map(|node| node.row)
        .collect();
    if let Some((parent, relation)) = correlation {
        let values: Vec<_> = relation
            .correlation
            .parent_field
            .iter()
            .map(|field| get(parent, field))
            .collect();
        rows.retain(|child| {
            relation
                .correlation
                .child_field
                .iter()
                .zip(values.iter())
                .all(|(field, value)| *value != JsonValue::Null && get(child, field) == *value)
        });
    }
    if let Some(condition) = &ast.where_ {
        let exists: ExistsFn<'_> = std::rc::Rc::new(|relation, parent| {
            matching_rows(&relation.subquery, Some((parent, relation)), sources, specs)
                .map(|rows| !rows.is_empty())
                .unwrap_or(false)
        });
        let predicate = create_predicate_with_exists(condition, exists);
        rows.retain(|row| predicate(row));
    }
    let ordering = completed_ordering(ast, specs)?;
    rows.sort_by(make_comparator(&ordering, false));
    if let Some(start) = &ast.start {
        rows = apply_start(rows, start, &ordering);
    }
    if let Some(limit) = ast.limit {
        rows.truncate(limit.max(0.0) as usize);
    }
    Ok(rows)
}

fn materialize_related(
    parents: &[Row],
    relation: &CorrelatedSubquery,
    sources: &HashMap<String, TableSource>,
    specs: &BTreeMap<String, SnapshotTableSpec>,
    output: &mut BTreeMap<String, MaterializedRow>,
) -> Result<(), PipelineError> {
    for parent in parents {
        let children = matching_rows(&relation.subquery, Some((parent, relation)), sources, specs)?;
        for child in &children {
            insert_row(&relation.subquery.table, child.clone(), specs, output)?;
        }
        for nested in relation.subquery.related.iter().flatten() {
            materialize_related(&children, nested, sources, specs, output)?;
        }
        if let Some(condition) = &relation.subquery.where_ {
            for nested in correlated_subqueries(condition) {
                materialize_related(&children, &nested, sources, specs, output)?;
            }
        }
    }
    Ok(())
}

fn completed_ordering(
    ast: &Ast,
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<Ordering, PipelineError> {
    let spec = specs
        .get(&ast.table)
        .ok_or_else(|| PipelineError::UnknownTable(ast.table.clone()))?;
    let mut ordering = ast.order_by.clone().unwrap_or_default();
    for key in &spec.primary_key {
        if !ordering.iter().any(|(column, _)| column == key) {
            ordering.push((key.clone(), Direction::Asc));
        }
    }
    Ok(ordering)
}

fn apply_start(rows: Vec<Row>, start: &Bound, ordering: &Ordering) -> Vec<Row> {
    let JsonValue::Object(bound) = &start.row else {
        return rows;
    };
    let compare = make_comparator(ordering, false);
    rows.into_iter()
        .filter(|row| {
            let result = compare(row, bound);
            result.is_gt() || (!start.exclusive && result.is_eq())
        })
        .collect()
}

fn correlated_subqueries(condition: &Condition) -> Vec<CorrelatedSubquery> {
    match condition {
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().flat_map(correlated_subqueries).collect()
        }
        Condition::CorrelatedSubquery { related, op, .. } if *op == ExistsOp::Exists => {
            vec![related.clone()]
        }
        _ => Vec::new(),
    }
}

fn insert_row(
    table: &str,
    row: Row,
    specs: &BTreeMap<String, SnapshotTableSpec>,
    output: &mut BTreeMap<String, MaterializedRow>,
) -> Result<(), PipelineError> {
    let entry = materialized_row(table, row, specs)?;
    output.insert(materialized_key(&entry), entry);
    Ok(())
}

fn materialized_row(
    table: &str,
    row: Row,
    specs: &BTreeMap<String, SnapshotTableSpec>,
) -> Result<MaterializedRow, PipelineError> {
    let spec = specs
        .get(table)
        .ok_or_else(|| PipelineError::UnknownTable(table.to_string()))?;
    let values: BTreeMap<_, _> = spec
        .primary_key
        .iter()
        .map(|column| (column.clone(), get(&row, column)))
        .collect();
    Ok(MaterializedRow {
        table: table.to_string(),
        row,
        row_key: values,
    })
}

fn materialized_key(entry: &MaterializedRow) -> String {
    format!(
        "{}:{}",
        entry.table,
        stringify(&JsonValue::Object(
            entry.row_key.clone().into_iter().collect()
        ))
    )
}

fn additions(query_id: &str, rows: &BTreeMap<String, MaterializedRow>) -> Vec<PipelineRowChange> {
    rows.values()
        .map(|entry| PipelineRowChange {
            query_id: query_id.to_string(),
            table: entry.table.clone(),
            kind: PipelineRowChangeKind::Add,
            row: entry.row.clone(),
            old_row: None,
            row_key: entry.row_key.clone(),
        })
        .collect()
}

fn signature_for_rows<'a>(
    rows: impl Iterator<Item = &'a MaterializedRow>,
) -> Result<u64, PipelineError> {
    let mut signature = 0;
    for row in rows {
        signature ^= row_id_signature_unit(&zero_cache_types::row_key::RowId::new(
            "public",
            row.table.clone(),
            row.row_key
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        ))?;
    }
    Ok(signature)
}

fn diff_rows(
    query_id: &str,
    previous: &BTreeMap<String, MaterializedRow>,
    next: &BTreeMap<String, MaterializedRow>,
) -> Vec<PipelineRowChange> {
    let keys: BTreeSet<_> = previous.keys().chain(next.keys()).cloned().collect();
    keys.into_iter()
        .filter_map(|key| match (previous.get(&key), next.get(&key)) {
            (None, Some(row)) => Some(PipelineRowChange {
                query_id: query_id.into(),
                table: row.table.clone(),
                kind: PipelineRowChangeKind::Add,
                row: row.row.clone(),
                old_row: None,
                row_key: row.row_key.clone(),
            }),
            (Some(row), None) => Some(PipelineRowChange {
                query_id: query_id.into(),
                table: row.table.clone(),
                kind: PipelineRowChangeKind::Remove,
                row: row.row.clone(),
                old_row: None,
                row_key: row.row_key.clone(),
            }),
            (Some(old), Some(new)) if old.row != new.row => Some(PipelineRowChange {
                query_id: query_id.into(),
                table: new.table.clone(),
                kind: PipelineRowChangeKind::Edit,
                row: new.row.clone(),
                old_row: Some(old.row.clone()),
                row_key: new.row_key.clone(),
            }),
            _ => None,
        })
        .collect()
}

fn get(row: &Row, field: &str) -> JsonValue {
    row.iter()
        .find(|(column, _)| column == field)
        .map(|(_, value)| value.clone())
        .unwrap_or(JsonValue::Null)
}

/// Converts a raw SQLite replica row into a ZQL row, restoring each column's
/// declared ZQL value type (`fromSQLiteTypes` upstream). Without the type map,
/// booleans stored as SQLite `0`/`1` would ship as numbers and JSON columns as
/// raw text — diverging from official Zero, which emits `true`/`false` and
/// parsed JSON on both the hydration and the incremental (poke) paths.
/// Unknown columns default to a generic (string/number) conversion.
///
/// The row's `_0_version` is clamped up to `min_row_version` (upstream
/// `Streamer.#streamNodes`): a row whose stored version predates a table's
/// minimum (e.g. just after the table was re-added/backfilled) must be emitted
/// at the minimum, or the client's CVR row versions diverge from the reference
/// server. Versions are lexi-encoded, so a plain string comparison suffices.
fn sql_row_to_zql(
    row: Vec<(String, SqlValue)>,
    column_types: &ColumnTypes,
    min_row_version: Option<&str>,
) -> Row {
    row.into_iter()
        .map(|(column, value)| {
            if column == ZERO_VERSION_COLUMN_NAME {
                if let (SqlValue::Text(version), Some(min)) = (&value, min_row_version) {
                    if version.as_str() < min {
                        return (column, JsonValue::String(min.to_string()));
                    }
                }
            }
            let value_type = column_types.get(&column).copied();
            let value = sql_value_to_zql(value, value_type);
            (column, value)
        })
        .collect()
}

fn sql_value_to_zql(value: SqlValue, value_type: Option<ColumnValueType>) -> JsonValue {
    match value_type {
        Some(ColumnValueType::Boolean) => match value {
            SqlValue::Null => JsonValue::Null,
            SqlValue::Integer(value) => JsonValue::Bool(value != 0),
            SqlValue::Real(value) => JsonValue::Bool(value != 0.0),
            SqlValue::Text(value) => JsonValue::Bool(matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "t" | "true"
            )),
            SqlValue::Blob(value) => JsonValue::Bool(!value.is_empty() && value != b"0"),
        },
        Some(ColumnValueType::Json) => match value {
            SqlValue::Null => JsonValue::Null,
            SqlValue::Text(value) => {
                zero_cache_shared::bigint_json::parse(&value).unwrap_or(JsonValue::String(value))
            }
            other => generic_sql_value_to_zql(other),
        },
        _ => generic_sql_value_to_zql(value),
    }
}

fn generic_sql_value_to_zql(value: SqlValue) -> JsonValue {
    match value {
        SqlValue::Null => JsonValue::Null,
        SqlValue::Integer(value) => JsonValue::Number(value as f64),
        SqlValue::Real(value) => JsonValue::Number(value),
        SqlValue::Text(value) => JsonValue::String(value),
        SqlValue::Blob(value) => JsonValue::String(String::from_utf8_lossy(&value).into()),
    }
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zero_cache_protocol::ast::{ColumnReference, LiteralValue, SimpleOperator, ValuePosition};
    use zero_cache_sqlite::change_log::{ChangeLog, CREATE_CHANGELOG_SCHEMA};
    use zero_cache_sqlite::replication_state::{
        init_replication_state, update_replication_watermark,
    };
    use zero_cache_sqlite::StatementRunner;

    fn path() -> String {
        std::env::temp_dir()
            .join(format!(
                "zero-pipeline-{}-{}.db",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .to_string_lossy()
            .into_owned()
    }

    fn query() -> Ast {
        Ast {
            table: "issue".into(),
            where_: Some(Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column(ColumnReference {
                    name: "active".into(),
                }),
                right: ValuePosition::Literal(LiteralValue::Number(1.0)),
            }),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        }
    }

    fn specs() -> BTreeMap<String, SnapshotTableSpec> {
        BTreeMap::from([(
            "issue".into(),
            SnapshotTableSpec {
                name: "issue".into(),
                columns: vec!["id".into(), "active".into(), "_0_version".into()],
                column_types: BTreeMap::new(),
                primary_key: vec!["id".into()],
                unique_keys: vec![],
                min_row_version: Some("00".into()),
            },
        )])
    }

    #[test]
    fn persistent_pipeline_hydrates_once_then_advances_from_snapshot_diff() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (1, 1, '00')", &[])
            .unwrap();

        let mut driver = PipelineDriver::new(
            &path,
            "zero",
            None,
            specs(),
            BTreeSet::from(["issue".into()]),
        )
        .unwrap();
        let initial = driver.add_query("q", query()).unwrap();
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].kind, PipelineRowChangeKind::Add);
        let initial_signature = driver.row_set_signature("q").unwrap();
        assert_ne!(initial_signature, 0);

        writer
            .run("UPDATE issue SET active=0, _0_version='01' WHERE id=1", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "01",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(1.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "01").unwrap();

        let changes = driver.advance().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].kind, PipelineRowChangeKind::Remove);
        assert_eq!(driver.row_set_signature("q"), Some(0));
        assert_eq!(driver.version().unwrap(), "01");

        writer
            .run("UPDATE issue SET active=1, _0_version='02' WHERE id=1", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "02",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(1.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "02").unwrap();
        let changes = driver.advance().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].kind, PipelineRowChangeKind::Add);
        assert_eq!(driver.row_set_signature("q"), Some(initial_signature));

        writer
            .run("UPDATE issue SET _0_version='03' WHERE id=1", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "03",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(1.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "03").unwrap();
        let changes = driver.advance().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].kind, PipelineRowChangeKind::Edit);
        assert_eq!(driver.row_set_signature("q"), Some(initial_signature));
        assert_eq!(driver.version().unwrap(), "03");

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    fn typed_specs() -> BTreeMap<String, SnapshotTableSpec> {
        BTreeMap::from([(
            "issue".into(),
            SnapshotTableSpec {
                name: "issue".into(),
                columns: vec!["id".into(), "active".into(), "_0_version".into()],
                column_types: BTreeMap::from([("active".into(), ColumnValueType::Boolean)]),
                primary_key: vec!["id".into()],
                unique_keys: vec![],
                min_row_version: Some("00".into()),
            },
        )])
    }

    /// A row whose stored `_0_version` predates the table's `minRowVersion`
    /// must be emitted at the minimum, matching upstream `Streamer.#streamNodes`
    /// — otherwise the client's CVR row versions diverge from the reference.
    #[test]
    fn emitted_rows_clamp_version_up_to_min_row_version() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "05", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        // Stored version "01" is below the table's minimum "05".
        writer
            .run("INSERT INTO issue VALUES (1, 1, '01')", &[])
            .unwrap();

        let mut specs = typed_specs();
        specs.get_mut("issue").unwrap().min_row_version = Some("05".into());

        let query = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };
        let mut driver =
            PipelineDriver::new(&path, "zero", None, specs, BTreeSet::from(["issue".into()]))
                .unwrap();
        let initial = driver.add_query("q", query).unwrap();
        assert_eq!(initial.len(), 1);
        assert_eq!(
            get(&initial[0].row, "_0_version"),
            JsonValue::String("05".into()),
            "version below minRowVersion must be clamped up"
        );

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }

    /// Regression for the differential-conformance bug where a boolean column
    /// updated live shipped `1` instead of `true`: the incremental (poke) path
    /// must restore the declared `Boolean` value type just like hydration does.
    #[test]
    fn incremental_advance_restores_boolean_value_type() {
        let path = path();
        let writer = StatementRunner::open_file(&path).unwrap();
        init_replication_state(&writer, &[], "00", &JsonValue::Object(vec![]), true).unwrap();
        writer.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        writer
            .exec("CREATE TABLE issue (id INTEGER PRIMARY KEY, active INTEGER, _0_version TEXT)")
            .unwrap();
        writer
            .run("INSERT INTO issue VALUES (1, 0, '00')", &[])
            .unwrap();

        // Query all rows (no `active` predicate) so the live update produces an
        // Edit carrying the row's `active` value on the wire.
        let query = Ast {
            table: "issue".into(),
            order_by: Some(vec![("id".into(), Direction::Asc)]),
            ..Default::default()
        };

        let mut driver = PipelineDriver::new(
            &path,
            "zero",
            None,
            typed_specs(),
            BTreeSet::from(["issue".into()]),
        )
        .unwrap();
        let initial = driver.add_query("q", query).unwrap();
        assert_eq!(initial.len(), 1);
        // Hydration path already restores the boolean.
        assert_eq!(get(&initial[0].row, "active"), JsonValue::Bool(false));

        writer
            .run("UPDATE issue SET active=1, _0_version='01' WHERE id=1", &[])
            .unwrap();
        ChangeLog::new(&writer)
            .log_set_op(
                "01",
                0,
                "issue",
                &vec![("id".into(), JsonValue::Number(1.0))],
                None,
            )
            .unwrap();
        update_replication_watermark(&writer, "01").unwrap();

        let changes = driver.advance().unwrap();
        assert_eq!(changes.len(), 1);
        // The incremental path must ship `true`, not `1`.
        assert_eq!(get(&changes[0].row, "active"), JsonValue::Bool(true));

        driver.destroy().unwrap();
        drop(writer);
        let _ = std::fs::remove_file(path);
    }
}
