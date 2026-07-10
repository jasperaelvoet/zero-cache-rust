//! Port of the DDL-handling methods of `ChangeProcessor` in
//! `zero-cache/src/services/replicator/change-processor.ts`:
//! `processCreateTable`, `processRenameTable`, `processDropColumn`,
//! `processDropTable`, `processCreateIndex`, `processDropIndex`.
//!
//! Schema changes that introduce data being backfilled must remain invisible
//! to consumers until the corresponding `backfill-completed` message arrives.
//! The dispatcher chooses that visibility policy and calls the `*_with_visibility`
//! variants below; this module performs the SQLite/metadata mutations atomically
//! with the surrounding replication transaction.

use zero_cache_types::pg_to_lite::map_postgres_to_lite_column;
use zero_cache_types::specs::{ColumnSpec, IndexSpec};
use zero_cache_types::sql::id;

use crate::change_log::ChangeLog;
use crate::column_metadata::ColumnMetadataStore;
use crate::create::{create_lite_index_statement, create_lite_table_statement, lite_column_def};
use crate::lite_tables::list_indexes;
use crate::table_metadata::TableMetadataTracker;
use crate::{DbError, StatementRunner};

/// Errors from applying a DDL change: either a database error, or an
/// unsupported column default that must trigger backfill (surfaced from
/// `mapPostgresToLiteColumn`'s allowlist).
#[derive(Debug, thiserror::Error)]
pub enum DdlError {
    #[error(transparent)]
    Db(#[from] DbError),
    #[error(transparent)]
    UnsupportedDefault(#[from] zero_cache_types::pg_to_lite::UnsupportedColumnDefaultError),
}

/// Applies schema (DDL) changes to the SQLite replica, updates the table
/// metadata store, and records reset-ops in the change log. Port of the DDL
/// portion of `ChangeProcessor`.
pub struct DdlApplier<'a> {
    pub db: &'a StatementRunner,
    pub change_log: ChangeLog<'a>,
    pub table_metadata: TableMetadataTracker<'a>,
    pub column_metadata: ColumnMetadataStore<'a>,
}

impl<'a> DdlApplier<'a> {
    pub fn new(db: &'a StatementRunner) -> Self {
        DdlApplier {
            db,
            change_log: ChangeLog::new(db),
            table_metadata: TableMetadataTracker::new(db),
            column_metadata: ColumnMetadataStore::new(db),
        }
    }

    /// Adds a column to a table and makes it immediately visible (bumps the
    /// table's version). This is the ordinary, non-backfill path.
    pub fn add_column(
        &self,
        table_lite_name: &str,
        column_name: &str,
        pg_spec: &ColumnSpec,
        table_schema: &str,
        table_name: &str,
        version: &str,
    ) -> Result<(), DdlError> {
        self.add_column_with_visibility(
            table_lite_name,
            column_name,
            pg_spec,
            table_schema,
            table_name,
            version,
            None,
            true,
        )
    }

    /// Adds a column and records its optional upstream backfill id. When
    /// `make_visible` is false, no minimum-version bump or reset-op is emitted:
    /// the new column remains hidden until `backfill-completed` exposes it.
    /// Port of `processAddColumn`'s backfill-aware branch.
    #[allow(clippy::too_many_arguments)]
    pub fn add_column_with_visibility(
        &self,
        table_lite_name: &str,
        column_name: &str,
        pg_spec: &ColumnSpec,
        table_schema: &str,
        table_name: &str,
        version: &str,
        backfill: Option<&zero_cache_shared::bigint_json::JsonValue>,
        make_visible: bool,
    ) -> Result<(), DdlError> {
        let lite_spec = map_postgres_to_lite_column(table_lite_name, column_name, pg_spec, false)?;
        self.db.exec(&format!(
            "ALTER TABLE {} ADD {} {}",
            id(table_lite_name),
            id(column_name),
            lite_column_def(&lite_spec)
        ))?;

        self.column_metadata
            .insert(table_lite_name, column_name, pg_spec, backfill)?;
        if make_visible {
            self.bump_versions(table_lite_name, table_schema, table_name, version)?;
        }
        Ok(())
    }

    /// Renames and/or retypes a column. If only the name changed, a plain
    /// `ALTER TABLE ... RENAME` suffices. If the (lite) data type changed, a
    /// new column is added, values are copied over, the old column is
    /// dropped, and any indexes referencing the old column are recreated
    /// against the new one (SQLite's `ALTER TABLE` cannot change a column's
    /// type in place). No-ops if neither the name nor the lite data type
    /// changed (matching upstream: default-only changes don't affect
    /// existing replicated rows). Port of `processUpdateColumn`.
    pub fn update_column(
        &self,
        table_lite_name: &str,
        old_column_name: &str,
        new_column_name: &str,
        old_pg_spec: &ColumnSpec,
        new_pg_spec: &ColumnSpec,
        table_schema: &str,
        table_name: &str,
        version: &str,
    ) -> Result<(), DdlError> {
        let old_spec =
            map_postgres_to_lite_column(table_lite_name, old_column_name, old_pg_spec, true)?;
        let new_spec =
            map_postgres_to_lite_column(table_lite_name, new_column_name, new_pg_spec, true)?;

        if old_column_name == new_column_name && old_spec.data_type == new_spec.data_type {
            return Ok(());
        }

        let mut current_old_name = old_column_name.to_string();

        if old_spec.data_type != new_spec.data_type {
            let indexes: Vec<_> = list_indexes(self.db)?
                .into_iter()
                .filter(|idx| {
                    idx.table_name == table_lite_name
                        && idx.columns.iter().any(|(c, _)| c == old_column_name)
                })
                .collect();

            for idx in &indexes {
                self.db
                    .exec(&format!("DROP INDEX IF EXISTS {}", id(&idx.name)))?;
            }

            let tmp_name = format!("tmp.{new_column_name}");
            self.db.exec(&format!(
                "ALTER TABLE {} ADD {} {};
                 UPDATE {} SET {} = {};
                 ALTER TABLE {} DROP {};",
                id(table_lite_name),
                id(&tmp_name),
                lite_column_def(&new_spec),
                id(table_lite_name),
                id(&tmp_name),
                id(&current_old_name),
                id(table_lite_name),
                id(&current_old_name),
            ))?;

            for idx in indexes {
                // Re-create the index against the new (temp-named) column.
                let mut renamed = idx.clone();
                for (col, _) in renamed.columns.iter_mut() {
                    if col == old_column_name {
                        *col = tmp_name.clone();
                    }
                }
                self.db.exec(&create_lite_index_statement(&renamed))?;
            }
            current_old_name = tmp_name;
        }

        if current_old_name != new_column_name {
            self.db.exec(&format!(
                "ALTER TABLE {} RENAME {} TO {}",
                id(table_lite_name),
                id(&current_old_name),
                id(new_column_name)
            ))?;
        }

        self.column_metadata.update(
            table_lite_name,
            old_column_name,
            new_column_name,
            new_pg_spec,
        )?;
        self.bump_versions(table_lite_name, table_schema, table_name, version)?;
        Ok(())
    }

    /// Creates a table from its lite spec (already mapped via
    /// `mapPostgresToLite`) and logs a reset-op to make it visible. This is the
    /// ordinary, non-backfill path.
    pub fn create_table(
        &self,
        table: &zero_cache_types::specs::LiteTableSpec,
        version: &str,
    ) -> Result<(), DbError> {
        self.create_table_with_visibility(table, version, true)
    }

    /// Creates a table, optionally deferring its visibility while every source
    /// column is being backfilled. The dispatcher records column metadata and
    /// chooses `make_visible` from the change-source backfill ids.
    pub fn create_table_with_visibility(
        &self,
        table: &zero_cache_types::specs::LiteTableSpec,
        version: &str,
        make_visible: bool,
    ) -> Result<(), DbError> {
        self.db.exec(&create_lite_table_statement(table))?;
        if make_visible {
            self.change_log.log_reset_op(version, &table.name)?;
        }
        Ok(())
    }

    /// Renames a table (both the replica table and its metadata tracking
    /// entries), bumps the old name's version, and logs a reset. Port of
    /// `processRenameTable`.
    pub fn rename_table(
        &self,
        old_schema: &str,
        old_name: &str,
        new_schema: &str,
        new_name: &str,
        old_lite_name: &str,
        new_lite_name: &str,
        version: &str,
    ) -> Result<(), DbError> {
        self.table_metadata
            .rename(old_schema, old_name, new_schema, new_name)?;
        self.db.exec(&format!(
            "ALTER TABLE {} RENAME TO {}",
            id(old_lite_name),
            id(new_lite_name)
        ))?;
        self.column_metadata
            .rename_table(old_lite_name, new_lite_name)?;
        self.table_metadata
            .set_min_row_version(new_schema, new_name, version)?;
        self.change_log.log_reset_op(version, old_lite_name)?;
        Ok(())
    }

    /// Drops a column, updates metadata, and bumps the table's version. Port
    /// of `processDropColumn`.
    pub fn drop_column(
        &self,
        table_lite_name: &str,
        column: &str,
        table_schema: &str,
        table_name: &str,
        version: &str,
    ) -> Result<(), DbError> {
        self.db.exec(&format!(
            "ALTER TABLE {} DROP {}",
            id(table_lite_name),
            id(column)
        ))?;
        self.column_metadata
            .delete_column(table_lite_name, column)?;
        self.bump_versions(table_lite_name, table_schema, table_name, version)?;
        Ok(())
    }

    /// Drops a table and its metadata. Port of `processDropTable`.
    pub fn drop_table(
        &self,
        schema: &str,
        name: &str,
        lite_name: &str,
        version: &str,
    ) -> Result<(), DbError> {
        self.table_metadata.drop(schema, name)?;
        self.db
            .exec(&format!("DROP TABLE IF EXISTS {}", id(lite_name)))?;
        self.column_metadata.delete_table(lite_name)?;
        self.change_log.log_reset_op(version, lite_name)?;
        Ok(())
    }

    /// Creates an index and logs a reset-op (index changes can affect table
    /// visibility, since sync-ability is gated on a unique index existing).
    pub fn create_index(&self, index: &IndexSpec, version: &str) -> Result<(), DbError> {
        self.create_index_with_visibility(index, version, true)
    }

    /// Creates an index, optionally deferring its reset while the table is
    /// wholly backfilling and therefore still hidden. Port of
    /// `processCreateIndex`'s backfill-aware branch.
    pub fn create_index_with_visibility(
        &self,
        index: &IndexSpec,
        version: &str,
        make_visible: bool,
    ) -> Result<(), DbError> {
        let lite_index = lite_index_from_pg(index);
        self.db.exec(&create_lite_index_statement(&lite_index))?;
        if make_visible {
            self.change_log
                .log_reset_op(version, &lite_index.table_name)?;
        }
        Ok(())
    }

    /// Drops an index. Port of `processDropIndex`. No change-log entry: an
    /// index drop alone doesn't affect row visibility the way its creation
    /// might.
    pub fn drop_index(&self, name: &str) -> Result<(), DbError> {
        self.db
            .exec(&format!("DROP INDEX IF EXISTS {}", id(name)))?;
        Ok(())
    }

    /// Makes a table schema visible at `version`: update its minimum row
    /// version and reset dependent consumers. Shared by ordinary DDL and the
    /// completion side of a backfill.
    pub fn bump_versions(
        &self,
        table_lite_name: &str,
        table_schema: &str,
        table_name: &str,
        version: &str,
    ) -> Result<(), DbError> {
        self.table_metadata
            .set_min_row_version(table_schema, table_name, version)?;
        self.change_log.log_reset_op(version, table_lite_name)?;
        Ok(())
    }
}

/// Maps a Postgres [`IndexSpec`] to its lite equivalent for DDL purposes.
/// Delegates to `map_postgres_to_lite_index` so the index's `table_name` is
/// lite-mapped through `lite_table_name` (i.e. a non-`public`-schema table
/// becomes `"<schema>.<table>"`). Copying `table_name` verbatim here was a bug:
/// a `CREATE INDEX ... ON <table>` for a table in a non-public schema (e.g. the
/// shard's internal `<app>.permissions` metadata table) referenced the
/// unqualified name and failed with "no such table".
fn lite_index_from_pg(index: &IndexSpec) -> zero_cache_types::specs::LiteIndexSpec {
    zero_cache_types::pg_to_lite::map_postgres_to_lite_index(index)
}

/// Convenience: builds an [`IndexSpec`] for tests without every field.
#[cfg(test)]
fn test_index(
    name: &str,
    table_name: &str,
    unique: bool,
    columns: &[(&str, zero_cache_types::specs::Direction)],
) -> IndexSpec {
    IndexSpec {
        name: name.into(),
        table_name: table_name.into(),
        schema: "public".into(),
        unique,
        columns: columns.iter().map(|(c, d)| (c.to_string(), *d)).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_log::CREATE_CHANGELOG_SCHEMA;
    use crate::table_metadata::CREATE_TABLE_METADATA_TABLE;
    use zero_cache_types::specs::{ColumnSpec, Direction, LiteTableSpec};

    fn setup() -> StatementRunner {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(CREATE_CHANGELOG_SCHEMA).unwrap();
        db.exec(CREATE_TABLE_METADATA_TABLE).unwrap();
        db.exec(crate::column_metadata::CREATE_COLUMN_METADATA_TABLE)
            .unwrap();
        db
    }

    fn issues_spec() -> LiteTableSpec {
        LiteTableSpec {
            name: "issues".into(),
            columns: vec![
                ("id".into(), ColumnSpec::new("text", 1)),
                ("title".into(), ColumnSpec::new("text", 2)),
            ],
            primary_key: Some(vec!["id".into()]),
        }
    }

    #[test]
    fn create_table_creates_and_logs_reset() {
        let db = setup();
        let applier = DdlApplier::new(&db);
        applier.create_table(&issues_spec(), "01").unwrap();

        let tables = db
            .query_uncached(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='issues'",
                &[],
            )
            .unwrap();
        assert_eq!(tables.len(), 1);

        // Table-wide ops (pos=-1, rowKey=version) aren't row-keyed, so verify
        // via a raw query rather than get_latest_row_op.
        let rows = db
            .query_uncached(
                "SELECT op FROM \"_zero.changeLog2\" WHERE \"table\" = 'issues'",
                &[],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn drop_table_removes_table_and_metadata() {
        let db = setup();
        let applier = DdlApplier::new(&db);
        applier.create_table(&issues_spec(), "01").unwrap();
        applier
            .table_metadata
            .set_upstream_metadata(
                "public",
                "issues",
                &zero_cache_shared::bigint_json::JsonValue::Null,
            )
            .unwrap();

        applier
            .drop_table("public", "issues", "issues", "02")
            .unwrap();

        let tables = db
            .query_uncached(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='issues'",
                &[],
            )
            .unwrap();
        assert!(tables.is_empty());
        assert!(applier
            .table_metadata
            .get_min_row_versions()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn rename_table_renames_and_updates_metadata() {
        let db = setup();
        let applier = DdlApplier::new(&db);
        applier.create_table(&issues_spec(), "01").unwrap();
        applier
            .table_metadata
            .set_min_row_version("public", "issues", "01")
            .unwrap();

        applier
            .rename_table(
                "public", "issues", "public", "tickets", "issues", "tickets", "02",
            )
            .unwrap();

        let old = db
            .query_uncached(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='issues'",
                &[],
            )
            .unwrap();
        assert!(old.is_empty());
        let new = db
            .query_uncached(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='tickets'",
                &[],
            )
            .unwrap();
        assert_eq!(new.len(), 1);
        assert_eq!(
            applier
                .table_metadata
                .get_min_row_versions()
                .unwrap()
                .get("tickets"),
            Some(&"02".to_string())
        );
    }

    #[test]
    fn drop_column_removes_column() {
        let db = setup();
        let applier = DdlApplier::new(&db);
        applier.create_table(&issues_spec(), "01").unwrap();
        applier
            .drop_column("issues", "title", "public", "issues", "02")
            .unwrap();

        let cols = db
            .query_uncached("SELECT name FROM pragma_table_info('issues')", &[])
            .unwrap();
        let names: Vec<String> = cols
            .into_iter()
            .map(|r| match &r[0].1 {
                crate::Value::Text(s) => s.clone(),
                _ => String::new(),
            })
            .collect();
        assert!(!names.contains(&"title".to_string()));
        assert!(names.contains(&"id".to_string()));
    }

    #[test]
    fn create_and_drop_index() {
        let db = setup();
        let applier = DdlApplier::new(&db);
        applier.create_table(&issues_spec(), "01").unwrap();

        let idx = test_index(
            "issues_title_idx",
            "issues",
            false,
            &[("title", Direction::Asc)],
        );
        applier.create_index(&idx, "02").unwrap();
        let idxs = db
            .query_uncached(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='issues_title_idx'",
                &[],
            )
            .unwrap();
        assert_eq!(idxs.len(), 1);

        applier.drop_index("issues_title_idx").unwrap();
        let idxs2 = db
            .query_uncached(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='issues_title_idx'",
                &[],
            )
            .unwrap();
        assert!(idxs2.is_empty());
    }

    #[test]
    fn add_column_adds_and_bumps_version() {
        let db = setup();
        let applier = DdlApplier::new(&db);
        applier.create_table(&issues_spec(), "01").unwrap();

        let spec = ColumnSpec::new("bool", 3);
        applier
            .add_column("issues", "closed", &spec, "public", "issues", "02")
            .unwrap();

        let cols = db
            .query_uncached("SELECT name FROM pragma_table_info('issues')", &[])
            .unwrap();
        let names: Vec<String> = cols
            .into_iter()
            .map(|r| match &r[0].1 {
                crate::Value::Text(s) => s.clone(),
                _ => String::new(),
            })
            .collect();
        assert!(names.contains(&"closed".to_string()));
        assert_eq!(
            applier
                .table_metadata
                .get_min_row_versions()
                .unwrap()
                .get("issues"),
            Some(&"02".to_string())
        );
    }

    #[test]
    fn update_column_rename_only() {
        let db = setup();
        let applier = DdlApplier::new(&db);
        applier.create_table(&issues_spec(), "01").unwrap();

        let spec = ColumnSpec::new("text", 2);
        applier
            .update_column(
                "issues", "title", "subject", &spec, &spec, "public", "issues", "02",
            )
            .unwrap();

        let cols = db
            .query_uncached("SELECT name FROM pragma_table_info('issues')", &[])
            .unwrap();
        let names: Vec<String> = cols
            .into_iter()
            .map(|r| match &r[0].1 {
                crate::Value::Text(s) => s.clone(),
                _ => String::new(),
            })
            .collect();
        assert!(names.contains(&"subject".to_string()));
        assert!(!names.contains(&"title".to_string()));
    }

    #[test]
    fn update_column_type_change_copies_values_and_recreates_indexes() {
        let db = setup();
        let applier = DdlApplier::new(&db);
        applier.create_table(&issues_spec(), "01").unwrap();
        db.run(
            "INSERT INTO issues (id, title) VALUES (?, ?)",
            &[
                crate::Value::Text("a".into()),
                crate::Value::Text("hello".into()),
            ],
        )
        .unwrap();

        let idx = test_index(
            "issues_title_idx",
            "issues",
            false,
            &[("title", Direction::Asc)],
        );
        applier.create_index(&idx, "01").unwrap();

        let old_spec = ColumnSpec::new("text", 2);
        let new_spec = ColumnSpec::new("json", 2);
        applier
            .update_column(
                "issues", "title", "title", &old_spec, &new_spec, "public", "issues", "02",
            )
            .unwrap();

        // Value was copied over through the rename dance.
        let rows = db
            .query_uncached(
                "SELECT title FROM issues WHERE id = ?",
                &[crate::Value::Text("a".into())],
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].1, crate::Value::Text("hello".into()));

        // The index was recreated (still present, now on the retyped column).
        let idxs = db
            .query_uncached(
                "SELECT name FROM sqlite_master WHERE type='index' AND name='issues_title_idx'",
                &[],
            )
            .unwrap();
        assert_eq!(idxs.len(), 1);
    }

    #[test]
    fn update_column_noop_when_nothing_changed() {
        let db = setup();
        let applier = DdlApplier::new(&db);
        applier.create_table(&issues_spec(), "01").unwrap();
        let spec = ColumnSpec::new("text", 2);
        // Same name, same type -> no-op (defaults are intentionally ignored).
        applier
            .update_column(
                "issues", "title", "title", &spec, &spec, "public", "issues", "02",
            )
            .unwrap();
        assert_eq!(
            applier
                .table_metadata
                .get_min_row_versions()
                .unwrap()
                .get("issues"),
            None
        );
    }
}
