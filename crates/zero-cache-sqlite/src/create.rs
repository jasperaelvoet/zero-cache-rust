//! Port of `zero-cache/src/db/create.ts`.
//!
//! SQL generation for creating replica tables/indexes from `LiteTableSpec`/
//! `LiteIndexSpec` — the DDL statements `ChangeProcessor.processCreateTable`
//! and `processCreateIndex` execute when a schema change arrives.

use zero_cache_types::lite::assert_valid_lite_column_spec;
use zero_cache_types::specs::{ColumnSpec, LiteIndexSpec, LiteTableSpec};
use zero_cache_types::sql::{id, id_list};

/// The column definition fragment (`"name" TYPE(len) NOT NULL DEFAULT ...`) for
/// a single column. `spec` is expected to come from `mapPostgresToLiteColumn`.
/// Port of `liteColumnDef`. Panics if the spec's `dataType` encoding is
/// internally inconsistent (matching the TS `assert`).
pub fn lite_column_def(spec: &ColumnSpec) -> String {
    assert_valid_lite_column_spec(spec).expect("invalid lite column spec");

    let mut def = id(&spec.data_type);
    if let Some(len) = spec.character_maximum_length {
        if len != 0 {
            def.push_str(&format!("({len})"));
        }
    }
    if spec.not_null == Some(true) {
        def.push_str(" NOT NULL");
    }
    if let Some(dflt) = &spec.dflt {
        def.push_str(&format!(" DEFAULT {dflt}"));
    }
    def
}

/// Constructs a `CREATE TABLE` statement for `spec`, columns ordered by their
/// `pos`. Port of `createLiteTableStatement`.
pub fn create_lite_table_statement(spec: &LiteTableSpec) -> String {
    let mut columns: Vec<&(String, ColumnSpec)> = spec.columns.iter().collect();
    columns.sort_by_key(|(_, c)| c.pos);

    let mut defs: Vec<String> = columns
        .iter()
        .map(|(name, col)| format!("{} {}", id(name), lite_column_def(col)))
        .collect();

    if let Some(pk) = &spec.primary_key {
        defs.push(format!(
            "PRIMARY KEY ({})",
            id_list(pk.iter().map(|s| s.as_str()))
        ));
    }

    format!(
        "CREATE TABLE {} (\n{}\n);",
        id(&spec.name),
        defs.join(",\n")
    )
}

/// Constructs a `CREATE [UNIQUE] INDEX` statement for `index`. Port of
/// `createLiteIndexStatement`.
pub fn create_lite_index_statement(index: &LiteIndexSpec) -> String {
    let columns: Vec<String> = index
        .columns
        .iter()
        .map(|(name, dir)| format!("{} {}", id(name), dir.as_str()))
        .collect();
    let unique = if index.unique { "UNIQUE" } else { "" };
    format!(
        "CREATE {unique} INDEX {} ON {} ({});",
        id(&index.name),
        id(&index.table_name),
        columns.join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StatementRunner;
    use zero_cache_types::specs::Direction;

    fn col(
        data_type: &str,
        pos: i64,
        not_null: bool,
        dflt: Option<&str>,
        cml: Option<i64>,
    ) -> ColumnSpec {
        ColumnSpec {
            pos,
            data_type: data_type.into(),
            pg_type_class: None,
            elem_pg_type_class: None,
            character_maximum_length: cml,
            not_null: Some(not_null),
            dflt: dflt.map(|s| s.into()),
        }
    }

    #[test]
    fn lite_column_def_variants() {
        assert_eq!(
            lite_column_def(&col("text", 1, false, None, None)),
            "\"text\""
        );
        assert_eq!(
            lite_column_def(&col("text", 1, true, None, None)),
            "\"text\" NOT NULL"
        );
        assert_eq!(
            lite_column_def(&col("varchar", 1, false, None, Some(180))),
            "\"varchar\"(180)"
        );
        assert_eq!(
            lite_column_def(&col("int8", 1, true, Some("'0'"), None)),
            "\"int8\" NOT NULL DEFAULT '0'"
        );
    }

    #[test]
    fn create_table_statement_orders_by_pos_and_appends_pk() {
        let spec = LiteTableSpec {
            name: "issues".into(),
            columns: vec![
                ("title".into(), col("text", 2, false, None, None)),
                ("id".into(), col("text", 1, true, None, None)),
            ],
            primary_key: Some(vec!["id".into()]),
        };
        let sql = create_lite_table_statement(&spec);
        assert_eq!(
            sql,
            "CREATE TABLE \"issues\" (\n\"id\" \"text\" NOT NULL,\n\"title\" \"text\",\nPRIMARY KEY (\"id\")\n);"
        );
    }

    #[test]
    fn create_index_statement() {
        let index = LiteIndexSpec {
            name: "issues_title_idx".into(),
            table_name: "issues".into(),
            unique: false,
            columns: vec![("title".into(), Direction::Asc)],
        };
        assert_eq!(
            create_lite_index_statement(&index),
            "CREATE  INDEX \"issues_title_idx\" ON \"issues\" (\"title\" ASC);"
        );

        let unique_index = LiteIndexSpec {
            name: "issues_pkey".into(),
            table_name: "issues".into(),
            unique: true,
            columns: vec![("id".into(), Direction::Asc)],
        };
        assert_eq!(
            create_lite_index_statement(&unique_index),
            "CREATE UNIQUE INDEX \"issues_pkey\" ON \"issues\" (\"id\" ASC);"
        );
    }

    #[test]
    fn generated_sql_actually_creates_tables_in_sqlite() {
        let db = StatementRunner::open_in_memory().unwrap();
        let spec = LiteTableSpec {
            name: "issues".into(),
            columns: vec![
                ("id".into(), col("text", 1, true, None, None)),
                ("count".into(), col("int8", 2, false, Some("0"), None)),
            ],
            primary_key: Some(vec!["id".into()]),
        };
        db.exec(&create_lite_table_statement(&spec)).unwrap();
        db.run(
            "INSERT INTO issues (id) VALUES (?)",
            &[crate::Value::Text("a".into())],
        )
        .unwrap();
        let rows = db
            .query_uncached("SELECT id, count FROM issues", &[])
            .unwrap();
        assert_eq!(rows.len(), 1);

        let index = LiteIndexSpec {
            name: "issues_count_idx".into(),
            table_name: "issues".into(),
            unique: false,
            columns: vec![("count".into(), Direction::Desc)],
        };
        db.exec(&create_lite_index_statement(&index)).unwrap();
        let indexes = db
            .query_uncached("SELECT name FROM sqlite_master WHERE type='index'", &[])
            .unwrap();
        assert!(!indexes.is_empty());
    }
}
