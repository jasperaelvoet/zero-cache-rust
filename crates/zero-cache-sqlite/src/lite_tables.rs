//! Partial port of `zero-cache/src/db/lite-tables.ts`.
//!
//! Reads table and index specifications from a SQLite replica by introspecting
//! `sqlite_master` and the `pragma_table_info` / `pragma_index_*` functions.
//!
//! The column/table *metadata table* integration (`ColumnMetadataStore`,
//! `TableMetadataTracker`) is part of the not-yet-ported replicator schema
//! modules; this ports the fallback path that reads column types directly from
//! SQLite. When a replicated table declaration omits its primary key, row keys
//! are recovered from its unique indexes (the key-selection part of upstream's
//! `computeZqlSpecs`).

use zero_cache_types::lite::{is_array, is_enum};
use zero_cache_types::specs::{ColumnSpec, Direction, LiteIndexSpec, LiteTableSpec, PgTypeClass};

use crate::{StatementRunner, Value};

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        _ => String::new(),
    }
}
fn opt_text(v: &Value) -> Option<String> {
    match v {
        Value::Text(s) => Some(s.clone()),
        _ => None,
    }
}
fn int(v: &Value) -> i64 {
    match v {
        Value::Integer(n) => *n,
        _ => 0,
    }
}

/// The element type class for an array column's lite type, or `None` for scalar
/// columns. Mirrors the `elemPgTypeClass` computation in `listTables`.
fn elem_pg_type_class(lite_type: &str) -> Option<PgTypeClass> {
    if is_array(lite_type) {
        Some(if is_enum(lite_type) {
            PgTypeClass::Enum
        } else {
            PgTypeClass::Base
        })
    } else {
        None
    }
}

/// Lists the user tables of the replica with their column specs. Port of
/// `listTables` (fallback path: reads types from SQLite, no metadata table).
pub fn list_tables(db: &StatementRunner) -> Result<Vec<LiteTableSpec>, crate::DbError> {
    let rows = db.query_uncached(
        r#"
        SELECT
          m.name as "table",
          p.name as name,
          p.type as type,
          p."notnull" as "notNull",
          p.dflt_value as "dflt",
          p.pk as keyPos
        FROM sqlite_master as m
        LEFT JOIN pragma_table_info(m.name) as p
        WHERE m.type = 'table'
        AND m.name NOT LIKE 'sqlite_%'
        AND m.name NOT LIKE '_zero.%'
        AND m.name NOT LIKE '_litestream_%'
        "#,
        &[],
    )?;

    let mut tables: Vec<LiteTableSpec> = Vec::new();
    for row in &rows {
        let table_name = text(&row[0].1);
        let col_name = text(&row[1].1);
        let col_type = text(&row[2].1);
        let not_null = int(&row[3].1) != 0;
        let dflt = opt_text(&row[4].1);
        let key_pos = int(&row[5].1);

        if tables.last().map(|t| &t.name) != Some(&table_name) {
            tables.push(LiteTableSpec {
                name: table_name.clone(),
                columns: Vec::new(),
                primary_key: None,
            });
        }
        let table = tables.last_mut().unwrap();

        let pos = table.columns.len() as i64 + 1;
        table.columns.push((
            col_name.clone(),
            ColumnSpec {
                pos,
                data_type: col_type.clone(),
                pg_type_class: None,
                elem_pg_type_class: elem_pg_type_class(&col_type),
                character_maximum_length: None,
                not_null: Some(not_null),
                dflt,
            },
        ));

        if key_pos > 0 {
            let pk = table.primary_key.get_or_insert_with(Vec::new);
            while (pk.len() as i64) < key_pos {
                pk.push(String::new());
            }
            pk[(key_pos - 1) as usize] = col_name;
        }
    }

    // A schema change that is still being backfilled must not become visible
    // to query planning or hydration.  The replicator records that state in
    // `_zero.column_metadata.backfill`; filter those columns at the same
    // boundary as upstream `listTables` does.  If a primary-key column is
    // backfilling, hide the whole table until completion rather than exposing
    // a table with an invalid row identity.
    let backfill_rows = db
        .query_uncached(
            r#"SELECT table_name, column_name
           FROM "_zero.column_metadata"
           WHERE backfill IS NOT NULL"#,
            &[],
        )
        .unwrap_or_default();
    if !backfill_rows.is_empty() {
        let hidden: std::collections::HashMap<String, std::collections::HashSet<String>> =
            backfill_rows
                .into_iter()
                .filter_map(|row| {
                    let table = text(&row[0].1);
                    let column = text(&row[1].1);
                    (!table.is_empty() && !column.is_empty()).then_some((table, column))
                })
                .fold(
                    std::collections::HashMap::new(),
                    |mut map, (table, column)| {
                        map.entry(table).or_default().insert(column);
                        map
                    },
                );
        tables.retain_mut(|table| {
            let Some(columns) = hidden.get(&table.name) else {
                return true;
            };
            if table
                .primary_key
                .as_ref()
                .is_some_and(|key| key.iter().any(|column| columns.contains(column)))
            {
                return false;
            }
            table.columns.retain(|(name, _)| !columns.contains(name));
            !table.columns.is_empty()
        });
    }

    // `map_postgres_to_lite` deliberately creates the replica table without a
    // PRIMARY KEY and recreates upstream indexes after the snapshot copy. In
    // that normal replicated shape pragma_table_info(...).pk is therefore
    // empty. Recover the row identity from a unique index, preferring the
    // conventional Postgres `<table>_pkey` index. The deterministic fallback
    // also supports tables whose replica identity is another unique index.
    let indexes = list_indexes(db)?;
    for table in &mut tables {
        if table.primary_key.is_some() {
            continue;
        }

        let expected_primary = format!("{}_pkey", table.name);
        let mut candidates: Vec<&LiteIndexSpec> = indexes
            .iter()
            .filter(|index| {
                index.table_name == table.name && index.unique && !index.columns.is_empty()
            })
            .collect();
        candidates.sort_by(|a, b| {
            let a_primary = a.name == expected_primary || a.name.ends_with("_pkey");
            let b_primary = b.name == expected_primary || b.name.ends_with("_pkey");
            b_primary
                .cmp(&a_primary)
                .then_with(|| a.columns.len().cmp(&b.columns.len()))
                .then_with(|| a.name.cmp(&b.name))
        });

        if let Some(index) = candidates.first() {
            table.primary_key = Some(
                index
                    .columns
                    .iter()
                    .map(|(column, _)| column.clone())
                    .collect(),
            );
        }
    }

    Ok(tables)
}

/// Lists the indexes of the replica. Port of `listIndexes`.
pub fn list_indexes(db: &StatementRunner) -> Result<Vec<LiteIndexSpec>, crate::DbError> {
    let rows = db.query_uncached(
        r#"
        SELECT
          idx.name as indexName,
          idx.tbl_name as tableName,
          info."unique" as "unique",
          col.name as column,
          CASE WHEN col.desc = 0 THEN 'ASC' ELSE 'DESC' END as dir
        FROM sqlite_master as idx
        JOIN pragma_index_list(idx.tbl_name) AS info ON info.name = idx.name
        JOIN pragma_index_xinfo(idx.name) as col
        WHERE idx.type = 'index' AND
              col.key = 1 AND
              idx.tbl_name NOT LIKE '_zero.%'
        ORDER BY idx.name, col.seqno ASC
        "#,
        &[],
    )?;

    let mut ret: Vec<LiteIndexSpec> = Vec::new();
    for row in &rows {
        let name = text(&row[0].1);
        let table_name = text(&row[1].1);
        let unique = int(&row[2].1) != 0;
        let column = text(&row[3].1);
        let dir = Direction::from_str(&text(&row[4].1)).unwrap_or(Direction::Asc);

        if ret.last().map(|i| &i.name) == Some(&name) {
            ret.last_mut().unwrap().columns.push((column, dir));
        } else {
            ret.push(LiteIndexSpec {
                name,
                table_name,
                unique,
                columns: vec![(column, dir)],
            });
        }
    }

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(
        pos: i64,
        data_type: &str,
        elem: Option<PgTypeClass>,
        not_null: bool,
        dflt: Option<&str>,
    ) -> ColumnSpec {
        ColumnSpec {
            pos,
            data_type: data_type.into(),
            pg_type_class: None,
            elem_pg_type_class: elem,
            character_maximum_length: None,
            not_null: Some(not_null),
            dflt: dflt.map(|s| s.into()),
        }
    }

    #[test]
    fn no_tables() {
        let db = StatementRunner::open_in_memory().unwrap();
        assert_eq!(list_tables(&db).unwrap(), vec![]);
    }

    #[test]
    fn zero_clients() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(
            r#"CREATE TABLE "zero.clients" (
                "clientID" VARCHAR (180) PRIMARY KEY,
                "lastMutationID" BIGINT
            );"#,
        )
        .unwrap();
        let tables = list_tables(&db).unwrap();
        assert_eq!(
            tables,
            vec![LiteTableSpec {
                name: "zero.clients".into(),
                columns: vec![
                    (
                        "clientID".into(),
                        col(1, "VARCHAR (180)", None, false, None)
                    ),
                    ("lastMutationID".into(), col(2, "BIGINT", None, false, None)),
                ],
                primary_key: Some(vec!["clientID".into()]),
            }]
        );
    }

    #[test]
    fn types_and_array_types() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(
            r#"CREATE TABLE users (
                user_id INTEGER PRIMARY KEY,
                handle text DEFAULT 'foo',
                address text[],
                bigint BIGINT DEFAULT '2147483648',
                bool_array BOOL[],
                int_array INTEGER[] DEFAULT '{1, 2, 3}',
                json_val JSONB,
                time_array TIME[]
            );"#,
        )
        .unwrap();
        let tables = list_tables(&db).unwrap();
        let cols = &tables[0].columns;
        assert_eq!(
            cols[0],
            ("user_id".into(), col(1, "INTEGER", None, false, None))
        );
        assert_eq!(
            cols[1],
            ("handle".into(), col(2, "TEXT", None, false, Some("'foo'")))
        );
        assert_eq!(
            cols[2],
            (
                "address".into(),
                col(3, "text[]", Some(PgTypeClass::Base), false, None)
            )
        );
        assert_eq!(
            cols[3],
            (
                "bigint".into(),
                col(4, "BIGINT", None, false, Some("'2147483648'"))
            )
        );
        assert_eq!(
            cols[4],
            (
                "bool_array".into(),
                col(5, "BOOL[]", Some(PgTypeClass::Base), false, None)
            )
        );
        assert_eq!(
            cols[5],
            (
                "int_array".into(),
                col(
                    6,
                    "INTEGER[]",
                    Some(PgTypeClass::Base),
                    false,
                    Some("'{1, 2, 3}'")
                )
            )
        );
        assert_eq!(
            cols[6],
            ("json_val".into(), col(7, "JSONB", None, false, None))
        );
        assert_eq!(
            cols[7],
            (
                "time_array".into(),
                col(8, "TIME[]", Some(PgTypeClass::Base), false, None)
            )
        );
        assert_eq!(tables[0].primary_key, Some(vec!["user_id".into()]));
    }

    #[test]
    fn compound_primary_key_order() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(
            r#"CREATE TABLE issues (
                issue_id INTEGER,
                description TEXT,
                org_id INTEGER NOT NULL,
                component_id INTEGER,
                PRIMARY KEY (org_id, component_id, issue_id)
            );"#,
        )
        .unwrap();
        let tables = list_tables(&db).unwrap();
        assert_eq!(
            tables[0].primary_key,
            Some(vec![
                "org_id".into(),
                "component_id".into(),
                "issue_id".into()
            ])
        );
        // NOT NULL is reflected on org_id.
        assert_eq!(tables[0].columns[2].1.not_null, Some(true));
        assert_eq!(tables[0].columns[0].1.not_null, Some(false));
    }

    #[test]
    fn recovers_primary_key_from_replicated_pkey_index() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(
            r#"CREATE TABLE "Game" (
                "id" "text|NOT_NULL",
                "joinCode" "text|NOT_NULL"
            );
            CREATE UNIQUE INDEX "Game_joinCode_key" ON "Game" ("joinCode" ASC);
            CREATE UNIQUE INDEX "Game_pkey" ON "Game" ("id" ASC);"#,
        )
        .unwrap();

        let tables = list_tables(&db).unwrap();
        let game = tables.iter().find(|table| table.name == "Game").unwrap();
        assert_eq!(game.primary_key, Some(vec!["id".into()]));
    }

    #[test]
    fn indexes() {
        let db = StatementRunner::open_in_memory().unwrap();
        db.exec(
            r#"CREATE TABLE users (
                userID VARCHAR (180) PRIMARY KEY,
                first TEXT,
                last TEXT,
                handle TEXT UNIQUE
            );
            CREATE INDEX full_name ON users (last desc, first);"#,
        )
        .unwrap();
        let indexes = list_indexes(&db).unwrap();
        assert_eq!(
            indexes,
            vec![
                LiteIndexSpec {
                    name: "full_name".into(),
                    table_name: "users".into(),
                    unique: false,
                    columns: vec![
                        ("last".into(), Direction::Desc),
                        ("first".into(), Direction::Asc)
                    ],
                },
                LiteIndexSpec {
                    name: "sqlite_autoindex_users_1".into(),
                    table_name: "users".into(),
                    unique: true,
                    columns: vec![("userID".into(), Direction::Asc)],
                },
                LiteIndexSpec {
                    name: "sqlite_autoindex_users_2".into(),
                    table_name: "users".into(),
                    unique: true,
                    columns: vec![("handle".into(), Direction::Asc)],
                },
            ]
        );
    }
}
