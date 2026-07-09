//! Port of the DDL-generation half of
//! `view-syncer/schema/cvr.ts`: the Postgres schema CVRStore persists to
//! (`instances`/`clients`/`queries`/`desires`/`rowsVersion`/`rows` tables).
//!
//! Scope: SQL text generation only — `CREATE SCHEMA`/`CREATE TABLE`/
//! `CREATE INDEX` statements, byte-for-byte structurally matching
//! upstream's template strings (quoted identifiers via
//! `zero_cache_types::sql::id`, matching upstream's `pg-format` `ident()`).
//! NOT ported: the `CVRStore` class itself (~1447 lines — load/flush
//! against a live Postgres connection, row-diffing, catchup-patch queries)
//! or the `InstancesRow`/`ClientsRow`/`QueriesRow`/`DesiresRow`/`RowsRow`
//! row types and their `compare*Rows`/`rowsRowToRowRecord`/
//! `rowRecordToRowsRow` conversions — those operate on live query results
//! this crate has no Postgres CVR connection to produce yet. This is
//! deliberately the same "port the SQL text first" slice this project used
//! for `zero-cache-sqlite::create` (replica DDL) and
//! `zero-cache-mutagen::sql` (CRUD SQL) before their respective live
//! execution layers existed.

use zero_cache_types::shards::{cvr_schema, ShardError, ShardId};
use zero_cache_types::sql::id;

fn schema_id(shard: &ShardId) -> Result<String, ShardError> {
    Ok(id(&cvr_schema(shard)?))
}

/// Port of `createSchema`.
pub fn create_schema_sql(shard: &ShardId) -> Result<String, ShardError> {
    Ok(format!(
        "CREATE SCHEMA IF NOT EXISTS {};",
        schema_id(shard)?
    ))
}

/// Port of `createInstancesTable`.
pub fn create_instances_table_sql(shard: &ShardId) -> Result<String, ShardError> {
    let s = schema_id(shard)?;
    Ok(format!(
        "\nCREATE TABLE {s}.instances (\n  \
         \"clientGroupID\"  TEXT PRIMARY KEY,\n  \
         \"version\"        TEXT NOT NULL,\n  \
         \"lastActive\"     TIMESTAMPTZ NOT NULL,\n  \
         \"ttlClock\"       DOUBLE PRECISION NOT NULL DEFAULT 0,\n  \
         \"replicaVersion\" TEXT,\n  \
         \"owner\"          TEXT,\n  \
         \"grantedAt\"      TIMESTAMPTZ,\n  \
         \"clientSchema\"   JSONB,\n  \
         \"profileID\"      TEXT,\n  \
         \"deleted\"        BOOL DEFAULT FALSE\n\
         );\n\n\
         CREATE INDEX instances_last_active\n  \
         ON {s}.instances (\"lastActive\") WHERE NOT \"deleted\";\n\
         CREATE INDEX tombstones_last_active\n  \
         ON {s}.instances (\"lastActive\") WHERE \"deleted\";\n\n\
         CREATE INDEX profile_ids_last_active ON {s}.instances (\"lastActive\", \"profileID\")\n  \
         WHERE \"profileID\" IS NOT NULL;\n"
    ))
}

/// Port of `createClientsTable`.
pub fn create_clients_table_sql(shard: &ShardId) -> Result<String, ShardError> {
    let s = schema_id(shard)?;
    Ok(format!(
        "\nCREATE TABLE {s}.clients (\n  \
         \"clientGroupID\"      TEXT,\n  \
         \"clientID\"           TEXT,\n\n  \
         PRIMARY KEY (\"clientGroupID\", \"clientID\"),\n\n  \
         CONSTRAINT fk_clients_client_group\n    \
         FOREIGN KEY(\"clientGroupID\")\n    \
         REFERENCES {s}.instances(\"clientGroupID\")\n    \
         ON DELETE CASCADE\n\
         );\n\n"
    ))
}

/// Port of `createQueriesTable`.
pub fn create_queries_table_sql(shard: &ShardId) -> Result<String, ShardError> {
    let s = schema_id(shard)?;
    Ok(format!(
        "\nCREATE TABLE {s}.queries (\n  \
         \"clientGroupID\"         TEXT,\n  \
         \"queryHash\"             TEXT,\n  \
         \"clientAST\"             JSONB,\n  \
         \"queryName\"             TEXT,\n  \
         \"queryArgs\"             JSON,\n  \
         \"patchVersion\"          TEXT,\n  \
         \"transformationHash\"    TEXT,\n  \
         \"transformationVersion\" TEXT,\n  \
         \"internal\"              BOOL,\n  \
         \"deleted\"               BOOL,\n  \
         \"rowSetSignature\"       TEXT,\n\n  \
         PRIMARY KEY (\"clientGroupID\", \"queryHash\"),\n\n  \
         CONSTRAINT fk_queries_client_group\n    \
         FOREIGN KEY(\"clientGroupID\")\n    \
         REFERENCES {s}.instances(\"clientGroupID\")\n    \
         ON DELETE CASCADE\n\
         );\n\n\
         CREATE INDEX queries_patch_version \n  \
         ON {s}.queries (\"patchVersion\" NULLS FIRST);\n"
    ))
}

/// Port of `createDesiresTable`.
pub fn create_desires_table_sql(shard: &ShardId) -> Result<String, ShardError> {
    let s = schema_id(shard)?;
    Ok(format!(
        "\nCREATE TABLE {s}.desires (\n  \
         \"clientGroupID\"      TEXT,\n  \
         \"clientID\"           TEXT,\n  \
         \"queryHash\"          TEXT,\n  \
         \"patchVersion\"       TEXT NOT NULL,\n  \
         \"deleted\"            BOOL,\n  \
         \"ttl\"                INTERVAL,\n  \
         \"ttlMs\"              DOUBLE PRECISION,\n  \
         \"inactivatedAt\"      TIMESTAMPTZ,\n  \
         \"inactivatedAtMs\"    DOUBLE PRECISION,\n\n  \
         PRIMARY KEY (\"clientGroupID\", \"clientID\", \"queryHash\"),\n\n  \
         CONSTRAINT fk_desires_query\n    \
         FOREIGN KEY(\"clientGroupID\", \"queryHash\")\n    \
         REFERENCES {s}.queries(\"clientGroupID\", \"queryHash\")\n    \
         ON DELETE CASCADE\n\
         );\n\n\
         CREATE INDEX desires_patch_version\n  \
         ON {s}.desires (\"patchVersion\");\n\n\
         CREATE INDEX desires_inactivated_at\n  \
         ON {s}.desires (\"inactivatedAt\");\n"
    ))
}

/// Port of `createRowsVersionTable`.
pub fn create_rows_version_table_sql(shard: &ShardId) -> Result<String, ShardError> {
    let s = schema_id(shard)?;
    Ok(format!(
        "\nCREATE TABLE {s}.\"rowsVersion\" (\n  \
         \"clientGroupID\" TEXT PRIMARY KEY,\n  \
         \"version\"       TEXT NOT NULL\n\
         );\n"
    ))
}

/// Port of `createRowsTable`.
pub fn create_rows_table_sql(shard: &ShardId) -> Result<String, ShardError> {
    let s = schema_id(shard)?;
    Ok(format!(
        "\nCREATE TABLE {s}.rows (\n  \
         \"clientGroupID\"    TEXT,\n  \
         \"schema\"           TEXT,\n  \
         \"table\"            TEXT,\n  \
         \"rowKey\"           JSONB,\n  \
         \"rowVersion\"       TEXT NOT NULL,\n  \
         \"patchVersion\"     TEXT NOT NULL,\n  \
         \"refCounts\"        JSONB,\n\n  \
         PRIMARY KEY (\"clientGroupID\", \"schema\", \"table\", \"rowKey\"),\n\n  \
         CONSTRAINT fk_rows_client_group\n    \
         FOREIGN KEY(\"clientGroupID\")\n    \
         REFERENCES {s}.\"rowsVersion\" (\"clientGroupID\")\n    \
         ON DELETE CASCADE\n\
         );\n\n\
         CREATE INDEX row_patch_version \n  \
         ON {s}.rows (\"patchVersion\");\n\n\
         CREATE INDEX row_ref_counts ON {s}.rows \n  \
         USING GIN (\"refCounts\");\n"
    ))
}

/// All statements needed to create the full CVR schema, in dependency
/// order (schema, then instances, then everything that FK-references it).
/// Not a named upstream export — upstream's `CREATE_CVR_SCHEMA` init logic
/// lives in `schema/init.ts`, unported; this is a convenience for callers
/// of this module alone.
pub fn create_cvr_schema_statements(shard: &ShardId) -> Result<Vec<String>, ShardError> {
    Ok(vec![
        create_schema_sql(shard)?,
        create_instances_table_sql(shard)?,
        create_clients_table_sql(shard)?,
        create_queries_table_sql(shard)?,
        create_desires_table_sql(shard)?,
        create_rows_version_table_sql(shard)?,
        create_rows_table_sql(shard)?,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard() -> ShardId {
        ShardId {
            app_id: "myapp".into(),
            shard_num: 0,
        }
    }

    #[test]
    fn create_schema_sql_quotes_the_slash_containing_schema_name() {
        assert_eq!(
            create_schema_sql(&shard()).unwrap(),
            "CREATE SCHEMA IF NOT EXISTS \"myapp_0/cvr\";"
        );
    }

    #[test]
    fn instances_table_has_primary_key_and_indexes() {
        let sql = create_instances_table_sql(&shard()).unwrap();
        assert!(sql.contains("CREATE TABLE \"myapp_0/cvr\".instances"));
        assert!(sql.contains("\"clientGroupID\"  TEXT PRIMARY KEY"));
        assert!(sql.contains("CREATE INDEX instances_last_active"));
        assert!(sql.contains("CREATE INDEX profile_ids_last_active"));
    }

    #[test]
    fn clients_table_references_instances() {
        let sql = create_clients_table_sql(&shard()).unwrap();
        assert!(sql.contains("REFERENCES \"myapp_0/cvr\".instances(\"clientGroupID\")"));
        assert!(sql.contains("ON DELETE CASCADE"));
    }

    #[test]
    fn queries_table_has_composite_primary_key() {
        let sql = create_queries_table_sql(&shard()).unwrap();
        assert!(sql.contains("PRIMARY KEY (\"clientGroupID\", \"queryHash\")"));
        assert!(sql.contains("CREATE INDEX queries_patch_version"));
    }

    #[test]
    fn desires_table_references_queries_composite_key() {
        let sql = create_desires_table_sql(&shard()).unwrap();
        assert!(
            sql.contains("REFERENCES \"myapp_0/cvr\".queries(\"clientGroupID\", \"queryHash\")")
        );
        assert!(sql.contains("CREATE INDEX desires_inactivated_at"));
    }

    #[test]
    fn rows_version_table_is_minimal() {
        let sql = create_rows_version_table_sql(&shard()).unwrap();
        assert!(sql.contains("CREATE TABLE \"myapp_0/cvr\".\"rowsVersion\""));
        assert!(sql.contains("\"clientGroupID\" TEXT PRIMARY KEY"));
    }

    #[test]
    fn rows_table_references_rows_version_and_has_gin_index() {
        let sql = create_rows_table_sql(&shard()).unwrap();
        assert!(sql.contains("REFERENCES \"myapp_0/cvr\".\"rowsVersion\" (\"clientGroupID\")"));
        assert!(sql.contains("USING GIN (\"refCounts\")"));
    }

    #[test]
    fn create_cvr_schema_statements_orders_schema_and_instances_first() {
        let stmts = create_cvr_schema_statements(&shard()).unwrap();
        assert_eq!(stmts.len(), 7);
        assert!(stmts[0].starts_with("CREATE SCHEMA"));
        assert!(stmts[1].contains("CREATE TABLE \"myapp_0/cvr\".instances"));
    }

    #[test]
    fn invalid_app_id_propagates_shard_error() {
        let bad = ShardId {
            app_id: "Not-Valid!".into(),
            shard_num: 0,
        };
        assert!(create_schema_sql(&bad).is_err());
    }

    /// Live verification: the generated DDL is not just string-matched
    /// against upstream's template, it's executed against a real Postgres
    /// instance and confirmed to actually create the expected tables —
    /// the same "don't just trust the string, run it" standard this port
    /// has applied to every other SQL-generation module (DDL apply, CRUD
    /// SQL). Skips gracefully if no local test Postgres is reachable.
    #[tokio::test]
    async fn generated_ddl_is_valid_and_creates_the_expected_tables() {
        let conn_str = std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        });
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "cvrtest".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"cvrtest_0/cvr\" CASCADE;")
            .await
            .unwrap();

        for stmt in create_cvr_schema_statements(&shard).unwrap() {
            client
                .batch_execute(&stmt)
                .await
                .unwrap_or_else(|e| panic!("statement failed: {stmt}\n{e}"));
        }

        let rows = client
            .query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name",
                &[&"cvrtest_0/cvr"],
            )
            .await
            .unwrap();
        let tables: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
        assert_eq!(
            tables,
            vec![
                "clients",
                "desires",
                "instances",
                "queries",
                "rows",
                "rowsVersion"
            ]
        );

        client
            .batch_execute("DROP SCHEMA \"cvrtest_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }
}
