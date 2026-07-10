//! Pure name/identifier and DDL-string builders from
//! `change-source/pg/schema/shard.ts` — the publication/replication-slot naming
//! scheme and the `dropShard` teardown SQL.
//!
//! These underpin `ensurePublishedTables`/`setupTablesAndReplication` (the
//! shard-schema DDL orchestration, still to come, which runs live `CREATE
//! SCHEMA`/`CREATE PUBLICATION` statements). Ported here first because the
//! naming rules are pure, self-contained, and an injection-safety boundary
//! (`validate_publication_name` is upstream's "defense-in-depth against SQL
//! injection when publication names are used in replication commands").
//!
//! Slot/schema names are derived from an [`AppId`]/[`ShardId`] via the existing
//! `zero_cache_types::shards` helpers; identifier quoting reuses
//! `zero_cache_types::sql::id`.

use zero_cache_types::shards::{app_schema, check, upstream_schema, AppId, ShardError, ShardId};
use zero_cache_types::sql::id;

use crate::published_schema::literal_list;

/// Port of `SHARD_CONFIG_TABLE`.
pub const SHARD_CONFIG_TABLE: &str = "shardConfig";

/// A publication name that is not a valid Postgres identifier (or too long).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublicationNameError {
    #[error(
        "Invalid publication name \"{0}\". Publication names must start with a letter or \
         underscore and contain only letters, digits, and underscores."
    )]
    Invalid(String),
    #[error("Publication name \"{0}\" exceeds PostgreSQL's 63-character identifier limit.")]
    TooLong(String),
}

/// Port of `validatePublicationName`: an unquoted Postgres identifier must
/// start with a letter/underscore, contain only letters/digits/underscores,
/// and be at most 63 characters. Order matches upstream (charset first, then
/// length).
pub fn validate_publication_name(name: &str) -> Result<(), PublicationNameError> {
    if !is_valid_identifier(name) {
        return Err(PublicationNameError::Invalid(name.to_string()));
    }
    if name.len() > 63 {
        return Err(PublicationNameError::TooLong(name.to_string()));
    }
    Ok(())
}

/// `^[a-zA-Z_][a-zA-Z0-9_]*$` — port of `VALID_PUBLICATION_NAME`.
fn is_valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Port of `internalPublicationPrefix`: `_<appID>_`.
pub fn internal_publication_prefix(app_id: &str) -> String {
    format!("_{app_id}_")
}

/// Port of `legacyReplicationSlot`: `<appID>_<shardNum>` (no validation, as
/// upstream — it reads the raw fields).
pub fn legacy_replication_slot(app_id: &str, shard_num: i64) -> String {
    format!("{app_id}_{shard_num}")
}

/// Port of `replicationSlotPrefix`: `<appID>_<shardNum>_`, after validating the
/// shard.
pub fn replication_slot_prefix(shard: &ShardId) -> Result<String, ShardError> {
    let (app_id, shard_num) = check(shard)?;
    Ok(format!("{app_id}_{shard_num}_"))
}

/// Port of `replicationSlotExpression`: the slot prefix followed by `%` for use
/// in a `LIKE`, with every underscore escaped (`\_`) since `_` is a LIKE
/// wildcard.
pub fn replication_slot_expression(shard: &ShardId) -> Result<String, ShardError> {
    let raw = format!("{}%", replication_slot_prefix(shard)?);
    Ok(raw.replace('_', "\\_"))
}

/// Port of `defaultPublicationName`: `_<appID>_public_<shardID>`.
pub fn default_publication_name(app_id: &str, shard_num: i64) -> String {
    format!("_{app_id}_public_{shard_num}")
}

/// Port of `metadataPublicationName`: `_<appID>_metadata_<shardID>`.
pub fn metadata_publication_name(app_id: &str, shard_num: i64) -> String {
    format!("_{app_id}_metadata_{shard_num}")
}

/// Port of `dropShard`: the teardown SQL that drops the shard's two internal
/// publications (explicitly — `DROP SCHEMA CASCADE` does not drop dependent
/// publications) and then the shard schema. Identifiers are quoted via `id`.
pub fn drop_shard(app_id: &str, shard_num: i64) -> String {
    let schema = format!("{app_id}_{shard_num}");
    let metadata_publication = metadata_publication_name(app_id, shard_num);
    let default_publication = default_publication_name(app_id, shard_num);
    format!(
        "\n    DROP PUBLICATION IF EXISTS {};\n    \
         DROP PUBLICATION IF EXISTS {};\n    \
         DROP SCHEMA IF EXISTS {} CASCADE;\n  ",
        id(&default_publication),
        id(&metadata_publication),
        id(&schema),
    )
}

/// Port of `getClientsTableDefinition`: the `clients` table (last-mutation-id
/// tracking) in the given (already-quoted) `schema`.
pub fn get_clients_table_definition(schema: &str) -> String {
    format!(
        "\n  CREATE TABLE {schema}.\"clients\" (\n    \
         \"clientGroupID\"  TEXT NOT NULL,\n    \
         \"clientID\"       TEXT NOT NULL,\n    \
         \"lastMutationID\" BIGINT NOT NULL,\n    \
         \"userID\"         TEXT,\n    \
         PRIMARY KEY(\"clientGroupID\", \"clientID\")\n  );"
    )
}

/// Port of `getMutationsTableDefinition`: the `mutations` result-tracking table
/// in the given (already-quoted) `schema`.
pub fn get_mutations_table_definition(schema: &str) -> String {
    format!(
        "\n  CREATE TABLE {schema}.\"mutations\" (\n    \
         \"clientGroupID\"  TEXT NOT NULL,\n    \
         \"clientID\"       TEXT NOT NULL,\n    \
         \"mutationID\"     BIGINT NOT NULL,\n    \
         \"result\"         JSON NOT NULL,\n    \
         PRIMARY KEY(\"clientGroupID\", \"clientID\", \"mutationID\")\n  );"
    )
}

/// Port of `globalSetup`: the idempotent app-wide schema + `permissions` table,
/// its hash trigger, and the seed row. Idempotent (all `IF NOT EXISTS` / `OR
/// REPLACE`) since it runs once per shard. `app_id` is validated via
/// `app_schema`.
pub fn global_setup(app_id: &AppId) -> Result<String, ShardError> {
    let app = id(&app_schema(app_id)?);
    Ok(format!(
        r#"
  CREATE SCHEMA IF NOT EXISTS {app};

  CREATE TABLE IF NOT EXISTS {app}.permissions (
    "permissions" JSONB,
    "hash"        TEXT,

    -- Ensure that there is only a single row in the table.
    -- Application code can be agnostic to this column, and
    -- simply invoke UPDATE statements on the version columns.
    "lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
  );

  CREATE OR REPLACE FUNCTION {app}.set_permissions_hash()
  RETURNS TRIGGER AS $$
  BEGIN
      NEW.hash = md5(NEW.permissions::text);
      RETURN NEW;
  END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE TRIGGER on_set_permissions
    BEFORE INSERT OR UPDATE ON {app}.permissions
    FOR EACH ROW
    EXECUTE FUNCTION {app}.set_permissions_hash();

  INSERT INTO {app}.permissions (permissions) VALUES (NULL) ON CONFLICT DO NOTHING;
"#
    ))
}

/// A [`shard_setup`] precondition violation.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum ShardSetupError {
    #[error(transparent)]
    Shard(#[from] ShardError),
    #[error("Publications must include {0}")]
    MissingMetadataPublication(String),
}

/// Port of `shardSetup`: the per-shard schema DDL — the shard schema, its
/// `clients`/`mutations` tables, the metadata publication (which must be one of
/// the shard's publications), the `shardConfig` singleton row (seeding the
/// sorted publication list), and the `replicas` table. `publications` is the
/// shard's publication list; it must include `metadata_publication`.
pub fn shard_setup(
    shard: &ShardConfigInput,
    metadata_publication: &str,
) -> Result<String, ShardSetupError> {
    let app_id = AppId {
        app_id: shard.app_id.clone(),
    };
    let shard_id = ShardId {
        app_id: shard.app_id.clone(),
        shard_num: shard.shard_num,
    };
    let app = id(&app_schema(&app_id)?);
    let shard_schema = id(&upstream_schema(&shard_id)?);

    let mut pubs = shard.publications.clone();
    pubs.sort();
    if !pubs.iter().any(|p| p == metadata_publication) {
        return Err(ShardSetupError::MissingMetadataPublication(
            metadata_publication.to_string(),
        ));
    }

    let clients = get_clients_table_definition(&shard_schema);
    let mutations = get_mutations_table_definition(&shard_schema);
    let meta_pub = id(metadata_publication);
    let pubs_literal = literal_list(&pubs);
    let cfg = SHARD_CONFIG_TABLE;

    Ok(format!(
        r#"
  CREATE SCHEMA IF NOT EXISTS {shard_schema};
{clients}
{mutations}

  DROP PUBLICATION IF EXISTS {meta_pub};
  CREATE PUBLICATION {meta_pub}
    FOR TABLE {app}."permissions", TABLE {shard_schema}."clients", {shard_schema}."mutations";

  CREATE TABLE {shard_schema}."{cfg}" (
    "publications"  TEXT[] NOT NULL,
    "ddlDetection"  BOOL NOT NULL,

    -- Ensure that there is only a single row in the table.
    "lock" BOOL PRIMARY KEY DEFAULT true CHECK (lock)
  );

  INSERT INTO {shard_schema}."{cfg}" (
      "publications",
      "ddlDetection"
    ) VALUES (
      ARRAY[{pubs_literal}],
      false  -- set in SAVEPOINT with triggerSetup() statements
    );

  CREATE TABLE {shard_schema}.replicas (
    -- The DEFAULT exists purely for backwards compatibility support.
    -- New code always specifies a value based on Date.now().
    "id"                 TEXT PRIMARY KEY DEFAULT replace(gen_random_uuid()::text, '-', ''),
    "rank"               BIGSERIAL,
    "slot"               TEXT NOT NULL,
    "version"            TEXT NOT NULL,
    "initialSchema"      JSON,  -- set after initial sync
    "initialSyncContext" JSON,
    "subscriberContext"  JSON
  );
  "#
    ))
}

/// The inputs `shard_setup` reads (mirrors `ShardConfig`; a local alias so the
/// signature is explicit about what it needs).
pub type ShardConfigInput = zero_cache_types::shards::ShardConfig;

/// Port of `internalShardConfigSchema` — the shard's persisted config row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalShardConfig {
    pub publications: Vec<String>,
    pub ddl_detection: bool,
}

/// A rejected requested publication (port of the two throws in
/// `setupTablesAndReplication`'s validation loop).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RequestedPublicationError {
    #[error(transparent)]
    Name(#[from] PublicationNameError),
    #[error(
        "Publication names starting with \"_\" are reserved for internal use.\n\
         Please use a different name for publication \"{0}\"."
    )]
    ReservedPrefix(String),
}

/// Port of `setupTablesAndReplication`'s requested-publication validation loop:
/// each requested publication must be a valid identifier and must not start
/// with `_` (that prefix is reserved for zero-cache's internal publications).
pub fn validate_requested_publications(
    publications: &[String],
) -> Result<(), RequestedPublicationError> {
    for pub_name in publications {
        validate_publication_name(pub_name)?;
        if pub_name.starts_with('_') {
            return Err(RequestedPublicationError::ReservedPrefix(pub_name.clone()));
        }
    }
    Ok(())
}

/// Errors from [`get_internal_shard_config`].
#[derive(Debug, thiserror::Error)]
pub enum InternalShardConfigError {
    #[error(transparent)]
    Shard(#[from] ShardError),
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error("Expected exactly one shardConfig row, got {0}")]
    WrongRowCount(usize),
}

/// Live port of `getInternalShardConfig`: reads the shard's `shardConfig`
/// singleton row (`publications`, `ddlDetection`). Errors if the row count is
/// not exactly one (matching upstream's assert).
pub async fn get_internal_shard_config(
    client: &tokio_postgres::Client,
    shard: &ShardId,
) -> Result<InternalShardConfig, InternalShardConfigError> {
    let schema = upstream_schema(shard)?;
    let rows = client
        .query(
            &format!(
                r#"SELECT "publications", "ddlDetection" FROM {}."shardConfig""#,
                id(&schema)
            ),
            &[],
        )
        .await?;
    if rows.len() != 1 {
        return Err(InternalShardConfigError::WrongRowCount(rows.len()));
    }
    let row = &rows[0];
    Ok(InternalShardConfig {
        publications: row.get("publications"),
        ddl_detection: row.get("ddlDetection"),
    })
}

/// Port of `setupTablesAndReplication`'s default-publication DDL: drops any
/// stale default publication and recreates it over all of `public`, publishing
/// through partition roots. Used when the caller requested no explicit
/// publications. Identifiers quoted via `sql::id`.
pub fn default_publication_ddl(app_id: &str, shard_num: i64) -> String {
    let default_pub = id(&default_publication_name(app_id, shard_num));
    format!(
        "DROP PUBLICATION IF EXISTS {default_pub};\n\
         CREATE PUBLICATION {default_pub}\n  \
         FOR TABLES IN SCHEMA public\n  \
         WITH (publish_via_partition_root = true);"
    )
}

/// Errors from [`setup_tables_and_replication`].
#[derive(Debug, thiserror::Error)]
pub enum SetupTablesError {
    #[error(transparent)]
    Requested(#[from] RequestedPublicationError),
    #[error(transparent)]
    ShardSetup(#[from] ShardSetupError),
    #[error(transparent)]
    Pg(#[from] crate::pg_connection::PgError),
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
    #[error("Unknown or invalid publications. Specified: [{specified}]. Found: [{found}]")]
    UnknownPublications { specified: String, found: String },
}

/// Live port of the core of `setupTablesAndReplication` (sans `setupTriggers`
/// and the replica-identity apply, which are separate follow-ups): validates
/// the requested publications, resolves the full publication set (verifying
/// requested ones exist, or creating a default `public`-schema publication when
/// none were requested), appends the internal metadata publication, and runs
/// `globalSetup` + `shardSetup` in a single transaction. Returns the full
/// publication list recorded in the shard's `shardConfig`.
///
/// NOT yet included (deferred, matching upstream's later steps): applying
/// `replica_identities_for_tables_without_primary_keys` and `setupTriggers`
/// (the event-trigger DDL for schema-change detection). A caller that needs
/// those runs them after this returns.
pub async fn setup_tables_and_replication(
    client: &tokio_postgres::Client,
    requested: &ShardConfigInput,
) -> Result<Vec<String>, SetupTablesError> {
    validate_requested_publications(&requested.publications)?;

    let mut all_publications: Vec<String>;
    if requested.publications.is_empty() {
        // No explicit publications: (re)create the default over all of public.
        client
            .batch_execute(&default_publication_ddl(
                &requested.app_id,
                requested.shard_num,
            ))
            .await?;
        all_publications = vec![default_publication_name(
            &requested.app_id,
            requested.shard_num,
        )];
    } else {
        // Verify every requested publication actually exists upstream.
        let found =
            crate::pg_connection::existing_publications(client, &requested.publications).await?;
        if found.len() != requested.publications.len() {
            return Err(SetupTablesError::UnknownPublications {
                specified: requested.publications.join(","),
                found: found.join(","),
            });
        }
        all_publications = requested.publications.clone();
    }

    let metadata_publication = metadata_publication_name(&requested.app_id, requested.shard_num);
    all_publications.push(metadata_publication.clone());

    let shard = ShardConfigInput {
        app_id: requested.app_id.clone(),
        shard_num: requested.shard_num,
        publications: all_publications.clone(),
    };

    // Guard against re-provisioning an already-set-up shard — e.g. a database
    // already managed by real zero, or a previous boot of this server. Upstream
    // runs setup through a VERSIONED MIGRATION framework (`ensureShardSchema` →
    // `runSchemaMigrations`) that no-ops when the schema is already at the
    // current version; the port's setup DDL faithfully matches upstream's and
    // is therefore NOT idempotent for the `shardConfig`/`replicas` tables (no
    // `IF NOT EXISTS`), so re-running it against an existing shard schema fails
    // with "relation already exists" and rolls back the whole transaction. If
    // the shard's `shardConfig` table already exists, the shard is provisioned;
    // skip the DDL and proceed to replication.
    {
        let shard_schema_name = upstream_schema(&ShardId {
            app_id: requested.app_id.clone(),
            shard_num: requested.shard_num,
        })
        .map_err(ShardSetupError::from)?;
        let existing = client
            .query_opt(
                "SELECT 1 FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name = $2",
                &[&shard_schema_name, &SHARD_CONFIG_TABLE],
            )
            .await?;
        if existing.is_some() {
            return Ok(all_publications);
        }
    }

    // globalSetup + shardSetup run together in one transaction, matching
    // upstream's single `sql.unsafe(globalSetup(shard) + shardSetup(shard, …))`.
    let app_id = AppId {
        app_id: requested.app_id.clone(),
    };
    let ddl = format!(
        "{}{}",
        global_setup(&app_id).map_err(ShardSetupError::from)?,
        shard_setup(&shard, &metadata_publication)?,
    );
    let txn = format!("BEGIN;\n{ddl}\nCOMMIT;");
    if let Err(e) = client.batch_execute(&txn).await {
        let _ = client.batch_execute("ROLLBACK").await;
        return Err(e.into());
    }

    Ok(all_publications)
}

/// A table (without a usable primary key) paired with the index chosen to be
/// its `REPLICA IDENTITY`. Port of the entries in
/// `replicaIdentitiesForTablesWithoutPrimaryKeys`'s `replicaIdentities` list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaIdentityChoice {
    pub schema: String,
    pub table_name: String,
    pub index_name: String,
}

/// Pure decision half of `replicaIdentitiesForTablesWithoutPrimaryKeys`: for
/// each published table that has no primary key and still uses the *default*
/// replica identity, pick the first published index that can serve as
/// `REPLICA IDENTITY USING INDEX` — it must be UNIQUE, immediate (not
/// deferrable), and over columns that are all NOT NULL. (Partial/expression
/// indexes are already excluded upstream by the introspection query, so they
/// never appear here.) Returns one choice per such table (empty if none).
pub fn replica_identities_for_tables_without_primary_keys(
    tables: &[zero_cache_types::specs::PublishedTableSpec],
    indexes: &[zero_cache_types::specs::PublishedIndexSpec],
) -> Vec<ReplicaIdentityChoice> {
    use zero_cache_types::specs::ReplicaIdentity;

    let mut out = Vec::new();
    for table in tables {
        let has_pk = table.primary_key.as_ref().is_some_and(|pk| !pk.is_empty());
        if has_pk || table.replica_identity != Some(ReplicaIdentity::Default) {
            continue;
        }
        // First suitable index for this table wins.
        for idx in indexes.iter().filter(|idx| {
            idx.schema == table.schema
                && idx.table_name == table.name
                && idx.unique
                && idx.is_immediate == Some(true)
        }) {
            // Every indexed column must be NOT NULL in the table.
            let all_not_null = idx.columns.iter().all(|(col, _)| {
                table
                    .columns
                    .iter()
                    .find(|(name, _)| name == col)
                    .is_some_and(|(_, spec)| spec.column.not_null == Some(true))
            });
            if !all_not_null {
                continue;
            }
            out.push(ReplicaIdentityChoice {
                schema: table.schema.clone(),
                table_name: table.name.clone(),
                index_name: idx.name.clone(),
            });
            break;
        }
    }
    out
}

/// Builds the `ALTER TABLE ... REPLICA IDENTITY USING INDEX ...` statement for
/// a [`ReplicaIdentityChoice`] (the live `apply` step's per-table SQL), with
/// identifiers quoted via `sql::id`.
pub fn replica_identity_alter_sql(choice: &ReplicaIdentityChoice) -> String {
    format!(
        "ALTER TABLE {}.{} REPLICA IDENTITY USING INDEX {}",
        id(&choice.schema),
        id(&choice.table_name),
        id(&choice.index_name),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(app: &str, num: i64) -> ShardId {
        ShardId {
            app_id: app.to_string(),
            shard_num: num,
        }
    }

    #[test]
    fn valid_publication_names_accepted() {
        assert!(validate_publication_name("zero_data").is_ok());
        assert!(validate_publication_name("_myapp_public_0").is_ok());
        assert!(validate_publication_name("A1_b2").is_ok());
    }

    #[test]
    fn invalid_publication_names_rejected() {
        // Starts with a digit.
        assert_eq!(
            validate_publication_name("1pub"),
            Err(PublicationNameError::Invalid("1pub".into()))
        );
        // Contains a dash.
        assert_eq!(
            validate_publication_name("my-pub"),
            Err(PublicationNameError::Invalid("my-pub".into()))
        );
        // Empty.
        assert_eq!(
            validate_publication_name(""),
            Err(PublicationNameError::Invalid("".into()))
        );
    }

    #[test]
    fn publication_name_length_limit() {
        let ok = "a".repeat(63);
        assert!(validate_publication_name(&ok).is_ok());
        let too_long = "a".repeat(64);
        assert_eq!(
            validate_publication_name(&too_long),
            Err(PublicationNameError::TooLong(too_long))
        );
    }

    #[test]
    fn slot_and_publication_names() {
        assert_eq!(internal_publication_prefix("zero"), "_zero_");
        assert_eq!(legacy_replication_slot("zero", 0), "zero_0");
        assert_eq!(
            replication_slot_prefix(&shard("zero", 0)).unwrap(),
            "zero_0_"
        );
        assert_eq!(default_publication_name("zero", 2), "_zero_public_2");
        assert_eq!(metadata_publication_name("zero", 2), "_zero_metadata_2");
    }

    #[test]
    fn replication_slot_expression_escapes_underscores() {
        // "zero_0_%" with each underscore escaped for LIKE.
        assert_eq!(
            replication_slot_expression(&shard("zero", 0)).unwrap(),
            "zero\\_0\\_%"
        );
    }

    #[test]
    fn replication_slot_prefix_validates_the_app_id() {
        // Uppercase is not allowed by ALLOWED_APP_ID_CHARACTERS (`^[a-z0-9_]+$`).
        assert!(replication_slot_prefix(&shard("Bad", 0)).is_err());
    }

    #[test]
    fn table_definitions_have_expected_primary_keys() {
        let clients = get_clients_table_definition(r#""zero_0""#);
        assert!(clients.contains(r#"CREATE TABLE "zero_0"."clients""#));
        assert!(clients.contains(r#"PRIMARY KEY("clientGroupID", "clientID")"#));
        let mutations = get_mutations_table_definition(r#""zero_0""#);
        assert!(mutations.contains(r#"CREATE TABLE "zero_0"."mutations""#));
        assert!(mutations.contains(r#"PRIMARY KEY("clientGroupID", "clientID", "mutationID")"#));
    }

    #[test]
    fn global_setup_is_idempotent_and_app_scoped() {
        let sql = global_setup(&AppId {
            app_id: "zero".into(),
        })
        .unwrap();
        // The app schema is quoted via `id`.
        assert!(sql.contains(r#"CREATE SCHEMA IF NOT EXISTS "zero";"#));
        assert!(sql.contains(r#"CREATE TABLE IF NOT EXISTS "zero".permissions"#));
        assert!(sql.contains(r#"CREATE OR REPLACE FUNCTION "zero".set_permissions_hash()"#));
        assert!(sql.contains("ON CONFLICT DO NOTHING"));
        // Invalid app id rejected.
        assert!(global_setup(&AppId {
            app_id: "Bad".into()
        })
        .is_err());
    }

    fn shard_config(app: &str, num: i64, pubs: &[&str]) -> ShardConfigInput {
        ShardConfigInput {
            app_id: app.into(),
            shard_num: num,
            publications: pubs.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn shard_setup_builds_schema_publication_and_config_row() {
        let meta = metadata_publication_name("zero", 0); // _zero_metadata_0
        let cfg = shard_config("zero", 0, &["_zero_public_0", &meta]);
        let sql = shard_setup(&cfg, &meta).unwrap();

        assert!(sql.contains(r#"CREATE SCHEMA IF NOT EXISTS "zero_0""#));
        assert!(sql.contains(r#"CREATE TABLE "zero_0"."clients""#));
        assert!(sql.contains(r#"CREATE PUBLICATION "_zero_metadata_0""#));
        assert!(
            sql.contains(r#""zero"."permissions""#),
            "metadata pub includes permissions"
        );
        assert!(sql.contains(r#"CREATE TABLE "zero_0"."shardConfig""#));
        // Publications inserted sorted and quoted as SQL literals.
        assert!(sql.contains("ARRAY['_zero_metadata_0', '_zero_public_0']"));
        assert!(sql.contains(r#"CREATE TABLE "zero_0".replicas"#));
    }

    #[test]
    fn shard_setup_requires_metadata_publication_present() {
        let cfg = shard_config("zero", 0, &["_zero_public_0"]);
        let err = shard_setup(&cfg, "_zero_metadata_0").unwrap_err();
        assert_eq!(
            err,
            ShardSetupError::MissingMetadataPublication("_zero_metadata_0".into())
        );
    }

    /// Live: execute the ported `global_setup` + `shard_setup` DDL against real
    /// Postgres, confirm the shard schema/config row/publication exist, then
    /// tear it all down with `drop_shard`. Validates the large DDL strings are
    /// well-formed end to end.
    #[tokio::test]
    async fn live_global_and_shard_setup_then_drop() {
        let conn_str = std::env::var("ZERO_TEST_PG")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into());
        let Ok(client) = crate::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        // Use a distinct app id and clean any prior run.
        let app = "zerotest";
        client.batch_execute(&drop_shard(app, 0)).await.ok();
        client
            .batch_execute(&format!(r#"DROP SCHEMA IF EXISTS "{app}" CASCADE;"#))
            .await
            .ok();

        let meta = metadata_publication_name(app, 0);
        let default_pub = default_publication_name(app, 0);
        // Note: we deliberately do NOT create the default (`_public_`)
        // publication here. `shard_setup` only *creates* the metadata
        // publication and records the config's publication *names* as text — it
        // doesn't require the default publication to exist. Creating a broad
        // `FOR TABLES IN SCHEMA public` publication would pollute other live
        // tests sharing this Postgres instance, so it's omitted.

        client
            .batch_execute(&global_setup(&AppId { app_id: app.into() }).unwrap())
            .await
            .unwrap();
        let cfg = shard_config(app, 0, &[&default_pub, &meta]);
        client
            .batch_execute(&shard_setup(&cfg, &meta).unwrap())
            .await
            .unwrap();

        // The shardConfig singleton row exists with the sorted publications.
        let row = client
            .query_one(
                &format!(r#"SELECT "publications", "ddlDetection" FROM "{app}_0"."shardConfig""#),
                &[],
            )
            .await
            .unwrap();
        let pubs: Vec<String> = row.get("publications");
        let mut expected = vec![default_pub.clone(), meta.clone()];
        expected.sort();
        assert_eq!(pubs, expected);
        assert!(!row.get::<_, bool>("ddlDetection"));

        // The metadata publication was created.
        let found =
            crate::pg_connection::existing_publications(&client, std::slice::from_ref(&meta))
                .await
                .unwrap();
        assert_eq!(found, vec![meta.clone()]);

        // getInternalShardConfig reads the same row back into a struct.
        let internal = get_internal_shard_config(&client, &shard(app, 0))
            .await
            .unwrap();
        assert_eq!(internal.publications, expected);
        assert!(!internal.ddl_detection);

        // Teardown drops publications + schema.
        client.batch_execute(&drop_shard(app, 0)).await.unwrap();
        client
            .batch_execute(&format!(r#"DROP SCHEMA IF EXISTS "{app}" CASCADE;"#))
            .await
            .unwrap();
        let gone = crate::pg_connection::existing_publications(&client, &[meta])
            .await
            .unwrap();
        assert!(
            gone.is_empty(),
            "drop_shard removed the metadata publication"
        );
    }

    #[test]
    fn validate_requested_publications_rejects_reserved_and_invalid() {
        // Ordinary app publications are fine.
        assert!(validate_requested_publications(&["zero_data".into(), "app_pub".into()]).is_ok());
        // Underscore-prefixed names are reserved.
        assert_eq!(
            validate_requested_publications(&["_internal".into()]),
            Err(RequestedPublicationError::ReservedPrefix(
                "_internal".into()
            ))
        );
        // Invalid identifier surfaces as a name error before the prefix check.
        assert!(matches!(
            validate_requested_publications(&["bad-name".into()]),
            Err(RequestedPublicationError::Name(_))
        ));
        // Empty list is trivially valid (the default-publication path).
        assert!(validate_requested_publications(&[]).is_ok());
    }

    mod replica_identity {
        use super::*;
        use std::collections::BTreeMap;
        use zero_cache_types::specs::{
            ColumnSpec, Direction, PublishedColumnSpec, PublishedIndexSpec, PublishedTableSpec,
            ReplicaIdentity,
        };

        fn col(name: &str, pos: i64, not_null: bool) -> (String, PublishedColumnSpec) {
            let mut spec = ColumnSpec::new("text", pos);
            spec.not_null = Some(not_null);
            (
                name.to_string(),
                PublishedColumnSpec {
                    column: spec,
                    type_oid: 25,
                },
            )
        }

        fn table(
            name: &str,
            pk: Option<Vec<&str>>,
            ri: ReplicaIdentity,
            cols: Vec<(String, PublishedColumnSpec)>,
        ) -> PublishedTableSpec {
            PublishedTableSpec {
                name: name.into(),
                schema: "public".into(),
                oid: 1,
                schema_oid: None,
                columns: cols,
                primary_key: pk.map(|v| v.iter().map(|s| s.to_string()).collect()),
                replica_identity: Some(ri),
                publications: BTreeMap::new(),
            }
        }

        fn index(
            name: &str,
            table: &str,
            unique: bool,
            immediate: bool,
            cols: &[&str],
        ) -> PublishedIndexSpec {
            PublishedIndexSpec {
                name: name.into(),
                table_name: table.into(),
                schema: "public".into(),
                unique,
                columns: cols
                    .iter()
                    .map(|c| (c.to_string(), Direction::Asc))
                    .collect(),
                is_replica_identity: None,
                is_primary_key: None,
                is_immediate: Some(immediate),
            }
        }

        #[test]
        fn picks_a_unique_immediate_all_not_null_index() {
            let tables = vec![table(
                "t",
                None,
                ReplicaIdentity::Default,
                vec![col("a", 1, true), col("b", 2, true)],
            )];
            let indexes = vec![index("t_ab", "t", true, true, &["a", "b"])];
            let choices = replica_identities_for_tables_without_primary_keys(&tables, &indexes);
            assert_eq!(choices.len(), 1);
            assert_eq!(choices[0].index_name, "t_ab");
            assert_eq!(
                replica_identity_alter_sql(&choices[0]),
                r#"ALTER TABLE "public"."t" REPLICA IDENTITY USING INDEX "t_ab""#
            );
        }

        #[test]
        fn skips_tables_with_a_primary_key_or_non_default_identity() {
            let with_pk = table(
                "t",
                Some(vec!["a"]),
                ReplicaIdentity::Default,
                vec![col("a", 1, true)],
            );
            let non_default = table("u", None, ReplicaIdentity::Full, vec![col("a", 1, true)]);
            let indexes = vec![
                index("t_a", "t", true, true, &["a"]),
                index("u_a", "u", true, true, &["a"]),
            ];
            let choices = replica_identities_for_tables_without_primary_keys(
                &[with_pk, non_default],
                &indexes,
            );
            assert!(choices.is_empty());
        }

        #[test]
        fn rejects_indexes_that_are_not_unique_immediate_or_all_not_null() {
            let tables = vec![table(
                "t",
                None,
                ReplicaIdentity::Default,
                vec![col("a", 1, true), col("nullable", 2, false)],
            )];
            // non-unique, deferred, and nullable-column indexes are all unsuitable.
            let indexes = vec![
                index("not_unique", "t", false, true, &["a"]),
                index("deferred", "t", true, false, &["a"]),
                index("has_null", "t", true, true, &["nullable"]),
            ];
            let choices = replica_identities_for_tables_without_primary_keys(&tables, &indexes);
            assert!(choices.is_empty(), "no suitable index -> no choice");
        }
    }

    #[test]
    fn default_publication_ddl_recreates_over_public() {
        let sql = default_publication_ddl("zero", 0);
        assert!(sql.contains(r#"DROP PUBLICATION IF EXISTS "_zero_public_0""#));
        assert!(sql.contains(r#"CREATE PUBLICATION "_zero_public_0""#));
        assert!(sql.contains("FOR TABLES IN SCHEMA public"));
        assert!(sql.contains("publish_via_partition_root = true"));
    }

    /// Live: drive `setup_tables_and_replication` through the *requested*-
    /// publication branch (avoids creating a broad public-schema publication
    /// that would pollute other live tests). Verifies the returned publication
    /// set and that the shard config was written and reads back.
    #[tokio::test]
    async fn live_setup_tables_and_replication_requested_branch() {
        let conn_str = std::env::var("ZERO_TEST_PG")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into());
        let Ok(client) = crate::pg_connection::connect(&conn_str).await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        let app = "zerotestreq";
        // Clean any prior run.
        client.batch_execute(&drop_shard(app, 0)).await.ok();
        client
            .batch_execute(&format!(r#"DROP SCHEMA IF EXISTS "{app}" CASCADE;"#))
            .await
            .ok();
        client
            .batch_execute(
                "DROP TABLE IF EXISTS strc_test CASCADE; \
                 CREATE TABLE strc_test(id int primary key); \
                 DROP PUBLICATION IF EXISTS strc_app_pub; \
                 CREATE PUBLICATION strc_app_pub FOR TABLE strc_test;",
            )
            .await
            .unwrap();

        let requested = ShardConfigInput {
            app_id: app.into(),
            shard_num: 0,
            publications: vec!["strc_app_pub".into()],
        };
        let all_pubs = setup_tables_and_replication(&client, &requested)
            .await
            .unwrap();
        let meta = metadata_publication_name(app, 0);
        assert_eq!(all_pubs, vec!["strc_app_pub".to_string(), meta.clone()]);

        // The config row reflects the sorted publication set.
        let internal = get_internal_shard_config(&client, &shard(app, 0))
            .await
            .unwrap();
        let mut expected = all_pubs.clone();
        expected.sort();
        assert_eq!(internal.publications, expected);

        // A requested publication that does not exist is rejected.
        let bad = ShardConfigInput {
            app_id: app.into(),
            shard_num: 1,
            publications: vec!["does_not_exist_pub".into()],
        };
        assert!(matches!(
            setup_tables_and_replication(&client, &bad).await,
            Err(SetupTablesError::UnknownPublications { .. })
        ));

        // Teardown.
        client.batch_execute(&drop_shard(app, 0)).await.unwrap();
        client
            .batch_execute(&format!(r#"DROP SCHEMA IF EXISTS "{app}" CASCADE;"#))
            .await
            .unwrap();
        client
            .batch_execute(
                "DROP PUBLICATION IF EXISTS strc_app_pub; DROP TABLE IF EXISTS strc_test;",
            )
            .await
            .unwrap();
    }

    #[test]
    fn drop_shard_drops_publications_then_schema() {
        let sql = drop_shard("zero", 0);
        assert!(sql.contains(r#"DROP PUBLICATION IF EXISTS "_zero_public_0""#));
        assert!(sql.contains(r#"DROP PUBLICATION IF EXISTS "_zero_metadata_0""#));
        assert!(sql.contains(r#"DROP SCHEMA IF EXISTS "zero_0" CASCADE"#));
        // Publications dropped before the schema.
        let pub_pos = sql.find("DROP PUBLICATION").unwrap();
        let schema_pos = sql.find("DROP SCHEMA").unwrap();
        assert!(pub_pos < schema_pos);
    }
}
