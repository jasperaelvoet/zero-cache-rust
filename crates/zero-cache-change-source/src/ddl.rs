//! Port of `change-source/pg/schema/ddl.ts` — the Postgres EVENT TRIGGER
//! machinery that streams DDL (schema changes) inline through the logical
//! replication stream via `pg_logical_emit_message`.
//!
//! This is the *install* half of upstream's `triggerSetup()`: it builds the
//! SQL that (1) installs the per-shard trigger *functions*
//! ([`create_event_function_statements`]) and (2) installs the
//! `ddl_command_start` / `ddl_command_end` EVENT TRIGGERS that invoke them
//! ([`create_event_trigger_statements`]). Once installed, every relevant DDL
//! statement emits a `ddlStart` / `ddlUpdate` / `schemaSnapshot` JSON message
//! on the `{appID}/{shardNum}/ddl` prefix, which arrives in-band in the
//! pgoutput stream (preserving exact commit ordering).
//!
//! Upstream keeps each shard's trigger/function stack fully self-contained
//! (functions live in the shard schema, event-trigger names are globally
//! namespaced with the app id + shard num) so shards can be upgraded
//! independently — this port keeps that isolation.
//!
//! ## What is and isn't ported here
//!
//! Ported (self-contained, this file + [`crate::shard_schema::setup_triggers`]):
//!   - the trigger-function + event-trigger install SQL,
//!   - flipping `shardConfig."ddlDetection"` to `true` when the triggers
//!     install successfully (degrading gracefully to `false` on insufficient
//!     privilege, exactly as upstream).
//!
//! NOT ported here (the *apply* half — deliberately out of scope, see the
//! module-level TODO in [`crate::shard_schema`]): decoding a captured
//! `{appID}/{shardNum}/ddl` logical message back into a `ReplicationEvent`
//! and translating its `previousSchema`/`schema` diff into an incremental
//! replica schema change. That belongs in the pgoutput → change translation
//! path (`pg_to_change.rs`) and the replica apply path
//! (`replication_apply.rs`), both owned by other agents. Until that lands, the
//! interim periodic published-schema-hash poll (Wave 1) remains the fallback
//! for detecting DML-less DDL, and the emitted messages are simply ignored by
//! the current change decoder.

use zero_cache_types::shards::{upstream_schema, ShardError, ShardId};
use zero_cache_types::sql::id;

use crate::published_schema::{literal_list, published_schema_query};
use crate::shard_schema::ShardConfigInput;

/// Sent in the `version` tag of `ddlStart`/`ddlUpdate` event messages so that
/// the message constructed by the upstream trigger function stays compatible
/// with the code that processes it. Port of `PROTOCOL_VERSION`.
pub const PROTOCOL_VERSION: i64 = 1;

/// `pg_advisory_xact_lock` key used to serialize DDL statements so correct
/// schema-change diffs can be computed. Port of `DDL_SERIALIZATION_LOCK`
/// (`0x3c6b8468f1bac0b0`). Formatting this `i64` with `{}` yields the same
/// decimal literal upstream's `BigInt` template interpolation produces.
const DDL_SERIALIZATION_LOCK: i64 = 0x3c6b_8468_f1ba_c0b0;

/// The command tags that fire the DDL event triggers. Port of `TAGS`.
pub const TAGS: [&str; 7] = [
    "CREATE TABLE",
    "ALTER TABLE",
    "CREATE INDEX",
    "DROP TABLE",
    "DROP INDEX",
    "ALTER PUBLICATION",
    "ALTER SCHEMA",
];

/// Port of `append(shardNum)`: appends `_{shard_num}` to `name` and quotes the
/// result as a valid identifier.
fn sharded(name: &str, shard_num: i64) -> String {
    id(&format!("{name}_{shard_num}"))
}

/// Port of `createEventFunctionStatements`: the per-shard trigger *function*
/// stack (context helper, published-schema snapshot table + accessor, the
/// `update_schemas` emitter, and the `emit_ddl_start`/`emit_ddl_end` event
/// trigger functions). Installed even when event triggers themselves are not
/// permitted, so `update_schemas()` can be invoked manually as a workaround.
pub fn create_event_function_statements(shard: &ShardConfigInput) -> Result<String, ShardError> {
    let app_id = &shard.app_id;
    let shard_num = shard.shard_num;
    let schema = id(&upstream_schema(&ShardId {
        app_id: shard.app_id.clone(),
        shard_num: shard.shard_num,
    })?);
    let specs_query = published_schema_query(&shard.publications);
    let pubs_literal = literal_list(&shard.publications);

    Ok(format!(
        r#"
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE OR REPLACE FUNCTION {schema}.get_trigger_context()
RETURNS record AS $$
DECLARE
  result record;
BEGIN
  SELECT COALESCE(current_query(), 'current_query() returned NULL') AS "query" into result;
  RETURN result;
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION {schema}.notice_ignore(reason TEXT, tag TEXT, target record)
RETURNS void AS $$
BEGIN
  RAISE NOTICE '{app_id}_{shard_num} ignoring % % %', reason, tag,
    COALESCE(row_to_json(target)::text, '');
END
$$ LANGUAGE plpgsql;


-- Note: DROP and CREATE to upgrade from v20 to v21 because the
-- return type has changed. This can be simplified to CREATE OR REPLACE
-- once 1.5.0 is rollback safe.
DROP FUNCTION IF EXISTS {schema}.schema_specs();
CREATE FUNCTION {schema}.schema_specs()
RETURNS JSON
STABLE
AS $$
  {specs_query}
$$ LANGUAGE sql;


-- Stores the most recent published schema
CREATE TABLE IF NOT EXISTS {schema}."publishedSchema" (
  current JSON,
  exists BOOL PRIMARY KEY DEFAULT true CHECK (exists)
);

INSERT INTO {schema}."publishedSchema" (current) VALUES ({schema}.schema_specs())
  ON CONFLICT (exists) DO
  UPDATE SET current = excluded.current;


CREATE OR REPLACE FUNCTION {schema}.update_schemas(event_type text, tag text, target record)
RETURNS void AS $$
DECLARE
  prev_schema_specs JSON;
  schema_specs JSON;
  message TEXT;
BEGIN
  SELECT current FROM {schema}."publishedSchema" INTO prev_schema_specs;
  SELECT {schema}.schema_specs() INTO schema_specs;

  IF prev_schema_specs::text != schema_specs::text THEN
    UPDATE {schema}."publishedSchema" SET current = schema_specs;
  ELSIF event_type = 'ddlStart' THEN
    -- ddlStart events are always be emitted to allow the zero-cache
    -- to track the context of the current command tag in the face of
    -- nested event triggers (e.g. start->start->end->end).
    prev_schema_specs = NULL;
  ELSIF event_type = 'ddlUpdate' THEN
    -- TODO: fold 'schemaSnapshot' into this condition too (i.e. make it "ELSE")
    -- when 1.5.0 is rollback safe. Until then, noop schemaSnapshots are sent
    -- for compatibility with 1.0.0 ~ 1.4.0.
    PERFORM {schema}.notice_ignore('noop', tag, target);
    RETURN;
  END IF;

  SELECT json_build_object(
    'type', event_type,
    'version', {PROTOCOL_VERSION},
    'previousSchema', prev_schema_specs,
    'schema', schema_specs,
    'event', json_build_object('tag', tag),
    'context', {schema}.get_trigger_context()
  ) INTO message;

  PERFORM pg_logical_emit_message(true, '{app_id}/{shard_num}/ddl', message);

  RAISE NOTICE 'Emitted {app_id}_{shard_num} % for % %', event_type, tag,
    COALESCE(row_to_json(target)::text, '');
END
$$ LANGUAGE plpgsql;


-- Hook/workaround to manually trigger replication of schema changes on DBs
-- that do not support/allow event triggers.
CREATE OR REPLACE FUNCTION {schema}.update_schemas()
RETURNS void AS $$
BEGIN
  PERFORM {schema}.update_schemas('schemaSnapshot', 'MANUAL', NULL);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION {schema}.emit_ddl_start()
RETURNS event_trigger AS $$
DECLARE
  schema_specs JSON;
  message TEXT;
BEGIN
  -- serialize DDL statements to compute correct schema change diffs
  PERFORM pg_advisory_xact_lock({DDL_SERIALIZATION_LOCK});
  PERFORM {schema}.update_schemas('ddlStart', TG_TAG, NULL);
END
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION {schema}.emit_ddl_end()
RETURNS event_trigger AS $$
DECLARE
  publications TEXT[];
  target RECORD;
  relevant RECORD;
  schema_specs JSON;
  message TEXT;
  event TEXT;
BEGIN
  publications := ARRAY[{pubs_literal}];

  SELECT objid, object_type, object_identity
    FROM pg_event_trigger_ddl_commands()
    LIMIT 1 INTO target;

  -- Filter DDL updates that are not relevant to the shard (i.e. publications) when possible.
  SELECT true INTO relevant;

  -- Note: ALTER TABLE statements may *remove* the table from the set of published
  --       tables, and there is no way to determine if the table "used to be" in the
  --       set. Thus, all ALTER TABLE statements must produce a ddl update, similar to
  --       any DROP * statement.
  IF (target.object_type = 'table' AND TG_TAG != 'ALTER TABLE')
     OR target.object_type = 'table column' THEN
    SELECT ns.nspname AS "schema", c.relname AS "name" FROM pg_class AS c
      JOIN pg_namespace AS ns ON c.relnamespace = ns.oid
      JOIN pg_publication_tables AS pb ON pb.schemaname = ns.nspname AND pb.tablename = c.relname
      WHERE c.oid = target.objid AND pb.pubname = ANY (publications)
      INTO relevant;

  ELSIF target.object_type = 'index' THEN
    SELECT ns.nspname AS "schema", c.relname AS "name" FROM pg_class AS c
      JOIN pg_namespace AS ns ON c.relnamespace = ns.oid
      JOIN pg_indexes as ind ON ind.schemaname = ns.nspname AND ind.indexname = c.relname
      JOIN pg_publication_tables AS pb ON pb.schemaname = ns.nspname AND pb.tablename = ind.tablename
      WHERE c.oid = target.objid AND pb.pubname = ANY (publications)
      INTO relevant;

  ELSIF target.object_type = 'publication relation' THEN
    SELECT pb.pubname FROM pg_publication_rel AS rel
      JOIN pg_publication AS pb ON pb.oid = rel.prpubid
      WHERE rel.oid = target.objid AND pb.pubname = ANY (publications)
      INTO relevant;

  ELSIF target.object_type = 'publication namespace' THEN
    SELECT pb.pubname FROM pg_publication_namespace AS ns
      JOIN pg_publication AS pb ON pb.oid = ns.pnpubid
      WHERE ns.oid = target.objid AND pb.pubname = ANY (publications)
      INTO relevant;

  ELSIF target.object_type = 'schema' THEN
    SELECT ns.nspname AS "schema", c.relname AS "name" FROM pg_class AS c
      JOIN pg_namespace AS ns ON c.relnamespace = ns.oid
      JOIN pg_publication_tables AS pb ON pb.schemaname = ns.nspname AND pb.tablename = c.relname
      WHERE ns.oid = target.objid AND pb.pubname = ANY (publications)
      INTO relevant;

  ELSIF target.object_type = 'publication' THEN
    SELECT 1 WHERE target.object_identity = ANY (publications)
      INTO relevant;

  -- no-op CREATE IF NOT EXIST statements
  ELSIF TG_TAG LIKE 'CREATE %' AND target.object_type IS NULL THEN
    relevant := NULL;
  END IF;

  IF relevant IS NULL THEN
    PERFORM {schema}.notice_ignore('irrelevant', TG_TAG, target);
    RETURN;
  END IF;

  IF TG_TAG = 'COMMENT' THEN
    -- Only make schemaSnapshots for COMMENT ON PUBLICATION
    IF target.object_type != 'publication' THEN
      PERFORM {schema}.notice_ignore('irrelevant', TG_TAG, target);
      RETURN;
    END IF;
    PERFORM {schema}.update_schemas('schemaSnapshot', TG_TAG, target);
  ELSE
    PERFORM {schema}.update_schemas('ddlUpdate', TG_TAG, target);
  END IF;

END
$$ LANGUAGE plpgsql;
"#
    ))
}

/// A precondition violation for [`create_event_trigger_statements`].
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum EventTriggerError {
    #[error(transparent)]
    Shard(#[from] ShardError),
    #[error("shard publications must be non-empty")]
    NoPublications,
}

/// Port of `createEventTriggerStatements`: installs the `ddl_command_start` and
/// `ddl_command_end` EVENT TRIGGERS (globally namespaced with app id + shard
/// num) and drops legacy per-tag functions/triggers. Asserts a non-empty
/// publication list, matching upstream.
pub fn create_event_trigger_statements(
    shard: &ShardConfigInput,
) -> Result<String, EventTriggerError> {
    if shard.publications.is_empty() {
        return Err(EventTriggerError::NoPublications);
    }
    let app_id = &shard.app_id;
    let shard_num = shard.shard_num;
    let schema = id(&upstream_schema(&ShardId {
        app_id: shard.app_id.clone(),
        shard_num: shard.shard_num,
    })?);

    let start_trigger = sharded(&format!("{app_id}_ddl_start"), shard_num);
    let end_trigger = sharded(&format!("{app_id}_ddl_end"), shard_num);
    let start_tags = literal_list(&TAGS[..]);
    let mut end_tag_vec: Vec<&str> = TAGS.to_vec();
    end_tag_vec.push("COMMENT");
    let end_tags = literal_list(&end_tag_vec[..]);

    let mut out = String::new();
    out.push_str(&drop_event_trigger_statements(app_id, shard_num));
    out.push_str(&format!(
        r#"
CREATE EVENT TRIGGER {start_trigger}
  ON ddl_command_start
  WHEN TAG IN ({start_tags})
  EXECUTE PROCEDURE {schema}.emit_ddl_start();

CREATE EVENT TRIGGER {end_trigger}
  ON ddl_command_end
  WHEN TAG IN ({end_tags})
  EXECUTE PROCEDURE {schema}.emit_ddl_end();
"#
    ));

    // Drop legacy functions / triggers.
    out.push_str(&format!(
        "DROP FUNCTION IF EXISTS {schema}.emit_ddl_end(text) CASCADE;"
    ));
    out.push_str(&format!(
        "DROP FUNCTION IF EXISTS {schema}.notice_ignore(text, record);"
    ));
    for tag in end_tag_vec {
        let tag_id = tag.to_lowercase().replacen(' ', "_", 1);
        out.push_str(&format!(
            "DROP FUNCTION IF EXISTS {schema}.emit_{tag_id}() CASCADE;"
        ));
    }
    Ok(out)
}

/// Port of `dropEventTriggerStatements`: drops the shard's two event triggers
/// (their names live in the global namespace).
pub fn drop_event_trigger_statements(app_id: &str, shard_id: i64) -> String {
    format!(
        r#"
    DROP EVENT TRIGGER IF EXISTS {};
    DROP EVENT TRIGGER IF EXISTS {};
  "#,
        id(&format!("{app_id}_ddl_start_{shard_id}")),
        id(&format!("{app_id}_ddl_end_{shard_id}")),
    )
}

/// Port of `triggerSetup`: the SAVEPOINT-scoped statements that install the
/// event triggers and, on success, flip `shardConfig."ddlDetection"` to `true`.
/// Run in a sub-transaction so a failure (e.g. missing superuser) can be
/// rolled back independently, leaving replication in degraded
/// (`ddlDetection = false`) mode.
pub fn trigger_setup(shard: &ShardConfigInput) -> Result<String, EventTriggerError> {
    let schema = id(&upstream_schema(&ShardId {
        app_id: shard.app_id.clone(),
        shard_num: shard.shard_num,
    })?);
    Ok(format!(
        "{}UPDATE {schema}.\"shardConfig\" SET \"ddlDetection\" = true;",
        create_event_trigger_statements(shard)?
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(app: &str, num: i64, pubs: &[&str]) -> ShardConfigInput {
        ShardConfigInput {
            app_id: app.into(),
            shard_num: num,
            publications: pubs.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn ddl_serialization_lock_is_the_upstream_bigint() {
        // 0x3c6b8468f1bac0b0 in decimal — the value JS interpolates from the
        // BigInt literal. Guards against a transcription error in the constant.
        assert_eq!(DDL_SERIALIZATION_LOCK, 4353719051050729648);
        assert!(DDL_SERIALIZATION_LOCK > 0, "must fit a positive bigint");
    }

    #[test]
    fn event_functions_reference_the_shard_schema_and_emit_prefix() {
        let sql = create_event_function_statements(&shard("zero", 0, &["_zero_public_0"])).unwrap();
        // Functions are namespaced in the shard schema.
        assert!(sql.contains(r#"CREATE SCHEMA IF NOT EXISTS "zero_0";"#));
        assert!(sql.contains(r#"CREATE OR REPLACE FUNCTION "zero_0".emit_ddl_start()"#));
        assert!(sql.contains(r#"CREATE OR REPLACE FUNCTION "zero_0".emit_ddl_end()"#));
        assert!(sql.contains(r#"CREATE OR REPLACE FUNCTION "zero_0".update_schemas("#));
        assert!(sql.contains(r#""zero_0"."publishedSchema""#));
        // Emits on the {appID}/{shardNum}/ddl logical-message prefix.
        assert!(sql.contains("pg_logical_emit_message(true, 'zero/0/ddl', message)"));
        // Serialization lock and protocol version are spliced in.
        assert!(sql.contains("pg_advisory_xact_lock(4353719051050729648)"));
        assert!(sql.contains("'version', 1,"));
        // Publication list drives both schema_specs() and the relevance ARRAY.
        assert!(sql.contains("ARRAY['_zero_public_0']"));
    }

    #[test]
    fn event_triggers_are_globally_namespaced_and_drop_legacy() {
        let sql = create_event_trigger_statements(&shard("zero", 2, &["_zero_public_2"])).unwrap();
        assert!(sql.contains(r#"DROP EVENT TRIGGER IF EXISTS "zero_ddl_start_2""#));
        assert!(sql.contains(r#"CREATE EVENT TRIGGER "zero_ddl_start_2""#));
        assert!(sql.contains(r#"CREATE EVENT TRIGGER "zero_ddl_end_2""#));
        assert!(sql.contains("ON ddl_command_start"));
        assert!(sql.contains("ON ddl_command_end"));
        assert!(sql.contains(r#"EXECUTE PROCEDURE "zero_2".emit_ddl_start()"#));
        // Start triggers on TAGS; end triggers on TAGS + COMMENT.
        assert!(sql.contains("'CREATE TABLE', 'ALTER TABLE'"));
        assert!(sql.contains("'ALTER SCHEMA', 'COMMENT'"));
        // Legacy per-tag functions are dropped (space -> underscore, first only).
        assert!(sql.contains(r#"DROP FUNCTION IF EXISTS "zero_2".emit_create_table() CASCADE;"#));
        assert!(sql.contains(r#"DROP FUNCTION IF EXISTS "zero_2".emit_comment() CASCADE;"#));
    }

    #[test]
    fn trigger_setup_flips_ddl_detection_true() {
        let sql = trigger_setup(&shard("zero", 0, &["_zero_public_0"])).unwrap();
        assert!(sql.contains(r#"CREATE EVENT TRIGGER "zero_ddl_start_0""#));
        assert!(sql.contains(r#"UPDATE "zero_0"."shardConfig" SET "ddlDetection" = true;"#));
    }

    #[test]
    fn create_event_triggers_requires_publications() {
        assert_eq!(
            create_event_trigger_statements(&shard("zero", 0, &[])),
            Err(EventTriggerError::NoPublications)
        );
        assert_eq!(
            trigger_setup(&shard("zero", 0, &[])),
            Err(EventTriggerError::NoPublications)
        );
    }

    #[test]
    fn drop_event_triggers_names_both_triggers() {
        let sql = drop_event_trigger_statements("zero", 3);
        assert!(sql.contains(r#"DROP EVENT TRIGGER IF EXISTS "zero_ddl_start_3""#));
        assert!(sql.contains(r#"DROP EVENT TRIGGER IF EXISTS "zero_ddl_end_3""#));
    }
}
