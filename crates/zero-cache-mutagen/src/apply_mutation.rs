//! The live transaction executor `orchestration.rs`'s module doc names as
//! deferred: "actually executing the returned SQL against a live transaction
//! ... remain[s] unported." This closes that gap — a real `tokio-postgres`
//! transaction that runs `checkSchemaVersionAndIncrementLastMutationID`
//! (`last_mutation_id::get_upsert_last_mutation_id_sql` +
//! `check_mutation_id`) followed by `plan_mutation_sql`'s statements, mirroring
//! `mutagen.ts`'s `processMutationWithTx`.
//!
//! Scope: this is one mutation, applied once, against a plain `BEGIN`/`COMMIT`
//! transaction, PLUS (see [`apply_crud_mutation_with_retry`]) the
//! `PG_SERIALIZATION_FAILURE` retry loop `orchestration.rs`'s module doc named
//! as deferred (`processMutation`'s `MAX_SERIALIZATION_ATTEMPTS` loop). Still
//! out of scope: write-authorization (the caller passes `authorized: bool`,
//! per `orchestration::plan_mutation_sql`'s existing boundary — this module
//! composes with `zero-cache-auth`'s authorizer the same way `orchestration.rs`
//! does, not by depending on it directly) and the custom-mutator path (CRUD
//! ops only, matching every function this module calls).

use tokio_postgres::error::SqlState;
use tokio_postgres::Client;

use crate::crud_ops::CrudOp;
use crate::last_mutation_id::{
    check_mutation_id, get_upsert_last_mutation_id_sql, MutationIdCheck,
};
use crate::orchestration::plan_mutation_sql;

/// Port of `MAX_SERIALIZATION_ATTEMPTS`.
pub const MAX_SERIALIZATION_ATTEMPTS: usize = 10;

/// The outcome of applying one mutation: which `MutationIdCheck` branch was
/// taken (and therefore whether the CRUD ops actually ran).
#[derive(Debug, Clone, PartialEq)]
pub struct ApplyResult {
    pub check: MutationIdCheck,
    /// The client's `lastMutationID` after the upsert (whatever `check` was
    /// computed from).
    pub last_mutation_id: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum ApplyMutationError {
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),
}

/// Applies one client's CRUD mutation in a single real Postgres transaction:
/// `BEGIN` -> upsert-and-check the client's last-mutation-id -> (if the check
/// says to proceed) run `plan_mutation_sql`'s statements -> `COMMIT`.
///
/// * [`MutationIdCheck::Ok`]: the mutation ID matched; the CRUD ops ran (if
///   `authorized` AND NOT `error_mode`) and the last-mutation-id increment is
///   committed either way (matching upstream: "confirm the mutation even
///   though it may have been blocked by the authorizer"). `error_mode` mirrors
///   `processMutationWithTx`'s `errorMode` flag — when true, `plan_mutation_sql`
///   returns no statements regardless of `authorized`, so the transaction only
///   confirms the mutation id (see [`apply_crud_mutation_with_retry`], which
///   drives this flag as its application-error fallback).
/// * [`MutationIdCheck::AlreadyProcessed`]: the ID was stale (a retried
///   push); NO ops run and the transaction rolls back the speculative
///   upsert. This is essential: a replay must not consume another mutation
///   ID. The returned `last_mutation_id` is the *provisional* expected ID from
///   the rolled-back upsert, used only to form the upstream-compatible replay
///   diagnostic; the durable counter remains unchanged.
/// * [`MutationIdCheck::Unexpected`]: an out-of-order ID — a real protocol
///   violation. This transaction is rolled back (nothing committed, including
///   the last-mutation-id upsert) and the error surfaces to the caller.
#[allow(clippy::too_many_arguments)]
pub async fn apply_crud_mutation(
    client: &mut Client,
    upstream_schema: &str,
    client_group_id: &str,
    client_id: &str,
    received_mutation_id: i64,
    ops: &[CrudOp],
    authorized: bool,
    error_mode: bool,
) -> Result<ApplyResult, ApplyMutationError> {
    let txn = client.transaction().await?;

    let upsert_sql = get_upsert_last_mutation_id_sql(upstream_schema, client_group_id, client_id);
    let row = txn.query_one(&upsert_sql, &[]).await?;
    let last_mutation_id: i64 = row.get("lastMutationID");

    let check = check_mutation_id(client_id, received_mutation_id, last_mutation_id);

    match &check {
        MutationIdCheck::Unexpected(_) => {
            txn.rollback().await?;
            return Ok(ApplyResult {
                check,
                last_mutation_id,
            });
        }
        MutationIdCheck::AlreadyProcessed(_) => {
            // The SQL upsert increments before the comparison, just as the
            // upstream transaction does. Upstream throws the replay exception
            // from inside that transaction, so it rolls back; committing here
            // would turn every stale replay into a new durable LMID and make
            // later diagnostics drift (100 -> expected 101, then 89 -> 102
            // rather than remaining 101). Preserve the exact rollback shape.
            txn.rollback().await?;
            return Ok(ApplyResult {
                check,
                last_mutation_id,
            });
        }
        MutationIdCheck::Ok => {}
    }

    for stmt in plan_mutation_sql(ops, error_mode, authorized) {
        txn.batch_execute(&stmt).await?;
    }
    txn.commit().await?;

    Ok(ApplyResult {
        check,
        last_mutation_id,
    })
}

/// What [`apply_crud_mutation_with_retry`] decided after the whole retry
/// policy ran.
#[derive(Debug, PartialEq)]
pub enum RetryOutcome {
    /// The mutation applied normally (or was a stale retry, silently
    /// ignored) — nothing to report to the app.
    Applied(ApplyResult),
    /// The mutation failed on its first (non-serialization-failure) attempt,
    /// was retried once in "error mode" (confirming the mutation id but
    /// skipping the CRUD ops), and that retry succeeded — the upstream policy
    /// assumes this was an application-level error (bad data, constraint
    /// violation, etc.) rather than something internal, and reports it to the
    /// client while still confirming the mutation. Port of `processMutation`'s
    /// `result` variable.
    AppError { result: ApplyResult, error: String },
}

#[derive(Debug, thiserror::Error)]
pub enum RetryError {
    /// An out-of-order mutation id (a real protocol violation) surfaced even
    /// through the error-mode retry — not something retrying fixes.
    #[error("out-of-order mutation id")]
    Protocol(MutationIdCheck),
    /// A non-serialization-failure error occurred even while already
    /// retrying in error mode — an internal failure, not an application
    /// error (matching upstream's `if (isProtocolError(e) || errorMode)
    /// throw e`).
    #[error("mutation failed even in error mode: {0}")]
    Internal(String),
    /// Kept hitting `PG_SERIALIZATION_FAILURE` for `MAX_SERIALIZATION_ATTEMPTS`
    /// attempts without making progress.
    #[error("exhausted {MAX_SERIALIZATION_ATTEMPTS} serialization-failure retries")]
    RetriesExhausted,
}

fn is_serialization_failure(e: &tokio_postgres::Error) -> bool {
    e.code() == Some(&SqlState::T_R_SERIALIZATION_FAILURE)
}

/// Port of `processMutation`'s retry loop around `processMutationWithTx`:
/// retries [`apply_crud_mutation`] up to [`MAX_SERIALIZATION_ATTEMPTS`] times,
/// implementing upstream's documented policy (see `mutagen.ts`'s long comment,
/// quoted in spirit here):
///
/// 1. A `PG_SERIALIZATION_FAILURE` (Postgres SQLSTATE `40001`, from a
///    `SERIALIZABLE`-isolation conflict) is retried — it counts against the
///    attempt budget but does not flip error mode.
/// 2. Any OTHER Postgres error on the FIRST attempt is ambiguous (could be an
///    application error, e.g. a constraint violation from bad client data, or
///    a real internal/network problem) — so the mutation is retried EXACTLY
///    ONCE in "error mode" (`apply_crud_mutation`'s ops are skipped; only the
///    mutation id is confirmed). If that retry succeeds, the original error is
///    assumed to have been an application error and is reported to the client
///    via [`RetryOutcome::AppError`] (the mutation itself is still confirmed —
///    "authorizer blocking a mutation is not an error"). This does NOT consume
///    a serialization-attempt slot (matches upstream's `i--`).
/// 3. An error mode retry that ALSO fails is a genuine internal failure —
///    returned as [`RetryError::Internal`], not silently swallowed.
/// 4. [`MutationIdCheck::AlreadyProcessed`]/[`MutationIdCheck::Ok`] (first try)
///    short-circuit immediately via [`RetryOutcome::Applied`].
/// 5. [`MutationIdCheck::Unexpected`] is a protocol violation, never retried.
#[allow(clippy::too_many_arguments)]
pub async fn apply_crud_mutation_with_retry(
    client: &mut Client,
    upstream_schema: &str,
    client_group_id: &str,
    client_id: &str,
    received_mutation_id: i64,
    ops: &[CrudOp],
    authorized: bool,
) -> Result<RetryOutcome, RetryError> {
    let mut error_mode = false;
    let mut first_error: Option<String> = None;
    let mut attempts = 0;

    while attempts < MAX_SERIALIZATION_ATTEMPTS {
        let result = apply_crud_mutation(
            client,
            upstream_schema,
            client_group_id,
            client_id,
            received_mutation_id,
            ops,
            authorized,
            error_mode,
        )
        .await;

        match result {
            Ok(applied) => match &applied.check {
                MutationIdCheck::Unexpected(_) => return Err(RetryError::Protocol(applied.check)),
                MutationIdCheck::AlreadyProcessed(_) => return Ok(RetryOutcome::Applied(applied)),
                MutationIdCheck::Ok => {
                    return Ok(match first_error {
                        // The error-mode retry succeeded: report the ORIGINAL
                        // error to the app (matching upstream: the confirmed
                        // mutation still surfaces its first failure).
                        Some(error) => RetryOutcome::AppError {
                            result: applied,
                            error,
                        },
                        None => RetryOutcome::Applied(applied),
                    });
                }
            },
            Err(ApplyMutationError::Postgres(e)) if is_serialization_failure(&e) => {
                attempts += 1;
                continue; // Retry — consumes a serialization-attempt slot.
            }
            Err(ApplyMutationError::Postgres(e)) => {
                if error_mode {
                    // Failed even while just confirming the mutation id —
                    // a genuine internal problem, not an application error.
                    return Err(RetryError::Internal(e.to_string()));
                }
                // First (non-serialization) failure: assume it might be an
                // application error and retry once in error mode. Does NOT
                // consume a serialization-attempt slot (upstream's `i--`).
                first_error = Some(e.to_string());
                error_mode = true;
            }
        }
    }

    Err(RetryError::RetriesExhausted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crud_ops::{DeleteOp, InsertOp, UpdateOp};
    use zero_cache_shared::bigint_json::JsonValue;

    fn conn_str() -> String {
        std::env::var("ZERO_TEST_PG")
            .unwrap_or_else(|_| "host=localhost port=54329 user=postgres dbname=postgres".into())
    }

    /// `get_insert_sql`/etc quote `table_name` as ONE identifier (no
    /// dot-splitting), so a schema-qualified `"schema.table"` string is
    /// invalid — the connection's `search_path` is how these tests reach a
    /// non-`public` schema with a plain table name, matching how this port's
    /// SQL layer expects `table_name` to already be resolved by the caller.
    async fn connect(search_path_schema: &str) -> Option<Client> {
        let (client, connection) = tokio_postgres::connect(&conn_str(), tokio_postgres::NoTls)
            .await
            .ok()?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("SET search_path = {search_path_schema}, public"))
            .await
            .ok()?;
        Some(client)
    }

    fn insert(table: &str, id_val: &str, title: &str) -> CrudOp {
        CrudOp::Insert(InsertOp {
            table_name: table.into(),
            primary_key: vec!["id".into()],
            value: vec![
                ("id".to_string(), JsonValue::String(id_val.into())),
                ("title".to_string(), JsonValue::String(title.into())),
            ],
        })
    }

    /// Live: applies a real INSERT through the full transaction path against
    /// real Postgres, verifies both the row landed AND the client's
    /// `lastMutationID` incremented — the two effects
    /// `checkSchemaVersionAndIncrementLastMutationID` + the CRUD ops are
    /// supposed to have, atomically, in one transaction.
    #[tokio::test]
    async fn live_applies_insert_and_increments_last_mutation_id() {
        let Some(mut client) = connect("mutagen_apply_test").await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS mutagen_apply_test CASCADE; \
                 CREATE SCHEMA mutagen_apply_test; \
                 CREATE TABLE mutagen_apply_test.clients ( \
                   \"clientGroupID\" TEXT, \"clientID\" TEXT, \"lastMutationID\" BIGINT, \
                   PRIMARY KEY(\"clientGroupID\", \"clientID\")); \
                 CREATE TABLE mutagen_apply_test.issue (id TEXT PRIMARY KEY, title TEXT);",
            )
            .await
            .unwrap();

        let result = apply_crud_mutation(
            &mut client,
            "mutagen_apply_test",
            "cg1",
            "c1",
            1, // first mutation from a fresh client -> expected id 1
            &[insert("issue", "1", "hello")],
            true,
            false,
        )
        .await
        .unwrap();

        assert_eq!(result.check, MutationIdCheck::Ok);
        assert_eq!(result.last_mutation_id, 1);

        let row = client
            .query_one(
                "SELECT title FROM mutagen_apply_test.issue WHERE id = '1'",
                &[],
            )
            .await
            .unwrap();
        let title: String = row.get(0);
        assert_eq!(title, "hello", "the real INSERT landed in real Postgres");

        let lmid_row = client
            .query_one(
                "SELECT \"lastMutationID\" FROM mutagen_apply_test.clients WHERE \"clientID\" = 'c1'",
                &[],
            )
            .await
            .unwrap();
        let lmid: i64 = lmid_row.get(0);
        assert_eq!(lmid, 1, "lastMutationID persisted in the same transaction");

        client
            .batch_execute("DROP SCHEMA mutagen_apply_test CASCADE;")
            .await
            .unwrap();
    }

    fn upsert(table: &str, id_val: &str, title: &str) -> CrudOp {
        CrudOp::Upsert(crate::crud_ops::UpsertOp {
            table_name: table.into(),
            primary_key: vec!["id".into()],
            value: vec![
                ("id".to_string(), JsonValue::String(id_val.into())),
                ("title".to_string(), JsonValue::String(title.into())),
            ],
        })
    }

    /// Live: an UPSERT CrudOp exercises BOTH branches of `ON CONFLICT DO UPDATE`
    /// against real Postgres — first insert (no conflict), then a second upsert
    /// of the same key that takes the conflict-update path.
    #[tokio::test]
    async fn live_applies_upsert_insert_then_conflict_update() {
        let Some(mut client) = connect("mutagen_up_test").await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS mutagen_up_test CASCADE; \
                 CREATE SCHEMA mutagen_up_test; \
                 CREATE TABLE mutagen_up_test.clients ( \
                   \"clientGroupID\" TEXT, \"clientID\" TEXT, \"lastMutationID\" BIGINT, \
                   PRIMARY KEY(\"clientGroupID\", \"clientID\")); \
                 CREATE TABLE mutagen_up_test.issue (id TEXT PRIMARY KEY, title TEXT);",
            )
            .await
            .unwrap();

        // First upsert: no existing row -> INSERT branch.
        apply_crud_mutation(
            &mut client,
            "mutagen_up_test",
            "cg1",
            "c1",
            1,
            &[upsert("issue", "1", "first")],
            true,
            false,
        )
        .await
        .unwrap();
        let title: String = client
            .query_one(
                "SELECT title FROM mutagen_up_test.issue WHERE id = '1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(title, "first", "upsert inserted the new row");

        // Second upsert of the same key -> ON CONFLICT DO UPDATE branch.
        apply_crud_mutation(
            &mut client,
            "mutagen_up_test",
            "cg1",
            "c1",
            2,
            &[upsert("issue", "1", "second")],
            true,
            false,
        )
        .await
        .unwrap();
        let rows = client
            .query(
                "SELECT title FROM mutagen_up_test.issue WHERE id = '1'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "still exactly one row (upsert, not double-insert)"
        );
        let title: String = rows[0].get(0);
        assert_eq!(title, "second", "upsert took the conflict-update branch");

        client
            .batch_execute("DROP SCHEMA mutagen_up_test CASCADE;")
            .await
            .unwrap();
    }

    fn update(table: &str, id_val: &str, title: &str) -> CrudOp {
        CrudOp::Update(UpdateOp {
            table_name: table.into(),
            primary_key: vec!["id".into()],
            value: vec![
                ("id".to_string(), JsonValue::String(id_val.into())),
                ("title".to_string(), JsonValue::String(title.into())),
            ],
        })
    }

    fn delete(table: &str, id_val: &str) -> CrudOp {
        CrudOp::Delete(DeleteOp {
            table_name: table.into(),
            primary_key: vec!["id".into()],
            value: std::collections::BTreeMap::from([(
                "id".to_string(),
                JsonValue::String(id_val.into()),
            )]),
        })
    }

    /// Live: applies UPDATE then DELETE CrudOps through the full transaction
    /// path against real Postgres — the update/delete op dispatch is only
    /// unit-tested at the SQL-string level otherwise. Each mutation increments
    /// the client's lastMutationID.
    #[tokio::test]
    async fn live_applies_update_then_delete() {
        let Some(mut client) = connect("mutagen_ud_test").await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS mutagen_ud_test CASCADE; \
                 CREATE SCHEMA mutagen_ud_test; \
                 CREATE TABLE mutagen_ud_test.clients ( \
                   \"clientGroupID\" TEXT, \"clientID\" TEXT, \"lastMutationID\" BIGINT, \
                   PRIMARY KEY(\"clientGroupID\", \"clientID\")); \
                 CREATE TABLE mutagen_ud_test.issue (id TEXT PRIMARY KEY, title TEXT);",
            )
            .await
            .unwrap();

        // Mutation 1: insert.
        apply_crud_mutation(
            &mut client,
            "mutagen_ud_test",
            "cg1",
            "c1",
            1,
            &[insert("issue", "1", "orig")],
            true,
            false,
        )
        .await
        .unwrap();

        // Mutation 2: update the title.
        let upd = apply_crud_mutation(
            &mut client,
            "mutagen_ud_test",
            "cg1",
            "c1",
            2,
            &[update("issue", "1", "updated")],
            true,
            false,
        )
        .await
        .unwrap();
        assert_eq!(upd.last_mutation_id, 2);
        let title: String = client
            .query_one(
                "SELECT title FROM mutagen_ud_test.issue WHERE id = '1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(title, "updated", "the UPDATE applied against live Postgres");

        // Mutation 3: delete the row.
        let del = apply_crud_mutation(
            &mut client,
            "mutagen_ud_test",
            "cg1",
            "c1",
            3,
            &[delete("issue", "1")],
            true,
            false,
        )
        .await
        .unwrap();
        assert_eq!(del.last_mutation_id, 3);
        let count: i64 = client
            .query_one("SELECT count(*) FROM mutagen_ud_test.issue", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 0, "the DELETE removed the row from live Postgres");

        client
            .batch_execute("DROP SCHEMA mutagen_ud_test CASCADE;")
            .await
            .unwrap();
    }

    /// Pinned-upstream regression: a stale (already-processed) mutation ID
    /// does NOT re-apply its ops OR advance the durable LMID. Upstream's
    /// `PushProcessor` asserts this exact behavior in its "previously seen
    /// mutation" Postgres test; rolling back the speculative upsert is what
    /// keeps all repeated stale messages reporting the same expected ID.
    #[tokio::test]
    async fn live_already_processed_mutation_is_ignored_not_reapplied() {
        let Some(mut client) = connect("mutagen_apply_test2").await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS mutagen_apply_test2 CASCADE; \
                 CREATE SCHEMA mutagen_apply_test2; \
                 CREATE TABLE mutagen_apply_test2.clients ( \
                   \"clientGroupID\" TEXT, \"clientID\" TEXT, \"lastMutationID\" BIGINT, \
                   PRIMARY KEY(\"clientGroupID\", \"clientID\")); \
                 CREATE TABLE mutagen_apply_test2.issue (id TEXT PRIMARY KEY, title TEXT);",
            )
            .await
            .unwrap();

        // First mutation succeeds, advancing lastMutationID to 1.
        apply_crud_mutation(
            &mut client,
            "mutagen_apply_test2",
            "cg1",
            "c1",
            1,
            &[insert("issue", "1", "first")],
            true,
            false,
        )
        .await
        .unwrap();

        // A retried push resends mutation id 1 (stale — already processed).
        // The speculative upsert observes an expected ID of 2, but it MUST be
        // rolled back: upstream treats this as a replay, not an accepted next
        // mutation.
        let result = apply_crud_mutation(
            &mut client,
            "mutagen_apply_test2",
            "cg1",
            "c1",
            1,
            &[insert("issue", "2", "should not land")],
            true,
            false,
        )
        .await
        .unwrap();
        assert!(matches!(result.check, MutationIdCheck::AlreadyProcessed(_)));

        // The second insert's row must NOT exist.
        let count_row = client
            .query_one("SELECT count(*) FROM mutagen_apply_test2.issue", &[])
            .await
            .unwrap();
        let count: i64 = count_row.get(0);
        assert_eq!(count, 1, "stale mutation's ops were not applied");

        let lmid: i64 = client
            .query_one(
                "SELECT \"lastMutationID\" FROM mutagen_apply_test2.clients \
                 WHERE \"clientGroupID\" = 'cg1' AND \"clientID\" = 'c1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            lmid, 1,
            "replaying id 1 must leave durable lastMutationID at 1 (next expected id remains 2)"
        );

        // A second stale replay must still see the same expected ID; it cannot
        // drift merely because the first stale request arrived.
        let second_replay = apply_crud_mutation(
            &mut client,
            "mutagen_apply_test2",
            "cg1",
            "c1",
            1,
            &[],
            true,
            false,
        )
        .await
        .unwrap();
        let MutationIdCheck::AlreadyProcessed(replay_error) = second_replay.check else {
            panic!("expected a second stale replay")
        };
        assert_eq!(
            replay_error.to_string(),
            "Ignoring mutation from c1 with ID 1 as it was already processed. Expected: 2"
        );

        let lmid_after_second_replay: i64 = client
            .query_one(
                "SELECT \"lastMutationID\" FROM mutagen_apply_test2.clients \
                 WHERE \"clientGroupID\" = 'cg1' AND \"clientID\" = 'c1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(lmid_after_second_replay, 1);

        client
            .batch_execute("DROP SCHEMA mutagen_apply_test2 CASCADE;")
            .await
            .unwrap();
    }

    /// Live: an out-of-order mutation ID (skipping ahead) is a protocol
    /// error and the WHOLE transaction rolls back — including the
    /// last-mutation-id upsert, which must not have advanced.
    #[tokio::test]
    async fn live_unexpected_mutation_id_rolls_back_the_whole_transaction() {
        let Some(mut client) = connect("mutagen_apply_test3").await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS mutagen_apply_test3 CASCADE; \
                 CREATE SCHEMA mutagen_apply_test3; \
                 CREATE TABLE mutagen_apply_test3.clients ( \
                   \"clientGroupID\" TEXT, \"clientID\" TEXT, \"lastMutationID\" BIGINT, \
                   PRIMARY KEY(\"clientGroupID\", \"clientID\")); \
                 CREATE TABLE mutagen_apply_test3.issue (id TEXT PRIMARY KEY, title TEXT);",
            )
            .await
            .unwrap();

        // Client's first mutation should be id 1; sending id 5 is out of order.
        let result = apply_crud_mutation(
            &mut client,
            "mutagen_apply_test3",
            "cg1",
            "c1",
            5,
            &[insert("issue", "1", "never lands")],
            true,
            false,
        )
        .await
        .unwrap();
        assert!(matches!(result.check, MutationIdCheck::Unexpected(_)));

        // Nothing committed: no row, no clients entry (the upsert itself
        // rolled back).
        let count_row = client
            .query_one("SELECT count(*) FROM mutagen_apply_test3.issue", &[])
            .await
            .unwrap();
        let count: i64 = count_row.get(0);
        assert_eq!(count, 0, "no rows: the whole transaction rolled back");
        let client_count_row = client
            .query_one("SELECT count(*) FROM mutagen_apply_test3.clients", &[])
            .await
            .unwrap();
        let client_count: i64 = client_count_row.get(0);
        assert_eq!(
            client_count, 0,
            "the last-mutation-id upsert also rolled back"
        );

        client
            .batch_execute("DROP SCHEMA mutagen_apply_test3 CASCADE;")
            .await
            .unwrap();
    }

    /// Live: an unauthorized mutation still commits (confirming the mutation
    /// / advancing last-mutation-id), but its CRUD ops do not run — matching
    /// upstream's "authorizer blocking a mutation is not an error."
    #[tokio::test]
    async fn live_unauthorized_mutation_confirms_but_does_not_apply_ops() {
        let Some(mut client) = connect("mutagen_apply_test4").await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS mutagen_apply_test4 CASCADE; \
                 CREATE SCHEMA mutagen_apply_test4; \
                 CREATE TABLE mutagen_apply_test4.clients ( \
                   \"clientGroupID\" TEXT, \"clientID\" TEXT, \"lastMutationID\" BIGINT, \
                   PRIMARY KEY(\"clientGroupID\", \"clientID\")); \
                 CREATE TABLE mutagen_apply_test4.issue (id TEXT PRIMARY KEY, title TEXT);",
            )
            .await
            .unwrap();

        let result = apply_crud_mutation(
            &mut client,
            "mutagen_apply_test4",
            "cg1",
            "c1",
            1,
            &[insert("issue", "1", "blocked")],
            false, // not authorized
            false,
        )
        .await
        .unwrap();

        assert_eq!(result.check, MutationIdCheck::Ok);
        assert_eq!(
            result.last_mutation_id, 1,
            "mutation confirmed despite being blocked"
        );
        let count_row = client
            .query_one("SELECT count(*) FROM mutagen_apply_test4.issue", &[])
            .await
            .unwrap();
        let count: i64 = count_row.get(0);
        assert_eq!(count, 0, "the blocked op did not run");

        client
            .batch_execute("DROP SCHEMA mutagen_apply_test4 CASCADE;")
            .await
            .unwrap();
    }

    /// Live: `apply_crud_mutation_with_retry`'s error-mode fallback. A mutation
    /// whose op violates a constraint (duplicate primary key) fails on the
    /// first attempt, is retried ONCE in error mode (which skips the ops but
    /// still confirms the mutation id), and returns `AppError` — the client's
    /// lastMutationID advances while the offending row is NOT written.
    #[tokio::test]
    async fn live_retry_error_mode_confirms_id_but_skips_failing_op() {
        let Some(mut client) = connect("mutagen_retry_test").await else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };
        client
            .batch_execute(
                "DROP SCHEMA IF EXISTS mutagen_retry_test CASCADE; \
                 CREATE SCHEMA mutagen_retry_test; \
                 CREATE TABLE mutagen_retry_test.clients ( \
                   \"clientGroupID\" TEXT, \"clientID\" TEXT, \"lastMutationID\" BIGINT, \
                   PRIMARY KEY(\"clientGroupID\", \"clientID\")); \
                 CREATE TABLE mutagen_retry_test.issue (id TEXT PRIMARY KEY, title TEXT);",
            )
            .await
            .unwrap();

        // Mutation 1: insert id=1 normally.
        apply_crud_mutation(
            &mut client,
            "mutagen_retry_test",
            "cg1",
            "c1",
            1,
            &[insert("issue", "1", "orig")],
            true,
            false,
        )
        .await
        .unwrap();

        // Mutation 2: insert id=1 AGAIN — a duplicate-key violation. The retry
        // loop should fall into error mode and return AppError.
        let outcome = apply_crud_mutation_with_retry(
            &mut client,
            "mutagen_retry_test",
            "cg1",
            "c1",
            2,
            &[insert("issue", "1", "dup")],
            true,
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, RetryOutcome::AppError { .. }),
            "duplicate-key mutation surfaces as an app error, got {outcome:?}"
        );

        // The mutation id was confirmed (advanced to 2)...
        let lmid: i64 = client
            .query_one(
                "SELECT \"lastMutationID\" FROM mutagen_retry_test.clients WHERE \"clientID\" = 'c1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(lmid, 2, "lastMutationID advanced despite the failed op");
        // ...but the failing op did NOT change the row (still 'orig', not 'dup').
        let title: String = client
            .query_one(
                "SELECT title FROM mutagen_retry_test.issue WHERE id = '1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            title, "orig",
            "the failing insert was skipped in error mode"
        );

        client
            .batch_execute("DROP SCHEMA mutagen_retry_test CASCADE;")
            .await
            .unwrap();
    }
}
