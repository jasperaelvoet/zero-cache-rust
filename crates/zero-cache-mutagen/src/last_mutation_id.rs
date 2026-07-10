//! Port of `mutagen.ts`'s `checkSchemaVersionAndIncrementLastMutationID`
//! (despite the name, it only does the last-mutation-id half — no schema-
//! version check exists in the current upstream source under that name)
//! and `services/mutagen/error.ts`'s `MutationAlreadyProcessedError`.
//!
//! Split into a pure decision function ([`check_mutation_id`]) and a SQL
//! text generator ([`get_upsert_last_mutation_id_sql`]), rather than one
//! function coupled to a live `postgres.js` transaction — the actual
//! `INSERT ... ON CONFLICT ... RETURNING` round-trip against a real
//! upstream connection is deferred with the rest of `MutagenService`'s
//! transaction orchestration (see `PORTING.md`).

use zero_cache_protocol::error_kind::ErrorKind;
use zero_cache_protocol::error_origin::ErrorOrigin;
use zero_cache_protocol::{ErrorBody, ProtocolError};
use zero_cache_types::sql::{id, lit};

/// Port of `MutationAlreadyProcessedError`: the received mutation id is
/// strictly less than the client's already-recorded last-mutation-id, i.e.
/// this mutation was already applied and should be silently ignored (not
/// double-counted, not treated as an error).
#[derive(Debug, Clone, PartialEq)]
pub struct MutationAlreadyProcessedError {
    pub client_id: String,
    pub received: i64,
    pub actual: i64,
}

impl std::fmt::Display for MutationAlreadyProcessedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Ignoring mutation from {} with ID {} as it was already processed. Expected: {}",
            self.client_id, self.received, self.actual
        )
    }
}

impl std::error::Error for MutationAlreadyProcessedError {}

/// The outcome of checking a received mutation id against the client's
/// current last-mutation-id after incrementing it. Port of the two `throw`
/// branches (plus the implicit success path) in
/// `checkSchemaVersionAndIncrementLastMutationID`.
#[derive(Debug, Clone, PartialEq)]
pub enum MutationIdCheck {
    /// `received == actual` (after increment) — proceed normally.
    Ok,
    /// `received < actual` — already processed; ignore.
    AlreadyProcessed(MutationAlreadyProcessedError),
    /// `received > actual` — out of order; a protocol error.
    Unexpected(ProtocolError),
}

/// Compares `received_mutation_id` (from the client) against
/// `last_mutation_id` (the client-group's tracked counter, AFTER the SQL
/// upsert has already incremented it — see [`get_upsert_last_mutation_id_sql`]).
/// Port of the comparison logic in
/// `checkSchemaVersionAndIncrementLastMutationID`.
pub fn check_mutation_id(
    client_id: &str,
    received_mutation_id: i64,
    last_mutation_id: i64,
) -> MutationIdCheck {
    use std::cmp::Ordering::*;
    match received_mutation_id.cmp(&last_mutation_id) {
        Less => MutationIdCheck::AlreadyProcessed(MutationAlreadyProcessedError {
            client_id: client_id.to_string(),
            received: received_mutation_id,
            actual: last_mutation_id,
        }),
        Greater => MutationIdCheck::Unexpected(ProtocolError::new(ErrorBody::new(
            ErrorKind::InvalidPush,
            format!(
                "Push contains unexpected mutation id {received_mutation_id} for client {client_id}. Expected mutation id {last_mutation_id}."
            ),
            Some(ErrorOrigin::ZeroCache),
        ))),
        Equal => MutationIdCheck::Ok,
    }
}

/// Generates the `INSERT ... ON CONFLICT ... DO UPDATE ... RETURNING`
/// upsert that atomically initializes-or-increments a client's
/// last-mutation-id counter. Port of the SQL template in
/// `checkSchemaVersionAndIncrementLastMutationID`; `upstream_schema` is the
/// already-resolved `{appID}_{shardNum}` schema name (from
/// `zero_cache_types::shards::upstream_schema`).
pub fn get_upsert_last_mutation_id_sql(
    upstream_schema: &str,
    client_group_id: &str,
    client_id: &str,
) -> String {
    format!(
        "INSERT INTO {}.clients as current (\"clientGroupID\", \"clientID\", \"lastMutationID\") \
         VALUES ({}, {}, 1) \
         ON CONFLICT (\"clientGroupID\", \"clientID\") \
         DO UPDATE SET \"lastMutationID\" = current.\"lastMutationID\" + 1 \
         RETURNING \"lastMutationID\"",
        id(upstream_schema),
        lit(client_group_id),
        lit(client_id),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_is_ok() {
        assert_eq!(check_mutation_id("c1", 5, 5), MutationIdCheck::Ok);
    }

    #[test]
    fn received_less_than_actual_is_already_processed() {
        let result = check_mutation_id("c1", 4, 5);
        assert_eq!(
            result,
            MutationIdCheck::AlreadyProcessed(MutationAlreadyProcessedError {
                client_id: "c1".into(),
                received: 4,
                actual: 5,
            })
        );
    }

    #[test]
    fn received_greater_than_actual_is_unexpected_protocol_error() {
        let result = check_mutation_id("c1", 6, 5);
        let MutationIdCheck::Unexpected(err) = result else {
            panic!("expected Unexpected")
        };
        assert_eq!(err.kind(), ErrorKind::InvalidPush);
        assert!(err.message().contains("unexpected mutation id 6"));
        assert!(err.message().contains("Expected mutation id 5"));
    }

    #[test]
    fn already_processed_error_display() {
        let err = MutationAlreadyProcessedError {
            client_id: "c1".into(),
            received: 4,
            actual: 5,
        };
        assert_eq!(
            err.to_string(),
            "Ignoring mutation from c1 with ID 4 as it was already processed. Expected: 5"
        );
    }

    /// Exact replay diagnostics from pinned upstream `zero/v1.7.0`.
    /// `actual` is the next expected ID observed inside the transaction; it is
    /// not incremented by replays themselves (the transaction rolls back in
    /// `apply_crud_mutation`). Thus a report of ID 100/expected 101 followed
    /// later by ID 89/expected 105 means four accepted IDs advanced the state
    /// in between, not that stale replays consumed IDs.
    #[test]
    fn replay_diagnostics_match_pinned_upstream_examples() {
        let MutationIdCheck::AlreadyProcessed(first) = check_mutation_id("cid", 100, 101) else {
            panic!("100 must be stale when 101 is expected")
        };
        assert_eq!(
            first.to_string(),
            "Ignoring mutation from cid with ID 100 as it was already processed. Expected: 101"
        );

        let MutationIdCheck::AlreadyProcessed(later) = check_mutation_id("cid", 89, 105) else {
            panic!("89 must be stale when 105 is expected")
        };
        assert_eq!(
            later.to_string(),
            "Ignoring mutation from cid with ID 89 as it was already processed. Expected: 105"
        );
    }

    #[test]
    fn upsert_sql_shape() {
        let sql = get_upsert_last_mutation_id_sql("app_0", "cg1", "client1");
        assert_eq!(
            sql,
            "INSERT INTO \"app_0\".clients as current (\"clientGroupID\", \"clientID\", \"lastMutationID\") \
             VALUES ('cg1', 'client1', 1) \
             ON CONFLICT (\"clientGroupID\", \"clientID\") \
             DO UPDATE SET \"lastMutationID\" = current.\"lastMutationID\" + 1 \
             RETURNING \"lastMutationID\""
        );
    }
}
