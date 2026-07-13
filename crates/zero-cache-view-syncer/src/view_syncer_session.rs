//! The first genuinely STATEFUL slice of `ViewSyncerService` itself —
//! previously every `view_syncer_lifecycle.rs`/`query_set_sync.rs`/
//! `row_set_signature.rs` piece was either a free function taking all its
//! state as parameters, or (after the "wire it" rounds) a free function
//! composed with another free function. Nothing in this port had yet
//! actually OWNED a `Cvr` loaded from real Postgres and used it to drive
//! decisions the way the real `ViewSyncerService` object does.
//!
//! `ViewSyncerSession` is a small, honest slice of that: it owns a loaded
//! `Cvr` plus the two per-connection pure state machines
//! (`view_syncer_lifecycle::KeepAlive`/`ThrashDetector`) and exposes
//! methods that delegate to the already-ported pure decision functions
//! against that owned state. This is NOT `ViewSyncerService` — no IVM
//! pipeline, no pokers, no query hydration/catchup, no connection-lock
//! machinery, no background auth maintenance timer. It's the connection-
//! lifecycle sliver: load a CVR, validate an incoming client's base
//! version against it, track keepalive/shutdown/thrashing state for the
//! connection's lifetime. The rest of `ViewSyncerService`'s orchestration
//! remains the real, substantial, unstarted gap this doc has named every
//! round it's been discussed.

use std::collections::{HashMap, HashSet};

use crate::cvr_store_pg::{
    load_cvr_with_attempts, LoadCvrError, LoadCvrRetryOutcome, LOAD_ATTEMPT_INTERVAL_MS,
    MAX_LOAD_ATTEMPTS,
};
use crate::cvr_types::{Cvr, QueryRecord, TtlClock};
use crate::cvr_version::NullableCvrVersion;
use crate::query_set_sync::{apply_forced_version_bump_if_needed, AddedQuery, ForcedBumpReason};
use crate::row_set_signature::detect_row_set_signature_drift;
use crate::view_syncer_lifecycle::{
    check_client_and_cvr_versions, check_shutdown_conditions, schedule_expire_eviction_delay,
    KeepAlive, ShutdownDecision, ThrashDetector, VersionCheckError,
};
use zero_cache_types::shards::ShardId;

/// The outcome of [`ViewSyncerSession::connect`], which exhausts the bounded
/// CVR-load retry loop internally
/// ([`load_cvr_with_attempts`](crate::cvr_store_pg::load_cvr_with_attempts)):
/// either a usable `ViewSyncerSession`, or the terminal `Reset` when the row
/// cache never caught up within the retry budget.
#[allow(clippy::large_enum_variant)]
pub enum ConnectOutcome {
    Session(ViewSyncerSession),
    /// Terminal: the CVR never caught up to its row version, so the client's
    /// CVR must be reset (upstream's `ClientNotFoundError`, which forces a
    /// client CVR reset). The caller must reset the client rather than
    /// retrying — surfacing this instead of looping forever is the fix for
    /// finding M5.
    Reset {
        version: String,
        rows_version: Option<String>,
    },
}

/// Owns one loaded `Cvr` plus its connection-lifetime state machines.
pub struct ViewSyncerSession {
    pub cvr: Cvr,
    keep_alive: KeepAlive,
    thrash: ThrashDetector,
    /// Port of `#ttlClockBase` — the wall-clock time `cvr.ttl_clock` was
    /// last computed at, so [`Self::advance_ttl_clock`] knows how much
    /// delta to apply. Scope simplification vs. upstream: upstream tracks
    /// `#ttlClock` as a separate field from `#cvr.ttlClock` (only
    /// reconciled at flush time); this session has no separate flush
    /// step yet, so `advance_ttl_clock` mutates `self.cvr.ttl_clock`
    /// directly — the session-as-CVR-holder design already established
    /// by every other method here.
    ttl_clock_base: f64,
}

impl ViewSyncerSession {
    /// Loads the CVR for `client_group_id` from real Postgres (via
    /// `cvr_store_pg::load_cvr`) and wraps it in a fresh session. Port of
    /// the CVR-loading half of `ViewSyncerService`'s connection setup —
    /// NOT the IVM-pipeline-hydration half that follows it upstream.
    /// `now` stands in for `Date.now()` at the moment `#ttlClockBase` is
    /// set alongside `#ttlClock = cvr.ttlClock` (view-syncer.ts
    /// ~line 453-454).
    pub async fn connect(
        client: &tokio_postgres::Client,
        shard: &ShardId,
        client_group_id: &str,
        task_id: &str,
        last_connect_time: f64,
        now: f64,
    ) -> Result<ConnectOutcome, LoadCvrError> {
        Self::connect_with_attempts(
            client,
            shard,
            client_group_id,
            task_id,
            last_connect_time,
            now,
            MAX_LOAD_ATTEMPTS,
            LOAD_ATTEMPT_INTERVAL_MS,
        )
        .await
    }

    /// [`Self::connect`] with the CVR-load retry budget exposed, so tests can
    /// drive the terminal-reset path without the production default of ten
    /// 500 ms waits. Runs the bounded retry loop
    /// ([`load_cvr_with_attempts`](crate::cvr_store_pg::load_cvr_with_attempts))
    /// and, when the row cache never catches up, surfaces the terminal
    /// `Reset` (rather than retrying forever) — the M5 fix.
    #[allow(clippy::too_many_arguments)]
    pub async fn connect_with_attempts(
        client: &tokio_postgres::Client,
        shard: &ShardId,
        client_group_id: &str,
        task_id: &str,
        last_connect_time: f64,
        now: f64,
        max_attempts: u32,
        interval_ms: u64,
    ) -> Result<ConnectOutcome, LoadCvrError> {
        match load_cvr_with_attempts(
            client,
            shard,
            client_group_id,
            task_id,
            last_connect_time,
            max_attempts,
            interval_ms,
        )
        .await?
        {
            LoadCvrRetryOutcome::Loaded(cvr) => Ok(ConnectOutcome::Session(ViewSyncerSession {
                cvr,
                keep_alive: KeepAlive::new(),
                thrash: ThrashDetector::new(),
                ttl_clock_base: now,
            })),
            LoadCvrRetryOutcome::Reset {
                version,
                rows_version,
            } => Ok(ConnectOutcome::Reset {
                version,
                rows_version,
            }),
        }
    }

    /// Port of `#getTTLClock`: advances the session's tracked `ttlClock`
    /// by the wall-clock delta since it was last computed, mutating both
    /// the clock (`self.cvr.ttl_clock` — see the struct doc on why this
    /// session folds `#ttlClock`/`#cvr.ttlClock` into one field) and its
    /// base. Panics if the advanced clock would exceed `now`, matching
    /// upstream's assert (a monotonic-clock invariant: the TTL clock must
    /// never run ahead of wall-clock time).
    pub fn advance_ttl_clock(&mut self, now: f64) -> TtlClock {
        let delta = now - self.ttl_clock_base;
        let ttl_clock = TtlClock::from_number(self.cvr.ttl_clock.0 + delta);
        assert!(
            ttl_clock.0 <= now,
            "ttlClock should be less than or equal to now"
        );
        self.cvr.ttl_clock = ttl_clock;
        self.ttl_clock_base = now;
        ttl_clock
    }

    /// Validates a client's claimed base version against this session's
    /// loaded CVR. Port of `checkClientAndCVRVersions`, now genuinely
    /// checked against a real loaded CVR rather than a hand-built test
    /// fixture.
    pub fn validate_client_version(
        &self,
        client_version: &NullableCvrVersion,
    ) -> Result<(), VersionCheckError> {
        check_client_and_cvr_versions(client_version, &self.cvr.version)
    }

    /// Port of `keepalive()`, now operating on this session's own
    /// `KeepAlive` state instead of a caller-owned one.
    pub fn keepalive(&mut self, active: bool, now: i64, keepalive_ms: i64) -> bool {
        self.keep_alive.keepalive(active, now, keepalive_ms)
    }

    /// Port of `#checkForShutdownConditionsInLock`'s decision, against
    /// this session's own keepalive state.
    pub fn check_shutdown(
        &self,
        client_count: usize,
        now: i64,
        keepalive_ms: i64,
    ) -> ShutdownDecision {
        check_shutdown_conditions(client_count, now, self.keep_alive_until(), keepalive_ms)
    }

    fn keep_alive_until(&self) -> i64 {
        self.keep_alive.keep_alive_until
    }

    /// Port of `#checkForThrashing`, against this session's own
    /// `ThrashDetector` state.
    pub fn check_for_thrashing(&mut self, query_id: &str, now: i64) -> bool {
        self.thrash.check_for_thrashing(query_id, now)
    }

    /// Port of `#scheduleExpireEviction`'s delay computation, against this
    /// session's own loaded `Cvr` — the second `view_syncer_lifecycle`
    /// piece wired into the session (alongside `KeepAlive`/`ThrashDetector`
    /// above), closing the gap flagged when `schedule_expire_eviction_delay`
    /// was first ported: it existed but nothing owned a `Cvr` to call it
    /// against.
    pub fn schedule_expire_eviction_delay(&self) -> Option<f64> {
        schedule_expire_eviction_delay(&self.cvr)
    }

    /// Port of the row-set-signature drift check, against this session's
    /// own loaded `Cvr` — looks up `query_id`'s stored signature (absent
    /// for an internal query, which has no `rowSetSignature` field at
    /// all) and compares it to `candidate_sig` via
    /// `row_set_signature::detect_row_set_signature_drift`. `None` if the
    /// query isn't in this CVR at all, or has nothing stored to compare
    /// against yet (both cases upstream skips the check for).
    pub fn detect_row_set_signature_drift(
        &self,
        query_id: &str,
        candidate_sig: u64,
    ) -> Option<bool> {
        let stored = match self.cvr.queries.get(query_id)? {
            QueryRecord::Client(q) => q.base.row_set_signature.as_deref(),
            QueryRecord::Custom(q) => q.base.row_set_signature.as_deref(),
            QueryRecord::Internal(_) => None,
        };
        detect_row_set_signature_drift(stored, candidate_sig)
    }

    /// Port of the "force a CVR version bump for same-hash rehydrated
    /// queries" decision, against this session's own loaded `Cvr` — the
    /// fourth `view_syncer_lifecycle`/`query_set_sync` piece wired into
    /// the session. Mutates `self.cvr.version` in place when a bump is
    /// forced (via `ensure_new_version` internally), matching how the
    /// real `CVRUpdater` mutates the CVR's version as a side effect of
    /// this decision.
    ///
    /// `current_db_state_version` stands in for
    /// `this.#pipelines.currentVersion()` — the live IVM pipeline's
    /// current replica state version, which this port doesn't have a real
    /// source for yet (no live pipeline object exists). A plain caller-
    /// supplied parameter, matching the established pattern used
    /// throughout this port for pieces of state a not-yet-built
    /// dependency would otherwise provide (e.g. `row_exists`/
    /// `existing_row` closures elsewhere in this crate).
    ///
    /// `drifted_query_ids` similarly stands in for the caller's own
    /// tracking of which queries [`Self::detect_row_set_signature_drift`]
    /// flagged this cycle — this method doesn't call that one internally,
    /// since upstream computes drift once per query during hydration and
    /// only aggregates the results into `driftedQueryIDs` afterward, a
    /// step this session doesn't drive on its own yet.
    pub fn apply_forced_version_bump(
        &mut self,
        add_queries: &[AddedQuery],
        remove_count: usize,
        current_db_state_version: i64,
        drifted_query_ids: &HashSet<String>,
    ) -> Option<ForcedBumpReason> {
        let cvr_query_transformation_hashes: HashMap<String, String> = self
            .cvr
            .queries
            .iter()
            .filter_map(|(id, q)| {
                let hash = match q {
                    QueryRecord::Client(q) => q.base.transformation_hash.as_ref(),
                    QueryRecord::Custom(q) => q.base.transformation_hash.as_ref(),
                    QueryRecord::Internal(q) => q.transformation_hash.as_ref(),
                };
                hash.map(|h| (id.clone(), h.clone()))
            })
            .collect();
        let cvr_state_version = state_version_as_i64(&self.cvr.version.state_version);
        let orig_version = self.cvr.version.clone();
        apply_forced_version_bump_if_needed(
            &cvr_query_transformation_hashes,
            add_queries,
            remove_count,
            current_db_state_version,
            cvr_state_version,
            drifted_query_ids,
            &orig_version,
            &mut self.cvr.version,
        )
    }
}

/// Decodes a CVR's lexi-encoded `stateVersion` string back to a plain
/// `i64` for comparison against a live DB state version. Falls back to `0`
/// on a malformed version rather than propagating an error — a CVR's own
/// `stateVersion` is always well-formed by construction (written by this
/// same codebase's `lexi_version` encoder), so a decode failure here would
/// indicate corruption this method isn't the right place to handle.
fn state_version_as_i64(state_version: &str) -> i64 {
    zero_cache_types::lexi_version::version_from_lexi(state_version)
        .ok()
        .and_then(|b| i64::try_from(b).ok())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_version::CvrVersion;

    fn test_conn_str() -> String {
        std::env::var("ZERO_TEST_PG_URL").unwrap_or_else(|_| {
            "host=/tmp/zc-pg-sock port=54329 user=postgres dbname=postgres".to_string()
        })
    }

    /// Live end-to-end: creates a real CVR schema, inserts an instance row
    /// directly (standing in for a prior flush), then drives a
    /// `ViewSyncerSession` through `connect` -> `validate_client_version`
    /// -> `keepalive` -> `check_shutdown` -> `check_for_thrashing` — the
    /// first proof that a REAL loaded CVR and this port's connection-
    /// lifecycle decision functions actually compose into one coherent
    /// session object, not just individually-tested pure functions.
    #[tokio::test]
    async fn a_real_loaded_cvr_drives_the_full_session_lifecycle() {
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&test_conn_str()).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "vssession".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"vssession_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }
        let s = "\"vssession_0/cvr\"";
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.instances (\"clientGroupID\", \"version\", \"lastActive\", \"replicaVersion\") VALUES ('cg1', '05', now(), 'rv1');"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!("INSERT INTO {s}.\"rowsVersion\" (\"clientGroupID\", \"version\") VALUES ('cg1', '05');"))
            .await
            .unwrap();

        let outcome =
            ViewSyncerSession::connect(&client, &shard, "cg1", "my-task", 1_000_000.0, 1_000_000.0)
                .await
                .unwrap();
        let ConnectOutcome::Session(mut session) = outcome else {
            panic!("expected a real Session")
        };
        assert_eq!(session.cvr.id, "cg1");

        // A client claiming to be ahead of the real loaded CVR must be
        // rejected as a stale base cookie — genuinely checked against
        // Postgres-sourced state, not a hand-built fixture.
        let ahead = Some(CvrVersion {
            state_version: "99".to_string(),
            config_version: None,
        });
        let err = session.validate_client_version(&ahead).unwrap_err();
        assert!(matches!(err, VersionCheckError::StaleBaseCookie(_)));

        // A client at or behind the CVR's version is accepted.
        assert!(session
            .validate_client_version(&Some(session.cvr.version.clone()))
            .is_ok());

        // Session-owned keepalive/shutdown/thrash state actually works
        // through the session, not a caller-managed struct.
        assert!(session.keepalive(true, 1000, 5000));
        assert_eq!(
            session.check_shutdown(0, 3000, 5000),
            ShutdownDecision::KeepAliveActive {
                retry_delay_ms: 5000
            }
        );
        assert_eq!(
            session.check_shutdown(1, 3000, 5000),
            ShutdownDecision::HasClients
        );
        assert!(!session.check_for_thrashing("q1", 0));

        // ttl_clock_base was set to 1_000_000.0 at connect() time (ttlClock
        // starts at 0 from the schema default); a later `now` advances the
        // session's own cvr.ttl_clock by the wall-clock delta.
        let advanced = session.advance_ttl_clock(1_000_500.0);
        assert_eq!(advanced.0, 500.0);
        assert_eq!(
            session.cvr.ttl_clock.0, 500.0,
            "advance_ttl_clock must actually mutate cvr.ttl_clock, not just return a value"
        );

        client
            .batch_execute("DROP SCHEMA \"vssession_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    /// M5: an `instances.version` that stays ahead of `rowsVersion.version`
    /// (the row cache never catches up) exhausts the bounded retry loop and
    /// surfaces the terminal `Reset` — NOT `RowsBehind` forever, and NOT a
    /// hung session. Driven through `connect_with_attempts` with a zero
    /// interval so the exhaustion path runs without real waiting.
    #[tokio::test]
    async fn connect_resets_when_rows_never_catch_up() {
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&test_conn_str()).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "vssession2".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"vssession2_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }
        let s = "\"vssession2_0/cvr\"";
        // instances.version ahead of rowsVersion.version -> RowsBehind on
        // every attempt, so the retry loop terminates in a Reset.
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.instances (\"clientGroupID\", \"version\", \"lastActive\", \"replicaVersion\") VALUES ('cg1', '05', now(), 'rv1');"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!("INSERT INTO {s}.\"rowsVersion\" (\"clientGroupID\", \"version\") VALUES ('cg1', '01');"))
            .await
            .unwrap();

        let outcome = ViewSyncerSession::connect_with_attempts(
            &client,
            &shard,
            "cg1",
            "my-task",
            1_000_000.0,
            1_000_000.0,
            3,
            0,
        )
        .await
        .unwrap();
        assert!(
            matches!(outcome, ConnectOutcome::Reset { .. }),
            "row cache never catching up must terminate in a Reset"
        );

        client
            .batch_execute("DROP SCHEMA \"vssession2_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    /// Live end-to-end: a real loaded CVR with an inactivated (TTL-bearing)
    /// desire and a query carrying a stored `rowSetSignature` drives both
    /// newly-wired session methods against genuinely Postgres-sourced
    /// state, not hand-built fixtures.
    #[tokio::test]
    async fn schedule_expire_eviction_and_drift_detection_use_the_real_loaded_cvr() {
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&test_conn_str()).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "vssession3".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"vssession3_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }
        let s = "\"vssession3_0/cvr\"";
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.instances (\"clientGroupID\", \"version\", \"lastActive\", \"ttlClock\", \"replicaVersion\") VALUES ('cg1', '05', now(), 1000, 'rv1');"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!("INSERT INTO {s}.\"rowsVersion\" (\"clientGroupID\", \"version\") VALUES ('cg1', '05');"))
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.clients (\"clientGroupID\", \"clientID\") VALUES ('cg1', 'c1');"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.queries (\"clientGroupID\", \"queryHash\", \"queryName\", \"queryArgs\", \
                 \"patchVersion\", \"internal\", \"rowSetSignature\") VALUES ('cg1', 'h1', 'myQuery', '[]', '05', false, '2a');"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.desires (\"clientGroupID\", \"clientID\", \"queryHash\", \"patchVersion\", \
                 \"ttlMs\", \"inactivatedAtMs\") VALUES ('cg1', 'c1', 'h1', '05', 500, 900);"
            ))
            .await
            .unwrap();

        let outcome =
            ViewSyncerSession::connect(&client, &shard, "cg1", "my-task", 1_000_000.0, 1_000_000.0)
                .await
                .unwrap();
        let ConnectOutcome::Session(mut session) = outcome else {
            panic!("expected a real Session")
        };

        // ttlClock=1000, inactivatedAt=900, ttl=500 -> evicts at 1400, so
        // delay = (1400 - 1000 + 50) = 450 (below MAX_TTL_MS).
        assert_eq!(session.schedule_expire_eviction_delay(), Some(450.0));

        // Stored signature "2a" = 42 decimal.
        assert_eq!(
            session.detect_row_set_signature_drift("h1", 42),
            Some(false)
        );
        assert_eq!(session.detect_row_set_signature_drift("h1", 99), Some(true));
        assert_eq!(
            session.detect_row_set_signature_drift("does-not-exist", 1),
            None
        );

        // h1 has no stored transformationHash yet (NULL column), so it
        // can't be "same-hash rehydrated" -> no forced bump.
        let orig_version = session.cvr.version.clone();
        let add_queries = vec![AddedQuery {
            id: "h1".to_string(),
            transformation_hash: "th1".to_string(),
        }];
        let drifted: HashSet<String> = ["h1".to_string()].into_iter().collect();
        let reason = session.apply_forced_version_bump(&add_queries, 0, 5, &drifted);
        assert_eq!(reason, None);
        assert_eq!(session.cvr.version, orig_version);

        client
            .batch_execute("DROP SCHEMA \"vssession3_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }

    /// Live end-to-end: a real loaded CVR whose query DOES have a stored
    /// `transformationHash` matching a re-executed add -> the session's
    /// `apply_forced_version_bump` genuinely forces (and applies) a CVR
    /// version bump against real Postgres-sourced state.
    #[tokio::test]
    async fn apply_forced_version_bump_actually_bumps_a_real_session_cvr() {
        let Ok(client) = zero_cache_change_source::pg_connection::connect(&test_conn_str()).await
        else {
            eprintln!("skipping: no local test Postgres available");
            return;
        };

        let shard = ShardId {
            app_id: "vssession4".into(),
            shard_num: 0,
        };
        client
            .batch_execute("DROP SCHEMA IF EXISTS \"vssession4_0/cvr\" CASCADE;")
            .await
            .unwrap();
        for stmt in crate::cvr_schema_sql::create_cvr_schema_statements(&shard).unwrap() {
            client.batch_execute(&stmt).await.unwrap();
        }
        let s = "\"vssession4_0/cvr\"";
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.instances (\"clientGroupID\", \"version\", \"lastActive\", \"replicaVersion\") VALUES ('cg1', '05', now(), 'rv1');"
            ))
            .await
            .unwrap();
        client
            .batch_execute(&format!("INSERT INTO {s}.\"rowsVersion\" (\"clientGroupID\", \"version\") VALUES ('cg1', '05');"))
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "INSERT INTO {s}.queries (\"clientGroupID\", \"queryHash\", \"queryName\", \"queryArgs\", \
                 \"patchVersion\", \"internal\", \"transformationHash\") VALUES ('cg1', 'h1', 'myQuery', '[]', '05', false, 'th1');"
            ))
            .await
            .unwrap();

        let outcome =
            ViewSyncerSession::connect(&client, &shard, "cg1", "my-task", 1_000_000.0, 1_000_000.0)
                .await
                .unwrap();
        let ConnectOutcome::Session(mut session) = outcome else {
            panic!("expected a real Session")
        };

        let orig_version = session.cvr.version.clone();
        // State version "05" decodes to 5 -> current_db_state_version=5
        // matches exactly, so trackQueries alone wouldn't bump.
        let add_queries = vec![AddedQuery {
            id: "h1".to_string(),
            transformation_hash: "th1".to_string(),
        }];
        let drifted: HashSet<String> = ["h1".to_string()].into_iter().collect();
        let reason = session.apply_forced_version_bump(&add_queries, 0, 5, &drifted);
        assert_eq!(reason, Some(ForcedBumpReason::RowSetSignatureDrift));
        assert_ne!(
            session.cvr.version, orig_version,
            "the session's own CVR version must actually be mutated"
        );

        client
            .batch_execute("DROP SCHEMA \"vssession4_0/cvr\" CASCADE;")
            .await
            .unwrap();
    }
}
