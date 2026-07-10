//! Port of a handful of pure decision functions from
//! `services/view-syncer/view-syncer.ts`'s `ViewSyncerService` — the first
//! slice of `ViewSyncerService` itself, which otherwise doesn't exist
//! anywhere in this port yet (a real, large remaining gap: ~2900 lines
//! orchestrating CVR flush/load, IVM pipeline sync, query hydration,
//! catchup, and connection lifecycle, all coupled to a live CVR store +
//! SQLite replica + WebSocket connections). This module ports the pieces
//! that are genuinely pure state machines independent of all of that:
//! `keepalive()`'s deadline tracking, `#checkForShutdownConditionsInLock`'s
//! post-flush decision, `#checkForThrashing`'s query-replacement-rate
//! detector, and the file-level standalone helper functions at the bottom
//! of `view-syncer.ts` (`contentsAndVersion`, `checkClientAndCVRVersions`,
//! `isTransformFailedError`, `expired`, `hasExpiredQueries`). `now`/
//! `ttl_clock` are explicit parameters everywhere (this port's determinism
//! convention) rather than an ambient `Date.now()`/`this.#getTTLClock()`.
//!
//! NOT ported (the actual remaining `ViewSyncerService` gap): CVR
//! snapshot/updater orchestration, IVM pipeline sync
//! (`#syncQueryPipelineSet`/`#addAndRemoveQueries`/`#advancePipelines`),
//! query hydration/catchup, auth maintenance/background retransform, and
//! the `run()`/connection-lock machinery gluing it all together — a real,
//! substantial future increment, not attempted here.

use std::collections::HashMap;

use zero_cache_protocol::error::{
    ErrorBody, ProtocolError, TransformFailedBody, TransformFailedReason,
};
use zero_cache_protocol::error_kind::ErrorKind;
use zero_cache_protocol::error_origin::ErrorOrigin;
use zero_cache_protocol::query_hash::to_base36;
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_types::error_with_level::{LogLevel, ProtocolErrorWithLevel};
use zero_cache_types::pg_to_lite::ZERO_VERSION_COLUMN_NAME;
use zero_cache_zql::ttl::{clamp_ttl, Ttl, MAX_TTL_MS};

use crate::cvr_eviction::next_eviction_time;
use crate::cvr_types::{Cvr, QueryRecord, TtlClock};
use crate::cvr_version::{
    cmp_versions, empty_cvr_version, version_to_cookie, CvrVersion, NullableCvrVersion,
};

/// Port of `TTL_TIMER_HYSTERESIS` (ms) — the small delay added to eviction
/// scheduling so multiple near-simultaneous evictions collapse into one
/// timer instead of firing separately.
pub const TTL_TIMER_HYSTERESIS: f64 = 50.0;

/// Port of `ViewSyncerService#keepalive`'s state:
/// `#keepAliveUntil`/`#keepaliveMs`. `active` stands in for upstream's
/// `this.#stateChanges.active` check (whether the underlying change stream
/// is still live) — a caller passes this in rather than this module owning
/// a `Subscription`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct KeepAlive {
    pub keep_alive_until: i64,
}

impl KeepAlive {
    pub fn new() -> Self {
        KeepAlive {
            keep_alive_until: 0,
        }
    }

    /// Port of `keepalive()`. Returns `true` (and pushes the deadline out
    /// by `keepalive_ms`) if `active`; returns `false` (leaving the
    /// deadline untouched) if the service is already shutting down.
    pub fn keepalive(&mut self, active: bool, now: i64, keepalive_ms: i64) -> bool {
        if !active {
            return false;
        }
        self.keep_alive_until = now + keepalive_ms;
        true
    }
}

/// Port of `#checkForShutdownConditionsInLock`'s decision, taken AFTER the
/// caller has already awaited `cvrStore.flushed()` (the one async
/// precondition this module doesn't model) and already knows the current
/// client count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownDecision {
    /// Clients are still connected — the common case, no shutdown check
    /// needed at all (upstream's early return before ever awaiting flush).
    HasClients,
    /// No clients, but still within the keepalive window — reschedule
    /// another check in `retry_delay_ms`.
    KeepAliveActive { retry_delay_ms: i64 },
    /// No clients and the keepalive window has passed — shut down.
    Shutdown,
}

/// Port of `#checkForShutdownConditionsInLock`'s logic (the part after the
/// `flushed()` await, whose result this function doesn't need — a caller
/// only calls this once flush has completed, matching upstream's
/// sequencing).
pub fn check_shutdown_conditions(
    client_count: usize,
    now: i64,
    keep_alive_until: i64,
    keepalive_ms: i64,
) -> ShutdownDecision {
    if client_count > 0 {
        return ShutdownDecision::HasClients;
    }
    if now <= keep_alive_until {
        return ShutdownDecision::KeepAliveActive {
            retry_delay_ms: keepalive_ms,
        };
    }
    ShutdownDecision::Shutdown
}

const THRASH_WINDOW_MS: i64 = 60_000;
const THRASH_THRESHOLD: u32 = 3;

struct ThrashRecord {
    count: u32,
    window_start: i64,
}

/// Port of `#queryReplacements`/`#checkForThrashing` — detects a query
/// being replaced (re-registered under a new auth context, typically) more
/// than `THRASH_THRESHOLD` times within a `THRASH_WINDOW_MS` sliding
/// window, which upstream logs a warning for ("clients with different auth
/// contexts connecting to the same client group").
#[derive(Default)]
pub struct ThrashDetector {
    replacements: HashMap<String, ThrashRecord>,
}

impl ThrashDetector {
    pub fn new() -> Self {
        ThrashDetector {
            replacements: HashMap::new(),
        }
    }

    /// Port of `#checkForThrashing`. Returns `true` exactly when upstream
    /// would log the thrashing warning (`record.count >= THRASH_THRESHOLD`
    /// within the window) — the caller decides how to surface that.
    pub fn check_for_thrashing(&mut self, query_id: &str, now: i64) -> bool {
        match self.replacements.get_mut(query_id) {
            None => {
                self.replacements.insert(
                    query_id.to_string(),
                    ThrashRecord {
                        count: 1,
                        window_start: now,
                    },
                );
                false
            }
            Some(record) if now - record.window_start > THRASH_WINDOW_MS => {
                self.replacements.insert(
                    query_id.to_string(),
                    ThrashRecord {
                        count: 1,
                        window_start: now,
                    },
                );
                false
            }
            Some(record) => {
                record.count += 1;
                record.count >= THRASH_THRESHOLD
            }
        }
    }
}

/// Port of `contentsAndVersion`: splits a replica row into its
/// `_0_version` (the row-version watermark column every replicated table
/// carries) and the rest of the row's columns. Errors if the column is
/// missing or empty, matching upstream's thrown `Error`.
pub fn contents_and_version(
    row: Vec<(String, JsonValue)>,
) -> Result<(Vec<(String, JsonValue)>, String), String> {
    let mut version = None;
    let mut contents = Vec::with_capacity(row.len());
    for (k, v) in row {
        if k == ZERO_VERSION_COLUMN_NAME {
            version = Some(v);
        } else {
            contents.push((k, v));
        }
    }
    match version {
        Some(JsonValue::String(s)) if !s.is_empty() => Ok((contents, s)),
        _ => Err(format!("Invalid {ZERO_VERSION_COLUMN_NAME} in row")),
    }
}

/// The outcome of [`check_client_and_cvr_versions`]'s two failure modes.
/// Port of the two `throw` sites in `checkClientAndCVRVersions`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionCheckError {
    /// CVR is empty but the client claims a later version — the CVR must
    /// have been deleted/never existed for this client. Port of the
    /// `ClientNotFoundError` throw.
    ClientNotFound,
    /// The client is ahead of a non-empty CVR — a stale/corrupted base
    /// cookie. Port of the `ProtocolError(InvalidConnectionRequestBaseCookie)`
    /// throw.
    StaleBaseCookie(ProtocolError),
}

/// Port of `checkClientAndCVRVersions`: validates a client's claimed base
/// version against the CVR's actual version before starting a sync,
/// catching two corruption/staleness cases up front.
pub fn check_client_and_cvr_versions(
    client: &NullableCvrVersion,
    cvr: &CvrVersion,
) -> Result<(), VersionCheckError> {
    let new_cvr_version = empty_cvr_version();
    if cmp_versions(&Some(cvr.clone()), &Some(new_cvr_version.clone())) == 0
        && cmp_versions(client, &Some(new_cvr_version)) > 0
    {
        return Err(VersionCheckError::ClientNotFound);
    }
    if cmp_versions(client, &Some(cvr.clone())) > 0 {
        let cvr_version_string = version_to_cookie(cvr).unwrap_or_default();
        return Err(VersionCheckError::StaleBaseCookie(ProtocolError::new(
            ErrorBody::new(
                ErrorKind::InvalidConnectionRequestBaseCookie,
                format!("CVR is at version {cvr_version_string}"),
                Some(ErrorOrigin::ZeroCache),
            ),
        )));
    }
    Ok(())
}

/// Port of `isAuthErrorBody`, narrowed to the one call site that survives
/// in this port (`isTransformFailedError`, which only ever calls it on a
/// `TransformFailedBody` — the generic `ErrorBody`/legacy-`PushError`
/// branches of the full upstream function have no caller here yet since
/// `PushFailedBody` isn't ported). Port of the `ZeroCacheHttp` +
/// `status in {401, 403}` branch.
fn is_transform_failed_auth_error(body: &TransformFailedBody) -> bool {
    matches!(body.reason, TransformFailedReason::ZeroCacheHttp { status, .. } if status == 401.0 || status == 403.0)
}

/// Port of `isTransformFailedError`: true for a `TransformFailed` body that
/// ISN'T itself an auth failure (auth failures are handled separately by
/// the caller, not treated as "the transform failed").
pub fn is_transform_failed_error(body: &TransformFailedBody) -> bool {
    !is_transform_failed_auth_error(body)
}

/// Port of `expired`: a query is expired only once EVERY client referencing
/// it has inactivated it and that inactivation's clamped TTL has elapsed.
/// Internal queries never expire.
pub fn expired(ttl_clock: TtlClock, query: &QueryRecord) -> bool {
    let client_state = match query {
        QueryRecord::Internal(_) => return false,
        QueryRecord::Client(q) => &q.base.client_state,
        QueryRecord::Custom(q) => &q.base.client_state,
    };
    for state in client_state.values() {
        let Some(inactivated_at) = state.inactivated_at else {
            return false;
        };
        let (clamped_ttl_ms, _) = clamp_ttl(&Ttl::Millis(state.ttl));
        if inactivated_at.0 + clamped_ttl_ms > ttl_clock.0 {
            return false;
        }
    }
    true
}

/// Port of `hasExpiredQueries`: true if ANY query in the CVR is expired.
pub fn has_expired_queries<'a>(
    ttl_clock: TtlClock,
    queries: impl Iterator<Item = &'a QueryRecord>,
) -> bool {
    queries.into_iter().any(|q| expired(ttl_clock, q))
}

/// WIRING: port of `#scheduleExpireEviction`'s pure delay computation,
/// composing `cvr_eviction::next_eviction_time` (already ported, but never
/// consumed anywhere in this port until now) with the hysteresis/clamping
/// math upstream applies before actually scheduling a timer. Returns
/// `None` when there's nothing to schedule (`nextEvictionTime` found no
/// inactive queries with a TTL — the caller should cancel any existing
/// timer, matching upstream's early return), or the delay in milliseconds
/// otherwise: `max(hysteresis, min(next - now + hysteresis, MAX_TTL_MS))`,
/// verbatim upstream's formula — collapsing near-simultaneous evictions
/// into one timer via the hysteresis padding, while never scheduling
/// further out than `MAX_TTL_MS` even if `next` is absurdly far away.
/// Actually starting a real timer (`#setTimeout`) is the caller's job, not
/// modeled here.
pub fn schedule_expire_eviction_delay(cvr: &Cvr) -> Option<f64> {
    let next = next_eviction_time(cvr)?;
    let delay =
        (next.0 - cvr.ttl_clock.0 + TTL_TIMER_HYSTERESIS).clamp(TTL_TIMER_HYSTERESIS, MAX_TTL_MS);
    Some(delay)
}

/// Port of `randomID`: a random base36 instance-tag id, used to
/// disambiguate multiple `ViewSyncerService` instances/logger contexts for
/// the same client group. `random_value` stands in for the ambient
/// `randInt(1, Number.MAX_SAFE_INTEGER)` call, matching this port's
/// determinism convention.
pub fn random_id(random_value: u64) -> String {
    to_base36(random_value)
}

/// Port of `shutdownBeforeInitializationError` — the fixed error a
/// `ViewSyncerService` method returns/throws when called before its
/// initialization completed.
pub fn shutdown_before_initialization_error() -> ProtocolErrorWithLevel {
    ProtocolErrorWithLevel::new(
        ErrorBody::new(
            ErrorKind::Internal,
            "shut down before initialization completed",
            Some(ErrorOrigin::ZeroCache),
        ),
        LogLevel::Warn,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_pushes_deadline_out_when_active() {
        let mut k = KeepAlive::new();
        assert!(k.keepalive(true, 1000, 5000));
        assert_eq!(k.keep_alive_until, 6000);
    }

    #[test]
    fn keepalive_returns_false_and_leaves_deadline_when_inactive() {
        let mut k = KeepAlive::new();
        k.keepalive(true, 1000, 5000);
        let before = k.keep_alive_until;
        assert!(!k.keepalive(false, 9000, 5000));
        assert_eq!(k.keep_alive_until, before);
    }

    #[test]
    fn shutdown_decision_has_clients_short_circuits() {
        assert_eq!(
            check_shutdown_conditions(1, 0, 0, 5000),
            ShutdownDecision::HasClients
        );
    }

    #[test]
    fn shutdown_decision_within_keepalive_window_reschedules() {
        assert_eq!(
            check_shutdown_conditions(0, 1000, 5000, 5000),
            ShutdownDecision::KeepAliveActive {
                retry_delay_ms: 5000
            }
        );
    }

    #[test]
    fn shutdown_decision_past_keepalive_window_shuts_down() {
        assert_eq!(
            check_shutdown_conditions(0, 6000, 5000, 5000),
            ShutdownDecision::Shutdown
        );
    }

    #[test]
    fn shutdown_decision_exactly_at_deadline_still_keeps_alive() {
        // Port of upstream's `<=` comparison.
        assert_eq!(
            check_shutdown_conditions(0, 5000, 5000, 5000),
            ShutdownDecision::KeepAliveActive {
                retry_delay_ms: 5000
            }
        );
    }

    #[test]
    fn thrash_detector_first_seen_query_never_warns() {
        let mut d = ThrashDetector::new();
        assert!(!d.check_for_thrashing("q1", 0));
    }

    #[test]
    fn thrash_detector_warns_at_threshold_within_window() {
        let mut d = ThrashDetector::new();
        assert!(!d.check_for_thrashing("q1", 0)); // count=1
        assert!(!d.check_for_thrashing("q1", 1000)); // count=2
        assert!(d.check_for_thrashing("q1", 2000)); // count=3 -> warn
    }

    #[test]
    fn thrash_detector_resets_outside_window() {
        let mut d = ThrashDetector::new();
        d.check_for_thrashing("q1", 0);
        d.check_for_thrashing("q1", 1000);
        // Well past THRASH_WINDOW_MS since window_start=0.
        assert!(
            !d.check_for_thrashing("q1", 70_000),
            "window reset should not warn on the first replacement of a new window"
        );
    }

    #[test]
    fn thrash_detector_tracks_queries_independently() {
        let mut d = ThrashDetector::new();
        d.check_for_thrashing("q1", 0);
        d.check_for_thrashing("q1", 100);
        assert!(
            !d.check_for_thrashing("q2", 100),
            "a different queryID should have its own independent counter"
        );
    }

    use crate::cvr_types::{
        ClientQueryRecord, ClientQueryState, ExternalQueryBase, InternalQueryRecord,
    };
    use std::collections::BTreeMap;
    use zero_cache_protocol::error_reason::ErrorReason;

    #[test]
    fn contents_and_version_splits_off_the_version_column() {
        let row = vec![
            ("id".to_string(), JsonValue::Number(1.0)),
            (
                ZERO_VERSION_COLUMN_NAME.to_string(),
                JsonValue::String("ab".to_string()),
            ),
        ];
        let (contents, version) = contents_and_version(row).unwrap();
        assert_eq!(contents, vec![("id".to_string(), JsonValue::Number(1.0))]);
        assert_eq!(version, "ab");
    }

    #[test]
    fn contents_and_version_errors_on_missing_or_empty_version() {
        assert!(contents_and_version(vec![("id".to_string(), JsonValue::Number(1.0))]).is_err());
        assert!(contents_and_version(vec![(
            ZERO_VERSION_COLUMN_NAME.to_string(),
            JsonValue::String("".to_string())
        )])
        .is_err());
    }

    #[test]
    fn check_client_and_cvr_versions_allows_client_at_or_behind_cvr() {
        let cvr = empty_cvr_version();
        assert!(check_client_and_cvr_versions(&None, &cvr).is_ok());
        assert!(check_client_and_cvr_versions(&Some(cvr.clone()), &cvr).is_ok());
    }

    #[test]
    fn check_client_and_cvr_versions_rejects_client_ahead_of_empty_cvr_as_not_found() {
        let cvr = empty_cvr_version();
        let ahead = CvrVersion {
            state_version: "99".to_string(),
            config_version: None,
        };
        let err = check_client_and_cvr_versions(&Some(ahead), &cvr).unwrap_err();
        assert_eq!(err, VersionCheckError::ClientNotFound);
    }

    #[test]
    fn check_client_and_cvr_versions_rejects_client_ahead_of_nonempty_cvr_as_stale_cookie() {
        let cvr = CvrVersion {
            state_version: "05".to_string(),
            config_version: None,
        };
        let ahead = CvrVersion {
            state_version: "99".to_string(),
            config_version: None,
        };
        let err = check_client_and_cvr_versions(&Some(ahead), &cvr).unwrap_err();
        assert!(matches!(err, VersionCheckError::StaleBaseCookie(_)));
    }

    fn transform_failed(reason: TransformFailedReason) -> TransformFailedBody {
        TransformFailedBody {
            reason,
            query_ids: vec![],
            message: "boom".to_string(),
            details: None,
        }
    }

    #[test]
    fn transform_failed_error_is_true_for_non_auth_failures() {
        assert!(is_transform_failed_error(&transform_failed(
            TransformFailedReason::ZeroCacheOther(ErrorReason::Internal)
        )));
    }

    #[test]
    fn transform_failed_error_is_false_for_401_or_403() {
        assert!(!is_transform_failed_error(&transform_failed(
            TransformFailedReason::ZeroCacheHttp {
                status: 401.0,
                body_preview: None
            }
        )));
        assert!(!is_transform_failed_error(&transform_failed(
            TransformFailedReason::ZeroCacheHttp {
                status: 403.0,
                body_preview: None
            }
        )));
        assert!(is_transform_failed_error(&transform_failed(
            TransformFailedReason::ZeroCacheHttp {
                status: 500.0,
                body_preview: None
            }
        )));
    }

    fn client_query(client_state: BTreeMap<String, ClientQueryState>) -> QueryRecord {
        QueryRecord::Client(ClientQueryRecord {
            base: ExternalQueryBase {
                id: "q1".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
                client_state,
                patch_version: None,
            },
            ast: zero_cache_protocol::ast::Ast::table("t"),
        })
    }

    #[test]
    fn internal_queries_never_expire() {
        let q = QueryRecord::Internal(InternalQueryRecord {
            id: "internal".to_string(),
            transformation_hash: None,
            transformation_version: None,
            row_set_signature: None,
            ast: zero_cache_protocol::ast::Ast::table("t"),
        });
        assert!(!expired(TtlClock::from_number(1_000_000.0), &q));
    }

    #[test]
    fn a_query_with_an_active_client_never_expires() {
        let mut states = BTreeMap::new();
        states.insert(
            "c1".to_string(),
            ClientQueryState {
                inactivated_at: None,
                ttl: 1000.0,
                deleted: false,
                version: empty_cvr_version(),
            },
        );
        assert!(!expired(
            TtlClock::from_number(1_000_000.0),
            &client_query(states)
        ));
    }

    #[test]
    fn a_query_expires_once_every_client_has_inactivated_it_past_its_ttl() {
        let mut states = BTreeMap::new();
        states.insert(
            "c1".to_string(),
            ClientQueryState {
                inactivated_at: Some(TtlClock::from_number(1000.0)),
                ttl: 500.0,
                deleted: true,
                version: empty_cvr_version(),
            },
        );
        // Not yet expired: inactivatedAt(1000) + ttl(500) = 1500 > now(1400).
        assert!(!expired(
            TtlClock::from_number(1400.0),
            &client_query(states.clone())
        ));
        // Expired: 1500 <= now(1600).
        assert!(expired(
            TtlClock::from_number(1600.0),
            &client_query(states)
        ));
    }

    #[test]
    fn a_query_does_not_expire_while_any_single_client_still_has_it_active() {
        let mut states = BTreeMap::new();
        states.insert(
            "c1".to_string(),
            ClientQueryState {
                inactivated_at: Some(TtlClock::from_number(0.0)),
                ttl: 100.0,
                deleted: true,
                version: empty_cvr_version(),
            },
        );
        states.insert(
            "c2".to_string(),
            ClientQueryState {
                inactivated_at: None,
                ttl: 100.0,
                deleted: false,
                version: empty_cvr_version(),
            },
        );
        assert!(
            !expired(TtlClock::from_number(1_000_000.0), &client_query(states)),
            "c2 is still active, so the query as a whole must not be expired"
        );
    }

    #[test]
    fn has_expired_queries_is_true_if_any_query_is_expired() {
        let mut expired_state = BTreeMap::new();
        expired_state.insert(
            "c1".to_string(),
            ClientQueryState {
                inactivated_at: Some(TtlClock::from_number(0.0)),
                ttl: 0.0,
                deleted: true,
                version: empty_cvr_version(),
            },
        );
        let mut active_state = BTreeMap::new();
        active_state.insert(
            "c1".to_string(),
            ClientQueryState {
                inactivated_at: None,
                ttl: 100.0,
                deleted: false,
                version: empty_cvr_version(),
            },
        );

        let queries = [client_query(active_state), client_query(expired_state)];
        assert!(has_expired_queries(
            TtlClock::from_number(1_000_000.0),
            queries.iter()
        ));

        let all_active = [client_query(BTreeMap::from([(
            "c1".to_string(),
            ClientQueryState {
                inactivated_at: None,
                ttl: 100.0,
                deleted: false,
                version: empty_cvr_version(),
            },
        )]))];
        assert!(!has_expired_queries(
            TtlClock::from_number(1_000_000.0),
            all_active.iter()
        ));
    }

    fn make_cvr(ttl_clock_ms: f64, queries: Vec<(&str, QueryRecord)>) -> Cvr {
        Cvr {
            id: "cvr1".into(),
            version: empty_cvr_version(),
            last_active: 0.0,
            ttl_clock: TtlClock::from_number(ttl_clock_ms),
            replica_version: None,
            clients: BTreeMap::new(),
            queries: queries
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            client_schema: None,
            profile_id: None,
        }
    }

    #[test]
    fn schedule_expire_eviction_delay_is_none_when_no_inactive_queries_have_a_ttl() {
        let cvr = make_cvr(0.0, vec![]);
        assert_eq!(schedule_expire_eviction_delay(&cvr), None);
    }

    #[test]
    fn schedule_expire_eviction_delay_adds_hysteresis() {
        // Inactivated at t=1000 with a 500ms ttl -> evicts at t=1500.
        let mut states = BTreeMap::new();
        states.insert(
            "c1".to_string(),
            ClientQueryState {
                inactivated_at: Some(TtlClock::from_number(1000.0)),
                ttl: 500.0,
                deleted: true,
                version: empty_cvr_version(),
            },
        );
        let cvr = make_cvr(1000.0, vec![("q1", client_query(states))]);
        // next(1500) - now(1000) + hysteresis(50) = 550.
        assert_eq!(schedule_expire_eviction_delay(&cvr), Some(550.0));
    }

    #[test]
    fn schedule_expire_eviction_delay_never_goes_below_hysteresis() {
        // Eviction time already passed (in the past relative to ttl_clock).
        let mut states = BTreeMap::new();
        states.insert(
            "c1".to_string(),
            ClientQueryState {
                inactivated_at: Some(TtlClock::from_number(0.0)),
                ttl: 0.0,
                deleted: true,
                version: empty_cvr_version(),
            },
        );
        let cvr = make_cvr(1_000_000.0, vec![("q1", client_query(states))]);
        assert_eq!(
            schedule_expire_eviction_delay(&cvr),
            Some(TTL_TIMER_HYSTERESIS)
        );
    }

    #[test]
    fn schedule_expire_eviction_delay_never_exceeds_max_ttl_ms() {
        let mut states = BTreeMap::new();
        states.insert(
            "c1".to_string(),
            ClientQueryState {
                inactivated_at: Some(TtlClock::from_number(0.0)),
                ttl: MAX_TTL_MS * 100.0,
                deleted: true,
                version: empty_cvr_version(),
            },
        );
        let cvr = make_cvr(0.0, vec![("q1", client_query(states))]);
        assert_eq!(schedule_expire_eviction_delay(&cvr), Some(MAX_TTL_MS));
    }

    #[test]
    fn random_id_matches_js_to_string_36() {
        assert_eq!(random_id(0), "0");
        assert_eq!(random_id(35), "z");
        assert_eq!(random_id(36), "10");
    }

    #[test]
    fn random_id_is_deterministic_for_the_same_input() {
        assert_eq!(random_id(123456), random_id(123456));
    }

    #[test]
    fn shutdown_before_initialization_error_has_the_expected_message_and_level() {
        let err = shutdown_before_initialization_error();
        assert_eq!(err.message(), "shut down before initialization completed");
        assert_eq!(err.log_level, LogLevel::Warn);
        assert_eq!(err.error_body.kind, ErrorKind::Internal);
        assert_eq!(err.error_body.origin, Some(ErrorOrigin::ZeroCache));
    }
}
