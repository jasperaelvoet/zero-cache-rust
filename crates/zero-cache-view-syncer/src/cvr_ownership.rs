//! Port of the ownership/lease-conflict and row-catchup decision logic in
//! `CVRStore.#load` — the part that runs BEFORE the row-merging
//! (`cvr_load::load_cvr_from_rows`) this port already had: given the
//! `instances` (LEFT JOINed with `rowsVersion`) row for a client group,
//! decide whether this task may proceed to load/own the CVR, whether the
//! CVR is brand new, or whether it must wait for row catchup.
//!
//! Split into a pure decision function ([`decide_instance_load`]) and a SQL
//! text generator ([`get_claim_ownership_sql`]) for the fire-and-forget
//! ownership-claim UPDATE, following this port's established pattern of
//! separating decision logic from the live query it's paired with.

use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_types::sql::id;

use crate::cvr_types::TtlClock;
use crate::cvr_version::{empty_cvr_version, version_from_string, CvrVersion, VersionError};

/// The `instances` row (LEFT JOINed with `rowsVersion.version` as
/// `rowsVersion`), as `CVRStore.#load`'s first query fetches it. Port of
/// the inline type in `#load`'s first `tx<...>` query.
#[derive(Debug, Clone, PartialEq)]
pub struct LoadedInstanceRow {
    pub version: String,
    pub last_active: f64,
    pub ttl_clock: f64,
    pub replica_version: Option<String>,
    pub owner: Option<String>,
    /// Milliseconds since epoch, mirroring the SQL `grantedAt` timestamp
    /// compared against `lastConnectTime` (also ms).
    pub granted_at: Option<f64>,
    pub client_schema: Option<JsonValue>,
    pub profile_id: Option<String>,
    pub deleted: bool,
    /// `rowsVersion.version`, `None` if the LEFT JOIN found no matching row.
    pub rows_version: Option<String>,
}

/// The `Cvr` fields to overlay once an existing instance row is confirmed
/// loadable (ownership resolved, row-version caught up). Port of the
/// `cvr.version = ...` etc. assignments following the ownership/catchup
/// checks in `#load`.
#[derive(Debug, Clone, PartialEq)]
pub struct InstanceOverlay {
    pub version: CvrVersion,
    pub last_active: f64,
    pub ttl_clock: TtlClock,
    pub replica_version: Option<String>,
    pub profile_id: Option<String>,
    /// Opaque — see `cvr_types::Cvr::client_schema`'s doc on why this
    /// isn't parsed into a structured `ClientSchema` yet.
    pub client_schema: Option<JsonValue>,
}

/// Port of the three `throw`/early-return conditions `#load` checks before
/// overlaying instance fields onto the in-progress `Cvr`.
#[derive(Debug, PartialEq, thiserror::Error)]
pub enum LoadInstanceError {
    /// Port of `ClientNotFoundError` — the CVR's tombstone (`deleted`) flag
    /// is set.
    #[error("Client has been purged due to inactivity")]
    ClientNotFound,
    /// Port of `OwnershipError`: another task holds a still-valid lease.
    #[error("CVR owned by {owner:?} (granted at {granted_at:?}), which is after this connection's lastConnectTime {last_connect_time}")]
    Ownership {
        owner: Option<String>,
        granted_at: Option<f64>,
        last_connect_time: f64,
    },
    #[error(transparent)]
    Version(#[from] VersionError),
}

/// The result of resolving an `instances` row (or its absence) into a
/// decision about how to proceed. Port of `#load`'s three-way branch:
/// no row (`New`), a row whose `rowsVersion` hasn't caught up
/// (`RowsBehind` — port of `RowsVersionBehindError`, a *returned* value
/// upstream, not thrown, since it's a normal retry signal rather than a
/// hard failure), or a row that's ready to load (`Ready`).
#[derive(Debug, Clone, PartialEq)]
pub enum InstanceLoadOutcome {
    /// No `instances` row exists yet — this is a brand-new CVR.
    New,
    /// The row exists and ownership is fine, but `rowsVersion` (the row
    /// cache's actual state) hasn't caught up to `instances.version` yet;
    /// port of `RowsVersionBehindError`.
    RowsBehind {
        version: String,
        rows_version: Option<String>,
    },
    /// The row is loadable; overlay these fields onto the `Cvr`.
    /// `claim_ownership` is `true` if this task doesn't currently hold the
    /// lease and should fire the ownership-claim UPDATE (port of the
    /// fire-and-forget `UPDATE ... SET owner = ...` in `#load`).
    Ready {
        overlay: InstanceOverlay,
        claim_ownership: bool,
    },
}

/// Port of the `(grantedAt ?? 0) > lastConnectTime` conflict check shared
/// by `#load` and `#checkVersionAndOwnership`.
fn ownership_conflicts(granted_at: Option<f64>, last_connect_time: f64) -> bool {
    granted_at.unwrap_or(0.0) > last_connect_time
}

/// Decides how to proceed given the (possibly absent) `instances` row for
/// this client group. Port of the body of `#load` from `if (instance.length
/// === 0)` through the `cvr.clientSchema = ...` assignment (JSON validation
/// of `clientSchema` itself is left to the caller — this crate has no
/// `ClientSchema` JSON deserializer, see `InstanceOverlay::client_schema`'s
/// doc).
pub fn decide_instance_load(
    instance: Option<&LoadedInstanceRow>,
    task_id: &str,
    last_connect_time: f64,
) -> Result<InstanceLoadOutcome, LoadInstanceError> {
    let Some(row) = instance else {
        return Ok(InstanceLoadOutcome::New);
    };

    if row.deleted {
        return Err(LoadInstanceError::ClientNotFound);
    }

    let claim_ownership = row.owner.as_deref() != Some(task_id);
    if claim_ownership && ownership_conflicts(row.granted_at, last_connect_time) {
        return Err(LoadInstanceError::Ownership {
            owner: row.owner.clone(),
            granted_at: row.granted_at,
            last_connect_time,
        });
    }

    // Port of `version !== (rowsVersion ?? EMPTY_CVR_VERSION.stateVersion)`.
    let effective_rows_version = row
        .rows_version
        .clone()
        .unwrap_or_else(|| empty_cvr_version().state_version);
    if row.version != effective_rows_version {
        return Ok(InstanceLoadOutcome::RowsBehind {
            version: row.version.clone(),
            rows_version: row.rows_version.clone(),
        });
    }

    Ok(InstanceLoadOutcome::Ready {
        overlay: InstanceOverlay {
            version: version_from_string(&row.version)?,
            last_active: row.last_active,
            ttl_clock: TtlClock::from_number(row.ttl_clock),
            replica_version: row.replica_version.clone(),
            profile_id: row.profile_id.clone(),
            client_schema: row.client_schema.clone(),
        },
        claim_ownership,
    })
}

/// Generates the fire-and-forget ownership-claim UPDATE. Port of the SQL
/// template in `#load`'s `else` branch. `last_connect_time` is milliseconds
/// since epoch; the `to_timestamp($/1000)` conversion matches upstream's
/// `to_timestamp(${lastConnectTime / 1000})`.
pub fn get_claim_ownership_sql(
    cvr_schema: &str,
    client_group_id: &str,
    task_id: &str,
    last_connect_time: f64,
) -> String {
    format!(
        "UPDATE {}.instances SET \"owner\" = '{}', \"grantedAt\" = to_timestamp({}) \
         WHERE \"clientGroupID\" = '{}' AND (\"grantedAt\" IS NULL OR \"grantedAt\" <= to_timestamp({}))",
        id(cvr_schema),
        task_id.replace('\'', "''"),
        last_connect_time / 1000.0,
        client_group_id.replace('\'', "''"),
        last_connect_time / 1000.0,
    )
}

/// The `version`/`owner`/`grantedAt` row `#checkVersionAndOwnership`
/// selects `FOR UPDATE` before flushing. Port of its inline
/// `Pick<InstancesRow, 'version'|'owner'|'grantedAt'>` result type.
#[derive(Debug, Clone, PartialEq)]
pub struct VersionOwnershipRow {
    pub version: String,
    pub owner: Option<String>,
    pub granted_at: Option<f64>,
}

/// Port of `#checkVersionAndOwnership`'s two failure modes (a third,
/// `ClientNotFoundError`, isn't checked here — upstream's write-time check
/// doesn't look at `deleted` at all, only `#load`'s read-time check does).
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum CheckVersionError {
    #[error("CVR owned by {owner:?} (granted at {granted_at:?}), which is after this connection's lastConnectTime {last_connect_time}")]
    Ownership {
        owner: Option<String>,
        granted_at: Option<f64>,
        last_connect_time: f64,
    },
    /// Port of `ConcurrentModificationException`: another flush already
    /// advanced the CVR's version past what this flush expected.
    #[error("Expected CVR version {expected:?} but read {actual:?} — concurrent modification")]
    ConcurrentModification { expected: String, actual: String },
}

/// Validates that this task may flush: it either owns the CVR (or no one
/// does / the lease has lapsed) AND the CVR's version matches what this
/// flush expected to be building on. Port of `#checkVersionAndOwnership`
/// (minus issuing the live `SELECT ... FOR UPDATE` itself — `row` is the
/// caller's already-fetched result, `None` if no `instances` row exists
/// yet, matching upstream's `EMPTY_CVR_VERSION.stateVersion`/`null`/`null`
/// fallback).
pub fn check_version_and_ownership(
    row: Option<&VersionOwnershipRow>,
    task_id: &str,
    last_connect_time: f64,
    expected_version: &str,
) -> Result<(), CheckVersionError> {
    let (version, owner, granted_at) = match row {
        Some(r) => (r.version.clone(), r.owner.clone(), r.granted_at),
        None => (empty_cvr_version().state_version, None, None),
    };

    if owner.as_deref() != Some(task_id) && ownership_conflicts(granted_at, last_connect_time) {
        return Err(CheckVersionError::Ownership {
            owner,
            granted_at,
            last_connect_time,
        });
    }
    if version != expected_version {
        return Err(CheckVersionError::ConcurrentModification {
            expected: expected_version.to_string(),
            actual: version,
        });
    }
    Ok(())
}

/// Port of the `SELECT "version", "owner", "grantedAt" FROM ... WHERE
/// "clientGroupID" = ... FOR UPDATE` query `#checkVersionAndOwnership`
/// issues to both read and row-lock the instance before flushing.
pub fn get_check_version_and_ownership_sql(cvr_schema: &str, client_group_id: &str) -> String {
    format!(
        "SELECT \"version\", \"owner\", (extract(epoch from \"grantedAt\") * 1000)::float8 AS \"grantedAt\" \
         FROM {}.instances WHERE \"clientGroupID\" = '{}' FOR UPDATE",
        id(cvr_schema),
        client_group_id.replace('\'', "''"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_row() -> LoadedInstanceRow {
        LoadedInstanceRow {
            version: "01".into(),
            last_active: 100.0,
            ttl_clock: 0.0,
            replica_version: Some("rv1".into()),
            owner: Some("task-1".into()),
            granted_at: Some(50.0),
            client_schema: None,
            profile_id: None,
            deleted: false,
            rows_version: Some("01".into()),
        }
    }

    #[test]
    fn no_row_is_new_cvr() {
        assert_eq!(
            decide_instance_load(None, "task-1", 1000.0).unwrap(),
            InstanceLoadOutcome::New
        );
    }

    #[test]
    fn deleted_row_errors_client_not_found() {
        let row = LoadedInstanceRow {
            deleted: true,
            ..base_row()
        };
        assert_eq!(
            decide_instance_load(Some(&row), "task-1", 1000.0),
            Err(LoadInstanceError::ClientNotFound)
        );
    }

    #[test]
    fn same_owner_never_conflicts_regardless_of_granted_at() {
        let row = LoadedInstanceRow {
            owner: Some("task-1".into()),
            granted_at: Some(999_999.0),
            ..base_row()
        };
        let outcome = decide_instance_load(Some(&row), "task-1", 100.0).unwrap();
        assert!(matches!(
            outcome,
            InstanceLoadOutcome::Ready {
                claim_ownership: false,
                ..
            }
        ));
    }

    #[test]
    fn different_owner_with_future_grant_conflicts() {
        let row = LoadedInstanceRow {
            owner: Some("task-2".into()),
            granted_at: Some(2000.0),
            ..base_row()
        };
        let err = decide_instance_load(Some(&row), "task-1", 1000.0).unwrap_err();
        assert_eq!(
            err,
            LoadInstanceError::Ownership {
                owner: Some("task-2".into()),
                granted_at: Some(2000.0),
                last_connect_time: 1000.0
            }
        );
    }

    #[test]
    fn different_owner_with_past_grant_claims_ownership() {
        let row = LoadedInstanceRow {
            owner: Some("task-2".into()),
            granted_at: Some(500.0),
            ..base_row()
        };
        let outcome = decide_instance_load(Some(&row), "task-1", 1000.0).unwrap();
        assert!(matches!(
            outcome,
            InstanceLoadOutcome::Ready {
                claim_ownership: true,
                ..
            }
        ));
    }

    #[test]
    fn null_owner_claims_ownership() {
        let row = LoadedInstanceRow {
            owner: None,
            granted_at: None,
            ..base_row()
        };
        let outcome = decide_instance_load(Some(&row), "task-1", 1000.0).unwrap();
        assert!(matches!(
            outcome,
            InstanceLoadOutcome::Ready {
                claim_ownership: true,
                ..
            }
        ));
    }

    #[test]
    fn version_mismatch_returns_rows_behind() {
        let row = LoadedInstanceRow {
            version: "02".into(),
            rows_version: Some("01".into()),
            ..base_row()
        };
        let outcome = decide_instance_load(Some(&row), "task-1", 1000.0).unwrap();
        assert_eq!(
            outcome,
            InstanceLoadOutcome::RowsBehind {
                version: "02".into(),
                rows_version: Some("01".into())
            }
        );
    }

    #[test]
    fn null_rows_version_compares_against_empty_cvr_version() {
        // rows_version None means the LEFT JOIN found nothing -> compares
        // against the empty CVR's state version, not "01".
        let row = LoadedInstanceRow {
            version: "01".into(),
            rows_version: None,
            ..base_row()
        };
        let outcome = decide_instance_load(Some(&row), "task-1", 1000.0).unwrap();
        assert!(matches!(outcome, InstanceLoadOutcome::RowsBehind { .. }));
    }

    #[test]
    fn ready_overlay_carries_all_fields() {
        let row = base_row();
        let outcome = decide_instance_load(Some(&row), "task-1", 1000.0).unwrap();
        let InstanceLoadOutcome::Ready { overlay, .. } = outcome else {
            panic!("expected Ready")
        };
        assert_eq!(overlay.last_active, 100.0);
        assert_eq!(overlay.replica_version, Some("rv1".into()));
    }

    #[test]
    fn claim_ownership_sql_shape() {
        let sql = get_claim_ownership_sql("app_0/cvr", "cg1", "task-1", 60_000.0);
        assert_eq!(
            sql,
            "UPDATE \"app_0/cvr\".instances SET \"owner\" = 'task-1', \"grantedAt\" = to_timestamp(60) \
             WHERE \"clientGroupID\" = 'cg1' AND (\"grantedAt\" IS NULL OR \"grantedAt\" <= to_timestamp(60))"
        );
    }

    #[test]
    fn check_version_no_row_uses_empty_version_and_succeeds_if_expected_matches() {
        let expected = empty_cvr_version().state_version;
        assert_eq!(
            check_version_and_ownership(None, "task-1", 1000.0, &expected),
            Ok(())
        );
    }

    #[test]
    fn check_version_no_row_fails_if_expected_is_not_empty() {
        let err = check_version_and_ownership(None, "task-1", 1000.0, "01").unwrap_err();
        assert!(matches!(
            err,
            CheckVersionError::ConcurrentModification { .. }
        ));
    }

    #[test]
    fn check_version_same_owner_never_conflicts() {
        let row = VersionOwnershipRow {
            version: "01".into(),
            owner: Some("task-1".into()),
            granted_at: Some(999_999.0),
        };
        assert_eq!(
            check_version_and_ownership(Some(&row), "task-1", 100.0, "01"),
            Ok(())
        );
    }

    #[test]
    fn check_version_different_owner_with_future_grant_conflicts() {
        let row = VersionOwnershipRow {
            version: "01".into(),
            owner: Some("task-2".into()),
            granted_at: Some(2000.0),
        };
        let err = check_version_and_ownership(Some(&row), "task-1", 1000.0, "01").unwrap_err();
        assert_eq!(
            err,
            CheckVersionError::Ownership {
                owner: Some("task-2".into()),
                granted_at: Some(2000.0),
                last_connect_time: 1000.0
            }
        );
    }

    #[test]
    fn check_version_mismatch_errors_concurrent_modification() {
        let row = VersionOwnershipRow {
            version: "02".into(),
            owner: Some("task-1".into()),
            granted_at: None,
        };
        let err = check_version_and_ownership(Some(&row), "task-1", 1000.0, "01").unwrap_err();
        assert_eq!(
            err,
            CheckVersionError::ConcurrentModification {
                expected: "01".into(),
                actual: "02".into()
            }
        );
    }

    #[test]
    fn check_version_and_ownership_sql_shape() {
        let sql = get_check_version_and_ownership_sql("app_0/cvr", "cg1");
        assert_eq!(
            sql,
            "SELECT \"version\", \"owner\", (extract(epoch from \"grantedAt\") * 1000)::float8 AS \"grantedAt\" \
             FROM \"app_0/cvr\".instances WHERE \"clientGroupID\" = 'cg1' FOR UPDATE"
        );
    }
}
