//! Port of `CVRConfigDrivenUpdater.setClientSchema`/`setProfileID` from
//! `zero-cache/src/services/view-syncer/cvr.ts`.
//!
//! Both are small CVR-level state transitions with real validation logic;
//! `CVRStore` persistence (`putInstance`) is out of scope (see the module docs
//! on [`crate::cvr_desired_queries`] for this port's established boundary).

use zero_cache_protocol::error::{ErrorBody, ProtocolError};
use zero_cache_protocol::{ErrorKind, ErrorOrigin};
use zero_cache_shared::bigint_json::JsonValue;

use crate::cvr_types::Cvr;

/// Sets the CVR's client schema on first use, or validates that a
/// subsequently-supplied schema matches. Port of `setClientSchema`.
///
/// Returns an [`Err`] `ProtocolError` (`InvalidConnectionRequest`) if the CVR
/// already has a client schema that differs from `client_schema` — this
/// should not happen with a correct Zero client, since all clients of a CVR
/// share a schema (it's part of the IDB key), but is defensively checked.
pub fn set_client_schema(cvr: &mut Cvr, client_schema: &JsonValue) -> Result<(), ProtocolError> {
    match &cvr.client_schema {
        None => {
            cvr.client_schema = Some(client_schema.clone());
            Ok(())
        }
        Some(existing) if existing != client_schema => Err(ProtocolError::new(ErrorBody::new(
            ErrorKind::InvalidConnectionRequest,
            "Provided schema does not match previous schema",
            Some(ErrorOrigin::ZeroCache),
        ))),
        Some(_) => Ok(()),
    }
}

/// Sets the CVR's profile ID if it changed. Port of `setProfileID`. Returns
/// `true` if a warning should be logged (the ID changed from something other
/// than `null` or a back-filled `"cg..."` placeholder — both expected
/// transitions — signaling a potentially pathological condition); the actual
/// logging is left to the caller.
pub fn set_profile_id(cvr: &mut Cvr, profile_id: &str) -> bool {
    if cvr.profile_id.as_deref() == Some(profile_id) {
        return false;
    }
    let should_warn = match &cvr.profile_id {
        None => false,
        Some(existing) => !existing.starts_with("cg"),
    };
    cvr.profile_id = Some(profile_id.to_string());
    should_warn
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_types::TtlClock;
    use crate::cvr_version::empty_cvr_version;
    use std::collections::BTreeMap;

    fn empty_cvr() -> Cvr {
        Cvr {
            id: "cvr1".into(),
            version: empty_cvr_version(),
            last_active: 0.0,
            ttl_clock: TtlClock::from_number(0.0),
            replica_version: None,
            clients: BTreeMap::new(),
            queries: BTreeMap::new(),
            client_schema: None,
            profile_id: None,
        }
    }

    fn schema(v: &str) -> JsonValue {
        JsonValue::Object(vec![("hash".into(), JsonValue::String(v.into()))])
    }

    #[test]
    fn set_client_schema_first_use_sets_it() {
        let mut cvr = empty_cvr();
        set_client_schema(&mut cvr, &schema("a")).unwrap();
        assert_eq!(cvr.client_schema, Some(schema("a")));
    }

    #[test]
    fn set_client_schema_matching_is_a_noop() {
        let mut cvr = empty_cvr();
        set_client_schema(&mut cvr, &schema("a")).unwrap();
        set_client_schema(&mut cvr, &schema("a")).unwrap();
        assert_eq!(cvr.client_schema, Some(schema("a")));
    }

    #[test]
    fn set_client_schema_mismatch_errors() {
        let mut cvr = empty_cvr();
        set_client_schema(&mut cvr, &schema("a")).unwrap();
        let err = set_client_schema(&mut cvr, &schema("b")).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidConnectionRequest);
        // Schema is unchanged after the rejected update.
        assert_eq!(cvr.client_schema, Some(schema("a")));
    }

    #[test]
    fn set_profile_id_from_null_no_warning() {
        let mut cvr = empty_cvr();
        assert!(!set_profile_id(&mut cvr, "profile1"));
        assert_eq!(cvr.profile_id, Some("profile1".to_string()));
    }

    #[test]
    fn set_profile_id_from_backfilled_placeholder_no_warning() {
        let mut cvr = empty_cvr();
        cvr.profile_id = Some("cg-abc123".to_string());
        assert!(!set_profile_id(&mut cvr, "profile1"));
    }

    #[test]
    fn set_profile_id_change_from_real_id_warns() {
        let mut cvr = empty_cvr();
        cvr.profile_id = Some("profile-old".to_string());
        assert!(set_profile_id(&mut cvr, "profile-new"));
        assert_eq!(cvr.profile_id, Some("profile-new".to_string()));
    }

    #[test]
    fn set_profile_id_same_id_is_noop() {
        let mut cvr = empty_cvr();
        cvr.profile_id = Some("profile1".to_string());
        assert!(!set_profile_id(&mut cvr, "profile1"));
    }
}
