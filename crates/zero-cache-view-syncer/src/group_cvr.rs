//! Group-owned CVR state (redesign §6 C2): ONE in-memory CVR per client
//! group, shared by every connection in the group.
//!
//! Upstream's `ViewSyncerService` holds `#cvr` — a single CVR per client group
//! mutated under `#lock` (`mono-src/packages/zero-cache/src/services/
//! view-syncer/view-syncer.ts`). This port's per-connection handlers each kept
//! a private copy and re-loaded it from Postgres at EVERY transition to stay
//! coherent; the cell here replaces that with in-memory hand-off:
//!
//! - a transition **checks out** the group state (under the group's transition
//!   lock, which every CVR transition already holds across
//!   refresh→apply→persist),
//! - mutates it through the existing synchronous handler code, and
//! - **checks it back in** once the durable flush succeeds.
//!
//! A failed transition simply never checks back in, leaving the cell empty —
//! the next transition falls back to the durable Postgres load, which is also
//! how a version-CAS loss against another NODE self-heals. Without durable
//! persistence the cell itself is the group's source of truth, giving the
//! group's connections one consistent CVR version chain instead of a fresh
//! per-connection CVR each.
//!
//! The cell lives on [`crate::group_registry::GroupService`], so its lifetime
//! is exactly the group's: dropped (state discarded) when the group's last
//! connection disconnects, re-seeded from the durable store on the next.

use std::sync::Mutex;

use zero_cache_protocol::row_patch::Row;

use crate::cvr_row_cache_sql::RowUpdate;
use crate::cvr_types::{Cvr, RowId, RowRecord};

/// The group-scoped CVR state a connection's transition operates on. Exactly
/// the fields the server's connection handler used to own privately per
/// connection.
pub struct GroupCvrState {
    pub cvr: Cvr,
    /// The group's row records (query ref-counts per row). `Arc`-wrapped so a
    /// transition that does not change rows (a 2nd+ desirer of an
    /// already-hydrated query — the common connect-time case) checks in / snapshots
    /// by cloning the `Arc`, not the 1000-row vec; mutations copy-on-write via
    /// `Arc::make_mut`.
    pub row_records: std::sync::Arc<Vec<RowRecord>>,
    /// By-id row body store backing forced row wiring and delete patches.
    /// `Arc`-wrapped for the same reason as `row_records`.
    pub row_bodies: std::sync::Arc<Vec<(RowId, Row)>>,
    /// Row-cache changes not yet durably flushed. Non-empty between
    /// transitions only when no durable persistence is configured.
    pub pending_row_updates: Vec<RowUpdate>,
}

/// One client group's shared CVR slot. All access happens under the group's
/// transition lock, so the internal mutex is uncontended and never held across
/// an await.
#[derive(Default)]
pub struct GroupCvrCell {
    state: Mutex<Option<GroupCvrState>>,
}

impl GroupCvrCell {
    /// Checks the group state out for a transition. Empty when no transition
    /// has checked in yet (first connection) or the previous transition failed
    /// (fall back to the durable load).
    pub fn take(&self) -> Option<GroupCvrState> {
        self.lock().take()
    }

    /// Checks a transition's resulting state back in as the group truth.
    pub fn put(&self, state: GroupCvrState) {
        *self.lock() = Some(state);
    }

    /// A clone of the current group state, if any — used at connection
    /// bootstrap to seed the handler without a durable load (and without
    /// consuming the cell).
    pub fn snapshot(&self) -> Option<GroupCvrState> {
        let guard = self.lock();
        guard.as_ref().map(|state| GroupCvrState {
            cvr: state.cvr.clone(),
            row_records: state.row_records.clone(),
            row_bodies: state.row_bodies.clone(),
            pending_row_updates: state.pending_row_updates.clone(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<GroupCvrState>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cvr_version::empty_cvr_version;

    fn state(config_version: i64) -> GroupCvrState {
        let mut version = empty_cvr_version();
        version.config_version = Some(config_version);
        GroupCvrState {
            cvr: Cvr {
                id: "g".into(),
                version,
                last_active: 0.0,
                ttl_clock: crate::cvr_types::TtlClock(0.0),
                replica_version: None,
                clients: Default::default(),
                queries: Default::default(),
                client_schema: None,
                profile_id: None,
            },
            row_records: std::sync::Arc::new(Vec::new()),
            row_bodies: std::sync::Arc::new(Vec::new()),
            pending_row_updates: Vec::new(),
        }
    }

    /// The check-out/check-in cycle: taking empties the cell (a concurrent
    /// transition can never build on the same snapshot), putting restores it.
    #[test]
    fn take_empties_and_put_restores() {
        let cell = GroupCvrCell::default();
        assert!(cell.take().is_none(), "fresh cell is empty");

        cell.put(state(1));
        let taken = cell.take().expect("state was checked in");
        assert_eq!(taken.cvr.version.config_version, Some(1));
        assert!(
            cell.take().is_none(),
            "a checked-out state is exclusively owned by the transition"
        );
    }

    /// `snapshot` clones without consuming — bootstrap reads must not steal the
    /// group state from under a transition.
    #[test]
    fn snapshot_does_not_consume() {
        let cell = GroupCvrCell::default();
        cell.put(state(2));
        assert_eq!(cell.snapshot().unwrap().cvr.version.config_version, Some(2));
        assert!(cell.take().is_some(), "snapshot left the state in place");
    }
}
