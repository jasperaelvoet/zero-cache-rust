//! The supervision decision for the ongoing-replication apply loop.
//!
//! `drive_apply_loop` ([`crate::replication_apply::drive_apply_loop`]) runs
//! until one of three things happens, reported in its [`ApplyLoopOutcome`]:
//! the upstream stream ended, the caller's `should_stop` fired, or a streamed
//! `Relation` message drifted from the schema the replica was built from (an
//! upstream DDL change). A long-lived replicator service must decide what to
//! do next after each run — this module is that pure decision, factored out of
//! the (I/O-bound, connection-owning) service loop so it is independently
//! testable.
//!
//! This mirrors upstream's change-streamer/replicator supervision: a schema
//! change cannot be applied incrementally to a replica built from the old
//! schema, so the replicator tears down and re-runs initial sync
//! (`Resync`); any other clean end just reconnects and resumes streaming from
//! the last confirmed watermark (`Reconnect`). A caller-requested stop is
//! terminal (`Stop`).

use crate::replication_apply::ApplyLoopOutcome;

/// What the replicator service should do after a [`drive_apply_loop`] run
/// returns.
///
/// [`drive_apply_loop`]: crate::replication_apply::drive_apply_loop
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SupervisorDecision {
    /// The upstream schema drifted from the replica's — the replica is stale
    /// and cannot follow the new schema incrementally. Tear it down and re-run
    /// initial sync. Carries the drift reason for logging.
    Resync { reason: String },
    /// The stream ended cleanly (e.g. a transient disconnect) with no schema
    /// change. Reconnect and resume streaming from the last confirmed
    /// watermark — the `commits` applied this run are durable.
    Reconnect { applied_commits: usize },
    /// The caller asked the loop to stop (`should_stop` returned true) and no
    /// drift occurred. Terminal — do not reconnect.
    Stop,
}

/// Decides the next supervisor action from a loop outcome.
///
/// Drift always wins: even if `should_stop` also fired on the same run, a
/// detected schema change must force a resync rather than a silent stop, or
/// the replica would be left stale. When there is no drift, `requested_stop`
/// (whether the caller's `should_stop` returned true this run, as opposed to
/// the stream simply ending) distinguishes a terminal [`SupervisorDecision::Stop`]
/// from a [`SupervisorDecision::Reconnect`].
pub fn decide_next_action(outcome: &ApplyLoopOutcome, requested_stop: bool) -> SupervisorDecision {
    if let Some(reason) = &outcome.drift {
        return SupervisorDecision::Resync {
            reason: reason.clone(),
        };
    }
    if requested_stop {
        SupervisorDecision::Stop
    } else {
        SupervisorDecision::Reconnect {
            applied_commits: outcome.commits,
        }
    }
}

/// The running lifecycle state a long-lived replicator service accumulates
/// across many `drive_apply_loop` cycles — the stateful shell around
/// [`decide_next_action`] that the assembled service loop carries between
/// reconnects and resyncs.
///
/// Each time a loop cycle ends, the service calls [`ReplicatorSupervisor::record`]
/// with that cycle's outcome; it folds the applied-commit count into a running
/// total, tallies how many reconnects and resyncs have happened, and returns
/// the [`SupervisorDecision`] to act on. A resync tallies but does NOT reset
/// the cumulative commit total (the replica really did apply those commits
/// before the drift was seen). This keeps the service's own bookkeeping in one
/// tested place rather than scattered through the (I/O-owning) loop body.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplicatorSupervisor {
    /// Total transactions applied to the replica across every cycle so far.
    pub total_commits: usize,
    /// How many times the stream was reconnected after a clean end.
    pub reconnects: usize,
    /// How many times a schema-drift resync was triggered.
    pub resyncs: usize,
}

impl ReplicatorSupervisor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a completed loop cycle and returns what to do next. Mirrors the
    /// pure [`decide_next_action`] rule (drift → resync, else stop-or-reconnect)
    /// while accumulating lifecycle counters.
    pub fn record(
        &mut self,
        outcome: &ApplyLoopOutcome,
        requested_stop: bool,
    ) -> SupervisorDecision {
        self.total_commits += outcome.commits;
        let decision = decide_next_action(outcome, requested_stop);
        match &decision {
            SupervisorDecision::Reconnect { .. } => self.reconnects += 1,
            SupervisorDecision::Resync { .. } => self.resyncs += 1,
            SupervisorDecision::Stop => {}
        }
        decision
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drift_forces_a_resync() {
        let outcome = ApplyLoopOutcome {
            commits: 3,
            drift: Some("issue.title type changed".into()),
        };
        assert_eq!(
            decide_next_action(&outcome, false),
            SupervisorDecision::Resync {
                reason: "issue.title type changed".into()
            }
        );
    }

    #[test]
    fn drift_wins_even_when_stop_was_requested() {
        // A schema change must not be masked by a coincident stop request —
        // otherwise the replica is silently left stale.
        let outcome = ApplyLoopOutcome {
            commits: 1,
            drift: Some("column added".into()),
        };
        assert_eq!(
            decide_next_action(&outcome, true),
            SupervisorDecision::Resync {
                reason: "column added".into()
            }
        );
    }

    #[test]
    fn clean_stream_end_reconnects_and_reports_applied_commits() {
        let outcome = ApplyLoopOutcome {
            commits: 5,
            drift: None,
        };
        assert_eq!(
            decide_next_action(&outcome, false),
            SupervisorDecision::Reconnect { applied_commits: 5 }
        );
    }

    #[test]
    fn requested_stop_without_drift_is_terminal() {
        let outcome = ApplyLoopOutcome {
            commits: 2,
            drift: None,
        };
        assert_eq!(decide_next_action(&outcome, true), SupervisorDecision::Stop);
    }

    #[test]
    fn supervisor_accumulates_a_realistic_service_lifecycle() {
        // A service that: applies 3 commits then the stream drops (reconnect),
        // applies 2 more then hits schema drift (resync), applies 4 more after
        // the rebuild then is asked to shut down (stop).
        let mut sup = ReplicatorSupervisor::new();

        let d1 = sup.record(
            &ApplyLoopOutcome {
                commits: 3,
                drift: None,
            },
            false,
        );
        assert_eq!(d1, SupervisorDecision::Reconnect { applied_commits: 3 });

        let d2 = sup.record(
            &ApplyLoopOutcome {
                commits: 2,
                drift: Some("issue.priority added".into()),
            },
            false,
        );
        assert_eq!(
            d2,
            SupervisorDecision::Resync {
                reason: "issue.priority added".into()
            }
        );

        let d3 = sup.record(
            &ApplyLoopOutcome {
                commits: 4,
                drift: None,
            },
            true,
        );
        assert_eq!(d3, SupervisorDecision::Stop);

        // Cumulative bookkeeping across the whole lifecycle.
        assert_eq!(sup.total_commits, 9, "3 + 2 + 4 applied across all cycles");
        assert_eq!(sup.reconnects, 1);
        assert_eq!(sup.resyncs, 1);
    }

    #[test]
    fn supervisor_new_starts_zeroed() {
        assert_eq!(
            ReplicatorSupervisor::new(),
            ReplicatorSupervisor {
                total_commits: 0,
                reconnects: 0,
                resyncs: 0,
            }
        );
    }
}
