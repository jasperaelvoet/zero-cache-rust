//! Port of the pure serving-lag statistics machinery in
//! `zero-cache/src/workers/syncer.ts` — the first slice of `syncer.ts`
//! (694 lines, the actual `ViewSyncerService`-hosting worker). Entirely
//! self-contained: binary-searches over a bounded ring of
//! `ReplicaReadyState` samples to figure out, per `ViewSyncer`, how far
//! behind the latest ready replica state it is, then reduces that into
//! percentile stats — no `Worker`/`ViewSyncer`/WebSocket machinery
//! involved, so this needed none of the process-model or `ViewSyncerService`
//! decision-layer work from prior rounds to become portable.
//!
//! Scope: ports every pure function verbatim — `bound_replica_ready_states`/
//! `prune_replica_ready_states` (bounding a growing ring buffer of replica
//! readiness samples), `lower_bound_replica_ready_time_ms`/
//! `upper_bound_watermark` (binary searches over the two sort orders the
//! same slice is searched by), `find_first_unserved_index`,
//! `percentile_nearest_rank`, and the two public entry points
//! `compute_serving_lag_stats_ms`/`compute_max_serving_lag_ms`. NOT
//! ported: `Syncer` itself (owns the `WebSocketServer`, `ServiceRunner`,
//! `DrainCoordinator`, and the actual `#recordReplicaReadyState`/
//! `#computeServingLagStats` caching wrapper around these pure functions)
//! — that needs the full `ViewSyncerService`/`Worker` machinery this slice
//! deliberately doesn't require.

/// One recorded replica-ready sample. Port of `ReplicaReadyState`.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaReadyState {
    pub watermark: String,
    pub replica_ready_time_ms: f64,
}

/// The subset of `ViewSyncer` this module reads. Port of
/// `ServingLagViewSyncer`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ServingLagViewSyncer<'a> {
    pub created_at_ms: f64,
    pub served_version: Option<&'a str>,
}

/// Port of `ServingLagStats`.
#[derive(Debug, Clone, PartialEq)]
pub struct ServingLagStats {
    pub active_client_groups: usize,
    pub lagging_client_groups: usize,
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p75_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

/// The cap `MAX_REPLICA_READY_STATES` bounds the ring to.
pub const MAX_REPLICA_READY_STATES: usize = 10_000;

/// Port of `boundReplicaReadyStates`: drops the oldest entries once the
/// slice exceeds [`MAX_REPLICA_READY_STATES`].
pub fn bound_replica_ready_states(replica_ready_states: &mut Vec<ReplicaReadyState>) {
    if replica_ready_states.len() > MAX_REPLICA_READY_STATES {
        let excess = replica_ready_states.len() - MAX_REPLICA_READY_STATES;
        replica_ready_states.drain(0..excess);
    }
}

/// Port of `pruneReplicaReadyStates`: drops everything before
/// `first_needed_index` (no `ViewSyncer` still needs it), then re-applies
/// the size bound.
pub fn prune_replica_ready_states(
    replica_ready_states: &mut Vec<ReplicaReadyState>,
    first_needed_index: usize,
) {
    if first_needed_index > 0 {
        let drop_to = first_needed_index.min(replica_ready_states.len());
        replica_ready_states.drain(0..drop_to);
    }
    bound_replica_ready_states(replica_ready_states);
}

/// Port of `lowerBoundReplicaReadyTimeMs`: the index of the first sample
/// whose `replica_ready_time_ms` is `>= replica_ready_time_ms`.
pub fn lower_bound_replica_ready_time_ms(
    replica_ready_states: &[ReplicaReadyState],
    replica_ready_time_ms: f64,
) -> usize {
    let mut low = 0usize;
    let mut high = replica_ready_states.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if replica_ready_states[mid].replica_ready_time_ms < replica_ready_time_ms {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

/// Port of `upperBoundWatermark`: the index of the first sample whose
/// `watermark` is strictly greater than `watermark` (i.e. one past every
/// sample with `watermark <= watermark`).
pub fn upper_bound_watermark(replica_ready_states: &[ReplicaReadyState], watermark: &str) -> usize {
    let mut low = 0usize;
    let mut high = replica_ready_states.len();
    while low < high {
        let mid = low + (high - low) / 2;
        if replica_ready_states[mid].watermark.as_str() <= watermark {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

/// Port of `findFirstUnservedIndex`: the earliest replica-ready sample this
/// `ViewSyncer` hasn't caught up to yet (both "existed since it was
/// created" AND "newer than what it's already served"), or `None` if it's
/// fully caught up.
pub fn find_first_unserved_index(
    replica_ready_states: &[ReplicaReadyState],
    view_syncer: ServingLagViewSyncer,
) -> Option<usize> {
    let first_ready_after_creation =
        lower_bound_replica_ready_time_ms(replica_ready_states, view_syncer.created_at_ms);
    let first_after_served_version = match view_syncer.served_version {
        None => 0,
        Some(v) => upper_bound_watermark(replica_ready_states, v),
    };

    let first_unserved_index = first_ready_after_creation.max(first_after_served_version);
    if first_unserved_index < replica_ready_states.len() {
        Some(first_unserved_index)
    } else {
        None
    }
}

/// Port of `percentileNearestRank` (the "nearest rank" percentile method,
/// not interpolated).
pub fn percentile_nearest_rank(sorted_values: &[f64], percentile: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }
    let raw_index = ((percentile / 100.0) * sorted_values.len() as f64).ceil() - 1.0;
    let index = raw_index.max(0.0).min((sorted_values.len() - 1) as f64) as usize;
    sorted_values[index]
}

/// Port of `computeServingLagStatsMs`: for each `ViewSyncer`, finds how
/// stale its view is relative to the latest replica-ready samples, prunes
/// `replica_ready_states` down to what's still needed by ANY view syncer
/// (mutating it in place, matching upstream's side-effecting call), and
/// reduces the per-syncer lags into percentile stats.
pub fn compute_serving_lag_stats_ms(
    now: f64,
    replica_ready_states: &mut Vec<ReplicaReadyState>,
    view_syncers: impl IntoIterator<Item = ServingLagViewSyncer<'static>>,
) -> ServingLagStats {
    let mut lags: Vec<f64> = Vec::new();
    let mut lagging_client_groups = 0usize;
    let mut first_needed_index = replica_ready_states.len();

    for view_syncer in view_syncers {
        let Some(first_unserved_index) =
            find_first_unserved_index(replica_ready_states, view_syncer)
        else {
            lags.push(0.0);
            continue;
        };

        first_needed_index = first_needed_index.min(first_unserved_index);
        let lag_ms =
            (now - replica_ready_states[first_unserved_index].replica_ready_time_ms).max(0.0);
        lags.push(lag_ms);
        if lag_ms > 0.0 {
            lagging_client_groups += 1;
        }
    }

    prune_replica_ready_states(replica_ready_states, first_needed_index);

    lags.sort_by(|a, b| a.partial_cmp(b).unwrap());
    ServingLagStats {
        active_client_groups: lags.len(),
        lagging_client_groups,
        min_ms: lags.first().copied().unwrap_or(0.0),
        p50_ms: percentile_nearest_rank(&lags, 50.0),
        p75_ms: percentile_nearest_rank(&lags, 75.0),
        p99_ms: percentile_nearest_rank(&lags, 99.0),
        max_ms: lags.last().copied().unwrap_or(0.0),
    }
}

/// Port of `computeMaxServingLagMs`.
pub fn compute_max_serving_lag_ms(
    now: f64,
    replica_ready_states: &mut Vec<ReplicaReadyState>,
    view_syncers: impl IntoIterator<Item = ServingLagViewSyncer<'static>>,
) -> f64 {
    compute_serving_lag_stats_ms(now, replica_ready_states, view_syncers).max_ms
}

/// Port of `Syncer#recordReplicaReadyState`: appends a new sample, skipping
/// mid-hydration snapshots (no watermark/ready-time yet) and any watermark
/// that isn't strictly newer than the last recorded one (guards against a
/// stale race), then either clears the whole buffer (no `ViewSyncer` left to
/// consume it — `has_active_view_syncers` stands in for upstream's
/// `#viewSyncers.size === 0`) or re-applies the size bound.
pub fn record_replica_ready_state(
    replica_ready_states: &mut Vec<ReplicaReadyState>,
    watermark: Option<String>,
    replica_ready_time_ms: Option<f64>,
    has_active_view_syncers: bool,
) {
    let (Some(watermark), Some(replica_ready_time_ms)) = (watermark, replica_ready_time_ms) else {
        return;
    };
    if let Some(last) = replica_ready_states.last() {
        if last.watermark.as_str() >= watermark.as_str() {
            return;
        }
    }
    replica_ready_states.push(ReplicaReadyState {
        watermark,
        replica_ready_time_ms,
    });
    if !has_active_view_syncers {
        replica_ready_states.clear();
        return;
    }
    bound_replica_ready_states(replica_ready_states);
}

/// Port of `#servingLagStatsCache`/`#computeServingLagStats`'s memoization
/// wrapper: caches the last computed [`ServingLagStats`] until explicitly
/// cleared. Upstream schedules the clear via `queueMicrotask` right after
/// the first compute in a tick, so every metrics-gauge callback within that
/// same tick shares one computation; this port takes that as an explicit
/// [`Self::clear`] call the caller makes at the tick boundary instead of
/// scheduling one ambiently, matching this port's convention of never
/// reading ambient scheduling directly.
#[derive(Debug, Default)]
pub struct ServingLagStatsCache {
    cached: Option<ServingLagStats>,
}

impl ServingLagStatsCache {
    pub fn get_or_compute(
        &mut self,
        now: f64,
        replica_ready_states: &mut Vec<ReplicaReadyState>,
        view_syncers: impl IntoIterator<Item = ServingLagViewSyncer<'static>>,
    ) -> ServingLagStats {
        if let Some(cached) = &self.cached {
            return cached.clone();
        }
        let stats = compute_serving_lag_stats_ms(now, replica_ready_states, view_syncers);
        self.cached = Some(stats.clone());
        stats
    }

    pub fn clear(&mut self) {
        self.cached = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(watermark: &str, ready_ms: f64) -> ReplicaReadyState {
        ReplicaReadyState {
            watermark: watermark.to_string(),
            replica_ready_time_ms: ready_ms,
        }
    }

    fn samples() -> Vec<ReplicaReadyState> {
        vec![
            state("01", 100.0),
            state("02", 200.0),
            state("03", 300.0),
            state("04", 400.0),
        ]
    }

    #[test]
    fn bound_replica_ready_states_drops_oldest_past_the_cap() {
        let mut states: Vec<ReplicaReadyState> = (0..(MAX_REPLICA_READY_STATES + 5))
            .map(|i| state(&i.to_string(), i as f64))
            .collect();
        bound_replica_ready_states(&mut states);
        assert_eq!(states.len(), MAX_REPLICA_READY_STATES);
        assert_eq!(states[0].replica_ready_time_ms, 5.0);
    }

    #[test]
    fn bound_replica_ready_states_noop_under_the_cap() {
        let mut states = samples();
        bound_replica_ready_states(&mut states);
        assert_eq!(states.len(), 4);
    }

    #[test]
    fn prune_replica_ready_states_drops_up_to_the_needed_index() {
        let mut states = samples();
        prune_replica_ready_states(&mut states, 2);
        assert_eq!(
            states
                .iter()
                .map(|s| s.watermark.as_str())
                .collect::<Vec<_>>(),
            vec!["03", "04"]
        );
    }

    #[test]
    fn lower_bound_replica_ready_time_ms_finds_first_at_or_after() {
        let states = samples();
        assert_eq!(lower_bound_replica_ready_time_ms(&states, 0.0), 0);
        assert_eq!(lower_bound_replica_ready_time_ms(&states, 250.0), 2);
        assert_eq!(lower_bound_replica_ready_time_ms(&states, 400.0), 3);
        assert_eq!(lower_bound_replica_ready_time_ms(&states, 500.0), 4);
    }

    #[test]
    fn upper_bound_watermark_finds_first_strictly_after() {
        let states = samples();
        assert_eq!(upper_bound_watermark(&states, "00"), 0);
        assert_eq!(
            upper_bound_watermark(&states, "02"),
            2,
            "inclusive of the matching watermark itself"
        );
        assert_eq!(upper_bound_watermark(&states, "04"), 4);
    }

    #[test]
    fn find_first_unserved_index_combines_both_bounds() {
        let states = samples();
        // Created before everything, served nothing yet -> first sample.
        assert_eq!(
            find_first_unserved_index(
                &states,
                ServingLagViewSyncer {
                    created_at_ms: 0.0,
                    served_version: None
                }
            ),
            Some(0)
        );
        // Already served up through "02" -> first unserved is "03".
        assert_eq!(
            find_first_unserved_index(
                &states,
                ServingLagViewSyncer {
                    created_at_ms: 0.0,
                    served_version: Some("02")
                }
            ),
            Some(2)
        );
        // Fully caught up.
        assert_eq!(
            find_first_unserved_index(
                &states,
                ServingLagViewSyncer {
                    created_at_ms: 0.0,
                    served_version: Some("04")
                }
            ),
            None
        );
        // Created after every sample so far -> also caught up.
        assert_eq!(
            find_first_unserved_index(
                &states,
                ServingLagViewSyncer {
                    created_at_ms: 1000.0,
                    served_version: None
                }
            ),
            None
        );
    }

    #[test]
    fn percentile_nearest_rank_matches_known_cases() {
        let sorted = [10.0, 20.0, 30.0, 40.0, 50.0];
        assert_eq!(percentile_nearest_rank(&sorted, 50.0), 30.0);
        assert_eq!(percentile_nearest_rank(&sorted, 99.0), 50.0);
        assert_eq!(percentile_nearest_rank(&[], 50.0), 0.0);
        assert_eq!(percentile_nearest_rank(&[42.0], 1.0), 42.0);
    }

    #[test]
    fn compute_serving_lag_stats_reduces_per_syncer_lag_into_percentiles() {
        let mut states = samples();
        let syncers = vec![
            ServingLagViewSyncer {
                created_at_ms: 0.0,
                served_version: Some("04"),
            }, // caught up: lag 0
            ServingLagViewSyncer {
                created_at_ms: 0.0,
                served_version: Some("02"),
            }, // lag = now - 300
        ];
        let stats = compute_serving_lag_stats_ms(500.0, &mut states, syncers);

        assert_eq!(stats.active_client_groups, 2);
        assert_eq!(stats.lagging_client_groups, 1);
        assert_eq!(stats.min_ms, 0.0);
        assert_eq!(stats.max_ms, 200.0); // 500 - 300
    }

    #[test]
    fn compute_serving_lag_stats_prunes_states_no_longer_needed_by_anyone() {
        let mut states = samples();
        // Both syncers have already served "02" -> nothing before index 2
        // ("03") is needed by anyone anymore.
        let syncers = vec![
            ServingLagViewSyncer {
                created_at_ms: 0.0,
                served_version: Some("02"),
            },
            ServingLagViewSyncer {
                created_at_ms: 0.0,
                served_version: Some("03"),
            },
        ];
        compute_serving_lag_stats_ms(500.0, &mut states, syncers);
        assert_eq!(
            states
                .iter()
                .map(|s| s.watermark.as_str())
                .collect::<Vec<_>>(),
            vec!["03", "04"]
        );
    }

    #[test]
    fn compute_serving_lag_stats_with_no_syncers_prunes_everything() {
        let mut states = samples();
        compute_serving_lag_stats_ms(500.0, &mut states, vec![]);
        assert!(states.is_empty());
    }

    #[test]
    fn compute_max_serving_lag_ms_matches_the_max_field() {
        let mut states = samples();
        let syncers = vec![ServingLagViewSyncer {
            created_at_ms: 0.0,
            served_version: Some("02"),
        }];
        let max = compute_max_serving_lag_ms(500.0, &mut states.clone(), syncers.clone());
        let stats = compute_serving_lag_stats_ms(500.0, &mut states, syncers);
        assert_eq!(max, stats.max_ms);
    }

    #[test]
    fn record_replica_ready_state_skips_a_mid_hydration_snapshot() {
        let mut states = vec![];
        record_replica_ready_state(&mut states, None, Some(100.0), true);
        record_replica_ready_state(&mut states, Some("01".into()), None, true);
        assert!(states.is_empty());
    }

    #[test]
    fn record_replica_ready_state_skips_a_non_newer_watermark() {
        let mut states = vec![state("02", 200.0)];
        record_replica_ready_state(&mut states, Some("02".into()), Some(300.0), true);
        record_replica_ready_state(&mut states, Some("01".into()), Some(300.0), true);
        assert_eq!(
            states.len(),
            1,
            "neither an equal nor an older watermark should be appended"
        );
    }

    #[test]
    fn record_replica_ready_state_appends_a_strictly_newer_watermark() {
        let mut states = vec![state("01", 100.0)];
        record_replica_ready_state(&mut states, Some("02".into()), Some(200.0), true);
        assert_eq!(states.len(), 2);
        assert_eq!(states[1], state("02", 200.0));
    }

    #[test]
    fn record_replica_ready_state_clears_everything_once_no_view_syncers_remain() {
        let mut states = samples();
        record_replica_ready_state(&mut states, Some("05".into()), Some(500.0), false);
        assert!(
            states.is_empty(),
            "an empty view-syncer set means nobody needs any of this history"
        );
    }

    #[test]
    fn record_replica_ready_state_still_applies_the_size_bound() {
        let mut states: Vec<ReplicaReadyState> = (0..MAX_REPLICA_READY_STATES)
            .map(|i| state(&i.to_string(), i as f64))
            .collect();
        record_replica_ready_state(&mut states, Some("newest".into()), Some(999_999.0), true);
        assert_eq!(states.len(), MAX_REPLICA_READY_STATES);
        assert_eq!(states.last().unwrap().watermark, "newest");
    }

    #[test]
    fn serving_lag_stats_cache_memoizes_until_cleared() {
        let mut cache = ServingLagStatsCache::default();
        let mut states = samples();
        let syncers = || {
            vec![ServingLagViewSyncer {
                created_at_ms: 0.0,
                served_version: Some("02"),
            }]
        };

        let first = cache.get_or_compute(500.0, &mut states, syncers());
        // A drastically different `now` would change the result if actually
        // recomputed — proves the second call returns the cached value.
        let second = cache.get_or_compute(999_999.0, &mut states, syncers());
        assert_eq!(first, second);

        cache.clear();
        let third = cache.get_or_compute(999_999.0, &mut states, syncers());
        assert_ne!(first, third, "after clear(), the next call must recompute");
    }
}
