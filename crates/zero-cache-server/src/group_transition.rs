//! The GROUP-scoped half of a served connection's handler — the CVR/row/query
//! transition core extracted from [`crate::live_connection::DesiredQueriesHandler`]
//! (query-pipeline redesign §6, group-loop plan increment 1).
//!
//! [`GroupTransitionCore`] owns everything that is state of the CLIENT GROUP
//! rather than of one WebSocket connection: the replica handle, the working
//! CVR ([`CvrQueryHandler`]), the group's row records/bodies and pending row
//! updates, the desired-put registry and hydration-tracking index, the query
//! pipeline (owned or group-shared), durable CVR persistence, and the
//! defer/barrier/group-cell plumbing. Its methods are the group-scoped
//! transitions: reload/adopt the group CVR, apply a desired-queries patch and
//! hydrate its puts, advance the pipeline into row patches, and persist the
//! transition.
//!
//! Per-connection DELIVERY state (poke sequencing, base/last cookies, poked
//! last-mutation-ids, staged hydration) and all auth/mutation/inspect state
//! stay on `DesiredQueriesHandler`, which delegates here. This split is the
//! seam the upcoming per-group processor loop attaches to: the loop will own
//! ONE `GroupTransitionCore` per client group and fan the resulting patches
//! out to every connection's own poke state.

use std::collections::{BTreeMap, HashMap, HashSet};

use zero_cache_protocol::ast::{
    ColumnReference, Condition, CorrelatedSubquery, Direction, LiteralValue, Ordering,
    SimpleOperator, ValuePosition,
};
use zero_cache_protocol::queries_patch::{UpQueriesPatchOp, UpQueriesPutOp};
use zero_cache_shared::bigint_json::JsonValue;
use zero_cache_sqlite::lite_tables::list_tables;
use zero_cache_sqlite::StatementRunner;
use zero_cache_view_syncer::client_patch::PatchToVersion;
use zero_cache_view_syncer::cvr_delete_unreferenced_rows::ExistingRow as DeleteExistingRow;
use zero_cache_view_syncer::cvr_query_handler::CvrQueryHandler;
use zero_cache_view_syncer::cvr_row_cache_sql::RowUpdate;
use zero_cache_view_syncer::cvr_row_received::ExistingRow as ReceivedExistingRow;
use zero_cache_view_syncer::cvr_types::{CvrRecordBase, RowId, RowRecord};
use zero_cache_view_syncer::pipeline_driver::{PipelineRowChange, PipelineRowChangeKind};
use zero_cache_zql::ivm::constraint::PrimaryKey;
use zero_cache_zql::ivm::data::Row as ZqlRow;

use crate::live_connection::CvrPersistence;
use crate::live_hydration::{
    hydrate_patches_from_sqlite_with_row_updates, hydrate_rows_from_sqlite_with_row_updates,
    HydratePatchesResult, RowIdentity,
};

#[derive(Clone)]
struct HydrationPlan {
    table_name: String,
    primary_key: PrimaryKey,
    columns: Vec<String>,
}

fn replica_row_version(row: &ZqlRow, fallback: impl FnOnce() -> String) -> String {
    match row.iter().find(|(name, _)| name == "_0_version") {
        Some((_, JsonValue::String(version))) => version.clone(),
        Some((_, version)) => version.stringify(),
        None => fallback(),
    }
}

/// A deterministic string key uniquely identifying a [`RowId`], for O(1) hashed
/// lookup/dedup of row records/bodies. `RowId`'s `row_key` values are
/// `JsonValue` (not `Hash`/`Ord`), so the row can't key a map directly; this
/// serializes `schema`, `table`, and the (ordered `BTreeMap`) key columns into a
/// collision-free string using control-char separators.
fn row_id_key(id: &RowId) -> String {
    let mut key = String::with_capacity(id.schema.len() + id.table.len() + 16);
    key.push_str(&id.schema);
    key.push('\u{1}');
    key.push_str(&id.table);
    for (column, value) in &id.row_key {
        key.push('\u{2}');
        key.push_str(column);
        key.push('\u{3}');
        key.push_str(&value.stringify());
    }
    key
}

fn identity_for_plan(plan: &HydrationPlan, query_id: &str) -> RowIdentity<String> {
    let table = plan.table_name.clone();
    let primary_key = plan.primary_key.clone();
    let row_key_primary_key = primary_key.clone();
    let ref_counts_query_id = query_id.to_string();
    let version_primary_key = primary_key.clone();
    let wire_primary_key = primary_key;
    RowIdentity {
        row_key: Box::new(move |row| row_key_from_primary_key(row, &row_key_primary_key)),
        row_ref_counts: Box::new(move |_row| BTreeMap::from([(ref_counts_query_id.clone(), 1i64)])),
        row_version: Box::new(move |row| {
            replica_row_version(row, || {
                format!("v{}", row_key_from_primary_key(row, &version_primary_key))
            })
        }),
        wire_row_id: Box::new(move |key: &String| RowId {
            schema: "public".into(),
            table: table.clone(),
            row_key: row_id_from_key_string(key, &wire_primary_key),
        }),
    }
}

/// The SQL `ORDER BY` for a query's root/related hydration: the query's own
/// `orderBy` when present, otherwise plain primary-key order. When an explicit
/// `orderBy` is given, the primary key is appended (skipping columns the
/// `orderBy` already names) so the ordering is total and the top-N kept under a
/// `limit` is deterministic even for a non-unique sort column.
fn sort_for_hydration(plan: &HydrationPlan, order_by: Option<&Ordering>) -> Ordering {
    match order_by {
        Some(order_by) if !order_by.is_empty() => {
            let mut sort: Ordering = order_by.clone();
            for key in &plan.primary_key {
                if !sort.iter().any(|(col, _)| col == key) {
                    sort.push((key.clone(), Direction::Asc));
                }
            }
            sort
        }
        _ => plan
            .primary_key
            .iter()
            .map(|s| (s.to_string(), Direction::Asc))
            .collect(),
    }
}

fn row_key_from_primary_key(row: &ZqlRow, primary_key: &[String]) -> String {
    primary_key
        .iter()
        .map(|field| {
            let value = row
                .iter()
                .find(|(name, _)| name == field)
                .map(|(_, value)| value.clone())
                .unwrap_or(JsonValue::Null);
            format!("{}={}", field, value.stringify())
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn row_id_from_key_string(key: &str, primary_key: &[String]) -> BTreeMap<String, JsonValue> {
    let mut row_key = BTreeMap::new();
    for (index, field) in primary_key.iter().enumerate() {
        let prefix = format!("{field}=");
        let raw = key
            .split('|')
            .nth(index)
            .and_then(|part| part.strip_prefix(&prefix))
            .unwrap_or("null");
        let value = zero_cache_shared::bigint_json::parse(raw).unwrap_or(JsonValue::Null);
        row_key.insert(field.clone(), value);
    }
    row_key
}

fn row_key_string_from_row_id(id: &RowId, primary_key: &[String]) -> Option<String> {
    primary_key
        .iter()
        .map(|field| {
            id.row_key
                .get(field)
                .map(|value| format!("{}={}", field, value.stringify()))
        })
        .collect::<Option<Vec<_>>>()
        .map(|parts| parts.join("|"))
}

fn hydration_plan_from_ast(
    db: &StatementRunner,
    ast: &zero_cache_protocol::ast::Ast,
) -> Result<HydrationPlan, zero_cache_sqlite::DbError> {
    let tables = list_tables(db)?;
    let Some(table) = tables.into_iter().find(|table| table.name == ast.table) else {
        return Err(zero_cache_sqlite::DbError(format!(
            "desired-query hydration table `{}` is not in SQLite replica",
            ast.table
        )));
    };
    let primary_key = table.primary_key.unwrap_or_default();
    Ok(HydrationPlan {
        table_name: table.name,
        primary_key,
        columns: table.columns.into_iter().map(|(name, _)| name).collect(),
    })
}

fn json_to_literal(value: &JsonValue) -> Option<LiteralValue> {
    match value {
        JsonValue::String(s) => Some(LiteralValue::String(s.clone())),
        JsonValue::Number(n) => Some(LiteralValue::Number(*n)),
        JsonValue::BigInt(n) => n.to_string().parse::<f64>().ok().map(LiteralValue::Number),
        JsonValue::Bool(b) => Some(LiteralValue::Bool(*b)),
        JsonValue::Null => Some(LiteralValue::Null),
        JsonValue::Array(_) | JsonValue::Object(_) => None,
    }
}

fn related_filter_from_parent_rows(
    parent_rows: &[(RowId, zero_cache_protocol::row_patch::Row)],
    related: &CorrelatedSubquery,
) -> Option<Condition> {
    if related.correlation.parent_field.is_empty()
        || related.correlation.parent_field.len() != related.correlation.child_field.len()
    {
        return None;
    }

    let correlation_filter = if parent_rows.is_empty() {
        Condition::Or { conditions: vec![] }
    } else if related.correlation.parent_field.len() == 1 {
        let parent_field = &related.correlation.parent_field[0];
        let child_field = &related.correlation.child_field[0];
        let mut values = Vec::new();
        for (_, row) in parent_rows {
            if let Some((_, value)) = row.iter().find(|(field, _)| field == parent_field) {
                if let Some(literal) = json_to_literal(value) {
                    values.push(literal);
                }
            }
        }
        if values.is_empty() {
            Condition::Or { conditions: vec![] }
        } else {
            Condition::Simple {
                op: SimpleOperator::In,
                left: ValuePosition::Column(ColumnReference {
                    name: child_field.clone(),
                }),
                right: ValuePosition::Literal(LiteralValue::Array(values)),
            }
        }
    } else {
        let mut tuple_conditions = Vec::new();
        for (_, row) in parent_rows {
            let mut field_conditions = Vec::new();
            for (parent_field, child_field) in related
                .correlation
                .parent_field
                .iter()
                .zip(&related.correlation.child_field)
            {
                let Some((_, value)) = row.iter().find(|(field, _)| field == parent_field) else {
                    field_conditions.clear();
                    break;
                };
                let Some(literal) = json_to_literal(value) else {
                    field_conditions.clear();
                    break;
                };
                field_conditions.push(Condition::Simple {
                    op: SimpleOperator::Eq,
                    left: ValuePosition::Column(ColumnReference {
                        name: child_field.clone(),
                    }),
                    right: ValuePosition::Literal(literal),
                });
            }
            if !field_conditions.is_empty() {
                tuple_conditions.push(Condition::And {
                    conditions: field_conditions,
                });
            }
        }
        Condition::Or {
            conditions: tuple_conditions,
        }
    };

    Some(match related.subquery.where_.as_ref() {
        Some(where_) => Condition::And {
            conditions: vec![where_.clone(), correlation_filter],
        },
        None => correlation_filter,
    })
}

/// Collects correlated subqueries from one condition scope. Subqueries nested
/// inside a collected relation's own AST are intentionally left for the
/// recursive hydration pass, after that relation's child rows are known.
fn correlated_subqueries_in_condition(condition: &Condition) -> Vec<CorrelatedSubquery> {
    fn collect(condition: &Condition, out: &mut Vec<CorrelatedSubquery>) {
        match condition {
            Condition::Simple { .. } => {}
            Condition::And { conditions } | Condition::Or { conditions } => {
                for condition in conditions {
                    collect(condition, out);
                }
            }
            Condition::CorrelatedSubquery { related, .. } => out.push(related.clone()),
        }
    }

    let mut related = Vec::new();
    collect(condition, &mut related);
    related
}

fn hydrate_related_rows_recursive(
    db: &StatementRunner,
    cvr: &mut zero_cache_view_syncer::cvr_types::Cvr,
    orig_version: &zero_cache_view_syncer::cvr_version::CvrVersion,
    query_hash: &str,
    parent_rows: &[(RowId, zero_cache_protocol::row_patch::Row)],
    related: &[CorrelatedSubquery],
) -> Result<HydratePatchesResult, zero_cache_sqlite::DbError> {
    let mut result = HydratePatchesResult {
        patches: Vec::new(),
        row_updates: Vec::new(),
        row_bodies: Vec::new(),
    };

    for relation in related {
        let child_plan = hydration_plan_from_ast(db, &relation.subquery)?;
        let child_identity = identity_for_plan(&child_plan, query_hash);
        let child_sort = sort_for_hydration(&child_plan, relation.subquery.order_by.as_ref());
        let child_limit = relation.subquery.limit.map(|n| n.max(0.0) as usize);
        let child_start = relation.subquery.start.as_ref();

        // A related subquery's `limit`/`start` are per-parent (upstream applies
        // them in `Take`/skip operators downstream of the correlation). Fetching
        // all children for all parents in one `IN (...)` read and then
        // truncating/seeking would be a wrong global cap/cursor, so when either
        // is present we fetch each parent's children separately (single-parent
        // correlation filter + child ordering + per-parent truncate/cursor = a
        // correct per-parent window). Without them the batched single-read path
        // stays, since ordering alone drops no rows.
        let parent_batches: Vec<Vec<(RowId, zero_cache_protocol::row_patch::Row)>> =
            if child_limit.is_some() || child_start.is_some() {
                parent_rows.iter().map(|row| vec![row.clone()]).collect()
            } else {
                vec![parent_rows.to_vec()]
            };

        let mut child_result = HydratePatchesResult {
            patches: Vec::new(),
            row_updates: Vec::new(),
            row_bodies: Vec::new(),
        };
        for batch in &parent_batches {
            let Some(child_filter) = related_filter_from_parent_rows(batch, relation) else {
                continue;
            };
            let batch_result = hydrate_rows_from_sqlite_with_row_updates(
                db,
                child_plan.table_name.clone(),
                child_plan.primary_key.clone(),
                child_sort.clone(),
                child_plan.columns.clone(),
                cvr,
                orig_version,
                &child_identity,
                &HashMap::new(),
                Some(&child_filter),
                child_limit,
                child_start,
            )?;
            child_result.patches.extend(batch_result.patches);
            child_result.row_updates.extend(batch_result.row_updates);
            child_result.row_bodies.extend(batch_result.row_bodies);
        }

        // A related row is evaluated by the client with the subquery's full
        // local pipeline. Hydrate correlated-subquery rows referenced by its
        // `where_` as well as rows requested for result shaping via
        // `related`, otherwise a nested `whereExists` sees an empty local
        // child table and incorrectly removes this row on the client.
        if let Some(where_) = &relation.subquery.where_ {
            let exists_related = correlated_subqueries_in_condition(where_);
            let nested_result = hydrate_related_rows_recursive(
                db,
                cvr,
                orig_version,
                query_hash,
                &child_result.row_bodies,
                &exists_related,
            )?;
            child_result.row_updates.extend(nested_result.row_updates);
            child_result.row_bodies.extend(nested_result.row_bodies);
            child_result.patches.extend(nested_result.patches);
        }

        if let Some(nested) = &relation.subquery.related {
            let nested_result = hydrate_related_rows_recursive(
                db,
                cvr,
                orig_version,
                query_hash,
                &child_result.row_bodies,
                nested,
            )?;
            child_result.row_updates.extend(nested_result.row_updates);
            child_result.row_bodies.extend(nested_result.row_bodies);
            child_result.patches.extend(nested_result.patches);
        }

        result.row_updates.extend(child_result.row_updates);
        result.row_bodies.extend(child_result.row_bodies);
        result.patches.extend(child_result.patches);
    }

    Ok(result)
}

/// Query hashes `client_id` newly desired in this transition — the hashes whose
/// desired-queries config patch (a `Put` addressed to this client) came out of
/// `apply_desired_queries_patch`. Used to distinguish "this client just asked
/// for the query" (needs the group's got state replayed at the current version)
/// from a re-put of something it already desired.
fn newly_desired_hashes(config_patches: &[PatchToVersion], client_id: &str) -> HashSet<String> {
    config_patches
        .iter()
        .filter_map(|patch| match &patch.patch {
            zero_cache_view_syncer::client_patch::Patch::Config(config)
                if config.op == zero_cache_view_syncer::cvr_types::PatchOp::Put
                    && config.client_id.as_deref() == Some(client_id) =>
            {
                Some(config.id.clone())
            }
            _ => None,
        })
        .collect()
}

/// The result of applying a desired-queries patch in STAGED form: the config
/// patches (queries put/deleted) separated from the hydration patches (row/got
/// state) so the caller can emit them as two chained pokes.
pub(crate) struct StagedPatch {
    /// The CVR version before the patch was applied.
    pub(crate) orig_version: zero_cache_view_syncer::cvr_version::CvrVersion,
    /// The version the config patches land at (the hydration poke's base).
    pub(crate) config_version: zero_cache_view_syncer::cvr_version::CvrVersion,
    /// Desired-queries config patches.
    pub(crate) config: Vec<PatchToVersion>,
    /// Hydration (row/got) patches for the puts in the patch.
    pub(crate) hydration: Vec<PatchToVersion>,
}

/// The group-scoped transition core extracted from `DesiredQueriesHandler`
/// (see the module doc). One instance per connection today; the per-group
/// processor loop will own one per client group.
pub(crate) struct GroupTransitionCore {
    pub(crate) db: StatementRunner,
    /// The real v1.7-style persistent pipeline owner. Synced production
    /// handlers use this for commit advancement; the legacy hydration path is
    /// retained only for initial query hydration while wire/CVR state is built.
    /// Either a per-connection driver ([`crate::query_pipeline::QueryPipeline::Owned`],
    /// default) or the group-shared pipeline
    /// ([`crate::query_pipeline::QueryPipeline::Shared`], `ZERO_GROUP_OWNERSHIP`).
    pub(crate) query_pipeline: Option<crate::query_pipeline::QueryPipeline>,
    pub(crate) cvr_handler: CvrQueryHandler,
    pub(crate) client_group_id: String,
    // `Arc`-wrapped so checking in / snapshotting the group CVR clones the `Arc`
    // (cheap) rather than the full row vecs; mutations copy-on-write via
    // `Arc::make_mut`. This removes the per-connect/per-commit 1000-row clone
    // that dominated the flag-on hydrate path.
    pub(crate) row_records: std::sync::Arc<Vec<RowRecord>>,
    pub(crate) row_bodies: std::sync::Arc<Vec<(RowId, zero_cache_protocol::row_patch::Row)>>,
    pub(crate) tracked: HashSet<String>,
    /// Per-client last-mutation-id counters, standing in for the real
    /// upstream-Postgres `clients` table upsert
    /// (`zero_cache_mutagen::apply_mutation::apply_crud_mutation`, built and
    /// live-tested separately against real Postgres). See
    /// `DesiredQueriesHandler::apply_push`'s doc for why push mutations are
    /// applied against the local replica there instead of calling that async
    /// executor.
    pub(crate) last_mutation_ids: BTreeMap<String, i64>,
    /// The group's currently-desired PUT ops for this client, kept so the
    /// queries can be RE-hydrated against the replica on each upstream commit
    /// (live sync). Keyed by query hash; a `del`/`clear` removes them.
    pub(crate) desired_puts: BTreeMap<String, UpQueriesPutOp>,
    /// Optional shared CVR persistence for synced deployments. Standalone
    /// handlers retain the existing in-memory behavior when this is absent.
    pub(crate) cvr_persistence: Option<CvrPersistence>,
    /// When set (via `ZERO_DEFER_CVR_ROWS`), the CVR row-record flush is moved
    /// off the hydration critical path: the config/version transaction commits
    /// synchronously before the poke is returned, and the rows land in a spawned
    /// task chained through `cvr_row_flush_barrier`. Off by default — the flag
    /// keeps the flush a single synchronous config+rows transaction.
    pub(crate) defer_cvr_rows: bool,
    /// Process-local per-client-group barrier ordering deferred row flushes and
    /// letting a reconnect load await pending rows. Present only when
    /// `defer_cvr_rows` is enabled and durable CVR persistence is configured.
    pub(crate) cvr_row_flush_barrier:
        Option<std::sync::Arc<crate::cvr_row_flush_barrier::RowFlushBarrier>>,
    /// Group-owned CVR cell (redesign §6 C2, group-ownership path): the ONE
    /// in-memory CVR shared by every connection in this client group. When
    /// set, a transition checks the group state out here (instead of
    /// re-loading the durable CVR from Postgres) and checks it back in after
    /// a successful flush; see [`zero_cache_view_syncer::group_cvr`].
    pub(crate) group_cvr: Option<std::sync::Arc<zero_cache_view_syncer::group_cvr::GroupCvrCell>>,
    /// Row-cache changes produced by hydration since the last durable flush.
    pub(crate) pending_row_updates: Vec<RowUpdate>,
}

impl GroupTransitionCore {
    pub(crate) fn new(db: StatementRunner, client_group_id: &str, client_id: &str) -> Self {
        GroupTransitionCore {
            db,
            query_pipeline: None,
            cvr_handler: CvrQueryHandler::new(client_group_id, client_id, None),
            client_group_id: client_group_id.to_string(),
            row_records: std::sync::Arc::new(Vec::new()),
            row_bodies: std::sync::Arc::new(Vec::new()),
            tracked: HashSet::new(),
            last_mutation_ids: BTreeMap::new(),
            desired_puts: BTreeMap::new(),
            cvr_persistence: None,
            defer_cvr_rows: false,
            cvr_row_flush_barrier: None,
            group_cvr: None,
            pending_row_updates: Vec::new(),
        }
    }

    /// Refreshes the working group state before a transition: adopt the group
    /// cell's checked-out state, or reload the durable CVR. Returns `true`
    /// when the working state was replaced (the caller must discard any staged
    /// per-connection work derived from the previous state) and `false` when
    /// neither a group cell nor durable persistence supplied fresh state.
    pub(crate) async fn refresh_durable_cvr(&mut self) -> Result<bool, String> {
        // Group-owned CVR: adopt the group's in-memory state instead of
        // re-loading the durable CVR. The check-out is exclusive (the cell is
        // emptied) and race-free — every CVR transition holds the group's
        // transition lock across refresh→apply→persist. An empty cell (first
        // connection of the group, or a failed/CAS-lost transition) falls
        // through to the durable load below.
        if let Some(cell) = self.group_cvr.clone() {
            if let Some(state) = cell.take() {
                self.adopt_group_state(state);
                return Ok(true);
            }
        }
        let Some(persistence) = self.cvr_persistence.as_ref() else {
            return Ok(false);
        };
        // Preserve the durable invariant on this single node: a reconnect load
        // must never read durable rows that a deferred flush has not committed
        // yet. Await the group's pending row flush before reading.
        if let Some(barrier) = &self.cvr_row_flush_barrier {
            barrier.wait_for_pending().await;
        }
        let (cvr, rows) = persistence.load(&self.client_group_id).await?;
        let client_id = self.cvr_handler.client_id().to_string();
        self.cvr_handler = CvrQueryHandler::from_cvr(cvr, &self.client_group_id, &client_id);
        self.desired_puts = self
            .cvr_handler
            .desired_puts_for_client()
            .into_iter()
            .map(|put| (put.hash.clone(), put))
            .collect();
        self.row_records = std::sync::Arc::new(rows);
        self.pending_row_updates.clear();
        self.tracked
            .retain(|hash| self.desired_puts.contains_key(hash));
        Ok(true)
    }

    /// Adopts the checked-out group CVR state as this transition's working
    /// copy, re-deriving this connection's per-client view (its desired puts
    /// and hydration index) — the in-memory analogue of the durable reload.
    fn adopt_group_state(&mut self, state: zero_cache_view_syncer::group_cvr::GroupCvrState) {
        let client_id = self.cvr_handler.client_id().to_string();
        self.cvr_handler = CvrQueryHandler::from_cvr(state.cvr, &self.client_group_id, &client_id);
        self.desired_puts = self
            .cvr_handler
            .desired_puts_for_client()
            .into_iter()
            .map(|put| (put.hash.clone(), put))
            .collect();
        self.row_records = state.row_records;
        self.row_bodies = state.row_bodies;
        self.pending_row_updates = state.pending_row_updates;
        self.tracked
            .retain(|hash| self.desired_puts.contains_key(hash));
    }

    /// Checks this transition's resulting state back into the group cell as
    /// the group truth. Called only after the durable flush succeeded (or when
    /// no durable persistence is configured, where the cell IS the truth); a
    /// failed transition leaves the cell empty so the next one reloads from
    /// Postgres.
    fn checkin_group_state(&self) {
        let Some(cell) = &self.group_cvr else {
            return;
        };
        cell.put(zero_cache_view_syncer::group_cvr::GroupCvrState {
            cvr: self.cvr_handler.cvr.clone(),
            row_records: self.row_records.clone(),
            row_bodies: self.row_bodies.clone(),
            pending_row_updates: self.pending_row_updates.clone(),
        });
    }

    pub(crate) async fn persist_transition(
        &mut self,
        before: &zero_cache_view_syncer::cvr_types::Cvr,
    ) -> Result<(), String> {
        // Take a barrier handle up front so the mutable borrow of persistence
        // below does not conflict with the immutable barrier field.
        let barrier = self.cvr_row_flush_barrier.clone();
        let defer = self.defer_cvr_rows;
        let client_group_id = self.client_group_id.clone();
        let Some(persistence) = self.cvr_persistence.as_mut() else {
            // No durable store: the group cell (when present) is the group's
            // source of truth — check the transition's result in directly.
            self.checkin_group_state();
            return Ok(());
        };
        let after = self.cvr_handler.cvr.clone();
        let row_updates = std::mem::take(&mut self.pending_row_updates);

        // Deferred path: commit config synchronously (keeping the version CAS on
        // the critical path), then spawn the row-record flush behind the group's
        // barrier. Only taken with the flag ON and a barrier configured; the
        // barrier is what preserves the durable invariant on this single node.
        if defer {
            if let Some(barrier) = barrier {
                match persistence.flush_config_only(before, &after).await {
                    Ok(rows_version) => {
                        persistence.spawn_deferred_rows_flush(
                            &barrier,
                            client_group_id,
                            row_updates,
                            rows_version,
                        );
                        self.checkin_group_state();
                        return Ok(());
                    }
                    Err(error) => {
                        // The CAS lost (or the config write failed): no rows are
                        // deferred, exactly as the synchronous path writes none.
                        // The group cell stays empty, so the retry re-loads the
                        // durable CVR another writer moved.
                        self.pending_row_updates = row_updates;
                        return Err(error);
                    }
                }
            }
        }

        // Default path (flag OFF, or no barrier): a single synchronous
        // config+rows transaction — byte-identical to the pre-flag behavior.
        if let Err(error) = persistence.flush(before, &after, &row_updates).await {
            self.pending_row_updates = row_updates;
            return Err(error);
        }
        self.checkin_group_state();
        Ok(())
    }

    /// Applies a desired-queries patch to the CVR and hydrates its puts,
    /// returning the pre-transition version and ALL resulting patches merged
    /// (the un-staged shape `apply_and_poke` emits as one poke).
    /// `resolved_asts` carries the per-put transformed AST the CONNECTION
    /// resolved (custom-query transform + its read permissions) — the group
    /// core never consults per-connection auth state.
    pub(crate) fn apply_desired_patch(
        &mut self,
        patch: &[UpQueriesPatchOp],
        resolved_asts: &HashMap<String, Option<zero_cache_protocol::ast::Ast>>,
    ) -> Result<
        (
            zero_cache_view_syncer::cvr_version::CvrVersion,
            Vec<PatchToVersion>,
        ),
        String,
    > {
        let orig_version = self.cvr_handler.version().clone();
        let mut patches = self
            .cvr_handler
            .apply_desired_queries_patch(&patch.to_vec());
        let newly_desired = newly_desired_hashes(&patches, self.cvr_handler.client_id());

        // Hydrate every newly-put query this connection recognizes, and
        // remember/forget the put ops so they can be re-hydrated on later
        // upstream commits (see `DesiredQueriesHandler::rehydrate_tracked`).
        for op in patch {
            match op {
                UpQueriesPatchOp::Put(p) => {
                    self.desired_puts.insert(p.hash.clone(), p.clone());
                    let transformed_ast = resolved_asts.get(&p.hash).cloned().flatten();
                    patches.extend(self.hydrate_put(
                        p,
                        transformed_ast,
                        &orig_version,
                        true,
                        newly_desired.contains(&p.hash),
                    )?);
                }
                UpQueriesPatchOp::Del(d) => {
                    self.desired_puts.remove(&d.hash);
                    if let Some(driver) = self.query_pipeline.as_mut() {
                        driver.remove_query(&d.hash);
                    }
                }
                UpQueriesPatchOp::Clear(_) => {
                    if let Some(driver) = self.query_pipeline.as_mut() {
                        for hash in self.desired_puts.keys() {
                            driver.remove_query(hash);
                        }
                    }
                    self.desired_puts.clear();
                }
            }
        }
        Ok((orig_version, patches))
    }

    /// The STAGED variant of [`Self::apply_desired_patch`]: config patches and
    /// hydration patches are returned separately so the caller can emit the
    /// config poke first and chain the hydration poke on its cookie. When the
    /// patch contains a put, the CVR version is bumped past the config version
    /// up front so hydration rows land at a later version.
    pub(crate) fn apply_desired_patch_staged(
        &mut self,
        patch: &[UpQueriesPatchOp],
        resolved_asts: &HashMap<String, Option<zero_cache_protocol::ast::Ast>>,
    ) -> Result<StagedPatch, String> {
        let orig_version = self.cvr_handler.version().clone();
        let config = self
            .cvr_handler
            .apply_desired_queries_patch(&patch.to_vec());
        let config_version = config
            .iter()
            .map(|patch| patch.to_version.clone())
            .max_by_key(|version| {
                zero_cache_view_syncer::cvr_version::version_to_cookie(version).unwrap_or_default()
            })
            .unwrap_or_else(|| orig_version.clone());
        if !config.is_empty()
            && patch
                .iter()
                .any(|op| matches!(op, UpQueriesPatchOp::Put(_)))
        {
            zero_cache_view_syncer::cvr_updater::ensure_new_version(
                &config_version,
                &mut self.cvr_handler.cvr.version,
            );
        }
        let newly_desired = newly_desired_hashes(&config, self.cvr_handler.client_id());
        let mut hydration = Vec::new();
        for op in patch {
            match op {
                UpQueriesPatchOp::Put(p) => {
                    self.desired_puts.insert(p.hash.clone(), p.clone());
                    let transformed_ast = resolved_asts.get(&p.hash).cloned().flatten();
                    hydration.extend(self.hydrate_put(
                        p,
                        transformed_ast,
                        &config_version,
                        true,
                        newly_desired.contains(&p.hash),
                    )?);
                }
                UpQueriesPatchOp::Del(d) => {
                    self.desired_puts.remove(&d.hash);
                    if let Some(driver) = self.query_pipeline.as_mut() {
                        driver.remove_query(&d.hash);
                    }
                }
                UpQueriesPatchOp::Clear(_) => {
                    if let Some(driver) = self.query_pipeline.as_mut() {
                        for hash in self.desired_puts.keys() {
                            driver.remove_query(hash);
                        }
                    }
                    self.desired_puts.clear();
                }
            }
        }
        Ok(StagedPatch {
            orig_version,
            config_version,
            config,
            hydration,
        })
    }

    /// The advance half of `DesiredQueriesHandler::rehydrate_tracked`: brings
    /// the query pipeline to the replica head and converts its row changes to
    /// client patches at the CURRENT (already-bumped) CVR version. Requires a
    /// pipeline to be configured.
    pub(crate) fn advance_pipeline_to_patches(&mut self) -> Result<Vec<PatchToVersion>, String> {
        let changes = self
            .query_pipeline
            .as_mut()
            .expect("advance_pipeline_to_patches requires a query pipeline")
            .advance()
            .map_err(|error| format!("incremental pipeline advance failed: {error}"))?;
        Ok(self.pipeline_changes_to_patches(changes))
    }

    /// Single-owner (group-loop) variant of [`Self::advance_pipeline_to_patches`]:
    /// advances the group pipeline ONCE via the single-owner path (the loop is
    /// the group's sole advancer, so the per-connection fan-out cursors are
    /// bypassed) and converts the resulting row changes to client patches at the
    /// CURRENT (already-bumped) CVR version.
    pub(crate) fn advance_group_pipeline_to_patches(
        &mut self,
    ) -> Result<Vec<PatchToVersion>, String> {
        let changes = self
            .query_pipeline
            .as_mut()
            .expect("advance_group_pipeline_to_patches requires a query pipeline")
            .advance_single_owner()
            .map_err(|error| format!("incremental pipeline advance failed: {error}"))?;
        Ok(self.pipeline_changes_to_patches(changes))
    }

    /// Repoints the group core at `client_id` for a desired-queries transition
    /// (group loop only): switches the CVR handler's active client IN PLACE
    /// (clone-free — the group CVR is shared, not copied), rebuilds this client's
    /// desired-put index from the group CVR, and keys the shared pipeline's
    /// ref-count at this client so its desire/undesire is attributed correctly.
    pub(crate) fn set_active_client(&mut self, client_id: &str) {
        self.cvr_handler.set_client_id(client_id);
        self.desired_puts = self
            .cvr_handler
            .desired_puts_for_client()
            .into_iter()
            .map(|put| (put.hash.clone(), put))
            .collect();
        if let Some(pipeline) = self.query_pipeline.as_mut() {
            pipeline.set_client_id(client_id);
        }
    }

    pub(crate) fn pipeline_changes_to_patches(
        &mut self,
        changes: Vec<PipelineRowChange>,
    ) -> Vec<PatchToVersion> {
        use zero_cache_view_syncer::client_patch::{
            ClientDeleteRowPatch, ClientPutRowPatch, ClientRowPatch, Patch,
        };

        let version = self.cvr_handler.cvr.version.clone();
        let mut patches = Vec::new();
        // Index the current row records once (O(n)) for O(1) lookup, and batch
        // all record/body writes into single O(n+m) applies at the end — the
        // per-change `.find()` + per-row `apply_row_*` was O(n²) (the hydrate
        // bottleneck on the 1-CPU bench). The index is updated in-place so
        // multiple changes to the same row within one batch see prior writes.
        let mut index: std::collections::HashMap<String, RowRecord> = self
            .row_records
            .iter()
            .map(|record| (row_id_key(&record.id), record.clone()))
            .collect();
        let mut row_updates: Vec<(RowId, Option<RowRecord>)> = Vec::new();
        let mut body_updates: Vec<(RowId, zero_cache_protocol::row_patch::Row)> = Vec::new();
        for change in changes {
            let id = RowId {
                schema: "public".into(),
                table: change.table.clone(),
                row_key: change.row_key.clone(),
            };
            let key = row_id_key(&id);
            match change.kind {
                PipelineRowChangeKind::Add | PipelineRowChangeKind::Edit => {
                    let mut refs = index
                        .get(&key)
                        .and_then(|record| record.ref_counts.clone())
                        .unwrap_or_default();
                    refs.insert(change.query_id.clone(), 1);
                    let row_version = change
                        .row
                        .iter()
                        .find_map(|(column, value)| {
                            (column == zero_cache_types::pg_to_lite::ZERO_VERSION_COLUMN_NAME)
                                .then_some(value)
                                .and_then(|value| match value {
                                    JsonValue::String(value) => Some(value.clone()),
                                    _ => None,
                                })
                        })
                        .unwrap_or_else(|| version.state_version.clone());
                    let record = RowRecord {
                        base: CvrRecordBase {
                            patch_version: version.clone(),
                        },
                        id: id.clone(),
                        row_version,
                        ref_counts: Some(refs),
                    };
                    index.insert(key, record.clone());
                    row_updates.push((id.clone(), Some(record)));
                    body_updates.push((id.clone(), change.row.clone()));
                    patches.push(PatchToVersion {
                        patch: Patch::Row(ClientRowPatch::Put(ClientPutRowPatch {
                            id,
                            contents: change.row,
                        })),
                        to_version: version.clone(),
                    });
                }
                PipelineRowChangeKind::Remove => {
                    let existing = index.get(&key).cloned();
                    let mut refs = existing
                        .as_ref()
                        .and_then(|record| record.ref_counts.clone())
                        .unwrap_or_default();
                    refs.remove(&change.query_id);
                    if refs.is_empty() {
                        let tombstone = RowRecord {
                            base: CvrRecordBase {
                                patch_version: version.clone(),
                            },
                            id: id.clone(),
                            row_version: existing
                                .map(|record| record.row_version)
                                .unwrap_or_else(|| version.state_version.clone()),
                            ref_counts: None,
                        };
                        index.insert(key, tombstone.clone());
                        // A tombstone (ref_counts=None) drops its body in
                        // `apply_row_updates`, so no separate body removal here.
                        row_updates.push((id.clone(), Some(tombstone)));
                        patches.push(PatchToVersion {
                            patch: Patch::Row(ClientRowPatch::Delete(ClientDeleteRowPatch { id })),
                            to_version: version.clone(),
                        });
                    } else if let Some(mut record) = existing {
                        record.base.patch_version = version.clone();
                        record.ref_counts = Some(refs);
                        index.insert(key, record.clone());
                        row_updates.push((id, Some(record)));
                    }
                }
            }
        }
        // Apply all accumulated record/body writes in a single O(n+m) pass.
        self.apply_row_updates(row_updates);
        self.apply_row_bodies(body_updates);
        patches
    }

    /// Refreshes the client group's mutation acknowledgements from the
    /// replicated shard metadata table. This is intentionally read from the
    /// replica rather than inferred from `pushResponse`: application errors,
    /// retries, and pushes for inactive clients all have subtly different
    /// acknowledgement rules, while `<shard>.clients` already records the
    /// authoritative result produced by the mutate server.
    pub(crate) fn refresh_last_mutation_ids(&mut self) {
        let Ok(tables) = list_tables(&self.db) else {
            return;
        };
        for table in tables {
            if !table.name.ends_with(".clients")
                || !table
                    .columns
                    .iter()
                    .any(|(name, _)| name == "clientGroupID")
                || !table.columns.iter().any(|(name, _)| name == "clientID")
                || !table
                    .columns
                    .iter()
                    .any(|(name, _)| name == "lastMutationID")
            {
                continue;
            }
            let quoted_table = table.name.replace('"', "\"\"");
            let sql = format!(
                "SELECT \"clientID\", \"lastMutationID\" FROM \"{quoted_table}\" WHERE \"clientGroupID\" = ?"
            );
            let Ok(rows) = self.db.all(
                &sql,
                &[zero_cache_sqlite::Value::Text(self.client_group_id.clone())],
            ) else {
                continue;
            };
            for row in rows {
                let client_id = row.iter().find_map(|(name, value)| {
                    (name == "clientID")
                        .then_some(value)
                        .and_then(|value| match value {
                            zero_cache_sqlite::Value::Text(value) => Some(value.clone()),
                            _ => None,
                        })
                });
                let last_mutation_id = row.iter().find_map(|(name, value)| {
                    (name == "lastMutationID")
                        .then_some(value)
                        .and_then(|value| match value {
                            zero_cache_sqlite::Value::Integer(value) => Some(*value),
                            _ => None,
                        })
                });
                if let (Some(client_id), Some(last_mutation_id)) = (client_id, last_mutation_id) {
                    self.last_mutation_ids.insert(client_id, last_mutation_id);
                }
            }
        }
    }

    /// Hydrates one desired PUT op against the replica, returning the patches
    /// (query-state + row) it contributes. `transformed_ast` is the AST the
    /// CONNECTION resolved for this put (custom-query transform + per-connection
    /// read permissions); the core performs no auth work itself. Shared by the
    /// desired-patch application above and `DesiredQueriesHandler`'s
    /// `rehydrate_tracked` fallback.
    pub(crate) fn hydrate_put(
        &mut self,
        p: &UpQueriesPutOp,
        transformed_ast: Option<zero_cache_protocol::ast::Ast>,
        orig_version: &zero_cache_view_syncer::cvr_version::CvrVersion,
        force_wire_rows: bool,
        newly_desired: bool,
    ) -> Result<Vec<PatchToVersion>, String> {
        let mut patches = Vec::new();
        let started = std::time::Instant::now();
        {
            // Register the transformed query with the persistent client-group
            // pipeline. Bring its snapshot to head first so initial SQL
            // hydration and subsequent incremental advancement share the same
            // replica timeline.
            if let (Some(driver), Some(ast)) =
                (self.query_pipeline.as_mut(), transformed_ast.as_ref())
            {
                driver.advance_to_head().map_err(|error| {
                    format!(
                        "incremental pipeline advance while adding `{}` failed: {error}",
                        p.hash
                    )
                })?;
                // A desired-query put may replace a query with the same hash.
                // Replace its persistent pipeline atomically with the transformed
                // AST rather than retaining the stale pipeline or ignoring a
                // duplicate-registration error.
                driver.remove_query(&p.hash);
                // Direct-incremental queries are registered LATER via
                // `register_query`, reusing the rows the live hydration fetch
                // below already produced — avoiding a redundant second fetch +
                // extra snapshot connection. Complex queries still hydrate here
                // through `add_query`.
                if !driver.uses_prehydrated_rows(ast) {
                    driver
                        .add_query(p.hash.clone(), ast.clone())
                        .map_err(|error| {
                            format!(
                                "incremental pipeline registration for `{}` failed: {error}",
                                p.hash
                            )
                        })?;
                }
            }
            let ast_plan = transformed_ast
                .as_ref()
                .and_then(|ast| hydration_plan_from_ast(&self.db, ast).ok());
            let Some(plan) = ast_plan else {
                return Ok(patches);
            };
            // Whether this query was ALREADY executed for the group (upstream
            // executes a query once per client group; the port re-hydrates it per
            // connection). When it was, the group CVR already holds this query's
            // rows and ref-counts, so re-processing them yields only redundant
            // row-record writes — every row re-written with a bumped (additive,
            // upstream-faithful) ref-count. Those writes are the entire flag-on
            // hydration wall (measured: config_flush ~3s, deferred_rows=1000 per
            // connection, §9l). Since a row's ref-count is per-QUERY (kept while
            // ANY client desires the query, dropped only when the query leaves the
            // group — its VALUE is immaterial), dropping this connection's
            // redundant row writes is safe; the client still gets every row via
            // the `force_wire_rows` patches below.
            let already_executed = self.tracked.contains(&p.hash);
            let identity = identity_for_plan(&plan, &p.hash);
            let existing_key =
                |row: &RowRecord| row_key_string_from_row_id(&row.id, &plan.primary_key);
            // Build both the received-row index (every existing row of this
            // table) and the deletion set (rows this query ref-counts) in ONE
            // pass over the group's row_records, rather than two passes with the
            // same table filter + key computation.
            let mut existing_received: HashMap<String, ReceivedExistingRow> = HashMap::new();
            let mut existing_for_deletion: Vec<DeleteExistingRow<String>> = Vec::new();
            for row in self
                .row_records
                .iter()
                .filter(|row| row.id.schema == "public" && row.id.table == plan.table_name)
            {
                let Some(key) = existing_key(row) else {
                    continue;
                };
                if row
                    .ref_counts
                    .as_ref()
                    .is_some_and(|counts| counts.contains_key(&p.hash))
                {
                    existing_for_deletion.push(DeleteExistingRow {
                        id: key.clone(),
                        row_version: row.row_version.clone(),
                        patch_version: row.base.patch_version.clone(),
                        ref_counts: row.ref_counts.clone(),
                    });
                }
                existing_received.insert(
                    key,
                    ReceivedExistingRow {
                        row_version: row.row_version.clone(),
                        patch_version: row.base.patch_version.clone(),
                        ref_counts: row.ref_counts.clone(),
                    },
                );
            }
            // A client/transformed custom query's real `orderBy` becomes the
            // SQL `ORDER BY`; without one, fall back to primary-key order. The
            // primary key is always appended as a tiebreaker so the top-N under
            // `limit` is deterministic even when the query orders on a
            // non-unique column (matching how the upstream query builder
            // completes an `orderBy` with the primary key).
            let sort = sort_for_hydration(
                &plan,
                transformed_ast
                    .as_ref()
                    .and_then(|ast| ast.order_by.as_ref()),
            );

            // A client or already-transformed custom query's real `where_`
            // condition — pushed all the way into SQL via `fetch_filtered`,
            // not evaluated in memory.
            let where_ = transformed_ast.as_ref().and_then(|ast| ast.where_.as_ref());

            // The query's `limit`: hydrate only the top-N rows under `sort`.
            let limit = transformed_ast
                .as_ref()
                .and_then(|ast| ast.limit)
                .map(|n| n.max(0.0) as usize);

            // The query's `start` cursor bound: resume the SQL read at/after the
            // boundary row under `sort`, pushed into SQL by the fetch path.
            let start = transformed_ast.as_ref().and_then(|ast| ast.start.as_ref());

            let root_result = hydrate_patches_from_sqlite_with_row_updates(
                &self.db,
                plan.table_name.clone(),
                plan.primary_key.clone(),
                sort,
                plan.columns.clone(),
                &mut self.cvr_handler.cvr,
                orig_version,
                &mut self.tracked,
                &p.hash,
                &p.hash, // transformation hash: reuse the query hash for this slice (no real AST-hash compiler wired here).
                &identity,
                &existing_received,
                &existing_for_deletion,
                where_,
                limit,
                start,
            );
            match root_result {
                Ok(mut result) => {
                    // Feed the pipeline the rows this SINGLE hydration fetch
                    // already produced (direct-incremental case), instead of
                    // letting `add_query` open a second snapshot and re-fetch
                    // the same rows. Captured here, before any related-row
                    // extension below (direct queries have none), so only the
                    // root table's rows are handed to the pipeline. The rows are
                    // the typed ZQL bodies; `register_query` applies the
                    // identical `_0_version` clamp + keying the graph path uses.
                    if let (Some(driver), Some(ast)) =
                        (self.query_pipeline.as_mut(), transformed_ast.as_ref())
                    {
                        if driver.uses_prehydrated_rows(ast) {
                            let rows = result
                                .row_bodies
                                .iter()
                                .map(|(_, row)| row.clone())
                                .collect();
                            driver
                                .register_query(p.hash.clone(), ast.clone(), rows)
                                .map_err(|error| {
                                    format!(
                                        "incremental pipeline registration for `{}` failed: {error}",
                                        p.hash
                                    )
                                })?;
                        }
                    }
                    if let Some(ast) = transformed_ast.as_ref() {
                        // SQL pushdown uses correlated subqueries only to
                        // decide which root rows match. The Zero client runs
                        // the same pipeline over its local replica, so it also
                        // needs the matching child rows that made each
                        // `whereExists` true (including nodes below AND/OR).
                        if let Some(where_) = &ast.where_ {
                            let exists_related = correlated_subqueries_in_condition(where_);
                            if let Ok(related_result) = hydrate_related_rows_recursive(
                                &self.db,
                                &mut self.cvr_handler.cvr,
                                orig_version,
                                &p.hash,
                                &result.row_bodies,
                                &exists_related,
                            ) {
                                result.row_updates.extend(related_result.row_updates);
                                result.row_bodies.extend(related_result.row_bodies);
                                result.patches.extend(related_result.patches);
                            }
                        }
                        if let Some(related) = &ast.related {
                            if let Ok(related_result) = hydrate_related_rows_recursive(
                                &self.db,
                                &mut self.cvr_handler.cvr,
                                orig_version,
                                &p.hash,
                                &result.row_bodies,
                                related,
                            ) {
                                result.row_updates.extend(related_result.row_updates);
                                result.row_bodies.extend(related_result.row_bodies);
                                result.patches.extend(related_result.patches);
                            }
                        }
                    }
                    // CVR row records are shared by the whole client group,
                    // while each connected client has its own local replica
                    // snapshot. If another client already referenced an
                    // unchanged row, `received()` only updates ref-counts and
                    // legitimately produces no row patch. A client adding a
                    // new desired query still needs the row body, however; a
                    // got-query without these puts completes as an empty
                    // result in the official JS client.
                    if force_wire_rows {
                        // Index the current-version put patches ONCE by row key so
                        // the per-body "already patched?" check below is O(1). The
                        // previous `patches.iter().any(...)` inside the row_bodies
                        // loop was O(bodies × patches) — 1M RowId comparisons for a
                        // 1000-row hydration, on the 1-CPU bench's hot path.
                        let current_version = self.cvr_handler.cvr.version.clone();
                        let already_patched_keys: std::collections::HashSet<String> = result
                            .patches
                            .iter()
                            .filter_map(|patch| match &patch.patch {
                                zero_cache_view_syncer::client_patch::Patch::Row(
                                    zero_cache_view_syncer::client_patch::ClientRowPatch::Put(put),
                                ) if patch.to_version == current_version => {
                                    Some(row_id_key(&put.id))
                                }
                                _ => None,
                            })
                            .collect();
                        for (id, contents) in &result.row_bodies {
                            if !already_patched_keys.contains(&row_id_key(id)) {
                                result.patches.push(
                                    zero_cache_view_syncer::client_patch::PatchToVersion {
                                        patch: zero_cache_view_syncer::client_patch::Patch::Row(
                                            zero_cache_view_syncer::client_patch::ClientRowPatch::Put(
                                                zero_cache_view_syncer::client_patch::ClientPutRowPatch {
                                                    id: id.clone(),
                                                    contents: contents.clone(),
                                                },
                                            ),
                                        ),
                                        to_version: self.cvr_handler.cvr.version.clone(),
                                    },
                                );
                            }
                        }
                    }
                    // A query already executed for the group re-derives the same
                    // row records it already holds (differing only by a redundant
                    // ref-count bump, whose value is immaterial to GC). Dropping
                    // these writes turns the flag-on connect burst from 300x
                    // full-table row flushes into 30 (one per group's first
                    // hydration); the `force_wire_rows` patches above still carry
                    // every row into THIS connection's poke.
                    if already_executed {
                        // Keep any genuine deletions (a row that stopped matching);
                        // drop only the redundant re-puts of rows the group holds.
                        result.row_updates.retain(|(_, record)| record.is_none());
                    }
                    let hydrated_rows = result.row_bodies.len();
                    self.apply_row_updates(result.row_updates);
                    self.apply_row_bodies(result.row_bodies);
                    patches.extend(result.patches);
                    // Slow-query observability (ZERO_LOG_SLOW_HYDRATE_THRESHOLD /
                    // ZERO_LOG_SLOW_ROW_THRESHOLD).
                    crate::logging::maybe_log_slow_hydrate(
                        &p.hash,
                        started.elapsed().as_millis() as u64,
                        hydrated_rows,
                    );
                }
                Err(error) => {
                    return Err(format!("query hydration for `{}` failed: {error}", p.hash));
                }
            }
        }
        // The "gotten" state is GROUP-scoped: when another client in the group
        // already hydrated this query, `track_executed` sees an unchanged
        // transformation hash and emits no fresh got patch — but THIS client may
        // never have been told the query is gotten, and without the patch it
        // stays "loading" forever. Replay the got patch at its recorded version
        // (upstream's catchup-config-patches equivalent); the per-client
        // base-cookie filter in `build_poke_outcome` drops it for clients that
        // already received it.
        let has_got_put = patches.iter().any(|patch| {
            matches!(
                &patch.patch,
                zero_cache_view_syncer::client_patch::Patch::Config(config)
                    if config.op == zero_cache_view_syncer::cvr_types::PatchOp::Put
                        && config.client_id.is_none()
                        && config.id == p.hash
            )
        });
        if !has_got_put {
            use zero_cache_view_syncer::cvr_types::QueryRecord;
            let got_version = match self.cvr_handler.cvr.queries.get(&p.hash) {
                Some(QueryRecord::Client(query)) => query.base.patch_version.clone(),
                Some(QueryRecord::Custom(query)) => query.base.patch_version.clone(),
                _ => None,
            };
            if let Some(got_version) = got_version {
                // A client NEWLY desiring the query must receive the patch even
                // though its cookie already chained past the recorded got
                // version (the config poke advanced it) — stamp it at the
                // current version, exactly as `force_wire_rows` does for the
                // row bodies. A re-put of an already-desired query keeps the
                // recorded version, so the base-cookie filter only lets it
                // through for a client genuinely behind it (reconnect catchup).
                let to_version = if newly_desired {
                    self.cvr_handler.cvr.version.clone()
                } else {
                    got_version
                };
                patches.push(zero_cache_view_syncer::client_patch::PatchToVersion {
                    patch: zero_cache_view_syncer::client_patch::Patch::Config(
                        zero_cache_view_syncer::cvr_types::QueryPatch {
                            op: zero_cache_view_syncer::cvr_types::PatchOp::Put,
                            id: p.hash.clone(),
                            client_id: None,
                        },
                    ),
                    to_version,
                });
            }
        }
        Ok(patches)
    }

    /// Applies a BATCH of row-record updates in O(n + m) rather than O(n·m).
    /// The previous per-element form did a full `Vec::retain` for every update;
    /// hydrating a 1000-row query (1000 single-element calls) was O(n²) and, on
    /// the 1-CPU bench container, the dominant hydrate cost. Here we collect the
    /// affected ids into a set, do ONE retain, then push the survivors.
    pub(crate) fn apply_row_updates(&mut self, updates: Vec<(RowId, Option<RowRecord>)>) {
        if updates.is_empty() {
            return;
        }
        self.pending_row_updates.extend(updates.iter().cloned());
        // Bodies are dropped for tombstones / ref-count-less records.
        let body_drop: std::collections::HashSet<String> = updates
            .iter()
            .filter(|(_, record)| {
                record
                    .as_ref()
                    .is_none_or(|record| record.ref_counts.is_none())
            })
            .map(|(id, _)| row_id_key(id))
            .collect();
        let update_keys: std::collections::HashSet<String> =
            updates.iter().map(|(id, _)| row_id_key(id)).collect();
        if !body_drop.is_empty() {
            std::sync::Arc::make_mut(&mut self.row_bodies)
                .retain(|(existing, _)| !body_drop.contains(&row_id_key(existing)));
        }
        std::sync::Arc::make_mut(&mut self.row_records)
            .retain(|existing| !update_keys.contains(&row_id_key(&existing.id)));
        // Keep only the last record per id (later updates supersede earlier).
        let mut seen = std::collections::HashSet::new();
        for (id, record) in updates.into_iter().rev() {
            if !seen.insert(row_id_key(&id)) {
                continue;
            }
            if let Some(record) = record {
                std::sync::Arc::make_mut(&mut self.row_records).push(record);
            }
        }
    }

    pub(crate) fn apply_row_bodies(
        &mut self,
        updates: Vec<(RowId, zero_cache_protocol::row_patch::Row)>,
    ) {
        if updates.is_empty() {
            return;
        }
        let update_keys: std::collections::HashSet<String> =
            updates.iter().map(|(id, _)| row_id_key(id)).collect();
        std::sync::Arc::make_mut(&mut self.row_bodies)
            .retain(|(existing, _)| !update_keys.contains(&row_id_key(existing)));
        // `row_bodies` is a by-id lookup store, so element order is irrelevant;
        // walking the batch in reverse keeps the last write per id.
        let mut seen = std::collections::HashSet::new();
        for (id, row) in updates.into_iter().rev() {
            if seen.insert(row_id_key(&id)) {
                std::sync::Arc::make_mut(&mut self.row_bodies).push((id, row));
            }
        }
    }
}
