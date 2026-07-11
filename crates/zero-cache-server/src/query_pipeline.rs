//! The query-pipeline vehicle a [`crate::live_connection::DesiredQueriesHandler`]
//! drives — either a per-connection [`PipelineDriver`] it owns (today's default
//! path), or the client group's shared service (`SharedGroupPipeline` inside a
//! `GroupService`) resolved from the client-group registry (redesign §6, behind
//! `ZERO_GROUP_OWNERSHIP`).
//!
//! The two variants expose the SAME synchronous method surface the handler
//! already calls, so wiring group ownership is a field-type swap, not a rewrite
//! of the handler's query lifecycle. On the shared path, `add_query`/
//! `remove_query` become the ref-counted `desire`/`undesire` (upstream hydrates
//! a query once per group and removes it when the last client drops it), and
//! `register_query` becomes `register_desire`: the FIRST desirer seeds the
//! shared driver from the caller's single hydration fetch (preserving the
//! direct-incremental single-fetch fast path), a later desirer from the
//! already-active query's rows.

use std::sync::Arc;

use zero_cache_protocol::ast::Ast;
use zero_cache_view_syncer::group_registry::GroupService;
use zero_cache_view_syncer::pipeline_driver::{PipelineDriver, PipelineError, PipelineRowChange};
use zero_cache_zql::ivm::data::Row;

/// Owned-per-connection or shared-per-group query pipeline. The `Owned` arm is
/// byte-for-byte today's behavior; the `Shared` arm ref-counts queries across a
/// group's connections.
pub enum QueryPipeline {
    /// One `PipelineDriver` per WebSocket connection (default / flag-off path).
    /// Boxed to keep the enum small (the shared arm is just an `Arc` + `String`).
    Owned(Box<PipelineDriver>),
    /// The client group's shared service, keyed by this connection's
    /// `client_id` for query ref-counting. Holding the `Arc<GroupService>` keeps
    /// the group alive while this connection is open — the registry reaps the
    /// service when the last connection's `Arc` drops.
    Shared {
        service: Arc<GroupService>,
        client_id: String,
    },
}

impl QueryPipeline {
    /// Advances the pipeline to the replica head, returning the row changes this
    /// connection must apply. On the shared path this is the fan-out
    /// `poll_advance`: the first connection to process a commit advances the
    /// shared driver, and every connection reads that commit's changes exactly
    /// once from its own cursor.
    pub fn advance(&mut self) -> Result<Vec<PipelineRowChange>, PipelineError> {
        match self {
            QueryPipeline::Owned(driver) => driver.advance(),
            QueryPipeline::Shared { service, client_id } => {
                service.pipeline.poll_advance(client_id)
            }
        }
    }

    /// Brings the pipeline's snapshot to the replica head WITHOUT returning
    /// changes to this connection — used before an initial query fetch so
    /// hydration reads a current snapshot. Matches the historical behavior of
    /// discarding the sync-advance's changes for the advancing connection; on
    /// the shared path the changes are still delivered to the group's OTHER
    /// connections.
    pub fn advance_to_head(&mut self) -> Result<(), PipelineError> {
        match self {
            QueryPipeline::Owned(driver) => driver.advance().map(|_| ()),
            QueryPipeline::Shared { service, client_id } => {
                service.pipeline.advance_to_head(client_id)
            }
        }
    }

    /// Removes a query. On the shared path this drops this client's desire and
    /// removes the query from the driver only if it was the last desirer.
    pub fn remove_query(&mut self, query_id: &str) -> Vec<PipelineRowChange> {
        match self {
            QueryPipeline::Owned(driver) => driver.remove_query(query_id),
            QueryPipeline::Shared { service, client_id } => {
                service.pipeline.undesire(client_id, query_id)
            }
        }
    }

    /// Whether the pipeline would hydrate `ast` via the direct-incremental graph
    /// and so could accept caller-pre-fetched rows. The shared path forwards to
    /// the shared driver: its `register_desire` seeds the FIRST desirer from the
    /// caller's single fetch (no second hydration), and a later desirer from the
    /// already-active query's rows.
    pub fn uses_prehydrated_rows(&self, ast: &Ast) -> bool {
        match self {
            QueryPipeline::Owned(driver) => driver.uses_prehydrated_rows(ast),
            QueryPipeline::Shared { service, .. } => service.pipeline.uses_prehydrated_rows(ast),
        }
    }

    /// Adds (hydrates) a query. On the shared path this records this client's
    /// desire and hydrates the query on the shared driver only for the first
    /// desirer; a later desirer is seeded from the driver's existing rows.
    pub fn add_query(
        &mut self,
        query_id: String,
        ast: Ast,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        match self {
            QueryPipeline::Owned(driver) => driver.add_query(query_id, ast),
            QueryPipeline::Shared { service, client_id } => {
                service.pipeline.desire(client_id, &query_id, ast)
            }
        }
    }

    /// Advances the pipeline to head from the SINGLE-OWNER perspective (the
    /// per-group processor loop): on the shared path this is
    /// `SharedGroupPipeline::advance()` — the loop is the group's only advancer,
    /// so the per-connection `AdvanceLog`/`poll_advance` fan-out cursors are
    /// bypassed. On the owned path it is a plain driver advance.
    pub fn advance_single_owner(&mut self) -> Result<Vec<PipelineRowChange>, PipelineError> {
        match self {
            QueryPipeline::Owned(driver) => driver.advance(),
            QueryPipeline::Shared { service, .. } => service.pipeline.advance(),
        }
    }

    /// Repoints the shared pipeline's ref-count key at another client in the
    /// group (loop only). Owned pipelines have no per-client key and ignore it.
    pub fn set_client_id(&mut self, id: &str) {
        if let QueryPipeline::Shared { client_id, .. } = self {
            *client_id = id.to_string();
        }
    }

    /// Resets a shared query so it re-hydrates from scratch (the loop's
    /// transformation-hash guard). A no-op on the owned path.
    pub fn reset_query(&mut self, query_id: &str) {
        if let QueryPipeline::Shared { service, .. } = self {
            let _ = service.pipeline.reset_query(query_id);
        }
    }

    /// Drops every query `client_id` solely desired from the shared pipeline on
    /// disconnect (loop only). A no-op on the owned path (per-connection drivers
    /// die with the connection).
    pub fn remove_group_client(&self, client_id: &str) {
        if let QueryPipeline::Shared { service, .. } = self {
            let _ = service.pipeline.remove_client(client_id);
        }
    }

    /// Registers a query with rows the caller already fetched (direct-
    /// incremental single-fetch fast path). On the shared path the first
    /// desirer registers the pre-fetched rows on the shared driver; a later
    /// desirer is seeded from the active query instead.
    pub fn register_query(
        &mut self,
        query_id: String,
        ast: Ast,
        rows: Vec<Row>,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        match self {
            QueryPipeline::Owned(driver) => driver.register_query(query_id, ast, rows),
            QueryPipeline::Shared { service, client_id } => service
                .pipeline
                .register_desire(client_id, &query_id, ast, rows),
        }
    }
}
