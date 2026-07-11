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
//! a query once per group and removes it when the last client drops it); a
//! shared query never needs pre-fetched rows, so `uses_prehydrated_rows` is
//! `false`, routing hydration through `add_query`/`desire` and skipping the
//! per-connection `register_query` fast path.

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
    /// Advances the pipeline to the replica head, returning the row changes.
    pub fn advance(&mut self) -> Result<Vec<PipelineRowChange>, PipelineError> {
        match self {
            QueryPipeline::Owned(driver) => driver.advance(),
            QueryPipeline::Shared { service, .. } => service.pipeline.advance(),
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
    /// and so could accept caller-pre-fetched rows. Always `false` on the shared
    /// path — the shared driver hydrates the query itself, once per group.
    pub fn uses_prehydrated_rows(&self, ast: &Ast) -> bool {
        match self {
            QueryPipeline::Owned(driver) => driver.uses_prehydrated_rows(ast),
            QueryPipeline::Shared { .. } => false,
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

    /// Registers a query with rows the caller already fetched (direct-
    /// incremental fast path). Only reached on the owned path, since the shared
    /// path reports `uses_prehydrated_rows == false`; the shared arm falls back
    /// to `desire` (hydrating on the shared driver) for safety.
    pub fn register_query(
        &mut self,
        query_id: String,
        ast: Ast,
        rows: Vec<Row>,
    ) -> Result<Vec<PipelineRowChange>, PipelineError> {
        match self {
            QueryPipeline::Owned(driver) => driver.register_query(query_id, ast, rows),
            QueryPipeline::Shared { service, client_id } => {
                service.pipeline.desire(client_id, &query_id, ast)
            }
        }
    }
}
