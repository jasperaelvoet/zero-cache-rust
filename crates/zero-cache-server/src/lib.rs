//! WebSocket sync transport for zero-cache, ported from `src/server` +
//! `src/workers/syncer.ts`. Incremental — see `PORTING.md`.

pub mod analyze_query;
pub mod auth_token;
pub mod bootstrap;
pub mod change_streamer_server;
pub mod change_streamer_wire;
pub mod client_handler;
pub mod commit_dispatch;
pub mod config;
pub mod custom_mutation;
pub mod cvr_pool;
pub mod cvr_provision;
pub mod cvr_row_flush_barrier;
pub(crate) mod group_processor;
pub(crate) mod group_transition;
pub mod http_dispatch;
pub mod inspect_handler;
pub mod inspector_delegate;
pub mod litestream;
pub mod live_connection;
pub mod live_hydration;
pub mod logging;
pub mod otlp_exporter;
pub mod public_http;
pub mod query_pipeline;
pub mod replicator_service;
pub mod serve_connection;
pub mod service_lifecycle;
pub mod sync_server;
pub mod sync_service;
pub mod ws_close;
pub mod ws_connection;

pub mod view_syncer_client;
