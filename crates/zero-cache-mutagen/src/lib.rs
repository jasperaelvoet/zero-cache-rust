//! Client CRUD mutation ingestion for zero-cache, ported from
//! `packages/zero-cache/src/services/mutagen`. Incremental — see
//! `PORTING.md`.

pub mod api_fetch;
pub mod api_request;
pub mod apply_mutation;
pub mod crud_ops;
pub mod crud_ops_json;
pub mod last_mutation_id;
pub mod orchestration;
pub mod pusher_batch;
pub mod pusher_response;
pub mod pusher_service;
pub mod sliding_window_limiter;
pub mod sql;
