//! Foundational value types for zero-cache.
//!
//! This crate is the Rust port of `zero-cache/src/types`. Each module here
//! corresponds to a file in that directory; modules are added incrementally as
//! the port progresses. See `PORTING.md` at the workspace root for the mapping
//! and progress tracker.

pub mod column_metadata;
pub mod error_with_level;
pub mod initial_sync_sql;
pub mod lexi_version;
pub mod lite;
pub mod lsn;
pub mod names;
pub mod pg;
pub mod pg_copy_binary;
pub mod pg_data_type;
pub mod pg_to_lite;
pub mod pg_types;
pub mod published_schema_json;
pub mod row_key;
pub mod shards;
pub mod specs;
pub mod sql;
pub mod state_version;
pub mod strings;
pub mod subscription;
pub mod timeout;
pub mod url_params;
pub mod warmup;

pub use lexi_version::{
    max, min, version_from_lexi, version_to_lexi, version_to_lexi_big, LexiError, LexiVersion,
    Version, MAX_SAFE_INTEGER,
};
pub use row_key::{
    normalized_key_order, row_id_hash, row_id_string, row_key_string, RowId, RowKey, RowKeyError,
};
pub use state_version::{
    major_version_from_string, major_version_to_string, state_version_from_string,
    state_version_to_string, StateVersion,
};
