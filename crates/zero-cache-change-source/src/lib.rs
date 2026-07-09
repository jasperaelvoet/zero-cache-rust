//! Change-source protocol and ingestion for zero-cache, ported from
//! `zero-cache/src/services/change-source`. Incremental — see `PORTING.md`.

pub mod control;
pub mod data;
pub mod downstream;
pub mod pg_connection;
pub mod pg_schema_diff;
pub mod pg_to_change;
pub mod pgoutput;
pub mod published_schema;
pub mod replication_conn;
pub mod report_schema;
pub mod shard_schema;
pub mod status;
pub mod upstream;
