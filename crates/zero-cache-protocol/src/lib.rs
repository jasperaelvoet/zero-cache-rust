//! Protocol types for zero-cache, ported from `packages/zero-protocol`.
//!
//! Incremental: only the pieces zero-cache exercises are ported so far. See
//! `PORTING.md` at the workspace root.

pub mod analyze_query_result;
pub mod application_error;
pub mod ast;
pub mod ast_json;
pub mod change_desired_queries;
pub mod client_schema;
pub mod close_connection;
pub mod complete_ordering;
pub mod connect;
pub mod custom_queries;
pub mod delete_clients;
pub mod error;
pub mod error_kind;
pub mod error_origin;
pub mod error_reason;
pub mod inspect_down;
pub mod inspect_down_json;
pub mod inspect_up;
pub mod mutation_id;
pub mod mutation_result;
pub mod mutations_patch;
pub mod name_mapper;
pub mod ping;
pub mod poke;
pub mod poke_json;
pub mod pong;
pub mod protocol_version;
pub mod pull;
pub mod pull_json;
pub mod push;
pub mod push_json;
pub mod queries_patch;
pub mod query_hash;
pub mod query_server;
pub mod query_server_json;
pub mod row_patch;
pub mod up;
pub mod up_json;
pub mod update_auth;
pub mod version;

pub use error::{ErrorBody, ProtocolError};
pub use error_kind::ErrorKind;
pub use error_origin::ErrorOrigin;
