//! Shared primitives used across zero-cache, ported from `packages/shared`.
//!
//! Only the parts that zero-cache depends on are ported, incrementally. See
//! `PORTING.md` at the workspace root.

pub mod arrays;
pub mod bigint_json;
pub mod binary_search;
pub mod centroid;
pub mod deep_merge;
pub mod error_details;
pub mod event_publish;
pub mod float_to_ordered_string;
pub mod hash;
pub mod logarithmic_histogram;
pub mod parse_big_int;
pub mod queue;
pub mod ref_count;
pub mod tdigest;
pub mod timed_cache;

pub use bigint_json::{parse, stringify, JsonValue};
pub use hash::{h128, h32, h64, xx_hash32};
pub use parse_big_int::parse_big_int;
