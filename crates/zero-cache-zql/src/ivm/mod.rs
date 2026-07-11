//! Port of `zql/src/ivm` — incremental view maintenance. Incremental; see
//! `PORTING.md` and this module's children for scope notes on what's ported
//! so far vs. deferred (the `Node`/`Stream`/`Operator` generator-driven
//! pipeline machinery).

pub mod change;
pub mod constraint;
pub mod data;
pub mod exists;
pub mod filter;
pub mod join;
pub mod memory_storage;
pub mod operator;
pub mod table_source;
