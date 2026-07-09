//! Port of `zero-protocol/src/version.ts`.
//!
//! The client-cookie "version" string used throughout downstream sync
//! messages (`poke.ts`'s `baseCookie`/`cookie`).

/// A cookie version string. Port of `Version`.
pub type Version = String;

/// A `Version`, or `null` for a client's very first connection (before it
/// has received any poke). Port of `NullableVersion`.
pub type NullableVersion = Option<Version>;
