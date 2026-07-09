//! Port of `zero-protocol/src/close-connection.ts`.
//!
//! Deprecated upstream; kept only for wire compatibility with older
//! clients. The body is unused (`v.array(v.unknown())`).

/// Port of `CloseConnectionBody` — an unused array, modeled as an opaque
/// count of elements rather than a real value list since nothing reads its
/// contents (`unknown()` upstream too).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CloseConnectionBody;
