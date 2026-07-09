//! Port of `zero-protocol/src/mutation-id.ts`.

/// Identifies one client mutation: the client's own monotonic mutation
/// counter plus which client it came from. Port of `MutationID`.
#[derive(Debug, Clone, PartialEq)]
pub struct MutationId {
    pub id: f64,
    pub client_id: String,
}
