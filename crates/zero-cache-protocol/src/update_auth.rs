//! Port of `zero-protocol/src/update-auth.ts`.

/// Port of `UpdateAuthBody`.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateAuthBody {
    pub auth: String,
}
