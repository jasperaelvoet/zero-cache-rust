//! Port of `zero-protocol/src/delete-clients.ts`.

/// Port of `DeleteClientsBody`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteClientsBody {
    pub client_ids: Option<Vec<String>>,
    pub client_group_ids: Option<Vec<String>>,
}
