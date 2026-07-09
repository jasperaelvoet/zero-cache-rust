//! Port of `zero-protocol/src/mutations-patch.ts`.
//!
//! Mutation results are stored ephemerally in the client, hence only a
//! `put` (resolve/reject the mutation promise and release the reference)
//! and `del` (drop the ephemeral entry) operation exist — no `update`.

use crate::mutation_id::MutationId;
use crate::mutation_result::MutationResponse;

/// Port of `mutationsPatchSchema`'s `put` operation.
#[derive(Debug, Clone, PartialEq)]
pub struct MutationPutOp {
    pub mutation: MutationResponse,
}

/// Port of `mutationsPatchSchema`'s `del` operation.
#[derive(Debug, Clone, PartialEq)]
pub struct MutationDelOp {
    pub id: MutationId,
}

/// Port of `MutationPatch` (one element of `mutationsPatchSchema`).
#[derive(Debug, Clone, PartialEq)]
pub enum MutationPatchOp {
    Put(MutationPutOp),
    Del(MutationDelOp),
}

/// Port of `mutationsPatchSchema` (`v.array(patchOpSchema)`).
pub type MutationsPatch = Vec<MutationPatchOp>;
