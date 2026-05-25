//! Chain tip primitives used by the staged-fork pipeline.

use super::ChainWork;
use crate::types::HashedBlockHeader;
use dashcore::{BlockHash, Header as BlockHeader};

/// Represents a chain tip with its metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct ChainTip {
    /// The block hash of this tip.
    pub hash: BlockHash,
    /// The height of this tip.
    pub height: u32,
    /// The header at this tip.
    pub header: BlockHeader,
    /// Cumulative chain work up to this tip.
    pub chain_work: ChainWork,
    /// Whether this is currently the active (best) chain.
    pub is_active: bool,
}

impl ChainTip {
    /// Create a new chain tip.
    pub fn new(header: BlockHeader, height: u32, chain_work: ChainWork) -> Self {
        Self {
            hash: header.block_hash(),
            height,
            header,
            chain_work,
            is_active: false,
        }
    }
}

/// A buffered fork branch that has been validated against the active chain.
///
/// Carries the common-ancestor height in the active chain, the validated
/// headers that extend past that ancestor, and the resulting cumulative work
/// at the fork tip. Phase 3 promotes a candidate once its `total_work`
/// strictly exceeds the active chain's work.
#[derive(Debug, Clone)]
pub(crate) struct ForkCandidate {
    pub(crate) ancestor_height: u32,
    pub(crate) headers: Vec<HashedBlockHeader>,
    pub(crate) total_work: ChainWork,
}
