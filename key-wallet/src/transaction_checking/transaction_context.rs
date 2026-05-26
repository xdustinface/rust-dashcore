//! Block metadata and transaction context types.

use dashcore::ephemerealdata::instant_lock::InstantLock;
use dashcore::prelude::CoreBlockHeight;
use dashcore::BlockHash;

/// Block metadata attached to confirmed transactions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct BlockInfo {
    pub(crate) height: CoreBlockHeight,
    pub(crate) block_hash: BlockHash,
    pub(crate) timestamp: u32,
}

impl BlockInfo {
    pub fn new(height: CoreBlockHeight, block_hash: BlockHash, timestamp: u32) -> Self {
        Self {
            height,
            block_hash,
            timestamp,
        }
    }

    pub fn height(&self) -> CoreBlockHeight {
        self.height
    }

    pub fn block_hash(&self) -> BlockHash {
        self.block_hash
    }

    pub fn timestamp(&self) -> u32 {
        self.timestamp
    }
}

/// Context for transaction processing
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TransactionContext {
    /// Transaction is in the mempool (unconfirmed)
    Mempool,
    /// Transaction is in the mempool with an InstantSend lock
    InstantSend(InstantLock),
    /// Transaction is in a block at the given height
    InBlock(BlockInfo),
    /// Transaction is in a chain-locked block at the given height
    InChainLockedBlock(BlockInfo),
    /// Transaction was reorganized out and is now superseded by a
    /// double-spending transaction on the active chain. `previous`
    /// remembers the last confirmed-or-mempool context so the UI can
    /// surface what state the tx was in before the conflict.
    ///
    /// Invariant: `previous` must be an active context (`Mempool`,
    /// `InstantSend`, `InBlock`, `InChainLockedBlock`) — never another
    /// `Conflicted` or `Abandoned`. The type does not enforce this, so
    /// constructors must uphold it.
    Conflicted {
        previous: Box<TransactionContext>,
    },
    /// Transaction was reorganized out and is not expected to confirm
    /// again (e.g. its inputs have been spent elsewhere and the user
    /// has chosen to drop it). Terminal state.
    Abandoned,
}

impl std::fmt::Display for TransactionContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionContext::Mempool => write!(f, "mempool"),
            TransactionContext::InstantSend(_) => write!(f, "instant send"),
            TransactionContext::InBlock(info) => write!(f, "block {}", info.height),
            TransactionContext::InChainLockedBlock(info) => {
                write!(f, "chainlocked block {}", info.height)
            }
            TransactionContext::Conflicted {
                previous,
            } => write!(f, "conflicted (was {})", previous),
            TransactionContext::Abandoned => write!(f, "abandoned"),
        }
    }
}

impl TransactionContext {
    /// Returns the confirmation state.
    pub(crate) fn confirmed(&self) -> bool {
        matches!(self, TransactionContext::InChainLockedBlock(_) | TransactionContext::InBlock(_))
    }

    /// Returns whether this context is an InstantSend lock.
    pub(crate) fn is_instant_send(&self) -> bool {
        matches!(self, TransactionContext::InstantSend(_))
    }

    /// Returns whether the transaction has been mined in a block that is
    /// itself chainlocked, the strongest finality signal we have, and
    /// the only one we treat as truly "finalized".
    ///
    /// `InBlock` alone is not enough (the block can still be reorganized
    /// out), and `InstantSend` alone is not enough either (the
    /// surrounding block confirmation may still arrive and write the
    /// height / block hash before the chainlock catches up). Only
    /// `InChainLockedBlock` qualifies.
    pub fn is_chain_locked(&self) -> bool {
        matches!(self, TransactionContext::InChainLockedBlock(_))
    }

    /// Returns the block info if confirmed.
    pub fn block_info(&self) -> Option<&BlockInfo> {
        match self {
            TransactionContext::Mempool
            | TransactionContext::InstantSend(_)
            | TransactionContext::Conflicted {
                ..
            }
            | TransactionContext::Abandoned => None,
            TransactionContext::InBlock(info) | TransactionContext::InChainLockedBlock(info) => {
                Some(info)
            }
        }
    }

    /// Returns whether this record has been reorganized out and no
    /// longer contributes to the spendable balance. True for both
    /// [`TransactionContext::Conflicted`] and
    /// [`TransactionContext::Abandoned`].
    pub(crate) fn is_inactive(&self) -> bool {
        matches!(self, TransactionContext::Conflicted { .. } | TransactionContext::Abandoned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashcore::ephemerealdata::instant_lock::InstantLock;
    use dashcore::hashes::Hash;
    use dashcore::BlockHash;

    fn sample_block_info() -> BlockInfo {
        BlockInfo::new(100, BlockHash::all_zeros(), 1_700_000_000)
    }

    #[test]
    fn conflicted_helpers_are_negative() {
        let previous = TransactionContext::InstantSend(InstantLock::default());
        let conflicted = TransactionContext::Conflicted {
            previous: Box::new(previous),
        };
        assert!(!conflicted.confirmed());
        assert!(!conflicted.is_instant_send());
        assert!(!conflicted.is_chain_locked());
        assert!(conflicted.is_inactive());
        assert!(conflicted.block_info().is_none());
        assert_eq!(format!("{}", conflicted), "conflicted (was instant send)");
    }

    #[test]
    fn abandoned_helpers_are_negative() {
        let abandoned = TransactionContext::Abandoned;
        assert!(!abandoned.confirmed());
        assert!(!abandoned.is_instant_send());
        assert!(!abandoned.is_chain_locked());
        assert!(abandoned.is_inactive());
        assert!(abandoned.block_info().is_none());
        assert_eq!(format!("{}", abandoned), "abandoned");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_round_trip_conflicted() {
        let original = TransactionContext::Conflicted {
            previous: Box::new(TransactionContext::InBlock(sample_block_info())),
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let restored: TransactionContext = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, restored);
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_round_trip_abandoned() {
        let original = TransactionContext::Abandoned;
        let json = serde_json::to_string(&original).expect("serialize");
        let restored: TransactionContext = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, restored);
    }

    #[test]
    fn conflicted_preserves_previous_in_block_context() {
        let previous = TransactionContext::InBlock(sample_block_info());
        let conflicted = TransactionContext::Conflicted {
            previous: Box::new(previous.clone()),
        };
        let TransactionContext::Conflicted {
            previous: restored,
        } = conflicted
        else {
            panic!("expected Conflicted variant");
        };
        assert_eq!(*restored, previous);
    }
}
