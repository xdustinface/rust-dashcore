//! Checkpoint system for chain validation and sync optimization
//!
//! Checkpoints are hardcoded blocks at specific heights that help:
//! - Prevent accepting blocks from invalid chains
//! - Optimize initial sync by starting from recent checkpoints
//! - Protect against deep reorganizations
//! - Bootstrap masternode lists at specific heights

use dashcore::BlockHash;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

/// A checkpoint representing a known valid block
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Block height
    pub height: u32,
    /// Block hash
    pub block_hash: BlockHash,
    /// Block timestamp
    pub timestamp: u32,
}
/// Checkpoint override settings
#[derive(Debug, Clone, Default)]
pub struct CheckpointOverride {
    /// Override checkpoint height for sync chain
    pub sync_override_height: Option<u32>,
    /// Override checkpoint height for terminal chain
    pub terminal_override_height: Option<u32>,
    /// Whether to sync from genesis
    pub sync_from_genesis: bool,
}

/// Manages checkpoints for a specific network
pub struct CheckpointManager {
    /// Checkpoints indexed by height
    checkpoints: HashMap<u32, Checkpoint>,
    /// Sorted list of checkpoint heights for efficient searching
    sorted_heights: Vec<u32>,
    /// Checkpoint override settings (not persisted)
    override_settings: CheckpointOverride,
}

impl CheckpointManager {
    /// Create a new checkpoint manager from a list of checkpoints
    pub fn new(checkpoints: Vec<Checkpoint>) -> Self {
        let mut checkpoint_map = HashMap::new();
        let mut heights = Vec::new();

        for checkpoint in checkpoints {
            heights.push(checkpoint.height);
            checkpoint_map.insert(checkpoint.height, checkpoint);
        }

        heights.sort_unstable();

        Self {
            checkpoints: checkpoint_map,
            sorted_heights: heights,
            override_settings: CheckpointOverride::default(),
        }
    }

    /// Get a checkpoint at a specific height
    pub fn get_checkpoint(&self, height: u32) -> Option<&Checkpoint> {
        self.checkpoints.get(&height)
    }

    /// Check if a block hash matches the checkpoint at the given height
    pub fn validate_block(&self, height: u32, block_hash: &BlockHash) -> bool {
        match self.checkpoints.get(&height) {
            Some(checkpoint) => checkpoint.block_hash == *block_hash,
            None => true, // No checkpoint at this height, so it's valid
        }
    }

    /// Get the last checkpoint at or before the given height
    pub fn last_checkpoint_before_height(&self, height: u32) -> Option<&Checkpoint> {
        // Binary search for the highest checkpoint <= height
        let pos = self.sorted_heights.partition_point(|&h| h <= height);
        if pos > 0 {
            let checkpoint_height = self.sorted_heights[pos - 1];
            self.checkpoints.get(&checkpoint_height)
        } else {
            None
        }
    }

    /// Get the last checkpoint
    pub fn last_checkpoint(&self) -> Option<&Checkpoint> {
        self.sorted_heights.last().and_then(|&height| self.checkpoints.get(&height))
    }

    /// Get all checkpoint heights
    pub fn checkpoint_heights(&self) -> &[u32] {
        &self.sorted_heights
    }

    /// Check if we're past the last checkpoint
    pub fn is_past_last_checkpoint(&self, height: u32) -> bool {
        self.sorted_heights.last().is_none_or(|&last| height > last)
    }

    /// Get the last checkpoint before a given timestamp
    pub fn last_checkpoint_before_timestamp(&self, timestamp: u32) -> Option<&Checkpoint> {
        let mut best_checkpoint = None;
        let mut best_height = 0;

        for checkpoint in self.checkpoints.values() {
            if checkpoint.timestamp <= timestamp && checkpoint.height >= best_height {
                best_height = checkpoint.height;
                best_checkpoint = Some(checkpoint);
            }
        }

        best_checkpoint
    }

    /// Find the best checkpoint at or before a given height
    pub fn best_checkpoint_at_or_before_height(&self, height: u32) -> Option<Checkpoint> {
        let mut best_checkpoint = None;
        let mut best_height = 0;

        for checkpoint in self.checkpoints.values() {
            if checkpoint.height <= height && checkpoint.height >= best_height {
                best_height = checkpoint.height;
                best_checkpoint = Some(checkpoint.clone());
            }
        }

        best_checkpoint
    }

    /// Set override checkpoint for sync chain
    pub fn set_sync_override(&mut self, height: Option<u32>) {
        self.override_settings.sync_override_height = height;
    }

    /// Set override checkpoint for terminal chain
    pub fn set_terminal_override(&mut self, height: Option<u32>) {
        self.override_settings.terminal_override_height = height;
    }

    /// Set whether to sync from genesis
    pub fn set_sync_from_genesis(&mut self, from_genesis: bool) {
        self.override_settings.sync_from_genesis = from_genesis;
    }

    /// Get the checkpoint to use for sync chain based on override settings
    pub fn get_sync_checkpoint(&self, wallet_creation_time: Option<u32>) -> Option<&Checkpoint> {
        if self.override_settings.sync_from_genesis {
            return self.get_checkpoint(0);
        }

        if let Some(override_height) = self.override_settings.sync_override_height {
            return self.last_checkpoint_before_height(override_height);
        }

        // Default to checkpoint based on wallet creation time
        if let Some(creation_time) = wallet_creation_time {
            self.last_checkpoint_before_timestamp(creation_time)
        } else {
            self.last_checkpoint()
        }
    }

    /// Get the checkpoint to use for terminal chain based on override settings
    pub fn get_terminal_checkpoint(&self) -> Option<&Checkpoint> {
        if let Some(override_height) = self.override_settings.terminal_override_height {
            self.last_checkpoint_before_height(override_height)
        } else {
            self.last_checkpoint()
        }
    }

    /// Check if a fork at the given height should be rejected due to checkpoint
    pub fn should_reject_fork(&self, fork_height: u32) -> bool {
        if let Some(last_checkpoint) = self.last_checkpoint() {
            fork_height <= last_checkpoint.height
        } else {
            false
        }
    }
}

macro_rules! checkpoint {
    ($height:expr, $hash:literal, $timestamp:expr) => {
        Checkpoint {
            height: $height,
            block_hash: BlockHash::from_str($hash).unwrap(),
            timestamp: $timestamp,
        }
    };
}

/// Create mainnet checkpoints
pub fn mainnet_checkpoints() -> Vec<Checkpoint> {
    vec![
        // Genesis block
        checkpoint!(
            0,
            "00000ffd590b1485b3caadc19b22e6379c733355108f107a430458cdf3407ab6",
            1390095618
        ),
        // Early network checkpoint (1 week after genesis)
        checkpoint!(
            4991,
            "000000003b01809551952460744d5dbb8fcbd6cbae3c220267bf7fa43f837367",
            1390163520
        ),
        // 3 months checkpoint
        checkpoint!(
            107996,
            "00000000000a23840ac16115407488267aa3da2b9bc843e301185b7d17e4dc40",
            1395522898
        ),
        // 2017 checkpoint
        checkpoint!(
            750000,
            "00000000000000b4181bbbdddbae464ce11fede5d0292fb63fdede1e7c8ab21c",
            1491953700
        ),
        // 2022 checkpoint
        checkpoint!(
            1700000,
            "000000000000001d7579a371e782fd9c4480f626a62b916fa4eb97e16a49043a",
            1657142113
        ),
        // 2022/2023 checkpoint
        checkpoint!(
            1900000,
            "000000000000001b8187c744355da78857cca5b9aeb665c39d12f26a0e3a9af5",
            1688744911
        ),
        // 2025 checkpoint
        checkpoint!(
            2300000,
            "00000000000000186f9f2fde843be3d66b8ae317cabb7d43dbde943d02a4b4d7",
            1751767455
        ),
    ]
}

/// Create testnet checkpoints
pub fn testnet_checkpoints() -> Vec<Checkpoint> {
    vec![
        // Genesis block
        checkpoint!(
            0,
            "00000bafbc94add76cb75e2ec92894837288a481e5c005f6563d91623bf8bc2c",
            1390666206
        ),
        // Height 500000
        checkpoint!(
            500000,
            "000000d0f2239d3ea3d1e39e624f651c5a349b5ca729eec29540aeae0ecc94a7",
            1621049765
        ),
        // Height 800000
        checkpoint!(
            800000,
            "00000075cdfa0a552e488406074bb95d831aee16c0ec30114319a587a8a8fb0c",
            1671238603
        ),
        // Height 1100000
        checkpoint!(
            1100000,
            "000000078cc3952c7f594de921ae82fcf430a5f3b86755cd72acd819d0001015",
            1725934127
        ),
    ]
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use dashcore_hashes::Hash;
    use super::*;

    #[test]
    fn test_checkpoint_validation() {
        let checkpoints = mainnet_checkpoints();
        let manager = CheckpointManager::new(checkpoints);

        // Test genesis block
        let genesis_checkpoint =
            manager.get_checkpoint(0).expect("Genesis checkpoint should exist");
        assert_eq!(genesis_checkpoint.height, 0);
        assert_eq!(genesis_checkpoint.timestamp, 1390095618);

        // Test validation
        let genesis_hash =
            BlockHash::from_str("00000ffd590b1485b3caadc19b22e6379c733355108f107a430458cdf3407ab6")
                .expect("Failed to parse genesis hash for test");
        assert!(manager.validate_block(0, &genesis_hash));

        // Test invalid hash
        let invalid_hash = BlockHash::from_byte_array([1u8; 32]);
        assert!(!manager.validate_block(0, &invalid_hash));

        // Test no checkpoint at height
        assert!(manager.validate_block(1, &invalid_hash)); // No checkpoint at height 1
    }

    #[test]
    fn test_last_checkpoint_before() {
        let checkpoints = mainnet_checkpoints();
        let manager = CheckpointManager::new(checkpoints);

        // Test finding checkpoint before various heights
        assert_eq!(
            manager.last_checkpoint_before_height(0).expect("Should find checkpoint").height,
            0
        );
        assert_eq!(
            manager.last_checkpoint_before_height(1000).expect("Should find checkpoint").height,
            0
        );
        assert_eq!(
            manager.last_checkpoint_before_height(5000).expect("Should find checkpoint").height,
            4991
        );
        assert_eq!(
            manager.last_checkpoint_before_height(200000).expect("Should find checkpoint").height,
            107996
        );
    }

    #[test]
    fn test_checkpoint_overrides() {
        let checkpoints = mainnet_checkpoints();
        let mut manager = CheckpointManager::new(checkpoints);

        // Test sync override
        manager.set_sync_override(Some(5000));
        let sync_checkpoint = manager.get_sync_checkpoint(None);
        assert_eq!(sync_checkpoint.expect("Should have sync checkpoint").height, 4991);

        // Test terminal override
        manager.set_terminal_override(Some(800000));
        let terminal_checkpoint = manager.get_terminal_checkpoint();
        assert_eq!(terminal_checkpoint.expect("Should have terminal checkpoint").height, 750000);

        // Test sync from genesis
        manager.set_sync_from_genesis(true);
        let genesis_checkpoint = manager.get_sync_checkpoint(None);
        assert_eq!(genesis_checkpoint.expect("Should have genesis checkpoint").height, 0);
    }

    #[test]
    #[ignore] // Test depends on specific mainnet checkpoint data
    fn test_fork_rejection() {
        let checkpoints = mainnet_checkpoints();
        let manager = CheckpointManager::new(checkpoints);

        // Should reject fork at checkpoint height
        assert!(manager.should_reject_fork(1500));
        assert!(manager.should_reject_fork(750000));

        // Should not reject fork after last checkpoint
        assert!(!manager.should_reject_fork(2000000));
    }

    #[test]
    fn test_checkpoint_by_timestamp() {
        let checkpoints = mainnet_checkpoints();
        let manager = CheckpointManager::new(checkpoints);

        // Test finding checkpoint by timestamp
        let checkpoint = manager.last_checkpoint_before_timestamp(1500000000);
        assert!(checkpoint.is_some());
        assert!(checkpoint.expect("Should find checkpoint by timestamp").timestamp <= 1500000000);
    }
}
