//! Reorg cascade: guards, deny-list, and downstream storage truncation.
//!
//! This module owns the cross-manager logic that runs once a fork candidate
//! has been promoted by the staged-fork pipeline. Guards (single-flight,
//! deny-list, checkpoint floor, chainlock floor, depth cap) run before any
//! storage mutation. If all guards pass, the cascade bumps the generation
//! counter and truncates header, filter-header, filter, and block storage to
//! the common ancestor.

use std::collections::HashMap;

use dashcore::BlockHash;

/// Single-flight gate plus deny-list of fork tip hashes that have been
/// rejected by a guard.
///
/// The deny-list maps a rejected fork tip hash to the chainlock height at
/// which it should be evicted. `0` means "expire only on explicit reset";
/// non-zero TTLs let chainlock-floor rejections drop out once the local node
/// has progressed past the floor.
#[derive(Debug, Default)]
pub(crate) struct ReorgState {
    pub(super) deny_list: HashMap<BlockHash, u32>,
    pub(super) in_flight: bool,
}

impl ReorgState {
    /// Drop deny-list entries whose TTL is at or below the current best
    /// chainlock height. A wider chainlock floor means previously denied
    /// branches are no longer reachable from the active chain, so the
    /// deny-list entry can be released.
    pub(crate) fn evict_expired_denials(&mut self, best_chainlock_height: u32) {
        self.deny_list.retain(|_, ttl| *ttl > best_chainlock_height);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashcore_hashes::Hash;

    #[test]
    fn evict_expired_denials_drops_entries_at_or_below_height() {
        let mut state = ReorgState::default();
        state.deny_list.insert(BlockHash::from_byte_array([1u8; 32]), 100);
        state.deny_list.insert(BlockHash::from_byte_array([2u8; 32]), 200);
        state.deny_list.insert(BlockHash::from_byte_array([3u8; 32]), 300);

        state.evict_expired_denials(200);

        assert!(!state.deny_list.contains_key(&BlockHash::from_byte_array([1u8; 32])));
        assert!(!state.deny_list.contains_key(&BlockHash::from_byte_array([2u8; 32])));
        assert!(state.deny_list.contains_key(&BlockHash::from_byte_array([3u8; 32])));
    }
}
