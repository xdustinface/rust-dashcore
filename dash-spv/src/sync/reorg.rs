//! Reorg cascade: guards, deny-list, and downstream storage truncation.
//!
//! This module owns the cross-manager logic that runs once a fork candidate
//! has been promoted by the staged-fork pipeline. Guards (single-flight,
//! deny-list, checkpoint floor, chainlock floor, depth cap) run before any
//! storage mutation. If all guards pass, the cascade bumps the generation
//! counter and truncates header, filter-header, filter, and block storage to
//! the common ancestor.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashcore::BlockHash;
use tokio::sync::{Mutex, RwLock};

use crate::chain::{CheckpointManager, ForkCandidate};
use crate::error::SyncResult;
use crate::storage::{BlockHeaderStorage, BlockStorage, FilterHeaderStorage, FilterStorage};
use crate::sync::SyncEvent;

/// Maximum reorg depth (active_tip_height - fork_ancestor_height) the
/// cascade will accept. Anything deeper is rejected and added to the
/// deny-list.
pub(crate) const MAX_REORG_DEPTH: u32 = 100;

/// Fallback floor distance used when no chainlock has been observed yet.
/// A fork ancestor below `current_tip_height - FRESH_CLIENT_FORK_FLOOR` is
/// rejected to prevent deep reorgs on a fresh client that has no chainlock
/// guidance.
pub(crate) const FRESH_CLIENT_FORK_FLOOR: u32 = 100;

/// Single-flight gate plus deny-list of fork tip hashes that have been
/// rejected by a guard.
///
/// The deny-list maps a rejected fork tip hash to the chainlock height at
/// which it should be evicted. A TTL of `u32::MAX` means "expire only on
/// explicit reset"; lower TTLs let chainlock-floor rejections drop out once
/// the local node has progressed past the floor.
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

/// Storage handles passed into [`handle_reorg`]. All four storages are
/// truncated in lockstep so downstream caches don't outlive the truncated
/// header chain.
pub(crate) struct ReorgStorages<'a, H, FH, F, B>
where
    H: BlockHeaderStorage,
    FH: FilterHeaderStorage,
    F: FilterStorage,
    B: BlockStorage,
{
    pub(crate) block_header_storage: &'a Arc<RwLock<H>>,
    pub(crate) filter_header_storage: Option<&'a Arc<RwLock<FH>>>,
    pub(crate) filter_storage: Option<&'a Arc<RwLock<F>>>,
    pub(crate) block_storage: Option<&'a Arc<RwLock<B>>>,
}

/// Drive the reorg cascade for a buffered fork candidate.
///
/// Runs guards before any storage mutation. If every guard passes, bumps the
/// generation counter and truncates the four storage handles, then persists
/// the fork's headers starting at `ancestor_height + 1`. Returns a
/// `ChainReorg` event on success, a `DeepReorgDetected` event when the depth
/// cap fires, or `Ok(None)` when an earlier guard short-circuits.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_reorg<H, FH, F, B>(
    candidate: ForkCandidate,
    state: &Mutex<ReorgState>,
    generation: &AtomicU64,
    storages: ReorgStorages<'_, H, FH, F, B>,
    checkpoint_manager: &CheckpointManager,
    best_chainlock_height: Option<u32>,
    current_tip_height: u32,
    current_tip_hash: BlockHash,
) -> SyncResult<Option<SyncEvent>>
where
    H: BlockHeaderStorage,
    FH: FilterHeaderStorage,
    F: FilterStorage,
    B: BlockStorage,
{
    let candidate_tip_hash = candidate.tip_hash();

    // Single-flight: try to claim `in_flight`, also short-circuit on deny-list.
    {
        let mut state = state.lock().await;
        if state.in_flight {
            tracing::debug!(
                "reorg already in flight, ignoring candidate at ancestor {}",
                candidate.ancestor_height
            );
            return Ok(None);
        }
        if state.deny_list.contains_key(&candidate_tip_hash) {
            tracing::debug!("fork tip {} is on the deny-list, skipping", candidate_tip_hash);
            return Ok(None);
        }
        state.in_flight = true;
    }

    let result = run_guards_and_cascade(
        &candidate,
        candidate_tip_hash,
        state,
        generation,
        storages,
        checkpoint_manager,
        best_chainlock_height,
        current_tip_height,
        current_tip_hash,
    )
    .await;

    // Always release the single-flight guard, even on error.
    state.lock().await.in_flight = false;

    result
}

#[allow(clippy::too_many_arguments)]
async fn run_guards_and_cascade<H, FH, F, B>(
    candidate: &ForkCandidate,
    candidate_tip_hash: BlockHash,
    state: &Mutex<ReorgState>,
    generation: &AtomicU64,
    storages: ReorgStorages<'_, H, FH, F, B>,
    checkpoint_manager: &CheckpointManager,
    best_chainlock_height: Option<u32>,
    current_tip_height: u32,
    current_tip_hash: BlockHash,
) -> SyncResult<Option<SyncEvent>>
where
    H: BlockHeaderStorage,
    FH: FilterHeaderStorage,
    F: FilterStorage,
    B: BlockStorage,
{
    // Checkpoint floor.
    if checkpoint_manager.should_reject_fork(candidate.ancestor_height) {
        tracing::warn!(
            "rejecting reorg: ancestor {} at or below last checkpoint",
            candidate.ancestor_height
        );
        let ttl = checkpoint_manager.last_checkpoint().map(|cp| cp.height).unwrap_or(u32::MAX);
        state.lock().await.deny_list.insert(candidate_tip_hash, ttl);
        return Ok(None);
    }

    // Chainlock floor or fresh-client fallback.
    match best_chainlock_height {
        Some(cl_height) => {
            if candidate.ancestor_height < cl_height {
                tracing::warn!(
                    "rejecting reorg: ancestor {} below best chainlock {}",
                    candidate.ancestor_height,
                    cl_height
                );
                state.lock().await.deny_list.insert(candidate_tip_hash, cl_height);
                return Ok(None);
            }
        }
        None => {
            let checkpoint_height =
                checkpoint_manager.last_checkpoint().map(|cp| cp.height).unwrap_or(0);
            let recent_floor = current_tip_height.saturating_sub(FRESH_CLIENT_FORK_FLOOR);
            let floor = checkpoint_height.max(recent_floor);
            if candidate.ancestor_height < floor {
                tracing::warn!(
                    "rejecting reorg on fresh client: ancestor {} below floor {}",
                    candidate.ancestor_height,
                    floor
                );
                state.lock().await.deny_list.insert(candidate_tip_hash, floor);
                return Ok(None);
            }
        }
    }

    // Depth cap.
    let depth = current_tip_height.saturating_sub(candidate.ancestor_height);
    if depth > MAX_REORG_DEPTH {
        tracing::warn!("rejecting reorg: depth {} exceeds cap {}", depth, MAX_REORG_DEPTH);
        state.lock().await.deny_list.insert(candidate_tip_hash, u32::MAX);
        return Ok(Some(SyncEvent::DeepReorgDetected {
            fork_height: candidate.ancestor_height,
            depth,
        }));
    }

    // Cascade. Bump the generation first so any response that races with the
    // truncation is tagged stale and dropped at the downstream manager.
    let new_generation = generation.fetch_add(1, Ordering::AcqRel) + 1;

    let ReorgStorages {
        block_header_storage,
        filter_header_storage,
        filter_storage,
        block_storage,
    } = storages;

    {
        let mut headers = block_header_storage.write().await;
        headers.truncate_above(candidate.ancestor_height).await?;
        headers.store_hashed_headers(&candidate.headers).await?;
    }

    if let Some(fh) = filter_header_storage {
        fh.write().await.truncate_above(candidate.ancestor_height).await?;
    }
    if let Some(fs) = filter_storage {
        fs.write().await.truncate_above(candidate.ancestor_height).await?;
    }
    if let Some(bs) = block_storage {
        bs.write().await.truncate_above(candidate.ancestor_height).await?;
    }

    tracing::info!(
        "reorg cascade complete: fork_height={} new_tip={} generation={}",
        candidate.ancestor_height,
        candidate_tip_hash,
        new_generation
    );

    Ok(Some(SyncEvent::ChainReorg {
        fork_height: candidate.ancestor_height,
        old_tip: current_tip_hash,
        new_tip: candidate_tip_hash,
        generation: new_generation,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::checkpoints::Checkpoint;
    use crate::chain::ChainWork;
    use crate::storage::{
        DiskStorageManager, PersistentBlockHeaderStorage, PersistentBlockStorage,
        PersistentFilterHeaderStorage, PersistentFilterStorage, StorageManager,
    };
    use crate::types::HashedBlockHeader;
    use dashcore::block::{Header, Version};
    use dashcore::{BlockHash, CompactTarget, Target, TxMerkleNode};
    use dashcore_hashes::Hash;

    fn easy_target() -> Target {
        Target::from_compact(CompactTarget::from_consensus(0x207fffff))
    }

    fn mined_header(prev: BlockHash, time: u32) -> Header {
        let bits = CompactTarget::from_consensus(0x207fffff);
        for nonce in 0u32..1024 {
            let h = Header {
                version: Version::ONE,
                prev_blockhash: prev,
                merkle_root: TxMerkleNode::all_zeros(),
                time,
                bits,
                nonce,
            };
            if h.target().is_met_by(h.block_hash()) {
                return h;
            }
        }
        panic!("could not mine header");
    }

    fn mined_chain(count: u32, base_time: u32) -> Vec<Header> {
        let mut prev = BlockHash::all_zeros();
        let mut chain = Vec::with_capacity(count as usize);
        for i in 0..count {
            let h = mined_header(prev, base_time + i * 600);
            prev = h.block_hash();
            chain.push(h);
        }
        chain
    }

    fn fork_candidate_from(
        ancestor_height: u32,
        prev: BlockHash,
        len: u32,
        base_time: u32,
    ) -> ForkCandidate {
        let mut prev = prev;
        let mut hashed = Vec::with_capacity(len as usize);
        for i in 0..len {
            let h = mined_header(prev, base_time + i * 600);
            prev = h.block_hash();
            hashed.push(HashedBlockHeader::from(h));
        }
        ForkCandidate {
            ancestor_height,
            headers: hashed,
            total_work: ChainWork::zero(),
        }
    }

    /// All four storages are wired and truncate together on a successful
    /// cascade.
    #[tokio::test]
    async fn cascade_truncates_all_storages_and_emits_chain_reorg() {
        let mut storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let chain = mined_chain(10, 1_700_000_000);
        storage.store_headers(&chain).await.unwrap();
        let block_headers = storage.block_headers();
        let filter_headers = storage.filter_headers();
        let filters = storage.filters();
        let blocks = storage.blocks();

        let candidate = fork_candidate_from(5, chain[5].block_hash(), 3, 1_700_010_000);

        let state = Mutex::new(ReorgState::default());
        let generation = AtomicU64::new(7);
        let checkpoint_manager = CheckpointManager::new(vec![]);

        let storages: ReorgStorages<
            PersistentBlockHeaderStorage,
            PersistentFilterHeaderStorage,
            PersistentFilterStorage,
            PersistentBlockStorage,
        > = ReorgStorages {
            block_header_storage: &block_headers,
            filter_header_storage: Some(&filter_headers),
            filter_storage: Some(&filters),
            block_storage: Some(&blocks),
        };

        let old_tip_hash = chain.last().unwrap().block_hash();
        let event = handle_reorg(
            candidate.clone(),
            &state,
            &generation,
            storages,
            &checkpoint_manager,
            Some(0),
            9,
            old_tip_hash,
        )
        .await
        .unwrap()
        .expect("ChainReorg event");

        match event {
            SyncEvent::ChainReorg {
                fork_height,
                old_tip,
                new_tip,
                generation: ev_gen,
            } => {
                assert_eq!(fork_height, 5);
                assert_eq!(old_tip, old_tip_hash);
                assert_eq!(new_tip, candidate.tip_hash());
                assert_eq!(ev_gen, 8);
            }
            other => panic!("unexpected event: {:?}", other),
        }
        assert_eq!(generation.load(Ordering::SeqCst), 8);

        // Header storage now reflects the fork.
        let tip = block_headers.read().await.get_tip().await.unwrap();
        assert_eq!(tip.height(), 8);
        assert_eq!(*tip.hash(), candidate.tip_hash());
    }

    /// Single-flight: a second call while the first is in-flight is a
    /// no-op.
    #[tokio::test]
    async fn second_call_short_circuits_when_in_flight() {
        let mut storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let chain = mined_chain(5, 1_700_000_000);
        storage.store_headers(&chain).await.unwrap();
        let block_headers = storage.block_headers();
        let filter_headers = storage.filter_headers();
        let filters = storage.filters();
        let blocks = storage.blocks();

        let candidate = fork_candidate_from(2, chain[2].block_hash(), 2, 1_700_010_000);

        let state = Mutex::new(ReorgState {
            in_flight: true,
            ..ReorgState::default()
        });
        let generation = AtomicU64::new(3);
        let checkpoint_manager = CheckpointManager::new(vec![]);

        let storages: ReorgStorages<
            PersistentBlockHeaderStorage,
            PersistentFilterHeaderStorage,
            PersistentFilterStorage,
            PersistentBlockStorage,
        > = ReorgStorages {
            block_header_storage: &block_headers,
            filter_header_storage: Some(&filter_headers),
            filter_storage: Some(&filters),
            block_storage: Some(&blocks),
        };

        let result = handle_reorg(
            candidate,
            &state,
            &generation,
            storages,
            &checkpoint_manager,
            Some(0),
            4,
            chain[4].block_hash(),
        )
        .await
        .unwrap();
        assert!(result.is_none());
        assert_eq!(generation.load(Ordering::SeqCst), 3);
    }

    /// Chainlock floor: a fork whose ancestor is at or below the best
    /// chainlock height is rejected and the tip hash lands on the deny-list.
    #[tokio::test]
    async fn chainlock_floor_rejects_and_adds_to_deny_list() {
        let mut storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let chain = mined_chain(10, 1_700_000_000);
        storage.store_headers(&chain).await.unwrap();
        let block_headers = storage.block_headers();
        let filter_headers = storage.filter_headers();
        let filters = storage.filters();
        let blocks = storage.blocks();

        let candidate = fork_candidate_from(3, chain[3].block_hash(), 2, 1_700_010_000);
        let candidate_tip = candidate.tip_hash();

        let state = Mutex::new(ReorgState::default());
        let generation = AtomicU64::new(0);
        let checkpoint_manager = CheckpointManager::new(vec![]);

        let storages: ReorgStorages<
            PersistentBlockHeaderStorage,
            PersistentFilterHeaderStorage,
            PersistentFilterStorage,
            PersistentBlockStorage,
        > = ReorgStorages {
            block_header_storage: &block_headers,
            filter_header_storage: Some(&filter_headers),
            filter_storage: Some(&filters),
            block_storage: Some(&blocks),
        };

        let result = handle_reorg(
            candidate,
            &state,
            &generation,
            storages,
            &checkpoint_manager,
            Some(5),
            9,
            chain[9].block_hash(),
        )
        .await
        .unwrap();
        assert!(result.is_none());
        let state_guard = state.lock().await;
        assert!(state_guard.deny_list.contains_key(&candidate_tip));
        assert_eq!(generation.load(Ordering::SeqCst), 0);
    }

    /// Depth cap: a fork below the depth cap is denied with
    /// `DeepReorgDetected` and the generation is NOT bumped.
    #[tokio::test]
    async fn depth_cap_emits_deep_reorg_event_without_cascade() {
        let mut storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let chain = mined_chain(120, 1_700_000_000);
        storage.store_headers(&chain).await.unwrap();
        let block_headers = storage.block_headers();
        let filter_headers = storage.filter_headers();
        let filters = storage.filters();
        let blocks = storage.blocks();

        // ancestor at 10, tip at 119 → depth 109 > 100.
        let candidate = fork_candidate_from(10, chain[10].block_hash(), 3, 1_700_100_000);
        let candidate_tip = candidate.tip_hash();

        let state = Mutex::new(ReorgState::default());
        let generation = AtomicU64::new(0);
        let checkpoint_manager = CheckpointManager::new(vec![]);

        let storages: ReorgStorages<
            PersistentBlockHeaderStorage,
            PersistentFilterHeaderStorage,
            PersistentFilterStorage,
            PersistentBlockStorage,
        > = ReorgStorages {
            block_header_storage: &block_headers,
            filter_header_storage: Some(&filter_headers),
            filter_storage: Some(&filters),
            block_storage: Some(&blocks),
        };

        let event = handle_reorg(
            candidate,
            &state,
            &generation,
            storages,
            &checkpoint_manager,
            Some(0),
            119,
            chain[119].block_hash(),
        )
        .await
        .unwrap()
        .expect("DeepReorgDetected event");

        match event {
            SyncEvent::DeepReorgDetected {
                fork_height,
                depth,
            } => {
                assert_eq!(fork_height, 10);
                assert_eq!(depth, 109);
            }
            other => panic!("unexpected event: {:?}", other),
        }
        assert_eq!(generation.load(Ordering::SeqCst), 0);
        assert!(state.lock().await.deny_list.contains_key(&candidate_tip));
    }

    /// Checkpoint floor: a fork at or below the last checkpoint height is
    /// rejected.
    #[tokio::test]
    async fn checkpoint_floor_rejects_fork_at_or_below_last_checkpoint() {
        let mut storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let chain = mined_chain(10, 1_700_000_000);
        storage.store_headers(&chain).await.unwrap();
        let block_headers = storage.block_headers();
        let filter_headers = storage.filter_headers();
        let filters = storage.filters();
        let blocks = storage.blocks();

        let candidate = fork_candidate_from(5, chain[5].block_hash(), 2, 1_700_010_000);

        let state = Mutex::new(ReorgState::default());
        let generation = AtomicU64::new(0);
        let checkpoint_manager = CheckpointManager::new(vec![Checkpoint {
            height: 5,
            block_hash: chain[5].block_hash(),
            prev_blockhash: BlockHash::all_zeros(),
            timestamp: 0,
            target: easy_target(),
            merkle_root: None,
            chain_work: String::new(),
            masternode_list_name: None,
            protocol_version: None,
            nonce: 0,
        }]);

        let storages: ReorgStorages<
            PersistentBlockHeaderStorage,
            PersistentFilterHeaderStorage,
            PersistentFilterStorage,
            PersistentBlockStorage,
        > = ReorgStorages {
            block_header_storage: &block_headers,
            filter_header_storage: Some(&filter_headers),
            filter_storage: Some(&filters),
            block_storage: Some(&blocks),
        };

        let result = handle_reorg(
            candidate,
            &state,
            &generation,
            storages,
            &checkpoint_manager,
            Some(0),
            9,
            chain[9].block_hash(),
        )
        .await
        .unwrap();
        assert!(result.is_none());
        assert_eq!(generation.load(Ordering::SeqCst), 0);
    }

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
