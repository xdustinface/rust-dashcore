//! Blocks manager for parallel sync.
//!
//! Downloads blocks that matched wallet filters and processes them in height order.
//! Subscribes to BlockNeeded events and emits BlockProcessed events.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::RwLock;

use super::pipeline::BlocksPipeline;
use crate::error::SyncResult;
use crate::network::RequestSender;
use crate::storage::{BlockHeaderStorage, BlockStorage};
use crate::sync::{BlocksProgress, SyncEvent, SyncManager, SyncState};
use dashcore::BlockHash;
use key_wallet_manager::{BackfillAdvance, WalletInterface};

/// Blocks manager for downloading and processing matching blocks.
///
/// This manager:
/// - Subscribes to BlockNeeded events from FiltersManager
/// - Downloads blocks using pipelined requests
/// - Processes blocks through wallet in height order
/// - Emits BlockProcessed events
///
/// Generic over:
/// - `H: BlockHeaderStorage` for height lookups
/// - `B: BlockStorage` for storing and loading blocks
/// - `W: WalletInterface` for wallet operations
pub struct BlocksManager<H: BlockHeaderStorage, B: BlockStorage, W: WalletInterface> {
    /// Current progress of the manager.
    pub(super) progress: BlocksProgress,
    /// Block header storage (for height lookups).
    pub(super) header_storage: Arc<RwLock<H>>,
    /// Block storage (for storing and loading blocks).
    pub(super) block_storage: Arc<RwLock<B>>,
    /// Wallet for processing blocks.
    pub(super) wallet: Arc<RwLock<W>>,
    /// Pipeline for downloading blocks (handles buffering and height ordering).
    pub(super) pipeline: BlocksPipeline,
    /// Whether FiltersSyncComplete has been received.
    pub(super) filters_sync_complete: bool,
    /// Per-block backfill obligations carried alongside the pipeline. A
    /// block whose hash is keyed here is processed via
    /// `process_backfill_block_for_wallets`, which emits
    /// `WalletEvent::RescanBlockProcessed` and calls `advance_rescan`.
    pub(super) backfill_advances: HashMap<BlockHash, Vec<BackfillAdvance>>,
}

impl<H: BlockHeaderStorage, B: BlockStorage, W: WalletInterface> BlocksManager<H, B, W> {
    /// Create a new blocks manager with the given storage references.
    pub async fn new(
        wallet: Arc<RwLock<W>>,
        header_storage: Arc<RwLock<H>>,
        block_storage: Arc<RwLock<B>>,
    ) -> Self {
        let last_processed_height = wallet.read().await.last_processed_height();

        let mut initial_progress = BlocksProgress::default();
        initial_progress.update_last_processed(last_processed_height);

        Self {
            progress: initial_progress,
            header_storage,
            block_storage,
            wallet,
            pipeline: BlocksPipeline::new(),
            filters_sync_complete: false,
            backfill_advances: HashMap::new(),
        }
    }

    /// Drop entries from `backfill_advances` whose block hash is no longer
    /// claimed by any live pending advance in the worker. Without this
    /// cleanup, a block that was queued via `BackfillBlocksNeeded` but
    /// then cancelled (peer disconnect, range completed elsewhere, reorg
    /// invalidation) would sit in the map forever, leaking memory and
    /// shadowing future forward-sync routing for the same hash.
    ///
    /// `live_hashes` is the set of block hashes the
    /// [`super::super::filters::backfill::BackfillWorker`] still considers
    /// in-flight; everything else is stale.
    pub(super) fn prune_stale_backfill_advances(&mut self, live_hashes: &HashSet<BlockHash>) {
        if self.backfill_advances.is_empty() {
            return;
        }
        self.backfill_advances.retain(|hash, _| live_hashes.contains(hash));
    }

    pub(super) async fn send_pending(&mut self, requests: &RequestSender) -> SyncResult<()> {
        let sent = self.pipeline.send_pending(requests).await?;
        if sent > 0 {
            self.progress.add_requested(sent as u32);
        }
        Ok(())
    }

    /// Process buffered blocks in height order.
    ///
    /// Uses the pipeline's height-ordering logic to ensure blocks are processed
    /// in the correct sequence. A block hash recorded in `backfill_advances`
    /// is processed via the backfill path (one `RescanBlockProcessed` event
    /// per advance, atomic with `advance_rescan`). When the same block hash
    /// is also in the pipeline's `interested` set (forward-sync wallets that
    /// matched the block too), the forward-sync wallets are processed
    /// alongside via `process_block_for_wallets` so neither side is silently
    /// skipped.
    pub(super) async fn process_buffered_blocks(&mut self) -> SyncResult<Vec<SyncEvent>> {
        use key_wallet_manager::WalletId;
        use std::collections::BTreeSet;

        let mut events = Vec::new();

        // Process blocks in height order using pipeline's ordering logic
        while let Some((block, height, interested)) = self.pipeline.take_next_ordered_block() {
            let hash = block.block_hash();

            let backfill_advances = self.backfill_advances.remove(&hash);

            // Forward-sync wallets that this block was queued for, minus
            // any wallet already covered by a backfill advance. The two
            // sets normally don't overlap, but if they do (a wallet
            // catching up via forward sync also has a pending sync range
            // matched at this height) the backfill path takes precedence
            // because it carries the per-range `advance_to` obligation.
            let backfill_wallets: BTreeSet<WalletId> = backfill_advances
                .as_ref()
                .map(|advs| advs.iter().map(|a| a.wallet_id).collect())
                .unwrap_or_default();
            let forward_only: BTreeSet<WalletId> =
                interested.difference(&backfill_wallets).cloned().collect();
            let has_forward = !forward_only.is_empty();

            let mut result = if has_forward {
                let mut wallet = self.wallet.write().await;
                let r = wallet.process_block_for_wallets(&block, height, &forward_only).await;
                drop(wallet);
                r
            } else {
                key_wallet_manager::BlockProcessingResult::default()
            };

            if let Some(advances) = backfill_advances.as_ref() {
                let mut wallet = self.wallet.write().await;
                let bf = wallet.process_backfill_block_for_wallets(&block, height, advances).await;
                drop(wallet);
                // Merge backfill stats into the forward-sync result so the
                // logging and progress counters below reflect the full
                // block. `BlockProcessingResult` exposes additive
                // accessors via the existing fields; merge by extending.
                result.new_txids.extend(bf.new_txids);
                result.existing_txids.extend(bf.existing_txids);
                for (wid, addrs) in bf.new_addresses {
                    result.new_addresses.entry(wid).or_default().extend(addrs);
                }
            }

            let total_relevant = result.relevant_tx_count();
            let new_addresses_total: usize = result.new_addresses.values().map(|v| v.len()).sum();
            if total_relevant > 0 {
                tracing::info!(
                    "Found {} relevant transactions ({} new, {} existing) {} at height {}, new addresses: {}",
                    total_relevant,
                    result.new_txids.len(),
                    result.existing_txids.len(),
                    hash,
                    height,
                    new_addresses_total
                );
            }

            // Collect confirmed txids before moving new_addresses out of result
            let confirmed_txids: Vec<_> = result.relevant_txids().cloned().collect();

            // Collect new addresses for gap limit rescanning
            let new_addresses = result.new_addresses;
            if new_addresses_total > 0 {
                tracing::debug!(
                    "Block {} generated {} new addresses for gap limit maintenance across {} wallets",
                    height,
                    new_addresses_total,
                    new_addresses.len()
                );
            }

            self.progress.add_processed(1);
            if total_relevant > 0 {
                self.progress.add_relevant(1);
            }
            // Only count new transactions to avoid double-counting during rescans
            self.progress.add_transactions(result.new_txids.len() as u32);
            // Backfill-only blocks live below the forward edge and must not
            // bump `last_processed_height` backwards. A mixed block (some
            // forward-sync wallets, some backfill wallets) DOES advance the
            // forward edge for the forward-sync wallets, so the bump is
            // gated on the forward path having actually run.
            if has_forward {
                self.progress.update_last_processed(height);
            }

            // Forward sync emits BlockProcessed for downstream consumers
            // (FiltersManager tracks in-flight, etc.). Backfill blocks emit
            // BlockProcessed too so FiltersManager's BlockMatchTracker can
            // clear its in-flight state — they're distinguishable by
            // FiltersManager via the worker's pending_advances.
            events.push(SyncEvent::BlockProcessed {
                block_hash: hash,
                height,
                wallets: interested,
                new_addresses,
                confirmed_txids,
            });
        }

        // Check if pipeline is empty
        if self.pipeline.is_complete() && self.state() == SyncState::Syncing {
            if self.filters_sync_complete {
                // Filters are done and pipeline is empty - we're fully synced
                self.progress.set_state(SyncState::Synced);
                tracing::info!(
                    "Block sync complete, processed {} blocks",
                    self.progress.processed()
                );
            } else {
                // Pipeline empty but filters still syncing - wait for more blocks
                self.progress.set_state(SyncState::WaitForEvents);
            }
        }

        Ok(events)
    }
}

impl<H: BlockHeaderStorage, B: BlockStorage, W: WalletInterface> std::fmt::Debug
    for BlocksManager<H, B, W>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlocksManager")
            .field("progress", &self.progress)
            .field("pipeline", &self.pipeline)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{MessageType, NetworkManager};
    use crate::storage::{
        DiskStorageManager, PersistentBlockHeaderStorage, PersistentBlockStorage, StorageManager,
    };
    use crate::sync::{ManagerIdentifier, SyncEvent, SyncManagerProgress};
    use crate::test_utils::MockNetworkManager;
    use key_wallet_manager::test_utils::{MockWallet, MOCK_WALLET_ID};
    use key_wallet_manager::FilterMatchKey;
    use std::collections::{BTreeMap, BTreeSet};

    type TestBlocksManager =
        BlocksManager<PersistentBlockHeaderStorage, PersistentBlockStorage, MockWallet>;
    type TestSyncManager = dyn SyncManager;

    async fn create_test_manager() -> TestBlocksManager {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let wallet = Arc::new(RwLock::new(MockWallet::new()));
        BlocksManager::new(wallet, storage.block_headers(), storage.blocks()).await
    }

    #[tokio::test]
    async fn test_blocks_manager_new() {
        let manager = create_test_manager().await;
        assert_eq!(manager.identifier(), ManagerIdentifier::Block);
        assert_eq!(manager.state(), SyncState::WaitForEvents);
        assert_eq!(manager.wanted_message_types(), vec![MessageType::Block]);
    }

    #[tokio::test]
    async fn test_blocks_manager_progress() {
        let mut manager = create_test_manager().await;
        manager.progress.update_last_processed(500);
        manager.progress.add_processed(10);

        let manager_ref: &TestSyncManager = &manager;
        let progress = manager_ref.progress();
        if let SyncManagerProgress::Blocks(blocks_progress) = progress {
            assert_eq!(blocks_progress.last_processed(), 500);
            assert_eq!(blocks_progress.processed(), 10);
        } else {
            panic!("Expected SyncManagerProgress::Blocks");
        }
    }

    #[tokio::test]
    async fn test_blocks_manager_handle_blocks_needed_event() {
        let mut manager = create_test_manager().await;
        manager.progress.set_state(SyncState::Synced);

        let network = MockNetworkManager::new();
        let requests = network.request_sender();

        let block_hash = dashcore::BlockHash::dummy(0);
        let mut blocks = BTreeMap::new();
        blocks.insert(FilterMatchKey::new(100, block_hash), BTreeSet::from([MOCK_WALLET_ID]));
        let event = SyncEvent::BlocksNeeded {
            blocks,
        };

        let events = manager.handle_sync_event(&event, &requests).await.unwrap();

        // Should queue the block
        assert_eq!(manager.state(), SyncState::Syncing);
        assert!(events.is_empty());
    }

    /// `process_buffered_blocks` must call `process_block_for_wallets` with
    /// the exact wallet set carried in the pipeline so already-synced
    /// wallets are not touched by routing logic.
    #[tokio::test]
    async fn test_process_buffered_blocks_routes_wallet_set() {
        use dashcore::block::Header;
        use dashcore::{Block, TxMerkleNode};
        use dashcore_hashes::Hash;

        let mut manager = create_test_manager().await;
        manager.progress.set_state(SyncState::Syncing);

        let header = Header {
            version: dashcore::blockdata::block::Version::from_consensus(1),
            prev_blockhash: dashcore::BlockHash::all_zeros(),
            merkle_root: TxMerkleNode::all_zeros(),
            time: 0,
            bits: dashcore::CompactTarget::from_consensus(0),
            nonce: 0,
        };
        let block = Block {
            header,
            txdata: vec![],
        };
        manager.pipeline.add_from_storage(block.clone(), 100, BTreeSet::from([MOCK_WALLET_ID]));

        let events = manager.process_buffered_blocks().await.unwrap();
        assert!(matches!(events.first(), Some(SyncEvent::BlockProcessed { .. })));

        // MOCK_WALLET_ID was in the routed set, so MockWallet recorded the
        // block. (MockWallet::process_block_for_wallets returns early when
        // its id is absent.)
        let processed = manager.wallet.read().await.processed_blocks();
        let processed = processed.lock().await;
        assert_eq!(processed.len(), 1);
        assert_eq!(processed[0].1, 100);
    }

    /// A wallet that is NOT in the pipeline's interested set must not be
    /// routed the block. Two wallets are registered, but only `wallet_in`
    /// appears in the routed set; the other wallet's processed log must
    /// stay empty for that block.
    #[tokio::test]
    async fn test_process_buffered_blocks_excludes_uninterested_wallet() {
        use dashcore::block::Header;
        use dashcore::{Block, TxMerkleNode};
        use dashcore_hashes::Hash;
        use key_wallet_manager::test_utils::{MockWalletState, MultiMockWallet};
        use key_wallet_manager::WalletId;

        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let multi = MultiMockWallet::new();
        let wallet_in: WalletId = [0xAA; 32];
        let wallet_out: WalletId = [0xBB; 32];
        let multi = Arc::new(RwLock::new(multi));
        {
            let mut w = multi.write().await;
            w.insert_wallet(wallet_in, MockWalletState::default());
            w.insert_wallet(wallet_out, MockWalletState::default());
        }
        let mut manager: BlocksManager<
            PersistentBlockHeaderStorage,
            PersistentBlockStorage,
            MultiMockWallet,
        > = BlocksManager::new(multi.clone(), storage.block_headers(), storage.blocks()).await;
        manager.progress.set_state(SyncState::Syncing);

        let header = Header {
            version: dashcore::blockdata::block::Version::from_consensus(1),
            prev_blockhash: dashcore::BlockHash::all_zeros(),
            merkle_root: TxMerkleNode::all_zeros(),
            time: 0,
            bits: dashcore::CompactTarget::from_consensus(0),
            nonce: 0,
        };
        let block = Block {
            header,
            txdata: vec![],
        };
        // Only wallet_in is in the routed set.
        manager.pipeline.add_from_storage(block.clone(), 100, BTreeSet::from([wallet_in]));

        let _ = manager.process_buffered_blocks().await.unwrap();

        let processed = multi.read().await.processed();
        let processed = processed.lock().await;
        // Exactly one entry, for wallet_in only.
        assert_eq!(processed.len(), 1);
        assert_eq!(processed[0].0, wallet_in);
        assert_eq!(processed[0].2, 100);
        assert!(
            !processed.iter().any(|(id, _, _)| *id == wallet_out),
            "wallet_out was not in the routed set, must not be processed"
        );
    }
}
