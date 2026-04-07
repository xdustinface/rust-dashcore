//! Masternode manager for parallel sync.
//!
//! Handles masternode list synchronization via QRInfo and MnListDiff messages.
//! Subscribes to BlockHeaderSyncComplete events to start sync after headers are caught up.
//! Emits MasternodeStateUpdated events.

use std::sync::Arc;
use std::time::Instant;

use dashcore::sml::llmq_type::network::NetworkLLMQExt;
use dashcore::sml::masternode_list_engine::MasternodeListEngine;
use tokio::sync::RwLock;

use super::pipeline::MnListDiffPipeline;
use crate::error::{SyncError, SyncResult};
use crate::network::RequestSender;
use crate::storage::BlockHeaderStorage;
use crate::sync::{MasternodesProgress, SyncEvent, SyncManager, SyncState};
use dashcore::BlockHash;
use dashcore_hashes::Hash;
use std::collections::{BTreeSet, HashSet};

/// What the MnListDiff pipeline is currently being used for.
#[derive(Debug, Default)]
pub(super) enum PipelineMode {
    /// Post-QRInfo quorum validation diffs. Run full `verify_and_complete()` on completion.
    #[default]
    QuorumValidation,
    /// Per-block incremental masternode list update. Verifies non-rotating quorums on completion.
    Incremental,
}

/// Sync state for masternode list synchronization.
#[derive(Debug, Default)]
pub(super) struct MasternodeSyncState {
    /// Block hashes for which we have received MnListDiffs.
    pub(super) known_block_hashes: HashSet<BlockHash>,
    /// Heights where the engine has masternode lists (for chaining diffs).
    pub(super) known_mn_list_heights: BTreeSet<u32>,
    /// Last block hash we synced the masternode list to (base for both QRInfo and incremental diffs).
    pub(super) last_synced_block_hash: Option<BlockHash>,
    /// Pipeline for MnListDiff requests.
    pub(super) mnlistdiff_pipeline: MnListDiffPipeline,
    /// Whether we are waiting for a QRInfo response.
    pub(super) waiting_for_qrinfo: bool,
    /// When we started waiting for QRInfo response.
    pub(super) qrinfo_wait_start: Option<Instant>,
    /// Current retry count for QRInfo.
    pub(super) qrinfo_retry_count: u8,
    /// When to retry after a ChainLock unavailability error.
    /// The QRInfo response includes the current tip which may not have ChainLock yet.
    pub(super) chainlock_retry_after: Option<Instant>,
    /// Controls pipeline completion behavior (quorum validation vs incremental update).
    pipeline_mode: PipelineMode,
}

impl MasternodeSyncState {
    fn new() -> Self {
        Self::default()
    }

    pub(super) fn is_incremental(&self) -> bool {
        matches!(self.pipeline_mode, PipelineMode::Incremental)
    }

    pub(super) fn has_pending_requests(&self) -> bool {
        !self.mnlistdiff_pipeline.is_complete()
            || self.waiting_for_qrinfo
            || self.chainlock_retry_after.is_some()
    }

    pub(super) fn clear_pending(&mut self) {
        self.mnlistdiff_pipeline.clear();
        self.waiting_for_qrinfo = false;
        self.qrinfo_wait_start = None;
        self.chainlock_retry_after = None;
        self.pipeline_mode = PipelineMode::QuorumValidation;
    }

    fn start_waiting_for_qrinfo(&mut self) {
        self.waiting_for_qrinfo = true;
        self.qrinfo_wait_start = Some(Instant::now());
    }

    pub(super) fn qrinfo_received(&mut self) {
        self.waiting_for_qrinfo = false;
        self.qrinfo_wait_start = None;
    }
}

/// Masternode manager for synchronizing masternode lists.
///
/// This manager:
/// - Waits for BlockHeaderSyncComplete event before starting sync
/// - Handles QRInfo and MnListDiff messages
/// - Verifies quorums
/// - Emits MasternodeStateUpdated events
///
/// Generic over `H: BlockHeaderStorage` to allow different storage implementations.
pub struct MasternodesManager<H: BlockHeaderStorage> {
    /// Current progress of the manager.
    pub(super) progress: MasternodesProgress,
    /// Block header storage (for height lookups).
    pub(super) header_storage: Arc<RwLock<H>>,
    /// Shared Masternode list engine.
    pub(super) engine: Arc<RwLock<MasternodeListEngine>>,
    /// Network type for genesis hash.
    network: dashcore::Network,
    /// Sync state tracking.
    pub(super) sync_state: MasternodeSyncState,
}

impl<H: BlockHeaderStorage> MasternodesManager<H> {
    /// Create a new masternode manager with the given header storage.
    pub async fn new(
        header_storage: Arc<RwLock<H>>,
        engine: Arc<RwLock<MasternodeListEngine>>,
        network: dashcore::Network,
    ) -> Self {
        // Recover sync state from engine's persisted masternode lists
        let engine_guard = engine.read().await;
        let (current_height, last_synced_block_hash) =
            match engine_guard.masternode_lists.iter().next_back() {
                Some((&height, list)) => (height, Some(list.block_hash)),
                None => (0, None),
            };
        drop(engine_guard);

        // Load block header tip for progress display
        let header_tip =
            header_storage.read().await.get_tip().await.map(|t| t.height()).unwrap_or(0);

        let mut initial_progress = MasternodesProgress::default();
        initial_progress.update_current_height(current_height);
        initial_progress.update_target_height(header_tip);
        initial_progress.update_block_header_tip_height(header_tip);
        initial_progress.set_state(SyncState::WaitingForConnections);

        let mut sync_state = MasternodeSyncState::new();
        sync_state.last_synced_block_hash = last_synced_block_hash;

        Self {
            progress: initial_progress,
            header_storage,
            engine,
            network,
            sync_state,
        }
    }

    /// Check whether the tip crosses a rotating quorum cycle boundary relative to
    /// our last synced height. Returns true when a full QRInfo is needed (first sync
    /// or crossing an `isd_llmq_type` DKG interval boundary).
    pub(super) fn needs_qrinfo_update(&self, tip_height: u32) -> bool {
        let last = self.progress.current_height();
        if last == 0 {
            return true;
        }
        let interval = self.network.isd_llmq_type().params().dkg_params.interval;
        if interval == 0 {
            return true;
        }
        tip_height / interval > last / interval
    }

    /// Send an incremental GetMnListDiff for the current tip.
    pub(super) async fn send_mnlistdiff_for_tip(
        &mut self,
        requests: &RequestSender,
    ) -> SyncResult<Vec<SyncEvent>> {
        if !self.sync_state.mnlistdiff_pipeline.is_complete() {
            tracing::warn!("send_mnlistdiff_for_tip called with non-empty pipeline, skipping");
            return Ok(vec![]);
        }

        let storage = self.header_storage.read().await;
        let tip = match storage.get_tip().await {
            Some(tip) => tip,
            None => return Ok(vec![]),
        };
        let tip_hash = *tip.hash();
        drop(storage);

        let base_hash = self
            .sync_state
            .last_synced_block_hash
            .or_else(|| self.network.known_genesis_block_hash())
            .unwrap_or_else(|| {
                tracing::warn!("No last synced block hash or genesis hash available, falling back to all-zeros hash");
                BlockHash::all_zeros()
            });

        if base_hash == tip_hash {
            tracing::debug!(
                "Skipping incremental MnListDiff: base and tip are the same ({})",
                tip_hash
            );
            return Ok(vec![]);
        }

        self.sync_state.pipeline_mode = PipelineMode::Incremental;
        self.sync_state.mnlistdiff_pipeline.queue_requests(vec![(base_hash, tip_hash)]);
        self.sync_state.mnlistdiff_pipeline.send_pending(requests)?;

        tracing::debug!("Requesting incremental MnListDiff: {} -> {}", base_hash, tip_hash);
        Ok(vec![])
    }

    /// Send QRInfo request for the current tip.
    ///
    /// Called when BlockHeaderSyncComplete is received, ensuring we have all headers.
    pub(super) async fn send_qrinfo_for_tip(
        &mut self,
        requests: &RequestSender,
    ) -> SyncResult<Vec<SyncEvent>> {
        // Get info from storage
        let (tip_height, tip_block_hash) = {
            let storage = self.header_storage.read().await;
            match storage.get_tip().await {
                Some(tip) => (tip.height(), *tip.hash()),
                None => {
                    tracing::warn!("MasternodesManager: No headers available for QRInfo request");
                    return Ok(vec![]);
                }
            }
        };

        if tip_height == 0 {
            tracing::info!("MasternodesManager: At genesis, nothing to sync");
            return Ok(vec![]);
        }

        // Only transition to Syncing if not already Synced (incremental updates stay Synced)
        if self.state() != SyncState::Synced {
            self.set_state(SyncState::Syncing);
        }

        // Build known hashes from tracked block hashes
        let mut known_hashes: Vec<_> = self.sync_state.known_block_hashes.iter().copied().collect();

        // Add base hash
        let base_hash = self
            .sync_state
            .last_synced_block_hash
            .or_else(|| self.network.known_genesis_block_hash());
        if let Some(hash) = base_hash {
            known_hashes.push(hash);
        }

        // Send QRInfo request for the tip
        // Note: The server's response includes `mn_list_diff_tip` which is always the current tip,
        // regardless of the requested block. If the tip was just mined and doesn't have a ChainLock
        // yet, we'll retry after a delay.
        tracing::info!("Requesting QRInfo for tip at height {}", tip_height);
        requests.request_qr_info(known_hashes, tip_block_hash, true)?;

        self.sync_state.start_waiting_for_qrinfo();

        Ok(vec![])
    }

    /// Handle pipeline completion by branching on the current pipeline mode.
    ///
    /// - `QuorumValidation`: run full quorum verification (post-QRInfo flow).
    /// - `Incremental`: update progress and emit event without quorum verification.
    pub(super) async fn complete_pipeline(&mut self) -> SyncResult<Vec<SyncEvent>> {
        match &self.sync_state.pipeline_mode {
            PipelineMode::QuorumValidation => {
                tracing::info!("All MnListDiff responses received");
                self.verify_and_complete().await
            }
            PipelineMode::Incremental => {
                // The engine maintains a single latest list for incremental updates,
                // so reading from the engine's last entry gives us the result of the
                // diff we just applied.
                let mut engine = self.engine.write().await;
                if let Some((&height, list)) = engine.masternode_lists.iter().next_back() {
                    let last_hash = list.block_hash;

                    if let Err(e) = engine.verify_non_rotating_masternode_list_quorums(height, &[])
                    {
                        tracing::warn!(
                            "Incremental quorum verification failed at height {}: {}",
                            height,
                            e
                        );
                        // Remove unverified entry so the engine stays consistent with
                        // `last_synced_block_hash` and future diffs don't build on
                        // unverified state.
                        engine.masternode_lists.remove(&height);
                        drop(engine);
                        self.sync_state.known_mn_list_heights.remove(&height);
                        self.sync_state.known_block_hashes.remove(&last_hash);
                        return Ok(vec![]);
                    }

                    drop(engine);
                    self.sync_state.last_synced_block_hash = Some(last_hash);
                    self.progress.update_current_height(height);
                    tracing::debug!("Incremental MnListDiff complete at height {}", height);
                    return Ok(vec![SyncEvent::MasternodeStateUpdated {
                        height,
                    }]);
                }
                Ok(vec![])
            }
        }
    }

    /// Verify non-rotating quorums and finalize the post-QRInfo sync pipeline.
    ///
    /// Emits `MasternodeStateUpdated` on success. On initial sync, also transitions
    /// state to `Synced`.
    pub(super) async fn verify_and_complete(&mut self) -> SyncResult<Vec<SyncEvent>> {
        let mut events = Vec::new();
        let is_initial_sync = self.state() == SyncState::Syncing;

        let mut engine = self.engine.write().await;

        // Get the latest height from the engine and verify at that height
        if let Some((&height, list)) = engine.masternode_lists.iter().next_back() {
            let last_hash = list.block_hash;

            if let Err(e) = engine.verify_non_rotating_masternode_list_quorums(height, &[]) {
                drop(engine);
                self.set_state(SyncState::Error);
                return Err(SyncError::MasternodeSyncFailed(format!(
                    "Quorum verification failed at height {}: {}",
                    height, e
                )));
            }

            tracing::info!("Non-rotating quorum verification completed at height {}", height);

            self.sync_state.last_synced_block_hash = Some(last_hash);
            self.progress.update_current_height(height);

            events.push(SyncEvent::MasternodeStateUpdated {
                height,
            });
        } else if is_initial_sync {
            drop(engine);
            self.set_state(SyncState::Error);
            return Err(SyncError::MasternodeSyncFailed("No masternode lists available".into()));
        }

        drop(engine);

        if is_initial_sync {
            self.set_state(SyncState::Synced);
            tracing::info!("Masternode sync complete at height {}", self.progress.current_height());
        }

        Ok(events)
    }
}

impl<H: BlockHeaderStorage> std::fmt::Debug for MasternodesManager<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MasternodesManager").field("progress", &self.progress).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashcore::block::Header;
    use dashcore::network::message::NetworkMessage;
    use dashcore::sml::masternode_list::MasternodeList;
    use tokio::sync::mpsc;

    use crate::network::{MessageType, NetworkRequest, RequestSender};
    use crate::storage::{
        BlockHeaderStorage, DiskStorageManager, PersistentBlockHeaderStorage, StorageManager,
    };
    use crate::sync::sync_manager::SyncManager;
    use crate::sync::{ManagerIdentifier, SyncManagerProgress};

    type TestMasternodesManager = MasternodesManager<PersistentBlockHeaderStorage>;

    async fn create_test_manager() -> TestMasternodesManager {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let engine = Arc::new(RwLock::new(MasternodeListEngine::default_for_network(
            dashcore::Network::Testnet,
        )));
        MasternodesManager::new(storage.block_headers(), engine, dashcore::Network::Testnet).await
    }

    #[tokio::test]
    async fn test_masternode_manager_new() {
        let manager = create_test_manager().await;
        assert_eq!(manager.identifier(), ManagerIdentifier::Masternode);
        assert_eq!(manager.state(), SyncState::WaitingForConnections);
        assert_eq!(
            manager.wanted_message_types(),
            vec![MessageType::MnListDiff, MessageType::QRInfo]
        );
    }

    #[tokio::test]
    async fn test_masternode_manager_progress() {
        let mut manager = create_test_manager().await;
        manager.progress.update_current_height(500);
        manager.progress.update_target_height(1000);
        manager.progress.add_diffs_processed(10);

        let progress = manager.progress();
        if let SyncManagerProgress::Masternodes(progress) = progress {
            assert_eq!(progress.current_height(), 500);
            assert_eq!(progress.target_height(), 1000);
            assert_eq!(progress.diffs_processed(), 10);
            assert!(progress.last_activity().elapsed().as_secs() < 1);
        } else {
            panic!("Expected SyncManagerProgress::Masternodes");
        }
    }

    #[tokio::test]
    async fn test_needs_qrinfo_when_never_synced() {
        let manager = create_test_manager().await;
        // current_height == 0 means we've never synced, always need QRInfo
        assert!(manager.needs_qrinfo_update(1));
        assert!(manager.needs_qrinfo_update(288));
        assert!(manager.needs_qrinfo_update(1000));
    }

    #[tokio::test]
    async fn test_needs_qrinfo_at_cycle_boundary() {
        let mut manager = create_test_manager().await;
        // isd_llmq_type for testnet is Llmqtype60_75 with interval 288
        manager.progress.update_current_height(287);

        // Tip at 288 crosses the cycle boundary (287/288 = 0, 288/288 = 1)
        assert!(manager.needs_qrinfo_update(288));

        // Multiple cycles ahead also triggers (100/288 = 0, 1000/288 = 3)
        manager.progress.update_current_height(100);
        assert!(manager.needs_qrinfo_update(1000));
    }

    #[tokio::test]
    async fn test_skips_qrinfo_within_cycle() {
        let mut manager = create_test_manager().await;
        manager.progress.update_current_height(290);

        // Tip within the same cycle (290/288 = 1, 300/288 = 1)
        assert!(!manager.needs_qrinfo_update(300));
        assert!(!manager.needs_qrinfo_update(575));
    }

    #[tokio::test]
    async fn test_clear_pending_resets_pipeline_mode() {
        let mut manager = create_test_manager().await;
        manager.sync_state.pipeline_mode = PipelineMode::Incremental;
        manager.sync_state.chainlock_retry_after = Some(Instant::now());
        manager.sync_state.clear_pending();
        assert!(matches!(manager.sync_state.pipeline_mode, PipelineMode::QuorumValidation));
        assert!(manager.sync_state.chainlock_retry_after.is_none());
    }

    #[tokio::test]
    async fn test_complete_pipeline_incremental_emits_event() {
        let mut manager = create_test_manager().await;
        let fake_hash = BlockHash::all_zeros();

        // Insert a fake masternode list at height 100 into the engine
        let mn_list = MasternodeList::empty(fake_hash, 100);
        manager.engine.write().await.masternode_lists.insert(100, mn_list);

        manager.sync_state.pipeline_mode = PipelineMode::Incremental;
        let events = manager.complete_pipeline().await.unwrap();

        // `last_synced_block_hash` should be derived from the engine's actual state
        assert_eq!(manager.sync_state.last_synced_block_hash, Some(fake_hash));
        assert_eq!(manager.progress.current_height(), 100);
        assert!(events.iter().any(|e| matches!(
            e,
            SyncEvent::MasternodeStateUpdated {
                height: 100
            }
        )));
    }

    #[tokio::test]
    async fn test_complete_pipeline_incremental_no_op_when_engine_empty() {
        let mut manager = create_test_manager().await;
        manager.sync_state.pipeline_mode = PipelineMode::Incremental;
        let events = manager.complete_pipeline().await.unwrap();

        // Engine has no masternode lists, so nothing should be emitted or advanced
        assert!(events.is_empty());
        assert!(manager.sync_state.last_synced_block_hash.is_none());
    }

    fn create_test_request_sender() -> (RequestSender, mpsc::UnboundedReceiver<NetworkRequest>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (RequestSender::new(tx), rx)
    }

    #[tokio::test]
    async fn test_send_mnlistdiff_for_tip_uses_last_synced_hash() {
        let mut manager = create_test_manager().await;
        let (requests, mut rx) = create_test_request_sender();

        // Store a header so `get_tip()` returns something
        let header = Header::dummy(100);
        let tip_hash = header.block_hash();
        manager.header_storage.write().await.store_headers_at_height(&[header], 100).await.unwrap();

        // Set last_synced_block_hash to simulate a previous sync
        let base_hash = BlockHash::from_byte_array([0xAA; 32]);
        manager.sync_state.last_synced_block_hash = Some(base_hash);

        manager.send_mnlistdiff_for_tip(&requests).await.unwrap();

        assert!(matches!(manager.sync_state.pipeline_mode, PipelineMode::Incremental));
        assert!(!manager.sync_state.mnlistdiff_pipeline.is_complete());

        // Verify the sent message uses last_synced_block_hash as base
        let msg = rx.try_recv().unwrap();
        if let NetworkRequest::SendMessage(NetworkMessage::GetMnListD(get_diff)) = msg {
            assert_eq!(get_diff.base_block_hash, base_hash);
            assert_eq!(get_diff.block_hash, tip_hash);
        } else {
            panic!("Expected GetMnListD message");
        }
    }

    #[tokio::test]
    async fn test_send_mnlistdiff_for_tip_falls_back_to_genesis() {
        let mut manager = create_test_manager().await;
        let (requests, mut rx) = create_test_request_sender();

        // Store a header so `get_tip()` returns something
        let header = Header::dummy(100);
        let tip_hash = header.block_hash();
        manager.header_storage.write().await.store_headers_at_height(&[header], 100).await.unwrap();

        // No last_synced_block_hash set, should fall back to genesis
        assert!(manager.sync_state.last_synced_block_hash.is_none());

        manager.send_mnlistdiff_for_tip(&requests).await.unwrap();

        assert!(matches!(manager.sync_state.pipeline_mode, PipelineMode::Incremental));

        // Verify the sent message uses the genesis hash as base
        let genesis_hash = dashcore::Network::Testnet.known_genesis_block_hash().unwrap();
        let msg = rx.try_recv().unwrap();
        if let NetworkRequest::SendMessage(NetworkMessage::GetMnListD(get_diff)) = msg {
            assert_eq!(get_diff.base_block_hash, genesis_hash);
            assert_eq!(get_diff.block_hash, tip_hash);
        } else {
            panic!("Expected GetMnListD message");
        }
    }

    #[tokio::test]
    async fn test_send_mnlistdiff_for_tip_no_op_without_headers() {
        let mut manager = create_test_manager().await;
        let (requests, _rx) = create_test_request_sender();

        // No headers stored, should return empty
        let events = manager.send_mnlistdiff_for_tip(&requests).await.unwrap();
        assert!(events.is_empty());
        assert!(manager.sync_state.mnlistdiff_pipeline.is_complete());
    }
}
