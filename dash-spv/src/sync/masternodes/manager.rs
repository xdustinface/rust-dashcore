//! Masternode manager for parallel sync.
//!
//! Handles masternode list synchronization via QRInfo and MnListDiff messages.
//! Subscribes to BlockHeaderSyncComplete events to start sync after headers are caught up.
//! Emits MasternodeStateUpdated events.

use std::sync::Arc;
use std::time::Instant;

use dashcore::sml::masternode_list_engine::MasternodeListEngine;
use tokio::sync::RwLock;

use super::pipeline::MnListDiffPipeline;
use crate::error::{SyncError, SyncResult};
use crate::network::RequestSender;
use crate::storage::BlockHeaderStorage;
use crate::sync::{MasternodesProgress, SyncEvent, SyncManager, SyncState};
use dashcore::BlockHash;
use std::collections::{BTreeSet, HashSet};

/// Sync state for masternode list synchronization.
#[derive(Debug, Default)]
pub(super) struct MasternodeSyncState {
    /// Block hashes for which we have received MnListDiffs.
    pub(super) known_block_hashes: HashSet<BlockHash>,
    /// Heights where the engine has masternode lists (for chaining diffs).
    pub(super) known_mn_list_heights: BTreeSet<u32>,
    /// Last successfully processed QRInfo block hash (for progressive sync).
    pub(super) last_qrinfo_block_hash: Option<BlockHash>,
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
}

impl MasternodeSyncState {
    fn new() -> Self {
        Self::default()
    }

    pub(super) fn has_pending_requests(&self) -> bool {
        !self.mnlistdiff_pipeline.is_complete() || self.waiting_for_qrinfo
    }

    pub(super) fn clear_pending(&mut self) {
        self.mnlistdiff_pipeline.clear();
        self.waiting_for_qrinfo = false;
        self.qrinfo_wait_start = None;
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
        // Load current height from engine's masternode lists
        let current_height =
            engine.read().await.masternode_lists.keys().last().copied().unwrap_or(0);

        // Load block header tip for progress display
        let header_tip =
            header_storage.read().await.get_tip().await.map(|t| t.height()).unwrap_or(0);

        let mut initial_progress = MasternodesProgress::default();
        initial_progress.update_current_height(current_height);
        initial_progress.update_target_height(header_tip);
        initial_progress.update_block_header_tip_height(header_tip);
        initial_progress.set_state(SyncState::WaitingForConnections);

        Self {
            progress: initial_progress,
            header_storage,
            engine,
            network,
            sync_state: MasternodeSyncState::new(),
        }
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
            .last_qrinfo_block_hash
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

    /// Verify quorums and mark complete.
    ///
    /// For initial sync (state == Syncing), emits MasternodeStateUpdated and logs completion.
    /// For incremental updates (state == Synced), updates quietly without events.
    pub(super) async fn verify_and_complete(&mut self) -> SyncResult<Vec<SyncEvent>> {
        let mut events = Vec::new();
        let is_initial_sync = self.state() == SyncState::Syncing;

        let mut engine = self.engine.write().await;

        // Get the latest height from the engine and verify at that height
        if let Some(&height) = engine.masternode_lists.keys().last() {
            if let Err(e) = engine.verify_non_rotating_masternode_list_quorums(height, &[]) {
                drop(engine);
                self.set_state(SyncState::Error);
                return Err(SyncError::MasternodeSyncFailed(format!(
                    "Quorum verification failed at height {}: {}",
                    height, e
                )));
            }

            tracing::info!("Non-rotating quorum verification completed at height {}", height);

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
    use crate::network::{MessageType, NetworkEvent, NetworkRequest};
    use crate::storage::{DiskStorageManager, PersistentBlockHeaderStorage, StorageManager};
    use crate::sync::sync_manager::SyncManager;
    use crate::sync::{ManagerIdentifier, SyncManagerProgress};
    use tokio::sync::mpsc;

    type TestMasternodesManager = MasternodesManager<PersistentBlockHeaderStorage>;

    async fn create_test_manager() -> TestMasternodesManager {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let engine = Arc::new(RwLock::new(MasternodeListEngine::default_for_network(
            dashcore::Network::Testnet,
        )));
        MasternodesManager::new(storage.block_headers(), engine, dashcore::Network::Testnet).await
    }

    fn create_request_sender() -> (RequestSender, mpsc::UnboundedReceiver<NetworkRequest>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (RequestSender::new(tx), rx)
    }

    fn peers_updated_event(count: usize, height: Option<u32>) -> NetworkEvent {
        NetworkEvent::PeersUpdated {
            connected_count: count,
            addresses: vec![],
            best_height: height,
        }
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
    async fn test_recovery_from_error_state_on_peer_reconnect() {
        let mut manager = create_test_manager().await;
        let (requests, _rx) = create_request_sender();

        // Simulate: initialized -> error state (as happens after QRInfo timeouts)
        manager.set_state(SyncState::Error);
        assert_eq!(manager.state(), SyncState::Error);

        // Peers reconnect
        let event = peers_updated_event(3, Some(1000));
        let _ = manager.handle_network_event(&event, &requests).await;

        // Should have recovered from error state
        assert_ne!(manager.state(), SyncState::Error);
        assert!(
            manager.state() == SyncState::WaitForEvents || manager.state() == SyncState::Syncing,
            "Expected WaitForEvents or Syncing, got {:?}",
            manager.state()
        );
    }

    #[tokio::test]
    async fn test_error_state_resets_retry_count() {
        let mut manager = create_test_manager().await;
        let (requests, _rx) = create_request_sender();

        // Simulate exhausted retries leading to error state
        manager.set_state(SyncState::Error);
        manager.sync_state.qrinfo_retry_count = 3;
        manager.sync_state.waiting_for_qrinfo = true;

        // Peers reconnect
        let event = peers_updated_event(2, Some(500));
        let _ = manager.handle_network_event(&event, &requests).await;

        // Retry count should be reset
        assert_eq!(manager.sync_state.qrinfo_retry_count, 0);
        assert!(!manager.sync_state.waiting_for_qrinfo);
    }

    #[tokio::test]
    async fn test_no_recovery_when_no_peers() {
        let mut manager = create_test_manager().await;
        let (requests, _rx) = create_request_sender();

        manager.set_state(SyncState::Error);

        // All peers disconnected
        let event = peers_updated_event(0, None);
        let _ = manager.handle_network_event(&event, &requests).await;

        // Should transition to WaitingForConnections (stop_sync), not stay in Error
        assert_eq!(manager.state(), SyncState::WaitingForConnections);
    }

    #[tokio::test]
    async fn test_waiting_for_connections_still_works() {
        let mut manager = create_test_manager().await;
        let (requests, _rx) = create_request_sender();

        manager.set_state(SyncState::WaitingForConnections);

        // Peers connect
        let event = peers_updated_event(1, Some(100));
        let _ = manager.handle_network_event(&event, &requests).await;

        // Should transition to WaitForEvents via start_sync()
        assert_eq!(manager.state(), SyncState::WaitForEvents);
    }

    #[tokio::test]
    async fn test_synced_state_not_affected_by_peer_update() {
        let mut manager = create_test_manager().await;
        let (requests, _rx) = create_request_sender();

        manager.set_state(SyncState::Synced);

        // Peers update
        let event = peers_updated_event(3, Some(1000));
        let _ = manager.handle_network_event(&event, &requests).await;

        // Should stay Synced (no unnecessary recovery)
        assert_eq!(manager.state(), SyncState::Synced);
    }

    #[tokio::test]
    async fn test_full_disconnect_reconnect_cycle_recovery() {
        let mut manager = create_test_manager().await;
        let (requests, _rx) = create_request_sender();

        // Step 1: Manager in Error state after QRInfo timeouts
        manager.set_state(SyncState::Error);
        manager.sync_state.qrinfo_retry_count = 3;

        // Step 2: All peers disconnect (wifi off)
        let disconnect = peers_updated_event(0, None);
        let _ = manager.handle_network_event(&disconnect, &requests).await;
        assert_eq!(manager.state(), SyncState::WaitingForConnections);

        // Step 3: Peers reconnect (wifi back)
        let reconnect = peers_updated_event(3, Some(1000));
        let _ = manager.handle_network_event(&reconnect, &requests).await;

        // Should have started sync via start_sync() from WaitingForConnections
        assert_eq!(manager.state(), SyncState::WaitForEvents);
    }

    #[tokio::test]
    async fn test_error_recovery_without_disconnect_first() {
        let mut manager = create_test_manager().await;
        let (requests, _rx) = create_request_sender();

        // Error state but peers never fully disconnected (e.g., only QRInfo failed)
        manager.set_state(SyncState::Error);
        manager.sync_state.qrinfo_retry_count = 3;

        // A new peer connects (PeersUpdated with more peers)
        let event = peers_updated_event(4, Some(2000));
        let _ = manager.handle_network_event(&event, &requests).await;

        // Should recover directly from Error state
        assert_ne!(manager.state(), SyncState::Error);
        assert_eq!(manager.sync_state.qrinfo_retry_count, 0);
    }
}
