//! ChainLock manager for parallel sync.
//!
//! Handles ChainLock messages (clsig) from the network. Validates ChainLocks
//! only after masternode data is available. Since ChainLocks are cumulative
//! (all blocks below the best ChainLock are implicitly locked), we only track
//! the best validated ChainLock.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashcore::ephemerealdata::chain_lock::ChainLock;
use dashcore::hash_types::ChainLockHash;
use dashcore::sml::masternode_list_engine::MasternodeListEngine;
use dashcore::BlockHash;
use tokio::sync::RwLock;

use crate::error::SyncResult;
use crate::network::{MisbehaviorKind, RequestSender};
use crate::storage::{BlockHeaderStorage, MetadataStorage};
use crate::sync::{ChainLockProgress, SyncEvent};

/// Metadata key for persisting the best validated ChainLock.
const BEST_CHAINLOCK_KEY: &str = "best_chainlock";

/// How long a CLSig waiting on an unresolved header is kept before eviction.
pub(super) const PENDING_UNKNOWN_HASH_TTL: Duration = Duration::from_secs(5 * 60);

/// Result of matching a CLSig against the local header chain.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum BlockHashVerification {
    /// Local header at the chainlock's height matches the chainlock's hash.
    Match,
    /// Local header at the chainlock's height resolves to a different hash.
    Mismatch {
        local_header_hash: BlockHash,
    },
    /// No header at the chainlock's height in local storage yet.
    Unknown,
}

/// ChainLock manager for the parallel sync coordinator.
///
/// This manager:
/// - Subscribes to CLSig messages from the network
/// - Validates ChainLocks only after masternode sync is complete
/// - Tracks only the best (highest) validated ChainLock
/// - Emits ChainLockReceived events
pub struct ChainLockManager<H: BlockHeaderStorage, M: MetadataStorage> {
    /// Current progress of the manager.
    pub(super) progress: ChainLockProgress,
    /// Block header storage for hash verification.
    header_storage: Arc<RwLock<H>>,
    /// Metadata storage for persisting the best chainlock.
    metadata_storage: Arc<RwLock<M>>,
    /// Masternode engine for BLS signature validation.
    masternode_engine: Arc<RwLock<MasternodeListEngine>>,
    /// The best (highest height) validated ChainLock.
    best_chainlock: Option<ChainLock>,
    /// ChainLock hashes that have been requested (to avoid duplicate requests).
    pub(super) requested_chainlocks: HashSet<ChainLockHash>,
    /// Whether masternode sync is complete and we can validate signatures.
    masternode_ready: bool,
    /// Highest chainlock that arrived before `masternode_ready` and
    /// therefore could not be validated yet. Re-validated on the
    /// not-ready → ready transition (see [`Self::on_masternode_ready`])
    /// so we don't lose a chainlock that landed during the gap between
    /// the chainlock manager starting and masternode sync completing.
    pending_validation: Option<ChainLock>,
    /// CLSigs that arrived for a block hash the local chain has not yet
    /// resolved at the chainlock's claimed height. Re-evaluated when new
    /// headers land. Keyed by the chainlock's block hash so duplicates
    /// from the same announcement collapse. Entries past
    /// [`PENDING_UNKNOWN_HASH_TTL`] are dropped on `tick`. The
    /// `Option<SocketAddr>` is the peer that delivered the CLSig, used for
    /// misbehavior reporting when the hash later resolves to a `Mismatch`.
    pub(super) pending_unknown_hash: HashMap<BlockHash, (ChainLock, Option<SocketAddr>, Instant)>,
    /// Shared snapshot of the best validated chainlock height. `0` means
    /// "no chainlock observed yet". Read by `BlockHeadersManager` as the
    /// floor for the reorg cascade.
    chainlock_height: Arc<AtomicU32>,
}

impl<H: BlockHeaderStorage, M: MetadataStorage> ChainLockManager<H, M> {
    /// Create a new ChainLock manager.
    pub async fn new(
        header_storage: Arc<RwLock<H>>,
        metadata_storage: Arc<RwLock<M>>,
        masternode_engine: Arc<RwLock<MasternodeListEngine>>,
        chainlock_height: Arc<AtomicU32>,
    ) -> Self {
        let mut manager = Self {
            progress: ChainLockProgress::default(),
            header_storage,
            metadata_storage,
            masternode_engine,
            best_chainlock: None,
            requested_chainlocks: HashSet::new(),
            masternode_ready: false,
            pending_validation: None,
            pending_unknown_hash: HashMap::new(),
            chainlock_height,
        };

        // TODO: Move load_best_chainlock() and save_best_chainlock() to MetadataStorage trait.
        manager.load_best_chainlock().await;
        if let Some(cl) = &manager.best_chainlock {
            manager.chainlock_height.store(cl.block_height, Ordering::Release);
        }

        manager
    }

    /// Apply the masternode-ready transition.
    ///
    /// Validates any chainlock cached in `pending_validation` (i.e. a
    /// chainlock that arrived before masternode state was available).
    /// Returns the events to emit to downstream consumers:
    ///
    /// - `Match` / `Unknown`: validates the BLS signature; on success emits
    ///   `ChainLockReceived { validated: true }` and re-broadcasts the best
    ///   chainlock (including the persisted-from-disk one if no pending).
    /// - `Mismatch`: the resolved header disagrees with the chainlock's claimed
    ///   hash. If the BLS signature validates, emits `ChainLockForcedReorg` so
    ///   the block-headers manager can drive a cascade toward the chainlocked
    ///   chain. If the signature is invalid, the chainlock is dropped silently.
    pub(super) async fn on_masternode_ready(&mut self) -> Vec<SyncEvent> {
        self.masternode_ready = true;

        if let Some(pending) = self.pending_validation.take() {
            match self.verify_block_hash(&pending).await {
                BlockHashVerification::Match | BlockHashVerification::Unknown => {
                    if self.validate_signature(&pending).await {
                        self.progress.add_valid(1);
                        self.progress.update_best_validated_height(pending.block_height);
                        let height = pending.block_height;
                        self.best_chainlock = Some(pending);
                        self.chainlock_height.store(height, Ordering::Release);
                        self.save_best_chainlock().await;
                    } else {
                        self.progress.add_invalid(1);
                    }
                }
                BlockHashVerification::Mismatch {
                    ..
                } => {
                    if self.validate_signature(&pending).await {
                        self.progress.add_valid(1);
                        self.progress.update_best_validated_height(pending.block_height);
                        let height = pending.block_height;
                        let fork_height = height.saturating_sub(1);
                        self.best_chainlock = Some(pending.clone());
                        self.chainlock_height.store(height, Ordering::Release);
                        self.save_best_chainlock().await;
                        return vec![SyncEvent::ChainLockForcedReorg {
                            chain_lock: pending,
                            fork_height,
                        }];
                    } else {
                        self.progress.add_invalid(1);
                    }
                }
            }
        }

        if let Some(chain_lock) = self.best_chainlock.clone() {
            vec![SyncEvent::ChainLockReceived {
                chain_lock,
                validated: true,
            }]
        } else {
            vec![]
        }
    }

    pub(super) fn is_masternode_ready(&self) -> bool {
        self.masternode_ready
    }

    /// Reset state for a chain reorg, blocking validation until masternode data
    /// is re-established on the new chain.
    pub(super) fn reset_for_reorg(&mut self) {
        self.masternode_ready = false;
        self.pending_validation = None;
    }

    /// Reset state for a peer disconnect. `pending_validation` is intentionally
    /// kept: a chainlock that arrived before `masternode_ready` remains valid on
    /// the same chain and must be re-evaluated when `on_masternode_ready` fires
    /// on the next reconnect.
    ///
    /// Safety invariant: correctness here relies on header sync always completing
    /// (and emitting any `ChainReorg` → `reset_for_reorg`) before masternode sync
    /// emits `MasternodeStateUpdated`. If those phases are ever parallelized, a
    /// `ChainReorg` check must be added in this path.
    pub(super) fn reset_for_disconnect(&mut self) {
        self.masternode_ready = false;
    }

    /// Process an incoming ChainLock message.
    ///
    /// `peer` is the address that delivered the CLSig, used for misbehavior
    /// reporting when the local chain disagrees on the chainlocked block and
    /// the BLS signature does not verify. `requests` is the channel into the
    /// network manager so the reputation penalty can be applied.
    pub(super) async fn process_chainlock(
        &mut self,
        chainlock: &ChainLock,
        peer: Option<SocketAddr>,
        requests: &RequestSender,
    ) -> SyncResult<Vec<SyncEvent>> {
        let height = chainlock.block_height;
        let block_hash = chainlock.block_hash;

        tracing::info!("Processing ChainLock for height {} hash {}", height, block_hash);

        // Skip if we already have a better or equal ChainLock
        if let Some(best) = &self.best_chainlock {
            if height <= best.block_height {
                tracing::debug!(
                    "Ignoring ChainLock at height {} (best is {})",
                    height,
                    best.block_height
                );
                return Ok(vec![]);
            }
        }

        // Only validate if masternode sync is complete. Cache the
        // highest pre-ready chainlock so the masternode-ready
        // transition can retry validation rather than discarding it
        // (`on_masternode_ready`). This cache is independent of the
        // unknown-hash queue: header arrival is checked again on the
        // ready transition.
        if !self.masternode_ready {
            tracing::debug!(
                "Caching ChainLock at height {} for validation once masternode sync completes",
                height
            );
            let replace = self
                .pending_validation
                .as_ref()
                .is_none_or(|existing| height > existing.block_height);
            if replace {
                self.pending_validation = Some(chainlock.clone());
            }
            return Ok(vec![SyncEvent::ChainLockReceived {
                chain_lock: chainlock.clone(),
                validated: false,
            }]);
        }

        match self.verify_block_hash(chainlock).await {
            BlockHashVerification::Match => {
                // Fall through to the existing validated path below.
            }
            BlockHashVerification::Unknown => {
                tracing::info!(
                    "Queueing ChainLock for unresolved header at height {} hash {}",
                    height,
                    block_hash
                );
                self.pending_unknown_hash
                    .entry(block_hash)
                    .or_insert_with(|| (chainlock.clone(), peer, Instant::now()));
                return Ok(vec![SyncEvent::PendingChainLockQueued {
                    chainlock: chainlock.clone(),
                }]);
            }
            BlockHashVerification::Mismatch {
                local_header_hash,
            } => {
                tracing::warn!(
                    "ChainLock hash mismatch at height {}: local {} vs CLSig {}; validating signature",
                    height,
                    local_header_hash,
                    block_hash
                );
                if !self.validate_signature(chainlock).await {
                    self.progress.add_invalid(1);
                    if let Some(peer_addr) = peer {
                        if let Err(e) = requests.report_misbehavior(
                            peer_addr,
                            MisbehaviorKind::InvalidChainLockSignature,
                        ) {
                            tracing::warn!(
                                "Failed to report invalid-CLSig misbehavior for {}: {}",
                                peer_addr,
                                e
                            );
                        }
                    }
                    return Ok(vec![]);
                }
                // Valid signature against a local-disagreeing block: drive a
                // forced reorg through `BlockHeadersManager`. `fork_height`
                // is the parent of the chainlocked block since the chainlock
                // locks block at `block_height` and rewrites everything from
                // there.
                let fork_height = height.saturating_sub(1);
                self.progress.add_valid(1);
                return Ok(vec![SyncEvent::ChainLockForcedReorg {
                    chain_lock: chainlock.clone(),
                    fork_height,
                }]);
            }
        }

        // Validate with masternode engine
        let validated = self.validate_signature(chainlock).await;

        if validated {
            self.progress.add_valid(1);
            self.progress.update_best_validated_height(height);

            // Update best ChainLock and persist to storage
            self.best_chainlock = Some(chainlock.clone());
            self.chainlock_height.store(height, Ordering::Release);
            self.save_best_chainlock().await;
        } else {
            self.progress.add_invalid(1);
        }

        Ok(vec![SyncEvent::ChainLockReceived {
            chain_lock: chainlock.clone(),
            validated,
        }])
    }

    /// Persist the best chainlock to metadata storage.
    async fn save_best_chainlock(&self) {
        let Some(chainlock) = &self.best_chainlock else {
            return;
        };
        match serde_json::to_vec(chainlock) {
            Ok(bytes) => {
                let mut storage = self.metadata_storage.write().await;
                if let Err(e) = storage.store_metadata(BEST_CHAINLOCK_KEY, &bytes).await {
                    tracing::warn!("Failed to persist best chainlock: {}", e);
                }
            }
            Err(e) => {
                tracing::warn!("Failed to serialize best chainlock: {}", e);
            }
        }
    }

    /// Load the best chainlock from metadata storage and restore progress.
    pub(super) async fn load_best_chainlock(&mut self) {
        let storage = self.metadata_storage.read().await;
        match storage.load_metadata(BEST_CHAINLOCK_KEY).await {
            Ok(Some(bytes)) => match serde_json::from_slice::<ChainLock>(&bytes) {
                Ok(chainlock) => {
                    let height = chainlock.block_height;
                    tracing::info!("Restored persisted ChainLock at height {}", height);
                    self.progress.update_best_validated_height(height);
                    self.best_chainlock = Some(chainlock);
                }
                Err(e) => {
                    tracing::warn!("Failed to deserialize persisted chainlock: {}", e);
                }
            },
            Ok(None) => {
                tracing::debug!("No persisted chainlock found (fresh start)");
            }
            Err(e) => {
                tracing::warn!("Failed to load persisted chainlock: {}", e);
            }
        }
    }

    /// Match the ChainLock's claimed block hash against our stored header.
    ///
    /// - [`BlockHashVerification::Match`]: local header at this height has
    ///   the same hash.
    /// - [`BlockHashVerification::Mismatch`]: local header has a different
    ///   hash. The chainlock either points at a fork branch we don't have or
    ///   the signature is invalid; the caller decides which.
    /// - [`BlockHashVerification::Unknown`]: no header yet. Caller queues the
    ///   chainlock and re-checks once headers progress.
    pub(super) async fn verify_block_hash(&self, chainlock: &ChainLock) -> BlockHashVerification {
        let storage = self.header_storage.read().await;
        match storage.get_header(chainlock.block_height).await {
            Ok(Some(header)) => {
                let local_header_hash = header.block_hash();
                if local_header_hash == chainlock.block_hash {
                    BlockHashVerification::Match
                } else {
                    BlockHashVerification::Mismatch {
                        local_header_hash,
                    }
                }
            }
            Ok(None) => BlockHashVerification::Unknown,
            Err(e) => {
                tracing::warn!(
                    "Storage error checking ChainLock header at height {}: {}",
                    chainlock.block_height,
                    e
                );
                BlockHashVerification::Unknown
            }
        }
    }

    /// Validate the ChainLock BLS signature using the masternode engine.
    async fn validate_signature(&self, chainlock: &ChainLock) -> bool {
        let engine = self.masternode_engine.read().await;

        match engine.verify_chain_lock(chainlock) {
            Ok(()) => {
                tracing::info!(
                    "ChainLock signature verified for height {}",
                    chainlock.block_height
                );
                true
            }
            Err(e) => {
                tracing::warn!(
                    "ChainLock signature verification failed for height {}: {}",
                    chainlock.block_height,
                    e
                );
                false
            }
        }
    }

    /// Re-evaluate every queued CLSig against the current header chain.
    ///
    /// Called when new headers land (`BlockHeadersStored`). Each entry's
    /// claimed block hash is re-checked:
    ///
    /// - `Match`: the entry is removed and the CLSig is fed back through
    ///   `process_chainlock`, which validates the signature and promotes
    ///   it to `best_chainlock` on success.
    /// - `Mismatch`: the entry is removed and re-processed through
    ///   `process_chainlock`. If the BLS signature validates, `ChainLockForcedReorg`
    ///   is emitted and the delivering peer is penalised if present. If it does
    ///   not validate, the entry is dropped and misbehavior is reported.
    /// - `Unknown`: the entry stays queued for a later `BlockHeadersStored`,
    ///   subject to TTL eviction in `tick`.
    pub(super) async fn retry_pending_unknown_hash(
        &mut self,
        requests: &RequestSender,
    ) -> SyncResult<Vec<SyncEvent>> {
        if self.pending_unknown_hash.is_empty() {
            return Ok(vec![]);
        }
        let mut events = Vec::new();
        let mut to_retry: Vec<(BlockHash, ChainLock, Option<SocketAddr>)> = Vec::new();
        for (hash, (cl, peer, _)) in &self.pending_unknown_hash {
            match self.verify_block_hash(cl).await {
                BlockHashVerification::Match
                | BlockHashVerification::Mismatch {
                    ..
                } => {
                    to_retry.push((*hash, cl.clone(), *peer));
                }
                BlockHashVerification::Unknown => {}
            }
        }
        for (hash, cl, peer) in to_retry {
            self.pending_unknown_hash.remove(&hash);
            // Re-run process_chainlock so the resolved branch is handled
            // through the canonical path: `Match` validates the signature,
            // `Mismatch` either reports misbehavior (bad sig) or emits
            // `ChainLockForcedReorg` (good sig).
            let mut produced = self.process_chainlock(&cl, peer, requests).await?;
            events.append(&mut produced);
        }
        Ok(events)
    }

    /// Drop entries from `pending_unknown_hash` whose age exceeds
    /// [`PENDING_UNKNOWN_HASH_TTL`]. Returns the number of evicted entries.
    pub(super) fn drain_expired_pending(&mut self) -> usize {
        let before = self.pending_unknown_hash.len();
        let now = Instant::now();
        self.pending_unknown_hash.retain(|_, (_, _, queued_at)| {
            now.duration_since(*queued_at) < PENDING_UNKNOWN_HASH_TTL
        });
        before - self.pending_unknown_hash.len()
    }

    /// Get the best validated ChainLock.
    pub fn best_chainlock(&self) -> Option<&ChainLock> {
        self.best_chainlock.as_ref()
    }

    /// Check if a block at the given height is chainlocked.
    /// All blocks at or below the best validated ChainLock height are considered locked.
    pub fn is_block_chainlocked(&self, height: u32) -> bool {
        self.best_chainlock.as_ref().map(|cl| height <= cl.block_height).unwrap_or(false)
    }
}

impl<H: BlockHeaderStorage, M: MetadataStorage> std::fmt::Debug for ChainLockManager<H, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainLockManager")
            .field("progress", &self.progress)
            .field("best_height", &self.best_chainlock.as_ref().map(|cl| cl.block_height))
            .field("masternode_ready", &self.masternode_ready)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::{MessageType, RequestSender};
    use crate::storage::{
        DiskStorageManager, PersistentBlockHeaderStorage, PersistentMetadataStorage, StorageManager,
    };
    use crate::sync::{ManagerIdentifier, SyncManager, SyncManagerProgress, SyncState};
    use crate::Network;
    use dashcore::bls_sig_utils::BLSSignature;
    use dashcore::hashes::Hash;
    use dashcore::BlockHash;
    use tokio::sync::mpsc::unbounded_channel;

    type TestChainLockManager =
        ChainLockManager<PersistentBlockHeaderStorage, PersistentMetadataStorage>;

    async fn create_test_manager() -> TestChainLockManager {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let engine =
            Arc::new(RwLock::new(MasternodeListEngine::default_for_network(Network::Testnet)));
        ChainLockManager::new(
            storage.block_headers(),
            storage.metadata(),
            engine,
            Arc::new(AtomicU32::new(0)),
        )
        .await
    }

    async fn create_test_manager_with_storage(
        storage: &DiskStorageManager,
    ) -> TestChainLockManager {
        let engine =
            Arc::new(RwLock::new(MasternodeListEngine::default_for_network(Network::Testnet)));
        ChainLockManager::new(
            storage.block_headers(),
            storage.metadata(),
            engine,
            Arc::new(AtomicU32::new(0)),
        )
        .await
    }

    fn create_test_chainlock(height: u32) -> ChainLock {
        ChainLock {
            block_height: height,
            block_hash: BlockHash::all_zeros(),
            signature: BLSSignature::from([0u8; 96]),
        }
    }

    /// Test helper: a stand-alone `RequestSender` whose receiver is
    /// discarded. Used by `process_chainlock` call sites that don't care
    /// about the misbehavior-reporting side-effect.
    fn test_requests() -> RequestSender {
        let (tx, _rx) = unbounded_channel();
        RequestSender::new(tx)
    }

    #[tokio::test]
    async fn test_chainlock_manager_new() {
        let manager = create_test_manager().await;
        assert_eq!(manager.identifier(), ManagerIdentifier::ChainLock);
        assert_eq!(manager.state(), SyncState::WaitForEvents);
        assert_eq!(manager.wanted_message_types(), vec![MessageType::CLSig, MessageType::Inv]);
    }

    /// Buffered `MasternodeStateUpdated` events delivered during
    /// `WaitingForConnections` must not force a `Synced` transition.
    /// `MasternodesManager` re-emits the event once it completes its next
    /// sync cycle after reconnect, so dropping it here is safe.
    #[tokio::test]
    async fn test_handle_sync_event_drops_masternode_state_updated_in_waiting_for_connections() {
        let mut manager = create_test_manager().await;
        manager.set_state(SyncState::WaitingForConnections);

        let event = SyncEvent::MasternodeStateUpdated {
            height: 100,
            qr_info_result: None,
        };
        let (tx, _rx) = unbounded_channel();
        let events = manager.handle_sync_event(&event, &RequestSender::new(tx)).await.unwrap();

        assert!(events.is_empty());
        assert_eq!(manager.state(), SyncState::WaitingForConnections);
        assert!(!manager.masternode_ready);
    }

    #[tokio::test]
    async fn test_chainlock_skips_validation_before_masternode_ready() {
        let mut manager = create_test_manager().await;

        // Before masternode sync, ChainLocks should not be validated
        let chainlock = create_test_chainlock(100);
        let events = manager.process_chainlock(&chainlock, None, &test_requests()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(manager.progress.valid(), 0);
        assert_eq!(manager.progress.invalid(), 0);
        assert!(manager.best_chainlock().is_none());
        // But the chainlock must be cached for retry once masternode
        // state arrives, rather than discarded.
        assert!(manager.pending_validation.is_some());
    }

    #[tokio::test]
    async fn test_pending_validation_keeps_highest() {
        let mut manager = create_test_manager().await;

        // Lower height first, then higher — pending_validation tracks
        // the highest seen pre-ready chainlock so the retry on
        // masternode-ready always validates the most recent.
        let _ = manager
            .process_chainlock(&create_test_chainlock(100), None, &test_requests())
            .await
            .unwrap();
        let _ = manager
            .process_chainlock(&create_test_chainlock(200), None, &test_requests())
            .await
            .unwrap();
        let _ = manager
            .process_chainlock(&create_test_chainlock(150), None, &test_requests())
            .await
            .unwrap();

        assert_eq!(manager.pending_validation.as_ref().map(|cl| cl.block_height), Some(200));
    }

    #[tokio::test]
    async fn test_on_masternode_ready_rejects_pending_chainlock_on_block_hash_mismatch() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let mut manager = create_test_manager_with_storage(&storage).await;

        // Cache a chainlock for height 100 BEFORE any header exists.
        // `process_chainlock`'s permissive `verify_block_hash` lets it
        // through and it lands in `pending_validation`.
        let _ = manager
            .process_chainlock(&create_test_chainlock(100), None, &test_requests())
            .await
            .unwrap();
        assert!(manager.pending_validation.is_some());

        // Header for height 100 resolves later with a hash that differs
        // from the cached chainlock's `BlockHash::all_zeros()`. The
        // readiness transition must re-check `verify_block_hash` and
        // drop the chainlock instead of moving the finality boundary.
        let header = dashcore::block::Header::dummy(100);
        storage
            .block_headers()
            .write()
            .await
            .store_headers_at_height(&[header], 100)
            .await
            .expect("store header at 100");

        let events = manager.on_masternode_ready().await;
        assert!(events.is_empty(), "mismatched chainlock must not be re-broadcast");
        assert!(manager.best_chainlock().is_none(), "mismatched chainlock must not be persisted");
        assert!(manager.pending_validation.is_none(), "pending_validation must be consumed");
        assert_eq!(manager.progress.invalid(), 1);
        assert_eq!(manager.progress.valid(), 0);
    }

    #[tokio::test]
    async fn test_on_masternode_ready_retries_pending_validation() {
        let mut manager = create_test_manager().await;
        let _ = manager
            .process_chainlock(&create_test_chainlock(100), None, &test_requests())
            .await
            .unwrap();
        assert!(manager.pending_validation.is_some());

        // With the default empty engine, validation fails — the
        // pending chainlock is consumed (cleared) and counted as
        // invalid; `best_chainlock` stays `None`.
        let events = manager.on_masternode_ready().await;
        assert!(events.is_empty());
        assert!(manager.pending_validation.is_none());
        assert!(manager.best_chainlock().is_none());
        assert_eq!(manager.progress.invalid(), 1);
        assert!(manager.masternode_ready);
    }

    #[tokio::test]
    async fn test_chainlock_validates_after_masternode_ready() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let mut manager = create_test_manager_with_storage(&storage).await;
        let _ = manager.on_masternode_ready().await;

        // Store a header at height 100 and a chainlock with the same hash
        // so `verify_block_hash` returns `Match` and the signature path runs
        // (empty engine fails validation, counts invalid).
        let header = dashcore::block::Header {
            version: dashcore::block::Version::ONE,
            prev_blockhash: BlockHash::all_zeros(),
            merkle_root: dashcore::TxMerkleNode::all_zeros(),
            time: 0,
            bits: dashcore::CompactTarget::from_consensus(0x207fffff),
            nonce: 0,
        };
        let chainlock = ChainLock {
            block_height: 100,
            block_hash: header.block_hash(),
            signature: BLSSignature::from([0u8; 96]),
        };
        storage
            .block_headers()
            .write()
            .await
            .store_headers_at_height(&[header], 100)
            .await
            .expect("store header at 100");

        let _ = manager.process_chainlock(&chainlock, None, &test_requests()).await.unwrap();

        assert_eq!(manager.progress.invalid(), 1);
        assert_eq!(manager.progress.valid(), 0);
    }

    #[tokio::test]
    async fn test_chainlock_keeps_only_best() {
        let mut manager = create_test_manager().await;

        // Manually set a best chainlock
        manager.best_chainlock = Some(create_test_chainlock(200));

        // Lower height should be ignored
        let chainlock_lower = create_test_chainlock(150);
        let events =
            manager.process_chainlock(&chainlock_lower, None, &test_requests()).await.unwrap();
        assert_eq!(events.len(), 0);

        // Equal height should also be ignored
        let chainlock_equal = create_test_chainlock(200);
        let events =
            manager.process_chainlock(&chainlock_equal, None, &test_requests()).await.unwrap();
        assert_eq!(events.len(), 0);

        // Higher height should be processed
        let chainlock_higher = create_test_chainlock(300);
        let events =
            manager.process_chainlock(&chainlock_higher, None, &test_requests()).await.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn test_chainlock_progress() {
        let mut manager = create_test_manager().await;
        manager.set_state(SyncState::Syncing);
        manager.progress.update_best_validated_height(500);
        manager.progress.add_valid(8);
        manager.progress.add_invalid(2);

        let progress = manager.progress();
        if let SyncManagerProgress::ChainLock(cp) = progress {
            assert_eq!(cp.state(), SyncState::Syncing);
            assert_eq!(cp.best_validated_height(), 500);
            assert_eq!(cp.valid(), 8);
            assert_eq!(cp.invalid(), 2);
        } else {
            panic!("Expected SyncManagerProgress::ChainLock");
        }
    }

    #[tokio::test]
    async fn test_is_block_chainlocked() {
        let mut manager = create_test_manager().await;

        // No ChainLock yet
        assert!(!manager.is_block_chainlocked(100));

        // Manually set best chainlock for testing
        manager.best_chainlock = Some(create_test_chainlock(500));

        // All blocks at or below 500 should be chainlocked
        assert!(manager.is_block_chainlocked(1));
        assert!(manager.is_block_chainlocked(500));
        assert!(!manager.is_block_chainlocked(501));
    }

    #[tokio::test]
    async fn test_load_from_empty_storage_returns_none() {
        let mut manager = create_test_manager().await;

        manager.load_best_chainlock().await;

        assert!(manager.best_chainlock().is_none());
        assert_eq!(manager.progress.best_validated_height(), 0);
    }

    #[tokio::test]
    async fn test_save_and_load_chainlock_round_trip() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let chainlock = create_test_chainlock(42000);

        // Save a chainlock via the first manager
        {
            let mut manager = create_test_manager_with_storage(&storage).await;
            manager.best_chainlock = Some(chainlock.clone());
            manager.save_best_chainlock().await;
        }

        // Fresh manager sharing the same storage should load the chainlock automatically
        {
            let manager = create_test_manager_with_storage(&storage).await;

            let restored = manager.best_chainlock().expect("chainlock should be restored");
            assert_eq!(restored.block_height, 42000);
            assert_eq!(restored.block_hash, chainlock.block_hash);
            assert_eq!(restored.signature, chainlock.signature);
            assert_eq!(manager.progress.best_validated_height(), 42000);
        }
    }

    #[tokio::test]
    async fn test_initialize_restores_persisted_chainlock() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let chainlock = create_test_chainlock(99999);

        // Persist a chainlock directly via metadata storage
        {
            let bytes = serde_json::to_vec(&chainlock).unwrap();
            let meta_storage = storage.metadata();
            let mut meta = meta_storage.write().await;
            meta.store_metadata(BEST_CHAINLOCK_KEY, &bytes).await.unwrap();
        }

        // Create a new manager and call initialize (the SyncManager trait method)
        let manager = create_test_manager_with_storage(&storage).await;

        let restored =
            manager.best_chainlock().expect("chainlock should be restored after initialize");
        assert_eq!(restored.block_height, 99999);
        assert_eq!(manager.progress.best_validated_height(), 99999);
        assert_eq!(manager.state(), SyncState::WaitForEvents);
    }

    #[tokio::test]
    async fn test_process_chainlock_persists_on_validation() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let mut manager = create_test_manager_with_storage(&storage).await;

        // Without masternode ready, chainlocks are not validated and not persisted
        let chainlock = create_test_chainlock(500);
        manager.process_chainlock(&chainlock, None, &test_requests()).await.unwrap();
        assert!(manager.best_chainlock().is_none());

        // Verify nothing was persisted
        {
            let meta_storage = storage.metadata();
            let meta = meta_storage.read().await;
            let loaded = meta.load_metadata(BEST_CHAINLOCK_KEY).await.unwrap();
            assert!(loaded.is_none());
        }
    }

    #[tokio::test]
    async fn test_save_overwrites_previous_chainlock() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();

        // Save first chainlock
        {
            let mut manager = create_test_manager_with_storage(&storage).await;
            manager.best_chainlock = Some(create_test_chainlock(100));
            manager.save_best_chainlock().await;
        }

        // Save a higher chainlock
        {
            let mut manager = create_test_manager_with_storage(&storage).await;
            manager.best_chainlock = Some(create_test_chainlock(200));
            manager.save_best_chainlock().await;
        }

        // Load and verify only the latest is stored
        {
            let mut manager = create_test_manager_with_storage(&storage).await;
            manager.load_best_chainlock().await;

            let restored = manager.best_chainlock().expect("chainlock should be restored");
            assert_eq!(restored.block_height, 200);
        }
    }

    /// `ChainReorg` hard-blocks CL validation by flipping `masternode_ready`
    /// back to `false` and dropping any `pending_validation`. After the
    /// cascade, an incoming chainlock must take the pre-ready path again
    /// (cached in `pending_validation`, not validated), waiting for the next
    /// `MasternodeStateUpdated` to retry.
    #[tokio::test]
    async fn test_chain_reorg_hard_blocks_chainlock_validation() {
        let mut manager = create_test_manager().await;
        let _ = manager.on_masternode_ready().await;
        manager.pending_validation = Some(create_test_chainlock(123));
        assert!(manager.masternode_ready);

        let (tx, _rx) = unbounded_channel();
        let requests = RequestSender::new(tx);
        let event = SyncEvent::ChainReorg {
            fork_height: 80,
            old_tip: BlockHash::all_zeros(),
            new_tip: BlockHash::all_zeros(),
            generation: 1,
        };
        manager.handle_sync_event(&event, &requests).await.expect("handle_sync_event succeeds");

        assert!(!manager.masternode_ready, "ChainReorg must flip masternode_ready back to false");
        assert!(manager.pending_validation.is_none(), "ChainReorg must drop pending_validation");

        let _ = manager
            .process_chainlock(&create_test_chainlock(150), None, &test_requests())
            .await
            .unwrap();
        assert!(manager.pending_validation.is_some());
        assert_eq!(manager.progress.valid(), 0);
        assert_eq!(manager.progress.invalid(), 0);
    }

    /// `reset_for_reorg` clears `pending_validation`, so a subsequent
    /// `on_masternode_ready` cannot re-validate a chainlock that arrived on the
    /// orphaned branch. This is the phase-ordering invariant documented on
    /// `reset_for_disconnect`: `ChainReorg → reset_for_reorg` must fire before
    /// `MasternodeStateUpdated → on_masternode_ready`.
    #[tokio::test]
    async fn test_reorg_prevents_stale_pending_validation_from_being_revalidated() {
        let mut manager = create_test_manager().await;

        let _ = manager
            .process_chainlock(&create_test_chainlock(100), None, &test_requests())
            .await
            .unwrap();
        assert!(manager.pending_validation.is_some());

        manager.reset_for_reorg();

        assert!(
            manager.pending_validation.is_none(),
            "reset_for_reorg must drop pending_validation from the orphaned branch"
        );
        assert!(!manager.masternode_ready);

        let events = manager.on_masternode_ready().await;
        assert!(
            events.is_empty(),
            "on_masternode_ready must not re-validate a stale chainlock cleared by reorg"
        );
        assert!(
            manager.masternode_ready,
            "on_masternode_ready must set masternode_ready=true even when pending_validation is None"
        );
        assert_eq!(
            manager.progress.valid(),
            0,
            "no chainlock from the orphaned branch was validated"
        );
        assert_eq!(
            manager.progress.invalid(),
            0,
            "dropped chainlock must not be counted as invalid"
        );
    }

    /// `pending_validation` must survive a disconnect so the cached chainlock
    /// can be re-evaluated after reconnect. `on_masternode_ready` must consume
    /// it on the next ready transition rather than silently discarding it.
    #[tokio::test]
    async fn test_pending_validation_survives_disconnect_and_consumed_on_reconnect() {
        let mut manager = create_test_manager().await;

        let _ = manager
            .process_chainlock(&create_test_chainlock(100), None, &test_requests())
            .await
            .unwrap();
        assert!(manager.pending_validation.is_some());
        assert!(!manager.masternode_ready);

        manager.reset_for_disconnect();

        assert!(
            manager.pending_validation.is_some(),
            "pending_validation must not be dropped on disconnect"
        );
        assert!(!manager.masternode_ready, "masternode_ready must be cleared on disconnect");

        let events = manager.on_masternode_ready().await;

        assert!(
            manager.pending_validation.is_none(),
            "on_masternode_ready must consume pending_validation"
        );
        assert!(manager.masternode_ready);
        // The empty engine cannot validate the signature, so the chainlock is
        // counted as invalid and best_chainlock stays None — events is empty.
        // The invariant is that pending_validation was consumed (not silently
        // dropped), which is covered by the is_none() assertion above.
        assert!(events.is_empty());
        assert_eq!(manager.progress.invalid(), 1);
    }

    #[tokio::test]
    async fn test_lower_chainlock_rejected_after_load() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();

        // Save chainlock at height 200
        {
            let mut manager = create_test_manager_with_storage(&storage).await;
            manager.best_chainlock = Some(create_test_chainlock(200));
            manager.save_best_chainlock().await;
        }

        // Load and try to process a lower chainlock
        {
            let mut manager = create_test_manager_with_storage(&storage).await;
            manager.load_best_chainlock().await;

            // Try to process a lower chainlock
            let lower_chainlock = create_test_chainlock(100);
            let events =
                manager.process_chainlock(&lower_chainlock, None, &test_requests()).await.unwrap();

            // Should be rejected (no events)
            assert_eq!(events.len(), 0);

            // Best should still be 200
            let best = manager.best_chainlock().expect("should have best chainlock");
            assert_eq!(best.block_height, 200);
        }
    }

    /// `verify_block_hash` returns `Match` when the local header at the
    /// chainlock's height equals the chainlock's claimed block hash.
    #[tokio::test]
    async fn test_verify_block_hash_match_branch() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let manager = create_test_manager_with_storage(&storage).await;

        let header = dashcore::block::Header::dummy(50);
        let hash = header.block_hash();
        storage
            .block_headers()
            .write()
            .await
            .store_headers_at_height(&[header], 50)
            .await
            .expect("store header at 50");

        let chainlock = ChainLock {
            block_height: 50,
            block_hash: hash,
            signature: BLSSignature::from([0u8; 96]),
        };

        assert_eq!(manager.verify_block_hash(&chainlock).await, BlockHashVerification::Match);
    }

    /// `verify_block_hash` returns `Unknown` when no header at the chainlock's
    /// height is stored locally.
    #[tokio::test]
    async fn test_verify_block_hash_unknown_branch() {
        let manager = create_test_manager().await;
        let chainlock = create_test_chainlock(100);

        assert_eq!(manager.verify_block_hash(&chainlock).await, BlockHashVerification::Unknown);
    }

    /// `verify_block_hash` returns `Mismatch` when the local header at the
    /// chainlock's height resolves to a different hash, carrying that hash so
    /// the caller can drive the forced-reorg path.
    #[tokio::test]
    async fn test_verify_block_hash_mismatch_branch() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let manager = create_test_manager_with_storage(&storage).await;

        let header = dashcore::block::Header::dummy(75);
        let local_hash = header.block_hash();
        storage
            .block_headers()
            .write()
            .await
            .store_headers_at_height(&[header], 75)
            .await
            .expect("store header at 75");

        let chainlock = ChainLock {
            block_height: 75,
            block_hash: BlockHash::from_byte_array([0xAA; 32]),
            signature: BLSSignature::from([0u8; 96]),
        };

        assert_eq!(
            manager.verify_block_hash(&chainlock).await,
            BlockHashVerification::Mismatch {
                local_header_hash: local_hash,
            }
        );
    }

    /// `process_chainlock` for an unknown-hash CLSig queues the entry and
    /// emits `PendingChainLockQueued` instead of validating immediately.
    /// The queued entry preserves the delivering peer for misbehavior
    /// reporting on a later mismatch.
    #[tokio::test]
    async fn test_process_chainlock_queues_unknown_hash() {
        let mut manager = create_test_manager().await;
        manager.masternode_ready = true;

        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();
        // No header at height 200 → `Unknown` branch.
        let chainlock = ChainLock {
            block_height: 200,
            block_hash: BlockHash::from_byte_array([0xCC; 32]),
            signature: BLSSignature::from([0u8; 96]),
        };
        let events =
            manager.process_chainlock(&chainlock, Some(peer), &test_requests()).await.unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], SyncEvent::PendingChainLockQueued { .. }));
        assert_eq!(manager.pending_unknown_hash.len(), 1);
        let (queued_cl, queued_peer, _queued_at) =
            manager.pending_unknown_hash.get(&chainlock.block_hash).unwrap();
        assert_eq!(queued_cl.block_height, 200);
        assert_eq!(*queued_peer, Some(peer));
    }

    /// `drain_expired_pending` removes entries past
    /// [`PENDING_UNKNOWN_HASH_TTL`] and keeps fresh ones.
    #[tokio::test]
    async fn test_drain_expired_pending_evicts_only_stale_entries() {
        let mut manager = create_test_manager().await;

        let fresh_hash = BlockHash::from_byte_array([0x01; 32]);
        let stale_hash = BlockHash::from_byte_array([0x02; 32]);
        manager.pending_unknown_hash.insert(
            fresh_hash,
            (
                ChainLock {
                    block_height: 1,
                    block_hash: fresh_hash,
                    signature: BLSSignature::from([0u8; 96]),
                },
                None,
                Instant::now(),
            ),
        );
        manager.pending_unknown_hash.insert(
            stale_hash,
            (
                ChainLock {
                    block_height: 2,
                    block_hash: stale_hash,
                    signature: BLSSignature::from([0u8; 96]),
                },
                None,
                Instant::now() - PENDING_UNKNOWN_HASH_TTL - Duration::from_secs(1),
            ),
        );

        let evicted = manager.drain_expired_pending();
        assert_eq!(evicted, 1);
        assert!(manager.pending_unknown_hash.contains_key(&fresh_hash));
        assert!(!manager.pending_unknown_hash.contains_key(&stale_hash));
    }

    /// `retry_pending_unknown_hash` promotes a queued CLSig whose claimed
    /// hash now matches a freshly-arrived header. The entry is removed from
    /// the queue and the signature path runs (empty engine fails the
    /// signature, so it is counted invalid).
    #[tokio::test]
    async fn test_retry_pending_unknown_hash_promotes_match_branch() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let mut manager = create_test_manager_with_storage(&storage).await;
        manager.masternode_ready = true;

        let header = dashcore::block::Header::dummy(123);
        let header_hash = header.block_hash();
        let chainlock = ChainLock {
            block_height: 123,
            block_hash: header_hash,
            signature: BLSSignature::from([0u8; 96]),
        };

        // Queue the chainlock first (header not stored yet → Unknown branch).
        let events = manager.process_chainlock(&chainlock, None, &test_requests()).await.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], SyncEvent::PendingChainLockQueued { .. }));
        assert_eq!(manager.pending_unknown_hash.len(), 1);

        // Store the matching header and retry.
        storage
            .block_headers()
            .write()
            .await
            .store_headers_at_height(&[header], 123)
            .await
            .expect("store header at 123");

        let retry_events = manager.retry_pending_unknown_hash(&test_requests()).await.unwrap();
        // process_chainlock returned the `ChainLockReceived` event for the
        // signature-validation outcome (empty engine fails, validated=false).
        assert_eq!(retry_events.len(), 1);
        assert!(matches!(
            retry_events[0],
            SyncEvent::ChainLockReceived {
                validated: false,
                ..
            }
        ));
        assert!(manager.pending_unknown_hash.is_empty(), "matched entry must be removed");
        assert_eq!(manager.progress.invalid(), 1);
    }

    /// `retry_pending_unknown_hash` removes a queued CLSig that resolves to a
    /// `Mismatch` and re-routes through `process_chainlock`. With an invalid BLS
    /// signature (empty engine), the entry is dropped and counted as invalid.
    #[tokio::test]
    async fn test_retry_pending_unknown_hash_mismatch_invalid_sig_drops_entry() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let mut manager = create_test_manager_with_storage(&storage).await;
        manager.masternode_ready = true;

        let header = dashcore::block::Header::dummy(200);
        let chainlock_hash = BlockHash::from_byte_array([0xDE; 32]);
        let chainlock = ChainLock {
            block_height: 200,
            block_hash: chainlock_hash,
            signature: BLSSignature::from([0u8; 96]),
        };

        let _ = manager.process_chainlock(&chainlock, None, &test_requests()).await.unwrap();
        assert_eq!(manager.pending_unknown_hash.len(), 1);

        // Store a header at 200 whose hash differs from `chainlock_hash` →
        // `Mismatch`. The empty engine rejects the BLS signature → drop.
        storage
            .block_headers()
            .write()
            .await
            .store_headers_at_height(&[header], 200)
            .await
            .expect("store header at 200");

        let events = manager.retry_pending_unknown_hash(&test_requests()).await.unwrap();
        assert!(
            manager.pending_unknown_hash.is_empty(),
            "mismatched entry must be removed from the queue"
        );
        assert!(events.is_empty(), "invalid-sig mismatch must not emit ChainLockForcedReorg");
        assert_eq!(manager.progress.invalid(), 1);
    }

    /// `Mismatch` with an invalid signature reports peer misbehavior and
    /// does not emit `ChainLockForcedReorg`. The receiver of the underlying
    /// request channel sees a `ReportMisbehavior` request for the delivering
    /// peer with `InvalidChainLockSignature`.
    #[tokio::test]
    async fn test_process_chainlock_mismatch_invalid_signature_reports_misbehavior() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let mut manager = create_test_manager_with_storage(&storage).await;
        manager.masternode_ready = true;

        // Store a header at height 50 that differs from the chainlock's hash
        // so `verify_block_hash` returns `Mismatch`.
        let header = dashcore::block::Header::dummy(50);
        storage
            .block_headers()
            .write()
            .await
            .store_headers_at_height(&[header], 50)
            .await
            .expect("store header at 50");
        let chainlock = ChainLock {
            block_height: 50,
            block_hash: BlockHash::from_byte_array([0xFE; 32]),
            signature: BLSSignature::from([0u8; 96]),
        };
        let peer: SocketAddr = "1.2.3.4:9999".parse().unwrap();

        let (tx, mut rx) = unbounded_channel();
        let requests = RequestSender::new(tx);

        let events = manager.process_chainlock(&chainlock, Some(peer), &requests).await.unwrap();
        assert!(
            events.is_empty(),
            "invalid signature must not emit ChainLockForcedReorg, got {:?}",
            events
        );
        let req = rx.try_recv().expect("misbehavior report must be queued");
        match req {
            crate::network::NetworkRequest::ReportMisbehavior(reported_peer, kind) => {
                assert_eq!(reported_peer, peer);
                assert_eq!(kind, MisbehaviorKind::InvalidChainLockSignature);
            }
            other => panic!("expected ReportMisbehavior, got {:?}", other),
        }
        assert_eq!(manager.progress.invalid(), 1);
    }
}
