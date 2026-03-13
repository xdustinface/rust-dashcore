//! UniFFI bridge module for dash-spv.
//!
//! Provides callback traits and UniFFI-compatible event record types for
//! bridging the SPV client to foreign (e.g. React Native / Swift) code.
//!
//! Compiled only when the `uniffi` feature is enabled.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use dashcore::Network;
use key_wallet::wallet::managed_wallet_info::ManagedWalletInfo;
use key_wallet_manager::wallet_manager::WalletManager;
use tokio::sync::RwLock;

use crate::client::{ClientConfig, DashSpvClient};
use crate::error::SpvError;
use crate::network::PeerNetworkManager;
use crate::storage::DiskStorageManager;
use crate::sync::{ProgressPercentage, SyncProgress, SyncState};

// ============ custom_type! mappings ============

uniffi::custom_type!(Network, String, {
    remote,
    lower: |n| n.to_string(),
    try_lift: |s| s.parse().map_err(|e: String| uniffi::deps::anyhow::anyhow!(e)),
});

uniffi::custom_type!(SocketAddr, String, {
    remote,
    lower: |a| a.to_string(),
    try_lift: |s| s.parse::<SocketAddr>().map_err(|e| uniffi::deps::anyhow::anyhow!(e)),
});

uniffi::custom_type!(PathBuf, String, {
    remote,
    lower: |p| p.to_string_lossy().into_owned(),
    try_lift: |s| Ok::<PathBuf, uniffi::deps::anyhow::Error>(PathBuf::from(s)),
});

// ============ Event types ============

/// UniFFI-compatible representation of a sync event.
///
/// This is a flattened version of the internal [`crate::sync::SyncEvent`] that
/// uses only types expressible across the UniFFI boundary.  Complex fields
/// (e.g. `BlockHash`, `Address`, `ChainLock`) are represented as `String` or
/// decomposed into primitive fields.
#[derive(uniffi::Enum, Clone, Debug)]
pub enum SyncEvent {
    /// A sync manager has started a sync operation.
    SyncStart {
        /// Display name of the manager that started syncing.
        identifier: String,
    },

    /// New block headers have been stored.
    BlockHeadersStored {
        /// New chain-tip height after storage.
        tip_height: u32,
    },

    /// Block headers have reached the chain tip (initial header sync complete).
    BlockHeaderSyncComplete {
        /// Tip height when sync completed.
        tip_height: u32,
    },

    /// New compact-filter headers have been stored.
    FilterHeadersStored {
        /// Lowest height stored in this batch.
        start_height: u32,
        /// Highest height stored in this batch.
        end_height: u32,
        /// New tip height after storage.
        tip_height: u32,
    },

    /// Filter headers have reached the chain tip.
    FilterHeadersSyncComplete {
        /// Tip height when sync completed.
        tip_height: u32,
    },

    /// Compact block filters have been stored and are ready for matching.
    FiltersStored {
        /// Lowest height stored.
        start_height: u32,
        /// Highest height stored.
        end_height: u32,
    },

    /// Filter sync has reached the chain tip (all filters processed).
    FiltersSyncComplete {
        /// Tip height when sync completed.
        tip_height: u32,
    },

    /// Filters matched the wallet; blocks need downloading.
    BlocksNeeded {
        /// Number of blocks that need to be downloaded.
        block_count: u32,
    },

    /// A block was downloaded and processed through the wallet.
    BlockProcessed {
        /// Hex-encoded hash of the processed block.
        block_hash: String,
        /// Height of the processed block.
        height: u32,
        /// Number of new addresses derived from gap-limit maintenance.
        new_address_count: u32,
    },

    /// Masternode state has been updated to a new height.
    MasternodeStateUpdated {
        /// New masternode-state height.
        height: u32,
    },

    /// A sync manager encountered a recoverable error.
    ManagerError {
        /// Display name of the manager that encountered the error.
        manager: String,
        /// Human-readable error description.
        error: String,
    },

    /// A ChainLock was received and processed.
    ChainLockReceived {
        /// Block height covered by this ChainLock.
        block_height: u32,
        /// Whether the BLS signature was successfully validated.
        validated: bool,
    },

    /// An InstantSend lock was received and processed.
    InstantLockReceived {
        /// Hex-encoded transaction ID covered by this InstantLock.
        txid: String,
        /// Whether the BLS signature was successfully validated.
        validated: bool,
    },

    /// All sync managers have reached the chain tip.
    SyncComplete {
        /// Final header tip height.
        header_tip: u32,
        /// Sync cycle (0 = initial sync, 1+ = incremental).
        cycle: u32,
    },
}

/// UniFFI-compatible representation of a network event.
///
/// This is a flattened version of the internal [`crate::network::NetworkEvent`]
/// that uses only types expressible across the UniFFI boundary.  `SocketAddr`
/// values are serialised as `"<ip>:<port>"` strings.
#[derive(uniffi::Enum, Clone, Debug)]
pub enum NetworkEvent {
    /// A peer has connected.
    PeerConnected {
        /// Socket address of the connected peer, e.g. `"192.0.2.1:9999"`.
        address: String,
    },

    /// A peer has disconnected.
    PeerDisconnected {
        /// Socket address of the disconnected peer.
        address: String,
    },

    /// Summary of the peer pool emitted after every connect / disconnect.
    PeersUpdated {
        /// Number of currently connected peers.
        connected_count: u64,
        /// Socket addresses of all connected peers.
        addresses: Vec<String>,
        /// Best chain height reported by connected peers, if known.
        best_height: Option<u32>,
    },
}

// ============ Network info types ============

/// UniFFI-compatible record describing a single connected peer.
#[derive(uniffi::Record, Clone, Debug)]
pub struct PeerInfo {
    /// Socket address of the peer, e.g. `"192.0.2.1:9999"`.
    pub address: String,
    /// User-agent string reported by the peer.
    pub user_agent: String,
    /// Best block height reported by the peer.
    pub best_height: u32,
    /// Unix timestamp (seconds) of when the peer connected.
    pub connected_since: u64,
    /// Services bitmask advertised by the peer.
    pub services: u64,
}

/// UniFFI-compatible record describing the current network state.
#[derive(uniffi::Record, Clone, Debug)]
pub struct NetworkInfo {
    /// Network name (e.g. `"mainnet"`, `"testnet"`, `"regtest"`).
    pub network: String,
    /// Number of currently connected peers.
    pub peer_count: u32,
    /// Details for each connected peer.
    ///
    /// TODO: populate with real peer data from `PeerNetworkManager`.
    pub peers: Vec<PeerInfo>,
}

/// Callback interface for receiving SPV client events on the foreign side.
///
/// Implement this trait in React Native / Swift and register it via
/// `SpvClient::subscribe`.  The SPV client spawns a background tokio task that
/// reads from its internal broadcast channels and calls these methods.
///
/// All methods are called from a background thread; implementations must be
/// thread-safe (`Send + Sync`).
#[uniffi::export(with_foreign)]
pub trait SpvEventListener: Send + Sync {
    /// Called whenever a sync event occurs (header stored, sync complete, etc.).
    fn on_sync_event(&self, event: SyncEvent);

    /// Called whenever a network event occurs (peer connected / disconnected).
    fn on_network_event(&self, event: NetworkEvent);

    /// Called when overall sync progress changes.
    ///
    /// * `percentage`     – completion ratio in `[0.0, 1.0]`
    /// * `current_height` – current chain-tip height
    /// * `target_height`  – estimated target height (best peer height)
    fn on_sync_progress(&self, percentage: f64, current_height: u32, target_height: u32);
}

// ============ Sync progress types ============

/// Per-phase progress snapshot exposed over the UniFFI boundary.
#[derive(uniffi::Record, Clone, Debug, PartialEq)]
pub struct PhaseProgress {
    /// Current block height or item count for this phase.
    pub current: u32,
    /// Target block height or item count for this phase.
    pub target: u32,
    /// Completion ratio in `[0.0, 1.0]`.
    pub percentage: f64,
}

impl PhaseProgress {
    fn zero() -> Self {
        Self {
            current: 0,
            target: 0,
            percentage: 0.0,
        }
    }
}

/// Full sync progress snapshot for all phases, exposed over the UniFFI boundary.
#[derive(uniffi::Record, Clone, Debug)]
pub struct SyncProgressInfo {
    /// Overall sync state: `"WaitForEvents"`, `"WaitingForConnections"`, `"Syncing"`, `"Synced"`, or `"Error"`.
    pub state: String,
    /// Whether all sync phases have completed successfully.
    pub is_synced: bool,
    /// Overall completion ratio in `[0.0, 1.0]`.
    pub overall_percentage: f64,
    /// Block header synchronization progress.
    pub headers: PhaseProgress,
    /// Compact filter-header synchronization progress.
    pub filter_headers: PhaseProgress,
    /// Compact filter synchronization progress.
    pub filters: PhaseProgress,
    /// Block download and wallet-processing progress.
    pub blocks: PhaseProgress,
    /// Masternode list synchronization progress.
    pub masternodes: PhaseProgress,
}

impl From<&SyncProgress> for SyncProgressInfo {
    fn from(p: &SyncProgress) -> Self {
        let headers = p
            .headers()
            .map(|h| PhaseProgress {
                current: h.current_height(),
                target: h.target_height(),
                percentage: h.percentage(),
            })
            .unwrap_or_else(|_| PhaseProgress::zero());

        let filter_headers = p
            .filter_headers()
            .map(|fh| PhaseProgress {
                current: fh.current_height(),
                target: fh.target_height(),
                percentage: fh.percentage(),
            })
            .unwrap_or_else(|_| PhaseProgress::zero());

        let filters = p
            .filters()
            .map(|f| PhaseProgress {
                current: f.current_height(),
                target: f.target_height(),
                percentage: f.percentage(),
            })
            .unwrap_or_else(|_| PhaseProgress::zero());

        let blocks = p
            .blocks()
            .map(|b| {
                let current = b.processed();
                let target = b.requested();
                let percentage = if target > 0 {
                    (current as f64 / target as f64).min(1.0)
                } else {
                    0.0
                };
                PhaseProgress {
                    current,
                    target,
                    percentage,
                }
            })
            .unwrap_or_else(|_| PhaseProgress::zero());

        let masternodes = p
            .masternodes()
            .map(|m| {
                let current = m.current_height();
                let target = m.target_height();
                let percentage = if target > 0 {
                    (current as f64 / target as f64).min(1.0)
                } else {
                    0.0
                };
                PhaseProgress {
                    current,
                    target,
                    percentage,
                }
            })
            .unwrap_or_else(|_| PhaseProgress::zero());

        SyncProgressInfo {
            state: format!("{:?}", p.state()),
            is_synced: p.is_synced(),
            overall_percentage: p.percentage(),
            headers,
            filter_headers,
            filters,
            blocks,
            masternodes,
        }
    }
}

// ============ Error type ============

/// Error type for the UniFFI SpvClient wrapper.
#[derive(Debug, uniffi::Error, thiserror::Error)]
pub enum SpvClientError {
    #[error("Configuration error: {message}")]
    Config {
        message: String,
    },
    #[error("Network error: {message}")]
    Network {
        message: String,
    },
    #[error("Storage error: {message}")]
    Storage {
        message: String,
    },
    #[error("Sync error: {message}")]
    Sync {
        message: String,
    },
    #[error("General error: {message}")]
    General {
        message: String,
    },
}

impl From<SpvError> for SpvClientError {
    fn from(err: SpvError) -> Self {
        match err {
            SpvError::Config(msg) => SpvClientError::Config {
                message: msg,
            },
            SpvError::Network(e) => SpvClientError::Network {
                message: e.to_string(),
            },
            SpvError::Storage(e) => SpvClientError::Storage {
                message: e.to_string(),
            },
            SpvError::Sync(e) => SpvClientError::Sync {
                message: e.to_string(),
            },
            other => SpvClientError::General {
                message: other.to_string(),
            },
        }
    }
}

// ============ Wallet record types ============

/// UniFFI-compatible wallet balance record.
///
/// All amounts are in duffs (1 DASH = 100,000,000 duffs).
#[derive(uniffi::Record, Clone, Debug, PartialEq)]
pub struct WalletBalance {
    /// Confirmed spendable balance, in duffs.
    pub confirmed: u64,
    /// Unconfirmed (pending) balance, in duffs.
    pub unconfirmed: u64,
    /// Immature coinbase balance not yet spendable, in duffs.
    pub immature: u64,
}

/// UniFFI-compatible transaction summary record.
#[derive(uniffi::Record, Clone, Debug, PartialEq)]
pub struct TransactionInfo {
    /// Transaction ID as a hex string.
    pub txid: String,
    /// Net amount in duffs — positive for incoming, negative for outgoing.
    pub amount: i64,
    /// Fee paid in duffs.
    pub fee: u64,
    /// Number of confirmations (0 = unconfirmed).
    pub confirmations: u32,
    /// Unix timestamp of when the transaction was first seen.
    pub timestamp: u64,
    /// `true` if the transaction added funds to this wallet.
    pub is_incoming: bool,
}

// ============ Concrete type alias ============

type ConcreteClient =
    DashSpvClient<WalletManager<ManagedWalletInfo>, PeerNetworkManager, DiskStorageManager>;

// ============ SpvClient wrapper ============

/// Concrete UniFFI-compatible wrapper for the Dash SPV client.
///
/// `DashSpvClient` is generic and cannot be exported via UniFFI directly.
/// This wrapper fixes the type parameters to the standard production
/// implementations and exposes lifecycle and state-query methods.
#[derive(uniffi::Object)]
pub struct SpvClient {
    inner: ConcreteClient,
}

#[uniffi::export]
impl SpvClient {
    /// Create a new `SpvClient` from the given configuration.
    ///
    /// Constructs the network manager, storage manager, and wallet, then
    /// hands them to `DashSpvClient::new`.
    #[uniffi::constructor]
    pub async fn new(config: ClientConfig) -> Result<Arc<Self>, SpvClientError> {
        let network = PeerNetworkManager::new(&config).await.map_err(SpvClientError::from)?;
        let storage =
            DiskStorageManager::new(&config).await.map_err(|e| SpvClientError::Storage {
                message: e.to_string(),
            })?;
        let wallet = Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(config.network)));

        let inner = DashSpvClient::new(config, network, storage, wallet)
            .await
            .map_err(SpvClientError::from)?;

        Ok(Arc::new(Self {
            inner,
        }))
    }

    /// Start the client — connect to the network and begin syncing.
    pub async fn start(&self) -> Result<(), SpvClientError> {
        self.inner.start().await.map_err(SpvClientError::from)
    }

    /// Stop the client — disconnect from the network and flush storage.
    pub async fn stop(&self) -> Result<(), SpvClientError> {
        self.inner.stop().await.map_err(SpvClientError::from)
    }

    /// Shutdown the client (alias for `stop`).
    pub async fn shutdown(&self) -> Result<(), SpvClientError> {
        self.inner.shutdown().await.map_err(SpvClientError::from)
    }

    /// Returns `true` if the client is currently running.
    pub async fn is_running(&self) -> bool {
        self.inner.is_running().await
    }

    /// Returns the current chain tip height (0 if no headers yet).
    pub async fn tip_height(&self) -> u32 {
        self.inner.tip_height().await
    }

    /// Returns the current chain tip hash as a hex string, or `None` if unavailable.
    pub async fn tip_hash(&self) -> Option<String> {
        self.inner.tip_hash().await.map(|h| h.to_string())
    }

    /// Returns the number of connected peers.
    pub async fn peer_count(&self) -> u64 {
        self.inner.peer_count().await as u64
    }

    /// Returns the overall sync completion percentage in the range `[0.0, 1.0]`.
    pub async fn sync_progress(&self) -> f64 {
        self.inner.sync_progress().await.percentage()
    }

    /// Returns a detailed snapshot of sync progress for all phases.
    pub async fn get_sync_progress(&self) -> SyncProgressInfo {
        SyncProgressInfo::from(&self.inner.sync_progress().await)
    }

    /// Returns `true` when the client is actively downloading and processing blocks.
    pub async fn is_syncing(&self) -> bool {
        matches!(self.inner.sync_progress().await.state(), SyncState::Syncing)
    }

    /// Returns the wallet balance.
    ///
    /// TODO: delegate to `self.inner.wallet()` once wallet integration is complete.
    pub async fn get_balance(&self) -> WalletBalance {
        WalletBalance {
            confirmed: 0,
            unconfirmed: 0,
            immature: 0,
        }
    }

    /// Returns network and peer information.
    ///
    /// Currently returns a stub with the network name, peer count, and an empty
    /// peer list.  Peer details will be wired up once `PeerNetworkManager`
    /// exposes per-peer metadata.
    ///
    /// TODO: populate `peers` with real data from `PeerNetworkManager`.
    pub async fn get_network_info(&self) -> NetworkInfo {
        let network = self.inner.network().await;
        let peer_count = self.inner.peer_count().await as u32;
        NetworkInfo {
            network: network.to_string(),
            peer_count,
            peers: vec![], // TODO: populate with real peer data
        }
    }
}

// ============ Masternode and Governance types ============

/// UniFFI-compatible record representing a single masternode entry.
///
/// Fields are mapped from `MasternodeListEntry` internals. All hashes and
/// addresses are represented as `String` values for cross-language convenience.
#[derive(uniffi::Record, Clone, Debug)]
pub struct MasternodeInfo {
    /// ProRegTx hash that uniquely identifies this masternode.
    pub pro_tx_hash: String,
    /// Service address of the masternode (IP:port).
    pub address: String,
    /// Status of the masternode (e.g. "Enabled", "PoSeBanned").
    pub status: String,
    /// Proof-of-Service penalty score.
    pub pose_penalty: u32,
    /// Height at which this masternode was last paid.
    pub last_paid_height: u32,
    /// Block height at which the masternode was registered.
    pub registered_height: u32,
}

/// UniFFI-compatible record representing a governance proposal.
///
/// These fields are stubs — governance sync is not yet implemented. The type
/// is exported so foreign-language bindings can be generated in advance.
#[derive(uniffi::Record, Clone, Debug)]
pub struct GovernanceProposal {
    /// Hash of the governance proposal object.
    pub hash: String,
    /// Human-readable name of the proposal.
    pub name: String,
    /// URL linking to the proposal details.
    pub url: String,
    /// Dash address that will receive the payment if the proposal passes.
    pub payment_address: String,
    /// Requested payment amount in duffs.
    pub payment_amount: u64,
    /// Number of "yes" votes cast for this proposal.
    pub yes_count: u32,
    /// Number of "no" votes cast against this proposal.
    pub no_count: u32,
    /// Number of "abstain" votes cast for this proposal.
    pub abstain_count: u32,
}

#[uniffi::export]
impl SpvClient {
    /// Returns the number of masternodes in the current masternode list.
    ///
    /// TODO: wire up to `MasternodeListEngine` once the engine is accessible
    /// from `DashSpvClient`.
    pub async fn get_masternode_count(&self) -> u32 {
        // TODO: return self.inner.masternode_list_engine()?.read().await.count()
        0
    }

    /// Returns all masternodes from the current masternode list.
    ///
    /// TODO: wire up to `MasternodeListEngine` once the engine is accessible
    /// from `DashSpvClient`.
    pub async fn get_masternodes(&self) -> Vec<MasternodeInfo> {
        // TODO: map MasternodeListEntry fields to MasternodeInfo
        vec![]
    }
}

// ============ Stub functions ============

/// Returns a greeting string (sanity-check export).
#[uniffi::export]
pub fn hello() -> String {
    "Hello from dash-spv!".to_string()
}

/// Returns the library version string.
#[uniffi::export]
pub async fn get_version() -> String {
    crate::VERSION.to_string()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_hello() {
        assert_eq!(hello(), "Hello from dash-spv!");
    }

    #[tokio::test]
    async fn test_get_version() {
        let version = get_version().await;
        assert!(!version.is_empty(), "version should not be empty");
        assert_eq!(version, crate::VERSION);
    }

    struct MockListener {
        sync_events: Mutex<Vec<SyncEvent>>,
        network_events: Mutex<Vec<NetworkEvent>>,
        progress_events: Mutex<Vec<(f64, u32, u32)>>,
    }

    impl MockListener {
        fn new() -> Self {
            Self {
                sync_events: Mutex::new(Vec::new()),
                network_events: Mutex::new(Vec::new()),
                progress_events: Mutex::new(Vec::new()),
            }
        }
    }

    impl SpvEventListener for MockListener {
        fn on_sync_event(&self, event: SyncEvent) {
            self.sync_events.lock().unwrap().push(event);
        }

        fn on_network_event(&self, event: NetworkEvent) {
            self.network_events.lock().unwrap().push(event);
        }

        fn on_sync_progress(&self, percentage: f64, current_height: u32, target_height: u32) {
            self.progress_events.lock().unwrap().push((percentage, current_height, target_height));
        }
    }

    #[test]
    fn test_listener_receives_sync_event() {
        let listener = MockListener::new();
        listener.on_sync_event(SyncEvent::SyncComplete {
            header_tip: 100,
            cycle: 0,
        });
        let events = listener.sync_events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            SyncEvent::SyncComplete {
                header_tip: 100,
                cycle: 0
            }
        ));
    }

    #[test]
    fn test_listener_receives_network_event() {
        let listener = MockListener::new();
        listener.on_network_event(NetworkEvent::PeerConnected {
            address: "127.0.0.1:9999".to_string(),
        });
        let events = listener.network_events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], NetworkEvent::PeerConnected { .. }));
    }

    #[test]
    fn test_listener_receives_progress() {
        let listener = MockListener::new();
        listener.on_sync_progress(0.5, 500, 1000);
        let events = listener.progress_events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], (0.5, 500, 1000));
    }

    #[test]
    fn test_sync_event_variants() {
        let events: Vec<SyncEvent> = vec![
            SyncEvent::SyncStart {
                identifier: "BlockHeader".to_string(),
            },
            SyncEvent::BlockHeadersStored {
                tip_height: 1000,
            },
            SyncEvent::BlockHeaderSyncComplete {
                tip_height: 1000,
            },
            SyncEvent::FilterHeadersStored {
                start_height: 0,
                end_height: 999,
                tip_height: 1000,
            },
            SyncEvent::FilterHeadersSyncComplete {
                tip_height: 1000,
            },
            SyncEvent::FiltersStored {
                start_height: 0,
                end_height: 999,
            },
            SyncEvent::FiltersSyncComplete {
                tip_height: 1000,
            },
            SyncEvent::BlocksNeeded {
                block_count: 5,
            },
            SyncEvent::BlockProcessed {
                block_hash: "deadbeef".to_string(),
                height: 500,
                new_address_count: 2,
            },
            SyncEvent::MasternodeStateUpdated {
                height: 1000,
            },
            SyncEvent::ManagerError {
                manager: "Filter".to_string(),
                error: "timeout".to_string(),
            },
            SyncEvent::ChainLockReceived {
                block_height: 1000,
                validated: true,
            },
            SyncEvent::InstantLockReceived {
                txid: "abcd1234".to_string(),
                validated: false,
            },
            SyncEvent::SyncComplete {
                header_tip: 1000,
                cycle: 0,
            },
        ];
        let _cloned: Vec<SyncEvent> = events.to_vec();
        assert_eq!(events.len(), 14);
    }

    #[test]
    fn test_network_event_variants() {
        let events: Vec<NetworkEvent> = vec![
            NetworkEvent::PeerConnected {
                address: "127.0.0.1:9999".to_string(),
            },
            NetworkEvent::PeerDisconnected {
                address: "127.0.0.1:9999".to_string(),
            },
            NetworkEvent::PeersUpdated {
                connected_count: 3,
                addresses: vec!["127.0.0.1:9999".to_string()],
                best_height: Some(1000),
            },
            NetworkEvent::PeersUpdated {
                connected_count: 0,
                addresses: vec![],
                best_height: None,
            },
        ];
        let _cloned: Vec<NetworkEvent> = events.to_vec();
        assert_eq!(events.len(), 4);
    }

    /// Verify that `SpvClient` can be constructed from a minimal regtest config.
    #[tokio::test]
    async fn test_spv_client_construction() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = ClientConfig::regtest()
            .without_filters()
            .without_masternodes()
            .with_storage_path(temp_dir.path());

        let client = SpvClient::new(config).await;
        assert!(client.is_ok(), "SpvClient construction should succeed");

        let client = client.unwrap();
        assert!(!client.is_running().await, "Client should not be running after construction");
        assert_eq!(client.tip_height().await, 0, "Tip height should start at 0 (genesis)");
        assert_eq!(client.peer_count().await, 0, "Peer count should be 0 before start");
    }

    /// Verify that `sync_progress` and `is_syncing` return sensible defaults.
    #[tokio::test]
    async fn test_spv_client_state_queries() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = ClientConfig::regtest()
            .without_filters()
            .without_masternodes()
            .with_storage_path(temp_dir.path());

        let client = SpvClient::new(config).await.expect("SpvClient construction must succeed");

        let progress = client.sync_progress().await;
        assert!(
            (0.0..=1.0).contains(&progress),
            "sync_progress should be in [0.0, 1.0], got {progress}"
        );

        assert!(!client.is_syncing().await, "Client should not be syncing before start()");
    }

    // ============ PhaseProgress tests ============

    #[test]
    fn test_phase_progress_zero() {
        let p = PhaseProgress::zero();
        assert_eq!(p.current, 0);
        assert_eq!(p.target, 0);
        assert_eq!(p.percentage, 0.0);
    }

    #[test]
    fn test_phase_progress_fields() {
        let p = PhaseProgress {
            current: 500,
            target: 1000,
            percentage: 0.5,
        };
        assert_eq!(p.current, 500);
        assert_eq!(p.target, 1000);
        assert_eq!(p.percentage, 0.5);
    }

    // ============ SyncProgressInfo mapping tests ============

    #[test]
    fn test_sync_progress_info_default_sync_progress() {
        use crate::sync::SyncProgress;

        let progress = SyncProgress::default();
        let info = SyncProgressInfo::from(&progress);

        assert_eq!(info.state, "WaitForEvents");
        assert!(!info.is_synced);
        assert_eq!(info.overall_percentage, 0.0);

        // All phases should be zero when no managers have started.
        assert_eq!(info.headers, PhaseProgress::zero());
        assert_eq!(info.filter_headers, PhaseProgress::zero());
        assert_eq!(info.filters, PhaseProgress::zero());
        assert_eq!(info.blocks, PhaseProgress::zero());
        assert_eq!(info.masternodes, PhaseProgress::zero());
    }

    #[test]
    fn test_sync_progress_info_state_strings() {
        use crate::sync::{BlockHeadersProgress, SyncProgress, SyncState};

        // Build a SyncProgress with a headers entry in the Syncing state so the
        // aggregate state is Syncing.
        let mut headers = BlockHeadersProgress::default();
        headers.set_state(SyncState::Syncing);
        headers.update_target_height(1000);
        headers.update_tip_height(500);

        let mut progress = SyncProgress::default();
        progress.update_headers(headers);

        let info = SyncProgressInfo::from(&progress);
        assert_eq!(info.state, "Syncing");
        assert!(!info.is_synced);
    }

    #[test]
    fn test_sync_progress_info_headers_phase() {
        use crate::sync::{BlockHeadersProgress, SyncProgress, SyncState};

        let mut headers = BlockHeadersProgress::default();
        headers.set_state(SyncState::Syncing);
        headers.update_target_height(1000);
        headers.update_tip_height(750);

        let mut progress = SyncProgress::default();
        progress.update_headers(headers);

        let info = SyncProgressInfo::from(&progress);
        assert_eq!(info.headers.current, 750);
        assert_eq!(info.headers.target, 1000);
        assert!(
            (info.headers.percentage - 0.75).abs() < 1e-9,
            "expected 0.75, got {}",
            info.headers.percentage
        );
    }

    #[test]
    fn test_sync_progress_info_blocks_phase() {
        use crate::sync::{BlocksProgress, SyncProgress};

        let mut blocks = BlocksProgress::default();
        blocks.add_requested(100);
        blocks.add_processed(60);

        let mut progress = SyncProgress::default();
        progress.update_blocks(blocks);

        let info = SyncProgressInfo::from(&progress);
        assert_eq!(info.blocks.current, 60);
        assert_eq!(info.blocks.target, 100);
        assert!(
            (info.blocks.percentage - 0.6).abs() < 1e-9,
            "expected 0.6, got {}",
            info.blocks.percentage
        );
    }

    #[test]
    fn test_sync_progress_info_blocks_zero_requested() {
        use crate::sync::{BlocksProgress, SyncProgress};

        let blocks = BlocksProgress::default(); // requested = 0
        let mut progress = SyncProgress::default();
        progress.update_blocks(blocks);

        let info = SyncProgressInfo::from(&progress);
        assert_eq!(info.blocks.percentage, 0.0);
    }

    #[test]
    fn test_sync_progress_info_masternodes_phase() {
        use crate::sync::{MasternodesProgress, SyncProgress};

        let mut masternodes = MasternodesProgress::default();
        masternodes.update_target_height(2000);
        masternodes.update_current_height(1000);

        let mut progress = SyncProgress::default();
        progress.update_masternodes(masternodes);

        let info = SyncProgressInfo::from(&progress);
        assert_eq!(info.masternodes.current, 1000);
        assert_eq!(info.masternodes.target, 2000);
        assert!(
            (info.masternodes.percentage - 0.5).abs() < 1e-9,
            "expected 0.5, got {}",
            info.masternodes.percentage
        );
    }

    #[test]
    fn test_sync_progress_info_percentage_clamped() {
        use crate::sync::{MasternodesProgress, SyncProgress};

        // current > target should clamp to 1.0
        let mut masternodes = MasternodesProgress::default();
        masternodes.update_target_height(100);
        masternodes.update_current_height(200);

        let mut progress = SyncProgress::default();
        progress.update_masternodes(masternodes);

        let info = SyncProgressInfo::from(&progress);
        assert_eq!(info.masternodes.percentage, 1.0);
    }

    /// Verify `get_sync_progress()` on a freshly-constructed client.
    #[tokio::test]
    async fn test_get_sync_progress_initial_state() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = ClientConfig::regtest()
            .without_filters()
            .without_masternodes()
            .with_storage_path(temp_dir.path());

        let client = SpvClient::new(config).await.expect("SpvClient construction must succeed");
        let info = client.get_sync_progress().await;

        // Before start() the client should not be fully synced.
        assert!(!info.is_synced);
        // The state must be one of the known variant strings.
        let valid_states = ["WaitForEvents", "WaitingForConnections", "Syncing", "Synced", "Error"];
        assert!(valid_states.contains(&info.state.as_str()), "unexpected state: {}", info.state);
        assert!(
            (0.0..=1.0).contains(&info.overall_percentage),
            "overall_percentage out of range: {}",
            info.overall_percentage
        );
    }

    // ---- WalletBalance record tests ----

    #[test]
    fn test_wallet_balance_fields() {
        let balance = WalletBalance {
            confirmed: 100_000_000,
            unconfirmed: 50_000,
            immature: 0,
        };
        assert_eq!(balance.confirmed, 100_000_000);
        assert_eq!(balance.unconfirmed, 50_000);
        assert_eq!(balance.immature, 0);
    }

    #[test]
    fn test_wallet_balance_zero() {
        let balance = WalletBalance {
            confirmed: 0,
            unconfirmed: 0,
            immature: 0,
        };
        assert_eq!(
            balance,
            WalletBalance {
                confirmed: 0,
                unconfirmed: 0,
                immature: 0
            }
        );
    }

    // ---- TransactionInfo record tests ----

    #[test]
    fn test_transaction_info_incoming() {
        let tx = TransactionInfo {
            txid: "abcd1234".to_string(),
            amount: 500_000_000,
            fee: 1_000,
            confirmations: 6,
            timestamp: 1_700_000_000,
            is_incoming: true,
        };
        assert_eq!(tx.txid, "abcd1234");
        assert_eq!(tx.amount, 500_000_000);
        assert_eq!(tx.fee, 1_000);
        assert_eq!(tx.confirmations, 6);
        assert_eq!(tx.timestamp, 1_700_000_000);
        assert!(tx.is_incoming);
    }

    #[test]
    fn test_transaction_info_outgoing() {
        let tx = TransactionInfo {
            txid: "deadbeef".to_string(),
            amount: -200_000_000,
            fee: 2_000,
            confirmations: 0,
            timestamp: 1_700_001_000,
            is_incoming: false,
        };
        assert!(tx.amount < 0);
        assert!(!tx.is_incoming);
        assert_eq!(tx.confirmations, 0);
    }

    // ---- SpvClient::get_balance stub test ----

    #[tokio::test]
    async fn test_get_balance_stub() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = ClientConfig::regtest()
            .without_filters()
            .without_masternodes()
            .with_storage_path(temp_dir.path());

        let client = SpvClient::new(config).await.expect("SpvClient construction must succeed");
        let balance = client.get_balance().await;
        assert_eq!(
            balance,
            WalletBalance {
                confirmed: 0,
                unconfirmed: 0,
                immature: 0
            },
            "stub get_balance should return all-zero balance"
        );
    }

    // ---- PeerInfo / NetworkInfo record tests ----

    #[test]
    fn test_peer_info_record() {
        let peer = PeerInfo {
            address: "192.0.2.1:9999".to_string(),
            user_agent: "/DashCore:0.18.0/".to_string(),
            best_height: 1234,
            connected_since: 1_700_000_000,
            services: 0x40d,
        };
        assert_eq!(peer.address, "192.0.2.1:9999");
        assert_eq!(peer.user_agent, "/DashCore:0.18.0/");
        assert_eq!(peer.best_height, 1234);
        assert_eq!(peer.connected_since, 1_700_000_000);
        assert_eq!(peer.services, 0x40d);
    }

    #[test]
    fn test_network_info_record_empty_peers() {
        let info = NetworkInfo {
            network: "mainnet".to_string(),
            peer_count: 0,
            peers: vec![],
        };
        assert_eq!(info.network, "mainnet");
        assert_eq!(info.peer_count, 0);
        assert!(info.peers.is_empty());
    }

    #[test]
    fn test_network_info_record_with_peers() {
        let peers = vec![
            PeerInfo {
                address: "10.0.0.1:9999".to_string(),
                user_agent: "/DashCore:0.19.0/".to_string(),
                best_height: 500,
                connected_since: 1_600_000_000,
                services: 1,
            },
            PeerInfo {
                address: "10.0.0.2:9999".to_string(),
                user_agent: "/DashCore:0.20.0/".to_string(),
                best_height: 501,
                connected_since: 1_600_000_001,
                services: 5,
            },
        ];
        let info = NetworkInfo {
            network: "testnet".to_string(),
            peer_count: 2,
            peers: peers.clone(),
        };
        assert_eq!(info.network, "testnet");
        assert_eq!(info.peer_count, 2);
        assert_eq!(info.peers.len(), 2);
        assert_eq!(info.peers[0].address, "10.0.0.1:9999");
        assert_eq!(info.peers[1].best_height, 501);
    }

    /// Verify that `get_network_info` returns a stub with the correct network
    /// name and zero peers before the client is started.
    #[tokio::test]
    async fn test_get_network_info_stub() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = ClientConfig::regtest()
            .without_filters()
            .without_masternodes()
            .with_storage_path(temp_dir.path());

        let client = SpvClient::new(config).await.expect("SpvClient construction must succeed");
        let info = client.get_network_info().await;

        assert_eq!(info.network, "regtest", "network name should be 'regtest'");
        assert_eq!(info.peer_count, 0, "peer count should be 0 before start");
        assert!(info.peers.is_empty(), "peers should be empty in stub implementation");
    }

    // ---- MasternodeInfo / GovernanceProposal record tests ----

    #[test]
    fn test_masternode_info_fields() {
        let info = MasternodeInfo {
            pro_tx_hash: "abcd1234".to_string(),
            address: "1.2.3.4:9999".to_string(),
            status: "Enabled".to_string(),
            pose_penalty: 0,
            last_paid_height: 500,
            registered_height: 100,
        };
        assert_eq!(info.pro_tx_hash, "abcd1234");
        assert_eq!(info.address, "1.2.3.4:9999");
        assert_eq!(info.status, "Enabled");
        assert_eq!(info.pose_penalty, 0);
        assert_eq!(info.last_paid_height, 500);
        assert_eq!(info.registered_height, 100);
    }

    #[test]
    fn test_governance_proposal_fields() {
        let proposal = GovernanceProposal {
            hash: "deadbeef".to_string(),
            name: "Test Proposal".to_string(),
            url: "https://example.com".to_string(),
            payment_address: "XtestAddr".to_string(),
            payment_amount: 100_000_000,
            yes_count: 10,
            no_count: 2,
            abstain_count: 1,
        };
        assert_eq!(proposal.hash, "deadbeef");
        assert_eq!(proposal.name, "Test Proposal");
        assert_eq!(proposal.url, "https://example.com");
        assert_eq!(proposal.payment_address, "XtestAddr");
        assert_eq!(proposal.payment_amount, 100_000_000);
        assert_eq!(proposal.yes_count, 10);
        assert_eq!(proposal.no_count, 2);
        assert_eq!(proposal.abstain_count, 1);
    }

    #[tokio::test]
    async fn test_get_masternode_count_stub() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = ClientConfig::regtest()
            .without_filters()
            .without_masternodes()
            .with_storage_path(temp_dir.path());

        let client = SpvClient::new(config).await.expect("SpvClient construction must succeed");
        assert_eq!(client.get_masternode_count().await, 0, "stub should return 0");
    }

    #[tokio::test]
    async fn test_get_masternodes_stub() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = ClientConfig::regtest()
            .without_filters()
            .without_masternodes()
            .with_storage_path(temp_dir.path());

        let client = SpvClient::new(config).await.expect("SpvClient construction must succeed");
        let masternodes = client.get_masternodes().await;
        assert!(masternodes.is_empty(), "stub should return empty vec");
    }
}
