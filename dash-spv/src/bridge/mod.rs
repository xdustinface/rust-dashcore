//! UniFFI bridge module for dash-spv.
//!
//! Exposes a concrete `SpvClient` object (wrapping the generic `DashSpvClient`) and
//! supporting types over the UniFFI foreign-language interface.
//!
//! Compiled only when the `uniffi` feature is enabled.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use dashcore::Network;
use tokio::sync::{Mutex, RwLock};

use crate::client::ClientConfig;
use crate::network::manager::PeerNetworkManager;
use crate::storage::DiskStorageManager;
use crate::DashSpvClient;
use key_wallet::wallet::managed_wallet_info::ManagedWalletInfo;
use key_wallet_manager::wallet_manager::WalletManager;

// ---------------------------------------------------------------------------
// custom_type! mappings for non-UniFFI-native types used in ClientConfig
// ---------------------------------------------------------------------------

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
    try_lift: |s| Ok(PathBuf::from(s)),
});

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors returned by `SpvClient` operations.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum SpvClientError {
    #[error("Network error: {0}")]
    Network(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("General error: {0}")]
    General(String),
}

impl From<crate::error::SpvError> for SpvClientError {
    fn from(e: crate::error::SpvError) -> Self {
        match e {
            crate::error::SpvError::Network(inner) => SpvClientError::Network(inner.to_string()),
            crate::error::SpvError::Storage(inner) => SpvClientError::Storage(inner.to_string()),
            crate::error::SpvError::Config(msg) => SpvClientError::Config(msg),
            other => SpvClientError::General(other.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Concrete type alias
// ---------------------------------------------------------------------------

type ConcreteClient =
    DashSpvClient<WalletManager<ManagedWalletInfo>, PeerNetworkManager, DiskStorageManager>;

// ---------------------------------------------------------------------------
// SpvClient UniFFI object
// ---------------------------------------------------------------------------

/// A concrete SPV client object suitable for export via UniFFI.
///
/// Wraps `DashSpvClient<WalletManager<ManagedWalletInfo>, PeerNetworkManager, DiskStorageManager>`
/// because UniFFI cannot export generic types directly.
#[derive(uniffi::Object)]
pub struct SpvClient {
    inner: Mutex<ConcreteClient>,
}

#[uniffi::export]
impl SpvClient {
    /// Construct a new `SpvClient` from the given `ClientConfig`.
    ///
    /// Creates `PeerNetworkManager`, `DiskStorageManager`, and an empty
    /// `WalletManager` from the config, then initialises `DashSpvClient`.
    #[uniffi::constructor]
    pub async fn new(config: ClientConfig) -> Result<Arc<Self>, SpvClientError> {
        let network_manager =
            PeerNetworkManager::new(&config).await.map_err(|e| SpvClientError::Network(e.to_string()))?;

        let storage =
            DiskStorageManager::new(&config).await.map_err(|e| SpvClientError::Storage(e.to_string()))?;

        let wallet = Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(config.network)));

        let inner = DashSpvClient::new(config, network_manager, storage, wallet)
            .await
            .map_err(SpvClientError::from)?;

        Ok(Arc::new(Self { inner: Mutex::new(inner) }))
    }

    /// Start the client: connect to peers and begin synchronisation.
    pub async fn start(&self) -> Result<(), SpvClientError> {
        self.inner.lock().await.start().await.map_err(SpvClientError::from)
    }

    /// Stop the client: disconnect from peers and flush storage.
    pub async fn stop(&self) -> Result<(), SpvClientError> {
        self.inner.lock().await.stop().await.map_err(SpvClientError::from)
    }

    /// Returns `true` if the client is currently running.
    pub async fn is_running(&self) -> bool {
        self.inner.lock().await.is_running().await
    }

    /// Returns the overall sync progress as a value in `[0.0, 1.0]`.
    pub async fn sync_progress(&self) -> f64 {
        self.inner.lock().await.sync_progress().await.percentage()
    }

    /// Returns `true` while the client is actively syncing.
    pub async fn is_syncing(&self) -> bool {
        use crate::sync::SyncState;
        self.inner.lock().await.sync_progress().await.state() == SyncState::Syncing
    }

    /// Current best-known block height.
    pub async fn tip_height(&self) -> u32 {
        self.inner.lock().await.tip_height().await
    }

    /// Number of currently connected peers.
    pub async fn peer_count(&self) -> u64 {
        self.inner.lock().await.peer_count().await as u64
    }
}

// ---------------------------------------------------------------------------
// Simple free functions (kept from previous bridge module)
// ---------------------------------------------------------------------------

/// Returns a greeting string (smoke-test export).
#[uniffi::export]
pub fn hello() -> String {
    "Hello from dash-spv!".to_string()
}

/// Returns the library version (async smoke-test export).
#[uniffi::export]
pub async fn get_version() -> String {
    crate::VERSION.to_string()
}

/// Callback interface for receiving SPV sync progress events.
#[uniffi::export(with_foreign)]
pub trait SpvEventListener: Send + Sync {
    /// Called when sync progress changes.
    fn on_sync_progress(&self, percentage: f64);
}

/// Starts a mock sync that reports progress via the listener callback.
///
/// Invokes `on_sync_progress` with 0.0 and 100.0 to simulate start and completion.
#[uniffi::export]
pub async fn start_mock_sync(listener: Arc<dyn SpvEventListener>) {
    listener.on_sync_progress(0.0);
    tokio::task::yield_now().await;
    listener.on_sync_progress(100.0);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    // --- Free function tests -------------------------------------------------

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

    // --- SpvEventListener mock ----------------------------------------------

    struct MockListener {
        events: StdMutex<Vec<f64>>,
    }

    impl SpvEventListener for MockListener {
        fn on_sync_progress(&self, percentage: f64) {
            self.events.lock().unwrap().push(percentage);
        }
    }

    #[tokio::test]
    async fn test_start_mock_sync() {
        let listener = Arc::new(MockListener { events: StdMutex::new(Vec::new()) });
        start_mock_sync(listener.clone()).await;
        let events = listener.events.lock().unwrap();
        assert_eq!(*events, vec![0.0, 100.0]);
    }

    // --- SpvClientError conversion ------------------------------------------

    #[test]
    fn test_error_from_spv_error_config() {
        let spv_err = crate::error::SpvError::Config("bad config".to_string());
        let bridge_err = SpvClientError::from(spv_err);
        assert!(matches!(bridge_err, SpvClientError::Config(_)));
        assert!(bridge_err.to_string().contains("bad config"));
    }

    #[test]
    fn test_error_from_spv_error_general() {
        let spv_err = crate::error::SpvError::General("oops".to_string());
        let bridge_err = SpvClientError::from(spv_err);
        assert!(matches!(bridge_err, SpvClientError::General(_)));
    }

    // --- SpvClient construction test ----------------------------------------
    // Requires the `test-utils` feature (provides MockNetworkManager and
    // the `tempfile` dependency used for temporary storage directories).

    #[cfg(feature = "test-utils")]
    #[tokio::test]
    async fn test_spv_client_construction() {
        use tempfile::TempDir;

        let tmp = TempDir::new().expect("tempdir");
        let config = ClientConfig::regtest()
            .without_masternodes()
            .without_filters()
            .with_storage_path(tmp.path());

        let storage = DiskStorageManager::new(&config)
            .await
            .expect("storage creation must succeed");
        let wallet =
            Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(config.network)));

        // Use a mock network manager so we don't need a live dashd peer.
        let network_manager = crate::test_utils::MockNetworkManager::new();
        let inner = DashSpvClient::new(config, network_manager, storage, wallet)
            .await
            .expect("client construction must succeed");

        assert!(!inner.is_running().await);
    }
}
