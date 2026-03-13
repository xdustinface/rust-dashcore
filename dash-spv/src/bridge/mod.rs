//! Minimal UniFFI bridge module for dash-spv.
//!
//! This module validates three UniFFI call patterns:
//! - Sync function
//! - Async function
//! - Callback interface
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
use crate::sync::SyncState;

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

    /// Returns `true` when the client is actively downloading and processing blocks.
    pub async fn is_syncing(&self) -> bool {
        matches!(self.inner.sync_progress().await.state(), SyncState::Syncing)
    }
}

// ============ Legacy stub functions (kept for backward compatibility) ============

/// A simple sync function that returns a greeting string.
#[uniffi::export]
pub fn hello() -> String {
    "Hello from dash-spv!".to_string()
}

/// An async function that returns the library version.
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
    // Simulate minimal async work
    tokio::task::yield_now().await;
    listener.on_sync_progress(100.0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::TempDir;

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
        events: Mutex<Vec<f64>>,
    }

    impl SpvEventListener for MockListener {
        fn on_sync_progress(&self, percentage: f64) {
            self.events.lock().unwrap().push(percentage);
        }
    }

    #[tokio::test]
    async fn test_start_mock_sync() {
        let listener = Arc::new(MockListener {
            events: Mutex::new(Vec::new()),
        });
        start_mock_sync(listener.clone()).await;
        let events = listener.events.lock().unwrap();
        assert_eq!(*events, vec![0.0, 100.0]);
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

        // Before start(), the client is not actively syncing.
        assert!(!client.is_syncing().await, "Client should not be syncing before start()");
    }
}

