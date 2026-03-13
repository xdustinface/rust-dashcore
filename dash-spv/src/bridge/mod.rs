//! Full SpvClient UniFFI facade.
//!
//! Exposes:
//! - `SpvConfig` — UniFFI record wrapping [`ClientConfig`] with per-network defaults
//! - `SpvClient` — UniFFI object wrapping the monomorphised [`DashSpvClient`]
//! - Lifecycle methods: `start`, `stop`, `shutdown`
//! - State queries: `is_running`, `tip_hash`, `tip_height`, `peer_count`
//! - Legacy shims: `hello`, `get_version`, `start_mock_sync`, `SpvEventListener`
//!
//! All code in this module is compiled only when the `uniffi` feature is enabled.

use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;

use crate::client::ClientConfig;
use crate::network::PeerNetworkManager;
use crate::storage::DiskStorageManager;
use crate::types::ValidationMode;
use key_wallet::wallet::managed_wallet_info::ManagedWalletInfo;
use key_wallet_manager::wallet_manager::WalletManager;

// ─────────────────────────── Legacy shims ────────────────────────────────────

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

// ─────────────────────────── Network enum ────────────────────────────────────

/// Dash network variant.
#[derive(uniffi::Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpvNetwork {
    Mainnet,
    Testnet,
    Regtest,
}

impl From<SpvNetwork> for dashcore::Network {
    fn from(n: SpvNetwork) -> Self {
        match n {
            SpvNetwork::Mainnet => dashcore::Network::Mainnet,
            SpvNetwork::Testnet => dashcore::Network::Testnet,
            SpvNetwork::Regtest => dashcore::Network::Regtest,
        }
    }
}

impl From<dashcore::Network> for SpvNetwork {
    fn from(n: dashcore::Network) -> Self {
        match n {
            dashcore::Network::Mainnet => SpvNetwork::Mainnet,
            dashcore::Network::Testnet => SpvNetwork::Testnet,
            dashcore::Network::Regtest => SpvNetwork::Regtest,
            _ => SpvNetwork::Mainnet,
        }
    }
}

// ─────────────────────────── Validation mode enum ────────────────────────────

/// Header validation mode.
#[derive(uniffi::Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpvValidationMode {
    /// Skip most validation — useful for testing.
    None,
    /// Validate basic structure only.
    Basic,
    /// Validate proof of work and chain continuity.
    Full,
}

impl From<SpvValidationMode> for ValidationMode {
    fn from(m: SpvValidationMode) -> Self {
        match m {
            SpvValidationMode::None => ValidationMode::None,
            SpvValidationMode::Basic => ValidationMode::Basic,
            SpvValidationMode::Full => ValidationMode::Full,
        }
    }
}

// ─────────────────────────── SpvConfig record ────────────────────────────────

/// Configuration for the SPV client.
///
/// Use the free constructor functions (`spv_config_mainnet`, `spv_config_testnet`,
/// `spv_config_regtest`) to get sensible per-network defaults, then customise as needed.
#[derive(uniffi::Record, Debug, Clone)]
pub struct SpvConfig {
    /// Network to connect to.
    pub network: SpvNetwork,
    /// Directory used for persistent storage.
    pub storage_path: String,
    /// Header validation strictness.
    pub validation_mode: SpvValidationMode,
    /// If set, sync starts from the nearest checkpoint at or before this height.
    pub start_height: Option<u32>,
    /// Whether to sync compact block filters (BIP 157/158).
    pub enable_filters: bool,
    /// Whether to sync the deterministic masternode list.
    pub enable_masternodes: bool,
    /// Maximum number of simultaneously connected peers.
    pub max_peers: u32,
}

impl SpvConfig {
    fn mainnet_defaults(storage_path: String) -> Self {
        Self {
            network: SpvNetwork::Mainnet,
            storage_path,
            validation_mode: SpvValidationMode::Full,
            start_height: None,
            enable_filters: true,
            enable_masternodes: true,
            max_peers: 8,
        }
    }
}

impl From<SpvConfig> for ClientConfig {
    fn from(c: SpvConfig) -> Self {
        let mut cfg = ClientConfig::new(dashcore::Network::from(c.network))
            .with_storage_path(c.storage_path)
            .with_validation_mode(c.validation_mode.into());
        cfg.enable_filters = c.enable_filters;
        cfg.enable_masternodes = c.enable_masternodes;
        cfg.max_peers = c.max_peers;
        if let Some(h) = c.start_height {
            cfg = cfg.with_start_height(h);
        }
        cfg
    }
}

/// Create a [`SpvConfig`] with mainnet defaults.
#[uniffi::export]
pub fn spv_config_mainnet(storage_path: String) -> SpvConfig {
    SpvConfig::mainnet_defaults(storage_path)
}

/// Create a [`SpvConfig`] with testnet defaults.
#[uniffi::export]
pub fn spv_config_testnet(storage_path: String) -> SpvConfig {
    SpvConfig {
        network: SpvNetwork::Testnet,
        ..SpvConfig::mainnet_defaults(storage_path)
    }
}

/// Create a [`SpvConfig`] with regtest defaults.
///
/// Filters and masternode sync are disabled; validation is relaxed.
#[uniffi::export]
pub fn spv_config_regtest(storage_path: String) -> SpvConfig {
    SpvConfig {
        network: SpvNetwork::Regtest,
        validation_mode: SpvValidationMode::None,
        enable_filters: false,
        enable_masternodes: false,
        max_peers: 1,
        ..SpvConfig::mainnet_defaults(storage_path)
    }
}

// ─────────────────────────── Error type ──────────────────────────────────────

/// Errors that can be returned by the [`SpvClient`] facade.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum SpvClientError {
    #[error("Configuration error: {message}")]
    Config { message: String },
    #[error("Network error: {message}")]
    Network { message: String },
    #[error("Storage error: {message}")]
    Storage { message: String },
    #[error("Client is already running")]
    AlreadyRunning,
    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl From<crate::error::SpvError> for SpvClientError {
    fn from(e: crate::error::SpvError) -> Self {
        match e {
            crate::error::SpvError::Config(msg) => SpvClientError::Config { message: msg },
            crate::error::SpvError::Network(e) => {
                SpvClientError::Network { message: e.to_string() }
            }
            crate::error::SpvError::Storage(e) => {
                SpvClientError::Storage { message: e.to_string() }
            }
            other => SpvClientError::Internal { message: other.to_string() },
        }
    }
}

// ─────────────────────────── SpvClient object ────────────────────────────────

type MonoClient = crate::client::DashSpvClient<
    WalletManager<ManagedWalletInfo>,
    PeerNetworkManager,
    DiskStorageManager,
>;

/// Internal run-loop state.
struct LifecycleState {
    token: Option<CancellationToken>,
    run_handle: Option<tokio::task::JoinHandle<()>>,
}

/// SPV client UniFFI object.
///
/// Create via [`SpvClient::new`], then call [`SpvClient::start`] to connect to the
/// Dash network and begin synchronisation.  Use [`SpvClient::stop`] (or
/// [`SpvClient::shutdown`]) to tear down gracefully.
///
/// All methods are `async`; on the foreign side they map to coroutines / `suspend fun` / etc.
#[derive(uniffi::Object)]
pub struct SpvClient {
    inner: Arc<MonoClient>,
    lifecycle: Mutex<LifecycleState>,
}

#[uniffi::export]
impl SpvClient {
    /// Construct a new `SpvClient` from the given configuration.
    ///
    /// Initialises on-disk storage, the wallet, and the network manager, but does
    /// **not** open any peer connections.  Call [`start`][SpvClient::start] to begin
    /// synchronisation.
    #[uniffi::constructor]
    pub async fn new(config: SpvConfig) -> Result<Arc<Self>, SpvClientError> {
        let client_config: ClientConfig = config.into();

        let network = PeerNetworkManager::new(&client_config)
            .await
            .map_err(|e| SpvClientError::Network { message: e.to_string() })?;

        let storage = DiskStorageManager::new(&client_config)
            .await
            .map_err(|e| SpvClientError::Storage { message: e.to_string() })?;

        let wallet =
            Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(client_config.network)));

        let inner =
            crate::client::DashSpvClient::new(client_config, network, storage, wallet)
                .await
                .map_err(SpvClientError::from)?;

        Ok(Arc::new(Self {
            inner: Arc::new(inner),
            lifecycle: Mutex::new(LifecycleState { token: None, run_handle: None }),
        }))
    }

    /// Connect to peers and begin synchronisation.
    ///
    /// Returns immediately; the sync loop runs in a background task.
    /// Call [`stop`][SpvClient::stop] to shut down gracefully.
    ///
    /// Returns [`SpvClientError::AlreadyRunning`] if the client is already started.
    pub async fn start(&self) -> Result<(), SpvClientError> {
        let mut lc = self.lifecycle.lock().await;
        if lc.token.is_some() {
            return Err(SpvClientError::AlreadyRunning);
        }

        let token = CancellationToken::new();
        let client = Arc::clone(&self.inner);
        let token_clone = token.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = client.run(token_clone).await {
                tracing::error!("SpvClient run loop exited with error: {e}");
            }
        });

        lc.token = Some(token);
        lc.run_handle = Some(handle);

        Ok(())
    }

    /// Stop the client gracefully.
    ///
    /// Cancels the run loop and waits for the background task to finish.
    /// Safe to call even if the client is not running.
    pub async fn stop(&self) -> Result<(), SpvClientError> {
        // Take ownership of the lifecycle handles while holding the lock, then
        // release the lock before awaiting — prevents deadlock.
        let (token, handle) = {
            let mut lc = self.lifecycle.lock().await;
            (lc.token.take(), lc.run_handle.take())
        };

        if let Some(token) = token {
            token.cancel();
        }
        if let Some(handle) = handle {
            let _ = handle.await;
        }

        Ok(())
    }

    /// Shut down the client gracefully (alias for [`stop`][SpvClient::stop]).
    pub async fn shutdown(&self) -> Result<(), SpvClientError> {
        self.stop().await
    }

    /// Return `true` if the client is currently running.
    pub async fn is_running(&self) -> bool {
        self.inner.is_running().await
    }

    /// Return the current chain tip block hash as a hex string, or `None` if unavailable.
    pub async fn tip_hash(&self) -> Option<String> {
        self.inner.tip_hash().await.map(|h| h.to_string())
    }

    /// Return the current chain tip height (0 if no headers have been synced yet).
    pub async fn tip_height(&self) -> u32 {
        self.inner.tip_height().await
    }

    /// Return the number of currently connected peers.
    pub async fn peer_count(&self) -> u64 {
        self.inner.peer_count().await as u64
    }
}

// ─────────────────────────── Tests ───────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;

    // ── SpvConfig defaults ────────────────────────────────────────────────────

    #[test]
    fn test_spv_config_mainnet_defaults() {
        let config = spv_config_mainnet("/tmp/spv-test".to_string());
        assert_eq!(config.network, SpvNetwork::Mainnet);
        assert_eq!(config.validation_mode, SpvValidationMode::Full);
        assert!(config.enable_filters);
        assert!(config.enable_masternodes);
        assert_eq!(config.max_peers, 8);
        assert!(config.start_height.is_none());
    }

    #[test]
    fn test_spv_config_testnet_defaults() {
        let config = spv_config_testnet("/tmp/spv-test".to_string());
        assert_eq!(config.network, SpvNetwork::Testnet);
        assert_eq!(config.validation_mode, SpvValidationMode::Full);
        assert!(config.enable_filters);
        assert!(config.enable_masternodes);
    }

    #[test]
    fn test_spv_config_regtest_defaults() {
        let config = spv_config_regtest("/tmp/spv-test".to_string());
        assert_eq!(config.network, SpvNetwork::Regtest);
        assert_eq!(config.validation_mode, SpvValidationMode::None);
        assert!(!config.enable_filters);
        assert!(!config.enable_masternodes);
        assert_eq!(config.max_peers, 1);
    }

    #[test]
    fn test_spv_config_to_client_config() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        let config = SpvConfig {
            network: SpvNetwork::Testnet,
            storage_path: path.clone(),
            validation_mode: SpvValidationMode::Basic,
            start_height: Some(100_000),
            enable_filters: false,
            enable_masternodes: false,
            max_peers: 4,
        };

        let client_config: ClientConfig = config.into();

        assert_eq!(client_config.network, dashcore::Network::Testnet);
        assert_eq!(client_config.validation_mode, ValidationMode::Basic);
        assert!(!client_config.enable_filters);
        assert!(!client_config.enable_masternodes);
        assert_eq!(client_config.max_peers, 4);
        assert_eq!(client_config.start_from_height, Some(100_000));
    }

    // ── Network enum conversions ──────────────────────────────────────────────

    #[test]
    fn test_network_enum_roundtrip() {
        for (spv, core) in [
            (SpvNetwork::Mainnet, dashcore::Network::Mainnet),
            (SpvNetwork::Testnet, dashcore::Network::Testnet),
            (SpvNetwork::Regtest, dashcore::Network::Regtest),
        ] {
            assert_eq!(dashcore::Network::from(spv), core);
            assert_eq!(SpvNetwork::from(core), spv);
        }
    }

    // ── Validation mode conversions ───────────────────────────────────────────

    #[test]
    fn test_validation_mode_conversion() {
        assert_eq!(ValidationMode::from(SpvValidationMode::None), ValidationMode::None);
        assert_eq!(ValidationMode::from(SpvValidationMode::Basic), ValidationMode::Basic);
        assert_eq!(ValidationMode::from(SpvValidationMode::Full), ValidationMode::Full);
    }

    // ── SpvClient construction and initial state ──────────────────────────────

    #[tokio::test]
    async fn test_spv_client_initial_state() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let config = spv_config_regtest(path);

        let client = SpvClient::new(config).await.expect("client construction should succeed");

        assert!(!client.is_running().await, "client should not be running before start");
        assert_eq!(client.peer_count().await, 0, "no peers before start");
        // tip_height and tip_hash are safe to call before start
        let _ = client.tip_height().await;
        let _ = client.tip_hash().await;
    }

    #[tokio::test]
    async fn test_spv_client_start_stop() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let config = spv_config_regtest(path);

        let client = SpvClient::new(config).await.expect("client construction should succeed");
        client.start().await.expect("start should succeed");
        client.stop().await.expect("stop should succeed");
    }

    #[tokio::test]
    async fn test_spv_client_double_start_returns_error() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let config = spv_config_regtest(path);

        let client = SpvClient::new(config).await.expect("client construction should succeed");
        client.start().await.expect("first start should succeed");

        let result = client.start().await;
        assert!(
            matches!(result, Err(SpvClientError::AlreadyRunning)),
            "second start should return AlreadyRunning"
        );

        client.stop().await.expect("stop should succeed");
    }

    #[tokio::test]
    async fn test_spv_client_shutdown_is_alias_for_stop() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let config = spv_config_regtest(path);

        let client = SpvClient::new(config).await.expect("client construction should succeed");
        client.start().await.expect("start should succeed");
        client.shutdown().await.expect("shutdown should succeed");
    }

    #[tokio::test]
    async fn test_spv_client_stop_when_not_started_is_noop() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let config = spv_config_regtest(path);

        let client = SpvClient::new(config).await.expect("client construction should succeed");
        // stop() on a not-yet-started client should succeed silently
        client.stop().await.expect("stop before start should be a no-op");
    }

    // ── Legacy shim tests ─────────────────────────────────────────────────────

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
        let listener = Arc::new(MockListener { events: Mutex::new(Vec::new()) });
        start_mock_sync(listener.clone()).await;
        let events = listener.events.lock().unwrap();
        assert_eq!(*events, vec![0.0, 100.0]);
    }
}
