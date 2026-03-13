//! UniFFI bridge module for dash-spv.
//!
//! Provides callback traits and UniFFI-compatible event record types for
//! bridging the SPV client to foreign (e.g. React Native / Swift) code.
//!
//! Compiled only when the `uniffi` feature is enabled.

use dashcore::Network;

uniffi::custom_type!(Network, String, {
    remote,
    lower: |n| n.to_string(),
    try_lift: |s| s.parse().map_err(|e: String| uniffi::deps::anyhow::anyhow!(e)),
});

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
        // Verify all variants can be constructed and cloned.
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
        // Clone succeeds for all variants.
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
}
