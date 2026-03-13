//! High-level client API for the Dash SPV client.
//!
//! Provides `DashSpvClient`, the main entry point for SPV operations including
//! sync orchestration, mempool tracking, peer/masternode queries, and transaction
//! broadcasting.
//!
//! ## Module Structure
//!
//! - `config.rs` - Client configuration
//! - `core.rs` - Core `DashSpvClient` struct definition and simple accessors
//! - `lifecycle.rs` - Client lifecycle (new, start, stop, shutdown)
//! - `events.rs` - Event emission and progress tracking receivers
//! - `mempool.rs` - Mempool tracking and coordination
//! - `queries.rs` - Peer, masternode, and balance queries
//! - `transactions.rs` - Transaction operations (e.g., broadcast)
//! - `sync_coordinator.rs` - Sync orchestration and network monitoring
//!
//! ## Lock Ordering
//!
//! When acquiring multiple locks, always use this order:
//! 1. running (`Arc<RwLock<bool>>`)
//! 2. mempool_state (`Arc<RwLock<MempoolState>>`)
//! 3. storage (`Arc<Mutex<S>>`)
//!
//! Never acquire locks in reverse order or deadlock will occur!

pub mod config;

mod core;
mod events;
mod lifecycle;
mod mempool;
mod queries;
mod sync_coordinator;
mod transactions;

// Re-export public types from extracted modules
pub use config::ClientConfig;

// Re-export the main client struct
pub use core::DashSpvClient;

#[cfg(test)]
mod config_test;

#[cfg(test)]
mod tests {
    use super::{ClientConfig, DashSpvClient};
    use crate::client::config::MempoolStrategy;
    use crate::storage::DiskStorageManager;
    use crate::{test_utils::MockNetworkManager, types::UnconfirmedTransaction};
    use dashcore::sml::masternode_list::MasternodeList;
    use dashcore::sml::masternode_list_entry::{
        EntryMasternodeType, MasternodeListEntry,
        qualified_masternode_list_entry::QualifiedMasternodeListEntry,
    };
    use dashcore::sml::masternode_list_engine::MasternodeListEngine;
    use dashcore::{Address, Amount, BlockHash, ProTxHash, Transaction, TxOut};
    use dashcore_hashes::Hash;
    use key_wallet::wallet::managed_wallet_info::ManagedWalletInfo;
    use key_wallet_manager::wallet_manager::WalletManager;
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    // Tests for get_mempool_balance function
    // These tests validate that the balance calculation correctly handles:
    // 1. The sign of net_amount
    // 2. Validation of transaction effects on addresses
    // 3. Edge cases like zero amounts and conflicting signs

    #[tokio::test]
    async fn client_exposes_shared_wallet_manager() {
        let config = ClientConfig::mainnet()
            .without_filters()
            .without_masternodes()
            .with_mempool_tracking(MempoolStrategy::FetchAll)
            .with_storage_path(TempDir::new().unwrap().path());

        let network_manager = MockNetworkManager::new();
        let storage =
            DiskStorageManager::with_temp_dir().await.expect("Failed to create tmp storage");
        let wallet = Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(config.network)));

        let client = DashSpvClient::new(config, network_manager, storage, wallet)
            .await
            .expect("client construction must succeed");

        // Verify the wallet is accessible
        let wallet_ref = client.wallet();
        let _wallet_guard = wallet_ref.read().await;
        // Success: we can access the shared wallet
    }

    #[tokio::test]
    async fn test_get_mempool_balance_logic() {
        // This test validates the get_mempool_balance logic by directly testing
        // the balance calculation code using a mocked mempool state.

        let config = ClientConfig::testnet()
            .without_filters()
            .without_masternodes()
            .with_mempool_tracking(MempoolStrategy::FetchAll)
            .with_storage_path(TempDir::new().unwrap().path());

        let network_manager = MockNetworkManager::new();
        let storage = DiskStorageManager::new(&config).await.expect("Failed to create tmp storage");
        let wallet = Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(config.network)));

        let test_address = Address::dummy(config.network, 0);

        let client = DashSpvClient::new(config, network_manager, storage, wallet)
            .await
            .expect("client construction must succeed");

        // Create a transaction that sends 10 Dash to the test address
        let tx = Transaction {
            version: 2,
            lock_time: 0,
            input: vec![],
            output: vec![TxOut {
                value: 1_000_000_000, // 10 Dash in satoshis
                script_pubkey: test_address.script_pubkey(),
            }],
            special_transaction_payload: None,
        };

        // Add to mempool state
        {
            let mut mempool_state = client.mempool_state.write().await;
            let tx_record = UnconfirmedTransaction {
                transaction: tx.clone(),
                first_seen: std::time::Instant::now(),
                fee: Amount::ZERO,
                size: 0,
                is_instant_send: false,
                addresses: vec![test_address.clone()],
                net_amount: 1_000_000_000, // Incoming 10 Dash
                is_outgoing: false,
            };
            mempool_state.transactions.insert(tx.txid(), tx_record);
        }

        // Get balance for the test address
        let balance = client
            .get_mempool_balance(&test_address)
            .await
            .expect("balance calculation must succeed");

        // Verify the pending balance is correct
        assert_eq!(
            balance.pending,
            Amount::from_sat(1_000_000_000),
            "Pending balance should be 10 Dash"
        );
        assert_eq!(balance.pending_instant, Amount::ZERO, "InstantSend balance should be zero");

        // Test with InstantSend transaction
        {
            // Modify transaction to be InstantSend
            let mut mempool_state = client.mempool_state.write().await;
            if let Some(tx_record) = mempool_state.transactions.get_mut(&tx.txid()) {
                tx_record.is_instant_send = true;
            }
        }

        let balance = client
            .get_mempool_balance(&test_address)
            .await
            .expect("balance calculation must succeed");

        // Verify InstantSend balance
        assert_eq!(balance.pending, Amount::ZERO, "Regular pending should be zero");
        assert_eq!(
            balance.pending_instant,
            Amount::from_sat(1_000_000_000),
            "InstantSend balance should be 10 Dash"
        );
    }

    // ============ masternode_engine() accessor tests ============

    /// `masternode_engine()` returns `None` when masternodes are disabled.
    #[tokio::test]
    async fn test_masternode_engine_none_when_disabled() {
        let config = ClientConfig::testnet()
            .without_filters()
            .without_masternodes()
            .with_storage_path(TempDir::new().unwrap().path());

        let network_manager = MockNetworkManager::new();
        let storage = DiskStorageManager::new(&config).await.expect("storage");
        let wallet = Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(config.network)));

        let client = DashSpvClient::new(config, network_manager, storage, wallet)
            .await
            .expect("client construction must succeed");

        assert!(
            client.masternode_engine().await.is_none(),
            "masternode_engine() should be None when masternodes are disabled"
        );
    }

    /// `masternode_engine()` returns `Some` when masternodes are enabled.
    #[tokio::test]
    async fn test_masternode_engine_some_when_enabled() {
        let config = ClientConfig::testnet()
            .without_filters()
            .with_storage_path(TempDir::new().unwrap().path());

        let network_manager = MockNetworkManager::new();
        let storage = DiskStorageManager::new(&config).await.expect("storage");
        let wallet = Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(config.network)));

        let client = DashSpvClient::new(config, network_manager, storage, wallet)
            .await
            .expect("client construction must succeed");

        assert!(
            client.masternode_engine().await.is_some(),
            "masternode_engine() should be Some when masternodes are enabled"
        );
    }

    /// Verifies that after manually inserting a masternode list into the engine
    /// the count and entries are visible via the engine handle.
    #[tokio::test]
    async fn test_masternode_engine_populated_reflects_entries() {
        let config = ClientConfig::testnet()
            .without_filters()
            .with_storage_path(TempDir::new().unwrap().path());
        let network = config.network;

        let network_manager = MockNetworkManager::new();
        let storage = DiskStorageManager::new(&config).await.expect("storage");
        let wallet = Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new(network)));

        let client = DashSpvClient::new(config, network_manager, storage, wallet)
            .await
            .expect("client construction must succeed");

        let engine_arc = client
            .masternode_engine()
            .await
            .expect("engine should be Some");

        // Build a minimal MasternodeListEntry for testing.
        let pro_reg_tx_hash = ProTxHash::all_zeros();
        let entry = MasternodeListEntry {
            version: 1,
            pro_reg_tx_hash,
            confirmed_hash: None,
            service_address: SocketAddr::from_str("1.2.3.4:9999").unwrap(),
            operator_public_key: dashcore::bls_sig_utils::BLSPublicKey::from([0u8; 48]),
            key_id_voting: dashcore::PubkeyHash::all_zeros(),
            is_valid: true,
            mn_type: EntryMasternodeType::Regular,
        };
        let qualified: QualifiedMasternodeListEntry = entry.into();

        // Insert the entry into a MasternodeList and load it into the engine.
        let block_hash = BlockHash::all_zeros();
        let masternodes = BTreeMap::from([(pro_reg_tx_hash, qualified)]);
        let list = MasternodeList::build(masternodes, BTreeMap::new(), block_hash, 100).build();

        {
            let mut engine = engine_arc.write().await;
            engine.masternode_lists.insert(100, list);
        }

        // Now read back via the accessor.
        let engine_arc2 = client
            .masternode_engine()
            .await
            .expect("engine should still be Some");
        let engine = engine_arc2.read().await;
        let latest = engine.latest_masternode_list().expect("list should be present");

        assert_eq!(latest.masternodes.len(), 1, "should have 1 masternode");
        let mn = latest.masternodes.values().next().unwrap();
        assert!(mn.masternode_list_entry.is_valid, "masternode should be valid (Enabled)");
        assert_eq!(
            mn.masternode_list_entry.service_address.to_string(),
            "1.2.3.4:9999"
        );
    }
}
