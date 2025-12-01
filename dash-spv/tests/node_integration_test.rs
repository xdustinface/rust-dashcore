///! SPV sync tests using real Dash Core node (dashd).
///!
///! These tests demonstrate realistic SPV sync scenarios against a real dashd instance.
mod test_utils;

use dash_spv::{
    client::{ClientConfig, DashSpvClient},
    network::PeerNetworkManager,
    storage::MemoryStorageManager,
    types::ValidationMode,
    Network as SpvNetwork, Network,
};
use key_wallet::wallet::initialization::WalletAccountCreationOptions;
use key_wallet::wallet::managed_wallet_info::wallet_info_interface::WalletInfoInterface;
use key_wallet::wallet::managed_wallet_info::ManagedWalletInfo;
use key_wallet::Network as WalletNetwork;
use key_wallet_manager::wallet_interface::WalletInterface;
use key_wallet_manager::wallet_manager::WalletManager;
use serde::Deserialize;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use test_utils::{is_dashd_available, DashCoreNode};
use tokio::sync::RwLock;
use tracing::{info, warn};

fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();
}

fn kill_all_dashd() {
    use std::process::Command;
    // Kill any existing dashd processes
    let _ = Command::new("pkill").arg("-9").arg("dashd").output();
    // Wait a moment for processes to die
    std::thread::sleep(Duration::from_millis(500));
}

// No NodeGuard needed - just use regular cleanup

/// Data structures for wallet validation

/// Test 1: Basic header sync from genesis using real dashd
#[tokio::test]
async fn test_sync_headers_with_dashcore() {
    init_test_logging();
    info!("=== Test: Sync headers with real dashd ===");

    // Skip if dashd is not available
    if !is_dashd_available() {
        warn!("dashd not available, skipping test");
        return;
    }

    // Try to start DashCoreNode
    let mut node = match DashCoreNode::new() {
        Ok(n) => n,
        Err(e) => {
            warn!("Failed to create DashCoreNode: {}", e);
            return;
        }
    };

    let addr = match node.start().await {
        Ok(a) => {
            info!("✅ DashCoreNode started at {}", a);
            a
        }
        Err(e) => {
            warn!("Failed to start dashd: {}", e);
            warn!("This is likely due to file descriptor limits on macOS");
            warn!("Try running: sudo launchctl limit maxfiles 65536 200000");
            return;
        }
    };

    // Create SPV client configuration
    let mut config = ClientConfig::new(SpvNetwork::Regtest)
        .with_validation_mode(ValidationMode::Basic)
        .with_connection_timeout(Duration::from_secs(10));

    config.peers.clear();
    config.peers.push(addr);

    // Create network and storage managers
    let network_manager = match PeerNetworkManager::new(&config).await {
        Ok(nm) => nm,
        Err(e) => {
            warn!("Failed to create network manager: {}", e);
            node.stop().await;
            return;
        }
    };

    let storage_manager = match MemoryStorageManager::new().await {
        Ok(sm) => sm,
        Err(e) => {
            panic!("Failed to create storage manager: {}", e);
            node.stop().await;
            return;
        }
    };

    // Create wallet manager
    let wallet = Arc::new(RwLock::new(WalletManager::<ManagedWalletInfo>::new()));

    // Create SPV client
    let mut client =
        match DashSpvClient::new(config, network_manager, storage_manager, wallet).await {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to create SPV client: {}", e);
                node.stop().await;
                return;
            }
        };

    // Start syncing
    info!("Starting SPV client sync...");
    if let Err(e) = client.start().await {
        warn!("Failed to start client: {}", e);
        node.stop().await;
        return;
    }

    // Let it sync for a bit
    //tokio::time::sleep(Duration::from_secs(5)).await;

    // Check sync progress
    let progress = client.sync_progress().await;
    info!("Sync progress: {:?}", progress);

    // Get block count from dashd for comparison
    match node.get_block_count().await {
        Ok(count) => {
            info!("Dashd has {} blocks", count);
            assert!(count > 0, "Dashd should have blocks");
        }
        Err(e) => {
            warn!("Failed to get block count: {}", e);
        }
    }

    // Shutdown
    let _ = client.shutdown().await;
    node.stop().await;

    info!("✅ Test completed successfully");
}

/// Test 2: Sync with specific addresses using real dashd
#[tokio::test]
async fn test_address_tracking_with_dashcore() {
    init_test_logging();
    info!("=== Test: Address tracking with real dashd ===");

    // Skip if dashd is not available
    if !is_dashd_available() {
        warn!("dashd not available, skipping test");
        return;
    }

    // Try to start DashCoreNode
    let mut node = match DashCoreNode::new() {
        Ok(n) => n,
        Err(e) => {
            warn!("Failed to create DashCoreNode: {}", e);
            return;
        }
    };

    let _addr = match node.start().await {
        Ok(a) => {
            info!("✅ DashCoreNode started at {}", a);
            a
        }
        Err(e) => {
            warn!("Failed to start dashd: {}", e);
            return;
        }
    };

    // TODO: Create wallet with specific addresses to track
    // TODO: Sync and verify address detection

    node.stop().await;
    info!("✅ Test completed");
}

/// Test 3: Verify compact filter support in real dashd
#[tokio::test]
async fn test_compact_filters_with_dashcore() {
    init_test_logging();
    info!("=== Test: Compact filters with real dashd ===");

    // Skip if dashd is not available
    if !is_dashd_available() {
        warn!("dashd not available, skipping test");
        return;
    }

    // Try to start DashCoreNode
    let mut node = match DashCoreNode::new() {
        Ok(n) => n,
        Err(e) => {
            warn!("Failed to create DashCoreNode: {}", e);
            return;
        }
    };

    let _addr = match node.start().await {
        Ok(a) => {
            info!("✅ DashCoreNode started at {}", a);
            a
        }
        Err(e) => {
            warn!("Failed to start dashd: {}", e);
            return;
        }
    };

    // TODO: Test compact filter requests and responses
    // TODO: Verify filter matching works correctly

    node.stop().await;
    info!("✅ Test completed");
}

/// Test 4: Performance test with real dashd
#[tokio::test]
async fn test_sync_performance_with_dashcore() {
    init_test_logging();
    info!("=== Test: Sync performance with real dashd ===");

    // Skip if dashd is not available
    if !is_dashd_available() {
        warn!("dashd not available, skipping test");
        return;
    }

    // Try to start DashCoreNode
    let mut node = match DashCoreNode::new() {
        Ok(n) => n,
        Err(e) => {
            warn!("Failed to create DashCoreNode: {}", e);
            return;
        }
    };

    let _addr = match node.start().await {
        Ok(a) => {
            info!("✅ DashCoreNode started at {}", a);
            a
        }
        Err(e) => {
            warn!("Failed to start dashd: {}", e);
            return;
        }
    };

    let start_time = std::time::Instant::now();

    // TODO: Measure sync performance metrics
    // - Headers per second
    // - Filter headers per second
    // - Block download rate

    let elapsed = start_time.elapsed();
    info!("Performance test completed in {:?}", elapsed);

    node.stop().await;
    info!("✅ Test completed");
}

/// Test 5: Full sync with validation against dashd
#[tokio::test]
async fn test_full_sync() {
    init_test_logging();
    kill_all_dashd();
    info!("=== Test: Full sync with validation ===");

    // Skip if dashd is not available
    if !is_dashd_available() {
        warn!("dashd not available, skipping test");
        return;
    }

    // Find test data directory
    let test_data_dir = find_test_data_dir().expect(
        "Test data not found. Generate with: cd test-data && python3 generate.py --blocks 1000",
    );
    info!("✅ Found test data at: {:?}", test_data_dir);

    // Load light wallet from test data
    let light_wallet = load_light_wallet(&test_data_dir).expect("Failed to load light wallet");
    info!(
        "✅ Loaded light wallet with {} transactions, {} UTXOs, balance: {:.8} DASH",
        light_wallet.transaction_count, light_wallet.utxo_count, light_wallet.balance
    );

    // Create DashCoreNode with pre-generated datadir
    // Note: test_data_dir is regtest-N/, which contains regtest/ subdirectory
    // dashd will look for regtest/ inside the provided datadir
    let config = test_utils::node::DashCoreConfig {
        dashd_path: test_utils::node::default_dashd_path(),
        datadir: test_data_dir.clone(), // Use regtest-N/ as datadir
        rpc_user: None,
        rpc_password: None,
        wallet: "light".to_string(),
    };

    let mut node = DashCoreNode::with_config(config).expect(
        "Failed to create DashCoreNode. Check that dashd binary exists at the configured path.",
    );

    let addr = node.start().await.expect(
        "Failed to start dashd. This test requires dashd to run. \
                 On macOS, you may need to increase file descriptor limits: \
                 sudo launchctl limit maxfiles 65536 200000 && ulimit -n 10000",
    );
    info!("✅ DashCoreNode started at {}", addr);

    // Wait for RPC to be fully ready
    info!("Waiting for RPC to be ready...");
    //tokio::time::sleep(Duration::from_secs(2)).await;

    // Get expected block count from dashd
    let expected_height =
        node.get_block_count().await.expect("Failed to get block count from dashd");
    info!("Dashd has {} blocks", expected_height);

    // Create SPV client configuration
    use dash_spv::client::config::MempoolStrategy;
    let mut config = ClientConfig::new(SpvNetwork::Regtest)
        .with_validation_mode(ValidationMode::Basic)
        .with_connection_timeout(Duration::from_secs(30))
        .with_mempool_tracking(MempoolStrategy::BloomFilter)
        .without_masternodes(); // Regtest doesn't have masternodes/quorums

    config.peers.clear();
    config.peers.push(addr);

    // Wait for dashd P2P to be fully ready
    info!("Waiting for dashd P2P to be fully ready...");
    //tokio::time::sleep(Duration::from_secs(3)).await;

    // Create network and storage managers
    let network_manager =
        PeerNetworkManager::new(&config).await.expect("Failed to create network manager");
    let storage_manager =
        MemoryStorageManager::new().await.expect("Failed to create storage manager");

    // Create wallet from mnemonic
    let wallet_network = WalletNetwork::Regtest;
    let mut wallet_manager = WalletManager::<ManagedWalletInfo>::new();
    let wallet_id = wallet_manager
        .create_wallet_from_mnemonic(
            &light_wallet.mnemonic,
            "", // No passphrase
            &[wallet_network],
            None, // birth_height
            WalletAccountCreationOptions::SpecificAccounts(
                {
                    let mut accounts = std::collections::BTreeSet::new();
                    accounts.insert(0); // Create only BIP44 account 0
                    accounts
                },
                std::collections::BTreeSet::new(), // No BIP32 accounts
                std::collections::BTreeSet::new(), // No CoinJoin accounts
                std::collections::BTreeSet::new(), // No identity top-up accounts
                None,                              // No additional special accounts
            ),
        )
        .expect("Failed to create wallet from mnemonic");
    info!("✅ Created wallet from mnemonic, ID: {:?}", wallet_id);
    //
    // // PRE-GENERATE ADDRESSES for SPV sync
    // // For SPV to work correctly from genesis, we need to pre-generate enough addresses
    // // to cover ALL addresses that will ever be used throughout the blockchain history.
    // // Based on dashd showing 2063 transactions, we need thousands of addresses.
    // info!("Pre-generating addresses for SPV compact filter matching...");
    //
    // use key_wallet::managed_account::address_pool::KeySource;
    // use key_wallet::wallet::managed_wallet_info::wallet_info_interface::WalletInfoInterface;
    //
    // // First, collect the accounts
    // let mut accounts_info = Vec::new();
    // if let Some(wallet) = wallet_manager.get_wallet(&wallet_id) {
    //     if let Some(account_collection) = wallet.accounts.get(&wallet_network) {
    //         for account in account_collection.all_accounts() {
    //             accounts_info.push((account.account_type, account.account_xpub));
    //         }
    //     }
    // }
    //
    // // Then, use them to pre-generate addresses
    // if let Some(wallet_info) = wallet_manager.get_wallet_info_mut(&wallet_id) {
    //     if let Some(managed_collection) = wallet_info.accounts_mut(wallet_network) {
    //         for managed_account in managed_collection.all_accounts_mut() {
    //             let managed_account_type = managed_account.account_type.to_account_type();
    //
    //             // Find matching account xpub
    //             if let Some((_, account_xpub)) = accounts_info
    //                 .iter()
    //                 .find(|(account_type, _)| *account_type == managed_account_type)
    //             {
    //                 let key_source = KeySource::Public(*account_xpub);
    //
    //                 // Pre-generate 10,000 addresses for each pool (external + internal)
    //                 let pools = managed_account.account_type.address_pools_mut();
    //                 for pool in pools {
    //                     let count_before = pool.addresses.len();
    //                     // Generate addresses from 0 to 9999 (10,000 total)
    //                     if let Ok(_) = pool.address_range(0, 1000, &key_source) {
    //                         let count_after = pool.addresses.len();
    //                         info!(
    //                             "  Generated {} addresses for {} pool (total: {})",
    //                             count_after - count_before,
    //                             if pool.is_external() {
    //                                 "external"
    //                             } else {
    //                                 "internal"
    //                             },
    //                             count_after
    //                         );
    //                     }
    //                 }
    //             }
    //         }
    //     }
    // }
    // info!("✅ Address pre-generation complete");

    let wallet = Arc::new(RwLock::new(wallet_manager));

    // Create SPV client
    let mut client = DashSpvClient::new(config, network_manager, storage_manager, wallet.clone())
        .await
        .expect("Failed to create SPV client");

    // Start syncing
    info!("Starting SPV client sync...");
    client.start().await.expect("Failed to start SPV client");

    // Take the progress receiver
    let mut progress_receiver =
        client.take_progress_receiver().expect("Progress receiver should be available");

    // Spawn monitor_network() in background
    info!("Starting network monitoring task...");
    let monitor_handle = tokio::task::spawn(async move {
        if let Err(e) = client.monitor_network().await {
            warn!("Monitor network error: {}", e);
        }
        client
    });

    // Wait for sync to complete
    info!("Waiting for sync to complete (expected height: {})...", expected_height);
    let start_time = tokio::time::Instant::now();
    let timeout = Duration::from_secs(800);
    let mut last_progress = None;

    let final_progress = loop {
        assert!(
            start_time.elapsed() <= timeout,
            "SPV client sync timeout after {} seconds at height {:?}",
            timeout.as_secs(),
            last_progress
                .as_ref()
                .map(|p: &dash_spv::types::DetailedSyncProgress| p.sync_progress.header_height)
        );

        match tokio::time::timeout(Duration::from_secs(1), progress_receiver.recv()).await {
            Ok(Some(progress)) => {
                let height = progress.sync_progress.header_height;

                // Log progress when height changes
                if last_progress
                    .as_ref()
                    .map(|p: &dash_spv::types::DetailedSyncProgress| p.sync_progress.header_height)
                    != Some(height)
                {
                    info!(
                        "Sync progress: {}/{} headers ({:.1}%) - Stage: {:?}",
                        height, expected_height, progress.percentage, progress.sync_stage
                    );
                }

                last_progress = Some(progress.clone());

                // Check if sync is complete
                if progress.sync_stage == dash_spv::types::SyncStage::Complete {
                    info!(
                        "✅ Sync completed! Headers: {}, Filter headers: {}, Filters: {}",
                        progress.sync_progress.header_height,
                        progress.sync_progress.filter_header_height,
                        progress.sync_progress.filters_downloaded
                    );
                    break progress.sync_progress;
                }

                // Check for failed state
                if let dash_spv::types::SyncStage::Failed(reason) = &progress.sync_stage {
                    assert!(false, "Sync failed: {}", reason);
                }
            }
            Ok(None) => {
                warn!("Progress channel closed unexpectedly");
                break last_progress.map(|p| p.sync_progress).unwrap_or_default();
            }
            Err(_) => {
                // Timeout waiting for progress - continue waiting
            }
        }
    };

    tokio::time::sleep(Duration::from_secs(5)).await;

    // Abort the monitoring task
    info!("Aborting network monitoring task...");
    monitor_handle.abort();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Validate sync results
    info!("=== Validation ===");

    assert_eq!(final_progress.header_height, expected_height, "Header height mismatch");
    info!("✅ Header height matches: {}", final_progress.header_height);

    assert!(final_progress.peer_count > 0, "No peers connected");
    info!("✅ Connected to {} peer(s)", final_progress.peer_count);

    // Get the read lock of the wallet
    let wallet_read = wallet.read().await;

    // Get SPV UTXOs and write to file for comparison
    if let Some(wallet_info) = wallet_read.get_wallet_info(&wallet_id) {
        let utxos = wallet_info.get_utxos(wallet_network);

        let mut spv_utxos: Vec<String> = utxos
            .iter()
            .map(|(outpoint, _utxo)| format!("{}:{}", outpoint.txid, outpoint.vout))
            .collect();
        spv_utxos.sort();

        use std::fs::File;
        use std::io::Write;
        let mut file = File::create("/tmp/spv_utxos.txt").expect("Failed to create SPV UTXOs file");
        for utxo in &spv_utxos {
            writeln!(file, "{}", utxo).expect("Failed to write UTXO");
        }
        info!("✅ Wrote {} SPV UTXOs to /tmp/spv_utxos.txt", spv_utxos.len());
    } else {
        warn!("Wallet info not found for wallet_id: {:?}", wallet_id);
    }

    if let Some(wallet_info) = wallet_read.get_wallet_info(&wallet_id) {
        // Get all SPV transaction IDs
        let mut spv_txids = std::collections::HashSet::new();
        if let Some(managed_collection) = wallet_info.accounts(wallet_network) {
            for managed_account in managed_collection.all_accounts() {
                for txid in managed_account.transactions.keys() {
                    spv_txids.insert(format!("{}", txid));
                }
            }
        }
        // Add all immature transactions
        let immature = wallet_info.immature_transactions(wallet_network).unwrap().all();
        for tx in immature {
            spv_txids.insert(tx.txid.to_string());
        }

        // Get expected transaction IDs from JSON
        let mut expected_txids = std::collections::HashSet::new();
        for tx in &light_wallet.transactions {
            if let Some(txid) = tx.get("txid").and_then(|v| v.as_str()) {
                expected_txids.insert(txid.to_string());
            }
        }

        info!("Transaction comparison:");
        info!("  SPV found:      {} transactions", spv_txids.len());
        info!("  Expected:       {} transactions", expected_txids.len());
        info!("  JSON tx_count:  {}", light_wallet.transaction_count);

        // Export SPV txids to file
        {
            use std::fs::File;
            use std::io::Write;
            let mut file =
                File::create("/tmp/spv_txids_actual.txt").expect("Failed to create SPV txids file");
            let mut sorted_spv: Vec<_> = spv_txids.iter().map(|s| s.as_str()).collect();
            sorted_spv.sort();
            for txid in sorted_spv {
                writeln!(file, "{}", txid).expect("Failed to write txid");
            }
            info!("✅ Wrote {} SPV transaction IDs to /tmp/spv_txids_actual.txt", spv_txids.len());
        }

        // Find missing and extra transactions
        let missing_txids: Vec<_> = expected_txids.difference(&spv_txids).collect();
        let extra_txids: Vec<_> = spv_txids.difference(&expected_txids).collect();

        if !missing_txids.is_empty() {
            warn!("⚠ Missing {} transactions in SPV wallet:", missing_txids.len());
            for txid in missing_txids.iter().take(10) {
                warn!("    {}", txid);
            }
            if missing_txids.len() > 10 {
                warn!("    ... and {} more", missing_txids.len() - 10);
            }

            // Export missing txids to file
            use std::fs::File;
            use std::io::Write;
            let mut file = File::create("/tmp/missing_txids.txt")
                .expect("Failed to create missing txids file");
            let mut sorted_missing: Vec<_> = missing_txids.iter().map(|s| s.as_str()).collect();
            sorted_missing.sort();
            for txid in sorted_missing {
                writeln!(file, "{}", txid).expect("Failed to write txid");
            }
            info!(
                "✅ Wrote {} missing transaction IDs to /tmp/missing_txids.txt",
                missing_txids.len()
            );
        }

        if !extra_txids.is_empty() {
            warn!("⚠ Extra {} transactions in SPV wallet:", extra_txids.len());
            for txid in extra_txids.iter().take(10) {
                warn!("    {}", txid);
            }
            if extra_txids.len() > 10 {
                warn!("    ... and {} more", extra_txids.len() - 10);
            }
        }

        // Assert transaction count matches
        assert_eq!(
            spv_txids.len(),
            expected_txids.len(),
            "Transaction count mismatch: SPV has {}, expected {}",
            spv_txids.len(),
            expected_txids.len()
        );

        // Assert all expected transactions are present
        assert!(
            missing_txids.is_empty(),
            "SPV wallet is missing {} transactions",
            missing_txids.len()
        );

        // Assert no unexpected transactions
        assert!(
            extra_txids.is_empty(),
            "SPV wallet has {} unexpected transactions",
            extra_txids.len()
        );

        info!("✅ All {} transactions match expected set", spv_txids.len());
    }
    drop(wallet_read);

    let wallet_read = wallet.read().await;

    // Check wallet balance
    let balance = wallet_read.get_wallet_balance(&wallet_id).expect("Failed to get wallet balance");

    info!(
        "SPV Wallet balance: {} satoshis ({:.8} DASH)",
        balance.total,
        balance.total as f64 / 100_000_000.0
    );

    let expected = light_wallet
        .utxos
        .iter()
        .filter_map(|u| u.get("amount").and_then(|v| v.as_f64()))
        .map(|dash| (dash * 100_000_000.0) as u64)
        .sum::<u64>();
    info!("Expected balance: {} satoshis ({:.8} DASH)", expected, expected as f64 / 100_000_000.0);

    assert_eq!(
        balance.total, expected,
        "Wallet balance mismatch: SPV has {}, expected {}",
        balance.total, expected
    );
    info!("✅ Balance matches expected value from JSON");

    drop(wallet_read);

    // Cleanup
    node.stop().await;

    info!("✅ Full sync validation test completed successfully");
}

/// Wallet file structure (individual wallet JSON)
#[derive(Debug, Deserialize)]
struct WalletFile {
    #[allow(dead_code)]
    wallet_name: String,
    mnemonic: String,
    balance: f64,
    transaction_count: usize,
    utxo_count: usize,
    #[allow(dead_code)]
    transactions: Vec<serde_json::Value>,
    #[allow(dead_code)]
    utxos: Vec<serde_json::Value>,
}

/// Helper function to load light wallet from test data
fn load_light_wallet(
    test_data_dir: &std::path::Path,
) -> Result<WalletFile, Box<dyn std::error::Error>> {
    let wallet_path = test_data_dir.join("wallets/light.json");

    let json_content = fs::read_to_string(&wallet_path)?;
    let wallet_file: WalletFile = serde_json::from_str(&json_content)?;

    Ok(wallet_file)
}

/// Helper function to find the test data directory
fn find_test_data_dir() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let workspace_root =
        std::env::current_dir()?.parent().ok_or("Could not get parent directory")?.to_path_buf();

    let possible_paths = vec![
        //workspace_root.join("../test-blockchain/data/regtest-10000"),
        workspace_root.join("../test-blockchain/data/regtest-1000"),
    ];

    // New structure: regtest-N/ contains regtest/ subdirectory (not datadir/)
    possible_paths
        .iter()
        .find(|p| p.exists() && p.join("regtest").exists() && p.join("wallets").exists())
        .map(|p| p.to_path_buf())
        .ok_or_else(|| {
            "No test data found. Generate with: cd test-data && python3 generate.py --blocks 1000"
                .into()
        })
}
/// Helper function to get block hash from dashd via RPC
async fn get_block_hash_from_dashd(node: &DashCoreNode, height: u32) -> Option<String> {
    let dash_cli = node.config().dashd_path.parent().and_then(|p| Some(p.join("dash-cli")))?;

    let output = std::process::Command::new(dash_cli)
        .arg("-regtest")
        .arg(format!("-datadir={}", node.config().datadir.display()))
        .arg("-rpcport=19998")
        .arg("getblockhash")
        .arg(height.to_string())
        .output()
        .ok()?;

    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        None
    }
}
