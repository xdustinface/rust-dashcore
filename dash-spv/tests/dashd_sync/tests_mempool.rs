use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use dash_spv::client::config::MempoolStrategy;
use dash_spv::network::NetworkEvent;
use dash_spv::test_utils::DashdTestContext;
use dashcore::Amount;

use super::helpers::{
    assert_no_mempool_tx, wait_for_mempool_activated, wait_for_mempool_tx, wait_for_network_event,
    wait_for_sync,
};
use super::setup::{create_and_start_client, TestContext};

const MEMPOOL_TIMEOUT: Duration = Duration::from_secs(30);

/// Verify mempool detects an incoming wallet transaction using the default FetchAll strategy.
#[tokio::test]
async fn test_mempool_detects_incoming_tx() {
    let Some(ctx) = TestContext::new().await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let mut client_handle = ctx.spawn_new_client().await;
    wait_for_sync(&mut client_handle.progress_receiver, ctx.dashd.initial_height).await;

    let receive_address = ctx.receive_address().await;
    let txid = ctx.dashd.node.send_to_address(&receive_address, Amount::from_sat(100_000_000));
    tracing::info!("Sent tx to SPV wallet (no mine), txid: {}", txid);

    let mempool_txid =
        wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
            .await
            .expect("Expected mempool TransactionReceived event");
    assert_eq!(mempool_txid, txid, "Mempool event txid should match sent txid");

    let mempool_count = client_handle.client.get_mempool_transaction_count().await;
    assert_eq!(
        mempool_count, 1,
        "Mempool should contain exactly 1 transaction, got {}",
        mempool_count
    );

    client_handle.stop().await;
    tracing::info!("test_mempool_detects_incoming_tx passed");
}

/// Verify mempool detects an incoming wallet transaction using the BloomFilter strategy.
#[tokio::test]
async fn test_mempool_bloom_filter_detects_incoming_tx() {
    let Some(ctx) = TestContext::new().await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let mut config = ctx.client_config.clone();
    config.mempool_strategy = MempoolStrategy::BloomFilter;

    let mut client_handle = create_and_start_client(&config, Arc::clone(&ctx.wallet)).await;
    wait_for_sync(&mut client_handle.progress_receiver, ctx.dashd.initial_height).await;

    let receive_address = ctx.receive_address().await;
    let txid = ctx.dashd.node.send_to_address(&receive_address, Amount::from_sat(100_000_000));
    tracing::info!("Sent tx to SPV wallet (BloomFilter), txid: {}", txid);

    let mempool_txid =
        wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
            .await
            .expect("Expected mempool TransactionReceived event (BloomFilter)");
    assert_eq!(mempool_txid, txid, "Mempool event txid should match sent txid");

    let mempool_count = client_handle.client.get_mempool_transaction_count().await;
    assert_eq!(
        mempool_count, 1,
        "Mempool should contain exactly 1 transaction, got {}",
        mempool_count
    );

    client_handle.stop().await;
    tracing::info!("test_mempool_bloom_filter_detects_incoming_tx passed");
}

/// Verify mempool ignores transactions not relevant to the SPV wallet.
#[tokio::test]
async fn test_mempool_ignores_irrelevant_tx() {
    let Some(ctx) = TestContext::new().await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    // Fund the "default" wallet with a regular (non-coinbase) output so it's
    // immediately spendable. Send from the primary wallet and mine the tx.
    let default_addr = ctx.dashd.node.get_new_address_from_wallet("default");
    ctx.dashd.node.send_to_address(&default_addr, Amount::from_sat(100_000_000));
    let miner_addr = ctx.dashd.node.get_new_address_from_wallet("default");
    ctx.dashd.node.generate_blocks(1, &miner_addr);
    let funded_height = ctx.dashd.initial_height + 1;

    let mut client_handle = ctx.spawn_new_client().await;
    wait_for_sync(&mut client_handle.progress_receiver, funded_height).await;

    // Send from the "default" wallet to itself (no relation to SPV wallet)
    let non_wallet_address = ctx.dashd.node.get_new_address_from_wallet("default");
    let txid = ctx.dashd.node.send_to_address_from_wallet(
        "default",
        &non_wallet_address,
        Amount::from_sat(50_000_000),
    );
    tracing::info!("Sent irrelevant tx (not to SPV wallet), txid: {}", txid);

    assert_no_mempool_tx(&mut client_handle.wallet_event_receiver, Duration::from_secs(3)).await;

    let mempool_count = client_handle.client.get_mempool_transaction_count().await;
    assert_eq!(
        mempool_count, 0,
        "Mempool should have 0 wallet-relevant transactions, got {}",
        mempool_count
    );

    client_handle.stop().await;
    tracing::info!("test_mempool_ignores_irrelevant_tx passed");
}

/// Verify a mempool transaction transitions to confirmed after mining.
#[tokio::test]
async fn test_mempool_to_confirmed_lifecycle() {
    let Some(ctx) = TestContext::new().await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let mut client_handle = ctx.spawn_new_client().await;
    wait_for_sync(&mut client_handle.progress_receiver, ctx.dashd.initial_height).await;

    let receive_address = ctx.receive_address().await;
    let txid = ctx.dashd.node.send_to_address(&receive_address, Amount::from_sat(100_000_000));
    tracing::info!("Sent tx to SPV wallet (lifecycle test), txid: {}", txid);

    let mempool_txid =
        wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
            .await
            .expect("Expected mempool TransactionReceived event");
    assert_eq!(mempool_txid, txid);

    let mempool_count = client_handle.client.get_mempool_transaction_count().await;
    assert_eq!(mempool_count, 1, "Mempool should have exactly 1 tx before mining");

    // Mine the transaction
    let miner_address = ctx.dashd.node.get_new_address_from_wallet("default");
    ctx.dashd.node.generate_blocks(1, &miner_address);
    let new_height = ctx.dashd.initial_height + 1;
    wait_for_sync(&mut client_handle.progress_receiver, new_height).await;

    let mempool_count_after = client_handle.client.get_mempool_transaction_count().await;
    assert_eq!(
        mempool_count_after, 0,
        "Mempool should be empty after confirmation, got {}",
        mempool_count_after
    );
    assert!(ctx.has_transaction(&txid).await, "Confirmed transaction should be in wallet");

    client_handle.stop().await;
    tracing::info!("test_mempool_to_confirmed_lifecycle passed");
}

/// Verify multiple mempool transactions are all detected.
#[tokio::test]
async fn test_mempool_multiple_txs() {
    let Some(ctx) = TestContext::new().await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let mut client_handle = ctx.spawn_new_client().await;
    wait_for_sync(&mut client_handle.progress_receiver, ctx.dashd.initial_height).await;

    let receive_address = ctx.receive_address().await;
    let amounts =
        [Amount::from_sat(50_000_000), Amount::from_sat(75_000_000), Amount::from_sat(120_000_000)];
    let mut expected_txids = HashSet::new();
    for amount in &amounts {
        let txid = ctx.dashd.node.send_to_address(&receive_address, *amount);
        tracing::info!("Sent {} to SPV wallet (multi-tx test), txid: {}", amount, txid);
        expected_txids.insert(txid);
    }

    // Collect 3 mempool events
    let mut received_txids = HashSet::new();
    for _ in 0..3 {
        let txid = wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
            .await
            .expect("Expected mempool TransactionReceived event");
        received_txids.insert(txid);
    }

    assert_eq!(received_txids, expected_txids, "Received mempool txids should match sent txids");

    let mempool_count = client_handle.client.get_mempool_transaction_count().await;
    assert_eq!(
        mempool_count, 3,
        "Mempool should contain exactly 3 transactions, got {}",
        mempool_count
    );

    client_handle.stop().await;
    tracing::info!("test_mempool_multiple_txs passed");
}

/// Verify mempool detects both incoming (address match) and outgoing (outpoint match) transactions.
///
/// 1. Sync to tip
/// 2. Send from "default" wallet TO the SPV wallet receive address (incoming)
/// 3. Wait for mempool event (address match)
/// 4. Mine the tx so it becomes a confirmed UTXO in the SPV wallet
/// 5. Craft a raw tx that spends the wallet UTXO with all outputs going to an external
///    "default" address (no change back to the wallet) and broadcast it
/// 6. Wait for mempool event (outpoint match only, no address match)
/// 7. Assert both txids were detected
#[tokio::test]
async fn test_mempool_incoming_and_outgoing_tx() {
    let Some(ctx) = TestContext::new().await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let mut client_handle = ctx.spawn_new_client().await;
    wait_for_sync(&mut client_handle.progress_receiver, ctx.dashd.initial_height).await;

    // Step 1: Send an incoming transaction to the SPV wallet
    let receive_address = ctx.receive_address().await;
    let incoming_amount = Amount::from_sat(200_000_000);
    let incoming_txid = ctx.dashd.node.send_to_address(&receive_address, incoming_amount);
    tracing::info!("Sent incoming tx to SPV wallet, txid: {}", incoming_txid);

    let mempool_txid =
        wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
            .await
            .expect("Expected mempool event for incoming tx");
    assert_eq!(mempool_txid, incoming_txid);

    // Step 2: Mine the incoming tx so it becomes a confirmed UTXO
    let miner_address = ctx.dashd.node.get_new_address_from_wallet("default");
    ctx.dashd.node.generate_blocks(1, &miner_address);
    let mined_height = ctx.dashd.initial_height + 1;
    wait_for_sync(&mut client_handle.progress_receiver, mined_height).await;

    // Step 3: Craft a raw transaction that spends the wallet UTXO with all outputs
    // going to an external address. This ensures the mempool detects it purely via
    // the watched outpoint, not via any output address match.
    let wallet_name = &ctx.dashd.wallet.wallet_name;
    let utxos = ctx.dashd.node.list_unspent_from_wallet(wallet_name);
    let utxo = utxos
        .iter()
        .find(|u| u.txid == incoming_txid)
        .expect("Incoming tx UTXO not found in wallet");

    let external_address = ctx.dashd.node.get_new_address_from_wallet("default");
    let fee = Amount::from_sat(10_000);
    let outgoing_txid = ctx.dashd.node.send_raw_from_wallet(
        wallet_name,
        utxo.txid,
        utxo.vout,
        utxo.amount,
        &external_address,
        fee,
    );
    tracing::info!("Sent raw outgoing tx (outpoint-only match), txid: {}", outgoing_txid);

    let mempool_txid =
        wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
            .await
            .expect("Expected mempool event for outgoing tx (outpoint match)");
    assert_eq!(mempool_txid, outgoing_txid);

    client_handle.stop().await;
    tracing::info!("test_mempool_incoming_and_outgoing_tx passed");
}

/// Verify mempool detects both incoming and outgoing transactions using the BloomFilter strategy.
///
/// This validates that `rebuild_filter()` correctly includes new watched outpoints in the
/// bloom filter after block processing. The bloom filter must be rebuilt after a new UTXO
/// is confirmed so that spending transactions (outpoint matches) are detected.
#[tokio::test]
async fn test_mempool_bloom_filter_incoming_and_outgoing_tx() {
    let Some(ctx) = TestContext::new().await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let mut config = ctx.client_config.clone();
    config.mempool_strategy = MempoolStrategy::BloomFilter;

    let mut client_handle = create_and_start_client(&config, Arc::clone(&ctx.wallet)).await;
    wait_for_sync(&mut client_handle.progress_receiver, ctx.dashd.initial_height).await;

    // Step 1: Send an incoming transaction to the SPV wallet
    let receive_address = ctx.receive_address().await;
    let incoming_amount = Amount::from_sat(200_000_000);
    let incoming_txid = ctx.dashd.node.send_to_address(&receive_address, incoming_amount);
    tracing::info!("Sent incoming tx to SPV wallet (BloomFilter), txid: {}", incoming_txid);

    let mempool_txid =
        wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
            .await
            .expect("Expected mempool event for incoming tx (BloomFilter)");
    assert_eq!(mempool_txid, incoming_txid);

    // Step 2: Mine the incoming tx so it becomes a confirmed UTXO
    let miner_address = ctx.dashd.node.get_new_address_from_wallet("default");
    ctx.dashd.node.generate_blocks(1, &miner_address);
    let mined_height = ctx.dashd.initial_height + 1;
    wait_for_sync(&mut client_handle.progress_receiver, mined_height).await;

    // Step 3: Craft a raw transaction that spends the wallet UTXO with all outputs
    // going to an external address. The bloom filter must have been rebuilt after
    // block processing to include the new outpoint for this to be detected.
    let wallet_name = &ctx.dashd.wallet.wallet_name;
    let utxos = ctx.dashd.node.list_unspent_from_wallet(wallet_name);
    let utxo = utxos
        .iter()
        .find(|u| u.txid == incoming_txid)
        .expect("Incoming tx UTXO not found in wallet");

    let external_address = ctx.dashd.node.get_new_address_from_wallet("default");
    let fee = Amount::from_sat(10_000);
    let outgoing_txid = ctx.dashd.node.send_raw_from_wallet(
        wallet_name,
        utxo.txid,
        utxo.vout,
        utxo.amount,
        &external_address,
        fee,
    );
    tracing::info!(
        "Sent raw outgoing tx (BloomFilter, outpoint-only match), txid: {}",
        outgoing_txid
    );

    let mempool_txid =
        wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
            .await
            .expect("Expected mempool event for outgoing tx (BloomFilter, outpoint match)");
    assert_eq!(mempool_txid, outgoing_txid);

    client_handle.stop().await;
    tracing::info!("test_mempool_bloom_filter_incoming_and_outgoing_tx passed");
}

/// Verify mempool handles peer disconnection and reactivation across multiple scenarios.
///
/// Uses two dashd nodes connected to each other. The SPV client connects to both peers and
/// exercises these scenarios in sequence:
/// 1. Mempool activates after sync — verify tx detection works
/// 2. Disconnect the mempool peer — verify reactivation on the other peer, then tx detection
/// 3. Disconnect both peers, reconnect one — verify mempool recovers and detects a tx
#[tokio::test]
async fn test_mempool_peer_disconnect_reactivation() {
    let Some(ctx) = TestContext::new().await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let Some(dashd2) = DashdTestContext::new().await else {
        eprintln!("Skipping test (could not create second dashd node)");
        return;
    };

    // Connect the two dashd nodes so mempool transactions propagate between them
    ctx.dashd.node.connect_to_node(dashd2.addr).await;

    // Configure SPV client with both peers and sync
    let mut config = ctx.client_config.clone();
    config.add_peer(dashd2.addr);

    let mut client_handle = create_and_start_client(&config, Arc::clone(&ctx.wallet)).await;
    wait_for_sync(&mut client_handle.progress_receiver, ctx.dashd.initial_height).await;

    // Identify which peer was chosen for mempool relay.
    // Brief sleep lets the MemPool/FilterClear messages reach the peer before we send txs.
    let mempool_peer = wait_for_mempool_activated(&mut client_handle.sync_event_receiver)
        .await
        .expect("Expected MempoolActivated event after sync");
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("Mempool activated on peer {}", mempool_peer);

    let (mempool_node, other_node, other_addr) = if mempool_peer == ctx.dashd.addr {
        (&ctx.dashd.node, &dashd2.node, dashd2.addr)
    } else {
        (&dashd2.node, &ctx.dashd.node, ctx.dashd.addr)
    };

    // --- Scenario 1: baseline mempool detection with both peers ---
    // Send from the mempool peer's node directly so the tx is immediately in its mempool
    // and relayed to SPV without needing inter-node propagation.
    let receive_address = ctx.receive_address().await;
    let txid1 = mempool_node.send_to_address(&receive_address, Amount::from_sat(50_000_000));
    tracing::info!("[scenario 1] sent tx {} from mempool peer", txid1);

    let detected = wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
        .await
        .expect("Scenario 1: expected mempool tx detection");
    assert_eq!(detected, txid1);

    let mempool_count = client_handle.client.get_mempool_transaction_count().await;
    assert_eq!(mempool_count, 1, "Scenario 1: expected 1 mempool tx");

    // --- Scenario 2: disconnect the mempool peer, verify reactivation on the other ---
    mempool_node.disconnect_all_peers();

    let saw_disconnect = wait_for_network_event(
        &mut client_handle.network_event_receiver,
        |e| matches!(e, NetworkEvent::PeerDisconnected { address } if *address == mempool_peer),
        Duration::from_secs(10),
    )
    .await;
    assert!(saw_disconnect, "SPV should observe PeerDisconnected for mempool peer");

    let new_mempool_peer = wait_for_mempool_activated(&mut client_handle.sync_event_receiver)
        .await
        .expect("Expected MempoolActivated after mempool peer disconnect");
    assert_eq!(new_mempool_peer, other_addr, "Mempool should reactivate on the remaining peer");
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("[scenario 2] mempool reactivated on {}", new_mempool_peer);

    let txid2 = other_node.send_to_address(&receive_address, Amount::from_sat(60_000_000));
    tracing::info!("[scenario 2] sent tx {} from reactivated mempool peer", txid2);

    let detected = wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
        .await
        .expect("Scenario 2: expected mempool tx detection after reactivation");
    assert_eq!(detected, txid2);

    let mempool_count = client_handle.client.get_mempool_transaction_count().await;
    assert_eq!(mempool_count, 2, "Scenario 2: expected 2 mempool txs (cumulative)");

    // --- Scenario 3: disconnect both peers, verify recovery ---
    // Reconnect dashd nodes so SPV can reach either after full disconnect.
    mempool_node.connect_to_node(other_addr).await;
    other_node.disconnect_all_peers();

    let saw_disconnect = wait_for_network_event(
        &mut client_handle.network_event_receiver,
        |e| matches!(e, NetworkEvent::PeerDisconnected { address } if *address == other_addr),
        Duration::from_secs(10),
    )
    .await;
    assert!(saw_disconnect, "SPV should observe PeerDisconnected for other peer");

    // SPV has no peers. Wait for reconnection and mempool reactivation.
    let saw_reconnect = wait_for_network_event(
        &mut client_handle.network_event_receiver,
        |e| matches!(e, NetworkEvent::PeerConnected { .. }),
        Duration::from_secs(30),
    )
    .await;
    assert!(saw_reconnect, "SPV should reconnect to a peer");

    let recovered_peer = wait_for_mempool_activated(&mut client_handle.sync_event_receiver)
        .await
        .expect("Expected MempoolActivated after full disconnect recovery");
    tokio::time::sleep(Duration::from_secs(1)).await;
    tracing::info!("[scenario 3] mempool recovered on peer {}", recovered_peer);

    let recovered_node = if recovered_peer == ctx.dashd.addr {
        &ctx.dashd.node
    } else {
        &dashd2.node
    };
    let txid3 = recovered_node.send_to_address(&receive_address, Amount::from_sat(70_000_000));
    tracing::info!("[scenario 3] sent tx {} from recovered mempool peer", txid3);

    let detected = wait_for_mempool_tx(&mut client_handle.wallet_event_receiver, MEMPOOL_TIMEOUT)
        .await
        .expect("Scenario 3: expected mempool tx detection after full disconnect recovery");
    assert_eq!(detected, txid3);

    let mempool_count = client_handle.client.get_mempool_transaction_count().await;
    assert_eq!(mempool_count, 3, "Scenario 3: expected 3 mempool txs (cumulative)");

    client_handle.stop().await;
    tracing::info!("test_mempool_peer_disconnect_reactivation passed");
}
