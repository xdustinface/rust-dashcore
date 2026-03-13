use super::*;
use crate::wallet_interface::WalletInterface;
use dashcore::hashes::Hash;
use dashcore::{BlockHash, OutPoint, ScriptBuf, TxIn, TxOut, Txid, Witness};
use key_wallet::wallet::initialization::WalletAccountCreationOptions;
use key_wallet::wallet::managed_wallet_info::wallet_info_interface::WalletInfoInterface;
use key_wallet::wallet::managed_wallet_info::ManagedWalletInfo;
use tokio::sync::broadcast;

const TEST_MNEMONIC: &str =
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

const TX_AMOUNT: u64 = 100_000;

fn setup_manager_with_wallet() -> (WalletManager<ManagedWalletInfo>, WalletId, Address) {
    let mut manager = WalletManager::new(Network::Testnet);
    let wallet_id = manager
        .create_wallet_from_mnemonic(TEST_MNEMONIC, "", 0, WalletAccountCreationOptions::Default)
        .unwrap();
    let addresses = manager.monitored_addresses();
    assert!(!addresses.is_empty(), "wallet should have monitored addresses");
    let addr = addresses[0].clone();
    (manager, wallet_id, addr)
}

fn create_tx_paying_to(addr: &Address, input_seed: u8) -> Transaction {
    Transaction {
        version: 2,
        lock_time: 0,
        input: vec![TxIn {
            previous_output: OutPoint {
                txid: Txid::from_byte_array([input_seed; 32]),
                vout: 0,
            },
            script_sig: ScriptBuf::new(),
            sequence: u32::MAX,
            witness: Witness::default(),
        }],
        output: vec![TxOut {
            value: TX_AMOUNT,
            script_pubkey: addr.script_pubkey(),
        }],
        special_transaction_payload: None,
    }
}

fn drain_events(rx: &mut broadcast::Receiver<WalletEvent>) -> Vec<WalletEvent> {
    let mut events = Vec::new();
    while let Ok(e) = rx.try_recv() {
        events.push(e);
    }
    events
}

/// Drain events from the receiver, assert exactly one was emitted, and return it.
fn assert_single_event(rx: &mut broadcast::Receiver<WalletEvent>) -> WalletEvent {
    let events = drain_events(rx);
    assert_eq!(events.len(), 1, "expected 1 event, got {}: {:?}", events.len(), events);
    events.into_iter().next().unwrap()
}

/// Drain events and assert none were emitted.
fn assert_no_events(rx: &mut broadcast::Receiver<WalletEvent>) {
    let events = drain_events(rx);
    assert!(events.is_empty(), "expected no events, got {}: {:?}", events.len(), events);
}

// ---------------------------------------------------------------------------
// Lifecycle flow tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mempool_to_confirmed_event_flow() {
    let (mut manager, _wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0xaa);

    // First time in mempool
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    let event = assert_single_event(&mut rx);
    assert!(
        matches!(
            event,
            WalletEvent::TransactionReceived {
                status: TransactionContext::Mempool,
                ..
            }
        ),
        "expected TransactionReceived(Mempool), got {:?}",
        event
    );

    // Same tx now confirmed in a block
    let block_ctx = TransactionContext::InBlock {
        height: 100,
        block_hash: Some(BlockHash::from_byte_array([0xaa; 32])),
        timestamp: Some(1000),
    };
    manager.check_transaction_in_all_wallets(&tx, block_ctx, true).await;
    let event = assert_single_event(&mut rx);
    assert!(
        matches!(
            event,
            WalletEvent::TransactionStatusChanged {
                status: TransactionContext::InBlock {
                    height: 100,
                    ..
                },
                ..
            }
        ),
        "expected TransactionStatusChanged(InBlock), got {:?}",
        event
    );
}

#[tokio::test]
async fn test_mempool_to_instantsend_to_confirmed_event_flow() {
    let (mut manager, _wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0xbb);

    // Mempool
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    let event = assert_single_event(&mut rx);
    assert!(matches!(
        event,
        WalletEvent::TransactionReceived {
            status: TransactionContext::Mempool,
            ..
        }
    ));

    // InstantSend
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::InstantSend, true).await;
    let event = assert_single_event(&mut rx);
    assert!(matches!(
        event,
        WalletEvent::TransactionStatusChanged {
            status: TransactionContext::InstantSend,
            ..
        }
    ));

    // Confirmed in block
    let block_ctx = TransactionContext::InBlock {
        height: 200,
        block_hash: Some(BlockHash::from_byte_array([0xbb; 32])),
        timestamp: Some(2000),
    };
    manager.check_transaction_in_all_wallets(&tx, block_ctx, true).await;
    let event = assert_single_event(&mut rx);
    assert!(matches!(
        event,
        WalletEvent::TransactionStatusChanged {
            status: TransactionContext::InBlock {
                height: 200,
                ..
            },
            ..
        }
    ));
}

#[tokio::test]
async fn test_mempool_to_confirmed_to_chainlocked_event_flow() {
    let (mut manager, _wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0xcc);
    let txid = tx.txid();

    // Mempool
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    let event = assert_single_event(&mut rx);
    assert!(matches!(
        event,
        WalletEvent::TransactionReceived {
            status: TransactionContext::Mempool,
            ..
        }
    ));

    // Confirmed in block
    let block_hash = BlockHash::from_byte_array([0xbb; 32]);
    let block_ctx = TransactionContext::InBlock {
        height: 500,
        block_hash: Some(block_hash),
        timestamp: Some(5000),
    };
    manager.check_transaction_in_all_wallets(&tx, block_ctx, true).await;
    let event = assert_single_event(&mut rx);
    assert!(matches!(
        event,
        WalletEvent::TransactionStatusChanged {
            status: TransactionContext::InBlock { .. },
            ..
        }
    ));

    // Chainlock at height 500
    manager.process_chainlock(500);
    let event = assert_single_event(&mut rx);
    assert!(
        matches!(
            &event,
            WalletEvent::TransactionStatusChanged {
                txid: event_txid,
                status: TransactionContext::InChainLockedBlock { height: 500, .. },
            } if *event_txid == txid
        ),
        "expected TransactionStatusChanged(InChainLockedBlock), got {:?}",
        event
    );
}

#[tokio::test]
async fn test_first_seen_in_block_event_flow() {
    let (mut manager, _wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0xdd);

    let block_ctx = TransactionContext::InBlock {
        height: 1000,
        block_hash: Some(BlockHash::from_byte_array([0xdd; 32])),
        timestamp: Some(10000),
    };
    manager.check_transaction_in_all_wallets(&tx, block_ctx, true).await;
    let event = assert_single_event(&mut rx);
    assert!(
        matches!(
            event,
            WalletEvent::TransactionReceived {
                status: TransactionContext::InBlock {
                    height: 1000,
                    ..
                },
                ..
            }
        ),
        "expected TransactionReceived(InBlock), got {:?}",
        event
    );
}

#[tokio::test]
async fn test_first_seen_in_chainlocked_block_event_flow() {
    let (mut manager, _wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0xee);

    let ctx = TransactionContext::InChainLockedBlock {
        height: 2000,
        block_hash: Some(BlockHash::from_byte_array([0xee; 32])),
        timestamp: Some(20000),
    };
    manager.check_transaction_in_all_wallets(&tx, ctx, true).await;
    let event = assert_single_event(&mut rx);
    assert!(
        matches!(
            event,
            WalletEvent::TransactionReceived {
                status: TransactionContext::InChainLockedBlock {
                    height: 2000,
                    ..
                },
                ..
            }
        ),
        "expected TransactionReceived(InChainLockedBlock), got {:?}",
        event
    );
}

// ---------------------------------------------------------------------------
// Duplicate suppression tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_duplicate_mempool_emits_no_event() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0x11);

    // First mempool submission
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    assert_single_event(&mut rx);

    // Duplicate mempool submission — no state change, no event
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    assert_no_events(&mut rx);

    // Wallet state: still exactly 1 record for this txid
    let history = manager.wallet_transaction_history(&wallet_id).unwrap();
    assert_eq!(history.iter().filter(|r| r.txid == tx.txid()).count(), 1);
}

#[tokio::test]
async fn test_duplicate_instantsend_emits_no_event() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0x22);

    // Mempool, then IS
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    assert_single_event(&mut rx);

    manager.check_transaction_in_all_wallets(&tx, TransactionContext::InstantSend, true).await;
    assert_single_event(&mut rx);

    // Duplicate IS — no state change, no event
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::InstantSend, true).await;
    assert_no_events(&mut rx);

    // Wallet state: still exactly 1 record
    let history = manager.wallet_transaction_history(&wallet_id).unwrap();
    assert_eq!(history.iter().filter(|r| r.txid == tx.txid()).count(), 1);
}

#[tokio::test]
async fn test_duplicate_confirmed_emits_no_event() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0x33);

    // First seen in block
    let block_ctx = TransactionContext::InBlock {
        height: 300,
        block_hash: Some(BlockHash::from_byte_array([0x33; 32])),
        timestamp: Some(3000),
    };
    manager.check_transaction_in_all_wallets(&tx, block_ctx, true).await;
    assert_single_event(&mut rx);

    // Same block context again — no event
    manager.check_transaction_in_all_wallets(&tx, block_ctx, true).await;
    assert_no_events(&mut rx);

    // Wallet state: still exactly 1 record at height 300
    let history = manager.wallet_transaction_history(&wallet_id).unwrap();
    let records: Vec<_> = history.iter().filter(|r| r.txid == tx.txid()).collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].height, Some(300));
}

#[tokio::test]
async fn test_duplicate_chainlock_emits_no_event() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0x44);

    // Mempool -> Confirmed -> ChainLocked
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    assert_single_event(&mut rx);

    let block_ctx = TransactionContext::InBlock {
        height: 400,
        block_hash: Some(BlockHash::from_byte_array([0x44; 32])),
        timestamp: Some(4000),
    };
    manager.check_transaction_in_all_wallets(&tx, block_ctx, true).await;
    assert_single_event(&mut rx);

    manager.process_chainlock(400);
    assert_single_event(&mut rx);

    // Duplicate chainlock at same height — no event
    manager.process_chainlock(400);
    assert_no_events(&mut rx);

    // Wallet state: still exactly 1 record, still chainlocked
    let history = manager.wallet_transaction_history(&wallet_id).unwrap();
    assert_eq!(history.iter().filter(|r| r.txid == tx.txid()).count(), 1);
    let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
    assert!(info.is_transaction_chainlocked(&tx.txid()));
}

// ---------------------------------------------------------------------------
// Edge case tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_first_seen_as_instantsend_then_duplicate() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0x55);

    // First seen directly as IS (skipping mempool)
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::InstantSend, true).await;
    let event = assert_single_event(&mut rx);
    assert!(
        matches!(
            event,
            WalletEvent::TransactionReceived {
                status: TransactionContext::InstantSend,
                ..
            }
        ),
        "expected TransactionReceived(InstantSend), got {:?}",
        event
    );

    // Duplicate IS should trigger no event
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::InstantSend, true).await;
    assert_no_events(&mut rx);

    // Wallet state: still exactly 1 record
    let history = manager.wallet_transaction_history(&wallet_id).unwrap();
    assert_eq!(history.iter().filter(|r| r.txid == tx.txid()).count(), 1);
}

#[tokio::test]
async fn test_first_seen_in_chainlocked_block_then_process_chainlock() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0x66);

    // First seen in a chainlocked block
    let ctx = TransactionContext::InChainLockedBlock {
        height: 700,
        block_hash: Some(BlockHash::from_byte_array([0x66; 32])),
        timestamp: Some(7000),
    };
    manager.check_transaction_in_all_wallets(&tx, ctx, true).await;
    let event = assert_single_event(&mut rx);
    assert!(matches!(
        event,
        WalletEvent::TransactionReceived {
            status: TransactionContext::InChainLockedBlock { .. },
            ..
        }
    ));

    // process_chainlock at the same height — tx is already in the chainlock set
    manager.process_chainlock(700);
    assert_no_events(&mut rx);

    // Wallet state: tx remains chainlocked with correct height
    let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
    assert!(info.is_transaction_chainlocked(&tx.txid()));
    let history = manager.wallet_transaction_history(&wallet_id).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].height, Some(700));
}

#[tokio::test]
async fn test_late_instantsend_after_confirmation_is_ignored() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0x77);

    // Mempool
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    assert_single_event(&mut rx);

    // Confirmed in block
    let block_ctx = TransactionContext::InBlock {
        height: 800,
        block_hash: Some(BlockHash::from_byte_array([0x77; 32])),
        timestamp: Some(8000),
    };
    manager.check_transaction_in_all_wallets(&tx, block_ctx, true).await;
    assert_single_event(&mut rx);

    // Late IS lock arrives after confirmation — should be suppressed
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::InstantSend, true).await;
    assert_no_events(&mut rx);

    // Wallet state: tx still confirmed at height 800, not regressed to IS
    let history = manager.wallet_transaction_history(&wallet_id).unwrap();
    let records: Vec<_> = history.iter().filter(|r| r.txid == tx.txid()).collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].height, Some(800));
}

#[tokio::test]
async fn test_instantsend_to_confirmed_to_chainlocked() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0x88);
    let txid = tx.txid();

    // First seen as IS (skipping mempool)
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::InstantSend, true).await;
    let event = assert_single_event(&mut rx);
    assert!(matches!(
        event,
        WalletEvent::TransactionReceived {
            status: TransactionContext::InstantSend,
            ..
        }
    ));

    // Confirmed in block
    let block_ctx = TransactionContext::InBlock {
        height: 900,
        block_hash: Some(BlockHash::from_byte_array([0x88; 32])),
        timestamp: Some(9000),
    };
    manager.check_transaction_in_all_wallets(&tx, block_ctx, true).await;
    let event = assert_single_event(&mut rx);
    assert!(matches!(
        event,
        WalletEvent::TransactionStatusChanged {
            status: TransactionContext::InBlock {
                height: 900,
                ..
            },
            ..
        }
    ));

    // Chainlocked
    manager.process_chainlock(900);
    let event = assert_single_event(&mut rx);
    assert!(
        matches!(
            &event,
            WalletEvent::TransactionStatusChanged {
                txid: event_txid,
                status: TransactionContext::InChainLockedBlock { height: 900, .. },
            } if *event_txid == txid
        ),
        "expected TransactionStatusChanged(InChainLockedBlock), got {:?}",
        event
    );

    // Wallet state: tx confirmed at height 900 and chainlocked
    let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
    assert!(info.is_transaction_chainlocked(&txid));
    let history = manager.wallet_transaction_history(&wallet_id).unwrap();
    let records: Vec<_> = history.iter().filter(|r| r.txid == txid).collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].height, Some(900));
}

#[tokio::test]
async fn test_chainlock_via_check_transaction_deduplicates_with_notify() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0x99);

    // Mempool -> Confirmed
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    assert_single_event(&mut rx);

    let block_ctx = TransactionContext::InBlock {
        height: 1100,
        block_hash: Some(BlockHash::from_byte_array([0x99; 32])),
        timestamp: Some(11000),
    };
    manager.check_transaction_in_all_wallets(&tx, block_ctx, true).await;
    assert_single_event(&mut rx);

    // Chainlock arrives via check_transaction_in_all_wallets
    let cl_ctx = TransactionContext::InChainLockedBlock {
        height: 1100,
        block_hash: Some(BlockHash::from_byte_array([0x99; 32])),
        timestamp: Some(11000),
    };
    manager.check_transaction_in_all_wallets(&tx, cl_ctx, true).await;
    assert_single_event(&mut rx);

    // process_chainlock at the same height — already handled, no event
    manager.process_chainlock(1100);
    assert_no_events(&mut rx);

    // Wallet state: tx chainlocked at height 1100
    let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
    assert!(info.is_transaction_chainlocked(&tx.txid()));
    let history = manager.wallet_transaction_history(&wallet_id).unwrap();
    assert_eq!(history.iter().filter(|r| r.txid == tx.txid()).count(), 1);
}

#[tokio::test]
async fn test_mempool_after_instantsend_is_suppressed() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0xab);

    // Mempool first
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    assert_single_event(&mut rx);

    // IS lock
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::InstantSend, true).await;
    assert_single_event(&mut rx);

    // Mempool re-broadcast — should be suppressed
    manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;
    assert_no_events(&mut rx);

    // Wallet state: still exactly 1 record
    let history = manager.wallet_transaction_history(&wallet_id).unwrap();
    assert_eq!(history.iter().filter(|r| r.txid == tx.txid()).count(), 1);
}

// ---------------------------------------------------------------------------
// BalanceUpdated event tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_mempool_tx_emits_balance_updated() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0xf1);

    manager.process_mempool_transaction(&tx, false).await;

    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(
            e,
            WalletEvent::BalanceUpdated {
                wallet_id: wid,
                unconfirmed,
                ..
            } if *wid == wallet_id && *unconfirmed == TX_AMOUNT
        )),
        "expected BalanceUpdated with unconfirmed={TX_AMOUNT}, got {:?}",
        events
    );
}

#[tokio::test]
async fn test_instantsend_tx_emits_balance_updated_spendable() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0xf2);

    manager.process_mempool_transaction(&tx, true).await;

    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(
            e,
            WalletEvent::BalanceUpdated {
                wallet_id: wid,
                spendable,
                ..
            } if *wid == wallet_id && *spendable == TX_AMOUNT
        )),
        "expected BalanceUpdated with spendable={TX_AMOUNT}, got {:?}",
        events
    );
}

#[tokio::test]
async fn test_mempool_to_instantsend_transitions_balance() {
    let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
    let mut rx = manager.subscribe_events();
    let tx = create_tx_paying_to(&addr, 0xf3);

    // Mempool tx: balance should be unconfirmed
    manager.process_mempool_transaction(&tx, false).await;
    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(
            e,
            WalletEvent::BalanceUpdated {
                wallet_id: wid,
                unconfirmed,
                spendable,
                ..
            } if *wid == wallet_id && *unconfirmed == TX_AMOUNT && *spendable == 0
        )),
        "expected unconfirmed balance after mempool, got {:?}",
        events
    );

    // IS lock: balance should move from unconfirmed to spendable
    manager.process_instant_send_lock(tx.txid());
    let events = drain_events(&mut rx);
    assert!(
        events.iter().any(|e| matches!(
            e,
            WalletEvent::BalanceUpdated {
                wallet_id: wid,
                spendable,
                unconfirmed,
                ..
            } if *wid == wallet_id && *spendable == TX_AMOUNT && *unconfirmed == 0
        )),
        "expected spendable balance after IS lock, got {:?}",
        events
    );
}

#[tokio::test]
async fn test_check_transaction_populates_totals() {
    let (mut manager, _wallet_id, addr) = setup_manager_with_wallet();

    let tx = create_tx_paying_to(&addr, 0xf0);
    let result =
        manager.check_transaction_in_all_wallets(&tx, TransactionContext::Mempool, true).await;

    assert!(!result.affected_wallets.is_empty());
    assert_eq!(result.total_received, TX_AMOUNT);
    assert_eq!(result.total_sent, 0);
    assert!(
        !result.involved_addresses.is_empty(),
        "involved_addresses should contain the target address"
    );
    assert!(
        result.involved_addresses.contains(&addr),
        "involved_addresses should contain the target address"
    );
}
