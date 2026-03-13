use crate::wallet_interface::{BlockProcessingResult, MempoolTransactionResult, WalletInterface};
use crate::WalletEvent;
use crate::WalletManager;
use alloc::string::String;
use alloc::vec::Vec;
use async_trait::async_trait;
use core::fmt::Write as _;
use dashcore::prelude::CoreBlockHeight;
use dashcore::{Address, Block, Transaction, Txid};
use key_wallet::transaction_checking::transaction_router::TransactionRouter;
use key_wallet::transaction_checking::TransactionContext;
use key_wallet::wallet::managed_wallet_info::wallet_info_interface::WalletInfoInterface;
use std::collections::HashSet;
use tokio::sync::broadcast;

#[async_trait]
impl<T: WalletInfoInterface + Send + Sync + 'static> WalletInterface for WalletManager<T> {
    async fn process_block(
        &mut self,
        block: &Block,
        height: CoreBlockHeight,
        best_chainlock_height: Option<u32>,
    ) -> BlockProcessingResult {
        let mut result = BlockProcessingResult::default();
        let block_hash = Some(block.block_hash());
        let timestamp = block.header.time;

        // Process each transaction using the base manager
        for tx in &block.txdata {
            let context = if best_chainlock_height.is_some_and(|cl| height <= cl) {
                TransactionContext::InChainLockedBlock {
                    height,
                    block_hash,
                    timestamp: Some(timestamp),
                }
            } else {
                TransactionContext::InBlock {
                    height,
                    block_hash,
                    timestamp: Some(timestamp),
                }
            };

            let check_result = self.check_transaction_in_all_wallets(tx, context, true).await;

            if !check_result.affected_wallets.is_empty() {
                if check_result.is_new_transaction {
                    result.new_txids.push(tx.txid());
                } else {
                    result.existing_txids.push(tx.txid());
                }
            }

            result.new_addresses.extend(check_result.new_addresses);
        }

        self.update_synced_height(height);

        result
    }

    async fn process_mempool_transaction(
        &mut self,
        tx: &Transaction,
        is_instant_send: bool,
    ) -> MempoolTransactionResult {
        let context = if is_instant_send {
            TransactionContext::InstantSend
        } else {
            TransactionContext::Mempool
        };

        // Capture balances before processing so we can detect changes
        let old_balances: Vec<_> =
            self.wallet_infos.iter().map(|(id, info)| (*id, info.balance())).collect();

        let check_result = self
            .check_transaction_in_all_wallets(
                tx, context, true, // update state
            )
            .await;

        let is_relevant = !check_result.affected_wallets.is_empty();
        let net_amount = if is_relevant {
            check_result.total_received as i64 - check_result.total_sent as i64
        } else {
            0
        };

        // Emit BalanceUpdated for any wallets whose balance changed
        if is_relevant {
            for (wallet_id, old_balance) in &old_balances {
                if let Some(info) = self.wallet_infos.get(wallet_id) {
                    let new_balance = info.balance();
                    if *old_balance != new_balance {
                        let event = WalletEvent::BalanceUpdated {
                            wallet_id: *wallet_id,
                            spendable: new_balance.spendable(),
                            unconfirmed: new_balance.unconfirmed(),
                            immature: new_balance.immature(),
                            locked: new_balance.locked(),
                        };
                        let _ = self.event_sender.send(event);
                    }
                }
            }
        }

        MempoolTransactionResult {
            is_relevant,
            net_amount,
            is_outgoing: net_amount < 0,
            addresses: check_result.involved_addresses,
            new_addresses: check_result.new_addresses,
        }
    }

    fn monitored_addresses(&self) -> Vec<Address> {
        self.monitored_addresses()
    }

    fn watched_outpoints(&self) -> Vec<dashcore::OutPoint> {
        self.watched_outpoints()
    }

    async fn transaction_effect(&self, tx: &Transaction) -> Option<(i64, Vec<String>)> {
        // Aggregate across all managed wallets. If any wallet considers it relevant,
        // compute net = total_received - total_sent and collect involved addresses.
        let mut total_received: u64 = 0;
        let mut total_sent: u64 = 0;
        let mut addresses: Vec<String> = Vec::new();

        let mut is_relevant_any = false;
        for info in self.wallet_infos.values() {
            let collection = info.accounts();
            // Reuse the same routing/check logic used in normal processing
            let tx_type = TransactionRouter::classify_transaction(tx);
            let account_types = TransactionRouter::get_relevant_account_types(&tx_type);
            let result = collection.check_transaction(tx, &account_types);

            if result.is_relevant {
                is_relevant_any = true;
                total_received = total_received.saturating_add(result.total_received);
                total_sent = total_sent.saturating_add(result.total_sent);

                // Collect involved addresses from affected accounts
                for account_match in result.affected_accounts {
                    for addr_info in account_match.account_type_match.all_involved_addresses() {
                        addresses.push(addr_info.address.to_string());
                    }
                }
            }
        }

        if is_relevant_any {
            // Deduplicate addresses while preserving order
            let mut seen = alloc::collections::BTreeSet::new();
            addresses.retain(|a| seen.insert(a.clone()));
            let net = (total_received as i64) - (total_sent as i64);
            Some((net, addresses))
        } else {
            None
        }
    }

    async fn earliest_required_height(&self) -> CoreBlockHeight {
        self.wallet_infos.values().map(|info| info.birth_height()).min().unwrap_or(0)
    }

    fn synced_height(&self) -> CoreBlockHeight {
        self.synced_height
    }

    fn update_synced_height(&mut self, height: CoreBlockHeight) {
        self.synced_height = height;

        // Update each wallet and emit BalanceUpdated events if balance changed
        for (wallet_id, info) in self.wallet_infos.iter_mut() {
            let old_balance = info.balance();
            info.update_synced_height(height);
            let new_balance = info.balance();

            // Emit event if balance changed
            #[cfg(feature = "std")]
            if old_balance != new_balance {
                let event = WalletEvent::BalanceUpdated {
                    wallet_id: *wallet_id,
                    spendable: new_balance.spendable(),
                    unconfirmed: new_balance.unconfirmed(),
                    immature: new_balance.immature(),
                    locked: new_balance.locked(),
                };
                let _ = self.event_sender.send(event);
            }
        }
    }

    fn filter_committed_height(&self) -> CoreBlockHeight {
        self.filter_committed_height
    }

    fn update_filter_committed_height(&mut self, height: CoreBlockHeight) {
        self.filter_committed_height = height;
        if height > self.synced_height {
            self.update_synced_height(height);
        }
    }

    fn subscribe_events(&self) -> broadcast::Receiver<WalletEvent> {
        self.event_sender.subscribe()
    }

    fn notify_transaction_status_changed(&self, txid: Txid, status: TransactionContext) {
        let event = WalletEvent::TransactionStatusChanged {
            txid,
            status,
        };
        let _ = self.event_sender().send(event);
    }

    fn process_instant_send_lock(&mut self, txid: Txid) {
        let old_balances: Vec<_> =
            self.wallet_infos.iter().map(|(id, info)| (*id, info.balance())).collect();

        for info in self.wallet_infos.values_mut() {
            info.mark_instant_send_utxos(&txid);
            info.update_balance();
        }

        let event = WalletEvent::TransactionStatusChanged {
            txid,
            status: TransactionContext::InstantSend,
        };
        let _ = self.event_sender().send(event);

        for (wallet_id, old_balance) in &old_balances {
            if let Some(info) = self.wallet_infos.get(wallet_id) {
                let new_balance = info.balance();
                if *old_balance != new_balance {
                    let event = WalletEvent::BalanceUpdated {
                        wallet_id: *wallet_id,
                        spendable: new_balance.spendable(),
                        unconfirmed: new_balance.unconfirmed(),
                        immature: new_balance.immature(),
                        locked: new_balance.locked(),
                    };
                    let _ = self.event_sender().send(event);
                }
            }
        }
    }

    fn process_chainlock(&mut self, height: u32) {
        // Collect (wallet_id, txid, context) triples to avoid borrow conflicts.
        // A txid may appear in multiple accounts/wallets, so we dedup by txid
        // for event emission while marking each owning wallet individually.
        let mut pending = Vec::new();
        for (wallet_id, info) in &self.wallet_infos {
            for account in info.accounts().all_accounts() {
                for record in account.transactions.values() {
                    if let Some(tx_height) = record.height {
                        if tx_height <= height && !info.is_transaction_chainlocked(&record.txid) {
                            pending.push((
                                *wallet_id,
                                record.txid,
                                TransactionContext::InChainLockedBlock {
                                    height: tx_height,
                                    block_hash: record.block_hash,
                                    timestamp: Some(record.timestamp as u32),
                                },
                            ));
                        }
                    }
                }
            }
        }

        let mut emitted = HashSet::new();
        for (wallet_id, txid, context) in &pending {
            if let Some(info) = self.wallet_infos.get_mut(wallet_id) {
                info.mark_transaction_chainlocked(*txid);
            }
            if emitted.insert(*txid) {
                self.notify_transaction_status_changed(*txid, *context);
            }
        }
    }

    async fn describe(&self) -> String {
        let wallet_count = self.wallet_infos.len();
        if wallet_count == 0 {
            return format!("WalletManager: 0 wallets (network {})", self.network);
        }

        let mut details = Vec::with_capacity(wallet_count);
        for (wallet_id, info) in &self.wallet_infos {
            let name = info.name().unwrap_or("unnamed");

            let mut wallet_id_hex = String::with_capacity(wallet_id.len() * 2);
            for byte in wallet_id {
                let _ = write!(&mut wallet_id_hex, "{:02x}", byte);
            }

            let script_count = info.monitored_addresses().len();
            let summary = format!("{} scripts", script_count);

            details.push(format!("{} ({}): {}", name, wallet_id_hex, summary));
        }

        format!(
            "WalletManager: {} wallet(s) on {}\n{}",
            wallet_count,
            self.network,
            details.join("\n")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wallet_manager::WalletId;
    use dashcore::block::{Header, Version};
    use dashcore::hashes::Hash;
    use dashcore::pow::CompactTarget;
    use dashcore::{
        BlockHash, Network, OutPoint, ScriptBuf, TxIn, TxMerkleNode, TxOut, Txid, Witness,
    };
    use key_wallet::wallet::initialization::WalletAccountCreationOptions;
    use key_wallet::wallet::managed_wallet_info::wallet_info_interface::WalletInfoInterface;
    use key_wallet::wallet::managed_wallet_info::ManagedWalletInfo;

    const TEST_MNEMONIC: &str =
        "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

    fn setup_manager_with_wallet() -> (WalletManager<ManagedWalletInfo>, WalletId, Address) {
        let mut manager = WalletManager::new(Network::Testnet);
        let wallet_id = manager
            .create_wallet_from_mnemonic(
                TEST_MNEMONIC,
                "",
                0,
                WalletAccountCreationOptions::Default,
            )
            .unwrap();
        let addresses = manager.monitored_addresses();
        assert!(!addresses.is_empty());
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
                value: 100_000,
                script_pubkey: addr.script_pubkey(),
            }],
            special_transaction_payload: None,
        }
    }

    fn make_block(txdata: Vec<Transaction>) -> Block {
        Block {
            header: Header {
                version: Version::ONE,
                prev_blockhash: BlockHash::from_byte_array([0; 32]),
                merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                time: 1000,
                bits: CompactTarget::from_consensus(0x1d00ffff),
                nonce: 0,
            },
            txdata,
        }
    }

    #[tokio::test]
    async fn test_synced_height() {
        let mut manager: WalletManager<ManagedWalletInfo> = WalletManager::new(Network::Testnet);
        // Initial state
        assert_eq!(manager.synced_height(), 0);
        // Inrease synced height
        manager.update_synced_height(1000);
        assert_eq!(manager.synced_height(), 1000);
        //Increase synced height again
        manager.update_synced_height(5000);
        assert_eq!(manager.synced_height(), 5000);
        // Decrease synced height
        manager.update_synced_height(10);
        assert_eq!(manager.synced_height(), 10);
    }

    #[tokio::test]
    async fn test_process_chainlock_idempotent() {
        let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
        let tx = create_tx_paying_to(&addr, 0xaa);
        let txid = tx.txid();
        let block = make_block(vec![tx]);

        // Process block at height 500 so the wallet has a confirmed tx
        let result = manager.process_block(&block, 500, None).await;
        assert_eq!(result.new_txids, vec![txid]);

        // First chainlock marks the transaction
        manager.process_chainlock(500);
        let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
        assert!(info.is_transaction_chainlocked(&txid));
        let history = manager.wallet_transaction_history(&wallet_id).unwrap();
        assert_eq!(history.len(), 1);

        // Calling again with same height is idempotent
        manager.process_chainlock(500);
        let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
        assert!(info.is_transaction_chainlocked(&txid));
        let history = manager.wallet_transaction_history(&wallet_id).unwrap();
        assert_eq!(history.len(), 1);

        // Calling with lower height is safe
        manager.process_chainlock(300);
        let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
        assert!(info.is_transaction_chainlocked(&txid));
    }

    #[tokio::test]
    async fn test_process_chainlock_incremental() {
        let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
        let tx1 = create_tx_paying_to(&addr, 0xaa);
        let tx2 = create_tx_paying_to(&addr, 0xbb);
        let txid1 = tx1.txid();
        let txid2 = tx2.txid();

        // Process two blocks at different heights
        let block1 = make_block(vec![tx1]);
        let block2 = make_block(vec![tx2]);
        manager.process_block(&block1, 500, None).await;
        manager.process_block(&block2, 600, None).await;

        let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
        assert!(!info.is_transaction_chainlocked(&txid1));
        assert!(!info.is_transaction_chainlocked(&txid2));

        // Chainlock at 500 marks only the first tx
        manager.process_chainlock(500);
        let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
        assert!(info.is_transaction_chainlocked(&txid1));
        assert!(!info.is_transaction_chainlocked(&txid2));

        // Chainlock at 600 also marks the second tx
        manager.process_chainlock(600);
        let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
        assert!(info.is_transaction_chainlocked(&txid1));
        assert!(info.is_transaction_chainlocked(&txid2));
    }

    #[tokio::test]
    async fn test_process_block_uses_chainlocked_context() {
        let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
        let tx = create_tx_paying_to(&addr, 0xcc);
        let txid = tx.txid();
        let block = make_block(vec![tx]);

        // Process block at height 500 with best_chainlock_height=1000 (height <= cl)
        let result = manager.process_block(&block, 500, Some(1000)).await;
        assert_eq!(result.new_txids, vec![txid]);
        assert_eq!(manager.synced_height(), 500);

        // Transaction should be in history and already marked as chainlocked
        let history = manager.wallet_transaction_history(&wallet_id).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].txid, txid);
        assert_eq!(history[0].height, Some(500));

        let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
        assert!(info.is_transaction_chainlocked(&txid));
    }

    #[tokio::test]
    async fn test_process_block_uses_confirmed_context() {
        let (mut manager, wallet_id, addr) = setup_manager_with_wallet();
        let tx = create_tx_paying_to(&addr, 0xdd);
        let txid = tx.txid();
        let block = make_block(vec![tx]);

        // Process block at height 500 with best_chainlock_height=100 (height > cl)
        let result = manager.process_block(&block, 500, Some(100)).await;
        assert_eq!(result.new_txids, vec![txid]);
        assert_eq!(manager.synced_height(), 500);

        // Transaction should be confirmed but not chainlocked
        let history = manager.wallet_transaction_history(&wallet_id).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].txid, txid);
        assert_eq!(history[0].height, Some(500));

        let info = manager.get_all_wallet_infos().get(&wallet_id).unwrap();
        assert!(!info.is_transaction_chainlocked(&txid));
    }

    #[tokio::test]
    async fn test_mempool_transaction_result_contains_wallet_effect_data() {
        let (mut manager, _wallet_id, addr) = setup_manager_with_wallet();
        let tx = create_tx_paying_to(&addr, 0xaa);

        let result = manager.process_mempool_transaction(&tx, false).await;

        assert!(result.is_relevant);
        assert_eq!(result.net_amount, 100_000);
        assert!(!result.is_outgoing);
        assert!(!result.addresses.is_empty());
    }
}
