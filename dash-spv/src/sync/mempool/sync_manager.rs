use crate::error::SyncResult;
use crate::network::{Message, MessageType, NetworkEvent, RequestSender};
use crate::sync::{
    ManagerIdentifier, MempoolManager, SyncEvent, SyncManager, SyncManagerProgress, SyncState,
};
use async_trait::async_trait;
use dashcore::network::message::NetworkMessage;
use key_wallet_manager::wallet_interface::WalletInterface;

#[async_trait]
impl<W: WalletInterface + 'static> SyncManager for MempoolManager<W> {
    fn identifier(&self) -> ManagerIdentifier {
        ManagerIdentifier::Mempool
    }

    fn state(&self) -> SyncState {
        self.progress.state()
    }

    fn set_state(&mut self, state: SyncState) {
        self.progress.set_state(state);
    }

    fn clear_in_flight_state(&mut self) {
        self.clear_pending();
    }

    fn wanted_message_types(&self) -> &'static [MessageType] {
        &[MessageType::Inv, MessageType::Tx]
    }

    async fn handle_message(
        &mut self,
        msg: Message,
        requests: &RequestSender,
    ) -> SyncResult<Vec<SyncEvent>> {
        match msg.inner() {
            NetworkMessage::Inv(inv) => self.handle_inv(inv, msg.peer_address(), requests),
            NetworkMessage::Tx(tx) => self.handle_tx(tx.clone()).await,
            _ => Ok(vec![]),
        }
    }

    async fn handle_sync_event(
        &mut self,
        event: &SyncEvent,
        requests: &RequestSender,
    ) -> SyncResult<Vec<SyncEvent>> {
        match event {
            SyncEvent::SyncComplete {
                ..
            } => {
                if self.state() != SyncState::Synced {
                    // Activate (or re-activate after reconnect) mempool monitoring
                    self.activate(requests).await?;
                    self.set_state(SyncState::Synced);
                    tracing::info!("Mempool manager activated after sync complete");
                }
                Ok(vec![])
            }
            SyncEvent::BlockProcessed {
                new_addresses,
                confirmed_txids,
                ..
            } => {
                // Remove confirmed transactions from mempool
                if !confirmed_txids.is_empty() {
                    self.remove_confirmed(confirmed_txids).await;
                }
                // Rebuild bloom filter if new addresses were discovered
                if !new_addresses.is_empty() && self.state() == SyncState::Synced {
                    self.rebuild_filter(requests).await?;
                }
                Ok(vec![])
            }
            SyncEvent::InstantLockReceived {
                instant_lock,
                ..
            } => {
                self.mark_instant_send(&instant_lock.txid).await;
                Ok(vec![])
            }
            SyncEvent::ChainLockReceived {
                chain_lock,
                ..
            } => {
                self.forward_chainlock(chain_lock.block_height).await;
                Ok(vec![])
            }
            _ => Ok(vec![]),
        }
    }

    async fn handle_network_event(
        &mut self,
        event: &NetworkEvent,
        requests: &RequestSender,
    ) -> SyncResult<Vec<SyncEvent>> {
        match event {
            NetworkEvent::PeerConnected {
                address,
            } => {
                self.handle_peer_connected(*address);
            }
            NetworkEvent::PeerDisconnected {
                address,
            } => {
                self.handle_peer_disconnected(*address);
            }
            NetworkEvent::PeersUpdated {
                connected_count,
                best_height,
                ..
            } => {
                if let Some(best_height) = best_height {
                    self.update_target_height(*best_height);
                }
                if *connected_count == 0 {
                    self.stop_sync();
                } else if self.state() == SyncState::WaitingForConnections {
                    return self.start_sync(requests).await;
                }
            }
        }
        Ok(vec![])
    }

    async fn tick(&mut self, requests: &RequestSender) -> SyncResult<Vec<SyncEvent>> {
        if self.state() != SyncState::Synced {
            return Ok(vec![]);
        }

        // Retry activation if no inventory response arrived within the timeout
        if self.needs_activation_retry() {
            tracing::debug!("Retrying mempool activation (no response within timeout)");
            self.activate(requests).await?;
        }

        // Prune expired transactions periodically
        self.prune_expired().await;

        // Prune pending requests that never received a response
        self.prune_pending_requests();

        // Send queued getdata requests now that slots may have freed up
        self.send_queued(requests)?;

        // Check if bloom filter needs rebuilding
        self.check_filter_staleness(requests).await;

        Ok(vec![])
    }

    fn progress(&self) -> SyncManagerProgress {
        SyncManagerProgress::Mempool(self.progress.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::config::MempoolStrategy;
    use crate::network::NetworkRequest;
    use crate::test_utils::test_socket_address;
    use crate::types::{MempoolState, UnconfirmedTransaction};
    use dashcore::ephemerealdata::chain_lock::ChainLock;
    use dashcore::hashes::Hash;
    use dashcore::{Amount, BlockHash, Transaction, Txid};
    use key_wallet_manager::test_utils::MockWallet;
    use std::sync::Arc;
    use tokio::sync::{mpsc, RwLock};

    fn create_test_manager(
    ) -> (MempoolManager<MockWallet>, RequestSender, mpsc::UnboundedReceiver<NetworkRequest>) {
        let wallet = Arc::new(RwLock::new(MockWallet::new()));
        let mempool_state = Arc::new(RwLock::new(MempoolState::default()));
        let (tx, rx) = mpsc::unbounded_channel::<NetworkRequest>();
        let requests = RequestSender::new(tx);

        let manager = MempoolManager::new(
            wallet,
            mempool_state,
            MempoolStrategy::FetchAll,
            1000,
        );

        (manager, requests, rx)
    }

    #[test]
    fn test_identifier() {
        let (manager, _, _rx) = create_test_manager();
        assert_eq!(manager.identifier(), ManagerIdentifier::Mempool);
    }

    #[test]
    fn test_initial_state() {
        let (manager, _, _rx) = create_test_manager();
        assert_eq!(manager.state(), SyncState::WaitForEvents);
    }

    #[test]
    fn test_wanted_message_types() {
        let (manager, _, _rx) = create_test_manager();
        let types = manager.wanted_message_types();
        assert!(types.contains(&MessageType::Inv));
        assert!(types.contains(&MessageType::Tx));
        assert_eq!(types.len(), 2);
    }

    #[test]
    fn test_set_state() {
        let (mut manager, _, _rx) = create_test_manager();
        manager.set_state(SyncState::Synced);
        assert_eq!(manager.state(), SyncState::Synced);
    }

    #[test]
    fn test_progress_variant() {
        let (manager, _, _rx) = create_test_manager();
        let progress = manager.progress();
        assert!(matches!(progress, SyncManagerProgress::Mempool(_)));
    }

    #[tokio::test]
    async fn test_handle_sync_complete_activates() {
        let (mut manager, requests, _rx) = create_test_manager();

        let event = SyncEvent::SyncComplete {
            header_tip: 1000,
            cycle: 0,
        };

        let events = manager.handle_sync_event(&event, &requests).await.unwrap();
        assert!(events.is_empty());
        assert_eq!(manager.state(), SyncState::Synced);
    }

    #[tokio::test]
    async fn test_handle_sync_complete_subsequent_cycles() {
        let (mut manager, requests, _rx) = create_test_manager();

        // Activate first
        let event0 = SyncEvent::SyncComplete {
            header_tip: 1000,
            cycle: 0,
        };
        manager.handle_sync_event(&event0, &requests).await.unwrap();

        // Subsequent cycles should not change state
        let event1 = SyncEvent::SyncComplete {
            header_tip: 1001,
            cycle: 1,
        };
        let events = manager.handle_sync_event(&event1, &requests).await.unwrap();
        assert!(events.is_empty());
        assert_eq!(manager.state(), SyncState::Synced);
    }

    #[tokio::test]
    async fn test_tick_before_synced() {
        let (mut manager, requests, _rx) = create_test_manager();
        // Before sync complete, tick should do nothing
        let events = manager.tick(&requests).await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_block_processed_removes_confirmed_txids() {
        let (mut manager, requests, _rx) = create_test_manager();

        // Add two transactions to mempool state
        let tx1 = Transaction {
            version: 1,
            lock_time: 0,
            input: vec![],
            output: vec![],
            special_transaction_payload: None,
        };
        let tx2 = Transaction {
            version: 1,
            lock_time: 1,
            input: vec![],
            output: vec![],
            special_transaction_payload: None,
        };
        let txid1 = tx1.txid();
        let txid2 = tx2.txid();
        {
            let mut state = manager.mempool_state.write().await;
            state.add_transaction(UnconfirmedTransaction::new(
                tx1,
                Amount::from_sat(0),
                false,
                false,
                Vec::new(),
                0,
            ));
            state.add_transaction(UnconfirmedTransaction::new(
                tx2,
                Amount::from_sat(0),
                false,
                false,
                Vec::new(),
                0,
            ));
        }

        let event = SyncEvent::BlockProcessed {
            block_hash: BlockHash::all_zeros(),
            height: 100,
            new_addresses: vec![],
            confirmed_txids: vec![txid1],
        };

        let events = manager.handle_sync_event(&event, &requests).await.unwrap();
        assert!(events.is_empty());

        let state = manager.mempool_state.read().await;
        assert!(!state.transactions.contains_key(&txid1));
        assert!(state.transactions.contains_key(&txid2));
        assert_eq!(manager.progress.removed(), 1);
    }

    #[tokio::test]
    async fn test_block_processed_with_unknown_txids() {
        let (mut manager, requests, _rx) = create_test_manager();

        // Add one transaction to mempool
        let tx = Transaction {
            version: 1,
            lock_time: 0,
            input: vec![],
            output: vec![],
            special_transaction_payload: None,
        };
        let txid = tx.txid();
        let unknown_txid = Txid::from_byte_array([0xaa; 32]);
        {
            let mut state = manager.mempool_state.write().await;
            state.add_transaction(UnconfirmedTransaction::new(
                tx,
                Amount::from_sat(0),
                false,
                false,
                Vec::new(),
                0,
            ));
        }

        // Confirm with a mix of known and unknown txids
        let event = SyncEvent::BlockProcessed {
            block_hash: BlockHash::all_zeros(),
            height: 100,
            new_addresses: vec![],
            confirmed_txids: vec![unknown_txid, txid],
        };

        let events = manager.handle_sync_event(&event, &requests).await.unwrap();
        assert!(events.is_empty());

        let state = manager.mempool_state.read().await;
        assert!(state.transactions.is_empty());
        assert_eq!(manager.progress.removed(), 1);
    }

    #[tokio::test]
    async fn test_block_processed_empty_confirmed_txids() {
        let (mut manager, requests, _rx) = create_test_manager();

        // Add a transaction to mempool
        let tx = Transaction {
            version: 1,
            lock_time: 0,
            input: vec![],
            output: vec![],
            special_transaction_payload: None,
        };
        {
            let mut state = manager.mempool_state.write().await;
            state.add_transaction(UnconfirmedTransaction::new(
                tx,
                Amount::from_sat(0),
                false,
                false,
                Vec::new(),
                0,
            ));
        }

        // Empty confirmed_txids should not remove anything
        let event = SyncEvent::BlockProcessed {
            block_hash: BlockHash::all_zeros(),
            height: 100,
            new_addresses: vec![],
            confirmed_txids: vec![],
        };

        manager.handle_sync_event(&event, &requests).await.unwrap();

        let state = manager.mempool_state.read().await;
        assert_eq!(state.transactions.len(), 1);
        assert_eq!(manager.progress.removed(), 0);
    }

    #[tokio::test]
    async fn test_chainlock_received_notifies_wallet() {
        let (mut manager, requests, _rx) = create_test_manager();

        let event = SyncEvent::ChainLockReceived {
            chain_lock: ChainLock {
                block_height: 1000,
                block_hash: BlockHash::from_byte_array([0; 32]),
                signature: [0u8; 96].into(),
            },
            validated: true,
        };

        let events = manager.handle_sync_event(&event, &requests).await.unwrap();
        assert!(events.is_empty());

        let wallet = manager.wallet.read().await;
        let notifications = wallet.chainlock_notifications();
        let notifications = notifications.lock().await;
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0], 1000);
    }

    #[tokio::test]
    async fn test_reactivation_after_disconnect() {
        let (mut manager, requests, _rx) = create_test_manager();

        // Initial activation
        let event = SyncEvent::SyncComplete {
            header_tip: 1000,
            cycle: 0,
        };
        manager.handle_sync_event(&event, &requests).await.unwrap();
        assert_eq!(manager.state(), SyncState::Synced);

        // Simulate disconnect by resetting state
        manager.set_state(SyncState::WaitForEvents);

        // Re-sync should re-activate
        let event = SyncEvent::SyncComplete {
            header_tip: 1001,
            cycle: 1,
        };
        manager.handle_sync_event(&event, &requests).await.unwrap();
        assert_eq!(manager.state(), SyncState::Synced);
    }

    #[tokio::test]
    async fn test_network_event_peer_connect_disconnect() {
        let (mut manager, requests, _rx) = create_test_manager();

        let peer1 = test_socket_address(1);
        let peer2 = test_socket_address(2);

        // Connect and disconnect should not error
        let connect1 = NetworkEvent::PeerConnected {
            address: peer1,
        };
        manager.handle_network_event(&connect1, &requests).await.unwrap();
        let connect2 = NetworkEvent::PeerConnected {
            address: peer2,
        };
        manager.handle_network_event(&connect2, &requests).await.unwrap();

        let disconnect1 = NetworkEvent::PeerDisconnected {
            address: peer1,
        };
        manager.handle_network_event(&disconnect1, &requests).await.unwrap();

        // Disconnecting an already-disconnected peer should not error
        manager.handle_network_event(&disconnect1, &requests).await.unwrap();
    }
}
