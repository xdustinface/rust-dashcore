use crate::{
    BlockProcessingResult, MempoolTransactionResult, WalletEvent, WalletId, WalletInterface,
};
use dashcore::ephemerealdata::instant_lock::InstantLock;
use dashcore::prelude::CoreBlockHeight;
use dashcore::{Address, Block, OutPoint, Transaction, Txid};
use key_wallet::transaction_checking::TransactionContext;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};

// Type alias for captured IS lock payloads
type InstantLockCaptures = Arc<Mutex<Vec<(Txid, Option<InstantLock>)>>>;

/// Default wallet ID used by `MockWallet` and `NonMatchingMockWallet` for tests
/// that don't care about per-wallet attribution.
pub const MOCK_WALLET_ID: WalletId = [0u8; 32];

pub struct MockWallet {
    wallet_id: WalletId,
    processed_blocks: Arc<Mutex<Vec<(dashcore::BlockHash, u32)>>>,
    processed_transactions: Arc<Mutex<Vec<dashcore::Txid>>>,
    last_processed_height: CoreBlockHeight,
    synced_height: CoreBlockHeight,
    event_sender: broadcast::Sender<WalletEvent>,
    /// When true, process_mempool_transaction returns is_relevant=true.
    mempool_relevant: bool,
    /// Addresses returned by monitored_addresses.
    addresses: Vec<Address>,
    /// Outpoints returned by watched_outpoints.
    outpoints: Vec<OutPoint>,
    /// Net amount returned by process_mempool_transaction.
    mempool_net_amount: i64,
    /// Addresses returned by process_mempool_transaction.
    mempool_addresses: Vec<Address>,
    /// New addresses returned by process_mempool_transaction.
    mempool_new_addresses: Vec<Address>,
    /// Recorded status change notifications for test assertions.
    status_changes: Arc<Mutex<Vec<(Txid, TransactionContext)>>>,
    /// Captured `InstantLock` payloads from both `process_mempool_transaction` and
    /// `process_instant_send_lock`, for test assertions.
    pub processed_instant_locks: InstantLockCaptures,
    /// Monitor revision counter for staleness detection.
    monitor_revision: u64,
}

impl Default for MockWallet {
    fn default() -> Self {
        Self::new()
    }
}

impl MockWallet {
    pub fn new() -> Self {
        let (event_sender, _) = broadcast::channel(16);
        Self {
            wallet_id: MOCK_WALLET_ID,
            processed_blocks: Arc::new(Mutex::new(Vec::new())),
            processed_transactions: Arc::new(Mutex::new(Vec::new())),
            last_processed_height: 0,
            synced_height: 0,
            event_sender,
            mempool_relevant: false,
            addresses: Vec::new(),
            outpoints: Vec::new(),
            mempool_net_amount: 0,
            mempool_addresses: Vec::new(),
            mempool_new_addresses: Vec::new(),
            status_changes: Arc::new(Mutex::new(Vec::new())),
            processed_instant_locks: Arc::new(Mutex::new(Vec::new())),
            monitor_revision: 0,
        }
    }

    /// Override the wallet id used for per-wallet API surfaces.
    pub fn set_wallet_id(&mut self, wallet_id: WalletId) {
        self.wallet_id = wallet_id;
    }

    /// Configure whether mempool transactions are reported as relevant.
    pub fn set_mempool_relevant(&mut self, relevant: bool) {
        self.mempool_relevant = relevant;
    }

    /// Set the addresses returned by monitored_addresses.
    pub fn set_addresses(&mut self, addresses: Vec<Address>) {
        self.addresses = addresses;
        self.monitor_revision += 1;
    }

    /// Set the outpoints returned by watched_outpoints.
    pub fn set_outpoints(&mut self, outpoints: Vec<OutPoint>) {
        self.outpoints = outpoints;
        self.monitor_revision += 1;
    }

    /// Set the net amount returned by process_mempool_transaction.
    pub fn set_mempool_net_amount(&mut self, amount: i64) {
        self.mempool_net_amount = amount;
    }

    /// Set the addresses returned by process_mempool_transaction.
    pub fn set_mempool_addresses(&mut self, addresses: Vec<Address>) {
        self.mempool_addresses = addresses;
    }

    /// Set new addresses returned by process_mempool_transaction.
    pub fn set_mempool_new_addresses(&mut self, addresses: Vec<Address>) {
        self.mempool_new_addresses = addresses;
    }

    pub fn status_changes(&self) -> Arc<Mutex<Vec<(Txid, TransactionContext)>>> {
        self.status_changes.clone()
    }

    pub fn processed_blocks(&self) -> Arc<Mutex<Vec<(dashcore::BlockHash, u32)>>> {
        self.processed_blocks.clone()
    }

    pub fn processed_transactions(&self) -> Arc<Mutex<Vec<dashcore::Txid>>> {
        self.processed_transactions.clone()
    }
}

#[async_trait::async_trait]
impl WalletInterface for MockWallet {
    async fn process_block_for_wallets(
        &mut self,
        block: &Block,
        height: u32,
        wallets: &BTreeSet<WalletId>,
    ) -> BlockProcessingResult {
        if !wallets.contains(&self.wallet_id) {
            return BlockProcessingResult::default();
        }
        let mut processed = self.processed_blocks.lock().await;
        processed.push((block.block_hash(), height));
        if height > self.last_processed_height {
            self.last_processed_height = height;
        }

        BlockProcessingResult {
            new_txids: block.txdata.iter().map(|tx| tx.txid()).collect(),
            existing_txids: Vec::new(),
            new_addresses: Default::default(),
        }
    }

    async fn process_mempool_transaction(
        &mut self,
        tx: &Transaction,
        instant_lock: Option<InstantLock>,
    ) -> MempoolTransactionResult {
        let mut processed = self.processed_transactions.lock().await;
        processed.push(tx.txid());

        let mut locks = self.processed_instant_locks.lock().await;
        locks.push((tx.txid(), instant_lock));
        drop(locks);

        if !self.mempool_relevant {
            return MempoolTransactionResult::default();
        }

        MempoolTransactionResult {
            is_relevant: true,
            net_amount: self.mempool_net_amount,
            is_outgoing: self.mempool_net_amount < 0,
            addresses: self.mempool_addresses.clone(),
            new_addresses: self.mempool_new_addresses.clone(),
        }
    }

    async fn describe(&self) -> String {
        "MockWallet (test implementation)".to_string()
    }

    fn monitored_addresses(&self) -> Vec<Address> {
        self.addresses.clone()
    }

    fn monitored_addresses_for(&self, wallet_id: &WalletId) -> Vec<Address> {
        if wallet_id == &self.wallet_id {
            self.addresses.clone()
        } else {
            Vec::new()
        }
    }

    fn watched_outpoints(&self) -> Vec<OutPoint> {
        self.outpoints.clone()
    }

    fn last_processed_height(&self) -> CoreBlockHeight {
        self.last_processed_height
    }

    fn synced_height(&self) -> CoreBlockHeight {
        self.synced_height
    }

    fn wallets_behind(&self, height: CoreBlockHeight) -> BTreeSet<WalletId> {
        if self.synced_height < height {
            BTreeSet::from([self.wallet_id])
        } else {
            BTreeSet::new()
        }
    }

    fn wallet_synced_height(&self, wallet_id: &WalletId) -> CoreBlockHeight {
        if wallet_id == &self.wallet_id {
            self.synced_height
        } else {
            0
        }
    }

    fn update_wallet_synced_height(&mut self, wallet_id: &WalletId, height: CoreBlockHeight) {
        if wallet_id == &self.wallet_id && height > self.synced_height {
            self.synced_height = height;
        }
    }

    fn update_wallet_last_processed_height(
        &mut self,
        wallet_id: &WalletId,
        height: CoreBlockHeight,
    ) {
        if wallet_id == &self.wallet_id && height > self.last_processed_height {
            self.last_processed_height = height;
        }
    }

    fn monitor_revision(&self) -> u64 {
        self.monitor_revision
    }

    fn subscribe_events(&self) -> broadcast::Receiver<WalletEvent> {
        self.event_sender.subscribe()
    }

    fn process_instant_send_lock(&mut self, instant_lock: InstantLock) {
        let txid = instant_lock.txid;
        let mut locks = self
            .processed_instant_locks
            .try_lock()
            .expect("processed_instant_locks lock contention in test helper");
        locks.push((txid, Some(instant_lock.clone())));
        drop(locks);

        let mut changes =
            self.status_changes.try_lock().expect("status_changes lock contention in test helper");
        changes.push((txid, TransactionContext::InstantSend(instant_lock)));
    }
}

/// Mock wallet that returns false for filter checks
pub struct NonMatchingMockWallet {
    wallet_id: WalletId,
    last_processed_height: CoreBlockHeight,
    synced_height: CoreBlockHeight,
    event_sender: broadcast::Sender<WalletEvent>,
}

impl Default for NonMatchingMockWallet {
    fn default() -> Self {
        Self::new()
    }
}

impl NonMatchingMockWallet {
    pub fn new() -> Self {
        let (event_sender, _) = broadcast::channel(16);
        Self {
            wallet_id: MOCK_WALLET_ID,
            last_processed_height: 0,
            synced_height: 0,
            event_sender,
        }
    }
}

#[async_trait::async_trait]
impl WalletInterface for NonMatchingMockWallet {
    async fn process_block_for_wallets(
        &mut self,
        _block: &Block,
        height: u32,
        wallets: &BTreeSet<WalletId>,
    ) -> BlockProcessingResult {
        if wallets.contains(&self.wallet_id) && height > self.last_processed_height {
            self.last_processed_height = height;
        }
        BlockProcessingResult::default()
    }

    async fn process_mempool_transaction(
        &mut self,
        _tx: &Transaction,
        _instant_lock: Option<InstantLock>,
    ) -> MempoolTransactionResult {
        MempoolTransactionResult::default()
    }

    fn monitored_addresses(&self) -> Vec<Address> {
        Vec::new()
    }

    fn monitored_addresses_for(&self, _wallet_id: &WalletId) -> Vec<Address> {
        Vec::new()
    }

    fn watched_outpoints(&self) -> Vec<OutPoint> {
        Vec::new()
    }

    fn last_processed_height(&self) -> CoreBlockHeight {
        self.last_processed_height
    }

    fn synced_height(&self) -> CoreBlockHeight {
        self.synced_height
    }

    fn wallets_behind(&self, height: CoreBlockHeight) -> BTreeSet<WalletId> {
        if self.synced_height < height {
            BTreeSet::from([self.wallet_id])
        } else {
            BTreeSet::new()
        }
    }

    fn wallet_synced_height(&self, wallet_id: &WalletId) -> CoreBlockHeight {
        if wallet_id == &self.wallet_id {
            self.synced_height
        } else {
            0
        }
    }

    fn update_wallet_synced_height(&mut self, wallet_id: &WalletId, height: CoreBlockHeight) {
        if wallet_id == &self.wallet_id && height > self.synced_height {
            self.synced_height = height;
        }
    }

    fn update_wallet_last_processed_height(
        &mut self,
        wallet_id: &WalletId,
        height: CoreBlockHeight,
    ) {
        if wallet_id == &self.wallet_id && height > self.last_processed_height {
            self.last_processed_height = height;
        }
    }

    fn subscribe_events(&self) -> broadcast::Receiver<WalletEvent> {
        self.event_sender.subscribe()
    }

    async fn describe(&self) -> String {
        "NonMatchingWallet (test implementation)".to_string()
    }
}

/// Per-wallet state held inside `MultiMockWallet`.
#[derive(Default)]
pub struct MockWalletState {
    pub addresses: Vec<Address>,
    pub synced_height: CoreBlockHeight,
    pub last_processed_height: CoreBlockHeight,
}

#[derive(Debug, Clone)]
pub struct MockPendingRange {
    pub pool: key_wallet::managed_account::address_pool::AddressPoolType,
    pub indexes: core::ops::Range<u32>,
    pub since_height: CoreBlockHeight,
    pub addresses: Vec<Address>,
    pub caught_up_to: Option<CoreBlockHeight>,
}

impl MockPendingRange {
    pub fn is_complete(&self) -> bool {
        match self.caught_up_to {
            Some(c) => c + 1 >= self.since_height,
            None => self.since_height == 0,
        }
    }
}

/// Multi-wallet mock that holds independent state for several wallet IDs,
/// enabling tests that exercise per-wallet attribution paths.
pub struct MultiMockWallet {
    wallets: std::collections::BTreeMap<WalletId, MockWalletState>,
    event_sender: broadcast::Sender<WalletEvent>,
    /// Track every block processed for assertions.
    processed: Arc<Mutex<Vec<(WalletId, dashcore::BlockHash, u32)>>>,
    /// Per-wallet birth heights for backfill tests; defaults to 0 when
    /// not explicitly set.
    birth_heights: std::collections::BTreeMap<WalletId, CoreBlockHeight>,
    /// Per-wallet pending sync ranges for backfill tests. Built directly
    /// without a real `ManagedWalletInfo` so the routing path can be
    /// exercised in isolation.
    pending_ranges: std::collections::BTreeMap<WalletId, Vec<MockPendingRange>>,
}

impl Default for MultiMockWallet {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiMockWallet {
    pub fn new() -> Self {
        let (event_sender, _) = broadcast::channel(16);
        Self {
            wallets: std::collections::BTreeMap::new(),
            event_sender,
            processed: Arc::new(Mutex::new(Vec::new())),
            birth_heights: std::collections::BTreeMap::new(),
            pending_ranges: std::collections::BTreeMap::new(),
        }
    }

    pub fn set_birth_height(&mut self, wallet_id: WalletId, height: CoreBlockHeight) {
        self.birth_heights.insert(wallet_id, height);
    }

    /// Insert or replace a wallet's state.
    pub fn insert_wallet(&mut self, wallet_id: WalletId, state: MockWalletState) {
        self.wallets.insert(wallet_id, state);
    }

    /// Mutable access to a wallet's state, panicking if absent.
    pub fn wallet_mut(&mut self, wallet_id: &WalletId) -> &mut MockWalletState {
        self.wallets.get_mut(wallet_id).expect("wallet present")
    }

    pub fn processed(&self) -> Arc<Mutex<Vec<(WalletId, dashcore::BlockHash, u32)>>> {
        self.processed.clone()
    }

    /// Push a pending sync range onto a wallet for the backfill worker
    /// to discover. Tests use this instead of mutating real address
    /// pools.
    pub fn push_sync_range_for_test(
        &mut self,
        wallet_id: WalletId,
        pool: key_wallet::managed_account::address_pool::AddressPoolType,
        indexes: core::ops::Range<u32>,
        since_height: CoreBlockHeight,
        addresses: Vec<Address>,
    ) {
        self.pending_ranges.entry(wallet_id).or_default().push(MockPendingRange {
            pool,
            indexes,
            since_height,
            addresses,
            caught_up_to: None,
        });
    }

    pub fn pending_ranges_for(&self, wallet_id: &WalletId) -> Vec<MockPendingRange> {
        self.pending_ranges.get(wallet_id).cloned().unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl WalletInterface for MultiMockWallet {
    async fn process_block_for_wallets(
        &mut self,
        block: &Block,
        height: CoreBlockHeight,
        wallets: &BTreeSet<WalletId>,
    ) -> BlockProcessingResult {
        let hash = block.block_hash();
        let mut processed = self.processed.lock().await;
        for wallet_id in wallets {
            if let Some(state) = self.wallets.get_mut(wallet_id) {
                processed.push((*wallet_id, hash, height));
                if height > state.last_processed_height {
                    state.last_processed_height = height;
                }
            }
        }
        BlockProcessingResult::default()
    }

    async fn process_mempool_transaction(
        &mut self,
        _tx: &Transaction,
        _instant_lock: Option<InstantLock>,
    ) -> MempoolTransactionResult {
        MempoolTransactionResult::default()
    }

    fn monitored_addresses(&self) -> Vec<Address> {
        self.wallets.values().flat_map(|s| s.addresses.iter().cloned()).collect()
    }

    fn monitored_addresses_for(&self, wallet_id: &WalletId) -> Vec<Address> {
        self.wallets.get(wallet_id).map(|s| s.addresses.clone()).unwrap_or_default()
    }

    fn watched_outpoints(&self) -> Vec<OutPoint> {
        Vec::new()
    }

    fn last_processed_height(&self) -> CoreBlockHeight {
        self.wallets.values().map(|s| s.last_processed_height).max().unwrap_or(0)
    }

    fn synced_height(&self) -> CoreBlockHeight {
        self.wallets.values().map(|s| s.synced_height).min().unwrap_or(0)
    }

    fn wallets_behind(&self, height: CoreBlockHeight) -> BTreeSet<WalletId> {
        self.wallets
            .iter()
            .filter_map(|(id, s)| {
                if s.synced_height < height {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect()
    }

    fn wallet_synced_height(&self, wallet_id: &WalletId) -> CoreBlockHeight {
        self.wallets.get(wallet_id).map(|s| s.synced_height).unwrap_or(0)
    }

    fn update_wallet_synced_height(&mut self, wallet_id: &WalletId, height: CoreBlockHeight) {
        if let Some(state) = self.wallets.get_mut(wallet_id) {
            if height > state.synced_height {
                state.synced_height = height;
            }
        }
    }

    fn update_wallet_last_processed_height(
        &mut self,
        wallet_id: &WalletId,
        height: CoreBlockHeight,
    ) {
        if let Some(state) = self.wallets.get_mut(wallet_id) {
            if height > state.last_processed_height {
                state.last_processed_height = height;
            }
        }
    }

    fn subscribe_events(&self) -> broadcast::Receiver<WalletEvent> {
        self.event_sender.subscribe()
    }

    fn pending_rescans(&self) -> Vec<crate::PendingRescan> {
        let mut out = Vec::new();
        for (wallet_id, ranges) in &self.pending_ranges {
            let birth = self.birth_heights.get(wallet_id).copied().unwrap_or(0);
            for range in ranges {
                if range.is_complete() {
                    continue;
                }
                let ceiling = range.since_height.saturating_sub(1);
                let resume_from = range
                    .caught_up_to
                    .map(|c| c.saturating_add(1).max(birth))
                    .unwrap_or(birth);
                if resume_from > ceiling {
                    continue;
                }
                out.push(crate::PendingRescan {
                    wallet_id: *wallet_id,
                    pool: range.pool,
                    indexes: range.indexes.clone(),
                    addresses: range.addresses.clone(),
                    floor: birth,
                    ceiling,
                    resume_from,
                });
            }
        }
        out
    }

    fn advance_rescan(
        &mut self,
        wallet_id: &WalletId,
        pool: key_wallet::managed_account::address_pool::AddressPoolType,
        indexes: core::ops::Range<u32>,
        scanned_through: CoreBlockHeight,
    ) {
        let Some(ranges) = self.pending_ranges.get_mut(wallet_id) else {
            return;
        };
        for range in ranges.iter_mut() {
            if range.pool == pool && range.indexes == indexes {
                let cap = range.since_height.saturating_sub(1);
                let new = scanned_through.min(cap);
                if range.caught_up_to.map(|c| new > c).unwrap_or(true) {
                    range.caught_up_to = Some(new);
                }
            }
        }
        ranges.retain(|r| !r.is_complete());
    }

    async fn process_backfill_block_for_wallets(
        &mut self,
        block: &Block,
        height: CoreBlockHeight,
        advances: &[crate::BackfillAdvance],
    ) -> BlockProcessingResult {
        let hash = block.block_hash();
        {
            let mut processed = self.processed.lock().await;
            for advance in advances {
                processed.push((advance.wallet_id, hash, height));
            }
        }
        for advance in advances {
            let _ = self.event_sender.send(WalletEvent::RescanBlockProcessed {
                wallet_id: advance.wallet_id,
                height,
                pool: advance.pool,
                indexes: advance.indexes.clone(),
                advance_to: advance.advance_to,
                inserted: Vec::new(),
                updated: Vec::new(),
                matured: Vec::new(),
                balance: key_wallet::WalletCoreBalance::default(),
                account_balances: BTreeMap::new(),
                addresses_derived: Vec::new(),
            });
            self.advance_rescan(
                &advance.wallet_id,
                advance.pool,
                advance.indexes.clone(),
                advance.advance_to,
            );
        }
        BlockProcessingResult::default()
    }

    async fn describe(&self) -> String {
        "MultiMockWallet (test implementation)".to_string()
    }
}
