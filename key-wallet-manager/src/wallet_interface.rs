//! Wallet interface for SPV client integration
//!
//! This module defines the trait that SPV clients use to interact with wallets.

use crate::{PendingRescan, WalletEvent, WalletId};
use async_trait::async_trait;
use core::ops::Range;
use dashcore::ephemerealdata::instant_lock::InstantLock;
use dashcore::prelude::CoreBlockHeight;
use dashcore::{Address, Block, OutPoint, Transaction, Txid};
use key_wallet::managed_account::address_pool::AddressPoolType;
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::broadcast;

/// Result of processing a block through the wallet
#[derive(Debug, Default, Clone)]
pub struct BlockProcessingResult {
    /// Transaction IDs that were newly discovered
    pub new_txids: Vec<Txid>,
    /// Transaction IDs that were already in wallet history
    pub existing_txids: Vec<Txid>,
    /// New addresses generated per wallet during gap-limit maintenance.
    pub new_addresses: BTreeMap<WalletId, Vec<Address>>,
}

/// Result of processing a mempool transaction through the wallet
#[derive(Debug, Default, Clone)]
pub struct MempoolTransactionResult {
    /// Whether the transaction was relevant to any wallet.
    pub is_relevant: bool,
    /// Net amount change for the wallet (received - sent) in satoshis.
    pub net_amount: i64,
    /// Whether this is an outgoing transaction (net_amount < 0).
    pub is_outgoing: bool,
    /// Addresses involved in this transaction.
    pub addresses: Vec<Address>,
    /// New addresses generated during gap limit maintenance.
    pub new_addresses: Vec<Address>,
}

impl BlockProcessingResult {
    /// Returns all relevant transaction IDs (new and existing)
    pub fn relevant_txids(&self) -> impl Iterator<Item = &Txid> {
        self.new_txids.iter().chain(self.existing_txids.iter())
    }

    /// Returns the count of all relevant transactions (new and existing)
    pub fn relevant_tx_count(&self) -> usize {
        self.new_txids.len() + self.existing_txids.len()
    }

    /// Iterate over every newly generated address regardless of wallet attribution.
    pub fn all_new_addresses(&self) -> impl Iterator<Item = &Address> {
        self.new_addresses.values().flatten()
    }
}

/// One backfill obligation associated with a block: which sync range to
/// advance, and to what height. Carries both the matched block's height
/// (so the worker can prune stale obligations on reorg or completion) and
/// `advance_to`, the chunk_end of the backfill sweep that produced the
/// match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillAdvance {
    pub wallet_id: WalletId,
    pub pool: AddressPoolType,
    pub indexes: Range<u32>,
    /// Height of the matched block whose download is awaited.
    pub height: CoreBlockHeight,
    /// Where `caught_up_to` should land after this block is processed.
    pub advance_to: CoreBlockHeight,
}

/// Trait for wallet implementations to receive SPV events
#[async_trait]
pub trait WalletInterface: Send + Sync + 'static {
    /// Process a block, but only against the listed wallets. Implementations
    /// must update the per-wallet `last_processed_height` for each wallet in
    /// `wallets` once the block is applied to its state.
    ///
    /// Pass the result of `wallets_behind(height)` for the canonical "scan
    /// only the wallets that need this block" semantics.
    async fn process_block_for_wallets(
        &mut self,
        block: &Block,
        height: CoreBlockHeight,
        wallets: &BTreeSet<WalletId>,
    ) -> BlockProcessingResult;

    /// Process a block discovered by the backfill worker. Bundles the
    /// per-sync-range advance obligations so a downstream persister can
    /// write the records and the `caught_up_to` advance atomically via
    /// [`WalletEvent::RescanBlockProcessed`].
    ///
    /// The default implementation derives the wallet set from `advances`
    /// and delegates to [`Self::process_block_for_wallets`], so minimal
    /// mock implementations keep working unchanged. Real implementations
    /// should override this to emit the dedicated event instead of
    /// `BlockProcessed` for backfill blocks.
    ///
    /// [`WalletEvent::RescanBlockProcessed`]: crate::WalletEvent::RescanBlockProcessed
    async fn process_backfill_block_for_wallets(
        &mut self,
        block: &Block,
        height: CoreBlockHeight,
        advances: &[BackfillAdvance],
    ) -> BlockProcessingResult {
        let wallets: BTreeSet<WalletId> = advances.iter().map(|a| a.wallet_id).collect();
        let result = self.process_block_for_wallets(block, height, &wallets).await;
        for advance in advances {
            self.advance_rescan(
                &advance.wallet_id,
                advance.pool,
                advance.indexes.clone(),
                advance.advance_to,
            );
        }
        result
    }

    /// Called when a transaction is seen in the mempool.
    /// Returns whether the transaction was relevant and any new addresses generated.
    /// When `instant_lock` is `Some`, the transaction already has an IS lock.
    async fn process_mempool_transaction(
        &mut self,
        tx: &Transaction,
        instant_lock: Option<InstantLock>,
    ) -> MempoolTransactionResult;

    /// Get all addresses the wallet is monitoring for incoming transactions
    fn monitored_addresses(&self) -> Vec<Address>;

    /// Get monitored addresses for a specific wallet.
    fn monitored_addresses_for(&self, wallet_id: &WalletId) -> Vec<Address>;

    /// Get all outpoints the wallet is watching (unspent outputs).
    /// Used for bloom filter construction to detect spends of our UTXOs.
    fn watched_outpoints(&self) -> Vec<OutPoint>;

    /// Return the earliest block height that should be scanned for this wallet on the
    /// specified network. Implementations can use the wallet's birth height or other
    /// metadata to provide a more precise rescan starting point.
    ///
    /// The default implementation returns `None`, which signals that the caller should
    /// fall back to its existing behaviour.
    async fn earliest_required_height(&self) -> CoreBlockHeight {
        0
    }

    /// Return the last fully processed height of the wallet.
    fn last_processed_height(&self) -> CoreBlockHeight;

    /// Return the lowest committed sync checkpoint across all managed wallets.
    /// Filter scanning resumes from this height. A new wallet added behind this
    /// drags the value down and triggers a rescan.
    fn synced_height(&self) -> CoreBlockHeight;

    /// Return the wallet IDs whose `synced_height` is strictly less than `height`,
    /// i.e. the wallets that still need filter coverage at that height.
    fn wallets_behind(&self, height: CoreBlockHeight) -> BTreeSet<WalletId>;

    /// Return the wallet IDs that still need filter coverage at heights up to
    /// and including `height`. Equivalent to `wallets_behind(height + 1)` but
    /// expresses the inclusive intent at the call site, so callers don't have
    /// to compensate the strict-less-than semantics with `+ 1`.
    fn wallets_not_yet_at(&self, height: CoreBlockHeight) -> BTreeSet<WalletId> {
        self.wallets_behind(height.saturating_add(1))
    }

    /// Return the per-wallet committed sync checkpoint, or `0` if unknown.
    ///
    /// This is the strictly monotonic forward edge. Pair with
    /// [`Self::wallet_convergence_height`] when a consumer needs the
    /// "everything below this is final" semantics.
    fn wallet_synced_height(&self, wallet_id: &WalletId) -> CoreBlockHeight;

    /// Per-wallet convergence height, or `None` if the wallet is unknown.
    ///
    /// See [`key_wallet::wallet::managed_wallet_info::wallet_info_interface::WalletInfoInterface::convergence_height`]
    /// for the semantics. Non-monotonic by nature.
    ///
    /// The default implementation falls back to
    /// [`Self::wallet_synced_height`], which is correct for any wallet
    /// implementation that does not track sync ranges.
    fn wallet_convergence_height(&self, wallet_id: &WalletId) -> Option<CoreBlockHeight> {
        Some(self.wallet_synced_height(wallet_id))
    }

    /// Wallet IDs whose `convergence_height` is strictly below `height`,
    /// i.e. the wallets that still have pending backfill obligations
    /// reaching that high.
    ///
    /// The default implementation falls back to [`Self::wallets_behind`].
    fn wallets_pending_convergence(&self, height: CoreBlockHeight) -> BTreeSet<WalletId> {
        self.wallets_behind(height)
    }

    /// All pending rescan obligations across all wallets, suitable for the
    /// backfill worker to drive its sweep-line scan.
    ///
    /// The default implementation returns an empty vec, suitable for
    /// implementations that do not track sync ranges.
    fn pending_rescans(&self) -> Vec<PendingRescan> {
        Vec::new()
    }

    /// Mark a contiguous slice of filter heights as scanned for a
    /// particular pool's pending sync range. The wallet advances each
    /// matching range's `caught_up_to`, dropping any that complete.
    ///
    /// The default implementation is a no-op, suitable for
    /// implementations that do not track sync ranges.
    fn advance_rescan(
        &mut self,
        _wallet_id: &WalletId,
        _pool: AddressPoolType,
        _indexes: Range<u32>,
        _scanned_through: CoreBlockHeight,
    ) {
    }

    /// React to a confirmed chain reorg to `fork_height`. Clamps every
    /// pending sync range's `caught_up_to` to at most `fork_height` so the
    /// backfill worker re-covers any window that progressed past the fork.
    ///
    /// The default implementation is a no-op, suitable for implementations
    /// that do not track sync ranges.
    fn on_chain_reorg(&mut self, _fork_height: CoreBlockHeight) {}

    /// Advance one wallet's committed sync checkpoint. Implementations must
    /// only advance forward (a value below the current is silently ignored).
    fn update_wallet_synced_height(&mut self, wallet_id: &WalletId, height: CoreBlockHeight);

    /// Advance one wallet's last-processed height after a block has been applied
    /// to its state. Implementations must only advance forward.
    fn update_wallet_last_processed_height(
        &mut self,
        wallet_id: &WalletId,
        height: CoreBlockHeight,
    );

    /// Return a revision counter that increments whenever the set of monitored
    /// addresses or watched outpoints changes. The mempool manager uses this to
    /// detect when its bloom filter is stale without requiring an external signal.
    fn monitor_revision(&self) -> u64 {
        0
    }

    /// Subscribe to wallet events (e.g. transactions received, balance changes).
    fn subscribe_events(&self) -> broadcast::Receiver<WalletEvent>;

    /// Process an InstantSend lock for a transaction already in the wallet.
    /// Marks UTXOs as IS-locked, emits status change and balance update events.
    fn process_instant_send_lock(&mut self, _instant_lock: InstantLock) {}

    /// Provide a human-readable description of the wallet implementation.
    ///
    /// Implementations are encouraged to include high-level state such as the
    /// number of managed wallets, networks, or tracked scripts.
    async fn describe(&self) -> String {
        "Wallet interface description unavailable".to_string()
    }
}
