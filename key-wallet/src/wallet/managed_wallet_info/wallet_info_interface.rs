//! Trait defining the interface for wallet info types
//!
//! This trait allows WalletManager to work with different wallet info implementations

use std::collections::{BTreeMap, BTreeSet};

use super::managed_account_operations::ManagedAccountOperations;
use crate::account::{AccountType, ManagedAccountTrait};
use crate::managed_account::managed_account_collection::ManagedAccountCollection;
use crate::managed_account::managed_account_ref::ManagedAccountRefMut;
use crate::transaction_checking::TransactionContext;
use crate::transaction_checking::WalletTransactionChecker;
use crate::wallet::managed_wallet_info::TransactionRecord;
use crate::wallet::ManagedWalletInfo;
use crate::{Network, Utxo, Wallet, WalletCoreBalance};
use dashcore::ephemerealdata::chain_lock::ChainLock;
use dashcore::ephemerealdata::instant_lock::InstantLock;
use dashcore::prelude::CoreBlockHeight;
use dashcore::{Address as DashAddress, OutPoint, Transaction, Txid};

/// Outcome of [`WalletInfoInterface::rewind_to_height`].
///
/// Captures the per-wallet rewind effects so the manager-level emitter
/// (in `key-wallet-manager`) can fire a single atomic
/// `WalletEvent::Reorg` per wallet whose state changed.
#[derive(Debug, Clone, Default)]
pub struct RewindOutcome {
    /// Records demoted to an active-but-unconfirmed context
    /// (`Mempool` or retained `InstantSend`).
    pub demoted_txids: Vec<Txid>,
    /// Records demoted to a terminal inactive context
    /// (`Conflicted` / `Abandoned`). Reserved for the follow-up
    /// self-conflict detection work; currently always empty.
    pub conflicted_txids: Vec<Txid>,
    /// `true` iff this wallet's `last_processed_height` was rolled back
    /// or any record was demoted; `false` when the reorg had no
    /// observable effect on this wallet's state.
    pub state_changed: bool,
}

/// Why a [`WalletInfoInterface::rewind_to_height`] call rejected the rewind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewindRejection {
    /// `height` is strictly below this wallet's
    /// `last_applied_chain_lock.block_height`. Crossing the chainlock
    /// floor would invalidate finality the wallet has already accepted.
    BelowChainLockFloor {
        /// The height the caller asked to rewind to.
        requested: CoreBlockHeight,
        /// The wallet's chainlock floor.
        floor: CoreBlockHeight,
    },
}

/// Outcome of [`WalletInfoInterface::apply_chain_lock`].
///
/// Captures both effects of applying a chainlock so the manager-level
/// emitter (in `key-wallet-manager`) can fire a single atomic
/// `WalletEvent::ChainLockProcessed` whenever the wallet's
/// `last_applied_chain_lock` metadata advanced — carrying any net-new
/// promotions in `locked_transactions` (empty when the metadata
/// advance promoted nothing).
#[derive(Debug, Clone, Default)]
pub struct ApplyChainLockOutcome {
    /// Per-account net-new finalized txids: records that flipped from
    /// `InBlock` to `InChainLockedBlock` in this promotion. Accounts
    /// with no net-new promotions are omitted. Empty when the chainlock
    /// landed on a wallet that has no `InBlock` records at heights
    /// `<= chain_lock.block_height`.
    pub locked_transactions: BTreeMap<AccountType, Vec<Txid>>,
    /// `true` iff the wallet's `last_applied_chain_lock` strictly
    /// advanced (or moved from `None` to `Some`) as a result of this
    /// call. `false` when the incoming chainlock's height did not
    /// exceed the already-stored chainlock's height.
    pub metadata_advanced: bool,
}

/// Trait that wallet info types must implement to work with WalletManager
pub trait WalletInfoInterface: Sized + WalletTransactionChecker + ManagedAccountOperations {
    /// Create a wallet info from an existing wallet, seeding the sync checkpoint at
    /// `birth_height`.
    ///
    /// Both `synced_height` and `last_processed_height` are seeded to
    /// `birth_height.saturating_sub(1)` so the next block to scan is `birth_height`.
    /// Taking `birth_height` at construction makes the sync checkpoint a required
    /// invariant of the type rather than something callers have to remember to set.
    fn from_wallet(wallet: &Wallet, birth_height: CoreBlockHeight) -> Self;

    /// Create a wallet info with a name, seeding the sync checkpoint at `birth_height`
    /// (see `from_wallet` for details).
    fn from_wallet_with_name(wallet: &Wallet, name: String, birth_height: CoreBlockHeight) -> Self;

    /// Get the wallet's network
    fn network(&self) -> Network;

    /// Get the wallet's unique ID
    fn wallet_id(&self) -> [u8; 32];

    /// Get the wallet's name
    fn name(&self) -> Option<&str>;

    /// Set the wallet's name
    fn set_name(&mut self, name: String);

    /// Get the wallet's description
    fn description(&self) -> Option<&str>;

    /// Set the wallet's description
    fn set_description(&mut self, description: Option<String>);

    /// Get the birth height of the wallet
    fn birth_height(&self) -> CoreBlockHeight;

    /// Update last synced timestamp
    fn update_last_synced(&mut self, timestamp: u64);

    /// Get all monitored addresses
    fn monitored_addresses(&self) -> Vec<DashAddress>;

    /// Get all UTXOs for the wallet
    fn utxos(&self) -> BTreeSet<&Utxo>;

    /// Get spendable UTXOs (confirmed and not locked)
    fn get_spendable_utxos(&self) -> BTreeSet<&Utxo>;

    /// Get the wallet balance
    fn balance(&self) -> WalletCoreBalance;

    /// Update the wallet balance
    fn update_balance(&mut self);

    /// Per-account balances keyed by `AccountType`.
    ///
    /// Only funds-bearing accounts (Standard, CoinJoin, DashPay) carry a
    /// balance — keys-only accounts (identity, asset-lock, provider) are
    /// excluded from the result entirely rather than reported with a zero
    /// balance.
    fn account_balances(&self) -> BTreeMap<AccountType, WalletCoreBalance> {
        self.accounts()
            .all_funding_accounts()
            .iter()
            .map(|funds| (funds.managed_account_type().to_account_type(), funds.balance))
            .collect()
    }

    /// Get transaction history
    fn transaction_history(&self) -> Vec<&TransactionRecord>;

    /// Get accounts (mutable)
    fn accounts_mut(&mut self) -> &mut ManagedAccountCollection;

    /// Get accounts (immutable)
    fn accounts(&self) -> &ManagedAccountCollection;

    /// Get immature transactions
    fn immature_transactions(&self) -> Vec<Transaction>;

    /// Return the last fully processed height of the wallet.
    fn last_processed_height(&self) -> CoreBlockHeight;

    /// Return the durable wallet sync checkpoint height.
    fn synced_height(&self) -> CoreBlockHeight;

    /// Return the highest chainlock that has been applied to this
    /// wallet, retaining the signing proof. Blocks at or below
    /// `chain_lock.block_height` are considered chainlock-finalized
    /// for this wallet. `None` until the first chainlock arrives.
    fn last_applied_chain_lock(&self) -> Option<&ChainLock> {
        None
    }

    /// Promote every `InBlock` transaction record across this wallet's
    /// accounts whose block height is `<= chain_lock.block_height` to
    /// `TransactionContext::InChainLockedBlock`, advance the wallet's
    /// `last_applied_chain_lock` to `chain_lock` (clamped forward by
    /// height), and return both effects in a single
    /// [`ApplyChainLockOutcome`].
    ///
    /// Field semantics:
    ///
    /// - `locked_transactions` is populated when records were promoted.
    ///   Accounts with no net-new promotions are omitted. Empty when no
    ///   record was `InBlock` at a height `<= chain_lock.block_height`.
    /// - `metadata_advanced` is `true` when the wallet's
    ///   `last_applied_chain_lock` strictly advanced (or moved from
    ///   `None` to `Some`) as a result of this call. The manager (in
    ///   `key-wallet-manager`) emits one
    ///   `WalletEvent::ChainLockProcessed` per wallet when this is
    ///   `true`, regardless of whether `locked_transactions` is empty —
    ///   a chainlock that lands above a wallet's currently recorded
    ///   history still establishes the finality boundary for future
    ///   blocks that arrive in that range via the late-block path in
    ///   block processing, and durable consumers must persist the new
    ///   `last_applied_chain_lock` to benefit from that boundary across
    ///   restarts.
    ///
    /// Under the default `keep-finalized-transactions=OFF` feature the
    /// promoted records are dropped and only their txids are retained —
    /// the txids are still surfaced in `locked_transactions` so the
    /// caller can emit the `ChainLockProcessed` event before the
    /// records disappear.
    fn apply_chain_lock(&mut self, _chain_lock: ChainLock) -> ApplyChainLockOutcome {
        ApplyChainLockOutcome::default()
    }

    /// Roll this wallet back to `height` after a chain reorg.
    ///
    /// Demotes every confirmed record on every account whose mined
    /// block height is strictly greater than `height`, plus the
    /// transitive in-wallet spend descendants of those records.
    /// Rebuilds per-account UTXO state from the surviving records.
    /// Clamps `last_processed_height` and `synced_height` to
    /// `min(height, current)`. Refreshes cached balances.
    ///
    /// `used_addresses` markers on every address pool are intentionally
    /// preserved. Address usage is monotonic from a wallet's perspective:
    /// even if the reorg removes the on-chain evidence for that usage,
    /// the user has still committed the addresses and could publish them
    /// again. Resurrecting unused addresses also risks recycling them
    /// across separate counterparties.
    ///
    /// Refuses the rewind when `height` is strictly below this
    /// wallet's `last_applied_chain_lock.block_height` and returns
    /// [`RewindRejection::BelowChainLockFloor`] in that case.
    fn rewind_to_height(
        &mut self,
        height: CoreBlockHeight,
    ) -> Result<RewindOutcome, RewindRejection>;

    /// Update chain state and process any matured transactions
    /// This should be called when the chain tip advances to a new height
    fn update_last_processed_height(&mut self, current_height: u32);

    /// Record that the durable wallet sync checkpoint has advanced to `current_height`.
    fn update_synced_height(&mut self, current_height: u32);

    /// Records whose coinbase maturity threshold lies in
    /// `(old_height, new_height]`, i.e. coinbase records that just matured
    /// during the height advance from `old_height` to `new_height`.
    ///
    /// Returns clones of the matured records so the caller can include them
    /// in atomic events without mutating wallet state.
    fn matured_coinbase_records(
        &self,
        old_height: CoreBlockHeight,
        new_height: CoreBlockHeight,
    ) -> Vec<TransactionRecord>;

    /// Mark UTXOs for a transaction as InstantSend-locked across all accounts
    /// and update the corresponding transaction record context.
    /// Returns `true` if any UTXO was newly marked.
    fn mark_instant_send_utxos(&mut self, txid: &Txid, lock: &InstantLock) -> bool;

    /// Return the aggregated monitor revision across all accounts.
    /// Increments whenever the monitored address set changes.
    fn monitor_revision(&self) -> u64 {
        0
    }
}

/// Default implementation for ManagedWalletInfo
impl WalletInfoInterface for ManagedWalletInfo {
    fn from_wallet(wallet: &Wallet, birth_height: CoreBlockHeight) -> Self {
        Self::from_wallet(wallet, birth_height)
    }

    fn from_wallet_with_name(wallet: &Wallet, name: String, birth_height: CoreBlockHeight) -> Self {
        Self::from_wallet_with_name(wallet, name, birth_height)
    }

    fn network(&self) -> Network {
        self.network
    }

    fn wallet_id(&self) -> [u8; 32] {
        self.wallet_id
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn set_name(&mut self, name: String) {
        self.name = Some(name);
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn set_description(&mut self, description: Option<String>) {
        self.description = description;
    }

    fn birth_height(&self) -> CoreBlockHeight {
        self.metadata.birth_height
    }

    fn last_processed_height(&self) -> CoreBlockHeight {
        self.metadata.last_processed_height
    }

    fn synced_height(&self) -> CoreBlockHeight {
        self.metadata.synced_height
    }

    fn last_applied_chain_lock(&self) -> Option<&ChainLock> {
        self.metadata.last_applied_chain_lock.as_ref()
    }

    fn apply_chain_lock(&mut self, chain_lock: ChainLock) -> ApplyChainLockOutcome {
        let cl_height = chain_lock.block_height;
        let mut locked_transactions: BTreeMap<AccountType, Vec<Txid>> = BTreeMap::new();

        // Promote across every account: funds-bearing (Standard,
        // CoinJoin, DashPay) and keys-only (identity, asset-lock,
        // provider, platform-payment). Keys-only accounts hold
        // transactions such as identity registrations and asset locks
        // that under the default `keep-finalized-transactions=false`
        // feature must be dropped to bound memory once chainlocked,
        // exactly like funds-account txs.
        for account in self.accounts.all_accounts_mut() {
            let (account_type, finalized_txids) = match account {
                ManagedAccountRefMut::Funds(funds) => (
                    funds.managed_account_type().to_account_type(),
                    funds.apply_chain_lock(cl_height),
                ),
                ManagedAccountRefMut::Keys(keys) => (
                    keys.managed_account_type().to_account_type(),
                    keys.apply_chain_lock(cl_height),
                ),
            };
            if !finalized_txids.is_empty() {
                locked_transactions.insert(account_type, finalized_txids);
            }
        }

        let advance = self
            .metadata
            .last_applied_chain_lock
            .as_ref()
            .is_none_or(|existing| cl_height > existing.block_height);
        if advance {
            self.metadata.last_applied_chain_lock = Some(chain_lock);
        }

        ApplyChainLockOutcome {
            locked_transactions,
            metadata_advanced: advance,
        }
    }

    fn update_last_synced(&mut self, timestamp: u64) {
        self.metadata.last_synced = Some(timestamp);
    }

    fn monitored_addresses(&self) -> Vec<DashAddress> {
        let mut addresses = Vec::new();
        for account in self.accounts.all_accounts() {
            addresses.extend(account.all_addresses());
        }
        addresses
    }

    fn utxos(&self) -> BTreeSet<&Utxo> {
        let mut utxos = BTreeSet::new();
        for account in self.accounts.all_funding_accounts() {
            utxos.extend(account.utxos.values());
        }
        utxos
    }
    fn get_spendable_utxos(&self) -> BTreeSet<&Utxo> {
        self.utxos()
            .into_iter()
            .filter(|utxo| utxo.is_spendable(self.last_processed_height()))
            .collect()
    }

    fn balance(&self) -> WalletCoreBalance {
        self.balance
    }

    fn update_balance(&mut self) {
        // Only funds-bearing accounts contribute to the wallet balance.
        let mut balance = WalletCoreBalance::default();
        let last_processed_height = self.last_processed_height();
        for funds in self.accounts.all_funding_accounts_mut() {
            funds.update_balance(last_processed_height);
            balance += funds.balance;
        }
        self.balance = balance;
    }

    fn transaction_history(&self) -> Vec<&TransactionRecord> {
        let mut transactions = Vec::new();
        for account in self.accounts.all_accounts() {
            transactions.extend(account.transactions().values());
        }
        transactions
    }

    fn accounts_mut(&mut self) -> &mut ManagedAccountCollection {
        &mut self.accounts
    }

    fn accounts(&self) -> &ManagedAccountCollection {
        &self.accounts
    }

    fn immature_transactions(&self) -> Vec<Transaction> {
        // Coinbase UTXOs only live on funds-bearing accounts.
        let mut immature_txids: BTreeSet<Txid> = BTreeSet::new();
        for account in self.accounts.all_funding_accounts() {
            for utxo in account.utxos.values() {
                if utxo.is_coinbase && !utxo.is_mature(self.last_processed_height()) {
                    immature_txids.insert(utxo.outpoint.txid);
                }
            }
        }

        // Look up the matching transaction records on the same funds accounts.
        let mut transactions = Vec::new();
        for account in self.accounts.all_funding_accounts() {
            for (txid, record) in account.transactions() {
                if immature_txids.contains(txid) {
                    transactions.push(record.transaction.clone());
                }
            }
        }
        transactions
    }

    fn update_last_processed_height(&mut self, current_height: u32) {
        self.metadata.last_processed_height = current_height;
        // Update cached balance
        self.update_balance();
    }

    fn rewind_to_height(
        &mut self,
        height: CoreBlockHeight,
    ) -> Result<RewindOutcome, RewindRejection> {
        if let Some(cl) = self.metadata.last_applied_chain_lock.as_ref() {
            if height < cl.block_height {
                return Err(RewindRejection::BelowChainLockFloor {
                    requested: height,
                    floor: cl.block_height,
                });
            }
        }

        // TODO(issue #146): self-conflict detection. Once the wallet
        // can prove that an in-wallet record's inputs are spent by a
        // surviving chain transaction, demote those records to
        // `Conflicted { previous: .. }` and surface them via
        // `conflicted_txids` instead of `demoted_txids`.
        //
        // TODO(issue #145, follow-up): ISLock-quorum re-validation.
        // Records currently in `InstantSend(islock)` are retained
        // through the rewind (their context carries no block height
        // so the cut below does not touch them). If the post-reorg
        // masternode list no longer contains the signing quorum the
        // record's IS lock claims, the lock is no longer
        // verifiable and the record should be demoted to `Mempool`.
        // That decision requires masternode-list knowledge the
        // wallet does not have, so the caller (SPV side) is expected
        // to drive a follow-up demotion via the existing
        // `process_mempool_transaction(tx, None)` path.
        let mut demoted_txids: Vec<Txid> = Vec::new();
        let conflicted_txids: Vec<Txid> = Vec::new();

        for mut account in self.accounts.all_accounts_mut() {
            let local = match &mut account {
                ManagedAccountRefMut::Funds(funds) => funds.demote_records_above(height),
                ManagedAccountRefMut::Keys(keys) => keys.demote_records_above(height),
            };
            demoted_txids.extend(local);
        }

        // Cross-account descendant cascade. Demoted records release
        // their outputs, so any in-wallet transaction (possibly on a
        // different account) whose inputs spend those outputs must
        // also demote. Iterate to a fixed point.
        //
        // `frontier` carries only the txids demoted in the previous
        // wave: any newly reachable descendant must spend an output of
        // that wave, so re-scanning earlier waves would be wasted work.
        let mut already_demoted: BTreeSet<Txid> = demoted_txids.iter().copied().collect();
        let mut frontier: Vec<Txid> = demoted_txids.clone();
        loop {
            let mut released_outpoints: BTreeSet<OutPoint> = BTreeSet::new();
            for account in self.accounts.all_accounts() {
                let txs = account.transactions();
                for txid in &frontier {
                    if let Some(record) = txs.get(txid) {
                        for vout in 0..record.transaction.output.len() as u32 {
                            released_outpoints.insert(OutPoint {
                                txid: *txid,
                                vout,
                            });
                        }
                    }
                }
            }
            if released_outpoints.is_empty() {
                break;
            }
            let mut new_demoted: Vec<Txid> = Vec::new();
            for mut account in self.accounts.all_accounts_mut() {
                let candidates: Vec<Txid> = account
                    .transactions()
                    .iter()
                    .filter(|(txid, record)| {
                        if already_demoted.contains(*txid) {
                            return false;
                        }
                        if record.context.is_inactive() {
                            return false;
                        }
                        record
                            .transaction
                            .input
                            .iter()
                            .any(|input| released_outpoints.contains(&input.previous_output))
                    })
                    .map(|(txid, _)| *txid)
                    .collect();
                for txid in candidates {
                    let did = match &mut account {
                        ManagedAccountRefMut::Funds(funds) => funds.demote_record(&txid),
                        ManagedAccountRefMut::Keys(keys) => keys.demote_record(&txid),
                    };
                    if did {
                        new_demoted.push(txid);
                    }
                }
            }
            if new_demoted.is_empty() {
                break;
            }
            for txid in &new_demoted {
                already_demoted.insert(*txid);
            }
            demoted_txids.extend(new_demoted.iter().copied());
            frontier = new_demoted;
        }

        // UTXO rebuild on every funds account so the spendable set
        // reflects the final post-rewind context for every record.
        for funds in self.accounts.all_funding_accounts_mut() {
            funds.rebuild_utxos();
        }

        let prior_last_processed = self.metadata.last_processed_height;
        let prior_synced = self.metadata.synced_height;
        let new_last_processed = prior_last_processed.min(height);
        let new_synced = prior_synced.min(height);
        self.metadata.last_processed_height = new_last_processed;
        self.metadata.synced_height = new_synced;

        // Refresh cached balances now that UTXOs and heights are settled.
        self.update_balance();

        let state_changed = !demoted_txids.is_empty()
            || !conflicted_txids.is_empty()
            || new_last_processed != prior_last_processed
            || new_synced != prior_synced;

        Ok(RewindOutcome {
            demoted_txids,
            conflicted_txids,
            state_changed,
        })
    }

    fn update_synced_height(&mut self, current_height: u32) {
        self.metadata.synced_height = current_height;
    }

    fn matured_coinbase_records(
        &self,
        old_height: CoreBlockHeight,
        new_height: CoreBlockHeight,
    ) -> Vec<TransactionRecord> {
        if new_height <= old_height {
            return Vec::new();
        }
        // Coinbase records only land on funds-bearing accounts.
        let mut matured = Vec::new();
        for account in self.accounts.all_funding_accounts() {
            for record in account.transactions().values() {
                if !record.transaction.is_coin_base() {
                    continue;
                }
                let Some(record_height) = record.height() else {
                    continue;
                };
                let maturity_height = record_height.saturating_add(100);
                if maturity_height > old_height && maturity_height <= new_height {
                    matured.push(record.clone());
                }
            }
        }
        matured
    }

    fn mark_instant_send_utxos(&mut self, txid: &Txid, lock: &InstantLock) -> bool {
        if !self.instant_send_locks.insert(*txid) {
            return false;
        }
        let mut any_changed = false;
        for mut account in self.accounts.all_accounts_mut() {
            if account.mark_utxos_instant_send(txid) {
                any_changed = true;
            }
            if let Some(record) = account.transactions_mut().get_mut(txid) {
                record.update_context(TransactionContext::InstantSend(lock.clone()));
            }
        }
        if any_changed {
            self.update_balance();
        }
        any_changed
    }

    fn monitor_revision(&self) -> u64 {
        self.accounts.all_accounts().iter().map(|a| a.monitor_revision()).sum()
    }
}
