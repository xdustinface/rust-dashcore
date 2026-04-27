//! Wallet events for notifying consumers of wallet state changes.
//!
//! Each variant is self-contained: it carries the transaction record(s) that
//! triggered it and the wallet's new balance after the change. Consumers can
//! persist the transaction(s) and balance atomically off a single event.

use dashcore::ephemerealdata::instant_lock::InstantLock;
use dashcore::prelude::CoreBlockHeight;
use dashcore::Txid;
use key_wallet::managed_account::transaction_record::TransactionRecord;
use key_wallet::WalletCoreBalance;

use crate::WalletId;

/// Whether a record is newly stored or updates a previously stored record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordAction {
    /// The record is new (first time stored for this wallet).
    Inserted,
    /// The record was already stored and has been updated (e.g. confirmation
    /// or InstantSend lock applied to a known mempool tx).
    Updated,
}

/// A transaction record paired with the action that just happened to it
/// (`Inserted` for a fresh record, `Updated` for a record whose state changed).
///
/// Used as a single value by `TransactionReceived` and as a `Vec` by
/// `BlockProcessed`, so consumers can share a single record-handling code
/// path across both events. The owning account is available on
/// `record.account_type`.
#[derive(Debug, Clone)]
pub struct TransactionRecordUpdate {
    /// Whether the record is new or an update to a previously stored one.
    pub action: RecordAction,
    /// The full transaction record.
    pub record: TransactionRecord,
}

/// Events emitted by the wallet manager.
///
/// Each event represents a meaningful wallet state change. Events that
/// modify balance carry the wallet's balance *after* the change so
/// consumers can persist the record(s) and balance atomically.
#[derive(Debug, Clone)]
pub enum WalletEvent {
    /// A wallet-relevant transaction was first seen off-chain (mempool, or
    /// directly via an InstantSend lock — in that case `update.record.context`
    /// is `InstantSend(..)`).
    TransactionReceived {
        /// ID of the affected wallet.
        wallet_id: WalletId,
        /// The newly-recorded transaction with its account context.
        ///
        /// Boxed to keep the enum compact: `TransactionRecord` is large and
        /// would otherwise inflate every variant to its size.
        update: Box<TransactionRecordUpdate>,
        /// Wallet balance after the transaction was recorded.
        balance: WalletCoreBalance,
    },
    /// A previously-seen wallet-relevant transaction was InstantSend-locked.
    TransactionInstantSendLocked {
        /// ID of the affected wallet.
        wallet_id: WalletId,
        /// Transaction ID that was locked.
        txid: Txid,
        /// The InstantSend lock that locked the transaction.
        instant_send_lock: InstantLock,
        /// Wallet balance after the lock was applied.
        balance: WalletCoreBalance,
    },
    /// A block was processed for a wallet. Carries the newly-recorded and
    /// state-modified transaction records plus the post-block balance.
    /// `updates` may be empty when only the balance shifted (e.g. a
    /// coinbase maturing as the scanned height advanced past its threshold).
    BlockProcessed {
        /// ID of the affected wallet.
        wallet_id: WalletId,
        /// Height of the block that was processed.
        height: CoreBlockHeight,
        /// Transaction records recorded or updated by this block.
        updates: Vec<TransactionRecordUpdate>,
        /// Wallet balance after the block was processed.
        balance: WalletCoreBalance,
    },
    /// The wallet's scan cursor advanced because the filter pipeline
    /// committed a batch covering blocks up to `height`. No new records or
    /// balance are carried — consumers can persist this as a checkpoint
    /// atomically with any records/balance already persisted from prior
    /// `BlockProcessed` events inside the batch.
    SyncedHeightUpdated {
        /// ID of the affected wallet.
        wallet_id: WalletId,
        /// New scanned height for the wallet.
        height: CoreBlockHeight,
    },
}

impl WalletEvent {
    /// ID of the wallet this event pertains to.
    pub fn wallet_id(&self) -> WalletId {
        match self {
            WalletEvent::TransactionReceived {
                wallet_id,
                ..
            }
            | WalletEvent::TransactionInstantSendLocked {
                wallet_id,
                ..
            }
            | WalletEvent::BlockProcessed {
                wallet_id,
                ..
            }
            | WalletEvent::SyncedHeightUpdated {
                wallet_id,
                ..
            } => *wallet_id,
        }
    }

    /// Short description for logging.
    pub fn description(&self) -> String {
        match self {
            WalletEvent::TransactionReceived {
                update,
                balance,
                ..
            } => {
                format!(
                    "TransactionReceived(txid={}, context={}, balance={})",
                    update.record.txid, update.record.context, balance
                )
            }
            WalletEvent::TransactionInstantSendLocked {
                txid,
                balance,
                ..
            } => {
                format!("TransactionInstantSendLocked(txid={}, balance={})", txid, balance)
            }
            WalletEvent::BlockProcessed {
                height,
                updates,
                balance,
                ..
            } => {
                format!(
                    "BlockProcessed(height={}, updates={}, balance={})",
                    height,
                    updates.len(),
                    balance
                )
            }
            WalletEvent::SyncedHeightUpdated {
                height,
                ..
            } => {
                format!("SyncedHeightUpdated(height={})", height)
            }
        }
    }
}
