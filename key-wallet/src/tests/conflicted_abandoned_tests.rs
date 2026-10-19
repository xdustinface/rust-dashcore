//! Lifecycle tests for [`TransactionContext::Conflicted`] and
//! [`TransactionContext::Abandoned`]. Covers UTXO release semantics
//! when an existing record transitions from `InBlock` to an inactive
//! state, plus the no-op behavior when an inactive transaction is
//! first sighted by a fresh account.

use dashcore::blockdata::transaction::OutPoint;
use dashcore::hashes::Hash;
use dashcore::{BlockHash, Transaction, TxIn, TxOut};

use crate::managed_account::managed_account_trait::ManagedAccountTrait;
use crate::managed_account::ManagedCoreFundsAccount;
use crate::test_utils::TestWalletContext;
use crate::transaction_checking::transaction_router::TransactionType;
use crate::transaction_checking::{BlockInfo, TransactionContext};
use crate::wallet::balance::WalletCoreBalance;
use crate::wallet::managed_wallet_info::wallet_info_interface::WalletInfoInterface;

const BLOCK_HEIGHT: u32 = 200;
const FUND_AMOUNT: u64 = 250_000;

fn block_context() -> TransactionContext {
    let hash = BlockHash::from_slice(&[0x42; 32]).expect("hash");
    TransactionContext::InBlock(BlockInfo::new(BLOCK_HEIGHT, hash, 1_700_000_000))
}

/// Funds the wallet's receive address and promotes the funding tx into
/// `InBlock`, ensuring a UTXO is recorded as confirmed before we drive
/// it to an inactive context.
async fn funded_in_block() -> (TestWalletContext, Transaction) {
    let (mut ctx, tx) = TestWalletContext::new_random().with_mempool_funding(FUND_AMOUNT).await;
    let result = ctx.check_transaction(&tx, block_context()).await;
    assert!(result.state_modified);
    assert_eq!(ctx.bip44_account().utxos.len(), 1);
    (ctx, tx)
}

/// Promote an existing record from `InBlock` to the given inactive
/// context via the low-level funds-account API.
fn transition_to_inactive(
    ctx: &mut TestWalletContext,
    tx: &Transaction,
    inactive: TransactionContext,
) {
    let relevant = ctx
        .managed_wallet
        .first_bip44_managed_account()
        .expect("bip44 account")
        .check_transaction_for_match(tx, Some(0))
        .expect("tx matches funded account");
    let account = ctx.managed_wallet.first_bip44_managed_account_mut().expect("bip44 account");
    account.confirm_transaction(tx, &relevant, inactive, TransactionType::Standard);
}

#[tokio::test]
async fn fresh_account_with_conflicted_tx_keeps_empty_balance() {
    let mut ctx = TestWalletContext::new_random();
    let tx = Transaction::dummy(&ctx.receive_address, 0..1, &[FUND_AMOUNT]);
    let conflicted = TransactionContext::Conflicted {
        previous: Box::new(TransactionContext::Mempool),
    };

    let _ = ctx.check_transaction(&tx, conflicted.clone()).await;

    assert!(
        ctx.bip44_account().utxos.is_empty(),
        "no UTXOs should be tracked for an inactive incoming tx"
    );
    ctx.managed_wallet.update_balance();
    assert_eq!(ctx.managed_wallet.balance(), WalletCoreBalance::default());

    let record = ctx
        .managed_wallet
        .first_bip44_managed_account()
        .expect("bip44 account")
        .transactions()
        .get(&tx.txid())
        .expect("record was inserted");
    assert!(matches!(record.context, TransactionContext::Conflicted { .. }));
}

#[tokio::test]
async fn in_block_to_conflicted_releases_utxos_and_preserves_previous() {
    let (mut ctx, tx) = funded_in_block().await;
    let pre = block_context();

    transition_to_inactive(
        &mut ctx,
        &tx,
        TransactionContext::Conflicted {
            previous: Box::new(pre.clone()),
        },
    );

    assert!(
        ctx.bip44_account().utxos.is_empty(),
        "conflicted tx must drop its outputs from the spendable set"
    );
    ctx.managed_wallet.update_balance();
    assert_eq!(ctx.managed_wallet.balance(), WalletCoreBalance::default());

    let record = ctx
        .managed_wallet
        .first_bip44_managed_account()
        .expect("bip44 account")
        .transactions()
        .get(&tx.txid())
        .expect("record still present after conflict");
    let TransactionContext::Conflicted {
        previous,
    } = &record.context
    else {
        panic!("expected Conflicted context");
    };
    assert_eq!(**previous, pre, "previous context must round-trip");
}

/// Build a transaction that spends `outpoint` and pays to an
/// unrelated script (an empty `script_pubkey`), so the spending tx
/// has no outputs the wallet considers its own.
fn spending_tx(outpoint: OutPoint, value: u64) -> Transaction {
    Transaction {
        version: 1,
        lock_time: 0,
        input: vec![TxIn {
            previous_output: outpoint,
            ..Default::default()
        }],
        output: vec![TxOut {
            value,
            script_pubkey: Default::default(),
        }],
        special_transaction_payload: None,
    }
}

#[tokio::test]
async fn spending_tx_going_inactive_releases_input_outpoint() {
    let (mut ctx, funding_tx) = funded_in_block().await;
    let funded_outpoint = OutPoint {
        txid: funding_tx.txid(),
        vout: 0,
    };
    let spend = spending_tx(funded_outpoint, FUND_AMOUNT);

    // Capture the AccountMatch before processing the spend, since the
    // spend removes the input UTXO and subsequent lookups would fail.
    let spend_match = ctx
        .managed_wallet
        .first_bip44_managed_account()
        .expect("bip44 account")
        .check_transaction_for_match(&spend, Some(0))
        .expect("spend matches funded account before processing");

    let result = ctx.check_transaction(&spend, block_context()).await;
    assert!(result.state_modified, "spending tx must be recognised as ours");
    assert!(
        ctx.bip44_account().utxos.is_empty(),
        "the spent UTXO must leave the spendable set once the spend confirms"
    );

    let account = ctx.managed_wallet.first_bip44_managed_account_mut().expect("bip44 account");
    account.confirm_transaction(
        &spend,
        &spend_match,
        TransactionContext::Abandoned,
        TransactionType::Standard,
    );

    // With the spending tx abandoned, the funding outpoint is no longer
    // considered spent. Reprocessing the funding tx must re-track its
    // output as a UTXO, which exercises the `spent_outpoints` release
    // branch inside `release_inactive_utxos`.
    let result = ctx.check_transaction(&funding_tx, block_context()).await;
    assert!(result.is_relevant);
    assert_eq!(
        ctx.bip44_account().utxos.len(),
        1,
        "UTXO must be re-trackable once its consumer is abandoned"
    );
}

/// Regression: the `Deserialize` impl for `ManagedCoreFundsAccount` must skip
/// inputs from inactive (`Conflicted`/`Abandoned`) records when rebuilding
/// `spent_outpoints`. Otherwise a round-trip would re-mark the funded outpoint
/// as spent and the funding UTXO could not be re-tracked after its consumer
/// has been abandoned.
#[cfg(feature = "serde")]
#[tokio::test]
async fn serde_round_trip_does_not_resurrect_inactive_spent_outpoints() {
    let (mut ctx, funding_tx) = funded_in_block().await;
    let funded_outpoint = OutPoint {
        txid: funding_tx.txid(),
        vout: 0,
    };
    let spend = spending_tx(funded_outpoint, FUND_AMOUNT);

    let spend_match = ctx
        .managed_wallet
        .first_bip44_managed_account()
        .expect("bip44 account")
        .check_transaction_for_match(&spend, Some(0))
        .expect("spend matches funded account");

    let result = ctx.check_transaction(&spend, block_context()).await;
    assert!(result.state_modified);

    let account = ctx.managed_wallet.first_bip44_managed_account_mut().expect("bip44 account");
    account.confirm_transaction(
        &spend,
        &spend_match,
        TransactionContext::Abandoned,
        TransactionType::Standard,
    );

    let json = serde_json::to_string(account).expect("serialize");
    let deserialized: ManagedCoreFundsAccount = serde_json::from_str(&json).expect("deserialize");
    *account = deserialized;

    let result = ctx.check_transaction(&funding_tx, block_context()).await;
    assert!(result.is_relevant);
    assert_eq!(
        ctx.bip44_account().utxos.len(),
        1,
        "funding UTXO must be re-trackable after serde round-trip with abandoned spend"
    );
}

#[tokio::test]
async fn in_block_to_abandoned_releases_utxos() {
    let (mut ctx, tx) = funded_in_block().await;

    transition_to_inactive(&mut ctx, &tx, TransactionContext::Abandoned);

    assert!(
        ctx.bip44_account().utxos.is_empty(),
        "abandoned tx must drop its outputs from the spendable set"
    );
    ctx.managed_wallet.update_balance();
    assert_eq!(ctx.managed_wallet.balance(), WalletCoreBalance::default());

    let record = ctx
        .managed_wallet
        .first_bip44_managed_account()
        .expect("bip44 account")
        .transactions()
        .get(&tx.txid())
        .expect("record still present after abandonment");
    assert_eq!(record.context, TransactionContext::Abandoned);
}
