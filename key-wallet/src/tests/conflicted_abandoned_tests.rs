//! Lifecycle tests for [`TransactionContext::Conflicted`] and
//! [`TransactionContext::Abandoned`]. Covers UTXO release semantics
//! when an existing record transitions from `InBlock` to an inactive
//! state, plus the no-op behavior when an inactive transaction is
//! first sighted by a fresh account.

use dashcore::hashes::Hash;
use dashcore::{BlockHash, Transaction};

use crate::managed_account::managed_account_trait::ManagedAccountTrait;
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
/// context via the low-level funds-account API. Mirrors what the
/// rewind logic in #145 will do at the orchestration layer.
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
