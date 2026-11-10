use std::time::Duration;

use super::helpers::{wait_for_sync, wait_for_wallet_synced};
use super::setup::TestContext;
use dash_spv::test_utils::TestChain;
use dashcore::Amount;
use key_wallet::wallet::managed_wallet_info::wallet_info_interface::WalletInfoInterface;

/// Verify a synced client follows the network onto a longer branch that
/// replaces its last blocks, and finds a payment in a new block below its
/// old tip.
#[tokio::test]
async fn test_sync_follows_reorg() {
    let Some(ctx) = TestContext::new(TestChain::Minimal).await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }
    let mut client_handle = ctx.spawn_new_client().await;
    wait_for_sync(&mut client_handle.progress_receiver, ctx.dashd.initial_height).await;

    // Replace the last 3 blocks with a branch of 5 whose first block pays the wallet.
    let fork_height = ctx.dashd.initial_height - 3;
    let node = &ctx.dashd.node;
    node.invalidate_block(&node.get_block_hash(fork_height + 1));
    let txid = node.send_to_address(&ctx.receive_address().await, Amount::from_sat(100_000_000));
    node.generate_blocks(5, &node.get_new_address());
    let new_tip = fork_height + 5;

    tokio::time::timeout(
        Duration::from_secs(60),
        wait_for_sync(&mut client_handle.progress_receiver, new_tip),
    )
    .await
    .expect("client did not follow the reorg");

    assert_eq!(client_handle.client.tip_height().await, new_tip);
    assert_eq!(client_handle.client.tip_hash().await, Some(node.get_best_block_hash()));

    wait_for_wallet_synced(&ctx.wallet, &ctx.wallet_id, new_tip).await;
    let paid_height = {
        let wallet = ctx.wallet.read().await;
        let wallet_info = wallet.get_wallet_info(&ctx.wallet_id).expect("wallet info");
        wallet_info
            .accounts()
            .all_accounts()
            .iter()
            .find_map(|account| account.transactions().get(&txid))
            .and_then(|record| record.height())
    };
    assert_eq!(paid_height, Some(fork_height + 1), "payment in the new branch not found");

    client_handle.stop().await;
}
