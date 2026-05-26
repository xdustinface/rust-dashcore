//! Reorg integration tests driving real chain reorgs against a single
//! regtest dashd via `invalidateblock` + `generatetoaddress`.
//!
//! Each scenario waits for the SPV client to sync to dashd's current tip,
//! then invalidates a block at `fork_height + 1` on dashd, mines a competing
//! chain past the original tip, and verifies the SPV client reacts.
//!
//! Several scenarios from the [#147] checklist are gated on infrastructure
//! that does not exist in the current harness and are marked `#[ignore]`
//! with a link to the gating issue: CLSIG injection (no LLMQs in
//! `without_masternodes()` regtest), DIP-24 rotation, IS-locks, and the
//! post-cascade downstream resync.

use std::sync::Arc;
use std::time::Duration;

use dash_spv::sync::SyncEvent;
use dash_spv::test_utils::{create_test_wallet, TestChain};
use dash_spv::Network;
use dashcore::{Amount, BlockHash};
use key_wallet_manager::WalletEvent;
use tokio::sync::broadcast;

use super::helpers::wait_for_sync;
use super::setup::{create_and_start_client, TestContext};

/// How long to wait for a reorg-related event after triggering the reorg
/// on dashd. Generous because the cascade walks through fork detection,
/// guard evaluation, and four storage truncations.
const REORG_EVENT_TIMEOUT: Duration = Duration::from_secs(60);

/// Wait for a `SyncEvent::ChainReorg` whose `fork_height` matches `expected`.
/// Returns the event so the caller can inspect old/new tip and generation.
async fn wait_for_chain_reorg(
    receiver: &mut broadcast::Receiver<SyncEvent>,
    expected_fork_height: u32,
) -> SyncEvent {
    let deadline = tokio::time::sleep(REORG_EVENT_TIMEOUT);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => panic!(
                "timed out waiting for ChainReorg(fork_height={})",
                expected_fork_height
            ),
            recv = receiver.recv() => match recv {
                Ok(event @ SyncEvent::ChainReorg { fork_height, .. })
                    if fork_height == expected_fork_height => return event,
                Ok(_) => continue,
                Err(err) => panic!("sync event channel error waiting for ChainReorg: {}", err),
            }
        }
    }
}

/// Wait for a `WalletEvent::Reorg` whose `fork_height` matches `expected`.
async fn wait_for_wallet_reorg(
    receiver: &mut broadcast::Receiver<WalletEvent>,
    expected_fork_height: u32,
) -> WalletEvent {
    let deadline = tokio::time::sleep(REORG_EVENT_TIMEOUT);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => panic!(
                "timed out waiting for WalletEvent::Reorg(fork_height={})",
                expected_fork_height
            ),
            recv = receiver.recv() => match recv {
                Ok(event @ WalletEvent::Reorg { fork_height, .. })
                    if fork_height == expected_fork_height => return event,
                Ok(_) => continue,
                Err(err) => panic!("wallet event channel error waiting for Reorg: {}", err),
            }
        }
    }
}

/// Roll dashd back to `fork_height` and mine `new_chain_len` competing
/// blocks to a fresh address from the "default" wallet so the new
/// coinbase differs from the invalidated one. Returns the dashd tip hash
/// after the fork.
fn fork_dashd(ctx: &TestContext, fork_height: u32, new_chain_len: u64) -> BlockHash {
    let invalidate_hash = ctx.dashd.node.get_block_hash(fork_height + 1);
    ctx.dashd.node.invalidate_block(&invalidate_hash);
    let fork_miner = ctx.dashd.node.get_new_address_from_wallet("default");
    ctx.dashd.node.generate_blocks(new_chain_len, &fork_miner);
    ctx.dashd.node.get_best_block_hash()
}

/// Shallow reorg: SPV emits `SyncEvent::ChainReorg` with the expected
/// fork height and a bumped generation counter.
///
/// Asserts cascade-emission only. Full post-cascade resync of filter
/// headers / filters / blocks is gated on a downstream wiring fix tracked
/// separately, so this test stops at the event boundary.
#[tokio::test]
async fn test_shallow_reorg_emits_chain_reorg_event() {
    let Some(ctx) = TestContext::new(TestChain::Minimal).await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let mut client = ctx.spawn_new_client().await;
    wait_for_sync(&mut client.progress_receiver, ctx.dashd.initial_height).await;

    let miner_address = ctx.dashd.node.get_new_address_from_wallet("default");
    ctx.dashd.node.generate_blocks(6, &miner_address);
    let original_tip_height = ctx.dashd.initial_height + 6;
    wait_for_sync(&mut client.progress_receiver, original_tip_height).await;

    let fork_height = ctx.dashd.initial_height + 1;
    fork_dashd(&ctx, fork_height, 7);

    let reorg_event = wait_for_chain_reorg(&mut client.sync_event_receiver, fork_height).await;
    match reorg_event {
        SyncEvent::ChainReorg {
            fork_height: ev_fork,
            generation,
            ..
        } => {
            assert_eq!(ev_fork, fork_height);
            assert!(generation > 0, "generation counter must advance on cascade");
        }
        other => panic!("expected ChainReorg, got {:?}", other),
    }

    client.stop().await;
}

/// Deep reorg approaching cap (90 blocks): cascade still runs and emits
/// `ChainReorg`.
///
/// Asserts cascade-emission only, same reasoning as
/// `test_shallow_reorg_emits_chain_reorg_event`.
#[tokio::test]
async fn test_deep_reorg_within_cap_emits_chain_reorg() {
    let Some(ctx) = TestContext::new(TestChain::Minimal).await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let mut client = ctx.spawn_new_client().await;
    wait_for_sync(&mut client.progress_receiver, ctx.dashd.initial_height).await;

    let miner_address = ctx.dashd.node.get_new_address_from_wallet("default");
    ctx.dashd.node.generate_blocks(90, &miner_address);
    let original_tip_height = ctx.dashd.initial_height + 90;
    wait_for_sync(&mut client.progress_receiver, original_tip_height).await;

    // Fork at depth 90, fresh-client floor is current_tip - 100, so this
    // sits above the floor and below the depth cap.
    let fork_height = ctx.dashd.initial_height;
    fork_dashd(&ctx, fork_height, 91);

    wait_for_chain_reorg(&mut client.sync_event_receiver, fork_height).await;

    client.stop().await;
}

/// Reorg exceeding the depth cap (101+ blocks).
///
/// `DeepReorgDetected` only fires when the depth-cap guard runs. On a
/// fresh client without an observed chainlock the fresh-client fork floor
/// (`FRESH_CLIENT_FORK_FLOOR = 100`) rejects the candidate before the
/// depth-cap guard sees it, so the path to `DeepReorgDetected` requires
/// injecting a chainlock at the fork height.
#[ignore = "DeepReorgDetected requires a chainlock floor; CLSIG injection harness missing (see https://github.com/xdustinface/rust-dashcore/issues/141)"]
#[tokio::test]
async fn test_reorg_exceeding_cap_emits_deep_reorg_detected() {}

/// ChainLock-forced reorg via simulated CLSIG injection: bypasses depth
/// cap and chainlock floor.
///
/// Regtest with `without_masternodes()` has no LLMQs, so no
/// BLS-validated ChainLock can be produced end-to-end. The `force=true`
/// unit-level behavior is already covered in
/// `dash-spv/src/sync/reorg.rs`.
#[ignore = "requires CLSIG injection harness, see https://github.com/xdustinface/rust-dashcore/issues/141"]
#[tokio::test]
async fn test_chainlock_forced_reorg_bypasses_cap() {}

/// Reorg crossing a DIP-24 rotation cycle boundary: quorum set refreshes.
///
/// Needs a running masternode network so rotated quorums actually exist
/// across the fork. The current dashd integration harness runs
/// `without_masternodes()`.
#[ignore = "requires masternode harness for DIP-24 rotation, see https://github.com/xdustinface/rust-dashcore/issues/142"]
#[tokio::test]
async fn test_reorg_across_rotation_cycle_boundary() {}

/// Reorg with IS-locked transaction: IS context retained iff the
/// signing quorum survives.
///
/// Same blocker as the rotation-cycle test: no LLMQs in this harness, so
/// no IS-locks can be produced.
#[ignore = "requires masternode harness for IS-locks, see https://github.com/xdustinface/rust-dashcore/issues/142"]
#[tokio::test]
async fn test_reorg_islocked_tx_retains_context_if_quorum_survives() {}

/// Reorg with a descendant tx chain in the wallet: the cascade demotes
/// the parent and child in `WalletEvent::Reorg`. We only assert the
/// event fires after a reorg that crosses the heights containing both
/// records.
#[tokio::test]
async fn test_reorg_with_descendant_tx_chain_emits_wallet_reorg() {
    let Some(ctx) = TestContext::new(TestChain::Minimal).await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let mut client = ctx.spawn_new_client().await;
    wait_for_sync(&mut client.progress_receiver, ctx.dashd.initial_height).await;

    let miner_address = ctx.dashd.node.get_new_address_from_wallet("default");
    let receive_address = ctx.receive_address().await;

    let parent_txid =
        ctx.dashd.node.send_to_address(&receive_address, Amount::from_sat(200_000_000));
    ctx.dashd.node.generate_blocks(1, &miner_address);
    let parent_height = ctx.dashd.initial_height + 1;
    wait_for_sync(&mut client.progress_receiver, parent_height).await;
    assert!(ctx.has_transaction(&parent_txid).await);

    let receive_address_2 = ctx.receive_address().await;
    let child_txid =
        ctx.dashd.node.send_to_address(&receive_address_2, Amount::from_sat(50_000_000));
    ctx.dashd.node.generate_blocks(5, &miner_address);
    let after_child_height = ctx.dashd.initial_height + 6;
    wait_for_sync(&mut client.progress_receiver, after_child_height).await;
    assert!(ctx.has_transaction(&child_txid).await);

    let fork_height = ctx.dashd.initial_height;
    fork_dashd(&ctx, fork_height, 8);

    wait_for_wallet_reorg(&mut client.wallet_event_receiver, fork_height).await;

    client.stop().await;
}

/// Concurrent filter fetch in flight when a reorg fires: the generation
/// counter discards stale responses. Asserts the cascade still emits
/// `ChainReorg` and the generation counter advances.
///
/// The race is impossible to target deterministically from outside, so
/// this is a smoke test for the cascade firing under heavier in-flight
/// load.
#[tokio::test]
async fn test_reorg_concurrent_filter_fetch_emits_chain_reorg() {
    let Some(ctx) = TestContext::new(TestChain::Minimal).await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let mut client = ctx.spawn_new_client().await;
    wait_for_sync(&mut client.progress_receiver, ctx.dashd.initial_height).await;

    let miner_address = ctx.dashd.node.get_new_address_from_wallet("default");
    ctx.dashd.node.generate_blocks(10, &miner_address);
    let original_tip = ctx.dashd.initial_height + 10;
    wait_for_sync(&mut client.progress_receiver, original_tip).await;

    let fork_height = ctx.dashd.initial_height + 2;
    fork_dashd(&ctx, fork_height, 12);

    let event = wait_for_chain_reorg(&mut client.sync_event_receiver, fork_height).await;
    if let SyncEvent::ChainReorg {
        generation,
        ..
    } = event
    {
        assert!(generation > 0);
    }

    client.stop().await;
}

/// Fork-then-fork ping-pong.
///
/// On a fresh client without a chainlock the deny-list TTL is set to
/// `floor` (`current_tip - FRESH_CLIENT_FORK_FLOOR`), not `u32::MAX`,
/// because the rejection guard is the fresh-client floor and not the
/// depth cap. As a result the deny-list entry can age out as the active
/// tip advances. Reliable assertion of damper behaviour against dashd
/// needs `DeepReorgDetected` to fire, which in turn needs the CLSIG
/// injection harness.
#[ignore = "fresh-client floor TTL ages out the deny-list; need chainlock-driven rejection to test damper deterministically (see https://github.com/xdustinface/rust-dashcore/issues/141)"]
#[tokio::test]
async fn test_fork_then_fork_ping_pong_denied() {}

/// Crash mid-reorg recovery. Kill the SPV between cascade truncation and
/// downstream resync, restart, and verify the startup consistency check
/// recomputes a safe tip.
///
/// Currently gated on the post-cascade downstream resync being driven to
/// completion on restart. The cascade itself fires, but the filter/block
/// pipelines do not resume against dashd's new tip without further
/// wiring.
#[ignore = "post-cascade downstream resync not yet driven on restart; tracked alongside the reorg recovery work (see https://github.com/xdustinface/rust-dashcore/issues/143)"]
#[tokio::test]
async fn test_crash_mid_reorg_then_restart_recovers() {}

/// Auto-rebroadcast: a wallet-owned outgoing tx demoted by a reorg is
/// re-enqueued and reaches the new chain's mempool.
///
/// The rebroadcast loop runs against the SPV's mempool sync, which needs
/// filter sync to resume after the cascade (see the same gating note as
/// the crash-mid-reorg test). Once filter sync resumes, rebroadcast
/// becomes observable.
#[ignore = "needs post-cascade filter/mempool resync (see https://github.com/xdustinface/rust-dashcore/issues/143)"]
#[tokio::test]
async fn test_auto_rebroadcast_after_reorg() {}

/// Auto-rebroadcast suppressed when the demoted tx's input is conflicted
/// on the new chain.
///
/// Same blocker as `test_auto_rebroadcast_after_reorg`: needs filter
/// resync. Also requires double-spend tooling in the harness to
/// deterministically stage the conflicting input, which is not yet
/// available.
#[ignore = "needs post-cascade resync plus double-spend harness (see https://github.com/xdustinface/rust-dashcore/issues/143)"]
#[tokio::test]
async fn test_auto_rebroadcast_suppressed_when_input_conflicted() {}

/// Both `SyncEvent::ChainReorg` and `WalletEvent::Reorg` arrive after a
/// reorg that crosses heights containing wallet-relevant blocks.
#[tokio::test]
async fn test_event_handler_observes_chain_reorg_and_wallet_reorg() {
    let Some(ctx) = TestContext::new(TestChain::Minimal).await else {
        return;
    };
    if !ctx.dashd.supports_mining {
        eprintln!("Skipping test (dashd RPC miner not available)");
        return;
    }

    let (wallet, _) = create_test_wallet(&ctx.dashd.wallet.mnemonic, Network::Regtest);
    let mut client = create_and_start_client(&ctx.client_config, Arc::clone(&wallet)).await;
    wait_for_sync(&mut client.progress_receiver, ctx.dashd.initial_height).await;

    let miner_address = ctx.dashd.node.get_new_address_from_wallet("default");
    let receive_address = ctx.receive_address().await;
    ctx.dashd.node.send_to_address(&receive_address, Amount::from_sat(100_000_000));
    ctx.dashd.node.generate_blocks(3, &miner_address);
    let original_tip = ctx.dashd.initial_height + 3;
    wait_for_sync(&mut client.progress_receiver, original_tip).await;

    let fork_height = ctx.dashd.initial_height;
    fork_dashd(&ctx, fork_height, 5);

    let (sync_evt, wallet_evt) = tokio::join!(
        wait_for_chain_reorg(&mut client.sync_event_receiver, fork_height),
        wait_for_wallet_reorg(&mut client.wallet_event_receiver, fork_height),
    );
    assert!(matches!(sync_evt, SyncEvent::ChainReorg { .. }));
    assert!(matches!(wallet_evt, WalletEvent::Reorg { .. }));

    client.stop().await;
}
