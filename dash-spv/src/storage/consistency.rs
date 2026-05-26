//! Cross-storage startup consistency check.
//!
//! Runs unconditionally during `DiskStorageManager::new`, after every
//! sub-storage has been opened. Verifies that downstream storages
//! (filter headers, filters, blocks) cannot point above the block-header
//! tip, and repairs any violation by truncating the offending storage
//! and persisting it. Idempotent: a second invocation on already-repaired
//! storages is a no-op.

use std::path::PathBuf;
use std::sync::Arc;

use dashcore::ephemerealdata::chain_lock::ChainLock;
use tokio::sync::RwLock;

use crate::error::StorageResult;
use crate::storage::{
    BlockHeaderStorage, BlockStorage, FilterHeaderStorage, FilterStorage, MetadataStorage,
    PersistentBlockHeaderStorage, PersistentBlockStorage, PersistentFilterHeaderStorage,
    PersistentFilterStorage, PersistentMetadataStorage, PersistentStorage,
};
use crate::sync::BEST_CHAINLOCK_KEY;

/// Run the cross-storage consistency repair sweep.
///
/// `storage_path` is forwarded to each sub-storage's `persist` so a repair
/// is durable before the function returns. Designed to run during storage
/// open, before any sync task observes the storages.
pub(crate) async fn check_and_repair_consistency(
    storage_path: &PathBuf,
    block_headers: &Arc<RwLock<PersistentBlockHeaderStorage>>,
    filter_headers: &Arc<RwLock<PersistentFilterHeaderStorage>>,
    filters: &Arc<RwLock<PersistentFilterStorage>>,
    blocks: &Arc<RwLock<PersistentBlockStorage>>,
    metadata: &Arc<RwLock<PersistentMetadataStorage>>,
) -> StorageResult<()> {
    // Sentinel branch: a previous run crashed mid-cascade between the
    // generation bump and the final clear. Recover by recomputing the
    // highest header whose parent chains correctly back through the
    // hash index, then truncate every storage to that safe tip.
    if metadata.read().await.is_reorg_sentinel_set() {
        let safe_tip = block_headers.read().await.highest_valid_tip().await;
        match safe_tip {
            Some(safe_tip) => {
                tracing::warn!(
                    "consistency: reorg sentinel set, truncating all storages to safe tip {}",
                    safe_tip
                );
                {
                    let mut guard = block_headers.write().await;
                    guard.truncate_above(safe_tip).await?;
                    guard.persist(storage_path).await?;
                }
                {
                    let mut guard = filter_headers.write().await;
                    guard.truncate_above(safe_tip).await?;
                    guard.persist(storage_path).await?;
                }
                {
                    let mut guard = filters.write().await;
                    guard.truncate_above(safe_tip).await?;
                    guard.persist(storage_path).await?;
                }
                {
                    let mut guard = blocks.write().await;
                    guard.truncate_above(safe_tip).await?;
                    guard.persist(storage_path).await?;
                }
                metadata.write().await.clear_reorg_sentinel().await?;
            }
            None => {
                tracing::warn!(
                    "consistency: reorg sentinel set but no headers available, clearing sentinel"
                );
                metadata.write().await.clear_reorg_sentinel().await?;
            }
        }
    }

    let block_tip = block_headers.read().await.get_tip_height().await;
    let block_tip = match block_tip {
        Some(h) => h,
        None => {
            tracing::debug!("consistency: no block header tip, skipping repair");
            return Ok(());
        }
    };

    // Stale chainlock: a persisted chainlock above the header tip cannot be
    // valid because we have no header to match it against. Clear it so the
    // chainlock manager starts from scratch instead of trusting a value the
    // header tip can no longer corroborate.
    if let Some(bytes) = metadata.read().await.load_metadata(BEST_CHAINLOCK_KEY).await? {
        match serde_json::from_slice::<ChainLock>(&bytes) {
            Ok(chainlock) if chainlock.block_height > block_tip => {
                tracing::warn!(
                    "consistency: persisted chainlock at height {} above block tip {}, clearing",
                    chainlock.block_height,
                    block_tip
                );
                metadata.write().await.delete_metadata(BEST_CHAINLOCK_KEY).await?;
            }
            Ok(_) => {}
            Err(err) => {
                tracing::warn!(
                    "consistency: persisted chainlock failed to deserialize ({}), clearing",
                    err
                );
                metadata.write().await.delete_metadata(BEST_CHAINLOCK_KEY).await?;
            }
        }
    }

    // Filter header invariant: `filter_header_tip <= block_header_tip`.
    let fh_tip = filter_headers.read().await.get_filter_tip_height().await?;
    if let Some(fh_tip) = fh_tip {
        if fh_tip > block_tip {
            tracing::warn!(
                "consistency: filter header tip {} > block header tip {}, truncating",
                fh_tip,
                block_tip
            );
            let mut guard = filter_headers.write().await;
            guard.truncate_above(block_tip).await?;
            guard.persist(storage_path).await?;
        }
    }

    // Filter invariant: `filter_tip <= filter_header_tip`.
    let fh_tip_after = filter_headers.read().await.get_filter_tip_height().await?;
    let f_tip = filters.read().await.filter_tip_height().await?;
    let fh_bound = fh_tip_after.unwrap_or(0);
    if f_tip > fh_bound {
        tracing::warn!(
            "consistency: filter tip {} > filter header tip {}, truncating",
            f_tip,
            fh_bound
        );
        let mut guard = filters.write().await;
        guard.truncate_above(fh_bound).await?;
        guard.persist(storage_path).await?;
    }

    // Block storage invariant: `block_storage_tip <= block_header_tip`. We
    // walk down from `block_tip + 1` checking for the highest stored block;
    // if any exist strictly above the tip, truncate.
    repair_blocks_above_tip(storage_path, blocks, block_tip).await?;

    // BIP157 chain integrity: any filter header within [start, tip] must be
    // loadable without error. A read failure here indicates a corrupted or
    // partially-truncated filter-header range, so we truncate filter headers
    // and filters back to the block header tip.
    if let Some(start) = filter_headers.read().await.get_filter_start_height().await {
        let tip = filter_headers
            .read()
            .await
            .get_filter_tip_height()
            .await?
            .unwrap_or(start);
        if tip >= start {
            let read = filter_headers.read().await.load_filter_headers(start..tip + 1).await;
            if let Err(err) = read {
                tracing::warn!(
                    "consistency: filter header chain unreadable ({:?}), truncating to block tip {}",
                    err,
                    block_tip
                );
                {
                    let mut guard = filter_headers.write().await;
                    guard.truncate_above(block_tip).await?;
                    guard.persist(storage_path).await?;
                }
                let mut guard = filters.write().await;
                guard.truncate_above(block_tip).await?;
                guard.persist(storage_path).await?;
            }
        }
    }

    Ok(())
}

/// Probe block storage above `block_tip` and truncate when stale blocks are
/// found. `PersistentBlockStorage` does not expose its own tip, so we sweep
/// a bounded window above the header tip and truncate when any hit is found.
async fn repair_blocks_above_tip(
    storage_path: &PathBuf,
    blocks: &Arc<RwLock<PersistentBlockStorage>>,
    block_tip: u32,
) -> StorageResult<()> {
    const PROBE_WINDOW: u32 = 1_024;
    let upper = block_tip.saturating_add(PROBE_WINDOW);
    let guard = blocks.read().await;
    let mut found_stale = false;
    let mut probe = block_tip.saturating_add(1);
    while probe <= upper {
        if guard.load_block(probe).await?.is_some() {
            found_stale = true;
            break;
        }
        probe += 1;
    }
    drop(guard);

    if found_stale {
        tracing::warn!(
            "consistency: block storage holds blocks above header tip {}, truncating",
            block_tip
        );
        let mut guard = blocks.write().await;
        guard.truncate_above(block_tip).await?;
        guard.persist(storage_path).await?;
    }
    Ok(())
}
