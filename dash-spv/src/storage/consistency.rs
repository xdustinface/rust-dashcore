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
use crate::sync::reorg::MAX_REORG_DEPTH;
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
    if metadata.read().await.is_reorg_sentinel_set().await {
        let safe_tip = block_headers.write().await.highest_valid_tip().await;
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
    let stored_chainlock = metadata.read().await.load_metadata(BEST_CHAINLOCK_KEY).await?;
    if let Some(bytes) = stored_chainlock {
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
    if let Some(fh_bound) = fh_tip_after {
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
    } else if f_tip > 0 {
        tracing::warn!("consistency: filters exist (tip {}) but no filter headers present", f_tip);
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
        let tip = filter_headers.read().await.get_filter_tip_height().await?.unwrap_or(start);
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
    const _: () = assert!(
        PROBE_WINDOW > MAX_REORG_DEPTH,
        "PROBE_WINDOW must exceed MAX_REORG_DEPTH to guarantee stale-block detection"
    );
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use dashcore::bls_sig_utils::BLSSignature;
    use dashcore::ephemerealdata::chain_lock::ChainLock;
    use dashcore::hash_types::FilterHeader;
    use dashcore::{BlockHash, Header as BlockHeader};
    use dashcore_hashes::Hash;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    use crate::storage::{
        BlockHeaderStorage, BlockStorage, FilterHeaderStorage, FilterStorage, MetadataStorage,
        PersistentBlockHeaderStorage, PersistentBlockStorage, PersistentFilterHeaderStorage,
        PersistentFilterStorage, PersistentMetadataStorage, PersistentStorage,
    };
    use crate::sync::BEST_CHAINLOCK_KEY;
    use crate::types::HashedBlock;

    use super::check_and_repair_consistency;

    async fn open_all(
        path: &Path,
    ) -> (
        PathBuf,
        Arc<RwLock<PersistentBlockHeaderStorage>>,
        Arc<RwLock<PersistentFilterHeaderStorage>>,
        Arc<RwLock<PersistentFilterStorage>>,
        Arc<RwLock<PersistentBlockStorage>>,
        Arc<RwLock<PersistentMetadataStorage>>,
    ) {
        let storage_path = path.to_path_buf();
        let bh =
            Arc::new(RwLock::new(PersistentBlockHeaderStorage::open(&storage_path).await.unwrap()));
        let fh = Arc::new(RwLock::new(
            PersistentFilterHeaderStorage::open(&storage_path).await.unwrap(),
        ));
        let f = Arc::new(RwLock::new(PersistentFilterStorage::open(&storage_path).await.unwrap()));
        let b = Arc::new(RwLock::new(PersistentBlockStorage::open(&storage_path).await.unwrap()));
        let m =
            Arc::new(RwLock::new(PersistentMetadataStorage::open(&storage_path).await.unwrap()));
        (storage_path, bh, fh, f, b, m)
    }

    #[tokio::test]
    async fn highest_valid_tip_returns_full_chain_when_intact() {
        let tmp = TempDir::new().unwrap();
        let chain = BlockHeader::dummy_chain(5, BlockHash::all_zeros());
        let storage = PersistentBlockHeaderStorage::open(tmp.path()).await.unwrap();
        let mut storage = storage;
        storage.store_headers(&chain).await.unwrap();
        let tip = storage.highest_valid_tip().await;
        assert_eq!(tip, Some(4));
    }

    #[tokio::test]
    async fn highest_valid_tip_returns_lower_height_when_chain_broken() {
        let tmp = TempDir::new().unwrap();
        // `BlockHeader::dummy_batch(0..5)` produces headers whose
        // `prev_blockhash` is `BlockHash::dummy(h-1)`, which does not equal
        // the previous header's actual block_hash. So the index chain is
        // broken everywhere except height 0, where `prev_blockhash` =
        // `BlockHash::dummy(0)` which is stored in the index only when
        // `block_hash() == dummy(0)`. Since that's also unlikely, the function
        // bails to the start sentinel `Some(0)`.
        let chain = BlockHeader::dummy_batch(0..5);
        let mut storage = PersistentBlockHeaderStorage::open(tmp.path()).await.unwrap();
        storage.store_headers(&chain).await.unwrap();
        let tip = storage.highest_valid_tip().await;
        assert_eq!(tip, Some(0), "broken chain must fall back to the start height");
    }

    #[tokio::test]
    async fn highest_valid_tip_returns_last_good_height_when_only_top_broken() {
        let tmp = TempDir::new().unwrap();
        // Build a valid chain of 8 headers (heights 0-7), then append one
        // disconnected header at height 8 using dummy_batch, which produces a
        // header whose `prev_blockhash` does not match height 7's block_hash.
        // The function must walk back to height 7 and return Some(7).
        let connected = BlockHeader::dummy_chain(8, BlockHash::all_zeros());
        let disconnected = BlockHeader::dummy_batch(8..9);
        let mut storage = PersistentBlockHeaderStorage::open(tmp.path()).await.unwrap();
        storage.store_headers(&connected).await.unwrap();
        storage.store_headers_at_height(&disconnected, 8).await.unwrap();
        assert_eq!(storage.get_tip_height().await, Some(8));
        let tip = storage.highest_valid_tip().await;
        assert_eq!(tip, Some(7), "only the top link is broken; must return the last good height");
    }

    #[tokio::test]
    async fn filter_header_above_block_tip_is_truncated() {
        let tmp = TempDir::new().unwrap();
        let (path, bh, fh, f, b, m) = open_all(tmp.path()).await;

        let chain = BlockHeader::dummy_chain(5, BlockHash::all_zeros());
        bh.write().await.store_headers(&chain).await.unwrap();
        fh.write().await.store_filter_headers(&FilterHeader::dummy_batch(0..10)).await.unwrap();
        assert_eq!(fh.read().await.get_filter_tip_height().await.unwrap(), Some(9));

        check_and_repair_consistency(&path, &bh, &fh, &f, &b, &m).await.unwrap();

        assert_eq!(fh.read().await.get_filter_tip_height().await.unwrap(), Some(4));
    }

    #[tokio::test]
    async fn filter_above_filter_header_tip_is_truncated() {
        let tmp = TempDir::new().unwrap();
        let (path, bh, fh, f, b, m) = open_all(tmp.path()).await;

        let chain = BlockHeader::dummy_chain(10, BlockHash::all_zeros());
        bh.write().await.store_headers(&chain).await.unwrap();
        fh.write().await.store_filter_headers(&FilterHeader::dummy_batch(0..5)).await.unwrap();
        for h in 0..8 {
            f.write().await.store_filter(h, &[0xAA; 4]).await.unwrap();
        }
        assert_eq!(f.read().await.filter_tip_height().await.unwrap(), 7);

        check_and_repair_consistency(&path, &bh, &fh, &f, &b, &m).await.unwrap();

        assert_eq!(f.read().await.filter_tip_height().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn block_above_block_tip_is_truncated() {
        let tmp = TempDir::new().unwrap();
        let (path, bh, fh, f, b, m) = open_all(tmp.path()).await;

        // Header tip at 4, block storage holds a block at heights 3 and 10.
        // Block 3 is within bounds, block 10 is stale and must be dropped.
        let chain = BlockHeader::dummy_chain(5, BlockHash::all_zeros());
        bh.write().await.store_headers(&chain).await.unwrap();
        b.write().await.store_block(3, HashedBlock::dummy(3, vec![])).await.unwrap();
        b.write().await.store_block(10, HashedBlock::dummy(10, vec![])).await.unwrap();

        check_and_repair_consistency(&path, &bh, &fh, &f, &b, &m).await.unwrap();

        assert_eq!(b.read().await.load_block(10).await.unwrap(), None);
        assert!(b.read().await.load_block(3).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn block_at_probe_window_boundary_is_truncated() {
        let tmp = TempDir::new().unwrap();
        let (path, bh, fh, f, b, m) = open_all(tmp.path()).await;

        let chain = BlockHeader::dummy_chain(5, BlockHash::all_zeros());
        bh.write().await.store_headers(&chain).await.unwrap();
        // Store a block within range to anchor start_height, then one at the
        // exact PROBE_WINDOW boundary so it is found and truncated.
        let at_boundary = 4 + 1_024u32;
        b.write().await.store_block(0, HashedBlock::dummy(0, vec![])).await.unwrap();
        b.write()
            .await
            .store_block(at_boundary, HashedBlock::dummy(at_boundary, vec![]))
            .await
            .unwrap();

        check_and_repair_consistency(&path, &bh, &fh, &f, &b, &m).await.unwrap();

        assert_eq!(b.read().await.load_block(at_boundary).await.unwrap(), None);
    }

    #[tokio::test]
    async fn block_beyond_probe_window_is_not_detected() {
        let tmp = TempDir::new().unwrap();
        let (path, bh, fh, f, b, m) = open_all(tmp.path()).await;

        let chain = BlockHeader::dummy_chain(5, BlockHash::all_zeros());
        bh.write().await.store_headers(&chain).await.unwrap();
        // Store a block within range to anchor start_height, then one beyond
        // PROBE_WINDOW. The beyond-window block is a known limitation.
        let beyond_window = 4 + 1_024u32 + 1;
        b.write().await.store_block(0, HashedBlock::dummy(0, vec![])).await.unwrap();
        b.write()
            .await
            .store_block(beyond_window, HashedBlock::dummy(beyond_window, vec![]))
            .await
            .unwrap();

        check_and_repair_consistency(&path, &bh, &fh, &f, &b, &m).await.unwrap();

        // Known limitation: blocks beyond PROBE_WINDOW are not detected.
        // MAX_REORG_DEPTH (100) << PROBE_WINDOW (1024) so this cannot occur
        // via normal reorg paths.
        assert!(b.read().await.load_block(beyond_window).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn stale_chainlock_is_cleared() {
        let tmp = TempDir::new().unwrap();
        let (path, bh, fh, f, b, m) = open_all(tmp.path()).await;

        let chain = BlockHeader::dummy_chain(5, BlockHash::all_zeros());
        bh.write().await.store_headers(&chain).await.unwrap();

        let chainlock = ChainLock {
            block_height: 100,
            block_hash: BlockHash::all_zeros(),
            signature: BLSSignature::from([0u8; 96]),
        };
        let bytes = serde_json::to_vec(&chainlock).unwrap();
        m.write().await.store_metadata(BEST_CHAINLOCK_KEY, &bytes).await.unwrap();

        check_and_repair_consistency(&path, &bh, &fh, &f, &b, &m).await.unwrap();

        let after = m.read().await.load_metadata(BEST_CHAINLOCK_KEY).await.unwrap();
        assert!(after.is_none(), "stale chainlock must be cleared");
    }

    #[tokio::test]
    async fn filters_without_filter_headers_are_not_truncated() {
        let tmp = TempDir::new().unwrap();
        let (path, bh, fh, f, b, m) = open_all(tmp.path()).await;

        let chain = BlockHeader::dummy_chain(5, BlockHash::all_zeros());
        bh.write().await.store_headers(&chain).await.unwrap();
        for h in 0..3u32 {
            f.write().await.store_filter(h, &[0xAA; 4]).await.unwrap();
        }
        assert_eq!(f.read().await.filter_tip_height().await.unwrap(), 2);
        assert!(fh.read().await.get_filter_tip_height().await.unwrap().is_none());

        check_and_repair_consistency(&path, &bh, &fh, &f, &b, &m).await.unwrap();

        assert_eq!(
            f.read().await.filter_tip_height().await.unwrap(),
            2,
            "filter storage must be left untouched when filter headers are absent"
        );
    }

    #[tokio::test]
    async fn sentinel_triggers_safe_tip_truncation() {
        let tmp = TempDir::new().unwrap();
        let (path, bh, fh, f, b, m) = open_all(tmp.path()).await;

        let chain = BlockHeader::dummy_chain(10, BlockHash::all_zeros());
        bh.write().await.store_headers(&chain).await.unwrap();

        // Populate filter headers and filters above the block-header tip to
        // exercise cascade truncation through all downstream storages.
        fh.write().await.store_filter_headers(&FilterHeader::dummy_batch(0..15)).await.unwrap();
        for h in 0..12u32 {
            f.write().await.store_filter(h, &[0xBB; 4]).await.unwrap();
        }
        // Store blocks: one within the safe range (anchors start_height) and one
        // above the safe tip to verify block-storage truncation by the sentinel path.
        let stale_height = 11u32;
        b.write().await.store_block(0, HashedBlock::dummy(0, vec![])).await.unwrap();
        b.write()
            .await
            .store_block(stale_height, HashedBlock::dummy(stale_height, vec![]))
            .await
            .unwrap();
        assert_eq!(fh.read().await.get_filter_tip_height().await.unwrap(), Some(14));
        assert_eq!(f.read().await.filter_tip_height().await.unwrap(), 11);

        m.write().await.write_reorg_sentinel().await.unwrap();
        assert!(m.read().await.is_reorg_sentinel_set().await);

        check_and_repair_consistency(&path, &bh, &fh, &f, &b, &m).await.unwrap();

        assert!(
            !m.read().await.is_reorg_sentinel_set().await,
            "sentinel must be cleared after safe-tip recovery"
        );
        // Intact chain reopens at the same tip.
        let safe_tip = bh.read().await.get_tip_height().await.unwrap();
        assert_eq!(safe_tip, 9);
        // Downstream storages must be truncated to the safe tip.
        assert_eq!(fh.read().await.get_filter_tip_height().await.unwrap(), Some(safe_tip));
        assert_eq!(f.read().await.filter_tip_height().await.unwrap(), safe_tip);
        assert_eq!(
            b.read().await.load_block(stale_height).await.unwrap(),
            None,
            "stale block above safe tip must be removed during sentinel recovery"
        );
    }
}
