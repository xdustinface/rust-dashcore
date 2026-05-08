//! Backfill worker for pending sync ranges.
//!
//! When `maintain_gap_limit` derives new addresses on a wallet, the
//! resulting [`AddressSyncRange`] covers an index window that joined the
//! monitored set at `since_height`. Filters at heights below `since_height`
//! were originally scanned without those addresses in the active set, so a
//! tx paying one of them at an earlier height would have been silently
//! missed by the live filter pipeline. The backfill worker re-scans
//! `[birth_height..since_height-1]` for each pending range and advances
//! `caught_up_to` chunk by chunk; when a range catches up the wallet drops
//! it and `convergence_height` rises.
//!
//! Walks the union of all pending range height windows in one sweep so a
//! chunk's filters are loaded from disk once even when several ranges
//! overlap that height window. Cost scales with how far back the work
//! extends, not with the number of historical gap extensions.
//!
//! Block-request dedup against forward sync is the caller's responsibility:
//! `tick` returns the block hashes whose download should be requested, and
//! the existing `BlockMatchTracker` (or whatever the caller uses for
//! forward sync) will deduplicate concurrent requests.
//!
//! [`AddressSyncRange`]: key_wallet::managed_account::address_pool::AddressSyncRange

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use dashcore::bip158::BlockFilter;
use dashcore::prelude::CoreBlockHeight;
use dashcore::{Address, BlockHash};
use key_wallet::managed_account::address_pool::AddressPoolType;
use key_wallet_manager::{
    check_compact_filters_for_addresses, BackfillAdvance, FilterMatchKey, PendingRescan, WalletId,
    WalletInterface,
};
use tokio::sync::RwLock;

use crate::error::SyncResult;
use crate::storage::{BlockHeaderStorage, FilterStorage};

/// Maximum filters scanned per chunk before yielding back to the runtime.
/// Matches the forward-sync batch size so disk reads stay cache-friendly.
const BACKFILL_CHUNK_SIZE: u32 = 5000;

/// Per-block obligation tracked while waiting for a backfill block to be
/// downloaded and processed. Held in [`BackfillWorker::pending_advances`]
/// keyed by block hash. Multiple obligations can attach to the same block
/// when several sync ranges share an address that matched the same filter,
/// so the value is a `Vec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingAdvance {
    pub wallet_id: WalletId,
    pub pool: AddressPoolType,
    pub indexes: Range<u32>,
    pub height: CoreBlockHeight,
    pub advance_to: CoreBlockHeight,
}

impl PendingAdvance {
    fn to_backfill_advance(&self) -> BackfillAdvance {
        BackfillAdvance {
            wallet_id: self.wallet_id,
            pool: self.pool,
            indexes: self.indexes.clone(),
            advance_to: self.advance_to,
        }
    }
}

/// Sweep-line backfill worker over pending sync ranges.
///
/// Owns short-lived state (`pending_advances`) plus shared references to
/// storage and the wallet. The caller drives `tick` (typically wired to a
/// wake channel signalled when `pending_rescans` becomes non-empty) and
/// `on_block_processed` (when a block we requested has been downloaded
/// through the existing block path).
pub(crate) struct BackfillWorker<F, H, W> {
    filter_storage: Arc<RwLock<F>>,
    header_storage: Arc<RwLock<H>>,
    wallet: Arc<RwLock<W>>,
    pending_advances: HashMap<BlockHash, Vec<PendingAdvance>>,
}

impl<F, H, W> BackfillWorker<F, H, W>
where
    F: FilterStorage,
    H: BlockHeaderStorage,
    W: WalletInterface + 'static,
{
    pub(crate) fn new(
        filter_storage: Arc<RwLock<F>>,
        header_storage: Arc<RwLock<H>>,
        wallet: Arc<RwLock<W>>,
    ) -> Self {
        Self {
            filter_storage,
            header_storage,
            wallet,
            pending_advances: HashMap::new(),
        }
    }

    /// Run one sweep over the union of pending range height windows.
    ///
    /// For each chunk: load filters once, scan against the union of every
    /// active range's address set, and either advance `caught_up_to`
    /// directly (if no matches) or record per-block obligations and return
    /// them keyed by `FilterMatchKey` for the caller to dispatch via the
    /// existing block-needed channel.
    ///
    /// Yields between chunks via `tokio::task::yield_now()` so forward sync
    /// continues to make progress.
    pub(crate) async fn tick(
        &mut self,
    ) -> SyncResult<BTreeMap<FilterMatchKey, Vec<BackfillAdvance>>> {
        let rescans = self.wallet.read().await.pending_rescans();
        if rescans.is_empty() {
            return Ok(BTreeMap::new());
        }

        let mut global_min = u32::MAX;
        let mut global_max = 0u32;
        for r in &rescans {
            if r.resume_from > r.ceiling {
                continue;
            }
            global_min = global_min.min(r.resume_from);
            global_max = global_max.max(r.ceiling);
        }
        if global_min == u32::MAX {
            return Ok(BTreeMap::new());
        }

        let mut new_requests: BTreeMap<FilterMatchKey, Vec<BackfillAdvance>> = BTreeMap::new();
        let mut chunk_start = global_min;
        while chunk_start <= global_max {
            let chunk_end = chunk_start
                .saturating_add(BACKFILL_CHUNK_SIZE.saturating_sub(1))
                .min(global_max);

            let active: Vec<&PendingRescan> = rescans
                .iter()
                .filter(|r| r.resume_from <= chunk_end && r.ceiling >= chunk_start)
                .collect();
            if active.is_empty() {
                chunk_start = chunk_end.saturating_add(1);
                continue;
            }

            let mut union_addresses: HashSet<Address> = HashSet::new();
            for r in &active {
                for a in &r.addresses {
                    union_addresses.insert(a.clone());
                }
            }
            let address_vec: Vec<Address> = union_addresses.into_iter().collect();

            let filters = self.load_filters(chunk_start, chunk_end).await?;
            if filters.is_empty() {
                chunk_start = chunk_end.saturating_add(1);
                continue;
            }

            let matches = check_compact_filters_for_addresses(
                &filters,
                address_vec,
                chunk_start.saturating_sub(1),
            );

            let mut matched_range_keys: HashSet<(WalletId, AddressPoolType, u32, u32)> =
                HashSet::new();
            for key in &matches {
                let height = key.height();
                let hash = *key.hash();
                for r in &active {
                    if r.resume_from > height || r.ceiling < height {
                        continue;
                    }
                    matched_range_keys.insert((r.wallet_id, r.pool, r.indexes.start, r.indexes.end));
                    let pending = PendingAdvance {
                        wallet_id: r.wallet_id,
                        pool: r.pool,
                        indexes: r.indexes.clone(),
                        height,
                        advance_to: chunk_end,
                    };
                    let advance = pending.to_backfill_advance();
                    self.pending_advances.entry(hash).or_default().push(pending);
                    new_requests.entry(key.clone()).or_default().push(advance);
                }
            }

            // Active ranges that did NOT match anything in this chunk advance
            // straight to `chunk_end`. Ranges that matched defer the advance
            // until the block actually arrives so persistence stays atomic.
            let no_match: Vec<(WalletId, AddressPoolType, Range<u32>)> = active
                .iter()
                .filter(|r| {
                    !matched_range_keys.contains(&(
                        r.wallet_id,
                        r.pool,
                        r.indexes.start,
                        r.indexes.end,
                    ))
                })
                .map(|r| (r.wallet_id, r.pool, r.indexes.clone()))
                .collect();
            if !no_match.is_empty() {
                let mut wallet = self.wallet.write().await;
                for (wallet_id, pool, indexes) in no_match {
                    wallet.advance_rescan(&wallet_id, pool, indexes, chunk_end);
                }
            }

            tokio::task::yield_now().await;
            chunk_start = chunk_end.saturating_add(1);
        }

        Ok(new_requests)
    }

    /// Called by the orchestrator after a backfill block has been processed
    /// via [`WalletInterface::process_backfill_block_for_wallets`], which
    /// already advanced `caught_up_to` and emitted
    /// `WalletEvent::RescanBlockProcessed`. Removes the block from the
    /// pending set.
    ///
    /// Returns `true` when the hash was in the worker's pending set — i.e.
    /// the block was backfill's, not forward sync's.
    ///
    /// [`WalletInterface::process_backfill_block_for_wallets`]: key_wallet_manager::WalletInterface::process_backfill_block_for_wallets
    pub(crate) async fn on_block_processed(&mut self, hash: &BlockHash) -> bool {
        self.pending_advances.remove(hash).is_some()
    }

    async fn load_filters(
        &self,
        start: u32,
        end: u32,
    ) -> SyncResult<HashMap<FilterMatchKey, BlockFilter>> {
        let filter_data = self.filter_storage.read().await.load_filters(start..end + 1).await?;
        let headers = self.header_storage.read().await.load_headers(start..end + 1).await?;

        let mut out = HashMap::new();
        for (idx, (data, header)) in filter_data.iter().zip(headers.iter()).enumerate() {
            let height = start + idx as u32;
            let key = FilterMatchKey::new(height, header.block_hash());
            out.insert(key, BlockFilter::new(data));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{DiskStorageManager, StorageManager};
    use dashcore::block::Header;
    use dashcore::Network;
    use dashcore::{Block, Transaction};
    use key_wallet_manager::test_utils::{MockWalletState, MultiMockWallet};

    /// Backfill matched-block dispatch path: a sync range pending below the
    /// wallet's `synced_height` produces a matched filter at height H, the
    /// worker returns the obligation keyed by `FilterMatchKey`, and a
    /// follow-up call to `on_block_processed` after wallet processing
    /// clears the pending entry.
    #[tokio::test]
    async fn backfill_worker_returns_matched_block_obligations() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let wallet_id: WalletId = [0xAA; 32];
        let pool = AddressPoolType::External;
        let address = Address::dummy(Network::Regtest, 7);

        let multi = MultiMockWallet::new();
        let multi = Arc::new(RwLock::new(multi));
        {
            let mut w = multi.write().await;
            w.insert_wallet(
                wallet_id,
                MockWalletState {
                    addresses: vec![address.clone()],
                    synced_height: 200,
                    last_processed_height: 200,
                },
            );
            w.set_birth_height(wallet_id, 0);
            w.push_sync_range_for_test(wallet_id, pool, 5..10, 80, vec![address.clone()]);
        }

        // Real matching block at height 50; dummy elsewhere. Headers
        // populate offsets 0..=100 so the storage segment is fully filled.
        let match_height: u32 = 50;
        let tx = Transaction::dummy(&address, 0..0, &[match_height as u64]);
        let match_block = Block::dummy(match_height, vec![tx]);
        let match_block_hash = match_block.block_hash();
        let match_filter = BlockFilter::dummy(&match_block);

        let mut headers: Vec<Header> = Header::dummy_batch(0..101);
        headers[match_height as usize] = match_block.header;
        storage.block_headers().write().await.store_headers(&headers).await.unwrap();

        let dummy_filter = BlockFilter::new(&[0u8; 32]);
        let filter_store = storage.filters();
        {
            let mut fs = filter_store.write().await;
            for h in 0..=100u32 {
                let bytes = if h == match_height {
                    match_filter.content.clone()
                } else {
                    dummy_filter.content.clone()
                };
                fs.store_filter(h, &bytes).await.unwrap();
            }
        }

        let mut worker: BackfillWorker<_, _, MultiMockWallet> = BackfillWorker::new(
            storage.filters(),
            storage.block_headers(),
            multi.clone(),
        );

        let matched = worker.tick().await.unwrap();

        assert_eq!(matched.len(), 1, "expected one matched block: {:?}", matched);
        let (key, advances) = matched.iter().next().unwrap();
        assert_eq!(key.height(), match_height);
        assert_eq!(key.hash(), &match_block_hash);
        assert_eq!(advances.len(), 1);
        let adv = &advances[0];
        assert_eq!(adv.wallet_id, wallet_id);
        assert_eq!(adv.pool, pool);
        assert_eq!(adv.indexes, 5..10);
        assert!(adv.advance_to <= 79, "advance_to must not exceed since-1=79");

        assert!(worker.pending_advances.contains_key(&match_block_hash));
        let cleared = worker.on_block_processed(&match_block_hash).await;
        assert!(cleared);
        assert!(!worker.pending_advances.contains_key(&match_block_hash));
        assert!(!worker.on_block_processed(&match_block_hash).await);
    }
}

