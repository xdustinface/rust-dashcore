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

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use dashcore::bip158::BlockFilter;
use dashcore::prelude::CoreBlockHeight;
use dashcore::{Address, BlockHash};
use key_wallet::managed_account::address_pool::AddressPoolType;
use key_wallet_manager::{
    check_compact_filters_for_addresses, FilterMatchKey, PendingRescan, WalletId, WalletInterface,
};
use tokio::sync::RwLock;

use crate::error::SyncResult;
use crate::storage::{BlockHeaderStorage, FilterStorage};

/// Maximum filters scanned per chunk before yielding back to the runtime.
/// Matches the forward-sync batch size so disk reads stay cache-friendly.
const BACKFILL_CHUNK_SIZE: u32 = 5000;

/// Per-block obligation tracked while waiting for a backfill block to be
/// downloaded and processed. Held in [`BackfillWorker::pending_advances`]
/// keyed by block hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingAdvance {
    pub wallet_id: WalletId,
    pub pool: AddressPoolType,
    pub indexes: Range<u32>,
    pub height: CoreBlockHeight,
    pub advance_to: CoreBlockHeight,
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
    pending_advances: HashMap<BlockHash, PendingAdvance>,
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
    /// the matched block hashes for download.
    ///
    /// Yields between chunks via `tokio::task::yield_now()` so forward sync
    /// continues to make progress.
    pub(crate) async fn tick(&mut self) -> SyncResult<Vec<BlockHash>> {
        let rescans = self.wallet.read().await.pending_rescans();
        if rescans.is_empty() {
            return Ok(Vec::new());
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
            return Ok(Vec::new());
        }

        let mut new_requests: Vec<BlockHash> = Vec::new();
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
                    self.pending_advances.insert(
                        hash,
                        PendingAdvance {
                            wallet_id: r.wallet_id,
                            pool: r.pool,
                            indexes: r.indexes.clone(),
                            height,
                            advance_to: chunk_end,
                        },
                    );
                    new_requests.push(hash);
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

    /// Called by the orchestrator when a block requested via [`tick`] has
    /// been downloaded and processed by the existing block-handling path.
    ///
    /// Advances `caught_up_to` for the corresponding sync range and returns
    /// the obligation so the caller can emit
    /// `WalletEvent::RescanBlockProcessed` with the tx records the block
    /// produced (the worker does not see those records itself).
    ///
    /// Returns `None` when the hash was not in the worker's pending set —
    /// e.g. the block was forward sync's, not ours.
    pub(crate) async fn on_block_processed(
        &mut self,
        hash: &BlockHash,
    ) -> Option<PendingAdvance> {
        let entry = self.pending_advances.remove(hash)?;
        let mut wallet = self.wallet.write().await;
        wallet.advance_rescan(&entry.wallet_id, entry.pool, entry.indexes.clone(), entry.advance_to);
        Some(entry)
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

