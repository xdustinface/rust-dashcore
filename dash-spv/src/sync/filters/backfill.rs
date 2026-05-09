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

/// Sweep-line backfill worker over pending sync ranges.
///
/// Owns short-lived state (`pending_advances`) plus shared references to
/// storage and the wallet. The caller drives `tick` (typically wired to a
/// wake channel signalled when `pending_rescans` becomes non-empty) and
/// `on_block_processed` (when a block we requested has been downloaded
/// through the existing block path).
///
/// `pending_advances` is keyed by block hash because several sync ranges
/// can share an address that matched the same filter, so the value is a
/// `Vec<BackfillAdvance>`. Entries that no longer correspond to a live
/// pending sync range (range completed, range advanced past `height`, or
/// range removed by reorg clamp) are pruned at the top of every `tick`,
/// keeping the map bounded under repeated polling and download failures.
pub(crate) struct BackfillWorker<F, H, W> {
    filter_storage: Arc<RwLock<F>>,
    header_storage: Arc<RwLock<H>>,
    wallet: Arc<RwLock<W>>,
    pending_advances: HashMap<BlockHash, Vec<BackfillAdvance>>,
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
        self.prune_pending_advances(&rescans);
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

        // Clip to what's actually stored. During initial sync (or any time
        // forward filter sync is still downloading) the storage may not yet
        // hold filters past `filter_tip`; requesting them would panic in
        // segments::get_items. The next tick re-evaluates as forward sync
        // catches up.
        let filter_tip = self.filter_storage.read().await.filter_tip_height().await?;
        if global_min > filter_tip {
            return Ok(BTreeMap::new());
        }
        global_max = global_max.min(filter_tip);

        let mut new_requests: BTreeMap<FilterMatchKey, Vec<BackfillAdvance>> = BTreeMap::new();
        // Ranges that match in any chunk are excluded from no-match advance
        // for every later chunk too: a range with an in-flight matched block
        // must not have its `caught_up_to` advanced past that block's height
        // until the block is processed and `RescanBlockProcessed` fires.
        let mut matched_range_keys: HashSet<(WalletId, AddressPoolType, u32, u32)> = HashSet::new();
        let mut chunk_start = global_min;
        while chunk_start <= global_max {
            let chunk_end =
                chunk_start.saturating_add(BACKFILL_CHUNK_SIZE.saturating_sub(1)).min(global_max);

            let active: Vec<&PendingRescan> = rescans
                .iter()
                .filter(|r| {
                    r.resume_from <= r.ceiling
                        && r.resume_from <= chunk_end
                        && r.ceiling >= chunk_start
                })
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

            // `check_compact_filters_for_addresses` includes a filter when
            // `key.height() > min_height`, so we pass `chunk_start - 1` to
            // include the chunk's first height. For `chunk_start == 0`,
            // `0u32.saturating_sub(1) == 0`, which means filters at height
            // 0 (genesis) are skipped — acceptable in practice because the
            // genesis block has no spendable wallet outputs.
            let matches = check_compact_filters_for_addresses(
                &filters,
                address_vec,
                chunk_start.saturating_sub(1),
            );

            for key in &matches {
                let height = key.height();
                let hash = *key.hash();
                for r in &active {
                    if r.resume_from > height || r.ceiling < height {
                        continue;
                    }
                    let range_key = (r.wallet_id, r.pool, r.indexes.start, r.indexes.end);
                    matched_range_keys.insert(range_key);
                    let advance = BackfillAdvance {
                        wallet_id: r.wallet_id,
                        pool: r.pool,
                        indexes: r.indexes.clone(),
                        height,
                        advance_to: chunk_end,
                    };
                    let pending = self.pending_advances.entry(hash).or_default();
                    let already_pending = pending.iter().any(|p| {
                        p.wallet_id == advance.wallet_id
                            && p.pool == advance.pool
                            && p.indexes == advance.indexes
                    });
                    if !already_pending {
                        pending.push(advance.clone());
                    }
                    // Always re-emit the request so a previously-failed
                    // download is retried. The BlocksManager pipeline will
                    // dedup the actual network request via its own
                    // bookkeeping, but a backfill match must never fail to
                    // surface a request when a download was lost.
                    new_requests.entry(key.clone()).or_default().push(advance);
                }
            }

            // Active ranges that did NOT match anything in *any* chunk so far
            // advance straight to `chunk_end`. Ranges that matched anywhere
            // defer the advance until their matched block is processed so
            // persistence stays atomic.
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

    /// Drop pending advances whose corresponding sync range is no longer
    /// pending or has already advanced past the matched block's height.
    /// Keeps `pending_advances` bounded across reorgs, gap-extension churn,
    /// and download failures.
    ///
    /// Builds the live-key index by folding rather than collecting so two
    /// `PendingRescan` entries that hash to the same key (same wallet, pool,
    /// and indexes) keep the most conservative `resume_from` (the lowest)
    /// rather than silently dropping one. In a healthy wallet the key is
    /// already unique because pools collapse adjacent ranges; the fold makes
    /// the prune robust against a future regression in that invariant.
    fn prune_pending_advances(&mut self, rescans: &[PendingRescan]) {
        if self.pending_advances.is_empty() {
            return;
        }
        let mut live: HashMap<(WalletId, AddressPoolType, u32, u32), CoreBlockHeight> =
            HashMap::new();
        for r in rescans {
            let key = (r.wallet_id, r.pool, r.indexes.start, r.indexes.end);
            live.entry(key).and_modify(|v| *v = (*v).min(r.resume_from)).or_insert(r.resume_from);
        }
        self.pending_advances.retain(|_hash, advances| {
            advances.retain(|adv| {
                live.get(&(adv.wallet_id, adv.pool, adv.indexes.start, adv.indexes.end))
                    .map(|resume| adv.height >= *resume)
                    .unwrap_or(false)
            });
            !advances.is_empty()
        });
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

    /// Snapshot the set of block hashes this worker still considers
    /// in-flight. Used by the orchestrator to prune stale entries from
    /// the BlocksManager's parallel `backfill_advances` map after each
    /// tick, so cancelled or completed advances don't leak there either.
    pub(crate) fn live_block_hashes(&self) -> HashSet<BlockHash> {
        self.pending_advances.keys().copied().collect()
    }

    async fn load_filters(
        &self,
        start: u32,
        end: u32,
    ) -> SyncResult<HashMap<FilterMatchKey, BlockFilter>> {
        let filter_data = self.filter_storage.read().await.load_filters(start..end + 1).await?;
        let headers = self.header_storage.read().await.load_headers(start..end + 1).await?;

        // A length mismatch means the storage layer returned a partial view
        // for the chunk: filters truncated, headers truncated, or both. Pairing
        // by `zip` would silently drop the tail and the caller would advance
        // `caught_up_to` past heights that were never actually scanned,
        // permanently masking any transactions there. Surface the
        // inconsistency so the caller can recover.
        if filter_data.len() != headers.len() {
            return Err(crate::error::SyncError::Storage(format!(
                "backfill load_filters length mismatch for range {start}..={end}: filters={}, headers={}",
                filter_data.len(),
                headers.len(),
            )));
        }

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

        let mut worker: BackfillWorker<_, _, MultiMockWallet> =
            BackfillWorker::new(storage.filters(), storage.block_headers(), multi.clone());

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

    /// Regression for the original `crazy-task` blind spot: a block at H1
    /// pays addresses at indexes the wallet hadn't derived yet, so forward
    /// sync at H1 only matched the in-gap-limit address. A later tx at H2
    /// extends the gap; backfill must revisit H1 and recover the missed
    /// outputs by scanning against the post-extension address set.
    #[tokio::test]
    async fn backfill_recovers_missed_outputs_after_gap_limit_extension() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let wallet_id: WalletId = [0xCC; 32];
        let pool = AddressPoolType::External;

        // Index 0 was inside the original gap limit; 32..41 weren't.
        let addr_zero = Address::dummy(Network::Regtest, 100);
        let high_addrs: Vec<Address> =
            (32..41).map(|i| Address::dummy(Network::Regtest, 100 + i as usize)).collect();

        let multi = Arc::new(RwLock::new(MultiMockWallet::new()));
        {
            let mut w = multi.write().await;
            // The wallet's monitored set today still only contains index 0;
            // the post-extension addresses live on the pending sync range.
            w.insert_wallet(
                wallet_id,
                MockWalletState {
                    addresses: vec![addr_zero.clone()],
                    synced_height: 200,
                    last_processed_height: 200,
                },
            );
            w.set_birth_height(wallet_id, 0);
            w.push_sync_range_for_test(wallet_id, pool, 32..41, 200, high_addrs.clone());
        }

        // Block at H1=50 pays all 10 outputs (index 0 plus 32..41), so its
        // filter matches every address. Forward sync would have only checked
        // index 0; backfill scans against the post-extension slice and must
        // discover the high indexes.
        let h1: u32 = 50;
        let mut txs = vec![Transaction::dummy(&addr_zero, 0..0, &[1u64])];
        for (i, addr) in high_addrs.iter().enumerate() {
            txs.push(Transaction::dummy(addr, 1..2, &[(2 + i) as u64]));
        }
        let block_h1 = Block::dummy(h1, txs);
        let block_h1_hash = block_h1.block_hash();
        let filter_h1 = BlockFilter::dummy(&block_h1);

        let mut headers: Vec<Header> = Header::dummy_batch(0..201);
        headers[h1 as usize] = block_h1.header;
        storage.block_headers().write().await.store_headers(&headers).await.unwrap();

        let dummy_filter = BlockFilter::new(&[0u8; 32]);
        let filter_store = storage.filters();
        {
            let mut fs = filter_store.write().await;
            for h in 0..=200u32 {
                let bytes = if h == h1 {
                    filter_h1.content.clone()
                } else {
                    dummy_filter.content.clone()
                };
                fs.store_filter(h, &bytes).await.unwrap();
            }
        }

        let mut worker: BackfillWorker<_, _, MultiMockWallet> =
            BackfillWorker::new(storage.filters(), storage.block_headers(), multi.clone());

        let matched = worker.tick().await.unwrap();

        assert_eq!(matched.len(), 1, "expected one matched block, got {:?}", matched);
        let (key, advances) = matched.iter().next().unwrap();
        assert_eq!(key.height(), h1);
        assert_eq!(key.hash(), &block_h1_hash);
        assert_eq!(advances.len(), 1, "one advance entry expected for the single range");
        let adv = &advances[0];
        assert_eq!(adv.wallet_id, wallet_id);
        assert_eq!(adv.pool, pool);
        assert_eq!(adv.indexes, 32..41);
        assert!(
            adv.advance_to <= 199,
            "advance_to must not exceed since_height-1=199, got {}",
            adv.advance_to,
        );
    }

    /// `RescanBlockProcessed` must carry both the records (`inserted`) and
    /// the `advance_to` field in a single event so a downstream persister
    /// writes them atomically. The forward-sync `BlockProcessed` event must
    /// not also fire for the same backfill block, otherwise the persister
    /// would double-write.
    #[tokio::test]
    async fn rescan_block_processed_bundles_advance_in_a_single_event() {
        use key_wallet_manager::BackfillAdvance;

        let multi = Arc::new(RwLock::new(MultiMockWallet::new()));
        let wallet_id: WalletId = [0xDD; 32];
        let pool = AddressPoolType::External;
        let address = Address::dummy(Network::Regtest, 51);
        let mut event_rx = {
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
            // Push a range whose since-1 exactly matches the advance_to
            // below so the wallet completes and drops it. That side effect
            // confirms the "records + advance" pair is processed together.
            w.push_sync_range_for_test(wallet_id, pool, 5..10, 50, vec![address.clone()]);
            w.subscribe_events()
        };

        let block = Block::dummy(50, vec![Transaction::dummy(&address, 0..0, &[7u64])]);

        {
            let mut w = multi.write().await;
            w.process_backfill_block_for_wallets(
                &block,
                50,
                &[BackfillAdvance {
                    wallet_id,
                    pool,
                    indexes: 5..10,
                    height: 50,
                    advance_to: 49,
                }],
            )
            .await;
        }

        let mut events = Vec::new();
        while let Ok(ev) = event_rx.try_recv() {
            events.push(ev);
        }

        let rescan_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, key_wallet_manager::WalletEvent::RescanBlockProcessed { .. }))
            .collect();
        assert_eq!(
            rescan_events.len(),
            1,
            "exactly one RescanBlockProcessed expected, got {:?}",
            events,
        );
        match rescan_events[0] {
            key_wallet_manager::WalletEvent::RescanBlockProcessed {
                wallet_id: wid,
                height,
                pool: ev_pool,
                indexes,
                advance_to,
                ..
            } => {
                assert_eq!(*wid, wallet_id);
                assert_eq!(*height, 50);
                assert_eq!(*ev_pool, pool);
                assert_eq!(indexes, &(5..10));
                assert_eq!(*advance_to, 49);
            }
            _ => unreachable!(),
        }

        let block_processed_for_hash = events
            .iter()
            .any(|e| matches!(e, key_wallet_manager::WalletEvent::BlockProcessed { .. },));
        assert!(
            !block_processed_for_hash,
            "backfill block must not also fire BlockProcessed: {:?}",
            events,
        );

        // Sanity: the mock's advance_rescan side effect drops the range,
        // confirming records-and-advance are tied to the same operation.
        let pending = multi.read().await.pending_rescans();
        assert!(
            pending.is_empty(),
            "advance_to=since-1 must complete and drop the range, got {:?}",
            pending,
        );
    }

    /// A range whose backfill window straddles the chunk boundary must
    /// take more than one iteration of `tick`'s inner loop. The first chunk
    /// covers `[0..=BACKFILL_CHUNK_SIZE-1]` and advances `caught_up_to` to
    /// `chunk_end`; the second covers `[BACKFILL_CHUNK_SIZE..=ceiling]`.
    /// Verifies the loop's `chunk_start = chunk_end + 1` step, exercising
    /// the cross-chunk advance the existing single-chunk tests do not.
    #[tokio::test]
    async fn backfill_worker_walks_multiple_chunks_for_a_long_range() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let wallet_id: WalletId = [0xEE; 32];
        let pool = AddressPoolType::External;
        let address = Address::dummy(Network::Regtest, 200);

        // since=BACKFILL_CHUNK_SIZE+2 → ceiling=BACKFILL_CHUNK_SIZE+1, well
        // past the first chunk's end at BACKFILL_CHUNK_SIZE-1.
        let since: u32 = BACKFILL_CHUNK_SIZE + 2;
        let ceiling: u32 = since - 1;

        let multi = Arc::new(RwLock::new(MultiMockWallet::new()));
        {
            let mut w = multi.write().await;
            w.insert_wallet(
                wallet_id,
                MockWalletState {
                    addresses: vec![address.clone()],
                    synced_height: since + 100,
                    last_processed_height: since + 100,
                },
            );
            w.set_birth_height(wallet_id, 0);
            w.push_sync_range_for_test(wallet_id, pool, 5..10, since, vec![address.clone()]);
        }

        // No matching filters anywhere — every chunk hits the no-match
        // advance path, so the range completes and drops.
        let dummy_filter = BlockFilter::new(&[0u8; 32]);
        let header_end = ceiling + 1;
        let headers: Vec<Header> = Header::dummy_batch(0..header_end);
        storage.block_headers().write().await.store_headers(&headers).await.unwrap();
        let filter_store = storage.filters();
        {
            let mut fs = filter_store.write().await;
            for h in 0..=ceiling {
                fs.store_filter(h, &dummy_filter.content).await.unwrap();
            }
        }

        let mut worker: BackfillWorker<_, _, MultiMockWallet> =
            BackfillWorker::new(storage.filters(), storage.block_headers(), multi.clone());

        let matched = worker.tick().await.unwrap();
        assert!(matched.is_empty(), "no filter matches expected, got {:?}", matched);

        // After two chunk iterations the no-match advance pushed
        // `caught_up_to` to `ceiling`, completing the range.
        let pending = multi.read().await.pending_rescans();
        assert!(
            pending.is_empty(),
            "multi-chunk no-match sweep must complete and drop the range, got {:?}",
            pending,
        );
    }

    /// During initial sync the storage may hold filters only up to some
    /// `filter_tip` while a pending sync range's `ceiling` extends beyond.
    /// `tick` must clip its scan window to the available tip so it does not
    /// request out-of-range offsets and panic in `segments::get_items`. The
    /// next tick will re-evaluate as forward sync downloads more filters.
    #[tokio::test]
    async fn backfill_tick_skips_chunks_past_available_filter_tip() {
        let storage = DiskStorageManager::with_temp_dir().await.unwrap();
        let wallet_id: WalletId = [0xDD; 32];
        let pool = AddressPoolType::External;
        let address = Address::dummy(Network::Regtest, 300);

        // Sync range expects backfill through ceiling 9_999, but only the
        // first 100 filters are stored locally so far.
        let since: u32 = 10_000;
        let stored_through: u32 = 99;

        let multi = Arc::new(RwLock::new(MultiMockWallet::new()));
        {
            let mut w = multi.write().await;
            w.insert_wallet(
                wallet_id,
                MockWalletState {
                    addresses: vec![address.clone()],
                    synced_height: since + 100,
                    last_processed_height: since + 100,
                },
            );
            w.set_birth_height(wallet_id, 0);
            w.push_sync_range_for_test(wallet_id, pool, 5..10, since, vec![address.clone()]);
        }

        let dummy_filter = BlockFilter::new(&[0u8; 32]);
        let headers: Vec<Header> = Header::dummy_batch(0..(stored_through + 1));
        storage.block_headers().write().await.store_headers(&headers).await.unwrap();
        let filter_store = storage.filters();
        {
            let mut fs = filter_store.write().await;
            for h in 0..=stored_through {
                fs.store_filter(h, &dummy_filter.content).await.unwrap();
            }
        }

        let mut worker: BackfillWorker<_, _, MultiMockWallet> =
            BackfillWorker::new(storage.filters(), storage.block_headers(), multi.clone());

        // The bug: this used to panic in segments::get_items when the
        // chunk extended past stored_through.
        let matched = worker.tick().await.unwrap();
        assert!(matched.is_empty(), "no matches expected, got {:?}", matched);

        // The clipped chunk had no filter matches, so the active range's
        // caught_up_to advanced to filter_tip; range stays pending until
        // forward sync downloads the rest.
        let pending = multi.read().await.pending_rescans();
        assert_eq!(pending.len(), 1, "range still pending past filter_tip");
        assert_eq!(pending[0].resume_from, stored_through + 1);
    }
}
