# Plan: Batched Transaction Sync with Address Re-scanning

## Overview

Implement a batched approach to transaction synchronization that:
1. Downloads filters in batches of 10,000 blocks
2. Matches filters and downloads relevant blocks
3. Re-scans the batch if new HD addresses are generated
4. Repeats until no new addresses, then moves to next batch

## Key Insight

Commit `ce7f1de3` already added `BlockProcessingResult` with `new_addresses: Vec<Address>` field that tracks new addresses generated during gap limit maintenance. We can use this to detect when re-scanning is needed.

---

## Current Progress

- [x] Add `TRANSACTION_SYNC_BATCH_SIZE` constant to `dash-spv/src/sync/filters/types.rs`
- [x] Update `DownloadingTransactions` phase with batch tracking fields
- [x] Update phase execution logic for batch-based sync
- [x] Modify CFilter handler to store filters and track batch completion
- [x] Modify Block handler with re-scan logic for new addresses
- [x] Add `rescan_current_batch` and `advance_to_next_batch` methods
- [x] Add `clear_filter_cache` to WalletInterface
- [x] Update `SyncStage` for batch progress display
- [x] Update progress mapping in `progress.rs`
- [x] Build and test

---

## Review Summary

### Implementation Complete

All batched transaction sync functionality has been implemented. The key changes:

**dash-spv/src/sync/sequential/phases.rs:**
- Added batch tracking fields: `batch_start`, `batch_end`, `tip_height`, `current_batch`, `batches_total`
- Added filter storage: `stored_filters: HashMap<BlockHash, Vec<u8>>`
- Added re-scan tracking: `new_addresses_found`, `scan_pass`
- Updated `progress()` method to show batch progress

**dash-spv/src/sync/sequential/transitions.rs:**
- Added `create_transactions_phase()` that calculates batch parameters
- Updated `are_transactions_complete()` to check for final batch and no re-scan needed

**dash-spv/src/sync/sequential/phase_execution.rs:**
- Updated phase execution to use batch bounds
- Added `rescan_current_batch()` method to re-scan with new addresses
- Added `advance_to_next_batch()` method to progress through batches

**dash-spv/src/sync/sequential/message_handlers.rs:**
- CFilter handler now stores filters in `stored_filters` for potential re-scan
- Block handler tracks `new_addresses_found` flag from `BlockProcessingResult`
- Both handlers trigger batch completion logic (rescan or advance)

**key-wallet-manager/src/wallet_interface.rs:**
- Added `clear_filter_cache()` method to trait for re-scan support

**key-wallet-manager/src/wallet_manager/process_block.rs:**
- Implemented `clear_filter_cache()` to remove cached match results

**dash-spv/src/types.rs:**
- Updated `SyncStage::DownloadingTransactions` with batch progress fields

**dash-spv/src/client/progress.rs:**
- Updated `map_phase_to_stage()` to include batch information

### Testing

All 220 tests pass. Build completes with only minor warnings (unused imports fixed).

---

## Implementation Steps

### Step 1: Add Constants (DONE)

**File: `dash-spv/src/sync/filters/types.rs`**

Added:
```rust
pub const TRANSACTION_SYNC_BATCH_SIZE: u32 = 10_000;
```

### Step 2: Update `DownloadingTransactions` Phase

**File: `dash-spv/src/sync/sequential/phases.rs`**

Replace current phase fields with batch tracking:

```rust
DownloadingTransactions {
    start_time: Instant,
    // Batch tracking
    batch_start: u32,
    batch_end: u32,
    tip_height: u32,
    // Filter storage for re-scanning
    stored_filters: HashMap<BlockHash, Vec<u8>>,
    // Current batch state
    filters_downloaded: u32,
    filters_matched: HashSet<u32>,
    blocks_to_download: Vec<BlockHash>,
    blocks_downloading: HashMap<BlockHash, Instant>,
    blocks_completed: HashSet<BlockHash>,
    // Re-scan tracking
    new_addresses_found: bool,
    scan_pass: u32,
    // Progress
    last_progress: Instant,
    batches_total: u32,
    current_batch: u32,
}
```

Update methods: `name()`, `last_progress_time()`, `update_progress()`, `progress()`

### Step 3: Update Phase Execution Logic

**File: `dash-spv/src/sync/sequential/phase_execution.rs`**

Rewrite `execute_current_phase()` for `DownloadingTransactions`:

```rust
SyncPhase::DownloadingTransactions { .. } => {
    // 1. Calculate batch bounds
    let batch_start = current_batch * TRANSACTION_SYNC_BATCH_SIZE + base_height;
    let batch_end = (batch_start + TRANSACTION_SYNC_BATCH_SIZE - 1).min(tip_height);

    // 2. Download filters for this batch only
    self.filter_sync.sync_filters(
        network, storage,
        Some(batch_start),
        Some(batch_end - batch_start + 1)
    ).await?;
}
```

### Step 4: Modify CFilter Message Handler

**File: `dash-spv/src/sync/sequential/message_handlers.rs`**

When receiving CFilter:
1. **Store the filter data** for potential re-scanning
2. Check for matches
3. Track batch completion

```rust
// In handle_cfilter_message():
// Store filter for potential re-scan
if let SyncPhase::DownloadingTransactions { stored_filters, .. } = &mut self.current_phase {
    stored_filters.insert(cfilter.block_hash, cfilter.filter.clone());
}
```

### Step 5: Modify Block Message Handler with Re-scan Logic

**File: `dash-spv/src/sync/sequential/message_handlers.rs`**

When processing a block:
1. Get `BlockProcessingResult` from wallet
2. If `new_addresses` is non-empty, set `needs_rescan = true`
3. When all blocks in batch complete:
   - If `needs_rescan`: clear filter match cache, re-match stored filters
   - Else: advance to next batch

```rust
// In handle_block_message():
let result = wallet.process_block(&block, block_height, network).await;

// Check if new addresses were generated
if !result.new_addresses.is_empty() {
    tracing::info!("🔄 {} new addresses generated, will re-scan batch",
        result.new_addresses.len());
    if let SyncPhase::DownloadingTransactions { new_addresses_found, .. } = &mut self.current_phase {
        *new_addresses_found = true;
    }
}

// When all blocks complete, check if re-scan needed
if all_blocks_complete {
    if needs_rescan {
        self.rescan_current_batch(network, storage).await?;
    } else {
        self.advance_to_next_batch(network, storage).await?;
    }
}
```

### Step 6: Add Re-scan and Batch Advancement Methods

**File: `dash-spv/src/sync/sequential/phase_execution.rs`**

```rust
async fn rescan_current_batch(&mut self, network: &mut N, storage: &mut S) -> SyncResult<()> {
    if let SyncPhase::DownloadingTransactions {
        stored_filters,
        filters_matched,
        blocks_to_download,
        blocks_completed,
        new_addresses_found,
        scan_pass,
        ..
    } = &mut self.current_phase {
        tracing::info!("🔄 Re-scanning batch (pass {})", *scan_pass + 1);

        // Clear previous match state
        filters_matched.clear();
        blocks_to_download.clear();
        blocks_completed.clear();
        *new_addresses_found = false;
        *scan_pass += 1;

        // Clear wallet's filter match cache for this batch
        let mut wallet = self.wallet.write().await;
        for block_hash in stored_filters.keys() {
            wallet.clear_filter_cache(block_hash);
        }
        drop(wallet);

        // Re-match all stored filters
        for (block_hash, filter_data) in stored_filters.iter() {
            let matches = self.check_filter_for_matches(filter_data, block_hash).await?;
            if matches {
                // Queue block for download
            }
        }
    }
    Ok(())
}

async fn advance_to_next_batch(&mut self, network: &mut N, storage: &mut S) -> SyncResult<()> {
    if let SyncPhase::DownloadingTransactions {
        batch_start,
        batch_end,
        tip_height,
        stored_filters,
        current_batch,
        batches_total,
        ..
    } = &mut self.current_phase {
        // Clear stored filters (free memory)
        stored_filters.clear();

        *current_batch += 1;

        if *batch_end >= *tip_height {
            // All batches complete - transition to FullySynced
            self.transition_to_next_phase(storage, network, "All batches complete").await?;
        } else {
            // Start next batch
            let new_start = *batch_end + 1;
            let new_end = (new_start + TRANSACTION_SYNC_BATCH_SIZE - 1).min(*tip_height);
            *batch_start = new_start;
            *batch_end = new_end;

            tracing::info!("📦 Starting batch {}/{}: heights {} to {}",
                *current_batch + 1, *batches_total, new_start, new_end);

            // Execute filter download for new batch
            self.execute_current_phase(network, storage).await?;
        }
    }
    Ok(())
}
```

### Step 7: Add Filter Cache Clearing to WalletInterface

**File: `key-wallet-manager/src/wallet_interface.rs`**

```rust
/// Clear the filter match cache for a specific block
/// Called during re-scan to allow re-matching with new addresses
async fn clear_filter_cache(&mut self, block_hash: &BlockHash);
```

**File: `key-wallet-manager/src/wallet_manager/process_block.rs`**

```rust
async fn clear_filter_cache(&mut self, block_hash: &BlockHash) {
    for cache in self.filter_matches.values_mut() {
        cache.remove(block_hash);
    }
}
```

### Step 8: Update Progress Display

**File: `dash-spv/src/types.rs`**

Update `SyncStage::DownloadingTransactions`:

```rust
DownloadingTransactions {
    current_batch: u32,
    batches_total: u32,
    batch_filters_completed: u32,
    batch_filters_total: u32,
    blocks_pending: usize,
    scan_pass: u32,
}
```

**File: `dash-spv/src/client/progress.rs`**

Update `map_phase_to_stage()` to map new phase fields to stage.

---

## Files to Modify

| File | Changes |
|------|---------|
| `dash-spv/src/sync/filters/types.rs` | Add `TRANSACTION_SYNC_BATCH_SIZE` constant (DONE) |
| `dash-spv/src/sync/sequential/phases.rs` | Add batch tracking fields to phase |
| `dash-spv/src/sync/sequential/phase_execution.rs` | Batch-based execution, rescan, advance logic |
| `dash-spv/src/sync/sequential/message_handlers.rs` | Store filters, track new addresses, trigger rescan |
| `dash-spv/src/sync/sequential/transitions.rs` | Update phase creation with batch fields |
| `dash-spv/src/types.rs` | Update `SyncStage` for batch progress |
| `dash-spv/src/client/progress.rs` | Update progress mapping |
| `key-wallet-manager/src/wallet_interface.rs` | Add `clear_filter_cache()` method |
| `key-wallet-manager/src/wallet_manager/process_block.rs` | Implement cache clearing |

---

## Sync Flow

```
Start DownloadingTransactions
    │
    ▼
┌─────────────────────────────────────┐
│ Batch N (e.g., blocks 0-9999)       │
│                                     │
│  1. Download 10,000 filters         │
│     └─ Store filter data            │
│                                     │
│  2. Match filters → get block list  │
│                                     │
│  3. Download & process blocks       │
│     └─ Check new_addresses          │
│                                     │
│  4. All blocks done?                │
│     ├─ new_addresses found?         │
│     │   └─ YES: Clear cache,        │
│     │          re-match filters,    │
│     │          goto step 3          │
│     │                               │
│     └─ NO: Clear stored filters,    │
│            advance to Batch N+1     │
└─────────────────────────────────────┘
    │
    ▼ (when batch_end >= tip_height)
FullySynced
```

---

## Completion Criteria

Batch completes when:
1. All filters in batch downloaded
2. All matched blocks downloaded and processed
3. No new addresses generated in this scan pass

Phase completes when:
1. Final batch processed (batch_end >= tip_height)
2. No pending blocks or filters

---

## Memory Considerations

- Each batch stores up to 10,000 filters (~200 bytes each = ~2MB per batch)
- Filters cleared after batch advances
- Only one batch's filters in memory at a time

---

## Key Code References

- `BlockProcessingResult` with `new_addresses` field: commit `ce7f1de3`
- Current `DownloadingTransactions` phase: `dash-spv/src/sync/sequential/phases.rs`
- Filter matching: `dash-spv/src/sync/filters/matching.rs`
- Message handlers: `dash-spv/src/sync/sequential/message_handlers.rs`
- WalletInterface: `key-wallet-manager/src/wallet_interface.rs`
