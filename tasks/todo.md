# Task: Merge DownloadingFilters + DownloadingBlocks → DownloadingTransactions

## Analysis Summary

### Current Architecture
The sync manager uses 7 phases:
1. `Idle` - Not syncing
2. `DownloadingHeaders` - Downloads block headers
3. `DownloadingMnList` - Downloads masternode lists
4. `DownloadingCFHeaders` - Downloads compact filter headers
5. `DownloadingFilters` - Downloads compact filters and checks for matches
6. `DownloadingBlocks` - Downloads full blocks for matched filters
7. `FullySynced` - Done syncing

### Rationale for Merge
- Both phases are about finding wallet transactions
- Filters identify which blocks contain relevant transactions
- Blocks provide the actual transaction data
- Currently `DownloadingBlocks` is mostly a placeholder (transitions immediately)
- Blocks are already requested during filter processing when matches are found
- The phases are logically one operation: "find and download my transactions"

### New Combined Phase: `DownloadingTransactions`
Will track:
- Filter download progress (from `DownloadingFilters`)
- Block download progress (from `DownloadingBlocks`)
- Both CFilter and Block message handling

---

## Files to Modify

### 1. `dash-spv/src/sync/sequential/phases.rs`
- Remove `DownloadingFilters` and `DownloadingBlocks` variants
- Add `DownloadingTransactions` combining fields from both
- Update `name()`, `last_progress_time()`, `update_progress()`, `progress()` methods

### 2. `dash-spv/src/sync/sequential/transitions.rs`
- Update transitions: `DownloadingCFHeaders` → `DownloadingTransactions` → `FullySynced`
- Update `get_next_phase()` method
- Update/merge helper methods for completion checks

### 3. `dash-spv/src/types.rs`
- Update `SyncStage` enum: merge into `DownloadingTransactions`

### 4. `dash-spv/src/sync/sequential/message_handlers.rs`
- Update `is_message_expected_in_phase()` to handle `DownloadingTransactions` for both `CFilter` and `Block`
- Update message routing in `handle_message()`
- Update `handle_cfilter_message()` and `handle_block_message()` for new phase

### 5. `dash-spv/src/sync/sequential/phase_execution.rs`
- Update `execute_current_phase()` for new phase
- Update `check_timeout()` for new phase
- Update stats calculation methods to look for new phase name

### 6. `dash-spv/src/client/progress.rs`
- Update `map_phase_to_stage()` mapping

---

## Todo List

- [ ] Update `SyncPhase` enum in phases.rs
- [ ] Update phase methods in phases.rs (name, progress, etc.)
- [ ] Update `SyncStage` enum in types.rs
- [ ] Update transition logic in transitions.rs
- [ ] Update message handlers in message_handlers.rs
- [ ] Update phase execution in phase_execution.rs
- [ ] Update progress mapping in progress.rs
- [ ] Run tests to verify changes
- [ ] Fix any compilation errors

---

## Review

### Summary of Changes
Successfully merged `DownloadingFilters` and `DownloadingBlocks` phases into single `DownloadingTransactions` phase.

### Files Modified
1. **`dash-spv/src/sync/sequential/phases.rs`**
   - Replaced `DownloadingFilters` and `DownloadingBlocks` variants with `DownloadingTransactions`
   - Combined fields: filter tracking + block tracking in one variant
   - Updated `name()`, `last_progress_time()`, `update_progress()`, `progress()` methods

2. **`dash-spv/src/types.rs`**
   - Replaced `DownloadingFilters` and `DownloadingBlocks` in `SyncStage` with `DownloadingTransactions`
   - New variant has `filters_completed`, `filters_total`, `blocks_pending` fields

3. **`dash-spv/src/sync/sequential/transitions.rs`**
   - Updated transition rules: `DownloadingCFHeaders` → `DownloadingTransactions` → `FullySynced`
   - Replaced `are_filters_complete()` and `are_blocks_complete()` with `are_transactions_complete()`
   - Updated `get_next_phase()` to create the new combined phase

4. **`dash-spv/src/sync/sequential/message_handlers.rs`**
   - Updated `is_message_expected_in_phase()` to accept both `CFilter` and `Block` in `DownloadingTransactions`
   - Updated `handle_message()` routing
   - Modified `handle_cfilter_message()` and `handle_block_message()` to work with combined phase

5. **`dash-spv/src/sync/sequential/phase_execution.rs`**
   - Combined execution logic for filters and blocks
   - Updated `check_timeout()` for the new phase
   - Updated stats calculation methods to look for "Downloading Transactions" phase name
   - Removed unused `no_more_pending_blocks()` method

6. **`dash-spv/src/client/progress.rs`**
   - Updated `map_phase_to_stage()` for new phase

7. **`dash-spv/src/sync/sequential/manager.rs`**
   - Renamed `is_in_downloading_blocks_phase()` to `is_in_downloading_transactions_phase()`

8. **`dash-spv/src/client/message_handler.rs`**
   - Updated method call to use new name

### Test Results
- All 226 unit tests pass
- All integration tests pass
- Build succeeds with only pre-existing warnings

### Behavior Changes
- No functional changes - this is a pure refactor
- The sync process still downloads filters first, requests blocks when filters match
- Progress is now shown as a single combined "Downloading Transactions" phase