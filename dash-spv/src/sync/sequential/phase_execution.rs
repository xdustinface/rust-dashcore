//! Phase execution, transitions, timeout handling, and recovery logic.

use std::time::Instant;

use dashcore::BlockHash;

use crate::error::{SyncError, SyncResult};
use crate::network::NetworkManager;
use crate::storage::StorageManager;
use crate::sync::filters::types::TRANSACTION_SYNC_BATCH_SIZE;
use key_wallet_manager::wallet_interface::WalletInterface;

use super::manager::SequentialSyncManager;
use super::phases::{StoredFilter, SyncPhase};

impl<
        S: StorageManager + Send + Sync + 'static,
        N: NetworkManager + Send + Sync + 'static,
        W: WalletInterface,
    > SequentialSyncManager<S, N, W>
{
    /// Execute the current sync phase
    pub(super) async fn execute_current_phase(
        &mut self,
        network: &mut N,
        storage: &mut S,
    ) -> SyncResult<()> {
        match &self.current_phase {
            SyncPhase::DownloadingHeaders {
                ..
            } => {
                tracing::info!("📥 Starting header download phase");
                // Don't call start_sync if already prepared - just send the request
                if self.header_sync.is_syncing() {
                    // Already prepared, just send the initial request
                    let base_hash = self.get_base_hash_from_storage(storage).await?;

                    self.header_sync.request_headers(network, base_hash).await?;
                } else {
                    // Not prepared yet, start sync normally
                    self.header_sync.start_sync(network, storage).await?;
                }
            }

            SyncPhase::DownloadingMnList {
                ..
            } => {
                tracing::info!("📥 Starting masternode list download phase");
                // Get the effective chain height from header sync which accounts for checkpoint base
                let effective_height = self.header_sync.get_chain_height();
                let sync_base_height = self.header_sync.get_sync_base_height();

                // Also get the actual tip height to verify (blockchain height)
                let storage_tip = storage
                    .get_tip_height()
                    .await
                    .map_err(|e| SyncError::Storage(format!("Failed to get storage tip: {}", e)))?;

                // Debug: Check chain state
                let chain_state = storage.load_chain_state().await.map_err(|e| {
                    SyncError::Storage(format!("Failed to load chain state: {}", e))
                })?;
                let chain_state_height = chain_state.as_ref().map(|s| s.get_height()).unwrap_or(0);

                tracing::info!(
                    "Starting masternode sync: effective_height={}, sync_base={}, storage_tip={:?}, chain_state_height={}, expected_storage_index={}",
                    effective_height,
                    sync_base_height,
                    storage_tip,
                    chain_state_height,
                    if sync_base_height > 0 { effective_height.saturating_sub(sync_base_height) } else { effective_height }
                );

                // Use the minimum of effective height and what's actually in storage
                let _safe_height = if let Some(tip) = storage_tip {
                    let storage_based_height = tip;
                    if storage_based_height < effective_height {
                        tracing::warn!(
                            "Chain state height {} exceeds storage height {}, using storage height",
                            effective_height,
                            storage_based_height
                        );
                        storage_based_height
                    } else {
                        effective_height
                    }
                } else {
                    effective_height
                };

                // Start masternode sync (unified processing)
                match self.masternode_sync.start_sync(network, storage).await {
                    Ok(_) => {
                        tracing::info!("🚀 Masternode sync initiated successfully, will complete when QRInfo arrives");
                    }
                    Err(e) => {
                        tracing::error!("❌ Failed to start masternode sync: {}", e);
                        return Err(e);
                    }
                }
            }

            SyncPhase::DownloadingCFHeaders {
                ..
            } => {
                tracing::info!("📥 Starting filter header download phase");

                // Get sync base height from header sync
                let sync_base_height = self.header_sync.get_sync_base_height();
                if sync_base_height > 0 {
                    tracing::info!(
                        "Setting filter sync base height to {} for checkpoint sync",
                        sync_base_height
                    );
                    self.filter_sync.set_sync_base_height(sync_base_height);
                }

                let sync_started =
                    self.filter_sync.start_sync_filter_headers(network, storage).await?;

                if !sync_started {
                    // No peers support compact filters or already up to date
                    tracing::info!("Filter header sync not started (no peers support filters or already synced)");
                    // Transition to next phase immediately
                    self.transition_to_next_phase(
                        storage,
                        network,
                        "Filter sync skipped - no peer support",
                    )
                    .await?;
                    // Return early to let the main sync loop execute the next phase
                    return Ok(());
                }
            }

            SyncPhase::DownloadingTransactions {
                batch_start,
                batch_end,
                current_batch,
                total_batches,
                scan_pass,
                ..
            } => {
                let batch_start = *batch_start;
                let batch_end = *batch_end;
                let current_batch = *current_batch;
                let total_batches = *total_batches;
                let scan_pass = *scan_pass;

                if scan_pass > 0 {
                    tracing::info!(
                        "📥 Re-scanning batch {}/{} (pass {}): heights {} to {}",
                        current_batch + 1,
                        total_batches,
                        scan_pass + 1,
                        batch_start,
                        batch_end
                    );
                } else {
                    tracing::info!(
                        "📥 Starting batch {}/{}: heights {} to {}",
                        current_batch + 1,
                        total_batches,
                        batch_start,
                        batch_end
                    );
                }

                let count = batch_end - batch_start + 1;

                // Update the phase to track the expected total for this batch
                if let SyncPhase::DownloadingTransactions {
                    total_filters,
                    ..
                } = &mut self.current_phase
                {
                    *total_filters = count;
                }

                // Use the filter sync manager to download filters for this batch only
                // Blocks will be requested automatically when filters match
                self.filter_sync
                    .sync_filters(network, storage, Some(batch_start), Some(count))
                    .await?;
            }

            _ => {
                // Idle or FullySynced - nothing to execute
            }
        }

        Ok(())
    }

    /// Transition to the next phase
    pub(super) async fn transition_to_next_phase(
        &mut self,
        storage: &mut S,
        network: &N,
        reason: &str,
    ) -> SyncResult<()> {
        // Get the next phase
        let next_phase =
            self.transition_manager.get_next_phase(&self.current_phase, storage, network).await?;

        if let Some(next) = next_phase {
            // Check if transition is allowed
            if !self
                .transition_manager
                .can_transition_to(&self.current_phase, &next, storage)
                .await?
            {
                return Err(SyncError::Validation(format!(
                    "Invalid phase transition from {} to {}",
                    self.current_phase.name(),
                    next.name()
                )));
            }

            // Create transition record
            let transition = self.transition_manager.create_transition(
                &self.current_phase,
                &next,
                reason.to_string(),
            );

            tracing::info!(
                "🔄 Phase transition: {} → {} (reason: {})",
                transition.from_phase,
                transition.to_phase,
                transition.reason
            );

            // Log final progress of the phase
            if let Some(ref progress) = transition.final_progress {
                tracing::info!(
                    "📊 Phase {} completed: {} items in {:?} ({:.1} items/sec)",
                    transition.from_phase,
                    progress.items_completed,
                    progress.elapsed,
                    progress.rate
                );
            }

            self.phase_history.push(transition);
            self.current_phase = next;
            self.current_phase_retries = 0;

            // Start the next phase
            // Note: We can't execute the next phase here as we don't have network access
            // The caller will need to execute the next phase
        } else {
            tracing::info!("✅ Sequential sync complete!");

            // Calculate total sync stats
            if let Some(start_time) = self.sync_start_time {
                let total_time = start_time.elapsed();
                let headers_synced = self.calculate_total_headers_synced();
                let filters_synced = self.calculate_total_filters_synced();
                let blocks_downloaded = self.calculate_total_blocks_downloaded();

                self.current_phase = SyncPhase::FullySynced {
                    sync_completed_at: Instant::now(),
                    total_sync_time: total_time,
                    headers_synced,
                    filters_synced,
                    blocks_downloaded,
                };

                tracing::info!(
                    "🎉 Sync completed in {:?} - {} headers, {} filters, {} blocks",
                    total_time,
                    headers_synced,
                    filters_synced,
                    blocks_downloaded
                );
            }
        }

        Ok(())
    }

    /// Check for timeouts and handle recovery
    pub async fn check_timeout(&mut self, network: &mut N, storage: &mut S) -> SyncResult<()> {
        // First check if the current phase needs to be executed (e.g., after a transition)
        if self.current_phase_needs_execution() {
            tracing::info!("Executing phase {} after transition", self.current_phase.name());
            self.execute_current_phase(network, storage).await?;
            return Ok(());
        }

        if let Some(last_progress) = self.current_phase.last_progress_time() {
            if last_progress.elapsed() > self.phase_timeout {
                tracing::warn!(
                    "⏰ Phase {} timed out after {:?}",
                    self.current_phase.name(),
                    self.phase_timeout
                );

                // Attempt recovery
                self.recover_from_timeout(network, storage).await?;
            }
        }

        // Also check phase-specific timeouts
        match &self.current_phase {
            SyncPhase::DownloadingHeaders {
                ..
            } => {
                self.header_sync.check_sync_timeout(storage, network).await?;
            }
            SyncPhase::DownloadingCFHeaders {
                ..
            } => {
                self.filter_sync.check_filter_header_request_timeouts(network, storage).await?;
            }
            SyncPhase::DownloadingMnList {
                ..
            } => {
                self.masternode_sync.check_sync_timeout(storage, network).await?;

                // After checking timeout, see if sync completed (either normally or via timeout)
                if !self.masternode_sync.is_syncing() {
                    tracing::info!("Masternode sync completed (detected in timeout check), transitioning to next phase");
                    self.transition_to_next_phase(storage, network, "Masternode sync complete")
                        .await?;
                    self.execute_current_phase(network, storage).await?;
                }
            }
            SyncPhase::DownloadingTransactions {
                ..
            } => {
                // Always check for timed out filter requests, not just during phase timeout
                self.filter_sync.check_filter_request_timeouts(network, storage).await?;

                // For transaction downloads, we need custom timeout handling
                if let Some(last_progress) = self.current_phase.last_progress_time() {
                    if last_progress.elapsed() > self.phase_timeout {
                        tracing::warn!(
                            "⏰ Transaction download phase timed out after {:?}",
                            self.phase_timeout
                        );

                        // Check if we have any active requests
                        let active_count = self.filter_sync.active_request_count();
                        let pending_count = self.filter_sync.pending_download_count();

                        tracing::warn!(
                            "Transaction sync status: {} active requests, {} pending",
                            active_count,
                            pending_count
                        );

                        // First check for timed out filter requests
                        self.filter_sync.check_filter_request_timeouts(network, storage).await?;

                        // Try to recover by sending more requests if we have pending ones
                        if self.filter_sync.has_pending_filter_requests() && active_count < 10 {
                            tracing::info!("Attempting to recover by sending more filter requests");
                            self.filter_sync.send_next_filter_batch(network).await?;
                            self.current_phase.update_progress();
                        } else if active_count == 0
                            && !self.filter_sync.has_pending_filter_requests()
                        {
                            // No active requests and no pending - we're stuck
                            tracing::error!(
                                "Transaction sync stalled with no active or pending requests"
                            );

                            // Check if we received some filters but not all
                            let received_count = self.filter_sync.get_received_filter_count();
                            if let SyncPhase::DownloadingTransactions {
                                total_filters,
                                ..
                            } = &self.current_phase
                            {
                                if received_count > 0 && received_count < *total_filters {
                                    tracing::warn!(
                                        "Transaction sync stalled at {}/{} filters - attempting recovery",
                                        received_count, total_filters
                                    );

                                    // Retry the entire transaction sync phase
                                    self.current_phase_retries += 1;
                                    if self.current_phase_retries <= self.max_phase_retries {
                                        tracing::info!(
                                            "🔄 Retrying transaction sync (attempt {}/{})",
                                            self.current_phase_retries,
                                            self.max_phase_retries
                                        );

                                        // Clear the filter sync state and restart
                                        self.filter_sync.reset();
                                        self.filter_sync.set_syncing_filters(false); // Allow restart

                                        // Update progress to prevent immediate timeout
                                        self.current_phase.update_progress();

                                        // Re-execute the phase
                                        self.execute_current_phase(network, storage).await?;
                                        return Ok(());
                                    } else {
                                        tracing::error!(
                                            "Transaction sync failed after {} retries, forcing completion",
                                            self.max_phase_retries
                                        );
                                    }
                                }
                            }

                            // Force transition to next phase to avoid permanent stall
                            self.transition_to_next_phase(
                                storage,
                                network,
                                "Transaction sync timeout - forcing completion",
                            )
                            .await?;
                        }
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Recover from a timeout
    async fn recover_from_timeout(&mut self, network: &mut N, storage: &mut S) -> SyncResult<()> {
        self.current_phase_retries += 1;

        if self.current_phase_retries > self.max_phase_retries {
            return Err(SyncError::Timeout(format!(
                "Phase {} failed after {} retries",
                self.current_phase.name(),
                self.max_phase_retries
            )));
        }

        tracing::warn!(
            "🔄 Retrying phase {} (attempt {}/{})",
            self.current_phase.name(),
            self.current_phase_retries,
            self.max_phase_retries
        );

        // Update progress time to prevent immediate re-timeout
        self.current_phase.update_progress();

        // Execute phase-specific recovery
        match &self.current_phase {
            SyncPhase::DownloadingHeaders {
                ..
            } => {
                self.header_sync.check_sync_timeout(storage, network).await?;
            }
            SyncPhase::DownloadingMnList {
                ..
            } => {
                self.masternode_sync.check_sync_timeout(storage, network).await?;
            }
            SyncPhase::DownloadingCFHeaders {
                ..
            } => {
                self.filter_sync.check_filter_header_request_timeouts(network, storage).await?;
            }
            _ => {
                // For other phases, we'll need phase-specific recovery
            }
        }

        Ok(())
    }

    /// Re-scan the current batch after new addresses were generated
    /// This checks ALL filters against only the newly generated addresses
    pub(super) async fn rescan_current_batch(
        &mut self,
        network: &mut N,
        storage: &mut S,
    ) -> SyncResult<()> {
        // Collect the stored filters and new addresses to avoid borrow issues
        // Vec<StoredFilter> is already in height order since filters arrive sequentially
        let (filters_to_check, addresses_to_check): (Vec<StoredFilter>, Vec<dashcore::Address>) =
            if let SyncPhase::DownloadingTransactions {
                stored_filters,
                new_addresses,
                ..
            } = &self.current_phase
            {
                (
                    stored_filters.iter().cloned().collect(),
                    new_addresses.clone(),
                )
            } else {
                return Ok(());
            };

        if addresses_to_check.is_empty() {
            tracing::info!("🔄 Rescan: No new addresses to check, advancing to next batch");
            self.advance_to_next_batch(network, storage).await?;
            return Ok(());
        }

        // Update phase state - clear the new_addresses and increment scan_pass
        if let SyncPhase::DownloadingTransactions {
            new_addresses,
            scan_pass,
            last_progress,
            ..
        } = &mut self.current_phase
        {
            tracing::info!(
                "🔄 Re-scanning batch (pass {}): checking {} filters against {} new addresses",
                *scan_pass + 1,
                filters_to_check.len(),
                addresses_to_check.len()
            );

            new_addresses.clear();
            *scan_pass += 1;
            *last_progress = Instant::now();
        }

        // Check ALL filters against only the new addresses
        // Filters are already in height order from the Vec
        let mut matches_found: Vec<(BlockHash, u32)> = Vec::new();
        for stored_filter in filters_to_check {
            let filter = dashcore::bip158::BlockFilter::new(&stored_filter.filter_data);

            // Check if filter matches any of the new addresses
            let wallet = self.wallet.read().await;
            let matches = wallet
                .check_filter_against_addresses(&filter, &stored_filter.block_hash, &addresses_to_check, self.config.network)
                .await;
            drop(wallet);

            // Queue block if it matches the new addresses
            if matches {
                tracing::info!(
                    "🔄 Rescan: New match at height {} (block {})",
                    stored_filter.height,
                    stored_filter.block_hash
                );
                matches_found.push((stored_filter.block_hash, stored_filter.height));
            }
        }

        // matches_found is already in height order since we iterated the Vec in order

        // Update state and build request list
        let mut matches_to_request = Vec::new();
        for (block_hash, height) in matches_found {
            if let SyncPhase::DownloadingTransactions {
                pending_blocks,
                downloading_blocks,
                last_progress,
                ..
            } = &mut self.current_phase
            {
                pending_blocks.push((block_hash, height));
                downloading_blocks.insert(block_hash, Instant::now());
                *last_progress = Instant::now();
            }

            matches_to_request.push(crate::types::FilterMatch {
                block_hash,
                height,
                block_requested: false,
            });
        }

        // Request all newly matched blocks
        if !matches_to_request.is_empty() {
            tracing::info!(
                "🔄 Rescan found {} new matches, requesting blocks",
                matches_to_request.len()
            );
            self.filter_sync
                .process_filter_matches_and_download(matches_to_request, network)
                .await?;
        } else {
            tracing::info!("🔄 Rescan found no new matches, batch complete");
            // No new matches - advance to next batch
            self.advance_to_next_batch(network, storage).await?;
        }

        Ok(())
    }

    /// Advance to the next batch after completing the current one
    pub(super) async fn advance_to_next_batch(
        &mut self,
        network: &mut N,
        storage: &mut S,
    ) -> SyncResult<()> {
        if let SyncPhase::DownloadingTransactions {
            batch_start,
            batch_end,
            tip_height,
            current_batch,
            total_batches,
            stored_filters,
            completed_filter_heights,
            pending_blocks,
            downloading_blocks,
            completed_blocks,
            total_blocks,
            new_addresses,
            scan_pass,
            last_progress,
            ..
        } = &mut self.current_phase
        {
            let old_batch_end = *batch_end;
            let tip = *tip_height;

            // Check if this was the final batch
            if old_batch_end >= tip {
                tracing::info!("All batches complete, transitioning to FullySynced");
                self.transition_to_next_phase(storage, network, "All batches complete")
                    .await?;
                return Ok(());
            }

            // Clear stored filters to free memory
            stored_filters.clear();

            // Advance batch counters
            *current_batch += 1;
            let new_start = old_batch_end + 1;
            let new_end = (new_start + TRANSACTION_SYNC_BATCH_SIZE - 1).min(tip);

            *batch_start = new_start;
            *batch_end = new_end;

            // Reset batch state
            completed_filter_heights.clear();
            pending_blocks.clear();
            downloading_blocks.clear();
            completed_blocks.clear();
            *total_blocks = 0;
            new_addresses.clear();
            *scan_pass = 0;
            *last_progress = Instant::now();

            tracing::info!(
                "Starting batch {}/{}: heights {} to {}",
                *current_batch + 1,
                *total_batches,
                new_start,
                new_end
            );

            // Reset filter sync state and execute the new batch
            self.filter_sync.reset();
            self.filter_sync.set_syncing_filters(false);
            self.execute_current_phase(network, storage).await?;
        }
        Ok(())
    }

    // Helper methods for calculating totals

    pub(super) fn calculate_total_headers_synced(&self) -> u32 {
        self.phase_history
            .iter()
            .find(|t| t.from_phase == "Downloading Headers")
            .and_then(|t| t.final_progress.as_ref())
            .map(|p| p.items_completed)
            .unwrap_or(0)
    }

    pub(super) fn calculate_total_filters_synced(&self) -> u32 {
        self.phase_history
            .iter()
            .find(|t| t.from_phase == "Downloading Transactions")
            .and_then(|t| t.final_progress.as_ref())
            .map(|p| p.items_completed)
            .unwrap_or(0)
    }

    pub(super) fn calculate_total_blocks_downloaded(&self) -> u32 {
        // Blocks are now part of the combined transactions phase
        // The items_completed includes both filters and blocks
        self.phase_history
            .iter()
            .find(|t| t.from_phase == "Downloading Transactions")
            .and_then(|t| t.final_progress.as_ref())
            .map(|_p| 0) // Individual block count not tracked separately
            .unwrap_or(0)
    }
}
