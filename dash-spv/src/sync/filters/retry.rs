//! Timeout and retry logic for filter synchronization.
//!
//! This module handles:
//! - Detecting timed-out filter and CFHeader requests
//! - Retrying failed requests with exponential backoff
//! - Managing retry counts and giving up after max attempts
//! - Sync progress timeout detection

use super::types::*;
use crate::error::{SyncError, SyncResult};
use crate::network::NetworkManager;
use crate::storage::StorageManager;
use dashcore::BlockHash;

impl<S: StorageManager + Send + Sync + 'static, N: NetworkManager + Send + Sync + 'static>
    super::manager::FilterSyncManager<S, N>
{
    /// Check for timed out CFHeader requests and retry them.
    ///
    /// Called periodically when flow control is enabled to detect and recover from
    /// requests that never received responses.
    pub async fn check_cfheader_request_timeouts(
        &mut self,
        network: &mut N,
        storage: &S,
    ) -> SyncResult<()> {
        if !self.cfheaders_flow_control_enabled || !self.syncing_filter_headers {
            return Ok(());
        }

        let now = std::time::Instant::now();
        let mut timed_out_requests = Vec::new();

        // Check for timed out active requests
        for (start_height, active_req) in &self.active_cfheader_requests {
            if now.duration_since(active_req.sent_time) > self.cfheader_request_timeout {
                timed_out_requests.push((*start_height, active_req.stop_hash));
            }
        }

        // Handle timeouts: remove from active, retry or give up based on retry count
        for (start_height, stop_hash) in timed_out_requests {
            self.handle_cfheader_request_timeout(start_height, stop_hash, network, storage).await?;
        }

        // Check queue status and send next batch if needed
        self.process_next_queued_cfheader_requests(network).await?;

        Ok(())
    }

    /// Handle a specific CFHeaders request timeout.
    async fn handle_cfheader_request_timeout(
        &mut self,
        start_height: u32,
        stop_hash: BlockHash,
        _network: &mut N,
        _storage: &S,
    ) -> SyncResult<()> {
        let retry_count = self.cfheader_retry_counts.get(&start_height).copied().unwrap_or(0);

        // Remove from active requests
        self.active_cfheader_requests.remove(&start_height);

        if retry_count >= self.max_cfheader_retries {
            tracing::error!(
                "❌ CFHeaders request for height {} failed after {} retries, giving up",
                start_height,
                retry_count
            );
            return Ok(());
        }

        tracing::info!(
            "🔄 Retrying timed out CFHeaders request for height {} (attempt {}/{})",
            start_height,
            retry_count + 1,
            self.max_cfheader_retries
        );

        // Create new request and add back to queue for retry
        let retry_request = CFHeaderRequest {
            start_height,
            stop_hash,
            is_retry: true,
        };

        // Update retry count
        self.cfheader_retry_counts.insert(start_height, retry_count + 1);

        // Add to front of queue for priority retry
        self.pending_cfheader_requests.push_front(retry_request);

        Ok(())
    }

    /// Check for timed out filter requests and retry them.
    ///
    /// When flow control is enabled, checks active requests for timeouts.
    /// When flow control is disabled, delegates to check_and_retry_missing_filters.
    pub async fn check_filter_request_timeouts(
        &mut self,
        network: &mut N,
        storage: &S,
    ) -> SyncResult<()> {
        if !self.flow_control_enabled {
            // Fall back to original timeout checking
            return self.check_and_retry_missing_filters(network, storage).await;
        }

        let now = std::time::Instant::now();
        let timeout_duration = std::time::Duration::from_secs(REQUEST_TIMEOUT_SECONDS);

        // Check for timed out active requests
        let mut timed_out_requests = Vec::new();
        for ((start, end), active_req) in &self.active_filter_requests {
            if now.duration_since(active_req.sent_time) > timeout_duration {
                timed_out_requests.push((*start, *end));
            }
        }

        // Handle timeouts: remove from active, retry or give up based on retry count
        for range in timed_out_requests {
            self.handle_request_timeout(range, network, storage).await?;
        }

        // Check queue status and send next batch if needed
        self.process_next_queued_requests(network).await?;

        Ok(())
    }

    /// Handle a specific filter request timeout.
    async fn handle_request_timeout(
        &mut self,
        range: (u32, u32),
        _network: &mut dyn NetworkManager,
        storage: &S,
    ) -> SyncResult<()> {
        let (start, end) = range;
        let retry_count = self.filter_retry_counts.get(&range).copied().unwrap_or(0);

        // Remove from active requests
        self.active_filter_requests.remove(&range);

        if retry_count >= self.max_filter_retries {
            tracing::error!(
                "❌ Filter range {}-{} failed after {} retries, giving up",
                start,
                end,
                retry_count
            );
            return Ok(());
        }

        // Calculate stop hash for retry; ensure height is within the stored window
        if self.header_abs_to_storage_index(end).is_none() {
            tracing::debug!(
                "Skipping retry for range {}-{} because end is below checkpoint base {}",
                start,
                end,
                self.sync_base_height
            );
            return Ok(());
        }

        match storage.get_header(end).await {
            Ok(Some(header)) => {
                let stop_hash = header.block_hash();

                tracing::info!(
                    "🔄 Retrying timed out filter range {}-{} (attempt {}/{})",
                    start,
                    end,
                    retry_count + 1,
                    self.max_filter_retries
                );

                // Create new request and add back to queue for retry
                let retry_request = FilterRequest {
                    start_height: start,
                    end_height: end,
                    stop_hash,
                    is_retry: true,
                };

                // Update retry count
                self.filter_retry_counts.insert(range, retry_count + 1);

                // Add to front of queue for priority retry
                self.pending_filter_requests.push_front(retry_request);

                Ok(())
            }
            Ok(None) => {
                tracing::error!(
                    "Cannot retry filter range {}-{}: header not found at height {}",
                    start,
                    end,
                    end
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to get header at height {} for retry: {}", end, e);
                Ok(())
            }
        }
    }

    /// Get filter ranges that have timed out (no response within timeout_duration).
    ///
    /// Returns list of (start_height, end_height) tuples for incomplete ranges.
    pub fn get_timed_out_ranges(&self, timeout_duration: std::time::Duration) -> Vec<(u32, u32)> {
        let now = std::time::Instant::now();
        let mut timed_out = Vec::new();

        let heights = match self.received_filter_heights.try_lock() {
            Ok(heights) => heights.clone(),
            Err(_) => return timed_out,
        };

        for ((start, end), request_time) in &self.requested_filter_ranges {
            if now.duration_since(*request_time) > timeout_duration {
                // Check if this range is incomplete
                let mut is_incomplete = false;
                for height in *start..=*end {
                    if !heights.contains(&height) {
                        is_incomplete = true;
                        break;
                    }
                }

                if is_incomplete {
                    timed_out.push((*start, *end));
                }
            }
        }

        timed_out
    }
}
