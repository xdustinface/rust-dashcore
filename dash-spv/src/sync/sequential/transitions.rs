//! Phase transition logic for sequential sync

use crate::client::ClientConfig;
use crate::error::{SyncError, SyncResult};
use crate::network::NetworkManager;
use crate::storage::StorageManager;
use crate::sync::filters::types::TRANSACTION_SYNC_BATCH_SIZE;
use dashcore::network::constants::ServiceFlags;

use super::phases::{PhaseTransition, SyncPhase};
use std::time::Instant;

/// Manages phase transitions and validation
pub struct TransitionManager {
    config: ClientConfig,
}

impl TransitionManager {
    /// Create a new transition manager
    pub fn new(config: &ClientConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    /// Check if we can transition from current phase to target phase
    pub async fn can_transition_to(
        &self,
        current_phase: &SyncPhase,
        target_phase: &SyncPhase,
        storage: &dyn StorageManager,
    ) -> SyncResult<bool> {
        // Can't transition to the same phase
        if std::mem::discriminant(current_phase) == std::mem::discriminant(target_phase) {
            return Ok(false);
        }

        // Check specific transition rules
        match (current_phase, target_phase) {
            // From Idle, can only go to DownloadingHeaders
            (
                SyncPhase::Idle,
                SyncPhase::DownloadingHeaders {
                    ..
                },
            ) => Ok(true),

            // From DownloadingHeaders, check completion
            (
                SyncPhase::DownloadingHeaders {
                    ..
                },
                next_phase,
            ) => {
                // Headers must be complete
                if !self.are_headers_complete(current_phase, storage).await? {
                    return Ok(false);
                }

                // Can go to MnList if enabled, or skip to CFHeaders
                match next_phase {
                    SyncPhase::DownloadingMnList {
                        ..
                    } => Ok(self.config.enable_masternodes),
                    SyncPhase::DownloadingCFHeaders {
                        ..
                    } => Ok(!self.config.enable_masternodes && self.config.enable_filters),
                    SyncPhase::FullySynced {
                        ..
                    } => Ok(!self.config.enable_masternodes && !self.config.enable_filters),
                    _ => Ok(false),
                }
            }

            // From DownloadingMnList
            (
                SyncPhase::DownloadingMnList {
                    ..
                },
                next_phase,
            ) => {
                // MnList must be complete
                if !self.are_masternodes_complete(current_phase, storage).await? {
                    return Ok(false);
                }

                match next_phase {
                    SyncPhase::DownloadingCFHeaders {
                        ..
                    } => Ok(self.config.enable_filters),
                    SyncPhase::FullySynced {
                        ..
                    } => Ok(!self.config.enable_filters),
                    _ => Ok(false),
                }
            }

            // From DownloadingCFHeaders
            (
                SyncPhase::DownloadingCFHeaders {
                    ..
                },
                next_phase,
            ) => {
                match next_phase {
                    SyncPhase::DownloadingTransactions {
                        ..
                    } => {
                        // Normal case: download transactions after filter_headers
                        // CFHeaders must be complete
                        Ok(self.are_filter_headers_complete(current_phase, storage).await?)
                    }
                    SyncPhase::FullySynced {
                        ..
                    } => {
                        // Allow skipping to FullySynced if no peers support filters
                        Ok(true)
                    }
                    _ => Ok(false),
                }
            }

            // From DownloadingTransactions
            (
                SyncPhase::DownloadingTransactions {
                    ..
                },
                SyncPhase::FullySynced {
                    ..
                },
            ) => {
                // All filters and blocks must be complete
                Ok(self.are_transactions_complete(current_phase))
            }

            // All other transitions are invalid
            _ => Ok(false),
        }
    }

    /// Get the next phase based on current phase and configuration
    pub async fn get_next_phase(
        &self,
        current_phase: &SyncPhase,
        storage: &dyn StorageManager,
        network: &dyn NetworkManager,
    ) -> SyncResult<Option<SyncPhase>> {
        match current_phase {
            SyncPhase::Idle => {
                // Always start with headers
                let start_height = storage
                    .get_tip_height()
                    .await
                    .map_err(|e| SyncError::Storage(format!("Failed to get tip height: {}", e)))?
                    .unwrap_or(0);

                Ok(Some(SyncPhase::DownloadingHeaders {
                    start_time: Instant::now(),
                    start_height,
                    current_height: start_height,
                    target_height: None,
                    last_progress: Instant::now(),
                    headers_downloaded: 0,
                    headers_per_second: 0.0,
                    received_empty_response: false,
                }))
            }

            SyncPhase::DownloadingHeaders {
                ..
            } => {
                if self.config.enable_masternodes {
                    let header_tip = storage
                        .get_tip_height()
                        .await
                        .map_err(|e| {
                            SyncError::Storage(format!("Failed to get header tip: {}", e))
                        })?
                        .unwrap_or(0);

                    let mn_height = match storage.load_masternode_state().await {
                        Ok(Some(state)) => state.last_height,
                        _ => 0,
                    };

                    Ok(Some(SyncPhase::DownloadingMnList {
                        start_time: Instant::now(),
                        start_height: mn_height,
                        current_height: mn_height,
                        target_height: header_tip,
                        last_progress: Instant::now(),
                        diffs_processed: 0,
                        requests_total: 0,
                        requests_completed: 0,
                    }))
                } else if self.config.enable_filters {
                    self.create_filter_headers_phase(storage).await
                } else {
                    self.create_fully_synced_phase(storage).await
                }
            }

            SyncPhase::DownloadingMnList {
                ..
            } => {
                if self.config.enable_filters {
                    self.create_filter_headers_phase(storage).await
                } else {
                    self.create_fully_synced_phase(storage).await
                }
            }

            SyncPhase::DownloadingCFHeaders {
                ..
            } => {
                // Check if any peer supports compact filters
                if !network.has_peer_with_service(ServiceFlags::COMPACT_FILTERS).await {
                    tracing::info!(
                        "No peers support compact filters, skipping transaction download phase"
                    );
                    // Skip directly to fully synced since we can't download filters
                    self.create_fully_synced_phase(storage).await
                } else {
                    // After CFHeaders, start the combined transactions phase with batching
                    self.create_transactions_phase(storage).await
                }
            }

            SyncPhase::DownloadingTransactions {
                ..
            } => self.create_fully_synced_phase(storage).await,

            SyncPhase::FullySynced {
                ..
            } => Ok(None), // Already synced
        }
    }

    /// Create a phase transition record
    pub fn create_transition(
        &self,
        from_phase: &SyncPhase,
        to_phase: &SyncPhase,
        reason: String,
    ) -> PhaseTransition {
        PhaseTransition {
            from_phase: from_phase.name().to_string(),
            to_phase: to_phase.name().to_string(),
            timestamp: Instant::now(),
            reason,
            final_progress: if from_phase.is_syncing() {
                Some(from_phase.progress())
            } else {
                None
            },
        }
    }

    // Helper methods for checking phase completion

    async fn are_headers_complete(
        &self,
        phase: &SyncPhase,
        _storage: &dyn StorageManager,
    ) -> SyncResult<bool> {
        if let SyncPhase::DownloadingHeaders {
            received_empty_response,
            ..
        } = phase
        {
            // Headers are complete when we receive an empty response
            Ok(*received_empty_response)
        } else {
            Ok(false)
        }
    }

    async fn are_masternodes_complete(
        &self,
        phase: &SyncPhase,
        storage: &dyn StorageManager,
    ) -> SyncResult<bool> {
        if let SyncPhase::DownloadingMnList {
            current_height,
            target_height,
            ..
        } = phase
        {
            // Check if we've reached the target
            if current_height >= target_height {
                return Ok(true);
            }

            // Also check storage to be sure
            if let Ok(Some(state)) = storage.load_masternode_state().await {
                Ok(state.last_height >= *target_height)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    async fn are_filter_headers_complete(
        &self,
        phase: &SyncPhase,
        _storage: &dyn StorageManager,
    ) -> SyncResult<bool> {
        if let SyncPhase::DownloadingCFHeaders {
            current_height,
            target_height,
            ..
        } = phase
        {
            Ok(current_height >= target_height)
        } else {
            Ok(false)
        }
    }

    fn are_transactions_complete(&self, phase: &SyncPhase) -> bool {
        if let SyncPhase::DownloadingTransactions {
            completed_filter_heights,
            total_filters,
            pending_blocks,
            downloading_blocks,
            batch_end,
            tip_height,
            new_addresses,
            ..
        } = phase
        {
            // All filters in current batch downloaded
            let filters_complete = completed_filter_heights.len() as u32 >= *total_filters;
            // All blocks downloaded
            let blocks_complete = pending_blocks.is_empty() && downloading_blocks.is_empty();
            // No re-scan needed (no new addresses to check)
            let no_rescan = new_addresses.is_empty();
            // This is the final batch
            let is_final_batch = *batch_end >= *tip_height;

            filters_complete && blocks_complete && no_rescan && is_final_batch
        } else {
            false
        }
    }

    async fn create_filter_headers_phase(
        &self,
        storage: &dyn StorageManager,
    ) -> SyncResult<Option<SyncPhase>> {
        let header_tip = storage
            .get_tip_height()
            .await
            .map_err(|e| SyncError::Storage(format!("Failed to get header tip: {}", e)))?
            .unwrap_or(0);

        let filter_tip = storage
            .get_filter_tip_height()
            .await
            .map_err(|e| SyncError::Storage(format!("Failed to get filter tip: {}", e)))?
            .unwrap_or(0);

        Ok(Some(SyncPhase::DownloadingCFHeaders {
            start_time: Instant::now(),
            start_height: filter_tip,
            current_height: filter_tip,
            target_height: header_tip,
            last_progress: Instant::now(),
            filter_headers_downloaded: 0,
            filter_headers_per_second: 0.0,
        }))
    }

    async fn create_transactions_phase(
        &self,
        storage: &dyn StorageManager,
    ) -> SyncResult<Option<SyncPhase>> {
        // Get the filter header tip which defines our sync range
        let tip_height = storage
            .get_filter_tip_height()
            .await
            .map_err(|e| SyncError::Storage(format!("Failed to get filter tip: {}", e)))?
            .unwrap_or(0);

        if tip_height == 0 {
            // No filters to download, skip to fully synced
            return self.create_fully_synced_phase(storage).await;
        }

        // Calculate batch parameters
        // Start from height 1 (skip genesis block which has no transactions)
        let batch_start = 1u32;
        let batch_end = batch_start
            .saturating_add(TRANSACTION_SYNC_BATCH_SIZE)
            .saturating_sub(1)
            .min(tip_height);
        let total_batches = ((tip_height - batch_start + 1) as f64
            / TRANSACTION_SYNC_BATCH_SIZE as f64)
            .ceil() as u32;
        let total_filters = batch_end - batch_start + 1;

        tracing::info!(
            "Starting batched transaction sync: {} batches, first batch heights {} to {} ({} filters)",
            total_batches,
            batch_start,
            batch_end,
            total_filters
        );

        Ok(Some(SyncPhase::DownloadingTransactions {
            start_time: Instant::now(),
            batch_start,
            batch_end,
            tip_height,
            current_batch: 0,
            total_batches,
            stored_filters: Vec::new(),
            completed_filter_heights: std::collections::HashSet::new(),
            total_filters,
            pending_blocks: Vec::new(),
            downloading_blocks: std::collections::HashMap::new(),
            completed_blocks: Vec::new(),
            total_blocks: 0,
            last_progress: Instant::now(),
            batches_processed: 0,
            new_addresses: Vec::new(),
            scan_pass: 0,
        }))
    }

    async fn create_fully_synced_phase(
        &self,
        _storage: &dyn StorageManager,
    ) -> SyncResult<Option<SyncPhase>> {
        Ok(Some(SyncPhase::FullySynced {
            sync_completed_at: Instant::now(),
            total_sync_time: Duration::from_secs(0), // Will be calculated from phase history
            headers_synced: 0,                       // Will be calculated from phase history
            filters_synced: 0,                       // Will be calculated from phase history
            blocks_downloaded: 0,                    // Will be calculated from phase history
        }))
    }
}

use std::time::Duration;
