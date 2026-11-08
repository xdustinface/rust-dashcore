use tokio::sync::watch;

use super::SYNC_TIMEOUT;
use crate::sync::{ProgressPercentage, SyncProgress};

/// Wait for sync to reach target height.
pub async fn wait_for_sync(
    progress_receiver: &mut watch::Receiver<SyncProgress>,
    target_height: u32,
) {
    let timeout = tokio::time::sleep(SYNC_TIMEOUT);
    tokio::pin!(timeout);

    loop {
        // Check current state before waiting for changes — the receiver may
        // already hold a value that satisfies the condition.
        {
            let update = progress_receiver.borrow_and_update();
            let header_height = update.headers().ok().map(|h| h.current_height()).unwrap_or(0);
            let filters_height = update.filters().ok().map(|f| f.committed_height()).unwrap_or(0);
            if update.is_synced()
                && header_height >= target_height
                && filters_height >= target_height
            {
                return;
            }
        }

        tokio::select! {
            _ = &mut timeout => {
                let update = progress_receiver.borrow();
                panic!("Timeout waiting for sync to height {}. Current progress: {:?}",
                    target_height, update
                );
            }
            result = progress_receiver.changed() => {
                if result.is_err() {
                    panic!("Progress channel closed");
                }
            }
        }
    }
}
