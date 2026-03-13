use crate::sync::SyncState;
use std::fmt;
use tokio::time::Instant;

/// Progress tracking for mempool transaction monitoring.
#[derive(Debug, Clone, PartialEq)]
pub struct MempoolProgress {
    /// Current sync state.
    state: SyncState,
    /// Total transactions received from the network.
    received: u32,
    /// Transactions that matched wallet addresses.
    relevant: u32,
    /// Transactions currently tracked in mempool state (wallet-relevant).
    tracked: u32,
    /// Transactions removed (confirmed or expired).
    removed: u32,
    /// Time of last activity.
    last_activity: Instant,
}

impl Default for MempoolProgress {
    fn default() -> Self {
        Self {
            state: Default::default(),
            received: 0,
            relevant: 0,
            tracked: 0,
            removed: 0,
            last_activity: Instant::now(),
        }
    }
}

impl MempoolProgress {
    pub fn state(&self) -> SyncState {
        self.state
    }

    pub fn received(&self) -> u32 {
        self.received
    }

    pub fn relevant(&self) -> u32 {
        self.relevant
    }

    pub fn tracked(&self) -> u32 {
        self.tracked
    }

    pub fn removed(&self) -> u32 {
        self.removed
    }

    pub fn last_activity(&self) -> Instant {
        self.last_activity
    }

    pub(super) fn set_state(&mut self, state: SyncState) {
        self.state = state;
        self.bump_last_activity();
    }

    pub(super) fn add_received(&mut self, count: u32) {
        self.received += count;
        self.bump_last_activity();
    }

    pub(super) fn add_relevant(&mut self, count: u32) {
        self.relevant += count;
        self.bump_last_activity();
    }

    pub(super) fn set_tracked(&mut self, count: u32) {
        self.tracked = count;
        self.bump_last_activity();
    }

    pub(super) fn add_removed(&mut self, count: u32) {
        self.removed += count;
        self.bump_last_activity();
    }

    fn bump_last_activity(&mut self) {
        self.last_activity = Instant::now();
    }
}

impl fmt::Display for MempoolProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} received: {}, relevant: {}, tracked: {}, removed: {}, last_activity: {}s",
            self.state,
            self.received,
            self.relevant,
            self.tracked,
            self.removed,
            self.last_activity.elapsed().as_secs()
        )
    }
}
