//! Peer reputation management system
//!
//! This module implements a reputation system to track peer behavior and protect
//! against malicious peers. It tracks both positive and negative behaviors,
//! implements automatic banning for excessive misbehavior, and provides reputation
//! decay over time for recovery.

use crate::network::required_services::RequiredServices;
use crate::storage::PeerStorage;
use dashcore::network::address::AddrV2Message;
use dashcore::network::constants::ServiceFlags;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/// Misbehavior score thresholds for different violations
pub mod misbehavior_scores {
    /// Invalid message format or protocol violation
    pub const INVALID_MESSAGE: i32 = 10;

    /// Invalid block header
    pub const INVALID_HEADER: i32 = 50;

    /// Invalid compact filter
    pub const INVALID_FILTER: i32 = 25;

    /// Timeout or slow response
    pub const TIMEOUT: i32 = 5;

    /// Sending unsolicited data
    pub const UNSOLICITED_DATA: i32 = 15;

    /// Invalid transaction
    pub const INVALID_TRANSACTION: i32 = 20;

    /// Invalid masternode list diff
    pub const INVALID_MASTERNODE_DIFF: i32 = 30;

    /// Invalid ChainLock
    pub const INVALID_CHAINLOCK: i32 = 40;

    /// Invalid InstantLock
    pub const INVALID_INSTANTLOCK: i32 = 35;

    /// Duplicate message
    pub const DUPLICATE_MESSAGE: i32 = 5;

    /// Connection flood attempt
    pub const CONNECTION_FLOOD: i32 = 20;
}

/// Positive behavior scores
pub mod positive_scores {
    /// Successfully provided valid headers
    pub const VALID_HEADERS: i32 = -5;

    /// Successfully provided valid filters
    pub const VALID_FILTERS: i32 = -3;

    /// Successfully provided valid block
    pub const VALID_BLOCK: i32 = -10;

    /// Fast response time
    pub const FAST_RESPONSE: i32 = -2;

    /// Long uptime connection
    pub const LONG_UPTIME: i32 = -5;
}

/// Ban duration for misbehaving peers
const BAN_DURATION: Duration = Duration::from_secs(24 * 60 * 60); // 24 hours

/// Reputation decay interval
const DECAY_INTERVAL: Duration = Duration::from_secs(60 * 60); // 1 hour

/// Amount to decay reputation score per interval
const DECAY_AMOUNT: i32 = 5;

/// Maximum misbehavior score before a peer is banned
const MAX_MISBEHAVIOR_SCORE: i32 = 100;

/// Minimum score (most positive reputation)
const MIN_MISBEHAVIOR_SCORE: i32 = -50;

const MAX_BAN_COUNT: u32 = 1000;

const MAX_ACTION_COUNT: u64 = 1_000_000;

fn clamp_peer_score<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: Deserializer<'de>,
{
    let mut v = i32::deserialize(deserializer)?;

    if v < MIN_MISBEHAVIOR_SCORE {
        tracing::warn!("Peer has invalid score {v}, clamping to min {MIN_MISBEHAVIOR_SCORE}");
        v = MIN_MISBEHAVIOR_SCORE
    } else if v > MAX_MISBEHAVIOR_SCORE {
        tracing::warn!("Peer has invalid score {v}, clamping to max {MAX_MISBEHAVIOR_SCORE}");
        v = MAX_MISBEHAVIOR_SCORE
    }

    Ok(v)
}

fn clamp_peer_ban_count<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let mut v = u32::deserialize(deserializer)?;

    if v > MAX_BAN_COUNT {
        tracing::warn!("Peer has excessive ban count {v}, clamping to {MAX_BAN_COUNT}");
        v = MAX_BAN_COUNT
    }

    Ok(v)
}

fn clamp_peer_connection_attempts<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let mut v = u64::deserialize(deserializer)?;

    v = v.min(MAX_ACTION_COUNT);

    Ok(v)
}

const MAX_CONSECUTIVE_FAILURES: u32 = 1_000;

fn clamp_peer_consecutive_failures<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let mut v = u32::deserialize(deserializer)?;

    if v > MAX_CONSECUTIVE_FAILURES {
        tracing::warn!(
            "Peer has excessive consecutive failures {v}, clamping to {MAX_CONSECUTIVE_FAILURES}"
        );
        v = MAX_CONSECUTIVE_FAILURES
    }

    Ok(v)
}

/// Clock-drift tolerance for future timestamps: up to 10 seconds ahead is accepted.
const FUTURE_TIMESTAMP_TOLERANCE: Duration = Duration::from_secs(10);

/// Exponential backoff steps applied after repeated connection failures. Indexed
/// by `consecutive_failures - 1`, clamped to the last entry once the streak
/// exceeds the table length.
pub(crate) const COOLDOWN_STEPS: [Duration; 5] = [
    Duration::from_secs(30),
    Duration::from_secs(60),
    Duration::from_secs(5 * 60),
    Duration::from_secs(30 * 60),
    Duration::from_secs(2 * 60 * 60),
];

/// Tunables for the capability-aware peer-selection score. Lower composite score is
/// better, matching the existing reputation convention.
mod scoring_weights {
    /// Weight applied to the success ratio (successful / attempts). Subtracted from
    /// the composite score so peers with a high historical success rate rank ahead
    /// of peers at the same reputation score.
    pub(super) const SUCCESS_RATIO_WEIGHT: f64 = 30.0;

    /// Seconds of `AddrV2.time` staleness per 1 point of penalty.
    pub(super) const STALENESS_SECS_PER_POINT: f64 = 600.0;

    /// Cap on the staleness penalty so a very old address cannot dominate the score.
    pub(super) const STALENESS_CAP: f64 = 50.0;

    /// Bonus subtracted when the peer advertises `NODE_HEADERS_COMPRESSED`.
    pub(super) const PREFERRED_SERVICES_BONUS: f64 = 15.0;
}

fn clamp_future_system_time<'de, D>(d: D) -> Result<Option<SystemTime>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<SystemTime>::deserialize(d)?;
    let now = SystemTime::now();
    let deadline = now.checked_add(FUTURE_TIMESTAMP_TOLERANCE).unwrap_or(now);
    Ok(opt.filter(|t| *t <= deadline))
}

/// Peer reputation entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerReputation {
    /// Current misbehavior score
    #[serde(deserialize_with = "clamp_peer_score")]
    pub score: i32,

    /// Number of times this peer has been banned
    #[serde(deserialize_with = "clamp_peer_ban_count")]
    pub ban_count: u32,

    /// Time when the peer was banned (if currently banned)
    #[serde(skip)]
    pub banned_until: Option<Instant>,

    /// Last time the reputation was updated
    #[serde(skip, default = "Instant::now")]
    pub last_update: Instant,

    /// Total number of positive actions
    pub positive_actions: u64,

    /// Total number of negative actions
    pub negative_actions: u64,

    /// Connection count
    #[serde(deserialize_with = "clamp_peer_connection_attempts")]
    pub connection_attempts: u64,

    /// Successful connection count
    pub successful_connections: u64,

    /// Monotonic instant of the last connection attempt within the current process session.
    /// Resets to `None` on restart. Used for runtime decisions such as immediate
    /// reconnect throttling. For persistent cooldown/backoff logic use `last_tried`.
    #[serde(skip)]
    pub last_connection: Option<Instant>,

    /// Wall-clock time of the last successful handshake with this peer.
    #[serde(default, deserialize_with = "clamp_future_system_time")]
    pub last_success: Option<SystemTime>,

    /// Wall-clock time of the last attempted connection to this peer (persisted across
    /// restarts). The canonical source for cooldown and backoff decisions that must
    /// survive process restarts. Distinct from `last_connection`, which is session-only.
    #[serde(default, deserialize_with = "clamp_future_system_time")]
    pub last_tried: Option<SystemTime>,

    /// Failures since the last success. Resets to 0 on a successful handshake.
    #[serde(deserialize_with = "clamp_peer_consecutive_failures")]
    pub consecutive_failures: u32,
}

impl Default for PeerReputation {
    fn default() -> Self {
        Self {
            score: 0,
            ban_count: 0,
            banned_until: None,
            last_update: Instant::now(),
            positive_actions: 0,
            negative_actions: 0,
            connection_attempts: 0,
            successful_connections: 0,
            last_connection: None,
            last_success: None,
            last_tried: None,
            consecutive_failures: 0,
        }
    }
}

impl PeerReputation {
    /// Enforce internal consistency after loading from persistent storage. If
    /// `last_tried` was discarded (e.g., because it was a future timestamp),
    /// `consecutive_failures` has no temporal anchor and must be reset to 0 to
    /// avoid incorrect backoff behaviour.
    fn normalize_after_load(&mut self) {
        if self.last_tried.is_none() && self.consecutive_failures > 0 {
            self.consecutive_failures = 0;
        }
    }

    /// Check if the peer is currently banned
    pub fn is_banned(&self) -> bool {
        self.banned_until.is_some_and(|until| Instant::now() < until)
    }

    /// Get remaining ban time
    pub fn ban_time_remaining(&self) -> Option<Duration> {
        self.banned_until.and_then(|until| {
            let now = Instant::now();
            if now < until {
                Some(until - now)
            } else {
                None
            }
        })
    }

    /// Apply the common fields updated on every failure: refresh `last_tried` and
    /// increment `consecutive_failures`, clamped to `MAX_CONSECUTIVE_FAILURES`.
    fn record_failure_fields(&mut self) {
        self.last_tried = Some(SystemTime::now());
        self.consecutive_failures =
            self.consecutive_failures.saturating_add(1).min(MAX_CONSECUTIVE_FAILURES);
    }

    /// Apply a score change. Assumes decay has already been applied.
    /// Returns `true` if the peer was banned by this call.
    fn apply_score_change(&mut self, score_change: i32, peer: SocketAddr, reason: &str) -> bool {
        let old_score = self.score;
        self.score =
            (self.score + score_change).clamp(MIN_MISBEHAVIOR_SCORE, MAX_MISBEHAVIOR_SCORE);

        if score_change > 0 {
            self.negative_actions += 1;
        } else if score_change < 0 {
            self.positive_actions += 1;
        }

        let should_ban = self.score >= MAX_MISBEHAVIOR_SCORE && !self.is_banned();
        if should_ban {
            self.banned_until = Some(Instant::now() + BAN_DURATION);
            self.ban_count += 1;
            tracing::warn!(
                "Peer {} banned for misbehavior (score: {}, ban #{}, reason: {})",
                peer,
                self.score,
                self.ban_count,
                reason
            );
        }

        if score_change.abs() >= 10 || should_ban {
            tracing::info!(
                "Peer {} reputation changed: {} -> {} (change: {}, reason: {})",
                peer,
                old_score,
                self.score,
                score_change,
                reason
            );
        }

        should_ban
    }

    /// Return the backoff duration that should apply after `last_tried` given
    /// the current `consecutive_failures` streak. `None` means no cooldown is
    /// imposed, either because there are no failures or because there is no
    /// temporal anchor in `last_tried`.
    pub(crate) fn cooldown(&self) -> Option<Duration> {
        if self.consecutive_failures == 0 {
            return None;
        }
        let idx =
            (self.consecutive_failures as usize).saturating_sub(1).min(COOLDOWN_STEPS.len() - 1);
        Some(COOLDOWN_STEPS[idx])
    }

    /// True when `now` still falls inside the cooldown window anchored at
    /// `last_tried`. Returns false if either the streak is zero or `last_tried`
    /// is missing (e.g. discarded on load).
    pub(crate) fn in_cooldown(&self, now: SystemTime) -> bool {
        match (self.last_tried, self.cooldown()) {
            (Some(last), Some(cd)) => match last.checked_add(cd) {
                Some(deadline) => deadline > now,
                None => false,
            },
            _ => false,
        }
    }

    /// Apply reputation decay
    pub fn apply_decay(&mut self) {
        let now = Instant::now();
        let elapsed = now - self.last_update;

        // Apply decay for each interval that has passed
        let intervals = elapsed.as_secs() / DECAY_INTERVAL.as_secs();
        if intervals > 0 {
            // Use saturating conversion to prevent overflow
            // Cap at a reasonable maximum to avoid excessive decay
            let intervals_i32 = intervals.min(i32::MAX as u64) as i32;
            let decay = intervals_i32.saturating_mul(DECAY_AMOUNT);
            self.score = (self.score - decay).max(MIN_MISBEHAVIOR_SCORE);
            self.last_update = now;
        }

        // Check if ban has expired
        if self.is_banned() && self.ban_time_remaining().is_none() {
            self.banned_until = None;
        }
    }
}

/// Reputation change event
#[derive(Debug, Clone)]
pub struct ReputationEvent {
    pub peer: SocketAddr,
    pub change: i32,
    pub reason: String,
    pub timestamp: Instant,
}

/// Peer reputation manager
pub struct PeerReputationManager {
    /// Reputation data for each peer
    reputations: Arc<RwLock<HashMap<SocketAddr, PeerReputation>>>,

    /// Recent reputation events for monitoring
    recent_events: Arc<RwLock<Vec<ReputationEvent>>>,

    /// Maximum number of events to keep
    max_events: usize,
}

impl Default for PeerReputationManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerReputationManager {
    /// Create a new reputation manager
    pub fn new() -> Self {
        Self {
            reputations: Arc::new(RwLock::new(HashMap::new())),
            recent_events: Arc::new(RwLock::new(Vec::new())),
            max_events: 1000,
        }
    }

    /// Update peer reputation
    pub async fn update_reputation(
        &self,
        peer: SocketAddr,
        score_change: i32,
        reason: &str,
    ) -> bool {
        let mut reputations = self.reputations.write().await;
        let reputation = reputations.entry(peer).or_default();

        reputation.apply_decay();
        let should_ban = reputation.apply_score_change(score_change, peer, reason);

        let event = ReputationEvent {
            peer,
            change: score_change,
            reason: reason.to_string(),
            timestamp: Instant::now(),
        };

        drop(reputations); // Release lock before recording event
        self.record_event(event).await;

        should_ban
    }

    /// Record a reputation event
    async fn record_event(&self, event: ReputationEvent) {
        let mut events = self.recent_events.write().await;
        events.push(event);

        // Keep only recent events
        if events.len() > self.max_events {
            let drain_count = events.len() - self.max_events;
            events.drain(0..drain_count);
        }
    }

    /// Check if a peer is banned
    pub async fn is_banned(&self, peer: &SocketAddr) -> bool {
        let mut reputations = self.reputations.write().await;
        if let Some(reputation) = reputations.get_mut(peer) {
            reputation.apply_decay();
            reputation.is_banned()
        } else {
            false
        }
    }

    /// Get peer reputation score
    pub async fn get_score(&self, peer: &SocketAddr) -> i32 {
        let mut reputations = self.reputations.write().await;
        if let Some(reputation) = reputations.get_mut(peer) {
            reputation.apply_decay();
            reputation.score
        } else {
            0
        }
    }

    /// Temporarily ban a peer for a specified duration, regardless of score.
    /// This can be used for critical protocol violations (e.g., invalid ChainLocks).
    pub async fn temporary_ban_peer(&self, peer: SocketAddr, duration: Duration, reason: &str) {
        let mut reputations = self.reputations.write().await;
        let reputation = reputations.entry(peer).or_default();

        reputation.banned_until = Some(Instant::now() + duration);
        reputation.ban_count += 1;

        tracing::warn!(
            "Peer {} temporarily banned for {:?} (ban #{}, reason: {})",
            peer,
            duration,
            reputation.ban_count,
            reason
        );
    }

    /// Record a connection attempt
    pub async fn record_connection_attempt(&self, peer: SocketAddr) {
        let mut reputations = self.reputations.write().await;
        let reputation = reputations.entry(peer).or_default();
        reputation.connection_attempts += 1;
        reputation.last_connection = Some(Instant::now());
        reputation.last_tried = Some(SystemTime::now());
    }

    /// Record a successful connection
    pub async fn record_successful_connection(&self, peer: SocketAddr) {
        let mut reputations = self.reputations.write().await;
        let reputation = reputations.entry(peer).or_default();
        reputation.successful_connections += 1;
        reputation.last_success = Some(SystemTime::now());
        reputation.consecutive_failures = 0;
    }

    /// Record a connection failure and apply a reputation penalty in a single write-lock
    /// acquisition. Returns `true` if the peer was banned by this call.
    ///
    /// `score_change` must be non-negative. A value of `0` records the failure (increments
    /// `consecutive_failures`, updates `last_tried`) without applying a reputation penalty,
    /// which is useful for tracking failures whose root cause doesn't warrant a ban contribution.
    /// Any negative `score_change` is clamped to 0 (panics in debug).
    pub async fn record_failure_with_penalty(
        &self,
        peer: SocketAddr,
        score_change: i32,
        reason: &str,
    ) -> bool {
        debug_assert!(
            score_change >= 0,
            "record_failure_with_penalty expects non-negative score change"
        );
        let score_change = score_change.max(0);
        let should_ban = {
            let mut reputations = self.reputations.write().await;
            let reputation = reputations.entry(peer).or_default();

            reputation.record_failure_fields();

            reputation.apply_decay();
            reputation.apply_score_change(score_change, peer, reason)
        };

        let event = ReputationEvent {
            peer,
            change: score_change,
            reason: reason.to_string(),
            timestamp: Instant::now(),
        };
        self.record_event(event).await;

        should_ban
    }

    /// Get all peer reputations
    pub async fn get_all_reputations(&self) -> HashMap<SocketAddr, PeerReputation> {
        let mut reputations = self.reputations.write().await;

        // Apply decay to all peers
        for reputation in reputations.values_mut() {
            reputation.apply_decay();
        }

        reputations.clone()
    }

    /// Get recent reputation events
    pub async fn get_recent_events(&self) -> Vec<ReputationEvent> {
        self.recent_events.read().await.clone()
    }

    /// Clear banned status for a peer (admin function)
    pub async fn unban_peer(&self, peer: &SocketAddr) {
        let mut reputations = self.reputations.write().await;
        if let Some(reputation) = reputations.get_mut(peer) {
            reputation.banned_until = None;
            reputation.score = reputation.score.min(MAX_MISBEHAVIOR_SCORE - 10);
            tracing::info!("Manually unbanned peer {}", peer);
        }
    }

    /// Reset reputation for a peer
    pub async fn reset_reputation(&self, peer: &SocketAddr) {
        let mut reputations = self.reputations.write().await;
        reputations.remove(peer);
        tracing::info!("Reset reputation for peer {}", peer);
    }

    /// Get peers sorted by reputation (best first)
    pub async fn get_peers_by_reputation(&self) -> Vec<(SocketAddr, i32)> {
        let mut reputations = self.reputations.write().await;

        // Apply decay and collect scores
        let mut peer_scores: Vec<(SocketAddr, i32)> = reputations
            .iter_mut()
            .map(|(addr, rep)| {
                rep.apply_decay();
                (*addr, rep.score)
            })
            .filter(|(_, score)| *score < MAX_MISBEHAVIOR_SCORE) // Exclude banned peers
            .collect();

        // Sort by score (lower is better)
        peer_scores.sort_by_key(|(_, score)| *score);

        peer_scores
    }

    /// Save reputation data to persistent storage
    pub async fn save_to_storage(&self, storage: &impl PeerStorage) -> std::io::Result<()> {
        let reputations = self.reputations.read().await;

        storage.save_peers_reputation(&reputations).await.map_err(std::io::Error::other)
    }

    /// Load reputation data from persistent storage
    pub async fn load_from_storage(&self, storage: &impl PeerStorage) -> std::io::Result<()> {
        let data = storage.load_peers_reputation().await.map_err(std::io::Error::other)?;

        let mut reputations = self.reputations.write().await;
        let mut loaded_count = 0;
        let mut skipped_count = 0;

        for (addr, mut reputation) in data {
            // Validate successful connections don't exceed attempts
            reputation.successful_connections =
                reputation.successful_connections.min(reputation.connection_attempts);

            reputation.normalize_after_load();

            // Skip entry if data appears corrupted
            if reputation.positive_actions > MAX_ACTION_COUNT
                || reputation.negative_actions > MAX_ACTION_COUNT
            {
                tracing::warn!("Skipping peer {} with potentially corrupted action counts", addr);
                skipped_count += 1;
                continue;
            }

            // Apply initial decay based on ban count
            if reputation.ban_count > 0 {
                reputation.score = reputation.score.max(50); // Start with higher score for previously banned peers
            }

            reputations.insert(addr, reputation);
            loaded_count += 1;
        }

        tracing::info!(
            "Loaded reputation data for {} peers (skipped {} corrupted entries)",
            loaded_count,
            skipped_count
        );
        Ok(())
    }
}

/// Combine reputation score, historical success ratio, gossip staleness and the
/// preferred-services bonus into a single score. Lower is better.
fn composite_score(reputation: &PeerReputation, addr: &AddrV2Message, now_epoch_secs: u64) -> f64 {
    let mut score = reputation.score as f64;

    let attempts = reputation.connection_attempts.max(1) as f64;
    let success_ratio = reputation.successful_connections as f64 / attempts;
    score -= success_ratio * scoring_weights::SUCCESS_RATIO_WEIGHT;

    let staleness_secs = now_epoch_secs.saturating_sub(addr.time as u64) as f64;
    let staleness_penalty = (staleness_secs / scoring_weights::STALENESS_SECS_PER_POINT)
        .min(scoring_weights::STALENESS_CAP);
    score += staleness_penalty;

    if addr.services.has(ServiceFlags::NODE_HEADERS_COMPRESSED) {
        score -= scoring_weights::PREFERRED_SERVICES_BONUS;
    }

    score
}

/// Helper trait for reputation-aware peer selection
pub(crate) trait ReputationAware {
    /// Select best peers that satisfy `required` services and are not banned.
    /// An empty return value indicates no capable survivors, allowing callers
    /// to fall back to DNS discovery rather than connecting to an incapable peer.
    fn select_best_peers(
        &self,
        required: RequiredServices,
        available_peers: Vec<AddrV2Message>,
        count: usize,
    ) -> impl std::future::Future<Output = Vec<SocketAddr>> + Send;

    /// Check if we should connect to a peer based on reputation
    fn should_connect_to_peer(
        &self,
        peer: &SocketAddr,
    ) -> impl std::future::Future<Output = bool> + Send;
}

impl ReputationAware for PeerReputationManager {
    async fn select_best_peers(
        &self,
        required: RequiredServices,
        available_peers: Vec<AddrV2Message>,
        count: usize,
    ) -> Vec<SocketAddr> {
        if count == 0 {
            return Vec::new();
        }

        let now_system = SystemTime::now();
        let now_epoch_secs =
            now_system.duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);

        let mut reputations = self.reputations.write().await;
        let mut scored: Vec<(SocketAddr, f64)> = Vec::with_capacity(available_peers.len());

        for peer in available_peers {
            let Ok(socket_addr) = peer.socket_addr() else {
                tracing::warn!("Skip invalid peer address: {:?}", peer);
                continue;
            };

            if !required.is_satisfied_by(peer.services) {
                continue;
            }

            let reputation = reputations.entry(socket_addr).or_default();
            reputation.apply_decay();

            if reputation.is_banned() {
                continue;
            }

            if reputation.in_cooldown(now_system) {
                continue;
            }

            scored.push((socket_addr, composite_score(reputation, &peer, now_epoch_secs)));
        }

        scored.sort_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        scored.into_iter().take(count).map(|(peer, _)| peer).collect()
    }

    async fn should_connect_to_peer(&self, peer: &SocketAddr) -> bool {
        !self.is_banned(peer).await
    }
}

// Include tests module
#[cfg(test)]
#[path = "reputation_tests.rs"]
mod reputation_tests;
