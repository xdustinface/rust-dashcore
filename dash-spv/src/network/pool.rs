//! Peer pool for managing multiple peer connections

use crate::error::{NetworkError, SpvError as Error};
use crate::network::constants::TARGET_PEERS;
use crate::network::peer::Peer;
use dashcore::network::constants::ServiceFlags;
use dashcore::prelude::CoreBlockHeight;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Pool for managing multiple peer instances
pub struct PeerPool {
    /// Active peers mapped by address
    peers: Arc<RwLock<HashMap<SocketAddr, Arc<RwLock<Peer>>>>>,
    /// Addresses currently being connected to
    connecting: Arc<RwLock<HashSet<SocketAddr>>>,
}

impl PeerPool {
    /// Create a new peer pool
    pub fn new() -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            connecting: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Mark an address as being connected to
    pub async fn mark_connecting(&self, addr: SocketAddr) -> bool {
        let mut connecting = self.connecting.write().await;
        connecting.insert(addr)
    }

    /// Add a peer to the pool
    pub async fn add_peer(&self, addr: SocketAddr, peer: Peer) -> Result<(), Error> {
        let mut peers = self.peers.write().await;
        let mut connecting = self.connecting.write().await;

        // Remove from connecting set
        connecting.remove(&addr);

        // Check if we're at capacity
        if peers.len() >= TARGET_PEERS {
            return Err(Error::Network(NetworkError::ConnectionFailed(format!(
                "Maximum peers ({}) reached",
                TARGET_PEERS
            ))));
        }

        // Check if already connected
        if peers.contains_key(&addr) {
            return Err(Error::Network(NetworkError::ConnectionFailed(format!(
                "Already connected to {}",
                addr
            ))));
        }

        peers.insert(addr, Arc::new(RwLock::new(peer)));
        log::info!("Added peer {}, total peers: {}", addr, peers.len());
        Ok(())
    }

    /// Remove a peer from the pool and clear connecting state
    pub async fn remove_peer(&self, addr: &SocketAddr) -> Option<Arc<RwLock<Peer>>> {
        self.connecting.write().await.remove(addr);
        let removed = self.peers.write().await.remove(addr);
        if removed.is_some() {
            log::info!("Removed peer {}", addr);
        }
        removed
    }

    /// Get all active peers
    pub async fn get_all_peers(&self) -> Vec<(SocketAddr, Arc<RwLock<Peer>>)> {
        self.peers.read().await.iter().map(|(addr, peer)| (*addr, peer.clone())).collect()
    }

    /// Get a specific peer
    pub async fn get_peer(&self, addr: &SocketAddr) -> Option<Arc<RwLock<Peer>>> {
        self.peers.read().await.get(addr).cloned()
    }

    /// Get the number of active peers
    pub async fn peer_count(&self) -> usize {
        self.peers.read().await.len()
    }

    /// Check if connected to a specific peer
    pub async fn is_connected(&self, addr: &SocketAddr) -> bool {
        self.peers.read().await.contains_key(addr)
    }

    /// Check if currently connecting to a peer
    pub async fn is_connecting(&self, addr: &SocketAddr) -> bool {
        self.connecting.read().await.contains(addr)
    }

    /// Get all connected peer addresses
    pub async fn get_connected_addresses(&self) -> Vec<SocketAddr> {
        self.peers.read().await.keys().copied().collect()
    }

    pub async fn get_best_height(&self) -> Option<CoreBlockHeight> {
        let peers = self.get_all_peers().await;

        if peers.is_empty() {
            log::debug!("get_best_height: No peers available");
            return None;
        }

        let mut best_height = 0u32;
        let mut peer_count = 0;

        for (addr, peer) in peers.iter() {
            let peer_guard = peer.read().await;
            peer_count += 1;

            log::debug!(
                "get_best_height: Peer {} - best_height: {:?}, version: {:?}, connected: {}",
                addr,
                peer_guard.best_height(),
                peer_guard.version(),
                peer_guard.is_connected(),
            );

            if let Some(peer_height) = peer_guard.best_height() {
                if peer_height > 0 {
                    best_height = best_height.max(peer_height);
                    log::debug!(
                        "get_best_height: Updated best_height to {} from peer {}",
                        best_height,
                        addr
                    );
                }
            }
        }

        log::debug!("get_best_height: Checked {} peers, best_height: {}", peer_count, best_height);

        if best_height > 0 {
            Some(best_height)
        } else {
            None
        }
    }

    /// Find the first connected peer that advertises the given service flags.
    pub(crate) async fn peer_with_service(
        &self,
        flags: ServiceFlags,
    ) -> Option<(SocketAddr, Arc<RwLock<Peer>>)> {
        let peers = self.peers.read().await;
        for (addr, peer) in peers.iter() {
            if peer.read().await.has_service(flags) {
                return Some((*addr, Arc::clone(peer)));
            }
        }
        None
    }

    /// Collect all connected peers that advertise the given service flags.
    pub(crate) async fn peers_with_service(
        &self,
        flags: ServiceFlags,
    ) -> Vec<(SocketAddr, Arc<RwLock<Peer>>)> {
        let peers = self.peers.read().await;
        let mut result = Vec::new();
        for (addr, peer) in peers.iter() {
            if peer.read().await.has_service(flags) {
                result.push((*addr, peer.clone()));
            }
        }
        result
    }

    /// Check if we need more peers
    pub async fn needs_more_peers(&self) -> bool {
        self.peer_count().await < TARGET_PEERS
    }

    /// Check if we can accept more peers
    pub async fn can_accept_peers(&self) -> bool {
        self.peer_count().await < TARGET_PEERS
    }

    /// Remove unhealthy peers and return their addresses so the caller can
    /// emit the appropriate network events.
    pub async fn remove_unhealthy(&self) -> Vec<SocketAddr> {
        let peers = self.peers.read().await;
        let mut unhealthy = Vec::new();

        // Check each peer's health
        for (addr, peer) in peers.iter() {
            // Use blocking read to properly check health
            let peer_guard = peer.read().await;
            if !peer_guard.is_healthy() {
                unhealthy.push(*addr);
            }
        }

        // Release read lock before taking write lock
        drop(peers);

        // Remove unhealthy connections
        if !unhealthy.is_empty() {
            let mut peers = self.peers.write().await;
            unhealthy.retain(|addr| peers.remove(addr).is_some());
        }

        unhealthy
    }
}

impl Default for PeerPool {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl PeerPool {
    async fn insert_peer_with_services(&self, addr: SocketAddr, flags: ServiceFlags) {
        let mut peer = Peer::dummy(addr);
        peer.set_services(flags);
        self.peers.write().await.insert(addr, Arc::new(RwLock::new(peer)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_peer_pool_basic() {
        let pool = PeerPool::new();

        // Initial state
        assert_eq!(pool.peer_count().await, 0);
        assert!(pool.needs_more_peers().await);
        assert!(pool.can_accept_peers().await);

        // Test marking as connecting
        let addr = "127.0.0.1:9999".parse().expect("Failed to parse test address");
        assert!(pool.mark_connecting(addr).await);
        assert!(!pool.mark_connecting(addr).await); // Already marked
        assert!(pool.is_connecting(&addr).await);
    }

    #[tokio::test]
    async fn test_peer_with_service() {
        let pool = PeerPool::new();
        let flags = ServiceFlags::COMPACT_FILTERS;

        // No match on empty pool
        assert!(pool.peer_with_service(flags).await.is_none());

        // No match when peers lack the requested flag
        let addr1: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        pool.insert_peer_with_services(addr1, ServiceFlags::NETWORK).await;
        assert!(pool.peer_with_service(flags).await.is_none());

        // Returns the matching peer with correct address and services
        let addr2: SocketAddr = "127.0.0.1:1002".parse().unwrap();
        pool.insert_peer_with_services(addr2, ServiceFlags::NETWORK | flags).await;
        let (found_addr, found_peer) = pool.peer_with_service(flags).await.unwrap();
        assert_eq!(found_addr, addr2);
        assert!(found_peer.read().await.has_service(flags));
    }

    #[tokio::test]
    async fn test_peers_with_service() {
        let pool = PeerPool::new();
        let flags = ServiceFlags::COMPACT_FILTERS;

        // Empty on empty pool
        assert!(pool.peers_with_service(flags).await.is_empty());

        // Empty when no peer has the flag
        let addr1: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        pool.insert_peer_with_services(addr1, ServiceFlags::NETWORK).await;
        assert!(pool.peers_with_service(flags).await.is_empty());

        // Returns all matching peers, skips non-matching
        let addr2: SocketAddr = "127.0.0.1:1002".parse().unwrap();
        let addr3: SocketAddr = "127.0.0.1:1003".parse().unwrap();
        pool.insert_peer_with_services(addr2, flags).await;
        pool.insert_peer_with_services(addr3, ServiceFlags::NETWORK | flags).await;

        let results: HashMap<SocketAddr, _> =
            pool.peers_with_service(flags).await.into_iter().collect();
        assert_eq!(results.len(), 2);
        assert!(results[&addr2].read().await.has_service(flags));
        assert!(results[&addr3].read().await.has_service(flags));
    }

    #[tokio::test]
    async fn test_service_lookup_with_combined_flags() {
        let pool = PeerPool::new();
        let combined = ServiceFlags::COMPACT_FILTERS | ServiceFlags::NODE_HEADERS_COMPRESSED;

        // Peer with only one of the two flags does not match combined query
        let addr1: SocketAddr = "127.0.0.1:1001".parse().unwrap();
        pool.insert_peer_with_services(addr1, ServiceFlags::COMPACT_FILTERS).await;
        assert!(pool.peer_with_service(combined).await.is_none());
        assert!(pool.peers_with_service(combined).await.is_empty());

        // Peer with both flags matches
        let addr2: SocketAddr = "127.0.0.1:1002".parse().unwrap();
        pool.insert_peer_with_services(addr2, combined | ServiceFlags::NETWORK).await;
        let (found, _) = pool.peer_with_service(combined).await.unwrap();
        assert_eq!(found, addr2);

        let all = pool.peers_with_service(combined).await;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, addr2);

        // Single-flag queries still match both peers appropriately
        assert!(pool.peer_with_service(ServiceFlags::COMPACT_FILTERS).await.is_some());
        let cf_peers = pool.peers_with_service(ServiceFlags::COMPACT_FILTERS).await;
        assert_eq!(cf_peers.len(), 2);
    }
}
