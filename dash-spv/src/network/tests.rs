//! Unit tests for network module

#[cfg(test)]
mod peer_tests {
    use crate::network::peer::Peer;
    use dashcore::Network;
    use std::time::Duration;

    #[test]
    fn test_peer_creation() {
        let addr = "127.0.0.1:9999".parse().unwrap();
        let timeout = Duration::from_secs(30);
        let peer = Peer::new(addr, timeout, Network::Mainnet);

        assert!(!peer.is_connected());
        assert_eq!(peer.address(), addr);
    }
}

#[cfg(test)]
mod pool_tests {
    use crate::network::manager::PeerNetworkManager;
    use crate::network::peer::Peer;
    use crate::network::pool::PeerPool;
    use crate::network::reputation::misbehavior_scores;
    use crate::test_utils::test_socket_address;
    use dashcore::network::constants::ServiceFlags;
    use dashcore::Network;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_pool_limits() {
        let pool = PeerPool::new();

        // Test needs_more_peers logic
        assert!(pool.needs_more_peers().await);

        // Can accept up to TARGET_PEERS
        assert!(pool.can_accept_peers().await);

        // Test peer count
        assert_eq!(pool.peer_count().await, 0);
    }

    #[tokio::test]
    async fn test_capability_policy_for_handshake_and_eviction() {
        let cf = ServiceFlags::COMPACT_FILTERS;
        let mut incapable =
            Peer::new(test_socket_address(9), Duration::from_secs(10), Network::Testnet);
        incapable.set_services(ServiceFlags::NETWORK);

        // Handshake admission: keep fallback when no capable peer exists yet.
        let manager = PeerNetworkManager::new_for_test(cf).await;
        assert!(!manager.test_has_capable_peer().await);
        assert!(!manager.test_should_reject_after_handshake(&incapable).await);

        // Handshake admission: reject incapable peers once a capable peer exists.
        let manager = PeerNetworkManager::new_for_test(cf).await;
        manager.insert_test_peer(test_socket_address(1), cf).await;
        assert!(manager.test_has_capable_peer().await);
        assert!(manager.test_should_reject_after_handshake(&incapable).await);

        // Healthy pool: all peers match, nothing evicted
        let manager = PeerNetworkManager::new_for_test(cf).await;
        manager.insert_test_peer(test_socket_address(1), cf).await;
        manager.insert_test_peer(test_socket_address(2), cf).await;
        manager.insert_test_peer(test_socket_address(3), cf).await;
        manager.evict_mismatched_peers().await;
        assert_eq!(manager.test_peer_count().await, 3);

        // Lone mismatched peer is preserved (never drop to zero)
        let manager = PeerNetworkManager::new_for_test(cf).await;
        manager.insert_test_peer(test_socket_address(1), ServiceFlags::NETWORK).await;
        manager.evict_mismatched_peers().await;
        assert_eq!(manager.test_peer_count().await, 1);

        // All peers lack service: tick 1 drops all but 1, tick 2 preserves the lone peer
        let manager = PeerNetworkManager::new_for_test(cf).await;
        manager.insert_test_peer(test_socket_address(1), ServiceFlags::NETWORK).await;
        manager.insert_test_peer(test_socket_address(2), ServiceFlags::NETWORK).await;
        manager.insert_test_peer(test_socket_address(3), ServiceFlags::NETWORK).await;
        manager.evict_mismatched_peers().await;
        assert_eq!(manager.test_peer_count().await, 1);
        manager.evict_mismatched_peers().await;
        assert_eq!(manager.test_peer_count().await, 1);

        // Mixed pool: only mismatched peers are dropped, matching peers survive
        let manager = PeerNetworkManager::new_for_test(cf).await;
        let p1 = test_socket_address(1);
        let p2 = test_socket_address(2);
        let p3 = test_socket_address(3);
        let p4 = test_socket_address(4);
        manager.insert_test_peer(p1, cf).await;
        manager.insert_test_peer(p2, cf).await;
        manager.insert_test_peer(p3, ServiceFlags::NETWORK).await;
        manager.insert_test_peer(p4, ServiceFlags::NETWORK).await;
        manager.evict_mismatched_peers().await;
        assert_eq!(manager.test_peer_count().await, 2);
        assert!(manager.test_is_connected(&p1).await);
        assert!(manager.test_is_connected(&p2).await);
        assert!(!manager.test_is_connected(&p3).await);
        assert!(!manager.test_is_connected(&p4).await);
    }

    #[tokio::test]
    async fn test_next_peer_skips_banned_and_favours_high_score() {
        let manager = PeerNetworkManager::new_for_test(ServiceFlags::NONE).await;
        let good = test_socket_address(11);
        let neutral = test_socket_address(12);
        let banned = test_socket_address(13);

        manager.insert_test_peer(good, ServiceFlags::NETWORK).await;
        manager.insert_test_peer(neutral, ServiceFlags::NETWORK).await;
        manager.insert_test_peer(banned, ServiceFlags::NETWORK).await;

        let reputation = manager.test_reputation_manager();
        reputation.update_reputation(good, -50, "best").await;
        for _ in 0..10 {
            reputation
                .update_reputation(banned, misbehavior_scores::INVALID_MESSAGE, "abuse")
                .await;
        }
        assert!(reputation.is_banned(&banned).await);

        let samples = 2_000;
        let mut good_hits = 0u32;
        let mut neutral_hits = 0u32;
        for _ in 0..samples {
            match manager.test_next_peer_from_pool().await {
                Some(addr) if addr == good => good_hits += 1,
                Some(addr) if addr == neutral => neutral_hits += 1,
                Some(addr) if addr == banned => panic!("banned peer must never be selected"),
                Some(other) => panic!("unexpected peer {}", other),
                None => panic!("expected a selectable peer"),
            }
        }
        assert_eq!(good_hits + neutral_hits, samples);

        // Weight good = max(1, -50 + 51) = 1, weight neutral = max(1, 0 + 51) = 51.
        // Neutral should dominate by ~51x. Use a loose lower bound to stay robust.
        assert!(
            neutral_hits > good_hits * 10,
            "neutral (weight 51) should massively outpace good (weight 1): neutral={}, good={}",
            neutral_hits,
            good_hits
        );
    }

    #[tokio::test]
    async fn test_next_peer_weighted_selection_distribution() {
        let manager = PeerNetworkManager::new_for_test(ServiceFlags::NONE).await;
        let high = test_socket_address(21);
        let zero = test_socket_address(22);

        manager.insert_test_peer(high, ServiceFlags::NETWORK).await;
        manager.insert_test_peer(zero, ServiceFlags::NETWORK).await;

        let reputation = manager.test_reputation_manager();
        // score=100 is the ban threshold, push score to 99 to keep peer eligible.
        reputation.update_reputation(high, 99, "near-ban score for weighting").await;

        let samples = 4_000;
        let mut high_hits = 0u32;
        let mut zero_hits = 0u32;
        for _ in 0..samples {
            match manager.test_next_peer_from_pool().await.expect("peer present") {
                addr if addr == high => high_hits += 1,
                addr if addr == zero => zero_hits += 1,
                other => panic!("unexpected peer {}", other),
            }
        }

        // Expected weight ratio: high = max(1, 99 + 51) = 150, zero = 51.
        // Ratio = 150/51 ≈ 2.94. Use a loose tolerance to avoid flakes.
        let ratio = high_hits as f64 / zero_hits.max(1) as f64;
        assert!(
            (2.0..4.0).contains(&ratio),
            "high-score peer should win between 2x and 4x as often, ratio={}, high={}, zero={}",
            ratio,
            high_hits,
            zero_hits
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_capability_rejection_cache_expires() {
        let manager = PeerNetworkManager::new_for_test(ServiceFlags::COMPACT_FILTERS).await;
        let fresh = test_socket_address(42);
        let expired = test_socket_address(43);

        manager.insert_test_capability_rejected(expired).await;
        tokio::time::advance(Duration::from_secs(31 * 60)).await;
        manager.insert_test_capability_rejected(fresh).await;

        assert!(manager.test_is_capability_rejected(&fresh).await);
        assert!(!manager.test_is_capability_rejected(&expired).await);

        assert_eq!(manager.test_capability_rejected_count().await, 1);
    }
}
