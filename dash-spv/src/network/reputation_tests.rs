//! Unit tests for reputation system (in-module tests)

#[cfg(test)]
mod tests {
    use crate::storage::{PersistentPeerStorage, PersistentStorage};

    use super::super::*;
    use std::net::SocketAddr;
    use std::time::SystemTime;

    #[tokio::test]
    async fn test_basic_reputation_operations() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:8333".parse().unwrap();

        // Initial score should be 0
        assert_eq!(manager.get_score(&peer).await, 0);

        // Test misbehavior
        manager
            .update_reputation(peer, misbehavior_scores::INVALID_MESSAGE, "Test invalid message")
            .await;
        assert_eq!(manager.get_score(&peer).await, 10);

        // Test positive behavior
        manager.update_reputation(peer, positive_scores::VALID_HEADERS, "Test valid headers").await;
        assert_eq!(manager.get_score(&peer).await, 5);
    }

    #[tokio::test]
    async fn test_banning_mechanism() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "192.168.1.1:8333".parse().unwrap();

        // Accumulate misbehavior
        for i in 0..10 {
            let banned = manager
                .update_reputation(
                    peer,
                    misbehavior_scores::INVALID_MESSAGE,
                    &format!("Violation {}", i),
                )
                .await;

            // Should be banned on the 10th violation (total score = 100)
            if i == 9 {
                assert!(banned);
            } else {
                assert!(!banned);
            }
        }

        assert!(manager.is_banned(&peer).await);
    }

    #[tokio::test]
    async fn test_reputation_persistence() {
        let manager = PeerReputationManager::new();
        let peer1: SocketAddr = "10.0.0.1:8333".parse().unwrap();
        let peer2: SocketAddr = "10.0.0.2:8333".parse().unwrap();

        // Set reputations
        manager.update_reputation(peer1, -10, "Good peer").await;
        manager.update_reputation(peer2, 50, "Bad peer").await;

        // Save and load
        let temp_dir = tempfile::TempDir::new().unwrap();
        let peer_storage = PersistentPeerStorage::open(temp_dir.path())
            .await
            .expect("Failed to open PersistentPeerStorage");
        manager.save_to_storage(&peer_storage).await.unwrap();

        let new_manager = PeerReputationManager::new();
        new_manager.load_from_storage(&peer_storage).await.unwrap();

        // Verify scores were preserved
        assert_eq!(new_manager.get_score(&peer1).await, -10);
        assert_eq!(new_manager.get_score(&peer2).await, 50);
    }

    #[tokio::test]
    async fn test_peer_selection() {
        let manager = PeerReputationManager::new();

        let good_peer = AddrV2Message::dummy(0, "1.1.1.1".parse().unwrap(), 8333);
        let neutral_peer = AddrV2Message::dummy(0, "2.2.2.2".parse().unwrap(), 8333);
        let bad_peer = AddrV2Message::dummy(0, "3.3.3.3".parse().unwrap(), 8333);

        // Set different reputations
        manager.update_reputation(good_peer.socket_addr().unwrap(), -20, "Very good").await;
        manager.update_reputation(bad_peer.socket_addr().unwrap(), 80, "Very bad").await;
        // neutral_peer has default score of 0

        let all_peers = vec![good_peer.clone(), neutral_peer.clone(), bad_peer.clone()];
        let selected = manager.select_best_peers(all_peers, 2).await;

        // Should select good_peer first, then neutral_peer
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0], good_peer.socket_addr().unwrap());
        assert_eq!(selected[1], neutral_peer.socket_addr().unwrap());
    }

    #[tokio::test]
    async fn test_connection_tracking() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        // Track connection attempts
        manager.record_connection_attempt(peer).await;
        manager.record_connection_attempt(peer).await;
        manager.record_successful_connection(peer).await;

        let reputations = manager.get_all_reputations().await;
        let rep = &reputations[&peer];

        assert_eq!(rep.connection_attempts, 2);
        assert_eq!(rep.successful_connections, 1);
    }

    #[test]
    fn test_default_session_outcomes_are_empty() {
        let rep = PeerReputation::default();

        assert!(rep.last_success.is_none());
        assert!(rep.last_tried.is_none());
        assert_eq!(rep.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn test_record_connection_attempt_sets_last_tried() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:1111".parse().unwrap();

        let before = SystemTime::now();
        manager.record_connection_attempt(peer).await;
        let after = SystemTime::now();

        let reputations = manager.get_all_reputations().await;
        let rep = &reputations[&peer];

        let last_tried = rep.last_tried.expect("last_tried should be set");
        assert!(last_tried >= before);
        assert!(last_tried <= after);
        assert!(rep.last_success.is_none());
        assert_eq!(rep.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn test_record_successful_connection_sets_last_success_and_resets_failures() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:2222".parse().unwrap();

        // Seed a non-zero failure streak first to verify the reset behaviour.
        manager.record_failure_with_penalty(peer, 1, "seed").await;
        manager.record_failure_with_penalty(peer, 1, "seed").await;
        manager.record_failure_with_penalty(peer, 1, "seed").await;
        let last_tried_before_success = {
            let reputations = manager.get_all_reputations().await;
            assert_eq!(reputations[&peer].consecutive_failures, 3);
            reputations[&peer].last_tried.expect("last_tried set by failure seeds")
        };

        let before = SystemTime::now();
        manager.record_successful_connection(peer).await;
        let after = SystemTime::now();

        let reputations = manager.get_all_reputations().await;
        let rep = &reputations[&peer];

        let last_success = rep.last_success.expect("last_success should be set");
        assert!(last_success >= before);
        assert!(last_success <= after);
        assert_eq!(rep.consecutive_failures, 0);
        assert_eq!(rep.successful_connections, 1);
        assert_eq!(
            rep.last_tried,
            Some(last_tried_before_success),
            "record_successful_connection must not clear last_tried"
        );
    }

    #[tokio::test]
    async fn test_record_failure_with_penalty_increments_streak_and_applies_score() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:5555".parse().unwrap();

        manager.record_successful_connection(peer).await;
        let success_ts = manager
            .get_all_reputations()
            .await
            .get(&peer)
            .expect("peer exists")
            .last_success
            .expect("last_success should be set after successful connection");

        let banned = manager
            .record_failure_with_penalty(peer, misbehavior_scores::INVALID_MESSAGE, "test")
            .await;
        assert!(!banned);

        let reputations = manager.get_all_reputations().await;
        let rep = &reputations[&peer];
        assert_eq!(rep.consecutive_failures, 1);
        assert_eq!(rep.score, misbehavior_scores::INVALID_MESSAGE);
        assert_eq!(
            rep.last_success,
            Some(success_ts),
            "record_failure_with_penalty must not clear last_success"
        );
    }

    #[tokio::test]
    async fn test_record_failure_with_penalty_always_sets_last_tried() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:6666".parse().unwrap();

        let before = SystemTime::now();
        manager.record_failure_with_penalty(peer, 5, "test").await;
        let after = SystemTime::now();

        let reputations = manager.get_all_reputations().await;
        let last_tried = reputations[&peer].last_tried.expect("last_tried should be set");
        assert!(last_tried >= before);
        assert!(last_tried <= after);

        let before_second = SystemTime::now();
        manager.record_failure_with_penalty(peer, 5, "test again").await;
        let after_second = SystemTime::now();

        let second_tried = manager
            .get_all_reputations()
            .await
            .get(&peer)
            .unwrap()
            .last_tried
            .expect("last_tried should be updated");
        assert!(second_tried >= before_second);
        assert!(second_tried <= after_second);
    }

    #[tokio::test]
    async fn test_record_failure_with_penalty_triggers_ban() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:7777".parse().unwrap();

        // Apply enough score to bring the peer to the ban threshold.
        // INVALID_HEADER = 50, so two calls reach 100 = MAX_MISBEHAVIOR_SCORE.
        let first = manager
            .record_failure_with_penalty(peer, misbehavior_scores::INVALID_HEADER, "bad header 1")
            .await;
        assert!(!first);

        let second = manager
            .record_failure_with_penalty(peer, misbehavior_scores::INVALID_HEADER, "bad header 2")
            .await;
        assert!(second, "second call should trigger a ban");
        assert!(manager.is_banned(&peer).await);
        assert_eq!(manager.get_all_reputations().await[&peer].consecutive_failures, 2);
    }

    #[test]
    fn test_consecutive_failures_clamped_on_deserialize() {
        let json = r#"{"score":0,"ban_count":0,"positive_actions":0,"negative_actions":0,"connection_attempts":0,"successful_connections":0,"last_success":null,"last_tried":null,"consecutive_failures":99999}"#;
        let rep: PeerReputation = serde_json::from_str(json).expect("deserialize");
        assert_eq!(rep.consecutive_failures, MAX_CONSECUTIVE_FAILURES);
    }

    #[tokio::test]
    async fn test_record_failure_with_penalty_streak_keeps_incrementing() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:8888".parse().unwrap();

        // Each call should increment the streak independently, never resetting.
        for expected in 1u32..=5 {
            manager.record_failure_with_penalty(peer, 1, "extra failure").await;
            let reputations = manager.get_all_reputations().await;
            assert_eq!(reputations[&peer].consecutive_failures, expected);
        }
    }

    #[tokio::test]
    async fn test_consecutive_failures_saturates_at_runtime() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:9191".parse().unwrap();

        for _ in 0..=MAX_CONSECUTIVE_FAILURES {
            manager.record_failure_with_penalty(peer, 1, "flood").await;
        }

        let reputations = manager.get_all_reputations().await;
        assert_eq!(reputations[&peer].consecutive_failures, MAX_CONSECUTIVE_FAILURES);
    }

    #[tokio::test]
    async fn test_record_failure_with_penalty_updates_last_tried_after_attempt() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:9100".parse().unwrap();

        manager.record_connection_attempt(peer).await;
        let attempt_time = manager.get_all_reputations().await[&peer]
            .last_tried
            .expect("last_tried set by attempt");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        manager
            .record_failure_with_penalty(peer, misbehavior_scores::INVALID_MESSAGE, "test")
            .await;
        let after_failure = manager.get_all_reputations().await[&peer]
            .last_tried
            .expect("last_tried still set after failure");

        assert!(after_failure > attempt_time, "failure should update last_tried");
    }

    #[tokio::test]
    async fn test_connection_attempt_then_success_preserves_last_tried() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:9101".parse().unwrap();

        manager.record_connection_attempt(peer).await;
        let tried_after_attempt =
            manager.get_all_reputations().await[&peer].last_tried.expect("attempt sets last_tried");
        assert!(manager.get_all_reputations().await[&peer].last_success.is_none());

        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        manager.record_successful_connection(peer).await;

        let rep = &manager.get_all_reputations().await[&peer];
        assert_eq!(
            rep.last_tried,
            Some(tried_after_attempt),
            "success must preserve last_tried from the preceding attempt"
        );
        assert!(rep.last_success.is_some(), "success sets last_success");
        assert_eq!(rep.consecutive_failures, 0);
    }
}
