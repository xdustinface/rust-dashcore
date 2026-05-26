//! Unit tests for reputation system (in-module tests)

#[cfg(test)]
mod tests {
    use crate::storage::{PersistentPeerStorage, PersistentStorage};

    use super::super::*;
    use std::net::SocketAddr;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    fn make_reputation_json(
        last_tried_secs: Option<u64>,
        last_success_secs: Option<u64>,
    ) -> String {
        let last_tried = match last_tried_secs {
            Some(s) => format!(r#"{{"secs_since_epoch":{s},"nanos_since_epoch":0}}"#),
            None => "null".to_string(),
        };
        let last_success = match last_success_secs {
            Some(s) => format!(r#"{{"secs_since_epoch":{s},"nanos_since_epoch":0}}"#),
            None => "null".to_string(),
        };
        format!(
            r#"{{"score":0,"ban_count":0,"positive_actions":0,"negative_actions":0,"connection_attempts":0,"successful_connections":0,"last_success":{last_success},"last_tried":{last_tried},"consecutive_failures":0}}"#
        )
    }

    #[test]
    fn test_clamp_future_system_time() {
        let now_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        // Future timestamps (1 hour ahead) must be discarded.
        let future_secs = now_secs + 3600;
        let json = make_reputation_json(Some(future_secs), Some(future_secs));
        let rep: PeerReputation = serde_json::from_str(&json).expect("deserialize");
        assert!(rep.last_tried.is_none(), "future last_tried must be nulled");
        assert!(rep.last_success.is_none(), "future last_success must be nulled");

        // Recent past (10 seconds ago) must be preserved.
        let recent_secs = now_secs - 10;
        let json = make_reputation_json(Some(recent_secs), Some(recent_secs));
        let rep: PeerReputation = serde_json::from_str(&json).expect("deserialize");
        assert!(rep.last_tried.is_some(), "recent last_tried must be preserved");
        assert!(rep.last_success.is_some(), "recent last_success must be preserved");
        let expected = UNIX_EPOCH + Duration::from_secs(recent_secs);
        assert_eq!(rep.last_tried.unwrap(), expected);
        assert_eq!(rep.last_success.unwrap(), expected);
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

    #[tokio::test]
    async fn test_normalize_after_load_via_storage_round_trip() {
        let temp_dir = tempfile::TempDir::new().unwrap();

        // Write a reputations.json directly so we can embed a future `last_tried`
        // that `clamp_future_system_time` will discard on load, and a non-zero
        // `consecutive_failures` that `normalize_after_load` must then reset to 0.
        let peers_dir = temp_dir.path().join("peers");
        std::fs::create_dir_all(&peers_dir).unwrap();
        let reputation_file = peers_dir.join("reputations.json");

        let future_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + 3_600;
        let json = format!(
            r#"{{"127.0.0.1:9202":{{"score":0,"ban_count":0,"positive_actions":0,"negative_actions":0,"connection_attempts":3,"successful_connections":2,"last_success":null,"last_tried":{{"secs_since_epoch":{future_secs},"nanos_since_epoch":0}},"consecutive_failures":5}}}}"#
        );
        std::fs::write(&reputation_file, json).unwrap();

        let peer_storage = PersistentPeerStorage::open(temp_dir.path())
            .await
            .expect("Failed to open PersistentPeerStorage");

        let manager = PeerReputationManager::new();
        manager.load_from_storage(&peer_storage).await.unwrap();

        let peer: SocketAddr = "127.0.0.1:9202".parse().unwrap();
        let reputations = manager.get_all_reputations().await;
        let rep = reputations.get(&peer).expect("peer must be present after load");

        assert!(rep.last_tried.is_none(), "future last_tried must be discarded by clamp");
        assert_eq!(
            rep.consecutive_failures, 0,
            "normalize_after_load must reset streak when last_tried is absent"
        );
    }

    #[tokio::test]
    async fn test_normalize_after_load_preserves_failures_when_last_tried_valid() {
        let temp_dir = tempfile::TempDir::new().unwrap();

        let peers_dir = temp_dir.path().join("peers");
        std::fs::create_dir_all(&peers_dir).unwrap();
        let reputation_file = peers_dir.join("reputations.json");

        let recent_secs = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - 300;
        let json = format!(
            r#"{{"127.0.0.1:9203":{{"score":0,"ban_count":0,"positive_actions":0,"negative_actions":0,"connection_attempts":5,"successful_connections":2,"last_success":null,"last_tried":{{"secs_since_epoch":{recent_secs},"nanos_since_epoch":0}},"consecutive_failures":3}}}}"#
        );
        std::fs::write(&reputation_file, json).unwrap();

        let peer_storage = PersistentPeerStorage::open(temp_dir.path())
            .await
            .expect("Failed to open PersistentPeerStorage");

        let manager = PeerReputationManager::new();
        manager.load_from_storage(&peer_storage).await.unwrap();

        let peer: SocketAddr = "127.0.0.1:9203".parse().unwrap();
        let reputations = manager.get_all_reputations().await;
        let rep = reputations.get(&peer).expect("peer must be present after load");

        assert!(rep.last_tried.is_some(), "valid last_tried must be preserved");
        assert_eq!(
            rep.consecutive_failures, 3,
            "non-zero streak must be preserved when last_tried is valid"
        );
    }

    #[tokio::test]
    async fn test_filter_unbanned_excludes_banned_entries() {
        let manager = PeerReputationManager::new();
        let good: SocketAddr = "127.0.0.1:5001".parse().unwrap();
        let bad: SocketAddr = "127.0.0.1:5002".parse().unwrap();
        let unknown: SocketAddr = "127.0.0.1:5003".parse().unwrap();

        manager.update_reputation(good, -10, "good").await;
        for _ in 0..10 {
            manager.update_reputation(bad, misbehavior_scores::INVALID_MESSAGE, "abuse").await;
        }
        assert!(manager.is_banned(&bad).await);

        let input = vec![(good, ()), (bad, ()), (unknown, ())];
        let surviving = manager.filter_unbanned(input).await;
        let addrs: Vec<SocketAddr> = surviving.iter().map(|(a, _)| *a).collect();

        assert!(addrs.contains(&good));
        assert!(addrs.contains(&unknown));
        assert!(!addrs.contains(&bad), "banned peer must be excluded");
    }

    #[tokio::test]
    async fn test_score_based_ban_survives_restart() {
        let peer: SocketAddr = "127.0.0.1:5010".parse().unwrap();

        let temp_dir = tempfile::TempDir::new().unwrap();
        let peer_storage = PersistentPeerStorage::open(temp_dir.path())
            .await
            .expect("Failed to open PersistentPeerStorage");

        // Accumulate score to the ban threshold in the first session.
        let session_one = PeerReputationManager::new();
        for _ in 0..10 {
            session_one.update_reputation(peer, misbehavior_scores::INVALID_MESSAGE, "abuse").await;
        }
        assert!(session_one.is_banned(&peer).await, "peer must be banned in session one");
        session_one.save_to_storage(&peer_storage).await.unwrap();

        // Load into a fresh manager (simulating a restart — `banned_until` is not persisted).
        let session_two = PeerReputationManager::new();
        session_two.load_from_storage(&peer_storage).await.unwrap();

        assert!(
            session_two.is_banned(&peer).await,
            "score-based ban must survive restart even when banned_until is not persisted"
        );

        let input = vec![(peer, ())];
        let surviving = session_two.filter_unbanned(input).await;
        assert!(
            surviving.is_empty(),
            "filter_unbanned must exclude a peer whose score is at the ban threshold after restart"
        );
    }

    #[tokio::test]
    async fn test_selection_weights_match_score_offset() {
        let manager = PeerReputationManager::new();
        let best: SocketAddr = "127.0.0.1:6001".parse().unwrap();
        let neutral: SocketAddr = "127.0.0.1:6002".parse().unwrap();
        let worst: SocketAddr = "127.0.0.1:6003".parse().unwrap();
        let banned: SocketAddr = "127.0.0.1:6004".parse().unwrap();

        manager.update_reputation(best, -50, "great").await;
        manager.update_reputation(worst, 90, "bad").await;
        for _ in 0..10 {
            manager.update_reputation(banned, misbehavior_scores::INVALID_MESSAGE, "abuse").await;
        }

        let peers = vec![(best, ()), (neutral, ()), (worst, ()), (banned, ())];
        let weights = manager.selection_weights(&peers).await;
        assert_eq!(weights.len(), 4);
        assert_eq!(weights[0], 150, "score -50 maps to weight 150 (highest priority)");
        assert_eq!(weights[1], 100, "score 0 maps to weight 100 (neutral)");
        assert_eq!(weights[2], 10, "score 90 maps to weight 10 (near-ban, rarely selected)");
        assert_eq!(weights[3], 0, "banned peer must have weight 0");
    }

    #[tokio::test]
    async fn test_pick_worst_uses_highest_score() {
        let manager = PeerReputationManager::new();
        let mild: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let bad: SocketAddr = "127.0.0.1:7002".parse().unwrap();
        let neutral: SocketAddr = "127.0.0.1:7003".parse().unwrap();

        manager.update_reputation(mild, 20, "mild").await;
        manager.update_reputation(bad, 80, "bad").await;

        let peers = vec![(mild, ()), (bad, ()), (neutral, ())];
        let (victim, victim_score) = manager.pick_worst(&peers).await.expect("non-empty input");
        assert_eq!(victim, bad);
        assert_eq!(victim_score, 80, "returned score must match the worst observed score");
    }

    #[tokio::test]
    async fn test_pick_worst_tie_breaks_on_most_recent_negative_event() {
        let manager = PeerReputationManager::new();
        let stale: SocketAddr = "127.0.0.1:7101".parse().unwrap();
        let fresh: SocketAddr = "127.0.0.1:7102".parse().unwrap();

        manager.update_reputation(stale, 30, "first abuse").await;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        manager.update_reputation(fresh, 30, "later abuse").await;

        let peers = vec![(stale, ()), (fresh, ())];
        let (victim, victim_score) = manager.pick_worst(&peers).await.expect("non-empty input");
        assert_eq!(victim, fresh, "tie at score 30 should pick the most recent offender");
        assert_eq!(victim_score, 30, "returned score must match the tied worst score");
    }

    #[tokio::test]
    async fn test_pick_worst_all_neutral_returns_some_peer() {
        let manager = PeerReputationManager::new();
        let a: SocketAddr = "127.0.0.1:8001".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:8002".parse().unwrap();
        let peers = vec![(a, ()), (b, ())];
        let (victim, victim_score) =
            manager.pick_worst(&peers).await.expect("must return Some for non-empty input");
        assert!(
            victim == a || victim == b,
            "any peer is valid when scores are equal and no events exist"
        );
        assert_eq!(victim_score, 0, "neutral peers must report score 0");
    }

    #[tokio::test]
    async fn test_record_disconnect_sets_last_reason_and_event() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:7201".parse().unwrap();

        manager.record_disconnect(peer, DisconnectReason::PingTimeout).await;

        let reputations = manager.get_all_reputations().await;
        let rep = &reputations[&peer];
        assert_eq!(rep.last_disconnect_reason, Some(DisconnectReason::PingTimeout));

        let events = manager.get_recent_events().await;
        let event = events.iter().rev().find(|e| e.peer == peer).expect("disconnect event present");
        let expected = format!("disconnect: {}", DisconnectReason::PingTimeout.as_str());
        assert_eq!(event.reason, expected, "event reason must encode the disconnect cause exactly");
    }

    #[tokio::test]
    async fn test_disconnect_reason_persists_across_save_load() {
        let manager = PeerReputationManager::new();
        let peer: SocketAddr = "127.0.0.1:7301".parse().unwrap();

        manager.record_disconnect(peer, DisconnectReason::DecodeError).await;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let peer_storage = PersistentPeerStorage::open(temp_dir.path())
            .await
            .expect("Failed to open PersistentPeerStorage");
        manager.save_to_storage(&peer_storage).await.unwrap();

        let restored = PeerReputationManager::new();
        restored.load_from_storage(&peer_storage).await.unwrap();
        let reputations = restored.get_all_reputations().await;
        assert_eq!(
            reputations[&peer].last_disconnect_reason,
            Some(DisconnectReason::DecodeError),
            "disconnect reason must survive save/load round trip"
        );
    }

    #[test]
    fn test_reputation_loads_without_last_disconnect_reason_field() {
        let json = r#"{"score":5,"ban_count":0,"positive_actions":0,"negative_actions":0,"connection_attempts":0,"successful_connections":0,"last_success":null,"last_tried":null,"consecutive_failures":0}"#;
        let rep: PeerReputation = serde_json::from_str(json).expect("legacy file must deserialize");
        assert!(
            rep.last_disconnect_reason.is_none(),
            "missing field must default to None for backward compatibility"
        );
    }
}
