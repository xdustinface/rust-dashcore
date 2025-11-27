//! Minimal test for regtest handshake

use dash_spv::client::config::MempoolStrategy;
use dash_spv::network::{HandshakeManager, Peer};
use dashcore::Network;
use std::net::SocketAddr;

#[tokio::test]
async fn test_regtest_handshake() {
    // Initialize tracing for debug output
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    tracing::info!("=== Testing Regtest Handshake ===");

    // Connect to regtest node
    let peer_addr: SocketAddr = "127.0.0.1:19999".parse().expect("Valid peer address");
    tracing::info!("Connecting to regtest node at {}", peer_addr);

    let mut connection = match Peer::connect(peer_addr, 10, Network::Regtest).await {
        Ok(conn) => {
            tracing::info!("✓ TCP connection established");
            conn
        }
        Err(e) => {
            tracing::error!("✗ TCP connection failed: {}", e);
            panic!("Failed to connect: {}", e);
        }
    };

    // Perform handshake
    let mut handshake_manager = HandshakeManager::new(
        Network::Regtest,
        MempoolStrategy::BloomFilter,
        Some("/test-regtest-handshake/".to_string()),
    );

    tracing::info!("Starting handshake...");
    match handshake_manager.perform_handshake(&mut connection).await {
        Ok(()) => {
            tracing::info!("✓ Handshake successful!");
            assert!(
                connection.is_connected(),
                "Peer should be connected after successful handshake"
            );

            // Get peer info
            let peer_info = connection.peer_info();
            tracing::info!("Peer info: {:?}", peer_info);

            connection.disconnect().await.expect("Failed to disconnect");
        }
        Err(e) => {
            tracing::error!("✗ Handshake failed: {}", e);
            panic!("Handshake failed: {}", e);
        }
    }
}
