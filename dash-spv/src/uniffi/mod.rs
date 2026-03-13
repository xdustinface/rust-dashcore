//! Minimal UniFFI bridge module for dash-spv.
//!
//! This module validates three UniFFI call patterns:
//! - Sync function
//! - Async function
//! - Callback interface
//!
//! Compiled only when the `uniffi` feature is enabled.

use std::sync::Arc;

uniffi::setup_scaffolding!();

/// A simple sync function that returns a greeting string.
#[uniffi::export]
pub fn hello() -> String {
    "Hello from dash-spv!".to_string()
}

/// An async function that returns the library version.
#[uniffi::export]
pub async fn get_version() -> String {
    crate::VERSION.to_string()
}

/// Callback interface for receiving SPV sync progress events.
#[uniffi::export(callback_interface)]
pub trait SpvEventListener: Send + Sync {
    /// Called when sync progress changes.
    fn on_sync_progress(&self, percentage: f64);
}

/// Starts a mock sync that reports progress via the listener callback.
///
/// Invokes `on_sync_progress` with 0.0 and 100.0 to simulate start and completion.
#[uniffi::export]
pub async fn start_mock_sync(listener: Arc<dyn SpvEventListener>) {
    listener.on_sync_progress(0.0);
    // Simulate minimal async work
    tokio::task::yield_now().await;
    listener.on_sync_progress(100.0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_hello() {
        assert_eq!(hello(), "Hello from dash-spv!");
    }

    #[tokio::test]
    async fn test_get_version() {
        let version = get_version().await;
        assert!(!version.is_empty(), "version should not be empty");
        assert_eq!(version, crate::VERSION);
    }

    struct MockListener {
        events: Mutex<Vec<f64>>,
    }

    impl SpvEventListener for MockListener {
        fn on_sync_progress(&self, percentage: f64) {
            self.events.lock().unwrap().push(percentage);
        }
    }

    #[tokio::test]
    async fn test_start_mock_sync() {
        let listener = Arc::new(MockListener {
            events: Mutex::new(Vec::new()),
        });
        start_mock_sync(listener.clone()).await;
        let events = listener.events.lock().unwrap();
        assert_eq!(*events, vec![0.0, 100.0]);
    }
}
