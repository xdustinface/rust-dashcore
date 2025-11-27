///! Test utilities for dash-spv integration testing.
///!
///! Provides DashCoreNode for realistic testing with real dashd instances.
pub mod node;

pub use node::{is_dashd_available, DashCoreNode};
