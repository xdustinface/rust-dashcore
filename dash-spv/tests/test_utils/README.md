# Test Utilities

This directory contains utilities for testing the dash-spv client.

## Components

### DashCoreNode (node.rs)
A harness for running a real Dash Core (dashd) node for integration testing.

**Features:**
- Starts real dashd with configurable parameters
- Uses existing regtest blockchain data
- Handles file descriptor limits via bash wrapper with `ulimit -n 10000`
- Provides proper cleanup on test completion

**Configuration:**
Uses environment variables for paths:
- `DASHD_PATH` - Path to dashd binary (default: `$HOME/GIT/dash/src/dashd`)
- `DASHD_DATADIR` - Path to datadir (default: `$HOME/dashcore-regtest-data`)

## Wallet Integration

Tests use the real `WalletManager<ManagedWalletInfo>` implementation from `key-wallet-manager`, providing authentic wallet behavior identical to production use.
