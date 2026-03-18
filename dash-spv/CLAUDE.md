# dash-spv crate

Async, event-driven SPV client with parallel sync managers.

## Architecture

```
DashSpvClient<W, N, S>  (generic over wallet, network, storage traits)
  └── SyncCoordinator
        ├── BlockHeadersManager  → triggers FilterHeadersManager
        ├── FilterHeadersManager → triggers FiltersManager
        ├── FiltersManager       → batch pipeline, triggers BlocksManager
        ├── BlocksManager        → downloads matched blocks
        ├── MasternodesManager   → syncs masternode list
        ├── ChainLockManager     → monitors chain locks
        ├── InstantSendManager   → monitors instant locks
        └── MempoolManager       → mempool relay coordination
```

Each manager runs in its own tokio task via `spawn_manager!` macro. Managers communicate through `SyncEvent` broadcast channels.

## Key Types

- `DashSpvClient<W, N, S>` — main client, generic over `WalletInterface`, `NetworkManager`, `StorageManager`
- `ClientConfig` — builder-pattern configuration
- `SyncEvent` — event enum (BlockHeadersStored, FiltersSyncComplete, BlockProcessed, ChainLockReceived, etc.)
- `SyncState` — WaitForEvents/WaitingForConnections/Syncing/Synced/Error
- `SyncProgress` — aggregate progress across all managers
- `HashedBlockHeader` — header wrapper caching X11 hash (performance optimization)
- `PeerId(u64)` — peer identifier
- `SpvError` — top-level error wrapping domain errors

## Patterns

- **Event coordination**: Managers emit `SyncEvent`, others react. Not imperative.
- **RequestSender**: Managers queue network requests via `RequestSender`, decoupled from network layer.
- **Progress tracking**: Each manager implements `progress()`, coordinator aggregates via watch channels.
- **Storage abstraction**: `StorageManager` is a trait composition. `DiskStorageManager` uses segmented files.
- **Error domains**: `SpvError`, `NetworkError`, `StorageError`, `SyncError`, `ValidationError`, `WalletError` with Result type aliases.

## Test Utilities

Located in `src/test_utils/`, feature-gated with `test-utils`:

- `DashdTestContext` — full integration test harness with real dashd
- `DashCoreNode` — spawns/controls dashd regtest process
- `MockNetworkManager` — in-memory network for unit tests (no real connections)
- `test_socket_address()` — generate test peer addresses
- `WalletFile` — RPC wallet creation helper

Integration tests require: `eval $(python3 contrib/setup-dashd.py)`

### Debugging integration test failures

When dashd integration tests fail, use these env vars to retain logs for analysis:

- `DASHD_TEST_LOG=1` — enable per-test console logging (use with `--nocapture`)
- `DASHD_TEST_RETAIN_DIR=<path>` — retain test data directories on failure (contains dashd logs, SPV storage, wallet state)
- `DASHD_TEST_RETAIN_ALWAYS=1` — retain even on success

Example: `DASHD_TEST_LOG=1 DASHD_TEST_RETAIN_DIR=/tmp/test-debug cargo test -p dash-spv dashd_sync -- --nocapture`

The retained directory contains dashd's `debug.log` and the SPV client's storage files — check both when diagnosing sync or protocol issues.

## Feature Flags

- `test-utils` — expose test infrastructure (enables dashcore/test-utils, tempfile, dashcore-rpc)
- `uniffi` — mobile FFI bindings via UniFFI bridge
