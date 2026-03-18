---
description: Cross-crate architecture and dependency relationships
---

## Crate Dependency Graph

```
internals  ←──  hashes  ←──  dash  ←──┬── dash-spv  ←── dash-spv-ffi
                                       ├── key-wallet ←── key-wallet-ffi
                                       ├── rpc-json
                                       └── rpc-client (uses rpc-json)

dash-spv also depends on: key-wallet (via WalletInterface trait)
```

## Leaf vs Integration Crates

- **Leaf crates** (no workspace dependents): `dash-spv-ffi`, `key-wallet-ffi`, `rpc-client`, `rpc-integration-test`, `fuzz`
- **Core crates** (many dependents): `internals`, `hashes`, `dash`
- **Integration crates**: `dash-spv` (combines dash + key-wallet + network + storage)

## Shared Abstractions

- `Encodable` / `Decodable` (dash) — consensus serialization, used everywhere
- `Hash` / `HashEngine` (hashes) — hash trait, used by dash and all dependents
- `WalletInterface` trait (key-wallet-manager) — used by dash-spv for wallet integration
- `StorageManager` trait (dash-spv) — composed from sub-traits (BlockHeaderStorage, FilterHeaderStorage, etc.)
- `NetworkManager` trait (dash-spv) — peer connection abstraction

## Feature-Gated Cross-Crate Test Utilities

When crate A needs test helpers from crate B:
1. Crate B exports helpers behind `#[cfg(any(test, feature = "test-utils"))]`
2. Crate A adds `B = { features = ["test-utils"] }` in `[dev-dependencies]`

Currently available:
- `dash` exports: block fixtures, transaction builders, address helpers, chainlock/instantlock fixtures
- `dash-spv` exports: `DashdTestContext`, `MockNetworkManager`, `DashCoreNode`
- `key-wallet` exports: wallet, account, and UTXO test fixtures

## Data Flow Examples

**Transaction from network to wallet:**
```
Network peer → PeerNetworkManager → MessageDispatcher → BlocksManager
  → block contains tx → WalletInterface::check_transaction() → key-wallet
  → updates UTXOs, balance → emits SyncEvent::BlockProcessed
```

**SPV filter matching:**
```
FilterHeadersManager syncs filter headers → FiltersManager downloads filters in batches
  → WalletInterface provides filter match keys (script pubkeys)
  → matching filters → BlocksManager downloads full blocks
  → transactions extracted and checked against wallet
```
