# dash crate

Core Dash protocol types — blocks, transactions, scripts, addresses, masternode lists.

## Key Types

- `Block` / `Header` — blockchain structures
- `Transaction` / `TxIn` / `TxOut` — transaction types with special transaction support (DIP2/DIP3)
- `TransactionPayload` — enum of special tx types (provider registration, asset lock/unlock, quorum commitment)
- `Script` / `ScriptBuf` — unsized/owned script types with builder pattern
- `Address<V>` — type-state validated addresses (`NetworkUnchecked` → `NetworkChecked`)
- `Amount` / `SignedAmount` — satoshi amounts with denomination support
- `PublicKey` / `PrivateKey` — ECDSA key pairs
- `MasternodeList` / `MasternodeListEntry` / `QuorumEntry` — DIP3 masternode system
- `ChainLock` / `InstantLock` — ephemeral Dash security mechanisms
- `Network` — enum: Mainnet, Testnet, Devnet, Regtest
- Hash newtypes: `BlockHash`, `Txid`, `Wtxid` — type-safe, never mix them

## Patterns

- **Consensus encoding**: Types implement `Encodable`/`Decodable`. Use `impl_consensus_encoding!` macro for simple structs.
- **Hash newtypes**: Use `hash_newtype!` macro from hashes crate. Prevents semantic hash mixing.
- **Type-state addresses**: `Address<NetworkUnchecked>` must be validated before use.
- **Script builder**: `ScriptBuf::builder().push_opcode(...).push_slice(...).into_script()`
- **Network params**: Always from `Network` enum, never hardcoded.
- **Little-endian**: Numbers use LE for consensus. Hashes are display-reversed.

## Test Utilities

Located in `src/test_utils/`, feature-gated with `#[cfg(any(test, feature = "test-utils"))]`:

- `test_utils::address` — test address helpers
- `test_utils::block` — fixture blocks and header builders
- `test_utils::transaction` — test transaction builders
- `test_utils::chainlock` — ChainLock fixtures
- `test_utils::instantlock` — InstantLock fixtures
- `test_utils::filter` — filter test data
- `test_utils::network` — network test utilities

Always check these before creating new test fixtures.

## Feature Flags

- `std` (default), `serde`, `bincode` — serialization
- `core-block-hash-use-x11` — Dash X11 block hashing
- `bls` / `quorum_validation` / `message_verification` — BLS/quorum features
- `signer` / `secp-recovery` — signing helpers
- `test-utils` — expose test fixtures to dependent crates
