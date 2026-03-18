# key-wallet-ffi crate

C-compatible FFI bindings for wallet functionality.

## Key Types

- `FFIWalletManager` — holds `Arc<RwLock<WalletManager>>`, tokio runtime, and network
- `FFIBalance` — struct with confirmed, unconfirmed, immature, locked, total (all u64)
- `FFIWallet` — opaque wallet handle wrapping `Arc<Wallet>`
- `FFIUTXO` — UTXO representation for FFI
- `FFIError` / `FFIErrorCode` — error handling

## Module Coverage

Comprehensive FFI surface: wallet_manager, account, managed_account, address, address_pool, mnemonic, derivation, keys, transaction, transaction_checking, utxo, bip38.

## FFI Safety Rules

Same patterns as dash-spv-ffi:
- Opaque pointers with `_free()` functions
- `Arc<RwLock<T>>` for shared ownership
- Null pointer validation on all inputs
- No panics across FFI boundary

## Feature Flags

- `default = ["bincode", "eddsa", "bls", "bip38"]`
- `bincode` — binary serialization
- `eddsa` — Ed25519 support
- `bls` — BLS signature support
- `bip38` — encrypted private keys

## Test Utilities

Uses `key-wallet` with `test-utils` feature. Comprehensive test modules embedded in each source file.
