# hashes crate

Cryptographic hash implementations used throughout the workspace.

## Key Types

- `Hash` trait — fixed-size hash with `hash()`, `from_engine()`, `from_hex()`
- `HashEngine` trait — stateful hashing (implements `io::Write`)
- Hash types: `sha256::Hash`, `sha256d::Hash`, `hash160::Hash`, `ripemd160::Hash`, `sha512::Hash`, `sha512_256::Hash`, `siphash24::Hash`
- `hash_x11::Hash` — Dash X11 mining algorithm (feature-gated with `x11`)
- `Hmac<T>` / `HmacEngine<T>` — HMAC wrappers

## Patterns

- **No-std compatible**: Core functionality works without std (use `alloc` feature)
- **`hash_newtype!` macro**: Creates type-safe hash newtypes in dependent crates (prevents mixing `Txid` with `BlockHash`)
- **Engine pattern**: `let mut engine = sha256::Hash::engine(); engine.input(data); sha256::Hash::from_engine(engine)`
- **Hex display**: All hash types implement hex display formatting

## Feature Flags

- `std` (default), `alloc` — std/alloc support
- `serde` / `serde-std` — serialization
- `x11` — X11 hash function (requires `rs-x11-hash`)
- `schemars` — JSON schema generation
