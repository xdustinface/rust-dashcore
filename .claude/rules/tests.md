---
description: Testing conventions for rust-dashcore
paths: ["**/tests/**", "**/test_utils/**", "**/*_test*"]
---

## Test helpers

Before creating test helpers, search for existing utilities:
- Each crate has a `test_utils/` module with shared fixtures and builders
- Feature-gated test exports: `#[cfg(any(test, feature = "test-utils"))]`
- Cross-crate test utilities available via `test-utils` feature flag in Cargo.toml

Key test utility locations:
- `dash/src/test_utils/` — protocol type fixtures (blocks, transactions, addresses, chainlocks)
- `dash-spv/src/test_utils/` — `DashdTestContext`, `MockNetworkManager`, `DashCoreNode`
- `key-wallet/src/test_utils/` — wallet, account, and UTXO fixtures

## Test structure

Prefer consolidating related test cases into fewer, well-structured tests over many tiny isolated tests. Separate tests only when setup or assertions are genuinely different.

## Test commands

Use `cargo test --lib` for unit tests (skips doc-test compilation).
Use `cargo test -p <crate>` for crate-specific tests.
Integration tests with dashd: `eval $(python3 contrib/setup-dashd.py)` first.
