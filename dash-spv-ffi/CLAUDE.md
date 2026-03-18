# dash-spv-ffi crate

C-compatible FFI bindings for the SPV client.

## Key Types

- `FFIDashSpvClient` — opaque handle to SPV client
- `FFISyncState` — C-safe enum mirroring `SyncState`
- `FFISyncProgress` — progress tracking for FFI consumers
- `FFIString` — FFI-safe string with `ptr` and `length`
- `FFINetwork` — C-safe network enum
- `FFIWalletManager` — opaque handle to wallet manager

## Patterns

- **Opaque pointers**: Rust types exposed as `*mut FFIType` with matching `_free()` functions
- **Memory ownership**: Functions returning pointers transfer ownership to caller
- **Error propagation**: Thread-local error storage via `dash_spv_ffi_get_last_error()`
- **Callbacks**: Async callbacks via function pointers, bridged through tokio broadcast/watch channels
- **`#[no_mangle] extern "C"`**: All public FFI functions use this pattern

## FFI Safety Rules

- Every allocation must have a corresponding `_free()` function
- Never panic across FFI boundary — catch and convert to error codes
- Use `Arc<RwLock<T>>` for shared ownership across FFI
- Validate all pointer arguments for null before dereferencing
- CString allocation/deallocation with dedicated `_free_string()`

## Test Utilities

Uses `dash-spv` with `test-utils` feature as dev dependency.

- Integration tests in `tests/dashd_sync/` (requires real dashd node)
- Unit tests in `tests/unit/` (type conversions, error handling, configuration, lifecycle, memory)
- C tests in `tests/c_tests/`
