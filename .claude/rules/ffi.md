---
description: FFI boundary rules for dash-spv-ffi and key-wallet-ffi crates
paths: ["dash-spv-ffi/**", "key-wallet-ffi/**"]
---

## Memory safety

- Every `_create()` or allocation function must have a matching `_free()` function
- Never panic across FFI boundary — catch all panics and convert to error codes
- Validate all pointer arguments for null before dereferencing
- Use `Arc<RwLock<T>>` for shared ownership across FFI

## Type constraints

- All public types must be C-compatible (`#[repr(C)]` or opaque pointers)
- Use `#[no_mangle] extern "C"` for all public FFI functions
- Strings: return as `*const c_char` (caller frees), accept as `*const c_char` (borrowed)
- Error handling: thread-local storage pattern (`get_last_error()`)

## Callbacks

- Async callbacks use C function pointers bridged through tokio channels
- Callbacks may fire from any thread — document thread safety requirements
