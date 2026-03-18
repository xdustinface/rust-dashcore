---
description: Before creating new functions, helpers, or types, search for existing code that can be extended
---

Before creating any new function, helper, type, or utility:

1. Search the current crate for similar existing code
2. Search dependent crates for feature-gated exports (e.g., `test-utils` feature)
3. Check the crate's `test_utils/` module if writing tests

Adding a parameter to an existing function is almost always better than creating a new one. If similar logic exists elsewhere, refactor to share code in a separate commit rather than duplicating it.

This applies to production code and test code equally. Many crates export test utilities behind `#[cfg(any(test, feature = "test-utils"))]` — use them.
