//! Library surface for `codex-provider-proxyctl`.
//!
//! This exists primarily so integration tests (and other crates) can reuse
//! platform-specific helpers like the process scanner.

pub mod log_prune;
pub mod process_scan;
