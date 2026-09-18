//! Structured JSONL logging for rEtcd (ADR-0013).
//!
//! Every log line is one JSON object with the canonical render fields
//! `@t`, `@l`, `@m`, `@logger`, `application`, plus **all span fields flattened to the top
//! level** (root span first, leaf span last, so more specific spans override). That is what
//! makes DuckDB queries like `WHERE testMethod = '...' AND trace_id = '...'` possible.
//!
//! * [`init`] installs the global subscriber for a process (daemon or embedder).
//! * [`testing`] provides the per-test root span (`testModule`, `testMethod`, `testRun`) and
//!   the `#[retcd_test]` attribute re-exported from `config-log-macros`.
//! * [`context`] provides [`TraceContext`] (`trace_id`, `span_id`, `request_id`) and the
//!   header names used to propagate it across gRPC calls.
//!
//! Nothing here writes to stdout unless explicitly configured; console logs are not useful
//! for distributed debugging.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod context;
pub mod init;
pub mod layer;
pub mod testing;

pub use context::{TraceContext, HEADER_PARENT_SPAN, HEADER_REQUEST_ID, HEADER_TRACE_ID};
pub use init::{init, LogConfig, LogGuard, LogInitError};
pub use testing::retcd_test;

/// Re-export so downstream crates use one `tracing` version.
pub use tracing;
