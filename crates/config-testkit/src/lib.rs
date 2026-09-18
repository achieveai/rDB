//! Engine-independent test toolkit for rEtcd (ADR-0014; test plan TA-8, TA-10, TA-11).
//!
//! This crate holds the part of the M1 test harness that does not need a running cluster:
//!
//! * [`conformance`] — one scenario list (C-01..C-15) run against any `Arc<dyn ConfigStore>`
//!   (TA-10), so `DirectClient` and `GrpcClient` are proven to agree without duplicating the
//!   assertions.
//! * [`memstore`] — [`memstore::MemStore`], an in-memory reference [`config_core::ConfigStore`]
//!   over [`config_core::KvState`] with no Raft. It exists to test the conformance suite
//!   itself and to give other crates' tests "some `ConfigStore`" without a cluster.
//! * [`poll`] — deadline-bounded polling (`poll_until`, `poll_until_async`) and
//!   [`poll::TestTimers`], so waits are expressed as multiples of an election timeout rather
//!   than a literal duration (anti-flake rules 2-3).
//! * [`logs`] — DuckDB-backed assertions over `target/test-logs/**/*.jsonl` (spec §5).
//! * [`ports`] / [`fs`] — ephemeral listeners and per-test temp directories (TA-11, anti-flake
//!   rules 4-5).
//! * [`scan`] — source scanners enforcing anti-flake rules 1 (no fixed sleeps) and 4 (no
//!   literal ports).
//!
//! # What is not here yet
//!
//! The `Cluster` harness (test plan §4.1) spins up a real N-node in-process cluster over
//! `config-engine` and lands once the engine does. Nothing in this crate needs to change to
//! accept it: `Cluster` only has to produce an `Arc<dyn config_core::ConfigStore>`.
//!
//! ```ignore
//! impl Cluster {
//!     pub fn client(&self, id: NodeId) -> Arc<dyn ConfigStore>;    // -> conformance::run_all
//!     pub async fn grpc_client_multi(&self) -> GrpcClient;         // -> conformance::run_all
//! }
//!
//! let report_direct = conformance::run_all(cluster.client(leader), cfg.clone()).await;
//! let report_grpc = conformance::run_all(
//!     Arc::new(cluster.grpc_client_multi().await),
//!     cfg,
//! ).await;
//! assert!(report_direct.diff(&report_grpc).is_empty());
//! ```
//!
//! [`memstore::MemStore`] is the reference implementation that proves the suite, the poll
//! helpers, and the log assertions all work before `Cluster` exists — the extension point is
//! "produce an `Arc<dyn ConfigStore>`", nothing more.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod conformance;
pub mod fs;
pub mod logs;
pub mod memstore;
pub mod poll;
pub mod ports;
pub mod scan;

pub use conformance::{ConformanceConfig, ConformanceReport, ScenarioResult};
pub use memstore::MemStore;
pub use poll::{election_timeout_multiple, poll_until, poll_until_async, TestTimers, Timeout};
