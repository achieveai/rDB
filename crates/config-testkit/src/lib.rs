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
//! * [`cluster`] — [`cluster::Cluster`], an N-node rEtcd cluster over the **real** gRPC peer
//!   and client planes (test plan §4.1). This is what the M1 rows are written against.
//!
//! # Running the conformance suite over both clients
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use config_testkit::{cluster::{Cluster, StorageKind}, conformance, ConformanceConfig};
//! # async fn run() {
//! let cluster = Cluster::start(3, StorageKind::Ephemeral).await;
//! let leader = cluster.leader().await;
//!
//! let direct = conformance::run_all(cluster.client(leader), ConformanceConfig::unique("direct")).await;
//! let grpc = conformance::run_all(
//!     Arc::new(cluster.grpc_client_multi()),
//!     ConformanceConfig::unique("grpc"),
//! ).await;
//! assert!(direct.diff(&grpc).is_empty());
//! cluster.shutdown().await;
//! # }
//! ```
//!
//! [`memstore::MemStore`] remains the reference implementation that proves the suite, the poll
//! helpers, and the log assertions without needing a cluster at all.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod cluster;
pub mod conformance;
pub mod evidence;
pub mod fs;
pub mod logs;
pub mod manifest;
pub mod memstore;
pub mod poll;
pub mod ports;
pub mod scan;
pub mod tls;

pub use cluster::{
    AuthzKind, Cluster, ClusterBuilder, ClusterConfig, ClusterTls, GossipControl, GossipKind,
    NodeStartError, PoisonSpec, RocksSpec, StorageKind,
};
pub use conformance::{ConformanceConfig, ConformanceReport, ScenarioResult};
pub use manifest::{Manifest, ManifestFixture, ManifestPaths, Tamper, Voter};
pub use memstore::MemStore;
pub use poll::{election_timeout_multiple, poll_until, poll_until_async, TestTimers, Timeout};
pub use tls::{CertOverrides, CertPair, CertPaths, CertProfile, TlsFixture};
