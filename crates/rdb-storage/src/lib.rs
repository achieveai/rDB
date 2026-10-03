//! rDB storage adapter (M8): the M1 storage seam on a real RocksDB engine.
//!
//! S0: write canonical batches, crash the process, reopen, and check what survived. S1: switch a
//! partition to a new generation that reads through to the old one (ADR-rdb-0010).
//!
//! - [`RocksEngine`] commits [`rdb_core::contracts::storage::Batch`]es atomically (WAL on,
//!   `sync=false`), syncs the WAL on request, and restores its watermarks on open.
//! - [`verify_lineage`] checks a stored history is a whole, digest-chained prefix.
//! - [`dump`] lists every stored record read-only, for debugging.
//! - [`RocksEngine::inherit`] links a new generation to its parent at a cutoff; [`RocksEngine::get`]
//!   reads through the chain.
//! - [`keys`] is the physical layout, [`keys::FORMAT_VERSION`] `1` (ADR-rdb-0010).
//!
//! Manual entry point: `cargo run -p rdb-storage --example rocks_scenario -- --help`. The
//! scenario walk-through is `crates/rdb-storage/README.md`.
//!
//! The arrow points one way: `rdb-storage -> rdb-core` (rdb ADR-0002). No `config-*` crate in
//! `[dependencies]`.

#![deny(missing_docs)]

mod engine;
mod inherit;
pub mod keys;
mod lineage;
mod snapshot;

pub use engine::{dump, InjectedFault, OpenError, RawRecord, RocksEngine};
pub use inherit::{ConflictReason, InheritError, Inherited, COPY_BATCH_KEYS};
pub use keys::Link;
pub use lineage::{verify_lineage, LineageFault, VerifiedLineage};
pub use snapshot::RocksSnapshot;
