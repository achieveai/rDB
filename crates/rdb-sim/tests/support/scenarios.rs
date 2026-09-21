//! Scenario grammar, generator, reducer and coverage. **Owned by team verification, packages G1
//! and Q1.**
//!
//! Registered in [`super`] by team foundation so the seam exists on day one. This file is the
//! scenarios module's own root: Rust forbids a crate having both `scenarios.rs` and
//! `scenarios/mod.rs`, so the registered file **is** the module and the submodules hang off it.
//!
//! | Module | What it owns |
//! |---|---|
//! | [`grammar`] | the six op enums, [`grammar::Scenario`], [`grammar::Budget`] — plain data, serialized as the fixture (decision D4) |
//! | [`gen`] | the seeded generator and the `BoundaryId -> ScenarioOp` producer table |
//! | [`reduce`] | ddmin over the op list, deletion only (decision D3) |
//! | [`coverage`] | the required lists, the family map and the capability gating table |
//! | [`mutate`] | the three trace rewrites that test the oracle itself (decision D5) |
//! | [`builder`] | `TraceBuilder`, the hand-built fixture writer (VA-1) |
//!
//! `builder` lives here rather than under `oracle/` on purpose: it *writes* what the oracle
//! judges, and it needs `contracts::event::Budgets` and `contracts::version` to fill a trace
//! header — two paths row **M7V-01**'s allowlist has no reason to admit into the judge itself.
//!
//! The generator emits this grammar, and the runner lowers it onto the provider API —
//! [`rdb_sim::sim::network::NetworkOp`], [`rdb_sim::storage::StorageOp`] and
//! [`rdb_sim::sim::control::ControlOp`]. Two layers, not two copies: a scenario says "partition
//! these two sets" and "crash this node before its flush", while a provider op says `SetLink` and
//! `Fail`. Topology breadth, including how many partitions a scenario runs, is decided here —
//! [`rdb_sim::sim::cluster::ClusterConfig`] holds a `Vec<PartitionSpec>` and fixes nothing.

pub mod builder;
pub mod coverage;
pub mod gen;
pub mod grammar;
pub mod mutate;
pub mod reduce;
