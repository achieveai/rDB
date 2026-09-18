//! rEtcd storage layer (ADR-0008): the OpenRaft `TypeConfig`, the fault-injection contract
//! consulted at every durability boundary (test plan TA-4), the `StateReader` handle the engine
//! uses for leader-linearizable reads, and the concrete stores.
//!
//! * `EphemeralStore` (M1): in-memory log + state machine, unmistakably `Durability::Ephemeral`.
//! * `RocksStore` (M2): four column families, one atomic synced `WriteBatch` per apply.
//!
//! Both stores wrap [`config_core::KvState`] so apply semantics are identical and the
//! `state_hash` oracle is comparable across store kinds.
#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod ephemeral;
pub mod fault;
pub mod reader;
pub mod types;

pub use ephemeral::{EphemeralLog, EphemeralSm, EphemeralStore, NoSnapshots};
pub use fault::{Boundary, FaultAction, FaultCounters, FaultInjector, NoFaults};
pub use reader::StateReader;
pub use types::{RaftNodeId, TypeConfig};
