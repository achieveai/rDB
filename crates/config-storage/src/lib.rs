//! rEtcd storage layer (ADR-0008): the OpenRaft `TypeConfig`, the fault-injection contract
//! consulted at every durability boundary (test plan TA-4), the `StateReader` handle the engine
//! uses for leader-linearizable reads, and the concrete stores.
//!
//! * `EphemeralStore` (M1): in-memory log + state machine, unmistakably `Durability::Ephemeral`.
//! * `RocksStore` (M2, format v2 at M4): five column families, one atomic synced `WriteBatch`
//!   per apply, carrying the KV change and its journal events together.
//!
//! The M4 event journal and the apply-side publish seam live in [`journal`]: one event per
//! mutation, written in the same synced batch as the mutation, so no crash can leave a watch
//! resuming across a revision that no longer has a record (ADR-0019).
//!
//! Both stores wrap [`config_core::KvState`] so apply semantics are identical and the
//! `state_hash` oracle is comparable across store kinds.
#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub mod ephemeral;
pub mod fault;
pub mod journal;
pub mod reader;
pub mod rocks;
pub mod snapshot;
pub mod trace;
pub mod types;
pub(crate) mod util;

pub use ephemeral::{EphemeralLog, EphemeralSm, EphemeralStore, NoSnapshots};
pub use fault::{Boundary, FaultAction, FaultCounters, FaultInjector, NoFaults};
pub use journal::{
    event_bytes, journal_hash, AppliedBatch, AppliedBatchSink, JournalStats, NoopSink,
};
pub use reader::{DedupStats, PinnedRead, PinnedView, StateReader, StorageReadError};
pub use rocks::{
    RocksLog, RocksOptions, RocksSm, RocksStore, StorageOpenError, CF_DEDUP, CF_EVENTS, CF_KV,
    CF_RAFT_LOG, CF_RAFT_META, CF_STATE_META, COLUMN_FAMILIES, FORMAT_VERSION,
};
pub use snapshot::{
    export_snapshot, is_snapshot_data_cf, restore_into_fresh_store, RestoreReport, SnapshotConfig,
    SnapshotConfigError, SnapshotFileError, SnapshotHeader, SnapshotReader, SnapshotRecord,
    SnapshotWriter, StorageMetrics, StoredSnapshot, RECV_EXT, SNAPSHOT_DIR, SNAP_EXT, TMP_EXT,
};
pub use trace::{fingerprint, CommandFingerprint, TraceRegistry, TRACE_REGISTRY_CAPACITY};
pub use types::{RaftNode, RaftNodeId, TypeConfig};
