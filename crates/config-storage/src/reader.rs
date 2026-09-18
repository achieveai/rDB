//! Read access to applied state for the engine (ADR-0009).
//!
//! The engine calls `Raft::ensure_linearizable()` first, then reads through this handle. The
//! handle never goes through Raft itself; that is why reads are leader-linearizable only when
//! the engine gates them.

use config_core::KvState;
use openraft::{LogId, StoredMembership};

use crate::types::{RaftNode, RaftNodeId};

/// Synchronous, cheap access to the state machine's applied state.
pub trait StateReader: Send + Sync {
    /// Run `f` against the current applied [`KvState`] under the store's lock. `f` must be
    /// short (no I/O, no await); the lock is shared with the apply path.
    fn with_state(&self, f: &mut dyn FnMut(&KvState));

    /// Last applied log id, if any.
    fn last_applied(&self) -> Option<LogId<RaftNodeId>>;

    /// Committed membership as recorded by the state machine.
    fn membership(&self) -> StoredMembership<RaftNodeId, RaftNode>;

    /// Convenience: the public cluster revision.
    fn cluster_revision(&self) -> u64 {
        let mut rev = 0;
        self.with_state(&mut |s| rev = s.cluster_revision());
        rev
    }

    /// Convenience: the deterministic state hash (test plan TA-2).
    fn state_hash(&self) -> [u8; 32] {
        let mut h = [0u8; 32];
        self.with_state(&mut |s| h = s.state_hash());
        h
    }
}
