//! Apply-time trace correlation (ADR-0013).
//!
//! ADR-0013 requires that one client write can be followed through leader → followers, which
//! means the `trace_id` of the client's operation has to reach the **state machine apply
//! line on every voter**. The replicated payload cannot carry it: [`config_core::Command`] is
//! a canonical, byte-exact envelope (ADR-0007) whose bytes are the determinism oracle, and
//! adding a per-request field to it would make two voters' "identical command sequence" depend
//! on a debugging field.
//!
//! So the trace travels beside the entry rather than inside it:
//!
//! 1. the leader records `fingerprint(command) → trace_id` here before `Raft::client_write`;
//! 2. the leader's Raft network stamps that trace on the `AppendEntries` envelope, which is
//!    the same cross-wire mechanism ADR-0013 already specifies for peer RPCs;
//! 3. the receiving node records `fingerprint(command) → trace_id` here for every command
//!    entry the envelope carried;
//! 4. `apply` looks the fingerprint up and adds `trace_id` to its line.
//!
//! The registry is advisory, bounded and lossy by construction — a miss costs a log field and
//! nothing else. Two properties are deliberate:
//!
//! * **Identical commands share a fingerprint.** Putting the same key and value twice yields
//!   one entry here, so the second write's trace overwrites the first's. The apply line for an
//!   older duplicate can therefore name a newer trace. It never names a *fabricated* one.
//! * **A batched `AppendEntries` propagates one trace.** When a single RPC carries several
//!   command entries that do not all belong to one client trace, the envelope carries the
//!   replication RPC's own trace instead, and that is what the apply lines show — still a true
//!   statement about why the entry was applied, just not the client's trace.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use config_core::Command;

/// How many `fingerprint → trace_id` pairs one node remembers.
///
/// A bound, not a tuning knob: the registry exists to label log lines, and an unbounded map
/// keyed by command content would be a memory leak on a busy node. Oldest entries are evicted
/// first; an evicted command's apply line simply carries no `trace_id`.
pub const TRACE_REGISTRY_CAPACITY: usize = 1024;

/// A 128-bit digest of a command's canonical bytes.
///
/// FNV-1a rather than SHA-2: this is a log-correlation key, not a security or determinism
/// boundary, and it must be cheap enough to compute on the apply path for every entry. It is
/// still fully deterministic — the same [`Command`] yields the same fingerprint in every
/// process and on every voter, which is what makes leader-side and follower-side lookups agree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CommandFingerprint(u128);

impl CommandFingerprint {
    /// The digest as a `u128`, for tests and diagnostics.
    pub const fn as_u128(self) -> u128 {
        self.0
    }
}

const FNV_OFFSET: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const FNV_PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

/// Fingerprint a command by its canonical [`Command::encode`] bytes.
pub fn fingerprint(cmd: &Command) -> CommandFingerprint {
    let mut hash = FNV_OFFSET;
    for byte in cmd.encode() {
        hash ^= u128::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    CommandFingerprint(hash)
}

#[derive(Debug, Default)]
struct Inner {
    map: HashMap<CommandFingerprint, String>,
    /// Insertion order, for eviction. Holds one entry per *new* key.
    order: VecDeque<CommandFingerprint>,
}

/// One node's bounded `fingerprint → trace_id` side table.
///
/// Created by the store (so the apply path can read it without a new constructor argument) and
/// written by the engine. Cloning the store shares the registry, which is the point: the log
/// half, the state-machine half and the engine all see the same table.
#[derive(Debug, Default)]
pub struct TraceRegistry {
    inner: Mutex<Inner>,
}

impl TraceRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Remember that `cmd` belongs to `trace_id`.
    ///
    /// Ignores an empty `trace_id` — recording one would make `apply` claim a trace that does
    /// not exist, which is worse than recording nothing.
    pub fn record(&self, cmd: &Command, trace_id: &str) {
        if trace_id.is_empty() {
            return;
        }
        let fp = fingerprint(cmd);
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if inner.map.insert(fp, trace_id.to_string()).is_none() {
            inner.order.push_back(fp);
            while inner.order.len() > TRACE_REGISTRY_CAPACITY {
                if let Some(evicted) = inner.order.pop_front() {
                    inner.map.remove(&evicted);
                }
            }
        }
    }

    /// The trace id recorded for `cmd`, if it is still remembered.
    ///
    /// Non-consuming: the leader replicates an entry to several followers and applies it
    /// itself, so the same fingerprint is looked up more than once.
    pub fn lookup(&self, cmd: &Command) -> Option<String> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.map.get(&fingerprint(cmd)).cloned()
    }

    /// How many pairs are currently remembered.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map
            .len()
    }

    /// Whether nothing is remembered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn put(key: &str, value: &str) -> Command {
        Command::Put {
            key: Bytes::from(key.to_string()),
            value: Bytes::from(value.to_string()),
            expected_mod_revision: None,
            dedup: None,
        }
    }

    #[test]
    fn fingerprint_is_deterministic_and_discriminating() {
        assert_eq!(fingerprint(&put("/a", "1")), fingerprint(&put("/a", "1")));
        assert_ne!(fingerprint(&put("/a", "1")), fingerprint(&put("/a", "2")));
        assert_ne!(fingerprint(&put("/a", "1")), fingerprint(&put("/b", "1")));
        assert_ne!(
            fingerprint(&put("/a", "1")),
            fingerprint(&Command::Delete {
                key: Bytes::from_static(b"/a"),
                expected_mod_revision: None,
                dedup: None,
            })
        );
    }

    #[test]
    fn record_then_lookup_round_trips_and_ignores_empty() {
        let reg = TraceRegistry::new();
        assert!(reg.is_empty());
        reg.record(&put("/a", "1"), "");
        assert!(reg.is_empty(), "an empty trace id must not be recorded");

        reg.record(&put("/a", "1"), "0123456789abcdef0123456789abcdef");
        assert_eq!(
            reg.lookup(&put("/a", "1")).as_deref(),
            Some("0123456789abcdef0123456789abcdef")
        );
        // Non-consuming: the leader looks the same entry up once per follower plus once for
        // its own apply.
        assert!(reg.lookup(&put("/a", "1")).is_some());
        assert_eq!(reg.lookup(&put("/a", "2")), None);
    }

    #[test]
    fn capacity_is_bounded_and_evicts_oldest_first() {
        let reg = TraceRegistry::new();
        for i in 0..(TRACE_REGISTRY_CAPACITY + 10) {
            reg.record(&put(&format!("/k{i}"), "v"), &format!("trace{i}"));
        }
        assert_eq!(reg.len(), TRACE_REGISTRY_CAPACITY);
        assert_eq!(reg.lookup(&put("/k0", "v")), None, "oldest must be evicted");
        let newest = TRACE_REGISTRY_CAPACITY + 9;
        assert_eq!(
            reg.lookup(&put(&format!("/k{newest}"), "v")).as_deref(),
            Some(format!("trace{newest}").as_str())
        );
    }

    #[test]
    fn re_recording_the_same_command_overwrites_without_growing() {
        let reg = TraceRegistry::new();
        reg.record(&put("/a", "1"), "first");
        reg.record(&put("/a", "1"), "second");
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.lookup(&put("/a", "1")).as_deref(), Some("second"));
    }
}
