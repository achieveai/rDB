//! The deterministic in-memory KV state machine (M0, spec §7.4, ADR-0005, ADR-0006).
//!
//! [`KvState`] is pure and synchronous. It reads no clock, draws no entropy, consults no
//! ambient process state, performs no I/O, and iterates nothing unordered. Two replicas fed
//! the same command sequence therefore reach byte-identical state — which is the whole point,
//! since a replicated state machine that diverges is worse than one that fails.
//!
//! Everything node-local lives elsewhere: `last_applied`, membership, and storage identity
//! belong to `config-storage`, and are deliberately excluded from [`KvState::state_hash`] so
//! two nodes with the same applied prefix hash identically.

use std::collections::BTreeMap;

use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::command::{Command, CommandResponse, MutationEvent};
use crate::limits::Limits;
use crate::types::{GetResponse, ListRequest, ListResponse, MutationResponse, Record};
use crate::validate::{validate_command, validate_list};

/// Maximum key bytes rendered into the `key_hex` log field, giving 64 hex characters
/// (ADR-0013 redaction rule).
const KEY_HEX_MAX_BYTES: usize = 32;

/// The replicated key/value state.
///
/// Keys are ordered by unsigned bytewise lexical order via [`BTreeMap`], which is both the
/// order `List` must return and the order [`KvState::state_hash`] folds records in. Using an
/// ordered map is not an optimization here — an unordered one would make the hash
/// non-deterministic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvState {
    records: BTreeMap<Bytes, Record>,
    cluster_revision: u64,
    limits: Limits,
}

impl Default for KvState {
    fn default() -> Self {
        Self::new()
    }
}

impl KvState {
    /// An empty state machine at `cluster_revision = 0` with the spec §7.1 caps.
    pub fn new() -> Self {
        Self::with_limits(Limits::DEFAULT)
    }

    /// An empty state machine with explicit caps.
    ///
    /// Apply-time validation is part of the replicated state machine, so **every voter must
    /// be configured with identical limits**; otherwise a borderline command could be applied
    /// on one node and rejected on another. Tests use this to shrink caps to a few bytes
    /// instead of building megabyte payloads.
    pub fn with_limits(limits: Limits) -> Self {
        Self {
            records: BTreeMap::new(),
            cluster_revision: 0,
            limits,
        }
    }

    /// Rebuild a state machine from a restored snapshot or a storage scan.
    ///
    /// The caller is responsible for the pair being consistent: `revision` must be the
    /// `cluster_revision` those records were captured at. This is the seam `config-storage`
    /// uses on restart, and the seam a test uses to prove that a restored machine and a
    /// replayed one hash identically.
    ///
    /// `limits` must be the same caps the machine ran with before the restart; apply-time
    /// validation is replicated state, so restoring under different caps diverges the voter.
    pub fn from_parts(limits: Limits, revision: u64, records: BTreeMap<Bytes, Record>) -> Self {
        Self {
            records,
            cluster_revision: revision,
            limits,
        }
    }

    /// The caps this machine validates against.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// The public revision: `0` for an empty store, `+1` for each state-changing mutation.
    ///
    /// This is **not** the Raft log index. Blank, membership, rejected, and conflicting
    /// entries occupy log indexes without allocating a revision (ADR-0005).
    pub fn cluster_revision(&self) -> u64 {
        self.cluster_revision
    }

    /// Number of live records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the store holds no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Records in ascending unsigned bytewise key order.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Record)> {
        self.records.iter()
    }

    /// Look up one key.
    pub fn get(&self, key: &[u8]) -> Option<&Record> {
        self.records.get(key)
    }

    /// Look up one key and pair it with the revision the read observed.
    ///
    /// An absent key yields `record: None` with the revision still populated, so a caller can
    /// tell "absent as of revision N" from "the read did not happen" (spec §7.3).
    pub fn get_response(&self, key: &[u8]) -> GetResponse {
        GetResponse {
            record: self.get(key).cloned(),
            read_revision: self.cluster_revision,
        }
    }

    /// Bounded prefix scan in ascending unsigned bytewise key order (spec §10.2).
    ///
    /// This is read-only: it allocates no revision and changes no state, so
    /// [`KvState::state_hash`] is identical before and after any number of calls.
    ///
    /// The request is clamped through [`validate_list`] first. Records are accumulated until
    /// `max_items` or `max_bytes` would be exceeded, at which point `truncated` is set and
    /// the scan stops. One special case guarantees forward progress: if the *first* matching
    /// record alone exceeds `max_bytes`, it is returned anyway with `truncated = true`,
    /// because a zero-record truncated response tells the caller nothing it can act on.
    ///
    /// A prefix that fails validation yields an empty, untruncated response at the current
    /// revision; the API edge rejects such a request before ever reaching here.
    pub fn list(&self, req: &ListRequest) -> ListResponse {
        let Ok(req) = validate_list(req, &self.limits) else {
            return ListResponse {
                records: Vec::new(),
                read_revision: self.cluster_revision,
                truncated: false,
            };
        };

        let prefix = req.prefix.clone();
        let mut records: Vec<Record> = Vec::new();
        let mut used: u64 = 0;
        let mut truncated = false;

        // Start at the prefix and walk forward, stopping at the first key that no longer
        // carries it. Walking rather than computing an exclusive upper bound sidesteps the
        // all-0xFF prefix overflow entirely.
        for (key, record) in self.records.range(prefix.clone()..) {
            if !key.starts_with(&prefix) {
                break;
            }
            if records.len() as u64 >= u64::from(req.max_items) {
                truncated = true;
                break;
            }
            let cost = Limits::list_record_cost(key.len(), record.value.len());
            if records.is_empty() {
                records.push(record.clone());
                used = cost;
                if cost > req.max_bytes {
                    truncated = true;
                    break;
                }
            } else if used + cost > req.max_bytes {
                truncated = true;
                break;
            } else {
                used += cost;
                records.push(record.clone());
            }
        }

        ListResponse {
            records,
            read_revision: self.cluster_revision,
            truncated,
        }
    }

    /// Apply one replicated command.
    ///
    /// Total and infallible: every input, including a crafted or over-cap one, produces a
    /// [`CommandResponse`]. It never returns `Result` and never panics, because a state
    /// machine that can fail to answer for a committed entry stalls apply on that node and
    /// diverges it from its peers.
    ///
    /// Revision allocation is inside this function, so it happens exactly once per
    /// state-changing mutation in committed apply order (ADR-0005). Rejected, conflicting,
    /// and not-found commands allocate nothing.
    pub fn apply(&mut self, cmd: &Command) -> CommandResponse {
        // Invariant: `validate_list` can only fail on `prefix > max_key_bytes`, which matches
        // no legal key, so an empty page is the honest answer rather than an error.
        if let Err(err) = validate_command(cmd, &self.limits) {
            let reason = err.to_string();
            tracing::warn!(
                op = cmd.op_name(),
                key_hex = %key_hex(cmd.key()),
                outcome = "rejected",
                revision = self.cluster_revision,
                expected = ?cmd.expected_mod_revision(),
                reason = %reason,
                "rejected replicated command at apply time"
            );
            return CommandResponse::Rejected { reason };
        }
        // Reachable only through a corrupted persisted revision, never by 2^64 mutations.
        // Wrapping to 0 in release builds would silently diverge voters, so refuse instead.
        if self.cluster_revision == u64::MAX {
            let reason = "cluster_revision exhausted".to_string();
            tracing::error!(
                op = cmd.op_name(),
                key_hex = %key_hex(cmd.key()),
                outcome = "rejected",
                revision = self.cluster_revision,
                reason = %reason,
                "rejected replicated command: revision space exhausted"
            );
            return CommandResponse::Rejected { reason };
        }

        match cmd {
            Command::Put {
                key,
                value,
                expected_mod_revision,
            } => self.apply_put(key, value, *expected_mod_revision),
            Command::Delete {
                key,
                expected_mod_revision,
            } => self.apply_delete(key, *expected_mod_revision),
        }
    }

    fn apply_put(&mut self, key: &Bytes, value: &Bytes, expected: Option<u64>) -> CommandResponse {
        let current = self.records.get(key);
        // ADR-0006 rows 1-6, evaluated against the state immediately preceding this command.
        let conflict = match (expected, current) {
            (None, _) => None,
            (Some(0), None) => None,
            (Some(0), Some(rec)) => Some((true, rec.mod_revision)),
            (Some(n), Some(rec)) if rec.mod_revision == n => None,
            (Some(_), Some(rec)) => Some((true, rec.mod_revision)),
            // A conditional Put against an absent key conflicts with `exists = false`; it is
            // not a NotFound, because the caller asked to replace a specific revision.
            (Some(_), None) => Some((false, 0)),
        };

        if let Some((exists, current_mod_revision)) = conflict {
            return self.reject(
                "put",
                key,
                expected,
                MutationResponse::conflict(self.cluster_revision, exists, current_mod_revision),
            );
        }

        let revision = self.cluster_revision + 1;
        // `create_revision` survives updates but resets after a delete, because it names the
        // revision at which the key came into existence *this* time (ADR-0005).
        let create_revision = current.map_or(revision, |rec| rec.create_revision);
        let record = Record {
            key: key.clone(),
            value: value.clone(),
            create_revision,
            mod_revision: revision,
        };
        let event = MutationEvent::put(revision, &record);
        self.records.insert(key.clone(), record);
        self.cluster_revision = revision;

        tracing::debug!(
            op = "put",
            key_hex = %key_hex(key),
            outcome = "applied",
            revision,
            expected = ?expected,
            "applied put"
        );
        CommandResponse::Mutation {
            response: MutationResponse::applied_put(revision),
            event: Some(event),
        }
    }

    fn apply_delete(&mut self, key: &Bytes, expected: Option<u64>) -> CommandResponse {
        // ADR-0006 rows 7-12. An absent key is NOT_FOUND whether or not a positive expected
        // revision was supplied — the easy-to-get-wrong row, and the one that makes
        // "exactly one of N competing deletes applies" come out right.
        let Some(current) = self.records.get(key) else {
            return self.reject(
                "delete",
                key,
                expected,
                MutationResponse::not_found(self.cluster_revision),
            );
        };

        if let Some(n) = expected {
            if current.mod_revision != n {
                let current_mod_revision = current.mod_revision;
                return self.reject(
                    "delete",
                    key,
                    expected,
                    MutationResponse::conflict(self.cluster_revision, true, current_mod_revision),
                );
            }
        }

        let revision = self.cluster_revision + 1;
        self.records.remove(key);
        self.cluster_revision = revision;

        tracing::debug!(
            op = "delete",
            key_hex = %key_hex(key),
            outcome = "applied",
            revision,
            expected = ?expected,
            "applied delete"
        );
        CommandResponse::Mutation {
            response: MutationResponse::applied_delete(revision),
            event: Some(MutationEvent::delete(revision, key.clone())),
        }
    }

    /// Log and wrap a non-applying outcome. Allocates nothing and mutates nothing.
    fn reject(
        &self,
        op: &'static str,
        key: &Bytes,
        expected: Option<u64>,
        response: MutationResponse,
    ) -> CommandResponse {
        tracing::debug!(
            op,
            key_hex = %key_hex(key),
            outcome = ?response.outcome,
            revision = response.revision,
            expected = ?expected,
            current_mod_revision = response.current_mod_revision,
            "mutation did not change state"
        );
        CommandResponse::Mutation {
            response,
            event: None,
        }
    }

    /// Stable 32-byte digest of the replicated state (TA-2, ADR-0007 Clarifications).
    ///
    /// SHA-256 over, in this exact order:
    ///
    /// 1. `cluster_revision` as `u64` LE;
    /// 2. the record count as `u64` LE;
    /// 3. each record in ascending key order as
    ///    `key len u32 LE | key | value len u32 LE | value | create_revision u64 LE |
    ///    mod_revision u64 LE`.
    ///
    /// Every variable-length field is length-prefixed so no two distinct states can collide
    /// by concatenation (`{"ab" -> ""}` and `{"a" -> "b"}` must not hash alike). Node-local
    /// data — `last_applied`, membership, [`Limits`] — is excluded on purpose, so two nodes
    /// that applied the same prefix hash identically even though their log metadata differs.
    ///
    /// This ships unconditionally rather than behind a test feature: M2 compares nodes with
    /// it after replay, and a `cfg`-gated hash would be a different code path from the one
    /// under test.
    pub fn state_hash(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.cluster_revision.to_le_bytes());
        hasher.update((self.records.len() as u64).to_le_bytes());
        for (key, record) in &self.records {
            hasher.update((key.len() as u32).to_le_bytes());
            hasher.update(key);
            hasher.update((record.value.len() as u32).to_le_bytes());
            hasher.update(&record.value);
            hasher.update(record.create_revision.to_le_bytes());
            hasher.update(record.mod_revision.to_le_bytes());
        }
        hasher.finalize().into()
    }
}

/// Render a key as lowercase hex, truncated to [`KEY_HEX_MAX_BYTES`] so a log line can never
/// carry a full large key (ADR-0013). Values are never rendered at all.
fn key_hex(key: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(KEY_HEX_MAX_BYTES * 2);
    for byte in key.iter().take(KEY_HEX_MAX_BYTES) {
        out.push(DIGITS[usize::from(byte >> 4)] as char);
        out.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[config_log::retcd_test]
    fn key_hex_is_lowercase_and_capped() {
        assert_eq!(key_hex(b"\x00\xff\x0a"), "00ff0a");
        assert_eq!(key_hex(&[0xabu8; 64]).len(), 64);
    }
}
