//! The canonical versioned replicated command envelope (ADR-0007, spec §7.4, §17).
//!
//! [`Command`] is the only payload type that ever enters a Raft log entry. Its wire form is a
//! hand-written binary encoding rather than a serde format, because serde container ordering,
//! field skipping, and platform defaults are not part of any stable contract — and two voters
//! that decode a log entry differently diverge silently.
//!
//! ```text
//! magic "RCMD" (4) | version u16 LE (= 2)
//! op u8 (1 = Put, 2 = Delete, 3 = Compact, 4 = RetireNode)
//!
//! Put / Delete:
//!   key len u32 LE | key
//!   value len u32 LE | value          -- Put only; omitted entirely for Delete
//!   has_expected u8 (0 | 1) | expected_mod_revision u64 LE
//!   has_dedup u8 (0 | 1) | principal_hash (32) | client_id (16) | request_id u64 LE
//!                                      -- the 56 bytes only when has_dedup = 1
//!
//! Compact:
//!   up_to_revision u64 LE             -- no key, no value, no CAS guard
//!   has_trim u8 (0 | 1) | dedup_trim_below u64 LE -- the u64 only when has_trim = 1
//!
//! RetireNode:
//!   node_id u64 LE                    -- no key, no value, no CAS guard
//! ```
//!
//! # Version 2 (M4, ADR-0007 note of 2026-09-18, ADR-0019; extended at M5)
//!
//! Version 2 adds the `Compact` op and changes nothing about the `Put` / `Delete` layout apart
//! from the version field itself. The bump is nonetheless mandatory: a version-1 build decodes
//! only `op ∈ {1, 2}`, so a `Compact` entry replicated to it would be an
//! [`DecodeError::UnknownOp`] on that voter and an applied maintenance command on the others.
//! Refusing the whole envelope by version is the honest failure (spec §17 rolling-upgrade
//! rule): every voter must be upgraded before a v2 envelope is emitted.
//!
//! M5 extends version 2 rather than bumping it again (ADR-0025, ADR-0023): the dedup group,
//! `Compact`'s trim watermark, and `RetireNode` all arrive in the same milestone, behind the
//! same gate, so a single "every voter must be upgraded before a v2 envelope is emitted" rule
//! covers them. The additions are *not* backward compatible within version 2 — an M4 build
//! decoding an M5 `Put` fails [`DecodeError::Truncated`] on the dedup group rather than
//! misreading it — which is why they may only ship together with the M5 format bump.
//! `dedup_trim_below` is `Option` so the shape of a *logically M4-era* `Compact` is still
//! expressible (OQ-48), not so its M4 bytes still decode.
//!
//! The encoding is **canonical**: every command has exactly one valid byte string. Trailing
//! bytes, a `has_expected` byte outside `{0, 1}`, and a non-zero revision behind
//! `has_expected == 0` are all typed decode errors rather than tolerated slack, so
//! "identical command sequence" and "identical bytes" mean the same thing.
//!
//! [`Command`] also derives `serde::{Serialize, Deserialize}` because OpenRaft 0.9 requires
//! serde on its `D`/`R` types. The encoding above is the envelope *inside* a Raft entry's
//! payload, not the layout of a stored record: the M2 store writes `postcard(Entry<TypeConfig>)`
//! whose payload carries the command through that derive (`config-storage`'s `rocks.rs` module
//! docs; ADR-0008 note of 2026-09-18). [`Command::encode`] is the canonical form wherever
//! command *identity* is the question — determinism assertions, the replay oracle, and the
//! command fingerprint.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::identity::NodeId;
use crate::types::{DeleteRequest, MutationResponse, PutRequest, Record};

/// Envelope magic. Identifies a `config-core` command and cheaply rejects foreign bytes.
pub const COMMAND_MAGIC: [u8; 4] = *b"RCMD";

/// Envelope version. Bumped only for an incompatible layout or op-set change; every voter must
/// be upgraded before a new version is emitted (spec §17 rolling-upgrade rule).
///
/// `2` since M4, which added [`Command::Compact`].
pub const COMMAND_ENVELOPE_VERSION: u16 = 2;

/// Op byte for [`Command::Put`].
pub const OP_PUT: u8 = 1;
/// Op byte for [`Command::Delete`].
pub const OP_DELETE: u8 = 2;
/// Op byte for [`Command::Compact`] (envelope version 2 and later).
pub const OP_COMPACT: u8 = 3;
/// Op byte for [`Command::RetireNode`] (M5, ADR-0023).
pub const OP_RETIRE_NODE: u8 = 4;

/// Bytes the dedup group adds when it is **present**: `principal_hash`, `client_id`,
/// `request_id`, after the `has_dedup` flag (M5, ADR-0025, lead ruling M5-R17).
///
/// The group is variable-width: absent is the flag byte alone, not a flag followed by 56 zero
/// bytes. ADR-0025 says so in as many words, and the reason matters — a fixed group would add
/// 57 bytes to every mutation on a cluster that has dedup switched off, silently redefining
/// what `max_request_bytes` admits for deployments that asked for none of this.
const DEDUP_PRESENT_LEN: usize = 32 + 16 + 8;
/// Fixed envelope overhead for a `Put`: magic, version, op, key length, value length,
/// `has_expected`, the expected revision, and the dedup group's flag byte.
const PUT_OVERHEAD: usize = 4 + 2 + 1 + 4 + 4 + 1 + 8 + 1;
/// Fixed envelope overhead for a `Delete` (no value length field).
const DELETE_OVERHEAD: usize = 4 + 2 + 1 + 4 + 1 + 8 + 1;
/// Fixed envelope size for a `Compact`: magic, version, op, the watermark, and the optional
/// dedup trim watermark.
const COMPACT_BASE_LEN: usize = 4 + 2 + 1 + 8 + 1;
/// Fixed envelope size for a `RetireNode`: magic, version, op, and the node id.
const RETIRE_NODE_LEN: usize = 4 + 2 + 1 + 8;

/// A client's half of a deduplication key (M5, ADR-0025 "Envelope v2 dedup key").
///
/// Exactly 24 bytes. `client_id` is caller-chosen — typically a UUID minted once per
/// client process — and `request_id` is caller-assigned and must be strictly increasing per
/// `client_id` under one principal. It deliberately carries **no** principal: the identity a
/// dedup key is scoped to is the one the leader authenticated, not one a message can claim
/// (ADR-0012, ADR-0025 "Principal binding"). See [`DedupStamp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DedupKey {
    /// Caller-chosen client namespace.
    pub client_id: [u8; 16],
    /// Caller-assigned, strictly increasing per `(principal, client_id)`.
    pub request_id: u64,
}

impl DedupKey {
    /// A key for `request_id` under `client_id`.
    pub const fn new(client_id: [u8; 16], request_id: u64) -> Self {
        Self {
            client_id,
            request_id,
        }
    }

    /// Bind `principal_hash` to this key, producing the form that rides the envelope.
    pub const fn stamp(self, principal_hash: [u8; 32]) -> DedupStamp {
        DedupStamp {
            principal_hash,
            key: self,
        }
    }
}

/// SHA-256 of a principal's canonical name bytes — the `principal_hash` half of a
/// [`DedupStamp`] (ADR-0025).
///
/// One definition, because the leader that binds a stamp, the storage layer that keys the
/// `dedup` column family, and any test that predicts either must agree byte for byte. The
/// input is [`crate::Principal::name`] exactly as authenticated: the name is the identity
/// authorization is decided on, so deduplication is scoped to the same thing, and the digest
/// keeps a variable-length name out of every key of the column family.
pub fn principal_hash(name: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(name.as_bytes());
    hasher.finalize().into()
}

/// A [`DedupKey`] after the **leader** has bound the authenticated principal to it.
///
/// # Why the bound principal is in the envelope
///
/// ADR-0025 requires the effective dedup identity to be `(principal, client_id, request_id)`
/// with the principal bound by the leader "at propose time — never read from the message
/// itself". Both halves of that are satisfied here, and they have to be satisfied *together*:
///
/// * **Never from the message.** The leader overwrites this field unconditionally from the
///   authenticated session before proposing ([`crate::KvState::apply_with_principal`] is the
///   same bind expressed at apply time). A value a client put on the wire is discarded, so one
///   principal can never address another's `client_id` namespace.
/// * **In the envelope anyway.** Every voter applies the *same* committed entry and must reach
///   the *same* dedup decision. A follower has no authenticated session for an entry it
///   replicated, so a principal it could not read would make `apply` non-deterministic — the
///   one failure the envelope exists to prevent (spec §7.4). The bound hash therefore travels
///   with the command.
///
/// `principal_hash` is SHA-256 of the principal's canonical name bytes: fixed width, and it
/// keeps a variable-length principal name out of every key of the `dedup` column family
/// (ADR-0025 "State machine: `dedup` column family").
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DedupStamp {
    /// SHA-256 of the principal the leader authenticated.
    pub principal_hash: [u8; 32],
    /// The client's half of the key.
    pub key: DedupKey,
}

impl DedupStamp {
    /// The client namespace this stamp addresses.
    pub const fn client_id(&self) -> [u8; 16] {
        self.key.client_id
    }

    /// The request id this stamp addresses.
    pub const fn request_id(&self) -> u64 {
        self.key.request_id
    }

    /// Rebind this stamp to `principal_hash`, discarding whatever was there.
    pub const fn rebind(self, principal_hash: [u8; 32]) -> Self {
        Self {
            principal_hash,
            key: self.key,
        }
    }
}

/// The key reported for a [`Command::Compact`], which addresses no key.
///
/// [`Command::key`] returns a reference, and every caller (apply logging, the storage apply
/// path, the engine) only ever hexes it for a log field. An empty key is the honest answer and
/// keeps the accessor infallible; widening the return type to `Option` would churn five crates
/// to express the same thing.
static EMPTY_KEY: Bytes = Bytes::new();

/// A replicated state-machine command.
///
/// The `expected_mod_revision` guard is carried in the envelope rather than evaluated before
/// replication, because CAS must be decided against the state immediately preceding the
/// command in *committed apply order* — not against whatever the leader saw when it accepted
/// the request (spec §19.4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Write a value, optionally guarded by a compare-and-swap.
    Put {
        /// Opaque key bytes.
        key: Bytes,
        /// Opaque value bytes.
        value: Bytes,
        /// `None` unconditional, `Some(0)` create-only, `Some(n > 0)` conditional.
        expected_mod_revision: Option<u64>,
        /// Bounded deduplication key, bound to the authenticated principal by the leader
        /// (M5, ADR-0025). `None` opts out, which is the M0–M4 behaviour exactly.,
        dedup: Option<DedupStamp>,
    },
    /// Remove a key, optionally guarded by a compare-and-swap.
    Delete {
        /// Opaque key bytes.
        key: Bytes,
        /// `None` unconditional, `Some(n > 0)` conditional. `Some(0)` is invalid and is
        /// rejected by [`crate::validate_delete`] before encoding, and again at apply time if
        /// such an entry ever reaches the log.
        expected_mod_revision: Option<u64>,
        /// Bounded deduplication key, bound to the authenticated principal by the leader
        /// (M5, ADR-0025).,
        dedup: Option<DedupStamp>,
    },
    /// Drop retained journal events with `revision <= up_to_revision` (M4, ADR-0019).
    ///
    /// A **maintenance** command, not a state-changing mutation: it allocates no public
    /// revision, touches no record, and emits no [`MutationEvent`], so spec §19.3 is unaffected
    /// (ADR-0019 note of 2026-09-18, ruling R6). It is replicated rather than run node-locally
    /// so every voter's retained history — and therefore every voter's answer to "is this
    /// resume cursor still valid?" — is the same deterministic function of the applied log.
    ///
    /// Applying it is monotonic: a watermark at or below the current one is a no-op that still
    /// answers [`CommandResponse::Compacted`].
    Compact {
        /// Inclusive upper bound of the revisions whose events are dropped. Clamped to the
        /// current `cluster_revision` at apply time, so a watermark above applied state can
        /// never mark a future revision as compacted.
        up_to_revision: u64,
        /// Inclusive upper bound of the `DedupRecord::applied_revision` whose deduplication
        /// records are dropped (M5, ADR-0025; OQ-48 rides the existing command rather than
        /// adding a second retention command). `None` trims nothing, which is what an
        /// M4-era compaction means.,
        dedup_trim_below: Option<u64>,
    },
    /// Record that `node_id` has been removed from the cluster and may never rejoin (M5,
    /// ADR-0023, lead ruling M5-R5).
    ///
    /// Like [`Command::Compact`] this is a **maintenance** command: it allocates no public
    /// revision, touches no record, and emits no [`MutationEvent`]. It is replicated rather
    /// than kept node-locally because "stale identities cannot rejoin" (spec §21 M5) has to
    /// hold on every node that a retired node might dial, including one that was itself
    /// partitioned while the removal happened.
    ///
    /// Applying it is idempotent: retiring an already-retired id answers
    /// [`CommandResponse::Retired`] with the same id and changes nothing.
    RetireNode {
        /// The node id that is now permanently fenced out.
        node_id: NodeId,
    },
}

/// Why a byte string is not a valid [`Command`] envelope.
///
/// Decoding is total: it returns one of these or a `Command`, and never panics, hangs, or
/// allocates based on an unvalidated length field.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    /// The first four bytes are not [`COMMAND_MAGIC`].
    #[error("bad magic: expected {:?}, got {got:?}", COMMAND_MAGIC)]
    BadMagic {
        /// The four bytes actually present.
        got: [u8; 4],
    },
    /// The envelope version is not understood by this build (spec §17).
    #[error("unsupported command envelope version {0}")]
    UnsupportedVersion(u16),
    /// The op byte names no known operation.
    #[error("unknown command op {0}")]
    UnknownOp(u8),
    /// The buffer ended before a declared field was complete. Also covers a length field so
    /// large that it cannot be satisfied — the check happens before any allocation.
    #[error("truncated envelope: needed {needed} more bytes for {field}, {remaining} remain")]
    Truncated {
        /// Which field could not be read.
        field: &'static str,
        /// Bytes the field declared it needed.
        needed: usize,
        /// Bytes actually left in the buffer.
        remaining: usize,
    },
    /// The envelope decoded successfully but bytes remain. A canonical encoding admits no
    /// slack, so this is an error rather than something to ignore.
    #[error("{extra} trailing byte(s) after a complete envelope")]
    TrailingBytes {
        /// How many bytes were left over.
        extra: usize,
    },
    /// The `has_expected` discriminator byte was neither `0` nor `1`.
    #[error("invalid has_expected byte {0}, expected 0 or 1")]
    InvalidHasExpected(u8),
    /// `has_expected == 0` but the revision field was non-zero, which would give one logical
    /// command two encodings.
    #[error("non-canonical envelope: has_expected=0 but expected_mod_revision={0}")]
    NonCanonicalExpected(u64),
    /// The `has_dedup` discriminator byte was neither `0` nor `1` (M5).
    #[error("invalid has_dedup byte {0}, expected 0 or 1")]
    InvalidHasDedup(u8),
    /// `has_dedup == 0` but one of the dedup fields was non-zero, which would give one
    /// logical command many encodings.
    #[error("non-canonical envelope: has_dedup=0 but the {field} field is non-zero")]
    NonCanonicalDedup {
        /// Which of `principal_hash`, `client_id`, `request_id` was dirty.
        field: &'static str,
    },
    /// The `has_trim` discriminator byte was neither `0` nor `1` (M5).
    #[error("invalid has_trim byte {0}, expected 0 or 1")]
    InvalidHasTrim(u8),
    /// `has_trim == 0` but the trim watermark was non-zero.
    #[error("non-canonical envelope: has_trim=0 but dedup_trim_below={0}")]
    NonCanonicalTrim(u64),
}

/// A bounds-checked forward reader over the envelope bytes.
///
/// Every read is length-checked against what actually remains, so a hostile `key len` of
/// `u32::MAX` produces a [`DecodeError::Truncated`] instead of a 4 GiB allocation.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize, field: &'static str) -> Result<&'a [u8], DecodeError> {
        if self.remaining() < n {
            return Err(DecodeError::Truncated {
                field,
                needed: n,
                remaining: self.remaining(),
            });
        }
        let out = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, DecodeError> {
        Ok(self.take(1, field)?[0])
    }

    fn u16_le(&mut self, field: &'static str) -> Result<u16, DecodeError> {
        let b = self.take(2, field)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn u32_le(&mut self, field: &'static str) -> Result<u32, DecodeError> {
        let b = self.take(4, field)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64_le(&mut self, field: &'static str) -> Result<u64, DecodeError> {
        let b = self.take(8, field)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }

    /// Read a `u32 LE` length followed by that many bytes, checking the length against what
    /// remains **before** copying.
    fn length_prefixed(&mut self, field: &'static str) -> Result<Bytes, DecodeError> {
        let len = self.u32_le(field)? as usize;
        let body = self.take(len, field)?;
        Ok(Bytes::copy_from_slice(body))
    }
}

impl Command {
    /// The key this command addresses, or an empty key for [`Command::Compact`], which
    /// addresses none.
    pub fn key(&self) -> &Bytes {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
            Self::Compact { .. } | Self::RetireNode { .. } => &EMPTY_KEY,
        }
    }

    /// The CAS guard this command carries; `None` for the maintenance commands, which have
    /// none.
    pub fn expected_mod_revision(&self) -> Option<u64> {
        match self {
            Self::Put {
                expected_mod_revision,
                ..
            }
            | Self::Delete {
                expected_mod_revision,
                ..
            } => *expected_mod_revision,
            Self::Compact { .. } | Self::RetireNode { .. } => None,
        }
    }

    /// The leader-bound deduplication stamp this command carries, if any (M5, ADR-0025).
    pub fn dedup(&self) -> Option<DedupStamp> {
        match self {
            Self::Put { dedup, .. } | Self::Delete { dedup, .. } => *dedup,
            Self::Compact { .. } | Self::RetireNode { .. } => None,
        }
    }

    /// This command with `principal_hash` bound to its dedup stamp, if it carries one.
    ///
    /// The leader's bind seam: it is called with the hash of the principal the write RPC
    /// authenticated, so whatever the caller put on the wire is replaced rather than trusted
    /// (ADR-0025 "Principal binding"). A command with no dedup stamp is returned unchanged.
    pub fn bind_principal(self, principal_hash: [u8; 32]) -> Self {
        match self {
            Self::Put {
                key,
                value,
                expected_mod_revision,
                dedup,
            } => Self::Put {
                key,
                value,
                expected_mod_revision,
                dedup: dedup.map(|d| d.rebind(principal_hash)),
            },
            Self::Delete {
                key,
                expected_mod_revision,
                dedup,
            } => Self::Delete {
                key,
                expected_mod_revision,
                dedup: dedup.map(|d| d.rebind(principal_hash)),
            },
            other => other,
        }
    }

    /// Short operation name (`"put"` / `"delete"` / `"compact"` / `"retire_node"`), used as
    /// the `op` log field (ADR-0013).
    pub fn op_name(&self) -> &'static str {
        match self {
            Self::Put { .. } => "put",
            Self::Delete { .. } => "delete",
            Self::Compact { .. } => "compact",
            Self::RetireNode { .. } => "retire_node",
        }
    }

    /// Exact length [`Command::encode`] will produce, computed without encoding.
    ///
    /// This is the measure used for [`crate::Limits::max_request_bytes`], so the edge check
    /// and the bytes that actually enter the log agree by construction.
    pub fn encoded_len(&self) -> usize {
        match self {
            Self::Put {
                key, value, dedup, ..
            } => PUT_OVERHEAD + key.len() + value.len() + dedup_extra(*dedup),
            Self::Delete { key, dedup, .. } => DELETE_OVERHEAD + key.len() + dedup_extra(*dedup),
            Self::Compact {
                dedup_trim_below, ..
            } => COMPACT_BASE_LEN + if dedup_trim_below.is_some() { 8 } else { 0 },
            Self::RetireNode { .. } => RETIRE_NODE_LEN,
        }
    }

    /// Serialize to the canonical [`CommandV1`](self) bytes.
    ///
    /// The output depends only on the logical command, never on how the [`Bytes`] were
    /// constructed or on any ambient state, so the same command encodes identically on every
    /// voter and in every process.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        out.extend_from_slice(&COMMAND_MAGIC);
        out.extend_from_slice(&COMMAND_ENVELOPE_VERSION.to_le_bytes());
        match self {
            Self::Put {
                key,
                value,
                expected_mod_revision,
                dedup,
            } => {
                out.push(OP_PUT);
                out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                out.extend_from_slice(key);
                out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                out.extend_from_slice(value);
                push_expected(&mut out, *expected_mod_revision);
                push_dedup(&mut out, *dedup);
            }
            Self::Delete {
                key,
                expected_mod_revision,
                dedup,
            } => {
                out.push(OP_DELETE);
                out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                out.extend_from_slice(key);
                push_expected(&mut out, *expected_mod_revision);
                push_dedup(&mut out, *dedup);
            }
            Self::Compact {
                up_to_revision,
                dedup_trim_below,
            } => {
                out.push(OP_COMPACT);
                out.extend_from_slice(&up_to_revision.to_le_bytes());
                push_optional_u64(&mut out, *dedup_trim_below);
            }
            Self::RetireNode { node_id } => {
                out.push(OP_RETIRE_NODE);
                out.extend_from_slice(&node_id.0.to_le_bytes());
            }
        }
        debug_assert_eq!(out.len(), self.encoded_len());
        out
    }

    /// Parse canonical [`CommandV1`](self) bytes.
    ///
    /// Returns `Ok` only for a byte string that [`Command::encode`] could have produced.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        let mut r = Reader::new(buf);

        let magic = r.take(4, "magic")?;
        if magic != COMMAND_MAGIC {
            let mut got = [0u8; 4];
            got.copy_from_slice(magic);
            return Err(DecodeError::BadMagic { got });
        }

        let version = r.u16_le("version")?;
        if version != COMMAND_ENVELOPE_VERSION {
            return Err(DecodeError::UnsupportedVersion(version));
        }

        let op = r.u8("op")?;
        let cmd = match op {
            OP_PUT => {
                let key = r.length_prefixed("key")?;
                let value = r.length_prefixed("value")?;
                Self::Put {
                    key,
                    value,
                    expected_mod_revision: read_expected(&mut r)?,
                    dedup: read_dedup(&mut r)?,
                }
            }
            OP_DELETE => {
                let key = r.length_prefixed("key")?;
                Self::Delete {
                    key,
                    expected_mod_revision: read_expected(&mut r)?,
                    dedup: read_dedup(&mut r)?,
                }
            }
            OP_COMPACT => Self::Compact {
                up_to_revision: r.u64_le("up_to_revision")?,
                dedup_trim_below: read_trim(&mut r)?,
            },
            OP_RETIRE_NODE => Self::RetireNode {
                node_id: NodeId(r.u64_le("node_id")?),
            },
            other => return Err(DecodeError::UnknownOp(other)),
        };

        if r.remaining() != 0 {
            return Err(DecodeError::TrailingBytes {
                extra: r.remaining(),
            });
        }
        Ok(cmd)
    }
}

fn push_expected(out: &mut Vec<u8>, expected: Option<u64>) {
    match expected {
        Some(v) => {
            out.push(1);
            out.extend_from_slice(&v.to_le_bytes());
        }
        None => {
            out.push(0);
            out.extend_from_slice(&0u64.to_le_bytes());
        }
    }
}

/// Bytes the dedup group contributes beyond its flag byte.
const fn dedup_extra(dedup: Option<DedupStamp>) -> usize {
    match dedup {
        Some(_) => DEDUP_PRESENT_LEN,
        None => 0,
    }
}

/// Push the optional dedup group (M5, ADR-0025, lead ruling M5-R17).
///
/// Absent is the flag byte and nothing else — the shape ADR-0025 specifies, and the shape that
/// keeps a dedup-free `Put` one byte larger than its M4 self rather than fifty-seven. The
/// encoding stays canonical because the flag decides the length: there is no second byte
/// string for the same logical command, and a truncated envelope is caught by the reader
/// running out of input on a field it was told to read.
fn push_dedup(out: &mut Vec<u8>, dedup: Option<DedupStamp>) {
    match dedup {
        Some(stamp) => {
            out.push(1);
            out.extend_from_slice(&stamp.principal_hash);
            out.extend_from_slice(&stamp.key.client_id);
            out.extend_from_slice(&stamp.key.request_id.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn read_dedup(r: &mut Reader<'_>) -> Result<Option<DedupStamp>, DecodeError> {
    match r.u8("has_dedup")? {
        // Nothing follows the flag, so there is nothing to check for canonical form: an absent
        // dedup group has exactly one encoding by construction rather than by inspection.
        0 => Ok(None),
        1 => {
            let mut principal_hash = [0u8; 32];
            principal_hash.copy_from_slice(r.take(32, "principal_hash")?);
            let mut client_id = [0u8; 16];
            client_id.copy_from_slice(r.take(16, "client_id")?);
            let request_id = r.u64_le("request_id")?;
            Ok(Some(DedupStamp {
                principal_hash,
                key: DedupKey {
                    client_id,
                    request_id,
                },
            }))
        }
        other => Err(DecodeError::InvalidHasDedup(other)),
    }
}

/// Push [`Command::Compact`]'s optional trim watermark: the flag alone when absent.
///
/// Variable-width for the same reason the dedup group is (ADR-0025, lead ruling M5-R17): a
/// `Compact` that trims no deduplication records is the M4-era command, and it should not grow
/// eight bytes of zeroes to say so. Unlike `expected_mod_revision`, whose fixed width predates
/// M5 and is frozen by M0's golden bytes, this field is new and can be shaped correctly.
fn push_optional_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(v) => {
            out.push(1);
            out.extend_from_slice(&v.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn read_trim(r: &mut Reader<'_>) -> Result<Option<u64>, DecodeError> {
    match r.u8("has_trim")? {
        // Nothing follows the flag, so `NonCanonicalTrim` cannot arise: absent has one encoding
        // by construction. The variant is retained because a v2 envelope written by an earlier
        // M5 build is still refused by name rather than by a length error.
        0 => Ok(None),
        1 => Ok(Some(r.u64_le("dedup_trim_below")?)),
        other => Err(DecodeError::InvalidHasTrim(other)),
    }
}

fn read_expected(r: &mut Reader<'_>) -> Result<Option<u64>, DecodeError> {
    let has = r.u8("has_expected")?;
    let raw = r.u64_le("expected_mod_revision")?;
    match has {
        0 if raw != 0 => Err(DecodeError::NonCanonicalExpected(raw)),
        0 => Ok(None),
        1 => Ok(Some(raw)),
        other => Err(DecodeError::InvalidHasExpected(other)),
    }
}

impl From<&PutRequest> for Command {
    fn from(req: &PutRequest) -> Self {
        Self::Put {
            key: req.key.clone(),
            value: req.value.clone(),
            expected_mod_revision: req.expected_mod_revision,
            dedup: req.dedup.map(|k| k.stamp([0u8; 32])),
        }
    }
}

impl From<&DeleteRequest> for Command {
    fn from(req: &DeleteRequest) -> Self {
        Self::Delete {
            key: req.key.clone(),
            expected_mod_revision: req.expected_mod_revision,
            dedup: req.dedup.map(|k| k.stamp([0u8; 32])),
        }
    }
}

/// What an applied mutation did, in the form a future watch stream will replay (spec §7.2).
///
/// M0–M3 construct this deterministically inside apply but neither retain nor deliver it.
/// It exists now so that M4's event journal is a retention change rather than a change to
/// the state machine's semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationEvent {
    /// The public revision this mutation allocated.
    pub revision: u64,
    /// The key that changed.
    pub key: Bytes,
    /// What kind of change it was.
    pub kind: MutationEventKind,
}

/// The two shapes a [`MutationEvent`] can take.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationEventKind {
    /// The key now holds `value`.
    Put {
        /// The value written.
        value: Bytes,
        /// The record's `create_revision` after the write.
        create_revision: u64,
    },
    /// The key was removed. A tombstone carries no value.
    Delete,
}

impl MutationEvent {
    /// Build the event for an applied `Put`.
    pub fn put(revision: u64, record: &Record) -> Self {
        Self {
            revision,
            key: record.key.clone(),
            kind: MutationEventKind::Put {
                value: record.value.clone(),
                create_revision: record.create_revision,
            },
        }
    }

    /// Build the tombstone event for an applied `Delete`.
    pub fn delete(revision: u64, key: Bytes) -> Self {
        Self {
            revision,
            key,
            kind: MutationEventKind::Delete,
        }
    }
}

/// The state machine's reply to one replicated log entry.
///
/// This is OpenRaft's `R` type. Every log entry produces exactly one of these, which is why
/// [`CommandResponse::Noop`] and [`CommandResponse::Rejected`] exist alongside the ordinary
/// mutation reply: a state machine that could not answer for an entry would stall apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommandResponse {
    /// A [`Command`] was evaluated against committed state.
    Mutation {
        /// The client-visible outcome, revision, and CAS observation.
        response: MutationResponse,
        /// The deterministically constructed event. `Some` exactly when the outcome is
        /// [`crate::MutationOutcome::Applied`]; M0–M3 discard it. Always `None` on a
        /// deduplication hit: a duplicate "creates no second event" (spec §19.5).
        event: Option<MutationEvent>,
        /// Whether `response` was replayed from a retained deduplication record rather than
        /// produced by evaluating this entry (M5, ADR-0025). A hit allocates no revision.,
        dedup_hit: bool,
        /// Whether a deduplication record now retains this outcome, so the caller may safely
        /// resubmit the same `request_id` within the window.
        ///
        /// `false` when the command carried no dedup key, when dedup is not configured, and —
        /// the case that matters — when the global `max_records` cap was already reached
        /// (OQ-49): the mutation still applies normally, but nothing will recognize a
        /// resubmission, and the caller is told so rather than left to assume otherwise.,
        dedup_recorded: bool,
    },
    /// The entry decoded, but failed deterministic apply-time validation — for example a
    /// `Delete` with `expected_mod_revision == 0`, or a payload over the configured caps,
    /// that reached the log despite edge validation (ADR-0006).
    ///
    /// This is deliberately **outside** [`crate::MutationOutcome`]: reusing `Conflict` would
    /// corrupt CAS semantics for callers. It allocates no revision and changes no state, and
    /// the API edge maps it to [`crate::ConfigError::InvalidArgument`].
    Rejected {
        /// Why the entry was rejected. Names the violated rule; never contains a value.
        reason: String,
    },
    /// The entry carried no [`Command`] — an OpenRaft blank or membership entry.
    ///
    /// [`crate::KvState::apply`] never returns this; `config-storage` produces it directly
    /// for the non-command entry kinds, which allocate no public revision (ADR-0005).
    Noop,
    /// A [`Command::Compact`] was applied (M4, ADR-0019).
    ///
    /// Carries the watermark **after** applying, which is the unchanged current one when the
    /// command was a monotonic no-op. It is deliberately not a [`CommandResponse::Mutation`]:
    /// `Compact` allocates no revision and has no [`crate::MutationOutcome`], and squeezing it
    /// into one would make [`CommandResponse::is_applied`] lie about spec §19.3.
    Compacted {
        /// The retained-history watermark after this entry: every revision at or below it has
        /// had its journal event dropped.
        compact_revision: u64,
    },
    /// A [`Command::RetireNode`] was applied (M5, ADR-0023).
    ///
    /// Carries the id that is now retired, whether this entry is what retired it or an
    /// earlier one already had. Like [`CommandResponse::Compacted`] it is deliberately not a
    /// [`CommandResponse::Mutation`]: it allocates no revision and has no
    /// [`crate::MutationOutcome`].
    Retired {
        /// The permanently fenced-out node id.
        node_id: NodeId,
    },
}

impl CommandResponse {
    /// The client-visible response, or `None` for a rejected or non-command entry.
    pub fn mutation(&self) -> Option<&MutationResponse> {
        match self {
            Self::Mutation { response, .. } => Some(response),
            Self::Rejected { .. } | Self::Noop | Self::Compacted { .. } | Self::Retired { .. } => {
                None
            }
        }
    }

    /// The deterministically constructed event, when this entry changed state.
    pub fn event(&self) -> Option<&MutationEvent> {
        match self {
            Self::Mutation { event, .. } => event.as_ref(),
            Self::Rejected { .. } | Self::Noop | Self::Compacted { .. } | Self::Retired { .. } => {
                None
            }
        }
    }

    /// Whether this response was replayed from a retained deduplication record (M5).
    pub fn dedup_hit(&self) -> bool {
        matches!(
            self,
            Self::Mutation {
                dedup_hit: true,
                ..
            }
        )
    }

    /// Whether a deduplication record now retains this outcome (M5).
    pub fn dedup_recorded(&self) -> bool {
        matches!(
            self,
            Self::Mutation {
                dedup_recorded: true,
                ..
            }
        )
    }

    /// The watermark a [`CommandResponse::Compacted`] reports, if this is one.
    pub fn compact_revision(&self) -> Option<u64> {
        match self {
            Self::Compacted { compact_revision } => Some(*compact_revision),
            _ => None,
        }
    }

    /// The node id a [`CommandResponse::Retired`] reports, if this is one.
    pub fn retired_node(&self) -> Option<NodeId> {
        match self {
            Self::Retired { node_id } => Some(*node_id),
            _ => None,
        }
    }

    /// Whether this entry changed state and allocated a revision.
    pub fn is_applied(&self) -> bool {
        self.mutation().is_some_and(MutationResponse::is_applied)
    }
}
