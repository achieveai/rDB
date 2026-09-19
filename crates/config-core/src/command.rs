//! The canonical versioned replicated command envelope (ADR-0007, spec §7.4, §17).
//!
//! [`Command`] is the only payload type that ever enters a Raft log entry. Its wire form is a
//! hand-written binary encoding rather than a serde format, because serde container ordering,
//! field skipping, and platform defaults are not part of any stable contract — and two voters
//! that decode a log entry differently diverge silently.
//!
//! ```text
//! magic "RCMD" (4) | version u16 LE (= 1) | op u8 (1 = Put, 2 = Delete)
//! key len u32 LE | key
//! value len u32 LE | value          -- Put only; omitted entirely for Delete
//! has_expected u8 (0 | 1) | expected_mod_revision u64 LE
//! ```
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

use crate::types::{DeleteRequest, MutationResponse, PutRequest, Record};

/// Envelope magic. Identifies a `config-core` command and cheaply rejects foreign bytes.
pub const COMMAND_MAGIC: [u8; 4] = *b"RCMD";

/// Envelope version. Bumped only for an incompatible layout change; every voter must be
/// upgraded before a new version is emitted (spec §17 rolling-upgrade rule).
pub const COMMAND_VERSION: u16 = 1;

/// Op byte for [`Command::Put`].
pub const OP_PUT: u8 = 1;
/// Op byte for [`Command::Delete`].
pub const OP_DELETE: u8 = 2;

/// Fixed envelope overhead for a `Put`: magic, version, op, key length, value length,
/// `has_expected`, and the expected revision.
const PUT_OVERHEAD: usize = 4 + 2 + 1 + 4 + 4 + 1 + 8;
/// Fixed envelope overhead for a `Delete` (no value length field).
const DELETE_OVERHEAD: usize = 4 + 2 + 1 + 4 + 1 + 8;

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
    },
    /// Remove a key, optionally guarded by a compare-and-swap.
    Delete {
        /// Opaque key bytes.
        key: Bytes,
        /// `None` unconditional, `Some(n > 0)` conditional. `Some(0)` is invalid and is
        /// rejected by [`crate::validate_delete`] before encoding, and again at apply time if
        /// such an entry ever reaches the log.
        expected_mod_revision: Option<u64>,
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
    /// The key this command addresses, whichever variant it is.
    pub fn key(&self) -> &Bytes {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }

    /// The CAS guard this command carries, whichever variant it is.
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
        }
    }

    /// Short operation name (`"put"` / `"delete"`), used as the `op` log field (ADR-0013).
    pub fn op_name(&self) -> &'static str {
        match self {
            Self::Put { .. } => "put",
            Self::Delete { .. } => "delete",
        }
    }

    /// Exact length [`Command::encode`] will produce, computed without encoding.
    ///
    /// This is the measure used for [`crate::Limits::max_request_bytes`], so the edge check
    /// and the bytes that actually enter the log agree by construction.
    pub fn encoded_len(&self) -> usize {
        match self {
            Self::Put { key, value, .. } => PUT_OVERHEAD + key.len() + value.len(),
            Self::Delete { key, .. } => DELETE_OVERHEAD + key.len(),
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
        out.extend_from_slice(&COMMAND_VERSION.to_le_bytes());
        match self {
            Self::Put {
                key,
                value,
                expected_mod_revision,
            } => {
                out.push(OP_PUT);
                out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                out.extend_from_slice(key);
                out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                out.extend_from_slice(value);
                push_expected(&mut out, *expected_mod_revision);
            }
            Self::Delete {
                key,
                expected_mod_revision,
            } => {
                out.push(OP_DELETE);
                out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                out.extend_from_slice(key);
                push_expected(&mut out, *expected_mod_revision);
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
        if version != COMMAND_VERSION {
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
                }
            }
            OP_DELETE => {
                let key = r.length_prefixed("key")?;
                Self::Delete {
                    key,
                    expected_mod_revision: read_expected(&mut r)?,
                }
            }
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
        }
    }
}

impl From<&DeleteRequest> for Command {
    fn from(req: &DeleteRequest) -> Self {
        Self::Delete {
            key: req.key.clone(),
            expected_mod_revision: req.expected_mod_revision,
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
        /// [`crate::MutationOutcome::Applied`]; M0–M3 discard it.
        event: Option<MutationEvent>,
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
}

impl CommandResponse {
    /// The client-visible response, or `None` for a rejected or non-command entry.
    pub fn mutation(&self) -> Option<&MutationResponse> {
        match self {
            Self::Mutation { response, .. } => Some(response),
            Self::Rejected { .. } | Self::Noop => None,
        }
    }

    /// The deterministically constructed event, when this entry changed state.
    pub fn event(&self) -> Option<&MutationEvent> {
        match self {
            Self::Mutation { event, .. } => event.as_ref(),
            Self::Rejected { .. } | Self::Noop => None,
        }
    }

    /// Whether this entry changed state and allocated a revision.
    pub fn is_applied(&self) -> bool {
        self.mutation().is_some_and(MutationResponse::is_applied)
    }
}
