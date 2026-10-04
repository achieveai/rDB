//! Compile document ops to one whole-document `Put`, with its precondition attached
//! (ADR-rdb-0012 decision 11).
//!
//! read the record → [`open`] the envelope (digest check) → strict [`decode`] →
//! [`materialize`] → [`encode`] (refuses too deep or too large) → [`seal`]. So an accepted write
//! always reads back.

use bytes::Bytes;
use rdb_core::{Condition, Mutation, Namespace, SnapshotRead};

use crate::cbor::{decode, encode, CodecError, EncodeError};
use crate::delta::{materialize, ApplyError, Delta};
use crate::envelope::{open, seal, EnvelopeError, Kind, OversizedPayload};
use crate::value::Value;

/// What the caller expects to find at the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    /// No record: a create. The kernel re-checks it at apply through
    /// [`Condition::Absent`], so of two racing creates the second fails.
    Absent,
    /// A record at exactly this version. The kernel re-checks it at apply through
    /// `expected_version`.
    Version(u64),
}

/// One whole-document `Put`, and the condition that must hold when it applies. Returned
/// together so a caller cannot drop the create-if-absent condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compiled {
    /// The `Put`: the key, the sealed after-image, and `expected_version`.
    pub mutation: Mutation,
    /// `Some(Absent { key })` for [`Expected::Absent`]; `None` for [`Expected::Version`],
    /// whose check rides on the mutation's `expected_version`.
    pub condition: Option<Condition>,
}

/// A stored document, read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Document {
    /// The storage record's version: the `seq` of the transaction that wrote it.
    pub version: u64,
    /// The decoded document.
    pub value: Value,
}

/// Stored bytes that do not read back. Kept apart from a client error, so a damaged record never
/// reads as bad input. Nothing is repaired.
///
/// Not every cause is damage. [`EnvelopeError`] separates a record **written by a newer build**
/// (an unknown format, codec or digest, or a reserved kind) from a damaged one. Mixed-version
/// windows are supported, so the first can be a valid record this build cannot read. The M9 admin
/// path must not offer a delete or replace for it (ADR-rdb-0012 §12).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Corrupt {
    /// The envelope does not open: damaged, or written by a newer build (see [`EnvelopeError`]).
    #[error("{0}")]
    Envelope(EnvelopeError),
    /// The envelope is sound but the payload is not canonical CBOR.
    #[error("{0}")]
    Codec(CodecError),
    /// The snapshot has a value but no version, or a version but no value, which
    /// `SnapshotRead` promises never happens.
    #[error("the snapshot holds a value without a version, or a version without a value")]
    VersionWithoutValue,
}

/// Why a read or a compile is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ValueError {
    /// The ops cannot be applied, or the result cannot be written.
    #[error("{0}")]
    Apply(ApplyError),
    /// The stored record is damaged.
    #[error("corrupt record: {0}")]
    Corrupt(Corrupt),
}

impl From<ApplyError> for ValueError {
    fn from(e: ApplyError) -> Self {
        Self::Apply(e)
    }
}

impl From<EncodeError> for ApplyError {
    fn from(e: EncodeError) -> Self {
        match e {
            EncodeError::TooDeep => Self::TooDeep,
            EncodeError::TooLarge => Self::TooLarge,
        }
    }
}

impl From<OversizedPayload> for ApplyError {
    fn from(_: OversizedPayload) -> Self {
        Self::TooLarge
    }
}

/// The document at `key` in [`Namespace::User`], or `None` when there is no record.
///
/// # Errors
/// [`ValueError::Corrupt`] when the record does not open or decode.
pub fn read(snapshot: &dyn SnapshotRead, key: &[u8]) -> Result<Option<Document>, ValueError> {
    let stored = snapshot.get(Namespace::User, key);
    let version = snapshot.version(Namespace::User, key);
    match (stored, version) {
        (None, None) => Ok(None),
        (Some(bytes), Some(version)) => {
            let opened = open(&bytes).map_err(|e| ValueError::Corrupt(Corrupt::Envelope(e)))?;
            let value =
                decode(opened.payload).map_err(|e| ValueError::Corrupt(Corrupt::Codec(e)))?;
            Ok(Some(Document { version, value }))
        }
        _ => Err(ValueError::Corrupt(Corrupt::VersionWithoutValue)),
    }
}

/// Compile `delta` against the document at `key` into one `Put` and its precondition.
///
/// [`Expected::Version`] is checked here first, before any work, and again by the kernel at
/// apply. [`Expected::Absent`] does not read the record at all: the delta applies to an absent
/// base, and [`Condition::Absent`] makes the kernel refuse it if a record exists by then.
///
/// # Errors
/// [`ApplyError::ObjectAbsent`] for [`Expected::Version`] on a missing record;
/// [`ApplyError::VersionConflict`]; any [`ApplyError`] from the ops or the encoder; or
/// [`ValueError::Corrupt`] when the stored record does not read back.
pub fn compile(
    snapshot: &dyn SnapshotRead,
    key: &[u8],
    expected: Expected,
    delta: &Delta,
) -> Result<Compiled, ValueError> {
    let base = match expected {
        Expected::Absent => None,
        Expected::Version(want) => {
            match snapshot.version(Namespace::User, key) {
                None => return Err(ApplyError::ObjectAbsent.into()),
                Some(found) if found != want => {
                    return Err(ApplyError::VersionConflict {
                        expected: want,
                        found,
                    }
                    .into())
                }
                Some(_) => {}
            }
            let document = read(snapshot, key)?.ok_or(ApplyError::ObjectAbsent)?;
            Some(document.value)
        }
    };
    let after = materialize(base, delta)?;
    let payload = encode(&after).map_err(ApplyError::from)?;
    let value = seal(Kind::Document, &payload).map_err(ApplyError::from)?;
    let key = Bytes::copy_from_slice(key);
    let (expected_version, condition) = match expected {
        Expected::Absent => (None, Some(Condition::Absent { key: key.clone() })),
        Expected::Version(v) => (Some(v), None),
    };
    Ok(Compiled {
        mutation: Mutation::Put {
            key,
            value,
            expected_version,
        },
        condition,
    })
}
