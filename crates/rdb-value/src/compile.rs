//! Compile document ops to one whole-document `Put`, with its precondition attached
//! (ADR-rdb-0012 decision 11). A document lives at its object's root key (ADR-rdb-0013 §1).
//!
//! read the record → [`open`] the envelope (digest check) → strict [`decode`] →
//! [`materialize`] → [`encode`] (refuses too deep or too large) → [`seal`]. So an accepted write
//! always reads back.

use bytes::Bytes;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::record_len;
use rdb_core::{Condition, Mutation, Namespace, SnapshotRead};

use crate::cbor::{decode, encode, CodecError, EncodeError};
use crate::delta::{materialize, ApplyError, Delta, SizeLimit};
use crate::envelope::{open, seal, EnvelopeError, Kind, OversizedPayload};
use crate::keys::{KeyError, RootKey};
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

/// The writes of one compiled delta, and the conditions that must hold when they apply. Returned
/// together so a caller cannot drop the create-if-absent condition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compiled {
    /// The writes, one per touched key, in key order. A document compile returns exactly one
    /// `Put`: the key, the sealed after-image, and `expected_version`. A collection compile
    /// returns its root `Put` first, then one write per element that changed
    /// (ADR-rdb-0013 §9).
    pub mutations: Vec<Mutation>,
    /// `[Absent { key }]` on the root key for [`Expected::Absent`]; empty for
    /// [`Expected::Version`], whose check rides on the root write's `expected_version`. A list,
    /// in the kernel request's order, so a write guarded by several keys fits (lead ruling
    /// L-R186j).
    pub conditions: Vec<Condition>,
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
    /// An object record's key does not decode (ADR-rdb-0013 §4).
    #[error("key: {0}")]
    Key(KeyError),
    /// A collection root's payload is not exactly `{"keys": n, "count": n}`, or its count plus
    /// the elements a write adds is over `u64::MAX`, so the stored count cannot be right.
    #[error("collection root: {0}")]
    Root(&'static str),
    /// A list root's payload breaks a rule of ADR-rdb-0016 §3, or a counter it or a node holds
    /// would overflow. The message names the list.
    #[error("{0}")]
    ListRoot(&'static str),
    /// A collection root names an element key profile this build does not know. Written by a
    /// newer build, not damage (ADR-rdb-0012 §12).
    #[error("unknown element key profile {0}")]
    UnknownKeyProfile(i128),
    /// A map entry's envelope is not a document.
    #[error("map entry envelope is a {found:?}, not a Document")]
    EntryNotDocument {
        /// The entry's envelope kind.
        found: Kind,
    },
    /// A list item's envelope is not a document.
    #[error("list item envelope is a {found:?}, not a Document")]
    ItemNotDocument {
        /// The item's envelope kind.
        found: Kind,
    },
    /// A set member's record holds bytes; it must be empty.
    #[error("set member record holds {len} bytes")]
    SetMemberHasValue {
        /// The bytes it holds.
        len: usize,
    },
    /// An element's storage version is above its root's. Every element write also writes the
    /// root, so this never happens through `rdb-value` (ADR-rdb-0013 §10).
    #[error("element version {element} is above its root's version {root}")]
    ElementNewerThanRoot {
        /// The element's version.
        element: u64,
        /// The root's version.
        root: u64,
    },
    /// Element records exist under an object with no root, or a root whose `count` is 0
    /// (ADR-rdb-0013 §11).
    #[error("element records exist that the root does not account for")]
    OrphanElement,
    /// The root's `count` is above 0 but no element record exists (ADR-rdb-0013 §11).
    #[error("the root counts {count} elements but none exists")]
    CountMismatch {
        /// The root's count.
        count: u64,
    },
    /// A blob root's payload is not a v1 manifest (ADR-rdb-0014 §2, "strict reading").
    #[error("blob manifest: {0}")]
    Manifest(ManifestError),
    /// A chunk record does not open (ADR-rdb-0014 §6 check 3, §7).
    #[error("chunk {index}: {error}")]
    Chunk {
        /// The chunk's index.
        index: u32,
        /// Why it does not open.
        error: EnvelopeError,
    },
    /// A published manifest names a chunk that is not stored (ADR-rdb-0014 §7).
    #[error("the manifest names chunk {index}, which is not stored")]
    ChunkMissing {
        /// The missing index.
        index: u32,
    },
    /// A list page that a read or a compile opened is damaged: missing, not a page, or not the
    /// node its parent names (ADR-rdb-0016 §3, §7).
    #[error("list page {id:032x}: {fault}")]
    Page {
        /// The page's id.
        id: u128,
        /// What is wrong with it.
        fault: PageFault,
    },
    /// A list leaf names an item that has no record (ADR-rdb-0016 §7).
    #[error("the list names item {id:032x}, which is not stored")]
    ItemMissing {
        /// The item's id.
        id: u128,
    },
    /// A stored chunk is not the one the manifest names: another kind of record, or another
    /// length or digest (ADR-rdb-0014 §7).
    #[error("chunk {index} is not the chunk the manifest names")]
    ChunkMismatch {
        /// The chunk's index.
        index: u32,
    },
}

/// Why a list page is refused (ADR-rdb-0016 §7). An unknown kind, codec or format inside
/// [`PageFault::Envelope`] stays written-by-a-newer-build (ADR-rdb-0012 §12).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PageFault {
    /// Its parent names it, but no record is stored.
    #[error("not stored")]
    Missing,
    /// The envelope does not open.
    #[error("{0}")]
    Envelope(EnvelopeError),
    /// The record is another kind.
    #[error("a {found:?} record, not a list page")]
    NotAPage {
        /// The kind found.
        found: Kind,
    },
    /// The payload is not canonical CBOR.
    #[error("{0}")]
    Codec(CodecError),
    /// The payload decodes but is not the node its parent names.
    #[error("{0}")]
    Shape(&'static str),
    /// The page's version is above its root's.
    #[error("page version {page} is above its root's version {root}")]
    NewerThanRoot {
        /// The page's version.
        page: u64,
        /// The root's version.
        root: u64,
    },
}

/// Why a blob root's payload is not a v1 manifest (ADR-rdb-0014 §2).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    /// The payload is not canonical CBOR.
    #[error("{0}")]
    Codec(CodecError),
    /// The payload decodes but is not exactly the five fields, with their types and limits.
    #[error("{0}")]
    Shape(&'static str),
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
            EncodeError::TooLarge => Self::TooLarge {
                limit: SizeLimit::Value,
            },
        }
    }
}

impl From<OversizedPayload> for ApplyError {
    fn from(_: OversizedPayload) -> Self {
        Self::TooLarge {
            limit: SizeLimit::Value,
        }
    }
}

/// The record at `key` with its version, or `None` when there is none.
pub(crate) fn record(
    snapshot: &dyn SnapshotRead,
    key: &[u8],
) -> Result<Option<(u64, Bytes)>, ValueError> {
    match (
        snapshot.get(Namespace::User, key),
        snapshot.version(Namespace::User, key),
    ) {
        (None, None) => Ok(None),
        (Some(bytes), Some(version)) => Ok(Some((version, bytes))),
        _ => Err(ValueError::Corrupt(Corrupt::VersionWithoutValue)),
    }
}

/// The document at `root` in [`Namespace::User`], or `None` when there is no record.
///
/// # Errors
/// [`ApplyError::KindMismatch`] when the object is a map or a set; [`ValueError::Corrupt`] when
/// the record does not open or decode.
pub fn read(snapshot: &dyn SnapshotRead, root: &RootKey) -> Result<Option<Document>, ValueError> {
    let Some((version, bytes)) = record(snapshot, root.as_bytes())? else {
        return Ok(None);
    };
    let opened = open(&bytes).map_err(|e| ValueError::Corrupt(Corrupt::Envelope(e)))?;
    if opened.kind != Kind::Document {
        return Err(ApplyError::KindMismatch { found: opened.kind }.into());
    }
    let value = decode(opened.payload).map_err(|e| ValueError::Corrupt(Corrupt::Codec(e)))?;
    Ok(Some(Document { version, value }))
}

/// Compile `delta` against the document at `root` into one `Put` and its precondition.
///
/// [`Expected::Version`] is checked here first, before any work, and again by the kernel at
/// apply. [`Expected::Absent`] does not read the record at all: the delta applies to an absent
/// base, and [`Condition::Absent`] makes the kernel refuse it if a record exists by then.
///
/// # Errors
/// [`ApplyError::ObjectAbsent`] for [`Expected::Version`] on a missing record;
/// [`ApplyError::VersionConflict`]; [`ApplyError::KindMismatch`] when the object is a map or a
/// set; any [`ApplyError`] from the ops or the encoder; or [`ValueError::Corrupt`] when the
/// stored record does not read back.
pub fn compile(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    expected: Expected,
    delta: &Delta,
) -> Result<Compiled, ValueError> {
    let base = match expected {
        Expected::Absent => None,
        Expected::Version(want) => {
            check_version(snapshot, root, want)?;
            let document = read(snapshot, root)?.ok_or(ApplyError::ObjectAbsent)?;
            Some(document.value)
        }
    };
    let after = materialize(base, delta)?;
    let payload = encode(&after).map_err(ApplyError::from)?;
    let value = seal(Kind::Document, &payload).map_err(ApplyError::from)?;
    let key = root.to_bytes();
    let (expected_version, conditions) = match expected {
        Expected::Absent => (None, vec![Condition::Absent { key: key.clone() }]),
        Expected::Version(v) => (Some(v), Vec::new()),
    };
    let mutations = vec![Mutation::Put {
        key,
        value,
        expected_version,
    }];
    // The kernel's own measure of the record it would ship, so compile refuses exactly what
    // admission check 10 refuses (L-R186v).
    if record_len(conditions.len(), &mutations) > MAX_ENVELOPE_BYTES {
        return Err(ApplyError::TooLarge {
            limit: SizeLimit::Write,
        }
        .into());
    }
    Ok(Compiled {
        mutations,
        conditions,
    })
}

/// The root's version must be `want`.
///
/// # Errors
/// [`ApplyError::ObjectAbsent`] or [`ApplyError::VersionConflict`].
pub(crate) fn check_version(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    want: u64,
) -> Result<(), ApplyError> {
    match snapshot.version(Namespace::User, root.as_bytes()) {
        None => Err(ApplyError::ObjectAbsent),
        Some(found) if found != want => Err(ApplyError::VersionConflict {
            expected: want,
            found,
        }),
        Some(_) => Ok(()),
    }
}
