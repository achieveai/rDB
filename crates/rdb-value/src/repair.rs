//! Clear a damaged object (M9 S6; ADR-rdb-0013 decision 11, ADR-rdb-0012 §7 and §12).
//!
//! [`clear_object`] proves the object is damaged, then compiles one batch of `Delete`s:
//!
//! - **Prove first.** The object is read as the read path reads it, then its drop is compiled at
//!   the root's version. An object that passes both is [`ApplyError::NotDamaged`], and nothing is
//!   deleted.
//! - **Never a newer build's record.** A record written by a newer build may be valid. Any one
//!   under the object refuses the whole clear ([`Corrupt::is_newer_build`]).
//! - **Each delete names the version it saw.** A record rewritten, repaired or recreated before
//!   apply makes the kernel refuse the whole batch. So does a record the proof found missing, by
//!   its `Absent` condition.
//! - **Clean records first; faulting records, then the root, last.** A batch that does not empty
//!   the object keeps the root and every faulting record, so the object still reads as damaged
//!   until the last batch.

use bytes::Bytes;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::admission::MAX_REQUEST_MUTATIONS;
use rdb_core::transaction::record_len;
use rdb_core::{Condition, Mutation, Namespace, SnapshotRead};

use crate::blob::{check_chunk, read_blob, verify_chunks, Manifest};
use crate::collection::{
    check_element, collection, drop_collection, members, Collection, CollectionKind,
};
use crate::compile::{read, record, BlockFault, Compiled, Corrupt, SlotFault, ValueError};
use crate::delta::{ApplyError, SizeLimit};
use crate::envelope::{open, Kind};
use crate::keys::{block_key, chunk_key, item_key, parse, KeyError, RootKey, Sub};
use crate::list::{check_base, check_item, check_slot, drop_list, items, list, Start};

/// The most deletes of records other than the root in one batch: one less than a request
/// carries, so the batch that empties the object still has room for the root.
pub const MAX_CLEAR_DELETES: usize = MAX_REQUEST_MUTATIONS - 1;

/// Records read per page of the object scan.
const SCAN_PAGE: usize = 64;

/// One batch of a clear, and the damage that proved it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cleared {
    /// The deletes, and the conditions that guard them.
    pub compiled: Compiled,
    /// The first damage the proof found.
    pub damage: Corrupt,
    /// How many of the deletes are of records that fail a check the read makes, the root aside.
    pub faulting: usize,
}

/// How the clear checks each record under the object: as the read of the root's kind does.
enum Reader {
    /// No root, or a root that does not open: only a newer build's record is looked for, and
    /// no record is faulting.
    Unchecked,
    /// A document, or a root whose own read fails: each record is opened as an envelope.
    Envelope,
    /// A map or a set: elements as [`members`] reads them.
    Collection(Collection),
    /// A list whose root is at this version: items and blocks as [`items`] reads them.
    List(u64),
    /// A blob: chunks as [`crate::blob::read_range`] reads them.
    Blob(Manifest),
}

/// What the proof found: the first damage, how to check each record, and the keys it found
/// missing.
struct Proof {
    damage: Corrupt,
    reader: Reader,
    missing: Vec<Bytes>,
}

/// A record of the object other than its root, as the scan found it.
#[derive(Debug)]
struct Found {
    key: Bytes,
    version: u64,
    /// Its sub, or why its key does not parse.
    sub: Result<Sub, KeyError>,
    /// It fails a check the read makes on it.
    faulting: bool,
}

/// Compile one batch of the clear of the object at `root`.
///
/// `Ok(None)`: nothing is stored under the object. Otherwise the batch deletes up to
/// [`MAX_CLEAR_DELETES`] records besides the root, each at the version it was read at; the root
/// goes in the batch that empties the rest. Call again until `Ok(None)`.
///
/// # Errors
/// [`ApplyError::NotDamaged`] when the object reads back cleanly; [`ValueError::Corrupt`] with a
/// newer build's error ([`Corrupt::is_newer_build`]) when any record under the object is one;
/// [`ApplyError::KindMismatch`] for a chunk record at the root key, which no read takes.
pub fn clear_object(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
) -> Result<Option<Cleared>, ValueError> {
    let (root_version, proof) = match record(snapshot, root.as_bytes())? {
        None => (None, None),
        Some((version, bytes)) => match open(&bytes) {
            Ok(opened) => (
                Some(version),
                Some(prove(snapshot, root, version, opened.kind)?),
            ),
            Err(error) if error.is_newer_build() => {
                return Err(ValueError::Corrupt(Corrupt::Envelope(error)))
            }
            Err(error) => (
                Some(version),
                Some(Proof {
                    damage: Corrupt::Envelope(error),
                    reader: Reader::Unchecked,
                    missing: Vec::new(),
                }),
            ),
        },
    };
    let reader = proof.as_ref().map_or(&Reader::Unchecked, |p| &p.reader);
    let records = scan(snapshot, root, reader)?;
    let (damage, missing) = match proof {
        Some(proof) => (proof.damage, proof.missing),
        None if records.is_empty() => return Ok(None),
        None => (prove_absent(&records)?, Vec::new()),
    };
    let (compiled, faulting) = batch(root, root_version, missing, records)?;
    Ok(Some(Cleared {
        compiled,
        damage,
        faulting,
    }))
}

/// No root: an element, item or block record is an orphan, and a record whose key does not
/// parse is damage. Chunks alone are not damage: `blob gc` removes them (ADR-rdb-0014 §9).
fn prove_absent(records: &[Found]) -> Result<Corrupt, ValueError> {
    for found in records {
        match &found.sub {
            Ok(Sub::Element | Sub::Item | Sub::Block) => return Ok(Corrupt::OrphanElement),
            Err(error) => return Ok(Corrupt::Key(error.clone())),
            Ok(_) => {}
        }
    }
    Err(ApplyError::NotDamaged.into())
}

/// A read's verdict: `None` when it reads back cleanly, the damage otherwise. A newer build's
/// `Corrupt`, or any other error, is returned as the refusal.
fn verdict(read: Result<(), ValueError>) -> Result<Option<Corrupt>, ValueError> {
    match read {
        Ok(()) => Ok(None),
        Err(ValueError::Corrupt(corrupt)) if !corrupt.is_newer_build() => Ok(Some(corrupt)),
        Err(refused) => Err(refused),
    }
}

/// A drop compile's verdict: a drop refused only because the object holds elements or items is
/// not damage (A-2).
fn drop_verdict(dropped: Result<(), ValueError>) -> Result<Option<Corrupt>, ValueError> {
    verdict(match dropped {
        Err(ValueError::Apply(ApplyError::NotEmpty { .. } | ApplyError::ListNotEmpty { .. })) => {
            Ok(())
        }
        other => other,
    })
}

/// The proof, once the first damage is known: none is [`ApplyError::NotDamaged`].
fn proven(
    damage: Option<Corrupt>,
    reader: Reader,
    missing: Vec<Bytes>,
) -> Result<Proof, ValueError> {
    let damage = damage.ok_or(ApplyError::NotDamaged)?;
    Ok(Proof {
        damage,
        reader,
        missing,
    })
}

/// The root opens as `kind`: run that kind's full read, then its drop compile at `version`.
fn prove(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    version: u64,
    kind: Kind,
) -> Result<Proof, ValueError> {
    match kind {
        // A list block or slot at the root key reads as `ListRecordAtRoot`, as for a document.
        Kind::Document | Kind::ListBlock | Kind::ListSlot => proven(
            verdict(read(snapshot, root).map(drop))?,
            Reader::Envelope,
            Vec::new(),
        ),
        Kind::Map | Kind::Set => prove_collection(snapshot, root, version),
        Kind::List => prove_list(snapshot, root, version),
        Kind::Blob => prove_blob(snapshot, root),
        // No read takes a chunk record at a root key, so nothing proves it damaged (S6 non-goal).
        Kind::Chunk => Err(ApplyError::KindMismatch { found: kind }.into()),
    }
}

/// A map or set: every element as [`members`] reads it, the root's count against the elements
/// stored, then [`drop_collection`].
fn prove_collection(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    version: u64,
) -> Result<Proof, ValueError> {
    let found = match collection(snapshot, root) {
        Ok(Some(found)) => found,
        other => return proven(verdict(other.map(drop))?, Reader::Envelope, Vec::new()),
    };
    let read = members(snapshot, root, None, usize::MAX).and_then(|listed| {
        let stored = u64::try_from(listed.members.len()).unwrap_or(u64::MAX);
        if stored == found.count {
            Ok(())
        } else {
            Err(ValueError::Corrupt(Corrupt::CountMismatch {
                count: found.count,
            }))
        }
    });
    let damage = match verdict(read)? {
        Some(damage) => Some(damage),
        None => drop_verdict(drop_collection(snapshot, root, version).map(drop))?,
    };
    proven(damage, Reader::Collection(found), Vec::new())
}

/// A list: its root, every item as [`items`] reads them, then [`drop_list`]. A block or item the
/// list names and the read found missing gets an `Absent` condition.
fn prove_list(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    version: u64,
) -> Result<Proof, ValueError> {
    let found = match list(snapshot, root) {
        Ok(Some(found)) => found,
        other => return proven(verdict(other.map(drop))?, Reader::Envelope, Vec::new()),
    };
    let count = usize::try_from(found.count).unwrap_or(usize::MAX);
    let read = items(snapshot, root, Start::Position(0), count).map(drop);
    let damage = match verdict(read)? {
        Some(damage) => Some(damage),
        None => drop_verdict(drop_list(snapshot, root, version).map(drop))?,
    };
    let missing = match &damage {
        Some(Corrupt::Block {
            id,
            fault: BlockFault::Missing,
        }) => vec![block_key(root, *id)],
        Some(Corrupt::ItemMissing { id }) => vec![item_key(root, *id)],
        _ => Vec::new(),
    };
    proven(damage, Reader::List(version), missing)
}

/// A blob: its manifest, then every chunk it names, checked as a range read checks it and
/// kept nowhere. Each chunk it names that is not stored gets an `Absent` condition: at most 255,
/// within `MAX_CONDITIONS` with the root guard.
fn prove_blob(snapshot: &dyn SnapshotRead, root: &RootKey) -> Result<Proof, ValueError> {
    let blob = match read_blob(snapshot, root) {
        Ok(Some(blob)) => blob,
        other => return proven(verdict(other.map(drop))?, Reader::Envelope, Vec::new()),
    };
    let m = blob.manifest;
    let damage = verdict(verify_chunks(snapshot, root, &m))?;
    let missing = (0..m.chunks())
        .map(|index| chunk_key(root, &m.upload, index))
        .filter(|key| snapshot.get(Namespace::User, key).is_none())
        .collect();
    proven(damage, Reader::Blob(m), missing)
}

/// Every record under the object except its root, in key order, each checked by `reader`.
fn scan(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    reader: &Reader,
) -> Result<Vec<Found>, ValueError> {
    let prefix = root.object_prefix();
    let mut records = Vec::new();
    let mut from = prefix.to_vec();
    loop {
        let page = snapshot.scan(Namespace::User, &from, SCAN_PAGE);
        let full = page.len() == SCAN_PAGE;
        let mut last = None;
        for (key, bytes) in page {
            if !key.starts_with(prefix) {
                last = None;
                break;
            }
            last = Some(key.clone());
            if key.as_ref() == root.as_bytes() {
                continue;
            }
            let version = snapshot
                .version(Namespace::User, &key)
                .ok_or(ValueError::Corrupt(Corrupt::VersionWithoutValue))?;
            let faulting = faulting(snapshot, root, reader, &key, version, &bytes)?;
            records.push(Found {
                sub: parse(&key).map(|parsed| parsed.sub),
                key,
                version,
                faulting,
            });
        }
        match last {
            Some(key) if full => {
                from = key.to_vec();
                from.push(0x00);
            }
            _ => break,
        }
    }
    Ok(records)
}

/// Whether one record under the object fails a check the read of `reader` makes on it, with
/// that read's own decoder, nested envelopes included (A-4). A newer build's record refuses the
/// clear.
fn faulting(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    reader: &Reader,
    key: &[u8],
    version: u64,
    bytes: &[u8],
) -> Result<bool, ValueError> {
    let checked = !matches!(reader, Reader::Unchecked);
    let parsed = parse(key);
    // A newer build's envelope refuses the clear whatever else is wrong with its record: never
    // delete what this build cannot read (A-R139 D1). A set member's bytes are never opened.
    let element = matches!(&parsed, Ok(p) if matches!(p.sub, Sub::Element));
    let set = matches!(reader, Reader::Collection(c) if c.kind == CollectionKind::Set);
    if !(element && (set || bytes.is_empty())) {
        if let Err(error) = open(bytes) {
            if error.is_newer_build() {
                return Err(ValueError::Corrupt(Corrupt::Envelope(error)));
            }
        }
    }
    let Ok(parsed) = parsed else {
        return Ok(checked);
    };
    let check = match (
        reader,
        parsed.sub,
        parsed.list_id,
        parsed.slot,
        parsed.chunk,
    ) {
        // No build this one knows writes there: a newer build's record.
        (_, Sub::Reserved(byte), ..) => return Err(ValueError::Corrupt(Corrupt::UnknownSub(byte))),
        (Reader::Collection(found), Sub::Element, ..) => {
            check_element(root, found, key, version, bytes)
        }
        (Reader::List(at), Sub::Item, Some(id), ..) => check_item(snapshot, root, id, *at),
        (Reader::List(at), Sub::Block, Some(id), None, _) => {
            check_base(id, version, *at, bytes).map_err(ValueError::Corrupt)
        }
        (Reader::List(_), Sub::Block, Some(_), Some(_), _) => {
            return match check_slot(bytes) {
                Ok(()) => Ok(false),
                Err(SlotFault::Envelope(error)) if error.is_newer_build() => {
                    Err(ValueError::Corrupt(Corrupt::Envelope(error)))
                }
                Err(_) => Ok(true),
            }
        }
        (Reader::Blob(m), Sub::Chunk, _, _, Some((upload, index))) => {
            check_chunk(m, &upload, index, bytes)
        }
        // A set member holds no bytes.
        (_, Sub::Element, ..) if bytes.is_empty() => Ok(()),
        _ => open(bytes)
            .map(drop)
            .map_err(|error| ValueError::Corrupt(Corrupt::Envelope(error))),
    };
    match check {
        Ok(()) => Ok(false),
        Err(ValueError::Corrupt(corrupt)) if corrupt.is_newer_build() => {
            Err(ValueError::Corrupt(corrupt))
        }
        Err(ValueError::Corrupt(_)) => Ok(checked),
        Err(refused) => Err(refused),
    }
}

/// Clean records in key order, then faulting ones, then the root in the batch that empties the
/// rest. Every batch carries an `Absent` for each key the proof found missing, and every batch
/// that keeps the root is guarded by it as read: its version, or its absence. Also returns how
/// many faulting records the batch deletes.
fn batch(
    root: &RootKey,
    root_version: Option<u64>,
    missing: Vec<Bytes>,
    records: Vec<Found>,
) -> Result<(Compiled, usize), ValueError> {
    let total = records.len();
    let (clean, faulting): (Vec<_>, Vec<_>) = records.into_iter().partition(|f| !f.faulting);
    let clean_len = clean.len();
    let guard = match root_version {
        Some(version) => Condition::VersionEquals {
            key: root.to_bytes(),
            version,
        },
        None => Condition::Absent {
            key: root.to_bytes(),
        },
    };
    let absent: Vec<_> = missing
        .into_iter()
        .map(|key| Condition::Absent { key })
        .collect();
    let mut mutations = Vec::new();
    for found in clean.into_iter().chain(faulting) {
        if mutations.len() == MAX_CLEAR_DELETES {
            break;
        }
        mutations.push(Mutation::Delete {
            key: found.key,
            expected_version: Some(found.version),
        });
        if record_len(absent.len() + 1, &mutations) > MAX_ENVELOPE_BYTES {
            mutations.pop();
            break;
        }
    }
    if let Some(version) = root_version.filter(|_| mutations.len() == total) {
        // The root's own `expected_version` guards it in its last batch.
        mutations.push(Mutation::Delete {
            key: root.to_bytes(),
            expected_version: Some(version),
        });
        if record_len(absent.len(), &mutations) <= MAX_ENVELOPE_BYTES {
            let faulting = total - clean_len;
            return Ok((
                Compiled {
                    mutations,
                    conditions: absent,
                },
                faulting,
            ));
        }
        mutations.pop();
    }
    if mutations.is_empty() {
        return Err(ApplyError::TooLarge {
            limit: SizeLimit::Write,
        }
        .into());
    }
    let faulting = mutations.len().saturating_sub(clean_len);
    let mut conditions = vec![guard];
    conditions.extend(absent);
    Ok((
        Compiled {
            mutations,
            conditions,
        },
        faulting,
    ))
}
