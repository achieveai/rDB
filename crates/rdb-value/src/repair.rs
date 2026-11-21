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
//!   the object keeps the root and every faulting record.
//! - **Never healthy while half cleared.** A batch that leaves any record is checked against
//!   what it leaves: the object must still prove damaged, or deletes are taken back from its end
//!   until it does (review R-1). Otherwise the next call would answer "not damaged" over what is
//!   left.

use std::collections::BTreeSet;

use bytes::Bytes;
use rdb_core::contracts::trace::Version;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::admission::MAX_REQUEST_MUTATIONS;
use rdb_core::transaction::record_len;
use rdb_core::{Condition, Generation, Mutation, Namespace, Seq, SnapshotHandle, SnapshotRead};

use crate::blob::{check_chunk, read_blob, verify_chunks, Manifest};
use crate::collection::{
    check_count, check_element, collection, compile_collection, drop_collection, members,
    Collection, CollectionKind, ElemOp,
};
use crate::compile::{
    read, record, BlockFault, Compiled, Corrupt, Expected, SlotFault, ValueError,
};
use crate::delta::{ApplyError, SizeLimit};
use crate::envelope::{newer_build, open, Kind};
use crate::keys::{block_key, chunk_key, item_key, parse, slot_key, KeyError, RootKey, Sub};
use crate::list::{
    check_base, check_item, check_slot, compile_list, drop_list, items, list, slot_of, ListOp,
    Start, DEFAULT_BLOCK_MAX,
};

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
/// [`ApplyError::NotDamaged`] when the object reads back cleanly, [`ApplyError::OnlyChunks`]
/// when only upload chunks are stored under it; [`ValueError::Corrupt`] with a
/// newer build's error ([`Corrupt::is_newer_build`]) when any record under the object is one;
/// [`ApplyError::KindMismatch`] for a chunk record at the root key, which no read takes.
pub fn clear_object(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
) -> Result<Option<Cleared>, ValueError> {
    let (root_version, proof) = assess(snapshot, root)?;
    let reader = proof.as_ref().map_or(&Reader::Unchecked, |p| &p.reader);
    let records = scan(snapshot, root, reader)?;
    // Only a blob's read takes its chunks, so under any other root, or none, no proof rests on
    // one (review R2-01).
    let chunks_first = !matches!(reader, Reader::Blob(_));
    let (damage, missing) = match proof {
        Some(proof) => (proof.damage, proof.missing),
        None if records.is_empty() => return Ok(None),
        None => (prove_absent(&records)?, Vec::new()),
    };
    let still_damaged = |deletes: &[Mutation]| proves_after(snapshot, root, deletes);
    let (compiled, faulting) = batch(
        root,
        root_version,
        missing,
        records,
        chunks_first,
        still_damaged,
    )?;
    Ok(Some(Cleared {
        compiled,
        damage,
        faulting,
    }))
}

/// The root's version and the proof that the object is damaged, both `None` with no root. A
/// root that opens is proved by its kind's read; one that does not is damage.
fn assess(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
) -> Result<(Option<u64>, Option<Proof>), ValueError> {
    Ok(match record(snapshot, root.as_bytes())? {
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
    })
}

/// Whether the object still proves damaged once `deletes` apply, by the same proof the clear
/// starts with (review R-1). A batch that leaves any record must leave this true.
fn proves_after(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    deletes: &[Mutation],
) -> Result<bool, ValueError> {
    let left = Without::new(snapshot, deletes);
    let proved = match assess(&left, root) {
        Ok((_, Some(_))) => return Ok(true),
        Ok((_, None)) => scan(&left, root, &Reader::Unchecked).and_then(|r| prove_absent(&r)),
        Err(error) => Err(error),
    };
    match proved {
        Ok(_) => Ok(true),
        Err(ValueError::Apply(ApplyError::NotDamaged | ApplyError::OnlyChunks)) => Ok(false),
        Err(error) => Err(error),
    }
}

/// A snapshot with the records some deletes name taken out: what a batch would leave.
struct Without<'a> {
    snapshot: &'a dyn SnapshotRead,
    deleted: BTreeSet<&'a [u8]>,
}

impl<'a> Without<'a> {
    fn new(snapshot: &'a dyn SnapshotRead, deletes: &'a [Mutation]) -> Self {
        let deleted = deletes
            .iter()
            .filter_map(|m| match m {
                Mutation::Delete { key, .. } => Some(key.as_ref()),
                Mutation::Put { .. } => None,
            })
            .collect();
        Self { snapshot, deleted }
    }

    fn gone(&self, ns: Namespace, key: &[u8]) -> bool {
        ns == Namespace::User && self.deleted.contains(key)
    }
}

impl SnapshotRead for Without<'_> {
    fn handle(&self) -> SnapshotHandle {
        self.snapshot.handle()
    }

    fn at(&self) -> Seq {
        self.snapshot.at()
    }

    fn generation(&self) -> Generation {
        self.snapshot.generation()
    }

    fn get(&self, ns: Namespace, key: &[u8]) -> Option<Bytes> {
        (!self.gone(ns, key))
            .then(|| self.snapshot.get(ns, key))
            .flatten()
    }

    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version> {
        (!self.gone(ns, key))
            .then(|| self.snapshot.version(ns, key))
            .flatten()
    }

    /// Pages past the records taken out, so a scan still returns `limit` records when that
    /// many are left.
    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        let mut out = Vec::new();
        let mut from = from.to_vec();
        while out.len() < limit {
            let want = limit - out.len();
            let page = self.snapshot.scan(ns, &from, want);
            let full = page.len() == want;
            let Some((last, _)) = page.last() else {
                break;
            };
            from = last.to_vec();
            from.push(0x00);
            out.extend(page.into_iter().filter(|(key, _)| !self.gone(ns, key)));
            if !full {
                break;
            }
        }
        out
    }
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
    Err(ApplyError::OnlyChunks.into())
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
    // Every element in one call, so the read applies the count rule itself; it is applied here
    // too, by the same helper, so the proof never depends on how `members` pages.
    let read = members(snapshot, root, None, usize::MAX)
        .and_then(|listed| check_count(&found, listed.members.len()));
    let damage = match verdict(read)? {
        Some(damage) => Some(damage),
        None => drop_verdict(drop_collection(snapshot, root, version).map(drop))?,
    };
    proven(damage, Reader::Collection(found), Vec::new())
}

/// A list: its root, every item as [`items`] reads them, then [`drop_list`]. A block, item or
/// pending slot the list names and the read found missing gets an `Absent` condition.
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
        // A pending slot not stored (review Q-1). `OpMissing` also names a slot stored with
        // another op in it: that one is in the scan, and its delete names its version.
        Some(Corrupt::Block {
            id,
            fault: BlockFault::OpMissing { op },
        }) => Some(slot_key(root, *id, slot_of(*op)))
            .filter(|key| snapshot.get(Namespace::User, key).is_none())
            .into_iter()
            .collect(),
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
        if let Some(error) = newer_build(bytes) {
            return Err(ValueError::Corrupt(Corrupt::Envelope(error)));
        }
    }
    let parsed = match parsed {
        Ok(parsed) => parsed,
        // An element key tag this build does not know is a newer build's key (review R-3).
        Err(error) => {
            let corrupt = Corrupt::Key(error);
            if corrupt.is_newer_build() {
                return Err(ValueError::Corrupt(corrupt));
            }
            return Ok(checked);
        }
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

/// Chunks first when `chunks_first`, then clean records in key order, then faulting ones, then
/// the root in the batch that empties the rest. Every batch carries an `Absent` for each key the
/// proof found missing, and every batch that keeps the root is guarded by it as read: its
/// version, or its absence. A batch that leaves any record leaves the object `still_damaged`
/// (review F-003 for a map's count, R-1 for every kind). Chunks go first when no proof rests on them: otherwise they sort after the
/// object's last damage witness, fill the batch, and every delete is taken back (R2-01). Also
/// returns how many faulting records the batch deletes.
fn batch(
    root: &RootKey,
    root_version: Option<u64>,
    missing: Vec<Bytes>,
    records: Vec<Found>,
    chunks_first: bool,
    still_damaged: impl Fn(&[Mutation]) -> Result<bool, ValueError>,
) -> Result<(Compiled, usize), ValueError> {
    let total = records.len();
    let (chunks, rest): (Vec<_>, Vec<_>) = records
        .into_iter()
        .partition(|f| chunks_first && matches!(f.sub, Ok(Sub::Chunk)));
    let (clean, faulting): (Vec<_>, Vec<_>) = rest.into_iter().partition(|f| !f.faulting);
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
    // Per delete: whether it is of a faulting record.
    let mut deleted = Vec::new();
    for found in chunks.into_iter().chain(clean).chain(faulting) {
        if mutations.len() == MAX_CLEAR_DELETES {
            break;
        }
        deleted.push(found.faulting);
        mutations.push(Mutation::Delete {
            key: found.key,
            expected_version: Some(found.version),
        });
        if record_len(absent.len() + 1, &mutations) > MAX_ENVELOPE_BYTES {
            mutations.pop();
            deleted.pop();
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
            let faulting = deleted.iter().filter(|faulting| **faulting).count();
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
    // The batch leaves the root, or with no root some record. If what it leaves would read back
    // cleanly, the next call would answer "not damaged" over it: a map left holding exactly the
    // `count` elements its root names (F-003), an empty list left without its orphans (R-1).
    // Each delete taken back from the end leaves one more record; with none taken, the object
    // is as proved.
    if root_version.is_some() || mutations.len() < total {
        while !mutations.is_empty() && !still_damaged(&mutations)? {
            mutations.pop();
            deleted.pop();
        }
    }
    if mutations.is_empty() {
        return Err(ApplyError::TooLarge {
            limit: SizeLimit::Write,
        }
        .into());
    }
    let faulting = deleted.iter().filter(|faulting| **faulting).count();
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

/// One batch of an ordinary delete of the object at `root`, of any kind (#147): no damage proof.
/// Every state between two batches reads as a valid object or as absent, never as damage, so a
/// crash or a stop between batches leaves nothing a read refuses (PR #178 review r1, T4-T6).
///
/// - Everything fits one request: the records other than the root, then the root, each at the
///   version read. This is the last batch.
/// - A map or set that does not fit: up to [`MAX_CLEAR_DELETES`] of its elements removed by
///   [`compile_collection`], which rewrites the root's count. A smaller map.
/// - A list that does not fit: its first items removed by [`compile_list`], which relinks its
///   blocks and rewrites the root. A shorter list.
/// - A blob that does not fit: its root first, with chunks up to the limit. What is left is
///   chunks with no root, which reads as absent and is what an upload never published leaves.
/// - Any other object (a document with upload chunks, or a root that does not open): up to
///   [`MAX_CLEAR_DELETES`] records other than the root, in key order, root last, as Clear does.
///   A document's read never looks at chunks, so it reads back unchanged; a root that does not
///   open is damage already.
/// - With no root, up to [`MAX_CLEAR_DELETES`] records, guarded by the root's absence.
/// - `Ok(None)`: nothing is stored under the object. Call again until then.
///
/// With `expect`, the root must be at that version: every batch is a CAS. Once a blob's root has
/// gone, the chunks left behind go on under the root's absence with `expect` still given, since
/// the object it named is deleted.
///
/// # Errors
/// [`ApplyError::VersionConflict`] when the root is at another version than `expect`, and
/// [`ApplyError::ObjectAbsent`] when `expect` names a version and there is no root but records
/// other than chunks are stored. [`Corrupt::VersionWithoutValue`] for a record with no version.
/// A map's, set's or list's own compile errors for a damaged element, item or block.
pub fn delete_object(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    expect: Option<u64>,
) -> Result<Option<Compiled>, ValueError> {
    let root_version = snapshot.version(Namespace::User, root.as_bytes());
    let prefix = root.object_prefix();
    let mut mutations = Vec::new();
    let mut from = prefix.to_vec();
    let mut more = false;
    'scan: loop {
        let page = snapshot.scan(Namespace::User, &from, SCAN_PAGE);
        let full = page.len() == SCAN_PAGE;
        for (key, _) in page {
            if !key.starts_with(prefix) {
                break 'scan;
            }
            from = key.to_vec();
            from.push(0x00);
            if key.as_ref() == root.as_bytes() {
                continue;
            }
            if mutations.len() == MAX_CLEAR_DELETES {
                more = true;
                break 'scan;
            }
            let version = snapshot
                .version(Namespace::User, &key)
                .ok_or(ValueError::Corrupt(Corrupt::VersionWithoutValue))?;
            mutations.push(Mutation::Delete {
                key,
                expected_version: Some(version),
            });
            if record_len(1, &mutations) > MAX_ENVELOPE_BYTES {
                mutations.pop();
                more = true;
                break 'scan;
            }
        }
        if !full {
            break;
        }
    }
    if mutations.is_empty() && root_version.is_none() {
        return Ok(None);
    }
    match (expect, root_version) {
        (Some(expected), Some(found)) if expected != found => {
            return Err(ApplyError::VersionConflict { expected, found }.into());
        }
        (Some(_), None) if !only_chunks(&mutations) => return Err(ApplyError::ObjectAbsent.into()),
        _ => {}
    }
    if let Some(version) = root_version {
        if !more {
            mutations.push(Mutation::Delete {
                key: root.to_bytes(),
                expected_version: Some(version),
            });
            if record_len(0, &mutations) <= MAX_ENVELOPE_BYTES {
                return Ok(Some(Compiled {
                    mutations,
                    conditions: Vec::new(),
                }));
            }
            mutations.pop();
        }
        if let Some(partial) = partial_delete(snapshot, root, version, &mut mutations)? {
            return Ok(Some(partial));
        }
    }
    let guard = match root_version {
        Some(version) => Condition::VersionEquals {
            key: root.to_bytes(),
            version,
        },
        None => Condition::Absent {
            key: root.to_bytes(),
        },
    };
    Ok(Some(Compiled {
        mutations,
        conditions: vec![guard],
    }))
}

/// Whether every record in `mutations` is a blob chunk (its key parses as [`Sub::Chunk`]).
fn only_chunks(mutations: &[Mutation]) -> bool {
    mutations.iter().all(|mutation| match mutation {
        Mutation::Delete { key, .. } | Mutation::Put { key, .. } => {
            matches!(parse(key).map(|parsed| parsed.sub), Ok(Sub::Chunk))
        }
    })
}

/// A batch of [`delete_object`] for an object at root version `version` that does not fit one
/// request, which leaves a valid object behind; `scanned` holds the raw deletes of its first
/// records. `None`: the kind has no such batch, so the raw one is valid (see
/// [`delete_object`]).
fn partial_delete(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    version: u64,
    scanned: &mut Vec<Mutation>,
) -> Result<Option<Compiled>, ValueError> {
    let Some((_, bytes)) = record(snapshot, root.as_bytes())? else {
        return Ok(None);
    };
    let Ok(opened) = open(&bytes) else {
        return Ok(None);
    };
    let fits = |compiled: &Compiled| {
        compiled.mutations.len() <= MAX_REQUEST_MUTATIONS
            && record_len(0, &compiled.mutations) <= MAX_ENVELOPE_BYTES
    };
    match opened.kind {
        Kind::Map | Kind::Set => {
            let kind = if opened.kind == Kind::Map {
                CollectionKind::Map
            } else {
                CollectionKind::Set
            };
            let page = members(snapshot, root, None, MAX_CLEAR_DELETES)?;
            let ops: Vec<_> = page
                .members
                .into_iter()
                .map(|member| ElemOp::Remove(member.key))
                .collect();
            let mut take = ops.len();
            while take > 0 {
                match compile_collection(
                    snapshot,
                    root,
                    kind,
                    Expected::Version(version),
                    &ops[..take],
                ) {
                    Ok(compiled) if fits(&compiled) => return Ok(Some(compiled)),
                    Ok(_)
                    | Err(ValueError::Apply(
                        ApplyError::TooLarge { .. } | ApplyError::TooManyWrites { .. },
                    )) => take /= 2,
                    Err(error) => return Err(error),
                }
            }
            Ok(None)
        }
        Kind::List => {
            let count = list(snapshot, root)?.map_or(0, |found| found.count);
            let mut take = count.min(MAX_CLEAR_DELETES as u64);
            while take > 0 {
                let ops =
                    vec![ListOp::Remove { at: 0 }; usize::try_from(take).unwrap_or(usize::MAX)];
                match compile_list(
                    snapshot,
                    root,
                    Expected::Version(version),
                    DEFAULT_BLOCK_MAX,
                    &ops,
                ) {
                    Ok(compiled) if fits(compiled.compiled()) => {
                        return Ok(Some(compiled.compiled().clone()))
                    }
                    Ok(_)
                    | Err(ValueError::Apply(
                        ApplyError::TooLarge { .. } | ApplyError::TooManyWrites { .. },
                    )) => take /= 2,
                    Err(error) => return Err(error),
                }
            }
            Ok(None)
        }
        Kind::Blob => {
            let mut mutations = vec![Mutation::Delete {
                key: root.to_bytes(),
                expected_version: Some(version),
            }];
            mutations.extend(scanned.drain(..).take(MAX_CLEAR_DELETES));
            while record_len(0, &mutations) > MAX_ENVELOPE_BYTES {
                mutations.pop();
            }
            Ok(Some(Compiled {
                mutations,
                conditions: Vec::new(),
            }))
        }
        Kind::Document | Kind::Chunk | Kind::ListBlock | Kind::ListSlot => Ok(None),
    }
}
