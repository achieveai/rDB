//! Clear a damaged object (M9 S6; ADR-rdb-0013 decision 11, ADR-rdb-0012 §7 and §12).
//!
//! [`clear_object`] proves the object is damaged, then compiles one batch of `Delete`s:
//!
//! - **Prove first.** The object is read as the read path reads it. An object that reads back
//!   cleanly is [`ApplyError::NotDamaged`], and nothing is deleted.
//! - **Never a newer build's record.** A record written by a newer build may be valid. Any one
//!   under the object refuses the whole clear ([`Corrupt::is_newer_build`]).
//! - **Each delete names the version it saw.** A record rewritten, repaired or recreated before
//!   apply makes the kernel refuse the whole batch.
//! - **Clean records first; faulting records, then the root, last.** A batch that does not empty
//!   the object keeps the root and every faulting record, so the object still reads as damaged
//!   until the last batch.

use bytes::Bytes;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::admission::MAX_REQUEST_MUTATIONS;
use rdb_core::transaction::record_len;
use rdb_core::{Condition, Mutation, Namespace, SnapshotRead};

use crate::compile::{read, record, Compiled, Corrupt, ValueError};
use crate::delta::{ApplyError, SizeLimit};
use crate::envelope::{open, Kind};
use crate::keys::{parse, KeyError, RootKey, Sub};

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

/// The object's root record, as the clear found it.
enum Root {
    /// No root record.
    Absent,
    /// A root at this version that does not open: that is the damage, and its kind is unknown.
    Unreadable(u64, Corrupt),
    /// A root at this version that opens as this kind.
    Opens(u64, Kind),
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
/// [`ApplyError::KindMismatch`] for a kind this build does not clear yet.
pub fn clear_object(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
) -> Result<Option<Cleared>, ValueError> {
    let state = match record(snapshot, root.as_bytes())? {
        None => Root::Absent,
        Some((version, bytes)) => match open(&bytes) {
            Ok(opened) => Root::Opens(version, opened.kind),
            Err(error) if error.is_newer_build() => {
                return Err(ValueError::Corrupt(Corrupt::Envelope(error)))
            }
            Err(error) => Root::Unreadable(version, Corrupt::Envelope(error)),
        },
    };
    let records = scan(snapshot, root, matches!(state, Root::Opens(..)))?;
    let (damage, root_version) = match state {
        Root::Absent if records.is_empty() => return Ok(None),
        Root::Absent => (prove_absent(&records)?, None),
        Root::Unreadable(version, damage) => (damage, Some(version)),
        Root::Opens(version, kind) => (prove(snapshot, root, kind)?, Some(version)),
    };
    let (compiled, faulting) = batch(root, root_version, records)?;
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

/// The root opens as `kind`: run that kind's full read. A newer build's error is the refusal;
/// any other `Corrupt` is the damage.
fn prove(snapshot: &dyn SnapshotRead, root: &RootKey, kind: Kind) -> Result<Corrupt, ValueError> {
    let read = match kind {
        // A list block or slot at the root key reads as `ListRecordAtRoot`, as for a document.
        Kind::Document | Kind::ListBlock | Kind::ListSlot => read(snapshot, root).map(|_| ()),
        found => return Err(ApplyError::KindMismatch { found }.into()),
    };
    match read {
        Ok(()) => Err(ApplyError::NotDamaged.into()),
        Err(ValueError::Corrupt(corrupt)) if !corrupt.is_newer_build() => Ok(corrupt),
        Err(refused) => Err(refused),
    }
}

/// Every record under the object except its root, in key order. A newer build's record refuses
/// the clear. `checked`: the root opened, so a record that fails its own open is faulting.
fn scan(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    checked: bool,
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
            let sub = parse(&key).map(|parsed| parsed.sub);
            let opens = match sub {
                // No build this one knows writes there: a newer build's record.
                Ok(Sub::Reserved(byte)) => {
                    return Err(ValueError::Corrupt(Corrupt::UnknownSub(byte)))
                }
                // A set member holds no bytes.
                Ok(Sub::Element) if bytes.is_empty() => true,
                Ok(_) => match open(&bytes) {
                    Ok(_) => true,
                    Err(error) if error.is_newer_build() => {
                        return Err(ValueError::Corrupt(Corrupt::Envelope(error)))
                    }
                    Err(_) => false,
                },
                Err(_) => false,
            };
            records.push(Found {
                key,
                version,
                sub,
                faulting: checked && !opens,
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

/// Clean records in key order, then faulting ones, then the root in the batch that empties the
/// rest. Every batch that keeps the root is guarded by it as read: its version, or its absence.
/// Also returns how many faulting records the batch deletes.
fn batch(
    root: &RootKey,
    root_version: Option<u64>,
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
    let conditions = vec![guard];
    let mut mutations = Vec::new();
    for found in clean.into_iter().chain(faulting) {
        if mutations.len() == MAX_CLEAR_DELETES {
            break;
        }
        mutations.push(Mutation::Delete {
            key: found.key,
            expected_version: Some(found.version),
        });
        if record_len(conditions.len(), &mutations) > MAX_ENVELOPE_BYTES {
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
        if record_len(0, &mutations) <= MAX_ENVELOPE_BYTES {
            let faulting = total - clean_len;
            return Ok((
                Compiled {
                    mutations,
                    conditions: Vec::new(),
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
    Ok((
        Compiled {
            mutations,
            conditions,
        },
        faulting,
    ))
}
