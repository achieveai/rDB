//! Maps and sets: a root record plus one record per element (ADR-rdb-0013 decisions 7–13).
//!
//! - The root, at the object's [`RootKey`], is an envelope of kind [`Kind::Map`] or [`Kind::Set`]
//!   whose payload is canonical CBOR of exactly `{"keys": 1, "count": n}`.
//! - A map entry, at [`element_key`], is a document envelope holding the value. A set member is
//!   an empty record.
//! - [`compile_collection`] turns ops into one root `Put` plus one write per element that changed,
//!   in key order. The collection's version is its root record's storage version.
//!
//! Reads go through `&dyn SnapshotRead` only. Nothing is repaired: a breach of the integrity rule
//! (decision 11) is reported as a named [`Corrupt`].

use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::BTreeMap;

use bytes::Bytes;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::admission::MAX_REQUEST_MUTATIONS;
use rdb_core::transaction::record_len;
use rdb_core::{Condition, Mutation, Namespace, SnapshotRead};

use crate::cbor::{decode, encode};
use crate::compile::{check_version, record, Compiled, Corrupt, Expected, ValueError};
use crate::delta::{ApplyError, SizeLimit};
use crate::envelope::{open, seal, Kind};
use crate::keys::{decode_element_key, element_key, encode_element, RootKey};
use crate::value::{Int, Map, MapKey, Value};

/// Element key profile v1 (ADR-rdb-0013 decision 4), the root's `keys` field.
pub const KEY_PROFILE_V1: i128 = 1;

/// Which collection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectionKind {
    /// Keys with values.
    Map,
    /// Members without values.
    Set,
}

impl CollectionKind {
    /// The root envelope's kind.
    #[must_use]
    pub const fn kind(self) -> Kind {
        match self {
            Self::Map => Kind::Map,
            Self::Set => Kind::Set,
        }
    }

    const fn of(kind: Kind) -> Option<Self> {
        match kind {
            Kind::Map => Some(Self::Map),
            Kind::Set => Some(Self::Set),
            Kind::Document => None,
        }
    }
}

/// One collection op (ADR-rdb-0013 decision 9). Ops apply in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElemOp {
    /// Map only: insert or replace the entry `key → value`.
    Put(Value, Value),
    /// Set only: insert the member; a no-op if it is present.
    Add(Value),
    /// Remove the element; a no-op if it is absent.
    Remove(Value),
    /// Refuse the whole delta unless the element is present at this point
    /// ([`ApplyError::ElementAbsent`]).
    NeedPresent(Value),
    /// Refuse the whole delta unless the element is absent at this point
    /// ([`ApplyError::ElementExists`]).
    NeedAbsent(Value),
}

/// A collection's root, read back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Collection {
    /// Map or set.
    pub kind: CollectionKind,
    /// The root record's storage version: the `seq` of the last transaction that changed it.
    pub version: u64,
    /// How many elements it holds, by its root.
    pub count: u64,
}

/// One element, read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    /// The map key or set member.
    pub key: Value,
    /// A map entry's value; `None` for a set member.
    pub value: Option<Value>,
    /// The element record's storage version. Not a concurrency token.
    pub version: u64,
}

/// A page of elements, in element order, with the root it was read under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Members {
    /// The root.
    pub collection: Collection,
    /// The elements listed.
    pub members: Vec<Member>,
}

/// The root payload for `count` elements: canonical CBOR of `{"keys": 1, "count": count}`.
fn root_payload(count: u64) -> Vec<u8> {
    let mut map = Map::new();
    map.insert(
        MapKey::new("keys"),
        Value::Integer(Int::new(KEY_PROFILE_V1).expect("1 is in range")),
    );
    map.insert(MapKey::new("count"), Value::Integer(Int::from(count)));
    encode(&Value::Map(map)).expect("a two-field map encodes")
}

/// Open a root record. A document is a [`ApplyError::KindMismatch`].
fn open_root(version: u64, bytes: &[u8]) -> Result<Collection, ValueError> {
    let corrupt = ValueError::Corrupt;
    let opened = open(bytes).map_err(|e| corrupt(Corrupt::Envelope(e)))?;
    let kind =
        CollectionKind::of(opened.kind).ok_or(ApplyError::KindMismatch { found: opened.kind })?;
    let payload = decode(opened.payload).map_err(|e| corrupt(Corrupt::Codec(e)))?;
    let Value::Map(fields) = payload else {
        return Err(corrupt(Corrupt::Root("the payload is not a map")));
    };
    let int = |name: &str| match fields.get(&MapKey::new(name)) {
        Some(Value::Integer(i)) => Ok(i.get()),
        Some(_) => Err(corrupt(Corrupt::Root("a field is not an integer"))),
        None => Err(corrupt(Corrupt::Root("a field is missing"))),
    };
    // The profile first: a newer build's root reads as that, whatever else it holds.
    let keys = int("keys")?;
    if keys != KEY_PROFILE_V1 {
        return Err(corrupt(Corrupt::UnknownKeyProfile(keys)));
    }
    let count =
        u64::try_from(int("count")?).map_err(|_| corrupt(Corrupt::Root("count is negative")))?;
    if fields.len() != 2 {
        return Err(corrupt(Corrupt::Root("fields other than keys and count")));
    }
    Ok(Collection {
        kind,
        version,
        count,
    })
}

/// The collection at `root`, or `None` when there is no root record.
///
/// # Errors
/// [`ApplyError::KindMismatch`] when the object is a document; [`ValueError::Corrupt`] when the
/// root does not open or decode.
pub fn collection(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
) -> Result<Option<Collection>, ValueError> {
    record(snapshot, root.as_bytes())?
        .map(|(version, bytes)| open_root(version, &bytes))
        .transpose()
}

/// The collection at `root`, which must exist.
fn existing(snapshot: &dyn SnapshotRead, root: &RootKey) -> Result<Collection, ValueError> {
    collection(snapshot, root)?.ok_or_else(|| ApplyError::ObjectAbsent.into())
}

/// Whether any element record exists under `root` (one limit-1 scan).
fn any_element(snapshot: &dyn SnapshotRead, root: &RootKey) -> bool {
    let prefix = root.element_prefix();
    snapshot
        .scan(Namespace::User, &prefix, 1)
        .first()
        .is_some_and(|(key, _)| key.starts_with(&prefix))
}

/// One touched element: present before the ops, present now, and the last write, if an op
/// wrote it (`Some(None)` for a set member, `Some(Some(v))` for a map entry).
struct Touched {
    before: bool,
    present: bool,
    written: Option<Option<Value>>,
}

/// Compile `ops` against the collection at `root` into one root `Put`, one write per element
/// that changed, and the create condition (ADR-rdb-0013 decision 9).
///
/// - [`Expected::Version`]: the root must exist, be at that version and be `kind`.
/// - [`Expected::Absent`]: a create. The root `Put` carries [`Condition::Absent`]. When the root
///   is absent here, any element record under the object is [`Corrupt::OrphanElement`].
///
/// # Errors
/// [`ApplyError::ObjectAbsent`], [`ApplyError::VersionConflict`], [`ApplyError::KindMismatch`],
/// [`ApplyError::UnsupportedKeyType`], [`ApplyError::ElementAbsent`],
/// [`ApplyError::ElementExists`], [`ApplyError::TooManyWrites`], [`ApplyError::TooLarge`],
/// [`ApplyError::TooDeep`], or [`ValueError::Corrupt`].
pub fn compile_collection(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    kind: CollectionKind,
    expected: Expected,
    ops: &[ElemOp],
) -> Result<Compiled, ValueError> {
    let count_before = match expected {
        Expected::Absent => {
            let root_absent = snapshot.version(Namespace::User, root.as_bytes()).is_none();
            if root_absent && any_element(snapshot, root) {
                return Err(ValueError::Corrupt(Corrupt::OrphanElement));
            }
            0
        }
        Expected::Version(want) => {
            check_version(snapshot, root, want)?;
            let found = existing(snapshot, root)?;
            if found.kind != kind {
                return Err(ApplyError::KindMismatch {
                    found: found.kind.kind(),
                }
                .into());
            }
            found.count
        }
    };

    let mut touched: BTreeMap<Bytes, Touched> = BTreeMap::new();
    for op in ops {
        let key = match (op, kind) {
            (ElemOp::Put(..), CollectionKind::Set) | (ElemOp::Add(_), CollectionKind::Map) => {
                return Err(ApplyError::KindMismatch { found: kind.kind() }.into())
            }
            (
                ElemOp::Put(k, _)
                | ElemOp::Add(k)
                | ElemOp::Remove(k)
                | ElemOp::NeedPresent(k)
                | ElemOp::NeedAbsent(k),
                _,
            ) => k,
        };
        let full = element_key(root, key)?;
        let entry = match touched.entry(full) {
            BTreeEntry::Occupied(entry) => entry.into_mut(),
            BTreeEntry::Vacant(entry) => {
                let version = match expected {
                    Expected::Version(_) => snapshot.version(Namespace::User, entry.key()),
                    Expected::Absent => None,
                };
                // Lead ruling on tester W1 A2: a touched element newer than its root is the
                // same breach a read reports, so the write that would mask it is refused.
                if let (Some(element), Expected::Version(root)) = (version, expected) {
                    if element > root {
                        return Err(ValueError::Corrupt(Corrupt::ElementNewerThanRoot {
                            element,
                            root,
                        }));
                    }
                }
                entry.insert(Touched {
                    before: version.is_some(),
                    present: version.is_some(),
                    written: None,
                })
            }
        };
        match op {
            ElemOp::Put(_, v) => {
                entry.present = true;
                entry.written = Some(Some(v.clone()));
            }
            // ADR-rdb-0013 decision 9: a no-op if it is present (review Q-1).
            ElemOp::Add(_) if entry.present => {}
            ElemOp::Add(_) => {
                entry.present = true;
                entry.written = Some(None);
            }
            ElemOp::Remove(_) => {
                entry.present = false;
                entry.written = None;
            }
            ElemOp::NeedPresent(_) if !entry.present => {
                return Err(ApplyError::ElementAbsent.into())
            }
            ElemOp::NeedAbsent(_) if entry.present => return Err(ApplyError::ElementExists.into()),
            ElemOp::NeedPresent(_) | ElemOp::NeedAbsent(_) => {}
        }
    }

    let mut mutations = Vec::with_capacity(touched.len() + 1);
    let (mut added, mut removed) = (0_u64, 0_u64);
    for (key, element) in touched {
        match (element.before, element.present, element.written) {
            // Absent before and after, or only checked by a `need`: nothing to write.
            (false, false, _) | (_, true, None) => {}
            (true, false, _) => {
                removed += 1;
                mutations.push(Mutation::Delete {
                    key,
                    expected_version: None,
                });
            }
            (before, true, Some(value)) => {
                added += u64::from(!before);
                let value = match value {
                    None => Bytes::new(),
                    Some(v) => {
                        let payload = encode(&v).map_err(ApplyError::from)?;
                        seal(Kind::Document, &payload).map_err(ApplyError::from)?
                    }
                };
                mutations.push(Mutation::Put {
                    key,
                    value,
                    expected_version: None,
                });
            }
        }
    }
    // In i128, so only the result is range-checked: one in and one out at `u64::MAX` is fine.
    // Below 0, more were removed than the root counts: elements exist that it does not account
    // for. Above `u64::MAX`, the stored count cannot be right (tester D1).
    let count = i128::from(count_before) + i128::from(added) - i128::from(removed);
    let count = match u64::try_from(count) {
        Ok(count) => count,
        Err(_) if count < 0 => return Err(ValueError::Corrupt(Corrupt::OrphanElement)),
        Err(_) => {
            return Err(ValueError::Corrupt(Corrupt::Root(
                "count plus the elements added overflows u64",
            )))
        }
    };
    let root_value =
        seal(kind.kind(), &root_payload(count)).expect("a root payload is a few bytes");
    let (expected_version, conditions) = match expected {
        Expected::Absent => (
            None,
            vec![Condition::Absent {
                key: root.to_bytes(),
            }],
        ),
        Expected::Version(v) => (Some(v), Vec::new()),
    };
    // The root sorts before every element (sub 0x00 < 0x01), so this keeps key order.
    mutations.insert(
        0,
        Mutation::Put {
            key: root.to_bytes(),
            value: root_value,
            expected_version,
        },
    );
    if mutations.len() > MAX_REQUEST_MUTATIONS {
        return Err(ApplyError::TooManyWrites {
            writes: mutations.len(),
        }
        .into());
    }
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

/// Compile the deletion of the empty collection at `root`, at `version` (ADR-rdb-0013
/// decision 9). One limit-1 scan checks `count` against the elements.
///
/// # Errors
/// [`ApplyError::ObjectAbsent`], [`ApplyError::VersionConflict`], [`ApplyError::KindMismatch`]
/// for a document, [`ApplyError::NotEmpty`], [`Corrupt::CountMismatch`] (`count` > 0, no element)
/// or [`Corrupt::OrphanElement`] (`count` = 0, an element).
pub fn drop_collection(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    version: u64,
) -> Result<Compiled, ValueError> {
    check_version(snapshot, root, version)?;
    let found = existing(snapshot, root)?;
    match (found.count > 0, any_element(snapshot, root)) {
        (false, false) => Ok(Compiled {
            mutations: vec![Mutation::Delete {
                key: root.to_bytes(),
                expected_version: Some(version),
            }],
            conditions: Vec::new(),
        }),
        (true, true) => Err(ApplyError::NotEmpty { count: found.count }.into()),
        (true, false) => Err(ValueError::Corrupt(Corrupt::CountMismatch {
            count: found.count,
        })),
        (false, true) => Err(ValueError::Corrupt(Corrupt::OrphanElement)),
    }
}

/// Check one element record read under `root` and return its value (`None` for a set member).
fn element_value(
    root: &Collection,
    version: u64,
    bytes: &[u8],
) -> Result<Option<Value>, ValueError> {
    let corrupt = ValueError::Corrupt;
    if version > root.version {
        return Err(corrupt(Corrupt::ElementNewerThanRoot {
            element: version,
            root: root.version,
        }));
    }
    match root.kind {
        CollectionKind::Set if bytes.is_empty() => Ok(None),
        CollectionKind::Set => Err(corrupt(Corrupt::SetMemberHasValue { len: bytes.len() })),
        CollectionKind::Map => {
            let opened = open(bytes).map_err(|e| corrupt(Corrupt::Envelope(e)))?;
            if opened.kind != Kind::Document {
                return Err(corrupt(Corrupt::EntryNotDocument { found: opened.kind }));
            }
            let value = decode(opened.payload).map_err(|e| corrupt(Corrupt::Codec(e)))?;
            Ok(Some(value))
        }
    }
}

/// The element `key` of the collection at `root`, or `None` when it is absent.
///
/// # Errors
/// [`ApplyError::ObjectAbsent`] when there is no root; [`ApplyError::KindMismatch`] for a
/// document; [`ApplyError::UnsupportedKeyType`]; [`ValueError::Corrupt`].
pub fn member(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    key: &Value,
) -> Result<Option<Member>, ValueError> {
    let found = existing(snapshot, root)?;
    let full = element_key(root, key)?;
    record(snapshot, &full)?
        .map(|(version, bytes)| {
            Ok(Member {
                key: key.clone(),
                value: element_value(&found, version, &bytes)?,
                version,
            })
        })
        .transpose()
}

/// Up to `limit` elements of the collection at `root`, in element order, starting after `after`
/// (which need not be an element) or at the first.
///
/// # Errors
/// [`ApplyError::ObjectAbsent`] when there is no root; [`ApplyError::KindMismatch`] for a
/// document; [`ApplyError::UnsupportedKeyType`] for `after`; [`ValueError::Corrupt`] for the
/// first element that does not read back.
pub fn members(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    after: Option<&Value>,
    limit: usize,
) -> Result<Members, ValueError> {
    let found = existing(snapshot, root)?;
    let prefix = root.element_prefix();
    let mut from = prefix.clone();
    if let Some(after) = after {
        // Element keys are prefix-free, so the next key after `k` is `k | 0x00`.
        from.extend_from_slice(&encode_element(after)?);
        from.push(0x00);
    }
    let mut out = Vec::new();
    for (key, bytes) in snapshot.scan(Namespace::User, &from, limit) {
        if !key.starts_with(&prefix) {
            break;
        }
        let element =
            decode_element_key(root, &key).map_err(|e| ValueError::Corrupt(Corrupt::Key(e)))?;
        let version = snapshot
            .version(Namespace::User, &key)
            .ok_or(ValueError::Corrupt(Corrupt::VersionWithoutValue))?;
        out.push(Member {
            key: element,
            value: element_value(&found, version, &bytes)?,
            version,
        });
    }
    Ok(Members {
        collection: found,
        members: out,
    })
}
