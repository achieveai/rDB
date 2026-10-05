//! Ordered lists: a root, one record per item, and pages (ADR-rdb-0016).
//!
//! - The root, at the object's [`RootKey`], is an envelope of kind [`Kind::List`] whose payload is
//!   canonical CBOR of exactly `{next, tree, bytes, count}`: the id counter, the top node of the
//!   tree, and the items' count and stored bytes (decision 3).
//! - An item, at [`item_key`], is a document envelope holding the item's value.
//! - A page, at [`page_key`], is a node below the top, kind [`Kind::ListPage`].
//! - [`compile_list`] turns positional ops into one root `Put` plus one write per item or page
//!   that changed, in key order, with the snapshot's generation attached (decision 8).
//!
//! The top node is a leaf here: a list holds as many items as one node of `node_max` bytes
//! names. Splitting a node into pages is not built yet, and a compile that would need it stops
//! loudly rather than write a node over `node_max`.
//!
//! Reads go through `&dyn SnapshotRead` only. Nothing is repaired: damage is a named
//! [`Corrupt`].

use std::collections::BTreeMap;

use bytes::Bytes;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::admission::MAX_REQUEST_MUTATIONS;
use rdb_core::transaction::record_len;
use rdb_core::{Condition, Generation, Mutation, Namespace, SnapshotRead};

use crate::cbor::{decode, encode};
use crate::compile::{check_version, record, Compiled, Corrupt, Expected, ValueError};
use crate::delta::{ApplyError, SizeLimit};
use crate::envelope::{open, seal, Kind};
use crate::keys::{item_key, RootKey, LIST_ID_LEN, SUB_ITEM, SUB_PAGE};
use crate::value::{Int, Map, MapKey, Value};

/// The node size rDB writes with: a node over it splits (ADR-rdb-0016 §4). A compile argument,
/// never stored.
pub const DEFAULT_NODE_MAX: usize = 24_576;
/// The smallest `node_max` a compile takes: a node over it holds at least 4 entries, so both
/// halves of a split are non-empty (ADR-rdb-0016 §4).
pub const MIN_NODE_MAX: usize = 128;
/// The largest item envelope a write takes (ADR-rdb-0016 §4). A write limit, not a format one:
/// reads never check it.
pub const MAX_ITEM: usize = 524_288;
/// The highest level a node has: a tree is at most 8 high (ADR-rdb-0016 §4).
pub const MAX_LEVEL: u64 = 7;

// One insert or replace on a list of height 8 at the default node size, with the shortest
// object id, fits one request (ADR-rdb-0016 §4).
const _: () = assert!(1_532 + 16 * 2 + 15 * DEFAULT_NODE_MAX + MAX_ITEM <= MAX_ENVELOPE_BYTES);

/// One positional op (ADR-rdb-0016 §5). Ops apply in order; positions are 0-based and read the
/// list as the earlier ops left it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListOp {
    /// Add the value at the end.
    Push(Value),
    /// Add the value at position `at`, `at ≤ count`; the items from `at` on move up one.
    Insert {
        /// The new item's position.
        at: u64,
        /// The new item's value.
        value: Value,
    },
}

/// A compiled list write: the writes, the ids it minted, and the generation of the snapshot it
/// was compiled from. A request built from it must name that generation (ADR-rdb-0016 §8), so
/// the three are returned together and only this module makes one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListCompiled {
    compiled: Compiled,
    ids: Vec<u128>,
    generation: Generation,
}

impl ListCompiled {
    /// The writes and their conditions.
    #[must_use]
    pub const fn compiled(&self) -> &Compiled {
        &self.compiled
    }

    /// The item ids the ops minted, in op order.
    #[must_use]
    pub fn ids(&self) -> &[u128] {
        &self.ids
    }

    /// The snapshot's generation: the request's `expected_generation`.
    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }
}

/// A list's root, read back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct List {
    /// The root record's storage version: the `seq` of the last transaction that changed it.
    pub version: u64,
    /// How many items it holds.
    pub count: u64,
    /// The items' stored bytes, envelopes included. Kept by every compile, not checked by reads.
    pub bytes: u64,
    /// The tree's height: 1 when the root's top node is a leaf.
    pub height: u8,
}

/// One item, read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    /// Its position in the list.
    pub position: u64,
    /// Its stable id.
    pub id: u128,
    /// Its value.
    pub value: Value,
    /// The item record's storage version. Not a concurrency token.
    pub version: u64,
}

/// Where a later [`items`] call resumes: valid only under the same generation and root version
/// (ADR-rdb-0016 §6). Not stored; its shape can change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    /// The generation of the snapshot it was read from.
    pub generation: Generation,
    /// The root's version it was read under.
    pub version: u64,
    /// The next position to read.
    pub position: u64,
}

/// Where [`items`] starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Start {
    /// At this position, under whatever version the snapshot holds.
    Position(u64),
    /// Where an earlier page stopped.
    Token(Token),
}

/// A page of items, in list order, with the root it was read under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Items {
    /// The root.
    pub list: List,
    /// The items read.
    pub items: Vec<Item>,
    /// Where to resume, when items remain after these.
    pub next: Option<Token>,
}

/// A root payload, decoded.
struct Root {
    /// The next id to mint.
    next: u128,
    /// The top node, a leaf: the item ids in list order.
    ids: Vec<u128>,
    bytes: u64,
    count: u64,
}

fn uint(v: u64) -> Value {
    Value::Integer(Int::from(v))
}

fn id_value(id: u128) -> Value {
    Value::Bytes(id.to_be_bytes().to_vec())
}

/// A leaf node: `{ids: [...], level: 0}`.
fn leaf(ids: &[u128]) -> Value {
    let mut node = Map::new();
    node.insert(
        MapKey::new("ids"),
        Value::Array(ids.iter().copied().map(id_value).collect()),
    );
    node.insert(MapKey::new("level"), uint(0));
    Value::Map(node)
}

/// The root payload: `{next, tree, bytes, count}`.
fn root_payload(root: &Root) -> Result<Vec<u8>, ApplyError> {
    let mut map = Map::new();
    map.insert(MapKey::new("next"), id_value(root.next));
    map.insert(MapKey::new("tree"), leaf(&root.ids));
    map.insert(MapKey::new("bytes"), uint(root.bytes));
    map.insert(MapKey::new("count"), uint(root.count));
    encode(&Value::Map(map)).map_err(ApplyError::from)
}

fn corrupt_root(what: &'static str) -> ValueError {
    ValueError::Corrupt(Corrupt::Root(what))
}

/// A 16-byte id, or `None`.
fn as_id(value: &Value) -> Option<u128> {
    match value {
        Value::Bytes(b) => <[u8; LIST_ID_LEN]>::try_from(b.as_slice())
            .ok()
            .map(u128::from_be_bytes),
        _ => None,
    }
}

/// A non-negative integer that fits u64, or `None`.
fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Integer(i) => u64::try_from(i.get()).ok(),
        _ => None,
    }
}

/// Open a root record. Another kind is an [`ApplyError::KindMismatch`].
fn open_root(bytes: &[u8]) -> Result<Root, ValueError> {
    let opened = open(bytes).map_err(|e| ValueError::Corrupt(Corrupt::Envelope(e)))?;
    if opened.kind != Kind::List {
        return Err(ApplyError::KindMismatch { found: opened.kind }.into());
    }
    let payload = decode(opened.payload).map_err(|e| ValueError::Corrupt(Corrupt::Codec(e)))?;
    let Value::Map(fields) = payload else {
        return Err(corrupt_root("the list root's payload is not a map"));
    };
    let field = |name: &str| fields.get(&MapKey::new(name));
    let (Some(next), Some(tree), Some(bytes), Some(count)) =
        (field("next"), field("tree"), field("bytes"), field("count"))
    else {
        return Err(corrupt_root(
            "the list root is not exactly next, tree, bytes and count",
        ));
    };
    if fields.len() != 4 {
        return Err(corrupt_root(
            "the list root is not exactly next, tree, bytes and count",
        ));
    }
    let next = as_id(next).ok_or(corrupt_root("the list root's next is not a 16-byte id"))?;
    let bytes = as_u64(bytes).ok_or(corrupt_root(
        "the list root's bytes is not an unsigned integer",
    ))?;
    let count = as_u64(count).ok_or(corrupt_root(
        "the list root's count is not an unsigned integer",
    ))?;
    let ids = top_node(tree)?;
    if u64::try_from(ids.len()).ok() != Some(count) {
        return Err(corrupt_root("the list root's count is not its tree's"));
    }
    Ok(Root {
        next,
        ids,
        bytes,
        count,
    })
}

/// The root's top node. Only a leaf is read here; a node of a higher level names pages.
fn top_node(tree: &Value) -> Result<Vec<u128>, ValueError> {
    let Value::Map(node) = tree else {
        return Err(corrupt_root("the list root's tree is not a node"));
    };
    let level = node
        .get(&MapKey::new("level"))
        .and_then(as_u64)
        .ok_or(corrupt_root(
            "a list node's level is not an unsigned integer",
        ))?;
    if level > MAX_LEVEL {
        return Err(corrupt_root("a list node's level is over 7"));
    }
    if level > 0 {
        unimplemented!("reading a list whose top node names pages is not built yet");
    }
    let (Some(Value::Array(ids)), 2) = (node.get(&MapKey::new("ids")), node.len()) else {
        return Err(corrupt_root("a list leaf is not exactly ids and level"));
    };
    ids.iter()
        .map(|id| as_id(id).ok_or(corrupt_root("a list leaf names an id that is not 16 bytes")))
        .collect()
}

/// The list at `root`, or `None` when there is no root record.
///
/// # Errors
/// [`ApplyError::KindMismatch`] when the object is not a list; [`ValueError::Corrupt`] when the
/// root does not read back.
pub fn list(snapshot: &dyn SnapshotRead, root: &RootKey) -> Result<Option<List>, ValueError> {
    record(snapshot, root.as_bytes())?
        .map(|(version, bytes)| open_root(&bytes).map(|found| summary(version, &found)))
        .transpose()
}

fn summary(version: u64, root: &Root) -> List {
    List {
        version,
        count: root.count,
        bytes: root.bytes,
        height: 1,
    }
}

/// Up to `limit` items of the list at `root`, in list order, from `start`. Every item returned
/// is read and checked before any is returned.
///
/// # Errors
/// [`ApplyError::ObjectAbsent`]; [`ApplyError::KindMismatch`]; for a token,
/// [`ApplyError::GenerationChanged`] or [`ApplyError::VersionConflict`];
/// [`ApplyError::PositionInvalid`] past the end; [`ValueError::Corrupt`].
pub fn items(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    start: Start,
    limit: usize,
) -> Result<Items, ValueError> {
    let (version, bytes) = record(snapshot, root.as_bytes())?.ok_or(ApplyError::ObjectAbsent)?;
    let found = open_root(&bytes)?;
    let position = match start {
        Start::Position(p) => p,
        Start::Token(token) => {
            let current = snapshot.generation();
            if token.generation != current {
                return Err(ApplyError::GenerationChanged {
                    expected: token.generation.0,
                    found: current.0,
                }
                .into());
            }
            if token.version != version {
                return Err(ApplyError::VersionConflict {
                    expected: token.version,
                    found: version,
                }
                .into());
            }
            token.position
        }
    };
    if position > found.count {
        return Err(ApplyError::PositionInvalid {
            position,
            len: found.count,
        }
        .into());
    }
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    let end = found.count.min(position.saturating_add(limit));
    let mut out = Vec::new();
    for p in position..end {
        let id = found.ids[usize::try_from(p).expect("a position below count indexes ids")];
        let (value, item_version) = read_item(snapshot, root, id, version)?;
        out.push(Item {
            position: p,
            id,
            value,
            version: item_version,
        });
    }
    let next = (end < found.count).then_some(Token {
        generation: snapshot.generation(),
        version,
        position: end,
    });
    Ok(Items {
        list: summary(version, &found),
        items: out,
        next,
    })
}

/// Read and check the item `id` under a root at `root_version`.
fn read_item(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    id: u128,
    root_version: u64,
) -> Result<(Value, u64), ValueError> {
    let corrupt = ValueError::Corrupt;
    let (version, bytes) =
        record(snapshot, &item_key(root, id))?.ok_or(corrupt(Corrupt::ItemMissing { id }))?;
    if version > root_version {
        return Err(corrupt(Corrupt::ElementNewerThanRoot {
            element: version,
            root: root_version,
        }));
    }
    let opened = open(&bytes).map_err(|e| corrupt(Corrupt::Envelope(e)))?;
    if opened.kind != Kind::Document {
        return Err(corrupt(Corrupt::EntryNotDocument { found: opened.kind }));
    }
    let value = decode(opened.payload).map_err(|e| corrupt(Corrupt::Codec(e)))?;
    Ok((value, version))
}

/// Whether a record exists under `root`'s item or page range (one limit-1 scan). A key after
/// both ranges, such as a chunk or a neighbouring object, does not count (ADR-rdb-0016 §1).
fn any_item_or_page(snapshot: &dyn SnapshotRead, root: &RootKey) -> bool {
    let (items, pages) = (root.sub_prefix(SUB_ITEM), root.sub_prefix(SUB_PAGE));
    snapshot
        .scan(Namespace::User, &items, 1)
        .first()
        .is_some_and(|(key, _)| key.starts_with(&items) || key.starts_with(&pages))
}

/// Compile `ops` against the list at `root` into one root `Put` and one write per item that
/// changed, in key order (ADR-rdb-0016 §5).
///
/// - [`Expected::Version`]: the root must exist, be at that version and be a list.
/// - [`Expected::Absent`]: a create, seeded with `next = snapshot.at() << 64`. The root `Put`
///   carries [`Condition::Absent`]. When the root is absent here, any record under the item or
///   page range is [`Corrupt::OrphanElement`].
///
/// # Errors
/// [`ApplyError::InvalidNodeSize`], [`ApplyError::ObjectAbsent`], [`ApplyError::VersionConflict`],
/// [`ApplyError::KindMismatch`], [`ApplyError::PositionInvalid`], [`ApplyError::TooLarge`],
/// [`ApplyError::TooDeep`], [`ApplyError::TooManyWrites`], or [`ValueError::Corrupt`].
///
/// # Panics
/// When the top node would grow past `node_max`: splitting it into pages is not built yet.
pub fn compile_list(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    expected: Expected,
    node_max: usize,
    ops: &[ListOp],
) -> Result<ListCompiled, ValueError> {
    if !(MIN_NODE_MAX..=DEFAULT_NODE_MAX).contains(&node_max) {
        return Err(ApplyError::InvalidNodeSize { found: node_max }.into());
    }
    let (mut state, expected_version, conditions) = match expected {
        Expected::Absent => {
            let root_absent = snapshot.version(Namespace::User, root.as_bytes()).is_none();
            if root_absent && any_item_or_page(snapshot, root) {
                return Err(ValueError::Corrupt(Corrupt::OrphanElement));
            }
            let fresh = Root {
                next: u128::from(snapshot.at().0) << 64,
                ids: Vec::new(),
                bytes: 0,
                count: 0,
            };
            let absent = Condition::Absent {
                key: root.to_bytes(),
            };
            (fresh, None, vec![absent])
        }
        Expected::Version(want) => {
            check_version(snapshot, root, want)?;
            let (_, bytes) = record(snapshot, root.as_bytes())?.ok_or(ApplyError::ObjectAbsent)?;
            (open_root(&bytes)?, Some(want), Vec::new())
        }
    };

    let mut writes: BTreeMap<Bytes, Bytes> = BTreeMap::new();
    let mut minted = Vec::new();
    for op in ops {
        let (at, value) = match op {
            ListOp::Push(value) => (state.count, value),
            ListOp::Insert { at, value } => (*at, value),
        };
        if at > state.count {
            return Err(ApplyError::PositionInvalid {
                position: at,
                len: state.count,
            }
            .into());
        }
        let payload = encode(value).map_err(ApplyError::from)?;
        let envelope = seal(Kind::Document, &payload).map_err(ApplyError::from)?;
        if envelope.len() > MAX_ITEM {
            return Err(ApplyError::TooLarge {
                limit: SizeLimit::Item,
            }
            .into());
        }
        let id = state.next;
        state.next = id.checked_add(1).ok_or(corrupt_root(
            "the list root's next id is the largest there is",
        ))?;
        let key = item_key(root, id);
        if snapshot.version(Namespace::User, &key).is_some() {
            return Err(ValueError::Corrupt(Corrupt::OrphanElement));
        }
        state.count = state
            .count
            .checked_add(1)
            .ok_or(corrupt_root("the list root's count overflows u64"))?;
        state.bytes = state
            .bytes
            .checked_add(u64::try_from(envelope.len()).expect("an envelope fits u64"))
            .ok_or(corrupt_root("the list root's bytes overflows u64"))?;
        state.ids.insert(
            usize::try_from(at).expect("a position at most count indexes ids"),
            id,
        );
        let node = encode(&leaf(&state.ids)).map_err(ApplyError::from)?;
        if node.len() > node_max {
            unimplemented!(
                "the top node is {} bytes, over node_max {node_max}; splitting it into pages \
                 is not built yet",
                node.len()
            );
        }
        writes.insert(key, envelope);
        minted.push(id);
    }

    let root_value = seal(Kind::List, &root_payload(&state)?).map_err(ApplyError::from)?;
    // The root sorts before every item (sub 0x00 < 0x02), so this keeps key order.
    let mut mutations = vec![Mutation::Put {
        key: root.to_bytes(),
        value: root_value,
        expected_version,
    }];
    mutations.extend(writes.into_iter().map(|(key, value)| Mutation::Put {
        key,
        value,
        expected_version: None,
    }));
    if mutations.len() > MAX_REQUEST_MUTATIONS {
        return Err(ApplyError::TooManyWrites {
            writes: mutations.len(),
        }
        .into());
    }
    // The kernel's own measure of the record it would ship (L-R186v).
    if record_len(conditions.len(), &mutations) > MAX_ENVELOPE_BYTES {
        return Err(ApplyError::TooLarge {
            limit: SizeLimit::Write,
        }
        .into());
    }
    Ok(ListCompiled {
        compiled: Compiled {
            mutations,
            conditions,
        },
        ids: minted,
        generation: snapshot.generation(),
    })
}

/// Compile the deletion of the empty list at `root`, at `version` (ADR-rdb-0016 §5).
///
/// # Errors
/// [`ApplyError::ObjectAbsent`], [`ApplyError::VersionConflict`], [`ApplyError::KindMismatch`],
/// [`ApplyError::NotEmpty`], [`Corrupt::OrphanElement`] (an item or page record under an empty
/// list), or any other [`ValueError::Corrupt`] of the root.
pub fn drop_list(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    version: u64,
) -> Result<ListCompiled, ValueError> {
    check_version(snapshot, root, version)?;
    let (_, bytes) = record(snapshot, root.as_bytes())?.ok_or(ApplyError::ObjectAbsent)?;
    let found = open_root(&bytes)?;
    if found.count > 0 {
        return Err(ApplyError::NotEmpty { count: found.count }.into());
    }
    if any_item_or_page(snapshot, root) {
        return Err(ValueError::Corrupt(Corrupt::OrphanElement));
    }
    Ok(ListCompiled {
        compiled: Compiled {
            mutations: vec![Mutation::Delete {
                key: root.to_bytes(),
                expected_version: Some(version),
            }],
            conditions: Vec::new(),
        },
        ids: Vec::new(),
        generation: snapshot.generation(),
    })
}
