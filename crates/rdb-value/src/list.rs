//! Ordered lists: a root, one record per item, and pages (ADR-rdb-0016).
//!
//! - The root, at the object's [`RootKey`], is an envelope of kind [`Kind::List`] whose payload is
//!   canonical CBOR of exactly `{next, tree, bytes, count}`: the id counter, the top node of the
//!   tree, and the items' count and stored bytes (decision 3).
//! - An item, at [`item_key`], is a document envelope holding the item's value.
//! - A page, at [`page_key`], is a node below the top, kind [`Kind::ListPage`].
//! - The tree is an order-statistic B+ tree: a leaf names item ids in list order; an internal
//!   node names its kids with each kid's item count and stored bytes.
//! - [`compile_list`] turns positional ops into one root `Put` plus one write per item or page
//!   that changed, in key order, with the snapshot's generation attached (decision 8).
//!
//! Reads go through `&dyn SnapshotRead` only. Every page is checked before it is used, and
//! nothing is repaired: damage is a named [`Corrupt`].

use std::collections::BTreeMap;

use bytes::Bytes;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::admission::MAX_REQUEST_MUTATIONS;
use rdb_core::transaction::record_len;
use rdb_core::{Condition, Generation, Mutation, Namespace, SnapshotRead};

use crate::cbor::{decode, encode};
use crate::compile::{check_version, record, Compiled, Corrupt, Expected, PageFault, ValueError};
use crate::delta::{ApplyError, SizeLimit};
use crate::envelope::{open, seal, Kind};
use crate::keys::{item_key, page_key, RootKey, LIST_ID_LEN, SUB_ITEM, SUB_PAGE};
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
pub const MAX_LEVEL: u8 = 7;

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
    /// Remove the item at `at`, `at < count`; the items after it move down one.
    Remove {
        /// The item's position.
        at: u64,
    },
    /// Replace the value of the item at `at`, `at < count`. Its id stays.
    Replace {
        /// The item's position.
        at: u64,
        /// Its new value.
        value: Value,
    },
    /// Move the item at `from` so that it ends at `to`; both `< count`. Its id and its record
    /// stay; only pages are written.
    Move {
        /// The item's position now.
        from: u64,
        /// Its position after the move.
        to: u64,
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

// ---- nodes ---------------------------------------------------------------------------------

/// An internal node's entry for one kid: the kid's page id, item count and stored bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Kid {
    id: u128,
    count: u64,
    bytes: u64,
}

/// A kid entry's `(count, bytes)`.
type Entry = (u64, u64);

/// A tree node (ADR-rdb-0016 §3): the root's top node, or a page's payload.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
    /// Level 0: item ids in list order.
    Leaf(Vec<u128>),
    /// Level 1 to 7: kids in list order.
    Internal { level: u8, kids: Vec<Kid> },
}

impl Node {
    const fn level(&self) -> u8 {
        match self {
            Self::Leaf(_) => 0,
            Self::Internal { level, .. } => *level,
        }
    }

    fn entries(&self) -> usize {
        match self {
            Self::Leaf(ids) => ids.len(),
            Self::Internal { kids, .. } => kids.len(),
        }
    }

    /// The items under it, or `None` when that overflows `u64`.
    fn count(&self) -> Option<u64> {
        match self {
            Self::Leaf(ids) => u64::try_from(ids.len()).ok(),
            Self::Internal { kids, .. } => kids
                .iter()
                .try_fold(0_u64, |total, kid| total.checked_add(kid.count)),
        }
    }

    fn entry_values(&self) -> Vec<Value> {
        match self {
            Self::Leaf(ids) => ids.iter().copied().map(id_value).collect(),
            Self::Internal { kids, .. } => kids
                .iter()
                .map(|kid| Value::Array(vec![id_value(kid.id), uint(kid.count), uint(kid.bytes)]))
                .collect(),
        }
    }

    /// `{ids: [...], level: 0}` or `{kids: [[id, count, bytes]...], level: L}`.
    fn value(&self) -> Value {
        let mut node = Map::new();
        let name = match self {
            Self::Leaf(_) => "ids",
            Self::Internal { .. } => "kids",
        };
        node.insert(MapKey::new(name), Value::Array(self.entry_values()));
        node.insert(MapKey::new("level"), uint(u64::from(self.level())));
        Value::Map(node)
    }

    fn encoded(&self) -> Result<Vec<u8>, ApplyError> {
        encode(&self.value()).map_err(ApplyError::from)
    }

    fn size(&self) -> Result<usize, ApplyError> {
        self.encoded().map(|bytes| bytes.len())
    }

    /// Keep the first `at` entries; return the rest as a node of the same level.
    fn split_off(&mut self, at: usize) -> Self {
        match self {
            Self::Leaf(ids) => Self::Leaf(ids.split_off(at)),
            Self::Internal { level, kids } => Self::Internal {
                level: *level,
                kids: kids.split_off(at),
            },
        }
    }

    /// Append `other`'s entries; both are at one level.
    fn append(&mut self, other: Self) {
        match (self, other) {
            (Self::Leaf(ids), Self::Leaf(more)) => ids.extend(more),
            (Self::Internal { kids, .. }, Self::Internal { kids: more, .. }) => kids.extend(more),
            _ => unreachable!("siblings are at one level"),
        }
    }
}

/// Split `node` into two byte-balanced halves (ADR-rdb-0016 §4): the cut that makes the two
/// halves' encoded entries closest in size, the first such cut on a tie. `node` keeps the left
/// half; the right is returned.
fn balanced_split(node: &mut Node) -> Result<Node, ApplyError> {
    let sizes = node
        .entry_values()
        .iter()
        .map(|entry| encode(entry).map(|bytes| bytes.len()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApplyError::from)?;
    assert!(
        sizes.len() >= 2,
        "a node over node_max, or a pair over two thirds of it, has at least 2 entries"
    );
    let total: usize = sizes.iter().sum();
    let (mut best, mut cut, mut prefix) = (usize::MAX, 1, 0);
    for (i, size) in sizes.iter().enumerate().take(sizes.len() - 1) {
        prefix += size;
        let gap = prefix.abs_diff(total - prefix);
        if gap < best {
            (best, cut) = (gap, i + 1);
        }
    }
    Ok(node.split_off(cut))
}

fn uint(v: u64) -> Value {
    Value::Integer(Int::from(v))
}

fn id_value(id: u128) -> Value {
    Value::Bytes(id.to_be_bytes().to_vec())
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

const KID_SHAPE: &str = "a list kid is not [16-byte id, count, bytes]";

/// Decode a node, checking only what the node says of itself. `Err` names what is wrong.
fn parse_node(value: &Value) -> Result<Node, &'static str> {
    let Value::Map(node) = value else {
        return Err("a list node is not a map");
    };
    let level = node
        .get(&MapKey::new("level"))
        .and_then(as_u64)
        .ok_or("a list node's level is not an unsigned integer")?;
    if level > u64::from(MAX_LEVEL) {
        return Err("a list node's level is over 7");
    }
    let level = u8::try_from(level).expect("a level of at most 7 fits u8");
    if level == 0 {
        let (Some(Value::Array(ids)), 2) = (node.get(&MapKey::new("ids")), node.len()) else {
            return Err("a list leaf is not exactly ids and level");
        };
        let ids = ids
            .iter()
            .map(|id| as_id(id).ok_or("a list leaf names an id that is not 16 bytes"))
            .collect::<Result<_, _>>()?;
        return Ok(Node::Leaf(ids));
    }
    let (Some(Value::Array(kids)), 2) = (node.get(&MapKey::new("kids")), node.len()) else {
        return Err("a list internal node is not exactly kids and level");
    };
    let kids = kids.iter().map(parse_kid).collect::<Result<_, _>>()?;
    Ok(Node::Internal { level, kids })
}

fn parse_kid(value: &Value) -> Result<Kid, &'static str> {
    let Value::Array(parts) = value else {
        return Err(KID_SHAPE);
    };
    let [id, count, bytes] = parts.as_slice() else {
        return Err(KID_SHAPE);
    };
    let kid = Kid {
        id: as_id(id).ok_or(KID_SHAPE)?,
        count: as_u64(count).ok_or(KID_SHAPE)?,
        bytes: as_u64(bytes).ok_or(KID_SHAPE)?,
    };
    if kid.count == 0 {
        return Err("a list kid has count 0");
    }
    Ok(kid)
}

// ---- the root and pages ----------------------------------------------------------------------

/// A root payload, decoded.
struct Root {
    /// The next id to mint.
    next: u128,
    top: Node,
    bytes: u64,
    count: u64,
}

/// The root payload: `{next, tree, bytes, count}`.
fn root_payload(next: u128, top: &Node, bytes: u64, count: u64) -> Result<Vec<u8>, ApplyError> {
    let mut map = Map::new();
    map.insert(MapKey::new("next"), id_value(next));
    map.insert(MapKey::new("tree"), top.value());
    map.insert(MapKey::new("bytes"), uint(bytes));
    map.insert(MapKey::new("count"), uint(count));
    encode(&Value::Map(map)).map_err(ApplyError::from)
}

fn corrupt_root(what: &'static str) -> ValueError {
    ValueError::Corrupt(Corrupt::Root(what))
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
    let (Some(next), Some(tree), Some(bytes), Some(count), 4) = (
        field("next"),
        field("tree"),
        field("bytes"),
        field("count"),
        fields.len(),
    ) else {
        return Err(corrupt_root(
            "the list root is not exactly next, tree, bytes and count",
        ));
    };
    let next = as_id(next).ok_or(corrupt_root("the list root's next is not a 16-byte id"))?;
    let bytes = as_u64(bytes).ok_or(corrupt_root(
        "the list root's bytes is not an unsigned integer",
    ))?;
    let count = as_u64(count).ok_or(corrupt_root(
        "the list root's count is not an unsigned integer",
    ))?;
    let top = parse_node(tree).map_err(corrupt_root)?;
    if let Node::Internal { kids, .. } = &top {
        if kids.len() < 2 {
            return Err(corrupt_root(
                "the list root's top node has fewer than 2 kids",
            ));
        }
    }
    if top.count() != Some(count) {
        return Err(corrupt_root("the list root's count is not its tree's"));
    }
    Ok(Root {
        next,
        top,
        bytes,
        count,
    })
}

/// Read the page `id`, which a node one level up names with `count` items, and check it
/// (ADR-rdb-0016 §3) before anything uses it.
fn load_page(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    root_version: u64,
    id: u128,
    level: u8,
    count: u64,
) -> Result<Node, ValueError> {
    let fault = |fault| ValueError::Corrupt(Corrupt::Page { id, fault });
    let (version, bytes) =
        record(snapshot, &page_key(root, id))?.ok_or_else(|| fault(PageFault::Missing))?;
    if version > root_version {
        return Err(fault(PageFault::NewerThanRoot {
            page: version,
            root: root_version,
        }));
    }
    let opened = open(&bytes).map_err(|e| fault(PageFault::Envelope(e)))?;
    if opened.kind != Kind::ListPage {
        return Err(fault(PageFault::NotAPage { found: opened.kind }));
    }
    let payload = decode(opened.payload).map_err(|e| fault(PageFault::Codec(e)))?;
    let node = parse_node(&payload).map_err(|what| fault(PageFault::Shape(what)))?;
    if node.level() != level {
        return Err(fault(PageFault::Shape(
            "a list page's level is not one below its parent's",
        )));
    }
    if node.entries() == 0 {
        return Err(fault(PageFault::Shape("a list page is empty")));
    }
    if node.count() != Some(count) {
        return Err(fault(PageFault::Shape(
            "a list page's count is not its parent's entry",
        )));
    }
    Ok(node)
}

// ---- reads ---------------------------------------------------------------------------------

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
        height: root.top.level() + 1,
    }
}

/// Up to `limit` items of the list at `root`, in list order, from `start`. Every page on the way
/// and every item returned is read and checked before any is returned.
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
    let want = usize::try_from(end - position).expect("a page of ids fits memory");
    let mut ids = Vec::with_capacity(want);
    if want > 0 {
        let pages = Reader {
            snapshot,
            root,
            root_version: version,
            want,
        };
        pages.collect(&found.top, position, &mut ids)?;
    }
    let mut out = Vec::with_capacity(ids.len());
    for (p, id) in (position..).zip(ids) {
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

/// A positional read's walk over the tree: opens only the pages its positions are under.
struct Reader<'a> {
    snapshot: &'a dyn SnapshotRead,
    root: &'a RootKey,
    root_version: u64,
    want: usize,
}

impl Reader<'_> {
    /// Append to `out` the ids under `node` from its position `skip`, until `out` holds `want`.
    fn collect(&self, node: &Node, mut skip: u64, out: &mut Vec<u128>) -> Result<(), ValueError> {
        match node {
            Node::Leaf(ids) => {
                let from = usize::try_from(skip).map_or(ids.len(), |s| s.min(ids.len()));
                let room = self.want - out.len();
                out.extend(ids[from..].iter().take(room));
            }
            Node::Internal { level, kids } => {
                for kid in kids {
                    if out.len() >= self.want {
                        break;
                    }
                    if skip >= kid.count {
                        skip -= kid.count;
                        continue;
                    }
                    let child = load_page(
                        self.snapshot,
                        self.root,
                        self.root_version,
                        kid.id,
                        level - 1,
                        kid.count,
                    )?;
                    self.collect(&child, skip, out)?;
                    skip = 0;
                }
            }
        }
        Ok(())
    }
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

// ---- compile -------------------------------------------------------------------------------

/// A node in the compile's copy of the tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum At {
    Top,
    Page(u128),
}

/// A page the compile opened or minted. `node` is `None` once freed.
struct PageSlot {
    stored: bool,
    node: Option<Node>,
    dirty: bool,
}

/// An item the compile wrote. `value` is `None` once removed.
struct ItemSlot {
    stored: bool,
    value: Option<Bytes>,
}

/// The compile's copy of one list: the root's fields, every page it opened (each checked when
/// opened) or minted, and every item it wrote.
struct Tree<'a> {
    snapshot: &'a dyn SnapshotRead,
    root: &'a RootKey,
    root_version: u64,
    node_max: usize,
    next: u128,
    count: u64,
    bytes: u64,
    top: Node,
    pages: BTreeMap<u128, PageSlot>,
    items: BTreeMap<u128, ItemSlot>,
}

/// `v + delta`, or `None` outside `u64`.
fn shift(v: u64, delta: i128) -> Option<u64> {
    u64::try_from(i128::from(v) + delta).ok()
}

fn len_u64(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).expect("a record length fits u64")
}

impl Tree<'_> {
    fn node(&self, at: At) -> &Node {
        match at {
            At::Top => &self.top,
            At::Page(id) => self.pages[&id]
                .node
                .as_ref()
                .expect("a page the tree names is live"),
        }
    }

    fn node_mut(&mut self, at: At) -> &mut Node {
        match at {
            At::Top => &mut self.top,
            At::Page(id) => {
                let slot = self
                    .pages
                    .get_mut(&id)
                    .expect("a page the tree names is open");
                slot.dirty = true;
                slot.node.as_mut().expect("a page the tree names is live")
            }
        }
    }

    fn kids_mut(&mut self, at: At) -> &mut Vec<Kid> {
        match self.node_mut(at) {
            Node::Internal { kids, .. } => kids,
            Node::Leaf(_) => unreachable!("a node above another is internal"),
        }
    }

    fn kids(&self, at: At) -> &[Kid] {
        match self.node(at) {
            Node::Internal { kids, .. } => kids,
            Node::Leaf(_) => unreachable!("a node above another is internal"),
        }
    }

    fn set_page(&mut self, id: u128, node: Option<Node>) {
        let slot = self
            .pages
            .get_mut(&id)
            .expect("a page the tree names is open");
        slot.node = node;
        slot.dirty = true;
    }

    /// The next id from the counter items and pages share (ADR-rdb-0016 §2).
    fn mint(&mut self) -> Result<u128, ValueError> {
        let id = self.next;
        self.next = id.checked_add(1).ok_or(corrupt_root(
            "the list root's next id is the largest there is",
        ))?;
        Ok(id)
    }

    /// A new page holding `node`. A record already at its key is an orphan.
    fn mint_page(&mut self, node: Node) -> Result<u128, ValueError> {
        let id = self.mint()?;
        if self
            .snapshot
            .version(Namespace::User, &page_key(self.root, id))
            .is_some()
        {
            return Err(ValueError::Corrupt(Corrupt::OrphanElement));
        }
        self.pages.insert(
            id,
            PageSlot {
                stored: false,
                node: Some(node),
                dirty: true,
            },
        );
        Ok(id)
    }

    /// Open the page `kid` names, below a node at `level + 1`, unless this compile already has it.
    fn open_kid(&mut self, kid: Kid, level: u8) -> Result<(), ValueError> {
        if self.pages.contains_key(&kid.id) {
            return Ok(());
        }
        let node = load_page(
            self.snapshot,
            self.root,
            self.root_version,
            kid.id,
            level,
            kid.count,
        )?;
        self.pages.insert(
            kid.id,
            PageSlot {
                stored: true,
                node: Some(node),
                dirty: false,
            },
        );
        Ok(())
    }

    /// An item's stored length: from this compile's write, or read and checked (ADR-rdb-0016 §5).
    fn item_len(&self, id: u128) -> Result<u64, ValueError> {
        if let Some(slot) = self.items.get(&id) {
            let value = slot.value.as_ref().expect("an item the tree names is live");
            return Ok(len_u64(value));
        }
        let (version, bytes) = record(self.snapshot, &item_key(self.root, id))?
            .ok_or(ValueError::Corrupt(Corrupt::ItemMissing { id }))?;
        if version > self.root_version {
            return Err(ValueError::Corrupt(Corrupt::ElementNewerThanRoot {
                element: version,
                root: self.root_version,
            }));
        }
        Ok(len_u64(&bytes))
    }

    /// The path from the top to the leaf holding position `pos` (`pos = count` is the end), the
    /// kid index taken at each internal node, and the position inside the leaf. Every page on
    /// it is opened and checked.
    fn descend(&mut self, pos: u64) -> Result<(Vec<At>, Vec<usize>, usize), ValueError> {
        let (mut path, mut taken, mut rem) = (vec![At::Top], Vec::new(), pos);
        loop {
            let at = *path.last().expect("the path starts at the top");
            let Node::Internal { level, kids } = self.node(at) else {
                let leaf_pos = usize::try_from(rem).expect("a leaf position fits usize");
                return Ok((path, taken, leaf_pos));
            };
            let mut j = 0;
            while j + 1 < kids.len() && rem >= kids[j].count {
                rem -= kids[j].count;
                j += 1;
            }
            let (kid, level) = (kids[j], *level);
            self.open_kid(kid, level - 1)?;
            taken.push(j);
            path.push(At::Page(kid.id));
        }
    }

    /// Move every entry on the path, and the root's totals, by `count` items and `bytes`.
    fn adjust(
        &mut self,
        path: &[At],
        taken: &[usize],
        count: i128,
        bytes: i128,
    ) -> Result<(), ValueError> {
        if count == 0 && bytes == 0 {
            return Ok(());
        }
        for (k, &j) in taken.iter().enumerate() {
            let kid = &mut self.kids_mut(path[k])[j];
            kid.count =
                shift(kid.count, count).ok_or(corrupt_root("a list kid's count leaves u64"))?;
            kid.bytes =
                shift(kid.bytes, bytes).ok_or(corrupt_root("a list kid's bytes leaves u64"))?;
        }
        self.count =
            shift(self.count, count).ok_or(corrupt_root("the list root's count leaves u64"))?;
        self.bytes =
            shift(self.bytes, bytes).ok_or(corrupt_root("the list root's bytes leaves u64"))?;
        Ok(())
    }

    /// The `(count, bytes)` entries of `left` and `right`, the two halves of a node whose entry
    /// was `total`. A leaf's left bytes are summed from its items; the right is the rest.
    fn halves(
        &self,
        left: &Node,
        right: &Node,
        total: Entry,
    ) -> Result<(Entry, Entry), ValueError> {
        let overflow = || corrupt_root("a list node's count or bytes leaves u64");
        let (lc, rc) = (
            left.count().ok_or_else(overflow)?,
            right.count().ok_or_else(overflow)?,
        );
        let (lb, rb) = match (left, right) {
            (Node::Leaf(ids), Node::Leaf(_)) => {
                let mut lb = 0_u64;
                for id in ids {
                    lb = lb.checked_add(self.item_len(*id)?).ok_or_else(overflow)?;
                }
                let rb = total
                    .1
                    .checked_sub(lb)
                    .ok_or(corrupt_root("a list node's bytes is under its items'"))?;
                (lb, rb)
            }
            (Node::Internal { kids: l, .. }, Node::Internal { kids: r, .. }) => {
                let sum = |kids: &[Kid]| {
                    kids.iter()
                        .try_fold(0_u64, |total, kid| total.checked_add(kid.bytes))
                };
                (sum(l).ok_or_else(overflow)?, sum(r).ok_or_else(overflow)?)
            }
            _ => unreachable!("halves are at one level"),
        };
        Ok(((lc, lb), (rc, rb)))
    }

    /// Rebalance every node on `path`, leaf first (ADR-rdb-0016 §4): over `node_max`, split in
    /// two; under a third of it, take one sibling, the left if any, and merge when the pair
    /// fits two thirds, else split the pair evenly. Then the top: split it into two pages under
    /// a new top, or replace a top with one kid by that kid.
    fn rebalance(&mut self, path: &[At], taken: &[usize]) -> Result<(), ValueError> {
        let p = self.node_max;
        for k in (1..path.len()).rev() {
            let At::Page(id) = path[k] else {
                unreachable!("only the path's first node is the top")
            };
            let (parent, j) = (path[k - 1], taken[k - 1]);
            let size = self.node(path[k]).size()?;
            if size > p {
                let mut left = self.node(path[k]).clone();
                let right = balanced_split(&mut left)?;
                let entry = self.kids(parent)[j];
                let (l, r) = self.halves(&left, &right, (entry.count, entry.bytes))?;
                self.set_page(id, Some(left));
                let new_id = self.mint_page(right)?;
                let kids = self.kids_mut(parent);
                kids[j] = Kid {
                    id,
                    count: l.0,
                    bytes: l.1,
                };
                kids.insert(
                    j + 1,
                    Kid {
                        id: new_id,
                        count: r.0,
                        bytes: r.1,
                    },
                );
            } else if size * 3 < p && self.kids(parent).len() > 1 {
                let (lj, rj) = if j > 0 { (j - 1, j) } else { (j, j + 1) };
                let (lk, rk) = (self.kids(parent)[lj], self.kids(parent)[rj]);
                let level = self.node(path[k]).level();
                self.open_kid(if lj == j { rk } else { lk }, level)?;
                let mut pair = self.node(At::Page(lk.id)).clone();
                pair.append(self.node(At::Page(rk.id)).clone());
                let overflow = || corrupt_root("a list node's count or bytes leaves u64");
                let total = (
                    lk.count.checked_add(rk.count).ok_or_else(overflow)?,
                    lk.bytes.checked_add(rk.bytes).ok_or_else(overflow)?,
                );
                if pair.size()? * 3 <= 2 * p {
                    self.set_page(lk.id, Some(pair));
                    self.set_page(rk.id, None);
                    let kids = self.kids_mut(parent);
                    kids[lj] = Kid {
                        id: lk.id,
                        count: total.0,
                        bytes: total.1,
                    };
                    kids.remove(rj);
                } else {
                    let right = balanced_split(&mut pair)?;
                    let (l, r) = self.halves(&pair, &right, total)?;
                    self.set_page(lk.id, Some(pair));
                    self.set_page(rk.id, Some(right));
                    let kids = self.kids_mut(parent);
                    kids[lj] = Kid {
                        id: lk.id,
                        count: l.0,
                        bytes: l.1,
                    };
                    kids[rj] = Kid {
                        id: rk.id,
                        count: r.0,
                        bytes: r.1,
                    };
                }
            }
        }
        self.settle_top()
    }

    fn settle_top(&mut self) -> Result<(), ValueError> {
        loop {
            if self.top.size()? > self.node_max {
                let level = self.top.level();
                if level == MAX_LEVEL {
                    return Err(ApplyError::ListTooTall.into());
                }
                let mut left = std::mem::replace(&mut self.top, Node::Leaf(Vec::new()));
                let right = balanced_split(&mut left)?;
                let (l, r) = self.halves(&left, &right, (self.count, self.bytes))?;
                let left_id = self.mint_page(left)?;
                let right_id = self.mint_page(right)?;
                self.top = Node::Internal {
                    level: level + 1,
                    kids: vec![
                        Kid {
                            id: left_id,
                            count: l.0,
                            bytes: l.1,
                        },
                        Kid {
                            id: right_id,
                            count: r.0,
                            bytes: r.1,
                        },
                    ],
                };
                return Ok(());
            }
            let Node::Internal { level, kids } = &self.top else {
                return Ok(());
            };
            let ([kid], level) = (kids.as_slice(), *level) else {
                return Ok(());
            };
            let kid = *kid;
            self.open_kid(kid, level - 1)?;
            let node = self.node(At::Page(kid.id)).clone();
            self.set_page(kid.id, None);
            self.top = node;
        }
    }

    /// Put item `id`, `len` bytes, at `pos`.
    fn insert(&mut self, pos: u64, id: u128, len: u64) -> Result<(), ValueError> {
        let (path, taken, at) = self.descend(pos)?;
        let leaf = path.last().copied().expect("the path ends at a leaf");
        let Node::Leaf(ids) = self.node_mut(leaf) else {
            unreachable!("the path ends at a leaf")
        };
        ids.insert(at, id);
        self.adjust(&path, &taken, 1, i128::from(len))?;
        self.rebalance(&path, &taken)
    }

    /// Take the item at `pos` out of the tree; its record is not touched. Returns its id and
    /// stored length.
    fn take(&mut self, pos: u64) -> Result<(u128, u64), ValueError> {
        let (path, taken, at) = self.descend(pos)?;
        let leaf = path.last().copied().expect("the path ends at a leaf");
        let Node::Leaf(ids) = self.node(leaf) else {
            unreachable!("the path ends at a leaf")
        };
        let id = ids[at];
        let len = self.item_len(id)?;
        let Node::Leaf(ids) = self.node_mut(leaf) else {
            unreachable!("the path ends at a leaf")
        };
        ids.remove(at);
        self.adjust(&path, &taken, -1, -i128::from(len))?;
        self.rebalance(&path, &taken)?;
        Ok((id, len))
    }

    /// Give the item at `pos` the record `envelope`.
    fn replace(&mut self, pos: u64, envelope: Bytes) -> Result<(), ValueError> {
        let (path, taken, at) = self.descend(pos)?;
        let leaf = path.last().copied().expect("the path ends at a leaf");
        let Node::Leaf(ids) = self.node(leaf) else {
            unreachable!("the path ends at a leaf")
        };
        let id = ids[at];
        let old = self.item_len(id)?;
        let new = len_u64(&envelope);
        let stored = self.items.get(&id).is_none_or(|slot| slot.stored);
        self.items.insert(
            id,
            ItemSlot {
                stored,
                value: Some(envelope),
            },
        );
        self.adjust(&path, &taken, 0, i128::from(new) - i128::from(old))?;
        self.rebalance(&path, &taken)
    }

    /// Check `at` against the list as the earlier ops left it.
    fn check_position(&self, at: u64, inclusive: bool) -> Result<(), ValueError> {
        let inside = if inclusive {
            at <= self.count
        } else {
            at < self.count
        };
        if inside {
            Ok(())
        } else {
            Err(ApplyError::PositionInvalid {
                position: at,
                len: self.count,
            }
            .into())
        }
    }

    /// An item's record for `value`, within [`MAX_ITEM`].
    fn item_record(value: &Value) -> Result<Bytes, ValueError> {
        let payload = encode(value).map_err(ApplyError::from)?;
        let envelope = seal(Kind::Document, &payload).map_err(ApplyError::from)?;
        if envelope.len() > MAX_ITEM {
            return Err(ApplyError::TooLarge {
                limit: SizeLimit::Item,
            }
            .into());
        }
        Ok(envelope)
    }

    /// Run one op; returns the id it minted, if any.
    fn apply(&mut self, op: &ListOp) -> Result<Option<u128>, ValueError> {
        match op {
            ListOp::Push(value) => self.add(self.count, value).map(Some),
            ListOp::Insert { at, value } => self.add(*at, value).map(Some),
            ListOp::Remove { at } => {
                self.check_position(*at, false)?;
                let (id, _) = self.take(*at)?;
                match self.items.get_mut(&id) {
                    // Minted and freed in this compile: no record before or after.
                    Some(slot) if !slot.stored => {
                        self.items.remove(&id);
                    }
                    Some(slot) => slot.value = None,
                    None => {
                        self.items.insert(
                            id,
                            ItemSlot {
                                stored: true,
                                value: None,
                            },
                        );
                    }
                }
                Ok(None)
            }
            ListOp::Replace { at, value } => {
                self.check_position(*at, false)?;
                let envelope = Self::item_record(value)?;
                self.replace(*at, envelope)?;
                Ok(None)
            }
            ListOp::Move { from, to } => {
                self.check_position(*from, false)?;
                self.check_position(*to, false)?;
                let (id, len) = self.take(*from)?;
                self.insert(*to, id, len)?;
                Ok(None)
            }
        }
    }

    /// Push or insert `value` at `at` under a new id.
    fn add(&mut self, at: u64, value: &Value) -> Result<u128, ValueError> {
        self.check_position(at, true)?;
        let envelope = Self::item_record(value)?;
        let id = self.mint()?;
        if self
            .snapshot
            .version(Namespace::User, &item_key(self.root, id))
            .is_some()
        {
            return Err(ValueError::Corrupt(Corrupt::OrphanElement));
        }
        let len = len_u64(&envelope);
        self.items.insert(
            id,
            ItemSlot {
                stored: false,
                value: Some(envelope),
            },
        );
        self.insert(at, id, len)?;
        Ok(id)
    }

    /// The item and page writes, by key: `Some` puts, `None` deletes. A record minted and
    /// freed in this compile writes nothing.
    fn writes(self) -> Result<BTreeMap<Bytes, Option<Bytes>>, ValueError> {
        let mut writes = BTreeMap::new();
        for (id, slot) in self.items {
            if slot.stored || slot.value.is_some() {
                writes.insert(item_key(self.root, id), slot.value);
            }
        }
        for (id, slot) in self.pages {
            let value = match (slot.dirty, slot.stored, slot.node) {
                (false, _, _) | (true, false, None) => continue,
                (true, true, None) => None,
                (true, _, Some(node)) => {
                    Some(seal(Kind::ListPage, &node.encoded()?).map_err(ApplyError::from)?)
                }
            };
            writes.insert(page_key(self.root, id), value);
        }
        Ok(writes)
    }
}

/// Compile `ops` against the list at `root` into one root `Put` and one write per item or page
/// that changed, in key order (ADR-rdb-0016 §5).
///
/// - [`Expected::Version`]: the root must exist, be at that version and be a list.
/// - [`Expected::Absent`]: a create, seeded with `next = snapshot.at() << 64`. The root `Put`
///   carries [`Condition::Absent`]. When the root is absent here, any record under the item or
///   page range is [`Corrupt::OrphanElement`].
///
/// # Errors
/// [`ApplyError::InvalidNodeSize`], [`ApplyError::ObjectAbsent`], [`ApplyError::VersionConflict`],
/// [`ApplyError::KindMismatch`], [`ApplyError::PositionInvalid`], [`ApplyError::TooLarge`],
/// [`ApplyError::TooDeep`], [`ApplyError::ListTooTall`], [`ApplyError::TooManyWrites`], or
/// [`ValueError::Corrupt`].
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
    let (found, root_version, expected_version, conditions) = match expected {
        Expected::Absent => {
            let root_absent = snapshot.version(Namespace::User, root.as_bytes()).is_none();
            if root_absent && any_item_or_page(snapshot, root) {
                return Err(ValueError::Corrupt(Corrupt::OrphanElement));
            }
            let fresh = Root {
                next: u128::from(snapshot.at().0) << 64,
                top: Node::Leaf(Vec::new()),
                bytes: 0,
                count: 0,
            };
            let absent = Condition::Absent {
                key: root.to_bytes(),
            };
            // A fresh tree names no stored page or item, so the root version is never compared.
            (fresh, 0, None, vec![absent])
        }
        Expected::Version(want) => {
            check_version(snapshot, root, want)?;
            let (_, bytes) = record(snapshot, root.as_bytes())?.ok_or(ApplyError::ObjectAbsent)?;
            (open_root(&bytes)?, want, Some(want), Vec::new())
        }
    };

    let mut tree = Tree {
        snapshot,
        root,
        root_version,
        node_max,
        next: found.next,
        count: found.count,
        bytes: found.bytes,
        top: found.top,
        pages: BTreeMap::new(),
        items: BTreeMap::new(),
    };
    let mut minted = Vec::new();
    for op in ops {
        if let Some(id) = tree.apply(op)? {
            minted.push(id);
        }
    }

    let root_value = seal(
        Kind::List,
        &root_payload(tree.next, &tree.top, tree.bytes, tree.count)?,
    )
    .map_err(ApplyError::from)?;
    // The root sorts before every item and page (sub 0x00 < 0x02 < 0x03), so this keeps key
    // order.
    let mut mutations = vec![Mutation::Put {
        key: root.to_bytes(),
        value: root_value,
        expected_version,
    }];
    mutations.extend(tree.writes()?.into_iter().map(|(key, value)| match value {
        Some(value) => Mutation::Put {
            key,
            value,
            expected_version: None,
        },
        None => Mutation::Delete {
            key,
            expected_version: None,
        },
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

/// Compile the deletion of the empty list at `root`, at `version` (ADR-rdb-0016 §5). An empty
/// list's root is an empty leaf: the root's checks refuse any other top node at `count = 0`.
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
