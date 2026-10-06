//! Ordered lists: a root, blocks with their change slots, and a record for each item too large
//! for its block (ADR-rdb-0016).
//!
//! - The root, at the object's [`RootKey`], is an envelope of kind [`Kind::List`] whose payload is
//!   canonical CBOR of exactly `{next, seed, bytes, count, blocks, records}`: the id counter, the
//!   create's seed, the items' value bytes and count, the block index (`[n, count, head]`
//!   per block, in list order) and whether every item keeps its own record.
//! - A block's base, at [`block_key`], is kind [`Kind::ListBlock`]: `{items, folded}`, the entries
//!   as of op no `folded`. Its later ops, `folded + 1 … head`, each live in change slot
//!   `op no mod 240`, at [`slot_key`], kind [`Kind::ListSlot`]: `[op no, op]`. A slot holding any
//!   other op no is stale and never read.
//! - An entry is an item's `n` alone, when the item has its own record, or `[n, value]`, when the
//!   block holds its value. The item id is `seed << 64 | n`. A value goes in its block when it
//!   encodes to at most `min(`[`INLINE_MAX`]`, block_max / 4 − 16)`, unless the list was created
//!   with `records`. Readers check neither the limit nor the flag.
//! - An item record, at [`item_key`], is a document envelope holding the item's value.
//! - [`compile_list`] turns positional ops into one root `Put`, the item writes, and per touched
//!   block either one slot per op or, when the pending ops pass 240 or a quarter of the base's
//!   bytes, a new base (a fold), in key order, with the snapshot's generation attached
//!   (decision 8). A fold deletes nothing.
//!
//! Reads go through `&dyn SnapshotRead` only. Every block is replayed and checked before it is
//! used, and nothing is repaired: damage is a named [`Corrupt`].

use std::collections::BTreeMap;

use bytes::Bytes;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::admission::MAX_REQUEST_MUTATIONS;
use rdb_core::transaction::record_len;
use rdb_core::{Condition, Generation, Mutation, Namespace, SnapshotRead};

use crate::cbor::{decode, encode};
use crate::compile::{
    check_version, record, refuse_list_record_at_root, BlockFault, Compiled, Corrupt, Expected,
    SlotFault, ValueError,
};
use crate::delta::{ApplyError, SizeLimit};
use crate::envelope::{open, seal, Kind, HEADER_LEN};
use crate::keys::{
    block_key, item_key, slot_key, KeyError, RootKey, LIST_ID_LEN, LIST_SLOTS, SUB_BLOCK, SUB_ITEM,
};
use crate::value::{Int, Map, MapKey, Value};

/// The block size rDB writes with: a fold whose base would pass it splits (ADR-rdb-0016 §4). A
/// compile argument, never stored.
pub const DEFAULT_BLOCK_MAX: usize = 131_072;
/// The smallest `block_max` a compile takes (ADR-rdb-0016 §4).
pub const MIN_BLOCK_MAX: usize = 1_024;
/// The largest `block_max` a compile takes (ADR-rdb-0016 §4): 192 KiB, so that a base left
/// unsplit at the block cap stays within [`MAX_BLOCK_PAYLOAD`].
pub const MAX_BLOCK_MAX: usize = 196_608;
/// The largest block payload a reader takes (ADR-rdb-0016 §4, §7).
pub const MAX_BLOCK_PAYLOAD: usize = 262_144;
/// The longest escaped object id, `|esc(object id)|`, a list compile takes (ADR-rdb-0016 §4).
/// Within it, emptying or dropping a list never fails on bytes.
pub const MAX_ID_ESCAPED: usize = 3_072;
/// The most blocks a list has (ADR-rdb-0016 §4). Readers check it.
pub const MAX_BLOCKS: usize = 512;
/// The largest item envelope a write takes (ADR-rdb-0016 §4). A write limit, not a format one:
/// reads never check it.
pub const MAX_ITEM: usize = 524_288;
/// L: the largest encoded value a block entry holds (ADR-rdb-0016 §4). A code constant, never
/// stored; readers never check it.
pub const INLINE_MAX: usize = 256;

/// [`LIST_SLOTS`] as an op-no modulus.
const SLOTS: u64 = LIST_SLOTS as u64;

/// The largest encoded value a block entry holds at `block_max`: `min(L, B / 4 − 16)`, so a
/// block over B holds at least 4 entries (ADR-rdb-0016 §4). At 1,024 it is 240; L binds from
/// 1,088.
const fn inline_limit(block_max: usize) -> usize {
    let fit = (block_max / 4).saturating_sub(16);
    if fit < INLINE_MAX {
        fit
    } else {
        INLINE_MAX
    }
}

/// The largest root payload: [`MAX_BLOCKS`] index entries of at most 28 bytes, and 78 bytes of
/// the rest (ADR-rdb-0016 §4).
const MAX_ROOT_PAYLOAD: usize = MAX_BLOCKS * 28 + 78;
/// `record_len`'s cost of one `Put` past its key and value.
const PUT_COST: usize = 10;
/// An object key's bytes besides the escaped object id: the 12-byte scope and `sub`.
const SCOPE_AND_SUB: usize = 13;
/// The largest slot payload: `[op no, [code, at, [n, value]]]` with every integer at 9 bytes and
/// an inline value of [`INLINE_MAX`] bytes (287), rounded up.
const MAX_SLOT_PAYLOAD: usize = 300;

/// The bytes one op can write at `block_max` for an object id that escapes to `e` bytes
/// (ADR-rdb-0016 §4): the request; the root; a fold of up to 1.25 · B and one entry, written as
/// two blocks; a slot; the largest item.
const fn worst_op(block_max: usize, e: usize) -> usize {
    let key = SCOPE_AND_SUB + e;
    266 + (PUT_COST + key + HEADER_LEN + MAX_ROOT_PAYLOAD)
        + 2 * (PUT_COST + key + LIST_ID_LEN + HEADER_LEN)
        + block_max / 4 * 5
        + MAX_SLOT_PAYLOAD
        + (PUT_COST + key + LIST_ID_LEN + 1 + HEADER_LEN + MAX_SLOT_PAYLOAD)
        + (PUT_COST + key + LIST_ID_LEN + MAX_ITEM)
}

// Any one op fits one request, at the default and at the largest block size, for the longest id
// a list takes (ADR-rdb-0016 §4).
const _: () = assert!(worst_op(DEFAULT_BLOCK_MAX, MAX_ID_ESCAPED) <= MAX_ENVELOPE_BYTES);
const _: () = assert!(worst_op(MAX_BLOCK_MAX, MAX_ID_ESCAPED) <= MAX_ENVELOPE_BYTES);
/// The bytes a Move that retires one block can write (ADR-rdb-0016 §4): the request, the root,
/// the other block folded and split, and the retired block's 241 key deletes.
const fn worst_retire(block_max: usize, e: usize) -> usize {
    let key = SCOPE_AND_SUB + e;
    266 + (PUT_COST + key + HEADER_LEN + MAX_ROOT_PAYLOAD)
        + 2 * (PUT_COST + key + LIST_ID_LEN + HEADER_LEN)
        + block_max / 4 * 5
        + MAX_SLOT_PAYLOAD
        + (6 + key + LIST_ID_LEN)
        + LIST_SLOTS as usize * (6 + key + LIST_ID_LEN + 1)
}

// Retiring a block fits one request at both block sizes for the longest id: emptying a list
// never fails on bytes.
const _: () = assert!(worst_retire(DEFAULT_BLOCK_MAX, MAX_ID_ESCAPED) <= MAX_ENVELOPE_BYTES);
const _: () = assert!(worst_retire(MAX_BLOCK_MAX, MAX_ID_ESCAPED) <= MAX_ENVELOPE_BYTES);
// A base left unsplit at the block cap, 1.25 · B and one entry, is one a reader takes.
const _: () = assert!(MAX_BLOCK_MAX / 4 * 5 + MAX_SLOT_PAYLOAD <= MAX_BLOCK_PAYLOAD);

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
    /// stay; only blocks are written.
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
    /// The items' value bytes: each item's canonical encoded length, wherever it is stored.
    pub bytes: u64,
    /// How many blocks it has: at least 1.
    pub blocks: usize,
    /// Whether every item keeps its own record. Set when the list is created.
    pub records: bool,
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
    /// Whether its block holds the value; `false` when the item has its own record.
    pub inline: bool,
    /// The storage version of the record that holds the value: the item's own record, or for an
    /// inline item the root's. Not a concurrency token.
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

// ---- entries and ops ------------------------------------------------------------------------

/// A block entry (ADR-rdb-0016 §1): an item's `n`, and its value when the block holds it. With
/// no value here, the item has its own record.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    n: u64,
    inline: Option<Value>,
}

impl Entry {
    /// `n` or `[n, value]`.
    fn value(&self) -> Value {
        match &self.inline {
            None => uint(self.n),
            Some(value) => Value::Array(vec![uint(self.n), value.clone()]),
        }
    }
}

/// One block op (ADR-rdb-0016 §1): `[0, at, entry]`, `[1, at]` or `[2, at, entry]`. `at` is a
/// position in the block after every earlier op of that block.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Change {
    Insert { at: u64, entry: Entry },
    Remove { at: u64 },
    Replace { at: u64, entry: Entry },
}

impl Change {
    fn value(&self) -> Value {
        match self {
            Self::Insert { at, entry } => Value::Array(vec![uint(0), uint(*at), entry.value()]),
            Self::Remove { at } => Value::Array(vec![uint(1), uint(*at)]),
            Self::Replace { at, entry } => Value::Array(vec![uint(2), uint(*at), entry.value()]),
        }
    }

    /// Apply to `entries`; `false` when `at` is outside them, which changes nothing.
    fn apply(self, entries: &mut Vec<Entry>) -> bool {
        let len = len_u64(entries.len());
        match self {
            Self::Insert { at, entry } if at <= len => entries.insert(index(at), entry),
            Self::Remove { at } if at < len => {
                entries.remove(index(at));
            }
            Self::Replace { at, entry } if at < len => entries[index(at)] = entry,
            _ => return false,
        }
        true
    }
}

fn uint(v: u64) -> Value {
    Value::Integer(Int::from(v))
}

/// A non-negative integer that fits u64, or `None`.
fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::Integer(i) => u64::try_from(i.get()).ok(),
        _ => None,
    }
}

fn len_u64(len: usize) -> u64 {
    u64::try_from(len).expect("a length fits u64")
}

/// A position inside a block already checked against its length.
fn index(at: u64) -> usize {
    usize::try_from(at).expect("a block position fits usize")
}

/// `v + delta`, or `None` outside `u64`.
fn shift(v: u64, delta: i128) -> Option<u64> {
    u64::try_from(i128::from(v) + delta).ok()
}

const ENTRY_SHAPE: &str = "a list block entry is not n or [n, value]";
const OP_SHAPE: &str =
    "a list change slot is not [op no, [0, at, entry] or [1, at] or [2, at, entry]]";

/// `n`, or `[n, value]` with any value.
fn parse_entry(value: &Value) -> Result<Entry, &'static str> {
    if let Some(n) = as_u64(value) {
        return Ok(Entry { n, inline: None });
    }
    let Value::Array(parts) = value else {
        return Err(ENTRY_SHAPE);
    };
    let [n, inline] = parts.as_slice() else {
        return Err(ENTRY_SHAPE);
    };
    Ok(Entry {
        n: as_u64(n).ok_or(ENTRY_SHAPE)?,
        inline: Some(inline.clone()),
    })
}

/// A change slot's payload checked as a read checks it: its op no, or why it is not
/// `[op no, op]`. For tools that show a slot without replaying its block, such as a dump.
pub fn slot_op_no(value: &Value) -> Result<u64, &'static str> {
    parse_slot(value).map(|(op_no, _)| op_no)
}

/// A slot's `[op no, op]`.
fn parse_slot(value: &Value) -> Result<(u64, Change), &'static str> {
    let Value::Array(parts) = value else {
        return Err(OP_SHAPE);
    };
    let [op_no, op] = parts.as_slice() else {
        return Err(OP_SHAPE);
    };
    let op_no = as_u64(op_no).ok_or(OP_SHAPE)?;
    let Value::Array(op) = op else {
        return Err(OP_SHAPE);
    };
    let change = match op.as_slice() {
        [code, at, entry] if as_u64(code) == Some(0) => Change::Insert {
            at: as_u64(at).ok_or(OP_SHAPE)?,
            entry: parse_entry(entry)?,
        },
        [code, at] if as_u64(code) == Some(1) => Change::Remove {
            at: as_u64(at).ok_or(OP_SHAPE)?,
        },
        [code, at, entry] if as_u64(code) == Some(2) => Change::Replace {
            at: as_u64(at).ok_or(OP_SHAPE)?,
            entry: parse_entry(entry)?,
        },
        _ => return Err(OP_SHAPE),
    };
    Ok((op_no, change))
}

/// A block payload: `{items: [entry...], folded}`.
fn block_value(entries: &[Entry], folded: u64) -> Value {
    let mut map = Map::new();
    map.insert(
        MapKey::new("items"),
        Value::Array(entries.iter().map(Entry::value).collect()),
    );
    map.insert(MapKey::new("folded"), uint(folded));
    Value::Map(map)
}

/// The encoded length of a block's entries: what "grows its replayed payload" measures at the
/// block cap (ADR-rdb-0016 §4). `folded` is left out, so an op no gaining a digit is no growth.
fn items_len(entries: &[Entry]) -> Result<usize, ValueError> {
    let items = Value::Array(entries.iter().map(Entry::value).collect());
    Ok(encode(&items).map_err(ApplyError::from)?.len())
}

/// Where the last block splits (G35): the longest prefix whose base, at `folded`, is at most
/// `block_max`. Called only when the whole block is over it.
fn end_split(entries: &[Entry], folded: u64, block_max: usize) -> Result<usize, ValueError> {
    let fits = |k: usize| -> Result<bool, ValueError> {
        let base = encode(&block_value(&entries[..k], folded)).map_err(ApplyError::from)?;
        Ok(base.len() <= block_max)
    };
    // A base grows with its prefix: the empty prefix fits and the whole block does not.
    let (mut lo, mut hi) = (0, entries.len());
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if fits(mid)? {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Where any other block splits (G36): the first cut, `1 … len − 1`, that leaves the two sides'
/// entry bytes closest.
fn halves(entries: &[Entry]) -> Result<usize, ValueError> {
    let sizes = entries
        .iter()
        .map(|entry| encode(&entry.value()).map(|bytes| bytes.len()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApplyError::from)?;
    let total: usize = sizes.iter().sum();
    let (mut best, mut cut, mut left) = (usize::MAX, 1, 0);
    for (k, size) in sizes.iter().enumerate().take(sizes.len().saturating_sub(1)) {
        left += size;
        let gap = (2 * left).abs_diff(total);
        if gap < best {
            (best, cut) = (gap, k + 1);
        }
    }
    Ok(cut)
}

/// A block payload, decoded: its entries and `folded`.
fn parse_block(value: &Value) -> Result<(Vec<Entry>, u64), &'static str> {
    let Value::Map(fields) = value else {
        return Err("a list block is not a map");
    };
    let (Some(Value::Array(items)), Some(folded), 2) = (
        fields.get(&MapKey::new("items")),
        fields.get(&MapKey::new("folded")),
        fields.len(),
    ) else {
        return Err("a list block is not exactly items and folded");
    };
    let folded = as_u64(folded).ok_or("a list block's folded is not an unsigned integer")?;
    let entries = items.iter().map(parse_entry).collect::<Result<_, _>>()?;
    Ok((entries, folded))
}

// ---- the root ------------------------------------------------------------------------------

/// A root's index entry for one block: its `n`, item count and newest op no. It holds no byte
/// total, so nothing that moves entries between blocks reads an item record (D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockRef {
    n: u64,
    count: u64,
    head: u64,
}

/// A root payload, decoded.
#[derive(Clone)]
struct Root {
    /// The next `n` to mint, for items and blocks.
    next: u64,
    seed: u64,
    bytes: u64,
    count: u64,
    blocks: Vec<BlockRef>,
    /// Every item keeps its own record.
    records: bool,
}

impl Root {
    /// The full id of `n`: `seed << 64 | n`.
    fn id(&self, n: u64) -> u128 {
        (u128::from(self.seed) << 64) | u128::from(n)
    }

    /// The root payload: `{next, seed, bytes, count, blocks, records}`.
    fn payload(&self) -> Result<Vec<u8>, ApplyError> {
        let blocks = self
            .blocks
            .iter()
            .map(|b| Value::Array(vec![uint(b.n), uint(b.count), uint(b.head)]))
            .collect();
        let mut map = Map::new();
        map.insert(MapKey::new("next"), uint(self.next));
        map.insert(MapKey::new("seed"), uint(self.seed));
        map.insert(MapKey::new("bytes"), uint(self.bytes));
        map.insert(MapKey::new("count"), uint(self.count));
        map.insert(MapKey::new("blocks"), Value::Array(blocks));
        map.insert(MapKey::new("records"), Value::Bool(self.records));
        encode(&Value::Map(map)).map_err(ApplyError::from)
    }
}

fn corrupt_root(what: &'static str) -> ValueError {
    ValueError::Corrupt(Corrupt::ListRoot(what))
}

const BLOCK_REF_SHAPE: &str = "a list root's block entry is not [n, count, head]";

fn parse_block_ref(value: &Value) -> Result<BlockRef, &'static str> {
    let Value::Array(parts) = value else {
        return Err(BLOCK_REF_SHAPE);
    };
    let [n, count, head] = parts.as_slice() else {
        return Err(BLOCK_REF_SHAPE);
    };
    let field = |v| as_u64(v).ok_or(BLOCK_REF_SHAPE);
    Ok(BlockRef {
        n: field(n)?,
        count: field(count)?,
        head: field(head)?,
    })
}

/// Open a root record. Another kind is an [`ApplyError::KindMismatch`].
fn open_root(bytes: &[u8]) -> Result<Root, ValueError> {
    let opened = open(bytes).map_err(|e| ValueError::Corrupt(Corrupt::Envelope(e)))?;
    // A block or slot record is never written at a root key, so one there is damage, not
    // another kind of object, named as every other root path names it.
    refuse_list_record_at_root(opened.kind)?;
    if opened.kind != Kind::List {
        return Err(ApplyError::KindMismatch { found: opened.kind }.into());
    }
    let payload = decode(opened.payload).map_err(|e| ValueError::Corrupt(Corrupt::Codec(e)))?;
    let Value::Map(fields) = payload else {
        return Err(corrupt_root("the list root's payload is not a map"));
    };
    let field = |name: &str| fields.get(&MapKey::new(name));
    let (Some(next), Some(seed), Some(bytes), Some(count), Some(blocks), Some(records), 6) = (
        field("next"),
        field("seed"),
        field("bytes"),
        field("count"),
        field("blocks"),
        field("records"),
        fields.len(),
    ) else {
        return Err(corrupt_root(
            "the list root is not exactly next, seed, bytes, count, blocks and records",
        ));
    };
    let &Value::Bool(records) = records else {
        return Err(corrupt_root("the list root's records is not a bool"));
    };
    let unsigned = |v, what| as_u64(v).ok_or(corrupt_root(what));
    let next = unsigned(next, "the list root's next is not an unsigned integer")?;
    let seed = unsigned(seed, "the list root's seed is not an unsigned integer")?;
    let bytes = unsigned(bytes, "the list root's bytes is not an unsigned integer")?;
    let count = unsigned(count, "the list root's count is not an unsigned integer")?;
    let Value::Array(blocks) = blocks else {
        return Err(corrupt_root("the list root's blocks is not an array"));
    };
    if blocks.is_empty() || blocks.len() > MAX_BLOCKS {
        return Err(corrupt_root(
            "the list root has no blocks, or more than 512",
        ));
    }
    let blocks = blocks
        .iter()
        .map(parse_block_ref)
        .collect::<Result<Vec<_>, _>>()
        .map_err(corrupt_root)?;
    let overflow = || corrupt_root("the list root's block counts leave u64");
    let mut counted = 0_u64;
    for block in &blocks {
        if block.n >= next {
            return Err(corrupt_root("a list root's block n is not below next"));
        }
        counted = counted.checked_add(block.count).ok_or_else(overflow)?;
    }
    if blocks.len() > 1 && blocks.iter().any(|block| block.count == 0) {
        return Err(corrupt_root(
            "the list root names an empty block beside others; only an only block is empty",
        ));
    }
    if counted != count {
        return Err(corrupt_root(
            "the list root's count is not the sum of its blocks'",
        ));
    }
    Ok(Root {
        next,
        seed,
        bytes,
        count,
        blocks,
        records,
    })
}

// ---- blocks --------------------------------------------------------------------------------

/// A block read back and replayed: its entries after every pending op, with what a write needs
/// to decide a fold.
struct Loaded {
    /// The base's payload length.
    base_len: usize,
    folded: u64,
    entries: Vec<Entry>,
    /// The pending slots' payload bytes.
    pending_bytes: usize,
}

/// The slot op no `op` lives in: `op mod 240`.
fn slot_of(op: u64) -> u8 {
    u8::try_from(op % SLOTS).expect("a slot is below 240")
}

/// Every key block `id` can hold after op no `head`: its base, then the slot of each op no
/// `1 … head`, ascending (ADR-rdb-0016 §3). A fold writes no slot, so some may be absent; a
/// retire or a drop deletes them all, present or not.
fn block_keys(root: &RootKey, id: u128, head: u64) -> Vec<Bytes> {
    let mut slots: Vec<u8> = (1..=head.min(SLOTS)).map(slot_of).collect();
    slots.sort_unstable();
    let mut keys = vec![block_key(root, id)];
    keys.extend(slots.into_iter().map(|slot| slot_key(root, id, slot)));
    keys
}

/// Read block `id`, which the root (at `root_version`) names as `block`: `get` its base, then
/// read only its pending slots, with one `scan` from the first, or two when they wrap past slot
/// 239 (ADR-rdb-0016 §3). No stale slot and no key past the block is read, so a neighbour's
/// records never are (G67). Replay the pending ops and check the block (§7) before anything
/// uses it.
fn load_block(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    root_version: u64,
    id: u128,
    block: &BlockRef,
) -> Result<Loaded, ValueError> {
    let fault = |fault| ValueError::Corrupt(Corrupt::Block { id, fault });
    let key = block_key(root, id);
    let bytes = snapshot
        .get(Namespace::User, &key)
        .ok_or_else(|| fault(BlockFault::Missing))?;
    let version = snapshot
        .version(Namespace::User, &key)
        .ok_or(ValueError::Corrupt(Corrupt::VersionWithoutValue))?;
    if version > root_version {
        return Err(fault(BlockFault::NewerThanRoot {
            block: version,
            root: root_version,
        }));
    }
    let opened = open(&bytes).map_err(|e| fault(BlockFault::Envelope(e)))?;
    if opened.kind != Kind::ListBlock {
        return Err(fault(BlockFault::NotABlock { found: opened.kind }));
    }
    let base_len = opened.payload.len();
    if base_len > MAX_BLOCK_PAYLOAD {
        return Err(fault(BlockFault::TooLarge { len: base_len }));
    }
    let payload = decode(opened.payload).map_err(|e| fault(BlockFault::Codec(e)))?;
    let (mut entries, folded) = parse_block(&payload).map_err(|w| fault(BlockFault::Shape(w)))?;
    if folded > block.head {
        return Err(fault(BlockFault::Shape(
            "a list block's folded is past its root entry's head",
        )));
    }
    let pending = block.head - folded;
    if pending > SLOTS {
        return Err(fault(BlockFault::Shape(
            "a list block has more than 240 pending ops",
        )));
    }

    // The pending slots run from slot (folded + 1) mod 240; past slot 239 they wrap to 0.
    let start = folded + 1;
    let first_run = pending.min(SLOTS - start % SLOTS);
    let runs = [(start, first_run), (start + first_run, pending - first_run)];
    let mut pending_bytes = 0;
    for (from, len) in runs.into_iter().filter(|&(_, len)| len > 0) {
        let limit = usize::try_from(len).expect("at most 240 pending ops");
        let found = snapshot.scan(Namespace::User, &slot_key(root, id, slot_of(from)), limit);
        for (op, i) in (from..from + len).zip(0..) {
            // A scan entry past this op's slot means the slot is missing (§3); one before it is a
            // stray key between two slots, and is named as a drop names one.
            let expected = slot_key(root, id, slot_of(op));
            let raw = match found.get(i) {
                Some((found_key, value)) if *found_key == expected => value,
                Some((found_key, _)) if *found_key < expected => {
                    return Err(stray(root, found_key))
                }
                _ => return Err(fault(BlockFault::OpMissing { op })),
            };
            let bad = |fault| {
                ValueError::Corrupt(Corrupt::Block {
                    id,
                    fault: BlockFault::OpBad { op, fault },
                })
            };
            let opened = open(raw).map_err(|e| bad(SlotFault::Envelope(e)))?;
            if opened.kind != Kind::ListSlot {
                return Err(bad(SlotFault::NotASlot { found: opened.kind }));
            }
            let value = decode(opened.payload).map_err(|e| bad(SlotFault::Codec(e)))?;
            let (op_no, change) = parse_slot(&value).map_err(|w| bad(SlotFault::Shape(w)))?;
            // A slot is trusted by its op no: the root names the pending range, and only a write
            // of that root put op `op` here (ADR-rdb-0016 §7).
            if op_no != op {
                return Err(fault(BlockFault::OpMissing { op }));
            }
            if !change.apply(&mut entries) {
                return Err(fault(BlockFault::OpOutOfRange { op }));
            }
            pending_bytes += opened.payload.len();
        }
    }
    if len_u64(entries.len()) != block.count {
        return Err(fault(BlockFault::Shape(
            "a list block's replayed count is not its root entry's",
        )));
    }
    Ok(Loaded {
        base_len,
        folded,
        entries,
        pending_bytes,
    })
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
        blocks: root.blocks.len(),
        records: root.records,
    }
}

/// Up to `limit` items of the list at `root`, in list order, from `start`. Every block the
/// items are in, and every item record returned, is read and checked before any is returned.
/// `limit` 0 reads the root only: no items and no token, since a token could not advance.
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
    let mut entries = Vec::new();
    let mut first = 0_u64;
    for block in &found.blocks {
        let after = first + block.count;
        // Nothing from here on is in [position, end): `limit` 0 reads no block from any position.
        if first.max(position) >= end {
            break;
        }
        if after > position {
            let loaded = load_block(snapshot, root, version, found.id(block.n), block)?;
            let from = index(position.saturating_sub(first));
            let to = index(end.min(after) - first);
            entries.extend(loaded.entries.into_iter().take(to).skip(from));
        }
        first = after;
    }
    let mut out = Vec::with_capacity(entries.len());
    for (p, entry) in (position..).zip(entries) {
        let id = found.id(entry.n);
        // An inline item is its block's, and its version the root's (ADR-rdb-0016 §6).
        let (value, item_version, inline) = match entry.inline {
            Some(value) => (value, version, true),
            None => {
                let (value, item_version, _) = read_item(snapshot, root, id, version)?;
                (value, item_version, false)
            }
        };
        out.push(Item {
            position: p,
            id,
            value,
            inline,
            version: item_version,
        });
    }
    let next = (limit > 0 && end < found.count).then_some(Token {
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

/// Read and check the item `id` under a root at `root_version`: its value, version and payload
/// length. A read and a write that sizes the item check it alike, so both name the same fault.
fn read_item(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    id: u128,
    root_version: u64,
) -> Result<(Value, u64, usize), ValueError> {
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
        return Err(corrupt(Corrupt::ItemNotDocument { found: opened.kind }));
    }
    let value = decode(opened.payload).map_err(|e| corrupt(Corrupt::Codec(e)))?;
    Ok((value, version, opened.payload.len()))
}

/// Whether a record exists under `root`'s item or block range (one limit-1 scan). A key after
/// both ranges, such as a chunk or a neighbouring object, does not count (ADR-rdb-0016 §5).
fn any_item_or_block(snapshot: &dyn SnapshotRead, root: &RootKey) -> bool {
    let (items, blocks) = (root.sub_prefix(SUB_ITEM), root.sub_prefix(SUB_BLOCK));
    snapshot
        .scan(Namespace::User, &items, 1)
        .first()
        .is_some_and(|(key, _)| key.starts_with(&items) || key.starts_with(&blocks))
}

// ---- compile -------------------------------------------------------------------------------

/// An item record the compile touched (the item overlay, ADR-rdb-0016 §5). `stored`: a record
/// existed in the snapshot. `value`: the record in the compile's view, `None` when there is none.
struct ItemSlot {
    stored: bool,
    value: Option<Bytes>,
}

/// A block the compile opened or made: its entries as the ops leave them, and the ops.
struct Work {
    /// The stored base's payload length; `None` for a block this compile made.
    base_len: Option<usize>,
    /// The stored base's `folded`.
    folded: u64,
    /// The stored pending slots' payload bytes.
    pending_bytes: usize,
    entries: Vec<Entry>,
    /// The encoded length of its entries when opened: an op at the block cap must not grow it
    /// past B (ADR-rdb-0016 §4).
    start_items: usize,
    /// This compile's ops on the block, in order: op nos `head − len + 1 … head`.
    ops: Vec<Change>,
}

/// How a block the compile touched or made ends (ADR-rdb-0016 §4).
#[derive(Clone)]
enum Fate {
    /// Written whole: this base payload.
    Base(Vec<u8>),
    /// Each op in its slot: the op no and the slot payload.
    Slots(Vec<(u64, Vec<u8>)>),
    /// Deleted: every key it has stored.
    Retired(Vec<Bytes>),
}

/// Each touched or made block's fate, by `n`.
type Fates = BTreeMap<u64, Fate>;

/// A compile's end: the root and the writes, and the same with one merge-back, when there is
/// one. The merge-back is taken only if it fits both request caps.
struct Finished {
    list: Root,
    writes: BTreeMap<Bytes, Option<Bytes>>,
    merged: Option<(Root, BTreeMap<Bytes, Option<Bytes>>)>,
}

/// The item, block and slot writes, by key: `Some` puts, `None` deletes.
fn emit(
    root: &RootKey,
    list: &Root,
    items: &BTreeMap<Bytes, Option<Bytes>>,
    fates: &BTreeMap<u64, Fate>,
) -> Result<BTreeMap<Bytes, Option<Bytes>>, ValueError> {
    let mut writes = items.clone();
    for (&n, fate) in fates {
        let id = list.id(n);
        match fate {
            Fate::Base(payload) => {
                let base = seal(Kind::ListBlock, payload).map_err(ApplyError::from)?;
                writes.insert(block_key(root, id), Some(base));
            }
            Fate::Slots(slots) => {
                for (op, payload) in slots {
                    let slot = u8::try_from(op % SLOTS).expect("a slot is below 240");
                    let sealed = seal(Kind::ListSlot, payload).map_err(ApplyError::from)?;
                    writes.insert(slot_key(root, id, slot), Some(sealed));
                }
            }
            Fate::Retired(keys) => {
                for key in keys {
                    writes.insert(key.clone(), None);
                }
            }
        }
    }
    Ok(writes)
}

/// The compile's copy of one list: the root's fields, every block it opened (each replayed and
/// checked when opened) or made, and every item it wrote.
struct Draft<'a> {
    snapshot: &'a dyn SnapshotRead,
    root: &'a RootKey,
    root_version: u64,
    block_max: usize,
    list: Root,
    blocks: BTreeMap<u64, Work>,
    items: BTreeMap<u128, ItemSlot>,
}

impl Draft<'_> {
    /// The next `n` from the counter items and blocks share (ADR-rdb-0016 §2).
    fn mint(&mut self) -> Result<u64, ValueError> {
        let n = self.list.next;
        self.list.next = n
            .checked_add(1)
            .filter(|next| *next < u64::MAX)
            .ok_or(corrupt_root("the list root's next is the largest there is"))?;
        Ok(n)
    }

    /// The index of the block holding position `pos`, and `pos` inside it. With `end`, `pos` may
    /// be a block's length: the end of the first block it ends.
    fn locate(&self, pos: u64, end: bool) -> (usize, u64) {
        let mut first = 0_u64;
        for (i, block) in self.list.blocks.iter().enumerate() {
            let after = first + block.count;
            if pos < after || (end && pos == after) {
                return (i, pos - first);
            }
            first = after;
        }
        unreachable!("a position is checked against the list's count first")
    }

    /// The block at index `i`, opened (replayed and checked) unless this compile already has it.
    fn open_block(&mut self, i: usize) -> Result<&mut Work, ValueError> {
        let block = self.list.blocks[i];
        if !self.blocks.contains_key(&block.n) {
            let id = self.list.id(block.n);
            let loaded = load_block(self.snapshot, self.root, self.root_version, id, &block)?;
            self.blocks.insert(
                block.n,
                Work {
                    base_len: Some(loaded.base_len),
                    folded: loaded.folded,
                    pending_bytes: loaded.pending_bytes,
                    start_items: items_len(&loaded.entries)?,
                    entries: loaded.entries,
                    ops: Vec::new(),
                },
            );
        }
        Ok(self.blocks.get_mut(&block.n).expect("just opened"))
    }

    /// Record `change`, already applied to block `i`'s entries, and move the block's and the
    /// root's counts by `count` and the root's bytes by `bytes`.
    fn record(
        &mut self,
        i: usize,
        change: Change,
        count: i128,
        bytes: i128,
    ) -> Result<(), ValueError> {
        let n = self.list.blocks[i].n;
        self.blocks
            .get_mut(&n)
            .expect("an op's block is open")
            .ops
            .push(change);
        let overflow =
            || corrupt_root("a list's count or bytes, or a block's count or head, leaves u64");
        let block = &mut self.list.blocks[i];
        block.count = shift(block.count, count).ok_or_else(overflow)?;
        block.head = block.head.checked_add(1).ok_or_else(overflow)?;
        self.list.count = shift(self.list.count, count).ok_or_else(overflow)?;
        self.list.bytes = shift(self.list.bytes, bytes).ok_or_else(overflow)?;
        Ok(())
    }

    /// An item's value length (ADR-rdb-0016 §1): an inline value's encoding, else its record's
    /// payload, from the overlay first, else read from the snapshot and checked as a read checks it.
    fn item_len(&self, entry: &Entry) -> Result<u64, ValueError> {
        if let Some(value) = &entry.inline {
            return Ok(len_u64(encode(value).map_err(ApplyError::from)?.len()));
        }
        let id = self.list.id(entry.n);
        let missing = ValueError::Corrupt(Corrupt::ItemMissing { id });
        if let Some(slot) = self.items.get(&id) {
            let envelope = slot.value.as_ref().ok_or(missing)?;
            return Ok(len_u64(envelope.len() - HEADER_LEN));
        }
        let (_, _, payload) = read_item(self.snapshot, self.root, id, self.root_version)?;
        Ok(len_u64(payload))
    }

    /// Whether item `id` has a record in the compile's view: the overlay's, else the snapshot's.
    fn has_record(&self, id: u128) -> bool {
        self.items.get(&id).map_or_else(
            || {
                self.snapshot
                    .version(Namespace::User, &item_key(self.root, id))
                    .is_some()
            },
            |slot| slot.value.is_some(),
        )
    }

    /// Item `id`'s overlay slot, made on first touch with `stored` from the snapshot.
    fn touch(&mut self, id: u128) -> &mut ItemSlot {
        if !self.items.contains_key(&id) {
            let stored = self
                .snapshot
                .version(Namespace::User, &item_key(self.root, id))
                .is_some();
            self.items.insert(
                id,
                ItemSlot {
                    stored,
                    value: None,
                },
            );
        }
        self.items.get_mut(&id).expect("just made")
    }

    /// Whether a value of `len` encoded bytes goes in its block entry (ADR-rdb-0016 §4, §5).
    const fn fits_inline(&self, len: usize) -> bool {
        !self.list.records && len <= inline_limit(self.block_max)
    }

    /// Put `entry`, an item of `len` value bytes, at `pos`; with `push`, at the end of the last
    /// block (ADR-rdb-0016 §4's boundary tie).
    fn insert(&mut self, pos: u64, push: bool, entry: Entry, len: u64) -> Result<(), ValueError> {
        let (i, at) = if push {
            let last = self.list.blocks.len() - 1;
            (last, self.list.blocks[last].count)
        } else {
            self.locate(pos, true)
        };
        self.open_block(i)?.entries.insert(index(at), entry.clone());
        self.record(i, Change::Insert { at, entry }, 1, i128::from(len))
    }

    /// The entry of the item at `pos`.
    fn entry(&mut self, pos: u64) -> Result<Entry, ValueError> {
        let (i, at) = self.locate(pos, false);
        Ok(self.open_block(i)?.entries[index(at)].clone())
    }

    /// Take the item at `pos` out of its block, taking `len` off the list's bytes; its record is
    /// not touched. Returns its entry. It reads no item record (D4): a Remove measures its item
    /// first, and a Move takes and puts back 0.
    fn take(&mut self, pos: u64, len: u64) -> Result<Entry, ValueError> {
        let (i, at) = self.locate(pos, false);
        let entry = self.open_block(i)?.entries.remove(index(at));
        self.record(i, Change::Remove { at }, -1, -i128::from(len))?;
        Ok(entry)
    }

    /// Give the item at `pos` the value `value`, `len` encoded bytes: in its block entry when
    /// `envelope` is `None`, else in its record `envelope` (ADR-rdb-0016 §5).
    fn replace(
        &mut self,
        pos: u64,
        value: &Value,
        len: usize,
        envelope: Option<Bytes>,
    ) -> Result<(), ValueError> {
        let (i, at) = self.locate(pos, false);
        let old = self.open_block(i)?.entries[index(at)].clone();
        let old_len = self.item_len(&old)?;
        let id = self.list.id(old.n);
        let mut entry = old.clone();
        match envelope {
            None => {
                // Into the block. An out-of-line item's record goes; one never stored writes
                // nothing. An inline entry has no record to delete: a compile gives an item a
                // record only by making its entry bare.
                if old.inline.is_none() {
                    self.touch(id).value = None;
                }
                entry.inline = Some(value.clone());
            }
            Some(envelope) => {
                if old.inline.is_some() {
                    // Out of the block: its key must hold no record in the overlay view.
                    if self.has_record(id) {
                        return Err(ValueError::Corrupt(Corrupt::OrphanElement));
                    }
                    entry.inline = None;
                }
                self.touch(id).value = Some(envelope);
            }
        }
        self.open_block(i)?.entries[index(at)] = entry.clone();
        let bytes = i128::from(len_u64(len)) - i128::from(old_len);
        self.record(i, Change::Replace { at, entry }, 0, bytes)
    }

    /// Check `at` against the list as the earlier ops left it.
    fn check_position(&self, at: u64, inclusive: bool) -> Result<(), ValueError> {
        let inside = if inclusive {
            at <= self.list.count
        } else {
            at < self.list.count
        };
        if inside {
            Ok(())
        } else {
            Err(ApplyError::PositionInvalid {
                position: at,
                len: self.list.count,
            }
            .into())
        }
    }

    /// Where `value` goes: its encoding, and its record when it does not go in its block entry
    /// (ADR-rdb-0016 §5). A record is within [`MAX_ITEM`].
    fn place(&self, value: &Value) -> Result<(usize, Option<Bytes>), ValueError> {
        let payload = encode(value).map_err(ApplyError::from)?;
        if self.fits_inline(payload.len()) {
            return Ok((payload.len(), None));
        }
        let envelope = seal(Kind::Document, &payload).map_err(ApplyError::from)?;
        if envelope.len() > MAX_ITEM {
            return Err(ApplyError::TooLarge {
                limit: SizeLimit::Item,
            }
            .into());
        }
        Ok((payload.len(), Some(envelope)))
    }

    /// Run one op; returns the id it minted, if any.
    fn apply(&mut self, op: &ListOp) -> Result<Option<u128>, ValueError> {
        match op {
            ListOp::Push(value) => self.add(self.list.count, true, value).map(Some),
            ListOp::Insert { at, value } => self.add(*at, false, value).map(Some),
            ListOp::Remove { at } => {
                self.check_position(*at, false)?;
                let entry = self.entry(*at)?;
                let len = self.item_len(&entry)?;
                let entry = self.take(*at, len)?;
                // An inline item has no record to delete.
                if entry.inline.is_none() {
                    let id = self.list.id(entry.n);
                    self.touch(id).value = None;
                }
                Ok(None)
            }
            ListOp::Replace { at, value } => {
                self.check_position(*at, false)?;
                let (len, envelope) = self.place(value)?;
                self.replace(*at, value, len, envelope)?;
                Ok(None)
            }
            ListOp::Move { from, to } => {
                self.check_position(*from, false)?;
                self.check_position(*to, false)?;
                let entry = self.take(*from, 0)?;
                self.insert(*to, false, entry, 0)?;
                Ok(None)
            }
        }
    }

    /// Push (`push`, with `at` the count) or insert `value` at `at` under a new id.
    fn add(&mut self, at: u64, push: bool, value: &Value) -> Result<u128, ValueError> {
        self.check_position(at, true)?;
        let (len, envelope) = self.place(value)?;
        let n = self.mint()?;
        let id = self.list.id(n);
        if self.has_record(id) {
            return Err(ValueError::Corrupt(Corrupt::OrphanElement));
        }
        let inline = match envelope {
            None => Some(value.clone()),
            Some(envelope) => {
                self.touch(id).value = Some(envelope);
                None
            }
        };
        self.insert(at, push, Entry { n, inline }, len_u64(len))?;
        Ok(id)
    }

    /// End the compile (ADR-rdb-0016 §4): retire the emptied blocks, fold or slot each touched
    /// block, split the folds over B in block order, then find at most one merge-back. A record
    /// minted and freed in this compile writes nothing.
    fn finish(mut self) -> Result<Finished, ValueError> {
        let mut items = BTreeMap::new();
        for (&id, slot) in &self.items {
            if slot.stored || slot.value.is_some() {
                items.insert(item_key(self.root, id), slot.value.clone());
            }
        }
        let mut fates = BTreeMap::new();
        let retired = self.retire(&mut fates);
        self.settle(&mut fates)?;
        // No merge-back beside a retire (ADR-rdb-0016 §4).
        let merged = if retired {
            None
        } else {
            self.merge_back(&fates)?
        };
        let writes = emit(self.root, &self.list, &items, &fates)?;
        let merged = merged
            .map(|(list, fates)| {
                let writes = emit(self.root, &list, &items, &fates)?;
                Ok::<_, ValueError>((list, writes))
            })
            .transpose()?;
        Ok(Finished {
            list: self.list,
            writes,
            merged,
        })
    }

    /// Retire every block the ops emptied that is not the only one: its base and the slot of
    /// every op no 1 … `head` go, present or not (G62). When the ops emptied the list, the first
    /// block stays. Returns whether any block was retired.
    fn retire(&mut self, fates: &mut BTreeMap<u64, Fate>) -> bool {
        let all_empty = self.list.count == 0;
        let mut kept = Vec::with_capacity(self.list.blocks.len());
        for (i, block) in self.list.blocks.iter().enumerate() {
            if block.count == 0 && !(all_empty && i == 0) {
                self.blocks
                    .remove(&block.n)
                    .expect("a root names an empty block only alone, so the ops emptied this one");
                let keys = block_keys(self.root, self.list.id(block.n), block.head);
                fates.insert(block.n, Fate::Retired(keys));
            } else {
                kept.push(*block);
            }
        }
        let retired = kept.len() < self.list.blocks.len();
        self.list.blocks = kept;
        retired
    }

    /// Fold or slot each touched block, in block order, and split a fold over B: the last block
    /// at its end (G35), any other in halves (G36). A piece over B is refused (G61). At the block
    /// cap a fold over B is written unsplit (G63), unless the ops grew the block past B, which is
    /// refused even when it would take only slots (G38).
    fn settle(&mut self, fates: &mut BTreeMap<u64, Fate>) -> Result<(), ValueError> {
        let block_max = self.block_max;
        let too_large = |len| -> ValueError {
            ApplyError::TooLarge {
                limit: SizeLimit::List { len, block_max },
            }
            .into()
        };
        // A delta that empties the list writes its first block empty, `folded = head`.
        let emptied = self.list.count == 0;
        let mut i = 0;
        while i < self.list.blocks.len() {
            let block = self.list.blocks[i];
            i += 1;
            let Some(work) = self.blocks.get(&block.n) else {
                continue;
            };
            let first = block.head - len_u64(work.ops.len()) + 1;
            let slots = (first..)
                .zip(&work.ops)
                .map(|(op, change)| {
                    let payload = encode(&Value::Array(vec![uint(op), change.value()]))
                        .map_err(ApplyError::from)?;
                    Ok((op, payload))
                })
                .collect::<Result<Vec<_>, ValueError>>()?;
            let new_bytes: usize = slots.iter().map(|(_, payload)| payload.len()).sum();
            // Fold when the pending ops would pass the slots, or their bytes a quarter of the
            // base's; a block this compile made, or a base over B, always folds (ADR-rdb-0016
            // §3, G66).
            let fold = emptied
                || work.base_len.is_none_or(|base_len| {
                    base_len > self.block_max
                        || block.head - work.folded > SLOTS
                        || 4 * (work.pending_bytes + new_bytes) > base_len
                });
            let payload =
                encode(&block_value(&work.entries, block.head)).map_err(ApplyError::from)?;
            let at_cap = self.list.blocks.len() >= MAX_BLOCKS;
            if at_cap
                && payload.len() > self.block_max
                && items_len(&work.entries)? > work.start_items
            {
                return Err(too_large(payload.len()));
            }
            if !fold {
                fates.insert(block.n, Fate::Slots(slots));
                continue;
            }
            if payload.len() <= self.block_max || at_cap {
                fates.insert(block.n, Fate::Base(payload));
                continue;
            }
            let entries = &work.entries;
            let k = if i == self.list.blocks.len() {
                end_split(entries, block.head, self.block_max)?
            } else {
                halves(entries)?
            };
            let left = encode(&block_value(&entries[..k], block.head)).map_err(ApplyError::from)?;
            let right = encode(&block_value(&entries[k..], 0)).map_err(ApplyError::from)?;
            if left.len() > self.block_max || right.len() > self.block_max {
                return Err(too_large(left.len().max(right.len())));
            }
            // A split moves entries, not bytes: it reads no item record (D4).
            let moved = entries[k..].to_vec();
            let n = self.mint()?;
            let count = len_u64(moved.len());
            self.list.blocks[i - 1].count -= count;
            self.list.blocks.insert(i, BlockRef { n, count, head: 0 });
            self.blocks
                .get_mut(&block.n)
                .expect("the block being split is open")
                .entries
                .truncate(k);
            self.blocks.insert(
                n,
                Work {
                    base_len: None,
                    folded: 0,
                    pending_bytes: 0,
                    start_items: 0,
                    entries: moved,
                    ops: Vec::new(),
                },
            );
            fates.insert(block.n, Fate::Base(left));
            fates.insert(n, Fate::Base(right));
            // The new block is written whole; skip it.
            i += 1;
        }
        Ok(())
    }

    /// The first merge-back in block order (G37): a block this compile folded under B/4, with
    /// its left neighbour, else its right, when the pair is at most ¾ · B. The left block
    /// survives, `folded = head`; every key the right one can hold is deleted, and this
    /// compile's slots to either are dropped. Returns the root and fates with it, or `None`.
    fn merge_back(&self, fates: &Fates) -> Result<Option<(Root, Fates)>, ValueError> {
        let blocks = &self.list.blocks;
        for (i, block) in blocks.iter().enumerate() {
            let Some(Fate::Base(payload)) = fates.get(&block.n) else {
                continue;
            };
            if 4 * payload.len() >= self.block_max {
                continue;
            }
            let neighbours = [i.checked_sub(1), (i + 1 < blocks.len()).then_some(i + 1)];
            for j in neighbours.into_iter().flatten() {
                let (l, r) = (i.min(j), i.max(j));
                let (left, right) = (blocks[l], blocks[r]);
                let mut entries = self.replayed(&left)?;
                entries.extend(self.replayed(&right)?);
                let pair = encode(&block_value(&entries, left.head)).map_err(ApplyError::from)?;
                if 4 * pair.len() > 3 * self.block_max {
                    continue;
                }
                let mut list = self.list.clone();
                list.blocks.remove(r);
                list.blocks[l].count += right.count;
                let mut merged = fates.clone();
                merged.insert(left.n, Fate::Base(pair));
                let right_keys = block_keys(self.root, self.list.id(right.n), right.head);
                merged.insert(right.n, Fate::Retired(right_keys));
                return Ok(Some((list, merged)));
            }
        }
        Ok(None)
    }

    /// A block's entries as the ops leave them: this compile's copy, or the block read,
    /// replayed and checked.
    fn replayed(&self, block: &BlockRef) -> Result<Vec<Entry>, ValueError> {
        if let Some(work) = self.blocks.get(&block.n) {
            return Ok(work.entries.clone());
        }
        let id = self.list.id(block.n);
        let loaded = load_block(self.snapshot, self.root, self.root_version, id, block)?;
        Ok(loaded.entries)
    }
}

/// Compile `ops` against the list at `root` into one root `Put` and one write per item, block
/// or slot that changed, in key order (ADR-rdb-0016 §5).
///
/// - [`Expected::Version`]: the root must exist, be at that version and be a list.
/// - [`Expected::Absent`]: a create, seeded with `seed = snapshot.at()`, of an ordinary list
///   (`records` false; [`create_list`] takes the flag) with one empty block. The root `Put`
///   carries [`Condition::Absent`]. When the root is absent here, any record under the item or
///   block range is [`Corrupt::OrphanElement`].
///
/// # Errors
/// [`ApplyError::InvalidBlockSize`], [`ApplyError::ObjectAbsent`], [`ApplyError::VersionConflict`],
/// [`ApplyError::KindMismatch`], [`ApplyError::PositionInvalid`], [`ApplyError::TooLarge`],
/// [`ApplyError::TooDeep`], [`ApplyError::TooManyWrites`], or [`ValueError::Corrupt`].
pub fn compile_list(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    expected: Expected,
    block_max: usize,
    ops: &[ListOp],
) -> Result<ListCompiled, ValueError> {
    compile(snapshot, root, expected, false, block_max, ops)
}

/// Compile the create of the list at `root`, then `ops` on it, as [`compile_list`] with
/// [`Expected::Absent`]. With `records`, every item the list ever holds keeps its own record and
/// no block entry holds a value (ADR-rdb-0016 §1); no later op changes it.
///
/// # Errors
/// As [`compile_list`].
pub fn create_list(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    records: bool,
    block_max: usize,
    ops: &[ListOp],
) -> Result<ListCompiled, ValueError> {
    compile(snapshot, root, Expected::Absent, records, block_max, ops)
}

/// [`compile_list`], with `records` for a create; an update reads it from the root.
fn compile(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    expected: Expected,
    records: bool,
    block_max: usize,
    ops: &[ListOp],
) -> Result<ListCompiled, ValueError> {
    if !(MIN_BLOCK_MAX..=MAX_BLOCK_MAX).contains(&block_max) {
        return Err(ApplyError::InvalidBlockSize { found: block_max }.into());
    }
    if root.as_bytes().len() - SCOPE_AND_SUB > MAX_ID_ESCAPED {
        return Err(ApplyError::TooLarge {
            limit: SizeLimit::ObjectId,
        }
        .into());
    }
    let mut blocks = BTreeMap::new();
    let (found, root_version, expected_version, conditions) = match expected {
        Expected::Absent => {
            let root_absent = snapshot.version(Namespace::User, root.as_bytes()).is_none();
            if root_absent && any_item_or_block(snapshot, root) {
                return Err(ValueError::Corrupt(Corrupt::OrphanElement));
            }
            // Block 0 takes n = 0; items start at 1.
            let fresh = Root {
                next: 1,
                seed: snapshot.at().0,
                bytes: 0,
                count: 0,
                blocks: vec![BlockRef {
                    n: 0,
                    count: 0,
                    head: 0,
                }],
                records,
            };
            blocks.insert(
                0,
                Work {
                    base_len: None,
                    folded: 0,
                    pending_bytes: 0,
                    entries: Vec::new(),
                    start_items: items_len(&[])?,
                    ops: Vec::new(),
                },
            );
            let absent = Condition::Absent {
                key: root.to_bytes(),
            };
            // A fresh list names no stored block or item, so the root version is never compared.
            (fresh, 0, None, vec![absent])
        }
        Expected::Version(want) => {
            check_version(snapshot, root, want)?;
            let (_, bytes) = record(snapshot, root.as_bytes())?.ok_or(ApplyError::ObjectAbsent)?;
            (open_root(&bytes)?, want, Some(want), Vec::new())
        }
    };

    let mut draft = Draft {
        snapshot,
        root,
        root_version,
        block_max,
        list: found,
        blocks,
        items: BTreeMap::new(),
    };
    let mut minted = Vec::new();
    for op in ops {
        if let Some(id) = draft.apply(op)? {
            minted.push(id);
        }
    }

    let finished = draft.finish()?;
    let request = |list: &Root, writes: BTreeMap<Bytes, Option<Bytes>>| {
        let value = seal(Kind::List, &list.payload()?).map_err(ApplyError::from)?;
        // The root sorts before every item, block and slot (sub 0x00 < 0x02 < 0x03), so this
        // keeps key order.
        let mut mutations = vec![Mutation::Put {
            key: root.to_bytes(),
            value,
            expected_version,
        }];
        mutations.extend(writes.into_iter().map(|(key, value)| match value {
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
        Ok::<_, ValueError>(mutations)
    };
    // The kernel's own measure of the record it would ship (L-R186v).
    let fits = |mutations: &[Mutation]| {
        mutations.len() <= MAX_REQUEST_MUTATIONS
            && record_len(conditions.len(), mutations) <= MAX_ENVELOPE_BYTES
    };
    // A merge-back is taken only when it fits; it is never a reason to refuse (G37).
    let merged = match finished.merged {
        Some((list, writes)) => Some(request(&list, writes)?).filter(|m| fits(m)),
        None => None,
    };
    let mutations = match merged {
        Some(mutations) => mutations,
        None => request(&finished.list, finished.writes)?,
    };
    if mutations.len() > MAX_REQUEST_MUTATIONS {
        return Err(ApplyError::TooManyWrites {
            writes: mutations.len(),
        }
        .into());
    }
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

/// Compile the deletion of the empty list at `root`, at `version` (ADR-rdb-0016 §5): the root,
/// its one block's base and the slot key of every op no `1 … head`, present or not. The block
/// is replayed and checked first. Two orphan checks read one record each: the first record
/// under the item or block range must be the base, and none under the block range may follow
/// its last slot (G59).
///
/// # Errors
/// [`ApplyError::ObjectAbsent`], [`ApplyError::VersionConflict`], [`ApplyError::KindMismatch`],
/// [`ApplyError::ListNotEmpty`], [`Corrupt::OrphanElement`] (an item or block record beside an
/// empty list), [`Corrupt::Key`] (such a key with a malformed tail), or any other
/// [`ValueError::Corrupt`] of the root or its block.
pub fn drop_list(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    version: u64,
) -> Result<ListCompiled, ValueError> {
    check_version(snapshot, root, version)?;
    let (_, bytes) = record(snapshot, root.as_bytes())?.ok_or(ApplyError::ObjectAbsent)?;
    let found = open_root(&bytes)?;
    if found.count > 0 {
        return Err(ApplyError::ListNotEmpty { count: found.count }.into());
    }
    let [block] = found.blocks.as_slice() else {
        unreachable!("open_root refuses a count-0 block beside others, so an empty list has one");
    };
    let id = found.id(block.n);
    load_block(snapshot, root, version, id, block)?;
    let base = block_key(root, id);
    let (items, blocks) = (root.sub_prefix(SUB_ITEM), root.sub_prefix(SUB_BLOCK));
    let under = |key: &Bytes| key.starts_with(&items) || key.starts_with(&blocks);
    match snapshot.scan(Namespace::User, &items, 1).first() {
        Some((key, _)) if *key == base => {}
        Some((key, _)) if under(key) => return Err(stray(root, key)),
        // `load_block` just read the base, so the scan cannot pass it.
        _ => {
            return Err(ValueError::Corrupt(Corrupt::Block {
                id,
                fault: BlockFault::Missing,
            }))
        }
    }
    let mut past = base.to_vec();
    past.push(LIST_SLOTS);
    if let Some((key, _)) = snapshot.scan(Namespace::User, &past, 1).first() {
        if key.starts_with(&blocks) {
            return Err(stray(root, key));
        }
    }
    let mut mutations = vec![Mutation::Delete {
        key: root.to_bytes(),
        expected_version: Some(version),
    }];
    mutations.extend(
        block_keys(root, id, block.head)
            .into_iter()
            .map(|key| Mutation::Delete {
                key,
                expected_version: None,
            }),
    );
    Ok(ListCompiled {
        compiled: Compiled {
            mutations,
            conditions: Vec::new(),
        },
        ids: Vec::new(),
        generation: snapshot.generation(),
    })
}

/// A record a drop's orphan check found under the item or block range that is not its block's
/// (ADR-rdb-0016 §7): a key whose tail no list writes is `Key(..)`, any other an orphan.
fn stray(root: &RootKey, key: &[u8]) -> ValueError {
    let (items, blocks) = (root.sub_prefix(SUB_ITEM), root.sub_prefix(SUB_BLOCK));
    let fault = if let Some(tail) = key.strip_prefix(items.as_slice()) {
        (tail.len() != LIST_ID_LEN).then_some(KeyError::ListIdTail { len: tail.len() })
    } else if let Some(tail) = key.strip_prefix(blocks.as_slice()) {
        match *tail {
            [.., slot] if tail.len() == LIST_ID_LEN + 1 && slot >= LIST_SLOTS => {
                Some(KeyError::SlotOutOfRange { slot })
            }
            _ if tail.len() == LIST_ID_LEN || tail.len() == LIST_ID_LEN + 1 => None,
            _ => Some(KeyError::ListIdTail { len: tail.len() }),
        }
    } else {
        None
    };
    ValueError::Corrupt(fault.map_or(Corrupt::OrphanElement, Corrupt::Key))
}
