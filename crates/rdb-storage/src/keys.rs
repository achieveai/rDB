//! Physical layout: column families, key prefix, value framing and the format marker.
//!
//! [`FORMAT_VERSION`] `1`, settled by ADR-rdb-0010 decision 11. S0's provisional `0` is refused
//! at open, so no S0 data directory is read as format 1.
//!
//! - **Key:** `partition u32 BE | generation u64 BE | ns u8 | key`. Fixed-width, so a key is
//!   unambiguous, and partition- and generation-contiguous, so a prefix export or a
//!   `delete_range` over one lineage stays possible. Widening either id is a format bump.
//! - **Column families:** [`Namespace::User`] → `data`, [`Namespace::History`] → `history`,
//!   [`Namespace::Dedup`] → `dedup`, [`Namespace::Progress`] and [`Namespace::Meta`] →
//!   `metadata` (the ns byte separates them). `actor` is created empty.
//! - **Value frame:** `u8 kind | u64 BE version | bytes`; kind `0` a value, kind `1` a tombstone
//!   (no bytes; only in a lineage with a parent). The version is the writing
//!   batch's sequence, as `MemoryEngine` sets it.
//! - **Engine-private records** live in `metadata` under ns byte `0xFF`: per lineage `applied`
//!   and `durable` (u64 BE, no frame); on an inherited lineage, `parent` ([`Link`], 17 bytes);
//!   on a parent, `sealed` (the child generation, u64 BE); on a child mid full copy, `copying`
//!   (parent and base, 16 bytes). A lineage with no parent has no
//!   `parent` record, never "parent 0": generation 0 is legal (ADR-rdb-0010 decision 11). The
//!   engine-wide format marker is the key `format`, which is shorter than the 13-byte lineage
//!   prefix and so can never be mistaken for a lineage key.

use rdb_core::contracts::ids::{Generation, PartitionId, Seq};
use rdb_core::contracts::storage::Namespace;
use rdb_core::contracts::trace::Version;

/// The on-disk format this build writes and accepts (ADR-rdb-0010 decision 11). `0` was S0's
/// provisional layout and is refused.
pub const FORMAT_VERSION: u32 = 1;

/// Column family for [`Namespace::User`].
pub const CF_DATA: &str = "data";
/// Column family for [`Namespace::History`].
pub const CF_HISTORY: &str = "history";
/// Column family for [`Namespace::Progress`], [`Namespace::Meta`] and engine-private records.
pub const CF_METADATA: &str = "metadata";
/// Column family for [`Namespace::Dedup`].
pub const CF_DEDUP: &str = "dedup";
/// Reserved for actor state (spec §4.1). Created empty; nothing writes it yet.
pub const CF_ACTOR: &str = "actor";

/// The fixed column-family set (spec §4.1). An open refuses any other set.
pub const COLUMN_FAMILIES: [&str; 5] = [CF_DATA, CF_HISTORY, CF_METADATA, CF_DEDUP, CF_ACTOR];

/// Length of the `partition | generation | ns` prefix every lineage key starts with.
pub(crate) const PREFIX_LEN: usize = 4 + 8 + 1;
/// The ns byte of engine-private records. Never a [`Namespace`].
pub(crate) const PRIVATE_NS: u8 = 0xFF;
/// Engine-private record: the lineage's applied watermark.
pub(crate) const APPLIED_KEY: &[u8] = b"applied";
/// Engine-private record: the lineage's durable watermark.
pub(crate) const DURABLE_KEY: &[u8] = b"durable";
/// Engine-private record: the inherited lineage's [`Link`].
pub(crate) const PARENT_KEY: &[u8] = b"parent";
/// Engine-private record: on a parent, the child generation that sealed it.
pub(crate) const SEALED_KEY: &[u8] = b"sealed";
/// Engine-private record: on a child mid full copy, `parent u64 BE | base u64 BE` (16 bytes).
/// Written by the copy's first batch, deleted by its switch batch.
pub(crate) const COPYING_KEY: &[u8] = b"copying";

/// Encode a `copying` record.
pub(crate) fn encode_copying(parent: Generation, base: Seq) -> [u8; 16] {
    let mut out = [0; 16];
    out[0..8].copy_from_slice(&parent.0.to_be_bytes());
    out[8..16].copy_from_slice(&base.0.to_be_bytes());
    out
}

/// Decode a `copying` record; `None` when it is not exactly 16 bytes.
pub(crate) fn decode_copying(raw: &[u8]) -> Option<(Generation, Seq)> {
    if raw.len() != 16 {
        return None;
    }
    Some((
        Generation(decode_mark(&raw[0..8])?),
        Seq(decode_mark(&raw[8..16])?),
    ))
}
/// The engine-wide format marker key in `metadata`. Shorter than [`PREFIX_LEN`] on purpose.
pub(crate) const FORMAT_KEY: &[u8] = b"format";
/// The value frame's kind byte for a value.
pub(crate) const FRAME_VALUE: u8 = 0;
/// The value frame's kind byte for a tombstone (no bytes follow the version).
pub(crate) const FRAME_TOMBSTONE: u8 = 1;
/// Length of the value frame header: kind byte plus u64 record version.
pub(crate) const FRAME_LEN: usize = 1 + 8;

/// The ns byte of a namespace.
pub(crate) const fn ns_byte(ns: Namespace) -> u8 {
    match ns {
        Namespace::User => 0,
        Namespace::History => 1,
        Namespace::Dedup => 2,
        Namespace::Progress => 3,
        Namespace::Meta => 4,
    }
}

/// The namespace an ns byte names, or `None` for the private byte or an unknown one.
pub(crate) const fn ns_from_byte(byte: u8) -> Option<Namespace> {
    match byte {
        0 => Some(Namespace::User),
        1 => Some(Namespace::History),
        2 => Some(Namespace::Dedup),
        3 => Some(Namespace::Progress),
        4 => Some(Namespace::Meta),
        _ => None,
    }
}

/// The column family a namespace's records live in.
#[must_use]
pub const fn cf_for(ns: Namespace) -> &'static str {
    match ns {
        Namespace::User => CF_DATA,
        Namespace::History => CF_HISTORY,
        Namespace::Dedup => CF_DEDUP,
        Namespace::Progress | Namespace::Meta => CF_METADATA,
    }
}

/// The physical key of `key` in `ns` of lineage `(partition, generation)`.
#[must_use]
pub fn encode_key(
    partition: PartitionId,
    generation: Generation,
    ns: Namespace,
    key: &[u8],
) -> Vec<u8> {
    raw_key(partition, generation, ns_byte(ns), key)
}

/// The physical key of an engine-private record of one lineage.
pub(crate) fn private_key(partition: PartitionId, generation: Generation, name: &[u8]) -> Vec<u8> {
    raw_key(partition, generation, PRIVATE_NS, name)
}

fn raw_key(partition: PartitionId, generation: Generation, ns: u8, key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(PREFIX_LEN + key.len());
    out.extend_from_slice(&partition.0.to_be_bytes());
    out.extend_from_slice(&generation.0.to_be_bytes());
    out.push(ns);
    out.extend_from_slice(key);
    out
}

/// Split a lineage key into `(partition, generation, ns byte, key)`; `None` when it is shorter
/// than the prefix (the format marker is the only such key).
pub(crate) fn decode_key(raw: &[u8]) -> Option<(PartitionId, Generation, u8, &[u8])> {
    if raw.len() < PREFIX_LEN {
        return None;
    }
    let partition = u32::from_be_bytes(raw[0..4].try_into().ok()?);
    let generation = u64::from_be_bytes(raw[4..12].try_into().ok()?);
    Some((
        PartitionId(partition),
        Generation(generation),
        raw[12],
        &raw[PREFIX_LEN..],
    ))
}

/// Frame a stored value with its record version.
pub(crate) fn frame(version: Version, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_LEN + value.len());
    out.push(FRAME_VALUE);
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(value);
    out
}

/// A tombstone: the delete at `version` of a key in a lineage with a parent (ADR-rdb-0010
/// decision 4). It reads as absent and stops the walk.
pub(crate) fn tombstone(version: Version) -> Vec<u8> {
    let mut out = Vec::with_capacity(FRAME_LEN);
    out.push(FRAME_TOMBSTONE);
    out.extend_from_slice(&version.to_be_bytes());
    out
}

/// A decoded value frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Frame<'a> {
    /// A value written at a version.
    Value(Version, &'a [u8]),
    /// A delete at a version, in a lineage with a parent.
    Tombstone(Version),
}

/// Decode a framed value; `None` when the header is short, the kind byte is unknown, or a
/// tombstone carries bytes.
pub(crate) fn unframe(raw: &[u8]) -> Option<Frame<'_>> {
    if raw.len() < FRAME_LEN {
        return None;
    }
    let version = u64::from_be_bytes(raw[1..FRAME_LEN].try_into().ok()?);
    match raw[0] {
        FRAME_VALUE => Some(Frame::Value(version, &raw[FRAME_LEN..])),
        FRAME_TOMBSTONE if raw.len() == FRAME_LEN => Some(Frame::Tombstone(version)),
        _ => None,
    }
}

/// Decode an engine-private u64 watermark; `None` when it is not exactly 8 bytes.
pub(crate) fn decode_mark(raw: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(raw.try_into().ok()?))
}

/// An inherited lineage's record: the generation it reads through to, and the cutoff.
///
/// Stored as `parent u64 BE | base u64 BE | flags u8` (17 bytes). Flag bit 0 is `copied`; any
/// other bit is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Link {
    /// The generation a read falls through to. Always older than the lineage's own.
    pub parent: Generation,
    /// The cutoff: the lineage starts at the parent's state at this sequence.
    pub base: Seq,
    /// The lineage was full-copied as of `base`: only `History` (`seq <= base`) falls through.
    pub copied: bool,
}

/// Length of an encoded [`Link`].
const LINK_LEN: usize = 8 + 8 + 1;
/// [`Link::copied`]'s flag bit.
const LINK_COPIED: u8 = 1;

impl Link {
    /// The stored bytes.
    pub(crate) fn encode(self) -> [u8; LINK_LEN] {
        let mut out = [0; LINK_LEN];
        out[0..8].copy_from_slice(&self.parent.0.to_be_bytes());
        out[8..16].copy_from_slice(&self.base.0.to_be_bytes());
        out[16] = if self.copied { LINK_COPIED } else { 0 };
        out
    }

    /// `None` on a wrong length or an unknown flag bit.
    pub(crate) fn decode(raw: &[u8]) -> Option<Self> {
        if raw.len() != LINK_LEN || raw[16] & !LINK_COPIED != 0 {
            return None;
        }
        Some(Self {
            parent: Generation(decode_mark(&raw[0..8])?),
            base: Seq(decode_mark(&raw[8..16])?),
            copied: raw[16] & LINK_COPIED != 0,
        })
    }
}
