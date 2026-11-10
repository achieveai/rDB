//! Which partition a key lives in, and the record that says how many partitions a database has
//! (issue #79). Pure: no clock, no I/O.
//!
//! [`affinity_hash`] is a **permanent compatibility contract**: its answer for a tenant and an
//! affinity group is the same across every rDB version, so an app may store it. An app stores
//! the affinity group and this hash, never a partition id.
//!
//! [`partition_of`] is valid only for a database whose `routes/0` root says
//! [`RoutesRoot::HASH_VERSION`] 1. Once a partition has split, it can name the parent of a key
//! that a child now owns, and the caller could not tell.

use bytes::Bytes;

use crate::contracts::digest::{Digest, Domain};
use crate::contracts::ids::{AffinityId, PartitionId, TenantId};

/// The most partitions a database may be formed with (customer ruling Q3). The limit is enforced
/// at formation and never faults at runtime; a root naming more does not decode.
pub const MAX_PARTITIONS: u32 = 512;

/// The frozen hash of an affinity group: the first eight bytes, big-endian, of
/// `SHA-256("RDBH" || 0x07 || len(a) || a || len(b) || b)`, where `a` is `tenant` as exactly eight
/// little-endian bytes, `b` is `affinity` as exactly eight little-endian bytes, and `len(x)` is
/// the value 8 as eight little-endian bytes.
///
/// The widths are the contract, whatever the newtypes hold. Stable across rDB versions,
/// permanently; `partition_map_frozen_values` pins it.
#[must_use]
pub fn affinity_hash(tenant: TenantId, affinity: AffinityId) -> u64 {
    let a = u64::from(tenant.0).to_le_bytes();
    let b = affinity.0.to_le_bytes();
    let digest = Digest::of(Domain::Route, &[&a, &b]);
    let mut first = [0u8; 8];
    first.copy_from_slice(&digest.0[..8]);
    u64::from_be_bytes(first)
}

/// The partition, of `partitions` in a database whose root says hash version 1, that holds a key
/// of hash `hash`: `1 + ((hash * partitions) >> 64)`. So each partition is one contiguous range
/// of the hash, and ids are `1..=partitions`. `partitions` is a formation count, at least 1;
/// asked with 0 it answers partition 1.
#[must_use]
pub fn partition_of(hash: u64, partitions: u32) -> PartitionId {
    let scaled = (u128::from(hash) * u128::from(partitions)) >> 64;
    // Below `partitions`, so it fits: `hash < 2^64`.
    PartitionId(1 + scaled as u32)
}

/// The `routes/0` control record: how a database maps a hash to a partition. An N=1 database
/// writes none, and no root means N=1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutesRoot {
    /// How many partitions, fixed at formation.
    pub partition_count: u32,
}

impl RoutesRoot {
    /// The layout this build writes and reads.
    pub const LAYOUT: u8 = 1;
    /// The hash version this build writes and reads: [`partition_of`] over [`affinity_hash`].
    pub const HASH_VERSION: u8 = 1;

    /// This root as the bytes of `routes/0`.
    #[must_use]
    pub fn encode(self) -> Bytes {
        let mut out = Vec::with_capacity(6);
        out.push(Self::LAYOUT);
        out.push(Self::HASH_VERSION);
        out.extend_from_slice(&self.partition_count.to_le_bytes());
        Bytes::from(out)
    }

    /// The root in `body`, or `None` for an unknown layout or hash version, a body of the wrong
    /// length, or a count of 0 or above [`MAX_PARTITIONS`].
    #[must_use]
    pub fn decode(body: &[u8]) -> Option<Self> {
        let [Self::LAYOUT, Self::HASH_VERSION, count @ ..] = body else {
            return None;
        };
        let partition_count = u32::from_le_bytes(count.try_into().ok()?);
        (1..=MAX_PARTITIONS)
            .contains(&partition_count)
            .then_some(Self { partition_count })
    }
}
