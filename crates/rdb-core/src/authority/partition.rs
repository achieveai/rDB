//! The `partitions/{id}` record: its shape, its CAS body, and the ownership classification A1
//! runs over a read of it (team kernel-a `design.md` §5).
//!
//! The sibling of [`crate::authority::grant`], and deliberately the same shape of API: public
//! fields, [`PartitionRecord::encode`], [`PartitionRecord::decode`], [`classify`]. Everything
//! module `grant`'s header says about scope and about the byte layout applies here unchanged —
//! the layout is A1's M7 spelling of an opaque body, not a canonical codec, and it goes when
//! package C0's codec lands without moving the rule.
//!
//! # Why it exists at all (lead ruling A-R34)
//!
//! [`crate::contracts::control::ControlEvent::FamilySnapshot`] carries
//! [`crate::contracts::control::ControlRecord`]s whose values are opaque bytes. A fixture that
//! wants to hand A1 a family of partitions it owns has no way to author those bytes unless one
//! module says what they are. This is that module. A fixture builds a [`PartitionRecord`], calls
//! [`PartitionRecord::encode`], and the same code path A1 uses in production reads it back.
//!
//! # What is **not** here
//!
//! No lineage digest and no replica set. `design.md` §2.4 compares four things on this record —
//! the owner, the generation, the owner epoch and the config version — and a field no rule reads
//! is a field that drifts. The placement side is package R1's. The fifth field, the lifecycle, is
//! read by the takeover rows (`design.md` §2.6a, lead ruling A-R51). No candidate id: kernel-b
//! resolves racing recoverers by CAS.

use bytes::Bytes;

use crate::contracts::authority::DenyReason;
use crate::contracts::digest::Digest;
use crate::contracts::ids::{
    BootId, ConfigVersion, Generation, NodeId, OwnerEpoch, PartitionId, Revision, Seq,
};

/// The version byte every encoded record starts with.
///
/// 2 since [`PartitionRecord::lifecycle`] (lead ruling A-R51). A version-1 body no longer
/// decodes: nothing durable was ever written in it, and guessing `Serving` for a missing
/// lifecycle is exactly the guess that would hide a takeover in progress.
const LAYOUT_VERSION: u8 = 2;

/// Version byte, `PartitionId`, `NodeId`, `Generation`, `OwnerEpoch`, `ConfigVersion`,
/// lifecycle byte.
const ENCODED_LEN: usize = 1 + 4 + 4 + 8 + 8 + 8 + 1;

/// The version byte of a record that also carries its [`PartitionRoot`] (M10, arch R3b).
const ROOTED_LAYOUT_VERSION: u8 = 3;

/// Layout 2's fields, then mode, `owner_boot`, `base_seq`, `base_digest`,
/// `predecessor_generation`, `predecessor_cutoff`, the instance id and the run.
const ROOTED_LEN: usize = ENCODED_LEN + 1 + 8 + 8 + 32 + 8 + 8 + INSTANCE_LEN + 8;

/// The length of an instance id: a uuid's 16 bytes.
pub const INSTANCE_LEN: usize = 16;

/// Where a partition is in spec §7.3's ownership transfer (lead ruling A-R51; ADR-rdb-0008's
/// "lifecycle state" in `partitions/{id}`).
///
/// One enum and not a lifecycle plus a drained flag, because a `Serving` record with the
/// drained bit set is a state that must not be spellable.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PartitionLifecycle {
    /// The owner serves it. No takeover is in progress, and A1 starts none.
    #[default]
    Serving,
    /// Spec §7.3 step 1: the planner has CASed the partition to `FENCING`. A takeover of the
    /// named owner may begin; its grant still has to be read frozen.
    Fencing,
    /// `Fencing`, and the old owner's irrevocable revocation ACK is durable (spec §7.3 step 2).
    /// The revision of the read that saw it is a `DurableDrain` proof's `ack_revision`.
    FencingDrained,
}

impl PartitionLifecycle {
    const fn to_byte(self) -> u8 {
        match self {
            Self::Serving => 0,
            Self::Fencing => 1,
            Self::FencingDrained => 2,
        }
    }

    const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Serving),
            1 => Some(Self::Fencing),
            2 => Some(Self::FencingDrained),
            _ => None,
        }
    }
}

/// Which step of a recovery a layout-3 record was written by (M10, arch R3b).
///
/// A reopen reads it to tell a generation that was committed but never activated, which it may
/// roll forward (lead ruling A-R161), from one that served writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordMode {
    /// F1's recovery CAS wrote it: the generation is committed, not yet serving.
    Committed,
    /// F1's activation CAS wrote it: the generation serves.
    Active,
}

impl RecordMode {
    const fn to_byte(self) -> u8 {
        match self {
            Self::Committed => 0,
            Self::Active => 1,
        }
    }

    const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Committed),
            1 => Some(Self::Active),
            _ => None,
        }
    }
}

/// What a layout-3 record adds to [`PartitionRecord`]: where the generation's history starts,
/// which owner boot and which run wrote it, and which rDB instance it belongs to (M10, arch R3b).
///
/// A separate type, not more fields on [`PartitionRecord`], so every M7 fixture that builds a
/// record literally still compiles and still writes layout 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PartitionRoot {
    /// Which recovery step wrote the record.
    pub mode: RecordMode,
    /// The boot of the owner that wrote it.
    pub owner_boot: BootId,
    /// The sequence the generation's history starts after.
    pub base_seq: Seq,
    /// The lineage digest at [`Self::base_seq`].
    pub base_digest: Digest,
    /// The generation this one succeeds.
    pub predecessor_generation: Generation,
    /// The last sequence kept from [`Self::predecessor_generation`].
    pub predecessor_cutoff: Seq,
    /// The rDB instance the record belongs to.
    pub instance: [u8; INSTANCE_LEN],
    /// The control revision the recovery was proven at: its fence's `control_revision`.
    pub run: Revision,
}

impl PartitionRoot {
    /// The root a layout-3 body carries, or `None` for a layout-2 body or bytes that are not a
    /// record. A caller that needs the root treats `None` as "this record has no root", never as
    /// a zero root.
    #[must_use]
    pub fn decode(body: &[u8]) -> Option<Self> {
        if body.len() != ROOTED_LEN || body[0] != ROOTED_LAYOUT_VERSION {
            return None;
        }
        PartitionRecord::decode(body)?;
        let u64_at = |offset: usize| -> u64 {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(&body[offset..offset + 8]);
            u64::from_le_bytes(buf)
        };
        let mut at = ENCODED_LEN;
        let mode = RecordMode::from_byte(body[at])?;
        at += 1;
        let owner_boot = BootId(u64_at(at));
        at += 8;
        let base_seq = Seq(u64_at(at));
        at += 8;
        let mut digest = [0_u8; 32];
        digest.copy_from_slice(&body[at..at + 32]);
        at += 32;
        let predecessor_generation = Generation(u64_at(at));
        at += 8;
        let predecessor_cutoff = Seq(u64_at(at));
        at += 8;
        let mut instance = [0_u8; INSTANCE_LEN];
        instance.copy_from_slice(&body[at..at + INSTANCE_LEN]);
        at += INSTANCE_LEN;
        let run = Revision(u64_at(at));
        Some(Self {
            mode,
            owner_boot,
            base_seq,
            base_digest: Digest(digest),
            predecessor_generation,
            predecessor_cutoff,
            instance,
            run,
        })
    }
}

/// One `partitions/{id}` record: who owns this partition, and under which lineage.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionRecord {
    /// Which partition the record is about.
    pub partition: PartitionId,
    /// The node the control plane currently names as owner.
    pub owner: NodeId,
    /// Partition-history incarnation (spec §2).
    pub generation: Generation,
    /// Ownership transition counter inside that generation (spec §7.3).
    pub owner_epoch: OwnerEpoch,
    /// The required-copy set version this ownership was installed under (spec §6.2).
    pub config_version: ConfigVersion,
    /// Where the partition is in an ownership transfer. A1 starts a takeover only for a record
    /// that is not [`PartitionLifecycle::Serving`] (lead ruling A-R51): a freeze is node-scoped,
    /// so without this every held node would take over every partition of a frozen node.
    pub lifecycle: PartitionLifecycle,
}

impl PartitionRecord {
    /// This record as the body of a [`crate::contracts::control::ControlEffect::Cas`] or of a
    /// [`crate::contracts::control::ControlRecord`] in a family snapshot.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        Bytes::from(self.head(LAYOUT_VERSION, ENCODED_LEN))
    }

    /// This record and its root, as layout 3 (M10, arch R3b). [`Self::decode`] reads it back as
    /// the same record; [`PartitionRoot::decode`] reads the root.
    #[must_use]
    pub fn encode_with(&self, root: &PartitionRoot) -> Bytes {
        let mut out = self.head(ROOTED_LAYOUT_VERSION, ROOTED_LEN);
        out.push(root.mode.to_byte());
        out.extend_from_slice(&root.owner_boot.0.to_le_bytes());
        out.extend_from_slice(&root.base_seq.0.to_le_bytes());
        out.extend_from_slice(&root.base_digest.0);
        out.extend_from_slice(&root.predecessor_generation.0.to_le_bytes());
        out.extend_from_slice(&root.predecessor_cutoff.0.to_le_bytes());
        out.extend_from_slice(&root.instance);
        out.extend_from_slice(&root.run.0.to_le_bytes());
        debug_assert_eq!(out.len(), ROOTED_LEN);
        Bytes::from(out)
    }

    /// The fields both layouts share, behind `version`.
    fn head(&self, version: u8, capacity: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(capacity);
        out.push(version);
        out.extend_from_slice(&self.partition.0.to_le_bytes());
        out.extend_from_slice(&self.owner.0.to_le_bytes());
        out.extend_from_slice(&self.generation.0.to_le_bytes());
        out.extend_from_slice(&self.owner_epoch.0.to_le_bytes());
        out.extend_from_slice(&self.config_version.0.to_le_bytes());
        out.push(self.lifecycle.to_byte());
        out
    }

    /// Read a record back, or `None` when the bytes are not one.
    ///
    /// `None` rather than an error, for the reason [`crate::authority::grant::GrantRecord::decode`]
    /// gives: a body A1 cannot read is not a partition it owns, and nothing is guessed from a
    /// partial read.
    #[must_use]
    pub fn decode(body: &[u8]) -> Option<Self> {
        let layout = match body.first() {
            Some(&LAYOUT_VERSION) => ENCODED_LEN,
            Some(&ROOTED_LAYOUT_VERSION) => ROOTED_LEN,
            _ => return None,
        };
        if body.len() != layout {
            return None;
        }
        let u32_at = |offset: usize| -> u32 {
            let mut buf = [0_u8; 4];
            buf.copy_from_slice(&body[offset..offset + 4]);
            u32::from_le_bytes(buf)
        };
        let u64_at = |offset: usize| -> u64 {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(&body[offset..offset + 8]);
            u64::from_le_bytes(buf)
        };
        Some(Self {
            partition: PartitionId(u32_at(1)),
            owner: NodeId(u32_at(5)),
            generation: Generation(u64_at(9)),
            owner_epoch: OwnerEpoch(u64_at(17)),
            config_version: ConfigVersion(u64_at(25)),
            lifecycle: PartitionLifecycle::from_byte(body[33])?,
        })
    }

    /// The lineage this record names, as A1 stores it in `served`.
    #[must_use]
    pub const fn lineage(&self) -> crate::authority::ServedLineage {
        crate::authority::ServedLineage {
            generation: self.generation,
            owner_epoch: self.owner_epoch,
            config_version: self.config_version,
        }
    }
}

/// Why a read of `partitions/{id}` ends this node's right to serve that partition, or `None`
/// when it does not.
///
/// One row, and its absence is the interesting half. `GenerationChanged` is **partition-scoped**
/// (lead ruling A-R33): the control plane moved one partition, which says nothing about the
/// node's grant, so a node fence here would drop the other partitions for no reason.
///
/// A record naming someone else and **no record at all** are the same answer — the second of
/// `design.md`'s two triggers for this fence. That is not a shortcut. A partition this node was
/// serving whose record has been deleted has had its ownership withdrawn exactly as much as one
/// reassigned, and the caller has no record to pass, which is why the `None`-record trigger calls
/// [`classify_absent`] rather than this function.
#[must_use]
pub fn classify(record: &PartitionRecord, us: NodeId) -> Option<DenyReason> {
    if record.owner == us {
        return None;
    }
    Some(DenyReason::GenerationChanged)
}

/// The same answer for a partition whose record is **absent** from the control plane.
///
/// A named function rather than an `Option` threaded through [`classify`], so the two triggers of
/// this one fence are two call sites a reader can find (lead ruling A-R33 counts them
/// separately).
#[must_use]
pub const fn classify_absent() -> DenyReason {
    DenyReason::GenerationChanged
}
