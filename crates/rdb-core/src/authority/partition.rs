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
//!
//! Since bug bash #64 (ruling A-R426) a layout-4 record also carries the members its
//! configuration pins, `(copy, node, role)` each, read by a promote after every node restarted.
//! [`PartitionRecord`] still holds no replica set: the members are a sibling decode,
//! [`RecordMember::decode_all`], as the root is.

use bytes::Bytes;

use crate::contracts::authority::DenyReason;
use crate::contracts::digest::Digest;
use crate::contracts::ids::{
    BootId, ConfigVersion, Generation, NodeId, OwnerEpoch, PartitionId, ReplicaRole, Revision, Seq,
};
use crate::contracts::membership::{CopyId, Member, PartitionConfig};

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

/// The version byte of a record that also carries the members its configuration pins
/// (bug bash #64, ruling A-R426): layout 3, then a member count and each member.
const MEMBERED_LAYOUT_VERSION: u8 = 4;

/// One member in a layout-4 record: copy slot, node id, role byte.
const MEMBER_LEN: usize = 1 + 4 + 1;

/// The most members a layout-4 record carries. A body counting more is not a record.
pub const MAX_RECORD_MEMBERS: usize = 8;

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
/// It names the CAS that wrote the record, not whether the generation serves. A recovery that
/// commits with every required copy already holding the barrier goes straight to
/// `PartitionMode::Active` and runs no activation CAS, so its record stays `Committed` while it
/// serves writes. Only a recovery that rebuilds a copy writes `Active`.
///
/// Nothing decides on it today (traced 2026-10-22, M10): a reopen names it only in
/// `OpenError::Stale`'s detail, the heal reads the root's boot and base but not its mode, and
/// F1's own re-read of its CAS ignores the mode. A reader that needs "has this generation served
/// writes" (lead ruling A-R161's roll-forward) cannot get it from this field alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecordMode {
    /// F1's recovery CAS wrote it: the generation is committed. It may already serve, if the
    /// commit needed no rebuild.
    Committed,
    /// F1's activation CAS wrote it, after a rebuild: the generation serves.
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
    /// The root a layout-3 or layout-4 body carries, or `None` for a layout-2 body or bytes that
    /// are not a record. A caller that needs the root treats `None` as "this record has no
    /// root", never as a zero root. Layout 4 is rooted too: its members follow the root (bug
    /// bash #64, ruling A-R426 M2).
    #[must_use]
    pub fn decode(body: &[u8]) -> Option<Self> {
        if !matches!(
            body.first(),
            Some(&(ROOTED_LAYOUT_VERSION | MEMBERED_LAYOUT_VERSION))
        ) {
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

/// One member a layout-4 record pins (bug bash #64, ruling A-R426): a [`Member`] without its
/// boot. A boot is stale after any restart, so a promote re-pins it from the live links.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordMember {
    /// The slot.
    pub copy: CopyId,
    /// The node filling it.
    pub node: NodeId,
    /// What the slot may do.
    pub role: ReplicaRole,
}

impl RecordMember {
    /// The member `member` pins, without its boot.
    #[must_use]
    pub const fn of(member: &Member) -> Self {
        Self {
            copy: member.copy,
            node: member.node,
            role: member.role,
        }
    }

    /// The members `config` pins, in its order: what every writer of a layout-4 record puts
    /// after the root (A-R426 M1).
    #[must_use]
    pub fn pinned(config: &PartitionConfig) -> Vec<Self> {
        config.members.iter().map(Self::of).collect()
    }

    /// This member at `boot`.
    #[must_use]
    pub const fn at(self, boot: BootId) -> Member {
        Member {
            copy: self.copy,
            node: self.node,
            boot,
            role: self.role,
        }
    }

    /// The members a layout-4 body carries, in the order written, or `None` for a layout-2 or
    /// layout-3 body or bytes that are not a record. A caller treats `None` as "this record
    /// names no members", never as an empty set.
    #[must_use]
    pub fn decode_all(body: &[u8]) -> Option<Vec<Self>> {
        if body.first() != Some(&MEMBERED_LAYOUT_VERSION) {
            return None;
        }
        PartitionRecord::decode(body)?;
        // `decode` checked the count, the length and every role byte.
        let count = usize::from(body[ROOTED_LEN]);
        (0..count)
            .map(|index| {
                let at = ROOTED_LEN + 1 + index * MEMBER_LEN;
                let mut node = [0_u8; 4];
                node.copy_from_slice(&body[at + 1..at + 5]);
                Some(Self {
                    copy: CopyId(body[at]),
                    node: NodeId(u32::from_le_bytes(node)),
                    role: role_from_byte(body[at + 5])?,
                })
            })
            .collect()
    }
}

const fn role_to_byte(role: ReplicaRole) -> u8 {
    match role {
        ReplicaRole::Primary => 0,
        ReplicaRole::RegularSecondary => 1,
        ReplicaRole::Shadow => 2,
    }
}

const fn role_from_byte(byte: u8) -> Option<ReplicaRole> {
    match byte {
        0 => Some(ReplicaRole::Primary),
        1 => Some(ReplicaRole::RegularSecondary),
        2 => Some(ReplicaRole::Shadow),
        _ => None,
    }
}

/// Whether `body`'s tail after layout 3 is a valid member list: a count of at most
/// [`MAX_RECORD_MEMBERS`], exactly that many members, each with a known role byte.
fn members_tail_valid(body: &[u8]) -> bool {
    let Some(&count) = body.get(ROOTED_LEN) else {
        return false;
    };
    let count = usize::from(count);
    count <= MAX_RECORD_MEMBERS
        && body.len() == ROOTED_LEN + 1 + count * MEMBER_LEN
        && (0..count)
            .all(|index| role_from_byte(body[ROOTED_LEN + 1 + index * MEMBER_LEN + 5]).is_some())
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
        Bytes::from(self.rooted(ROOTED_LAYOUT_VERSION, ROOTED_LEN, root))
    }

    /// This record, its root and the members its configuration pins, as layout 4 (bug bash
    /// #64, ruling A-R426). [`Self::decode`] and [`PartitionRoot::decode`] read it as layout 3
    /// does; [`RecordMember::decode_all`] reads the members back in this order. A build that
    /// knows only layouts 2 and 3 reads it as no record at all, never as one without members.
    ///
    /// # Panics
    ///
    /// When `members` holds more than [`MAX_RECORD_MEMBERS`]: such a body would not decode.
    #[must_use]
    pub fn encode_with_members(&self, root: &PartitionRoot, members: &[RecordMember]) -> Bytes {
        assert!(
            members.len() <= MAX_RECORD_MEMBERS,
            "a partition record carries at most {MAX_RECORD_MEMBERS} members, not {}",
            members.len()
        );
        let len = ROOTED_LEN + 1 + members.len() * MEMBER_LEN;
        let mut out = self.rooted(MEMBERED_LAYOUT_VERSION, len, root);
        // At most `MAX_RECORD_MEMBERS`, checked above.
        out.push(u8::try_from(members.len()).unwrap_or(u8::MAX));
        for member in members {
            out.push(member.copy.0);
            out.extend_from_slice(&member.node.0.to_le_bytes());
            out.push(role_to_byte(member.role));
        }
        debug_assert_eq!(out.len(), len);
        Bytes::from(out)
    }

    /// This record in `body`'s layout, with `body`'s members and `root` (or `body`'s root when
    /// `root` is `None`): the one way to rewrite a stored record without losing what it
    /// carries (A-R426 M1). `None` when `body` is not a record, or `root` is given for a
    /// layout-2 body, which has none.
    #[must_use]
    pub fn encode_over(&self, body: &[u8], root: Option<&PartitionRoot>) -> Option<Bytes> {
        PartitionRecord::decode(body)?;
        let root = match root {
            Some(root) => Some(*root),
            None => PartitionRoot::decode(body),
        };
        match (root, RecordMember::decode_all(body)) {
            (Some(root), Some(members)) => Some(self.encode_with_members(&root, &members)),
            (Some(root), None) if body[0] == ROOTED_LAYOUT_VERSION => Some(self.encode_with(&root)),
            (None, None) if body[0] == LAYOUT_VERSION => Some(self.encode()),
            _ => None,
        }
    }

    /// The head, then `root`, behind `version`.
    fn rooted(&self, version: u8, capacity: usize, root: &PartitionRoot) -> Vec<u8> {
        let mut out = self.head(version, capacity);
        out.push(root.mode.to_byte());
        out.extend_from_slice(&root.owner_boot.0.to_le_bytes());
        out.extend_from_slice(&root.base_seq.0.to_le_bytes());
        out.extend_from_slice(&root.base_digest.0);
        out.extend_from_slice(&root.predecessor_generation.0.to_le_bytes());
        out.extend_from_slice(&root.predecessor_cutoff.0.to_le_bytes());
        out.extend_from_slice(&root.instance);
        out.extend_from_slice(&root.run.0.to_le_bytes());
        debug_assert_eq!(out.len(), ROOTED_LEN);
        out
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
        let valid = match body.first() {
            Some(&LAYOUT_VERSION) => body.len() == ENCODED_LEN,
            Some(&ROOTED_LAYOUT_VERSION) => body.len() == ROOTED_LEN,
            Some(&MEMBERED_LAYOUT_VERSION) => members_tail_valid(body),
            _ => false,
        };
        if !valid {
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

#[cfg(test)]
mod tests {
    //! Layout 4 (bug bash #64, ruling A-R426). Unit checks of one file's codec, not `M7*` rows,
    //! so no `#[retcd_test]`, as in `contracts::ignore`.

    use super::*;

    fn record() -> PartitionRecord {
        PartitionRecord {
            partition: PartitionId(1),
            owner: NodeId(2),
            generation: Generation(3),
            owner_epoch: OwnerEpoch(4),
            config_version: ConfigVersion(5),
            lifecycle: PartitionLifecycle::Fencing,
        }
    }

    fn root() -> PartitionRoot {
        PartitionRoot {
            mode: RecordMode::Active,
            owner_boot: BootId(6),
            base_seq: Seq(7),
            base_digest: Digest::ROOT,
            predecessor_generation: Generation(2),
            predecessor_cutoff: Seq(7),
            instance: [9; INSTANCE_LEN],
            run: Revision(8),
        }
    }

    fn members() -> Vec<RecordMember> {
        [
            (0, 1, ReplicaRole::Primary),
            (1, 2, ReplicaRole::RegularSecondary),
            (2, 3, ReplicaRole::Shadow),
        ]
        .into_iter()
        .map(|(copy, node, role)| RecordMember {
            copy: CopyId(copy),
            node: NodeId(node),
            role,
        })
        .collect()
    }

    /// Layout 4 reads back as the same record, the same root and the same members in order.
    #[test]
    fn layout_4_round_trips_its_record_root_and_members() {
        let bytes = record().encode_with_members(&root(), &members());
        assert_eq!(bytes[0], 4, "the layout byte");
        assert_eq!(bytes.len(), ROOTED_LEN + 1 + 3 * MEMBER_LEN);
        assert_eq!(PartitionRecord::decode(&bytes), Some(record()));
        assert_eq!(PartitionRoot::decode(&bytes), Some(root()));
        assert_eq!(RecordMember::decode_all(&bytes), Some(members()));
        let none = record().encode_with_members(&root(), &[]);
        assert_eq!(
            RecordMember::decode_all(&none),
            Some(Vec::new()),
            "zero is a count"
        );
    }

    /// Layouts 2 and 3 decode as before and name no members (A-R426: must not change).
    #[test]
    fn layouts_2_and_3_are_unchanged_and_carry_no_members() {
        let two = record().encode();
        assert_eq!((two[0], two.len()), (2, ENCODED_LEN));
        assert_eq!(PartitionRecord::decode(&two), Some(record()));
        assert_eq!(PartitionRoot::decode(&two), None);
        assert_eq!(RecordMember::decode_all(&two), None);
        let three = record().encode_with(&root());
        assert_eq!((three[0], three.len()), (3, ROOTED_LEN));
        assert_eq!(PartitionRecord::decode(&three), Some(record()));
        assert_eq!(PartitionRoot::decode(&three), Some(root()));
        assert_eq!(RecordMember::decode_all(&three), None);
    }

    /// A count over the bound, a length that disagrees with the count, an unknown role byte or
    /// an unknown layout byte is no record at all, never a partial one (A-R426 A1, row (d)).
    #[test]
    fn a_bad_count_role_length_or_layout_is_no_record() {
        let good = record().encode_with_members(&root(), &members()).to_vec();
        let undecodable = |bytes: &[u8], why: &str| {
            assert_eq!(PartitionRecord::decode(bytes), None, "{why}");
            assert_eq!(PartitionRoot::decode(bytes), None, "{why}");
            assert_eq!(RecordMember::decode_all(bytes), None, "{why}");
        };
        let mut over = good.clone();
        over[ROOTED_LEN] = 9;
        over.resize(ROOTED_LEN + 1 + 9 * MEMBER_LEN, 0);
        undecodable(&over, "nine members");
        let mut short = good.clone();
        short.pop();
        undecodable(&short, "one byte short");
        let mut long = good.clone();
        long.push(0);
        undecodable(&long, "one byte long");
        let mut role = good.clone();
        role[ROOTED_LEN + MEMBER_LEN] = 3;
        undecodable(&role, "role byte 3");
        let mut five = good;
        five[0] = 5;
        undecodable(&five, "layout 5");
        undecodable(&good_three_as_four(), "layout 3 bytes tagged 4, no count");
    }

    fn good_three_as_four() -> Vec<u8> {
        let mut bytes = record().encode_with(&root()).to_vec();
        bytes[0] = 4;
        bytes
    }

    /// `encode_over` keeps the body's layout and members while it rewrites the head (A-R426 M1).
    #[test]
    fn encode_over_keeps_the_layout_and_the_members() {
        let mut moved = record();
        moved.owner = NodeId(3);
        let four = record().encode_with_members(&root(), &members());
        let over = moved.encode_over(&four, None).expect("layout 4");
        assert_eq!(PartitionRecord::decode(&over), Some(moved));
        assert_eq!(RecordMember::decode_all(&over), Some(members()));
        let rerooted = PartitionRoot {
            run: Revision(99),
            ..root()
        };
        let over = moved.encode_over(&four, Some(&rerooted)).expect("layout 4");
        assert_eq!(PartitionRoot::decode(&over), Some(rerooted));
        assert_eq!(RecordMember::decode_all(&over), Some(members()));
        let three = moved
            .encode_over(&record().encode_with(&root()), None)
            .expect("layout 3");
        assert_eq!((three[0], three.len()), (3, ROOTED_LEN));
        let two = moved
            .encode_over(&record().encode(), None)
            .expect("layout 2");
        assert_eq!((two[0], PartitionRecord::decode(&two)), (2, Some(moved)));
        assert_eq!(moved.encode_over(&record().encode(), Some(&root())), None);
        assert_eq!(moved.encode_over(b"junk", None), None);
    }
}
