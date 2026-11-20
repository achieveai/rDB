//! Issue #61 S2 in the simulator (PR-A commit A9b): the twin of the host's snapshot transfer
//! (`rdb_api::host::snapshot`, A8a). The messages, the chunk chain and the re-seed's ACK rule
//! live here; [`crate::harness::dispatch::Dispatcher`] runs the protocol.
//!
//! **What is the host's, copied.** The refill-or-transfer rule (lead ruling 1: refill when the
//! primary's ladder is not dense from its engine's floor, [`ladder_dense`], A8a2's count), one transfer per `(partition, copy)`
//! that a newer barrier never replaces (lead ruling 2), the offer sent only once the source's
//! `durable` reaches `S`, the target's offer check in the host's order, the chain checked
//! before a page is written, `install_begin` on the first page, and after `install_finish` a
//! live receiver at `(S, barrier digest)`, durable `S`, whose own ACK goes to the primary
//! unsolicited (lead ruling A; [`reseed_ack`]). The transfer ends on the copy's `CopyCaughtUp`,
//! a refusal, or a restart of either node. The trim refuses past an open transfer's `S`.
//!
//! **What the simulator does differently, and why.**
//! - Offer, pulls and pages are calls, not frames: once the offer is due the whole install runs
//!   in one step. Only the ACK goes through the network. So the network's link state does not
//!   reach the transfer, and an offer to a node that is down is lost (`snapshot_send_failed`)
//!   until that node's restart ends the transfer.
//! - No idle abandonment (`SNAPSHOT_IDLE_MILLIS`): no sim transfer waits on a pull.
//! - An install is left open only by a unit row's private hook (the host's debug
//!   `snapshot_pages_held_from`, S2-R2's twin). An offer that passes the check replaces it,
//!   `snapshot_install_replaced`, as on the host; when a new offer can reach it is the source's
//!   rule (R13: the transfer ended, or a new generation).
//! - A target's restart ends its transfers at the restart; the host ends them when the source
//!   hears `PeerBooted`, which the simulator has no message for.
//! - Each page entry's generation is the offered one: the memory engine keeps no level per
//!   record. It only feeds the chain, which both ends compute from the same page, and no parity
//!   row compares chunk bytes (lead ruling R4).

use bytes::Bytes;
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::envelope::AppendAck;
use rdb_core::contracts::ids::{BootId, ConfigVersion, Generation, NodeId, Seq};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::storage::Namespace;
use rdb_core::contracts::trace::Version;
use rdb_core::replication::append::AppendReceiver;
use rdb_core::replication::progress::DigestLadder;

/// The label part of every chunk chain digest: the host's `SNAPSHOT_CHUNK_LABEL`. `rdb-sim`
/// cannot depend on `rdb-api`, so this is a copy, pinned by the known-answer row below to the
/// host's `t61_chunk_chain_known_answer`.
pub const SNAPSHOT_CHUNK_LABEL: &[u8] = b"rdb snapshot chunk v1";

/// Entries per page: the host's default `Host::snapshot_page_keys`, `COPY_BATCH_KEYS`.
pub const SNAPSHOT_PAGE_KEYS: usize = 1024;

/// One page entry as the chain reads it, the host's `rdb_storage::SnapshotEntry`: namespace,
/// key, value, version, and the generation that holds it.
pub type ChunkEntry = (Namespace, Bytes, Bytes, Version, Generation);

/// Why a target refused an offer or stopped an install: the host's `SnapshotRefusal` without
/// `TailHeld`, which is A9c's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotRefusal {
    /// The target's receiver did not adopt the offer's `(partition, generation, owner_epoch)`,
    /// or it holds no live receiver of the partition.
    NotAdopted,
    /// The offerer is not the primary node and boot the adopted config names.
    NotPrimary,
    /// `S` is at or below the receiver's applied head: nothing to install.
    NotAhead,
    /// The barrier record does not decode, names another seq, or does not recompute.
    BadBarrier,
    /// A page's chain did not match the entries it carried. Nothing was switched.
    Chain,
    /// The engine refused a step of the install.
    Install,
}

impl SnapshotRefusal {
    /// The `reason` field of `snapshot_offer_refused` and `snapshot_install_refused`, as the
    /// host names it.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::NotAdopted => "not_adopted",
            Self::NotPrimary => "not_primary",
            Self::NotAhead => "not_ahead",
            Self::BadBarrier => "bad_barrier",
            Self::Chain => "chain",
            Self::Install => "install",
        }
    }
}

/// The primary's offer of a snapshot of `lineage` at `at` to `copy`: the host's `SnapshotOffer`
/// without `released_through`, which is A9c's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotOffer {
    /// The offerer's lineage.
    pub lineage: Lineage,
    /// The offerer's config version.
    pub config_version: ConfigVersion,
    /// The offering node, the config's primary.
    pub from: NodeId,
    /// Its boot, as the config names it.
    pub boot: BootId,
    /// The copy offered to.
    pub copy: CopyId,
    /// `S`.
    pub at: Seq,
    /// The lineage's root, written into the installed trim floor.
    pub root: (Seq, Digest),
    /// The canonical record at `S`.
    pub barrier: Bytes,
}

/// An outbound transfer a primary serves, the host's `Transfer` without the view: the memory
/// engine is read in the same step the offer is taken, so nothing moves under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotTransfer {
    /// The lineage offered.
    pub lineage: Lineage,
    /// The target's node.
    pub node: NodeId,
    /// `S`.
    pub at: Seq,
    /// The offer, until it is sent once the source's `durable` reaches `S`.
    pub offer: Option<SnapshotOffer>,
}

/// The chain digest of a page, byte for byte the host's `chunk_chain`:
/// [`SNAPSHOT_CHUNK_LABEL`], `prev`, then each entry's namespace tag, key, value, version LE and
/// generation LE. `Digest::of` length-prefixes every part.
#[must_use]
pub fn chunk_chain(prev: Digest, entries: &[ChunkEntry]) -> Digest {
    let tags: Vec<[u8; 1]> = entries.iter().map(|(ns, ..)| [ns_tag(*ns)]).collect();
    let numbers: Vec<([u8; 8], [u8; 8])> = entries
        .iter()
        .map(|(_, _, _, version, generation)| (version.to_le_bytes(), generation.0.to_le_bytes()))
        .collect();
    let mut parts: Vec<&[u8]> = Vec::with_capacity(2 + entries.len() * 5);
    parts.push(SNAPSHOT_CHUNK_LABEL);
    parts.push(&prev.0);
    for (((_, key, value, _, _), tag), (version, generation)) in
        entries.iter().zip(&tags).zip(&numbers)
    {
        parts.extend_from_slice(&[tag, key, value, version, generation]);
    }
    Digest::of(Domain::Record, &parts)
}

/// The host's `ladder_dense` (A8a2, lead ruling 1): `ladder` holds a rung at every seq from
/// `floor` (the engine's floor) through its highest, with none below. Counted, never walked:
/// rungs are distinct seqs, so a ladder whose lowest is `floor` is dense exactly when it has
/// `highest - floor + 1`. An empty ladder is not dense.
#[must_use]
pub fn ladder_dense(ladder: &DigestLadder, floor: Seq) -> bool {
    match (ladder.floor(), ladder.highest()) {
        (Some(lowest), Some(highest)) => {
            lowest == floor
                && u64::try_from(ladder.len()).is_ok_and(|len| len == highest.0 - lowest.0 + 1)
        }
        _ => false,
    }
}

/// The host's `reseed_ack`: the ACK a re-seeded receiver sends its primary, only when it is at
/// `at` and durable through it.
#[must_use]
pub fn reseed_ack(receiver: &AppendReceiver, at: Seq) -> Option<AppendAck> {
    let ack = receiver.current_ack();
    (ack.progress.durable.0 >= at.0 && receiver.applied_head().seq == at).then_some(ack)
}

/// A namespace's tag in the chain, the envelope's wire tag (the host's `ns_tag`).
const fn ns_tag(ns: Namespace) -> u8 {
    match ns {
        Namespace::User => 1,
        Namespace::History => 2,
        Namespace::Dedup => 3,
        Namespace::Progress => 4,
        Namespace::Meta => 5,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rdb_core::contracts::digest::{Digest, Domain};
    use rdb_core::contracts::ids::Generation;
    use rdb_core::contracts::storage::Namespace;

    use super::{chunk_chain, ChunkEntry};

    /// Issue #61 lead ruling R3: the simulator's chain is the host's, byte for byte. The
    /// expected digests are `t61_chunk_chain_known_answer`'s (`rdb-api`, `host/catchup_tests.rs`)
    /// for the same inputs. Cut: any change to the label, a tag, the order or a width, red.
    /// Unit, under a second.
    #[test]
    fn t61_sim_chunk_chain_matches_the_hosts_known_answer() {
        let hex =
            |digest: Digest| -> String { digest.0.iter().map(|b| format!("{b:02x}")).collect() };
        let barrier = Digest::of(Domain::Record, &[b"barrier"]);
        let entry =
            |ns, key: &'static [u8], value: &'static [u8], version, generation| -> ChunkEntry {
                (
                    ns,
                    Bytes::from_static(key),
                    Bytes::from_static(value),
                    version,
                    Generation(generation),
                )
            };
        let first = [
            entry(Namespace::User, b"a", b"1", 7, 2),
            entry(Namespace::Dedup, b"b", b"", 8, 3),
        ];
        let second = [entry(Namespace::Progress, b"c", b"3", 9, 3)];
        let one = chunk_chain(barrier, &first);
        let two = chunk_chain(one, &second);
        assert_eq!(
            (hex(one), hex(two), hex(chunk_chain(barrier, &[]))),
            (
                "0e0607de51f50f0fcd0ce19c419996f45880bf894411fb348032f30ac9c5fc75".to_owned(),
                "164ff67cfb79c6342b15d1979f39cc50e3236363bd94e89cfc375a05466aa64e".to_owned(),
                "cebb77f7a3478e41f2072ae75121888594fd5b50dbfeecf20da013017ef16be8".to_owned()
            ),
            "the chunk chain's encoding moved off the host's"
        );
    }
}
