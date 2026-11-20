//! The `grants/{node}` record: its shape, its CAS body, and the freeze/revoke classification A1
//! runs over a linearizable read of it (team kernel-a `design.md` §5).
//!
//! # Scope: the node-side consumer plus the CAS shape, and nothing else
//!
//! Team kernel-a `design.md` §6 puts the grant **service** — the planner-side writer of
//! `grants/{node}` and its freeze loop — at M8/M9, and keeps for A1 "the node-side consumer plus
//! the CAS shape". [`GrantRecord`] is that shape. The planner's behaviour is modelled as scenario
//! events, which is what makes the races testable without it.
//!
//! # The byte layout is A1's M7 spelling, not a canonical codec
//!
//! [`crate::contracts::control::ReadOutcome::Found`] carries opaque
//! [`bytes::Bytes`], so *something* has to say what those bytes are. No canonical encoder exists
//! in this workspace — [`crate::contracts::errors::Capability::Codec`] is package C0's and is not
//! wired — and `rdb-core` has no serialisation format among its dependencies, only `serde`'s
//! traits. So [`GrantRecord::encode`] and [`GrantRecord::decode`] are a fixed-width
//! little-endian layout with a version byte, written here because this is the only module on
//! either side of it.
//!
//! **It is deliberately boring and deliberately replaceable.** When C0's canonical codec lands,
//! these two functions go and nothing else in A1 moves: [`classify`] takes a decoded
//! [`GrantRecord`] and never sees a byte, so the rule A1 is judged on does not depend on the
//! spelling. A fixture can call [`classify`] directly for the same reason.

use bytes::Bytes;

use crate::contracts::authority::DenyReason;
use crate::contracts::ids::{AuthorityGeneration, BootId, GrantId, NodeId};

/// The version byte every encoded record starts with. A decoder that meets a different one
/// answers `None` rather than reading the remaining bytes as fields.
const LAYOUT_VERSION: u8 = 1;

/// Version byte, `GrantId`, `NodeId`, `BootId`, `AuthorityGeneration`, `expiry_utc_ms`, `frozen`.
const ENCODED_LEN: usize = 1 + 8 + 4 + 8 + 8 + 8 + 1;

/// One `grants/{node}` record, as spec §7.1 describes it and as §7.3's steps read it.
///
/// Every field is here because a `design.md` §2.4 row compares it: the four identity fields
/// decide *whose* grant this is, `frozen` is the planner's revocation step 1, and
/// `expiry_utc_ms` is the `E` the whole bounded-clock rule is written against.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GrantRecord {
    /// Which grant this is. A new grant id is the only thing that gets a node out of
    /// [`crate::authority::AuthorityState::Fenced`].
    pub grant: GrantId,
    /// The node the grant was issued to.
    pub node: NodeId,
    /// The process lifetime it was issued to. A restart gets a new one, so a grant issued to an
    /// earlier boot can never be honoured by the new process (spec §7.2).
    pub boot: BootId,
    /// The cluster authority generation the grant was issued under. The planner bumps it on
    /// every fence, so a holder that sees a different one has had its right re-issued under it.
    pub authority_generation: AuthorityGeneration,
    /// `E`, exactly as the committed record says. Never advanced locally.
    pub expiry_utc_ms: i64,
    /// The planner froze this exact revision (spec §7.3 step 1).
    pub frozen: bool,
}

impl GrantRecord {
    /// This record as the body of a [`crate::contracts::control::ControlEffect::Cas`].
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut out = Vec::with_capacity(ENCODED_LEN);
        out.push(LAYOUT_VERSION);
        out.extend_from_slice(&self.grant.0.to_le_bytes());
        out.extend_from_slice(&self.node.0.to_le_bytes());
        out.extend_from_slice(&self.boot.0.to_le_bytes());
        out.extend_from_slice(&self.authority_generation.0.to_le_bytes());
        out.extend_from_slice(&self.expiry_utc_ms.to_le_bytes());
        out.push(u8::from(self.frozen));
        Bytes::from(out)
    }

    /// Read a record back, or `None` when the bytes are not one.
    ///
    /// `None` rather than an error: a body A1 cannot read is not a grant it holds, and every
    /// caller here already has a "this is not our grant" path. Nothing is guessed from a partial
    /// read.
    #[must_use]
    pub fn decode(body: &[u8]) -> Option<Self> {
        if body.len() != ENCODED_LEN || body[0] != LAYOUT_VERSION {
            return None;
        }
        let u64_at = |offset: usize| -> u64 {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(&body[offset..offset + 8]);
            u64::from_le_bytes(buf)
        };
        let mut node = [0_u8; 4];
        node.copy_from_slice(&body[9..13]);
        Some(Self {
            grant: GrantId(u64_at(1)),
            node: NodeId(u32::from_le_bytes(node)),
            boot: BootId(u64_at(13)),
            authority_generation: AuthorityGeneration(u64_at(21)),
            expiry_utc_ms: i64::from_le_bytes({
                let mut buf = [0_u8; 8];
                buf.copy_from_slice(&body[29..37]);
                buf
            }),
            frozen: body[37] != 0,
        })
    }
}

/// The identity a held grant compares a read-back record against.
///
/// A value rather than a borrow of `Held`, so [`classify`] is callable from a fixture that has
/// no kernel (team kernel-a `KA-1`) and so the comparison cannot accidentally read a field of
/// `Held` that the rule does not name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeldIdentity {
    /// The grant id this node believes it holds.
    pub grant: GrantId,
    /// Its own process lifetime.
    pub boot: BootId,
    /// The authority generation the grant was adopted under.
    pub authority_generation: AuthorityGeneration,
}

/// Why a read-back of `grants/{node}` ends the grant, or `None` when it does not.
///
/// The four fencing rows of team kernel-a `design.md` §2.4's steady-state table, in the table's
/// own order. Order is part of the contract (property 5, finding K-A-43): the first matching row
/// fires and no later row is consulted, so the same trace always names the same reason.
///
/// `None` is **not** "healthy and adopted". It means only that no fence fires; whether the
/// record is worth adopting is the read-back row, and that row is not this function's business.
#[must_use]
pub fn classify(record: &GrantRecord, held: HeldIdentity) -> Option<DenyReason> {
    // Row 1. Freeze wins without a special case elsewhere: the planner's freeze CAS bumps the
    // revision, so a renewal built on the pre-freeze revision loses and re-reads into here.
    if record.frozen {
        return Some(DenyReason::Frozen);
    }
    // Row 2. A different boot is a different process wearing this node's name.
    if record.boot != held.boot {
        return Some(DenyReason::BootMismatch);
    }
    // Row 2 again, the other half: the record names a grant that is not the one we hold, so ours
    // was replaced.
    if record.grant != held.grant {
        return Some(DenyReason::Revoked);
    }
    // Row 3. The cluster re-issued authority under us.
    if record.authority_generation != held.authority_generation {
        return Some(DenyReason::AuthorityGenerationChanged);
    }
    None
}

/// What a fenced boot may do with its own `grants/{node}` record (lead ruling A-R96, M9 S2b-E).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfReplace {
    /// Write over it, by a CAS at the exact revision read, under a new grant id.
    Replace,
    /// This boot's record, but its expiry is not proven past yet: ask again later.
    NotYet,
    /// Not this boot's to replace: frozen (a takeover's), another boot's, or issued under another
    /// authority generation. Nothing in M9 moves it: the way on is M10's takeover or rejoin (#42).
    Never,
}

/// May the boot `boot` of `node`, fenced, replace `record` — its own `grants/{node}` — with a new
/// grant id? [`SelfReplace::Replace`] only when all of these hold:
///
/// 1. it names `node` and `boot`: the same process, so nothing but this kernel writes it;
/// 2. it is not frozen: a frozen record belongs to a takeover (spec §7.3 step 1);
/// 3. its authority generation is the default, the one this build writes ([`classify`]'s row 3
///    fences on any other);
/// 4. [`expiry_proven`](crate::authority::clock::expiry_proven) holds on `clock` at `now`: a
///    bounded mode and a fresh, valid sample show control time past `E + epsilon + delta`.
///
/// # Why `expiry_proven` is sound here although the record is not frozen
///
/// Its doc speaks of a **frozen** prior grant, because a takeover proves the expiry of a record
/// the planner froze, and the freeze is what stops the old owner extending `E` under it. Here no
/// freeze is needed for the same guarantee, for two reasons. The only writer of an unfrozen
/// `grants/{node}` of this boot is this boot (A1, one CAS in flight at a time), and it is fenced:
/// it issues no renewal, so `E` does not move unless a renewal dispatched **before** the fence
/// lands late. And the replace is a CAS at the exact revision that was read: if such a late
/// renewal (or a planner freeze) commits first, the revision moves, the replace conflicts, and the
/// re-read judges the new record afresh — a late renewal is then `NotYet` until its own `E` is
/// proven past. So the `E` that was proven is the `E` the record still holds when it is replaced,
/// and past it this boot admits nowhere ([`utc_ok`](crate::authority::clock::utc_ok) is the same
/// inequality from the other side).
///
/// **Not checked here, an M10 gate:** the planner's grant-clearing service has a third guard,
/// "no partition naming the node as owner is in transfer" ([`ClearRefusal::PartitionInTransfer`]).
/// A self-replace keeps the same `owner_epoch` for every partition the node serves, so it cannot
/// race a takeover that has not frozen the grant; a transfer that does not freeze first is an M10
/// question and is recorded there.
#[must_use]
pub fn self_replace(
    record: &GrantRecord,
    node: NodeId,
    boot: BootId,
    clock: &crate::authority::clock::ClockView,
    now: crate::contracts::time::Tick,
    budgets: &crate::contracts::event::Budgets,
) -> SelfReplace {
    if record.node != node
        || record.boot != boot
        || record.frozen
        || record.authority_generation != AuthorityGeneration::default()
    {
        return SelfReplace::Never;
    }
    if crate::authority::clock::expiry_proven(clock, record.expiry_utc_ms, now, budgets).is_none() {
        return SelfReplace::NotYet;
    }
    SelfReplace::Replace
}

/// What the planner's grant-clearing service may do with a `grants/{node}` record it read: the
/// three guards of [`clear_verdict`], decided before anything is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClearVerdict {
    /// The record names the node's current boot: it is the live process's grant, not a stale one.
    Current,
    /// All three guards hold: the record may go, by an exact-revision delete.
    Clear,
    /// A guard holds the record in place.
    Refused(ClearRefusal),
}

/// Which guard of [`clear_verdict`] refused, in the order they are checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClearRefusal {
    /// Guard 1: the record is frozen. A frozen record belongs to a takeover (spec §7.3 step 1).
    Frozen,
    /// Guard 2: control time is not proven past `E_old + epsilon + delta`.
    NotProvenExpired,
    /// Guard 3: this partition names the node as owner and is not `Serving`.
    PartitionInTransfer(crate::contracts::ids::PartitionId),
}

/// May a restarted node's stale `grants/{node}` record be deleted? The three guards, as a pure
/// function. The simulator's model of the service calls it today; an S2 host admin is planned
/// to share them.
///
/// A1's acquisition is create-only, so a restarted node cannot acquire while its old boot's
/// record stands. Removing it is safe only when **all three** hold:
///
/// 1. the record is not frozen;
/// 2. the service's control time is past `E_old + epsilon + delta`, the same inequality a
///    takeover proves ([`crate::authority::clock::expiry_proven`]), so the old process can no
///    longer admit anywhere;
/// 3. no `partitions/{id}` naming the node as owner has a lifecycle other than `Serving`.
///
/// `record` is what `grants/{node}` holds. `boot` is the node's current boot as the service
/// knows it; `sample` and `now` are the
/// service's own clock. `partitions` reads the `partitions/` family and is called only once
/// guards 1 and 2 hold, so a caller whose read can fail fails no earlier than it did before the
/// extraction. Its error is returned unchanged.
///
/// # Errors
///
/// Whatever `partitions` returns.
pub fn clear_verdict<E>(
    node: NodeId,
    record: &GrantRecord,
    boot: BootId,
    sample: crate::contracts::time::ControlTime,
    now: crate::contracts::time::Tick,
    budgets: &crate::contracts::event::Budgets,
    partitions: impl FnOnce() -> Result<Vec<crate::authority::partition::PartitionRecord>, E>,
) -> Result<ClearVerdict, E> {
    use crate::authority::clock::{expiry_proven, ClockView};
    use crate::authority::partition::PartitionLifecycle;

    if record.boot == boot {
        return Ok(ClearVerdict::Current);
    }
    if record.frozen {
        return Ok(ClearVerdict::Refused(ClearRefusal::Frozen));
    }
    let mut clock = ClockView::new();
    clock.accept(sample);
    if expiry_proven(&clock, record.expiry_utc_ms, now, budgets).is_none() {
        return Ok(ClearVerdict::Refused(ClearRefusal::NotProvenExpired));
    }
    for partition in partitions()? {
        if partition.owner == node && partition.lifecycle != PartitionLifecycle::Serving {
            return Ok(ClearVerdict::Refused(ClearRefusal::PartitionInTransfer(
                partition.partition,
            )));
        }
    }
    Ok(ClearVerdict::Clear)
}

/// Why [`clear_frozen_verdict`] keeps a frozen `grants/{node}` record, in the order the guards
/// are checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrozenClearRefusal {
    /// The record is not frozen: [`clear_verdict`]'s case, not this one.
    NotFrozen,
    /// The record's boot is not older than the node's live boot, or no live boot is known: a
    /// same-boot frozen grant still belongs to that process's takeover.
    BootNotOlder,
    /// This partition names the node as owner, in any lifecycle (`Fencing` too): a takeover of
    /// it may still need the freeze.
    OwnsPartition(crate::contracts::ids::PartitionId),
    /// Control time is not proven past the frozen record's `E + epsilon + delta`.
    NotProvenExpired,
}

/// May the failover planner delete a **frozen** `grants/{node}` record (#81 X2, owner answer
/// (a) on #81, 2026-11-19)? A frozen record of a boot that is gone blocks the node's new boot
/// for good: the planner excludes a frozen node from `choose_target`, and a promote onto it is
/// refused `TargetFrozen`. It may go, by an exact-revision delete, only when **all** hold:
///
/// 1. the record is frozen;
/// 2. its boot is older than `live_boot`, the node's boot as the planner's links know it: the
///    old process is gone (the folder `LOCK` and a newer boot);
/// 3. no `partitions/{id}` names the node as owner, in any lifecycle: a takeover needs a
///    `Fencing` record naming it, so none is in flight;
/// 4. control time is past `E + epsilon + delta` of the frozen record
///    ([`crate::authority::clock::expiry_proven`]).
///
/// A freeze after the delete recreates the key frozen, so a later takeover still fences.
///
/// # Errors
///
/// The first guard that keeps the record.
pub fn clear_frozen_verdict(
    node: NodeId,
    record: &GrantRecord,
    live_boot: Option<BootId>,
    partitions: &[crate::authority::partition::PartitionRecord],
    sample: crate::contracts::time::ControlTime,
    now: crate::contracts::time::Tick,
    budgets: &crate::contracts::event::Budgets,
) -> Result<(), FrozenClearRefusal> {
    use crate::authority::clock::{expiry_proven, ClockView};

    if !record.frozen {
        return Err(FrozenClearRefusal::NotFrozen);
    }
    if !live_boot.is_some_and(|live| record.boot < live) {
        return Err(FrozenClearRefusal::BootNotOlder);
    }
    if let Some(owned) = partitions.iter().find(|partition| partition.owner == node) {
        return Err(FrozenClearRefusal::OwnsPartition(owned.partition));
    }
    let mut clock = ClockView::new();
    clock.accept(sample);
    if expiry_proven(&clock, record.expiry_utc_ms, now, budgets).is_none() {
        return Err(FrozenClearRefusal::NotProvenExpired);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{clear_verdict, ClearVerdict, GrantRecord};
    use crate::authority::partition::PartitionRecord;
    use crate::contracts::event::Budgets;
    use crate::contracts::ids::{AuthorityGeneration, BootId, GrantId, NodeId};
    use crate::contracts::time::{ControlTime, Tick};

    const NODE: NodeId = NodeId(1);
    const BOOT: BootId = BootId(1);

    /// Not frozen and long past `E + epsilon + delta`: every other guard would let it go.
    fn expired_record() -> GrantRecord {
        GrantRecord {
            grant: GrantId(1),
            node: NODE,
            boot: BOOT,
            authority_generation: AuthorityGeneration(1),
            expiry_utc_ms: 0,
            frozen: false,
        }
    }

    fn verdict(boot: BootId) -> ClearVerdict {
        let now = Tick(1_000_000_000);
        let sample = ControlTime {
            estimate: now,
            error_millis: Budgets::SPEC_DEFAULTS.clock_error_millis,
            bound_established: true,
            sampled_at: now,
        };
        let partitions = || Ok::<Vec<PartitionRecord>, ()>(Vec::new());
        clear_verdict(
            NODE,
            &expired_record(),
            boot,
            sample,
            now,
            &Budgets::SPEC_DEFAULTS,
            partitions,
        )
        .unwrap_or_else(|()| unreachable!("the partition read cannot fail here"))
    }

    /// The record names the node's current boot, so it is the live process's grant: the service
    /// must not delete it, however expired it reads. Deleting it would take the grant from under
    /// a process that still serves. The twin, the same record judged for a later boot, clears,
    /// so the refusal comes from the boot guard and nothing else. Removing that guard survived
    /// every other rdb-core and rdb-sim row (M9 S0, 2026-10-07).
    #[test]
    fn clearing_the_current_boots_grant_is_refused() {
        assert_eq!(verdict(BOOT), ClearVerdict::Current);
        assert_eq!(verdict(BootId(2)), ClearVerdict::Clear);
    }

    /// #81 X2: a frozen record of boot 1, the node live at boot 2, owning nothing, expiry long
    /// past: every guard of `clear_frozen_verdict` lets it go. Each case changes one input.
    fn frozen_verdict(
        record: GrantRecord,
        live: Option<BootId>,
        partitions: &[PartitionRecord],
        now: Tick,
    ) -> Result<(), super::FrozenClearRefusal> {
        let sample = ControlTime {
            estimate: now,
            error_millis: Budgets::SPEC_DEFAULTS.clock_error_millis,
            bound_established: true,
            sampled_at: now,
        };
        super::clear_frozen_verdict(
            NODE,
            &record,
            live,
            partitions,
            sample,
            now,
            &Budgets::SPEC_DEFAULTS,
        )
    }

    fn frozen_record() -> GrantRecord {
        GrantRecord {
            frozen: true,
            ..expired_record()
        }
    }

    const LATER: Tick = Tick(1_000_000_000);

    /// All four guards hold: the stale frozen record may go. Unit.
    #[test]
    fn own81_x2_a_frozen_grant_of_a_gone_boot_owning_nothing_clears() {
        assert_eq!(
            frozen_verdict(frozen_record(), Some(BootId(2)), &[], LATER),
            Ok(())
        );
    }

    /// Guard 3: a `Fencing` partition naming the node keeps its frozen record: that takeover
    /// still needs the freeze. Unit.
    /// Cut: drop the ownership guard; red.
    #[test]
    fn own81_x2_a_fencing_partition_naming_the_node_keeps_its_frozen_grant() {
        use crate::authority::partition::PartitionLifecycle;
        use crate::contracts::ids::PartitionId;
        let fencing = PartitionRecord {
            partition: PartitionId(3),
            owner: NODE,
            lifecycle: PartitionLifecycle::Fencing,
            ..PartitionRecord::default()
        };
        assert_eq!(
            frozen_verdict(frozen_record(), Some(BootId(2)), &[fencing], LATER),
            Err(super::FrozenClearRefusal::OwnsPartition(PartitionId(3)))
        );
    }

    /// Guard 2: a frozen record of the live boot (a same-boot return, Q7), or with no live boot
    /// known, stays. Unit.
    /// Cut: drop the boot guard; red.
    #[test]
    fn own81_x2_a_frozen_grant_of_the_live_boot_stays() {
        assert_eq!(
            frozen_verdict(frozen_record(), Some(BOOT), &[], LATER),
            Err(super::FrozenClearRefusal::BootNotOlder)
        );
        assert_eq!(
            frozen_verdict(frozen_record(), None, &[], LATER),
            Err(super::FrozenClearRefusal::BootNotOlder)
        );
    }

    /// Guard 1: an unfrozen record is `clear_verdict`'s, never this one's. Unit.
    /// Cut: drop the frozen guard; red.
    #[test]
    fn own81_x2_an_unfrozen_grant_is_not_cleared_as_frozen() {
        assert_eq!(
            frozen_verdict(expired_record(), Some(BootId(2)), &[], LATER),
            Err(super::FrozenClearRefusal::NotFrozen)
        );
    }

    /// Guard 4: control time not proven past `E + epsilon + delta` keeps it. Unit.
    /// Cut: drop the expiry guard; red.
    #[test]
    fn own81_x2_a_frozen_grant_not_proven_expired_stays() {
        let record = GrantRecord {
            expiry_utc_ms: 1_000_000_000,
            ..frozen_record()
        };
        assert_eq!(
            frozen_verdict(record, Some(BootId(2)), &[], LATER),
            Err(super::FrozenClearRefusal::NotProvenExpired)
        );
    }
}
