//! Kernel-to-kernel routing: which modules a kernel fact is for, in what order, and which of
//! those edges are still owed (none, since 2026-09-28).
//!
//! A kernel fact leaves a module as [`EffectKind::Kernel`] and arrives at another as
//! [`EventKind::Kernel`] (ruling R-S6). [`crate::harness::dispatch::Dispatcher::deliver`] turns
//! the effect into its event with [`event_for`], schedules it at the same tick on the same node
//! and partition, and marks it *routed*. The run loop then offers it in [`offer_order`]: the
//! named consumers first, in the order [`consumers`] gives, then every other module in
//! [`ModuleName::ALL`] order. Every event is still offered to all six, so the six
//! `ModuleDispatch` records per pop stay what they were.
//!
//! # What a named consumer's decline means
//!
//! For a **routed** event, a named consumer that answers `Unavailable` stops the run as
//! `StopReason::Declined`, with the consumer as the module and its answer as the error (ruling
//! B-R28: nothing is absorbed). Until 2026-10-02 that stop was `Refused` under a
//! `harness::run::route` seam; it is a module's answer, not a delivery the harness cannot make. The exception was an edge in [`OWED_EDGES`]: a
//! consumer whose package was not wired yet (lead ruling A-R62), whose decline was recorded
//! while the run continued. The table is empty since 2026-09-28 (lead rulings A-R82..A-R84), so
//! every named consumer's decline on a routed event now stops the run. A seeded event is the
//! scenario's own input, not a routed one, so its named consumers are offered first but a
//! decline is an ordinary decline.
//!
//! # Order
//!
//! `ConfigChanged` reaches L1 **before** R1 (carried item of L-R175, ruling B-R46e(2)): L1 pins
//! the new predicate before R1 can report a durable view for it, so R1 is never a configuration
//! change ahead of L1. P1 is not a consumer (lead ruling A-R82): it reads the configuration
//! version off A1's view, and its design has no `ConfigChanged` input.
//!
//! `LocalApplied` reaches L1 and then R1 in the same tick it was emitted, and ahead of anything
//! the same step shipped (B-R48): the dispatcher schedules effects in vector order, and the
//! primary emits it before the record it ships.
//!
//! L1's `SetAdmission` reaches T1 and R1 (ruling B-R60: R1 runs its keepalive exactly while
//! admission is rejected). R1 takes a `Recovered` **before** the `SetAdmission(Reject)` L1 emits
//! for it: `Recovered` is offered to R1 ahead of L1, and L1's answer is a new event scheduled
//! behind it. Otherwise a partition paused at birth would find no primary to start a keepalive.
//!
//! F1's `CatchUp` and `CatchUpBeforeGrant` reach R1 as [`KernelEvent::CatchUp`] at the node that
//! holds the named source copy (rulings B-R59, B-R59a); the dispatcher picks that node, not this
//! table. R1's `CopyCaughtUp` from that source goes back to the node of the F1 that asked.

use rdb_core::contracts::authority::{AuthorityEffect, AuthorityEvent, Checkpoint};
use rdb_core::contracts::event::{KernelEffect, KernelEvent, ModuleName};
use rdb_core::contracts::recovery::RecoveryEvent;

/// The [`KernelEvent`] arms, without their payloads, so a table can name one.
///
/// [`KernelEvent::Authority`] is split five ways because its consumers differ by leaf: A1's own
/// inputs go to A1, and the three twins of A1's outputs (lead ruling A-R63) go to the modules
/// A1 was talking to. The answer twin is split once more by checkpoint (lead ruling A-R83),
/// because an answer is "delivered to the module that asked" and T1 and P1 ask at different
/// checkpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Arm {
    /// [`KernelEvent::PeerProgress`].
    PeerProgress,
    /// [`KernelEvent::CopyLost`].
    CopyLost,
    /// [`KernelEvent::SetAdmission`].
    SetAdmission,
    /// [`KernelEvent::Recovered`].
    Recovered,
    /// [`KernelEvent::QualificationChanged`].
    QualificationChanged,
    /// [`KernelEvent::Authority`] carrying an input to A1: `Check`, `RevokeEpochRequested`,
    /// `EpochRevocationPersisted`, and any leaf added later.
    AuthorityInput,
    /// [`KernelEvent::Authority`] carrying [`AuthorityEvent::Answer`] at
    /// [`Checkpoint::StorageDispatch`]: T1's check (lead ruling A-R83).
    DispatchAnswer,
    /// [`KernelEvent::Authority`] carrying [`AuthorityEvent::Answer`] at any other checkpoint:
    /// P1's `Publication`, `Reply` and `Read` checks (lead ruling A-R83). `Admission` is
    /// synchronous by design and `OutboxDispatch` is unused in M7; neither is asked through the
    /// harness, and both land here, where P1 answers rather than declines.
    AuthorityAnswer,
    /// [`KernelEvent::Authority`] carrying [`AuthorityEvent::Fence`].
    AuthorityFence,
    /// [`KernelEvent::Authority`] carrying [`AuthorityEvent::View`].
    AuthorityView,
    /// [`KernelEvent::LocalApplied`].
    LocalApplied,
    /// [`KernelEvent::DurableAdvanced`].
    DurableAdvanced,
    /// [`KernelEvent::ConfigChanged`].
    ConfigChanged,
    /// [`KernelEvent::TransitionBarrierConfirmed`].
    TransitionBarrierConfirmed,
    /// [`KernelEvent::BlockPartition`].
    BlockPartition,
    /// [`KernelEvent::Recovery`].
    Recovery,
    /// [`KernelEvent::DivergenceDetected`].
    DivergenceDetected,
    /// [`KernelEvent::CopyQuarantined`].
    CopyQuarantined,
    /// [`KernelEvent::AppliedCandidate`].
    AppliedCandidate,
    /// [`KernelEvent::Published`].
    Published,
    /// [`KernelEvent::Publication`].
    Publication,
    /// [`KernelEvent::DedupTrim`].
    DedupTrim,
    /// [`KernelEvent::StatusTrim`].
    StatusTrim,
    /// [`KernelEvent::RetireGeneration`].
    RetireGeneration,
    /// [`KernelEvent::CatchUp`].
    CatchUp,
}

impl Arm {
    /// Every arm, in declaration order.
    pub const ALL: [Self; 25] = [
        Self::PeerProgress,
        Self::CopyLost,
        Self::SetAdmission,
        Self::Recovered,
        Self::QualificationChanged,
        Self::AuthorityInput,
        Self::DispatchAnswer,
        Self::AuthorityAnswer,
        Self::AuthorityFence,
        Self::AuthorityView,
        Self::LocalApplied,
        Self::DurableAdvanced,
        Self::ConfigChanged,
        Self::TransitionBarrierConfirmed,
        Self::BlockPartition,
        Self::Recovery,
        Self::DivergenceDetected,
        Self::CopyQuarantined,
        Self::AppliedCandidate,
        Self::Published,
        Self::Publication,
        Self::DedupTrim,
        Self::StatusTrim,
        Self::RetireGeneration,
        Self::CatchUp,
    ];

    /// The arm of `event`, or `None` for an arm added to the `#[non_exhaustive]` enum after this
    /// table was written. Such an event has no named consumer and is offered in
    /// [`ModuleName::ALL`] order.
    #[must_use]
    pub const fn of(event: &KernelEvent) -> Option<Self> {
        Some(match event {
            KernelEvent::PeerProgress { .. } => Self::PeerProgress,
            KernelEvent::CopyLost { .. } => Self::CopyLost,
            KernelEvent::SetAdmission(_) => Self::SetAdmission,
            KernelEvent::Recovered(_) => Self::Recovered,
            KernelEvent::QualificationChanged(_) => Self::QualificationChanged,
            KernelEvent::Authority(AuthorityEvent::Answer(answer)) => match answer.checkpoint {
                Checkpoint::StorageDispatch => Self::DispatchAnswer,
                Checkpoint::Publication
                | Checkpoint::Reply
                | Checkpoint::Read
                | Checkpoint::Admission
                | Checkpoint::OutboxDispatch => Self::AuthorityAnswer,
            },
            KernelEvent::Authority(AuthorityEvent::Fence { .. }) => Self::AuthorityFence,
            KernelEvent::Authority(AuthorityEvent::View(_)) => Self::AuthorityView,
            KernelEvent::Authority(_) => Self::AuthorityInput,
            KernelEvent::LocalApplied { .. } => Self::LocalApplied,
            KernelEvent::DurableAdvanced { .. } => Self::DurableAdvanced,
            KernelEvent::ConfigChanged(_) => Self::ConfigChanged,
            KernelEvent::TransitionBarrierConfirmed { .. } => Self::TransitionBarrierConfirmed,
            KernelEvent::BlockPartition(_) => Self::BlockPartition,
            KernelEvent::Recovery(_) => Self::Recovery,
            KernelEvent::DivergenceDetected { .. } => Self::DivergenceDetected,
            KernelEvent::CopyQuarantined { .. } => Self::CopyQuarantined,
            KernelEvent::AppliedCandidate(_) => Self::AppliedCandidate,
            KernelEvent::Published { .. } => Self::Published,
            KernelEvent::Publication(_) => Self::Publication,
            KernelEvent::DedupTrim { .. } => Self::DedupTrim,
            KernelEvent::StatusTrim { .. } => Self::StatusTrim,
            KernelEvent::RetireGeneration { .. } => Self::RetireGeneration,
            KernelEvent::CatchUp { .. } => Self::CatchUp,
            _ => return None,
        })
    }
}

/// The modules an arm is delivered to, in delivery order, each as its contract doc names it.
///
/// Owed consumers would be included: the table says who the event is *for*, and [`OWED_EDGES`]
/// says which of those cannot take it yet (none, since 2026-09-28).
#[must_use]
pub const fn consumers(arm: Arm) -> &'static [ModuleName] {
    use ModuleName::{Authority, Protection, Publication, Recovery, Replication, Transaction};
    match arm {
        // "Delivered to L1" (design §4.1); F1 drops a lost source (§5).
        Arm::CopyLost => &[Protection, Recovery],
        Arm::PeerProgress | Arm::DurableAdvanced => &[Protection],
        // T1's admission gate, and R1's keepalive (ruling B-R60; ADR-rdb-0006 amendment).
        Arm::SetAdmission => &[Transaction, Replication],
        // "Delivered to T1" (design §4.5; kernel-a §4.2, §4.4).
        Arm::Published | Arm::DedupTrim => &[Transaction],
        // R1 rewrites its receiver and primary; L1 rebuilds its state (design §5.8).
        Arm::Recovered => &[Replication, Protection],
        // "Delivered to L1 ... and to P1" (design §4.1); `BlockPartition` likewise.
        Arm::QualificationChanged | Arm::BlockPartition => &[Protection, Publication],
        Arm::AuthorityInput => &[Authority],
        // "Delivered to the module that asked": only T1 and P1 ask (`AuthorityCheck`), T1 at
        // `StorageDispatch` and P1 at `Publication`, `Reply` and `Read` (A-R83).
        Arm::DispatchAnswer => &[Transaction],
        Arm::AuthorityAnswer => &[Publication],
        // A1's fence is "broadcast to T1 and P1" (A-R28), and `RetireGeneration` names both.
        Arm::AuthorityFence | Arm::RetireGeneration => &[Transaction, Publication],
        // "A1 pushes its authority state to R1, T1 and P1" (kernel-a design §1.7).
        Arm::AuthorityView => &[Replication, Transaction, Publication],
        // "Delivered to L1 and to R1" (design §4.1, §4.3; ruling B-R47).
        Arm::LocalApplied | Arm::TransitionBarrierConfirmed => &[Protection, Replication],
        // L1 before R1: ruling B-R46e(2). Not P1, which has no such input (A-R82).
        Arm::ConfigChanged => &[Protection, Replication],
        Arm::Recovery => &[Recovery],
        Arm::DivergenceDetected | Arm::CopyQuarantined => &[Replication],
        // F1's catch-up request, at the node of its source copy (rulings B-R59, B-R59a).
        Arm::CatchUp => &[Replication],
        // "T1's applied candidate, for P1" (kernel-a design §1.3). Not R1: R1 learns the
        // history from `LocalApplied` alone, once per seq and in order (B-R47, A-R65).
        Arm::AppliedCandidate | Arm::Publication | Arm::StatusTrim => &[Publication],
    }
}

/// The team that owns an owed edge's consumer, and so retires the edge by building its body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    /// Team kernel-a: T1 and P1.
    KernelA,
    /// Team kernel-b: R1 (lead ruling A-R65).
    KernelB,
}

/// The edges whose consumer has no body for the arm yet (lead rulings A-R62, A-R63, A-R65).
///
/// On one of these a decline is recorded as `DispatchOutcome::DeclinedOwed` and the run goes on;
/// on any other edge of a routed event it stops the run. Enumerated, never a wildcard over a
/// module, so an edge leaves the table only by being deleted.
///
/// Empty since 2026-09-28. The seventeen edges to T1 and P1 (A-R63) left together: fourteen
/// because both bodies answer them, and three more: T1's answer edge by splitting the arm by
/// checkpoint (A-R83), P1's fence edge once P1 answers another partition's fence
/// `Ignored(NotOurs)` (A-R84), and P1's `ConfigChanged` edge because P1 is no longer a consumer
/// (A-R82). R1's one edge, A1's view (A-R65), left on 2026-09-26 (B-R53). So every named
/// consumer's decline on a routed event stops the run, and the T1/P1 capability flip is no longer
/// blocked by this table. The tripwire `route_nothing_is_owed_any_more` (`tests/dispatch.rs`)
/// keeps it empty; an edge comes back only with a ruling.
pub const OWED_EDGES: [(Arm, ModuleName, Owner); 0] = [];

/// How one module stands to one event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Edge {
    /// A named consumer whose package is wired. On a routed event, its decline stops the run.
    Named,
    /// A named consumer on an [`OWED_EDGES`] edge. Its decline is recorded, not fatal.
    Owed,
    /// Not a consumer. Offered anyway; a decline is the ordinary answer.
    Other,
}

/// How `module` stands to an event of `arm`.
#[must_use]
pub fn edge(arm: Option<Arm>, module: ModuleName) -> Edge {
    let Some(arm) = arm else {
        return Edge::Other;
    };
    if OWED_EDGES
        .iter()
        .any(|&(owed, consumer, _)| (owed, consumer) == (arm, module))
    {
        Edge::Owed
    } else if consumers(arm).contains(&module) {
        Edge::Named
    } else {
        Edge::Other
    }
}

/// The order the loop offers an event of `arm` in: its consumers, then the rest in
/// [`ModuleName::ALL`] order. Always all six, each once.
#[must_use]
pub fn offer_order(arm: Option<Arm>) -> [ModuleName; 6] {
    let mut order = ModuleName::ALL;
    let Some(arm) = arm else {
        return order;
    };
    let named = consumers(arm);
    let mut slot = 0;
    for module in named.iter().copied() {
        order[slot] = module;
        slot += 1;
    }
    for module in ModuleName::ALL {
        if !named.contains(&module) {
            order[slot] = module;
            slot += 1;
        }
    }
    order
}

/// The event a kernel effect is delivered as, or `None` when it is not routed.
///
/// Five arms change shape on the way. A1's `FenceProven` arrives as
/// [`RecoveryEvent::FenceProven`] (the only door out of F1's `Idle`), and R1's `CopyCaughtUp` as
/// [`RecoveryEvent::CopyCaughtUp`], whose fields it mirrors name for name. A1's `Answer`, `Fence`
/// and `PublishAuthorityView` arrive as their [`AuthorityEvent`] twins (lead ruling A-R63), and
/// T1's or P1's `AuthorityCheck` as [`AuthorityEvent::Check`] with the same three fields.
/// Everything else keeps its name.
///
/// `None` for: the recorded arms (`Ignored`, `Alert`, A1's `Fact`, `ProtectionWarn`, F1's fact
/// arms, P1's `Publication(..)` outputs under A-R65.3), which the dispatcher keeps as notes; F1's
/// requests to the environment, which go to a provider or are refused by name; and every arm with
/// no consumer this table knows — R1's `SnapshotCatchupRequired` and `CopyAheadOnControl` —
/// which the dispatcher refuses by name. R1's `SendEnvelopes` (B-R57) and
/// `SendRecoveryEnvelopes` (B-R59) have providers instead, and so do F1's `CatchUp` and
/// `CatchUpBeforeGrant`, which the dispatcher routes as [`KernelEvent::CatchUp`] to the source's
/// node. `SetAdmission` is routed **and** noted (ruling B-R42's note stays).
#[must_use]
pub fn event_for(effect: &KernelEffect) -> Option<KernelEvent> {
    Some(match effect {
        KernelEffect::SetAdmission(state) => KernelEvent::SetAdmission(state.clone()),
        KernelEffect::Recovered(result) => KernelEvent::Recovered(result.clone()),
        KernelEffect::QualificationChanged(edge) => KernelEvent::QualificationChanged(edge.clone()),
        KernelEffect::Authority(AuthorityEffect::FenceProven(proof)) => {
            KernelEvent::Recovery(RecoveryEvent::FenceProven(Box::new(proof.clone())))
        }
        KernelEffect::Authority(AuthorityEffect::Answer(decision)) => {
            KernelEvent::Authority(AuthorityEvent::Answer(*decision))
        }
        KernelEffect::Authority(AuthorityEffect::Fence { scope, reason }) => {
            KernelEvent::Authority(AuthorityEvent::Fence {
                scope: *scope,
                reason: *reason,
            })
        }
        KernelEffect::Authority(AuthorityEffect::PublishAuthorityView(view)) => {
            KernelEvent::Authority(AuthorityEvent::View(*view))
        }
        KernelEffect::AuthorityCheck {
            checkpoint,
            lineage,
            correlation,
        } => KernelEvent::Authority(AuthorityEvent::Check {
            checkpoint: *checkpoint,
            lineage: *lineage,
            correlation: *correlation,
        }),
        KernelEffect::PeerProgress {
            peer,
            contiguous_seq,
        } => KernelEvent::PeerProgress {
            peer: *peer,
            contiguous_seq: *contiguous_seq,
        },
        KernelEffect::CopyLost { copy } => KernelEvent::CopyLost { copy: *copy },
        KernelEffect::LocalApplied {
            seq,
            bytes,
            record_digest,
        } => KernelEvent::LocalApplied {
            seq: *seq,
            bytes: *bytes,
            record_digest: *record_digest,
        },
        KernelEffect::DurableAdvanced { per_predicate } => KernelEvent::DurableAdvanced {
            per_predicate: per_predicate.clone(),
        },
        KernelEffect::BlockPartition(reason) => KernelEvent::BlockPartition(reason.clone()),
        KernelEffect::DivergenceDetected { copy } => {
            KernelEvent::DivergenceDetected { copy: *copy }
        }
        KernelEffect::CopyQuarantined { copy } => KernelEvent::CopyQuarantined { copy: *copy },
        KernelEffect::CopyCaughtUp { copy, head, digest } => {
            KernelEvent::Recovery(RecoveryEvent::CopyCaughtUp {
                copy: *copy,
                head: *head,
                digest: *digest,
            })
        }
        KernelEffect::AppliedCandidate(candidate) => {
            KernelEvent::AppliedCandidate(candidate.clone())
        }
        KernelEffect::Published {
            lineage,
            seq,
            record_digest,
            request,
        } => KernelEvent::Published {
            lineage: *lineage,
            seq: *seq,
            record_digest: *record_digest,
            request: *request,
        },
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    //! SCAFFOLDING, not test rows: the table's own shape.

    use super::{consumers, edge, offer_order, Arm, Edge, Owner, OWED_EDGES};
    use rdb_core::contracts::event::ModuleName;

    /// Every offer order is a permutation of the six, consumers first.
    #[test]
    fn every_order_offers_all_six_once_consumers_first() {
        for arm in Arm::ALL {
            let order = offer_order(Some(arm));
            let mut sorted = order;
            sorted.sort_unstable();
            let mut all = ModuleName::ALL;
            all.sort_unstable();
            assert_eq!(sorted, all, "{arm:?}");
            let named = consumers(arm);
            assert_eq!(&order[..named.len()], named, "{arm:?}");
        }
        assert_eq!(offer_order(None), ModuleName::ALL);
    }

    /// `ConfigChanged` reaches L1 before R1 (B-R46e(2)); P1 is not a consumer (A-R82).
    #[test]
    fn a_config_change_reaches_l1_before_r1() {
        let order = offer_order(Some(Arm::ConfigChanged));
        let at = |m| order.iter().position(|x| *x == m).expect("offered");
        assert!(at(ModuleName::Protection) < at(ModuleName::Replication));
        assert_eq!(
            consumers(Arm::ConfigChanged),
            [ModuleName::Protection, ModuleName::Replication]
        );
    }

    /// An answer goes to the module that asked (A-R83): `StorageDispatch` is T1's check, and
    /// every other checkpoint routes to P1's arm, `Publication`, `Reply` and `Read` being P1's.
    #[test]
    fn an_answer_is_for_the_module_that_asked() {
        use rdb_core::contracts::authority::{
            AuthorityDecision, AuthorityEvent, Checkpoint, Lineage, Verdict,
        };
        use rdb_core::contracts::event::KernelEvent;
        use rdb_core::contracts::ids::{
            AuthorityGeneration, BootId, CorrelationId, Generation, GrantId, NodeId, OwnerEpoch,
            PartitionId,
        };
        use rdb_core::contracts::time::Tick;
        let arm = |checkpoint| {
            Arm::of(&KernelEvent::Authority(AuthorityEvent::Answer(
                AuthorityDecision {
                    owner: NodeId(1),
                    boot: BootId(1),
                    grant: GrantId(1),
                    authority_generation: AuthorityGeneration(1),
                    lineage: Lineage {
                        partition: PartitionId(1),
                        generation: Generation(1),
                        owner_epoch: OwnerEpoch(1),
                    },
                    expiry_utc_ms: 0,
                    decided_at: Tick::ZERO,
                    authority_seq: 1,
                    checkpoint,
                    correlation: CorrelationId(1),
                    verdict: Verdict::Admit,
                },
            )))
        };
        assert_eq!(arm(Checkpoint::StorageDispatch), Some(Arm::DispatchAnswer));
        assert_eq!(consumers(Arm::DispatchAnswer), [ModuleName::Transaction]);
        for checkpoint in [
            Checkpoint::Publication,
            Checkpoint::Reply,
            Checkpoint::Read,
            Checkpoint::Admission,
            Checkpoint::OutboxDispatch,
        ] {
            assert_eq!(
                arm(checkpoint),
                Some(Arm::AuthorityAnswer),
                "{checkpoint:?}"
            );
        }
        assert_eq!(consumers(Arm::AuthorityAnswer), [ModuleName::Publication]);
    }

    /// Each owed edge is an edge the consumer table names, and is listed once.
    #[test]
    fn every_owed_edge_is_a_named_edge() {
        for (at, (arm, module, _)) in OWED_EDGES.into_iter().enumerate() {
            assert!(consumers(arm).contains(&module), "{arm:?} -> {module:?}");
            assert_eq!(edge(Some(arm), module), Edge::Owed);
            assert!(
                !OWED_EDGES[..at]
                    .iter()
                    .any(|&(a, m, _)| (a, m) == (arm, module)),
                "{arm:?} -> {module:?} twice"
            );
        }
    }

    /// What is still owed, written out: nothing (2026-09-28, A-R82..A-R84). T1's and P1's
    /// seventeen edges and R1's one have all left, so every named consumer is `Named` and a
    /// decline by any of them on a routed event stops the run. An edge added back fails here
    /// until this list names it.
    #[test]
    fn nothing_is_owed() {
        const STILL_OWED: [(Arm, ModuleName, Owner); 0] = [];
        assert_eq!(OWED_EDGES, STILL_OWED);
        for arm in Arm::ALL {
            for module in consumers(arm).iter().copied() {
                assert_eq!(
                    edge(Some(arm), module),
                    Edge::Named,
                    "{arm:?} -> {module:?}"
                );
            }
        }
    }

    /// `AppliedCandidate` is P1's alone: R1's history comes from `LocalApplied` (A-R65).
    #[test]
    fn an_applied_candidate_is_for_p1_only() {
        assert_eq!(consumers(Arm::AppliedCandidate), [ModuleName::Publication]);
    }

    /// F1's catch-up reaches R1 by name (B-R59), so R1's decline on it stops the run.
    #[test]
    fn a_catch_up_is_for_r1_by_name() {
        use rdb_core::contracts::authority::FenceCredential;
        use rdb_core::contracts::event::KernelEvent;
        use rdb_core::contracts::ids::{Generation, OwnerEpoch, PartitionId, Revision, Seq};
        use rdb_core::contracts::membership::CopyId;
        let event = KernelEvent::CatchUp {
            from: CopyId(1),
            to: CopyId(2),
            through: Seq(1),
            credential: FenceCredential {
                partition: PartitionId(1),
                prior_generation: Generation(1),
                prior_owner_epoch: OwnerEpoch(1),
                control_revision: Revision(1),
                sender: CopyId(1),
            },
        };
        assert_eq!(Arm::of(&event), Some(Arm::CatchUp));
        assert_eq!(consumers(Arm::CatchUp), [ModuleName::Replication]);
    }
}
