//! Kernel-to-kernel routing: which modules a kernel fact is for, in what order, and which of
//! those edges are still owed.
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
//! `StopReason::Refused` under the one seam `harness::run::route`, with the consumer as the
//! module (ruling B-R28: nothing is absorbed). The exception is an edge in [`OWED_EDGES`]: a
//! consumer whose package is not wired yet (lead ruling A-R62). Its decline is recorded and the
//! run continues. A seeded event is the scenario's own input, not a routed one, so its named
//! consumers are offered first but a decline is an ordinary decline.
//!
//! # Order
//!
//! `ConfigChanged` reaches L1 and P1 **before** R1 (carried item of L-R175, ruling B-R46e(2)):
//! L1 pins the new predicate before R1 can report a durable view for it, so R1 is never a
//! configuration change ahead of L1. `LocalApplied` reaches L1 and then R1 in the same tick it
//! was emitted, and ahead of anything the same step shipped (B-R48): the dispatcher schedules
//! effects in vector order, and the primary emits it before the record it ships.

use rdb_core::contracts::authority::{AuthorityEffect, AuthorityEvent};
use rdb_core::contracts::event::{KernelEffect, KernelEvent, ModuleName};
use rdb_core::contracts::recovery::RecoveryEvent;

/// The [`KernelEvent`] arms, without their payloads, so a table can name one.
///
/// [`KernelEvent::Authority`] is split four ways because its consumers differ by leaf: A1's own
/// inputs go to A1, and the three twins of A1's outputs (lead ruling A-R63) go to the modules
/// A1 was talking to.
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
    /// [`KernelEvent::Authority`] carrying [`AuthorityEvent::Answer`].
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
}

impl Arm {
    /// Every arm, in declaration order.
    pub const ALL: [Self; 23] = [
        Self::PeerProgress,
        Self::CopyLost,
        Self::SetAdmission,
        Self::Recovered,
        Self::QualificationChanged,
        Self::AuthorityInput,
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
            KernelEvent::Authority(AuthorityEvent::Answer(_)) => Self::AuthorityAnswer,
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
            _ => return None,
        })
    }
}

/// The modules an arm is delivered to, in delivery order, each as its contract doc names it.
///
/// Owed consumers are included: the table says who the event is *for*, and [`OWED_EDGES`] says
/// which of those cannot take it yet.
#[must_use]
pub const fn consumers(arm: Arm) -> &'static [ModuleName] {
    use ModuleName::{Authority, Protection, Publication, Recovery, Replication, Transaction};
    match arm {
        // "Delivered to L1" (design §4.1); F1 drops a lost source (§5).
        Arm::CopyLost => &[Protection, Recovery],
        Arm::PeerProgress | Arm::DurableAdvanced => &[Protection],
        // "Delivered to T1" (design §4.5; kernel-a §4.2, §4.4).
        Arm::SetAdmission | Arm::Published | Arm::DedupTrim => &[Transaction],
        // R1 rewrites its receiver and primary; L1 rebuilds its state (design §5.8).
        Arm::Recovered => &[Replication, Protection],
        // "Delivered to L1 ... and to P1" (design §4.1); `BlockPartition` likewise.
        Arm::QualificationChanged | Arm::BlockPartition => &[Protection, Publication],
        Arm::AuthorityInput => &[Authority],
        // "Delivered to the module that asked": only T1 and P1 ask (`AuthorityCheck`). A1's
        // fence is "broadcast to T1 and P1" (A-R28), and `RetireGeneration` names both.
        Arm::AuthorityAnswer | Arm::AuthorityFence | Arm::RetireGeneration => {
            &[Transaction, Publication]
        }
        // "A1 pushes its authority state to R1, T1 and P1" (kernel-a design §1.7).
        Arm::AuthorityView => &[Replication, Transaction, Publication],
        // "Delivered to L1 and to R1" (design §4.1, §4.3; ruling B-R47).
        Arm::LocalApplied | Arm::TransitionBarrierConfirmed => &[Protection, Replication],
        // L1 and P1 before R1: ruling B-R46e(2).
        Arm::ConfigChanged => &[Protection, Publication, Replication],
        Arm::Recovery => &[Recovery],
        Arm::DivergenceDetected | Arm::CopyQuarantined => &[Replication],
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
/// Seventeen edges are to T1 and P1, the two packages kernel-a is still building (A-R63). The
/// scaffolding test `route_every_owed_consumer_still_reports_unavailable` (`tests/dispatch.rs`)
/// fails once T1 or P1 stops reporting `CapabilityState::Unavailable`, which forces this table to
/// be re-read then. T1 already answers `AuthorityEvent::View` (lead ruling A-R68); its edge
/// stays listed until T1 passes its manual-tester gate (A-R67.1), and an answer on an owed edge
/// is recorded as an answer.
///
/// R1 owes nothing. Its one edge, A1's view (A-R65), left the table on 2026-09-26 when R1 began
/// answering `AuthorityEvent::View` on every node, installed or not (lead ruling B-R53); a
/// decline by R1 on it now stops the run like any named consumer's. The scaffolding test still
/// lists kernel-b's edges separately, because R1's package-level capability cannot retire one.
pub const OWED_EDGES: [(Arm, ModuleName, Owner); 17] = [
    (Arm::SetAdmission, ModuleName::Transaction, Owner::KernelA),
    (
        Arm::AuthorityAnswer,
        ModuleName::Transaction,
        Owner::KernelA,
    ),
    (Arm::AuthorityFence, ModuleName::Transaction, Owner::KernelA),
    (Arm::AuthorityView, ModuleName::Transaction, Owner::KernelA),
    (Arm::Published, ModuleName::Transaction, Owner::KernelA),
    (Arm::DedupTrim, ModuleName::Transaction, Owner::KernelA),
    (
        Arm::RetireGeneration,
        ModuleName::Transaction,
        Owner::KernelA,
    ),
    (
        Arm::QualificationChanged,
        ModuleName::Publication,
        Owner::KernelA,
    ),
    (Arm::ConfigChanged, ModuleName::Publication, Owner::KernelA),
    (Arm::BlockPartition, ModuleName::Publication, Owner::KernelA),
    (
        Arm::AuthorityAnswer,
        ModuleName::Publication,
        Owner::KernelA,
    ),
    (Arm::AuthorityFence, ModuleName::Publication, Owner::KernelA),
    (Arm::AuthorityView, ModuleName::Publication, Owner::KernelA),
    (
        Arm::AppliedCandidate,
        ModuleName::Publication,
        Owner::KernelA,
    ),
    (Arm::Publication, ModuleName::Publication, Owner::KernelA),
    (Arm::StatusTrim, ModuleName::Publication, Owner::KernelA),
    (
        Arm::RetireGeneration,
        ModuleName::Publication,
        Owner::KernelA,
    ),
];

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
/// which the dispatcher refuses by name; R1's `SendEnvelopes` has a provider instead (B-R57). `SetAdmission` is routed **and**
/// noted (ruling B-R42's note stays).
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

    /// `ConfigChanged` reaches L1 and P1 before R1 (B-R46e(2)).
    #[test]
    fn a_config_change_reaches_l1_and_p1_before_r1() {
        let order = offer_order(Some(Arm::ConfigChanged));
        let at = |m| order.iter().position(|x| *x == m).expect("offered");
        assert!(at(ModuleName::Protection) < at(ModuleName::Replication));
        assert!(at(ModuleName::Publication) < at(ModuleName::Replication));
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

    /// Every edge to T1 or P1 is owed and owned by kernel-a (A-R63); A1, L1, F1 and, since it
    /// answers A1's view (B-R53), R1 are owed nothing.
    #[test]
    fn every_edge_to_t1_or_p1_is_owed_and_no_other_is() {
        for arm in Arm::ALL {
            for module in consumers(arm).iter().copied() {
                let owner = OWED_EDGES
                    .iter()
                    .find(|&&(a, m, _)| (a, m) == (arm, module))
                    .map(|&(_, _, owner)| owner);
                let expected = match module {
                    ModuleName::Transaction | ModuleName::Publication => Some(Owner::KernelA),
                    _ => None,
                };
                assert_eq!(owner, expected, "{arm:?} -> {module:?}");
            }
        }
    }

    /// `AppliedCandidate` is P1's alone: R1's history comes from `LocalApplied` (A-R65).
    #[test]
    fn an_applied_candidate_is_for_p1_only() {
        assert_eq!(consumers(Arm::AppliedCandidate), [ModuleName::Publication]);
    }
}
