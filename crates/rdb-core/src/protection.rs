//! Unsafe-age admission control and the durable resume state machine.
//!
//! Warns at the configured age, pauses admission at the pause threshold, and resumes only on the
//! exact durable barrier plus the hysteresis hold. The age is measured against the copy set
//! pinned by [`crate::contracts::ids::ConfigVersion`], so renaming or replacing a replica resets
//! nothing.
//!
//! **Owner:** team kernel-b, package L1. Specification: spec §6.2; team kernel-b `design.md` §4.
//!
//! # Shape
//!
//! One instance serves one partition on one node. It is **inert** until a
//! [`KernelEvent::Recovered`] names this node the primary (design §4.7: L1 runs on the primary
//! only). An inert instance answers every L1 input with `Ignored{Error(NotPrimary)}`. The live
//! state starts `Paused` at the recovery cutoff (design §4.1, K-B-47), so the first way out is a
//! resume.
//!
//! The first promotion binds the instance to its partition, and the binding outlives demotion.
//! An input addressed to another partition, a `Recovered` pinning another partition's
//! configuration, and a `ConfigChanged` for another partition each answer
//! `Ignored{Replica(InvalidConfig)}` and change nothing (review M1): L1 does not rely on the
//! dispatcher keying instances by partition.
//!
//! # Inputs reached through [`Module::step`]
//!
//! Three events drive transitions (design §4.4): a [`crate::contracts::time::TimerFired`] on
//! [`HEALTH_EVAL_TIMER`] (the health evaluation, read at `ctx.now`),
//! [`KernelEvent::QualificationChanged`] and [`KernelEvent::BlockPartition`]. `PeerProgress`,
//! `CopyLost`, `LocalApplied`, `DurableAdvanced`, `ConfigChanged` and
//! `TransitionBarrierConfirmed` only write state, which the next health evaluation reads; a drain
//! may also return Warn to Healthy (B-R42). Every step on an L1 input returns at least one
//! effect: `SetAdmission` on an admission edge, `ProtectionWarn` at the warn threshold, otherwise
//! exactly one `Ignored` (lead ruling B-R33, BA-2). Anything that is not an L1 input is refused
//! with `Err(Unavailable)` and returns no effects.
//!
//! L1 reads no `PartitionMode` (T-B-03): no input it consumes carries one.

mod state;

use crate::contracts::authority::BlockReason;
use crate::contracts::errors::{Capability, ErrorKind, RdbError};
use crate::contracts::event::{
    Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, ModuleName, StepCtx,
};
use crate::contracts::ids::{ConfigVersion, Generation, NodeId, PartitionId, TimerId};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::membership::CopyId;
use crate::contracts::protection::AdmissionState;
use crate::contracts::recovery::RecoveryResult;
use crate::contracts::time::Tick;
use crate::contracts::trace::ProtectionPhase;

use state::State;

/// The first [`TimerId`] L1 owns. Every id in `[PROTECTION_TIMER_BASE, +1)` is L1's.
///
/// A reserved block, the same pattern as `authority::AUTHORITY_TIMER_BASE` (`0x00A1_0000`,
/// four ids), and far from it: the two blocks cannot overlap. L1 arms no timer itself (design
/// §4.7); H1 fires this id on the health cadence and a fixture may author it directly.
pub const PROTECTION_TIMER_BASE: u64 = 0x00B1_0000;

/// The health-evaluation timer: design §4.4's `HealthEval{now}`, with `now` read from
/// `ctx.now` and never from the firing (T-B-02).
pub const HEALTH_EVAL_TIMER: TimerId = TimerId(PROTECTION_TIMER_BASE);

/// L1's protection mode (design §4.1).
///
/// `Paused` and `Reprotecting` carry no barrier here: the pause prefix and the resume barrier are
/// held beside the mode, because [`AdmissionState`] reports them in every mode (they are
/// "always meaningful"). In `Reprotecting` the barrier is [`AdmissionState::resume_barrier`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Within budget; admission allowed.
    Healthy,
    /// The oldest unsafe record is at or above the warn age; admission still allowed.
    Warn,
    /// Admission rejected until the resume barrier is durable on every predicate, a secondary
    /// qualifies, and the partition is not blocked.
    Paused,
    /// The barrier is met; admission stays rejected until `replication_lag` has stayed below
    /// `resume_lag_millis` for `resume_hold_millis`. A hold that completes while the oldest
    /// unsafe record is at or past `pause_age_millis` pauses at the head instead (B-R46 S1).
    Reprotecting {
        /// When the lag was first seen below the threshold in the current run of evaluations.
        below_since: Option<Tick>,
    },
}

impl Mode {
    /// The trace phase for this mode. `Reprotecting` is traced as
    /// [`ProtectionPhase::Resuming`] (design §4.5, ruling B-R33); the other three map by name.
    #[must_use]
    pub const fn phase(self) -> ProtectionPhase {
        match self {
            Self::Healthy => ProtectionPhase::Healthy,
            Self::Warn => ProtectionPhase::Warn,
            Self::Paused => ProtectionPhase::Paused,
            Self::Reprotecting { .. } => ProtectionPhase::Resuming,
        }
    }
}

/// Unsafe-age admission control and the durable resume state machine.
///
/// `Default` is the inert instance: the sim's dispatcher constructs every module by `Default`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Protection {
    /// The partition bound at the first promotion and the highest generation seen since. Kept
    /// through demotion, so a stale `Recovered` stays stale (review A1, B-R46 S4) and the
    /// instance stays bound to one partition (review M1). `None` until the first promotion.
    served: Option<Served>,
    /// `None` while this node is not the primary (design §4.7).
    state: Option<State>,
}

/// The partition an instance is bound to and the highest generation it has seen for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Served {
    partition: PartitionId,
    generation: Generation,
}

impl Protection {
    /// An inert instance. It becomes live on a `Recovered` that names this node the primary.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            served: None,
            state: None,
        }
    }

    /// The current mode, or `None` while inert.
    #[must_use]
    pub fn mode(&self) -> Option<Mode> {
        self.state.as_ref().map(State::mode)
    }

    /// What L1 would publish at `now`, or `None` while inert (design §4.5).
    #[must_use]
    pub fn admission_state(&self, now: Tick) -> Option<AdmissionState> {
        self.state.as_ref().map(|s| s.admission_state(now))
    }

    /// Whether the last qualification edge was `Gained`. `false` while inert.
    #[must_use]
    pub fn qualifies_now_at_head(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(State::qualifies_now_at_head)
    }

    /// The block, if one arrived. Never cleared inside a live instance (design §4.4).
    #[must_use]
    pub fn blocked(&self) -> Option<&BlockReason> {
        self.state.as_ref().and_then(State::blocked)
    }

    /// The copies whose liveness gates resume: the current predicate minus self minus lost
    /// (design §4.2). Empty while inert.
    #[must_use]
    pub fn lag_domain(&self) -> Vec<CopyId> {
        self.state
            .as_ref()
            .map(|s| s.lag_domain().into_iter().collect())
            .unwrap_or_default()
    }

    /// The nodes of the current required-copy predicate: the primary plus the regular
    /// secondaries, never a shadow, sorted (lead ruling B-R42). Empty while inert, including
    /// after a demotion. An old predicate still awaiting its transition barrier is not included.
    #[must_use]
    pub fn required_copy_set(&self) -> Vec<NodeId> {
        self.state
            .as_ref()
            .map(State::required_copy_set)
            .unwrap_or_default()
    }

    /// How many records are applied but not yet durable on every active predicate.
    #[must_use]
    pub fn unsafe_len(&self) -> usize {
        self.state.as_ref().map_or(0, State::unsafe_len)
    }

    /// The configuration versions above the current predicate that R1 reported durable before
    /// L1 pinned them (lead ruling B-R46d), ascending. Empty while inert.
    #[must_use]
    pub fn pending_durable_versions(&self) -> Vec<ConfigVersion> {
        self.state
            .as_ref()
            .map(State::pending_durable_versions)
            .unwrap_or_default()
    }

    /// The earliest tick at which a health evaluation could change the state (design §4.7).
    ///
    /// A hint for the scheduler, never a contract: the health cadence stands whether or not it
    /// is honoured.
    #[must_use]
    pub fn next_interesting_tick(&self) -> Option<Tick> {
        self.state.as_ref().and_then(State::next_interesting_tick)
    }

    /// Builds a fresh live instance when `result` names this node the primary; otherwise inert.
    ///
    /// A `Recovered` for the generation already served is F1's activation re-emission (design
    /// §5.6a). L1 is mode-blind, so it changes nothing and the instance is kept: rebuilding here
    /// would re-pause a partition whose qualification edge R1 will not send again. An older
    /// generation is stale and is kept the same way, unsafe queue included (lead ruling B-R38),
    /// and stays stale after a demotion (review A1).
    ///
    /// Once bound, every newer `Recovered` raises the served generation, one that demotes or
    /// names another node included, so a generation below any seen stays stale (lead ruling
    /// B-R46 S4). An unbound instance records nothing until it is promoted (M7B-82).
    ///
    /// A live instance that a newer generation demotes goes inert fail-closed
    /// ([`State::demote`]): one that was admitting publishes a reject, one already rejecting
    /// answers `Ignored{Error(NotPrimary)}`, because what it last published already rejects.
    fn on_recovered(
        &mut self,
        ctx: &StepCtx<'_>,
        partition: PartitionId,
        result: &RecoveryResult,
    ) -> Vec<KernelEffect> {
        if self.serves_another(partition) {
            return invalid_config();
        }
        let rebuilt = match State::at_recovery(result, partition, ctx.node, *ctx.budgets) {
            Ok(rebuilt) => rebuilt,
            Err(reason) => return ignored(KernelIgnoredReason::Replica(reason)),
        };
        if self
            .served
            .is_some_and(|served| served.generation >= result.new_generation)
        {
            return ignored(KernelIgnoredReason::Replica(
                ReplicaIgnoreReason::NotRequired,
            ));
        }
        if self.served.is_some() || rebuilt.is_some() {
            self.served = Some(Served {
                partition,
                generation: result.new_generation,
            });
        }
        let previous = std::mem::replace(&mut self.state, rebuilt);
        match (&self.state, previous) {
            (Some(live), _) => vec![KernelEffect::SetAdmission(live.admission_state(ctx.now))],
            (None, Some(demoted)) => demoted
                .demote(ctx.now)
                .map_or_else(not_primary, |e| vec![e]),
            (None, None) => not_primary(),
        }
    }

    /// Whether this instance is bound to a partition other than `partition` (review M1).
    fn serves_another(&self, partition: PartitionId) -> bool {
        self.served
            .is_some_and(|served| served.partition != partition)
    }

    /// The live state for an input addressed to `partition`, or the answer when there is none:
    /// `InvalidConfig` for another partition's input (review M1), `NotPrimary` while inert.
    fn live(&mut self, partition: PartitionId) -> Result<&mut State, Vec<KernelEffect>> {
        if self.serves_another(partition) {
            return Err(invalid_config());
        }
        self.state.as_mut().ok_or_else(not_primary)
    }
}

impl Module for Protection {
    fn name(&self) -> ModuleName {
        ModuleName::Protection
    }

    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
        let kinds = match &event.kind {
            EventKind::Timer(fired) if fired.id == HEALTH_EVAL_TIMER => {
                match self.live(event.partition) {
                    Ok(live) => live.health_eval(ctx.now),
                    Err(refusal) => refusal,
                }
            }
            EventKind::Kernel(KernelEvent::Recovered(result)) => {
                self.on_recovered(ctx, event.partition, result)
            }
            EventKind::Kernel(input) => match self.live(event.partition) {
                Ok(live) => live.on_input(ctx.now, input),
                Err(refusal) => State::is_input(input).then_some(refusal),
            }
            .ok_or_else(not_an_input)?,
            _ => return Err(not_an_input()),
        };
        Ok(kinds
            .into_iter()
            .map(|kind| Effect {
                correlation: event.correlation,
                from: ModuleName::Protection,
                partition: event.partition,
                kind: EffectKind::Kernel(kind),
            })
            .collect())
    }
}

/// The refusal for anything that is not [`HEALTH_EVAL_TIMER`] or an L1 kernel input.
fn not_an_input() -> RdbError {
    RdbError::unavailable(
        Capability::Protection,
        "protection: not an L1 input (HEALTH_EVAL_TIMER or an L1 kernel event)",
    )
}

/// Exactly one `Ignored` (BA-2).
fn ignored(reason: KernelIgnoredReason) -> Vec<KernelEffect> {
    vec![KernelEffect::Ignored { reason }]
}

/// The inert instance's answer to every L1 input (M7B-82).
fn not_primary() -> Vec<KernelEffect> {
    ignored(KernelIgnoredReason::Error(ErrorKind::NotPrimary))
}

/// The answer to an input addressed to a partition this instance does not serve (review M1).
fn invalid_config() -> Vec<KernelEffect> {
    ignored(KernelIgnoredReason::Replica(
        ReplicaIgnoreReason::InvalidConfig,
    ))
}
