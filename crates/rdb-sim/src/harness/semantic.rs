//! The semantic trace lines: what the kernel decided, written at the seam where the harness
//! sees the effect that says so.
//!
//! Until this module the sim recorded only its own plumbing (`Capability`, `ModuleDispatch`,
//! `KernelNoted`, `ControlInteraction`, `ProtectionState`, `TopologyChange`). The oracle's
//! INV-AUTH arms on `AuthorityDecision` and INV-PUB on `Publish`, so on a real run neither could
//! ever arm, and a kernel that replied under a denied authority passed every sim row.
//!
//! [`Semantic::record`] is called by the run loop once per **answered** offer, after the
//! `ModuleDispatch` record and **before** the effects are delivered, with the effects the module
//! returned. Every line is a projection of one effect the kernel really emitted, plus state the
//! harness already holds (A1's grant, R1's tracker); none is inferred from an absence.
//!
//! | Line | From | Notes |
//! |---|---|---|
//! | `AuthorityDecision` | A1's `AuthorityEffect::Answer` | the window is A1's own local rule, see [`window`] |
//! | `Publish` | P1's `KernelEffect::Published`, in the step answering its `Publication` check | evidence from the co-located R1 tracker; `authority_recheck` is the decision line that answer was recorded as |
//! | `ClientOutcomeReported` | any `ReplyEffect::Transaction` or `ReplyEffect::Failed` | `delivered` is always true: the sim has no lost-reply fault |
//! | `ReplicationAck` | R1's `SendEffect::Unicast` whose body decodes as `AppendOutcome::Accepted` | at the acknowledging node, `Buffered` at `buffered_applied`, see [`ack_line`] |
//! | `RecoveryDecision` | F1's `RecoveryEffect::Quarantine`, written just before its `Quarantine` line | `mode = Quarantine`; no source, cutoff, digest or new generation (`None`, L-R177gd); the rest from what the harness delivered to F1, see [`quarantine_decision`] |
//! | `Quarantine` | F1's `RecoveryEffect::Quarantine` | in the plan anchor's generation, sources by F1's plan placement, see [`quarantine_line`] |
//!
//! Two more lines are what the **environment** did rather than what a module answered, so the
//! dispatcher writes them where it carries the effect out, and the run loop drains them after
//! each delivery ([`crate::harness::dispatch::Dispatcher::take_lines`]):
//!
//! | Line | From | Notes |
//! |---|---|---|
//! | `BatchApply` | every `StoreEffect::Commit`, and every scenario preload | the batch's own `History` record's digests, see [`apply_line`] |
//! | `DurabilityAdvance` | every engine sync: a host flush, F1's `SyncWalThrough`, a durable preload | one per captured prefix, `Synced` only where the engine reported the whole capture durable, `Partial` where it reported less (V-R36), see [`durability_lines`] |
//!
//! An acknowledgement is recorded when R1 answers the `Committed` its own staging produced, so
//! the acknowledging node's `BatchApply` always precedes it — which is what the validator's
//! M7V-88 rule (`acks_against_applies`) checks.
//!
//! # What is deliberately not recorded
//!
//! * A decision at `Checkpoint::Read` or `Checkpoint::OutboxDispatch`: the trace's
//!   `AuthorityGate` has no member for either, and writing one as another gate would be false.
//! * A `Durable` acknowledgement class. R1's `AppendAck` carries one progress triple, and the
//!   line is written once, at its buffered position; a second line at `durable` would replace the
//!   first in the oracle's `(node, boot)` fold. `Publish` evidence stays `Buffered` for the same
//!   reason.
//! * `BatchApply::key_versions`: always empty. A trace names a key by `KeyId`, which the scenario
//!   generator assigns, and the harness holds only byte keys. INV-ATOM and the read rules that
//!   compare versions therefore see no versions from a real run.
//! * A rejected append (`accepted = false`): R1 answers a refusal with `AppendOutcome::Rejected`
//!   or an `Ignored` note, and neither carries the `AckRejectReason` the line needs.
//! * `ReplyEffect::Status` and `ReplyEffect::Read`: neither is a write's outcome.
//! * `RecoveryDecision` for a recovery that **selected** (`TwoSurvivor`, `LoneSurvivorReadOnly`).
//!   Not written yet: only a quarantine's decision is (row M7V-80). A gap, not a ruling.

use std::collections::BTreeMap;

use rdb_core::contracts::authority::{AuthorityDecision, AuthorityEffect, Checkpoint, DenyReason};
use rdb_core::contracts::authority::{AuthorityEvent, Verdict};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::{AppendOutcome, ReplicationEnvelope};
use rdb_core::contracts::event::{
    Budgets, Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, ModuleName,
    ReplyEffect,
};
use rdb_core::contracts::ids::{
    BootId, CorrelationId, Generation, NodeId, OwnerEpoch, PartitionId, ReplicaRole,
    RequestIdentity, Seq,
};
use rdb_core::contracts::membership::{CopyId, Member};
use rdb_core::contracts::recovery::{DivergenceEvidence, RecoveryEffect, RecoveryEvent};
use rdb_core::contracts::storage::{Batch, CapturedPrefix, DurablePrefix, StorageFault};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    AckEvidence, ApplyOutcome, AuthorityGate, AuthorityOutcome, ClientOutcome, DurabilityClass,
    EventRef, QuarantineReason, QueriedSource, RecoveryMode, SyncOutcome, TraceKind,
};
use rdb_core::contracts::transport::{Frame, SendEffect};
use rdb_core::contracts::txn::Outcome;
use rdb_core::replication::wire::decode_reply;

use crate::error::SimError;
use crate::harness::dispatch::Dispatcher;
use crate::harness::trace::{Recorder, Site};
use crate::storage::history::history_writes;
use crate::storage::memory::MemoryEngine;

/// What a published record's reply needs from the `Publish` line before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Published {
    generation: Generation,
    seq: Seq,
    digest: Digest,
}

/// The cross-line state: which event each decision was recorded as, and what each published
/// request's reply will report.
#[derive(Debug, Default)]
pub struct Semantic {
    /// `(node, the kernel's correlation) -> the AuthorityDecision line`. The kernel correlation is
    /// the one A1 answered under, which is how P1 matches its answer, so it is how a `Publish`
    /// names its recheck.
    decisions: BTreeMap<(NodeId, CorrelationId), EventRef>,
    /// `(node, request) -> what its Publish line said`.
    published: BTreeMap<(NodeId, RequestIdentity), Published>,
    /// `(F1's node, partition) -> the anchor generation and the pinned members`, from the
    /// `RecoveryEvent::Plan` F1 answered: what a `Quarantine` line names its sources with, and
    /// what a quarantine's `RecoveryDecision` lists as queried (F1 queries every member).
    plans: BTreeMap<(NodeId, PartitionId), (Generation, Vec<Member>)>,
    /// `(F1's node, partition) -> what its discovery was delivered`, from the fence F1 accepted.
    discoveries: BTreeMap<(NodeId, PartitionId), Discovery>,
}

/// One F1 discovery, as the harness delivered it: what a quarantine's `RecoveryDecision` says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discovery {
    /// The epoch the accepted fence proved fenced.
    pub fenced_epoch: OwnerEpoch,
    /// The tick that fence arrived at F1: the window opens there, never at the proof's
    /// `decision_tick` (K-B-14).
    pub opened_at: Tick,
    /// The tick F1 emitted `CloseWindow`, if it has.
    pub closed_at: Option<Tick>,
    /// Each copy's answer while the window was open: its `(generation, head, digest)` for an
    /// inventory, `None` for `InventoryFailed`. A copy with no entry never answered.
    pub answers: BTreeMap<CopyId, Option<(Generation, Seq, Digest)>>,
}

impl Semantic {
    /// Record the semantic lines `module`'s answer to `event` carries, at `site`.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `semantic::publish_recheck` when P1 publishes in a step that
    /// is not the answer to a recorded `Publication` decision: a publish without its recheck is
    /// exactly what INV-AUTH exists to catch, and the harness will not paper over it. Otherwise
    /// whatever [`Recorder::record`] returns.
    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &mut self,
        recorder: &mut Recorder,
        site: Site,
        module: ModuleName,
        event: &Event,
        effects: &[Effect],
        dispatcher: &Dispatcher,
        budgets: &Budgets,
    ) -> Result<(), SimError> {
        if module == ModuleName::Recovery {
            self.recovery_input(event, effects);
        }
        for effect in effects {
            match &effect.kind {
                EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::CloseWindow))
                    if module == ModuleName::Recovery =>
                {
                    if let Some(discovery) =
                        self.discoveries.get_mut(&(event.node, event.partition))
                    {
                        discovery.closed_at.get_or_insert(event.at);
                    }
                }
                EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::Quarantine(
                    evidence,
                ))) if module == ModuleName::Recovery => {
                    let key = (event.node, event.partition);
                    let plan = self.plans.get(&key);
                    let decision = plan
                        .zip(self.discoveries.get(&key))
                        .and_then(|((_, members), discovery)| {
                            quarantine_decision(discovery, event.at, members, |node| {
                                dispatcher.boot(node)
                            })
                        })
                        .ok_or(SimError::Config {
                            field: "semantic::recovery_decision",
                        })?;
                    recorder.record(site, decision)?;
                    let line = plan
                        .and_then(|(generation, members)| {
                            let nodes = members
                                .iter()
                                .map(|member| (member.copy, member.node))
                                .collect();
                            quarantine_line(*generation, &nodes, evidence)
                        })
                        .ok_or(SimError::Config {
                            field: "semantic::quarantine_sources",
                        })?;
                    recorder.record(site, line)?;
                }
                EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Answer(decision))) => {
                    let Some(line) = decision_line(decision, dispatcher, budgets) else {
                        continue;
                    };
                    let at = recorder.record(site, line)?;
                    self.decisions
                        .insert((event.node, decision.correlation), at);
                }
                EffectKind::Kernel(KernelEffect::Published {
                    lineage,
                    seq,
                    record_digest,
                    request,
                }) if module == ModuleName::Publication => {
                    let recheck = publication_answer(event)
                        .and_then(|answer| self.decisions.get(&(event.node, answer)).copied())
                        .ok_or(SimError::Config {
                            field: "semantic::publish_recheck",
                        })?;
                    let line = TraceKind::Publish {
                        generation: lineage.generation,
                        seq: *seq,
                        published_digest: *record_digest,
                        ack_evidence: ack_evidence(dispatcher, event, *seq),
                        authority_recheck: recheck,
                    };
                    recorder.record(site, line)?;
                    self.published.insert(
                        (event.node, *request),
                        Published {
                            generation: lineage.generation,
                            seq: *seq,
                            digest: *record_digest,
                        },
                    );
                }
                EffectKind::Reply(reply) => {
                    if let Some(line) = self.outcome_line(reply, event, dispatcher) {
                        recorder.record(site, line)?;
                    }
                }
                EffectKind::Send(SendEffect::Unicast { to, frame })
                    if module == ModuleName::Replication =>
                {
                    if let Some(line) = ack_line(*to, frame) {
                        recorder.record(site, line)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Fold what the harness delivered to F1: its plan, the fence it **accepted** (the step asks
    /// for inventory; a refused fence opens nothing), and each copy's answer while the window
    /// is open. A new accepted fence starts a new discovery.
    fn recovery_input(&mut self, event: &Event, effects: &[Effect]) {
        let EventKind::Kernel(KernelEvent::Recovery(input)) = &event.kind else {
            return;
        };
        let key = (event.node, event.partition);
        match input {
            RecoveryEvent::Plan(plan) => {
                self.plans.insert(
                    key,
                    (plan.anchor.lineage.generation, plan.config.members.clone()),
                );
            }
            RecoveryEvent::FenceProven(proof) => {
                let accepted = effects.iter().any(|effect| {
                    matches!(
                        effect.kind,
                        EffectKind::Kernel(KernelEffect::Recovery(
                            RecoveryEffect::QueryInventory { .. }
                        ))
                    )
                });
                if accepted {
                    self.discoveries.insert(
                        key,
                        Discovery {
                            fenced_epoch: proof.prior_owner_epoch,
                            opened_at: event.at,
                            closed_at: None,
                            answers: BTreeMap::new(),
                        },
                    );
                }
            }
            RecoveryEvent::InventoryReported(inventory)
            | RecoveryEvent::StaleOwnerReturned(inventory) => {
                if let Some(discovery) = self.open_discovery(key) {
                    discovery.answers.insert(
                        inventory.copy,
                        Some((
                            inventory.anchor_seen.lineage.generation,
                            inventory.head.0,
                            inventory.head.1,
                        )),
                    );
                }
            }
            RecoveryEvent::InventoryFailed { copy } => {
                if let Some(discovery) = self.open_discovery(key) {
                    discovery.answers.insert(*copy, None);
                }
            }
            _ => {}
        }
    }

    /// The discovery at `key`, while its window is open.
    fn open_discovery(&mut self, key: (NodeId, PartitionId)) -> Option<&mut Discovery> {
        self.discoveries
            .get_mut(&key)
            .filter(|discovery| discovery.closed_at.is_none())
    }

    fn outcome_line(
        &self,
        reply: &ReplyEffect,
        event: &Event,
        dispatcher: &Dispatcher,
    ) -> Option<TraceKind> {
        match reply {
            ReplyEffect::Transaction { identity, result } => {
                let digest = self
                    .published
                    .get(&(event.node, *identity))
                    .filter(|p| p.generation == result.generation && p.seq == result.seq)
                    .map_or(Digest::ROOT, |p| p.digest);
                Some(TraceKind::ClientOutcomeReported {
                    request: identity.request,
                    outcome: match result.outcome {
                        Outcome::Published => ClientOutcome::Success,
                        Outcome::RecoveredApplied => ClientOutcome::RecoveredApplied,
                    },
                    generation: result.generation,
                    seq: Some(result.seq),
                    result_digest: digest,
                    delivered: true,
                })
            }
            ReplyEffect::Failed { identity, error } => Some(TraceKind::ClientOutcomeReported {
                request: identity.request,
                outcome: ClientOutcome::Error(error.kind()),
                generation: dispatcher.adopted(event.node, event.partition).generation,
                seq: None,
                result_digest: Digest::ROOT,
                delivered: true,
            }),
            _ => None,
        }
    }
}

/// The correlation of the `Publication` decision `event` delivers, if it delivers one.
fn publication_answer(event: &Event) -> Option<CorrelationId> {
    match &event.kind {
        EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Answer(answer)))
            if answer.checkpoint == Checkpoint::Publication =>
        {
            Some(answer.correlation)
        }
        _ => None,
    }
}

/// `decision` as a trace line, or `None` for a checkpoint the trace has no gate for.
fn decision_line(
    decision: &AuthorityDecision,
    dispatcher: &Dispatcher,
    budgets: &Budgets,
) -> Option<TraceKind> {
    let gate = gate(decision.checkpoint)?;
    let renewed_at = dispatcher
        .authority(decision.owner)
        .and_then(|a1| a1.view().renewed_at);
    let (valid_from, expiry) = window(renewed_at, decision.decided_at, budgets);
    Some(TraceKind::AuthorityDecision {
        gate,
        owner_node: decision.owner,
        owner_epoch: decision.lineage.owner_epoch,
        grant: decision.grant,
        grant_boot: decision.boot,
        generation: decision.lineage.generation,
        valid_from_tick: valid_from,
        expiry_tick: expiry,
        decision_tick: decision.decided_at.0,
        authority_seq: decision.authority_seq,
        outcome: outcome(decision.verdict),
    })
}

/// The trace's gate for `checkpoint`. `Read` and `OutboxDispatch` have none.
#[must_use]
pub const fn gate(checkpoint: Checkpoint) -> Option<AuthorityGate> {
    match checkpoint {
        Checkpoint::Admission => Some(AuthorityGate::Admission),
        Checkpoint::StorageDispatch => Some(AuthorityGate::Dispatch),
        Checkpoint::Publication => Some(AuthorityGate::Publication),
        Checkpoint::Reply => Some(AuthorityGate::Reply),
        Checkpoint::Read | Checkpoint::OutboxDispatch => None,
    }
}

/// The grant window a decision is judged against, half-open, in ticks.
///
/// A1's local rule (`authority::clock::local_ok`): a grant renewed at `r` admits at `now` while
/// `now - r + dispatch_margin < grant`, so the window is `[r, r + grant - margin)`. That is the
/// conservative bound A1 itself enforces; the control-time bound (`utc_ok`) is a second
/// conjunct that can only close the window earlier, so a `Valid` decision lies inside this one
/// whenever A1 is correct. With no grant held the window is empty at the decision tick.
#[must_use]
pub const fn window(renewed_at: Option<Tick>, decided_at: Tick, budgets: &Budgets) -> (u64, u64) {
    match renewed_at {
        Some(at) => (
            at.0,
            at.0.saturating_add(budgets.grant_millis)
                .saturating_sub(budgets.dispatch_margin_millis),
        ),
        None => (decided_at.0, decided_at.0),
    }
}

/// The trace's outcome for a verdict, as lead ruling **A-R76** fixes it.
///
/// No wildcard: a reason added to [`DenyReason`] fails this match, and its class is then the
/// lead's to rule, not this module's to guess. `Expired` is a grant that ran out or was never
/// held; `Fenced` is authority that something ended (a freeze, a revocation, a fence, a lineage
/// or boot that moved); `Uncertain` is every reason A1 denies because it cannot tell (spec §7.2:
/// uncertainty denies).
///
/// | Outcome | Deny reasons (A-R76) |
/// |---|---|
/// | `Valid` | none: `Verdict::Admit` |
/// | `Expired` | `NoGrant`, `Expired` |
/// | `Fenced` | `Frozen`, `Revoked`, `EpochRevoked`, `BootMismatch`, `AuthorityGenerationChanged`, `GenerationChanged`, `SelfFenced`, `LocalStorageFenced` |
/// | `Uncertain` | `ExpiryUnproven`, `ClockUnbounded`, `ClockModeUnbounded`, `ClockSampleStale`, `ProcessSuspended`, `ControlUnavailable` |
#[must_use]
pub const fn outcome(verdict: Verdict) -> AuthorityOutcome {
    match verdict {
        Verdict::Admit => AuthorityOutcome::Valid,
        Verdict::Deny(reason) => match reason {
            DenyReason::NoGrant | DenyReason::Expired => AuthorityOutcome::Expired,
            DenyReason::Frozen
            | DenyReason::Revoked
            | DenyReason::EpochRevoked
            | DenyReason::BootMismatch
            | DenyReason::AuthorityGenerationChanged
            | DenyReason::GenerationChanged
            | DenyReason::SelfFenced
            | DenyReason::LocalStorageFenced => AuthorityOutcome::Fenced,
            DenyReason::ExpiryUnproven
            | DenyReason::ClockUnbounded
            | DenyReason::ClockModeUnbounded
            | DenyReason::ClockSampleStale
            | DenyReason::ProcessSuspended
            | DenyReason::ControlUnavailable => AuthorityOutcome::Uncertain,
        },
    }
}

/// The copies R1's tracker on `event`'s node counts for `seq` — the same set P1's predicate
/// read — as buffered evidence. Empty when that node holds no primary for the partition.
fn ack_evidence(dispatcher: &Dispatcher, event: &Event, seq: Seq) -> Vec<AckEvidence> {
    let Some(primary) = dispatcher
        .replication()
        .primary(event.node, event.partition)
    else {
        return Vec::new();
    };
    let tracker = primary.tracker();
    tracker
        .qualified_copies(seq)
        .into_iter()
        .filter_map(|copy| tracker.peer(copy))
        .map(|peer| AckEvidence {
            node: peer.node,
            boot: peer.boot,
            role: peer.role,
            durability: DurabilityClass::Buffered,
        })
        .collect()
}

/// The `ReplicationAck` line for an R1 reply `frame` sent to `to`, when it accepts.
///
/// Every field is the acknowledging copy's own `AppendAck`: the role it claims, its boot, the
/// configuration and lineage it is pinned to, and `buffered_applied` with the digest there. The
/// class is `Buffered` because that is the position the line names. `None` for any frame that is
/// not an accepted reply: an append, a refusal, `AlreadyHave`, `Busy`.
#[must_use]
pub fn ack_line(to: NodeId, frame: &Frame) -> Option<TraceKind> {
    let Ok(AppendOutcome::Accepted(ack)) = decode_reply(&frame.body) else {
        return None;
    };
    Some(TraceKind::ReplicationAck {
        from_node: ack.from,
        to_node: to,
        peer_role: ack.role,
        peer_boot: ack.boot,
        config_version: ack.config_version,
        generation: ack.generation,
        owner_epoch: ack.owner_epoch,
        contiguous_seq: Seq(ack.progress.buffered_applied.0),
        contiguous_digest: ack.digest_at_buffered,
        durability_class: DurabilityClass::Buffered,
        accepted: true,
        reject_reason: None,
    })
}

/// The `Quarantine` line for F1's `RecoveryEffect::Quarantine(evidence)`, in the anchor's
/// `generation`, naming each copy the evidence names by the node `nodes` (F1's plan) places it on.
///
/// Two survivors holding different digests at one position is `DigestConflict` at that position.
/// A survivor whose ladder holds another digest at the root's base is `DigestConflict` at the
/// base; one with no rung there at all is `CorruptHistory` ("required history is missing").
/// `None` when a named copy is not in the plan: a source list short of one would be false.
#[must_use]
pub fn quarantine_line(
    generation: Generation,
    nodes: &BTreeMap<CopyId, NodeId>,
    evidence: &DivergenceEvidence,
) -> Option<TraceKind> {
    let (reason, seq, copies) = match evidence {
        DivergenceEvidence::Pairwise { seq, a, b } => {
            (QuarantineReason::DigestConflict, *seq, vec![a.0, b.0])
        }
        DivergenceEvidence::RootMismatch {
            copy,
            base_seq,
            found,
            ..
        } => (
            if found.is_some() {
                QuarantineReason::DigestConflict
            } else {
                QuarantineReason::CorruptHistory
            },
            *base_seq,
            vec![*copy],
        ),
    };
    let sources = copies
        .iter()
        .map(|copy| nodes.get(copy).copied())
        .collect::<Option<Vec<NodeId>>>()?;
    Some(TraceKind::Quarantine {
        reason,
        generation,
        seq,
        sources,
    })
}

/// The `RecoveryDecision` line for F1's quarantine, decided at `decided_at`, over `discovery`
/// and the pinned `members` F1 queried, each at the boot `boot_of` says its node runs under.
///
/// Every field is either what F1 was delivered or `None` where F1 chose nothing:
///
/// * `mode` is `Quarantine`; `selected_source`, `selected_cutoff_seq`, `selected_digest` and
///   `new_generation` are `None`. A quarantine selects no prefix and creates no lineage, and the
///   contract says so (L-R177gd). A stand-in here is the value V-R29 refused.
/// * `fenced_epoch` is the accepted fence's `prior_owner_epoch`.
/// * `discovery_window_ticks` runs from the fence's arrival to `CloseWindow`, or to the
///   quarantine when F1 decided before its window closed.
/// * `queried_sources` is one entry per member, in the plan's order. `reachable` means the copy
///   answered with an inventory while the window was open; its report is that inventory's
///   lineage generation and head. A failed or silent copy is unreachable and reports nothing.
/// * `loss_uncertainty` is `false`: F1 builds its loss record, whose `uncertain` compares the
///   highest advertised position with the cutoff, only for a selected cutoff. A quarantine cuts
///   nothing and deletes nothing (spec §8.4), so no suffix may have been lost by this decision.
///
/// `None` when a member's node has no boot: a source list with an invented boot would be false.
#[must_use]
pub fn quarantine_decision(
    discovery: &Discovery,
    decided_at: Tick,
    members: &[Member],
    boot_of: impl Fn(NodeId) -> Option<BootId>,
) -> Option<TraceKind> {
    let queried_sources = members
        .iter()
        .map(|member| {
            let report = discovery.answers.get(&member.copy).copied().flatten();
            Some(QueriedSource {
                node: member.node,
                boot: boot_of(member.node)?,
                role: member.role,
                reachable: report.is_some(),
                reported_generation: report.map(|(generation, _, _)| generation),
                reported_seq: report.map(|(_, seq, _)| seq),
                reported_digest: report.map(|(_, _, digest)| digest),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let closed = discovery.closed_at.unwrap_or(decided_at);
    Some(TraceKind::RecoveryDecision {
        fenced_epoch: discovery.fenced_epoch,
        discovery_window_ticks: closed.0.saturating_sub(discovery.opened_at.0),
        queried_sources,
        selected_source: None,
        selected_cutoff_seq: None,
        selected_digest: None,
        mode: RecoveryMode::Quarantine,
        loss_uncertainty: false,
        new_generation: None,
    })
}

/// The `BatchApply` line for `batch`, committed on a node holding `role`, ending `outcome`.
///
/// The digests are the batch's own `History` record at its seq: `prev_digest` as the
/// predecessor and `record_digest` as the entry — the bytes the engine holds once the commit
/// lands. `None` for a batch that writes no decodable record there: without one the harness
/// cannot say which entry was applied, and a made-up digest would arm INV-LIN on a value the
/// kernel never wrote. `key_versions` is empty (see the module notes).
#[must_use]
pub fn apply_line(batch: &Batch, role: ReplicaRole, outcome: ApplyOutcome) -> Option<TraceKind> {
    let (record, _) = history_writes(batch, batch.seq)?;
    let envelope = ReplicationEnvelope::decode(&record).ok()?;
    Some(TraceKind::BatchApply {
        role,
        generation: batch.generation,
        seq: batch.seq,
        predecessor_seq: Seq(batch.seq.0.saturating_sub(1)),
        predecessor_digest: envelope.prev_digest,
        entry_digest: envelope.record_digest,
        batch: batch.id.0,
        key_versions: Vec::new(),
        outcome,
    })
}

/// The `DurabilityAdvance` lines one sync of `captured` on `engine` owes, one per captured
/// prefix, each with the partition it belongs to. `synced` is what the sync returned, and
/// `engine` is read **after** it.
///
/// * The sync failed: `Failed`, at the watermark the engine still holds.
/// * It succeeded and reported the prefix durable through at least what was captured: `Synced`,
///   at what it reported (which may be above the capture, when an earlier sync got further).
/// * It succeeded and reported the prefix durable through **less** than was captured:
///   `Partial`, at what it reported. A `ShortFlush` is this case, and so is a capture above
///   what the engine had applied. The `SyncOutcome` contract says only `Synced` publishes the
///   captured prefix, and this one was not all made durable (lead ruling V-R36, rows M7V-98 and
///   M7V-99).
/// * It succeeded and did not report the prefix: `Partial`, at the watermark the engine still
///   holds. A `FalseDurable` flush is this case: it completes with an empty `durable`, so no
///   watermark moved, and the line never says `Synced` for it.
///
/// The digest is the engine's own record at that position, or [`Digest::ROOT`] where it holds
/// none (position 0, or a history committed without `History` records).
#[must_use]
pub fn durability_lines(
    engine: &MemoryEngine,
    ticket: u64,
    captured: &[CapturedPrefix],
    synced: Result<&[DurablePrefix], &StorageFault>,
) -> Vec<(PartitionId, TraceKind)> {
    let all: Vec<(PartitionId, Seq)> = captured
        .iter()
        .map(|prefix| (prefix.partition, Seq(prefix.through.0)))
        .collect();
    captured
        .iter()
        .map(|prefix| {
            let (partition, generation) = (prefix.partition, prefix.generation);
            let held = Seq(engine.durable(partition, generation).0);
            let (durable_seq, outcome) = match synced {
                Err(_) => (held, SyncOutcome::Failed),
                Ok(durable) => durable
                    .iter()
                    .find(|d| d.partition == partition && d.generation == generation)
                    .map_or((held, SyncOutcome::Partial), |d| {
                        let outcome = if d.through.0 < prefix.through.0 {
                            SyncOutcome::Partial
                        } else {
                            SyncOutcome::Synced
                        };
                        (Seq(d.through.0), outcome)
                    }),
            };
            let line = TraceKind::DurabilityAdvance {
                generation,
                durable_seq,
                durable_digest: record_digest(engine, partition, generation, durable_seq),
                flush_ticket: ticket,
                captured: all.clone(),
                outcome,
            };
            (partition, line)
        })
        .collect()
}

/// The digest of the record `engine` holds at `seq`, or the root where it holds none.
fn record_digest(
    engine: &MemoryEngine,
    partition: PartitionId,
    generation: Generation,
    seq: Seq,
) -> Digest {
    engine
        .history_at(partition, generation, seq)
        .and_then(|(record, _)| ReplicationEnvelope::decode(&record).ok())
        .map_or(Digest::ROOT, |envelope| envelope.record_digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_is_a1s_local_rule_and_empty_without_a_grant() {
        let budgets = Budgets::SPEC_DEFAULTS;
        let (from, until) = window(Some(Tick(1_000)), Tick(1_200), &budgets);
        assert_eq!(from, 1_000);
        assert_eq!(
            until,
            1_000 + budgets.grant_millis - budgets.dispatch_margin_millis
        );
        // The boundary is A1's: at `until - 1` local_ok admits, at `until` it does not.
        assert!(rdb_core::authority::clock::local_ok(
            Tick(1_000),
            Tick(until - 1),
            &budgets
        ));
        assert!(!rdb_core::authority::clock::local_ok(
            Tick(1_000),
            Tick(until),
            &budgets
        ));
        assert_eq!(window(None, Tick(7), &budgets), (7, 7));
    }

    #[test]
    fn read_and_outbox_checkpoints_have_no_gate() {
        assert_eq!(gate(Checkpoint::Read), None);
        assert_eq!(gate(Checkpoint::OutboxDispatch), None);
        assert_eq!(
            gate(Checkpoint::StorageDispatch),
            Some(AuthorityGate::Dispatch)
        );
    }

    #[test]
    fn admit_is_valid_and_each_deny_class_is_named() {
        assert_eq!(outcome(Verdict::Admit), AuthorityOutcome::Valid);
        assert_eq!(
            outcome(Verdict::Deny(DenyReason::Expired)),
            AuthorityOutcome::Expired
        );
        assert_eq!(
            outcome(Verdict::Deny(DenyReason::GenerationChanged)),
            AuthorityOutcome::Fenced
        );
        assert_eq!(
            outcome(Verdict::Deny(DenyReason::ClockSampleStale)),
            AuthorityOutcome::Uncertain
        );
    }

    // Tester finding F5 (mutant T1): the scaffold in `tests/dispatch.rs` drives only `Pairwise`, so
    // both `RootMismatch` shapes are pinned here. A survivor holding another digest at the root
    // is a conflict; one with no rung there has a corrupt history. Either names its own node.
    #[test]
    fn quarantine_line_names_both_root_mismatch_shapes() {
        let nodes: BTreeMap<CopyId, NodeId> =
            [(CopyId(0), NodeId(1)), (CopyId(2), NodeId(3))].into();
        let mismatch = |found| DivergenceEvidence::RootMismatch {
            copy: CopyId(2),
            base_seq: Seq(4),
            expected: Digest([1; 32]),
            found,
        };
        let line = |reason| TraceKind::Quarantine {
            reason,
            generation: Generation(2),
            seq: Seq(4),
            sources: vec![NodeId(3)],
        };
        assert_eq!(
            quarantine_line(Generation(2), &nodes, &mismatch(Some(Digest([2; 32])))),
            Some(line(QuarantineReason::DigestConflict))
        );
        assert_eq!(
            quarantine_line(Generation(2), &nodes, &mismatch(None)),
            Some(line(QuarantineReason::CorruptHistory))
        );
        // A copy with no node in the configuration names no source, so no line is invented.
        let unmapped = DivergenceEvidence::RootMismatch {
            copy: CopyId(1),
            base_seq: Seq(4),
            expected: Digest([1; 32]),
            found: None,
        };
        assert_eq!(quarantine_line(Generation(2), &nodes, &unmapped), None);
    }

    /// M7V-80's recorder slice, unit level. The window runs from the accepted fence to the
    /// window's close, not to the decision, when the two differ; a copy that failed its
    /// inventory and one that never answered are both unreachable with nothing reported; a
    /// member whose node has no boot gives no line at all, never an invented boot.
    #[test]
    fn quarantine_decision_measures_the_window_to_its_close_and_invents_nothing() {
        let digest = Digest([7; 32]);
        let member = |copy: u8, node: u32, role: ReplicaRole| Member {
            copy: CopyId(copy),
            node: NodeId(node),
            boot: BootId(1),
            role,
        };
        let members = [
            member(0, 1, ReplicaRole::Primary),
            member(1, 2, ReplicaRole::RegularSecondary),
            member(2, 3, ReplicaRole::RegularSecondary),
        ];
        let discovery = Discovery {
            fenced_epoch: OwnerEpoch(4),
            opened_at: Tick(3),
            closed_at: Some(Tick(10)),
            answers: BTreeMap::from([
                (CopyId(0), Some((Generation(1), Seq(2), digest))),
                (CopyId(1), None),
            ]),
        };
        let boot = |node: NodeId| Some(BootId(u64::from(node.0) + 10));
        let Some(TraceKind::RecoveryDecision {
            fenced_epoch,
            discovery_window_ticks,
            queried_sources,
            selected_source,
            selected_cutoff_seq,
            selected_digest,
            mode,
            loss_uncertainty,
            new_generation,
        }) = quarantine_decision(&discovery, Tick(12), &members, boot)
        else {
            panic!("a decision line")
        };
        assert_eq!(fenced_epoch, OwnerEpoch(4));
        assert_eq!(
            discovery_window_ticks, 7,
            "to the close at 10, not the decision at 12"
        );
        assert_eq!(mode, RecoveryMode::Quarantine);
        assert_eq!(
            (
                selected_source,
                selected_cutoff_seq,
                selected_digest,
                new_generation
            ),
            (None, None, None, None)
        );
        assert!(!loss_uncertainty);
        let reach: Vec<(NodeId, BootId, bool, Option<Seq>)> = queried_sources
            .iter()
            .map(|source| {
                (
                    source.node,
                    source.boot,
                    source.reachable,
                    source.reported_seq,
                )
            })
            .collect();
        assert_eq!(
            reach,
            vec![
                (NodeId(1), BootId(11), true, Some(Seq(2))),
                (NodeId(2), BootId(12), false, None),
                (NodeId(3), BootId(13), false, None),
            ]
        );
        assert_eq!(queried_sources[0].reported_digest, Some(digest));

        let still_open = Discovery {
            closed_at: None,
            ..discovery.clone()
        };
        let Some(TraceKind::RecoveryDecision {
            discovery_window_ticks,
            ..
        }) = quarantine_decision(&still_open, Tick(12), &members, boot)
        else {
            panic!("a decision line")
        };
        assert_eq!(discovery_window_ticks, 9, "unclosed: to the decision");

        let no_boot = |node: NodeId| (node != NodeId(3)).then_some(BootId(1));
        assert_eq!(
            quarantine_decision(&discovery, Tick(12), &members, no_boot),
            None
        );
    }
}
