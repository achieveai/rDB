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
//! | `Quarantine` | F1's `RecoveryEffect::Quarantine` | in the plan anchor's generation, sources by F1's plan placement, see [`quarantine_line`] |
//!
//! Two more lines are what the **environment** did rather than what a module answered, so the
//! dispatcher writes them where it carries the effect out, and the run loop drains them after
//! each delivery ([`crate::harness::dispatch::Dispatcher::take_lines`]):
//!
//! | Line | From | Notes |
//! |---|---|---|
//! | `BatchApply` | every `StoreEffect::Commit`, and every scenario preload | the batch's own `History` record's digests, see [`apply_line`] |
//! | `DurabilityAdvance` | every engine sync: a host flush, F1's `SyncWalThrough`, a durable preload | one per captured prefix, `Synced` only where the engine reported it durable, see [`durability_lines`] |
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
//! * `RecoveryDecision` on a quarantine (lead ruling V-R29). Its `selected_cutoff_seq`,
//!   `selected_digest` and `new_generation` are not optional, and on a quarantine F1 selects no
//!   position and creates no lineage. Any stand-in is a value F1 never chose, and INV-LIN's
//!   `cutoff_below_an_available_recorded_prefix` judges it. Held on a contract ask.

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
    CorrelationId, Generation, NodeId, PartitionId, ReplicaRole, RequestIdentity, Seq,
};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::recovery::{DivergenceEvidence, RecoveryEffect, RecoveryEvent};
use rdb_core::contracts::storage::{Batch, CapturedPrefix, DurablePrefix, StorageFault};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    AckEvidence, ApplyOutcome, AuthorityGate, AuthorityOutcome, ClientOutcome, DurabilityClass,
    EventRef, QuarantineReason, SyncOutcome, TraceKind,
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
    /// `(F1's node, partition) -> the anchor generation and each copy's node`, from the
    /// `RecoveryEvent::Plan` F1 answered: what a `Quarantine` line names its sources with.
    plans: BTreeMap<(NodeId, PartitionId), (Generation, BTreeMap<CopyId, NodeId>)>,
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
        if let EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(plan))) = &event.kind {
            if module == ModuleName::Recovery {
                let nodes = plan
                    .config
                    .members
                    .iter()
                    .map(|member| (member.copy, member.node))
                    .collect();
                self.plans.insert(
                    (event.node, event.partition),
                    (plan.anchor.lineage.generation, nodes),
                );
            }
        }
        for effect in effects {
            match &effect.kind {
                EffectKind::Kernel(KernelEffect::Recovery(RecoveryEffect::Quarantine(
                    evidence,
                ))) if module == ModuleName::Recovery => {
                    let line = self
                        .plans
                        .get(&(event.node, event.partition))
                        .and_then(|(generation, nodes)| {
                            quarantine_line(*generation, nodes, evidence)
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
/// * It succeeded and reported the prefix durable: `Synced`, at what it reported — a
///   `ShortFlush` reports less than was captured, and the line says the less.
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
                        (Seq(d.through.0), SyncOutcome::Synced)
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
}
