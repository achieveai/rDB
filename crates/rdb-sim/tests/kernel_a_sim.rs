//! Kernel-a sim rows of `docs/testing/test-plan-m7-kernel-a.md` that need the real runner: A1, T1,
//! P1, R1 and F1 wired by `rdb-sim`'s dispatcher, one scenario each.
//!
//! | Row | Claim |
//! |---|---|
//! | M7A-85 | a batch applied on the primary alone gets no success reply in 10 000 ticks; the one reply is P1's `UNKNOWN_OUTCOME` at the post-apply deadline |
//! | M7A-131 | authority lapses between request 11's publication and its delayed `Reply` check: the fence withholds the reply, the check is answered `Deny(Expired)`, nothing is replied, the status stays `Published`; the log holds the publish and no delivered outcome |
//! | M7A-136 | a retry during the recovery, on the same node or another, is replayed or refused and never re-executed: seq 5 is applied once per node and no retry's correlation reaches storage |
//! | M7A-117 | a retry in g+1 of a request g executed is answered from g's retained dedup, never executed again; a different node's fresh T1 admits nothing before its dedup seed lands, even with an open grant and a seed that never lands |
//! | M7A-135 | the recovery fold answers `RecoveredApplied` below `retained_through`, `Expired` once g is retired, `Unknown` for an entry trimmed in a live generation and for an uncertain map; a retry naming g is refused `GENERATION_CHANGED` whatever was trimmed or retired |
//! | M7A-139 | a fence between the dispatch answer and the batch completion, injected or A1's own, keeps the dispatched batch, keeps the fence's cause, and resolves the request `UNKNOWN_OUTCOME` |
//! | M7A-163 | a renewal whose completion is dropped ends in an expiry fence at the local horizon that drains T1's queue and P1's `Fresh` waiters in the same tick |
//! | M7A-194 | a failover node's P1 answers a previous generation's status `RecoveredApplied` from the durable dedup rows, with and without the generation, as the node that applied it does; inside the seed window the generation answers `Unknown`, never `StatusExpired`; a row that does not decode is skipped and the seed lands; a retired generation is never resurrected and a trim watermark is honoured (A-R90) |
//!
//! Fixtures come from the verification corpus, never a new route: the A1/P1 case
//! (`cases::case_a1_p1_new_generation_between_publish_and_reply`) without its unrunnable
//! activation op, so B (node 1) recovers gen 2 at `PLAN_AT`, L1 lifts its resume hold, and the
//! grammar's write (request 11) goes to B at `A1_P1_SUBMIT_AT`. What a row adds is a seed event,
//! a network cut, a control fault, or a recovery root carried out on B.
//!
//! Every assertion reads what the kernels hold (through `Dispatcher`'s accessors), the replies a
//! run returned, or the landed `TraceKind` lines the run recorded. Log fields are ids, ticks and
//! counts, never a key or a value.

mod support;

use std::collections::BTreeSet;

use bytes::Bytes;
use config_log::retcd_test;
use config_log::testing::{test_log_dir, test_run_id};
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::PartitionRecord;
use rdb_core::authority::AuthorityState;
use rdb_core::contracts::authority::{AuthorityEvent, Checkpoint, DenyReason, FenceScope, Lineage};
use rdb_core::contracts::control::{CasOutcome, ControlEvent, ControlKey, ReadOutcome};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::errors::{ErrorKind, RdbError};
use rdb_core::contracts::event::{
    Budgets, ClientEvent, Effect, EffectKind, EventKind, KernelEffect, KernelEvent, ModuleName,
    NodeLifecycle, ReplyEffect,
};
use rdb_core::contracts::ids::{
    AffinityId, AuthorityGeneration, BootId, ClientId, ControlRequestId, CorrelationId, DurableSeq,
    Generation, NodeId, OwnerEpoch, PartitionId, RequestId, RequestIdentity, Revision, Seq,
    SnapshotHandle, TenantId,
};
use rdb_core::contracts::publication::PublicationEffect;
use rdb_core::contracts::recovery::{DurableProof, RecoveryBarrier, RecoveryResult};
use rdb_core::contracts::storage::{Namespace, SnapshotRead, Write};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{
    AuthorityGate, AuthorityOutcome, ClientOutcome, ControlOutcomeKind, KernelNote,
    ReadServiceOutcome, TraceEvent, TraceKind,
};
use rdb_core::contracts::txn::{scoped_key, Mutation, Outcome, TxnRequest, TxnResult, TxnStatus};
use rdb_core::contracts::version::API_VERSION;
use rdb_core::publication::{
    FreezeCause, PubMode, PubStateView, StatusOutcome, POST_APPLY_DEADLINE_MILLIS,
};
use rdb_core::transaction::{Inflight, QueueMode, Retained, RetainedAnswer, TxnKernel};
use rdb_sim::harness::hop::HopDelay;
use rdb_sim::harness::run::{RunLimits, RunPlan, Runner, SeedEvent};
use rdb_sim::harness::trace::{log_jsonl_path, write_log_jsonl, LogTags};
use rdb_sim::sim::control::ControlOp;
use rdb_sim::sim::network::{LinkState, NetworkOp};
use support::scenarios::cases;
use support::scenarios::grammar::{ClientOp, RecoveryOp, ScenarioOp};
use support::scenarios::run::{self as scenario_run, BOOT};

/// The recovered primary of every fixture here.
const B: NodeId = cases::B_NODE;
const PART: PartitionId = PartitionId(1);
/// The grammar's write in the A1/P1 case.
const SUBMIT: u64 = cases::A1_P1_SUBMIT_AT;
/// Its request id, and the sequence it lands at: the preload fills 1..=10.
const WRITE: u64 = 11;
const HEAD: u64 = 10;

type Replies = Vec<(NodeId, ReplyEffect)>;

// ---- fixtures -------------------------------------------------------------------------------

fn identity(request: u64) -> RequestIdentity {
    RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(1),
        request: RequestId(request),
    }
}

/// A reader: another client, so its identities never meet a writer's.
fn reader(request: u64) -> RequestIdentity {
    RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(2),
        request: RequestId(request),
    }
}

/// The canonical history's request shape for `request`; `value == request` keeps its digest.
fn txn(request: u64, value: u64) -> TxnRequest {
    TxnRequest {
        api_version: API_VERSION,
        identity: identity(request),
        affinity: AffinityId(1),
        expected_generation: None,
        remaining_millis: 1_000,
        conditions: Vec::new(),
        mutations: vec![Mutation::Put {
            key: scoped_key(TenantId(1), AffinityId(1), b"k"),
            value: Bytes::copy_from_slice(&value.to_be_bytes()),
            expected_version: None,
        }],
    }
}

fn submit(request: TxnRequest) -> EventKind {
    EventKind::Client(ClientEvent::Submit(request))
}

fn status(request: u64, generation: Option<u64>) -> EventKind {
    EventKind::Client(ClientEvent::Status {
        identity: identity(request),
        generation: generation.map(Generation),
    })
}

fn read(request: u64) -> EventKind {
    EventKind::Client(ClientEvent::Read {
        identity: reader(request),
        key: scoped_key(TenantId(1), AffinityId(1), b"k"),
    })
}

fn on_b(at: u64, correlation: u64, kind: EventKind) -> SeedEvent {
    SeedEvent {
        at: Tick(at),
        node: B,
        boot: BOOT,
        partition: PART,
        correlation: CorrelationId(correlation),
        kind,
    }
}

fn queue(runner: &mut Runner, at: u64, correlation: u64, kind: EventKind) {
    runner.queue(&on_b(at, correlation, kind)).expect("queue");
}

/// The A1/P1 case without its activation op: recovers gen 2 on B, then B takes request 11.
fn a1p1_plan() -> RunPlan {
    let mut scenario = cases::case_a1_p1_new_generation_between_publish_and_reply();
    cases::without_activation(&mut scenario);
    scenario_run::lower(&scenario).expect("the A1/P1 case lowers")
}

/// Run to `to` inclusive and return the replies the segment produced.
fn run_to(runner: &mut Runner, to: u64) -> Replies {
    let report = runner
        .run(RunLimits {
            max_events: 50_000,
            deadline: Tick(to),
        })
        .expect("run");
    tracing::info!(
        to,
        replies = report.replies.len(),
        last_tick = report.last_tick.0,
        "segment"
    );
    report.replies
}

/// Pop one event at or before `deadline`. `false` when none is left.
fn step(runner: &mut Runner, deadline: u64, replies: &mut Replies) -> bool {
    let report = runner
        .run(RunLimits {
            max_events: 1,
            deadline: Tick(deadline),
        })
        .expect("step");
    replies.extend(report.replies);
    report.events_consumed == 1
}

/// B cut from both other copies: nothing it sends is received, no ACK comes back.
fn cut_b(runner: &mut Runner) {
    for other in [cases::C_NODE, cases::A_NODE] {
        runner
            .dispatcher_mut()
            .inject_network(NetworkOp::SetLink {
                a: B,
                b: other,
                state: LinkState::Partitioned,
            })
            .expect("cut");
    }
}

fn t1(runner: &Runner) -> &TxnKernel {
    runner
        .dispatcher()
        .transaction()
        .kernel(B, PART)
        .expect("T1 on B")
}

fn p1(runner: &Runner) -> PubStateView {
    runner
        .dispatcher()
        .publication()
        .view(B, PART)
        .expect("P1 on B")
}

/// `(completed)` of T1's inflight when it is `Dispatched`, else `None`.
fn dispatched(runner: &Runner) -> Option<(Seq, bool)> {
    match t1(runner).inflight() {
        Some(Inflight::Dispatched { seq, completed, .. }) => Some((*seq, *completed)),
        _ => None,
    }
}

fn replies_for(replies: &Replies, request: RequestIdentity) -> Vec<&ReplyEffect> {
    replies
        .iter()
        .map(|(_, reply)| reply)
        .filter(|reply| match reply {
            ReplyEffect::Transaction { identity, .. }
            | ReplyEffect::Status { identity, .. }
            | ReplyEffect::Failed { identity, .. }
            | ReplyEffect::Read { identity, .. } => *identity == request,
        })
        .collect()
}

fn unknown_outcome(request: u64) -> ReplyEffect {
    ReplyEffect::Failed {
        identity: identity(request),
        error: RdbError::UnknownOutcome {
            partition: PART,
            identity: identity(request),
        },
    }
}

/// Every `(node, generation, seq)` a `BatchApply` line names, with its correlation.
fn batch_applies(trace: &[TraceEvent]) -> Vec<(u64, NodeId, Generation, Seq)> {
    trace
        .iter()
        .filter_map(|e| match &e.kind {
            TraceKind::BatchApply {
                generation, seq, ..
            } => Some((e.correlation.0, e.node, *generation, *seq)),
            _ => None,
        })
        .collect()
}

/// `(tick, outcome)` of every `ClientOutcomeReported` for `request` under `correlation`, or
/// under any correlation when `None`.
fn outcomes(
    trace: &[TraceEvent],
    request: u64,
    correlation: Option<u64>,
) -> Vec<(u64, ClientOutcome)> {
    trace
        .iter()
        .filter_map(|e| match &e.kind {
            TraceKind::ClientOutcomeReported {
                request: r,
                outcome,
                ..
            } if r.0 == request && correlation.is_none_or(|c| c == e.correlation.0) => {
                Some((e.logical_tick, *outcome))
            }
            _ => None,
        })
        .collect()
}

// ---- M7A-85 ---------------------------------------------------------------------------------

/// M7A-85 (ADR 0004 no-ACK row; spec §5.2 steps 4–7). B applies request 11 at seq 11 and no copy
/// can ACK it: B is cut from C and A the tick before the write. For 10 000 ticks no reply carries
/// a result and `published` stays at 10; the only reply is P1's `UNKNOWN_OUTCOME` at the
/// post-apply deadline. The input clause "no `QualificationChanged` ever" is checked, not
/// assumed: no accepted ACK reaches seq 11, and P1's candidate never turns `qualifying`.
///
/// Positive control: the same fixture without the cut publishes request 11 and replies
/// `Success`, so the silence above is the cut's doing, not a fixture that never publishes.
#[retcd_test]
fn m7a_85_local_apply_no_ack_no_success_reply() {
    support::preamble();

    // Positive control.
    let mut control = Runner::new(&a1p1_plan()).expect("runner");
    let replies = run_to(&mut control, SUBMIT + 100);
    let published: Vec<_> = replies_for(&replies, identity(WRITE))
        .into_iter()
        .filter_map(|reply| match reply {
            ReplyEffect::Transaction { result, .. } => Some(*result),
            _ => None,
        })
        .collect();
    assert_eq!(
        published.len(),
        1,
        "control: uncut, request 11 publishes and replies Success once: {replies:?}"
    );
    assert_eq!(
        (
            published[0].generation,
            published[0].seq,
            published[0].outcome
        ),
        (Generation(2), Seq(WRITE), Outcome::Published),
        "control: the reply is the gen-2 publication at seq 11"
    );

    // The row.
    let mut runner = Runner::new(&a1p1_plan()).expect("runner");
    let before = run_to(&mut runner, SUBMIT - 1);
    assert!(
        replies_for(&before, identity(WRITE)).is_empty(),
        "nothing about request 11 before it is submitted"
    );
    assert_eq!(p1(&runner).published.seq, Seq(HEAD), "gen 2 starts at 10");
    cut_b(&mut runner);

    let deadline = Tick(SUBMIT).plus_millis(POST_APPLY_DEADLINE_MILLIS).0;
    let mut all = Replies::new();
    for to in [
        SUBMIT,
        SUBMIT + 1,
        SUBMIT + 10,
        SUBMIT + 1_000,
        deadline - 1,
        deadline,
        deadline + 1,
        SUBMIT + 5_000,
        SUBMIT + 10_000,
    ] {
        let replies = run_to(&mut runner, to);
        let view = p1(&runner);
        tracing::info!(
            to,
            replies = replies.len(),
            published = view.published.seq.0,
            pending = view.pending.is_some(),
            "M7A-85 checkpoint"
        );
        assert_eq!(
            view.published.seq,
            Seq(HEAD),
            "published_seq unchanged at {to}"
        );
        let pending = view.pending.expect("the applied candidate stays pending");
        assert_eq!(
            pending.request,
            identity(WRITE),
            "the pending candidate is request 11"
        );
        assert!(
            !pending.qualifying,
            "no qualification was ever held, at {to}"
        );
        let (seq, completed) = dispatched(&runner).expect("T1 keeps the dispatched batch");
        assert_eq!(seq, Seq(WRITE), "the batch is seq 11");
        assert!(
            completed,
            "BatchCompleted{{Ok}} arrived (local apply), at {to}"
        );
        if to == deadline {
            assert_eq!(
                replies,
                vec![(B, unknown_outcome(WRITE))],
                "the deadline's UNKNOWN_OUTCOME is the only reply, at {to}"
            );
        } else {
            assert!(replies.is_empty(), "no reply at {to}: {replies:?}");
        }
        all.extend(replies);
    }
    assert!(
        all.iter()
            .all(|(_, reply)| !matches!(reply, ReplyEffect::Transaction { .. })),
        "zero replies carrying a result"
    );

    let trace = runner.finish().expect("trace");
    let applies = batch_applies(&trace.events);
    let at_11: Vec<_> = applies
        .iter()
        .filter(|(_, _, generation, seq)| *generation == Generation(2) && *seq == Seq(WRITE))
        .collect();
    assert_eq!(
        at_11.iter().map(|(_, node, ..)| *node).collect::<Vec<_>>(),
        vec![B],
        "seq 11 is applied on B and nowhere else"
    );
    let acks_past_head = trace
        .events
        .iter()
        .filter(|e| {
            matches!(
                &e.kind,
                TraceKind::ReplicationAck { accepted: true, contiguous_seq, .. }
                    if *contiguous_seq >= Seq(WRITE)
            )
        })
        .count();
    assert_eq!(acks_past_head, 0, "no copy ever acknowledged seq 11");
    let publishes = trace
        .events
        .iter()
        .filter(|e| matches!(&e.kind, TraceKind::Publish { seq, .. } if *seq >= Seq(WRITE)))
        .count();
    assert_eq!(publishes, 0, "seq 11 is never published");
    assert_eq!(
        outcomes(&trace.events, WRITE, None),
        vec![(deadline, ClientOutcome::Error(ErrorKind::UnknownOutcome))],
        "one outcome for request 11, UNKNOWN_OUTCOME at the deadline tick"
    );
}

// ---- M7A-139 --------------------------------------------------------------------------------

/// The fence A1 sends when a node's grant lapses. Injected, because a real lapse cannot land
/// between the dispatch answer and the completion: A1 answers `Dispatch` only inside its
/// `dispatch_margin`, so the expiry fence of that same view is always later than the completion.
fn expiry_fence() -> EventKind {
    EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Fence {
        scope: FenceScope::Node,
        reason: DenyReason::Expired,
    }))
}

fn dispatch_decided(runner: &Runner, at: u64) -> bool {
    runner.recorded().iter().any(|e| {
        e.node == B
            && matches!(
                &e.kind,
                TraceKind::AuthorityDecision { gate: AuthorityGate::Dispatch, decision_tick, .. }
                    if *decision_tick == at
            )
    })
}

/// Where M7A-139's fence comes from.
#[derive(Debug, Clone, Copy)]
enum FenceSource {
    /// `Fence{Node, Expired}` straight to B's modules, queued behind A1's `Dispatch` answer.
    Injected,
    /// A1's own: `EpochRevocationPersisted` for the served epoch, queued while T1 waits on its
    /// dispatch check, so A1 answers `Admit` first and fences on the next event it takes.
    A1Revocation,
}

/// M7A-139 (K-A-33 companion, K-A-46). B's write is admitted and A1 answers its `Dispatch` check;
/// the batch goes to storage, and a fence reaches B's modules **before** the `BatchCompleted`.
/// T1 keeps the `Dispatched` inflight through the freeze; the completion then emits the candidate
/// (P1's `pending` appears only after it), advances `next_seq`, and leaves T1's mode exactly
/// `Frozen{AuthorityLost(reason), unresolved: Some(11)}`. The request resolves `UNKNOWN_OUTCOME`
/// at P1's deadline and never as a rejection; P1 stays `Frozen{AuthorityLost(reason)}`, and T1's
/// next `Submit` is refused `LEASE_EXPIRED`.
///
/// Two sub-cases, one fact apart: the fence's source.
///
/// - **Injected** `Fence{Node, Expired}`, the row's own input. A real lapse cannot land here
///   (see [`expiry_fence`]), and this fence leaves A1 itself `Held`.
/// - **A1-driven** (tester F7): a persisted revocation of the served epoch, so A1 fences the
///   partition itself, `EpochRevoked`. What is asserted is the same list, with that reason.
///
/// B is cut from the other copies so no ACK can publish seq 11 before the deadline; M7A-85 shows
/// that cut alone changes nothing but the missing ACK.
#[retcd_test]
fn m7a_139_freeze_keeps_dispatched_inflight_resolves_unknown() {
    support::preamble();
    freeze_while_dispatched(FenceSource::Injected);
    freeze_while_dispatched(FenceSource::A1Revocation);
}

fn freeze_while_dispatched(source: FenceSource) {
    let mut runner = Runner::new(&a1p1_plan()).expect("runner");
    let mut replies = run_to(&mut runner, SUBMIT - 1);
    cut_b(&mut runner);
    let next_before = t1(&runner).next_seq();
    assert_eq!(next_before, Seq(WRITE), "T1 would reserve 11 next");

    let reason = match source {
        FenceSource::Injected => {
            // Up to A1's `Dispatch` answer; the fence is queued behind it, in the same tick.
            while !dispatch_decided(&runner, SUBMIT) {
                assert!(
                    step(&mut runner, SUBMIT, &mut replies),
                    "the tick ran out before the answer"
                );
            }
            queue(&mut runner, SUBMIT, 9_501, expiry_fence());
            DenyReason::Expired
        }
        FenceSource::A1Revocation => {
            // Up to T1's dispatch check being in A1's queue; the revocation goes in behind it.
            while !matches!(
                t1(&runner).inflight(),
                Some(Inflight::AwaitingDispatchCheck { .. })
            ) {
                assert!(
                    step(&mut runner, SUBMIT, &mut replies),
                    "the tick ran out before T1 admitted request 11"
                );
            }
            let epoch = t1(&runner).lineage().owner_epoch;
            queue(
                &mut runner,
                SUBMIT,
                9_501,
                EventKind::Kernel(KernelEvent::Authority(
                    AuthorityEvent::EpochRevocationPersisted {
                        partition: PART,
                        epoch,
                    },
                )),
            );
            DenyReason::EpochRevoked
        }
    };
    tracing::info!(source = ?source, "M7A-139 fence queued");

    let expired = FreezeCause::AuthorityLost(reason);
    let mut frozen_before_completion = false;
    let mut completed_at = None;
    let mut i = 0;
    let mut seen_dispatched = false;
    while step(&mut runner, SUBMIT, &mut replies) {
        i += 1;
        let Some((seq, completed)) = dispatched(&runner) else {
            // Only the A1-driven case steps from before the answer.
            assert!(
                !seen_dispatched
                    && matches!(
                        t1(&runner).inflight(),
                        Some(Inflight::AwaitingDispatchCheck { .. })
                    ),
                "the Dispatched inflight is never dropped (step {i})"
            );
            continue;
        };
        seen_dispatched = true;
        assert_eq!(seq, Seq(WRITE));
        let mode = *t1(&runner).mode();
        let pending = p1(&runner).pending;
        tracing::info!(
            i,
            completed,
            frozen = mode != QueueMode::Open,
            pending = pending.is_some(),
            "M7A-139 step"
        );
        if !completed {
            assert!(
                pending.is_none(),
                "no candidate before BatchCompleted (step {i})"
            );
            if matches!(mode, QueueMode::Frozen { cause, .. } if cause == expired) {
                frozen_before_completion = true;
            }
        } else if completed_at.is_none() {
            completed_at = Some(i);
            assert!(
                frozen_before_completion,
                "the fence froze T1 while the batch was still in storage"
            );
            assert_eq!(
                mode,
                QueueMode::Frozen {
                    cause: expired,
                    unresolved: Some(Seq(WRITE)),
                },
                "after BatchCompleted the freeze's cause is kept and unresolved is seq 11 (K-A-46)"
            );
            assert_eq!(
                t1(&runner).next_seq(),
                Seq(WRITE + 1),
                "next_seq advanced past 11"
            );
        }
        if i > 40 {
            break;
        }
    }
    assert!(
        completed_at.is_some(),
        "BatchCompleted arrived in the submit tick"
    );
    let pending = p1(&runner)
        .pending
        .expect("the completion emitted the candidate");
    assert_eq!(
        pending.request,
        identity(WRITE),
        "Candidate{{seq 11}} is request 11's"
    );
    assert_eq!(
        p1(&runner).mode,
        PubMode::Frozen { cause: expired },
        "P1 took the same fence"
    );

    let deadline = Tick(SUBMIT).plus_millis(POST_APPLY_DEADLINE_MILLIS).0;
    replies.extend(run_to(&mut runner, deadline - 1));
    assert!(
        replies_for(&replies, identity(WRITE)).is_empty(),
        "no answer for request 11 before the deadline: {replies:?}"
    );
    replies.extend(run_to(&mut runner, deadline));
    assert_eq!(
        replies_for(&replies, identity(WRITE)),
        vec![&unknown_outcome(WRITE)],
        "request 11 resolves UNKNOWN_OUTCOME at P1's deadline"
    );
    assert_eq!(
        p1(&runner).mode,
        PubMode::Frozen { cause: expired },
        "after the deadline P1 is still Frozen{{AuthorityLost({reason:?})}}"
    );
    assert_eq!(
        *t1(&runner).mode(),
        QueueMode::Frozen {
            cause: expired,
            unresolved: Some(Seq(WRITE)),
        },
        "T1's cause survives the deadline too"
    );

    let grant = p1(&runner).authority.expect("P1 holds a view").grant_id;
    queue(&mut runner, deadline + 100, 9_502, submit(txn(12, 12)));
    replies.extend(run_to(&mut runner, deadline + 200));
    assert_eq!(
        replies_for(&replies, identity(12)),
        vec![&ReplyEffect::Failed {
            identity: identity(12),
            error: RdbError::LeaseExpired {
                partition: PART,
                grant,
            },
        }],
        "the kept cause picks LEASE_EXPIRED for the next Submit, not PROTECTION_PAUSED"
    );
    assert_eq!(
        replies_for(&replies, identity(WRITE)).len(),
        1,
        "exactly one reply for request 11, and it is not a rejection"
    );
    let trace = runner.finish().expect("trace");
    assert_eq!(
        outcomes(&trace.events, WRITE, None),
        vec![(deadline, ClientOutcome::Error(ErrorKind::UnknownOutcome))],
        "the trace agrees: one outcome for request 11, at the deadline"
    );
}

// ---- M7A-163 --------------------------------------------------------------------------------

/// M7A-163 (ADR 0008 "Dropped control operation", both consumers). The control completion B's
/// A1 is waiting on is dropped at `SUBMIT - 1600`, so the renewal dispatched after it never hears
/// `Committed`. T1 holds requests 12 and 13 queued behind request 11 (dispatched, unpublished:
/// B is cut from its copies), and P1 holds two `Fresh` reads waiting at the barrier. When the
/// last view's horizon passes, A1 fences `Node/Expired` at `valid_through_tick + 1` and pushes a
/// superseding view already past its horizon; in that same tick T1's queue drains
/// `LEASE_EXPIRED` and P1's waiters are answered.
///
/// **The waiters' code is `LEASE_EXPIRED`, not the row's `UNKNOWN_OUTCOME`.** Lead rulings A-R72
/// and A-R72a (P1's `deny_kind`): a refused read wrote nothing, so it has no outcome to be unknown
/// about, and its code comes from `DenyReason::client_error_kind`. The row's claim that they
/// drain *at the fence* is asserted unchanged.
#[retcd_test]
fn m7a_163_dropped_control_operation_drains_both_consumers() {
    support::preamble();
    let mut runner = Runner::new(&a1p1_plan()).expect("runner");
    let mut replies = run_to(&mut runner, SUBMIT - 1_600);
    runner
        .control_mut()
        .inject(ControlOp::DropCompletion { node: B })
        .expect("drop B's next control completion");
    let drop_index = runner.recorded().len();
    queue(&mut runner, SUBMIT, 9_601, submit(txn(12, 12)));
    queue(&mut runner, SUBMIT, 9_602, submit(txn(13, 13)));
    queue(&mut runner, SUBMIT + 10, 9_603, read(1));
    queue(&mut runner, SUBMIT + 10, 9_604, read(2));
    replies.extend(run_to(&mut runner, SUBMIT - 1));
    cut_b(&mut runner);

    // The last view before the fence: its horizon is where the fence must land.
    replies.extend(run_to(&mut runner, SUBMIT + 20));
    let last = p1(&runner).authority.expect("P1 holds A1's view");
    let fence_tick = last.valid_through_tick.0 + 1;
    tracing::info!(
        authority_seq = last.authority_seq,
        valid_through = last.valid_through_tick.0,
        fence_tick,
        "M7A-163 last live view"
    );
    let a1 = runner.dispatcher().authority(B).expect("A1 on B").view();
    let renewal = a1.renewal.expect("a renewal is outstanding and unanswered");
    assert!(
        matches!(a1.state, AuthorityState::Held(_)),
        "still held before the horizon"
    );

    replies.extend(run_to(&mut runner, fence_tick - 1));
    let view = runner.dispatcher().authority(B).expect("A1").view();
    assert!(
        matches!(view.state, AuthorityState::Held(_)),
        "held through {}",
        fence_tick - 1
    );
    assert_eq!(
        view.renewal,
        Some(renewal),
        "the same renewal is still waiting"
    );
    assert_eq!(t1(&runner).queue_len(), 2, "T1 holds requests 12 and 13");
    assert_eq!(
        p1(&runner).waiters,
        vec![reader(1), reader(2)],
        "P1 holds the two Fresh reads"
    );
    for id in [identity(12), identity(13), reader(1), reader(2)] {
        assert!(
            replies_for(&replies, id).is_empty(),
            "{id:?} unanswered before the fence"
        );
    }

    let at_fence = run_to(&mut runner, fence_tick);
    let a1 = runner.dispatcher().authority(B).expect("A1").view();
    assert_eq!(
        a1.state,
        AuthorityState::Fenced {
            reason: DenyReason::Expired,
            at: Tick(fence_tick),
        },
        "Fence{{Node, Expired}} at the local horizon"
    );
    let superseding = p1(&runner).authority.expect("a view after the fence");
    assert!(
        superseding.authority_seq > last.authority_seq,
        "the fence pushed a superseding view"
    );
    assert_eq!(
        (superseding.valid_through_tick, superseding.past_horizon),
        (Tick(fence_tick - 1), DenyReason::Expired),
        "already past its horizon: valid_through == fence_tick - 1, past_horizon Expired"
    );
    let grant = superseding.grant_id;
    let lease_expired = |request| ReplyEffect::Failed {
        identity: identity(request),
        error: RdbError::LeaseExpired {
            partition: PART,
            grant,
        },
    };
    let refused = |who| ReplyEffect::Read {
        identity: who,
        outcome: ReadServiceOutcome::Rejected(ErrorKind::LeaseExpired),
        value: None,
    };
    let mut got: Vec<_> = at_fence.iter().map(|(_, reply)| reply.clone()).collect();
    got.sort_by_key(|reply| format!("{reply:?}"));
    let mut want = vec![
        lease_expired(12),
        lease_expired(13),
        refused(reader(1)),
        refused(reader(2)),
    ];
    want.sort_by_key(|reply| format!("{reply:?}"));
    assert_eq!(
        got, want,
        "both consumers drain in the fence tick, and nothing else replies"
    );
    assert_eq!(t1(&runner).queue_len(), 0, "T1's queue is empty");
    assert!(p1(&runner).waiters.is_empty(), "P1 holds no waiter");

    replies.extend(at_fence);
    replies.extend(run_to(&mut runner, SUBMIT + 2_500));
    let committed_after_drop = runner.recorded()[drop_index..]
        .iter()
        .filter(|e| {
            e.node == B
                && matches!(
                    &e.kind,
                    TraceKind::ControlInteraction {
                        outcome: ControlOutcomeKind::Committed,
                        ..
                    }
                )
        })
        .count();
    assert_eq!(
        committed_after_drop, 0,
        "zero Committed ever reaches B after the drop"
    );
    let trace = runner.finish().expect("trace");
    for request in [12, 13] {
        assert_eq!(
            outcomes(&trace.events, request, None),
            vec![(fence_tick, ClientOutcome::Error(ErrorKind::LeaseExpired))],
            "request {request} is refused once, at the fence tick"
        );
    }
}

// ---- M7A-117 and M7A-135: a same-node recovery of gen 2 into gen 3 --------------------------

/// Request 4 lands at seq 4 (the grammar's write) and request 5 at seq 5.
const EARLIER: u64 = 4;
const WRITTEN: u64 = 5;

/// The A1/P1 case with gen 2 recovered at head 3, so gen 2's own writes are seq 4 (request 4,
/// the grammar's) and seq 5 (request 5, a seed ten ticks later, same shape, own id). Returns the
/// plan and request 5.
fn same_node_plan() -> (RunPlan, TxnRequest) {
    let mut scenario = cases::case_a1_p1_new_generation_between_publish_and_reply();
    cases::without_activation(&mut scenario);
    for op in &mut scenario.ops {
        match op {
            ScenarioOp::Recovery(RecoveryOp::Synchronize { to, .. }) => *to = Seq(EARLIER - 1),
            ScenarioOp::Client(ClientOp::Submit { request, .. }) => *request = RequestId(EARLIER),
            _ => {}
        }
    }
    let mut plan = scenario_run::lower(&scenario).expect("lowers");
    let (at, mut written) = plan
        .seed
        .iter()
        .find_map(|s| match &s.kind {
            EventKind::Client(ClientEvent::Submit(req)) => Some((s.at.0, req.clone())),
            _ => None,
        })
        .expect("the grammar's write");
    written.identity = identity(WRITTEN);
    plan.seed
        .push(on_b(at + 10, 9_399, submit(written.clone())));
    (plan, written)
}

/// The F1 root that committed gen 2.
fn gen2_root(runner: &Runner) -> Box<RecoveryResult> {
    runner
        .recorded()
        .iter()
        .rev()
        .find_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredFact { result },
                ..
            } if result.new_generation == Generation(2) => Some(result.clone()),
            _ => None,
        })
        .expect("F1 committed gen 2")
}

/// Carry out an F1 root into gen 3 on B at its head (seq 5), after rewriting `partitions/1` to
/// name gen 3: `RetainedStatusMap{retained_through: 5, discarded_from: None, uncertain}`.
fn recover_gen3(runner: &mut Runner, uncertain: bool) {
    let kernel = t1(runner);
    let digest = kernel.prev_digest();
    let cutoff = Seq(kernel.next_seq().0 - 1);
    assert_eq!(cutoff, Seq(WRITTEN), "gen 2 holds seq 4 and 5");
    recover_gen3_at(runner, (cutoff, digest), None, uncertain);
}

/// Carry out an F1 root into gen 3 on B cut at `cutoff` with that position's digest, after
/// rewriting `partitions/1` to name gen 3:
/// `RetainedStatusMap{retained_through: cutoff, discarded_from, uncertain}`.
fn recover_gen3_at(
    runner: &mut Runner,
    (cutoff, digest): (Seq, Digest),
    discarded_from: Option<Seq>,
    uncertain: bool,
) {
    let root = gen2_root(runner);
    let (revision, record) = match runner.control_mut().get(ControlKey::Partition(PART)) {
        ReadOutcome::Found { revision, value } => {
            (revision, PartitionRecord::decode(&value).expect("record"))
        }
        other => panic!("partitions/1: {other:?}"),
    };
    let mut next = record;
    next.generation = Generation(3);
    next.owner_epoch = OwnerEpoch(record.owner_epoch.0 + 1);
    let lineage3 = Lineage {
        partition: PART,
        generation: Generation(3),
        owner_epoch: next.owner_epoch,
    };
    let rewritten = runner.control_mut().scenario_cas(
        ControlKey::Partition(PART),
        Some(revision),
        Some(next.encode()),
    );
    assert!(
        matches!(rewritten, CasOutcome::Committed(_)),
        "partitions/1 names gen 3: {rewritten:?}"
    );
    let copy = root
        .committed
        .pinned_config
        .members
        .iter()
        .find(|m| m.node == B)
        .expect("B is a member")
        .copy;
    let mut later = root.clone();
    later.fenced_prior.prior_generation = Generation(2);
    later.fenced_prior.prior_owner_epoch = root.committed.authority_view.lineage.owner_epoch;
    later.new_generation = Generation(3);
    later.selected.root = lineage3;
    later.selected.cutoff_seq = cutoff;
    later.selected.cutoff_digest = digest;
    later.selected.source = copy;
    let proof = DurableProof {
        copy,
        partition: PART,
        seq: DurableSeq(cutoff.0),
        digest,
    };
    later.barrier = RecoveryBarrier::try_new(&[proof], &BTreeSet::from([copy]), cutoff, digest)
        .expect("barrier");
    later.loss.cutoff_seq = cutoff;
    later.loss.highest_advertised_seq = cutoff;
    later.loss.uncertain = uncertain;
    later.committed.revision = Revision(root.committed.revision.0 + 1);
    later.committed.authority_view.lineage = lineage3;
    later.retained_status_map.predecessor_generation = Generation(2);
    later.retained_status_map.predecessor_cutoff = cutoff;
    later.retained_status_map.retained_through = cutoff;
    later.retained_status_map.discarded_from = discarded_from;
    later.retained_status_map.uncertain = uncertain;
    let now = runner.dispatcher().clock().now();
    for member in &later.committed.pinned_config.members {
        if member.node != B {
            runner
                .queue(&SeedEvent {
                    at: now,
                    node: member.node,
                    boot: BOOT,
                    partition: PART,
                    correlation: CorrelationId(9_300),
                    kind: EventKind::Kernel(KernelEvent::Recovered(later.clone())),
                })
                .expect("the root reaches the secondaries");
        }
    }
    runner
        .carry_out(
            B,
            BOOT,
            vec![Effect {
                correlation: CorrelationId(9_300),
                from: ModuleName::Recovery,
                partition: PART,
                kind: EffectKind::Kernel(KernelEffect::Recovered(later)),
            }],
        )
        .expect("the gen-3 root on B");
    let _ = runner.dispatcher_mut().take_notes();
}

/// Gen 2's published result for `seq`, as P1 recorded it, with `outcome` in place.
fn gen2_result(seq: u64, outcome: Outcome, published: &TxnResult) -> TxnResult {
    TxnResult {
        seq: Seq(seq),
        outcome,
        ..*published
    }
}

/// Gen 2 commits request 5 at seq 5; returns the runner just after, with request 5's
/// publication (from its own `Success` reply) and request 5.
fn gen2_committed() -> (Runner, TxnResult, TxnRequest) {
    let (plan, written) = same_node_plan();
    let mut runner = Runner::new(&plan).expect("runner");
    let replies = run_to(&mut runner, SUBMIT + 100);
    let result = replies_for(&replies, identity(WRITTEN))
        .into_iter()
        .find_map(|reply| match reply {
            ReplyEffect::Transaction { result, .. } => Some(*result),
            _ => None,
        })
        .expect("request 5 publishes in gen 2");
    assert_eq!(
        (result.generation, result.seq, result.outcome),
        (Generation(2), Seq(WRITTEN), Outcome::Published),
        "request 5 is gen 2's seq 5"
    );
    (runner, result, written)
}

/// `request` carrying `expected_generation = generation`.
fn expecting(mut request: TxnRequest, generation: u64) -> TxnRequest {
    request.expected_generation = Some(Generation(generation));
    request
}

/// Gen 2's request `request` retried in gen 3: `GENERATION_CHANGED{2, 3}`.
fn generation_changed(request: u64) -> ReplyEffect {
    ReplyEffect::Failed {
        identity: identity(request),
        error: RdbError::GenerationChanged {
            expected: Generation(2),
            current: Generation(3),
        },
    }
}

/// The `Status` answers among `replies` for request `request`.
fn status_reply(replies: &Replies, request: u64) -> Vec<TxnStatus> {
    replies_for(replies, identity(request))
        .into_iter()
        .filter_map(|reply| match reply {
            ReplyEffect::Status { status, .. } => Some(*status),
            _ => None,
        })
        .collect()
}

/// When gen 3 on B admits: past its resume hold. Found by the scenario (S-117b), where a
/// `Submit` at this tick was admitted and published in gen 3.
const GEN3_ADMITS: u64 = SUBMIT + 110 + 9_000;

/// M7A-117 (ADR 0004 generation reconciliation; A-R68 Q1/Q12). Two sub-cases.
///
/// **Same node.** Gen 2 on B commits request 5 at seq 5; an F1 root carries B into gen 3. P1's
/// `Status` for request 5 answers `RecoveredApplied`. Once gen 3 admits, a retry with the same
/// digest gets `GENERATION_CHANGED{expected: 2, current: 3}` and one with a different digest
/// `REQUEST_ID_REUSE`; neither reserves a sequence nor emits a `StorageBatch`, and a fresh
/// request then takes exactly the sequence they did not.
///
/// **Different node.** Gen 2's primary B runs a fresh T1 that holds none of gen 1's dedup in
/// memory; it is seeded from the durable entries at or below `retained_through` (10). L1's edge
/// is forced open during the seed, so what refuses the retry there is T1's own seed gate
/// (`PROTECTION_PAUSED`) and nothing is executed. After the seed lands, gen 1's request 5 retried
/// under its digest gets `GENERATION_CHANGED{1, 2}`, request 6 under another digest
/// `REQUEST_ID_REUSE`, again with no sequence and no batch.
///
/// In that window no A1 view admits yet, so with the seed gate removed the early retry is still
/// refused, `LEASE_EXPIRED` instead: there the row pins the gate by its code. The sub-case where
/// the gate is the only barrier, with an open grant and a seed that never lands, is
/// [`seed_blocked_for_ever_executes_nothing`]; it asserts zero gen-2 batches.
#[retcd_test]
fn m7a_117_generation_reconciliation_folds_previous_generation() {
    support::preamble();

    // Same node.
    let (mut runner, published, written) = gen2_committed();
    recover_gen3(&mut runner, false);
    let t = SUBMIT + 110;
    queue(&mut runner, t, 9_402, status(WRITTEN, Some(2)));
    let replies = run_to(&mut runner, t + 5);
    assert_eq!(
        status_reply(&replies, WRITTEN),
        vec![TxnStatus::Resolved(gen2_result(
            WRITTEN,
            Outcome::RecoveredApplied,
            &published
        ))],
        "P1's Status answer for request 5 in gen 3 is RecoveredApplied"
    );
    run_to(&mut runner, GEN3_ADMITS - 1);
    let next = t1(&runner).next_seq();
    assert_eq!(next, Seq(WRITTEN + 1), "gen 3 continues after seq 5");
    let mut changed = written.clone();
    changed.mutations = vec![Mutation::Put {
        key: scoped_key(TenantId(1), AffinityId(1), b"other"),
        value: Bytes::from_static(b"x"),
        expected_version: None,
    }];
    queue(&mut runner, GEN3_ADMITS, 9_410, submit(written.clone()));
    queue(&mut runner, GEN3_ADMITS + 1, 9_411, submit(changed));
    let replies = run_to(&mut runner, GEN3_ADMITS + 50);
    assert_eq!(
        replies_for(&replies, identity(WRITTEN)),
        vec![
            &ReplyEffect::Failed {
                identity: identity(WRITTEN),
                error: RdbError::GenerationChanged {
                    expected: Generation(2),
                    current: Generation(3),
                },
            },
            &ReplyEffect::Failed {
                identity: identity(WRITTEN),
                error: RdbError::RequestIdReuse {
                    identity: identity(WRITTEN),
                },
            },
        ],
        "same digest: GENERATION_CHANGED{{2, 3}}; different digest: REQUEST_ID_REUSE"
    );
    assert_eq!(
        t1(&runner).next_seq(),
        next,
        "neither retry reserved a sequence"
    );
    queue(&mut runner, GEN3_ADMITS + 60, 9_412, submit(txn(40, 40)));
    let replies = run_to(&mut runner, GEN3_ADMITS + 110);
    assert!(
        matches!(
            replies_for(&replies, identity(40)).as_slice(),
            [ReplyEffect::Transaction { result, .. }]
                if result.generation == Generation(3) && result.seq == next
        ),
        "control: gen 3 admits, and a fresh write takes the sequence the retries did not: {replies:?}"
    );
    let trace = runner.finish().expect("trace");
    let applies = batch_applies(&trace.events);
    assert!(
        applies
            .iter()
            .all(|(correlation, ..)| *correlation != 9_410 && *correlation != 9_411),
        "no StorageBatch for either retry"
    );
    assert_eq!(
        applies
            .iter()
            .filter(|(_, node, generation, _)| *node == B && *generation == Generation(3))
            .count(),
        1,
        "gen 3 applied one batch on B, the fresh write's"
    );

    seed_blocked_for_ever_executes_nothing();

    // Different node.
    let mut runner = Runner::new(&a1p1_plan()).expect("runner");
    // Inside the seed window (S-117c: pending from PLAN_AT + 2 001 to PLAN_AT + 2 500).
    let seed_open = cases::PLAN_AT + 2_097;
    run_to(&mut runner, seed_open);
    let kernel = t1(&runner);
    let seed = kernel
        .seed_pending()
        .expect("B's gen-2 T1 is waiting for its dedup seed");
    assert_eq!(
        (seed.serving, seed.retained_through),
        (Generation(2), Seq(HEAD)),
        "the seed covers gen 1's durable entries through retained_through 10"
    );
    assert_eq!(kernel.dedup().len(), 0, "nothing of gen 1 is in memory yet");
    let mut open = kernel.admission().cloned().expect("an L1 edge");
    open.allow = true;
    open.reason = None;
    queue(
        &mut runner,
        seed_open + 1,
        9_501,
        EventKind::Kernel(KernelEvent::SetAdmission(open)),
    );
    queue(
        &mut runner,
        seed_open + 2,
        9_502,
        submit(txn(WRITTEN, WRITTEN)),
    );
    let replies = run_to(&mut runner, seed_open + 10);
    assert!(
        t1(&runner).seed_pending().is_some(),
        "the seed has not landed at {}",
        seed_open + 10
    );
    assert!(
        matches!(
            replies_for(&replies, identity(WRITTEN)).as_slice(),
            [ReplyEffect::Failed { error: RdbError::ProtectionPaused { partition, .. }, .. }]
                if *partition == PART
        ),
        "admission open, seed pending: T1's own gate refuses the retry: {replies:?}"
    );
    assert_eq!(
        t1(&runner).next_seq(),
        Seq(HEAD + 1),
        "nothing reserved before the seed"
    );

    let after_seed = seed_open + 503;
    run_to(&mut runner, after_seed - 1);
    let kernel = t1(&runner);
    assert!(kernel.seed_pending().is_none(), "the seed landed");
    assert_eq!(
        kernel.dedup().len(),
        HEAD as usize,
        "gen 1's ten entries are seeded"
    );
    queue(
        &mut runner,
        after_seed,
        9_504,
        submit(txn(WRITTEN, WRITTEN)),
    );
    queue(&mut runner, after_seed + 1, 9_505, submit(txn(6, 999)));
    let replies = run_to(&mut runner, after_seed + 100);
    assert_eq!(
        replies_for(&replies, identity(WRITTEN)),
        vec![&ReplyEffect::Failed {
            identity: identity(WRITTEN),
            error: RdbError::GenerationChanged {
                expected: Generation(1),
                current: Generation(2),
            },
        }],
        "gen 1's request 5, same digest: GENERATION_CHANGED{{1, 2}}"
    );
    assert_eq!(
        replies_for(&replies, identity(6)),
        vec![&ReplyEffect::Failed {
            identity: identity(6),
            error: RdbError::RequestIdReuse {
                identity: identity(6),
            },
        }],
        "gen 1's request 6, another digest: REQUEST_ID_REUSE"
    );
    assert_eq!(
        t1(&runner).next_seq(),
        Seq(HEAD + 1),
        "no sequence reserved"
    );
    let trace = runner.finish().expect("trace");
    assert!(
        batch_applies(&trace.events)
            .iter()
            .all(|(correlation, ..)| ![9_502, 9_504, 9_505].contains(correlation)),
        "no StorageBatch for any retry"
    );
}

/// M7A-117, different node, with the seed gate as the only barrier (tester S-117g, lead F6). B's
/// preloaded seq-10 batch also carries one `Dedup` row that does not decode, so its gen-2 dedup
/// seed fails closed and stays pending for ever, while its grant stays open. Every `Submit` —
/// the grammar's request 11, gen 1's request 5 under its own digest, and fresh ids — is refused
/// `PROTECTION_PAUSED{paused_after: 10}`, `next_seq` stays 11, and gen 2 applies no batch above
/// the preloaded history. The refusal code alone proves the grant admitted: check 6 (the horizon)
/// runs before the seed gate at check 7. Without the gate, gen 1's request 5 is executed a second
/// time at gen 2 seq 12, which is what this sub-case exists to catch.
fn seed_blocked_for_ever_executes_nothing() {
    let mut plan = a1p1_plan();
    let mut planted = 0;
    for (node, batch) in &mut plan.preloads {
        if *node == B && batch.seq == Seq(HEAD) {
            batch.writes.push(Write {
                ns: Namespace::Dedup,
                key: Bytes::from_static(&[1, 2, 3]),
                value: Some(Bytes::from_static(&[4, 5, 6])),
            });
            planted += 1;
        }
    }
    assert_eq!(planted, 1, "one malformed Dedup row in B's seq-10 batch");
    let mut runner = Runner::new(&plan).expect("runner");
    let mut replies = run_to(&mut runner, SUBMIT + 100);
    let submits: [(u64, u64, TxnRequest); 4] = [
        (SUBMIT + 200, 9_800, txn(WRITTEN, WRITTEN)),
        (SUBMIT + 201, 9_801, txn(50, 7)),
        (SUBMIT + 1_000, 9_802, txn(WRITTEN, WRITTEN)),
        (SUBMIT + 1_001, 9_803, txn(52, 7)),
    ];
    for (at, correlation, request) in &submits {
        queue(&mut runner, *at, *correlation, submit(request.clone()));
    }
    replies.extend(run_to(&mut runner, SUBMIT + 1_100));
    let seed_pending = t1(&runner).seed_pending().is_some();
    let next = t1(&runner).next_seq();
    let trace = runner.finish().expect("trace");
    let executed: Vec<_> = batch_applies(&trace.events)
        .into_iter()
        .filter(|(_, _, generation, seq)| *generation == Generation(2) && seq.0 > HEAD)
        .collect();
    assert_eq!(
        executed,
        Vec::new(),
        "zero gen-2 batches while the seed is pending (none for gen 1's request 5)"
    );
    assert!(seed_pending, "the malformed row keeps the seed pending");
    assert_eq!(next, Seq(HEAD + 1), "no sequence reserved");
    let paused = |request: u64| ReplyEffect::Failed {
        identity: identity(request),
        error: RdbError::ProtectionPaused {
            partition: PART,
            paused_after: Seq(HEAD),
        },
    };
    assert_eq!(
        replies_for(&replies, identity(WRITE)),
        vec![&paused(WRITE)],
        "the grammar's request 11 is refused by the seed gate"
    );
    assert_eq!(
        replies_for(&replies, identity(WRITTEN)),
        vec![&paused(WRITTEN), &paused(WRITTEN)],
        "gen 1's request 5, twice, refused by the seed gate"
    );
    for fresh in [50, 52] {
        assert_eq!(
            replies_for(&replies, identity(fresh)),
            vec![&paused(fresh)],
            "fresh request {fresh} refused by the seed gate"
        );
    }
}

/// M7A-135 (K-A-52; ADR 0004 retention boundary). Gen 2 commits request 4 at seq 4 and request 5
/// at seq 5; an F1 root carries B into gen 3 with `RetainedStatusMap{retained_through: 5,
/// discarded_from: None}`.
///
/// - **Certain map.** The first retry's answer is `RecoveredApplied{result}`, gen 2's result with
///   only the outcome changed. After `RetireGeneration{2}` the answer is `Expired` asked with the
///   generation, not `Unknown`, and `Unknown` asked without it (tester F8). A `Submit` retry of
///   request 5 carrying `expected_generation = 2` is then refused `GENERATION_CHANGED{2, 3}` at
///   check 5, with no sequence and no batch.
/// - **`DedupTrim{g, below: 5}` + `StatusTrim{g, below: 5}` in a live (not retired)
///   generation** — T1 takes the first and P1 the second (`route.rs`), and P1's `Status` answer
///   comes from `StatusTrim` alone. Both are exclusive, so they drop seq < 5 and keep seq 5: the
///   trimmed identity's `Status` ⇒ `Unknown`; seq 5's entry is still
///   retained, so its same-digest `Submit` retry ⇒ `GENERATION_CHANGED{g, g+1}`, never a second
///   execution of seq 5's mutation. Past retention (a trim or retire that covers the identity) a
///   retry **without** `expected_generation` is admitted as a new request — M7A-89's rule (A-R68
///   Q2, spec §5.3), the raw-wire limit of ADR 0004 §8; one **with** `expected_generation = g` is
///   refused `GENERATION_CHANGED` at check 5 whatever was trimmed. Asserted here by trimming
///   through seq 5 (both trims, `below: 6`) afterwards: the `Some(2)` twin is refused with no batch, and the
///   `None` retry is published at gen 3 as a new request.
/// - **Uncertain map** (one fact changed): the first retry answers `Unknown` instead.
#[retcd_test]
fn m7a_135_f1_t1_p1_retention_boundary_recovered_applied_then_unknown() {
    support::preamble();
    let t = SUBMIT + 110;

    // Certain.
    let (mut runner, published, written) = gen2_committed();
    queue(&mut runner, SUBMIT + 101, 9_401, status(WRITTEN, Some(2)));
    let replies = run_to(&mut runner, SUBMIT + 102);
    assert_eq!(
        status_reply(&replies, WRITTEN),
        vec![TxnStatus::Resolved(published)],
        "before the recovery request 5 is gen 2's publication"
    );
    recover_gen3(&mut runner, false);
    queue(&mut runner, t, 9_402, status(WRITTEN, Some(2)));
    queue(&mut runner, t + 1, 9_403, status(WRITTEN, None));
    let replies = run_to(&mut runner, t + 5);
    let recovered =
        TxnStatus::Resolved(gen2_result(WRITTEN, Outcome::RecoveredApplied, &published));
    assert_eq!(
        status_reply(&replies, WRITTEN),
        vec![recovered, recovered],
        "RecoveredApplied{{result}}, asked with and without the generation"
    );
    queue(
        &mut runner,
        t + 100,
        9_420,
        EventKind::Kernel(KernelEvent::RetireGeneration {
            generation: Generation(2),
        }),
    );
    queue(&mut runner, t + 101, 9_421, status(WRITTEN, Some(2)));
    let replies = run_to(&mut runner, t + 110);
    assert_eq!(
        status_reply(&replies, WRITTEN),
        vec![TxnStatus::Expired],
        "after RetireGeneration{{2}} the answer is StatusExpired, not Unknown"
    );
    queue(&mut runner, t + 111, 9_422, status(WRITTEN, None));
    let replies = run_to(&mut runner, t + 120);
    assert_eq!(
        status_reply(&replies, WRITTEN),
        vec![TxnStatus::Unknown],
        "after RetireGeneration{{2}}, asked without the generation: Unknown"
    );
    run_to(&mut runner, GEN3_ADMITS - 1);
    let next = t1(&runner).next_seq();
    queue(
        &mut runner,
        GEN3_ADMITS,
        9_423,
        submit(expecting(written.clone(), 2)),
    );
    let replies = run_to(&mut runner, GEN3_ADMITS + 50);
    assert_eq!(
        replies_for(&replies, identity(WRITTEN)),
        vec![&generation_changed(WRITTEN)],
        "after the retire, the Some(2) retry is refused at check 5"
    );
    assert_eq!(t1(&runner).next_seq(), next, "no sequence reserved");
    let trace = runner.finish().expect("trace");
    assert!(
        batch_applies(&trace.events)
            .iter()
            .all(|(correlation, ..)| *correlation != 9_423),
        "zero StorageBatch for the Some(2) retry after the retire"
    );

    // DedupTrim below 5, generation live.
    let (mut runner, published, written) = gen2_committed();
    recover_gen3(&mut runner, false);
    queue(&mut runner, t, 9_402, status(EARLIER, Some(2)));
    let replies = run_to(&mut runner, t + 5);
    assert_eq!(
        status_reply(&replies, EARLIER),
        vec![TxnStatus::Resolved(gen2_result(
            EARLIER,
            Outcome::RecoveredApplied,
            &published
        ))],
        "request 4 is RecoveredApplied before the trim"
    );
    for kind in [
        KernelEvent::DedupTrim {
            generation: Generation(2),
            below: Seq(WRITTEN),
        },
        KernelEvent::StatusTrim {
            generation: Generation(2),
            below: Seq(WRITTEN),
        },
    ] {
        queue(&mut runner, t + 20, 9_404, EventKind::Kernel(kind));
    }
    queue(&mut runner, t + 21, 9_406, status(EARLIER, Some(2)));
    queue(&mut runner, t + 21, 9_407, status(WRITTEN, Some(2)));
    let replies = run_to(&mut runner, t + 30);
    assert_eq!(
        status_reply(&replies, EARLIER),
        vec![TxnStatus::Unknown],
        "the trimmed entry answers Unknown (the generation is live), not StatusExpired"
    );
    assert_eq!(
        status_reply(&replies, WRITTEN),
        vec![TxnStatus::Resolved(gen2_result(
            WRITTEN,
            Outcome::RecoveredApplied,
            &published
        ))],
        "seq 5 is above the trim and still RecoveredApplied"
    );
    let next = t1(&runner).next_seq();
    queue(&mut runner, GEN3_ADMITS, 9_410, submit(written.clone()));
    let replies = run_to(&mut runner, GEN3_ADMITS + 50);
    assert_eq!(
        replies_for(&replies, identity(WRITTEN)),
        vec![&generation_changed(WRITTEN)],
        "request 5's retry (no expected generation) after the trim is reconciled, not executed"
    );
    assert_eq!(t1(&runner).next_seq(), next, "no sequence reserved");

    // Past retention: a trim through seq 5 (`below: 6`) covers request 5 too.
    for kind in [
        KernelEvent::DedupTrim {
            generation: Generation(2),
            below: Seq(WRITTEN + 1),
        },
        KernelEvent::StatusTrim {
            generation: Generation(2),
            below: Seq(WRITTEN + 1),
        },
    ] {
        queue(
            &mut runner,
            GEN3_ADMITS + 60,
            9_429,
            EventKind::Kernel(kind),
        );
    }
    queue(
        &mut runner,
        GEN3_ADMITS + 61,
        9_430,
        submit(expecting(written.clone(), 2)),
    );
    let replies = run_to(&mut runner, GEN3_ADMITS + 110);
    assert_eq!(
        replies_for(&replies, identity(WRITTEN)),
        vec![&generation_changed(WRITTEN)],
        "trimmed through seq 5, the Some(2) retry is still refused at check 5"
    );
    assert_eq!(t1(&runner).next_seq(), next, "no sequence reserved");
    queue(&mut runner, GEN3_ADMITS + 120, 9_431, submit(written));
    let replies = run_to(&mut runner, GEN3_ADMITS + 170);
    assert!(
        matches!(
            replies_for(&replies, identity(WRITTEN)).as_slice(),
            [ReplyEffect::Transaction { result, .. }]
                if result.generation == Generation(3) && result.seq == next
        ),
        "past retention the retry without expected_generation is a new request (M7A-89): {replies:?}"
    );
    let trace = runner.finish().expect("trace");
    assert!(
        batch_applies(&trace.events)
            .iter()
            .all(|(correlation, ..)| *correlation != 9_410 && *correlation != 9_430),
        "never a second execution of seq 5's mutation inside retention, nor for the Some(2) twin"
    );
    let successes = |correlation: Option<u64>| {
        outcomes(&trace.events, WRITTEN, correlation)
            .iter()
            .filter(|(_, outcome)| *outcome == ClientOutcome::Success)
            .count()
    };
    assert_eq!(
        (successes(None), successes(Some(9_431))),
        (2, 1),
        "request 5 succeeded in gen 2, and again only as the new request past retention"
    );

    // Uncertain.
    let (mut runner, _, _) = gen2_committed();
    recover_gen3(&mut runner, true);
    queue(&mut runner, t, 9_402, status(WRITTEN, Some(2)));
    let replies = run_to(&mut runner, t + 5);
    assert_eq!(
        status_reply(&replies, WRITTEN),
        vec![TxnStatus::Unknown],
        "uncertain: the first retry answers Unknown instead of RecoveredApplied"
    );
}

// ---- M7A-194 --------------------------------------------------------------------------------

/// M7A-194 (spec §8.1 "Lineage rules", plan §8.9). Gen 1 applied requests 1..=10 on another
/// node; B recovers gen 2 with the durable rows and its T1 seeds gen 1's dedup from them
/// (M7A-117). P1's status index is seeded from the same rows at the same point, so `Status` for
/// gen 1's request 5 on B answers `RecoveredApplied{result}` — asked with `Some(1)` and with
/// `None` — the answer the node that applied it gives, and the result T1's seed replays for the
/// same retry. Request 10 is the retention boundary (`retained_through`) and answers the same. An
/// identity gen 1 never applied answers `Unknown`, not `StatusExpired`: a seeded generation is a
/// retained one, and absence in it proves nothing (§4.3 invariant 5).
///
/// Red at 3ee83ac: `Expired` for `Some(1)` and `Unknown` for `None` (tester-ka-rows S-117s), the
/// answers for a generation P1 never opened.
#[retcd_test]
fn m7a_194_p1_on_a_failover_node_answers_a_previous_generations_status() {
    support::preamble();
    let mut runner = Runner::new(&a1p1_plan()).expect("runner");
    let (seed_open, after_seed) = seed_window();
    // The seed window (A-R90, tester-ka-next T1): the recovery sets the seed at `seed_open` and
    // the snapshot reaches the cut about 500 ticks later. Inside it gen 1 is retained but not
    // loaded, so an identity it holds answers `Unknown` — never `StatusExpired`, the answer
    // M7A-89 lets a client act on by resubmitting.
    run_to(&mut runner, seed_open);
    assert!(
        p1k(&runner).seed_pending().is_some(),
        "fixture: the seed is set and owed at {seed_open}"
    );
    queue(&mut runner, seed_open + 1, 9_700, status(WRITTEN, Some(1)));
    let inside = run_to(&mut runner, after_seed - 1);
    assert_eq!(
        status_reply(&inside, WRITTEN),
        vec![TxnStatus::Unknown],
        "inside the seed window a retained generation answers Unknown, not StatusExpired"
    );
    let kernel = t1(&runner);
    assert!(
        kernel.seed_pending().is_none(),
        "fixture: T1's dedup seed landed (M7A-117)"
    );
    let replayed = match kernel
        .dedup()
        .get(Generation(1), AffinityId(1), identity(WRITTEN))
    {
        Some(Retained {
            answer: RetainedAnswer::Applied(result),
            ..
        }) => *result,
        other => panic!("fixture: T1 replays gen 1's request 5 from its seed: {other:?}"),
    };
    assert_eq!(
        (replayed.generation, replayed.seq),
        (Generation(1), Seq(WRITTEN)),
        "fixture: the replayed result is gen 1's seq 5"
    );

    queue(&mut runner, after_seed, 9_701, status(WRITTEN, Some(1)));
    queue(&mut runner, after_seed + 1, 9_702, status(WRITTEN, None));
    queue(&mut runner, after_seed + 2, 9_703, status(HEAD, Some(1)));
    queue(&mut runner, after_seed + 3, 9_704, status(WRITE, Some(1)));
    let replies = run_to(&mut runner, after_seed + 10);
    let recovered = |seq| {
        TxnStatus::Resolved(TxnResult {
            seq: Seq(seq),
            outcome: Outcome::RecoveredApplied,
            ..replayed
        })
    };
    assert_eq!(
        status_reply(&replies, WRITTEN),
        vec![recovered(WRITTEN), recovered(WRITTEN)],
        "gen 1's request 5 on B: RecoveredApplied with T1's replayed result, with and without the generation"
    );
    assert_eq!(
        status_reply(&replies, HEAD),
        vec![recovered(HEAD)],
        "request 10, at retained_through, is retained too"
    );
    assert_eq!(
        status_reply(&replies, WRITE),
        vec![TxnStatus::Unknown],
        "an identity gen 1 never applied is Unknown in a seeded generation, never StatusExpired"
    );
    assert_eq!(
        p1(&runner).status.len(),
        HEAD as usize,
        "gen 1's ten entries are seeded, and nothing else is held"
    );
}

/// P1's kernel on B, for what `PubStateView` does not expose: the pending seed.
fn p1k(runner: &Runner) -> &rdb_core::publication::PubKernel {
    runner
        .dispatcher()
        .publication()
        .kernel(B, PART)
        .expect("P1 kernel on B")
}

/// `(seed_open, after_seed)` on `a1p1_plan()`: the tick B's recovery sets the seed, and the tick
/// M7A-117 shows T1's seed landed by, ~500 ticks later, when the snapshot reaches the cut.
fn seed_window() -> (u64, u64) {
    let seed_open = cases::PLAN_AT + 2_097;
    (seed_open, seed_open + 503)
}

fn trim_status(generation: u64, below: u64) -> EventKind {
    EventKind::Kernel(KernelEvent::StatusTrim {
        generation: Generation(generation),
        below: Seq(below),
    })
}

fn retire(generation: u64) -> EventKind {
    EventKind::Kernel(KernelEvent::RetireGeneration {
        generation: Generation(generation),
    })
}

/// M7A-194, the seed past a row that does not decode (A-R90, tester-ka-next T1): one malformed
/// `Dedup` row in B's durable prefix. The seed skips it and lands; every well-formed row is held
/// and gen 1's request 5 answers `RecoveredApplied` with and without the generation. (T1's own
/// dedup seed stays pending on the same row — M7A-117's reading, not asserted here.)
#[retcd_test]
fn m7a_194_status_seed_skips_a_malformed_row_and_lands() {
    support::preamble();
    let mut plan = a1p1_plan();
    for (node, batch) in &mut plan.preloads {
        if *node == B && batch.seq == Seq(HEAD) {
            batch.writes.push(Write {
                ns: Namespace::Dedup,
                key: Bytes::from_static(&[1, 2, 3]),
                value: Some(Bytes::from_static(&[4, 5, 6])),
            });
        }
    }
    let mut runner = Runner::new(&plan).expect("runner");
    let (_, after_seed) = seed_window();
    run_to(&mut runner, after_seed + 10);
    assert!(
        p1k(&runner).seed_pending().is_none(),
        "the seed completed past the malformed row"
    );
    assert_eq!(
        p1(&runner).status.len(),
        HEAD as usize,
        "every well-formed row landed"
    );
    queue(
        &mut runner,
        after_seed + 11,
        9_730,
        status(WRITTEN, Some(1)),
    );
    queue(&mut runner, after_seed + 12, 9_731, status(WRITTEN, None));
    let replies = run_to(&mut runner, after_seed + 22);
    let answers = status_reply(&replies, WRITTEN);
    assert_eq!(answers.len(), 2, "{answers:?}");
    assert!(
        answers.iter().all(|status| matches!(
            status,
            TxnStatus::Resolved(result)
                if result.outcome == Outcome::RecoveredApplied
                    && result.generation == Generation(1)
                    && result.seq == Seq(WRITTEN)
        )),
        "gen 1's request 5 answers RecoveredApplied, with and without the generation: {answers:?}"
    );
}

/// M7A-194, the seed's refusals, first half (A-R90, tester-ka-next T7): `RetireGeneration{1}`
/// lands after the recovery set the seed and before the snapshot reaches the cut. Nothing of gen
/// 1 is resurrected: the seed is consumed with every row refused, the index holds nothing, and
/// request 5 answers `StatusExpired` with the generation and `Unknown` without it (A-R63).
#[retcd_test]
fn m7a_194_status_seed_never_resurrects_a_retired_generation() {
    support::preamble();
    let mut runner = Runner::new(&a1p1_plan()).expect("runner");
    let (seed_open, after_seed) = seed_window();
    run_to(&mut runner, seed_open);
    assert!(
        p1k(&runner).seed_pending().is_some(),
        "fixture: the seed is owed"
    );
    queue(&mut runner, seed_open + 1, 9_740, retire(1));
    run_to(&mut runner, after_seed - 1);
    queue(&mut runner, after_seed, 9_741, status(WRITTEN, Some(1)));
    queue(&mut runner, after_seed + 1, 9_742, status(WRITTEN, None));
    let replies = run_to(&mut runner, after_seed + 10);
    assert_eq!(
        status_reply(&replies, WRITTEN),
        vec![TxnStatus::Expired, TxnStatus::Unknown],
        "a retired generation stays retired: Expired with it, Unknown without it"
    );
    assert_eq!(p1(&runner).status.len(), 0, "nothing of gen 1 is held");
    assert!(
        p1k(&runner).seed_pending().is_none(),
        "the seed is consumed although every row was refused"
    );
}

/// M7A-194, the seed's refusals, second half (A-R90, tester-ka-next T7): `StatusTrim{1, below
/// 6}` before the seed lands (the watermark refuses the rows below it) and after it (the trim
/// drops them). The same answers either way: request 5 `Unknown` with and without the
/// generation, request 6 at the floor and request 10 `RecoveredApplied`, five rows held.
#[retcd_test]
fn m7a_194_status_seed_respects_a_trim_watermark() {
    support::preamble();
    for before in [true, false] {
        let mut runner = Runner::new(&a1p1_plan()).expect("runner");
        let (seed_open, after_seed) = seed_window();
        if before {
            run_to(&mut runner, seed_open);
            assert!(
                p1k(&runner).seed_pending().is_some(),
                "fixture: the seed is owed"
            );
            queue(&mut runner, seed_open + 1, 9_750, trim_status(1, 6));
            run_to(&mut runner, after_seed - 1);
        } else {
            run_to(&mut runner, after_seed - 1);
            queue(&mut runner, after_seed, 9_750, trim_status(1, 6));
            run_to(&mut runner, after_seed + 1);
        }
        assert!(
            p1k(&runner).seed_pending().is_none(),
            "before {before}: the seed landed"
        );
        let t = after_seed + 2;
        queue(&mut runner, t, 9_751, status(WRITTEN, Some(1)));
        queue(&mut runner, t + 1, 9_752, status(6, Some(1)));
        queue(&mut runner, t + 2, 9_753, status(HEAD, Some(1)));
        queue(&mut runner, t + 3, 9_754, status(WRITTEN, None));
        let replies = run_to(&mut runner, t + 10);
        assert_eq!(
            status_reply(&replies, WRITTEN),
            vec![TxnStatus::Unknown, TxnStatus::Unknown],
            "before {before}: trimmed request 5 is Unknown, with and without the generation"
        );
        let kept = |request: u64| {
            matches!(
                status_reply(&replies, request).as_slice(),
                [TxnStatus::Resolved(result)]
                    if result.outcome == Outcome::RecoveredApplied
                        && result.generation == Generation(1)
                        && result.seq == Seq(request)
            )
        };
        assert!(kept(6), "before {before}: request 6, at the floor, is kept");
        assert!(kept(HEAD), "before {before}: request 10 is kept");
        assert_eq!(
            p1(&runner).status.len(),
            5,
            "before {before}: rows 6..=10 are held"
        );
    }
}

// ---- M7A-131 --------------------------------------------------------------------------------

/// M7A-131 (charter A1/P1 adversarial "expire authority between publication and reply", ADR
/// 0007 "late old dispatch quarantine only", §4.2 step 6). Request 11 passes `Dispatch` and
/// `Publication` at `SUBMIT` and is published at seq 11 (the fixture's "seq 5"); its
/// `Check{Reply}` is delayed 2 000 ms by a hop delay on B's `Reply` checkpoint (P-3), and the
/// control completion A1 waits on was dropped at `SUBMIT - 1600` (as M7A-163), so B's local
/// window lapses at the last view's horizon — after the publication, before the delayed check is
/// answered. A1 fences `Node/Expired` with a superseding view; P1's freeze withholds the reply
/// (`awaiting_reply` empty at the fence, A-R90); the delayed check is then answered
/// `Deny(Expired)` at the `Reply` gate, finds nothing awaiting, and nothing is replied: zero `ReplyEffect` carrying a result for request 11,
/// `Status == Published{result}` for it, `published_seq` still 11.
///
/// The control run (no drop) is the same run without the lapse: the delayed check is answered
/// `Valid` at `SUBMIT + 2000` and the one reply is delivered then, so the zero above is the
/// fence's doing and not the delay's.
///
/// Log half (Q-43, quiesced — the trace is written after `finish`): one `publish` line for seq
/// 11 in both runs; one `client_outcome_reported` for request 11 with `delivered = true` in the
/// control run and **none** in the expired run. A `delivered = false` line has no producer in
/// this build: `PubFact::ReplyWithheld` leaves P1 as `Ignored(ReplyWithheld)`, an `Ignored`
/// effect carries no request identity and is never written to the trace (`run.rs`), and
/// `AuthorityIgnoreReason` is a frozen contract — ruled A-R88: the zero-beside-control form
/// stands, no contract change.
#[retcd_test]
fn m7a_131_a1_p1_expire_authority_between_publication_and_reply() {
    support::preamble();
    let delay = 2_000;
    // Control first: its lines put every column the expired run's queries name into the
    // relation, so a missing line reads as a zero and never as a binder error.
    for drop in [false, true] {
        let mut runner = Runner::new(&a1p1_plan()).expect("runner");
        let mut replies = run_to(&mut runner, SUBMIT - 1_600);
        if drop {
            runner
                .control_mut()
                .inject(ControlOp::DropCompletion { node: B })
                .expect("drop B's next control completion");
        }
        runner.dispatcher_mut().delay_hop(HopDelay {
            node: B,
            checkpoint: Checkpoint::Reply,
            by_millis: delay,
        });
        replies.extend(run_to(&mut runner, SUBMIT + 20));
        let view = p1(&runner);
        assert_eq!(
            view.published.seq,
            Seq(WRITE),
            "drop {drop}: request 11 passed Publication and is published at seq 11"
        );
        assert_eq!(
            view.awaiting_reply.len(),
            1,
            "drop {drop}: its reply waits on the delayed Reply check"
        );
        assert!(
            replies_for(&replies, identity(WRITE)).is_empty(),
            "drop {drop}: nothing replied before the check"
        );
        let last = view.authority.expect("P1 holds A1's view");
        let fence_tick = last.valid_through_tick.0 + 1;
        if drop {
            assert!(
                (SUBMIT + 20..SUBMIT + delay).contains(&fence_tick),
                "the lapse ({fence_tick}) lands between the publication and the delayed check"
            );
            replies.extend(run_to(&mut runner, fence_tick));
            let a1 = runner.dispatcher().authority(B).expect("A1 on B").view();
            assert_eq!(
                a1.state,
                AuthorityState::Fenced {
                    reason: DenyReason::Expired,
                    at: Tick(fence_tick),
                },
                "Fence{{Node, Expired}} at the local horizon"
            );
            let superseding = p1(&runner).authority.expect("a view after the fence");
            assert!(
                superseding.authority_seq > last.authority_seq
                    && superseding.past_horizon == DenyReason::Expired,
                "the fence pushed its superseding view: {superseding:?}"
            );
            assert!(
                p1(&runner).awaiting_reply.is_empty(),
                "the freeze withheld the awaited reply"
            );
            assert_eq!(
                p1(&runner).mode,
                PubMode::Frozen {
                    cause: FreezeCause::AuthorityLost(DenyReason::Expired)
                },
                "the fence froze P1: this freeze withheld the reply, not the delayed Deny (A-R90)"
            );
        }
        replies.extend(run_to(&mut runner, SUBMIT + delay + 500));
        let decided: Vec<_> = runner
            .recorded()
            .iter()
            .filter_map(|e| match &e.kind {
                TraceKind::AuthorityDecision {
                    gate: AuthorityGate::Reply,
                    outcome,
                    decision_tick,
                    ..
                } if e.node == B => Some((*decision_tick, *outcome)),
                _ => None,
            })
            .collect();
        let want = if drop {
            AuthorityOutcome::Expired
        } else {
            AuthorityOutcome::Valid
        };
        assert_eq!(
            decided,
            vec![(SUBMIT + delay, want)],
            "drop {drop}: one Reply decision, at the delayed check"
        );
        let view = p1(&runner);
        assert_eq!(
            view.published.seq,
            Seq(WRITE),
            "drop {drop}: published_seq unchanged"
        );
        assert!(
            view.awaiting_reply.is_empty(),
            "drop {drop}: awaiting_reply empty"
        );
        assert!(
            matches!(
                view.status.lookup(identity(WRITE), Generation(2)),
                StatusOutcome::Published { result } if result.seq == Seq(WRITE)
            ),
            "drop {drop}: Status == Published{{result}}: {:?}",
            view.status.lookup(identity(WRITE), Generation(2))
        );
        let for_11 = replies_for(&replies, identity(WRITE));
        if drop {
            assert!(
                for_11.is_empty(),
                "zero ReplyEffect carrying a result for request 11: {for_11:?}"
            );
        } else {
            assert!(
                matches!(
                    for_11.as_slice(),
                    [ReplyEffect::Transaction { result, .. }] if result.seq == Seq(WRITE)
                ),
                "control: the delayed check admits and the one reply is delivered: {for_11:?}"
            );
        }

        // Log half.
        let trace = runner.finish().expect("trace");
        let want_outcomes = if drop {
            vec![]
        } else {
            vec![(SUBMIT + delay, ClientOutcome::Success)]
        };
        assert_eq!(
            outcomes(&trace.events, WRITE, None),
            want_outcomes,
            "drop {drop}: the trace's outcome lines for request 11"
        );
        let method = if drop {
            "m7a_131_expired"
        } else {
            "m7a_131_control"
        };
        let tags = LogTags::new(module_path!(), method, test_run_id());
        let path = log_jsonl_path(&test_log_dir(), module_path!(), method);
        write_log_jsonl(&trace.events, &tags, &path).expect("the tier-1 lines are written");
        let relation = config_testkit::logs::test_logs_relation();
        let count = |sql: String| -> u64 {
            let rows = config_testkit::logs::query(&sql);
            rows[0]["n"].as_u64().expect("a count")
        };
        let publishes = count(format!(
            "SELECT count(*) AS n FROM {relation} WHERE testMethod = '{method}' \
             AND \"@m\" = 'publish' AND seq = {WRITE}"
        ));
        let delivered = count(format!(
            "SELECT count(*) AS n FROM {relation} WHERE testMethod = '{method}' \
             AND \"@m\" = 'client_outcome_reported' AND request = {WRITE} AND delivered"
        ));
        let reported = count(format!(
            "SELECT count(*) AS n FROM {relation} WHERE testMethod = '{method}' \
             AND \"@m\" = 'client_outcome_reported' AND request = {WRITE}"
        ));
        assert_eq!(
            (publishes, reported, delivered),
            (1, u64::from(!drop), u64::from(!drop)),
            "drop {drop}: one publish for seq 11; a delivered outcome only in the control run"
        );
    }
}

// ---- M7A-136 --------------------------------------------------------------------------------

/// M7A-136 (spike §6 F1/T1, ADR 0004 "generation reconciliation"). As M7A-117, with the client
/// retrying **during** the recovery rather than after it: the mutation is applied exactly once
/// across both generations. Asserted from the `BatchApply` lines the run records — INV-DEDUP's
/// subject, read the way M7A-117 reads it (S-117g) rather than through O1: every apply of
/// request 5's mutation is the one its own submission made, and no retry's correlation ever
/// reaches storage.
///
/// - **Same node** (`gen2_committed`, then gen 3's root at `now`): a retry queued at `now`, ahead
///   of the root, is answered from gen 2's own index — the published result replayed, no batch;
///   the first queued behind the root, during the resume hold, is refused `PROTECTION_PAUSED`;
///   the other four, through gen 3's first admission, are refused `GENERATION_CHANGED{2, 3}` at
///   check 5. The sequence is pinned; `NOT_PRIMARY` is never a same-node answer (reviewer T-3).
///   `Status{Some(2)}` folds `RecoveredApplied`. A fresh request at `GEN3_ADMITS + 2` is
///   published in gen 3 at the next sequence, so gen 3 admits — it just does not re-execute.
/// - **Different node** (the A1/P1 case): retries reach B before its T1 exists (`NOT_PRIMARY`),
///   while its seed is pending (`PROTECTION_PAUSED`, M7A-117), and after it lands
///   (`GENERATION_CHANGED{1, 2}`); the grammar's request 11 and a fresh request 41 are gen 2's
///   only batches.
///
/// In both halves every node applies seq 5 exactly once — for the copy that lacked the prefix,
/// as the recovered lineage's catch-up under the recovery's own correlation, never under a
/// retry's.
#[retcd_test]
fn m7a_136_f1_t1_generation_reconciliation_no_double_apply() {
    support::preamble();
    // Different node only: `NOT_PRIMARY` is B's answer before its T1 exists, and the same node
    // never gives it (reviewer T-3).
    let refused = |reply: &ReplyEffect, current: u64| {
        matches!(
            reply,
            ReplyEffect::Failed {
                error: RdbError::ProtectionPaused { .. } | RdbError::NotPrimary { .. },
                ..
            }
        ) || matches!(
            reply,
            ReplyEffect::Failed {
                error: RdbError::GenerationChanged { current: got, .. },
                ..
            } if got.0 == current
        )
    };
    let kind = |reply: &ReplyEffect| match reply {
        ReplyEffect::Failed {
            error: RdbError::ProtectionPaused { .. },
            ..
        } => "PROTECTION_PAUSED".to_owned(),
        ReplyEffect::Failed {
            error: RdbError::NotPrimary { .. },
            ..
        } => "NOT_PRIMARY".to_owned(),
        ReplyEffect::Failed {
            error: RdbError::GenerationChanged { expected, current },
            ..
        } => format!("GENERATION_CHANGED{{{}, {}}}", expected.0, current.0),
        other => format!("{other:?}"),
    };

    // Same node.
    let (mut runner, published, written) = gen2_committed();
    let now = runner.dispatcher().clock().now().0;
    let retries = 9_601..=9_607;
    queue(&mut runner, now, 9_601, submit(written.clone()));
    recover_gen3(&mut runner, false);
    queue(&mut runner, now + 1, 9_602, submit(written.clone()));
    queue(
        &mut runner,
        now + 2,
        9_603,
        submit(expecting(written.clone(), 2)),
    );
    queue(&mut runner, now + 3, 9_604, status(WRITTEN, Some(2)));
    queue(&mut runner, GEN3_ADMITS - 1, 9_605, submit(written.clone()));
    queue(&mut runner, GEN3_ADMITS, 9_606, submit(written.clone()));
    queue(
        &mut runner,
        GEN3_ADMITS + 1,
        9_607,
        submit(expecting(written, 2)),
    );
    queue(&mut runner, GEN3_ADMITS + 2, 9_608, submit(txn(40, 40)));
    let replies = run_to(&mut runner, GEN3_ADMITS + 60);
    // The submits' answers; the status query's is asserted on its own below.
    let for_5: Vec<_> = replies_for(&replies, identity(WRITTEN))
        .into_iter()
        .filter(|reply| !matches!(reply, ReplyEffect::Status { .. }))
        .collect();
    assert_eq!(for_5.len(), 6, "one answer per retry: {for_5:?}");
    assert_eq!(
        for_5[0],
        &ReplyEffect::Transaction {
            identity: identity(WRITTEN),
            result: published,
        },
        "ahead of the root: gen 2's own index replays the published result"
    );
    assert_eq!(
        for_5[1..]
            .iter()
            .map(|reply| kind(reply))
            .collect::<Vec<_>>(),
        [
            "PROTECTION_PAUSED",
            "GENERATION_CHANGED{2, 3}",
            "GENERATION_CHANGED{2, 3}",
            "GENERATION_CHANGED{2, 3}",
            "GENERATION_CHANGED{2, 3}",
        ],
        "behind the root, in reply order: PROTECTION_PAUSED in the resume hold, then \
         GENERATION_CHANGED{{2, 3}} through gen 3's admission; never NOT_PRIMARY here"
    );
    assert_eq!(
        status_reply(&replies, WRITTEN),
        vec![TxnStatus::Resolved(gen2_result(
            WRITTEN,
            Outcome::RecoveredApplied,
            &published
        ))],
        "Status{{Some(2)}} during the recovery folds RecoveredApplied"
    );
    assert!(
        matches!(
            replies_for(&replies, identity(40)).as_slice(),
            [ReplyEffect::Transaction { result, .. }]
                if result.generation == Generation(3) && result.seq == Seq(WRITTEN + 1)
        ),
        "gen 3 admits a fresh request at the next sequence"
    );
    let trace = runner.finish().expect("trace");
    let applies = batch_applies(&trace.events);
    let at_5: Vec<_> = applies
        .iter()
        .filter(|(.., seq)| *seq == Seq(WRITTEN))
        .collect();
    assert_eq!(
        at_5,
        vec![
            &(9_399, NodeId(1), Generation(2), Seq(WRITTEN)),
            &(9_399, NodeId(2), Generation(2), Seq(WRITTEN)),
            &(9_399, NodeId(3), Generation(2), Seq(WRITTEN)),
        ],
        "seq 5 is applied once per node, by its own submission, in gen 2"
    );
    assert!(
        applies
            .iter()
            .all(|(correlation, ..)| !retries.contains(correlation)),
        "no retry's correlation reaches storage: {applies:?}"
    );
    assert_eq!(
        applies
            .iter()
            .filter(|(_, _, generation, _)| *generation == Generation(3))
            .map(|(correlation, _, _, seq)| (*correlation, *seq))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([(9_608, Seq(WRITTEN + 1))]),
        "gen 3's only batch is the fresh request"
    );

    // Different node.
    let mut runner = Runner::new(&a1p1_plan()).expect("runner");
    let seed_open = cases::PLAN_AT + 2_097;
    let after_seed = seed_open + 503;
    let retries = 9_611..=9_616;
    for (at, correlation) in [
        (cases::PLAN_AT + 1, 9_611),
        (cases::PLAN_AT + 1_000, 9_612),
        (cases::PLAN_AT + 2_002, 9_613),
        (seed_open, 9_614),
        (after_seed, 9_615),
        (SUBMIT + 200, 9_616),
    ] {
        queue(&mut runner, at, correlation, submit(txn(WRITTEN, WRITTEN)));
    }
    queue(&mut runner, SUBMIT + 201, 9_617, submit(txn(41, 41)));
    let replies = run_to(&mut runner, SUBMIT + 300);
    let for_5 = replies_for(&replies, identity(WRITTEN));
    assert_eq!(for_5.len(), 6, "one answer per retry: {for_5:?}");
    assert!(
        for_5.iter().all(|reply| refused(reply, 2)),
        "every retry on B is refused, NOT_PRIMARY, PROTECTION_PAUSED or GENERATION_CHANGED{{1, 2}}: {for_5:?}"
    );
    assert_eq!(
        for_5[5],
        &ReplyEffect::Failed {
            identity: identity(WRITTEN),
            error: RdbError::GenerationChanged {
                expected: Generation(1),
                current: Generation(2),
            },
        },
        "after the seed the retry is reconciled from gen 1's row"
    );
    let trace = runner.finish().expect("trace");
    let applies = batch_applies(&trace.events);
    for node in [NodeId(1), NodeId(2), NodeId(3)] {
        let at_5 = applies
            .iter()
            .filter(|(_, n, _, seq)| *n == node && *seq == Seq(WRITTEN))
            .count();
        assert_eq!(at_5, 1, "{node:?} applies seq 5 exactly once: {applies:?}");
    }
    assert!(
        applies
            .iter()
            .all(|(correlation, ..)| !retries.contains(correlation)),
        "no retry's correlation reaches storage: {applies:?}"
    );
    assert_eq!(
        applies
            .iter()
            .filter(|(_, n, generation, _)| *n == B && *generation == Generation(2))
            .map(|(_, _, _, seq)| *seq)
            .collect::<Vec<_>>(),
        vec![Seq(WRITE), Seq(WRITE + 1)],
        "gen 2's batches on B are request 11 and the fresh request 41"
    );
}

// ---- M7A-132..M7A-134 -----------------------------------------------------------------------

/// What reaches B's A1 between its `Dispatch` answer for request 11 and T1's `BatchCompleted`.
#[derive(Debug, Clone, Copy)]
enum LateInput {
    /// `NodeLifecycle::Resumed` with a gap one millisecond past the tolerance (M7A-132).
    Pause,
    /// `NodeLifecycle::Rebooted` naming a boot other than the grant's (M7A-133).
    Reboot,
    /// The planner re-issues B's grant record under `authority_generation + 1`, and B's read of
    /// `grants/{B}` answers `Found` with that record (M7A-134).
    NewGeneration,
}

impl LateInput {
    /// The fence reason the plan names for this input.
    const fn reason(self) -> DenyReason {
        match self {
            Self::Pause => DenyReason::ProcessSuspended,
            Self::Reboot => DenyReason::BootMismatch,
            Self::NewGeneration => DenyReason::AuthorityGenerationChanged,
        }
    }
}

/// What the late old dispatch leaves behind.
struct LateRun {
    runner: Runner,
    replies: Replies,
    /// T1's queue left `Open` while seq 11's batch was dispatched and not yet completed.
    fenced_before_completion: bool,
    /// The batch completed after that: T1 saw `BatchCompleted` for seq 11.
    completed_after_fence: bool,
}

/// The fenced generation: the one B recovered and dispatched request 11 under.
const G2: Generation = Generation(2);

/// The late old dispatch: request 11 (seq 11 stands in for the plan's "seq 5") is admitted and
/// dispatched under generation 2, `input` reaches B's A1 behind its `Dispatch` answer and before
/// the batch completes, and the run goes on past P1's post-apply deadline.
fn late_old_dispatch(input: LateInput) -> LateRun {
    let mut runner = Runner::new(&a1p1_plan()).expect("runner");
    let mut replies = run_to(&mut runner, SUBMIT - 1);
    while !matches!(
        t1(&runner).inflight(),
        Some(Inflight::AwaitingDispatchCheck { .. })
    ) {
        assert!(
            step(&mut runner, SUBMIT, &mut replies),
            "the tick ran out before T1 admitted request 11"
        );
    }
    let kind = match input {
        LateInput::Pause => EventKind::Node(NodeLifecycle::Resumed {
            suspended_millis: Budgets::SPEC_DEFAULTS.resume_gap_tolerance_millis + 1,
        }),
        LateInput::Reboot => EventKind::Node(NodeLifecycle::Rebooted {
            boot: BootId(BOOT.0 + 1),
        }),
        LateInput::NewGeneration => {
            // The planner's re-issue, written to the store as the planner would write it; the
            // read answer B's A1 classifies is the store's own body for it.
            let key = ControlKey::Grant(B);
            let ReadOutcome::Found { revision, value } = runner.control_mut().get(key) else {
                panic!("B holds a grant record");
            };
            let mut record = GrantRecord::decode(&value).expect("a grant record");
            record.authority_generation = AuthorityGeneration(record.authority_generation.0 + 1);
            let CasOutcome::Committed(_) =
                runner
                    .control_mut()
                    .scenario_cas(key, Some(revision), Some(record.encode()))
            else {
                panic!("the planner's re-issue commits");
            };
            let outcome = runner.control_mut().get(key);
            EventKind::Control(ControlEvent::Value {
                request: ControlRequestId(9_602),
                key,
                outcome,
            })
        }
    };
    queue(&mut runner, SUBMIT, 9_601, kind);
    tracing::info!(
        ?input,
        "M7A-132..134 late input queued behind the Dispatch answer"
    );

    let mut fenced_before_completion = false;
    let mut completed_after_fence = false;
    while step(&mut runner, SUBMIT, &mut replies) {
        let open = *t1(&runner).mode() == QueueMode::Open;
        match dispatched(&runner) {
            Some((seq, false)) if seq == Seq(WRITE) && !open => fenced_before_completion = true,
            Some((seq, true)) if seq == Seq(WRITE) && fenced_before_completion => {
                completed_after_fence = true;
            }
            _ => {}
        }
    }
    let deadline = Tick(SUBMIT).plus_millis(POST_APPLY_DEADLINE_MILLIS).0;
    replies.extend(run_to(&mut runner, deadline + 100));
    LateRun {
        runner,
        replies,
        fenced_before_completion,
        completed_after_fence,
    }
}

/// M7A-132..M7A-134's shared claim: the late batch leaves quarantined bytes and nothing else.
fn assert_quarantined_bytes_only(row: &str, input: LateInput) {
    let reason = input.reason();
    let run = late_old_dispatch(input);
    let (runner, replies) = (&run.runner, &run.replies);
    assert!(
        run.fenced_before_completion && run.completed_after_fence,
        "{row}: the fence lands while seq {WRITE} is dispatched, and the batch completes after it"
    );
    assert!(
        matches!(
            runner.dispatcher().authority(B).expect("A1 on B").view().state,
            AuthorityState::Fenced { reason: fenced, .. } if fenced == reason
        ),
        "{row}: A1 on B fenced the node for {reason:?}"
    );

    // The kernel half: P1 quarantines (g, 11), freezes for the reason, publishes nothing at 11.
    let quarantined: Vec<(Generation, Seq)> = runner
        .recorded()
        .iter()
        .filter(|event| event.node == B)
        .filter_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note:
                    KernelNote::PublicationFact {
                        effect: PublicationEffect::Quarantined { generation, seq },
                    },
                ..
            } => Some((*generation, *seq)),
            _ => None,
        })
        .collect();
    assert_eq!(
        quarantined,
        vec![(G2, Seq(WRITE))],
        "{row}: Fact(Quarantined{{g, {WRITE}}}) once, under the fenced generation"
    );
    let view = p1(runner);
    assert_eq!(
        view.mode,
        PubMode::Frozen {
            cause: FreezeCause::AuthorityLost(reason)
        },
        "{row}: P1 froze for the fence's reason"
    );
    assert_eq!(
        view.published.seq,
        Seq(HEAD),
        "{row}: the published prefix never reaches {WRITE}"
    );
    let published: Vec<Seq> = runner
        .recorded()
        .iter()
        .filter_map(|event| match &event.kind {
            TraceKind::Publish { seq, .. } if *seq >= Seq(WRITE) => Some(*seq),
            _ => None,
        })
        .collect();
    assert!(
        published.is_empty(),
        "{row}: no Publish at or past {WRITE}: {published:?}"
    );
    let answers = replies_for(replies, identity(WRITE));
    assert!(
        !answers.is_empty()
            && answers
                .iter()
                .all(|reply| !matches!(reply, ReplyEffect::Transaction { .. })),
        "{row}: request 11 is answered, and never with a result: {answers:?}"
    );

    // The inventory half: every copy holds seq 11's bytes in the fenced generation's namespace
    // and in no other generation of the partition.
    for node in [cases::A_NODE, B, cases::C_NODE] {
        let engine = runner
            .dispatcher()
            .engine(node)
            .expect("an engine on every node");
        assert_eq!(
            engine.holders(PART, Seq(WRITE)),
            vec![G2],
            "{row}: {node:?} holds seq {WRITE} under the fenced generation only"
        );
    }

    // The next generation (critic F11): recovery cuts gen 3 at the published head, gen 3 exists
    // on every copy, and what it shows — its inherited prefix included — never holds seq 11.
    let root = gen2_root(&run.runner);
    assert_eq!(
        root.selected.cutoff_seq,
        Seq(HEAD),
        "{row}: gen 2 began at {HEAD}, so its root's cutoff digest is seq {HEAD}'s"
    );
    let cut = (Seq(HEAD), root.selected.cutoff_digest);
    let mut runner = run.runner;
    recover_gen3_at(&mut runner, cut, Some(Seq(WRITE)), false);
    let landed = runner.dispatcher().clock().now().0 + 1_000;
    let _ = run_to(&mut runner, landed);
    assert_next_generation_hides_the_late_write(row, &runner);
}

/// Gen 3 exists on B, the barrier's only copy, inheriting gen 2 through [`HEAD`]. Its readable
/// view shows the inherited prefix and not seq 11, whose bytes stay readable under gen 2. A and
/// C are outside the barrier, so they inherit nothing (`in_barrier` in the dispatcher); on every
/// copy, seq 11's bytes are still held under gen 2 and no other generation.
fn assert_next_generation_hides_the_late_write(row: &str, runner: &Runner) {
    const G3: Generation = Generation(3);
    let from_late_write = |view: &dyn SnapshotRead| -> Vec<Bytes> {
        view.scan(Namespace::User, &[], usize::MAX)
            .into_iter()
            .map(|(key, _)| key)
            .filter(|key| view.version(Namespace::User, key) == Some(WRITE))
            .collect()
    };
    {
        let node = B;
        let engine = runner.dispatcher().engine(B).expect("B's engine");
        assert_eq!(
            (engine.parent(PART, G3), engine.base(PART, G3)),
            (Some(G2), Seq(HEAD)),
            "{row}: {node:?} holds gen 3, inheriting gen 2 through {HEAD}"
        );
        assert!(
            engine.history_at(PART, G3, Seq(HEAD)).is_some(),
            "{row}: {node:?}'s gen 3 shows the inherited seq {HEAD}"
        );
        assert_eq!(
            engine.history_at(PART, G3, Seq(WRITE)),
            None,
            "{row}: {node:?}'s gen 3 shows no seq {WRITE}"
        );
        assert!(
            engine.history_at(PART, G2, Seq(WRITE)).is_some(),
            "{row}: {node:?}'s gen 2 still holds seq {WRITE}'s bytes"
        );
        let old = engine.snapshot(PART, G2, SnapshotHandle(1));
        let new = engine.snapshot(PART, G3, SnapshotHandle(2));
        assert!(
            !from_late_write(&old).is_empty(),
            "{row}: {node:?}'s gen 2 view shows a user record seq {WRITE} wrote"
        );
        assert_eq!(
            from_late_write(&new),
            Vec::<Bytes>::new(),
            "{row}: {node:?}'s gen 3 view shows no user record seq {WRITE} wrote"
        );
    }
    for node in [cases::A_NODE, B, cases::C_NODE] {
        let engine = runner
            .dispatcher()
            .engine(node)
            .expect("an engine on every node");
        assert_eq!(
            engine.holders(PART, Seq(WRITE)),
            vec![G2],
            "{row}: {node:?} holds seq {WRITE} under gen 2 only, after gen 3"
        );
    }
}

#[retcd_test]
fn m7a_132_a1_p1_delayed_old_dispatch_after_pause_quarantined_bytes_only() {
    support::preamble();
    assert_quarantined_bytes_only("M7A-132", LateInput::Pause);
}

#[retcd_test]
fn m7a_133_a1_p1_delayed_old_dispatch_after_reboot_quarantined_bytes_only() {
    support::preamble();
    assert_quarantined_bytes_only("M7A-133", LateInput::Reboot);
}

#[retcd_test]
fn m7a_134_a1_p1_delayed_old_dispatch_after_new_generation_quarantined_bytes_only() {
    support::preamble();
    assert_quarantined_bytes_only("M7A-134", LateInput::NewGeneration);
}
