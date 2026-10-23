//! Spike §6's mandatory cross-package cases, as authored constructors (design §3.1, family 2).
//!
//! Rust, not JSON: a hand-typed op list that lands "expire authority between publication and
//! reply" is fragile and uncompiled. Each constructor carries `Provenance::Authored` naming
//! itself, and row **M7V-47** runs it through [`super::run`].
//!
//! All four are here. The F1/T1/P1 and F1/T1 cases (lead ruling V-R37) share the A1/P1 case's
//! arming shape — survivors, one recovery, the resume hold, then client writes — and differ only
//! in what they submit after it: a retry of an identity the prior generation retained, and
//! retries of one identity under the same and another digest.

use rdb_core::authority::partition::PartitionRecord;
use rdb_core::contracts::authority::{Checkpoint, Lineage};
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::event::{Budgets, EventKind, KernelEvent};
use rdb_core::contracts::ids::{
    ClientId, ConfigVersion, DurableSeq, Generation, NodeId, OwnerEpoch, PartitionId, ReplicaRole,
    RequestId, Seq, TenantId,
};
use rdb_core::contracts::membership::CopyId;
use rdb_core::contracts::recovery::{RecoveryEvent, SurvivorInventory};
use rdb_core::contracts::trace::{KeyId, Provenance};
use rdb_core::recovery::MAX_WINDOW_EXTENSIONS;
use rdb_sim::harness::run::RunPlan;

use super::grammar::{
    Budget, ClientOp, NetworkOp, Placement, RecoveryOp, Scenario, ScenarioOp, TimeOp, Topology,
    SCENARIO_GENERATOR_VERSION, SCENARIO_SCHEMA_VERSION,
};

/// The partition every case recovers.
pub const PARTITION: PartitionId = PartitionId(1);
/// Copy 0, the primary slot: where F1 runs and R1 leads after recovery.
pub const B_NODE: NodeId = NodeId(1);
/// Copy 1: the F1/R1 case's transferring source.
pub const C_NODE: NodeId = NodeId(2);
/// Copy 2: the dead prior owner. It neither survives nor transfers.
pub const A_NODE: NodeId = NodeId(3);

/// The F1/R1 case: B's head.
pub const B_HEAD: u64 = 100;
/// The F1/R1 case: what C advertises and never delivers.
pub const C_ADVERTISED: u64 = 150;
/// The F1/R1 case: the tick placement's plan reaches F1. The fence arrives one tick later.
pub const PLAN_AT: u64 = 2;
/// The F1/R1 case: the tick C goes silent.
pub const C_STOPS_AT: u64 = 3_000;
/// The F1/R1 case: its deadline.
pub const F1_R1_MAX_TICKS: u64 = 12_000;
/// The F1/R1 case: the tick discovery closes and L1 pauses. The fence lands at `PLAN_AT + 1` and
/// C's transfer extends the 2 s window once, so the close is fence + 4 s.
pub const F1_R1_PAUSED_AT: u64 = PLAN_AT + 1 + 4_000;
/// The F1/R1 case: the tick of the host's first flush round, derived. The lowering starts the
/// flusher at the **latest** cutoff F1 can choose, fence + window x (1 + `MAX_WINDOW_EXTENSIONS`)
/// = 3 + 2_000 x 4 = 8_003, and A1's first `AcquireDue` 500 ms after that. The window really
/// closes at [`F1_R1_PAUSED_AT`], after one extension, so L1 is paused for 4_000 ticks, exactly
/// two rate windows, before the first round: one window is the walk (window 0), one is paused.
pub const F1_R1_FIRST_HOST_ROUND_AT: u64 = PLAN_AT
    + 1
    + Budgets::SPEC_DEFAULTS.discovery_window_millis * (1 + MAX_WINDOW_EXTENSIONS as u64);
const _: () = assert!(
    (F1_R1_FIRST_HOST_ROUND_AT - F1_R1_PAUSED_AT).is_multiple_of(F1_R1_RATE_WINDOW),
    "the first host round starts a rate window, so no window is part paused and part resumed"
);
/// The F1/R1 case: pops that are neither a rate nor the first host round (ruling A-R201), from
/// the census of 2026-10-23 (int f5b3306, after A-R197): 1_249 pops in a 12_000-tick run, less
/// the paused rate over `[F1_R1_PAUSED_AT, F1_R1_FIRST_HOST_ROUND_AT)` (181 x 2 = 362), the first
/// host round (19) and the resumed rate over the rest (116 x 3_997 / 2_000 = 231.8): 636.2,
/// rounded up. Most of it is at close + 10, where R1 walks C and A up from the root (one record
/// per ACK below the cutoff, B-R58c) and F1 rebuilds and activates; 7 pops are before the close.
/// The 2026-09-26 measurement on joint-b60b, against the old single rate, was 634.4.
pub const F1_R1_BASE_POPS: u64 = 637;
/// The window the F1/R1 rates are counted over.
pub const F1_R1_RATE_WINDOW: u64 = 2_000;
/// The F1/R1 case: pops per [`F1_R1_RATE_WINDOW`] while L1 is paused, derived by pop kind from
/// the census of 2026-10-23 (ruling A-R201):
///
/// ```text
///   L1's evaluation timer, every 50 ms                      2_000 / 50            =  40
///   R1's B-R60 keepalive, 2_000 / 100 (KEEPALIVE_MS) = 20 rounds x 7              = 140
///     B's timer 1, its frame to C and to A 2, and their replies on B 4
///     (an Ack and an AlreadyHave from each)
///   F1's last deadline, armed for one discovery window at the rebuild (close + 10),
///     firing once at 6_013 with nothing left to bound                                 1
///                                                                                  = 181
/// ```
///
/// Measured 181 in `[6_003, 8_003)`. No host flush and no A1 renewal: both start at the latest
/// cutoff, [`F1_R1_FIRST_HOST_ROUND_AT`]. The last term is a one-off, kept in the rate because the
/// row checks exactly one paused window, `[F1_R1_PAUSED_AT + W, F1_R1_FIRST_HOST_ROUND_AT)`, and
/// the deadline falls in it. Before A-R197 the keepalive never stopped and this term was 261.
/// The 100 is written out, not read from `KEEPALIVE_MS`: a keepalive that speeds up must fail
/// here by name, not carry the bound up with it.
pub const F1_R1_PAUSED_POPS_PER_WINDOW: u64 =
    F1_R1_RATE_WINDOW / 50 + (F1_R1_RATE_WINDOW / 100) * 7 + 1;
/// The F1/R1 case: the host's flush cadence per [`F1_R1_RATE_WINDOW`] (ruling V-R30), derived and
/// not measured:
///
/// ```text
///   nodes x rounds per window x pops per flush
/// = 3 (B, C, A) x (2_000 / HOST_FLUSH_EVERY_MILLIS = 100) x 1 = 60
/// ```
///
/// One pop per flush because `Dispatcher::run_due_flushes` queues exactly one `Storage` event per
/// flush that completes (a stalled one queues none). A flush over a prefix that is already durable
/// makes no ACK: the copies answer it `NothingOutstanding`. Cross-checked, not fitted, on
/// 2026-09-27 against a run with only the first round: the window holding that round differed by
/// 57 = 60 less the first round's own 3 `Flushed`, which both runs held. On 2026-10-23 (A-R201)
/// every resumed round holds exactly 3 `Flushed`, one per node.
/// The first round is a one-off on top, [`F1_R1_FIRST_HOST_ROUND_POPS`], and is not in this term.
pub const F1_R1_HOST_FLUSH_POPS_PER_WINDOW: u64 =
    3 * (F1_R1_RATE_WINDOW / super::run::HOST_FLUSH_EVERY_MILLIS);
/// The F1/R1 case: A1's renewals per [`F1_R1_RATE_WINDOW`] once lowering has acquired a grant
/// (ruling V-R30), derived:
///
/// ```text
///   renewals per window x pops per renewal
/// = (2_000 / renew_millis = 500) x (2 A1 pops + 1 authority-view pop after each) = 4 x 4 = 16
/// ```
///
/// The two A1 pops are the renew timer and its CAS's completion. Each publishes the authority
/// view, which is one more pop. The acquisition costs the same four and replaces a renewal.
pub const F1_R1_A1_RENEWAL_POPS_PER_WINDOW: u64 =
    (F1_R1_RATE_WINDOW / Budgets::SPEC_DEFAULTS.renew_millis) * 4;
/// The F1/R1 case: pops per [`F1_R1_RATE_WINDOW`] once L1 has resumed (ruling A-R201), derived:
///
/// ```text
///   L1's evaluation timer, every 50 ms     2_000 / 50 =  40
///   the host's flushes                     F1_R1_HOST_FLUSH_POPS_PER_WINDOW =  60
///   A1's renewals                          F1_R1_A1_RENEWAL_POPS_PER_WINDOW =  16
///                                                                         = 116
/// ```
///
/// Measured 116 in `[10_003, 12_000)`, matching by kind. R1's keepalive is not in it: it runs only
/// while L1 is paused (B-R60), and since A-R197 the `Active` re-emit restates R1's tracker instead
/// of pausing L1 again, so L1 resumes once and stays resumed. Before A-R197 it never resumed here,
/// and that was defect D12, not this case's design. The cadences run to the deadline: ruling B-R65
/// bounds the rate, not the time.
pub const F1_R1_RESUMED_POPS_PER_WINDOW: u64 =
    F1_R1_RATE_WINDOW / 50 + F1_R1_HOST_FLUSH_POPS_PER_WINDOW + F1_R1_A1_RENEWAL_POPS_PER_WINDOW;
/// The F1/R1 case: what the host's **first** flush round adds, once, on all three nodes, over the
/// resumed round after it (rulings V-R31, A-R201). It is the first time the copies' prefix is
/// durable: R1 and L1 see the barrier go durable, and L1 resumes inside the round.
///
/// Derived by pop kind from the trace, over `[8_003, 8_103)` against `[8_103, 8_203)`. The kinds
/// come from the runner's Debug `runner pop` line
/// (`RETCD_TEST_LOG=info,rdb_sim::harness::run=debug`), queried with DuckDB. Measured on
/// 2026-10-23 at int f5b3306 (after A-R197):
///
/// ```text
///   kind                                                       first   next  extra
///   R1's last keepalive round: it fires at the round's tick,        7      0      7
///     before L1 resumes (B's timer 1, the frames to C and A 2,
///     their replies on B 4)
///   Transport: C's and A's durable ACK, sent on their first         2      0      2
///     synced flush (a later flush answers NothingOutstanding)
///   Kernel, R1 -> L1 on B: PeerProgress 4, DurableAdvanced 1        5      0      5
///   Timer: L1's evaluation on B, one per R1 -> L1 event at the      6      2      4
///     round's tick (5) where a resumed round has 1, plus +50
///   Kernel: L1's resume, SetAdmission{allow: true}                  1      0      1
///   Storage: Flushed, one per node                                  3      3      0
///   total                                                          24      5     19
/// ```
///
/// Counted on all three nodes, where the V-R31 table counted B alone: before A-R197 C's and A's
/// rounds were alike (a keepalive frame and a flush each), and now the first holds the last
/// keepalive frame and the next does not. B alone is 20 against 3.
///
/// It is counted once, in the one window that holds the first round ([`f1_r1_window_bound`]),
/// never per window: added to every window it would loosen windows it is not in.
pub const F1_R1_FIRST_HOST_ROUND_POPS: u64 = 7 + 2 + 5 + 4 + 1;

/// The F1/R1 case: the rate of the window starting at `start`. Paused until the host's first
/// round (the tick of the lowered plan's first host flush), resumed from it. With no host round L1
/// never resumes.
#[must_use]
pub const fn f1_r1_window_rate(start: u64, first_host_round: Option<u64>) -> u64 {
    match first_host_round {
        Some(at) if start + F1_R1_RATE_WINDOW > at => F1_R1_RESUMED_POPS_PER_WINDOW,
        _ => F1_R1_PAUSED_POPS_PER_WINDOW,
    }
}

/// The F1/R1 case: the most pops the rate window starting at `start` may hold. Its rate, plus
/// [`F1_R1_FIRST_HOST_ROUND_POPS`] only when this window holds `first_host_round`.
#[must_use]
pub const fn f1_r1_window_bound(start: u64, first_host_round: Option<u64>) -> u64 {
    let rate = f1_r1_window_rate(start, first_host_round);
    match first_host_round {
        Some(at) if start <= at && at < start + F1_R1_RATE_WINDOW => {
            rate + F1_R1_FIRST_HOST_ROUND_POPS
        }
        _ => rate,
    }
}

/// The F1/R1 case's event budget for a deadline of `max_ticks`: base (with the host's first round,
/// a one-off) plus the paused rate up to the first round and the resumed rate after it, times 1.5,
/// rounded up (ruling B-R65: a function of the deadline, never flat).
#[must_use]
pub const fn f1_r1_max_events(max_ticks: u64) -> u32 {
    let paused_until = if max_ticks < F1_R1_FIRST_HOST_ROUND_AT {
        max_ticks
    } else {
        F1_R1_FIRST_HOST_ROUND_AT
    };
    let paused = paused_until.saturating_sub(F1_R1_PAUSED_AT);
    let resumed = max_ticks.saturating_sub(F1_R1_FIRST_HOST_ROUND_AT);
    let scaled = (F1_R1_BASE_POPS + F1_R1_FIRST_HOST_ROUND_POPS) * F1_R1_RATE_WINDOW
        + F1_R1_PAUSED_POPS_PER_WINDOW * paused
        + F1_R1_RESUMED_POPS_PER_WINDOW * resumed;
    let budget = (scaled * 3).div_ceil(2 * F1_R1_RATE_WINDOW);
    assert!(budget <= u32::MAX as u64, "an F1/R1 budget fits a u32");
    budget as u32
}

/// Three nodes, one RF3 partition: B primary, C and A regular secondaries.
#[must_use]
pub fn rf3_partition_1() -> Topology {
    Topology {
        nodes: 3,
        partitions: 1,
        config_version_0: ConfigVersion(1),
        placements: [
            (B_NODE, ReplicaRole::Primary),
            (C_NODE, ReplicaRole::RegularSecondary),
            (A_NODE, ReplicaRole::RegularSecondary),
        ]
        .into_iter()
        .map(|(node, role)| Placement {
            partition: PARTITION,
            node,
            role,
        })
        .collect(),
    }
}

fn authored(case: &str, budget: Budget, ops: Vec<ScenarioOp>) -> Scenario {
    Scenario {
        schema_version: SCENARIO_SCHEMA_VERSION,
        generator_version: SCENARIO_GENERATOR_VERSION,
        provenance: Provenance::Authored {
            case: case.to_owned(),
        },
        topology: rf3_partition_1(),
        budget,
        ops,
    }
}

/// Spike §6, F1/R1: "query reachable eligible prior regular members and verified shadows within
/// the 2 s discovery window. Extend while a higher compatible prefix transfers; record source
/// failure before choosing a shorter prefix and preserve loss uncertainty."
///
/// A (the prior owner) is dead. B holds `1..=100`. C advertises 150 but has sent only 100 when
/// discovery asks, moves 10 records every 1.5 s, and goes silent at tick 3000. So the first
/// deadline (fence + 2 s) sees C moving and extends; the second (fence + 4 s) sees it stopped,
/// records it `Stalled`, and closes on B's shorter 100 with the loss uncertain.
///
/// The same world as `f1_scenarios.rs`'s M7B-96, reached through the grammar instead of a
/// hand-built `RunPlan`, on a topology without the shadow.
#[must_use]
pub fn case_f1_r1_discovery_window() -> Scenario {
    authored(
        "case_f1_r1_discovery_window",
        Budget {
            // A function of the deadline (ruling B-R65): the base scales with B_HEAD and the
            // steady rate with the keepalive and health cadences; change any of them and
            // re-measure both. Row m7v_47 asserts the rate by name, before the budget.
            max_events: f1_r1_max_events(F1_R1_MAX_TICKS),
            max_ticks: F1_R1_MAX_TICKS,
        },
        vec![
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: B_NODE,
                to: Seq(B_HEAD),
            }),
            ScenarioOp::Recovery(RecoveryOp::Transfer {
                partition: PARTITION,
                node: C_NODE,
                received: Seq(B_HEAD),
                advertised: Seq(C_ADVERTISED),
                per_step: 10,
                step_ticks: 1_500,
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: 2_000,
            }),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: C_STOPS_AT - PLAN_AT,
            }),
            ScenarioOp::Time(TimeOp::Pause {
                node: C_NODE,
                ticks: F1_R1_MAX_TICKS - C_STOPS_AT,
            }),
        ],
    )
}

/// The index of [`case_a1_p1_new_generation_between_publish_and_reply`]'s second
/// `InspectSurvivors`: the op that activates the new generation.
pub const A1_P1_ACTIVATE_OP: usize = 6;
/// The index of the same case's hold on B's `Reply` check, right after the activation and at the
/// same tick. [`without_activation`] removes both.
pub const A1_P1_HOLD_OP: usize = 7;
/// How long the case holds B's `Reply` check on its hop: the second recovery's 2 s discovery
/// window, which is when its commit lands on B, plus half a second. The check decides at
/// `A1_P1_SUBMIT_AT + A1_P1_HOLD_MILLIS`, inside [`A1_P1_MAX_TICKS`].
pub const A1_P1_HOLD_MILLIS: u64 = 2_500;
/// The A1/P1 case: where B and C survive. `Synchronize` preloads seqs 1..=10 as client writes
/// with request ids 1..=10, so those ids are taken.
pub const A1_P1_HEAD: u64 = 10;
/// The A1/P1 case: the tick L1 lifts B's resume hold. Recovery fences at `PLAN_AT + 1`, the
/// survivor window closes 2 s later, and L1 holds writes for `resume_hold_millis` after that.
pub const A1_P1_RESUMES_AT: u64 = PLAN_AT + 1 + 2_000 + Budgets::SPEC_DEFAULTS.resume_hold_millis;
/// The A1/P1 case: the tick its write is submitted on B. After the resume hold: submitted
/// inside it, L1 refuses the write `ProtectionPaused` and nothing is dispatched.
pub const A1_P1_SUBMIT_AT: u64 = A1_P1_RESUMES_AT + 300;
/// The A1/P1 case: its write's request id. The first id the preload did not use: reusing one of
/// the preloaded ids under another digest is refused `RequestIdReuse`.
pub const A1_P1_REQUEST: RequestId = RequestId(A1_P1_HEAD + 1);
/// The A1/P1 case: its deadline.
pub const A1_P1_MAX_TICKS: u64 = A1_P1_SUBMIT_AT + 3_000;

/// Spike §6, A1/P1: "Delayed old dispatch after pause/reboot/new-generation activation may leave
/// quarantined bytes, never active-lineage publication, ACK, export or replication."
///
/// Authored as a **new generation activating between publish and reply** (lead ruling
/// L-R177dq), not as a grant expiring there. An expiry cannot reach the reply decision: A1
/// revalidates first and its `Fence` precedes the `Answer`, and P1 withholds every awaiting
/// reply on that fence. A new generation moves the lineage without fencing the old primary's P1
/// slot, so A1 answers its `Reply` check `Deny(GenerationChanged)`, and that deny is what P1's
/// reply arm must honour.
///
/// B and C survive at 10; A is dead. Once recovery lands and L1's resume hold lifts, one client
/// write goes to B, and a second recovery of the partition starts at the same tick, led by C
/// (the bridge's `reinspect`). B's `Reply` check is held on its hop ([`A1_P1_HOLD_MILLIS`],
/// P-3's hop delay, `NetworkOp::DelayCheck`) so that the second recovery commits in between.
///
/// What the run does (walked by hand, 2026-10-02): B publishes seq 11 in generation 2. C's
/// recovery commits generation 3 at cutoff 11 when its window closes, and when that lands on B,
/// P1 withholds the awaiting reply. The held check then decides `Fenced`, not
/// `Deny(GenerationChanged)`, and P1 ignores that stale answer. The client hears nothing from
/// generation 2. Generation 3 is committed but never activated: C, which led the recovery but
/// does not own the new generation, never proves the rebuild (row M7V-47 records why).
///
/// [`without_activation`] gives the case's healthy shape: the write publishes through A1 and
/// replies `Success`.
#[must_use]
pub fn case_a1_p1_new_generation_between_publish_and_reply() -> Scenario {
    authored(
        "case_a1_p1_new_generation_between_publish_and_reply",
        Budget {
            max_events: 2_000,
            max_ticks: A1_P1_MAX_TICKS,
        },
        vec![
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: B_NODE,
                to: Seq(A1_P1_HEAD),
            }),
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: C_NODE,
                to: Seq(A1_P1_HEAD),
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: 2_000,
            }),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: A1_P1_SUBMIT_AT - PLAN_AT,
            }),
            ScenarioOp::Client(ClientOp::Submit {
                partition: PARTITION,
                tenant: TenantId(1),
                client: ClientId(1),
                request: A1_P1_REQUEST,
                digest_id: 1,
                affinity: 1,
                expected_generation: None,
                keys: vec![KeyId(1)],
            }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: 2_000,
            }),
            ScenarioOp::Network(NetworkOp::DelayCheck {
                node: B_NODE,
                checkpoint: Checkpoint::Reply,
                by_millis: A1_P1_HOLD_MILLIS,
            }),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: A1_P1_MAX_TICKS - A1_P1_SUBMIT_AT,
            }),
        ],
    )
}

/// Remove the A1/P1 case's activation and its hold from `scenario`, leaving its healthy shape,
/// and return the removed activation. Panics naming the op if either is not where the case
/// puts it, so a caller never runs a shape it did not ask for.
pub fn without_activation(scenario: &mut Scenario) -> ScenarioOp {
    let hold = scenario.ops.remove(A1_P1_HOLD_OP);
    assert!(
        matches!(hold, ScenarioOp::Network(NetworkOp::DelayCheck { .. })),
        "A1/P1 op {A1_P1_HOLD_OP} is not the reply hold: {hold:?}"
    );
    let activation = scenario.ops.remove(A1_P1_ACTIVATE_OP);
    assert!(
        matches!(
            activation,
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors { .. })
        ),
        "A1/P1 op {A1_P1_ACTIVATE_OP} is not the activation: {activation:?}"
    );
    activation
}

/// The gap between the F1/T1 cases' successive submits. Off the host's flush grid by nothing:
/// every submit lands on a flush tick, as the A1/P1 case's does.
pub const F1_T1_STEP: u64 = 100;
/// The F1/T1/P1 case: the prior generation's identity it retries. `Synchronize` preloads seqs
/// `1..=A1_P1_HEAD` as requests `1..=A1_P1_HEAD` of client 1, so this one was applied at seq 5
/// in generation 1.
pub const F1_T1_P1_RETAINED_REQUEST: RequestId = RequestId(5);
/// The F1/T1/P1 case: the fresh write after the retry. It takes the sequence after
/// [`A1_P1_REQUEST`]'s, because the refused retry reserves none.
pub const F1_T1_P1_NEXT_REQUEST: RequestId = RequestId(A1_P1_HEAD + 2);
/// The F1/T1 case: the digest the retried identity is resubmitted under. Any value but the
/// first submit's `digest_id`.
pub const F1_T1_OTHER_DIGEST: u64 = 2;

fn submit(request: RequestId, digest_id: u64) -> ScenarioOp {
    ScenarioOp::Client(ClientOp::Submit {
        partition: PARTITION,
        tenant: TenantId(1),
        client: ClientId(1),
        request,
        digest_id,
        affinity: 1,
        expected_generation: None,
        keys: vec![KeyId(1)],
    })
}

/// The A1/P1 case's arming shape (ruling V-R37): B and C survive at [`A1_P1_HEAD`], A is dead,
/// one recovery at [`PLAN_AT`], and the cursor at [`A1_P1_SUBMIT_AT`], past L1's resume hold,
/// where a write is admitted. `tail` follows.
fn armed_then(tail: Vec<ScenarioOp>) -> Vec<ScenarioOp> {
    let mut ops = vec![
        ScenarioOp::Recovery(RecoveryOp::Synchronize {
            node: B_NODE,
            to: Seq(A1_P1_HEAD),
        }),
        ScenarioOp::Recovery(RecoveryOp::Synchronize {
            node: C_NODE,
            to: Seq(A1_P1_HEAD),
        }),
        ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
        ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
            partition: PARTITION,
            window: 2_000,
        }),
        ScenarioOp::Time(TimeOp::Advance {
            ticks: A1_P1_SUBMIT_AT - PLAN_AT,
        }),
    ];
    ops.extend(tail);
    ops
}

/// Spike §6, F1/T1/P1: "retain old-generation status mappings for 24 h ... RECOVERED_APPLIED /
/// UNKNOWN_OUTCOME / STATUS_EXPIRED, never proof of nonexecution."
///
/// Generation 1 applied requests `1..=10`; recovery activates generation 2 on B with a retained
/// status map over them. After the hold, one fresh write ([`A1_P1_REQUEST`]) publishes in
/// generation 2; then [`F1_T1_P1_RETAINED_REQUEST`] — an identity generation 1 retained, under
/// a payload the grammar cannot make equal to the preloaded one — is retried, and the kernel
/// must answer from the retained mapping (`REQUEST_ID_REUSE`, spec §8.1: never re-execute, never
/// replay transparently) rather than execute it again; then [`F1_T1_P1_NEXT_REQUEST`] shows the
/// refusal reserved no sequence.
///
/// The 24 h expiry half (`STATUS_EXPIRED`) is not in this case: the grammar has no `Status` op
/// the bridge lowers and no retire op, and the sim's F1 never emits `RetireGeneration`, so the
/// retention is modelled and never crossed here (ruling V-R37 asks for the arming shape and a
/// submit after it, which this is).
#[must_use]
pub fn case_f1_t1_p1_retained_status_24h() -> Scenario {
    authored(
        "case_f1_t1_p1_retained_status_24h",
        Budget {
            max_events: 2_000,
            max_ticks: A1_P1_MAX_TICKS,
        },
        armed_then(vec![
            submit(A1_P1_REQUEST, 1),
            ScenarioOp::Time(TimeOp::Advance { ticks: F1_T1_STEP }),
            submit(F1_T1_P1_RETAINED_REQUEST, 1),
            ScenarioOp::Time(TimeOp::Advance { ticks: F1_T1_STEP }),
            submit(F1_T1_P1_NEXT_REQUEST, 1),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: A1_P1_MAX_TICKS - A1_P1_SUBMIT_AT - 2 * F1_T1_STEP,
            }),
        ]),
    )
}

/// Spike §6, F1/T1: "same/different request digests before/after recovery and expiry."
///
/// After recovery, [`A1_P1_REQUEST`] is written once, retried under the same digest — the
/// retained answer replays and nothing is applied twice (INV-ATOM, INV-DEDUP) — and then
/// resubmitted under [`F1_T1_OTHER_DIGEST`], which is `REQUEST_ID_REUSE` and reserves nothing.
/// The before-recovery digests are generation 1's preloaded requests; the retained-status case
/// retries one of those.
#[must_use]
pub fn case_f1_t1_digest_across_recovery() -> Scenario {
    authored(
        "case_f1_t1_digest_across_recovery",
        Budget {
            max_events: 2_000,
            max_ticks: A1_P1_MAX_TICKS,
        },
        armed_then(vec![
            submit(A1_P1_REQUEST, 1),
            ScenarioOp::Time(TimeOp::Advance { ticks: F1_T1_STEP }),
            submit(A1_P1_REQUEST, 1),
            ScenarioOp::Time(TimeOp::Advance { ticks: F1_T1_STEP }),
            submit(A1_P1_REQUEST, F1_T1_OTHER_DIGEST),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: A1_P1_MAX_TICKS - A1_P1_SUBMIT_AT - 2 * F1_T1_STEP,
            }),
        ]),
    )
}

/// The M9 S0 case: when the client writes. After the fence (one tick past the plan),
/// discovery's window, L1's resume hold, and 1.3 s of room for the start record to publish
/// and L1 to resume; the probe saw L1 healthy at t7503 (`s0-probe.md`).
pub const M9_S0_SUBMIT_AT: u64 =
    PLAN_AT + 1 + 2_000 + Budgets::SPEC_DEFAULTS.resume_hold_millis + 1_300;

/// The M9 S0 case's tick budget: the write's deadline and its publication, with room.
pub const M9_S0_MAX_TICKS: u64 = M9_S0_SUBMIT_AT + 2_000;

/// The M9 S0 case's one client request.
pub const M9_S0_REQUEST: RequestId = RequestId(10);

/// The M9 S0 empty-partition world, for rows that edit the lowered plan: the survivors in
/// `survivors` hold the prior lineage through `head`, placement's plan reaches F1 at
/// [`PLAN_AT`], and the run ends at `max_ticks`.
///
/// Moved here from `m9_start_record.rs` with [`m9_s0_case_b`] (M9 close-out C1), so the host
/// comparison runs the same world as the S0 rows.
#[must_use]
pub fn m9_s0_held(survivors: &[NodeId], head: Seq, max_ticks: u64) -> Scenario {
    let mut ops: Vec<ScenarioOp> = survivors
        .iter()
        .map(|node| {
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: *node,
                to: head,
            })
        })
        .collect();
    ops.extend([
        ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
        ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
            partition: PARTITION,
            window: Budgets::SPEC_DEFAULTS.discovery_window_millis,
        }),
        ScenarioOp::Time(TimeOp::Advance {
            ticks: max_ticks - PLAN_AT,
        }),
    ]);
    authored(
        "m9_s0_empty_partition",
        Budget {
            max_events: 50_000,
            max_ticks,
        },
        ops,
    )
}

/// M9 S0 case B, the real host's shape, with no client request: the caller adds its own seeds.
/// Three empty copies; the lowering's plan is edited so the prior lineage is generation 0,
/// epoch 0, owned by B, and A is an empty survivor too, so F1 creates generation 1. The run
/// ends at `max_ticks`.
#[must_use]
pub fn m9_s0_case_b(max_ticks: u64) -> RunPlan {
    let mut plan =
        super::run::lower(&m9_s0_held(&[B_NODE, C_NODE], Seq::ZERO, max_ticks)).expect("lowers");
    let gen0 = |lineage: &mut Lineage| {
        assert_eq!(
            lineage.generation,
            Generation(1),
            "the lowering's prior lineage"
        );
        lineage.generation = Generation(0);
        lineage.owner_epoch = OwnerEpoch(0);
    };
    for (key, value) in &mut plan.control_records {
        if *key == ControlKey::Partition(PARTITION) {
            let mut record = PartitionRecord::decode(value).expect("decodes");
            record.generation = Generation(0);
            record.owner_epoch = OwnerEpoch(0);
            record.owner = B_NODE;
            *value = record.encode();
        }
    }
    for seed in &mut plan.seed {
        match &mut seed.kind {
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(p))) => {
                gen0(&mut p.anchor.lineage);
                gen0(&mut p.authority_view.lineage);
            }
            EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(f))) => {
                f.prior_generation = Generation(0);
                f.prior_owner_epoch = OwnerEpoch(0);
            }
            _ => {}
        }
    }
    for (_, _, generation, _) in &mut plan.preload_durable {
        *generation = Generation(0);
    }
    for (_, _, inventory) in &mut plan.survivors {
        gen0(&mut inventory.anchor_seen.lineage);
        assert_eq!(
            inventory.head,
            (Seq::ZERO, Digest::ROOT),
            "an empty survivor heads at the root"
        );
    }
    let a_inventory = SurvivorInventory {
        copy: CopyId(2),
        ..plan.survivors[0].2.clone()
    };
    plan.preload_durable
        .push((A_NODE, PARTITION, Generation(0), DurableSeq(0)));
    plan.survivors.push((A_NODE, PARTITION, a_inventory));
    plan
}

/// M9 S0: an empty recovery followed by a submit (lead ruling "S0 start record", Gautam chose A
/// on 2026-10-07).
///
/// B and C survive empty, A (the prior owner) is dead, and F1 recovers at cutoff 0. Before the
/// kernel's start record, nothing could be written: L1 resumes only after a copy ACKs a record,
/// and at head 0 there is none, so the write was answered `PROTECTION_PAUSED`. Now the start
/// record is published at seq 1 and the client's write at seq 2. It is the corpus's only history
/// that recovers an empty prefix, so it is the only one in which every oracle sees a publish
/// with no client behind it.
#[must_use]
pub fn case_m9_s0_empty_recovery_then_submit() -> Scenario {
    authored(
        "case_m9_s0_empty_recovery_then_submit",
        Budget {
            max_events: 2_000,
            max_ticks: M9_S0_MAX_TICKS,
        },
        vec![
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: B_NODE,
                to: Seq::ZERO,
            }),
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: C_NODE,
                to: Seq::ZERO,
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: Budgets::SPEC_DEFAULTS.discovery_window_millis,
            }),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: M9_S0_SUBMIT_AT - PLAN_AT,
            }),
            submit(M9_S0_REQUEST, 1),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: M9_S0_MAX_TICKS - M9_S0_SUBMIT_AT,
            }),
        ],
    )
}

/// The M9 D2 case: when the cut links heal. After the commit (fence plus discovery's window) and
/// before the rebuild's first sync deadline, which falls one window after the commit.
pub const M9_D2_HEAL_AT: u64 = 3_000;

/// The M9 D2 case: when the client writes. The partition goes `Active` at the first sync deadline
/// after the heal (commit plus one window), and L1 is healthy by t10000 in the observed run.
pub const M9_D2_SUBMIT_AT: u64 = 12_000;

/// The M9 D2 case's tick budget: the write's deadline and its publication, with room.
pub const M9_D2_MAX_TICKS: u64 = M9_D2_SUBMIT_AT + 2_000;

/// M9 D2: an empty partition recovered read-only while two copies are cut off, then healed and
/// written to (lead ruling "S0 D2", Gautam chose option 1 on 2026-10-07).
///
/// B alone survives, empty, and its links to C and A are cut from the start. F1 commits
/// `ReadOnly` at cutoff 0. Before the D2 fix nothing ever pinned the rebuild: no copy is behind
/// an empty prefix, so R1 starts no catch-up and reports no `CopyCaughtUp`, and the partition
/// stayed read-only for ever. Now F1 pins `(0, ROOT)` at the commit and asks every copy to sync;
/// the two cut-off copies cannot answer, and the deadline asks them again after the heal. The
/// partition goes `Active`, the start record is published at seq 1 and the write at seq 2.
#[must_use]
pub fn case_m9_d2_read_only_empty_heals_then_submit() -> Scenario {
    authored(
        "case_m9_d2_read_only_empty_heals_then_submit",
        Budget {
            max_events: 6_000,
            max_ticks: M9_D2_MAX_TICKS,
        },
        vec![
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: B_NODE,
                to: Seq::ZERO,
            }),
            ScenarioOp::Network(NetworkOp::Partition {
                set_a: vec![B_NODE],
                set_b: vec![C_NODE, A_NODE],
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: Budgets::SPEC_DEFAULTS.discovery_window_millis,
            }),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: M9_D2_HEAL_AT - PLAN_AT,
            }),
            ScenarioOp::Network(NetworkOp::Heal),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: M9_D2_SUBMIT_AT - M9_D2_HEAL_AT,
            }),
            // The S0 case's request: the M9 sim rows read one client identity.
            submit(M9_S0_REQUEST, 1),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: M9_D2_MAX_TICKS - M9_D2_SUBMIT_AT,
            }),
        ],
    )
}

/// The M9 D3 case: when A's cut links heal. After the commit and the start record's publication
/// (about t2500), and before the rebuild's second sync deadline.
pub const M9_D3_A_BACK_AT: u64 = 4_000;

/// The M9 D3 case's two client writes. L1 resumes its 5 s hold when the partition goes `Active`
/// (about t4010), and was healthy by t10000 in the probe's run (`s0-probe.md` E6).
pub const M9_D3_SUBMIT_AT: [u64; 2] = [12_000, 15_000];

/// The M9 D3 case's second request. The first is [`M9_S0_REQUEST`].
pub const M9_D3_SECOND_REQUEST: RequestId = RequestId(11);

/// The M9 D3 case's tick budget: the second write's deadline and its publication, with room.
pub const M9_D3_MAX_TICKS: u64 = M9_D3_SUBMIT_AT[1] + 2_000;

/// M9 D3: an empty partition that starts with one copy cut off, then gets it back (lead ruling
/// "S0 D3", Gautam chose option 1 on 2026-10-07).
///
/// B and C survive empty; A's links are cut until [`M9_D3_A_BACK_AT`]. F1 commits `DegradedRf2`
/// at cutoff 0, and the start record is published at seq 1 on B and C. A returns, catches up to
/// seq 1, and F1 re-emits the same result as `Active`. That re-emit used to rewind R1 and P1 to
/// the selected cutoff, 0: R1 then had no head to send, so no copy's lag ever dropped and L1
/// never resumed; P1's published position went from 1 back to 0, so a read was refused. Now a
/// re-emit in the generation already served keeps what is held above the cutoff, and both
/// writes are published, at seq 2 and 3.
#[must_use]
pub fn case_m9_d3_degraded_empty_gets_a_copy_back_then_writes() -> Scenario {
    authored(
        "case_m9_d3_degraded_empty_gets_a_copy_back_then_writes",
        Budget {
            max_events: 8_000,
            max_ticks: M9_D3_MAX_TICKS,
        },
        vec![
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: B_NODE,
                to: Seq::ZERO,
            }),
            ScenarioOp::Recovery(RecoveryOp::Synchronize {
                node: C_NODE,
                to: Seq::ZERO,
            }),
            ScenarioOp::Network(NetworkOp::Partition {
                set_a: vec![A_NODE],
                set_b: vec![B_NODE, C_NODE],
            }),
            ScenarioOp::Time(TimeOp::Advance { ticks: PLAN_AT }),
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors {
                partition: PARTITION,
                window: Budgets::SPEC_DEFAULTS.discovery_window_millis,
            }),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: M9_D3_A_BACK_AT - PLAN_AT,
            }),
            ScenarioOp::Network(NetworkOp::Heal),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: M9_D3_SUBMIT_AT[0] - M9_D3_A_BACK_AT,
            }),
            submit(M9_S0_REQUEST, 1),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: M9_D3_SUBMIT_AT[1] - M9_D3_SUBMIT_AT[0],
            }),
            submit(M9_D3_SECOND_REQUEST, 1),
            ScenarioOp::Time(TimeOp::Advance {
                ticks: M9_D3_MAX_TICKS - M9_D3_SUBMIT_AT[1],
            }),
        ],
    )
}
