//! The runner bridge: a grammar [`Scenario`] lowered onto the real `rdb_sim::harness` runner, and
//! the recorded trace handed to the oracle.
//!
//! Nothing here simulates anything. [`lower`] turns a scenario into the harness's own reproducer,
//! [`RunPlan`], and [`run`] hands that plan to [`Runner`] — the same loop, dispatcher and
//! providers `f1_scenarios.rs` drives — then judges the trace with [`Oracle`]. The harness
//! already has seeds, preloads, survivors and transfers; this file only says which grammar op
//! becomes which of them.
//!
//! # The lowering table
//!
//! A cursor starts at tick 0 and only [`TimeOp::Advance`] moves it. Ops before the first advance
//! are **initial state**; the rest happen at the cursor.
//!
//! | Op | Becomes |
//! |---|---|
//! | `Budget{max_events, max_ticks}` | `RunLimits{max_events, deadline: max_ticks}`. The runner counts **popped** events; the trace records several lines per pop (six `ModuleDispatch` offers alone), so a trace is longer than `max_events` |
//! | `topology.placements` | the trace header's initial placement, each at `config_version_0`. The oracle reads a node's role from it, and without it INV-PUB counts no regular ACK |
//! | `Time(Advance{ticks})` | the cursor moves by `ticks` |
//! | `Recovery(Synchronize{node, to})`, initial | a survivor: the prior lineage's canonical records `1..=to` preloaded on `node`, durable there, and placed as its inventory |
//! | `Recovery(Transfer{..})`, initial | a [`TransferPlan`]: the harness plays the transfer out when discovery queries that copy |
//! | `Time(Pause{node, ticks})` on a transfer's node | the transfer's `stop_at` is the cursor. It must last the rest of the budget: a transfer cannot resume |
//! | `Recovery(InspectSurvivors{partition, window})` | F1 on the partition's primary node gets placement's `Plan` at the cursor and the prior owner's fence one tick later. The prior owner is the highest placed node that neither survives nor transfers, and its `partitions/{id}` record is control revision 1. A `window` other than the default is a `DiscoveryWindow` budget override |
//! | `Client(Submit{..})` | a `Submit` seeded on the partition's primary node at the cursor |
//! | `Client(Retry{..})`, its `Submit` lowered earlier | that `Submit` again, same identity, with the retry's `digest_id` as its body, at the cursor, plus a step recording `fault_injected{Client, RetainedDedupHit}` (same `digest_id`) or `{Client, ChangedDigest}` (different). After the dedup retention window it is [`Unlowerable`]: `ExpiredDedup` has no lowering |
//! | `Client(Retry{..})`, its `Submit` gone | nothing runs; a step records `op_skipped{ReferentGone}` and no fault (design.md §4.3) |
//! | `Network(Deliver / Drop / Duplicate{from, to})` | a [`ScenarioStep`] at the cursor planning the next `from -> to` frame: `PlanNext` with `Deliver{0}`, `Drop`, or `Duplicate{0, DUPLICATE_GAP_MILLIS}`, its fault tag `taken: None` and no line. `from == to` is refused: the sim network has no self-link |
//! | `Network(Partition{set_a, set_b})` | one step per pair across the sides at the cursor setting the link `Partitioned`, with no line. A node on both sides is refused |
//! | `Network(Heal)` | one step per node pair at the cursor setting the link `Up`. **No** `schedule_phase{Healed}` line (lead ruling L-R182m): that arms INV-LIVE and INV-ISO and belongs to the V-R40 Healed slice, so a generated seed arms no liveness check |
//! | any `InspectSurvivors`, at the end | the host (see [`host`]): a flush on every node every [`HOST_FLUSH_EVERY_MILLIS`] from the latest cutoff F1 can choose through the deadline, and A1's first `AcquireDue` on each primary [`ACQUIRE_AFTER_CUTOFF_MILLIS`] after that. Neither is a grammar op: no kernel emits either, so a recovered scenario without them never resumes L1 or holds a grant |
//!
//! **Every other op is [`Unlowerable`], by index, never dropped.** A silently skipped op would
//! make a scenario that asked for a fault read as a scenario that survived it. The reason names
//! what is missing, so the list of unlowerable ops is also the list of harness asks.
//!
//! The prior lineage is always generation 1, owner epoch 1, at the topology's
//! `config_version_0`, chained from [`Digest::ROOT`] by `storage::history::canonical_history`:
//! real envelopes the `SendEnvelopes` provider can serve and verify.
//!
//! # Frames
//!
//! Nothing here builds a `transport::Frame`. If a lowering ever needs one, it is built in one
//! helper in this file, so R1's B-R58 `Frame.sender` lands in one place.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::authority::{Authority, AuthorityTimer, Held};
use rdb_core::contracts::authority::{
    AuthorityView, DenyReason, FencingProof, Lineage, Revocation,
};
use rdb_core::contracts::control::{ControlKey, ReadOutcome};
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::event::{Budgets, ClientEvent, EventKind, KernelEvent};
use rdb_core::contracts::ids::ReplicaRole;
use rdb_core::contracts::ids::{
    AffinityId, AuthorityGeneration, BootId, ClientId, CorrelationId, DurableSeq, Generation,
    GrantId, NodeId, OwnerEpoch, PartitionId, RequestId, RequestIdentity, Revision, Seq, TenantId,
    TimerVersion,
};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::recovery::{
    Candidate, LineageAnchor, RecoveryEvent, RecoveryPlan, SurvivorInventory,
};
use rdb_core::contracts::time::{Tick, TimerFired};
use rdb_core::contracts::trace::{
    BoundaryId, BudgetName, KernelNote, SkipReason, TopologyEntry, Trace, TraceKind,
};
use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest};
use rdb_core::contracts::version::API_VERSION;
use rdb_core::recovery::MAX_WINDOW_EXTENSIONS;
use rdb_sim::harness::hop::HopDelay;
use rdb_sim::harness::manifest::BudgetOverride;
use rdb_sim::harness::run::{
    RunLimits, RunPlan, RunReport, Runner, ScenarioLine, ScenarioStep, SeedEvent, StepAction,
    StopReason,
};
use rdb_sim::harness::trace::validate;
use rdb_sim::harness::transfer::TransferPlan;
use rdb_sim::sim::cluster::{ClusterConfig, NodeSpec, PartitionSpec};
use rdb_sim::sim::network::{Delivery, LinkState, NetworkOp as SimNetworkOp};
use rdb_sim::storage::history::{canonical_history, CanonicalHistory};

use super::grammar::{ClientOp, NetworkOp, RecoveryOp, Scenario, ScenarioOp, TimeOp, Topology};
use crate::support::oracle::{Oracle, Report};

/// Every node runs at boot 1: no grammar op reboots one yet (`Storage(Reopen)` is unlowerable).
pub const BOOT: BootId = BootId(1);

/// How long a lowered `Submit` has left, in milliseconds. The grammar carries no deadline.
pub const SUBMIT_REMAINING_MILLIS: u64 = 1_000;

/// Placement's status retention for a lowered recovery plan, in milliseconds.
pub const RETENTION_MILLIS: u64 = 1_000;

/// How often a recovered scenario's host flusher runs on each node, in milliseconds. The same
/// period as R1's keepalive (B-R60); the only requirement is that a copy walked up after the
/// cutoff is flushed within the run.
pub const HOST_FLUSH_EVERY_MILLIS: u64 = 100;

/// How long after the latest possible cutoff a recovered scenario's primary first wakes A1, in
/// milliseconds: room for R1's rebuild walk and F1's activation CAS, which follow the close at
/// once in every measured run (close + 10 in `case_f1_r1_discovery_window`).
pub const ACQUIRE_AFTER_CUTOFF_MILLIS: u64 = 500;

/// The partition a network or time step is recorded under. Neither belongs to a partition; this
/// is the one the generator's producer table aims every boundary at (`gen::producer`).
pub const FAULT_PARTITION: PartitionId = PartitionId(0);

/// How long after the first copy a lowered `Duplicate`'s second copy lands, in milliseconds. One,
/// so the two copies are two pops rather than one tick's tie.
pub const DUPLICATE_GAP_MILLIS: u64 = 1;

/// Why a scenario has no [`RunPlan`]. Never a partial plan: the whole scenario lowers or none of
/// it does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unlowerable {
    /// The op that could not be lowered, by index into `Scenario::ops`. `None` when the
    /// scenario as a whole cannot (its topology, say).
    pub op_index: Option<usize>,
    /// What is missing, stated as the ask it is.
    pub reason: &'static str,
}

impl Unlowerable {
    const fn at(op_index: usize, reason: &'static str) -> Self {
        Self {
            op_index: Some(op_index),
            reason,
        }
    }
}

/// One scenario, run: the plan it lowered to, the runner's report, the validated trace, and the
/// oracle's report on that trace.
#[derive(Debug)]
pub struct ScenarioRun {
    /// The reproducer the scenario lowered to.
    pub plan: RunPlan,
    /// How the loop stopped and what it counted.
    pub report: RunReport,
    /// The recorded trace, validated.
    pub trace: Trace,
    /// The oracle's verdicts on `trace`.
    pub oracle: Report,
}

/// A scenario lowered with its staged ops: what [`attempt`] runs.
#[derive(Debug)]
pub struct Lowered {
    /// Everything that lowers before the run starts.
    pub plan: RunPlan,
    /// The ops applied to the run at their tick, in op order.
    pub stages: Vec<Staged>,
}

/// One staged op: applied after the run has popped everything before `at`, and before anything
/// at `at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Staged {
    /// The tick it acts at: the cursor when it was lowered.
    pub at: u64,
    /// Its index in `Scenario::ops`.
    pub op_index: usize,
    /// What it does.
    pub stage: Stage,
}

/// What a staged op does to the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stage {
    /// `Network(DelayCheck)`: [`Dispatcher::delay_hop`] from this tick on.
    ///
    /// [`Dispatcher::delay_hop`]: rdb_sim::harness::dispatch::Dispatcher::delay_hop
    Hold(HopDelay),
    /// A second `InspectSurvivors` of a partition (ruling V-R34). It runs on the next surviving
    /// copy, and everything it names is read from the run at its tick: the `partitions/{id}`
    /// record the first recovery committed, the owner's held grant, and the selected prefix of
    /// the generation it fences. None of it is known when the scenario is lowered.
    Reinspect {
        /// The partition.
        partition: PartitionId,
        /// The surviving copy F1 runs on this time.
        leader: NodeId,
        /// The `Plan` seed's correlation; the fence's is the next one.
        correlation: u64,
    },
}

/// The members of `partition`, copy ids in placement order, every one at [`BOOT`].
#[must_use]
pub fn config(topology: &Topology, partition: PartitionId) -> PartitionConfig {
    let members = topology
        .placements
        .iter()
        .filter(|placement| placement.partition == partition)
        .enumerate()
        .map(|(copy, placement)| Member {
            copy: CopyId(u8::try_from(copy).unwrap_or(u8::MAX)),
            node: placement.node,
            boot: BOOT,
            role: placement.role,
        })
        .collect();
    PartitionConfig::new(partition, topology.config_version_0, members)
}

/// The sim's cluster for `topology`: nodes `1..=nodes`, one failure domain and one core set each.
#[must_use]
pub fn cluster(topology: &Topology) -> ClusterConfig {
    let partitions: BTreeSet<PartitionId> = topology
        .placements
        .iter()
        .map(|placement| placement.partition)
        .collect();
    ClusterConfig {
        nodes: (1..=u32::from(topology.nodes))
            .map(|node| NodeSpec {
                node: NodeId(node),
                boot: BOOT,
                failure_domain: u16::try_from(node).unwrap_or(u16::MAX),
                core_sets: 1,
            })
            .collect(),
        partitions: partitions
            .into_iter()
            .map(|partition| PartitionSpec {
                partition,
                config: config(topology, partition),
            })
            .collect(),
        initial_config_version: topology.config_version_0,
    }
}

/// The lineage every lowered recovery recovers from.
#[must_use]
pub const fn prior(partition: PartitionId) -> Lineage {
    Lineage {
        partition,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    }
}

/// Lower `scenario` onto the harness (module docs, "The lowering table").
///
/// # Errors
///
/// [`Unlowerable`] for the first op, in list order, the table has no row for, or whose
/// preconditions do not hold. A scenario with a staged op is refused at that op: a plan alone
/// cannot carry it, and [`attempt`] runs it ([`lower_staged`]).
pub fn lower(scenario: &Scenario) -> Result<RunPlan, Unlowerable> {
    let lowered = lower_staged(scenario)?;
    match lowered.stages.first() {
        Some(staged) => Err(Unlowerable::at(
            staged.op_index,
            "a staged op reads the run at its tick, so it runs only through attempt",
        )),
        None => Ok(lowered.plan),
    }
}

/// [`lower`], keeping the ops that act on the run as it stands at their tick ([`Stage`]).
///
/// # Errors
///
/// [`Unlowerable`] exactly as [`lower`], staged ops excepted.
pub fn lower_staged(scenario: &Scenario) -> Result<Lowered, Unlowerable> {
    let topology = &scenario.topology;
    let mut state = Lowering::default();
    let mut stages = Vec::new();
    let mut plan = RunPlan::new(cluster(topology));
    plan.provenance = scenario.provenance.clone();
    plan.generator_version = scenario.generator_version;
    plan.partitions = topology.partitions;
    // The header's initial placement. Without it the oracle's model finds no role for any node,
    // so INV-PUB counts no regular ACK and fires on a correct publication.
    plan.topology = topology
        .placements
        .iter()
        .map(|placement| TopologyEntry {
            partition: placement.partition,
            node: placement.node,
            role: placement.role,
            config_version: topology.config_version_0,
        })
        .collect();
    plan.limits = RunLimits {
        max_events: scenario.budget.max_events,
        deadline: Tick(scenario.budget.max_ticks),
    };

    for (index, op) in scenario.ops.iter().enumerate() {
        match op {
            ScenarioOp::Time(TimeOp::Advance { ticks }) => {
                state.cursor = state.cursor.saturating_add(*ticks);
                state.started = true;
            }
            ScenarioOp::Recovery(RecoveryOp::Synchronize { node, to }) => {
                if state.started {
                    return Err(Unlowerable::at(
                        index,
                        "Synchronize after the first Advance: bringing a live copy up to a \
                         sequence needs a catch-up provider op the harness does not take",
                    ));
                }
                let partition = only_partition(topology, *node).ok_or(Unlowerable::at(
                    index,
                    "Synchronize names a node placed in no partition, or in several",
                ))?;
                if state.holds.insert((partition, *node), *to).is_some() {
                    return Err(Unlowerable::at(index, "a second Synchronize of one copy"));
                }
            }
            ScenarioOp::Recovery(RecoveryOp::Transfer {
                partition,
                node,
                received,
                advertised,
                per_step,
                step_ticks,
            }) => {
                if state.started {
                    return Err(Unlowerable::at(
                        index,
                        "Transfer after the first Advance: the harness declares transfers \
                         before the run",
                    ));
                }
                let copy = copy_of(topology, *partition, *node).ok_or(Unlowerable::at(
                    index,
                    "Transfer from a node not placed in the partition",
                ))?;
                if state.holds.contains_key(&(*partition, *node))
                    || plan
                        .transfers
                        .iter()
                        .any(|(p, t)| *p == *partition && t.holder == *node)
                {
                    return Err(Unlowerable::at(
                        index,
                        "a copy that both survives and transfers, or transfers twice",
                    ));
                }
                plan.transfers.push((
                    *partition,
                    TransferPlan {
                        copy,
                        holder: *node,
                        advertised: *advertised,
                        from: *received,
                        per_step: *per_step,
                        step_millis: *step_ticks,
                        stop_at: None,
                        stall_at: None,
                    },
                ));
            }
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors { partition, window })
                if state.recovered.iter().any(|(p, _, _)| p == partition) =>
            {
                let stage = reinspect(&plan, &mut state, topology, *partition, *window)
                    .map_err(|reason| Unlowerable::at(index, reason))?;
                stages.push(Staged {
                    at: state.cursor,
                    op_index: index,
                    stage,
                });
            }
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors { partition, window }) => {
                inspect(&mut plan, &mut state, topology, *partition, *window)
                    .map_err(|reason| Unlowerable::at(index, reason))?;
            }
            ScenarioOp::Network(NetworkOp::DelayCheck {
                node,
                checkpoint,
                by_millis,
            }) => stages.push(Staged {
                at: state.cursor,
                op_index: index,
                stage: Stage::Hold(HopDelay {
                    node: *node,
                    checkpoint: *checkpoint,
                    by_millis: *by_millis,
                }),
            }),
            ScenarioOp::Client(
                submit @ ClientOp::Submit {
                    partition,
                    tenant,
                    client,
                    request,
                    digest_id,
                    ..
                },
            ) => {
                let node = primary(topology, *partition).ok_or(Unlowerable::at(
                    index,
                    "Submit to a partition with no primary placement",
                ))?;
                let at = state.cursor;
                let body = txn_request(submit, *digest_id);
                let seed = state.seed(
                    at,
                    node,
                    *partition,
                    EventKind::Client(ClientEvent::Submit(body)),
                );
                plan.seed.push(seed);
                state.submits.insert(
                    (*partition, *tenant, *client, *request),
                    (at, submit.clone()),
                );
            }
            ScenarioOp::Client(ClientOp::Retry {
                partition,
                tenant,
                client,
                request,
                digest_id,
            }) => {
                let node = primary(topology, *partition).ok_or(Unlowerable::at(
                    index,
                    "Retry to a partition with no primary placement",
                ))?;
                let op_index = u32::try_from(index).expect("an op index fits u32");
                let at = state.cursor;
                let Some((submitted_at, submit)) = state
                    .submits
                    .get(&(*partition, *tenant, *client, *request))
                    .cloned()
                else {
                    // The request it retries is gone (a reducer deleted it): skipped, and no
                    // fault is claimed for it (design.md §4.3, M7V-22).
                    plan.steps.push(ScenarioStep {
                        at: Tick(at),
                        node,
                        partition: *partition,
                        action: StepAction::Mark,
                        line: Some(ScenarioLine::Skipped {
                            op_index,
                            reason: SkipReason::ReferentGone,
                        }),
                        taken: None,
                    });
                    continue;
                };
                if at.saturating_sub(submitted_at) >= Budgets::SPEC_DEFAULTS.dedup_retention_millis
                {
                    return Err(Unlowerable::at(
                        index,
                        "Retry after the dedup retention window: ExpiredDedup has no lowering",
                    ));
                }
                let ClientOp::Submit {
                    digest_id: original,
                    ..
                } = submit
                else {
                    unreachable!("only a Submit is kept as a retry's referent");
                };
                let boundary = if original == *digest_id {
                    BoundaryId::RetainedDedupHit
                } else {
                    BoundaryId::ChangedDigest
                };
                let body = txn_request(&submit, *digest_id);
                let seed = state.seed(
                    at,
                    node,
                    *partition,
                    EventKind::Client(ClientEvent::Submit(body)),
                );
                plan.seed.push(seed);
                plan.steps.push(ScenarioStep {
                    at: Tick(at),
                    node,
                    partition: *partition,
                    action: StepAction::Mark,
                    line: Some(ScenarioLine::Fault { boundary, op_index }),
                    taken: None,
                });
            }
            ScenarioOp::Time(TimeOp::Pause { node, ticks }) => {
                let cursor = state.cursor;
                let Some((_, transfer)) = plan
                    .transfers
                    .iter_mut()
                    .find(|(_, transfer)| transfer.holder == *node)
                else {
                    return Err(Unlowerable::at(
                        index,
                        "Pause of a node that sends no transfer: the harness has no timed pause \
                         provider",
                    ));
                };
                if transfer.stop_at.is_some() {
                    return Err(Unlowerable::at(index, "a second Pause of one transfer"));
                }
                if cursor.saturating_add(*ticks) < scenario.budget.max_ticks {
                    return Err(Unlowerable::at(
                        index,
                        "a Pause that ends inside the budget: a harness transfer cannot resume",
                    ));
                }
                transfer.stop_at = Some(Tick(cursor));
            }
            ScenarioOp::Network(
                op @ (NetworkOp::Deliver { from, to }
                | NetworkOp::Drop { from, to }
                | NetworkOp::Duplicate { from, to }),
            ) => {
                if from == to {
                    return Err(Unlowerable::at(
                        index,
                        "a network op from a node to itself: the sim network has no self-link",
                    ));
                }
                let delivery = match op {
                    NetworkOp::Drop { .. } => Delivery::Drop,
                    NetworkOp::Duplicate { .. } => Delivery::Duplicate {
                        delay_millis: 0,
                        second_delay_millis: DUPLICATE_GAP_MILLIS,
                    },
                    _ => Delivery::Deliver { delay_millis: 0 },
                };
                plan.steps.push(ScenarioStep {
                    at: Tick(state.cursor),
                    node: *to,
                    partition: FAULT_PARTITION,
                    action: StepAction::Network(SimNetworkOp::PlanNext {
                        from: *from,
                        to: *to,
                        delivery,
                    }),
                    // A plan is deferred: no line at apply (L-R182m). No fault tag either: a
                    // weighted draw names no boundary, and a boundary is never inferred from
                    // the op's family (critic F1).
                    line: None,
                    taken: None,
                });
            }
            // Delivery restored on every link. No `schedule_phase{Healed}` line: that arms
            // INV-LIVE and INV-ISO and belongs to the V-R40 Healed slice (L-R182m), so a
            // generated seed arms no liveness check, and the coverage artifact says so.
            ScenarioOp::Network(NetworkOp::Heal) => {
                for a in 1..=u32::from(topology.nodes) {
                    for b in a + 1..=u32::from(topology.nodes) {
                        plan.steps.push(ScenarioStep {
                            at: Tick(state.cursor),
                            node: NodeId(a),
                            partition: FAULT_PARTITION,
                            action: StepAction::Network(SimNetworkOp::SetLink {
                                a: NodeId(a),
                                b: NodeId(b),
                                state: LinkState::Up,
                            }),
                            line: None,
                            taken: None,
                        });
                    }
                }
            }
            // Every link from one side to the other cut at the cursor, until a `Heal`. No line,
            // as for `Heal`: a cut is the harness's own fault, and no boundary is inferred.
            ScenarioOp::Network(NetworkOp::Partition { set_a, set_b }) => {
                for &a in set_a {
                    for &b in set_b {
                        if a == b {
                            return Err(Unlowerable::at(
                                index,
                                "a node on both sides of a partition: the sim network has no \
                                 self-link",
                            ));
                        }
                        plan.steps.push(ScenarioStep {
                            at: Tick(state.cursor),
                            node: a,
                            partition: FAULT_PARTITION,
                            action: StepAction::Network(SimNetworkOp::SetLink {
                                a,
                                b,
                                state: LinkState::Partitioned,
                            }),
                            line: None,
                            taken: None,
                        });
                    }
                }
            }
            _ => {
                return Err(Unlowerable::at(
                    index,
                    "no lowering for this op: no harness provider or scenario step applies it \
                     at its tick",
                ))
            }
        }
    }

    host(&mut plan, &mut state, topology, scenario.budget.max_ticks);
    place_survivors(&mut plan, &state, topology);
    Ok(Lowered { plan, stages })
}

/// Lower and run `scenario`, validate its trace, and judge it.
///
/// # Errors
///
/// [`Unlowerable`] exactly as [`lower`]. A harness error is not a scenario's fault and panics
/// with the harness's own message.
pub fn run(scenario: &Scenario) -> Result<ScenarioRun, Unlowerable> {
    match attempt(scenario) {
        Ok(run) => Ok(run),
        Err(NoRun::Unlowerable(unlowerable)) => Err(unlowerable),
        Err(NoRun::Harness(message)) => panic!("{message}"),
    }
}

/// Why [`attempt`] produced no judged run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoRun {
    /// The scenario has no plan, exactly as [`lower`] says.
    Unlowerable(Unlowerable),
    /// The harness refused the plan, stopped with an error, or recorded a trace its own
    /// validator rejects. The message is the harness's.
    Harness(String),
}

/// [`run`] for a caller that must survive a candidate the harness refuses: the reducer's
/// executor, which hands the harness op lists nobody authored.
///
/// # Errors
///
/// [`NoRun`], never a panic.
pub fn attempt(scenario: &Scenario) -> Result<ScenarioRun, NoRun> {
    let Lowered { plan, stages } = lower_staged(scenario).map_err(NoRun::Unlowerable)?;
    let mut runner = Runner::new(&plan)
        .map_err(|e| NoRun::Harness(format!("the harness refuses the lowered plan: {e:?}")))?;
    let stop = |e| NoRun::Harness(format!("the harness stops the lowered plan: {e:?}"));
    // One segment per staged tick, then the rest. The event budget is the scenario's, spent
    // across all of them. A segment that stops for any reason but its own deadline ends the run
    // there: the stages after it never apply.
    let mut report: Option<RunReport> = None;
    let mut cut_short = false;
    for staged in &stages {
        if staged.at > 0 {
            let segment = runner
                .run(remaining(plan.limits, report.as_ref(), Tick(staged.at - 1)))
                .map_err(stop)?;
            cut_short = !matches!(
                segment.stop,
                StopReason::DeadlineReached { .. } | StopReason::QueueEmpty
            );
            report = Some(merged(report, segment));
            if cut_short {
                break;
            }
        }
        match &staged.stage {
            Stage::Hold(hop) => runner.dispatcher_mut().delay_hop(*hop),
            Stage::Reinspect {
                partition,
                leader,
                correlation,
            } => reinspect_now(
                &mut runner,
                &scenario.topology,
                staged.at,
                (*partition, *leader, *correlation),
            )
            .map_err(NoRun::Harness)?,
        }
    }
    if !cut_short {
        let last = runner
            .run(remaining(
                plan.limits,
                report.as_ref(),
                plan.limits.deadline,
            ))
            .map_err(stop)?;
        report = Some(merged(report, last));
    }
    let mut report = report.expect("at least one segment ran");
    // A later segment is handed only what is left of the budget, so its stop names that rest.
    // The run's own cap is the one a reader asked for (tester-q1 T2).
    if let StopReason::EventBudgetExhausted { max_events, .. } = &mut report.stop {
        *max_events = plan.limits.max_events;
    }
    let trace = runner
        .finish()
        .map_err(|e| NoRun::Harness(format!("the harness hands back no trace: {e:?}")))?;
    validate(&trace).map_err(|e| NoRun::Harness(format!("a malformed trace: {e:?}")))?;
    let oracle = Oracle::new().judge(&trace);
    Ok(ScenarioRun {
        plan,
        report,
        trace,
        oracle,
    })
}

/// `limits` with the events `spent` has already used taken off, up to `deadline`.
fn remaining(limits: RunLimits, spent: Option<&RunReport>, deadline: Tick) -> RunLimits {
    RunLimits {
        max_events: limits
            .max_events
            .saturating_sub(spent.map_or(0, |report| report.events_consumed)),
        deadline,
    }
}

/// Two segments' reports as one run's: counts summed, replies in order, the later stop.
fn merged(earlier: Option<RunReport>, later: RunReport) -> RunReport {
    let Some(mut out) = earlier else {
        return later;
    };
    out.stop = later.stop;
    out.events_consumed += later.events_consumed;
    out.steps_offered += later.steps_offered;
    out.effects_offered += later.effects_offered;
    out.recorded = later.recorded;
    out.last_tick = out.last_tick.max(later.last_tick);
    for slot in 0..out.answered.len() {
        out.answered[slot] += later.answered[slot];
        out.declined[slot] += later.declined[slot];
    }
    out.replies.extend(later.replies);
    out
}

/// What the op walk has declared so far.
#[derive(Debug, Default)]
struct Lowering {
    /// The tick the next timed op lands on.
    cursor: u64,
    /// Whether the cursor has moved: initial-state ops are refused after that.
    started: bool,
    /// Survivors: `(partition, node) -> head`.
    holds: BTreeMap<(PartitionId, NodeId), Seq>,
    /// The last correlation handed out.
    correlation: u64,
    /// Every inspected partition: `(partition, primary node, latest tick its cutoff can be
    /// chosen)`. What [`host`] schedules the flusher and A1's first wake from.
    recovered: Vec<(PartitionId, NodeId, u64)>,
    /// Every lowered `Submit`, by `(partition, tenant, client, request)`, with its tick: what a
    /// `Retry` resends, or finds gone.
    submits: BTreeMap<(PartitionId, TenantId, ClientId, RequestId), (u64, ClientOp)>,
    /// Every copy a staged second recovery runs on: `(partition, node)`.
    releaders: Vec<(PartitionId, NodeId)>,
}

impl Lowering {
    /// A seed at `at`, with the next correlation.
    fn seed(
        &mut self,
        at: u64,
        node: NodeId,
        partition: PartitionId,
        kind: EventKind,
    ) -> SeedEvent {
        self.correlation += 1;
        SeedEvent {
            at: Tick(at),
            node,
            boot: BOOT,
            partition,
            correlation: CorrelationId(self.correlation),
            kind,
        }
    }
}

/// The request a lowered `Submit` carries, with `digest_id` as its body: a `Retry` resends its
/// `Submit` with its own `digest_id`, so a different one is a different request digest.
fn txn_request(submit: &ClientOp, digest_id: u64) -> TxnRequest {
    let ClientOp::Submit {
        tenant,
        client,
        request,
        affinity,
        expected_generation,
        keys,
        ..
    } = submit
    else {
        unreachable!("txn_request is called with a Submit only");
    };
    let affinity = AffinityId(*affinity);
    let value = Bytes::copy_from_slice(&digest_id.to_be_bytes());
    TxnRequest {
        api_version: API_VERSION,
        identity: RequestIdentity {
            tenant: *tenant,
            client: *client,
            request: *request,
        },
        affinity,
        expected_generation: *expected_generation,
        remaining_millis: SUBMIT_REMAINING_MILLIS,
        conditions: Vec::new(),
        mutations: keys
            .iter()
            .map(|key| Mutation::Put {
                key: scoped_key(*tenant, affinity, &key.0.to_be_bytes()),
                value: value.clone(),
                expected_version: None,
            })
            .collect(),
    }
}

/// Placement's recovery plan for `partition`, anchored at `anchor`: every member a candidate,
/// every non-shadow copy required for the rebuild, and the authority view placement hands the
/// new owner on the anchor's lineage.
fn recovery_plan(
    topology: &Topology,
    partition: PartitionId,
    anchor: LineageAnchor,
) -> RecoveryPlan {
    let config = config(topology, partition);
    RecoveryPlan {
        anchor,
        candidates: config
            .members
            .iter()
            .map(|member| Candidate {
                copy: member.copy,
                primary_eligible: true,
                healthy: true,
                within_capacity: true,
                has_valid_grant: true,
            })
            .collect(),
        rebuild_required: config
            .members
            .iter()
            .filter(|member| member.role != ReplicaRole::Shadow)
            .map(|member| member.copy)
            .collect(),
        authority_view: AuthorityView {
            lineage: anchor.lineage,
            grant_id: GrantId(1),
            boot_id: BOOT,
            authority_generation: AuthorityGeneration(1),
            config_version: topology.config_version_0,
            authority_seq: 1,
            valid_through_tick: Tick(u64::MAX),
            past_horizon: DenyReason::NoGrant,
        },
        retention_millis: RETENTION_MILLIS,
        config,
    }
}

/// Lower a second `InspectSurvivors` of `partition` to a [`Stage::Reinspect`] on the next
/// surviving copy in placement order that has not led a recovery of it yet.
///
/// Walked by hand on the A1/P1 case (2026-10-02). That leader commits the next generation, and
/// the owner F1 picks (the selected copy's holder, not the leader) installs its lineage, but the
/// generation never activates: the leader's rebuild hears no `SyncProven` after its commit, and
/// a write in it is refused `RecoveryReadOnly`. Led by the owner instead, the run stops at R1's
/// `CopyAheadOnControl`, which `harness::dispatch::deliver::kernel` still refuses (I1, owed).
fn reinspect(
    plan: &RunPlan,
    state: &mut Lowering,
    topology: &Topology,
    partition: PartitionId,
    window: u64,
) -> Result<Stage, &'static str> {
    let led = |node: NodeId| {
        state
            .recovered
            .iter()
            .any(|(p, leader, _)| *p == partition && *leader == node)
            || state.releaders.contains(&(partition, node))
    };
    let leader = config(topology, partition)
        .members
        .iter()
        .map(|member| member.node)
        .find(|node| state.holds.contains_key(&(partition, *node)) && !led(*node))
        .ok_or("a second InspectSurvivors with no surviving copy left to lead it")?;
    let overridden = plan
        .overrides
        .iter()
        .any(|o| o.name == BudgetName::DiscoveryWindow && o.millis == window);
    if window != Budgets::SPEC_DEFAULTS.discovery_window_millis && !overridden {
        return Err("a second InspectSurvivors with a discovery window the first did not set");
    }
    state.releaders.push((partition, leader));
    state.correlation += 2;
    Ok(Stage::Reinspect {
        partition,
        leader,
        correlation: state.correlation - 1,
    })
}

/// Apply a [`Stage::Reinspect`] at `at`: F1 on `leader` gets placement's `Plan` at `at`, and
/// the current owner's fence one tick later.
///
/// Every value is read from the run, never assumed. The `partitions/{id}` record names the
/// generation being fenced, its owner and epoch, and the revision the fence proves at. The
/// owner's A1 names the grant and boot it holds. The `RecoveredFact` note that created the
/// generation names its selected prefix, which is the anchor every survivor of that generation
/// now reports (`Dispatcher::answer_inventory`).
fn reinspect_now(
    runner: &mut Runner,
    topology: &Topology,
    at: u64,
    stage: (PartitionId, NodeId, u64),
) -> Result<(), String> {
    let (partition, leader, correlation) = stage;
    let ReadOutcome::Found { revision, value } =
        runner.control_mut().get(ControlKey::Partition(partition))
    else {
        return Err(format!(
            "no partitions/{} record to fence at tick {at}",
            partition.0
        ));
    };
    let record = PartitionRecord::decode(&value)
        .ok_or_else(|| format!("partitions/{} does not decode", partition.0))?;
    let held = runner
        .dispatcher()
        .authority(record.owner)
        .and_then(Authority::held)
        .map(Held::identity)
        .ok_or_else(|| format!("the owner n{} holds no grant at tick {at}", record.owner.0))?;
    let selected = runner
        .recorded()
        .iter()
        .rev()
        .find_map(|event| match &event.kind {
            TraceKind::KernelNoted {
                note: KernelNote::RecoveredFact { result },
                ..
            } if event.partition == partition && result.new_generation == record.generation => {
                Some(result.selected)
            }
            _ => None,
        })
        .ok_or_else(|| {
            format!(
                "no recovered fact created generation {} of partition {}",
                record.generation.0, partition.0
            )
        })?;
    let anchor = LineageAnchor {
        lineage: selected.root,
        base_seq: selected.cutoff_seq,
        base_digest: selected.cutoff_digest,
    };
    let fence = FencingProof {
        partition,
        prior_generation: record.generation,
        prior_owner_epoch: record.owner_epoch,
        prior_grant_id: held.grant,
        prior_boot_id: held.boot,
        revocation: Revocation::DurableDrain {
            ack_revision: revision,
        },
        control_revision: revision,
        decision_tick: Tick(at + 1),
    };
    tracing::info!(
        partition = partition.0,
        leader = leader.0,
        owner = record.owner.0,
        generation = record.generation.0,
        revision = revision.0,
        cutoff = selected.cutoff_seq.0,
        "second recovery staged"
    );
    let seed = |at: u64, correlation: u64, event: RecoveryEvent| SeedEvent {
        at: Tick(at),
        node: leader,
        boot: BOOT,
        partition,
        correlation: CorrelationId(correlation),
        kind: EventKind::Kernel(KernelEvent::Recovery(event)),
    };
    let plan = recovery_plan(topology, partition, anchor);
    for event in [
        seed(at, correlation, RecoveryEvent::Plan(Box::new(plan))),
        seed(
            at + 1,
            correlation + 1,
            RecoveryEvent::FenceProven(Box::new(fence)),
        ),
    ] {
        runner
            .queue(&event)
            .map_err(|e| format!("the second recovery's seed is refused: {e:?}"))?;
    }
    Ok(())
}

/// The one partition `node` is placed in.
fn only_partition(topology: &Topology, node: NodeId) -> Option<PartitionId> {
    let partitions: BTreeSet<PartitionId> = topology
        .placements
        .iter()
        .filter(|placement| placement.node == node)
        .map(|placement| placement.partition)
        .collect();
    (partitions.len() == 1).then(|| *partitions.iter().next().expect("one"))
}

/// The node placed as `partition`'s primary.
fn primary(topology: &Topology, partition: PartitionId) -> Option<NodeId> {
    topology
        .placements
        .iter()
        .find(|placement| {
            placement.partition == partition && placement.role == ReplicaRole::Primary
        })
        .map(|placement| placement.node)
}

/// `node`'s copy in `partition`.
fn copy_of(topology: &Topology, partition: PartitionId, node: NodeId) -> Option<CopyId> {
    config(topology, partition)
        .members
        .iter()
        .find(|member| member.node == node)
        .map(|member| member.copy)
}

/// The prior lineage's records `1..=head`.
fn history(topology: &Topology, partition: PartitionId, head: u64) -> CanonicalHistory {
    canonical_history(prior(partition), topology.config_version_0, head)
        .expect("a canonical history")
}

/// `InspectSurvivors`: the prior owner's record, placement's plan at the cursor, the fence one
/// tick later, and the window as a budget override when it is not the default.
fn inspect(
    plan: &mut RunPlan,
    state: &mut Lowering,
    topology: &Topology,
    partition: PartitionId,
    window: u64,
) -> Result<(), &'static str> {
    let leader = primary(topology, partition)
        .ok_or("InspectSurvivors on a partition with no primary placement")?;
    let config = config(topology, partition);
    let owner = config
        .members
        .iter()
        .rev()
        .find(|member| {
            !state.holds.contains_key(&(partition, member.node))
                && !plan
                    .transfers
                    .iter()
                    .any(|(p, t)| *p == partition && t.holder == member.node)
        })
        .map(|member| member.node)
        .ok_or(
            "InspectSurvivors with no dead prior owner: every placed copy survives or transfers",
        )?;
    if plan
        .control_records
        .iter()
        .any(|(key, _)| *key == ControlKey::Partition(partition))
    {
        return Err("a second InspectSurvivors of one partition");
    }
    let record = PartitionRecord {
        partition,
        owner,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        config_version: topology.config_version_0,
        lifecycle: PartitionLifecycle::Serving,
    };
    plan.control_records
        .push((ControlKey::Partition(partition), record.encode()));
    if window != Budgets::SPEC_DEFAULTS.discovery_window_millis {
        plan.overrides.push(BudgetOverride {
            name: BudgetName::DiscoveryWindow,
            millis: window,
        });
    }

    let anchor = LineageAnchor {
        lineage: prior(partition),
        base_seq: Seq(0),
        base_digest: Digest::ROOT,
    };
    let recovery = recovery_plan(topology, partition, anchor);
    let fence_at = state.cursor.saturating_add(1);
    let fence = FencingProof {
        partition,
        prior_generation: Generation(1),
        prior_owner_epoch: OwnerEpoch(1),
        prior_grant_id: GrantId(1),
        prior_boot_id: BOOT,
        revocation: Revocation::DurableDrain {
            ack_revision: Revision(1),
        },
        control_revision: Revision(1),
        decision_tick: Tick(fence_at),
    };
    let at = state.cursor;
    let plan_seed = state.seed(
        at,
        leader,
        partition,
        EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::Plan(Box::new(
            recovery,
        )))),
    );
    let fence_seed = state.seed(
        fence_at,
        leader,
        partition,
        EventKind::Kernel(KernelEvent::Recovery(RecoveryEvent::FenceProven(Box::new(
            fence,
        )))),
    );
    plan.seed.push(plan_seed);
    plan.seed.push(fence_seed);

    // Discovery opens at the fence and closes on a deadline; only a transfer extends it, at most
    // `MAX_WINDOW_EXTENSIONS` times. So this is the latest the cutoff can be chosen.
    let extensions = if plan.transfers.iter().any(|(p, _)| *p == partition) {
        u64::from(MAX_WINDOW_EXTENSIONS)
    } else {
        0
    };
    let cutoff_by = fence_at.saturating_add(window.saturating_mul(1 + extensions));
    state.recovered.push((partition, leader, cutoff_by));
    Ok(())
}

/// A recovered scenario's host: what no kernel emits, scheduled after the latest cutoff.
///
/// * **The flusher.** Every node flushes every [`HOST_FLUSH_EVERY_MILLIS`] from the latest
///   cutoff through `deadline`. R1 walks the barrier's copies up after the cutoff, and without a
///   flush after that walk a copy reports durable 0 for ever, so L1 never resumes (L-R177gf).
///   Never before the cutoff: a flush there cannot make the walked-up copies durable, and it could
///   move a transfer's durable head under discovery.
/// * **A1's first wake.** One `AcquireDue` per primary node, [`ACQUIRE_AFTER_CUTOFF_MILLIS`]
///   after its partition's latest cutoff, so the grant's reload reads the record F1's
///   activation CAS wrote. Nothing arms the first `AcquireDue` (A1's own docs), and the sim's
///   control store delivers no watch unless told to, so a grant taken earlier would keep the
///   prior owner's record. Only the primary: the dead prior owner acquiring would put its
///   fenced identity (grant 1, boot 1) back.
///
/// A scenario that recovers nothing gets neither, so nothing here moves one that does not.
fn host(plan: &mut RunPlan, state: &mut Lowering, topology: &Topology, deadline: u64) {
    let Some(start) = state.recovered.iter().map(|(_, _, by)| *by).max() else {
        return;
    };
    let mut at = start;
    while at <= deadline {
        for node in 1..=u32::from(topology.nodes) {
            plan.flushes.push((Tick(at), NodeId(node)));
        }
        at = at.saturating_add(HOST_FLUSH_EVERY_MILLIS);
    }

    let mut wakes: BTreeMap<NodeId, (PartitionId, u64)> = BTreeMap::new();
    for &(partition, leader, by) in &state.recovered {
        let at = by.saturating_add(ACQUIRE_AFTER_CUTOFF_MILLIS);
        let wake = wakes.entry(leader).or_insert((partition, at));
        if at > wake.1 {
            *wake = (partition, at);
        }
    }
    for (node, (partition, at)) in wakes {
        if at > deadline {
            continue;
        }
        let seed = state.seed(
            at,
            node,
            partition,
            EventKind::Timer(TimerFired {
                id: AuthorityTimer::Acquire.id(),
                version: TimerVersion(0),
                scheduled_at: Tick(at),
            }),
        );
        plan.seed.push(seed);
    }
}

/// Every survivor: its batches preloaded, durable at its head, and placed as its inventory.
fn place_survivors(plan: &mut RunPlan, state: &Lowering, topology: &Topology) {
    for (&(partition, node), &head) in &state.holds {
        let history = history(topology, partition, head.0);
        plan.preloads
            .extend(history.batches.iter().map(|batch| (node, batch.clone())));
        plan.preload_durable
            .push((node, partition, Generation(1), DurableSeq(head.0)));
        let copy = copy_of(topology, partition, node).expect("a Synchronize names a placed node");
        plan.survivors.push((
            node,
            partition,
            SurvivorInventory {
                copy,
                anchor_seen: LineageAnchor {
                    lineage: prior(partition),
                    base_seq: Seq(0),
                    base_digest: Digest::ROOT,
                },
                head: (head, history.digest(head.0)),
                ladder: (0..=head.0)
                    .map(|seq| (Seq(seq), history.digest(seq)))
                    .collect(),
                quarantined: None,
            },
        ));
    }
}

/// How many of each trace kind, and of each kernel note and dispatch outcome, a trace holds.
/// For a tester reading a run and for the determinism log, never for a verdict.
#[must_use]
pub fn census(trace: &Trace) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in &trace.events {
        let name = match &event.kind {
            TraceKind::KernelNoted { module, note, .. } => {
                format!("KernelNoted/{module:?}/{}", head(&format!("{note:?}")))
            }
            TraceKind::ModuleDispatch {
                module, outcome, ..
            } => format!(
                "ModuleDispatch/{module:?}/{}",
                head(&format!("{outcome:?}"))
            ),
            other => head(&format!("{other:?}")).to_owned(),
        };
        *counts.entry(name).or_insert(0) += 1;
    }
    counts
}

/// A digest of the whole trace, stable across runs of one build: FNV-1a over its JSON. Two runs
/// of one scenario must print the same value.
#[must_use]
pub fn fingerprint(trace: &Trace) -> u64 {
    let text = serde_json::to_vec(trace).expect("a trace serializes");
    text.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn head(text: &str) -> &str {
    let end = text.find([' ', '{', '(']).unwrap_or(text.len());
    &text[..end]
}
