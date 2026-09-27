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
//! | `Time(Advance{ticks})` | the cursor moves by `ticks` |
//! | `Recovery(Synchronize{node, to})`, initial | a survivor: the prior lineage's canonical records `1..=to` preloaded on `node`, durable there, and placed as its inventory |
//! | `Recovery(Transfer{..})`, initial | a [`TransferPlan`]: the harness plays the transfer out when discovery queries that copy |
//! | `Time(Pause{node, ticks})` on a transfer's node | the transfer's `stop_at` is the cursor. It must last the rest of the budget: a transfer cannot resume |
//! | `Recovery(InspectSurvivors{partition, window})` | F1 on the partition's primary node gets placement's `Plan` at the cursor and the prior owner's fence one tick later. The prior owner is the highest placed node that neither survives nor transfers, and its `partitions/{id}` record is control revision 1. A `window` other than the default is a `DiscoveryWindow` budget override |
//! | `Client(Submit{..})` | a `Submit` seeded on the partition's primary node at the cursor |
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
use rdb_core::contracts::authority::{
    AuthorityView, DenyReason, FencingProof, Lineage, Revocation,
};
use rdb_core::contracts::control::ControlKey;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::event::{Budgets, ClientEvent, EventKind, KernelEvent};
use rdb_core::contracts::ids::ReplicaRole;
use rdb_core::contracts::ids::{
    AffinityId, AuthorityGeneration, BootId, CorrelationId, DurableSeq, Generation, GrantId,
    NodeId, OwnerEpoch, PartitionId, RequestIdentity, Revision, Seq,
};
use rdb_core::contracts::membership::{CopyId, Member, PartitionConfig};
use rdb_core::contracts::recovery::{
    Candidate, LineageAnchor, RecoveryEvent, RecoveryPlan, SurvivorInventory,
};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{BudgetName, Trace, TraceKind};
use rdb_core::contracts::txn::{scoped_key, Mutation, TxnRequest};
use rdb_core::contracts::version::API_VERSION;
use rdb_sim::harness::manifest::BudgetOverride;
use rdb_sim::harness::run::{RunLimits, RunPlan, RunReport, Runner, SeedEvent};
use rdb_sim::harness::trace::validate;
use rdb_sim::harness::transfer::TransferPlan;
use rdb_sim::sim::cluster::{ClusterConfig, NodeSpec, PartitionSpec};
use rdb_sim::storage::history::{canonical_history, CanonicalHistory};

use super::grammar::{ClientOp, RecoveryOp, Scenario, ScenarioOp, TimeOp, Topology};
use crate::support::oracle::{Oracle, Report};

/// Every node runs at boot 1: no grammar op reboots one yet (`Storage(Reopen)` is unlowerable).
pub const BOOT: BootId = BootId(1);

/// How long a lowered `Submit` has left, in milliseconds. The grammar carries no deadline.
pub const SUBMIT_REMAINING_MILLIS: u64 = 1_000;

/// Placement's status retention for a lowered recovery plan, in milliseconds.
pub const RETENTION_MILLIS: u64 = 1_000;

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
/// preconditions do not hold.
pub fn lower(scenario: &Scenario) -> Result<RunPlan, Unlowerable> {
    let topology = &scenario.topology;
    let mut state = Lowering::default();
    let mut plan = RunPlan::new(cluster(topology));
    plan.provenance = scenario.provenance.clone();
    plan.generator_version = scenario.generator_version;
    plan.partitions = topology.partitions;
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
            ScenarioOp::Recovery(RecoveryOp::InspectSurvivors { partition, window }) => {
                inspect(&mut plan, &mut state, topology, *partition, *window)
                    .map_err(|reason| Unlowerable::at(index, reason))?;
            }
            ScenarioOp::Client(ClientOp::Submit {
                partition,
                tenant,
                client,
                request,
                digest_id,
                affinity,
                expected_generation,
                keys,
            }) => {
                let node = primary(topology, *partition).ok_or(Unlowerable::at(
                    index,
                    "Submit to a partition with no primary placement",
                ))?;
                let affinity = AffinityId(*affinity);
                let value = Bytes::copy_from_slice(&digest_id.to_be_bytes());
                let request = TxnRequest {
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
                };
                let at = state.cursor;
                let seed = state.seed(
                    at,
                    node,
                    *partition,
                    EventKind::Client(ClientEvent::Submit(request)),
                );
                plan.seed.push(seed);
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
            _ => {
                return Err(Unlowerable::at(
                    index,
                    "no lowering for this op: the harness takes provider ops only before the \
                     first pop, so a timed fault has no RunPlan field",
                ))
            }
        }
    }

    place_survivors(&mut plan, &state, topology);
    Ok(plan)
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
    let plan = lower(scenario).map_err(NoRun::Unlowerable)?;
    let mut runner = Runner::new(&plan)
        .map_err(|e| NoRun::Harness(format!("the harness refuses the lowered plan: {e:?}")))?;
    let report = runner
        .run(plan.limits)
        .map_err(|e| NoRun::Harness(format!("the harness stops the lowered plan: {e:?}")))?;
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
    let recovery = RecoveryPlan {
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
            lineage: prior(partition),
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
    };
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
    Ok(())
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
