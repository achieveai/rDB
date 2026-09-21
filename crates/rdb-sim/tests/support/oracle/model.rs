//! The tiny visible-state model (design §2.1).
//!
//! It holds **declared facts**, not decisions. There is no transaction application, no prefix
//! selection, no ancestry repair and no admission arithmetic here: everything below is something
//! the trace said, filed where a checker can look it up in one step.
//!
//! Two facts come from the **environment** rather than from the kernel (design §2.5): the
//! config-versioned topology (the header's initial snapshot plus every
//! [`TraceKind::TopologyChange`]) and the recorded [`TraceKind::DurabilityAdvance`] stream. Those
//! two are what stop a kernel-computed label — a shadow that calls itself a regular copy, a
//! buffered prefix that calls itself durable — from being self-consistently wrong.

use std::collections::{BTreeMap, BTreeSet};

use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::ids::{
    BootId, ClientId, ConfigVersion, CorrelationId, Generation, NodeId, PartitionId, ReplicaRole,
    RequestId, Seq, TenantId,
};
use rdb_core::contracts::trace::{
    ApplyOutcome, BoundaryId, CapabilityState, DurabilityClass, KeyId, LineageSource, PackageId,
    ProtectionPhase, SchedulePhase, TraceEvent, TraceHeader, TraceKind, Version,
};

/// The dedup identity, as the oracle keys it. `RequestIdentity` in `ids.rs` is the same three
/// fields; this is a local copy so the model's maps key on a type the oracle owns.
pub type Identity = (TenantId, ClientId, RequestId);

/// One member of [`TraceKind`], as a closed set the signature can carry.
///
/// C0 has no such type — [`TraceKind`] is a data enum — and the reducer's acceptance predicate
/// needs one value per variant it can compare and print. Declared here, enumerated, so a new
/// [`TraceKind`] variant without a member fails to compile rather than folding into a catch-all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TraceEventKind {
    /// [`TraceKind::ClientSubmit`].
    ClientSubmit,
    /// [`TraceKind::AdmissionDecision`].
    AdmissionDecision,
    /// [`TraceKind::AuthorityDecision`].
    AuthorityDecision,
    /// [`TraceKind::BatchApply`].
    BatchApply,
    /// [`TraceKind::ReplicationSend`].
    ReplicationSend,
    /// [`TraceKind::ReplicationAck`].
    ReplicationAck,
    /// [`TraceKind::ReplicationAckDelivered`].
    ReplicationAckDelivered,
    /// [`TraceKind::DurabilityAdvance`].
    DurabilityAdvance,
    /// [`TraceKind::Publish`].
    Publish,
    /// [`TraceKind::ClientOutcomeReported`].
    ClientOutcomeReported,
    /// [`TraceKind::Read`].
    Read,
    /// [`TraceKind::DedupRecord`].
    DedupRecord,
    /// [`TraceKind::LineageRoot`].
    LineageRoot,
    /// [`TraceKind::RecoveryDecision`].
    RecoveryDecision,
    /// [`TraceKind::Quarantine`].
    Quarantine,
    /// [`TraceKind::ProtectionState`].
    ProtectionState,
    /// [`TraceKind::VersionCheck`].
    VersionCheck,
    /// [`TraceKind::FaultInjected`].
    FaultInjected,
    /// [`TraceKind::TopologyChange`].
    TopologyChange,
    /// [`TraceKind::SchedulePhaseChanged`].
    SchedulePhaseChanged,
    /// [`TraceKind::Capability`].
    Capability,
    /// [`TraceKind::OpSkipped`].
    OpSkipped,
    /// [`TraceKind::ControlInteraction`].
    ControlInteraction,
    /// [`TraceKind::FamilyReload`].
    FamilyReload,
}

impl TraceEventKind {
    /// Every kind, in [`TraceKind`] declaration order.
    pub const ALL: [Self; 24] = [
        Self::ClientSubmit,
        Self::AdmissionDecision,
        Self::AuthorityDecision,
        Self::BatchApply,
        Self::ReplicationSend,
        Self::ReplicationAck,
        Self::ReplicationAckDelivered,
        Self::DurabilityAdvance,
        Self::Publish,
        Self::ClientOutcomeReported,
        Self::Read,
        Self::DedupRecord,
        Self::LineageRoot,
        Self::RecoveryDecision,
        Self::Quarantine,
        Self::ProtectionState,
        Self::VersionCheck,
        Self::FaultInjected,
        Self::TopologyChange,
        Self::SchedulePhaseChanged,
        Self::Capability,
        Self::OpSkipped,
        Self::ControlInteraction,
        Self::FamilyReload,
    ];

    /// The kind of `kind`. Exhaustive on purpose: no `_` arm.
    #[must_use]
    pub const fn of(kind: &TraceKind) -> Self {
        match kind {
            TraceKind::ClientSubmit { .. } => Self::ClientSubmit,
            TraceKind::AdmissionDecision { .. } => Self::AdmissionDecision,
            TraceKind::AuthorityDecision { .. } => Self::AuthorityDecision,
            TraceKind::BatchApply { .. } => Self::BatchApply,
            TraceKind::ReplicationSend { .. } => Self::ReplicationSend,
            TraceKind::ReplicationAck { .. } => Self::ReplicationAck,
            TraceKind::ReplicationAckDelivered { .. } => Self::ReplicationAckDelivered,
            TraceKind::DurabilityAdvance { .. } => Self::DurabilityAdvance,
            TraceKind::Publish { .. } => Self::Publish,
            TraceKind::ClientOutcomeReported { .. } => Self::ClientOutcomeReported,
            TraceKind::Read { .. } => Self::Read,
            TraceKind::DedupRecord { .. } => Self::DedupRecord,
            TraceKind::LineageRoot { .. } => Self::LineageRoot,
            TraceKind::RecoveryDecision { .. } => Self::RecoveryDecision,
            TraceKind::Quarantine { .. } => Self::Quarantine,
            TraceKind::ProtectionState { .. } => Self::ProtectionState,
            TraceKind::VersionCheck { .. } => Self::VersionCheck,
            TraceKind::FaultInjected { .. } => Self::FaultInjected,
            TraceKind::TopologyChange { .. } => Self::TopologyChange,
            TraceKind::SchedulePhaseChanged { .. } => Self::SchedulePhaseChanged,
            TraceKind::Capability { .. } => Self::Capability,
            TraceKind::OpSkipped { .. } => Self::OpSkipped,
            TraceKind::ControlInteraction { .. } => Self::ControlInteraction,
            TraceKind::FamilyReload { .. } => Self::FamilyReload,
        }
    }

    /// The JSONL `@m` name, as VA-7 writes it.
    #[must_use]
    pub const fn snake_case(self) -> &'static str {
        match self {
            Self::ClientSubmit => "client_submit",
            Self::AdmissionDecision => "admission_decision",
            Self::AuthorityDecision => "authority_decision",
            Self::BatchApply => "batch_apply",
            Self::ReplicationSend => "replication_send",
            Self::ReplicationAck => "replication_ack",
            Self::ReplicationAckDelivered => "replication_ack_delivered",
            Self::DurabilityAdvance => "durability_advance",
            Self::Publish => "publish",
            Self::ClientOutcomeReported => "client_outcome_reported",
            Self::Read => "read",
            Self::DedupRecord => "dedup_record",
            Self::LineageRoot => "lineage_root",
            Self::RecoveryDecision => "recovery_decision",
            Self::Quarantine => "quarantine",
            Self::ProtectionState => "protection_state",
            Self::VersionCheck => "version_check",
            Self::FaultInjected => "fault_injected",
            Self::TopologyChange => "topology_change",
            Self::SchedulePhaseChanged => "schedule_phase_changed",
            Self::Capability => "capability",
            Self::OpSkipped => "op_skipped",
            Self::ControlInteraction => "control_interaction",
            Self::FamilyReload => "family_reload",
        }
    }
}

/// The acknowledgement rule the oracle **derives** from `required_copy_set.len()`.
///
/// Ruling V-R20 (1): never read from a trace field. The derived value is authoritative and keys
/// the `derived_quorum_rule` coverage cell (design §2.3, §6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DerivedQuorumRule {
    /// Three copies: two regular secondary acknowledgements.
    Rf3,
    /// Two survivors under spec §8.3: `min_regular_acks` 1-of-1 (ruling B-R3).
    DegradedRf2,
}

impl DerivedQuorumRule {
    /// Both rules, for the coverage axis.
    pub const ALL: [Self; 2] = [Self::Rf3, Self::DegradedRf2];

    /// The rule a required-copy set of this size implies, or `None` for a shape no rule covers.
    ///
    /// `None` is INV-PUB's `required_copy_set_shape` violation, not a fixture check (design
    /// §2.3, ruling by the lead on critic T-39).
    #[must_use]
    pub const fn of_len(len: usize) -> Option<Self> {
        match len {
            2 => Some(Self::DegradedRf2),
            3 => Some(Self::Rf3),
            _ => None,
        }
    }

    /// How many regular-secondary acknowledgements a publication needs under this rule.
    #[must_use]
    pub const fn min_regular_acks(self) -> usize {
        match self {
            Self::Rf3 => 2,
            Self::DegradedRf2 => 1,
        }
    }

    /// The coverage-cell name.
    #[must_use]
    pub const fn cell(self) -> &'static str {
        match self {
            Self::Rf3 => "Rf3",
            Self::DegradedRf2 => "DegradedRf2",
        }
    }
}

/// One recorded `batch_apply`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRec {
    /// The node that applied it.
    pub node: NodeId,
    /// Its role in that apply.
    pub role: ReplicaRole,
    /// This record's digest.
    pub entry_digest: Digest,
    /// The keys and versions the batch wrote.
    pub key_versions: Vec<(KeyId, Version)>,
    /// How it ended.
    pub outcome: ApplyOutcome,
    /// The request it belongs to.
    pub correlation: CorrelationId,
}

/// One recorded acknowledgement, at the secondary where it was generated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckRec {
    /// The role the acknowledgement **claimed**. Never trusted on its own (design §2.5).
    pub claimed_role: ReplicaRole,
    /// The configuration it was pinned to.
    pub config_version: ConfigVersion,
    /// The watermark it holds. Evidence for every sequence at or below it (convention 2).
    pub contiguous_seq: Seq,
    /// How strongly.
    pub durability: DurabilityClass,
    /// Whether the append was accepted.
    pub accepted: bool,
}

/// One recorded `admission_decision{outcome=Admitted}` — the pin INV-PUB resolves against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionRec {
    /// The sequence it was admitted at.
    pub admitted_seq: Option<Seq>,
    /// The configuration the required-copy set was pinned by.
    pub config_version: ConfigVersion,
    /// The required-copy set in force at that moment.
    pub required_copies: Vec<NodeId>,
    /// When.
    pub tick: u64,
}

/// One recorded `protection_state`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtectionRec {
    /// The phase.
    pub phase: ProtectionPhase,
    /// Age of the oldest transaction not durably on the required copies.
    pub oldest_unsafe_age_ms: u64,
    /// The required-copy set.
    pub required_copy_set: Vec<NodeId>,
    /// The configuration the set is pinned to.
    pub config_version: ConfigVersion,
    /// The prefix admission paused after.
    pub paused_prefix_seq: Seq,
    /// The exact durable barrier required to resume.
    pub resume_barrier_seq: Seq,
    /// When this state was declared.
    pub tick: u64,
}

/// One recorded `lineage_root`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RootRec {
    /// The lineage it starts.
    pub generation: Generation,
    /// Its base position.
    pub base_seq: Seq,
    /// The digest at that position.
    pub base_digest: Digest,
    /// The lineage it descends from.
    pub predecessor_generation: Option<Generation>,
    /// The predecessor position it cuts off at.
    pub predecessor_cutoff: Option<Seq>,
    /// Where it came from.
    pub source: LineageSource,
    /// The `event_id` it was declared at, so "since the last recovery root" is an ordering.
    pub at_event: u64,
}

/// The authority window one decision declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityRec {
    /// The lineage.
    pub generation: Generation,
    /// Earliest tick the grant is valid from.
    pub valid_from_tick: u64,
    /// The grant's expiry tick.
    pub expiry_tick: u64,
    /// How it came out.
    pub outcome: rdb_core::contracts::trace::AuthorityOutcome,
}

/// Everything the oracle knows about one partition.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PartitionModel {
    /// The only client-visible state: `key -> (version, the sequence that published it)`.
    pub published: BTreeMap<KeyId, (Version, Seq)>,
    /// The highest published position, per generation in force.
    pub published_seq: Option<Seq>,
    /// Every `batch_apply`, keyed by its lineage position.
    pub applies: BTreeMap<(Generation, Seq), ApplyRec>,
    /// Append-only lineage roots.
    pub roots: Vec<RootRec>,
    /// Acknowledgements, keyed by the acknowledging `(node, boot)`.
    pub acks: BTreeMap<(NodeId, BootId), AckRec>,
    /// `node -> (boot, durable_seq)` from `durability_advance{outcome=Synced}`. The only thing
    /// that grounds a `Durable` label (design §2.5).
    pub durable: BTreeMap<NodeId, (BootId, Seq)>,
    /// Admissions, by the correlation that carries the request through the write path.
    pub admissions: BTreeMap<CorrelationId, AdmissionRec>,
    /// Every `protection_state`, in order.
    pub protection: Vec<ProtectionRec>,
    /// Authority decisions, by the `event_id` a `publish` back-references.
    pub authority: BTreeMap<u64, AuthorityRec>,
    /// The dedup identity behind a correlation, from `client_submit`.
    pub identity_of: BTreeMap<CorrelationId, Identity>,
    /// Correlations admitted and not yet answered.
    pub inflight: BTreeSet<CorrelationId>,
    /// Correlations that reached a terminal `client_outcome`.
    pub terminal: BTreeSet<CorrelationId>,
    /// Whether any request was admitted on this partition at all.
    pub admitted_any: bool,
}

impl PartitionModel {
    /// The latest declared `protection_state`, when there is one.
    #[must_use]
    pub fn last_protection(&self) -> Option<&ProtectionRec> {
        self.protection.last()
    }

    /// The most recent recovery root, when there is one.
    #[must_use]
    pub fn last_recovery_root(&self) -> Option<&RootRec> {
        self.roots
            .iter()
            .rev()
            .find(|root| root.source == LineageSource::Recovery)
    }

    /// The recorded `entry_digest` at one lineage position.
    #[must_use]
    pub fn digest_at(&self, generation: Generation, seq: Seq) -> Option<Digest> {
        self.applies
            .get(&(generation, seq))
            .map(|rec| rec.entry_digest)
    }

    /// Whether `node` holds `seq` durably at `boot`, from the recorded flush stream.
    #[must_use]
    pub fn durable_through(&self, node: NodeId, seq: Seq) -> bool {
        self.durable
            .get(&node)
            .is_some_and(|(_, durable_seq)| *durable_seq >= seq)
    }

    /// Whether `node` at `boot` holds `seq` durably.
    #[must_use]
    pub fn durable_at_boot(&self, node: NodeId, boot: BootId, seq: Seq) -> bool {
        self.durable
            .get(&node)
            .is_some_and(|(b, durable_seq)| *b == boot && *durable_seq >= seq)
    }

    /// The acknowledgement `node` generated at `boot`, when it covers `seq`.
    #[must_use]
    pub fn ack_covering(&self, node: NodeId, boot: BootId, seq: Seq) -> Option<&AckRec> {
        self.acks
            .get(&(node, boot))
            .filter(|ack| ack.accepted && ack.contiguous_seq >= seq)
    }

    /// Every `(node, boot)` whose accepted acknowledgement covers `seq`.
    pub fn holders(&self, seq: Seq) -> impl Iterator<Item = (NodeId, BootId, &AckRec)> {
        self.acks
            .iter()
            .filter(move |(_, ack)| ack.accepted && ack.contiguous_seq >= seq)
            .map(|((node, boot), ack)| (*node, *boot, ack))
    }
}

/// The whole model: declared facts, filed for one-step lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Model {
    /// `(partition, config_version) -> node -> role`, from the header snapshot plus every
    /// `topology_change`. The environment's declaration, never the kernel's belief.
    pub topology: BTreeMap<(PartitionId, ConfigVersion), BTreeMap<NodeId, ReplicaRole>>,
    /// What each package reported at trace start.
    pub capabilities: BTreeMap<PackageId, CapabilityState>,
    /// Every boundary the run injected, for the signature and the coverage matrix.
    pub faults: BTreeSet<BoundaryId>,
    /// One entry per partition the trace mentions.
    pub parts: BTreeMap<PartitionId, PartitionModel>,
    /// The schedule's character. `Healed` is what arms INV-LIVE and INV-ISO.
    pub phase: SchedulePhase,
    /// The `event_id` the fold has reached.
    pub at_event: u64,
}

impl Model {
    /// A model holding only what the header declared.
    #[must_use]
    pub fn new(header: &TraceHeader) -> Self {
        let mut topology: BTreeMap<(PartitionId, ConfigVersion), BTreeMap<NodeId, ReplicaRole>> =
            BTreeMap::new();
        for entry in &header.topology {
            topology
                .entry((entry.partition, entry.config_version))
                .or_default()
                .insert(entry.node, entry.role);
        }
        Self {
            topology,
            capabilities: BTreeMap::new(),
            faults: BTreeSet::new(),
            parts: BTreeMap::new(),
            phase: SchedulePhase::Chaotic,
            at_event: 0,
        }
    }

    /// The partition's facts, or the empty set if the trace has not mentioned it.
    #[must_use]
    pub fn part(&self, partition: PartitionId) -> &PartitionModel {
        static EMPTY: std::sync::OnceLock<PartitionModel> = std::sync::OnceLock::new();
        self.parts
            .get(&partition)
            .unwrap_or_else(|| EMPTY.get_or_init(PartitionModel::default))
    }

    /// The role the **environment** gives `node` on `partition` under `config_version`.
    ///
    /// Resolved from the topology in force at that configuration, falling back to the closest
    /// earlier configuration, because a `topology_change` only restates the placement it changed
    /// (design §2.5, ruling V-R12). `None` means the environment never placed that node, which is
    /// a fact a checker must handle rather than guess at.
    #[must_use]
    pub fn role_at(
        &self,
        partition: PartitionId,
        config_version: ConfigVersion,
        node: NodeId,
    ) -> Option<ReplicaRole> {
        self.topology
            .range(..=(partition, config_version))
            .rev()
            .take_while(|((p, _), _)| *p == partition)
            .find_map(|(_, placement)| placement.get(&node).copied())
    }

    /// Fold `event` into the facts. Called **after** every checker has observed it.
    #[allow(clippy::too_many_lines)]
    pub fn absorb(&mut self, event: &TraceEvent) {
        self.at_event = event.event_id.0;
        let partition = event.partition;
        let node = event.node;
        let boot = event.boot;
        let correlation = event.correlation;

        match &event.kind {
            TraceKind::Capability { package, state } => {
                self.capabilities.insert(*package, *state);
                return;
            }
            TraceKind::FaultInjected { boundary, .. } => {
                self.faults.insert(*boundary);
                return;
            }
            TraceKind::SchedulePhaseChanged { phase, .. } => {
                self.phase = *phase;
                return;
            }
            TraceKind::TopologyChange {
                config_version,
                nodes,
            } => {
                let placement = self
                    .topology
                    .entry((partition, *config_version))
                    .or_default();
                for (node, role) in nodes {
                    placement.insert(*node, *role);
                }
                return;
            }
            _ => {}
        }

        let part = self.parts.entry(partition).or_default();
        match &event.kind {
            TraceKind::ClientSubmit {
                request,
                tenant,
                client,
                ..
            } => {
                part.identity_of
                    .insert(correlation, (*tenant, *client, *request));
            }
            TraceKind::AdmissionDecision {
                outcome,
                admitted_seq,
                required_copies,
                config_version,
                ..
            } => {
                if *outcome == rdb_core::contracts::trace::AdmissionOutcome::Admitted {
                    part.admissions.insert(
                        correlation,
                        AdmissionRec {
                            admitted_seq: *admitted_seq,
                            config_version: *config_version,
                            required_copies: required_copies.clone(),
                            tick: event.logical_tick,
                        },
                    );
                    part.inflight.insert(correlation);
                    part.admitted_any = true;
                }
            }
            TraceKind::AuthorityDecision {
                generation,
                valid_from_tick,
                expiry_tick,
                outcome,
                ..
            } => {
                part.authority.insert(
                    event.event_id.0,
                    AuthorityRec {
                        generation: *generation,
                        valid_from_tick: *valid_from_tick,
                        expiry_tick: *expiry_tick,
                        outcome: *outcome,
                    },
                );
            }
            TraceKind::BatchApply {
                role,
                generation,
                seq,
                entry_digest,
                key_versions,
                outcome,
                ..
            } => {
                part.applies.entry((*generation, *seq)).or_insert(ApplyRec {
                    node,
                    role: *role,
                    entry_digest: *entry_digest,
                    key_versions: key_versions.clone(),
                    outcome: *outcome,
                    correlation,
                });
            }
            TraceKind::ReplicationAck {
                from_node,
                peer_role,
                peer_boot,
                config_version,
                contiguous_seq,
                durability_class,
                accepted,
                ..
            } => {
                part.acks.insert(
                    (*from_node, *peer_boot),
                    AckRec {
                        claimed_role: *peer_role,
                        config_version: *config_version,
                        contiguous_seq: *contiguous_seq,
                        durability: *durability_class,
                        accepted: *accepted,
                    },
                );
            }
            TraceKind::DurabilityAdvance {
                durable_seq,
                outcome,
                ..
            } => {
                if *outcome == rdb_core::contracts::trace::SyncOutcome::Synced {
                    let slot = part.durable.entry(node).or_insert((boot, Seq::ZERO));
                    if slot.0 != boot || slot.1 < *durable_seq {
                        *slot = (boot, *durable_seq);
                    }
                }
            }
            TraceKind::Publish {
                generation, seq, ..
            } => {
                let from = part.published_seq.map_or(Seq::ZERO, |s| s.next());
                for ((_, apply_seq), rec) in part
                    .applies
                    .range((*generation, from)..=(*generation, *seq))
                {
                    if rec.outcome != ApplyOutcome::Applied
                        && rec.outcome != ApplyOutcome::CrashedAfterCommit
                    {
                        continue;
                    }
                    for (key, version) in &rec.key_versions {
                        part.published.insert(*key, (*version, *apply_seq));
                    }
                }
                part.published_seq = Some(part.published_seq.map_or(*seq, |s| s.max(*seq)));
            }
            TraceKind::ClientOutcomeReported { .. } => {
                part.inflight.remove(&correlation);
                part.terminal.insert(correlation);
            }
            TraceKind::LineageRoot {
                generation,
                base_seq,
                base_digest,
                predecessor_generation,
                predecessor_cutoff,
                source,
                ..
            } => {
                part.roots.push(RootRec {
                    generation: *generation,
                    base_seq: *base_seq,
                    base_digest: *base_digest,
                    predecessor_generation: *predecessor_generation,
                    predecessor_cutoff: *predecessor_cutoff,
                    source: *source,
                    at_event: event.event_id.0,
                });
            }
            TraceKind::ProtectionState {
                phase,
                oldest_unsafe_age_ms,
                required_copy_set,
                config_version,
                paused_prefix_seq,
                resume_barrier_seq,
                ..
            } => {
                part.protection.push(ProtectionRec {
                    phase: *phase,
                    oldest_unsafe_age_ms: *oldest_unsafe_age_ms,
                    required_copy_set: required_copy_set.clone(),
                    config_version: *config_version,
                    paused_prefix_seq: *paused_prefix_seq,
                    resume_barrier_seq: *resume_barrier_seq,
                    tick: event.logical_tick,
                });
            }
            _ => {}
        }
    }
}
