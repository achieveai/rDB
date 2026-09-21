//! The hand-built trace builder (VA-1).
//!
//! Every `M7V-01..M7V-41` row is a trace written by hand, and writing a `TraceEvent` literal per
//! row would put a `event_id`, a `logical_tick`, a partition, a node, a boot and a correlation in
//! front of the one fact the row is about. This builder assigns the envelope so a row states only
//! what differs from the scenario around it.
//!
//! Three things it owns that a row must not:
//!
//! 1. **`event_id` is builder-assigned and strictly increasing.** The oracle is a single
//!    left-to-right fold and never sorts; a row that numbered its own events could write a trace
//!    the harness can never produce.
//! 2. **`ack_from` emits two events, in order** (ruling V-R20 (8)): the secondary's own
//!    `batch_apply` first, then its `replication_ack`. A copy that acknowledges a record it never
//!    applied is not a copy, and a row that writes only the acknowledgement asserts against a
//!    trace no correct kernel emits.
//! 3. **Well-formedness is checked at [`TraceBuilder::build`]**, not by a checker. A malformed
//!    envelope is a fixture defect and must fail loudly where it was written, rather than arrive
//!    at the oracle as a violation of somebody's invariant.
//!
//! No wall clock anywhere: [`TraceBuilder::at`] sets the logical tick and nothing else moves it.

use std::collections::BTreeMap;

use rdb_core::contracts::digest::{Digest, Domain};
use rdb_core::contracts::event::Budgets;
use rdb_core::contracts::ids::{
    BootId, ConfigVersion, CorrelationId, EventId, Generation, NodeId, PartitionId, ReplicaRole,
    Seq,
};
use rdb_core::contracts::trace::{
    AckEvidence, ApplyOutcome, CapabilityState, DurabilityClass, KeyId, PackageId, Provenance,
    RunManifest, SyncOutcome, TopologyEntry, Trace, TraceEvent, TraceHeader, TraceKind, Version,
};
use rdb_core::contracts::version::TRACE_SCHEMA_VERSION;

/// The generator version hand-built fixtures declare. Distinct from the seeded generator's own,
/// because an authored trace is not in the generator's image (finding K-F-09).
pub const AUTHORED_GENERATOR_VERSION: u16 = 1;

/// The default membership configuration a builder starts in.
pub const CONFIG_V1: ConfigVersion = ConfigVersion(1);

/// The lineage every fixture starts in.
pub const GEN_1: Generation = Generation(1);

/// Every package, in declaration order. C0 has no `ALL` for [`PackageId`] — the contract crate
/// does not enumerate itself — so the capability block's completeness is asserted by row
/// **M7V-82** against this list rather than assumed.
pub const ALL_PACKAGES: [PackageId; 10] = [
    PackageId::C0,
    PackageId::H1,
    PackageId::M1,
    PackageId::I1,
    PackageId::A1,
    PackageId::T1,
    PackageId::R1,
    PackageId::P1,
    PackageId::L1,
    PackageId::F1,
];

/// A deterministic digest for one lineage position. Content-free on purpose: the trace carries
/// no key or value bytes, and a checker compares digests only for equality.
#[must_use]
pub fn digest_at(generation: Generation, seq: Seq) -> Digest {
    Digest::of(
        Domain::Lineage,
        &[&generation.0.to_le_bytes(), &seq.0.to_le_bytes()],
    )
}

/// A deterministic digest that is *not* [`digest_at`] for the same position. The one way a
/// fixture expresses divergence without inventing bytes.
#[must_use]
pub fn forked_digest_at(generation: Generation, seq: Seq) -> Digest {
    Digest::of(
        Domain::Lineage,
        &[&generation.0.to_le_bytes(), &seq.0.to_le_bytes(), b"fork"],
    )
}

/// Builds one hand-written [`Trace`].
#[derive(Debug, Clone)]
pub struct TraceBuilder {
    provenance: Provenance,
    partitions: u8,
    topology: Vec<TopologyEntry>,
    events: Vec<TraceEvent>,
    next_event_id: u64,
    tick: u64,
    partition: PartitionId,
    node: NodeId,
    boot: BootId,
    correlation: CorrelationId,
    generation: Generation,
    config_version: ConfigVersion,
    event_cap: u32,
}

impl Default for TraceBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TraceBuilder {
    /// An RF3 single-partition builder: `n1` primary, `n2` and `n3` regular secondaries, all at
    /// [`CONFIG_V1`], positioned on `n1` at tick 0.
    #[must_use]
    pub fn new() -> Self {
        Self {
            provenance: Provenance::Authored {
                case: "hand-built".to_owned(),
            },
            partitions: 1,
            topology: Vec::new(),
            events: Vec::new(),
            next_event_id: 1,
            tick: 0,
            partition: PartitionId(0),
            node: NodeId(1),
            boot: BootId(1),
            correlation: CorrelationId(1),
            generation: GEN_1,
            config_version: CONFIG_V1,
            event_cap: 10_000,
        }
        .rf3(PartitionId(0), CONFIG_V1)
    }

    /// Name the case this fixture is. Rows pass their own row id so a failing artifact says
    /// which row wrote it.
    #[must_use]
    pub fn case(mut self, case: &str) -> Self {
        self.provenance = Provenance::Authored {
            case: case.to_owned(),
        };
        self
    }

    /// Place `n1` primary and `n2`, `n3` regular secondaries on `partition` at `config_version`.
    #[must_use]
    pub fn rf3(self, partition: PartitionId, config_version: ConfigVersion) -> Self {
        self.place(
            partition,
            config_version,
            &[
                (NodeId(1), ReplicaRole::Primary),
                (NodeId(2), ReplicaRole::RegularSecondary),
                (NodeId(3), ReplicaRole::RegularSecondary),
            ],
        )
    }

    /// Declare an arbitrary placement in the **header snapshot**. Later membership changes are
    /// `topology_change` events, never another call to this (ruling V-R12).
    #[must_use]
    pub fn place(
        mut self,
        partition: PartitionId,
        config_version: ConfigVersion,
        nodes: &[(NodeId, ReplicaRole)],
    ) -> Self {
        for (node, role) in nodes {
            self.topology.retain(|entry| {
                !(entry.partition == partition
                    && entry.node == *node
                    && entry.config_version == config_version)
            });
            self.topology.push(TopologyEntry {
                partition,
                node: *node,
                role: *role,
                config_version,
            });
        }
        let implied = u8::try_from(partition.0.saturating_add(1)).unwrap_or(u8::MAX);
        self.partitions = self.partitions.max(implied);
        self
    }

    /// How many partitions the header declares, when it is more than the placements imply.
    #[must_use]
    pub const fn partitions(mut self, partitions: u8) -> Self {
        self.partitions = partitions;
        self
    }

    /// The event budget the header's manifest declares.
    #[must_use]
    pub const fn event_cap(mut self, event_cap: u32) -> Self {
        self.event_cap = event_cap;
        self
    }

    // -- envelope ------------------------------------------------------------------------

    /// Move the logical clock. Never goes backwards: [`Self::build`] rejects a trace whose ticks
    /// decrease, because the fold reads them as time.
    #[must_use]
    pub const fn at(mut self, tick: u64) -> Self {
        self.tick = tick;
        self
    }

    /// Which partition subsequent events are on.
    #[must_use]
    pub const fn on(mut self, partition: PartitionId) -> Self {
        self.partition = partition;
        self
    }

    /// Which node and boot subsequent events happen at.
    #[must_use]
    pub const fn by(mut self, node: NodeId, boot: BootId) -> Self {
        self.node = node;
        self.boot = boot;
        self
    }

    /// Which request subsequent events belong to.
    #[must_use]
    pub const fn about(mut self, correlation: CorrelationId) -> Self {
        self.correlation = correlation;
        self
    }

    /// Which lineage subsequent helpers write in.
    #[must_use]
    pub const fn in_generation(mut self, generation: Generation) -> Self {
        self.generation = generation;
        self
    }

    /// Which membership configuration subsequent helpers pin to.
    #[must_use]
    pub const fn pinned_to(mut self, config_version: ConfigVersion) -> Self {
        self.config_version = config_version;
        self
    }

    /// The `event_id` the **next** pushed event will carry. Rows use it to fill a back-reference
    /// — a publication's `authority_recheck`, a read's `barrier` — without counting by hand.
    #[must_use]
    pub const fn next_event(&self) -> EventId {
        EventId(self.next_event_id)
    }

    /// The `event_id` of the event pushed most recently.
    ///
    /// # Panics
    ///
    /// When nothing has been pushed yet: a back-reference to no event is a fixture defect.
    #[must_use]
    pub fn last_event(&self) -> EventId {
        self.events
            .last()
            .expect("a back-reference needs an event to point at")
            .event_id
    }

    // -- events --------------------------------------------------------------------------

    /// Push `kind` with the current envelope.
    #[must_use]
    pub fn push(mut self, kind: TraceKind) -> Self {
        self.events.push(TraceEvent {
            event_id: EventId(self.next_event_id),
            logical_tick: self.tick,
            partition: self.partition,
            node: self.node,
            boot: self.boot,
            correlation: self.correlation,
            kind,
        });
        self.next_event_id += 1;
        self
    }

    /// The capability block, at trace start (V-R18). Every package not named reports
    /// [`CapabilityState::Wired`], so a row states only what is missing.
    ///
    /// Derived from the crate's actual wiring by the runner; a hand-built row states it because
    /// the row *is* the wiring for that trace.
    #[must_use]
    pub fn capabilities(mut self, unavailable: &[PackageId]) -> Self {
        let mut block: Vec<(PackageId, CapabilityState)> = ALL_PACKAGES
            .iter()
            .map(|package| {
                let state = if unavailable.contains(package) {
                    CapabilityState::Unavailable
                } else {
                    CapabilityState::Wired
                };
                (*package, state)
            })
            .collect();
        block.sort_by_key(|(package, _)| *package);
        for (package, state) in block {
            self = self.push(TraceKind::Capability { package, state });
        }
        self
    }

    /// A secondary's acknowledgement of `seq`, as the **two** events it really is (V-R20 (8)):
    /// the secondary's own `batch_apply` first, then its `replication_ack`.
    ///
    /// The role is resolved from the header placement at the builder's pinned `config_version`,
    /// so a fixture cannot accidentally declare one role in the topology and another in the ack.
    /// A row that wants that disagreement writes the `replication_ack` itself, which is exactly
    /// what the `ack_role_claim_mismatch` rows do.
    #[must_use]
    pub fn ack_from(self, node: NodeId, seq: Seq, durability: DurabilityClass) -> Self {
        let role = self.declared_role(node).unwrap_or(ReplicaRole::Shadow);
        let generation = self.generation;
        let config_version = self.config_version;
        let (partition, primary, boot) = (self.partition, self.node, self.boot);
        self.by(node, BootId(1))
            .push(TraceKind::BatchApply {
                role,
                generation,
                seq,
                predecessor_seq: previous(seq),
                predecessor_digest: digest_at(generation, previous(seq)),
                entry_digest: digest_at(generation, seq),
                batch: seq.0,
                key_versions: Vec::new(),
                outcome: ApplyOutcome::Applied,
            })
            .push(TraceKind::ReplicationAck {
                from_node: node,
                to_node: primary,
                peer_role: role,
                peer_boot: BootId(1),
                config_version,
                generation,
                owner_epoch: rdb_core::contracts::ids::OwnerEpoch(1),
                contiguous_seq: seq,
                contiguous_digest: digest_at(generation, seq),
                durability_class: durability,
                accepted: true,
                reject_reason: None,
            })
            .on(partition)
            .by(primary, boot)
    }

    /// A completed fsync on `node` through `seq`. The only thing that grounds a `Durable`
    /// acknowledgement (design §2.5).
    #[must_use]
    pub fn flush(self, node: NodeId, seq: Seq) -> Self {
        let generation = self.generation;
        let partition = self.partition;
        let (primary, boot) = (self.node, self.boot);
        self.by(node, BootId(1))
            .push(TraceKind::DurabilityAdvance {
                generation,
                durable_seq: seq,
                durable_digest: digest_at(generation, seq),
                flush_ticket: seq.0,
                captured: vec![(partition, seq)],
                outcome: SyncOutcome::Synced,
            })
            .by(primary, boot)
    }

    /// The primary's own apply of `seq`, writing `key_versions`.
    #[must_use]
    pub fn apply(self, seq: Seq, key_versions: &[(KeyId, Version)], outcome: ApplyOutcome) -> Self {
        let generation = self.generation;
        self.push(TraceKind::BatchApply {
            role: ReplicaRole::Primary,
            generation,
            seq,
            predecessor_seq: previous(seq),
            predecessor_digest: digest_at(generation, previous(seq)),
            entry_digest: digest_at(generation, seq),
            batch: seq.0,
            key_versions: key_versions.to_vec(),
            outcome,
        })
    }

    /// A publication of `seq` resting on `evidence` and the authority decision at `recheck`.
    #[must_use]
    pub fn publish(self, seq: Seq, evidence: &[AckEvidence], recheck: EventId) -> Self {
        let generation = self.generation;
        self.push(TraceKind::Publish {
            generation,
            seq,
            published_digest: digest_at(generation, seq),
            ack_evidence: evidence.to_vec(),
            authority_recheck: recheck,
        })
    }

    /// The role the **header** gives `node` on the current partition at the pinned
    /// `config_version`, falling back to the closest earlier configuration.
    #[must_use]
    pub fn declared_role(&self, node: NodeId) -> Option<ReplicaRole> {
        self.topology
            .iter()
            .filter(|entry| {
                entry.partition == self.partition
                    && entry.node == node
                    && entry.config_version <= self.config_version
            })
            .max_by_key(|entry| entry.config_version)
            .map(|entry| entry.role)
    }

    // -- output --------------------------------------------------------------------------

    /// The finished trace.
    ///
    /// # Panics
    ///
    /// When the envelope is malformed: `event_id` not strictly increasing, `logical_tick` going
    /// backwards, or an event on a node the header never placed. Every one of those is a fixture
    /// defect, and it must fail here rather than reach the oracle as somebody's violation.
    #[must_use]
    pub fn build(mut self) -> Trace {
        self.topology.sort();
        let placed: BTreeMap<(PartitionId, NodeId), ()> = self
            .topology
            .iter()
            .map(|entry| ((entry.partition, entry.node), ()))
            .collect();

        let mut previous_id = 0;
        let mut previous_tick = 0;
        for event in &self.events {
            assert!(
                event.event_id.0 > previous_id,
                "event_id must be strictly increasing: {} after {previous_id}",
                event.event_id.0
            );
            assert!(
                event.logical_tick >= previous_tick,
                "logical_tick must not go backwards: {} after {previous_tick} at event {}",
                event.logical_tick,
                event.event_id.0
            );
            assert!(
                placed.contains_key(&(event.partition, event.node)),
                "event {} is on node {} of partition {}, which the header never placed",
                event.event_id.0,
                event.node.0,
                event.partition.0
            );
            previous_id = event.event_id.0;
            previous_tick = event.logical_tick;
        }

        let nodes = u8::try_from(placed.len()).unwrap_or(u8::MAX);
        Trace {
            header: TraceHeader {
                schema_version: TRACE_SCHEMA_VERSION,
                generator_version: AUTHORED_GENERATOR_VERSION,
                provenance: self.provenance,
                config: RunManifest {
                    budgets: Budgets::SPEC_DEFAULTS,
                    overridden: Vec::new(),
                    nodes,
                    event_cap: self.event_cap,
                },
                partitions: self.partitions,
                topology: self.topology,
                oracle_checkpoint_digest: Digest::ROOT,
            },
            events: self.events,
        }
    }
}

/// The position before `seq`, saturating at [`Seq::ZERO`].
#[must_use]
pub fn previous(seq: Seq) -> Seq {
    Seq(seq.0.saturating_sub(1))
}

/// One piece of acknowledgement evidence, as a publication records it.
#[must_use]
pub const fn evidence(node: NodeId, role: ReplicaRole, durability: DurabilityClass) -> AckEvidence {
    AckEvidence {
        node,
        boot: BootId(1),
        role,
        durability,
    }
}
