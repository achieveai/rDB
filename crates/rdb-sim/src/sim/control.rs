//! The fake single-record CAS control store, with a watch that is allowed to end badly.
//!
//! This stands in for rEtcd. It is a *fake*, not a certification: passing against it says nothing
//! about the real `ConfigStore` (spike §9). Its value is the opposite — it must be **no
//! friendlier** than the real surface, because a kernel that only works against a polite store is
//! a kernel that does not work.
//!
//! ADR-rdb-0008 §7, as amended by lead ruling A-R15 (2026-09-20), lists the hostile behaviours the
//! fake must be able to produce under scenario control:
//!
//! | Requirement | Where it lives |
//! |---|---|
//! | 1. a conflict hides the winning value | [`rdb_core::contracts::control::CasOutcome::Conflict`] carries `exists` and `current`, never a value |
//! | 2. `Unknown` is distinct from `Unavailable` and from `Conflict` | three separate `CasOutcome` variants; [`ControlOp::PlanCas`] forces any of them |
//! | 3. all five typed watch terminations, plus a progress tick | [`ControlOp::TerminateWatch`], [`ControlOp::EmitProgress`] |
//! | 4. ~~no silent gap~~ | **kernel-side assertion, not a fake property** (A-R15). [`ControlOp::EmitWatch`] delivers contiguously and a gap is only ever a termination, so "the kernel reloaded without being told to" is an assertion the oracle makes against the trace — the fake cannot enforce it and does not try. Since correction round 1 the trace has the two events to assert over: every completion here produces a [`TraceKind::ControlInteraction`] (finding K-F-06) |
//! | 5. `Unavailable` inside a generous deadline | **restored** by lead ruling F-R3, after A-R15 had deleted it: [`ControlOp::PlanReadUnavailable`] |
//! | 6. a coherent family read with a resumable `snapshot_revision` | [`ControlStore::snapshot_family`], which returns the records (finding K-F-12) |
//! | 7. a completion delivered arbitrarily late, after the grant expired | [`ControlOp::DelayCompletion`] |
//! | 8. a control effect that never completes at all | [`ControlOp::DropCompletion`] |
//!
//! 7 and 8 are the two that catch a kernel which treats "I asked" as "I have it". A grant renewal
//! whose CAS lands after the grant expired must not revive the grant, and an effect with no
//! completion must not leave a partition waiting for one — spec §7.2 fails closed on both.
//!
//! # The store is asynchronous (finding K-F-14)
//!
//! The kernel never calls the store. It emits a [`ControlEffect`]; the harness hands it to
//! [`ControlStore::submit`]; the store decides the outcome *at submit time* — it is linearizable,
//! and the decision is what a real store would have made at that instant — and holds the
//! [`ControlEvent`] until [`ControlStore::complete`] drains it. `DelayCompletion` and
//! `DropCompletion` act on that held queue, which is what gives items 7 and 8 a mechanism
//! instead of a variant.

use std::collections::BTreeMap;

use bytes::Bytes;
use rdb_core::contracts::control::{
    CasOutcome, ControlChange, ControlEffect, ControlEvent, ControlKey, ControlPrefix,
    ControlRecord, ReadOutcome, WatchCursor, WatchTermination,
};
use rdb_core::contracts::event::{Effect, EffectKind};
use rdb_core::contracts::ids::{CorrelationId, NodeId, PartitionId, Revision};
use rdb_core::contracts::time::Tick;
use rdb_core::contracts::trace::{ControlOpKind, ControlOutcomeKind, TraceKind};

use crate::error::SimError;

/// An injectable control-plane fault, as a scenario writes it.
///
/// One enum, like [`crate::sim::network::NetworkOp`] and [`crate::storage::StorageOp`], so the
/// generator, the shrinker and the coverage matrix can enumerate and compare operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ControlOp {
    /// Force what `node`'s next CAS **reports**, whatever the record actually holds.
    ///
    /// The store still applies the CAS for real; only the answer is replaced. That is what
    /// `Unknown` means — the write may have landed — and it is the only way a scenario produces
    /// it, since by construction it cannot be derived from the store's own state. Per node
    /// (finding K-F-14): one racer's report can be forced while the other's proceeds.
    PlanCas {
        /// Whose next CAS.
        node: NodeId,
        /// What it will report.
        outcome: CasOutcome,
    },
    /// Force the next linearizable read to report
    /// [`rdb_core::contracts::control::ReadOutcome::Unavailable`], whatever the record holds.
    ///
    /// Lead ruling F-R3 (2026-09-20) restored this injection point, which A-R15 had removed with
    /// ADR-rdb-0008 §7 item 5. The behaviour is real, not a courtesy: rEtcd bounds its read
    /// barrier by the server's own `read_timeout`, independent of the caller's (rEtcd ADR-0009),
    /// so a read comes back unavailable well inside a generous deadline. Package A1's rows need
    /// it because a grant holder that treats an unavailable read as "probably still mine" keeps
    /// writing after it has lost the control plane.
    ///
    /// Carries no outcome parameter: `Found` and `Absent` follow from the store's own state, and
    /// this is the one answer that cannot.
    PlanReadUnavailable,
    /// Deliver the pending watch changes to `node`, contiguously.
    EmitWatch {
        /// The watcher.
        node: NodeId,
    },
    /// Emit a progress tick carrying only the current revision: a cache-freshness watermark that
    /// conveys no authority and no record content.
    EmitProgress {
        /// The watcher.
        node: NodeId,
    },
    /// End `node`'s watches with a specific termination.
    TerminateWatch {
        /// The watcher.
        node: NodeId,
        /// Why it ended.
        termination: WatchTermination,
    },
    /// Compact history below `up_to`, so a watcher resuming from before it must terminate with
    /// [`WatchTermination::RevisionCompacted`].
    Compact {
        /// The lowest revision that will remain available.
        up_to: Revision,
    },
    /// Hold `node`'s next control completion back by `by_millis` of logical time.
    ///
    /// The completion still arrives, and it still reports what really happened — arbitrarily
    /// late. Long enough and the grant it belongs to has expired, which is the case spec §7.2
    /// fails closed on: the CAS committed, and the right it would have granted is gone.
    DelayCompletion {
        /// The waiting node.
        node: NodeId,
        /// How long to hold it.
        by_millis: u64,
    },
    /// Drop `node`'s next control completion entirely. It never arrives.
    ///
    /// Distinct from [`Self::PlanCas`] with `CasOutcome::Unknown`: there, the caller is *told*
    /// nothing is known. Here the caller is told nothing at all, and must still make progress off
    /// its own deadline rather than waiting forever.
    DropCompletion {
        /// The waiting node.
        node: NodeId,
    },
}

/// One completed control effect, as [`ControlStore::complete`] hands it to the harness.
///
/// Carries the partition and correlation of the effect it answers, so the harness can build the
/// completion [`rdb_core::contracts::event::Event`] without remembering anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completion {
    /// The node that asked.
    pub node: NodeId,
    /// The partition the effect concerned.
    pub partition: PartitionId,
    /// The request the effect served.
    pub correlation: CorrelationId,
    /// When the completion is to be delivered: the drain tick, plus any planned delay.
    pub at: Tick,
    /// What the store said.
    pub event: ControlEvent,
}

/// One open watch.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Watch {
    prefix: ControlPrefix,
    /// The last revision delivered without a gap.
    cursor: Revision,
    partition: PartitionId,
    correlation: CorrelationId,
}

/// A completion decided but not yet drained.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pending {
    node: NodeId,
    partition: PartitionId,
    correlation: CorrelationId,
    event: ControlEvent,
    interaction: TraceKind,
}

/// The fake control store.
///
/// Holds its state and is not `Copy` (finding K-F-29). Every map is a `BTreeMap`.
#[derive(Debug, Default)]
pub struct ControlStore {
    revision: Revision,
    records: BTreeMap<ControlKey, (Revision, Bytes)>,
    /// Every committed write, in revision order, for watch delivery. Trimmed by `Compact`.
    history: Vec<(Revision, ControlKey)>,
    /// The lowest revision still in `history`. One until the first `Compact`.
    minimum_available: Revision,
    watches: BTreeMap<NodeId, Vec<Watch>>,
    pending: Vec<Pending>,
    planned: Vec<ControlOp>,
    /// The `ControlInteraction` of every completion drained, in drain order.
    interactions: Vec<TraceKind>,
}

impl ControlStore {
    /// An empty store at revision zero.
    #[must_use]
    pub fn new() -> Self {
        Self {
            minimum_available: Revision(1),
            ..Self::default()
        }
    }

    /// The store's current revision: the revision of the last committed write.
    #[must_use]
    pub const fn revision(&self) -> Revision {
        self.revision
    }

    /// Apply one scenario operation.
    ///
    /// Plans ([`ControlOp::PlanCas`], [`ControlOp::PlanReadUnavailable`],
    /// [`ControlOp::DelayCompletion`], [`ControlOp::DropCompletion`]) are kept, in order, until
    /// the operation they name arrives. Watch operations produce their events at once, held for
    /// the next [`ControlStore::complete`]. [`ControlOp::Compact`] takes effect at once.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `watch` for a watch operation on a node with no open watch —
    /// a scenario that emits to nobody is a typo, not a no-op.
    pub fn inject(&mut self, op: ControlOp) -> Result<(), SimError> {
        match op {
            ControlOp::PlanCas { .. }
            | ControlOp::PlanReadUnavailable
            | ControlOp::DelayCompletion { .. }
            | ControlOp::DropCompletion { .. } => {
                self.planned.push(op);
                Ok(())
            }
            ControlOp::EmitWatch { node } => {
                let watches = self.take_watches(node)?;
                let mut kept = Vec::with_capacity(watches.len());
                for mut watch in watches {
                    if watch.cursor.0 + 1 < self.minimum_available.0 {
                        let termination = WatchTermination::RevisionCompacted {
                            minimum_available_revision: self.minimum_available,
                        };
                        self.terminate(node, &watch, termination);
                        continue;
                    }
                    let changes: Vec<ControlChange> = self
                        .history
                        .iter()
                        .filter(|(revision, key)| {
                            *revision > watch.cursor && watch.prefix.contains(*key)
                        })
                        .map(|(revision, key)| ControlChange {
                            key: *key,
                            revision: *revision,
                        })
                        .collect();
                    let cursor = WatchCursor {
                        revision: self.revision,
                    };
                    self.pending.push(Pending {
                        node,
                        partition: watch.partition,
                        correlation: watch.correlation,
                        event: ControlEvent::Watched {
                            prefix: watch.prefix,
                            cursor,
                            changes,
                        },
                        interaction: interaction(
                            ControlOpKind::Watch,
                            None,
                            Some(watch.prefix),
                            ControlOutcomeKind::Progress,
                        ),
                    });
                    watch.cursor = self.revision;
                    kept.push(watch);
                }
                if !kept.is_empty() {
                    self.watches.insert(node, kept);
                }
                Ok(())
            }
            ControlOp::EmitProgress { node } => {
                let watches = self.take_watches(node)?;
                for watch in &watches {
                    self.pending.push(Pending {
                        node,
                        partition: watch.partition,
                        correlation: watch.correlation,
                        event: ControlEvent::WatchProgress {
                            prefix: watch.prefix,
                            revision: self.revision,
                        },
                        interaction: interaction(
                            ControlOpKind::Watch,
                            None,
                            Some(watch.prefix),
                            ControlOutcomeKind::Progress,
                        ),
                    });
                }
                self.watches.insert(node, watches);
                Ok(())
            }
            ControlOp::TerminateWatch { node, termination } => {
                let watches = self.take_watches(node)?;
                for watch in &watches {
                    self.terminate(node, watch, termination);
                }
                Ok(())
            }
            ControlOp::Compact { up_to } => {
                if up_to > self.minimum_available {
                    self.minimum_available = up_to;
                    self.history.retain(|(revision, _)| *revision >= up_to);
                }
                Ok(())
            }
        }
    }

    /// Hand a control effect to the store on behalf of `node`.
    ///
    /// The outcome is decided now, against the store's current state — the store is
    /// linearizable — and delivered by [`ControlStore::complete`]. A plan for `node` replaces
    /// the *report*, never the state.
    ///
    /// # Errors
    ///
    /// [`SimError::Config`] naming `effect` when `effect.kind` is not a control effect, and
    /// naming `from` when a watch resumes from a revision this store has compacted away — that
    /// is not an error a real store gives, so the fake refuses it instead of answering with a
    /// termination the kernel would have to have asked for.
    pub fn submit(&mut self, node: NodeId, effect: &Effect) -> Result<(), SimError> {
        let EffectKind::Control(control) = &effect.kind else {
            return Err(SimError::Config { field: "effect" });
        };
        let (partition, correlation) = (effect.partition, effect.correlation);
        match control {
            ControlEffect::Cas {
                key,
                expected,
                value,
            } => {
                let real = self.apply_cas(*key, *expected, value.clone());
                let outcome = self.take_planned_cas(node).unwrap_or(real);
                self.pending.push(Pending {
                    node,
                    partition,
                    correlation,
                    event: ControlEvent::CasResult { key: *key, outcome },
                    interaction: interaction(
                        ControlOpKind::Cas,
                        Some(*key),
                        None,
                        cas_kind(outcome),
                    ),
                });
            }
            ControlEffect::Get { key } => {
                let outcome = if self.take_planned_read_unavailable() {
                    ReadOutcome::Unavailable
                } else {
                    self.get(*key)
                };
                let kind = read_kind(&outcome);
                self.pending.push(Pending {
                    node,
                    partition,
                    correlation,
                    event: ControlEvent::Value { key: *key, outcome },
                    interaction: interaction(ControlOpKind::Get, Some(*key), None, kind),
                });
            }
            ControlEffect::Watch { prefix, from } => {
                if from.0 + 1 < self.minimum_available.0 {
                    return Err(SimError::Config { field: "from" });
                }
                let watches = self.watches.entry(node).or_default();
                watches.retain(|watch| watch.prefix != *prefix);
                watches.push(Watch {
                    prefix: *prefix,
                    cursor: *from,
                    partition,
                    correlation,
                });
            }
            ControlEffect::Reload { prefix } => {
                let (snapshot_revision, records) = self.snapshot_family(*prefix)?;
                self.pending.push(Pending {
                    node,
                    partition,
                    correlation,
                    event: ControlEvent::FamilySnapshot {
                        prefix: *prefix,
                        snapshot_revision,
                        records,
                    },
                    interaction: interaction(
                        ControlOpKind::Reload,
                        None,
                        Some(*prefix),
                        ControlOutcomeKind::Found,
                    ),
                });
            }
        }
        Ok(())
    }

    /// Drain every decided completion, stamped with the tick it is to be delivered at.
    ///
    /// `now` is the drain tick. An undelayed completion is due at `now`; a
    /// [`ControlOp::DelayCompletion`] for its node moves it to `now + by_millis`; a
    /// [`ControlOp::DropCompletion`] removes it and it is never seen again — and, since it never
    /// completed, it leaves no [`TraceKind::ControlInteraction`] either. Plans are consumed in
    /// injection order, one per completion.
    pub fn complete(&mut self, now: Tick) -> Vec<Completion> {
        let pending = std::mem::take(&mut self.pending);
        let mut out = Vec::with_capacity(pending.len());
        for item in pending {
            if self
                .take_planned(
                    |op| matches!(op, ControlOp::DropCompletion { node } if *node == item.node),
                )
                .is_some()
            {
                continue;
            }
            let delay = match self.take_planned(
                |op| matches!(op, ControlOp::DelayCompletion { node, .. } if *node == item.node),
            ) {
                Some(ControlOp::DelayCompletion { by_millis, .. }) => by_millis,
                _ => 0,
            };
            self.interactions.push(item.interaction);
            out.push(Completion {
                node: item.node,
                partition: item.partition,
                correlation: item.correlation,
                at: now.plus_millis(delay),
                event: item.event,
            });
        }
        out
    }

    /// The [`TraceKind::ControlInteraction`] of every completion drained since the last call,
    /// in drain order. The H1 provider's declaration of what the store said (finding K-F-06).
    pub fn drain_interactions(&mut self) -> Vec<TraceKind> {
        std::mem::take(&mut self.interactions)
    }

    /// Linearizable read of one record, as the store holds it now.
    #[must_use]
    pub fn get(&self, key: ControlKey) -> ReadOutcome {
        match self.records.get(&key) {
            Some((revision, value)) => ReadOutcome::Found {
                revision: *revision,
                value: value.clone(),
            },
            None => ReadOutcome::Absent {
                as_of: self.revision,
            },
        }
    }

    /// A coherent snapshot of one key family, with the revision a watch may resume after.
    ///
    /// The only sanctioned answer to a gap (spec §7.1): one operation, never a diff against
    /// remembered state. Returns the records (finding K-F-12) in [`ControlKey`] order, owned —
    /// a later write does not reach into a snapshot already taken.
    ///
    /// # Errors
    ///
    /// None today; the signature keeps the error channel a real store needs.
    pub fn snapshot_family(
        &self,
        prefix: ControlPrefix,
    ) -> Result<(Revision, Vec<ControlRecord>), SimError> {
        let records = self
            .records
            .iter()
            .filter(|(key, _)| prefix.contains(**key))
            .map(|(key, (revision, value))| ControlRecord {
                key: *key,
                revision: *revision,
                value: value.clone(),
            })
            .collect();
        Ok((self.revision, records))
    }

    /// How many watches `node` has open.
    #[must_use]
    pub fn open_watches(&self, node: NodeId) -> usize {
        self.watches.get(&node).map_or(0, Vec::len)
    }

    /// How many completions are decided and not yet drained.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// The real compare-and-swap, against the store's state.
    fn apply_cas(
        &mut self,
        key: ControlKey,
        expected: Option<Revision>,
        value: Option<Bytes>,
    ) -> CasOutcome {
        let current = self.records.get(&key).map(|(revision, _)| *revision);
        let matches = match (expected, current) {
            (None, None) => true,
            (Some(expected), Some(current)) => expected == current,
            _ => false,
        };
        if !matches {
            return CasOutcome::Conflict {
                exists: current.is_some(),
                current: current.unwrap_or(self.revision),
            };
        }
        self.revision = Revision(self.revision.0 + 1);
        match value {
            Some(value) => {
                self.records.insert(key, (self.revision, value));
            }
            None => {
                self.records.remove(&key);
            }
        }
        self.history.push((self.revision, key));
        CasOutcome::Committed(self.revision)
    }

    fn terminate(&mut self, node: NodeId, watch: &Watch, termination: WatchTermination) {
        self.pending.push(Pending {
            node,
            partition: watch.partition,
            correlation: watch.correlation,
            event: ControlEvent::WatchTerminated {
                prefix: watch.prefix,
                from: watch.cursor,
                termination,
            },
            interaction: interaction(
                ControlOpKind::Watch,
                None,
                Some(watch.prefix),
                ControlOutcomeKind::Terminated {
                    termination,
                    gap: termination.is_gap(),
                },
            ),
        });
    }

    fn take_watches(&mut self, node: NodeId) -> Result<Vec<Watch>, SimError> {
        match self.watches.remove(&node) {
            Some(watches) if !watches.is_empty() => Ok(watches),
            _ => Err(SimError::Config { field: "watch" }),
        }
    }

    fn take_planned(&mut self, matches: impl Fn(&ControlOp) -> bool) -> Option<ControlOp> {
        let index = self.planned.iter().position(matches)?;
        Some(self.planned.remove(index))
    }

    fn take_planned_cas(&mut self, node: NodeId) -> Option<CasOutcome> {
        match self.take_planned(
            |op| matches!(op, ControlOp::PlanCas { node: planned, .. } if *planned == node),
        ) {
            Some(ControlOp::PlanCas { outcome, .. }) => Some(outcome),
            _ => None,
        }
    }

    fn take_planned_read_unavailable(&mut self) -> bool {
        self.take_planned(|op| matches!(op, ControlOp::PlanReadUnavailable))
            .is_some()
    }
}

const fn interaction(
    op: ControlOpKind,
    key: Option<ControlKey>,
    prefix: Option<ControlPrefix>,
    outcome: ControlOutcomeKind,
) -> TraceKind {
    TraceKind::ControlInteraction {
        op,
        key,
        prefix,
        outcome,
    }
}

const fn cas_kind(outcome: CasOutcome) -> ControlOutcomeKind {
    match outcome {
        CasOutcome::Committed(_) => ControlOutcomeKind::Committed,
        CasOutcome::Conflict { .. } => ControlOutcomeKind::Conflict,
        CasOutcome::Unknown => ControlOutcomeKind::Unknown,
        CasOutcome::Unavailable => ControlOutcomeKind::Unavailable,
    }
}

const fn read_kind(outcome: &ReadOutcome) -> ControlOutcomeKind {
    match outcome {
        ReadOutcome::Found { .. } => ControlOutcomeKind::Found,
        ReadOutcome::Absent { .. } => ControlOutcomeKind::Absent,
        ReadOutcome::Unavailable => ControlOutcomeKind::Unavailable,
    }
}
