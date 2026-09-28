//! The publication barrier, reads, status and uncertain outcomes.
//!
//! Publication is a distinct local state transition: the required regular acknowledgement plus an
//! authority recheck, and nothing else, may publish an applied prefix. Every reader — API,
//! export, actor, diagnostic — acquires the barrier and sees the declared published prefix, never
//! the raw applied one.
//!
//! **Owner:** team kernel-a, package P1. Specification: spec §5.3, §5.4. Design: team kernel-a
//! `design.md` §4.
//!
//! # Layout
//!
//! - [`kernel`]: [`PubKernel`], the §4.2 table for one partition on one boot, in P1's own
//!   vocabulary ([`PubEvent`], [`PubEffect`]).
//! - [`status`]: the status index and the 24-hour window (§4.4).
//! - [`view`]: R1's live publish predicate as P1 reads it (§1.6), and the KA-8 fake.
//! - This file: [`Publication`], the [`Module`] — one [`PubKernel`] per `(node, partition)`, and
//!   the translation between the contract's carriers (lead ruling A-R63) and P1's vocabulary.

pub mod kernel;
pub mod status;
pub mod view;

use std::collections::BTreeMap;

use bytes::Bytes;

pub use self::kernel::{
    AppliedCandidate, AwaitingReply, FreezeCause, PendingView, PredicateFalse, PubConfig,
    PubEffect, PubEvent, PubFact, PubKernel, PubMode, PubStateView, PublishedAt, ReadIntent,
    ReplyOutcome, StatusEntry, StatusOutcome, Withheld, POST_APPLY_DEADLINE_MILLIS,
    PUBLICATION_CORRELATION_BASE, PUBLICATION_SNAPSHOT_BASE, WAITER_CAP,
};
pub use self::status::StatusIndex;
pub use self::view::{ReplicationView, ScriptedReplication};

use crate::contracts::authority::{
    AuthorityEvent, AuthorityIgnoreReason, DenyReason, FenceScope, Lineage,
};
use crate::contracts::digest::{Digest, Domain};
use crate::contracts::errors::{Capability, ErrorKind, RdbError};
use crate::contracts::event::{
    ClientEvent, Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module,
    ModuleName, ReplyEffect, StepCtx,
};
use crate::contracts::ids::{BootId, NodeId, PartitionId, RequestIdentity, Seq, TimerId};
use crate::contracts::ignore::KernelIgnoredReason;
use crate::contracts::publication::{PublicationEffect, PublicationEvent};
use crate::contracts::storage::{Namespace, SnapshotRead, StorageEvent, StoreEffect};
use crate::contracts::time::TimerEffect;
use crate::contracts::trace::{CapabilityState, ReadServiceOutcome, Version};

/// The first [`TimerId`] P1 owns. P1 arms one timer per partition, at `base + partition`.
pub const PUBLICATION_TIMER_BASE: u64 = 0x00D1 << 48;

/// The post-apply deadline timer for `partition`.
#[must_use]
pub fn post_apply_timer(partition: PartitionId) -> TimerId {
    TimerId(PUBLICATION_TIMER_BASE + u64::from(partition.0))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Slot {
    kernel: PubKernel,
    /// The key each outstanding [`ClientEvent::Read`] asked for, until its barrier answers.
    read_keys: BTreeMap<RequestIdentity, Bytes>,
}

impl Slot {
    fn new(config: PubConfig, boot: BootId, lineage: Lineage, published_seq: Seq) -> Self {
        Self {
            kernel: PubKernel::new(config, boot, lineage, published_seq),
            read_keys: BTreeMap::new(),
        }
    }
}

/// The publication barrier, reads, status and uncertain outcomes.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Publication {
    config: PubConfig,
    slots: BTreeMap<(NodeId, PartitionId), Slot>,
    scripted: BTreeMap<(NodeId, PartitionId), ScriptedReplication>,
}

impl Publication {
    /// A module holding no partition yet, with the default [`PubConfig`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A module holding no partition yet.
    #[must_use]
    pub fn with_config(config: PubConfig) -> Self {
        Self {
            config,
            ..Self::default()
        }
    }

    /// Start `(node, partition)` on `boot` serving `lineage` with `published_seq` published.
    /// Replaces any kernel already there. Without it, the first event creates one from the
    /// step context's authority triple at `Seq::ZERO`.
    ///
    /// Returns the storage effect that asks for a view at `published_seq` (lead ruling A-R71
    /// F6). Deliver it: once storage binds that view there, `ReadPrevious` answers from it, and
    /// until then it answers `Unavailable`. A replaced kernel's views are not released.
    #[must_use = "the installed position's view opens only if this effect reaches storage"]
    pub fn install(
        &mut self,
        node: NodeId,
        boot: BootId,
        lineage: Lineage,
        published_seq: Seq,
    ) -> EffectKind {
        let mut slot = Slot::new(self.config, boot, lineage, published_seq);
        let handle = slot.kernel.open_view();
        self.slots.insert((node, lineage.partition), slot);
        EffectKind::Store(StoreEffect::Snapshot {
            handle,
            partition: lineage.partition,
        })
    }

    /// Give [`Module::step`] a scripted R1 view for `(node, partition)` (test plan KA-8). The
    /// dispatcher passes the real one through [`Self::step_with`] instead.
    pub fn script_replication(
        &mut self,
        node: NodeId,
        partition: PartitionId,
        view: ScriptedReplication,
    ) {
        self.scripted.insert((node, partition), view);
    }

    /// The scripted view for `(node, partition)`, to change between steps.
    pub fn scripted_mut(
        &mut self,
        node: NodeId,
        partition: PartitionId,
    ) -> Option<&mut ScriptedReplication> {
        self.scripted.get_mut(&(node, partition))
    }

    /// The kernel for `(node, partition)`.
    #[must_use]
    pub fn kernel(&self, node: NodeId, partition: PartitionId) -> Option<&PubKernel> {
        self.slots.get(&(node, partition)).map(|slot| &slot.kernel)
    }

    /// Forget every kernel and scripted view held for `node`. All of it is process memory, so
    /// this is what a restart of that node loses (lead ruling V-R35); every other node is
    /// untouched. The simulator calls it from its restart.
    pub fn forget_node(&mut self, node: NodeId) {
        self.slots.retain(|(held, _), _| *held != node);
        self.scripted.retain(|(held, _), _| *held != node);
    }

    /// Everything a row may assert on for `(node, partition)`.
    #[must_use]
    pub fn view(&self, node: NodeId, partition: PartitionId) -> Option<PubStateView> {
        self.kernel(node, partition).map(PubKernel::view)
    }

    /// How many Fresh-read keys the barrier holds for `(node, partition)`: one per identity whose
    /// read is still gated or waiting. An answered read frees its key (tester-p1 N22), so this
    /// returns to its earlier size once every read is answered.
    #[must_use]
    pub fn held_read_keys(&self, node: NodeId, partition: PartitionId) -> usize {
        self.slots
            .get(&(node, partition))
            .map_or(0, |slot| slot.read_keys.len())
    }

    /// One step with R1's live predicate for this partition, or `None` when no R1 view is
    /// reachable (then nothing publishes). What the dispatcher calls, handing it the primary's
    /// [`crate::replication::progress::ProgressTracker`].
    ///
    /// # Errors
    ///
    /// [`RdbError::Unavailable`] naming the event, for an event that is not P1's. The kernel is
    /// not stepped.
    pub fn step_with(
        &mut self,
        ctx: &StepCtx<'_>,
        event: &Event,
        repl: Option<&dyn ReplicationView>,
    ) -> Result<Vec<Effect>, RdbError> {
        if Self::not_ours(event) {
            return Ok(vec![Effect {
                correlation: event.correlation,
                from: ModuleName::Publication,
                partition: event.partition,
                kind: ignored(AuthorityIgnoreReason::NotOurs),
            }]);
        }
        let input = Self::input(event)?;
        let config = self.config;
        let slot = self
            .slots
            .entry((event.node, event.partition))
            .or_insert_with(|| Slot::new(config, ctx.boot, ctx_lineage(ctx), Seq::ZERO));
        // P1's state is volatile: a new boot starts with nothing pending and nothing owed.
        if slot.kernel.boot() != ctx.boot {
            *slot = Slot::new(config, ctx.boot, ctx_lineage(ctx), Seq::ZERO);
        }
        let kinds = match &event.kind {
            // Q1 (lead ruling A-R71): the barrier holds one key per identity, so a second read
            // while the first still waits is refused on arrival, and never queued. Overwriting
            // the key would answer the first read from the second one's key, and
            // `ReplyEffect::Read` carries no key for the client to notice by.
            EventKind::Client(ClientEvent::Read { identity, .. })
                if slot.read_keys.contains_key(identity) =>
            {
                vec![EffectKind::Reply(ReplyEffect::Read {
                    identity: *identity,
                    outcome: ReadServiceOutcome::Rejected(ErrorKind::RequestIdReuse),
                    value: None,
                })]
            }
            kind => {
                if let EventKind::Client(ClientEvent::Read { identity, key }) = kind {
                    slot.read_keys.insert(*identity, key.clone());
                }
                let produced = slot.kernel.apply(ctx.now, input, repl);
                produced
                    .into_iter()
                    .flat_map(|effect| route(ctx, event, slot, effect))
                    .collect()
            }
        };
        Ok(kinds
            .into_iter()
            .map(|kind| Effect {
                correlation: event.correlation,
                from: ModuleName::Publication,
                partition: event.partition,
                kind,
            })
            .collect())
    }

    /// A1's fence or view for a partition other than the event's (lead ruling A-R84, batch C):
    /// P1's input, but not this partition's to act on. Answered `Ignored(NotOurs)`, as T1's
    /// `on_freeze` answers the same fence, before any slot is touched or made.
    fn not_ours(event: &Event) -> bool {
        match &event.kind {
            EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::Fence { scope, .. })) => {
                !covers(*scope, event.partition)
            }
            EventKind::Kernel(KernelEvent::Authority(AuthorityEvent::View(view))) => {
                view.lineage.partition != event.partition
            }
            _ => false,
        }
    }

    /// The carrier-to-vocabulary half. Refuses what is not P1's by name.
    fn input(event: &Event) -> Result<PubEvent, RdbError> {
        let reason = match &event.kind {
            EventKind::Client(ClientEvent::Read { identity, .. }) => {
                return Ok(PubEvent::BarrierAcquire {
                    reader: *identity,
                    intent: ReadIntent::Fresh,
                });
            }
            EventKind::Client(ClientEvent::Status {
                identity,
                generation,
            }) => {
                return Ok(PubEvent::StatusQuery {
                    request: *identity,
                    generation: *generation,
                });
            }
            EventKind::Timer(fired) if fired.id == post_apply_timer(event.partition) => {
                return Ok(PubEvent::PostApplyDeadline {
                    version: fired.version,
                });
            }
            EventKind::Kernel(kernel) => match kernel {
                KernelEvent::AppliedCandidate(cand) => return Ok(PubEvent::Candidate(**cand)),
                KernelEvent::QualificationChanged(q) => {
                    return Ok(PubEvent::QualificationChanged(q.clone()));
                }
                KernelEvent::Authority(AuthorityEvent::Answer(answer)) => {
                    return Ok(PubEvent::AuthorityAnswer(*answer));
                }
                KernelEvent::Authority(AuthorityEvent::View(view)) => {
                    return Ok(PubEvent::AuthorityView(*view));
                }
                // Another partition's fence never reaches here: `not_ours` answered it.
                KernelEvent::Authority(AuthorityEvent::Fence { reason, .. }) => {
                    return Ok(PubEvent::Freeze {
                        cause: freeze_cause(*reason),
                    });
                }
                KernelEvent::BlockPartition(reason) => {
                    return Ok(PubEvent::BlockPartition {
                        reason: reason.clone(),
                    });
                }
                KernelEvent::Recovered(result) => return Ok(PubEvent::Recovered(result.clone())),
                KernelEvent::Publication(PublicationEvent::ReadPrevious { identity }) => {
                    return Ok(PubEvent::BarrierAcquire {
                        reader: *identity,
                        intent: ReadIntent::PreviousPublished,
                    });
                }
                KernelEvent::Publication(PublicationEvent::ModeQuery { identity }) => {
                    return Ok(PubEvent::ModeQuery { reader: *identity });
                }
                KernelEvent::StatusTrim { generation, below } => {
                    return Ok(PubEvent::StatusTrim {
                        generation: *generation,
                        below: *below,
                    });
                }
                KernelEvent::RetireGeneration { generation } => {
                    return Ok(PubEvent::RetireGeneration {
                        generation: *generation,
                    });
                }
                KernelEvent::Authority(_) => "publication: an A1 input, not P1's",
                _ => "publication: not a P1 input",
            },
            EventKind::Client(_) => "publication: Submit reaches T1, not P1",
            EventKind::Timer(_) => "publication: a timer outside P1's block",
            EventKind::Node(_) => "publication: lifecycle reaches A1; P1 resets on a new boot",
            EventKind::Transport(_) => "publication: transport frames reach R1, never P1",
            EventKind::Storage(StorageEvent::SnapshotReady { handle, at })
                if kernel::is_publication_snapshot(*handle, event.partition) =>
            {
                return Ok(PubEvent::SnapshotReady {
                    handle: *handle,
                    at: *at,
                });
            }
            EventKind::Storage(_) => {
                "publication: storage completions reach T1 and R1, except P1's own views"
            }
            EventKind::Control(_) => "publication: control answers reach A1 and F1",
            EventKind::ExternalFenceVerified { .. } => "publication: fence proofs reach A1",
        };
        Err(RdbError::unavailable(Capability::Publication, reason))
    }
}

impl Module for Publication {
    fn name(&self) -> ModuleName {
        ModuleName::Publication
    }

    fn capability(&self) -> CapabilityState {
        // Deliberately still `Unavailable`: every §4.2 row is reachable through `step`, and the
        // sim dispatcher routes every P1 edge and hands P1 the primary's tracker through
        // `step_with` (A-R66); nothing in routing holds it back any more (A-R82..A-R84).
        // Flipping it is a lead call.
        CapabilityState::Unavailable
    }

    fn step(&mut self, ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
        let scripted = self.scripted.get(&(event.node, event.partition)).cloned();
        self.step_with(
            ctx,
            event,
            scripted.as_ref().map(|s| s as &dyn ReplicationView),
        )
    }
}

fn ctx_lineage(ctx: &StepCtx<'_>) -> Lineage {
    Lineage {
        partition: ctx.partition,
        generation: ctx.generation,
        owner_epoch: ctx.owner_epoch,
    }
}

/// Whether an A1 fence of `scope` reaches `partition` (design §4.2 `Freeze` guard).
const fn covers(scope: FenceScope, partition: PartitionId) -> bool {
    match scope {
        FenceScope::Node => true,
        FenceScope::Partition(fenced) => fenced.0 == partition.0,
    }
}

/// A1's fence reason as P1's freeze cause (design §3.3, "mapped by the dispatcher").
const fn freeze_cause(reason: DenyReason) -> FreezeCause {
    match reason {
        DenyReason::LocalStorageFenced => FreezeCause::LocalStorageFenced,
        other => FreezeCause::AuthorityLost(other),
    }
}

fn ignored(reason: AuthorityIgnoreReason) -> EffectKind {
    EffectKind::Kernel(KernelEffect::Ignored {
        reason: KernelIgnoredReason::Authority(reason),
    })
}

/// The vocabulary-to-carrier half.
fn route(ctx: &StepCtx<'_>, event: &Event, slot: &mut Slot, effect: PubEffect) -> Vec<EffectKind> {
    let timer = post_apply_timer(event.partition);
    let kind = match effect {
        PubEffect::ArmTimer { version, at } => EffectKind::Timer(TimerEffect::Arm {
            id: timer,
            version,
            at,
        }),
        PubEffect::CancelTimer { version } => {
            EffectKind::Timer(TimerEffect::Cancel { id: timer, version })
        }
        PubEffect::AuthorityCheck {
            checkpoint,
            lineage,
            correlation,
        } => EffectKind::Kernel(KernelEffect::AuthorityCheck {
            checkpoint,
            lineage,
            correlation,
        }),
        PubEffect::Status(entry) => {
            EffectKind::Kernel(KernelEffect::Publication(PublicationEffect::Status(entry)))
        }
        PubEffect::Reply {
            request,
            outcome: ReplyOutcome::Published { result },
        } => EffectKind::Reply(ReplyEffect::Transaction {
            identity: request,
            result,
        }),
        PubEffect::Reply {
            request,
            outcome: ReplyOutcome::Unknown,
        } => EffectKind::Reply(ReplyEffect::Failed {
            identity: request,
            error: RdbError::UnknownOutcome {
                partition: event.partition,
                identity: request,
            },
        }),
        PubEffect::StatusAnswer {
            request, outcome, ..
        } => EffectKind::Reply(ReplyEffect::Status {
            identity: request,
            status: status::to_wire(outcome),
        }),
        PubEffect::Mode { reader, mode } => {
            EffectKind::Kernel(KernelEffect::Publication(PublicationEffect::Mode {
                identity: reader,
                mode,
            }))
        }
        PubEffect::NotifyTxn {
            lineage,
            seq,
            record_digest,
            request,
        } => EffectKind::Kernel(KernelEffect::Published {
            lineage,
            seq,
            record_digest,
            request,
        }),
        PubEffect::OpenSnapshot { handle } => EffectKind::Store(StoreEffect::Snapshot {
            handle,
            partition: event.partition,
        }),
        PubEffect::ReleaseSnapshot { handle } => EffectKind::Store(StoreEffect::Release { handle }),
        PubEffect::Previous { reader, handle } => {
            EffectKind::Kernel(KernelEffect::Publication(PublicationEffect::Snapshot {
                identity: reader,
                handle,
            }))
        }
        PubEffect::Answer {
            reader,
            answer,
            waited,
        } => return answer_reader(ctx.snapshot, slot, reader, answer, waited),
        PubEffect::Fact(PubFact::Quarantined { generation, seq }) => {
            EffectKind::Kernel(KernelEffect::Publication(PublicationEffect::Quarantined {
                generation,
                seq,
            }))
        }
        PubEffect::Fact(fact) => ignored(fact_name(fact)),
    };
    vec![kind]
}

/// A `Fresh` barrier answer, as the [`ReplyEffect::Read`] of the [`ClientEvent::Read`] that
/// asked. (`PublicationEvent::ReadPrevious` never comes here: it is answered from P1's kept view
/// as [`PubEffect::Previous`], never from the step's.)
///
/// The step's snapshot is used **only when it is the published position the kernel handed out**
/// (§4.3 invariant 2). Any other view — the raw applied prefix above it, or a stale one below
/// it — serves nothing, answers `Unavailable`, and says so with `ReadViewNotPublished`.
///
/// The key is held per identity, and [`Publication::step_with`] refuses a second read under an
/// identity that still waits (A-R71), so every answer finds its own key. An answer that finds
/// none cannot know what was asked, so it is refused as `Unavailable`, never served.
fn answer_reader(
    snapshot: &dyn SnapshotRead,
    slot: &mut Slot,
    reader: RequestIdentity,
    answer: Result<PublishedAt, ErrorKind>,
    waited: bool,
) -> Vec<EffectKind> {
    let at_published =
        |at: PublishedAt| snapshot.generation() == at.generation && snapshot.at() == at.seq;
    let not_published = matches!(answer, Ok(at) if !at_published(at));
    let answer = match answer {
        Ok(at) if at_published(at) => Ok(at),
        Ok(_) => Err(ErrorKind::Unavailable),
        Err(kind) => Err(kind),
    };
    let (outcome, value) = match (slot.read_keys.remove(&reader), answer) {
        (_, Err(kind)) => (ReadServiceOutcome::Rejected(kind), None),
        (None, Ok(_)) => (ReadServiceOutcome::Rejected(ErrorKind::Unavailable), None),
        (Some(key), Ok(_)) => (
            if waited {
                ReadServiceOutcome::WaitedAtBarrier
            } else {
                ReadServiceOutcome::Served
            },
            read_value(snapshot, &key),
        ),
    };
    let reply = EffectKind::Reply(ReplyEffect::Read {
        identity: reader,
        outcome,
        value,
    });
    if not_published {
        vec![ignored(AuthorityIgnoreReason::ReadViewNotPublished), reply]
    } else {
        vec![reply]
    }
}

/// The value at `key` as its version and digest — never bytes (lead ruling F-R7), in its own
/// domain so a served value never shares a preimage with an oracle checkpoint (A-R66).
fn read_value(snapshot: &dyn SnapshotRead, key: &[u8]) -> Option<(Version, Digest)> {
    let version = snapshot.version(Namespace::User, key)?;
    let bytes = snapshot.get(Namespace::User, key)?;
    Some((version, Digest::of(Domain::ReadValue, &[key, &bytes])))
}

/// P1's fact as its `AuthorityIgnoreReason` name.
fn fact_name(fact: PubFact) -> AuthorityIgnoreReason {
    match fact {
        PubFact::CandidateWhileNotServing { .. } => AuthorityIgnoreReason::CandidateWhileNotServing,
        PubFact::CandidateUnreachable { .. } => AuthorityIgnoreReason::CandidateUnreachable,
        PubFact::NotForThisCandidate { .. } => AuthorityIgnoreReason::NotForThisCandidate,
        PubFact::RecheckOutstanding => AuthorityIgnoreReason::RecheckOutstanding,
        PubFact::QualificationLost { .. } => AuthorityIgnoreReason::QualificationLost,
        PubFact::QualificationLostAfterPublish { .. } => {
            AuthorityIgnoreReason::QualificationLostAfterPublish
        }
        PubFact::StaleAuthorityAnswer => AuthorityIgnoreReason::StaleAuthorityAnswer,
        PubFact::PublishRefusedBlocked => AuthorityIgnoreReason::PublishRefusedBlocked,
        PubFact::PublishDeferred => AuthorityIgnoreReason::PublishDeferred,
        PubFact::PublishPredicateFalse { .. } => AuthorityIgnoreReason::PublishPredicateFalse,
        PubFact::Quarantined { .. } => AuthorityIgnoreReason::Quarantined,
        PubFact::ReplySuppressedAfterTimeout => AuthorityIgnoreReason::ReplySuppressedAfterTimeout,
        PubFact::ReplyWithheld { .. } => AuthorityIgnoreReason::ReplyWithheld,
        PubFact::StaleTimer => AuthorityIgnoreReason::StaleTimer,
        PubFact::StaleAuthorityView => AuthorityIgnoreReason::StaleAuthorityView,
        PubFact::Blocked { reason } => AuthorityIgnoreReason::Blocked { reason },
        PubFact::AlreadyBlocked => AuthorityIgnoreReason::AlreadyBlocked,
        PubFact::FenceWhileBlocked { .. } => AuthorityIgnoreReason::FenceWhileBlocked,
    }
}
