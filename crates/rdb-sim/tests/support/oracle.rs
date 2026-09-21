//! Invariant checkers. **Owned by team verification, package O1.**
//!
//! Registered in [`super`] by team foundation so the seam exists on day one. This file is the
//! oracle's own module root: it declares the submodules and holds the vocabulary every checker
//! and every row shares.
//!
//! What goes here reads [`rdb_core::contracts::trace::Trace`] and nothing else — never simulator
//! state, never a kernel type's internals. The oracle judges what the system declared, not what
//! it can be asked.
//!
//! # Independence (charter O1, design §2.2)
//!
//! Nothing under this module may import `rdb_core::{authority, transaction, replication,
//! publication, protection, recovery}`. Row **M7V-01** is what keeps that true; the grep in the
//! handoff is the human-readable half.
//!
//! # Shape (design §2.2, §2.3, D1 and D2)
//!
//! The fixture writer lives **outside** this module, at
//! [`crate::support::scenarios::builder::TraceBuilder`]: it is an input to the judge, not part of
//! it, and keeping it out is what lets row M7V-01's allowlist stay as tight as it is.
//!
//! One input type, `&Trace`. One left-to-right fold. Per event: every checker observes the event
//! against the facts the [`model::Model`] recorded from **earlier** events, and only then does
//! the model absorb it. That ordering is what makes "cite the digest at `seq - 1`" a lookup
//! rather than a search, and it is why no checker ever needs to scan backwards.

pub mod checks;
pub mod model;

use std::collections::{BTreeMap, BTreeSet};

use rdb_core::contracts::ids::{PartitionId, ReplicaRole};
use rdb_core::contracts::trace::{BoundaryId, CapabilityState, PackageId, Trace, TraceKind};

use self::checks::Checker;
use self::model::{Model, TraceEventKind};

/// The ten invariants the oracle carries (design §2.3; V-R3 adds VER and LAG, V-R8 adds ISO).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Invariant {
    /// Atomicity: all of a transaction's mutations are published, or none.
    Atom,
    /// Publication: nothing is observable above the last publish, and a publish rests on the
    /// pinned required-copy set plus a valid authority recheck.
    Pub,
    /// Authority: no two generations hold valid authority over one partition at one tick.
    Auth,
    /// Lineage: every apply cites the recorded predecessor digest; one position, one digest.
    Lin,
    /// Dedup: one effect per retained identity; the three pre-mutation rejections.
    Dedup,
    /// Restricted loss: a published version disappears only across a declared recovery cutoff.
    Loss,
    /// Controlled liveness, armed only by a healed schedule.
    Live,
    /// Multi-partition isolation, armed exactly like [`Self::Live`].
    Iso,
    /// Compatibility: an unknown mandatory field is refused before apply.
    Ver,
    /// Lag protection, transition legality only (design §2.6).
    Lag,
}

impl Invariant {
    /// Every invariant, in report order. A registry that walks this cannot forget one.
    pub const ALL: [Self; 10] = [
        Self::Atom,
        Self::Pub,
        Self::Auth,
        Self::Lin,
        Self::Dedup,
        Self::Loss,
        Self::Live,
        Self::Iso,
        Self::Ver,
        Self::Lag,
    ];

    /// The id printed in the status table, the artifact and every signature.
    #[must_use]
    pub const fn id(self) -> &'static str {
        match self {
            Self::Atom => "INV-ATOM",
            Self::Pub => "INV-PUB",
            Self::Auth => "INV-AUTH",
            Self::Lin => "INV-LIN",
            Self::Dedup => "INV-DEDUP",
            Self::Loss => "INV-LOSS",
            Self::Live => "INV-LIVE",
            Self::Iso => "INV-ISO",
            Self::Ver => "INV-VER",
            Self::Lag => "INV-LAG",
        }
    }

    /// The packages whose events this checker reads. When any of them reports
    /// [`CapabilityState::Unavailable`] at trace start the verdict is
    /// [`Unavailable::Capability`] — reported, never passed (design §2.4).
    ///
    /// `C0` is not listed anywhere: the contract crate is the vocabulary, not a producer.
    #[must_use]
    pub const fn needs(self) -> &'static [PackageId] {
        match self {
            Self::Atom => &[PackageId::T1],
            Self::Pub => &[PackageId::P1, PackageId::R1],
            Self::Auth => &[PackageId::A1],
            Self::Lin => &[PackageId::T1, PackageId::F1],
            Self::Dedup => &[PackageId::T1],
            Self::Loss => &[PackageId::F1, PackageId::R1],
            Self::Live => &[PackageId::P1, PackageId::H1],
            Self::Iso => &[PackageId::P1, PackageId::H1],
            Self::Ver => &[PackageId::R1],
            Self::Lag => &[PackageId::L1],
        }
    }
}

/// Why a checker could not reach a verdict. Both arms **report, never pass** (design §2.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Unavailable {
    /// A package the checker needs reported `capability{state=Unavailable}` at trace start.
    /// Never inferred from silence: this arm needs the event.
    Capability(PackageId),
    /// Every package is wired, the fold finished, and the checker never armed — or armed and
    /// then disarmed. This arm **is** the conclusion from silence, stated as such.
    NotArmed,
}

/// A checker's answer for one trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Armed at least once, and nothing was violated.
    Proven,
    /// Reported, never a pass.
    Unavailable(Unavailable),
    /// A violation, with the signature the reducer accepts on.
    Violated(Signature),
}

impl Verdict {
    /// Whether this verdict counts towards `seeds_armed` (design §2.4: `proven` and
    /// `seeds_armed` count the same seeds).
    #[must_use]
    pub const fn is_proven(&self) -> bool {
        matches!(self, Self::Proven)
    }

    /// The package this verdict points at, when it points at one.
    #[must_use]
    pub const fn capability(&self) -> Option<PackageId> {
        match self {
            Self::Unavailable(Unavailable::Capability(p)) => Some(*p),
            _ => None,
        }
    }
}

/// What a checker hands back when a clause fires. The oracle turns it into a [`Signature`]:
/// a checker never has to know its own partition, event kind or fault set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// The named clause. Stable, because the reducer's acceptance predicate compares it.
    pub rule: &'static str,
    /// The role of the node in the violating event.
    pub role: ReplicaRole,
    /// Everything a reader needs and the predicate must not see. Reporting only.
    pub detail: String,
}

impl Violation {
    /// A violation of `rule` by a node in `role`, with no further detail.
    #[must_use]
    pub fn new(rule: &'static str, role: ReplicaRole) -> Self {
        Self {
            rule,
            role,
            detail: String::new(),
        }
    }

    /// A violation carrying a rendered detail string, for the report only.
    #[must_use]
    pub fn detailed(rule: &'static str, role: ReplicaRole, detail: impl Into<String>) -> Self {
        Self {
            rule,
            role,
            detail: detail.into(),
        }
    }
}

/// The part of a [`Signature`] the reducer accepts on (design §4.4).
///
/// Deliberately excludes `faults`, `event_id`, `seq`, `logical_tick`, key ids, node ids and the
/// scenario length: every one of those moves when an op is deleted, and a predicate that
/// compares them rejects every useful candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CoreTuple {
    /// Which checker fired.
    pub checker: &'static str,
    /// Which clause.
    pub rule: &'static str,
    /// The partition the violating event was on.
    pub partition: PartitionId,
    /// The role of the node in the violating event.
    pub role: ReplicaRole,
    /// The kind of the violating event.
    pub event_kind: TraceEventKind,
}

/// What must survive shrinking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    /// The core tuple, and the whole acceptance predicate (design §4.4, critic F21).
    pub core: CoreTuple,
    /// Recorded and **reported**, never compared to accept or reject a candidate.
    pub faults: BTreeSet<BoundaryId>,
    /// Where it fired. Reporting only.
    pub event_id: u64,
    /// The checker's own rendering. Reporting only.
    pub detail: String,
}

impl Signature {
    /// A filesystem-safe slug for the reducer's two fixture files (design §4.1).
    #[must_use]
    pub fn slug(&self) -> String {
        format!(
            "{}-{}-p{}",
            self.core
                .checker
                .to_ascii_lowercase()
                .replace("inv-", "inv_"),
            self.core.rule,
            self.core.partition.0
        )
    }
}

/// Every checker's verdict for one trace, plus the facts a row or the campaign reports on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    verdicts: BTreeMap<Invariant, Verdict>,
    /// The capability block the trace opened with, in `PackageId` order.
    pub capabilities: BTreeMap<PackageId, CapabilityState>,
    /// Every boundary the run injected. Reporting and coverage.
    pub faults: BTreeSet<BoundaryId>,
    /// How many events the fold consumed.
    pub events: usize,
}

impl Report {
    /// The verdict for one invariant. Every invariant always has one — "silently absent" is the
    /// state this type exists to make unrepresentable.
    #[must_use]
    pub fn verdict(&self, invariant: Invariant) -> &Verdict {
        self.verdicts
            .get(&invariant)
            .expect("every invariant has a verdict")
    }

    /// Every verdict, in [`Invariant::ALL`] order.
    pub fn verdicts(&self) -> impl Iterator<Item = (Invariant, &Verdict)> {
        Invariant::ALL
            .into_iter()
            .map(|inv| (inv, self.verdict(inv)))
    }

    /// The invariants that reported [`Verdict::Violated`].
    #[must_use]
    pub fn violations(&self) -> Vec<(Invariant, Signature)> {
        self.verdicts()
            .filter_map(|(inv, verdict)| match verdict {
                Verdict::Violated(signature) => Some((inv, signature.clone())),
                _ => None,
            })
            .collect()
    }

    /// Whether any checker fired.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.violations().is_empty()
    }

    /// Suppress every violation whose `rule` is `rule`, as if that clause were not implemented.
    ///
    /// The one assertion-lowering surface in this crate, and the reason row **M7V-85** bounds it
    /// to a single call site: M7V-23 needs "a checker configuration in which the minimized
    /// fixture passes" to show the `.orig.json` companion still fails. Nothing else may call it.
    #[must_use]
    pub fn without_rule(mut self, rule: &str) -> Self {
        for verdict in self.verdicts.values_mut() {
            let suppress = matches!(verdict, Verdict::Violated(s) if s.core.rule == rule);
            if suppress {
                *verdict = Verdict::Unavailable(Unavailable::NotArmed);
            }
        }
        self
    }
}

/// The independent judge (design §2).
#[derive(Debug, Default)]
pub struct Oracle {
    _private: (),
}

impl Oracle {
    /// An oracle. It holds no configuration: everything it knows comes from the trace.
    #[must_use]
    pub const fn new() -> Self {
        Self { _private: () }
    }

    /// Fold `trace` left to right and report one verdict per invariant.
    ///
    /// One pass. Checkers observe an event against the facts of the events **before** it; the
    /// model absorbs the event afterwards. A checker that violates aborts only itself: the other
    /// nine keep folding, because a seed with two independent defects must report both.
    #[must_use]
    pub fn judge(&self, trace: &Trace) -> Report {
        let mut model = Model::new(&trace.header);
        let mut checkers = checks::registry();
        let mut fired: BTreeMap<Invariant, Signature> = BTreeMap::new();

        for event in &trace.events {
            if let TraceKind::Capability { package, state } = event.kind {
                model.capabilities.insert(package, state);
            }
            for checker in &mut checkers {
                let invariant = checker.invariant();
                if fired.contains_key(&invariant) {
                    continue;
                }
                if let Err(violation) = checker.observe(&model, event) {
                    fired.insert(
                        invariant,
                        signature(invariant, &violation, event.partition, &model, event),
                    );
                }
            }
            model.absorb(event);
        }

        for checker in &mut checkers {
            let invariant = checker.invariant();
            if fired.contains_key(&invariant) {
                continue;
            }
            if let Some((partition, violation)) = checker.finish(&model) {
                let last = trace.events.last();
                let kind = last.map_or(TraceEventKind::Capability, |e| TraceEventKind::of(&e.kind));
                fired.insert(
                    invariant,
                    Signature {
                        core: CoreTuple {
                            checker: invariant.id(),
                            rule: violation.rule,
                            partition,
                            role: violation.role,
                            event_kind: kind,
                        },
                        faults: model.faults.clone(),
                        event_id: last.map_or(0, |e| e.event_id.0),
                        detail: violation.detail,
                    },
                );
            }
        }

        let verdicts = checkers
            .iter()
            .map(|checker| {
                let invariant = checker.invariant();
                let verdict = fired.get(&invariant).map_or_else(
                    || verdict_for(invariant, checker.as_ref(), &model),
                    |signature| Verdict::Violated(signature.clone()),
                );
                (invariant, verdict)
            })
            .collect();

        Report {
            verdicts,
            capabilities: model.capabilities.clone(),
            faults: model.faults.clone(),
            events: trace.events.len(),
        }
    }
}

/// The non-violating half of the per-seed verdict (design §2.4).
///
/// Capability first, because a checker that could not run must point at the package that did not
/// land rather than at the corpus.
fn verdict_for(invariant: Invariant, checker: &dyn Checker, model: &Model) -> Verdict {
    for package in invariant.needs() {
        if model.capabilities.get(package) == Some(&CapabilityState::Unavailable) {
            return Verdict::Unavailable(Unavailable::Capability(*package));
        }
    }
    if checker.armed() {
        Verdict::Proven
    } else {
        Verdict::Unavailable(Unavailable::NotArmed)
    }
}

fn signature(
    invariant: Invariant,
    violation: &Violation,
    partition: PartitionId,
    model: &Model,
    event: &rdb_core::contracts::trace::TraceEvent,
) -> Signature {
    Signature {
        core: CoreTuple {
            checker: invariant.id(),
            rule: violation.rule,
            partition,
            role: violation.role,
            event_kind: TraceEventKind::of(&event.kind),
        },
        faults: model.faults.clone(),
        event_id: event.event_id.0,
        detail: violation.detail.clone(),
    }
}
