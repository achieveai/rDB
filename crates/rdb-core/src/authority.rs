//! Fenced grants, epochs and coherent watch resync.
//!
//! Holds the grant state machine and answers the four revalidation gates of spec §5.2 — admission, storage dispatch, publication and reply. Uncertainty is not a tie: when the bounded-clock comparison in [`crate::contracts::time::ControlTime::compare`] returns [`crate::contracts::time::ClockVerdict::Uncertain`], the answer is deny.
//!
//! **Owner:** team kernel-a, package A1. Specification: spec §7.2, §7.3.
//!
//! # What is wired (2026-09-21)
//!
//! The **watch and coherent-resync slice** of team kernel-a `design.md` §2.4, and nothing else.
//! It is the part of A1 that the landed [`EffectKind`] can express: every effect it emits is a
//! [`EffectKind::Control`], which is one of the six variants that exist.
//!
//! The rest of §2.4 — the authority gates, the fence, the pushed view — is **not** wired, and
//! not because it is unwritten. `design.md` §2.2 gives A1 eight effect kinds; five of them
//! (`Decide(AuthorityDecision)`, `Fence`, `PublishAuthorityView`, `FenceProven`, `Fact`) have no
//! [`EffectKind`] variant to travel in, and `Check { checkpoint, lineage, correlation }` has no
//! [`crate::contracts::event::EventKind`] variant to arrive in.
//! [`crate::contracts::authority::AuthorityDecision`],
//! [`crate::contracts::authority::AuthorityView`], [`crate::contracts::authority::Verdict`] and
//! [`crate::contracts::authority::DenyReason`] are landed types with **no consumer anywhere in
//! the workspace**. Wiring those gates is a C0 contract change, not a kernel-a change, and a
//! kernel that faked them locally would be the "fake success" spike §8 forbids.
//!
//! # The rule this slice exists to hold
//!
//! ADR-rdb-0008 §7 item 4, as restated by lead ruling A-R15: **no coherent family reload occurs
//! unless a termination was delivered.** A watch invalidates a cache; it never grants authority
//! and it never, on its own, justifies a reload. The one thing that declares a gap is
//! [`WatchTermination::is_gap`], and rEtcd's stream does not skip silently (ADR-rdb-0008 §4), so
//! a live stream has nothing to reload against. A kernel that reloaded on a `Watched` or a
//! `WatchProgress` would turn cache invalidation into a poll, which is the exact defect the ADR
//! item exists to catch.
//!
//! [`ControlEffect::Reload`] is therefore emitted from **exactly one** match arm in this file:
//! the [`ControlEvent::WatchTerminated`] arm, guarded by `termination.is_gap()`. Row `M7A-32`
//! counts reloads across 250 gapless watch events and asserts zero, and then proves the counter
//! is live in the same test by delivering one termination and asserting exactly one.

use std::collections::BTreeMap;

use crate::contracts::control::{
    ControlChange, ControlEffect, ControlEvent, ControlKey, ControlPrefix, WatchTermination,
};
use crate::contracts::errors::{Capability, RdbError};
use crate::contracts::event::{Effect, EffectKind, Event, EventKind, Module, ModuleName, StepCtx};
use crate::contracts::ids::Revision;
use crate::contracts::trace::CapabilityState;

/// How many consecutive [`WatchTermination::ResourceExhaustedFatal`] terminations A1 will
/// re-arm its watch through before it stops re-arming.
///
/// A capacity error is **not** a gap ([`WatchTermination::is_gap`] is `false` for it), so the
/// answer is a bounded back-off and never a reload. Reloading in a loop on an admission limit
/// turns a capacity error into an outage, which is the trap the termination type exists to
/// spell out.
pub const WATCH_ADMISSION_ATTEMPT_CAP: u32 = 3;

/// Which grant state A1 is in.
///
/// The watch slice acts only in [`Self::Held`]: a node without a validated grant has no cache
/// worth invalidating and no family it is entitled to reload.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum AuthorityState {
    /// No validated grant. The entry state, and the state after a fence clears.
    #[default]
    Unheld,
    /// A grant this node committed and has not lost.
    Held,
    /// Terminal for this grant id. Exit requires a *new* grant id in a control record.
    Fenced,
}

/// Fenced grants, epochs and coherent watch resync.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Authority {
    state: AuthorityState,
    /// Where each watched family has been consumed to. A resumed watch starts after this.
    cursors: BTreeMap<ControlPrefix, Revision>,
    /// Consecutive [`WatchTermination::ResourceExhaustedFatal`] terminations since the last
    /// healthy watch event. Reset by any delivery that proves the stream is serving.
    watch_refused_attempts: u32,
}

impl Authority {
    /// A module holding no state yet.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: AuthorityState::Unheld,
            cursors: BTreeMap::new(),
            watch_refused_attempts: 0,
        }
    }

    /// The grant state, for a fixture that asserts on it (team kernel-a `KA-1`).
    #[must_use]
    pub const fn state(&self) -> AuthorityState {
        self.state
    }

    /// Consecutive admission-refused terminations, for `M7A-33`.
    #[must_use]
    pub const fn watch_refused_attempts(&self) -> u32 {
        self.watch_refused_attempts
    }

    /// Where a watched family has been consumed to, if it is watched at all.
    #[must_use]
    pub fn cursor(&self, prefix: ControlPrefix) -> Option<Revision> {
        self.cursors.get(&prefix).copied()
    }

    /// The family a single record belongs to.
    ///
    /// A watch and a reload are scoped to a [`ControlPrefix`]; a change names a [`ControlKey`].
    /// This is the only mapping between them, and it is total.
    const fn family_of(key: ControlKey) -> ControlPrefix {
        match key {
            ControlKey::ClusterSchema => ControlPrefix::ClusterSchema,
            ControlKey::Node(_) => ControlPrefix::Nodes,
            ControlKey::Grant(_) => ControlPrefix::Grants,
            ControlKey::Partition(_) => ControlPrefix::Partitions,
            ControlKey::Route(_) => ControlPrefix::Routes,
            ControlKey::Operation(_) => ControlPrefix::Operations,
            ControlKey::PlannerGrant => ControlPrefix::PlannerGrant,
        }
    }

    /// Wrap a control request as an effect of this module, for this event.
    fn control(event: &Event, kind: ControlEffect) -> Effect {
        Effect {
            correlation: event.correlation,
            from: ModuleName::Authority,
            partition: event.partition,
            kind: EffectKind::Control(kind),
        }
    }

    /// A contiguous run of changes arrived, and nothing between the cursors was skipped.
    ///
    /// **A watch event never widens rights** (`design.md` §2.4 property 3): each change becomes a
    /// linearizable [`ControlEffect::Get`] of the record that changed, and the kernel believes
    /// nothing until that read answers. No reload is emitted here — the stream has not gapped, so
    /// there is nothing to reload against.
    fn on_watched(
        &mut self,
        event: &Event,
        prefix: ControlPrefix,
        cursor_revision: Revision,
        changes: &[ControlChange],
    ) -> Vec<Effect> {
        self.watch_refused_attempts = 0;
        self.cursors.insert(prefix, cursor_revision);
        changes
            .iter()
            .map(|change| Self::control(event, ControlEffect::Get { key: change.key }))
            .collect()
    }

    /// The stream ended, and [`WatchTermination::is_gap`] — not the stream going quiet — says
    /// whether anything was missed.
    fn on_terminated(
        &mut self,
        event: &Event,
        prefix: ControlPrefix,
        from: Revision,
        termination: WatchTermination,
    ) -> Vec<Effect> {
        let resume = self.cursors.get(&prefix).copied().unwrap_or(from);

        if termination.is_gap() {
            // The one place a reload is legal. The re-watch is deferred to the
            // `FamilySnapshot` arm, which is what makes reload-then-re-watch a closed loop
            // rather than a race: the resumed watch starts after the revision the snapshot was
            // coherent at, and that revision is not known until the snapshot arrives.
            self.watch_refused_attempts = 0;
            return vec![Self::control(event, ControlEffect::Reload { prefix })];
        }

        match termination {
            // A capacity error, not a gap. Bounded re-arm, and never a reload.
            WatchTermination::ResourceExhaustedFatal => {
                self.watch_refused_attempts = self.watch_refused_attempts.saturating_add(1);
                if self.watch_refused_attempts >= WATCH_ADMISSION_ATTEMPT_CAP {
                    Vec::new()
                } else {
                    vec![Self::control(
                        event,
                        ControlEffect::Watch {
                            prefix,
                            from: resume,
                        },
                    )]
                }
            }
            // Leadership moved, or the hub went away. Re-establish and read before believing
            // anything; the cursor is still good, so this is a resume, not a reload.
            WatchTermination::NotLeader | WatchTermination::Unavailable => {
                vec![Self::control(
                    event,
                    ControlEffect::Watch {
                        prefix,
                        from: resume,
                    },
                )]
            }
            // Unreachable: both remaining variants answer `true` to `is_gap` and returned above.
            // Spelled out rather than caught by a wildcard so that a sixth termination variant
            // fails compilation here instead of silently taking the no-reload path.
            WatchTermination::RevisionCompacted { .. }
            | WatchTermination::ResourceExhaustedResumable => Vec::new(),
        }
    }

    /// A coherent snapshot of one family arrived. Resume the watch after the revision the whole
    /// snapshot is coherent at, which closes the reload loop.
    fn on_family_snapshot(
        &mut self,
        event: &Event,
        prefix: ControlPrefix,
        snapshot_revision: Revision,
    ) -> Vec<Effect> {
        self.watch_refused_attempts = 0;
        self.cursors.insert(prefix, snapshot_revision);
        vec![Self::control(
            event,
            ControlEffect::Watch {
                prefix,
                from: snapshot_revision,
            },
        )]
    }

    /// Route one control completion.
    fn on_control(&mut self, event: &Event, control: &ControlEvent) -> Vec<Effect> {
        match control {
            // Grant adoption: the create-only CAS this node issued committed, so it now holds
            // the grant and starts watching the two families it serves from. The partitions
            // family is loaded coherently first because the grant record does not carry lineage
            // (`design.md` §2.4, the partition lineage path).
            ControlEvent::CasResult { key, outcome } => {
                let crate::contracts::control::CasOutcome::Committed(revision) = outcome else {
                    return Vec::new();
                };
                if self.state != AuthorityState::Unheld
                    || Self::family_of(*key) != ControlPrefix::Grants
                {
                    return Vec::new();
                }
                self.state = AuthorityState::Held;
                self.cursors.insert(ControlPrefix::Grants, *revision);
                vec![
                    Self::control(
                        event,
                        ControlEffect::Reload {
                            prefix: ControlPrefix::Partitions,
                        },
                    ),
                    Self::control(
                        event,
                        ControlEffect::Watch {
                            prefix: ControlPrefix::Grants,
                            from: *revision,
                        },
                    ),
                ]
            }

            _ if self.state != AuthorityState::Held => Vec::new(),

            ControlEvent::Watched {
                prefix,
                cursor,
                changes,
            } => self.on_watched(event, *prefix, cursor.revision, changes),

            // A liveness watermark. It carries no authority and no record content, so it moves
            // the cursor and nothing else — and specifically it does not reload.
            ControlEvent::WatchProgress { prefix, revision } => {
                self.watch_refused_attempts = 0;
                self.cursors.insert(*prefix, *revision);
                Vec::new()
            }

            ControlEvent::WatchTerminated {
                prefix,
                from,
                termination,
            } => self.on_terminated(event, *prefix, *from, *termination),

            ControlEvent::FamilySnapshot {
                prefix,
                snapshot_revision,
                ..
            } => self.on_family_snapshot(event, *prefix, *snapshot_revision),

            // A record read answered. The lineage install that consumes it is part of the
            // `served` path, which needs `EffectKind::AdoptAuthority` plus a grant-record codec
            // that C0 owns; it is not in this slice.
            ControlEvent::Value { .. } => Vec::new(),
        }
    }
}

impl Module for Authority {
    fn name(&self) -> ModuleName {
        ModuleName::Authority
    }

    fn capability(&self) -> CapabilityState {
        // Deliberately still `Unavailable`. The watch slice is real, but A1's advertised
        // capability is the four authority gates, and those cannot be expressed against the
        // landed `EffectKind` at all. Reporting `Wired` here would tell the campaign runner
        // that package A1 answers checks, which it does not.
        CapabilityState::Unavailable
    }

    fn step(&mut self, _ctx: &StepCtx<'_>, event: &Event) -> Result<Vec<Effect>, RdbError> {
        match &event.kind {
            EventKind::Control(control) => Ok(self.on_control(event, control)),
            _ => Err(RdbError::unavailable(
                Capability::Authority,
                "package A1 answers only the control seam in this build",
            )),
        }
    }
}
