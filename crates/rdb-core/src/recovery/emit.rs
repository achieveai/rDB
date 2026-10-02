//! What one F1 step emits, in order (team kernel-b `design.md` §5; BA-1, BA-2).
//!
//! Every effect goes through [`Emit`], which stamps the event's correlation and partition, so
//! the phase code never builds an [`Effect`] by hand. F1's own effects travel in
//! [`KernelEffect::Recovery`]; a decision to do nothing travels as
//! [`KernelEffect::Ignored`] with a [`ReplicaIgnoreReason`] leaf. Never silence: an event with
//! no other effect yields one `Ignored`.

use crate::contracts::event::{Effect, EffectKind, Event, KernelEffect, ModuleName};
use crate::contracts::ignore::{KernelIgnoredReason, ReplicaIgnoreReason};
use crate::contracts::recovery::RecoveryEffect;

/// The output of one step, stamped with the event's correlation and partition.
#[derive(Debug)]
pub(crate) struct Emit<'e> {
    event: &'e Event,
    out: Vec<Effect>,
}

impl<'e> Emit<'e> {
    pub(crate) const fn new(event: &'e Event) -> Self {
        Self {
            event,
            out: Vec::new(),
        }
    }

    /// The event this step is answering.
    pub(crate) const fn event(&self) -> &'e Event {
        self.event
    }

    /// A landed effect: a control CAS or read, a timer, `Recovered`.
    pub(crate) fn kind(&mut self, kind: EffectKind) {
        self.out.push(Effect {
            correlation: self.event.correlation,
            from: ModuleName::Recovery,
            partition: self.event.partition,
            kind,
        });
    }

    /// One of F1's own effects.
    pub(crate) fn recovery(&mut self, effect: RecoveryEffect) {
        self.kind(EffectKind::Kernel(KernelEffect::Recovery(effect)));
    }

    /// The decision to do nothing, and why.
    pub(crate) fn ignored(&mut self, reason: ReplicaIgnoreReason) {
        self.kind(EffectKind::Kernel(KernelEffect::Ignored {
            reason: KernelIgnoredReason::Replica(reason),
        }));
    }

    pub(crate) fn finish(self) -> Vec<Effect> {
        self.out
    }
}
