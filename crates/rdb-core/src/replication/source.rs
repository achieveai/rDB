//! The source side of a recovery catch-up (lead rulings B-R59, B-R59a; design §3.2a, §3.6).
//!
//! F1 names the copy that holds the selected prefix and asks it to send that prefix to a copy
//! that lags, through a routed [`KernelEvent::CatchUp`](crate::contracts::event::KernelEvent).
//! That copy is a secondary, not the primary, so it has no tracker: its [`Source`] runs the
//! primary's own [`CatchupCursor`] against the receiver's ladder, and every record it asks for
//! leaves as [`KernelEffect::SendRecoveryEnvelopes`] carrying F1's credential unchanged. The
//! target's rows 5R, 6R and 6R′ check that credential; the source never reads it.
//!
//! # The gate
//!
//! A primary's cursor trusts the ACKs it is given because the tracker's ladder admits them first.
//! A source has no tracker, so it gates the ACKs itself, with the same reasons: the label must be
//! authenticated and speak for itself (`ForgedIdentity`), and the ACK's digest must be the one
//! the source's ladder holds at that sequence (`DigestMismatch`, or `Unverifiable` where it holds
//! none). Routing sends a source only the replies labelled with its target's node; a reply from
//! any other node, on a node running sources and no primary, is `NotAMember`.
//!
//! # Life cycle
//!
//! A source is dropped when its cursor reports `CopyCaughtUp` or stops, and on `Recovered` for
//! its partition, because the cut it was sending towards may be gone. A later reply finds no
//! source. While it runs, the partition's retransmit timer re-sends a record whose ACK was lost,
//! exactly as it does for a primary's cursor (lead ruling B-R67).

use crate::contracts::authority::FenceCredential;
use crate::contracts::envelope::AppendOutcome;
use crate::contracts::event::{EffectKind, KernelEffect};
use crate::contracts::ids::Seq;
use crate::contracts::ignore::KernelIgnoredReason;
use crate::contracts::membership::Member;
use crate::contracts::trace::AckRejectReason;
use crate::contracts::transport::PeerLabel;
use crate::replication::catchup::CatchupCursor;
use crate::replication::progress::{DigestLadder, DigestLookup};
use crate::replication::{ignored, wire};

/// One running recovery catch-up, on the node of the copy that sends it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    to: Member,
    through: Seq,
    credential: FenceCredential,
    cursor: CatchupCursor,
}

impl Source {
    /// A source sending to `to` through `through` under `credential`, and the effects that start
    /// it: the record at `through`, which the target answers with `NeedPrefix` from its own
    /// head, or with `AlreadyHave` and its ACK when it holds it.
    #[must_use]
    pub fn start(to: Member, through: Seq, credential: FenceCredential) -> (Self, Vec<EffectKind>) {
        let mut source = Self {
            to,
            through,
            credential,
            cursor: CatchupCursor::new(to.copy),
        };
        let effects = source.cursor.start(through);
        let effects = source.rewrap(effects);
        (source, effects)
    }

    /// The copy being caught up.
    #[must_use]
    pub const fn to(&self) -> Member {
        self.to
    }

    /// Where the catch-up ends.
    #[must_use]
    pub const fn through(&self) -> Seq {
        self.through
    }

    /// The credential every send carries.
    #[must_use]
    pub const fn credential(&self) -> FenceCredential {
        self.credential
    }

    /// The cursor walking the target.
    #[must_use]
    pub const fn cursor(&self) -> &CatchupCursor {
        &self.cursor
    }

    /// Whether this source is finished and routing should drop it: it reported `CopyCaughtUp`
    /// in `effects`, or its cursor stopped.
    #[must_use]
    pub fn done(&self, effects: &[EffectKind]) -> bool {
        self.cursor.stopped().is_some()
            || effects.iter().any(|effect| {
                matches!(
                    effect,
                    EffectKind::Kernel(KernelEffect::CopyCaughtUp { .. })
                )
            })
    }

    /// Whether the cursor has a record it sent and no ACK has answered (lead ruling B-R67a).
    #[must_use]
    pub const fn awaits_ack(&self) -> bool {
        self.cursor.unacked().is_some()
    }

    /// One fire of the partition's retransmit timer: the cursor's re-send, if it makes one,
    /// leaving as the same recovery send under the same credential (lead rulings B-R67,
    /// B-R59a).
    pub fn on_retransmit(&mut self) -> Option<EffectKind> {
        let send = self.cursor.on_retransmit()?;
        self.rewrap(vec![send]).pop()
    }

    /// A reply frame (`RDBR`) from the target's node. `history` is the source receiver's ladder.
    pub fn on_reply(
        &mut self,
        from: &PeerLabel,
        body: &[u8],
        history: &DigestLadder,
    ) -> Vec<EffectKind> {
        let outcome = match wire::decode_reply(body) {
            Ok(outcome) => outcome,
            Err(error) => return vec![ignored(KernelIgnoredReason::Error(error.kind()))],
        };
        if !from.authenticated {
            return vec![refused(AckRejectReason::ForgedIdentity)];
        }
        if let AppendOutcome::Accepted(ack) = &outcome {
            if ack.from != from.node {
                return vec![refused(AckRejectReason::ForgedIdentity)];
            }
            let at = Seq(ack.progress.buffered_applied.0);
            match history.lookup(at, ack.digest_at_buffered) {
                DigestLookup::Match => {}
                DigestLookup::Differs { .. } => {
                    return vec![refused(AckRejectReason::DigestMismatch)]
                }
                DigestLookup::NotRetained => return vec![refused(AckRejectReason::Unverifiable)],
            }
        }
        let effects = self.cursor.on_outcome(outcome, history, self.through);
        self.rewrap(effects)
    }

    /// The cursor's sends leave as recovery sends under the credential, byte for byte (lead
    /// ruling B-R59a); every other effect passes unchanged.
    fn rewrap(&self, effects: Vec<EffectKind>) -> Vec<EffectKind> {
        effects
            .into_iter()
            .map(|effect| match effect {
                EffectKind::Kernel(KernelEffect::SendEnvelopes {
                    copy,
                    from,
                    through,
                }) => EffectKind::Kernel(KernelEffect::SendRecoveryEnvelopes {
                    copy,
                    from,
                    through,
                    credential: self.credential,
                }),
                other => other,
            })
            .collect()
    }
}

/// The recovery catch-up's own refusal of a reply, before its cursor sees it.
const fn refused(reason: AckRejectReason) -> EffectKind {
    ignored(KernelIgnoredReason::AckRejected(reason))
}
