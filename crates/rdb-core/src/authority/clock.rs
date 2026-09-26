//! The two admission conjuncts, and the only place in the workspace that compares anything
//! against a grant's expiry `E`.
//!
//! Pure: every function here takes its inputs as values and reads no state of its own, so a
//! fixture can drive the whole admission rule without constructing an [`crate::authority::Authority`]
//! (team kernel-a `design.md` §2.3).
//!
//! # There is no `ClockSample` type, and that is a ruling, not an omission
//!
//! Team kernel-a `design.md` §2.1 declares `ClockSample { at, utc_ms, epsilon_ms, valid }`.
//! Lead ruling **A-R27** refuses it: [`ControlTime`] is that struct field for field —
//! `sampled_at`/`at`, `estimate`/`utc_ms`, `error_millis`/`epsilon_ms`,
//! `bound_established`/`valid` — and [`crate::contracts::event::StepCtx::control_time`] already
//! hands one to `step` on every call. A fourth copy would be a second spelling of
//! `ctx.control_time` that drifts from it. So [`ClockView`] holds an `Option<ControlTime>` and
//! A1 reads `ctx`; there is **no clock event and no tick event**, and the periodic wake is an
//! [`crate::contracts::event::EventKind::Timer`] A1 arms itself.
//!
//! # Every threshold is a [`Budgets`] field, and none is a private constant (lead ruling A-R36)
//!
//! `max_sample_age_millis`, `clock_rate_ppm` and `clock_sample_period_millis` were constants in this
//! module until A-R36. They are `Budgets` fields now for one reason: a scenario that changes the
//! grant duration and not the sample window is testing a combination no operator can configure,
//! and a constant that lives beside the rule it feeds is a constant nobody re-reads. Do not move
//! one back here for convenience.
//!
//! # Where each term comes from (finding K-A-01)
//!
//! `delta` is configuration ([`Budgets::dispatch_margin_millis`]). `epsilon` is the **sample's**
//! [`ControlTime::error_millis`], and [`Budgets::clock_error_millis`] is only a *ceiling* on it.
//! Comparing against the configured number and never reading the sample would make the whole
//! epsilon/delta contract decorative: a sample reporting a five-second bound would be treated as
//! 100 ms, while the takeover side activates at `E + epsilon + delta` computed from the same
//! configured number — so A1 and the oracle would agree and both be wrong.
//!
//! The drift allowance is added **after** the ceiling check, so `effective_epsilon` may return a
//! value above [`Budgets::clock_error_millis`] for an old sample. That is conservative on both
//! sides of spec §7.2 — a wider epsilon tightens old-owner admission and delays new activation —
//! so it is not a leak past the bound. Clamping the *result* would be the defect; do not "fix" it.
//!
//! # No unchecked arithmetic (finding K-A-08)
//!
//! Every subtraction here saturates, and a sample stamped ahead of the processing tick is
//! *invalid* rather than zero-age. A sample's fields are event data a scenario sets freely, and a
//! panic would reach the campaign runner as a harness crash with nothing to shrink.

use crate::contracts::authority::{DenyReason, Revocation};
use crate::contracts::event::Budgets;
use crate::contracts::time::{ControlTime, Tick};

/// Whether the node's bounded-clock mode is configured at all.
///
/// A configuration input, not an observation, which is what makes it different from a sample
/// that fails its guard. Spec §7.2: a node that cannot establish an error bound "stops accepting
/// requests"; it does **not** fence, because there is no grant-ending event — the node simply
/// denies every check until the mode is bounded.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClockMode {
    /// An error bound can be established. The default: a node is assumed configured until a
    /// scenario says otherwise.
    #[default]
    Bounded,
    /// No error bound can be established. Every [`utc_ok`] on a **valid** sample answers
    /// [`DenyReason::ClockModeUnbounded`] — a **denial**, and not [`DenyReason::ClockUnbounded`],
    /// which fences (lead ruling A-R42). A *rejected* or retracted sample still answers
    /// `ClockUnbounded` and fences, as in `Bounded` mode (finding F1): the mode says the node
    /// cannot bound its clock, not that a reading the subsystem disowned is fine.
    Unbounded,
}

/// The clock, as A1 holds it.
///
/// Lives on the kernel rather than inside `Held` (the K-A-39 sweep): samples arrive in every
/// state, and an `Unheld` kernel needs one to compute the `E_new` it would acquire with. The
/// round-1 note kept it inside `Held`, which left `Unheld` with no sample at all.
///
/// [`ClockMode::Bounded`] carries no numbers (finding K-A-01): there is exactly one source for
/// each term, and the mode is not one of them.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ClockView {
    mode: ClockMode,
    sample: Option<ControlTime>,
}

impl ClockView {
    /// A bounded-mode view holding no sample. The entry state.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            mode: ClockMode::Bounded,
            sample: None,
        }
    }

    /// Whether an error bound can be established on this node at all.
    #[must_use]
    pub const fn mode(&self) -> ClockMode {
        self.mode
    }

    /// The last **accepted** sample, or `None` if there is none or the last one was retracted.
    ///
    /// `None` is what makes [`e_new`] `None`, which is what makes an acquisition withhold its
    /// CAS instead of writing an expiry it cannot justify (finding K-A-50).
    #[must_use]
    pub const fn sample(&self) -> Option<ControlTime> {
        self.sample
    }

    /// Set the configured mode. A scenario input; nothing inside the kernel calls this.
    pub const fn set_mode(&mut self, mode: ClockMode) {
        self.mode = mode;
    }

    /// Record an accepted sample.
    pub const fn accept(&mut self, sample: ControlTime) {
        self.sample = Some(sample);
    }

    /// Drop the held sample.
    ///
    /// A good sample followed by a bad one is the **no-sample** condition, not the good-sample
    /// one (finding K-A-50): the clock subsystem has declared its bound gone, and a later
    /// `E_new` must not be derived from the reading it just disowned. A grant's `E` is the one
    /// number that crosses machines — it is the input to another node's `C_auth > E + eps + delta`
    /// — and it is never written from nothing.
    pub const fn retract(&mut self) {
        self.sample = None;
    }
}

/// Why [`effective_epsilon`] refused a sample.
///
/// Two outcomes, not one, because they end differently: `Terminal` is one of rDB ADR-rdb-0007
/// §3's listed fence triggers and `Stale` is a denial the next fresh sample undoes (lead ruling
/// A-R12). Collapsing them terminally fenced a healthy primary on one late sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClockFault {
    /// The bound is gone, the sample is stamped in the future, or its error exceeds the
    /// configured ceiling. Fences.
    Terminal,
    /// The sample is simply old. Denies, and recovers on the next fresh one.
    Stale,
}

/// The one place epsilon is decided: the sample's own error, widened by the drift accrued since
/// it was taken.
///
/// # Errors
///
/// [`ClockFault::Terminal`] when no bound is established, when the sample is stamped after
/// `now` (finding K-A-08), or when its error exceeds [`Budgets::clock_error_millis`];
/// [`ClockFault::Stale`] when it is older than [`Budgets::max_sample_age_millis`].
pub const fn effective_epsilon(
    sample: &ControlTime,
    now: Tick,
    budgets: &Budgets,
) -> Result<u64, ClockFault> {
    if !sample.bound_established {
        return Err(ClockFault::Terminal);
    }
    if sample.sampled_at.0 > now.0 {
        return Err(ClockFault::Terminal);
    }
    if sample.error_millis > budgets.clock_error_millis {
        return Err(ClockFault::Terminal);
    }
    if now.0.saturating_sub(sample.sampled_at.0) > budgets.max_sample_age_millis {
        return Err(ClockFault::Stale);
    }
    Ok(drift_epsilon(sample, now, budgets))
}

/// The sample's own error widened by the drift accrued since it was taken, with no guard at all.
///
/// The arithmetic half of [`effective_epsilon`], shared with [`is_backward_jump`], which must
/// judge both samples whatever their age (lead rulings A-R54.3, A-R56.2). One formula, so the
/// tolerance a jump is judged by cannot drift from the epsilon admission is judged by.
const fn drift_epsilon(sample: &ControlTime, now: Tick, budgets: &Budgets) -> u64 {
    let age = now.0.saturating_sub(sample.sampled_at.0);
    let drift = age.saturating_mul(budgets.clock_rate_ppm) / 1_000_000;
    sample.error_millis.saturating_add(drift)
}

/// The sample's estimate of the authority clock, carried forward to `now`.
#[must_use]
pub const fn extrapolated_utc_ms(sample: &ControlTime, now: Tick) -> i64 {
    let elapsed = now.0.saturating_sub(sample.sampled_at.0);
    // `Tick` is milliseconds since the start of the run and `estimate` is the authority-clock
    // estimate on that same scale, so this is a widening cast that saturates at `i64::MAX`.
    let taken_at = if sample.estimate.0 > i64::MAX as u64 {
        i64::MAX
    } else {
        sample.estimate.0 as i64
    };
    let since = if elapsed > i64::MAX as u64 {
        i64::MAX
    } else {
        elapsed as i64
    };
    taken_at.saturating_add(since)
}

/// The conservative local conjunct: the Chubby client-lease rule.
///
/// Reads the monotonic tick only, so it is immune to UTC error entirely — but **not** to
/// suspension, which arrives separately as [`crate::contracts::event::NodeLifecycle::Resumed`].
///
/// `renewed_at` is the tick the last committed renewal CAS was **dispatched**, never the tick it
/// completed (finding K-A-06), so this under-counts nothing: anchoring at completion would omit
/// the control round trip and make the conjunct optimistic by exactly the delay an adversarial
/// scenario injects.
#[must_use]
pub const fn local_ok(renewed_at: Tick, now: Tick, budgets: &Budgets) -> bool {
    let elapsed = now.0.saturating_sub(renewed_at.0);
    elapsed.saturating_add(budgets.dispatch_margin_millis) < budgets.grant_millis
}

/// Spec §7.2's bounded-clock rule, `C_old < E - epsilon - delta`.
///
/// Takes the view and the expiry as values rather than a `Held`, so the steady-state check and
/// the adopt rows — which have a record's `E` and no `Held` at all — are the same call (the
/// K-A-39 sweep). There is exactly one function in this workspace that compares a holder's own
/// `E`, and this is it; its taking-over mirror, against a frozen prior `F`, is [`expiry_proven`].
///
/// # Three refusals, not two, and the split is load-bearing (lead ruling A-R42)
///
/// This function used to answer [`DenyReason::ClockUnbounded`] for the configured *mode* and for
/// [`ClockFault::Terminal`] alike, and `Authority::revalidate` fences on that value with no way
/// to tell them apart — so `set_mode(Unbounded)` terminally fenced a healthy primary, against
/// [`ClockMode`]'s own doc. **That re-collapsed what [`ClockFault`] two screens up exists to
/// split**, and it is the same reading the kernel-a architect rejected once for the *renewal*
/// guard, where the fix was to guard on `e_new is None` — the sample, never the mode. The fix is
/// here rather than in the caller: a caller re-deriving a distinction the returned value threw
/// away is the shape that caused this.
///
/// # Errors
///
/// In this order: [`DenyReason::ClockUnbounded`] when there is no sample or when
/// [`effective_epsilon`] refuses terminally — both fence, in either mode (finding F1);
/// [`DenyReason::ClockModeUnbounded`] when the **configured mode** is [`ClockMode::Unbounded`]
/// — denies, never fences; [`DenyReason::ClockSampleStale`] when it refuses on age;
/// [`DenyReason::Expired`] when the comparison itself fails.
pub const fn utc_ok(
    clock: &ClockView,
    expiry_utc_ms: i64,
    now: Tick,
    budgets: &Budgets,
) -> Result<(), DenyReason> {
    // **The sample is judged before the mode** (finding F1, lead ruling on the phase-2 gate). A
    // rejected sample fences `ClockUnbounded` in *either* mode; the mode only decides what a
    // valid sample does. The mode used to be checked first, so in `Unbounded` mode a backward
    // jump merely retracted, the next sample was accepted against nothing, renewals wrote an `E`
    // from the jumped clock, and a return to `Bounded` admitted on an expiry already in the true
    // past (tester probe `finding_1e`).
    //
    // **No sample stays on `ClockUnbounded`, and therefore keeps fencing** (lead ruling A-R42
    // left this side open; decided here). It is not a third case that happens to share a name:
    // it is how the rejected-sample fence fires at all. `Authority::absorb_sample` calls
    // [`ClockView::retract`] on a rejected sample and returns **no** fence of its own — its
    // comment says "the fence is the conjunct row's, one frame up" — so the row below is the
    // only thing that ends the grant for a sample with no bound, a future stamp, an error over
    // the ceiling, or a backward jump. All four are ADR-rdb-0007 §3 fence triggers, and
    // [`ClockView::retract`]'s own doc (finding K-A-50) says the subsystem "has declared its
    // bound gone", which reads `ClockFault::Terminal`. Moving this arm to the deny-only reason
    // would silently disarm four triggers and the plan rows that assert them
    // (`M7A-40`/`41`/`42`/`165` each assert `Fence{Node, ClockUnbounded}` **and**
    // `clock.sample == None` in the same breath).
    //
    // It cannot fire on a node that never held a sample: `revalidate` returns before this unless
    // the kernel is `Held`, and a grant is only entered through an `e_new` that a sample
    // produced. No sample while `Held` therefore always means a sample was retracted.
    let Some(sample) = clock.sample else {
        return Err(DenyReason::ClockUnbounded);
    };
    let epsilon = effective_epsilon(&sample, now, budgets);
    if matches!(epsilon, Err(ClockFault::Terminal)) {
        return Err(DenyReason::ClockUnbounded);
    }
    // Only now the mode: a valid sample in `Unbounded` mode denies and never fences, and so
    // does a merely stale one — staleness is a denial in `Bounded` mode too (lead ruling A-R12).
    if !matches!(clock.mode, ClockMode::Bounded) {
        return Err(DenyReason::ClockModeUnbounded);
    }
    let epsilon = match epsilon {
        Ok(epsilon) => epsilon,
        Err(_) => return Err(DenyReason::ClockSampleStale),
    };
    let c_now = extrapolated_utc_ms(&sample, now);
    let slack = epsilon.saturating_add(budgets.dispatch_margin_millis);
    let slack = if slack > i64::MAX as u64 {
        i64::MAX
    } else {
        slack as i64
    };
    if c_now < expiry_utc_ms.saturating_sub(slack) {
        Ok(())
    } else {
        Err(DenyReason::Expired)
    }
}

/// Spec §7.3 step 3 from the taking-over side: `C_auth > F + epsilon + delta` for a **frozen**
/// prior grant's final expiry `F` (team kernel-a `design.md` §2.6a T9).
///
/// The mirror image of [`utc_ok`], and deliberately built from the same three terms: the
/// sample's [`effective_epsilon`], [`extrapolated_utc_ms`] and [`Budgets::dispatch_margin_millis`].
/// Were the two sides to derive epsilon differently, an old owner could still admit at an instant
/// the new one had already proven past.
///
/// `None` — no proof yet, never a refusal — unless the sample is admissible exactly as [`utc_ok`]
/// requires: the mode is [`ClockMode::Bounded`], a sample is held, and [`effective_epsilon`]
/// accepts it (bound established, not stamped ahead, within the ceiling, fresh). Also `None` when
/// epsilon or delta does not fit the proof's `u32` fields: a truncated term would let a reviewer
/// re-derive an inequality that does not hold, and withholding only delays activation.
#[must_use]
pub fn expiry_proven(
    clock: &ClockView,
    frozen_expiry_utc_ms: i64,
    now: Tick,
    budgets: &Budgets,
) -> Option<Revocation> {
    if !matches!(clock.mode, ClockMode::Bounded) {
        return None;
    }
    let sample = clock.sample?;
    let epsilon = effective_epsilon(&sample, now, budgets).ok()?;
    let epsilon_ms = u32::try_from(epsilon).ok()?;
    let delta_ms = u32::try_from(budgets.dispatch_margin_millis).ok()?;
    let authority_utc_ms = extrapolated_utc_ms(&sample, now);
    let threshold = frozen_expiry_utc_ms
        .saturating_add(i64::from(epsilon_ms))
        .saturating_add(i64::from(delta_ms));
    (authority_utc_ms > threshold).then_some(Revocation::ExpiryProven {
        frozen_expiry_utc_ms,
        authority_utc_ms,
        authority_tick: now,
        epsilon_ms,
        delta_ms,
    })
}

/// `E_new` for a renewal or an acquisition CAS **dispatched** at `now`.
///
/// `Some` only for a valid, fresh, in-bound sample (ADR-rdb-0007 §2; findings K-A-05, K-A-36).
/// No sample, no `E_new`, no CAS — the caller withholds the write rather than building one on a
/// number it cannot justify.
///
/// It is never `E + grant_duration`. Renewing every 500 ms with a 3 s duration under that rule
/// drives `E` ahead of real time at six times the clock rate without bound: safe, but it
/// destroys the bounded takeover wait that spec §7.3 exists to give.
#[must_use]
pub const fn e_new(clock: &ClockView, now: Tick, budgets: &Budgets) -> Option<i64> {
    let Some(sample) = clock.sample else {
        return None;
    };
    if effective_epsilon(&sample, now, budgets).is_err() {
        return None;
    }
    let grant = if budgets.grant_millis > i64::MAX as u64 {
        i64::MAX
    } else {
        budgets.grant_millis as i64
    };
    Some(extrapolated_utc_ms(&sample, now).saturating_add(grant))
}

/// The tick an adopted record's `E` says its CAS was dispatched at: `tick_of(E − grant_millis)`
/// through the sample (team kernel-a `design.md` §2.4, "the adopt path does not restart the local
/// window"; finding K-A-06).
///
/// The inverse of [`e_new`]: a record written by a CAS dispatched at tick `t` under this sample
/// has `E = extrapolated_utc_ms(t) + grant_millis`, and this answers `t`. A row that adopts a
/// record it did not see committed sets `renewed_at` from this and never from `now`, because
/// `now` after an arbitrarily delayed read-back hands out a full `grant_millis` of local window on
/// a grant that has nearly expired in true time.
///
/// Clamped to `now` and to zero, and both clamps are the conservative direction: an earlier
/// `renewed_at` only shortens the window [`local_ok`] grants. `None` when there is no sample, in
/// which case the caller must not adopt at all — [`utc_ok`] already refuses without one.
#[must_use]
pub const fn renewed_at_for(
    clock: &ClockView,
    expiry_utc_ms: i64,
    now: Tick,
    budgets: &Budgets,
) -> Option<Tick> {
    let Some(sample) = clock.sample else {
        return None;
    };
    let grant = if budgets.grant_millis > i64::MAX as u64 {
        i64::MAX
    } else {
        budgets.grant_millis as i64
    };
    let taken_at = if sample.estimate.0 > i64::MAX as u64 {
        i64::MAX
    } else {
        sample.estimate.0 as i64
    };
    // How far after the sample the dispatch was, on the authority clock; the tick moves with it.
    let after_sample = expiry_utc_ms.saturating_sub(grant).saturating_sub(taken_at);
    let tick = if after_sample < 0 {
        sample
            .sampled_at
            .0
            .saturating_sub(after_sample.unsigned_abs())
    } else {
        sample
            .sampled_at
            .0
            .saturating_add(after_sample.unsigned_abs())
    };
    Some(Tick(if tick < now.0 { tick } else { now.0 }))
}

/// Whether `next` puts the authority clock **behind** where `previous` said it would be, by more
/// than the error bound can explain.
///
/// A backward jump is one of ADR-rdb-0007 §3's fence triggers, and it is the one that cannot be
/// seen in a single sample: it is a relation between two. That is the reason [`ClockView`] holds
/// the previous sample at all (lead ruling A-R27) — `ctx.control_time` alone cannot express it.
///
/// Judged on the two samples' estimates carried forward to the same tick, so a sample that is
/// merely *older* is not mistaken for one that went backwards. The tolerance is **both** samples'
/// error plus drift since each was taken (lead ruling A-R56.2, finding J1): each estimate is an
/// interval, and the clock went backwards only if the new interval lies wholly behind the old one.
/// A jump inside the two bounds the environment claims is not a jump, it is the bounds doing their
/// job. Before A-R56 only the new sample's bound counted, so a precise sample after a loose one
/// fenced a clock that had not moved.
///
/// **Judged whatever the sample's age** (lead ruling A-R54.3, finding N2). This used to answer
/// "no jump" whenever [`effective_epsilon`] refused `next`, on the claim that such a sample is
/// refused before this is consulted. That held for the terminal refusals and not for the stale
/// one, which `Authority::absorb_sample` deliberately accepts: a stale sample 1000 ms behind was
/// accepted as no jump, and the next fresh sample was then judged against it, so the jump was
/// never seen. The terminal refusals are still the caller's to make first.
#[must_use]
pub fn is_backward_jump(
    previous: &ControlTime,
    next: &ControlTime,
    now: Tick,
    budgets: &Budgets,
) -> bool {
    let epsilon =
        drift_epsilon(previous, now, budgets).saturating_add(drift_epsilon(next, now, budgets));
    let was = extrapolated_utc_ms(previous, now);
    let is = extrapolated_utc_ms(next, now);
    let tolerance = if epsilon > i64::MAX as u64 {
        i64::MAX
    } else {
        epsilon as i64
    };
    was.saturating_sub(is) > tolerance
}
