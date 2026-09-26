//! Package A1 rows: the authority kernel's watch and coherent-resync slice.
//!
//! | Row | Claim |
//! |---|---|
//! | M7A-28 | a gap termination produces one `Reload` of the affected family, and the snapshot's revision is what the resumed `Watch` starts after |
//! | M7A-29 | a `Watched` run produces one linearizable `Get` per change and never a `Reload` — a watch invalidates a cache, it does not grant |
//! | M7A-32 | **no coherent family reload occurs unless a termination was delivered** (ADR-rdb-0008 §7 item 4, lead ruling A-R15), with a positive control in the same test |
//! | M7A-31 | `ResourceExhaustedFatal` is a capacity error, not a gap: a **bounded, non-decreasing** back-off, never a reload and never an immediate re-watch; at the cap A1 latches with `Fact(WatchAdmissionExhausted)` and no re-arm at all |
//!
//! Both M7A-31 rows were named `m7a_33_*` and were on `scripts/m7-census.sh`'s `MISCREDITED`
//! list: back-off and the cap are M7A-31's subject, never M7A-33's. They are renamed here rather
//! than patched, and only because `AuthorityTimer::WatchBackoff` is now built — an id comes off
//! `MISCREDITED` by its claim becoming true on disk, never to settle a count. **M7A-33 now has
//! zero functions and is correctly `owed`.**
//!
//! Log fields are revisions, ticks and counts; never a key or value byte.
//!
//! # Why these rows drive the real `ControlStore`
//!
//! `sim/control.rs`'s own header says it, under ADR-rdb-0008 §7 item 4: *"no silent gap"* is a
//! **kernel-side assertion, not a fake property** (lead ruling A-R15). `ControlOp::EmitWatch`
//! delivers contiguously and a gap is only ever a termination, so "the kernel reloaded without
//! being told to" is a claim about the kernel that only the kernel can falsify. Driving the
//! kernel with hand-built `ControlEvent` values would assert the same sentence against a fixture
//! of this file's own making; driving it through the store asserts it against the seam A1 will
//! actually meet.

mod support;

use bytes::Bytes;
use config_log::retcd_test;
use rdb_core::authority::{
    Authority, AuthorityTimer, WATCH_ADMISSION_ATTEMPT_CAP, WATCH_BACKOFF_CAP_MILLIS,
};
use rdb_core::contracts::authority::{AuthorityEffect, AuthorityFact, AuthorityIgnoreReason};
use rdb_core::contracts::control::{
    ControlEffect, ControlEvent, ControlKey, ControlPrefix, WatchTermination,
};
use rdb_core::contracts::event::{Effect, EffectKind, Event, EventKind, KernelEffect, Module};
use rdb_core::contracts::ids::{
    BootId, CorrelationId, EventId, NodeId, PartitionId, Revision, TimerVersion,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::time::{Tick, TimerEffect, TimerFired};
use rdb_sim::sim::control::{ControlOp, ControlStore};

const A: NodeId = NodeId(1);

/// One kernel under one control store, driven the way the dispatcher will drive it.
///
/// Holds no clock and no channel (team kernel-a `KA-1`): `step` is called with one event and the
/// returned vector is kept as a value. `reloads` is the counter `M7A-32` asserts on, and it is
/// incremented from the returned effects, never from the store.
struct Driver {
    kernel: Authority,
    store: ControlStore,
    now: Tick,
    next_event: u64,
    /// Every `ControlEffect::Reload` the kernel has returned since the last `take_reloads`.
    reloads: Vec<ControlPrefix>,
    /// Every `ControlEffect::Get` the kernel has returned since the last `take_gets`.
    gets: Vec<ControlKey>,
    /// Every `WatchTerminated` the store has delivered since the last `take_terminations`.
    ///
    /// Recorded because `ControlOp::TerminateWatch` ends **every** watch the node holds
    /// (`take_watches` in `sim/control.rs`), and A1 watches two families. The number of
    /// terminations is therefore a fact of the run, not a constant a row may hard-code — the
    /// first draft of `M7A-32` asserted "exactly 1 reload" and saw 2, because two families
    /// gapped and the kernel correctly reloaded both.
    terminations: Vec<(ControlPrefix, WatchTermination)>,
}

impl Driver {
    fn new() -> Self {
        Self {
            kernel: Authority::new(),
            store: ControlStore::new(),
            now: Tick::ZERO,
            next_event: 1,
            reloads: Vec::new(),
            gets: Vec::new(),
            terminations: Vec::new(),
        }
    }

    /// Wrap a control completion as the event the dispatcher would build from it.
    fn event_for(&mut self, control: ControlEvent) -> Event {
        let id = self.next_event;
        self.next_event += 1;
        Event {
            id: EventId(id),
            at: self.now,
            node: A,
            boot: BootId(1),
            partition: PartitionId(1),
            correlation: CorrelationId(1),
            kind: EventKind::Control(control),
        }
    }

    /// Feed one control completion to the kernel and record what it asked for.
    ///
    /// Returns the effects, so a row can assert on the vector directly (team kernel-a `KA-4`
    /// surface 1) rather than only on the counters.
    fn deliver(&mut self, control: ControlEvent) -> Vec<Effect> {
        if let ControlEvent::WatchTerminated {
            prefix,
            termination,
            ..
        } = &control
        {
            self.terminations.push((*prefix, *termination));
        }
        let event = self.event_for(control);
        let effects = self
            .kernel
            .step(&support::ctx(), &event)
            .expect("the control seam is wired");
        for effect in &effects {
            match &effect.kind {
                EffectKind::Control(ControlEffect::Reload { prefix }) => {
                    self.reloads.push(*prefix);
                }
                EffectKind::Control(ControlEffect::Get { key }) => self.gets.push(*key),
                _ => {}
            }
        }
        effects
    }

    /// Hand every control effect in `effects` to the store, then deliver what it completes.
    ///
    /// One round only: the effects a delivery produces are returned rather than fed back, so a
    /// row decides how far to drive the loop.
    fn submit_and_complete(&mut self, effects: &[Effect]) -> Vec<Effect> {
        for effect in effects {
            if matches!(effect.kind, EffectKind::Control(_)) {
                self.store.submit(A, effect).expect("submit");
            }
        }
        let mut produced = Vec::new();
        for completion in self.store.complete(self.now) {
            produced.extend(self.deliver(completion.event));
        }
        produced
    }

    /// Commit a create-only record, so the watch on its family has a real change to deliver.
    fn create(&mut self, key: ControlKey, value: &'static [u8]) {
        let effect = support::control_effect(
            1,
            ControlEffect::Cas {
                key,
                expected: None,
                value: Some(Bytes::from_static(value)),
            },
        );
        self.store.submit(A, &effect).expect("create");
    }

    /// Drain the store's queue without showing the kernel anything.
    fn discard_completions(&mut self) {
        let _ = self.store.complete(self.now);
    }

    fn take_reloads(&mut self) -> Vec<ControlPrefix> {
        std::mem::take(&mut self.reloads)
    }

    fn take_gets(&mut self) -> Vec<ControlKey> {
        std::mem::take(&mut self.gets)
    }

    /// Every `Watch { prefix, from }` in `effects`, sorted — the re-arms a termination produced.
    fn watches(effects: &[Effect]) -> Vec<(ControlPrefix, Revision)> {
        let mut watches: Vec<_> = effects
            .iter()
            .filter_map(|effect| match &effect.kind {
                EffectKind::Control(ControlEffect::Watch { prefix, from }) => {
                    Some((*prefix, *from))
                }
                _ => None,
            })
            .collect();
        watches.sort_unstable();
        watches
    }

    /// Every `AuthorityIgnoreReason` in `effects`, in order.
    ///
    /// Destructures through [`KernelIgnoredReason::Authority`] rather than matching the whole
    /// effect, so a reason on another kernel's arm is not silently counted as one of A1's.
    fn ignores(effects: &[Effect]) -> Vec<AuthorityIgnoreReason> {
        effects
            .iter()
            .filter_map(|effect| match &effect.kind {
                EffectKind::Kernel(KernelEffect::Ignored {
                    reason: KernelIgnoredReason::Authority(reason),
                }) => Some(reason.clone()),
                _ => None,
            })
            .collect()
    }

    /// Every `AuthorityFact` in `effects`, in order.
    fn facts(effects: &[Effect]) -> Vec<AuthorityFact> {
        effects
            .iter()
            .filter_map(|effect| match &effect.kind {
                EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fact(fact))) => {
                    Some(fact.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Every `TimerEffect::Arm` in `effects` whose id is `kind`, as `(version, at)`.
    ///
    /// Filtered by kind and not merely by "is a timer": A1 owns four, and the clock wake re-arms
    /// itself on every firing, so an unfiltered count would read a clock wake as a back-off.
    fn arms(effects: &[Effect], kind: AuthorityTimer) -> Vec<(TimerVersion, Tick)> {
        effects
            .iter()
            .filter_map(|effect| match &effect.kind {
                EffectKind::Timer(TimerEffect::Arm { id, version, at }) if *id == kind.id() => {
                    Some((*version, *at))
                }
                _ => None,
            })
            .collect()
    }

    /// Fire `kind` at the version the kernel currently has armed, and return its effects.
    ///
    /// The current version deliberately, so this is the *live* firing and never a `StaleTimer`:
    /// a row that means to drive the back-off must not accidentally test the staleness guard.
    fn fire(&mut self, kind: AuthorityTimer) -> Vec<Effect> {
        let fired = TimerFired {
            id: kind.id(),
            version: self.kernel.timer_version(kind),
            scheduled_at: self.now,
        };
        let id = self.next_event;
        self.next_event += 1;
        let event = Event {
            id: EventId(id),
            at: self.now,
            node: A,
            boot: BootId(1),
            partition: PartitionId(1),
            correlation: CorrelationId(1),
            kind: EventKind::Timer(fired),
        };
        let effects = self
            .kernel
            .step(&support::ctx(), &event)
            .expect("the timer seam is wired");
        for effect in &effects {
            match &effect.kind {
                EffectKind::Control(ControlEffect::Reload { prefix }) => {
                    self.reloads.push(*prefix);
                }
                EffectKind::Control(ControlEffect::Get { key }) => self.gets.push(*key),
                _ => {}
            }
        }
        effects
    }

    /// The families that have gapped since the last call, sorted — the exact set a correct
    /// kernel must reload, one reload each.
    fn take_gapped_families(&mut self) -> Vec<ControlPrefix> {
        let mut gapped: Vec<_> = std::mem::take(&mut self.terminations)
            .into_iter()
            .filter(|(_, termination)| termination.is_gap())
            .map(|(prefix, _)| prefix)
            .collect();
        gapped.sort_unstable();
        gapped
    }

    /// Inject a termination, deliver every completion it produces, and return the kernel's
    /// effects.
    fn terminate(&mut self, termination: WatchTermination) -> Vec<Effect> {
        self.store
            .inject(ControlOp::TerminateWatch {
                node: A,
                termination,
            })
            .expect("the node has at least one watch open to terminate");
        let mut produced = Vec::new();
        for completion in self.store.complete(self.now) {
            produced.extend(self.deliver(completion.event));
        }
        produced
    }

    /// Hand the kernel's re-armed `Watch` effects back to the store, so the next termination
    /// has something to terminate. A watch the kernel re-arms but nobody opens is not a
    /// re-arm, and `take_watches` refuses an empty set with `SimError::Config`.
    fn reopen(&mut self, effects: &[Effect]) {
        for effect in effects {
            if matches!(
                effect.kind,
                EffectKind::Control(ControlEffect::Watch { .. })
            ) {
                self.store.submit(A, effect).expect("reopen watch");
            }
        }
        let _ = self.store.complete(self.now);
    }

    /// Drive the kernel from `Unheld` to `Held` with both families watched.
    ///
    /// This is the acquisition row's effect sequence, and it is where the **only** legitimate
    /// reload outside a gap happens: a freshly adopted grant has no coherent view of the
    /// partitions family, so it loads one. Rows that count reloads call `take_reloads` after
    /// this to start from a clean counter — the point of `M7A-32` is reloads that happen *while
    /// a healthy stream is delivering*, not the one that opened the stream.
    ///
    /// Through the real acquisition (lead ruling A-R47): the live `AcquireDue` makes A1 issue its
    /// own create-only CAS on `grants/{node}`, the store commits it, and A1 adopts on that
    /// completion. Until A-R47 this submitted a hand-built CAS, whose commit A1 now ignores as a
    /// completion for a CAS it never issued.
    fn become_held(&mut self) {
        let grant = self.fire(AuthorityTimer::Acquire);
        let adopted = self.submit_and_complete(&grant);
        assert!(
            self.kernel.state().is_held(),
            "a committed create-only CAS on grants/{{node}} adopts the grant"
        );

        // The adoption emits `Reload{Partitions}` and `Watch{Grants}`. Completing the reload
        // yields a `FamilySnapshot`, and the kernel answers that with `Watch{Partitions}` from
        // the snapshot revision — the closed loop M7A-28 asserts.
        let after_snapshot = self.submit_and_complete(&adopted);
        let _ = self.submit_and_complete(&after_snapshot);
        assert!(
            self.kernel.cursor(ControlPrefix::Partitions).is_some(),
            "the partitions family is watched after its coherent load"
        );
    }
}

/// M7A-29 — a watch invalidates a cache; it never grants, and it never reloads.
#[retcd_test]
fn m7a_29_watched_run_reads_each_change_and_never_reloads() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();
    let _ = driver.take_gets();

    driver.create(ControlKey::Partition(PartitionId(7)), b"p7");
    driver.discard_completions();
    driver
        .store
        .inject(ControlOp::EmitWatch { node: A })
        .expect("emit");
    for completion in driver.store.complete(driver.now) {
        let _ = driver.deliver(completion.event);
    }

    assert_eq!(
        driver.take_reloads(),
        Vec::<ControlPrefix>::new(),
        "a contiguous watch run is not a gap and must not provoke a reload"
    );
    assert!(
        driver
            .take_gets()
            .contains(&ControlKey::Partition(PartitionId(7))),
        "each change is followed by a linearizable read of that record"
    );
}

/// M7A-32 — no coherent family reload occurs unless a termination was delivered.
///
/// ADR-rdb-0008 §7 item 4 as restated by lead ruling A-R15, verbatim: *"no coherent family
/// reload occurs unless a termination was delivered"*. Two arms on **one** kernel, and both
/// numbers are load-bearing.
///
/// **Arm 1 (the claim).** 200 `EmitWatch` deliveries carrying real `ControlChange`s, interleaved
/// with 50 `EmitProgress` watermarks, and no `TerminateWatch` at any point: the reload count
/// across all 250 deliveries must be **0**. Zero is correct because `Reload` is the sanctioned
/// answer to a **gap**, the only thing that declares a gap is `WatchTermination::is_gap`, and
/// rEtcd's stream does not skip silently (ADR-rdb-0008 §4) — so a live stream has nothing to
/// reload against. A kernel that reloads on a `Watched` or a `WatchProgress` turns cache
/// invalidation into a poll, which is the defect this item exists to catch.
///
/// **Arm 2 (the positive control).** One `TerminateWatch{RevisionCompacted}` on the same kernel,
/// immediately after: exactly **1** reload, for the terminated family only. Without this arm a
/// reload count of zero is unfalsifiable by inspection, because a row cannot tell a kernel that
/// never reloads from a counter that never moves. Round 4 of the plan asserted arm 1 over
/// `Control(Get{family})` — a shape `ControlKey` cannot express — so it was zero in every
/// possible run including the reload-on-every-event run, and §12 reported the ADR item covered
/// while nothing tested it.
#[retcd_test]
fn m7a_32_no_read_family_without_a_termination() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();

    // ---- Arm 1: 250 healthy deliveries, no termination anywhere. ----
    for i in 0..200_u32 {
        driver.create(ControlKey::Partition(PartitionId(1_000 + i)), b"p");
        driver.discard_completions();
        driver
            .store
            .inject(ControlOp::EmitWatch { node: A })
            .expect("emit watch");
        for completion in driver.store.complete(driver.now) {
            let _ = driver.deliver(completion.event);
        }
        if i % 4 == 0 {
            driver
                .store
                .inject(ControlOp::EmitProgress { node: A })
                .expect("emit progress");
            for completion in driver.store.complete(driver.now) {
                let _ = driver.deliver(completion.event);
            }
        }
    }

    assert_eq!(
        driver.take_reloads(),
        Vec::<ControlPrefix>::new(),
        "arm 1: 200 contiguous watch runs and 50 progress watermarks declare no gap, \
         so the kernel must not reload once"
    );

    // ---- Arm 2: the positive control, same kernel, immediately after. ----
    let _ = driver.terminate(WatchTermination::RevisionCompacted {
        minimum_available_revision: Revision(1),
    });

    let gapped = driver.take_gapped_families();
    let mut reloads = driver.take_reloads();
    reloads.sort_unstable();

    assert!(
        !gapped.is_empty(),
        "arm 2 is only a control if a gap was actually delivered"
    );
    assert_eq!(
        reloads, gapped,
        "arm 2: exactly one reload per gapped family, naming that family — if this is empty \
         the arm-1 counter was never live and arm 1 proved nothing"
    );
}

/// M7A-31 — a capacity refusal answers with a **bounded, non-decreasing** back-off, never a
/// reload and never an immediate re-watch.
///
/// # This function was named `m7a_33_admission_refused_backs_off_and_never_reloads`
///
/// It was on `scripts/m7-census.sh`'s `MISCREDITED` list, because back-off and the cap are
/// M7A-31's subject and never M7A-33's — M7A-33 is the `revoked_epochs` / `partitions_revision`
/// row. The rename is not a rename onto the nearest free id: **M7A-31 now has a real subject**,
/// because `AuthorityTimer::WatchBackoff` is built. Until it was, the under-cap arm re-watched
/// immediately and "bounded backoff, non-decreasing, `backoff_20 == cap`" had nothing to assert
/// against — so what this function used to assert was the *stub*, which is exactly what made it
/// a miscredit rather than a miss.
///
/// # Three spellings of one name, and the contract's wins
///
/// `design.md:1087` calls the under-cap fact `WatchAdmissionRefused`; plan row M7A-31 writes
/// `Fact(AdmissionRefused)`; the landed variant is `AuthorityIgnoreReason::AdmissionRefused`,
/// which is an **ignore reason and not a fact**. The design's name exists nowhere in the
/// contracts. This row asserts the contract's spelling. That is the second homograph on this one
/// plan row — the first is `design.md` §2.4's input `WatchGap{AdmissionRefused}`, which is really
/// `WatchTermination::ResourceExhaustedFatal` (A-R44).
///
/// `ResourceExhaustedFatal` answers `false` to `is_gap`, and reloading in a loop on it turns a
/// capacity error into an outage. The twin that differs by exactly one fact (`KA-6`) is
/// `m7a_28_gap_termination_reloads_then_rewatches`, which sends a termination that *is* a gap.
#[retcd_test]
fn m7a_31_watch_admission_refusal_backs_off_and_never_reloads() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();

    let reopened = driver.terminate(WatchTermination::ResourceExhaustedFatal);
    let refused = driver.terminations.len();
    assert!(
        refused > 0,
        "the row is only a test if a termination was delivered"
    );

    assert_eq!(
        driver.take_gapped_families(),
        Vec::<ControlPrefix>::new(),
        "`ResourceExhaustedFatal` answers false to `is_gap` — it is a capacity error, not a gap"
    );
    assert_eq!(
        driver.take_reloads(),
        Vec::<ControlPrefix>::new(),
        "an admission limit must never provoke a reload: reloading in a loop on a capacity \
         error turns it into an outage"
    );
    assert_eq!(
        driver.kernel.watch_refused_attempts(),
        u32::try_from(refused).expect("a handful of terminations"),
        "every refused termination is counted, so the re-arm can be bounded"
    );

    // The under-cap vector, in the contract's spelling: the refusal is *said*, and the re-arm is
    // *timed*. Both families terminated, so there is one of each per family.
    assert_eq!(
        Driver::ignores(&reopened),
        vec![
            AuthorityIgnoreReason::AdmissionRefused,
            AuthorityIgnoreReason::AdmissionRefused
        ],
        "under the cap A1 declines and will retry, and says so — an effect vector that is only a \
         timer cannot be told from a kernel that armed one for something else"
    );
    assert_eq!(
        Driver::arms(&reopened, AuthorityTimer::WatchBackoff).len(),
        2,
        "one back-off arm per refused family: below the cap a back-off is a back-off, not a stop"
    );
    assert_eq!(
        Driver::watches(&reopened),
        Vec::new(),
        "and **no immediate re-watch**. Answering a capacity refusal by making the same request \
         again in the same instant is the admission-limit-as-outage shape the termination type \
         exists to spell out; this is the assertion that had no subject while the re-arm was \
         immediate, and the reason this function used to be miscredited to M7A-33"
    );

    // The delay is bounded and non-decreasing. The event path can only reach attempts 1 and 2
    // before the cap latches, so the two it reaches are asserted here and the shape of the rest
    // is asserted against `watch_backoff_millis` below. Asserting only the two would be a claim
    // about a curve from two of its points.
    let armed: Vec<u64> = Driver::arms(&reopened, AuthorityTimer::WatchBackoff)
        .into_iter()
        .map(|(_, at)| at.0 - driver.now.0)
        .collect();
    assert_eq!(
        armed,
        vec![
            Authority::watch_backoff_millis(1),
            Authority::watch_backoff_millis(2)
        ],
        "the two the event path reaches are the first two of the published curve, and not two \
         numbers that happen to look like a back-off"
    );

    let curve: Vec<u64> = (0..=20).map(Authority::watch_backoff_millis).collect();
    assert!(
        curve.windows(2).all(|pair| pair[0] <= pair[1]),
        "non-decreasing across every attempt, not merely across the two the cap lets through: \
         {curve:?}"
    );
    assert_eq!(
        Authority::watch_backoff_millis(20),
        WATCH_BACKOFF_CAP_MILLIS,
        "and bounded — `backoff_20 == cap`, which is the half of M7A-31 that says a back-off \
         cannot grow without limit"
    );
    assert!(
        Authority::watch_backoff_millis(1) < WATCH_BACKOFF_CAP_MILLIS,
        "positive control on the two assertions above: if the curve were the constant cap they \
         would both pass and neither would mean anything"
    );

    // Firing the back-off re-watches. Without this the row proves the kernel stopped, not that
    // it backed off — and those are the two different behaviours the cap exists to separate.
    let resumed = driver.fire(AuthorityTimer::WatchBackoff);
    assert_eq!(
        Driver::watches(&resumed).len(),
        2,
        "when the back-off elapses both refused families are re-watched"
    );
    assert_eq!(
        driver.take_reloads(),
        Vec::<ControlPrefix>::new(),
        "and the whole sequence completes without a single reload: a capacity error is not a gap, \
         so there is nothing to re-read"
    );
}

/// M7A-28 — a gap termination reloads the affected family, then re-watches from the snapshot.
#[retcd_test]
fn m7a_28_gap_termination_reloads_then_rewatches() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();

    let reload_effects = driver.terminate(WatchTermination::ResourceExhaustedResumable);

    let gapped = driver.take_gapped_families();
    let mut reloads = driver.take_reloads();
    reloads.sort_unstable();
    assert!(
        !gapped.is_empty(),
        "a gap must actually have been delivered"
    );
    assert_eq!(
        reloads, gapped,
        "a resumable-exhaustion termination is a gap: one reload per gapped family, no more"
    );

    // Completing the reload yields a coherent snapshot; the resumed watch starts after the
    // revision that snapshot was coherent at, which is what closes the loop.
    let rewatch = driver.submit_and_complete(&reload_effects);
    let resumed: Vec<_> = rewatch
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Control(ControlEffect::Watch { prefix, from }) => Some((*prefix, *from)),
            _ => None,
        })
        .collect();
    assert!(
        !resumed.is_empty(),
        "the reload must be followed by a resumed watch, or the gap is never closed"
    );
    for (prefix, from) in resumed {
        assert_eq!(
            Some(from),
            driver.kernel.cursor(prefix),
            "the resumed watch starts after the snapshot revision, not the old cursor"
        );
    }
}

/// M7A-31, second row — the cap is exactly 3, and reaching it **latches**: a
/// `Fact(WatchAdmissionExhausted)` and no re-arm at all.
///
/// # This function was named `m7a_33_admission_cap_is_exactly_three`
///
/// Renamed for the reason its sibling was: the cap is M7A-31's subject, it was on `MISCREDITED`,
/// and M7A-31 has a real subject now. The latch is the half the old function could not reach —
/// with the re-arm immediate there was no timer to *not* arm, so "stops re-arming" and "re-armed
/// with zero delay" were the same observation.
///
/// The old function drove its tail loop with `while ... < WATCH_ADMISSION_ATTEMPT_CAP`, so it
/// re-derived the cap from the same constant it was meant to check and could not notice the
/// constant moving to 2 or to 4 (manual-tester finding K4, 2026-09-21). That property is kept:
/// this row hardcodes the expected counts, so a changed cap value fails it.
///
/// `become_held` leaves both `Grants` and `Partitions` watched, and `watch_refused_attempts` is
/// one counter shared by every family (see `Driver::terminations`'s doc comment: one `terminate`
/// ends every open watch). So each `terminate` call advances the shared counter by 2, once per
/// family, and the cap is checked separately for each family's own increment within that call.
#[retcd_test]
fn m7a_31_watch_admission_cap_is_exactly_three_and_latches() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();

    // Said once, plainly, so that a cap moved to 2 or 4 fails *here* with the number in the
    // message rather than downstream as an unexplained count mismatch. This is the only place
    // the row names the constant: every assertion below is a literal, which is what finding K4
    // asked for — an assertion that reads its expected value out of the thing under test cannot
    // notice that thing changing.
    assert_eq!(
        WATCH_ADMISSION_ATTEMPT_CAP, 3,
        "M7A-31: the counts below are written for a cap of exactly 3"
    );

    // Call 1 carries the counter through 1, then 2 (one increment per watched family). Both are
    // below the cap of 3, so both families back off.
    let reopened_1 = driver.terminate(WatchTermination::ResourceExhaustedFatal);
    assert_eq!(
        driver.kernel.watch_refused_attempts(),
        2,
        "M7A-31: one call terminates both watched families, advancing the shared counter by 2"
    );
    assert_eq!(
        Driver::arms(&reopened_1, AuthorityTimer::WatchBackoff).len(),
        2,
        "M7A-31: attempts 1 and 2 are both below the cap of 3, so both families arm a back-off"
    );
    assert_eq!(
        Driver::facts(&reopened_1),
        Vec::new(),
        "M7A-31: and neither latches, or the cap would be 1"
    );

    // Let the back-off elapse so both families are watched again and there is something for the
    // next call to terminate.
    let resumed = driver.fire(AuthorityTimer::WatchBackoff);
    driver.reopen(&resumed);

    // Call 2 carries the counter through 3, then 4. The cap is exactly 3: both increments on
    // this call land at or past it, so neither family re-arms.
    let reopened_2 = driver.terminate(WatchTermination::ResourceExhaustedFatal);
    assert_eq!(
        driver.kernel.watch_refused_attempts(),
        4,
        "M7A-31: the counter keeps advancing regardless of the cap"
    );
    assert_eq!(
        Driver::facts(&reopened_2),
        vec![
            AuthorityFact::WatchAdmissionExhausted,
            AuthorityFact::WatchAdmissionExhausted
        ],
        "M7A-31: at the cap A1 latches, and says so. `watch_refused_attempts` is state and the \
         latch is behaviour — a row reading the counter still cannot tell a kernel that gave up \
         from one that crashed, and in a trace that is the whole difference"
    );
    assert_eq!(
        Driver::arms(&reopened_2, AuthorityTimer::WatchBackoff),
        Vec::new(),
        "M7A-31: **the absent re-arm is the claim.** The cap is exactly 3 -- attempts 3 and 4 on \
         this call are both at or past it, so neither family arms a back-off"
    );
    assert_eq!(
        Driver::watches(&reopened_2),
        Vec::new(),
        "M7A-31: and no immediate re-watch either, or the back-off would only have moved"
    );
    assert_eq!(
        Driver::ignores(&reopened_2),
        Vec::new(),
        "M7A-31: at the cap the outcome is a fact and not an ignore reason. The two are one arm \
         apart on purpose: as adjacent unit variants in one enum, this assertion would pass on \
         the under-cap reason"
    );
    assert_eq!(
        driver.take_reloads(),
        Vec::<ControlPrefix>::new(),
        "M7A-33: reaching the cap never provokes a reload"
    );
}

/// M7A-28, second row — the resumed watch starts after the *snapshot* revision, and `M7A-28`
/// alone cannot tell that from the stale cursor.
///
/// Manual-tester finding K7, 2026-09-21. `M7A-28`'s closing assertion compares the resumed
/// `Watch { from }` against `driver.kernel.cursor(prefix)` — the kernel's own state. A mutation
/// that makes [`on_family_snapshot`] keep the pre-gap cursor instead of adopting the snapshot
/// revision moves **both** sides of that equality together, so the row passes while resuming at
/// a revision the snapshot is not coherent at. The tester applied exactly that mutation: `M7A-28`
/// stayed green.
///
/// This is the same defect shape as K4 one row below: an assertion that reads its expected value
/// out of the thing under test. The fix is the same — get the expectation from somewhere the
/// mutation does not reach. Here that is the cursor captured *before* the gap, plus unrelated
/// committed writes that force the snapshot revision strictly past it. Without those writes the
/// two revisions coincide in this fixture and the row is vacuous for a second reason.
#[retcd_test]
fn m7a_28_resumed_watch_uses_the_snapshot_revision_not_the_stale_cursor() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();

    // The expectation, taken before the mutation's reach: where each family's watch stood when
    // the stream was healthy.
    let stale: Vec<(ControlPrefix, Revision)> = [ControlPrefix::Grants, ControlPrefix::Partitions]
        .into_iter()
        .filter_map(|prefix| driver.kernel.cursor(prefix).map(|at| (prefix, at)))
        .collect();
    assert!(
        !stale.is_empty(),
        "the row is only a test if some family had a cursor to go stale"
    );

    // Commit changes the kernel is never shown, so the store's revision runs ahead of every
    // cursor above. This is what makes the two candidate answers different values.
    for i in 0..8_u32 {
        driver.create(ControlKey::Partition(PartitionId(7_000 + i)), b"p");
        driver.discard_completions();
    }

    let reload_effects = driver.terminate(WatchTermination::ResourceExhaustedResumable);
    assert!(
        !driver.take_gapped_families().is_empty(),
        "a gap must actually have been delivered"
    );

    let rewatch = driver.submit_and_complete(&reload_effects);
    let resumed: Vec<_> = rewatch
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Control(ControlEffect::Watch { prefix, from }) => Some((*prefix, *from)),
            _ => None,
        })
        .collect();
    assert!(
        !resumed.is_empty(),
        "the reload must be followed by a resumed watch, or the gap is never closed"
    );

    for (prefix, from) in resumed {
        let Some((_, was)) = stale.iter().copied().find(|(p, _)| *p == prefix) else {
            continue;
        };
        assert!(
            from > was,
            "the resumed watch on {prefix:?} starts after the snapshot revision, not the cursor \
             it held before the gap: resumed from {from:?}, stale cursor was {was:?}. Eight \
             committed writes separate them, so equality here means the snapshot revision was \
             never adopted"
        );
    }
}
