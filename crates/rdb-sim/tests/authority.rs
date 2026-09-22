//! Package A1 rows: the authority kernel's watch and coherent-resync slice.
//!
//! | Row | Claim |
//! |---|---|
//! | M7A-28 | a gap termination produces one `Reload` of the affected family, and the snapshot's revision is what the resumed `Watch` starts after |
//! | M7A-29 | a `Watched` run produces one linearizable `Get` per change and never a `Reload` — a watch invalidates a cache, it does not grant |
//! | M7A-32 | **no coherent family reload occurs unless a termination was delivered** (ADR-rdb-0008 §7 item 4, lead ruling A-R15), with a positive control in the same test |
//! | M7A-33 | `ResourceExhaustedFatal` is a capacity error, not a gap: bounded re-arm, never a reload |
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
use rdb_core::authority::{Authority, AuthorityState, WATCH_ADMISSION_ATTEMPT_CAP};
use rdb_core::contracts::control::{
    ControlEffect, ControlEvent, ControlKey, ControlPrefix, WatchTermination,
};
use rdb_core::contracts::event::{Effect, EffectKind, Event, EventKind, Module};
use rdb_core::contracts::ids::{BootId, CorrelationId, EventId, NodeId, PartitionId, Revision};
use rdb_core::contracts::time::Tick;
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
    fn become_held(&mut self) {
        let grant = support::control_effect(
            1,
            ControlEffect::Cas {
                key: ControlKey::Grant(A),
                expected: None,
                value: Some(Bytes::from_static(b"grant")),
            },
        );
        let adopted = self.submit_and_complete(&[grant]);
        assert_eq!(
            self.kernel.state(),
            AuthorityState::Held,
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

/// M7A-33 — an admission limit is a capacity error, not a gap.
///
/// `ResourceExhaustedFatal` answers `false` to `is_gap`, and reloading in a loop on it turns a
/// capacity error into an outage. The twin that differs by exactly one fact (`KA-6`) is
/// `m7a_28_gap_termination_reloads_then_rewatches`, which sends a termination that *is* a gap.
#[retcd_test]
fn m7a_33_admission_refused_backs_off_and_never_reloads() {
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
    assert!(
        !reopened.is_empty(),
        "below the cap the watch is re-armed, so a back-off is a back-off and not a stop"
    );

    // Past the cap the kernel stops re-arming. Drive it there and prove the re-arm stops
    // without ever having reloaded.
    driver.reopen(&reopened);
    while driver.kernel.watch_refused_attempts() < WATCH_ADMISSION_ATTEMPT_CAP {
        let again = driver.terminate(WatchTermination::ResourceExhaustedFatal);
        if again.is_empty() {
            break;
        }
        driver.reopen(&again);
    }
    assert!(
        driver.kernel.watch_refused_attempts() >= WATCH_ADMISSION_ATTEMPT_CAP,
        "the bounded back-off reaches its cap"
    );
    assert_eq!(
        driver.take_reloads(),
        Vec::<ControlPrefix>::new(),
        "and reaches it without a single reload"
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

/// M7A-33, second row — `WATCH_ADMISSION_ATTEMPT_CAP` is exactly 3, not merely "some cap".
///
/// `m7a_33` drives its own tail loop with `while ... < WATCH_ADMISSION_ATTEMPT_CAP`, so it
/// re-derives the cap from the same constant it is meant to check and cannot notice the constant
/// itself moving to 2 or to 4 (manual-tester finding K4, 2026-09-21). This row hardcodes the expected
/// counts instead of reading them back from the constant, so a changed cap value fails it.
///
/// `become_held` leaves both `Grants` and `Partitions` watched, and `watch_refused_attempts` is
/// one counter shared by every family (see `Driver::terminations`'s doc comment: one `terminate`
/// ends every open watch). So each `terminate` call advances the shared counter by 2, once per
/// family, and the cap is checked separately for each family's own increment within that call.
#[retcd_test]
fn m7a_33_admission_cap_is_exactly_three() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();

    // Call 1 carries the counter through 1, then 2 (one increment per watched family). Both are
    // below the cap of 3, so both families re-arm.
    let reopened_1 = driver.terminate(WatchTermination::ResourceExhaustedFatal);
    assert_eq!(
        driver.kernel.watch_refused_attempts(),
        2,
        "M7A-33: one call terminates both watched families, advancing the shared counter by 2"
    );
    assert_eq!(
        reopened_1.len(),
        2,
        "M7A-33: attempts 1 and 2 are both below the cap of 3, so both families re-arm"
    );
    driver.reopen(&reopened_1);

    // Call 2 carries the counter through 3, then 4. The cap is exactly 3: both increments on
    // this call land at or past it, so neither family re-arms.
    let reopened_2 = driver.terminate(WatchTermination::ResourceExhaustedFatal);
    assert_eq!(
        driver.kernel.watch_refused_attempts(),
        4,
        "M7A-33: the counter keeps advancing regardless of the cap"
    );
    assert!(
        reopened_2.is_empty(),
        "M7A-33: the cap is exactly 3 -- attempts 3 and 4 on this call are both at or past \
         it, so neither family re-arms"
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
