//! Package A1 rows: the authority kernel's watch and coherent-resync slice, and the §6
//! control-fake conformance rows (ADR-rdb-0008).
//!
//! | Row | Claim |
//! |---|---|
//! | M7A-28 | `RevisionCompacted` is a gap: one `Reload` per affected family, and the snapshot's revision is what the resumed `Watch` starts after |
//! | M7A-29 | `ResourceExhaustedResumable` is also a gap, with M7A-28's shape; and a `Watched` run produces one linearizable `Get` per change and never a `Reload` |
//! | M7A-30 | `NotLeader` / `Unavailable` are not gaps: `[Get{Grant(us)}, Timer(WatchBackoff)]`, zero `Reload`, still `Held` |
//! | M7A-31 | `ResourceExhaustedFatal` is a capacity error, not a gap: a **bounded, non-decreasing** back-off, never a reload and never an immediate re-watch; at the cap A1 latches with `Fact(WatchAdmissionExhausted)` and no re-arm at all |
//! | M7A-32 | **no coherent family reload occurs unless a termination was delivered** (ADR-rdb-0008 §7 item 4, lead ruling A-R15), with a positive control in the same test |
//! | M7A-33 | a resynced kernel serves what an uninterrupted one serves (`partitions_revision` per A-R30) |
//! | M7A-34 | a watch event never grants; the `Get` it provokes does |
//! | M7A-35 | no automatic promotion: a frozen then deleted primary grant and 100 ticks produce zero `Cas` |
//! | M7A-59 | an unavailable control read denies `ControlUnavailable` and does not fence |
//! | M7A-119..129 | §6: the fake's CAS, termination, snapshot, delay, drop and staging semantics, and A1's answer to each; M7A-128 is parked on the absent placement/V5 seam |
//!
//! The M7A-31 rows were once named `m7a_33_*`, and the M7A-28 rows once sent M7A-29's input;
//! both ids sat on `scripts/m7-census.sh`'s `MISCREDITED` list. Each came off by its claim
//! becoming true on disk, never to settle a count.
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
use rdb_core::authority::grant::GrantRecord;
use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
use rdb_core::authority::{
    Authority, AuthorityTimer, WATCH_ADMISSION_ATTEMPT_CAP, WATCH_BACKOFF_CAP_MILLIS,
};
use rdb_core::contracts::authority::{
    AuthorityEffect, AuthorityEvent, AuthorityFact, AuthorityIgnoreReason, Checkpoint, DenyReason,
    FenceScope, Lineage, Verdict,
};
use rdb_core::contracts::control::{
    CasOutcome, ControlChange, ControlEffect, ControlEvent, ControlKey, ControlPrefix, ReadOutcome,
    WatchCursor, WatchTermination,
};
use rdb_core::contracts::event::{
    Effect, EffectKind, Event, EventKind, KernelEffect, KernelEvent, Module, StepCtx,
};
use rdb_core::contracts::ids::{
    AuthorityGeneration, BootId, ConfigVersion, ControlRequestId, CorrelationId, EventId,
    Generation, GrantId, NodeId, OperationId, OwnerEpoch, PartitionId, RangeId, Revision,
    TimerVersion,
};
use rdb_core::contracts::ignore::KernelIgnoredReason;
use rdb_core::contracts::time::{Tick, TimerEffect, TimerFired};
use rdb_core::contracts::trace::{CapabilityState, PackageId};
use rdb_sim::harness::environment_capabilities;
use rdb_sim::sim::control::{Completion, ControlOp, ControlStore};

const A: NodeId = NodeId(1);

/// One kernel under one control store, driven the way the dispatcher will drive it.
///
/// Holds no clock and no channel (team kernel-a `KA-1`): `step` is called with one event and the
/// returned vector is kept as a value. `reloads` is the counter `M7A-32` asserts on, and it is
/// incremented from the returned effects, never from the store.
struct Driver {
    kernel: Authority,
    store: ControlStore,
    /// The node this kernel runs on. [`A`] unless a row needs a second node (M7A-127).
    node: NodeId,
    /// The step tick. Zero unless a row advances it to let a window lapse (M7A-124, M7A-125);
    /// the clock sample stays [`support::ctx`]'s frozen one either way, so a moved sample never
    /// adds views to a vector a row compares exactly (lead ruling A-R45).
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
        Self::on(A)
    }

    /// A driver for a kernel on `node`, with its own store.
    fn on(node: NodeId) -> Self {
        Self {
            kernel: Authority::new(),
            store: ControlStore::new(),
            node,
            now: Tick::ZERO,
            next_event: 1,
            reloads: Vec::new(),
            gets: Vec::new(),
            terminations: Vec::new(),
        }
    }

    /// [`support::ctx`] on this driver's node at this driver's tick.
    fn ctx(&self) -> StepCtx<'static> {
        StepCtx {
            now: self.now,
            node: self.node,
            ..support::ctx()
        }
    }

    /// Commit `value` at `key` for real — create, overwrite, or delete when `None` — and return
    /// the revision it landed at. Nothing is shown to the kernel; call only with nothing pending.
    fn put(&mut self, key: ControlKey, value: Option<Bytes>) -> Revision {
        let expected = match self.store.get(key) {
            ReadOutcome::Found { revision, .. } => Some(revision),
            _ => None,
        };
        let effect = support::control_effect(
            1,
            ControlEffect::Cas {
                request: ANY,
                key,
                expected,
                value,
            },
        );
        self.store.submit(self.node, &effect).expect("put");
        let completions = self.store.complete(self.now);
        let [Completion {
            event:
                ControlEvent::CasResult {
                    outcome: CasOutcome::Committed(revision),
                    ..
                },
            ..
        }] = completions.as_slice()
        else {
            panic!("fixture: one committed write and nothing else pending: {completions:?}");
        };
        *revision
    }

    /// Step the kernel with a check of `lineage` at `checkpoint`, and return its verdict.
    fn check(&mut self, checkpoint: Checkpoint, lineage: Lineage) -> (Verdict, Vec<Effect>) {
        let effects = self.step(EventKind::Kernel(KernelEvent::Authority(
            AuthorityEvent::Check {
                checkpoint,
                lineage,
                correlation: CorrelationId(900),
            },
        )));
        let verdict = effects
            .iter()
            .find_map(|effect| match &effect.kind {
                EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Answer(decision))) => {
                    Some(decision.verdict)
                }
                _ => None,
            })
            .expect("a check is always answered");
        (verdict, effects)
    }

    /// Step the kernel with any event kind at this driver's tick.
    fn step(&mut self, kind: EventKind) -> Vec<Effect> {
        let id = self.next_event;
        self.next_event += 1;
        let event = Event {
            id: EventId(id),
            at: self.now,
            node: self.node,
            boot: BootId(1),
            partition: PartitionId(1),
            correlation: CorrelationId(1),
            kind,
        };
        self.kernel
            .step(&self.ctx(), &event)
            .expect("the seam is wired")
    }

    /// Inject a termination and deliver each completion on its own, returning each family's
    /// effect vector separately — the shape a row compares exactly.
    fn terminate_each(
        &mut self,
        termination: WatchTermination,
    ) -> Vec<(ControlPrefix, Vec<Effect>)> {
        self.store
            .inject(ControlOp::TerminateWatch {
                node: self.node,
                termination,
            })
            .expect("the node has at least one watch open to terminate");
        let mut each = Vec::new();
        for completion in self.store.complete(self.now) {
            let ControlEvent::WatchTerminated { prefix, .. } = completion.event else {
                panic!("a termination completes as WatchTerminated: {completion:?}");
            };
            each.push((prefix, self.deliver(completion.event)));
        }
        each.sort_by_key(|(prefix, _)| *prefix);
        each
    }

    /// Emit the pending watch changes and deliver every completion, returning all effects.
    fn emit(&mut self) -> Vec<Effect> {
        self.store
            .inject(ControlOp::EmitWatch { node: self.node })
            .expect("emit");
        let mut produced = Vec::new();
        for completion in self.store.complete(self.now) {
            produced.extend(self.deliver(completion.event));
        }
        produced
    }

    /// Wrap a control completion as the event the dispatcher would build from it.
    fn event_for(&mut self, control: ControlEvent) -> Event {
        let id = self.next_event;
        self.next_event += 1;
        Event {
            id: EventId(id),
            at: self.now,
            node: self.node,
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
            .step(&self.ctx(), &event)
            .expect("the control seam is wired");
        for effect in &effects {
            match &effect.kind {
                EffectKind::Control(ControlEffect::Reload { prefix }) => {
                    self.reloads.push(*prefix);
                }
                EffectKind::Control(ControlEffect::Get { key, .. }) => self.gets.push(*key),
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
                self.store.submit(self.node, effect).expect("submit");
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
                request: ANY,
                key,
                expected: None,
                value: Some(Bytes::from_static(value)),
            },
        );
        self.store.submit(self.node, &effect).expect("create");
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
            node: self.node,
            boot: BootId(1),
            partition: PartitionId(1),
            correlation: CorrelationId(1),
            kind: EventKind::Timer(fired),
        };
        let effects = self
            .kernel
            .step(&self.ctx(), &event)
            .expect("the timer seam is wired");
        for effect in &effects {
            match &effect.kind {
                EffectKind::Control(ControlEffect::Reload { prefix }) => {
                    self.reloads.push(*prefix);
                }
                EffectKind::Control(ControlEffect::Get { key, .. }) => self.gets.push(*key),
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
                node: self.node,
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
                self.store.submit(self.node, effect).expect("reopen watch");
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
/// `m7a_29_watch_gap_lagged_resumable_read_family_and_rewatch`, which sends a termination that
/// *is* a gap.
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

/// Every effect's kind, in order: what a row compares when it says "effects = [..]".
fn kinds(effects: &[Effect]) -> Vec<EffectKind> {
    effects.iter().map(|effect| anon(&effect.kind)).collect()
}

/// The request id these rows write, and the one [`anon`] writes over whatever A1 minted. The rows
/// assert which record is read or written; the id is A1's to choose, and matching an answer by it
/// is `M7A-179`..`M7A-181`'s subject in rdb-core (lead ledger L-R177hs).
const ANY: ControlRequestId = ControlRequestId(0);

/// `kind` with the request id of a control `Cas` or `Get` replaced by [`ANY`].
fn anon(kind: &EffectKind) -> EffectKind {
    let mut kind = kind.clone();
    if let EffectKind::Control(
        ControlEffect::Cas { request, .. } | ControlEffect::Get { request, .. },
    ) = &mut kind
    {
        *request = ANY;
    }
    kind
}

/// A `partitions/{id}` record naming `owner`, lineage (1, 1, 1), serving.
fn record(partition: u32, owner: NodeId) -> PartitionRecord {
    PartitionRecord {
        partition: PartitionId(partition),
        owner,
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
        config_version: ConfigVersion(1),
        lifecycle: PartitionLifecycle::Serving,
    }
}

/// The lineage [`record`] installs for `partition`.
const fn lineage_of(partition: u32) -> Lineage {
    Lineage {
        partition: PartitionId(partition),
        generation: Generation(1),
        owner_epoch: OwnerEpoch(1),
    }
}

/// M7A-28 and M7A-29 share one claim and differ in exactly one fact, the termination kind. Two
/// steps, in this order, each compared as a whole vector:
///
/// 1. At the termination, each terminated family answers `[Control(Reload{that family})]`.
/// 2. At the snapshot, `[.., Control(Watch{prefix, from: r})]` where `r` is the snapshot's own
///    revision — **not** `r + 1` (TD-18). `from` is exclusive, so a `+1` spelling drops a change
///    at exactly `r + 1`. The row commits one there, after the snapshot is taken and before the
///    re-watch opens, and requires the resumed watch to deliver it.
///
/// The partitions snapshot also carries `Fact(LineageLoaded)` ahead of the watch: that is
/// `design.md` §2.4's `FamilyOk{partitions}` row, and the grants snapshot, which installs
/// nothing, is the exact `[Control(Watch{..})]` the plan cell names. The fact — and the
/// `AdoptAuthority` and p8 view ahead of it — are there because the gap hid a real change (p8,
/// ours) that only the family read can recover; A1 must serve it afterwards.
fn gap_resync(row: &str, termination: WatchTermination) {
    assert!(
        termination.is_gap(),
        "{row}: fixture: the termination is a gap"
    );
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();

    // The change the gap hides: committed while the watch is open, never delivered. The family
    // read is the only way the kernel learns of it, so the snapshot must carry it and A1 must
    // serve it — without this, the snapshot equals what A1 already holds, and it answers
    // `LineageUnchanged`, which proves the reload happened but not that it read anything.
    let missed = driver.put(
        ControlKey::Partition(PartitionId(8)),
        Some(record(8, A).encode()),
    );

    // ---- Step 1: the termination. ----
    let each = driver.terminate_each(termination);
    assert_eq!(
        each.iter().map(|(prefix, _)| *prefix).collect::<Vec<_>>(),
        vec![ControlPrefix::Grants, ControlPrefix::Partitions],
        "{row}: fixture: TerminateWatch ends both watched families"
    );
    for (prefix, effects) in &each {
        assert_eq!(
            kinds(effects),
            vec![EffectKind::Control(ControlEffect::Reload {
                prefix: *prefix
            })],
            "{row}: at the termination the whole vector is one Reload of the gapped family — \
             the family read is Reload, never Get{{family}} (TD-12)"
        );
    }

    // ---- Step 2: the snapshot, with a change committed at exactly r + 1 behind it. ----
    let r = driver.store.revision();
    for (_, effects) in &each {
        for effect in effects {
            driver.store.submit(driver.node, effect).expect("reload");
        }
    }
    let late = ControlKey::Partition(PartitionId(9));
    driver
        .store
        .submit(
            A,
            &support::control_effect(
                1,
                ControlEffect::Cas {
                    request: ANY,
                    key: late,
                    expected: None,
                    value: Some(record(9, NodeId(2)).encode()),
                },
            ),
        )
        .expect("the change at r + 1");
    let mut resumed = Vec::new();
    for completion in driver.store.complete(driver.now) {
        match completion.event {
            ControlEvent::FamilySnapshot {
                prefix,
                snapshot_revision,
                ..
            } => {
                assert_eq!(snapshot_revision, r, "{row}: fixture: the snapshot is at r");
                let effects = driver.deliver(completion.event);
                let expected = if prefix == ControlPrefix::Partitions {
                    // The view's horizon and sequence are A1's to compute; its lineage is not.
                    let published = match effects.get(1).map(|effect| &effect.kind) {
                        Some(
                            kind @ EffectKind::Kernel(KernelEffect::Authority(
                                AuthorityEffect::PublishAuthorityView(view),
                            )),
                        ) if view.lineage == lineage_of(8) => kind.clone(),
                        other => panic!("{row}: adopting p8 publishes p8's view: {other:?}"),
                    };
                    let loaded = record(8, A);
                    vec![
                        EffectKind::AdoptAuthority {
                            partition: PartitionId(8),
                            generation: loaded.generation,
                            owner_epoch: loaded.owner_epoch,
                            config_version: loaded.config_version,
                        },
                        published,
                        EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fact(
                            AuthorityFact::LineageLoaded,
                        ))),
                        EffectKind::Control(ControlEffect::Watch { prefix, from: r }),
                    ]
                } else {
                    vec![EffectKind::Control(ControlEffect::Watch {
                        prefix,
                        from: r,
                    })]
                };
                assert_eq!(
                    kinds(&effects),
                    expected,
                    "{row}: at the {prefix:?} snapshot the re-watch is from r = {r:?}, not r + 1"
                );
                resumed.extend(effects);
            }
            ControlEvent::CasResult {
                outcome: CasOutcome::Committed(at),
                ..
            } => assert_eq!(
                at,
                Revision(r.0 + 1),
                "{row}: fixture: the change is at r + 1"
            ),
            other => panic!("{row}: unexpected completion {other:?}"),
        }
    }
    assert_eq!(
        driver.take_reloads(),
        vec![ControlPrefix::Grants, ControlPrefix::Partitions],
        "{row}: one reload per gapped family, and none from the snapshot"
    );
    let view = driver.kernel.view();
    assert_eq!(
        (
            view.served.contains_key(&PartitionId(8)),
            view.partitions_revision
        ),
        (true, Some(r)),
        "{row}: the family read recovered the change the gap hid (p8 at {missed:?}), and the \
         snapshot is the coherent load at r (A-R30)"
    );

    // ---- The resumed watch delivers the change at r + 1. ----
    driver.reopen(&resumed);
    driver
        .store
        .inject(ControlOp::EmitWatch { node: A })
        .expect("emit");
    let mut delivered = Vec::new();
    for completion in driver.store.complete(driver.now) {
        if let ControlEvent::Watched {
            prefix: ControlPrefix::Partitions,
            changes,
            ..
        } = &completion.event
        {
            delivered.extend(changes.iter().map(|change| (change.key, change.revision)));
        }
        let _ = driver.deliver(completion.event);
    }
    assert_eq!(
        delivered,
        vec![(late, Revision(r.0 + 1))],
        "{row}: the change at exactly r + 1 reaches the kernel; a watch resumed from r + 1 \
         would have skipped it silently"
    );
    assert!(
        driver.take_gets().contains(&late),
        "{row}: and the kernel reads it, as for any change"
    );
}

/// M7A-28 — `RevisionCompacted` is a gap: `[Reload{prefix}]`, then the snapshot's re-watch is
/// from `snapshot_revision`, exactly.
///
/// # This function was `m7a_28_gap_termination_reloads_then_rewatches`
///
/// It sent `ResourceExhaustedResumable`, which is M7A-29's input, so M7A-28 was on the census
/// `MISCREDITED` list (kernel-a manual tester, 2026-09-22). It now sends M7A-28's own
/// termination and asserts both steps of the plan row as whole vectors. M7A-29's claim moved to
/// `m7a_29_watch_gap_lagged_resumable_read_family_and_rewatch`, one fact apart.
#[retcd_test]
fn m7a_28_watch_gap_revision_compacted_read_family_and_rewatch() {
    support::preamble();
    gap_resync(
        "M7A-28",
        WatchTermination::RevisionCompacted {
            minimum_available_revision: Revision(1),
        },
    );
}

/// M7A-29 — `ResourceExhaustedResumable` is also a gap, with M7A-28's effect shape. One fact
/// apart from M7A-28 (the termination kind) and from M7A-31 (`ResourceExhaustedFatal`, not a gap).
#[retcd_test]
fn m7a_29_watch_gap_lagged_resumable_read_family_and_rewatch() {
    support::preamble();
    gap_resync("M7A-29", WatchTermination::ResourceExhaustedResumable);
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

    let reload_effects = driver.terminate(WatchTermination::RevisionCompacted {
        minimum_available_revision: Revision(1),
    });
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

// =============================================================================================
// §3 A1 rows that need the control fake: M7A-30, M7A-33, M7A-34, M7A-35, M7A-59.
// =============================================================================================

/// Every `Fence` in `effects`, as `(scope, reason)`.
fn fences(effects: &[Effect]) -> Vec<(FenceScope, DenyReason)> {
    effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fence {
                scope,
                reason,
            })) => Some((*scope, *reason)),
            _ => None,
        })
        .collect()
}

/// Every `Cas` in `effects`, as `(key, expected)`.
fn cases(effects: &[Effect]) -> Vec<(ControlKey, Option<Revision>)> {
    effects
        .iter()
        .filter_map(|effect| match &effect.kind {
            EffectKind::Control(ControlEffect::Cas { key, expected, .. }) => {
                Some((*key, *expected))
            }
            _ => None,
        })
        .collect()
}

/// M7A-30 — `NotLeader` and `Unavailable` are not gaps: read our own record and back off.
///
/// Plan row: effects = `[Control(Get{Grant(us)}), Timer(backoff)]`, zero `Reload`, state `Held`.
/// `design.md` §2.4: `Held | WatchGap{NotLeader|Unavailable} ⇒ Control(Read{key}) + backoff
/// rearm`. Both terminations, in one test, on one kernel; each family's vector is compared whole.
///
/// Positive control: firing the back-off re-watches both families from the cursors they held,
/// so the row proves a timed resume and not a stop.
#[retcd_test]
fn m7a_30_watch_not_leader_or_unavailable_read_and_backoff_no_read_family() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();

    for termination in [WatchTermination::NotLeader, WatchTermination::Unavailable] {
        assert!(
            !termination.is_gap(),
            "fixture: {termination:?} is not a gap"
        );
        let cursors: Vec<(ControlPrefix, Revision)> =
            [ControlPrefix::Grants, ControlPrefix::Partitions]
                .into_iter()
                .map(|prefix| (prefix, driver.kernel.cursor(prefix).expect("watched")))
                .collect();
        let refused = driver.kernel.watch_refused_attempts();

        let each = driver.terminate_each(termination);
        assert_eq!(
            each.iter().map(|(prefix, _)| *prefix).collect::<Vec<_>>(),
            vec![ControlPrefix::Grants, ControlPrefix::Partitions],
            "fixture: TerminateWatch ends both watched families"
        );
        assert_eq!(
            driver.kernel.watch_refused_attempts(),
            refused,
            "M7A-30 {termination:?}: not an admission refusal — the capacity counter is untouched, \
             so leader changes can never latch the watch (A-R78 F3)"
        );
        for (prefix, effects) in &each {
            let [get, arm] = effects.as_slice() else {
                panic!("M7A-30 {termination:?} on {prefix:?}: [Get, Timer], got {effects:?}");
            };
            assert_eq!(
                anon(&get.kind),
                EffectKind::Control(ControlEffect::Get {
                    request: ANY,
                    key: ControlKey::Grant(A)
                }),
                "M7A-30 {termination:?}: a Get of one record, our own grant"
            );
            let EffectKind::Timer(TimerEffect::Arm { id, at, .. }) = &arm.kind else {
                panic!("M7A-30 {termination:?}: the second effect is a timer arm: {arm:?}");
            };
            assert_eq!(
                *id,
                AuthorityTimer::WatchBackoff.id(),
                "M7A-30 {termination:?}: the timer is the watch back-off"
            );
            assert!(
                *at > driver.now,
                "M7A-30 {termination:?}: a back-off, not an immediate retry: {at:?}"
            );
        }
        assert_eq!(
            driver.take_reloads(),
            Vec::<ControlPrefix>::new(),
            "M7A-30 {termination:?}: zero Reload — is_gap is false, so there is nothing to re-read"
        );
        assert!(
            driver.kernel.state().is_held(),
            "M7A-30 {termination:?}: still Held"
        );

        let resumed = driver.fire(AuthorityTimer::WatchBackoff);
        assert_eq!(
            Driver::watches(&resumed),
            cursors,
            "M7A-30 {termination:?}: the back-off resumes both families from their cursors"
        );
        assert_eq!(driver.take_reloads(), Vec::<ControlPrefix>::new());
        driver.reopen(&resumed);
    }
}

/// The ten partitions-family writes M7A-33 drives, as `(key, body)` for event `k`.
///
/// Event 7 moves an already-served partition (12) to another node, so the gap B skips contains
/// a **removal** as well as additions. Without it, a snapshot that merged into `served` instead
/// of replacing it would produce the same state as the uninterrupted run.
fn m7a_33_event(k: u32) -> (ControlKey, Bytes) {
    if k == 7 {
        (
            ControlKey::Partition(PartitionId(12)),
            record(12, NodeId(2)).encode(),
        )
    } else {
        (
            ControlKey::Partition(PartitionId(10 + k)),
            record(10 + k, A).encode(),
        )
    }
}

/// Deliver the pending watch changes and the reads they provoke, one round.
fn consume(driver: &mut Driver) {
    let gets = driver.emit();
    let _ = driver.submit_and_complete(&gets);
}

/// M7A-33 — a resynced kernel ends in the same serving state as one that never lost its watch.
///
/// Plan row: A gets events 1..10 uninterrupted; B gets 1..5, `RevisionCompacted`, a
/// `FamilySnapshot` at the revision of event 8, then the re-watch delivers 9..10. A's and B's
/// `served`, `revoked_epochs` and the `Partitions` cursor are equal after event 10;
/// `partitions_revision` is not (plan cell corrected to A-R30 under A-R78).
///
/// # `partitions_revision` is not equal, and lead ruling A-R30 is why
///
/// A-R30 makes `partitions_revision` the revision of the **last
/// coherent snapshot** and nothing else; single reads do not move it. A never takes a snapshot
/// after its first, and B takes one at event 8 — so the two differ by construction, and a row
/// asserting them equal would be asserting against the ruling. This row asserts each one's
/// value instead. The claim the row exists for — the resync converges on the same **rights** —
/// is carried by `served`, `revoked_epochs`, the partitions cursor, and the verdict for every
/// lineage involved.
///
/// `revoked_epochs` is made non-empty on purpose: both kernels persist the same revocation
/// before the gap, so a snapshot that wiped it would be red. It names a partition outside the
/// family, so the snapshot has no record that could legitimately touch it.
#[retcd_test]
fn m7a_33_watch_resync_state_equals_uninterrupted_watch() {
    support::preamble();
    let revoke = |driver: &mut Driver| {
        let _ = driver.step(EventKind::Kernel(KernelEvent::Authority(
            AuthorityEvent::EpochRevocationPersisted {
                partition: PartitionId(99),
                epoch: OwnerEpoch(1),
            },
        )));
    };

    // ---- A: uninterrupted. ----
    let mut a = Driver::new();
    a.become_held();
    let acquired_at = a.kernel.view().partitions_revision;
    for k in 1..=10 {
        let (key, body) = m7a_33_event(k);
        let _ = a.put(key, Some(body));
        consume(&mut a);
        if k == 2 {
            revoke(&mut a);
        }
    }

    // ---- B: events 1..5 live, 6..8 missed, a gap, a snapshot at 8, then 9..10 live. ----
    let mut b = Driver::new();
    b.become_held();
    for k in 1..=5 {
        let (key, body) = m7a_33_event(k);
        let _ = b.put(key, Some(body));
        consume(&mut b);
        if k == 2 {
            revoke(&mut b);
        }
    }
    for k in 6..=8 {
        let (key, body) = m7a_33_event(k);
        let _ = b.put(key, Some(body));
    }
    let event_8 = b.store.revision();
    let each = b.terminate_each(WatchTermination::RevisionCompacted {
        minimum_available_revision: event_8,
    });
    let reloads: Vec<Effect> = each.into_iter().flat_map(|(_, effects)| effects).collect();
    let rewatch = b.submit_and_complete(&reloads);
    b.reopen(&rewatch);
    for k in 9..=10 {
        let (key, body) = m7a_33_event(k);
        let _ = b.put(key, Some(body));
        consume(&mut b);
    }

    let (va, vb) = (a.kernel.view(), b.kernel.view());
    let expected: Vec<PartitionId> = [11, 13, 14, 15, 16, 18, 19, 20]
        .into_iter()
        .map(PartitionId)
        .collect();
    assert_eq!(
        va.served.keys().copied().collect::<Vec<_>>(),
        expected,
        "M7A-33 fixture: the uninterrupted run serves what the store says (12 moved away at 7)"
    );
    assert_eq!(
        vb.served, va.served,
        "M7A-33: the resynced kernel serves exactly what the uninterrupted one serves"
    );
    assert_eq!(
        vb.revoked_epochs, va.revoked_epochs,
        "M7A-33: the resync neither drops nor invents a durable revocation"
    );
    assert_eq!(
        va.revoked_epochs.iter().copied().collect::<Vec<_>>(),
        vec![(PartitionId(99), OwnerEpoch(1))],
        "M7A-33 fixture: the revocation set is not empty, so its equality means something"
    );
    assert_eq!(
        vb.cursors.get(&ControlPrefix::Partitions),
        va.cursors.get(&ControlPrefix::Partitions),
        "M7A-33: both have consumed the partitions family to the same revision"
    );
    assert_eq!(
        (va.partitions_revision, vb.partitions_revision),
        (acquired_at, Some(event_8)),
        "M7A-33 under A-R30: partitions_revision is each kernel's last coherent snapshot — A's \
         acquisition load, B's resync at event 8 — and not the cursor"
    );
    for partition in (11..=20).chain([99]) {
        let lineage = lineage_of(partition);
        assert_eq!(
            b.kernel.may_admit(&b.ctx(), lineage),
            a.kernel.may_admit(&a.ctx(), lineage),
            "M7A-33: the same verdict for p{partition} on both kernels"
        );
    }
}

/// M7A-34 — a watch event never grants, even when the record it names would.
///
/// Plan row (TD-17): `Unheld`; `Watched{grants, [Grant(us) @ r]}` where the record at `r` names
/// us. Effects = `[Control(Get{Grant(us)})]`; still `Unheld`; `may_admit == Deny(NoGrant)`.
///
/// Twin, one fact apart (the read's answer): submitting that `Get` to the store returns the
/// record, and **that** answer adopts it. So the record really was adoptable, and the `Unheld`
/// after the watch event is the kernel declining to believe a revision, not a record it could
/// not have used.
#[retcd_test]
fn m7a_34_watch_event_never_grants() {
    support::preamble();
    let mut driver = Driver::new();
    // Absorb the frozen sample, so the kernel can say which `E` an adoptable record carries.
    let _ = driver.fire(AuthorityTimer::ClockWake);
    let expiry = driver
        .kernel
        .e_new(driver.now, &support::BUDGETS)
        .expect("fixture: a fresh sample");
    driver
        .store
        .submit(
            A,
            &support::control_effect(
                1,
                ControlEffect::Watch {
                    prefix: ControlPrefix::Grants,
                    from: Revision(0),
                },
            ),
        )
        .expect("watch the grants family");
    let ours = GrantRecord {
        grant: GrantId(1),
        node: A,
        boot: BootId(1),
        authority_generation: AuthorityGeneration::default(),
        expiry_utc_ms: expiry,
        frozen: false,
    };
    let r = driver.put(ControlKey::Grant(A), Some(ours.encode()));

    driver
        .store
        .inject(ControlOp::EmitWatch { node: A })
        .expect("emit");
    let [completion] = driver
        .store
        .complete(driver.now)
        .try_into()
        .expect("one watch");
    let ControlEvent::Watched { changes, .. } = &completion.event else {
        panic!("fixture: a Watched: {completion:?}");
    };
    assert_eq!(
        changes
            .iter()
            .map(|change| (change.key, change.revision))
            .collect::<Vec<_>>(),
        vec![(ControlKey::Grant(A), r)],
        "fixture: the watch carries the revision, never the body"
    );
    let before = driver.kernel.view();
    let effects = driver.deliver(completion.event);
    assert_eq!(
        driver.kernel.view(),
        before,
        "M7A-34: the watch event asks and changes nothing — no cursor, no refusal count, no \
         state (A-R78 F2: an Unheld-era cursor could outlive enter_held)"
    );
    assert_eq!(
        kinds(&effects),
        vec![EffectKind::Control(ControlEffect::Get {
            request: ANY,
            key: ControlKey::Grant(A)
        })],
        "M7A-34: a watch event on our own grant key becomes one linearizable read, and nothing else"
    );
    assert!(
        driver.kernel.state().is_unheld(),
        "M7A-34: still Unheld after the watch event"
    );
    assert_eq!(
        driver.kernel.may_admit(&driver.ctx(), lineage_of(1)),
        Verdict::Deny(DenyReason::NoGrant),
        "M7A-34: and it admits nothing"
    );

    let _ = driver.submit_and_complete(&effects);
    assert!(
        driver.kernel.state().is_held(),
        "M7A-34 twin: the read's answer is what grants — the same record, through a Get, adopts"
    );

    // A batched event is read change by change, in order, and still moves nothing
    // (tester-ka-a1 PROBE-11, mutant n6).
    let mut batched = Driver::new();
    let _ = batched.fire(AuthorityTimer::ClockWake);
    let quiet = batched.kernel.view();
    let batch = batched.deliver(ControlEvent::Watched {
        prefix: ControlPrefix::Grants,
        cursor: WatchCursor {
            revision: Revision(4),
        },
        changes: [2_u32, 3, 4]
            .into_iter()
            .map(|n| ControlChange {
                key: ControlKey::Grant(NodeId(n)),
                revision: Revision(u64::from(n)),
            })
            .collect(),
    });
    assert_eq!(
        kinds(&batch),
        [2_u32, 3, 4]
            .into_iter()
            .map(|n| EffectKind::Control(ControlEffect::Get {
                request: ANY,
                key: ControlKey::Grant(NodeId(n))
            }))
            .collect::<Vec<_>>(),
        "M7A-34: every change in a batch is read, one Get each, in order"
    );
    assert_eq!(
        batched.kernel.view(),
        quiet,
        "M7A-34: and the batch moves nothing"
    );

    // The reads-on-watch arm is `Unheld` only. A `Fenced` kernel given a watch event with a
    // real change answers nothing and moves nothing (tester-ka-a1 A4, mutant m10).
    let mut fenced = Driver::new();
    fenced.become_held();
    fenced.now = Tick(3_000);
    let lapse = fenced.fire(AuthorityTimer::ClockWake);
    assert_eq!(
        fences(&lapse),
        vec![(FenceScope::Node, DenyReason::Expired)],
        "M7A-34 fixture: the second kernel is Fenced"
    );
    let _ = fenced.put(
        ControlKey::Partition(PartitionId(4)),
        Some(record(4, NodeId(2)).encode()),
    );
    let still = fenced.kernel.view();
    assert_eq!(
        fenced.emit(),
        Vec::new(),
        "M7A-34: a Fenced kernel reads nothing on a watch event"
    );
    assert_eq!(fenced.kernel.view(), still, "M7A-34: and changes nothing");
}

/// M7A-35 — no automatic promotion: a secondary that watches the primary's grant be frozen and
/// then deleted does not try to take it.
///
/// Plan row: `Unheld` secondary; grants-family changes `Grant(primary) @ r1` (the record is
/// frozen) then `@ r2` (deleted); 100 ticks; no `AcquireDue`. Zero create-only `Cas`; the `Get`s
/// return `Found{frozen}` then `Absent`, and neither promotes it.
///
/// # Not vacuous (ledger L-R118)
///
/// L-R118 found this row zero in every run, because A1 then emitted no `Cas` on any input. A1
/// now emits a create-only `Cas` on `AcquireDue`, and the positive control at the end fires one
/// on the same kernel: exactly one appears. So the zero above it is a counter that can move.
/// The watch half is not vacuous either: each change must first produce the `Get` of the
/// primary's key, so the kernel demonstrably saw both changes.
///
/// # Deviation, accepted by the lead (L-R177dq)
///
/// The driver has no timer wheel: a timer A1 arms is fired only when the row fires it. So "100
/// ticks" is 100 `ClockWake` firings, and a kernel that merely *armed* `Acquire` in response to
/// the deletion would pass here. The row's own input says "no `AcquireDue`", so an armed but
/// never-fired acquisition is outside it; a Cas on any delivered input is not.
#[retcd_test]
fn m7a_35_no_automatic_promotion() {
    const PRIMARY: NodeId = NodeId(2);
    support::preamble();
    let mut driver = Driver::new();
    let primary = GrantRecord {
        grant: GrantId(7),
        node: PRIMARY,
        boot: BootId(1),
        authority_generation: AuthorityGeneration::default(),
        expiry_utc_ms: 3_000,
        frozen: false,
    };
    let r0 = driver.put(ControlKey::Grant(PRIMARY), Some(primary.encode()));
    driver
        .store
        .submit(
            A,
            &support::control_effect(
                1,
                ControlEffect::Watch {
                    prefix: ControlPrefix::Grants,
                    from: r0,
                },
            ),
        )
        .expect("the secondary watches the grants family");

    let mut seen = Vec::new();
    let mut answers = Vec::new();
    for body in [
        Some(
            GrantRecord {
                frozen: true,
                ..primary
            }
            .encode(),
        ),
        None,
    ] {
        let _ = driver.put(ControlKey::Grant(PRIMARY), body);
        let watched = driver.emit();
        assert_eq!(
            kinds(&watched),
            vec![EffectKind::Control(ControlEffect::Get {
                request: ANY,
                key: ControlKey::Grant(PRIMARY)
            })],
            "M7A-35: each change on the primary's grant becomes one read of it"
        );
        for effect in &watched {
            driver.store.submit(A, effect).expect("read");
        }
        for completion in driver.store.complete(driver.now) {
            if let ControlEvent::Value { outcome, .. } = &completion.event {
                answers.push(outcome.clone());
            }
            seen.extend(driver.deliver(completion.event));
        }
        seen.extend(watched);
    }
    let [ReadOutcome::Found { value, .. }, ReadOutcome::Absent { .. }] = answers.as_slice() else {
        panic!("M7A-35 fixture: Found then Absent, got {answers:?}");
    };
    assert!(
        GrantRecord::decode(value).is_some_and(|record| record.frozen),
        "M7A-35 fixture: the first read finds the primary's record frozen"
    );

    for tick in 1..=100_u64 {
        driver.now = Tick(tick * 5);
        seen.extend(driver.fire(AuthorityTimer::ClockWake));
    }
    assert_eq!(
        cases(&seen),
        Vec::new(),
        "M7A-35: a frozen, then deleted, primary grant and 100 ticks produce zero Cas — no \
         automatic promotion"
    );
    assert!(driver.kernel.state().is_unheld(), "M7A-35: still Unheld");

    let acquire = driver.fire(AuthorityTimer::Acquire);
    assert_eq!(
        cases(&acquire),
        vec![(ControlKey::Grant(A), None)],
        "M7A-35 positive control: the same kernel's own AcquireDue issues exactly one \
         create-only Cas, so the zero above is a counter that moves"
    );
}

/// M7A-59 — control unavailable denies `ControlUnavailable`, and does not fence.
///
/// Plan row: `Held`; `PlanReadUnavailable` on the pending `Get`; `Check{StorageDispatch}` answers
/// `Deny(ControlUnavailable)`; no `Fence`. Twin in the same test: after `Value{Found{ours}}` the
/// next `Check` answers `Admit`.
///
/// Lead ruling A-R44 found this row unwritable: an unavailable read of our own grant answered
/// `Ignored(AdmissionSuspended)` and changed nothing, so the next check admitted. `design.md`
/// §2.4 property 6 — a control Unavailable denies without ending the grant — is what the row
/// asserts, and A1 now remembers the unavailable read until a read of our record answers. Lead
/// ruling A-R77a accepts that and supersedes A-R44. The deny is check-time only: A1 republishes
/// no view on the unavailable read (A-R77a; ADR-rdb-0008 "Control-quorum loss denies" — existing
/// service ends at conservative local expiry), and the row asserts the delivery's whole vector.
///
/// Two paths clear the deny: a read that finds our record (the plan's twin), and a matched
/// committed renewal (A-R78 F1) — both are linearizable answers from control on our own key.
/// Nothing else does: sending a renewal, a renewal CAS that answers `Unavailable`, and a
/// completion for no CAS in flight all leave it standing (A-R78 R1).
///
/// Known window, accepted (A-R77a; tester-ka-a1 A2 / PROBE-8): a watch `Unavailable`
/// termination does not set the deny by itself. Only the `Get` it provokes does, when that read
/// answers `Unavailable`, so checks can admit for one read round trip after the termination.
/// The grant's local expiry still bounds that window.
///
/// The pending `Get` is a real one: a committed renewal's watch echo on `grants/{us}`. The twin's
/// `Found` is the same `Get` answered again, which is what a retry of it returns.
#[retcd_test]
fn m7a_59_control_unavailable_denies_control_unavailable_no_fence() {
    support::preamble();
    let mut driver = Driver::new();
    driver
        .store
        .seed(ControlKey::Partition(PartitionId(1)), record(1, A).encode())
        .expect("seed p1");
    driver.become_held();
    driver.now = Tick(10);
    assert_eq!(
        driver.check(Checkpoint::StorageDispatch, lineage_of(1)).0,
        Verdict::Admit,
        "M7A-59 fixture: p1 is served and admits before the read fails"
    );

    let renew = driver.fire(AuthorityTimer::Renew);
    let _ = driver.submit_and_complete(&renew);
    let echo = driver.emit();
    let gets: Vec<Effect> = echo
        .into_iter()
        .filter(|effect| {
            anon(&effect.kind)
                == EffectKind::Control(ControlEffect::Get {
                    request: ANY,
                    key: ControlKey::Grant(A),
                })
        })
        .collect();
    assert_eq!(
        gets.len(),
        1,
        "M7A-59 fixture: one pending Get of our own grant"
    );

    driver
        .store
        .inject(ControlOp::PlanReadUnavailable)
        .expect("plan");
    let unavailable = driver.submit_and_complete(&gets);
    assert_eq!(
        (unavailable.len(), Driver::ignores(&unavailable)),
        (1, vec![AuthorityIgnoreReason::AdmissionSuspended]),
        "M7A-59: the unavailable read is answered by one ignore and nothing else — no view \
         (A-R77a): {unavailable:?}"
    );
    let (verdict, answered) = driver.check(Checkpoint::StorageDispatch, lineage_of(1));
    assert_eq!(
        verdict,
        Verdict::Deny(DenyReason::ControlUnavailable),
        "M7A-59: an unavailable control read denies, with its own reason"
    );
    assert_eq!(
        fences(&unavailable)
            .into_iter()
            .chain(fences(&answered))
            .collect::<Vec<_>>(),
        Vec::new(),
        "M7A-59: and does not fence — the grant has not ended, we cannot see it"
    );
    assert!(driver.kernel.state().is_held(), "M7A-59: still Held");

    let found = driver.submit_and_complete(&gets);
    assert_eq!(
        fences(&found),
        Vec::new(),
        "M7A-59 twin fixture: the retry reads our own record"
    );
    assert_eq!(
        driver.check(Checkpoint::StorageDispatch, lineage_of(1)).0,
        Verdict::Admit,
        "M7A-59 twin: once a read of our record answers, the next check admits again"
    );

    // A second clear path (A-R78 F1): a matched committed renewal is a linearizable CAS on our
    // own key, so control answers again. Without it a latched watch leaves the deny standing
    // across renewal after renewal (tester-ka-a1 PROBE-6/6b).
    driver
        .store
        .inject(ControlOp::PlanReadUnavailable)
        .expect("plan");
    let _ = driver.submit_and_complete(&gets);
    assert_eq!(
        driver.check(Checkpoint::StorageDispatch, lineage_of(1)).0,
        Verdict::Deny(DenyReason::ControlUnavailable),
        "M7A-59 fixture: denied again"
    );

    let renew = driver.fire(AuthorityTimer::Renew);
    let committed = driver.submit_and_complete(&renew);
    assert_eq!(
        Driver::arms(&committed, AuthorityTimer::Renew).len(),
        1,
        "M7A-59 fixture: the renewal committed and re-armed: {committed:?}"
    );
    assert_eq!(
        driver.check(Checkpoint::StorageDispatch, lineage_of(1)).0,
        Verdict::Admit,
        "M7A-59: a committed renewal of our own grant proves control answers, and the next \
         check admits (A-R78 F1)"
    );

    // Re-latch the deny, then send the neighbours of a matched commit.
    driver
        .store
        .inject(ControlOp::PlanReadUnavailable)
        .expect("plan");
    let _ = driver.submit_and_complete(&gets);
    assert_eq!(
        driver.check(Checkpoint::StorageDispatch, lineage_of(1)).0,
        Verdict::Deny(DenyReason::ControlUnavailable),
        "M7A-59 fixture: denied once more"
    );
    // Only a matched commit clears it (A-R78 R1; tester-ka-a1 PROBE-9/10, mutants n1, n2, n3).
    // Under quorum loss a renewal is still sent and its CAS answers `Unavailable`; neither the
    // send nor that answer is a quorum answer, and neither is a completion for no CAS in flight.
    let unanswered = driver.fire(AuthorityTimer::Renew);
    assert_eq!(
        driver.check(Checkpoint::StorageDispatch, lineage_of(1)).0,
        Verdict::Deny(DenyReason::ControlUnavailable),
        "M7A-59: sending a renewal is not a quorum answer; the deny stands"
    );
    driver
        .store
        .inject(ControlOp::PlanCas {
            node: A,
            outcome: CasOutcome::Unavailable,
        })
        .expect("plan");
    let _ = driver.submit_and_complete(&unanswered);
    assert_eq!(
        driver.check(Checkpoint::StorageDispatch, lineage_of(1)).0,
        Verdict::Deny(DenyReason::ControlUnavailable),
        "M7A-59: a renewal whose CAS answers Unavailable keeps the deny"
    );
    let stray = driver.deliver(ControlEvent::CasResult {
        request: ANY,
        key: ControlKey::Grant(A),
        outcome: CasOutcome::Committed(Revision(99)),
    });
    assert_eq!(
        driver.check(Checkpoint::StorageDispatch, lineage_of(1)).0,
        Verdict::Deny(DenyReason::ControlUnavailable),
        "M7A-59: a Committed completion for no CAS in flight proves nothing: {stray:?}"
    );
}

// =============================================================================================
// §6 rows: ADR-rdb-0008 control-fake conformance, M7A-119..M7A-129.
// =============================================================================================

/// Fire `Renew` on a held driver under `outcome`, and return what the store delivered and A1's
/// answer to it.
fn renewal_under(outcome: CasOutcome) -> (CasOutcome, Vec<Effect>) {
    let mut driver = Driver::new();
    driver.become_held();
    let renew = driver.fire(AuthorityTimer::Renew);
    driver
        .store
        .inject(ControlOp::PlanCas { node: A, outcome })
        .expect("plan");
    for effect in &renew {
        if matches!(effect.kind, EffectKind::Control(_)) {
            driver.store.submit(A, effect).expect("renewal cas");
        }
    }
    let [completion] = driver
        .store
        .complete(driver.now)
        .try_into()
        .expect("one result");
    let ControlEvent::CasResult { outcome, .. } = completion.event else {
        panic!("a CAS completes as CasResult: {completion:?}");
    };
    (outcome, driver.deliver(completion.event))
}

/// M7A-119 — a CAS conflict carries no value; A1 reads.
///
/// Plan row: `PlanCas{Conflict}`; `CasOutcome::Conflict{..}` has no value field for the kernel to
/// read; A1 issues `Get` (M7A-02). The "no value field" half is a compile-time claim, so it is
/// written as one: the pattern below names every field of `Conflict` and has no `..`, so a value
/// field added to the variant stops this file compiling.
///
/// Twin, one fact apart (the plan): the same acquisition unplanned commits and adopts.
#[retcd_test]
fn m7a_119_fake_cas_conflict_carries_no_value() {
    support::preamble();
    let mut driver = Driver::new();
    driver
        .store
        .inject(ControlOp::PlanCas {
            node: A,
            outcome: CasOutcome::Conflict {
                exists: true,
                current: Revision(7),
            },
        })
        .expect("plan");
    let cas = driver.fire(AuthorityTimer::Acquire);
    for effect in &cas {
        if matches!(effect.kind, EffectKind::Control(_)) {
            driver.store.submit(A, effect).expect("acquire cas");
        }
    }
    let [completion] = driver
        .store
        .complete(driver.now)
        .try_into()
        .expect("one result");
    let ControlEvent::CasResult {
        key,
        outcome: CasOutcome::Conflict { exists, current },
        ..
    } = completion.event
    else {
        panic!("M7A-119: the planned conflict is delivered as one: {completion:?}");
    };
    assert_eq!(
        (key, exists, current),
        (ControlKey::Grant(A), true, Revision(7)),
        "M7A-119: the conflict says that a record exists and at which revision, and nothing else"
    );
    let effects = driver.deliver(completion.event);
    assert_eq!(
        kinds(&effects),
        vec![
            EffectKind::Control(ControlEffect::Get {
                request: ANY,
                key: ControlKey::Grant(A)
            }),
            EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fact(
                AuthorityFact::AcquireLost
            ))),
        ],
        "M7A-119: learning nothing from the conflict, A1 reads the record (M7A-02)"
    );
    assert!(
        driver.kernel.state().is_unheld(),
        "M7A-119: and holds nothing"
    );

    let mut twin = Driver::new();
    let cas = twin.fire(AuthorityTimer::Acquire);
    let _ = twin.submit_and_complete(&cas);
    assert!(
        twin.kernel.state().is_held(),
        "M7A-119 twin: the same acquisition without the planned conflict commits and adopts"
    );
}

/// M7A-120 — `Unknown`, `Unavailable` and `Conflict` are three outcomes, and A1 answers each
/// differently.
///
/// Plan row: three distinct `CasOutcome` variants delivered; A1's three responses differ
/// (M7A-03, M7A-15, M7A-13). All three are driven in the **same** state — a renewal — so the
/// outcome is the only fact that differs, and each response is compared whole. (Ledger L-R118
/// found this row vacuous when all three took one empty arm.)
///
/// Measured, and not claimed by this row: at **acquisition** `Unknown` and `Unavailable` both
/// answer `[Get]`, the same vector.
#[retcd_test]
fn m7a_120_fake_unknown_distinct_from_unavailable_and_conflict() {
    support::preamble();
    let conflict = CasOutcome::Conflict {
        exists: true,
        current: Revision(9),
    };
    let runs: Vec<(CasOutcome, Vec<Effect>)> =
        [CasOutcome::Unknown, CasOutcome::Unavailable, conflict]
            .into_iter()
            .map(renewal_under)
            .collect();
    assert_eq!(
        runs.iter().map(|(outcome, _)| *outcome).collect::<Vec<_>>(),
        vec![CasOutcome::Unknown, CasOutcome::Unavailable, conflict],
        "M7A-120: each planned outcome is delivered as itself — three distinct variants"
    );

    let read = EffectKind::Control(ControlEffect::Get {
        request: ANY,
        key: ControlKey::Grant(A),
    });
    let fact = |fact| EffectKind::Kernel(KernelEffect::Authority(AuthorityEffect::Fact(fact)));
    assert_eq!(
        kinds(&runs[0].1),
        vec![read.clone(), fact(AuthorityFact::RenewUnknown)],
        "M7A-120: Unknown reads and says the outcome is unknown"
    );
    let [arm, get] = runs[1].1.as_slice() else {
        panic!("M7A-120: Unavailable answers [Timer, Get]: {:?}", runs[1].1);
    };
    assert!(
        matches!(&arm.kind, EffectKind::Timer(TimerEffect::Arm { id, .. })
            if *id == AuthorityTimer::Renew.id()),
        "M7A-120: Unavailable retries on the renewal timer: {arm:?}"
    );
    assert_eq!(anon(&get.kind), read, "M7A-120: and reads");
    assert_eq!(
        kinds(&runs[2].1),
        vec![read, fact(AuthorityFact::RenewLost)],
        "M7A-120: Conflict reads and says the renewal was lost"
    );
    for i in 0..3 {
        for j in (i + 1)..3 {
            assert_ne!(
                kinds(&runs[i].1),
                kinds(&runs[j].1),
                "M7A-120: responses {i} and {j} must differ"
            );
        }
    }
}

/// Whether `t` is a gap, by an exhaustive match: a sixth variant fails compilation here.
const fn gap_by_match(t: WatchTermination) -> bool {
    match t {
        WatchTermination::RevisionCompacted { .. }
        | WatchTermination::ResourceExhaustedResumable => true,
        WatchTermination::ResourceExhaustedFatal
        | WatchTermination::NotLeader
        | WatchTermination::Unavailable => false,
    }
}

/// M7A-121 — the fake delivers each of the five terminations as scripted, and progress as a
/// watermark.
///
/// Plan row: `TerminateWatch{node, t}` once per `t`, then `EmitProgress{node}`. Each delivered as
/// `WatchTerminated{prefix, from, termination: t}`; progress as `WatchProgress{prefix,
/// revision}`; the match over `WatchTermination` is exhaustive (KA-7) and `is_gap()` is true for
/// exactly the first two. `t` is an index, not a key (TD-20): the op ends every watch of `node`.
#[retcd_test]
fn m7a_121_fake_five_watch_terminations_and_progress() {
    support::preamble();
    let five = [
        WatchTermination::RevisionCompacted {
            minimum_available_revision: Revision(3),
        },
        WatchTermination::ResourceExhaustedResumable,
        WatchTermination::ResourceExhaustedFatal,
        WatchTermination::NotLeader,
        WatchTermination::Unavailable,
    ];
    assert_eq!(
        five.map(WatchTermination::is_gap),
        [true, true, false, false, false],
        "M7A-121: is_gap is true for exactly the first two"
    );
    assert_eq!(
        five.map(gap_by_match),
        five.map(WatchTermination::is_gap),
        "M7A-121: and agrees with an exhaustive match"
    );

    for t in five {
        let mut driver = Driver::new();
        driver.become_held();
        let expected_from: Vec<(ControlPrefix, Revision)> =
            [ControlPrefix::Grants, ControlPrefix::Partitions]
                .into_iter()
                .map(|prefix| (prefix, driver.kernel.cursor(prefix).expect("watched")))
                .collect();
        driver
            .store
            .inject(ControlOp::TerminateWatch {
                node: A,
                termination: t,
            })
            .expect("terminate");
        let mut delivered: Vec<(ControlPrefix, Revision)> = Vec::new();
        for completion in driver.store.complete(driver.now) {
            let ControlEvent::WatchTerminated {
                prefix,
                from,
                termination,
            } = completion.event
            else {
                panic!("M7A-121 {t:?}: a WatchTerminated: {completion:?}");
            };
            assert_eq!(
                termination, t,
                "M7A-121: delivered as the termination scripted"
            );
            delivered.push((prefix, from));
        }
        delivered.sort_unstable();
        assert_eq!(
            delivered, expected_from,
            "M7A-121 {t:?}: one per watched family, each from where that watch stood"
        );
        assert_eq!(
            driver.store.open_watches(A),
            0,
            "M7A-121 {t:?}: every watch ended"
        );
    }

    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.put(
        ControlKey::Partition(PartitionId(4)),
        Some(record(4, NodeId(2)).encode()),
    );
    let revision = driver.store.revision();
    driver
        .store
        .inject(ControlOp::EmitProgress { node: A })
        .expect("progress");
    let mut progress: Vec<(ControlPrefix, Revision)> = driver
        .store
        .complete(driver.now)
        .into_iter()
        .map(|completion| match completion.event {
            ControlEvent::WatchProgress { prefix, revision } => (prefix, revision),
            other => panic!("M7A-121: progress is a WatchProgress: {other:?}"),
        })
        .collect();
    progress.sort_unstable();
    assert_eq!(
        progress,
        vec![
            (ControlPrefix::Grants, revision),
            (ControlPrefix::Partitions, revision)
        ],
        "M7A-121: progress carries the store's revision and nothing else, per watched family"
    );
}

/// M7A-122 — `PlanReadUnavailable` makes the next `Get` answer `Unavailable`.
///
/// Plan row: `PlanReadUnavailable` then `Get` ⇒ `Value{ReadOutcome::Unavailable}`. Twin, one
/// fact apart (the plan): the next `Get` of the same key answers from the store.
#[retcd_test]
fn m7a_122_fake_plan_read_unavailable() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let get = support::control_effect(
        1,
        ControlEffect::Get {
            request: ANY,
            key: ControlKey::Grant(A),
        },
    );
    driver
        .store
        .inject(ControlOp::PlanReadUnavailable)
        .expect("plan");
    let mut outcomes = Vec::new();
    for _ in 0..2 {
        driver.store.submit(A, &get).expect("get");
        for completion in driver.store.complete(driver.now) {
            let ControlEvent::Value { key, outcome, .. } = completion.event else {
                panic!("M7A-122: a Get completes as Value: {completion:?}");
            };
            assert_eq!(key, ControlKey::Grant(A));
            outcomes.push(outcome);
        }
    }
    assert_eq!(outcomes.len(), 2, "M7A-122 fixture: two reads, two answers");
    assert_eq!(
        outcomes[0],
        ReadOutcome::Unavailable,
        "M7A-122: the planned read is unavailable, although the record exists"
    );
    assert!(
        matches!(outcomes[1], ReadOutcome::Found { .. }),
        "M7A-122 twin: the plan is consumed; the next read answers from the store: {:?}",
        outcomes[1]
    );
}

/// M7A-123 — a family reload completes as `FamilySnapshot{prefix, snapshot_revision, records}`,
/// and A1 resumes from `snapshot_revision` itself.
///
/// Plan row (TD-12, TD-18): `Control(Reload{prefix})` ⇒ `FamilySnapshot`; the re-watch resumes at
/// `from: snapshot_revision`, not `+ 1`.
#[retcd_test]
fn m7a_123_fake_family_snapshot_carries_snapshot_revision() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    for partition in [4, 5] {
        let _ = driver.put(
            ControlKey::Partition(PartitionId(partition)),
            Some(record(partition, NodeId(2)).encode()),
        );
    }
    let at = driver.store.revision();
    driver
        .store
        .submit(
            A,
            &support::control_effect(
                1,
                ControlEffect::Reload {
                    prefix: ControlPrefix::Partitions,
                },
            ),
        )
        .expect("reload");
    let [completion] = driver
        .store
        .complete(driver.now)
        .try_into()
        .expect("one snapshot");
    let ControlEvent::FamilySnapshot {
        prefix,
        snapshot_revision,
        records,
    } = &completion.event
    else {
        panic!("M7A-123: a Reload completes as FamilySnapshot: {completion:?}");
    };
    assert_eq!(
        (*prefix, *snapshot_revision),
        (ControlPrefix::Partitions, at),
        "M7A-123: the snapshot names its family and the revision it is coherent at"
    );
    assert_eq!(
        records.iter().map(|record| record.key).collect::<Vec<_>>(),
        vec![
            ControlKey::Partition(PartitionId(4)),
            ControlKey::Partition(PartitionId(5))
        ],
        "M7A-123: every record of the family and none of another (grants/{{us}} exists too)"
    );
    let effects = driver.deliver(completion.event);
    assert_eq!(
        Driver::watches(&effects),
        vec![(ControlPrefix::Partitions, at)],
        "M7A-123: A1 resumes from snapshot_revision itself — from is exclusive"
    );
}

/// M7A-124 — an arbitrarily late completion lands after the grant expired, and is ignored.
///
/// Plan row: `PlanCas{Committed}`, then `DelayCompletion{node, by_millis: 5_000}`. The
/// `CasResult` arrives after A1 fenced `Expired`; A1 answers `LateRenewalIgnored` (M7A-22), and
/// the fenced state is untouched by it.
#[retcd_test]
fn m7a_124_fake_arbitrarily_late_completion_after_expiry() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();

    let renew = driver.fire(AuthorityTimer::Renew);
    let lands_at = Revision(driver.store.revision().0 + 1);
    driver
        .store
        .inject(ControlOp::PlanCas {
            node: A,
            outcome: CasOutcome::Committed(lands_at),
        })
        .expect("plan");
    driver
        .store
        .inject(ControlOp::DelayCompletion {
            node: A,
            by_millis: 5_000,
        })
        .expect("delay");
    for effect in &renew {
        if matches!(effect.kind, EffectKind::Control(_)) {
            driver.store.submit(A, effect).expect("renewal cas");
        }
    }
    let [late] = driver
        .store
        .complete(driver.now)
        .try_into()
        .expect("one result");
    assert_eq!(
        late.at,
        Tick(5_000),
        "M7A-124: the completion is held back 5 000 ms of logical time"
    );

    driver.now = Tick(3_000);
    let lapse = driver.fire(AuthorityTimer::ClockWake);
    assert_eq!(
        fences(&lapse),
        vec![(FenceScope::Node, DenyReason::Expired)],
        "M7A-124 fixture: the grant expires while the completion is still in flight"
    );
    let fenced = driver.kernel.view();

    driver.now = late.at;
    let ControlEvent::CasResult { outcome, .. } = late.event else {
        panic!("M7A-124: a CasResult: {late:?}");
    };
    assert_eq!(
        outcome,
        CasOutcome::Committed(lands_at),
        "M7A-124: it did commit"
    );
    let effects = driver.deliver(late.event);
    assert_eq!(
        Driver::ignores(&effects),
        vec![AuthorityIgnoreReason::LateRenewalIgnored],
        "M7A-124: a commit that lands after the fence is ignored (M7A-22)"
    );
    assert_eq!(effects.len(), 1, "M7A-124: and nothing else: {effects:?}");
    assert_eq!(
        driver.kernel.view(),
        fenced,
        "M7A-124: the late commit writes nothing — no E, no renewal, still Fenced{{Expired}}"
    );
}

/// M7A-125 — a dropped completion never arrives, and the grant's own window ends it.
///
/// Plan row: `PlanCas{Committed}`, then `DropCompletion{node}`. No `CasResult` ever; A1's window
/// lapses ⇒ `Fence{Expired}`; never `Committed`.
#[retcd_test]
fn m7a_125_fake_dropped_operation_never_completes() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let expiry = driver.kernel.view().expiry_utc_ms;

    let renew = driver.fire(AuthorityTimer::Renew);
    let lands_at = Revision(driver.store.revision().0 + 1);
    driver
        .store
        .inject(ControlOp::PlanCas {
            node: A,
            outcome: CasOutcome::Committed(lands_at),
        })
        .expect("plan");
    driver
        .store
        .inject(ControlOp::DropCompletion { node: A })
        .expect("drop");
    for effect in &renew {
        if matches!(effect.kind, EffectKind::Control(_)) {
            driver.store.submit(A, effect).expect("renewal cas");
        }
    }

    let mut fenced_at = None;
    for step in 1..=30_u64 {
        driver.now = Tick(step * 100);
        assert_eq!(
            driver.store.complete(driver.now),
            Vec::new(),
            "M7A-125: the dropped completion never arrives, at any tick"
        );
        let effects = driver.fire(AuthorityTimer::ClockWake);
        if fenced_at.is_none() && !fences(&effects).is_empty() {
            assert_eq!(
                fences(&effects),
                vec![(FenceScope::Node, DenyReason::Expired)],
                "M7A-125: the window lapses and the grant is fenced Expired"
            );
            fenced_at = Some(driver.now);
        }
        if fenced_at.is_none() {
            assert_eq!(
                driver.kernel.view().expiry_utc_ms,
                expiry,
                "M7A-125: never Committed — E is never advanced by a write A1 was not told of"
            );
        }
    }
    let fenced_at = fenced_at.expect("M7A-125: the window lapsed within 3 000 ms");
    assert!(
        fenced_at > Tick(1_000) && fenced_at <= Tick(2_900),
        "M7A-125: fenced by the grant's own window (renewed at 0, grant 3 000, margin 100), not \
         by the drop itself: {fenced_at:?}"
    );
    assert_eq!(
        driver.kernel.state().fence_reason(),
        Some(DenyReason::Expired),
        "M7A-125: and stays Fenced{{Expired}}"
    );
}

/// M7A-126 — staged data is invisible to a family read until the cutover commits.
///
/// Plan row: an `Operation` record staging p3; `Reload{partitions}`; `FamilySnapshot.records`
/// excludes p3 until the route cutover commits. The cutover here is two single-key commits,
/// `Route` then `Partition(3)` — the fake has no multi-key CAS, and spec §7.3 step 4's one CAS
/// installs both. A1's `served` is asserted beside the records, so the row is about what the
/// kernel serves and not only what the fake returns.
#[retcd_test]
fn m7a_126_staged_data_invisible_to_family_read() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let staged = record(3, A).encode();
    let _ = driver.put(ControlKey::Operation(OperationId(1)), Some(staged.clone()));

    let reload = |driver: &mut Driver| {
        driver
            .store
            .submit(
                A,
                &support::control_effect(
                    1,
                    ControlEffect::Reload {
                        prefix: ControlPrefix::Partitions,
                    },
                ),
            )
            .expect("reload");
        let [completion] = driver
            .store
            .complete(driver.now)
            .try_into()
            .expect("one snapshot");
        let ControlEvent::FamilySnapshot { records, .. } = &completion.event else {
            panic!("a FamilySnapshot: {completion:?}");
        };
        let keys: Vec<ControlKey> = records.iter().map(|record| record.key).collect();
        let _ = driver.deliver(completion.event);
        keys
    };

    assert_eq!(
        reload(&mut driver),
        Vec::new(),
        "M7A-126: the staged operation is not a partition record, so the family read omits it"
    );
    assert!(
        !driver.kernel.view().served.contains_key(&PartitionId(3)),
        "M7A-126: and A1 does not serve p3"
    );

    let _ = driver.put(
        ControlKey::Route(RangeId(1)),
        Some(Bytes::from_static(b"p3")),
    );
    let _ = driver.put(ControlKey::Partition(PartitionId(3)), Some(staged));
    assert_eq!(
        reload(&mut driver),
        vec![ControlKey::Partition(PartitionId(3))],
        "M7A-126 twin: once the cutover commits p3's record, the family read carries it"
    );
    assert!(
        driver.kernel.view().served.contains_key(&PartitionId(3)),
        "M7A-126 twin: and A1 serves p3"
    );
}

/// One route-cutover race on a fresh store: `first` and then `second` CAS the same `Route` key
/// at the same expected revision, with `second`'s report forced to `Conflict`. Returns each
/// node's delivered outcome and the body the store holds afterwards.
fn cutover_race(first: NodeId, second: NodeId) -> (Vec<(NodeId, CasOutcome)>, Bytes) {
    let route = ControlKey::Route(RangeId(1));
    let mut store = ControlStore::new();
    let r0 = store
        .seed(route, Bytes::from_static(b"route@r0"))
        .expect("seed");
    store
        .inject(ControlOp::PlanCas {
            node: second,
            outcome: CasOutcome::Conflict {
                exists: true,
                current: Revision(r0.0 + 1),
            },
        })
        .expect("plan");
    for node in [first, second] {
        let effect = support::control_effect(
            u64::from(node.0),
            ControlEffect::Cas {
                request: ANY,
                key: route,
                expected: Some(r0),
                value: Some(record(5, node).encode()),
            },
        );
        store.submit(node, &effect).expect("cutover cas");
    }
    let outcomes = store
        .complete(Tick::ZERO)
        .into_iter()
        .map(|completion| match completion.event {
            ControlEvent::CasResult { outcome, .. } => (completion.node, outcome),
            other => panic!("a CasResult: {other:?}"),
        })
        .collect();
    let ReadOutcome::Found { value, .. } = store.get(route) else {
        panic!("the route exists");
    };
    (outcomes, value)
}

/// M7A-127 — a route cutover has one winner, and A1 on each node sees one owner.
///
/// Plan row: `n1` and `n2` CAS the same `Route` key at the same expected revision;
/// `PlanCas{node: n2, Conflict}` forces `n2`'s report while `n1`'s proceeds. Exactly one
/// `Committed`, to `n1`; `n2` gets `Conflict`; A1's lineage view shows one owner. Twin: the
/// forced node swaps, and so do winner and loser.
///
/// # The twin swaps the submission order too, and the fake is why
///
/// `ControlOp::PlanCas` replaces the **report** and still applies the write (`sim/control.rs`).
/// Forcing `n1` while `n1` submits first would land `n1`'s write and report it as a conflict,
/// and `n2` would then conflict for real: zero winners. So the forced node must be the second
/// submitter for "exactly one Committed" to hold, and the twin is two facts apart, not one.
/// Recorded as a plan deviation, not papered over.
///
/// # "A1's lineage view shows one owner"
///
/// The cutover body names the new owner as a `partitions/{5}` record. Each node's A1 then loads
/// the committed body as its family and serves p5 only if the body names it. Spec §7.3 step 4
/// installs route and owner in one CAS; the fake's CAS is single-key, so the owner half is
/// seeded from the route's committed body.
#[retcd_test]
fn m7a_127_route_cutover_one_winner() {
    const N1: NodeId = NodeId(1);
    const N2: NodeId = NodeId(2);
    support::preamble();

    for (first, second) in [(N1, N2), (N2, N1)] {
        let (outcomes, body) = cutover_race(first, second);
        let committed: Vec<NodeId> = outcomes
            .iter()
            .filter(|(_, outcome)| matches!(outcome, CasOutcome::Committed(_)))
            .map(|(node, _)| *node)
            .collect();
        assert_eq!(
            committed,
            vec![first],
            "M7A-127 (forced {second:?}): exactly one Committed, to the other node"
        );
        assert!(
            outcomes
                .iter()
                .any(|(node, outcome)| *node == second
                    && matches!(outcome, CasOutcome::Conflict { .. })),
            "M7A-127 (forced {second:?}): the loser is told Conflict: {outcomes:?}"
        );
        assert_eq!(
            PartitionRecord::decode(&body).map(|record| record.owner),
            Some(first),
            "M7A-127 (forced {second:?}): the store holds the winner's body"
        );

        let serving: Vec<NodeId> = [N1, N2]
            .into_iter()
            .filter(|node| {
                let mut driver = Driver::on(*node);
                driver
                    .store
                    .seed(ControlKey::Partition(PartitionId(5)), body.clone())
                    .expect("seed");
                driver.become_held();
                driver.kernel.view().served.contains_key(&PartitionId(5))
            })
            .collect();
        assert_eq!(
            serving,
            vec![first],
            "M7A-127 (forced {second:?}): A1's lineage view shows one owner, the winner"
        );
    }
}

/// Report a row as parked on a missing seam, as `campaign.rs` does. Asserts the named package
/// still reports `Unavailable`, so the row fails once the package is wired.
fn parked(row: &str, package: PackageId, what: &str) {
    let state = environment_capabilities()
        .into_iter()
        .find(|(candidate, _)| *candidate == package)
        .map(|(_, state)| state);
    assert_eq!(
        state,
        Some(CapabilityState::Unavailable),
        "{package:?} now reports Wired: {row} must be written rather than left parked"
    );
    println!("{row}: unavailable(capability({package:?})) — {what}");
}

/// M7A-128 — parent/root validated (V5). **Parked**: the placement/V5 seam does not exist.
///
/// Plan row: a `Partition` record whose parent is not in the family ⇒ `Fence{Partition,
/// GenerationChanged}` or `Fact(FamilyRejected)`; never adopt. Plan §11 lists it `Unavailable
/// until placement/V5 seam`.
///
/// `PartitionRecord` has no parent and no root field, so there is no input to spell. The
/// destructure below names every field and has no `..`: when the seam adds a parent, this file
/// stops compiling here, which is the prompt to write the row. The `parked` call names `H1`
/// because the seam itself has no `PackageId` to ask; H1 is the placement host package and
/// reports `Unavailable` today.
#[retcd_test]
fn m7a_128_parent_root_validated_v5() {
    support::preamble();
    let PartitionRecord {
        partition: _,
        owner: _,
        generation: _,
        owner_epoch: _,
        config_version: _,
        lifecycle: _,
    } = PartitionRecord::default();
    parked(
        "M7A-128",
        PackageId::H1,
        "placement/V5 seam absent: PartitionRecord carries no parent or root to validate",
    );
}

/// M7A-129 — an admission limit is not a reload loop.
///
/// Plan row: M7A-31's 20 `ResourceExhaustedFatal` terminations, then one
/// `ResourceExhaustedResumable`. Zero `Reload` across the 20; exactly one for the resumable.
/// The count lives here only; M7A-31 owns the back-off and the cap.
///
/// The cap latches at 3, so A1 stops re-arming long before 20. The row re-opens the partitions
/// watch itself between terminations so all 20 refusals are really delivered. Only one family
/// is open for the last termination, so "exactly one" is one family's reload and not a
/// coincidence of two.
#[retcd_test]
fn m7a_129_admission_limit_not_reload_loop() {
    support::preamble();
    let mut driver = Driver::new();
    driver.become_held();
    let _ = driver.take_reloads();
    let reopen_partitions = |driver: &mut Driver| {
        let from = driver
            .kernel
            .cursor(ControlPrefix::Partitions)
            .expect("watched");
        driver
            .store
            .submit(
                A,
                &support::control_effect(
                    1,
                    ControlEffect::Watch {
                        prefix: ControlPrefix::Partitions,
                        from,
                    },
                ),
            )
            .expect("reopen");
    };

    while driver.terminations.len() < 20 {
        if driver.store.open_watches(A) == 0 {
            reopen_partitions(&mut driver);
        }
        let _ = driver.terminate(WatchTermination::ResourceExhaustedFatal);
    }
    assert_eq!(
        driver.terminations.len(),
        20,
        "M7A-129 fixture: exactly 20 refusals"
    );
    assert!(
        driver
            .terminations
            .iter()
            .all(|(_, t)| *t == WatchTermination::ResourceExhaustedFatal),
        "M7A-129 fixture: all 20 are the admission limit"
    );
    assert_eq!(
        driver.take_reloads(),
        Vec::<ControlPrefix>::new(),
        "M7A-129: zero Reload across 20 admission-limit terminations"
    );

    reopen_partitions(&mut driver);
    let _ = driver.terminate(WatchTermination::ResourceExhaustedResumable);
    assert_eq!(
        driver.take_reloads(),
        vec![ControlPrefix::Partitions],
        "M7A-129 positive control: one resumable termination, exactly one Reload"
    );
}
