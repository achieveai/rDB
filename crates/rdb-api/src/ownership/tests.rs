//! #84 rows at the pure layer: [`owns_at`] against write admission's check 6, and the token's
//! order. #83 rows at the same layer: the ownership state table, the lead's arithmetic, a
//! lagging subscriber and the route marks. Unit, well under a second.

use rdb_core::contracts::authority::DenyReason;
use rdb_core::contracts::ids::{AuthorityGeneration, BootId, ConfigVersion, GrantId};

use super::*;

const P: PartitionId = PartitionId(3);

fn token(generation: u64, owner_epoch: u64) -> OwnerToken {
    OwnerToken {
        generation: Generation(generation),
        owner_epoch: OwnerEpoch(owner_epoch),
    }
}

/// A second partition, for the rows that check one partition's change leaves another alone.
const Q: PartitionId = PartitionId(5);

fn view(held: OwnerToken, valid_through: u64) -> AuthorityView {
    view_of(P, held, valid_through)
}

fn view_of(partition: PartitionId, held: OwnerToken, valid_through: u64) -> AuthorityView {
    AuthorityView {
        lineage: held.lineage(partition),
        grant_id: GrantId(7),
        boot_id: BootId(1),
        authority_generation: AuthorityGeneration(1),
        config_version: ConfigVersion(1),
        authority_seq: 4,
        valid_through_tick: Tick(valid_through),
        past_horizon: DenyReason::Expired,
    }
}

/// Write admission's check 6 on `view` for a T1 serving the view's own lineage, at `now`:
/// [`view_admits`], which `transaction::admit` calls. The truth table above it is the row's
/// independent claim; this line pins that `owns_at` adds nothing to the shared test.
fn check_6_admits(view: AuthorityView, now: Tick) -> bool {
    view_admits(&view, view.lineage, now) == ViewAdmits::Admits
}

/// own84_1: `owns_at` is check 6 at every tick around the horizon. A view admits through its
/// `valid_through_tick` and not one tick after; a fence's view (horizon the tick before the
/// fence) owns nothing at the fence tick; a token other than the view's, older or newer, is never
/// owned. Unit.
#[test]
fn own84_1_owns_at_matches_the_entry_check_at_every_tick() {
    let held = token(5, 9);
    let h = 1_000;
    for horizon in [h - 1, h, h + 1] {
        let v = view(held, horizon);
        for now in [h - 1, h, h + 1] {
            let now = Tick(now);
            assert_eq!(
                owns_at(&v, now, held),
                now.0 <= horizon,
                "horizon {horizon}, now {}",
                now.0
            );
            assert_eq!(owns_at(&v, now, held), check_6_admits(v, now));
        }
    }
    // A fence at tick `h` publishes a view through `h - 1` (`Authority::fence`).
    let fenced = view(held, h - 1);
    assert!(
        !owns_at(&fenced, Tick(h), held),
        "a fenced view owns nothing"
    );
    assert!(owns_at(&fenced, Tick(h - 1), held), "precondition");
    let live = view(held, u64::MAX);
    for other in [
        token(5, 8),
        token(4, 9),
        token(4, 99),
        token(5, 10),
        token(6, 1),
    ] {
        assert!(
            !owns_at(&live, Tick(h), other),
            "{other} is not the view's {held}"
        );
    }
}

/// own84_1b: the token orders by generation first, then epoch, as the ADR-rdb-0007 sequencer
/// does. A later generation outranks any epoch of an earlier one. Unit.
#[test]
fn own84_1b_tokens_order_by_generation_then_epoch() {
    assert!(token(2, 1) > token(1, 9));
    assert!(token(2, 3) > token(2, 2));
    assert_eq!(token(2, 3), OwnerToken::of(token(2, 3).lineage(P)));
    assert_eq!(token(2, 3).to_string(), "2.3");
}

/// own84_1c: the shared map keeps only the newest view per partition, and a cleared map (a
/// stopped node) owns nothing. On a held clock, so `remaining` is exact: it counts the whole
/// milliseconds to the last admitting tick, reads zero on that tick, and the tick after it owns
/// nothing. Two short, distinct horizons, so a constant answer cannot pass. Unit.
#[test]
fn own84_1c_owner_views_read_the_newest_view_and_clear_on_stop() {
    let clock = HostClock::capped(20);
    let _hold = clock.hold();
    let now = clock.now();
    let map = OwnerViews::new(NodeId(2), clock);
    let old = token(1, 1);
    let new = token(2, 2);
    assert!(map.ownership(P).is_none(), "no view, no owner");
    map.publish(view(old, u64::MAX), clock, now, false);
    assert!(map.still_owner(P, old));
    map.publish(view(new, now.0 + 250), clock, now, false);
    assert!(
        !map.still_owner(P, old),
        "a newer view retires the old token"
    );
    let owned = map.ownership(P).expect("owned");
    assert_eq!(
        owned,
        Ownership {
            node: NodeId(2),
            partition: P,
            token: new,
            remaining: Duration::from_millis(250),
        }
    );
    map.publish(view(new, now.0 + 40), clock, now, false);
    assert_eq!(
        map.ownership(P).map(|o| o.remaining),
        Some(Duration::from_millis(40)),
        "a second, shorter horizon"
    );
    map.publish(view(new, now.0), clock, now, false);
    assert_eq!(
        map.ownership(P).map(|o| o.remaining),
        Some(Duration::ZERO),
        "the last admitting tick: still owned, nothing left to spend"
    );
    assert!(map.still_owner(P, new));
    map.publish(view(new, now.0 - 1), clock, now, false);
    assert!(map.ownership(P).is_none(), "the tick after the horizon");
    assert!(!map.still_owner(P, new));
    map.publish(view(new, now.0 + 250), clock, now, false);
    assert!(!map.still_owner(PartitionId(4), new), "another partition");
    map.clear();
    assert!(!map.still_owner(P, new));
    assert!(map.ownership(P).is_none());
}

// ------------------------------------------------------------------ #83

/// A partition's state at `now` with `view`, every writability half as given.
fn judged(view: Option<AuthorityView>, now: u64, writable: bool, queue_open: bool) -> Judged {
    Judged {
        view,
        now: Tick(now),
        faulted: false,
        writable,
        queue_open,
        lead_millis: deactivating_lead_millis(&Budgets::SPEC_DEFAULTS),
    }
}

/// own83_1: the ownership state table. Each row is (state before, what the host knows, state
/// after, transitions sent). A skipped `Deactivating` (a fence, a fault, a newer token) is
/// sent with the deadline now, so every `Deactivated` follows a `Deactivating`; a read-only
/// commit (not writable) and a frozen queue never activate; once active, neither deactivates;
/// a renewal that lands while `Deactivating` sends `Activated` again with the same token. Unit.
#[test]
fn own83_1_ownership_state_table() {
    use OwnState::{Active, Deactivating, Inactive};
    let t = token(2, 2);
    let newer = token(3, 3);
    // Horizon 10_000: the lead (1 s at the spec's budgets) starts at 9_001, `Deactivated` is
    // due at 10_001.
    let live = Some(view(t, 10_000));
    let fenced = Some(view(t, 4_999));
    let rows: Vec<(&str, OwnState, Judged, OwnState, Vec<Step>)> = vec![
        (
            "no view",
            Inactive,
            judged(None, 5_000, true, true),
            Inactive,
            vec![],
        ),
        (
            "writable primary activates",
            Inactive,
            judged(live, 5_000, true, true),
            Active(t),
            vec![Step::Activated(t)],
        ),
        (
            "a read-only-capped commit is not writable: no Activated",
            Inactive,
            judged(live, 5_000, false, true),
            Inactive,
            vec![],
        ),
        (
            "a frozen queue does not activate",
            Inactive,
            judged(live, 5_000, true, false),
            Inactive,
            vec![],
        ),
        (
            "inside the lead does not activate",
            Inactive,
            judged(live, 9_001, true, true),
            Inactive,
            vec![],
        ),
        (
            "active stays active through a brief freeze",
            Active(t),
            judged(live, 5_000, false, false),
            Active(t),
            vec![],
        ),
        (
            "active, one tick before the lead",
            Active(t),
            judged(live, 9_000, true, true),
            Active(t),
            vec![],
        ),
        (
            "the lead starts: Deactivating with the stop tick",
            Active(t),
            judged(live, 9_001, true, true),
            Deactivating(t),
            vec![Step::Deactivating(t, Tick(10_001))],
        ),
        (
            "deactivating, last admitting tick",
            Deactivating(t),
            judged(live, 10_000, true, true),
            Deactivating(t),
            vec![],
        ),
        (
            "the tick after the horizon: Deactivated",
            Deactivating(t),
            judged(live, 10_001, true, true),
            Inactive,
            vec![Step::Deactivated(t)],
        ),
        (
            "a renewal while deactivating: resume, same token",
            Deactivating(t),
            judged(Some(view(t, 15_000)), 9_500, false, false),
            Active(t),
            vec![Step::Activated(t)],
        ),
        (
            "a fence view: Deactivating now, then Deactivated",
            Active(t),
            judged(fenced, 5_000, true, true),
            Inactive,
            vec![Step::Deactivating(t, Tick(5_000)), Step::Deactivated(t)],
        ),
        (
            "a newer token: the old one ends, the new one activates",
            Active(t),
            judged(Some(view(newer, 10_000)), 5_000, true, true),
            Active(newer),
            vec![
                Step::Deactivating(t, Tick(5_000)),
                Step::Deactivated(t),
                Step::Activated(newer),
            ],
        ),
        (
            "past the horizon while inactive: nothing",
            Inactive,
            judged(live, 10_001, true, true),
            Inactive,
            vec![],
        ),
    ];
    for (name, before, judged, after, steps) in rows {
        assert_eq!(ownership_step(before, &judged), (after, steps), "{name}");
    }
    let faulted = Judged {
        faulted: true,
        ..judged(live, 5_000, true, true)
    };
    assert_eq!(
        ownership_step(Active(t), &faulted),
        (
            Inactive,
            vec![Step::Deactivating(t, Tick(5_000)), Step::Deactivated(t)]
        ),
        "a faulted host deactivates at once"
    );
    assert_eq!(ownership_step(Inactive, &faulted), (Inactive, vec![]));
    let lead = deactivating_lead_millis(&Budgets::SPEC_DEFAULTS);
    let at = view(t, 10_000);
    assert_eq!(ownership_due(Active(t), Some(&at), lead), Some(Tick(9_001)));
    assert_eq!(
        ownership_due(Deactivating(t), Some(&at), lead),
        Some(Tick(10_001)),
        "Deactivated has its own timer at the horizon + 1 (ruling B-1)"
    );
    assert_eq!(ownership_due(Inactive, Some(&at), lead), None);
}

/// The spec's horizon after a renewal dispatched at `dispatch`: `G - eps - delta - 1` later,
/// as `Authority::admission_horizon` gives it at full `eps` ([`deactivating_lead_millis`]'s doc).
fn spec_horizon(dispatch: u64) -> u64 {
    let b = Budgets::SPEC_DEFAULTS;
    dispatch + b.grant_millis - b.clock_error_millis - b.dispatch_margin_millis - 1
}

/// Every transition an active partition sends, tick by tick from a committed renewal at 0 to
/// `until`, when its next renewal is dispatched at `dispatch` and lands at `lands`.
fn stall_steps(dispatch: u64, lands: u64, until: u64) -> Vec<(u64, Step)> {
    let t = token(2, 2);
    let mut state = OwnState::Active(t);
    let mut sent = Vec::new();
    for now in 0..=until {
        let horizon = if now < lands {
            spec_horizon(0)
        } else {
            spec_horizon(dispatch)
        };
        let (next, steps) = ownership_step(state, &judged(Some(view(t, horizon)), now, true, true));
        state = next;
        sent.extend(steps.into_iter().map(|step| (now, step)));
    }
    sent
}

/// own83_8 at the pure layer (ruling B-2): a 3 s control stall that starts 499 ms after a
/// committed renewal, whose next renewal is dispatched as it ends and lands 300 ms later, sends
/// nothing. A 4.5 s stall sends `Deactivating` the lead (1 s) before the old horizon, then
/// `Activated`
/// with the same token when its renewal lands, and never `Deactivated`. Driven tick by tick
/// on the spec's budgets. It models the grant's bound on the horizon only; the clock sample's
/// bound, which binds in steady state, is the host row's (`own83_8_..._on_three_hosts`, a
/// real stall on three hosts).
/// Cut: `deactivating_lead_millis` without its clock term (the lead is then the grant's `G/4`,
/// 1500), red at "2000 - 2 x 500 at the spec's budgets"; both stalls' steps stay as asserted
/// at that lead. Unit.
#[test]
fn own83_8_a_3s_control_stall_gives_no_deactivating() {
    let t = token(2, 2);
    // The stall: 499 .. 3_499; the renewal is dispatched at its end and commits 300 ms later.
    // Each run ends well before the next renewal's own lead would start.
    let short = stall_steps(3_499, 3_799, 6_000);
    assert_eq!(short, vec![], "a 3 s stall is ridden out silently");

    let long = stall_steps(4_999, 5_299, 8_000);
    let old_stop = spec_horizon(0) + 1;
    assert_eq!(
        long,
        vec![
            (
                old_stop - DEACTIVATING_LEAD_MILLIS,
                Step::Deactivating(t, Tick(old_stop))
            ),
            (5_299, Step::Activated(t)),
        ],
        "a 4.5 s stall warns, then resumes with the same token"
    );
    assert_eq!(
        deactivating_lead_millis(&Budgets::SPEC_DEFAULTS),
        DEACTIVATING_LEAD_MILLIS
    );
    assert_eq!(
        DEACTIVATING_LEAD_MILLIS, 1_000,
        "2000 - 2 x 500 at the spec's budgets"
    );
}

/// Every transition an active partition sends, tick by tick from 0 to `until`, at the spec's
/// budgets, with A1 stepped at each tick in `samples` (each step a fresh local sample) and every
/// renewal landing on time. The horizon is `Authority::admission_horizon`'s: the grant's
/// (`spec_horizon` of the last renewal, one per `renew_millis`) or the sample's (the last sample
/// plus `max_sample_age`, past which `effective_epsilon` calls it stale), whichever is first. A1
/// runs before the ownership duty on a turn, so a sample at `now` counts at `now`.
fn sampled_steps(samples: &[u64], until: u64) -> Vec<(u64, Step)> {
    let b = Budgets::SPEC_DEFAULTS;
    let t = token(2, 2);
    let mut state = OwnState::Active(t);
    let mut sent = Vec::new();
    for now in 0..=until {
        let renewed = now - now % b.renew_millis;
        let sampled = samples
            .iter()
            .copied()
            .filter(|at| *at <= now)
            .max()
            .expect("a sample at 0");
        let horizon = spec_horizon(renewed).min(sampled + b.max_sample_age_millis);
        let (next, steps) = ownership_step(state, &judged(Some(view(t, horizon)), now, true, true));
        state = next;
        sent.extend(steps.into_iter().map(|step| (now, step)));
    }
    sent
}

/// own83_10 (lead ruling on B1 item 8, row a): in steady state, an A1 step late by one whole
/// `clock_sample_period` (samples at 3000 then 4000, not 3500) sends no event. The steady-state
/// horizon is the sample's, 2000 ms ahead, so a false `Deactivating` here is what SBX would
/// act on. Cut: the old lead, `min(1500, G/4)`, red: `Deactivating` at 3501, `Activated` at
/// 4000. Unit.
#[test]
fn own83_10_a_sample_late_by_one_period_gives_no_deactivating() {
    let period = Budgets::SPEC_DEFAULTS.clock_sample_period_millis;
    let mut samples: Vec<u64> = (0..=6).map(|i| i * period).collect();
    samples.extend((8..=16).map(|i| i * period));
    assert!(!samples.contains(&3_500), "the late one");
    assert_eq!(
        sampled_steps(&samples, 8_000),
        vec![],
        "a late sample warns nothing"
    );
}

/// own83_10 row b: when A1's samples stop altogether (the last at 3000), ownership really ends at
/// that sample + `max_sample_age`, and `Deactivating` comes at least the lead before
/// `Deactivated`. Cut: `Judged::low` always false, red: `Deactivating` and `Deactivated` on the
/// same tick. Unit.
#[test]
fn own83_10b_samples_stopping_still_warn_the_lead_ahead() {
    let t = token(2, 2);
    let period = Budgets::SPEC_DEFAULTS.clock_sample_period_millis;
    let samples: Vec<u64> = (0..=6).map(|i| i * period).collect();
    let stop = 3_000 + Budgets::SPEC_DEFAULTS.max_sample_age_millis + 1;
    let sent = sampled_steps(&samples, 8_000);
    assert_eq!(
        sent,
        vec![
            (
                stop - DEACTIVATING_LEAD_MILLIS,
                Step::Deactivating(t, Tick(stop))
            ),
            (stop, Step::Deactivated(t)),
        ],
        "warned, then stopped at the sample's horizon + 1"
    );
    assert!(sent[1].0 - sent[0].0 >= DEACTIVATING_LEAD_MILLIS);
}

/// own83_12 (#123 review): a fresh clock sample alone withdraws a warning. One renewal, at 0,
/// and none after, so the grant's horizon stays at `spec_horizon(0)` throughout. Samples every
/// period to 1000, then none until 2600: the sample's horizon (3000) comes within the lead at
/// 2001, and the sample at 2600 moves it out to 4600 with no renewal: `Activated` again, same
/// token. Run to 4000, before the grant's own lead starts (4800). Cut: `ownership_step` keeping
/// a `Deactivating` term whose time left is above the lead again, red: no `Activated` at 2600.
/// Unit.
#[test]
fn own83_12_a_fresh_sample_withdraws_a_warning_without_a_renewal() {
    let b = Budgets::SPEC_DEFAULTS;
    let t = token(2, 2);
    let samples = [0, 500, 1_000, 2_600, 3_000, 3_500, 4_000];
    let mut state = OwnState::Active(t);
    let mut sent = Vec::new();
    for now in 0..=4_000 {
        let sampled = samples
            .iter()
            .copied()
            .filter(|at| *at <= now)
            .max()
            .expect("a sample at 0");
        let horizon = spec_horizon(0).min(sampled + b.max_sample_age_millis);
        let (next, steps) = ownership_step(state, &judged(Some(view(t, horizon)), now, true, true));
        state = next;
        sent.extend(steps.into_iter().map(|step| (now, step)));
    }
    let stop = 1_000 + b.max_sample_age_millis + 1;
    assert_eq!(
        sent,
        vec![
            (
                stop - DEACTIVATING_LEAD_MILLIS,
                Step::Deactivating(t, Tick(stop))
            ),
            (2_600, Step::Activated(t)),
        ],
        "warned by the stale sample, withdrawn by the fresh one"
    );
}

/// own83_13 (#123 review): a fence ends a term at once, and what it sends depends on where the
/// term stands. From `Active`: `Deactivating` with the deadline now, then `Deactivated`. From
/// `Deactivating` (already warned): `Deactivated` only. Not held: nothing. A fence is the view
/// gone (the host judges a fence's view as none) or a view that no longer admits now.
/// Cut: `ownership_step` sending `Deactivating` before `Deactivated` from either state, red at
/// "from Deactivating". Unit.
#[test]
fn own83_13_a_fence_while_deactivating_sends_only_deactivated() {
    use OwnState::{Active, Deactivating, Inactive};
    let t = token(2, 2);
    let fenced_at = 5_000;
    for (name, fenced) in [
        ("the view gone", None),
        (
            "a view through the tick before",
            Some(view(t, fenced_at - 1)),
        ),
    ] {
        let judged = judged(fenced, fenced_at, true, true);
        assert_eq!(
            ownership_step(Active(t), &judged),
            (
                Inactive,
                vec![Step::Deactivating(t, Tick(fenced_at)), Step::Deactivated(t)]
            ),
            "from Active, {name}"
        );
        assert_eq!(
            ownership_step(Deactivating(t), &judged),
            (Inactive, vec![Step::Deactivated(t)]),
            "from Deactivating, {name}"
        );
        assert_eq!(
            ownership_step(Inactive, &judged),
            (Inactive, vec![]),
            "not held, {name}"
        );
    }
}

/// own83_14 (#123 review): the lead's formula as `embed.md` gives it,
/// `min(max_sample_age - 2 x clock_sample_period, grant / 4)`: the clock term at the defaults,
/// the grant cap for short grants (rounded down), and a clock term that saturates at 0 rather
/// than wrapping. Cut: no grant cap, red at "the crate tests' 600 ms grant". Unit.
#[test]
fn own83_14_the_lead_is_the_clock_term_capped_by_a_quarter_grant() {
    let spec = Budgets::SPEC_DEFAULTS;
    let with = |grant_millis, max_sample_age_millis, clock_sample_period_millis| Budgets {
        grant_millis,
        max_sample_age_millis,
        clock_sample_period_millis,
        ..spec
    };
    assert_eq!(
        deactivating_lead_millis(&spec),
        1_000,
        "defaults: the clock term"
    );
    assert_eq!(DEACTIVATING_LEAD_MILLIS, deactivating_lead_millis(&spec));
    assert_eq!(
        deactivating_lead_millis(&with(600, 2_000, 500)),
        150,
        "the crate tests' 600 ms grant: the cap"
    );
    assert_eq!(
        deactivating_lead_millis(&with(3_000, 2_000, 500)),
        750,
        "a grant whose quarter is below the clock term"
    );
    assert_eq!(
        deactivating_lead_millis(&with(601, 2_000, 500)),
        150,
        "a quarter rounds down"
    );
    assert_eq!(
        deactivating_lead_millis(&with(6_000, 900, 500)),
        0,
        "the clock term saturates at 0"
    );
    assert_eq!(
        deactivating_lead_millis(&with(6_000, 2_000, u64::MAX)),
        0,
        "and so does twice the period"
    );
}

/// own83_7b: a subscriber that falls [`SUBSCRIBER_CAPACITY`] events behind is dropped for every
/// host, and its receiver then ends; one that keeps up is never dropped. Unit.
#[test]
fn own83_7b_a_lagging_subscriber_is_dropped_and_its_receiver_ends() {
    let event = OwnershipEvent::Activated {
        node: NodeId(1),
        partition: P,
        token: token(2, 2),
    };
    let (lagging, unread) = Subscriber::new(2);
    assert_eq!(lagging.send(event), Ok(()));
    assert_eq!(lagging.send(event), Ok(()));
    assert_eq!(lagging.send(event), Err("full"));
    lagging.drop_everywhere();
    assert_eq!(lagging.send(event), Err("dropped"), "every host lets it go");
    drop(lagging);
    assert_eq!(
        unread.iter().count(),
        2,
        "what it held is still read, then it ends"
    );

    // #123 review: dropping the receiver drops the subscriber for every host, so a host lets
    // go of it at its next turn even when it has nothing to send.
    let (gone, receiver) = Subscriber::new(2);
    drop(receiver);
    assert!(gone.is_dropped(), "a dropped receiver tells the hosts");
    assert_eq!(gone.send(event), Err("dropped"));
}

/// own83_3 at the cache layer: a route read under an open `partitions/` watch is current until a
/// change of its own partition is delivered or any watch ends; another partition's change
/// leaves it current; with no watch open nothing is cached, because nothing would tell the
/// cache. Unit.
#[test]
fn own83_3_route_marks_make_an_answer_stale_on_its_partition_or_a_watch_end() {
    let marks = Arc::new(RouteMarks::default());
    let p2 = PartitionId(2);
    assert_eq!(marks.stamp(p2), None, "no watch open: no cache");
    let first = marks.opened();
    let stamp = marks.stamp(p2).expect("a watch is open");
    assert!(marks.current(p2, stamp));
    marks.changed(PartitionId(3));
    assert!(marks.current(p2, stamp), "another partition's change");
    marks.changed(p2);
    assert!(!marks.current(p2, stamp), "its own partition's change");

    let stamp = marks.stamp(p2).expect("still open");
    let second = marks.opened();
    assert!(marks.current(p2, stamp), "a second node's watch opening");
    drop(first);
    assert!(
        !marks.current(p2, stamp),
        "a watch's end may have missed a change"
    );
    let stamp = marks.stamp(p2).expect("the second is open");
    drop(second);
    assert!(!marks.current(p2, stamp));
    assert_eq!(marks.stamp(p2), None);
}

/// own84_1d: a publication brings its host's clock, and the map reads by the newest one. Two
/// held readings 10 s apart (the trio's `lagging` clock): the same view reads a different
/// `remaining` on each, and a view live on the lagging clock is past on the other. Unit.
#[test]
fn own84_1d_a_publication_replaces_the_clock_the_map_reads() {
    let ahead = HostClock::capped(20);
    let _hold = ahead.hold();
    let behind = ahead.lagging(10_000);
    let (a, b) = (ahead.now(), behind.now());
    assert_eq!(
        a.0 - b.0,
        10_000,
        "precondition: two distinct held readings"
    );
    let held = token(3, 1);
    let map = OwnerViews::new(NodeId(1), ahead);
    map.publish(view(held, a.0 + 100), ahead, a, false);
    assert_eq!(
        map.ownership(P).map(|o| o.remaining),
        Some(Duration::from_millis(100))
    );
    map.publish(view(held, a.0 + 100), behind, b, false);
    assert_eq!(
        map.ownership(P).map(|o| o.remaining),
        Some(Duration::from_millis(10_100)),
        "read on the clock the last view came with"
    );
    map.publish(view(held, b.0 + 5), behind, b, false);
    assert!(map.still_owner(P, held), "live on the lagging clock");
    map.publish(view(held, b.0 + 5), ahead, b, false);
    assert!(
        !map.still_owner(P, held),
        "the same view, read on the other clock, is past"
    );
}

/// own84_1e: `remaining` is a lower bound at every phase inside a tick. A tick is a whole
/// millisecond, so the real time behind tick `n` is `n + f` for `f` in `[0, 1)` ms, and the view
/// admits until real time `horizon + 1`. At every tenth of a millisecond around the horizon, the
/// budget is at most the real time left, and never short by a whole tick or more. Cut: `+ 1`
/// back in `remaining_at`, red at `f = 0.1` on every tick. Unit.
#[test]
fn own84_1e_remaining_never_overstates_the_time_left_inside_a_tick() {
    let horizon = 1_000_u64;
    for n in horizon - 3..=horizon {
        for tenth in 0..10_u64 {
            let real_us = n * 1_000 + tenth * 100;
            let left_us = (horizon + 1) * 1_000 - real_us;
            let budget = remaining_at(Tick(horizon), Tick(n));
            let budget_us = u64::try_from(budget.as_micros()).expect("small");
            assert!(
                budget_us <= left_us,
                "tick {n} + 0.{tenth} ms: budget {budget_us} us, left {left_us} us"
            );
            assert!(
                left_us - budget_us <= 1_000,
                "tick {n} + 0.{tenth} ms: budget {budget_us} us short of {left_us} us by a tick"
            );
        }
    }
    assert_eq!(remaining_at(Tick(horizon), Tick(horizon)), Duration::ZERO);
    assert_eq!(
        remaining_at(Tick(0), Tick(0)),
        Duration::ZERO,
        "tick 0, equal"
    );
    assert_eq!(
        remaining_at(Tick(0), Tick(1)),
        Duration::ZERO,
        "tick 0, past"
    );
    assert_eq!(
        remaining_at(Tick(horizon), Tick(horizon + 5)),
        Duration::ZERO
    );
}

/// own84_1f: a fence's view (horizon the tick before its publication) is not kept, so a clock
/// stepped back, once or again, never makes the ended token owned again. A later live view of
/// the same token (a regrant, as admission admits it) is owned. Read on a guarded clock, the one
/// `rdb_node --clock-step-file` steps back. Cut: keep the past view in `publish`, red at the
/// first step. Unit.
#[test]
fn own84_1f_a_fenced_view_stays_unowned_across_repeated_backward_steps() {
    let clock = HostClock::guarded(0);
    let held = token(4, 2);
    let map = OwnerViews::new(NodeId(1), clock);
    let granted = clock.now();
    map.publish(view(held, granted.0 + 60_000), clock, granted, false);
    let other = view_of(Q, held, granted.0 + 600_000);
    map.publish(other, clock, granted, false);
    assert!(map.still_owner(P, held), "precondition: owned");
    let fenced_at = clock.now();
    map.publish(view(held, fenced_at.0 - 1), clock, fenced_at, false);
    assert!(!map.still_owner(P, held), "the fence ends it");
    assert!(
        map.still_owner(Q, held) && map.ownership(Q).is_some(),
        "another partition's live view is kept"
    );
    for step in 1..=3 {
        clock.step_back(5_000);
        assert!(
            clock.now() < fenced_at,
            "precondition: step {step} is back past the fence"
        );
        assert!(!map.still_owner(P, held), "owned again after step {step}");
        assert!(map.ownership(P).is_none(), "step {step}");
    }
    let regranted = clock.now();
    map.publish(view(held, regranted.0 + 60_000), clock, regranted, false);
    assert!(map.still_owner(P, held), "a live regrant of the same token");
    let taken = token(4, 3);
    map.publish(view(taken, regranted.0 + 60_000), clock, regranted, false);
    assert!(
        !map.still_owner(P, held),
        "a higher token here retires the old one"
    );
}

/// own84_1g: inputs no live view answers. A zero token against a live, nonzero view; partition
/// zero and a partition with no view; each through both readers. Unit.
#[test]
fn own84_1g_a_zero_token_and_unknown_partitions_own_nothing() {
    let clock = HostClock::capped(20);
    let _hold = clock.hold();
    let now = clock.now();
    let map = OwnerViews::new(NodeId(1), clock);
    let held = token(2, 3);
    map.publish(view(held, now.0 + 1_000), clock, now, false);
    assert!(map.still_owner(P, held), "precondition");
    assert!(!map.still_owner(P, token(0, 0)), "a zero token");
    for partition in [PartitionId(0), PartitionId(99)] {
        assert!(map.ownership(partition).is_none(), "{partition:?}");
        assert!(!map.still_owner(partition, held), "{partition:?}");
        assert!(!map.still_owner(partition, token(0, 0)), "{partition:?}");
    }
}

/// own84_1h: a revoked map (a faulted host) owns nothing, and keeps nothing published after,
/// however many times it is revoked. Unit.
#[test]
fn own84_1h_a_revoked_map_owns_nothing_for_good() {
    let clock = HostClock::capped(20);
    let _hold = clock.hold();
    let now = clock.now();
    let map = OwnerViews::new(NodeId(1), clock);
    let held = token(2, 3);
    map.publish(view(held, now.0 + 1_000), clock, now, false);
    assert!(map.still_owner(P, held), "precondition");
    map.publish(view_of(Q, held, now.0 + 1_000), clock, now, false);
    assert!(map.still_owner(Q, held), "precondition: a second partition");
    let other_clock = clock.lagging(10_000);
    for round in 1..=2 {
        map.revoke();
        assert!(!map.still_owner(P, held), "round {round}");
        assert!(map.ownership(P).is_none(), "round {round}");
        map.publish(view(held, now.0 + 1_000), clock, now, false);
        // Another partition, on another clock: revoke is the whole map's, not one entry's.
        map.publish(
            view_of(Q, held, now.0 + 1_000),
            other_clock,
            other_clock.now(),
            false,
        );
        for partition in [P, Q] {
            assert!(
                !map.still_owner(partition, held),
                "a view after revoke, round {round}, {partition:?}"
            );
            assert!(
                map.ownership(partition).is_none(),
                "round {round}, {partition:?}"
            );
        }
    }
    // A stopped node's clear does not undo the revoke.
    map.clear();
    map.publish(view(held, now.0 + 1_000), clock, now, false);
    assert!(!map.still_owner(P, held), "revoke, clear, then a view");
    assert!(map.ownership(P).is_none(), "revoke, clear, then a view");
}

/// own84_1i (#84 review): the clock is stepped back between the host's step capture and the
/// fence's publication. The past view is published with the captured tick, so it is removed:
/// both readers read false at once and after another step back. Compared with the clock's
/// fresh reading instead, the view would be kept and admit again. Read on a guarded clock.
/// Cut: `publish` comparing the horizon with `clock.now()` rather than `at`, red at "at once".
/// Unit.
#[test]
fn own84_1i_a_rollback_between_the_step_and_the_fence_publication_keeps_it_ended() {
    let clock = HostClock::guarded(0);
    let held = token(4, 2);
    let map = OwnerViews::new(NodeId(1), clock);
    let granted = clock.now();
    map.publish(view(held, granted.0 + 60_000), clock, granted, false);
    assert!(map.still_owner(P, held), "precondition: owned");
    let fenced_at = clock.now();
    clock.step_back(5_000);
    assert!(
        clock.now() < fenced_at,
        "precondition: rolled back past the step"
    );
    map.publish(view(held, fenced_at.0 - 1), clock, fenced_at, false);
    assert!(!map.still_owner(P, held), "at once");
    assert!(map.ownership(P).is_none(), "at once");
    clock.step_back(5_000);
    assert!(!map.still_owner(P, held), "after another step back");
    assert!(map.ownership(P).is_none(), "after another step back");
}

/// own84_1j (#84 review): a still-live view's clock rolls back. On a held clock with a guard,
/// `remaining` is exact before and after each step back: it grows by exactly the step, and the
/// view stays owned. Unit.
#[test]
fn own84_1j_remaining_follows_a_rollback_of_a_live_view_exactly() {
    let clock = HostClock::capped(20).with_guard();
    let _hold = clock.hold();
    let now = clock.now();
    let held = token(3, 1);
    let map = OwnerViews::new(NodeId(1), clock);
    map.publish(view(held, now.0 + 1_000), clock, now, false);
    let remaining = |map: &OwnerViews| map.ownership(P).map(|owned| owned.remaining);
    assert_eq!(
        remaining(&map),
        Some(Duration::from_millis(1_000)),
        "before"
    );
    clock.step_back(300);
    assert_eq!(
        clock.now().0,
        now.0 - 300,
        "precondition: the held clock stepped"
    );
    assert_eq!(
        remaining(&map),
        Some(Duration::from_millis(1_300)),
        "one step back"
    );
    assert!(map.still_owner(P, held));
    clock.step_back(200);
    assert_eq!(
        remaining(&map),
        Some(Duration::from_millis(1_500)),
        "two steps back"
    );
    assert!(map.still_owner(P, held));
}

/// own84_1k (#84 review): a view marked as a fence's (`fenced`: A1 fenced its partition in that
/// step) is removed whatever its horizon, including a horizon equal to the step tick, which a
/// fence at tick 0 publishes. Unmarked, the same view through the step tick is owned. A
/// regrant after it is owned. Another partition is left alone. Cut: `publish` ignoring
/// `fenced`, red. Unit.
#[test]
fn own84_1k_a_fenced_view_is_removed_even_through_its_step_tick() {
    let clock = HostClock::capped(20);
    let _hold = clock.hold();
    let now = clock.now();
    let held = token(2, 3);
    let map = OwnerViews::new(NodeId(1), clock);
    map.publish(view_of(Q, held, now.0 + 1_000), clock, now, false);
    map.publish(view(held, now.0), clock, now, false);
    assert!(map.still_owner(P, held), "precondition: owned through now");
    map.publish(view(held, now.0), clock, now, true);
    assert!(!map.still_owner(P, held), "the fence's view");
    assert!(map.ownership(P).is_none(), "the fence's view");
    assert!(map.still_owner(Q, held), "another partition");
    map.publish(view(held, now.0 + 1_000), clock, now, false);
    assert!(map.still_owner(P, held), "a regrant");
}
