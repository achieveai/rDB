//! #84 rows at the pure layer: [`owns_at`] against write admission's check 6, and the token's
//! order. Unit, well under a second.

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
