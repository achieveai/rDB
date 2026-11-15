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

fn view(held: OwnerToken, valid_through: u64) -> AuthorityView {
    AuthorityView {
        lineage: held.lineage(P),
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

/// own84_1c: the shared map reads a view on the clock it was published with, keeps only the
/// newest per partition, and a cleared map (a stopped node) owns nothing. `remaining` counts to
/// the first tick that does not admit. Unit.
#[test]
fn own84_1c_owner_views_read_the_newest_view_and_clear_on_stop() {
    let clock = HostClock::start();
    let map = OwnerViews::new(NodeId(2), clock);
    let old = token(1, 1);
    let new = token(2, 2);
    assert!(map.ownership(P).is_none(), "no view, no owner");
    map.publish(view(old, u64::MAX), clock);
    assert!(map.still_owner(P, old));
    map.publish(view(new, clock.now().0 + 60_000), clock);
    assert!(
        !map.still_owner(P, old),
        "a newer view retires the old token"
    );
    let owned = map.ownership(P).expect("owned");
    assert_eq!(
        (owned.node, owned.partition, owned.token),
        (NodeId(2), P, new)
    );
    assert!(
        owned.remaining <= Duration::from_millis(60_001),
        "{owned:?}"
    );
    assert!(owned.remaining > Duration::from_millis(59_000), "{owned:?}");
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
    // Horizon 10_000: the lead (1.5 s) starts at 8_501, `Deactivated` is due at 10_001.
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
            judged(live, 8_501, true, true),
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
            judged(live, 8_500, true, true),
            Active(t),
            vec![],
        ),
        (
            "the lead starts: Deactivating with the stop tick",
            Active(t),
            judged(live, 8_501, true, true),
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
            judged(Some(view(t, 15_000)), 9_000, false, false),
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
    assert_eq!(ownership_due(Active(t), Some(&at), lead), Some(Tick(8_501)));
    assert_eq!(
        ownership_due(Deactivating(t), Some(&at), lead),
        Some(Tick(10_001)),
        "Deactivated has its own timer at the horizon + 1 (ruling B-1)"
    );
    assert_eq!(ownership_due(Inactive, Some(&at), lead), None);
}

/// The spec's horizon after a renewal dispatched at `dispatch`: `G - eps - delta - 1` later,
/// as `Authority::admission_horizon` gives it at full `eps` (`DEACTIVATING_LEAD_MILLIS`'s doc).
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
/// nothing. A 4.5 s stall sends `Deactivating` 1.5 s before the old horizon, then `Activated`
/// with the same token when its renewal lands, and never `Deactivated`. Driven tick by tick
/// on the spec's budgets. It models the grant's bound on the horizon only; the clock sample's
/// bound, which binds in steady state, is the host row's (`own83_8_..._on_three_hosts`, a
/// real stall on three hosts).
/// Cut: `DEACTIVATING_LEAD_MILLIS = G/2` (3000), red: the 3 s stall sends `Deactivating` at
/// 2799. Unit.
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

    let (gone, receiver) = Subscriber::new(2);
    drop(receiver);
    assert_eq!(gone.send(event), Err("gone"));
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
