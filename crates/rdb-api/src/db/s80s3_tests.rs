//! #80 S3 (design `arch-80-s34` rev 3): kill one host of five, and every partition stays
//! writable within [`write_outage_bound`]. Five declared nodes on the S1b rig
//! ([`super::formation_tests`]), each process on its own folder, its own host and its own
//! runtime, sixteen partitions on the ring, the failover planner on every node.
//!
//! A probe thread per partition puts through a surviving node every [`PROBE_GAP`], with puts that
//! time out after [`PROBE_PUT_TIMEOUT`], the timeout the bound assumes. A partition's time to
//! writable is from the moment the host is lost to the first put that lands after one was
//! refused; a partition that refused nothing had no outage. Each class's time is logged as
//! `s80s3_time_to_writable_ms`. Deadlines are the bound times `RETCD_TEST_DEADLINE_SCALE`
//! (`host::test_patience`), so these rows do not evidence 15 s at the default timers; the
//! Docker run D1a does.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex as StdMutex;
use std::time::SystemTime;

use super::formation_tests::{
    declared_rig_opts, group_of, pinned_nodes, pins_until, put_lands, ring, Rig, S1B_K, S1B_P,
};
use super::reopen_tests::fast;
use super::*;
use crate::planner::{write_outage_bound, Failover, OutageClass, PROBE_PUT_TIMEOUT};
use rdb_core::authority::grant::GrantRecord;
use rdb_core::contracts::ids::ReplicaRole;

/// How often a probe puts.
const PROBE_GAP: Duration = Duration::from_millis(200);

/// What the lost host was to a partition, before it was lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seat {
    /// It owned the partition.
    Owner,
    /// It held a copy.
    Copy,
    /// It held nothing of it.
    Untouched,
}

/// One partition's probe, as it ran.
#[derive(Debug, Default, Clone)]
struct Probed {
    /// From the loss to the first refused put, if any was.
    first_err: Option<Duration>,
    /// From the loss to the first put landed after `first_err`.
    first_ok_after_err: Option<Duration>,
    /// UTC milliseconds when that put landed.
    first_ok_after_err_utc_ms: Option<i64>,
    /// The longest time between two landed puts, the first measured from the loss.
    max_gap: Duration,
    /// Whether the last put landed.
    last_ok: bool,
    /// The value of the last put that landed.
    last_ok_i: Option<u64>,
    /// Refused puts, by their error's text up to the first `(` or `{`.
    errs: BTreeMap<String, u64>,
    /// The last refusal's whole text, so a red says why.
    last_err: Option<String>,
}

impl Probed {
    /// How long the partition took no write: none when nothing was refused.
    fn outage(&self) -> Option<Duration> {
        self.first_err
            .map(|_| self.first_ok_after_err.unwrap_or(Duration::MAX))
    }
}

/// `d` in whole milliseconds, for a log field.
fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn utc_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

/// The partition record's `(generation, owner_epoch)` and owner.
fn token_of(rig: &Rig, partition: u32) -> ((u64, u64), NodeId) {
    let record = rig
        .rt
        .block_on(admin::read_partition(&*rig.store, PartitionId(partition)))
        .unwrap_or_else(|e| panic!("partitions/{partition}: {e}"))
        .0;
    ((record.generation.0, record.owner_epoch.0), record.owner)
}

fn grant_of(rig: &Rig, key: ControlKey) -> Option<GrantRecord> {
    rig.rt
        .block_on(rig.store.get(config_core::GetRequest {
            key: Bytes::from(key.encode()),
        }))
        .expect("the store answers")
        .record
        .map(|found| GrantRecord::decode(&found.value).expect("a grant record"))
}

/// The five-node rig with the planner on every node, ready, the planner's grant held.
fn failover_rig(timeouts: Timeouts, failover: Option<Failover>) -> Rig {
    let rig = declared_rig_opts(S1B_K, S1B_P, true, timeouts, failover);
    rig.wait_ready();
    if failover.is_some() {
        let held = Instant::now() + crate::host::test_patience(Duration::from_secs(20));
        while grant_of(&rig, ControlKey::PlannerGrant).is_none() {
            assert!(Instant::now() < held, "no planner took planner/grant");
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    rig
}

/// The rig's timeouts: the probe's put timeout, the rest the defaults.
fn probe_timeouts() -> Timeouts {
    Timeouts {
        put: PROBE_PUT_TIMEOUT,
        ..Timeouts::default()
    }
}

/// The node to lose (N-6): the `planner/grant` holder when it owns a partition, else the node
/// owning the most.
fn victim(rig: &Rig) -> NodeId {
    let owners: Vec<NodeId> = (1..=S1B_P).map(|p| token_of(rig, p).1).collect();
    let holder = grant_of(rig, ControlKey::PlannerGrant).map(|grant| grant.node);
    let owned = |node: NodeId| owners.iter().filter(|o| **o == node).count();
    let most = (1..=S1B_K)
        .map(NodeId)
        .max_by_key(|node| (owned(*node), std::cmp::Reverse(node.0)))
        .expect("five nodes");
    let chosen = holder.filter(|node| owned(*node) > 0).unwrap_or(most);
    tracing::info!(
        node = chosen.0,
        planner_holder = holder.map(|node| node.0),
        owned = owned(chosen),
        "s80s3_victim"
    );
    chosen
}

/// How the host is lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Loss {
    /// Its process ends: runtime dropped, sockets closed (FIN), as `docker kill` does.
    Kill,
    /// It goes silent with its sockets open: links mute, cut from rEtcd, as a pause does.
    Silent,
}

/// What [`lose_one`] measured.
struct Lost {
    rig: Rig,
    dead: NodeId,
    seats: BTreeMap<u32, Seat>,
    groups: BTreeMap<u32, AffinityId>,
    probed: BTreeMap<u32, Probed>,
    tokens: BTreeMap<u32, ((u64, u64), NodeId)>,
    /// Which class a copy seat is measured as.
    copy_class: OutageClass,
}

/// A node other than the dead one.
fn survivor(lost: &Lost) -> NodeId {
    (1..=S1B_K)
        .map(NodeId)
        .find(|node| *node != lost.dead)
        .expect("a survivor")
}

/// Five nodes, a pre-loss value in every partition, then lose [`victim`] by `loss` and probe
/// every partition until each class's expected bound has passed and every partition's last
/// put landed, or until the scaled bound of the slowest class present runs out.
fn lose_one(loss: Loss, timeouts: Timeouts, failover: Option<Failover>) -> Lost {
    let rig = failover_rig(timeouts, failover);
    let dead = victim(&rig);
    lose(rig, dead, loss, timeouts, failover)
}

/// [`lose_one`] on a rig already running: a pre-loss value in every partition, then lose `dead`
/// by `loss` and probe every partition as [`lose_one`] says.
fn lose(
    mut rig: Rig,
    dead: NodeId,
    loss: Loss,
    timeouts: Timeouts,
    failover: Option<Failover>,
) -> Lost {
    let prober = (1..=S1B_K)
        .map(NodeId)
        .find(|node| *node != dead)
        .expect("a survivor");
    let groups: BTreeMap<u32, AffinityId> = (1..=S1B_P)
        .map(|p| (p, group_of(rig.of(prober), p)))
        .collect();
    for (p, group) in &groups {
        put_lands(rig.of(prober), *group, b"s3-pre", p.to_string().as_bytes());
    }
    for p in 1..=S1B_P {
        let placed: BTreeSet<u32> = ring(S1B_K, p).copies.into_iter().collect();
        pins_until(
            &rig,
            p,
            Duration::from_secs(10),
            "the ring's pins",
            |pins| *pins == placed,
        );
    }
    let tokens: BTreeMap<u32, ((u64, u64), NodeId)> =
        (1..=S1B_P).map(|p| (p, token_of(&rig, p))).collect();
    let seats: BTreeMap<u32, Seat> = (1..=S1B_P)
        .map(|p| {
            let seat = if tokens[&p].1 == dead {
                Seat::Owner
            } else if pinned_nodes(&rig, p).contains(&dead.0) {
                Seat::Copy
            } else {
                Seat::Untouched
            };
            (p, seat)
        })
        .collect();
    let copy_class = match loss {
        Loss::Kill => OutageClass::SecondaryKill,
        Loss::Silent => OutageClass::SecondarySilent,
    };
    let budgets = fast();
    let bound = write_outage_bound(
        &budgets,
        &timeouts,
        &failover.unwrap_or_default(),
        timeouts.put,
    );
    let expected = |seat: Seat| match seat {
        Seat::Owner => bound.owner,
        Seat::Copy => bound.of(copy_class),
        Seat::Untouched => Duration::ZERO,
    };
    // What each seat fails above: the scaled expected term, except a silent copy, which fails
    // only above the owner's loose 25 s ceiling, unscaled, and is reported against 17 s (B-1 (a)).
    let (silent_stated, silent_ceiling) = bound.silent();
    let limit = |seat: Seat| match (seat, copy_class) {
        (Seat::Copy, OutageClass::SecondarySilent) => silent_ceiling,
        _ => crate::host::test_patience(expected(seat)),
    };
    let slowest = seats.values().map(|s| limit(*s)).max().unwrap_or_default();
    let cap = slowest + timeouts.put;
    let expiry_before = grant_of(&rig, ControlKey::Grant(dead))
        .expect("the victim's grant")
        .expiry_utc_ms;
    tracing::info!(
        dead = dead.0,
        ?loss,
        owner_ms = ms(bound.owner),
        copy_ms = ms(bound.of(copy_class)),
        cap_ms = ms(cap),
        expiry_before,
        "s80s3_loss"
    );

    let t0 = Instant::now();
    match loss {
        Loss::Kill => rig.stop(dead),
        Loss::Silent => rig.faults_of(dead).apply("mute\ncut-control\n"),
    }
    let probed: BTreeMap<u32, StdMutex<Probed>> = (1..=S1B_P)
        .map(|p| (p, StdMutex::new(Probed::default())))
        .collect();
    let done = AtomicBool::new(false);
    let db = rig.of(prober);
    std::thread::scope(|scope| {
        for (p, group) in &groups {
            let slot = &probed[p];
            let done = &done;
            scope.spawn(move || {
                let mut last_landed = Duration::ZERO;
                let mut i = 0_u64;
                while !done.load(Ordering::Relaxed) {
                    i += 1;
                    let put = db.put_in(*group, b"s3-run", i.to_string().as_bytes(), None);
                    let at = t0.elapsed();
                    let mut probe = slot.lock().expect("a probe");
                    match put {
                        Ok(_) => {
                            probe.max_gap = probe.max_gap.max(at - last_landed);
                            last_landed = at;
                            probe.last_ok = true;
                            probe.last_ok_i = Some(i);
                            if probe.first_err.is_some() && probe.first_ok_after_err.is_none() {
                                probe.first_ok_after_err = Some(at);
                                probe.first_ok_after_err_utc_ms = Some(utc_ms());
                            }
                        }
                        Err(refused) => {
                            probe.last_ok = false;
                            probe.first_err.get_or_insert(at);
                            let text = format!("{refused:?}");
                            let kind = text
                                .split(['(', '{', ' '])
                                .next()
                                .unwrap_or_default()
                                .to_owned();
                            *probe.errs.entry(kind).or_default() += 1;
                            probe.last_err = Some(text);
                        }
                    }
                    drop(probe);
                    std::thread::sleep(PROBE_GAP);
                }
            });
        }
        loop {
            std::thread::sleep(Duration::from_millis(100));
            let elapsed = t0.elapsed();
            let settled = seats.iter().all(|(p, seat)| {
                let probe = probed[p].lock().expect("a probe");
                elapsed >= expected(*seat) && probe.last_ok
            });
            if settled || elapsed >= cap {
                break;
            }
        }
        done.store(true, Ordering::Relaxed);
    });
    let probed: BTreeMap<u32, Probed> = probed
        .into_iter()
        .map(|(p, slot)| (p, slot.into_inner().expect("a probe")))
        .collect();
    for (p, probe) in &probed {
        tracing::info!(
            partition = p,
            seat = ?seats[p],
            first_err_ms = probe.first_err.map(ms),
            first_ok_after_err_ms = probe.first_ok_after_err.map(ms),
            max_gap_ms = ms(probe.max_gap),
            last_ok_i = probe.last_ok_i,
            errs = ?probe.errs,
            last_err = probe.last_err.as_deref(),
            "s80s3_probe"
        );
    }
    for (seat, class) in [
        (Seat::Owner, OutageClass::Owner.word()),
        (Seat::Copy, copy_class.word()),
        (Seat::Untouched, "untouched"),
    ] {
        let mut times: Vec<u64> = seats
            .iter()
            .filter(|(_, s)| **s == seat)
            .map(|(p, _)| probed[p].outage().map_or(0, ms))
            .collect();
        times.sort_unstable();
        tracing::info!(
            class,
            partitions = times.len(),
            max_ms = times.last().copied(),
            p50_ms = times.get(times.len() / 2).copied(),
            bound_ms = ms(expected(seat)),
            limit_ms = ms(limit(seat)),
            stated_ms = (seat == Seat::Copy && copy_class == OutageClass::SecondarySilent)
                .then(|| ms(silent_stated)),
            "s80s3_time_to_writable_ms"
        );
    }

    // Every partition takes a put, each class within its scaled bound.
    for (p, seat) in &seats {
        let probe = &probed[p];
        assert!(
            probe.last_ok,
            "partition {p} ({seat:?}) takes no put: {probe:?}"
        );
        if *seat == Seat::Untouched {
            continue;
        }
        if let Some(outage) = probe.outage() {
            assert!(
                outage <= limit(*seat),
                "partition {p} ({seat:?}) was unwritable for {outage:?}, over {:?}: {probe:?}",
                limit(*seat)
            );
        }
    }
    // The untouched class sees no gap over the probe's put timeout.
    for (p, _) in seats.iter().filter(|(_, s)| **s == Seat::Untouched) {
        let probe = &probed[p];
        assert!(
            probe.max_gap <= crate::host::test_patience(timeouts.put),
            "untouched partition {p}: {probe:?}"
        );
    }
    // The owner class is refused until the dead grant is proven past (S-8).
    let floor = expiry_before
        + i64::try_from(budgets.clock_error_millis + budgets.dispatch_margin_millis)
            .expect("small");
    for (p, _) in seats.iter().filter(|(_, s)| **s == Seat::Owner) {
        let probe = &probed[p];
        let landed = probe
            .first_ok_after_err_utc_ms
            .unwrap_or_else(|| panic!("partition {p} was never refused: {probe:?}"));
        assert!(
            landed >= floor,
            "partition {p} took a write at {landed}, before the dead grant ran out at {floor}"
        );
    }
    Lost {
        rig,
        dead,
        seats,
        groups,
        probed,
        tokens,
        copy_class,
    }
}

/// After [`lose_one`]: each partition the dead node owned has a higher token and a surviving
/// owner, every other keeps its owner; `grants/{dead}` is frozen; each copy seat was dropped
/// by the clock its loss names.
fn after_loss(lost: &Lost, test: &str) {
    let rig = &lost.rig;
    for (p, seat) in &lost.seats {
        let (token, owner) = token_of(rig, *p);
        let (token_before, owner_before) = lost.tokens[p];
        if *seat == Seat::Owner {
            assert!(
                token > token_before,
                "partition {p}: {token_before:?} -> {token:?}"
            );
            assert_ne!(owner, lost.dead, "partition {p}");
        } else {
            assert_eq!(owner, owner_before, "partition {p} kept its owner");
        }
    }
    let frozen = grant_of(rig, ControlKey::Grant(lost.dead)).expect("the dead node's grant");
    assert!(
        frozen.frozen,
        "grants/{} is frozen: {frozen:?}",
        lost.dead.0
    );
    let clock = match lost.copy_class {
        OutageClass::SecondarySilent => "paused",
        _ => "unreachable",
    };
    let lines = config_testkit::logs::lines_for_current_test(module_path!(), test);
    for (p, _) in lost.seats.iter().filter(|(_, s)| **s == Seat::Copy) {
        let drops: Vec<&serde_json::Value> = lines
            .iter()
            .filter(|line| {
                line["@m"] == "membership_drop_planned"
                    && line["partition"] == *p
                    && line["dropped"] == lost.dead.0
            })
            .collect();
        assert!(
            !drops.is_empty() && drops.iter().all(|line| line["clock"] == clock),
            "partition {p}: the dead copy is dropped by the {clock} clock: {drops:?}"
        );
    }
}

/// Through `reader`, every partition's pre-loss value reads back, and its probe value is at
/// least the last one that landed (AC2: nothing acknowledged is lost).
fn reads_back(lost: &Lost, reader: NodeId) {
    let db = lost.rig.of(reader);
    for (p, group) in &lost.groups {
        let pre = db.get_in(*group, b"s3-pre").expect("a get");
        assert_eq!(
            pre.value.as_ref().map(|(_, v)| v.to_vec()),
            Some(p.to_string().into_bytes()),
            "partition {p}'s pre-loss value"
        );
        let run = db.get_in(*group, b"s3-run").expect("a get");
        let read: u64 = run
            .value
            .as_ref()
            .and_then(|(_, v)| std::str::from_utf8(v).ok()?.parse().ok())
            .unwrap_or(0);
        let landed = lost.probed[p].last_ok_i.unwrap_or(0);
        assert!(
            read >= landed,
            "partition {p}: read {read}, landed {landed}"
        );
    }
}

/// #80 S3 R1 (AC1-AC3): five declared nodes, sixteen partitions, the planner on every node.
/// The `planner/grant` holder, which owns partitions, has its process ended (sockets closed, as
/// `docker kill`; N-6 logs `s80s3_victim`). Every partition takes a put again: the owner class
/// within `bound.owner` and not before the dead grant's expiry plus epsilon and delta; the kill
/// copy class within `bound.secondary_kill`, its copy dropped by the unreachable clock; the
/// untouched class with no gap over the put timeout; scaled by `RETCD_TEST_DEADLINE_SCALE`.
/// Moved partitions have a higher token, the others their owner; `grants/{dead}` is frozen;
/// nothing acknowledged is lost. Integration (~20-40 s).
/// Cut (a): `failover: None`; the owner seats take no put, red, observed 2026-11-20.
/// Cut (b): the host's unreachable clock at `u64::MAX` (`host::spawn`'s
/// `unreachable_drop_after_millis`); the copies are dropped by the paused clock instead, still
/// inside the scaled bound (16.4 s), so the drop-clock assertion is what goes red, observed
/// 2026-11-20.
#[config_log::retcd_test]
fn s80s3_killing_one_of_five_declared_nodes_keeps_every_partition_writable() {
    let lost = lose_one(Loss::Kill, probe_timeouts(), Some(Failover::default()));
    after_loss(
        &lost,
        "s80s3_killing_one_of_five_declared_nodes_keeps_every_partition_writable",
    );
    reads_back(&lost, survivor(&lost));
}

/// #80 S3 R1s (critic B-1): as R1, but the lost node goes silent with its sockets open: its
/// links mute and it is cut from rEtcd (the fault file's `mute` and `cut-control`, as a pause
/// or a power loss). The copy class is dropped by the paused clock, logged against the stated
/// 17 s, and fails only above the owner's 25 s ceiling, unscaled (`OutageBound::silent`, B-1
/// ruling (a)). Integration (~20-50 s).
/// Cut (a): `failover: None`; the owner seats take no put, red, observed 2026-11-20.
/// Cut (b): the host's paused clock at `u64::MAX` (`Host`'s `peer_drop_after_millis`); the
/// silent copy is never dropped and its seats stay `PROTECTION_PAUSED` past the 25 s ceiling,
/// red, observed 2026-11-20.
/// Cut (c): `Db::call`'s timeout arm does not re-read the owner; the prober's route stays on the
/// silent node and the owner seats take no put, red, observed 2026-11-20. This row found that
/// defect: before the fix it was red the same way.
#[config_log::retcd_test]
fn s80s3_a_silently_stopped_node_of_five_keeps_every_partition_writable() {
    let lost = lose_one(Loss::Silent, probe_timeouts(), Some(Failover::default()));
    after_loss(
        &lost,
        "s80s3_a_silently_stopped_node_of_five_keeps_every_partition_writable",
    );
    reads_back(&lost, survivor(&lost));
}

/// The roles `partitions/{partition}` pins, by node.
fn pinned_roles(rig: &Rig, partition: u32) -> BTreeMap<u32, ReplicaRole> {
    let read = rig
        .rt
        .block_on(rig.store.get(config_core::GetRequest {
            key: Bytes::from(ControlKey::Partition(PartitionId(partition)).encode()),
        }))
        .expect("the store answers");
    read.record
        .and_then(|found| RecordMember::decode_all(&found.value))
        .map(|members| members.iter().map(|m| (m.node.0, m.role)).collect())
        .unwrap_or_default()
}

/// #80 S3 R2 (AC4, risk A2): after R1's kill, the dead node restarts on its own folder. Every
/// partition whose ring seat named it pins it again as a regular copy, three regular copies
/// each, with no operator action and one restart; it owns no partition (ownership never fails
/// back); the pre-loss values and the probe's last landed value still read back, and a put lands
/// in every partition. Integration (~40-70 s).
/// Green on its first run (2026-11-20): the existing restart re-plan (`Host::peer_booted`, then
/// `rejoin_if_stale`) re-adds the node, about 0.8 s after it is ready; design risk A2 did not
/// occur. Cut: `peer_booted` checks no partition; partition 1 stays pinned to nodes 2 and 3
/// only, red, observed 2026-11-20.
#[config_log::retcd_test]
fn s80s3_a_killed_node_restarted_on_its_folder_is_pinned_again_everywhere() {
    const TEST: &str = "s80s3_a_killed_node_restarted_on_its_folder_is_pinned_again_everywhere";
    let mut lost = lose_one(Loss::Kill, probe_timeouts(), Some(Failover::default()));
    after_loss(&lost, TEST);
    let dead = lost.dead;
    lost.rig.restart(dead, probe_timeouts());
    lost.rig
        .of(dead)
        .wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .unwrap_or_else(|e| panic!("node {} is ready again: {e}", dead.0));
    let rejoined = Instant::now();
    for p in (1..=S1B_P).filter(|p| ring(S1B_K, *p).copies.contains(&dead.0)) {
        let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(60));
        loop {
            let roles = pinned_roles(&lost.rig, p);
            let regular = roles
                .values()
                .filter(|role| **role != ReplicaRole::Shadow)
                .count();
            if roles
                .get(&dead.0)
                .is_some_and(|role| *role != ReplicaRole::Shadow)
                && regular == 3
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "partition {p} never pins node {} regular with three regular copies: {roles:?}",
                dead.0
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    tracing::info!(
        node = dead.0,
        rejoin_ms = ms(rejoined.elapsed()),
        "s80s3_rejoined"
    );
    for p in 1..=S1B_P {
        assert_ne!(
            token_of(&lost.rig, p).1,
            dead,
            "partition {p} did not fail back"
        );
    }
    reads_back(&lost, dead);
    for (p, group) in &lost.groups {
        put_lands(
            lost.rig.of(dead),
            *group,
            b"s3-after",
            p.to_string().as_bytes(),
        );
    }
}

/// Puts through `db` into `group` until one lands or `patience` runs out: the put, or the last
/// refusal.
fn put_within(db: &Db, group: AffinityId, patience: Duration) -> Result<PutOk, String> {
    let deadline = Instant::now() + patience;
    loop {
        match db.put_in(group, b"s3-two", b"1", None) {
            Ok(put) => return Ok(put),
            Err(refused) if Instant::now() >= deadline => return Err(format!("{refused:?}")),
            Err(_) => std::thread::sleep(PROBE_GAP),
        }
    }
}

/// #80 S3 R5 (AC6, #81 Q1 on the ring): five declared nodes, sixteen partitions, the planner on
/// every node; two ring-adjacent nodes, 2 and 3, have their processes ended. A partition whose
/// dead owner's only other copies are both down has one answering copy, so the planner refuses
/// it (`planner_no_eligible_copy`, `lone_survivor`) and writes nothing to its record: the same
/// owner and token, and no put lands. A partition whose dead owner has two answering copies is
/// promoted and takes a put. A partition whose owner lives and has its two copies down takes
/// no put either (it pauses, then is read-only: no copy can acknowledge) and keeps its owner;
/// its owner's own drop re-plan may raise its token. Every other partition takes a put, and the
/// writable ones read their pre-loss value back. Integration (~30-40 s).
/// Two nodes down is beyond S3's promise (one host killed or hung), so the row stops there.
/// Two restart findings are open (lead ruling, 2026-11-20).
/// (a) #168: restarting node 3 brings nothing back. The promote of node 3's partitions
/// froze `grants/3`, and a frozen node is never a promote target (`PromoteError::TargetFrozen`,
/// #61 critic Y3).
/// (b) #169: restarting node 2, the refused partitions' owner, with node 3 still down, it
/// never recovers them (`wait_ready` times out with "recovery phase is partition 2: none"); it
/// does once node 3 is back too.
/// Cut: `choose_target` needs one candidate, not two; red, observed 2026-11-20.
#[config_log::retcd_test]
fn s80s3_two_of_a_partitions_copies_down_is_not_promoted_others_are() {
    const TEST: &str = "s80s3_two_of_a_partitions_copies_down_is_not_promoted_others_are";
    let mut rig = failover_rig(probe_timeouts(), Some(Failover::default()));
    let down = [2_u32, 3];
    let via = NodeId(4);
    let groups: BTreeMap<u32, AffinityId> =
        (1..=S1B_P).map(|p| (p, group_of(rig.of(via), p))).collect();
    for (p, group) in &groups {
        put_lands(rig.of(via), *group, b"s3-pre", p.to_string().as_bytes());
    }
    for p in 1..=S1B_P {
        let placed: BTreeSet<u32> = ring(S1B_K, p).copies.into_iter().collect();
        pins_until(
            &rig,
            p,
            Duration::from_secs(10),
            "the ring's pins",
            |pins| *pins == placed,
        );
    }
    let tokens: BTreeMap<u32, ((u64, u64), NodeId)> =
        (1..=S1B_P).map(|p| (p, token_of(&rig, p))).collect();
    let both_down = |p: u32| {
        let pins = pinned_nodes(&rig, p);
        down.iter().all(|node| pins.contains(node))
    };
    let owner_down = |p: u32| down.contains(&tokens[&p].1 .0);
    let lone: Vec<u32> = (1..=S1B_P)
        .filter(|p| owner_down(*p) && both_down(*p))
        .collect();
    let promoted: Vec<u32> = (1..=S1B_P)
        .filter(|p| owner_down(*p) && !both_down(*p))
        .collect();
    let owner_alone: Vec<u32> = (1..=S1B_P)
        .filter(|p| !owner_down(*p) && both_down(*p))
        .collect();
    let rest: Vec<u32> = (1..=S1B_P)
        .filter(|p| !lone.contains(p) && !promoted.contains(p) && !owner_alone.contains(p))
        .collect();
    tracing::info!(
        ?lone,
        ?promoted,
        ?owner_alone,
        ?rest,
        "s80s3_two_down_classes"
    );
    assert!(
        !lone.is_empty() && !promoted.is_empty(),
        "the ring puts partitions in both"
    );

    let t0 = Instant::now();
    for node in down {
        rig.stop(NodeId(node));
    }
    let patience = crate::host::test_patience(Duration::from_secs(30));
    let db = rig.of(via);
    for p in promoted.iter().chain(&rest) {
        let put = put_within(db, groups[p], patience);
        tracing::info!(
            partition = p,
            at_ms = ms(t0.elapsed()),
            ok = put.is_ok(),
            "s80s3_two_down_put"
        );
        put.unwrap_or_else(|e| panic!("partition {p} takes a put with two nodes down: {e}"));
    }
    for p in &owner_alone {
        let put = put_within(db, groups[p], Duration::from_secs(5));
        tracing::info!(
            partition = p,
            at_ms = ms(t0.elapsed()),
            outcome = ?put.as_ref().map(|_| ()),
            pins = ?pinned_roles(&rig, *p),
            "s80s3_two_down_owner_alone"
        );
        assert!(
            put.is_err(),
            "partition {p}: no copy can acknowledge, yet a put landed"
        );
        // Its owner's own drop re-plan may raise the token; the owner stays.
        assert_eq!(
            token_of(&rig, *p).1,
            tokens[p].1,
            "partition {p}: not failed over"
        );
    }
    for p in &lone {
        let deadline = Instant::now() + patience;
        loop {
            let lines = config_testkit::logs::lines_for_current_test(module_path!(), TEST);
            let said = lines.iter().any(|line| {
                line["@m"] == "planner_no_eligible_copy"
                    && line["partition"] == *p
                    && line["reason"] == "lone_survivor"
            });
            if said {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "partition {p}: no planner_no_eligible_copy lone_survivor"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        assert_eq!(
            token_of(&rig, *p),
            tokens[p],
            "partition {p}: nothing written"
        );
        let put = db.put_in(groups[p], b"s3-two", b"1", None);
        assert!(put.is_err(), "partition {p} takes no put: {put:?}");
    }
    let promotes: Vec<serde_json::Value> =
        config_testkit::logs::lines_for_current_test(module_path!(), TEST)
            .into_iter()
            .filter(|line| line["@m"] == "planner_promoted")
            .collect();
    for p in &lone {
        assert!(
            !promotes.iter().any(|line| line["partition"] == *p),
            "partition {p} was not promoted: {promotes:?}"
        );
    }
    for p in &promoted {
        let (token, owner) = token_of(&rig, *p);
        assert!(
            token > tokens[p].0 && !down.contains(&owner.0),
            "partition {p}: {owner:?}"
        );
    }

    for p in promoted.iter().chain(&rest) {
        let group = &groups[p];
        let pre = db.get_in(*group, b"s3-pre").expect("a get");
        assert_eq!(
            pre.value.as_ref().map(|(_, v)| v.to_vec()),
            Some(p.to_string().into_bytes()),
            "partition {p}'s pre-loss value"
        );
    }
}

/// #80 S3 R3 (AC5): R1's kill, then R2's restart of the killed node V on its folder, until every
/// partition whose ring seat names V pins it regular again with three regular copies and V's
/// frozen grant is cleared (#81 X2: `planner_frozen_grant_cleared`). Then a second kill: M,
/// the owner now of the lowest partition V owned before, whose copies are V and one other.
/// Every partition takes a put again within the bound, as in R1: M's partitions move (that
/// one with V a candidate beside the other copy), M's copy seats are dropped by the unreachable
/// clock, the rest see no gap; nothing acknowledged is lost. Integration (~35-60 s).
/// Cut: `Planner::sweep` skips `clear_frozen`; V's grant stays frozen after its restart, so V
/// is never a candidate again ("grants/1 is still frozen after its restart"), red, observed
/// 2026-11-20.
#[config_log::retcd_test]
fn s80s3_after_a_kill_and_restart_a_second_kill_keeps_every_partition_writable() {
    const TEST: &str =
        "s80s3_after_a_kill_and_restart_a_second_kill_keeps_every_partition_writable";
    let mut lost = lose_one(Loss::Kill, probe_timeouts(), Some(Failover::default()));
    after_loss(&lost, TEST);
    let first = lost.dead;
    let moved: Vec<u32> = lost
        .seats
        .iter()
        .filter(|(_, seat)| **seat == Seat::Owner)
        .map(|(p, _)| *p)
        .collect();
    lost.rig.restart(first, probe_timeouts());
    lost.rig
        .of(first)
        .wait_ready(crate::host::test_patience(Duration::from_secs(30)))
        .unwrap_or_else(|e| panic!("node {} is ready again: {e}", first.0));
    for p in (1..=S1B_P).filter(|p| ring(S1B_K, *p).copies.contains(&first.0)) {
        let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(60));
        loop {
            let roles = pinned_roles(&lost.rig, p);
            let regular = roles
                .values()
                .filter(|role| **role != ReplicaRole::Shadow)
                .count();
            if roles
                .get(&first.0)
                .is_some_and(|role| *role != ReplicaRole::Shadow)
                && regular == 3
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "partition {p} never pins node {} regular with three regular copies: {roles:?}",
                first.0
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    // X2 lets the old boot's frozen grant go once the node owns nothing; until then it is no
    // promote candidate, so the second kill is measured from there.
    let deadline = Instant::now() + crate::host::test_patience(Duration::from_secs(30));
    while grant_of(&lost.rig, ControlKey::Grant(first)).is_some_and(|grant| grant.frozen) {
        assert!(
            Instant::now() < deadline,
            "grants/{} is still frozen after its restart",
            first.0
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let lowest = *moved.first().expect("the first kill moved a partition");
    let second = token_of(&lost.rig, lowest).1;
    assert!(
        second != first && pinned_nodes(&lost.rig, lowest).contains(&first.0),
        "partition {lowest}: owner {second:?}, node {} a copy",
        first.0
    );
    tracing::info!(
        first = first.0,
        second = second.0,
        partition = lowest,
        "s80s3_second_kill"
    );
    let lost = lose(
        lost.rig,
        second,
        Loss::Kill,
        probe_timeouts(),
        Some(Failover::default()),
    );
    after_loss(&lost, TEST);
    reads_back(&lost, first);
    // The restarted node was a candidate for that partition (X2), not left out as frozen.
    let decided: Vec<serde_json::Value> =
        config_testkit::logs::lines_for_current_test(module_path!(), TEST)
            .into_iter()
            .filter(|line| {
                line["@m"] == "planner_promote_decision"
                    && line["partition"] == lowest
                    && line["dead"] == second.0
            })
            .collect();
    assert!(
        decided.iter().any(|line| line["answering"]
            .as_str()
            .is_some_and(|nodes| nodes.contains(&first.0.to_string()))),
        "partition {lowest}: node {} answers as a candidate: {decided:?}",
        first.0
    );
    let (_, owner) = token_of(&lost.rig, lowest);
    tracing::info!(
        partition = lowest,
        owner = owner.0,
        "s80s3_second_kill_moved"
    );
}
