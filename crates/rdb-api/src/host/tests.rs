//! Host rows. Moved out of `host.rs` (M10 S2 event 0); the catch-up rows are in
//! `catchup_tests.rs` and the harness in `trio.rs`.

use super::trio::{current_thread, host_over, logged, write_chain, Trio};
use super::*;
use rdb_core::contracts::errors::RetryRule;
use rdb_core::contracts::ids::{ClientId, ReceivedSeq, RequestId, TenantId};
use rdb_core::contracts::txn::{Durability, Outcome};
use rdb_value::delta::Op;
use rdb_value::path::Path;
use rdb_value::value::{Int, Map, MapKey};

/// This test's `replication_copy_not_advancing` lines.
macro_rules! not_advancing {
    ($method:expr) => {
        logged!($method, "replication_copy_not_advancing")
    };
}

/// M9 S2b, critic M2 (guard 4): after `Db::restart_node`, a `grants/1` read the previous
/// process asked for can still land in the new process's mailbox, since `abort` reaches a
/// control call only at an await. An unheld A1 acts on any read of its own grant: on
/// `Absent` it re-arms `AcquireDue`, and the acquire that follows commits a grant for the
/// new boot, which has no R1 instance to serve it. So the host drops a completion stamped
/// with another boot. The second half is the control: the same read stamped with this
/// process's boot does arm the acquire, so the first half's quiet is the boot check's.
/// A control fault is dropped the same way (review F-006): the previous process's failed
/// call must not fault the new one, and the same fault from this boot does.
/// Unit (~300 ms): one host, no thread; a `MemStore` shows whether a grant was written.
#[test]
fn a_grant_read_from_the_previous_boot_never_starts_an_acquire() {
    use config_core::{ConfigStore, GetRequest};
    use rdb_core::contracts::control::{ControlKey, ReadOutcome};
    use rdb_core::contracts::ids::{ControlRequestId, Revision};

    let dir = config_testkit::fs::temp_dir();
    let engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let links = Links::new();
    let (tx, _completions) = mpsc::channel();
    links.register(NodeId(1), tx, crate::host::BOOT);
    let store = Arc::new(config_testkit::MemStore::new());
    let control = ControlAdapter::new(
        Arc::clone(&store) as Arc<dyn ConfigStore>,
        rt.handle().clone(),
        Arc::clone(&links),
    );
    let mut host = Host::new(NodeId(1), engine, links, control, HostClock::start());
    host.boot = BootId(2);
    // The retry an acted-on read arms is due one renewal later: 20 ms here, not 500.
    host.budgets.renew_millis = 20;
    let read_from = |boot| Msg::Control {
        partition: PartitionId(1),
        correlation: CorrelationId(9),
        boot,
        event: ControlEvent::Value {
            request: ControlRequestId(1),
            key: ControlKey::Grant(NodeId(1)),
            outcome: ReadOutcome::Absent { as_of: Revision(5) },
        },
    };
    let fault_from = |boot| Msg::Fault {
        boot,
        kind: "control_reload_failed",
        detail: "grants/: the store could not list it".to_owned(),
    };
    let grant = || {
        rt.block_on(store.get(GetRequest {
            key: Bytes::from(ControlKey::Grant(NodeId(1)).encode()),
        }))
        .expect("read grants/1")
        .record
    };

    host.handle(read_from(BOOT));
    // Fifteen retry intervals: an acted-on read has committed its grant long before.
    let until = Instant::now() + Duration::from_millis(300);
    while Instant::now() < until {
        host.run_due();
        host.drain();
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(host.fault.is_none(), "{:?}", host.fault);
    assert_eq!(
        grant(),
        None,
        "a read the boot-1 process asked for made the boot-2 process acquire grants/1"
    );
    assert!(
        host.timers.is_empty(),
        "the stale read armed {:?}",
        host.timers
    );
    assert!(host.authority.view().state.is_unheld());
    host.handle(fault_from(BOOT));
    assert!(
        host.fault.is_none(),
        "a control fault from the boot-1 process faulted the boot-2 process: {:?}",
        host.fault
    );

    host.handle(read_from(BootId(2)));
    host.drain();
    assert_eq!(
        host.timers.len(),
        1,
        "the same read from this boot arms the acquire's retry"
    );

    host.handle(fault_from(BootId(2)));
    assert!(
        host.fault.is_some(),
        "the same fault from this boot faults the node"
    );
}

/// Defect D1 (2026-10-07, `rdb_dev` walk 2) and the lead's ruling on it: a put sent before
/// the node adopted a generation was compiled against generation 0 and refused
/// `GENERATION_CHANGED`. Every sent put carries `expected_generation`, so before adoption
/// there is nothing to fence it with: the host refuses it, retryable, and sends nothing.
#[test]
fn a_put_before_any_adoption_is_refused_here_and_never_sent() {
    let dir = config_testkit::fs::temp_dir();
    let engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let links = Links::new();
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let control = ControlAdapter::new(store, rt.handle().clone(), Arc::clone(&links));
    let mut host = Host::new(NodeId(1), engine, links, control, HostClock::start());
    let (reply, answers) = mpsc::channel();
    let identity = RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(1),
        request: RequestId(7),
    };
    host.client(Client {
        partition: PartitionId(1),
        call: ClientCall::Put {
            identity,
            object: Bytes::from_static(b"a"),
            change: byte_doc(b"v"),
            expect: None,
            remaining_millis: 5_000,
        },
        reply,
    })
    .expect("no host fault");

    match answers.try_recv() {
        Ok(Answer::Error { error, request }) => {
            assert_eq!(error, ApiError::not_adopted(NodeId(1), PartitionId(1)));
            assert_eq!(error.name(), "UNAVAILABLE");
            assert!(error.no_mutation, "nothing was sent, so nothing mutated");
            assert!(request.is_none(), "no request exists for `retry` to replay");
        }
        other => panic!("expected a local refusal, got {other:?}"),
    }
    assert!(host.queue.is_empty(), "nothing reached T1");
    assert!(host.pending.is_empty(), "no reply is awaited");
}

/// D2 ruling, rule 3: a sync at seq 0 is answered `Digest::ROOT` by the holder itself,
/// after commit as before it; at seq 1 with no stored record it is still withheld.
#[test]
fn a_post_commit_sync_proves_root_at_seq_zero_and_nothing_without_a_record() {
    let dir = config_testkit::fs::temp_dir();
    let mut engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
    let (partition, generation) = (PartitionId(1), Generation(1));
    // Durable through seq 1, with no history record there.
    engine
        .commit(rdb_core::contracts::storage::Batch {
            id: rdb_core::contracts::ids::BatchId(1),
            partition,
            generation,
            seq: Seq(1),
            writes: Vec::new(),
        })
        .expect("commit seq 1");
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");
    let links = Links::new();
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let control = ControlAdapter::new(store, rt.handle().clone(), Arc::clone(&links));
    let mut host = Host::new(NodeId(2), engine, links, control, HostClock::start());

    let proof = match host.sync(partition, CopyId(1), Seq::ZERO, generation, true) {
        Some(RecoveryEvent::DurableAt(proof)) => proof,
        other => panic!("expected a proof at seq 0, got {other:?}"),
    };
    assert_eq!(
        (proof.copy, proof.seq, proof.digest),
        (CopyId(1), DurableSeq(0), Digest::ROOT)
    );
    assert_eq!(
        host.sync(partition, CopyId(1), Seq(1), generation, true),
        None,
        "no record at seq 1: withheld, never ROOT"
    );
    // Withheld for the missing record, not for a short sync.
    assert_eq!(host.engine.durable(partition, generation), DurableSeq(1));
}

/// A-R192b: the flush duty syncs only while some lineage is applied past its durable mark.
/// Once all are durable through applied, it pushes the same `Flushed` with no fsync: a
/// WAL-flush fault armed then stays armed. The next commit makes the duty sync again, and
/// the fault fires there. Cut (never skip): the fault fires early. Cut (always skip): the
/// first duty leaves seq 1 not durable.
#[cfg(debug_assertions)]
#[test]
fn the_flush_duty_syncs_only_while_a_lineage_is_applied_past_durable() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    let mut host = host_over(&path, &rt);
    let (partition, generation) = (PartitionId(1), Generation(1));
    let commit = |host: &mut Host, seq: u64| {
        host.engine
            .commit(rdb_core::contracts::storage::Batch {
                id: rdb_core::contracts::ids::BatchId(seq),
                partition,
                generation,
                seq: Seq(seq),
                writes: Vec::new(),
            })
            .expect("commit");
    };
    let last = |host: &mut Host| {
        host.queue
            .pop_back()
            .map(|queued| queued.event.kind)
            .expect("the duty pushed an event")
    };
    let durable_through = |seq: u64| {
        EventKind::Storage(StorageEvent::Flushed {
            ticket: FlushTicket(0),
            durable: vec![DurablePrefix {
                partition,
                generation,
                through: DurableSeq(seq),
            }],
        })
    };
    let without_ticket = |kind: EventKind| match kind {
        EventKind::Storage(StorageEvent::Flushed { durable, .. }) => {
            EventKind::Storage(StorageEvent::Flushed {
                ticket: FlushTicket(0),
                durable,
            })
        }
        other => other,
    };

    commit(&mut host, 1);
    host.flush_duty().expect("flush");
    assert_eq!(host.engine.durable(partition, generation), DurableSeq(1));
    assert_eq!(without_ticket(last(&mut host)), durable_through(1));
    // A-R214: that sync wrote the durable mark unsynced, so one more duty syncs it.
    host.flush_duty().expect("flush");
    assert!(host.engine.wal_synced_through_latest());
    assert_eq!(without_ticket(last(&mut host)), durable_through(1));

    host.engine
        .inject_fault(rdb_storage::InjectedFault::WalFlush);
    host.flush_duty().expect("flush");
    assert_eq!(
        host.engine.armed_fault(),
        Some(rdb_storage::InjectedFault::WalFlush),
        "nothing to sync: no fsync"
    );
    assert_eq!(without_ticket(last(&mut host)), durable_through(1));

    commit(&mut host, 2);
    host.flush_duty().expect("flush");
    assert_eq!(host.engine.armed_fault(), None, "seq 2 needed a sync");
    assert!(
        matches!(
            last(&mut host),
            EventKind::Storage(StorageEvent::FlushFailed { .. })
        ),
        "the armed fault failed that sync"
    );
    assert_eq!(host.engine.durable(partition, generation), DurableSeq(1));
}

/// A-R214: a host over a fresh engine whose one lineage is synced and quiet: a WAL-flush
/// fault armed then survives a duty. Every lineage is durable through applied after it.
#[cfg(debug_assertions)]
fn quiet_host_with_wal_flush_armed(path: &std::path::Path, rt: &tokio::runtime::Runtime) -> Host {
    let mut host = host_over(path, rt);
    host.engine
        .commit(rdb_core::contracts::storage::Batch {
            id: rdb_core::contracts::ids::BatchId(1),
            partition: PartitionId(2),
            generation: Generation(1),
            seq: Seq(1),
            writes: Vec::new(),
        })
        .expect("commit");
    host.flush_duty().expect("flush");
    host.flush_duty().expect("flush");
    host.engine
        .inject_fault(rdb_storage::InjectedFault::WalFlush);
    host.flush_duty().expect("flush");
    assert_eq!(
        host.engine.armed_fault(),
        Some(rdb_storage::InjectedFault::WalFlush),
        "a quiet duty does not sync"
    );
    host
}

/// A-R214: with every lineage still durable through applied, the next duty syncs anyway,
/// and the armed fault fails that sync.
#[cfg(debug_assertions)]
fn assert_next_duty_syncs(host: &mut Host, what: &str) {
    let durable_at_applied = host
        .engine
        .lineages()
        .into_iter()
        .all(|(p, g)| host.engine.durable(p, g).0 == host.engine.buffered_applied(p, g).0);
    assert!(durable_at_applied, "{what}: no applied mark moved");
    host.flush_duty().expect("flush");
    assert_eq!(host.engine.armed_fault(), None, "{what}: the duty synced");
    assert!(
        matches!(
            host.queue.pop_back().map(|queued| queued.event.kind),
            Some(EventKind::Storage(StorageEvent::FlushFailed { .. }))
        ),
        "{what}: the armed fault failed that sync"
    );
}

/// A-R214 row 1: the retire duty's dedup delete raises no applied mark, so durable ==
/// applied alone would skip the next duty and leave the delete in the OS cache. The
/// duty syncs it. Cut (durable == applied alone): the fault stays armed, red.
#[cfg(debug_assertions)]
#[test]
fn a_dedup_delete_alone_still_makes_the_flush_duty_sync() {
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let mut host = quiet_host_with_wal_flush_armed(&dir.path().join("node"), &rt);
    host.engine
        .delete_dedup_below(PartitionId(2), Generation(1), &2_u64.to_be_bytes())
        .expect("dedup delete");
    assert_next_duty_syncs(&mut host, "a dedup delete");
}

/// Review-s1 F-003 (D10's cap): a mailbox holding more than `MAIL_BEFORE_DUTIES` letters,
/// then `Stop`, with the flush duty due. `Host::run` takes exactly that many, runs the due
/// duties, and only then reads on to `Stop`, so steady mail cannot starve them. Each letter
/// is a stale-boot `Activated`, which logs one line and changes nothing. Cut: the read's
/// bound `taken < MAIL_BEFORE_DUTIES` lifted; the run reads every letter and `Stop` before
/// any duty, and no flush line is logged, red. Dropping `|| taken == MAIL_BEFORE_DUTIES`
/// alone stays green, and rightly: `taken` counts across passes, so the next pass reads
/// nothing and the loop ends at the same letter. That clause saves one drain. Unit, under
/// a second.
#[config_log::retcd_test]
fn a_full_mailbox_runs_the_due_duties_after_the_cap_before_reading_on() {
    const METHOD: &str = "a_full_mailbox_runs_the_due_duties_after_the_cap_before_reading_on";
    const LETTERS: usize = MAIL_BEFORE_DUTIES + 44;
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let mut host = host_over(&dir.path().join("node"), &rt);
    host.engine
        .commit(rdb_core::contracts::storage::Batch {
            id: rdb_core::contracts::ids::BatchId(1),
            partition: PartitionId(2),
            generation: Generation(1),
            seq: Seq(1),
            writes: Vec::new(),
        })
        .expect("commit");
    host.next_flush = host.clock.now();
    let stale = BootId(host.boot.0 + 1);
    let (tx, rx) = mpsc::channel();
    for _ in 0..LETTERS {
        tx.send(Msg::Activated {
            boot: stale,
            partition: PartitionId(2),
            generation: Generation(1),
        })
        .expect("send");
    }
    tx.send(Msg::Stop).expect("send");
    host.run(&rx);

    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let letters = lines
        .iter()
        .filter(|line| line["@m"] == "control_stale_boot_dropped")
        .count();
    assert_eq!(letters, LETTERS, "every letter was read");
    let flush = lines
        .iter()
        .position(|line| line["@m"] == "host_duty" && line["kind"] == "flush")
        .expect("the flush duty ran before Stop");
    let before = lines[..flush]
        .iter()
        .filter(|line| line["@m"] == "control_stale_boot_dropped")
        .count();
    assert_eq!(
        before, MAIL_BEFORE_DUTIES,
        "the duty ran right after the cap"
    );
}

/// PR #60 review F4: a queued event that faults the node ends its turn. The mailbox holds
/// `MAIL_BEFORE_DUTIES` letters, then `Stop`, with the flush duty due: first an injected
/// timer whose offer faults (`fail_step_view`), then stale-boot letters that change nothing.
/// The read stops at the cap with the timer still queued, so the drain after it faults, and
/// the flush duty after that drain must not run. Cut: the duties run on whatever the
/// drain did, and a `flush` duty line follows the fault, red. Unit, under a second.
#[config_log::retcd_test]
fn a_fault_in_the_drain_after_the_cap_runs_no_duty_in_that_turn() {
    const METHOD: &str = "a_fault_in_the_drain_after_the_cap_runs_no_duty_in_that_turn";
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let mut host = host_over(&dir.path().join("node"), &rt);
    host.engine
        .commit(rdb_core::contracts::storage::Batch {
            id: rdb_core::contracts::ids::BatchId(1),
            partition: PartitionId(2),
            generation: Generation(1),
            seq: Seq(1),
            writes: Vec::new(),
        })
        .expect("commit");
    host.next_flush = host.clock.now();
    host.fail_step_view = true;
    let stale = BootId(host.boot.0 + 1);
    let (tx, rx) = mpsc::channel();
    tx.send(Msg::Inject {
        partition: PartitionId(2),
        correlation: CorrelationId::default(),
        kind: EventKind::Timer(TimerFired {
            id: HEALTH_EVAL_TIMER,
            version: TimerVersion(0),
            scheduled_at: host.clock.now(),
        }),
    })
    .expect("send");
    for _ in 1..MAIL_BEFORE_DUTIES {
        tx.send(Msg::Activated {
            boot: stale,
            partition: PartitionId(2),
            generation: Generation(1),
        })
        .expect("send");
    }
    tx.send(Msg::Stop).expect("send");
    host.run(&rx);

    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let fault = lines
        .iter()
        .position(|line| line["@m"] == "host_fault")
        .expect("the injected timer faulted the node");
    let letters = lines[..fault]
        .iter()
        .filter(|line| line["@m"] == "control_stale_boot_dropped")
        .count();
    assert_eq!(letters, MAIL_BEFORE_DUTIES - 1, "it faulted after the cap");
    let after: Vec<_> = lines[fault + 1..]
        .iter()
        .filter(|line| line["@m"] == "host_duty" || line["@m"] == "host_fault")
        .collect();
    assert!(after.is_empty(), "nothing ran after the fault: {after:?}");
}

/// PR #60 review F4: the first fault is the one the node keeps and reports. A later one is
/// logged, never kept. Cut: `set_fault` overwrites, and the status names the later one, red.
#[test]
fn a_second_fault_keeps_the_first() {
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let mut host = host_over(&dir.path().join("node"), &rt);
    host.set_fault(Fault::from("the first".to_owned()));
    host.set_fault(Fault::from("the second".to_owned()));
    assert_eq!(
        host.status(PartitionId(2)).fault.as_deref(),
        Some("the first"),
        "the first fault is kept"
    );
}

/// PR #60 review F8: the final marks a clean stop writes are a synced write, timed like
/// every other duty, so a slow one logs `host_slow_step`. The host restored its marks and
/// published seq 6 of generation 1, so the stop has marks to write; that write is held past
/// the 250 ms threshold (`step_pause`, inside the write only). The row checks the write ran
/// (`host_duty` `final_marks`, the mark on disk) and was logged slow, after it. Cut: the
/// `slow_step` call removed, and no such line, red. Unit, about 0.3 s.
#[config_log::retcd_test]
fn a_slow_final_marks_write_on_a_clean_stop_is_logged_as_slow() {
    const METHOD: &str = "a_slow_final_marks_write_on_a_clean_stop_is_logged_as_slow";
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    reopened_with_marks(&path, 2, &[]);
    let published = (Generation(1), Seq(6), Tick(HostClock::start().now().0 - 10));
    {
        let mut host = host_over(&path, &rt);
        host.restore_marks(PartitionId(1), Generation(2))
            .expect("restore");
        host.published_at
            .entry(PartitionId(1))
            .or_default()
            .push_back(published);
        host.step_pause = Duration::from_millis(SLOW_STEP_MILLIS + 50);
        let (tx, rx) = mpsc::channel();
        tx.send(Msg::Stop).expect("send stop");
        host.run(&rx);
    }

    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let wrote = lines
        .iter()
        .position(|line| line["@m"] == "host_duty" && line["kind"] == "final_marks")
        .expect("the clean stop wrote its final marks");
    let slow: Vec<_> = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| line["@m"] == "host_slow_step")
        .map(|(at, line)| (at, line["step"].clone(), line["what"].clone()))
        .collect();
    assert_eq!(
        slow.iter()
            .map(|(_, step, what)| (step, what))
            .collect::<Vec<_>>(),
        [(
            &serde_json::json!("duty"),
            &serde_json::json!("final_marks")
        )],
        "the slow final write was logged as slow"
    );
    assert!(slow[0].0 > wrote, "logged once the write had run");
    let marks: Vec<Mark> = RocksEngine::open(&path)
        .expect("open engine")
        .publish_marks(PartitionId(1))
        .expect("read marks")
        .into_iter()
        .map(|mark| (mark.generation, mark.seq, mark.tick))
        .collect();
    assert_eq!(marks, [published], "the final write reached the disk");
}

/// PR #60 review F9: an offer is timed whatever it answers. Here the step view fails
/// (`fail_step_view`), so Transaction's offer of an injected timer ends in a host error,
/// after a pause past the threshold. Cut: only an offer that delivers is timed, and no
/// `Transaction` line, red. Unit, about 0.6 s: Authority's offer is held too.
#[config_log::retcd_test]
fn a_slow_offer_that_fails_is_logged_as_slow() {
    const METHOD: &str = "a_slow_offer_that_fails_is_logged_as_slow";
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let mut host = host_over(&dir.path().join("node"), &rt);
    host.step_pause = Duration::from_millis(SLOW_STEP_MILLIS + 50);
    host.fail_step_view = true;
    host.handle(Msg::Inject {
        partition: PartitionId(2),
        correlation: CorrelationId::default(),
        kind: EventKind::Timer(TimerFired {
            id: HEALTH_EVAL_TIMER,
            version: TimerVersion(0),
            scheduled_at: host.clock.now(),
        }),
    });
    host.drain();
    let fault = host.fault.clone().expect("the offer failed");
    assert!(fault.detail.contains("injected"), "{fault:?}");

    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    let slow: Vec<String> = lines
        .iter()
        .filter(|line| line["@m"] == "host_slow_step" && line["step"] == "module")
        .filter_map(|line| line["what"].as_str().map(str::to_owned))
        .collect();
    assert!(
        slow.iter().any(|what| what.starts_with("Transaction on")),
        "the failed offer was timed: {slow:?}"
    );
}

/// A-R214 row 2: an inherit at base 0 (a landing at cutoff 0) writes the link and the
/// seal, and leaves both lineages applied and durable at 0. The duty syncs it. Cut
/// (durable == applied alone): the fault stays armed, red.
#[cfg(debug_assertions)]
#[test]
fn an_inherit_at_base_zero_still_makes_the_flush_duty_sync() {
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let mut host = quiet_host_with_wal_flush_armed(&dir.path().join("node"), &rt);
    let inherited = host
        .engine
        .inherit(PartitionId(1), Generation(1), Generation(2), Seq::ZERO)
        .expect("inherit");
    assert_eq!(inherited, Inherited::Linked);
    assert_next_duty_syncs(&mut host, "an inherit at base 0");
}

/// Defect D2 (2026-10-07, `rdb_dev --hold 1-2,1-3`, walk A10): with both secondaries
/// unreachable F1 commits `ReadOnly` at cutoff 0, and the partition stayed read-only in
/// silence. Since the D2 kernel step, F1 pins that rebuild at the commit and, at each
/// deadline, names every copy that has not proved the point (`RebuildStalled`). The host
/// turns that into one stall line naming the mode and those copies, which stays as printed
/// while F1 keeps naming them. PC15 and mutant M26: F1 names both copies in one step, and a
/// repeat names no new copy, so across six deadlines the log holds exactly one error line.
///
/// Window kind: this row claims what is reported, not when the window closes, so until F1
/// commits it runs the spec's 2 s times `RETCD_TEST_DEADLINE_SCALE` (A-R237). That window is
/// also the commit's CAS deadline, and on int gate k429 a starved host ran that CAS 3.7-5.1 s
/// late, so the plain spec 2 s blocked F1 with `ControlUnknown`. The product default is 2 s.
#[config_log::retcd_test]
fn a_read_only_commit_at_cutoff_zero_reports_its_unproven_copies_stalled() {
    const METHOD: &str = "a_read_only_commit_at_cutoff_zero_reports_its_unproven_copies_stalled";
    let dir = config_testkit::fs::temp_dir();
    let engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let links = Links::new();
    let (tx, rx) = mpsc::channel();
    links.register(NodeId(1), tx.clone(), crate::host::BOOT);
    links.hold(NodeId(1), NodeId(2));
    links.hold(NodeId(1), NodeId(3));
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let control = ControlAdapter::new(Arc::clone(&store), rt.handle().clone(), Arc::clone(&links));
    let clock = HostClock::start();
    let mut host = Host::new(NodeId(1), engine, links, control, clock);
    // The walk, faster: F1's deadline is the discovery window, shortened once F1 has
    // committed, so the commit's CAS keeps the scaled spec window against a host stall. The
    // never-pinned watch is pushed out of reach, so only F1's own report can produce the line.
    host.budgets.discovery_window_millis = Budgets::SPEC_DEFAULTS.discovery_window_millis
        * u64::from(config_testkit::poll::deadline_scale());
    host.rebuild_pin_wait_millis = 60_000;
    // A-R379: the drop clock too. A stalled host stretched the asks past it, node 1 dropped
    // node 3, and the re-plan began F1's count again (loader run 2026-10-24).
    host.peer_drop_after_millis = 600_000;
    rt.block_on(crate::admin::bootstrap(&store, clock.now(), |msg| {
        tx.send(msg).map_err(|_| NodeStopped(NodeId(1)))
    }))
    .expect("bootstrap");
    let partition = PartitionId(1);
    let step = |host: &mut Host| {
        if host.status(partition).recovered.is_some() {
            host.budgets.discovery_window_millis = test_discovery_window_millis();
        }
        host.run_due();
        host.drain();
        assert_eq!(host.fault, None, "a stall is reported, never a fault");
        assert_not_blocked(
            &host.status(partition),
            host.budgets.discovery_window_millis,
        );
        if let Ok(msg) = rx.recv_timeout(host.wait().min(Duration::from_millis(10))) {
            host.handle(msg);
        }
    };

    // The line lands at F1's first deadline after the commit, two 2 s windows (4.0 s at
    // scale 1): `Trio::PATIENCE`'s margin.
    let patience = test_patience(Duration::from_secs(8));
    let deadline = Instant::now() + patience;
    let stalled = loop {
        step(&mut host);
        if let Some(line) = host.status(partition).stalled {
            break line;
        }
        assert!(
            Instant::now() < deadline,
            "no stall reported within {patience:?}"
        );
    };
    assert!(host.rebuilds[&partition].pinned, "F1 pinned at the commit");
    assert_eq!(
        host.status(partition).recovery.as_deref(),
        Some("Rebuilding")
    );
    for part in [
        "recovery_rebuild_stalled partition=1 gen=1 mode=ReadOnly cutoff=0 required=[0, 1, 2] \
         unproven=[1, 2]",
        "phase=Rebuilding",
    ] {
        assert!(stalled.contains(part), "{part:?} missing from {stalled:?}");
    }

    // Five more of F1's deadlines: it names both copies again, and the line does not move.
    // Each deadline re-asks copy 2 (`rebuild_pinned`), so they are counted from the log,
    // not assumed from elapsed time: the scaled window made a fixed 100 ms less than one.
    let asks_of_copy_2 = || {
        logged!(METHOD, "rebuild_pinned")
            .into_iter()
            .filter(|line| line["copy"] == 2)
            .count()
    };
    let more = asks_of_copy_2() + 5;
    let mut read = Instant::now();
    loop {
        step(&mut host);
        if read.elapsed() >= Duration::from_millis(50) {
            read = Instant::now();
            if asks_of_copy_2() >= more {
                break;
            }
        }
        assert!(
            Instant::now() < deadline + patience,
            "five more deadlines not within {patience:?}"
        );
    }
    assert_eq!(host.status(partition).stalled, Some(stalled));
    let logged = logged!(METHOD, "recovery_rebuild_stalled");
    assert_eq!(logged.len(), 1, "one error line for the stall: {logged:?}");
    assert_eq!(logged[0]["@l"], "Error", "{}", logged[0]);
}

/// Tester gap G1 (2026-10-07, mutant M3): the stall watch must stay quiet for an `Active`
/// commit. Watching one too printed `stalled ... mode=Active phase=Committed` on every
/// healthy start, and no row failed. Three nodes, no holds, driven past the watch's wait.
#[test]
fn an_active_commit_is_never_reported_stalled() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        host.rebuild_pin_wait_millis = 50;
    });
    trio.bootstrap();

    // Until node 1 has landed generation 1, then for 4x the watch's wait.
    let quiet_until = trio.until("generation 1 landed on node 1", |trio| {
        let status = trio.status(0);
        assert_eq!(status.stalled, None, "a healthy start reported a stall");
        (status.recovered == Some(Generation(1))).then(|| {
            assert_eq!(status.recovery.as_deref(), Some("Committed"));
            Instant::now() + Duration::from_millis(200)
        })
    });
    trio.until("the watch's wait passed", |trio| {
        assert_eq!(
            trio.status(0).stalled,
            None,
            "a healthy start reported a stall"
        );
        (Instant::now() >= quiet_until).then_some(())
    });
}

/// A-R102 addendum, item 3, at the host (W10-D1): a resolving activation still names the
/// pinned configuration. Node 1 starts with its links to 2 and 3 held, so F1 commits
/// `ReadOnly` and rebuilds; the control store goes down before the links heal, so the
/// activation CAS answers `Unavailable` and F1 waits in `ActivationUnresolved`. A
/// `SyncWalThrough` asked then is served from the pinned configuration (`rebuild_pinned`),
/// as in `Committed`, `Rebuilding` and `ActivationProposed`. When the store answers again,
/// writes resume with no restart (Walk 10's r24n, which used to stay `Blocked`).
/// Integration (~3 s): bootstrap's discovery window, then the activation's own deadline.
#[config_log::retcd_test]
fn a_resolving_activation_still_serves_syncs_from_the_pinned_configuration() {
    const METHOD: &str = "a_resolving_activation_still_serves_syncs_from_the_pinned_configuration";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    trio.bootstrap();
    trio.until("F1 rebuilding on node 1", |trio| {
        (trio.status(0).recovery.as_deref() == Some("Rebuilding")).then_some(())
    });
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    trio.links.heal_all();
    trio.until("F1 resolving on node 1", |trio| {
        (trio.status(0).recovery.as_deref() == Some("ActivationUnresolved")).then_some(())
    });

    let pinned_asks_of_copy_2 = || {
        logged!(METHOD, "rebuild_pinned")
            .into_iter()
            .filter(|line| line["node"] == 1 && line["copy"] == 2)
            .count()
    };
    let before = pinned_asks_of_copy_2();
    let sync = RecoveryEffect::SyncWalThrough {
        copy: CopyId(2),
        cutoff: Seq::ZERO,
    };
    let site = Site {
        partition: PartitionId(1),
        correlation: CorrelationId(0),
    };
    trio.nodes[0]
        .0
        .recovery_effect(&sync, site)
        .expect("sync effect");
    assert_eq!(
        trio.status(0).recovery.as_deref(),
        Some("ActivationUnresolved"),
        "precondition: still resolving when the sync was asked"
    );
    assert_eq!(
        pinned_asks_of_copy_2(),
        before + 1,
        "a sync asked while resolving is served from the pinned configuration"
    );

    trio.mem.stop_failing();
    trio.ready();
    assert_eq!(trio.status(0).recovery.as_deref(), Some("Committed"));
}

/// Node 1's `recovery_fact` lines for `BlockPromotion`, as `(effect, terminal)`.
fn block_facts(method: &str) -> Vec<(String, Option<bool>)> {
    logged!(method, "recovery_fact")
        .into_iter()
        .filter(|line| line["node"] == 1)
        .filter_map(|line| {
            let effect = line["effect"].as_str()?.to_owned();
            effect
                .starts_with("BlockPromotion")
                .then(|| (effect, line["terminal"].as_bool()))
        })
        .collect()
}

/// BlockPromotion item (lead, 2026-10-23): a block a control-store outage resolves is
/// logged `terminal=false`. The activation row's start: F1 commits `ReadOnly` with its
/// peers held, the store goes down, and the activation CAS answers `Unavailable`. F1 reports
/// `BlockPromotion { reason: ControlUnavailable }` and resolves in `ActivationUnresolved`
/// (A-R102), so the block does not last. Cut: `terminal` from the reason and the binding (a
/// control outage terminal when unbound, as every `Trio` host is) logs `true` here.
/// Integration (~3 s): bootstrap's discovery window, then the activation's own deadline.
#[config_log::retcd_test]
fn m10_a_block_a_store_outage_resolves_is_logged_not_terminal() {
    const METHOD: &str = "m10_a_block_a_store_outage_resolves_is_logged_not_terminal";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    trio.bootstrap();
    trio.until("F1 rebuilding on node 1", |trio| {
        (trio.status(0).recovery.as_deref() == Some("Rebuilding")).then_some(())
    });
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    trio.links.heal_all();
    trio.until("F1 resolving on node 1", |trio| {
        (trio.status(0).recovery.as_deref() == Some("ActivationUnresolved")).then_some(())
    });

    let facts = block_facts(METHOD);
    assert!(!facts.is_empty(), "the outage was reported as a block");
    for (effect, terminal) in &facts {
        assert!(
            effect.starts_with("BlockPromotion { reason: Control"),
            "only the outage blocked: {facts:?}"
        );
        assert_eq!(*terminal, Some(false), "F1 resolves this one: {facts:?}");
    }
    trio.mem.stop_failing();
    trio.ready();
}

/// BlockPromotion item (lead, 2026-10-23): a block that lasts is logged `terminal=true`.
/// Node 1 starts with its peers held, and the control store goes down while F1 collects, so
/// the recovery CAS answers `Unavailable`. Unbound, as every `Trio` host is, F1 stays
/// `Blocked(ControlUnavailable)` until a fresh fence, and no fence comes while the store is
/// down. Cut: `terminal` from the reason alone (a control outage never terminal) logs
/// `false` here. Integration (~2 s): bootstrap's discovery window.
#[config_log::retcd_test]
fn m10_a_block_that_lasts_is_logged_terminal() {
    const METHOD: &str = "m10_a_block_that_lasts_is_logged_terminal";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    trio.bootstrap();
    trio.until("F1 collecting on node 1", |trio| {
        matches!(
            trio.status(0).recovery.as_deref(),
            Some("Fenced" | "Collecting")
        )
        .then_some(())
    });
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    // `Trio::step` fails a row on a block, which is what this row waits for: step by hand.
    let deadline = Instant::now() + test_patience(Trio::PATIENCE);
    let phase = loop {
        for (host, _, rx) in &mut trio.nodes {
            host.run_due();
            host.drain();
            while let Ok(msg) = rx.try_recv() {
                host.handle(msg);
            }
            assert_eq!(host.fault, None, "node {} faulted", host.node.0);
        }
        if let Some(phase) = trio
            .status(0)
            .recovery
            .filter(|phase| phase.starts_with("Blocked("))
        {
            break phase;
        }
        assert!(Instant::now() < deadline, "F1 never blocked on node 1");
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(phase, "Blocked(ControlUnavailable)");

    assert_eq!(
        block_facts(METHOD),
        vec![(
            "BlockPromotion { reason: ControlUnavailable }".to_owned(),
            Some(true)
        )]
    );
    trio.mem.stop_failing();
}

/// Tester gap G2 (2026-10-07, mutant M4): once F1 sends its post-commit sync, the rebuild
/// is pinned and the never-pinned watch must stay quiet. The D2 row's start (`ReadOnly` at
/// cutoff 0), then the `SyncWalThrough` F1 sends at commit, through the host's own effect
/// path. Since the D2 kernel step F1 also names the held copies at its deadline, so a stall
/// line may appear; this row pins only that it is never the never-pinned one.
///
/// Window kind: this row claims what is reported, not when the window closes, so it runs the
/// spec's 2 s times `RETCD_TEST_DEADLINE_SCALE` (A-R237). The window is also the CAS deadline
/// a host stall must not outlast, and on int gate k429 a starved host ran that CAS 3.7-5.1 s
/// late. The product default stays 2 s.
#[config_log::retcd_test]
fn a_rebuild_pinned_by_its_post_commit_sync_is_never_reported_stalled() {
    let dir = config_testkit::fs::temp_dir();
    let engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");
    let links = Links::new();
    let (tx, rx) = mpsc::channel();
    links.register(NodeId(1), tx.clone(), crate::host::BOOT);
    links.hold(NodeId(1), NodeId(2));
    links.hold(NodeId(1), NodeId(3));
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let control = ControlAdapter::new(Arc::clone(&store), rt.handle().clone(), Arc::clone(&links));
    let clock = HostClock::start();
    let mut host = Host::new(NodeId(1), engine, links, control, clock);
    host.budgets.discovery_window_millis = Budgets::SPEC_DEFAULTS.discovery_window_millis
        * u64::from(config_testkit::poll::deadline_scale());
    host.rebuild_pin_wait_millis = 50;
    rt.block_on(crate::admin::bootstrap(&store, clock.now(), |msg| {
        tx.send(msg).map_err(|_| NodeStopped(NodeId(1)))
    }))
    .expect("bootstrap");
    let partition = PartitionId(1);

    let step = |host: &mut Host| {
        host.run_due();
        host.drain();
        assert_eq!(host.fault, None);
        assert_not_blocked(
            &host.status(partition),
            host.budgets.discovery_window_millis,
        );
        if let Some(line) = host.status(partition).stalled {
            assert!(
                line.contains("unproven="),
                "a pinned rebuild's only stall line is F1's own: {line}"
            );
        }
        if let Ok(msg) = rx.recv_timeout(host.wait().min(Duration::from_millis(10))) {
            host.handle(msg);
        }
    };
    let patience = test_patience(Duration::from_secs(5));
    let deadline = Instant::now() + patience;
    while !host.rebuilds.contains_key(&partition) {
        assert!(
            Instant::now() < deadline,
            "no rebuild watch within {patience:?}"
        );
        step(&mut host);
    }
    assert_eq!(
        host.status(partition).recovery.as_deref(),
        Some("Rebuilding")
    );

    // Copy 1's holder is node 2, behind a held link: the ask is buffered, nothing answers.
    let sync = RecoveryEffect::SyncWalThrough {
        copy: CopyId(1),
        cutoff: Seq::ZERO,
    };
    let site = Site {
        partition,
        correlation: CorrelationId(0),
    };
    host.recovery_effect(&sync, site).expect("sync effect");
    assert!(
        host.rebuilds[&partition].pinned,
        "a post-commit sync pins the watch"
    );

    // Past the check: it ran, and it stayed quiet.
    let quiet_until = Instant::now() + Duration::from_millis(200);
    while Instant::now() < quiet_until {
        step(&mut host);
    }
    assert!(
        !host
            .delayed
            .values()
            .any(|item| matches!(item, Delayed::RebuildCheck { .. })),
        "the rebuild check has not run yet"
    );
}

/// OB2 (2026-10-07, `rdb_dev` at dd41368): `nodes` printed every secondary as `gen=0
/// applied=0 durable=0` while the owner's puts were `BufferedOnTwo`, because the view read
/// the store at the adopted generation and a secondary adopts none. A secondary now reports
/// what its receiver took, and a node holding nothing says so instead of printing zeros.
/// Node 2's flush is held, so its durable point stays behind the head it applied: `applied`
/// must be the receiver's buffered head, not its durable point (tester gap G3, mutant M7).
#[test]
fn a_secondary_reports_the_lineage_and_head_its_receiver_holds() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |_| {});
    let partition = PartitionId(1);
    for (host, _, _) in &trio.nodes {
        assert_eq!(
            host.status(partition).holds,
            None,
            "nothing held before bootstrap"
        );
    }
    trio.nodes[1].0.next_flush = Tick(u64::MAX);
    trio.bootstrap();

    // Until node 2's receiver has taken the start record at seq 1.
    let head = trio.until("node 2's receiver took a record", |trio| {
        let head = trio.nodes[1]
            .0
            .replication
            .receiver(NodeId(2), partition)
            .map_or(0, |receiver| receiver.buffered_applied_seq().0);
        (head >= 1).then_some(head)
    });
    let secondary = &trio.nodes[1].0;
    let status = secondary.status(partition);
    assert_eq!(status.role, "secondary");
    let held = status
        .holds
        .expect("a secondary with a receiver holds its lineage");
    assert_eq!(
        (held.applied, held.durable),
        (head, 0),
        "applied is the buffered head; durable stays at 0 while node 2's flush is held"
    );
    assert_eq!(
        secondary.adopted(partition).generation,
        Generation(0),
        "a secondary adopts no generation, so the store view at it is empty"
    );
    assert_eq!(
        (held.generation, held.owner_epoch),
        (Generation(1), OwnerEpoch(1))
    );
    assert_eq!(
        held.applied,
        secondary
            .engine
            .buffered_applied(partition, Generation(1))
            .0,
        "the head is the record the store holds at the receiver's generation"
    );
}

/// T1 (the tester's write-path row; walks A9, B2, C2, D1, D3 and I1): one request id's
/// life, over three real hosts. A put publishes at seq 2, after the start record. The same
/// request sent again is answered seq 2 and appends nothing. A put under a stale version is
/// refused, provably mutates nothing, and takes no seq. The same id with another value is
/// `REQUEST_ID_REUSE` and leaves the object as it was. Status resolves the id to seq 2.
#[test]
fn one_request_id_publishes_once_and_its_misuses_are_refused() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();

    let first = match trio.ask(put(1, b"a", b"1", None)) {
        Answer::Txn { result, request } => {
            assert_eq!(
                (
                    result.generation,
                    result.seq,
                    result.outcome,
                    result.durability
                ),
                (
                    Generation(1),
                    Seq(2),
                    Outcome::Published,
                    Durability::BufferedOnTwo
                )
            );
            assert_eq!(
                request.expected_generation,
                Some(Generation(1)),
                "every sent put carries its generation fence"
            );
            request
        }
        other => panic!("put a=1: {other:?}"),
    };
    let head = trio.head();
    assert_eq!(head, Some(2));

    match trio.ask(ClientCall::Resend { request: first }) {
        Answer::Txn { result, .. } => {
            assert_eq!(result.seq, Seq(2), "a resend is the same transaction");
        }
        other => panic!("resend: {other:?}"),
    }
    assert_eq!(trio.head(), head, "a resend appends nothing");

    match trio.ask(put(2, b"a", b"x", Some(1))) {
        Answer::Error { error, .. } => {
            assert_eq!(error.name(), "CONDITION_FAILED");
            assert!(error.no_mutation, "a failed condition mutated nothing");
        }
        other => panic!("put a=x at stale version 1: {other:?}"),
    }
    match trio.ask(put(3, b"b", b"2", None)) {
        Answer::Txn { result, .. } => {
            assert_eq!(result.seq, Seq(3), "the refused put took no seq");
        }
        other => panic!("put b=2: {other:?}"),
    }

    match trio.ask(put(1, b"a", b"other", None)) {
        Answer::Error { error, .. } => assert_eq!(error.name(), "REQUEST_ID_REUSE"),
        other => panic!("request 1 with another value: {other:?}"),
    }
    match trio.ask(get(4, b"a")) {
        Answer::Read { seen, .. } => {
            assert_eq!(
                doc_bytes(seen),
                Some((2, Bytes::from_static(b"1"))),
                "a is unchanged"
            );
        }
        other => panic!("get a: {other:?}"),
    }

    match trio.ask(ClientCall::Status {
        identity: identity(1),
        generation: None,
    }) {
        Answer::Status(TxnStatus::Resolved(result)) => {
            assert_eq!((result.generation, result.seq), (Generation(1), Seq(2)));
        }
        other => panic!("status of request 1: {other:?}"),
    }
}

/// T2 (the tester's barrier-read row; walks E1, E2 and E3): with both secondaries cut off,
/// node 1 applies a put that no copy can acknowledge. A read at the barrier waits for it,
/// and never answers the old value. A read of the previous view answers the old value at
/// once. When the links heal, the put publishes and the waiting read answers the new value.
#[test]
fn a_read_waits_at_the_barrier_while_the_previous_view_answers_at_once() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    match trio.ask(put(1, b"a", b"old", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("put a=old: {other:?}"),
    }

    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    let put_new = trio.call(put(2, b"a", b"new", None));
    trio.until("node 1 applied a=new", |trio| {
        (trio.head() == Some(3)).then_some(())
    });
    let barrier = trio.call(get(3, b"a"));
    trio.until("node 1 took the read", |trio| {
        trio.nodes[0]
            .0
            .pending
            .contains_key(&identity(3))
            .then_some(())
    });
    // The next round's drain runs the read through P1, which answers or parks it there.
    trio.step();
    assert!(barrier.try_recv().is_err(), "the read waits at the barrier");
    assert!(put_new.try_recv().is_err(), "no copy acknowledged the put");

    match trio.ask(ClientCall::GetPrevious {
        identity: identity(4),
        object: Bytes::from_static(b"a"),
    }) {
        Answer::Read {
            outcome,
            seen,
            generation,
            at,
            ..
        } => assert_eq!(
            (outcome, doc_bytes(seen), generation, at),
            (
                ReadServiceOutcome::Served,
                Some((2, Bytes::from_static(b"old"))),
                Generation(1),
                Seq(2)
            )
        ),
        other => panic!("get a --previous: {other:?}"),
    }
    assert!(barrier.try_recv().is_err(), "the read still waits");

    trio.links.heal_all();
    match trio.answer(&put_new) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3)),
        other => panic!("put a=new after the heal: {other:?}"),
    }
    match trio.answer(&barrier) {
        Answer::Read {
            outcome, seen, at, ..
        } => assert_eq!(
            (outcome, doc_bytes(seen), at),
            (
                ReadServiceOutcome::WaitedAtBarrier,
                Some((3, Bytes::from_static(b"new"))),
                Seq(3)
            )
        ),
        other => panic!("the waiting read: {other:?}"),
    }
}

/// A10, end to end (the tester's row (b), mutant M19). Both secondaries are cut off from the
/// start, so F1 commits `ReadOnly` at cutoff 0 and names both copies stalled. After the heal
/// the partition activates, and activation clears the stall line. Then two puts publish at
/// seq 2 and 3, and L1 stays unpaused past its pause age, except for a pause that a stall of
/// this thread explains, which must end with L1 admitting (D16). Cuts (D16): a 600 ms stall
/// of the thread fails the old watch and passes this one; with that stall, an L1 that never
/// resumes after a pause fails this one.
///
/// Not D4's regression guard: stepped by hand, it passed 20 of 20 with D4 unfixed, where the
/// threaded walk lost a copy in ~3 of 23. The sim row `m9_d4_00` guards D4.
/// Integration (~5 s at scale 1): the stall line waits two 2 s windows, then the row watches
/// twice L1's pause age to show L1 never paused. Tagged so its lines land in its own log
/// file (#50).
#[config_log::retcd_test]
fn a_start_cut_off_from_both_secondaries_heals_into_a_writable_partition() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast_pause);
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    trio.bootstrap();
    let stalled = trio.until("F1's stall line", |trio| trio.status(0).stalled);
    assert!(stalled.contains("unproven=[1, 2]"), "{stalled}");

    trio.links.heal_all();
    trio.ready();
    assert_eq!(
        trio.status(0).stalled,
        None,
        "activation clears the stall line"
    );
    // D16: this one thread steps all three hosts, so a stall of it leaves the puts undurable
    // everywhere for as long as it lasts. Past the pause age L1 pauses, as the spec says it
    // must (600-680 ms under gate load, int fa72544; diag-d16). So a gap of a flush period
    // or more between looks is a stall; a pause that begins within one pause age of one is
    // allowed, and the window starts again from the stall. The watch ends only with L1
    // healthy and admitting, so a pause with no stall, or one that never ends, still fails.
    let stall = Duration::from_millis(HOST_FLUSH_EVERY_MILLIS);
    let pause_age = Duration::from_millis(Trio::PAUSE_AGE_MILLIS);
    let mut last = Instant::now();
    for (request, object, seq) in [(1, b"a", 2), (2, b"b", 3)] {
        match trio.ask(put(request, object, b"v", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(seq)),
            other => panic!("put {request}: {other:?}"),
        }
    }
    let mut past_pause = Instant::now() + 2 * pause_age;
    let mut woke: Option<Instant> = None;
    let mut held_since: Option<Instant> = None;
    trio.until("twice the pause age, unpaused and admitting", |trio| {
        let now = Instant::now();
        if now.duration_since(last) >= stall {
            woke = Some(now);
            past_pause = past_pause.max(now + 2 * pause_age);
        }
        last = now;
        let status = trio.status(0);
        let held = matches!(
            status.protection.as_deref(),
            Some("paused" | "reprotecting")
        );
        if held {
            let began = *held_since.get_or_insert(now);
            assert!(
                woke.and_then(|woke| began.checked_duration_since(woke))
                    .is_some_and(|since| since < pause_age),
                "L1 paused after the heal with no stall of this thread before it: {status:?}"
            );
        } else {
            held_since = None;
        }
        (now >= past_pause && !held && status.admits == Some(true)).then_some(())
    });
}

/// The tester's row (d), moved onto the pinned stall by the K1 ruling (condition 6), and
/// defect D7. Only link 1-3 is held, so F1 commits `DegradedRf2` at cutoff 0 and, since K1,
/// pins that rebuild at the commit and asks every copy to sync. Copy 2 never answers, so F1
/// names it at its deadline and the host's line says what is true while L1 pauses writes for
/// the unheard copy (B-R38): writes stay paused until it returns (PC17, mutant M28).
/// D7: node 2 heard the sync for generation 1 before it landed that generation, proved it,
/// and then faulted inheriting it. A sync for a generation not yet landed now waits for the
/// landing, so the proof is of a generation the copy holds.
#[config_log::retcd_test]
fn a_degraded_commit_with_a_copy_away_is_reported_stalled_with_writes_paused() {
    const METHOD: &str =
        "a_degraded_commit_with_a_copy_away_is_reported_stalled_with_writes_paused";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        host.rebuild_pin_wait_millis = 50;
    });
    trio.links.hold(NodeId(1), NodeId(3));
    trio.bootstrap();
    trio.short_window_after_commit();
    let stalled = trio.until("a stall line", |trio| trio.status(0).stalled);
    assert!(
        trio.nodes[0].0.rebuilds[&PartitionId(1)].pinned,
        "F1 pinned the rebuild at the commit"
    );
    assert_eq!(trio.status(0).admits, Some(false), "L1 pauses writes");
    for part in [
        "recovery_rebuild_stalled partition=1 gen=1 mode=DegradedRf2 cutoff=0 \
         required=[0, 1, 2] unproven=[",
        "2] phase=Rebuilding: F1 pinned its rebuild, but these copies have not proved it, so \
         the partition stays DegradedRf2, and its writes stay paused until the database is \
         opened again",
    ] {
        assert!(stalled.contains(part), "{part:?} missing from {stalled:?}");
    }
    assert!(!stalled.contains("never pinned"), "{stalled}");

    // The never-pinned report (M27), asked for directly: since K1 no M9 run leaves a
    // rebuild unpinned. While L1 pauses writes it is printed, in PC17's words.
    let host = &mut trio.nodes[0].0;
    let watch = host.rebuilds.get_mut(&PartitionId(1)).expect("the watch");
    watch.pinned = false;
    let generation = watch.generation;
    host.stalled.remove(&PartitionId(1));
    host.rebuild_check(PartitionId(1), generation);
    let never = host
        .status(PartitionId(1))
        .stalled
        .expect("the never-pinned line");
    assert!(
        never.ends_with(
            "F1 never pinned its rebuild, so the partition stays DegradedRf2, and its writes \
             stay paused until the database is opened again"
        ),
        "{never}"
    );

    // D7: node 2 proved generation 1 only after it landed it.
    let node2 = |message: &str| {
        logged!(METHOD, message)
            .into_iter()
            .filter(|line| line["node"] == 2 && line["generation"] == 1)
            .map(|line| line["@t"].as_str().unwrap_or_default().to_owned())
            .collect::<Vec<_>>()
    };
    let landed = node2("recovered_landed");
    let proved = node2("sync_proven");
    assert_eq!(landed.len(), 1, "node 2 landed generation 1 once");
    assert!(
        !proved.is_empty() && proved.iter().all(|at| *at > landed[0]),
        "node 2 proved generation 1 at {proved:?}, landed it at {landed:?}"
    );
}

/// PR #33 F-002 (amending ADR-rdb-0009 D3 rule 4). Link 1-3 is held, so F1 commits
/// `DegradedRf2` at cutoff 0 and node 1 applies the start record at seq 1. From the commit on,
/// node 1's peer frames are set aside, so no copy's acknowledgement reaches R1 and seq 1 stays
/// unpublished. After the heal, node 3 proves the rebuild and F1's activation lands while
/// seq 1 is still pending: the order in which a CAS answer beats copy 2's ack. P1 keeps the
/// view it holds at seq 0, so `GetPrevious` is answered from it, not refused. The frames are
/// then delivered and seq 1 publishes. Stepped by hand; no link 1-2 hold, which R1 may report
/// as a lost copy.
#[test]
fn a_re_emit_while_the_start_record_is_pending_keeps_the_previous_view() {
    /// [`Trio::step`], with every `Frame` to node 1 set aside in `deferred`.
    fn step(trio: &mut Trio, deferred: &mut Vec<Msg>) {
        for (index, (host, _, rx)) in trio.nodes.iter_mut().enumerate() {
            host.run_due();
            host.drain();
            while let Ok(msg) = rx.try_recv() {
                if index == 0 && matches!(msg, Msg::Frame { .. }) {
                    deferred.push(msg);
                } else {
                    host.handle(msg);
                }
            }
            assert_eq!(host.fault, None, "node {} faulted", host.node.0);
            assert_not_blocked(
                &host.status(PartitionId(1)),
                host.budgets.discovery_window_millis,
            );
        }
    }
    fn until<T>(
        trio: &mut Trio,
        deferred: &mut Vec<Msg>,
        what: &str,
        mut done: impl FnMut(&Trio) -> Option<T>,
    ) -> T {
        let deadline = Instant::now() + test_patience(Trio::PATIENCE);
        loop {
            step(trio, deferred);
            if let Some(found) = done(trio) {
                return found;
            }
            assert!(Instant::now() < deadline, "not in time: {what}");
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    let view = |trio: &Trio| {
        trio.nodes[0]
            .0
            .publication
            .view(NodeId(1), PartitionId(1))
            .expect("P1 serves partition 1 on node 1")
    };

    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.links.hold(NodeId(1), NodeId(3));
    trio.bootstrap();
    trio.until("F1 committed on node 1", |trio| {
        trio.status(0).recovered.map(|_| ())
    });
    let mut deferred = Vec::new();
    until(&mut trio, &mut deferred, "node 1 applied seq 1", |trio| {
        (trio.head() == Some(1)).then_some(())
    });
    let mode = |trio: &Trio| trio.nodes[0].0.committed[&PartitionId(1)].mode.clone();
    assert_eq!(mode(&trio), PartitionMode::DegradedRf2);
    assert_eq!(
        trio.status(0).published,
        Some((Generation(1), Seq(0))),
        "seq 1 is pending"
    );
    let before = view(&trio);
    assert!(before.kept.is_some(), "P1 holds the view at seq 0");

    trio.links.heal(NodeId(1), NodeId(3));
    until(&mut trio, &mut deferred, "the activation landed", |trio| {
        (mode(trio) == PartitionMode::Active).then_some(())
    });
    step(&mut trio, &mut deferred);
    assert_eq!(
        trio.status(0).published,
        Some((Generation(1), Seq(0))),
        "the activation landed before any ack of seq 1"
    );
    let after = view(&trio);

    let previous = trio.call(ClientCall::GetPrevious {
        identity: identity(1),
        object: Bytes::from_static(b"a"),
    });
    match until(&mut trio, &mut deferred, "the previous read", |_| {
        previous.try_recv().ok()
    }) {
        Answer::Read {
            outcome, seen, at, ..
        } => assert_eq!(
            (outcome, doc_bytes(seen), at),
            (ReadServiceOutcome::Served, None, Seq(0)),
            "answered from the view kept at seq 0"
        ),
        other => panic!("get a --previous while seq 1 is pending: {other:?}"),
    }
    assert_eq!(
        (&after.kept, after.opening.len()),
        (&before.kept, 0),
        "the re-emit kept the view and opened none"
    );

    assert!(!deferred.is_empty(), "the copies' acks were set aside");
    for msg in deferred {
        trio.nodes[0].0.handle(msg);
    }
    trio.until("seq 1 published", |trio| {
        (trio.status(0).published == Some((Generation(1), Seq(1)))).then_some(())
    });
}

/// F8, the host half (K1 ruling, condition 5): in `DegradedRf2` with copy 2 away, F1 names
/// it again at every deadline, and the host prints its stall once. Each deadline re-asks
/// copy 2, which the host logs as `rebuild_pinned`, so the row counts deadlines from the log
/// rather than assuming them from elapsed time. Mutant M26 (report at every naming) fails it.
#[config_log::retcd_test]
fn a_degraded_stall_is_printed_once_across_three_deadlines() {
    const METHOD: &str = "a_degraded_stall_is_printed_once_across_three_deadlines";
    let asks_of_copy_2 = || {
        logged!(METHOD, "rebuild_pinned")
            .into_iter()
            .filter(|line| line["node"] == 1 && line["copy"] == 2)
            .count()
    };
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        host.rebuild_pin_wait_millis = 50;
    });
    trio.links.hold(NodeId(1), NodeId(3));
    trio.bootstrap();
    trio.short_window_after_commit();
    let stalled = trio.until("a stall line", |trio| trio.status(0).stalled);

    // The commit's ask and three deadlines' asks; the log is read every 50 ms, not per step.
    let mut read = Instant::now();
    trio.until("three more of F1's deadlines", |_| {
        if read.elapsed() < Duration::from_millis(50) {
            return None;
        }
        read = Instant::now();
        (asks_of_copy_2() >= 4).then_some(())
    });
    assert_eq!(
        trio.status(0).stalled,
        Some(stalled),
        "the line stays as printed"
    );
    let logged = logged!(METHOD, "recovery_rebuild_stalled");
    assert_eq!(logged.len(), 1, "one line across the deadlines: {logged:?}");
    assert_eq!(logged[0]["@l"], "Error", "{}", logged[0]);
}

/// K1 ruling, condition 6: no stall line says writes are paused while L1 admits them, and
/// a rebuild that never pinned is not reported at all then. M9 reaches neither: K1 pins
/// every cutoff-0 commit, and an unheard copy pauses writes (B-R38). So the watch is set by
/// hand on an `Active`, admitting node, and each report is asked for directly.
#[test]
fn a_stall_line_never_says_writes_are_paused_while_they_are_admitted() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    assert_eq!(trio.status(0).admits, Some(true), "L1 admits writes");
    let partition = PartitionId(1);
    let host = &mut trio.nodes[0].0;
    let generation = Generation(9);
    host.rebuilds.insert(
        partition,
        RebuildWatch {
            generation,
            mode: PartitionMode::DegradedRf2,
            cutoff: Seq(1),
            required: vec![CopyId(0), CopyId(1), CopyId(2)],
            since: host.clock.now(),
            pinned: false,
            unproven: BTreeSet::new(),
            unreported: false,
        },
    );

    host.rebuild_check(partition, generation);
    assert_eq!(
        host.stalled.get(&partition),
        None,
        "never-pinned while admitting"
    );

    let watch = host.rebuilds.get_mut(&partition).expect("watch");
    watch.pinned = true;
    watch.unproven.insert(CopyId(2));
    watch.unreported = true;
    host.report_unproven();
    let line = host
        .status(partition)
        .stalled
        .expect("the pinned stall line");
    assert!(
        line.ends_with("so the partition stays DegradedRf2 and does not activate"),
        "{line}"
    );
}

/// F-012 (S0 review): the stall line says writes stay paused only while L1 pauses them. L1
/// is mode-blind, so nothing orders its resume after F1's activation, which is what clears
/// the line. On a real `DegradedRf2` heal F1 activated first in 6 of 6 runs here, even with
/// a zero resume hold, so this row orders them by hand instead. L1 pauses for real: both
/// links are held, and a put stays unacknowledged past the pause age. Then a stall line is
/// written for a `DegradedRf2` watch set by hand, as the K1 row above does, and it says
/// writes stay paused. After the heal L1 resumes, and that watch never activates. Once L1
/// admits writes, the line must stop saying they are paused. Status names the held put as
/// applied and unpublished (`in_flight`), and nothing once it publishes. The queue is not
/// frozen on it (A-R252), so `waits_on` stays `None` throughout.
/// Integration (~1 s): three hosts stepped by hand, through a real L1 pause and resume.
#[test]
fn a_stall_line_stops_saying_writes_are_paused_once_l1_admits_them() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast_pause);
    trio.bootstrap();
    trio.ready();
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    let unacknowledged = trio.call(put(1, b"a", b"1", None));
    let paused = trio.until("L1 pauses writes", |trio| {
        let status = trio.status(0);
        (status.admits == Some(false)).then_some(status)
    });
    // The queue stays open behind an applied, unpublished put (A-R252), so nothing is frozen
    // for `waits_on` to name; `in_flight` names the put.
    assert_eq!(
        (paused.in_flight, paused.waits_on),
        (Some(RequestId(1)), None),
        "in_flight names the applied, unpublished put: {paused:?}"
    );

    let partition = PartitionId(1);
    let host = &mut trio.nodes[0].0;
    let generation = Generation(9);
    host.rebuilds.insert(
        partition,
        RebuildWatch {
            generation,
            mode: PartitionMode::DegradedRf2,
            cutoff: Seq(1),
            required: vec![CopyId(0), CopyId(1), CopyId(2)],
            since: host.clock.now(),
            pinned: true,
            unproven: BTreeSet::from([CopyId(2)]),
            unreported: true,
        },
    );
    host.report_unproven();
    let paused = trio.status(0).stalled.expect("the stall line");
    assert!(paused.contains("writes stay paused"), "{paused}");

    trio.links.heal_all();
    trio.ready();
    let status = trio.status(0);
    assert_eq!(
        trio.nodes[0]
            .0
            .rebuilds
            .get(&partition)
            .map(|w| w.generation),
        Some(generation),
        "the watch is still there: nothing activated it"
    );
    let line = status.stalled.expect("the stall line still stands");
    assert!(
        !line.contains("writes stay paused"),
        "L1 admits writes while the stall line says: {line}"
    );
    assert!(
        line.ends_with("so the partition stays DegradedRf2 and does not activate"),
        "{line}"
    );
    let published = trio.until("seq 2 publishes", |trio| {
        let status = trio.status(0);
        (status.published.map(|(_, seq)| seq) == Some(Seq(2))).then_some(status)
    });
    assert_eq!(
        (published.in_flight, published.waits_on),
        (None, None),
        "nothing in flight once the put published: {published:?}"
    );
    drop(unacknowledged);
}

/// A node that faults answers every waiting caller instead of leaving it hanging, and a
/// waiting put keeps its request so the caller can resend it elsewhere. Every call after
/// that is refused at once. The fault here is the control adapter's own (`Msg::Fault`), the
/// one input that faults a healthy node without a kernel or storage defect.
#[test]
fn a_faulted_node_answers_its_waiting_put_with_the_request_and_refuses_new_calls() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let waiting = trio.call(put(1, b"a", b"1", None));
    let (host, _, mailbox) = &mut trio.nodes[0];
    // The mailbox is shared with peer and control traffic, so the put need not be first.
    while !host.pending.contains_key(&identity(1)) {
        match mailbox.try_recv() {
            Ok(msg) => host.handle(msg),
            Err(_) => break,
        }
    }
    assert!(
        host.pending.contains_key(&identity(1)),
        "the put waits: {:?}",
        waiting.try_recv()
    );

    host.handle(Msg::Fault {
        boot: BOOT,
        kind: "control_fault_test",
        detail: "injected".to_owned(),
    });
    assert!(host.fault.is_some(), "the node is faulted");
    match waiting.try_recv() {
        Ok(Answer::Error {
            error,
            request: Some(request),
        }) => {
            assert_eq!(error.kind, ErrorKind::Unavailable, "{error:?}");
            assert!(error.detail.contains("injected"), "{error:?}");
            assert_eq!(request.identity, identity(1));
        }
        other => panic!("the waiting put: {other:?}"),
    }

    let (reply, refused) = mpsc::channel();
    host.handle(Msg::Client(Client {
        partition: PartitionId(1),
        call: get(2, b"a"),
        reply,
    }));
    match refused.try_recv() {
        Ok(Answer::Error {
            error,
            request: None,
        }) => assert!(error.detail.contains("injected"), "{error:?}"),
        other => panic!("a call after the fault: {other:?}"),
    }
    assert!(host.pending.is_empty(), "nothing is left waiting");
}

/// D5's guard on R1's real send path: a start held paused logs no
/// `replication_copy_not_advancing` while R1's keepalive re-sends seq 1 every `KEEPALIVE_MS` to
/// copies that hold it. A copy level with the primary's head is not stuck, however often a record
/// is re-sent to it. `resume_lag_millis = 0` keeps L1 from resuming (lag is never below 0), so
/// the keepalive runs for the whole half second; with `stuck_resends = 1` the second identical
/// send to one copy would warn without the level check. Before A-R344 a start ran L1's resume
/// hold and a healthy start did this; a first resume now skips the hold (ADR-0006 amendment
/// 2026-10-23), and a healthy start re-sends seq 1 twice only on some runs.
#[config_log::retcd_test]
fn a_start_held_paused_reports_no_copy_stuck_while_r1_re_sends_seq_1() {
    const METHOD: &str = "a_start_held_paused_reports_no_copy_stuck_while_r1_re_sends_seq_1";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.resume_lag_millis = 0;
        host.stuck_resends = 1;
    });
    trio.bootstrap();
    let since = trio.until("R1 sent seq 1", |trio| {
        trio.nodes[0]
            .0
            .resends
            .values()
            .any(|entry| entry.through == Seq(1))
            .then(|| trio.clock.now().0)
    });
    trio.until("half a second of keepalives", |trio| {
        (trio.clock.now().0 >= since + 500).then_some(())
    });
    assert_eq!(trio.status(0).admits, Some(false), "L1 never resumed");
    let warned = not_advancing!(METHOD);
    assert!(warned.is_empty(), "a start held paused warned: {warned:?}");
}

/// D4's shape at the host: the copy acknowledged the record R1 keeps re-sending (through 1,
/// acked 1), and the head moved past it to 2. The host cannot make R1 do that without the
/// kernel defect, so the row hands `count_resend` the sends directly, against node 1's real
/// head. It uses a copy id R1 never sends to, so only the row's sends touch the slot, as only
/// R1's through-1 re-sends touched copy 2's in D4 (copy 2's own slot takes the put's seq 2).
/// Level with the head nothing is counted. The first send once behind repeats the last
/// level one, so it is re-send 1; the `stuck_resends`-th warns, and later ones do not.
/// The kernel side is the sim row `m9_d4_00`. Then a member copy's line names the node and
/// boot its configuration pins (Message 6), where the unsent copy's names neither.
#[config_log::retcd_test]
fn a_copy_acked_below_the_head_is_reported_once_it_stops_advancing() {
    const METHOD: &str = "a_copy_acked_below_the_head_is_reported_once_it_stops_advancing";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.stuck_resends = 3;
    });
    trio.bootstrap();
    trio.ready();
    let send = |trio: &mut Trio| {
        trio.nodes[0].0.count_resend(
            PartitionId(1),
            Generation(1),
            UNSENT,
            Seq(1),
            acked_at(1, 1),
        );
    };

    assert_eq!(trio.head(), Some(1));
    for _ in 0..6 {
        send(&mut trio);
    }
    assert_eq!(not_advancing!(METHOD).len(), 0, "level with the head");

    // A stall on this thread past the grant lapses it (#50): ask with time left on it.
    trio.admits_for(1_000);
    match trio.ask(put(1, b"a", b"1", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("put a=1: {other:?}"),
    }
    assert_eq!(trio.head(), Some(2));
    assert_eq!(
        trio.nodes[0].0.resends[&(PartitionId(1), UNSENT)].episode(),
        (Generation(1), Seq(1), acked_at(1, 1)),
        "R1 did not touch the row's slot"
    );
    send(&mut trio);
    send(&mut trio);
    assert_eq!(
        not_advancing!(METHOD).len(),
        0,
        "re-sends 1 and 2 while behind"
    );
    send(&mut trio);
    assert_eq!(not_advancing!(METHOD).len(), 1, "re-send 3 warns");
    send(&mut trio);
    send(&mut trio);
    let warned = not_advancing!(METHOD);
    assert_eq!(warned.len(), 1, "one line per episode: {warned:?}");
    let line = &warned[0];
    assert_eq!(
        (
            &line["copy"],
            &line["through"],
            &line["head"],
            &line["acked_applied"],
            &line["resends"]
        ),
        (&UNSENT.0.into(), &1.into(), &2.into(), &1.into(), &3.into()),
        "{line}"
    );
    assert!(
        line["copy_node"].is_null() && line["copy_boot"].is_null(),
        "a copy outside the configuration names no member: {line}"
    );

    // Message 6: a member's line names the node and the boot the configuration pins to it,
    // so a restarted node (a later boot) reads as the copy that stopped. Copy 1 is node 2.
    // Its slot holds R1's last level send, so the first send here starts the episode.
    for _ in 0..4 {
        trio.nodes[0].0.count_resend(
            PartitionId(1),
            Generation(1),
            CopyId(1),
            Seq(1),
            acked_at(1, 1),
        );
    }
    let warned = not_advancing!(METHOD);
    assert_eq!(warned.len(), 2, "{warned:?}");
    let line = &warned[1];
    assert_eq!(
        (&line["copy"], &line["copy_node"], &line["copy_boot"]),
        (&1.into(), &2.into(), &BOOT.0.into()),
        "{line}"
    );
}

/// Lead ruling, mutants M35 and M40: the rule reads the copy's acked **applied** position,
/// and a change in it starts a new episode. R1 re-sends seq 3 at head 3 while the copy's
/// acked applied climbs 1, 2, 3 and its acked durable stays at 1. Each step is a copy that
/// is advancing, so no line: two re-sends at 1 and two at 2 stay under the threshold only
/// if the climb restarts the count (M40), and at 3 the copy is level by applied though not
/// by durable (M35). Same direct route and unsent copy id as the D4-shape row.
#[config_log::retcd_test]
fn a_copy_whose_acked_applied_climbs_is_not_reported_while_durable_lags() {
    const METHOD: &str = "a_copy_whose_acked_applied_climbs_is_not_reported_while_durable_lags";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.stuck_resends = 3;
    });
    trio.bootstrap();
    trio.ready();
    for (request, object, seq) in [(1, b"a", 2), (2, b"b", 3)] {
        match trio.ask(put(request, object, b"v", None)) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(seq)),
            other => panic!("put {request}: {other:?}"),
        }
    }
    assert_eq!(trio.head(), Some(3));

    for (applied, sends) in [(1, 3), (2, 3), (3, 4)] {
        for _ in 0..sends {
            trio.nodes[0].0.count_resend(
                PartitionId(1),
                Generation(1),
                UNSENT,
                Seq(3),
                acked_at(applied, 1),
            );
        }
    }
    let warned = not_advancing!(METHOD);
    assert!(warned.is_empty(), "an advancing copy warned: {warned:?}");
}

/// M9 S2a ruling guard "the `past_horizon` computation": `authority_view` logs
/// `margin_ms` = `valid_through` - `at`, and `past_horizon` only when that is below 0.
/// While renewals commit every view has time left; once the store stops answering, the
/// grant runs out and the view A1 publishes at the fence has none.
/// Integration (~5.5 s at scale 1, measured alone, review F-006): the grant must expire at
/// the spec's horizon.
#[config_log::retcd_test]
fn the_view_line_says_past_horizon_only_once_no_time_is_left() {
    const METHOD: &str = "the_view_line_says_past_horizon_only_once_no_time_is_left";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    trio.until("node 1 fenced", |trio| {
        trio.status(0).authority.starts_with("fenced").then_some(())
    });

    let views: Vec<_> = logged!(METHOD, "authority_view")
        .into_iter()
        .filter(|line| line["node"] == 1)
        .collect();
    for line in &views {
        let left = line["valid_through"].as_i64().expect("valid_through")
            - line["at"].as_i64().expect("at");
        assert_eq!(line["margin_ms"], left, "{line}");
        assert_eq!(line["past_horizon"], left < 0, "{line}");
    }
    assert!(
        views.iter().any(|line| line["past_horizon"] == false),
        "a view with time left: {views:?}"
    );
    assert!(
        views.iter().any(|line| line["past_horizon"] == true),
        "the fence's view has none: {views:?}"
    );
}

/// The `Acquire` firings queued on `host`, with the tick each is queued for.
fn queued_acquires(host: &Host) -> Vec<(Tick, TimerFired)> {
    host.delayed
        .iter()
        .filter_map(|((at, _), item)| match item {
            Delayed::Event(Queued {
                event:
                    Event {
                        kind: EventKind::Timer(fired),
                        ..
                    },
                ..
            }) if fired.id == AuthorityTimer::Acquire.id() => Some((*at, *fired)),
            _ => None,
        })
        .collect()
}

/// A-R165 item 2: after an `Active` commit the primary's first `Acquire` is queued
/// [`ACQUIRE_AFTER_ACTIVE_MILLIS`] out; after any other commit it keeps
/// [`ACQUIRE_AFTER_RECOVERED_MILLIS`]. Three nodes with no holds commit `Active`; with node
/// 1's links to 2 and 3 held, F1 commits `ReadOnly`. The firing is read off node 1's queue
/// in the round the commit lands, before it can fire.
/// Integration (~4 s): two discovery windows.
#[test]
fn the_first_acquire_after_an_active_commit_is_queued_sooner() {
    let first_acquire = |held: bool| {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        if held {
            trio.links.hold(NodeId(1), NodeId(2));
            trio.links.hold(NodeId(1), NodeId(3));
        }
        trio.bootstrap();
        trio.until("F1 committed on node 1", |trio| {
            trio.status(0).recovered.map(|_| {
                let now = trio.clock.now().0;
                queued_acquires(&trio.nodes[0].0)
                    .iter()
                    .map(|(at, _)| at.0.saturating_sub(now))
                    .collect::<Vec<_>>()
            })
        })
    };
    let active = first_acquire(false);
    let [left] = active.as_slice() else {
        panic!("one Acquire queued after the Active commit, not {active:?}");
    };
    assert!(
        *left <= ACQUIRE_AFTER_ACTIVE_MILLIS,
        "Active: due in {left} ms"
    );
    let read_only = first_acquire(true);
    let [left] = read_only.as_slice() else {
        panic!("one Acquire queued after the ReadOnly commit, not {read_only:?}");
    };
    assert!(
        *left > ACQUIRE_AFTER_ACTIVE_MILLIS && *left <= ACQUIRE_AFTER_RECOVERED_MILLIS,
        "ReadOnly: due in {left} ms"
    );
}

/// Message 6, round 4 (A-R96 host trigger and status). A node fence queues one `Acquire`
/// firing at A1's current `Acquire` timer version, one renewal later: the kernel drops a
/// firing older than that version as stale, and without the firing A1 never polls its grant
/// and writes never resume. Status says `admits=false` while fenced, though L1 still allows.
/// The second fence shows the version is read, not assumed: A1 has re-armed `Acquire`
/// since the first, so it is above 0 there. Between them the grant is replaced, so writes
/// resume each time.
/// Integration (~3 s): bootstrap's discovery window, then two fences on a 600 ms grant.
#[config_log::retcd_test]
fn each_node_fence_queues_an_acquire_at_the_current_timer_version() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.grant_millis = 600;
        host.budgets.renew_millis = 100;
    });
    trio.bootstrap();
    trio.ready();
    let mut versions = Vec::new();
    for fence in 1..=2 {
        trio.mem
            .failing_with(config_core::ConfigError::Unavailable {
                reason: "the control store is down".to_owned(),
            });
        let (status, queued, current) = trio.until("node 1 fenced", |trio| {
            let status = trio.status(0);
            let host = &trio.nodes[0].0;
            status.authority.starts_with("fenced").then(|| {
                let current = host.authority.timer_version(AuthorityTimer::Acquire);
                (status, queued_acquires(host), current)
            })
        });
        assert_eq!(
            (status.admits, status.protection.as_deref()),
            (Some(false), Some("healthy")),
            "fence {fence}: L1 allows, A1 does not: {status:?}"
        );
        let [(at, fired)] = queued.as_slice() else {
            panic!("fence {fence}: one Acquire queued, not {queued:?}");
        };
        assert_eq!(fired.version, current, "fence {fence}");
        assert_eq!(fired.scheduled_at, *at, "fence {fence}");
        versions.push(current.0);

        // D15: A1 re-arms `Acquire` only when a poll does not acquire. While the store is
        // down every poll reads `Unavailable` and re-arms, so wait for that before the store
        // returns. Otherwise a first poll that comes after the old grant expires acquires at
        // once, and the version stays 0 (seen under load). Versions never fall in a boot,
        // so the second fence's version is above 0 by construction.
        if fence == 1 {
            trio.until("A1 re-armed Acquire during the outage", |trio| {
                let host = &trio.nodes[0].0;
                (host.authority.timer_version(AuthorityTimer::Acquire) > current).then_some(())
            });
        }
        trio.mem.stop_failing();
        trio.until("node 1 holds a grant again", |trio| {
            (trio.status(0).admits == Some(true)).then_some(())
        });
    }
    assert!(versions[1] > 0, "the second fence's version: {versions:?}");
}

/// Msg 8, F-001: `admits` is A1's own admission verdict, not "L1 allows and A1 is `Held`".
/// While the control store is down A1 still holds, but denies `ControlUnavailable` before
/// it fences, and status said `admits=true` there. After the store returns, `admits=true`
/// means a put lands.
/// Integration (~2 s): bootstrap's discovery window, then one store outage on a 600 ms grant.
#[config_log::retcd_test]
fn admits_follows_a1s_verdict_while_the_grant_is_still_held() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.grant_millis = 600;
        host.budgets.renew_millis = 100;
    });
    trio.bootstrap();
    trio.ready();
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    let denied = trio.until("node 1 stops admitting", |trio| {
        let status = trio.status(0);
        (status.admits != Some(true)).then_some(status)
    });
    trio.mem.stop_failing();
    assert!(
        denied.authority.starts_with("held("),
        "admits went false before the fence, while the grant is still held: {denied:?}"
    );
    assert_eq!(denied.admits, Some(false), "{denied:?}");
    trio.until("node 1 admits again", |trio| {
        (trio.status(0).admits == Some(true)).then_some(())
    });
    match trio.ask(put(1, b"a", b"1", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("a put once status admits: {other:?}"),
    }
}

/// Msg 8, walk 8d: while a self-replace re-acquires, A1 is `Unheld` with the old fence as
/// `last_fence`, and status reported it as a live fence. rdb_dev then said "nothing in M9
/// replaces it" one step before the new grant committed. Only `Fenced` is a fence.
/// Unit (~0 ms).
#[test]
fn status_reports_a_fence_only_while_fenced() {
    let unheld = AuthorityState::Unheld {
        last_fence: Some(DenyReason::Expired),
        acquire: None,
    };
    assert_eq!(fence_of(&unheld), None, "unheld after a fence");
    assert_eq!(fence_of(&AuthorityState::default()), None, "unheld at boot");
    let fenced = AuthorityState::Fenced {
        reason: DenyReason::Expired,
        at: Tick(0),
    };
    assert_eq!(fence_of(&fenced), Some(DenyReason::Expired));
}

/// Msg 8, F-004 and F-016: status says whether A1's after-fence poll is live, from A1
/// itself. An `Expired` fence polls; a freeze of the record afterwards stops the poll while
/// the fence still reads `Expired`, so the reason alone cannot tell (F-016). The freeze is
/// written with no step in between, after the failing calls have drained, so no poll can
/// read the record unfrozen and replace it.
/// Integration (~2 s): bootstrap's discovery window, then one fence on a 600 ms grant.
#[config_log::retcd_test]
fn status_says_the_after_fence_poll_stopped_when_the_record_is_frozen_later() {
    use config_core::{ConfigStore as _, GetRequest, MutationOutcome, PutRequest};
    use rdb_core::authority::grant::GrantRecord;
    use rdb_core::contracts::control::ControlKey;

    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.grant_millis = 600;
        host.budgets.renew_millis = 100;
    });
    trio.bootstrap();
    trio.ready();
    let held = trio.status(0);
    assert_eq!(
        (held.fence, held.fence_polling, held.replace_stopped),
        (None, false, false),
        "{held:?}"
    );
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    let fenced = trio.until("node 1 fenced", |trio| {
        let status = trio.status(0);
        status.fence.is_some().then_some(status)
    });
    assert_eq!(
        (
            fenced.fence,
            fenced.fence_polling,
            fenced.replace_stopped,
            fenced.admits
        ),
        (Some(DenyReason::Expired), true, false, Some(false)),
        "{fenced:?}"
    );

    // No step from here to the freeze: nothing new is sent, and what was sent fails.
    std::thread::sleep(Duration::from_millis(50));
    trio.mem.stop_failing();
    let key = Bytes::from(ControlKey::Grant(NodeId(1)).encode());
    let found = trio
        .rt
        .block_on(trio.mem.get(GetRequest { key: key.clone() }))
        .expect("get grants/1")
        .record
        .expect("grants/1 exists");
    let mut record = GrantRecord::decode(&found.value).expect("a grant record");
    record.frozen = true;
    let put = trio
        .rt
        .block_on(trio.mem.put(PutRequest {
            key,
            value: record.encode(),
            expected_mod_revision: Some(found.mod_revision),
            dedup: None,
        }))
        .expect("put grants/1 frozen");
    assert_eq!(put.outcome, MutationOutcome::Applied);

    let stopped = trio.until("the poll reads the frozen record", |trio| {
        let status = trio.status(0);
        (!status.fence_polling).then_some(status)
    });
    assert_eq!(
        (
            stopped.fence,
            stopped.authority.as_str(),
            stopped.replace_stopped,
            stopped.admits
        ),
        (
            Some(DenyReason::Expired),
            "fenced(Expired)",
            true,
            Some(false)
        ),
        "the reason is unchanged; only the poll says it stopped: {stopped:?}"
    );
}

/// U1 (ruling A-R109) end to end: a write applied and unpublished when a node fence lands.
/// Both copies' ACKs to the owner are dropped, so the put (seq 2) is applied on node 1 and
/// never published, and T1's queue is frozen on it. The control store then fails until A1
/// fences, which turns that freeze into a lost-authority one: status names seq 2 as the
/// write the queue waits on, and does not admit. The store returns and A1 replaces its own
/// grant (A-R96), and the queue still waits on seq 2: only its publication resolves it. Once
/// the links heal it publishes, `waits_on` clears, and writes are admitted again.
/// Integration (~3.5 s): bootstrap's discovery window, then one fence on a 600 ms grant.
#[config_log::retcd_test]
fn a_write_unpublished_at_a_fence_is_what_the_queue_waits_on_until_it_publishes() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.grant_millis = 600;
        host.budgets.renew_millis = 100;
    });
    trio.bootstrap();
    trio.ready();
    trio.links.drop_direction(NodeId(2), NodeId(1));
    trio.links.drop_direction(NodeId(3), NodeId(1));
    let unacknowledged = trio.call(put(1, b"a", b"1", None));
    trio.until("node 1 applies seq 2", |trio| {
        (trio.head() == Some(2)).then_some(())
    });

    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    let fenced = trio.until("node 1 fenced", |trio| {
        let status = trio.status(0);
        status.fence.is_some().then_some(status)
    });
    assert_eq!(
        (
            fenced.waits_on,
            fenced.admits,
            fenced.published.map(|(_, seq)| seq)
        ),
        (Some(Seq(2)), Some(false), Some(Seq(1))),
        "waits_on names the unpublished put at the fence: {fenced:?}"
    );

    trio.mem.stop_failing();
    let replaced = trio.until("node 1 holds a grant again", |trio| {
        let status = trio.status(0);
        status.authority.starts_with("held(").then_some(status)
    });
    assert_eq!(
        (replaced.waits_on, replaced.published.map(|(_, seq)| seq)),
        (Some(Seq(2)), Some(Seq(1))),
        "waits_on still names the put under the new grant: {replaced:?}"
    );

    trio.links.heal_all();
    let published = trio.until("seq 2 publishes", |trio| {
        let status = trio.status(0);
        (status.published.map(|(_, seq)| seq) == Some(Seq(2))).then_some(status)
    });
    assert_eq!(
        published.waits_on, None,
        "waits_on once the put published: {published:?}"
    );
    trio.until("node 1 admits writes", |trio| {
        (trio.status(0).admits == Some(true)).then_some(())
    });
    drop(unacknowledged);
}

/// A-R324: a node fence that A1 sites at partition 0 still freezes partition 1's queue. A1
/// sites a fence at the partition of the event that finds the grant expired. Under load that
/// can be a host duty or a control answer at partition 0, where no T1 or P1 serves; before
/// A-R324 the fence reached only partition 0, so partition 1's T1 never froze, never recorded
/// its lost authority, and `waits_on` stayed `None` with seq 2 unpublished (the
/// `a_write_unpublished_at_a_fence...` red, perf and w8 runs 2026-10-23). Here node 1 is held
/// still past its grant's expiry, then given only the flush duty, which is sited at partition 0.
///
/// A-R331: the fan-out set is A1's own fence views, kept to partitions with a T1 or P1. Node 1
/// also serves partition 2, a `partitions/2` record naming it owner, with no plan, so no T1.
/// A1's fence views name 0, 1 and 2. Partition 2's P1, made by A1's view, takes the fence; no
/// T1 is made there and the node does not fault. Partition 1 freezes (A-13).
/// Integration (~2 s): bootstrap's discovery window, then one 600 ms grant left to lapse.
#[config_log::retcd_test]
fn a_node_fence_sited_at_partition_0_freezes_partition_1s_queue() {
    use config_core::{ConfigStore as _, MutationOutcome, PutRequest};
    use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
    use rdb_core::contracts::control::ControlKey;

    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.grant_millis = 600;
        host.budgets.renew_millis = 100;
    });
    let p2 = PartitionRecord {
        partition: PartitionId(2),
        owner: NodeId(1),
        generation: Generation(0),
        owner_epoch: OwnerEpoch(0),
        config_version: crate::admin::CONFIG_VERSION,
        lifecycle: PartitionLifecycle::Serving,
    };
    let seeded = trio
        .rt
        .block_on(trio.mem.put(PutRequest {
            key: Bytes::from(ControlKey::Partition(PartitionId(2)).encode()),
            value: p2.encode(),
            expected_mod_revision: Some(0),
            dedup: None,
        }))
        .expect("put partitions/2");
    assert_eq!(seeded.outcome, MutationOutcome::Applied);
    // Seeded first, so the reload of node 1's first acquisition lists it.
    trio.bootstrap();
    trio.ready();
    trio.until("node 1's A1 serves partition 2", |trio| {
        trio.nodes[0]
            .0
            .authority_views
            .contains_key(&PartitionId(2))
            .then_some(())
    });
    trio.links.drop_direction(NodeId(2), NodeId(1));
    trio.links.drop_direction(NodeId(3), NodeId(1));
    let unacknowledged = trio.call(put(1, b"a", b"1", None));
    trio.until("node 1 applies seq 2", |trio| {
        (trio.head() == Some(2)).then_some(())
    });
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    // `step` hands the mailbox to the host after it drains, so the last `until` can leave
    // partition-1 events queued. Drained now, before the expiry, none of them can find it
    // (under 2x load one did, and sited the fence at partition 1).
    trio.nodes[0].0.drain();
    let through = trio.nodes[0]
        .0
        .authority_views
        .get(&PartitionId(1))
        .map(|view| view.valid_through_tick)
        .expect("node 1 holds a view for partition 1");
    // Not stepped, so no partition-1 timer can find the expiry first. The grant ends a renew
    // period after the view's horizon at most; 700 ms is past both.
    while trio.clock.now() <= through.plus_millis(700) {
        std::thread::sleep(Duration::from_millis(5));
    }
    let host = &mut trio.nodes[0].0;
    assert_eq!(host.queue.len(), 0, "nothing but the flush duty can run");
    host.flush_duty().expect("the flush duty");
    host.drain();
    let status = host.status(PartitionId(1));
    assert_eq!(
        (status.fence, status.waits_on, status.admits),
        (Some(DenyReason::Expired), Some(Seq(2)), Some(false)),
        "partition 1's queue freezes on the unpublished put: {status:?}"
    );
    let p2_view = host.authority_views.get(&PartitionId(2)).copied();
    assert_eq!(
        p2_view.map(|view| view.past_horizon),
        Some(DenyReason::Expired),
        "A1's fence superseded partition 2's view too: {p2_view:?}"
    );
    assert_eq!(
        (
            host.publication
                .kernel(NodeId(1), PartitionId(2))
                .map(|kernel| kernel.view().mode),
            host.transaction.kernel(NodeId(1), PartitionId(2)).is_some(),
            host.fault.as_ref(),
        ),
        (
            Some(rdb_core::contracts::publication::PubMode::Frozen {
                cause: FreezeCause::AuthorityLost(DenyReason::Expired)
            }),
            false,
            None
        ),
        "partition 2's P1 (made by A1's view) took the fence; no T1 was made; no fault"
    );
    let fences: Vec<_> = logged!(
        "a_node_fence_sited_at_partition_0_freezes_partition_1s_queue",
        "authority_fence"
    )
    .iter()
    .map(|line| line["partition"].as_u64())
    .collect();
    assert_eq!(fences, [Some(0)], "the one fence was sited at partition 0");
    drop(unacknowledged);
}

/// A held link buffers; a stopped peer cannot. A send to a node whose mailbox is gone is
/// logged, never dropped in silence, and the primary neither faults nor stops taking
/// writes: copy 1 still acknowledges. The `SendFailed` event it also queues is not asserted:
/// no M9 kernel module consumes it yet.
#[config_log::retcd_test]
fn a_send_to_a_stopped_peer_is_logged_and_writes_go_on() {
    const METHOD: &str = "a_send_to_a_stopped_peer_is_logged_and_writes_go_on";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    // Node 3 stops: its mailbox is gone, so a send to it fails instead of being buffered.
    drop(std::mem::replace(&mut trio.nodes[2].2, mpsc::channel().1));
    match trio.ask(put(1, b"a", b"1", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("put a=1 with node 3 stopped: {other:?}"),
    }
    assert!(
        trio.nodes[0].0.fault.is_none(),
        "{:?}",
        trio.nodes[0].0.fault
    );

    let failed = logged!(METHOD, "send_failed");
    assert!(
        failed
            .iter()
            .any(|line| line["node"] == 1 && line["to"] == 3),
        "node 1's send to node 3 is reported: {failed:?}"
    );
}

/// The stuck-cursor warning (lead, after D4). Link 1-3 is held once the partition is
/// active, so copy 2 (node 3) never acknowledges the put's record and R1 re-sends it every
/// 100 ms. After `stuck_resends` re-sends the host warns once, naming copy 2; the re-sends
/// after that do not repeat it, and copy 1, which acknowledged, is never named.
/// Integration (~1.4 s): R1's retransmit runs on its fixed 100 ms timer.
#[config_log::retcd_test]
fn a_copy_that_never_acknowledges_a_record_is_reported_once() {
    const METHOD: &str = "a_copy_that_never_acknowledges_a_record_is_reported_once";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.stuck_resends = 3;
    });
    trio.bootstrap();
    trio.ready();
    trio.links.hold(NodeId(1), NodeId(3));
    match trio.ask(put(1, b"a", b"1", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("put a=1: {other:?}"),
    }
    trio.until("twice the re-sends that report a copy", |trio| {
        let resends = trio.nodes[0].0.resends.get(&(PartitionId(1), CopyId(2)));
        resends.filter(|entry| entry.resends >= 6).map(drop)
    });

    let warned = not_advancing!(METHOD);
    assert_eq!(warned.len(), 1, "one line per episode: {warned:?}");
    let line = &warned[0];
    assert_eq!(
        (
            &line["@l"],
            &line["partition"],
            &line["copy"],
            &line["through"]
        ),
        (&"Warning".into(), &1.into(), &2.into(), &2.into()),
        "{line}"
    );
}

/// Mutant M13: a read reply must match the step view it was served from, version and digest,
/// or the node faults rather than answer. P1 computes the digest from that same view, so no
/// client call reaches a mismatch; the row hands node 1 a forged reply instead.
#[test]
fn a_read_reply_that_does_not_match_the_step_view_faults_instead_of_answering() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    match trio.ask(put(1, b"a", b"1", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("put a=1: {other:?}"),
    }
    match trio.ask(get(2, b"a")) {
        Answer::Read { seen, .. } => {
            assert_eq!(doc_bytes(seen), Some((2, Bytes::from_static(b"1"))))
        }
        other => panic!("get a: {other:?}"),
    }

    let host = &mut trio.nodes[0].0;
    let root = root_key(TenantId(1), AFFINITY, b"a");
    let forged = Digest::of(Domain::ReadValue, &[root.as_bytes(), b"forged"]);
    let (reply, answer) = mpsc::channel();
    host.pending.insert(
        identity(3),
        Pending {
            reply,
            kind: PendingKind::Read(root, Look::Doc),
            event: EventId(0),
            since: Tick(0),
        },
    );
    let fault = host
        .reply(
            PartitionId(1),
            &ReplyEffect::Read {
                identity: identity(3),
                outcome: ReadServiceOutcome::Served,
                value: Some((2, forged)),
            },
        )
        .expect_err("a forged digest faults the node");
    assert!(
        fault.detail.contains("does not match the step view"),
        "{fault:?}"
    );
    // The waiting read is told the host faulted, at once, not left to time out as "no read
    // answer in 1s" (lead ruling 3, 2026-10-07).
    match answer.try_recv() {
        Ok(Answer::Error {
            error,
            request: None,
        }) => {
            assert_eq!(error.kind, ErrorKind::Unavailable, "{error:?}");
            assert_eq!(error.detail, format!("host fault: {}", fault.detail));
        }
        other => panic!("the waiting read is answered with the fault: {other:?}"),
    }
}

/// The answer a call's waiter holds right after `handle` returns: `None` when the waiter
/// was dropped unanswered, as a `Db` would then wait out its timeout.
fn answered_now(answer: &Receiver<Answer>) -> Option<Answer> {
    match answer.try_recv() {
        Ok(answer) => Some(answer),
        Err(mpsc::TryRecvError::Disconnected) => None,
        Err(mpsc::TryRecvError::Empty) => panic!("the waiter is neither answered nor dropped"),
    }
}

/// F-001 (S0 review): a put or compile whose step view cannot be built faults the node.
/// Its own caller hears that at once, as `set_fault` tells every pending call; dropped, it
/// would wait out the `Db`'s timeout and read as "no answer". Both arms that compile.
#[test]
fn a_call_whose_compile_faults_the_node_is_answered_with_the_host_fault() {
    for (name, call) in [
        ("put", put(1, b"a", b"1", None)),
        ("compile", compile(1, b"a", b"1")),
    ] {
        let dir = config_testkit::fs::temp_dir();
        let mut trio = Trio::new(dir.path(), Trio::fast);
        trio.bootstrap();
        trio.ready();
        let host = &mut trio.nodes[0].0;
        host.fail_step_view = true;
        let (reply, answer) = mpsc::channel();
        host.handle(Msg::Client(Client {
            partition: PartitionId(1),
            call,
            reply,
        }));
        let fault = host.fault.clone().expect("the node faulted");
        assert!(fault.detail.contains("injected"), "{name}: {fault:?}");
        match answered_now(&answer) {
            Some(Answer::Error {
                error,
                request: None,
            }) => assert_eq!(
                (error.retry, error.detail),
                (RetryRule::NotWired, format!("host fault: {}", fault.detail)),
                "{name}"
            ),
            other => panic!("{name}: the caller is told the host faulted: {other:?}"),
        }
    }
}

/// S1-8, admits at fault: a faulted node answers every call with its fault, so its status
/// never says it admits writes, though L1 still allows and A1 still holds its grant.
/// Before, `admits` read only L1 and A1 and said `true` beside `fault`.
#[test]
fn a_faulted_node_does_not_say_it_admits_writes() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let host = &mut trio.nodes[0].0;
    host.fail_step_view = true;
    let (reply, _answer) = mpsc::channel();
    host.handle(Msg::Client(Client {
        partition: PartitionId(1),
        call: put(1, b"a", b"1", None),
        reply,
    }));
    let status = trio.status(0);
    assert!(status.fault.is_some(), "the node faulted: {status:?}");
    assert_eq!(status.admits, Some(false), "{status:?}");
}

/// A-R178, the pending loop: a put already dispatched, waiting on its copies, when the
/// node faults on stored damage, is answered with that damage: `CORRUPT_HISTORY`, retry
/// `Quarantine`, its request named, and `no_mutation` false as before, since it was sent.
/// Only node 1 steps, so no copy acks and the put stays pending.
#[test]
fn a_dispatched_put_is_refused_with_the_damage_its_node_faulted_on() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let answer = trio.call(put(1, b"a", b"1", None));
    let (host, _, rx) = &mut trio.nodes[0];
    let deadline = Instant::now() + test_patience(Duration::from_secs(5));
    while host.pending.is_empty() {
        assert!(Instant::now() < deadline, "the put was never dispatched");
        while let Ok(msg) = rx.try_recv() {
            host.handle(msg);
        }
        host.drain();
    }
    assert!(answer.try_recv().is_err(), "the put is still waiting");
    host.set_fault(Fault {
        damage: Some(ErrorKind::CorruptHistory),
        detail: "a damaged read".to_owned(),
    });
    match answer.try_recv() {
        Ok(Answer::Error {
            error,
            request: Some(_),
        }) => assert_eq!(
            (error.kind, error.retry, error.no_mutation),
            (ErrorKind::CorruptHistory, RetryRule::Quarantine, false),
            "{error:?}"
        ),
        other => panic!("the pending put is answered with the damage: {other:?}"),
    }
}

/// F-001 (S0 review): a reply whose kind does not answer the waiting call faults the node,
/// and that call is told so at once. The same for P1's previous-view answer, to a call that
/// did not ask for one or under a handle storage never bound. The replies are forged: no
/// client call reaches these arms once one identity has one waiter (F-002).
#[test]
fn a_reply_the_waiting_call_cannot_take_answers_it_with_the_host_fault() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    let host = &mut trio.nodes[0].0;
    let root = root_key(TenantId(1), AFFINITY, b"a");
    let cases: [(&str, PendingKind); 3] = [
        // A status query is never a pending waiter since F-014, so the read reply meets a
        // previous-view call instead.
        (
            "read reply to a previous-view call",
            PendingKind::Previous(root.clone()),
        ),
        (
            "previous view to a read",
            PendingKind::Read(root.clone(), Look::Doc),
        ),
        ("unbound previous view", PendingKind::Previous(root)),
    ];
    for (n, (name, kind)) in (1u64..).zip(cases) {
        let (reply, answer) = mpsc::channel();
        let waiter = Pending {
            reply,
            kind,
            event: EventId(0),
            since: Tick(0),
        };
        host.pending.insert(identity(n), waiter);
        let fault = if n == 1 {
            host.reply(
                PartitionId(1),
                &ReplyEffect::Read {
                    identity: identity(n),
                    outcome: ReadServiceOutcome::Served,
                    value: None,
                },
            )
        } else {
            host.previous_answer(PartitionId(1), identity(n), Ok(SnapshotHandle(7)))
        }
        .expect_err(name);
        match answered_now(&answer) {
            Some(Answer::Error {
                error,
                request: None,
            }) => assert_eq!(
                (error.retry, error.detail),
                (RetryRule::NotWired, format!("host fault: {}", fault.detail)),
                "{name}"
            ),
            other => panic!("{name}: the caller is told the host faulted: {other:?}"),
        }
    }
}

/// A waiter that has neither an answer nor been dropped.
fn still_waits(name: &str, waiter: &Receiver<Answer>) {
    match waiter.try_recv() {
        Err(mpsc::TryRecvError::Empty) => {}
        other => panic!("{name} still waits: {other:?}"),
    }
}

/// F-002 (S0 review): one identity, one waiter. A `Db` is shared across threads (`rdb_dev`'s
/// `put&`), so a second call can come under an identity whose first call still waits. It
/// queues behind that call and is submitted once that call is answered: every caller gets
/// its own answer, and no reply meets a call it cannot answer. Before, the newer call
/// replaced the waiter, so the older caller was answered by nobody, and a put over a read
/// waiting at the barrier faulted the node.
///
/// - (a) Behind a put in flight: the same request resent, the same request resent by a
///   caller that has already given up, and a changed payload. A status query under the
///   same identity does not queue (F-014, round 2 R1): it is answered at once, `Unknown`
///   while the put is unpublished, and the put still waits. The changed
///   payload still ends in `REQUEST_ID_REUSE`, but only once the put is answered (here, at
///   the heal): queued, it waits for the first call's answer, where before it was refused
///   at once. The given-up caller's resend is still submitted, as its `UNKNOWN_OUTCOME`
///   allows, and publishes nothing more: the kernel de-duplicates it. A compile under the
///   waiting identity does not queue either (S1 guard G11): it sends nothing, and it is
///   how `retry N --payload` starts while request N still waits.
/// - (b) A put under the identity of a read waiting at the barrier.
/// - (c) A host fault answers the waiting call and every call queued behind it (F-001).
#[config_log::retcd_test]
fn a_second_call_under_a_waiting_identity_queues_and_each_caller_gets_its_own_answer() {
    const METHOD: &str =
        "a_second_call_under_a_waiting_identity_queues_and_each_caller_gets_its_own_answer";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    match trio.ask(put(1, b"a", b"1", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("put a=1: {other:?}"),
    }
    let hold = |trio: &Trio| {
        trio.links.hold(NodeId(1), NodeId(2));
        trio.links.hold(NodeId(1), NodeId(3));
    };

    // (a)
    let compiled = match trio.ask(compile(10, b"a", b"2")) {
        Answer::Compiled(request) => request,
        other => panic!("compile request 10: {other:?}"),
    };
    hold(&trio);
    let first = trio.call(ClientCall::Resend {
        request: compiled.clone(),
    });
    trio.until("node 1 applied request 10", |trio| {
        (trio.head() == Some(3)).then_some(())
    });
    match trio.ask(ClientCall::Status {
        identity: identity(10),
        generation: None,
    }) {
        Answer::Status(TxnStatus::Unknown) => {}
        other => panic!("status of request 10 while it waits: {other:?}"),
    }
    let recompiled = trio.call(compile(10, b"a", b"3"));
    for _ in 0..3 {
        trio.step();
    }
    match recompiled.try_recv() {
        Ok(Answer::Compiled(request)) => assert_eq!(request.identity, identity(10)),
        other => panic!("a compile under the waiting request 10 is not queued: {other:?}"),
    }
    let same = trio.call(ClientCall::Resend {
        request: compiled.clone(),
    });
    drop(trio.call(ClientCall::Resend { request: compiled }));
    let changed = trio.call(put(10, b"a", b"other", None));
    for _ in 0..3 {
        trio.step();
    }
    for (name, waiter) in [
        ("the put", &first),
        ("the same request", &same),
        ("the changed payload", &changed),
    ] {
        still_waits(name, waiter);
    }
    trio.links.heal_all();
    match trio.answer(&first) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3)),
        other => panic!("request 10: {other:?}"),
    }
    match trio.answer(&same) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3), "the same transaction"),
        other => panic!("request 10 resent: {other:?}"),
    }
    match trio.answer(&changed) {
        Answer::Error { error, .. } => assert_eq!(error.name(), "REQUEST_ID_REUSE"),
        other => panic!("request 10 with another value: {other:?}"),
    }
    let calls = |request: u64| {
        logged!(METHOD, "client_call")
            .iter()
            .filter(|line| line["request"] == request)
            .count()
    };
    trio.until("the given-up resend was submitted", |_| {
        (calls(10) == 5).then_some(())
    });
    for _ in 0..3 {
        trio.step();
    }
    assert_eq!(trio.head(), Some(3), "request 10 published once");

    // (b)
    // A-R124d: put 20 needs node 1's grant, which can lapse after (a)'s hold. No retention
    // or deadline here depends on the time.
    trio.admits_for(1_000);
    hold(&trio);
    let put_b = trio.call(put(20, b"b", b"1", None));
    trio.until("node 1 applied request 20", |trio| {
        (trio.head() == Some(4)).then_some(())
    });
    let read = trio.call(get(21, b"b"));
    trio.until("node 1 took the read", |trio| {
        trio.nodes[0]
            .0
            .pending
            .contains_key(&identity(21))
            .then_some(())
    });
    trio.step();
    let over = trio.call(put(21, b"c", b"1", None));
    for _ in 0..3 {
        trio.step();
    }
    for (name, waiter) in [("put b", &put_b), ("the read", &read), ("put c", &over)] {
        still_waits(name, waiter);
    }
    trio.links.heal_all();
    match trio.answer(&put_b) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(4)),
        other => panic!("put b: {other:?}"),
    }
    match trio.answer(&read) {
        Answer::Read { outcome, seen, .. } => assert_eq!(
            (outcome, doc_bytes(seen)),
            (
                ReadServiceOutcome::WaitedAtBarrier,
                Some((4, Bytes::from_static(b"1")))
            )
        ),
        other => panic!("the read at the barrier: {other:?}"),
    }
    // Sent the moment the read is answered, which can still be inside L1's resume hold
    // after the heal: then the kernel refuses it, and that refusal is its own answer.
    match trio.answer(&over) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(5)),
        Answer::Error { error, .. } => assert_eq!(error.name(), "PROTECTION_PAUSED"),
        other => panic!("put c under the read's identity: {other:?}"),
    }

    // (c)
    trio.ready();
    let head = trio.head().expect("node 1 holds the partition");
    hold(&trio);
    let put_d = trio.call(put(30, b"d", b"1", None));
    trio.until("node 1 applied request 30", |trio| {
        (trio.head() == Some(head + 1)).then_some(())
    });
    let queued = trio.call(get(30, b"d"));
    for _ in 0..3 {
        trio.step();
    }
    still_waits("the get behind put d", &queued);
    let host = &mut trio.nodes[0].0;
    host.fail_step_view = true;
    let (reply, faulting) = mpsc::channel();
    host.handle(Msg::Client(Client {
        partition: PartitionId(1),
        call: put(31, b"e", b"1", None),
        reply,
    }));
    assert!(host.fault.is_some(), "the node faulted");
    for (name, waiter) in [
        ("put d", &put_d),
        ("the get behind it", &queued),
        ("the faulting put", &faulting),
    ] {
        match waiter.try_recv() {
            Ok(Answer::Error { error, .. }) => {
                assert_eq!(error.retry, RetryRule::NotWired, "{name}: {error:?}");
            }
            other => panic!("{name} is told the host faulted: {other:?}"),
        }
    }
    assert_eq!(logged!(METHOD, "client_call_queued").len(), 5);
}

/// F-002's bound: past [`MAX_QUEUED_PER_IDENTITY`] calls queued under one identity, the next
/// is refused at once and nothing of it is sent. A write past the bound is `UNKNOWN_OUTCOME`
/// (round 2 R2): the call it queued behind may yet publish, so the caller asks `status`. A
/// resend carries its request back; a put was never compiled, so it has none (the `Db`
/// sends every put as a resend). A read past the bound stays `INVALID_ARGUMENT`. The queued
/// resends are each answered with the one transaction once the heal lets it publish, and
/// only they and the first were ever sent.
#[config_log::retcd_test]
fn a_call_past_the_queue_bound_for_its_identity_is_refused_and_sends_nothing() {
    const METHOD: &str =
        "a_call_past_the_queue_bound_for_its_identity_is_refused_and_sends_nothing";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let compiled = match trio.ask(compile(1, b"a", b"1")) {
        Answer::Compiled(request) => request,
        other => panic!("compile request 1: {other:?}"),
    };
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    let resend = || ClientCall::Resend {
        request: compiled.clone(),
    };
    let first = trio.call(resend());
    trio.until("node 1 applied request 1", |trio| {
        (trio.head() == Some(2)).then_some(())
    });
    let queued: Vec<_> = (0..MAX_QUEUED_PER_IDENTITY)
        .map(|_| trio.call(resend()))
        .collect();
    let past_resend = trio.call(resend());
    let past_put = trio.call(put(1, b"a", b"1", None));
    let past_get = trio.call(get(1, b"a"));
    for _ in 0..3 {
        trio.step();
    }
    for (name, waiter, request) in [
        ("a resend", &past_resend, Some(compiled.clone())),
        ("a put", &past_put, None),
    ] {
        match waiter.try_recv() {
            Ok(Answer::Error {
                error,
                request: got,
            }) => {
                assert_eq!(
                    (error.name(), error.retry, error.no_mutation),
                    ("UNKNOWN_OUTCOME".to_owned(), RetryRule::QueryStatus, false),
                    "{name}: {error:?}"
                );
                assert_eq!(got, request, "{name}");
            }
            other => panic!("{name} past the bound is refused at once: {other:?}"),
        }
    }
    match past_get.try_recv() {
        Ok(Answer::Error {
            error,
            request: None,
        }) => assert_eq!(
            (error.name(), error.retry, error.no_mutation),
            ("INVALID_ARGUMENT".to_owned(), RetryRule::Definitive, true),
            "{error:?}"
        ),
        other => panic!("a get past the bound is refused at once: {other:?}"),
    }
    still_waits("the first resend", &first);
    for waiter in &queued {
        still_waits("a queued resend", waiter);
    }
    trio.links.heal_all();
    for waiter in std::iter::once(&first).chain(&queued) {
        match trio.answer(waiter) {
            Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
            other => panic!("request 1: {other:?}"),
        }
    }
    let sent = logged!(METHOD, "client_call")
        .into_iter()
        .filter(|line| line["request"] == 1)
        .count();
    assert_eq!(
        sent,
        1 + MAX_QUEUED_PER_IDENTITY,
        "the first call and the queued ones; nothing past the bound"
    );
}

/// F-014 (round 2 R1): P1 may withhold a published put's reply for good (a Reply check A1
/// denies, a fence, a new lineage), and nothing else answers that waiter. Before, the waiter
/// held its identity forever and every later call under it queued behind, the status query
/// included: the one call that resolves an `UNKNOWN_OUTCOME` could never be answered.
/// Now a status query never queues behind a write, and a put's waiter expires at its
/// request's deadline plus [`TXN_WAITER_MARGIN_MILLIS`] as `UNKNOWN_OUTCOME`. Here A1
/// denies the Reply check: the status query answers `Resolved` at the put's seq while the
/// put still waits, the put expires, the resend queued behind it is released and gets the
/// published result again, and so does a resend after. The node never faults (`Trio::step`
/// checks).
#[config_log::retcd_test]
fn a_put_whose_reply_p1_withholds_expires_and_its_status_is_answered_meanwhile() {
    const METHOD: &str =
        "a_put_whose_reply_p1_withholds_expires_and_its_status_is_answered_meanwhile";
    const REMAINING_MILLIS: u64 = 1_000;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let compiled = match trio.ask(ClientCall::Compile {
        identity: identity(1),
        puts: vec![PutOp {
            group: AFFINITY,
            object: Bytes::from_static(b"a"),
            change: byte_doc(b"1"),
            expect: None,
        }],
        remaining_millis: REMAINING_MILLIS,
    }) {
        Answer::Compiled(request) => request,
        other => panic!("compile request 1: {other:?}"),
    };
    trio.nodes[0].0.deny_reply_checks = true;
    let sent = Instant::now();
    let put = trio.call(ClientCall::Resend {
        request: compiled.clone(),
    });
    trio.until("node 1 applied request 1", |trio| {
        (trio.head() == Some(2)).then_some(())
    });
    // Asked until P1 has published: before, its index answers `Unknown`. Each is answered
    // while the put still waits, which a status queued behind the put never is.
    let deadline = Instant::now() + test_patience(Trio::PATIENCE);
    let status = loop {
        let answer = trio.ask(ClientCall::Status {
            identity: identity(1),
            generation: None,
        });
        still_waits("the put whose reply P1 withheld", &put);
        match answer {
            Answer::Status(TxnStatus::Resolved(result)) => break result,
            Answer::Status(TxnStatus::Unknown | TxnStatus::Unresolved { .. }) => {}
            other => panic!("status of request 1: {other:?}"),
        }
        assert!(Instant::now() < deadline, "request 1 never resolved");
    };
    assert_eq!(status.seq, Seq(2), "the status is the put's");
    // P1 has already withheld the put's reply, so clearing the deny cannot revive it; it lets
    // the resend queued behind the put be answered once the expiry releases it.
    trio.nodes[0].0.deny_reply_checks = false;
    let behind = trio.call(ClientCall::Resend {
        request: compiled.clone(),
    });
    for _ in 0..3 {
        trio.step();
    }
    still_waits("the put whose reply P1 withheld", &put);
    still_waits("the resend queued behind it", &behind);

    let expired = trio.answer(&put);
    assert!(
        sent.elapsed() >= Duration::from_millis(REMAINING_MILLIS),
        "expired after {:?}, inside the request's deadline",
        sent.elapsed()
    );
    match expired {
        Answer::Error { error, request } => {
            assert_eq!(
                (error.name(), error.retry, error.no_mutation),
                ("UNKNOWN_OUTCOME".to_owned(), RetryRule::QueryStatus, false),
                "{error:?}"
            );
            assert_eq!(request, Some(compiled.clone()), "the request comes back");
        }
        other => panic!("the withheld put expires: {other:?}"),
    }
    let logged = logged!(METHOD, "txn_waiter_expired");
    assert_eq!(logged.len(), 1, "{logged:?}");
    assert_eq!(logged[0]["request"], 1, "{}", logged[0]);
    // The expiry frees the identity: the queued resend goes out and gets the replay.
    match trio.answer(&behind) {
        Answer::Txn { result, .. } => assert_eq!(result, status, "the queued resend"),
        other => panic!("the resend queued behind the withheld put: {other:?}"),
    }
    let dequeued = logged!(METHOD, "client_call_dequeued")
        .into_iter()
        .filter(|line| line["request"] == 1)
        .count();
    assert_eq!(dequeued, 1, "the queued resend was released by the expiry");

    // A stall on this thread past the grant lapses it (#50): ask with time left on it.
    trio.admits_for(1_000);
    match trio.ask(ClientCall::Resend { request: compiled }) {
        Answer::Txn { result, .. } => assert_eq!(result, status, "the replayed result"),
        other => panic!("request 1 resent after expiry: {other:?}"),
    }
    assert_eq!(trio.head(), Some(2), "request 1 published once");
}

/// F-014's guard (review leftover): an expiry meets only the waiter it was scheduled for.
/// Put A is answered before its deadline (a forged P1 refusal, as the rows above forge
/// replies), which releases resend B, queued behind it under the same identity. A's expiry
/// is still due and fires while B waits: it must leave B alone. Nothing is drained, so no
/// module answers B; only the host's own duties run.
#[config_log::retcd_test]
fn an_expiry_never_expires_the_call_released_after_its_put() {
    const METHOD: &str = "an_expiry_never_expires_the_call_released_after_its_put";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    let host = &mut trio.nodes[0].0;
    let request = |remaining_millis| TxnRequest {
        api_version: API_VERSION,
        identity: identity(1),
        affinity: AFFINITY,
        expected_generation: Some(Generation(1)),
        remaining_millis,
        conditions: Vec::new(),
        mutations: Vec::new(),
        acked_below: None,
        pinned: Vec::new(),
    };
    let send = |host: &mut Host, remaining_millis| {
        let (reply, answer) = mpsc::channel();
        host.client(Client {
            partition: PartitionId(1),
            call: ClientCall::Resend {
                request: request(remaining_millis),
            },
            reply,
        })
        .expect("sent");
        answer
    };
    // A's deadline is 0, so its expiry is due after the margin alone; B's is out of reach.
    let a = send(host, 0);
    let a_event = host.pending[&identity(1)].event;
    let b = send(host, 60_000);
    still_waits("resend B, queued behind A", &b);
    host.reply(
        PartitionId(1),
        &ReplyEffect::Failed {
            identity: identity(1),
            error: RdbError::UnknownOutcome {
                partition: PartitionId(1),
                identity: identity(1),
            },
        },
    )
    .expect("A is answered");
    match a.try_recv() {
        Ok(Answer::Error { error, .. }) => assert_eq!(error.name(), "UNKNOWN_OUTCOME"),
        other => panic!("put A: {other:?}"),
    }
    let b_event = host.pending[&identity(1)].event;
    assert_ne!(a_event, b_event, "B, released, is the waiter now");

    let a_due = |host: &Host| {
        host.delayed
            .values()
            .any(|item| matches!(item, Delayed::TxnExpiry { event, .. } if *event == a_event))
    };
    assert!(a_due(host), "A's expiry is still due");
    let deadline = Instant::now() + test_patience(Trio::PATIENCE);
    while a_due(host) {
        host.run_due();
        assert!(Instant::now() < deadline, "A's expiry never fired");
        std::thread::sleep(Duration::from_millis(1));
    }
    still_waits("resend B, after A's expiry fired", &b);
    assert_eq!(
        host.pending.get(&identity(1)).map(|waiter| waiter.event),
        Some(b_event)
    );
    assert_eq!(logged!(METHOD, "txn_waiter_expired").len(), 0);
    assert_eq!(host.fault, None);
}

/// M9 S3-2: the owner publishes a write after its caller stopped waiting. The answer has
/// nowhere to go, so the host logs `reply_undelivered` once, naming the node and request,
/// and does not fault; `status` with the same id then settles it at the seq it published.
/// Trio (~2 s): the hold keeps the write unpublished until the caller is gone.
#[config_log::retcd_test]
fn a_write_published_after_its_caller_left_is_logged_undelivered_and_status_settles_it() {
    const METHOD: &str =
        "a_write_published_after_its_caller_left_is_logged_undelivered_and_status_settles_it";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    match trio.ask(put(1, b"a", b"1", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("put a=1: {other:?}"),
    }
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    drop(trio.call(put(2, b"a", b"2", None)));
    trio.until("node 1 applied request 2", |trio| {
        (trio.head() == Some(3)).then_some(())
    });
    assert!(
        logged!(METHOD, "reply_undelivered").is_empty(),
        "not yet published"
    );

    trio.links.heal_all();
    let lines = trio.until("reply_undelivered", |_| {
        let lines = logged!(METHOD, "reply_undelivered");
        (!lines.is_empty()).then_some(lines)
    });
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert!(
        lines[0]["node"] == 1 && lines[0]["request"] == 2,
        "{lines:?}"
    );
    // A-R124d: after a 2 s-scale hold node 1's grant can lapse, and a fenced node answers
    // `status` `Unknown`. The default retention is a day, so the wait moves nothing past it.
    trio.admits_for(1_000);
    match trio.ask(ClientCall::Status {
        identity: identity(2),
        generation: None,
    }) {
        Answer::Status(TxnStatus::Resolved(result)) => {
            assert_eq!((result.generation, result.seq), (Generation(1), Seq(3)));
        }
        other => panic!("status of request 2: {other:?}"),
    }
}

/// M9 S3-5: the owner's trim duty lets a published write's retained answer go once it is
/// `dedup_retention_millis` old, never before (a 1.1 s look covers a trim tick, so a duty
/// that ignored the age would have trimmed). One `below` = the last aged seq + 1 covers
/// both writes, sent to T1 and P1 together: afterwards `status` is `Unknown` for both, and
/// a resend of the first is not replayed from the dedup index but runs afresh, where its
/// `Absent` condition fails. Only node 1 trims. Trio (~4 s): a 1.5 s retention.
#[config_log::retcd_test]
fn the_trim_duty_drops_both_retained_answers_once_they_age_and_not_before() {
    const METHOD: &str = "the_trim_duty_drops_both_retained_answers_once_they_age_and_not_before";
    const RETENTION_MILLIS: u64 = 1_500;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.dedup_retention_millis = RETENTION_MILLIS;
    });
    trio.bootstrap();
    // A-R124b: the status and resend inside the 1.5 s retention need the grant for about
    // 1.2 s more, and no wait can come between them without passing the retention.
    trio.admits_for(RETENTION_MILLIS);
    let first = match trio.ask(compile(1, b"a", b"1")) {
        Answer::Compiled(request) => request,
        other => panic!("compile request 1: {other:?}"),
    };
    let resend = |trio: &mut Trio| {
        trio.ask(ClientCall::Resend {
            request: first.clone(),
        })
    };
    match resend(&mut trio) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("request 1: {other:?}"),
    }
    match trio.ask(put(2, b"b", b"1", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3)),
        other => panic!("request 2: {other:?}"),
    }
    let status = |trio: &mut Trio, request| {
        trio.ask(ClientCall::Status {
            identity: identity(request),
            generation: None,
        })
    };

    let looked = trio.clock.now().plus_millis(1_100);
    trio.until("1.1 s, a trim tick", |trio| {
        (trio.clock.now() >= looked).then_some(())
    });
    match status(&mut trio, 1) {
        Answer::Status(TxnStatus::Resolved(result)) => assert_eq!(result.seq, Seq(2)),
        other => panic!("status 1 inside the retention: {other:?}"),
    }
    match resend(&mut trio) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2), "replayed"),
        other => panic!("resend 1 inside the retention: {other:?}"),
    }

    let trims = trio.until("node 1 trims below 4", |_| {
        let trims = logged!(METHOD, "host_duty")
            .into_iter()
            .filter(|line| line["kind"] == "trim")
            .collect::<Vec<_>>();
        trims.iter().any(|line| line["below"] == 4).then_some(trims)
    });
    assert!(
        trims
            .iter()
            .all(|line| line["node"] == 1 && line["generation"] == 1),
        "{trims:?}"
    );
    assert_eq!(
        trims.last().map(|line| line["below"] == 4),
        Some(true),
        "{trims:?}"
    );
    // A-R124b: node 1's grant can lapse and fence it here; a fenced node answers `Unknown`
    // and refuses the resend `LEASE_EXPIRED`. Both are trimmed already, so waiting only ages them.
    trio.admits_for(1_000);
    for request in [1, 2] {
        match status(&mut trio, request) {
            Answer::Status(TxnStatus::Unknown) => {}
            other => panic!("status {request} after the trim: {other:?}"),
        }
    }
    match resend(&mut trio) {
        Answer::Error { error, .. } => {
            assert_eq!(error.kind, ErrorKind::ConditionFailed, "{error:?}");
        }
        other => panic!("resend 1 after the trim runs afresh and fails: {other:?}"),
    }
}

/// Tester coverage ask (S3-5): the trim duty skips a partition with nothing published to
/// age and goes on to the next. Node 1 holds an empty publish queue for partition 0, which
/// sorts before partition 1, as an idle partition's does once its last entry aged out. The
/// write on partition 1 is still trimmed. A duty that stopped at the idle partition would
/// never trim partition 1. Trio (~4 s): a 300 ms retention, one trim tick past it.
#[config_log::retcd_test]
fn the_trim_duty_passes_an_idle_partition_and_trims_the_next() {
    const METHOD: &str = "the_trim_duty_passes_an_idle_partition_and_trims_the_next";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.dedup_retention_millis = 300;
    });
    trio.bootstrap();
    trio.ready();
    let owner = &mut trio.nodes[0].0;
    assert_eq!(owner.node, NodeId(1));
    owner.published_at.insert(PartitionId(0), VecDeque::new());
    match trio.ask(put(1, b"a", b"1", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("put a=1: {other:?}"),
    }
    let trims = trio.until("node 1 trims partition 1 below 3", |_| {
        let trims = logged!(METHOD, "host_duty")
            .into_iter()
            .filter(|line| line["kind"] == "trim")
            .collect::<Vec<_>>();
        trims.iter().any(|line| line["below"] == 3).then_some(trims)
    });
    assert!(
        trims
            .iter()
            .all(|line| line["node"] == 1 && line["partition"] == 1),
        "nothing trimmed for the idle partition: {trims:?}"
    );
}

/// D6 (ruling A-R172): the trim duty deletes trimmed Dedup rows on disk at most once per a
/// quarter of the retention, with the latest floor. Trim ticks every 50 ms and a 400 ms
/// retention make a 100 ms period; puts about 40 ms apart give a trim on most ticks. So
/// the deletes run, but on fewer ticks than the trims, and node 1's disk keeps fewer
/// generation-1 Dedup rows than it wrote. A delete on every trim fails the count.
/// Trio (~3 s): a 1.5 s run of puts on real trim ticks.
#[config_log::retcd_test]
fn the_durable_trim_delete_runs_at_most_once_per_period() {
    use rdb_core::transaction::dedup::parse_dedup_key;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.trim_every_millis = 50;
        host.budgets.dedup_retention_millis = 400;
        // One bucket a publish, as the trim cadence below assumes.
        host.publish_bucket_millis = 1;
    });
    trio.bootstrap();
    trio.ready();
    let until = trio.clock.now().plus_millis(1_500);
    let mut request = 1;
    let mut landed = 0_usize;
    while trio.clock.now() < until {
        match trio.ask(put(request, b"a", b"1", None)) {
            Answer::Txn { .. } => landed += 1,
            Answer::Error { error, .. } => {
                assert_ne!(
                    error.kind,
                    ErrorKind::Overloaded,
                    "put {request}: {error:?}"
                );
            }
            other => panic!("put {request}: {other:?}"),
        }
        request += 1;
        let next = trio.clock.now().plus_millis(40);
        trio.until("40 ms", |trio| (trio.clock.now() >= next).then_some(()));
    }
    let owner = &trio.nodes[0].0;
    let (trims, deletes) = (owner.trims_sent, owner.trim_deletes);
    assert!(trims >= 4, "trims {trims}, deletes {deletes}");
    assert!(
        deletes >= 1 && deletes < trims,
        "trims {trims}, deletes {deletes}: at most one delete per period"
    );
    let view = owner
        .engine
        .snapshot(PartitionId(1), Generation(1), STEP_VIEW)
        .expect("view");
    let held = view
        .scan(Namespace::Dedup, &[], usize::MAX)
        .into_iter()
        .filter(|(key, _)| parse_dedup_key(key).is_some_and(|key| key.generation == Generation(1)))
        .count();
    assert!(
        held < landed,
        "node 1 holds {held} generation-1 Dedup rows of {landed} puts that landed"
    );
}

/// A-R183 rows: the `(generation, below)` of every `DedupTrim` the host queued.
fn dedup_trims_queued(host: &Host) -> Vec<(u64, u64)> {
    host.queue
        .iter()
        .filter_map(|queued| match queued.event.kind {
            EventKind::Kernel(KernelEvent::DedupTrim { generation, below }) => {
                Some((generation.0, below.0))
            }
            _ => None,
        })
        .collect()
}

/// A-R183 rows: a reopened copy of partition 1 that served generation 1 through seq 7 and
/// now serves generation `serving`, linked onto it, with `marks` left by an earlier process.
fn reopened_with_marks(path: &std::path::Path, serving: u64, marks: &[Mark]) {
    let mut engine = RocksEngine::open(path).expect("open engine");
    if engine.lineages().is_empty() {
        write_chain(&mut engine, Generation(1), 7);
    }
    for to in 2..=serving {
        if !engine
            .lineages()
            .contains(&(PartitionId(1), Generation(to)))
        {
            engine
                .inherit(PartitionId(1), Generation(to - 1), Generation(to), Seq(7))
                .expect("inherit");
        }
    }
    engine
        .put_publish_marks(PartitionId(1), &publish_marks(marks))
        .expect("put marks");
}

/// A-R183 row 1, the tail (critic-d6b F1/F3), and A-R193: marks left by an earlier process
/// reach seq 5 of generation 1, published two retentions ago. The restore itself trims
/// seqs 1-5, before the landing's `Recovered`, with no trim tick, and nothing above: rows 6
/// and 7 were never marked, so they wait for the landing plus the retention, as before. The
/// floor keeps seq 5's tick read as later by the clock error (C5); restored out of order, the
/// trim would stop below seq 4.
#[test]
fn a_reopen_trims_what_the_marks_cover_at_restore_and_spares_the_tail() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    let now = HostClock::start().now();
    let retention = Budgets::SPEC_DEFAULTS.dedup_retention_millis;
    let old = Tick(now.0 - 2 * retention);
    reopened_with_marks(
        &path,
        2,
        &[
            (Generation(1), Seq(3), old),
            (Generation(1), Seq(5), old.plus_millis(10)),
        ],
    );
    let mut host = host_over(&path, &rt);
    host.restore_marks(PartitionId(1), Generation(2))
        .expect("restore");
    let epsilon = Budgets::SPEC_DEFAULTS.clock_error_millis;
    assert_eq!(
        dedup_trims_queued(&host),
        [(1, 6)],
        "one trim at restore, below seq 6"
    );
    assert_eq!(
        host.trim_floors[&(PartitionId(1), Generation(1))],
        (Seq(6), old.plus_millis(10 + epsilon)),
        "the floor is seq 5's tick read as later by the clock error (C5)"
    );
    assert!(
        host.published_at[&PartitionId(1)].is_empty(),
        "nothing marked is left to age"
    );
    host.trim_duty(host.clock.now()).expect("trim");
    assert_eq!(dedup_trims_queued(&host), [(1, 6)], "the tick adds no trim");
    assert_eq!(host.trim_deletes, 1, "the floor is deleted on disk too");
}

/// PR #60 review F2: marks of two generations, both a retention old, as several clean
/// reopens inside one window leave them. The restore trims both before the landing's
/// `Recovered`, a `DedupTrim` and a `StatusTrim` for each, with no trim tick. Before, the
/// sweep stopped at the first generation's end, and generation 2's entries counted against
/// the dedup cap until later ticks took them one generation each. Cut: `trim_aged` takes
/// one generation per sweep, and only generation 1 is queued.
#[test]
fn a_reopen_trims_the_aged_marks_of_every_generation_at_restore() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    let now = HostClock::start().now();
    let retention = Budgets::SPEC_DEFAULTS.dedup_retention_millis;
    let old = Tick(now.0 - 2 * retention);
    reopened_with_marks(
        &path,
        3,
        &[
            (Generation(1), Seq(5), old),
            (Generation(2), Seq(9), old.plus_millis(10)),
        ],
    );
    let mut host = host_over(&path, &rt);
    host.restore_marks(PartitionId(1), Generation(3))
        .expect("restore");
    assert_eq!(
        dedup_trims_queued(&host),
        [(1, 6), (2, 10)],
        "both generations trimmed at restore"
    );
    let status_trims: Vec<(u64, u64)> = host
        .queue
        .iter()
        .filter_map(|queued| match queued.event.kind {
            EventKind::Kernel(KernelEvent::StatusTrim { generation, below }) => {
                Some((generation.0, below.0))
            }
            _ => None,
        })
        .collect();
    assert_eq!(status_trims, [(1, 6), (2, 10)], "and both status floors");
    assert!(
        host.published_at[&PartitionId(1)].is_empty(),
        "nothing aged is left"
    );
}

/// A-R183 row 2: a store with no marks (a crash before the first write of marks, or data
/// from an older build) restores nothing, and the first trim takes nothing.
#[test]
fn a_reopen_with_no_marks_trims_nothing_on_its_first_tick() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    reopened_with_marks(&path, 2, &[]);
    let mut host = host_over(&path, &rt);
    host.restore_marks(PartitionId(1), Generation(2))
        .expect("restore");
    host.trim_duty(host.clock.now()).expect("trim");
    assert_eq!(dedup_trims_queued(&host), [], "nothing trimmed");
}

/// A-R183 row 3, the daily restart (critic-d6b F2): a middle process restores generation
/// 1's marks, publishes nothing, writes its marks and quits before they age. The next
/// reopen still holds them and, once they age, trims generation 1 at once. A process that
/// wrote only what it published itself would erase them, and generation 1 would wait for
/// a landing plus the retention after every restart.
#[test]
fn publish_marks_outlive_a_process_that_published_nothing() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    let now = HostClock::start().now();
    let retention = Budgets::SPEC_DEFAULTS.dedup_retention_millis;
    let period = trim_delete_period(retention);
    let young = Tick(now.0 - retention / 2);
    reopened_with_marks(&path, 2, &[(Generation(1), Seq(5), young)]);
    {
        let mut middle = host_over(&path, &rt);
        // A trim tick before the landing writes nothing: no marks were read yet.
        middle.trim_duty(now).expect("a tick before the landing");
        middle
            .restore_marks(PartitionId(1), Generation(2))
            .expect("restore");
        middle.trim_duty(now).expect("trim");
        middle
            .trim_duty(now.plus_millis(period + 1_000))
            .expect("a mark write");
        assert_eq!(
            dedup_trims_queued(&middle),
            [],
            "not aged in the middle process"
        );
    }
    RocksEngine::open(&path)
        .expect("open engine")
        .inherit(PartitionId(1), Generation(2), Generation(3), Seq(7))
        .expect("inherit");
    let mut next = host_over(&path, &rt);
    next.restore_marks(PartitionId(1), Generation(3))
        .expect("restore");
    next.trim_duty(young.plus_millis(retention + 1_000))
        .expect("trim");
    assert_eq!(
        dedup_trims_queued(&next),
        [(1, 6)],
        "generation 1 trimmed at once"
    );
}

/// A-R186: a clean stop writes the publish marks at once, inside the mark period. The host
/// restored its marks (none), then published seqs 6 and 7 of generation 1. Its stop through
/// `run` leaves both on disk, ticks verbatim (C1), with no trim tick in between. Without the
/// final write the disk keeps the empty set and a reopen ages them from its landing.
#[test]
fn a_clean_stop_writes_the_publish_marks_at_once() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    reopened_with_marks(&path, 2, &[]);
    let now = HostClock::start().now();
    let published = [
        (Generation(1), Seq(6), Tick(now.0 - 20)),
        (Generation(1), Seq(7), Tick(now.0 - 10)),
    ];
    {
        let mut host = host_over(&path, &rt);
        host.restore_marks(PartitionId(1), Generation(2))
            .expect("restore");
        host.published_at
            .entry(PartitionId(1))
            .or_default()
            .extend(published);
        let (tx, rx) = mpsc::channel();
        tx.send(Msg::Stop).expect("send stop");
        host.run(&rx);
    }
    let marks: Vec<Mark> = RocksEngine::open(&path)
        .expect("open engine")
        .publish_marks(PartitionId(1))
        .expect("read marks")
        .into_iter()
        .map(|mark| (mark.generation, mark.seq, mark.tick))
        .collect();
    assert_eq!(marks, published, "the stop wrote both marks verbatim");
}

/// A-R183, lead k205: rows trimmed in memory but not yet deleted on disk keep their mark. A
/// process trims generation 1 below 6 and quits inside the delete period, so seqs 1-5 are
/// still on disk. The next reopen trims them on its first tick, from their publish, not a
/// retention after its landing. Marks taken from the in-memory queue alone dropped them.
#[test]
fn rows_trimmed_but_not_yet_deleted_keep_their_mark_across_a_reopen() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    let now = HostClock::start().now();
    let retention = Budgets::SPEC_DEFAULTS.dedup_retention_millis;
    let period = trim_delete_period(retention);
    let old = Tick(now.0 - 2 * retention);
    reopened_with_marks(&path, 2, &[(Generation(1), Seq(5), old)]);
    {
        let mut first = host_over(&path, &rt);
        first
            .restore_marks(PartitionId(1), Generation(2))
            .expect("restore");
        // The last delete ran just now, so this process quits before the next one.
        first
            .trim_deleted_at
            .insert(PartitionId(1), now.plus_millis(1_000));
        first.trim_duty(now).expect("trim");
        first
            .trim_duty(now.plus_millis(period + 500))
            .expect("a mark write");
        assert_eq!(dedup_trims_queued(&first), [(1, 6)], "trimmed in memory");
        assert_eq!(first.trim_deletes, 0, "seqs 1-5 are still on disk");
    }
    RocksEngine::open(&path)
        .expect("open engine")
        .inherit(PartitionId(1), Generation(2), Generation(3), Seq(7))
        .expect("inherit");
    let mut next = host_over(&path, &rt);
    next.restore_marks(PartitionId(1), Generation(3))
        .expect("restore");
    next.trim_duty(next.clock.now()).expect("trim");
    assert_eq!(
        dedup_trims_queued(&next),
        [(1, 6)],
        "generation 1 trimmed on the first tick"
    );
}

/// A-R183 row 4 (critic-d6b C3): a mark of a lineage the engine does not hold, or of a
/// generation not below the one served, is not restored, and the node does not fault. A
/// restored mark of generation 1 here would send a durable delete for a lineage the engine
/// does not hold, which faults the node.
#[test]
fn publish_marks_of_a_lineage_not_held_or_not_older_are_not_restored() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    let now = HostClock::start().now();
    let old = Tick(now.0 - 2 * Budgets::SPEC_DEFAULTS.dedup_retention_millis);
    {
        let mut engine = RocksEngine::open(&path).expect("open engine");
        write_chain(&mut engine, Generation(2), 2);
        engine
            .put_publish_marks(
                PartitionId(1),
                &publish_marks(&[
                    (Generation(1), Seq(5), old),
                    (Generation(2), Seq(1), old),
                    (Generation(3), Seq(1), old),
                ]),
            )
            .expect("put marks");
    }
    let mut host = host_over(&path, &rt);
    host.restore_marks(PartitionId(1), Generation(2))
        .expect("restore");
    host.trim_duty(host.clock.now())
        .expect("no mark restored, so no delete to fault on");
    assert_eq!(dedup_trims_queued(&host), [], "nothing trimmed");
}

/// critic-d6b C6: floors of two generations of one partition trimmed inside one delete
/// period are both deleted on disk when the period ends. Keyed by partition alone, the
/// newer floor overwrote the older, and the older generation's rows stayed on disk.
#[test]
fn two_generations_trimmed_in_one_delete_period_are_both_deleted() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    reopened_with_marks(&path, 2, &[]);
    let mut host = host_over(&path, &rt);
    let now = host.clock.now();
    let retention = host.budgets.dedup_retention_millis;
    let old = Tick(now.0 - 2 * retention);
    host.published_at.insert(
        PartitionId(1),
        VecDeque::from([(Generation(1), Seq(3), old), (Generation(2), Seq(9), old)]),
    );
    host.trim_deleted_at.insert(PartitionId(1), now);
    host.trim_duty(now).expect("trim generation 1");
    host.trim_duty(now).expect("trim generation 2");
    assert_eq!(dedup_trims_queued(&host), [(1, 4), (2, 10)]);
    assert_eq!(host.trim_deletes, 0, "inside the period");
    host.trim_duty(now.plus_millis(trim_delete_period(retention)))
        .expect("the period ends");
    assert_eq!(host.trim_deletes, 2, "both floors deleted");
}

/// A-R183 row 5 (critic-d6b candidate 3): an `OVERLOADED` from a full dedup index says when
/// the trim duty can first free room: not before the first retire due or the oldest publish
/// one retention on, at the trim tick that runs it. Kind and retry rule stay the kernel's.
/// Trio (~2 s): a cap of 2 and a 5-minute retention, so nothing frees while it runs.
#[config_log::retcd_test]
fn a_dedup_cap_overload_says_when_the_cap_can_first_free() {
    const RETENTION: u64 = 300_000;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.dedup_retention_millis = RETENTION;
        host.transaction = Transaction::with_limits(Limits {
            dedup_cap: 2,
            ..Limits::default()
        });
    });
    trio.bootstrap();
    trio.ready();
    for (request, key) in [(1, b"a"), (2, b"b")] {
        match trio.ask(put(request, key, b"1", None)) {
            Answer::Txn { .. } => {}
            other => panic!("put {request}: {other:?}"),
        }
    }
    let error = match trio.ask(put(3, b"c", b"1", None)) {
        Answer::Error { error, .. } => error,
        other => panic!("put 3 on a full cap: {other:?}"),
    };
    let answered = trio.clock.now();
    assert_eq!(
        (error.kind, error.retry),
        (ErrorKind::Overloaded, RetryRule::BoundedJitter),
        "{error:?}"
    );
    let owner = &trio.nodes[0].0;
    let due = owner
        .retire_due
        .values()
        .copied()
        .chain(
            owner
                .published_at
                .get(&PartitionId(1))
                .and_then(VecDeque::front)
                .map(|&(_, _, at)| at.plus_millis(RETENTION)),
        )
        .min()
        .expect("a free is scheduled");
    let after = error.retry_after().expect("a not-before hint");
    assert!(
        answered.plus_millis(u64::try_from(after.as_millis()).expect("millis")) >= due,
        "the hint {after:?} names a time before the earliest free {due:?}"
    );
    assert!(
        after <= Duration::from_millis(RETENTION + owner.trim_every_millis),
        "the hint {after:?} is past one retention and a trim tick"
    );
    assert!(error.detail.contains("not before ~"), "{error:?}");
}

/// A-R187 (A-R209): a full dedup cap that nothing scheduled frees gives no hint, never an
/// invented one. Node 1 holds two scheduled frees that free nothing T1 holds. One is real:
/// the bootstrap's publish at seq 1, which carries no request, so T1 retained nothing for
/// it. The other is a retire due for the generation node 1 serves, below which no generation
/// holds a row. Before A-R187 each was named as the hint. Cut either half of the rule and
/// the hint comes back.
///
/// Constructed: T1's two entries are real puts, and the bucket is cut back to seq 1 by hand.
/// That stands in for entries no bucket covers (a condition failure T1 retains, A-R108),
/// which this Trio cannot retain at the cap without a publish; the retire due is set by
/// hand too. Trio (~1 s), a 5-minute retention.
#[config_log::retcd_test]
fn a_full_dedup_cap_that_nothing_scheduled_frees_gives_no_hint() {
    const RETENTION: u64 = 300_000;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.dedup_retention_millis = RETENTION;
        host.transaction = Transaction::with_limits(Limits {
            dedup_cap: 2,
            ..Limits::default()
        });
    });
    trio.bootstrap();
    trio.ready();
    for (request, key) in [(1, b"a"), (2, b"b")] {
        match trio.ask(put(request, key, b"1", None)) {
            Answer::Txn { .. } => {}
            other => panic!("put {request}: {other:?}"),
        }
    }
    let owner = &mut trio.nodes[0].0;
    let kernel = owner
        .transaction
        .kernel(owner.node, PartitionId(1))
        .expect("node 1 serves partition 1");
    assert_eq!(kernel.dedup().len(), 2, "the cap is full");
    let served = kernel.lineage().generation;
    let published = owner
        .published_at
        .get_mut(&PartitionId(1))
        .expect("the puts published");
    // The puts publish into the bootstrap's bucket (D8): cut it back to seq 1.
    let at = published.front().expect("a bucket").2;
    *published = VecDeque::from([(served, Seq(1), at)]);
    assert_eq!(
        published
            .iter()
            .map(|&(g, seq, _)| (g, seq))
            .collect::<Vec<_>>(),
        [(served, Seq(1))],
        "the bootstrap's publish is left"
    );
    let now = owner.clock.now();
    owner
        .retire_due
        .insert((PartitionId(1), served), now.plus_millis(2_000));

    let error = match trio.ask(put(3, b"c", b"1", None)) {
        Answer::Error { error, .. } => error,
        other => panic!("put 3 on a full cap: {other:?}"),
    };
    assert_eq!(
        (error.kind, error.retry),
        (ErrorKind::Overloaded, RetryRule::BoundedJitter),
        "{error:?}"
    );
    assert_eq!(error.retry_after(), None, "{error:?}");
    assert!(!error.detail.contains("not before"), "{error:?}");
}

/// A-R183 with A-R164: publish marks that do not decode fault the restore, so the landing
/// faults the node. They are never read as "no marks", which would hide the damage.
#[test]
fn undecodable_publish_marks_fault_the_restore() {
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    let rt = current_thread();
    reopened_with_marks(&path, 2, &[]);
    {
        let raw = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            &path,
            rdb_storage::keys::COLUMN_FAMILIES,
        )
        .expect("raw open");
        let cf = raw.cf_handle(rdb_storage::keys::CF_METADATA).expect("cf");
        let mut key = b"publish".to_vec();
        key.extend_from_slice(&1_u32.to_be_bytes());
        raw.put_cf(cf, &key, [1, 2, 3]).expect("raw put");
    }
    let mut host = host_over(&path, &rt);
    let fault = host
        .restore_marks(PartitionId(1), Generation(2))
        .expect_err("a fault, not an empty list");
    assert_eq!(fault.detail, "publish marks of p1: Corrupt", "{fault:?}");
}

/// A-R183 C2: the marks of a long queue are at most [`PUBLISH_MARKS`] spread along it, plus
/// the newest entry of each generation, in order.
#[test]
fn publish_marks_sample_the_queue_and_keep_each_generations_newest() {
    let mut published: VecDeque<(Generation, Seq, Tick)> = (1..=1_000)
        .map(|seq| (Generation(1), Seq(seq), Tick(seq)))
        .collect();
    published.extend((1_001..=1_003).map(|seq| (Generation(2), Seq(seq), Tick(seq))));
    let marks = sample_marks(&published);
    assert!(marks.len() <= PUBLISH_MARKS + 2, "{} marks", marks.len());
    assert!(marks.len() >= PUBLISH_MARKS / 2, "{} marks", marks.len());
    assert!(marks.contains(&(Generation(1), Seq(1_000), Tick(1_000))));
    assert_eq!(
        marks.last(),
        Some(&(Generation(2), Seq(1_003), Tick(1_003)))
    );
    assert!(marks.windows(2).all(|pair| pair[0] < pair[1]), "in order");
    assert_eq!(sample_marks(&VecDeque::new()), []);
}

/// Design-ackfloor D8 (§3, Rev 3 A4): `published_at` grows with seconds, not writes. 10,000
/// publishes 2 ms apart, over 20 s, leave at most 21 entries, one per (generation, 1 s
/// bucket). A bucket keeps its last seq and its newest tick, so the trim duty, which frees a
/// bucket once its tick is a retention old, never frees a seq before its own publish is: the
/// newest publish of each bucket is recorded verbatim. A new generation starts its own bucket
/// in the same second. It fails if every publish is its own entry (10,000).
#[test]
fn d8_published_at_keeps_one_entry_per_generation_and_second() {
    let mut published = VecDeque::new();
    let (start, step) = (7_300_u64, 2_u64);
    for n in 0..10_000_u64 {
        record_published(
            &mut published,
            PUBLISH_BUCKET_MILLIS,
            Generation(1),
            Seq(n + 1),
            Tick(start + n * step),
        );
    }
    assert!(published.len() <= 21, "{} entries", published.len());
    assert_eq!(
        published.back(),
        Some(&(Generation(1), Seq(10_000), Tick(start + 9_999 * step))),
        "the last bucket holds the newest publish"
    );
    for &(_, seq, at) in &published {
        // Seq n + 1 published at start + n * step: the bucket's tick is its last seq's own,
        // and the publish after it falls in the next second.
        assert_eq!(at, Tick(start + (seq.0 - 1) * step), "{published:?}");
        assert!(
            seq == Seq(10_000) || (at.0 + step) / 1_000 != at.0 / 1_000,
            "{seq:?} is not its second's newest: {published:?}"
        );
    }
    let last = *published.back().expect("one entry at least");
    record_published(
        &mut published,
        PUBLISH_BUCKET_MILLIS,
        Generation(2),
        Seq(1),
        Tick(last.2 .0 + 1),
    );
    assert_eq!(
        published.back(),
        Some(&(Generation(2), Seq(1), Tick(last.2 .0 + 1))),
        "a new generation is a new bucket"
    );
    assert_eq!(
        published[published.len() - 2],
        last,
        "the old bucket is unchanged"
    );
}

/// M9 S3-5c, guard 1: a write ages from its publication, never from its commit. Node 1
/// applies request 3 with both links held, so it is committed and unpublished. Past the
/// retention it is still held, and the trim spares it. Once healed it publishes, and a
/// resend right away replays it from the dedup index. Once aged from its publication a
/// trim takes it, and a resend runs afresh and fails its condition. Aged from the commit,
/// the host would trim it while held, when T1 has no entry for it yet, and never again:
/// the entry would outlive the retention and the last resend would still replay.
/// Trio (~4 s): a 300 ms retention. The hold ends at node 1's first trim tick once the
/// write is 300 ms old; the wait after publication is the retention, a trim period and a
/// margin.
#[config_log::retcd_test]
fn a_write_held_past_the_retention_ages_from_its_publication_not_its_commit() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.dedup_retention_millis = 300;
    });
    trio.bootstrap();
    trio.ready();
    match trio.ask(put(1, b"a", b"1", None)) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(2)),
        other => panic!("put a=1: {other:?}"),
    }
    let held = match trio.ask(compile(3, b"b", b"1")) {
        Answer::Compiled(request) => request,
        other => panic!("compile request 3: {other:?}"),
    };
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    let answer = trio.call(ClientCall::Resend {
        request: held.clone(),
    });
    trio.until("node 1 applied request 3", |trio| {
        (trio.head() == Some(3)).then_some(())
    });
    // A-R124a: held until node 1's first trim tick at or past the write's age, 0.3 to
    // 1.3 s, not a fixed 1.5 s, since P1 answers a held write UNKNOWN_OUTCOME 2 s after it
    // applies. A tick that trims nothing logs nothing, so the tick is read from
    // `next_trim`, which each tick sets to its own time plus the period.
    // Residual exposure: a stall over about 2 s minus this hold still crosses that deadline.
    let aged = trio.clock.now().plus_millis(300);
    trio.until("held past the retention and then a trim tick", |trio| {
        let ticked = trio.nodes[0].0.next_trim.0 - HOST_TRIM_EVERY_MILLIS;
        (ticked >= aged.0).then_some(())
    });
    trio.links.heal_all();
    match trio.answer(&answer) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3)),
        other => panic!("request 3 after the heal: {other:?}"),
    }
    match trio.ask(ClientCall::Resend {
        request: held.clone(),
    }) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3), "replayed"),
        other => panic!("resend 3 right after its publication: {other:?}"),
    }
    let aged = trio
        .clock
        .now()
        .plus_millis(300 + HOST_TRIM_EVERY_MILLIS + 200);
    trio.until("aged from its publication, then a trim tick", |trio| {
        (trio.clock.now() >= aged).then_some(())
    });
    // A-R123: by now the grant may have expired and fenced node 1, and a resend inside that
    // window is refused `LEASE_EXPIRED` rather than run. Waiting only ages the entry more.
    trio.admits_for(1_000);
    match trio.ask(ClientCall::Resend { request: held }) {
        Answer::Error { error, .. } => {
            assert_eq!(error.kind, ErrorKind::ConditionFailed, "{error:?}");
        }
        other => panic!("resend 3 once aged from its publication: {other:?}"),
    }
}

/// U1 (ruling A-R109): a check A1 refuses is logged `authority_check_denied` with its
/// reason, which the client's refusal does not carry. The positive control for that line:
/// with the control store down, A1 still holds its grant but denies `ControlUnavailable`,
/// and a put's dispatch check is refused for that reason.
/// Trio (~2 s): bootstrap, then one store outage on a 100 ms renewal.
#[config_log::retcd_test]
fn a_dispatch_check_a1_denies_is_logged_with_its_reason() {
    const METHOD: &str = "a_dispatch_check_a1_denies_is_logged_with_its_reason";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.renew_millis = 100;
    });
    trio.bootstrap();
    trio.ready();
    assert!(logged!(METHOD, "authority_check_denied").is_empty());
    // A stall on this thread past the grant lapses it (#50): ask with time left on it.
    trio.admits_for(1_000);
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    trio.until("node 1 stops admitting", |trio| {
        (trio.status(0).admits != Some(true)).then_some(())
    });
    let refused = trio.ask(put(1, b"a", b"1", None));
    trio.mem.stop_failing();
    assert!(matches!(refused, Answer::Error { .. }), "{refused:?}");
    let lines = trio.until("authority_check_denied", |_| {
        let lines = logged!(METHOD, "authority_check_denied");
        (!lines.is_empty()).then_some(lines)
    });
    assert!(
        lines.iter().any(|line| line["node"] == 1
            && line["checkpoint"] == "StorageDispatch"
            && line["reason"] == "ControlUnavailable"),
        "{lines:?}"
    );
}

/// Ruling A-R285: a put A1 refuses at dispatch for `ControlUnavailable` (control quorum lost,
/// grant still held) reaches the client as `PROTECTION_PAUSED` with its no-mutation proof, not
/// `LEASE_EXPIRED`. Under load on 3 processes an rEtcd election showed 19 such refusals as
/// `LEASE_EXPIRED`, which reads as a lost grant (dev-perf, lr R=30 run 2, 2026-10-24).
/// Trio (~2 s): bootstrap, then one store outage on a 100 ms renewal.
#[config_log::retcd_test]
fn a_control_unavailable_deny_reaches_the_client_as_protection_paused() {
    const METHOD: &str = "a_control_unavailable_deny_reaches_the_client_as_protection_paused";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.renew_millis = 100;
    });
    trio.bootstrap();
    trio.ready();
    // A stall on this thread past the grant lapses it (#50): ask with time left on it.
    trio.admits_for(1_000);
    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    trio.until("node 1 stops admitting", |trio| {
        (trio.status(0).admits != Some(true)).then_some(())
    });
    let refused = trio.ask(put(1, b"a", b"1", None));
    trio.mem.stop_failing();
    let lines = trio.until("authority_check_denied", |_| {
        let lines = logged!(METHOD, "authority_check_denied");
        (!lines.is_empty()).then_some(lines)
    });
    assert!(
        lines.iter().any(|line| line["node"] == 1
            && line["checkpoint"] == "StorageDispatch"
            && line["reason"] == "ControlUnavailable"),
        "the refusal is A1's ControlUnavailable deny at dispatch: {lines:?}"
    );
    match refused {
        Answer::Error { error, .. } => {
            assert_eq!(error.kind, ErrorKind::ProtectionPaused, "{error:?}");
            assert!(error.no_mutation, "refused before apply: {error:?}");
        }
        other => panic!("a put while control is unavailable: {other:?}"),
    }
}

/// A copy id outside the Trio's configuration: R1 never sends to it, so only a row's direct
/// `count_resend` calls touch its slot.
const UNSENT: CopyId = CopyId(9);

/// A copy's acknowledged progress, received with applied.
fn acked_at(applied: u64, durable: u64) -> ReplicaProgress {
    ReplicaProgress {
        received: ReceivedSeq(applied),
        buffered_applied: AppliedSeq(applied),
        durable: DurableSeq(durable),
    }
}

fn identity(request: u64) -> RequestIdentity {
    RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(1),
        request: RequestId(request),
    }
}

fn put(
    request: u64,
    object: &'static [u8],
    value: &'static [u8],
    if_version: Option<u64>,
) -> ClientCall {
    ClientCall::Put {
        identity: identity(request),
        object: Bytes::from_static(object),
        change: byte_doc(value),
        expect: if_version.map(Expected::Version),
        remaining_millis: 5_000,
    }
}

/// A byte-string document, as `Db::put` writes one.
fn byte_doc(value: &[u8]) -> Change {
    Change::Doc(Delta(vec![Op::Replace(Value::Bytes(value.to_vec()))]))
}

/// A read's version and bytes when it saw a byte-string document, `None` when absent.
fn doc_bytes(seen: Seen) -> Option<(u64, Bytes)> {
    match seen {
        Seen::Absent => None,
        Seen::Doc(Document {
            version,
            value: Value::Bytes(bytes),
        }) => Some((version, Bytes::from(bytes))),
        other => panic!("not a byte-string document: {other:?}"),
    }
}

fn compile(request: u64, object: &'static [u8], value: &'static [u8]) -> ClientCall {
    ClientCall::Compile {
        identity: identity(request),
        puts: vec![PutOp {
            group: AFFINITY,
            object: Bytes::from_static(object),
            change: byte_doc(value),
            expect: None,
        }],
        remaining_millis: 5_000,
    }
}

fn get(request: u64, object: &'static [u8]) -> ClientCall {
    ClientCall::Get {
        identity: identity(request),
        object: Bytes::from_static(object),
        look: Look::Doc,
    }
}

/// The upload every S4-4 row writes under.
const U1: Upload = [1; 16];

/// A compile of chunk `index` of [`U1`] under `object`.
fn chunk_call(
    request: u64,
    object: &'static [u8],
    index: u32,
    bytes: &'static [u8],
    expect: Option<Expected>,
) -> ClientCall {
    let change = chunk_of(U1, index, bytes);
    compile_change(request, object, change, expect)
}

/// Chunk `index` of `upload`.
fn chunk_of(upload: Upload, index: u32, bytes: &'static [u8]) -> Change {
    Change::Chunk {
        upload,
        index,
        bytes: Bytes::from_static(bytes),
    }
}

/// A compile of the publish of `blob`, uploaded as [`U1`] in chunks of 4.
fn publish_call(
    request: u64,
    object: &'static [u8],
    blob: &[u8],
    expect: Option<Expected>,
) -> ClientCall {
    compile_change(request, object, publish_of(U1, blob), expect)
}

/// The publish of `blob`, uploaded as `upload` in chunks of 4.
fn publish_of(upload: Upload, blob: &[u8]) -> Change {
    use sha2::Digest as _;
    Change::Publish {
        upload,
        size: blob.len() as u64,
        chunk_size: 4,
        sha256: sha2::Sha256::digest(blob).into(),
    }
}

/// A refusal as `(kind, retry, no_mutation, whether it names a request)`.
fn refusal(answer: Answer) -> (ErrorKind, RetryRule, bool, bool) {
    match answer {
        Answer::Error { error, request } => (
            error.kind,
            error.retry,
            error.no_mutation,
            request.is_some(),
        ),
        other => panic!("not refused: {other:?}"),
    }
}

/// S4-4 main line, 4c, 4x: arch §4 guards 7, 8, 10 and 11, and both blob kinds end to end.
/// A repeated chunk, a gc with nothing to collect, and the same publish compile to nothing,
/// and nothing is sent. The publish's check 0 needs `serving` to be the adopted generation.
/// A chunk under other bytes is UPLOAD_CHUNK_CONFLICT. A chunk or gc with an `expect` is
/// refused. A read of a damaged chunk is CORRUPT_BLOB. Every refusal is at the compile and
/// names no request.
/// Mutants: m7 (empty compile sent) and m8 (`Generation(0)`) answer `Compiled`. m10a and
/// m10b (rows dropped) answer `BoundedJitter`. m11 (expect ignored) answers `Unchanged`.
/// Trio, about 1 s.
#[test]
fn a_repeated_blob_write_sends_nothing_and_its_refusals_name_the_blob_kinds() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    assert_eq!(trio.write(chunk_call(1, b"b", 0, b"HELL", None)), Ok(2));
    assert_eq!(trio.write(chunk_call(2, b"b", 1, b"O", None)), Ok(3));
    let publish = publish_call(3, b"b", b"HELLO", Some(Expected::Absent));
    assert_eq!(trio.write(publish), Ok(4));

    for (what, call) in [
        ("the same chunk", chunk_call(4, b"b", 0, b"HELL", None)),
        (
            "a gc with nothing to collect",
            compile_change(5, b"b", Change::Gc { floor: 4 }, None),
        ),
        (
            "the same publish",
            publish_call(6, b"b", b"HELLO", Some(Expected::Absent)),
        ),
    ] {
        match trio.ask(call) {
            Answer::Unchanged => {}
            other => panic!("{what}: {other:?}"),
        }
    }

    assert_eq!(
        refusal(trio.ask(chunk_call(7, b"b", 0, b"XXXX", None))),
        (
            ErrorKind::UploadChunkConflict,
            RetryRule::Reconcile,
            true,
            false
        ),
        "other bytes under a stored chunk"
    );
    let stale = Some(Expected::Version(4));
    for (what, call) in [
        ("a chunk", chunk_call(8, b"b", 0, b"HELL", stale)),
        (
            "a gc",
            compile_change(9, b"b", Change::Gc { floor: 4 }, stale),
        ),
    ] {
        assert_eq!(
            refusal(trio.ask(call)),
            (
                ErrorKind::InvalidArgument,
                RetryRule::Definitive,
                true,
                false
            ),
            "{what} with an expect"
        );
    }

    let range = |trio: &mut Trio, request, offset, len| {
        trio.look(request, b"b", Look::Range { offset, len })
            .map(|seen| match seen {
                Seen::Range { bytes, .. } => bytes,
                other => panic!("a range read: {other:?}"),
            })
    };
    assert_eq!(range(&mut trio, 10, 0, 5), Ok(Bytes::from_static(b"HELLO")));

    // Other bytes over chunk 1, written as `rdb_dev admin damage-chunk` does.
    let root = root_key(TenantId(1), AFFINITY, b"b");
    let damage = TxnRequest {
        api_version: API_VERSION,
        identity: identity(11),
        affinity: AFFINITY,
        expected_generation: None,
        remaining_millis: 5_000,
        conditions: Vec::new(),
        mutations: vec![rdb_core::contracts::txn::Mutation::Put {
            key: rdb_value::keys::chunk_key(&root, &U1, 1),
            value: rdb_value::envelope::seal(rdb_value::envelope::Kind::Chunk, b"damaged")
                .expect("seven bytes fit an envelope"),
            expected_version: None,
        }],
        acked_below: None,
        pinned: Vec::new(),
    };
    match trio.ask(ClientCall::Resend { request: damage }) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(5)),
        other => panic!("the damage write: {other:?}"),
    }
    assert_eq!(
        range(&mut trio, 12, 0, 4),
        Ok(Bytes::from_static(b"HELL")),
        "chunk 0 is intact"
    );
    match range(&mut trio, 13, 4, 1) {
        Err(error) => assert_eq!(
            (error.kind, error.retry, error.no_mutation),
            (ErrorKind::CorruptBlob, RetryRule::Quarantine, false)
        ),
        Ok(bytes) => panic!("a damaged chunk read as {bytes:?}"),
    }

    // A blob root whose envelope does not open is none of the four blob variants: it is
    // CORRUPT_HISTORY, as for any object (review F4).
    let damage = TxnRequest {
        api_version: API_VERSION,
        identity: identity(14),
        affinity: AFFINITY,
        expected_generation: None,
        remaining_millis: 5_000,
        conditions: Vec::new(),
        mutations: vec![rdb_core::contracts::txn::Mutation::Put {
            key: root.to_bytes(),
            value: Bytes::from_static(b"not an envelope"),
            expected_version: None,
        }],
        acked_below: None,
        pinned: Vec::new(),
    };
    match trio.ask(ClientCall::Resend { request: damage }) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(6)),
        other => panic!("the root damage write: {other:?}"),
    }
    match range(&mut trio, 15, 0, 4) {
        Err(error) => assert_eq!(
            (error.kind, error.retry, error.no_mutation),
            (ErrorKind::CorruptHistory, RetryRule::Quarantine, false)
        ),
        Ok(bytes) => panic!("a damaged root read as {bytes:?}"),
    }
}

/// S4-4 4h, C-3: arch §4 guard 9. With both secondaries cut off, node 1 applies a publish
/// that cannot publish. The same publish re-issued meanwhile finds its blob at the root, but
/// above P1's published seq. So it is UNKNOWN_OUTCOME at the compile, naming no request, and
/// never `unchanged`. After the heal the held publish publishes, and the re-issue is
/// `unchanged`. Mutant m9 (the check dropped) answers `Unchanged` while held.
/// Trio, about 1 s.
#[test]
fn a_publish_reissued_before_its_blob_publishes_is_an_unknown_outcome_not_unchanged() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    assert_eq!(trio.write(chunk_call(1, b"h", 0, b"abcd", None)), Ok(2));
    let publish = |request| publish_call(request, b"h", b"abcd", Some(Expected::Absent));
    let request = match trio.ask(publish(2)) {
        Answer::Compiled(request) => request,
        other => panic!("compile the publish: {other:?}"),
    };
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    let held = trio.call(ClientCall::Resend { request });
    trio.until("node 1 applied the publish", |trio| {
        (trio.head() == Some(3)).then_some(())
    });

    assert_eq!(
        refusal(trio.ask(publish(3))),
        (
            ErrorKind::UnknownOutcome,
            RetryRule::QueryStatus,
            false,
            false
        ),
        "the blob is at the root and not published"
    );
    assert!(held.try_recv().is_err(), "no copy acknowledged the publish");

    trio.links.heal_all();
    match trio.answer(&held) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(3)),
        other => panic!("the held publish after the heal: {other:?}"),
    }
    match trio.ask(publish(4)) {
        Answer::Unchanged => {}
        other => panic!("the publish once published: {other:?}"),
    }
}

/// Review F1 (A-R133), C-3 and ADR-rdb-0014 O9: "already published" is a read, so it is
/// given only while a read would be. A blob publishes; the control store then fails until
/// A1 fences node 1, and a plain read is refused. The same publish re-issued meanwhile is
/// refused as that read is, and sends nothing, so it says `no_mutation`. Once the store
/// returns and node 1 admits again, the re-issue is `unchanged`. Mutant m24 (the read gate
/// cut) answers `Unchanged` while fenced, as 2cc5b24 did; mutant m28 (`no_mutation` not set)
/// says `false`.
/// Integration (~2 s): bootstrap's discovery window, then one fence on a 600 ms grant.
#[config_log::retcd_test]
fn a_publish_reissued_while_fenced_is_refused_as_a_read_is_and_unchanged_once_admitted() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.grant_millis = 600;
        host.budgets.renew_millis = 100;
    });
    trio.bootstrap();
    trio.ready();
    assert_eq!(trio.write(chunk_call(1, b"f", 0, b"abcd", None)), Ok(2));
    let publish = |request| publish_call(request, b"f", b"abcd", Some(Expected::Absent));
    assert_eq!(trio.write(publish(2)), Ok(3));

    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    trio.until("node 1 fenced", |trio| trio.status(0).fence.map(|_| ()));
    let read = ClientCall::Get {
        identity: identity(3),
        object: Bytes::from_static(b"f"),
        look: Look::Range { offset: 0, len: 4 },
    };
    let refused = match trio.ask(read) {
        Answer::Read {
            outcome: ReadServiceOutcome::Rejected(kind),
            ..
        } => crate::db::refused_read(kind),
        other => panic!("a read while fenced: {other:?}"),
    };
    assert_eq!(
        refusal(trio.ask(publish(4))),
        (refused.kind, refused.retry, true, false),
        "the publish while fenced, as the read ({refused:?}) but sending nothing"
    );

    trio.mem.stop_failing();
    trio.ready();
    match trio.ask(publish(5)) {
        Answer::Unchanged => {}
        other => panic!("the publish once node 1 admits again: {other:?}"),
    }
}

/// Review F-002, the Set arm: arch §4 guard 2. A set change compiled at version 2 and sent
/// after another write moved the set to 3 is refused by the kernel, on the version the
/// compile attached. Mutant m15 (the Set arm's version guard cut from what it compiles)
/// publishes it. Trio, about 1 s.
#[test]
fn a_set_change_sent_after_the_set_moved_is_refused_by_the_kernel() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let add = |n| Change::Set(vec![ElemOp::Add(int(n))]);
    let created = compile_change(1, b"s", add(1), Some(Expected::Absent));
    assert_eq!(trio.write(created), Ok(2));
    let stale = trio.compiled(compile_change(2, b"s", add(2), None));
    assert_eq!(trio.write(compile_change(3, b"s", add(3), None)), Ok(3));
    assert_eq!(
        trio.sent(stale),
        Err(Refused::ByKernel(ErrorKind::ConditionFailed)),
        "compiled at version 2, sent at 3"
    );
    let members = Look::Members {
        after: None,
        limit: 10,
    };
    match trio.look(4, b"s", members) {
        Ok(Seen::Members(page)) => {
            let keys: Vec<_> = page.members.into_iter().map(|m| m.key).collect();
            assert_eq!(keys, vec![int(1), int(3)], "2 was never added");
        }
        other => panic!("members of s: {other:?}"),
    }
}

/// Review F2 (A-R133), the Map arm: arch §4 guard 2. A map change compiled at version 2 and
/// sent after another write moved the map to 3 is refused by the kernel, on the version the
/// compile attached. The map keeps the other write's keys, values and count. Mutant m23 (the
/// Map arm's version guard cut from what it compiles) publishes it. Trio, about 1 s.
#[test]
fn a_map_change_sent_after_the_map_moved_is_refused_by_the_kernel() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let put = |key, n| Change::Map(vec![ElemOp::Put(text(key), int(n))]);
    let created = compile_change(1, b"m", put("a", 1), Some(Expected::Absent));
    assert_eq!(trio.write(created), Ok(2));
    let stale = trio.compiled(compile_change(2, b"m", put("b", 2), None));
    assert_eq!(
        trio.write(compile_change(3, b"m", put("c", 3), None)),
        Ok(3)
    );
    assert_eq!(
        trio.sent(stale),
        Err(Refused::ByKernel(ErrorKind::ConditionFailed)),
        "compiled at version 2, sent at 3"
    );
    let members = Look::Members {
        after: None,
        limit: 10,
    };
    match trio.look(4, b"m", members) {
        Ok(Seen::Members(page)) => {
            let entries: Vec<_> = page.members.into_iter().map(|m| (m.key, m.value)).collect();
            assert_eq!(
                (page.collection.version, page.collection.count, entries),
                (
                    3,
                    2,
                    vec![(text("a"), Some(int(1))), (text("c"), Some(int(3)))]
                ),
                "b was never put"
            );
        }
        other => panic!("members of m: {other:?}"),
    }
}

/// Review F-002, the Publish arm: arch §4 guard 2. A publish of upload 2 compiled over the
/// blob at version 3 and sent after a publish of upload 3 replaced it is refused by the
/// kernel. Mutant m16 (the Publish arm's version guard cut from what it compiles) publishes
/// it. Trio, about 1 s.
#[test]
fn a_publish_sent_after_the_blob_moved_is_refused_by_the_kernel() {
    const U2: Upload = [2; 16];
    const U3: Upload = [3; 16];
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    assert_eq!(trio.write(chunk_call(1, b"p", 0, b"abcd", None)), Ok(2));
    let first = publish_call(2, b"p", b"abcd", Some(Expected::Absent));
    assert_eq!(trio.write(first), Ok(3));
    let chunk =
        |request, upload, bytes| compile_change(request, b"p", chunk_of(upload, 0, bytes), None);
    assert_eq!(trio.write(chunk(3, U2, b"wxyz")), Ok(4));
    assert_eq!(trio.write(chunk(4, U3, b"1234")), Ok(5));
    let stale = trio.compiled(compile_change(5, b"p", publish_of(U2, b"wxyz"), None));
    let replace = publish_of(U3, b"1234");
    let replace = compile_change(6, b"p", replace, Some(Expected::Version(3)));
    assert_eq!(trio.write(replace), Ok(6));
    assert_eq!(
        trio.sent(stale),
        Err(Refused::ByKernel(ErrorKind::ConditionFailed)),
        "compiled over version 3, sent at 6"
    );
    match trio.look(7, b"p", Look::Range { offset: 0, len: 4 }) {
        Ok(Seen::Range { blob, bytes }) => assert_eq!(
            (blob.version, blob.manifest.upload, bytes),
            (6, U3, Bytes::from_static(b"1234"))
        ),
        other => panic!("a range read of p: {other:?}"),
    }
}

/// Review F-003: arch §4 guards 2 and 12 together. A list edit with a stale guard and a
/// position outside the list is CONDITION_FAILED, not INVALID_ARGUMENT: the guard is
/// compiled first, so a write that raced this one reads as the race. So is an edit to a
/// list that is not there. Mutant m17 (`list_compile` returns the position's refusal without
/// compiling the guard) answers INVALID_ARGUMENT for both. Trio, about 1 s.
#[test]
fn a_stale_list_edit_at_a_bad_position_is_a_condition_failure() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let push = |s: &str| ListEdit::Push(text(s));
    let created = Change::List(vec![push("a"), push("b"), push("c")]);
    let created = compile_change(1, b"l", created, Some(Expected::Absent));
    assert_eq!(trio.write(created), Ok(2));
    let more = compile_change(2, b"l", Change::List(vec![push("d")]), None);
    assert_eq!(trio.write(more), Ok(3));
    for (request, object, at) in [(3, &b"l"[..], -5), (4, &b"none"[..], 0)] {
        let call = ClientCall::Compile {
            identity: identity(request),
            puts: vec![PutOp {
                group: AFFINITY,
                object: Bytes::copy_from_slice(object),
                change: Change::List(vec![ListEdit::Remove { at }]),
                expect: Some(Expected::Version(2)),
            }],
            remaining_millis: 5_000,
        };
        assert_eq!(
            trio.write(call),
            Err(Refused::AtCompile(ErrorKind::ConditionFailed)),
            "remove {at} at version 2 of {object:?}"
        );
    }
}

/// Review F-004: a gc's floor reaches `collect_garbage` as the caller gave it. Two chunks of
/// an upload nothing publishes, at versions 2 and 3: a gc with floor 2 deletes the first and
/// keeps the second, so the first compiles to a write again and the second to nothing.
/// Mutant m18a (floor 0) collects nothing, so the gc answers `Unchanged`; m18b (floor
/// `u64::MAX`) collects both. Trio, about 1 s.
#[test]
fn a_gc_deletes_the_unnamed_chunks_at_or_below_its_floor_and_keeps_the_rest() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    assert_eq!(trio.write(chunk_call(1, b"g", 0, b"abcd", None)), Ok(2));
    assert_eq!(trio.write(chunk_call(2, b"g", 1, b"ef", None)), Ok(3));
    let gc = compile_change(3, b"g", Change::Gc { floor: 2 }, None);
    assert_eq!(trio.write(gc), Ok(4));
    match trio.ask(chunk_call(4, b"g", 0, b"abcd", None)) {
        Answer::Compiled(_) => {}
        other => panic!("chunk 0, at version 2, was collected: {other:?}"),
    }
    match trio.ask(chunk_call(5, b"g", 1, b"ef", None)) {
        Answer::Unchanged => {}
        other => panic!("chunk 1, at version 3, was kept: {other:?}"),
    }
}

/// Review F-008, the `GenerationChanged` half of a stale token (C-2): a token from another
/// generation is `STALE_CONTINUATION`, retry `Reconcile`, as one from another list version
/// is. Forged here, since no generation changes in a Trio row. Mutant m21 (that variant cut
/// from the arm) answers `INVALID_ARGUMENT`. Trio, about 1 s.
#[test]
fn a_token_from_another_generation_must_reconcile() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let created = Change::List(vec![ListEdit::Push(text("a")), ListEdit::Push(text("b"))]);
    let created = compile_change(1, b"l", created, Some(Expected::Absent));
    assert_eq!(trio.write(created), Ok(2));
    let first = match trio.look(2, b"l", Look::Items { at: 0, limit: 1 }) {
        Ok(Seen::Items(items)) => items,
        other => panic!("items of l: {other:?}"),
    };
    let token = first.next.expect("a page of 1 of 2 has a next token");
    let forged = Token {
        generation: Generation(token.generation.0 + 1),
        ..token
    };
    match trio.look(
        3,
        b"l",
        Look::ItemsAfter {
            token: forged,
            limit: 10,
        },
    ) {
        Err(error) => assert_eq!(
            (error.kind, error.retry),
            (ErrorKind::StaleContinuation, RetryRule::Reconcile),
            "{error:?}"
        ),
        Ok(seen) => panic!(
            "a token from generation {} read: {seen:?}",
            forged.generation.0
        ),
    }
}

/// A `ClientCall::Compile` of one change to `object`, as `Db::write` sends first.
fn compile_change(
    request: u64,
    object: &'static [u8],
    change: Change,
    expect: Option<Expected>,
) -> ClientCall {
    ClientCall::Compile {
        identity: identity(request),
        puts: vec![PutOp {
            group: AFFINITY,
            object: Bytes::from_static(object),
            change,
            expect,
        }],
        remaining_millis: 5_000,
    }
}

fn text(s: &str) -> Value {
    Value::Text(s.to_owned())
}

fn int(n: u64) -> Value {
    Value::Integer(Int::from(n))
}

fn json_map(entries: &[(&str, Value)]) -> Value {
    let mut map = Map::new();
    for (key, value) in entries {
        map.insert(MapKey::new(*key), value.clone());
    }
    Value::Map(map)
}

fn path(text: &str) -> Path {
    Path::parse(text).expect("a path")
}

/// Where a write was refused, as `rdb_dev` prints it: the compile answered an error, so no
/// request was built (`request=-`), or it compiled and the kernel refused the request it was
/// sent as (`request=<R>`).
#[derive(Debug, PartialEq, Eq)]
enum Refused {
    AtCompile(ErrorKind),
    ByKernel(ErrorKind),
}

impl Trio {
    /// Compile `call`, then send what it compiled to, as `Db::write` does. `Ok` is the
    /// published seq.
    fn write(&mut self, call: ClientCall) -> Result<u64, Refused> {
        let request = match self.ask(call) {
            Answer::Compiled(request) => request,
            Answer::Error { error, .. } => return Err(Refused::AtCompile(error.kind)),
            other => panic!("compile: {other:?}"),
        };
        self.sent(request)
    }

    /// The request `call` compiles to, not yet sent.
    fn compiled(&mut self, call: ClientCall) -> TxnRequest {
        match self.ask(call) {
            Answer::Compiled(request) => request,
            other => panic!("compile: {other:?}"),
        }
    }

    /// Send `request`, as [`Self::write`] does once it has compiled.
    fn sent(&mut self, request: TxnRequest) -> Result<u64, Refused> {
        match self.ask(ClientCall::Resend { request }) {
            Answer::Txn { result, .. } => Ok(result.seq.0),
            Answer::Error { error, .. } => Err(Refused::ByKernel(error.kind)),
            other => panic!("send: {other:?}"),
        }
    }

    /// What a read of `object` through `look` saw, or its refusal.
    fn look(&mut self, request: u64, object: &'static [u8], look: Look) -> Result<Seen, ApiError> {
        let call = ClientCall::Get {
            identity: identity(request),
            object: Bytes::from_static(object),
            look,
        };
        match self.ask(call) {
            Answer::Read { seen, .. } => Ok(seen),
            Answer::Error { error, .. } => Err(error),
            other => panic!("read: {other:?}"),
        }
    }
}

/// S4-1 (main line, 1k): arch §4 guards 1 and 2. A stale `Version` guard is refused by the
/// compile, before a request exists. A create over an existing document compiles, and the
/// kernel refuses it on the `Absent` condition the compile attached. Mutant m1 (the compile
/// ignores `expect`) publishes the stale change; m2 (conditions dropped) publishes the
/// create and empties the document. Trio, about 1 s.
#[test]
fn a_stale_document_change_is_refused_by_the_compile_and_a_create_over_one_by_the_kernel() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let replace = |value: Value| Change::Doc(Delta(vec![Op::Replace(value)]));
    let created = json_map(&[
        ("n", int(1)),
        ("tags", Value::Array(vec![text("a"), text("x")])),
    ]);
    assert_eq!(
        trio.write(compile_change(
            1,
            b"d",
            replace(created),
            Some(Expected::Absent)
        )),
        Ok(2)
    );
    let change = Change::Doc(Delta(vec![
        Op::Increment(path("/n"), Int::from(5_u64)),
        Op::Set(path("/tags/1"), text("b")),
    ]));
    assert_eq!(
        trio.write(compile_change(2, b"d", change, Some(Expected::Version(2)))),
        Ok(3)
    );
    let changed = Seen::Doc(Document {
        version: 3,
        value: json_map(&[
            ("n", int(6)),
            ("tags", Value::Array(vec![text("a"), text("b")])),
        ]),
    });
    assert_eq!(trio.look(3, b"d", Look::Doc), Ok(changed.clone()));

    let stale = Change::Doc(Delta(vec![Op::Increment(path("/n"), Int::from(1_u64))]));
    assert_eq!(
        trio.write(compile_change(4, b"d", stale, Some(Expected::Version(2)))),
        Err(Refused::AtCompile(ErrorKind::ConditionFailed)),
        "a stale guard is refused before a request exists"
    );
    let over = replace(json_map(&[]));
    assert_eq!(
        trio.write(compile_change(5, b"d", over, Some(Expected::Absent))),
        Err(Refused::ByKernel(ErrorKind::ConditionFailed)),
        "a create compiles without reading; the kernel's Absent condition refuses it"
    );
    assert_eq!(trio.look(6, b"d", Look::Doc), Ok(changed), "d is unchanged");
}

/// S4-2 (2n, 2k, 2p): arch §4 guards 2, 3 and 5. A failed `need` is a failed condition,
/// either way round. A create over an existing map is refused by the kernel. A document read
/// of a map is refused, never `absent`. Mutant m3 (the element arm cut) answers
/// `INVALID_ARGUMENT` for the needs; m5 (read errors as `Seen::Absent`) reads the map as
/// absent; m2 publishes the create. Trio, about 1 s.
#[test]
fn a_failed_need_is_a_condition_failure_and_a_map_never_reads_as_a_document() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let map = Change::Map(vec![
        ElemOp::Put(text("a"), int(1)),
        ElemOp::Put(text("b"), int(2)),
    ]);
    assert_eq!(
        trio.write(compile_change(1, b"m", map, Some(Expected::Absent))),
        Ok(2)
    );
    let set = Change::Set(vec![ElemOp::Add(int(1))]);
    assert_eq!(
        trio.write(compile_change(2, b"s", set, Some(Expected::Absent))),
        Ok(3)
    );

    for (request, need) in [
        (3, ElemOp::NeedPresent(int(9))),
        (4, ElemOp::NeedAbsent(int(1))),
    ] {
        let change = Change::Set(vec![need.clone()]);
        assert_eq!(
            trio.write(compile_change(
                request,
                b"s",
                change,
                Some(Expected::Version(3))
            )),
            Err(Refused::AtCompile(ErrorKind::ConditionFailed)),
            "{need:?}"
        );
    }
    let over = Change::Map(vec![ElemOp::Put(text("z"), int(1))]);
    assert_eq!(
        trio.write(compile_change(5, b"m", over, Some(Expected::Absent))),
        Err(Refused::ByKernel(ErrorKind::ConditionFailed)),
        "a create over the map is the kernel's refusal"
    );
    match trio.look(6, b"m", Look::Doc) {
        Err(error) => assert_eq!(error.kind, ErrorKind::InvalidArgument, "{error:?}"),
        Ok(seen) => panic!("a map read as a document: {seen:?}"),
    }
    let members = Look::Members {
        after: None,
        limit: 10,
    };
    match trio.look(7, b"m", members) {
        Ok(Seen::Members(page)) => {
            let keys: Vec<_> = page
                .members
                .iter()
                .map(|member| member.key.clone())
                .collect();
            assert_eq!(
                (page.collection.version, page.collection.count, keys),
                (2, 2, vec![text("a"), text("b")]),
                "m is as created"
            );
        }
        other => panic!("members of m: {other:?}"),
    }
}

/// S4-3 3n2, 3n4, 3n5, 3n6 and the running count (3x): arch §4 guards 12 and 13 (A-R116).
/// `-k` names `count - k`; a position below 0 or past the end is refused, naming the edit;
/// each push or insert moves the count up and each remove moves it down, so a later edit
/// resolves against the list as the earlier edits leave it. Mutant m12a (below 0 clamps to 0)
/// removes item 0; m12b (negatives never resolved) refuses `-1`; m12c (off by one) removes
/// item 1; m13 (a push does not move the count) removes the old last item. Unit.
#[test]
fn a_negative_position_counts_from_the_end_of_the_list_the_earlier_edits_leave() {
    let z = || Value::Text("z".to_owned());
    assert_eq!(
        resolve_edits(&[ListEdit::Remove { at: -1 }], 3),
        Ok(vec![ListOp::Remove { at: 2 }]),
        "-1 is the last item"
    );
    assert_eq!(
        resolve_edits(&[ListEdit::Push(z()), ListEdit::Remove { at: -1 }], 3),
        Ok(vec![ListOp::Push(z()), ListOp::Remove { at: 3 }]),
        "the push moved the end"
    );
    assert_eq!(
        resolve_edits(
            &[
                ListEdit::Insert { at: -1, value: z() },
                ListEdit::Move { from: -1, to: 0 }
            ],
            2
        ),
        Ok(vec![
            ListOp::Insert { at: 1, value: z() },
            ListOp::Move { from: 2, to: 0 }
        ]),
        "the insert moved the end"
    );
    assert_eq!(
        resolve_edits(&[ListEdit::Replace { at: -1, value: z() }], 3),
        Ok(vec![ListOp::Replace { at: 2, value: z() }]),
        "a replace resolves -1 as every edit does"
    );
    for (edits, count, refused) in [
        (
            vec![ListEdit::Remove { at: -4 }],
            3,
            "edit 0: position -4 is not inside the list of 3",
        ),
        (
            vec![ListEdit::Remove { at: -1 }, ListEdit::Remove { at: -1 }],
            1,
            "edit 1: position -1 is not inside the list of 0",
        ),
        (
            vec![ListEdit::Insert { at: -1, value: z() }],
            0,
            "edit 0: position -1 is not inside the list of 0",
        ),
        (
            vec![ListEdit::Replace { at: 3, value: z() }],
            3,
            "edit 0: position 3 is not inside the list of 3",
        ),
    ] {
        let error = resolve_edits(&edits, count).expect_err(refused);
        assert_eq!(
            (error.kind, error.detail.as_str()),
            (ErrorKind::InvalidArgument, refused)
        );
    }
}

/// Review F-008: an insert may name the count, the end of the list; no other edit may.
/// Mutant m19 (the end never allowed) refuses it. Unit.
#[test]
fn an_insert_at_the_count_adds_at_the_end() {
    let z = Value::Text("z".to_owned());
    assert_eq!(
        resolve_edits(
            &[ListEdit::Insert {
                at: 3,
                value: z.clone()
            }],
            3
        ),
        Ok(vec![ListOp::Insert { at: 3, value: z }])
    );
}

/// Review F-008: a move's destination counts from the end when negative, as its source
/// does: `-1` is the last position. Mutant m20 (`to` never resolved) refuses it. Unit.
#[test]
fn a_move_to_a_negative_position_counts_from_the_end() {
    assert_eq!(
        resolve_edits(&[ListEdit::Move { from: 0, to: -1 }], 3),
        Ok(vec![ListOp::Move { from: 0, to: 2 }])
    );
}

/// S4-3 3n1, 3n2, 3e and 3t: arch §4 guards 14, 12 and 4. A read's negative `--from`
/// resolves against the list the read sees, and a start before 0 or past the end is refused.
/// `-4` of 3 is refused by the compile, before a request exists. A fresh continuation token
/// continues after its page; one whose list has changed is `STALE_CONTINUATION` with retry
/// `Reconcile`, as the kernel's own stale continuation is. Mutant m14 (resolved against 0)
/// refuses `-1`; m12c (off by one) lists from item 1; m12a publishes the `-4` remove; m4a
/// (built with `ApiError::new`) answers retry `BoundedJitter`; m4b (the arm cut) answers
/// `CONDITION_FAILED`. Trio, about 1 s.
#[test]
fn a_list_read_resolves_its_start_against_the_list_and_a_stale_token_must_reconcile() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let push = |s: &str| ListEdit::Push(text(s));
    let created = Change::List(vec![push("a"), push("b"), push("c")]);
    assert_eq!(
        trio.write(compile_change(1, b"l", created, Some(Expected::Absent))),
        Ok(2)
    );
    let page = |seen: Result<Seen, ApiError>| match seen {
        Ok(Seen::Items(items)) => items,
        other => panic!("items of l: {other:?}"),
    };
    let last = page(trio.look(2, b"l", Look::Items { at: -1, limit: 10 }));
    let listed = |items: Items| -> Vec<(u64, Value)> {
        items
            .items
            .into_iter()
            .map(|item| (item.position, item.value))
            .collect()
    };
    assert_eq!(listed(last), vec![(2, text("c"))], "-1 is the last of 3");
    for (request, at) in [(3, -4), (4, 99)] {
        match trio.look(request, b"l", Look::Items { at, limit: 10 }) {
            Err(error) => assert_eq!(error.kind, ErrorKind::InvalidArgument, "{error:?}"),
            Ok(seen) => panic!("a read from {at} of 3 was answered: {seen:?}"),
        }
    }

    let past = Change::List(vec![ListEdit::Remove { at: -4 }]);
    assert_eq!(
        trio.write(compile_change(5, b"l", past, Some(Expected::Version(2)))),
        Err(Refused::AtCompile(ErrorKind::InvalidArgument)),
        "-4 of 3 is refused before a request exists"
    );

    let first = page(trio.look(6, b"l", Look::Items { at: 0, limit: 2 }));
    let token = first.next.expect("a page of 2 of 3 has a next token");
    let rest = page(trio.look(7, b"l", Look::ItemsAfter { token, limit: 10 }));
    assert_eq!(
        listed(rest),
        vec![(2, text("c"))],
        "a fresh token continues after its page"
    );
    let more = Change::List(vec![push("z")]);
    assert_eq!(
        trio.write(compile_change(8, b"l", more, Some(Expected::Version(2)))),
        Ok(3)
    );
    match trio.look(9, b"l", Look::ItemsAfter { token, limit: 10 }) {
        Err(error) => assert_eq!(
            (error.kind, error.retry),
            (ErrorKind::StaleContinuation, RetryRule::Reconcile),
            "{error:?}"
        ),
        Ok(seen) => panic!("a token from before the push still read: {seen:?}"),
    }
}

/// A raw write of `value` over the root of `object`, as `rdb_dev admin damage` sends it.
fn damage_write(request: u64, object: &[u8], value: Bytes) -> TxnRequest {
    TxnRequest {
        api_version: API_VERSION,
        identity: identity(request),
        affinity: AFFINITY,
        expected_generation: None,
        remaining_millis: 5_000,
        conditions: Vec::new(),
        mutations: vec![rdb_core::contracts::txn::Mutation::Put {
            key: root_key(TenantId(1), AFFINITY, object).to_bytes(),
            value,
            expected_version: None,
        }],
        acked_below: None,
        pinned: Vec::new(),
    }
}

/// A sound document envelope with `codec_version` `0x02` and its digest recomputed: a record
/// a newer build could have written, as `rdb_dev admin damage --as newer` writes it.
fn newer_build_document() -> Bytes {
    use rdb_value::envelope::{digest, seal, Kind, HEADER_LEN};
    let mut bytes = seal(Kind::Document, &[0xf6]).expect("a null fits").to_vec();
    bytes[2] = 0x02;
    let head: [u8; 8] = bytes[..8].try_into().expect("a header is 40 bytes");
    let sum = digest(&head, &bytes[HEADER_LEN..]);
    bytes[8..HEADER_LEN].copy_from_slice(&sum);
    Bytes::from(bytes)
}

fn clear_call(request: u64, object: &'static [u8]) -> ClientCall {
    compile_change(request, object, Change::Clear, None)
}

/// #55 (A-R150 F6, F7): a map write that names a newer build's entry is refused at the
/// compile, and the detail names the entry's key, after the op index and the newer-build
/// prefix: `op 0: written by a newer build: map entry "b": ...`. Two ops or one, the same
/// text. Nothing is sent, so `d` keeps its value; a page after `b` reads it.
/// Integration (~1 s): Trio, three map writes and a page.
#[config_log::retcd_test]
fn m9_s7_a_map_write_refused_for_a_newer_entry_names_its_key() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let made = Change::Map(vec![
        ElemOp::Put(text("a"), int(1)),
        ElemOp::Put(text("b"), int(2)),
        ElemOp::Put(text("d"), int(3)),
    ]);
    assert_eq!(
        trio.write(compile_change(1, b"m", made, Some(Expected::Absent))),
        Ok(2)
    );
    // `admin damage m --element "b" --as newer`: the entry's record, not the root's.
    let root = root_key(TenantId(1), AFFINITY, b"m");
    let damage = TxnRequest {
        mutations: vec![rdb_core::contracts::txn::Mutation::Put {
            key: rdb_value::keys::element_key(&root, &text("b")).expect("a text key"),
            value: newer_build_document(),
            expected_version: None,
        }],
        ..damage_write(2, b"m", Bytes::new())
    };
    assert_eq!(trio.sent(damage), Ok(3));

    let named = "op 0: written by a newer build: map entry \"b\": unknown codec version 0x02; \
                 open the folder with the newer build";
    for (request, ops) in [
        (
            3,
            vec![
                ElemOp::Put(text("d"), int(4)),
                ElemOp::Put(text("b"), int(9)),
            ],
        ),
        (4, vec![ElemOp::Put(text("b"), int(9))]),
    ] {
        match trio.ask(compile_change(request, b"m", Change::Map(ops), None)) {
            Answer::Error {
                error,
                request: sent,
            } => assert_eq!(
                (error.kind, error.detail.as_str(), sent.is_some()),
                (ErrorKind::IncompatibleVersion, named, false),
                "request {request}"
            ),
            other => panic!("not refused: {other:?}"),
        }
    }
    let after_b = Look::Members {
        after: Some(text("b")),
        limit: 10,
    };
    match trio.look(5, b"m", after_b) {
        Ok(Seen::Members(page)) => {
            let entries: Vec<_> = page
                .members
                .iter()
                .map(|member| (member.key.clone(), member.value.clone()))
                .collect();
            assert_eq!(entries, [(text("d"), Some(int(3)))], "d is unchanged");
        }
        other => panic!("members of m after b: {other:?}"),
    }
}

/// Ruling A-R154 (PR #56, Revobot Q4): a log line never carries a map key's text (ADR-0013).
/// A map write refused for a newer build's entry (#55) and a document write whose path
/// names a missing map key both answer the caller with the key readable, as before. Their
/// `client_compile` lines carry the refusal's kind only, and no line of the test's log
/// holds either key's text.
/// Integration (~1 s): Trio, two writes, a raw damage write and two refused compiles.
#[config_log::retcd_test]
fn m9_close_a_refused_write_answers_its_key_and_never_logs_it() {
    const METHOD: &str = "m9_close_a_refused_write_answers_its_key_and_never_logs_it";
    // Distinct from every other word in the log, the test's own name included.
    const ENTRY: &str = "kq7-entry";
    const SEGMENT: &str = "zx9-field";
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let made = Change::Map(vec![ElemOp::Put(text(ENTRY), int(1))]);
    assert_eq!(
        trio.write(compile_change(1, b"m", made, Some(Expected::Absent))),
        Ok(2)
    );
    let root = root_key(TenantId(1), AFFINITY, b"m");
    let damage = TxnRequest {
        mutations: vec![rdb_core::contracts::txn::Mutation::Put {
            key: rdb_value::keys::element_key(&root, &text(ENTRY)).expect("a text key"),
            value: newer_build_document(),
            expected_version: None,
        }],
        ..damage_write(2, b"m", Bytes::new())
    };
    assert_eq!(trio.sent(damage), Ok(3));
    let doc = Change::Doc(Delta(vec![Op::Replace(json_map(&[("a", int(1))]))]));
    assert_eq!(
        trio.write(compile_change(3, b"doc", doc, Some(Expected::Absent))),
        Ok(4)
    );

    let refused = [
        (
            4,
            b"m" as &'static [u8],
            Change::Map(vec![ElemOp::Put(text(ENTRY), int(9))]),
            ErrorKind::IncompatibleVersion,
            format!(
                "op 0: written by a newer build: map entry \"{ENTRY}\": unknown codec \
                 version 0x02; open the folder with the newer build"
            ),
        ),
        (
            5,
            b"doc",
            Change::Doc(Delta(vec![Op::Remove(path(&format!("/{SEGMENT}")))])),
            ErrorKind::InvalidArgument,
            format!("op 0: nothing at segment \"{SEGMENT}\""),
        ),
    ];
    for (request, object, change, kind, detail) in refused {
        match trio.ask(compile_change(request, object, change, None)) {
            Answer::Error { error, .. } => assert_eq!(
                (error.kind, error.detail.as_str()),
                (kind, detail.as_str()),
                "request {request}: the reply names the key readably"
            ),
            other => panic!("request {request} not refused: {other:?}"),
        }
    }

    let compiles = logged!(METHOD, "client_compile");
    for (request, kind) in [(4, "INCOMPATIBLE_VERSION"), (5, "INVALID_ARGUMENT")] {
        let lines: Vec<_> = compiles
            .iter()
            .filter(|line| line["request"] == request)
            .collect();
        assert!(
            !lines.is_empty(),
            "request {request}: no client_compile line"
        );
        for line in lines {
            assert!(
                line["compiled"] == false && line["kind"] == kind,
                "request {request}: {line}"
            );
        }
    }
    let leaks: Vec<String> = config_testkit::logs::lines_for_current_test(module_path!(), METHOD)
        .iter()
        .map(ToString::to_string)
        .filter(|line| line.contains(ENTRY) || line.contains(SEGMENT))
        .collect();
    assert!(leaks.is_empty(), "log lines carry a key's text: {leaks:#?}");
}

/// M9 S6 contracts 2 and 5 (tester-s6; A-R139, A-R140), guards 9, 10 and 14. A clear's answers
/// given without sending are reads, so a node gives them only while a read would be given.
/// Admitting: a healthy object and one with only upload chunks are CONDITION_FAILED, a
/// missing one `unchanged`, a newer build's one INCOMPATIBLE_VERSION, and a clear with an
/// expect INVALID_ARGUMENT, none naming a request. Fenced: healthy, chunks-only and missing
/// are refused as a plain read is, with `no_mutation`, never CONDITION_FAILED or
/// `unchanged`; newer is still
/// INCOMPATIBLE_VERSION, which claims nothing about the object; a damaged one compiles, is
/// sent, and the kernel refuses it. Once node 1 admits again that clear publishes.
/// Integration (~2 s): bootstrap's discovery window, then one fence on a 600 ms grant.
#[config_log::retcd_test]
fn m9_s6_a_clear_answers_without_sending_only_while_a_read_would_be_answered() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.budgets.grant_millis = 600;
        host.budgets.renew_millis = 100;
    });
    trio.bootstrap();
    trio.ready();
    let doc = || Change::Doc(Delta(vec![Op::Replace(text("v"))]));
    assert_eq!(
        trio.write(compile_change(1, b"healthy", doc(), None)),
        Ok(2)
    );
    assert_eq!(
        trio.write(compile_change(2, b"damaged", doc(), None)),
        Ok(3)
    );
    assert_eq!(trio.write(compile_change(3, b"newer", doc(), None)), Ok(4));
    let broken = Bytes::from_static(b"damaged");
    assert_eq!(trio.sent(damage_write(4, b"damaged", broken)), Ok(5));
    assert_eq!(
        trio.sent(damage_write(5, b"newer", newer_build_document())),
        Ok(6)
    );
    assert_eq!(
        trio.write(chunk_call(6, b"chunks", 0, b"HELL", None)),
        Ok(7)
    );

    let kind = |answer| refusal(answer).0;
    let (not_damaged, _, _, request) = refusal(trio.ask(clear_call(7, b"healthy")));
    assert_eq!((not_damaged, request), (ErrorKind::ConditionFailed, false));
    let (only_chunks, _, _, request) = refusal(trio.ask(clear_call(8, b"chunks")));
    assert_eq!((only_chunks, request), (ErrorKind::ConditionFailed, false));
    match trio.ask(clear_call(9, b"missing")) {
        Answer::Unchanged => {}
        other => panic!("a clear of nothing: {other:?}"),
    }
    let newer = (ErrorKind::IncompatibleVersion, RetryRule::Quarantine, false);
    let quarantined = |answer| {
        let (kind, retry, _, request) = refusal(answer);
        (kind, retry, request)
    };
    assert_eq!(quarantined(trio.ask(clear_call(10, b"newer"))), newer);
    let expect = Some(Expected::Version(2));
    assert_eq!(
        kind(trio.ask(compile_change(11, b"healthy", Change::Clear, expect))),
        ErrorKind::InvalidArgument,
        "a clear takes no expect"
    );

    trio.mem
        .failing_with(config_core::ConfigError::Unavailable {
            reason: "the control store is down".to_owned(),
        });
    trio.until("node 1 fenced", |trio| trio.status(0).fence.map(|_| ()));
    let read = ClientCall::Get {
        identity: identity(12),
        object: Bytes::from_static(b"healthy"),
        look: Look::Doc,
    };
    let refused = match trio.ask(read) {
        Answer::Read {
            outcome: ReadServiceOutcome::Rejected(kind),
            ..
        } => crate::db::refused_read(kind),
        other => panic!("a read while fenced: {other:?}"),
    };
    assert_eq!(refused.kind, ErrorKind::LeaseExpired, "{refused:?}");
    for (request, object) in [
        (13, &b"healthy"[..]),
        (14, &b"chunks"[..]),
        (15, &b"missing"[..]),
    ] {
        assert_eq!(
            refusal(trio.ask(clear_call(request, object))),
            (refused.kind, refused.retry, true, false),
            "{} while fenced, as the read but sending nothing",
            String::from_utf8_lossy(object)
        );
    }
    assert_eq!(
        quarantined(trio.ask(clear_call(16, b"newer"))),
        newer,
        "newer while fenced"
    );
    let request = trio.compiled(clear_call(17, b"damaged"));
    assert_eq!(
        trio.sent(request),
        Err(Refused::ByKernel(ErrorKind::LeaseExpired)),
        "a damaged object's clear is sent, and the fenced kernel refuses it"
    );

    trio.mem.stop_failing();
    trio.ready();
    assert!(trio.write(clear_call(18, b"damaged")).is_ok(), "cleared");
    match trio.ask(clear_call(19, b"damaged")) {
        Answer::Unchanged => {}
        other => panic!("a clear once cleared: {other:?}"),
    }
}

/// M9 S6 review F-006: the `clear_compiled` line names the root `deleted` when the batch
/// deletes it, `guarded` when a version condition keeps it, and `absent` when the guard
/// says there is none; `deletes` and `faulting` are the batch's own counts.
/// Unit (~10 ms): `log_clear` on three batches built by hand.
#[config_log::retcd_test]
fn m9_s6_the_clear_line_names_what_the_batch_does_to_the_root() {
    const METHOD: &str = "m9_s6_the_clear_line_names_what_the_batch_does_to_the_root";
    let root = root_key(TenantId(1), AFFINITY, b"o");
    let element = Bytes::from_static(b"an element key");
    let delete = |key: Bytes| Mutation::Delete {
        key,
        expected_version: Some(3),
    };
    let batches = [
        (
            1,
            vec![delete(element.clone()), delete(root.to_bytes())],
            vec![Condition::VersionEquals {
                key: root.to_bytes(),
                version: 3,
            }],
            1,
        ),
        (
            2,
            vec![delete(element.clone())],
            vec![Condition::VersionEquals {
                key: root.to_bytes(),
                version: 3,
            }],
            0,
        ),
        (
            3,
            vec![delete(element)],
            vec![Condition::Absent {
                key: root.to_bytes(),
            }],
            1,
        ),
    ];
    for (request, mutations, conditions, faulting) in batches {
        let cleared = Cleared {
            compiled: Compiled {
                mutations,
                conditions,
            },
            damage: Corrupt::VersionWithoutValue,
            faulting,
        };
        log_clear(NodeId(1), identity(request), &root, &cleared);
    }

    let lines = logged!(METHOD, "clear_compiled");
    let seen: Vec<_> = lines
        .iter()
        .map(|line| {
            (
                line["request"].clone(),
                line["root"].clone(),
                line["deletes"].clone(),
                line["faulting"].clone(),
                line["key_hex"].clone(),
            )
        })
        .collect();
    let want = |request: u64, root: &str, deletes: u64, faulting: u64| {
        (
            request.into(),
            root.into(),
            deletes.into(),
            faulting.into(),
            "6f".into(),
        )
    };
    assert_eq!(
        seen,
        vec![
            want(1, "deleted", 2, 1),
            want(2, "guarded", 1, 0),
            want(3, "absent", 1, 1),
        ],
        "{lines:?}"
    );
}

/// D9 (ADR-0013): the damage lines name the object by `key_hex`, its id's first 32 bytes
/// as lowercase hex, and never by its text. `stored_damage`, with and without a chunk, and
/// `clear_compiled` for the tester's `zqa` and for a 40-byte id cut to 32 bytes.
/// Unit (~10 ms): the two log functions called by hand.
#[config_log::retcd_test]
fn d9_damage_lines_carry_the_object_as_key_hex_capped_at_32_bytes() {
    const METHOD: &str = "d9_damage_lines_carry_the_object_as_key_hex_capped_at_32_bytes";
    let long = [b'z'; 40];
    let error = ApiError::new(ErrorKind::CorruptHistory, "damaged".to_owned());
    for (request, object) in [(1, &b"zqa"[..]), (2, &long[..])] {
        let root = root_key(TenantId(1), AFFINITY, object);
        log_stored_damage(NodeId(1), &root, &error, None, None);
        log_stored_damage(NodeId(1), &root, &error, None, Some(([7; 16], 2)));
        let cleared = Cleared {
            compiled: Compiled {
                mutations: vec![Mutation::Delete {
                    key: root.to_bytes(),
                    expected_version: Some(3),
                }],
                conditions: Vec::new(),
            },
            damage: Corrupt::VersionWithoutValue,
            faulting: 1,
        };
        log_clear(NodeId(1), identity(request), &root, &cleared);
    }

    let short = "7a7161".to_owned();
    let capped = "7a".repeat(32);
    for (message, want) in [
        ("stored_damage", vec![&short, &short, &capped, &capped]),
        ("clear_compiled", vec![&short, &capped]),
    ] {
        let lines = logged!(METHOD, message);
        let named: Vec<_> = lines.iter().map(|line| line["key_hex"].clone()).collect();
        let want: Vec<serde_json::Value> =
            want.into_iter().map(|hex| hex.as_str().into()).collect();
        assert_eq!(named, want, "{message}: {lines:?}");
        for line in &lines {
            let text = line.to_string();
            assert!(
                line.get("object").is_none() && line.get("root_unparsed").is_none(),
                "{message} names the object only by key_hex: {text}"
            );
            assert!(
                !text.contains("zqa") && !text.contains("zzzz"),
                "{message} carries the object's text: {text}"
            );
        }
    }
}

/// D9b: the damage lines name the damage by a code, never by the error's text. A map
/// entry's damage quotes the app's key in its `Display` ("map entry \"zqk-secret\": ..."),
/// so the refusal's detail is left out of `stored_damage`, and `clear_compiled` logs
/// `damage_code`. The damage is built by hand: no stored record carries it to these sites
/// today, so the row pins the sites, whatever text a refusal carries.
/// Unit (~10 ms): the two log functions called by hand.
#[config_log::retcd_test]
fn d9b_damage_lines_log_a_code_and_never_the_error_text() {
    const METHOD: &str = "d9b_damage_lines_log_a_code_and_never_the_error_text";
    let entry = Corrupt::Entry {
        key: Value::Text("zqk-secret".to_owned()),
        error: rdb_value::envelope::EnvelopeError::UnknownKind(0),
    };
    let text = entry.to_string();
    assert!(
        text.contains("zqk-secret"),
        "the fixture quotes the key: {text}"
    );
    let root = root_key(TenantId(1), AFFINITY, b"m");
    let refused = ApiError::new(ErrorKind::CorruptHistory, text);
    log_stored_damage(NodeId(1), &root, &refused, None, None);
    log_stored_damage(NodeId(1), &root, &refused, Some(damage_code(&entry)), None);
    log_stored_damage(
        NodeId(1),
        &root,
        &refused,
        Some(damage_code(&entry)),
        Some(([7; 16], 2)),
    );
    let cleared = Cleared {
        compiled: Compiled {
            mutations: vec![Mutation::Delete {
                key: root.to_bytes(),
                expected_version: Some(3),
            }],
            conditions: Vec::new(),
        },
        damage: entry,
        faulting: 1,
    };
    log_clear(NodeId(1), identity(1), &root, &cleared);

    let stored = logged!(METHOD, "stored_damage");
    let codes: Vec<_> = stored.iter().map(|line| line["damage"].clone()).collect();
    assert_eq!(
        codes,
        [serde_json::Value::Null, "entry".into(), "entry".into()],
        "{stored:?}"
    );
    let clear = logged!(METHOD, "clear_compiled");
    assert_eq!(clear.len(), 1, "{clear:?}");
    assert_eq!(clear[0]["damage"], "entry", "{clear:?}");
    let lines = config_testkit::logs::lines_for_current_test(module_path!(), METHOD);
    for line in &lines {
        let text = line.to_string();
        assert!(
            !text.contains("zqk") && line.get("detail").is_none(),
            "a log line carries the error's text: {text}"
        );
    }
}

/// M9 S6 review Q-1: "nothing to clear" is answered only for a view P1 has published in the
/// view's own generation. A published position of another generation, or none yet, is
/// UNKNOWN_OUTCOME naming the generations; the same seq in the view's generation is Ok.
/// The host adopts a generation from A1 and P1 takes its lineage from recovery, so the two
/// can differ for a step (`deliver`'s AdoptAuthority; publication's `on_recovered`).
/// Unit (~50 ms): one empty engine, no thread.
#[test]
fn m9_s6_nothing_to_clear_needs_a_publish_in_the_views_own_generation() {
    const CLAIM: &str = "nothing to clear";
    const THEN: &str = "if an earlier clear sent deletes, ask its status";
    let dir = config_testkit::fs::temp_dir();
    let engine = RocksEngine::open(dir.path().join("node")).expect("open engine");
    let view = engine
        .snapshot(PartitionId(1), Generation(1), SnapshotHandle(0))
        .expect("snapshot");
    assert_eq!(
        view_published(&view, Some((Generation(1), view.at())), CLAIM, THEN),
        Ok(())
    );
    let other =
        view_published(&view, Some((Generation(0), view.at())), CLAIM, THEN).expect_err("other");
    assert_eq!(other.kind, ErrorKind::UnknownOutcome, "{other:?}");
    assert!(
        other.detail.contains("of generation 0, not generation 1"),
        "{other:?}"
    );
    let none = view_published(&view, None, CLAIM, THEN).expect_err("none");
    assert_eq!(none.kind, ErrorKind::UnknownOutcome, "{none:?}");
    assert!(none.detail.contains("nothing published yet"), "{none:?}");
}

/// M9 S6 contract 5, held copies, guard 15: with both secondaries cut off, node 1 applies a
/// clear that cannot publish. The clear asked again meanwhile finds nothing stored, above
/// P1's published seq, so it is UNKNOWN_OUTCOME naming no request, never `unchanged`: the
/// first clear may still be lost. After the heal the held clear publishes, and the next is
/// `unchanged`. Trio, about 1 s.
#[test]
fn m9_s6_a_clear_asked_again_before_the_first_publishes_is_an_unknown_outcome() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let doc = Change::Doc(Delta(vec![Op::Replace(text("v"))]));
    assert_eq!(trio.write(compile_change(1, b"d", doc, None)), Ok(2));
    let broken = Bytes::from_static(b"damaged");
    assert_eq!(trio.sent(damage_write(2, b"d", broken)), Ok(3));
    let request = trio.compiled(clear_call(3, b"d"));
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    let held = trio.call(ClientCall::Resend { request });
    trio.until("node 1 applied the clear", |trio| {
        (trio.head() == Some(4)).then_some(())
    });

    assert_eq!(
        refusal(trio.ask(clear_call(4, b"d"))),
        (
            ErrorKind::UnknownOutcome,
            RetryRule::QueryStatus,
            false,
            false
        ),
        "nothing is stored, and that is not published"
    );
    assert!(held.try_recv().is_err(), "no copy acknowledged the clear");

    trio.links.heal_all();
    match trio.answer(&held) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(4)),
        other => panic!("the held clear after the heal: {other:?}"),
    }
    match trio.ask(clear_call(5, b"d")) {
        Answer::Unchanged => {}
        other => panic!("a clear once published: {other:?}"),
    }
}
/// Review R-2 (PR #53): "not damaged" is a read of the step view, which holds writes applied
/// and not yet published. With both secondaries cut off, an app put repairs a map's damaged
/// entry and applies on node 1 only. A clear asked meanwhile finds the map healthy, but the
/// repair may still be lost, so it is UNKNOWN_OUTCOME naming no request, never
/// CONDITION_FAILED. After the heal the repair publishes, and the clear is CONDITION_FAILED.
/// Trio, about 1 s.
#[test]
fn m9_s6_a_clear_that_finds_an_unpublished_repair_is_an_unknown_outcome() {
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast);
    trio.bootstrap();
    trio.ready();
    let put = |n| Change::Map(vec![ElemOp::Put(text("a"), int(n))]);
    assert_eq!(trio.write(compile_change(1, b"m", put(1), None)), Ok(2));
    // The entry's bytes broken, and the root written again in the same commit, so the
    // entry is not newer than its root and an app put still compiles over it.
    let root = root_key(TenantId(1), AFFINITY, b"m");
    let mut fields = rdb_value::value::Map::new();
    fields.insert(rdb_value::value::MapKey::new("keys"), int(1));
    fields.insert(rdb_value::value::MapKey::new("count"), int(1));
    let payload = rdb_value::cbor::encode(&Value::Map(fields)).expect("a root encodes");
    let sealed =
        rdb_value::envelope::seal(rdb_value::envelope::Kind::Map, &payload).expect("a root fits");
    let damage = TxnRequest {
        api_version: API_VERSION,
        identity: identity(2),
        affinity: AFFINITY,
        expected_generation: None,
        remaining_millis: 5_000,
        conditions: Vec::new(),
        mutations: vec![
            rdb_core::contracts::txn::Mutation::Put {
                key: root.to_bytes(),
                value: sealed,
                expected_version: None,
            },
            rdb_core::contracts::txn::Mutation::Put {
                key: rdb_value::keys::element_key(&root, &text("a")).expect("a text key"),
                value: Bytes::from_static(b"damaged"),
                expected_version: None,
            },
        ],
        acked_below: None,
        pinned: Vec::new(),
    };
    assert_eq!(trio.sent(damage), Ok(3));
    let members = Look::Members {
        after: None,
        limit: 10,
    };
    match trio.look(10, b"m", members) {
        Err(error) => {
            assert_eq!(error.kind, ErrorKind::CorruptHistory, "{error:?}");
            // #59, A-R230: the caller's text names its object and the damaged entry's key.
            assert!(
                error.detail.starts_with("object \"m\": ") && error.detail.contains("\"a\""),
                "{error:?}"
            );
        }
        other => panic!("members of a damaged map: {other:?}"),
    }
    let repair = trio.compiled(compile_change(3, b"m", put(2), None));
    trio.links.hold(NodeId(1), NodeId(2));
    trio.links.hold(NodeId(1), NodeId(3));
    let held = trio.call(ClientCall::Resend { request: repair });
    trio.until("node 1 applied the repair", |trio| {
        (trio.head() == Some(4)).then_some(())
    });

    assert_eq!(
        refusal(trio.ask(clear_call(4, b"m"))),
        (
            ErrorKind::UnknownOutcome,
            RetryRule::QueryStatus,
            false,
            false
        ),
        "the map reads back cleanly only through a repair not yet published"
    );
    assert!(held.try_recv().is_err(), "no copy acknowledged the repair");

    trio.links.heal_all();
    match trio.answer(&held) {
        Answer::Txn { result, .. } => assert_eq!(result.seq, Seq(4)),
        other => panic!("the held repair after the heal: {other:?}"),
    }
    assert_eq!(
        refusal(trio.ask(clear_call(5, b"m"))),
        (
            ErrorKind::ConditionFailed,
            RetryRule::Definitive,
            true,
            false
        ),
        "once the repair is published the map is not damaged"
    );
}
/// Ruling A-R184, the write gate: the lead asked that a second recovery in one open go
/// through the gate too, so a write is refused, and nothing is sent, while the primary has
/// not heard that the open counter marks its adopted generation activated, even with a grant
/// held. `admits` says false meanwhile, so `wait_ready` waits for the mark. Here the mark
/// is taken back by hand, which is what a second recovery's commit leaves until its own mark
/// lands. Cut: `unactivated` answers `None`, and the put publishes. Trio, about 1 s.
#[test]
fn a_write_under_a_generation_not_marked_activated_is_refused_and_sends_nothing() {
    let dir = config_testkit::fs::temp_dir();
    let instance = [7_u8; INSTANCE_LEN];
    let mut trio = Trio::new(dir.path(), |host| {
        Trio::fast(host);
        host.instance = Some(instance);
    });
    trio.rt
        .block_on(crate::admin::count_open(
            &*trio.store,
            &instance,
            true,
            crate::admin::COUNT_WITHIN,
        ))
        .expect("the open counter");
    trio.bootstrap();
    trio.ready();
    let add = |n| Change::Set(vec![ElemOp::Add(int(n))]);
    assert_eq!(
        trio.write(compile_change(1, b"s", add(1), Some(Expected::Absent))),
        Ok(2),
        "a write once the mark is heard"
    );
    let marked = trio.nodes[0]
        .0
        .activation
        .get_mut(&PartitionId(1))
        .expect("node 1 asked for a mark");
    assert_eq!(marked.marked, Generation(1), "{marked:?}");
    marked.marked = Generation(0);
    assert!(
        trio.status(0).authority.starts_with("held"),
        "{:?}",
        trio.status(0)
    );
    assert_eq!(trio.status(0).admits, Some(false), "{:?}", trio.status(0));
    // review-r184 A1: the refusal reads as one sentence, no run of spaces from a wrap.
    let refusal = trio.nodes[0]
        .0
        .unactivated(PartitionId(1))
        .expect("the refusal");
    assert_eq!(
        refusal.detail,
        "generation 1 of partition 1 on node 1 is not yet marked activated in the open \
         counter; nothing was sent"
    );
    let head = trio.head();
    assert_eq!(
        trio.write(compile_change(2, b"s", add(2), None)),
        Err(Refused::ByKernel(ErrorKind::Unavailable)),
        "refused before it is sent"
    );
    assert_eq!(trio.head(), head, "nothing was sent");
}

/// #42 (L-R178e, A-R256 (2)): a revocation an earlier process made durable reaches the new
/// process's A1 before anything else, so the new A1 keeps it. At start the host reads each from
/// the store and queues it as `EpochRevocationRestored`; a fresh A1 adds it to its revoked set.
/// The second host is the real start: `run` replays before it reads any mail. Cuts: the replay
/// queues nothing, and A1's set is empty, red; `run` never calls it, and no replay line counts
/// one, red. Unit (~200 ms): two hosts, one after the other, over one store.
#[config_log::retcd_test]
fn m10_s2_a_revocation_persisted_before_a_restart_is_restored_to_a1_at_start() {
    const METHOD: &str =
        "m10_s2_a_revocation_persisted_before_a_restart_is_restored_to_a1_at_start";
    let rt = current_thread();
    let dir = config_testkit::fs::temp_dir();
    let path = dir.path().join("node");
    {
        let mut host = host_over(&path, &rt);
        host.engine
            .persist_epoch_revocation(PartitionId(1), OwnerEpoch(1))
            .expect("persist");
        assert!(host.authority.view().revoked_epochs.is_empty());
        host.restore_revocations();
        host.drain();
        assert!(host.fault.is_none(), "{:?}", host.fault);
        assert_eq!(
            host.authority.view().revoked_epochs,
            BTreeSet::from([(PartitionId(1), OwnerEpoch(1))]),
            "A1 holds the restored revocation"
        );
    }
    let host = host_over(&path, &rt);
    let (tx, rx) = mpsc::channel();
    tx.send(Msg::Stop).expect("send");
    host.run(&rx);
    let counts: Vec<_> = logged!(METHOD, "host_duty")
        .into_iter()
        .filter(|line| line["kind"] == "revocation_replay")
        .map(|line| line["count"].clone())
        .collect();
    assert_eq!(
        counts,
        [serde_json::json!(1), serde_json::json!(1)],
        "the direct call, then `run` at the second start, each replayed the one revocation"
    );
}

/// A-R399 (tester-final D3, S2-3 cut arm): node 1 is cut from rEtcd, its peers are not. Its
/// grant runs out and A1 fences it for good; the watches stop with the fence (A-R96). While
/// it is cut, `lr promote 2 --drop 1` freezes `grants/1` and names node 2 in `partitions/1`.
/// Then the cut ends. Returns the trio once node 1 has read the owner after the fence, and
/// the put node 1 answered while it was cut.
fn cut_owner_promoted_away(dir: &std::path::Path) -> (Trio, Answer) {
    use crate::net::faults::{CutStore, Faults};
    use config_core::{ConfigStore as _, GetRequest, MutationOutcome, PutRequest};
    use rdb_core::authority::grant::GrantRecord;
    use rdb_core::authority::partition::{PartitionRecord, PartitionRoot};
    use rdb_core::contracts::control::ControlKey;

    let mut trio = Trio::new(dir, |host| {
        Trio::fast(host);
        host.budgets.grant_millis = 600;
        host.budgets.renew_millis = 100;
    });
    // Node 1 alone talks to the store through a cut it can be given, as `lr cut-control 1`.
    let faults = Faults::new();
    let cut: Arc<dyn config_core::ConfigStore> =
        CutStore::new(Arc::clone(&trio.store), Arc::clone(&faults));
    trio.nodes[0].0.control =
        ControlAdapter::new(cut, trio.rt.handle().clone(), Arc::clone(&trio.links));
    trio.bootstrap();
    trio.ready();
    faults.apply("cut-control");
    trio.until("node 1 fenced", |trio| trio.status(0).fence.map(|_| ()));
    let cut_answer = trio.ask(put(1, b"a", b"1", None));

    // The promote, made by another process while node 1 cannot hear it.
    let read = |key: &Bytes| {
        trio.rt
            .block_on(trio.mem.get(GetRequest { key: key.clone() }))
            .expect("get")
            .record
            .expect("the record exists")
    };
    let grants = Bytes::from(ControlKey::Grant(NodeId(1)).encode());
    let found = read(&grants);
    let mut grant = GrantRecord::decode(&found.value).expect("a grant record");
    grant.frozen = true;
    let partitions = Bytes::from(ControlKey::Partition(PartitionId(1)).encode());
    let owned = read(&partitions);
    let mut record = PartitionRecord::decode(&owned.value).expect("a partition record");
    record.owner = NodeId(2);
    // The trio's bootstrap writes no root; keep one when there is.
    let value = PartitionRoot::decode(&owned.value)
        .map_or_else(|| record.encode(), |root| record.encode_with(&root));
    for (key, value, revision) in [
        (grants, grant.encode(), found.mod_revision),
        (partitions, value, owned.mod_revision),
    ] {
        let put = trio
            .rt
            .block_on(trio.mem.put(PutRequest {
                key,
                value,
                expected_mod_revision: Some(revision),
                dedup: None,
            }))
            .expect("the promote's put");
        assert_eq!(put.outcome, MutationOutcome::Applied);
    }

    faults.apply("");
    trio.until("node 1 read the owner after its fence", |trio| {
        trio.nodes[0]
            .0
            .owner_after_fence
            .get(&PartitionId(1))
            .copied()
    });
    (trio, cut_answer)
}

/// A-R399 (tester-final D3): the old owner learns it was dropped. While cut it refuses a put
/// `LEASE_EXPIRED`, provably mutating nothing; once the cut ends it reads `partitions/1`, logs
/// `host_owner_learned` with node 2, and refuses the next put `NOT_PRIMARY` with node 2 as the
/// hint, still mutating nothing, so an app routed here re-reads the owner.
/// Cut: the fence starts no owner read (`read_owner_after_fence` not called): the wait for
/// the read times out, red.
/// Integration (~3 s): bootstrap's discovery window, a fence on a 600 ms grant, the read.
#[config_log::retcd_test]
fn m10_s2_a_fenced_owner_cut_from_control_answers_not_primary_once_it_reads_the_new_owner() {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, cut) = cut_owner_promoted_away(dir.path());
    let Answer::Error { error, .. } = cut else {
        panic!("a put while cut: {cut:?}");
    };
    assert_eq!(
        (error.kind, error.no_mutation),
        (ErrorKind::LeaseExpired, true),
        "while cut: {error:?}"
    );
    assert_eq!(
        trio.nodes[0].0.owner_after_fence.get(&PartitionId(1)),
        Some(&Some(NodeId(2))),
        "node 1 read node 2 as owner"
    );
    match trio.ask(put(2, b"a", b"2", None)) {
        Answer::Error { error, .. } => {
            assert_eq!(
                (error.kind, error.retry, error.no_mutation),
                (ErrorKind::NotPrimary, RetryRule::RefreshRoute, true),
                "{error:?}"
            );
            assert!(error.detail.contains("names node 2"), "{error:?}");
            // review-s2 F-014: the same text reaches a resend, which may have applied on an
            // earlier send, so it claims only this send.
            assert!(
                error.detail.ends_with("this send wrote nothing"),
                "{error:?}"
            );
        }
        other => panic!("a put after the read: {other:?}"),
    }
}

/// A-R399 condition (3), as far as a row can show it: after the read, node 1 stays fenced and
/// not admitting, and every put for half a second is refused with nothing mutated; its engine
/// holds nothing past what it held at the fence. What it does not show (critic-d3 A5): that
/// the read cannot re-arm A1. The grant is frozen, so a re-arm could not admit here anyway;
/// that half rests on the code (`Msg::OwnerLearned` sends A1 no event).
/// Cut: `redirect` sets `no_mutation = false` on its `NOT_PRIMARY`: put 10 is refused claiming
/// it may have mutated, red. (The row above goes red too.)
/// Integration (~3.5 s): as the row above, then half a second of puts.
#[config_log::retcd_test]
fn m10_s2_a_fenced_owner_refuses_every_put_with_no_mutation_after_it_reads_the_owner() {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _) = cut_owner_promoted_away(dir.path());
    let before = trio.status(0);
    let until = Instant::now() + Duration::from_millis(500);
    let mut request = 10;
    while Instant::now() < until {
        match trio.ask(put(request, b"a", b"3", None)) {
            Answer::Error { error, .. } => {
                assert!(error.no_mutation, "put {request}: {error:?}");
                assert!(
                    matches!(error.kind, ErrorKind::NotPrimary | ErrorKind::LeaseExpired),
                    "put {request}: {error:?}"
                );
            }
            other => panic!("put {request} after the read was not refused: {other:?}"),
        }
        request += 1;
    }
    let after = trio.status(0);
    assert_eq!(
        (after.fence.is_some(), after.admits),
        (true, Some(false)),
        "{after:?}"
    );
    assert_eq!(after.holds, before.holds, "nothing applied after the read");
}

/// A-R399 condition (2), lead leftover after critic: `redirect` rewrites only a fenced node's
/// `LEASE_EXPIRED` that provably mutated nothing, and only toward another node. Each guard gets
/// its own answer, and each must leave the answer as it was:
/// - not `LEASE_EXPIRED` (`!expired`): an `Unchanged`, and an `OVERLOADED` error;
/// - `LEASE_EXPIRED` that may have mutated (`error.no_mutation`): a `NOT_PRIMARY` with
///   `no_mutation = true` would tell the app nothing was written when something may have been;
/// - a node that is not fenced (`fence_of(..).is_none()`): node 2, given a learned owner;
/// - a learned owner that is this node (`owner == self.node`).
///
/// The positive control first: node 1's own refusal is rewritten, so the setup is live.
/// Cuts, one at a time: drop `!expired`; drop `&& error.no_mutation`; drop the fenced check;
/// drop the owner check. Each turns its case into `NOT_PRIMARY`, red. And the read path
/// (critic-d3 A4): the `Read` arm never matches: the rejected read stays `LEASE_EXPIRED`, red.
/// Integration (~3 s): `cut_owner_promoted_away`, then direct calls.
#[config_log::retcd_test]
fn m10_s2_a_fenced_owner_rewrites_only_a_lease_expired_that_mutated_nothing() {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, cut) = cut_owner_promoted_away(dir.path());
    let Answer::Error { error: lease, .. } = cut else {
        panic!("a put while cut: {cut:?}");
    };
    assert_eq!(
        (lease.kind, lease.no_mutation),
        (ErrorKind::LeaseExpired, true)
    );
    let error = |kind: ErrorKind, no_mutation: bool| Answer::Error {
        error: ApiError {
            kind,
            no_mutation,
            ..lease.clone()
        },
        request: None,
    };
    let shape = |answer: &Answer| match answer {
        Answer::Error { error, .. } => {
            format!("{:?} no_mutation={}", error.kind, error.no_mutation)
        }
        Answer::Unchanged => "Unchanged".to_owned(),
        Answer::Read { outcome, .. } => format!("Read {outcome:?}"),
        other => format!("{other:?}"),
    };
    let read = |outcome: ReadServiceOutcome| Answer::Read {
        outcome,
        seen: Seen::Absent,
        generation: Generation(1),
        at: Seq(0),
        recovery_mode: false,
    };
    let p1 = PartitionId(1);

    // Positive control: the case the rewrite is for.
    let rewritten = trio.nodes[0]
        .0
        .redirect(p1, error(ErrorKind::LeaseExpired, true));
    assert_eq!(shape(&rewritten), "NotPrimary no_mutation=true", "control");

    // `!expired`: anything else is not rewritten.
    let kept = trio.nodes[0].0.redirect(p1, Answer::Unchanged);
    assert_eq!(shape(&kept), "Unchanged", "!expired: Unchanged");
    let kept = trio.nodes[0]
        .0
        .redirect(p1, error(ErrorKind::Overloaded, true));
    assert_eq!(
        shape(&kept),
        "Overloaded no_mutation=true",
        "!expired: Overloaded"
    );

    // `error.no_mutation`: a refusal that may have mutated keeps saying so.
    let kept = trio.nodes[0]
        .0
        .redirect(p1, error(ErrorKind::LeaseExpired, false));
    assert_eq!(
        shape(&kept),
        "LeaseExpired no_mutation=false",
        "no_mutation: may have mutated"
    );

    // The read path (critic-d3 A4): P1's `LEASE_EXPIRED` rejection is rewritten, as a put's is;
    // a served read is an answer, never a refusal, and is left alone.
    let rewritten = trio.nodes[0].0.redirect(
        p1,
        read(ReadServiceOutcome::Rejected(ErrorKind::LeaseExpired)),
    );
    assert_eq!(
        shape(&rewritten),
        "NotPrimary no_mutation=true",
        "read: rejected"
    );
    let kept = trio.nodes[0]
        .0
        .redirect(p1, read(ReadServiceOutcome::Served));
    assert_eq!(shape(&kept), "Read Served", "read: served");

    // The fenced check: node 2 holds its grant, so a learned owner changes nothing there.
    assert!(trio.status(1).fence.is_none(), "node 2 is not fenced");
    trio.nodes[1]
        .0
        .owner_after_fence
        .insert(p1, Some(NodeId(3)));
    let kept = trio.nodes[1]
        .0
        .redirect(p1, error(ErrorKind::LeaseExpired, true));
    assert_eq!(
        shape(&kept),
        "LeaseExpired no_mutation=true",
        "fenced check: node 2"
    );

    // The owner check: a record naming this node never points the app back at itself.
    trio.nodes[0]
        .0
        .owner_after_fence
        .insert(p1, Some(NodeId(1)));
    let kept = trio.nodes[0]
        .0
        .redirect(p1, error(ErrorKind::LeaseExpired, true));
    assert_eq!(
        shape(&kept),
        "LeaseExpired no_mutation=true",
        "owner check: itself"
    );
}

/// critic-d3 A3: a learned owner belongs to one fence. An answer to a read begun under an
/// earlier fence is dropped, and once A1 holds a grant again (a view while not fenced) the
/// learned owners are forgotten, so a later fence of either scope never reuses them.
/// Cuts: the handler takes any `fence`: the stale answer replaces node 2's owner, red;
/// `report_view` keeps the map: node 2's entry survives its view, red.
/// Integration (~3 s): `cut_owner_promoted_away`, then direct calls.
#[config_log::retcd_test]
fn m10_s2_a_learned_owner_is_forgotten_once_a1_leaves_the_fence_it_was_learned_under() {
    let dir = config_testkit::fs::temp_dir();
    let (mut trio, _) = cut_owner_promoted_away(dir.path());
    let p1 = PartitionId(1);
    let host = &mut trio.nodes[0].0;
    let current = host.owner_fence;
    assert!(current >= 1, "node 1 counted its fence");
    let stale = Msg::OwnerLearned {
        boot: host.boot,
        partition: p1,
        fence: current - 1,
        owner: Some(NodeId(3)),
    };
    host.handle(stale);
    assert_eq!(
        host.owner_after_fence.get(&p1),
        Some(&Some(NodeId(2))),
        "a read from an earlier fence changes nothing"
    );

    // Node 2 is not fenced: a view it reports forgets what a fence once taught it. Node 2
    // serves no partition, so it is handed the view node 1 reported before its fence; only
    // node 2's own A1 state decides.
    let view = *host
        .authority_views
        .get(&p1)
        .expect("node 1 reported a view before its fence");
    assert!(trio.status(1).fence.is_none(), "node 2 is not fenced");
    let host = &mut trio.nodes[1].0;
    host.owner_after_fence.insert(p1, Some(NodeId(3)));
    host.report_view(&view);
    assert!(
        host.owner_after_fence.is_empty(),
        "held again: {:?}",
        host.owner_after_fence
    );
}

/// A-R446 (PR #63 review, option (b)): `bind_routed_fence`, one check at a time. `true_fence`
/// is a fence every check holds for; each row below breaks one fact and leaves the rest true,
/// so deleting that one check turns its row red.
mod routed_fence {
    use super::{bind_routed_fence, test_budgets, FenceFacts};
    use rdb_core::authority::clock::{expiry_proven, ClockView};
    use rdb_core::authority::grant::GrantRecord;
    use rdb_core::authority::partition::{PartitionLifecycle, PartitionRecord};
    use rdb_core::contracts::authority::{FencingProof, Revocation};
    use rdb_core::contracts::ids::{
        AuthorityGeneration, BootId, ConfigVersion, Generation, GrantId, NodeId, OwnerEpoch,
        PartitionId, Revision,
    };
    use rdb_core::contracts::time::Tick;

    use crate::clock::HostClock;

    const NOW: Tick = Tick(1_000_000);
    /// One minute before `NOW`: proven past on `test_budgets`.
    const EXPIRED: i64 = 940_000;

    fn true_fence() -> (FencingProof, FenceFacts) {
        let budgets = test_budgets();
        let mut view = ClockView::new();
        view.accept(HostClock::control_time(NOW, &budgets));
        let revocation =
            expiry_proven(&view, EXPIRED, NOW, &budgets).expect("an expiry a minute ago");
        let facts = FenceFacts {
            record: PartitionRecord {
                partition: PartitionId(1),
                owner: NodeId(1),
                generation: Generation(3),
                owner_epoch: OwnerEpoch(2),
                config_version: ConfigVersion(4),
                lifecycle: PartitionLifecycle::Fencing,
            },
            owner_boot: BootId(7),
            mod_revision: Revision(40),
            grant: Some(GrantRecord {
                grant: GrantId(5),
                node: NodeId(1),
                boot: BootId(7),
                authority_generation: AuthorityGeneration::default(),
                expiry_utc_ms: EXPIRED,
                frozen: true,
            }),
        };
        let proof = FencingProof {
            partition: PartitionId(1),
            prior_generation: Generation(3),
            prior_owner_epoch: OwnerEpoch(2),
            prior_grant_id: GrantId(5),
            prior_boot_id: BootId(7),
            revocation,
            control_revision: Revision(40),
            decision_tick: NOW,
        };
        (proof, facts)
    }

    fn bind(proof: &FencingProof, facts: FenceFacts) -> Result<(), &'static str> {
        bind_routed_fence(proof, Ok(facts), NOW, &test_budgets()).map_err(|(reason, _)| reason)
    }

    /// The positive control: a fence every check holds for binds.
    /// Cut: any check refuses a true fence, red.
    /// Unit (<1 ms).
    #[test]
    fn m10_s2_r446_a_true_routed_fence_binds() {
        let (proof, facts) = true_fence();
        assert_eq!(bind(&proof, facts), Ok(()));
    }

    /// Check 1: a fence for another revision of `partitions/1`, or another partition.
    /// Cut: check 1 deleted, red.
    /// Unit (<1 ms).
    #[test]
    fn m10_s2_r446_1_a_fence_at_another_revision_is_refused() {
        let (proof, mut facts) = true_fence();
        facts.mod_revision = Revision(41);
        assert_eq!(bind(&proof, facts), Err("revision"));
        let (mut proof, facts) = true_fence();
        proof.partition = PartitionId(2);
        assert_eq!(bind(&proof, facts), Err("revision"));
    }

    /// Check 2: a `Serving` or `FencingDrained` record, with every grant fact true.
    /// Cut: check 2 deleted, red.
    /// Unit (<1 ms).
    #[test]
    fn m10_s2_r446_2_a_fence_over_a_record_not_fencing_is_refused() {
        for lifecycle in [
            PartitionLifecycle::Serving,
            PartitionLifecycle::FencingDrained,
        ] {
            let (proof, mut facts) = true_fence();
            facts.record.lifecycle = lifecycle;
            assert_eq!(bind(&proof, facts), Err("not_fencing"), "{lifecycle:?}");
        }
    }

    /// Check 3: the proof's prior generation or owner epoch is not the record's.
    /// Cut: check 3 deleted, red.
    /// Unit (<1 ms).
    #[test]
    fn m10_s2_r446_3_a_fence_for_another_lineage_is_refused() {
        let (mut proof, facts) = true_fence();
        proof.prior_generation = Generation(2);
        assert_eq!(bind(&proof, facts), Err("lineage"));
        let (mut proof, facts) = true_fence();
        proof.prior_owner_epoch = OwnerEpoch(1);
        assert_eq!(bind(&proof, facts), Err("lineage"));
    }

    /// Check 4: the proof's prior boot is not the one the record's root names.
    /// Cut: check 4 deleted, red.
    /// Unit (<1 ms).
    #[test]
    fn m10_s2_r446_4_a_fence_naming_another_owner_boot_is_refused() {
        let (mut proof, facts) = true_fence();
        proof.prior_boot_id = BootId(8);
        assert_eq!(bind(&proof, facts), Err("owner_boot"));
    }

    /// Check 5: the owner's grant is not frozen, is another grant, or is absent; its expiry is
    /// past in each case, so only check 5 can refuse.
    /// Cut: check 5 deleted, red.
    /// Unit (<1 ms).
    #[test]
    fn m10_s2_r446_5_a_fence_over_a_grant_not_frozen_or_not_the_proofs_is_refused() {
        let (proof, mut facts) = true_fence();
        facts.grant.as_mut().expect("a grant").frozen = false;
        assert_eq!(bind(&proof, facts), Err("grant"), "not frozen");
        let (mut proof, facts) = true_fence();
        proof.prior_grant_id = GrantId(4);
        assert_eq!(bind(&proof, facts), Err("grant"), "another grant");
        let (proof, mut facts) = true_fence();
        facts.grant = None;
        assert_eq!(bind(&proof, facts), Err("grant"), "absent");
    }

    /// Check 6: the grant's expiry is not yet proven past on this node's clock, though the
    /// proof says it is; and a routed `DurableDrain`, which only local paths send.
    /// Cut: check 6 deleted, red.
    /// Unit (<1 ms).
    #[test]
    fn m10_s2_r446_6_a_fence_before_the_expiry_is_proven_or_by_drain_is_refused() {
        let (proof, mut facts) = true_fence();
        facts.grant.as_mut().expect("a grant").expiry_utc_ms = 1_000_500;
        assert_eq!(bind(&proof, facts), Err("expiry_not_proven"));
        let (mut proof, facts) = true_fence();
        proof.revocation = Revocation::DurableDrain {
            ack_revision: Revision(39),
        };
        assert_eq!(bind(&proof, facts), Err("revocation"));
    }

    /// A control read that failed refuses, never binds.
    /// Unit (<1 ms).
    #[test]
    fn m10_s2_r446_a_fence_whose_reads_failed_is_refused() {
        let (proof, _) = true_fence();
        assert_eq!(
            bind_routed_fence(&proof, Err("get".to_owned()), NOW, &test_budgets())
                .map_err(|(reason, _)| reason),
            Err("control_read_failed")
        );
    }
}
