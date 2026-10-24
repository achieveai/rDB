//! The host test harness: [`Trio`], three hosts stepped by hand on one thread, and the
//! one-host fixtures the host rows share. Moved out of `host.rs` (M10 S2 event 0).

use super::*;

/// This test's log lines whose message is `$message`. A macro, because the JSON value type
/// is not nameable here without a `serde_json` dependency.
macro_rules! logged {
    ($method:expr, $message:expr) => {
        config_testkit::logs::lines_for_current_test(module_path!(), $method)
            .into_iter()
            .filter(|line| line["@m"] == $message)
            .collect::<Vec<_>>()
    };
}
pub(super) use logged;
/// A-R177 rows: `n` chained records in `generation` of partition 1, committed as R1 stages
/// them (the record, its `History` row, the applied-head `Progress` row). Their digests,
/// `[0]` at seq 1.
pub(super) fn write_chain(engine: &mut RocksEngine, generation: Generation, n: u64) -> Vec<Digest> {
    write_chain_after(engine, generation, Vec::new(), n)
}

/// [`write_chain`] continued: `n` more records in `generation`, chained after `digests` (the
/// records already held, `[0]` at seq 1). Returns every digest, `digests` first.
pub(super) fn write_chain_after(
    engine: &mut RocksEngine,
    generation: Generation,
    mut digests: Vec<Digest>,
    n: u64,
) -> Vec<Digest> {
    use rdb_core::contracts::digest::Domain;
    use rdb_core::contracts::envelope::EnvelopeHeader;
    use rdb_core::contracts::ids::{BatchId, ClientId, LeaseId, TenantId};
    use rdb_core::contracts::storage::{Batch, Write};
    use rdb_core::contracts::txn::Outcome;
    use rdb_core::contracts::version::ENVELOPE_VERSION;
    let first = digests.len() as u64 + 1;
    for seq in first..first + n {
        let mut env = ReplicationEnvelope {
            header: EnvelopeHeader {
                protocol_version: ENVELOPE_VERSION,
                partition: PartitionId(1),
                generation,
                config_version: ConfigVersion(1),
                owner_epoch: OwnerEpoch(generation.0),
                seq: Seq(seq),
                body_len: 0,
            },
            lease_id: LeaseId(1),
            prev_digest: digests.last().copied().unwrap_or(Digest::ROOT),
            request_identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(seq),
            },
            request_digest: Digest::of(Domain::Record, &[&seq.to_le_bytes()]),
            conditions_result: Vec::new(),
            mutations: vec![Write {
                ns: Namespace::User,
                key: Bytes::copy_from_slice(&seq.to_be_bytes()),
                value: Some(Bytes::from_static(b"v")),
            }],
            result: Outcome::Published,
            record_digest: Digest::ROOT,
        };
        env.record_digest = env.compute_record_digest().expect("digest");
        digests.push(env.record_digest);
        let mut progress = seq.to_le_bytes().to_vec();
        progress.extend_from_slice(&env.record_digest.0);
        let mut writes = env.mutations.clone();
        writes.push(Write {
            ns: Namespace::History,
            key: Bytes::copy_from_slice(&seq.to_be_bytes()),
            value: Some(env.encode().expect("encode")),
        });
        writes.push(Write {
            ns: Namespace::Progress,
            key: Bytes::from_static(rdb_core::replication::append::PROGRESS_KEY),
            value: Some(Bytes::from(progress)),
        });
        engine
            .commit(Batch {
                id: BatchId(seq),
                partition: PartitionId(1),
                generation,
                seq: Seq(seq),
                writes,
            })
            .expect("commit");
    }
    digests
}
/// Node 2's host over the engine at `path`, which a row filled first.
pub(super) fn host_over(path: &std::path::Path, rt: &tokio::runtime::Runtime) -> Host {
    let engine = RocksEngine::open(path).expect("open engine");
    let links = Links::new();
    let store: Arc<dyn config_core::ConfigStore> = Arc::new(config_testkit::MemStore::new());
    let control = ControlAdapter::new(store, rt.handle().clone(), Arc::clone(&links));
    Host::new(NodeId(2), engine, links, control, HostClock::start())
}
pub(super) fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
}
/// Three hosts on this thread, stepped by hand, for the rows that need a whole partition.
/// Fields drop in order: the hosts close before the runtime their control calls run on.
pub(super) struct Trio {
    pub(super) nodes: Vec<(Host, Sender<Msg>, Receiver<Msg>)>,
    pub(super) links: Arc<Links>,
    pub(super) store: Arc<dyn config_core::ConfigStore>,
    /// The same store, to make it fail.
    pub(super) mem: Arc<config_testkit::MemStore>,
    pub(super) clock: HostClock,
    pub(super) rt: tokio::runtime::Runtime,
}

impl Trio {
    /// Every `until` gives up after this long, times the deadline scale. The longest wait is a
    /// stall line at F1's first deadline after a commit, two of the spec's 2 s windows: 4.0 s
    /// at scale 1 (2026-10-08), so this leaves 4 s.
    pub(super) const PATIENCE: Duration = Duration::from_secs(8);
    /// [`Self::fast_pause`]'s L1 pause age: the spec's 2,000 ms, shortened.
    pub(super) const PAUSE_AGE_MILLIS: u64 = 400;
    /// The most one reading of the trio's clock moves past the last (#50, [`HostClock::capped`]).
    /// Below every budget a row tunes (the shortest is [`Self::fast`]'s 50 ms resume hold) and
    /// far below the 600-680 ms stalls of this thread that failed rows (diag-d16), so a stall
    /// costs the hosts 20 ms of their time instead of all of it.
    pub(super) const CLOCK_STEP_CAP_MILLIS: u64 = 20;

    /// Three hosts on `dir`, each tuned by `tune`, not yet bootstrapped.
    pub(super) fn new(dir: &std::path::Path, tune: impl Fn(&mut Host)) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("runtime");
        let links = Links::new();
        let mem = Arc::new(config_testkit::MemStore::new());
        let store: Arc<dyn config_core::ConfigStore> = Arc::clone(&mem) as _;
        let control =
            ControlAdapter::new(Arc::clone(&store), rt.handle().clone(), Arc::clone(&links));
        let clock = HostClock::capped(Self::CLOCK_STEP_CAP_MILLIS);
        let nodes = (1..=3u32)
            .map(|n| {
                let engine = RocksEngine::open(dir.join(n.to_string())).expect("open engine");
                let (tx, rx) = mpsc::channel();
                let mut host = Host::new(
                    NodeId(n),
                    engine,
                    Arc::clone(&links),
                    Arc::clone(&control),
                    clock,
                );
                tune(&mut host);
                links.register(NodeId(n), tx.clone(), host.boot);
                (host, tx, rx)
            })
            .collect();
        Self {
            nodes,
            links,
            store,
            mem,
            clock,
            rt,
        }
    }

    /// The spec's budgets with L1's resume hold shortened, so a start reaches `ready` in
    /// about the 2 s discovery window. The window stays the spec's: it is also F1's CAS and
    /// barrier deadline, and a host stall past a short one blocks F1 for good (Trio flake,
    /// 2026-10-08: ~470 ms against 300 ms). Only rows that count F1's deadlines shorten it.
    pub(super) fn fast(host: &mut Host) {
        host.budgets.resume_hold_millis = 50;
    }

    /// [`Self::fast`], and L1 warns and pauses at a fifth of the spec's ages, so a row can
    /// watch past the pause age in well under a second.
    pub(super) fn fast_pause(host: &mut Host) {
        Self::fast(host);
        host.budgets.warn_age_millis = Self::PAUSE_AGE_MILLIS / 2;
        host.budgets.pause_age_millis = Self::PAUSE_AGE_MILLIS;
    }

    pub(super) fn bootstrap(&self) {
        let owner = self.nodes[0].1.clone();
        self.rt
            .block_on(crate::admin::bootstrap(
                &self.store,
                self.clock.now(),
                |msg| owner.send(msg).map_err(|_| NodeStopped(NodeId(1))),
            ))
            .expect("bootstrap");
    }

    /// For rows that count F1's post-commit deadlines: the spec's window until node 1 has
    /// committed, so the commit's CAS outlasts a host stall, then the short test window for
    /// every deadline armed after it.
    pub(super) fn short_window_after_commit(&mut self) {
        self.until("F1 committed on node 1", |trio| {
            trio.status(0).recovered.map(|_| ())
        });
        for (host, _, _) in &mut self.nodes {
            host.budgets.discovery_window_millis = test_discovery_window_millis();
        }
    }

    /// One round: each host runs its due work and its queue, then takes its mail.
    pub(super) fn step(&mut self) {
        for (host, _, rx) in &mut self.nodes {
            host.run_due();
            host.drain();
            while let Ok(msg) = rx.try_recv() {
                host.handle(msg);
            }
            assert_eq!(host.fault, None, "node {} faulted", host.node.0);
            assert_not_blocked(
                &host.status(PartitionId(1)),
                host.budgets.discovery_window_millis,
            );
        }
    }

    /// Step until `done` answers, or fail naming `what` after [`Self::PATIENCE`].
    pub(super) fn until<T>(&mut self, what: &str, mut done: impl FnMut(&Self) -> Option<T>) -> T {
        let patience = test_patience(Self::PATIENCE);
        let deadline = Instant::now() + patience;
        loop {
            self.step();
            if let Some(found) = done(self) {
                return found;
            }
            assert!(
                Instant::now() < deadline,
                "not within {:?}: {what}",
                patience
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub(super) fn status(&self, node: usize) -> NodeStatus {
        self.nodes[node].0.status(PartitionId(1))
    }

    pub(super) fn ready(&mut self) {
        self.until("node 1 admits writes", |trio| {
            (trio.status(0).admits == Some(true)).then_some(())
        });
    }

    /// [`Self::ready`], and node 1's grant runs at least `millis` more (A-R123, A-R124).
    /// Under full-suite load a grant sometimes stops renewing and lapses; `admits` stays
    /// true until the fence, but a check in the last 100 ms is refused `LEASE_EXPIRED`
    /// (seen 2026-10-19 at margin 29 ms). A lapsed grant makes this wait for the fence and
    /// the re-acquire.
    pub(super) fn admits_for(&mut self, millis: u64) {
        self.until("node 1 admits writes with time left on its grant", |trio| {
            let now = trio.clock.now().0;
            let left = trio.nodes[0]
                .0
                .authority_views
                .get(&PartitionId(1))
                .map(|view| view.valid_through_tick.0.saturating_sub(now));
            (trio.status(0).admits == Some(true) && left.is_some_and(|left| left >= millis))
                .then_some(())
        });
    }

    /// The head node 1 has applied.
    pub(super) fn head(&self) -> Option<u64> {
        self.status(0).holds.map(|held| held.applied)
    }

    /// Send `call` to node 1, the owner, as `Db` does.
    pub(super) fn call(&self, call: ClientCall) -> Receiver<Answer> {
        let (reply, answer) = mpsc::channel();
        self.nodes[0]
            .1
            .send(Msg::Client(Client {
                partition: PartitionId(1),
                call,
                reply,
            }))
            .expect("node 1's mailbox");
        answer
    }

    pub(super) fn answer(&mut self, answer: &Receiver<Answer>) -> Answer {
        self.until("an answer", |_| answer.try_recv().ok())
    }

    pub(super) fn ask(&mut self, call: ClientCall) -> Answer {
        let answer = self.call(call);
        self.answer(&answer)
    }
}

/// #50, the d16 probe as a row: a stall of the one thread that steps all three hosts, right
/// after two puts, leaves the puts undurable everywhere for as long as it lasts. On the real
/// clock that is 600 ms, past [`Trio::fast_pause`]'s 400 ms pause age, and L1 pauses at the
/// first eval after the wake (diag-d16: red 4/4). On the trio's capped clock it costs the
/// hosts [`Trio::CLOCK_STEP_CAP_MILLIS`], so L1 stays healthy and admitting for twice the
/// pause age of host time after it.
/// Cut: `Trio::new` on `HostClock::start()`, red (L1 paused).
/// Integration (~1.5 s): bootstrap's discovery window, the stall, then the watch.
#[test]
fn a_stall_of_the_trio_thread_after_two_puts_costs_one_clock_step_and_no_pause() {
    use rdb_core::contracts::ids::{ClientId, TenantId};
    use rdb_value::delta::Op;
    let dir = config_testkit::fs::temp_dir();
    let mut trio = Trio::new(dir.path(), Trio::fast_pause);
    trio.bootstrap();
    trio.admits_for(4 * Trio::PAUSE_AGE_MILLIS);
    for (request, object) in [(1, b"a"), (2, b"b")] {
        let put = ClientCall::Put {
            identity: RequestIdentity {
                tenant: TenantId(1),
                client: ClientId(1),
                request: RequestId(request),
            },
            object: Bytes::from_static(object),
            change: Change::Doc(Delta(vec![Op::Replace(Value::Bytes(b"v".to_vec()))])),
            expect: None,
            remaining_millis: 5_000,
        };
        match trio.ask(put) {
            Answer::Txn { .. } => {}
            other => panic!("put {request}: {other:?}"),
        }
    }
    let before = trio.clock.now();
    std::thread::sleep(Duration::from_millis(3 * Trio::PAUSE_AGE_MILLIS / 2));
    let woke = trio.clock.now();
    let watch_until = woke.plus_millis(2 * Trio::PAUSE_AGE_MILLIS);
    trio.until("twice the pause age of host time", |trio| {
        let status = trio.status(0);
        assert_ne!(
            status.protection.as_deref(),
            Some("paused"),
            "L1 paused after a stall of the trio thread: {status:?}"
        );
        (trio.clock.now() >= watch_until && status.admits == Some(true)).then_some(())
    });
    // Last, so the cut fails on L1, not on this.
    assert!(
        woke.0 - before.0 <= Trio::CLOCK_STEP_CAP_MILLIS,
        "the stall counted {} ms",
        woke.0 - before.0
    );
}
