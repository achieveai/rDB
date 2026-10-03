//! M8 S1 differential: [`RocksEngine`] against the M7 oracle's [`MemoryEngine`] (design §5).
//!
//! Seeded histories drive both engines with the same operations: canonical chained puts and
//! deletes, 0-3 chained inherits per partition (child = parent + 1, base drawn from
//! `0..=applied(parent)`, so linked, copied, base 0 and a base below the parent's own base all
//! occur), syncs, and RocksDB reopens. After every operation the two must agree on the active
//! lineage's snapshot, on `get` of every key ever written, on `history_at` record bytes
//! `1..=applied`, and on `parent`, `base`, `applied` and `durable` of every lineage.
//!
//! Not compared, because the oracle does not model them (design §5); each is a rocks-only
//! contract in `s1_inherit.rs`: refusals and the seal (#3-#9, #12, #18, #34, #35, S1-T2), a
//! root's snapshot once its partition has a child (the oracle's root view is partition-wide),
//! and per-seq Progress (RocksDB keeps the head only, S0 F1).
//!
//! Writes `docs/evidence/rdb-m8-storage-conformance.json` through `write_evidence`.

#[path = "support/delete.rs"]
mod delete;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use bytes::Bytes;
use config_log::retcd_test;
use config_testkit::evidence::{full_scale_requested, write_evidence, RunInfo};
use rdb_core::contracts::authority::Lineage;
use rdb_core::contracts::digest::Digest;
use rdb_core::contracts::envelope::ReplicationEnvelope;
use rdb_core::contracts::ids::{
    AppliedSeq, ConfigVersion, Generation, NodeId, OwnerEpoch, PartitionId, Seq, SnapshotHandle,
};
use rdb_core::contracts::storage::{Batch, CapturedPrefix, Namespace, SnapshotRead};
use rdb_sim::storage::history::canonical_history_from;
use rdb_sim::storage::memory::MemoryEngine;
use rdb_storage::{verify_lineage, Inherited, RocksEngine};

/// Seeds at reduced scale, the ordinary gate (ADR-0031: a fixed constant, never host-derived).
const REDUCED_SEEDS: u64 = 32;
/// Seeds at full scale, `RETCD_EVIDENCE=1` (design §5: 10,000 histories).
const FULL_SEEDS: u64 = 10_000;
/// Operations per seed, across both partitions.
const OPS_PER_SEED: usize = 40;
/// Inherits per partition per seed, at most (design §5: 0-3).
const MAX_INHERITS: u32 = 3;
const PARTITIONS: [PartitionId; 2] = [PartitionId(1), PartitionId(2)];

/// splitmix64: a dependency-free, seedable generator. Same seed, same history.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..=max`.
    fn upto(&mut self, max: u64) -> u64 {
        self.next() % (max + 1)
    }
}

/// What the generator knows about one partition.
struct Partition {
    id: PartitionId,
    /// The generation every write goes to; older ones are sealed parents.
    active: Generation,
    /// Every generation this partition has held, oldest first.
    generations: Vec<Generation>,
    /// The record digest at each seq of the active lineage's chain (`0` is `Digest::ROOT`).
    digests: BTreeMap<u64, Digest>,
    /// Every `(ns, key)` any batch of this partition wrote.
    written: BTreeSet<(Namespace, Bytes)>,
    inherits: u32,
}

/// Operation and comparison counts, for the evidence file.
#[derive(Default)]
struct Counts {
    ops: BTreeMap<&'static str, u64>,
    compared: BTreeMap<&'static str, u64>,
}

impl Counts {
    fn op(&mut self, name: &'static str) {
        *self.ops.entry(name).or_default() += 1;
    }

    fn compared(&mut self, name: &'static str, n: u64) {
        *self.compared.entry(name).or_default() += n;
    }
}

fn data_dir(name: &str) -> PathBuf {
    let root = std::env::var_os("RETCD_TEST_DATA_DIR")
        .map_or_else(|| PathBuf::from(env!("CARGO_TARGET_TMPDIR")), PathBuf::from);
    let dir = root.join(format!(
        "rdb-storage-conformance-{name}-{}",
        std::process::id()
    ));
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("remove a stale test directory");
    }
    std::fs::create_dir_all(&dir).expect("create the test directory");
    dir
}

fn lineage(partition: PartitionId, generation: Generation) -> Lineage {
    Lineage {
        partition,
        generation,
        owner_epoch: OwnerEpoch(1),
    }
}

/// The record digest a batch's History write carries.
fn record_digest(batch: &Batch) -> Digest {
    let record = batch
        .writes
        .iter()
        .find(|w| w.ns == Namespace::History)
        .and_then(|w| w.value.as_ref())
        .expect("every generated batch writes its History record");
    ReplicationEnvelope::decode(record)
        .expect("a generated History record decodes")
        .record_digest
}

/// One seeded history over two partitions.
struct Run {
    seed: u64,
    db: PathBuf,
    rocks: Option<RocksEngine>,
    memory: MemoryEngine,
    partitions: Vec<Partition>,
    counts: Counts,
    /// The operation being checked, for failure messages.
    step: String,
}

impl Run {
    fn new(seed: u64) -> Self {
        let db = data_dir(&format!("seed{seed}")).join("db");
        let rocks = RocksEngine::open(&db).expect("open");
        let partitions = PARTITIONS
            .iter()
            .map(|&id| Partition {
                id,
                active: Generation(1),
                generations: vec![Generation(1)],
                digests: BTreeMap::from([(0, Digest::ROOT)]),
                written: BTreeSet::new(),
                inherits: 0,
            })
            .collect();
        Self {
            seed,
            db,
            rocks: Some(rocks),
            memory: MemoryEngine::new(NodeId(1)),
            partitions,
            counts: Counts::default(),
            step: String::new(),
        }
    }

    fn rocks(&self) -> &RocksEngine {
        self.rocks.as_ref().expect("rocks is open between steps")
    }

    fn rocks_mut(&mut self) -> &mut RocksEngine {
        self.rocks.as_mut().expect("rocks is open between steps")
    }

    fn context(&self) -> String {
        format!("seed={} step={}", self.seed, self.step)
    }

    /// Commit one batch to both engines and record its keys and digest.
    fn commit(&mut self, index: usize, batch: &Batch) {
        let digest = record_digest(batch);
        let rocks = self.rocks_mut().commit(batch.clone());
        let memory = self.memory.commit(batch.clone());
        assert_eq!(
            rocks,
            memory,
            "{}: commit seq {}",
            self.context(),
            batch.seq.0
        );
        let partition = &mut self.partitions[index];
        partition.digests.insert(batch.seq.0, digest);
        for write in &batch.writes {
            partition.written.insert((write.ns, write.key.clone()));
        }
    }

    fn applied(&self, index: usize) -> u64 {
        let partition = &self.partitions[index];
        self.rocks()
            .buffered_applied(partition.id, partition.active)
            .0
    }

    fn put(&mut self, index: usize, n: u64) {
        let (id, active) = (self.partitions[index].id, self.partitions[index].active);
        let applied = self.applied(index);
        let head = self.partitions[index].digests[&applied];
        let history = canonical_history_from(
            lineage(id, active),
            ConfigVersion(1),
            (Seq(applied), head),
            applied + n,
        )
        .expect("canonical history");
        for batch in &history.batches {
            self.commit(index, batch);
        }
        self.counts.op("put");
    }

    fn delete(&mut self, index: usize) {
        let (id, active) = (self.partitions[index].id, self.partitions[index].active);
        let applied = self.applied(index);
        let head = self.partitions[index].digests[&applied];
        let batch =
            delete::delete_batch(lineage(id, active), Seq(applied + 1), head).expect("delete");
        self.commit(index, &batch);
        self.counts.op("delete");
    }

    fn inherit(&mut self, index: usize, base: u64) {
        let (id, from) = (self.partitions[index].id, self.partitions[index].active);
        let to = Generation(from.0 + 1);
        let applied = self.applied(index);
        let parent_base = self.rocks().link(id, from).map(|link| link.base.0);
        let inherited = self
            .rocks_mut()
            .inherit(id, from, to, Seq(base))
            .unwrap_or_else(|e| {
                panic!(
                    "seed={} inherit {from:?}->{to:?} base {base}: {e}",
                    self.seed
                )
            });
        self.memory
            .inherit(id, from, to, Seq(base))
            .expect("the oracle inherits");
        let mode = match inherited {
            Inherited::Linked => {
                assert_eq!(base, applied, "{}: linked below applied", self.context());
                "inherit_linked"
            }
            Inherited::Copied { .. } => {
                assert!(base < applied, "{}: copied at applied", self.context());
                "inherit_copied"
            }
            Inherited::AlreadyInherited => panic!("{}: a fresh child was a no-op", self.context()),
        };
        self.counts.op(mode);
        if base == 0 {
            self.counts.op("inherit_base_zero");
        }
        if parent_base.is_some_and(|parent_base| base < parent_base) {
            self.counts.op("inherit_base_below_parent_base");
        }
        let partition = &mut self.partitions[index];
        partition.active = to;
        partition.generations.push(to);
        partition.digests.retain(|&seq, _| seq <= base);
        partition.inherits += 1;
    }

    fn sync(&mut self, index: usize) {
        let (id, active) = (self.partitions[index].id, self.partitions[index].active);
        let capture = CapturedPrefix {
            partition: id,
            generation: active,
            through: AppliedSeq(self.applied(index)),
        };
        let rocks = self
            .rocks_mut()
            .sync_wal_through(vec![capture])
            .expect("rocks sync");
        let memory = self
            .memory
            .sync_wal_through(vec![capture])
            .expect("memory sync");
        assert_eq!(rocks, memory, "{}: sync answer", self.context());
        self.counts.op("sync");
    }

    fn reopen(&mut self) {
        drop(self.rocks.take());
        self.rocks = Some(RocksEngine::open(&self.db).expect("reopen"));
        self.counts.op("reopen");
    }

    /// Every comparison design §5 names, for every partition.
    fn compare(&mut self) {
        for index in 0..self.partitions.len() {
            self.compare_marks(index);
            self.compare_reads(index);
        }
    }

    /// `parent`, `base`, `applied` and `durable` of every lineage the partition has held.
    fn compare_marks(&mut self, index: usize) {
        let context = self.context();
        let (rocks, memory) = (self.rocks(), &self.memory);
        let partition = &self.partitions[index];
        let id = partition.id;
        for &generation in &partition.generations {
            let at = format!("{context} p{} g{}", id.0, generation.0);
            let link = rocks.link(id, generation);
            assert_eq!(
                link.map(|link| link.parent),
                memory.parent(id, generation),
                "{at}: parent"
            );
            assert_eq!(
                link.map_or(Seq(0), |link| link.base),
                memory.base(id, generation),
                "{at}: base"
            );
            assert_eq!(
                rocks.buffered_applied(id, generation),
                memory.buffered_applied(id, generation),
                "{at}: applied"
            );
            assert_eq!(
                rocks.durable(id, generation),
                memory.durable(id, generation),
                "{at}: durable"
            );
        }
        let lineages = u64::try_from(partition.generations.len()).expect("small");
        self.counts.compared("lineage_marks", lineages);
    }

    /// The active lineage's snapshot, `get` of every written key, and `history_at` bytes.
    fn compare_reads(&mut self, index: usize) {
        let context = self.context();
        let (rocks, memory) = (self.rocks(), &self.memory);
        let partition = &self.partitions[index];
        let (id, active) = (partition.id, partition.active);
        let at = format!("{context} p{} g{}", id.0, active.0);

        let rocks_view = rocks
            .snapshot(id, active, SnapshotHandle(1))
            .expect("rocks snapshot");
        let memory_view = memory.snapshot(id, active, SnapshotHandle(1));
        assert_eq!(rocks_view.len(), memory_view.len(), "{at}: snapshot len");
        assert_eq!(rocks_view.at(), memory_view.at(), "{at}: snapshot at");
        for (ns, key, value, version, _) in rocks_view.entries() {
            assert_eq!(
                memory_view.get(ns, key).as_ref(),
                Some(value),
                "{at}: {ns:?} {key:?}"
            );
            assert_eq!(
                memory_view.version(ns, key),
                Some(version),
                "{at}: {ns:?} {key:?} version"
            );
        }

        // The view as the kernel reads it: through `SnapshotRead`, never through `entries`.
        assert_eq!(rocks_view.handle(), memory_view.handle(), "{at}: handle");
        assert_eq!(
            rocks_view.generation(),
            memory_view.generation(),
            "{at}: generation"
        );
        let mut namespaces = Vec::new();
        for (ns, key) in &partition.written {
            assert_eq!(
                rocks_view.get(*ns, key),
                memory_view.get(*ns, key),
                "{at}: view get {ns:?} {key:?}"
            );
            assert_eq!(
                rocks_view.version(*ns, key),
                memory_view.version(*ns, key),
                "{at}: view version {ns:?} {key:?}"
            );
            if !namespaces.contains(ns) {
                namespaces.push(*ns);
            }
        }
        let mut scans = 0;
        for ns in namespaces {
            let all = memory_view.scan(ns, &[], usize::MAX);
            assert_eq!(
                rocks_view.scan(ns, &[], usize::MAX),
                all,
                "{at}: scan {ns:?}"
            );
            // From the middle key, with a limit that cuts the tail.
            let from = all.get(all.len() / 2).map_or(&[][..], |(key, _)| &key[..]);
            assert_eq!(
                rocks_view.scan(ns, from, 2),
                memory_view.scan(ns, from, 2),
                "{at}: scan {ns:?} from {from:?} limit 2"
            );
            scans += 2;
        }

        let mut gets = 0;
        for (ns, key) in &partition.written {
            let got = rocks.get(id, active, *ns, key).expect("rocks get");
            let expected = memory_view
                .get(*ns, key)
                .map(|value| (memory_view.version(*ns, key).expect("versioned"), value));
            assert_eq!(
                got.map(|(_, version, value)| (version, value)),
                expected,
                "{at}: get {ns:?} {key:?}"
            );
            gets += 1;
        }

        let applied = rocks.buffered_applied(id, active).0;
        for seq in 1..=applied {
            let rocks_record = rocks
                .history_at(id, active, Seq(seq))
                .expect("rocks history_at")
                .map(|(record, _)| record);
            let memory_record = memory
                .history_at(id, active, Seq(seq))
                .map(|(record, _)| record);
            assert_eq!(rocks_record, memory_record, "{at}: history_at {seq}");
        }

        self.counts.compared("snapshot", 1);
        self.counts.compared("snapshot_scan", scans);
        self.counts.compared("get", gets);
        self.counts.compared("history_at", applied);
    }

    /// One seeded operation on one partition.
    fn step(&mut self, rng: &mut Rng, op: usize) {
        let index = usize::try_from(rng.upto(1)).expect("0 or 1");
        let roll = rng.upto(99);
        let applied = self.applied(index);
        let can_inherit = self.partitions[index].inherits < MAX_INHERITS && applied > 0;
        self.step = format!("{op} p{} roll={roll}", self.partitions[index].id.0);
        match roll {
            0..=44 => {
                let n = 1 + rng.upto(2);
                self.put(index, n);
            }
            45..=59 => self.delete(index),
            60..=74 if can_inherit => {
                let base = rng.upto(applied);
                self.inherit(index, base);
            }
            75..=89 => self.sync(index),
            90..=94 => self.reopen(),
            _ => {
                self.put(index, 1);
            }
        }
        self.compare();
    }

    /// Every lineage verifies its own chain at the end of the history.
    fn verify_all(&self) {
        for partition in &self.partitions {
            for &generation in &partition.generations {
                verify_lineage(self.rocks(), partition.id, generation).unwrap_or_else(|fault| {
                    panic!(
                        "seed={} p{} g{} does not verify: {fault:?}",
                        self.seed, partition.id.0, generation.0
                    )
                });
            }
        }
    }
}

/// Seeds run is `0..32`, or `0..10_000` under `RETCD_EVIDENCE=1`; seed `n` is the same history
/// on every host, so a failing `seed=` in a message replays exactly.
///
/// Design §5 (critic F2, F7): over seeded histories, `RocksEngine` and `MemoryEngine` agree on
/// everything both model. The generator must reach every operation and inherit mode, or the run
/// fails rather than reporting an agreement it never tested.
#[retcd_test]
fn m8s_conformance_rocks_engine_agrees_with_the_oracle() {
    let seeds = if full_scale_requested() {
        FULL_SEEDS
    } else {
        REDUCED_SEEDS
    };
    #[allow(clippy::cast_precision_loss)] // both counts are far below 2^52
    let run_info = RunInfo::start(0).scaled(FULL_SEEDS as f64, seeds as f64);
    let mut totals = Counts::default();
    for seed in 0..seeds {
        let mut rng = Rng(seed);
        let mut run = Run::new(seed);
        for op in 0..OPS_PER_SEED {
            run.step(&mut rng, op);
        }
        run.verify_all();
        for (name, n) in run.counts.ops {
            *totals.ops.entry(name).or_default() += n;
        }
        for (name, n) in run.counts.compared {
            *totals.compared.entry(name).or_default() += n;
        }
        // A seed that passed leaves nothing behind: kept, 10,000 stores came to 3.7 GB. A seed
        // that fails panics above, so its store stays for whoever reads the failure.
        drop(run.rocks.take());
        let seed_dir = run
            .db
            .parent()
            .expect("the store sits in its seed directory");
        assert!(
            config_testkit::fs::try_remove(seed_dir),
            "seed={seed}: remove {} after the seed passed",
            seed_dir.display()
        );
    }

    for required in [
        "put",
        "delete",
        "sync",
        "reopen",
        "inherit_linked",
        "inherit_copied",
        "inherit_base_zero",
        "inherit_base_below_parent_base",
    ] {
        assert!(
            totals.ops.get(required).copied().unwrap_or(0) > 0,
            "the generator never reached {required}: {:?}",
            totals.ops
        );
    }

    write_evidence(
        "rdb-m8-storage-conformance",
        serde_json::json!({
            "seeds": seeds,
            "ops_per_seed": OPS_PER_SEED,
            "partitions": PARTITIONS.len(),
            "max_inherits_per_partition": MAX_INHERITS,
            "ops": totals.ops,
            "comparisons": totals.compared,
            "mismatches": 0,
            "not_compared": [
                "refusals and the seal (rocks-only rows #3-#9, #12, #18, #34, #35, S1-T2)",
                "a root's snapshot once its partition has a child (oracle root view is partition-wide)",
                "per-seq Progress (RocksDB keeps the head only, S0 F1)"
            ],
        }),
        run_info,
    );
}
