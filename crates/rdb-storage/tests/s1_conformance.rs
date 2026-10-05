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
//! contract in `s1_inherit.rs`: refusals and the seal (writes to a sealed parent, each inherit
//! conflict, a parent behind the cutoff, a parent left staging), a root's snapshot once its
//! partition has a child (the oracle's root view is partition-wide), and per-seq Progress
//! (RocksDB keeps the head only).
//!
//! **Value ops (S2 design §6 W3, ADR-rdb-0012 Verification).** A second seeded stream adds
//! document ops between the storage ops: a create, an update of 1-3 path ops, a stale version,
//! a create over an existing document, and ops that fail. Each is compiled with `rdb-value`
//! against the RocksDB snapshot and against a `MapSnapshot` holding the oracle's records; the two
//! results, `Ok(Compiled)` or the error, must be byte-equal. An accepted write that the kernel
//! would apply is committed to both engines as one chained batch, and then reads back on both
//! as the expected document at the writing `seq`.
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
    AffinityId, AppliedSeq, ConfigVersion, Generation, NodeId, OwnerEpoch, PartitionId, Seq,
    SnapshotHandle, TenantId,
};
use rdb_core::contracts::storage::{Batch, CapturedPrefix, Namespace, SnapshotRead};
use rdb_sim::storage::history::canonical_history_from;
use rdb_sim::storage::memory::MemoryEngine;
use rdb_storage::{verify_lineage, Inherited, RocksEngine};
use rdb_value::delta::{materialize, Delta, Op};
use rdb_value::keys::{root_key, RootKey};
use rdb_value::path::Path;
use rdb_value::testing::MapSnapshot;
use rdb_value::value::{Decimal, Float, Int, Map, MapKey, Timestamp, Value};
use rdb_value::{compile, read, Document, Expected, ValueError};

/// Seeds at reduced scale, the ordinary gate (ADR-0031: a fixed constant, never host-derived).
const REDUCED_SEEDS: u64 = 32;
/// Seeds at full scale, `RETCD_EVIDENCE=1` (design §5: 10,000 histories).
const FULL_SEEDS: u64 = 10_000;
/// Operations per seed, across both partitions.
const OPS_PER_SEED: usize = 40;
/// Inherits per partition per seed, at most (design §5: 0-3).
const MAX_INHERITS: u32 = 3;
const PARTITIONS: [PartitionId; 2] = [PartitionId(1), PartitionId(2)];
/// Percent of steps followed by one document op. A separate stream, salted from the seed, so
/// the storage ops keep their own sequence of draws.
const DOC_PERCENT: u64 = 40;
const DOC_SALT: u64 = 0xD0C5_0000_0000_0001;
/// The document keys, besides the canonical `k` the storage ops write.
const DOC_NAMES: [&[u8]; 2] = [b"doc-a", b"doc-b"];

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

fn pick(rng: &mut Rng, max: u64) -> usize {
    usize::try_from(rng.upto(max)).expect("a small index")
}

/// A document's root key (ADR-rdb-0013 §1).
fn doc_key(name: &[u8]) -> RootKey {
    root_key(TenantId(1), AffinityId(1), name)
}

/// Small, negative, or up to 2^64 - 1, so an increment can leave the integer range.
fn random_int(docs: &mut Rng) -> Int {
    match docs.upto(3) {
        0 => Int::from(docs.next()),
        1 => Int::from(-i64::try_from(docs.upto(1000)).expect("small")),
        _ => Int::from(docs.upto(100)),
    }
}

/// One scalar of each kind the profile has.
fn random_leaf(docs: &mut Rng) -> Value {
    match docs.upto(7) {
        0 => Value::Null,
        1 => Value::Bool(docs.upto(1) == 1),
        2 => Value::Integer(random_int(docs)),
        3 => {
            let eighths = u32::try_from(docs.upto(1_000_000)).expect("small");
            Value::Float(Float::new(f64::from(eighths) / 8.0).expect("finite"))
        }
        4 => {
            // The last digit is 1-9, so the mantissa is normalised.
            let mantissa = i128::from(docs.upto(999)) * 10 + 1 + i128::from(docs.upto(8));
            let exponent = -i64::try_from(docs.upto(20)).expect("small");
            Value::Decimal(
                Decimal::new(exponent, Int::new(mantissa).expect("in range")).expect("normalised"),
            )
        }
        5 => {
            let secs = i64::try_from(docs.upto(4_000_000_000)).expect("small");
            let nanos = u32::try_from(docs.upto(u64::from(Timestamp::MAX_NANOS))).expect("small");
            Value::Timestamp(Timestamp::new(secs, nanos).expect("valid nanos"))
        }
        6 => Value::Text(format!("s{}", docs.upto(999))),
        _ => {
            let len = docs.upto(4);
            Value::Bytes((0..len).map(|_| docs.next().to_le_bytes()[0]).collect())
        }
    }
}

/// A map holding some of `n`, `s`, `l` and `m`; one time in ten a bare scalar, so a path op on
/// it fails with `NotAContainer`.
fn random_doc(docs: &mut Rng) -> Value {
    if docs.upto(9) == 0 {
        return random_leaf(docs);
    }
    let mut root = Map::new();
    if docs.upto(3) > 0 {
        root.insert(MapKey::new("n"), Value::Integer(random_int(docs)));
    }
    if docs.upto(3) > 0 {
        root.insert(MapKey::new("s"), random_leaf(docs));
    }
    if docs.upto(3) > 0 {
        let len = docs.upto(3);
        let items = (0..len).map(|_| Value::Integer(random_int(docs))).collect();
        root.insert(MapKey::new("l"), Value::Array(items));
    }
    if docs.upto(3) > 0 {
        let mut nested = Map::new();
        for name in ["x", "d", "t", "b"] {
            if docs.upto(3) > 0 {
                nested.insert(MapKey::new(name), random_leaf(docs));
            }
        }
        root.insert(MapKey::new("m"), Value::Map(nested));
    }
    Value::Map(root)
}

/// One op on a path that may or may not exist, so some ops fail (`PathNotFound`,
/// `IndexInvalid`, `NotAContainer`, `TypeMismatch`, `Overflow`).
fn random_op(docs: &mut Rng) -> Op {
    const PATHS: [&str; 8] = ["/n", "/s", "/l/0", "/l/3", "/m/x", "/m/z", "/z", "/n/0"];
    let path = Path::parse(PATHS[pick(docs, 7)]).expect("a valid pointer");
    match docs.upto(9) {
        0 => Op::Replace(random_doc(docs)),
        1..=4 => Op::Set(path, random_leaf(docs)),
        5..=7 => Op::Increment(path, random_int(docs)),
        _ => Op::Remove(path),
    }
}

/// `min..=max` random ops.
fn random_ops(docs: &mut Rng, min: u64, max: u64) -> Vec<Op> {
    let count = min + docs.upto(max - min);
    (0..count).map(|_| random_op(docs)).collect()
}

/// The oracle's document records as a [`MapSnapshot`], the second store `rdb-value` reads.
fn oracle_map(view: &dyn SnapshotRead) -> MapSnapshot {
    let mut map = MapSnapshot::new(view.generation());
    for name in DOC_NAMES {
        let key = doc_key(name);
        match (
            view.get(Namespace::User, key.as_bytes()),
            view.version(Namespace::User, key.as_bytes()),
        ) {
            (Some(value), Some(version)) => map.insert(key.to_bytes(), version, value),
            (None, None) => {}
            other => {
                panic!("the oracle's view of {key:?} has a value or a version alone: {other:?}")
            }
        }
    }
    map
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

    /// One document op on one partition's active lineage: compiled against RocksDB and against
    /// the oracle's records, which must agree byte for byte; when the kernel would apply it,
    /// committed to both and read back on both.
    fn doc_op(&mut self, docs: &mut Rng) {
        let index = pick(docs, 1);
        let key = doc_key(DOC_NAMES[pick(docs, 1)]);
        let (id, active) = (self.partitions[index].id, self.partitions[index].active);
        let applied = self.applied(index);
        let at = format!("{} p{} g{} {key:?}", self.context(), id.0, active.0);

        let memory_view = self.memory.snapshot(id, active, SnapshotHandle(1));
        let current = memory_view.version(Namespace::User, key.as_bytes());
        let (expected, delta) = match (current, docs.upto(5)) {
            // A create: a whole document, sometimes edited in the same delta.
            (None, 0..=3) => {
                let mut ops = vec![Op::Replace(random_doc(docs))];
                ops.extend(random_ops(docs, 0, 2));
                (Expected::Absent, ops)
            }
            // An update of a document that does not exist: `ObjectAbsent`.
            (None, 4) => (
                Expected::Version(1 + docs.upto(applied)),
                random_ops(docs, 1, 1),
            ),
            // A create of path ops alone: `ObjectAbsent` unless one of them is a `Replace`.
            (None, _) => (Expected::Absent, random_ops(docs, 1, 3)),
            // An update at the current version, 1-3 path ops.
            (Some(version), 0..=3) => (Expected::Version(version), random_ops(docs, 1, 3)),
            // A stale version: `VersionConflict`.
            (Some(version), 4) => {
                let stale = if docs.upto(1) == 0 {
                    version - 1
                } else {
                    version + 1
                };
                (Expected::Version(stale), random_ops(docs, 1, 1))
            }
            // A create over an existing document: it compiles on both stores and is compared,
            // but the batch is never submitted (`!applies` below). The kernel's refusal on
            // `Condition::Absent` is tested in rdb-core's `transaction_t1`; ops_compile checks
            // that the condition is attached. Neither is checked here.
            (Some(_), _) => (Expected::Absent, vec![Op::Replace(random_doc(docs))]),
        };
        let delta = Delta(delta);

        let rocks_view = self
            .rocks()
            .snapshot(id, active, SnapshotHandle(1))
            .expect("rocks snapshot");
        let map = oracle_map(&memory_view);
        let before = read(&map, &key);
        assert_eq!(read(&rocks_view, &key), before, "{at}: read before");
        let compiled = compile(&rocks_view, &key, expected, &delta);
        assert_eq!(
            compiled,
            compile(&map, &key, expected, &delta),
            "{at}: compiled {expected:?} {delta:?}"
        );
        self.counts.compared("compiled", 1);

        let Ok(compiled) = compiled else {
            self.counts.op("doc_refused");
            return;
        };
        let applies = match expected {
            Expected::Absent => current.is_none(),
            Expected::Version(version) => current == Some(version),
        };
        // The harness decides this from the version it tracks; no batch reaches either store.
        if !applies {
            self.counts.op("doc_racing_create");
            return;
        }
        let base = before
            .expect("the read before matched on both stores")
            .map(|document| document.value);
        let value = materialize(base, &delta).expect("it compiled, so it applies");
        let seq = applied + 1;
        let head = self.partitions[index].digests[&applied];
        // A document write is one Put; a second mutation here would be dropped silently.
        assert_eq!(compiled.mutations.len(), 1, "{:?}", compiled.mutations);
        let batch = delete::request_batch(
            lineage(id, active),
            Seq(seq),
            head,
            compiled.conditions,
            compiled.mutations[0].clone(),
        )
        .expect("a document batch");
        self.commit(index, &batch);

        let want: Result<Option<Document>, ValueError> = Ok(Some(Document {
            version: seq,
            value,
        }));
        let rocks_view = self
            .rocks()
            .snapshot(id, active, SnapshotHandle(1))
            .expect("rocks snapshot");
        assert_eq!(read(&rocks_view, &key), want, "{at}: rocks read back");
        let memory_view = self.memory.snapshot(id, active, SnapshotHandle(1));
        assert_eq!(
            read(&oracle_map(&memory_view), &key),
            want,
            "{at}: oracle read back"
        );
        self.counts.compared("doc_read_back", 2);
        self.counts.op(match expected {
            Expected::Absent => "doc_create",
            Expected::Version(_) => "doc_update",
        });
    }

    /// One seeded operation on one partition, then, `DOC_PERCENT` of the time, one document op.
    fn step(&mut self, rng: &mut Rng, docs: &mut Rng, op: usize) {
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
        if docs.upto(99) < DOC_PERCENT {
            self.step.push_str(" +doc");
            self.doc_op(docs);
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
/// everything both model. The generator must reach every operation, inherit mode and document
/// outcome, or the run fails rather than reporting an agreement it never tested.
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
        let mut docs = Rng(seed ^ DOC_SALT);
        let mut run = Run::new(seed);
        for op in 0..OPS_PER_SEED {
            run.step(&mut rng, &mut docs, op);
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
        "doc_create",
        "doc_update",
        "doc_refused",
        "doc_racing_create",
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
            "doc_op_percent": DOC_PERCENT,
            "doc_keys": DOC_NAMES.len(),
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

/// M8 S5, the RocksDB row (ADR-rdb-0014 §2, §6, §7, Verification): blob B1 and a blob of one
/// full-size chunk, committed through `RocksEngine` as the
/// batches T1 commits, dropped, reopened, and read back byte-equal. Each request is compiled
/// against a `MapSnapshot` holding what the earlier commits wrote, so a fault that loses the
/// chunk batches still commits the publish batch and shows at the read, as
/// `Corrupt(ChunkMissing)`. One reopen after every commit landed: crash rows rest on S0 and S1.
#[retcd_test]
fn m8s_blobs_read_back_byte_equal_after_a_reopen() {
    use rdb_core::contracts::txn::Mutation;
    use rdb_value::blob::{publish, put_chunk, read_blob, read_range, MAX_CHUNK};
    use rdb_value::Compiled;

    /// Commit `compiled`, one `Put`, as the next chained batch, and record its write.
    fn submit(
        rocks: &mut RocksEngine,
        mirror: &mut MapSnapshot,
        head: &mut Digest,
        compiled: &Compiled,
    ) {
        assert_eq!(compiled.mutations.len(), 1, "{:?}", compiled.mutations);
        let Mutation::Put { key, value, .. } = &compiled.mutations[0] else {
            panic!("a blob write is a Put");
        };
        let seq = mirror.at().0 + 1;
        let batch = delete::request_batch(
            lineage(PartitionId(1), Generation(1)),
            Seq(seq),
            *head,
            compiled.conditions.clone(),
            compiled.mutations[0].clone(),
        )
        .expect("a blob batch");
        *head = record_digest(&batch);
        rocks.commit(batch).expect("commit");
        mirror.insert(key.clone(), seq, value.clone());
    }

    let hex = |b: &[u8]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    // Pinned from Node `crypto` and `sha256sum`: B1 (0014-blob-vectors.mjs rev 2.2), and
    // 1,044,480 bytes of `(i * 7 + 3) mod 256`.
    let full: Vec<u8> = (0..MAX_CHUNK).map(|i| (i * 7 + 3) as u8).collect();
    let blobs = [
        (
            doc_key(b"photo"),
            [0x11; 16],
            b"hello, blob!".to_vec(),
            4,
            "59953c428c8411494243bb403fd0e93b00ac1aa3c235334d37db42954e2b21c6",
        ),
        (
            doc_key(b"big"),
            [0x22; 16],
            full,
            MAX_CHUNK,
            "0440c90ea72fd79f5ed2a061bf52aa4299c6b89ac567c999e54a2acc537966c0",
        ),
    ];
    let dir = data_dir("s5-blobs");
    let db = dir.join("db");
    let mut rocks = RocksEngine::open(&db).expect("open");
    let mut mirror = MapSnapshot::new(Generation(1));
    let mut head = Digest::ROOT;
    for (root, upload, data, chunk_size, sha) in &blobs {
        for (index, piece) in (0..).zip(data.chunks(*chunk_size)) {
            let compiled = put_chunk(&mirror, root, upload, index, piece).expect("a chunk");
            submit(&mut rocks, &mut mirror, &mut head, &compiled);
        }
        let sha256: [u8; 32] =
            std::array::from_fn(|i| u8::from_str_radix(&sha[2 * i..2 * i + 2], 16).expect("hex"));
        let compiled = publish(
            &mirror,
            root,
            Expected::Absent,
            upload,
            data.len() as u64,
            *chunk_size as u64,
            &sha256,
            Generation(1),
        )
        .expect("a publish");
        submit(&mut rocks, &mut mirror, &mut head, &compiled);
    }

    drop(rocks);
    let rocks = RocksEngine::open(&db).expect("reopen");
    let view = rocks
        .snapshot(PartitionId(1), Generation(1), SnapshotHandle(1))
        .expect("rocks snapshot");
    for (root, upload, data, chunk_size, sha) in &blobs {
        let range = read_range(&view, root, 0, data.len() as u64);
        assert!(range.as_deref() == Ok(&data[..]), "read back: {range:?}");
        let blob = read_blob(&view, root).expect("reads").expect("published");
        assert_eq!(
            (
                blob.manifest.size,
                hex(&blob.manifest.sha256),
                blob.manifest.upload,
                blob.manifest.chunk_size
            ),
            (
                data.len() as u64,
                (*sha).to_owned(),
                *upload,
                *chunk_size as u64
            )
        );
    }
    for (key, version, value) in mirror.records() {
        assert_eq!(
            view.get(Namespace::User, key).as_ref(),
            Some(value),
            "{key:?}"
        );
        assert_eq!(view.version(Namespace::User, key), Some(version), "{key:?}");
    }
    drop(view);
    drop(rocks);
    assert!(
        config_testkit::fs::try_remove(&dir),
        "remove {}",
        dir.display()
    );
}
