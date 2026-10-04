//! Maps and sets at the library API (ADR-rdb-0013 §7–§13; s3-design §5).
//!
//! Scenario: the primary's transaction step compiles map and set ops against a snapshot, and
//! a damaged store is refused by name, never by a panic.

use bytes::Bytes;
use rdb_core::contracts::envelope::{EnvelopeHeader, ReplicationEnvelope};
use rdb_core::contracts::version::ENVELOPE_VERSION;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::dedup::{dedup_key, dedup_value};
use rdb_core::{
    AffinityId, ClientId, ConditionOutcome, ConfigVersion, Digest, Generation, LeaseId, Mutation,
    Namespace, Outcome, OwnerEpoch, PartitionId, RequestId, RequestIdentity, Seq, TenantId, Write,
};
use rdb_value::cbor::encode;
use rdb_value::collection::{collection, compile_collection, CollectionKind, ElemOp};
use rdb_value::envelope::{seal, EnvelopeError, Kind};
use rdb_value::keys::{element_key, root_key, RootKey};
use rdb_value::testing::MapSnapshot;
use rdb_value::value::{Int, Map, MapKey, Value};

use rdb_value::delta::{ApplyError, Delta, Op};
use rdb_value::{compile, read, Corrupt, Expected, ValueError};

fn cart() -> RootKey {
    root_key(TenantId(1), AffinityId(1), b"cart")
}

fn text(t: &str) -> Value {
    Value::Text(t.to_owned())
}

fn int(i: u64) -> Value {
    Value::Integer(Int::from(i))
}

/// A map root whose payload claims `count`, sealed soundly, so it opens and decodes.
fn map_root(count: u64) -> Bytes {
    let mut fields = Map::new();
    fields.insert(MapKey::new("keys"), int(1));
    fields.insert(MapKey::new("count"), int(count));
    seal(Kind::Map, &encode(&Value::Map(fields)).unwrap()).unwrap()
}

/// D1 (tester-m8-s3, basis a26d9d9): a stored root whose count is `u64::MAX` panicked on
/// `put` of a new key ("attempt to add with overflow"), and a release build would wrap the
/// count to 0 and write it. It is refused as `Corrupt(Root(..))` instead, and a `put` that
/// leaves the count unchanged still compiles.
#[test]
fn d1_a_root_count_at_u64_max_is_refused_by_name_never_wrapped() {
    let root = cart();
    let mut s = MapSnapshot::new(Generation(1));
    s.insert(root.to_bytes(), 4, map_root(u64::MAX));
    let banana = element_key(&root, &text("banana")).unwrap();
    s.insert(
        banana,
        4,
        seal(Kind::Document, &encode(&int(5)).unwrap()).unwrap(),
    );
    assert_eq!(collection(&s, &root).unwrap().unwrap().count, u64::MAX);

    let grow = [ElemOp::Put(text("z"), int(1))];
    let got = compile_collection(&s, &root, CollectionKind::Map, Expected::Version(4), &grow);
    assert!(
        matches!(got, Err(ValueError::Corrupt(Corrupt::Root(_)))),
        "{got:?}"
    );

    // Same count afterwards: one in, one out, or a replace.
    for ops in [
        vec![ElemOp::Put(text("banana"), int(6))],
        vec![
            ElemOp::Put(text("z"), int(1)),
            ElemOp::Remove(text("banana")),
        ],
    ] {
        let compiled =
            compile_collection(&s, &root, CollectionKind::Map, Expected::Version(4), &ops)
                .unwrap_or_else(|e| panic!("{ops:?}: {e:?}"));
        let Mutation::Put { value, .. } = &compiled.mutations[0] else {
            panic!("the root Put comes first");
        };
        assert_eq!(value, &map_root(u64::MAX), "{ops:?}");
    }
}

/// The record the kernel ships for these writes, built as `TxnKernel::envelope` builds it (the
/// writes, then step 13's one `Dedup` write) and measured by the contract's own `encode`. Every
/// field outside the request is fixed-width, so the ids chosen here do not move the length.
fn kernel_record_len(conditions: usize, mutations: &[Mutation]) -> usize {
    let identity = RequestIdentity {
        tenant: TenantId(1),
        client: ClientId(1),
        request: RequestId(1),
    };
    let mut writes: Vec<Write> = mutations
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, value, .. } => Write {
                ns: Namespace::User,
                key: key.clone(),
                value: Some(value.clone()),
            },
            Mutation::Delete { key, .. } => Write {
                ns: Namespace::User,
                key: key.clone(),
                value: None,
            },
        })
        .collect();
    writes.push(Write {
        ns: Namespace::Dedup,
        key: dedup_key(Generation(1), AffinityId(1), identity),
        value: Some(dedup_value(Digest::ROOT, Seq(1), OwnerEpoch(1))),
    });
    ReplicationEnvelope {
        header: EnvelopeHeader {
            protocol_version: ENVELOPE_VERSION,
            partition: PartitionId(1),
            generation: Generation(1),
            config_version: ConfigVersion(1),
            owner_epoch: OwnerEpoch(1),
            seq: Seq(1),
            body_len: 0,
        },
        lease_id: LeaseId(1),
        prev_digest: Digest::ROOT,
        request_identity: identity,
        request_digest: Digest::ROOT,
        conditions_result: vec![ConditionOutcome::Met; conditions],
        mutations: writes,
        result: Outcome::Published,
        record_digest: Digest::ROOT,
    }
    .encode()
    .expect("fits the wire's u32 lengths")
    .len()
}

/// L-R186r (architect-m8-s5): compile measured `TooLarge` as key plus value bytes, while the
/// kernel's admission check 10 measures the whole record against `MAX_ENVELOPE_BYTES`. A map
/// create of one large entry is walked across the kernel's cap: every compile that succeeds
/// must fit the record, and one byte past the cap must be `TooLarge`.
#[test]
fn l_r186r_a_compile_that_succeeds_fits_the_kernels_record_cap() {
    let root = cart();
    let s = MapSnapshot::new(Generation(1));
    let create = |n: usize| {
        let ops = [ElemOp::Put(text("x"), Value::Bytes(vec![0xAB; n]))];
        compile_collection(&s, &root, CollectionKind::Map, Expected::Absent, &ops)
    };
    let record = |n: usize| {
        let compiled = create(n).unwrap_or_else(|e| panic!("{n}: {e:?}"));
        kernel_record_len(compiled.conditions.len(), &compiled.mutations)
    };
    // One more value byte is one more record byte here (CBOR's 4-byte length covers both).
    let probe = 100_000;
    assert_eq!(record(probe + 1), record(probe) + 1);
    let at_cap = probe + (MAX_ENVELOPE_BYTES - record(probe));

    assert_eq!(
        record(at_cap),
        MAX_ENVELOPE_BYTES,
        "the largest create the kernel admits"
    );
    let over = create(at_cap + 1);
    assert!(
        matches!(over, Err(ValueError::Apply(ApplyError::TooLarge))),
        "one byte past the kernel's cap: {:?}",
        over.as_ref()
            .map(|c| kernel_record_len(c.conditions.len(), &c.mutations))
    );
}

/// L-R186r, the document half. It sits here beside [`kernel_record_len`] rather than in
/// `ops_compile.rs` so the oracle is written once. A document whose own envelope is within
/// 1 MiB can still make a record over the kernel's cap, once the key and the record's framing
/// are added.
#[test]
fn l_r186r_a_document_compile_that_succeeds_fits_the_kernels_record_cap() {
    let root = cart();
    let s = MapSnapshot::new(Generation(1));
    let create = |n: usize| {
        let delta = Delta(vec![Op::Replace(Value::Bytes(vec![0xAB; n]))]);
        compile(&s, &root, Expected::Absent, &delta)
    };
    let record = |n: usize| {
        let compiled = create(n).unwrap_or_else(|e| panic!("{n}: {e:?}"));
        kernel_record_len(compiled.conditions.len(), &compiled.mutations)
    };
    let probe = 100_000;
    assert_eq!(record(probe + 1), record(probe) + 1);
    let at_cap = probe + (MAX_ENVELOPE_BYTES - record(probe));

    assert_eq!(
        record(at_cap),
        MAX_ENVELOPE_BYTES,
        "the largest create the kernel admits"
    );
    let over = create(at_cap + 1);
    assert!(
        matches!(over, Err(ValueError::Apply(ApplyError::TooLarge))),
        "one byte past the kernel's cap: {:?}",
        over.as_ref()
            .map(|c| kernel_record_len(c.conditions.len(), &c.mutations))
    );
}

/// Ruling L-R186s, the tester's W1 A3 case (`p5-rootdoc.store`): a map root whose `kind` byte
/// is flipped to `0x01` read as the document `{"keys":1,"count":1}`, and one flipped to `0x03`
/// read as a set. The digest now covers the header, so every read path names the damage.
#[test]
fn l_r186s_a_flipped_root_kind_is_damage_on_every_read_path() {
    let root = cart();
    let damage = Err(ValueError::Corrupt(Corrupt::Envelope(
        EnvelopeError::DigestMismatch,
    )));
    for to in [0x01, 0x03] {
        let mut flipped = map_root(1).to_vec();
        flipped[1] = to;
        let mut s = MapSnapshot::new(Generation(1));
        s.insert(root.to_bytes(), 4, Bytes::from(flipped));
        assert_eq!(read(&s, &root).map(|_| ()), damage, "read, {to:#04x}");
        assert_eq!(
            collection(&s, &root).map(|_| ()),
            damage,
            "collection, {to:#04x}"
        );
    }
}

/// Lead ruling on tester W1 A2: `ElementNewerThanRoot` was erased by the next write. Step 10(a)
/// leaves banana at version 6 under a root at 4. Every op that touches banana (put, remove,
/// need) is refused by name; compile already reads the element's version, so this costs no
/// read. A write that touches only other elements still compiles (ADR-0013 §10's masking).
#[test]
fn a2_a_write_that_touches_an_element_newer_than_its_root_is_refused() {
    let root = cart();
    let mut s = MapSnapshot::new(Generation(1));
    s.insert(root.to_bytes(), 4, map_root(1));
    let banana = element_key(&root, &text("banana")).unwrap();
    s.insert(
        banana,
        6,
        seal(Kind::Document, &encode(&int(5)).unwrap()).unwrap(),
    );
    let newer = Err(ValueError::Corrupt(Corrupt::ElementNewerThanRoot {
        element: 6,
        root: 4,
    }));
    for ops in [
        vec![ElemOp::Put(text("banana"), int(1))],
        vec![ElemOp::Remove(text("banana"))],
        vec![ElemOp::NeedPresent(text("banana"))],
        vec![
            ElemOp::Put(text("kiwi"), int(1)),
            ElemOp::Remove(text("banana")),
        ],
    ] {
        let got = compile_collection(&s, &root, CollectionKind::Map, Expected::Version(4), &ops);
        assert_eq!(got.map(|_| ()), newer, "{ops:?}");
    }
    let kiwi = [ElemOp::Put(text("kiwi"), int(1))];
    assert!(
        compile_collection(&s, &root, CollectionKind::Map, Expected::Version(4), &kiwi).is_ok(),
        "an untouched element is not read"
    );
}
