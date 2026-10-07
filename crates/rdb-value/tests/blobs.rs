//! M8 S5: large blobs against `MapSnapshot` and a kernel stand-in (ADR-rdb-0014 Verification;
//! the RocksDB row is in `rdb-storage/tests/s1_conformance.rs`). Each test's doc says what it
//! checks.
//!
//! Independent references:
//! - every digest asserted here is pinned from Node `crypto` (`0014-blob-vectors.mjs`, rev 2.2)
//!   and agrees with `sha256sum`; `sha2` appears only as a client computing the digest it sends;
//! - the protocol model and the GC model are written from ADR-rdb-0014's rules. Their verdicts
//!   never call `blob.rs`: chunk keys are built by hand and payloads are sliced from the envelope;
//! - the size measure is the kernel's own `record_len`.

mod common;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use bytes::Bytes;
use common::{h, Kernel, Refused};
use proptest::prelude::*;
use rdb_core::contracts::trace::Version;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::admission::MAX_REQUEST_MUTATIONS;
use rdb_core::transaction::record_len;
use rdb_core::{
    AffinityId, Condition, Generation, Mutation, Namespace, Seq, SnapshotHandle, SnapshotRead,
    TenantId,
};
use rdb_value::blob::{
    chunk_count, collect_garbage, delete_blob, publish, put_chunk, read_blob, read_range, Upload,
    MAX_CHUNK, MAX_CHUNKS,
};
use rdb_value::cbor::encode;
use rdb_value::collection::{compile_collection, CollectionKind, ElemOp};
use rdb_value::delta::{ApplyError, Delta, Op, SizeLimit};
use rdb_value::envelope::{digest, seal, EnvelopeError, Kind, HEADER_LEN};
use rdb_value::keys::{chunk_key, parse, root_key, KeyError, RootKey};
use rdb_value::testing::MapSnapshot;
use rdb_value::value::{Int, Map, MapKey, Value};
use rdb_value::{compile, Compiled, Corrupt, Expected, ManifestError, ValueError};
use sha2::{Digest as _, Sha256};

const U1: Upload = [0x11; 16];
const U2: Upload = [0x22; 16];

fn photo() -> RootKey {
    root_key(TenantId(1), AffinityId(1), b"photo")
}

/// Chunk `index` of `upload` under `root`, built by hand from ADR-rdb-0014 §2: the root key with
/// its sub byte `0x00` swapped for `0x04`, then the upload and the index, big-endian.
fn chunk_of(root: &RootKey, upload: &Upload, index: u32) -> Bytes {
    let r = root.as_bytes();
    let mut key = r[..r.len() - 1].to_vec();
    key.push(0x04);
    key.extend_from_slice(upload);
    key.extend_from_slice(&index.to_be_bytes());
    Bytes::from(key)
}

/// A record's payload: everything after the 40-byte envelope header (ADR-rdb-0012 §7).
fn payload(record: &[u8]) -> &[u8] {
    &record[HEADER_LEN..]
}

fn sha(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn pin(hex: &str) -> [u8; 32] {
    h(hex).try_into().expect("32 bytes")
}

fn empty() -> Compiled {
    Compiled {
        mutations: Vec::new(),
        conditions: Vec::new(),
    }
}

fn apply_err<T>(e: ApplyError) -> Result<T, ValueError> {
    Err(ValueError::Apply(e))
}

fn corrupt<T>(c: Corrupt) -> Result<T, ValueError> {
    Err(ValueError::Corrupt(c))
}

/// What a client operation answered.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Answer {
    Published,
    AlreadyPublished,
    Collected,
    /// The process died after this many commits; nothing after them was sent.
    Crashed,
}

/// The client's upload: every piece through `put_chunk` (an "already stored" sends nothing), then
/// `publish` with the digest it computed. `stop` is a crash after that many commits.
fn upload(
    k: &mut Kernel,
    root: &RootKey,
    upload: &Upload,
    data: &[u8],
    chunk_size: usize,
    expected: Expected,
    stop: Option<usize>,
) -> Result<Answer, String> {
    let mut commits = 0;
    let pieces: Vec<&[u8]> = if data.is_empty() {
        Vec::new()
    } else {
        data.chunks(chunk_size).collect()
    };
    for (index, piece) in pieces.iter().enumerate() {
        let index = u32::try_from(index).expect("at most MAX_CHUNKS");
        let compiled = put_chunk(&k.snapshot(), root, upload, index, piece)
            .map_err(|e| format!("chunk {index}: {e:?}"))?;
        if compiled.mutations.is_empty() {
            continue;
        }
        if stop == Some(commits) {
            return Ok(Answer::Crashed);
        }
        let generation = k.generation;
        k.apply(&compiled, Some(generation))
            .map_err(|e| format!("chunk {index}: {e:?}"))?;
        commits += 1;
    }
    let compiled = publish(
        &k.snapshot(),
        root,
        expected,
        upload,
        data.len() as u64,
        chunk_size as u64,
        &sha(data),
        Generation(k.generation),
    )
    .map_err(|e| format!("publish: {e:?}"))?;
    if compiled.mutations.is_empty() {
        return Ok(Answer::AlreadyPublished);
    }
    if stop == Some(commits) {
        return Ok(Answer::Crashed);
    }
    let generation = k.generation;
    k.apply(&compiled, Some(generation))
        .map_err(|e| format!("publish: {e:?}"))?;
    Ok(Answer::Published)
}

/// Reachability GC to the end: batches until one compiles to nothing.
fn gc(k: &mut Kernel, root: &RootKey, floor: u64, stop: Option<usize>) -> Result<Answer, String> {
    let mut commits = 0;
    loop {
        let compiled = collect_garbage(&k.snapshot(), root, floor).map_err(|e| format!("{e:?}"))?;
        if compiled.mutations.is_empty() {
            return Ok(Answer::Collected);
        }
        if stop == Some(commits) {
            return Ok(Answer::Crashed);
        }
        let generation = k.generation;
        k.apply(&compiled, Some(generation))
            .map_err(|e| format!("gc: {e:?}"))?;
        commits += 1;
    }
}

/// The whole blob, as a reader sees it: `None` when absent.
fn read_all(k: &Kernel, root: &RootKey) -> Result<Option<Bytes>, ValueError> {
    let s = k.snapshot();
    match read_blob(&s, root)? {
        None => Ok(None),
        Some(blob) => read_range(&s, root, 0, blob.manifest.size).map(Some),
    }
}

// ---- the Node vectors (ADR-rdb-0014 §2, rev 2.2) --------------------------------------------

const ROOT_PHOTO: &str = "00000001000000000000000170686f746f000100";
const B1_CHUNKS: [(&str, &str, &str); 3] = [
    (
        "00000001000000000000000170686f746f0001041111111111111111111111111111111100000000",
        "0105010100000004c5dc41152b217b503175f6f6c0a437a26f39f199c781e44ab35aab5bb9b76b1e",
        "hell",
    ),
    (
        "00000001000000000000000170686f746f0001041111111111111111111111111111111100000001",
        "0105010100000004225207184030d694bb0b54b63fd524e9e77d16108002dcf160cc8501b359a7da",
        "o, b",
    ),
    (
        "00000001000000000000000170686f746f0001041111111111111111111111111111111100000002",
        "0105010100000004695bc3a7cbe3185a9c86211a444ad75a0e523db71db22516f68ef7bed6d89e57",
        "lob!",
    ),
];
const B1_SHA: &str = "59953c428c8411494243bb403fd0e93b00ac1aa3c235334d37db42954e2b21c6";
const B1_CHUNK_SHA: [&str; 3] = [
    "0ebdc3317b75839f643387d783535adc360ca01f33c75f7c1e7373adcd675c0b",
    "1c53743de87935ccac8f984af5032e9bb098eec9d1e9779ff9c9c5e0e795ff1c",
    "207b3f2864baf2588d9a046aaa54b64a7eee62d891d0788e761abbfc796c5df5",
];
const B1_ROOT: &str = concat!(
    "01040101000000c84349c1d8c72ee80da4c24b29c40f758a900d6814d9e26009420a8bfefecfd6c3",
    "a56473697a650c66736861323536582059953c428c8411494243bb403fd0e93b00ac1aa3c235334d37db42954e2b21c6",
    "6675706c6f616450111111111111111111111111111111116a6368756e6b5f73697a65046c6368756e6b5f736861323536",
    "8358200ebdc3317b75839f643387d783535adc360ca01f33c75f7c1e7373adcd675c0b",
    "58201c53743de87935ccac8f984af5032e9bb098eec9d1e9779ff9c9c5e0e795ff1c",
    "5820207b3f2864baf2588d9a046aaa54b64a7eee62d891d0788e761abbfc796c5df5",
);
const B2_CHUNKS: [(&str, &str, &str); 2] = [
    (
        "00000001000000000000000170686f746f0001042222222222222222222222222222222200000000",
        "0105010100000008891464b9a2f111ff8af817f19b0a1aadddbc0ac2a21582afb3505d1785529890",
        "HELLO, B",
    ),
    (
        "00000001000000000000000170686f746f0001042222222222222222222222222222222200000001",
        "01050101000000076e633070b9c3170026f4a6c778e808053fab5a7349e0a015023fb2f81657eeae",
        "LOB! v2",
    ),
];
const B2_SHA: &str = "e8f8c264b60e109faf9f8f33b2fe91a769b84c417e5d94af6c94ef2224f1a71b";
const B2_CHUNK_SHA: [&str; 2] = [
    "ab1012b26798993a7faede8a5e69545e9df69a46a720dfbe59cb815ff7d9e514",
    "421299cc2a7f15d8a758c08f9e2c71dd49f6094d1899e74f476812eaec664ad4",
];
const B2_ROOT: &str = concat!(
    "01040101000000a67168fcc28647a0c64d1e2a649a4cc7d76dea79e5d797395597fef80615df0071",
    "a56473697a650f667368613235365820e8f8c264b60e109faf9f8f33b2fe91a769b84c417e5d94af6c94ef2224f1a71b",
    "6675706c6f616450222222222222222222222222222222226a6368756e6b5f73697a65086c6368756e6b5f736861323536",
    "825820ab1012b26798993a7faede8a5e69545e9df69a46a720dfbe59cb815ff7d9e514",
    "5820421299cc2a7f15d8a758c08f9e2c71dd49f6094d1899e74f476812eaec664ad4",
);
const ROOT_EMPTY: &str = "000000010000000000000001656d707479000100";
const B3_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const B3_ROOT: &str = concat!(
    "01040101000000627c7cb33e48711c74f0b7eacf9acbfa3d57f11d1fdab0c548c2d41de1576f6924",
    "a56473697a6500667368613235365820e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "6675706c6f616450111111111111111111111111111111116a6368756e6b5f73697a65046c6368756e6b5f73686132353680",
);
const ZERO_ID_CHUNK_1: &str =
    "0000000100000000000000016100ff0001041111111111111111111111111111111100000001";

fn chunk_record(head_and_digest: &str, text: &str) -> Bytes {
    let mut r = h(head_and_digest);
    r.extend_from_slice(text.as_bytes());
    Bytes::from(r)
}

fn put(key: &str, value: Bytes, expected_version: Option<u64>) -> Mutation {
    Mutation::Put {
        key: Bytes::from(h(key)),
        value,
        expected_version,
    }
}

fn version_equals(key: &str, version: u64) -> Condition {
    Condition::VersionEquals {
        key: Bytes::from(h(key)),
        version,
    }
}

/// B1 committed through the compiles: chunks at versions 1–3, the root at 4.
fn b1() -> Kernel {
    let mut k = Kernel::new();
    let answer = upload(
        &mut k,
        &photo(),
        &U1,
        b"hello, blob!",
        4,
        Expected::Absent,
        None,
    );
    assert_eq!(answer, Ok(Answer::Published));
    k
}

/// A snapshot holding only `records`, each at its version: nothing compiled them.
fn stored(records: &[(&str, Bytes, u64)]) -> MapSnapshot {
    let mut s = MapSnapshot::new(Generation(1));
    for (key, value, version) in records {
        s.insert(Bytes::from(h(key)), *version, value.clone());
    }
    s
}

/// Vectors both ways: the compiles write
/// B1, B2 and B3 byte for byte as the Node script prints them, with exactly the conditions
/// ADR-rdb-0014 §6 names; and a store holding only those bytes reads back the blobs.
#[test]
fn the_node_vectors_are_written_and_read_byte_for_byte() {
    let root = photo();
    assert_eq!(root.as_bytes(), h(ROOT_PHOTO).as_slice());
    // Write: B1.
    let mut k = Kernel::new();
    for (i, (key, record, text)) in B1_CHUNKS.iter().enumerate() {
        let compiled = put_chunk(&k.snapshot(), &root, &U1, i as u32, text.as_bytes()).unwrap();
        assert_eq!(
            compiled.conditions,
            vec![Condition::Absent {
                key: Bytes::from(h(key))
            }]
        );
        assert_eq!(
            compiled.mutations,
            vec![put(key, chunk_record(record, text), None)]
        );
        assert_eq!(k.commit(&compiled), i as u64 + 1);
    }
    let compiled = publish(
        &k.snapshot(),
        &root,
        Expected::Absent,
        &U1,
        12,
        4,
        &pin(B1_SHA),
        Generation(1),
    )
    .unwrap();
    assert_eq!(
        compiled.mutations,
        vec![put(ROOT_PHOTO, Bytes::from(h(B1_ROOT)), None)]
    );
    assert_eq!(
        compiled.conditions,
        vec![
            Condition::Absent {
                key: Bytes::from(h(ROOT_PHOTO))
            },
            version_equals(B1_CHUNKS[0].0, 1),
            version_equals(B1_CHUNKS[1].0, 2),
            version_equals(B1_CHUNKS[2].0, 3),
        ]
    );
    assert_eq!(k.commit(&compiled), 4);
    // Write: B2 replaces B1 at version 4.
    for (i, (key, record, text)) in B2_CHUNKS.iter().enumerate() {
        let compiled = put_chunk(&k.snapshot(), &root, &U2, i as u32, text.as_bytes()).unwrap();
        assert_eq!(
            compiled.mutations,
            vec![put(key, chunk_record(record, text), None)]
        );
        k.commit(&compiled);
    }
    let compiled = publish(
        &k.snapshot(),
        &root,
        Expected::Version(4),
        &U2,
        15,
        8,
        &pin(B2_SHA),
        Generation(1),
    )
    .unwrap();
    assert_eq!(
        compiled.mutations,
        vec![put(ROOT_PHOTO, Bytes::from(h(B2_ROOT)), Some(4))]
    );
    assert_eq!(
        compiled.conditions,
        vec![
            version_equals(B2_CHUNKS[0].0, 5),
            version_equals(B2_CHUNKS[1].0, 6)
        ]
    );
    // Write: B3, the empty blob, has no chunks and so only the create's condition.
    let empty_root = root_key(TenantId(1), AffinityId(1), b"empty");
    assert_eq!(empty_root.as_bytes(), h(ROOT_EMPTY).as_slice());
    let compiled = publish(
        &k.snapshot(),
        &empty_root,
        Expected::Absent,
        &U1,
        0,
        4,
        &pin(B3_SHA),
        Generation(1),
    )
    .unwrap();
    assert_eq!(
        compiled.mutations,
        vec![put(ROOT_EMPTY, Bytes::from(h(B3_ROOT)), None)]
    );
    assert_eq!(
        compiled.conditions,
        vec![Condition::Absent {
            key: Bytes::from(h(ROOT_EMPTY))
        }]
    );

    // Read: stores built from the printed bytes alone.
    let mut b1: Vec<(&str, Bytes, u64)> = B1_CHUNKS
        .iter()
        .zip(1..)
        .map(|((key, record, text), v)| (*key, chunk_record(record, text), v))
        .collect();
    b1.push((ROOT_PHOTO, Bytes::from(h(B1_ROOT)), 4));
    let s = stored(&b1);
    let blob = read_blob(&s, &root).unwrap().expect("published");
    assert_eq!(blob.version, 4);
    assert_eq!(
        (
            blob.manifest.size,
            blob.manifest.sha256,
            blob.manifest.upload,
            blob.manifest.chunk_size
        ),
        (12, pin(B1_SHA), U1, 4)
    );
    assert_eq!(
        blob.manifest.chunk_sha256,
        B1_CHUNK_SHA.map(pin).to_vec(),
        "one digest per chunk, in order"
    );
    assert_eq!(read_range(&s, &root, 0, 12).unwrap(), &b"hello, blob!"[..]);
    let mut b2: Vec<(&str, Bytes, u64)> = B2_CHUNKS
        .iter()
        .zip(5..)
        .map(|((key, record, text), v)| (*key, chunk_record(record, text), v))
        .collect();
    b2.push((ROOT_PHOTO, Bytes::from(h(B2_ROOT)), 7));
    let s = stored(&b2);
    assert_eq!(
        read_range(&s, &root, 0, 15).unwrap(),
        &b"HELLO, BLOB! v2"[..]
    );
    assert_eq!(
        read_blob(&s, &root).unwrap().unwrap().manifest.chunk_sha256,
        B2_CHUNK_SHA.map(pin).to_vec()
    );
    let s = stored(&[(ROOT_EMPTY, Bytes::from(h(B3_ROOT)), 1)]);
    let blob = read_blob(&s, &empty_root).unwrap().expect("published");
    assert_eq!(
        (
            blob.manifest.size,
            blob.manifest.sha256,
            blob.manifest.chunks()
        ),
        (0, pin(B3_SHA), 0)
    );
    assert_eq!(read_range(&s, &empty_root, 0, 0).unwrap(), Bytes::new());

    // The zero-byte id: esc(h'6100') = 61 00 ff 00 01, then sub 0x04.
    let zero = root_key(TenantId(1), AffinityId(1), &[0x61, 0x00]);
    assert_eq!(chunk_key(&zero, &U1, 1), Bytes::from(h(ZERO_ID_CHUNK_1)));
    let parsed = parse(&h(ZERO_ID_CHUNK_1)).unwrap();
    assert_eq!(parsed.chunk, Some((U1, 1)));
    assert_eq!(parsed.root(), zero);
}

/// The digest differential: the whole-blob digest and
/// every chunk digest a publish writes equal values pinned from Node `crypto` and `sha256sum`.
#[test]
fn published_digests_equal_the_pinned_sha256_values() {
    let mut k = b1();
    let blob = read_blob(&k.snapshot(), &photo()).unwrap().unwrap();
    assert_eq!(blob.manifest.sha256, pin(B1_SHA));
    assert_eq!(blob.manifest.chunk_sha256, B1_CHUNK_SHA.map(pin).to_vec());
    let answer = upload(
        &mut k,
        &photo(),
        &U2,
        b"HELLO, BLOB! v2",
        8,
        Expected::Version(4),
        None,
    );
    assert_eq!(answer, Ok(Answer::Published));
    let blob = read_blob(&k.snapshot(), &photo()).unwrap().unwrap();
    assert_eq!(blob.manifest.sha256, pin(B2_SHA));
    assert_eq!(blob.manifest.chunk_sha256, B2_CHUNK_SHA.map(pin).to_vec());
    // One wrong digit in the whole-blob digest is refused; nothing is written.
    let mut wrong = pin(B1_SHA);
    wrong[31] ^= 0x01;
    let other = root_key(TenantId(1), AffinityId(1), b"other");
    for (i, (_, _, text)) in B1_CHUNKS.iter().enumerate() {
        let c = put_chunk(&k.snapshot(), &other, &U1, i as u32, text.as_bytes()).unwrap();
        k.commit(&c);
    }
    let s = k.snapshot();
    assert_eq!(
        publish(
            &s,
            &other,
            Expected::Absent,
            &U1,
            12,
            4,
            &wrong,
            Generation(1)
        ),
        apply_err(ApplyError::BlobDigestMismatch)
    );
    assert_eq!(
        publish(
            &s,
            &other,
            Expected::Absent,
            &U1,
            12,
            4,
            &pin(B1_SHA),
            Generation(1)
        )
        .map(|c| c.mutations.len()),
        Ok(1)
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// The digest differential's property: random blobs cut at random
    /// chunk sizes read back byte-equal, whole and in a random range.
    #[test]
    fn random_blobs_read_back_byte_equal(
        data in proptest::collection::vec(any::<u8>(), 0..2048),
        cut in 0.0f64..1.0,
        a in 0.0f64..1.0,
        b in 0.0f64..1.0,
    ) {
        let low = data.len().div_ceil(MAX_CHUNKS).max(1);
        let high = data.len().max(low);
        let chunk_size = low + ((high - low) as f64 * cut) as usize;
        let mut k = Kernel::new();
        let answer = upload(&mut k, &photo(), &U1, &data, chunk_size, Expected::Absent, None);
        prop_assert_eq!(answer, Ok(Answer::Published));
        prop_assert_eq!(read_all(&k, &photo()).unwrap().unwrap(), &data[..]);
        let from = (data.len() as f64 * a) as usize;
        let len = ((data.len() - from) as f64 * b) as usize;
        let got = read_range(&k.snapshot(), &photo(), from as u64, len as u64).unwrap();
        prop_assert_eq!(got, &data[from..from + len]);
    }
}

/// The idempotent chunk: resending a stored chunk with
/// the same bytes compiles to nothing; other bytes are `ChunkConflict`.
#[test]
fn a_resent_chunk_is_nothing_and_other_bytes_conflict() {
    let k = b1();
    let s = k.snapshot();
    assert_eq!(put_chunk(&s, &photo(), &U1, 1, b"o, b"), Ok(empty()));
    assert_eq!(
        put_chunk(&s, &photo(), &U1, 1, b"o, B"),
        apply_err(ApplyError::ChunkConflict { index: 1 })
    );
    assert_eq!(
        put_chunk(&s, &photo(), &U1, 1, b""),
        apply_err(ApplyError::ChunkConflict { index: 1 })
    );
    // An index not yet stored is a write.
    assert_eq!(
        put_chunk(&s, &photo(), &U1, 3, b"!").map(|c| c.mutations.len()),
        Ok(1)
    );
}

/// The chunks of `pieces` under `root`, committed, then the snapshot.
fn with_chunks(root: &RootKey, pieces: &[(u32, &[u8])]) -> MapSnapshot {
    let mut k = Kernel::new();
    for (index, bytes) in pieces {
        let c = put_chunk(&k.snapshot(), root, &U1, *index, bytes).unwrap();
        k.commit(&c);
    }
    k.snapshot()
}

/// Publish verifies: each way an
/// upload can disagree with its publish is refused by name, and a refusal is all a caller gets,
/// so nothing is written.
#[test]
fn publish_refuses_every_disagreement_by_name() {
    let o = photo();
    let sha12 = pin(B1_SHA);
    let p = |s: &MapSnapshot, size: u64, chunk_size: u64, digest: &[u8; 32]| {
        publish(
            s,
            &o,
            Expected::Absent,
            &U1,
            size,
            chunk_size,
            digest,
            Generation(1),
        )
    };
    // A missing middle chunk.
    let s = with_chunks(&o, &[(0, b"hell"), (2, b"lob!")]);
    assert_eq!(
        p(&s, 12, 4, &sha12),
        apply_err(ApplyError::ChunkMissing { index: 1 })
    );
    // One chunk past n.
    let s = with_chunks(&o, &[(0, b"hell"), (1, b"o, b"), (2, b"lob!")]);
    assert_eq!(
        p(&s, 8, 4, &sha(b"hello, b")),
        apply_err(ApplyError::ExtraChunk { index: 2 })
    );
    // A short chunk.
    let s = with_chunks(&o, &[(0, b"hell"), (1, b"o,"), (2, b"lob!")]);
    assert_eq!(
        p(&s, 10, 4, &sha(b"hello,lob!")),
        apply_err(ApplyError::ChunkLength {
            index: 1,
            expected: 4,
            found: 2
        })
    );
    // A short last chunk too: the last is `size − (n − 1)·chunk_size`, exactly.
    let s = with_chunks(&o, &[(0, b"hell"), (1, b"o, b"), (2, b"lo")]);
    assert_eq!(
        p(&s, 12, 4, &sha(b"hello, blo")),
        apply_err(ApplyError::ChunkLength {
            index: 2,
            expected: 4,
            found: 2
        })
    );
    // A wrong whole-blob digest.
    let s = with_chunks(&o, &[(0, b"hell"), (1, b"o, b"), (2, b"lob!")]);
    assert_eq!(
        p(&s, 12, 4, &[0; 32]),
        apply_err(ApplyError::BlobDigestMismatch)
    );
    // A chunk size of 0 or over MAX_CHUNK.
    for bad in [0, MAX_CHUNK as u64 + 1, u64::MAX] {
        assert_eq!(
            p(&s, 12, bad, &sha12),
            apply_err(ApplyError::InvalidChunkSize { found: bad })
        );
    }
    // More than MAX_CHUNKS chunks, and a size whose `n` would overflow a naive
    // `(size + chunk_size − 1) / chunk_size`.
    assert_eq!(p(&s, 256, 1, &sha12), apply_err(ApplyError::TooManyChunks));
    assert_eq!(
        p(&s, u64::MAX, 1, &sha12),
        apply_err(ApplyError::TooManyChunks)
    );
    assert_eq!(
        p(&s, u64::MAX, MAX_CHUNK as u64, &sha12),
        apply_err(ApplyError::TooManyChunks)
    );
    assert_eq!(
        chunk_count(u64::MAX, MAX_CHUNK as u64),
        Err(ApplyError::TooManyChunks)
    );
    // A stale `--expect`.
    let mut k = b1();
    assert_eq!(
        upload(
            &mut k,
            &o,
            &U2,
            b"HELLO, BLOB! v2",
            8,
            Expected::Version(4),
            None
        ),
        Ok(Answer::Published)
    );
    assert_eq!(
        publish(
            &k.snapshot(),
            &o,
            Expected::Version(4),
            &U1,
            12,
            4,
            &sha12,
            Generation(1)
        ),
        apply_err(ApplyError::VersionConflict {
            expected: 4,
            found: 7
        })
    );
    // An `--expect` on nothing.
    let none = root_key(TenantId(1), AffinityId(1), b"none");
    assert_eq!(
        publish(
            &k.snapshot(),
            &none,
            Expected::Version(1),
            &U1,
            0,
            4,
            &pin(B3_SHA),
            Generation(1)
        ),
        apply_err(ApplyError::ObjectAbsent)
    );
}

// ---- the protocol model ----------------------------------------------------------------------

/// mulberry32, as `0014-blob-vectors.mjs` uses it.
struct Rng(u32);

impl Rng {
    fn next(&mut self) -> f64 {
        self.0 = self.0.wrapping_add(0x6d2b_79f5);
        let mut t = self.0;
        t = (t ^ (t >> 15)).wrapping_mul(t | 1);
        t ^= t.wrapping_add((t ^ (t >> 7)).wrapping_mul(t | 61));
        f64::from(t ^ (t >> 14)) / 4_294_967_296.0
    }

    fn pick(&mut self, n: usize) -> usize {
        (self.next() * n as f64) as usize
    }
}

/// A publish whose commit the client may not hear about.
#[derive(Clone)]
struct Intent {
    upload: Upload,
    n: u32,
    expected: Expected,
    sha256: [u8; 32],
    /// The root record its compile wrote: canonical, so equal bytes are the same manifest.
    root_value: Bytes,
    commit_seq: u64,
    /// The snapshot taken right after its commit.
    after: Option<MapSnapshot>,
}

struct Pending {
    generation: u64,
    compiled: Compiled,
    /// For a publish: what it means, and the chunk payloads its compile read.
    publish: Option<(Intent, Vec<Bytes>)>,
}

#[derive(Debug, Default)]
struct Tally {
    applied: u64,
    refused: u64,
    /// Refusals by the generation fence (admission check 5) alone.
    refused_by_generation: u64,
    failovers: u64,
    missing: u64,
    different: u64,
    retries: u64,
    /// Retries of a publish whose commit a failover had undone.
    retries_after_rollback: u64,
    stale_retries_recompiled: u64,
    already_on_retry: u64,
    falsely_failed: u64,
    falsely_already: u64,
}

impl std::ops::AddAssign for Tally {
    fn add_assign(&mut self, o: Self) {
        self.applied += o.applied;
        self.refused += o.refused;
        self.refused_by_generation += o.refused_by_generation;
        self.failovers += o.failovers;
        self.missing += o.missing;
        self.different += o.different;
        self.retries += o.retries;
        self.retries_after_rollback += o.retries_after_rollback;
        self.stale_retries_recompiled += o.stale_retries_recompiled;
        self.already_on_retry += o.already_on_retry;
        self.falsely_failed += o.falsely_failed;
        self.falsely_already += o.falsely_already;
    }
}

enum Published {
    Already,
    Refused,
    Request(Compiled, Vec<Bytes>, [u8; 32]),
}

/// A client's publish of `n` 3-byte chunks of `upload`, on `snapshot`, sent to the kernel serving
/// `serving`. A fresh publish hashes the chunks it reads; a retry sends its original digest.
fn compile_publish(
    snapshot: &MapSnapshot,
    serving: u64,
    upload: &Upload,
    n: u32,
    expected: Expected,
    retry_sha: Option<[u8; 32]>,
) -> Published {
    let root = photo();
    let payloads: Vec<Bytes> = (0..n)
        .filter_map(|i| {
            snapshot
                .get(Namespace::User, &chunk_of(&root, upload, i))
                .map(|r| Bytes::copy_from_slice(payload(&r)))
        })
        .collect();
    let sha256 = retry_sha.unwrap_or_else(|| sha(&payloads.concat()));
    match publish(
        snapshot,
        &root,
        expected,
        upload,
        3 * u64::from(n),
        3,
        &sha256,
        Generation(serving),
    ) {
        Ok(c) if c.mutations.is_empty() => Published::Already,
        Ok(c) => Published::Request(c, payloads, sha256),
        Err(_) => Published::Refused,
    }
}

/// The invariant, from ADR-rdb-0014 §6 and §8: a published root names chunks that exist with the
/// bytes its publish read. `published` maps each root record any applied publish wrote to those
/// bytes.
fn broken(k: &Kernel, published: &HashMap<Bytes, (Upload, Vec<Bytes>)>) -> Option<&'static str> {
    let root = photo();
    let (_, value) = k.records.get(root.as_bytes())?;
    let (upload, payloads) = published
        .get(value)
        .expect("every root here was written by a publish this run applied");
    for (i, want) in payloads.iter().enumerate() {
        match k.records.get(&chunk_of(&root, upload, i as u32)) {
            None => return Some("missing"),
            Some((_, record)) if payload(record) != &want[..] => return Some("different"),
            Some(_) => {}
        }
    }
    None
}

/// One run of the ADR-rdb-0014 §6 model (Part 3b of the Node script, rules ported): stale
/// compiles applied in random order, chunk bytes that vary per write, failovers that roll back to
/// a base and reuse its versions, lost publish replies, and half the retries compiled on the
/// snapshot taken right after their commit.
fn protocol_run(seed: u32, m: &Model) -> Tally {
    let mut rng = Rng(seed);
    let root = photo();
    let mut t = Tally::default();
    let mut k = Kernel::new();
    let mut history = vec![k.records.clone()];
    let mut published: HashMap<Bytes, (Upload, Vec<Bytes>)> = HashMap::new();
    let mut pending: Vec<Pending> = Vec::new();
    let mut retries: VecDeque<Intent> = VecDeque::new();
    for _ in 0..m.steps {
        if rng.next() < m.p_failover {
            // Failover: a committed suffix is lost and a new generation starts at the base.
            let base = k.seq.saturating_sub(rng.pick(m.rollback + 1) as u64);
            k.records = history[base as usize].clone();
            k.seq = base;
            history.truncate(base as usize + 1);
            k.generation += 1;
            t.failovers += 1;
            continue;
        }
        if !retries.is_empty() && rng.next() < 0.2 {
            // A client retries a publish whose reply was lost, with its original Expected.
            let intent = retries.pop_front().expect("not empty");
            t.retries += 1;
            let still_there = k
                .records
                .get(root.as_bytes())
                .is_some_and(|(_, v)| *v == intent.root_value);
            let in_lineage = history
                .get(intent.commit_seq as usize)
                .and_then(|s| s.get(root.as_bytes()))
                .is_some_and(|(v, value)| *v == intent.commit_seq && *value == intent.root_value);
            if !in_lineage {
                t.retries_after_rollback += 1;
            }
            let first = if rng.next() < 0.5 {
                intent.after.clone().expect("set at commit")
            } else {
                k.snapshot()
            };
            let retry = |s: &MapSnapshot, serving: u64| {
                compile_publish(
                    s,
                    serving,
                    &intent.upload,
                    intent.n,
                    intent.expected,
                    Some(intent.sha256),
                )
            };
            let mut r = retry(&first, k.generation);
            if !matches!(r, Published::Already) && first.generation().0 != k.generation {
                // Refused, or fenced, on a stale snapshot: re-read and retry.
                t.stale_retries_recompiled += 1;
                r = retry(&k.snapshot(), k.generation);
            }
            if matches!(r, Published::Already) {
                t.already_on_retry += 1;
                if !in_lineage && !still_there {
                    t.falsely_already += 1;
                }
            } else if still_there {
                t.falsely_failed += 1;
            }
            continue;
        }
        if !pending.is_empty() && rng.next() < 0.5 {
            let req = pending.swap_remove(rng.pick(pending.len()));
            if let Err(refused) = k.apply(&req.compiled, Some(req.generation)) {
                t.refused += 1;
                if refused == Refused::GenerationChanged {
                    t.refused_by_generation += 1;
                }
                continue;
            }
            history.push(k.records.clone());
            t.applied += 1;
            if let Some((mut intent, payloads)) = req.publish {
                published.insert(intent.root_value.clone(), (intent.upload, payloads));
                intent.commit_seq = k.seq;
                intent.after = Some(k.snapshot());
                if rng.next() < 0.3 {
                    retries.push_back(intent);
                }
            }
            match broken(&k, &published) {
                Some("missing") => t.missing += 1,
                Some(_) => t.different += 1,
                None => {}
            }
            continue;
        }
        let snapshot = k.snapshot();
        let generation = k.generation;
        let upload: Upload = [0x30 + rng.pick(m.uploads) as u8; 16];
        let n = 1 + rng.pick(3) as u32;
        let roll = rng.pick(10);
        let root_version = snapshot.version(Namespace::User, root.as_bytes());
        let req = if roll < 5 {
            let i = rng.pick(n as usize + 1) as u32;
            let bytes = [upload[0], i as u8, rng.pick(2) as u8];
            put_chunk(&snapshot, &root, &upload, i, &bytes)
                .ok()
                .map(|c| (c, None))
        } else if roll < 7 {
            let expected = root_version.map_or(Expected::Absent, Expected::Version);
            match compile_publish(&snapshot, generation, &upload, n, expected, None) {
                Published::Request(c, payloads, sha256) => {
                    let Mutation::Put { value, .. } = &c.mutations[0] else {
                        panic!("publish writes one root Put");
                    };
                    let intent = Intent {
                        upload,
                        n,
                        expected,
                        sha256,
                        root_value: value.clone(),
                        commit_seq: 0,
                        after: None,
                    };
                    Some((c, Some((intent, payloads))))
                }
                Published::Already | Published::Refused => None,
            }
        } else if roll < 8 {
            root_version.and_then(|v| delete_blob(&snapshot, &root, v).ok().map(|c| (c, None)))
        } else {
            let floor = k.seq.saturating_sub(rng.pick(4) as u64);
            collect_garbage(&snapshot, &root, floor)
                .ok()
                .map(|c| (c, None))
        };
        if let Some((compiled, publish)) = req.filter(|(c, _)| !c.mutations.is_empty()) {
            pending.push(Pending {
                generation,
                compiled,
                publish,
            });
        }
    }
    t
}

/// The rates of one protocol-model run.
struct Model {
    steps: usize,
    /// The chance a step is a failover.
    p_failover: f64,
    /// A failover loses the last 0 ..= `rollback` commits.
    rollback: usize,
    /// Distinct upload ids the clients draw from.
    uploads: usize,
}

/// `seeds` runs of `m`. None may break a manifest or misanswer a retry, and together they
/// must have exercised failovers, refusals, retries and stale-snapshot retries, and at least one
/// refusal by the generation fence and one retry of a commit a failover undid.
fn assert_protocol(seeds: u32, m: &Model) {
    let mut t = Tally::default();
    for seed in 1..=seeds {
        t += protocol_run(seed, m);
    }
    eprintln!(
        "protocol model, {seeds} seeds x {} steps, failover p = {}: {t:?}",
        m.steps, m.p_failover
    );
    assert_eq!((t.missing, t.different), (0, 0), "broken manifests: {t:?}");
    assert_eq!(t.falsely_failed, 0, "{t:?}");
    assert_eq!(t.falsely_already, 0, "{t:?}");
    assert!(
        t.applied > 10_000 && t.failovers > 500 && t.refused > 1000,
        "{t:?}"
    );
    assert!(t.retries > 100 && t.already_on_retry > 50, "{t:?}");
    assert!(t.stale_retries_recompiled > 5, "{t:?}");
    assert!(t.refused_by_generation > 0, "no fence refusal: {t:?}");
    assert!(
        t.retries_after_rollback > 0,
        "no retry after a rollback: {t:?}"
    );
}

/// The protocol model at Node Part 3b's rates (ADR-rdb-0014 §6, §8 and §12): failover
/// p = 0.02 losing 0-3 commits, 4 uploads, lost publish replies p = 0.3, half the retries on the
/// snapshot taken after their commit; 200 seeds x 400 steps (Node ran 2,000). No published
/// root ever names a missing or different
/// chunk, no retry is told "failed" while its commit stands, and none is told "already
/// published" for a commit a failover undid.
#[test]
fn the_protocol_model_never_breaks_a_manifest_or_misanswers_a_retry() {
    assert_protocol(
        200,
        &Model {
            steps: 400,
            p_failover: 0.02,
            rollback: 3,
            uploads: 4,
        },
    );
}

/// The protocol model with failovers five times as often, p = 0.1. A request compiled before a
/// failover breaks a manifest only when a version it names is reused for other bytes after the
/// rollback, so the generation fence (admission check 5, applied first by the stand-in) needs
/// frequent failovers to be reached at this seed count. This test reaches the fence through real
/// failovers and checks that every manifest stays whole.
#[test]
fn under_frequent_failovers_the_generation_fence_keeps_every_manifest_whole() {
    assert_protocol(
        200,
        &Model {
            steps: 400,
            p_failover: 0.1,
            rollback: 3,
            uploads: 4,
        },
    );
}

// ---- GC against a model ----------------------------------------------------------------------

/// What GC must delete, from ADR-rdb-0014 §8 alone: every chunk that the root does not name and
/// that is at a version ≤ `floor`, in key order, as the longest prefix of at most
/// `MAX_REQUEST_MUTATIONS` deletes whose record, with its one root guard, fits
/// `MAX_ENVELOPE_BYTES` by the kernel's `record_len`.
fn gc_model(
    chunks: &BTreeMap<Bytes, (Upload, u32, u64)>,
    root: Option<(Upload, u32)>,
    floor: u64,
) -> Vec<Mutation> {
    let garbage: Vec<Mutation> = chunks
        .iter()
        .filter(|(_, (u, i, v))| !root.is_some_and(|(ru, n)| ru == *u && *i < n) && *v <= floor)
        .map(|(key, (_, _, v))| Mutation::Delete {
            key: key.clone(),
            expected_version: Some(*v),
        })
        .collect();
    let most = garbage.len().min(MAX_REQUEST_MUTATIONS);
    let fits = (0..=most)
        .rev()
        .find(|&p| record_len(1, &garbage[..p]) <= MAX_ENVELOPE_BYTES)
        .expect("an empty batch fits");
    garbage[..fits].to_vec()
}

/// A random object for GC: an id of 1 to 5,000 bytes, 1 to 3 uploads of 0 to 299 contiguous 1-byte
/// chunks at random versions, and, two times in three, a blob root naming the first upload's
/// first `n` chunks (so that upload also has unreachable chunks at index ≥ n).
struct GcWorld {
    k: Kernel,
    root: RootKey,
    chunks: BTreeMap<Bytes, (Upload, u32, u64)>,
    named: Option<(Upload, u32)>,
    root_version: Option<u64>,
}

fn gc_world(rng: &mut Rng) -> GcWorld {
    let id = vec![b'a'; [1, 5, 40, 4071, 5000][rng.pick(5)]];
    let root = root_key(TenantId(1), AffinityId(1), &id);
    let uploads = 1 + rng.pick(3);
    let mut chunks = BTreeMap::new();
    let mut counts = Vec::new();
    for u in 0..uploads {
        let upload: Upload = [0x40 + u as u8; 16];
        let count = rng.pick(300) as u32;
        counts.push(count);
        for i in 0..count {
            let version = 1 + rng.pick(60) as u64;
            chunks.insert(chunk_of(&root, &upload, i), (upload, i, version));
        }
    }
    let mut k = Kernel::new();
    for (key, (_, i, version)) in &chunks {
        let value = seal(Kind::Chunk, &[*i as u8]).unwrap();
        k.records.insert(key.clone(), (*version, value));
    }
    k.seq = 61;
    let mut named = None;
    let mut root_version = None;
    let first: Upload = [0x40; 16];
    if counts[0] > 0 && rng.pick(3) != 0 {
        let n = 1 + rng.pick(counts[0].min(6) as usize) as u32;
        // The root, compiled over a snapshot holding only the chunks it names.
        let mut s = MapSnapshot::new(Generation(1));
        let mut bytes = Vec::new();
        for i in 0..n {
            let key = chunk_of(&root, &first, i);
            let (version, value) = k.records[&key].clone();
            bytes.push(i as u8);
            s.insert(key, version, value);
        }
        let c = publish(
            &s,
            &root,
            Expected::Absent,
            &first,
            u64::from(n),
            1,
            &sha(&bytes),
            Generation(1),
        )
        .unwrap();
        let Mutation::Put { value, .. } = &c.mutations[0] else {
            panic!("one root Put");
        };
        k.seq += 1;
        k.records.insert(root.to_bytes(), (k.seq, value.clone()));
        named = Some((first, n));
        root_version = Some(k.seq);
    }
    GcWorld {
        k,
        root,
        chunks,
        named,
        root_version,
    }
}

/// GC against a model: each batch is
/// exactly the model's, guarded by the root as read; repeating GC to the end deletes every
/// unreachable chunk at or below the floor and nothing else; every batch fits the record cap.
#[test]
fn gc_deletes_exactly_what_the_model_says_within_both_bounds() {
    let mut batches = 0;
    let mut by_bytes = 0;
    for seed in 1..=GC_SEEDS {
        let mut rng = Rng(seed);
        let mut w = gc_world(&mut rng);
        let floor = rng.pick(63) as u64;
        let guard = match w.root_version {
            Some(version) => Condition::VersionEquals {
                key: w.root.to_bytes(),
                version,
            },
            None => Condition::Absent {
                key: w.root.to_bytes(),
            },
        };
        let mut left = w.chunks.clone();
        loop {
            let compiled = collect_garbage(&w.k.snapshot(), &w.root, floor).unwrap();
            let want = gc_model(&left, w.named, floor);
            assert_eq!(compiled.mutations, want, "seed {seed}");
            if want.is_empty() {
                assert_eq!(compiled.conditions, Vec::new(), "seed {seed}");
                break;
            }
            assert_eq!(compiled.conditions, vec![guard.clone()], "seed {seed}");
            let len = record_len(1, &compiled.mutations);
            assert!(len <= MAX_ENVELOPE_BYTES, "seed {seed}: {len}");
            let garbage = left
                .values()
                .filter(|(u, i, v)| {
                    !w.named.is_some_and(|(ru, n)| ru == *u && *i < n) && *v <= floor
                })
                .count();
            if want.len() < garbage.min(MAX_REQUEST_MUTATIONS) {
                by_bytes += 1;
            }
            w.k.commit(&compiled);
            batches += 1;
            for m in &compiled.mutations {
                let Mutation::Delete { key, .. } = m else {
                    panic!("GC only deletes");
                };
                left.remove(key);
            }
        }
        // Everything left is reachable or above the floor; nothing reachable was ever deleted.
        for (key, (u, i, v)) in &w.chunks {
            let reachable = w.named.is_some_and(|(ru, n)| ru == *u && *i < n);
            assert_eq!(
                w.k.records.contains_key(key),
                reachable || *v > floor,
                "seed {seed}"
            );
        }
    }
    eprintln!("gc model: {GC_SEEDS} seeds, {batches} batches, {by_bytes} cut by bytes");
    assert!(batches > GC_SEEDS as usize && by_bytes > 0);
}

const GC_SEEDS: u32 = 100;

/// GC's fixed points: the floor; the count bound, 255 then 45 then nothing; the byte bound under
/// a 5,000-byte id, 207 then 48 then nothing (Node Part 2); and chunks under a map root.
#[test]
fn gc_batches_match_the_walked_counts() {
    let abandoned = |id: &[u8], uploads: &[(Upload, u32)]| {
        let root = root_key(TenantId(1), AffinityId(1), id);
        let mut k = Kernel::new();
        for (upload, count) in uploads {
            for i in 0..*count {
                let c = put_chunk(&k.snapshot(), &root, upload, i, b"z").unwrap();
                k.commit(&c);
            }
        }
        (k, root)
    };
    let batch_sizes = |k: &mut Kernel, root: &RootKey, floor: u64| {
        let mut sizes = Vec::new();
        loop {
            let c = collect_garbage(&k.snapshot(), root, floor).unwrap();
            sizes.push(c.mutations.len());
            if c.mutations.is_empty() {
                return sizes;
            }
            assert!(record_len(c.conditions.len(), &c.mutations) <= MAX_ENVELOPE_BYTES);
            k.commit(&c);
        }
    };
    // The floor: versions 1, 2, 3 and floor 2.
    let (mut k, root) = abandoned(b"photo", &[(U1, 3)]);
    assert_eq!(batch_sizes(&mut k, &root, 2), vec![2, 0]);
    assert!(k.records.contains_key(&chunk_of(&root, &U1, 2)));
    // The count bound.
    let (mut k, root) = abandoned(b"big", &[(U1, 150), (U2, 150)]);
    let floor = k.seq;
    assert_eq!(batch_sizes(&mut k, &root, floor), vec![255, 45, 0]);
    // The byte bound.
    let (mut k, root) = abandoned(&[b'a'; 5000], &[(U1, 255)]);
    let floor = k.seq;
    assert_eq!(batch_sizes(&mut k, &root, floor), vec![207, 48, 0]);
    // A map at the root names no chunk, so its chunks are garbage. The batch is guarded by
    // the map's version and leaves the map as it was.
    let root = photo();
    let map = {
        let mut m = Map::new();
        m.insert(MapKey::new("keys"), Value::Integer(Int::from(1_u64)));
        m.insert(MapKey::new("count"), Value::Integer(Int::from(0_u64)));
        seal(Kind::Map, &encode(&Value::Map(m)).unwrap()).unwrap()
    };
    let mut k = Kernel::new();
    k.records.insert(root.to_bytes(), (1, map.clone()));
    k.seq = 1;
    let c = put_chunk(&k.snapshot(), &root, &U1, 0, b"z").unwrap();
    assert_eq!(k.commit(&c), 2);
    let c = collect_garbage(&k.snapshot(), &root, 2).unwrap();
    assert_eq!(
        c.conditions,
        vec![Condition::VersionEquals {
            key: root.to_bytes(),
            version: 1,
        }]
    );
    assert_eq!(
        c.mutations,
        vec![Mutation::Delete {
            key: chunk_of(&root, &U1, 0),
            expected_version: Some(2),
        }]
    );
    k.commit(&c);
    assert_eq!(k.records.get(&root.to_bytes()), Some(&(1, map)));
    assert_eq!(k.records.len(), 1);
}

/// GC's fixed case, G1/G2 neighbour after the chunk range. `photp` sorts right after every chunk
/// key of `photo`, so GC's scan reaches it and must stop at the end of `photo`'s chunk prefix.
/// G1: the neighbour holds a rootless chunk, which is not `photo`'s garbage. G2: the neighbour is
/// a published blob, whose root key would read as a chunk tail of length 0. In both, GC of
/// `photo` deletes exactly `photo`'s chunks and leaves every neighbour record byte-identical.
#[test]
fn gc_stops_at_the_end_of_the_chunk_range() {
    let root = photo();
    let neighbour = root_key(TenantId(1), AffinityId(1), b"photp");
    let last_chunk = chunk_of(&root, &[0xff; 16], u32::MAX);
    assert!(last_chunk < neighbour.to_bytes());
    assert!(last_chunk < chunk_of(&neighbour, &U2, 0));
    type AddNeighbour = fn(&mut Kernel, &RootKey);
    let cases: [(&str, AddNeighbour); 2] = [
        ("G1 rootless neighbour", |k, n| {
            let c = put_chunk(&k.snapshot(), n, &U2, 0, b"n").unwrap();
            k.commit(&c);
        }),
        ("G2 published neighbour", |k, n| {
            let answer = upload(k, n, &U2, b"neighbour", 4, Expected::Absent, None);
            assert_eq!(answer, Ok(Answer::Published));
        }),
    ];
    for (name, add_neighbour) in cases {
        let mut k = Kernel::new();
        for i in 0..3 {
            let c = put_chunk(&k.snapshot(), &root, &U1, i, b"z").unwrap();
            k.commit(&c);
        }
        add_neighbour(&mut k, &neighbour);
        let mut expected = k.records.clone();
        for i in 0..3 {
            assert!(
                expected.remove(&chunk_of(&root, &U1, i)).is_some(),
                "{name}"
            );
        }
        assert!(!expected.is_empty(), "{name}: the neighbour has records");
        let floor = k.seq;
        assert_eq!(
            gc(&mut k, &root, floor, None),
            Ok(Answer::Collected),
            "{name}"
        );
        assert_eq!(k.records, expected, "{name}");
    }
}

/// Scenario "stale delete after a replace" (ADR-rdb-0014 §8). A delete compiled from a snapshot
/// that still holds the old blob deletes the root by that version only, so after a replace the
/// kernel refuses it and nothing is written; a delete naming the old version on a fresh snapshot
/// is refused at compile. A delete naming the current version removes the root and only the root.
#[test]
fn a_stale_delete_never_removes_a_newer_blob() {
    let root = photo();
    let b1: &[u8] = b"hello, blob!";
    let b2: &[u8] = b"HELLO, BLOB! v2";
    let mut k = Kernel::new();
    let published = upload(&mut k, &root, &U1, b1, 4, Expected::Absent, None);
    assert_eq!(published, Ok(Answer::Published));
    let v1 = k.records[&root.to_bytes()].0;
    let stale = k.snapshot();
    let replaced = upload(&mut k, &root, &U2, b2, 4, Expected::Version(v1), None);
    assert_eq!(replaced, Ok(Answer::Published));
    let v2 = k.records[&root.to_bytes()].0;
    assert!(v2 > v1);
    let delete_at = |version| Compiled {
        mutations: vec![Mutation::Delete {
            key: root.to_bytes(),
            expected_version: Some(version),
        }],
        conditions: Vec::new(),
    };

    let c = delete_blob(&stale, &root, v1).unwrap();
    assert_eq!(c, delete_at(v1));
    let before = k.clone();
    let generation = k.generation;
    assert_eq!(
        k.apply(&c, Some(generation)),
        Err(Refused::ExpectedVersion(0))
    );
    assert_eq!(k, before, "a refused delete writes nothing");
    assert_eq!(read_all(&k, &root), Ok(Some(Bytes::from_static(b2))));

    assert_eq!(
        delete_blob(&k.snapshot(), &root, v1),
        apply_err(ApplyError::VersionConflict {
            expected: v1,
            found: v2,
        })
    );

    let c = delete_blob(&k.snapshot(), &root, v2).unwrap();
    assert_eq!(c, delete_at(v2));
    let mut expected = k.records.clone();
    expected.remove(&root.to_bytes());
    k.commit(&c);
    assert_eq!(k.records, expected);
    assert_eq!(read_all(&k, &root), Ok(None));
}

// ---- crash at every boundary -----------------------------------------------------------------

/// Crash at every boundary: after a crash at any commit of an
/// upload, a replace or a GC, a reader sees the object absent, old or new, wholly; a retry from
/// there, including after the publish committed and its reply was lost, answers success and ends
/// byte-identical to a run with no crash. A publish sent from a prefix that lacks a chunk is
/// refused by that chunk's index.
#[test]
fn a_crash_at_any_commit_reads_whole_and_a_retry_ends_identical() {
    let root = photo();
    let b1: &[u8] = b"hello, blob!";
    let b2: &[u8] = b"HELLO, BLOB! v2";
    type Step = fn(&mut Kernel, Option<usize>) -> Result<Answer, String>;
    type Readable<'a> = [Option<&'a [u8]>; 2];
    let steps: [(Step, usize, Readable); 3] = [
        (
            |k, stop| upload(k, &photo(), &U1, b"hello, blob!", 4, Expected::Absent, stop),
            4,
            [None, Some(b1)],
        ),
        (
            |k, stop| {
                upload(
                    k,
                    &photo(),
                    &U2,
                    b"HELLO, BLOB! v2",
                    8,
                    Expected::Version(4),
                    stop,
                )
            },
            3,
            [Some(b1), Some(b2)],
        ),
        (|k, stop| gc(k, &photo(), 7, stop), 1, [Some(b2), Some(b2)]),
    ];
    let mut base = Kernel::new();
    for (step, commits, readable) in steps {
        let mut reference = base.clone();
        step(&mut reference, None).unwrap();
        assert_eq!(reference.seq, base.seq + commits as u64);
        for crash in 0..=commits {
            let mut k = base.clone();
            let answer = step(&mut k, Some(crash)).unwrap();
            assert_eq!(answer == Answer::Crashed, crash < commits, "crash {crash}");
            assert_eq!(k.seq, base.seq + crash as u64);
            let seen = read_all(&k, &root).unwrap();
            assert!(
                readable.contains(&seen.as_deref()),
                "crash {crash}: read {seen:?}"
            );
            let retried = step(&mut k, None).unwrap();
            assert_ne!(retried, Answer::Crashed);
            assert_eq!(k, reference, "crash {crash}: the retry ends byte-identical");
        }
        base = reference;
    }
    // A publish sent from an upload prefix that lacks chunk K is refused by K.
    for crash in 0..3 {
        let mut k = Kernel::new();
        upload(&mut k, &root, &U1, b1, 4, Expected::Absent, Some(crash)).unwrap();
        assert_eq!(
            publish(
                &k.snapshot(),
                &root,
                Expected::Absent,
                &U1,
                12,
                4,
                &pin(B1_SHA),
                Generation(1)
            ),
            apply_err(ApplyError::ChunkMissing {
                index: crash as u32
            })
        );
    }
}

// ---- range reads -----------------------------------------------------------------------------

/// A snapshot that records which chunk indices were read.
struct Counting<'a> {
    inner: &'a MapSnapshot,
    root_len: usize,
    chunks_read: RefCell<BTreeSet<u32>>,
}

impl SnapshotRead for Counting<'_> {
    fn handle(&self) -> SnapshotHandle {
        self.inner.handle()
    }
    fn at(&self) -> Seq {
        self.inner.at()
    }
    fn generation(&self) -> Generation {
        self.inner.generation()
    }
    fn get(&self, ns: Namespace, key: &[u8]) -> Option<Bytes> {
        if key.len() > self.root_len {
            let index = u32::from_be_bytes(key[key.len() - 4..].try_into().expect("4 bytes"));
            self.chunks_read.borrow_mut().insert(index);
        }
        self.inner.get(ns, key)
    }
    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version> {
        self.inner.version(ns, key)
    }
    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        self.inner.scan(ns, from, limit)
    }
}

/// Read `offset … offset + len` and the chunk indices touched.
fn counted(
    s: &MapSnapshot,
    root: &RootKey,
    offset: u64,
    len: u64,
) -> (Result<Bytes, ValueError>, Vec<u32>) {
    let c = Counting {
        inner: s,
        root_len: root.as_bytes().len(),
        chunks_read: RefCell::new(BTreeSet::new()),
    };
    let got = read_range(&c, root, offset, len);
    (got, c.chunks_read.into_inner().into_iter().collect())
}

/// Range reads: every range equals the slice of the
/// input, reads only the chunks it intersects, and an offset near `u64::MAX` is `RangeInvalid`.
#[test]
fn every_range_is_the_exact_slice_and_reads_only_its_chunks() {
    let k = b1();
    let s = k.snapshot();
    let root = photo();
    let data = b"hello, blob!";
    for offset in 0..=12_u64 {
        for len in 0..=12 - offset {
            let (got, read) = counted(&s, &root, offset, len);
            assert_eq!(
                got.unwrap(),
                &data[offset as usize..(offset + len) as usize]
            );
            let want: Vec<u32> = if len == 0 {
                Vec::new()
            } else {
                (offset / 4..=(offset + len - 1) / 4)
                    .map(|i| i as u32)
                    .collect()
            };
            assert_eq!(read, want, "offset {offset} len {len}");
        }
    }
    // One middle chunk read alone, an empty range at the end, and each range past the end.
    assert_eq!(
        counted(&s, &root, 4, 4),
        (Ok(Bytes::from_static(b"o, b")), vec![1])
    );
    assert_eq!(counted(&s, &root, 12, 0), (Ok(Bytes::new()), vec![]));
    let invalid = apply_err(ApplyError::RangeInvalid { size: 12 });
    for (offset, len) in [
        (11, 2),
        (12, 1),
        (13, 0),
        (u64::MAX, 2),
        (u64::MAX, 0),
        (1, u64::MAX),
    ] {
        assert_eq!(counted(&s, &root, offset, len), (invalid.clone(), vec![]));
    }
    // A larger blob of 143 chunks: random ranges.
    let big: Vec<u8> = (0..1000_u32).map(|i| (i * 7 + 3) as u8).collect();
    let mut k = Kernel::new();
    upload(&mut k, &root, &U2, &big, 7, Expected::Absent, None).unwrap();
    let s = k.snapshot();
    let mut rng = Rng(7);
    for _ in 0..300 {
        let offset = rng.pick(1001) as u64;
        let len = rng.pick(1001 - offset as usize) as u64;
        let (got, read) = counted(&s, &root, offset, len);
        assert_eq!(got.unwrap(), &big[offset as usize..(offset + len) as usize]);
        let want = if len == 0 {
            0
        } else {
            (offset + len - 1) / 7 - offset / 7 + 1
        };
        assert_eq!(read.len() as u64, want, "offset {offset} len {len}");
    }
}

// ---- damage ----------------------------------------------------------------------------------

/// B1 stored from the Node bytes, with one record replaced (or removed for `None`), plus a
/// garbage chunk of another upload at version 1 that GC would delete.
fn b1_with(key: &str, record: Option<Bytes>) -> MapSnapshot {
    let mut records: BTreeMap<String, Bytes> = B1_CHUNKS
        .iter()
        .map(|(k, r, t)| ((*k).to_owned(), chunk_record(r, t)))
        .collect();
    records.insert(ROOT_PHOTO.to_owned(), Bytes::from(h(B1_ROOT)));
    match record {
        Some(r) => records.insert(key.to_owned(), r),
        None => records.remove(key),
    };
    let mut s = MapSnapshot::new(Generation(1));
    for (k, r) in records {
        s.insert(Bytes::from(h(&k)), 2, r);
    }
    s.insert(
        chunk_of(&photo(), &U2, 0),
        1,
        seal(Kind::Chunk, b"z").unwrap(),
    );
    s
}

/// `bytes` with a correct digest over a new header (a newer build's record, or a resealed edit).
fn resealed(mut bytes: Vec<u8>) -> Bytes {
    let head: [u8; 8] = bytes[..8].try_into().unwrap();
    let d = digest(&head, &bytes[HEADER_LEN..]);
    bytes[8..HEADER_LEN].copy_from_slice(&d);
    Bytes::from(bytes)
}

fn flipped(record: Bytes, at: usize) -> Bytes {
    let mut b = record.to_vec();
    b[at] ^= 0x01;
    Bytes::from(b)
}

/// Damage: each `Corrupt` cause is named, a read that
/// touches the damage returns no bytes, and GC refuses under a root it cannot read or a chunk key
/// it cannot parse, so nothing is deleted.
#[test]
fn damage_is_named_reads_fail_wholly_and_gc_refuses() {
    let root = photo();
    let root_record = || Bytes::from(h(B1_ROOT));
    // The root: a flipped payload byte; a kind no build knows, resealed; a blob payload that is
    // not a manifest, by shape and by codec.
    let mut unknown = h(B1_ROOT);
    unknown[1] = 0x7f;
    let not_manifest = |payload: &[u8]| {
        let mut b = h(B1_ROOT)[..HEADER_LEN].to_vec();
        b[4..8].copy_from_slice(&(payload.len() as u32).to_be_bytes());
        b.extend_from_slice(payload);
        resealed(b)
    };
    // Five well-typed fields, but no digest for any of the 3 chunks.
    let no_digests = {
        let mut m = Map::new();
        m.insert(MapKey::new("size"), Value::Integer(Int::from(12_u64)));
        m.insert(MapKey::new("sha256"), Value::Bytes(vec![0; 32]));
        m.insert(MapKey::new("upload"), Value::Bytes(U1.to_vec()));
        m.insert(MapKey::new("chunk_size"), Value::Integer(Int::from(4_u64)));
        m.insert(MapKey::new("chunk_sha256"), Value::Array(Vec::new()));
        encode(&Value::Map(m)).unwrap()
    };
    let root_cases: [(Bytes, Corrupt); 4] = [
        (
            flipped(root_record(), 60),
            Corrupt::Envelope(EnvelopeError::DigestMismatch),
        ),
        (
            resealed(unknown),
            Corrupt::Envelope(EnvelopeError::UnknownKind(0x7f)),
        ),
        (
            not_manifest(&no_digests),
            Corrupt::Manifest(ManifestError::Shape(
                "chunk_sha256 does not hold one digest per chunk",
            )),
        ),
        (
            not_manifest(&h("a0")),
            Corrupt::Manifest(ManifestError::Shape("not exactly the five manifest fields")),
        ),
    ];
    for (record, cause) in root_cases {
        let s = b1_with(ROOT_PHOTO, Some(record));
        assert_eq!(read_blob(&s, &root), corrupt(cause.clone()));
        assert_eq!(read_range(&s, &root, 0, 1), corrupt(cause.clone()));
        assert_eq!(collect_garbage(&s, &root, 99), corrupt(cause));
    }
    let s = b1_with(ROOT_PHOTO, Some(not_manifest(&h("ff"))));
    assert!(matches!(
        read_blob(&s, &root),
        Err(ValueError::Corrupt(Corrupt::Manifest(
            ManifestError::Codec(_)
        )))
    ));
    assert!(matches!(
        collect_garbage(&s, &root, 99),
        Err(ValueError::Corrupt(Corrupt::Manifest(
            ManifestError::Codec(_)
        )))
    ));

    // Chunk 1: flipped; missing; a sound record of another kind; sound with other bytes of the
    // same length; sound with another length. Reads across it fail; reads beside it do not.
    let c1 = B1_CHUNKS[1].0;
    let chunk_cases: [(Option<Bytes>, Corrupt); 5] = [
        (
            Some(flipped(chunk_record(B1_CHUNKS[1].1, "o, b"), 41)),
            Corrupt::Chunk {
                index: 1,
                error: EnvelopeError::DigestMismatch,
            },
        ),
        (None, Corrupt::ChunkMissing { index: 1 }),
        (
            Some(seal(Kind::Document, &h("a0")).unwrap()),
            Corrupt::ChunkMismatch { index: 1 },
        ),
        (
            Some(seal(Kind::Chunk, b"o, B").unwrap()),
            Corrupt::ChunkMismatch { index: 1 },
        ),
        (
            Some(seal(Kind::Chunk, b"o, bb").unwrap()),
            Corrupt::ChunkMismatch { index: 1 },
        ),
    ];
    for (record, cause) in chunk_cases {
        let s = b1_with(c1, record);
        assert_eq!(read_range(&s, &root, 0, 12), corrupt(cause.clone()));
        assert_eq!(read_range(&s, &root, 3, 2), corrupt(cause));
        assert_eq!(read_range(&s, &root, 0, 4).unwrap(), &b"hell"[..]);
        assert_eq!(read_range(&s, &root, 8, 4).unwrap(), &b"lob!"[..]);
    }
    // A chunk key one byte short refuses the whole batch.
    let mut s = b1_with(ROOT_PHOTO, Some(root_record()));
    let mut short = chunk_of(&root, &U2, 5).to_vec();
    short.pop();
    s.insert(Bytes::from(short), 1, seal(Kind::Chunk, b"z").unwrap());
    assert_eq!(
        collect_garbage(&s, &root, 99),
        corrupt(Corrupt::Key(KeyError::ChunkTail { len: 19 }))
    );
}

/// The five fields of a well-formed manifest: 12 bytes cut at 4, so 3 chunk digests.
fn manifest_fields() -> Map {
    let mut m = Map::new();
    m.insert(MapKey::new("size"), Value::Integer(Int::from(12_u64)));
    m.insert(MapKey::new("sha256"), Value::Bytes(vec![0; 32]));
    m.insert(MapKey::new("upload"), Value::Bytes(U1.to_vec()));
    m.insert(MapKey::new("chunk_size"), Value::Integer(Int::from(4_u64)));
    m.insert(
        MapKey::new("chunk_sha256"),
        Value::Array(vec![Value::Bytes(vec![0; 32]); 3]),
    );
    m
}

/// `manifest_fields` with `name` set to `value`.
fn manifest_with(name: &str, value: Value) -> Value {
    let mut m = manifest_fields();
    m.insert(MapKey::new(name), value);
    Value::Map(m)
}

/// The strict manifest reading (ADR-rdb-0014 §2). Protects a reader of a damaged root: a blob
/// root whose payload is canonical CBOR but not a v1 manifest, wrong in exactly one field, reads
/// as `Corrupt::Manifest` naming that fault, and no bytes are served. The unedited fields read
/// back as a blob. One row per `Shape` arm, except the two arms the damage test above already
/// asserts with too few fields and too few digests: for those, only the too-many side, which a
/// `<` in place of `!=` would let through.
#[test]
fn every_manifest_shape_fault_is_named() {
    let root = photo();
    let read = |payload: &Value| {
        let record = seal(Kind::Blob, &encode(payload).unwrap()).unwrap();
        read_blob(&stored(&[(ROOT_PHOTO, record, 1)]), &root)
    };
    // The control: the unedited fields are a manifest.
    let blob = read(&Value::Map(manifest_fields())).unwrap().unwrap();
    assert_eq!((blob.manifest.size, blob.manifest.chunks()), (12, 3));

    let renamed = {
        let mut m = manifest_fields();
        let size = m.remove(&MapKey::new("size")).unwrap();
        m.insert(MapKey::new("sizes"), size);
        Value::Map(m)
    };
    let digests = |items: Vec<Value>| manifest_with("chunk_sha256", Value::Array(items));
    let d32 = || Value::Bytes(vec![0; 32]);
    let rows: [(&str, Value, &str); 14] = [
        (
            "not a map (01)",
            Value::Integer(Int::from(1_u64)),
            "the payload is not a map",
        ),
        (
            "6 fields",
            manifest_with("extra", Value::Null),
            "not exactly the five manifest fields",
        ),
        ("size renamed", renamed, "a field is missing"),
        (
            "size: -1",
            manifest_with("size", Value::Integer(Int::from(-1_i64))),
            "a length is negative",
        ),
        (
            "size: \"x\"",
            manifest_with("size", Value::Text("x".into())),
            "a length is not an integer",
        ),
        (
            "sha256 31 B",
            manifest_with("sha256", Value::Bytes(vec![0; 31])),
            "a digest is not 32 bytes",
        ),
        (
            "sha256 as text",
            manifest_with("sha256", Value::Text("0".repeat(32))),
            "a digest is not a byte string",
        ),
        (
            "upload 15 B",
            manifest_with("upload", Value::Bytes(vec![0x11; 15])),
            "upload is not 16 bytes",
        ),
        (
            "upload as an integer",
            manifest_with("upload", Value::Integer(Int::from(7_u64))),
            "upload is not a byte string",
        ),
        (
            "chunk_sha256 as a map",
            manifest_with("chunk_sha256", Value::Map(Map::new())),
            "chunk_sha256 is not an array",
        ),
        (
            "one chunk digest 31 B",
            digests(vec![d32(), Value::Bytes(vec![0; 31]), d32()]),
            "a digest is not 32 bytes",
        ),
        (
            "chunk_size: 0",
            manifest_with("chunk_size", Value::Integer(Int::from(0_u64))),
            "chunk_size is outside 1 … MAX_CHUNK",
        ),
        (
            "size = 256 × chunk_size",
            manifest_with("size", Value::Integer(Int::from(256 * 4_u64))),
            "more chunks than MAX_CHUNKS",
        ),
        (
            "one digest too many",
            digests(vec![d32(); 4]),
            "chunk_sha256 does not hold one digest per chunk",
        ),
    ];
    // Every row is read before anything is asserted, so a fault names all the rows it breaks.
    let mut wrong = Vec::new();
    for (what, payload, message) in rows {
        let got = read(&payload);
        let text = got.as_ref().err().map(ToString::to_string);
        if got != corrupt(Corrupt::Manifest(ManifestError::Shape(message)))
            || text != Some(format!("corrupt record: blob manifest: {message}"))
        {
            wrong.push(format!("{what}: {got:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "rows that read wrong:\n{}",
        wrong.join("\n")
    );
}

// ---- limits ----------------------------------------------------------------------------------

/// Limits: a chunk of `MAX_CHUNK` bytes is accepted and one more byte
/// is `TooLarge`; index 254 is accepted and 255 is `TooManyChunks`; a full chunk under a 3,745-byte
/// id compiles and fits the record by `record_len`, and under 3,746 it is `TooLarge` (Node Part 2).
#[test]
fn chunk_limits_sit_exactly_at_the_format_constants() {
    let s = MapSnapshot::new(Generation(1));
    let root = photo();
    let full = vec![0x5a; MAX_CHUNK];
    assert_eq!(MAX_CHUNK, 1_044_480);
    assert_eq!(MAX_CHUNKS, 255);
    assert!(put_chunk(&s, &root, &U1, 0, &full).is_ok());
    assert_eq!(
        put_chunk(&s, &root, &U1, 0, &vec![0x5a; MAX_CHUNK + 1]),
        apply_err(ApplyError::TooLarge {
            limit: SizeLimit::Chunk
        })
    );
    assert!(put_chunk(&s, &root, &U1, 254, b"z").is_ok());
    assert_eq!(
        put_chunk(&s, &root, &U1, 255, b"z"),
        apply_err(ApplyError::TooManyChunks)
    );
    assert_eq!(
        put_chunk(&s, &root, &U1, u32::MAX, b"z"),
        apply_err(ApplyError::TooManyChunks)
    );
    assert_eq!(chunk_count(255, 1), Ok(255));
    assert_eq!(chunk_count(256, 1), Err(ApplyError::TooManyChunks));
    assert_eq!(
        chunk_count(255 * MAX_CHUNK as u64, MAX_CHUNK as u64),
        Ok(255)
    );
    let long = |n: usize| root_key(TenantId(1), AffinityId(1), &vec![b'a'; n]);
    let fits = put_chunk(&s, &long(3745), &U1, 0, &full).expect("a full chunk fits");
    assert!(record_len(fits.conditions.len(), &fits.mutations) <= MAX_ENVELOPE_BYTES);
    assert_eq!(
        put_chunk(&s, &long(3746), &U1, 0, &full),
        apply_err(ApplyError::TooLarge {
            limit: SizeLimit::Write
        })
    );
    assert!(put_chunk(&s, &long(3746), &U1, 0, &full[1..]).is_ok());
}

// ---- kind checks -----------------------------------------------------------------------------

/// Adjacent leftover to S4 P3: a list block or slot record at a blob root read as
/// `KindMismatch`, as if another kind of object were there. Those records are never written at
/// a root key, so every blob path that opens the root names the damage instead.
#[test]
fn a_list_block_or_slot_at_a_blob_root_is_damage_on_every_path() {
    let root = photo();
    for kind in [Kind::ListBlock, Kind::ListSlot] {
        let mut s = MapSnapshot::new(Generation(1));
        s.insert(root.to_bytes(), 4, seal(kind, &h("a0")).unwrap());
        let damage = ValueError::Corrupt(Corrupt::ListRecordAtRoot { found: kind });
        assert_eq!(
            read_blob(&s, &root),
            Err(damage.clone()),
            "read_blob, {kind:?}"
        );
        assert_eq!(
            read_range(&s, &root, 0, 0),
            Err(damage.clone()),
            "read_range, {kind:?}"
        );
        assert_eq!(
            delete_blob(&s, &root, 4).map(|_| ()),
            Err(damage.clone()),
            "delete_blob, {kind:?}"
        );
        assert_eq!(
            publish(
                &s,
                &root,
                Expected::Version(4),
                &U1,
                0,
                4,
                &pin(B3_SHA),
                Generation(1)
            )
            .map(|_| ()),
            Err(damage.clone()),
            "publish, {kind:?}"
        );
    }
}

/// Lead ruling L-R186dm: a list block or slot record at a blob root is damage, not a root, so GC
/// cannot decide reachability from it (ADR-rdb-0014 §8) and refuses. Before the ruling it read as
/// "a non-blob root names no upload" and offered every chunk for deletion. Whatever GC returns is
/// applied, so a batch that deletes anything shows up as a changed kernel.
#[test]
fn gc_refuses_a_list_block_or_slot_at_a_blob_root_and_deletes_nothing() {
    let root = photo();
    let prefix = root.chunk_prefix();
    for kind in [Kind::ListBlock, Kind::ListSlot] {
        let mut k = b1();
        let chunks = k
            .records
            .keys()
            .filter(|key| key.starts_with(&prefix))
            .count();
        assert_eq!(chunks, 3, "B1 has its three chunks, {kind:?}");
        let version = k.records[&root.to_bytes()].0;
        k.records
            .insert(root.to_bytes(), (version, seal(kind, &h("a0")).unwrap()));
        let before = k.clone();
        let got = collect_garbage(&k.snapshot(), &root, u64::MAX);
        if let Ok(compiled) = &got {
            if !compiled.mutations.is_empty() {
                let _ = k.apply(compiled, None);
            }
        }
        assert_eq!(
            k, before,
            "nothing is written; every chunk is still there, {kind:?}"
        );
        assert_eq!(
            got.map(|_| ()),
            corrupt(Corrupt::ListRecordAtRoot { found: kind }),
            "{kind:?}"
        );
    }
}

/// `KindMismatch` both ways: a blob operation on a document or a map names the
/// kind it found, and a document or map operation on a blob names `Blob`.
#[test]
fn blob_and_non_blob_operations_refuse_each_others_objects() {
    let root = photo();
    let doc = seal(Kind::Document, &h("a0")).unwrap();
    let map = {
        let mut m = Map::new();
        m.insert(MapKey::new("keys"), Value::Integer(Int::from(1_u64)));
        m.insert(MapKey::new("count"), Value::Integer(Int::from(0_u64)));
        seal(Kind::Map, &encode(&Value::Map(m)).unwrap()).unwrap()
    };
    for (record, kind) in [(doc, Kind::Document), (map, Kind::Map)] {
        let mut s = MapSnapshot::new(Generation(1));
        s.insert(root.to_bytes(), 1, record);
        let mismatch = ValueError::Apply(ApplyError::KindMismatch { found: kind });
        assert_eq!(
            publish(
                &s,
                &root,
                Expected::Version(1),
                &U1,
                0,
                4,
                &pin(B3_SHA),
                Generation(1)
            ),
            Err(mismatch.clone())
        );
        assert_eq!(delete_blob(&s, &root, 1), Err(mismatch.clone()));
        assert_eq!(read_blob(&s, &root), Err(mismatch.clone()));
        assert_eq!(read_range(&s, &root, 0, 0), Err(mismatch.clone()));
    }
    let s = b1().snapshot();
    let blob = apply_err(ApplyError::KindMismatch { found: Kind::Blob });
    assert_eq!(
        compile(
            &s,
            &root,
            Expected::Version(4),
            &Delta(vec![Op::Replace(Value::Null)])
        ),
        blob
    );
    assert_eq!(
        compile_collection(
            &s,
            &root,
            CollectionKind::Map,
            Expected::Version(4),
            &[ElemOp::Remove(Value::Null)]
        ),
        blob
    );
}
