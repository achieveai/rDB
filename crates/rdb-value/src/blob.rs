//! Large blobs: chunk records, one manifest root, publish, digest verify and reachability GC
//! (ADR-rdb-0014).
//!
//! - A chunk, at [`chunk_key`], is `seal(Kind::Chunk, bytes)` (ADR-rdb-0014 §2).
//! - The root, at the object's [`RootKey`], is `seal(Kind::Blob, manifest)`: canonical CBOR of the
//!   five fields of [`Manifest`] (ADR-rdb-0014 §2).
//! - [`put_chunk`] writes one chunk; [`publish`] verifies an upload and writes the root;
//!   [`read_blob`] and [`read_range`] read; [`delete_blob`] and [`collect_garbage`] remove.
//!
//! Every compile here returns a [`Compiled`]; one with no mutations means "already stored" or
//! "already published" and is never submitted. The caller builds the request with
//! `expected_generation: Some(snapshot.generation())` (ADR-rdb-0014 §12).

use bytes::Bytes;
use rdb_core::replication::append::MAX_ENVELOPE_BYTES;
use rdb_core::transaction::admission::{MAX_CONDITIONS, MAX_REQUEST_MUTATIONS};
use rdb_core::transaction::record_len;
use rdb_core::{Condition, Generation, Mutation, Namespace, SnapshotRead};
use sha2::{Digest as _, Sha256};

use crate::cbor::{decode, encode};
use crate::compile::{
    record, refuse_list_record_at_root, Compiled, Corrupt, Expected, ManifestError, ValueError,
};
use crate::delta::{ApplyError, SizeLimit};
use crate::envelope::{open, seal, Kind, Opened};
use crate::keys::{chunk_key, KeyError, RootKey, CHUNK_TAIL_LEN, UPLOAD_LEN};
use crate::value::{Int, Map, MapKey, Value};

/// The largest chunk, in bytes: a manifest-v1 format constant (ADR-rdb-0014 §4).
pub const MAX_CHUNK: usize = 1_044_480;
/// The most chunks one blob has: a manifest-v1 format constant (ADR-rdb-0014 §4).
pub const MAX_CHUNKS: usize = 255;

// ADR-rdb-0014 §4: publish carries one condition per chunk plus a create's `Absent{root}`.
const _: () = assert!(MAX_CHUNKS < MAX_CONDITIONS);
// ADR-rdb-0014 §4: a full chunk with an empty id fits one request. `MAX_CHUNK` is 1 MiB − 4 KiB;
// the 4 KiB holds the request's framing, the condition, the envelope header and the key. The
// exact measure, `record_len`, is applied by `put_chunk` on every write.
const _: () = assert!(MAX_CHUNK + 4096 == MAX_ENVELOPE_BYTES);

/// An upload id (ADR-rdb-0014 §1). Chosen by the caller.
pub type Upload = [u8; UPLOAD_LEN];

/// A blob's manifest, the decoded payload of its root record (ADR-rdb-0014 §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    /// The blob's length in bytes.
    pub size: u64,
    /// SHA-256 of the whole blob.
    pub sha256: [u8; 32],
    /// The upload whose chunks this manifest names.
    pub upload: Upload,
    /// Every chunk's length, except the last.
    pub chunk_size: u64,
    /// SHA-256 of chunk `i`'s bytes, at position `i`.
    pub chunk_sha256: Vec<[u8; 32]>,
}

/// A stored blob's root, read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Blob {
    /// The root record's storage version.
    pub version: u64,
    /// The manifest.
    pub manifest: Manifest,
}

/// `n = ceil(size / chunk_size)`, computed without overflow (ADR-rdb-0014 §6 check 1).
///
/// # Errors
/// [`ApplyError::InvalidChunkSize`] when `chunk_size` is outside `1 … MAX_CHUNK`;
/// [`ApplyError::TooManyChunks`] when `n` is over [`MAX_CHUNKS`].
pub fn chunk_count(size: u64, chunk_size: u64) -> Result<u32, ApplyError> {
    if chunk_size == 0 || chunk_size > MAX_CHUNK as u64 {
        return Err(ApplyError::InvalidChunkSize { found: chunk_size });
    }
    // `div_ceil` is `q + (r > 0)`: it never forms `size + chunk_size - 1`, so it cannot overflow.
    let n = size.div_ceil(chunk_size);
    if n > MAX_CHUNKS as u64 {
        return Err(ApplyError::TooManyChunks);
    }
    Ok(u32::try_from(n).expect("at most MAX_CHUNKS"))
}

/// Chunk `index`'s length in a blob of `size` bytes cut at `chunk_size`, `n` chunks.
fn chunk_len(size: u64, chunk_size: u64, n: u32, index: u32) -> u64 {
    if index + 1 < n {
        chunk_size
    } else {
        size - u64::from(n - 1) * chunk_size
    }
}

impl Manifest {
    /// The number of chunks, `n`.
    #[must_use]
    pub fn chunks(&self) -> u32 {
        u32::try_from(self.chunk_sha256.len()).expect("at most MAX_CHUNKS")
    }

    /// Chunk `index`'s length.
    #[must_use]
    pub fn chunk_len(&self, index: u32) -> u64 {
        chunk_len(self.size, self.chunk_size, self.chunks(), index)
    }

    /// The canonical CBOR payload (ADR-rdb-0014 §2).
    fn encode(&self) -> Vec<u8> {
        let mut map = Map::new();
        map.insert(MapKey::new("size"), Value::Integer(Int::from(self.size)));
        map.insert(MapKey::new("sha256"), Value::Bytes(self.sha256.to_vec()));
        map.insert(MapKey::new("upload"), Value::Bytes(self.upload.to_vec()));
        map.insert(
            MapKey::new("chunk_size"),
            Value::Integer(Int::from(self.chunk_size)),
        );
        map.insert(
            MapKey::new("chunk_sha256"),
            Value::Array(
                self.chunk_sha256
                    .iter()
                    .map(|d| Value::Bytes(d.to_vec()))
                    .collect(),
            ),
        );
        encode(&Value::Map(map)).expect("a manifest is at most 8,777 bytes")
    }

    /// The strict reading of ADR-rdb-0014 §2: the strict decoder, then exactly the five fields,
    /// their types and lengths, and the format limits.
    fn decode(payload: &[u8]) -> Result<Self, ManifestError> {
        let shape = ManifestError::Shape;
        let Value::Map(fields) = decode(payload).map_err(ManifestError::Codec)? else {
            return Err(shape("the payload is not a map"));
        };
        if fields.len() != 5 {
            return Err(shape("not exactly the five manifest fields"));
        }
        let field = |name: &str| {
            fields
                .get(&MapKey::new(name))
                .ok_or(shape("a field is missing"))
        };
        let uint = |name: &str| match field(name)? {
            Value::Integer(i) => u64::try_from(i.get()).map_err(|_| shape("a length is negative")),
            _ => Err(shape("a length is not an integer")),
        };
        let digest = |v: &Value| match v {
            Value::Bytes(b) => {
                <[u8; 32]>::try_from(b.as_slice()).map_err(|_| shape("a digest is not 32 bytes"))
            }
            _ => Err(shape("a digest is not a byte string")),
        };
        let size = uint("size")?;
        let chunk_size = uint("chunk_size")?;
        let sha256 = digest(field("sha256")?)?;
        let upload = match field("upload")? {
            Value::Bytes(b) => {
                Upload::try_from(b.as_slice()).map_err(|_| shape("upload is not 16 bytes"))?
            }
            _ => return Err(shape("upload is not a byte string")),
        };
        let Value::Array(items) = field("chunk_sha256")? else {
            return Err(shape("chunk_sha256 is not an array"));
        };
        let chunk_sha256 = items.iter().map(digest).collect::<Result<Vec<_>, _>>()?;
        let n = chunk_count(size, chunk_size).map_err(|e| match e {
            ApplyError::InvalidChunkSize { .. } => shape("chunk_size is outside 1 … MAX_CHUNK"),
            _ => shape("more chunks than MAX_CHUNKS"),
        })?;
        if chunk_sha256.len() != n as usize {
            return Err(shape("chunk_sha256 does not hold one digest per chunk"));
        }
        Ok(Self {
            size,
            sha256,
            upload,
            chunk_size,
            chunk_sha256,
        })
    }
}

/// Open a root record as a blob.
fn open_blob(version: u64, bytes: &[u8]) -> Result<Blob, ValueError> {
    let opened = open(bytes).map_err(|e| ValueError::Corrupt(Corrupt::Envelope(e)))?;
    refuse_list_record_at_root(opened.kind)?;
    if opened.kind != Kind::Blob {
        return Err(ApplyError::KindMismatch { found: opened.kind }.into());
    }
    let manifest =
        Manifest::decode(opened.payload).map_err(|e| ValueError::Corrupt(Corrupt::Manifest(e)))?;
    Ok(Blob { version, manifest })
}

/// Open a stored chunk record. A record of another kind is [`Corrupt::ChunkMismatch`].
fn open_chunk(index: u32, bytes: &[u8]) -> Result<Opened<'_>, ValueError> {
    let opened =
        open(bytes).map_err(|error| ValueError::Corrupt(Corrupt::Chunk { index, error }))?;
    if opened.kind != Kind::Chunk {
        return Err(ValueError::Corrupt(Corrupt::ChunkMismatch { index }));
    }
    Ok(opened)
}

/// The blob at `root`, or `None` when there is no root record (ADR-rdb-0014 §7).
///
/// # Errors
/// [`ApplyError::KindMismatch`] when the object is not a blob; [`ValueError::Corrupt`] when the
/// root does not open, is a list block or slot record, or its manifest does not decode.
pub fn read_blob(snapshot: &dyn SnapshotRead, root: &RootKey) -> Result<Option<Blob>, ValueError> {
    record(snapshot, root.as_bytes())?
        .map(|(version, bytes)| open_blob(version, &bytes))
        .transpose()
}

/// Bytes `offset … offset + len` of the blob at `root`, reading only the chunks that intersect
/// them, from this one snapshot (ADR-rdb-0014 §7). Any failure returns no bytes.
///
/// # Errors
/// [`ApplyError::ObjectAbsent`], [`ApplyError::KindMismatch`], [`ApplyError::RangeInvalid`], or
/// [`ValueError::Corrupt`] for the root or for a chunk read ([`Corrupt::ChunkMissing`],
/// [`Corrupt::Chunk`], [`Corrupt::ChunkMismatch`]).
pub fn read_range(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    offset: u64,
    len: u64,
) -> Result<Bytes, ValueError> {
    let blob = read_blob(snapshot, root)?.ok_or(ApplyError::ObjectAbsent)?;
    let m = &blob.manifest;
    let end = offset
        .checked_add(len)
        .filter(|end| *end <= m.size)
        .ok_or(ApplyError::RangeInvalid { size: m.size })?;
    if len == 0 {
        return Ok(Bytes::new());
    }
    let first = u32::try_from(offset / m.chunk_size).expect("below n");
    let last = u32::try_from((end - 1) / m.chunk_size).expect("below n");
    let mut out = Vec::with_capacity(usize::try_from(len).expect("a blob fits memory"));
    for index in first..=last {
        let key = chunk_key(root, &m.upload, index);
        let (_, bytes) =
            record(snapshot, &key)?.ok_or(ValueError::Corrupt(Corrupt::ChunkMissing { index }))?;
        let opened = open_chunk(index, &bytes)?;
        let digest: [u8; 32] = Sha256::digest(opened.payload).into();
        if opened.payload.len() as u64 != m.chunk_len(index)
            || digest != m.chunk_sha256[index as usize]
        {
            return Err(ValueError::Corrupt(Corrupt::ChunkMismatch { index }));
        }
        let start = u64::from(index) * m.chunk_size;
        let from = usize::try_from(offset.saturating_sub(start)).expect("below chunk_size");
        let to = usize::try_from(end.min(start + m.chunk_len(index)) - start)
            .expect("at most chunk_size");
        out.extend_from_slice(&opened.payload[from..to]);
    }
    Ok(Bytes::from(out))
}

/// Compile one chunk write (ADR-rdb-0014 §5): a `Put` with `Absent{chunk}`; or no mutations when
/// the same bytes are already stored.
///
/// # Errors
/// [`ApplyError::TooManyChunks`] for `index ≥ MAX_CHUNKS`; [`ApplyError::TooLarge`] for bytes
/// over [`MAX_CHUNK`] or a request over `MAX_ENVELOPE_BYTES`; [`ApplyError::ChunkConflict`] when
/// other bytes are stored; [`ValueError::Corrupt`] when the stored chunk does not open.
pub fn put_chunk(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    upload: &Upload,
    index: u32,
    bytes: &[u8],
) -> Result<Compiled, ValueError> {
    if index as usize >= MAX_CHUNKS {
        return Err(ApplyError::TooManyChunks.into());
    }
    if bytes.len() > MAX_CHUNK {
        return Err(ApplyError::TooLarge {
            limit: SizeLimit::Chunk,
        }
        .into());
    }
    let key = chunk_key(root, upload, index);
    if let Some((_, stored)) = record(snapshot, &key)? {
        let opened = open_chunk(index, &stored)?;
        if opened.payload == bytes {
            return Ok(Compiled {
                mutations: Vec::new(),
                conditions: Vec::new(),
            });
        }
        return Err(ApplyError::ChunkConflict { index }.into());
    }
    let value = seal(Kind::Chunk, bytes).expect("MAX_CHUNK is below MAX_PAYLOAD");
    let conditions = vec![Condition::Absent { key: key.clone() }];
    let mutations = vec![Mutation::Put {
        key,
        value,
        expected_version: None,
    }];
    within_write_limit(conditions.len(), &mutations)?;
    Ok(Compiled {
        mutations,
        conditions,
    })
}

/// The kernel's own measure of the record a request ships (ADR-rdb-0014 §4).
fn within_write_limit(conditions: usize, mutations: &[Mutation]) -> Result<(), ApplyError> {
    if record_len(conditions, mutations) > MAX_ENVELOPE_BYTES {
        return Err(ApplyError::TooLarge {
            limit: SizeLimit::Write,
        });
    }
    Ok(())
}

/// The stored root at `root`, which must be a blob at `version` (ADR-rdb-0014 §6 check 2).
fn existing_blob_root(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    version: u64,
) -> Result<(), ValueError> {
    let (found, bytes) = record(snapshot, root.as_bytes())?.ok_or(ApplyError::ObjectAbsent)?;
    let opened = open(&bytes).map_err(|e| ValueError::Corrupt(Corrupt::Envelope(e)))?;
    refuse_list_record_at_root(opened.kind)?;
    if opened.kind != Kind::Blob {
        return Err(ApplyError::KindMismatch { found: opened.kind }.into());
    }
    if found != version {
        return Err(ApplyError::VersionConflict {
            expected: version,
            found,
        }
        .into());
    }
    Ok(())
}

/// Compile the publication of `upload` as the blob at `root` (ADR-rdb-0014 §6): checks 0–5 in
/// order, then one root `Put` with its conditions. Nothing is written on a refusal.
///
/// `serving` is the generation of the kernel instance that will receive the request; check 0
/// answers only when the snapshot is of that generation.
///
/// # Errors
/// [`ApplyError::InvalidChunkSize`], [`ApplyError::TooManyChunks`], [`ApplyError::ObjectAbsent`],
/// [`ApplyError::KindMismatch`], [`ApplyError::VersionConflict`], [`ApplyError::ChunkMissing`],
/// [`ApplyError::ChunkLength`], [`ApplyError::ExtraChunk`], [`ApplyError::BlobDigestMismatch`],
/// [`ApplyError::TooLarge`], or [`ValueError::Corrupt`].
// The signature is ADR-rdb-0014 §6's.
#[allow(clippy::too_many_arguments)]
pub fn publish(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    expected: Expected,
    upload: &Upload,
    size: u64,
    chunk_size: u64,
    sha256: &[u8; 32],
    serving: Generation,
) -> Result<Compiled, ValueError> {
    // Check 0.
    if snapshot.generation() == serving {
        if let Some((version, bytes)) = record(snapshot, root.as_bytes())? {
            let opened = open(&bytes).map_err(|e| ValueError::Corrupt(Corrupt::Envelope(e)))?;
            if opened.kind == Kind::Blob {
                let m = open_blob(version, &bytes)?.manifest;
                if m.upload == *upload
                    && m.size == size
                    && m.chunk_size == chunk_size
                    && m.sha256 == *sha256
                {
                    return Ok(Compiled {
                        mutations: Vec::new(),
                        conditions: Vec::new(),
                    });
                }
            }
        }
    }
    // Check 1.
    let n = chunk_count(size, chunk_size)?;
    // Check 2.
    if let Expected::Version(v) = expected {
        existing_blob_root(snapshot, root, v)?;
    }
    // Check 3, with check 5's hash built on the way.
    let mut whole = Sha256::new();
    let mut chunk_sha256 = Vec::with_capacity(n as usize);
    let mut chunk_conditions = Vec::with_capacity(n as usize);
    for index in 0..n {
        let key = chunk_key(root, upload, index);
        let (version, bytes) = record(snapshot, &key)?.ok_or(ApplyError::ChunkMissing { index })?;
        let opened = open_chunk(index, &bytes)?;
        let expected_len = chunk_len(size, chunk_size, n, index);
        let found = opened.payload.len() as u64;
        if found != expected_len {
            return Err(ApplyError::ChunkLength {
                index,
                expected: expected_len,
                found,
            }
            .into());
        }
        whole.update(opened.payload);
        chunk_sha256.push(Sha256::digest(opened.payload).into());
        chunk_conditions.push(Condition::VersionEquals { key, version });
    }
    // Check 4.
    if record(snapshot, &chunk_key(root, upload, n))?.is_some() {
        return Err(ApplyError::ExtraChunk { index: n }.into());
    }
    // Check 5.
    if <[u8; 32]>::from(whole.finalize()) != *sha256 {
        return Err(ApplyError::BlobDigestMismatch.into());
    }
    let manifest = Manifest {
        size,
        sha256: *sha256,
        upload: *upload,
        chunk_size,
        chunk_sha256,
    };
    let value = seal(Kind::Blob, &manifest.encode()).expect("a manifest is at most 8,777 bytes");
    let (expected_version, mut conditions) = match expected {
        Expected::Absent => (
            None,
            vec![Condition::Absent {
                key: root.to_bytes(),
            }],
        ),
        Expected::Version(v) => (Some(v), Vec::new()),
    };
    conditions.extend(chunk_conditions);
    let mutations = vec![Mutation::Put {
        key: root.to_bytes(),
        value,
        expected_version,
    }];
    within_write_limit(conditions.len(), &mutations)?;
    Ok(Compiled {
        mutations,
        conditions,
    })
}

/// Compile the deletion of the blob at `root`, at `version` (ADR-rdb-0014 §8). Its chunks are
/// left for [`collect_garbage`].
///
/// # Errors
/// [`ApplyError::ObjectAbsent`], [`ApplyError::KindMismatch`], [`ApplyError::VersionConflict`],
/// or [`ValueError::Corrupt`] when the root does not open.
pub fn delete_blob(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    version: u64,
) -> Result<Compiled, ValueError> {
    existing_blob_root(snapshot, root, version)?;
    Ok(Compiled {
        mutations: vec![Mutation::Delete {
            key: root.to_bytes(),
            expected_version: Some(version),
        }],
        conditions: Vec::new(),
    })
}

/// Chunks scanned per page by [`collect_garbage`].
const SCAN_PAGE: usize = 256;

/// Compile one batch of reachability GC for the object at `root` (ADR-rdb-0014 §8): a `Delete` of
/// every chunk that is unreachable and at a version ≤ `floor`, in key order, each with its
/// version, guarded by the root as read. A batch stops at `MAX_REQUEST_MUTATIONS` deletes or at
/// the record size limit. No mutations means nothing is left to collect.
///
/// # Errors
/// [`ValueError::Corrupt`] when the root does not open or its manifest does not decode, or a
/// chunk key's tail is not 20 bytes ([`KeyError::ChunkTail`]). Nothing is deleted then.
pub fn collect_garbage(
    snapshot: &dyn SnapshotRead,
    root: &RootKey,
    floor: u64,
) -> Result<Compiled, ValueError> {
    let (guard, reachable) = match record(snapshot, root.as_bytes())? {
        None => (
            Condition::Absent {
                key: root.to_bytes(),
            },
            None,
        ),
        Some((version, bytes)) => {
            let opened = open(&bytes).map_err(|e| ValueError::Corrupt(Corrupt::Envelope(e)))?;
            let reachable = if opened.kind == Kind::Blob {
                let m = open_blob(version, &bytes)?.manifest;
                Some((m.upload, m.chunks()))
            } else {
                None
            };
            (
                Condition::VersionEquals {
                    key: root.to_bytes(),
                    version,
                },
                reachable,
            )
        }
    };
    // Every chunk key is read first, so a bad tail anywhere refuses the whole batch.
    let prefix = root.chunk_prefix();
    let mut garbage = Vec::new();
    let mut from = prefix.clone();
    loop {
        let page = snapshot.scan(Namespace::User, &from, SCAN_PAGE);
        let full = page.len() == SCAN_PAGE;
        let mut last = None;
        for (key, _) in page {
            if !key.starts_with(&prefix) {
                last = None;
                break;
            }
            let tail = &key[prefix.len()..];
            let tail: &[u8; CHUNK_TAIL_LEN] = tail.try_into().map_err(|_| {
                ValueError::Corrupt(Corrupt::Key(KeyError::ChunkTail { len: tail.len() }))
            })?;
            let (upload, index) = tail.split_at(UPLOAD_LEN);
            let index = u32::from_be_bytes(index.try_into().expect("4 bytes"));
            let version = snapshot
                .version(Namespace::User, &key)
                .ok_or(ValueError::Corrupt(Corrupt::VersionWithoutValue))?;
            let live = reachable.is_some_and(|(u, n): (Upload, u32)| u == upload && index < n);
            if !live && version <= floor {
                garbage.push((key.clone(), version));
            }
            last = Some(key);
        }
        match last {
            Some(key) if full => {
                from = key.to_vec();
                from.push(0x00);
            }
            _ => break,
        }
    }
    let conditions = vec![guard];
    let mut mutations = Vec::new();
    for (key, version) in garbage {
        if mutations.len() == MAX_REQUEST_MUTATIONS {
            break;
        }
        mutations.push(Mutation::Delete {
            key,
            expected_version: Some(version),
        });
        if record_len(conditions.len(), &mutations) > MAX_ENVELOPE_BYTES {
            mutations.pop();
            break;
        }
    }
    if mutations.is_empty() {
        return Ok(Compiled {
            mutations,
            conditions: Vec::new(),
        });
    }
    Ok(Compiled {
        mutations,
        conditions,
    })
}
