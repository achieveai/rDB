//! `doc_scenario`'s blob commands (s5-design §3). A separate file, as `coll.rs` is: it shares the
//! store, `apply` and the output of `doc_scenario.rs`.
//!
//! Every blob request carries the snapshot's generation (ADR-rdb-0014 §12): the compiled line
//! prints it as `generation`, and a commit goes through [`Store::apply_at`], which checks it
//! before the conditions, as the kernel's admission check 5 does.

use std::cell::RefCell;
use std::path::Path as FsPath;

use bytes::Bytes;
use rdb_core::contracts::trace::Version;
use rdb_core::{Generation, Namespace, Seq, SnapshotHandle, SnapshotRead};
use rdb_value::blob::{
    collect_garbage, delete_blob, publish, put_chunk, read_blob, read_range, Blob, Upload,
    MAX_CHUNK, MAX_CHUNKS,
};
use rdb_value::envelope::{self, Kind};
use rdb_value::keys::{self, RootKey};
use rdb_value::testing::MapSnapshot;
use rdb_value::{Compiled, Corrupt, Expected, ValueError};
use sha2::{Digest as _, Sha256};

use super::{
    compiled_fields, envelope_fields, hex_input, id_fields, json_str, read_bounded, root_of,
    set_expected, too_large, Failure, Fields, Line, Store,
};

/// `read` prints the bytes as hex up to this many.
const PRINT_LIMIT: usize = 4096;
/// `upload --file`: the largest blob, `MAX_CHUNKS` full chunks.
const MAX_BLOB_INPUT: usize = MAX_CHUNKS * MAX_CHUNK;
/// `chunk --file`: past `MAX_CHUNK`, so the library is what refuses an oversized chunk.
const MAX_CHUNK_INPUT: usize = 1 << 20;

/// The words of one blob command, after `<id>`.
struct Args<'a> {
    rest: &'a [String],
    seen: Vec<String>,
}

impl<'a> Args<'a> {
    fn new(rest: &'a [String]) -> Self {
        Self {
            rest,
            seen: Vec::new(),
        }
    }

    /// The next flag, or `None` at the end. A flag given twice is a usage error.
    fn flag(&mut self) -> Result<Option<String>, Failure> {
        let Some((word, tail)) = self.rest.split_first() else {
            return Ok(None);
        };
        self.rest = tail;
        if self.seen.contains(word) {
            return Err(Failure::usage(format!("{word} given twice")));
        }
        self.seen.push(word.clone());
        Ok(Some(word.clone()))
    }

    /// The value after a flag.
    fn value(&mut self, flag: &str) -> Result<&'a str, Failure> {
        let (value, tail) = self
            .rest
            .split_first()
            .ok_or_else(|| Failure::usage(format!("{flag} needs a value")))?;
        self.rest = tail;
        Ok(value)
    }

    fn number(&mut self, flag: &str) -> Result<u64, Failure> {
        let v = self.value(flag)?;
        v.parse::<u64>()
            .map_err(|_| Failure::usage(format!("{flag} {v:?} is not a u64")))
    }
}

/// `--upload H`: 32 hex digits.
fn upload_arg(text: &str) -> Result<Upload, Failure> {
    let bytes = hex::decode(text).map_err(|e| Failure::usage(format!("--upload: bad hex: {e}")))?;
    Upload::try_from(bytes.as_slice()).map_err(|_| {
        Failure::usage(format!(
            "--upload is 32 hex digits (16 bytes), got {} bytes",
            bytes.len()
        ))
    })
}

/// `--sha256 H`: 64 hex digits.
fn sha256_arg(text: &str) -> Result<[u8; 32], Failure> {
    let bytes = hex::decode(text).map_err(|e| Failure::usage(format!("--sha256: bad hex: {e}")))?;
    <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| {
        Failure::usage(format!(
            "--sha256 is 64 hex digits (32 bytes), got {} bytes",
            bytes.len()
        ))
    })
}

/// The bytes of `--text T`, `--hex H` or `--file F`; `limit` bounds a file.
fn data_arg(flag: &str, value: &str, limit: usize) -> Result<Vec<u8>, Failure> {
    match flag {
        "--text" => Ok(value.as_bytes().to_vec()),
        "--hex" => hex_input(value),
        _ => {
            let cannot = |e: std::io::Error| Failure::usage(format!("cannot read {value:?}: {e}"));
            let source = std::fs::File::open(value).map_err(cannot)?;
            read_bounded(source, limit)
                .map_err(cannot)?
                .ok_or_else(|| too_large(&format!("{value:?}"), limit))
        }
    }
}

/// The `<id>` that starts every blob command, its root key, and the rest.
fn object(rest: &[String]) -> Result<(&String, RootKey, &[String]), Failure> {
    let Some((id, rest)) = rest.split_first() else {
        return Err(Failure::usage("missing <id>"));
    };
    let root = root_of(id).map_err(|e| e.keyed(id))?;
    Ok((id, root, rest))
}

/// Commit `compiled` at `generation` and add `version`, or, for `compile_only`, add
/// `compile_only: true`. Then the compiled fields, with `generation` before them.
fn emit(
    store: &mut Store,
    compiled: &Compiled,
    compile_only: bool,
    generation: Generation,
    mut fields: Fields,
) -> Result<Fields, Failure> {
    if compile_only {
        fields.push(("compile_only", "true".to_owned()));
    } else {
        let version = store
            .apply_at(compiled, generation)
            .map_err(|e| e.with(fields.clone()))?;
        fields.push(("version", version.to_string()));
    }
    fields.push(("generation", generation.0.to_string()));
    fields.extend(compiled_fields(compiled)?);
    Ok(fields)
}

/// `chunk <id> --upload H --index I (--text T | --hex H | --file F) [--compile-only]`.
pub fn chunk_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    let (mut upload, mut index, mut data, mut compile_only) = (None, None, None, false);
    let mut args = Args::new(rest);
    while let Some(flag) = args.flag().map_err(|e| e.keyed(id))? {
        match flag.as_str() {
            "--upload" => upload = Some(upload_arg(args.value(&flag)?).map_err(|e| e.keyed(id))?),
            "--index" => {
                let i = args.number(&flag).map_err(|e| e.keyed(id))?;
                index =
                    Some(u32::try_from(i).map_err(|_| {
                        Failure::usage(format!("--index {i} is not a u32")).keyed(id)
                    })?);
            }
            "--text" | "--hex" | "--file" if data.is_none() => {
                let value = args.value(&flag)?;
                data = Some(data_arg(&flag, value, MAX_CHUNK_INPUT).map_err(|e| e.keyed(id))?);
            }
            "--compile-only" => compile_only = true,
            _ => return Err(chunk_usage().keyed(id)),
        }
    }
    let (Some(upload), Some(index), Some(data)) = (upload, index, data) else {
        return Err(chunk_usage().keyed(id));
    };
    let mut store = Store::load(store_path)?;
    let mut fields = id_fields(id, &root);
    fields.push(("upload", json_str(&hex::encode(upload))));
    fields.push(("index", index.to_string()));
    fields.push(("len", data.len().to_string()));
    let generation = store.snapshot.generation();
    let compiled = put_chunk(&store.snapshot, &root, &upload, index, &data)
        .map_err(|e| Failure::from(e).with(fields.clone()))?;
    if compiled.mutations.is_empty() {
        return Ok(already(&store, &root, "stored", &upload, index, fields));
    }
    emit(&mut store, &compiled, compile_only, generation, fields)
}

fn chunk_usage() -> Failure {
    Failure::usage(
        "chunk takes <id> --upload H --index I (--text T | --hex H | --file F) [--compile-only]",
    )
}

/// A compile with no mutations: `<what>: "already"` and the stored record's version. Never
/// committed: the kernel refuses an empty request.
fn already(
    store: &Store,
    root: &RootKey,
    what: &'static str,
    upload: &Upload,
    index: u32,
    mut fields: Fields,
) -> Fields {
    let key = if what == "stored" {
        keys::chunk_key(root, upload, index)
    } else {
        root.to_bytes()
    };
    fields.push((what, json_str("already")));
    if let Some(v) = store.snapshot.version(Namespace::User, &key) {
        fields.push(("version", v.to_string()));
    }
    fields.push(("committed", "false".to_owned()));
    fields
}

/// What `publish` and `upload` share: `(--absent | --expect V)`, `--upload`, `--chunk-size` and
/// `--serving`.
#[derive(Default)]
struct PublishArgs {
    expected: Option<Expected>,
    upload: Option<Upload>,
    chunk_size: Option<u64>,
    serving: Option<u64>,
}

impl PublishArgs {
    /// Take `flag` if it is one of the shared ones; `false` if it is not.
    fn take(&mut self, flag: &str, args: &mut Args<'_>) -> Result<bool, Failure> {
        match flag {
            "--absent" => set_expected(&mut self.expected, Expected::Absent)?,
            "--expect" => {
                let v = args.number(flag)?;
                set_expected(&mut self.expected, Expected::Version(v))?;
            }
            "--upload" => self.upload = Some(upload_arg(args.value(flag)?)?),
            "--chunk-size" => self.chunk_size = Some(args.number(flag)?),
            "--serving" => self.serving = Some(args.number(flag)?),
            _ => return Ok(false),
        }
        Ok(true)
    }
}

/// `publish <id> (--absent | --expect V) --upload H --size N --chunk-size C --sha256 H
/// [--serving G] [--compile-only]`.
pub fn publish_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    let mut shared = PublishArgs::default();
    let (mut size, mut sha256, mut compile_only) = (None, None, false);
    let mut args = Args::new(rest);
    while let Some(flag) = args.flag().map_err(|e| e.keyed(id))? {
        if shared.take(&flag, &mut args).map_err(|e| e.keyed(id))? {
            continue;
        }
        match flag.as_str() {
            "--size" => size = Some(args.number(&flag).map_err(|e| e.keyed(id))?),
            "--sha256" => sha256 = Some(sha256_arg(args.value(&flag)?).map_err(|e| e.keyed(id))?),
            "--compile-only" => compile_only = true,
            _ => return Err(publish_usage().keyed(id)),
        }
    }
    let (Some(expected), Some(upload), Some(chunk_size), Some(size), Some(sha256)) = (
        shared.expected,
        shared.upload,
        shared.chunk_size,
        size,
        sha256,
    ) else {
        return Err(publish_usage().keyed(id));
    };
    let mut store = Store::load(store_path)?;
    let serving = Generation(shared.serving.unwrap_or(store.snapshot.generation().0));
    let mut fields = id_fields(id, &root);
    fields.push(("upload", json_str(&hex::encode(upload))));
    fields.push(("serving", serving.0.to_string()));
    let generation = store.snapshot.generation();
    let compiled = publish(
        &store.snapshot,
        &root,
        expected,
        &upload,
        size,
        chunk_size,
        &sha256,
        serving,
    )
    .map_err(|e| Failure::from(e).with(fields.clone()))?;
    if compiled.mutations.is_empty() {
        return Ok(already(&store, &root, "published", &upload, 0, fields));
    }
    emit(&mut store, &compiled, compile_only, generation, fields)
}

fn publish_usage() -> Failure {
    Failure::usage(
        "publish takes <id> (--absent | --expect V) --upload H --size N --chunk-size C --sha256 H \
         [--serving G] [--compile-only]",
    )
}

/// `upload <id> (--absent | --expect V) --upload H --chunk-size C (--text T | --hex H | --file F)
/// [--stop-after K] [--serving G]`: one `chunk` per piece, then `publish`, one commit each.
/// `--stop-after K` stops, exit 0, once K commits are made.
pub fn upload_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    let mut shared = PublishArgs::default();
    let (mut data, mut stop_after) = (None, None);
    let mut args = Args::new(rest);
    while let Some(flag) = args.flag().map_err(|e| e.keyed(id))? {
        if shared.take(&flag, &mut args).map_err(|e| e.keyed(id))? {
            continue;
        }
        match flag.as_str() {
            "--text" | "--hex" | "--file" if data.is_none() => {
                let value = args.value(&flag)?;
                data = Some(data_arg(&flag, value, MAX_BLOB_INPUT).map_err(|e| e.keyed(id))?);
            }
            "--stop-after" => stop_after = Some(args.number(&flag).map_err(|e| e.keyed(id))?),
            _ => return Err(upload_usage().keyed(id)),
        }
    }
    let (Some(expected), Some(upload), Some(chunk_size), Some(data)) =
        (shared.expected, shared.upload, shared.chunk_size, data)
    else {
        return Err(upload_usage().keyed(id));
    };
    let mut store = Store::load(store_path)?;
    let serving = Generation(shared.serving.unwrap_or(store.snapshot.generation().0));
    let size = data.len() as u64;
    let sha256: [u8; 32] = Sha256::digest(&data).into();
    let mut fields = id_fields(id, &root);
    fields.push(("upload", json_str(&hex::encode(upload))));
    fields.push(("size", size.to_string()));
    fields.push(("chunk_size", chunk_size.to_string()));
    fields.push(("sha256", json_str(&hex::encode(sha256))));
    fields.push(("serving", serving.0.to_string()));
    // The pieces, cut as publish will count them; a bad chunk size is publish's refusal.
    let pieces: Vec<&[u8]> = match usize::try_from(chunk_size) {
        Ok(c) if c > 0 => data.chunks(c).collect(),
        _ => Vec::new(),
    };
    let mut steps = Vec::new();
    let mut committed = 0_u64;
    let stopped = |steps: &[String], fields: &Fields, committed: u64| -> Fields {
        let mut out = fields.clone();
        out.push(("steps", format!("[{}]", steps.join(","))));
        out.push(("committed", committed.to_string()));
        out.push(("stopped_after", committed.to_string()));
        out
    };
    let fail = |e: Failure, steps: &[String], fields: &Fields| -> Failure {
        let mut out = fields.clone();
        out.push(("steps", format!("[{}]", steps.join(","))));
        let mut e = e;
        out.extend(std::mem::take(&mut e.fields));
        e.with(out)
    };
    for (index, piece) in pieces.iter().enumerate() {
        let index = u32::try_from(index).unwrap_or(u32::MAX);
        let mut step = vec![("op", json_str("chunk")), ("index", index.to_string())];
        let generation = store.snapshot.generation();
        let compiled = put_chunk(&store.snapshot, &root, &upload, index, piece)
            .map_err(|e| fail(Failure::from(e).with(step.clone()), &steps, &fields))?;
        if compiled.mutations.is_empty() {
            let line = already(&store, &root, "stored", &upload, index, step);
            steps.push(render_fields(line));
            continue;
        }
        if stop_after == Some(committed) {
            return Ok(stopped(&steps, &fields, committed));
        }
        step = emit(&mut store, &compiled, false, generation, step)
            .map_err(|e| fail(e, &steps, &fields))?;
        committed += 1;
        steps.push(render_fields(step));
    }
    let step = vec![("op", json_str("publish"))];
    let generation = store.snapshot.generation();
    let compiled = publish(
        &store.snapshot,
        &root,
        expected,
        &upload,
        size,
        chunk_size,
        &sha256,
        serving,
    )
    .map_err(|e| fail(Failure::from(e).with(step.clone()), &steps, &fields))?;
    if compiled.mutations.is_empty() {
        steps.push(render_fields(already(
            &store,
            &root,
            "published",
            &upload,
            0,
            step,
        )));
    } else {
        if stop_after == Some(committed) {
            return Ok(stopped(&steps, &fields, committed));
        }
        let step = emit(&mut store, &compiled, false, generation, step)
            .map_err(|e| fail(e, &steps, &fields))?;
        committed += 1;
        steps.push(render_fields(step));
    }
    fields.push(("steps", format!("[{}]", steps.join(","))));
    fields.push(("committed", committed.to_string()));
    Ok(fields)
}

fn upload_usage() -> Failure {
    Failure::usage(
        "upload takes <id> (--absent | --expect V) --upload H --chunk-size C \
         (--text T | --hex H | --file F) [--stop-after K] [--serving G]",
    )
}

fn render_fields(fields: Fields) -> String {
    let mut line = Line::default();
    line.extend(fields);
    line.render()
}

/// The manifest's fields, for `blob` and `dump`.
fn manifest_fields(blob: &Blob) -> Fields {
    let m = &blob.manifest;
    let digests: Vec<String> = m
        .chunk_sha256
        .iter()
        .map(|d| json_str(&hex::encode(d)))
        .collect();
    vec![
        ("size", m.size.to_string()),
        ("sha256", json_str(&hex::encode(m.sha256))),
        ("upload", json_str(&hex::encode(m.upload))),
        ("chunk_size", m.chunk_size.to_string()),
        ("chunks", m.chunks().to_string()),
        ("chunk_sha256", format!("[{}]", digests.join(","))),
    ]
}

/// `blob <id>`: the manifest, its version and its envelope.
pub fn blob_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    if !rest.is_empty() {
        return Err(Failure::usage("blob takes <id>").keyed(id));
    }
    let store = Store::load(store_path)?;
    let mut fields = id_fields(id, &root);
    let blob = read_blob(&store.snapshot, &root)
        .map_err(|e| Failure::from(e).with(fields.clone()))?
        .ok_or_else(|| {
            Failure::from(rdb_value::delta::ApplyError::ObjectAbsent).with(fields.clone())
        })?;
    fields.push(("version", blob.version.to_string()));
    fields.extend(manifest_fields(&blob));
    let raw = store
        .snapshot
        .get(Namespace::User, root.as_bytes())
        .ok_or_else(|| Failure::store("record vanished between two reads"))?;
    fields.extend(envelope_fields(&raw)?);
    Ok(fields)
}

/// A snapshot that records every key read with `get`, so `read` can show which chunks it read.
struct Counting<'a> {
    inner: &'a MapSnapshot,
    gets: RefCell<Vec<Bytes>>,
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
        self.gets.borrow_mut().push(Bytes::copy_from_slice(key));
        self.inner.get(ns, key)
    }
    fn version(&self, ns: Namespace, key: &[u8]) -> Option<Version> {
        self.inner.version(ns, key)
    }
    fn scan(&self, ns: Namespace, from: &[u8], limit: usize) -> Vec<(Bytes, Bytes)> {
        self.inner.scan(ns, from, limit)
    }
}

/// `read <id> [--offset O --len L] [--out FILE]`: `len`, `chunks_read`, the bytes as hex up to
/// 4 KiB (and as `text` when they are UTF-8), and with `--out` the bytes in FILE. A refusal
/// writes no file.
pub fn read_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    let (mut offset, mut len, mut out) = (None, None, None);
    let mut args = Args::new(rest);
    while let Some(flag) = args.flag().map_err(|e| e.keyed(id))? {
        match flag.as_str() {
            "--offset" => offset = Some(args.number(&flag).map_err(|e| e.keyed(id))?),
            "--len" => len = Some(args.number(&flag).map_err(|e| e.keyed(id))?),
            "--out" => out = Some(args.value(&flag)?.to_owned()),
            _ => return Err(read_usage().keyed(id)),
        }
    }
    let store = Store::load(store_path)?;
    let counting = Counting {
        inner: &store.snapshot,
        gets: RefCell::new(Vec::new()),
    };
    let mut fields = id_fields(id, &root);
    let chunks_read = |counting: &Counting<'_>| -> String {
        let read: Vec<String> = counting
            .gets
            .borrow()
            .iter()
            .filter_map(|k| keys::parse(k).ok()?.chunk)
            .map(|(_, index)| index.to_string())
            .collect();
        format!("[{}]", read.join(","))
    };
    let refused = |e: ValueError, fields: &Fields, counting: &Counting<'_>| -> Failure {
        let mut fields = fields.clone();
        fields.push(("chunks_read", chunks_read(counting)));
        Failure::from(e).with(fields)
    };
    let (offset, len) = match (offset, len) {
        (Some(o), Some(l)) => (o, l),
        (None, None) => {
            let blob = read_blob(&counting, &root)
                .map_err(|e| refused(e, &fields, &counting))?
                .ok_or_else(|| {
                    Failure::from(rdb_value::delta::ApplyError::ObjectAbsent).with(fields.clone())
                })?;
            (0, blob.manifest.size)
        }
        _ => return Err(read_usage().keyed(id)),
    };
    fields.push(("offset", offset.to_string()));
    fields.push(("len", len.to_string()));
    let bytes =
        read_range(&counting, &root, offset, len).map_err(|e| refused(e, &fields, &counting))?;
    fields.push(("chunks_read", chunks_read(&counting)));
    if bytes.len() <= PRINT_LIMIT {
        fields.push(("hex", json_str(&hex::encode(&bytes))));
        if let Ok(text) = std::str::from_utf8(&bytes) {
            fields.push(("text", json_str(text)));
        }
    }
    if let Some(out) = out {
        std::fs::write(&out, &bytes)
            .map_err(|e| Failure::store(format!("cannot write {out:?}: {e}")))?;
        fields.push(("out", json_str(&out)));
    }
    Ok(fields)
}

fn read_usage() -> Failure {
    Failure::usage("read takes <id> [--offset O --len L] [--out FILE]")
}

/// `blob-delete <id> --expect V [--compile-only]`.
pub fn delete_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    let (mut version, mut compile_only) = (None, false);
    let mut args = Args::new(rest);
    while let Some(flag) = args.flag().map_err(|e| e.keyed(id))? {
        match flag.as_str() {
            "--expect" => version = Some(args.number(&flag).map_err(|e| e.keyed(id))?),
            "--compile-only" => compile_only = true,
            _ => {
                return Err(
                    Failure::usage("blob-delete takes <id> --expect V [--compile-only]").keyed(id),
                )
            }
        }
    }
    let version =
        version.ok_or_else(|| Failure::usage("blob-delete needs --expect V").keyed(id))?;
    let mut store = Store::load(store_path)?;
    let fields = id_fields(id, &root);
    let generation = store.snapshot.generation();
    let compiled = delete_blob(&store.snapshot, &root, version)
        .map_err(|e| Failure::from(e).with(fields.clone()))?;
    emit(&mut store, &compiled, compile_only, generation, fields)
}

/// `gc <id> --floor F [--compile-only]`: one batch, with its `record_len`. A batch with nothing
/// to delete is not committed.
pub fn gc_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    let (mut floor, mut compile_only) = (None, false);
    let mut args = Args::new(rest);
    while let Some(flag) = args.flag().map_err(|e| e.keyed(id))? {
        match flag.as_str() {
            "--floor" => floor = Some(args.number(&flag).map_err(|e| e.keyed(id))?),
            "--compile-only" => compile_only = true,
            _ => return Err(Failure::usage("gc takes <id> --floor F [--compile-only]").keyed(id)),
        }
    }
    let floor = floor.ok_or_else(|| Failure::usage("gc needs --floor F").keyed(id))?;
    let mut store = Store::load(store_path)?;
    let mut fields = id_fields(id, &root);
    fields.push(("floor", floor.to_string()));
    let generation = store.snapshot.generation();
    let compiled = collect_garbage(&store.snapshot, &root, floor)
        .map_err(|e| Failure::from(e).with(fields.clone()))?;
    fields.push(("deletes", compiled.mutations.len().to_string()));
    if compiled.mutations.is_empty() {
        fields.push(("committed", "false".to_owned()));
        fields.extend(compiled_fields(&compiled)?);
        return Ok(fields);
    }
    fields.push((
        "record_len",
        rdb_core::transaction::record_len(compiled.conditions.len(), &compiled.mutations)
            .to_string(),
    ));
    emit(&mut store, &compiled, compile_only, generation, fields)
}

// ---------------------------------------------------------------------------------------------
// `dump`, `apply` and the compile line
// ---------------------------------------------------------------------------------------------

/// `dump` for a blob root: the manifest, read through the library.
pub fn dump_root(snapshot: &MapSnapshot, root: &RootKey, line: &mut Line) -> Result<(), Failure> {
    let blob = read_blob(snapshot, root)?
        .ok_or_else(|| Failure::store("record vanished between two reads"))?;
    line.extend(manifest_fields(&blob));
    Ok(())
}

/// `dump` for a chunk record: its upload and index from the key, then its envelope. A record of
/// another kind is refused by name.
pub fn dump_chunk(upload: &Upload, index: u32, raw: &[u8], line: &mut Line) -> Result<(), Failure> {
    line.str("upload", &hex::encode(upload));
    line.raw("index", index.to_string());
    open_chunk(index, raw)?;
    line.extend(envelope_fields(raw)?);
    Ok(())
}

/// A chunk record as written: an envelope of kind `Chunk`.
pub fn open_chunk(index: u32, raw: &[u8]) -> Result<(), Failure> {
    let opened = envelope::open(raw)
        .map_err(|error| Failure::from(ValueError::Corrupt(Corrupt::Chunk { index, error })))?;
    if opened.kind != Kind::Chunk {
        return Err(Failure::from(ValueError::Corrupt(Corrupt::ChunkMismatch {
            index,
        })));
    }
    Ok(())
}

/// `generation` from a compile line, if it has one. Blob compiles always print it.
pub fn line_generation(json: &serde_json::Value) -> Result<Option<Generation>, String> {
    match &json["generation"] {
        serde_json::Value::Null => Ok(None),
        v => v
            .as_u64()
            .map(|g| Some(Generation(g)))
            .ok_or_else(|| "generation must be a u64".to_owned()),
    }
}
