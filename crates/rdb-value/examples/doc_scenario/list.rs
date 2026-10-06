//! `doc_scenario`'s list commands (ADR-rdb-0016). A separate file for size only: it shares the
//! store, `apply` and the output of `doc_scenario.rs`.
//!
//! A list write always commits through `apply_at` with the generation its compile returned, and
//! its compile line carries that generation, so `apply` fences it too (ADR-rdb-0016 §8).

use std::path::Path as FsPath;

use rdb_core::transaction::record_len;
use rdb_core::{Generation, Namespace, SnapshotRead};
use rdb_value::cbor;
use rdb_value::envelope::{self, Kind};
use rdb_value::keys::{block_key, RootKey, Sub, LIST_SLOTS};
use rdb_value::list::{compile_list, create_list, drop_list, items, list, ListOp, Start, Token};
use rdb_value::testing::{CountingSnapshot, MapSnapshot};
use rdb_value::value::{MapKey, Value};
use rdb_value::{Corrupt, Expected, ValueError};

use super::blob::emit;
use super::coll::{element_arg, parse_version};
use super::{
    envelope_fields, id_fields, json_str, render, root_of, set_expected, Failure, Fields, Line,
    Store,
};

/// `items` lists this many when `--limit` is not given.
const DEFAULT_LIMIT: usize = 1000;

/// A list id as printed: 32 hex digits.
fn show(id: u128) -> String {
    format!("{id:032x}")
}

/// The `<id>` that starts every list command, its root key, and the rest.
fn object(rest: &[String]) -> Result<(&String, RootKey, &[String]), Failure> {
    let Some((id, rest)) = rest.split_first() else {
        return Err(Failure::usage("missing <id>"));
    };
    let root = root_of(id).map_err(|e| e.keyed(id))?;
    Ok((id, root, rest))
}

/// `list <id> (--absent [--records] | --expect V) [--compile-only] (push J | insert P J |
/// remove P | replace P J | move P Q)...`: compile the ops at the store's `block_max`, then
/// commit at the compile's generation, or print with `--compile-only`. `--records` creates a
/// list whose every item keeps its own record. Prints the minted `ids` in op order and
/// `written`, the kernel's `record_len` of the request (ADR-rdb-0016 §Verification).
pub fn write_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    let body = parse_ops(rest).map_err(|e| e.keyed(id))?;
    let mut fields = id_fields(id, &root);
    let mut store = Store::load(store_path)?;
    let block_max = store.block_max();
    let compiled = match body.expected {
        Expected::Absent => create_list(&store.snapshot, &root, body.records, block_max, &body.ops),
        expected => compile_list(&store.snapshot, &root, expected, block_max, &body.ops),
    }
    .map_err(|e| Failure::from(e).with(fields.clone()))?;
    let ids: Vec<String> = compiled.ids().iter().map(|i| json_str(&show(*i))).collect();
    fields.push(("ids", format!("[{}]", ids.join(","))));
    let request = compiled.compiled();
    fields.push((
        "written",
        record_len(request.conditions.len(), &request.mutations).to_string(),
    ));
    emit(
        &mut store,
        compiled.compiled(),
        body.compile_only,
        compiled.generation(),
        fields,
    )
}

/// `drop <id> --expect V [--compile-only]` for a list.
pub fn drop_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, mut rest) = object(rest)?;
    let (mut version, mut compile_only) = (None, false);
    while let Some((word, tail)) = rest.split_first() {
        rest = tail;
        match (word.as_str(), rest.split_first()) {
            ("--expect", Some((v, tail))) if version.is_none() => {
                rest = tail;
                version = Some(parse_version(v).map_err(|e| e.keyed(id))?);
            }
            ("--compile-only", _) if !compile_only => compile_only = true,
            _ => {
                return Err(Failure::usage("drop takes <id> --expect V [--compile-only]").keyed(id))
            }
        }
    }
    let version = version.ok_or_else(|| Failure::usage("drop needs --expect V").keyed(id))?;
    let fields = id_fields(id, &root);
    let mut store = Store::load(store_path)?;
    let compiled = drop_list(&store.snapshot, &root, version)
        .map_err(|e| Failure::from(e).with(fields.clone()))?;
    emit(
        &mut store,
        compiled.compiled(),
        compile_only,
        compiled.generation(),
        fields,
    )
}

/// `items <id> [--from P | --token G:V:P] [--limit N]`: the list's `version`, `count`, `bytes`,
/// `blocks` and `records`, then the items (`position`, `id`, `value`, `version`, and `in`:
/// `block` when its block holds the value, `record` when it has its own), `next`, the token to
/// resume from, or `null` at the end, and `opened`: the `get`, `scan` and `version` calls the read
/// made and the value bytes they returned, from a [`CountingSnapshot`].
pub fn items_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, mut rest) = object(rest)?;
    let (mut start, mut limit) = (None, None);
    let usage = || Failure::usage("items takes <id> [--from P | --token G:V:P] [--limit N]");
    while let Some((word, tail)) = rest.split_first() {
        let Some((arg, tail)) = tail.split_first() else {
            return Err(Failure::usage(format!("{word} needs a value")).keyed(id));
        };
        rest = tail;
        match word.as_str() {
            "--from" if start.is_none() => {
                let p = arg.parse::<u64>().map_err(|_| {
                    Failure::usage(format!("--from {arg:?} is not a position")).keyed(id)
                })?;
                start = Some(Start::Position(p));
            }
            "--token" if start.is_none() => {
                start = Some(Start::Token(token_arg(arg).map_err(|e| e.keyed(id))?));
            }
            "--limit" if limit.is_none() => {
                limit = Some(arg.parse::<usize>().map_err(|_| {
                    Failure::usage(format!("--limit {arg:?} is not a count")).keyed(id)
                })?);
            }
            _ => return Err(usage().keyed(id)),
        }
    }
    let start = start.unwrap_or(Start::Position(0));
    let limit = limit.unwrap_or(DEFAULT_LIMIT);
    let store = Store::load(store_path)?;
    let mut fields = id_fields(id, &root);
    fields.push(("limit", limit.to_string()));
    let counting = CountingSnapshot::new(&store.snapshot);
    let page =
        items(&counting, &root, start, limit).map_err(|e| Failure::from(e).with(fields.clone()))?;
    let listed: Vec<String> = page
        .items
        .iter()
        .map(|item| {
            format!(
                "{{\"position\":{},\"id\":{},\"value\":{},\"version\":{},\"in\":{}}}",
                item.position,
                json_str(&show(item.id)),
                render(&item.value),
                item.version,
                json_str(if item.inline { "block" } else { "record" })
            )
        })
        .collect();
    fields.push(("version", page.list.version.to_string()));
    fields.push(("count", page.list.count.to_string()));
    fields.push(("bytes", page.list.bytes.to_string()));
    fields.push(("blocks", page.list.blocks.to_string()));
    fields.push(("records", page.list.records.to_string()));
    fields.push(("items", format!("[{}]", listed.join(","))));
    fields.push((
        "next",
        page.next.map_or_else(
            || "null".to_owned(),
            |t| {
                format!(
                    "{{\"generation\":{},\"version\":{},\"position\":{}}}",
                    t.generation.0, t.version, t.position
                )
            },
        ),
    ));
    fields.push((
        "opened",
        format!(
            "{{\"bytes\":{},\"calls\":{}}}",
            counting.bytes(),
            counting.calls()
        ),
    ));
    Ok(fields)
}

/// `G:V:P`: generation, root version, position.
fn token_arg(arg: &str) -> Result<Token, Failure> {
    let bad = || Failure::usage(format!("--token {arg:?} is not G:V:P"));
    let parts: Vec<&str> = arg.split(':').collect();
    let [g, v, p] = parts.as_slice() else {
        return Err(bad());
    };
    let number = |s: &str| s.parse::<u64>().map_err(|_| bad());
    Ok(Token {
        generation: Generation(number(g)?),
        version: number(v)?,
        position: number(p)?,
    })
}

struct Body {
    expected: Expected,
    records: bool,
    compile_only: bool,
    ops: Vec<ListOp>,
}

/// `(--absent [--records] | --expect V) [--compile-only] (push J | insert P J | remove P |
/// replace P J | move P Q)...`. No op at all is allowed: `list todo --absent` creates an empty
/// list.
fn parse_ops(mut rest: &[String]) -> Result<Body, Failure> {
    let (mut expected, mut compile_only, mut ops) = (None, false, Vec::new());
    let mut records = false;
    while let Some((word, tail)) = rest.split_first() {
        rest = tail;
        let mut take = |what: &str| -> Result<String, Failure> {
            let (value, tail) = rest
                .split_first()
                .ok_or_else(|| Failure::usage(format!("{word} needs {what}")))?;
            rest = tail;
            Ok(value.clone())
        };
        match word.as_str() {
            "--absent" => set_expected(&mut expected, Expected::Absent)?,
            "--expect" => {
                let v = parse_version(&take("a version")?)?;
                set_expected(&mut expected, Expected::Version(v))?;
            }
            "--compile-only" if !compile_only => compile_only = true,
            "--records" if !records => records = true,
            "push" => ops.push(ListOp::Push(element_arg(&take("a value J")?)?)),
            "insert" => {
                let at = position(word, &take("a position P")?)?;
                let value = element_arg(&take("a value J")?)?;
                ops.push(ListOp::Insert { at, value });
            }
            "remove" => {
                let at = position(word, &take("a position P")?)?;
                ops.push(ListOp::Remove { at });
            }
            "replace" => {
                let at = position(word, &take("a position P")?)?;
                let value = element_arg(&take("a value J")?)?;
                ops.push(ListOp::Replace { at, value });
            }
            "move" => {
                let from = position(word, &take("a position P")?)?;
                let to = position(word, &take("a position Q")?)?;
                ops.push(ListOp::Move { from, to });
            }
            other => return Err(Failure::usage(format!("unexpected {other:?}"))),
        }
    }
    let expected =
        expected.ok_or_else(|| Failure::usage("one of --absent or --expect V is required"))?;
    if records && expected != Expected::Absent {
        return Err(Failure::usage(
            "--records goes with --absent: a list is made with it or without it",
        ));
    }
    Ok(Body {
        expected,
        records,
        compile_only,
        ops,
    })
}

/// An op's position argument.
fn position(op: &str, text: &str) -> Result<u64, Failure> {
    text.parse::<u64>()
        .map_err(|_| Failure::usage(format!("{op} {text:?} is not a position")))
}

/// `dump`'s list root: `count`, `bytes` and `blocks` through the library's read, then the
/// payload as stored (`next`, `seed`, `bytes`, `count`, `blocks` as `[n, count, bytes, head]`
/// per block, `records`).
pub fn dump_root(snapshot: &MapSnapshot, root: &RootKey, line: &mut Line) -> Result<(), Failure> {
    let found =
        list(snapshot, root)?.ok_or_else(|| Failure::store("record vanished between two reads"))?;
    line.raw("count", found.count.to_string());
    line.raw("bytes", found.bytes.to_string());
    line.raw("blocks", found.blocks.to_string());
    let raw = snapshot
        .get(Namespace::User, root.as_bytes())
        .ok_or_else(|| Failure::store("record vanished between two reads"))?;
    let opened = envelope::open(&raw).map_err(corrupt_envelope)?;
    let payload = cbor::decode(opened.payload)?;
    line.raw("value", render(&payload));
    Ok(())
}

/// `dump`'s list item, block base or change slot: its `sub` and `id` (and `slot`), then the
/// record checked as [`check_record`] does, its envelope fields and its decoded payload. A base
/// renders `{items, folded}`; an entry is `n` when the item has its own record, or `[n, value]`.
/// A slot adds `op_no`, `op` and `state`: `pending` when the root's block entry and the base
/// make its op no one to replay, `stale` when not, or why neither could be read.
pub fn dump_record(
    snapshot: &MapSnapshot,
    root: &RootKey,
    sub: Sub,
    slot: Option<u8>,
    id: u128,
    raw: &[u8],
    line: &mut Line,
) -> Result<(), Failure> {
    line.str(
        "sub",
        match (sub, slot) {
            (Sub::Item, _) => "item",
            (_, None) => "block",
            (_, Some(_)) => "slot",
        },
    );
    line.str("id", &show(id));
    if let Some(slot) = slot {
        line.raw("slot", slot.to_string());
    }
    let value = check_record(sub, slot, raw)?;
    line.extend(envelope_fields(raw)?);
    line.raw("value", render(&value));
    if let (Some(slot), Value::Array(parts)) = (slot, &value) {
        if let [op_no, op] = parts.as_slice() {
            line.raw("op_no", render(op_no));
            line.raw("op", render(op));
            line.str("state", &slot_state(snapshot, root, id, slot, op_no));
        }
    }
    Ok(())
}

/// `pending` when op `op_no` is in its block's pending range `folded + 1 ..= head` and belongs in
/// `slot`; `stale` when it is not. When the root or the base does not decode, says so instead.
fn slot_state(snapshot: &MapSnapshot, root: &RootKey, id: u128, slot: u8, op_no: &Value) -> String {
    fn field(value: &Value, name: &str) -> Option<Value> {
        match value {
            Value::Map(m) => m.get(&MapKey::new(name)).cloned(),
            _ => None,
        }
    }
    fn uint(value: &Value) -> Option<u64> {
        match value {
            Value::Integer(i) => u64::try_from(i.get()).ok(),
            _ => None,
        }
    }
    let payload = |key: &[u8]| {
        let raw = snapshot.get(Namespace::User, key)?;
        let opened = envelope::open(&raw).ok()?;
        cbor::decode(opened.payload).ok()
    };
    let Some(op_no) = uint(op_no) else {
        return "unknown: the op no is not an unsigned integer".to_owned();
    };
    // A block's n is its id's low 64 bits.
    let n = u64::try_from(id & u128::from(u64::MAX)).expect("the low 64 bits fit u64");
    let head = payload(root.as_bytes()).and_then(|root| match field(&root, "blocks") {
        Some(Value::Array(blocks)) => blocks.iter().find_map(|block| match block {
            Value::Array(parts) if parts.first().and_then(uint) == Some(n) => {
                parts.get(3).and_then(uint)
            }
            _ => None,
        }),
        _ => None,
    });
    let Some(head) = head else {
        return "unknown: the root does not name this block".to_owned();
    };
    let folded = payload(&block_key(root, id))
        .and_then(|base| field(&base, "folded"))
        .as_ref()
        .and_then(uint);
    let Some(folded) = folded else {
        return "unknown: the block's base does not read".to_owned();
    };
    let mine = op_no % u64::from(LIST_SLOTS) == u64::from(slot);
    if mine && op_no > folded && op_no <= head {
        "pending".to_owned()
    } else {
        "stale".to_owned()
    }
}

/// An item record is a document envelope; a block base a block envelope; a change slot a slot
/// envelope. Every payload is canonical CBOR. Returns the decoded payload.
pub fn check_record(sub: Sub, slot: Option<u8>, raw: &[u8]) -> Result<Value, Failure> {
    let opened = envelope::open(raw).map_err(corrupt_envelope)?;
    match (sub, slot, opened.kind) {
        (Sub::Item, _, Kind::Document)
        | (Sub::Block, None, Kind::ListBlock)
        | (Sub::Block, Some(_), Kind::ListSlot) => {}
        (Sub::Item, _, found) => {
            return Err(Failure::from(ValueError::Corrupt(
                Corrupt::ItemNotDocument { found },
            )))
        }
        (_, _, found) => {
            return Err(Failure::from(rdb_value::delta::ApplyError::KindMismatch {
                found,
            }))
        }
    }
    cbor::decode(opened.payload).map_err(|e| Failure::from(ValueError::Corrupt(Corrupt::Codec(e))))
}

fn corrupt_envelope(e: envelope::EnvelopeError) -> Failure {
    Failure::from(ValueError::Corrupt(Corrupt::Envelope(e)))
}
