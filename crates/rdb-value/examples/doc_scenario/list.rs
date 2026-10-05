//! `doc_scenario`'s list commands (ADR-rdb-0016). A separate file for size only: it shares the
//! store, `apply` and the output of `doc_scenario.rs`.
//!
//! A list write always commits through `apply_at` with the generation its compile returned, and
//! its compile line carries that generation, so `apply` fences it too (ADR-rdb-0016 §8).

use std::path::Path as FsPath;

use rdb_core::{Generation, SnapshotRead};
use rdb_value::cbor;
use rdb_value::envelope::{self, Kind};
use rdb_value::keys::{RootKey, Sub};
use rdb_value::list::{compile_list, drop_list, items, list, ListOp, Start, Token};
use rdb_value::testing::MapSnapshot;
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

/// `list <id> (--absent | --expect V) [--compile-only] (push J | insert P J)...`: compile the ops
/// at the store's `node_max`, then commit at the compile's generation, or print with
/// `--compile-only`. Prints the minted `ids` in op order.
pub fn write_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    let body = parse_ops(rest).map_err(|e| e.keyed(id))?;
    let mut fields = id_fields(id, &root);
    let mut store = Store::load(store_path)?;
    let node_max = store.node_max();
    let compiled = compile_list(&store.snapshot, &root, body.expected, node_max, &body.ops)
        .map_err(|e| Failure::from(e).with(fields.clone()))?;
    let ids: Vec<String> = compiled.ids().iter().map(|i| json_str(&show(*i))).collect();
    fields.push(("ids", format!("[{}]", ids.join(","))));
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

/// `items <id> [--from P | --token G:V:P] [--limit N]`: the list's `version`, `count`, `bytes`
/// and `height`, then the items (`position`, `id`, `value`, `version`) and `next`, the token to
/// resume from, or `null` at the end.
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
    let page = items(&store.snapshot, &root, start, limit)
        .map_err(|e| Failure::from(e).with(fields.clone()))?;
    let listed: Vec<String> = page
        .items
        .iter()
        .map(|item| {
            format!(
                "{{\"position\":{},\"id\":{},\"value\":{},\"version\":{}}}",
                item.position,
                json_str(&show(item.id)),
                render(&item.value),
                item.version
            )
        })
        .collect();
    fields.push(("version", page.list.version.to_string()));
    fields.push(("count", page.list.count.to_string()));
    fields.push(("bytes", page.list.bytes.to_string()));
    fields.push(("height", page.list.height.to_string()));
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
    compile_only: bool,
    ops: Vec<ListOp>,
}

/// `(--absent | --expect V) [--compile-only] (push J | insert P J)...`. No op at all is allowed:
/// `list todo --absent` creates an empty list.
fn parse_ops(mut rest: &[String]) -> Result<Body, Failure> {
    let (mut expected, mut compile_only, mut ops) = (None, false, Vec::new());
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
            "push" => ops.push(ListOp::Push(element_arg(&take("a value J")?)?)),
            "insert" => {
                let p = take("a position P")?;
                let at = p
                    .parse::<u64>()
                    .map_err(|_| Failure::usage(format!("insert {p:?} is not a position")))?;
                let value = element_arg(&take("a value J")?)?;
                ops.push(ListOp::Insert { at, value });
            }
            other => return Err(Failure::usage(format!("unexpected {other:?}"))),
        }
    }
    let expected =
        expected.ok_or_else(|| Failure::usage("one of --absent or --expect V is required"))?;
    Ok(Body {
        expected,
        compile_only,
        ops,
    })
}

/// `dump`'s list root: `count`, `bytes` and `height` through the library's read, then the
/// payload as stored (`next`, `tree`, `bytes`, `count`).
pub fn dump_root(snapshot: &MapSnapshot, root: &RootKey, line: &mut Line) -> Result<(), Failure> {
    let found =
        list(snapshot, root)?.ok_or_else(|| Failure::store("record vanished between two reads"))?;
    line.raw("count", found.count.to_string());
    line.raw("bytes", found.bytes.to_string());
    line.raw("height", found.height.to_string());
    let raw = snapshot
        .get(rdb_core::Namespace::User, root.as_bytes())
        .ok_or_else(|| Failure::store("record vanished between two reads"))?;
    let opened = envelope::open(&raw).map_err(corrupt_envelope)?;
    let payload = cbor::decode(opened.payload)?;
    line.raw("value", render(&payload));
    Ok(())
}

/// `dump`'s list item or page: its `id`, then the record checked as [`check_record`] does, its
/// envelope fields and its decoded payload.
pub fn dump_record(sub: Sub, id: u128, raw: &[u8], line: &mut Line) -> Result<(), Failure> {
    line.str("sub", if sub == Sub::Item { "item" } else { "page" });
    line.str("id", &show(id));
    let value = check_record(sub, raw)?;
    line.extend(envelope_fields(raw)?);
    line.raw("value", render(&value));
    Ok(())
}

/// An item record is a document envelope; a page record is a page envelope. Either payload is
/// canonical CBOR. Returns the decoded payload.
pub fn check_record(sub: Sub, raw: &[u8]) -> Result<rdb_value::value::Value, Failure> {
    let opened = envelope::open(raw).map_err(corrupt_envelope)?;
    match (sub, opened.kind) {
        (Sub::Item, Kind::Document) | (Sub::Page, Kind::ListPage) => {}
        (Sub::Item, found) => {
            return Err(Failure::from(ValueError::Corrupt(
                Corrupt::EntryNotDocument { found },
            )))
        }
        (_, found) => {
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
