//! `doc_scenario`'s map and set commands (s3-design §3). A separate file for size only: it
//! shares the store, `apply` and the output of `doc_scenario.rs`.

use std::path::Path as FsPath;

use bytes::Bytes;
use rdb_core::{Generation, Mutation};
use rdb_value::cbor;
pub use rdb_value::collection::{collection, member, CollectionKind};
use rdb_value::collection::{compile_collection, drop_collection, members, ElemOp};
use rdb_value::delta::ApplyError;
use rdb_value::keys::{self, RootKey};
use rdb_value::testing::MapSnapshot;
use rdb_value::value::Value;
use rdb_value::{Compiled, Expected};

use super::{
    emit, id_fields, json_input, json_str, render, root_of, set_expected, Failure, Fields, Store,
};

/// `members` lists this many when `--limit` is not given.
const DEFAULT_LIMIT: usize = 1000;

/// `map` and `set`: compile the ops against the collection at `<id>`, then commit, or print
/// with `--compile-only`. Either command takes every op, so a map op on a set reaches the
/// library and is refused there (`KindMismatch`).
pub fn write_cmd(
    store_path: &FsPath,
    kind: CollectionKind,
    rest: &[String],
) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    let body = parse_ops(rest).map_err(|e| e.keyed(id))?;
    let mut fields = id_fields(id, &root);
    let mut store = Store::load(store_path)?;
    let compiled = compile_collection(&store.snapshot, &root, kind, body.expected, &body.ops)
        .map_err(|e| Failure::from(e).with(fields.clone()))?;
    fields.extend(root_after(&compiled, &root)?);
    emit(&mut store, &compiled, body.compile_only, fields)
}

/// `drop <id> --expect V [--compile-only]`.
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
    let compiled = drop_collection(&store.snapshot, &root, version)
        .map_err(|e| Failure::from(e).with(fields.clone()))?;
    emit(&mut store, &compiled, compile_only, fields)
}

/// `collection <id>`: the root's kind, version and count.
pub fn collection_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    if !rest.is_empty() {
        return Err(Failure::usage("collection takes <id>").keyed(id));
    }
    let store = Store::load(store_path)?;
    let mut fields = id_fields(id, &root);
    let found = collection(&store.snapshot, &root)
        .map_err(|e| Failure::from(e).with(fields.clone()))?
        .ok_or_else(|| Failure::from(ApplyError::ObjectAbsent).with(fields.clone()))?;
    fields.push(("kind", json_str(&format!("{:?}", found.kind))));
    fields.push(("version", found.version.to_string()));
    fields.push(("count", found.count.to_string()));
    Ok(fields)
}

/// `member <id> K`: one element, or `present: false`.
pub fn member_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, rest) = object(rest)?;
    let [k] = rest else {
        return Err(Failure::usage("member takes <id> K").keyed(id));
    };
    let key = element_arg(k).map_err(|e| e.keyed(id))?;
    let store = Store::load(store_path)?;
    let mut fields = id_fields(id, &root);
    fields.push(("element", render(&key)));
    let found =
        member(&store.snapshot, &root, &key).map_err(|e| Failure::from(e).with(fields.clone()))?;
    let element_key = keys::element_key(&root, &key).map_err(Failure::from)?;
    fields.push(("element_key_hex", json_str(&hex::encode(element_key))));
    fields.push(("present", found.is_some().to_string()));
    if let Some(found) = found {
        fields.push(("version", found.version.to_string()));
        if let Some(value) = &found.value {
            fields.push(("value", render(value)));
        }
    }
    Ok(fields)
}

/// `members <id> [--after K] [--limit N]`: a page of elements, in element order.
pub fn members_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (id, root, mut rest) = object(rest)?;
    let (mut after, mut limit) = (None, None);
    while let Some((word, tail)) = rest.split_first() {
        let Some((arg, tail)) = tail.split_first() else {
            return Err(Failure::usage(format!("{word} needs a value")).keyed(id));
        };
        rest = tail;
        match word.as_str() {
            "--after" if after.is_none() => {
                after = Some(element_arg(arg).map_err(|e| e.keyed(id))?)
            }
            "--limit" if limit.is_none() => {
                limit = Some(arg.parse::<usize>().map_err(|_| {
                    Failure::usage(format!("--limit {arg:?} is not a count")).keyed(id)
                })?);
            }
            _ => return Err(Failure::usage("members takes <id> [--after K] [--limit N]").keyed(id)),
        }
    }
    let limit = limit.unwrap_or(DEFAULT_LIMIT);
    let store = Store::load(store_path)?;
    let mut fields = id_fields(id, &root);
    if let Some(after) = &after {
        fields.push(("after", render(after)));
    }
    fields.push(("limit", limit.to_string()));
    let page = members(&store.snapshot, &root, after.as_ref(), limit)
        .map_err(|e| Failure::from(e).with(fields.clone()))?;
    let mut listed = Vec::new();
    for found in &page.members {
        let key_hex = hex::encode(keys::element_key(&root, &found.key).map_err(Failure::from)?);
        let mut item = vec![
            format!("\"key\":{}", render(&found.key)),
            format!("\"key_hex\":{}", json_str(&key_hex)),
        ];
        if let Some(value) = &found.value {
            item.push(format!("\"value\":{}", render(value)));
        }
        item.push(format!("\"version\":{}", found.version));
        listed.push(format!("{{{}}}", item.join(",")));
    }
    fields.push(("kind", json_str(&format!("{:?}", page.collection.kind))));
    fields.push(("version", page.collection.version.to_string()));
    fields.push(("count", page.collection.count.to_string()));
    fields.push(("listed", listed.len().to_string()));
    fields.push(("members", format!("[{}]", listed.join(","))));
    Ok(fields)
}

/// The `<id>` that starts every collection command, its root key, and the rest.
fn object(rest: &[String]) -> Result<(&String, RootKey, &[String]), Failure> {
    let Some((id, rest)) = rest.split_first() else {
        return Err(Failure::usage("missing <id>"));
    };
    let root = root_of(id).map_err(|e| e.keyed(id))?;
    Ok((id, root, rest))
}

/// `kind` and `count` as the compiled root `Put` writes them, read back through the library.
fn root_after(compiled: &Compiled, root: &RootKey) -> Result<Fields, Failure> {
    let mut written = MapSnapshot::new(Generation(1));
    for mutation in &compiled.mutations {
        if let Mutation::Put { key, value, .. } = mutation {
            if key.as_ref() == root.as_bytes() {
                written.insert(Bytes::clone(key), 1, value.clone());
            }
        }
    }
    let after = collection(&written, root)?
        .ok_or_else(|| Failure::store("a collection compile wrote no root"))?;
    Ok(vec![
        ("kind", json_str(&format!("{:?}", after.kind))),
        ("count", after.count.to_string()),
    ])
}

struct Body {
    expected: Expected,
    compile_only: bool,
    ops: Vec<ElemOp>,
}

/// `(--absent | --expect V) [--compile-only] (put K J | add K | del K | need K present|absent)...`
/// No op at all is allowed: `map cart --absent` creates an empty map.
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
            "put" => {
                let key = element_arg(&take("a key K")?)?;
                let value = element_arg(&take("a value J")?)?;
                ops.push(ElemOp::Put(key, value));
            }
            "add" => ops.push(ElemOp::Add(element_arg(&take("a member K")?)?)),
            "del" => ops.push(ElemOp::Remove(element_arg(&take("a key K")?)?)),
            "need" => {
                let key = element_arg(&take("a key K")?)?;
                match take("present or absent")?.as_str() {
                    "present" => ops.push(ElemOp::NeedPresent(key)),
                    "absent" => ops.push(ElemOp::NeedAbsent(key)),
                    other => {
                        return Err(Failure::usage(format!(
                            "need takes present or absent, not {other:?}"
                        )))
                    }
                }
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

fn parse_version(v: &str) -> Result<u64, Failure> {
    v.parse::<u64>()
        .map_err(|_| Failure::usage(format!("--expect {v:?} is not a u64")))
}

/// A key or a value: `cbor:<hex>`, strictly decoded, or JSON (`@FILE` reads it from FILE). An
/// array or a map is passed on, so the library is what refuses it as a key.
fn element_arg(arg: &str) -> Result<Value, Failure> {
    match arg.strip_prefix("cbor:") {
        Some(digits) => {
            let bytes = hex::decode(digits)
                .map_err(|e| Failure::usage(format!("{arg:?}: bad hex: {e}")))?;
            Ok(cbor::decode(&bytes)?)
        }
        None => json_input(arg),
    }
}
