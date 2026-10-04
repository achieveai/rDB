//! Hand entry point for M8 S2 (s2-design §3): documents in a file-backed store.
//!
//! ```text
//! doc_scenario --store <FILE> compile <key> (--absent | --expect V) <body>
//! doc_scenario --store <FILE> apply <COMPILED_FILE>
//! doc_scenario --store <FILE> put <key> (--absent | --expect V) <body>
//! doc_scenario --store <FILE> op  <key> (--absent | --expect V) <body>
//! doc_scenario --store <FILE> get <key> [PATH]
//! doc_scenario --store <FILE> dump
//! doc_scenario decode --hex H
//! doc_scenario --help
//!
//! <body> is one or more of, applied in order:
//!   --json J        replace the whole document with JSON J
//!   --cbor-hex H    replace the whole document with canonical CBOR H (strictly decoded)
//!   set P J         set JSON Pointer P to JSON J
//!   remove P        remove the map key or array element at P
//!   incr P N        add integer N to the integer at P
//! A value written @FILE (for --json, --cbor-hex, --hex) is read from FILE.
//! `get <key> ""` is the whole document (RFC 6901), like `get <key>`.
//! ```
//!
//! Output: one JSON object per command on stdout. Exit 0 done, 1 refused, 2 usage, 3 the store
//! file could not be read or written. A failure carries `error` (the error's `Debug` form),
//! `error_kind` (its variant name alone, for filtering) and a one-line `detail`; a usage
//! failure also prints the usage text on stderr. `--help` prints the usage text on stdout, not
//! JSON, and exits 0.
//!
//! JSON input: integers stay integers and `1.0` stays a float, rounded correctly (to nearest,
//! ties to even). JSON cannot carry an integer past 64 bits, or `-0`, without turning it into a
//! float, so such a token is refused (`JsonInput`); send it with `--cbor-hex`. JSON input has no
//! tagged forms: every JSON object is a map, keys as written. Bytes, decimals and timestamps need
//! `--cbor-hex`.
//!
//! Output renders those three as one-key objects: `{"$bytes":"<hex>"}`,
//! `{"$decimal":[exponent,mantissa]}` and `{"$timestamp":{"secs":S,"nanos":N}}`. A map key that
//! starts with `$` is printed with one more `$` (`{"$bytes":"00"}` as a map prints
//! `{"$$bytes":"00"}`), so a single leading `$` always means a tag.
//!
//! `value` is for reading; to copy a document, pass `payload_hex` to `--cbor-hex`.
//!
//! The store is one JSON line `{"seq":N}`, then one line per record
//! `{"key":..,"version":..,"value_hex":..}`, rewritten whole on every commit. `seq` stands in for
//! the kernel's transaction sequence: each commit takes the next one and stamps it as the
//! record's version. `apply` checks the condition and `expected_version` the way the kernel's
//! `first_failed_condition` does, and refuses with the kernel's name, `ConditionFailed`.

use std::path::{Path as FsPath, PathBuf};
use std::process::ExitCode;

use bytes::Bytes;
use rdb_core::{Condition, Generation, Mutation, Namespace, SnapshotRead};
use rdb_value::cbor;
use rdb_value::delta::{resolve, ApplyError, Delta, Op};
use rdb_value::envelope;
use rdb_value::path::{Path, PathError};
use rdb_value::testing::MapSnapshot;
use rdb_value::value::{Float, Int, Map, MapKey, Value};
use rdb_value::{compile, read, Compiled, Corrupt, Expected, ValueError};

const USAGE: &str = "usage: doc_scenario --store <FILE> (compile|put|op) <key> (--absent | --expect V) \
(--json J | --cbor-hex H | set P J | remove P | incr P N)...\n       doc_scenario --store <FILE> apply <COMPILED_FILE>\n       \
doc_scenario --store <FILE> get <key> [PATH]\n       doc_scenario --store <FILE> dump\n       \
doc_scenario decode --hex H\n       doc_scenario --help\n(a value written @FILE is read from FILE)\n\
`value` is for reading; to copy a document, pass `payload_hex` to `--cbor-hex`.";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (cmd, outcome) = run(&args);
    if cmd == "help" && outcome.is_ok() {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let mut line = Line::new(cmd);
    let code = match outcome {
        Ok(fields) => {
            line.extend(fields);
            0
        }
        Err(failure) => {
            line.extend(failure.fields);
            line.str("error", &failure.error);
            line.str("error_kind", kind_of(&failure.error));
            line.str("detail", &failure.detail);
            if failure.exit == 2 {
                eprintln!("{USAGE}");
            }
            failure.exit
        }
    };
    println!("{}", line.render());
    ExitCode::from(code)
}

/// The variant name alone: `PathNotFound { segment: "x" }` → `PathNotFound`,
/// `Corrupt(DigestMismatch)` → `Corrupt`.
fn kind_of(error: &str) -> &str {
    error
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .next()
        .unwrap_or(error)
}

/// A refusal or a failure, with the fields gathered before it happened.
#[derive(Debug)]
struct Failure {
    exit: u8,
    error: String,
    detail: String,
    fields: Vec<(&'static str, String)>,
}

impl Failure {
    fn usage(detail: impl Into<String>) -> Self {
        Self {
            exit: 2,
            error: "Usage".into(),
            detail: detail.into(),
            fields: Vec::new(),
        }
    }

    fn store(detail: impl Into<String>) -> Self {
        Self {
            exit: 3,
            error: "Store".into(),
            detail: detail.into(),
            fields: Vec::new(),
        }
    }

    fn refused(error: String, detail: String) -> Self {
        Self {
            exit: 1,
            error,
            detail,
            fields: Vec::new(),
        }
    }

    fn with(mut self, fields: Vec<(&'static str, String)>) -> Self {
        self.fields = fields;
        self
    }

    /// Put `key` first, so a refusal made before any work still names its key.
    fn keyed(mut self, key: &str) -> Self {
        self.fields.insert(0, ("key", json_str(key)));
        self
    }
}

impl From<ValueError> for Failure {
    fn from(e: ValueError) -> Self {
        let name = match &e {
            ValueError::Apply(a) => format!("{a:?}"),
            ValueError::Corrupt(Corrupt::Envelope(x)) => format!("Corrupt({x:?})"),
            ValueError::Corrupt(Corrupt::Codec(x)) => format!("Corrupt({x:?})"),
            ValueError::Corrupt(c) => format!("Corrupt({c:?})"),
        };
        Self::refused(name, e.to_string())
    }
}

impl From<ApplyError> for Failure {
    fn from(e: ApplyError) -> Self {
        ValueError::Apply(e).into()
    }
}

impl From<cbor::CodecError> for Failure {
    fn from(e: cbor::CodecError) -> Self {
        Self::refused(format!("{e:?}"), e.to_string())
    }
}

/// Parse a path argument. A refusal echoes the text it got, so a path a shell rewrote (Git Bash
/// turns `/x` into `C:/Program Files/Git/x`) explains itself.
fn parse_path(text: &str) -> Result<Path, Failure> {
    Path::parse(text).map_err(|e: PathError| {
        Failure::refused(format!("{e:?}"), format!("{e} in path {text:?}"))
            .with(vec![("path", json_str(text))])
    })
}

type Fields = Vec<(&'static str, String)>;

fn run(args: &[String]) -> (&'static str, Result<Fields, Failure>) {
    let mut store: Option<PathBuf> = None;
    let mut rest = args;
    while let [flag, value, tail @ ..] = rest {
        if flag != "--store" {
            break;
        }
        if store.replace(PathBuf::from(value)).is_some() {
            return ("usage", Err(Failure::usage("--store given twice")));
        }
        rest = tail;
    }
    let Some((cmd, rest)) = rest.split_first() else {
        return ("usage", Err(Failure::usage("no command")));
    };
    let name: &'static str = match cmd.as_str() {
        "compile" => "compile",
        "apply" => "apply",
        "put" => "put",
        "op" => "op",
        "get" => "get",
        "dump" => "dump",
        "decode" => "decode",
        "--help" | "-h" | "help" if rest.is_empty() => return ("help", Ok(Vec::new())),
        other => {
            return (
                "usage",
                Err(Failure::usage(format!("unknown command {other:?}"))),
            )
        }
    };
    if name == "decode" {
        return (name, decode_cmd(rest));
    }
    let Some(store) = store else {
        return (name, Err(Failure::usage("--store <FILE> is required")));
    };
    let outcome = match name {
        "compile" => compile_cmd(&store, rest),
        "apply" => apply_cmd(&store, rest),
        "put" | "op" => commit_cmd(&store, rest),
        "get" => get_cmd(&store, rest),
        _ => dump_cmd(&store, rest),
    };
    (name, outcome)
}

// ---------------------------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------------------------

fn decode_cmd(rest: &[String]) -> Result<Fields, Failure> {
    let [flag, hex_arg] = rest else {
        return Err(Failure::usage("decode takes exactly --hex H"));
    };
    if flag != "--hex" {
        return Err(Failure::usage("decode takes exactly --hex H"));
    }
    let bytes = hex_input(hex_arg)?;
    let fields = vec![("input_len", bytes.len().to_string())];
    let value = cbor::decode(&bytes).map_err(|e| Failure::from(e).with(fields.clone()))?;
    let mut fields = fields;
    fields.push(("value", render(&value)));
    Ok(fields)
}

fn compile_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let request = parse_request(rest)?;
    let mut fields = vec![("key", json_str(&request.key))];
    let store = Store::load(store_path)?;
    let compiled = compile(
        &store.snapshot,
        request.key.as_bytes(),
        request.expected,
        &request.delta,
    )
    .map_err(|e| Failure::from(e).with(fields.clone()))?;
    fields.extend(compiled_fields(&compiled)?);
    Ok(fields)
}

fn apply_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let [file] = rest else {
        return Err(Failure::usage("apply takes exactly one file"));
    };
    let compiled = load_compiled(FsPath::new(file))?;
    let mut store = Store::load(store_path)?;
    let key = vec![(
        "key",
        json_str(&String::from_utf8_lossy(compiled.mutation.key())),
    )];
    let version = store.apply(&compiled).map_err(|e| e.with(key.clone()))?;
    let mut fields = key;
    fields.push(("version", version.to_string()));
    fields.extend(compiled_fields(&compiled)?);
    Ok(fields)
}

fn commit_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let request = parse_request(rest)?;
    let key = vec![("key", json_str(&request.key))];
    let mut store = Store::load(store_path)?;
    let compiled = compile(
        &store.snapshot,
        request.key.as_bytes(),
        request.expected,
        &request.delta,
    )
    .map_err(|e| Failure::from(e).with(key.clone()))?;
    let version = store.apply(&compiled).map_err(|e| e.with(key.clone()))?;
    let mut fields = key;
    fields.push(("version", version.to_string()));
    fields.extend(compiled_fields(&compiled)?);
    Ok(fields)
}

fn get_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    let (key, path) = match rest {
        [key] => (key, None),
        // RFC 6901: "" is the whole document.
        [key, path] if path.is_empty() => (key, None),
        [key, path] => (key, Some(parse_path(path).map_err(|e| e.keyed(key))?)),
        _ => return Err(Failure::usage("get takes <key> [PATH]")),
    };
    let store = Store::load(store_path)?;
    let mut fields = vec![("key", json_str(key))];
    if let Some(version) = store.snapshot.version(Namespace::User, key.as_bytes()) {
        fields.push(("version", version.to_string()));
    }
    let document = read(&store.snapshot, key.as_bytes())
        .map_err(|e| Failure::from(e).with(fields.clone()))?
        .ok_or_else(|| Failure::from(ApplyError::ObjectAbsent).with(fields.clone()))?;
    let raw = store
        .snapshot
        .get(Namespace::User, key.as_bytes())
        .ok_or_else(|| Failure::store("record vanished between two reads"))?;
    fields.extend(envelope_fields(&raw)?);
    let value = match &path {
        None => &document.value,
        Some(path) => {
            fields.push(("path", json_str(&path.to_string())));
            resolve(&document.value, path).map_err(|e| Failure::from(e).with(fields.clone()))?
        }
    };
    fields.push(("value", render(value)));
    Ok(fields)
}

fn dump_cmd(store_path: &FsPath, rest: &[String]) -> Result<Fields, Failure> {
    if !rest.is_empty() {
        return Err(Failure::usage("dump takes no arguments"));
    }
    let store = Store::load(store_path)?;
    let mut records = Vec::new();
    for (key, version, raw) in store.snapshot.records() {
        let key_text = String::from_utf8_lossy(key).into_owned();
        let mut line = Line::default();
        line.raw("key", json_str(&key_text));
        line.raw("version", version.to_string());
        match read(&store.snapshot, key) {
            Ok(Some(document)) => {
                line.extend(envelope_fields(raw)?);
                line.raw("value", render(&document.value));
            }
            Ok(None) => return Err(Failure::store("record vanished between two reads")),
            Err(e) => {
                let failure = Failure::from(e);
                line.raw("envelope_len", raw.len().to_string());
                line.str("error", &failure.error);
                line.str("error_kind", kind_of(&failure.error));
                line.str("detail", &failure.detail);
            }
        }
        records.push(line.render());
    }
    Ok(vec![
        ("seq", store.seq.to_string()),
        ("records", format!("[{}]", records.join(","))),
    ])
}

// ---------------------------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------------------------

struct Request {
    key: String,
    expected: Expected,
    delta: Delta,
}

fn parse_request(args: &[String]) -> Result<Request, Failure> {
    let Some((key, rest)) = args.split_first() else {
        return Err(Failure::usage("missing <key>"));
    };
    let (expected, delta) = parse_body(rest).map_err(|e| e.keyed(key))?;
    Ok(Request {
        key: key.clone(),
        expected,
        delta,
    })
}

fn parse_body(mut rest: &[String]) -> Result<(Expected, Delta), Failure> {
    let mut expected: Option<Expected> = None;
    let mut ops = Vec::new();
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
                let v = take("a version")?;
                let v = v
                    .parse::<u64>()
                    .map_err(|_| Failure::usage(format!("--expect {v:?} is not a u64")))?;
                set_expected(&mut expected, Expected::Version(v))?;
            }
            "--json" => ops.push(Op::Replace(json_input(&take("JSON")?)?)),
            "--cbor-hex" => {
                let bytes = hex_input(&take("hex")?)?;
                ops.push(Op::Replace(cbor::decode(&bytes)?));
            }
            "set" => {
                let path = parse_path(&take("a path")?)?;
                let value = json_input(&take("JSON")?)?;
                ops.push(Op::Set(path, value));
            }
            "remove" => ops.push(Op::Remove(parse_path(&take("a path")?)?)),
            "incr" => {
                let path = parse_path(&take("a path")?)?;
                let n = take("an integer")?;
                let by = n.parse::<i128>().ok().and_then(Int::new).ok_or_else(|| {
                    Failure::usage(format!("incr {n:?} is not an integer in -2^64..2^64-1"))
                })?;
                ops.push(Op::Increment(path, by));
            }
            other => return Err(Failure::usage(format!("unexpected {other:?}"))),
        }
    }
    let expected =
        expected.ok_or_else(|| Failure::usage("one of --absent or --expect V is required"))?;
    if ops.is_empty() {
        return Err(Failure::usage(
            "no body: give --json, --cbor-hex, set, remove or incr",
        ));
    }
    Ok((expected, Delta(ops)))
}

fn set_expected(slot: &mut Option<Expected>, value: Expected) -> Result<(), Failure> {
    if slot.replace(value).is_some() {
        return Err(Failure::usage("give --absent or --expect V once"));
    }
    Ok(())
}

/// `@FILE` reads the argument from FILE.
fn arg_text(arg: &str) -> Result<String, Failure> {
    match arg.strip_prefix('@') {
        Some(file) => std::fs::read_to_string(file)
            .map(|s| s.trim().to_owned())
            .map_err(|e| Failure::usage(format!("cannot read {file:?}: {e}"))),
        None => Ok(arg.to_owned()),
    }
}

fn hex_input(arg: &str) -> Result<Vec<u8>, Failure> {
    let text = arg_text(arg)?;
    hex::decode(text.trim()).map_err(|e| Failure::usage(format!("bad hex: {e}")))
}

fn json_input(arg: &str) -> Result<Value, Failure> {
    let text = arg_text(arg)?;
    let value = serde_json::from_str::<Json>(&text)
        .map(|j| j.0)
        .map_err(|e| Failure::refused("JsonInput".into(), json_error(&text, &e)))?;
    // `serde_json` hands an integer past 64 bits, and `-0`, to the visitor as a float, with
    // nothing to say it was written as an integer. So find such a token in the text, which
    // parsed cleanly, and refuse it rather than store a float the caller did not write.
    if let Some(token) = float_integer(&text) {
        return Err(Failure::refused(
            "JsonInput".into(),
            format!(
                "integer {token} would become a float through JSON input (it is -0 or does not \
                 fit 64 bits); write 0, write a float, or send it with --cbor-hex"
            ),
        ));
    }
    Ok(value)
}

/// `serde_json`'s message, except for a bad surrogate escape: there its wording misleads
/// (`"\ud800"` reads "unexpected end of hex escape", a lone trailing `"\udc00"` reads "lone
/// leading surrogate"). If the first bad surrogate sits at or before where the parser stopped,
/// that is what stopped it, and our own message names it.
fn json_error(json: &str, e: &serde_json::Error) -> String {
    match bad_surrogate(json) {
        Some((line, column, escape)) if (line, column) <= (e.line(), e.column()) => {
            format!("invalid or lone UTF-16 surrogate {escape} at line {line} column {column}")
        }
        _ => e.to_string(),
    }
}

/// The first `\uXXXX` escape in a JSON string that is a lone or misordered surrogate: a high
/// surrogate (D800–DBFF) not followed by `\u` and a low one (DC00–DFFF), or a low one alone.
/// Returns its 1-based line and column (bytes) and its text.
fn bad_surrogate(json: &str) -> Option<(usize, usize, &str)> {
    let bytes = json.as_bytes();
    let unit = |at: usize| -> Option<u16> {
        let digits = json.get(at + 2..at + 6)?;
        (bytes.get(at + 1) == Some(&b'u'))
            .then(|| u16::from_str_radix(digits, 16).ok())
            .flatten()
    };
    let (mut i, mut in_string) = (0, false);
    while i < bytes.len() {
        match (in_string, bytes[i]) {
            (false, b'"') => in_string = true,
            (true, b'"') => in_string = false,
            (true, b'\\') => {
                if let Some(u) = unit(i) {
                    let paired = (0xD800..=0xDBFF).contains(&u)
                        && bytes.get(i + 6) == Some(&b'\\')
                        && unit(i + 6).is_some_and(|low| (0xDC00..=0xDFFF).contains(&low));
                    if paired {
                        i += 12;
                        continue;
                    }
                    if (0xD800..=0xDFFF).contains(&u) {
                        let line = 1 + json[..i].matches('\n').count();
                        let column = i - json[..i].rfind('\n').map_or(0, |n| n + 1) + 1;
                        return Some((line, column, &json[i..i + 6]));
                    }
                }
                i += 1; // skip the escaped character, whatever it is
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// The first number token in `json` (already known to be valid JSON) written as an integer (no
/// `.`, `e` or `E`) that `serde_json` turns into a float: `-0`, or one that fits neither `i64`
/// nor `u64`.
fn float_integer(json: &str) -> Option<&str> {
    let bytes = json.as_bytes();
    let (mut i, mut in_string) = (0, false);
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            match b {
                b'\\' => i += 1,
                b'"' => in_string = false,
                _ => {}
            }
            i += 1;
        } else if b == b'"' {
            in_string = true;
            i += 1;
        } else if b == b'-' || b.is_ascii_digit() {
            let start = i;
            while i < bytes.len()
                && matches!(bytes[i], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
            {
                i += 1;
            }
            let token = &json[start..i];
            let integer = !token.contains(['.', 'e', 'E']);
            let wide = token.parse::<i64>().is_err() && token.parse::<u64>().is_err();
            if integer && (wide || token == "-0") {
                return Some(token);
            }
        } else {
            i += 1;
        }
    }
    None
}

/// JSON → document value. Integers stay integers. A duplicate key is refused, not collapsed.
struct Json(Value);

impl<'de> serde::Deserialize<'de> for Json {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(JsonVisitor)
    }
}

struct JsonVisitor;

impl<'de> serde::de::Visitor<'de> for JsonVisitor {
    type Value = Json;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<Json, E> {
        Ok(Json(Value::Null))
    }

    fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Json, E> {
        Ok(Json(Value::Bool(v)))
    }

    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Json, E> {
        // JSON has no NaN or infinity; a literal too large for f64 is refused by the parser.
        Float::new(v)
            .map(|f| Json(Value::Float(f)))
            .ok_or_else(|| E::custom(format!("{v} is not a finite float")))
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut access: A) -> Result<Json, A::Error> {
        let mut items = Vec::new();
        while let Some(Json(item)) = access.next_element()? {
            items.push(item);
        }
        Ok(Json(Value::Array(items)))
    }

    fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Json, E> {
        Ok(Json(Value::Integer(Int::from(v))))
    }

    fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Json, E> {
        Ok(Json(Value::Integer(Int::from(v))))
    }

    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Json, E> {
        Ok(Json(Value::Text(v.to_owned())))
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut access: A) -> Result<Json, A::Error> {
        let mut map = Map::new();
        while let Some(key) = access.next_key::<String>()? {
            let Json(value) = access.next_value()?;
            if map.insert(MapKey::new(key.clone()), value).is_some() {
                return Err(serde::de::Error::custom(format!("duplicate key {key:?}")));
            }
        }
        Ok(Json(Value::Map(map)))
    }
}

// ---------------------------------------------------------------------------------------------
// The store
// ---------------------------------------------------------------------------------------------

struct Store {
    path: PathBuf,
    seq: u64,
    snapshot: MapSnapshot,
}

impl Store {
    /// A missing file is an empty store at `seq` 0.
    fn load(path: &FsPath) -> Result<Self, Failure> {
        let mut store = Self {
            path: path.to_owned(),
            seq: 0,
            snapshot: MapSnapshot::new(Generation(1)),
        };
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(store),
            Err(e) => {
                return Err(Failure::store(format!(
                    "cannot read {}: {e}",
                    path.display()
                )))
            }
        };
        let mut lines = text
            .lines()
            .enumerate()
            .filter(|(_, l)| !l.trim().is_empty());
        let bad = |n: usize, what: &str| {
            Failure::store(format!("{} line {}: {what}", path.display(), n + 1))
        };
        let (n, head) = lines.next().ok_or_else(|| bad(0, "empty file"))?;
        let head: serde_json::Value =
            serde_json::from_str(head).map_err(|e| bad(n, &e.to_string()))?;
        store.seq = head["seq"]
            .as_u64()
            .ok_or_else(|| bad(n, "first line must be {\"seq\":N}"))?;
        for (n, line) in lines {
            let record: serde_json::Value =
                serde_json::from_str(line).map_err(|e| bad(n, &e.to_string()))?;
            let key = record["key"]
                .as_str()
                .ok_or_else(|| bad(n, "missing \"key\""))?;
            let version = record["version"]
                .as_u64()
                .ok_or_else(|| bad(n, "missing \"version\""))?;
            let value_hex = record["value_hex"]
                .as_str()
                .ok_or_else(|| bad(n, "missing \"value_hex\""))?;
            let value = hex::decode(value_hex).map_err(|e| bad(n, &format!("value_hex: {e}")))?;
            if version > store.seq {
                return Err(bad(n, "version is newer than seq"));
            }
            store.snapshot.insert(
                Bytes::copy_from_slice(key.as_bytes()),
                version,
                Bytes::from(value),
            );
        }
        Ok(store)
    }

    /// Check `compiled` as the kernel does, then commit it at the next `seq`.
    fn apply(&mut self, compiled: &Compiled) -> Result<u64, Failure> {
        let Mutation::Put {
            key,
            value,
            expected_version,
        } = &compiled.mutation
        else {
            return Err(Failure::usage("only a Put can be applied"));
        };
        // The kernel's order: every condition, then every mutation's `expected_version`.
        // Each check: what it claims, the key it reads, and whether it held.
        let mut checks: Vec<(String, &Bytes, bool)> = Vec::new();
        if let Some(condition) = &compiled.condition {
            let (claim, key, held) = match condition {
                Condition::Absent { key } => (
                    "absent".to_owned(),
                    key,
                    self.snapshot.version(Namespace::User, key).is_none(),
                ),
                Condition::Present { key } => (
                    "present".to_owned(),
                    key,
                    self.snapshot.version(Namespace::User, key).is_some(),
                ),
                Condition::VersionEquals { key, version } => (
                    format!("version {version}"),
                    key,
                    self.snapshot.version(Namespace::User, key) == Some(*version),
                ),
            };
            checks.push((claim, key, held));
        }
        if let Some(want) = expected_version {
            let found = self.snapshot.version(Namespace::User, key);
            checks.push((
                format!("version {want} (expected_version)"),
                key,
                found == Some(*want),
            ));
        }
        if let Some(index) = checks.iter().position(|(_, _, held)| !held) {
            let (claim, key, _) = &checks[index];
            let found = match self.snapshot.version(Namespace::User, key) {
                Some(v) => format!("version {v}"),
                None => "absent".to_owned(),
            };
            return Err(Failure::refused(
                format!("ConditionFailed {{ index: {index} }}"),
                format!(
                    "check {index} failed for key {}: expected {claim}, found {found}",
                    json_str(&String::from_utf8_lossy(key))
                ),
            ));
        }
        let version = self.seq + 1;
        self.snapshot.insert(key.clone(), version, value.clone());
        self.seq = version;
        self.save()?;
        Ok(version)
    }

    /// Rewrite the whole file: to a sibling first, then rename over the old one.
    fn save(&self) -> Result<(), Failure> {
        let mut text = format!("{{\"seq\":{}}}\n", self.seq);
        for (key, version, value) in self.snapshot.records() {
            let key = String::from_utf8_lossy(key);
            text.push_str(&format!(
                "{{\"key\":{},\"version\":{version},\"value_hex\":\"{}\"}}
",
                json_str(&key),
                hex::encode(value)
            ));
        }
        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, text)
            .and_then(|()| std::fs::rename(&tmp, &self.path))
            .map_err(|e| Failure::store(format!("cannot write {}: {e}", self.path.display())))
    }
}

/// Read back the line `compile` printed.
fn load_compiled(file: &FsPath) -> Result<Compiled, Failure> {
    let text = std::fs::read_to_string(file)
        .map_err(|e| Failure::usage(format!("cannot read {}: {e}", file.display())))?;
    let line = text
        .lines()
        .find(|l| !l.trim().is_empty())
        .ok_or_else(|| Failure::usage(format!("{} is empty", file.display())))?;
    let bad =
        |what: &str| Failure::usage(format!("{} is not a compile line: {what}", file.display()));
    let json: serde_json::Value = serde_json::from_str(line).map_err(|e| bad(&e.to_string()))?;
    if json["cmd"] != "compile" || !json["error"].is_null() {
        return Err(bad("cmd must be \"compile\", with no error"));
    }
    let key = Bytes::copy_from_slice(json["key"].as_str().ok_or_else(|| bad("key"))?.as_bytes());
    let value = hex::decode(
        json["envelope_hex"]
            .as_str()
            .ok_or_else(|| bad("envelope_hex"))?,
    )
    .map_err(|e| bad(&format!("envelope_hex: {e}")))?;
    let expected_version = match &json["expected_version"] {
        serde_json::Value::Null => None,
        v => Some(v.as_u64().ok_or_else(|| bad("expected_version"))?),
    };
    let condition = match json["condition"].as_str() {
        None => None,
        Some("Absent") => Some(Condition::Absent { key: key.clone() }),
        Some(other) => return Err(bad(&format!("unknown condition {other:?}"))),
    };
    Ok(Compiled {
        mutation: Mutation::Put {
            key,
            value: Bytes::from(value),
            expected_version,
        },
        condition,
    })
}

// ---------------------------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------------------------

fn compiled_fields(compiled: &Compiled) -> Result<Fields, Failure> {
    let Mutation::Put {
        value,
        expected_version,
        ..
    } = &compiled.mutation
    else {
        return Err(Failure::usage(
            "compile produced something other than a Put",
        ));
    };
    let condition = match &compiled.condition {
        None => "null".to_owned(),
        Some(Condition::Absent { .. }) => json_str("Absent"),
        Some(other) => json_str(&format!("{other:?}")),
    };
    let mut fields = vec![
        ("condition", condition),
        (
            "expected_version",
            expected_version.map_or_else(|| "null".to_owned(), |v| v.to_string()),
        ),
    ];
    fields.extend(envelope_fields(value)?);
    fields.push(("envelope_hex", json_str(&hex::encode(value))));
    let opened = envelope::open(value)
        .map_err(|e| Failure::from(ValueError::Corrupt(Corrupt::Envelope(e))))?;
    let after = cbor::decode(opened.payload)
        .map_err(|e| Failure::from(ValueError::Corrupt(Corrupt::Codec(e))))?;
    fields.push(("value", render(&after)));
    Ok(fields)
}

fn envelope_fields(raw: &[u8]) -> Result<Fields, Failure> {
    let opened = envelope::open(raw)
        .map_err(|e| Failure::from(ValueError::Corrupt(Corrupt::Envelope(e))))?;
    Ok(vec![
        ("digest", json_str(&hex::encode(opened.digest))),
        ("payload_hex", json_str(&hex::encode(opened.payload))),
        ("payload_len", opened.payload.len().to_string()),
        ("envelope_len", raw.len().to_string()),
        ("envelope_head", json_str(&hex::encode(&raw[..8]))),
    ])
}

fn json_str(s: &str) -> String {
    serde_json::Value::String(s.to_owned()).to_string()
}

/// A document as JSON, keys in encoded order. Integers are written in full, even past 2^53.
/// Floats always carry a `.` or an exponent (`1.0`, `-0.0`, `1e300`), so they read back as
/// floats. Bytes, decimals and timestamps, which JSON lacks, are tagged objects with one key:
/// `$bytes`, `$decimal` or `$timestamp`. So that no map can print like one, a map key that
/// starts with `$` is written with one more `$` (see [`escape_key`]).
fn render(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(b) => b.to_string(),
        Value::Integer(i) => i.get().to_string(),
        Value::Float(f) => format!("{:?}", f.get()),
        Value::Decimal(d) => format!("{{\"$decimal\":[{},{}]}}", d.exponent(), d.mantissa().get()),
        Value::Timestamp(t) => format!(
            "{{\"$timestamp\":{{\"secs\":{},\"nanos\":{}}}}}",
            t.secs(),
            t.nanos()
        ),
        Value::Text(t) => json_str(t),
        Value::Bytes(b) => format!("{{\"$bytes\":{}}}", json_str(&hex::encode(b))),
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(render).collect();
            format!("[{}]", items.join(","))
        }
        Value::Map(map) => {
            let entries: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}:{}", json_str(&escape_key(k.as_str())), render(v)))
                .collect();
            format!("{{{}}}", entries.join(","))
        }
    }
}

/// The output rule that keeps tagged scalars apart from maps: a map key starting with `$` gets
/// one more `$` (`$bytes` → `$$bytes`, `$$x` → `$$$x`). A key with a single leading `$` is
/// therefore always a tag. Input needs no rule: JSON input has no tagged forms, so every JSON
/// object is a map with its keys as written.
fn escape_key(key: &str) -> String {
    if key.starts_with('$') {
        format!("${key}")
    } else {
        key.to_owned()
    }
}

/// One JSON object, fields in insertion order. Values are already JSON.
#[derive(Default)]
struct Line(Vec<(&'static str, String)>);

impl Line {
    fn new(cmd: &str) -> Self {
        let mut line = Self::default();
        line.str("cmd", cmd);
        line
    }

    fn raw(&mut self, key: &'static str, json: String) {
        self.0.push((key, json));
    }

    fn str(&mut self, key: &'static str, text: &str) {
        self.raw(key, json_str(text));
    }

    fn extend(&mut self, fields: Fields) {
        self.0.extend(fields);
    }

    fn render(&self) -> String {
        let body: Vec<String> = self
            .0
            .iter()
            .map(|(k, v)| format!("{}:{v}", json_str(k)))
            .collect();
        format!("{{{}}}", body.join(","))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn float_bits(json: &str) -> u64 {
        match json_input(json) {
            Ok(Value::Float(f)) => f.get().to_bits(),
            Ok(other) => panic!("{json} gave {other:?}, not a float"),
            Err(e) => panic!("{json} refused: {} {}", e.error, e.detail),
        }
    }

    /// D1 (tester W2): JSON floats were parsed best-effort, 1 ULP off in 105 of 311 cases.
    /// Expected bits are Node's `writeDoubleBE`, from the tester's report.
    #[test]
    fn d1_json_float_input_is_correctly_rounded() {
        assert_eq!(float_bits("9007199254740993.0"), 0x4340_0000_0000_0000);
        // Exactly halfway between 1.0 and the next double: ties to even.
        assert_eq!(
            float_bits("1.00000000000000011102230246251565404236316680908203125"),
            0x3ff0_0000_0000_0000
        );
        assert_eq!(float_bits("2.2250738585072011e-308"), 0x000f_ffff_ffff_ffff);
    }

    /// D1: the largest finite double spelled out in full (309 digits) was refused as "number out
    /// of range".
    #[test]
    fn d1_json_max_float_in_full_is_accepted() {
        let max = format!("{:.1}", f64::MAX);
        assert_eq!(max.len(), 311, "{max}");
        assert_eq!(float_bits(&max), f64::MAX.to_bits());
    }

    /// The `-0` refusal (dev W2 finding 1): `serde_json` reads the integer `-0` as float -0.0.
    #[test]
    fn json_integer_minus_zero_is_refused_in_every_position() {
        for json in ["-0", "[-0]", "{\"a\":-0}", "[1e2,-0]"] {
            let err = json_input(json).expect_err(json);
            assert_eq!(err.error, "JsonInput", "{json}");
        }
        assert_eq!(float_bits("-0.0"), (-0.0_f64).to_bits());
    }

    /// A3 (tester W2): the byte string `4100` and the JSON map `{"$bytes":"00"}` rendered alike.
    #[test]
    fn a3_a_map_never_renders_as_a_tagged_scalar() {
        let map = json_input("{\"$bytes\":\"00\"}").expect("a map");
        assert!(matches!(map, Value::Map(_)), "{map:?}");
        let bytes = cbor::decode(&[0x41, 0x00]).expect("bytes");
        assert_ne!(render(&map), render(&bytes));
        assert_eq!(render(&map), "{\"$$bytes\":\"00\"}");
        assert_eq!(render(&bytes), "{\"$bytes\":\"00\"}");
    }

    // ---- W3: the example's JSON contracts (tester W2 rows 11 and 12, re-test list) ----------

    /// A non-negative decimal integer as little-endian base-10 digits, for exact float edges.
    fn digits(mut v: u128) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            out.push(u8::try_from(v % 10).unwrap());
            v /= 10;
            if v == 0 {
                return out;
            }
        }
    }

    fn mul(ds: &[u8], by: u128) -> Vec<u8> {
        let (mut out, mut carry) = (Vec::new(), 0_u128);
        for d in ds {
            let x = u128::from(*d) * by + carry;
            out.push(u8::try_from(x % 10).unwrap());
            carry = x / 10;
        }
        while carry > 0 {
            out.push(u8::try_from(carry % 10).unwrap());
            carry /= 10;
        }
        out
    }

    fn pow(base: u128, exp: u32) -> Vec<u8> {
        (0..exp).fold(digits(1), |acc, _| mul(&acc, base))
    }

    fn minus_one(ds: &[u8]) -> Vec<u8> {
        let mut out = ds.to_vec();
        for d in &mut out {
            if *d > 0 {
                *d -= 1;
                break;
            }
            *d = 9;
        }
        while out.len() > 1 && out.last() == Some(&0) {
            out.pop();
        }
        out
    }

    fn text_of(ds: &[u8]) -> String {
        ds.iter().rev().map(|d| char::from(b'0' + d)).collect()
    }

    /// `ds × 10^-places` as JSON, e.g. digits of 5 with 1 place → `0.5`.
    fn fraction(ds: &[u8], places: usize) -> String {
        let t = text_of(ds);
        format!("0.{t:0>places$}")
    }

    /// Rounding edges, exact in decimal: halfway between the largest double and 2^1024 rounds
    /// to infinity and is refused, one less is the largest double; 2^-1075 (half the smallest
    /// subnormal) ties to even, i.e. 0; 3·2^-1075 ties to even, i.e. two units (re-test row 11).
    #[test]
    fn json_floats_round_to_nearest_even_at_the_edges() {
        let halfway = mul(&pow(2, 970), (1 << 54) - 1);
        let err = json_input(&format!("{}.0", text_of(&halfway))).expect_err("rounds to inf");
        assert_eq!(err.error, "JsonInput");
        assert_eq!(
            float_bits(&format!("{}.0", text_of(&minus_one(&halfway)))),
            f64::MAX.to_bits()
        );
        // 2^-1075 = 5^1075 / 10^1075.
        let half_min = pow(5, 1075);
        assert_eq!(float_bits(&fraction(&half_min, 1075)), 0);
        assert_eq!(float_bits(&fraction(&mul(&half_min, 3), 1075)), 2);
        assert_eq!(float_bits(&fraction(&mul(&half_min, 2), 1075)), 1);
    }

    /// ±1e-400 underflows to ±0.0 with its sign kept; ±1e400 is refused (L-R185q; ADR §3).
    #[test]
    fn json_underflow_keeps_the_sign_and_overflow_is_refused() {
        assert_eq!(float_bits("1e-400"), 0);
        assert_eq!(float_bits("-1e-400"), (-0.0_f64).to_bits());
        for json in ["1e400", "-1e400", "[1e400]"] {
            assert_eq!(
                json_input(json).expect_err(json).error,
                "JsonInput",
                "{json}"
            );
        }
    }

    proptest::proptest! {
        /// Rendered floats read back to the same bits through `--json` (re-test row 1j).
        #[test]
        fn rendered_floats_read_back_bit_exact(bits in proptest::prelude::any::<u64>()) {
            let f = f64::from_bits(bits);
            if let Some(float) = Float::new(f) {
                let shown = render(&Value::Float(float));
                proptest::prop_assert_eq!(float_bits(&shown), bits, "{}", shown);
            }
        }
    }

    /// JSON input never silently changes a value: an integer past 64 bits, `-0`, and a
    /// duplicate key are refused at any depth; integers at the 64-bit edges stay integers.
    #[test]
    fn json_input_refuses_what_it_cannot_keep_at_any_depth() {
        for json in [
            "18446744073709551616",
            "[-9223372036854775809]",
            "{\"a\":{\"b\":[18446744073709551616]}}",
            "{\"a\":{\"b\":1,\"b\":2}}",
            "[{\"x\":1,\"x\":1}]",
        ] {
            assert_eq!(
                json_input(json).expect_err(json).error,
                "JsonInput",
                "{json}"
            );
        }
        let edges = json_input("[18446744073709551615,-9223372036854775808]").expect("edges");
        assert_eq!(
            edges,
            Value::Array(vec![
                Value::Integer(Int::new(18_446_744_073_709_551_615).unwrap()),
                Value::Integer(Int::new(-9_223_372_036_854_775_808).unwrap()),
            ])
        );
        // The same digits inside a string are text.
        assert!(matches!(
            json_input("\"18446744073709551616\""),
            Ok(Value::Text(_))
        ));
    }

    /// A lone UTF-16 surrogate is refused with our own message and its position; a valid pair
    /// and an escaped backslash before `u` are accepted (L-R185o 3b).
    #[test]
    fn lone_surrogates_are_refused_and_named() {
        for (json, at) in [
            (r#""\ud800""#, "line 1 column 2"),
            (r#""\udc00""#, "line 1 column 2"),
            (r#""abc\ud800""#, "line 1 column 5"),
            (r#"{"\ud800":1}"#, "line 1 column 3"),
            (r#"["ok","\udfff"]"#, "line 1 column 8"),
            (r#""\ud800\ud800""#, "line 1 column 2"),
        ] {
            let err = json_input(json).expect_err(json);
            assert_eq!(err.error, "JsonInput", "{json}");
            assert!(
                err.detail.starts_with("invalid or lone UTF-16 surrogate")
                    && err.detail.ends_with(at),
                "{json}: {}",
                err.detail
            );
        }
        assert_eq!(
            json_input(r#""😀""#).expect("pair"),
            Value::Text("😀".into())
        );
        assert_eq!(
            json_input(r#""\\ud800""#).expect("text"),
            Value::Text("\\ud800".into())
        );
    }

    /// The render rule (re-test row 12): tags show with one `$`; every map key that starts with
    /// `$` gains one `$`, at any depth; other keys are untouched.
    #[test]
    fn tags_render_with_one_dollar_and_dollar_keys_gain_one() {
        let mut inner = Map::new();
        inner.insert(MapKey::new("$y"), Value::Null);
        let mut m = Map::new();
        m.insert(MapKey::new("a$"), Value::Integer(Int::from(1_u64)));
        m.insert(MapKey::new("$$x"), Value::Array(vec![Value::Map(inner)]));
        m.insert(
            MapKey::new("d"),
            cbor::decode(&[0xc4, 0x82, 0x21, 0x19, 0x04, 0xd2]).expect("decimal"),
        );
        m.insert(
            MapKey::new("t"),
            cbor::decode(&hex::decode("d903e9a2012028187b").unwrap()).expect("timestamp"),
        );
        m.insert(MapKey::new("b"), Value::Bytes(vec![0x00, 0xff]));
        assert_eq!(
            render(&Value::Map(m)),
            "{\"b\":{\"$bytes\":\"00ff\"},\"d\":{\"$decimal\":[-2,1234]},\
             \"t\":{\"$timestamp\":{\"secs\":-1,\"nanos\":123}},\"a$\":1,\
             \"$$$x\":[{\"$$y\":null}]}"
        );
    }
}
