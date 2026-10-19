//! JSON in and out for documents, shared by two examples through `#[path]`: `doc_scenario`
//! (this crate) and `rdb_dev` (`rdb-api`). One copy, so the M8 input fixes (a duplicate key,
//! `-0`, an integer past 64 bits, a bad surrogate) hold in both.
//!
//! Its includer's root supplies `arg_text`, `Failure` (with `Failure::refused(error, detail)`)
//! and `MAX_JSON_TEXT`. An item one includer does not use carries `#[allow(dead_code)]`.

use rdb_value::value::{Float, Int, Map, MapKey, Value};

use super::{arg_text, Failure, MAX_JSON_TEXT};

/// One JSON value read whole from an argument (or `@FILE`), with nothing after it but
/// whitespace.
#[allow(dead_code)] // `rdb_dev` reads its values from the line with `json_value`.
pub fn json_input(arg: &str) -> Result<Value, Failure> {
    let text = arg_text(arg, MAX_JSON_TEXT)?;
    let value = serde_json::from_str::<Json>(&text)
        .map(|j| j.0)
        .map_err(|e| Failure::refused("JsonInput".into(), json_error(&text, &e)))?;
    refuse_float_integers(&text)?;
    Ok(value)
}

/// One JSON value at the start of `text`, after any whitespace, and how many bytes of `text`
/// it used, so the caller reads on after it. Refused as [`json_input`] refuses; the integer
/// scan reads only the value's own text, never what follows it.
#[allow(dead_code)] // `doc_scenario` reads whole arguments with `json_input`.
pub fn json_value(text: &str) -> Result<(Value, usize), Failure> {
    let mut stream = serde_json::Deserializer::from_str(text).into_iter::<Json>();
    let value = match stream.next() {
        Some(Ok(Json(value))) => value,
        Some(Err(e)) => return Err(Failure::refused("JsonInput".into(), json_error(text, &e))),
        None => return Err(Failure::refused("JsonInput".into(), "no JSON value".into())),
    };
    let used = stream.byte_offset();
    refuse_float_integers(&text[..used])?;
    Ok((value, used))
}

/// `serde_json` hands an integer past 64 bits, and `-0`, to the visitor as a float, with
/// nothing to say it was written as an integer. So find such a token in the text, which
/// parsed cleanly, and refuse it rather than store a float the caller did not write.
fn refuse_float_integers(text: &str) -> Result<(), Failure> {
    match float_integer(text) {
        Some(token) => Err(Failure::refused(
            "JsonInput".into(),
            format!(
                "integer {token} would become a float through JSON input (it is -0 or does not \
                 fit 64 bits); write 0, write a float, or send it with --cbor-hex"
            ),
        )),
        None => Ok(()),
    }
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

pub fn json_str(s: &str) -> String {
    serde_json::Value::String(s.to_owned()).to_string()
}

/// A document as JSON, keys in encoded order. Integers are written in full, even past 2^53.
/// Floats always carry a `.` or an exponent (`1.0`, `-0.0`, `1e300`), so they read back as
/// floats. Bytes, decimals and timestamps, which JSON lacks, are tagged objects with one key:
/// `$bytes`, `$decimal` or `$timestamp`. So that no map can print like one, a map key that
/// starts with `$` is written with one more `$` (see [`escape_key`]).
pub fn render(value: &Value) -> String {
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
