//! Shared by the `rdb-value` test binaries: hex, value builders, the independent byte walk, the
//! second reader (`ciborium`) and a random-document strategy. Written apart from `src`, so a
//! fault in the encoder is not mirrored here (ADR-rdb-0012 Verification).

#![allow(dead_code)] // each test binary uses its own subset

use proptest::prelude::*;
use rdb_value::value::{Decimal, Float, Int, Map, MapKey, Timestamp, Value, INT_MAX, INT_MIN};

/// Bytes from hex. Panics on bad hex: a test typo, not a product fault.
pub fn h(hex: &str) -> Vec<u8> {
    hex::decode(hex).unwrap_or_else(|e| panic!("bad test hex {hex:?}: {e}"))
}

pub fn int(v: i128) -> Value {
    Value::Integer(Int::new(v).expect("in range"))
}

pub fn text(s: &str) -> Value {
    Value::Text(s.to_owned())
}

pub fn float(f: f64) -> Value {
    Value::Float(Float::new(f).expect("finite"))
}

pub fn decimal(exponent: i64, mantissa: i128) -> Value {
    Value::Decimal(Decimal::new(exponent, Int::new(mantissa).expect("in range")).expect("normal"))
}

pub fn timestamp(secs: i64, nanos: u32) -> Value {
    Value::Timestamp(Timestamp::new(secs, nanos).expect("nanos in range"))
}

pub fn map(entries: &[(&str, Value)]) -> Value {
    let mut m = Map::new();
    for (k, v) in entries {
        assert!(m.insert(MapKey::new(*k), v.clone()).is_none(), "dup {k}");
    }
    Value::Map(m)
}

/// `n` arrays around `inner`: `[[[inner]]]` for `n = 3`.
pub fn nested(n: usize, inner: Value) -> Value {
    (0..n).fold(inner, |v, _| Value::Array(vec![v]))
}

// ------------------------------------------------------------------------------------------------
// The independent byte walk (ADR-rdb-0012 Verification). It reads the bytes, not a `Value`, and
// checks the profile rules directly: every head is the shortest for its argument, no indefinite
// length, only `f4 f5 f6` and finite `fb` floats among the simple values, only tags 4 and 1001,
// text map keys strictly ascending by their encoded bytes, depth at most 64 for arrays and maps,
// and nothing after the item. It shares no code with `rdb_value::cbor`.
// ------------------------------------------------------------------------------------------------

/// `Ok(())` when `bytes` is exactly one item that obeys every profile rule above.
pub fn walk(bytes: &[u8]) -> Result<(), String> {
    let mut w = Walk { bytes, pos: 0 };
    w.item(0)?;
    if w.pos != bytes.len() {
        return Err(format!("{} bytes after the item", bytes.len() - w.pos));
    }
    Ok(())
}

struct Walk<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Walk<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], String> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| format!("truncated at {}", self.pos))?;
        let out = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(out)
    }

    /// One head: (major type, argument). Refuses indefinite and non-shortest heads.
    fn head(&mut self) -> Result<(u8, u64), String> {
        let at = self.pos;
        let first = self.take(1)?[0];
        let (major, info) = (first >> 5, first & 0x1f);
        let arg = match info {
            0..=23 => u64::from(info),
            24..=27 => {
                let width = 1_usize << (info - 24);
                let raw = self.take(width)?;
                let arg = raw.iter().fold(0_u64, |acc, b| (acc << 8) | u64::from(*b));
                // A float's argument is its bits, not a length: no shortest rule (and the profile
                // only allows the 8-byte form, checked by the caller).
                let floor = match info {
                    24 => 24,
                    25 => 0x100,
                    26 => 0x1_0000,
                    _ => 0x1_0000_0000,
                };
                if major != 7 && arg < floor {
                    return Err(format!("non-shortest head at {at}"));
                }
                arg
            }
            _ => return Err(format!("indefinite or reserved head {first:#04x} at {at}")),
        };
        Ok((major, arg))
    }

    fn item(&mut self, depth: usize) -> Result<(), String> {
        let at = self.pos;
        let first = *self.bytes.get(at).ok_or("truncated")?;
        let (major, arg) = self.head()?;
        match major {
            0 | 1 => Ok(()),
            2 | 3 => {
                let len = usize::try_from(arg).map_err(|_| "length past usize")?;
                let body = self.take(len)?;
                if major == 3 {
                    std::str::from_utf8(body).map_err(|_| format!("bad UTF-8 at {at}"))?;
                }
                Ok(())
            }
            4 => {
                if depth + 1 > 64 {
                    return Err(format!("array deeper than 64 at {at}"));
                }
                for _ in 0..arg {
                    self.item(depth + 1)?;
                }
                Ok(())
            }
            5 => {
                if depth + 1 > 64 {
                    return Err(format!("map deeper than 64 at {at}"));
                }
                let mut previous: Option<Vec<u8>> = None;
                for _ in 0..arg {
                    let key_at = self.pos;
                    if self.bytes.get(key_at).map(|b| b >> 5) != Some(3) {
                        return Err(format!("map key at {key_at} is not text"));
                    }
                    self.item(depth + 1)?;
                    let key = self.bytes[key_at..self.pos].to_vec();
                    if previous.as_ref().is_some_and(|p| *p >= key) {
                        return Err(format!("map key at {key_at} not strictly ascending"));
                    }
                    previous = Some(key);
                    self.item(depth + 1)?;
                }
                Ok(())
            }
            6 => match arg {
                4 => self.decimal(at),
                1001 => self.timestamp(at),
                other => Err(format!("tag {other} at {at}")),
            },
            _ => match first {
                0xf4..=0xf6 => Ok(()),
                0xfb => {
                    let bits = u64::from_be_bytes(self.bytes[at + 1..at + 9].try_into().unwrap());
                    if f64::from_bits(bits).is_finite() {
                        Ok(())
                    } else {
                        Err(format!("non-finite float at {at}"))
                    }
                }
                other => Err(format!("simple value or float {other:#04x} at {at}")),
            },
        }
    }

    /// Tag 4's content: `[exponent, mantissa]`, two integers, normalised.
    fn decimal(&mut self, at: usize) -> Result<(), String> {
        if self.head()? != (4, 2) {
            return Err(format!("tag 4 at {at} is not a 2-array"));
        }
        let exponent = self.integer()?;
        let mantissa = self.integer()?;
        if i64::try_from(exponent).is_err() {
            return Err(format!("decimal at {at}: exponent past i64"));
        }
        let normal = if mantissa == 0 {
            exponent == 0
        } else {
            mantissa % 10 != 0
        };
        normal
            .then_some(())
            .ok_or_else(|| format!("decimal at {at} not normalised"))
    }

    /// Tag 1001's content: `{1: secs}` or `{1: secs, -9: nanos}`, nanos 1 … 999,999,999.
    fn timestamp(&mut self, at: usize) -> Result<(), String> {
        let (major, entries) = self.head()?;
        if major != 5 || !(1..=2).contains(&entries) {
            return Err(format!("tag 1001 at {at} is not a 1- or 2-entry map"));
        }
        if self.integer()? != 1 {
            return Err(format!("tag 1001 at {at}: first key is not 1"));
        }
        let secs = self.integer()?;
        if i64::try_from(secs).is_err() {
            return Err(format!("tag 1001 at {at}: secs past i64"));
        }
        if entries == 2 {
            if self.integer()? != -9 {
                return Err(format!("tag 1001 at {at}: second key is not -9"));
            }
            let nanos = self.integer()?;
            if !(1..=999_999_999).contains(&nanos) {
                return Err(format!(
                    "tag 1001 at {at}: nanos {nanos} out of 1..=999999999"
                ));
            }
        }
        Ok(())
    }

    fn integer(&mut self) -> Result<i128, String> {
        match self.head()? {
            (0, arg) => Ok(i128::from(arg)),
            (1, arg) => Ok(-1 - i128::from(arg)),
            (major, _) => Err(format!("major {major} where an integer belongs")),
        }
    }
}

// ------------------------------------------------------------------------------------------------
// The second reader: `ciborium` decodes the bytes on its own, and a converter written here turns
// its value into ours. Run on accepted bytes only: it also accepts duplicate keys, bignums and
// deep nesting, so it cannot judge a refusal row (s2-design §5, R2-A2).
// ------------------------------------------------------------------------------------------------

/// What `ciborium` reads from `bytes`, as our `Value`. Panics when it reads something else, or
/// stops before the end: either is a disagreement between the two readers.
pub fn ciborium_reads(bytes: &[u8]) -> Value {
    let mut cursor = std::io::Cursor::new(bytes);
    let raw: ciborium::value::Value = ciborium::de::from_reader(&mut cursor).unwrap_or_else(|e| {
        panic!(
            "ciborium refused accepted bytes {}: {e}",
            hex::encode(bytes)
        )
    });
    assert_eq!(
        usize::try_from(cursor.position()).unwrap(),
        bytes.len(),
        "ciborium stopped early in {}",
        hex::encode(bytes)
    );
    from_ciborium(raw).unwrap_or_else(|| {
        panic!(
            "ciborium read a non-profile value from {}",
            hex::encode(bytes)
        )
    })
}

fn from_ciborium(raw: ciborium::value::Value) -> Option<Value> {
    use ciborium::value::Value as C;
    Some(match raw {
        C::Null => Value::Null,
        C::Bool(b) => Value::Bool(b),
        C::Integer(i) => Value::Integer(Int::new(i128::from(i))?),
        C::Float(f) => Value::Float(Float::new(f)?),
        C::Text(t) => Value::Text(t),
        C::Bytes(b) => Value::Bytes(b),
        C::Array(items) => Value::Array(
            items
                .into_iter()
                .map(from_ciborium)
                .collect::<Option<_>>()?,
        ),
        C::Map(entries) => {
            let mut m = Map::new();
            for (k, v) in entries {
                let C::Text(k) = k else { return None };
                if m.insert(MapKey::new(k), from_ciborium(v)?).is_some() {
                    return None;
                }
            }
            Value::Map(m)
        }
        C::Tag(4, inner) => {
            let C::Array(parts) = *inner else { return None };
            let [C::Integer(e), C::Integer(m)] = parts.as_slice() else {
                return None;
            };
            let e = i64::try_from(i128::from(*e)).ok()?;
            Value::Decimal(Decimal::new(e, Int::new(i128::from(*m))?)?)
        }
        C::Tag(1001, inner) => {
            let C::Map(entries) = *inner else { return None };
            let (mut secs, mut nanos) = (None, 0_u32);
            for (k, v) in entries {
                let (C::Integer(k), C::Integer(v)) = (k, v) else {
                    return None;
                };
                match i128::from(k) {
                    1 => secs = Some(i64::try_from(i128::from(v)).ok()?),
                    -9 => nanos = u32::try_from(i128::from(v)).ok()?,
                    _ => return None,
                }
            }
            Value::Timestamp(Timestamp::new(secs?, nanos)?)
        }
        _ => return None,
    })
}

// ------------------------------------------------------------------------------------------------
// Random documents: every type, the integer and length boundaries, small nesting.
// ------------------------------------------------------------------------------------------------

fn arb_int() -> impl Strategy<Value = Int> {
    let edges = [
        0_i128,
        23,
        24,
        255,
        256,
        65_535,
        65_536,
        (1 << 32) - 1,
        1 << 32,
        INT_MAX,
        -1,
        -24,
        -25,
        -256,
        -257,
        INT_MIN,
    ];
    prop_oneof![
        proptest::sample::select(edges.to_vec()),
        any::<i64>().prop_map(i128::from),
        any::<u64>().prop_map(i128::from),
        (INT_MIN..=INT_MAX),
    ]
    .prop_map(|v| Int::new(v).expect("in range"))
}

/// Any scalar: every type a map key or set member can be, with the integer edges.
pub fn arb_leaf() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        arb_int().prop_map(Value::Integer),
        any::<u64>()
            .prop_filter_map("finite", |bits| Float::new(f64::from_bits(bits)))
            .prop_map(Value::Float),
        (any::<i64>(), arb_int())
            .prop_filter_map("normalised", |(e, m)| Decimal::new(e, m))
            .prop_map(Value::Decimal),
        Just(decimal(0, 0)),
        (any::<i64>(), 0..=Timestamp::MAX_NANOS)
            .prop_map(|(s, n)| Value::Timestamp(Timestamp::new(s, n).expect("in range"))),
        "\\PC{0,30}".prop_map(Value::Text),
        proptest::collection::vec(any::<u8>(), 0..40).prop_map(Value::Bytes),
    ]
}

/// Any document: leaves, arrays and maps up to a few levels deep.
pub fn arb_value() -> impl Strategy<Value = Value> {
    arb_leaf().prop_recursive(4, 48, 6, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..6).prop_map(Value::Array),
            proptest::collection::btree_map("\\PC{0,8}", inner, 0..6).prop_map(|entries| {
                let mut m = Map::new();
                for (k, v) in entries {
                    m.insert(MapKey::new(k), v);
                }
                Value::Map(m)
            }),
        ]
    })
}
