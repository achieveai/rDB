//! The deterministic CBOR profile: a canonical [`encode`] and one strict [`decode`]
//! (ADR-rdb-0012 decisions 3–5).
//!
//! `decode` refuses anything [`encode`] would not have written. It never repairs. The steps, in
//! order (decision 4):
//! 1. size: over [`MAX_PAYLOAD`] is [`CodecError::TooLarge`], before anything is decoded;
//! 2. decode one item with `cbor4ii` (its depth overflow is [`CodecError::TooDeep`]);
//! 3. any byte after that item is [`CodecError::TrailingBytes`] (the library ignores them);
//! 4. convert, with named checks for what re-encodes byte-identically and would otherwise slip
//!    through (duplicate keys, NaN and ±Inf, nesting past [`MAX_DEPTH`]), and for the shapes of
//!    the two allowed tags;
//! 5. re-encode and compare with the **whole** input: any difference is
//!    [`CodecError::NonCanonical`] (long heads, indefinite lengths, unsorted keys, f32 floats,
//!    `undefined`). The pinned library cannot read f16 without its `half` feature, so an f16
//!    float is [`CodecError::Malformed`]: refused either way.
//!
//! Decimals (tag 4) and timestamps (tag 1001) are scalars of the data model: their inner array
//! or map does not count toward [`MAX_DEPTH`].

use std::convert::Infallible;

use cbor4ii::core::dec::{self, Decode as _};
use cbor4ii::core::enc::{self, Encode as _};
use cbor4ii::core::error::DecodeError;
use cbor4ii::core::{types, Value as Raw};

use crate::envelope::LIMIT_TEXT;
pub use crate::envelope::MAX_PAYLOAD;
use crate::value::{Decimal, Float, Int, Map, MapKey, Timestamp, Value};

/// The deepest nesting of maps and arrays the profile allows (decision 5).
/// A map or array of scalars is depth 1.
pub const MAX_DEPTH: usize = 64;

/// The library's recursion budget. It spends two steps per container, so this refuses at about
/// 128 levels, well past [`MAX_DEPTH`]: nesting 65–127 decodes, and our own check refuses it.
const LIBRARY_STEPS: usize = 256;

/// Why bytes from a client (or, wrapped in `Corrupt`, from storage) are refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    /// Not one well-formed CBOR item.
    #[error("malformed CBOR near byte {offset}")]
    Malformed {
        /// Where the reader stopped.
        offset: usize,
    },
    /// Bytes follow the first item.
    #[error("trailing bytes from byte {offset}")]
    TrailingBytes {
        /// The first byte after the item.
        offset: usize,
    },
    /// Well-formed, but not the bytes [`encode`] would write.
    #[error("non-canonical CBOR: first difference at byte {offset}")]
    NonCanonical {
        /// The first byte that differs from the canonical encoding.
        offset: usize,
    },
    /// A map holds the same key twice.
    #[error("duplicate map key")]
    DuplicateKey,
    /// A map key is not text.
    #[error("map key is not text")]
    NonTextKey,
    /// A tag this profile does not allow.
    #[error("unsupported tag {0}")]
    UnsupportedTag(u64),
    /// A float that is NaN (any payload) or ±infinity.
    #[error("float is NaN or infinite")]
    NonFiniteFloat,
    /// Tag 4 that is not `[exponent, mantissa]` with an i64 exponent, a plain integer mantissa,
    /// and the normalised form (mantissa not divisible by 10; zero is `[0, 0]`).
    #[error("tag 4 is not a normalised [exponent, mantissa] decimal")]
    InvalidDecimal,
    /// Tag 1001 that is not `{1: secs}` or `{1: secs, -9: nanos}` with an i64 `secs` and
    /// `nanos` in 1 … 999,999,999.
    #[error("tag 1001 is not a {{1: secs, -9: nanos}} timestamp")]
    InvalidTimestamp,
    /// Input over [`MAX_PAYLOAD`].
    #[error("input over the {LIMIT_TEXT} limit")]
    TooLarge,
    /// Nesting past [`MAX_DEPTH`].
    #[error("nested deeper than {MAX_DEPTH}")]
    TooDeep,
}

/// Why a value cannot be written. `encode` refuses what `decode` would refuse, so an accepted
/// write always reads back (decision 11).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    /// Nesting past [`MAX_DEPTH`].
    #[error("nested deeper than {MAX_DEPTH}")]
    TooDeep,
    /// The encoding would be over [`MAX_PAYLOAD`].
    #[error("encoding over the {LIMIT_TEXT} limit")]
    TooLarge,
}

/// The canonical bytes of `value`.
///
/// # Errors
/// [`EncodeError::TooDeep`] or [`EncodeError::TooLarge`]; nothing partial is returned.
pub fn encode(value: &Value) -> Result<Vec<u8>, EncodeError> {
    let (bytes, written) = encode_capped(value, MAX_PAYLOAD);
    written.map(|()| bytes)
}

/// The canonical bytes of `value`, stopping at `cap` bytes. On `TooLarge` the buffer holds
/// exactly the first `cap` bytes of the canonical form.
fn encode_capped(value: &Value, cap: usize) -> (Vec<u8>, Result<(), EncodeError>) {
    let mut out = CappedWriter {
        buf: Vec::new(),
        cap,
    };
    let written = write_value(value, 0, &mut out);
    (out.buf, written)
}

/// The one strict decoder, for client bytes and stored bytes alike.
///
/// # Errors
/// The first [`CodecError`] in the order the module docs give.
pub fn decode(input: &[u8]) -> Result<Value, CodecError> {
    if input.len() > MAX_PAYLOAD {
        return Err(CodecError::TooLarge);
    }
    let mut reader = Reader {
        buf: input,
        pos: 0,
        steps: LIBRARY_STEPS,
    };
    let raw = match Raw::decode(&mut reader) {
        Ok(raw) => raw,
        Err(DecodeError::DepthOverflow { .. }) => return Err(CodecError::TooDeep),
        Err(_) => return Err(CodecError::Malformed { offset: reader.pos }),
    };
    if reader.pos != input.len() {
        return Err(CodecError::TrailingBytes { offset: reader.pos });
    }
    let value = convert(raw, 0)?;
    // Encode no further than the input's own length: a canonical form that is longer differs
    // anyway, and the cap keeps the compare from building more than it reads.
    let (canonical, written) = encode_capped(&value, input.len());
    match written {
        Ok(()) => match first_difference(input, &canonical) {
            None => Ok(value),
            Some(offset) => Err(CodecError::NonCanonical { offset }),
        },
        Err(EncodeError::TooLarge) => Err(CodecError::NonCanonical {
            offset: first_difference(input, &canonical).unwrap_or(input.len()),
        }),
        // `convert` already refused this depth, so this arm is not reached; if it were, the
        // value really is too deep.
        Err(EncodeError::TooDeep) => Err(CodecError::TooDeep),
    }
}

fn first_difference(a: &[u8], b: &[u8]) -> Option<usize> {
    a.iter()
        .zip(b)
        .position(|(x, y)| x != y)
        .or_else(|| (a.len() != b.len()).then(|| a.len().min(b.len())))
}

/// `depth` is the number of containers around `raw`.
fn convert(raw: Raw, depth: usize) -> Result<Value, CodecError> {
    match raw {
        // The library builds `Integer` only from a major-0 or major-1 argument, so it is always
        // in range at the pinned 1.2.3. `None` would mean the library changed under the pin.
        Raw::Integer(v) => Int::new(v)
            .map(Value::Integer)
            .ok_or(CodecError::Malformed { offset: 0 }),
        Raw::Text(text) => Ok(Value::Text(text)),
        // The library reads `f7` (undefined) as null too; the compare refuses it.
        Raw::Null => Ok(Value::Null),
        Raw::Bool(b) => Ok(Value::Bool(b)),
        // f32 input arrives widened to f64; the compare refuses it after this check.
        Raw::Float(f) => Float::new(f)
            .map(Value::Float)
            .ok_or(CodecError::NonFiniteFloat),
        Raw::Bytes(bytes) => Ok(Value::Bytes(bytes)),
        Raw::Array(items) => {
            if depth + 1 > MAX_DEPTH {
                return Err(CodecError::TooDeep);
            }
            items
                .into_iter()
                .map(|item| convert(item, depth + 1))
                .collect::<Result<_, _>>()
                .map(Value::Array)
        }
        Raw::Map(entries) => {
            if depth + 1 > MAX_DEPTH {
                return Err(CodecError::TooDeep);
            }
            let mut map = Map::new();
            for (key, value) in entries {
                let Raw::Text(key) = key else {
                    return Err(CodecError::NonTextKey);
                };
                let value = convert(value, depth + 1)?;
                if map.insert(MapKey::new(key), value).is_some() {
                    return Err(CodecError::DuplicateKey);
                }
            }
            Ok(Value::Map(map))
        }
        Raw::Tag(TAG_DECIMAL, inner) => decimal(*inner)
            .map(Value::Decimal)
            .ok_or(CodecError::InvalidDecimal),
        Raw::Tag(TAG_TIMESTAMP, inner) => timestamp(*inner)
            .map(Value::Timestamp)
            .ok_or(CodecError::InvalidTimestamp),
        Raw::Tag(tag, _) => Err(CodecError::UnsupportedTag(tag)),
        // `Value` is `#[non_exhaustive]`; 1.2.3 has no other variant. One would mean the library
        // changed under the pin.
        _ => Err(CodecError::Malformed { offset: 0 }),
    }
}

/// Tag 4, decimal fraction (RFC 8949 §3.4.4).
const TAG_DECIMAL: u64 = 4;
/// Tag 1001, extended time (RFC 9581).
const TAG_TIMESTAMP: u64 = 1001;
/// RFC 9581 key 1: seconds from the epoch.
const TS_SECS: i128 = 1;
/// RFC 9581 key −9: nanoseconds.
const TS_NANOS: i128 = -9;

/// `[exponent, mantissa]`: an i64 exponent and a plain integer mantissa (a bignum tag is not an
/// `Integer`), in normalised form.
fn decimal(inner: Raw) -> Option<Decimal> {
    let Raw::Array(parts) = inner else {
        return None;
    };
    let [Raw::Integer(exponent), Raw::Integer(mantissa)] = parts.as_slice() else {
        return None;
    };
    Decimal::new(i64::try_from(*exponent).ok()?, Int::new(*mantissa)?)
}

/// `{1: secs}` or `{1: secs, -9: nanos}`, nanos in 1 … 999,999,999. No other key, no repeat.
/// Key order is left to the compare.
fn timestamp(inner: Raw) -> Option<Timestamp> {
    let Raw::Map(entries) = inner else {
        return None;
    };
    let (mut secs, mut nanos) = (None, None);
    for (key, value) in entries {
        let (Raw::Integer(key), Raw::Integer(value)) = (key, value) else {
            return None;
        };
        let slot = match key {
            TS_SECS => &mut secs,
            TS_NANOS => &mut nanos,
            _ => return None,
        };
        if slot.replace(value).is_some() {
            return None;
        }
    }
    let secs = i64::try_from(secs?).ok()?;
    let nanos = match nanos {
        None => 0,
        Some(n) => u32::try_from(n).ok().filter(|n| *n != 0)?,
    };
    Timestamp::new(secs, nanos)
}

/// `depth` is the number of containers around `value`.
fn write_value(value: &Value, depth: usize, out: &mut CappedWriter) -> Result<(), EncodeError> {
    match value {
        Value::Null => types::Null.encode(out).map_err(too_large),
        Value::Bool(b) => b.encode(out).map_err(too_large),
        Value::Integer(v) => v.get().encode(out).map_err(too_large),
        // Always `fb` + 8 bytes: the library never shortens an `f64`.
        Value::Float(f) => f.get().encode(out).map_err(too_large),
        Value::Decimal(d) => {
            types::Tag(TAG_DECIMAL, types::Nothing)
                .encode(out)
                .map_err(too_large)?;
            types::Array::<()>::bounded(2, out).map_err(too_large)?;
            d.exponent().encode(out).map_err(too_large)?;
            d.mantissa().get().encode(out).map_err(too_large)
        }
        Value::Timestamp(t) => {
            types::Tag(TAG_TIMESTAMP, types::Nothing)
                .encode(out)
                .map_err(too_large)?;
            // Key 1 (`01`) sorts before key −9 (`28`) by encoded bytes.
            let entries = if t.nanos() == 0 { 1 } else { 2 };
            types::Map::<()>::bounded(entries, out).map_err(too_large)?;
            TS_SECS.encode(out).map_err(too_large)?;
            t.secs().encode(out).map_err(too_large)?;
            if t.nanos() != 0 {
                TS_NANOS.encode(out).map_err(too_large)?;
                t.nanos().encode(out).map_err(too_large)?;
            }
            Ok(())
        }
        Value::Text(text) => text.as_str().encode(out).map_err(too_large),
        Value::Bytes(bytes) => types::Bytes(bytes.as_slice())
            .encode(out)
            .map_err(too_large),
        Value::Array(items) => {
            if depth + 1 > MAX_DEPTH {
                return Err(EncodeError::TooDeep);
            }
            types::Array::<()>::bounded(items.len(), out).map_err(too_large)?;
            for item in items {
                write_value(item, depth + 1, out)?;
            }
            Ok(())
        }
        Value::Map(map) => {
            if depth + 1 > MAX_DEPTH {
                return Err(EncodeError::TooDeep);
            }
            types::Map::<()>::bounded(map.len(), out).map_err(too_large)?;
            for (key, value) in map.iter() {
                key.as_str().encode(out).map_err(too_large)?;
                write_value(value, depth + 1, out)?;
            }
            Ok(())
        }
    }
}

/// The writer's only failure is the size cap. `enc::Error` is `#[non_exhaustive]` and has one
/// variant, `Write`, at the pinned 1.2.3.
fn too_large(_: enc::Error<OverCap>) -> EncodeError {
    EncodeError::TooLarge
}

/// The cap was reached.
#[derive(Debug)]
struct OverCap;

impl std::fmt::Display for OverCap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "encoding over its byte cap")
    }
}

impl std::error::Error for OverCap {}

/// A `Vec` that refuses to grow past `cap`, so an oversized value fails early instead of being
/// built in full first. On refusal it keeps the part that fits, for the compare's offset.
struct CappedWriter {
    buf: Vec<u8>,
    cap: usize,
}

impl enc::Write for CappedWriter {
    type Error = OverCap;

    fn push(&mut self, input: &[u8]) -> Result<(), OverCap> {
        let room = self.cap - self.buf.len();
        if input.len() > room {
            self.buf.extend_from_slice(&input[..room]);
            return Err(OverCap);
        }
        self.buf.extend_from_slice(input);
        Ok(())
    }
}

/// A slice reader that knows its position, for the offsets in [`CodecError`].
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
    steps: usize,
}

impl<'de> dec::Read<'de> for Reader<'de> {
    type Error = Infallible;

    fn fill<'short>(
        &'short mut self,
        want: usize,
    ) -> Result<dec::Reference<'de, 'short>, Infallible> {
        let rest: &'de [u8] = &self.buf[self.pos..];
        Ok(dec::Reference::Long(&rest[..want.min(rest.len())]))
    }

    fn advance(&mut self, n: usize) {
        self.pos += n.min(self.buf.len() - self.pos);
    }

    fn step_in(&mut self) -> bool {
        match self.steps.checked_sub(1) {
            Some(left) => {
                self.steps = left;
                true
            }
            None => false,
        }
    }

    fn step_out(&mut self) {
        self.steps += 1;
    }
}
