//! The object-key layout (ADR-rdb-0013 decisions 1–6). This module is the only code that builds
//! the key of a record that belongs to an object.
//!
//! A `Namespace::User` key of an object's record is
//! `tenant u32 BE | affinity u64 BE | esc(object_id) | sub u8 | tail`:
//!
//! - [`esc`]: the object id with each `0x00` written `0x00 0xFF`, then the terminator `0x00 0x01`
//!   (decision 2).
//! - `sub`: the record's role inside the object, from the table in [`Sub`] (decision 3).
//! - `tail`: empty for the root; for a map entry or set member, the element key in profile v1
//!   (decision 4), built by [`encode_element`]; for a list item or block, its 16-byte id, and
//!   for a block's change slot one more byte, the slot (ADR-rdb-0016 §1).
//!
//! A [`RootKey`] can only be made by [`root_key`], so a document or collection op can never be
//! aimed at an element key (decision 1).

use bytes::Bytes;
use rdb_core::contracts::txn::{key_scope, scoped_key, KEY_SCOPE_LEN};
use rdb_core::{AffinityId, TenantId};

use crate::delta::ApplyError;
use crate::value::{Decimal, Float, Int, Timestamp, Value};

/// `sub` of an object's root record. Its tail is empty.
pub const SUB_ROOT: u8 = 0x00;
/// `sub` of a map entry or a set member. Its tail is an element key, profile v1.
pub const SUB_ELEMENT: u8 = 0x01;
/// `sub` of a list item. Its tail is the item id, [`LIST_ID_LEN`] bytes (ADR-rdb-0016 §1).
pub const SUB_ITEM: u8 = 0x02;
/// `sub` of a list block and its change slots. A block's tail is its id, [`LIST_ID_LEN`] bytes; a
/// slot's is the block id and then the slot, one byte below [`LIST_SLOTS`] (ADR-rdb-0016 §1).
pub const SUB_BLOCK: u8 = 0x03;
/// A list item or block id's length: a u128, big-endian (ADR-rdb-0016 §2).
pub const LIST_ID_LEN: usize = 16;
/// How many change slots a list block has: a slot byte is below it (ADR-rdb-0016 §3). A format
/// constant: it fixes keys.
pub const LIST_SLOTS: u8 = 240;
/// `sub` of a blob chunk. Its tail is `upload_id (16) | index u32 BE` (ADR-rdb-0014 §1).
pub const SUB_CHUNK: u8 = 0x04;
/// A chunk key's tail length: [`UPLOAD_LEN`] bytes of upload id, then a u32 index.
pub const CHUNK_TAIL_LEN: usize = UPLOAD_LEN + 4;
/// An upload id's length (ADR-rdb-0014 §1).
pub const UPLOAD_LEN: usize = 16;

/// The sub-key discriminator table (ADR-rdb-0013 decision 3; ADR-rdb-0011 O4). This module owns
/// it; a later slice adds its row here and in the ADR.
///
/// | `sub` | Record |
/// |---|---|
/// | `0x00` | [`Sub::Root`] |
/// | `0x01` | [`Sub::Element`] |
/// | `0x02` | [`Sub::Item`] (ADR-rdb-0016 §1) |
/// | `0x03` | [`Sub::Block`] (ADR-rdb-0016 §1) |
/// | `0x04` | [`Sub::Chunk`] (ADR-rdb-0014 §1) |
/// | `0x05`–`0xFF` | unassigned |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sub {
    /// `0x00`: the object's root record.
    Root,
    /// `0x01`: a map entry or a set member.
    Element,
    /// `0x02`: a list item.
    Item,
    /// `0x03`: a list block, or one of its change slots.
    Block,
    /// `0x04`: a blob chunk.
    Chunk,
    /// Any other byte: reserved or unassigned. Nothing after it is decoded.
    Reserved(u8),
}

impl Sub {
    /// The `sub` a byte names.
    #[must_use]
    pub const fn from_byte(byte: u8) -> Self {
        match byte {
            SUB_ROOT => Self::Root,
            SUB_ELEMENT => Self::Element,
            SUB_ITEM => Self::Item,
            SUB_BLOCK => Self::Block,
            SUB_CHUNK => Self::Chunk,
            other => Self::Reserved(other),
        }
    }
}

/// Element key type tags (ADR-rdb-0013 decision 4). Gaps are left for later types.
mod tag {
    pub const NULL: u8 = 0x10;
    pub const FALSE: u8 = 0x20;
    pub const TRUE: u8 = 0x21;
    pub const INT_NEG: u8 = 0x30;
    pub const INT_POS: u8 = 0x31;
    pub const FLOAT: u8 = 0x40;
    pub const DEC_NEG: u8 = 0x50;
    pub const DEC_ZERO: u8 = 0x51;
    pub const DEC_POS: u8 = 0x52;
    pub const TEXT: u8 = 0x60;
    pub const BYTES: u8 = 0x70;
    pub const TIMESTAMP: u8 = 0x80;
}

/// `2^63`, the bias of a decimal's adjusted exponent and of a timestamp's seconds.
const BIAS: i128 = 1 << 63;
/// The largest adjusted exponent a decimal can have: `i64::MAX + 20 − 1`.
const MAX_ADJUSTED: i128 = BIAS + 18;
/// A mantissa has at most 20 digits: `2^64` is 20 digits long.
const MAX_DIGITS: usize = 20;

/// Why stored key bytes do not decode. Element keys come only from storage (a client sends a
/// [`Value`]), so every one of these is damage.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    /// Shorter than the tenant and affinity prefix.
    #[error("key of {len} bytes is shorter than the {KEY_SCOPE_LEN}-byte scope")]
    ShortScope {
        /// The key's length.
        len: usize,
    },
    /// A `0x00` followed by something other than `0xFF` (an escaped zero) or `0x01` (the end).
    #[error("bad escape at byte {at}: 0x00 must be followed by 0xff or 0x01")]
    BadEscape {
        /// Offset of the `0x00` in the key.
        at: usize,
    },
    /// An escaped string with no `0x00 0x01` terminator.
    #[error("escaped string has no 0x00 0x01 terminator")]
    Unterminated,
    /// The object id is not followed by a `sub` byte.
    #[error("the object id is not followed by a sub byte")]
    MissingSub,
    /// The root record's key has bytes after its `sub`.
    #[error("root key has {len} bytes after its sub byte")]
    RootHasTail {
        /// The extra bytes.
        len: usize,
    },
    /// The element key's type tag is not in profile v1.
    #[error("unknown element key tag {0:#04x}")]
    UnknownTag(u8),
    /// The element key ends inside a fixed-width body.
    #[error("element key with tag {tag:#04x} is truncated")]
    Truncated {
        /// The type tag.
        tag: u8,
    },
    /// A text key is not UTF-8.
    #[error("text key is not UTF-8")]
    NotUtf8,
    /// A float key decodes to NaN or an infinity.
    #[error("float key is not finite")]
    NonFiniteFloat,
    /// A timestamp key's nanoseconds are over 999,999,999.
    #[error("timestamp key has {0} nanoseconds")]
    NanosOutOfRange(u32),
    /// Decimal check 1: no digits.
    #[error("decimal key has no digits")]
    DecimalNoDigits,
    /// Decimal check 2: a digit byte outside `0x01`–`0x0A`.
    #[error("decimal key has digit byte {0:#04x}")]
    DecimalDigitByte(u8),
    /// Decimal check 3: a leading `0` digit.
    #[error("decimal key has a leading zero digit")]
    DecimalLeadingZero,
    /// Decimal check 4: a trailing `0` digit, so the mantissa is divisible by 10.
    #[error("decimal key has a trailing zero digit")]
    DecimalTrailingZero,
    /// Decimal check 5: more than 20 digits, or a magnitude over 2^64−1 (positive) or 2^64
    /// (negative).
    #[error("decimal key's mantissa is out of range")]
    DecimalMantissaRange,
    /// Decimal check 6: the adjusted exponent is over 2^63 + 18, or the exponent is outside `i64`.
    #[error("decimal key's exponent is out of range")]
    DecimalExponentRange,
    /// Bytes are left after a complete element key.
    #[error("{len} bytes follow a complete element key")]
    TrailingBytes {
        /// The bytes left over.
        len: usize,
    },
    /// The element key decodes, but re-encodes to different bytes.
    #[error("element key does not re-encode to the stored bytes")]
    NotCanonical,
    /// The key is not under this object's element prefix.
    #[error("key is not an element of this object")]
    OutsideObject,
    /// A chunk key's tail is not [`CHUNK_TAIL_LEN`] bytes (ADR-rdb-0014 §1).
    #[error("chunk key tail of {len} bytes; a chunk tail is {CHUNK_TAIL_LEN}")]
    ChunkTail {
        /// The tail's length.
        len: usize,
    },
    /// A list item key's tail is not [`LIST_ID_LEN`] bytes, or a block key's is neither that nor
    /// one more, a slot (ADR-rdb-0016 §1).
    #[error(
        "list item or block key tail of {len} bytes; an id is {LIST_ID_LEN}, and a block's slot one more"
    )]
    ListIdTail {
        /// The tail's length.
        len: usize,
    },
    /// A list block's slot byte is not below [`LIST_SLOTS`] (ADR-rdb-0016 §1, §7).
    #[error("list block slot {slot}; a slot is below {LIST_SLOTS}")]
    SlotOutOfRange {
        /// The slot byte.
        slot: u8,
    },
}

/// An object's root key: the full `Namespace::User` key `scope | esc(object_id) | 0x00`.
///
/// Only [`root_key`] makes one. So no document or collection op can be aimed at an element key
/// or a chunk key: raw bytes do not convert.
///
/// ```compile_fail
/// let key = rdb_value::keys::RootKey(bytes::Bytes::from_static(b"user:1"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootKey(Bytes);

impl RootKey {
    /// The full storage key.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// The full storage key, as a cheap clone.
    #[must_use]
    pub fn to_bytes(&self) -> Bytes {
        self.0.clone()
    }

    /// The object prefix, `scope | esc(object_id)`: every record of this object starts with it,
    /// and no other record does (ADR-rdb-0013 decision 1).
    #[must_use]
    pub fn object_prefix(&self) -> &[u8] {
        &self.0[..self.0.len() - 1]
    }

    /// `object_prefix | 0x01`: every map entry or set member of this object starts with it.
    #[must_use]
    pub fn element_prefix(&self) -> Vec<u8> {
        let mut out = self.object_prefix().to_vec();
        out.push(SUB_ELEMENT);
        out
    }

    /// `object_prefix | sub`: every record of this object with that `sub` starts with it.
    #[must_use]
    pub fn sub_prefix(&self, sub: u8) -> Vec<u8> {
        let mut out = self.object_prefix().to_vec();
        out.push(sub);
        out
    }

    /// `object_prefix | 0x04`: every blob chunk of this object starts with it, in
    /// `(upload, index)` order (ADR-rdb-0014 §1).
    #[must_use]
    pub fn chunk_prefix(&self) -> Vec<u8> {
        let mut out = self.object_prefix().to_vec();
        out.push(SUB_CHUNK);
        out
    }
}

/// The key of chunk `index` of `upload` under the object at `root` (ADR-rdb-0014 §1).
#[must_use]
pub fn chunk_key(root: &RootKey, upload: &[u8; UPLOAD_LEN], index: u32) -> Bytes {
    let mut out = root.chunk_prefix();
    out.extend_from_slice(upload);
    out.extend_from_slice(&index.to_be_bytes());
    Bytes::from(out)
}

/// The key of list item `id` under the object at `root` (ADR-rdb-0016 §1).
#[must_use]
pub fn item_key(root: &RootKey, id: u128) -> Bytes {
    let mut out = root.sub_prefix(SUB_ITEM);
    out.extend_from_slice(&id.to_be_bytes());
    Bytes::from(out)
}

/// The key of list block `id`'s base under the object at `root` (ADR-rdb-0016 §1). Its change
/// slots' keys start with it, so they sort right after it.
#[must_use]
pub fn block_key(root: &RootKey, id: u128) -> Bytes {
    let mut out = root.sub_prefix(SUB_BLOCK);
    out.extend_from_slice(&id.to_be_bytes());
    Bytes::from(out)
}

/// The key of change slot `slot` of list block `id` under the object at `root`
/// (ADR-rdb-0016 §1). Callers pass a slot below [`LIST_SLOTS`].
#[must_use]
pub fn slot_key(root: &RootKey, id: u128, slot: u8) -> Bytes {
    debug_assert!(slot < LIST_SLOTS, "a slot is below LIST_SLOTS");
    let mut out = root.sub_prefix(SUB_BLOCK);
    out.extend_from_slice(&id.to_be_bytes());
    out.push(slot);
    Bytes::from(out)
}

/// The root key of `object_id` in `(tenant, affinity)`. Any byte string is an id, the empty one
/// included.
#[must_use]
pub fn root_key(tenant: TenantId, affinity: AffinityId, object_id: &[u8]) -> RootKey {
    let mut user_key = Vec::with_capacity(object_id.len() + 3);
    esc(object_id, &mut user_key);
    user_key.push(SUB_ROOT);
    RootKey(scoped_key(tenant, affinity, &user_key))
}

/// Append `bytes` escaped to `out`: each `0x00` as `0x00 0xFF`, then `0x00 0x01`
/// (ADR-rdb-0013 decision 2). Prefix-free and order-preserving.
pub fn esc(bytes: &[u8], out: &mut Vec<u8>) {
    for &b in bytes {
        out.push(b);
        if b == 0 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x01]);
}

/// Undo [`esc`] at the start of `input`. Returns the bytes and the rest after the terminator.
/// `base` is `input`'s offset in the whole key, for the error.
fn unesc(input: &[u8], base: usize) -> Result<(Vec<u8>, &[u8]), KeyError> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < input.len() {
        let b = input[i];
        if b != 0 {
            out.push(b);
            i += 1;
            continue;
        }
        match input.get(i + 1) {
            Some(0xFF) => out.push(0),
            Some(0x01) => return Ok((out, &input[i + 2..])),
            Some(_) => return Err(KeyError::BadEscape { at: base + i }),
            None => return Err(KeyError::Unterminated),
        }
        i += 2;
    }
    Err(KeyError::Unterminated)
}

/// The element key of `key` in profile v1 (ADR-rdb-0013 decision 4): a type tag, then a body.
///
/// # Errors
/// [`ApplyError::UnsupportedKeyType`] for an array or a map.
pub fn encode_element(key: &Value) -> Result<Vec<u8>, ApplyError> {
    let mut out = Vec::new();
    match key {
        Value::Null => out.push(tag::NULL),
        Value::Bool(b) => out.push(if *b { tag::TRUE } else { tag::FALSE }),
        Value::Integer(i) => encode_int(*i, &mut out),
        Value::Float(f) => {
            let bits = f.get().to_bits();
            let flipped = if bits >> 63 == 1 {
                !bits
            } else {
                bits ^ (1 << 63)
            };
            out.push(tag::FLOAT);
            out.extend_from_slice(&flipped.to_be_bytes());
        }
        Value::Decimal(d) => encode_decimal(*d, &mut out),
        Value::Text(t) => {
            out.push(tag::TEXT);
            esc(t.as_bytes(), &mut out);
        }
        Value::Bytes(b) => {
            out.push(tag::BYTES);
            esc(b, &mut out);
        }
        Value::Timestamp(t) => {
            out.push(tag::TIMESTAMP);
            // Two's complement, sign bit flipped: i64 order becomes u64 order.
            let biased = u64::from_be_bytes(t.secs().to_be_bytes()) ^ (1 << 63);
            out.extend_from_slice(&biased.to_be_bytes());
            out.extend_from_slice(&t.nanos().to_be_bytes());
        }
        Value::Array(_) | Value::Map(_) => return Err(ApplyError::UnsupportedKeyType),
    }
    Ok(out)
}

fn encode_int(i: Int, out: &mut Vec<u8>) {
    let v = i.get();
    if v < 0 {
        // v = −1 − m with m in 0 … 2^64−1 (CBOR major type 1's argument); NOT m sorts by v.
        let m = u64::try_from(-1 - v).expect("Int is at least -2^64");
        out.push(tag::INT_NEG);
        out.extend_from_slice(&(!m).to_be_bytes());
    } else {
        let v = u64::try_from(v).expect("Int is at most 2^64-1");
        out.push(tag::INT_POS);
        out.extend_from_slice(&v.to_be_bytes());
    }
}

fn encode_decimal(d: Decimal, out: &mut Vec<u8>) {
    let m = d.mantissa().get();
    if m == 0 {
        out.push(tag::DEC_ZERO);
        return;
    }
    let digits = m.unsigned_abs().to_string();
    let len = i128::try_from(digits.len()).expect("at most 20 digits");
    let adjusted = i128::from(d.exponent()) + len - 1;
    let biased = u128::try_from(adjusted + BIAS).expect("adjusted >= -2^63");
    let mut body = biased.to_be_bytes()[7..].to_vec();
    body.extend(digits.bytes().map(|c| c - b'0' + 1));
    body.push(0x00);
    if m > 0 {
        out.push(tag::DEC_POS);
        out.extend_from_slice(&body);
    } else {
        out.push(tag::DEC_NEG);
        out.extend(body.iter().map(|b| !b));
    }
}

/// Decode a whole element key (tag and body, nothing after it), strictly: decode, re-encode,
/// compare with the input. Anything the encoder never writes is refused.
///
/// # Errors
/// A [`KeyError`] naming the fault.
pub fn decode_element(bytes: &[u8]) -> Result<Value, KeyError> {
    let (value, rest) = decode_one(bytes)?;
    if !rest.is_empty() {
        return Err(KeyError::TrailingBytes { len: rest.len() });
    }
    let again = encode_element(&value).map_err(|_| KeyError::NotCanonical)?;
    if again != bytes {
        return Err(KeyError::NotCanonical);
    }
    Ok(value)
}

/// `N` bytes after the tag, and the rest.
fn fixed<const N: usize>(body: &[u8], tag: u8) -> Result<([u8; N], &[u8]), KeyError> {
    let head = body.get(..N).ok_or(KeyError::Truncated { tag })?;
    let mut out = [0; N];
    out.copy_from_slice(head);
    Ok((out, &body[N..]))
}

fn decode_one(bytes: &[u8]) -> Result<(Value, &[u8]), KeyError> {
    let (&t, body) = bytes.split_first().ok_or(KeyError::Truncated { tag: 0 })?;
    Ok(match t {
        tag::NULL => (Value::Null, body),
        tag::FALSE => (Value::Bool(false), body),
        tag::TRUE => (Value::Bool(true), body),
        tag::INT_NEG => {
            let (x, rest) = fixed::<8>(body, t)?;
            let m = !u64::from_be_bytes(x);
            let v = Int::new(-1 - i128::from(m)).expect("−1 − u64 is in range");
            (Value::Integer(v), rest)
        }
        tag::INT_POS => {
            let (x, rest) = fixed::<8>(body, t)?;
            (Value::Integer(Int::from(u64::from_be_bytes(x))), rest)
        }
        tag::FLOAT => {
            let (x, rest) = fixed::<8>(body, t)?;
            let stored = u64::from_be_bytes(x);
            let bits = if stored >> 63 == 1 {
                stored ^ (1 << 63)
            } else {
                !stored
            };
            let f = Float::new(f64::from_bits(bits)).ok_or(KeyError::NonFiniteFloat)?;
            (Value::Float(f), rest)
        }
        tag::DEC_ZERO => {
            let zero = Decimal::new(0, Int::from(0_u64)).expect("[0, 0] is normalised");
            (Value::Decimal(zero), body)
        }
        tag::DEC_POS | tag::DEC_NEG => decode_decimal(t, body)?,
        tag::TEXT => {
            let (raw, rest) = unesc(body, 1)?;
            let text = String::from_utf8(raw).map_err(|_| KeyError::NotUtf8)?;
            (Value::Text(text), rest)
        }
        tag::BYTES => {
            let (raw, rest) = unesc(body, 1)?;
            (Value::Bytes(raw), rest)
        }
        tag::TIMESTAMP => {
            let (s, rest) = fixed::<8>(body, t)?;
            let (n, rest) = fixed::<4>(rest, t)?;
            let secs = i64::from_be_bytes((u64::from_be_bytes(s) ^ (1 << 63)).to_be_bytes());
            let nanos = u32::from_be_bytes(n);
            let ts = Timestamp::new(secs, nanos).ok_or(KeyError::NanosOutOfRange(nanos))?;
            (Value::Timestamp(ts), rest)
        }
        other => return Err(KeyError::UnknownTag(other)),
    })
}

/// A decimal body with its six named checks (ADR-rdb-0013 decision 4). Each check gives its
/// fault a name. It is not always the only step that refuses the fault: without the
/// leading-zero check, for one, the re-encode compare still refuses that body, as
/// `NotCanonical`.
fn decode_decimal(t: u8, body: &[u8]) -> Result<(Value, &[u8]), KeyError> {
    let negative = t == tag::DEC_NEG;
    let byte = |b: u8| if negative { !b } else { b };
    let (a, mut rest) = fixed::<9>(body, t)?;
    let mut wide = [0_u8; 16];
    for (slot, b) in wide[7..].iter_mut().zip(a) {
        *slot = byte(b);
    }
    let adjusted = i128::try_from(u128::from_be_bytes(wide)).expect("72 bits") - BIAS;
    let mut digits: Vec<u8> = Vec::new();
    loop {
        let (&raw, tail) = rest.split_first().ok_or(KeyError::Truncated { tag: t })?;
        rest = tail;
        match byte(raw) {
            0x00 => break,
            d @ 0x01..=0x0A => {
                // Check 5, first half: more than 20 digits. Checked as they are read, so a long
                // run of digit bytes is refused without building a number.
                if digits.len() == MAX_DIGITS {
                    return Err(KeyError::DecimalMantissaRange);
                }
                digits.push(d - 1);
            }
            // Check 2: a digit byte outside 0x01–0x0A.
            d => return Err(KeyError::DecimalDigitByte(d)),
        }
    }
    // Checks 1, 3, 4.
    match (digits.first(), digits.last()) {
        (None, _) => return Err(KeyError::DecimalNoDigits),
        (Some(0), _) => return Err(KeyError::DecimalLeadingZero),
        (_, Some(0)) => return Err(KeyError::DecimalTrailingZero),
        _ => {}
    }
    // Check 5, second half: the magnitude. At most 20 digits, so it fits u128.
    let magnitude = digits
        .iter()
        .fold(0_u128, |acc, d| acc * 10 + u128::from(*d));
    let limit = if negative {
        1_u128 << 64
    } else {
        (1_u128 << 64) - 1
    };
    if magnitude > limit {
        return Err(KeyError::DecimalMantissaRange);
    }
    // Check 6: the adjusted exponent, then the exponent, in i128.
    if adjusted > MAX_ADJUSTED {
        return Err(KeyError::DecimalExponentRange);
    }
    let count = i128::try_from(digits.len()).expect("at most 20");
    let exponent =
        i64::try_from(adjusted - count + 1).map_err(|_| KeyError::DecimalExponentRange)?;
    // Built through the constructors, which enforce checks 4 and 5 again.
    let signed = i128::try_from(magnitude).expect("at most 2^64");
    let mantissa =
        Int::new(if negative { -signed } else { signed }).ok_or(KeyError::DecimalMantissaRange)?;
    let decimal = Decimal::new(exponent, mantissa).ok_or(KeyError::DecimalTrailingZero)?;
    Ok((Value::Decimal(decimal), rest))
}

/// The full key of the element `key` of the object at `root`.
///
/// # Errors
/// [`ApplyError::UnsupportedKeyType`] for an array or a map.
pub fn element_key(root: &RootKey, key: &Value) -> Result<Bytes, ApplyError> {
    let mut out = root.element_prefix();
    out.extend_from_slice(&encode_element(key)?);
    Ok(Bytes::from(out))
}

/// The element a full key names, if the key is under `root`'s element prefix.
///
/// # Errors
/// [`KeyError::OutsideObject`], or the element key's own [`KeyError`].
pub fn decode_element_key(root: &RootKey, key: &[u8]) -> Result<Value, KeyError> {
    let tail = key
        .strip_prefix(root.element_prefix().as_slice())
        .ok_or(KeyError::OutsideObject)?;
    decode_element(tail)
}

/// A `Namespace::User` key of an object's record, taken apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parsed {
    /// The tenant.
    pub tenant: TenantId,
    /// The affinity group.
    pub affinity: AffinityId,
    /// The object id, unescaped.
    pub object_id: Vec<u8>,
    /// The record's role.
    pub sub: Sub,
    /// For [`Sub::Element`], the decoded element key. `None` otherwise.
    pub element: Option<Value>,
    /// For [`Sub::Chunk`], the upload id and the index. `None` otherwise.
    pub chunk: Option<([u8; UPLOAD_LEN], u32)>,
    /// For [`Sub::Item`] and [`Sub::Block`], the id. `None` otherwise.
    pub list_id: Option<u128>,
    /// For a [`Sub::Block`] change slot, the slot. `None` otherwise, a block's base included.
    pub slot: Option<u8>,
}

impl Parsed {
    /// The root key of the object this record belongs to.
    #[must_use]
    pub fn root(&self) -> RootKey {
        root_key(self.tenant, self.affinity, &self.object_id)
    }
}

/// Take apart an object record's full key. A reserved `sub` is reported, and nothing after it is
/// decoded.
///
/// # Errors
/// A [`KeyError`] naming the first fault.
pub fn parse(key: &[u8]) -> Result<Parsed, KeyError> {
    let (tenant, affinity) = key_scope(key).ok_or(KeyError::ShortScope { len: key.len() })?;
    let (object_id, rest) = unesc(&key[KEY_SCOPE_LEN..], KEY_SCOPE_LEN)?;
    let (&sub, tail) = rest.split_first().ok_or(KeyError::MissingSub)?;
    let sub = Sub::from_byte(sub);
    let (mut element, mut chunk, mut list_id, mut slot) = (None, None, None, None);
    match sub {
        Sub::Root if !tail.is_empty() => return Err(KeyError::RootHasTail { len: tail.len() }),
        Sub::Element => element = Some(decode_element(tail)?),
        Sub::Chunk => {
            let tail: &[u8; CHUNK_TAIL_LEN] = tail
                .try_into()
                .map_err(|_| KeyError::ChunkTail { len: tail.len() })?;
            let (upload, index) = tail.split_at(UPLOAD_LEN);
            chunk = Some((
                upload.try_into().expect("split at UPLOAD_LEN"),
                u32::from_be_bytes(index.try_into().expect("4 bytes")),
            ));
        }
        Sub::Item | Sub::Block => {
            let (id, rest) = match (sub, tail.len()) {
                (_, LIST_ID_LEN) => (tail, None),
                (Sub::Block, len) if len == LIST_ID_LEN + 1 => {
                    (&tail[..LIST_ID_LEN], Some(tail[LIST_ID_LEN]))
                }
                (_, len) => return Err(KeyError::ListIdTail { len }),
            };
            if let Some(byte) = rest {
                if byte >= LIST_SLOTS {
                    return Err(KeyError::SlotOutOfRange { slot: byte });
                }
            }
            list_id = Some(u128::from_be_bytes(
                id.try_into().expect("LIST_ID_LEN bytes"),
            ));
            slot = rest;
        }
        Sub::Root | Sub::Reserved(_) => {}
    }
    Ok(Parsed {
        tenant,
        affinity,
        object_id,
        sub,
        element,
        chunk,
        list_id,
        slot,
    })
}
