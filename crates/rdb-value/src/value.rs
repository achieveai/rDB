//! The document data model (ADR-rdb-0012 decision 2).
//!
//! A map's keys are text, and the map keeps them in **encoded order**: byte length first, then
//! bytes. So `"name"` sorts before `"email"`, and encoding needs no extra sort (decision 3).
//! There is no cross-type equality: `1`, `1.0` and decimal `[0, 1]` are three different values.
//! Every type here can only hold what the profile allows: an out-of-range integer, a non-finite
//! float, a non-normalised decimal or a bad timestamp cannot be built.

use std::cmp::Ordering;
use std::collections::BTreeMap;

/// The smallest integer the profile holds: −2^64 (CBOR major type 1 with argument 2^64−1).
pub const INT_MIN: i128 = -(1_i128 << 64);
/// The largest integer the profile holds: 2^64−1 (CBOR major type 0).
pub const INT_MAX: i128 = (1_i128 << 64) - 1;

/// An integer in the full CBOR range, −2^64 … 2^64−1. Out-of-range values cannot be built.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Int(i128);

impl Int {
    /// `v` if it is inside [`INT_MIN`] … [`INT_MAX`], else `None`.
    #[must_use]
    pub const fn new(v: i128) -> Option<Self> {
        if v >= INT_MIN && v <= INT_MAX {
            Some(Self(v))
        } else {
            None
        }
    }

    /// The value.
    #[must_use]
    pub const fn get(self) -> i128 {
        self.0
    }

    /// `self + other`, or `None` when the sum leaves the CBOR range.
    #[must_use]
    pub fn checked_add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).and_then(Self::new)
    }
}

impl From<u64> for Int {
    fn from(v: u64) -> Self {
        Self(i128::from(v))
    }
}

impl From<i64> for Int {
    fn from(v: i64) -> Self {
        Self(i128::from(v))
    }
}

/// A finite binary64 float. NaN (any payload) and ±infinity cannot be built. Equality is by
/// bits, so `-0.0` and `0.0` are different values, and `-0.0` is kept as written.
#[derive(Debug, Clone, Copy)]
pub struct Float(f64);

impl Float {
    /// `v` if it is finite, else `None`.
    #[must_use]
    pub fn new(v: f64) -> Option<Self> {
        v.is_finite().then_some(Self(v))
    }

    /// The value.
    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }
}

impl PartialEq for Float {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for Float {}

/// A decimal fraction `mantissa × 10^exponent` (RFC 8949 §3.4.4), normalised: the mantissa is
/// not divisible by 10, and zero is `[0, 0]`. So each number has exactly one form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal {
    exponent: i64,
    mantissa: Int,
}

impl Decimal {
    /// The decimal, if `(exponent, mantissa)` is normalised.
    #[must_use]
    pub const fn new(exponent: i64, mantissa: Int) -> Option<Self> {
        let normal = if mantissa.0 == 0 {
            exponent == 0
        } else {
            mantissa.0 % 10 != 0
        };
        if normal {
            Some(Self { exponent, mantissa })
        } else {
            None
        }
    }

    /// The power of ten.
    #[must_use]
    pub const fn exponent(self) -> i64 {
        self.exponent
    }

    /// The mantissa.
    #[must_use]
    pub const fn mantissa(self) -> Int {
        self.mantissa
    }
}

/// A point in time (RFC 9581 tag 1001): seconds from the epoch, and nanoseconds `0 …
/// 999,999,999`. Nanoseconds of `0` are omitted from the encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp {
    secs: i64,
    nanos: u32,
}

impl Timestamp {
    /// The largest nanosecond value.
    pub const MAX_NANOS: u32 = 999_999_999;

    /// The timestamp, if `nanos` is at most [`Self::MAX_NANOS`].
    #[must_use]
    pub const fn new(secs: i64, nanos: u32) -> Option<Self> {
        if nanos <= Self::MAX_NANOS {
            Some(Self { secs, nanos })
        } else {
            None
        }
    }

    /// Seconds from the epoch.
    #[must_use]
    pub const fn secs(self) -> i64 {
        self.secs
    }

    /// Nanoseconds within the second.
    #[must_use]
    pub const fn nanos(self) -> u32 {
        self.nanos
    }
}

/// A map key: text, ordered by (byte length, bytes), which is the order of its CBOR encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MapKey(String);

impl MapKey {
    /// A key holding `text`, byte for byte. No Unicode normalisation.
    #[must_use]
    pub fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }

    /// The key's text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Ord for MapKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0
            .len()
            .cmp(&other.0.len())
            .then_with(|| self.0.as_bytes().cmp(other.0.as_bytes()))
    }
}

impl PartialOrd for MapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A map with text keys in encoded order. No duplicates, by construction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Map(BTreeMap<MapKey, Value>);

impl Map {
    /// An empty map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The value at `key`.
    #[must_use]
    pub fn get(&self, key: &MapKey) -> Option<&Value> {
        self.0.get(key)
    }

    /// The value at `key`, mutably.
    pub fn get_mut(&mut self, key: &MapKey) -> Option<&mut Value> {
        self.0.get_mut(key)
    }

    /// Insert or replace. Returns the value it replaced.
    pub fn insert(&mut self, key: MapKey, value: Value) -> Option<Value> {
        self.0.insert(key, value)
    }

    /// Remove `key`. Returns the value it held.
    pub fn remove(&mut self, key: &MapKey) -> Option<Value> {
        self.0.remove(key)
    }

    /// The number of entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// `true` when there are no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The entries, in encoded order.
    pub fn iter(&self) -> impl Iterator<Item = (&MapKey, &Value)> {
        self.0.iter()
    }
}

/// One document value. Any of them may be a document's root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    /// `null` (`f6`).
    Null,
    /// `false` / `true` (`f4` / `f5`).
    Bool(bool),
    /// An integer (CBOR major type 0 or 1).
    Integer(Int),
    /// A finite binary64 float (`fb`).
    Float(Float),
    /// A decimal fraction (tag 4).
    Decimal(Decimal),
    /// A timestamp (tag 1001).
    Timestamp(Timestamp),
    /// UTF-8 text (major type 3), compared byte for byte.
    Text(String),
    /// A byte string (major type 2).
    Bytes(Vec<u8>),
    /// An array (major type 4).
    Array(Vec<Value>),
    /// A map with text keys (major type 5).
    Map(Map),
}
