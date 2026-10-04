//! Document ops and their application (ADR-rdb-0012 decision 10).
//!
//! A [`Delta`] is a list of ops, applied in order to the current document (or to none, for a
//! create). A path segment against a map is a key (exact bytes); against an array it is an index,
//! `0` or `[1-9][0-9]*`, naming an element that exists. No op appends to an array or creates a
//! parent.

use std::fmt;

use crate::envelope::LIMIT_TEXT;
use crate::path::Path;
use crate::value::{Int, MapKey, Value};

/// One document op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// The whole document becomes this value. Creates the document when it is absent.
    Replace(Value),
    /// Insert or replace at a map key, or replace an existing array element. The parent must
    /// already exist.
    Set(Path, Value),
    /// Remove an existing map key or array element. Later elements shift down.
    Remove(Path),
    /// Add to an existing integer. Leaving the CBOR integer range is an error.
    Increment(Path, Int),
}

/// Ops applied in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Delta(pub Vec<Op>);

/// Where in a document a value sits: the root, or the value under a path segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Location {
    /// The document itself.
    Root,
    /// The value under this segment (unescaped). `""` is the empty key, not the root.
    Segment(String),
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Root => write!(f, "the document root"),
            Self::Segment(segment) => write!(f, "segment {segment:?}"),
        }
    }
}

/// Why an op, or a compile, is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApplyError {
    /// An op other than `Replace` on a document that does not exist, or an empty delta on one.
    #[error("the document does not exist")]
    ObjectAbsent,
    /// A map has no such key.
    #[error("nothing at segment {segment:?}")]
    PathNotFound {
        /// The missing key.
        segment: String,
    },
    /// A path steps into a value that is not a map or an array.
    #[error("{at} is not a container")]
    NotAContainer {
        /// The value that is not a container.
        at: Location,
    },
    /// A segment against an array is not `0` or `[1-9][0-9]*`, or names no element.
    #[error(
        "segment {segment:?} is not an index of this array of {len} {} (no `-` append)",
        if *.len == 1 { "element" } else { "elements" }
    )]
    IndexInvalid {
        /// The segment.
        segment: String,
        /// The array's length, so the caller sees the valid range `0 .. len`.
        len: usize,
    },
    /// Increment on something that is not an integer.
    #[error("increment target is not an integer")]
    TypeMismatch,
    /// Increment left the integer range −2^64 … 2^64−1.
    #[error("increment overflows the integer range")]
    Overflow,
    /// The caller expected another version of the document.
    #[error("expected version {expected}, found {found}")]
    VersionConflict {
        /// The version the caller named.
        expected: u64,
        /// The version stored.
        found: u64,
    },
    /// The result nests deeper than the profile allows; nothing is written.
    #[error("the result nests deeper than the profile allows")]
    TooDeep,
    /// The result is over the size limit; nothing is written.
    #[error("the result is over the {LIMIT_TEXT} limit")]
    TooLarge,
    /// A map key or set member is an array or a map. Only scalars are keys (ADR-rdb-0013 §4).
    #[error("an array or a map cannot be a map key or set member")]
    UnsupportedKeyType,
}

/// Apply `delta` to `base` (`None` = absent).
///
/// # Errors
/// The first op's [`ApplyError`]; [`ApplyError::ObjectAbsent`] if the result is still absent.
pub fn materialize(base: Option<Value>, delta: &Delta) -> Result<Value, ApplyError> {
    let mut doc = base;
    for op in &delta.0 {
        match op {
            Op::Replace(value) => doc = Some(value.clone()),
            Op::Set(path, value) => {
                let (parent, at, last) = parent_of(doc.as_mut(), path)?;
                match parent {
                    Value::Map(map) => {
                        map.insert(MapKey::new(last.clone()), value.clone());
                    }
                    Value::Array(items) => {
                        let i = index(last, items.len())?;
                        items[i] = value.clone();
                    }
                    _ => return Err(ApplyError::NotAContainer { at }),
                }
            }
            Op::Remove(path) => {
                let (parent, at, last) = parent_of(doc.as_mut(), path)?;
                match parent {
                    Value::Map(map) => {
                        map.remove(&MapKey::new(last.clone())).ok_or_else(|| {
                            ApplyError::PathNotFound {
                                segment: last.clone(),
                            }
                        })?;
                    }
                    Value::Array(items) => {
                        let i = index(last, items.len())?;
                        items.remove(i);
                    }
                    _ => return Err(ApplyError::NotAContainer { at }),
                }
            }
            Op::Increment(path, by) => {
                let (parent, at, last) = parent_of(doc.as_mut(), path)?;
                let Value::Integer(current) = child_mut(parent, last, at)? else {
                    return Err(ApplyError::TypeMismatch);
                };
                *current = current.checked_add(*by).ok_or(ApplyError::Overflow)?;
            }
        }
    }
    doc.ok_or(ApplyError::ObjectAbsent)
}

/// The value `path` names inside `root`.
///
/// # Errors
/// [`ApplyError::PathNotFound`], [`ApplyError::IndexInvalid`] or [`ApplyError::NotAContainer`].
pub fn resolve<'a>(root: &'a Value, path: &Path) -> Result<&'a Value, ApplyError> {
    let mut at = root;
    let mut location = Location::Root;
    for segment in path.segments() {
        at = match at {
            Value::Map(map) => {
                map.get(&MapKey::new(segment.clone()))
                    .ok_or_else(|| ApplyError::PathNotFound {
                        segment: segment.clone(),
                    })?
            }
            Value::Array(items) => &items[index(segment, items.len())?],
            _ => return Err(ApplyError::NotAContainer { at: location }),
        };
        location = Location::Segment(segment.clone());
    }
    Ok(at)
}

/// The value that holds `path`'s last segment, where it sits, and that segment. Every segment
/// before the last must exist: parents are never created.
fn parent_of<'a, 'p>(
    root: Option<&'a mut Value>,
    path: &'p Path,
) -> Result<(&'a mut Value, Location, &'p String), ApplyError> {
    let mut at = root.ok_or(ApplyError::ObjectAbsent)?;
    // `Path` is never the root, so it has a last segment.
    let (last, parents) = path
        .segments()
        .split_last()
        .ok_or(ApplyError::PathNotFound {
            segment: String::new(),
        })?;
    let mut location = Location::Root;
    for segment in parents {
        at = child_mut(at, segment, location)?;
        location = Location::Segment(segment.clone());
    }
    Ok((at, location, last))
}

/// The child of `parent` (which sits at `location`) under `segment`.
fn child_mut<'a>(
    parent: &'a mut Value,
    segment: &str,
    location: Location,
) -> Result<&'a mut Value, ApplyError> {
    match parent {
        Value::Map(map) => {
            map.get_mut(&MapKey::new(segment))
                .ok_or_else(|| ApplyError::PathNotFound {
                    segment: segment.to_owned(),
                })
        }
        Value::Array(items) => {
            let i = index(segment, items.len())?;
            Ok(&mut items[i])
        }
        _ => Err(ApplyError::NotAContainer { at: location }),
    }
}

/// `segment` as an index into an array of `len` elements: `0` or `[1-9][0-9]*`, below `len`.
fn index(segment: &str, len: usize) -> Result<usize, ApplyError> {
    let digits = !segment.is_empty() && segment.bytes().all(|b| b.is_ascii_digit());
    let leading_zero = segment.len() > 1 && segment.starts_with('0');
    digits
        .then(|| segment.parse::<usize>().ok())
        .flatten()
        .filter(|i| !leading_zero && *i < len)
        .ok_or_else(|| ApplyError::IndexInvalid {
            segment: segment.to_owned(),
            len,
        })
}
