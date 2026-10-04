//! Paths: JSON Pointer, RFC 6901 (ADR-rdb-0012 decision 10).
//!
//! `/a/b` has segments `a` and `b`; `~1` reads as `/` and `~0` as `~`. A [`Path`] always names
//! something inside the document: `""` (the whole document) is refused with
//! [`PathError::RootNotAllowed`], because every op that takes a path (set, remove, increment)
//! refuses the root. The whole document is `Op::Replace`, which takes no path.

use std::fmt;

/// The most segments a path may have (decision 5).
pub const MAX_SEGMENTS: usize = 64;

/// Why a path is refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PathError {
    /// Not RFC 6901: no leading `/`, or a `~` not followed by `0` or `1`.
    #[error("path syntax error at byte {pos}")]
    PathSyntax {
        /// The byte that breaks the syntax.
        pos: usize,
    },
    /// More than [`MAX_SEGMENTS`] segments.
    #[error("path has more than {MAX_SEGMENTS} segments")]
    PathTooLong,
    /// `""`: an op aimed at the whole document. Use `Op::Replace`.
    #[error("the root path is not allowed here")]
    RootNotAllowed,
}

/// A parsed, non-root JSON Pointer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Path(Vec<String>);

impl Path {
    /// Parse `text` as RFC 6901.
    ///
    /// # Errors
    /// [`PathError`].
    pub fn parse(text: &str) -> Result<Self, PathError> {
        if text.is_empty() {
            return Err(PathError::RootNotAllowed);
        }
        if !text.starts_with('/') {
            return Err(PathError::PathSyntax { pos: 0 });
        }
        let mut segments = Vec::new();
        let mut current = String::new();
        let mut chars = text.char_indices().skip(1).peekable();
        while let Some((pos, c)) = chars.next() {
            match c {
                '/' => segments.push(std::mem::take(&mut current)),
                '~' => match chars.next() {
                    Some((_, '0')) => current.push('~'),
                    Some((_, '1')) => current.push('/'),
                    _ => return Err(PathError::PathSyntax { pos }),
                },
                c => current.push(c),
            }
            if segments.len() >= MAX_SEGMENTS {
                return Err(PathError::PathTooLong);
            }
        }
        segments.push(current);
        Ok(Self(segments))
    }

    /// The segments, unescaped. Never empty.
    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.0
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for segment in &self.0 {
            write!(f, "/{}", segment.replace('~', "~0").replace('/', "~1"))?;
        }
        Ok(())
    }
}
