//! A log file that rotates by size and keeps a fixed number of files (#158).
//!
//! `tracing-appender` rotates by time only, so a busy node's one file grew without limit. This
//! writer sits behind the same non-blocking worker as before. It appends to `<dir>/<name>`. When
//! a write would take that file past [`SizeRotation::max_bytes`], and the file already holds
//! something, it rotates first: `<name>.1` becomes `<name>.2` and so on, the oldest is removed,
//! `<name>` becomes `<name>.1`, and a new `<name>` is opened. So at most
//! [`SizeRotation::max_files`] files exist, `<name>` included.
//!
//! A line is never split. The layer hands each event to the writer as one `write_all`, which
//! reaches this writer as one `write`, and rotation happens only between writes. A single line
//! longer than `max_bytes` goes into a file of its own.
//!
//! Opening an existing file counts its size, so a restarted process keeps the same budget.
//!
//! The non-blocking worker discards writer errors without a word. So a rotation that fails (a
//! rename or open refused) prints the path and the error on stderr, and the writer goes on
//! appending to the file it has. No line is lost; the cap is exceeded, and says so.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// When a log file rotates, and how many files are kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SizeRotation {
    /// Rotate before a write would take the file past this many bytes.
    pub max_bytes: u64,
    /// Files kept in all, the live one included. At least 2.
    pub max_files: usize,
}

impl SizeRotation {
    /// The default for every process: 256 MiB a file, 8 files, so at most 2 GiB on disk.
    ///
    /// At info, a node logs state changes and faults, not writes (#158), so a file lasts days.
    /// With a module at debug for diagnosis (`RUST_LOG=info,rdb_api=debug`) a node logs a few KB
    /// a write, and 2 GiB holds the last several minutes at 500 writes/s.
    pub const DEFAULT: Self = Self {
        max_bytes: 256 * 1024 * 1024,
        max_files: 8,
    };

    /// The rotation a process's `--log-max-mb M` and `--log-files N` flags ask for, each `None`
    /// when not given and then [`Self::DEFAULT`]'s. `M` of 0 never rotates (`Ok(None)`).
    ///
    /// # Errors
    /// `N` under 2, as text naming the flag: one file cannot rotate and keep anything.
    pub fn from_flags(max_mb: Option<u64>, files: Option<usize>) -> Result<Option<Self>, String> {
        let max_files = files.unwrap_or(Self::DEFAULT.max_files);
        if max_files < 2 {
            return Err(format!(
                "--log-files {max_files}: keep at least 2 files (the live one and one before it)"
            ));
        }
        match max_mb {
            Some(0) => Ok(None),
            Some(mb) => Ok(Some(Self {
                max_bytes: mb.saturating_mul(1024 * 1024),
                max_files,
            })),
            None => Ok(Some(Self {
                max_files,
                ..Self::DEFAULT
            })),
        }
    }
}

/// A file named `name` in `dir` that rotates as [`SizeRotation`] says.
pub struct SizeRotatingFile {
    dir: PathBuf,
    name: String,
    rotation: SizeRotation,
    file: File,
    written: u64,
}

impl SizeRotatingFile {
    /// Open `dir/name` for appending, counting what it already holds.
    ///
    /// # Errors
    /// `max_files` under 2 or `max_bytes` of 0 is `InvalidInput`; a file that cannot be opened is
    /// that error.
    pub fn open(dir: &Path, name: &str, rotation: SizeRotation) -> io::Result<Self> {
        if rotation.max_files < 2 || rotation.max_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("log rotation needs max_files >= 2 and max_bytes > 0, got {rotation:?}"),
            ));
        }
        let path = dir.join(name);
        let file = open_append(&path)?;
        let written = file.metadata()?.len();
        Ok(Self {
            dir: dir.to_path_buf(),
            name: name.to_owned(),
            rotation,
            file,
            written,
        })
    }

    /// `dir/name` for `index` 0, `dir/name.<index>` otherwise.
    fn path(&self, index: usize) -> PathBuf {
        if index == 0 {
            self.dir.join(&self.name)
        } else {
            self.dir.join(format!("{}.{index}", self.name))
        }
    }

    /// Shift every kept file up one, drop the oldest, and open a new live file.
    fn rotate(&mut self) -> io::Result<()> {
        self.file.flush()?;
        let oldest = self.path(self.rotation.max_files - 1);
        match std::fs::remove_file(&oldest) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(with_path(e, "remove", &oldest)),
        }
        for index in (1..self.rotation.max_files - 1).rev() {
            let from = self.path(index);
            match std::fs::rename(&from, self.path(index + 1)) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(with_path(e, "rename", &from)),
            }
        }
        // The live file is renamed while still open (std opens with FILE_SHARE_DELETE on
        // Windows, and Unix allows it), and its handle is kept until the new file opens: if the
        // open fails, the writer goes on appending to the old handle, now named `<name>.1`.
        let live = self.path(0);
        std::fs::rename(&live, self.path(1)).map_err(|e| with_path(e, "rename", &live))?;
        self.file = open_append(&live)?;
        self.written = 0;
        Ok(())
    }
}

impl Write for SizeRotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len() as u64;
        if self.written > 0 && self.written + len > self.rotation.max_bytes {
            if let Err(e) = self.rotate() {
                eprintln!(
                    "config-log: log rotation of {} failed, still writing to the current \
                     file: {e}",
                    self.path(0).display()
                );
            }
        }
        self.file.write_all(buf)?;
        self.written += len;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn open_append(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| with_path(e, "open", path))
}

fn with_path(e: io::Error, verb: &str, path: &Path) -> io::Error {
    io::Error::new(e.kind(), format!("{verb} {}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROTATION: SizeRotation = SizeRotation {
        max_bytes: 100,
        max_files: 3,
    };

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .expect("read dir")
            .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    fn line(i: usize) -> Vec<u8> {
        // 43 bytes a line, so two fit in 100 and the third rotates.
        format!("{{\"n\":\"{i:04}\",\"pad\":\"xxxxxxxxxxxxxxxxxxxxx\"}}\n").into_bytes()
    }

    #[test]
    fn rotates_before_a_write_that_would_pass_the_bound_and_keeps_max_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut file = SizeRotatingFile::open(dir.path(), "n.jsonl", ROTATION).expect("open");
        for i in 0..20 {
            file.write_all(&line(i)).expect("write");
        }
        file.flush().expect("flush");
        assert_eq!(names(dir.path()), ["n.jsonl", "n.jsonl.1", "n.jsonl.2"]);
        for name in names(dir.path()) {
            let len = std::fs::metadata(dir.path().join(&name))
                .expect("meta")
                .len();
            assert!(len <= ROTATION.max_bytes, "{name} holds {len} bytes");
        }
        // Newest last: the live file holds lines 18 and 19, `.1` 16 and 17, `.2` 14 and 15.
        let live = std::fs::read_to_string(dir.path().join("n.jsonl")).expect("live");
        assert!(
            live.contains("\"n\":\"0018\"") && live.contains("\"n\":\"0019\""),
            "{live}"
        );
        let oldest = std::fs::read_to_string(dir.path().join("n.jsonl.2")).expect("oldest");
        assert!(oldest.contains("\"n\":\"0014\""), "{oldest}");
    }

    #[test]
    fn a_line_is_never_split_across_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut file = SizeRotatingFile::open(dir.path(), "n.jsonl", ROTATION).expect("open");
        for i in 0..7 {
            file.write_all(&line(i)).expect("write");
        }
        file.flush().expect("flush");
        for name in names(dir.path()) {
            let text = std::fs::read_to_string(dir.path().join(&name)).expect("read");
            assert!(text.ends_with('\n'), "{name} ends mid-line: {text:?}");
            for l in text.lines() {
                serde_json::from_str::<serde_json::Value>(l)
                    .unwrap_or_else(|e| panic!("{name} holds a split line {l:?}: {e}"));
            }
        }
    }

    #[test]
    fn a_reopened_file_counts_what_it_already_holds() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let mut file = SizeRotatingFile::open(dir.path(), "n.jsonl", ROTATION).expect("open");
            file.write_all(&line(0)).expect("write");
            file.write_all(&line(1)).expect("write");
        }
        let mut file = SizeRotatingFile::open(dir.path(), "n.jsonl", ROTATION).expect("reopen");
        file.write_all(&line(2)).expect("write");
        file.flush().expect("flush");
        assert_eq!(names(dir.path()), ["n.jsonl", "n.jsonl.1"]);
    }

    #[test]
    fn a_line_longer_than_the_bound_gets_a_file_of_its_own() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut file = SizeRotatingFile::open(dir.path(), "n.jsonl", ROTATION).expect("open");
        file.write_all(&line(0)).expect("write");
        let long = format!("{{\"pad\":\"{}\"}}\n", "y".repeat(200));
        file.write_all(long.as_bytes()).expect("write");
        file.write_all(&line(1)).expect("write");
        file.flush().expect("flush");
        let rotated = std::fs::read_to_string(dir.path().join("n.jsonl.1")).expect("read");
        assert_eq!(rotated, long);
    }

    #[test]
    fn the_flags_default_to_the_default_and_zero_megabytes_never_rotates() {
        assert_eq!(
            SizeRotation::from_flags(None, None),
            Ok(Some(SizeRotation::DEFAULT))
        );
        assert_eq!(
            SizeRotation::from_flags(Some(1), Some(3)),
            Ok(Some(SizeRotation {
                max_bytes: 1024 * 1024,
                max_files: 3
            }))
        );
        assert_eq!(SizeRotation::from_flags(Some(0), None), Ok(None));
        let refused = SizeRotation::from_flags(None, Some(1)).expect_err("one file is refused");
        assert!(refused.contains("--log-files 1"), "{refused}");
    }

    #[test]
    fn fewer_than_two_files_or_zero_bytes_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        for bad in [
            SizeRotation {
                max_bytes: 100,
                max_files: 1,
            },
            SizeRotation {
                max_bytes: 0,
                max_files: 3,
            },
        ] {
            let error = SizeRotatingFile::open(dir.path(), "n.jsonl", bad)
                .err()
                .unwrap_or_else(|| panic!("{bad:?} was accepted"));
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
    }
}
