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
//! Opening an existing file counts its size, so a restarted process keeps the same budget. The
//! open also removes this log's numbered files at or past `max_files`, so a restart with fewer
//! files keeps only as many as it was asked to.
//!
//! One writer per log. The open takes an exclusive OS lock on `<dir>/.<name>.lock` and holds it
//! until the writer drops; a second writer of the same log, in this process or another, is
//! refused with `WouldBlock` naming the log. Two writers would rename and remove each other's
//! files. [`crate::init`] takes the same lock for a log that does not rotate, so a rotating and
//! a non-rotating writer cannot share one either. The lock goes with the process, so a crash
//! leaves nothing to clear; the lock file itself stays, empty, and is hidden from a `<name>*`
//! glob. `name` must be a plain file name: not `.` or `..`, and no `/`, `\\` or `:`, so the
//! log, its rotations and its lock all stay in `dir` (on Windows `C:x` names a file in another
//! folder and `name:stream` a hidden stream of `name`).
//!
//! The non-blocking worker discards writer errors without a word. So a rotation that fails (a
//! remove, rename or open refused) prints the path and the error on stderr, best effort, and the
//! writer goes on appending to the file it holds. No line is lost; the cap is exceeded, and says
//! so. A retry repeats only the step that failed:
//! - The shift moves files up into the lowest free index, removing the oldest only when every
//!   index is taken, so a shift or live rename that failed is retried without removing more.
//! - When the new live file cannot be opened after the rename, the writer keeps the renamed
//!   handle, now `<name>.1`, does not rotate, and opens `<name>` again on each later write
//!   until one succeeds.

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
    /// False after the rename when the new live file could not be opened: `file` is then
    /// `<name>.1`, nothing rotates, and each write tries to open `<name>` first.
    live: bool,
    /// Held for the writer's life: the lock that keeps this log to one writer.
    _lock: LogLock,
    /// Where a failed rotation is reported: stderr, or a test's failing sink.
    diagnostics: Box<dyn Write + Send>,
    /// Test-only faults: the next this many renames of the live file, or opens of a new live
    /// file, fail as if the OS refused them.
    #[cfg(test)]
    fail_live_rename: u32,
    #[cfg(test)]
    fail_live_open: u32,
}

impl SizeRotatingFile {
    /// Open `dir/name` for appending, counting what it already holds, and remove its numbered
    /// files at or past `max_files`.
    ///
    /// # Errors
    /// `max_files` under 2, `max_bytes` of 0 or a `name` that is not a plain file name is
    /// `InvalidInput`; another writer holding this log is `WouldBlock`; a file that cannot be
    /// opened or removed is that error.
    pub fn open(dir: &Path, name: &str, rotation: SizeRotation) -> io::Result<Self> {
        check_name(name)?;
        if rotation.max_files < 2 || rotation.max_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("log rotation needs max_files >= 2 and max_bytes > 0, got {rotation:?}"),
            ));
        }
        let lock = lock_log(dir, name)?;
        prune(dir, name, rotation.max_files)?;
        let path = dir.join(name);
        let file = open_append(&path)?;
        let written = file.metadata()?.len();
        Ok(Self {
            dir: dir.to_path_buf(),
            name: name.to_owned(),
            rotation,
            file,
            written,
            live: true,
            _lock: lock,
            diagnostics: Box::new(io::stderr()),
            #[cfg(test)]
            fail_live_rename: 0,
            #[cfg(test)]
            fail_live_open: 0,
        })
    }

    /// Rename the live file to `<name>.1`.
    fn rename_live(&mut self) -> io::Result<()> {
        let live = self.path(0);
        #[cfg(test)]
        if self.fail_live_rename > 0 {
            self.fail_live_rename -= 1;
            return Err(with_path(io::Error::other("injected"), "rename", &live));
        }
        std::fs::rename(&live, self.path(1)).map_err(|e| with_path(e, "rename", &live))
    }

    /// Open (create) the live file for appending.
    fn open_live(&mut self) -> io::Result<File> {
        let live = self.path(0);
        #[cfg(test)]
        if self.fail_live_open > 0 {
            self.fail_live_open -= 1;
            return Err(with_path(io::Error::other("injected"), "open", &live));
        }
        open_append(&live)
    }

    /// `dir/name` for `index` 0, `dir/name.<index>` otherwise.
    fn path(&self, index: usize) -> PathBuf {
        if index == 0 {
            self.dir.join(&self.name)
        } else {
            self.dir.join(format!("{}.{index}", self.name))
        }
    }

    /// Free `<name>.1`: shift the files below the lowest free index up one, removing the oldest
    /// only when no index is free. Done again after a failure, it removes nothing more: the
    /// index the failed step left free is found first.
    fn free_first_index(&self) -> io::Result<()> {
        let oldest = self.rotation.max_files - 1;
        let mut free = None;
        for index in 1..=oldest {
            match std::fs::symlink_metadata(self.path(index)) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    free = Some(index);
                    break;
                }
                Err(e) => return Err(with_path(e, "stat", &self.path(index))),
            }
        }
        let free = match free {
            Some(index) => index,
            None => {
                let path = self.path(oldest);
                std::fs::remove_file(&path).map_err(|e| with_path(e, "remove", &path))?;
                oldest
            }
        };
        for index in (1..free).rev() {
            let from = self.path(index);
            std::fs::rename(&from, self.path(index + 1))
                .map_err(|e| with_path(e, "rename", &from))?;
        }
        Ok(())
    }

    /// Shift every kept file up one, drop the oldest, and open a new live file.
    fn rotate(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.free_first_index()?;
        // The live file is renamed while still open (std opens with FILE_SHARE_DELETE on
        // Windows, and Unix allows it), and its handle is kept until the new file opens: if the
        // open fails, the writer goes on appending to the old handle, now `<name>.1`.
        self.rename_live()?;
        match self.open_live() {
            Ok(file) => {
                self.file = file;
                self.written = 0;
                Ok(())
            }
            Err(e) => {
                self.live = false;
                Err(e)
            }
        }
    }

    /// After a failed open: open `<name>` again. True when it opened.
    fn reopen_live(&mut self) -> bool {
        match self.open_live() {
            Ok(file) => {
                self.file = file;
                self.written = self.file.metadata().map_or(0, |meta| meta.len());
                self.live = true;
                true
            }
            Err(_) => false,
        }
    }

    /// Best effort: a stderr that cannot be written must not stop the line being appended. The
    /// line goes to the sink as one write, so a partial write cannot interleave two reports.
    fn say(&mut self, what: &str) {
        let report = format!(
            "config-log: log rotation of {}: {what}\n",
            self.path(0).display()
        );
        let _ = self.diagnostics.write_all(report.as_bytes());
    }
}

impl Write for SizeRotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = buf.len() as u64;
        if !self.live {
            if self.reopen_live() {
                self.say("live file open again");
            }
        } else if self.written > 0 && self.written + len > self.rotation.max_bytes {
            if let Err(e) = self.rotate() {
                let held = if self.live { 0 } else { 1 };
                self.say(&format!(
                    "failed, still writing to {}: {e}",
                    self.path(held).display()
                ));
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

/// `name` as a plain file name: not empty, not `.` or `..`, and no `/` or `\\` (either is a
/// separator on Windows) or `:` (on Windows `C:x` is drive-relative, a file in that drive's
/// current folder, and `name:stream` is an alternate data stream of `name`).
fn check_name(name: &str) -> io::Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', ':']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "log file name {name:?} must be a plain file name: no folder, drive or stream \
                 (no `/`, `\\` or `:`)"
            ),
        ));
    }
    Ok(())
}

/// The OS lock on `<dir>/.<name>.lock`, held while a process writes `<dir>/<name>`; dropping it
/// releases the lock.
pub(crate) struct LogLock {
    _file: File,
}

/// Take the exclusive lock that keeps `dir/name` to one writer, rotating or not.
///
/// # Errors
/// A `name` that is not a plain file name is `InvalidInput`; another writer holding it is
/// `WouldBlock`, naming the log; a lock file that cannot be opened or locked is that error.
pub(crate) fn lock_log(dir: &Path, name: &str) -> io::Result<LogLock> {
    check_name(name)?;
    let path = dir.join(format!(".{name}.lock"));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .map_err(|e| with_path(e, "open", &path))?;
    match file.try_lock() {
        Ok(()) => Ok(LogLock { _file: file }),
        Err(std::fs::TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!(
                "{} is already written by another process (or writer): {} is locked; give \
                 each process its own log directory",
                dir.join(name).display(),
                path.display()
            ),
        )),
        Err(std::fs::TryLockError::Error(e)) => Err(with_path(e, "lock", &path)),
    }
}

/// Remove `dir/name.<k>` for every `k` at or past `max_files`: a restart with fewer files.
fn prune(dir: &Path, name: &str, max_files: usize) -> io::Result<()> {
    let prefix = format!("{name}.");
    for entry in std::fs::read_dir(dir).map_err(|e| with_path(e, "read", dir))? {
        let entry = entry.map_err(|e| with_path(e, "read", dir))?;
        let file_name = entry.file_name();
        let Some(index) = file_name
            .to_str()
            .and_then(|text| text.strip_prefix(&prefix))
            .filter(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|suffix| suffix.parse::<usize>().ok())
        else {
            continue;
        };
        if index >= max_files {
            let path = entry.path();
            std::fs::remove_file(&path).map_err(|e| with_path(e, "remove", &path))?;
        }
    }
    Ok(())
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

    /// The files in `dir`, sorted, leaving out the hidden lock file.
    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .expect("read dir")
            .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with('.'))
            .collect();
        names.sort();
        names
    }

    fn line(i: usize) -> Vec<u8> {
        // 43 bytes a line, so two fit in 100 and the third rotates.
        format!("{{\"n\":\"{i:04}\",\"pad\":\"xxxxxxxxxxxxxxxxxxxxx\"}}\n").into_bytes()
    }

    /// Every line number held by any log file in `dir`, sorted.
    fn lines_on_disk(dir: &Path) -> Vec<usize> {
        let mut found: Vec<usize> = names(dir)
            .iter()
            .flat_map(|name| {
                std::fs::read_to_string(dir.join(name))
                    .expect("read")
                    .lines()
                    .map(|l| {
                        let v: serde_json::Value = serde_json::from_str(l).expect("a json line");
                        v["n"]
                            .as_str()
                            .expect("n")
                            .parse::<usize>()
                            .expect("a number")
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        found.sort_unstable();
        found
    }

    /// Review r1 (PR #182): a restart with a smaller `--log-files` kept `.2` to `.7`, because
    /// rotation removes only the one oldest index. The open now removes this log's numbered
    /// files at or past the count, and nothing else.
    #[test]
    fn r158_an_open_with_fewer_files_removes_the_numbered_files_past_the_count() {
        let dir = tempfile::tempdir().expect("tempdir");
        for name in [
            "n.jsonl",
            "n.jsonl.1",
            "n.jsonl.2",
            "n.jsonl.3",
            "n.jsonl.7",
            "n.jsonl.notes",
            "n.jsonl.2.bak",
            "other.jsonl.5",
        ] {
            std::fs::write(dir.path().join(name), b"{}\n").expect("seed");
        }
        let _file = SizeRotatingFile::open(dir.path(), "n.jsonl", ROTATION).expect("open");
        assert_eq!(
            names(dir.path()),
            [
                "n.jsonl",
                "n.jsonl.1",
                "n.jsonl.2",
                "n.jsonl.2.bak",
                "n.jsonl.notes",
                "other.jsonl.5"
            ]
        );
    }

    /// Review r1 (PR #182): when the new live file could not be opened after the rename, the
    /// writer kept the renamed handle, the next rotations shifted it up, and the last one removed
    /// it: every later line went to a deleted file. Now the writer opens the live file again on
    /// the next write, and no line is lost.
    #[test]
    fn r158_a_failed_open_after_the_rename_loses_no_line() {
        let rotation = SizeRotation {
            max_bytes: 100,
            max_files: 20,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let mut file = SizeRotatingFile::open(dir.path(), "n.jsonl", rotation).expect("open");
        file.write_all(&line(0)).expect("write");
        file.write_all(&line(1)).expect("write");
        file.fail_live_open = 1;
        for i in 2..30 {
            file.write_all(&line(i)).expect("write");
        }
        file.flush().expect("flush");
        assert_eq!(lines_on_disk(dir.path()), (0..30).collect::<Vec<_>>());
        let live = std::fs::read_to_string(dir.path().join("n.jsonl")).expect("a live file");
        assert!(live.contains("\"n\":\"0029\""), "{live}");
    }

    /// Review r1 (PR #182): a live rename that failed ran after the oldest file was removed and
    /// the rest shifted, and every later over-cap write did both again while the live file never
    /// turned over, so three failures used up the history. Now a retry finds the slot already
    /// free and only renames: one rotation removes one file, however often it is retried.
    #[test]
    fn r158_a_failed_live_rename_is_retried_without_removing_more_files() {
        let rotation = SizeRotation {
            max_bytes: 100,
            max_files: 4,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let mut file = SizeRotatingFile::open(dir.path(), "n.jsonl", rotation).expect("open");
        for i in 0..8 {
            file.write_all(&line(i)).expect("write");
        }
        assert_eq!(lines_on_disk(dir.path()), (0..8).collect::<Vec<_>>());
        file.fail_live_rename = 3;
        for i in 8..12 {
            file.write_all(&line(i)).expect("write");
        }
        file.flush().expect("flush");
        // One rotation went through, so lines 0 and 1, the oldest file, are the only loss.
        assert_eq!(lines_on_disk(dir.path()), (2..12).collect::<Vec<_>>());
        assert_eq!(
            names(dir.path()),
            ["n.jsonl", "n.jsonl.1", "n.jsonl.2", "n.jsonl.3"]
        );
    }

    /// Review r1 (PR #182): two processes on one log directory renamed and removed each other's
    /// files. A second writer of the same log is refused while the first holds it, with an
    /// error that names the log; another log in the same directory is not affected.
    #[test]
    fn r158_a_second_writer_of_the_same_log_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = SizeRotatingFile::open(dir.path(), "n.jsonl", ROTATION).expect("open");
        let refused = SizeRotatingFile::open(dir.path(), "n.jsonl", ROTATION)
            .err()
            .expect("a second writer is refused");
        assert_eq!(refused.kind(), io::ErrorKind::WouldBlock, "{refused}");
        let text = refused.to_string();
        assert!(
            text.contains("n.jsonl") && text.contains("another"),
            "{text}"
        );
        let _other = SizeRotatingFile::open(dir.path(), "m.jsonl", ROTATION).expect("other log");
        drop(first);
        let _again = SizeRotatingFile::open(dir.path(), "n.jsonl", ROTATION).expect("reopen");
    }

    /// A diagnostics sink that refuses every write, as a closed or full stderr does, and keeps
    /// what it was offered so a row can see the report went to the sink.
    struct Unwritable(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl Write for Unwritable {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("offered").extend_from_slice(buf);
            Err(io::Error::other("stderr is gone"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("stderr is gone"))
        }
    }

    /// Review r2 (PR #182): a failed rotation reports on stderr, best effort. With stderr
    /// unwritable, the writer does not panic or fail, and every line still reaches the file:
    /// through a failed rename (the cap exceeded) and a failed open (the renamed file). Review r3:
    /// each report was offered to the sink, so a report sent straight to stderr is red here.
    #[test]
    fn r158_a_failed_rotation_with_stderr_unwritable_still_writes_every_line() {
        let rotation = SizeRotation {
            max_bytes: 100,
            max_files: 20,
        };
        let dir = tempfile::tempdir().expect("tempdir");
        let mut file = SizeRotatingFile::open(dir.path(), "n.jsonl", rotation).expect("open");
        let offered = std::sync::Arc::default();
        file.diagnostics = Box::new(Unwritable(std::sync::Arc::clone(&offered)));
        file.write_all(&line(0)).expect("write");
        file.write_all(&line(1)).expect("write");
        file.fail_live_rename = 2;
        for i in 2..5 {
            file.write_all(&line(i))
                .expect("a write through a failed rename");
        }
        file.fail_live_open = 1;
        for i in 5..10 {
            file.write_all(&line(i))
                .expect("a write through a failed open");
        }
        file.flush().expect("flush");
        assert_eq!(lines_on_disk(dir.path()), (0..10).collect::<Vec<_>>());
        let offered = String::from_utf8(offered.lock().expect("offered").clone()).expect("utf8");
        let live = dir.path().join("n.jsonl");
        let failed = format!(
            "config-log: log rotation of {}: failed, still writing to",
            live.display()
        );
        // Two failed renames, then one failed open, then the open that worked.
        assert_eq!(offered.matches(&failed).count(), 3, "{offered}");
        assert!(offered.contains("injected"), "{offered}");
        assert!(
            offered.contains(&format!(
                "config-log: log rotation of {}: live file open again\n",
                live.display()
            )),
            "{offered}"
        );
    }

    /// Review r2 (PR #182): the name is a plain file name. A name with a separator or `..`
    /// would put the log, its rotations and its lock outside `dir`; it is refused. Review r3: so
    /// is a `:`, which on Windows makes `C:x` drive-relative and `n:s` a stream of `n`.
    #[test]
    fn r158_a_name_that_is_not_a_plain_file_name_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        for bad in [
            "./n.jsonl",
            "a/n.jsonl",
            "../n.jsonl",
            "..",
            ".",
            "",
            "a\\n.jsonl",
            "C:n.jsonl",
            "n.jsonl:stream",
            "n.jsonl:stream:$DATA",
        ] {
            let error = SizeRotatingFile::open(dir.path(), bad, ROTATION)
                .err()
                .unwrap_or_else(|| panic!("{bad:?} was accepted"));
            assert_eq!(
                error.kind(),
                io::ErrorKind::InvalidInput,
                "{bad:?}: {error}"
            );
        }
        assert!(names(dir.path()).is_empty(), "{:?}", names(dir.path()));
    }

    /// Review r1 (PR #182): a write that brings the file to exactly `max_bytes` stays in it; one
    /// more byte rotates.
    #[test]
    fn r158_exactly_max_bytes_stays_and_one_byte_more_rotates() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut file = SizeRotatingFile::open(dir.path(), "n.jsonl", ROTATION).expect("open");
        file.write_all(&[b'a'; 60]).expect("write");
        file.write_all(&[b'b'; 40]).expect("write");
        file.flush().expect("flush");
        assert_eq!(names(dir.path()), ["n.jsonl"]);
        let len = |name: &str| {
            std::fs::metadata(dir.path().join(name))
                .expect("meta")
                .len()
        };
        assert_eq!(len("n.jsonl"), ROTATION.max_bytes);
        file.write_all(b"c").expect("write");
        file.flush().expect("flush");
        assert_eq!(names(dir.path()), ["n.jsonl", "n.jsonl.1"]);
        assert_eq!((len("n.jsonl.1"), len("n.jsonl")), (ROTATION.max_bytes, 1));
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
