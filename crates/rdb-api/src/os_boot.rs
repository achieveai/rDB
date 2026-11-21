//! R5: whether the operating system rebooted since this folder's last open that served writes.
//!
//! An owner acknowledges a write once it is in its WAL, unsynced, and applied on one regular
//! secondary (design spec §5.2 steps 3 and 6); the WAL is synced later, in groups (step 7). A
//! process that ends, killed or not, leaves those WAL writes with the OS, so the next process
//! on the same OS boot reads them back. A host that crashes or loses power may not: its
//! unsynced tail can be gone, and an acknowledged write may then sit only on the secondary that
//! acknowledged it. The reopen's heal may leave a copy out only while this folder's tail is
//! known intact, which is what [`OsBoot::Same`] says.
//!
//! The file `os_boot`, beside the node boot's `boot` file, holds the OS boot id under which the
//! folder's tail is known intact. The open reads it and the OS's id now:
//!
//! - equal: [`OsBoot::Same`]. The file stays.
//! - otherwise ([`OsBoot::Changed`], [`OsBoot::Unknown`], [`OsBoot::Unsupported`]): the file is
//!   removed at once, before the open writes anything, and written again with the OS's id only
//!   once this open is ready ([`record`], from `Db::wait_ready`), its heal having waited for
//!   every copy as before. An open that dies before then leaves no file, so the next open waits
//!   for every copy too, even on the same OS boot: the lost tail may still be only on the down
//!   copy.
//!
//! Linux only: `/proc/sys/kernel/random/boot_id`. A container reads its host kernel's id, so a
//! container restart counts as the same boot, which is right: the host's page cache survived.
//! Any other platform is [`OsBoot::Unsupported`] and waits for every copy, as before R5.
//!
//! Known limits: a VM or disk snapshot restored, or network storage that drops its write cache,
//! keeps the OS boot id while losing the tail. An operator who restores a snapshot deletes
//! `os_boot` from the node's folder before starting it; the open then waits for every copy.
//!
//! Crate tests never read `/proc`: [`inject`] gives a folder its id, and a folder given none
//! reads as unknown.

use std::path::{Path, PathBuf};

/// The file, beside `boot`, that holds the OS boot id this folder's tail is intact under.
pub(crate) const OS_BOOT_FILE: &str = "os_boot";

/// What the open found of the OS boot, against the folder's file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OsBoot {
    /// The OS has not rebooted since the folder's last open that was ready: its WAL tail is
    /// intact.
    Same,
    /// The OS rebooted since.
    Changed,
    /// No file (a first open, or the last open was never ready), or the id could not be read.
    Unknown,
    /// Not Linux: no id to compare.
    Unsupported,
}

impl OsBoot {
    /// The `heal_wait_all` reason for a verdict that is not [`OsBoot::Same`].
    pub(crate) const fn wait_reason(self) -> Option<&'static str> {
        match self {
            Self::Same => None,
            Self::Changed => Some("os_boot_changed"),
            Self::Unknown => Some("os_boot_unknown"),
            Self::Unsupported => Some("unsupported_platform"),
        }
    }
}

/// The verdict, and the id to [`record`] once this open is ready, when it is not on file yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Seen {
    pub(crate) verdict: OsBoot,
    pub(crate) to_record: Option<(PathBuf, String)>,
}

/// Read `dir`'s file and the OS's id now, and remove the file unless they are equal (module
/// doc). Logged `os_boot_seen{verdict, error}`.
///
/// # Errors
///
/// The removal of a file that does not hold the OS's id failed: the open must not go on with a
/// file that a later open could read as "same".
pub(crate) fn observe(dir: &Path) -> std::io::Result<Seen> {
    let file = dir.join(OS_BOOT_FILE);
    let stored = match std::fs::read_to_string(&file) {
        Ok(text) if text.trim().is_empty() => {
            // PR #181 review: an empty file proves no boot, so it is missing, not a reboot.
            tracing::warn!(file = %file.display(), "os_boot_file_empty");
            None
        }
        Ok(text) => Some(text.trim().to_owned()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            tracing::warn!(file = %file.display(), %error, "os_boot_file_unreadable");
            None
        }
    };
    let (verdict, current, error) = match current(dir) {
        Ok(None) => (OsBoot::Unsupported, None, None),
        Err(error) => (OsBoot::Unknown, None, Some(error)),
        Ok(Some(now)) => match stored.as_deref() {
            Some(was) if was == now => (OsBoot::Same, Some(now), None),
            Some(_) => (OsBoot::Changed, Some(now), None),
            None => (OsBoot::Unknown, Some(now), None),
        },
    };
    if verdict != OsBoot::Same {
        match std::fs::remove_file(&file) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    tracing::info!(
        dir = %dir.display(),
        verdict = ?verdict,
        error = error.as_deref().unwrap_or(""),
        "os_boot_seen"
    );
    Ok(Seen {
        verdict,
        to_record: current
            .filter(|_| verdict != OsBoot::Same)
            .map(|id| (dir.to_path_buf(), id)),
    })
}

/// Write `id` to `dir`'s file as `next_boot` writes `boot`: beside, synced, renamed over.
/// Logged `os_boot_recorded`.
///
/// # Errors
///
/// An I/O error writing, syncing or renaming.
pub(crate) fn record(dir: &Path, id: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    let staged = dir.join(format!("{OS_BOOT_FILE}.new"));
    {
        let mut out = std::fs::File::create(&staged)?;
        writeln!(out, "{id}")?;
        out.sync_all()?;
    }
    std::fs::rename(&staged, dir.join(OS_BOOT_FILE))?;
    tracing::info!(dir = %dir.display(), "os_boot_recorded");
    Ok(())
}

/// The OS's boot id now: `Ok(None)` off Linux, `Err` when it cannot be read.
#[cfg(not(test))]
fn current(_dir: &Path) -> Result<Option<String>, String> {
    if cfg!(target_os = "linux") {
        const PATH: &str = "/proc/sys/kernel/random/boot_id";
        let text = std::fs::read_to_string(PATH).map_err(|error| format!("{PATH}: {error}"))?;
        parse_id(PATH, &text).map(Some)
    } else {
        Ok(None)
    }
}

/// The OS boot id in `text`, read from `source`: trimmed, and an error when nothing is left.
fn parse_id(source: &str, text: &str) -> Result<String, String> {
    let id = text.trim();
    if id.is_empty() {
        return Err(format!("{source} is empty"));
    }
    Ok(id.to_owned())
}

/// Crate tests: the id [`inject`] gave `dir`, or an error when it gave none.
#[cfg(test)]
fn current(dir: &Path) -> Result<Option<String>, String> {
    INJECTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .rev()
        .find(|(at, _)| at == dir)
        .map_or_else(
            || Err(format!("no OS boot id injected for {}", dir.display())),
            |(_, id)| id.clone().map(Some),
        )
}

#[cfg(test)]
static INJECTED: std::sync::Mutex<Vec<(PathBuf, Result<String, String>)>> =
    std::sync::Mutex::new(Vec::new());

/// Crate tests: from now on `dir`'s opens read `id` as the OS's boot id; `Err` reads as an id
/// that could not be read.
#[cfg(test)]
pub(crate) fn inject(dir: &Path, id: Result<&str, &str>) {
    INJECTED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push((
            dir.to_path_buf(),
            id.map(str::to_owned).map_err(str::to_owned),
        ));
}

#[cfg(test)]
mod tests {
    use super::{inject, observe, parse_id, record, OsBoot, OS_BOOT_FILE};

    /// R5 rows 6 and 7, detection: the verdict of a folder against its file. The same id on file
    /// is `Same` and keeps the file. A different id is `Changed`; no file, or an id that cannot
    /// be read, is `Unknown`; each of those removes the file at once and asks to record the id
    /// now, if it has one, once the open is ready. Unit (~5 ms).
    /// Cuts: `observe` reads `Same` whenever a file exists: the changed case, red; `observe`
    /// keeps the file of a non-`Same` verdict: red at its absence.
    #[test]
    fn r5_h_the_os_boot_file_decides_same_changed_or_unknown() {
        let dir = config_testkit::fs::temp_dir();
        let file = dir.path().join(OS_BOOT_FILE);

        inject(dir.path(), Ok("boot-a"));
        let first = observe(dir.path()).expect("observed");
        assert_eq!(first.verdict, OsBoot::Unknown, "no file yet");
        assert_eq!(
            first.to_record,
            Some((dir.path().to_path_buf(), "boot-a".to_owned()))
        );
        record(dir.path(), "boot-a").expect("recorded");

        let same = observe(dir.path()).expect("observed");
        assert_eq!(same.verdict, OsBoot::Same);
        assert_eq!(same.to_record, None);
        assert!(file.exists(), "a same boot keeps the file");

        inject(dir.path(), Ok("boot-b"));
        let changed = observe(dir.path()).expect("observed");
        assert_eq!(changed.verdict, OsBoot::Changed);
        assert!(
            !file.exists(),
            "a reboot removes the file before anything is written"
        );
        assert_eq!(
            changed.to_record,
            Some((dir.path().to_path_buf(), "boot-b".to_owned()))
        );

        record(dir.path(), "boot-b").expect("recorded");
        inject(dir.path(), Err("unreadable"));
        let unknown = observe(dir.path()).expect("observed");
        assert_eq!(unknown.verdict, OsBoot::Unknown);
        assert_eq!(unknown.to_record, None, "no id to record");
        assert!(!file.exists(), "an unreadable id removes the file");
        // And the next open, with the id readable again, is not `Same`: nothing is on file.
        inject(dir.path(), Ok("boot-b"));
        assert_eq!(
            observe(dir.path()).expect("observed").verdict,
            OsBoot::Unknown
        );
    }

    /// The `@m` and `@l` of this test's log lines from this module.
    fn events(method: &str) -> Vec<(String, String)> {
        config_testkit::logs::lines_for_current_test(module_path!(), method)
            .into_iter()
            .map(|line| {
                (
                    line["@m"].as_str().unwrap_or_default().to_owned(),
                    line["@l"].as_str().unwrap_or_default().to_owned(),
                )
            })
            .collect()
    }

    /// PR #181 review: the production parse of `/proc/sys/kernel/random/boot_id`, which crate
    /// tests otherwise replace whole. Unit (~1 ms).
    /// Cut: `parse_id` keeps the newline or accepts blank text, red.
    #[test]
    fn r5_k_the_os_boot_id_is_trimmed_and_blank_text_is_no_id() {
        assert_eq!(parse_id("p", "abc-123\n"), Ok("abc-123".to_owned()));
        assert_eq!(parse_id("p", "  abc  "), Ok("abc".to_owned()));
        assert_eq!(parse_id("p", ""), Err("p is empty".to_owned()));
        assert_eq!(parse_id("p", " \n\t "), Err("p is empty".to_owned()));
    }

    /// PR #181 review: an empty or blank `os_boot` file proves no boot, so it reads as missing,
    /// `Unknown`, not as a reboot, and is warned once. The conservative wait is the same.
    /// Unit (~5 ms). Cut: blank text compared as an id (`Changed`), red.
    #[config_log::retcd_test]
    fn r5_l_a_blank_os_boot_file_is_unknown_not_changed() {
        const METHOD: &str = "r5_l_a_blank_os_boot_file_is_unknown_not_changed";
        for blank in ["", "  \n"] {
            let dir = config_testkit::fs::temp_dir();
            std::fs::write(dir.path().join(OS_BOOT_FILE), blank).expect("blank file");
            inject(dir.path(), Ok("boot-a"));
            let seen = observe(dir.path()).expect("observed");
            assert_eq!(seen.verdict, OsBoot::Unknown, "{blank:?}");
            assert!(!dir.path().join(OS_BOOT_FILE).exists(), "{blank:?}");
        }
        let empty: Vec<_> = events(METHOD)
            .into_iter()
            .filter(|(m, _)| m == "os_boot_file_empty")
            .collect();
        assert_eq!(empty.len(), 2, "{empty:?}");
        assert!(empty.iter().all(|(_, l)| l == "Warning"), "{empty:?}");
    }
}
