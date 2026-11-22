//! Process-level initialization.

use std::io::Write;
use std::path::PathBuf;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::layer::JsonlLayer;
use crate::rotate::{lock_log, LogLock, SizeRotatingFile, SizeRotation};

/// How a process logs.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Service/application name (`application` field).
    pub application: String,
    /// Directory for the JSONL file; created if missing.
    pub dir: PathBuf,
    /// File name, e.g. `config-server-node1.log.jsonl`.
    pub file_name: String,
    /// `RUST_LOG`-style directive, e.g. `info,config_engine=debug`.
    pub filter: String,
    /// Include `file`/`line` fields (cheap; on by default).
    pub include_location: bool,
    /// Also emit compact JSON to stderr (off by default; console logs are not the tool).
    pub also_stderr: bool,
    /// Per-test routing directory (tests only; see [`crate::testing`]).
    pub test_log_dir: Option<PathBuf>,
    /// Size-based rotation of the file (#158); `None` never rotates. Default
    /// [`SizeRotation::DEFAULT`]: 256 MiB a file, 8 files kept.
    pub rotate: Option<SizeRotation>,
}

impl LogConfig {
    /// Sensible defaults for an application writing to `./logs/<app>.log.jsonl`.
    pub fn for_application(application: impl Into<String>) -> Self {
        let application = application.into();
        Self {
            file_name: format!("{application}.log.jsonl"),
            application,
            dir: PathBuf::from("logs"),
            filter: std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
            include_location: true,
            also_stderr: false,
            test_log_dir: None,
            rotate: Some(SizeRotation::DEFAULT),
        }
    }
}

/// Keeps the non-blocking writer alive; drop flushes.
pub struct LogGuard {
    _guards: Vec<tracing_appender::non_blocking::WorkerGuard>,
}

/// Errors from [`init`].
#[derive(Debug, thiserror::Error)]
pub enum LogInitError {
    /// Could not create the log directory or file.
    #[error("log io error: {0}")]
    Io(#[from] std::io::Error),
    /// Bad filter directive.
    #[error("invalid log filter {0:?}: {1}")]
    Filter(String, String),
    /// A global subscriber was already installed.
    #[error("global tracing subscriber already set")]
    AlreadyInitialized,
}

/// A writer that holds its log's [`LogLock`] for as long as it lives.
struct Locked<W> {
    inner: W,
    _lock: LogLock,
}

impl<W: Write> Write for Locked<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct Tee(Vec<Box<dyn Write + Send>>);
impl Write for Tee {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        for w in &mut self.0 {
            w.write_all(buf)?;
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        for w in &mut self.0 {
            w.flush()?;
        }
        Ok(())
    }
}

/// Build the layer + filter without installing (used by [`init`] and the test bootstrap).
pub(crate) fn build(
    cfg: &LogConfig,
) -> Result<
    (
        JsonlLayer,
        EnvFilter,
        Vec<tracing_appender::non_blocking::WorkerGuard>,
    ),
    LogInitError,
> {
    std::fs::create_dir_all(&cfg.dir)?;
    let (nb, guard) = match cfg.rotate {
        Some(rotation) => tracing_appender::non_blocking(SizeRotatingFile::open(
            &cfg.dir,
            &cfg.file_name,
            rotation,
        )?),
        // Locked as a rotating file is (review r2, PR #182): a non-rotating writer beside a
        // rotating one would append to a file the other renames and removes.
        None => tracing_appender::non_blocking(Locked {
            _lock: lock_log(&cfg.dir, &cfg.file_name)?,
            inner: tracing_appender::rolling::never(&cfg.dir, &cfg.file_name),
        }),
    };
    let mut guards = vec![guard];
    let writer: Box<dyn Write + Send> = if cfg.also_stderr {
        let (nb_err, g2) = tracing_appender::non_blocking(std::io::stderr());
        guards.push(g2);
        Box::new(Tee(vec![Box::new(nb), Box::new(nb_err)]))
    } else {
        Box::new(nb)
    };
    let filter = EnvFilter::try_new(&cfg.filter)
        .map_err(|e| LogInitError::Filter(cfg.filter.clone(), e.to_string()))?;
    let layer = JsonlLayer::new(
        cfg.application.clone(),
        writer,
        cfg.test_log_dir.clone(),
        cfg.include_location,
    );
    Ok((layer, filter, guards))
}

/// Install the global subscriber. Call once per process; hold the guard until exit.
pub fn init(cfg: LogConfig) -> Result<LogGuard, LogInitError> {
    let (layer, filter, guards) = build(&cfg)?;
    tracing_subscriber::registry()
        .with(filter)
        .with(layer)
        .try_init()
        .map_err(|_| LogInitError::AlreadyInitialized)?;
    tracing::info!(
        application = %cfg.application,
        dir = %cfg.dir.display(),
        file = %cfg.file_name,
        filter = %cfg.filter,
        rotate = ?cfg.rotate,
        "logging initialized"
    );
    Ok(LogGuard { _guards: guards })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(dir: &std::path::Path, rotate: Option<SizeRotation>) -> LogConfig {
        LogConfig {
            dir: dir.to_path_buf(),
            file_name: "n.jsonl".to_owned(),
            filter: "info".to_owned(),
            rotate,
            ..LogConfig::for_application("init-tests")
        }
    }

    /// Review r2 (PR #182): `--log-max-mb 0` (no rotation) took no lock, so a non-rotating
    /// process could append to the live file a rotating peer renames and removes. A second
    /// writer of the same log is refused whether either one rotates.
    #[test]
    fn r158_a_non_rotating_writer_and_a_rotating_one_never_share_a_log() {
        let dir = tempfile::tempdir().expect("tempdir");
        for (first, second) in [
            (None, Some(SizeRotation::DEFAULT)),
            (Some(SizeRotation::DEFAULT), None),
            (None, None),
        ] {
            let held = build(&config(dir.path(), first)).expect("the first writer");
            let refused = build(&config(dir.path(), second))
                .err()
                .unwrap_or_else(|| panic!("{second:?} beside {first:?} was accepted"));
            match refused {
                LogInitError::Io(e) => {
                    assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock, "{e}");
                }
                other => panic!("{other}"),
            }
            drop(held);
        }
    }

    /// Where [`r158_hold_a_log_until_killed`] opens its log, and whether it rotates.
    const HOLD_DIR: &str = "R158_HOLD_LOG_DIR";
    const HOLD_ROTATES: &str = "R158_HOLD_LOG_ROTATES";
    /// What the holder prints once it holds the log. The harness has already printed
    /// `test <name> ... ` with no newline, so the marker is found inside a line, not as one.
    const READY: &str = "R158_HOLDER_READY";

    /// Not a check: [`r158_a_killed_writer_leaves_its_lock_file_and_a_restart_still_opens_the_log`]
    /// runs it as a child process. It opens the log, prints [`READY`] on stdout, and blocks on
    /// stdin until it is killed.
    #[test]
    #[ignore = "a child process of the restart row, not a check on its own"]
    fn r158_hold_a_log_until_killed() {
        use std::io::BufRead as _;
        let dir = std::env::var_os(HOLD_DIR).expect("the restart row sets the log dir");
        let rotate = std::env::var_os(HOLD_ROTATES).map(|_| SizeRotation::DEFAULT);
        let _held = build(&config(std::path::Path::new(&dir), rotate)).expect("the holder");
        // Straight to the process's stdout: the test harness captures only `print!`.
        let mut out = std::io::stdout();
        out.write_all(format!("{READY}\n").as_bytes())
            .expect("stdout");
        out.flush().expect("stdout");
        let _ = std::io::stdin().lock().read_line(&mut String::new());
    }

    /// The holder process, killed and reaped when this drops, so no failure in the row leaves
    /// it running.
    struct Holder(std::process::Child);

    impl Holder {
        /// Kill and reap the holder, and give back what it wrote on stderr.
        fn kill(&mut self) -> String {
            use std::io::Read as _;
            let _ = self.0.kill();
            let _ = self.0.wait();
            let mut stderr = String::new();
            if let Some(mut pipe) = self.0.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            stderr
        }
    }

    impl Drop for Holder {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    /// Review r3 (PR #182): a writer that is killed leaves `.<name>.lock` on disk. That file
    /// must not block the restart: the lock is the OS's, and it goes with the process. Shown in
    /// both modes, rotating and not (`--log-max-mb 0`): a real child process holds the log, a
    /// second writer is refused while it lives, and after the kill the lock file is still there
    /// and the log opens. The wait for the holder is bounded (30 s times
    /// `RETCD_TEST_DEADLINE_SCALE`), and the holder is killed on every way out of the row.
    #[test]
    fn r158_a_killed_writer_leaves_its_lock_file_and_a_restart_still_opens_the_log() {
        use std::io::BufRead as _;
        use std::process::{Command, Stdio};
        // As `config_testkit::poll::deadline_scale` reads it; config-testkit depends on this
        // crate, so it cannot be called from here.
        let scale = std::env::var("RETCD_TEST_DEADLINE_SCALE")
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(1)
            .max(1);
        let patience = std::time::Duration::from_secs(30) * scale;
        for rotate in [Some(SizeRotation::DEFAULT), None] {
            let dir = tempfile::tempdir().expect("tempdir");
            let mut command = Command::new(std::env::current_exe().expect("this test binary"));
            command
                .args([
                    "--exact",
                    "init::tests::r158_hold_a_log_until_killed",
                    "--ignored",
                    "--test-threads=1",
                ])
                .env(HOLD_DIR, dir.path())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if rotate.is_some() {
                command.env(HOLD_ROTATES, "1");
            }
            let mut holder = Holder(command.spawn().expect("spawn the holder"));
            let stdout = holder.0.stdout.take().expect("the holder's stdout");
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            // Reads until the marker or the end of the holder's stdout; it ends when the holder
            // is killed, which closes the pipe.
            std::thread::spawn(move || {
                let ready = std::io::BufReader::new(stdout)
                    .lines()
                    .map_while(Result::ok)
                    .any(|line| line.contains(READY));
                let _ = ready_tx.send(ready);
            });
            match ready_rx.recv_timeout(patience) {
                Ok(true) => {}
                Ok(false) => panic!(
                    "{rotate:?}: the holder exited without holding the log: {}",
                    holder.kill()
                ),
                Err(_) => panic!(
                    "{rotate:?}: the holder did not hold the log within {patience:?}: {}",
                    holder.kill()
                ),
            }
            match build(&config(dir.path(), rotate)) {
                Err(LogInitError::Io(e)) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(other) => panic!("{rotate:?}: {other}"),
                Ok(_) => panic!("{rotate:?}: a second writer beside the live holder was accepted"),
            }
            holder.kill();
            let lock = dir.path().join(".n.jsonl.lock");
            assert!(
                lock.exists(),
                "{rotate:?}: the killed holder's lock file is gone"
            );
            let restarted = build(&config(dir.path(), rotate))
                .unwrap_or_else(|e| panic!("{rotate:?}: the restart was refused: {e}"));
            drop(restarted);
        }
    }
}
