//! Driving `config-server` as a real child process (test plan TA-20, TA-26).
//!
//! This lives in the binary's own test directory rather than in `config-testkit` for one
//! mechanical reason: `CARGO_BIN_EXE_<name>` is only defined for integration tests of the
//! package that declares the binary (TA-25). Putting the driver in the testkit would mean the
//! testkit depending on a binary crate, which cargo does not express.
//!
//! # What this guarantees
//!
//! * **No orphans.** [`DaemonProcess`] kills its child on `Drop`, so a panicking test cannot
//!   leave a node holding a RocksDB lock or a port (TA-20.4, E2E-17).
//! * **No fixed sleeps.** Readiness is the child's own stdout line; exit is polled through
//!   `try_wait` under a derived deadline (anti-flake rules 1-3).
//! * **Diagnosable failures.** stdout and stderr are drained by reader threads into buffers, so
//!   a refusal's message is available to the assertion that failed, and a child can never block
//!   on a full pipe.

#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use config_testkit::poll::{poll_until, Timeout};

/// The binary under test, as cargo built it for this run.
pub const BINARY: &str = env!("CARGO_BIN_EXE_config-server");

/// How often a child's exit status is checked.
const EXIT_POLL: Duration = Duration::from_millis(25);

/// Everything one `config-server` invocation needs, in flag terms.
///
/// The *node* is described by the TOML file at [`DaemonSpec::config`]; this type only carries
/// what belongs to this run of the process (ADR-0018 §1).
#[derive(Debug, Clone)]
pub struct DaemonSpec {
    /// `--config`.
    pub config: PathBuf,
    /// `--log-dir`. One directory per node, so three daemons never append to one file.
    pub log_dir: PathBuf,
    /// `--shutdown-file`. Created by [`DaemonProcess::stop_gracefully`].
    pub shutdown_file: PathBuf,
    /// `--health-listen`.
    pub health_listen: Option<SocketAddr>,
    /// `--form`.
    pub form: bool,
    /// `--allow-insecure-dev`.
    pub allow_insecure_dev: bool,
    /// `--dev-allow-all`.
    pub dev_allow_all: bool,
    /// `--unsafe-no-sync`.
    pub unsafe_no_sync: bool,
    /// `--capabilities`.
    pub capabilities: bool,
    /// `--break-glass-policy-rollback` (M6, ADR-0027). Process-scoped, not one-shot (OQ-57):
    /// every rollback is permitted for the lifetime of a process spawned with this set.
    pub break_glass_policy_rollback: bool,
    /// `--compat-schema N` (M6, ADR-0030). Only `1` is accepted by the binary; `None` omits the
    /// flag entirely, which is every pre-E2E-42 row's argv, byte for byte.
    pub compat_schema: Option<u16>,
    /// Repeated `--log-field k=v`. The harness always passes `testModule`/`testMethod` so the
    /// cross-process DuckDB joins work (OQ-18).
    pub log_fields: Vec<(String, String)>,
}

impl DaemonSpec {
    /// A spec for a node directory laid out by the harness.
    pub fn new(
        config: impl Into<PathBuf>,
        log_dir: impl Into<PathBuf>,
        shutdown_file: impl Into<PathBuf>,
    ) -> Self {
        Self {
            config: config.into(),
            log_dir: log_dir.into(),
            shutdown_file: shutdown_file.into(),
            health_listen: None,
            form: false,
            allow_insecure_dev: false,
            dev_allow_all: false,
            unsafe_no_sync: false,
            capabilities: false,
            break_glass_policy_rollback: false,
            compat_schema: None,
            log_fields: Vec::new(),
        }
    }

    /// The argument vector this spec becomes.
    pub fn args(&self) -> Vec<String> {
        let mut args = vec![
            "--config".to_string(),
            self.config.display().to_string(),
            "--log-dir".to_string(),
            self.log_dir.display().to_string(),
            "--shutdown-file".to_string(),
            self.shutdown_file.display().to_string(),
        ];
        if let Some(addr) = self.health_listen {
            args.push("--health-listen".into());
            args.push(addr.to_string());
        }
        for (flag, on) in [
            ("--form", self.form),
            ("--allow-insecure-dev", self.allow_insecure_dev),
            ("--dev-allow-all", self.dev_allow_all),
            ("--unsafe-no-sync", self.unsafe_no_sync),
            ("--capabilities", self.capabilities),
            (
                "--break-glass-policy-rollback",
                self.break_glass_policy_rollback,
            ),
        ] {
            if on {
                args.push(flag.into());
            }
        }
        if let Some(schema) = self.compat_schema {
            args.push("--compat-schema".into());
            args.push(schema.to_string());
        }
        for (k, v) in &self.log_fields {
            args.push("--log-field".into());
            args.push(format!("{k}={v}"));
        }
        args
    }
}

/// The daemon's one stdout line (ADR-0018 §3).
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct Ready {
    /// Always `true`.
    pub ready: bool,
    /// The node that printed it.
    pub node_id: u64,
    /// Bound peer-plane address.
    pub peer: String,
    /// Bound client-plane address.
    pub client: String,
    /// Bound gossip address, when gossip is configured.
    #[serde(default)]
    pub gossip: Option<String>,
    /// Bound health address, when `--health-listen` was given.
    #[serde(default)]
    pub health: Option<String>,
}

/// A running (or finished) `config-server` child process.
pub struct DaemonProcess {
    spec: DaemonSpec,
    child: Option<Child>,
    lines: Receiver<String>,
    stdout: Arc<Mutex<Vec<String>>>,
    stderr: Arc<Mutex<String>>,
    ready: Option<Ready>,
}

impl std::fmt::Debug for DaemonProcess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DaemonProcess")
            .field("config", &self.spec.config)
            .field("pid", &self.child.as_ref().map(Child::id))
            .field("ready", &self.ready)
            .finish()
    }
}

impl DaemonProcess {
    /// Spawn the daemon. Does not wait for readiness.
    pub fn spawn(spec: DaemonSpec) -> Self {
        let mut child = Command::new(BINARY)
            .args(spec.args())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {BINARY}: {e}"));

        let stdout_buf = Arc::new(Mutex::new(Vec::new()));
        let stderr_buf = Arc::new(Mutex::new(String::new()));
        let (tx, rx) = std::sync::mpsc::channel();

        let out = child.stdout.take().expect("stdout is piped");
        let sink = Arc::clone(&stdout_buf);
        std::thread::spawn(move || {
            for line in BufReader::new(out).lines().map_while(Result::ok) {
                sink.lock().expect("stdout buffer").push(line.clone());
                // A closed receiver just means the test finished; keep draining the pipe.
                let _ = tx.send(line);
            }
        });

        let err = child.stderr.take().expect("stderr is piped");
        let sink = Arc::clone(&stderr_buf);
        std::thread::spawn(move || {
            for line in BufReader::new(err).lines().map_while(Result::ok) {
                let mut guard = sink.lock().expect("stderr buffer");
                guard.push_str(&line);
                guard.push('\n');
            }
        });

        Self {
            spec,
            child: Some(child),
            lines: rx,
            stdout: stdout_buf,
            stderr: stderr_buf,
            ready: None,
        }
    }

    /// Block until the ready line arrives, or `deadline` passes.
    ///
    /// Blocking rather than async on purpose: it waits on the child's pipe, not on the tokio
    /// runtime, and the caller's runtime has nothing to make progress on meanwhile.
    pub fn wait_ready(&mut self, deadline: Duration) -> Result<Ready, String> {
        match self.lines.recv_timeout(deadline) {
            Ok(line) => {
                let ready: Ready = serde_json::from_str(&line).map_err(|e| {
                    format!("the first stdout line is not a ready line ({e}): {line:?}")
                })?;
                self.ready = Some(ready.clone());
                Ok(ready)
            }
            Err(RecvTimeoutError::Timeout) => Err(format!(
                "no ready line within {deadline:?}; stderr:\n{}",
                self.stderr()
            )),
            Err(RecvTimeoutError::Disconnected) => Err(format!(
                "the daemon exited before printing a ready line; stderr:\n{}",
                self.stderr()
            )),
        }
    }

    /// The ready line, for a daemon that has produced one.
    pub fn ready(&self) -> &Ready {
        self.ready
            .as_ref()
            .expect("wait_ready() must succeed before the endpoints are read")
    }

    /// This node's client-plane endpoint.
    pub fn client_endpoint(&self) -> &str {
        &self.ready().client
    }

    /// This node's health endpoint.
    pub fn health_endpoint(&self) -> &str {
        self.ready()
            .health
            .as_deref()
            .expect("this daemon was started with --health-listen")
    }

    /// This node's id.
    pub fn node_id(&self) -> u64 {
        self.ready().node_id
    }

    /// This child process's OS process id.
    ///
    /// A row that proves a reload never restarts the daemon (E2E-41) reads this before and
    /// after, rather than trusting that "no `wait_ready` was called again" implies "the same
    /// process": a PID is the one thing the OS itself guarantees does not survive an exit/respawn
    /// cycle.
    pub fn pid(&self) -> u32 {
        self.child
            .as_ref()
            .expect("the process has not been reaped")
            .id()
    }

    /// Everything the child has written to stdout so far.
    pub fn stdout_lines(&self) -> Vec<String> {
        self.stdout.lock().expect("stdout buffer").clone()
    }

    /// Everything the child has written to stderr so far.
    pub fn stderr(&self) -> String {
        self.stderr.lock().expect("stderr buffer").clone()
    }

    /// The spec this process was spawned from.
    pub fn spec(&self) -> &DaemonSpec {
        &self.spec
    }

    /// This node's JSONL log file (`<log-dir>/<node_id>.jsonl`).
    pub fn log_file(&self) -> PathBuf {
        self.spec.log_dir.join(format!("{}.jsonl", self.node_id()))
    }

    /// Ask for a graceful shutdown and wait for exit code 0 (ADR-0018 §4, OQ-17).
    pub async fn stop_gracefully(&mut self, deadline: Duration) -> ExitStatus {
        let status = self.request_shutdown(deadline).await;
        assert_eq!(
            status.code(),
            Some(0),
            "graceful shutdown must exit 0; stderr:\n{}",
            self.stderr()
        );
        status
    }

    /// Ask for a graceful shutdown and return whatever the process exited with.
    pub async fn request_shutdown(&mut self, deadline: Duration) -> ExitStatus {
        std::fs::write(&self.spec.shutdown_file, b"stop")
            .unwrap_or_else(|e| panic!("write {}: {e}", self.spec.shutdown_file.display()));
        self.wait(deadline)
            .await
            .unwrap_or_else(|e| panic!("daemon did not exit after the shutdown file appeared: {e}"))
    }

    /// Kill the process without warning — the crash half of TA-26.
    pub fn kill(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }

    /// Poll until the process has exited, or `deadline` passes.
    pub async fn wait(&mut self, deadline: Duration) -> Result<ExitStatus, Timeout> {
        let Some(child) = self.child.as_mut() else {
            return Err(Timeout {
                elapsed: Duration::ZERO,
                last_diagnostic: "the process was already reaped".into(),
            });
        };
        let status = poll_until(deadline, EXIT_POLL, || {
            child.try_wait().expect("try_wait on a spawned child")
        })
        .await?;
        self.child = None;
        Ok(status)
    }

    /// Whether the process is still running.
    pub fn is_running(&mut self) -> bool {
        match self.child.as_mut() {
            Some(child) => matches!(child.try_wait(), Ok(None)),
            None => false,
        }
    }
}

impl Drop for DaemonProcess {
    fn drop(&mut self) {
        // A panicking test must not leave a node holding the data directory or a port.
        self.kill();
    }
}

/// Run `config-server` to completion and return `(exit code, stdout, stderr)`.
///
/// For the rows that are about a *refusal* (E2E-12) or about `--capabilities` (E2E-02), where
/// there is no ready line to wait for.
pub fn run_to_completion(spec: &DaemonSpec) -> (Option<i32>, String, String) {
    // Bounded: a row that expects the daemon to refuse and exit must not hang the suite when
    // the daemon instead starts normally (a spec without `--form`, say). On timeout the child
    // is killed and the exit code is `None`, with the reason appended to stderr.
    let deadline = super::startup_deadline();
    let mut child = Command::new(BINARY)
        .args(spec.args())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {BINARY}: {e}"));
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let out_reader = std::thread::spawn(move || read_to_string(stdout));
    let err_reader = std::thread::spawn(move || read_to_string(stderr));

    let started = std::time::Instant::now();
    let mut timed_out = false;
    let status = loop {
        match child.try_wait().expect("try_wait on the daemon") {
            Some(status) => break Some(status),
            None if started.elapsed() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                timed_out = true;
                break None;
            }
            // A bounded poll on a child process, not synchronization: the loop exits on the
            // child's exit or on `deadline`, never on the sleep itself.
            None => std::thread::sleep(Duration::from_millis(20)), // testkit:allow-sleep
        }
    };
    let stdout = out_reader.join().expect("stdout reader thread");
    let mut stderr = err_reader.join().expect("stderr reader thread");
    if timed_out {
        stderr.push_str(&format!(
            "
[harness] run_to_completion: the daemon did not exit within {deadline:?}; killed
"
        ));
    }
    (status.and_then(|s| s.code()), stdout, stderr)
}

fn read_to_string(mut pipe: impl std::io::Read) -> String {
    let mut bytes = Vec::new();
    let _ = pipe.read_to_end(&mut bytes);
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Forward-slashed glob over every daemon log file under `root`, for
/// `read_json_auto('<glob>', union_by_name=true)`.
///
/// Deliberately not in `config_testkit::logs`: the daemons write outside `target/test-logs`,
/// into the per-test temp tree, and only this crate knows that layout.
pub fn daemon_logs_glob(root: &Path) -> String {
    format!(
        "{}/**/*.jsonl",
        root.display().to_string().replace('\\', "/")
    )
}
