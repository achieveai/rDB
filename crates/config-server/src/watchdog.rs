//! Stall watchdog (A-R309).
//!
//! A control-plane leader stopped running tasks for 2.7-3.3 s, and the logs could not say why
//! (f4-flush §5). Two clocks tell the cases apart:
//!
//! * an OS thread that sleeps [`TICK`] and logs `process_stall` when it overslept by more than
//!   [`REPORT_OVER`]: the OS did not run this process;
//! * a tokio task that does the same and logs `runtime_stall`: the runtime's workers were
//!   blocked, while the process itself ran.
//!
//! Only `runtime_stall` means something blocks the workers; both mean the host suspended the
//! process; neither, across a silent gap in the log, points at the log path.
//!
//! Each loop measures its gap before it logs, so the measurement never waits on the log.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tracing::Instrument;

/// How often each clock wakes.
pub const TICK: Duration = Duration::from_millis(100);
/// A gap longer than this is logged.
pub const REPORT_OVER: Duration = Duration::from_millis(500);

/// Stops both clocks when dropped, so neither logs after the daemon's last line.
pub struct Watchdog(Arc<AtomicBool>);

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Start both clocks, their lines inside `span`. They run until the [`Watchdog`] is dropped.
pub fn start(runtime: &tokio::runtime::Handle, span: &tracing::Span) -> std::io::Result<Watchdog> {
    let stop = Arc::new(AtomicBool::new(false));
    let thread_span = span.clone();
    let thread_stop = Arc::clone(&stop);
    std::thread::Builder::new()
        .name("stall-watchdog".into())
        .spawn(move || process_clock(&thread_span, TICK, REPORT_OVER, &thread_stop))?;
    runtime.spawn(runtime_clock(TICK, REPORT_OVER, Arc::clone(&stop)).instrument(span.clone()));
    Ok(Watchdog(stop))
}

/// The OS-thread clock: `process_stall` when this thread overslept by more than `over`.
fn process_clock(span: &tracing::Span, tick: Duration, over: Duration, stop: &AtomicBool) {
    let mut ticks: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        let from_unix_ms = unix_ms();
        let started = Instant::now();
        std::thread::sleep(tick);
        let gap = started.elapsed();
        ticks += 1;
        if gap > over && !stop.load(Ordering::Relaxed) {
            let (gap_ms, tick_ms) = (ms(gap), ms(tick));
            span.in_scope(|| {
                tracing::warn!(gap_ms, tick = ticks, tick_ms, from_unix_ms, "process_stall");
            });
        }
    }
}

/// The tokio clock: `runtime_stall` when this task woke more than `over` late.
async fn runtime_clock(tick: Duration, over: Duration, stop: Arc<AtomicBool>) {
    let mut ticks: u64 = 0;
    while !stop.load(Ordering::Relaxed) {
        let from_unix_ms = unix_ms();
        let started = Instant::now();
        tokio::time::sleep(tick).await;
        let gap = started.elapsed();
        ticks += 1;
        if gap > over {
            let (gap_ms, tick_ms) = (ms(gap), ms(tick));
            tracing::warn!(gap_ms, tick = ticks, tick_ms, from_unix_ms, "runtime_stall");
        }
    }
}

/// A stall line's fields: `gap_ms` the measured wait, `tick` the tick that came late, `tick_ms`
/// the wait it expected, and `from_unix_ms` the wall clock when that wait began, to line a gap
/// up with other processes.
fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Barrier, Mutex};

    use tracing::instrument::WithSubscriber;

    use super::*;

    /// Everything the clocks logged, as text.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("capture").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl tracing_subscriber::fmt::MakeWriter<'_> for Captured {
        type Writer = Self;

        fn make_writer(&self) -> Self::Writer {
            self.clone()
        }
    }

    /// The gap_ms of every `name` line in `text`.
    fn gaps(text: &str, name: &str) -> Vec<u64> {
        text.lines()
            .filter(|line| line.contains(name))
            .filter_map(|line| {
                let at = line.find("gap_ms=")? + "gap_ms=".len();
                line[at..]
                    .split(|c: char| !c.is_ascii_digit())
                    .next()?
                    .parse()
                    .ok()
            })
            .collect()
    }

    /// A-R309 (a): with every runtime worker blocked for 1 s, the tokio clock logs
    /// `runtime_stall` with a gap of at least 900 ms, and the OS-thread clock, which kept
    /// running, logs no `process_stall`. That is the split the watchdog exists for.
    #[test]
    fn blocked_workers_log_a_runtime_stall_and_no_process_stall() {
        let sink = Captured::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .with_writer(sink.clone())
                .finish(),
        );
        const WORKERS: usize = 2;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(WORKERS)
            .enable_all()
            .build()
            .expect("runtime");
        let stop = Arc::new(AtomicBool::new(false));
        let clock = runtime.spawn(
            runtime_clock(TICK, REPORT_OVER, Arc::clone(&stop)).with_subscriber(dispatch.clone()),
        );
        let thread = {
            let (dispatch, stop) = (dispatch.clone(), Arc::clone(&stop));
            std::thread::spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    process_clock(&tracing::Span::none(), TICK, REPORT_OVER, &stop);
                });
            })
        };
        std::thread::sleep(Duration::from_millis(300));

        // Each blocker waits at the barrier first, so all of them hold a worker at once.
        let barrier = Arc::new(Barrier::new(WORKERS));
        let blockers: Vec<_> = (0..WORKERS)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                runtime.spawn(async move {
                    barrier.wait();
                    std::thread::sleep(Duration::from_secs(1));
                })
            })
            .collect();
        for blocker in blockers {
            runtime.block_on(blocker).expect("blocker");
        }
        std::thread::sleep(Duration::from_millis(400));
        stop.store(true, Ordering::Relaxed);
        thread.join().expect("process clock");
        runtime.block_on(clock).expect("runtime clock");

        let text = String::from_utf8(sink.0.lock().expect("capture").clone()).expect("utf-8");
        let runtime_gaps = gaps(&text, "runtime_stall");
        assert!(
            runtime_gaps.iter().any(|gap| *gap >= 900),
            "a 1 s block of every worker must log runtime_stall with gap_ms >= 900; got {runtime_gaps:?} in:\n{text}"
        );
        assert!(
            gaps(&text, "process_stall").is_empty(),
            "the OS thread kept running, so no process_stall: {text}"
        );
    }
}
