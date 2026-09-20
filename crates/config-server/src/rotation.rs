//! The daemon's half of TLS credential rotation (M6, ADR-0028).
//!
//! The rotation itself is [`config_grpc::TlsRotator`]: it owns the listeners' credentials, the
//! peer dialler, and the compile-before-swap discipline that makes a half-written certificate a
//! refusal rather than half a rotation. What lives here is the *schedule*, because
//! `tls.watch_files_secs` is a daemon configuration key and a transport library given its own
//! timer would be a library with an opinion about a file it was never handed.
//!
//! # Why a timer and not a filesystem watch
//!
//! Same answer as the signed-policy poller (ADR-0027): a watch is a per-platform API with
//! per-platform silent-failure modes, while the guarantee an operator actually needs is "the
//! new file is in force within a known bound", which a timer states outright.

use std::sync::Arc;
use std::time::Duration;

use config_grpc::TlsRotator;

/// Re-read the configured files every `interval` until `shutdown` fires.
///
/// A failed poll is counted and logged by [`TlsRotator::reload`] and the loop continues: the
/// node is still serving the material it had, and stopping the poller because one read failed
/// would turn a transient half-written file into a permanent refusal to rotate.
///
/// Started one step later than the planes it rotates, so the first tick cannot replace one
/// plane's credentials on a node whose other plane is still binding.
pub fn spawn_tls_poller(
    rotator: &Arc<TlsRotator>,
    interval: Duration,
    shutdown: Arc<tokio::sync::Notify>,
) -> tokio::task::JoinHandle<()> {
    let rotator = Arc::clone(rotator);
    tokio::spawn(config_log::testing::in_current_span(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick completes immediately and startup already loaded these files.
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                () = shutdown.notified() => return,
                _ = ticker.tick() => {}
            }
            let poll = Arc::clone(&rotator);
            // Blocking: three file reads. On the blocking pool for the same reason the policy
            // loader's reload is, and an error from it is either the pool being gone during
            // shutdown or the reload panicking — a node that keeps serving on credentials it
            // will never rotate again (F-003).
            if let Err(error) = tokio::task::spawn_blocking(move || poll.reload("poll")).await {
                crate::logging::poller_stopped("tls", &error);
                return;
            }
        }
    }))
}
