// embed.md's "Ownership events" code, compiled and run (#83 B1, lead ruling item 6).
//
// The text between the two marker lines is that section's code block, byte for byte; the row
// `the_ownership_events_sample_in_embed_md_is_the_one_that_compiles` in `reopen_tests.rs`
// checks it, and the rows beside it run `follow` on a real `Db` and on scripted streams.

// embed.md begin
use rdb_api::{Db, OwnerToken, OwnershipEvent, PartitionId};
use std::collections::BTreeSet;
use std::sync::mpsc::RecvTimeoutError;
use std::time::{Duration, Instant};

/// What your app does on each event: start, wind down and stop its own work.
pub trait OnOwnership {
    fn start(&mut self, partition: PartitionId, token: OwnerToken);
    fn wind_down(&mut self, partition: PartitionId, token: OwnerToken, deadline: Instant);
    fn stop(&mut self, partition: PartitionId, token: OwnerToken);
    fn shutting_down(&self) -> bool;
}

/// How often a quiet stream checks for shutdown. A healthy owner can send nothing for hours.
pub const CHECK_EVERY: Duration = Duration::from_millis(100);
/// How long to wait after a stream ends before subscribing again.
pub const RESUBSCRIBE_AFTER: Duration = Duration::from_millis(500);

/// Follows `db`'s ownership events until `app` shuts down.
///
/// When a stream ends, nothing says what it missed: a `Deactivated` may be lost, and the next
/// stream's first events list only what is held then. So everything started or warned on the
/// ended stream is stopped first (stop and reset), and the next stream starts from scratch.
pub fn follow(db: &Db, app: &mut impl OnOwnership) {
    // Every (partition, token) the current stream started or warned about, not yet stopped.
    let mut held = BTreeSet::new();
    while !app.shutting_down() {
        let events = db.ownership_events();
        loop {
            match events.recv_timeout(CHECK_EVERY) {
                Ok(OwnershipEvent::Activated {
                    partition, token, ..
                }) => {
                    held.insert((partition, token));
                    app.start(partition, token);
                }
                Ok(OwnershipEvent::Deactivating {
                    partition,
                    token,
                    deadline,
                    ..
                }) => {
                    held.insert((partition, token));
                    app.wind_down(partition, token, deadline);
                }
                Ok(OwnershipEvent::Deactivated {
                    partition, token, ..
                }) => {
                    held.remove(&(partition, token));
                    app.stop(partition, token);
                }
                // Quiet is healthy: keep everything held, and look at the shutdown flag.
                Err(RecvTimeoutError::Timeout) => {}
                // The stream ended.
                Err(RecvTimeoutError::Disconnected) => break,
            }
            if app.shutting_down() {
                return;
            }
        }
        // Stop and reset. Never keep work because the next stream does not mention it.
        for (partition, token) in std::mem::take(&mut held) {
            app.stop(partition, token);
        }
        // Wait before subscribing again (a `Db` that runs no node ends every stream at once).
        let until = Instant::now() + RESUBSCRIBE_AFTER;
        while !app.shutting_down() && Instant::now() < until {
            std::thread::sleep(CHECK_EVERY.min(until.saturating_duration_since(Instant::now())));
        }
    }
}
// embed.md end
