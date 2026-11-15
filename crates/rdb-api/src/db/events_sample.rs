// embed.md's "Ownership events" code, compiled and run (#83 B1, lead ruling item 6).
//
// The text between the two marker lines is that section's code block, byte for byte; the row
// `the_ownership_events_sample_in_embed_md_is_the_one_that_compiles` in `reopen_tests.rs`
// checks it, and `the_ownership_events_sample_runs_against_a_db` runs `follow` on a real `Db`.

// embed.md begin
use rdb_api::{Db, OwnerToken, OwnershipEvent, PartitionId};
use std::time::Instant;

/// What your app does on each event: start, wind down and stop its own work.
pub trait OnOwnership {
    fn start(&mut self, partition: PartitionId, token: OwnerToken);
    fn wind_down(&mut self, partition: PartitionId, token: OwnerToken, deadline: Instant);
    fn stop(&mut self, partition: PartitionId, token: OwnerToken);
    fn shutting_down(&self) -> bool;
}

/// Follows `db`'s ownership events until `app` shuts down. A subscriber ends when it falls
/// behind or a node stops; the next one starts with the current state, so nothing is missed.
pub fn follow(db: &Db, app: &mut impl OnOwnership) {
    while !app.shutting_down() {
        for event in db.ownership_events().iter() {
            match event {
                OwnershipEvent::Activated { partition, token, .. } => app.start(partition, token),
                OwnershipEvent::Deactivating {
                    partition,
                    token,
                    deadline,
                    ..
                } => app.wind_down(partition, token, deadline),
                OwnershipEvent::Deactivated { partition, token, .. } => app.stop(partition, token),
            }
            if app.shutting_down() {
                return;
            }
        }
    }
}
// embed.md end
