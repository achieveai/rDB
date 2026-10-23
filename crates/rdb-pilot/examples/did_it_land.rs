//! The snippet in `docs/rdb/embed.md`, section "After a reopen: did my write land?", compiled.
//!
//! The text between the two marker lines is the section's code block, byte for byte; the
//! pilot's row `the_did_it_land_snippet_in_embed_md_is_the_one_that_compiles` checks that.
//! It needs a running rEtcd and a saved request to do anything, so `main` only names it.

// embed.md begin
use std::error::Error;
use std::fs::File;
use std::io::Write;

use rdb_api::{
    app_request_id, ApiError, Db, ErrorKind, Generation, PutOk, RequestId, TxnRequest, TxnStatus,
};

/// A put you can look up after a kill: its id is saved, synced, before it is sent.
/// `n` is your own number for it, used once; the id is `app_request_id(n)`.
fn put_saved(
    db: &Db,
    ids: &mut File,
    n: u64,
    object: &[u8],
    value: &[u8],
) -> Result<PutOk, Box<dyn Error>> {
    let id = app_request_id(n).ok_or("n must be below 2^63")?;
    writeln!(ids, "{}", id.0)?;
    ids.sync_data()?;
    Ok(db
        .put_as(id, object, value, None)
        .map_err(|refused| refused.error)?)
}

/// `true`: request `id` landed. `false`: not known, which is not proof that it did not land.
/// `sent_in`: the generation it was sent under, when you know it.
fn landed(db: &Db, id: RequestId, sent_in: Option<Generation>) -> Result<bool, ApiError> {
    Ok(matches!(db.status(id, sent_in)?, TxnStatus::Resolved(_)))
}

/// The same question for a request saved after its answer (`ok.sent` or `refused.request`).
fn did_it_land(db: &Db, saved: TxnRequest) -> Result<bool, ApiError> {
    let (id, sent_in) = (saved.identity.request, saved.expected_generation);
    match db.resend(saved) {
        Ok(_) => return Ok(true), // same generation: applied now, or its answer replayed
        Err(refused) if refused.error.kind != ErrorKind::GenerationChanged => {
            return Err(refused.error)
        }
        Err(_) => {} // after a reopen: this send applied nothing; ask what the first one did
    }
    landed(db, id, sent_in)
}
// embed.md end

fn main() {
    let _ = (put_saved, did_it_land);
}
