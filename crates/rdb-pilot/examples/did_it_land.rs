// The snippet in `docs/rdb/embed.md`, section "After a reopen: did my write land?", compiled.
//
// The text between the two marker lines is the section's code block, byte for byte; the
// pilot's row `the_did_it_land_snippet_in_embed_md_is_the_one_that_compiles` checks that.
// It needs a running rEtcd and a saved request to do anything, so `main` only names it.
// rdb-api's row `m10_r357_the_guide_s_did_it_land_answers_an_app_restarted_over_the_links`
// includes this file and runs it against a real `Db` (ruling A-R359), which is why these are
// plain comments: an included file may not carry inner doc comments.

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

/// What became of a saved request, and what the app does next.
#[derive(Debug, PartialEq, Eq)]
enum Landed {
    /// It landed. Delete the saved copy.
    Yes,
    /// An answer for it came earlier, or it is too old to settle. Not proof either way: read
    /// the object to see its value, then delete the saved copy.
    ReadTheObject,
    /// Not known yet, which is not proof that it did not land: ask again in a few seconds, or
    /// read the object. Never send it as a new write.
    NotKnown,
}

/// What became of request `id`. `sent_in`: the generation it was sent under, when you know it.
fn landed(db: &Db, id: RequestId, sent_in: Option<Generation>) -> Result<Landed, ApiError> {
    Ok(match db.status(id, sent_in)? {
        TxnStatus::Resolved(_) => Landed::Yes,
        TxnStatus::Expired => Landed::ReadTheObject,
        TxnStatus::Unknown | TxnStatus::Unresolved { .. } => Landed::NotKnown,
    })
}

/// The same question for a request saved because its answer was not definitive:
/// `refused.request` of an `UNKNOWN_OUTCOME`. `Err`: this resend was refused for another
/// reason (`UNAVAILABLE`, say); its text says when to ask again.
fn did_it_land(db: &Db, saved: TxnRequest) -> Result<Landed, ApiError> {
    let (id, sent_in) = (saved.identity.request, saved.expected_generation);
    let refused = match db.resend(saved) {
        Ok(_) => return Ok(Landed::Yes), // applied now, or its answer replayed
        Err(refused) => refused.error,
    };
    match refused.kind {
        // Its answer was dropped: answered earlier, or too old. This resend applied nothing.
        ErrorKind::StatusExpired => Ok(Landed::ReadTheObject),
        // An embedded folder was reopened: this send applied nothing; ask about the first one.
        ErrorKind::GenerationChanged => landed(db, id, sent_in),
        _ => Err(refused),
    }
}
// embed.md end

fn main() {
    let _ = (put_saved, did_it_land);
}
