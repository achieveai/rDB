//! The snippet in `docs/rdb/embed.md`, section "After a reopen: did my write land?", compiled.
//!
//! The text between the two marker lines is the section's code block, byte for byte; the
//! pilot's row `the_did_it_land_snippet_in_embed_md_is_the_one_that_compiles` checks that.
//! It needs a running rEtcd and a saved request to do anything, so `main` only names it.

// embed.md begin
use rdb_api::{ApiError, Db};
use rdb_core::{ErrorKind, TxnRequest, TxnStatus};

/// `true`: it landed. `false`: not known yet, which is not proof that it did not land.
fn did_it_land(db: &Db, saved: TxnRequest) -> Result<bool, ApiError> {
    let id = saved.identity.request;
    match db.resend(saved) {
        Ok(_) => return Ok(true), // same generation: applied now, or its answer replayed
        Err(refused) if refused.error.kind != ErrorKind::GenerationChanged => {
            return Err(refused.error)
        }
        Err(_) => {} // after a reopen: this send applied nothing; ask what the first one did
    }
    Ok(matches!(db.status(id, None)?, TxnStatus::Resolved(_)))
}
// embed.md end

fn main() {
    let _ = did_it_land;
}
