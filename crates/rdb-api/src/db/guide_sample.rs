// embed.md's "A transaction and a map" code, compiled and run (lead ruling A-R357, G6).
//
// The text between the two marker lines is that section's code block, byte for byte; the row
// `the_txn_and_map_sample_in_embed_md_is_the_one_that_compiles` in `reopen_tests.rs` checks it,
// and `the_txn_and_map_sample_runs_against_a_db` runs both functions against a real `Db`.

// embed.md begin
use rdb_api::db::DEFAULT_GROUP;
use rdb_api::{Change, Db, ElemOp, Look, Seen, TxnPut, Value};
use std::error::Error;
use std::time::Duration;

/// Two objects in one transaction: both are written, or neither. `if_version: Some(v)` writes
/// only while the object is still at version `v`; a change since then refuses the whole
/// transaction with `CONDITION_FAILED`, nothing written.
fn transfer(db: &Db) -> Result<(), Box<dyn Error>> {
    let seen = db.get(b"acct-a")?.value.map(|(version, _)| version);
    let ok = db
        .txn(
            &[
                TxnPut {
                    group: DEFAULT_GROUP, // where `put` and `get` work
                    object: b"acct-a",
                    value: b"90",
                    if_version: seen,
                },
                TxnPut {
                    group: DEFAULT_GROUP,
                    object: b"acct-b",
                    value: b"10",
                    if_version: None, // write over whatever is there, or create it
                },
            ],
            Duration::from_secs(5), // the deadline; at most 30 s
        )
        .map_err(|refused| refused.error)?;
    println!("both written at {:?}", ok.seq);
    Ok(())
}

/// A map: put two entries in one write, then read every entry, 100 at a time.
fn profile(db: &Db) -> Result<Vec<(Value, Value)>, Box<dyn Error>> {
    let text = |s: &str| Value::Text(s.to_owned());
    db.write(
        b"profile",
        Change::Map(vec![
            ElemOp::Put(text("name"), text("ada")),
            ElemOp::Put(text("lang"), text("rust")),
        ]),
        None,
    )
    .map_err(|refused| refused.error)?;
    let mut entries = Vec::new();
    let mut after = None; // start from the first entry
    loop {
        let read = db.read(b"profile", Look::Members { after, limit: 100 })?;
        let Seen::Members(page) = read.seen else {
            return Err(format!("profile is not a map: {:?}", read.seen).into());
        };
        let full = page.members.len() == 100;
        after = page.members.last().map(|last| last.key.clone());
        for member in page.members {
            entries.extend(member.value.map(|value| (member.key, value)));
        }
        if !full {
            return Ok(entries);
        }
    }
}
// embed.md end
