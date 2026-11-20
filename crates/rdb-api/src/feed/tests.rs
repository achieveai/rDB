//! Issue #87 slice 1: every branch of [`resume`] on fake chains, and [`page`]'s caps.
//! Unit (~1 ms each): no engine, no host.

use super::*;
use rdb_core::contracts::digest::Domain;
use rdb_core::contracts::ids::{ClientId, RequestId, TenantId};
use std::collections::BTreeMap;
use std::convert::Infallible;

/// A stand-in digest for the record at `seq` stamped `stamp`.
fn d(stamp: u64, seq: u64) -> Digest {
    Digest::of(Domain::Record, &[&stamp.to_le_bytes(), &seq.to_le_bytes()])
}

fn at(stamp: u64, seq: u64) -> FeedCursor {
    FeedCursor {
        generation: Generation(stamp),
        seq: Seq(seq),
        digest: d(stamp, seq),
    }
}

/// What a copy holds, read through its chain from the newest level: seq to stamp.
struct Held(BTreeMap<u64, u64>);

impl Held {
    /// Seqs `from..=to` stamped `stamp`.
    fn with(mut self, stamp: u64, from: u64, to: u64) -> Self {
        for seq in from..=to {
            self.0.insert(seq, stamp);
        }
        self
    }

    fn lookup(&self) -> impl FnMut(Seq) -> Result<Option<(Generation, Digest)>, Infallible> + '_ {
        |seq| {
            Ok(self
                .0
                .get(&seq.0)
                .map(|stamp| (Generation(*stamp), d(*stamp, seq.0))))
        }
    }
}

fn held() -> Held {
    Held(BTreeMap::new())
}

fn level(generation: u64, exit: Option<u64>) -> Level {
    Level {
        generation: Generation(generation),
        exit: exit.map(Seq),
    }
}

fn check(cursor: FeedCursor, chain: &[Level], cap: u64, floor: u64, held: &Held) -> Resume {
    let Ok(resume) = resume(cursor, chain, Seq(cap), Seq(floor), held.lookup());
    resume
}

/// R1's shape: G1 wrote 1..=120, G2 is a ReadOnly level that took no writes and left G1 at 120,
/// G3 was cut at 100 by `forget-tail`. A cursor at (G1, 110) passes G1's own exit (120) but not
/// the effective one (100): `Rewound` at once, to the record at 100, which carries stamp G1.
/// Cut: take the stamp level's own exit instead of the minimum on the path, red (`Behind`).
#[test]
fn f87_1_resume_g1_wrote_120_g2_readonly_g3_cut_at_100_is_rewound() {
    let chain = [level(3, None), level(2, Some(100)), level(1, Some(120))];
    let copy = held().with(1, 1, 100);
    assert_eq!(
        check(at(1, 110), &chain, 100, 1, &copy),
        Resume::Answer(FeedPage::Rewound { to: at(1, 100) })
    );
}

/// B1: a cut at the stamp's own level with nothing written since is `Rewound`, whatever the
/// head is. Cut: ignore the exit, red (`Behind`).
#[test]
fn f87_1_resume_cut_at_the_stamp_level_with_no_write_since_is_rewound() {
    let chain = [level(2, None), level(1, Some(100))];
    let copy = held().with(1, 1, 100);
    assert_eq!(
        check(at(1, 120), &chain, 100, 1, &copy),
        Resume::Answer(FeedPage::Rewound { to: at(1, 100) })
    );
    // A write since does not change the answer.
    let copy = copy.with(2, 101, 130);
    assert_eq!(
        check(at(1, 120), &chain, 130, 1, &copy),
        Resume::Answer(FeedPage::Rewound { to: at(1, 100) })
    );
}

/// A cut to 0 rewinds to `START`; a cut below the floor is `Trimmed`.
/// Cut: drop the cut-to-0 branch, red (`Trimmed`: no record at seq 0).
#[test]
fn f87_1_resume_cut_to_zero_is_start_and_below_the_floor_is_trimmed() {
    let chain = [level(2, None), level(1, Some(0))];
    assert_eq!(
        check(at(1, 5), &chain, 0, 1, &held()),
        Resume::Answer(FeedPage::Rewound {
            to: FeedCursor::START
        })
    );
    let chain = [level(2, None), level(1, Some(40))];
    let copy = held().with(1, 50, 60).with(2, 61, 70);
    assert_eq!(
        check(at(1, 45), &chain, 70, 50, &copy),
        Resume::Answer(FeedPage::Trimmed { floor: Seq(50) })
    );
}

/// M1: a stamp newer than the copy's newest landed generation is `Behind`, even where the copy
/// holds that seq. Cut: drop the newer-stamp test, red (`Rewound` to `START`).
#[test]
fn f87_1_resume_newer_generation_cursor_on_a_stale_copy_is_behind() {
    let chain = [level(3, None), level(1, Some(50))];
    let copy = held().with(1, 1, 50).with(3, 51, 130);
    assert_eq!(
        check(at(4, 120), &chain, 130, 1, &copy),
        Resume::Answer(FeedPage::Behind { head: Seq(130) })
    );
}

/// M2: an older stamp not on the chain is a fork this copy cannot locate: `Rewound` to
/// `START`, or `Trimmed` when this copy holds no seq 1.
/// Cut: serve an off-chain cursor as it stands, red.
#[test]
fn f87_1_resume_off_chain_cursor_is_rewound_to_start() {
    let chain = [level(3, None), level(1, Some(50))];
    let copy = held().with(1, 1, 50).with(3, 51, 60);
    assert_eq!(
        check(at(2, 55), &chain, 60, 1, &copy),
        Resume::Answer(FeedPage::Rewound {
            to: FeedCursor::START
        })
    );
    assert_eq!(
        check(at(2, 55), &chain, 60, 7, &copy),
        Resume::Answer(FeedPage::Trimmed { floor: Seq(7) })
    );
}

/// A tail adopted under #105 keeps its stamp L on the anchor's level: the cursor matches by
/// digest, though L is not on the chain. Cut: drop the digest test, red (`Rewound` to `START`).
#[test]
fn f87_1_resume_cursor_into_an_adopted_tail_matches_by_digest() {
    let chain = [level(4, None), level(2, Some(12))];
    // G2 wrote 1..=10; L = G3 wrote 11..=12, replayed onto G2's level with its own stamps.
    let copy = held().with(2, 1, 10).with(3, 11, 12).with(4, 13, 20);
    assert_eq!(
        check(at(3, 12), &chain, 20, 1, &copy),
        Resume::Serve(at(3, 12))
    );
}

/// Review r1 (PR #170): a copy linked generation 2 at seq 1, then caught up the kept seq 2 with
/// its old stamp 1 onto its generation-2 level. A consumer reading one record per call (`max`
/// 1) from `START` keeps the cursor `(1, 2)`, which passes generation 1's exit (1). Its exact
/// stamp and digest are visible at seq 2 within the cap, so the cursor is kept and the walk
/// reaches the cap with every record once. A consumer reopened with the saved cursor is served
/// too. Guards kept: the same seq holding another record (a real cut) is `Rewound`, and a cursor
/// past the cap is never served.
/// Cut: drop the visible-match check ahead of the exit (as at 3dc6d66), red: 9 `Rewound` to
/// `(1, 1)` in 20 calls, the walk never past seq 2.
#[test]
fn f87_1_resume_a_kept_record_caught_up_above_the_link_base_is_served_with_max_1() {
    let chain = [level(2, None), level(1, Some(1))];
    let copy = held().with(1, 1, 2).with(2, 3, 4);
    let read = |seq: Seq| {
        let stamp = copy.0[&seq.0];
        let at = at(stamp, seq.0);
        Ok::<_, Infallible>(Scanned {
            at,
            bytes: 10,
            record: Some(FeedRecord {
                at,
                ..record(seq.0)
            }),
        })
    };
    let (mut cursor, mut got, mut rewinds) = (FeedCursor::START, Vec::new(), 0);
    for _ in 0..20 {
        if cursor.seq.0 == 4 {
            break;
        }
        match check(cursor, &chain, 4, 1, &copy) {
            Resume::Serve(after) => {
                let Ok(FeedPage::Records { records, next, .. }) = page(after, Seq(4), 1, read)
                else {
                    panic!("a page of records");
                };
                got.extend(records.iter().map(|record| record.at));
                cursor = next;
            }
            Resume::Answer(FeedPage::Rewound { to }) => {
                rewinds += 1;
                cursor = to;
            }
            other => panic!("at {cursor:?}: {other:?}"),
        }
    }
    assert_eq!(
        (got, cursor, rewinds),
        (vec![at(1, 1), at(1, 2), at(2, 3), at(2, 4)], at(2, 4), 0),
        "max-1 walk over the caught-up record"
    );
    assert_eq!(
        check(at(1, 2), &chain, 4, 1, &copy),
        Resume::Serve(at(1, 2)),
        "reopened with the saved cursor"
    );
    let cut = held().with(1, 1, 1).with(2, 2, 4);
    assert_eq!(
        check(at(1, 2), &chain, 4, 1, &cut),
        Resume::Answer(FeedPage::Rewound { to: at(1, 1) }),
        "seq 2 holds another record: a real cut"
    );
    assert_eq!(
        check(at(1, 2), &chain, 1, 1, &copy),
        Resume::Answer(FeedPage::Rewound { to: at(1, 1) }),
        "never served past the cap"
    );
}

/// The plain branches: `START` serves from 1 (or is `Trimmed` above floor 1), a cursor past the
/// cap is `Behind`, a held record with another digest is divergence, a missing one `Trimmed`,
/// and a match serves. Cut: skip the digest comparison, red (served).
#[test]
fn f87_1_resume_start_behind_diverged_missing_and_match() {
    let chain = [level(2, None), level(1, Some(10))];
    let copy = held().with(1, 1, 10).with(2, 11, 20);
    assert_eq!(
        check(FeedCursor::START, &chain, 20, 1, &copy),
        Resume::Serve(FeedCursor::START)
    );
    assert_eq!(
        check(FeedCursor::START, &chain, 20, 3, &copy),
        Resume::Answer(FeedPage::Trimmed { floor: Seq(3) })
    );
    assert_eq!(
        check(at(2, 25), &chain, 20, 1, &copy),
        Resume::Answer(FeedPage::Behind { head: Seq(20) })
    );
    let forked = FeedCursor {
        digest: d(9, 15),
        ..at(2, 15)
    };
    assert_eq!(
        check(forked, &chain, 20, 1, &copy),
        Resume::Diverged {
            seq: Seq(15),
            held: d(2, 15)
        }
    );
    let holed = held().with(1, 1, 4).with(1, 6, 10).with(2, 11, 20);
    assert_eq!(
        check(at(1, 5), &chain, 20, 1, &holed),
        Resume::Answer(FeedPage::Trimmed { floor: Seq(1) })
    );
    assert_eq!(
        check(at(1, 7), &chain, 20, 1, &copy),
        Resume::Serve(at(1, 7))
    );
}

fn record(seq: u64) -> FeedRecord {
    FeedRecord {
        at: at(1, seq),
        request: RequestIdentity {
            tenant: TenantId(1),
            client: ClientId(1),
            request: RequestId(seq),
        },
        writes: Vec::new(),
    }
}

fn scanned(seq: u64, bytes: usize, keep: bool) -> Scanned {
    Scanned {
        at: at(1, seq),
        bytes,
        record: keep.then(|| record(seq)),
    }
}

fn records(page: &FeedPage) -> (Vec<u64>, u64, bool) {
    match page {
        FeedPage::Records {
            records,
            next,
            more,
        } => (
            records.iter().map(|record| record.at.seq.0).collect(),
            next.seq.0,
            *more,
        ),
        other => panic!("not a page of records: {other:?}"),
    }
}

/// Page caps: `max`, [`FEED_PAGE_RECORDS`], [`FEED_PAGE_BYTES`] (an oversized record still
/// advances, alone), and [`FEED_SCAN_RECORDS`] on a call that skips records, which returns an
/// empty page with `next` moved and `more` set (R3). `more` is unset only at the cap.
/// Cut: set `more` to `false` whatever stopped the page, red.
#[test]
fn f87_1_page_and_scan_caps_hold_and_oversize_record_still_advances() {
    let page_of = |cap: u64, max: u32, read: &dyn Fn(u64) -> Scanned| {
        let Ok(page) = page::<Infallible>(FeedCursor::START, Seq(cap), max, |seq| Ok(read(seq.0)));
        records(&page)
    };
    let small = |seq| scanned(seq, 10, true);
    assert_eq!(page_of(3, 10, &small), (vec![1, 2, 3], 3, false));
    assert_eq!(page_of(3, 2, &small), (vec![1, 2], 2, true));
    let (got, next, more) = page_of(1_000, u32::MAX, &small);
    assert_eq!(
        (got.len(), next, more),
        (
            FEED_PAGE_RECORDS as usize,
            u64::from(FEED_PAGE_RECORDS),
            true
        )
    );
    let huge = |seq| scanned(seq, FEED_PAGE_BYTES + 1, true);
    assert_eq!(page_of(3, 10, &huge), (vec![1], 1, true));
    let half = |seq| scanned(seq, FEED_PAGE_BYTES / 2, true);
    assert_eq!(page_of(5, 10, &half), (vec![1, 2], 2, true));
    let skipped = |seq| scanned(seq, 10, false);
    assert_eq!(
        page_of(5_000, 10, &skipped),
        (Vec::new(), u64::from(FEED_SCAN_RECORDS), true)
    );
    assert_eq!(page_of(5, 10, &skipped), (Vec::new(), 5, false));
    assert_eq!(page_of(0, 10, &small), (Vec::new(), 0, false));
}

/// Review r1 (PR #170): skipped records count toward [`FEED_SCAN_BYTES`], so a call that skips
/// large records reads a bounded number of bytes, not up to [`FEED_SCAN_RECORDS`] of them. With
/// 1 MiB records skipped, the call reads 8 (8 MiB), moves `next` past them and sets `more`; one
/// skipped record larger than the cap is still read and passed alone. The returned-byte cap
/// and `more` are unchanged, and a page that both returns and skips stops at the scan cap with
/// what it returned so far.
/// Cut: drop `examined < FEED_SCAN_BYTES` from the loop, red (1024 reads, 1 GiB examined).
#[test]
fn f87_1_skipped_bytes_count_toward_the_scan_cap() {
    let mib = 1 << 20;
    let count = |cap: u64, max: u32, read: &dyn Fn(u64) -> Scanned| {
        let calls = std::cell::Cell::new(0_u32);
        let Ok(page) = page::<Infallible>(FeedCursor::START, Seq(cap), max, |seq| {
            calls.set(calls.get() + 1);
            Ok(read(seq.0))
        });
        (records(&page), calls.get())
    };
    let skipped = |seq| scanned(seq, mib, false);
    assert_eq!(count(5_000, 10, &skipped), ((Vec::new(), 8, true), 8));
    let huge = |seq| scanned(seq, FEED_SCAN_BYTES + 1, false);
    assert_eq!(count(5_000, 10, &huge), ((Vec::new(), 1, true), 1));
    // Every other record returned: 3 returned (3 MiB) and 5 skipped by the 8th read.
    let mixed = |seq: u64| scanned(seq, mib, seq % 2 == 0 && seq <= 6);
    assert_eq!(count(5_000, 10, &mixed), ((vec![2, 4, 6], 8, true), 8));
    // Under the scan cap nothing changes: the page ends at the cap with `more` unset.
    assert_eq!(count(5, 10, &skipped), ((Vec::new(), 5, false), 5));
}
