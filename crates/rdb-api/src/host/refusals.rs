//! Refused writes at info, bounded (#158, option B).
//!
//! Every refused request is a `reply` line at debug. At info, a refusal costs at most one line
//! per key plus one summary per window, however many arrive: an overloaded owner refusing 500
//! writes a second logged ~530 B for each at info, about 23 GB a day.
//!
//! A key is `(reason, partition)`. The first refusal of a key is one `refusal_first` line at
//! info. Every refusal is counted in its window, and when a window with refusals closes, one
//! `refusal_summary` line at info reports them: the [`TOP`] keys by count, each with its first
//! and last request id, and the keys and refusals folded into `others_keys` and
//! `others_refusals`. So an isolated refusal costs 2 info lines (its first, and a summary with
//! count 1), which is intended: the summaries' counts add up to the debug `reply` lines.
//!
//! A window opens at a refusal and lasts the window given to [`RefusalLog::new`]. When it closes with
//! refusals in it, the next window opens at its end, so windows chain back to back while
//! refusals go on, and a key refused in every window is a first only once. When a window closes
//! with none, a whole window passed quietly: nothing is "previous" any more, no window stays
//! open, and the next refusal of any key is a first again.

use std::collections::{BTreeMap, BTreeSet};

use rdb_core::contracts::errors::ErrorKind;

/// How many keys a summary names; the rest fold into `others_keys` and `others_refusals`.
pub(crate) const TOP: usize = 16;

/// The production window: one summary line per 10 s while refusals go on.
pub(crate) const WINDOW_MS: u64 = 10_000;

/// A refusal's key: the error's kind and the partition.
pub(crate) type Key = (ErrorKind, u32);

/// One key's refusals in one window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tally {
    pub(crate) count: u64,
    pub(crate) first: u64,
    pub(crate) last: u64,
}

/// A closed window with refusals in it: what one `refusal_summary` line reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Summary {
    pub(crate) window_ms: u64,
    /// Distinct keys refused in the window.
    pub(crate) keys: usize,
    /// Refusals in the window, all keys.
    pub(crate) refusals: u64,
    /// Up to [`TOP`] keys, most refused first (ties by key).
    pub(crate) top: Vec<(Key, Tally)>,
    /// Keys beyond [`TOP`], and their refusals.
    pub(crate) others_keys: usize,
    pub(crate) others_refusals: u64,
}

impl Summary {
    /// `top` as a JSON array, one object per key: `reason`, `partition`, `count`, `first`,
    /// `last` (the request ids), so an operator can find the requests. Written by hand: every
    /// value is an integer or an error kind's identifier, so nothing needs escaping.
    pub(crate) fn top_json(&self) -> String {
        let entries: Vec<String> = self
            .top
            .iter()
            .map(|((reason, partition), tally)| {
                format!(
                    r#"{{"reason":"{reason:?}","partition":{partition},"count":{},"first":{},"last":{}}}"#,
                    tally.count, tally.first, tally.last
                )
            })
            .collect();
        format!("[{}]", entries.join(","))
    }
}

/// The window state. Time is the host clock's milliseconds, passed in.
#[derive(Debug)]
pub(crate) struct RefusalLog {
    window_ms: u64,
    /// The open window: its start, and its tallies so far.
    open: Option<(u64, BTreeMap<Key, Tally>)>,
    /// Keys refused in the window that ended where the open one began.
    previous: BTreeSet<Key>,
}

impl RefusalLog {
    pub(crate) fn new(window_ms: u64) -> Self {
        Self {
            window_ms: window_ms.max(1),
            open: None,
            previous: BTreeSet::new(),
        }
    }

    /// Count one refusal of `key` by `request` at `now`. Returns the summaries of the windows
    /// this closed first, oldest first, and whether this refusal is the key's first.
    pub(crate) fn record(&mut self, now: u64, key: Key, request: u64) -> (Vec<Summary>, bool) {
        let closed = self.roll(now);
        let previous = &self.previous;
        let (_, tallies) = self.open.get_or_insert_with(|| (now, BTreeMap::new()));
        let first = !tallies.contains_key(&key) && !previous.contains(&key);
        tallies
            .entry(key)
            .and_modify(|tally| {
                tally.count += 1;
                tally.last = request;
            })
            .or_insert(Tally {
                count: 1,
                first: request,
                last: request,
            });
        (closed, first)
    }

    /// When the host must wake to close the open window: its end, only while it holds
    /// refusals. An empty window is closed lazily, by the next [`Self::roll`].
    pub(crate) fn due(&self) -> Option<u64> {
        self.open
            .as_ref()
            .filter(|(_, tallies)| !tallies.is_empty())
            .map(|(start, _)| start.saturating_add(self.window_ms))
    }

    /// Close every window that ended by `now`, oldest first; the summaries of those that held
    /// refusals.
    pub(crate) fn roll(&mut self, now: u64) -> Vec<Summary> {
        let mut closed = Vec::new();
        while let Some((start, _)) = self.open {
            let end = start.saturating_add(self.window_ms);
            if now < end {
                break;
            }
            let (_, tallies) = self.open.take().expect("an open window");
            if tallies.is_empty() {
                // A whole window with no refusal: nothing is previous any more.
                self.previous.clear();
                break;
            }
            self.previous = tallies.keys().copied().collect();
            closed.push(self.summarise(tallies));
            self.open = Some((end, BTreeMap::new()));
        }
        closed
    }

    fn summarise(&self, tallies: BTreeMap<Key, Tally>) -> Summary {
        let keys = tallies.len();
        let refusals = tallies.values().map(|tally| tally.count).sum();
        let mut ranked: Vec<(Key, Tally)> = tallies.into_iter().collect();
        ranked.sort_by(|a, b| b.1.count.cmp(&a.1.count).then(a.0.cmp(&b.0)));
        let others: Vec<(Key, Tally)> = ranked.split_off(ranked.len().min(TOP));
        Summary {
            window_ms: self.window_ms,
            keys,
            refusals,
            others_keys: others.len(),
            others_refusals: others.iter().map(|(_, tally)| tally.count).sum(),
            top: ranked,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAUSED: ErrorKind = ErrorKind::ProtectionPaused;

    #[test]
    fn the_first_refusal_of_a_key_is_first_and_the_rest_are_counted() {
        let mut log = RefusalLog::new(100);
        assert_eq!(log.record(0, (PAUSED, 1), 10), (vec![], true));
        assert_eq!(log.record(5, (PAUSED, 1), 11), (vec![], false));
        assert_eq!(
            log.record(6, (PAUSED, 2), 12),
            (vec![], true),
            "another partition"
        );
        assert_eq!(
            log.record(7, (ErrorKind::NotPrimary, 1), 13),
            (vec![], true),
            "another reason"
        );
        assert_eq!(log.due(), Some(100));
        let closed = log.roll(100);
        assert_eq!(closed.len(), 1);
        let summary = &closed[0];
        assert_eq!((summary.keys, summary.refusals), (3, 4));
        assert_eq!(
            summary.top[0],
            (
                (PAUSED, 1),
                Tally {
                    count: 2,
                    first: 10,
                    last: 11
                }
            )
        );
        assert_eq!((summary.others_keys, summary.others_refusals), (0, 0));
    }

    #[test]
    fn a_key_refused_in_the_window_before_is_not_first_again() {
        let mut log = RefusalLog::new(100);
        log.record(0, (PAUSED, 1), 1);
        // The next window opened at 100, where the first ended.
        let (closed, first) = log.record(150, (PAUSED, 1), 2);
        assert_eq!(closed.len(), 1);
        assert!(
            !first,
            "refused in the window that ended where this one began"
        );
    }

    #[test]
    fn a_key_is_first_again_after_a_whole_window_with_no_refusal() {
        let mut log = RefusalLog::new(100);
        log.record(0, (PAUSED, 1), 1);
        log.record(150, (PAUSED, 2), 2); // window 100..200: another key only
        let (closed, first) = log.record(250, (PAUSED, 1), 3); // window 200..300
        assert_eq!(closed.len(), 1);
        assert!(first, "not refused in 100..200");
    }

    #[test]
    fn after_a_gap_with_no_refusals_at_all_the_same_key_is_first_again() {
        let mut log = RefusalLog::new(100);
        log.record(0, (PAUSED, 1), 1);
        // The window closes at 100 with its summary; 100..200 then passes with no refusal.
        assert_eq!(log.roll(100).len(), 1);
        assert_eq!(
            log.due(),
            None,
            "nothing to wake for while the window is empty"
        );
        let (closed, first) = log.record(5_000, (PAUSED, 1), 2);
        assert!(closed.is_empty(), "the quiet window has no summary");
        assert!(
            first,
            "a gap longer than a window ends the previous window's keys"
        );
    }

    #[test]
    fn a_summary_names_the_top_keys_and_folds_the_rest_into_others() {
        let mut log = RefusalLog::new(100);
        let mut request = 0;
        for partition in 1..=20_u32 {
            // Partition p is refused p times, so 20..5 are the top 16 and 4..1 fold.
            for _ in 0..partition {
                request += 1;
                log.record(1, (PAUSED, partition), request);
            }
        }
        let summary = log.roll(101).pop().expect("a summary");
        assert_eq!(summary.keys, 20);
        assert_eq!(summary.refusals, (1..=20).sum::<u64>());
        assert_eq!(summary.top.len(), TOP);
        assert_eq!(summary.top[0].0, (PAUSED, 20));
        assert_eq!(summary.top[TOP - 1].0, (PAUSED, 5));
        assert_eq!(
            (summary.others_keys, summary.others_refusals),
            (4, 1 + 2 + 3 + 4)
        );
        let json: serde_json::Value = serde_json::from_str(&summary.top_json()).expect("json");
        assert_eq!(json.as_array().map(Vec::len), Some(TOP));
        assert_eq!(json[0]["reason"], "ProtectionPaused");
        assert_eq!(json[0]["partition"], 20);
        assert_eq!(json[0]["count"], 20);
    }
}
