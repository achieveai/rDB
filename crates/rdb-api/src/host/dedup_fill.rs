//! Issue #98 (lead option B): how full T1's dedup index is against its cap, as two warnings an
//! operator can act on before `put_as` writes start answering `OVERLOADED`.
//!
//! The host logs `dedup_cap_high` once when a partition's index reaches [`high_mark`] (80% of
//! the cap) and `dedup_cap_full` once when it reaches the cap. Each is logged once per crossing:
//! it is re-armed only after the index falls [`slack`] entries below its mark. Without that
//! slack a fill sitting on a mark would log on every write, since one write adds an entry and an
//! unpublished answer's expiry (or a trim) takes one away in the next step.
//!
//! Pure: the host observes after each step and logs what [`Fill::observe`] returns.

/// A mark the index reached in one observation, lowest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Crossed {
    /// [`high_mark`] reached: `dedup_cap_high`.
    High,
    /// The cap reached: `dedup_cap_full`.
    Full,
}

/// Which marks have been logged and not yet re-armed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Fill {
    /// Below both marks, or re-armed since: the next crossing of either logs.
    #[default]
    Clear,
    /// `dedup_cap_high` logged; `dedup_cap_full` armed.
    High,
    /// Both logged.
    Full,
}

/// 80% of `cap`, rounded up: the fill at which `dedup_cap_high` is logged.
#[must_use]
pub(crate) const fn high_mark(cap: usize) -> usize {
    cap - cap / 5
}

/// How far below a mark the index must fall to re-arm it: 5% of the cap, at least one entry.
/// At the default cap (65,536) that is 3,276 entries, so only a real drain re-arms a warning.
#[must_use]
pub(crate) const fn slack(cap: usize) -> usize {
    let five_percent = cap / 20;
    if five_percent == 0 {
        1
    } else {
        five_percent
    }
}

impl Fill {
    /// Take the index's length `len` against `cap`: first re-arm what it fell far enough below,
    /// then return each mark it now reaches that was armed, lowest first. A jump from below
    /// 80% straight to the cap returns both.
    pub(crate) fn observe(&mut self, len: usize, cap: usize) -> Vec<Crossed> {
        if *self == Self::Full && len < cap.saturating_sub(slack(cap)) {
            *self = Self::High;
        }
        if *self == Self::High && len < high_mark(cap).saturating_sub(slack(cap)) {
            *self = Self::Clear;
        }
        let mut crossed = Vec::new();
        if *self == Self::Clear && len >= high_mark(cap) {
            *self = Self::High;
            crossed.push(Crossed::High);
        }
        if *self == Self::High && len >= cap {
            *self = Self::Full;
            crossed.push(Crossed::Full);
        }
        crossed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #98: with a cap of 10, `dedup_cap_high` fires once at 8 and `dedup_cap_full` once
    /// at 10; neither fires again while the fill hovers on its mark; each re-arms only once the
    /// fill falls a slack (1 entry here) below it, and then fires again on the next crossing.
    /// Unit (~1 ms).
    #[config_log::retcd_test]
    fn i98_1_each_dedup_cap_mark_fires_once_per_crossing_and_rearms_below_its_slack() {
        let cap = 10;
        assert_eq!((high_mark(cap), slack(cap)), (8, 1));
        let mut fill = Fill::default();
        let mut seen = Vec::new();
        let mut walk = |lens: &[usize]| {
            for &len in lens {
                seen.push((len, fill.observe(len, cap)));
            }
        };
        // Up to the cap, then hovering on it as entries free and refill.
        walk(&[0, 7, 8, 9, 8, 9, 10, 9, 10]);
        // Down to 8 re-arms `full` (8 < 10 - 1), not `high`; back to 10 logs `full` again.
        walk(&[8, 10]);
        // Down to 7 is not below 8 - 1; to 6 re-arms `high`; back to 8 logs `high` again.
        walk(&[7, 8, 6, 8]);
        // A drain to 0 and one jump to the cap logs both, lowest first.
        walk(&[0, 10]);
        let logged: Vec<(usize, Vec<Crossed>)> = seen
            .into_iter()
            .filter(|(_, crossed)| !crossed.is_empty())
            .collect();
        tracing::info!(?logged, "i98_fill_walk");
        assert_eq!(
            logged,
            vec![
                (8, vec![Crossed::High]),
                (10, vec![Crossed::Full]),
                (10, vec![Crossed::Full]),
                (8, vec![Crossed::High]),
                (10, vec![Crossed::High, Crossed::Full]),
            ]
        );
    }

    /// The marks at the default cap and at the smallest cap an ack-floor open accepts (3), where
    /// 80% rounds up to the cap itself. Unit (~1 ms).
    #[config_log::retcd_test]
    fn i98_2_marks_round_up_and_keep_a_slack_of_at_least_one_entry() {
        let default = rdb_core::transaction::RETENTION_CAP_ENTRIES;
        assert_eq!((high_mark(default), slack(default)), (52_429, 3_276));
        assert_eq!((high_mark(3), slack(3)), (3, 1));
        let mut fill = Fill::default();
        assert_eq!(fill.observe(2, 3), Vec::new());
        assert_eq!(fill.observe(3, 3), vec![Crossed::High, Crossed::Full]);
    }
}
