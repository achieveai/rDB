//! The campaign artifact's fixed strings and its per-run naming.
//!
//! Only the names and the gate command live here. The artifact writer itself is
//! `config-testkit`'s `write_evidence`, which stamps the shared disclaimer, and nothing in this
//! crate may write a second copy of that string: a disclaimer a row can re-type is a disclaimer
//! a row can quietly weaken.

/// The M7 release gate, verbatim from
/// `docs/ADRs/rdb/0019-validation-gates-evidence-and-release-boundary.md` §2.1.
///
/// Held row **M7V-87** owns the cross-check that this const, the ADR and the plan's VA-9 all
/// agree. The const is declared now so the row has one place to point at when it is written, and
/// so the command lives beside the artifact names rather than in three documents only.
///
/// `SPIKE_ASSERT_WALL_MS=60000` is ADR-rdb-0019's 1,000-history/60 s target, asserted here and
/// nowhere else: the PR default records wall time only (V-R11, M7V-60).
pub const RELEASE_GATE_COMMAND: &str = "SPIKE_REQUIRE_ALL=1 RETCD_EVIDENCE=1 \
     SPIKE_ASSERT_WALL_MS=60000 CARGO_TARGET_DIR=.rtargets/campaign scripts/gate.sh test \
     --release -p rdb-sim --test campaign";

/// Which campaign produced an artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// The default corpus a PR runs.
    Debug,
    /// The 1,000-history release run. Ruling V-R17: the **only** artifact that may be cited for
    /// the 1,000-history budget.
    Release,
}

/// The artifact name for one profile.
///
/// Two names, not one with a field, because the release claim is made by citing a filename and a
/// single name would let a debug run be cited for a release budget.
#[must_use]
pub const fn artifact_name(profile: Profile) -> &'static str {
    match profile {
        Profile::Debug => "rdb-m7-campaign",
        Profile::Release => "rdb-m7-campaign-release",
    }
}

/// The two duration keys, kept distinct so neither can hide inside the other (critic F11).
pub const DURATION_KEYS: [&str; 3] = ["wall_ms", "shrink_ms", "compile_ms_excluded"];

impl Profile {
    /// The profile this binary was compiled under. A pure function of the build
    /// (`cfg!(debug_assertions)`), never of an environment variable (ruling V-R17).
    #[must_use]
    pub const fn current() -> Self {
        if cfg!(debug_assertions) {
            Self::Debug
        } else {
            Self::Release
        }
    }

    /// The artifact's `profile` value.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Debug => "debug",
            Self::Release => "release",
        }
    }
}

/// The target directory VA-9 reserves for commands 2 and 3.
pub const CAMPAIGN_TARGET_DIR: &str = ".rtargets/campaign";

/// The coverage artifact's name (ADR-rdb-0019 §2). One name under both profiles.
pub const COVERAGE_ARTIFACT: &str = "rdb-m7-coverage";
