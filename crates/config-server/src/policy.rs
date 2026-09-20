//! Loading, verifying and re-loading the signed policy document (M6, ADR-0027, spec §15.3).
//!
//! The daemon's half of signed RBAC. `config-core` owns the cryptography and the converging
//! evaluator; this module owns the two things a pure crate cannot have: the files and the
//! clock. It reads the document and its detached signature, hands both to
//! [`config_core::verify_policy`], and — on a document that verifies and moves the version
//! forward — revokes the watch streams the change narrows *before* the new document starts
//! answering questions.
//!
//! # Why the revoke happens here and not inside the authorizer
//!
//! `SignedPolicyAuthorizer::adopt` is the instant the new document takes effect. Ordering the
//! watch revocation *before* that call is what makes §15.3's "watches affected by a changed
//! grant terminate before events are enqueued under the new policy version" true, and the hub
//! is an engine type the authorizer cannot see. So the sequencing lives at the seam that can
//! see both, which is this one.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use config_core::policy::{Adoption, PolicyRejected, PolicyState, SignedPolicyAuthorizer};
use config_core::Authorizer;
use config_engine::{PolicyMetrics, WatchHub};
use config_grpc::PolicyReload;

use crate::config::SignedPolicyConfig;

/// Everything the daemon needs to keep one node's policy current.
///
/// Held behind an `Arc` and shared by the poller task and the `ReloadPolicy` RPC, which is why
/// [`PolicyLoader::reload`] is `&self` and internally synchronized: two reloads must never
/// interleave their read-verify-revoke-adopt sequences.
pub struct PolicyLoader {
    cfg: SignedPolicyConfig,
    authorizer: Arc<SignedPolicyAuthorizer>,
    hub: Arc<WatchHub>,
    /// Serializes whole reload attempts. Not a lock on the authorizer, which has its own.
    reloading: Mutex<Attempts>,
    /// The active version, republished for readers that cannot hold the authorizer.
    ///
    /// The paginator is the one such reader: a page token seals the version it was minted
    /// under, and a walk that outlived a policy change must be refused rather than continued
    /// under grants that no longer exist (M6-32, M6-71; ADR-0029). It binds this cell through
    /// [`PolicyLoader::policy_version_cell`], so the loader's single adopt point is also the
    /// single point at which outstanding tokens stop being honoured. `0` is the "no signed
    /// policy" encoding the paginator already uses, because a document's version is `>= 1`.
    version_cell: Arc<AtomicU64>,
}

/// What the last attempts left behind, for health and for the no-storm rule.
struct Attempts {
    /// The most recent refusal, or `None` if the last attempt succeeded.
    ///
    /// Health reports it so an operator sees *why* a node is stuck on an old version without
    /// having to correlate log lines across a fleet.
    last_rejection: Option<PolicyRejected>,
    /// Refused reloads by reason, seeded with every token so an unhit reason exports `0`
    /// rather than vanishing from the series (ADR-0026's closed-set rule).
    failures: BTreeMap<&'static str, u64>,
    /// Rollbacks `--break-glass-policy-rollback` permitted (M6-09).
    rollbacks: u64,
}

impl Default for Attempts {
    fn default() -> Self {
        Self {
            last_rejection: None,
            failures: PolicyRejected::ALL_REASONS
                .iter()
                .map(|reason| (*reason, 0))
                .collect(),
            rollbacks: 0,
        }
    }
}

impl PolicyLoader {
    /// Build a loader over `cfg`, adopting into `authorizer` and revoking through `hub`.
    pub fn new(
        cfg: SignedPolicyConfig,
        authorizer: Arc<SignedPolicyAuthorizer>,
        hub: Arc<WatchHub>,
    ) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            authorizer,
            hub,
            reloading: Mutex::new(Attempts::default()),
            version_cell: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Read, verify and adopt the configured files.
    ///
    /// **Synchronous, and only ever called from a blocking-pool thread**: it reads two files and
    /// takes the watch hub's journal gate, which parks a whole thread if a compaction holds it.
    ///
    /// `source` is the audit line's provenance — `"startup"`, `"poll"` or `"rpc"`. A failure
    /// leaves the active policy exactly as it was (M6-13): "fails closed" means *does not
    /// adopt*, not *forgets what it had*, and the opposite behaviour turns a typo into an
    /// outage.
    pub fn reload(&self, source: &'static str) -> Result<PolicyReload, PolicyRejected> {
        let mut attempts = self.reloading.lock().unwrap_or_else(|e| e.into_inner());
        let outcome = self.attempt(source);
        match &outcome {
            Ok((_, break_glass)) => {
                attempts.last_rejection = None;
                if *break_glass {
                    attempts.rollbacks += 1;
                }
            }
            Err(rejection) => {
                attempts.last_rejection = Some(rejection.clone());
                *attempts.failures.entry(rejection.reason()).or_insert(0) += 1;
                tracing::error!(
                    reason = rejection.reason(),
                    source,
                    active_version = self.authorizer.policy_version(),
                    detail = %rejection,
                    "policy_rejected"
                );
            }
        }
        outcome.map(|(reload, _)| reload)
    }

    /// One read-verify-revoke-adopt sequence, without the bookkeeping.
    ///
    /// The second half of the pair is whether break-glass was what permitted the adoption; it
    /// is returned rather than put on [`PolicyReload`] because the RPC response has no field
    /// for it and only the rollback counter needs it.
    fn attempt(&self, source: &'static str) -> Result<(PolicyReload, bool), PolicyRejected> {
        let document = read(&self.cfg.policy_file, PolicyRejected::PolicyFileMissing)?;
        let signature = read(
            &self.cfg.signature_file,
            PolicyRejected::SignatureFileMissing,
        )?;
        let signed = config_core::verify_policy(&document, &signature, &self.cfg.trust_keys)?;

        let from = self.authorizer.policy_version();
        let hash_hex = hex(&signed.hash);
        // Before the adoption, never after: see the module header. Only when the adoption will
        // actually replace an active document, though — the revocation bumps the watch policy
        // epoch and takes the journal gate, and the poller runs this every tick. A first load
        // has no stream that could have been opened under an older document; a byte-identical
        // re-write changes nothing; and a rollback `adopt` is about to refuse must not revoke
        // anything at all, or one stale file on disk becomes a watch outage that repeats every
        // poll interval (C6R-03).
        if self.authorizer.adopt_would_replace(&signed) {
            let old = self
                .authorizer
                .active_document()
                .expect("adopt_would_replace is false without an active document");
            self.hub.on_policy_change(&old, &signed.document);
        }
        let adoption = self.authorizer.adopt(signed)?;
        // Immediately after the adoption, and only here: this is the one statement in the daemon
        // that can change the version in force, so republishing it here is what makes every
        // outstanding page token expire at the instant the grants behind it do (M6-32). A
        // refused attempt never reaches this line, which is why a rollback the authorizer turns
        // down does not invalidate a walk.
        //
        // The order matters and is not interchangeable. This store *follows* `adopt`, so for a
        // few instructions the cell reads older than `Authorizer::policy_version()` — which is
        // what `/health` reports. A token minted inside that window therefore seals the *old*
        // version and is refused on resume: one extra expiry, never a missed one. Publishing the
        // cell first would invert that into the unsafe direction, sealing the new version onto a
        // walk whose pages were authorized under the old grants, and `PolicyVersion` would then
        // accept exactly the token M6-32 exists to refuse.
        self.version_cell.store(
            self.authorizer.policy_version().unwrap_or_default(),
            Ordering::Relaxed,
        );

        Ok(match adoption {
            // Byte-identical to what is already active. Reported, not logged: a poller that
            // wrote an audit line every tick would drown the one that matters (M6-11).
            Adoption::Unchanged => (
                PolicyReload {
                    from,
                    to: from.unwrap_or_default(),
                    hash_hex,
                    outcome: "unchanged",
                    reason: "identical_hash",
                },
                false,
            ),
            Adoption::Adopted {
                from,
                to,
                break_glass,
            } => {
                tracing::info!(
                    version = to,
                    previous_version = from,
                    hash = %hash_hex,
                    source,
                    break_glass,
                    "policy_loaded"
                );
                (
                    PolicyReload {
                        from,
                        to,
                        hash_hex,
                        outcome: "reloaded",
                        reason: "",
                    },
                    break_glass,
                )
            }
        })
    }

    /// What the health payload publishes: the active version and the state behind it (M6-16).
    ///
    /// Filled in by the daemon's health handler rather than by the engine: only the loader knows
    /// *why* the last load failed, because only it holds the files. The two come from one read
    /// of the authorizer — filling them separately let a reload slip between and publish a
    /// payload this node never occupied; see `SignedPolicyAuthorizer::state_and_version` (M6-20).
    pub fn state_and_version(&self) -> (PolicyState, Option<u64>) {
        let attempts = self.reloading.lock().unwrap_or_else(|e| e.into_inner());
        self.authorizer
            .state_and_version(attempts.last_rejection.as_ref())
    }

    /// What `/metrics` publishes about this node's policy (ADR-0027, ADR-0026).
    ///
    /// Assembled here rather than in the engine because the reload counters belong to the
    /// loader that owns the files; the engine holds only the authorizer.
    pub fn metrics(&self) -> PolicyMetrics {
        let attempts = self.reloading.lock().unwrap_or_else(|e| e.into_inner());
        let (state, version) = self
            .authorizer
            .state_and_version(attempts.last_rejection.as_ref());
        // Converging means some voter is still on `from`, so `from` is the newest version the
        // whole cluster is known to hold — which is exactly what the gauge claims.
        let converged_version = match &state {
            PolicyState::Active { version } => Some(*version),
            PolicyState::Converging { from, .. } => Some(*from),
            PolicyState::NoValidPolicy { .. } => None,
        };
        PolicyMetrics {
            version,
            converged_version,
            rollbacks: attempts.rollbacks,
            reload_failures: attempts.failures.clone(),
            break_glass_active: self.authorizer.break_glass_active(),
        }
    }

    /// The shared authorizer, for the admin plane's admin set and the capability report.
    pub fn authorizer(&self) -> &Arc<SignedPolicyAuthorizer> {
        &self.authorizer
    }

    /// The cell `Paginator::bind_policy_version` binds (M6-32, ADR-0029).
    ///
    /// Handed out as the shared `Arc` rather than as a value: the paginator is built once, at
    /// startup, and a copy taken then would keep honouring tokens minted under grants a later
    /// reload has already replaced.
    pub fn policy_version_cell(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.version_cell)
    }

    /// One convergence pass: tell the authorizer what the cluster is known to hold (D6.1).
    ///
    /// Separate from [`Self::reload`] because the two answer different questions — "has the
    /// document on disk moved?" and "has the rest of the cluster caught up with the one already
    /// in force?" — and only the second needs to see other nodes. Logs `policy_converged`
    /// exactly once per version, because `note_cluster_min_version` reports the *transition*
    /// rather than the state (M6-21, M6-119).
    pub async fn observe_convergence(&self, source: &dyn ClusterPolicyVersions) {
        // Publish before reading. A node that adopted on this very tick is already answering
        // under the new document, so the cluster is entitled to know; and doing it in the other
        // order would make every node wait a whole extra interval for the last one to speak.
        if let Some(version) = self.authorizer.policy_version() {
            source.advertise(version).await;
        }
        let view = source.view().await;
        if !self
            .authorizer
            .note_cluster_min_version(view.min_reported())
        {
            return;
        }
        tracing::info!(
            version = self.authorizer.policy_version().unwrap_or_default(),
            voters_reporting = view.voters_reporting(),
            voters_total = view.voters.len(),
            "policy_converged"
        );
    }

    /// Re-read the files every `authz.poll_interval` until `shutdown` fires (D6.1).
    ///
    /// Bounded polling rather than a filesystem watch: a watch is a per-platform API with
    /// per-platform silent-failure modes, and the recovery an operator needs is "it picks the
    /// file up within a known bound", which a timer states and a watch only implies.
    ///
    /// `convergence` is the cluster's advertised versions, when this node has a source for
    /// them. It rides the same tick as the reload rather than a timer of its own: both answer
    /// "is this node's policy still current?", and one timer is one thing for an operator to
    /// reason about. `None` leaves the authorizer converging until it restarts, which is what
    /// a node with gossip disabled has always done.
    pub fn spawn_poller(
        self: &Arc<Self>,
        shutdown: Arc<tokio::sync::Notify>,
        convergence: Option<Arc<dyn ClusterPolicyVersions>>,
    ) -> tokio::task::JoinHandle<()> {
        let loader = Arc::clone(self);
        let interval = self.cfg.poll_interval;
        tokio::spawn(config_log::testing::in_current_span(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick completes immediately and startup has already loaded once.
            ticker.tick().await;
            // One convergence pass before the first interval, so the version this node booted
            // with reaches the wire at once rather than a poll interval late: `run` advertises
            // no version at gossip start, because the trailer is built before the policy is.
            if let Some(source) = &convergence {
                loader.observe_convergence(source.as_ref()).await;
            }
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.notified() => return,
                    _ = ticker.tick() => {}
                }
                let poll = Arc::clone(&loader);
                // The reload blocks on file I/O and on the journal gate; a runtime worker is
                // the wrong thread for both.
                if let Err(error) = tokio::task::spawn_blocking(move || poll.reload("poll")).await {
                    // Either the blocking pool is gone, which only happens during shutdown, or
                    // the reload itself panicked — the case that leaves a ready node silently
                    // never reloading its policy again (F-003).
                    crate::logging::poller_stopped("policy", &error);
                    return;
                }
                // After the reload, never before: a document adopted on this tick is one this
                // node now holds, and asking whether the cluster has caught up with the
                // previous one would answer a question nobody asked.
                if let Some(source) = &convergence {
                    loader.observe_convergence(source.as_ref()).await;
                }
            }
        }))
    }
}

/// The daemon's [`ClusterPolicyVersions`]: gossip for what each node says, committed
/// membership for who has to say it.
///
/// The two halves come from different places on purpose. Gossip is advisory and a node may
/// advertise anything, so it is only ever allowed to *end* a narrowing that is already
/// fail-closed; membership is the authoritative statement of who counts, and it comes from
/// Raft (ADR-0003, ADR-0027 §15.3, OQ-56).
pub struct GossipPolicyVersions {
    gossip: Arc<config_gossip::GossipNode>,
    node: config_engine::ConfigNode,
    /// The version last put on the wire, so a tick that changes nothing broadcasts nothing.
    ///
    /// Written only once a broadcast has actually returned; see [`advertise_once`].
    advertised: Mutex<Option<u64>>,
}

impl GossipPolicyVersions {
    /// Build the source. Only the daemon can: it is the one place that holds both.
    pub fn new(gossip: Arc<config_gossip::GossipNode>, node: config_engine::ConfigNode) -> Self {
        Self {
            gossip,
            node,
            advertised: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl ClusterPolicyVersions for GossipPolicyVersions {
    async fn view(&self) -> ClusterPolicyView {
        let voters = self
            .node
            .membership_report()
            .membership
            .voters
            .into_iter()
            .collect();
        let mut reported = BTreeMap::new();
        for meta in self.gossip.member_meta().await {
            // A meta that does not decode is a peer this build cannot read, which is the same
            // thing as a peer that has not reported: it is left out, and counts as lagging.
            let Ok(hint) = config_gossip::decode_hint(&meta) else {
                continue;
            };
            if let Some(version) =
                config_gossip::decode_hint_extras(&meta).and_then(|extras| extras.policy_version)
            {
                reported.insert(hint.node_id, version);
            }
        }
        ClusterPolicyView { voters, reported }
    }

    async fn advertise(&self, version: u64) {
        // Only this field: `accepted_gossip_keys` belongs to the key rotation and `schema` to
        // the mixed-version gate, and a whole-value write here would revert either of them
        // mid-flight (ruling M6-R18).
        let broadcast = || {
            self.gossip
                .update_extras(|extras| extras.policy_version = Some(version))
        };
        if let Err(error) = advertise_once(&self.advertised, version, broadcast).await {
            tracing::warn!(
                %error,
                version,
                "gossip could not advertise this node's policy version; peers will keep                  treating it as lagging until the next poll"
            );
        }
    }
}

/// Put `version` on the wire through `broadcast`, unless it is already there.
///
/// `advertised` is written **after** the broadcast returns, never before. The difference only
/// shows up when the caller is cancelled at the await — today that is the shutdown abort in
/// `run`, where the consequence is nil — but the failure it prevents is permanent: a cell left
/// holding a version that never reached the wire makes every later tick take the early return,
/// so `ClusterPolicyView::min_reported` counts this node as lagging forever and its peers'
/// clients see `policy_converging` (F-019). The poller calls this sequentially, so the widened
/// window costs at most one redundant broadcast.
async fn advertise_once<E, F>(
    advertised: &Mutex<Option<u64>>,
    version: u64,
    broadcast: impl FnOnce() -> F,
) -> Result<(), E>
where
    F: std::future::Future<Output = Result<(), E>>,
{
    if *advertised.lock().unwrap_or_else(|e| e.into_inner()) == Some(version) {
        return Ok(());
    }
    broadcast().await?;
    *advertised.lock().unwrap_or_else(|e| e.into_inner()) = Some(version);
    Ok(())
}

/// What the convergence rule sees of the cluster, in one snapshot.
///
/// Deliberately not a gossip type. The rule is "every voter holds a version at least as new as
/// the one in force", and what it needs is the voter set and the versions — not the wire format
/// they arrived in. Keeping the two apart is what lets the rule be tested without a cluster,
/// and keeps the advisory transport out of a decision it must never take on its own
/// (ADR-0027 §15.3, OQ-56).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterPolicyView {
    /// The voters this node expects to hear from, including itself.
    pub voters: Vec<config_core::NodeId>,
    /// The version each node currently advertises. A voter absent from this map has not
    /// reported one.
    pub reported: BTreeMap<config_core::NodeId, u64>,
}

impl ClusterPolicyView {
    /// The lowest version *every* voter is known to hold, or `None` when any voter is silent.
    ///
    /// Absence is lagging, never "probably fine": failing open on a voter this node has not
    /// heard from is the exact bug the converging clause exists to prevent (M6-22). An empty
    /// voter set is silence too, so a node that knows of no voters never converges.
    ///
    /// `Option`'s own ordering does the work — `None` sorts below every `Some` — so one `min`
    /// covers both "somebody is silent" and "somebody is behind".
    pub fn min_reported(&self) -> Option<u64> {
        self.voters
            .iter()
            .map(|voter| self.reported.get(voter).copied())
            .min()
            .flatten()
    }

    /// How many voters have reported a version, for the `policy_converged` line (M6-119).
    pub fn voters_reporting(&self) -> usize {
        self.voters
            .iter()
            .filter(|voter| self.reported.contains_key(voter))
            .count()
    }
}

/// One snapshot of who must report and what they advertise.
///
/// A trait because the production source joins two things this module has no business knowing
/// about — committed membership and the gossip trailer — while the rule itself must stay
/// testable without either.
#[async_trait::async_trait]
pub trait ClusterPolicyVersions: Send + Sync {
    /// The current view. Cheap; called once per poll tick.
    async fn view(&self) -> ClusterPolicyView;

    /// Say that this node now holds `version`.
    ///
    /// The other half of the same fact: a node that reads its peers' versions without
    /// publishing its own would leave every *other* node converging forever. Idempotent — the
    /// caller states the version on every tick and the implementation decides whether anything
    /// has to go on the wire.
    async fn advertise(&self, version: u64);
}

/// Read a policy file, mapping "not there" onto the caller's typed reason.
///
/// A missing file and an unreadable one are deliberately the same refusal: both mean this node
/// cannot see the document right now, both keep the active policy, and both clear the moment
/// the file comes back. Distinguishing them would offer an operator a choice they do not have.
fn read(path: &Path, missing: PolicyRejected) -> Result<Vec<u8>, PolicyRejected> {
    std::fs::read(path).map_err(|_| missing)
}

/// Lowercase hex, for the audit line and the `PolicyInfo` response.
fn hex(bytes: &[u8; 32]) -> String {
    bytes.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[cfg(test)]
mod tests {
    //! C6R-03: what a reload does to open watches when the file on disk has *not* moved on.
    //!
    //! A loader, a real [`WatchHub`] and two real files — no node and no cluster, because the
    //! question is entirely about the order of two calls the loader makes. The watch policy epoch
    //! is the observable: `on_policy_change` is the only thing that increments it, so "the epoch
    //! did not move" is exactly "no stream was revoked and the journal gate was not taken".

    use std::sync::Arc;
    use std::time::Duration;

    use config_core::policy::{document_hash, grant, signature_payload, PolicySignature};
    use config_core::{Action, Limits, PolicyDocument};
    use config_engine::{SystemClock, WatchHub};
    use ed25519_dalek::{Signer, SigningKey};

    use super::*;

    const KEY_NAME: &str = "ops";

    /// A loader over a fresh temp directory, plus the hub it revokes through.
    struct Fixture {
        _dir: tempfile::TempDir,
        key: SigningKey,
        loader: Arc<PolicyLoader>,
        hub: Arc<WatchHub>,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("a temp directory");
            let key = SigningKey::from_bytes(&[0x5C; 32]);
            let policy_file = dir.path().join("policy.json");
            let signature_file = dir.path().join("policy.json.sig");
            let hub = WatchHub::new(Limits::default().watch, Arc::new(SystemClock));
            let loader = PolicyLoader::new(
                SignedPolicyConfig {
                    policy_file,
                    signature_file,
                    trust_keys: vec![(KEY_NAME.to_string(), key.verifying_key())],
                    poll_interval: Duration::from_secs(10),
                },
                Arc::new(SignedPolicyAuthorizer::new(false)),
                Arc::clone(&hub),
            );
            Self {
                _dir: dir,
                key,
                loader,
                hub,
            }
        }

        /// Write a document at `version` granting `app` read+write on each prefix.
        fn write(&self, version: u64, prefixes: &[&str]) {
            let document = PolicyDocument {
                version,
                issued_unix_ms: 1_700_000_000_000 + version,
                grants: prefixes
                    .iter()
                    .map(|p| grant("app", p, &[Action::Read, Action::Write]))
                    .collect(),
                admins: vec!["root".to_string()],
            };
            let bytes = serde_json::to_vec(&document).expect("a document serializes");
            let hash = document_hash(&bytes);
            let envelope = PolicySignature {
                envelope_version: config_core::policy::POLICY_SIGNATURE_VERSION,
                key_name: KEY_NAME.to_string(),
                version,
                hash,
                signature: self
                    .key
                    .sign(&signature_payload(&hash, version))
                    .to_bytes()
                    .to_vec(),
            }
            .encode()
            .expect("an envelope encodes");
            std::fs::write(&self.loader.cfg.signature_file, envelope).expect("write the signature");
            std::fs::write(&self.loader.cfg.policy_file, bytes).expect("write the document");
        }

        fn epoch(&self) -> u64 {
            self.hub.testing().policy_epoch()
        }
    }

    /// C6R-03, the identical half: a file that has not changed is a no-op however often the
    /// poller re-reads it. Bumping the epoch every tick would revoke a stream whose prefix the
    /// `skipped` branch covers, and would take the journal gate once per poll interval forever.
    #[config_log::retcd_test]
    fn an_unchanged_file_leaves_the_watch_epoch_alone_across_repeated_polls() {
        let fixture = Fixture::new();
        fixture.write(1, &["/a/"]);
        fixture.loader.reload("startup").expect("v1 loads");
        let after_first = fixture.epoch();
        assert_eq!(
            after_first, 0,
            "a first adoption replaces nothing, so it revokes nothing"
        );

        for tick in 0..3 {
            let reload = fixture
                .loader
                .reload("poll")
                .expect("an unchanged file reloads");
            assert_eq!(reload.outcome, "unchanged", "tick {tick}");
            assert_eq!(
                fixture.epoch(),
                after_first,
                "tick {tick}: a byte-identical document must not revoke a single watch"
            );
        }
    }

    /// C6R-03, the refused half: a rollback `adopt` will refuse must not revoke anything on its
    /// way to being refused. Otherwise one stale file left on disk is a watch outage that repeats
    /// every poll interval for as long as nobody notices it.
    #[config_log::retcd_test]
    fn a_refused_rollback_leaves_the_watch_epoch_alone_across_repeated_polls() {
        let fixture = Fixture::new();
        fixture.write(2, &["/a/"]);
        fixture.loader.reload("startup").expect("v2 loads");
        let before = fixture.epoch();

        // A redeploy of the superseded document: verifies perfectly, and `adopt` refuses it.
        fixture.write(1, &["/a/", "/b/"]);
        for tick in 0..3 {
            let rejection = fixture
                .loader
                .reload("poll")
                .expect_err("a rollback is refused");
            assert_eq!(rejection.reason(), "rollback", "tick {tick}");
            assert_eq!(
                fixture.epoch(),
                before,
                "tick {tick}: nothing was adopted, so nothing may have been revoked"
            );
        }
        assert_eq!(
            fixture.loader.authorizer.policy_version(),
            Some(2),
            "the active document is untouched by the refusals"
        );

        // And a genuine forward change still revokes, so the pre-check narrowed nothing else.
        fixture.write(3, &["/a/", "/b/"]);
        fixture.loader.reload("poll").expect("v3 adopts");
        assert_eq!(
            fixture.epoch(),
            before + 1,
            "a real change revokes exactly once"
        );
    }

    // -------------------------------------------------------------------------------------
    // F-019: what the cell that suppresses a redundant broadcast is allowed to remember.

    /// A cancellation at the await must leave the cell exactly as it found it, so the next
    /// tick retries rather than believing a broadcast that never happened. Mutation check:
    /// moving `advertise_once`'s write back above `broadcast().await` fails the second
    /// assertion, which is the defect this row closes.
    #[tokio::test]
    async fn a_cancelled_advertisement_is_retried_and_a_settled_one_is_not() {
        let advertised = Mutex::new(None);
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let count = || {
            attempts.fetch_add(1, Ordering::Relaxed);
        };

        // Cancelled at the await: `timeout` polls the broadcast once — enough for it to be a
        // real attempt — and then drops it, which is what an abort does to this future.
        let cancelled = tokio::time::timeout(Duration::ZERO, {
            advertise_once::<(), _>(&advertised, 7, || {
                count();
                std::future::pending()
            })
        })
        .await;
        assert!(cancelled.is_err(), "the broadcast never completed");
        assert_eq!(attempts.load(Ordering::Relaxed), 1, "it was attempted once");
        assert_eq!(
            *advertised.lock().expect("the advertised cell"),
            None,
            "a broadcast that was cancelled did not reach the wire, so nothing may be remembered"
        );

        advertise_once::<(), _>(&advertised, 7, || {
            count();
            std::future::ready(Ok(()))
        })
        .await
        .expect("the retry succeeds");
        assert_eq!(
            attempts.load(Ordering::Relaxed),
            2,
            "the next tick retries the version the cancelled attempt never delivered"
        );

        // And the no-storm rule the cell exists for is untouched: a settled version is not
        // broadcast again.
        advertise_once::<(), _>(&advertised, 7, || {
            count();
            std::future::ready(Ok(()))
        })
        .await
        .expect("an already-advertised version is a no-op");
        assert_eq!(
            attempts.load(Ordering::Relaxed),
            2,
            "a version already on the wire puts nothing further on it"
        );
    }

    // -------------------------------------------------------------------------------------
    // C6R-02: the convergence rule, without a cluster.
    //
    // `ClusterPolicyView` is the whole decision, so these exercise it directly: what the
    // gossip trailer had to travel through to get here is a separate question, covered by the
    // cluster rows (M6-20, M6-21).

    use super::ClusterPolicyView;
    use config_core::NodeId;

    fn view(voters: &[u64], reported: &[(u64, u64)]) -> ClusterPolicyView {
        ClusterPolicyView {
            voters: voters.iter().map(|id| NodeId(*id)).collect(),
            reported: reported
                .iter()
                .map(|(id, version)| (NodeId(*id), *version))
                .collect(),
        }
    }

    #[test]
    fn a_silent_voter_counts_as_lagging_not_as_agreement() {
        // Node 3 has reported nothing. Reading that as "probably fine" is the failure mode
        // §15.3's converging clause exists to prevent (M6-22).
        let v = view(&[1, 2, 3], &[(1, 8), (2, 8)]);
        assert_eq!(v.min_reported(), None);
        assert_eq!(v.voters_reporting(), 2);
    }

    #[test]
    fn the_minimum_is_taken_over_voters_only() {
        // A learner or a departed node advertising an old version must not hold the cluster
        // back: only voters decide, because only voters evaluate the policy.
        let v = view(&[1, 2], &[(1, 8), (2, 9), (3, 4)]);
        assert_eq!(v.min_reported(), Some(8));
        assert_eq!(v.voters_reporting(), 2);
    }

    #[test]
    fn a_node_that_knows_of_no_voters_never_converges() {
        // Fail closed on an empty membership: "nobody disagrees" is not "everybody agrees".
        assert_eq!(view(&[], &[(1, 8)]).min_reported(), None);
    }

    #[test]
    fn every_voter_reporting_the_new_version_converges() {
        let v = view(&[1, 2, 3], &[(1, 8), (2, 8), (3, 8)]);
        assert_eq!(v.min_reported(), Some(8));
        assert_eq!(v.voters_reporting(), 3);
    }
}
