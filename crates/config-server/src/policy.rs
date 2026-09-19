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
        // Before the adoption, never after: see the module header. Skipped when there is no
        // active document, because there is then no stream that could have been opened under
        // one and nothing whose grants could have changed.
        if let Some(old) = self.authorizer.active_document() {
            let new = signed.document.clone();
            self.hub.on_policy_change(&old, &new);
        }
        let adoption = self.authorizer.adopt(signed)?;

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

    /// What the health payload publishes: the active version, or why there is none (M6-16).
    ///
    /// Not yet called: `HealthPayload` lives in `config-engine::metrics`, which is being changed
    /// by another workstream, so M6-16/M6-25 land in the metrics round together with
    /// `retcd_policy_reload_failures_total`. The accessors exist now because the state they
    /// report is produced here and nowhere else.
    pub fn state(&self) -> PolicyState {
        let attempts = self.reloading.lock().unwrap_or_else(|e| e.into_inner());
        self.authorizer.state(attempts.last_rejection.as_ref())
    }

    /// What `/metrics` publishes about this node's policy (ADR-0027, ADR-0026).
    ///
    /// Assembled here rather than in the engine because the reload counters belong to the
    /// loader that owns the files; the engine holds only the authorizer.
    pub fn metrics(&self) -> PolicyMetrics {
        let attempts = self.reloading.lock().unwrap_or_else(|e| e.into_inner());
        let state = self.authorizer.state(attempts.last_rejection.as_ref());
        // Converging means some voter is still on `from`, so `from` is the newest version the
        // whole cluster is known to hold — which is exactly what the gauge claims.
        let converged_version = match &state {
            PolicyState::Active { version } => Some(*version),
            PolicyState::Converging { from, .. } => Some(*from),
            PolicyState::NoValidPolicy { .. } => None,
        };
        PolicyMetrics {
            version: self.authorizer.policy_version(),
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

    /// Re-read the files every `authz.poll_interval` until `shutdown` fires (D6.1).
    ///
    /// Bounded polling rather than a filesystem watch: a watch is a per-platform API with
    /// per-platform silent-failure modes, and the recovery an operator needs is "it picks the
    /// file up within a known bound", which a timer states and a watch only implies.
    pub fn spawn_poller(
        self: &Arc<Self>,
        shutdown: Arc<tokio::sync::Notify>,
    ) -> tokio::task::JoinHandle<()> {
        let loader = Arc::clone(self);
        let interval = self.cfg.poll_interval;
        tokio::spawn(config_log::testing::in_current_span(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick completes immediately and startup has already loaded once.
            ticker.tick().await;
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.notified() => return,
                    _ = ticker.tick() => {}
                }
                let poll = Arc::clone(&loader);
                // The reload blocks on file I/O and on the journal gate; a runtime worker is
                // the wrong thread for both.
                if tokio::task::spawn_blocking(move || poll.reload("poll"))
                    .await
                    .is_err()
                {
                    // The blocking pool is gone, which only happens during shutdown.
                    return;
                }
            }
        }))
    }
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
