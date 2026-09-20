//! Replacing a running node's TLS credentials (M6, ADR-0028).
//!
//! # What rotates, and what does not
//!
//! Three things hold this node's mutual-TLS material while it runs: the client plane's
//! listener, the peer plane's listener, and the peer *dialler* that opens outbound Raft
//! connections. All three are replaced together, because they are all this one node's
//! identity — a node serving a new certificate while still dialling with the old one is a node
//! whose peers disagree about who it is.
//!
//! Listeners are not restarted and connections are not renegotiated. A replacement changes what
//! the *next* handshake presents and accepts; sessions already established keep the material
//! they completed their handshake under until they are closed. That is the whole reason
//! [`GrpcPeerTransport::reload`] drops its pooled channels: a pooled connection would otherwise
//! carry withdrawn credentials for as long as it stayed up.
//!
//! # Mid-write files
//!
//! A certificate being written when a reload fires is not loaded partially. Nothing is swapped
//! until the whole set has compiled into a serveable profile, so a truncated PEM leaves every
//! plane on the material it already had and the next attempt retries.
//!
//! # Why this lives here and the timer does not
//!
//! Everything a rotation manipulates — [`CredentialSource`], [`Credentials`],
//! [`GrpcPeerTransport`], [`MtlsConfig`] — belongs to this crate, so the rotation belongs beside
//! the acceptor it rotates. What does *not* live here is the schedule: `tls.watch_files_secs` is
//! a daemon configuration key, and a library that spawned its own timer would be a library with
//! an opinion about a file it was never given. The daemon owns the poller and calls
//! [`TlsRotator::reload`] on its own clock.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use config_engine::{AuthnRejectReason, TlsMetrics};

use crate::admin_plane::TlsPlaneReload;
use crate::credentials::{CredentialSource, Credentials};
use crate::tls::{ca_fingerprints, CertFacts, MtlsConfig};
use crate::transport::GrpcPeerTransport;

/// How long before `notAfter` a served certificate starts warning (D6.2, M6-63).
///
/// Thirty days is the interval an operator can act inside: it spans a change window, a holiday
/// and an escalation. The warning is emitted once per crossing rather than once per scrape,
/// because a line repeated every scrape is a line every log pipeline learns to drop.
const CERT_EXPIRY_WARN_DAYS: i64 = 30;

/// `CERT_EXPIRY_WARN_DAYS` in seconds, which is the unit the gauge is in.
const CERT_EXPIRY_WARN_SECONDS: i64 = CERT_EXPIRY_WARN_DAYS * 24 * 60 * 60;

/// Where a node's mutual-TLS material is read from.
///
/// Three paths and nothing else. The poll interval is deliberately not here: this type says
/// *what* to re-read, and how often is the caller's schedule (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    /// The trust anchor bundle.
    pub ca: PathBuf,
    /// This node's certificate chain.
    pub cert: PathBuf,
    /// This node's private key.
    pub key: PathBuf,
}

/// Why a rotation could not be carried out.
///
/// Both variants are raised by the pre-checks — reading the three files and compiling them once
/// — which run before any plane is touched, so in practice a refusal is a refusal to change and
/// every plane keeps serving exactly what it served before (M6-46).
///
/// That is a statement about where these are raised, not an invariant of the swap loop. Each
/// plane recompiles the same bytes as it takes them, so a plane can in principle refuse after an
/// earlier plane has already swapped, leaving the node briefly split across two generations. The
/// window is very small — the bytes have already compiled once — and it is deliberately
/// recoverable rather than prevented: the rotator does not record the new material as served
/// unless every plane took it, so the next reload, RPC or poll, retries the whole set instead of
/// computing "unchanged" and leaving the split in place.
#[derive(Debug, thiserror::Error)]
pub enum RotationError {
    /// One of the configured PEM files could not be read.
    #[error("cannot read {what} at {path}: {detail}")]
    Read {
        /// Which configuration key named the file.
        what: &'static str,
        /// The path that was read.
        path: String,
        /// The underlying error.
        detail: String,
    },
    /// The material read is not something this node could serve.
    #[error("the TLS material on disk is not serveable: {0}")]
    Unusable(String),
}

impl RotationError {
    /// The stable reason token an audit line and a refusal message both carry.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Read { .. } => "tls_file_unreadable",
            Self::Unusable(_) => "tls_material_unusable",
        }
    }

    /// Every token [`RotationError::reason`] can produce.
    ///
    /// Exported so `retcd_tls_reload_failures_total` can publish a zero for a reason that has
    /// not fired: a counter that materialises only on first failure gives `rate()` nothing to
    /// return on a healthy node, which a dashboard draws as missing data rather than as zero.
    pub const ALL_REASONS: [&'static str; 2] = ["tls_file_unreadable", "tls_material_unusable"];
}

/// The one thing that knows how to replace this node's TLS credentials.
///
/// Holds the *planes* rather than being held by them: the two listeners are created at
/// different points of a node's start sequence and neither can own the other, so the rotator is
/// built first and each plane registers with it as its handle appears.
pub struct TlsRotator {
    /// Where to re-read from.
    files: TlsFiles,
    /// The non-PEM half of the served profile — `server_domain` and
    /// `allow_common_name_principals` — captured once at construction.
    ///
    /// Carried through every reload unchanged, because those are properties of the profile this
    /// node serves rather than of the bytes on disk. A rotation that could flip
    /// `allow_common_name_principals` would make file-write access a way to widen who may
    /// authenticate (M6-48).
    template: MtlsConfig,
    /// The listeners' credentials, in registration order.
    planes: Mutex<Vec<Arc<CredentialSource>>>,
    /// The outbound half. Always present, because a node that serves mutual TLS also dials it.
    peer_dial: Arc<GrpcPeerTransport>,
    /// What is being served right now, so a reload that finds nothing new can say so.
    served: Mutex<MtlsConfig>,
    /// Reloads that replaced the served material, as `retcd_tls_reloads_total` reports them.
    reloads: AtomicU64,
    /// Refused reloads, keyed by [`RotationError::reason`].
    reload_failures: Mutex<BTreeMap<&'static str, u64>>,
    /// Which planes have already warned about the certificate they serve expiring (M6-63).
    ///
    /// Per plane and latched, so the warning fires on the crossing rather than on every
    /// scrape, and re-arms when a rotation moves `notAfter` back beyond the threshold.
    expiry_warned: Mutex<BTreeMap<String, bool>>,
}

impl TlsRotator {
    /// A rotator for the material this node started with.
    ///
    /// `started_with` is the profile already being served, so the first [`Self::reload`] can
    /// tell "the operator replaced the files" from "the files are what they always were".
    pub fn new(
        files: TlsFiles,
        started_with: MtlsConfig,
        peer_dial: Arc<GrpcPeerTransport>,
    ) -> Arc<Self> {
        Arc::new(Self {
            files,
            template: started_with.clone(),
            planes: Mutex::new(Vec::new()),
            peer_dial,
            served: Mutex::new(started_with),
            reloads: AtomicU64::new(0),
            reload_failures: Mutex::new(
                RotationError::ALL_REASONS
                    .into_iter()
                    .map(|reason| (reason, 0))
                    .collect(),
            ),
            expiry_warned: Mutex::new(BTreeMap::new()),
        })
    }

    /// Rotate this plane's credentials along with the rest, from now on.
    ///
    /// Registration rather than construction: [`crate::ServerHandle::credentials`] only exists
    /// once the plane is serving, and a node's planes start serving several steps apart.
    pub fn register(&self, source: Arc<CredentialSource>) {
        self.planes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(source);
    }

    /// Re-read the configured files and serve what they hold.
    ///
    /// `source` is `"rpc"` or `"poll"` and appears in the log line, so an operator reading a
    /// rotation back can tell the one they asked for from the one that happened on schedule.
    ///
    /// Blocking: reads three files. Callers on a runtime thread must wrap this in
    /// `spawn_blocking`.
    ///
    /// # Errors
    ///
    /// [`RotationError`]. See its type documentation for what a refusal leaves behind: a
    /// pre-check failure changes nothing, and the swap loop's much rarer failure is recovered by
    /// the next call rather than prevented.
    pub fn reload(&self, source: &'static str) -> Result<Vec<TlsPlaneReload>, RotationError> {
        self.try_reload(source).inspect_err(|error| {
            *self
                .reload_failures
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(error.reason())
                .or_default() += 1;
            // Logged here rather than by each caller so the RPC and the poller leave the same
            // trail: an operator reading back a failed rotation must not have to know which of
            // the two attempted it to find the line.
            tracing::warn!(
                source,
                // `plane` is required on this line by M6-120 and `"all"` is the value
                // docs/runbooks/credential-rotation.md tells an operator to grep for, so both
                // stay. It scopes the line to the node — the counterpart of the per-plane
                // `tls_reloaded` lines below — and it is *not* an assertion that every plane is
                // untouched: `try_reload` propagates a plane's refusal with `?`, so a failure
                // there can follow an earlier plane's swap (see [`RotationError`]).
                plane = "all",
                reason = error.reason(),
                detail = %error,
                // `recovery` rather than a count: nothing here knows how many planes had taken
                // the material when one refused, and the useful thing to tell an operator
                // reading this line is what happens next, which is the same either way.
                recovery = "the next reload retries every plane",
                "tls_reload_failed"
            );
        })
    }

    /// One reload attempt, with nothing counted and nothing logged.
    fn try_reload(&self, source: &'static str) -> Result<Vec<TlsPlaneReload>, RotationError> {
        let found = self.read_material()?;
        // Compiled before anything is decided, and compiled *once*. It is the pre-check that
        // makes a partly-written file a refusal instead of half a rotation — no plane is swapped
        // until the whole set has produced a serveable profile — and it is also where the
        // reported fingerprint and expiry come from, so what is validated and what is reported
        // can never be two different certificates.
        let compiled = Credentials::compile(found.clone())
            .map_err(|e| RotationError::Unusable(e.to_string()))?;
        let facts = compiled.cert_facts();

        // Held across every swap below, and released only once they have all taken the new
        // material. Publishing `served` first would make a mid-loop failure permanent: the
        // next reload would compute `changed == false` and never retry the planes that did not
        // take it, leaving a split credential set that no poll can heal. Holding the guard also
        // serialises a poller tick against a concurrent `ReloadTls`, so "nothing is swapped
        // until the whole set has compiled" stays true with two callers as it does with one.
        let mut served = self.served.lock().unwrap_or_else(|e| e.into_inner());
        let changed = *served != found;

        let mut planes = Vec::new();
        for plane in self.planes.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            let generation = if changed {
                plane
                    .replace(found.clone())
                    .map_err(|e| RotationError::Unusable(e.to_string()))?
            } else {
                plane.generation()
            };
            planes.push(plane_report(
                plane.plane(),
                changed,
                generation,
                facts.as_ref(),
            ));
        }
        let dial_generation = if changed {
            self.peer_dial
                .reload(found.clone())
                .map_err(|e| RotationError::Unusable(e.to_string()))?
        } else {
            self.peer_dial.generation()
        };
        planes.push(plane_report(
            "peer_dial",
            changed,
            dial_generation,
            facts.as_ref(),
        ));
        if changed {
            *served = found.clone();
        }
        drop(served);

        // Only a rotation is logged, and one line per plane (M6-120). An unchanged reload
        // writing a line would put one `tls_reloaded` every poll interval into the log of a
        // node that has never rotated, which is how the lines that matter get filtered out; and
        // one line for the node would make "which plane picked this up?" unanswerable on the
        // exact failure that question is asked about.
        if changed {
            self.reloads.fetch_add(1, Ordering::Relaxed);
            let cas = ca_fingerprints(&found.ca_pem);
            for plane in &planes {
                tracing::info!(
                    plane = plane.plane,
                    source,
                    generation = plane.generation,
                    leaf_fingerprint = plane.cert_fingerprint.as_str(),
                    not_after_unix = plane.cert_expiry_unix,
                    ca_count = cas.len(),
                    ca_fingerprints = cas.join(","),
                    "tls_reloaded"
                );
            }
        }
        Ok(planes)
    }

    /// What `retcd_tls_reloads_total` and `retcd_tls_reload_failures_total` report.
    pub fn metrics(&self) -> TlsMetrics {
        TlsMetrics {
            reloads: self.reloads.load(Ordering::Relaxed),
            reload_failures: self
                .reload_failures
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        }
    }

    /// Handshakes each registered listener refused, as `(plane, reason, count)`.
    ///
    /// Read from the listeners rather than accumulated here: a refused handshake is counted by
    /// the accept loop that refused it, and a copy kept alongside would be one more thing that
    /// can disagree with the first.
    pub fn authn_rejections(&self) -> Vec<(&'static str, AuthnRejectReason, u64)> {
        self.planes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .flat_map(|plane| plane.rejections())
            .collect()
    }

    /// Seconds until each plane's served leaf expires, as `retcd_cert_expiry_seconds` reports
    /// it (ADR-0026).
    ///
    /// `now_unix` is passed in rather than read here so the metric can be asserted against a
    /// fixed clock (TA-64) instead of against whatever the test machine's wall clock says.
    ///
    /// Emits the `cert_expiring` warning as a side effect, because this is the one place that
    /// holds both a served certificate and a clock. Once per plane per crossing — see
    /// [`Self::warn_if_expiring`].
    pub fn expiry_seconds(&self, now_unix: i64) -> BTreeMap<String, i64> {
        let remaining: BTreeMap<String, i64> = self
            .planes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter_map(|plane| {
                let facts = plane.current().cert_facts()?;
                Some((plane.plane().to_string(), facts.not_after_unix - now_unix))
            })
            .collect();
        self.warn_if_expiring(&remaining);
        remaining
    }

    /// Warn once per plane each time its certificate crosses the 30-day threshold.
    ///
    /// Latched rather than rate-limited: a rate limit would still repeat forever, just more
    /// slowly, and an operator cannot tell a repeat from a second certificate going the same
    /// way. The latch clears when a plane's remaining life goes back above the threshold, so a
    /// rotation to a longer-lived certificate re-arms the warning for the next crossing
    /// (M6-63).
    fn warn_if_expiring(&self, remaining: &BTreeMap<String, i64>) {
        let mut warned = self.expiry_warned.lock().unwrap_or_else(|e| e.into_inner());
        for (plane, seconds) in remaining {
            let expiring = *seconds <= CERT_EXPIRY_WARN_SECONDS;
            let already = warned.get(plane).copied().unwrap_or(false);
            if expiring && !already {
                tracing::warn!(
                    plane = plane.as_str(),
                    // Truncated towards zero, and negative once the certificate has expired:
                    // "-2" is a fact an operator can act on, where a clamped "0" would read
                    // like the certificate expires today for as long as the node stays up.
                    days_remaining = *seconds / (24 * 60 * 60),
                    "cert_expiring"
                );
            }
            warned.insert(plane.clone(), expiring);
        }
    }

    /// Read the three configured PEM files into one serveable profile.
    ///
    /// Built from [`Self::template`] so that everything about the profile which is *not* on
    /// disk — the dialled server domain, the Common-Name gate — survives the read unchanged.
    fn read_material(&self) -> Result<MtlsConfig, RotationError> {
        let read = |what: &'static str, path: &std::path::Path| {
            std::fs::read(path).map_err(|e| RotationError::Read {
                what,
                path: path.display().to_string(),
                detail: e.to_string(),
            })
        };
        let mut found = self.template.clone();
        found.ca_pem = read("tls.ca", &self.files.ca)?;
        found.cert_pem = read("tls.cert", &self.files.cert)?;
        found.key_pem = read("tls.key", &self.files.key)?;
        Ok(found)
    }
}

/// One plane's line of the reply.
///
/// Every plane reports the *same* certificate, because there is only one: this node's identity
/// is one leaf served by both listeners and presented by the dialler. What differs per plane is
/// the generation, which is the only thing that can disagree.
fn plane_report(
    plane: &'static str,
    changed: bool,
    generation: u64,
    facts: Option<&CertFacts>,
) -> TlsPlaneReload {
    TlsPlaneReload {
        plane,
        outcome: if changed { "reloaded" } else { "unchanged" },
        generation,
        cert_fingerprint: facts.map(|f| f.fingerprint.clone()).unwrap_or_default(),
        cert_expiry_unix: facts.map(|f| f.not_after_unix).unwrap_or_default(),
    }
}

/// `Debug` never prints key bytes; only what is already safe to log.
impl std::fmt::Debug for TlsRotator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsRotator")
            .field("files", &self.files)
            .field(
                "planes",
                &self
                    .planes
                    .lock()
                    .map(|planes| planes.len())
                    .unwrap_or_default(),
            )
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use config_core::Limits;

    use crate::testing::TlsFixture;

    /// A directory holding one node's PEM set, and a rotator reading from it.
    ///
    /// Written to real files rather than held in memory because re-reading files *is* the
    /// behaviour under test: a rotator that were handed bytes would prove nothing about the
    /// path a rotation actually takes.
    struct Fixture {
        _dir: tempfile::TempDir,
        files: TlsFiles,
        rotator: Arc<TlsRotator>,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("temp dir");
            let files = TlsFiles {
                ca: dir.path().join("ca.pem"),
                cert: dir.path().join("node.pem"),
                key: dir.path().join("node.key"),
            };
            let started_with = write_material(&files);
            let peer_dial = GrpcPeerTransport::new(
                crate::TlsMode::MutualTls(started_with.clone()),
                config_engine::NetFault::new(),
                Limits::DEFAULT,
            );
            let rotator = TlsRotator::new(files.clone(), started_with, peer_dial);
            Self {
                _dir: dir,
                files,
                rotator,
            }
        }

        /// Register one listener, so the reply has a plane in it besides the dialler.
        fn with_client_plane(self) -> Self {
            let source =
                CredentialSource::new("client", self.rotator.read_material().expect("read"))
                    .expect("the material this node started with must compile");
            self.rotator.register(source);
            self
        }
    }

    /// Counts `cert_expiring` events, so a test can assert what was *emitted*.
    ///
    /// The latch itself is not the guarantee ADR-0028 makes — "warns once per crossing" is a
    /// statement about the operator's log. Asserting the latch flag instead would pass even
    /// with the suppression deleted, because clearing and setting the flag is the half of the
    /// behaviour that survives that deletion.
    #[derive(Clone, Default)]
    struct WarnCounter(Arc<AtomicU64>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCounter {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct IsExpiry(bool);
            impl tracing::field::Visit for IsExpiry {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" && format!("{value:?}").contains("cert_expiring") {
                        self.0 = true;
                    }
                }
            }
            let mut seen = IsExpiry(false);
            event.record(&mut seen);
            if seen.0 {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Mint a fresh CA and leaf and write the three PEMs where `files` says.
    ///
    /// Fresh every call, so writing twice is a real rotation: [`TlsFixture::new`] generates new
    /// keys, which is what makes the fingerprint assertions below mean anything.
    fn write_material(files: &TlsFiles) -> MtlsConfig {
        let material = TlsFixture::new().server_material();
        std::fs::write(&files.ca, &material.ca_pem).expect("write ca");
        std::fs::write(&files.cert, &material.cert_pem).expect("write cert");
        std::fs::write(&files.key, &material.key_pem).expect("write key");
        material
    }

    /// A reload that finds the same bytes must not look like a rotation. Reporting one would
    /// make every dashboard show a rotation every poll interval forever, which is how a real
    /// rotation stops being noticeable.
    #[test]
    fn an_unchanged_file_set_is_reported_as_unchanged() {
        let fixture = Fixture::new().with_client_plane();

        let planes = fixture.rotator.reload("poll").expect("reload");

        assert_eq!(planes.len(), 2, "one listener and the dialler: {planes:#?}");
        for plane in &planes {
            assert_eq!(plane.outcome, "unchanged", "{plane:#?}");
            assert_eq!(plane.generation, 0, "{plane:#?}");
        }
        assert_eq!(
            fixture.rotator.metrics().reloads,
            0,
            "an unchanged reload is not a rotation"
        );
    }

    /// The core claim: new bytes on disk become the served credentials, on every plane at once.
    #[test]
    fn new_material_is_served_by_every_plane() {
        let fixture = Fixture::new().with_client_plane();
        let before = fixture.rotator.reload("poll").expect("baseline");
        let old_fingerprint = before[0].cert_fingerprint.clone();

        write_material(&fixture.files);
        let planes = fixture.rotator.reload("rpc").expect("reload");

        for plane in &planes {
            assert_eq!(plane.outcome, "reloaded", "{plane:#?}");
            assert_eq!(plane.generation, 1, "{plane:#?}");
            assert_ne!(
                plane.cert_fingerprint, old_fingerprint,
                "every plane must report the leaf it now serves: {plane:#?}"
            );
        }
        // Same certificate everywhere: one node, one identity.
        assert!(
            planes
                .windows(2)
                .all(|w| w[0].cert_fingerprint == w[1].cert_fingerprint),
            "{planes:#?}"
        );
        assert_eq!(fixture.rotator.metrics().reloads, 1);
    }

    /// A certificate caught mid-write must leave the node serving what it already had. This is
    /// the difference between a rotation that is safe to automate and one that is not.
    #[test]
    fn a_half_written_certificate_changes_nothing() {
        let fixture = Fixture::new().with_client_plane();
        let good = fixture.rotator.reload("poll").expect("baseline");

        let whole = std::fs::read(&fixture.files.cert).expect("read cert");
        std::fs::write(&fixture.files.cert, &whole[..whole.len() / 2]).expect("truncate");
        let error = fixture
            .rotator
            .reload("poll")
            .expect_err("half a PEM is not serveable material");

        assert_eq!(error.reason(), "tls_material_unusable", "{error}");
        // Restoring the file and reloading again must still report *unchanged*: the refusal
        // above must not have recorded the truncated bytes as what is being served.
        std::fs::write(&fixture.files.cert, &whole).expect("restore");
        let after = fixture.rotator.reload("poll").expect("reload");
        for plane in &after {
            assert_eq!(plane.outcome, "unchanged", "{plane:#?}");
        }
        assert_eq!(after[0].cert_fingerprint, good[0].cert_fingerprint);
        assert_eq!(
            fixture.rotator.metrics().reload_failures["tls_material_unusable"],
            1
        );
    }

    /// A file that has gone missing is a different problem from one that is malformed, and an
    /// operator fixes them differently, so the reason has to say which.
    #[test]
    fn a_missing_file_is_reported_as_unreadable() {
        let fixture = Fixture::new();
        std::fs::remove_file(&fixture.files.key).expect("remove key");

        let error = fixture.rotator.reload("poll").expect_err("no key file");

        assert_eq!(error.reason(), "tls_file_unreadable", "{error}");
        assert!(error.to_string().contains("tls.key"), "{error}");
        assert_eq!(
            fixture.rotator.metrics().reload_failures["tls_file_unreadable"],
            1
        );
    }

    /// The gauge must read the *served* credential, not the one read at boot (M6-64).
    #[test]
    fn expiry_follows_the_served_certificate() {
        let fixture = Fixture::new().with_client_plane();
        let planes = fixture.rotator.reload("poll").expect("reload");
        let not_after = planes[0].cert_expiry_unix;

        // Ten minutes before the certificate expires, the gauge reads ten minutes.
        let expiry = fixture.rotator.expiry_seconds(not_after - 600);

        assert_eq!(expiry.get("client"), Some(&600), "{expiry:#?}");
        assert_eq!(
            expiry.len(),
            1,
            "only registered listeners are measured; the dialler serves nothing: {expiry:#?}"
        );
    }

    /// M6-63: the warning latches on the crossing and re-arms when a rotation moves it back.
    ///
    /// Asserted by counting the emitted events through [`WarnCounter`], for the reason its own
    /// doc gives: the latch is private, and a test that read it would pass while the line an
    /// operator pages on never reached a subscriber.
    #[test]
    fn the_expiry_warning_fires_once_per_crossing() {
        let fixture = Fixture::new().with_client_plane();
        let not_after = fixture.rotator.reload("poll").expect("reload")[0].cert_expiry_unix;
        let inside = not_after - CERT_EXPIRY_WARN_SECONDS + 1;
        let outside = not_after - CERT_EXPIRY_WARN_SECONDS - 1;

        let counter = WarnCounter::default();
        let warnings = || counter.0.load(Ordering::Relaxed);
        let subscriber = {
            use tracing_subscriber::layer::SubscriberExt as _;
            tracing_subscriber::registry().with(counter.clone())
        };
        let _guard = tracing::subscriber::set_default(subscriber);

        fixture.rotator.expiry_seconds(outside);
        assert_eq!(warnings(), 0, "a fresh certificate is not near expiry");

        fixture.rotator.expiry_seconds(inside);
        assert_eq!(warnings(), 1, "the crossing warns");

        // Ten more scrapes inside the window: the latch holds, so the operator's log does not
        // grow by one line per poll interval for the rest of the certificate's life.
        for _ in 0..10 {
            fixture.rotator.expiry_seconds(inside);
        }
        assert_eq!(warnings(), 1, "staying inside the window must not re-warn");

        // Back outside — a rotation to a longer-lived certificate — clears the latch, so the
        // *next* certificate to age into the window is reported rather than swallowed.
        fixture.rotator.expiry_seconds(outside);
        assert_eq!(warnings(), 1, "crossing back out is not itself a warning");
        fixture.rotator.expiry_seconds(inside);
        assert_eq!(warnings(), 2, "and the next crossing warns again");
    }
}
