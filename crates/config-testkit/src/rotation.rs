//! Credential rotation against a running [`Cluster`] (test plan TA-57, TA-58, TA-64).
//!
//! # What a rotation row is allowed to observe
//!
//! Three things, and deliberately not a fourth:
//!
//! 1. **What is on disk** — [`Cluster::rotate_files`] rewrites a node's PEM set and nothing
//!    else. It does not tell the node, because "the operator replaced the files" and "the node
//!    picked them up" are two events, and every §4.1 row is about the gap between them.
//! 2. **What the node was asked to do** — [`Cluster::reload_tls`] goes through the `ReloadTls`
//!    admin RPC over the real client plane, allowlist and audit line included, and
//!    [`Cluster::poll_tls`] takes the other route: the one call
//!    `config-server`'s `spawn_tls_poller` makes on its timer.
//! 3. **What is actually served** — [`Cluster::served_leaf_fingerprint`] completes a *real*
//!    TLS handshake against the listener and reads the leaf the node presented. TA-57 requires
//!    this rather than asking the node what it believes it is serving: a harness that asked
//!    could not tell a reload that worked from one that reported success and changed nothing.
//!
//! # Why the handshake accepts anything
//!
//! The verifier behind [`Cluster::served_leaf_fingerprint`] records the chain and approves it.
//! That is not a weakened assertion — it is the only way to read what a node presents during
//! the half of a rotation where the *observer's* trust anchor is deliberately the wrong one
//! (M6-45, M6-53). Whether a real client is accepted or refused is asserted by real clients,
//! through [`Cluster::grpc_client_with_tls`] and the counters on `/health`.
//!
//! # Gossip keys
//!
//! [`Cluster::gossip_key_op`] and its three named forms drive `RotateGossipKey` over the same
//! admin plane. The keyring itself is `memberlist`'s, reachable for assertions through
//! [`Cluster::gossip_keyring`] in fingerprints only — a test that could read a gossip key back
//! would be a test that put one somewhere it could be read.

use std::sync::{Arc, Mutex};

use config_core::NodeId;
use config_gossip::{GossipError, GossipKeyring};
use config_grpc::pb::admin_service_client::AdminServiceClient;
use config_grpc::{pb, GossipKeyOp, GossipKeyringView, MtlsConfig, TlsPlaneReload};
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tonic::transport::{Channel, Endpoint};

use crate::cluster::Cluster;
use crate::tls::{CertPair, CertProfile};

pub use config_grpc::{RotationError, TlsFiles};

/// Which of a node's two listeners a rotation assertion is about.
///
/// The `plane` label on `retcd_cert_expiry_seconds` and `retcd_authn_rejected_total`, and the
/// `plane` field of a [`TlsPlaneReload`], are all this same vocabulary (ADR-0026).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plane {
    /// The listener applications reach, which the admin service shares (OQ-43).
    Client,
    /// The listener Raft reaches.
    Peer,
}

impl Plane {
    /// The token this plane is labelled with everywhere it is reported.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Peer => "peer",
        }
    }
}

/// Parse a 64-character hex AES-256 key, or `None`.
///
/// The same shape check `config-server`'s `parse_gossip_key` makes, restated rather than
/// shared because that crate declares only a `[[bin]]` target and nothing can depend on it
/// (ADR-0028 as-built). Restated in one place, so a row and the harness cannot disagree.
pub(crate) fn parse_gossip_key(hex: &str) -> Option<[u8; 32]> {
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16)? as u8;
        let lo = (chunk[1] as char).to_digit(16)? as u8;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

/// Lowercase hex of an AES-256 key, as the `RotateGossipKey` request carries it.
pub fn gossip_key_hex(key: &[u8; 32]) -> String {
    key.iter().map(|b| format!("{b:02x}")).collect()
}

/// Map a keyring refusal onto the admin plane's error vocabulary.
///
/// Mirrors `config-server`'s `gossip_rotation_error` exactly, including the two greppable
/// prefixes a caller branches on (`gossip_key_still_needed:`, `gossip_keyring_refused:`), for
/// the same reason [`parse_gossip_key`] is restated: the daemon's copy is unreachable, and a
/// harness whose refusal read differently would make M6-59 assert a string production never
/// produces.
pub(crate) fn gossip_rotation_error(e: GossipError) -> config_engine::AdminError {
    match e {
        still_needed @ GossipError::GossipKeyStillNeeded { .. } => {
            config_engine::AdminError::InvalidArgument {
                detail: format!("gossip_key_still_needed: {still_needed}"),
            }
        }
        refused @ GossipError::Keyring(_) => config_engine::AdminError::InvalidArgument {
            detail: format!("gossip_keyring_refused: {refused}"),
        },
        other => config_engine::AdminError::Unavailable {
            reason: format!("gossip_advertise_failed: {other}"),
        },
    }
}

impl Cluster {
    // ------------------------------- files on disk -------------------------------

    /// Where node `id`'s PEM set lives.
    ///
    /// Panics on a cluster that was not built with [`crate::ClusterBuilder::rotatable_tls`]:
    /// a row asking for a path has already assumed the material is on disk.
    pub fn tls_files(&self, id: NodeId) -> TlsFiles {
        self.tls_files_of(id).unwrap_or_else(|| {
            panic!(
                "node {id} serves no TLS material from files; build the cluster with \
                 ClusterBuilder::rotatable_tls(seed)"
            )
        })
    }

    /// Replace node `id`'s PEM files with `next`, and tell nobody (TA-57).
    ///
    /// The CA bundle written is `next`'s own issuer. Use [`Cluster::rotate_files_with`] for a
    /// rotation whose trust anchors differ from the leaf's issuer — the overlap window of
    /// M6-44 and M6-51, where a node must serve one CA's leaf while still trusting two.
    pub fn rotate_files(&self, id: NodeId, next: &CertPair) {
        self.rotate_files_with(id, &next.mtls());
    }

    /// [`Cluster::rotate_files`] with the whole served profile spelled out.
    pub fn rotate_files_with(&self, id: NodeId, next: &MtlsConfig) {
        crate::cluster::write_tls_files(&self.tls_files(id), next);
    }

    /// Rewrite only node `id`'s trust anchors, leaving its leaf and key as they are.
    ///
    /// The second half of an overlapping rotation: adding a CA is how new clients are let in
    /// before anyone rotates, and removing one is how old clients are shut out afterwards
    /// (M6-44, M6-45, M6-53).
    pub fn rotate_ca_bundle(&self, id: NodeId, cas: &[&str]) {
        let files = self.tls_files(id);
        let bundle = cas.join("");
        std::fs::write(&files.ca, bundle.as_bytes())
            .unwrap_or_else(|e| panic!("write {}: {e}", files.ca.display()));
    }

    /// Overwrite one of node `id`'s three PEM files with arbitrary bytes (M6-46).
    ///
    /// The corruption writers a row needs: a truncated chain, a key that belongs to another
    /// identity, a chain that reaches no configured anchor. Each is a *file*, not a profile,
    /// because what the rotator must survive is the state of the filesystem.
    pub fn corrupt_tls_file(&self, id: NodeId, which: TlsFile, bytes: &[u8]) {
        let files = self.tls_files(id);
        let path = match which {
            TlsFile::Ca => &files.ca,
            TlsFile::Cert => &files.cert,
            TlsFile::Key => &files.key,
        };
        std::fs::write(path, bytes).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    }

    // ------------------------------- telling the node -------------------------------

    /// Call `ReloadTls` on node `id` as `principal` (M6-41).
    ///
    /// The real admin RPC over the real client plane: the allowlist is consulted first, so a
    /// principal outside [`crate::ClusterConfig::admins`] is refused here exactly as it would
    /// be against a daemon.
    pub async fn reload_tls(
        &self,
        id: NodeId,
        principal: &str,
    ) -> Result<Vec<TlsPlaneReload>, tonic::Status> {
        let response = self
            .admin_rpc(id, principal)
            .await
            .reload_tls(pb::ReloadTlsRequest {})
            .await?
            .into_inner();
        Ok(response
            .planes
            .into_iter()
            .map(|plane| TlsPlaneReload {
                // `TlsPlaneReload::plane` is `&'static str` on the server side; over the wire
                // it is a `String`, so the three known names are mapped back and anything else
                // is a harness bug rather than a silently different plane.
                plane: match plane.plane.as_str() {
                    "client" => "client",
                    "peer" => "peer",
                    "peer_dial" => "peer_dial",
                    other => panic!("unknown plane {other:?} in a ReloadTls reply"),
                },
                outcome: match plane.outcome.as_str() {
                    "reloaded" => "reloaded",
                    "unchanged" => "unchanged",
                    other => panic!("unknown outcome {other:?} in a ReloadTls reply"),
                },
                generation: plane.generation,
                cert_fingerprint: plane.cert_fingerprint,
                cert_expiry_unix: plane.cert_expiry_unix,
            })
            .collect())
    }

    /// Take one tick of the file poller against node `id` (M6-42, TA-65).
    ///
    /// This is the call `config-server::rotation::spawn_tls_poller` makes on its timer, with
    /// the timer left out: the daemon owns the schedule (ADR-0028, ruling M6-R19) and an
    /// in-process cluster has no `tls.watch_files_secs` to advance. What a row asserts through
    /// this is everything the poll route does *differently* from the RPC route — `source =
    /// "poll"` in the log line, no admin principal, no audit entry — and E2E-41 covers the
    /// timer itself at the process level.
    pub fn poll_tls(&self, id: NodeId) -> Result<Vec<TlsPlaneReload>, RotationError> {
        self.rotator(id).reload("poll")
    }

    /// What `retcd_tls_reloads_total` and `retcd_tls_reload_failures_total` report for node
    /// `id`.
    pub fn tls_metrics(&self, id: NodeId) -> config_engine::TlsMetrics {
        self.rotator(id).metrics()
    }

    /// Handshakes node `id`'s listeners refused, as `retcd_authn_rejected_total{plane, reason}`
    /// reports them (ADR-0026, M6-45).
    ///
    /// Read off the listeners, which is where a refused handshake is counted: it never produces
    /// a principal and never reaches a backend, so `NodeMetrics::authn_rejected_by_reason` —
    /// which counts what the *application* layer turned away — cannot see it. Every reason is
    /// present, including at zero.
    pub fn tls_authn_rejections(
        &self,
        id: NodeId,
    ) -> Vec<(&'static str, config_engine::AuthnRejectReason, u64)> {
        self.rotator(id).authn_rejections()
    }

    /// [`Cluster::tls_authn_rejections`] where a cluster without file-backed TLS material is a
    /// fact to report rather than a panic — the shape a row that enumerates *clusters* needs.
    pub fn try_tls_authn_rejections(
        &self,
        id: NodeId,
    ) -> Option<Vec<(&'static str, config_engine::AuthnRejectReason, u64)>> {
        Some(self.tls_rotator(id)?.authn_rejections())
    }

    /// How many handshakes node `id` refused on `plane` for `reason`.
    pub fn tls_authn_rejected(
        &self,
        id: NodeId,
        plane: Plane,
        reason: config_engine::AuthnRejectReason,
    ) -> u64 {
        self.tls_authn_rejections(id)
            .into_iter()
            .find(|(p, r, _)| *p == plane.as_str() && *r == reason)
            .map(|(_, _, count)| count)
            .unwrap_or(0)
    }

    /// Seconds until node `id`'s served leaf on `plane` expires, measured against `now_unix`
    /// (TA-64).
    ///
    /// The clock is a parameter because the metric's is: `TlsRotator::expiry_seconds` takes
    /// the instant rather than reading one, which is what lets M6-63 stand 29 days from
    /// expiry without waiting. `None` when that plane is not registered.
    pub fn cert_expiry_at(&self, id: NodeId, plane: Plane, now_unix: i64) -> Option<i64> {
        self.rotator(id)
            .expiry_seconds(now_unix)
            .get(plane.as_str())
            .copied()
    }

    /// [`Cluster::cert_expiry_at`] against the wall clock, which is what a scrape sees
    /// (M6-62).
    pub fn cert_expiry(&self, id: NodeId, plane: Plane) -> Option<i64> {
        self.cert_expiry_at(id, plane, now_unix())
    }

    /// The `notAfter` of the leaf node `id` currently serves on `plane`, in Unix seconds.
    ///
    /// Derived from the gauge rather than read separately, so a row that then asserts the
    /// gauge cannot be comparing two different certificates.
    pub fn served_not_after(&self, id: NodeId, plane: Plane) -> Option<i64> {
        let now = now_unix();
        self.cert_expiry_at(id, plane, now).map(|left| now + left)
    }

    // ------------------------------- what is served -------------------------------

    /// The leaf node `id` presents on `plane`, observed from a **new** TLS handshake (TA-57).
    ///
    /// The fingerprint is spelled exactly as `config_grpc::tls::cert_facts_from_der` spells
    /// it — the first eight bytes of the leaf DER's SHA-256, in hex — so it compares directly
    /// against the `cert_fingerprint` a [`TlsPlaneReload`] reports and against the
    /// `leaf_fingerprint` field of a `tls_reloaded` log line.
    ///
    /// A handshake this node *refuses* still yields the fingerprint: the server sends its
    /// certificate before it judges the client's, so M6-45 and M6-53 can assert what a node
    /// serves and that it refuses them in the same breath.
    pub async fn served_leaf_fingerprint(&self, id: NodeId, plane: Plane) -> String {
        self.try_served_leaf_fingerprint(id, plane)
            .await
            .unwrap_or_else(|e| panic!("no leaf observed from node {id}'s {plane:?} plane: {e}"))
    }

    /// [`Cluster::served_leaf_fingerprint`], reporting why no certificate was seen.
    pub async fn try_served_leaf_fingerprint(
        &self,
        id: NodeId,
        plane: Plane,
    ) -> Result<String, String> {
        let addr = match plane {
            Plane::Client => self.client_endpoint(id),
            Plane::Peer => self.peer_endpoint(id),
        };
        // A client certificate is presented because both listeners require one. It is the
        // development principal's, which every harness cluster's fixture mints; whether it is
        // *accepted* is beside the point here, and is asserted by the rows that care.
        let client = self.fixture().issue(CertProfile::client("dev"));
        observe_leaf(&addr, &client, &self.fixture().peer_domain(id)).await
    }

    /// Offer `pair` to node `id`'s `plane` in one TLS handshake and report the leaf it served.
    ///
    /// The point is the *server's* judgement of `pair`, which is recorded on the listener
    /// whether or not this observer likes what it got back — so a row can present a deliberately
    /// wrong identity and then read the `(plane, reason)` counter it moved
    /// ([`Cluster::tls_authn_rejected`]). Exactly one TCP connection and one handshake, which is
    /// what lets a row assert "exactly one" rather than "at least one": a gRPC client would
    /// re-dial.
    pub async fn probe_handshake(
        &self,
        id: NodeId,
        plane: Plane,
        pair: &CertPair,
    ) -> Result<String, String> {
        let addr = match plane {
            Plane::Client => self.client_endpoint(id),
            Plane::Peer => self.peer_endpoint(id),
        };
        observe_leaf(&addr, pair, &self.fixture().peer_domain(id)).await
    }

    // ------------------------------- gossip keyring -------------------------------

    /// What node `id`'s gossip keyring holds, in fingerprints (TA-58).
    ///
    /// `None` when the node runs no gossip, or runs it unencrypted — build the cluster with
    /// [`crate::ClusterBuilder::gossip_key`] and [`crate::GossipKind::Real`].
    pub fn gossip_keyring(&self, id: NodeId) -> Option<GossipKeyring> {
        self.gossip_node(id).and_then(|node| node.keyring())
    }

    /// Take one step of a gossip key rotation on node `id`, as `principal` (M6-57..M6-61).
    pub async fn gossip_key_op(
        &self,
        id: NodeId,
        op: GossipKeyOp,
        key: &[u8; 32],
        force: bool,
        principal: &str,
    ) -> Result<GossipKeyringView, tonic::Status> {
        let request = pb::RotateGossipKeyRequest {
            op: match op {
                GossipKeyOp::Add => pb::GossipKeyOp::Add as i32,
                GossipKeyOp::Use => pb::GossipKeyOp::Use as i32,
                GossipKeyOp::Remove => pb::GossipKeyOp::Remove as i32,
            },
            key_hex: gossip_key_hex(key),
            force,
        };
        let info = self
            .admin_rpc(id, principal)
            .await
            .rotate_gossip_key(request)
            .await?
            .into_inner();
        Ok(GossipKeyringView {
            primary_fingerprint: info.primary_fingerprint,
            accepted_fingerprints: info.accepted_fingerprints,
        })
    }

    /// Accept `key` on node `id` from now on — stage one of the rotation.
    pub async fn gossip_add_key(
        &self,
        id: NodeId,
        key: &[u8; 32],
        principal: &str,
    ) -> Result<GossipKeyringView, tonic::Status> {
        self.gossip_key_op(id, GossipKeyOp::Add, key, false, principal)
            .await
    }

    /// Sign node `id`'s outgoing gossip with `key` — stage two.
    pub async fn gossip_use_key(
        &self,
        id: NodeId,
        key: &[u8; 32],
        principal: &str,
    ) -> Result<GossipKeyringView, tonic::Status> {
        self.gossip_key_op(id, GossipKeyOp::Use, key, false, principal)
            .await
    }

    /// Stop node `id` accepting `key` — stage three, the destructive one.
    pub async fn gossip_remove_key(
        &self,
        id: NodeId,
        key: &[u8; 32],
        force: bool,
        principal: &str,
    ) -> Result<GossipKeyringView, tonic::Status> {
        self.gossip_key_op(id, GossipKeyOp::Remove, key, force, principal)
            .await
    }

    // ------------------------------- plumbing -------------------------------

    /// Node `id`'s rotator, or the explanation for why the row cannot proceed.
    fn rotator(&self, id: NodeId) -> Arc<config_grpc::TlsRotator> {
        self.tls_rotator(id).unwrap_or_else(|| {
            panic!(
                "node {id} has no TLS rotator: it is either stopped or was built without \
                 ClusterBuilder::rotatable_tls(seed)"
            )
        })
    }

    /// An admin stub over node `id`'s client plane, presenting `principal`'s certificate.
    ///
    /// The generated stub rather than [`config_client::AdminClient`], which carries no
    /// `ReloadTls` or `RotateGossipKey` method. Same listener, same certificate profile and
    /// same allowlist as [`Cluster::admin`] — only the two RPCs differ.
    async fn admin_rpc(&self, id: NodeId, principal: &str) -> AdminServiceClient<Channel> {
        let pair = self.fixture().issue(CertProfile::client(principal));
        let endpoint = Endpoint::from_shared(format!("https://{}", self.client_endpoint(id)))
            .expect("the harness's own client endpoint is a valid URI")
            .tls_config(pair.mtls().client_tls_config())
            .expect("a fixture-issued client profile is a valid tonic TLS configuration");
        let channel = endpoint
            .connect()
            .await
            .unwrap_or_else(|e| panic!("admin channel to node {id}: {e}"));
        AdminServiceClient::new(channel)
    }
}

/// Which of the three PEM files a corruption row writes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsFile {
    /// The trust anchor bundle.
    Ca,
    /// The certificate chain.
    Cert,
    /// The private key.
    Key,
}

/// Seconds since the Unix epoch, as the metric reports them.
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the test machine's clock is after 1970")
        .as_secs() as i64
}

/// Complete one TLS handshake against `addr` and report the leaf the server presented.
///
/// The `Err` case is a handshake that produced no certificate at all — a listener that is not
/// serving TLS, or one that closed before its `Certificate` message. That is a distinct fact
/// from "served the wrong leaf", and a row that cannot tell them apart cannot diagnose a
/// failed rotation.
async fn observe_leaf(addr: &str, client: &CertPair, domain: &str) -> Result<String, String> {
    let seen: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let mtls = client.mtls();
    let chain = rustls_chain(&mtls.cert_pem)?;
    let key = rustls_key(&mtls.key_pem)?;
    let config = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(CapturingVerifier {
            seen: Arc::clone(&seen),
        }))
        .with_client_auth_cert(chain, key)
        .map_err(|e| format!("the observer's own client certificate is unusable: {e}"))?;

    let server_name = ServerName::try_from(domain.to_string())
        .map_err(|e| format!("{domain} is not a TLS server name: {e}"))?;
    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| format!("connect {addr}: {e}"))?;
    // The handshake's own result is deliberately discarded: a listener that refuses *this*
    // observer still sent its certificate first, and that certificate is the observation.
    let _ = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(server_name, stream)
        .await;

    let der = seen
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
        .ok_or_else(|| format!("{addr} completed no handshake and presented no certificate"))?;
    config_grpc::tls::cert_facts_from_der(&der)
        .map(|facts| facts.fingerprint)
        .ok_or_else(|| format!("{addr} presented a leaf that does not parse as a certificate"))
}

/// Parse a PEM chain into the DER rustls wants.
fn rustls_chain(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, String> {
    rustls_pemfile_certs(pem).map_err(|e| format!("the observer's certificate chain: {e}"))
}

fn rustls_pemfile_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, std::io::Error> {
    let mut cursor = std::io::Cursor::new(pem);
    rustls_pemfile::certs(&mut cursor).collect()
}

/// Parse a PEM private key into the DER rustls wants.
fn rustls_key(
    pem: &[u8],
) -> Result<tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>, String> {
    let mut cursor = std::io::Cursor::new(pem);
    rustls_pemfile::private_key(&mut cursor)
        .map_err(|e| format!("the observer's private key: {e}"))?
        .ok_or_else(|| "the observer's private key file holds no key".to_string())
}

/// Records the leaf a server presented, and approves every chain.
///
/// Approval is what makes the observation possible at all: during a rotation the observer's
/// own trust anchor is routinely the wrong one — that is the state M6-45 and M6-53 are about —
/// and a verifying observer would report "refused" for both "the node serves the old leaf" and
/// "the node serves the new one". Trust decisions are asserted by real clients elsewhere.
#[derive(Debug)]
struct CapturingVerifier {
    seen: Arc<Mutex<Option<Vec<u8>>>>,
}

impl ServerCertVerifier for CapturingVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, tokio_rustls::rustls::Error> {
        *self.seen.lock().unwrap_or_else(|e| e.into_inner()) = Some(end_entity.to_vec());
        Ok(ServerCertVerified::assertion())
    }

    /// Asserted rather than checked, like the chain above and for the same reason.
    ///
    /// Verifying here would mean naming a crypto provider, and naming one would mean this
    /// observer could be using a different provider from the listener it is observing. Nothing
    /// in a rotation row turns on this signature: what the row reads is the certificate the
    /// server chose to send, which arrives before any of it is verified.
    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    /// As [`CapturingVerifier::verify_tls12_signature`].
    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    /// Every scheme rustls can offer, so the listener's own choice is never narrowed by the
    /// observer.
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}
