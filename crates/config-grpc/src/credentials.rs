//! Live mutual-TLS material for a listener, and the rustls profile compiled from it
//! (ADR-0028).
//!
//! # Why this exists at all
//!
//! [`MtlsConfig`] is static: it is read once at start and describes what a listener serves
//! forever. Rotating a certificate or a CA bundle without a restart needs one more thing —
//! somewhere the *currently served* material lives, that a reload can replace and an accept
//! loop re-reads per connection. That is [`CredentialSource`], and it is the only mutable
//! state on the TLS path.
//!
//! # Why the profile is compiled eagerly
//!
//! [`CredentialSource::replace`] parses the PEM and builds the rustls [`ServerConfig`] *before*
//! it swaps anything. A reload that names an unparseable chain, a key that does not match it or
//! a CA bundle rustls will not accept is therefore refused as a typed error at the moment the
//! operator asks for it, with the previous material still serving — not accepted silently and
//! then discovered by the next client to be refused at the handshake (M6-64).
//!
//! # What is *not* here
//!
//! Identity derivation. Which SAN grammar a certificate must satisfy, and whether a Common
//! Name may stand in for one, are properties of the listener rather than of the material it
//! serves, so they stay on [`MtlsConfig`] and stay off the reload path (M6-48).

use std::io::Cursor;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use config_engine::AuthnRejectReason;
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};

use crate::error::GrpcError;
use crate::tls::{cert_facts_from_der, CertFacts, MtlsConfig};

/// The ALPN token tonic's own acceptor advertises.
///
/// Mirrored rather than imported because tonic keeps it private. A listener that omits it
/// refuses every gRPC client at the handshake, so it is asserted by
/// [`tests::the_compiled_profile_advertises_h2`] rather than trusted.
const ALPN_H2: &[u8] = b"h2";

/// One generation of a listener's mutual-TLS material.
///
/// The operator's PEM and the rustls profile compiled from it are kept together so a reload
/// swaps both or neither: a listener can never serve one generation's certificate while
/// verifying against another generation's CA bundle.
#[derive(Debug)]
pub struct Credentials {
    mtls: MtlsConfig,
    server: Arc<ServerConfig>,
}

impl Credentials {
    /// Parse `mtls` and compile the rustls server profile, or say why it cannot be served.
    ///
    /// Also the workspace's one definition of "serveable material": the peer *dial* side
    /// validates through it too, because a node dials its peers with the same identity it
    /// serves them with, so material it could not serve is material it must not dial with.
    pub fn compile(mtls: MtlsConfig) -> Result<Self, GrpcError> {
        let mut roots = RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut Cursor::new(&mtls.ca_pem)) {
            let cert = cert.map_err(|e| GrpcError::Tls(format!("ca bundle is not PEM: {e}")))?;
            roots.add(cert).map_err(|e| {
                GrpcError::Tls(format!(
                    "ca bundle holds a certificate rustls will not trust: {e}"
                ))
            })?;
        }
        if roots.is_empty() {
            return Err(GrpcError::Tls(
                "ca bundle contains no certificates; a listener with no trust anchor would refuse every client".to_string(),
            ));
        }

        let chain = rustls_pemfile::certs(&mut Cursor::new(&mtls.cert_pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| GrpcError::Tls(format!("certificate chain is not PEM: {e}")))?;
        let key = rustls_pemfile::private_key(&mut Cursor::new(&mtls.key_pem))
            .map_err(|e| GrpcError::Tls(format!("private key is not PEM: {e}")))?
            .ok_or_else(|| {
                GrpcError::Tls("private key file holds no PKCS#8, PKCS#1 or SEC1 key".to_string())
            })?;

        // `client_auth_optional` is deliberately absent: this is the mutual-TLS profile, and a
        // listener that would serve an unauthenticated client is a different decision
        // (`TlsMode::Insecure`) rather than a knob on this one.
        let verifier = WebPkiClientVerifier::builder(roots.into())
            .build()
            .map_err(|e| GrpcError::Tls(format!("client verifier rejected the ca bundle: {e}")))?;
        let mut server = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain, key)
            .map_err(|e| {
                GrpcError::Tls(format!(
                    "certificate and private key do not form an identity: {e}"
                ))
            })?;
        server.alpn_protocols.push(ALPN_H2.to_vec());

        Ok(Self {
            mtls,
            server: Arc::new(server),
        })
    }

    /// The PEM material this generation serves.
    ///
    /// Read by the dial side, which needs a client profile rather than a server one, and by
    /// the certificate-expiry gauge, which must read the *served* credential.
    pub fn mtls(&self) -> &MtlsConfig {
        &self.mtls
    }

    /// The compiled rustls profile, ready to hand to an acceptor.
    /// What this node is serving, as an operator needs to see it (M6, ADR-0028).
    ///
    /// Read from the *first* certificate in the configured chain, which is the leaf: that is
    /// the one a peer validates and the one whose expiry ends the cluster's day. `None` only
    /// if the chain is empty or unparsable, neither of which a compiled `Credentials` can be —
    /// so in practice this is `Some`, and callers that must report something still do not have
    /// to panic to do it.
    pub fn cert_facts(&self) -> Option<CertFacts> {
        let der = rustls_pemfile::certs(&mut self.mtls.cert_pem.as_slice())
            .next()?
            .ok()?;
        cert_facts_from_der(&der)
    }

    pub(crate) fn server_config(&self) -> Arc<ServerConfig> {
        Arc::clone(&self.server)
    }
}

/// The mutual-TLS material a listener or a dialler is serving *right now*.
///
/// Cheap to read and rare to write, so an `RwLock` around an `Arc` rather than a lock-free
/// cell: every accepted connection takes the read lock for exactly as long as it takes to
/// clone one `Arc`, and a reload takes the write lock once. A lock-free swap was considered
/// and rejected — it would add a vendor for a contention profile this does not have.
#[derive(Debug)]
pub struct CredentialSource {
    /// `"client"` or `"peer"`, for log fields and for the metric's `plane` label.
    plane: &'static str,
    current: RwLock<Arc<Credentials>>,
    /// Bumped on every accepted replacement, starting at 0.
    ///
    /// The peer-dial cache keys its pooled channels on this: a channel opened under an older
    /// generation was authenticated with material the operator has since withdrawn, so it must
    /// not be reused after a reload (M6-49).
    generation: AtomicU64,
    /// Handshakes this listener refused, one counter per [`AuthnRejectReason`] (ADR-0028).
    ///
    /// Kept here because this is the only object the accept loop holds: a refused handshake
    /// never produces a principal, never reaches a backend, and would otherwise be logged and
    /// then lost. Indexed by `AuthnRejectReason::index`, so recording one is a relaxed add on
    /// a path an unauthenticated caller can drive at will.
    rejections: [AtomicU64; AuthnRejectReason::COUNT],
}

impl CredentialSource {
    /// Compile `mtls` and start serving it as generation 0.
    ///
    /// # Errors
    ///
    /// [`GrpcError::Tls`] if the material cannot be served; see [`Credentials::compile`].
    pub fn new(plane: &'static str, mtls: MtlsConfig) -> Result<Arc<Self>, GrpcError> {
        Ok(Arc::new(Self {
            plane,
            current: RwLock::new(Arc::new(Credentials::compile(mtls)?)),
            generation: AtomicU64::new(0),
            rejections: std::array::from_fn(|_| AtomicU64::new(0)),
        }))
    }

    /// Which plane this material serves.
    pub fn plane(&self) -> &'static str {
        self.plane
    }

    /// The material being served right now.
    pub fn current(&self) -> Arc<Credentials> {
        Arc::clone(&self.read())
    }

    /// How many times the material has been replaced since start.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Serve `mtls` from now on, returning the new generation.
    ///
    /// The new material is compiled *before* the swap, so a refusal leaves the previous
    /// generation serving untouched. Connections already established keep the material they
    /// were authenticated under, which is what makes a reload a non-event for in-flight calls
    /// (M6-42); only connections accepted after this call see the new generation.
    ///
    /// # Errors
    ///
    /// [`GrpcError::Tls`] if the material cannot be served. Nothing is swapped in that case.
    pub fn replace(&self, mtls: MtlsConfig) -> Result<u64, GrpcError> {
        let next = Arc::new(Credentials::compile(mtls)?);
        let generation = {
            let mut current = self.write();
            *current = next;
            // Inside the write lock so a reader can never observe a generation that does not
            // match the material it just read.
            self.generation.fetch_add(1, Ordering::AcqRel) + 1
        };
        tracing::info!(plane = self.plane, generation, "tls credentials reloaded");
        Ok(generation)
    }

    /// Count one refused handshake on this listener.
    pub(crate) fn record_rejection(&self, reason: AuthnRejectReason) {
        self.rejections[reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Refused handshakes as `(plane, reason, count)`, every reason included.
    ///
    /// Every reason, including the ones at zero, for the same purpose the engine's own
    /// breakdown serves: a series that appears only after its first increment makes a `rate()`
    /// over a healthy window return no data rather than zero.
    pub fn rejections(&self) -> Vec<(&'static str, AuthnRejectReason, u64)> {
        AuthnRejectReason::ALL
            .into_iter()
            .map(|reason| {
                (
                    self.plane,
                    reason,
                    self.rejections[reason.index()].load(Ordering::Relaxed),
                )
            })
            .collect()
    }

    /// The material and the generation it belongs to, read together.
    ///
    /// The accept loop needs both, and reading them in two calls could straddle a reload.
    pub(crate) fn current_with_generation(&self) -> (Arc<Credentials>, u64) {
        let guard = self.read();
        (Arc::clone(&guard), self.generation.load(Ordering::Acquire))
    }

    /// Read the current material, recovering from a poisoned lock.
    ///
    /// A panic while holding this lock cannot have left the material half-written — the only
    /// write is one `Arc` assignment — so refusing every subsequent connection because an
    /// unrelated task panicked would turn a logged incident into an outage.
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Arc<Credentials>> {
        self.current.read().unwrap_or_else(|e| e.into_inner())
    }

    /// As [`CredentialSource::read`], for the one writer.
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Arc<Credentials>> {
        self.current.write().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::TlsFixture;

    /// Without `h2` in the ALPN list every gRPC client is refused at the handshake, and the
    /// failure looks like a certificate problem rather than a protocol one. tonic keeps its
    /// own constant private, so this is asserted rather than shared.
    #[test]
    fn the_compiled_profile_advertises_h2() {
        let fixture = TlsFixture::new();
        let credentials =
            Credentials::compile(fixture.server_material()).expect("the fixture is serveable");
        assert_eq!(
            credentials.server_config().alpn_protocols,
            vec![b"h2".to_vec()]
        );
    }

    /// A reload that cannot be served must be refused with the previous generation still in
    /// place. The alternative — swapping first and discovering the problem at the next
    /// handshake — turns an operator's typo into an outage they cannot see from the reload.
    #[test]
    fn a_refused_reload_leaves_the_previous_generation_serving() {
        let fixture = TlsFixture::new();
        let source =
            CredentialSource::new("client", fixture.server_material()).expect("generation 0");
        let before = source.current();

        let mut broken = fixture.server_material();
        broken.key_pem =
            b"-----BEGIN PRIVATE KEY-----\nnot a key\n-----END PRIVATE KEY-----\n".to_vec();
        let error = source
            .replace(broken)
            .expect_err("an unparseable key is not serveable");

        assert!(
            matches!(error, GrpcError::Tls(_)),
            "expected a typed TLS error, got {error:?}"
        );
        assert_eq!(
            source.generation(),
            0,
            "a refused reload must not bump the generation"
        );
        assert!(
            Arc::ptr_eq(&before, &source.current()),
            "a refused reload must leave the previous material in place"
        );
    }

    /// The happy path, and the fact the peer-dial cache keys on: an accepted reload bumps the
    /// generation exactly once.
    #[test]
    fn an_accepted_reload_bumps_the_generation_once() {
        let fixture = TlsFixture::new();
        let source =
            CredentialSource::new("peer", fixture.server_material()).expect("generation 0");
        assert_eq!(source.generation(), 0);

        assert_eq!(
            source.replace(fixture.server_material()).expect("reload"),
            1
        );
        assert_eq!(source.generation(), 1);
        assert_eq!(
            source.replace(fixture.server_material()).expect("reload"),
            2
        );
    }

    /// An empty CA bundle compiles to a listener that refuses everyone. Refusing it at the
    /// reload names the real problem; accepting it produces a node that is up, healthy and
    /// unreachable.
    #[test]
    fn an_empty_ca_bundle_is_refused() {
        let fixture = TlsFixture::new();
        let mut material = fixture.server_material();
        material.ca_pem = Vec::new();

        let error = CredentialSource::new("client", material).expect_err("no trust anchor");
        assert!(
            matches!(error, GrpcError::Tls(ref detail) if detail.contains("no certificates")),
            "expected the empty-bundle refusal, got {error:?}"
        );
    }
}
