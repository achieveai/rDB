//! Transport security profiles and the identities derived from them (ADR-0010, ADR-0012).
//!
//! Both planes take a [`TlsMode`]. [`TlsMode::Insecure`] exists for the in-process M1 harness
//! and development only; it yields [`Principal::development`] so the weakness is visible in a
//! node's capability report instead of implied. [`TlsMode::MutualTls`] is the production
//! profile: the client principal and the peer node identity come from the certificate SAN and
//! from nothing else — never from a request field (spec §6.2).
//!
//! # SAN grammar
//!
//! * client plane: `retcd://<cluster_id>/client/<name>` → `Principal { name, Certificate }`
//! * peer plane: `retcd://<cluster_id>/node/<node_id>` → the sender's claimed identity, which
//!   must equal the envelope's `cluster_id` and `from_node_id`.
//!
//! The `<cluster_id>` is checked on both planes. A CA is frequently shared across a company,
//! so "signed by our CA" is not "minted for our cluster"; without the check, a certificate
//! issued for a neighbouring cluster would be served here.
//!
//! # The Common Name fallback, and its exact limit
//!
//! A certificate that asserts **no** `retcd://` URI SAN at all *may* fall back to its Common
//! Name, which covers CAs that cannot mint URI SANs — but only on a listener that sets
//! [`MtlsConfig::allow_common_name_principals`]. A Common Name carries no cluster id, so the
//! check above cannot run on it: under a shared CA, a CN-only certificate minted for a
//! neighbouring cluster is indistinguishable from one minted for this one and would
//! authenticate here as that CN, bounded only by the allowlist. The fallback is therefore off
//! by default, and with it off a CN-only certificate is refused exactly like a certificate
//! carrying no identity at all.
//!
//! A certificate that asserts a `retcd://` SAN and means something else by it — a node
//! identity, another cluster, a URI our grammar rejects — is refused outright whatever the
//! flag says. Falling back there would let a peer node's certificate log in as a client under
//! its CN, which is the separation of the two planes undone.

use std::fmt::Write as _;
use std::str::FromStr;

use config_core::{ClusterId, NodeId, Principal, PrincipalKind};
use sha2::{Digest, Sha256};
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tonic::Status;
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

/// PEM material for one mutual-TLS endpoint.
///
/// `Debug` deliberately prints sizes, not bytes: a private key must not reach a log line.
#[derive(Clone, PartialEq, Eq)]
pub struct MtlsConfig {
    /// Trust anchor used to verify the other side.
    pub ca_pem: Vec<u8>,
    /// This endpoint's certificate chain.
    pub cert_pem: Vec<u8>,
    /// This endpoint's private key.
    pub key_pem: Vec<u8>,
    /// Name to verify the server certificate against when dialing.
    ///
    /// Endpoints are `host:port`, frequently a literal address, while a certificate names a
    /// DNS identity. `None` verifies against the endpoint's own host.
    ///
    /// **Not honoured on the peer plane.** [`crate::GrpcPeerTransport`] derives the name it
    /// verifies from the envelope it is about to send — `peer_server_domain(cluster_id, to)`
    /// — because the name that matters there is the *dialled node's*, and one `MtlsConfig`
    /// describes one dialler talking to every peer. A single configured name would either be
    /// wrong for all but one peer or, worse, let any member's certificate satisfy a dial
    /// addressed to a different member. This field therefore configures the client plane
    /// only.
    pub server_domain: Option<String>,
    /// Whether a client certificate with no `retcd://` SAN may authenticate under its Common
    /// Name (client plane only; see the module docs).
    ///
    /// `false` by default, and that default is the security property: a Common Name carries
    /// no cluster id, so this listener cannot tell a CN-only certificate minted for its own
    /// cluster from one the same CA minted for a neighbouring cluster. Turning it on is a
    /// deliberate trade for CAs that cannot mint URI SANs, and narrows the cluster binding
    /// this module otherwise guarantees on both planes.
    pub allow_common_name_principals: bool,
    /// How long an accepted connection may take to complete the TLS handshake before the
    /// listener drops it and counts it (listener only; a dialler's bound is its own connect
    /// timeout).
    ///
    /// The unauthenticated path's bound: until the handshake completes, the peer has proved
    /// nothing, so the task and the descriptor it holds are owed to a client that may never
    /// speak. [`DEFAULT_HANDSHAKE_TIMEOUT`] is generous enough that no real client on a slow
    /// link reaches it, which is what makes an expiry safe to count as a refusal.
    pub handshake_timeout: std::time::Duration,
}

/// How long a listener waits for a TLS handshake it has accepted (ADR-0028, M6-45).
///
/// Ten seconds: two orders of magnitude above a loopback or same-datacentre handshake and well
/// above a slow WAN one, so the only connections it ends are the ones that were never going to
/// finish. A shorter bound would start refusing real clients on a congested link; a longer one
/// leaves an unauthenticated socket parked for no benefit.
pub const DEFAULT_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

impl MtlsConfig {
    /// Build from PEM material, verifying the dialed server against its own host name.
    pub fn new(ca_pem: Vec<u8>, cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        Self {
            ca_pem,
            cert_pem,
            key_pem,
            server_domain: None,
            allow_common_name_principals: false,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
        }
    }

    /// Bound accepted handshakes at `timeout` instead of [`DEFAULT_HANDSHAKE_TIMEOUT`].
    ///
    /// Read once, when the listener starts: a reload replaces the material a handshake is
    /// performed against, never the bound on how long one may take.
    #[must_use]
    pub fn with_handshake_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Verify dialed servers against `domain` instead of the endpoint host.
    pub fn with_server_domain(mut self, domain: impl Into<String>) -> Self {
        self.server_domain = Some(domain.into());
        self
    }

    /// Accept a Common Name as the client principal when the certificate asserts no
    /// `retcd://` SAN; see [`MtlsConfig::allow_common_name_principals`] for what that costs.
    pub fn with_common_name_principals(mut self, allow: bool) -> Self {
        self.allow_common_name_principals = allow;
        self
    }

    fn identity(&self) -> Identity {
        Identity::from_pem(&self.cert_pem, &self.key_pem)
    }

    fn ca(&self) -> Certificate {
        Certificate::from_pem(&self.ca_pem)
    }

    /// tonic server profile: present our identity, require a client certificate signed by our CA.
    pub fn server_tls_config(&self) -> ServerTlsConfig {
        ServerTlsConfig::new()
            .identity(self.identity())
            .client_ca_root(self.ca())
    }

    /// tonic client profile: present our identity, verify the server against our CA.
    ///
    /// Honours [`MtlsConfig::server_domain`]; see the field docs for where that does *not*
    /// apply.
    pub fn client_tls_config(&self) -> ClientTlsConfig {
        match &self.server_domain {
            Some(d) => self.client_tls_config_for(d),
            None => self.base_client_tls_config(),
        }
    }

    /// tonic client profile pinned to `domain`, whatever [`MtlsConfig::server_domain`] says.
    ///
    /// The peer plane and an authenticated leader-hint dial both know the identity of the
    /// node they are about to reach and must verify *that* name, not a name the profile was
    /// built with. Passing the name per dial is what makes one `MtlsConfig` usable against
    /// every member without weakening any of them.
    pub fn client_tls_config_for(&self, domain: &str) -> ClientTlsConfig {
        self.base_client_tls_config()
            .domain_name(domain.to_string())
    }

    fn base_client_tls_config(&self) -> ClientTlsConfig {
        ClientTlsConfig::new()
            .identity(self.identity())
            .ca_certificate(self.ca())
    }
}

/// The DNS name node `node_id` of `cluster_id` presents, and the only name a dialler may
/// accept from it (ADR-0010, ADR-0011, m3-architecture §3).
///
/// tonic can express a per-connection DNS name and cannot express a per-call URI-SAN check, so
/// the node identity that the peer plane checks *inside* the envelope is mirrored into a DNS
/// SAN that TLS itself can check *before* a byte of payload moves. This function is the one
/// definition of that string: the certificate fixture issues it, the peer transport pins it,
/// and [`config_client`](https://docs.rs/config-client) pins it when following a leader hint.
/// Two spellings of it would be a hole that never shows up as a test failure, only as an
/// accepted impostor.
///
/// ```
/// use config_core::{ClusterId, NodeId};
/// use config_grpc::peer_server_domain;
///
/// let cluster = ClusterId::from_bytes([0x11; 16]);
/// assert_eq!(
///     peer_server_domain(&cluster, NodeId(3)),
///     "node-3.11111111111111111111111111111111.retcd"
/// );
/// ```
pub fn peer_server_domain(cluster_id: &ClusterId, node_id: NodeId) -> String {
    format!("node-{node_id}.{cluster_id}.retcd")
}

impl std::fmt::Debug for MtlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MtlsConfig")
            .field("ca_pem_bytes", &self.ca_pem.len())
            .field("cert_pem_bytes", &self.cert_pem.len())
            .field("key_pem_bytes", &self.key_pem.len())
            .field("server_domain", &self.server_domain)
            .field(
                "allow_common_name_principals",
                &self.allow_common_name_principals,
            )
            .field("handshake_timeout", &self.handshake_timeout)
            .finish()
    }
}

/// How a plane protects its connections.
///
/// Deliberately not [`Default`]: the safe-looking default would be the insecure one, and a
/// caller that forgets to choose should not get an unauthenticated listener by omission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TlsMode {
    /// Plain TCP. Development and the in-process harness only (ADR-0010); a node serving this
    /// mode reports [`config_core::TransportSecurity::Insecure`].
    Insecure,
    /// Mutual TLS. The only mode from which an authenticated identity can be derived.
    MutualTls(MtlsConfig),
}

impl TlsMode {
    /// The capability a node serving this mode must report (ADR-0016).
    pub fn transport_security(&self) -> config_core::TransportSecurity {
        match self {
            Self::Insecure => config_core::TransportSecurity::Insecure,
            Self::MutualTls(_) => config_core::TransportSecurity::MutualTls,
        }
    }

    /// URI scheme a client dials this mode with.
    pub const fn scheme(&self) -> &'static str {
        match self {
            Self::Insecure => "http",
            Self::MutualTls(_) => "https",
        }
    }
}

/// What a peer certificate claims, parsed from its SAN URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertIdentity {
    /// `retcd://<cluster_id>/client/<name>`.
    Client {
        /// Cluster the certificate was minted for.
        cluster_id: ClusterId,
        /// Client name.
        name: String,
    },
    /// `retcd://<cluster_id>/node/<node_id>`.
    Node {
        /// Cluster the certificate was minted for.
        cluster_id: ClusterId,
        /// Node id.
        node_id: NodeId,
    },
}

/// URI scheme prefix every rEtcd identity SAN carries.
pub const RETCD_URI_PREFIX: &str = "retcd://";

/// Parse one `retcd://` SAN URI.
///
/// Returns `None` for any URI that is not a well-formed rEtcd identity, so an attacker cannot
/// smuggle an identity through a malformed SAN that a lenient parser would round up.
pub fn parse_san_uri(uri: &str) -> Option<CertIdentity> {
    let rest = uri.strip_prefix(RETCD_URI_PREFIX)?;
    let mut parts = rest.split('/');
    let cluster_id = ClusterId::from_str(parts.next()?).ok()?;
    let kind = parts.next()?;
    let value = parts.next()?;
    if parts.next().is_some() || value.is_empty() {
        return None;
    }
    match kind {
        "client" => Some(CertIdentity::Client {
            cluster_id,
            name: value.to_string(),
        }),
        "node" => Some(CertIdentity::Node {
            cluster_id,
            node_id: NodeId(value.parse().ok()?),
        }),
        _ => None,
    }
}

/// The two facts an operator needs about a certificate this node is serving (M6, ADR-0028).
///
/// Neither is sensitive, and that is the point: a subject DN, a serial or a SAN would name the
/// principal the certificate belongs to, and these values are exported as metric labels and
/// printed in admin replies (M6-62).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertFacts {
    /// Lowercase hex of the first eight bytes of `sha256(der)`.
    ///
    /// Enough to tell one deployed certificate from another when comparing nodes mid-rotation,
    /// and not a value anything authenticates on.
    pub fingerprint: String,
    /// `notAfter`, in seconds since the Unix epoch.
    pub not_after_unix: i64,
}

/// Read [`CertFacts`] off a leaf certificate, or `None` if the DER does not parse.
///
/// `None` rather than an error: this is reporting, not admission. A certificate this node is
/// already serving has been through rustls' own parser, so a failure here means the reporting
/// path disagrees with the serving path — which must not take a working listener down.
pub fn cert_facts_from_der(der: &[u8]) -> Option<CertFacts> {
    let (_, cert) = X509Certificate::from_der(der).ok()?;
    let digest = Sha256::digest(der);
    let fingerprint = digest[..8]
        .iter()
        .fold(String::with_capacity(16), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        });
    Some(CertFacts {
        fingerprint,
        not_after_unix: cert.validity().not_after.timestamp(),
    })
}

/// Fingerprint every certificate in a PEM bundle, in file order.
///
/// What a rotation log line names its trust anchors by (M6-120). Fingerprints rather than
/// subjects for the same reason [`CertFacts`] carries none: the line is written on a path an
/// operator greps, and a CA subject is the one field in a bundle that identifies an
/// organisation. Unparseable blocks are skipped rather than reported — the bundle has already
/// been through rustls by the time anything logs it.
pub fn ca_fingerprints(pem: &[u8]) -> Vec<String> {
    rustls_pemfile::certs(&mut std::io::Cursor::new(pem))
        .filter_map(|der| cert_facts_from_der(der.ok()?.as_ref()))
        .map(|facts| facts.fingerprint)
        .collect()
}

/// Every URI SAN a DER certificate asserts, verbatim.
///
/// Kept separate from [`identities_from_der`] because "asserts nothing" and "asserts something
/// we refuse to parse" are different facts, and only the first may fall back to a Common Name.
pub fn uri_sans_from_der(der: &[u8]) -> Vec<String> {
    let Ok((_, cert)) = X509Certificate::from_der(der) else {
        return Vec::new();
    };
    let Ok(Some(san)) = cert.subject_alternative_name() else {
        return Vec::new();
    };
    san.value
        .general_names
        .iter()
        .filter_map(|gn| match gn {
            GeneralName::URI(uri) => Some((*uri).to_string()),
            _ => None,
        })
        .collect()
}

/// Every rEtcd identity asserted by a DER certificate's SAN URIs.
pub fn identities_from_der(der: &[u8]) -> Vec<CertIdentity> {
    uri_sans_from_der(der)
        .iter()
        .filter_map(|uri| parse_san_uri(uri))
        .collect()
}

/// The certificate's Common Name, used only when no rEtcd SAN URI is present.
pub fn common_name_from_der(der: &[u8]) -> Option<String> {
    let (_, cert) = X509Certificate::from_der(der).ok()?;
    let subject = cert.subject();
    let cn = subject
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(str::to_string);
    cn.filter(|s| !s.is_empty())
}

/// Derive the client-plane principal from the presented certificate chain (ADR-0012).
///
/// The leaf certificate is the only one consulted; an intermediate asserting an identity
/// would let a CA delegate naming without the leaf saying so.
///
/// `expected_cluster` is this listener's cluster. A `retcd://` client SAN naming a different
/// cluster is refused rather than accepted under its Common Name, and so is a node SAN: see
/// the module docs for why the fallback stops there.
///
/// `allow_common_name_principals` is this listener's
/// [`MtlsConfig::allow_common_name_principals`]. With it `false` — the default — a certificate
/// asserting no `retcd://` SAN is refused instead of being served under its Common Name,
/// because a Common Name cannot be checked against `expected_cluster`.
pub fn principal_from_certs(
    certs: &[impl AsRef<[u8]>],
    expected_cluster: ClusterId,
    allow_common_name_principals: bool,
) -> Result<Principal, Status> {
    let leaf = certs
        .first()
        .ok_or_else(|| Status::unauthenticated("no client certificate presented"))?
        .as_ref();

    let asserted: Vec<String> = uri_sans_from_der(leaf)
        .into_iter()
        .filter(|uri| uri.starts_with(RETCD_URI_PREFIX))
        .collect();

    if asserted.is_empty() {
        // One refusal for the whole branch, whether the gate is shut or the certificate has no
        // Common Name either: the holder already knows what it presented, and a prober must
        // not be able to read this listener's gate setting off the message.
        let common_name = if allow_common_name_principals {
            common_name_from_der(leaf)
        } else {
            None
        };
        return match common_name {
            Some(cn) => Ok(Principal::new(cn, PrincipalKind::Certificate)),
            None => Err(Status::unauthenticated(
                "client certificate carries no retcd SAN URI and no principal could be derived \
                 from it",
            )),
        };
    }

    asserted
        .iter()
        .find_map(|uri| match parse_san_uri(uri) {
            Some(CertIdentity::Client { cluster_id, name }) if cluster_id == expected_cluster => {
                Some(Principal::new(name, PrincipalKind::Certificate))
            }
            _ => None,
        })
        .ok_or_else(|| {
            // No detail: the holder of the certificate already knows what it presented, and a
            // prober must not learn this listener's cluster id from a refusal.
            Status::unauthenticated(
                "client certificate asserts a retcd identity that is not a client of this cluster",
            )
        })
}

/// The node identity a peer certificate claims, if any.
pub fn node_identity_from_certs(certs: &[impl AsRef<[u8]>]) -> Option<(ClusterId, NodeId)> {
    identities_from_der(certs.first()?.as_ref())
        .into_iter()
        .find_map(|id| match id {
            CertIdentity::Node {
                cluster_id,
                node_id,
            } => Some((cluster_id, node_id)),
            CertIdentity::Client { .. } => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const CID: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn san_uri_grammar_is_strict() {
        let cluster = ClusterId::from_str(CID).unwrap();
        assert_eq!(
            parse_san_uri(&format!("retcd://{CID}/client/svc-a")),
            Some(CertIdentity::Client {
                cluster_id: cluster,
                name: "svc-a".into()
            })
        );
        assert_eq!(
            parse_san_uri(&format!("retcd://{CID}/node/7")),
            Some(CertIdentity::Node {
                cluster_id: cluster,
                node_id: NodeId(7)
            })
        );
        // Rejected: wrong scheme, bad cluster id, unknown kind, trailing segment, empty name,
        // non-numeric node id.
        for bad in [
            format!("https://{CID}/client/svc-a"),
            "retcd://nothex/client/svc-a".to_string(),
            format!("retcd://{CID}/admin/svc-a"),
            format!("retcd://{CID}/client/svc-a/extra"),
            format!("retcd://{CID}/client/"),
            format!("retcd://{CID}/node/seven"),
        ] {
            assert_eq!(parse_san_uri(&bad), None, "{bad}");
        }
    }
}
