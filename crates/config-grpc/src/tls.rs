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
//! A certificate with no usable SAN URI falls back to its Common Name, which covers CAs that
//! cannot mint URI SANs. A certificate with neither is refused.

use std::str::FromStr;

use config_core::{ClusterId, NodeId, Principal, PrincipalKind};
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};
use tonic::Status;
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

use crate::error::GrpcError;

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
    /// Peers are addressed by `host:port` from committed membership, which is frequently a
    /// literal address while the certificate names a DNS identity. `None` verifies against
    /// the endpoint's own host.
    pub server_domain: Option<String>,
}

impl MtlsConfig {
    /// Build from PEM material, verifying the dialed server against its own host name.
    pub fn new(ca_pem: Vec<u8>, cert_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        Self {
            ca_pem,
            cert_pem,
            key_pem,
            server_domain: None,
        }
    }

    /// Verify dialed servers against `domain` instead of the endpoint host.
    pub fn with_server_domain(mut self, domain: impl Into<String>) -> Self {
        self.server_domain = Some(domain.into());
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
    pub fn client_tls_config(&self) -> ClientTlsConfig {
        let cfg = ClientTlsConfig::new()
            .identity(self.identity())
            .ca_certificate(self.ca());
        match &self.server_domain {
            Some(d) => cfg.domain_name(d.clone()),
            None => cfg,
        }
    }
}

impl std::fmt::Debug for MtlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MtlsConfig")
            .field("ca_pem_bytes", &self.ca_pem.len())
            .field("cert_pem_bytes", &self.cert_pem.len())
            .field("key_pem_bytes", &self.key_pem.len())
            .field("server_domain", &self.server_domain)
            .finish()
    }
}

/// How a plane protects its connections.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum TlsMode {
    /// Plain TCP. Development and the in-process harness only (ADR-0010); a node serving this
    /// mode reports [`config_core::TransportSecurity::Insecure`].
    #[default]
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

    /// Apply this mode to a tonic server builder.
    pub fn apply_server(
        &self,
        builder: tonic::transport::Server,
    ) -> Result<tonic::transport::Server, GrpcError> {
        match self {
            Self::Insecure => Ok(builder),
            Self::MutualTls(cfg) => builder
                .tls_config(cfg.server_tls_config())
                .map_err(|e| GrpcError::Tls(e.to_string())),
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

/// Parse one `retcd://` SAN URI.
///
/// Returns `None` for any URI that is not a well-formed rEtcd identity, so an attacker cannot
/// smuggle an identity through a malformed SAN that a lenient parser would round up.
pub fn parse_san_uri(uri: &str) -> Option<CertIdentity> {
    let rest = uri.strip_prefix("retcd://")?;
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

/// Every rEtcd identity asserted by a DER certificate's SAN URIs.
pub fn identities_from_der(der: &[u8]) -> Vec<CertIdentity> {
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
            GeneralName::URI(uri) => parse_san_uri(uri),
            _ => None,
        })
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
pub fn principal_from_certs(certs: &[impl AsRef<[u8]>]) -> Result<Principal, Status> {
    let leaf = certs
        .first()
        .ok_or_else(|| Status::unauthenticated("no client certificate presented"))?
        .as_ref();

    if let Some(CertIdentity::Client { name, .. }) = identities_from_der(leaf)
        .into_iter()
        .find(|id| matches!(id, CertIdentity::Client { .. }))
    {
        return Ok(Principal::new(name, PrincipalKind::Certificate));
    }
    match common_name_from_der(leaf) {
        Some(cn) => Ok(Principal::new(cn, PrincipalKind::Certificate)),
        None => Err(Status::unauthenticated(
            "client certificate carries no retcd SAN URI and no common name",
        )),
    }
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
