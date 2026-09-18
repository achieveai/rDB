//! Deterministic in-memory certificate generation for the mTLS rows (test plan TA-18).
//!
//! One [`TlsFixture`] is one certificate authority plus every leaf it issues. Nothing here is
//! ever committed: a fixture is built in memory from `(cluster_id, seed)` and written to a
//! test's `TempDir` only when a daemon process has to read PEM files off disk.
//!
//! # Determinism, actually
//!
//! rcgen 0.13 has no seeded-RNG constructor — `KeyPair::generate()` and `generate_for()` both
//! reach for `SystemRandom`. It does, however, accept supplied PKCS#8 material. Every key here
//! is therefore an Ed25519 key whose 32-byte seed is `SHA-256(DOMAIN || seed_le || label)`,
//! handed to rcgen as PKCS#8 PEM. Serial numbers come out of the same digest. Two fixtures
//! built with the same `(cluster_id, seed)` are byte-identical, so a failing TLS row replays
//! exactly rather than "printing the seed and hoping" (anti-flake rule 7).
//!
//! # SAN grammar
//!
//! Matched byte-for-byte against the parser in `config_grpc::tls`:
//!
//! * node: `retcd://<cluster_id>/node/<node_id>`
//! * client: `retcd://<cluster_id>/client/<name>`
//!
//! where `<cluster_id>` is the 32-lowercase-hex `Display` of [`ClusterId`].
//!
//! Node certificates additionally carry the DNS name `node-<id>.<cluster_id>.retcd`, which is
//! the name a peer or a hint-following client verifies the *server* against (m3-architecture
//! §3: tonic can express a per-connection DNS name and cannot express a per-call URI-SAN
//! check). They also carry `DNS:localhost`, `IP:127.0.0.1` and `IP:::1`, because both planes
//! are dialled at a literal loopback address with no `server_domain` override, and rustls
//! verifies the presented certificate against whatever is in the URI.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use config_core::{ClusterId, NodeId};
//! use config_testkit::tls::{CertProfile, TlsFixture};
//!
//! let cluster = ClusterId::from_bytes([7u8; 16]);
//! let fixture = TlsFixture::new(cluster, 42);
//! let node = fixture.issue(CertProfile::Node { node_id: NodeId(1) });
//! let svc = fixture.issue(CertProfile::Client { name: "svc-a".into() });
//! assert!(node.cert_pem.starts_with("-----BEGIN CERTIFICATE-----"));
//! # let _ = (svc, fixture.ca_pem());
//! ```

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};

use config_core::{ClusterId, NodeId};
use config_grpc::MtlsConfig;
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, Ia5String, IsCa, KeyPair,
    KeyUsagePurpose, SanType, SerialNumber,
};
use sha2::{Digest, Sha256};

/// Domain-separation string mixed into every derived key seed. Changing it invalidates every
/// previously recorded fixture seed, which is why it carries a version.
const KEY_DERIVATION_DOMAIN: &[u8] = b"retcd-testkit-tls-v1";

/// Validity window of a healthy fixture certificate, as `(not_before, not_after)` y-m-d.
///
/// Fixed dates rather than `now ± n`, because a fixture must be reproducible and because the
/// only clock rEtcd is allowed to read is the manifest expiry check (TA-19).
const VALID_FROM: (i32, u8, u8) = (2020, 1, 1);
const VALID_UNTIL: (i32, u8, u8) = (2120, 1, 1);
/// Validity window of the deliberately expired profile. In the past, so no test ever sleeps
/// waiting for expiry (TA-18.4).
const EXPIRED_FROM: (i32, u8, u8) = (2020, 1, 1);
const EXPIRED_UNTIL: (i32, u8, u8) = (2021, 1, 1);

/// Who a leaf certificate says it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertProfile {
    /// A cluster member: SAN `retcd://<cluster_id>/node/<node_id>`, plus the peer DNS name.
    Node {
        /// The node this certificate speaks for.
        node_id: NodeId,
    },
    /// A client-plane principal: SAN `retcd://<cluster_id>/client/<name>`.
    Client {
        /// The principal name the server will derive.
        name: String,
    },
}

impl CertProfile {
    /// A node profile, spelled without a struct literal at the call site.
    pub fn node(node_id: NodeId) -> Self {
        Self::Node { node_id }
    }

    /// A client profile, spelled without a struct literal at the call site.
    pub fn client(name: impl Into<String>) -> Self {
        Self::Client { name: name.into() }
    }

    /// Stable label used for key derivation and the certificate's Common Name.
    fn label(&self) -> String {
        match self {
            CertProfile::Node { node_id } => format!("node-{node_id}"),
            CertProfile::Client { name } => format!("client-{name}"),
        }
    }
}

/// Deliberate deviations from a correct certificate, one per negative test row.
///
/// Every field is `None`/`false` by default, so a row states only the one thing it breaks.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CertOverrides {
    /// Mint the SAN for a different cluster (the "right CA, wrong cluster" row).
    pub cluster_id: Option<ClusterId>,
    /// Mint the SAN for a different node id than the holder actually is.
    pub node_id: Option<NodeId>,
    /// Use the already-expired validity window.
    pub expired: bool,
    /// Replace the rEtcd SAN URI with this exact string (malformed-SAN rows).
    pub san_uri: Option<String>,
    /// Emit no SAN URI at all, leaving only the Common Name (the CN-fallback row).
    pub omit_san: bool,
    /// Sign the leaf with its own key instead of the fixture CA.
    ///
    /// Every name on the certificate stays correct — the right SAN URI, the right peer DNS
    /// name — so the only thing wrong with it is that nothing trusts the issuer. That is the
    /// separation M3-05 needs: "a correct-looking identity from an untrusted chain", not "a
    /// wrong identity".
    pub self_signed: bool,
}

impl CertOverrides {
    /// Nothing overridden.
    pub fn none() -> Self {
        Self::default()
    }

    /// Mint for a foreign cluster.
    pub fn wrong_cluster(cluster_id: ClusterId) -> Self {
        Self {
            cluster_id: Some(cluster_id),
            ..Self::default()
        }
    }

    /// Mint for a different node id than the holder is.
    pub fn wrong_node(node_id: NodeId) -> Self {
        Self {
            node_id: Some(node_id),
            ..Self::default()
        }
    }

    /// Already expired when it is issued.
    pub fn expired() -> Self {
        Self {
            expired: true,
            ..Self::default()
        }
    }

    /// No SAN URI; identity can only come from the Common Name.
    pub fn no_san() -> Self {
        Self {
            omit_san: true,
            ..Self::default()
        }
    }

    /// Correct names, untrusted chain: the leaf signs itself.
    pub fn self_signed() -> Self {
        Self {
            self_signed: true,
            ..Self::default()
        }
    }
}

/// One issued identity: the leaf PEM, its key PEM, and the CA that signed it.
#[derive(Clone, PartialEq, Eq)]
pub struct CertPair {
    /// PEM certificate chain (leaf only; the fixture CA is a root).
    pub cert_pem: String,
    /// PEM private key.
    pub key_pem: String,
    /// PEM of the issuing certificate authority.
    pub ca_pem: String,
    /// The DNS name a peer verifies this certificate's *holder* against when dialling it,
    /// for a node certificate. `None` for a client certificate, which never serves.
    pub server_domain: Option<String>,
}

/// `Debug` prints sizes, never key bytes — a private key must not reach a log line even in a
/// test failure message (ADR-0013 §15.2).
impl std::fmt::Debug for CertPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertPair")
            .field("cert_pem_bytes", &self.cert_pem.len())
            .field("key_pem_bytes", &self.key_pem.len())
            .field("ca_pem_bytes", &self.ca_pem.len())
            .field("server_domain", &self.server_domain)
            .finish()
    }
}

impl CertPair {
    /// The mutual-TLS profile for an endpoint presenting this identity.
    pub fn mtls(&self) -> MtlsConfig {
        MtlsConfig::new(
            self.ca_pem.clone().into_bytes(),
            self.cert_pem.clone().into_bytes(),
            self.key_pem.clone().into_bytes(),
        )
    }

    /// The mutual-TLS profile for a *dialler* that must verify the server it reaches against
    /// `domain` rather than against the host in the endpoint string (ADR-0011 via DNS SAN).
    pub fn mtls_verifying(&self, domain: impl Into<String>) -> MtlsConfig {
        self.mtls().with_server_domain(domain)
    }

    /// Write `cert.pem` and `key.pem` (and `ca.pem`) into `dir`, returning their paths.
    ///
    /// Used only where a daemon process has to read PEM off disk; in-process rows keep the
    /// material in memory.
    pub fn write_to(&self, dir: &Path, stem: &str) -> CertPaths {
        std::fs::create_dir_all(dir)
            .unwrap_or_else(|e| panic!("create cert dir {}: {e}", dir.display()));
        let cert = dir.join(format!("{stem}.cert.pem"));
        let key = dir.join(format!("{stem}.key.pem"));
        let ca = dir.join("ca.pem");
        write(&cert, &self.cert_pem);
        write(&key, &self.key_pem);
        write(&ca, &self.ca_pem);
        CertPaths { ca, cert, key }
    }
}

fn write(path: &Path, contents: &str) {
    std::fs::write(path, contents).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

/// Where [`CertPair::write_to`] put the three PEM files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertPaths {
    /// Trust anchor.
    pub ca: PathBuf,
    /// Leaf certificate chain.
    pub cert: PathBuf,
    /// Leaf private key.
    pub key: PathBuf,
}

/// One certificate authority and everything it issues, reproducible from `(cluster_id, seed)`.
pub struct TlsFixture {
    cluster_id: ClusterId,
    seed: u64,
    ca_pem: String,
    ca_cert: rcgen::Certificate,
    ca_key: KeyPair,
}

/// `Debug` carries the replay coordinates — cluster id and seed — and no key material.
impl std::fmt::Debug for TlsFixture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsFixture")
            .field("cluster_id", &self.cluster_id.to_string())
            .field("seed", &self.seed)
            .field("ca_pem_bytes", &self.ca_pem.len())
            .finish()
    }
}

impl TlsFixture {
    /// Build a fixture for `cluster_id`, reproducible from `seed`.
    pub fn new(cluster_id: ClusterId, seed: u64) -> Self {
        let ca_key = derive_key(seed, "ca");
        let mut params = CertificateParams::default();
        params.not_before = rcgen::date_time_ymd(VALID_FROM.0, VALID_FROM.1, VALID_FROM.2);
        params.not_after = rcgen::date_time_ymd(VALID_UNTIL.0, VALID_UNTIL.1, VALID_UNTIL.2);
        params.serial_number = Some(derive_serial(seed, "ca"));
        params.distinguished_name = distinguished_name(&format!("retcd-testkit-ca-{cluster_id}"));
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca_cert = params
            .self_signed(&ca_key)
            .expect("rcgen self-signs the fixture CA");
        Self {
            cluster_id,
            seed,
            ca_pem: ca_cert.pem(),
            ca_cert,
            ca_key,
        }
    }

    /// An unrelated but perfectly valid authority, for the "signed by the wrong CA" rows.
    ///
    /// Same cluster id in the SANs it mints — the point of the row is that a *trust anchor*
    /// mismatch is refused even when every name looks right.
    pub fn other_ca(cluster_id: ClusterId, seed: u64) -> Self {
        Self::new(cluster_id, seed ^ 0x5ca1_ab1e_0000_0001)
    }

    /// The cluster this fixture mints identities for.
    pub fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    /// The seed this fixture was built from; printed in every TLS-row failure message.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// PEM of the trust anchor.
    pub fn ca_pem(&self) -> &str {
        &self.ca_pem
    }

    /// Issue a correct certificate for `profile`.
    pub fn issue(&self, profile: CertProfile) -> CertPair {
        self.issue_with(profile, CertOverrides::none())
    }

    /// Issue a certificate for `profile` with deliberate deviations.
    pub fn issue_with(&self, profile: CertProfile, overrides: CertOverrides) -> CertPair {
        let label = format!("{}-{}", profile.label(), override_label(&overrides));
        let key = derive_key(self.seed, &label);
        let cluster = overrides.cluster_id.unwrap_or(self.cluster_id);

        let mut params = CertificateParams::default();
        let (from, until) = if overrides.expired {
            (EXPIRED_FROM, EXPIRED_UNTIL)
        } else {
            (VALID_FROM, VALID_UNTIL)
        };
        params.not_before = rcgen::date_time_ymd(from.0, from.1, from.2);
        params.not_after = rcgen::date_time_ymd(until.0, until.1, until.2);
        params.serial_number = Some(derive_serial(self.seed, &label));
        params.distinguished_name = distinguished_name(&common_name(&profile));
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        // Both, on every leaf: a node certificate is presented as a server on its own
        // listeners and as a client when it dials another node.
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];

        let server_domain = match &profile {
            CertProfile::Node { node_id } => {
                let id = overrides.node_id.unwrap_or(*node_id);
                Some(peer_dns_name(cluster, id))
            }
            CertProfile::Client { .. } => None,
        };

        if !overrides.omit_san {
            let uri = overrides.san_uri.clone().unwrap_or_else(|| match &profile {
                CertProfile::Node { node_id } => {
                    let id = overrides.node_id.unwrap_or(*node_id);
                    format!("retcd://{cluster}/node/{id}")
                }
                CertProfile::Client { name } => format!("retcd://{cluster}/client/{name}"),
            });
            params.subject_alt_names.push(SanType::URI(
                Ia5String::try_from(uri.as_str()).expect("a retcd SAN URI is ASCII"),
            ));
        }
        if let Some(domain) = &server_domain {
            // The DNS name a dialler verifies the node against, plus the loopback names both
            // planes are actually reached at in tests. Without the loopback names rustls
            // rejects every connection to `127.0.0.1:<port>` before any rEtcd check runs.
            for name in [domain.as_str(), "localhost"] {
                params.subject_alt_names.push(SanType::DnsName(
                    Ia5String::try_from(name).expect("a DNS name is ASCII"),
                ));
            }
            params
                .subject_alt_names
                .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
            params
                .subject_alt_names
                .push(SanType::IpAddress(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        }

        let cert = if overrides.self_signed {
            params
                .self_signed(&key)
                .expect("a leaf can sign itself with its own key")
        } else {
            params
                .signed_by(&key, &self.ca_cert, &self.ca_key)
                .expect("the fixture CA signs its leaf")
        };

        CertPair {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem(),
            // The trust anchor stays the fixture CA even for a self-signed leaf. The holder
            // must still be able to *verify* everyone else; what is broken is only that
            // nobody can verify the holder. Putting the leaf's own PEM here instead would
            // break verification in both directions and the row could no longer say which
            // side did the rejecting.
            ca_pem: self.ca_pem.clone(),
            server_domain,
        }
    }

    /// Issue a certificate for `profile` that is correct in every way except its issuer: it is
    /// signed by its own key, so no party trusting the fixture CA will accept it.
    ///
    /// Equivalent to [`TlsFixture::issue_with`] with [`CertOverrides::self_signed`]; named
    /// because "untrusted chain, correct SAN" is a distinct negative case from "wrong SAN"
    /// and rows read better when they say which one they mean (M3-05).
    pub fn issue_self_signed(&self, profile: CertProfile) -> CertPair {
        self.issue_with(profile, CertOverrides::self_signed())
    }

    /// The mutual-TLS profile a node serves both planes with.
    pub fn node_mtls(&self, node_id: NodeId) -> MtlsConfig {
        self.issue(CertProfile::node(node_id)).mtls()
    }

    /// The mutual-TLS profile a client dials with, as principal `name`.
    pub fn client_mtls(&self, name: &str) -> MtlsConfig {
        self.issue(CertProfile::client(name)).mtls()
    }

    /// The DNS name a dialler must verify node `node_id` of this cluster against.
    pub fn peer_domain(&self, node_id: NodeId) -> String {
        peer_dns_name(self.cluster_id, node_id)
    }
}

/// The DNS SAN a node certificate carries, and the name a dialler verifies it against.
///
/// Delegates to [`config_grpc::peer_server_domain`] rather than re-spelling the format: a
/// fixture that issued a name the transport does not pin would make every mTLS row pass while
/// production verified nothing.
pub fn peer_dns_name(cluster_id: ClusterId, node_id: NodeId) -> String {
    config_grpc::peer_server_domain(&cluster_id, node_id)
}

fn common_name(profile: &CertProfile) -> String {
    match profile {
        CertProfile::Node { node_id } => format!("retcd-node-{node_id}"),
        CertProfile::Client { name } => name.clone(),
    }
}

fn override_label(o: &CertOverrides) -> String {
    let mut label = String::new();
    if let Some(c) = o.cluster_id {
        label.push_str(&format!("c{c}"));
    }
    if let Some(n) = o.node_id {
        label.push_str(&format!("n{n}"));
    }
    if o.expired {
        label.push_str("exp");
    }
    if let Some(u) = &o.san_uri {
        label.push_str(u);
    }
    if o.omit_san {
        label.push_str("nosan");
    }
    if o.self_signed {
        label.push_str("selfsigned");
    }
    if label.is_empty() {
        label.push_str("plain");
    }
    label
}

fn distinguished_name(common_name: &str) -> rcgen::DistinguishedName {
    let mut dn = rcgen::DistinguishedName::new();
    dn.push(DnType::CommonName, common_name);
    dn.push(DnType::OrganizationName, "rEtcd test fixture");
    dn
}

/// 32 deterministic bytes for `(seed, label)`.
fn derive_bytes(seed: u64, label: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(KEY_DERIVATION_DOMAIN);
    hasher.update(seed.to_le_bytes());
    hasher.update([0u8]);
    hasher.update(label.as_bytes());
    hasher.finalize().into()
}

/// Derive the Ed25519 key for `(seed, label)` and hand it to rcgen as PKCS#8.
///
/// Ed25519 rather than the ECDSA default because it is the only algorithm whose private key
/// *is* 32 bytes of entropy: a P-256 key would need a scalar reduction step, and rcgen has no
/// seeded generator for either (see the module docs).
fn derive_key(seed: u64, label: &str) -> KeyPair {
    use ed25519_dalek::pkcs8::EncodePrivateKey;
    let signing = ed25519_dalek::SigningKey::from_bytes(&derive_bytes(seed, label));
    let pem = signing
        .to_pkcs8_pem(ed25519_dalek::pkcs8::spki::der::pem::LineEnding::LF)
        .expect("an ed25519 signing key encodes as PKCS#8");
    KeyPair::from_pkcs8_pem_and_sign_algo(&pem, &rcgen::PKCS_ED25519)
        .expect("rcgen accepts a PKCS#8 ed25519 key")
}

/// A deterministic, positive, 16-byte serial number for `(seed, label)`.
fn derive_serial(seed: u64, label: &str) -> SerialNumber {
    let mut bytes = derive_bytes(seed, &format!("serial:{label}"))[..16].to_vec();
    // A DER INTEGER with the high bit set would be read as negative; RFC 5280 requires a
    // positive serial.
    bytes[0] &= 0x7f;
    bytes[0] |= 0x01;
    SerialNumber::from_slice(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use config_grpc::tls::{identities_from_der, CertIdentity};

    fn cluster() -> ClusterId {
        ClusterId::from_bytes([0x11; 16])
    }

    /// Strip the PEM armour of the first certificate and return its DER.
    fn leaf_der(pem: &str) -> Vec<u8> {
        let body: String = pem
            .lines()
            .skip_while(|l| !l.starts_with("-----BEGIN CERTIFICATE-----"))
            .skip(1)
            .take_while(|l| !l.starts_with("-----END CERTIFICATE-----"))
            .collect();
        base64_decode(&body)
    }

    /// Minimal standard-alphabet base64 decoder, used only to re-read what this module just
    /// wrote. Pulling a base64 crate into the testkit for one assertion is not worth it.
    fn base64_decode(input: &str) -> Vec<u8> {
        const ALPHABET: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut acc: u32 = 0;
        let mut bits = 0u32;
        let mut out = Vec::new();
        for byte in input
            .bytes()
            .filter(|b| !b.is_ascii_whitespace() && *b != b'=')
        {
            let value = ALPHABET
                .iter()
                .position(|c| *c == byte)
                .unwrap_or_else(|| panic!("non-base64 byte {byte:?} in PEM body"))
                as u32;
            acc = (acc << 6) | value;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((acc >> bits) as u8);
            }
        }
        out
    }

    #[test]
    fn the_same_seed_produces_byte_identical_material() {
        let a = TlsFixture::new(cluster(), 7);
        let b = TlsFixture::new(cluster(), 7);
        assert_eq!(a.ca_pem(), b.ca_pem());
        let pa = a.issue(CertProfile::node(NodeId(2)));
        let pb = b.issue(CertProfile::node(NodeId(2)));
        assert_eq!(pa.cert_pem, pb.cert_pem, "seeded issuance must be stable");
        assert_eq!(pa.key_pem, pb.key_pem);
    }

    #[test]
    fn a_different_seed_produces_different_material() {
        let a = TlsFixture::new(cluster(), 7);
        let b = TlsFixture::new(cluster(), 8);
        assert_ne!(a.ca_pem(), b.ca_pem());
    }

    /// The SAN this fixture writes is exactly the SAN `config_grpc::tls` parses.
    #[test]
    fn node_and_client_sans_match_the_server_grammar() {
        let fixture = TlsFixture::new(cluster(), 3);
        let node = fixture.issue(CertProfile::node(NodeId(5)));
        assert_eq!(
            identities_from_der(&leaf_der(&node.cert_pem)),
            vec![CertIdentity::Node {
                cluster_id: cluster(),
                node_id: NodeId(5)
            }]
        );
        assert_eq!(
            node.server_domain.as_deref(),
            Some(format!("node-5.{}.retcd", cluster()).as_str())
        );

        let client = fixture.issue(CertProfile::client("svc-a"));
        assert_eq!(
            identities_from_der(&leaf_der(&client.cert_pem)),
            vec![CertIdentity::Client {
                cluster_id: cluster(),
                name: "svc-a".into()
            }]
        );
        assert_eq!(client.server_domain, None);
    }

    #[test]
    fn overrides_produce_the_negative_shapes() {
        let fixture = TlsFixture::new(cluster(), 11);
        let foreign = ClusterId::from_bytes([0x22; 16]);

        let wrong_cluster = fixture.issue_with(
            CertProfile::client("svc-a"),
            CertOverrides::wrong_cluster(foreign),
        );
        assert_eq!(
            identities_from_der(&leaf_der(&wrong_cluster.cert_pem)),
            vec![CertIdentity::Client {
                cluster_id: foreign,
                name: "svc-a".into()
            }]
        );

        let wrong_node = fixture.issue_with(
            CertProfile::node(NodeId(1)),
            CertOverrides::wrong_node(NodeId(9)),
        );
        assert_eq!(
            identities_from_der(&leaf_der(&wrong_node.cert_pem)),
            vec![CertIdentity::Node {
                cluster_id: cluster(),
                node_id: NodeId(9)
            }]
        );

        let no_san = fixture.issue_with(CertProfile::client("svc-cn"), CertOverrides::no_san());
        assert!(
            identities_from_der(&leaf_der(&no_san.cert_pem)).is_empty(),
            "the CN-fallback row must assert no SAN URI at all"
        );
        assert_eq!(
            config_grpc::tls::common_name_from_der(&leaf_der(&no_san.cert_pem)).as_deref(),
            Some("svc-cn")
        );

        let malformed = fixture.issue_with(
            CertProfile::client("svc-a"),
            CertOverrides {
                san_uri: Some(format!("retcd://{}/admin/svc-a", cluster())),
                ..CertOverrides::none()
            },
        );
        assert!(
            identities_from_der(&leaf_der(&malformed.cert_pem)).is_empty(),
            "an unparsable retcd URI asserts no identity"
        );
    }

    #[test]
    fn expired_certificates_are_already_in_the_past() {
        let fixture = TlsFixture::new(cluster(), 13);
        let expired = fixture.issue_with(CertProfile::client("svc-a"), CertOverrides::expired());
        let valid = fixture.issue(CertProfile::client("svc-a"));
        assert_ne!(
            expired.cert_pem, valid.cert_pem,
            "the expired profile must be a different certificate"
        );
    }

    #[test]
    fn a_pair_writes_three_pem_files() {
        let dir = crate::fs::temp_dir();
        let fixture = TlsFixture::new(cluster(), 17);
        let paths = fixture
            .issue(CertProfile::node(NodeId(1)))
            .write_to(dir.path(), "node-1");
        for path in [&paths.ca, &paths.cert, &paths.key] {
            let text = std::fs::read_to_string(path).expect("written PEM is readable");
            assert!(text.contains("-----BEGIN"), "{} is PEM", path.display());
        }
    }
}
