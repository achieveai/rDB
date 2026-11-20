//! Development certificates for the process links (`rdb_node --gen-certs`).
//!
//! One CA per directory. A node's certificate names it twice: the URI SAN `rdb://node/<n>`,
//! which the listener reads as the dialler's label (S4 F1: the label comes from the
//! certificate, never from the handshake's claim), and the DNS SAN `node-<n>.rdb`, which a
//! dialler pins so that TLS itself proves it reached node `n`. An app's names it as
//! `rdb://app/<name>` and has no DNS name: nobody dials an app.
//!
//! Layout: `ca.pem`, and `node-<n>.pem` / `node-<n>.key`, `app-<name>.pem` / `app-<name>.key`.
//! Development only: the CA key is written next to the certificates so a later run can issue
//! more.
//!
//! #80 S2 (ADR-rdb-0024): the link to the control store (rEtcd) is mutual TLS too, from a
//! `--control-tls DIR` holding `ca.pem`, `client.pem` and `client.key` ([`load_control`]). The
//! client certificate names its rEtcd identity as the URI SAN `retcd://<cluster>/client/<name>`,
//! and the cluster id is read from there. [`generate_control`] writes development material in
//! that layout under `<cert-dir>/control/`, with each rEtcd voter's certificate beside it, signed
//! by the same development CA. Production brings its own files in the same layout.

use std::path::{Path, PathBuf};

use config_core::ClusterId;
use config_grpc::tls::CertIdentity;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose,
    Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use rdb_core::contracts::ids::NodeId;

use super::wire::Label;

/// The URI SAN prefix every rDB link identity carries.
pub const URI_PREFIX: &str = "rdb://";

/// The DNS name node `node` presents, and the one a dialler pins.
#[must_use]
pub fn node_domain(node: NodeId) -> String {
    format!("node-{}.rdb", node.0)
}

/// The URI SAN for `label`.
#[must_use]
pub fn label_uri(label: &Label) -> String {
    match label {
        Label::Node(node) => format!("{URI_PREFIX}node/{}", node.0),
        Label::App(name) => format!("{URI_PREFIX}app/{name}"),
    }
}

/// The label a URI SAN names, or `None` for anything that is not exactly one.
#[must_use]
pub fn parse_label_uri(uri: &str) -> Option<Label> {
    let rest = uri.strip_prefix(URI_PREFIX)?;
    let (kind, value) = rest.split_once('/')?;
    if value.is_empty() || value.contains('/') {
        return None;
    }
    match kind {
        "node" => Some(Label::Node(NodeId(value.parse().ok()?))),
        "app"
            if value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') =>
        {
            Some(Label::App(value.to_owned()))
        }
        _ => None,
    }
}

/// The one label a leaf certificate's URI SANs name. `None` when it names none, or more than
/// one: a certificate that could be read two ways is read neither.
#[must_use]
pub fn label_from_der(der: &[u8]) -> Option<Label> {
    let labels: Vec<Label> = config_grpc::tls::uri_sans_from_der(der)
        .iter()
        .filter_map(|uri| parse_label_uri(uri))
        .collect();
    match labels.as_slice() {
        [one] => Some(one.clone()),
        _ => None,
    }
}

/// The certificate and key file names for `label`.
#[must_use]
pub fn files(dir: &Path, label: &Label) -> (PathBuf, PathBuf) {
    let stem = label.to_string();
    (
        dir.join(format!("{stem}.pem")),
        dir.join(format!("{stem}.key")),
    )
}

/// Load `label`'s mutual-TLS material from `dir`.
///
/// # Errors
///
/// Names the file that could not be read, and the step: `rdb_node --gen-certs`.
pub fn load(dir: &Path, label: &Label) -> Result<config_grpc::MtlsConfig, String> {
    let (cert, key) = files(dir, label);
    let read = |path: &Path| {
        std::fs::read(path).map_err(|error| {
            format!(
                "{}: {error}; create the certificates with `rdb_node --gen-certs --cert-dir {}`",
                path.display(),
                dir.display()
            )
        })
    };
    Ok(config_grpc::MtlsConfig::new(
        read(&dir.join("ca.pem"))?,
        read(&cert)?,
        read(&key)?,
    ))
}

/// The folder under a `--cert-dir` that [`generate_control`] writes the rEtcd material into.
pub const CONTROL_DIR: &str = "control";

/// The rEtcd client name every rDB process presents on the control link (#80 S2 ruling 2: one
/// shared identity until the certificate join of S4 mints one per node).
pub const CONTROL_CLIENT: &str = "rdb";

/// What [`load_control`] read: the mutual-TLS material for the control link, and the rEtcd
/// cluster and client name its certificate names.
#[derive(Debug, Clone)]
pub struct ControlTls {
    /// `ca.pem`, `client.pem` and `client.key`, for `config_client::GrpcClientOptions::tls`.
    pub mtls: config_grpc::MtlsConfig,
    /// The cluster from `client.pem`'s `retcd://<cluster>/client/<name>` SAN: what
    /// `GrpcClient::with_cluster_id` needs before a leader hint may be followed under mutual
    /// TLS.
    pub cluster: ClusterId,
    /// The client name from that SAN: the principal rEtcd's policy grants.
    pub principal: String,
}

/// How [`load_control`] names the step that makes a control-TLS folder.
fn control_step(dir: &Path) -> String {
    format!(
        "create it with `rdb_node --gen-certs --cert-dir C --control-cluster CID --control-voters \
         1=HOST,...` (it writes C/{CONTROL_DIR}), or bring your own ca.pem, client.pem and \
         client.key in {}",
        dir.display()
    )
}

/// Load the control link's mutual-TLS material from `dir` (#80 S2, ADR-rdb-0024): `ca.pem`,
/// `client.pem` and `client.key`. The rEtcd cluster and client name are read from the client
/// certificate, which must name exactly one rEtcd identity, a client one.
///
/// # Errors
///
/// A missing or unreadable file, named with its path and the step that makes it; a
/// `client.pem` with no certificate, no `retcd://` client SAN, a node SAN, or more than one
/// rEtcd identity, named with its path.
pub fn load_control(dir: &Path) -> Result<ControlTls, String> {
    let read = |name: &str| {
        let path = dir.join(name);
        std::fs::read(&path)
            .map_err(|error| format!("{}: {error}; {}", path.display(), control_step(dir)))
    };
    let ca = read("ca.pem")?;
    let cert = read("client.pem")?;
    let key = read("client.key")?;
    let (cluster, principal) = control_identity(&cert)
        .map_err(|why| format!("{}: {why}", dir.join("client.pem").display()))?;
    Ok(ControlTls {
        mtls: config_grpc::MtlsConfig::new(ca, cert, key),
        cluster,
        principal,
    })
}

/// The rEtcd cluster and client name the first certificate of `pem` names, or why it names
/// none: a certificate that could be read two ways is read neither.
fn control_identity(pem: &[u8]) -> Result<(ClusterId, String), String> {
    let leaf = rustls_pemfile::certs(&mut &pem[..])
        .next()
        .ok_or_else(|| "holds no PEM certificate".to_owned())?
        .map_err(|error| format!("does not parse as a PEM certificate: {error}"))?;
    match config_grpc::tls::identities_from_der(leaf.as_ref()).as_slice() {
        [CertIdentity::Client { cluster_id, name }] => Ok((*cluster_id, name.clone())),
        [CertIdentity::Node {
            cluster_id,
            node_id,
        }] => Err(format!(
            "names rEtcd node {node_id} of cluster {cluster_id}, not a client: the control link \
             presents a client certificate, retcd://<cluster>/client/<name>"
        )),
        [] => Err(
            "names no rEtcd client: it carries no retcd://<cluster>/client/<name> URI SAN"
                .to_owned(),
        ),
        more => Err(format!(
            "names {} rEtcd identities: a control client certificate names exactly one",
            more.len()
        )),
    }
}

fn named(cn: &str) -> DistinguishedName {
    let mut name = DistinguishedName::new();
    name.push(DnType::CommonName, cn);
    name
}

fn ia5(text: &str) -> Result<Ia5String, String> {
    Ia5String::try_from(text).map_err(|error| format!("{text:?}: {error}"))
}

/// Write a fresh CA and one certificate per label into `dir`, creating it. Files already there
/// are replaced, so a run's certificates are always from one CA.
///
/// # Errors
///
/// The step that failed, with the path.
pub fn generate(dir: &Path, labels: &[Label]) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|error| format!("{}: {error}", dir.display()))?;
    let rcgen = |error: rcgen::Error| error.to_string();
    let ca_key = KeyPair::generate().map_err(rcgen)?;
    let ca_cert = dev_ca().self_signed(&ca_key).map_err(rcgen)?;
    let write = |path: PathBuf, text: String| {
        std::fs::write(&path, text).map_err(|error| format!("{}: {error}", path.display()))
    };
    write(dir.join("ca.pem"), ca_cert.pem())?;
    write(dir.join("ca.key"), ca_key.serialize_pem())?;
    for label in labels {
        let key = KeyPair::generate().map_err(rcgen)?;
        let mut params = CertificateParams::default();
        params.distinguished_name = named(&label.to_string());
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        // A node is a server on its listener and a client when it dials a peer.
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params
            .subject_alt_names
            .push(SanType::URI(ia5(&label_uri(label))?));
        if let Label::Node(node) = label {
            params
                .subject_alt_names
                .push(SanType::DnsName(ia5(&node_domain(*node))?));
        }
        let cert = params.signed_by(&key, &ca_cert, &ca_key).map_err(rcgen)?;
        let (cert_path, key_path) = files(dir, label);
        write(cert_path, cert.pem())?;
        write(key_path, key.serialize_pem())?;
    }
    tracing::info!(
        dir = %dir.display(),
        labels = ?labels.iter().map(ToString::to_string).collect::<Vec<_>>(),
        "net_certs_generated"
    );
    Ok(())
}

/// The development CA's parameters. [`generate_control`] makes them again over `ca.key` to
/// sign with: a certificate's issuer is matched by this name and checked by that key's
/// signature, so it chains to the `ca.pem` [`generate`] wrote.
fn dev_ca() -> CertificateParams {
    let mut ca = CertificateParams::default();
    ca.distinguished_name = named("rdb-dev-ca");
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    ca
}

/// Write development rEtcd material into `dir/control/` (#80 S2, ADR-rdb-0024), signed by the
/// development CA [`generate`] wrote into `dir` (`ca.pem`, `ca.key`):
///
/// - `ca.pem`, a copy of that CA;
/// - `client.pem` / `client.key`: SAN `retcd://<cluster>/client/rdb` ([`CONTROL_CLIENT`]),
///   client auth only; what every rDB process presents to rEtcd;
/// - per voter `(n, host)`, `voter-<n>.pem` / `voter-<n>.key`: URI SAN
///   `retcd://<cluster>/node/<n>`, DNS SAN `config_grpc::peer_server_domain(cluster, n)` (what a
///   leader-hint dial and a peer pin), and `host` as an IP SAN, or a DNS SAN when it is a name
///   (what a client dialling the voter's address verifies). Server and client auth: a voter
///   serves, and dials its peers.
///
/// # Errors
///
/// The step that failed, with the path.
pub fn generate_control(
    dir: &Path,
    cluster: ClusterId,
    voters: &[(u64, String)],
) -> Result<(), String> {
    let rcgen = |error: rcgen::Error| error.to_string();
    let read = |name: &str| {
        let path = dir.join(name);
        std::fs::read_to_string(&path).map_err(|error| {
            format!(
                "{}: {error}; the control material is signed by the development CA, so run \
                 `rdb_node --gen-certs --cert-dir {}` first",
                path.display(),
                dir.display()
            )
        })
    };
    let ca_pem = read("ca.pem")?;
    let ca_key = KeyPair::from_pem(&read("ca.key")?).map_err(rcgen)?;
    let ca_cert = dev_ca().self_signed(&ca_key).map_err(rcgen)?;
    let out = dir.join(CONTROL_DIR);
    std::fs::create_dir_all(&out).map_err(|error| format!("{}: {error}", out.display()))?;
    let write = |name: String, text: String| {
        let path = out.join(name);
        std::fs::write(&path, text).map_err(|error| format!("{}: {error}", path.display()))
    };
    write("ca.pem".to_owned(), ca_pem)?;
    let issue = |cn: String, sans: Vec<SanType>, server: bool| {
        let key = KeyPair::generate().map_err(rcgen)?;
        let mut params = CertificateParams::default();
        params.distinguished_name = named(&cn);
        params.is_ca = IsCa::NoCa;
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        params.extended_key_usages = if server {
            vec![
                ExtendedKeyUsagePurpose::ServerAuth,
                ExtendedKeyUsagePurpose::ClientAuth,
            ]
        } else {
            vec![ExtendedKeyUsagePurpose::ClientAuth]
        };
        params.subject_alt_names = sans;
        let cert = params.signed_by(&key, &ca_cert, &ca_key).map_err(rcgen)?;
        Ok::<_, String>((cert.pem(), key.serialize_pem()))
    };
    let client_uri = format!("retcd://{cluster}/client/{CONTROL_CLIENT}");
    let (cert, key) = issue(
        CONTROL_CLIENT.to_owned(),
        vec![SanType::URI(ia5(&client_uri)?)],
        false,
    )?;
    write("client.pem".to_owned(), cert)?;
    write("client.key".to_owned(), key)?;
    for (n, host) in voters {
        let node = config_core::NodeId(*n);
        let mut sans = vec![
            SanType::URI(ia5(&format!("retcd://{cluster}/node/{n}"))?),
            SanType::DnsName(ia5(&config_grpc::peer_server_domain(&cluster, node))?),
        ];
        sans.push(match host.parse::<std::net::IpAddr>() {
            Ok(ip) => SanType::IpAddress(ip),
            Err(_) => SanType::DnsName(ia5(host)?),
        });
        let (cert, key) = issue(format!("retcd-voter-{n}"), sans, true)?;
        write(format!("voter-{n}.pem"), cert)?;
        write(format!("voter-{n}.key"), key)?;
    }
    tracing::info!(
        dir = %out.display(),
        cluster = %cluster,
        voters = ?voters,
        "net_control_certs_generated"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The label grammar is exact: one kind, one value, nothing after.
    #[test]
    fn a_label_uri_parses_only_in_its_exact_form() {
        assert_eq!(
            parse_label_uri("rdb://node/2"),
            Some(Label::Node(NodeId(2)))
        );
        assert_eq!(
            parse_label_uri("rdb://app/pilot-1"),
            Some(Label::App("pilot-1".to_owned()))
        );
        for bad in [
            "rdb://node/2/x",
            "rdb://node/",
            "rdb://node/two",
            "rdb://app/a b",
            "retcd://node/2",
            "rdb://peer/2",
        ] {
            assert_eq!(parse_label_uri(bad), None, "{bad}");
        }
        assert_eq!(
            parse_label_uri(&label_uri(&Label::Node(NodeId(3)))),
            Some(Label::Node(NodeId(3)))
        );
    }

    /// `ca.pem`, `client.pem` and `client.key` of `pair` in `dir`: the `--control-tls` layout.
    fn write_control(dir: &Path, pair: &config_testkit::CertPair) {
        std::fs::create_dir_all(dir).expect("a folder");
        std::fs::write(dir.join("ca.pem"), &pair.ca_pem).expect("ca.pem");
        std::fs::write(dir.join("client.pem"), &pair.cert_pem).expect("client.pem");
        std::fs::write(dir.join("client.key"), &pair.key_pem).expect("client.key");
    }

    /// #80 S2 SC5 (ADR-rdb-0024): `load_control` reads the cluster id and the client name from
    /// the client certificate's `retcd://<cluster>/client/<name>` SAN. A missing file names its
    /// path and the step that makes it. A certificate with no `retcd://` SAN (an rDB node's) is
    /// refused, and so is an rEtcd *node* certificate: the control link presents a client one.
    /// Unit.
    /// Cut: `control_identity` takes the first rEtcd identity whatever its kind (a node's
    /// cluster with its id as the name): the node-certificate case is accepted, red.
    #[test]
    fn s2_load_control_reads_the_cluster_from_the_client_certificate() {
        let cluster = ClusterId::from_bytes([0x5a; 16]);
        let fixture = config_testkit::TlsFixture::new(cluster, 80);
        let root = config_testkit::fs::temp_dir();

        let good = root.path().join("good");
        write_control(
            &good,
            &fixture.issue(config_testkit::CertProfile::client("rdb")),
        );
        let loaded = load_control(&good).expect("a client certificate loads");
        assert_eq!(loaded.cluster, cluster);
        assert_eq!(loaded.principal, "rdb");

        let missing = root.path().join("missing");
        write_control(
            &missing,
            &fixture.issue(config_testkit::CertProfile::client("rdb")),
        );
        std::fs::remove_file(missing.join("client.key")).expect("remove client.key");
        let refused = load_control(&missing).expect_err("no client.key");
        assert!(
            refused.contains(&missing.join("client.key").display().to_string())
                && refused.contains("--gen-certs")
                && refused.contains("--control-cluster"),
            "{refused}"
        );

        // An rDB node certificate: `rdb://node/1`, no `retcd://` SAN.
        let rdb_node = root.path().join("rdb-node");
        generate(&rdb_node, &[Label::Node(NodeId(1))]).expect("certs");
        std::fs::copy(rdb_node.join("node-1.pem"), rdb_node.join("client.pem")).expect("copy");
        std::fs::copy(rdb_node.join("node-1.key"), rdb_node.join("client.key")).expect("copy");
        let refused = load_control(&rdb_node).expect_err("an rDB node certificate");
        assert!(
            refused.contains("client.pem") && refused.contains("names no rEtcd client"),
            "{refused}"
        );

        // An rEtcd node certificate of the right cluster: a node, not a client.
        let retcd_node = root.path().join("retcd-node");
        write_control(
            &retcd_node,
            &fixture.issue(config_testkit::CertProfile::node(config_core::NodeId(1))),
        );
        let refused = load_control(&retcd_node).expect_err("an rEtcd node certificate");
        assert!(
            refused.contains("client.pem") && refused.contains("not a client"),
            "{refused}"
        );
    }

    /// The first certificate of the PEM file at `path`, as DER.
    fn der_of(path: &Path) -> Vec<u8> {
        let pem = std::fs::read(path).expect("a PEM file");
        let der = rustls_pemfile::certs(&mut &pem[..])
            .next()
            .expect("one certificate")
            .expect("parses");
        der.as_ref().to_vec()
    }

    /// Every DNS and IP SAN of `der`, as text.
    fn dns_and_ip_sans(der: &[u8]) -> Vec<String> {
        use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};
        let (_, cert) = X509Certificate::from_der(der).expect("parses");
        let san = cert
            .subject_alternative_name()
            .expect("a SAN extension")
            .expect("present");
        san.value
            .general_names
            .iter()
            .filter_map(|name| match name {
                GeneralName::DNSName(dns) => Some((*dns).to_owned()),
                GeneralName::IPAddress(bytes) => match bytes.len() {
                    4 => Some(
                        std::net::Ipv4Addr::from(<[u8; 4]>::try_from(*bytes).expect("4"))
                            .to_string(),
                    ),
                    16 => Some(
                        std::net::Ipv6Addr::from(<[u8; 16]>::try_from(*bytes).expect("16"))
                            .to_string(),
                    ),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }

    /// #80 S2 SC6 (ADR-rdb-0024): `generate_control` writes `control/` beside the development
    /// CA: a copy of `ca.pem`; a client certificate rEtcd reads as principal `rdb` of the
    /// cluster; and per voter a certificate whose node identity is `(cluster, n)`, whose DNS SAN
    /// is `peer_server_domain(cluster, n)` (what a hint dial pins), and whose voter host is a
    /// SAN too, as an IP or as a name (what a client dialling that address verifies). Unit.
    /// Cut: the voter's DNS SAN spelled by hand as `node-<n>.retcd`: red at the DNS SAN.
    #[test]
    fn s2_gen_certs_control_material_parses_as_retcd_identities() {
        let cluster = ClusterId::from_bytes([0x80; 16]);
        let root = config_testkit::fs::temp_dir();
        let dir = root.path();
        generate(dir, &[Label::Node(NodeId(1))]).expect("certs");
        let voters = [
            (1, "172.30.80.2".to_owned()),
            (2, "172.30.80.3".to_owned()),
            (3, "voter-3.example".to_owned()),
        ];
        generate_control(dir, cluster, &voters).expect("control certs");
        let control = dir.join(CONTROL_DIR);
        assert_eq!(
            std::fs::read(control.join("ca.pem")).expect("control/ca.pem"),
            std::fs::read(dir.join("ca.pem")).expect("ca.pem"),
            "control/ca.pem is the development CA"
        );
        let loaded = load_control(&control).expect("the control client loads");
        assert_eq!(
            (loaded.cluster, loaded.principal.as_str()),
            (cluster, "rdb")
        );
        let client = der_of(&control.join("client.pem"));
        let principal = config_grpc::tls::principal_from_certs(&[&client], cluster, false)
            .expect("rEtcd takes the client certificate");
        assert_eq!(principal.name, CONTROL_CLIENT);
        for (n, host) in &voters {
            let der = der_of(&control.join(format!("voter-{n}.pem")));
            assert!(control.join(format!("voter-{n}.key")).exists());
            assert_eq!(
                config_grpc::tls::node_identity_from_certs(&[&der]),
                Some((cluster, config_core::NodeId(*n))),
                "voter {n}'s node identity"
            );
            let sans = dns_and_ip_sans(&der);
            assert!(
                sans.contains(&config_grpc::peer_server_domain(
                    &cluster,
                    config_core::NodeId(*n)
                )),
                "voter {n}'s DNS SAN: {sans:?}"
            );
            assert!(sans.contains(host), "voter {n}'s host SAN: {sans:?}");
        }
    }
}
