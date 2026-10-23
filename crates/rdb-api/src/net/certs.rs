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

use std::path::{Path, PathBuf};

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
    let mut ca = CertificateParams::default();
    ca.distinguished_name = named("rdb-dev-ca");
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_cert = ca.self_signed(&ca_key).map_err(rcgen)?;
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
}
