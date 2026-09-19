//! Certificate material for this crate's **unit** tests.
//!
//! Deliberately not shared with `config-testkit`'s [`TlsFixture`]: that crate depends on this
//! one, so reaching for it here would be a cycle. Deliberately not shared with this crate's
//! own integration tests either — `tests/mtls.rs` and `tests/m4_watch_wire.rs` compile as
//! separate crates and cannot see a `#[cfg(test)]` module. This is the one copy that unit
//! tests use, and it is kept to the minimum they need: a CA and one leaf it signs.
//!
//! [`TlsFixture`]: https://docs.rs/config-testkit

use rcgen::{
    BasicConstraints, CertificateParams, DnType, Ia5String, IsCa, KeyPair, KeyUsagePurpose, SanType,
};

use crate::tls::MtlsConfig;

/// The DNS name the generated leaf claims. Unit tests never dial it, so any name will do as
/// long as there is one — rustls requires an end-entity certificate to name something.
const TEST_DNS: &str = "retcd.test";

/// A throwaway CA and one leaf signed by it.
pub(crate) struct TlsFixture {
    ca_pem: String,
    cert_pem: String,
    key_pem: String,
}

impl TlsFixture {
    /// Mint a fresh CA and a leaf, both valid.
    pub(crate) fn new() -> Self {
        let ca_key = KeyPair::generate().expect("ca key");
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "retcd unit-test ca");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca = ca_params.self_signed(&ca_key).expect("self-signed ca");

        let leaf_key = KeyPair::generate().expect("leaf key");
        let mut leaf_params =
            CertificateParams::new(vec![TEST_DNS.to_string()]).expect("leaf params");
        leaf_params
            .distinguished_name
            .push(DnType::CommonName, "retcd unit-test leaf");
        leaf_params.subject_alt_names.push(SanType::URI(
            Ia5String::try_from("retcd://00000000000000000000000000000000/node/1")
                .expect("ascii uri"),
        ));
        let leaf = leaf_params
            .signed_by(&leaf_key, &ca, &ca_key)
            .expect("ca signs leaf");

        Self {
            ca_pem: ca.pem(),
            cert_pem: leaf.pem(),
            key_pem: leaf_key.serialize_pem(),
        }
    }

    /// Serveable mutual-TLS material: the leaf as the identity, the CA as the trust anchor.
    pub(crate) fn server_material(&self) -> MtlsConfig {
        MtlsConfig::new(
            self.ca_pem.clone().into_bytes(),
            self.cert_pem.clone().into_bytes(),
            self.key_pem.clone().into_bytes(),
        )
    }
}
