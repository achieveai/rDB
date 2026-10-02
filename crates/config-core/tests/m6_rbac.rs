//! M6-01..M6-10, M6-17..M6-24, M6-39, M6-40 — signed policy documents and the converging
//! evaluator (spec §15.3, ADR-0027).
//!
//! Everything provable without a node lives here: signature verification and its four distinct
//! refusal reasons, version monotonicity and break-glass, and the set-containment property the
//! fail-closed intersection rests on. The rows' cluster halves — unready-for-clients, health
//! fields, watch termination, the `ReloadPolicy` RPC — are in `config-engine`, `config-grpc` and
//! `config-server`, because a refusal that is correct in the evaluator and wrong at the edge is
//! still a hole.
//!
//! The suite signs its own documents. That is the point: a fixture that could not produce a
//! *valid* signature could not prove that an invalid one is refused for the right reason.

use config_core::policy::{
    changed_prefixes, evaluate_converging, grant, signature_payload, verify_policy, Adoption,
    PolicyDocument, PolicyRejected, PolicySignature, PolicyState, SignedPolicy,
    SignedPolicyAuthorizer, POLICY_SIGNATURE_VERSION, REASON_NO_VALID_POLICY,
    REASON_POLICY_CONVERGING,
};
use config_core::{
    Action, Authorizer, ClusterId, Decision, Principal, PrincipalKind, VerifyingKey,
};
use ed25519_dalek::{Signer, SigningKey};
use proptest::prelude::*;

// ---------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------

/// A deterministic signing key. Seeds are fixed so a failure reproduces exactly.
fn signing_key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn trust(entries: &[(&str, &SigningKey)]) -> Vec<(String, VerifyingKey)> {
    entries
        .iter()
        .map(|(name, key)| ((*name).to_string(), key.verifying_key()))
        .collect()
}

/// The cluster this suite's node belongs to.
const THIS_CLUSTER: ClusterId = ClusterId::from_bytes([0xC1; 16]);
/// A different cluster, for the G-06 rows.
const OTHER_CLUSTER: ClusterId = ClusterId::from_bytes([0xC2; 16]);

/// [`verify_policy`] as this suite's node would call it.
///
/// Every pre-G-06 row is about signatures, versions and hashes, and none of them is about the
/// cluster — so they say so once, here, rather than repeating the argument. The rows that *are*
/// about the cluster call [`verify_policy`] directly with the cluster they mean.
fn verify(
    doc_bytes: &[u8],
    sig_bytes: &[u8],
    trust_keys: &[(String, VerifyingKey)],
) -> Result<SignedPolicy, PolicyRejected> {
    verify_policy(doc_bytes, sig_bytes, trust_keys, THIS_CLUSTER)
}

/// A document with one read+write grant per `(principal, prefix)` pair, naming no cluster.
///
/// Unscoped, because that is what every document signed before G-06 looks like, and because a
/// fixture that quietly scoped itself would stop the legacy rows below proving anything.
fn doc(version: u64, grants: &[(&str, &str)], admins: &[&str]) -> PolicyDocument {
    PolicyDocument {
        version,
        issued_unix_ms: 1_700_000_000_000,
        grants: grants
            .iter()
            .map(|(principal, prefix)| grant(principal, prefix, &[Action::Read, Action::Write]))
            .collect(),
        admins: admins.iter().map(|a| (*a).to_string()).collect(),
        cluster_id: None,
    }
}

/// The same document, issued for `cluster`.
fn doc_for(
    cluster: ClusterId,
    version: u64,
    grants: &[(&str, &str)],
    admins: &[&str],
) -> PolicyDocument {
    PolicyDocument {
        cluster_id: Some(cluster),
        ..doc(version, grants, admins)
    }
}

fn encode(document: &PolicyDocument) -> Vec<u8> {
    serde_json::to_vec(document).expect("a policy document serializes")
}

/// Sign `doc_bytes` honestly: the envelope commits to the bytes' own hash and version.
fn sign(doc_bytes: &[u8], key_name: &str, key: &SigningKey, version: u64) -> Vec<u8> {
    sign_claiming(
        doc_bytes,
        key_name,
        key,
        version,
        config_core::policy::document_hash(doc_bytes),
    )
}

/// Sign an arbitrary `(version, hash)` pair, which is what the tamper rows need: an attacker
/// controls the envelope's contents, not only the document's.
fn sign_claiming(
    doc_bytes: &[u8],
    key_name: &str,
    key: &SigningKey,
    version: u64,
    hash: [u8; 32],
) -> Vec<u8> {
    let _ = doc_bytes;
    let signature = key.sign(&signature_payload(&hash, version));
    PolicySignature {
        envelope_version: POLICY_SIGNATURE_VERSION,
        key_name: key_name.to_string(),
        version,
        hash,
        signature: signature.to_bytes().to_vec(),
    }
    .encode()
    .expect("an envelope encodes")
}

fn app() -> Principal {
    Principal::new("app", PrincipalKind::Certificate)
}

fn allowed(decision: &Decision) -> bool {
    decision.is_allowed()
}

fn deny_reason(decision: &Decision) -> String {
    match decision {
        Decision::Allow => panic!("expected a denial, got Allow"),
        Decision::Deny { reason } => reason.clone(),
    }
}

// ---------------------------------------------------------------------------------------
// §3.1 Load and verify — M6-01..M6-06
// ---------------------------------------------------------------------------------------

/// M6-01 (core half): a correctly signed document verifies, activates, and its grants take
/// effect — while a principal the document does not name is refused.
///
/// The positive control. Without it every refusal row below could pass on an implementation
/// that accepts nothing at all.
#[config_log::retcd_test]
fn m6_01_good_signature_loads_and_activates() {
    let ops = signing_key(1);
    let document = doc(7, &[("app", "/a/")], &["root"]);
    let bytes = encode(&document);
    let signature = sign(&bytes, "ops", &ops, 7);

    let signed = verify(&bytes, &signature, &trust(&[("ops", &ops)]))
        .expect("an honestly signed document verifies");
    assert_eq!(signed.document, document);
    assert_eq!(signed.hash, config_core::policy::document_hash(&bytes));
    assert_eq!(signed.bytes.as_ref(), bytes.as_slice());

    let authorizer = SignedPolicyAuthorizer::new(false);
    assert_eq!(
        authorizer
            .adopt(signed)
            .expect("a first adoption never rolls back"),
        Adoption::Adopted {
            from: None,
            to: 7,
            break_glass: false
        }
    );
    assert_eq!(authorizer.policy_version(), Some(7));
    assert!(allowed(&authorizer.authorize(
        &app(),
        Action::Write,
        b"/a/k"
    )));
    assert!(!allowed(&authorizer.authorize(
        &Principal::new("stranger", PrincipalKind::Certificate),
        Action::Read,
        b"/a/k"
    )));
}

/// M6-02 (core half): a structurally broken signature is `signature_invalid`, and an authorizer
/// that adopted nothing denies everything rather than falling open.
#[config_log::retcd_test]
fn m6_02_bad_signature_is_refused_and_denies_everything() {
    let ops = signing_key(1);
    let bytes = encode(&doc(7, &[("app", "/a/")], &[]));

    for broken in [
        vec![0u8; 8],
        b"not postcard at all, just prose".to_vec(),
        Vec::new(),
    ] {
        assert_eq!(
            verify(&bytes, &broken, &trust(&[("ops", &ops)])).unwrap_err(),
            PolicyRejected::SignatureInvalid
        );
    }

    // A well-formed envelope whose signature bytes are not a signature over anything.
    let mut forged = PolicySignature::decode(&sign(&bytes, "ops", &ops, 7)).expect("decodes");
    forged.signature = vec![0u8; 64];
    assert_eq!(
        verify(
            &bytes,
            &forged.encode().expect("encodes"),
            &trust(&[("ops", &ops)])
        )
        .unwrap_err(),
        PolicyRejected::SignatureInvalid
    );

    let authorizer = SignedPolicyAuthorizer::new(false);
    assert_eq!(authorizer.policy_version(), None);
    assert_eq!(
        deny_reason(&authorizer.authorize(&app(), Action::Read, b"/a/k")),
        REASON_NO_VALID_POLICY
    );
    assert_eq!(
        authorizer.state(Some(&PolicyRejected::SignatureInvalid)),
        PolicyState::NoValidPolicy {
            reason: "signature_invalid".to_string()
        }
    );
}

/// M6-03: a structurally valid signature by a key that is not in `authz.trust_keys` is
/// `untrusted_signer`, **distinct** from `signature_invalid`.
///
/// Conflating the two costs an operator the diagnosis: "you rotated the signing key and forgot
/// to distribute it" and "your file is corrupt" have nothing in common.
#[config_log::retcd_test]
fn m6_03_signature_by_an_untrusted_key_is_refused() {
    let ops = signing_key(1);
    let attacker = signing_key(9);
    let bytes = encode(&doc(7, &[("app", "/a/")], &[]));

    // Signed perfectly, by a key nobody configured, and naming itself honestly.
    let signature = sign(&bytes, "attacker", &attacker, 7);
    assert_eq!(
        verify(&bytes, &signature, &trust(&[("ops", &ops)])).unwrap_err(),
        PolicyRejected::UntrustedSigner
    );

    // Claiming a trusted key's *name* does not help: the bytes are still checked against the
    // configured key, so the refusal simply moves one step later.
    let impostor = sign(&bytes, "ops", &attacker, 7);
    assert_eq!(
        verify(&bytes, &impostor, &trust(&[("ops", &ops)])).unwrap_err(),
        PolicyRejected::SignatureInvalid
    );
}

/// M6-04: an edited document body is `hash_mismatch`, and the pre-edit grants do not apply
/// either — a refused document leaves nothing behind.
#[config_log::retcd_test]
fn m6_04_tampered_document_body_is_refused() {
    let ops = signing_key(1);
    let original = doc(7, &[("app", "/a/")], &[]);
    let bytes = encode(&original);
    let signature = sign(&bytes, "ops", &ops, 7);

    // One grant's prefix widened after signing; the version is untouched, so the only thing
    // that can catch this is the hash.
    let tampered = encode(&doc(7, &[("app", "/")], &[]));
    assert_ne!(tampered, bytes);
    assert_eq!(
        verify(&tampered, &signature, &trust(&[("ops", &ops)])).unwrap_err(),
        PolicyRejected::HashMismatch
    );

    let authorizer = SignedPolicyAuthorizer::new(false);
    assert_eq!(authorizer.policy_version(), None);
    assert!(!allowed(&authorizer.authorize(
        &app(),
        Action::Read,
        b"/a/k"
    )));
}

/// M6-05: the version is bound **into** the signature payload, and a mismatch between the signed
/// version and the body's is `version_binding`.
///
/// Two shapes, both of which a bare detached signature would misreport: a relabelled document,
/// and two validly-signed documents whose signature files were swapped.
#[config_log::retcd_test]
fn m6_05_version_is_bound_to_the_document_hash() {
    let ops = signing_key(1);
    let keys = trust(&[("ops", &ops)]);

    // Signed as version 9; the body says 7.
    let bytes = encode(&doc(7, &[("app", "/a/")], &[]));
    let relabelled = sign_claiming(
        &bytes,
        "ops",
        &ops,
        9,
        config_core::policy::document_hash(&bytes),
    );
    assert_eq!(
        verify(&bytes, &relabelled, &keys).unwrap_err(),
        PolicyRejected::VersionBinding
    );

    // Two honestly-signed documents; their signature files swapped.
    let v7 = encode(&doc(7, &[("app", "/a/")], &[]));
    let v9 = encode(&doc(9, &[("app", "/b/")], &[]));
    let sig7 = sign(&v7, "ops", &ops, 7);
    let sig9 = sign(&v9, "ops", &ops, 9);
    assert!(verify(&v7, &sig7, &keys).is_ok());
    assert!(verify(&v9, &sig9, &keys).is_ok());
    assert_eq!(
        verify(&v7, &sig9, &keys).unwrap_err(),
        PolicyRejected::VersionBinding
    );
    assert_eq!(
        verify(&v9, &sig7, &keys).unwrap_err(),
        PolicyRejected::VersionBinding
    );

    // The payload really is `hash ‖ version_le`: a signature over the hash alone does not
    // verify. Without this the two rows above could pass on an implementation that bound the
    // version only by restating it in the envelope.
    let hash = config_core::policy::document_hash(&v7);
    let mut naive = PolicySignature::decode(&sig7).expect("decodes");
    naive.signature = ops.sign(&hash).to_bytes().to_vec();
    assert_eq!(
        verify(&v7, &naive.encode().expect("encodes"), &keys).unwrap_err(),
        PolicyRejected::SignatureInvalid
    );
}

/// M6-06: `authz.trust_keys` is a **set**, so rotating the signing key is a configuration edit
/// rather than a flag day — and removing a key really does stop honouring its signatures.
#[config_log::retcd_test]
fn m6_06_trust_key_set_is_a_set_not_a_single_key() {
    let ops = signing_key(1);
    let ops_next = signing_key(2);
    let bytes = encode(&doc(7, &[("app", "/a/")], &[]));
    let signature = sign(&bytes, "ops-next", &ops_next, 7);

    let both = trust(&[("ops", &ops), ("ops-next", &ops_next)]);
    assert!(verify(&bytes, &signature, &both).is_ok());

    let only_ops = trust(&[("ops", &ops)]);
    assert_eq!(
        verify(&bytes, &signature, &only_ops).unwrap_err(),
        PolicyRejected::UntrustedSigner
    );
}

// ---------------------------------------------------------------------------------------
// Gap G-06 — which cluster a signed document was issued for
// ---------------------------------------------------------------------------------------

/// G-06: a document signed by a trusted key but issued for **another cluster** is refused, and
/// the refusal is not a version refusal.
///
/// The attack this closes needs no forged signature. One operations key trusted by two clusters
/// was enough: cluster B's document verified on cluster A, and because `adopt` only ever asked
/// "is this version higher?", a higher-versioned foreign document replaced A's grants and
/// admins with B's, cleanly and unlogged.
///
/// Both halves are asserted, and the second is the one that keeps the fix honest. It is not
/// enough that the document is refused — it has to be refused for a reason an operator can act
/// on. `rollback` and `version_binding` both say "re-issue it at a higher version", which is
/// the worst possible advice here.
///
/// Mutation check: deleting the cluster comparison in `verify_policy` turns the first assertion
/// into an `Ok`, which is the defect itself.
#[config_log::retcd_test]
fn g06_a_document_issued_for_another_cluster_is_refused() {
    let ops = signing_key(1);
    // Higher-versioned and wider than anything this node holds: under the defect, exactly the
    // document that would have taken over.
    let foreign = doc_for(OTHER_CLUSTER, 99, &[("app", "/")], &["intruder"]);
    let bytes = encode(&foreign);
    let signature = sign(&bytes, "ops", &ops, 99);
    let keys = trust(&[("ops", &ops)]);

    assert_eq!(
        verify_policy(&bytes, &signature, &keys, THIS_CLUSTER).unwrap_err(),
        PolicyRejected::ClusterMismatch {
            expected: THIS_CLUSTER,
            document: OTHER_CLUSTER,
        },
        "a validly signed document for another cluster must not verify here"
    );

    let reason = verify_policy(&bytes, &signature, &keys, THIS_CLUSTER)
        .unwrap_err()
        .reason();
    assert_eq!(reason, "cluster_mismatch");
    assert_ne!(
        reason,
        PolicyRejected::Rollback {
            active: 1,
            incoming: 1
        }
        .reason(),
        "a cluster refusal must not read as a version refusal: the operator actions differ"
    );
    assert_ne!(reason, PolicyRejected::VersionBinding.reason());

    // The same bytes on the cluster they were issued for verify, which is what makes the
    // refusal above a statement about scope rather than about the document being broken.
    verify_policy(&bytes, &signature, &keys, OTHER_CLUSTER)
        .expect("the document is valid on its own cluster");
}

/// G-06: a document that names no cluster at all still verifies and still adopts.
///
/// This is the compatibility half, and it is the reason the field could be added without a flag
/// day: the signature covers the document *bytes*, so a document written before the field
/// existed hashes exactly as it always did and verifies exactly as it always did. If this row
/// fails, every signed document in every deployment became invalid on upgrade.
///
/// The warning an operator gets about it is the daemon's to emit — it has the log — and is
/// asserted in `config-server`'s `an_unscoped_document_adopts_and_warns`. What is asserted here
/// is the fact that warning is derived from, which is that the document carries no cluster.
#[config_log::retcd_test]
fn g06_a_legacy_unscoped_document_still_verifies_and_adopts() {
    let ops = signing_key(1);
    let legacy = doc(7, &[("app", "/a/")], &["root"]);
    let bytes = encode(&legacy);
    let signature = sign(&bytes, "ops", &ops, 7);

    let signed = verify_policy(&bytes, &signature, &trust(&[("ops", &ops)]), THIS_CLUSTER)
        .expect("a document predating the cluster field still verifies");
    assert_eq!(
        signed.document.cluster_id, None,
        "this is the fact the daemon's warning is derived from"
    );

    let authorizer = SignedPolicyAuthorizer::new(false);
    assert!(matches!(
        authorizer.adopt(signed).expect("it adopts"),
        Adoption::Adopted { to: 7, .. }
    ));
    assert!(allowed(&authorizer.authorize(
        &app(),
        Action::Read,
        b"/a/k"
    )));
}

/// G-06: the cluster travels as the hex an operator reads, and a document that is not that hex
/// is a parse error rather than a silently unscoped document.
///
/// The second half is the one worth having. `Option<ClusterId>` with a lenient decoder would
/// turn a typo in the one field that scopes the document into "no scope at all" — a fail-open
/// on the exact field added to fail closed.
#[config_log::retcd_test]
fn g06_the_cluster_is_reviewable_hex_and_a_bad_one_is_refused() {
    let scoped = doc_for(THIS_CLUSTER, 7, &[("app", "/a/")], &[]);
    let text = String::from_utf8(encode(&scoped)).expect("json is utf-8");
    assert!(
        text.contains(&format!("\"cluster_id\":\"{THIS_CLUSTER}\"")),
        "an operator signs this file by hand and must be able to read the field: {text}"
    );
    assert_eq!(
        serde_json::from_str::<PolicyDocument>(&text).expect("it round-trips"),
        scoped
    );

    let mangled = text.replace(&THIS_CLUSTER.to_string(), "not-a-cluster-id");
    let error = serde_json::from_str::<PolicyDocument>(&mangled)
        .expect_err("a cluster id that is not 32 hex characters is not a cluster id");
    assert!(
        error.to_string().contains("invalid cluster id"),
        "the deserializer says which field is wrong: {error}"
    );
}

// ---------------------------------------------------------------------------------------
// Gap G-09 — the version floor outlives the process
// ---------------------------------------------------------------------------------------

/// G-09 (core half): with a seeded floor, a fresh authorizer refuses a **strictly older**
/// document, and says so with a reason distinct from an ordinary rollback.
///
/// The durable half — that the floor is actually read back off a disk — is
/// `config-server`'s `a_restart_refuses_a_document_below_the_persisted_floor`. This row is the
/// rule that one proves is wired up. The positive control that says where the rule stops is
/// `g09_a_restart_against_an_equal_floor_is_an_ordinary_load`, and it is not optional: the
/// first version of this row asserted that `version == floor` was *also* refused, which would
/// have made every signed-policy node refuse its own unchanged document on every restart.
///
/// Mutation check: deleting the `below_floor` branch in `adopt`'s no-active-document arm makes
/// every `unwrap_err` here an `Adopted`.
#[config_log::retcd_test]
fn g09_a_seeded_floor_refuses_an_older_document_at_the_first_adoption() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(false);
    authorizer.seed_version_floor(7);

    for version in [0, 5, 6] {
        assert_eq!(
            adopt(&authorizer, &doc(version, &[("app", "/")], &[]), &ops).unwrap_err(),
            PolicyRejected::RollbackFloor {
                floor: 7,
                incoming: version
            },
            "version {version} is below the floor"
        );
    }
    assert_eq!(
        authorizer.policy_version(),
        None,
        "a refused first adoption leaves the node with no policy, not with the refused one"
    );

    // And forward still works, or the fix would be a node that can never load a policy again.
    assert!(matches!(
        adopt(&authorizer, &doc(8, &[("app", "/a/")], &[]), &ops).expect("v8 is above the floor"),
        Adoption::Adopted {
            from: None,
            to: 8,
            ..
        }
    ));
    assert_eq!(
        authorizer.version_floor(),
        8,
        "the floor follows what is in force"
    );
}

/// G-09, the positive control: a restart that reloads the document it was already serving is an
/// ordinary load, not a rollback.
///
/// This is the most common event the floor sees — every healthy restart is exactly this — and
/// the first version of the floor refused it, because `below_floor` was `to <= floor`. The
/// effect was not a subtle one: a signed-policy node would have come up `NoValidPolicy` and
/// denied every client call after any restart, which is a worse outage than the rollback the
/// floor exists to prevent. Found by `e2e_46_daemon_break_glass_rollback_is_audited`, which
/// asserts `break_glass: false` on a restart's first adoption and therefore noticed that the
/// restart was being classified as a break-glass rollback.
///
/// Two things are asserted, and the second is the one that guards the audit trail: the document
/// adopts, and it adopts *without* `break_glass`. A restart that rolled nothing back must not
/// leave a line claiming an operator forced a rollback — a false entry in a security audit log
/// is worse than a missing one, because it is acted upon.
///
/// Mutation check: restoring `to <= floor` makes the first assertion fail with
/// `RollbackFloor { floor: 7, incoming: 7 }`.
#[config_log::retcd_test]
fn g09_a_restart_against_an_equal_floor_is_an_ordinary_load() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(false);
    authorizer.seed_version_floor(7);

    assert!(
        matches!(
            adopt(&authorizer, &doc(7, &[("app", "/a/")], &[]), &ops)
                .expect("a node reloads the version it was already serving"),
            Adoption::Adopted {
                from: None,
                to: 7,
                break_glass: false
            }
        ),
        "an ordinary restart is an ordinary load, with no break-glass in the audit line"
    );
    assert_eq!(authorizer.policy_version(), Some(7));
    assert_eq!(
        authorizer.version_floor(),
        7,
        "reloading the same version leaves the floor where it was"
    );
}

/// G-09: an unseeded authorizer behaves exactly as it did before the floor existed.
///
/// A zero floor means "nothing durable is known", not "version zero was served". Reading it the
/// other way would make a fresh node refuse a `version: 0` document, which is a behaviour change
/// nobody asked for and which would surface as a node that cannot bootstrap.
#[config_log::retcd_test]
fn g09_an_unseeded_floor_changes_nothing() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(false);
    assert_eq!(authorizer.version_floor(), 0);
    assert!(matches!(
        adopt(&authorizer, &doc(0, &[("app", "/a/")], &[]), &ops).expect("a fresh node adopts"),
        Adoption::Adopted {
            from: None,
            to: 0,
            ..
        }
    ));
}

/// G-09: break-glass crosses the durable floor, and moves it down to what it installed.
///
/// Both halves are the fix. Letting break-glass through is what keeps the escape hatch working
/// across a restart. Moving the floor *down* is what stops the next restart refusing the
/// document the operator deliberately installed — without it, break-glass would have to be left
/// set forever, which is precisely the state ADR-0027's audit trail exists to make temporary.
#[config_log::retcd_test]
fn g09_break_glass_crosses_the_floor_and_resets_it() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(true);
    authorizer.seed_version_floor(7);

    assert_eq!(
        adopt(&authorizer, &doc(3, &[("app", "/a/")], &[]), &ops).expect("break-glass crosses it"),
        Adoption::Adopted {
            from: None,
            to: 3,
            break_glass: true
        },
        "the audit trail must say break-glass was what permitted this"
    );
    assert_eq!(
        authorizer.version_floor(),
        3,
        "the floor follows the version in force, or the next restart refuses it again"
    );
}

// ---------------------------------------------------------------------------------------
// §3.2 Rollback, break-glass and audit — M6-07..M6-10
// ---------------------------------------------------------------------------------------

/// Adopt `version` into `authorizer`, asserting the document verifies first.
fn adopt(
    authorizer: &SignedPolicyAuthorizer,
    document: &PolicyDocument,
    key: &SigningKey,
) -> Result<Adoption, PolicyRejected> {
    let bytes = encode(document);
    let signature = sign(&bytes, "ops", key, document.version);
    let signed =
        verify(&bytes, &signature, &trust(&[("ops", key)])).expect("the fixture signs honestly");
    authorizer.adopt(signed)
}

/// M6-07: a validly-signed *older* document is refused with `rollback`, naming both versions,
/// and the active document keeps serving.
///
/// The second half is the one that matters operationally: a refused reload must never un-ready a
/// node that already holds a valid policy, or a bad deploy takes the cluster down.
#[config_log::retcd_test]
fn m6_07_rollback_is_refused_by_default() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(false);
    adopt(&authorizer, &doc(7, &[("app", "/a/")], &[]), &ops).expect("v7 adopts");

    assert_eq!(
        adopt(&authorizer, &doc(5, &[("app", "/")], &[]), &ops).unwrap_err(),
        PolicyRejected::Rollback {
            active: 7,
            incoming: 5
        }
    );
    assert_eq!(authorizer.policy_version(), Some(7));
    assert!(allowed(&authorizer.authorize(
        &app(),
        Action::Write,
        b"/a/k"
    )));
    // The refused document's wider grant never took effect.
    assert!(!allowed(&authorizer.authorize(
        &app(),
        Action::Write,
        b"/z/k"
    )));
}

/// M6-08: an equal version is a rollback unless the bytes are identical, in which case it is a
/// silent no-op.
///
/// Both halves are load-bearing. A deploy system that touches files unconditionally must not
/// reload forever, and a *different* document smuggled in at the same number must not be adopted.
#[config_log::retcd_test]
fn m6_08_equal_version_is_refused_unless_identical() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(false);
    let v7 = doc(7, &[("app", "/a/")], &[]);
    adopt(&authorizer, &v7, &ops).expect("v7 adopts");

    // Byte-identical re-write: no reload, no version change.
    assert_eq!(
        adopt(&authorizer, &v7, &ops).expect("an identical re-write is a no-op"),
        Adoption::Unchanged
    );
    assert_eq!(authorizer.policy_version(), Some(7));
    assert!(!authorizer.is_converging(), "a no-op starts no convergence");

    // A different document at the same number is a rollback.
    assert_eq!(
        adopt(&authorizer, &doc(7, &[("app", "/")], &[]), &ops).unwrap_err(),
        PolicyRejected::Rollback {
            active: 7,
            incoming: 7
        }
    );
    assert!(!allowed(&authorizer.authorize(
        &app(),
        Action::Write,
        b"/z/k"
    )));
}

/// M6-09 / M6-10: the break-glass flag permits a rollback, reports that it did, and — being
/// process-scoped — permits the *next* one too (OQ-57's pinned default).
///
/// The row exists to stop the semantics drifting silently: a one-shot flag and a process-scoped
/// flag are both defensible, an unstated choice is not.
#[config_log::retcd_test]
fn m6_09_and_m6_10_break_glass_allows_rollback_and_is_not_sticky() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(true);
    assert!(authorizer.break_glass_active());
    adopt(&authorizer, &doc(7, &[("app", "/a/")], &[]), &ops).expect("v7 adopts");

    assert_eq!(
        adopt(&authorizer, &doc(5, &[("app", "/a/")], &[]), &ops).expect("break glass permits"),
        Adoption::Adopted {
            from: Some(7),
            to: 5,
            break_glass: true
        }
    );
    assert_eq!(authorizer.policy_version(), Some(5));

    // Process-scoped: a second rollback below the *new* active version is also permitted, and
    // reports `break_glass` again so the caller audits each one separately.
    assert_eq!(
        adopt(&authorizer, &doc(3, &[("app", "/a/")], &[]), &ops).expect("still armed"),
        Adoption::Adopted {
            from: Some(5),
            to: 3,
            break_glass: true
        }
    );
    assert_eq!(authorizer.policy_version(), Some(3));

    // A forward move with the flag set is still an ordinary adoption, not a break-glass one.
    assert_eq!(
        adopt(&authorizer, &doc(11, &[("app", "/a/")], &[]), &ops).expect("forward"),
        Adoption::Adopted {
            from: Some(3),
            to: 11,
            break_glass: false
        }
    );
}

/// A failed reload keeps the previously active policy (M6-13's core half).
///
/// "Fails closed" means *does not adopt*, not *forgets what it had*: the opposite turns a typo in
/// a redeployed file into an outage.
#[config_log::retcd_test]
fn m6_13_a_failed_reload_keeps_the_active_policy() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(false);
    adopt(&authorizer, &doc(8, &[("app", "/a/")], &[]), &ops).expect("v8 adopts");

    // A tampered v9: it never reaches `adopt`, which is the point — verification is the gate.
    let bytes = encode(&doc(9, &[("app", "/a/")], &[]));
    let signature = sign(&bytes, "ops", &ops, 9);
    let tampered = encode(&doc(9, &[("app", "/")], &[]));
    assert_eq!(
        verify(&tampered, &signature, &trust(&[("ops", &ops)])).unwrap_err(),
        PolicyRejected::HashMismatch
    );
    assert_eq!(authorizer.policy_version(), Some(8));
    assert!(allowed(&authorizer.authorize(
        &app(),
        Action::Read,
        b"/a/k"
    )));

    // A subsequent valid v9 loads normally.
    adopt(&authorizer, &doc(9, &[("app", "/a/")], &[]), &ops).expect("v9 adopts");
    assert_eq!(authorizer.policy_version(), Some(9));
}

// ---------------------------------------------------------------------------------------
// §3.4 Convergence and the fail-closed intersection — M6-17..M6-24
// ---------------------------------------------------------------------------------------

/// The ADR's scenario shape: v8 **adds** `/new/`, **removes** `/old/`, and leaves `/same/` alone.
fn v7() -> PolicyDocument {
    doc(7, &[("app", "/old/"), ("app", "/same/")], &["root"])
}

fn v8() -> PolicyDocument {
    doc(8, &[("app", "/new/"), ("app", "/same/")], &["root"])
}

/// A node that has adopted v8 while the cluster has not converged.
fn converging_node() -> SignedPolicyAuthorizer {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(false);
    adopt(&authorizer, &v7(), &ops).expect("v7 adopts");
    adopt(&authorizer, &v8(), &ops).expect("v8 adopts");
    assert!(authorizer.is_converging());
    authorizer
}

/// M6-17: the single most important row in §3. A prefix only the **new** document grants is
/// denied while converging, with a reason distinguishable from an ordinary denial.
#[config_log::retcd_test]
fn m6_17_intersection_never_expands_early() {
    let node = converging_node();
    assert_eq!(
        deny_reason(&node.authorize(&app(), Action::Write, b"/new/k")),
        REASON_POLICY_CONVERGING
    );
    // The reason really is distinguishable: a principal with no grant anywhere gets a different
    // one, so a client cannot confuse "wait" with "never".
    assert_ne!(
        deny_reason(&node.authorize(
            &Principal::new("stranger", PrincipalKind::Certificate),
            Action::Write,
            b"/new/k"
        )),
        REASON_POLICY_CONVERGING
    );
}

/// M6-18: a removal takes effect at once on the node that has seen it. The spec accepts the cost
/// of narrowing early; it does not accept expanding early.
#[config_log::retcd_test]
fn m6_18_intersection_narrows_immediately() {
    let node = converging_node();
    let denial = node.authorize(&app(), Action::Write, b"/old/k");
    assert!(!allowed(&denial));
    assert_ne!(
        deny_reason(&denial),
        REASON_POLICY_CONVERGING,
        "a revoked grant is gone, not pending — a caller must not be told to wait for it"
    );
}

/// M6-19: the blast radius of a policy change is the changed prefixes only. A whole-document
/// intersection would deny far more than §15.3 asks for.
#[config_log::retcd_test]
fn m6_19_unchanged_prefixes_are_unaffected_during_convergence() {
    let node = converging_node();
    assert!(allowed(&node.authorize(&app(), Action::Read, b"/same/k")));
    assert!(allowed(&node.authorize(&app(), Action::Write, b"/same/k")));

    assert_eq!(
        changed_prefixes(&v7(), &v8()),
        vec![
            bytes::Bytes::from_static(b"/new/"),
            bytes::Bytes::from_static(b"/old/")
        ],
        "only the two edited prefixes are changed, sorted and deduped"
    );
}

/// M6-17, three hops: the baseline a converging node narrows against is the **oldest** document
/// it has not yet retired, not merely the one going out of force (C6R-07).
///
/// v7 -> v8 -> v9 inside one convergence window. `/grow/` is denied by v7, granted by v8, and
/// untouched between v8 and v9 — so `changed_prefixes(v8, v9)` is empty. A node that kept only v8
/// as its baseline would evaluate `/grow/` against v9 alone and allow it, while a voter still on
/// v7 denies it: exactly the early expansion §15.3 forbids. A 10 s poll against a 30 s
/// convergence objective makes two adoptions in one window an ordinary occurrence, not a corner.
#[config_log::retcd_test]
fn m6_17_chained_adoptions_narrow_against_the_oldest_unconverged_document() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(false);
    let v7 = doc(7, &[("app", "/same/")], &["root"]);
    let v8 = doc(8, &[("app", "/same/"), ("app", "/grow/")], &["root"]);
    // Identical grants to v8: only the version and the issue time move.
    let v9 = doc(9, &[("app", "/same/"), ("app", "/grow/")], &["root"]);
    assert!(
        changed_prefixes(&v8, &v9).is_empty(),
        "the second hop must change no prefix, or the row proves nothing about the baseline"
    );

    adopt(&authorizer, &v7, &ops).expect("v7 adopts");
    adopt(&authorizer, &v8, &ops).expect("v8 adopts");
    adopt(&authorizer, &v9, &ops).expect("v9 adopts");

    assert!(authorizer.is_converging(), "no voter has reported v9 yet");
    assert_eq!(
        deny_reason(&authorizer.authorize(&app(), Action::Write, b"/grow/k")),
        REASON_POLICY_CONVERGING,
        "a grant introduced by v8 must not take effect before the cluster has left v7"
    );
    // The unchanged prefix is untouched throughout: the baseline widens the blast radius to
    // what actually changed since v7, not to the whole document.
    assert!(allowed(&authorizer.authorize(
        &app(),
        Action::Read,
        b"/same/k"
    )));

    assert!(
        authorizer.note_cluster_min_version(Some(9)),
        "the last voter's report completes convergence"
    );
    assert!(
        allowed(&authorizer.authorize(&app(), Action::Write, b"/grow/k")),
        "once every voter is on v9 the new grant takes effect"
    );
}

/// M6-21: convergence completes when the last voter reports, exactly once, and the narrowing
/// ends — `/new/` opens and `/old/` stays closed.
#[config_log::retcd_test]
fn m6_21_convergence_completes_when_the_last_voter_reports() {
    let node = converging_node();
    assert!(
        !node.note_cluster_min_version(Some(7)),
        "a voter still on v7 does not complete convergence"
    );
    assert!(node.is_converging());

    assert!(
        node.note_cluster_min_version(Some(8)),
        "the transition into convergence is reported exactly once"
    );
    assert!(
        !node.note_cluster_min_version(Some(8)),
        "and not again, so the caller logs one policy_converged line"
    );
    assert!(!node.is_converging());
    assert_eq!(node.state(None), PolicyState::Active { version: 8 });

    assert!(allowed(&node.authorize(&app(), Action::Write, b"/new/k")));
    assert!(!allowed(&node.authorize(&app(), Action::Write, b"/old/k")));
}

/// M6-22: an **unknown** voter version counts as lagging, never as "probably fine". Failing open
/// on absence is the exact bug §15.3's clause exists to prevent.
#[config_log::retcd_test]
fn m6_22_unknown_voter_version_counts_as_lagging() {
    let node = converging_node();
    assert!(!node.note_cluster_min_version(None));
    assert!(node.is_converging());
    assert_eq!(node.state(None), PolicyState::Converging { from: 7, to: 8 });
    assert_eq!(
        deny_reason(&node.authorize(&app(), Action::Write, b"/new/k")),
        REASON_POLICY_CONVERGING
    );
    // §19.12: the cluster keeps serving unchanged prefixes throughout. No unbounded block.
    assert!(allowed(&node.authorize(&app(), Action::Write, b"/same/k")));
}

/// M6-23: a forged convergence claim can only end the narrowing **early**. It can never grant
/// access that neither document grants — that is the honest statement of the residual risk.
#[config_log::retcd_test]
fn m6_23_gossip_cannot_be_used_to_expand_access() {
    let node = converging_node();
    // The forgery: a hint claiming every voter is already on v8 when one is not.
    assert!(node.note_cluster_min_version(Some(8)));

    // The narrowing ended early — that is the damage, and it is a convergence cost.
    assert!(allowed(&node.authorize(&app(), Action::Write, b"/new/k")));

    // The containment property: every allowed key still lies inside `allowed(v7) ∪ allowed(v8)`,
    // and no key outside both is ever permitted.
    for key in [
        b"/old/k".as_slice(),
        b"/new/k".as_slice(),
        b"/same/k".as_slice(),
        b"/elsewhere/k".as_slice(),
        b"/".as_slice(),
    ] {
        for action in [Action::Read, Action::Write] {
            if allowed(&node.authorize(&app(), action, key)) {
                let in_old = evaluate_converging(&v7(), &v7(), "app", action, key).is_allowed();
                let in_new = evaluate_converging(&v8(), &v8(), "app", action, key).is_allowed();
                assert!(
                    in_old || in_new,
                    "a forged hint permitted {key:?} for {action:?}, which neither document grants"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------------------
// M6-24 — the set-containment property over generated documents
// ---------------------------------------------------------------------------------------

/// Prefixes and principals are drawn from small closed sets on purpose: the interesting
/// structure is *overlap* between grants, and wide alphabets would make an overlapping pair
/// astronomically unlikely.
const PREFIXES: [&str; 6] = ["/", "/a/", "/a/b/", "/a/bc/", "/b/", "/c/"];
const PRINCIPALS: [&str; 3] = ["app", "ops", "svc"];

fn action_strategy() -> impl Strategy<Value = Action> {
    prop_oneof![Just(Action::Read), Just(Action::Write)]
}

fn document_strategy(version: u64) -> impl Strategy<Value = PolicyDocument> {
    proptest::collection::vec(
        (
            0..PRINCIPALS.len(),
            0..PREFIXES.len(),
            proptest::collection::vec(action_strategy(), 0..=2),
        ),
        0..6,
    )
    .prop_map(move |raw| PolicyDocument {
        version,
        issued_unix_ms: 0,
        grants: raw
            .into_iter()
            .map(|(principal, prefix, access)| {
                grant(PRINCIPALS[principal], PREFIXES[prefix], &access)
            })
            .collect(),
        admins: Vec::new(),
        cluster_id: None,
    })
}

/// M6-24 (TA-56.3): over ≥ 500 generated `(old, new)` pairs and random `(principal, key,
/// action)`, `evaluate_converging` allows a triple **only if** both documents allow it on a
/// changed-prefix key, and equals the new document exactly on an unchanged-prefix key.
///
/// A hand-written table of four cases does not establish a set-containment claim; this does.
#[config_log::retcd_test]
fn m6_24_intersection_is_a_subset_property_over_generated_documents() {
    let config = ProptestConfig {
        cases: 500,
        ..ProptestConfig::default()
    };
    proptest!(config, |(
        old in document_strategy(7),
        new in document_strategy(8),
        principal in 0..PRINCIPALS.len(),
        prefix in 0..PREFIXES.len(),
        suffix in proptest::collection::vec(any::<u8>(), 0..3),
        action in action_strategy(),
    )| {
        let principal = PRINCIPALS[principal];
        let mut key = PREFIXES[prefix].as_bytes().to_vec();
        key.extend_from_slice(&suffix);

        let changed = changed_prefixes(&old, &new);
        let decision = evaluate_converging(&old, &new, principal, action, &key);

        let in_old = evaluate_converging(&old, &old, principal, action, &key).is_allowed();
        let in_new = evaluate_converging(&new, &new, principal, action, &key).is_allowed();

        if config_core::policy::touches_changed_prefix(&changed, &key) {
            prop_assert_eq!(
                decision.is_allowed(),
                in_old && in_new,
                "changed prefix: the decision must be exactly the intersection"
            );
            if !decision.is_allowed() && in_new && !in_old {
                prop_assert_eq!(
                    match &decision { Decision::Deny { reason } => reason.as_str(), _ => "" },
                    REASON_POLICY_CONVERGING,
                    "a denial the new document would grant must be typed as converging"
                );
            }
        } else {
            prop_assert_eq!(
                decision.is_allowed(),
                in_new,
                "unchanged prefix: the new document alone decides"
            );
        }

        // The containment claim itself, stated without reference to the branch above.
        prop_assert!(
            !decision.is_allowed() || in_old || in_new,
            "the evaluator must never allow what neither document allows"
        );
    });
}

// ---------------------------------------------------------------------------------------
// §3.7 The embedded principal and the admin set — M6-39, M6-40
// ---------------------------------------------------------------------------------------

/// M6-39 (core half): the signed model keeps ADR-0012's non-forgeable principal. An unverified
/// principal never matches a grant by name, however exactly the names coincide.
#[config_log::retcd_test]
fn m6_39_embedded_client_principal_is_non_forgeable_under_signed_policy() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(false);
    adopt(&authorizer, &doc(7, &[("app", "/a/")], &[]), &ops).expect("v7 adopts");

    let embedded = Principal::new("app", PrincipalKind::Embedded);
    assert!(allowed(&authorizer.authorize(
        &embedded,
        Action::Write,
        b"/a/k"
    )));

    // Same name, unverified kind: refused, and for the identity reason rather than the grant
    // reason, so the audit line says which check fired.
    let forged = Principal::new("app", PrincipalKind::Development);
    let reason = deny_reason(&authorizer.authorize(&forged, Action::Write, b"/a/k"));
    assert!(
        reason.contains("unverified kind"),
        "expected the identity refusal, got {reason:?}"
    );
    assert!(!allowed(&authorizer.authorize(
        &Principal::development(),
        Action::Read,
        b"/a/k"
    )));
}

/// M6-40 (core half): the admin set comes from the signed document and from nowhere else.
///
/// `admin_set()` returning `Some` is the signal the admin plane uses to ignore its configured
/// list; a model without a signed document returns `None` and the M3/M5 behaviour applies.
#[config_log::retcd_test]
fn m6_40_admin_set_comes_only_from_the_signed_document() {
    let ops = signing_key(1);
    let authorizer = SignedPolicyAuthorizer::new(false);
    assert_eq!(
        authorizer.admin_set(),
        None,
        "with no document there is no signed admin set to enforce"
    );

    adopt(&authorizer, &doc(7, &[("app", "/a/")], &["root"]), &ops).expect("v7 adopts");
    assert_eq!(authorizer.admin_set(), Some(vec!["root".to_string()]));

    let document = authorizer.active_document().expect("a document is active");
    assert!(document.is_admin("root"));
    assert!(
        !document.is_admin("app"),
        "a data-plane grant is not an admin grant"
    );

    // The static model carries no admin set at all, which is what keeps M3's configured list in
    // force there.
    assert_eq!(config_core::StaticAllowlist::default().admin_set(), None);
    assert_eq!(
        config_core::StaticAllowlist::default().policy_version(),
        None
    );
}
