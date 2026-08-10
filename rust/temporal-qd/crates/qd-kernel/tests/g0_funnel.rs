//! Small, hermetic end-to-end coverage for the native post-construction G0
//! transaction.  The rich entry is the checked-in Python projection fixture;
//! this test deliberately supplies no proposed/selected candidate arrays to
//! the request and proves a sealed receipt can adopt after source removal.

use std::{
    fs,
    io::Read,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine, engine::general_purpose::STANDARD};
use flate2::read::GzDecoder;
use serde_json::{Map, Value};
use temporal_qd_contract::{canonical_json, canonical_sha256};
use temporal_qd_kernel::{
    g0_funnel::{
        G0_FUNNEL_RECEIPT_PATH, G0FunnelOutcome, G0FunnelRequest, MAX_G0_ADMISSION_THREAD_CAP,
        finalize_g0,
    },
    journal::{FinalNewline, JournalError, ProposalJournal, set_g0_test_crash_after_artifact},
    publication::PublicationPolicy,
};

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<Map<_, _>>(),
    )
}

fn sha_token(token: &str) -> String {
    canonical_sha256(&Value::String(token.to_owned())).expect("canonical fixture token")
}

fn replace_self_hash(value: &mut Value, field: &str) {
    let fields = value.as_object_mut().expect("self-hashed fixture object");
    fields.remove(field);
    let identity = canonical_sha256(value).expect("canonical fixture identity");
    value
        .as_object_mut()
        .expect("self-hashed fixture object")
        .insert(field.to_owned(), Value::String(identity));
}

fn one_immigrant_config() -> Value {
    let mut allocation = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_reproduction_allocation_v2".to_owned()),
        ),
        ("targetAcceptedCandidates", Value::from(1_u64)),
        ("desiredAcceptedOffspringCount", Value::from(0_u64)),
        ("desiredAcceptedImmigrantCount", Value::from(1_u64)),
    ]);
    replace_self_hash(&mut allocation, "allocationSha256");
    let mut config = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_pair_generation_v2".to_owned()),
        ),
        ("generationIndex", Value::from(1_u64)),
        ("targetUniqueCandidates", Value::from(1_u64)),
        ("maxProposalAttempts", Value::from(1_u64)),
        ("reproductionAllocation", allocation),
    ]);
    replace_self_hash(&mut config, "configSha256");
    config
}

fn one_immigrant_config_with_max_proposal_attempts(max_proposal_attempts: u64) -> Value {
    let mut config = one_immigrant_config();
    config
        .as_object_mut()
        .expect("fixture config object")
        .insert(
            "maxProposalAttempts".to_owned(),
            Value::from(max_proposal_attempts),
        );
    replace_self_hash(&mut config, "configSha256");
    config
}

fn one_immigrant_v1_config() -> Value {
    let mut allocation = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_reproduction_allocation_v1".to_owned()),
        ),
        ("targetEvaluatedCandidates", Value::from(1_u64)),
        ("desiredEvaluatedOffspringCount", Value::from(0_u64)),
        ("desiredEvaluatedImmigrantCount", Value::from(1_u64)),
    ]);
    replace_self_hash(&mut allocation, "allocationSha256");
    let mut config = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_pair_generation_v2".to_owned()),
        ),
        ("generationIndex", Value::from(1_u64)),
        ("targetUniqueCandidates", Value::from(1_u64)),
        ("maxProposalAttempts", Value::from(1_u64)),
        ("reproductionAllocation", allocation),
    ]);
    replace_self_hash(&mut config, "configSha256");
    config
}

fn temporary_root() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    std::env::temp_dir().join(format!("temporal-qd-g0-funnel-{nonce}"))
}

#[cfg(unix)]
fn create_file_alias(target: &Path, alias: &Path) -> bool {
    std::os::unix::fs::symlink(target, alias).is_ok()
}

#[cfg(windows)]
fn create_file_alias(target: &Path, alias: &Path) -> bool {
    std::os::windows::fs::symlink_file(target, alias).is_ok()
}

#[cfg(unix)]
fn create_directory_alias(target: &Path, alias: &Path) -> bool {
    std::os::unix::fs::symlink(target, alias).is_ok()
}

#[cfg(windows)]
fn create_directory_alias(target: &Path, alias: &Path) -> bool {
    if std::os::windows::fs::symlink_dir(target, alias).is_ok() {
        return true;
    }
    // A junction is also a Windows reparse point and does not require the
    // symbolic-link privilege on the Windows versions that support it.
    let command = format!(r#"mklink /J "{}" "{}""#, alias.display(), target.display());
    std::process::Command::new("cmd")
        .args(["/d", "/s", "/c", &command])
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(unix)]
fn remove_directory_alias(alias: &Path) {
    fs::remove_file(alias).expect("remove directory symlink");
}

#[cfg(windows)]
fn remove_directory_alias(alias: &Path) {
    fs::remove_dir(alias).expect("remove directory symlink or junction");
}

fn compressed_json_fixture(encoded: &str, label: &str) -> Value {
    let encoded = encoded.split_whitespace().collect::<String>();
    let compressed = STANDARD
        .decode(encoded)
        .unwrap_or_else(|error| panic!("decode {label}: {error}"));
    let mut decoder = GzDecoder::new(compressed.as_slice());
    let mut json = Vec::new();
    decoder
        .read_to_end(&mut json)
        .unwrap_or_else(|error| panic!("decompress {label}: {error}"));
    serde_json::from_slice(&json).unwrap_or_else(|error| panic!("parse {label}: {error}"))
}

fn v5_evolvable_golden() -> Value {
    compressed_json_fixture(
        include_str!("fixtures/g0_v5_evolvable_journal_golden.json.gz.b64"),
        "synthetic v5 evolvable golden",
    )
}

fn actual_v5_evolvable_golden() -> Value {
    compressed_json_fixture(
        include_str!("fixtures/g0_v5_actual_journal_golden.json.gz.b64"),
        "actual preserved v5 evolvable golden",
    )
}

fn rehydrate_actual_v5_entry(fixture: &Value) -> Value {
    let pair = fixture.get("pair").expect("actual golden pair").clone();
    let mut proposal = fixture
        .get("proposal")
        .and_then(Value::as_object)
        .expect("actual golden proposal")
        .clone();
    proposal.insert("factoryPair".to_owned(), pair.clone());
    let proposal = Value::Object(proposal);
    let mut candidate = fixture
        .get("candidate")
        .and_then(Value::as_object)
        .expect("actual golden candidate")
        .clone();
    candidate.insert("bidirectionalGenome".to_owned(), pair.clone());
    candidate.insert("pairProposal".to_owned(), proposal.clone());
    candidate.insert(
        "sourceProfile".to_owned(),
        pair.get("profile")
            .expect("actual golden pair profile")
            .clone(),
    );
    let mut entry = fixture
        .get("entrySurface")
        .and_then(Value::as_object)
        .expect("actual golden entry surface")
        .clone();
    entry.insert("candidate".to_owned(), Value::Object(candidate));
    entry.insert("proposal".to_owned(), proposal);
    entry.insert(
        "entrySha256".to_owned(),
        fixture
            .get("entrySha256")
            .expect("actual golden entry hash")
            .clone(),
    );
    Value::Object(entry)
}

fn write_bound_identity_ledger(
    root: &std::path::Path,
    semantic_sha256: &str,
    candidate_identity_sha256: &str,
) -> (std::path::PathBuf, Value) {
    let identity_policy = object([("fixture", Value::String("identity-policy".to_owned()))]);
    let policy_sha = sha_token("identity ledger policy");
    let mut ledger = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_identity_ledger_v3".to_owned()),
        ),
        (
            "qdVersion",
            Value::String("temporal_qd_evolution_v3".to_owned()),
        ),
        ("policyName", Value::String("bounded-v5-ledger".to_owned())),
        ("policySha256", Value::String(policy_sha.clone())),
        ("identityPolicy", identity_policy.clone()),
        (
            "records",
            Value::Array(vec![object([
                (
                    "candidateIdentitySha256",
                    Value::String(candidate_identity_sha256.to_owned()),
                ),
                ("programSha256", Value::String(sha_token("ledger program"))),
                (
                    "sourceProfileSha256",
                    Value::String(sha_token("ledger source profile")),
                ),
                (
                    "profileSnapshotSha256",
                    Value::String(sha_token("ledger profile snapshot")),
                ),
                (
                    "canonicalEvidenceIdentitySha256",
                    Value::String(sha_token("ledger canonical evidence")),
                ),
            ])]),
        ),
        (
            "uniqueCounts",
            object([
                ("candidateIdentity", Value::from(1_u64)),
                ("program", Value::from(1_u64)),
                ("sourceProfile", Value::from(1_u64)),
                ("profileSnapshot", Value::from(1_u64)),
                ("canonicalEvidence", Value::from(1_u64)),
            ]),
        ),
        (
            "duplicateCounters",
            object([
                ("candidateIdentity", Value::from(0_u64)),
                ("program", Value::from(0_u64)),
                ("sourceProfile", Value::from(0_u64)),
                ("profileSnapshot", Value::from(0_u64)),
                ("canonicalEvidence", Value::from(0_u64)),
                ("programDifferentEvidenceAllowed", Value::from(0_u64)),
            ]),
        ),
        (
            "proposalSlotCounters",
            object([
                ("proposalsObserved", Value::from(1_u64)),
                ("acceptedUniqueProposalSlots", Value::from(1_u64)),
                ("duplicateRejections", Value::from(0_u64)),
            ]),
        ),
        (
            "pairExecutableSemantics",
            Value::Array(vec![object([
                (
                    "schemaVersion",
                    Value::String("temporal_qd_pair_executable_semantic_record_v1".to_owned()),
                ),
                (
                    "pairGenomeSemanticSha256",
                    Value::String(semantic_sha256.to_owned()),
                ),
                (
                    "candidateIdentitySha256",
                    Value::String(candidate_identity_sha256.to_owned()),
                ),
            ])]),
        ),
        (
            "pairExecutableSemanticDuplicateRejections",
            Value::from(0_u64),
        ),
    ]);
    replace_self_hash(&mut ledger, "ledgerSha256");
    let path = root.join("identity-ledger.json");
    fs::write(
        &path,
        serde_json::to_vec(&ledger).expect("encode identity ledger"),
    )
    .expect("write identity ledger");
    let binding = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_native_g0_identity_ledger_binding_v1".to_owned()),
        ),
        (
            "ledgerPath",
            Value::String(path.to_string_lossy().into_owned()),
        ),
        ("policyName", Value::String("bounded-v5-ledger".to_owned())),
        ("policySha256", Value::String(policy_sha)),
        ("identityPolicy", identity_policy.clone()),
        (
            "identityPolicySha256",
            Value::String(canonical_sha256(&identity_policy).expect("identity policy hash")),
        ),
    ]);
    (path, binding)
}

fn fixture_accepted_entry(
    config_sha256: &str,
    operator_implementation_sha256: &str,
    proposal_ordinal: u64,
) -> Value {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/g0_python_rich_projection.json"))
            .expect("rich Python projection fixture");
    let mut entry = fixture.get("entry").expect("fixture entry").clone();
    let fields = entry.as_object_mut().expect("fixture entry object");
    fields.insert(
        "configSha256".to_owned(),
        Value::String(config_sha256.to_owned()),
    );
    fields.insert("generationIndex".to_owned(), Value::from(1_u64));
    fields.insert("proposalOrdinal".to_owned(), Value::from(proposal_ordinal));
    fields.insert(
        "operatorImplementationSha256".to_owned(),
        Value::String(operator_implementation_sha256.to_owned()),
    );
    fields
        .get_mut("candidate")
        .and_then(Value::as_object_mut)
        .expect("fixture candidate")
        .insert("generationIndex".to_owned(), Value::from(1_u64));
    replace_self_hash(&mut entry, "entrySha256");
    entry
}

fn materialized_rejected_entry(accepted: &Value, proposal_ordinal: u64) -> Value {
    let mut rejected = accepted.clone();
    let fields = rejected
        .as_object_mut()
        .expect("accepted fixture entry object");
    fields.remove("candidate");
    fields.remove("funnelCandidate");
    fields.insert("proposalOrdinal".to_owned(), Value::from(proposal_ordinal));
    fields.insert(
        "disposition".to_owned(),
        Value::String("duplicate_pair_genome".to_owned()),
    );
    replace_self_hash(&mut rejected, "entrySha256");
    rejected
}

fn bare_single_entry_request(root: &std::path::Path) -> G0FunnelRequest {
    let config = one_immigrant_config();
    let config_sha256 = config
        .get("configSha256")
        .and_then(Value::as_str)
        .expect("fixture config identity")
        .to_owned();
    let operator_implementation_identity = object([]);
    G0FunnelRequest {
        output_root: root.to_path_buf(),
        final_newline: FinalNewline::Lf,
        request_sha256: sha_token("small g0 funnel request"),
        authority_sha256: sha_token("small g0 funnel authority"),
        execution_authority: Value::Null,
        config,
        config_sha256,
        generation_index: 1,
        construction_pool_size: 1,
        evaluation_population_size: 1,
        max_proposal_attempts: 1,
        admission_thread_cap: 1,
        publication_policy: PublicationPolicy {
            qd_version: "temporal_qd_evolution_v3".to_owned(),
            policy_name: "hermetic_g0_funnel_test".to_owned(),
            policy_sha256: sha_token("small g0 publication policy"),
            pair_policy: object([]),
            operator_implementation_identity,
            predeclared_evidence_context_sha256: None,
            archive_policy_authority: None,
        },
        identity_ledger: None,
        global_identity_ledger: None,
        audit: false,
    }
}

fn single_entry_request(root: &std::path::Path) -> G0FunnelRequest {
    let request = bare_single_entry_request(root);
    let store = ProposalJournal::open(root, FinalNewline::Lf).expect("open fixture journal");
    let operator_implementation_sha256 =
        canonical_sha256(&request.publication_policy.operator_implementation_identity)
            .expect("operator identity");
    store
        .write_canonical_once(std::path::Path::new("pair-config.json"), &request.config)
        .expect("persist pair config");
    store
        .write_public_entry(
            0,
            &fixture_accepted_entry(&request.config_sha256, &operator_implementation_sha256, 0),
        )
        .expect("persist rich entry");
    request
}

#[test]
fn g0_admission_thread_cap_is_bounded_before_any_source_read() {
    let root = temporary_root();
    let mut request = bare_single_entry_request(&root);
    request.admission_thread_cap = 0;
    assert!(
        finalize_g0(&request)
            .expect_err("zero G0 admission workers must fail closed")
            .to_string()
            .contains("widths or proposal ceiling")
    );
    request.admission_thread_cap = MAX_G0_ADMISSION_THREAD_CAP + 1;
    assert!(
        finalize_g0(&request)
            .expect_err("over-cap G0 admission workers must fail closed")
            .to_string()
            .contains("widths or proposal ceiling")
    );
    if root.exists() {
        fs::remove_dir_all(root).expect("remove cap validation fixture");
    }
}

#[test]
fn journal_write_once_rejects_root_and_exact_byte_final_aliases() {
    let root_target = temporary_root();
    fs::create_dir_all(&root_target).expect("create real root target");
    let root_alias = temporary_root();
    assert!(
        create_directory_alias(&root_target, &root_alias),
        "test host must permit a root directory symlink or junction fixture"
    );
    let error = ProposalJournal::open(&root_alias, FinalNewline::Lf)
        .expect_err("fresh journal root alias must fail closed");
    assert!(
        error.to_string().contains("symlink") || error.to_string().contains("reparse"),
        "unexpected root alias error: {error}"
    );
    remove_directory_alias(&root_alias);
    fs::remove_dir_all(&root_target).expect("remove root target");

    let root = temporary_root();
    let store = ProposalJournal::open(&root, FinalNewline::Lf).expect("open fresh journal");
    let value = object([("fixture", Value::String("exact-byte-alias".to_owned()))]);
    let baseline = temporary_root();
    let baseline_store =
        ProposalJournal::open(&baseline, FinalNewline::Lf).expect("open baseline journal");
    baseline_store
        .write_canonical_once(Path::new("population.json"), &value)
        .expect("write baseline population");
    let expected = fs::read(baseline.join("population.json")).expect("read baseline population");
    let outside = temporary_root();
    fs::create_dir_all(&outside).expect("create outside target directory");
    let victim = outside.join("population.json");
    fs::write(&victim, &expected).expect("seed exact outside bytes");
    let alias = root.join("population.json");
    assert!(
        create_file_alias(&victim, &alias),
        "test host must permit a final-component file symlink/reparse fixture"
    );
    let error = store
        .write_canonical_once_streaming_rewritable(
            Path::new("population.json"),
            FinalNewline::Lf,
            |writer| {
                writer
                    .write_all(
                        canonical_json(&value)
                            .expect("canonical alias fixture")
                            .as_bytes(),
                    )
                    .map_err(JournalError::Io)
            },
        )
        .expect_err("exact-byte final symlink/reparse must not be adopted");
    assert!(
        error.to_string().contains("symlink") || error.to_string().contains("reparse"),
        "unexpected final alias error: {error}"
    );
    assert_eq!(
        fs::read(&victim).expect("read untouched outside target"),
        expected,
        "write-once publisher followed an exact-byte final alias"
    );
    fs::remove_file(&alias).expect("remove final alias");
    fs::remove_dir_all(&root).expect("remove fresh journal");
    fs::remove_dir_all(&baseline).expect("remove baseline journal");
    fs::remove_dir_all(&outside).expect("remove outside target directory");
}

#[test]
fn receiptless_g0_prefix_rejects_parent_alias_before_resuming() {
    let root = temporary_root();
    let request = single_entry_request(&root);
    set_g0_test_crash_after_artifact(Some("g0-bootstrap/accepted-pool.json"));
    let error = finalize_g0(&request).expect_err("inject receiptless public prefix");
    set_g0_test_crash_after_artifact(None);
    assert!(
        error.to_string().contains("injected G0 crash"),
        "receiptless-prefix setup did not reach the durable-artifact failpoint: {error}"
    );
    assert!(!root.join(G0_FUNNEL_RECEIPT_PATH).exists());

    let prefix = root.join("g0-bootstrap");
    let outside = temporary_root();
    fs::rename(&prefix, &outside).expect("move exact prefix outside root");
    assert!(
        create_directory_alias(&outside, &prefix),
        "test host must permit a parent directory symlink or junction fixture"
    );
    let error = finalize_g0(&request)
        .expect_err("receiptless prefix through parent symlink/junction must fail closed");
    assert!(
        error.to_string().contains("symlink") || error.to_string().contains("reparse"),
        "unexpected parent alias error: {error}"
    );
    assert!(
        !root.join(G0_FUNNEL_RECEIPT_PATH).exists(),
        "aliased receiptless prefix must not publish a sealed receipt"
    );
    assert!(
        outside.join("accepted-pool.json").is_file(),
        "parent alias test lost its exact pre-crash prefix"
    );
    remove_directory_alias(&prefix);
    fs::remove_dir_all(&outside).expect("remove outside exact prefix");
    fs::remove_dir_all(&root).expect("remove fixture root");
}

#[test]
fn one_entry_transaction_seals_then_adopts_without_source_journal() {
    let root = temporary_root();
    let store = ProposalJournal::open(&root, FinalNewline::Lf).expect("open fixture journal");
    let config = one_immigrant_config();
    let config_sha256 = config
        .get("configSha256")
        .and_then(Value::as_str)
        .expect("fixture config identity")
        .to_owned();
    let operator_implementation_identity = object([]);
    let operator_implementation_sha256 =
        canonical_sha256(&operator_implementation_identity).expect("operator identity");

    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/g0_python_rich_projection.json"))
            .expect("rich Python projection fixture");
    let mut entry = fixture.get("entry").expect("fixture entry").clone();
    let entry_fields = entry.as_object_mut().expect("fixture entry object");
    entry_fields.insert(
        "configSha256".to_owned(),
        Value::String(config_sha256.clone()),
    );
    entry_fields.insert("generationIndex".to_owned(), Value::from(1_u64));
    entry_fields.insert(
        "operatorImplementationSha256".to_owned(),
        Value::String(operator_implementation_sha256),
    );
    entry_fields
        .get_mut("candidate")
        .and_then(Value::as_object_mut)
        .expect("fixture candidate")
        .insert("generationIndex".to_owned(), Value::from(1_u64));
    replace_self_hash(&mut entry, "entrySha256");

    store
        .write_canonical_once(std::path::Path::new("pair-config.json"), &config)
        .expect("persist pair config");
    store
        .write_public_entry(0, &entry)
        .expect("persist rich entry");

    let request = G0FunnelRequest {
        output_root: root.clone(),
        final_newline: FinalNewline::Lf,
        request_sha256: sha_token("small g0 funnel request"),
        authority_sha256: sha_token("small g0 funnel authority"),
        execution_authority: Value::Null,
        config,
        config_sha256,
        generation_index: 1,
        construction_pool_size: 1,
        evaluation_population_size: 1,
        max_proposal_attempts: 1,
        admission_thread_cap: 1,
        publication_policy: PublicationPolicy {
            qd_version: "temporal_qd_evolution_v3".to_owned(),
            policy_name: "hermetic_g0_funnel_test".to_owned(),
            policy_sha256: sha_token("small g0 publication policy"),
            pair_policy: object([]),
            operator_implementation_identity,
            predeclared_evidence_context_sha256: None,
            archive_policy_authority: None,
        },
        identity_ledger: None,
        global_identity_ledger: None,
        audit: false,
    };

    // A divergent prefix from another process must fail closed. Exact prefixes
    // are resumable below because publication writes are deterministic.
    store
        .write_canonical_once(std::path::Path::new("population.json"), &object([]))
        .expect("persist synthetic interrupted output prefix");
    let partial = finalize_g0(&request).expect_err("divergent G0 prefix must fail closed");
    assert!(partial.to_string().contains("divergent"));
    assert!(!root.join(G0_FUNNEL_RECEIPT_PATH).exists());
    fs::remove_file(root.join("population.json"))
        .expect("remove synthetic interrupted output prefix");

    let (first_result, first_receipt) = match finalize_g0(&request).expect("complete funnel") {
        G0FunnelOutcome::Completed {
            pair_generation_result,
            receipt,
        } => (pair_generation_result, receipt),
        outcome => panic!("expected completed funnel, got {outcome:?}"),
    };
    assert!(root.join(G0_FUNNEL_RECEIPT_PATH).is_file());
    assert_eq!(
        first_result
            .get("constructionPoolSize")
            .and_then(Value::as_u64),
        Some(1)
    );
    assert!(
        !fs::read(root.join("population.json"))
            .expect("read sealed population")
            .windows(
                b"sha256:0000000000000000000000000000000000000000000000000000000000000000".len()
            )
            .any(|window| window
                == b"sha256:0000000000000000000000000000000000000000000000000000000000000000"),
        "private population self-hash placeholder reached a public artifact"
    );

    // A normal receipt adoption must not touch proposal-journal bytes.  The
    // source file is deliberately removed after sealing; opening the journal
    // recreates only an empty directory, while the receipt verifies the
    // compact public artifact chain.
    fs::remove_file(root.join("proposal-journal/00000000.json"))
        .expect("remove sealed source only after completion");
    let (adopted_result, adopted_receipt) = match finalize_g0(&request).expect("adopt receipt") {
        G0FunnelOutcome::Adopted {
            pair_generation_result,
            receipt,
            ..
        } => (pair_generation_result, receipt),
        outcome => panic!("expected receipt adoption, got {outcome:?}"),
    };
    assert_eq!(adopted_result, first_result);
    assert_eq!(adopted_receipt, first_receipt);

    // Receipt adoption streams every public output before parsing its compact
    // semantic chain.  Equal byte length cannot turn a replacement into a
    // trusting restart, even with the proposal journal unavailable.
    for public_path in [
        "population.json",
        "evaluation-population.json",
        "generation-journal.json",
    ] {
        let path = root.join(public_path);
        let original = fs::read(&path).expect("read sealed public fixture");
        let mut tampered = original.clone();
        tampered[0] = b'[';
        fs::write(&path, tampered).expect("write same-length public tamper");
        let error = finalize_g0(&request).expect_err("same-length public tamper must fail");
        assert!(
            error.to_string().contains("file SHA-256 drifted"),
            "{public_path} was not rehashed during adoption: {error}"
        );
        fs::write(&path, original).expect("restore sealed public fixture");
    }

    // Even after the source journal is unavailable, compact G0 artifacts are
    // independently validated against the receipt.  Corruption cannot turn
    // into a slow Python fallback or a trusting adoption.
    fs::write(
        root.join("g0-bootstrap/selection.json"),
        b"{\"tampered\":true}\n",
    )
    .expect("tamper sealed selection fixture");
    let corrupted =
        finalize_g0(&request).expect_err("sealed selection corruption must fail closed");
    assert!(corrupted.to_string().contains("selection"));

    fs::remove_dir_all(&root).expect("remove generated temporary fixture root");
}

#[test]
fn receiptless_exact_prefixes_resume_after_every_public_artifact() {
    for artifact in [
        "g0-bootstrap/accepted-pool.json",
        "g0-bootstrap/campaign-construction-ledger.json",
        "g0-bootstrap/selection.json",
        "population.json",
        "evaluation-population.json",
        "generation-journal.json",
        "before-receipt",
    ] {
        let root = temporary_root();
        let request = single_entry_request(&root);
        set_g0_test_crash_after_artifact(Some(artifact));
        let error = finalize_g0(&request).expect_err("injected G0 crash must stop publication");
        assert!(
            error.to_string().contains("injected G0 crash"),
            "{artifact} did not hit the intended failpoint: {error}"
        );
        set_g0_test_crash_after_artifact(None);
        assert!(
            !root.join(G0_FUNNEL_RECEIPT_PATH).exists(),
            "{artifact} must leave no sealed receipt"
        );
        assert!(matches!(
            finalize_g0(&request).expect("exact public prefix must resume natively"),
            G0FunnelOutcome::Completed { .. }
        ));
        assert!(
            root.join(G0_FUNNEL_RECEIPT_PATH).is_file(),
            "native restart did not seal receipt after {artifact}"
        );
        fs::remove_dir_all(&root).expect("remove crash-recovery fixture root");
    }
}

#[test]
fn journal_rejects_over_ceiling_post_completion_and_self_rehashed_rejections() {
    let operator_implementation_identity = object([]);
    let operator_implementation_sha256 =
        canonical_sha256(&operator_implementation_identity).expect("operator identity");

    // Inventory admission rejects a ceiling breach before it reads an
    // attacker-controlled excess entry.
    let ceiling_root = temporary_root();
    let ceiling_store =
        ProposalJournal::open(&ceiling_root, FinalNewline::Lf).expect("open ceiling fixture");
    let ceiling_config = one_immigrant_config();
    let ceiling_config_sha = ceiling_config
        .get("configSha256")
        .and_then(Value::as_str)
        .expect("ceiling config SHA")
        .to_owned();
    ceiling_store
        .write_canonical_once(std::path::Path::new("pair-config.json"), &ceiling_config)
        .expect("persist ceiling config");
    for ordinal in 0..=1 {
        ceiling_store
            .write_public_entry(
                ordinal,
                &fixture_accepted_entry(
                    &ceiling_config_sha,
                    &operator_implementation_sha256,
                    ordinal,
                ),
            )
            .expect("persist excess journal entry");
    }
    let mut ceiling_request = bare_single_entry_request(&ceiling_root);
    ceiling_request.output_root = ceiling_root.clone();
    ceiling_request.config = ceiling_config;
    ceiling_request.config_sha256 = ceiling_config_sha;
    ceiling_request
        .publication_policy
        .operator_implementation_identity = operator_implementation_identity.clone();
    let ceiling_error =
        finalize_g0(&ceiling_request).expect_err("over-ceiling journal must fail closed");
    assert!(ceiling_error.to_string().contains("maxProposalAttempts"));
    fs::remove_dir_all(&ceiling_root).expect("remove ceiling fixture root");

    // Once accepted target is reached, even a perfectly self-hashed rejected
    // row is forbidden; completeness is ordered, not inferred from counts.
    let completion_root = temporary_root();
    let completion_store =
        ProposalJournal::open(&completion_root, FinalNewline::Lf).expect("open completion fixture");
    let completion_config = one_immigrant_config_with_max_proposal_attempts(2);
    let completion_config_sha = completion_config
        .get("configSha256")
        .and_then(Value::as_str)
        .expect("completion config SHA")
        .to_owned();
    let accepted =
        fixture_accepted_entry(&completion_config_sha, &operator_implementation_sha256, 0);
    completion_store
        .write_canonical_once(std::path::Path::new("pair-config.json"), &completion_config)
        .expect("persist completion config");
    completion_store
        .write_public_entry(0, &accepted)
        .expect("persist completion accepted entry");
    completion_store
        .write_public_entry(1, &materialized_rejected_entry(&accepted, 1))
        .expect("persist post-completion rejection");
    let mut completion_request = bare_single_entry_request(&completion_root);
    completion_request.output_root = completion_root.clone();
    completion_request.config = completion_config;
    completion_request.config_sha256 = completion_config_sha;
    completion_request.max_proposal_attempts = 2;
    completion_request
        .publication_policy
        .operator_implementation_identity = operator_implementation_identity.clone();
    let completion_error = finalize_g0(&completion_request)
        .expect_err("entry after accepted construction target must fail");
    assert!(
        completion_error
            .to_string()
            .contains("after the construction target")
    );
    fs::remove_dir_all(&completion_root).expect("remove completion fixture root");

    // The rejection surface is closed independently of its outer self-hash:
    // a proposal that rehashes to a different disposition is still invalid.
    let malformed_root = temporary_root();
    let malformed_store = ProposalJournal::open(&malformed_root, FinalNewline::Lf)
        .expect("open malformed rejection fixture");
    let malformed_config = one_immigrant_config();
    let malformed_config_sha = malformed_config
        .get("configSha256")
        .and_then(Value::as_str)
        .expect("malformed config SHA")
        .to_owned();
    let accepted =
        fixture_accepted_entry(&malformed_config_sha, &operator_implementation_sha256, 0);
    let mut malformed = materialized_rejected_entry(&accepted, 0);
    let proposal = malformed
        .get_mut("proposal")
        .and_then(Value::as_object_mut)
        .expect("malformed rejection proposal");
    proposal.insert(
        "disposition".to_owned(),
        Value::String("operation_rejected".to_owned()),
    );
    let mut proposal_value = Value::Object(proposal.clone());
    replace_self_hash(&mut proposal_value, "proposalSha256");
    malformed
        .as_object_mut()
        .expect("malformed rejection entry")
        .insert("proposal".to_owned(), proposal_value);
    replace_self_hash(&mut malformed, "entrySha256");
    malformed_store
        .write_canonical_once(std::path::Path::new("pair-config.json"), &malformed_config)
        .expect("persist malformed config");
    malformed_store
        .write_public_entry(0, &malformed)
        .expect("persist self-rehashed malformed rejection");
    let mut malformed_request = bare_single_entry_request(&malformed_root);
    malformed_request.output_root = malformed_root.clone();
    malformed_request.config = malformed_config;
    malformed_request.config_sha256 = malformed_config_sha;
    malformed_request
        .publication_policy
        .operator_implementation_identity = operator_implementation_identity;
    let malformed_error =
        finalize_g0(&malformed_request).expect_err("self-rehashed rejected row must fail closed");
    assert!(
        malformed_error
            .to_string()
            .contains("proposal source surface drifted")
    );
    fs::remove_dir_all(&malformed_root).expect("remove malformed fixture root");

    // A rehashed audit is also a closed input, rather than opaque diagnostic
    // data.  An added field must fail independently of the enclosing row.
    let audit_root = temporary_root();
    let audit_store =
        ProposalJournal::open(&audit_root, FinalNewline::Lf).expect("open audit rejection fixture");
    let audit_config = one_immigrant_config();
    let audit_config_sha = audit_config
        .get("configSha256")
        .and_then(Value::as_str)
        .expect("audit config SHA")
        .to_owned();
    let audit_operator_identity = object([]);
    let audit_operator_sha =
        canonical_sha256(&audit_operator_identity).expect("audit operator identity");
    let accepted = fixture_accepted_entry(&audit_config_sha, &audit_operator_sha, 0);
    let mut malformed_audit = materialized_rejected_entry(&accepted, 0);
    let proposal = malformed_audit
        .get_mut("proposal")
        .and_then(Value::as_object_mut)
        .expect("audit rejection proposal");
    let pair_identity = proposal
        .get("pairIdentitySha256")
        .expect("audit proposal pair identity")
        .clone();
    let mut audit = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_rich_immigrant_pair_construction_v1".to_owned()),
        ),
        ("pairIdentitySha256", pair_identity),
        ("sides", object([])),
        ("unexpected", Value::Bool(true)),
    ]);
    replace_self_hash(&mut audit, "auditSha256");
    proposal.insert("factoryConstructionAudit".to_owned(), audit);
    let mut proposal_value = Value::Object(proposal.clone());
    replace_self_hash(&mut proposal_value, "proposalSha256");
    malformed_audit
        .as_object_mut()
        .expect("audit rejection entry")
        .insert("proposal".to_owned(), proposal_value);
    replace_self_hash(&mut malformed_audit, "entrySha256");
    let mut audit_request = bare_single_entry_request(&audit_root);
    audit_request.config = audit_config.clone();
    audit_request.config_sha256 = audit_config_sha;
    audit_request
        .publication_policy
        .operator_implementation_identity = audit_operator_identity;
    audit_store
        .write_canonical_once(std::path::Path::new("pair-config.json"), &audit_config)
        .expect("persist audit config");
    audit_store
        .write_public_entry(0, &malformed_audit)
        .expect("persist self-rehashed malformed audit");
    let audit_error =
        finalize_g0(&audit_request).expect_err("self-rehashed audit must fail closed");
    assert!(
        audit_error
            .to_string()
            .contains("rich construction audit fields are not exact")
    );
    fs::remove_dir_all(&audit_root).expect("remove audit fixture root");
}

#[test]
fn synthetic_v5_evolvable_entry_without_compiled_graph_fails_closed() {
    let fixture = v5_evolvable_golden();
    assert_eq!(
        fixture.get("schemaVersion").and_then(Value::as_str),
        Some("temporal_qd_v5_evolvable_g0_journal_golden_v1")
    );
    let entry = fixture.get("entry").expect("golden v5 entry").clone();
    let candidate = entry
        .get("candidate")
        .and_then(Value::as_object)
        .expect("golden v5 candidate");
    assert_eq!(
        candidate
            .get("bidirectionalGenome")
            .and_then(|pair| pair.get("long"))
            .and_then(|module| module.get("program"))
            .and_then(|program| program.get("programKind"))
            .and_then(Value::as_str),
        Some("evolvable_module_genome_v1")
    );

    let error = temporal_qd_kernel::g0::admit_accepted_pair_entry(&entry)
        .expect_err("test compiler lacks the production compiled graph");
    assert!(error.to_string().contains("missing transitions"));
}

#[test]
fn actual_v5_evolvable_journal_golden_matches_python_descriptor_then_seals() {
    let fixture = actual_v5_evolvable_golden();
    assert_eq!(
        fixture.get("schemaVersion").and_then(Value::as_str),
        Some("temporal_qd_v5_evolvable_g0_journal_golden_v2")
    );
    let entry = rehydrate_actual_v5_entry(&fixture);
    let mut entry_without_hash = entry.clone();
    entry_without_hash
        .as_object_mut()
        .expect("rehydrated actual entry")
        .remove("entrySha256");
    assert_eq!(
        canonical_sha256(&entry_without_hash).expect("hash rehydrated actual entry"),
        fixture
            .get("entrySha256")
            .and_then(Value::as_str)
            .expect("actual entry golden hash")
    );
    let admitted = temporal_qd_kernel::g0::admit_accepted_pair_entry(&entry)
        .expect("admit exact preserved v5 rich entry");
    assert_eq!(
        admitted.descriptor_projection,
        *fixture
            .get("expectedDescriptorProjection")
            .expect("Python descriptor projection golden")
    );

    // Keep the real, full v5 entry semantically intact while shrinking only
    // its G0 target for a bounded transaction test.  Config/entry bindings
    // are recomputed exactly; no rich candidate or selection list crosses the
    // transaction request.
    let config = one_immigrant_v1_config();
    let config_sha256 = config
        .get("configSha256")
        .and_then(Value::as_str)
        .expect("small v5 config identity")
        .to_owned();
    let mut small_entry = entry;
    small_entry
        .as_object_mut()
        .expect("actual entry object")
        .insert(
            "configSha256".to_owned(),
            Value::String(config_sha256.clone()),
        );
    replace_self_hash(&mut small_entry, "entrySha256");

    let operator_implementation_identity = fixture
        .get("operatorImplementationIdentity")
        .expect("actual v5 operator identity")
        .clone();
    assert_eq!(
        canonical_sha256(&operator_implementation_identity).expect("hash actual operator identity"),
        small_entry
            .get("operatorImplementationSha256")
            .and_then(Value::as_str)
            .expect("actual entry operator binding")
    );
    let archive_policy_authority = fixture
        .get("archivePolicyAuthority")
        .expect("actual archive policy authority")
        .clone();
    let publication = fixture
        .get("publication")
        .and_then(Value::as_object)
        .expect("actual publication policy surface");

    let root = temporary_root();
    let store =
        ProposalJournal::open(&root, FinalNewline::Lf).expect("open actual v5 test journal");
    store
        .write_canonical_once(std::path::Path::new("pair-config.json"), &config)
        .expect("persist bounded v5 pair config");
    store
        .write_public_entry(0, &small_entry)
        .expect("persist bounded v5 rich entry");
    let admitted = temporal_qd_kernel::g0::admit_accepted_pair_entry_bound_to_operator(
        &small_entry,
        &operator_implementation_identity,
    )
    .expect("admit bounded v5 entry for ledger binding");
    let (identity_ledger_path, identity_ledger_binding) = write_bound_identity_ledger(
        &root,
        &admitted.executable_semantic_sha256,
        &admitted.candidate_identity_sha256,
    );
    let request = G0FunnelRequest {
        output_root: root.clone(),
        final_newline: FinalNewline::Lf,
        request_sha256: sha_token("actual v5 golden request"),
        authority_sha256: sha_token("actual v5 golden authority"),
        execution_authority: Value::Null,
        config,
        config_sha256,
        generation_index: 1,
        construction_pool_size: 1,
        evaluation_population_size: 1,
        max_proposal_attempts: 1,
        admission_thread_cap: 1,
        publication_policy: PublicationPolicy {
            qd_version: publication
                .get("qdVersion")
                .and_then(Value::as_str)
                .expect("actual qd version")
                .to_owned(),
            policy_name: publication
                .get("policyName")
                .and_then(Value::as_str)
                .expect("actual policy name")
                .to_owned(),
            policy_sha256: publication
                .get("policySha256")
                .and_then(Value::as_str)
                .expect("actual policy identity")
                .to_owned(),
            pair_policy: fixture
                .get("pairPolicy")
                .expect("actual pair policy")
                .clone(),
            operator_implementation_identity,
            predeclared_evidence_context_sha256: None,
            archive_policy_authority: Some(archive_policy_authority.clone()),
        },
        identity_ledger: Some(identity_ledger_binding),
        global_identity_ledger: None,
        audit: false,
    };

    let (result, receipt) = match finalize_g0(&request).expect("complete bounded actual v5 funnel")
    {
        G0FunnelOutcome::Completed {
            pair_generation_result,
            receipt,
        } => (pair_generation_result, receipt),
        outcome => panic!("expected bounded actual v5 completion, got {outcome:?}"),
    };
    assert!(receipt.get("globalIdentityLedger").is_some());
    assert!(result.get("reproductionAllocation").is_some());
    assert!(result.get("reproductionAllocationAccounting").is_some());
    let pool = store
        .read_artifact(std::path::Path::new("g0-bootstrap/accepted-pool.json"))
        .expect("read sealed bounded v5 pool");
    let selected_descriptor = pool
        .get("acceptedReferences")
        .and_then(Value::as_array)
        .and_then(|references| references.first())
        .and_then(|reference| reference.get("descriptorProjection"))
        .expect("sealed actual v5 descriptor projection");
    assert_eq!(
        selected_descriptor,
        fixture
            .get("expectedDescriptorProjection")
            .expect("Python descriptor projection golden")
    );
    for public_path in [
        "population.json",
        "evaluation-population.json",
        "generation-journal.json",
    ] {
        let public = store
            .read_artifact(std::path::Path::new(public_path))
            .expect("read sealed v5 public artifact");
        assert_eq!(
            public.get("archivePolicyAuthority"),
            Some(&archive_policy_authority),
            "{public_path} omitted archive-policy authority"
        );
        if public_path == "evaluation-population.json" {
            // The exact v5 Python evaluation handoff deliberately carries its
            // selected bindings and archive authority, but not the full
            // construction allocation/accounting reductions.
            assert!(public.get("reproductionAllocation").is_none());
            assert!(public.get("reproductionAllocationAccounting").is_none());
        } else {
            assert!(
                public.get("reproductionAllocation").is_some(),
                "{public_path} omitted reproduction allocation"
            );
            assert!(
                public.get("reproductionAllocationAccounting").is_some(),
                "{public_path} omitted reproduction accounting"
            );
        }
    }

    fs::remove_file(root.join("proposal-journal/00000000.json"))
        .expect("remove source only after actual v5 receipt sealing");
    assert!(matches!(
        finalize_g0(&request).expect("adopt sealed actual v5 receipt"),
        G0FunnelOutcome::Adopted { .. }
    ));
    let mut tampered_ledger: Value = serde_json::from_slice(
        &fs::read(&identity_ledger_path).expect("read bounded v5 identity ledger"),
    )
    .expect("parse bounded v5 identity ledger");
    tampered_ledger
        .get_mut("pairExecutableSemantics")
        .and_then(Value::as_array_mut)
        .and_then(|rows| rows.first_mut())
        .and_then(Value::as_object_mut)
        .expect("pair semantic ledger row")
        .insert(
            "candidateIdentitySha256".to_owned(),
            Value::String(sha_token("tampered bounded v5 ledger candidate")),
        );
    replace_self_hash(&mut tampered_ledger, "ledgerSha256");
    fs::write(
        &identity_ledger_path,
        serde_json::to_vec(&tampered_ledger).expect("encode tampered identity ledger"),
    )
    .expect("write tampered identity ledger");
    assert!(
        finalize_g0(&request)
            .expect_err("tampered exact identity ledger must not adopt")
            .to_string()
            .contains("identity ledger")
    );
    fs::remove_dir_all(&root).expect("remove generated actual v5 test root");
}

#[test]
fn evolvable_audit_cannot_self_rehash_under_a_different_public_operator() {
    let fixture = actual_v5_evolvable_golden();
    let mut entry = rehydrate_actual_v5_entry(&fixture);
    let mut outer_operator = fixture
        .get("operatorImplementationIdentity")
        .expect("actual v5 operator identity")
        .clone();
    let operator = outer_operator
        .as_object_mut()
        .expect("actual v5 operator identity object");
    operator.insert(
        "authoritySha256".to_owned(),
        Value::String(sha_token("different operator authority")),
    );
    operator.insert(
        "compilerPolicySha256".to_owned(),
        Value::String(sha_token("different compiler policy")),
    );
    let outer_sha = canonical_sha256(&outer_operator).expect("hash changed operator identity");
    entry.as_object_mut().expect("actual entry object").insert(
        "operatorImplementationSha256".to_owned(),
        Value::String(outer_sha),
    );
    replace_self_hash(&mut entry, "entrySha256");

    let error = temporal_qd_kernel::g0::admit_accepted_pair_entry_bound_to_operator(
        &entry,
        &outer_operator,
    )
    .expect_err("self-rehashed audit from authority A must not pass operator B");
    assert!(
        error.to_string().contains("publication operator"),
        "unexpected authority cross-binding error: {error}"
    );
}
