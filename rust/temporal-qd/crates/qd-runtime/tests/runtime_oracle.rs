use std::{cell::RefCell, fs, path::Path, rc::Rc};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use temporal_qd_contract::canonical_sha256;
use temporal_qd_kernel::{
    factory::{NativeConstructionContext, NativePairAuthority, ParentReference, ProposalIntent},
    genome::{FrozenPair, IdentitySnapshot},
    identity::Side,
};
use temporal_qd_runtime::{DashboardPort, RuntimeManifest, RuntimePairAuthority};

const FIXTURE_ROOT: &str = "../../../../tests/fixtures/temporal_qd_runtime_oracle";
const ORACLE_FIXTURE_FILE_SHA256: &str =
    "e546d62ee99fa58bdf27a882907d24b9d31ee3edce6bc3be9dc071ece0b6f337";
const ORACLE_MANIFEST_FILE_SHA256: &str =
    "6a0167ff9c540673483d84fbf027f1ab3048e0264374b1f2b7b4bdfee2a6a598";
const ORACLE_TRANSCRIPT_FILE_SHA256: &str =
    "6bdfaba890a58e138d73c0000384e48163a6c3fd9fa3636e3b13e3306a5487bb";
const ORACLE_GENERATOR_SOURCE_SHA256: &str =
    "sha256:489d995a5d3f217255d21cd6c75b59982483782fa8876855df4ffa21cedab19f";

fn fixture_root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT)
}

fn expected_file_sha256(name: &str) -> &'static str {
    match name {
        "fixture.json" => ORACLE_FIXTURE_FILE_SHA256,
        "runtime-manifest.json" => ORACLE_MANIFEST_FILE_SHA256,
        "dashboard-jsonl-transcript.json" => ORACLE_TRANSCRIPT_FILE_SHA256,
        _ => panic!("unexpected runtime oracle file: {name}"),
    }
}

fn fixture_json(name: &str) -> Value {
    let bytes = fs::read(fixture_root().join(name)).expect("oracle fixture must exist");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        expected_file_sha256(name),
        "runtime oracle {name} file identity drifted"
    );
    serde_json::from_slice(&bytes).expect("oracle fixture must be JSON")
}

fn assert_generator_source_identity(fixture: &Value) {
    assert_eq!(
        fixture["generatorSourceIdentity"]["schemaVersion"],
        "temporal_qd_runtime_oracle_generator_source_v1"
    );
    assert_eq!(
        fixture["generatorSourceIdentity"]["generatorSourceSha256"],
        ORACLE_GENERATOR_SOURCE_SHA256
    );
}

#[derive(Clone)]
struct TranscriptDashboard(Rc<TranscriptState>);

struct TranscriptState {
    records: Vec<Value>,
    cursor: RefCell<usize>,
}

impl TranscriptDashboard {
    fn load() -> Self {
        let transcript = fixture_json("dashboard-jsonl-transcript.json");
        Self(Rc::new(TranscriptState {
            records: transcript["records"].as_array().expect("records").clone(),
            cursor: RefCell::new(0),
        }))
    }

    fn take(&self, operation: &str) -> temporal_qd_runtime::Result<Value> {
        let mut cursor = self.0.cursor.borrow_mut();
        let record = self.0.records.get(*cursor).ok_or_else(|| {
            temporal_qd_runtime::RuntimeError::Manifest("oracle transcript exhausted".into())
        })?;
        assert_eq!(
            record["ordinal"], *cursor as u64,
            "transcript ordinal drift"
        );
        assert_eq!(
            record["request"]["operation"], operation,
            "transcript order drift"
        );
        *cursor += 1;
        Ok(record.clone())
    }

    fn assert_exhausted(&self) {
        assert_eq!(
            *self.0.cursor.borrow(),
            self.0.records.len(),
            "extra transcript calls"
        );
    }

    fn consumed(&self) -> usize {
        *self.0.cursor.borrow()
    }
}

impl DashboardPort for TranscriptDashboard {
    fn validate_v2(
        &self,
        profile: &Value,
        candidate_id: &str,
    ) -> temporal_qd_runtime::Result<Value> {
        let record = self.take("validate_candidate")?;
        assert_eq!(
            record["request"]["candidateId"], candidate_id,
            "validate candidate ID at transcript ordinal {}",
            record["ordinal"]
        );
        assert_eq!(
            record["request"]["sourceProfile"], *profile,
            "validate profile"
        );
        assert_eq!(
            record["request"]["expectedRawSourceProfileSha256"],
            canonical_sha256(profile)?
        );
        Ok(record["response"]["report"].clone())
    }

    fn compile_bidirectional(
        &self,
        long: &Value,
        short: &Value,
        candidate_id: &str,
    ) -> temporal_qd_runtime::Result<Value> {
        let record = self.take("compile_bidirectional")?;
        assert_eq!(
            record["request"]["candidateId"], candidate_id,
            "compile candidate ID at transcript ordinal {}",
            record["ordinal"]
        );
        assert_eq!(
            record["request"]["longProfile"], *long,
            "compile long profile"
        );
        assert_eq!(
            record["request"]["shortProfile"], *short,
            "compile short profile"
        );
        assert_eq!(
            record["request"]["expectedLongRawSourceProfileSha256"],
            canonical_sha256(long)?
        );
        assert_eq!(
            record["request"]["expectedShortRawSourceProfileSha256"],
            canonical_sha256(short)?
        );
        Ok(
            json!({"profile": record["response"]["result"]["profile"], "validation": record["response"]["result"]["report"]}),
        )
    }
}

fn native_context(manifest: &RuntimeManifest) -> NativeConstructionContext {
    NativeConstructionContext {
        generation_index: manifest.generation_index,
        birth_ordinal: 0,
        proposal_ordinal: 0,
        pair_policy: manifest.bidirectional_pair_policy.clone(),
        evidence_identity_context: manifest.evidence_identity_context.clone(),
        frozen_construction_catalog: manifest.pair_run_config["longModule"]["catalog"]
            .as_object()
            .map(|_| manifest.pair_run_config["longModule"]["catalog"].clone()),
        g0_evaluation_width: Some(1),
        factory_construction_policy: None,
    }
}

fn parent(output: &temporal_qd_kernel::factory::NativeProposal, id: &str) -> ParentReference {
    let pair = output.proposal["pair"]
        .as_object()
        .map(|_| output.proposal["pair"].clone())
        .unwrap_or_else(|| output.proposal["factoryPair"].clone());
    ParentReference {
        pair_identity_sha256: pair["identities"]["pairIdentitySha256"]
            .as_str()
            .expect("pair identity")
            .into(),
        candidate_id: id.into(),
        pair_payload: pair,
        selection_audit: None,
    }
}

fn parent_from_pair(pair: &FrozenPair, id: &str) -> ParentReference {
    ParentReference {
        pair_identity_sha256: pair.identity_sha256().expect("pair identity"),
        candidate_id: id.into(),
        pair_payload: pair.canonical_payload().expect("pair payload"),
        selection_audit: None,
    }
}

#[test]
fn runtime_manifest_accepts_oracle_self_hashes_and_rejects_tampering_before_dashboard() {
    let fixture = fixture_json("fixture.json");
    assert_generator_source_identity(&fixture);
    let manifest_value = fixture_json("runtime-manifest.json");
    assert_eq!(
        fixture["runtimeManifestSha256"],
        canonical_sha256(&manifest_value).unwrap()
    );

    let manifest = RuntimeManifest::from_value(&manifest_value).expect("verified runtime manifest");
    assert_eq!(
        manifest.pair_run_config_sha256,
        manifest_value["pairRunConfigSha256"]
    );
    assert_eq!(
        manifest.evidence_identity_context_sha256.as_deref(),
        manifest_value["evidenceIdentityContextSha256"].as_str()
    );

    let dashboard = TranscriptDashboard::load();
    let mut tampered = manifest_value;
    tampered["pairRunConfig"]["runLabel"] = Value::String("tampered-before-dashboard".into());
    let error = RuntimeManifest::from_value(&tampered).expect_err("tampered config must fail");
    assert!(
        error
            .to_string()
            .contains("pair-run configuration identity drifted"),
        "unexpected manifest error: {error}"
    );
    assert_eq!(
        dashboard.consumed(),
        0,
        "manifest rejection must precede Dashboard admission"
    );
}

fn assert_materialized(output: &temporal_qd_kernel::factory::NativeProposal, expected: &Value) {
    assert_materialized_identity_integrity(output);
    assert_eq!(
        output.executable_semantic_sha256.as_deref(),
        expected["pairExecutableSemanticSha256"].as_str()
    );
    let candidate = output.candidate.as_ref().expect("materialized candidate");
    for key in [
        "sourceProfileSha256",
        "programSha256",
        "validationReportSha256",
    ] {
        assert_eq!(candidate[key], expected[key], "{key}");
    }
    assert_eq!(
        candidate["bidirectionalGenome"]["identities"]["pairIdentitySha256"],
        expected["pairIdentitySha256"]
    );
    assert_eq!(
        candidate["candidateIdentitySha256"],
        expected["candidateIdentitySha256"]
    );
    assert_eq!(
        output.proposal["proposalSha256"],
        expected["proposalSha256"]
    );
}

fn assert_materialized_identity_integrity(output: &temporal_qd_kernel::factory::NativeProposal) {
    let candidate = output.candidate.as_ref().expect("materialized candidate");
    // A candidate intentionally retains both public compatibility copies of
    // its frozen pair: one in the proposal and one as its genome. The runtime
    // may reduce transient allocation while building them, but never alter
    // either serialized value or their relationship.
    let proposal_pair = output.proposal.get("pair").unwrap_or_else(|| {
        output
            .proposal
            .get("factoryPair")
            .expect("materialized proposal must contain a frozen pair")
    });
    assert_eq!(
        &candidate["bidirectionalGenome"], proposal_pair,
        "candidate frozen genome must exactly reproduce proposal pair bytes"
    );
    assert_eq!(
        candidate["pairProposal"], output.proposal,
        "candidate compatibility proposal must exactly reproduce the public proposal"
    );
    assert_eq!(
        output.proposal["proposalSha256"],
        canonical_sha256(&{
            let mut material = output.proposal.clone();
            material
                .as_object_mut()
                .expect("proposal object")
                .remove("proposalSha256");
            material
        })
        .unwrap(),
        "materialized proposal must self-hash exactly"
    );
    assert_eq!(
        candidate["candidateIdentitySha256"],
        canonical_sha256(&candidate["candidateIdentityMaterial"]).unwrap(),
        "candidate identity must commit to its full identity material"
    );
    assert_eq!(
        candidate["candidateIdentityMaterial"]["materializedPairProposalSha256"],
        output.proposal["proposalSha256"],
        "candidate identity must commit to the materialized proposal identity"
    );
    assert!(
        candidate["canonicalEvidenceIdentitySha256"]
            .as_str()
            .is_some_and(|value| {
                value.starts_with("sha256:")
                    && value.len() == "sha256:".len() + 64
                    && value["sha256:".len()..]
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            }),
        "candidate canonical evidence identity must be a lowercase SHA-256"
    );
    assert_eq!(
        output
            .predeclared_lake_scope
            .as_ref()
            .and_then(|report| report.get("acceptable")),
        Some(&Value::Bool(true)),
        "materialized candidate must remain within an admitted evidence scope"
    );
    assert!(output.funnel_material.is_some());
}

fn assert_rich_immigrant_factory_construction_audit(
    output: &temporal_qd_kernel::factory::NativeProposal,
) {
    let pair = FrozenPair::from_payload(&output.proposal["factoryPair"])
        .expect("rich immigrant pair must remain restart-safe");
    let audit = &output.proposal["factoryConstructionAudit"];
    assert_eq!(
        audit["schemaVersion"],
        "temporal_qd_rich_immigrant_pair_construction_v1"
    );
    assert_eq!(
        audit["pairIdentitySha256"],
        pair.identity_sha256().expect("pair identity")
    );
    assert_eq!(
        audit["auditSha256"],
        canonical_sha256(&{
            let mut material = audit.clone();
            material
                .as_object_mut()
                .expect("factory construction audit object")
                .remove("auditSha256");
            material
        })
        .unwrap(),
        "factory construction audit must self-hash exactly"
    );
    let sides = audit["sides"]
        .as_object()
        .expect("factory construction audit sides");
    assert_eq!(sides.len(), 2, "factory audit may only record two sides");
    for module in [&pair.long, &pair.short] {
        let construction = module
            .lineage
            .iter()
            .rev()
            .find(|row| row["operation"].as_str() == Some("rich_immigrant_construction"))
            .expect("rich module construction lineage");
        let module_audit = &construction["audit"];
        assert_eq!(
            module_audit["schemaVersion"],
            "temporal_qd_rich_immigrant_module_construction_v1"
        );
        assert_eq!(module_audit["side"], module.direction);
        assert_eq!(
            module_audit["auditSha256"],
            canonical_sha256(&{
                let mut material = module_audit.clone();
                material
                    .as_object_mut()
                    .expect("module construction audit object")
                    .remove("auditSha256");
                material
            })
            .unwrap(),
            "module construction audit must self-hash exactly"
        );
        assert_eq!(
            sides.get(&module.direction),
            Some(module_audit),
            "factory audit must reproduce the module lineage audit exactly"
        );
    }
}

#[test]
fn runtime_replays_real_python_oracle_transcript_in_exact_order() {
    let fixture = fixture_json("fixture.json");
    assert_generator_source_identity(&fixture);
    let manifest_value = fixture_json("runtime-manifest.json");
    assert_eq!(
        fixture["runtimeManifestSha256"],
        canonical_sha256(&manifest_value).unwrap()
    );
    let manifest = RuntimeManifest::from_value(&manifest_value).expect("verified runtime manifest");
    assert_eq!(
        manifest.evidence_identity_context_sha256.as_deref(),
        fixture["cases"]["richImmigrant"]["predeclaredEvidenceContextSha256"].as_str(),
        "all oracle cases share the frozen predeclared evidence context"
    );
    let dashboard = TranscriptDashboard::load();
    let mut authority = RuntimePairAuthority::new(&manifest, dashboard.clone()).unwrap();
    let context = native_context(&manifest);
    let rich = authority
        .execute(
            &ProposalIntent::RichImmigrant {
                proposal_seed: "runtime-oracle-rich-immigrant-v1".into(),
                long_seed: "unused".into(),
                short_seed: "unused".into(),
            },
            &context,
        )
        .unwrap();
    assert_materialized_identity_integrity(&rich);
    assert_eq!(
        rich.executable_semantic_sha256.as_deref(),
        fixture["cases"]["richImmigrant"]["pairExecutableSemanticSha256"].as_str()
    );
    let rich_candidate = rich.candidate.as_ref().expect("rich candidate");
    for key in [
        "sourceProfileSha256",
        "programSha256",
        "validationReportSha256",
    ] {
        assert_eq!(
            rich_candidate[key], fixture["cases"]["richImmigrant"][key],
            "{key}"
        );
    }
    assert_eq!(
        rich_candidate["bidirectionalGenome"]["identities"]["pairIdentitySha256"],
        fixture["cases"]["richImmigrant"]["pairIdentitySha256"]
    );
    assert_rich_immigrant_factory_construction_audit(&rich);
    let root = parent(&rich, "rich");
    let depth2_expected = &fixture["cases"]["sequentialMutationDepth2"];
    let depth2 = authority
        .execute(
            &ProposalIntent::StructuralMutation {
                proposal_seed: depth2_expected["operation"]["proposalSeed"]
                    .as_str()
                    .expect("depth-2 oracle proposal seed")
                    .into(),
                parent: root.clone(),
                mutation_depth: 2,
                forced_operator_family: None,
            },
            &context,
        )
        .unwrap();
    assert_materialized(&depth2, depth2_expected);
    let depth3_expected = &fixture["cases"]["sequentialMutationDepth3"];
    let depth3 = authority
        .execute(
            &ProposalIntent::StructuralMutation {
                proposal_seed: depth3_expected["operation"]["proposalSeed"]
                    .as_str()
                    .expect("depth-3 oracle proposal seed")
                    .into(),
                parent: root,
                mutation_depth: 3,
                forced_operator_family: None,
            },
            &context,
        )
        .unwrap();
    assert_materialized(&depth3, depth3_expected);
    let crossover_expected = &fixture["cases"]["sameSideCrossoverMaterialized"];
    let crossover = authority
        .execute(
            &ProposalIntent::SameSideCrossover {
                proposal_seed: crossover_expected["operation"]["proposalSeed"]
                    .as_str()
                    .expect("materialized crossover oracle proposal seed")
                    .into(),
                side: Side::Long,
                parent: parent(&depth2, "depth2"),
                mate: parent(&depth3, "depth3"),
                mate_selection_attempts: vec![],
            },
            &context,
        )
        .unwrap();
    assert_materialized(&crossover, crossover_expected);
    let mut foreign_mate = FrozenPair::from_payload(&rich.proposal["factoryPair"])
        .expect("rich pair payload must be restart-safe");
    foreign_mate.long.native_authority = IdentitySnapshot::create(
        "nativeAuthority",
        "temporal_qd_runtime_oracle_negative_fixture_v1",
        &json!({"reason": "deliberately foreign frozen authority for rejection coverage"}),
    )
    .expect("foreign snapshot");
    let rejected = authority
        .execute(
            &ProposalIntent::SameSideCrossover {
                proposal_seed:
                    fixture["cases"]["sameSideCrossoverRejected"]["operation"]["proposalSeed"]
                        .as_str()
                        .expect("rejected crossover oracle proposal seed")
                        .into(),
                side: Side::Long,
                parent: parent(&rich, "rich"),
                mate: parent_from_pair(&foreign_mate, "foreign"),
                mate_selection_attempts: vec![],
            },
            &context,
        )
        .expect("foreign-authority crossover must be represented as a rejection");
    assert!(rejected.candidate.is_none());
    assert!(rejected.executable_semantic_sha256.is_none());
    assert_eq!(
        rejected.proposal["disposition"],
        fixture["cases"]["sameSideCrossoverRejected"]["operation"]["disposition"]
    );
    assert_eq!(
        rejected.proposal["proposalSha256"],
        fixture["cases"]["sameSideCrossoverRejected"]["proposalSha256"]
    );
    dashboard.assert_exhausted();
}
