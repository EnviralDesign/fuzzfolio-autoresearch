//! Crash-boundary and rich-entry integrity coverage for native generation.

use std::{fs, path::PathBuf};

use temporal_qd_contract::{
    JsonNewline, Value, canonical_json, canonical_sha256, python_pretty_json_line,
};
use temporal_qd_kernel::{
    factory::{
        NativeConstructionContext, NativeFunnelMaterial, NativePairAuthority, NativeProposal,
        ProposalIntent,
    },
    generation::{GenerateGenerationRequest, generate_generation},
    journal::{FinalNewline, ProposalJournal},
    proposal::{
        CandidateIdentityLedger, ExplicitParentRing, IdentityLedger, LedgerDecision, LedgerProposal,
    },
    publication::PublicationPolicy,
};

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}

#[derive(Clone)]
struct FacadeLedger {
    identity: Value,
    proposals: u64,
    archive_records: Vec<Value>,
}

impl FacadeLedger {
    fn new(identity: Value) -> Self {
        Self {
            identity,
            proposals: 0,
            archive_records: Vec::new(),
        }
    }

    fn with_archive_records(identity: Value, archive_records: Vec<Value>) -> Self {
        Self {
            identity,
            proposals: 0,
            archive_records,
        }
    }

    fn facade(&self) -> Value {
        let mut ledger = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_identity_ledger_v3".to_owned()),
            ),
            ("pairExecutableSemantics", Value::Array(Vec::new())),
            (
                "pairExecutableSemanticDuplicateRejections",
                Value::from(0_u64),
            ),
            ("proposals", Value::from(self.proposals)),
            ("records", Value::Array(self.archive_records.clone())),
            ("uniqueCounts", unique_counts(&self.archive_records)),
        ]);
        let identity = canonical_sha256(&ledger).expect("hash public ledger");
        ledger
            .as_object_mut()
            .expect("ledger object")
            .insert("ledgerSha256".to_owned(), Value::String(identity));
        ledger
    }
}

fn fixture_record(identity: char) -> Value {
    let sha =
        |character: char| Value::String(format!("sha256:{}", character.to_string().repeat(64)));
    object([
        ("candidateIdentitySha256", sha(identity)),
        ("programSha256", sha('p')),
        ("sourceProfileSha256", sha('s')),
        ("profileSnapshotSha256", sha('n')),
        ("canonicalEvidenceIdentitySha256", sha('e')),
    ])
}

fn unique_counts(records: &[Value]) -> Value {
    let fields = [
        ("candidateIdentity", "candidateIdentitySha256"),
        ("program", "programSha256"),
        ("sourceProfile", "sourceProfileSha256"),
        ("profileSnapshot", "profileSnapshotSha256"),
        ("canonicalEvidence", "canonicalEvidenceIdentitySha256"),
    ];
    object(fields.map(|(count_name, record_name)| {
        let identities = records
            .iter()
            .map(|record| {
                record.as_object().expect("fixture record object")[record_name]
                    .as_str()
                    .expect("fixture record identity")
                    .to_owned()
            })
            .collect::<std::collections::BTreeSet<_>>();
        (count_name, Value::from(identities.len() as u64))
    }))
}

impl IdentityLedger for FacadeLedger {
    fn identity(&self) -> &Value {
        &self.identity
    }

    fn prepare_proposal(
        &self,
        proposal: LedgerProposal<'_>,
    ) -> temporal_qd_kernel::proposal::Result<LedgerDecision> {
        Ok(LedgerDecision::new(
            proposal.tentative_disposition,
            object([]),
            object([
                ("proposalOrdinal", Value::from(proposal.proposal_ordinal)),
                (
                    "disposition",
                    Value::String(proposal.tentative_disposition.to_owned()),
                ),
            ]),
        ))
    }

    fn commit_prepared_delta(
        &mut self,
        _prepared_delta: &Value,
    ) -> temporal_qd_kernel::proposal::Result<()> {
        self.proposals += 1;
        Ok(())
    }

    fn compact_state(&self) -> Value {
        object([
            ("identity", self.identity.clone()),
            ("proposals", Value::from(self.proposals)),
        ])
    }

    fn restore_compact_state(&mut self, state: &Value) -> temporal_qd_kernel::proposal::Result<()> {
        let fields = state.as_object().ok_or_else(|| {
            temporal_qd_kernel::proposal::ProposalError::Contract(
                "test facade ledger state is invalid".to_owned(),
            )
        })?;
        if fields.get("identity") != Some(&self.identity) {
            return Err(temporal_qd_kernel::proposal::ProposalError::Contract(
                "test facade ledger identity mismatched".to_owned(),
            ));
        }
        self.proposals = fields
            .get("proposals")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                temporal_qd_kernel::proposal::ProposalError::Contract(
                    "test facade ledger proposals are invalid".to_owned(),
                )
            })?;
        Ok(())
    }

    fn public_ledger(&self) -> Option<Value> {
        Some(self.facade())
    }
}

fn temp_root(label: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "temporal-qd-kernel-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&root).expect("create test root");
    root
}

/// The only CP0 predecessor accepted by native pair generation: a valid
/// Python general identity ledger formed by removing the two pair-extension
/// fields from the exact runtime pair facade and re-hashing it.
fn general_ledger_before_pair_initialization(pair_ledger: &Value) -> Value {
    let mut general = pair_ledger.clone();
    let fields = general.as_object_mut().expect("pair ledger object");
    fields.remove("ledgerSha256");
    fields.remove("pairExecutableSemantics");
    fields.remove("pairExecutableSemanticDuplicateRejections");
    let hash = canonical_sha256(&general).expect("hash exact general ledger");
    general
        .as_object_mut()
        .expect("general ledger object")
        .insert("ledgerSha256".to_owned(), Value::String(hash));
    general
}

/// Python begins with the general facade, then its verified-parent bootstrap
/// appends historical records before adding the pair extension. This is the
/// compact shape produced by the native parent canary's first CP0 write.
fn general_ledger_before_archive_bootstrap(pair_ledger: &Value) -> Value {
    let mut general = general_ledger_before_pair_initialization(pair_ledger);
    let fields = general.as_object_mut().expect("general ledger object");
    fields.remove("ledgerSha256");
    fields.insert("records".to_owned(), Value::Array(Vec::new()));
    fields.insert("uniqueCounts".to_owned(), unique_counts(&[]));
    let hash = canonical_sha256(&general).expect("hash exact pre-bootstrap general ledger");
    general
        .as_object_mut()
        .expect("general ledger object")
        .insert("ledgerSha256".to_owned(), Value::String(hash));
    general
}

fn write_public_ledger(root: &PathBuf, ledger: &Value) {
    ProposalJournal::open(root, FinalNewline::Lf)
        .expect("open public-ledger fixture journal")
        .write_public_identity_ledger(ledger)
        .expect("write public-ledger fixture");
}

fn self_hashed_entry(ordinal: u64) -> Value {
    let mut entry = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_proposal_entry_v3".to_owned()),
        ),
        (
            "configSha256",
            Value::String(
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            ),
        ),
        ("generationIndex", Value::from(1_u64)),
        ("proposalOrdinal", Value::from(ordinal)),
        ("originKind", Value::String("random_immigrant".to_owned())),
        (
            "proposal",
            object([("floatingProfileWeight", Value::from(1.25_f64))]),
        ),
        (
            "operatorImplementationSha256",
            Value::String(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_owned(),
            ),
        ),
        (
            "disposition",
            Value::String("operation_rejected".to_owned()),
        ),
    ]);
    let hash = canonical_sha256(&entry).expect("hash entry");
    entry
        .as_object_mut()
        .expect("entry object")
        .insert("entrySha256".to_owned(), Value::String(hash));
    entry
}

#[test]
fn rich_entry_reopen_rejects_stale_hash_and_accepts_float_canonical_bytes() {
    let root = temp_root("entry-integrity");
    let store = ProposalJournal::open(&root, FinalNewline::Lf).expect("open journal");
    let entry = self_hashed_entry(0);
    store.write_public_entry(0, &entry).expect("write entry");
    assert_eq!(store.read_public_entry(0).expect("reopen entry"), entry);

    let path = root.join("proposal-journal/00000000.json");
    let bytes = fs::read(&path).expect("read entry bytes");
    let tampered = String::from_utf8(bytes)
        .expect("UTF-8 entry")
        .replace("1.25", "2.25");
    fs::write(&path, tampered).expect("tamper entry bytes");
    assert!(store.read_public_entry(0).is_err());

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[derive(Clone)]
struct RejectingAuthority {
    identity: Value,
}

impl NativePairAuthority for RejectingAuthority {
    fn authority_identity(&self) -> &Value {
        &self.identity
    }

    fn execute(
        &mut self,
        intent: &ProposalIntent,
        _context: &NativeConstructionContext,
    ) -> temporal_qd_kernel::factory::Result<NativeProposal> {
        let mut proposal = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_proposal_v2".to_owned()),
            ),
            (
                "proposalSeed",
                Value::String(intent.proposal_seed().to_owned()),
            ),
            ("originKind", Value::String(intent.origin_kind().to_owned())),
            (
                "disposition",
                Value::String("operation_rejected".to_owned()),
            ),
        ]);
        let hash = canonical_sha256(&proposal).expect("hash proposal");
        proposal
            .as_object_mut()
            .expect("proposal object")
            .insert("proposalSha256".to_owned(), Value::String(hash));
        Ok(NativeProposal::rejected(proposal))
    }
}

#[derive(Clone)]
struct MaterializingAuthority {
    identity: Value,
    predeclared_lake_scope: Option<Value>,
}

impl NativePairAuthority for MaterializingAuthority {
    fn authority_identity(&self) -> &Value {
        &self.identity
    }

    fn execute(
        &mut self,
        intent: &ProposalIntent,
        _context: &NativeConstructionContext,
    ) -> temporal_qd_kernel::factory::Result<NativeProposal> {
        let mut proposal = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_proposal_v2".to_owned()),
            ),
            (
                "proposalSeed",
                Value::String(intent.proposal_seed().to_owned()),
            ),
            ("originKind", Value::String(intent.origin_kind().to_owned())),
            ("disposition", Value::String("materialized".to_owned())),
        ]);
        let proposal_sha256 = canonical_sha256(&proposal).expect("hash proposal");
        proposal.as_object_mut().expect("proposal object").insert(
            "proposalSha256".to_owned(),
            Value::String(proposal_sha256.clone()),
        );
        let source_profile = object([("name", Value::String("test-source".to_owned()))]);
        let source_profile_sha256 = canonical_sha256(&source_profile).expect("hash source");
        let program_sha256 =
            canonical_sha256(&Value::String("test-program".to_owned())).expect("hash program");
        let validation_report_sha256 =
            canonical_sha256(&Value::String("test-validation".to_owned()))
                .expect("hash validation");
        let candidate_identity_material = object([
            ("proposalSha256", Value::String(proposal_sha256.clone())),
            (
                "proposalSeed",
                Value::String(intent.proposal_seed().to_owned()),
            ),
        ]);
        let candidate_identity_sha256 =
            canonical_sha256(&candidate_identity_material).expect("hash candidate identity");
        let candidate_id = format!("qd_{}", &candidate_identity_sha256[7..35]);
        let candidate = object([
            ("candidateId", Value::String(candidate_id)),
            ("candidateIdentityMaterial", candidate_identity_material),
            (
                "candidateIdentitySha256",
                Value::String(candidate_identity_sha256),
            ),
            ("pairProposal", proposal.clone()),
            ("pairProposalSha256", Value::String(proposal_sha256)),
            (
                "sourceMode",
                Value::String("test_random_immigrant".to_owned()),
            ),
            ("seedId", Value::String("test-seed".to_owned())),
            ("programSha256", Value::String(program_sha256.clone())),
            ("sourceProfile", source_profile),
            (
                "sourceProfileSha256",
                Value::String(source_profile_sha256.clone()),
            ),
            (
                "profileSnapshotSha256",
                Value::String(source_profile_sha256.clone()),
            ),
            (
                "validationReportSha256",
                Value::String(validation_report_sha256.clone()),
            ),
            ("structuralOperatorHistory", Value::Array(Vec::new())),
        ]);
        let mut native = NativeProposal::materialized(
            proposal,
            candidate,
            canonical_sha256(&Value::String(format!(
                "test-semantic:{}",
                intent.proposal_seed()
            )))
            .expect("hash executable semantic"),
        );
        native.predeclared_lake_scope = self.predeclared_lake_scope.clone();
        native.funnel_material = Some(NativeFunnelMaterial {
            raw_source_profile_sha256: source_profile_sha256.clone(),
            resolved_profile_sha256: source_profile_sha256,
            program_sha256,
            validation_report_sha256,
        });
        Ok(native)
    }
}

#[derive(Clone)]
struct DuplicatePairAuthority {
    inner: MaterializingAuthority,
    executable_semantic_sha256: String,
}

impl NativePairAuthority for DuplicatePairAuthority {
    fn authority_identity(&self) -> &Value {
        self.inner.authority_identity()
    }

    fn execute(
        &mut self,
        intent: &ProposalIntent,
        context: &NativeConstructionContext,
    ) -> temporal_qd_kernel::factory::Result<NativeProposal> {
        let mut proposal = self.inner.execute(intent, context)?;
        proposal.executable_semantic_sha256 = Some(self.executable_semantic_sha256.clone());
        Ok(proposal)
    }
}

fn request(root: PathBuf, authority_sha256: String) -> GenerateGenerationRequest {
    let mut pair_config = object([("schemaVersion", Value::String("test_config_v1".to_owned()))]);
    let config_sha256 = canonical_sha256(&pair_config).expect("hash config");
    pair_config.as_object_mut().expect("config object").insert(
        "configSha256".to_owned(),
        Value::String(config_sha256.clone()),
    );
    GenerateGenerationRequest {
        output_root: root,
        final_newline: FinalNewline::Lf,
        pair_config,
        config_sha256,
        generation_index: 1,
        target_unique_candidates: 1,
        max_proposal_attempts: 4,
        max_new_proposals: Some(1),
        parent_schedule: None,
        expected_native_authority_sha256: authority_sha256,
        publication_policy: PublicationPolicy {
            qd_version: "test".to_owned(),
            policy_name: "test".to_owned(),
            policy_sha256:
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc".to_owned(),
            pair_policy: object([]),
            operator_implementation_identity: object([]),
            predeclared_evidence_context_sha256: None,
        },
        g0_evaluation_width: None,
        evidence_identity_context: None,
        frozen_construction_catalog: None,
        factory_construction_policy: None,
    }
}

fn request_with_tripwire(root: PathBuf, authority_sha256: String) -> GenerateGenerationRequest {
    let mut generated = request(root, authority_sha256);
    let policy = object([(
        "collisionTripwire",
        object([
            ("minimumImmigrantAttempts", Value::from(1_u64)),
            ("minimumAcceptedRatio", Value::from(0.25_f64)),
        ]),
    )]);
    let mut pair_config = object([
        ("schemaVersion", Value::String("test_config_v1".to_owned())),
        ("immigrantConstructionPolicy", policy.clone()),
    ]);
    let config_sha256 = canonical_sha256(&pair_config).expect("hash tripwire config");
    pair_config.as_object_mut().expect("config object").insert(
        "configSha256".to_owned(),
        Value::String(config_sha256.clone()),
    );
    generated.pair_config = pair_config;
    generated.config_sha256 = config_sha256;
    generated.factory_construction_policy = Some(policy);
    generated
}

#[test]
fn orphan_segment_is_resealed_from_checkpoint_zero_without_reexecution() {
    let root = temp_root("orphan-segment");
    let identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let mut authority = RejectingAuthority { identity };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let ledger_identity = object([("ledger", Value::String("test".to_owned()))]);
    let mut ledger =
        CandidateIdentityLedger::new(ledger_identity.clone(), Vec::new()).expect("ledger");
    let generation_request = request(root.clone(), authority_sha256.clone());

    let progress = generate_generation(
        &generation_request,
        &mut authority,
        &mut parents,
        &mut ledger,
    )
    .expect("initial rejected proposal");
    assert_eq!(progress["completed"], Value::Bool(false));
    let checkpoint = root.join("internal/checkpoints/00000001.json");
    assert!(checkpoint.exists());
    fs::remove_file(&checkpoint).expect("remove exact final checkpoint");

    let mut resumed_authority = RejectingAuthority {
        identity: authority.authority_identity().clone(),
    };
    let mut resumed_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut resumed_ledger =
        CandidateIdentityLedger::new(ledger_identity, Vec::new()).expect("ledger");
    let mut resume_request = request(root.clone(), authority_sha256);
    resume_request.max_new_proposals = Some(0);
    let resumed = generate_generation(
        &resume_request,
        &mut resumed_authority,
        &mut resumed_parents,
        &mut resumed_ledger,
    )
    .expect("recovery must reseal segment instead of re-executing it");
    assert_eq!(resumed["proposalCount"], Value::from(1_u64));
    assert!(checkpoint.exists());

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn resume_refuses_a_divergent_pair_config_before_it_can_mutate_state() {
    let root = temp_root("pair-config-conflict");
    let identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let mut authority = RejectingAuthority { identity };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let ledger_identity = object([("ledger", Value::String("test".to_owned()))]);
    let mut ledger =
        CandidateIdentityLedger::new(ledger_identity.clone(), Vec::new()).expect("ledger");
    generate_generation(
        &request(root.clone(), authority_sha256.clone()),
        &mut authority,
        &mut parents,
        &mut ledger,
    )
    .expect("initial generation");

    let mut conflicting = request(root.clone(), authority_sha256);
    let mut material = object([
        ("schemaVersion", Value::String("test_config_v1".to_owned())),
        ("changed", Value::Bool(true)),
    ]);
    let changed_sha256 = canonical_sha256(&material).expect("hash changed config");
    material.as_object_mut().expect("config object").insert(
        "configSha256".to_owned(),
        Value::String(changed_sha256.clone()),
    );
    conflicting.pair_config = material;
    conflicting.config_sha256 = changed_sha256;

    let mut resumed_authority = RejectingAuthority {
        identity: authority.authority_identity().clone(),
    };
    let mut resumed_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut resumed_ledger =
        CandidateIdentityLedger::new(ledger_identity, Vec::new()).expect("ledger");
    assert!(
        generate_generation(
            &conflicting,
            &mut resumed_authority,
            &mut resumed_parents,
            &mut resumed_ledger,
        )
        .is_err()
    );

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn resume_refuses_a_missing_historical_public_entry() {
    let root = temp_root("missing-public-entry");
    let identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let mut authority = RejectingAuthority { identity };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let ledger_identity = object([("ledger", Value::String("test".to_owned()))]);
    let mut ledger =
        CandidateIdentityLedger::new(ledger_identity.clone(), Vec::new()).expect("ledger");
    let mut initial = request(root.clone(), authority_sha256.clone());
    initial.max_new_proposals = Some(2);
    generate_generation(&initial, &mut authority, &mut parents, &mut ledger)
        .expect("two sealed rejections");
    fs::remove_file(root.join("proposal-journal/00000000.json"))
        .expect("remove historical public entry");

    let mut resumed_authority = RejectingAuthority {
        identity: authority.authority_identity().clone(),
    };
    let mut resumed_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut resumed_ledger =
        CandidateIdentityLedger::new(ledger_identity, Vec::new()).expect("ledger");
    let mut continuation = request(root.clone(), authority_sha256);
    continuation.max_new_proposals = Some(0);
    assert!(
        generate_generation(
            &continuation,
            &mut resumed_authority,
            &mut resumed_parents,
            &mut resumed_ledger,
        )
        .is_err()
    );

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn resume_refuses_an_interposed_public_entry_without_a_segment() {
    let root = temp_root("interposed-public-entry");
    let identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let mut authority = RejectingAuthority { identity };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let ledger_identity = object([("ledger", Value::String("test".to_owned()))]);
    let mut ledger =
        CandidateIdentityLedger::new(ledger_identity.clone(), Vec::new()).expect("ledger");
    generate_generation(
        &request(root.clone(), authority_sha256.clone()),
        &mut authority,
        &mut parents,
        &mut ledger,
    )
    .expect("initial rejected proposal");
    ProposalJournal::open(&root, FinalNewline::Lf)
        .expect("open journal")
        .write_public_entry(1, &self_hashed_entry(1))
        .expect("write forged interposed public entry");

    let mut resumed_authority = RejectingAuthority {
        identity: authority.authority_identity().clone(),
    };
    let mut resumed_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut resumed_ledger =
        CandidateIdentityLedger::new(ledger_identity, Vec::new()).expect("ledger");
    let mut continuation = request(root.clone(), authority_sha256);
    continuation.max_new_proposals = Some(0);
    assert!(
        generate_generation(
            &continuation,
            &mut resumed_authority,
            &mut resumed_parents,
            &mut resumed_ledger,
        )
        .is_err()
    );

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn resume_replays_and_rejects_a_self_hashed_historical_checkpoint_forgery() {
    let root = temp_root("checkpoint-history-forgery");
    let identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let mut authority = RejectingAuthority { identity };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let ledger_identity = object([("ledger", Value::String("test".to_owned()))]);
    let mut ledger =
        CandidateIdentityLedger::new(ledger_identity.clone(), Vec::new()).expect("ledger");
    let mut initial = request(root.clone(), authority_sha256.clone());
    initial.max_new_proposals = Some(2);
    generate_generation(&initial, &mut authority, &mut parents, &mut ledger)
        .expect("two durable proposals");

    let checkpoint_path = root.join("internal/checkpoints/00000001.json");
    let mut forged: Value =
        serde_json::from_slice(&fs::read(&checkpoint_path).expect("read checkpoint"))
            .expect("checkpoint JSON");
    forged["proposalState"]["immigrantAttempts"] = Value::from(99_u64);
    forged
        .as_object_mut()
        .expect("checkpoint object")
        .remove("checkpointSha256");
    let checkpoint_sha256 = canonical_sha256(&forged).expect("rehash forged checkpoint");
    forged.as_object_mut().expect("checkpoint object").insert(
        "checkpointSha256".to_owned(),
        Value::String(checkpoint_sha256),
    );
    fs::write(
        &checkpoint_path,
        format!(
            "{}\n",
            canonical_json(&forged).expect("canonical checkpoint")
        ),
    )
    .expect("install self-hashed historical forgery");

    let mut resumed_authority = RejectingAuthority {
        identity: authority.authority_identity().clone(),
    };
    let mut resumed_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut resumed_ledger =
        CandidateIdentityLedger::new(ledger_identity, Vec::new()).expect("ledger");
    let mut continuation = request(root.clone(), authority_sha256);
    continuation.max_new_proposals = Some(0);
    assert!(
        generate_generation(
            &continuation,
            &mut resumed_authority,
            &mut resumed_parents,
            &mut resumed_ledger,
        )
        .is_err()
    );

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn resume_refuses_a_missing_historical_segment() {
    let root = temp_root("missing-historical-segment");
    let identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let mut authority = RejectingAuthority { identity };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let ledger_identity = object([("ledger", Value::String("test".to_owned()))]);
    let mut ledger =
        CandidateIdentityLedger::new(ledger_identity.clone(), Vec::new()).expect("ledger");
    let mut initial = request(root.clone(), authority_sha256.clone());
    initial.max_new_proposals = Some(2);
    generate_generation(&initial, &mut authority, &mut parents, &mut ledger)
        .expect("two sealed proposals");
    fs::remove_file(root.join("internal/segments/00000000.json"))
        .expect("remove historical segment");

    let mut resumed_authority = RejectingAuthority {
        identity: authority.authority_identity().clone(),
    };
    let mut resumed_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut resumed_ledger =
        CandidateIdentityLedger::new(ledger_identity, Vec::new()).expect("ledger");
    let mut continuation = request(root.clone(), authority_sha256);
    continuation.max_new_proposals = Some(0);
    assert!(
        generate_generation(
            &continuation,
            &mut resumed_authority,
            &mut resumed_parents,
            &mut resumed_ledger,
        )
        .is_err()
    );

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn immigrant_collision_tripwire_is_durable_and_rejects_the_same_resume() {
    let root = temp_root("collision-tripwire");
    let identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let ledger_identity = object([("ledger", Value::String("test".to_owned()))]);
    let generated_request = request_with_tripwire(root.clone(), authority_sha256.clone());
    let mut authority = RejectingAuthority { identity };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger =
        CandidateIdentityLedger::new(ledger_identity.clone(), Vec::new()).expect("ledger");
    assert!(
        generate_generation(
            &generated_request,
            &mut authority,
            &mut parents,
            &mut ledger
        )
        .is_err()
    );

    let failure_path = root.join("immigrant-collision-tripwire.json");
    let failure_bytes = fs::read(&failure_path).expect("durable tripwire artifact");
    let failure = String::from_utf8(failure_bytes.clone()).expect("UTF-8 tripwire artifact");
    assert!(failure.contains("temporal_qd_immigrant_collision_tripwire_v1"));
    assert!(failure.contains("\"immigrantAttempts\":1"));
    assert!(failure.contains("\"acceptedRatio\":0"));

    let mut resumed_authority = RejectingAuthority {
        identity: authority.authority_identity().clone(),
    };
    let mut resumed_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut resumed_ledger =
        CandidateIdentityLedger::new(ledger_identity, Vec::new()).expect("ledger");
    let mut resumed_request = request_with_tripwire(root.clone(), authority_sha256);
    resumed_request.max_new_proposals = Some(0);
    assert!(
        generate_generation(
            &resumed_request,
            &mut resumed_authority,
            &mut resumed_parents,
            &mut resumed_ledger,
        )
        .is_err()
    );
    assert_eq!(
        fs::read(&failure_path).expect("tripwire remains write-once"),
        failure_bytes
    );
    assert!(!root.join("internal/segments/00000001.json").exists());

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn public_identity_ledger_advances_per_segment_and_checkpoint_repairs_it() {
    let root = temp_root("public-ledger-facade");
    let identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let ledger_identity = object([("ledger", Value::String("facade".to_owned()))]);
    let mut authority = RejectingAuthority { identity };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = FacadeLedger::new(ledger_identity.clone());
    let mut first = request(root.clone(), authority_sha256.clone());
    first.max_new_proposals = Some(1);
    generate_generation(&first, &mut authority, &mut parents, &mut ledger)
        .expect("first sealed proposal");
    let ledger_path = root.join("identity-ledger.json");
    let after_first = fs::read(&ledger_path).expect("first public ledger facade");
    assert_eq!(
        after_first,
        python_pretty_json_line(&ledger.facade(), JsonNewline::Lf).expect("Python pretty ledger")
    );

    // Simulate the exact receipt window after the segment and mutable facade
    // are durable but before checkpoint 1 is installed. Recovery must first
    // reconcile the facade back to checkpoint 0, replay the segment once,
    // then republish the identical proposal-1 facade.
    fs::remove_file(root.join("internal/checkpoints/00000001.json"))
        .expect("remove checkpoint after durable segment/facade");
    let mut receipt_authority = RejectingAuthority {
        identity: authority.authority_identity().clone(),
    };
    let mut receipt_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut receipt_ledger = FacadeLedger::new(ledger_identity.clone());
    let mut receipt_recovery = request(root.clone(), authority_sha256.clone());
    receipt_recovery.max_new_proposals = Some(0);
    generate_generation(
        &receipt_recovery,
        &mut receipt_authority,
        &mut receipt_parents,
        &mut receipt_ledger,
    )
    .expect("segment/ledger receipt recovery");
    assert_eq!(
        fs::read(&ledger_path).expect("replayed proposal-1 facade"),
        after_first
    );
    assert!(root.join("internal/checkpoints/00000001.json").exists());

    let mut second_authority = RejectingAuthority {
        identity: authority.authority_identity().clone(),
    };
    let mut second_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut second_ledger = FacadeLedger::new(ledger_identity.clone());
    let mut second = request(root.clone(), authority_sha256.clone());
    second.max_new_proposals = Some(1);
    generate_generation(
        &second,
        &mut second_authority,
        &mut second_parents,
        &mut second_ledger,
    )
    .expect("second sealed proposal");
    let after_second = fs::read(&ledger_path).expect("advanced public ledger facade");
    assert_ne!(after_second, after_first);
    assert_eq!(
        after_second,
        python_pretty_json_line(&second_ledger.facade(), JsonNewline::Lf)
            .expect("Python pretty ledger")
    );

    fs::remove_file(&ledger_path).expect("remove facade in crash window");
    let mut repair_authority = RejectingAuthority {
        identity: second_authority.authority_identity().clone(),
    };
    let mut repair_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut repair_ledger = FacadeLedger::new(ledger_identity);
    let mut repair = request(root.clone(), authority_sha256);
    repair.max_new_proposals = Some(0);
    generate_generation(
        &repair,
        &mut repair_authority,
        &mut repair_parents,
        &mut repair_ledger,
    )
    .expect("checkpoint must repair missing public facade before continuing");
    assert_eq!(
        fs::read(&ledger_path).expect("repaired public ledger facade"),
        after_second
    );

    fs::write(&ledger_path, &after_first).expect("install stale but self-hashed facade");
    let mut stale_authority = RejectingAuthority {
        identity: repair_authority.authority_identity().clone(),
    };
    let mut stale_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut stale_ledger =
        FacadeLedger::new(object([("ledger", Value::String("facade".to_owned()))]));
    let mut stale_resume = request(
        root.clone(),
        canonical_sha256(stale_authority.authority_identity()).expect("hash authority"),
    );
    stale_resume.max_new_proposals = Some(0);
    generate_generation(
        &stale_resume,
        &mut stale_authority,
        &mut stale_parents,
        &mut stale_ledger,
    )
    .expect("checkpoint must repair stale public facade before continuing");
    assert_eq!(
        fs::read(&ledger_path).expect("stale facade repaired"),
        after_second
    );

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn cp0_exact_general_identity_ledger_upgrades_once_to_python_pair_facade() {
    let root = temp_root("cp0-general-ledger-upgrade");
    let authority_identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&authority_identity).expect("hash authority");
    let ledger_identity = object([("ledger", Value::String("facade".to_owned()))]);
    let expected_pair_ledger = FacadeLedger::new(ledger_identity.clone()).facade();
    write_public_ledger(
        &root,
        &general_ledger_before_pair_initialization(&expected_pair_ledger),
    );

    let mut authority = RejectingAuthority {
        identity: authority_identity,
    };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = FacadeLedger::new(ledger_identity);
    let mut generation_request = request(root.clone(), authority_sha256);
    generation_request.max_new_proposals = Some(0);
    generate_generation(
        &generation_request,
        &mut authority,
        &mut parents,
        &mut ledger,
    )
    .expect("CP0 exact general-to-pair facade transition");

    let persisted = fs::read(root.join("identity-ledger.json")).expect("pair facade persisted");
    assert_eq!(
        persisted,
        python_pretty_json_line(&expected_pair_ledger, JsonNewline::Lf)
            .expect("Python-compatible pair facade bytes")
    );
    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn cp0_general_ledger_upgrades_when_verified_parent_bootstrap_appends_records() {
    let root = temp_root("cp0-general-ledger-archive-bootstrap-upgrade");
    let authority_identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&authority_identity).expect("hash authority");
    let ledger_identity = object([("ledger", Value::String("facade".to_owned()))]);
    let historical_record = fixture_record('a');
    let expected_pair_ledger =
        FacadeLedger::with_archive_records(ledger_identity.clone(), vec![historical_record])
            .facade();
    write_public_ledger(
        &root,
        &general_ledger_before_archive_bootstrap(&expected_pair_ledger),
    );

    let mut authority = RejectingAuthority {
        identity: authority_identity,
    };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = FacadeLedger::with_archive_records(ledger_identity, vec![fixture_record('a')]);
    let mut generation_request = request(root.clone(), authority_sha256);
    generation_request.max_new_proposals = Some(0);
    generate_generation(
        &generation_request,
        &mut authority,
        &mut parents,
        &mut ledger,
    )
    .expect("CP0 general-to-pair transition may append verified archive records");

    let persisted = fs::read(root.join("identity-ledger.json")).expect("pair facade persisted");
    assert_eq!(
        persisted,
        python_pretty_json_line(&expected_pair_ledger, JsonNewline::Lf)
            .expect("Python-compatible pair facade bytes")
    );
    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn cp0_general_ledger_upgrade_rejects_nonprefix_historical_records() {
    let root = temp_root("cp0-general-ledger-archive-bootstrap-divergence");
    let authority_identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&authority_identity).expect("hash authority");
    let ledger_identity = object([("ledger", Value::String("facade".to_owned()))]);
    let expected_pair_ledger =
        FacadeLedger::with_archive_records(ledger_identity.clone(), vec![fixture_record('a')])
            .facade();
    let mut divergent = general_ledger_before_archive_bootstrap(&expected_pair_ledger);
    let fields = divergent.as_object_mut().expect("general ledger object");
    fields.remove("ledgerSha256");
    fields.insert(
        "records".to_owned(),
        Value::Array(vec![fixture_record('b')]),
    );
    fields.insert(
        "uniqueCounts".to_owned(),
        unique_counts(&[fixture_record('b')]),
    );
    let hash = canonical_sha256(&divergent).expect("rehash divergent predecessor");
    divergent
        .as_object_mut()
        .expect("general ledger object")
        .insert("ledgerSha256".to_owned(), Value::String(hash));
    write_public_ledger(&root, &divergent);

    let mut authority = RejectingAuthority {
        identity: authority_identity,
    };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = FacadeLedger::with_archive_records(ledger_identity, vec![fixture_record('a')]);
    let mut generation_request = request(root.clone(), authority_sha256);
    generation_request.max_new_proposals = Some(0);
    assert!(
        generate_generation(
            &generation_request,
            &mut authority,
            &mut parents,
            &mut ledger,
        )
        .is_err(),
        "a self-hashed but non-prefix general facade is not a Python bootstrap predecessor"
    );
    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn cp0_general_identity_ledger_transition_rejects_divergence_corruption_and_restart() {
    let authority_identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&authority_identity).expect("hash authority");
    let ledger_identity = object([("ledger", Value::String("facade".to_owned()))]);
    let pair_ledger = FacadeLedger::new(ledger_identity.clone()).facade();

    // A self-hashed general facade with any content drift is not the exact CP0
    // predecessor and must not be silently replaced.
    let divergent_root = temp_root("cp0-general-ledger-divergent");
    let mut divergent = general_ledger_before_pair_initialization(&pair_ledger);
    divergent
        .as_object_mut()
        .expect("general facade object")
        .insert("proposals".to_owned(), Value::from(1_u64));
    let mut material = divergent.clone();
    material
        .as_object_mut()
        .expect("general facade object")
        .remove("ledgerSha256");
    let hash = canonical_sha256(&material).expect("rehash divergent general facade");
    divergent
        .as_object_mut()
        .expect("general facade object")
        .insert("ledgerSha256".to_owned(), Value::String(hash));
    write_public_ledger(&divergent_root, &divergent);
    let mut authority = RejectingAuthority {
        identity: authority_identity.clone(),
    };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = FacadeLedger::new(ledger_identity.clone());
    let mut divergent_request = request(divergent_root.clone(), authority_sha256.clone());
    divergent_request.max_new_proposals = Some(0);
    assert!(
        generate_generation(
            &divergent_request,
            &mut authority,
            &mut parents,
            &mut ledger,
        )
        .is_err()
    );
    fs::remove_dir_all(&divergent_root).expect("remove exact divergent root");

    // A corrupt general predecessor is never upgraded.
    let corrupt_root = temp_root("cp0-general-ledger-corrupt");
    let mut corrupt = general_ledger_before_pair_initialization(&pair_ledger);
    corrupt
        .as_object_mut()
        .expect("general facade object")
        .insert(
            "ledgerSha256".to_owned(),
            Value::String("sha256:".to_owned() + &"0".repeat(64)),
        );
    write_public_ledger(&corrupt_root, &corrupt);
    let mut authority = RejectingAuthority {
        identity: authority_identity.clone(),
    };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = FacadeLedger::new(ledger_identity.clone());
    let mut corrupt_request = request(corrupt_root.clone(), authority_sha256.clone());
    corrupt_request.max_new_proposals = Some(0);
    assert!(
        generate_generation(&corrupt_request, &mut authority, &mut parents, &mut ledger).is_err()
    );
    fs::remove_dir_all(&corrupt_root).expect("remove exact corrupt root");

    // Once CP0 is checkpointed, putting a valid general facade back is a
    // restart contradiction—not another schema-transition opportunity.
    let restart_root = temp_root("cp0-general-ledger-restart");
    let mut authority = RejectingAuthority {
        identity: authority_identity.clone(),
    };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = FacadeLedger::new(ledger_identity.clone());
    let mut initial_request = request(restart_root.clone(), authority_sha256.clone());
    initial_request.max_new_proposals = Some(0);
    generate_generation(&initial_request, &mut authority, &mut parents, &mut ledger)
        .expect("write CP0 pair checkpoint");
    write_public_ledger(
        &restart_root,
        &general_ledger_before_pair_initialization(&pair_ledger),
    );
    let mut resumed_authority = RejectingAuthority {
        identity: authority_identity,
    };
    let mut resumed_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut resumed_ledger = FacadeLedger::new(ledger_identity);
    let mut resume_request = request(restart_root.clone(), authority_sha256);
    resume_request.max_new_proposals = Some(0);
    assert!(
        generate_generation(
            &resume_request,
            &mut resumed_authority,
            &mut resumed_parents,
            &mut resumed_ledger,
        )
        .is_err()
    );
    fs::remove_dir_all(&restart_root).expect("remove exact restart root");
}

#[test]
fn public_identity_ledger_rejects_divergent_whitespace() {
    let root = temp_root("public-ledger-whitespace");
    let store = ProposalJournal::open(&root, FinalNewline::Lf).expect("open journal");
    let ledger = object([
        (
            "nested",
            Value::Array(vec![Value::from(-0.0), Value::from(1e-7)]),
        ),
        ("text", Value::String("\u{007f} 😀".to_owned())),
    ]);
    store
        .write_public_identity_ledger(&ledger)
        .expect("write pretty ledger");
    let persisted = store
        .read_public_identity_ledger()
        .expect("read exact pretty ledger");
    assert_eq!(
        canonical_sha256(&persisted).expect("persisted identity"),
        canonical_sha256(&ledger).expect("source identity")
    );

    fs::write(
        root.join("identity-ledger.json"),
        format!("{}\n", canonical_json(&ledger).expect("compact JSON")),
    )
    .expect("install divergent compact ledger");
    assert!(
        store
            .read_public_identity_ledger()
            .expect_err("compact whitespace must be rejected")
            .to_string()
            .contains("exact Python pretty JSON")
    );
    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn crlf_public_artifacts_keep_pretty_ledger_and_private_journal_state_lf() {
    let root = temp_root("public-crlf-private-lf");
    let identity = object([("authority", Value::String("test".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let mut authority = RejectingAuthority { identity };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = FacadeLedger::new(object([("ledger", Value::String("facade".to_owned()))]));
    let mut generation_request = request(root.clone(), authority_sha256);
    generation_request.final_newline = FinalNewline::Crlf;

    generate_generation(
        &generation_request,
        &mut authority,
        &mut parents,
        &mut ledger,
    )
    .expect("generate CRLF public artifacts");

    let public_config = fs::read(root.join("pair-config.json")).expect("public pair config");
    assert!(public_config.ends_with(b"\r\n"));
    let public_entry =
        fs::read(root.join("proposal-journal/00000000.json")).expect("public proposal entry");
    assert!(public_entry.ends_with(b"\r\n"));
    let public_ledger = fs::read(root.join("identity-ledger.json")).expect("public ledger");
    assert_eq!(
        public_ledger,
        python_pretty_json_line(&ledger.facade(), JsonNewline::Lf)
            .expect("Python replacement-style LF ledger")
    );
    assert!(public_ledger.ends_with(b"\n"));
    assert!(!public_ledger.windows(2).any(|pair| pair == b"\r\n"));

    for relative in [
        "internal/segments/00000000.json",
        "internal/checkpoints/00000001.json",
    ] {
        let private = fs::read(root.join(relative)).expect("private journal artifact");
        assert!(private.ends_with(b"\n"));
        assert!(!private.ends_with(b"\r\n"));
    }
    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn completed_generation_reopens_with_the_exact_front_result_wrapper() {
    let root = temp_root("completed-front-result");
    let identity = object([("authority", Value::String("materializing".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let ledger_identity = object([("ledger", Value::String("test".to_owned()))]);
    let mut authority = MaterializingAuthority {
        identity,
        predeclared_lake_scope: None,
    };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger =
        CandidateIdentityLedger::new(ledger_identity.clone(), Vec::new()).expect("ledger");
    let first = generate_generation(
        &request(root.clone(), authority_sha256.clone()),
        &mut authority,
        &mut parents,
        &mut ledger,
    )
    .expect("completed generation");
    assert_eq!(first["completed"], Value::Bool(true));
    assert_eq!(
        first["pairGenerationResult"]["schemaVersion"],
        Value::String("temporal_qd_pair_generation_result_v1".to_owned())
    );

    let mut reopened_authority = MaterializingAuthority {
        identity: authority.authority_identity().clone(),
        predeclared_lake_scope: None,
    };
    let mut reopened_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut reopened_ledger =
        CandidateIdentityLedger::new(ledger_identity, Vec::new()).expect("ledger");
    let reopened = generate_generation(
        &request(root.clone(), authority_sha256),
        &mut reopened_authority,
        &mut reopened_parents,
        &mut reopened_ledger,
    )
    .expect("reopen completed generation");
    assert_eq!(reopened, first);

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn scope_rejection_omits_identity_checks_from_the_public_proposal_journal() {
    let root = temp_root("scope-rejection-journal-shape");
    let identity = object([("authority", Value::String("materializing".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let mut authority = MaterializingAuthority {
        identity,
        predeclared_lake_scope: Some(object([("acceptable", Value::Bool(false))])),
    };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = CandidateIdentityLedger::new(
        object([("ledger", Value::String("scope-rejection".to_owned()))]),
        Vec::new(),
    )
    .expect("ledger");

    generate_generation(
        &request(root.clone(), authority_sha256),
        &mut authority,
        &mut parents,
        &mut ledger,
    )
    .expect("generate scope-rejected materialization");

    let entry: Value = serde_json::from_slice(
        &fs::read(root.join("proposal-journal/00000000.json")).expect("read proposal journal"),
    )
    .expect("parse proposal journal");
    assert_eq!(
        entry["disposition"],
        Value::String("predeclared_lake_scope_rejected".to_owned())
    );
    assert!(
        entry.get("identityChecks").is_none(),
        "scope rejection must happen before public identity-check evidence"
    );

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn local_pair_duplicate_omits_identity_checks_before_the_ledger_boundary() {
    let root = temp_root("local-pair-duplicate-journal-shape");
    let identity = object([("authority", Value::String("duplicate-pair".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let mut authority = DuplicatePairAuthority {
        inner: MaterializingAuthority {
            identity,
            predeclared_lake_scope: None,
        },
        executable_semantic_sha256:
            "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd".to_owned(),
    };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = CandidateIdentityLedger::new(
        object([("ledger", Value::String("local-pair-duplicate".to_owned()))]),
        Vec::new(),
    )
    .expect("ledger");
    let mut generation_request = request(root.clone(), authority_sha256);
    generation_request.target_unique_candidates = 2;
    generation_request.max_new_proposals = Some(2);

    generate_generation(
        &generation_request,
        &mut authority,
        &mut parents,
        &mut ledger,
    )
    .expect("generate accepted candidate and local duplicate");

    let entry: Value = serde_json::from_slice(
        &fs::read(root.join("proposal-journal/00000001.json"))
            .expect("read duplicate proposal journal"),
    )
    .expect("parse duplicate proposal journal");
    assert_eq!(
        entry["disposition"],
        Value::String("duplicate_pair_genome".to_owned())
    );
    assert!(
        entry.get("identityChecks").is_none(),
        "generation-local duplicate rejection must precede public ledger checks"
    );
    assert_eq!(
        ledger.compact_state()["proposalCount"],
        Value::from(2_u64),
        "both accepted and locally rejected proposals must commit receipts"
    );

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn operation_rejection_omits_identity_checks_but_commits_its_ledger_receipt() {
    let root = temp_root("operation-rejection-journal-shape");
    let identity = object([("authority", Value::String("rejecting".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let mut authority = RejectingAuthority { identity };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger = CandidateIdentityLedger::new(
        object([("ledger", Value::String("operation-rejection".to_owned()))]),
        Vec::new(),
    )
    .expect("ledger");

    generate_generation(
        &request(root.clone(), authority_sha256),
        &mut authority,
        &mut parents,
        &mut ledger,
    )
    .expect("generate operation rejection");

    let entry: Value = serde_json::from_slice(
        &fs::read(root.join("proposal-journal/00000000.json")).expect("read proposal journal"),
    )
    .expect("parse proposal journal");
    assert_eq!(
        entry["disposition"],
        Value::String("operation_rejected".to_owned())
    );
    assert!(
        entry.get("identityChecks").is_none(),
        "non-materialized operation rejection must not publish identity checks"
    );
    assert_eq!(
        entry["entrySha256"],
        Value::String(
            "sha256:b85d3d11a04395a6646961ac557f7931751d00529da6e825e653c8e0ba2cce27".to_owned(),
        ),
        "the public operation-rejection entry is an exact journal fixture"
    );
    assert_eq!(
        ledger.compact_state()["proposalCount"],
        Value::from(1_u64),
        "the private ledger receipt must still commit"
    );

    fs::remove_dir_all(&root).expect("remove exact test root");
}

#[test]
fn resume_refuses_a_self_hashed_rich_entry_that_diverges_from_its_segment() {
    let root = temp_root("rich-entry-segment-forgery");
    let identity = object([("authority", Value::String("materializing".to_owned()))]);
    let authority_sha256 = canonical_sha256(&identity).expect("hash authority");
    let ledger_identity = object([("ledger", Value::String("test".to_owned()))]);
    let mut authority = MaterializingAuthority {
        identity,
        predeclared_lake_scope: None,
    };
    let mut parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut ledger =
        CandidateIdentityLedger::new(ledger_identity.clone(), Vec::new()).expect("ledger");
    let mut initial = request(root.clone(), authority_sha256.clone());
    initial.target_unique_candidates = 2;
    initial.max_new_proposals = Some(1);
    generate_generation(&initial, &mut authority, &mut parents, &mut ledger)
        .expect("one accepted rich entry without completion");

    let entry_path = root.join("proposal-journal/00000000.json");
    let mut forged: Value =
        serde_json::from_slice(&fs::read(&entry_path).expect("read entry")).expect("entry JSON");
    forged
        .as_object_mut()
        .expect("entry object")
        .insert("forgedButSelfHashed".to_owned(), Value::Bool(true));
    forged
        .as_object_mut()
        .expect("entry object")
        .remove("entrySha256");
    let entry_sha256 = canonical_sha256(&forged).expect("rehash forged rich entry");
    forged
        .as_object_mut()
        .expect("entry object")
        .insert("entrySha256".to_owned(), Value::String(entry_sha256));
    fs::write(
        &entry_path,
        format!(
            "{}\n",
            canonical_json(&forged).expect("canonical rich entry")
        ),
    )
    .expect("install self-hashed rich entry forgery");

    let mut resumed_authority = MaterializingAuthority {
        identity: authority.authority_identity().clone(),
        predeclared_lake_scope: None,
    };
    let mut resumed_parents = ExplicitParentRing::new(Vec::new()).expect("empty parent ring");
    let mut resumed_ledger =
        CandidateIdentityLedger::new(ledger_identity, Vec::new()).expect("ledger");
    let mut continuation = request(root.clone(), authority_sha256);
    continuation.target_unique_candidates = 2;
    continuation.max_new_proposals = Some(0);
    assert!(
        generate_generation(
            &continuation,
            &mut resumed_authority,
            &mut resumed_parents,
            &mut resumed_ledger,
        )
        .is_err()
    );

    fs::remove_dir_all(&root).expect("remove exact test root");
}
