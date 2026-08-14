    use std::collections::BTreeMap;
    use std::io::Read;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use flate2::read::GzDecoder;
    use temporal_qd_kernel::v5_publication::V5G0PublicationInputs;
    use temporal_qd_kernel::v5_transaction::{
        V5G0TransactionRequest, execute_v5_g0_transaction,
    };

    use super::*;

    fn object(rows: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
        Value::Object(
            rows.into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        )
    }

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Result<Self> {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("read test clock")?
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "temporal-qd-fast-ephemeral-{label}-{}-{nonce}",
                std::process::id()
            ));
            fs::create_dir_all(&path)
                .with_context(|| format!("create test root: {}", path.display()))?;
            Ok(Self(path))
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn shared_authority_fixture() -> Value {
        let compressed = include_bytes!(
            "../../../../../tests/fixtures/temporal_qd_v5_shared_authority_oracle.json.gz"
        );
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut payload = Vec::new();
        decoder
            .read_to_end(&mut payload)
            .expect("decompress v5 shared-authority fixture");
        let fixture: Value =
            serde_json::from_slice(&payload).expect("parse v5 shared-authority fixture");
        fixture
            .get("sealedAuthority")
            .cloned()
            .expect("fixture has sealed authority")
    }

    fn fixture_generation_config(target_accepted: u64, max_attempts: u64) -> Value {
        let shared = shared_authority_fixture();
        let authority = shared
            .get("authority")
            .expect("shared authority has payload");
        let evolvable = authority
            .get("evolvableModuleAuthority")
            .expect("shared authority has evolvable authority");
        let archive = evolvable
            .get("archivePolicyAuthority")
            .expect("evolvable authority has archive policy")
            .clone();
        let behavior = evolvable
            .get("behaviorAttributionRequirement")
            .expect("evolvable authority has behavior requirement")
            .clone();
        let behavior_sha256 = behavior
            .get("requirementSha256")
            .and_then(Value::as_str)
            .expect("behavior requirement has identity")
            .to_owned();
        let mut operator = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_evolvable_module_operator_implementation_v1".to_owned()),
            ),
            (
                "authoritySha256",
                evolvable
                    .get("authoritySha256")
                    .expect("evolvable authority identity")
                    .clone(),
            ),
            (
                "programKind",
                evolvable
                    .get("programKind")
                    .expect("evolvable program kind")
                    .clone(),
            ),
            (
                "codec",
                evolvable.get("codec").expect("evolvable codec").clone(),
            ),
            (
                "compilerPolicySha256",
                evolvable
                    .get("compilerPolicySha256")
                    .expect("evolvable compiler identity")
                    .clone(),
            ),
            (
                "operatorRegistry",
                evolvable
                    .get("operatorRegistry")
                    .expect("evolvable registry")
                    .clone(),
            ),
            (
                "budget",
                evolvable.get("budget").expect("evolvable budget").clone(),
            ),
            (
                "capacityContract",
                evolvable
                    .get("capacityContract")
                    .expect("evolvable capacity contract")
                    .clone(),
            ),
            (
                "archivePolicyAuthoritySha256",
                Value::String(canonical_sha256(&archive).expect("archive identity")),
            ),
            (
                "behaviorAttributionRequirementSha256",
                Value::String(behavior_sha256),
            ),
        ]);
        if let Some(receipt) = evolvable.get("capacityReceipt") {
            operator
                .as_object_mut()
                .expect("operator implementation is object")
                .insert(
                    "capacityReceiptSha256".to_owned(),
                    receipt
                        .get("semanticReceiptSha256")
                        .expect("capacity receipt semantic identity")
                        .clone(),
                );
        }
        let operator_sha256 =
            canonical_sha256(&operator).expect("operator implementation identity");
        operator
            .as_object_mut()
            .expect("operator implementation is object")
            .insert(
                "operatorImplementationSha256".to_owned(),
                Value::String(operator_sha256),
            );
        let allocation_semantic = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_reproduction_allocation_v2".to_owned()),
            ),
            ("targetAcceptedCandidates", Value::from(target_accepted)),
            ("desiredAcceptedOffspringCount", Value::from(0_u64)),
            (
                "desiredAcceptedImmigrantCount",
                Value::from(target_accepted),
            ),
        ]);
        let mut allocation = allocation_semantic
            .as_object()
            .expect("allocation is object")
            .clone();
        allocation.insert(
            "allocationSha256".to_owned(),
            Value::String(canonical_sha256(&allocation_semantic).expect("allocation identity")),
        );
        let mut run_config = object([
            ("archivePolicyAuthority", archive),
            ("behaviorAttributionRequirement", behavior),
            ("operatorImplementation", operator.clone()),
        ]);
        if let Some(receipt) = evolvable.get("capacityReceipt") {
            run_config
                .as_object_mut()
                .expect("generation run config is object")
                .insert("capacityReceipt".to_owned(), receipt.clone());
        }
        let mut config = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_generation_v2".to_owned()),
            ),
            ("generationIndex", Value::from(1_u64)),
            ("targetUniqueCandidates", Value::from(target_accepted)),
            ("maxProposalAttempts", Value::from(max_attempts)),
            ("reproductionAllocation", Value::Object(allocation)),
            ("runConfig", run_config),
            ("operatorImplementation", operator),
        ]);
        let config_sha256 = canonical_sha256(&config).expect("generation config identity");
        config
            .as_object_mut()
            .expect("generation config is object")
            .insert("configSha256".to_owned(), Value::String(config_sha256));
        config
    }

    fn request(target_accepted: u64, evaluation_width: u64) -> V5G0TransactionRequest {
        let generation_config = fixture_generation_config(target_accepted, target_accepted);
        let generation_config_sha256 = generation_config
            .get("configSha256")
            .and_then(Value::as_str)
            .expect("fixture config identity")
            .to_owned();
        V5G0TransactionRequest {
            shared_authority: shared_authority_fixture(),
            generation_config,
            generation_config_sha256,
            generation_index: 1,
            target_accepted,
            max_attempts: target_accepted,
            evaluation_width,
            thread_cap: 1,
            publication_inputs: V5G0PublicationInputs {
                final_newline: "lf".to_owned(),
                execution_authority: Value::Object(Map::new()),
                inputs: object([
                    (
                        "schemaVersion",
                        Value::String("temporal_qd_native_v5_proposal_inputs_v1".to_owned()),
                    ),
                    ("parentArchive", Value::Null),
                    ("identityLedger", Value::Null),
                ]),
            },
        }
    }

    fn fake_sha(label: &str) -> String {
        canonical_sha256(&Value::String(label.to_owned())).expect("fixture SHA-256")
    }

    fn evolved_manifest(frozen_authority: Value, output_root: &Path) -> V5ProposalManifest {
        V5ProposalManifest {
            authority_sha256: fake_sha("authority"),
            execution_authority: Value::Object(Map::new()),
            frozen_authority,
            expected_authority_sha256: fake_sha("expected-authority"),
            output_root: output_root.display().to_string(),
            final_newline: "lf".to_owned(),
            generation_config: Value::Object(Map::new()),
            generation_config_sha256: fake_sha("generation-config"),
            generation_index: 2,
            generation_kind: "evolved".to_owned(),
            requested_count: 2,
            evaluation_population_size: 2,
            max_proposal_attempts: 2,
            thread_cap: 1,
            inputs: Value::Object(Map::new()),
            result_path: "v5-proposal-result.json".to_owned(),
            manifest_sha256: fake_sha("manifest"),
        }
    }

    #[test]
    fn fast_ephemeral_g1_parent_closure_feeds_g2_and_rejects_substitution() -> Result<()> {
        let root = TestRoot::new("g1-g2-parent-closure")?;
        let generation_root = root.0.join("generations/generation-0001");
        let proposal_root = generation_root.join("proposal");
        fs::create_dir_all(&proposal_root)?;

        let request = request(2, 2);
        let frozen_authority = request.shared_authority.clone();
        let transaction =
            execute_v5_g0_transaction(request).context("execute production-shaped G0 fixture")?;
        let publication = publish_selected_g0_parent_objects(&proposal_root, &transaction)?;
        assert_eq!(publication.object_count, 4);
        assert!(publication.encoded_bytes > 0);

        let selected = transaction
            .selected_projection_index
            .as_ref()
            .expect("G0 fixture has selected projections");
        let records = transaction
            .accepted_records
            .iter()
            .map(|record| {
                (
                    record.record_sha256().expect("record identity"),
                    record,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let members = selected
            .projections
            .iter()
            .map(|projection| {
                let record = records
                    .get(&projection.record_sha256)
                    .expect("selected record exists");
                object([(
                    "candidate",
                    object([
                        (
                            "candidateId",
                            Value::String(record.candidate_id.clone()),
                        ),
                        (
                            "proposalEntrySha256",
                            Value::String(projection.record_sha256.clone()),
                        ),
                        (
                            "candidateIdentitySha256",
                            Value::String(record.candidate_identity_sha256.clone()),
                        ),
                        (
                            "programSha256",
                            Value::String(record.compiled.program_sha256.clone()),
                        ),
                        (
                            "sourceProfileSha256",
                            Value::String(record.compiled.raw_pair_sha256.clone()),
                        ),
                        (
                            "profileSnapshotSha256",
                            Value::String(record.compiled.profile_snapshot_sha256.clone()),
                        ),
                    ]),
                )])
            })
            .collect::<Vec<_>>();
        let archive_value = object([(
            "cells",
            Value::Array(vec![object([("members", Value::Array(members))])]),
        )]);
        let finalization_root = generation_root.join("native-finalization");
        fs::create_dir_all(&finalization_root)?;
        let archive_path = finalization_root.join("archive.json");
        fs::write(&archive_path, canonical_json_line(&archive_value)?)?;
        let archive = super::V5EvolvedInputDocument {
            value: archive_value,
            binding_sha256: fake_sha("archive-binding"),
            semantic_sha256: fake_sha("archive-semantic"),
            path: archive_path,
        };
        let manifest = evolved_manifest(frozen_authority, &root.0);

        let parents = super::native_v5_g0_parent_references(&manifest, &archive)
            .context("open G2 parents from fast-ephemeral G1 closure")?;
        assert_eq!(parents.len(), selected.projections.len());
        for record in records.values() {
            assert!(parents.contains_key(&record.candidate_id));
        }

        // The candidate-scale durable workspace is absent. Only the selected
        // record/delta object closure required by the archive remains.
        assert!(!proposal_root.join("v5-native/attempts.jsonl").exists());
        assert!(!proposal_root.join("v5-native/accepted-records.jsonl").exists());
        assert!(!proposal_root.join("v5-native/selected-projections.jsonl").exists());
        assert!(!proposal_root.join("internal").exists());

        let first = records
            .get(&selected.projections[0].record_sha256)
            .expect("first selected record");
        let second = records
            .get(&selected.projections[1].record_sha256)
            .expect("second selected record");
        let first_record_path = proposal_root.join(compact_record_object_relative_path(
            &selected.projections[0].record_sha256,
        )?);
        let second_record_path = proposal_root.join(compact_record_object_relative_path(
            &selected.projections[1].record_sha256,
        )?);
        let first_record_bytes = fs::read(&first_record_path)?;
        fs::write(&first_record_path, fs::read(&second_record_path)?)?;
        assert!(
            super::native_v5_g0_parent_references(&manifest, &archive).is_err(),
            "a valid compact record substituted under another archive identity must fail"
        );
        fs::write(&first_record_path, &first_record_bytes)?;

        let first_delta_path = proposal_root.join(compact_delta_object_relative_path(
            &first.proposal_delta_sha256,
        )?);
        let second_delta_path = proposal_root.join(compact_delta_object_relative_path(
            &second.proposal_delta_sha256,
        )?);
        let first_delta_bytes = fs::read(&first_delta_path)?;
        fs::write(&first_delta_path, fs::read(&second_delta_path)?)?;
        assert!(
            super::native_v5_g0_parent_references(&manifest, &archive).is_err(),
            "a valid compact delta substituted under another record identity must fail"
        );
        fs::write(&first_delta_path, first_delta_bytes)?;
        super::native_v5_g0_parent_references(&manifest, &archive)
            .context("restored fast-ephemeral G1 closure remains usable")?;

        Ok(())
    }
