//! Compile and contract coverage for the value-oriented later-generation v5
//! operator engine.  The module is intentionally not exported from the crate
//! until its factory/bridge integration lands; importing it by path keeps this
//! independently owned test surface buildable in the meantime.

#[path = "../src/v5_operators.rs"]
mod v5_operators;

use std::io::Read;

use flate2::read::GzDecoder;
use serde_json::Value;

fn gzip_fixture(bytes: &[u8]) -> Value {
    let mut decoder = GzDecoder::new(bytes);
    let mut text = String::new();
    decoder
        .read_to_string(&mut text)
        .expect("inflate checked oracle fixture");
    serde_json::from_str(&text).expect("parse checked oracle fixture")
}

fn operator_oracle() -> Value {
    gzip_fixture(include_bytes!(
        "../../../../../tests/fixtures/temporal_qd_v5_operator_python_oracle_corpus.json.gz"
    ))
}

fn shared_authority_oracle() -> Value {
    gzip_fixture(include_bytes!(
        "../../../../../tests/fixtures/temporal_qd_v5_shared_authority_oracle.json.gz"
    ))
}

fn oracle_side_authority(shared: &Value, side: &str) -> v5_operators::V5OperatorAuthority {
    let sealed = &shared["sealedAuthority"]["authority"];
    let side_authority = &sealed[side];
    let native_operator = &sealed["nativeOperatorAuthority"];
    let instrument = side_authority["grammarContext"]["payload"]["context"]["instrument"]
        .as_str()
        .expect("sealed grammar context instrument");
    v5_operators::V5OperatorAuthority::from_sealed_static_parts(
        shared["sealedAuthority"]["authoritySha256"]
            .as_str()
            .expect("sealed authority hash"),
        side,
        instrument,
        &side_authority["budget"],
        &side_authority["catalog"]["payload"]["catalog"],
        &side_authority["indicatorPolicy"],
        &sealed["holdOperatorPolicy"],
        &sealed["initialProtectionOperatorPolicy"],
        &native_operator["temporalDomains"],
    )
    .expect("build sealed operator authority")
    .with_legacy_selection_static(
        side_authority["catalog"]["payload"]["catalogSha256"]
            .as_str()
            .expect("legacy catalog hash"),
        side_authority["resourceOperatorSpecSha256"]
            .as_str()
            .expect("resource operator specification hash"),
        sealed["compilerPolicySha256"]
            .as_str()
            .expect("compiler policy hash"),
        &native_operator["temporalOperatorSpecification"],
        sealed["temporalOperatorSpecSha256"]
            .as_str()
            .expect("temporal operator specification hash"),
    )
    .expect("bind exact legacy selection static authority")
}

/// Recreate the real native G0 side that the compact-parent loader receives.
/// This stays at the public factory seam so the operator contract test does
/// not duplicate the crate-private compact record loader.
fn native_g0_parent_program(shared: &Value, side: &str) -> Value {
    let config_sha256 = temporal_qd_contract::canonical_sha256(&serde_json::json!({
        "schemaVersion": "temporal_qd_v5_evolved_native_mutation_probe_v1"
    }))
    .expect("canonical native G0 mutation probe configuration");
    let proposal_seed = temporal_qd_kernel::v5::v5_proposal_seed(&config_sha256, 11)
        .expect("native G0 proposal seed");
    let sealed = &shared["sealedAuthority"]["authority"];
    let side_authority = &sealed[side];
    temporal_qd_kernel::v5::build_immigrant_module(
        side,
        &proposal_seed,
        &side_authority["grammarContext"]["payload"]["context"],
        &side_authority["budget"],
    )
    .expect("real native G0 side construction")
    .program
}

fn native_g0_multimember_parent(shared: &Value) -> (&'static str, Value) {
    ["long", "short"]
        .into_iter()
        .map(|side| (side, native_g0_parent_program(shared, side)))
        .find(|(_, program)| {
            program["resources"]["evidenceGroups"]
                .as_array()
                .is_some_and(|groups| {
                    groups.iter().any(|group| {
                        group["indicatorInstanceIds"]
                            .as_array()
                            .is_some_and(|members| members.len() > 1)
                    })
                })
        })
        .expect("the native seed-11 G0 pair retains a multi-member fuzzy group")
}

fn oracle_profile_view(
    pair: &Value,
    side: &str,
    authority: &v5_operators::V5OperatorAuthority,
) -> v5_operators::V5CompiledProfileView {
    oracle_module_profile_view(&pair[side], authority)
}

fn oracle_module_profile_view(
    module: &Value,
    authority: &v5_operators::V5OperatorAuthority,
) -> v5_operators::V5CompiledProfileView {
    let identities = &module["identities"];
    v5_operators::V5CompiledProfileView::from_core_compilation(
        &module["program"],
        authority,
        identities["programSha256"]
            .as_str()
            .expect("module program hash")
            .to_owned(),
        module["profile"].clone(),
        identities["profileSha256"]
            .as_str()
            .expect("module profile hash")
            .to_owned(),
        identities["nativeSnapshotSha256"]
            .as_str()
            .expect("native profile snapshot hash")
            .to_owned(),
        identities["nativeProgramSha256"]
            .as_str()
            .expect("native program hash")
            .to_owned(),
        identities["nativeValidationReportSha256"]
            .as_str()
            .expect("native validation report hash")
            .to_owned(),
        module["nativeReport"].clone(),
    )
    .expect("bind native compiled profile to its module")
}

/// The Python fixture proves that its selection parent has no compiler-
/// admitted temporal candidates.  In production this same typed seam is
/// backed by `v5::compile_v5_module_profile`; keep the corpus gate focused on
/// the operator engine's exact wrapper/order behavior without teaching the
/// path-imported support test a second copy of the full compiler.
struct OracleCompiledChildAdmission;

impl v5_operators::V5EvolvedChildAdmission for OracleCompiledChildAdmission {
    fn admit_evolved_child(
        &self,
        operator_id: &str,
        _child_program: &Value,
    ) -> v5_operators::Result<()> {
        if operator_id == v5_operators::V5_TEMPORAL_OPERATOR_ID {
            return Err(v5_operators::V5OperatorError::Invalid(
                "fixture compiler rejects temporal candidate".to_owned(),
            ));
        }
        Ok(())
    }
}

struct OracleSequencePairRecompiler<'a> {
    authority: &'a v5_operators::V5OperatorAuthority,
    expected_step: &'a Value,
}

impl v5_operators::V5EvolvedPairRecompiler for OracleSequencePairRecompiler<'_> {
    fn recompile_evolved_pair(
        &self,
        delta: &v5_operators::V5EvolvedOperatorDelta,
    ) -> v5_operators::Result<v5_operators::V5RecompiledEvolvedPair> {
        let expected = self.expected_step;
        if delta.side != expected["side"].as_str().expect("sequence step side")
            || delta.parent_pair_identity_sha256
                != expected["parentPairIdentitySha256"]
                    .as_str()
                    .expect("sequence step parent pair identity")
            || delta.child_program != expected["changedModule"]["program"]
        {
            return Err(v5_operators::V5OperatorError::Invalid(
                "fixture pair compiler received a different evolved delta".to_owned(),
            ));
        }
        Ok(v5_operators::V5RecompiledEvolvedPair {
            pair_identity_sha256: expected["pairIdentitySha256"]
                .as_str()
                .expect("sequence step child pair identity")
                .to_owned(),
            module_identity_sha256: expected["changedModule"]["identities"]["moduleIdentitySha256"]
                .as_str()
                .expect("sequence step child frozen module identity")
                .to_owned(),
            compiled_profile: oracle_module_profile_view(
                &expected["changedModule"],
                self.authority,
            ),
        })
    }
}

fn fixture_state_for_pair_side(
    pair: &Value,
    side: &str,
    authority: &v5_operators::V5OperatorAuthority,
) -> v5_operators::V5EvolvedSideState {
    v5_operators::V5EvolvedSideState::from_recompiled_pair(
        pair["identities"]["pairIdentitySha256"]
            .as_str()
            .expect("frozen pair identity")
            .to_owned(),
        pair[side]["identities"]["moduleIdentitySha256"]
            .as_str()
            .expect("frozen side module identity")
            .to_owned(),
        authority,
        pair[side]["program"].clone(),
        oracle_module_profile_view(&pair[side], authority),
    )
    .expect("bind fixture pair side to its compiled profile")
}

fn fixture_selection_for_choice(
    state: &v5_operators::V5EvolvedSideState,
    authority: &v5_operators::V5OperatorAuthority,
    legacy_choice: &Value,
) -> v5_operators::V5EvolvedOperatorSelection {
    let choice = v5_operators::enumerate_evolved_operator_choices_with_admission(
        &state.program,
        authority,
        &state.compiled_profile,
        &OracleCompiledChildAdmission,
    )
    .expect("enumerate fixture sequence choices")
    .into_iter()
    .find(|candidate| candidate.legacy_choice == *legacy_choice)
    .expect("Python sequence operation remains an exact current native choice");
    v5_operators::V5EvolvedOperatorSelection {
        native_plan: choice.native_plan,
        legacy_choice: choice.legacy_choice,
        receipt: serde_json::json!({"fixture": "authority_proposal_sequence"}),
    }
}

#[test]
fn operator_module_is_linkable() {
    assert_eq!(
        v5_operators::V5_OPERATOR_PLAN_SCHEMA,
        "temporal_qd_v5_operator_plan_v1"
    );
}

#[test]
fn real_stopped_run_program_passes_the_evolved_genome_boundary() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/temporal_qd_v5_stopped_run_oracle.json"
    ))
    .expect("parse v5 stopped-run fixture");
    let program = &fixture["construction"]["sides"]["long"]["program"];
    v5_operators::validate_v5_operator_program(program)
        .expect("real native G0 program passes evolved boundary");
}

#[test]
fn real_g0_multimember_catalog_weights_admit_before_operator_selection() {
    let shared = shared_authority_oracle();
    let (side, program) = native_g0_multimember_parent(&shared);
    let authority = oracle_side_authority(&shared, side);
    let group = program["resources"]["evidenceGroups"]
        .as_array()
        .expect("native G0 evidence groups")
        .iter()
        .find(|group| {
            group["indicatorInstanceIds"]
                .as_array()
                .is_some_and(|members| members.len() > 1)
        })
        .expect("seed 11 must retain the real multi-member G0 group");
    let member_ids = group["indicatorInstanceIds"]
        .as_array()
        .expect("native G0 group members")
        .iter()
        .map(|member| member.as_str().expect("native G0 member ID"))
        .collect::<Vec<_>>();
    let weights = program["resources"]["indicators"]
        .as_array()
        .expect("native G0 indicators")
        .iter()
        .filter(|indicator| {
            indicator["meta"]["instanceId"]
                .as_str()
                .is_some_and(|id| member_ids.contains(&id))
        })
        .map(|indicator| {
            indicator["config"]["weight"]
                .as_f64()
                .expect("native G0 catalog weight")
        })
        .collect::<Vec<_>>();
    assert_eq!(weights.len(), member_ids.len());
    assert!(
        weights.iter().all(|weight| *weight == 1.0),
        "the factory must retain the sealed catalog/default weights verbatim"
    );
    assert!(
        weights.iter().sum::<f64>() > 1.0,
        "this is the preserved unnormalized factory representation"
    );

    v5_operators::validate_program(&program, &authority)
        .expect("real G0 parent must admit before any operator selection");
    let plans = v5_operators::enumerate_operator_plans(&program, &authority)
        .expect("admitted real G0 parent exposes its exact operator surface");
    let weight_plan = plans
        .iter()
        .find(|plan| {
            plan["operatorId"] == v5_operators::V5_RESOURCE_OPERATOR_ID
                && plan["construction"]["kind"] == "evidence_weight_mutate"
        })
        .expect("real multi-member G0 parent exposes a weight mutation");
    v5_operators::apply_operator_plan(&program, &authority, weight_plan)
        .expect("the exact normalized resource plan remains applicable");
}

#[test]
fn tampered_weight_application_with_wrong_sum_is_rejected_before_adoption() {
    let shared = shared_authority_oracle();
    let (side, program) = native_g0_multimember_parent(&shared);
    let authority = oracle_side_authority(&shared, side);
    let mut tampered = v5_operators::enumerate_operator_plans(&program, &authority)
        .expect("real G0 operator plans")
        .into_iter()
        .find(|plan| {
            plan["operatorId"] == v5_operators::V5_RESOURCE_OPERATOR_ID
                && plan["construction"]["kind"] == "evidence_weight_mutate"
        })
        .expect("real G0 weight plan");
    let after = tampered["construction"]["afterWeights"]
        .as_object_mut()
        .expect("weight plan after map");
    for value in after.values_mut() {
        *value = serde_json::json!(0.25);
    }
    let plan = tampered.as_object_mut().expect("operator plan object");
    plan.remove("planSha256").expect("operator plan identity");
    let tampered_sha256 = temporal_qd_contract::canonical_sha256(&tampered)
        .expect("rehash deliberately tampered operator plan");
    tampered
        .as_object_mut()
        .expect("operator plan object")
        .insert("planSha256".to_owned(), Value::String(tampered_sha256));

    let execution = v5_operators::execute_operator_plan(&program, &authority, &tampered)
        .expect("tampered resource application becomes a typed outcome");
    assert_eq!(
        execution.disposition,
        v5_operators::V5OperatorDisposition::Rejected
    );
    assert_eq!(
        execution.result.value["reasonCode"], "operator_rejected",
        "a self-rehashed wrong-sum resource construction is not currently applicable"
    );
    assert!(execution.application.is_none());
}

#[test]
fn evolved_selection_uses_all_152_exact_python_legacy_choice_wrappers() {
    let corpus = operator_oracle();
    assert_eq!(
        corpus["selection"]["legacyChoiceOrderingSha256"],
        "sha256:d19e7fd730df0b09348ff6890974d956871e91aea363a9c13b782415cf8a9cb6",
        "the fixture must remain the corrected Python wrapper-order corpus"
    );
    let shared = shared_authority_oracle();
    let pair = &corpus["selection"]["selectionParentFrozenPair"];
    let authority = oracle_side_authority(&shared, "long");
    let profile = oracle_profile_view(pair, "long", &authority);
    let actual = v5_operators::enumerate_evolved_operator_choices_with_admission(
        &pair["long"]["program"],
        &authority,
        &profile,
        &OracleCompiledChildAdmission,
    )
    .expect("enumerate exact evolved choices");
    let actual_wrappers = Value::Array(
        actual
            .iter()
            .map(|choice| choice.legacy_choice.clone())
            .collect(),
    );
    assert_eq!(actual.len(), 152, "all Python choices must remain admitted");
    assert_eq!(
        actual_wrappers, corpus["selection"]["orderedChoices"],
        "native selection must use byte-for-byte Python legacy choice wrappers"
    );
    assert_eq!(
        actual[0].legacy_choice_ordering_sha256,
        corpus["selection"]["legacyChoiceOrderingSha256"]
            .as_str()
            .expect("fixture ordering hash"),
    );
}

#[test]
fn evolved_fixed_seed_selection_receipts_match_python_oracle() {
    let corpus = operator_oracle();
    assert_eq!(
        corpus["corpusSha256"],
        "sha256:c362bac1a60a879b677a92934a664bb19d4f04d55a64206a21eed9471ba6c96e",
        "this gate consumes the current production-selection/crossover corpus"
    );
    let shared = shared_authority_oracle();
    let pair = &corpus["selection"]["selectionParentFrozenPair"];
    let authority = oracle_side_authority(&shared, "long");
    let profile = oracle_profile_view(pair, "long", &authority);
    let parent_identity = corpus["selection"]["parentPairIdentitySha256"]
        .as_str()
        .expect("selection parent pair identity");

    for transcript in corpus["selection"]["transcripts"]
        .as_array()
        .expect("fixed-seed selection transcripts")
    {
        let seed = transcript["seed"].as_str().expect("selection seed");
        let selection = v5_operators::select_evolved_operator_choice_with_admission(
            seed,
            parent_identity,
            &pair["long"]["program"],
            &authority,
            &profile,
            &OracleCompiledChildAdmission,
        )
        .expect("select exact Python legacy choice");

        assert_eq!(selection.legacy_choice, transcript["selectedChoice"]);
        assert_eq!(
            temporal_qd_contract::canonical_sha256(&selection.legacy_choice)
                .expect("hash selected legacy choice"),
            transcript["selectedChoiceSha256"]
                .as_str()
                .expect("selected choice hash"),
        );
        assert_eq!(
            selection.legacy_choice["kind"], transcript["selectedKind"],
            "family draw for {seed} drifted"
        );
        assert_eq!(
            temporal_qd_contract::canonical_sha256(
                selection
                    .legacy_choice
                    .get("plan")
                    .unwrap_or(&selection.legacy_choice),
            )
            .expect("hash semantic selected operation"),
            transcript["selectedSemanticOperationSha256"]
                .as_str()
                .expect("selected semantic operation hash"),
        );

        assert_eq!(
            selection.receipt["schemaVersion"],
            "temporal_qd_v5_evolved_operator_selection_v1"
        );
        assert_eq!(selection.receipt["proposalSeed"], seed);
        assert_eq!(selection.receipt["parentIdentitySha256"], parent_identity);
        assert_eq!(selection.receipt["family"], transcript["selectedKind"]);
        assert_eq!(
            selection.receipt["legacyChoice"],
            transcript["selectedChoice"]
        );
        assert_eq!(
            selection.receipt["legacyChoiceSha256"],
            transcript["selectedChoiceSha256"]
        );
        assert_eq!(
            selection.receipt["legacyChoiceOrderingSha256"],
            corpus["selection"]["legacyChoiceOrderingSha256"]
        );
        assert_eq!(
            selection.receipt["nativePlanSha256"],
            selection.native_plan["planSha256"]
        );
        let mut unsigned_receipt = selection.receipt.clone();
        let receipt_sha = unsigned_receipt
            .as_object_mut()
            .expect("selection receipt object")
            .remove("selectionSha256")
            .expect("selection receipt identity");
        assert_eq!(
            receipt_sha,
            temporal_qd_contract::canonical_sha256(&unsigned_receipt)
                .expect("rehash evolved selection receipt"),
            "selection receipt must bind the exact draw inputs and wrapper"
        );
    }
}

#[test]
fn evolved_depth_sequences_recompile_and_reidentify_before_every_next_step() {
    let corpus = operator_oracle();
    let shared = shared_authority_oracle();
    let sequences = corpus["authorityTranscripts"]
        .as_array()
        .expect("authority transcript matrix")
        .iter()
        .filter(|row| row["kind"] == "proposal_sequence")
        .collect::<Vec<_>>();
    assert_eq!(
        sequences.len(),
        3,
        "fixture must retain depth 1/2/3 witnesses"
    );

    for sequence in sequences {
        let expected_depth = sequence["mutationDepth"]
            .as_u64()
            .expect("sequence mutation depth") as usize;
        let steps = sequence["proposal"]["mutationSteps"]
            .as_array()
            .expect("sequence mutation steps");
        assert_eq!(steps.len(), expected_depth);
        assert_eq!(
            sequence["parentPairIdentitySha256"],
            sequence["parentPair"]["identities"]["pairIdentitySha256"]
        );

        for (ordinal, expected_step) in steps.iter().enumerate() {
            let side = expected_step["side"].as_str().expect("sequence side");
            let authority = oracle_side_authority(&shared, side);
            let state = fixture_state_for_pair_side(&expected_step["parentPair"], side, &authority);
            assert_eq!(
                state.pair_identity_sha256,
                expected_step["parentPairIdentitySha256"],
                "depth {expected_depth}, step {} did not begin from the immediately prior pair",
                ordinal + 1,
            );
            let stale_profile = state.compiled_profile.clone();
            let selection =
                fixture_selection_for_choice(&state, &authority, &expected_step["operation"]);
            let recompiler = OracleSequencePairRecompiler {
                authority: &authority,
                expected_step,
            };
            let result = v5_operators::execute_evolved_operator_step_from_state(
                &state,
                &authority,
                selection,
                &OracleCompiledChildAdmission,
                &recompiler,
            )
            .expect("execute and recompile exact authority sequence step");

            assert_eq!(
                result.disposition,
                v5_operators::V5OperatorDisposition::Accepted,
                "depth {expected_depth}, step {} must compile as a pair before continuation",
                ordinal + 1,
            );
            assert_eq!(result.reason_code, "accepted");
            let delta = result.delta.as_ref().expect("accepted evolved delta");
            assert_eq!(
                delta.parent_pair_identity_sha256,
                state.pair_identity_sha256
            );
            assert_eq!(
                delta.child_program,
                expected_step["changedModule"]["program"]
            );
            assert_eq!(
                delta.child_program_sha256,
                expected_step["changedModule"]["identities"]["programSha256"]
            );
            assert!(
                delta.trace.is_array() || delta.trace.is_object(),
                "accepted operator delta must retain a structured semantic trace"
            );
            assert_eq!(
                result
                    .operator_execution
                    .application
                    .as_ref()
                    .expect("accepted execution retains application")
                    .audit["mutationTrace"],
                delta.trace,
                "the typed delta must retain the exact application trace"
            );
            let next = result
                .next_side_state
                .as_ref()
                .expect("accepted pair recompilation yields next state");
            assert_eq!(
                next.pair_identity_sha256,
                expected_step["pairIdentitySha256"]
            );
            assert_eq!(
                next.module_identity_sha256,
                expected_step["changedModule"]["identities"]["moduleIdentitySha256"],
                "every recompiled step must carry the new frozen module identity"
            );
            assert_ne!(next.pair_identity_sha256, state.pair_identity_sha256);
            assert_eq!(next.program, expected_step["changedModule"]["program"]);
            assert_eq!(
                next.compiled_profile.genome_program_sha256(),
                expected_step["changedModule"]["identities"]["programSha256"]
                    .as_str()
                    .expect("compiled changed program identity"),
            );

            // A later step must not reuse the preceding compiled profile,
            // even when the pair identity and program are otherwise current.
            let stale = v5_operators::select_evolved_operator_choice_with_admission(
                "stale-compiled-profile-regression",
                &next.pair_identity_sha256,
                &next.program,
                &authority,
                &stale_profile,
                &OracleCompiledChildAdmission,
            );
            assert!(
                stale.is_err(),
                "depth {expected_depth}, step {} accepted a stale compiled profile",
                ordinal + 1,
            );
            assert!(
                v5_operators::select_evolved_operator_choice_from_state(
                    "fresh-compiled-profile-regression",
                    next,
                    &authority,
                    &OracleCompiledChildAdmission,
                )
                .is_ok(),
                "the freshly recompiled/reidentified state must remain selectable"
            );
        }
        assert_eq!(
            steps.last().expect("nonempty sequence")["pairIdentitySha256"],
            sequence["childPairIdentitySha256"],
            "every sequence must expose its final pair identity after its final recompile",
        );
    }
}

#[test]
fn same_side_crossover_replays_all_real_distinct_parent_python_transcripts() {
    let corpus = operator_oracle();
    assert_eq!(
        corpus["corpusSha256"],
        "sha256:c362bac1a60a879b677a92934a664bb19d4f04d55a64206a21eed9471ba6c96e",
        "this gate consumes the current production crossover corpus"
    );
    let shared = shared_authority_oracle();
    let transcripts = corpus["authorityTranscripts"]
        .as_array()
        .expect("authority transcript matrix")
        .iter()
        .filter(|row| row["kind"] == "same_side_crossover_distinct")
        .collect::<Vec<_>>();
    assert_eq!(
        transcripts.len(),
        3,
        "fixture must retain all three real ports"
    );
    assert_eq!(
        transcripts
            .iter()
            .map(|row| row["port"].as_str().expect("crossover port"))
            .collect::<Vec<_>>(),
        ["entry_setup", "management_hub", "exit_hub"],
        "fixture must cover every production crossover port"
    );

    for transcript in transcripts {
        let side = transcript["projection"]["side"]
            .as_str()
            .expect("crossover side");
        let authority = oracle_side_authority(&shared, side);
        assert_ne!(
            transcript["parentPairIdentitySha256"], transcript["matePairIdentitySha256"],
            "this is a distinct-pair production witness"
        );
        let parent = fixture_state_for_pair_side(&transcript["parentPair"], side, &authority);
        let mate = fixture_state_for_pair_side(&transcript["matePair"], side, &authority);
        let selected = v5_operators::select_evolved_same_side_crossover_from_states(
            transcript["proposalSeed"]
                .as_str()
                .expect("crossover proposal seed"),
            &parent,
            &mate,
            &authority,
        )
        .expect("select real exact crossover plan");
        assert_eq!(selected.side, side);
        assert_eq!(
            selected.selection, transcript["selection"],
            "selection must retain the exact Python parent ordering and two draw receipts"
        );
        assert_eq!(
            selected.recipient_module_identity_sha256,
            transcript["projection"]["recipientFrozenModuleIdentitySha256"]
        );
        assert_eq!(
            selected.donor_module_identity_sha256,
            transcript["projection"]["donorFrozenModuleIdentitySha256"]
        );
        let mut expected_plan = transcript["plan"].clone();
        let expected_plan_sha = expected_plan
            .as_object_mut()
            .expect("fixture crossover plan")
            .remove("planSha256")
            .expect("fixture crossover plan identity");
        assert_eq!(selected.native_plan, expected_plan);
        assert_eq!(
            temporal_qd_contract::canonical_sha256(&selected.native_plan)
                .expect("hash exact selected crossover plan"),
            expected_plan_sha,
        );
        let execution = v5_operators::execute_evolved_same_side_crossover_from_states(
            transcript["proposalSeed"]
                .as_str()
                .expect("crossover proposal seed"),
            &parent,
            &mate,
            &authority,
            selected.clone(),
        );
        assert_eq!(
            execution.disposition,
            v5_operators::V5OperatorDisposition::Accepted
        );
        assert_eq!(execution.reason_code, "accepted");
        assert_eq!(
            execution
                .selection
                .as_ref()
                .expect("accepted crossover retains selection")
                .selection,
            transcript["selection"]
        );
        let applied = execution
            .application
            .as_ref()
            .expect("accepted crossover retains application");
        let delta = execution
            .delta
            .as_ref()
            .expect("accepted crossover retains delta");
        assert_eq!(
            applied.child_program,
            transcript["projection"]["childProgram"]
        );
        assert_eq!(
            temporal_qd_contract::canonical_sha256(&applied.child_program)
                .expect("hash crossover child"),
            transcript["projection"]["childProgramSha256"]
        );
        assert_eq!(applied.audit["semanticDelta"], transcript["application"]);
        assert_eq!(delta.trace, transcript["application"]);
        assert_eq!(
            delta.child_program,
            transcript["projection"]["childProgram"]
        );
        assert_eq!(
            delta.child_program_sha256,
            transcript["projection"]["childProgramSha256"]
        );

        // Neither the compiled profile nor the selected two-parent identity
        // may survive a changed state.  The first real long witness is enough
        // to prove both regressions without inventing a synthetic parent.
        if transcript["port"] == "entry_setup" {
            let mut stale_profile_parent = parent.clone();
            stale_profile_parent.program = mate.program.clone();
            assert!(
                v5_operators::select_evolved_same_side_crossover_from_states(
                    transcript["proposalSeed"]
                        .as_str()
                        .expect("crossover proposal seed"),
                    &stale_profile_parent,
                    &mate,
                    &authority,
                )
                .is_err(),
                "crossover selection must reject a stale compiled profile"
            );
            let mut stale_selection = selected;
            stale_selection.recipient_pair_identity_sha256 =
                stale_selection.donor_pair_identity_sha256.clone();
            let stale_execution = v5_operators::execute_evolved_same_side_crossover_from_states(
                transcript["proposalSeed"]
                    .as_str()
                    .expect("crossover proposal seed"),
                &parent,
                &mate,
                &authority,
                stale_selection,
            );
            assert_eq!(
                stale_execution.disposition,
                v5_operators::V5OperatorDisposition::Rejected,
                "crossover execution must reject a stale parent identity"
            );
            assert_eq!(stale_execution.reason_code, "crossover_rejected");
            assert!(stale_execution.delta.is_none());
        }
    }
}

#[test]
fn same_side_crossover_real_terminal_rejection_stays_typed_and_replayable() {
    let corpus = operator_oracle();
    let shared = shared_authority_oracle();
    let terminal = corpus["authorityTranscripts"]
        .as_array()
        .expect("authority transcript matrix")
        .iter()
        .find(|row| row["kind"] == "same_side_crossover_terminal")
        .expect("real terminal crossover transcript");
    assert_eq!(terminal["terminalDisposition"], "operation_rejected");
    assert_eq!(
        terminal["proposalSeed"],
        "stopped-v5-authority-crossover-rejection-7-long-1"
    );
    assert_eq!(
        terminal["mateOrigin"]["factoryInput"]["proposalSeed"],
        "stopped-v5-authority-terminal-rejection-mate-7"
    );
    assert_eq!(
        terminal["proposal"]["rejection"],
        serde_json::json!({
            "schemaVersion": "temporal_qd_pair_rejection_audit_v1",
            "exceptionType": "TemporalDiscoveryContractError",
            "reasonCode": "crossover_rejected",
        }),
        "the fixture must retain the real production rejection contract"
    );
    let side = terminal["proposal"]["side"]
        .as_str()
        .expect("terminal side");
    let authority = oracle_side_authority(&shared, side);
    let parent = fixture_state_for_pair_side(&terminal["parentPair"], side, &authority);
    let mate = fixture_state_for_pair_side(&terminal["matePair"], side, &authority);
    let execution = v5_operators::attempt_evolved_same_side_crossover_from_states(
        terminal["proposalSeed"]
            .as_str()
            .expect("terminal proposal seed"),
        &parent,
        &mate,
        &authority,
    );
    assert_eq!(
        execution.disposition,
        v5_operators::V5OperatorDisposition::Rejected
    );
    assert_eq!(execution.reason_code, "crossover_rejected");
    assert_eq!(execution.reason_detail, terminal["proposal"]["rejection"]);
    assert!(execution.selection.is_none());
    assert!(execution.application.is_none());
    assert!(execution.delta.is_none());
    assert!(
        v5_operators::select_evolved_same_side_crossover_from_states(
            terminal["proposalSeed"]
                .as_str()
                .expect("terminal proposal seed"),
            &parent,
            &mate,
            &authority,
        )
        .is_err(),
        "the terminal has no plan that a caller could accidentally apply"
    );
}
