use serde_json::{Value, json};
use temporal_qd_kernel::{
    genome::{
        CanonicalPairCompiler, FrozenModule, FrozenPair, GenomeError, HoldMutationPlan,
        IdentitySnapshot, NativeModuleValidator as GenomeNativeModuleValidator,
        apply_hold_mutation, canonical_hold, canonical_sha256, proposal_side,
    },
    grammar::{
        GrammarContext, GrammarError, NativeValidator, TypedFragmentGrammar,
        compiled_graph_signature,
    },
};

struct GrammarAuthority;

impl NativeValidator for GrammarAuthority {
    fn validate_v2(&self, profile: &Value, candidate_id: &str) -> Result<Value, GrammarError> {
        let raw = temporal_qd_contract::canonical_sha256(profile)?;
        Ok(json!({
            "schemaVersion": "temporal_search_candidate_validation_v1",
            "candidateId": candidate_id,
            "rawSourceProfileSha256": raw,
            "profileSnapshotSha256": raw,
            "programSha256": temporal_qd_contract::canonical_sha256(&json!({"nativeV2": profile}))?,
            "validationReportSha256": temporal_qd_contract::canonical_sha256(&json!({"nativeValidation": profile}))?,
            "status": "valid_evaluable",
            "candidateAcceptable": true,
            "evaluatorId": "golden"
        }))
    }
}

struct PairAuthority;

struct GenomeAuthority;

impl GenomeNativeModuleValidator for GenomeAuthority {
    fn validate_v2(&self, profile: &Value, candidate_id: &str) -> Result<Value, GenomeError> {
        let raw = canonical_sha256(profile)?;
        Ok(json!({
            "schemaVersion": "temporal_search_candidate_validation_v1",
            "candidateId": candidate_id,
            "rawSourceProfileSha256": raw,
            "profileSnapshotSha256": raw,
            "programSha256": canonical_sha256(&json!({"nativeV2": profile}))?,
            "validationReportSha256": canonical_sha256(&json!({"nativeValidation": profile}))?,
            "status": "valid_evaluable",
            "candidateAcceptable": true,
            "evaluatorId": "golden"
        }))
    }
}

impl CanonicalPairCompiler for PairAuthority {
    fn compile_pair(
        &self,
        long: &Value,
        short: &Value,
        candidate_id: &str,
    ) -> Result<Value, GenomeError> {
        let profile = json!({
            "version": "v3", "directionMode": "both", "name": candidate_id,
            "graph": {"entryArbitration": {"modules": [
                {"direction": "long", "sourceProfileSnapshotSha256": canonical_sha256(long)?},
                {"direction": "short", "sourceProfileSnapshotSha256": canonical_sha256(short)?}
            ]}}
        });
        Ok(json!({
            "profile": profile,
            "validation": {
                "schemaVersion": "temporal_search_candidate_validation_v1",
                "rawSourceProfileSha256": canonical_sha256(&profile)?,
                "profileSnapshotSha256": canonical_sha256(&json!({"snapshot": profile}))?,
                "programSha256": canonical_sha256(&json!({"nativeV3": profile}))?,
                "validationReportSha256": canonical_sha256(&json!({"nativeV3Validation": profile}))?,
                "status": "valid_evaluable", "candidateAcceptable": true
            }
        }))
    }
}

fn context() -> GrammarContext {
    let indicators = ["rsi", "trend", "breakout", "volume"].into_iter().map(|name| {
        json!({"meta":{"id":format!("I_{}", name.to_ascii_uppercase()),"instanceId":name},"config":{"isActive":true,"useFormingBar":false,"timeframe":"M5"}})
    }).collect();
    let groups = ["rsi", "trend", "breakout", "volume"]
        .into_iter()
        .map(|name| json!({"id":format!("g_{name}"),"indicatorInstanceIds":[name]}))
        .collect();
    let events = ["rsi", "trend", "breakout", "volume"].into_iter().map(|name| json!({"id":format!("e_{name}"),"indicatorInstanceId":name,"longOutput":"bullish","shortOutput":"bearish"})).collect();
    GrammarContext::new(
        "EURUSD",
        indicators,
        groups,
        events,
        json!({"managementLibrary":{"version":"temporal_management_v1","defaultPlanId":"base","plans":[{"id":"base","initialStop":{"kind":"fixed_percent","percent":1.0},"initialTarget":{"kind":"reward_multiple","multiple":2.0}}]}}),
        None,
    )
}

fn snapshot(kind: &str, value: &str) -> IdentitySnapshot {
    IdentitySnapshot::create(kind, &format!("{kind}_v1"), &json!({"value": value})).unwrap()
}

fn expected<'a>(matrix: &'a Value, section: &str, key: &str) -> &'a Value {
    &matrix[section][key]
}

fn assert_program_vector(
    grammar: &TypedFragmentGrammar<'_>,
    program: &temporal_qd_kernel::grammar::ModuleProgram,
    vector: &Value,
) {
    let profile = grammar.materialize_profile(program).unwrap();
    assert_eq!(
        temporal_qd_contract::canonical_sha256(&program.canonical()).unwrap(),
        vector["p"]
    );
    assert_eq!(
        temporal_qd_contract::canonical_sha256(&profile).unwrap(),
        vector["m"]
    );
    assert_eq!(compiled_graph_signature(&profile).unwrap(), vector["g"]);
    assert_eq!(
        temporal_qd_contract::canonical_sha256(&Value::Array(program.lineage.clone())).unwrap(),
        vector["l"]
    );
}

fn frozen_module(
    grammar: &TypedFragmentGrammar<'_>,
    direction: &str,
    name: &str,
    candidate_id: &str,
) -> FrozenModule {
    let program = grammar.seed(direction, name, None, None, None).unwrap();
    let compiled = grammar.compile_module(&program, candidate_id).unwrap();
    FrozenModule::freeze(
        &compiled.program,
        &compiled.profile,
        &snapshot("grammarContext", "ctx"),
        &snapshot("catalog", "cat"),
        &snapshot("policy", "policy"),
        &snapshot("nativeAuthority", "authority"),
        &compiled.native_report,
        &program.lineage,
    )
    .unwrap()
}

#[test]
fn golden_python_grammar_and_genome_vectors_match_exactly() {
    let golden: Value =
        serde_json::from_str(include_str!("fixtures/grammar_genome_golden.json")).unwrap();
    let authority = GrammarAuthority;
    let grammar = TypedFragmentGrammar::new(context(), &authority).unwrap();
    let long_program = grammar
        .seed("long", "mean_reversion", None, None, None)
        .unwrap();
    assert_eq!(long_program.canonical(), golden["program"]);
    assert_eq!(
        temporal_qd_contract::canonical_sha256(&long_program.canonical()).unwrap(),
        golden["programSha256"]
    );
    assert_eq!(
        grammar.enumerate_operations(&long_program).unwrap().len(),
        golden["operationCount"]
    );
    let long = grammar
        .compile_module(&long_program, "golden_long")
        .unwrap();
    assert_eq!(
        temporal_qd_contract::canonical_sha256(&long.profile).unwrap(),
        golden["profileSha256"]
    );
    assert_eq!(
        compiled_graph_signature(&long.profile).unwrap(),
        golden["graphSha256"]
    );

    let frozen_long = FrozenModule::freeze(
        &long.program,
        &long.profile,
        &snapshot("grammarContext", "ctx"),
        &snapshot("catalog", "cat"),
        &snapshot("policy", "policy"),
        &snapshot("nativeAuthority", "authority"),
        &long.native_report,
        &long_program.lineage,
    )
    .unwrap();
    assert_eq!(
        frozen_long.identity_sha256().unwrap(),
        golden["moduleIdentitySha256"]
    );

    let short_program = grammar.seed("short", "trend", None, None, None).unwrap();
    let short = grammar
        .compile_module(&short_program, "golden_short")
        .unwrap();
    let frozen_short = FrozenModule::freeze(
        &short.program,
        &short.profile,
        &snapshot("grammarContext", "ctx"),
        &snapshot("catalog", "cat"),
        &snapshot("policy", "policy"),
        &snapshot("nativeAuthority", "authority"),
        &short.native_report,
        &short_program.lineage,
    )
    .unwrap();
    let pair = FrozenPair::compile(
        frozen_long,
        frozen_short,
        &snapshot("pairCompiler", "compiler"),
        &PairAuthority,
        "golden_pair",
        &[],
    )
    .unwrap();
    assert_eq!(
        pair.identity_sha256().unwrap(),
        golden["pairIdentitySha256"]
    );
    let persisted = pair.canonical_payload().unwrap();
    assert_eq!(
        FrozenPair::from_payload(&persisted)
            .unwrap()
            .canonical_payload()
            .unwrap(),
        persisted
    );
    assert_eq!(
        canonical_sha256(
            &canonical_hold(Some(
                &json!({"kind":"market_bars","bars":13,"timeframe":"M5"})
            ))
            .unwrap()
        )
        .unwrap(),
        golden["holdMarketBarsSha256"]
    );
    for (seed, expected) in golden["proposalSides"].as_object().unwrap() {
        assert_eq!(proposal_side(seed).unwrap(), expected.as_str().unwrap());
    }
}

#[test]
fn generated_programs_are_balanced_unique_and_closed() {
    let authority = GrammarAuthority;
    let grammar = TypedFragmentGrammar::new(context(), &authority).unwrap();
    let first = grammar.generate(20, 20260803).unwrap();
    let second = grammar.generate(20, 20260803).unwrap();
    assert_eq!(
        first
            .iter()
            .map(|item| item.canonical())
            .collect::<Vec<_>>(),
        second
            .iter()
            .map(|item| item.canonical())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        first.iter().filter(|item| item.direction == "long").count(),
        10
    );
    assert_eq!(
        first
            .iter()
            .map(|item| temporal_qd_contract::canonical_sha256(&item.canonical()).unwrap())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        20
    );
}

#[test]
fn exhaustive_python_operation_matrix_matches_both_sides() {
    let matrix: Value =
        serde_json::from_str(include_str!("fixtures/grammar_operation_matrix.json")).unwrap();
    assert_eq!(
        matrix["schemaVersion"],
        "temporal_qd_grammar_operation_matrix_v1"
    );
    let authority = GrammarAuthority;
    let grammar = TypedFragmentGrammar::new(context(), &authority).unwrap();

    for direction in ["long", "short"] {
        for root in ["mean_reversion", "breakout", "trend"] {
            let program = grammar.seed(direction, root, None, None, None).unwrap();
            assert_program_vector(
                &grammar,
                &program,
                expected(&matrix, "seeds", &format!("{direction}_{root}")),
            );
        }

        let cases = vec![
            (
                "substitute",
                vec![],
                json!({"operation":"substitute","index":0,"productionId":"arm_fresh_event"}),
            ),
            (
                "rebind",
                vec![],
                json!({"operation":"rebind","index":0,"slot":"group","value":"g_rsi"}),
            ),
            (
                "mutate_choice",
                vec![],
                json!({"operation":"mutate_choice","index":0,"choice":"threshold","value":50.0}),
            ),
            (
                "duplicate_specialize",
                vec![],
                json!({"operation":"duplicate_specialize","index":1}),
            ),
            (
                "insert",
                vec![],
                json!({"operation":"insert","productionId":"gate_delay"}),
            ),
            (
                "add_branch",
                vec![],
                json!({"operation":"add_branch","productionId":"move_break_even"}),
            ),
            (
                "remove",
                vec![json!({"operation":"insert","productionId":"gate_delay"})],
                json!({"operation":"remove","index":1}),
            ),
            (
                "remove_branch",
                vec![json!({"operation":"add_branch","productionId":"move_break_even"})],
                json!({"operation":"remove_branch","index":3}),
            ),
            (
                "move",
                vec![
                    json!({"operation":"insert","productionId":"gate_delay"}),
                    json!({"operation":"insert","productionId":"gate_fresh_event"}),
                ],
                json!({"operation":"move","from":1,"to":2}),
            ),
        ];
        for (name, prelude, operation) in cases {
            let mut parent = grammar
                .seed(direction, "mean_reversion", None, None, None)
                .unwrap();
            for operation in prelude {
                assert!(
                    grammar
                        .enumerate_operations(&parent)
                        .unwrap()
                        .contains(&operation)
                );
                parent = grammar.apply(&parent, &operation).unwrap();
            }
            assert!(
                grammar
                    .enumerate_operations(&parent)
                    .unwrap()
                    .contains(&operation)
            );
            let child = grammar.apply(&parent, &operation).unwrap();
            assert_program_vector(
                &grammar,
                &child,
                expected(&matrix, "operations", &format!("{direction}_{name}")),
            );
        }

        let mut left = grammar
            .seed(direction, "mean_reversion", None, None, None)
            .unwrap();
        let left_operation = json!({"operation":"add_branch","productionId":"move_break_even"});
        left = grammar.apply(&left, &left_operation).unwrap();
        let mut right = grammar
            .seed(direction, "breakout", None, None, None)
            .unwrap();
        let right_operation = json!({"operation":"add_branch","productionId":"set_target"});
        right = grammar.apply(&right, &right_operation).unwrap();
        let crossover = grammar.crossover(&left, &right, direction).unwrap();
        assert_program_vector(
            &grammar,
            &crossover,
            expected(&matrix, "operations", &format!("{direction}_crossover")),
        );
    }
}

#[test]
fn grammar_budget_and_noop_boundaries_fail_closed() {
    let authority = GrammarAuthority;
    let grammar = TypedFragmentGrammar::new(context(), &authority).unwrap();
    let source = grammar
        .seed("long", "mean_reversion", None, None, None)
        .unwrap();
    assert!(
        grammar
            .apply(
                &source,
                &json!({"operation":"mutate_choice","index":0,"choice":"threshold","value":35.0})
            )
            .is_err()
    );
    assert!(
        grammar
            .apply(
                &source,
                &json!({"operation":"insert","productionId":"unsealed"})
            )
            .is_err()
    );
    let mut invalid_resource = source.clone();
    invalid_resource.fragments[0]
        .resources
        .insert("group".to_owned(), "missing".to_owned());
    assert!(grammar.validate(&invalid_resource).is_err());
    assert!(grammar.generate(0, 1).is_err());

    let mut constrained = context();
    constrained.budgets.states = 1;
    let constrained = TypedFragmentGrammar::new(constrained, &authority).unwrap();
    assert!(
        constrained
            .seed("long", "mean_reversion", None, None, None)
            .is_err()
    );
}

#[test]
fn frozen_schema_tamper_and_hold_apply_remove_match_python_vectors() {
    let hold_matrix: Value =
        serde_json::from_str(include_str!("fixtures/genome_hold_matrix.json")).unwrap();
    let authority = GrammarAuthority;
    let grammar = TypedFragmentGrammar::new(context(), &authority).unwrap();
    let source = frozen_module(&grammar, "long", "mean_reversion", "golden_long");
    let genome_authority = GenomeAuthority;

    let market_plan = HoldMutationPlan::create(
        &source,
        "base",
        Some(&json!({"kind":"market_bars","bars":13,"timeframe":"M5"})),
    )
    .unwrap();
    let changed =
        apply_hold_mutation(&source, &market_plan, &genome_authority, "hold_market").unwrap();
    assert_eq!(
        market_plan.plan_sha256().unwrap(),
        hold_matrix["market"]["planSha256"]
    );
    assert_eq!(
        changed.program_sha256,
        hold_matrix["market"]["programSha256"]
    );
    assert_eq!(
        changed.profile_sha256,
        hold_matrix["market"]["profileSha256"]
    );
    assert_eq!(
        canonical_sha256(&Value::Array(changed.lineage.clone())).unwrap(),
        hold_matrix["market"]["lineageSha256"]
    );
    assert_eq!(
        changed.identity_sha256().unwrap(),
        hold_matrix["market"]["moduleIdentitySha256"]
    );

    let none_plan =
        HoldMutationPlan::create(&changed, "base", Some(&json!({"kind":"none"}))).unwrap();
    let restored =
        apply_hold_mutation(&changed, &none_plan, &genome_authority, "hold_none").unwrap();
    assert_eq!(
        none_plan.plan_sha256().unwrap(),
        hold_matrix["none"]["planSha256"]
    );
    assert_eq!(
        restored.program_sha256,
        hold_matrix["none"]["programSha256"]
    );
    assert_eq!(
        restored.profile_sha256,
        hold_matrix["none"]["profileSha256"]
    );
    assert_eq!(
        canonical_sha256(&Value::Array(restored.lineage.clone())).unwrap(),
        hold_matrix["none"]["lineageSha256"]
    );
    assert_eq!(
        restored.identity_sha256().unwrap(),
        hold_matrix["none"]["moduleIdentitySha256"]
    );
    assert!(
        restored.profile["executionConfig"]["managementLibrary"]["plans"][0]
            .get("holdPolicy")
            .is_none()
    );

    let pair = FrozenPair::compile(
        source,
        frozen_module(&grammar, "short", "trend", "golden_short"),
        &snapshot("pairCompiler", "compiler"),
        &PairAuthority,
        "golden_pair",
        &[],
    )
    .unwrap();
    let persisted = pair.canonical_payload().unwrap();
    let mut profile_tamper = persisted.clone();
    profile_tamper["long"]["profile"]["directionMode"] = json!("short");
    assert!(FrozenPair::from_payload(&profile_tamper).is_err());
    let mut closed_schema_tamper = persisted;
    closed_schema_tamper
        .as_object_mut()
        .unwrap()
        .insert("extra".to_owned(), Value::Null);
    assert!(FrozenPair::from_payload(&closed_schema_tamper).is_err());
    assert!(
        IdentitySnapshot::create("catalog", "catalog_v1", &json!({"catalogAlias":"latest"}))
            .is_err()
    );
}
