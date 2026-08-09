use serde_json::{Value, json};
use temporal_qd_contract::canonical_sha256;
use temporal_qd_kernel::{
    factory::{NativeConstructionContext, NativePairAuthority, ProposalIntent},
    genome::{CanonicalPairCompiler, FrozenModule, FrozenPair, GenomeError, IdentitySnapshot},
    grammar::{GrammarContext, GrammarError, NativeValidator, TypedFragmentGrammar},
    identity::proposal_side,
    indicator::{IndicatorCatalog, IndicatorLearningRegistry},
    proposal::ParentSelector,
    protection::default_initial_protection_policy,
};
use temporal_qd_runtime::{
    DashboardPort, RuntimeManifest, RuntimePairAuthority, RuntimeParentSelector,
    archive::{
        ArchiveParentSelector, CORRECTED_QD_POLICY_NAME, CORRECTED_QD_POLICY_SHA256,
        LEGACY_QD_POLICY_NAME, LEGACY_QD_POLICY_SHA256, VerifiedParentArchive,
    },
};

const DIRECTIONAL_QD_POLICY_NAME: &str = "stage5e7_v5_direction_aware_breeding_archive";
const DIRECTIONAL_QD_POLICY_SHA256: &str =
    "sha256:c8ea30b0a9d2825844d4267be9e4ccf82f36dc43a741ac061d41508fe486c3da";

const GENERATION_SEED: &str =
    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
// Deliberately distinct: parent selection is seeded by pair-generation
// `configSha256`, never by the frozen pair-run authority identity.
const PAIR_RUN_CONFIG_SHA256: &str =
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const CUMULATIVE_SHA: &str =
    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

struct GrammarAuthority;

impl NativeValidator for GrammarAuthority {
    fn validate_v2(&self, profile: &Value, candidate_id: &str) -> Result<Value, GrammarError> {
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
            "evaluatorId": "archive-test"
        }))
    }
}

struct PairAuthority;

impl CanonicalPairCompiler for PairAuthority {
    fn compile_pair(
        &self,
        long: &Value,
        short: &Value,
        candidate_id: &str,
    ) -> Result<Value, GenomeError> {
        let profile = json!({
            "version": "v3",
            "directionMode": "both",
            "name": candidate_id,
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
                "status": "valid_evaluable",
                "candidateAcceptable": true
            }
        }))
    }
}

fn grammar_context() -> GrammarContext {
    let indicators = ["rsi", "trend", "breakout", "volume"]
        .into_iter()
        .map(|name| {
            json!({
                "meta": {"id": format!("I_{}", name.to_ascii_uppercase()), "instanceId": name},
                "config": {"isActive": true, "useFormingBar": false, "timeframe": "M5"}
            })
        })
        .collect();
    let groups = ["rsi", "trend", "breakout", "volume"]
        .into_iter()
        .map(|name| json!({"id": format!("g_{name}"), "indicatorInstanceIds": [name]}))
        .collect();
    let events = ["rsi", "trend", "breakout", "volume"]
        .into_iter()
        .map(|name| {
            json!({
                "id": format!("e_{name}"),
                "indicatorInstanceId": name,
                "longOutput": "bullish",
                "shortOutput": "bearish"
            })
        })
        .collect();
    GrammarContext::new(
        "EURUSD",
        indicators,
        groups,
        events,
        json!({"managementLibrary": {
            "version": "temporal_management_v1",
            "defaultPlanId": "base",
            "plans": [{
                "id": "base",
                "initialStop": {"kind": "fixed_percent", "percent": 1.0},
                "initialTarget": {"kind": "reward_multiple", "multiple": 2.0}
            }]
        }}),
        None,
    )
}

fn snapshot(kind: &str, value: &str) -> IdentitySnapshot {
    IdentitySnapshot::create(kind, &format!("{kind}_v1"), &json!({"value": value})).unwrap()
}

fn compiler_snapshot() -> IdentitySnapshot {
    snapshot("pairCompiler", "archive-compiler")
}

fn frozen_module(grammar: &TypedFragmentGrammar<'_>, direction: &str, name: &str) -> FrozenModule {
    let program = grammar.seed(direction, name, None, None, None).unwrap();
    let compiled = grammar
        .compile_module(&program, &format!("archive-{direction}-{name}"))
        .unwrap();
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

fn pair(candidate_id: &str) -> FrozenPair {
    let authority = GrammarAuthority;
    let context = grammar_context();
    let grammar = TypedFragmentGrammar::new(context, &authority).unwrap();
    FrozenPair::compile(
        frozen_module(&grammar, "long", "mean_reversion"),
        frozen_module(&grammar, "short", "trend"),
        &compiler_snapshot(),
        &PairAuthority,
        candidate_id,
        &[],
    )
    .unwrap()
}

fn frozen_policy() -> Value {
    json!({
        "schemaVersion": "temporal_qd_policy_v4",
        "policyName": "stage5e7_v4_corrected_descriptor_archive",
        "economicObjectives": [
            {"name": "worstWindowConservativeNetR", "direction": "max"},
            {"name": "maximumDrawdownR", "direction": "min"},
            {"name": "structuralComplexity", "direction": "min"}
        ],
        "tradeSupport": {
            "minimumTotalTrades": 8,
            "minimumTradesPerWindow": 4,
            "capTrades": 20,
            "role": "eligibility_then_capped_tie_break"
        },
        "archive": {
            "defaultCellCapacity": 4,
            "lanes": {
                "quality": "finite_support_and_nonnegative_robust_return",
                "observational": "retained_without_quality_breeding_rights",
                "negativeNovelty": "finite_supported_negative_robust_return"
            },
            "negativeNoveltyMaxMembersPerCell": 1
        },
        "parentSelection": {
            "quality": "pareto_front_then_crowding_then_robust_return_then_capped_support_then_complexity",
            "negativeNoveltyMaxFraction": 0.10,
            "negativeNoveltySchedule": "every_tenth_structural_parent_selection"
        },
        "identity": {
            "candidateIdentity": "reject_exact_repeat",
            "sourceProfile": "reject_same_evidence_repeat",
            "program": "allow_only_for_different_canonical_evidence",
            "canonicalEvidence": "candidate_program_ordered_window_semantic_cost_execution"
        },
        "descriptorPolicy": {
            "schemaVersion": "temporal_qd_descriptor_policy_v2",
            "axes": ["operatorFamilies", "mutationDepth", "entryEvents", "managementActions", "graphNodes", "tradeFrequency", "medianHolding"],
            "lineageFamilyFields": ["operatorId", "operation", "kind"],
            "rootConstructionOperations": ["rich_immigrant_construction", "typed_seed"],
            "managementActionKinds": ["activate_trailing_stop_next_open", "cancel_target_next_open", "deactivate_trailing_stop_next_open", "move_stop_to_break_even_next_open", "set_target_next_open", "tighten_stop_next_open"],
            "graphNodeBounds": [40, 55, 70]
        },
        "resolvedExecutionDeduplication": {
            "required": true,
            "stage": "before_archive_reduction",
            "identity": "aggregate.resolvedProgramSha256",
            "representativeOrdering": [
                {"field": "finiteDataValidity.validForQuality", "direction": "max"},
                {"field": "objectives.worstWindowConservativeNetR", "direction": "max"},
                {"field": "cappedTradeSupport", "direction": "max"},
                {"field": "objectives.maximumDrawdownR", "direction": "min"},
                {"field": "objectives.structuralComplexity", "direction": "min"},
                {"field": "candidateId", "direction": "min"}
            ]
        }
    })
}

fn pair_policy() -> Value {
    json!({
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": true,
        "compilerAuthority": compiler_snapshot().canonical_payload()
    })
}

#[derive(Clone)]
struct RuntimeFixtureDashboard;

impl DashboardPort for RuntimeFixtureDashboard {
    fn validate_v2(
        &self,
        profile: &Value,
        candidate_id: &str,
    ) -> temporal_qd_runtime::Result<Value> {
        let raw = canonical_sha256(profile)?;
        Ok(json!({
            "schemaVersion": "temporal_search_candidate_validation_v1",
            "candidateId": candidate_id,
            "rawSourceProfileSha256": raw,
            "profileSnapshotSha256": canonical_sha256(&json!({"snapshot": profile}))?,
            "programSha256": canonical_sha256(&json!({"program": profile}))?,
            "validationReportSha256": canonical_sha256(&json!({"report": profile}))?,
            "status": "valid_evaluable",
            "candidateAcceptable": true
        }))
    }

    fn compile_bidirectional(
        &self,
        long: &Value,
        short: &Value,
        candidate_id: &str,
    ) -> temporal_qd_runtime::Result<Value> {
        let profile = json!({
            "version": "v3",
            "directionMode": "both",
            "name": candidate_id,
            "graph": {"entryArbitration": {"modules": [
                {"direction": "long", "sourceProfileSnapshotSha256": canonical_sha256(&json!({"snapshot": long}))?},
                {"direction": "short", "sourceProfileSnapshotSha256": canonical_sha256(&json!({"snapshot": short}))?}
            ]}}
        });
        Ok(json!({
            "profile": profile,
            "validation": {
                "schemaVersion": "temporal_search_candidate_validation_v1",
                "rawSourceProfileSha256": canonical_sha256(&profile)?,
                "profileSnapshotSha256": canonical_sha256(&json!({"snapshot": profile}))?,
                "programSha256": canonical_sha256(&json!({"program": profile}))?,
                "validationReportSha256": canonical_sha256(&json!({"report": profile}))?,
                "status": "valid_evaluable",
                "candidateAcceptable": true
            }
        }))
    }
}

fn runtime_catalog() -> Value {
    json!({
        "timeframes": {"M5": {}, "M15": {}, "H1": {}},
        "indicators": [
            {"meta": {"id": "I_RSI", "instanceId": "rsi"}, "config": {"isActive": true, "useFormingBar": false}},
            {"meta": {"id": "I_TREND", "instanceId": "trend"}, "config": {"isActive": true, "useFormingBar": false}},
            {"meta": {"id": "I_BREAKOUT", "instanceId": "breakout"}, "config": {"isActive": true, "useFormingBar": false}},
            {"meta": {"id": "I_VOLUME", "instanceId": "volume"}, "config": {"isActive": true, "useFormingBar": false}}
        ]
    })
}

fn runtime_manifest_fixture() -> RuntimeManifest {
    let catalog = runtime_catalog();
    let catalog_identity = IndicatorCatalog::new(&catalog).unwrap();
    let indicator_policy = IndicatorLearningRegistry::new(catalog_identity.clone())
        .unwrap()
        .policy()
        .clone();
    let mut context = grammar_context().normalized().unwrap();
    for indicator in context["indicators"].as_array_mut().unwrap() {
        indicator["config"]
            .as_object_mut()
            .unwrap()
            .remove("timeframe");
    }
    let context = GrammarContext::from_normalized(&context)
        .unwrap()
        .normalized()
        .unwrap();
    let side = json!({
        "seedNames": ["mean_reversion", "breakout", "trend"],
        "context": context,
        "catalog": catalog,
        "catalogSha256": catalog_identity.catalog_sha256(),
        "indicatorPolicy": indicator_policy,
        "policy": {"schemaVersion": "fixture_module_policy_v1"}
    });
    let native_authority = snapshot("nativeAuthority", "runtime-fixture").canonical_payload();
    let compiler_authority = compiler_snapshot().canonical_payload();
    let mut config = json!({
        "schemaVersion": "temporal_qd_bidirectional_pair_run_config_v2",
        "longModule": side,
        "shortModule": side,
        "grammarRegistry": {"schemaVersion": "fixture"},
        "holdOperatorPolicy": {"choices": [{"kind": "none"}, {"kind": "market_bars", "bars": 12, "timeframe": "M5"}]},
        "initialProtectionOperatorPolicy": default_initial_protection_policy(),
        "immigrantConstructionPolicy": {"grammarMutationDepthBuckets": [0], "indicatorMutationDepthBuckets": [0]},
        "nativeJsonlAuthority": {"fixture": true},
        "nativeAuthority": native_authority,
        "pairCompilerAuthority": compiler_authority,
        "operatorImplementation": {"schemaVersion": "fixture"}
    });
    let config_sha = canonical_sha256(&config).unwrap();
    config["pairRunConfigSha256"] = Value::String(config_sha.clone());
    RuntimeManifest {
        pair_run_config: config,
        pair_run_config_sha256: config_sha,
        bidirectional_pair_policy: pair_policy(),
        bidirectional_pair_policy_sha256: canonical_sha256(&pair_policy()).unwrap(),
        evidence_identity_context: None,
        evidence_identity_context_sha256: None,
        generation_index: 0,
        pair_generation_config_sha256: GENERATION_SEED.into(),
        parent_archive: temporal_qd_runtime::archive::VerifiedParentArchive::from_archive(
            &archive(),
        )
        .unwrap(),
        // The authority constructor consumes only frozen construction inputs;
        // manifest parsing/ledger bootstrap have their own archive tests.
        ledger: Value::Null,
    }
}

fn native_context() -> NativeConstructionContext {
    NativeConstructionContext {
        generation_index: 0,
        birth_ordinal: 0,
        proposal_ordinal: 0,
        pair_policy: pair_policy(),
        evidence_identity_context: None,
        frozen_construction_catalog: None,
        g0_evaluation_width: Some(1),
        factory_construction_policy: None,
    }
}

fn descriptor(operator_families: &str, mutation_depth: &str) -> Value {
    let mut descriptor = json!({
        "operatorFamilies": operator_families,
        "mutationDepth": mutation_depth,
        "entryEvents": "none",
        "managementActions": "none",
        "graphNodes": "small",
        "tradeFrequency": "dormant",
        "medianHolding": "none"
    });
    let cell_id = [
        operator_families,
        mutation_depth,
        "none",
        "none",
        "small",
        "dormant",
        "none",
    ]
    .join("|");
    descriptor["cellId"] = Value::String(cell_id);
    descriptor
}

#[allow(clippy::too_many_arguments)]
fn member(
    candidate_id: &str,
    lane: &str,
    robust_return: f64,
    pareto_front: i64,
    crowding: Option<f64>,
    support: u64,
    robust: Option<Value>,
    rotating: bool,
) -> Value {
    let pair = pair(candidate_id);
    let policy_sha = canonical_sha256(&pair_policy()).unwrap();
    let identity = pair.identity_sha256().unwrap();
    let mut value = json!({
        "candidateId": candidate_id,
        "archiveLane": lane,
        "finiteDataValidity": {
            "isFiniteData": true,
            "passesSupportGate": true,
            "validForQuality": true,
            "totalTrades": support,
            "capTrades": 20
        },
        "objectives": {
            "worstWindowConservativeNetR": robust_return,
            "maximumDrawdownR": 1.0,
            "structuralComplexity": 1.0
        },
        "paretoFront": pareto_front,
        "crowdingDistance": crowding,
        "candidate": {
            "candidateId": candidate_id,
            "sourceProfile": pair.profile,
            "sourceProfileSha256": pair.raw_pair_sha256,
            "programSha256": pair.native_program_sha256,
            "candidateIdentityMaterial": {
                "bidirectionalGenomeIdentitySha256": identity,
                "pairPolicySha256": policy_sha
            },
            "bidirectionalGenome": pair.canonical_payload().unwrap()
        }
    });
    if let Some(robust) = robust {
        value["robustObjectives"] = robust;
    }
    if rotating {
        value["robustBreederEligible"] = Value::Bool(true);
        value["cumulativeEvidenceArchiveSha256"] = Value::String(CUMULATIVE_SHA.to_owned());
    }
    value
}

fn cell(descriptor: Value, visits: u64, attempts: u64, mut members: Vec<Value>) -> Value {
    for member in &mut members {
        member
            .as_object_mut()
            .unwrap()
            .insert("descriptor".to_owned(), descriptor.clone());
    }
    json!({
        "cellId": descriptor["cellId"],
        "descriptor": descriptor,
        "selectionVisitCount": visits,
        "offspringAttemptCount": attempts,
        "members": members
    })
}

fn archive() -> Value {
    let policy = frozen_policy();
    assert_eq!(
        canonical_sha256(&policy).unwrap(),
        CORRECTED_QD_POLICY_SHA256
    );
    let mut archive = json!({
        "schemaVersion": "temporal_qd_archive_v3",
        "qdVersion": "temporal_qd_evolution_v3",
        "policyName": CORRECTED_QD_POLICY_NAME,
        "policySha256": CORRECTED_QD_POLICY_SHA256,
        "frozenPolicy": policy,
        "bidirectionalPairPolicy": pair_policy(),
        "rotatingEvidenceTransaction": {
            "schemaVersion": "temporal_qd_rotating_parent_projection_v1",
            "cumulativeArchiveSha256": CUMULATIVE_SHA
        },
        "cells": [
            cell(
                descriptor("none", "root"),
                5,
                7,
                vec![
                    member("q-best", "quality", 3.0, 0, None, 20, None, false),
                    member("q-second", "quality", 1.0, 0, Some(2.0), 12, None, false)
                ]
            ),
            cell(
                descriptor("one", "root"),
                1,
                2,
                vec![
                    member("q-third", "quality", 2.0, 1, Some(5.0), 20, None, false),
                    member("negative", "negative_novelty", -1.0, 0, Some(1.0), 20, None, false)
                ]
            ),
            cell(
                descriptor("one", "one"),
                3,
                4,
                vec![member(
                    "frontier",
                    "rotating_frontier",
                    -2.0,
                    0,
                    Some(1.0),
                    20,
                    Some(json!({
                        "worstWindowConservativeNetR": -0.5,
                        "drawdown": 2.0,
                        "costDrag": 0.3,
                        "novelty": 4.0
                    })),
                    true
                )]
            )
        ]
    });
    let identity = canonical_sha256(&archive).unwrap();
    archive["archiveSha256"] = Value::String(identity);
    archive
}

fn direction_behavior(
    long_net: f64,
    long_trades: u64,
    long_windows: u64,
    short_net: f64,
    short_trades: u64,
    short_windows: u64,
) -> Value {
    let side = |net: f64, closed: u64, active_windows: u64| {
        let cost = if closed > 0 { 0.2 } else { 0.0 };
        json!({
            "closedTrades": closed,
            "activeWindowCount": active_windows,
            "activeWindowFraction": active_windows as f64 / 4.0,
            "grossR": net + cost,
            "netR": net,
            "costR": cost,
            "active": closed > 0,
            "terminalDirectionCount": 0
        })
    };
    let mut value = json!({
        "schemaVersion": "temporal_realized_behavior_v1",
        "windowCount": 4,
        "sides": {
            "long": side(long_net, long_trades, long_windows),
            "short": side(short_net, short_trades, short_windows)
        }
    });
    value["identitySha256"] = Value::String(canonical_sha256(&value).unwrap());
    value
}

fn direction_selection(
    behavior: &Value,
    lane: &str,
    eligible: bool,
    specialist: Option<&str>,
) -> Value {
    let mut value = json!({
        "schemaVersion": "temporal_direction_selection_v1",
        "policyIdentitySha256": "sha256:2567175ff6ae6063baa485484c0faa0d742507af6814a593076020a68aef3ed1",
        "realizedBehaviorIdentitySha256": behavior["identitySha256"],
        "lane": lane,
        "selectionEligible": eligible,
        "specialistSide": specialist
    });
    value["identitySha256"] = Value::String(canonical_sha256(&value).unwrap());
    value
}

fn directional_archive(mut archive: Value, scenario: &str) -> Value {
    let mut policy = frozen_policy();
    let fields = policy.as_object_mut().unwrap();
    fields.insert(
        "schemaVersion".into(),
        Value::String("temporal_qd_policy_v5".into()),
    );
    fields.insert(
        "policyName".into(),
        Value::String(DIRECTIONAL_QD_POLICY_NAME.into()),
    );
    fields.insert("directionSelection".into(), json!({
        "schemaVersion": "temporal_qd_directional_breeding_policy_v1",
        "selectionPolicy": {
            "schemaVersion": "temporal_direction_selection_policy_v1",
            "minimum_closed_trades_per_side": 1,
            "minimum_active_windows_per_side": 1,
            "minimum_acceptable_side_net_r": 0.0,
            "harmful_opposite_net_r": -0.25
        },
        "selectionPolicySha256": "sha256:2567175ff6ae6063baa485484c0faa0d742507af6814a593076020a68aef3ed1",
        "memberBinding": "aggregate.realizedBehavior_then_direction_selection_v1",
        "breedingLanes": {
            "balanced_bidirectional": "supported_nonnegative_long_and_short",
            "long_specialist": "supported_nonnegative_long_with_inactive_or_unsupported_short",
            "short_specialist": "supported_nonnegative_short_with_inactive_or_unsupported_long"
        },
        "ineligibleLanes": ["harmful_opposite_side", "inactive_or_unsupported"],
        "perCellBreedingQuotas": {"balanced_bidirectional": 2, "long_specialist": 1, "short_specialist": 1},
        "fallback": "remaining_direction_eligible_quality_pareto_then_crowding"
    }));
    assert_eq!(
        canonical_sha256(&policy).unwrap(),
        DIRECTIONAL_QD_POLICY_SHA256
    );
    archive["policyName"] = Value::String(DIRECTIONAL_QD_POLICY_NAME.into());
    archive["policySha256"] = Value::String(DIRECTIONAL_QD_POLICY_SHA256.into());
    archive["frozenPolicy"] = policy;
    for cell in archive["cells"].as_array_mut().unwrap() {
        for member in cell["members"].as_array_mut().unwrap() {
            let (behavior, lane, eligible, specialist) = match scenario {
                "harmful" if member["candidateId"] == "q-best" => (
                    direction_behavior(2.0, 2, 1, -1.0, 2, 1),
                    "harmful_opposite_side",
                    false,
                    None,
                ),
                "long_specialist" => (
                    direction_behavior(2.0, 2, 1, 0.0, 0, 0),
                    "long_specialist",
                    true,
                    Some("long"),
                ),
                "short_specialist" => (
                    direction_behavior(0.0, 0, 0, 2.0, 2, 1),
                    "short_specialist",
                    true,
                    Some("short"),
                ),
                "inactive" => (
                    direction_behavior(0.0, 0, 0, 0.0, 0, 0),
                    "inactive_or_unsupported",
                    false,
                    None,
                ),
                _ => (
                    direction_behavior(2.0, 2, 1, 1.0, 2, 1),
                    "balanced_bidirectional",
                    true,
                    None,
                ),
            };
            member["aggregate"] = json!({"realizedBehavior": behavior});
            member["directionSelection"] = direction_selection(
                &member["aggregate"]["realizedBehavior"],
                lane,
                eligible,
                specialist,
            );
            member["directionBehaviorLane"] = Value::String(lane.into());
            member["directionBreedingLane"] = if eligible {
                Value::String(lane.into())
            } else {
                Value::Null
            };
        }
    }
    archive.as_object_mut().unwrap().remove("archiveSha256");
    archive["archiveSha256"] = Value::String(canonical_sha256(&archive).unwrap());
    archive
}

#[test]
fn direction_aware_archive_admits_balanced_breeding_and_rejects_harmful_hiding() {
    let balanced = directional_archive(archive(), "balanced");
    assert!(VerifiedParentArchive::from_archive(&balanced).is_ok());

    let harmful = directional_archive(archive(), "harmful");
    assert!(VerifiedParentArchive::from_archive(&harmful).is_ok());
    let selector = ArchiveParentSelector::from_archive(&harmful, GENERATION_SEED, false).unwrap();
    assert!(selector.eligible_parent_count() < 5);

    let mut forged = harmful;
    let member = &mut forged["cells"][0]["members"][0];
    member["directionBreedingLane"] = Value::String("balanced_bidirectional".into());
    forged.as_object_mut().unwrap().remove("archiveSha256");
    forged["archiveSha256"] = Value::String(canonical_sha256(&forged).unwrap());
    assert!(VerifiedParentArchive::from_archive(&forged).is_err());
}

#[test]
fn direction_aware_archive_preserves_specialists_and_excludes_inactive_lanes() {
    for scenario in ["long_specialist", "short_specialist"] {
        let archive = directional_archive(archive(), scenario);
        let selector = ArchiveParentSelector::from_archive(&archive, GENERATION_SEED, false)
            .expect("direction-specialist archive must parse");
        assert!(selector.eligible_parent_count() > 0, "{scenario}");
    }

    let inactive = directional_archive(archive(), "inactive");
    assert!(VerifiedParentArchive::from_archive(&inactive).is_ok());
    let selector = ArchiveParentSelector::from_archive(&inactive, GENERATION_SEED, true)
        .expect("inactive archive remains observable/readable");
    assert_eq!(selector.eligible_parent_count(), 0);
}

fn legacy_frozen_policy() -> Value {
    let mut policy = frozen_policy();
    let fields = policy.as_object_mut().unwrap();
    fields.insert(
        "schemaVersion".to_owned(),
        Value::String("temporal_qd_policy_v3".to_owned()),
    );
    fields.insert(
        "policyName".to_owned(),
        Value::String(LEGACY_QD_POLICY_NAME.to_owned()),
    );
    fields.remove("descriptorPolicy");
    assert_eq!(canonical_sha256(&policy).unwrap(), LEGACY_QD_POLICY_SHA256);
    policy
}

#[test]
fn legacy_archive_is_readable_but_cross_policy_identity_is_rejected() {
    let mut legacy = archive();
    legacy["policyName"] = Value::String(LEGACY_QD_POLICY_NAME.to_owned());
    legacy["policySha256"] = Value::String(LEGACY_QD_POLICY_SHA256.to_owned());
    legacy["frozenPolicy"] = legacy_frozen_policy();
    legacy.as_object_mut().unwrap().remove("archiveSha256");
    let hash = canonical_sha256(&legacy).unwrap();
    legacy["archiveSha256"] = Value::String(hash);
    VerifiedParentArchive::from_archive(&legacy).unwrap();

    legacy["policyName"] = Value::String("stage5e7_v4_corrected_descriptor_archive".to_owned());
    legacy.as_object_mut().unwrap().remove("archiveSha256");
    let hash = canonical_sha256(&legacy).unwrap();
    legacy["archiveSha256"] = Value::String(hash);
    assert!(VerifiedParentArchive::from_archive(&legacy).is_err());
}

fn audit_field<'a>(
    reference: &'a temporal_qd_kernel::factory::ParentReference,
    key: &str,
) -> &'a Value {
    &reference.selection_audit.as_ref().unwrap()[key]
}

#[test]
fn python_parent_selection_golden_matches_modes_ranks_frontier_and_negative_lane() {
    // Oracle: Python 3.13 random.Random plus temporal_qd_evolution._select_parent.
    // The vector deliberately contains mutation, crossover, and retry labels.
    let labels = [
        "mutation",
        "crossover_parent",
        "crossover_mate",
        "crossover_mate_retry_0",
        "mutation",
        "mutation",
        "crossover_parent",
        "crossover_mate",
        "crossover_mate_retry_0",
        "mutation",
        "mutation",
        "crossover_parent",
        "crossover_mate",
        "crossover_mate_retry_0",
        "mutation",
    ];
    let expected = [
        (
            "q-second",
            "none|root|none|none|small|dormant|none",
            "uniform_occupied_cell",
            "quality",
            "quality_eligible_parent",
        ),
        (
            "q-third",
            "one|root|none|none|small|dormant|none",
            "uniform_occupied_cell",
            "quality",
            "quality_eligible_parent",
        ),
        (
            "q-third",
            "one|root|none|none|small|dormant|none",
            "uniform_occupied_cell",
            "quality",
            "quality_eligible_parent",
        ),
        (
            "q-third",
            "one|root|none|none|small|dormant|none",
            "low_visit_cell",
            "quality",
            "quality_eligible_parent",
        ),
        (
            "q-third",
            "one|root|none|none|small|dormant|none",
            "uniform_occupied_cell",
            "quality",
            "quality_eligible_parent",
        ),
        (
            "q-second",
            "none|root|none|none|small|dormant|none",
            "sparse_descriptor_boundary",
            "quality",
            "quality_eligible_parent",
        ),
        (
            "frontier",
            "one|one|none|none|small|dormant|none",
            "low_visit_cell",
            "rotating_frontier",
            "bounded_cumulative_frontier_fallback",
        ),
        (
            "q-second",
            "none|root|none|none|small|dormant|none",
            "uniform_occupied_cell",
            "quality",
            "quality_eligible_parent",
        ),
        (
            "frontier",
            "one|one|none|none|small|dormant|none",
            "low_visit_cell",
            "rotating_frontier",
            "bounded_cumulative_frontier_fallback",
        ),
        (
            "negative",
            "one|root|none|none|small|dormant|none",
            "negative_novelty_exploration",
            "negative_novelty",
            "scheduled_every_tenth_structural_parent_selection",
        ),
        (
            "q-third",
            "one|root|none|none|small|dormant|none",
            "uniform_occupied_cell",
            "quality",
            "quality_eligible_parent",
        ),
        (
            "q-third",
            "one|root|none|none|small|dormant|none",
            "uniform_occupied_cell",
            "quality",
            "quality_eligible_parent",
        ),
        (
            "q-third",
            "one|root|none|none|small|dormant|none",
            "uniform_occupied_cell",
            "quality",
            "quality_eligible_parent",
        ),
        (
            "frontier",
            "one|one|none|none|small|dormant|none",
            "sparse_descriptor_boundary",
            "rotating_frontier",
            "bounded_cumulative_frontier_fallback",
        ),
        (
            "q-best",
            "none|root|none|none|small|dormant|none",
            "uniform_occupied_cell",
            "quality",
            "quality_eligible_parent",
        ),
    ];
    let mut selector =
        ArchiveParentSelector::from_archive(&archive(), GENERATION_SEED, false).unwrap();
    assert!(selector.archive_sha256().starts_with("sha256:"));
    assert!(selector.has_parents());
    assert_eq!(selector.archive_cell_count(), 3);
    assert_eq!(selector.eligible_parent_count(), 4);
    for (ordinal, (label, expected)) in labels.into_iter().zip(expected).enumerate() {
        let selected = selector.select(label, ordinal as u64).unwrap();
        selected.validate().unwrap();
        assert_eq!(selected.candidate_id, expected.0, "ordinal {ordinal}");
        assert_eq!(audit_field(&selected, "parentCellId"), expected.1);
        assert_eq!(audit_field(&selected, "selectionMode"), expected.2);
        assert_eq!(audit_field(&selected, "parentLane"), expected.3);
        assert_eq!(audit_field(&selected, "parentLaneReason"), expected.4);
        assert_eq!(audit_field(&selected, "parentCandidateId"), expected.0);
        assert_eq!(
            audit_field(&selected, "schemaVersion"),
            "temporal_qd_pair_parent_selection_v1"
        );
        if ordinal == 0 {
            assert_eq!(
                selected.selection_audit,
                Some(json!({
                    "schemaVersion": "temporal_qd_pair_parent_selection_v1",
                    "parentCellId": "none|root|none|none|small|dormant|none",
                    "parentCandidateId": "q-second",
                    "selectionMode": "uniform_occupied_cell",
                    "parentLane": "quality",
                    "parentLaneReason": "quality_eligible_parent",
                    "paretoFront": 0,
                    "crowdingDistance": 2.0
                }))
            );
        }
    }
    assert_eq!(
        selector.cell_counters("none|root|none|none|small|dormant|none"),
        Some((9, 11))
    );
    assert_eq!(
        selector.cell_counters("one|one|none|none|small|dormant|none"),
        Some((6, 7))
    );
    assert_eq!(
        selector.cell_counters("one|root|none|none|small|dormant|none"),
        Some((9, 10))
    );
}

#[test]
fn runtime_facade_uses_pair_generation_config_sha256_for_python_parent_golden() {
    assert_ne!(GENERATION_SEED, PAIR_RUN_CONFIG_SHA256);
    // Oracle: Python `_proposal_rng(_proposal_seed(configSha256, ...))`.
    // These exact rows are intentionally routed through RuntimeParentSelector,
    // which prevents a future caller from accidentally supplying the frozen
    // pair-run authority SHA in place of the pair-generation config SHA.
    let labels = [
        "mutation",
        "crossover_parent",
        "crossover_mate",
        "crossover_mate_retry_0",
        "mutation",
        "mutation",
        "crossover_parent",
        "crossover_mate",
        "crossover_mate_retry_0",
        "mutation",
    ];
    let expected = [
        "q-second", "q-third", "q-third", "q-third", "q-third", "q-second", "frontier", "q-second",
        "frontier", "negative",
    ];
    let mut selector = RuntimeParentSelector::from_archive(&archive(), GENERATION_SEED, false)
        .expect("verified archive plus pair-generation config SHA must open");
    for (ordinal, (label, candidate_id)) in labels.into_iter().zip(expected).enumerate() {
        let selected = selector.select(label, ordinal as u64).unwrap();
        assert_eq!(selected.candidate_id, candidate_id, "ordinal {ordinal}");
    }
}

#[test]
fn verified_archive_is_the_single_immutable_source_for_selection_and_members() {
    let mut raw = archive();
    let verified = VerifiedParentArchive::from_archive(&raw).unwrap();
    assert_eq!(verified.members().count(), 5);
    assert_eq!(
        verified.archive_sha256(),
        raw["archiveSha256"]
            .as_str()
            .expect("self-authenticating fixture")
    );
    // Verified parents retain only the exact data used by selection and ledger
    // bootstrap.  They deliberately do not retain a second full canonical
    // archive JSON tree after its self-hash and every member were validated.
    let expected_sha = verified.archive_sha256().to_owned();

    // The original JSON is no longer consulted after validation; selector
    // construction uses the frozen cell/member projection instead.
    raw["cells"][0]["members"][0]["candidateId"] = Value::String("tampered".into());
    let mut selector =
        ArchiveParentSelector::from_verified(&verified, GENERATION_SEED, false).unwrap();
    assert_eq!(selector.archive_sha256(), expected_sha);
    assert_eq!(
        selector.select("mutation", 0).unwrap().candidate_id,
        "q-second"
    );
}

#[test]
fn compact_restore_matches_uninterrupted_selection_across_retry_and_negative_slot() {
    let archive = archive();
    let labels = [
        "mutation",
        "crossover_parent",
        "crossover_mate",
        "crossover_mate_retry_0",
        "mutation",
        "mutation",
        "crossover_parent",
        "crossover_mate",
        "crossover_mate_retry_0",
        "mutation",
        "mutation",
        "crossover_parent",
    ];
    let mut full = ArchiveParentSelector::from_archive(&archive, GENERATION_SEED, false).unwrap();
    let full_results = labels
        .iter()
        .enumerate()
        .map(|(ordinal, label)| full.select(label, ordinal as u64).unwrap())
        .collect::<Vec<_>>();

    let mut first = ArchiveParentSelector::from_archive(&archive, GENERATION_SEED, false).unwrap();
    let mut resumed_results = labels[..7]
        .iter()
        .enumerate()
        .map(|(ordinal, label)| first.select(label, ordinal as u64).unwrap())
        .collect::<Vec<_>>();
    let checkpoint = first.compact_state();
    let mut resumed =
        ArchiveParentSelector::from_archive(&archive, GENERATION_SEED, false).unwrap();
    resumed.restore_compact_state(&checkpoint).unwrap();
    resumed_results.extend(
        labels[7..]
            .iter()
            .enumerate()
            .map(|(offset, label)| resumed.select(label, (offset + 7) as u64).unwrap()),
    );
    assert_eq!(
        resumed_results
            .iter()
            .map(|row| (&row.pair_identity_sha256, &row.selection_audit))
            .collect::<Vec<_>>(),
        full_results
            .iter()
            .map(|row| (&row.pair_identity_sha256, &row.selection_audit))
            .collect::<Vec<_>>()
    );
    assert_eq!(resumed.compact_state(), full.compact_state());
}

#[test]
fn archive_and_checkpoint_authorities_are_closed() {
    let valid = archive();
    let mut tampered = valid.clone();
    tampered["cells"][0]["members"][0]["candidateId"] = Value::String("tampered".to_owned());
    assert!(ArchiveParentSelector::from_archive(&tampered, GENERATION_SEED, false).is_err());

    let mut policy_extension = valid.clone();
    policy_extension["bidirectionalPairPolicy"]["unexpected"] = Value::Bool(true);
    policy_extension
        .as_object_mut()
        .unwrap()
        .remove("archiveSha256");
    let identity = canonical_sha256(&policy_extension).unwrap();
    policy_extension["archiveSha256"] = Value::String(identity);
    assert!(
        ArchiveParentSelector::from_archive(&policy_extension, GENERATION_SEED, false).is_err()
    );

    let mut selector = ArchiveParentSelector::from_archive(&valid, GENERATION_SEED, false).unwrap();
    selector.select("mutation", 0).unwrap();
    let mut state = selector.compact_state();
    state["generationSeed"] = Value::String(format!("sha256:{}", "b".repeat(64)));
    assert!(selector.restore_compact_state(&state).is_err());
}

#[test]
fn unavailable_tenth_negative_slot_uses_the_exact_quality_fallback_audit() {
    let mut without_negative = archive();
    without_negative["cells"][1]["members"]
        .as_array_mut()
        .unwrap()
        .retain(|member| member["archiveLane"] != "negative_novelty");
    without_negative
        .as_object_mut()
        .unwrap()
        .remove("archiveSha256");
    let identity = canonical_sha256(&without_negative).unwrap();
    without_negative["archiveSha256"] = Value::String(identity);
    let mut selector =
        ArchiveParentSelector::from_archive(&without_negative, GENERATION_SEED, false).unwrap();
    let selected = selector.select("mutation", 9).unwrap();
    assert_eq!(audit_field(&selected, "parentLane"), "quality");
    assert_eq!(
        audit_field(&selected, "parentLaneReason"),
        "negative_novelty_slot_unavailable_quality_fallback"
    );
    assert_ne!(
        audit_field(&selected, "selectionMode"),
        "negative_novelty_exploration"
    );
}

#[test]
fn fake_dashboard_runtime_authority_materializes_a_rich_immigrant_with_frozen_hashes() {
    let manifest = runtime_manifest_fixture();
    let mut authority = RuntimePairAuthority::new(&manifest, RuntimeFixtureDashboard).unwrap();
    let context = native_context();
    let proposal_seed = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let output = authority
        .execute(
            &ProposalIntent::RichImmigrant {
                proposal_seed: proposal_seed.into(),
                long_seed: "fixture-long".into(),
                short_seed: "fixture-short".into(),
            },
            &context,
        )
        .unwrap();
    assert_eq!(output.proposal["disposition"], "materialized");
    assert!(output.candidate.is_some());
    assert!(
        output
            .executable_semantic_sha256
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(output.funnel_material.is_some());
    assert_eq!(
        output.proposal["proposalSha256"],
        canonical_sha256(&{
            let mut material = output.proposal.clone();
            material.as_object_mut().unwrap().remove("proposalSha256");
            material
        })
        .unwrap()
    );
}

#[test]
fn rich_immigrant_proposal_side_matches_the_v6_multi_ordinal_oracle() {
    let manifest = runtime_manifest_fixture();
    let mut authority = RuntimePairAuthority::new(&manifest, RuntimeFixtureDashboard).unwrap();
    let context = native_context();
    // Proposal seeds 0..7 from the v6 cross-engine admission run.  Exercise
    // the published runtime proposal field, not only the kernel helper.
    for (seed, expected_side) in [
        (
            "sha256:62e4c53214b1f88f1d7251d89a57b77ad2aa904c96a1c091fd32b253bd62ef0a",
            "short",
        ),
        (
            "sha256:30a445ff61ed596e1df384de19252b376b4b518466d401f56c3449106bb17aca",
            "short",
        ),
        (
            "sha256:c98c909b449cdc58590bbfb420fdecf6bf93a70ad762a5e1afd381a6252e4f06",
            "long",
        ),
        (
            "sha256:42ca923185f8e49b8897c4000b1c42f608226e2deaa25d7e1b31f9c264945b69",
            "short",
        ),
        (
            "sha256:a75f1c88410e73a8b64ed00a8885626e3c93d83a14db012e981440ebe0683d0a",
            "long",
        ),
        (
            "sha256:bcb129394447b4a34399389d30963f534bedf6d63184503f8f85e45e4e0b52ab",
            "short",
        ),
        (
            "sha256:db4ca4bfd1e0687054c9e026376240dd57cfdb98a7a37c426348d5faae129868",
            "long",
        ),
        (
            "sha256:0f69b007ab3aa30a45e339158a60cc892961dd71a71e5575fe00a022fd727188",
            "short",
        ),
    ] {
        assert_eq!(proposal_side(seed).unwrap().as_str(), expected_side);
        let output = authority
            .execute(
                &ProposalIntent::RichImmigrant {
                    proposal_seed: seed.into(),
                    long_seed: "unused".into(),
                    short_seed: "unused".into(),
                },
                &context,
            )
            .unwrap();
        assert_eq!(output.proposal["side"], expected_side, "{seed}");
    }
}
