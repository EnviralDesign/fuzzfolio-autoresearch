use serde_json::json;
use temporal_qd_contract::{Value, canonical_sha256};
use temporal_qd_kernel::topology_coadaptation_matrix::{
    COADAPTATION_SCHEMA, from_generation_config, validate,
};

const FIXTURE: &str = include_str!("../../../../../research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v2.json");

fn minimal_contract() -> Value {
    json!({
        "schemaVersion": COADAPTATION_SCHEMA,
        "mode": "frozen_parent_topology_then_matched_settling_v2",
        "includeCrossover": false,
        "cloneControl": "re_evaluate_parent_on_frozen_panel",
        "productionArchiveWrite": false,
        "mutationDepth": 1,
        "morphologyNurseryDeferred": true,
        "parents": [{"candidateId": "parent_a", "role": "archive"}],
        "panelIdentities": {
            "developmentPanelId": "panel-3",
            "confirmationPanelIds": ["panel-1", "panel-2"],
            "rotatingEvidenceSha256": "sha256:abababababababababababababababababababababababababababababababab"
        },
        "topologyPlans": [
            {
                "planId": "insert_setup",
                "operation": "insert_setup",
                "operatorSchema": "evolvable_module_topology_operator_v1",
                "schemaVersion": "evolvable_module_topology_plan_v1",
                "arguments": {"edgeId": "start_setup", "guard": {"kind": "always"}, "kind": "context"},
                "v38ExampleOperatorPlanSha256": "sha256:1111111111111111111111111111111111111111111111111111111111111111"
            },
            {
                "planId": "insert_exit_region",
                "operation": "insert_exit_region",
                "operatorSchema": "evolvable_module_topology_operator_v1",
                "schemaVersion": "evolvable_module_topology_plan_v1",
                "arguments": {"guard": {"kind": "always"}, "kind": "region", "priority": 1},
                "v38ExampleOperatorPlanSha256": "sha256:2222222222222222222222222222222222222222222222222222222222222222"
            }
        ],
        "noMixedTopologyOperations": true,
        "preserveRawAndSettledIdentities": true,
        "independentConfirmationRequired": true,
        "firstExperimentContrast": "resource_semantic_directional_event_insert",
        "firstExperimentJustification": "V38's recovered positive resource tail was directional_event_insert around two archive parents, not period/range/lookback/timeframe mutation. The first experiment therefore matches topology children against a directional_event_insert settling lane and an identical resource-semantic control. Parameter-only settling remains specified but is not the first contrast because parameter learning was sparsely sampled rather than demonstrated.",
        "contrasts": [
            {
                "contrastId": "topology_plus_parameter_only_vs_parameter_only_control",
                "settlingLane": "parameter_only",
                "controlLane": "parameter_only",
                "includedInFirstExperiment": false
            },
            {
                "contrastId": "topology_plus_resource_semantic_vs_resource_semantic_control",
                "settlingLane": "resource_semantic",
                "controlLane": "resource_semantic",
                "includedInFirstExperiment": true
            }
        ],
        "arms": [
            "exact_parent_clone",
            "topology_only_child",
            "parameter_only_control",
            "resource_semantic_control",
            "topology_then_parameter_only_settling",
            "topology_then_resource_semantic_settling"
        ],
        "firstExperimentArms": [
            "exact_parent_clone",
            "topology_only_child",
            "resource_semantic_control",
            "topology_then_resource_semantic_settling"
        ],
        "settling": {
            "algorithmId": "deterministic_matched_settling_v2",
            "parameterOnly": {
                "eligibleKinds": [
                    "indicator_period_mutate",
                    "indicator_range_mutate",
                    "indicator_timeframe_mutate",
                    "indicator_lookback_mutate"
                ],
                "forbiddenKinds": [
                    "evidence_group_create",
                    "evidence_group_remove",
                    "evidence_group_split",
                    "evidence_group_merge",
                    "evidence_member_insert",
                    "evidence_member_remove",
                    "evidence_weight_mutate",
                    "evidence_threshold_mutate",
                    "indicator_instance_insert",
                    "indicator_instance_remove",
                    "indicator_substitute",
                    "directional_event_insert",
                    "directional_event_remove",
                    "directional_event_substitute"
                ],
                "maxSettlingPlans": 1,
                "planSelection": "lexicographic_canonical_construction_identity",
                "applicationMode": "sequential_single_step_from_pre_settling_genome",
                "evaluateIntermediatesOnDevelopmentPanel": true,
                "winnerSelection": "only_candidate_when_maxSettlingPlans_is_1",
                "matchedControlBudget": "identical_eligible_kind_set_order_and_maxSettlingPlans"
            },
            "resourceSemantic": {
                "eligibleKinds": ["directional_event_insert"],
                "forbiddenKinds": [
                    "evidence_group_create",
                    "evidence_group_remove",
                    "evidence_group_split",
                    "evidence_group_merge",
                    "evidence_member_insert",
                    "evidence_member_remove",
                    "evidence_weight_mutate",
                    "evidence_threshold_mutate",
                    "indicator_instance_insert",
                    "indicator_instance_remove",
                    "indicator_substitute",
                    "indicator_timeframe_mutate",
                    "indicator_lookback_mutate",
                    "indicator_period_mutate",
                    "indicator_range_mutate",
                    "directional_event_remove",
                    "directional_event_substitute"
                ],
                "maxSettlingPlans": 1,
                "planSelection": "lexicographic_canonical_construction_identity",
                "applicationMode": "sequential_single_step_from_pre_settling_genome",
                "evaluateIntermediatesOnDevelopmentPanel": true,
                "winnerSelection": "only_candidate_when_maxSettlingPlans_is_1",
                "matchedControlBudget": "identical_eligible_kind_set_order_and_maxSettlingPlans"
            },
            "developmentPanelUse": "frozen_v38_development_panel_only",
            "independentPanelConfirmation": "required_before_any_production_conclusion"
        },
        "slotBudget": {
            "cloneCountPerParent": 1,
            "topologyOnlyCountPerParentPerPlan": 1,
            "parameterOnlyControlCountPerParent": 1,
            "resourceSemanticControlCountPerParent": 1,
            "topologyThenParameterSettlingCountPerParentPerPlan": 1,
            "topologyThenSemanticSettlingCountPerParentPerPlan": 1,
            "firstExperimentSlotCount": 6
        },
        "successRule": {
            "noveltyIsNotQuality": true,
            "requireRepeatablePositiveParentRelativeTail": true,
            "forbidSystematicallyWorseWorstWindow": true,
            "requireIndependentPanelSurvival": true,
            "doNotPromoteOnDevelopmentPanelAlone": true
        },
        "notAdmittedOnFrontGenerationPath": true
    })
}

fn sealed_minimal_contract() -> Value {
    let mut value = minimal_contract();
    let hash = canonical_sha256(&value).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("contractSha256".to_owned(), json!(hash));
    value
}

#[test]
fn absent_overlay_leaves_production_config_inert() {
    let config = json!({"schemaVersion": "temporal_qd_pair_generation_v2"});
    assert_eq!(from_generation_config(&config).unwrap(), None);
}

#[test]
fn valid_overlay_parses_and_rejects_production_archive_writes() {
    validate(&sealed_minimal_contract()).unwrap();
    let mut bad = sealed_minimal_contract();
    bad.as_object_mut()
        .unwrap()
        .insert("productionArchiveWrite".to_owned(), json!(true));
    assert!(validate(&bad)
        .unwrap_err()
        .to_string()
        .contains("production archive"));
}

#[test]
fn rejects_missing_and_extra_fields() {
    let mut extra = sealed_minimal_contract();
    extra
        .as_object_mut()
        .unwrap()
        .insert("extraField".to_owned(), json!(true));
    assert!(validate(&extra)
        .unwrap_err()
        .to_string()
        .contains("unexpected schema"));
    let mut missing = sealed_minimal_contract();
    missing.as_object_mut().unwrap().remove("parents");
    assert!(validate(&missing)
        .unwrap_err()
        .to_string()
        .contains("unexpected schema"));
}

#[test]
fn parameter_only_lane_cannot_admit_event_insert() {
    let mut bad = sealed_minimal_contract();
    bad["settling"]["parameterOnly"]["eligibleKinds"] = json!([
        "directional_event_insert",
        "indicator_period_mutate",
        "indicator_range_mutate",
        "indicator_timeframe_mutate"
    ]);
    let err = validate(&bad).unwrap_err().to_string();
    assert!(
        err.contains("parameter-only lane") || err.contains("drifted"),
        "{err}"
    );
}

#[test]
fn emitted_v38_spec_validates_when_present() {
    let value: Value = serde_json::from_str(FIXTURE.trim()).unwrap();
    validate(&value).unwrap();
}
