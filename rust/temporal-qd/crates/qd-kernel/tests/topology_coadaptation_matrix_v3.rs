use serde_json::json;
use temporal_qd_contract::{Value, canonical_sha256};
use temporal_qd_kernel::topology_coadaptation_matrix_v3::{
    COADAPTATION_SCHEMA, from_generation_config, validate,
};

const FIXTURE: &str = include_str!("../../../../../research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v3.json");

fn sha(byte: &str) -> String {
    format!("sha256:{}", byte.repeat(32))
}

fn topology_plan(source: &str) -> Value {
    json!({
        "schemaVersion": "evolvable_module_topology_plan_v1",
        "operatorSchema": "evolvable_module_topology_operator_v1",
        "operation": "insert_setup",
        "sourceGenomeSha256": source,
        "arguments": {"edgeId": "start_setup", "guard": {"kind": "always"}, "kind": "context"}
    })
}

fn sealed_minimal_contract() -> Value {
    let long = sha("aa");
    let short = sha("bb");
    let plan = topology_plan(&short);
    let plan_sha = canonical_sha256(&plan).expect("plan sha");
    let added = format!("setup_{}", &plan_sha[7..23]);
    let delta = json!({
        "operation": "insert_setup",
        "planSha256": plan_sha,
        "sourceGenomeSha256": short,
        "addedSetupNodeId": added,
    });
    let delta_sha = canonical_sha256(&delta).expect("delta sha");
    let mut body = json!({
        "schemaVersion": COADAPTATION_SCHEMA,
        "mode": "frozen_parent_insert_setup_then_topology_local_event_v3",
        "includeCrossover": false,
        "cloneControl": "re_evaluate_parent_on_frozen_panel",
        "productionArchiveWrite": false,
        "mutationDepth": 1,
        "morphologyNurseryDeferred": true,
        "lexicographicFirstSettlingPlanForbidden": true,
        "parents": [{
            "candidateId": "parent_a",
            "role": "archive",
            "longProgramSha256": long,
            "shortProgramSha256": short
        }],
        "panelIdentities": {
            "developmentPanelId": "panel-3",
            "developmentRole": "discovery_and_selection",
            "replicationPanelIds": ["panel-1", "panel-2"],
            "replicationRole": "inspected_replication_not_untouched_confirmation",
            "futureConfirmationPanel": {
                "createdInThisTask": false,
                "requiredBeforeProductionConclusion": true,
                "authorityMustBeBoundBeforeLaunch": true,
                "label": "future_untouched_confirmation_panel"
            },
            "rotatingEvidenceSha256": sha("cc")
        },
        "firstExperimentOperation": "insert_setup",
        "forbiddenFirstExperimentOperations": ["insert_exit_region"],
        "topologyLocalEventRequired": true,
        "arms": [
            "exact_parent_clone",
            "topology_only_child",
            "event_only_control",
            "topology_then_topology_local_event"
        ],
        "topologyPlans": [{
            "planId": "insert_setup|parent_a|short",
            "parentCandidateId": "parent_a",
            "side": "short",
            "topologyPlan": plan,
            "planSha256": plan_sha,
            "addedSetupNodeId": added,
            "applicability": "source_genome_matches_parent_side_program",
            "topologySemanticDeltaIdentity": delta_sha
        }],
        "eventPrimitives": [{
            "primitiveId": "event|parent_a|short|TEST",
            "parentCandidateId": "parent_a",
            "side": "short",
            "indicatorId": "TEST",
            "contract": {"kind": "raw_event"},
            "originalNodeId": "setup",
            "originalNodeZone": "setup",
            "source": "v38_recovered_directional_event_insert"
        }],
        "slots": [
            {
                "slotId": "clone|parent_a",
                "arm": "exact_parent_clone",
                "parentCandidateId": "parent_a",
                "side": null,
                "eligibility": "eligible",
                "topologyPlanId": null,
                "eventPrimitiveId": null,
                "settlingNodeId": null,
                "ineligibilityReason": null
            },
            {
                "slotId": "topology_only|parent_a|short",
                "arm": "topology_only_child",
                "parentCandidateId": "parent_a",
                "side": "short",
                "eligibility": "eligible",
                "topologyPlanId": "insert_setup|parent_a|short",
                "eventPrimitiveId": null,
                "settlingNodeId": null,
                "ineligibilityReason": null
            },
            {
                "slotId": "event_only|parent_a|short",
                "arm": "event_only_control",
                "parentCandidateId": "parent_a",
                "side": "short",
                "eligibility": "eligible",
                "topologyPlanId": null,
                "eventPrimitiveId": "event|parent_a|short|TEST",
                "settlingNodeId": "setup",
                "ineligibilityReason": null
            },
            {
                "slotId": "topology_then_event|parent_a|short",
                "arm": "topology_then_topology_local_event",
                "parentCandidateId": "parent_a",
                "side": "short",
                "eligibility": "eligible",
                "topologyPlanId": "insert_setup|parent_a|short",
                "eventPrimitiveId": "event|parent_a|short|TEST",
                "settlingNodeId": added,
                "ineligibilityReason": null
            }
        ],
        "settling": {
            "kind": "directional_event_insert",
            "mustTargetAddedSetupNodeId": true,
            "selection": "frozen_matched_v38_event_primitive_set",
            "matchedControlSite": "parent_existing_setup_if_event_free_else_ineligible",
            "ineligibleCellsRemainExplicit": true
        },
        "successCalculation": {
            "schemaVersion": "temporal_qd_topology_coadaptation_success_v3",
            "metricEquality": "canonical_json_number_roundtrip_with_1e-12_encoding_floor",
            "parentBeat": "child_net_strictly_greater_under_canonical_metric_identity",
            "riskQualifiedBeat": "parentBeat_and_non_worse_worst_window",
            "fullEconomicPhenotypeTie": "equal_net_worst_median_active_window_fraction",
            "supportDirectionQualityGates": "unchanged_production_gates",
            "activityCostMechanismRequired": true,
            "parentBalancingRequired": true,
            "eventPlanBalancingRequired": true,
            "requireReplicationPanelSurvivalForPromisingClaim": true,
            "requireUntouchedConfirmationPanelBeforeProductionConclusion": true,
            "doNotPromoteOnDevelopmentPanelAlone": true,
            "noveltyIsNotQuality": true
        },
        "notAdmittedOnFrontGenerationPath": true
    });
    let hash = canonical_sha256(&body).expect("contract sha");
    body.as_object_mut()
        .expect("object")
        .insert("contractSha256".to_owned(), json!(hash));
    body
}

#[test]
fn absent_overlay_is_inert() {
    assert_eq!(
        from_generation_config(&json!({"schemaVersion": "temporal_qd_pair_generation_v2"})).unwrap(),
        None
    );
}

#[test]
fn validates_minimal_parent_bound_contract() {
    validate(&sealed_minimal_contract()).unwrap();
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
}

#[test]
fn topology_plan_rejects_another_parent_genome() {
    let mut bad = sealed_minimal_contract();
    bad["topologyPlans"][0]["topologyPlan"]["sourceGenomeSha256"] = json!(sha("dd"));
    let sha = canonical_sha256(&bad["topologyPlans"][0]["topologyPlan"]).unwrap();
    bad["topologyPlans"][0]["planSha256"] = json!(sha);
    let added = format!("setup_{}", &sha[7..23]);
    bad["topologyPlans"][0]["addedSetupNodeId"] = json!(added);
    let err = validate(&bad).unwrap_err().to_string();
    assert!(
        err.contains("stale topology plan") || err.contains("identity drift"),
        "{err}"
    );
}

#[test]
fn topology_plus_event_must_target_added_setup_node() {
    let mut bad = sealed_minimal_contract();
    bad["slots"][3]["settlingNodeId"] = json!("entry");
    let err = validate(&bad).unwrap_err().to_string();
    assert!(
        err.contains("newly added setup node") || err.contains("identity drift"),
        "{err}"
    );
}

#[test]
fn insert_exit_region_cannot_enter_first_contrast() {
    let mut bad = sealed_minimal_contract();
    bad["topologyPlans"][0]["topologyPlan"]["operation"] = json!("insert_exit_region");
    let err = validate(&bad).unwrap_err().to_string();
    assert!(
        err.contains("insert_exit_region cannot enter the event-settling first contrast"),
        "{err}"
    );
}

#[test]
fn generic_example_topology_plan_sha_cannot_satisfy_launch_grade_slots() {
    let mut bad = sealed_minimal_contract();
    bad["topologyPlans"][0]["topologyPlan"]
        .as_object_mut()
        .unwrap()
        .insert(
            "v38ExampleOperatorPlanSha256".to_owned(),
            json!(sha("11")),
        );
    let err = validate(&bad).unwrap_err().to_string();
    assert!(
        err.contains("generic example topology plan SHAs cannot satisfy launch-grade slots")
            || err.contains("unexpected schema"),
        "{err}"
    );
}

#[test]
fn panel_1_and_2_are_labeled_replication() {
    let contract = sealed_minimal_contract();
    assert_eq!(
        contract["panelIdentities"]["replicationRole"],
        json!("inspected_replication_not_untouched_confirmation")
    );
    validate(&contract).unwrap();
}

#[test]
fn emitted_v3_spec_validates_when_present() {
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let value: Value = serde_json::from_str(trimmed).unwrap();
    validate(&value).unwrap();
}
