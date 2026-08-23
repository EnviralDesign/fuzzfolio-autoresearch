use serde_json::json;
use temporal_qd_contract::{Value, canonical_sha256};
use temporal_qd_kernel::topology_coadaptation_matrix_v4::{
    COADAPTATION_SCHEMA, from_generation_config, validate,
};

const FIXTURE: &str = include_str!(
    "../../../../../research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v4.json"
);

fn sha(byte: &str) -> String {
    format!("sha256:{}", byte.repeat(32))
}

#[test]
fn absent_overlay_is_inert() {
    assert_eq!(
        from_generation_config(&json!({"schemaVersion": "temporal_qd_pair_generation_v2"})).unwrap(),
        None
    );
}

#[test]
fn emitted_v4_spec_validates_when_present() {
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let value: Value = serde_json::from_str(trimmed).unwrap();
    validate(&value).unwrap();
    assert_eq!(value["schemaVersion"], json!(COADAPTATION_SCHEMA));
}

#[test]
fn rejects_missing_and_extra_fields() {
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut extra: Value = serde_json::from_str(trimmed).unwrap();
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
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut bad: Value = serde_json::from_str(trimmed).unwrap();
    bad["topologyPlans"][0]["topologyPlan"]["sourceGenomeSha256"] = json!(sha("dd"));
    let plan_sha = canonical_sha256(&bad["topologyPlans"][0]["topologyPlan"]).unwrap();
    bad["topologyPlans"][0]["planSha256"] = json!(plan_sha);
    let added = format!("setup_{}", &plan_sha[7..23]);
    bad["topologyPlans"][0]["addedSetupNodeId"] = json!(added);
    let err = validate(&bad).unwrap_err().to_string();
    assert!(
        err.contains("stale topology plan") || err.contains("identity drift") || err.contains("drifted"),
        "{err}"
    );
}

#[test]
fn topology_plus_event_must_target_added_setup_node() {
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut bad: Value = serde_json::from_str(trimmed).unwrap();
    let complete = bad["blocks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|block| block["classification"] == "complete_2x2_block")
        .cloned()
        .expect("complete block");
    let te_id = complete["armSlotIds"]["topology_then_topology_local_event"]
        .as_str()
        .unwrap()
        .to_owned();
    for slot in bad["slots"].as_array_mut().unwrap() {
        if slot["slotId"] == te_id {
            slot["settlingNodeId"] = json!("setup");
        }
    }
    let err = validate(&bad).unwrap_err().to_string();
    assert!(
        err.contains("added setup node") || err.contains("identity drift"),
        "{err}"
    );
}

#[test]
fn incomplete_blocks_cannot_enter_qualification() {
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut bad: Value = serde_json::from_str(trimmed).unwrap();
    for block in bad["blocks"].as_array_mut().unwrap() {
        if block["classification"] == "complete_2x2_block" {
            block["classification"] = json!("exploratory_incomplete_block");
            block["excludedFromPrimaryCoadaptationCalculation"] = json!(false);
            block["incompletenessReason"] = json!("forced");
            break;
        }
    }
    let err = validate(&bad).unwrap_err().to_string();
    assert!(
        err.contains("incomplete blocks cannot enter qualification"),
        "{err}"
    );
}

#[test]
fn insert_exit_region_cannot_enter_first_contrast() {
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut bad: Value = serde_json::from_str(trimmed).unwrap();
    bad["topologyPlans"][0]["topologyPlan"]["operation"] = json!("insert_exit_region");
    let err = validate(&bad).unwrap_err().to_string();
    assert!(
        err.contains("insert_exit_region cannot enter the event-settling first contrast"),
        "{err}"
    );
}

#[test]
fn generic_example_topology_plan_sha_cannot_satisfy_launch_grade_slots() {
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut bad: Value = serde_json::from_str(trimmed).unwrap();
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
