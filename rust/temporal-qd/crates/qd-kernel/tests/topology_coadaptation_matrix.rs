use serde_json::json;
use temporal_qd_contract::Value;
use temporal_qd_kernel::topology_coadaptation_matrix::{
    COADAPTATION_SCHEMA, from_generation_config, validate,
};

fn contract() -> Value {
    json!({
        "schemaVersion": COADAPTATION_SCHEMA,
        "mode": "frozen_parent_topology_then_local_resource_settling_v1",
        "includeCrossover": false,
        "cloneControl": "re_evaluate_parent_on_frozen_panel",
        "productionArchiveWrite": false,
        "mutationDepth": 1,
        "arms": [
            "exact_parent_clone",
            "topology_only_child",
            "resource_parameter_only_control",
            "topology_then_bounded_resource_settling"
        ],
        "parents": [{"candidateId": "parent_a", "role": "archive"}],
        "settling": {"maxResourceSteps": 4, "families": ["resource"]},
        "morphologyNursery": {
            "schemaVersion": "temporal_qd_morphology_nursery_archive_v1",
            "productionBreedingRights": false
        }
    })
}

#[test]
fn absent_overlay_leaves_production_config_inert() {
    let config = json!({"schemaVersion": "temporal_qd_pair_generation_v2"});
    assert_eq!(from_generation_config(&config).unwrap(), None);
}

#[test]
fn valid_overlay_parses_and_rejects_production_archive_writes() {
    validate(&contract()).unwrap();
    let mut bad = contract();
    bad.as_object_mut()
        .unwrap()
        .insert("productionArchiveWrite".to_owned(), json!(true));
    assert!(validate(&bad).unwrap_err().to_string().contains("production archive"));
}
