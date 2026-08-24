use serde_json::json;
use temporal_qd_contract::Value;
use temporal_qd_kernel::topology_coadaptation_matrix_v5::{
    COADAPTATION_SCHEMA, from_generation_config, validate,
};

const FIXTURE: &str = include_str!(
    "../../../../../research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v5.json"
);

#[test]
fn absent_overlay_is_inert() {
    assert_eq!(
        from_generation_config(&json!({"schemaVersion": "temporal_qd_pair_generation_v2"})).unwrap(),
        None
    );
}

#[test]
fn emitted_v5_spec_validates_when_present() {
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let value: Value = serde_json::from_str(trimmed).unwrap();
    validate(&value).unwrap();
    assert_eq!(value["schemaVersion"], json!(COADAPTATION_SCHEMA));
}

#[test]
fn rejects_mislabeled_pair_identity_and_extra_fields() {
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

    let mut labeled: Value = serde_json::from_str(trimmed).unwrap();
    labeled["materializationReceipts"][0]["pairIdentitySha256"] =
        labeled["materializationReceipts"][0]["reconstructedPairProgramIdentitySha256"].clone();
    assert!(validate(&labeled)
        .unwrap_err()
        .to_string()
        .contains("pair or native"));
}

#[test]
fn rejects_te_parent_chain_break() {
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut broken: Value = serde_json::from_str(trimmed).unwrap();
    let receipts = broken["materializationReceipts"].as_array_mut().unwrap();
    for receipt in receipts {
        if receipt["arm"] == "topology_then_topology_local_event"
            && receipt["eligibility"] == "eligible"
        {
            let child = receipt["changedSideGenomeSha256"].clone();
            receipt["applicationParentGenomeSha256"] = child;
            break;
        }
    }
    assert!(validate(&broken)
        .unwrap_err()
        .to_string()
        .contains("TE application parent"));
}

#[test]
fn reconstructed_pair_identity_must_recompute() {
    let trimmed = FIXTURE.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut drifted: Value = serde_json::from_str(trimmed).unwrap();
    let receipts = drifted["materializationReceipts"].as_array_mut().unwrap();
    for receipt in receipts {
        if receipt["eligibility"] == "eligible" {
            receipt["reconstructedPairProgramIdentitySha256"] =
                json!(format!("sha256:{}", "ab".repeat(32)));
            break;
        }
    }
    let err = validate(&drifted).unwrap_err().to_string();
    assert!(
        err.contains("reconstructed pair") || err.contains("complete-block") || err.contains("drifted"),
        "{err}"
    );
}
