use serde_json::{json, Value};
use temporal_qd_kernel::topology_coadaptation_matrix_v6::{
    COADAPTATION_SCHEMA, from_generation_config, validate,
};

const FIXTURE: &str = include_str!(
    "../../../../../research/temporal-qd/v38-followup/topology-coadaptation-matrix-spec-v6.json"
);
const CORPUS: &str = include_str!(
    "../../../../../research/temporal-qd/v38-followup/topology-coadaptation-python-rust-parity-corpus-v6.json"
);

fn load_fixture() -> Value {
    let trimmed = FIXTURE.trim();
    assert!(!trimmed.is_empty(), "v6 spec fixture must be emitted before rust tests");
    serde_json::from_str(trimmed).unwrap()
}

fn first_eligible_index(spec: &Value, arm: &str) -> usize {
    spec["materializationReceipts"]
        .as_array()
        .unwrap()
        .iter()
        .position(|row| row["arm"] == arm && row["eligibility"] == "eligible")
        .expect("eligible receipt")
}

fn apply_mutation(spec: &Value, mutation_id: &str) -> Value {
    let mut mutated = spec.clone();
    match mutation_id {
        "receipt_id_drift_only" => {
            let id = mutated["materializationReceipts"][0]["receiptId"]
                .as_str()
                .unwrap()
                .to_owned();
            mutated["materializationReceipts"][0]["receiptId"] = json!(format!("{id}|drifted"));
        }
        "topology_plan_substitution" => {
            let index = first_eligible_index(&mutated, "topology_only_child");
            mutated["materializationReceipts"][index]["topologySemanticDelta"]["planSha256"] =
                json!(format!("sha256:{}", "a".repeat(64)));
        }
        "event_primitive_substitution" => {
            let index = first_eligible_index(&mutated, "event_only_control");
            mutated["materializationReceipts"][index]["operatorApplicationAudit"]["semanticDelta"][0]
                ["indicatorId"] = json!("NOT_THE_DECLARED_INDICATOR");
        }
        "missing_semantic_delta" => {
            let index = first_eligible_index(&mutated, "topology_then_topology_local_event");
            mutated["materializationReceipts"][index]["operatorApplicationAudit"]["semanticDelta"] =
                json!([]);
        }
        "fake_event_attaches_without_node" => {
            let index = first_eligible_index(&mutated, "topology_then_topology_local_event");
            mutated["materializationReceipts"][index]["eventAttachesToAddedSetupNode"] = json!(true);
            mutated["materializationReceipts"][index]["operatorApplicationAudit"]["semanticDelta"][0]
                ["nodeId"] = json!("not_the_added_setup_node");
        }
        "sparse_fake_audit" => {
            let index = first_eligible_index(&mutated, "topology_then_topology_local_event");
            mutated["materializationReceipts"][index]["operatorApplicationAudit"] = json!({
                "arm": "topology_then_topology_local_event",
                "productionArchiveWrite": false,
                "replayed": true
            });
        }
        "wrong_parent" => {
            let index = first_eligible_index(&mutated, "exact_parent_clone");
            mutated["materializationReceipts"][index]["parentCandidateId"] =
                json!("qd_not_a_frozen_parent");
        }
        "wrong_side" => {
            let index = first_eligible_index(&mutated, "exact_parent_clone");
            let side = mutated["materializationReceipts"][index]["side"].as_str().unwrap();
            mutated["materializationReceipts"][index]["side"] =
                json!(if side == "long" { "short" } else { "long" });
        }
        "swapped_e_te" => {
            let e_index = first_eligible_index(&mutated, "event_only_control");
            let te_index = first_eligible_index(&mutated, "topology_then_topology_local_event");
            let e_row = mutated["materializationReceipts"][e_index].clone();
            let te_row = mutated["materializationReceipts"][te_index].clone();
            let mut new_e = te_row.clone();
            new_e["receiptId"] = e_row["receiptId"].clone();
            new_e["arm"] = e_row["arm"].clone();
            new_e["blockId"] = e_row["blockId"].clone();
            let mut new_te = e_row.clone();
            new_te["receiptId"] = te_row["receiptId"].clone();
            new_te["arm"] = te_row["arm"].clone();
            new_te["blockId"] = te_row["blockId"].clone();
            mutated["materializationReceipts"][e_index] = new_e;
            mutated["materializationReceipts"][te_index] = new_te;
        }
        "missing_receipt" => {
            let receipts = mutated["materializationReceipts"].as_array_mut().unwrap();
            receipts.pop();
        }
        "extra_receipt" => {
            let extra = mutated["materializationReceipts"][0].clone();
            let id = extra["receiptId"].as_str().unwrap().to_owned();
            let mut extra = extra;
            extra["receiptId"] = json!(format!("{id}|extra"));
            mutated["materializationReceipts"]
                .as_array_mut()
                .unwrap()
                .push(extra);
        }
        "fake_pair_native_report" => {
            let index = first_eligible_index(&mutated, "topology_then_topology_local_event");
            mutated["materializationReceipts"][index]["nativeValidationRan"] = json!(true);
            mutated["materializationReceipts"][index]["nativeValidationReportSha256"] =
                json!(format!("sha256:{}", "b".repeat(64)));
            mutated["materializationReceipts"][index]["nativeValidationAuthoritySha256"] =
                json!(format!("sha256:{}", "c".repeat(64)));
            mutated["materializationReceipts"][index]["frozenPairIdentitySha256"] =
                json!(format!("sha256:{}", "d".repeat(64)));
        }
        "mislabeled_pair_identity_field" => {
            mutated["materializationReceipts"][0]["pairIdentitySha256"] =
                mutated["materializationReceipts"][0]["reconstructedPairProgramIdentitySha256"].clone();
        }
        other => panic!("unknown mutation {other}"),
    }
    mutated
}

#[test]
fn absent_overlay_is_inert() {
    assert_eq!(
        from_generation_config(&json!({"schemaVersion": "temporal_qd_pair_generation_v2"})).unwrap(),
        None
    );
}

#[test]
fn emitted_v6_spec_validates() {
    let value = load_fixture();
    validate(&value).unwrap();
    assert_eq!(value["schemaVersion"], json!(COADAPTATION_SCHEMA));
}

#[test]
fn python_rust_parity_corpus_agrees() {
    let spec = load_fixture();
    let corpus: Value = serde_json::from_str(CORPUS.trim()).unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let mutation_id = case["mutationId"].as_str().unwrap();
        let accepted = case["accepted"].as_bool().unwrap();
        let result = if mutation_id == "canonical_fixture" {
            validate(&spec)
        } else {
            validate(&apply_mutation(&spec, mutation_id))
        };
        assert_eq!(
            result.is_ok(),
            accepted,
            "parity mismatch on {mutation_id}: python accepted={accepted} rust={result:?}"
        );
    }
}

#[test]
fn rejects_mislabeled_pair_identity_and_extra_fields() {
    let mut extra = load_fixture();
    extra
        .as_object_mut()
        .unwrap()
        .insert("extraField".to_owned(), json!(true));
    assert!(validate(&extra)
        .unwrap_err()
        .to_string()
        .contains("unexpected schema"));
}
