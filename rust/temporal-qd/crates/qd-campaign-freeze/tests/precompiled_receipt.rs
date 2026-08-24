use serde_json::{Value, json};
use temporal_qd_campaign_freeze::validate_precompiled_execution_receipt;
use temporal_qd_contract::{canonical_sha256, canonical_sha256_without_object_field};

fn sha(ch: char) -> String {
    format!("sha256:{}", ch.to_string().repeat(64))
}

fn reseal(receipt: &mut Value) {
    let identity = canonical_sha256_without_object_field(receipt, "receipt_sha256").unwrap();
    receipt["receipt_sha256"] = Value::String(identity);
}

fn fixture() -> (Value, Value) {
    let rust_build_info = json!({"source_sha256": sha('b')});
    let runtime_platform = json!({"machine":"x86_64","system":"Linux"});
    let task = json!({
        "task_id":"task-1",
        "task_kind":"temporal_graph_candidate_window",
        "payload":{
            "schema_version":"temporal_graph_candidate_window_job_v2",
            "job_id":"task-1",
            "candidate_id":"candidate_1",
            "required_capabilities":["temporal_qd_precompiled_profile_execution_v1"],
            "precompiled_profile_execution_contract":{
                "contractSha256":sha('c'),
                "authoritySha256":sha('a')
            },
            "raw_source_profile_sha256":sha('d'),
            "normalized_profile_snapshot_sha256":sha('e'),
            "authored_program_sha256":sha('f'),
            "expected_resolved_profile_snapshot_sha256":sha('1'),
            "expected_resolved_program_sha256":sha('2'),
            "required_worker_contract_hash":sha('3'),
            "required_worker_contract_schema":"replay-worker-contract-v2",
            "required_worker_image_digest":sha('4'),
            "required_worker_source_git_commit":"5".repeat(40),
            "required_worker_rust_core_hash":sha('6'),
            "required_worker_rust_build_info_sha256":canonical_sha256(&rust_build_info).unwrap(),
            "required_worker_runtime_platform_sha256":canonical_sha256(&runtime_platform).unwrap()
        }
    });
    let mut receipt = json!({
        "schema_version":"temporal_qd_precompiled_profile_execution_receipt_v1",
        "task_id":"task-1",
        "candidate_id":"candidate_1",
        "precompiled_contract_sha256":sha('c'),
        "rust_authority_sha256":sha('a'),
        "raw_source_profile_sha256":sha('d'),
        "normalized_source_profile_sha256":sha('e'),
        "authored_program_sha256":sha('f'),
        "resolved_profile_sha256":sha('1'),
        "resolved_program_sha256":sha('2'),
        "worker_contract_hash":sha('3'),
        "worker_contract_schema":"replay-worker-contract-v2",
        "worker_image_digest":sha('4'),
        "worker_image_identity_mode":"image_digest",
        "worker_source_git_commit":"5".repeat(40),
        "rust_core_hash":sha('6'),
        "rust_build_info":rust_build_info,
        "runtime_platform":runtime_platform,
        "pair_recompile_attempted":false,
        "source_profile_rewritten":false,
        "receipt_sha256":sha('0')
    });
    reseal(&mut receipt);
    let result = json!({
        "schema_version":"temporal_graph_candidate_window_result_v2",
        "source_profile_snapshot_sha256":sha('e'),
        "resolved_profile_snapshot_sha256":sha('1'),
        "program_sha256":sha('2'),
        "precompiled_profile_execution_receipt":receipt
    });
    (task, result)
}

#[test]
fn exact_precompiled_receipt_is_admitted() {
    let (task, result) = fixture();
    validate_precompiled_execution_receipt(&task, &result).unwrap();
}

#[test]
fn missing_or_cross_candidate_receipt_is_rejected() {
    let (task, mut result) = fixture();
    result
        .as_object_mut()
        .unwrap()
        .remove("precompiled_profile_execution_receipt");
    assert!(validate_precompiled_execution_receipt(&task, &result).is_err());

    let (task, mut result) = fixture();
    let receipt = &mut result["precompiled_profile_execution_receipt"];
    receipt["candidate_id"] = json!("candidate_2");
    reseal(receipt);
    assert!(validate_precompiled_execution_receipt(&task, &result).is_err());
}

#[test]
fn receipt_recompile_rewrite_and_unknown_fields_are_rejected() {
    for (field, value) in [
        ("pair_recompile_attempted", json!(true)),
        ("source_profile_rewritten", json!(true)),
        ("unknown", json!(true)),
    ] {
        let (task, mut result) = fixture();
        let receipt = &mut result["precompiled_profile_execution_receipt"];
        receipt[field] = value;
        reseal(receipt);
        assert!(validate_precompiled_execution_receipt(&task, &result).is_err());
    }
}
