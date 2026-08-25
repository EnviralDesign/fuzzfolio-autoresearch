use anyhow::{Context, Result};
use serde_json::{Value, json};
use temporal_qd_campaign_seal::{
    CandidateWindowResultAdmission, admit_candidate_window_task_result,
};

fn fixture() -> Result<Value> {
    serde_json::from_str(include_str!(
        "../../../fixtures/v2_1_exact_worker_fixture.json"
    ))
    .context("parse exact V2.1 worker fixture")
}

fn admission_error(task: &Value, result: &Value) -> String {
    format!(
        "{:#}",
        admit_candidate_window_task_result(task, result)
            .expect_err("material must fail production campaign admission")
    )
}

#[test]
fn exact_v2_worker_result_passes_public_campaign_admission() -> Result<()> {
    let fixture = fixture()?;
    assert_eq!(
        admit_candidate_window_task_result(&fixture["task"], &fixture["result"])?,
        CandidateWindowResultAdmission::Admitted
    );
    Ok(())
}

#[test]
fn v2_campaign_admission_fails_closed_for_cross_version_and_runtime_drift() -> Result<()> {
    let fixture = fixture()?;
    let task = &fixture["task"];
    let result = &fixture["result"];

    let mut v1_result_for_v2_task = result.clone();
    v1_result_for_v2_task["schema_version"] = json!("temporal_graph_candidate_window_result_v1");
    assert!(
        admission_error(task, &v1_result_for_v2_task)
            .contains("result v1 is not admitted for task v2")
    );

    let mut v1_task = task.clone();
    v1_task["payload"]["schema_version"] = json!("temporal_graph_candidate_window_job_v1");
    assert!(admission_error(&v1_task, result).contains("requires candidate-window job v2"));

    let mut v1_with_v2_authority = result.clone();
    v1_with_v2_authority["schema_version"] = json!("temporal_graph_candidate_window_result_v1");
    assert!(
        admission_error(&v1_task, &v1_with_v2_authority)
            .contains("result v1 must not carry v2 execution authority")
    );

    let mut missing_receipt = result.clone();
    missing_receipt
        .as_object_mut()
        .context("fixture result must be an object")?
        .remove("precompiled_profile_execution_receipt");
    assert!(
        admission_error(task, &missing_receipt).contains("lacks its precompiled execution receipt")
    );

    let mut missing_attestation = result.clone();
    missing_attestation
        .as_object_mut()
        .context("fixture result must be an object")?
        .remove("runtime_program_identity_attestation");
    assert!(
        admission_error(task, &missing_attestation)
            .contains("lacks its runtime program identity attestation")
    );

    let mut nested_program_drift = result.clone();
    nested_program_drift["cost_view_results"]["research_conservative"]["replay_result"]["programSha256"] =
        json!("sha256:0000000000000000000000000000000000000000000000000000000000000000");
    let error = admission_error(task, &nested_program_drift);
    assert!(
        error.contains("candidate-window result v2 receipt/runtime admission failed"),
        "unexpected nested-program error: {error}"
    );
    Ok(())
}
