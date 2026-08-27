use anyhow::{Result, bail};
use serde_json::json;
use std::{env, path::Path};

fn main() {
    if let Err(error) = run() {
        eprintln!("ERROR: {error:#}");
        std::process::exit(2);
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 || args[1] != "--checkpoint" {
        bail!("usage: temporal-qd-campaign-input-open-json --checkpoint PATH");
    }
    let checkpoint =
        temporal_qd_campaign_freeze::open_v5_campaign_input_checkpoint(Path::new(&args[2]))?;
    println!(
        "{}",
        temporal_qd_contract::canonical_json(&json!({
            "schemaVersion": "temporal_qd_v5_campaign_input_open_result_v1",
            "panelId": checkpoint.panel_id,
            "generationIndex": checkpoint.generation_index,
            "campaignRole": checkpoint.campaign_role,
            "authorityId": checkpoint.authority_id,
            "campaignSha256": checkpoint.campaign_sha256,
            "evaluationIdentitySha256": checkpoint.evaluation_identity_sha256,
            "manifestSha256": checkpoint.manifest_sha256,
            "nativeRuntimeAuthoritySha256": checkpoint.native_runtime_authority_sha256,
            "checkpointSha256": checkpoint.checkpoint_sha256,
            "taskMatrixSha256": checkpoint.task_matrix_sha256,
            "candidateCount": checkpoint.candidate_count,
            "windowCount": checkpoint.window_count,
            "taskCount": checkpoint.task_count,
            "taskPackRawSha256": checkpoint.task_pack_raw_sha256,
            "taskPackSizeBytes": checkpoint.task_pack_size_bytes,
            "cohortPopulationSha256": checkpoint.cohort_population_sha256,
            "cohortPopulationRawSha256": checkpoint.cohort_population_raw_sha256,
            "cohortPopulationSizeBytes": checkpoint.cohort_population_size_bytes,
        }))?
    );
    Ok(())
}
