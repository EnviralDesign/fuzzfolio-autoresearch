use std::fs;
use std::process::Command;

use serde_json::{Value, json};
use tempfile::tempdir;
use temporal_qd_campaign_seal::{
    CAMPAIGN_OUTPUT_CHECKPOINT_PATH, CAMPAIGN_OUTPUT_MANIFEST_PATH,
    CAMPAIGN_OUTPUT_MANIFEST_SCHEMA, CAMPAIGN_OUTPUT_OPERATION,
};
use temporal_qd_contract::{CONTRACT_VERSION, canonical_json_line, canonical_sha256};

const HASH: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn write_manifest(root: &std::path::Path, file_name: &str) -> std::path::PathBuf {
    let input_path = root.join("campaign-input-checkpoint.json");
    let gateway_path = root.join("gateway-execution-receipt.json");
    fs::write(&input_path, b"{}\n").unwrap();
    fs::write(&gateway_path, b"{}\n").unwrap();
    let mut manifest = json!({
        "schemaVersion": CAMPAIGN_OUTPUT_MANIFEST_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "operation": CAMPAIGN_OUTPUT_OPERATION,
        "runtimeAuthoritySha256": HASH,
        "campaignInputCheckpointPath": input_path.canonicalize().unwrap(),
        "campaignInputCheckpointSha256": HASH,
        "gatewayExecutionReceiptPath": gateway_path.canonicalize().unwrap(),
        "gatewayExecutionReceiptSha256": HASH,
        "generationIndex": 1,
        "campaignRole": "proposal_current_panel",
        "panelId": "panel-a",
        "rotatingEvidenceSha256": HASH,
        "panel": {"panelId": "panel-a"},
        "cohortSource": {
            "kind": "proposal_evaluation_population",
            "sourceSemanticSha256": HASH,
            "candidateCount": 1,
            "selectionSha256": Value::Null,
        },
        "minimumTotalTrades": 8,
        "minimumTradesPerWindow": 4,
        "capTrades": 20,
        "provisionalLimit": 128,
        "resultPath": CAMPAIGN_OUTPUT_CHECKPOINT_PATH,
    });
    let identity = canonical_sha256(&manifest).unwrap();
    manifest
        .as_object_mut()
        .unwrap()
        .insert("manifestSha256".into(), Value::String(identity));
    let path = root.join(file_name);
    fs::write(&path, canonical_json_line(&manifest).unwrap()).unwrap();
    path
}

#[test]
fn campaign_output_manifest_reaches_the_checkpoint_input_boundary() {
    let root = tempdir().unwrap();
    let manifest_path = write_manifest(root.path(), CAMPAIGN_OUTPUT_MANIFEST_PATH);
    let output = Command::new(env!("CARGO_BIN_EXE_temporal-qd-campaign-seal"))
        .arg("--campaign-output-manifest")
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("campaign-input checkpoint"), "{stderr}");
    assert!(!stderr.contains("campaign seal source"), "{stderr}");
    assert!(!stderr.contains("usage:"), "{stderr}");
}

#[test]
fn retired_campaign_seal_cli_modes_are_rejected() {
    let root = tempdir().unwrap();
    let path = root.path().join("unused.json");
    fs::write(&path, b"{}\n").unwrap();
    for retired in ["--manifest", "--build-source-manifest"] {
        let output = Command::new(env!("CARGO_BIN_EXE_temporal-qd-campaign-seal"))
            .arg(retired)
            .arg(&path)
            .output()
            .unwrap();
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("usage: temporal-qd-campaign-seal --campaign-output-manifest PATH"),
            "{stderr}"
        );
    }
}

#[test]
fn campaign_output_manifest_filename_is_fixed() {
    let root = tempdir().unwrap();
    let manifest_path = write_manifest(root.path(), "manifest.json");
    let output = Command::new(env!("CARGO_BIN_EXE_temporal-qd-campaign-seal"))
        .arg("--campaign-output-manifest")
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("campaign-output manifest path is not fixed")
    );
}
