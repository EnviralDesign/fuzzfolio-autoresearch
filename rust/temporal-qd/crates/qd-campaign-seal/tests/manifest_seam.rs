use std::fs;
use std::process::Command;

use serde_json::{Value, json};
use tempfile::tempdir;
use temporal_qd_campaign_seal::{MANIFEST_SCHEMA, OPERATION, TRANSACTION_PATH};
use temporal_qd_contract::{CONTRACT_VERSION, canonical_json_line, canonical_sha256};

const HASH: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[test]
fn python_production_manifest_reaches_real_campaign_seal_parser() {
    let root = tempdir().unwrap();
    let mut manifest = json!({
        "schemaVersion": MANIFEST_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "operation": OPERATION,
        "runtimeAuthoritySha256": HASH,
        "sourcePath": root.path().join("absent-source.json").to_string_lossy(),
        "sourceSha256": HASH,
        "evaluationPopulationPath": root.path().join("absent-evaluation.json").to_string_lossy(),
        "evaluationPopulationSha256": HASH,
        "generationIndex": 1,
        "minimumTotalTrades": 8,
        "minimumTradesPerWindow": 4,
        "capTrades": 20,
        "provisionalLimit": 128,
        "resultPath": TRANSACTION_PATH,
    });
    let identity = canonical_sha256(&manifest).unwrap();
    manifest
        .as_object_mut()
        .unwrap()
        .insert("manifestSha256".into(), Value::String(identity));
    let manifest_path = root.path().join("manifest.json");
    fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_temporal-qd-campaign-seal"))
        .arg("--manifest")
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("campaign seal source"), "{stderr}");
    assert!(!stderr.contains("fields are not exact"), "{stderr}");
    assert!(!stderr.contains("lacks runtime authority"), "{stderr}");
}
