use std::{fs, path::Path, process::Command};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use temporal_qd_contract::{canonical_json_line, canonical_sha256};
use temporal_qd_rotating_prefinalizer::panel_receipt::{
    SIDECAR_RESULT_SCHEMA, build, build_sidecar_to_path,
};

fn fixture(root: &Path) {
    let python = std::env::var_os("PYTHON").unwrap_or_else(|| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(if cfg!(windows) {
                "../../../../.venv/Scripts/python.exe"
            } else {
                "../../../../.venv/bin/python"
            })
            .into_os_string()
    });
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/build_v5_panel_receipt_oracle_fixture.py");
    let output = Command::new(python).arg(script).arg(root).output().unwrap();
    assert!(
        output.status.success(),
        "fixture builder failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn self_hash(value: &mut Value, field: &str) {
    value[field] = json!(canonical_sha256(value).unwrap());
}
fn file_sha(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}
fn write_json(path: &Path, value: &Value) {
    fs::write(path, canonical_json_line(value).unwrap()).unwrap();
}
fn refresh_receipt(receipt: &mut Value) {
    receipt["candidatePanelBundles"]
        .as_object_mut()
        .unwrap()
        .remove("descriptorSha256");
    self_hash(&mut receipt["candidatePanelBundles"], "descriptorSha256");
    receipt.as_object_mut().unwrap().remove("receiptSha256");
    self_hash(receipt, "receiptSha256");
}

#[test]
fn panel_bundle_sidecar_matches_rich_oracle_and_hardens_restart() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let input_path = root.path().join("input.json");
    let bundles_path = root.path().join("candidate-panel-bundles.jsonl");
    let receipt_path = root.path().join("candidate-panel-bundles-receipt.json");
    let input: Value = serde_json::from_slice(&fs::read(&input_path).unwrap()).unwrap();
    let rich = build(&input).unwrap();
    let cli = env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer");
    let cli_output = Command::new(cli)
        .args(["build-panel-bundle-sidecar"])
        .arg(&input_path)
        .arg(&bundles_path)
        .arg(&receipt_path)
        .output()
        .unwrap();
    assert!(
        cli_output.status.success(),
        "{}",
        String::from_utf8_lossy(&cli_output.stderr)
    );
    let result: Value = serde_json::from_slice(&cli_output.stdout).unwrap();
    assert_eq!(result["schemaVersion"], SIDECAR_RESULT_SCHEMA);
    let expected_bytes = rich["candidatePanelBundles"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|row| canonical_json_line(row).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(fs::read(&bundles_path).unwrap(), expected_bytes);
    let receipt: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    assert_eq!(
        result["candidatePanelBundles"],
        receipt["candidatePanelBundles"]
    );

    // The committed receipt validates the sidecar without reopening campaign
    // member data or the v4 index.
    fs::remove_file(root.path().join("evaluated-members.jsonl")).unwrap();
    fs::remove_file(root.path().join("tail-result-index-v4.json")).unwrap();
    assert_eq!(
        build_sidecar_to_path(&input_path, &bundles_path, &receipt_path).unwrap(),
        result
    );

    let mut divergent = input.clone();
    divergent["campaignRole"] = json!("prior_panel_backfill");
    divergent.as_object_mut().unwrap().remove("inputSha256");
    self_hash(&mut divergent, "inputSha256");
    write_json(&input_path, &divergent);
    assert!(build_sidecar_to_path(&input_path, &bundles_path, &receipt_path).is_err());
    write_json(&input_path, &input);

    let mut hash_tamper = receipt.clone();
    hash_tamper["candidatePanelBundles"]["rawSha256"] =
        json!("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    write_json(&receipt_path, &hash_tamper);
    assert!(build_sidecar_to_path(&input_path, &bundles_path, &receipt_path).is_err());

    let mut path_tamper = receipt.clone();
    path_tamper["candidatePanelBundles"]["path"] = json!("elsewhere.jsonl");
    refresh_receipt(&mut path_tamper);
    write_json(&receipt_path, &path_tamper);
    assert!(build_sidecar_to_path(&input_path, &bundles_path, &receipt_path).is_err());

    let mut duplicate = receipt.clone();
    let mut bytes = fs::read(&bundles_path).unwrap();
    let first = bytes
        .split_inclusive(|byte| *byte == b'\n')
        .next()
        .unwrap()
        .to_vec();
    bytes.extend(first);
    fs::write(&bundles_path, &bytes).unwrap();
    duplicate["candidatePanelBundles"]["rawSha256"] = json!(file_sha(&bytes));
    duplicate["candidatePanelBundles"]["sizeBytes"] = json!(bytes.len());
    duplicate["candidatePanelBundles"]["recordCount"] = json!(3);
    refresh_receipt(&mut duplicate);
    write_json(&receipt_path, &duplicate);
    assert!(build_sidecar_to_path(&input_path, &bundles_path, &receipt_path).is_err());

    write_json(&receipt_path, &receipt);
    fs::remove_file(&bundles_path).unwrap();
    assert!(build_sidecar_to_path(&input_path, &bundles_path, &receipt_path).is_err());
}
