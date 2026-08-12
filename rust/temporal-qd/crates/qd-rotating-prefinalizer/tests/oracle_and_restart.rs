use std::{fs, path::Path, process::Command};

use serde_json::{Value, json};
use tempfile::tempdir;
use temporal_qd_contract::{CONTRACT_VERSION, canonical_json_line, canonical_sha256};
use temporal_qd_rotating_prefinalizer::{
    MANIFEST_SCHEMA, OPERATION, TRANSACTION_PATH, execute_manifest,
};

fn self_hash(value: &mut Value, field: &str) {
    value[field] = json!(canonical_sha256(value).unwrap());
}

fn oracle_python() -> std::ffi::OsString {
    std::env::var_os("PYTHON").unwrap_or_else(|| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(if cfg!(windows) {
                "../../../../.venv/Scripts/python.exe"
            } else {
                "../../../../.venv/bin/python"
            })
            .into_os_string()
    })
}

#[test]
fn python_oracle_fixture_matches_source_bytes_and_restart_is_compact() {
    let root = tempdir().unwrap();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/build_oracle_fixture.py");
    // CI can set PYTHON. The repository venv is the normal local oracle,
    // avoiding the Windows-App-Execution-Alias stub named `python`.
    let output = Command::new(oracle_python())
        .arg(script)
        .arg(root.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture builder failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let input: Value =
        serde_json::from_slice(&fs::read(root.path().join("input.json")).unwrap()).unwrap();
    let mut manifest = json!({"schemaVersion":MANIFEST_SCHEMA,"contractVersion":CONTRACT_VERSION,"operation":OPERATION,"inputPath":root.path().join("input.json").to_string_lossy(),"inputSha256":input["inputSha256"],"resultPath":TRANSACTION_PATH});
    self_hash(&mut manifest, "manifestSha256");
    let manifest_path = root.path().join("prefinalizer-manifest.json");
    fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();
    let first = execute_manifest(&manifest_path).unwrap();
    let transaction = &first["transaction"];
    assert_eq!(transaction["status"], "ready_for_finalizer");
    let expected: Value =
        serde_json::from_slice(&fs::read(root.path().join("expected.json")).unwrap()).unwrap();
    assert_eq!(transaction["cohort"], expected["cohort"]);
    assert_eq!(transaction["provisional"], expected["provisional"]);
    let source: Value =
        serde_json::from_slice(&fs::read(root.path().join("source.json")).unwrap()).unwrap();
    assert_eq!(source, expected["source"]);
    let second = execute_manifest(&manifest_path).unwrap();
    assert_eq!(second["restart"], true);
    assert_eq!(
        second["restartValidation"],
        "compact_transaction_and_output_hashes"
    );
    // The source files are intentionally not revisited on a completed restart.
    fs::remove_file(root.path().join("proposal-members.jsonl")).unwrap();
    assert_eq!(execute_manifest(&manifest_path).unwrap()["restart"], true);
}

#[test]
fn missing_current_panel_bundle_is_not_relabelled_as_backfill() {
    let root = tempdir().unwrap();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/build_oracle_fixture.py");
    assert!(
        Command::new(oracle_python())
            .arg(script)
            .arg(root.path())
            .status()
            .unwrap()
            .success()
    );
    let input_path = root.path().join("input.json");
    let mut input: Value = serde_json::from_slice(&fs::read(&input_path).unwrap()).unwrap();
    input["panelBundleReceipts"] = json!([]);
    input.as_object_mut().unwrap().remove("inputSha256");
    self_hash(&mut input, "inputSha256");
    fs::write(&input_path, canonical_json_line(&input).unwrap()).unwrap();
    let mut manifest = json!({"schemaVersion":MANIFEST_SCHEMA,"contractVersion":CONTRACT_VERSION,"operation":OPERATION,"inputPath":input_path.to_string_lossy(),"inputSha256":input["inputSha256"],"resultPath":TRANSACTION_PATH});
    self_hash(&mut manifest, "manifestSha256");
    let manifest_path = root.path().join("prefinalizer-manifest.json");
    fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();
    let result = execute_manifest(&manifest_path).unwrap();
    assert_eq!(
        result["transaction"]["status"],
        "awaiting_current_panel_bundle_receipts"
    );
    assert!(
        result["transaction"]["taskPlan"]["tasks"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn prior_panel_backfill_emits_relocatable_sealed_cohort_selection() {
    let root = tempdir().unwrap();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/build_oracle_fixture.py");
    assert!(
        Command::new(oracle_python())
            .arg(script)
            .arg(root.path())
            .status()
            .unwrap()
            .success()
    );
    let input_path = root.path().join("input.json");
    let mut input: Value = serde_json::from_slice(&fs::read(&input_path).unwrap()).unwrap();
    let receipt = &mut input["panelBundleReceipts"][0];
    let bundles = receipt["candidatePanelBundles"].as_array_mut().unwrap();
    bundles.retain(|bundle| bundle["panelId"] != "panel-1");
    receipt.as_object_mut().unwrap().remove("receiptSha256");
    self_hash(receipt, "receiptSha256");
    input.as_object_mut().unwrap().remove("inputSha256");
    self_hash(&mut input, "inputSha256");
    fs::write(&input_path, canonical_json_line(&input).unwrap()).unwrap();
    let mut manifest = json!({"schemaVersion":MANIFEST_SCHEMA,"contractVersion":CONTRACT_VERSION,"operation":OPERATION,"inputPath":input_path.to_string_lossy(),"inputSha256":input["inputSha256"],"resultPath":TRANSACTION_PATH});
    self_hash(&mut manifest, "manifestSha256");
    let manifest_path = root.path().join("prefinalizer-manifest.json");
    fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();
    let transaction = execute_manifest(&manifest_path).unwrap()["transaction"].clone();
    assert_eq!(transaction["status"], "awaiting_panel_bundle_receipts");
    let task = &transaction["taskPlan"]["tasks"][0];
    assert_eq!(task["role"], "prior_panel_backfill");
    let selection: Value = serde_json::from_slice(
        &fs::read(
            root.path()
                .join(task["cohortSelection"]["path"].as_str().unwrap()),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        selection["schemaVersion"],
        "temporal_qd_rotating_cohort_selection_v1"
    );
    assert_eq!(
        selection["candidateProjection"]["relativePath"],
        "selected-backfill-candidates.jsonl"
    );
    assert_eq!(
        selection["candidateProjection"]["rowSchema"],
        "temporal_qd_rotating_candidate_projection_row_v1"
    );
    let row: Value = serde_json::from_str(
        &fs::read_to_string(root.path().join("selected-backfill-candidates.jsonl"))
            .unwrap()
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    assert!(
        row["projectionRowSha256"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
}

#[test]
fn legacy_or_generic_panel_receipt_is_a_hard_v5_failure() {
    let root = tempdir().unwrap();
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/build_oracle_fixture.py");
    assert!(
        Command::new(oracle_python())
            .arg(script)
            .arg(root.path())
            .status()
            .unwrap()
            .success()
    );
    let input_path = root.path().join("input.json");
    let mut input: Value = serde_json::from_slice(&fs::read(&input_path).unwrap()).unwrap();
    let receipt = &mut input["panelBundleReceipts"][0];
    receipt["schemaVersion"] = json!("temporal_qd_rotating_panel_bundle_receipt_v1");
    receipt.as_object_mut().unwrap().remove("receiptSha256");
    self_hash(receipt, "receiptSha256");
    input.as_object_mut().unwrap().remove("inputSha256");
    self_hash(&mut input, "inputSha256");
    fs::write(&input_path, canonical_json_line(&input).unwrap()).unwrap();
    let mut manifest = json!({"schemaVersion":MANIFEST_SCHEMA,"contractVersion":CONTRACT_VERSION,"operation":OPERATION,"inputPath":input_path.to_string_lossy(),"inputSha256":input["inputSha256"],"resultPath":TRANSACTION_PATH});
    self_hash(&mut manifest, "manifestSha256");
    let manifest_path = root.path().join("prefinalizer-manifest.json");
    fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();
    let error = execute_manifest(&manifest_path).unwrap_err().to_string();
    assert!(
        error.contains("v3/generic panel bundle receipts are forbidden"),
        "{error}"
    );
}
