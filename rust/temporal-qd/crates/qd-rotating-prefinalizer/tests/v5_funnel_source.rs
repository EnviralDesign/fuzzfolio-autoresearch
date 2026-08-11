use std::{fs, path::Path, process::Command};

use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use temporal_qd_contract::{canonical_json_line, canonical_sha256};
use temporal_qd_rotating_prefinalizer::{
    core_receipt::extract_to_path, funnel_source::assemble_to_path,
};

fn fixture(root: &Path) {
    let python = std::env::var_os("PYTHON").unwrap_or_else(|| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../.venv/Scripts/python.exe")
            .into_os_string()
    });
    let script =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/build_v5_funnel_oracle_fixture.py");
    let output = Command::new(python).arg(script).arg(root).output().unwrap();
    assert!(
        output.status.success(),
        "fixture builder failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn reseal(value: &mut Value, field: &str) {
    value.as_object_mut().unwrap().remove(field);
    value[field] = Value::String(canonical_sha256(value).unwrap());
}

#[test]
fn v5_128_attempt_cli_receipt_and_stdout_are_bounded_and_receipt_last() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let input_path = root.path().join("input.json");
    let attempts_path = root.path().join("attempts.jsonl");
    let mut rows = fs::read(&attempts_path)
        .unwrap()
        .split_inclusive(|byte| *byte == b'\n')
        .map(|line| serde_json::from_slice::<Value>(&line[..line.len() - 1]).unwrap())
        .collect::<Vec<_>>();
    let candidate_free = rows[0].clone();
    for ordinal in 3..128u64 {
        let mut row = candidate_free.clone();
        row["proposalOrdinal"] = serde_json::json!(ordinal);
        row["entrySha256"] = Value::String(
            canonical_sha256(&serde_json::json!({"attemptOrdinal":ordinal})).unwrap(),
        );
        rows.push(row);
    }
    rows.sort_by_key(|row| row["proposalOrdinal"].as_u64().unwrap());
    let attempt_bytes = rows
        .iter()
        .flat_map(|row| canonical_json_line(row).unwrap())
        .collect::<Vec<_>>();
    fs::write(&attempts_path, &attempt_bytes).unwrap();

    let mut input: Value = serde_json::from_slice(&fs::read(&input_path).unwrap()).unwrap();
    let receipt_path = std::path::PathBuf::from(
        input["proposalAttemptAuthority"]["receiptPath"]
            .as_str()
            .unwrap(),
    );
    let mut attempt_receipt: Value =
        serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    attempt_receipt["attemptStream"]["rawSha256"] =
        Value::String(format!("sha256:{:x}", Sha256::digest(&attempt_bytes)));
    attempt_receipt["attemptStream"]["sizeBytes"] = serde_json::json!(attempt_bytes.len());
    attempt_receipt["attemptStream"]["recordCount"] = serde_json::json!(128);
    attempt_receipt["proposalAccounting"] = serde_json::json!({
        "proposalAttemptCount":128,
        "dispositionCounts":{"accepted":1,"rejected":127},
        "originProposalCounts":{"random_immigrant":127,"structural_offspring":1},
    });
    reseal(&mut attempt_receipt, "receiptSha256");
    let attempt_receipt_bytes = canonical_json_line(&attempt_receipt).unwrap();
    fs::write(&receipt_path, &attempt_receipt_bytes).unwrap();
    input["proposalAttemptAuthority"]["receiptFileSha256"] = Value::String(format!(
        "sha256:{:x}",
        Sha256::digest(&attempt_receipt_bytes)
    ));
    input["proposalAttemptAuthority"]["receiptSizeBytes"] =
        serde_json::json!(attempt_receipt_bytes.len());
    input["proposalAttemptAuthority"]["receiptSha256"] = attempt_receipt["receiptSha256"].clone();
    reseal(&mut input, "inputSha256");
    fs::write(&input_path, canonical_json_line(&input).unwrap()).unwrap();

    let source_path = root.path().join("source.json");
    let first = Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .arg("assemble-funnel")
        .arg(&input_path)
        .arg(&source_path)
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(first.stdout.len() < 8 * 1024);
    assert!(fs::metadata(&source_path).unwrap().len() > first.stdout.len() as u64);
    let execution: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(
        execution["schemaVersion"],
        "temporal_qd_v5_native_funnel_assembly_execution_v2"
    );
    assert_eq!(execution["restart"], false);
    assert!(execution.get("proposalAttempts").is_none());
    let receipt_file = root.path().join("funnel-assembly-receipt.json");
    assert!(receipt_file.is_file());
    assert!(fs::metadata(&receipt_file).unwrap().len() < 8 * 1024);

    fs::remove_file(&attempts_path).unwrap();
    fs::remove_file(root.path().join("tail-index.json")).unwrap();
    let restart = Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .arg("assemble-funnel")
        .arg(&input_path)
        .arg(&source_path)
        .output()
        .unwrap();
    assert!(restart.status.success());
    let restarted: Value = serde_json::from_slice(&restart.stdout).unwrap();
    assert_eq!(restarted["restart"], true);
    assert_eq!(restarted["receipt"], execution["receipt"]);

    let source_bytes = fs::read(&source_path).unwrap();
    fs::write(&source_path, b"{}\n").unwrap();
    let tampered = Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .arg("assemble-funnel")
        .arg(&input_path)
        .arg(&source_path)
        .output()
        .unwrap();
    assert!(!tampered.status.success());
    fs::write(source_path, source_bytes).unwrap();
}

#[test]
fn v5_full_attempt_stream_and_v4_tail_match_python_oracle_and_restart() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let source = assemble_to_path(
        &root.path().join("input.json"),
        &root.path().join("source.json"),
    )
    .unwrap();
    let expected: Value =
        serde_json::from_slice(&fs::read(root.path().join("expected.json")).unwrap()).unwrap();
    assert_eq!(source, expected);
    fs::remove_file(root.path().join("attempts.jsonl")).unwrap();
    assert!(
        assemble_to_path(
            &root.path().join("input.json"),
            &root.path().join("source.json")
        )
        .is_err()
    );
}

#[test]
fn v5_funnel_rejects_tail_or_attempt_tampering() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let index = root.path().join("tail-index.json");
    fs::write(&index, b"{}\n").unwrap();
    let error = assemble_to_path(
        &root.path().join("input.json"),
        &root.path().join("source.json"),
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("v4 tail index"), "{error:#}");
}

#[test]
fn v2_same_attempt_authority_rejects_changed_input_on_source_restart() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let input_path = root.path().join("input.json");
    let source_path = root.path().join("source.json");
    assemble_to_path(&input_path, &source_path).unwrap();
    let mut input: Value = serde_json::from_slice(&fs::read(&input_path).unwrap()).unwrap();
    input["minimumTotalTrades"] = serde_json::json!(9);
    input.as_object_mut().unwrap().remove("inputSha256");
    input["inputSha256"] = serde_json::json!(canonical_sha256(&input).unwrap());
    fs::write(&input_path, canonical_json_line(&input).unwrap()).unwrap();
    assert!(assemble_to_path(&input_path, &source_path).is_err());
}

#[test]
fn v1_direct_stream_and_accounting_input_is_rejected() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let input_path = root.path().join("input.json");
    let mut input: Value = serde_json::from_slice(&fs::read(&input_path).unwrap()).unwrap();
    input["schemaVersion"] = serde_json::json!("temporal_qd_v5_native_funnel_reduction_input_v1");
    input["proposalAttemptStream"] = serde_json::json!({});
    input["proposalAccounting"] = serde_json::json!({});
    input
        .as_object_mut()
        .unwrap()
        .remove("proposalAttemptAuthority");
    input.as_object_mut().unwrap().remove("inputSha256");
    input["inputSha256"] = serde_json::json!(canonical_sha256(&input).unwrap());
    fs::write(&input_path, canonical_json_line(&input).unwrap()).unwrap();
    assert!(assemble_to_path(&input_path, &root.path().join("source.json")).is_err());
}

#[test]
fn core_v2_receipt_extracts_the_complete_attempt_stream_for_the_reducer() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let attempts = extract_to_path(
        &root.path().join("core-adapter-input.json"),
        &root.path().join("core-attempts.jsonl"),
    )
    .unwrap();
    assert_eq!(attempts["recordCount"], 3);
    assert_eq!(attempts["recordCount"], 3);
}

#[test]
fn core_v2_receipt_tampering_is_a_hard_failure() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let path = root.path().join("core-adapter-input.json");
    let mut input: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    input["coreFragments"]["fragmentBundleSha256"] = Value::String(
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
    );
    input.as_object_mut().unwrap().remove("inputSha256");
    input["inputSha256"] = Value::String(canonical_sha256(&input).unwrap());
    fs::write(&path, canonical_json_line(&input).unwrap()).unwrap();
    let error = extract_to_path(&path, &root.path().join("core-attempts.jsonl")).unwrap_err();
    assert!(
        format!("{error:#}").contains("fragment receipt self hash"),
        "{error:#}"
    );
}
