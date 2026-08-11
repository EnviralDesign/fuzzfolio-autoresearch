use std::{fs, path::Path, process::Command};

use serde_json::Value;
use tempfile::tempdir;
use temporal_qd_contract::{canonical_json_line, canonical_sha256};
use temporal_qd_rotating_prefinalizer::panel_receipt::build_to_path;

fn fixture(root: &Path) {
    fixture_count(root, 2);
}

fn fixture_count(root: &Path, count: usize) {
    let python = std::env::var_os("PYTHON").unwrap_or_else(|| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../.venv/Scripts/python.exe")
            .into_os_string()
    });
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/build_v5_panel_receipt_oracle_fixture.py");
    let output = Command::new(python)
        .arg(script)
        .arg(root)
        .arg(count.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "fixture builder failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn v4_panel_receipt_streams_128_member_witness() {
    let root = tempdir().unwrap();
    fixture_count(root.path(), 128);
    let receipt = build_to_path(
        &root.path().join("input.json"),
        &root.path().join("receipt.json"),
    )
    .unwrap();
    assert_eq!(
        receipt["candidatePanelBundles"].as_array().unwrap().len(),
        128
    );
}

#[test]
fn v4_panel_receipt_matches_python_oracle_and_restart_is_input_free() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let receipt = build_to_path(
        &root.path().join("input.json"),
        &root.path().join("receipt.json"),
    )
    .unwrap();
    let expected: Value =
        serde_json::from_slice(&fs::read(root.path().join("expected.json")).unwrap()).unwrap();
    assert_eq!(receipt, expected);
    fs::remove_file(root.path().join("evaluated-members.jsonl")).unwrap();
    assert_eq!(
        build_to_path(
            &root.path().join("input.json"),
            &root.path().join("receipt.json")
        )
        .unwrap(),
        expected
    );
}

#[test]
fn v4_panel_receipt_rejects_tampered_sealed_index() {
    let root = tempdir().unwrap();
    fixture(root.path());
    fs::write(root.path().join("tail-result-index-v4.json"), b"{}\n").unwrap();
    let error = build_to_path(
        &root.path().join("input.json"),
        &root.path().join("receipt.json"),
    )
    .unwrap_err();
    assert!(format!("{error:#}").contains("v4 tail index"), "{error:#}");
}

#[test]
fn v4_panel_receipt_rejects_rehashed_tail_authority_substitution() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let authority_path = root.path().join("tail-authority.json");
    let mut authority: Value = serde_json::from_slice(&fs::read(&authority_path).unwrap()).unwrap();
    authority["tailResultIndexSha256"] = Value::String(
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
    );
    authority
        .as_object_mut()
        .unwrap()
        .remove("tailAuthoritySha256");
    authority["tailAuthoritySha256"] = Value::String(canonical_sha256(&authority).unwrap());
    fs::write(&authority_path, canonical_json_line(&authority).unwrap()).unwrap();
    let mut input: Value =
        serde_json::from_slice(&fs::read(root.path().join("input.json")).unwrap()).unwrap();
    input["tailAuthority"]["receiptSha256"] = authority["tailAuthoritySha256"].clone();
    input.as_object_mut().unwrap().remove("inputSha256");
    input["inputSha256"] = Value::String(canonical_sha256(&input).unwrap());
    fs::write(
        root.path().join("input.json"),
        canonical_json_line(&input).unwrap(),
    )
    .unwrap();
    let error = build_to_path(
        &root.path().join("input.json"),
        &root.path().join("receipt.json"),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("tailResultIndexSha256 binding"),
        "{error:#}"
    );
}

#[test]
fn v4_panel_receipt_rejects_authority_traversal() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let mut input: Value =
        serde_json::from_slice(&fs::read(root.path().join("input.json")).unwrap()).unwrap();
    input["tailAuthority"]["receiptPath"] = Value::String(
        root.path()
            .join("child")
            .join("..")
            .join("tail-authority.json")
            .to_string_lossy()
            .into_owned(),
    );
    input.as_object_mut().unwrap().remove("inputSha256");
    input["inputSha256"] = Value::String(canonical_sha256(&input).unwrap());
    fs::write(
        root.path().join("input.json"),
        canonical_json_line(&input).unwrap(),
    )
    .unwrap();
    let error = build_to_path(
        &root.path().join("input.json"),
        &root.path().join("receipt.json"),
    )
    .unwrap_err();
    assert!(
        format!("{error:#}").contains("path is invalid"),
        "{error:#}"
    );
}

#[cfg(windows)]
#[test]
fn v4_panel_receipt_rejects_authority_symlink() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let linked = root.path().join("linked-tail-authority.json");
    if std::os::windows::fs::symlink_file(root.path().join("tail-authority.json"), &linked).is_err()
    {
        return;
    }
    let mut input: Value =
        serde_json::from_slice(&fs::read(root.path().join("input.json")).unwrap()).unwrap();
    input["tailAuthority"]["receiptPath"] = Value::String(linked.to_string_lossy().into_owned());
    input.as_object_mut().unwrap().remove("inputSha256");
    input["inputSha256"] = Value::String(canonical_sha256(&input).unwrap());
    fs::write(
        root.path().join("input.json"),
        canonical_json_line(&input).unwrap(),
    )
    .unwrap();
    assert!(
        build_to_path(
            &root.path().join("input.json"),
            &root.path().join("receipt.json")
        )
        .is_err()
    );
}
