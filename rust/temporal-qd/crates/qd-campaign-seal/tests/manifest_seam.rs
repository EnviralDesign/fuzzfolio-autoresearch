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
    let mut tail_authority = json!({
        "schemaVersion": "temporal_qd_v5_directional_tail_authority_v1",
        "generationIndex": 1,
        "runtimeAuthoritySha256": HASH,
        "tailResultIndexSchema": "temporal_qd_tail_result_index_v4",
        "tailResultEntrySchema": "temporal_qd_tail_result_index_entry_v4",
        "rawRotatingProvenanceSchema": "temporal_qd_v5_raw_rotating_provenance_v1",
    });
    let tail_authority_sha = canonical_sha256(&tail_authority).unwrap();
    tail_authority["tailAuthoritySha256"] = Value::String(tail_authority_sha);
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
        "directionalTailAuthority": tail_authority,
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

#[test]
fn directional_v5_authority_is_manifest_bound_and_tamper_fails_before_source_open() {
    let root = tempdir().unwrap();
    let tail_authority_body = json!({
        "schemaVersion": "temporal_qd_v5_directional_tail_authority_v1",
        "generationIndex": 1,
        "runtimeAuthoritySha256": HASH,
        "tailResultIndexSchema": "temporal_qd_tail_result_index_v4",
        "tailResultEntrySchema": "temporal_qd_tail_result_index_entry_v4",
        "rawRotatingProvenanceSchema": "temporal_qd_v5_raw_rotating_provenance_v1",
    });
    let mut tail_authority = tail_authority_body;
    let tail_authority_sha = canonical_sha256(&tail_authority).unwrap();
    tail_authority.as_object_mut().unwrap().insert(
        "tailAuthoritySha256".into(),
        Value::String(tail_authority_sha),
    );
    let mut manifest = json!({
        "schemaVersion": MANIFEST_SCHEMA, "contractVersion": CONTRACT_VERSION,
        "operation": OPERATION, "runtimeAuthoritySha256": HASH,
        "sourcePath": root.path().join("absent-source.json").to_string_lossy(),
        "sourceSha256": HASH,
        "evaluationPopulationPath": root.path().join("absent-evaluation.json").to_string_lossy(),
        "evaluationPopulationSha256": HASH, "generationIndex": 1,
        "minimumTotalTrades": 8, "minimumTradesPerWindow": 4, "capTrades": 20,
        "provisionalLimit": 128, "directionalTailAuthority": tail_authority,
        "resultPath": TRANSACTION_PATH,
    });
    let manifest_sha = canonical_sha256(&manifest).unwrap();
    manifest
        .as_object_mut()
        .unwrap()
        .insert("manifestSha256".into(), Value::String(manifest_sha));
    let manifest_path = root.path().join("manifest-v4.json");
    fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_temporal-qd-campaign-seal"))
        .arg("--manifest")
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("campaign seal source"), "{stderr}");

    let mut tampered = manifest;
    tampered["directionalTailAuthority"]["tailResultEntrySchema"] =
        Value::String("temporal_qd_tail_result_index_entry_v3".to_owned());
    tampered.as_object_mut().unwrap().remove("manifestSha256");
    let tampered_sha = canonical_sha256(&tampered).unwrap();
    tampered
        .as_object_mut()
        .unwrap()
        .insert("manifestSha256".into(), Value::String(tampered_sha));
    fs::write(&manifest_path, canonical_json_line(&tampered).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_temporal-qd-campaign-seal"))
        .arg("--manifest")
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("directional tail authority contract")
    );
}
