use std::{fs, path::Path, process::Command};

use serde_json::Value;
use tempfile::tempdir;
use temporal_qd_contract::{canonical_json_line, canonical_sha256};
use temporal_qd_rotating_prefinalizer::core_receipt::{
    extract_g0_selected_attempts_to_path, validate_g0_selected_attempt,
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
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/build_g0_bridge_fixture.py");
    let output = Command::new(python).arg(script).arg(root).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn g0_real_bridge_chain_extracts_selected_local_attempts_and_is_receipt_last() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let chain = root.path().join("chain.json");
    let source = root.path().join("g0-selected-proposal-attempts.jsonl");
    let receipt = root.path().join("receipt.json");
    let adapter: Value =
        serde_json::from_slice(&fs::read(root.path().join("adapter.json")).unwrap()).unwrap();
    assert_eq!(adapter["selectedEvaluationCandidateCount"], 2);
    let first_receipt = extract_g0_selected_attempts_to_path(&chain, &source, &receipt).unwrap();
    let sealed: Vec<Value> = fs::read_to_string(&source)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(sealed.len(), 2);
    assert_eq!(
        first_receipt["proposalAccounting"]["dispositionCounts"]["accepted"],
        2
    );
    assert_eq!(
        first_receipt["proposalAccounting"]["originProposalCounts"]["random_immigrant"],
        2
    );
    assert_eq!(
        sealed[0]["schemaVersion"],
        "temporal_qd_v5_g0_selected_proposal_attempt_v1"
    );
    assert_eq!(sealed[0]["proposalOrdinal"], 0);
    assert_ne!(
        sealed[0]["constructionAttempt"]["proposalOrdinal"],
        sealed[1]["constructionAttempt"]["proposalOrdinal"]
    );
    assert_eq!(
        first_receipt["schemaVersion"],
        "temporal_qd_v5_g0_selected_attempt_stream_receipt_v1"
    );

    assert_eq!(
        extract_g0_selected_attempts_to_path(&chain, &source, &receipt).unwrap(),
        first_receipt
    );
}

#[test]
fn g0_rejects_source_without_receipt_and_tampered_chain_descriptor() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let chain = root.path().join("chain.json");
    let source = root.path().join("source.json");
    fs::write(&source, b"{}\n").unwrap();
    assert!(
        extract_g0_selected_attempts_to_path(&chain, &source, &root.path().join("receipt.json"))
            .is_err()
    );
    fs::remove_file(&source).unwrap();
    let mut input: Value = serde_json::from_slice(&fs::read(&chain).unwrap()).unwrap();
    input["adapter"]["rawSha256"] = serde_json::json!(
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    input.as_object_mut().unwrap().remove("inputSha256");
    input["inputSha256"] = serde_json::json!(canonical_sha256(&input).unwrap());
    fs::write(&chain, canonical_json_line(&input).unwrap()).unwrap();
    assert!(
        extract_g0_selected_attempts_to_path(
            &chain,
            &root.path().join("clean.json"),
            &root.path().join("clean-receipt.json")
        )
        .is_err()
    );
}

#[test]
fn g0_selected_wrapper_rejects_construction_identity_and_proof_replay() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let stream = root.path().join("g0-selected-proposal-attempts.jsonl");
    extract_g0_selected_attempts_to_path(
        &root.path().join("chain.json"),
        &stream,
        &root.path().join("receipt.json"),
    )
    .unwrap();
    let row: Value =
        serde_json::from_str(fs::read_to_string(&stream).unwrap().lines().next().unwrap()).unwrap();
    validate_g0_selected_attempt(&row, 0).unwrap();

    let mut construction_substitution = row.clone();
    construction_substitution["constructionAttempt"]["entrySha256"] = serde_json::json!(
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert!(validate_g0_selected_attempt(&construction_substitution, 0).is_err());

    let mut replayed_proof = row;
    replayed_proof["g0BootstrapProof"]["selectedProjectionSha256"] = serde_json::json!(
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    );
    assert!(validate_g0_selected_attempt(&replayed_proof, 0).is_err());
}

#[test]
fn g0_orphan_stream_divergence_is_not_adopted() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let stream = root.path().join("g0-selected-proposal-attempts.jsonl");
    let receipt = root.path().join("receipt.json");
    extract_g0_selected_attempts_to_path(&root.path().join("chain.json"), &stream, &receipt)
        .unwrap();
    fs::remove_file(&receipt).unwrap();
    let mut bytes = fs::read(&stream).unwrap();
    bytes.push(b' ');
    fs::write(&stream, bytes).unwrap();
    assert!(
        extract_g0_selected_attempts_to_path(&root.path().join("chain.json"), &stream, &receipt)
            .is_err()
    );
}

fn fresh_extract_rejects(root: &Path) {
    assert!(
        extract_g0_selected_attempts_to_path(
            &root.join("chain.json"),
            &root.join("g0-selected-proposal-attempts.jsonl"),
            &root.join("receipt.json")
        )
        .is_err()
    );
}

#[test]
fn g0_selected_projection_reorder_duplicate_missing_and_projection_mismatch_reject() {
    for mutation in ["reorder", "duplicate", "missing", "projection"] {
        let root = tempdir().unwrap();
        fixture(root.path());
        let path = root
            .path()
            .join("output/v5-native/selected-projections.jsonl");
        let mut lines = fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        assert_eq!(lines.len(), 2, "fixture must contain two real selections");
        match mutation {
            "reorder" => lines.reverse(),
            "duplicate" => lines[1] = lines[0].clone(),
            "missing" => {
                lines.pop();
            }
            "projection" => {
                let mut row: Value = serde_json::from_str(&lines[0]).unwrap();
                row["recordSha256"] = serde_json::json!(
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                );
                lines[0] = String::from_utf8(canonical_json_line(&row).unwrap()).unwrap();
                lines[0].pop();
            }
            _ => unreachable!(),
        }
        fs::write(&path, format!("{}\n", lines.join("\n"))).unwrap();
        fresh_extract_rejects(root.path());
    }
}

#[test]
fn g0_evaluation_fragment_mismatch_rejects() {
    let root = tempdir().unwrap();
    fixture(root.path());
    let adapter: Value =
        serde_json::from_slice(&fs::read(root.path().join("adapter.json")).unwrap()).unwrap();
    let fragment = adapter["g0FunnelFragments"]["absolutePath"]
        .as_str()
        .unwrap();
    let mut bytes = fs::read(fragment).unwrap();
    bytes.push(b' ');
    fs::write(fragment, bytes).unwrap();
    fresh_extract_rejects(root.path());
}
