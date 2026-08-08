use std::fs;
use std::process::Command;

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::Value;
use tempfile::tempdir;
use temporal_qd_tail_reducer::execute_manifest;

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(4)
        .expect("crate is nested under repo/rust/temporal-qd/crates")
        .to_path_buf()
}

fn python() -> std::path::PathBuf {
    let root = repo_root();
    let local = if cfg!(windows) {
        root.join(".venv/Scripts/python.exe")
    } else {
        root.join(".venv/bin/python")
    };
    if local.is_file() {
        local
    } else {
        std::path::PathBuf::from(if cfg!(windows) { "python" } else { "python3" })
    }
}

fn fixture() -> Result<(tempfile::TempDir, Value)> {
    let directory = tempdir()?;
    let status = Command::new(python())
        .arg(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/python_oracle_fixture.py"),
        )
        .arg(directory.path())
        .current_dir(repo_root())
        .status()
        .context("run production Python oracle fixture generator")?;
    ensure!(status.success(), "Python oracle fixture generation failed");
    let result = execute_manifest(&directory.path().join("manifest.json"))?;
    Ok((directory, result))
}

fn first_difference(left: &Value, right: &Value, path: &str) -> Option<String> {
    match (left, right) {
        (Value::Object(left), Value::Object(right)) => {
            for key in left.keys().chain(right.keys()) {
                if left.get(key) != right.get(key) {
                    return match (left.get(key), right.get(key)) {
                        (Some(l), Some(r)) => first_difference(l, r, &format!("{path}.{key}"))
                            .or_else(|| Some(format!("{path}.{key}: {l:?} != {r:?}"))),
                        pair => Some(format!("{path}.{key}: {pair:?}")),
                    };
                }
            }
            None
        }
        (Value::Array(left), Value::Array(right)) => {
            if left.len() != right.len() {
                return Some(format!("{path}: lengths {} != {}", left.len(), right.len()));
            }
            left.iter()
                .zip(right)
                .enumerate()
                .find_map(|(index, (l, r))| {
                    (l != r).then(|| {
                        first_difference(l, r, &format!("{path}[{index}]"))
                            .unwrap_or_else(|| format!("{path}[{index}]: {l:?} != {r:?}"))
                    })
                })
        }
        _ if left != right => Some(format!("{path}: {left:?} != {right:?}")),
        _ => None,
    }
}

#[test]
fn matches_python_oracle_for_members_rejections_and_diverse_subset() -> Result<()> {
    let (directory, result) = fixture()?;
    let expected: Value =
        serde_json::from_slice(&fs::read(directory.path().join("expected.json"))?)?;
    let members = fs::read_to_string(directory.path().join("evaluated-members.jsonl"))?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<Vec<Value>, _>>()?;
    let actual_members = Value::Array(members);
    assert_eq!(
        first_difference(&actual_members, &expected["members"], "members"),
        None
    );
    assert_eq!(
        result["evaluatedMembers"]["evaluationRejectedCandidates"],
        expected["evaluationRejectedCandidates"]
    );
    assert_eq!(result["resultSetSha256"], expected["resultSetSha256"]);
    assert_eq!(result["provisional"]["candidates"], expected["provisional"]);
    assert_eq!(result["evaluatedMembers"]["memberCount"], 3);
    assert_eq!(result["evaluatedMembers"]["evaluationRejectionCount"], 2);
    Ok(())
}

#[test]
fn restart_reuses_exact_artifact_and_detects_member_tamper() -> Result<()> {
    let (directory, first) = fixture()?;
    let second = execute_manifest(&directory.path().join("manifest.json"))?;
    assert_eq!(first, second);
    let member_path = directory.path().join("evaluated-members.jsonl");
    let mut bytes = fs::read(&member_path)?;
    let position = bytes
        .iter()
        .position(|byte| *byte == b'1')
        .ok_or_else(|| anyhow!("fixture lacks a mutable byte"))?;
    bytes[position] = b'2';
    fs::write(&member_path, bytes)?;
    let error = execute_manifest(&directory.path().join("manifest.json"))
        .expect_err("tamper must fail closed");
    assert!(format!("{error:#}").contains("corrupt, truncated, or replaced"));
    Ok(())
}

#[test]
fn input_identity_tamper_fails_before_reduction() -> Result<()> {
    let (directory, _) = fixture()?;
    fs::remove_file(directory.path().join("tail-reduction-result.json"))?;
    fs::remove_file(directory.path().join("evaluated-members.jsonl"))?;
    let index_path = directory.path().join("tail-result-index-v3.json");
    let mut index: Value = serde_json::from_slice(&fs::read(&index_path)?)?;
    index["entries"][0]["task"]["candidateId"] = Value::String("qd_tampered".to_owned());
    fs::write(&index_path, serde_json::to_vec(&index)?)?;
    let error = execute_manifest(&directory.path().join("manifest.json"))
        .expect_err("tamper must fail closed");
    assert!(format!("{error:#}").contains("tail result index identity mismatch"));
    Ok(())
}

#[test]
fn deterministic_inputs_reproduce_member_and_selection_identities() -> Result<()> {
    let (left, left_result) = fixture()?;
    let (right, right_result) = fixture()?;
    assert_eq!(
        fs::read(left.path().join("evaluation-population.json"))?,
        fs::read(right.path().join("evaluation-population.json"))?
    );
    assert_eq!(
        fs::read(left.path().join("tail-result-index-v3.json"))?,
        fs::read(right.path().join("tail-result-index-v3.json"))?
    );
    assert_eq!(
        fs::read(left.path().join("evaluated-members.jsonl"))?,
        fs::read(right.path().join("evaluated-members.jsonl"))?
    );
    assert_eq!(
        left_result["resultSetSha256"],
        right_result["resultSetSha256"]
    );
    assert_eq!(left_result["provisional"], right_result["provisional"]);
    Ok(())
}

#[test]
fn truncated_result_fails_closed_on_restart() -> Result<()> {
    let (directory, _) = fixture()?;
    let result_path = directory.path().join("tail-reduction-result.json");
    let bytes = fs::read(&result_path)?;
    fs::write(&result_path, &bytes[..bytes.len() / 2])?;
    let error = execute_manifest(&directory.path().join("manifest.json"))
        .expect_err("truncated result must fail closed");
    assert!(format!("{error:#}").contains("parse existing tail reduction result"));
    Ok(())
}
