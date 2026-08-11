use std::fs;
use std::process::Command;

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Value, json};
use tempfile::tempdir;
use temporal_qd_contract::{
    canonical_json_bytes, canonical_sha256, canonical_sha256_without_object_field,
};
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

fn fixture_v4() -> Result<(tempfile::TempDir, Value)> {
    let directory = tempdir()?;
    let status = Command::new(python())
        .arg(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/python_oracle_fixture.py"),
        )
        .arg(directory.path())
        .current_dir(repo_root())
        .status()
        .context("run production Python directional-tail fixture generator")?;
    ensure!(
        status.success(),
        "Python directional-tail fixture generation failed"
    );
    let manifest_path = directory.path().join("manifest-v4.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["runtimeAuthoritySha256"] =
        json!("sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
    rehash(&mut manifest, "manifestSha256")?;
    write_canonical(&manifest_path, &manifest, true)?;
    let result = execute_manifest(&manifest_path)?;
    Ok((directory, result))
}

fn write_canonical(path: &std::path::Path, value: &Value, trailing_lf: bool) -> Result<()> {
    let mut bytes = canonical_json_bytes(value)?;
    if trailing_lf {
        bytes.push(b'\n');
    }
    fs::write(path, bytes)?;
    Ok(())
}

fn rehash(value: &mut Value, field: &str) -> Result<String> {
    let sha = canonical_sha256_without_object_field(value, field)?;
    value[field] = Value::String(sha.clone());
    Ok(sha)
}

fn cohort_fixture(
    role: &str,
    panel: &str,
    candidate_ids: &[&str],
) -> Result<(tempfile::TempDir, std::path::PathBuf, Value)> {
    let (directory, _) = fixture()?;
    fs::remove_file(directory.path().join("tail-reduction-result.json"))?;
    fs::remove_file(directory.path().join("evaluated-members.jsonl"))?;

    let evaluation_path = directory.path().join("evaluation-population.json");
    let evaluation: Value = serde_json::from_slice(&fs::read(&evaluation_path)?)?;
    let candidates = evaluation["candidates"]
        .as_array()
        .context("fixture evaluation candidates missing")?
        .iter()
        .filter(|candidate| {
            candidate
                .get("candidateId")
                .and_then(Value::as_str)
                .is_some_and(|id| candidate_ids.contains(&id))
        })
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        candidates.len() == candidate_ids.len(),
        "requested cohort fixture candidate is unavailable"
    );
    let mut cohort = json!({
        "schemaVersion": "temporal_qd_rotating_cohort_population_v1",
        "generationIndex": evaluation["generationIndex"],
        "panelId": panel,
        "cohortRole": role,
        "rotatingEvidenceSha256": canonical_sha256(&json!("fixture-rotating"))?,
        "candidateCount": candidates.len(),
        "candidates": candidates,
        "proposalPopulation": role == "proposal_current_panel",
    });
    let cohort_sha = rehash(&mut cohort, "populationSha256")?;
    let cohort_path = directory.path().join("cohort-population.json");
    write_canonical(&cohort_path, &cohort, true)?;

    let index_path = directory.path().join("tail-result-index-v3.json");
    let mut index: Value = serde_json::from_slice(&fs::read(&index_path)?)?;
    index["entries"]
        .as_array_mut()
        .context("fixture index entries missing")?
        .retain(|entry| {
            entry["task"]["candidateId"]
                .as_str()
                .is_some_and(|id| candidate_ids.contains(&id))
        });
    let entry_count = index["entries"]
        .as_array()
        .context("fixture index entries missing")?
        .len();
    index["taskCount"] = json!(entry_count);
    index["sourceResultBlobBytes"] = json!(entry_count);
    let index_sha = rehash(&mut index, "tailResultIndexSha256")?;
    write_canonical(&index_path, &index, false)?;

    let manifest_path = directory.path().join("manifest.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["evaluationPopulationPath"] = json!(cohort_path);
    manifest["evaluationPopulationSha256"] = json!(cohort_sha);
    manifest["tailResultIndexSha256"] = json!(index_sha);
    rehash(&mut manifest, "manifestSha256")?;
    write_canonical(&manifest_path, &manifest, true)?;
    Ok((directory, manifest_path, cohort))
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
fn directional_v4_index_restarts_without_raw_results_and_rejects_provenance_tamper() -> Result<()> {
    let (directory, first) = fixture_v4()?;
    // The reducer is deliberately raw-blind. Its restart proof uses only the
    // self-hashed v4 compact index and refuses a provenance rebinding.
    let second = execute_manifest(&directory.path().join("manifest-v4.json"))?;
    assert_eq!(first, second);
    fs::remove_file(directory.path().join("tail-reduction-result.json"))?;
    fs::remove_file(directory.path().join("evaluated-members.jsonl"))?;
    fs::remove_file(directory.path().join("tail-authority.json"))?;
    let index_path = directory.path().join("tail-result-index-v4.json");
    let mut index: Value = serde_json::from_slice(&fs::read(&index_path)?)?;
    let entry = index["entries"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|entry| entry.get("rawRotatingProvenance").is_some())
        .unwrap();
    entry["rawRotatingProvenance"]["taskId"] = Value::String("task-rebound".to_owned());
    let entry_sha = canonical_sha256_without_object_field(entry, "entrySha256")?;
    entry["entrySha256"] = Value::String(entry_sha);
    let digest = canonical_sha256_without_object_field(&index, "tailResultIndexSha256")?;
    index["tailResultIndexSha256"] = Value::String(digest);
    fs::write(&index_path, canonical_json_bytes(&index)?)?;
    let manifest_path = directory.path().join("manifest-v4.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["tailResultIndexSha256"] = index["tailResultIndexSha256"].clone();
    let manifest_sha = canonical_sha256_without_object_field(&manifest, "manifestSha256")?;
    manifest["manifestSha256"] = Value::String(manifest_sha);
    let mut manifest_bytes = canonical_json_bytes(&manifest)?;
    manifest_bytes.push(b'\n');
    fs::write(&manifest_path, manifest_bytes)?;
    let error =
        execute_manifest(&manifest_path).expect_err("v4 raw provenance tamper must fail closed");
    assert!(
        format!("{error:#}").contains("raw rotating provenance task/result binding drifted"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn runtime_bound_tail_emits_receipt_last_authority() -> Result<()> {
    let (directory, result) = fixture_v4()?;
    let authority_path = directory.path().join("tail-authority.json");
    let authority: Value = serde_json::from_slice(&fs::read(&authority_path)?)?;
    assert_eq!(
        authority["schemaVersion"],
        json!("temporal_qd_tail_authority_receipt_v1")
    );
    assert_eq!(authority["generationIndex"], result["generationIndex"]);
    assert_eq!(
        authority["tailReductionManifestSha256"],
        result["manifestSha256"]
    );
    assert_eq!(
        authority["tailReductionResult"]["resultSha256"],
        result["resultSha256"]
    );
    assert_eq!(
        authority["evaluatedMembers"]["rawSha256"],
        result["evaluatedMembers"]["membersFile"]["rawSha256"]
    );
    assert_eq!(
        authority["tailAuthoritySha256"],
        canonical_sha256_without_object_field(&authority, "tailAuthoritySha256")?
    );
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
fn accepts_later_generation_population_without_g0_bootstrap_metadata() -> Result<()> {
    let (directory, _) = fixture()?;
    fs::remove_file(directory.path().join("tail-reduction-result.json"))?;
    fs::remove_file(directory.path().join("evaluated-members.jsonl"))?;

    let evaluation_path = directory.path().join("evaluation-population.json");
    let mut evaluation: Value = serde_json::from_slice(&fs::read(&evaluation_path)?)?;
    evaluation
        .as_object_mut()
        .context("evaluation population is not an object")?
        .remove("g0Bootstrap");
    evaluation["generationIndex"] = json!(2);
    let evaluation_sha =
        canonical_sha256_without_object_field(&evaluation, "evaluationPopulationSha256")?;
    evaluation["evaluationPopulationSha256"] = Value::String(evaluation_sha.clone());
    let mut evaluation_bytes = canonical_json_bytes(&evaluation)?;
    evaluation_bytes.push(b'\n');
    fs::write(&evaluation_path, evaluation_bytes)?;

    let manifest_path = directory.path().join("manifest.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["generationIndex"] = json!(2);
    manifest["evaluationPopulationSha256"] = Value::String(evaluation_sha);
    let manifest_sha = canonical_sha256_without_object_field(&manifest, "manifestSha256")?;
    manifest["manifestSha256"] = Value::String(manifest_sha);
    let mut manifest_bytes = canonical_json_bytes(&manifest)?;
    manifest_bytes.push(b'\n');
    fs::write(&manifest_path, manifest_bytes)?;

    let result = execute_manifest(&manifest_path)?;
    assert_eq!(result["generationIndex"], json!(2));
    assert_eq!(result["evaluatedMembers"]["memberCount"], json!(3));
    Ok(())
}

#[test]
fn rejects_unknown_evaluation_population_fields() -> Result<()> {
    let (directory, _) = fixture()?;
    fs::remove_file(directory.path().join("tail-reduction-result.json"))?;
    fs::remove_file(directory.path().join("evaluated-members.jsonl"))?;

    let evaluation_path = directory.path().join("evaluation-population.json");
    let mut evaluation: Value = serde_json::from_slice(&fs::read(&evaluation_path)?)?;
    evaluation["unexpectedField"] = json!("must-fail-closed");
    let evaluation_sha =
        canonical_sha256_without_object_field(&evaluation, "evaluationPopulationSha256")?;
    evaluation["evaluationPopulationSha256"] = Value::String(evaluation_sha.clone());
    let mut evaluation_bytes = canonical_json_bytes(&evaluation)?;
    evaluation_bytes.push(b'\n');
    fs::write(&evaluation_path, evaluation_bytes)?;

    let manifest_path = directory.path().join("manifest.json");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["evaluationPopulationSha256"] = Value::String(evaluation_sha);
    let manifest_sha = canonical_sha256_without_object_field(&manifest, "manifestSha256")?;
    manifest["manifestSha256"] = Value::String(manifest_sha);
    let mut manifest_bytes = canonical_json_bytes(&manifest)?;
    manifest_bytes.push(b'\n');
    fs::write(&manifest_path, manifest_bytes)?;

    let error = execute_manifest(&manifest_path)
        .expect_err("unknown evaluation population fields must fail closed");
    assert!(format!("{error:#}").contains("evaluation population fields are not exact"));
    Ok(())
}

#[test]
fn rotating_cohort_populations_reduce_all_campaign_roles() -> Result<()> {
    for (role, panel, ids) in [
        (
            "proposal_current_panel",
            "current-panel",
            &[
                "qd_fixture_1",
                "qd_fixture_2",
                "qd_fixture_3",
                "qd_fixture_4",
                "qd_fixture_5",
            ][..],
        ),
        (
            "retained_parent_current_panel",
            "current-panel",
            &["qd_fixture_2", "qd_fixture_3"][..],
        ),
        (
            "prior_panel_backfill",
            "prior-panel",
            &["qd_fixture_4", "qd_fixture_5"][..],
        ),
    ] {
        let (directory, manifest_path, cohort) = cohort_fixture(role, panel, ids)?;
        let result = execute_manifest(&manifest_path)?;
        assert_eq!(
            result["evaluationPopulationSha256"], cohort["populationSha256"],
            "{role} must bind the manifest/result identity to the cohort population"
        );
        assert_eq!(result["populationSha256"], cohort["populationSha256"]);
        let member_ids = fs::read_to_string(directory.path().join("evaluated-members.jsonl"))?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|member| member["candidateId"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert!(
            member_ids.iter().all(|id| ids.contains(&id.as_str())),
            "{role} reduced a candidate absent from its cohort population"
        );
        if role != "proposal_current_panel" {
            assert!(
                !member_ids.iter().any(|id| id == "qd_fixture_1"),
                "non-proposal cohort must not fall back to the original proposal population"
            );
        }
    }
    Ok(())
}

#[test]
fn rotating_cohort_self_rehashed_substitutions_remain_manifest_bound() -> Result<()> {
    for mutation in ["role", "panel", "rotating", "candidate"] {
        let (directory, manifest_path, mut cohort) = cohort_fixture(
            "proposal_current_panel",
            "current-panel",
            &["qd_fixture_1", "qd_fixture_2"],
        )?;
        match mutation {
            "role" => cohort["cohortRole"] = json!("prior_panel_backfill"),
            "panel" => cohort["panelId"] = json!("substituted-panel"),
            "rotating" => {
                cohort["rotatingEvidenceSha256"] = canonical_sha256(&json!("other"))?.into()
            }
            "candidate" => cohort["candidates"][0] = cohort["candidates"][1].clone(),
            _ => unreachable!(),
        }
        rehash(&mut cohort, "populationSha256")?;
        write_canonical(
            &directory.path().join("cohort-population.json"),
            &cohort,
            true,
        )?;
        let error = execute_manifest(&manifest_path).expect_err("substitution must fail closed");
        let message = format!("{error:#}");
        assert!(
            message.contains("manifest binding mismatch")
                || message.contains("proposal role binding mismatch"),
            "{mutation}: {message}"
        );
    }

    let (directory, manifest_path, mut cohort) = cohort_fixture(
        "proposal_current_panel",
        "current-panel",
        &["qd_fixture_1", "qd_fixture_2"],
    )?;
    cohort["proposalPopulation"] = json!(false);
    rehash(&mut cohort, "populationSha256")?;
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["evaluationPopulationSha256"] = cohort["populationSha256"].clone();
    rehash(&mut manifest, "manifestSha256")?;
    write_canonical(
        &directory.path().join("cohort-population.json"),
        &cohort,
        true,
    )?;
    write_canonical(&manifest_path, &manifest, true)?;
    let error = execute_manifest(&manifest_path).expect_err("proposal flag substitution must fail");
    assert!(
        format!("{error:#}").contains("proposal role binding mismatch"),
        "{error:#}"
    );
    Ok(())
}

#[test]
fn rotating_cohort_rejects_duplicate_missing_reordered_hash_and_manifest_binding() -> Result<()> {
    let (directory, manifest_path, mut cohort) = cohort_fixture(
        "retained_parent_current_panel",
        "current-panel",
        &["qd_fixture_1", "qd_fixture_2"],
    )?;
    cohort["candidates"][1] = cohort["candidates"][0].clone();
    rehash(&mut cohort, "populationSha256")?;
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["evaluationPopulationSha256"] = cohort["populationSha256"].clone();
    rehash(&mut manifest, "manifestSha256")?;
    write_canonical(
        &directory.path().join("cohort-population.json"),
        &cohort,
        true,
    )?;
    write_canonical(&manifest_path, &manifest, true)?;
    let error = execute_manifest(&manifest_path).expect_err("duplicate IDs must fail closed");
    assert!(
        format!("{error:#}").contains("repeats a candidate identity"),
        "{error:#}"
    );

    let (directory, manifest_path, mut cohort) = cohort_fixture(
        "retained_parent_current_panel",
        "current-panel",
        &["qd_fixture_1", "qd_fixture_2"],
    )?;
    cohort["candidates"].as_array_mut().unwrap().pop();
    rehash(&mut cohort, "populationSha256")?;
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["evaluationPopulationSha256"] = cohort["populationSha256"].clone();
    rehash(&mut manifest, "manifestSha256")?;
    write_canonical(
        &directory.path().join("cohort-population.json"),
        &cohort,
        true,
    )?;
    write_canonical(&manifest_path, &manifest, true)?;
    let error = execute_manifest(&manifest_path).expect_err("missing candidate must fail closed");
    assert!(format!("{error:#}").contains("candidate count mismatch"));

    let (directory, manifest_path, mut cohort) = cohort_fixture(
        "retained_parent_current_panel",
        "current-panel",
        &["qd_fixture_1", "qd_fixture_2"],
    )?;
    cohort["candidates"].as_array_mut().unwrap().reverse();
    rehash(&mut cohort, "populationSha256")?;
    write_canonical(
        &directory.path().join("cohort-population.json"),
        &cohort,
        true,
    )?;
    let error = execute_manifest(&manifest_path).expect_err("reorder must remain manifest-bound");
    assert!(format!("{error:#}").contains("manifest binding mismatch"));

    let (directory, manifest_path, mut cohort) = cohort_fixture(
        "retained_parent_current_panel",
        "current-panel",
        &["qd_fixture_1", "qd_fixture_2"],
    )?;
    cohort["populationSha256"] =
        json!("sha256:0000000000000000000000000000000000000000000000000000000000000000");
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["evaluationPopulationSha256"] = cohort["populationSha256"].clone();
    rehash(&mut manifest, "manifestSha256")?;
    write_canonical(
        &directory.path().join("cohort-population.json"),
        &cohort,
        true,
    )?;
    write_canonical(&manifest_path, &manifest, true)?;
    let error = execute_manifest(&manifest_path).expect_err("invalid cohort self-hash must fail");
    assert!(format!("{error:#}").contains("identity mismatch"));

    let (_directory, manifest_path, _cohort) = cohort_fixture(
        "retained_parent_current_panel",
        "current-panel",
        &["qd_fixture_1", "qd_fixture_2"],
    )?;
    let mut manifest: Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    manifest["evaluationPopulationSha256"] =
        json!("sha256:0000000000000000000000000000000000000000000000000000000000000000");
    rehash(&mut manifest, "manifestSha256")?;
    write_canonical(&manifest_path, &manifest, true)?;
    let error = execute_manifest(&manifest_path).expect_err("manifest binding must fail");
    assert!(format!("{error:#}").contains("manifest binding mismatch"));
    Ok(())
}

#[test]
fn legacy_evaluation_population_result_identity_is_unchanged() -> Result<()> {
    let (directory, result) = fixture()?;
    let evaluation: Value = serde_json::from_slice(&fs::read(
        directory.path().join("evaluation-population.json"),
    )?)?;
    assert_eq!(
        result["evaluationPopulationSha256"],
        evaluation["evaluationPopulationSha256"]
    );
    assert_eq!(result["populationSha256"], evaluation["populationSha256"]);
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
