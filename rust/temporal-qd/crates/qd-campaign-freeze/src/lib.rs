//! Streaming native materialization for an already sealed Temporal QD authority.
//!
//! The input is intentionally an authority, not a mutable factory.  This
//! prevents a future controller from silently falling back to Python candidate
//! construction while still moving the ``candidate × window`` task matrix and
//! its restart artifacts behind a bounded-memory native transaction.

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
};
use temporal_qd_contract::{
    CONTRACT_VERSION, JsonNewline, canonical_json_bytes, canonical_json_line, canonical_sha256,
    canonical_sha256_without_object_field, python_pretty_json_line,
};

pub const MANIFEST_SCHEMA: &str = "temporal_qd_native_campaign_task_matrix_manifest_v1";
pub const RESULT_SCHEMA: &str = "temporal_qd_native_campaign_task_matrix_result_v1";
/// The v5 freezer is deliberately a different manifest, rather than a loose
/// extension of the legacy authority-only materializer.  Its inputs are
/// sealed source artifacts and configuration; it has no Python candidate or
/// task construction escape hatch.
pub const V5_FREEZE_MANIFEST_SCHEMA: &str = "temporal_qd_v5_native_campaign_freeze_manifest_v2";
pub const V5_FREEZE_RESULT_SCHEMA: &str = "temporal_qd_v5_native_campaign_freeze_result_v1";
/// Evidence-ladder adapter: binds an archive-reducer output and the sealed
/// prefinalizer selection before delegating only task streaming to the v5
/// freezer.  The rotating freezer schema remains unchanged for non-ladder
/// campaigns.
pub const V5_LADDER_FREEZE_MANIFEST_SCHEMA: &str =
    "temporal_qd_v5_native_evidence_ladder_freeze_manifest_v2";
pub const V5_LADDER_FREEZE_RESULT_SCHEMA: &str =
    "temporal_qd_v5_native_evidence_ladder_freeze_result_v1";
/// Current archive-native evidence ladder.  Unlike v2, this request does not
/// accept an ambient evaluation population: the selected rich candidates are
/// projected from an authenticated archive authority.
pub const V5_LADDER_ARCHIVE_FREEZE_MANIFEST_SCHEMA: &str =
    "temporal_qd_v5_native_evidence_ladder_freeze_manifest_v3";
pub const V5_LADDER_ARCHIVE_FREEZE_RESULT_SCHEMA: &str =
    "temporal_qd_v5_native_evidence_ladder_freeze_result_v3";
pub const V5_LADDER_ARCHIVE_FREEZE_TRANSACTION_SCHEMA: &str =
    "temporal_qd_v5_native_evidence_ladder_freeze_transaction_v3";
pub const V5_LADDER_ARCHIVE_FREEZE_RECEIPT_SCHEMA: &str =
    "temporal_qd_v5_native_evidence_ladder_freeze_receipt_v3";
pub const V5_LADDER_AUTHORITY_SCHEMA: &str = "temporal_qd_v5_native_evidence_ladder_authority_v2";
pub const V5_LADDER_MATERIALIZATION_MANIFEST_SCHEMA: &str =
    "temporal_qd_v5_native_evidence_ladder_materialization_manifest_v2";
pub const V5_LADDER_MATERIALIZATION_RESULT_SCHEMA: &str =
    "temporal_qd_v5_native_evidence_ladder_materialization_result_v2";
pub const V5_LADDER_MATERIALIZATION_TRANSACTION_SCHEMA: &str =
    "temporal_qd_v5_native_evidence_ladder_materialization_transaction_v2";
pub const V5_LADDER_MATERIALIZATION_RECEIPT_SCHEMA: &str =
    "temporal_qd_v5_native_evidence_ladder_materialization_receipt_v2";
const AUTHORITY_SCHEMA: &str = "temporal_graph_candidate_window_authority_v1";
const TASK_MANIFEST_SCHEMA: &str = "temporal_graph_candidate_window_manifest_v1";
const CHECKPOINT_SCHEMA: &str = "temporal_graph_candidate_window_checkpoint_v1";
const TASK_KIND: &str = "temporal_graph_candidate_window";
const JOB_SCHEMA: &str = "temporal_graph_candidate_window_job_v1";
const TASK_CAPABILITY: &str = "temporal_graph_candidate_window_v1";
const BIDIRECTIONAL_CAPABILITY: &str = "temporal_graph_bidirectional_replay_v1";
const ATTRIBUTION_CAPABILITY: &str = "temporal_candidate_behavior_attribution_v1";
pub const V5_CAMPAIGN_INPUT_CHECKPOINT_SCHEMA: &str = "temporal_qd_v5_campaign_input_checkpoint_v1";
pub const V5_CAMPAIGN_INPUT_RESULT_SCHEMA: &str = "temporal_qd_v5_campaign_input_result_v1";
pub const V5_CAMPAIGN_INPUT_CHECKPOINT_PATH: &str = "campaign-input-checkpoint.json";
pub const V5_CAMPAIGN_TASK_PACK_RELATIVE_PATH: &str = "screening-run/tasks.jsonl";
pub const V5_CAMPAIGN_COHORT_POPULATION_RELATIVE_PATH: &str = "cohort-population.json";
const V5_RUNTIME_AUTHORITY_SCHEMA: &str = "temporal_qd_native_campaign_freeze_runtime_authority_v1";
pub const COHORT_SELECTION_SCHEMA: &str = "temporal_qd_rotating_cohort_selection_v1";
pub const COHORT_PROJECTION_ROW_SCHEMA: &str = "temporal_qd_rotating_candidate_projection_row_v1";
pub const NON_PROPOSAL_TASK_SELECTION_SCHEMA: &str =
    "temporal_qd_v5_non_proposal_task_selection_v2";
pub const NON_PROPOSAL_TASK_SELECTION_RECEIPT_SCHEMA: &str =
    "temporal_qd_v5_non_proposal_task_selection_receipt_v2";
pub const ROTATING_COHORT_POPULATION_SCHEMA: &str = "temporal_qd_rotating_cohort_population_v1";

#[derive(Clone, Debug)]
pub struct V5CampaignInputCheckpoint {
    pub value: Value,
    pub root: PathBuf,
    pub manifest_sha256: String,
    pub native_runtime_authority_sha256: String,
    pub generation_index: u64,
    pub campaign_role: String,
    pub panel_id: String,
    pub authority_id: String,
    pub campaign_sha256: String,
    pub evaluation_identity_sha256: String,
    pub task_matrix_sha256: String,
    pub candidate_count: u64,
    pub window_count: u64,
    pub task_count: u64,
    pub cohort_population_sha256: String,
    pub checkpoint_sha256: String,
    pub task_pack_path: PathBuf,
    pub task_pack_raw_sha256: String,
    pub task_pack_size_bytes: u64,
    pub cohort_population_path: PathBuf,
    pub cohort_population_raw_sha256: String,
    pub cohort_population_size_bytes: u64,
}

/// Open the one durable campaign-input boundary used by gateway dispatch and
/// campaign reduction.  The checkpoint authenticates only the two
/// candidate-scale payloads that remain necessary: the task matrix and the
/// role-specific cohort population.
pub fn open_v5_campaign_input_checkpoint(path: &Path) -> Result<V5CampaignInputCheckpoint> {
    let path = path
        .canonicalize()
        .with_context(|| format!("open campaign-input checkpoint: {}", path.display()))?;
    ensure!(path.is_file(), "campaign-input checkpoint is not a file");
    ensure!(
        !fs::symlink_metadata(&path)?.file_type().is_symlink(),
        "campaign-input checkpoint symlink is forbidden"
    );
    let root = path
        .parent()
        .ok_or_else(|| anyhow!("campaign-input checkpoint has no parent"))?
        .to_path_buf();
    let value = read_canonical_json_line(&path, "campaign-input checkpoint")?;
    let map = object(&value, "campaign-input checkpoint")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "contractVersion",
            "manifestSha256",
            "nativeRuntimeAuthoritySha256",
            "generationIndex",
            "campaignRole",
            "panelId",
            "authorityId",
            "campaignSha256",
            "evaluationIdentitySha256",
            "taskMatrixSha256",
            "candidateCount",
            "windowCount",
            "taskCount",
            "tasks",
            "cohortPopulation",
            "sourceInputs",
            "artifactMetrics",
            "checkpointSha256",
        ],
        "campaign-input checkpoint",
    )?;
    ensure!(
        string(map, "schemaVersion")? == V5_CAMPAIGN_INPUT_CHECKPOINT_SCHEMA
            && string(map, "contractVersion")? == CONTRACT_VERSION,
        "campaign-input checkpoint schema/version is incompatible"
    );
    let checkpoint_sha256 = string(map, "checkpointSha256")?.to_owned();
    require_sha(&checkpoint_sha256, "campaign-input checkpoint SHA-256")?;
    ensure!(
        canonical_sha256_without_object_field(&value, "checkpointSha256")? == checkpoint_sha256,
        "campaign-input checkpoint identity drifted"
    );
    let generation_index = map
        .get("generationIndex")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("campaign-input generation index is invalid"))?;
    let candidate_count = map
        .get("candidateCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("campaign-input candidate count is invalid"))?;
    let window_count = map
        .get("windowCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("campaign-input window count is invalid"))?;
    let task_count = map
        .get("taskCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("campaign-input task count is invalid"))?;
    ensure!(
        candidate_count
            .checked_mul(window_count)
            .is_some_and(|expected| expected == task_count),
        "campaign-input task cardinality drifted"
    );
    let task_pack_path = validate_campaign_input_artifact(
        &root,
        map.get("tasks")
            .ok_or_else(|| anyhow!("campaign-input task-pack descriptor is missing"))?,
        V5_CAMPAIGN_TASK_PACK_RELATIVE_PATH,
        "taskMatrixSha256",
        string(map, "taskMatrixSha256")?,
        "campaign-input task pack",
    )?;
    let task_pack_descriptor = object(
        map.get("tasks")
            .ok_or_else(|| anyhow!("campaign-input task-pack descriptor is missing"))?,
        "campaign-input task-pack descriptor",
    )?;
    let task_pack_raw_sha256 = string(task_pack_descriptor, "rawSha256")?.to_owned();
    let task_pack_size_bytes = task_pack_descriptor
        .get("sizeBytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("campaign-input task-pack size is invalid"))?;
    ensure!(
        task_pack_descriptor
            .get("recordCount")
            .and_then(Value::as_u64)
            == Some(task_count),
        "campaign-input task-pack record count drifted"
    );
    let cohort_population_descriptor = object(
        map.get("cohortPopulation")
            .ok_or_else(|| anyhow!("campaign-input cohort descriptor is missing"))?,
        "campaign-input cohort descriptor",
    )?;
    let cohort_population_raw_sha256 =
        string(cohort_population_descriptor, "rawSha256")?.to_owned();
    let cohort_population_size_bytes = cohort_population_descriptor
        .get("sizeBytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("campaign-input cohort size is invalid"))?;
    let cohort_population_sha256 = string(
        object(
            map.get("cohortPopulation")
                .ok_or_else(|| anyhow!("campaign-input cohort descriptor is missing"))?,
            "campaign-input cohort descriptor",
        )?,
        "populationSha256",
    )?
    .to_owned();
    let cohort_population_path = validate_campaign_input_artifact(
        &root,
        map.get("cohortPopulation")
            .ok_or_else(|| anyhow!("campaign-input cohort descriptor is missing"))?,
        V5_CAMPAIGN_COHORT_POPULATION_RELATIVE_PATH,
        "populationSha256",
        &cohort_population_sha256,
        "campaign-input cohort population",
    )?;
    let cohort_population =
        read_pretty_json(&cohort_population_path, "campaign-input cohort population")?;
    let cohort_map = object(&cohort_population, "campaign-input cohort population")?;
    ensure!(
        string(cohort_map, "schemaVersion")? == ROTATING_COHORT_POPULATION_SCHEMA
            && string(cohort_map, "populationSha256")? == cohort_population_sha256
            && canonical_sha256_without_object_field(&cohort_population, "populationSha256")?
                == cohort_population_sha256
            && cohort_map.get("candidateCount").and_then(Value::as_u64) == Some(candidate_count),
        "campaign-input cohort population binding drifted"
    );
    let source_inputs = object(
        map.get("sourceInputs")
            .ok_or_else(|| anyhow!("campaign-input source inputs are missing"))?,
        "campaign-input source inputs",
    )?;
    exact_keys(
        source_inputs,
        &[
            "evaluationPopulationRawSha256",
            "templatePreparationSha256",
            "constructionCatalogSha256",
            "preparationSha256",
        ],
        "campaign-input source inputs",
    )?;
    for field in [
        "evaluationPopulationRawSha256",
        "templatePreparationSha256",
        "constructionCatalogSha256",
        "preparationSha256",
    ] {
        require_sha(string(source_inputs, field)?, field)?;
    }
    let artifact_metrics = object(
        map.get("artifactMetrics")
            .ok_or_else(|| anyhow!("campaign-input artifact metrics are missing"))?,
        "campaign-input artifact metrics",
    )?;
    exact_keys(
        artifact_metrics,
        &[
            "payloadFileCount",
            "payloadBytes",
            "taskPackBytes",
            "cohortPopulationBytes",
        ],
        "campaign-input artifact metrics",
    )?;
    let task_pack_bytes = fs::metadata(&task_pack_path)?.len();
    let cohort_population_bytes = fs::metadata(&cohort_population_path)?.len();
    ensure!(
        artifact_metrics
            .get("payloadFileCount")
            .and_then(Value::as_u64)
            == Some(2)
            && artifact_metrics
                .get("taskPackBytes")
                .and_then(Value::as_u64)
                == Some(task_pack_bytes)
            && artifact_metrics
                .get("cohortPopulationBytes")
                .and_then(Value::as_u64)
                == Some(cohort_population_bytes)
            && artifact_metrics.get("payloadBytes").and_then(Value::as_u64)
                == task_pack_bytes.checked_add(cohort_population_bytes),
        "campaign-input artifact metrics drifted"
    );
    let manifest_sha256 = string(map, "manifestSha256")?.to_owned();
    let native_runtime_authority_sha256 = string(map, "nativeRuntimeAuthoritySha256")?.to_owned();
    let campaign_role = string(map, "campaignRole")?.to_owned();
    let panel_id = string(map, "panelId")?.to_owned();
    let authority_id = string(map, "authorityId")?.to_owned();
    let campaign_sha256 = string(map, "campaignSha256")?.to_owned();
    let evaluation_identity_sha256 = string(map, "evaluationIdentitySha256")?.to_owned();
    let task_matrix_sha256 = string(map, "taskMatrixSha256")?.to_owned();
    Ok(V5CampaignInputCheckpoint {
        value,
        root,
        manifest_sha256,
        native_runtime_authority_sha256,
        generation_index,
        campaign_role,
        panel_id,
        authority_id,
        campaign_sha256,
        evaluation_identity_sha256,
        task_matrix_sha256,
        candidate_count,
        window_count,
        task_count,
        cohort_population_sha256,
        checkpoint_sha256,
        task_pack_path,
        task_pack_raw_sha256,
        task_pack_size_bytes,
        cohort_population_path,
        cohort_population_raw_sha256,
        cohort_population_size_bytes,
    })
}

fn v5_campaign_input_result(checkpoint: &V5CampaignInputCheckpoint, restart: bool) -> Value {
    let artifact_metrics = checkpoint
        .value
        .get("artifactMetrics")
        .cloned()
        .unwrap_or(Value::Null);
    json!({
        "schemaVersion": V5_CAMPAIGN_INPUT_RESULT_SCHEMA,
        "checkpointPath": checkpoint.root.join(V5_CAMPAIGN_INPUT_CHECKPOINT_PATH),
        "checkpointSha256": checkpoint.checkpoint_sha256.clone(),
        "campaignSha256": checkpoint.campaign_sha256.clone(),
        "authorityId": checkpoint.authority_id.clone(),
        "taskMatrixSha256": checkpoint.task_matrix_sha256.clone(),
        "candidateCount": checkpoint.candidate_count,
        "windowCount": checkpoint.window_count,
        "taskCount": checkpoint.task_count,
        "campaignRole": checkpoint.campaign_role.clone(),
        "panelId": checkpoint.panel_id.clone(),
        "evaluationIdentitySha256": checkpoint.evaluation_identity_sha256.clone(),
        "cohortPopulationSha256": checkpoint.cohort_population_sha256.clone(),
        "outputRoot": checkpoint.root.clone(),
        "restart": restart,
        "artifactMetrics": artifact_metrics,
    })
}

fn validate_campaign_input_artifact(
    root: &Path,
    descriptor: &Value,
    expected_relative: &str,
    semantic_field: &str,
    semantic_sha256: &str,
    label: &str,
) -> Result<PathBuf> {
    let map = object(descriptor, label)?;
    let expected = if semantic_field == "taskMatrixSha256" {
        vec![
            "relativePath",
            "rawSha256",
            "sizeBytes",
            "recordCount",
            semantic_field,
        ]
    } else {
        vec!["relativePath", "rawSha256", "sizeBytes", semantic_field]
    };
    exact_keys(map, &expected, label)?;
    ensure!(
        string(map, "relativePath")? == expected_relative
            && string(map, semantic_field)? == semantic_sha256,
        "{label} semantic/path binding drifted"
    );
    require_sha(string(map, "rawSha256")?, label)?;
    require_sha(semantic_sha256, label)?;
    let relative = Path::new(expected_relative);
    ensure!(
        !relative.is_absolute()
            && relative
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "{label} relative path is unsafe"
    );
    let path = root.join(relative);
    ensure!(path.is_file(), "{label} is missing");
    ensure!(
        !fs::symlink_metadata(&path)?.file_type().is_symlink(),
        "{label} symlink is forbidden"
    );
    ensure!(
        fs::metadata(&path)?.len()
            == map
                .get("sizeBytes")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("{label} size is invalid"))?
            && file_sha256(&path)? == string(map, "rawSha256")?,
        "{label} byte identity drifted"
    );
    Ok(path)
}

const REQUIRED_CAPABILITIES: [&str; 8] = [
    TASK_CAPABILITY,
    BIDIRECTIONAL_CAPABILITY,
    "temporal_graph_replay_v1",
    "management.scalar.price_level.completed_bar",
    "management.scalar.price_distance.completed_bar",
    "management.initial.dynamic",
    "management.trailing.indicator",
    "management.action.dynamic",
];

pub fn execute_manifest(manifest_path: &Path) -> Result<Value> {
    let manifest = read_pretty_json(manifest_path, "campaign task-matrix manifest")?;
    let spec = object(&manifest, "campaign task-matrix manifest")?;
    if spec.get("schemaVersion").and_then(Value::as_str) == Some(V5_FREEZE_MANIFEST_SCHEMA) {
        return execute_v5_freeze_manifest(spec);
    }
    if spec.get("schemaVersion").and_then(Value::as_str) == Some(V5_LADDER_FREEZE_MANIFEST_SCHEMA) {
        return execute_v5_ladder_freeze_manifest(spec);
    }
    if spec.get("schemaVersion").and_then(Value::as_str)
        == Some(V5_LADDER_ARCHIVE_FREEZE_MANIFEST_SCHEMA)
    {
        return execute_v5_ladder_archive_freeze_manifest(spec);
    }
    if spec.get("schemaVersion").and_then(Value::as_str)
        == Some(V5_LADDER_MATERIALIZATION_MANIFEST_SCHEMA)
    {
        return execute_v5_ladder_materialization_manifest(spec);
    }
    exact_keys(
        spec,
        &[
            "schemaVersion",
            "authorityPath",
            "outputRoot",
            "behaviorAttributionRequirement",
        ],
        "campaign task-matrix manifest",
    )?;
    ensure!(
        string(spec, "schemaVersion")? == MANIFEST_SCHEMA,
        "campaign task-matrix manifest schema is incompatible"
    );
    let authority_path = PathBuf::from(string(spec, "authorityPath")?);
    let root = PathBuf::from(string(spec, "outputRoot")?);
    ensure!(
        authority_path.is_file(),
        "campaign task-matrix authority path is not a file"
    );
    fs::create_dir_all(&root).context("create campaign task-matrix output root")?;
    let authority = read_pretty_json(&authority_path, "campaign task-matrix authority")?;
    let authority_map = object(&authority, "campaign task-matrix authority")?;
    ensure!(
        string(authority_map, "schemaVersion")? == AUTHORITY_SCHEMA,
        "campaign task-matrix authority schema is incompatible"
    );
    let authority_id = string(authority_map, "authorityId")?.to_owned();
    require_sha(&authority_id, "authorityId")?;
    let attribution = spec
        .get("behaviorAttributionRequirement")
        .filter(|v| !v.is_null());
    write_once_pretty(&root.join("authority.json"), &authority)?;
    let generated = stream_task_manifest(authority_map, &authority_id, &root, attribution)?;
    write_or_verify_checkpoint(
        &root.join("checkpoint.json"),
        &authority_id,
        &generated.task_matrix_sha256,
    )?;
    Ok(json!({
        "schemaVersion": RESULT_SCHEMA, "authorityId": authority_id,
        "taskMatrixSha256": generated.task_matrix_sha256.clone(), "candidateCount": generated.candidate_count,
        "windowCount": generated.window_count, "taskCount": generated.task_count,
        // Preserve the manifest spelling.  Windows `canonicalize` prepends a
        // `\\?\\` device namespace, which would make the bridge's otherwise
        // identical path binding spuriously fail on every successful run.
        "outputRoot": root.to_string_lossy(),
        "telemetry": {"schemaVersion":"temporal_qd_native_campaign_task_matrix_telemetry_v1", "peakLiveTasks":1, "taskMatrixEncodedBytes":generated.bytes, "materialization":"candidate_window_stream_v1"}
    }))
}

struct Generated {
    task_matrix_sha256: String,
    candidate_count: usize,
    window_count: usize,
    task_count: usize,
    bytes: u64,
}

fn stream_task_manifest(
    authority: &Map<String, Value>,
    authority_id: &str,
    root: &Path,
    attribution: Option<&Value>,
) -> Result<Generated> {
    let candidates = array(authority, "candidates")?;
    let windows = array(authority, "developmentWindows")?;
    ensure!(
        !candidates.is_empty() && !windows.is_empty(),
        "campaign authority needs candidates and windows"
    );
    let expected = candidates
        .len()
        .checked_mul(windows.len())
        .ok_or_else(|| anyhow!("campaign task count overflow"))?;
    let staging = root.join(".native-task-manifest.staging");
    if staging.exists() {
        fs::remove_file(&staging).context("remove stale private native task staging")?;
    }
    // Sorted-key Python pretty JSON outer layout. The fixed-width digest is
    // patched after the one-pass canonical task-array hash completes.
    let mut out = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)?,
    );
    out.write_all(b"{\n  \"authorityId\": ")?;
    write_json_scalar(&mut out, authority_id)?;
    write!(
        out,
        ",\n  \"schemaVersion\": \"{}\",\n  \"taskCount\": {},\n  \"taskMatrixSha256\": \"",
        TASK_MANIFEST_SCHEMA, expected
    )?;
    let hash_offset = out.stream_position()?;
    out.write_all(b"sha256:")?;
    out.write_all(&[b'0'; 64])?;
    out.write_all(b"\",\n  \"tasks\": [")?;
    let mut hasher = Sha256::new();
    hasher.update(b"[");
    let mut count = 0usize;
    for candidate in candidates {
        let candidate = object(candidate, "campaign authority candidate")?;
        for window in windows {
            let task = build_task(
                authority,
                authority_id,
                candidate,
                object(window, "campaign authority window")?,
                attribution,
            )?;
            if count > 0 {
                out.write_all(b",")?;
                hasher.update(b",");
            }
            out.write_all(b"\n    ")?;
            write_nested_pretty(
                &mut out,
                &python_pretty_json_line(&task, JsonNewline::Lf)?,
                4,
            )?;
            hasher.update(canonical_json_bytes(&task)?);
            count += 1;
        }
    }
    hasher.update(b"]");
    ensure!(count == expected, "campaign task stream count drifted");
    let task_matrix_sha256 = prefixed(hasher.finalize());
    out.write_all(b"\n  ]\n}\n")?;
    out.flush()?;
    drop(out);
    let mut patch = OpenOptions::new().write(true).open(&staging)?;
    patch.seek(SeekFrom::Start(hash_offset))?;
    patch.write_all(task_matrix_sha256.as_bytes())?;
    patch.flush()?;
    drop(patch);
    let bytes = fs::metadata(&staging)?.len();
    write_once_from_staging(&root.join("task-manifest.json"), &staging)?;
    Ok(Generated {
        task_matrix_sha256,
        candidate_count: candidates.len(),
        window_count: windows.len(),
        task_count: count,
        bytes,
    })
}

#[derive(Clone, Debug)]
struct GeneratedTaskPack {
    task_matrix_sha256: String,
    raw_sha256: String,
    task_count: usize,
    bytes: u64,
}

/// Current-v5 task payload.  Tasks are written once as canonical JSONL so the
/// gateway can index and dispatch the committed payload directly; it no longer
/// copies the complete matrix into a second task-object or task-pack tree.
fn stream_v5_task_pack(
    authority: &Map<String, Value>,
    authority_id: &str,
    root: &Path,
    attribution: Option<&Value>,
) -> Result<GeneratedTaskPack> {
    let candidates = array(authority, "candidates")?;
    let windows = array(authority, "developmentWindows")?;
    ensure!(
        !candidates.is_empty() && !windows.is_empty(),
        "campaign authority needs candidates and windows"
    );
    let expected = candidates
        .len()
        .checked_mul(windows.len())
        .ok_or_else(|| anyhow!("campaign task count overflow"))?;
    let staging = root.join(".native-task-pack.staging");
    if staging.exists() {
        fs::remove_file(&staging).context("remove stale private task-pack staging")?;
    }
    let mut out = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)?,
    );
    let mut raw_hasher = Sha256::new();
    let mut matrix_hasher = Sha256::new();
    matrix_hasher.update(b"[");
    let mut count = 0usize;
    let mut bytes = 0u64;
    for candidate in candidates {
        let candidate = object(candidate, "campaign authority candidate")?;
        for window in windows {
            let task = build_task(
                authority,
                authority_id,
                candidate,
                object(window, "campaign authority window")?,
                attribution,
            )?;
            let canonical = canonical_json_bytes(&task)?;
            let line = canonical_json_line(&task)?;
            if count > 0 {
                matrix_hasher.update(b",");
            }
            matrix_hasher.update(&canonical);
            raw_hasher.update(&line);
            out.write_all(&line)?;
            bytes = bytes
                .checked_add(line.len() as u64)
                .ok_or_else(|| anyhow!("campaign task-pack byte count overflow"))?;
            count += 1;
        }
    }
    matrix_hasher.update(b"]");
    ensure!(count == expected, "campaign task-pack count drifted");
    out.flush()?;
    out.get_ref()
        .sync_all()
        .context("fsync campaign task pack")?;
    drop(out);
    let destination = root.join("tasks.jsonl");
    write_once_from_staging(&destination, &staging)?;
    Ok(GeneratedTaskPack {
        task_matrix_sha256: prefixed(matrix_hasher.finalize()),
        raw_sha256: prefixed(raw_hasher.finalize()),
        task_count: count,
        bytes,
    })
}

fn build_task(
    authority: &Map<String, Value>,
    authority_id: &str,
    candidate: &Map<String, Value>,
    window: &Map<String, Value>,
    attribution: Option<&Value>,
) -> Result<Value> {
    let candidate_id = string(candidate, "candidateId")?;
    let window_id = string(window, "windowId")?;
    let input = array(candidate, "windowInputs")?
        .iter()
        .find_map(|row| {
            row.as_object()
                .filter(|row| row.get("windowId").and_then(Value::as_str) == Some(window_id))
        })
        .ok_or_else(|| anyhow!("candidate lacks window input"))?;
    let evidence = object(
        input
            .get("evidencePlan")
            .ok_or_else(|| anyhow!("candidate evidence plan missing"))?,
        "candidate evidence plan",
    )?;
    let evidence_id = string(input, "evidencePlanId")?;
    let semantic = string(input, "lakeWindowSemanticSha256")?;
    let source_sha = string(candidate, "sourceProfileSha256")?;
    let shared = canonical_sha256(
        &json!({"candidateSnapshotSha256":source_sha,"evidencePlanId":evidence_id,"windowId":window_id,"windowSemanticSha256":semantic}),
    )?;
    let identity = canonical_sha256(
        &json!({"authorityId":authority_id,"candidateId":candidate_id,"windowId":window_id}),
    )?;
    let task_id = format!("temporal-search-{}", &identity[7..39]);
    let profile = object(
        candidate
            .get("sourceProfile")
            .ok_or_else(|| anyhow!("candidate profile missing"))?,
        "candidate profile",
    )?;
    let execution = object(
        profile
            .get("executionConfig")
            .ok_or_else(|| anyhow!("candidate execution config missing"))?,
        "candidate execution config",
    )?;
    let worker = object(
        authority
            .get("workerContract")
            .ok_or_else(|| anyhow!("authority worker contract missing"))?,
        "authority worker contract",
    )?;
    let mut job = Map::new();
    for (key, value) in [
        ("schema_version", JOB_SCHEMA),
        ("job_id", &task_id),
        ("candidate_id", candidate_id),
        ("window_id", window_id),
        ("authority_id", authority_id),
        ("lake_window_semantic_sha256", semantic),
        ("shared_observation_stream_id", &shared),
        ("user_id", "temporal-search"),
        ("profile_id", candidate_id),
    ] {
        put(&mut job, key, value);
    }
    job.insert(
        "inline_profile_snapshot".to_owned(),
        Value::Object(profile.clone()),
    );
    job.insert(
        "instruments".to_owned(),
        Value::Array(vec![
            candidate
                .get("instrument")
                .cloned()
                .ok_or_else(|| anyhow!("candidate instrument missing"))?,
        ]),
    );
    copy(&mut job, "timeframe", candidate, "timeframe")?;
    copy(&mut job, "bar_limit", candidate, "barLimit")?;
    put(
        &mut job,
        "evaluator_id",
        if profile.get("version").and_then(Value::as_str) == Some("v3") {
            "bar_bidirectional_single_position_execution_v2"
        } else {
            "bar_single_position_execution_v1"
        },
    );
    copy(
        &mut job,
        "analysis_window_start",
        window,
        "analysisWindowStart",
    )?;
    copy(&mut job, "analysis_window_end", window, "analysisWindowEnd")?;
    job.insert("evidence_plan".to_owned(), Value::Object(evidence.clone()));
    copy(
        &mut job,
        "required_worker_contract_hash",
        worker,
        "workerContractSha256",
    )?;
    copy(
        &mut job,
        "required_worker_contract_schema",
        worker,
        "workerContractSchema",
    )?;
    job.insert("required_capabilities".to_owned(), capabilities(false));
    put(&mut job, "client_origin", "temporal_search_controller");
    put(&mut job, "campaign_id", authority_id);
    put(&mut job, "lane_id", candidate_id);
    put(&mut job, "attempt_id", &task_id);
    for (source, target) in [
        (
            "profileSnapshotSha256",
            "normalized_profile_snapshot_sha256",
        ),
        ("programSha256", "authored_program_sha256"),
        (
            "resolvedProfileSnapshotSha256",
            "expected_resolved_profile_snapshot_sha256",
        ),
        ("resolvedProgramSha256", "expected_resolved_program_sha256"),
    ] {
        if let Some(value) = candidate.get(source) {
            job.insert(target.to_owned(), value.clone());
        }
    }
    if execution.contains_key("managementLibrary") {
        job.insert(
            "execution_config_sha256".to_owned(),
            Value::String(canonical_sha256(&Value::Object(execution.clone()))?),
        );
    } else {
        let exit = object(
            execution
                .get("exitPolicy")
                .ok_or_else(|| anyhow!("candidate exit policy missing"))?,
            "candidate exit policy",
        )?;
        job.insert(
            "execution_cell".to_owned(),
            exit.get("selectedCell")
                .cloned()
                .ok_or_else(|| anyhow!("candidate selected cell missing"))?,
        );
    }
    let with_attribution = attribution.is_some();
    if let Some(requirement) = attribution {
        job.insert("candidate_behavior_attribution_request".to_owned(), json!({"schema_version":"temporal_candidate_behavior_attribution_request_v1","enabled":true,"attribution_schema":"temporal_candidate_behavior_attribution_v1","replay_cost_view":"research_conservative","behavior_attribution_requirement":requirement}));
        job.insert("required_capabilities".to_owned(), capabilities(true));
    }
    let bounds = object(
        authority
            .get("bounds")
            .ok_or_else(|| anyhow!("authority bounds missing"))?,
        "authority bounds",
    )?;
    let mut task = Map::new();
    for (key, value) in [
        ("task_id", task_id.as_str()),
        ("lane_id", candidate_id),
        ("attempt_id", task_id.as_str()),
        ("task_kind", TASK_KIND),
    ] {
        put(&mut task, key, value);
    }
    task.insert("payload".to_owned(), Value::Object(job));
    task.insert(
        "required_worker_capabilities".to_owned(),
        capabilities(with_attribution),
    );
    copy(&mut task, "deadline_seconds", bounds, "deadlineSeconds")?;
    copy(&mut task, "max_attempts", bounds, "maxAttempts")?;
    Ok(Value::Object(task))
}

fn capabilities(attribution: bool) -> Value {
    if !attribution {
        return Value::Array(
            REQUIRED_CAPABILITIES
                .iter()
                .map(|v| Value::String((*v).to_owned()))
                .collect(),
        );
    }
    let mut values: BTreeSet<String> = REQUIRED_CAPABILITIES
        .iter()
        .map(|v| (*v).to_owned())
        .collect();
    values.insert(ATTRIBUTION_CAPABILITY.to_owned());
    Value::Array(values.into_iter().map(Value::String).collect())
}
fn put(map: &mut Map<String, Value>, key: &str, value: &str) {
    map.insert(key.to_owned(), Value::String(value.to_owned()));
}
fn copy(
    dst: &mut Map<String, Value>,
    target: &str,
    src: &Map<String, Value>,
    source: &str,
) -> Result<()> {
    dst.insert(
        target.to_owned(),
        src.get(source)
            .cloned()
            .ok_or_else(|| anyhow!("required field {source} missing"))?,
    );
    Ok(())
}

fn write_or_verify_checkpoint(
    path: &Path,
    authority_id: &str,
    task_matrix_sha256: &str,
) -> Result<()> {
    let checkpoint = json!({"schemaVersion":CHECKPOINT_SCHEMA,"authorityId":authority_id,"taskMatrixSha256":task_matrix_sha256,"completed":{},"journal":[]});
    if path.exists() {
        let actual = read_pretty_json(path, "campaign task-matrix checkpoint")?;
        let map = object(&actual, "campaign task-matrix checkpoint")?;
        ensure!(
            map.get("schemaVersion").and_then(Value::as_str) == Some(CHECKPOINT_SCHEMA)
                && map.get("authorityId").and_then(Value::as_str) == Some(authority_id)
                && map.get("taskMatrixSha256").and_then(Value::as_str) == Some(task_matrix_sha256),
            "existing checkpoint does not bind this immutable authority and task matrix"
        );
        return Ok(());
    }
    write_once_pretty(path, &checkpoint)
}
fn write_nested_pretty(writer: &mut impl Write, bytes: &[u8], extra_indent: usize) -> Result<()> {
    let body = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    for (index, line) in body.split(|byte| *byte == b'\n').enumerate() {
        if index > 0 {
            writer.write_all(b"\n")?;
            writer.write_all(&vec![b' '; extra_indent])?;
        }
        writer.write_all(line)?;
    }
    Ok(())
}
fn write_json_scalar(writer: &mut impl Write, value: &str) -> Result<()> {
    let bytes = python_pretty_json_line(&Value::String(value.to_owned()), JsonNewline::Lf)?;
    writer.write_all(bytes.strip_suffix(b"\n").unwrap_or(&bytes))?;
    Ok(())
}
fn write_once_pretty(path: &Path, value: &Value) -> Result<()> {
    write_once_bytes(path, &python_pretty_json_line(value, JsonNewline::Lf)?)
}
/// Current-v5 bounded control documents are consumed directly by the compact
/// control plane.  Keep that ABI separate from the Python-compatible campaign
/// artifacts written through `write_once_pretty`.
fn write_once_current_v5_compact_json(path: &Path, value: &Value) -> Result<()> {
    write_once_bytes(path, &canonical_json_line(value)?)
}
fn write_once_from_staging(path: &Path, staging: &Path) -> Result<()> {
    if path.exists() {
        ensure!(
            files_equal(path, staging)?,
            "refusing to overwrite divergent immutable file: {}",
            path.display()
        );
        fs::remove_file(staging)?;
        return Ok(());
    }
    // The staging file lives in the destination directory, so rename is a
    // same-volume publication step and never copies the O(C×W) artifact into
    // a second in-memory buffer.
    fs::rename(staging, path).with_context(|| format!("publish immutable file {}", path.display()))
}

fn files_equal(left: &Path, right: &Path) -> Result<bool> {
    if fs::metadata(left)?.len() != fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left_file = File::open(left)?;
    let mut right_file = File::open(right)?;
    let mut left_chunk = [0_u8; 64 * 1024];
    let mut right_chunk = [0_u8; 64 * 1024];
    loop {
        let left_count = left_file.read(&mut left_chunk)?;
        let right_count = right_file.read(&mut right_chunk)?;
        if left_count != right_count {
            return Ok(false);
        }
        if left_count == 0 {
            return Ok(true);
        }
        if left_chunk[..left_count] != right_chunk[..right_count] {
            return Ok(false);
        }
    }
}
fn write_once_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    if path.exists() {
        let mut current = Vec::new();
        File::open(path)?.read_to_end(&mut current)?;
        ensure!(
            current == bytes,
            "refusing to overwrite divergent immutable file: {}",
            path.display()
        );
        return Ok(());
    }
    fs::write(path, bytes).with_context(|| format!("write immutable file {}", path.display()))
}
fn read_pretty_json(path: &Path, name: &str) -> Result<Value> {
    let mut raw = Vec::new();
    File::open(path)
        .with_context(|| format!("open {name}"))?
        .read_to_end(&mut raw)?;
    let value: Value = serde_json::from_slice(&raw).with_context(|| format!("parse {name}"))?;
    ensure!(
        python_pretty_json_line(&value, JsonNewline::Lf)? == raw,
        "{name} must be canonical pretty JSON plus LF"
    );
    Ok(value)
}
fn read_canonical_json_line(path: &Path, name: &str) -> Result<Value> {
    let raw = fs::read(path).with_context(|| format!("open {name}"))?;
    let value: Value = serde_json::from_slice(&raw).with_context(|| format!("parse {name}"))?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "{name} must be canonical JSON followed by LF"
    );
    Ok(value)
}
fn object<'a>(value: &'a Value, name: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{name} must be an object"))
}
fn array<'a>(map: &'a Map<String, Value>, field: &str) -> Result<&'a Vec<Value>> {
    map.get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("campaign authority {field} must be an array"))
}
fn string<'a>(map: &'a Map<String, Value>, field: &str) -> Result<&'a str> {
    map.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("campaign authority {field} must be a string"))
}
fn exact_keys(map: &Map<String, Value>, expected: &[&str], name: &str) -> Result<()> {
    let actual: BTreeSet<&str> = map.keys().map(String::as_str).collect();
    let wanted: BTreeSet<&str> = expected.iter().copied().collect();
    ensure!(actual == wanted, "{name} has an incompatible field set");
    Ok(())
}
fn require_sha(value: &str, name: &str) -> Result<()> {
    ensure!(
        value.len() == 71
            && value.starts_with("sha256:")
            && value[7..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{name} must be a canonical sha256"
    );
    Ok(())
}
fn prefixed(digest: impl AsRef<[u8]>) -> String {
    format!(
        "sha256:{}",
        digest
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

/// Resolve exactly the prefinalizer-selected candidates.  This is deliberately
/// independent from ambient archive paths: every JSONL byte and row identity
/// is checked before a candidate payload can enter the native freezer.
pub fn load_sealed_cohort_selection(path: &Path) -> Result<Vec<Value>> {
    let selection = read_pretty_json(path, "rotating cohort selection")?;
    let map = object(&selection, "rotating cohort selection")?;
    ensure!(
        string(map, "schemaVersion")? == COHORT_SELECTION_SCHEMA,
        "rotating cohort selection schema is incompatible"
    );
    ensure!(
        canonical_sha256_without_object_field(&selection, "selectionSha256")?
            == string(map, "selectionSha256")?,
        "rotating cohort selection identity drifted"
    );
    let ids = array(map, "candidateIds")?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("rotating cohort candidate ID is invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        !ids.is_empty() && ids.windows(2).all(|pair| pair[0] < pair[1]),
        "rotating cohort IDs are not strict lexical order"
    );
    let descriptor = object(
        map.get("candidateProjection")
            .ok_or_else(|| anyhow!("rotating cohort selection lacks projection"))?,
        "rotating cohort projection",
    )?;
    exact_keys(
        descriptor,
        &[
            "relativePath",
            "rawSha256",
            "sizeBytes",
            "recordCount",
            "rowSchema",
        ],
        "rotating cohort projection",
    )?;
    ensure!(
        string(descriptor, "rowSchema")? == COHORT_PROJECTION_ROW_SCHEMA,
        "rotating cohort projection row schema is incompatible"
    );
    let relative = Path::new(string(descriptor, "relativePath")?);
    ensure!(
        !relative.is_absolute()
            && !relative
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir)),
        "rotating cohort projection path is unsafe"
    );
    let projection = path
        .parent()
        .ok_or_else(|| anyhow!("rotating cohort selection lacks parent"))?
        .join(relative);
    ensure!(
        fs::metadata(&projection)?.len()
            == descriptor
                .get("sizeBytes")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("rotating cohort projection size is invalid"))?,
        "rotating cohort projection size drifted"
    );
    ensure!(
        file_sha256(&projection)? == string(descriptor, "rawSha256")?,
        "rotating cohort projection raw identity drifted"
    );
    let mut rows = std::collections::BTreeMap::new();
    for line in BufReader::new(File::open(&projection)?).split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let row: Value = serde_json::from_slice(&line)?;
        let mut canonical = temporal_qd_contract::canonical_json_line(&row)?;
        canonical.pop();
        ensure!(
            canonical == line,
            "rotating cohort projection row is not canonical JSONL"
        );
        let row_map = object(&row, "rotating cohort projection row")?;
        exact_keys(
            row_map,
            &[
                "schemaVersion",
                "candidateId",
                "candidateIdentitySha256",
                "candidate",
                "projectionRowSha256",
            ],
            "rotating cohort projection row",
        )?;
        ensure!(
            string(row_map, "schemaVersion")? == COHORT_PROJECTION_ROW_SCHEMA
                && canonical_sha256_without_object_field(&row, "projectionRowSha256")?
                    == string(row_map, "projectionRowSha256")?,
            "rotating cohort projection row identity drifted"
        );
        let candidate = row_map
            .get("candidate")
            .cloned()
            .ok_or_else(|| anyhow!("rotating cohort row lacks candidate"))?;
        ensure!(
            object(&candidate, "rotating cohort candidate")?
                .get("candidateIdentitySha256")
                .and_then(Value::as_str)
                == Some(string(row_map, "candidateIdentitySha256")?),
            "rotating cohort candidate identity binding drifted"
        );
        ensure!(
            rows.insert(string(row_map, "candidateId")?.to_owned(), candidate)
                .is_none(),
            "rotating cohort projection has duplicate candidate ID"
        );
    }
    ensure!(
        rows.len()
            == descriptor
                .get("recordCount")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("rotating cohort projection count is invalid"))?
                as usize,
        "rotating cohort projection count drifted"
    );
    ids.into_iter()
        .map(|id| {
            rows.remove(&id).ok_or_else(|| {
                anyhow!("rotating cohort selected candidate missing from projection")
            })
        })
        .collect()
}

/// Open the receipt-last v2 prefinalizer handoff.  This is deliberately
/// separate from `load_sealed_cohort_selection`: v1 remains the ladder ABI
/// and is never widened into an authority for non-proposal work.
fn load_v2_non_proposal_task_selection(
    path: &Path,
    generation: u64,
    round: Option<u64>,
    role: &str,
    panel: &str,
    rotating_sha: &str,
) -> Result<Vec<Value>> {
    ensure!(path.is_file(), "v2 task-selection document is not a file");
    ensure!(
        !fs::symlink_metadata(path)?.file_type().is_symlink(),
        "v2 task-selection document symlink is forbidden"
    );
    let raw = fs::read(path)?;
    let document: Value = serde_json::from_slice(&raw)?;
    ensure!(
        canonical_json_line(&document)? == raw,
        "v2 task-selection document is not canonical"
    );
    let document_map = object(&document, "v2 task-selection document")?;
    exact_keys(
        document_map,
        &[
            "schemaVersion",
            "prefinalizerResultSha256",
            "taskPlanSha256",
            "taskSha256",
            "semanticAuthoritySha256",
            "generationIndex",
            "roundIndex",
            "campaignRole",
            "panelId",
            "rotatingEvidenceSha256",
            "candidateSetSha256",
            "candidateRows",
            "sourceAuthority",
            "selectionReceiptRelativePath",
            "selectionDocumentSha256",
        ],
        "v2 task-selection document",
    )?;
    ensure!(
        string(document_map, "schemaVersion")? == NON_PROPOSAL_TASK_SELECTION_SCHEMA
            && canonical_sha256_without_object_field(&document, "selectionDocumentSha256")?
                == string(document_map, "selectionDocumentSha256")?
            && document_map.get("generationIndex").and_then(Value::as_u64) == Some(generation)
            && document_map.get("campaignRole").and_then(Value::as_str) == Some(role)
            && document_map.get("panelId").and_then(Value::as_str) == Some(panel)
            && document_map
                .get("rotatingEvidenceSha256")
                .and_then(Value::as_str)
                == Some(rotating_sha),
        "v2 task-selection document binding drifted"
    );
    if let Some(round) = round {
        ensure!(
            document_map.get("roundIndex").and_then(Value::as_u64) == Some(round),
            "v2 task-selection round binding drifted"
        );
    }
    for key in [
        "prefinalizerResultSha256",
        "taskPlanSha256",
        "taskSha256",
        "semanticAuthoritySha256",
        "rotatingEvidenceSha256",
        "candidateSetSha256",
    ] {
        require_sha(string(document_map, key)?, key)?;
    }
    validate_v2_source_authority(document_map.get("sourceAuthority").unwrap(), role)?;
    let root = path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| anyhow!("v2 task-selection document root is invalid"))?;
    let receipt_relative = Path::new(string(document_map, "selectionReceiptRelativePath")?);
    ensure!(
        receipt_relative
            .components()
            .all(|part| matches!(part, Component::Normal(_))),
        "v2 task-selection receipt path is unsafe"
    );
    let receipt_path = root.join(receipt_relative);
    ensure!(
        receipt_path.is_file()
            && !fs::symlink_metadata(&receipt_path)?
                .file_type()
                .is_symlink(),
        "v2 task-selection receipt is missing or symlinked"
    );
    let receipt_raw = fs::read(&receipt_path)?;
    let receipt: Value = serde_json::from_slice(&receipt_raw)?;
    ensure!(
        canonical_json_line(&receipt)? == receipt_raw,
        "v2 task-selection receipt is not canonical"
    );
    let receipt_map = object(&receipt, "v2 task-selection receipt")?;
    exact_keys(
        receipt_map,
        &[
            "schemaVersion",
            "selectionDocumentSha256",
            "prefinalizerResultSha256",
            "taskPlanSha256",
            "taskSha256",
            "semanticAuthoritySha256",
            "generationIndex",
            "roundIndex",
            "campaignRole",
            "panelId",
            "rotatingEvidenceSha256",
            "candidateSetSha256",
            "candidateRowsSha256",
            "receiptSha256",
        ],
        "v2 task-selection receipt",
    )?;
    ensure!(
        string(receipt_map, "schemaVersion")? == NON_PROPOSAL_TASK_SELECTION_RECEIPT_SCHEMA
            && canonical_sha256_without_object_field(&receipt, "receiptSha256")?
                == string(receipt_map, "receiptSha256")?,
        "v2 task-selection receipt identity drifted"
    );
    for key in [
        "selectionDocumentSha256",
        "prefinalizerResultSha256",
        "taskPlanSha256",
        "taskSha256",
        "semanticAuthoritySha256",
        "rotatingEvidenceSha256",
        "candidateSetSha256",
    ] {
        ensure!(
            receipt_map.get(key) == document_map.get(key),
            "v2 task-selection receipt/document {key} drifted"
        );
    }
    for key in ["generationIndex", "roundIndex", "campaignRole", "panelId"] {
        ensure!(
            receipt_map.get(key) == document_map.get(key),
            "v2 task-selection receipt/document {key} drifted"
        );
    }
    ensure!(
        receipt_map.get("candidateRowsSha256")
            == document_map
                .get("candidateRows")
                .and_then(Value::as_object)
                .and_then(|m| m.get("descriptorSha256")),
        "v2 task-selection receipt descriptor drifted"
    );
    let local_rows = load_v2_rich_candidate_rows(
        root,
        document_map.get("candidateRows").unwrap(),
        string(document_map, "candidateSetSha256")?,
    )?;
    let authenticated_rows =
        temporal_qd_rotating_prefinalizer::v5::validate_v2_task_selection_handoff(path)?;
    ensure!(
        local_rows == authenticated_rows,
        "v2 task-selection rows differ from the authenticated prefinalizer handoff"
    );
    validate_v2_source_authority_rows(
        document_map.get("sourceAuthority").unwrap(),
        role,
        &local_rows,
    )?;
    Ok(local_rows)
}

fn validate_v2_source_authority(value: &Value, role: &str) -> Result<()> {
    let source = object(value, "v2 task-selection source authority")?;
    match role {
        "retained_parent_current_panel" => {
            exact_keys(
                source,
                &[
                    "schemaVersion",
                    "previousParentArchiveSha256",
                    "candidateMemberProof",
                ],
                "retained parent source authority",
            )?;
            ensure!(
                string(source, "schemaVersion")?
                    == "temporal_qd_v5_retained_parent_archive_member_proof_v1",
                "retained parent source authority schema is invalid"
            );
            require_sha(
                string(source, "previousParentArchiveSha256")?,
                "previous parent archive sha",
            )?;
            ensure!(
                !array(source, "candidateMemberProof")?.is_empty(),
                "retained parent source proof is empty"
            );
        }
        "prior_panel_backfill" => {
            exact_keys(
                source,
                &[
                    "schemaVersion",
                    "cohortSha256",
                    "provisionalSha256",
                    "admittedCampaignLedgerSha256",
                    "priorResultSha256",
                    "priorReceiptProof",
                    "selectedCandidateProof",
                ],
                "prior-panel backfill source authority",
            )?;
            ensure!(
                string(source, "schemaVersion")?
                    == "temporal_qd_v5_prior_panel_backfill_source_authority_v1",
                "prior-panel backfill source authority schema is invalid"
            );
            for key in [
                "cohortSha256",
                "provisionalSha256",
                "admittedCampaignLedgerSha256",
            ] {
                require_sha(string(source, key)?, key)?;
            }
            if !source.get("priorResultSha256").is_some_and(Value::is_null) {
                require_sha(
                    source
                        .get("priorResultSha256")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("prior result sha is invalid"))?,
                    "prior result sha",
                )?;
            }
            ensure!(
                !array(source, "priorReceiptProof")?.is_empty(),
                "prior-panel receipt proof is empty"
            );
        }
        _ => return Err(anyhow!("v2 task-selection role is invalid")),
    }
    Ok(())
}

fn validate_v2_source_authority_rows(value: &Value, role: &str, rows: &[Value]) -> Result<()> {
    let source = object(value, "v2 selected-candidate source authority")?;
    let proof_key = match role {
        "retained_parent_current_panel" => "candidateMemberProof",
        "prior_panel_backfill" => "selectedCandidateProof",
        _ => return Err(anyhow!("v2 selected-candidate source role is invalid")),
    };
    let mut proofs = BTreeMap::new();
    for proof in array(source, proof_key)? {
        let proof = object(proof, "v2 selected-candidate proof")?;
        exact_keys(
            proof,
            &[
                "candidateId",
                "candidateIdentitySha256",
                "programSha256",
                "profileSnapshotSha256",
            ],
            "v2 selected-candidate proof",
        )?;
        let id = string(proof, "candidateId")?.to_owned();
        require_sha(
            string(proof, "candidateIdentitySha256")?,
            "retained parent candidate identity",
        )?;
        require_sha(string(proof, "programSha256")?, "retained parent program")?;
        require_sha(
            string(proof, "profileSnapshotSha256")?,
            "retained parent profile snapshot",
        )?;
        ensure!(
            proofs.insert(id, proof).is_none(),
            "v2 selected-candidate proof has duplicate candidate ID"
        );
    }
    ensure!(
        proofs.len() == rows.len(),
        "v2 selected-candidate proof cardinality drifted"
    );
    for row in rows {
        let row = object(row, "v2 selected row")?;
        let proof = proofs
            .remove(string(row, "candidateId")?)
            .ok_or_else(|| anyhow!("selected row lacks v2 source proof"))?;
        for key in [
            "candidateIdentitySha256",
            "programSha256",
            "profileSnapshotSha256",
        ] {
            ensure!(
                proof.get(key) == row.get(key),
                "v2 selected-candidate source proof {key} drifted"
            );
        }
    }
    Ok(())
}

fn load_v2_rich_candidate_rows(
    root: &Path,
    descriptor: &Value,
    candidate_set_sha: &str,
) -> Result<Vec<Value>> {
    let descriptor = object(descriptor, "v2 task candidate descriptor")?;
    exact_keys(
        descriptor,
        &[
            "schemaVersion",
            "path",
            "rawSha256",
            "sizeBytes",
            "recordCount",
            "rowSchema",
            "candidateSetSha256",
            "inputAuthoritySha256",
            "descriptorSha256",
        ],
        "v2 task candidate descriptor",
    )?;
    ensure!(
        string(descriptor, "schemaVersion")?
            == "temporal_qd_v5_native_rich_candidate_jsonl_descriptor_v1"
            && string(descriptor, "rowSchema")? == "temporal_qd_selected_rich_candidate_v1"
            && canonical_sha256_without_object_field(
                &Value::Object(descriptor.clone()),
                "descriptorSha256"
            )? == string(descriptor, "descriptorSha256")?
            && string(descriptor, "candidateSetSha256")? == candidate_set_sha,
        "v2 task candidate descriptor drifted"
    );
    let relative = Path::new(string(descriptor, "path")?);
    ensure!(
        relative
            .components()
            .all(|part| matches!(part, Component::Normal(_))),
        "v2 task candidate path is unsafe"
    );
    let path = root.join(relative);
    ensure!(
        path.is_file() && !fs::symlink_metadata(&path)?.file_type().is_symlink(),
        "v2 task candidate sidecar is missing or symlinked"
    );
    let raw = fs::read(&path)?;
    ensure!(
        raw.len() as u64
            == descriptor
                .get("sizeBytes")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("v2 task candidate size is invalid"))?
            && file_sha256(&path)? == string(descriptor, "rawSha256")?,
        "v2 task candidate sidecar binding drifted"
    );
    let mut rows = Vec::new();
    let mut ids = BTreeSet::new();
    for line in raw.split_inclusive(|byte| *byte == b'\n') {
        ensure!(
            line.ends_with(b"\n") && line.len() > 1,
            "v2 task candidate JSONL row is invalid"
        );
        let row: Value = serde_json::from_slice(&line[..line.len() - 1])?;
        ensure!(
            canonical_json_line(&row)? == line,
            "v2 task candidate JSONL row is noncanonical"
        );
        let map = object(&row, "v2 task candidate")?;
        ensure!(
            !string(map, "candidateId")?.is_empty(),
            "v2 task candidate ID is empty"
        );
        for key in ["candidateIdentitySha256", "programSha256"] {
            require_sha(string(map, key)?, key)?;
        }
        if let Some(profile) = map.get("sourceProfileSha256").and_then(Value::as_str) {
            require_sha(profile, "source profile sha")?;
        } else {
            require_sha(
                string(map, "profileSnapshotSha256")?,
                "profile snapshot sha",
            )?;
        }
        ensure!(
            ids.insert(string(map, "candidateId")?.to_owned()),
            "v2 task candidate IDs duplicate"
        );
        rows.push(row);
    }
    ensure!(
        rows.len() as u64
            == descriptor
                .get("recordCount")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("v2 task candidate count is invalid"))?,
        "v2 task candidate count drifted"
    );
    let set = canonical_sha256(&Value::Array(rows.iter().map(|row| json!({"candidateId":row["candidateId"],"candidateIdentitySha256":row["candidateIdentitySha256"]})).collect()))?;
    ensure!(set == candidate_set_sha, "v2 task candidate set drifted");
    Ok(rows)
}

/// Direct G1 proposal-current-panel cohort derivation from the sealed native
/// evaluation population.  It replaces the historical Python candidate-list
/// loop and produces the legacy rotating cohort identity exactly.
pub fn derive_g1_proposal_cohort(
    input: &Value,
    generation_index: u64,
    panel_id: &str,
    rotating_evidence_sha256: &str,
) -> Result<Value> {
    let map = object(input, "native evaluation population")?;
    ensure!(
        map.get("schemaVersion").and_then(Value::as_str)
            == Some("temporal_qd_evaluation_population_v1"),
        "native evaluation population schema is incompatible"
    );
    ensure!(
        map.get("generationIndex").and_then(Value::as_u64) == Some(generation_index),
        "native evaluation population generation binding drifted"
    );
    ensure!(
        canonical_sha256_without_object_field(input, "evaluationPopulationSha256")?
            == string(map, "evaluationPopulationSha256")?,
        "native evaluation population identity drifted"
    );
    let mut candidates = array(map, "candidates")?.to_vec();
    ensure!(
        !candidates.is_empty(),
        "native evaluation population has no candidates"
    );
    candidates.sort_by(|left, right| {
        left.get("candidateId")
            .and_then(Value::as_str)
            .cmp(&right.get("candidateId").and_then(Value::as_str))
    });
    let mut seen = BTreeSet::new();
    for candidate in &candidates {
        let row = object(candidate, "native evaluation candidate")?;
        let id = string(row, "candidateId")?;
        ensure!(
            seen.insert(id.to_owned()),
            "native evaluation population has duplicate candidate IDs"
        );
        for key in [
            "candidateIdentitySha256",
            "programSha256",
            "sourceProfileSha256",
        ] {
            require_sha(string(row, key)?, key)?;
        }
        let profile = row
            .get("sourceProfile")
            .ok_or_else(|| anyhow!("native evaluation candidate lacks profile"))?;
        ensure!(
            canonical_sha256(profile)? == string(row, "sourceProfileSha256")?,
            "native evaluation candidate source profile identity drifted"
        );
    }
    let mut cohort = json!({"schemaVersion":ROTATING_COHORT_POPULATION_SCHEMA,"generationIndex":generation_index,"panelId":panel_id,"cohortRole":"proposal_current_panel","rotatingEvidenceSha256":rotating_evidence_sha256,"candidateCount":candidates.len(),"candidates":candidates,"proposalPopulation":true});
    let sha = canonical_sha256(&cohort)?;
    cohort
        .as_object_mut()
        .expect("cohort object")
        .insert("populationSha256".to_owned(), Value::String(sha));
    Ok(cohort)
}

/// Rotate only profile-bound fields while retaining the attested lake binding.
/// The full catalog-hydration pass is intentionally upstream of this small
/// primitive; this gate still rejects a changed pair/base timeframe before any
/// plan identity is minted.
pub fn rotate_evidence_plan_native(
    template: &Value,
    source_profile: &Value,
    source_profile_sha256: &str,
    base_timeframe: &str,
) -> Result<Value> {
    require_sha(source_profile_sha256, "source profile sha")?;
    let mut plan = template.clone();
    let map = object(&plan, "evidence plan template")?;
    ensure!(
        map.get("schema_version").and_then(Value::as_str)
            == Some("fuzzfolio.replay-evidence-plan.v2"),
        "evidence-plan rotation requires replay evidence plan v2"
    );
    ensure!(
        canonical_sha256(source_profile)? == source_profile_sha256,
        "evidence-plan source profile identity drifted"
    );
    let profile = object(source_profile, "evidence-plan source profile")?;
    let instruments = array(profile, "instruments")?;
    ensure!(
        instruments.len() == 1,
        "evidence-plan source profile requires exactly one instrument"
    );
    let binding = object(
        map.get("lake_window_binding")
            .ok_or_else(|| anyhow!("evidence plan lacks lake binding"))?,
        "evidence plan lake binding",
    )?;
    let request = object(
        binding
            .get("request")
            .ok_or_else(|| anyhow!("evidence plan lake request missing"))?,
        "evidence plan lake request",
    )?;
    ensure!(
        array(request, "pairs")?
            .iter()
            .any(|pair| pair == &instruments[0]),
        "candidate-derived lake scope is outside immutable pair binding"
    );
    let tf = base_timeframe.trim().to_ascii_uppercase();
    ensure!(
        !tf.is_empty()
            && array(request, "timeframes")?
                .iter()
                .any(|value| value.as_str() == Some(&tf)),
        "candidate-derived lake scope is outside immutable timeframe binding"
    );
    let execution = object(
        profile
            .get("executionConfig")
            .ok_or_else(|| anyhow!("source profile lacks execution config"))?,
        "source profile execution config",
    )?;
    let plan_map = plan.as_object_mut().expect("plan object");
    // The Python oracle opens the lake binding through its typed contract.
    // Materialized v5 templates already carry these fields, but normalizing
    // the closed defaults here preserves byte parity for the small oracle
    // fixtures as well and avoids a hidden Python/Pydantic dependency.
    let binding = plan_map
        .get_mut("lake_window_binding")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("evidence plan lake binding is not an object"))?;
    binding
        .entry("schema_version".to_owned())
        .or_insert_with(|| Value::String("fuzzfolio.market-data-window-binding.v1".to_owned()));
    binding
        .entry("semantic_contract_id".to_owned())
        .or_insert_with(|| Value::String("fuzzfolio.canonical-bars.semantic-digest.v2".to_owned()));
    for key in [
        "attestation_sha256",
        "creation_global_coverage_sha256",
        "creation_source_coverage_sha256",
        "legacy_selection_manifest_sha256",
    ] {
        binding.entry(key.to_owned()).or_insert(Value::Null);
    }
    let request = binding
        .get_mut("request")
        .and_then(Value::as_object_mut)
        .ok_or_else(|| anyhow!("evidence plan lake request is not an object"))?;
    request
        .entry("schema_version".to_owned())
        .or_insert_with(|| Value::String("fuzzfolio.market-data-window-request.v1".to_owned()));
    request
        .entry("dataset".to_owned())
        .or_insert_with(|| Value::String("bars".to_owned()));
    request
        .entry("coverage_policy".to_owned())
        .or_insert_with(|| Value::String("require_complete".to_owned()));
    plan_map.insert(
        "profile_snapshot_sha256".to_owned(),
        Value::String(source_profile_sha256.to_owned()),
    );
    let cell = if execution.contains_key("managementLibrary") {
        Value::Null
    } else {
        let exit = object(
            execution
                .get("exitPolicy")
                .ok_or_else(|| anyhow!("source profile lacks exit policy"))?,
            "source profile exit policy",
        )?;
        canonical_sha256(
            exit.get("selectedCell")
                .ok_or_else(|| anyhow!("source profile lacks selected cell"))?,
        )?
        .into()
    };
    plan_map.insert("execution_cell_sha256".to_owned(), cell);
    plan_map.remove("plan_id");
    plan_map.remove("lake_manifest_sha256");
    let identity = canonical_sha256(&plan)?;
    plan.as_object_mut()
        .expect("plan object")
        .insert("plan_id".to_owned(), Value::String(identity));
    Ok(plan)
}

/// Catalog-backed dependency check used before rotating a v5 plan.  It refuses
/// abbreviated active indicators: their catalog default timeframe/lookback and
/// required padding must be present in the pre-attested lake request.
pub fn validate_catalog_lake_containment(
    profile: &Value,
    catalog: &Value,
    binding: &Value,
    base_timeframe: &str,
) -> Result<()> {
    let profile = object(profile, "catalog lake profile")?;
    let catalog = object(catalog, "frozen construction catalog")?;
    let request = object(
        object(binding, "lake binding")?
            .get("request")
            .ok_or_else(|| anyhow!("lake binding lacks request"))?,
        "lake request",
    )?;
    let request_tfs = array(request, "timeframes")?;
    ensure!(
        request_tfs
            .iter()
            .any(|value| value.as_str() == Some(&base_timeframe.trim().to_ascii_uppercase())),
        "lake binding lacks base timeframe"
    );
    let catalog_rows = array(catalog, "indicators")?;
    for raw in profile
        .get("indicators")
        .and_then(Value::as_array)
        .unwrap_or(&Vec::new())
    {
        let raw = object(raw, "profile indicator")?;
        let meta = object(
            raw.get("meta")
                .ok_or_else(|| anyhow!("profile indicator lacks meta"))?,
            "profile indicator meta",
        )?;
        let id = string(meta, "id")?;
        let catalog_row = catalog_rows
            .iter()
            .find(|row| {
                row.get("meta")
                    .and_then(Value::as_object)
                    .and_then(|meta| meta.get("id"))
                    .and_then(Value::as_str)
                    == Some(id)
            })
            .ok_or_else(|| anyhow!("profile indicator absent from frozen catalog"))?;
        let config = object(
            catalog_row
                .get("config")
                .ok_or_else(|| anyhow!("catalog indicator lacks config"))?,
            "catalog indicator config",
        )?;
        let authored = object(
            raw.get("config")
                .ok_or_else(|| anyhow!("profile indicator lacks config"))?,
            "profile indicator config",
        )?;
        let active = authored.get("isActive").and_then(Value::as_bool).unwrap_or(
            config
                .get("isActive")
                .and_then(Value::as_bool)
                .unwrap_or(true),
        );
        if !active {
            continue;
        }
        let timeframe = authored
            .get("timeframe")
            .or_else(|| config.get("timeframe"))
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("catalog-hydrated indicator lacks timeframe"))?
            .trim()
            .to_ascii_uppercase();
        ensure!(
            request_tfs
                .iter()
                .any(|value| value.as_str() == Some(&timeframe)),
            "candidate-derived lake scope is outside immutable timeframe binding"
        );
        ensure!(
            object(
                catalog_row
                    .get("meta")
                    .ok_or_else(|| anyhow!("catalog indicator lacks meta"))?,
                "catalog indicator meta"
            )?
            .contains_key("requiredPaddingBars"),
            "catalog indicator lacks required padding"
        );
    }
    Ok(())
}

/// Certify two externally-attested, continuous research windows and assemble
/// the evidence-ladder authority.  Lake attestation is deliberately not
/// performed here: it is an external operation and cannot be reconstructed
/// from sealed rotating artifacts.  Rust owns all admission, derivation,
/// write-once publication, and receipt-last restart validation.
fn execute_v5_ladder_materialization_manifest(spec: &Map<String, Value>) -> Result<Value> {
    exact_keys(
        spec,
        &[
            "schemaVersion",
            "rotatingEvidenceContract",
            "rotatingEvidenceMaterialization",
            "sourceFinalizerAuthority",
            "panelTemplatePreparation",
            "constructionCatalog",
            "stageTemplatePreparations",
            "workerContractSha256",
            "executionEngineCommit",
            "archivePolicyAuthority",
            "behaviorAttributionRequirement",
            "outputRoot",
            "manifestSha256",
        ],
        "v2 ladder materialization manifest",
    )?;
    let manifest_sha =
        canonical_sha256_without_object_field(&Value::Object(spec.clone()), "manifestSha256")?;
    ensure!(
        string(spec, "schemaVersion")? == V5_LADDER_MATERIALIZATION_MANIFEST_SCHEMA
            && string(spec, "manifestSha256")? == manifest_sha,
        "v2 ladder materialization manifest identity drifted"
    );
    let root = PathBuf::from(string(spec, "outputRoot")?);
    fs::create_dir_all(&root).context("create v2 ladder materialization root")?;
    if root.join("materialization-receipt.json").exists() {
        return reopen_v5_ladder_materialization(&root, &manifest_sha);
    }

    let contract_desc = self_hashed_descriptor(
        spec.get("rotatingEvidenceContract")
            .ok_or_else(|| anyhow!("rotating evidence contract descriptor missing"))?,
        "rotatingEvidenceSha256",
        "rotating evidence contract",
    )?;
    validate_v5_rotating_contract(&contract_desc.value)?;
    let rotating_sha = string(
        object(&contract_desc.value, "rotating evidence contract")?,
        "rotatingEvidenceSha256",
    )?;

    let materialization_desc = self_hashed_descriptor(
        spec.get("rotatingEvidenceMaterialization")
            .ok_or_else(|| anyhow!("rotating evidence materialization descriptor missing"))?,
        "materializationSha256",
        "rotating evidence materialization",
    )?;
    validate_rotating_materialization(
        &materialization_desc.value,
        &contract_desc.value,
        rotating_sha,
    )?;

    let finalizer = reopen_ladder_archive_authority(
        spec.get("sourceFinalizerAuthority")
            .ok_or_else(|| anyhow!("source finalizer authority missing"))?,
    )?;
    ensure!(
        finalizer.kind == "generation_finalizer_commit",
        "v2 ladder materialization source is not a generation finalizer"
    );
    let generation = object(&finalizer.archive, "source final archive")?
        .get("generationIndex")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("source final archive generation is invalid"))?;
    ensure!(generation > 0, "source final generation must be positive");
    let panel_id = rotating_panel_for_generation(&contract_desc.value, generation)?;

    let panel_desc = preparation_descriptor(
        spec.get("panelTemplatePreparation")
            .ok_or_else(|| anyhow!("panel-template preparation descriptor missing"))?,
        "panel-template preparation",
    )?;
    validate_authorized_panel_template(
        &panel_desc,
        &contract_desc.value,
        &materialization_desc.value,
        panel_id,
    )?;

    let catalog_desc = bound_descriptor(
        spec.get("constructionCatalog")
            .ok_or_else(|| anyhow!("construction catalog descriptor missing"))?,
        "catalogSha256",
        "construction catalog",
    )?;
    let materialization_map = object(
        &materialization_desc.value,
        "rotating evidence materialization",
    )?;
    let materialization_catalog = object(
        materialization_map
            .get("constructionCatalog")
            .ok_or_else(|| anyhow!("rotating materialization lacks construction catalog"))?,
        "rotating materialization construction catalog",
    )?;
    ensure!(
        materialization_catalog.get("catalogSha256")
            == Some(&Value::String(catalog_desc.semantic_sha.clone())),
        "construction catalog substitution rejected"
    );
    let capability = materialization_map
        .get("catalogCapabilityEnvelope")
        .ok_or_else(|| anyhow!("catalog capability envelope missing"))?;
    let capability_map = object(capability, "catalog capability envelope")?;
    ensure!(
        canonical_sha256_without_object_field(capability, "capabilityEnvelopeSha256")?
            == string(capability_map, "capabilityEnvelopeSha256")?,
        "catalog capability envelope identity drifted"
    );

    let worker_sha = string(spec, "workerContractSha256")?;
    require_sha(worker_sha, "ladder worker contract")?;
    ensure!(
        object(
            materialization_map
                .get("workerContract")
                .ok_or_else(|| anyhow!("rotating materialization lacks worker contract"))?,
            "rotating materialization worker contract",
        )?
        .get("workerContractSha256")
            == Some(&Value::String(worker_sha.to_owned())),
        "ladder worker contract substitution rejected"
    );
    let commit = string(spec, "executionEngineCommit")?;
    ensure!(
        commit.len() == 40 && commit.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "ladder execution-engine commit is invalid"
    );
    validate_v5_archive_authority(
        spec.get("archivePolicyAuthority")
            .ok_or_else(|| anyhow!("ladder archive policy authority missing"))?,
    )?;
    validate_v5_behavior_requirement(
        spec.get("behaviorAttributionRequirement")
            .ok_or_else(|| anyhow!("ladder behavior requirement missing"))?,
    )?;

    let stages_desc = object(
        spec.get("stageTemplatePreparations")
            .ok_or_else(|| anyhow!("stage template preparations missing"))?,
        "stage template preparations",
    )?;
    exact_keys(
        stages_desc,
        &["validation", "scrutiny"],
        "stage template preparations",
    )?;
    let exemplar = array(
        object(&panel_desc.value, "authorized panel template")?,
        "candidates",
    )?
    .first()
    .ok_or_else(|| anyhow!("authorized panel template has no exemplar"))?;
    let mut certified = BTreeMap::<String, PreparationDescriptor>::new();
    for (stage, months, role) in [
        ("validation", 12_u64, "validation"),
        ("scrutiny", 36_u64, "scrutiny"),
    ] {
        let descriptor = preparation_descriptor(
            stages_desc
                .get(stage)
                .ok_or_else(|| anyhow!("{stage} template descriptor missing"))?,
            &format!("{stage} template preparation"),
        )?;
        let window = rotating_scrutiny_window(&contract_desc.value, stage, months)?;
        certify_ladder_stage_template(
            &descriptor,
            exemplar,
            stage,
            role,
            months,
            &window,
            worker_sha,
            capability,
            &materialization_desc.value,
        )?;
        certified.insert(stage.to_owned(), descriptor);
    }

    let output_paths = BTreeMap::from([
        (
            "validation".to_owned(),
            root.join("validation-12m-template-preparation.json"),
        ),
        (
            "scrutiny".to_owned(),
            root.join("scrutiny-36m-template-preparation.json"),
        ),
    ]);
    for stage in ["validation", "scrutiny"] {
        let descriptor = &certified[stage];
        write_once_pretty(&output_paths[stage], &descriptor.value)?;
        write_once_pretty(
            &root.join(format!("{stage}-authority.json")),
            &descriptor.authority,
        )?;
    }

    let outer_tail = object(&contract_desc.value, "rotating evidence contract")?
        .get("outerTail")
        .cloned()
        .ok_or_else(|| anyhow!("rotating evidence outer tail missing"))?;
    let catalog_path = PathBuf::from(string(
        object(
            spec.get("constructionCatalog")
                .ok_or_else(|| anyhow!("construction catalog descriptor missing"))?,
            "construction catalog descriptor",
        )?,
        "path",
    )?);
    let mut stages = Map::new();
    for (stage, months, evidence_role, limit, source_kind, campaign_role) in [
        (
            "validation",
            12_u64,
            "validation",
            128_u64,
            "generation_finalizer_commit",
            "evidence_ladder_validation",
        ),
        (
            "scrutiny",
            36_u64,
            "scrutiny",
            32_u64,
            "qd_archive_reducer_result",
            "evidence_ladder_scrutiny",
        ),
    ] {
        let descriptor = &certified[stage];
        stages.insert(
            stage.to_owned(),
            json!({
                "stage":stage,
                "sourceArchiveAuthorityKind":source_kind,
                "campaignRole":campaign_role,
                "window":rotating_scrutiny_window(&contract_desc.value,stage,months)?,
                "requestedHorizonMonths":months,
                "evidenceRole":evidence_role,
                "candidateLimit":limit,
                "templatePreparationPath":output_paths[stage].to_string_lossy(),
                "templatePreparationSha256":descriptor.semantic_sha,
                "templateAuthorityId":descriptor.authority_id,
                "constructionCatalogPath":catalog_path.to_string_lossy(),
                "constructionCatalogSha256":catalog_desc.semantic_sha,
                "archivePolicyAuthority":spec.get("archivePolicyAuthority").cloned().unwrap_or(Value::Null),
                "behaviorAttributionRequirement":spec.get("behaviorAttributionRequirement").cloned().unwrap_or(Value::Null),
            }),
        );
    }
    let mut ladder = json!({
        "schemaVersion":V5_LADDER_AUTHORITY_SCHEMA,
        "rotatingEvidenceSha256":rotating_sha,
        "sourceGenerationIndex":generation,
        "panelId":panel_id,
        "stageOrder":["validation","scrutiny"],
        "outerTail":outer_tail,
        "workerContractSha256":worker_sha,
        "executionEngineCommit":commit,
        "stages":stages,
    });
    ladder["ladderAuthoritySha256"] = Value::String(canonical_sha256(&ladder)?);
    validate_v2_ladder_authority(&ladder)?;
    write_once_pretty(&root.join("ladder-authority.json"), &ladder)?;

    let result = json!({
        "schemaVersion":V5_LADDER_MATERIALIZATION_RESULT_SCHEMA,
        "manifestSha256":manifest_sha,
        "rotatingEvidenceSha256":rotating_sha,
        "sourceGenerationIndex":generation,
        "panelId":panel_id,
        "ladderAuthoritySha256":ladder["ladderAuthoritySha256"],
        "validationTemplatePreparationSha256":certified["validation"].semantic_sha,
        "validationTemplateAuthorityId":certified["validation"].authority_id,
        "scrutinyTemplatePreparationSha256":certified["scrutiny"].semantic_sha,
        "scrutinyTemplateAuthorityId":certified["scrutiny"].authority_id,
        "outputRoot":root.to_string_lossy(),
    });
    write_once_pretty(&root.join("materialization-result.json"), &result)?;
    write_ladder_materialization_commit(
        &root,
        &manifest_sha,
        rotating_sha,
        generation,
        panel_id,
        &finalizer.receipt_sha256,
        &ladder,
        &certified,
    )?;
    Ok(result)
}

#[derive(Clone)]
struct BoundDescriptor {
    value: Value,
    semantic_sha: String,
}

#[derive(Clone)]
struct PreparationDescriptor {
    value: Value,
    semantic_sha: String,
    authority: Value,
    authority_id: String,
}

fn bound_descriptor(value: &Value, sha_field: &str, name: &str) -> Result<BoundDescriptor> {
    let descriptor = object(value, &format!("{name} descriptor"))?;
    exact_keys(
        descriptor,
        &["path", sha_field],
        &format!("{name} descriptor"),
    )?;
    let expected = string(descriptor, sha_field)?.to_owned();
    let path = PathBuf::from(string(descriptor, "path")?);
    require_regular_path_without_symlink_ancestors(&path, name)?;
    let document = read_bound_historical_pretty_semantic_json(&path, &expected, name)?;
    Ok(BoundDescriptor {
        value: document,
        semantic_sha: expected,
    })
}

fn self_hashed_descriptor(value: &Value, sha_field: &str, name: &str) -> Result<BoundDescriptor> {
    let descriptor = object(value, &format!("{name} descriptor"))?;
    exact_keys(
        descriptor,
        &["path", sha_field],
        &format!("{name} descriptor"),
    )?;
    let expected = string(descriptor, sha_field)?.to_owned();
    require_sha(&expected, name)?;
    let path = PathBuf::from(string(descriptor, "path")?);
    require_regular_path_without_symlink_ancestors(&path, name)?;
    let document = read_pretty_json(&path, name)?;
    let document_map = object(&document, name)?;
    ensure!(
        document_map.get(sha_field) == Some(&Value::String(expected.clone()))
            && canonical_sha256_without_object_field(&document, sha_field)? == expected,
        "{name} semantic identity drifted"
    );
    Ok(BoundDescriptor {
        value: document,
        semantic_sha: expected,
    })
}

fn preparation_descriptor(value: &Value, name: &str) -> Result<PreparationDescriptor> {
    let descriptor = object(value, &format!("{name} descriptor"))?;
    exact_keys(
        descriptor,
        &["path", "preparationSha256", "authorityId"],
        &format!("{name} descriptor"),
    )?;
    let expected = string(descriptor, "preparationSha256")?.to_owned();
    let expected_authority = string(descriptor, "authorityId")?.to_owned();
    require_sha(&expected_authority, &format!("{name} authority"))?;
    let path = PathBuf::from(string(descriptor, "path")?);
    require_regular_path_without_symlink_ancestors(&path, name)?;
    let document = read_bound_historical_pretty_semantic_json(&path, &expected, name)?;
    let authority = build_native_authority(&document)?;
    let authority_id = string(object(&authority, name)?, "authorityId")?.to_owned();
    ensure!(
        authority_id == expected_authority,
        "{name} authority substitution rejected"
    );
    Ok(PreparationDescriptor {
        value: document,
        semantic_sha: expected,
        authority,
        authority_id,
    })
}

fn validate_v5_rotating_contract(value: &Value) -> Result<()> {
    let map = object(value, "rotating evidence contract")?;
    ensure!(
        string(map, "schemaVersion")? == "temporal_qd_rotating_evidence_v1",
        "rotating evidence contract schema is incompatible"
    );
    let supplied = string(map, "rotatingEvidenceSha256")?;
    ensure!(
        canonical_sha256_without_object_field(value, "rotatingEvidenceSha256")? == supplied,
        "rotating evidence contract identity drifted"
    );
    let mapping = object(
        map.get("absoluteGenerationMapping")
            .ok_or_else(|| anyhow!("rotating generation mapping missing"))?,
        "rotating generation mapping",
    )?;
    ensure!(
        mapping.get("firstGenerationIndex").and_then(Value::as_u64) == Some(1)
            && mapping.get("mapping").and_then(Value::as_str) == Some("one_based_modulo_cycle")
            && mapping.get("cycleLength").and_then(Value::as_u64)
                == Some(array(map, "panels")?.len() as u64),
        "rotating generation mapping is incompatible"
    );
    Ok(())
}

fn validate_rotating_materialization(
    value: &Value,
    contract: &Value,
    rotating_sha: &str,
) -> Result<()> {
    let map = object(value, "rotating evidence materialization")?;
    ensure!(
        string(map, "schemaVersion")? == "temporal_qd_rotating_evidence_materialization_v1"
            && canonical_sha256_without_object_field(value, "materializationSha256")?
                == string(map, "materializationSha256")?
            && map.get("rotatingEvidence") == Some(contract)
            && string(map, "curriculumSha256")? == rotating_sha
            && map
                .get("remoteAttestationRequired")
                .and_then(Value::as_bool)
                == Some(true),
        "rotating evidence materialization identity/contract drifted"
    );
    Ok(())
}

fn rotating_panel_for_generation(contract: &Value, generation: u64) -> Result<&str> {
    let map = object(contract, "rotating evidence contract")?;
    let panels = array(map, "panels")?;
    let cycle = object(
        map.get("absoluteGenerationMapping")
            .ok_or_else(|| anyhow!("rotating mapping missing"))?,
        "rotating mapping",
    )?
    .get("cycleLength")
    .and_then(Value::as_u64)
    .ok_or_else(|| anyhow!("rotating cycle missing"))?;
    string(
        object(
            &panels[((generation - 1) % cycle) as usize],
            "rotating panel",
        )?,
        "panelId",
    )
}

fn validate_authorized_panel_template(
    descriptor: &PreparationDescriptor,
    contract: &Value,
    materialization: &Value,
    panel_id: &str,
) -> Result<()> {
    let contract_template = object(
        object(contract, "rotating evidence contract")?
            .get("panelTemplates")
            .ok_or_else(|| anyhow!("rotating contract lacks panel templates"))?,
        "rotating panel templates",
    )?
    .get(panel_id)
    .ok_or_else(|| anyhow!("rotating contract lacks mapped panel template"))?;
    let contract_template = object(contract_template, "mapped panel template")?;
    ensure!(
        contract_template.get("preparationSha256")
            == Some(&Value::String(descriptor.semantic_sha.clone()))
            && contract_template.get("authorityId")
                == Some(&Value::String(descriptor.authority_id.clone())),
        "panel-template substitution rejected"
    );
    let materialized_template = object(
        object(materialization, "rotating evidence materialization")?
            .get("templates")
            .ok_or_else(|| anyhow!("rotating materialization lacks templates"))?,
        "rotating materialization templates",
    )?
    .get(panel_id)
    .ok_or_else(|| anyhow!("rotating materialization lacks mapped panel template"))?;
    let materialized_template = object(materialized_template, "materialized panel template")?;
    ensure!(
        materialized_template.get("preparationSha256")
            == Some(&Value::String(descriptor.semantic_sha.clone()))
            && materialized_template.get("authorityId")
                == Some(&Value::String(descriptor.authority_id.clone())),
        "master-template or panel-template substitution rejected"
    );
    Ok(())
}

fn rotating_scrutiny_window(contract: &Value, stage: &str, months: u64) -> Result<Value> {
    let research = object(
        object(contract, "rotating evidence contract")?
            .get("researchScrutiny")
            .ok_or_else(|| anyhow!("rotating research scrutiny missing"))?,
        "rotating research scrutiny",
    )?;
    ensure!(
        research.get("selectionInput").and_then(Value::as_bool) == Some(false),
        "rotating research scrutiny is a selection input"
    );
    let binding = object(
        research
            .get(stage)
            .ok_or_else(|| anyhow!("rotating {stage} window missing"))?,
        &format!("rotating {stage} binding"),
    )?;
    ensure!(
        binding.get("months").and_then(Value::as_u64) == Some(months),
        "rotating {stage} requested horizon drifted"
    );
    let window = binding
        .get("window")
        .cloned()
        .ok_or_else(|| anyhow!("rotating {stage} window missing"))?;
    validate_continuous_month_window(&window, months, stage)?;
    Ok(window)
}

fn validate_continuous_month_window(window: &Value, months: u64, name: &str) -> Result<()> {
    let map = object(window, &format!("{name} window"))?;
    exact_keys(
        map,
        &["windowId", "analysisWindowStart", "analysisWindowEnd"],
        &format!("{name} window"),
    )?;
    let parse = |field: &str| -> Result<(u64, u64)> {
        let value = string(map, field)?;
        ensure!(
            value.len() == 20 && &value[4..5] == "-" && value[7..].starts_with("-01T00:00:00Z"),
            "{name} {field} is not a UTC calendar-month boundary"
        );
        let year = value[0..4].parse::<u64>()?;
        let month = value[5..7].parse::<u64>()?;
        ensure!((1..=12).contains(&month), "{name} {field} month is invalid");
        Ok((year, month))
    };
    let (start_year, start_month) = parse("analysisWindowStart")?;
    let (end_year, end_month) = parse("analysisWindowEnd")?;
    let start = start_year * 12 + start_month - 1;
    let end = end_year * 12 + end_month - 1;
    ensure!(
        end == start + months,
        "{name} window is not continuous {months}m"
    );
    Ok(())
}

fn certify_ladder_stage_template(
    descriptor: &PreparationDescriptor,
    exemplar: &Value,
    stage: &str,
    evidence_role: &str,
    months: u64,
    window: &Value,
    worker_sha: &str,
    capability: &Value,
    rotating_materialization: &Value,
) -> Result<()> {
    let map = object(&descriptor.value, &format!("{stage} template"))?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "authorityLabel",
            "workerContract",
            "candidates",
            "developmentWindows",
            "prohibitedEvidence",
            "bounds",
        ],
        &format!("{stage} template"),
    )?;
    ensure!(
        string(map, "schemaVersion")? == "temporal_graph_candidate_window_preparation_v1"
            && array(map, "developmentWindows")? == std::slice::from_ref(window),
        "{stage} template window substitution rejected"
    );
    let worker = object(
        map.get("workerContract")
            .ok_or_else(|| anyhow!("{stage} template worker contract missing"))?,
        &format!("{stage} template worker contract"),
    )?;
    ensure!(
        worker.get("workerContractSha256") == Some(&Value::String(worker_sha.to_owned())),
        "{stage} template worker contract drifted"
    );
    let candidates = array(map, "candidates")?;
    ensure!(
        candidates.len() == 1,
        "{stage} template must contain exactly one authorized exemplar"
    );
    let candidate = object(&candidates[0], &format!("{stage} exemplar"))?;
    let source = object(exemplar, "authorized panel exemplar")?;
    for field in [
        "candidateId",
        "sourceProfile",
        "sourceProfileSha256",
        "instrument",
        "timeframe",
        "barLimit",
    ] {
        ensure!(
            candidate.get(field) == source.get(field),
            "{stage} template panel-exemplar substitution rejected: {field}"
        );
    }
    let inputs = array(candidate, "windowInputs")?;
    ensure!(
        inputs.len() == 1,
        "{stage} template must have one continuous input"
    );
    let input = object(&inputs[0], &format!("{stage} window input"))?;
    ensure!(
        input.get("windowId") == object(window, "stage window")?.get("windowId"),
        "{stage} window input identity drifted"
    );
    let plan = input
        .get("evidencePlan")
        .ok_or_else(|| anyhow!("{stage} template lacks fresh lake-attested evidence plan"))?;
    let plan_map = object(plan, &format!("{stage} evidence plan"))?;
    ensure!(
        plan_map.get("schema_version").and_then(Value::as_str)
            == Some("fuzzfolio.replay-evidence-plan.v2")
            && plan_map.get("analysis_window_start")
                == object(window, "stage window")?.get("analysisWindowStart")
            && plan_map.get("analysis_window_end")
                == object(window, "stage window")?.get("analysisWindowEnd")
            && plan_map.get("selection_data_end")
                == object(window, "stage window")?.get("analysisWindowEnd")
            && plan_map.get("data_availability_cutoff")
                == object(window, "stage window")?.get("analysisWindowEnd")
            && plan_map.get("evidence_role").and_then(Value::as_str) == Some(evidence_role)
            && plan_map
                .get("requested_horizon_months")
                .and_then(Value::as_u64)
                == Some(months)
            && plan_map.get("coverage_policy").and_then(Value::as_str) == Some("require_complete"),
        "{stage} evidence plan full-window binding drifted"
    );
    let binding = object(
        plan_map
            .get("lake_window_binding")
            .ok_or_else(|| anyhow!("{stage} fresh lake attestation authority is missing"))?,
        &format!("{stage} lake binding"),
    )?;
    let attestation = string(binding, "attestation_sha256")?;
    require_sha(attestation, &format!("{stage} fresh lake attestation"))?;
    require_sha(
        string(binding, "window_semantic_sha256")?,
        &format!("{stage} lake window semantic identity"),
    )?;
    let request = object(
        binding
            .get("request")
            .ok_or_else(|| anyhow!("{stage} lake request missing"))?,
        &format!("{stage} lake request"),
    )?;
    let request_start = string(request, "data_start")?;
    validate_canonical_utc_timestamp(request_start, &format!("{stage} lake request data_start"))?;
    let analysis_start = string(object(window, "stage window")?, "analysisWindowStart")?;
    ensure!(
        request.get("data_end") == object(window, "stage window")?.get("analysisWindowEnd")
            && request_start <= analysis_start
            && request.get("coverage_policy").and_then(Value::as_str) == Some("require_complete")
            && array(request, "pairs")?
                .iter()
                .any(|pair| pair == &candidate["instrument"])
            && array(request, "timeframes")?
                .iter()
                .any(|timeframe| timeframe == &candidate["timeframe"]),
        "{stage} fresh lake request does not cover the full-window exemplar"
    );
    for timeframe in array(
        object(capability, "catalog capability envelope")?,
        "admittedTimeframes",
    )? {
        ensure!(
            array(request, "timeframes")?.contains(timeframe),
            "{stage} fresh lake request omits a catalog-capability timeframe"
        );
    }
    let quarter_attestations = array(
        object(
            rotating_materialization,
            "rotating evidence materialization",
        )?,
        "quarters",
    )?
    .iter()
    .filter_map(|quarter| {
        quarter
            .pointer("/remoteBinding/attestation_sha256")
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
    .collect::<BTreeSet<_>>();
    ensure!(
        !quarter_attestations.contains(attestation),
        "{stage} lake attestation is stale rotating-quarter evidence"
    );
    let outer = object(
        object(
            rotating_materialization,
            "rotating evidence materialization",
        )?
        .get("outerTail")
        .ok_or_else(|| anyhow!("rotating materialization outer tail missing"))?,
        "rotating materialization outer tail",
    )?;
    ensure!(
        object(window, "stage window")?
            .get("analysisWindowEnd")
            .and_then(Value::as_str)
            <= outer.get("analysisWindowStart").and_then(Value::as_str),
        "{stage} evidence touches the untouched outer tail"
    );
    Ok(())
}

fn validate_canonical_utc_timestamp(value: &str, name: &str) -> Result<()> {
    let bytes = value.as_bytes();
    ensure!(
        bytes.len() == 20
            && bytes[4] == b'-'
            && bytes[7] == b'-'
            && bytes[10] == b'T'
            && bytes[13] == b':'
            && bytes[16] == b':'
            && bytes[19] == b'Z'
            && bytes.iter().enumerate().all(|(index, byte)| {
                matches!(index, 4 | 7 | 10 | 13 | 16 | 19) || byte.is_ascii_digit()
            }),
        "{name} is not a canonical UTC timestamp"
    );
    let parse = |start: usize, end: usize| -> Result<u32> { Ok(value[start..end].parse::<u32>()?) };
    let month = parse(5, 7)?;
    let day = parse(8, 10)?;
    let hour = parse(11, 13)?;
    let minute = parse(14, 16)?;
    let second = parse(17, 19)?;
    ensure!(
        (1..=12).contains(&month)
            && (1..=31).contains(&day)
            && hour <= 23
            && minute <= 59
            && second <= 59,
        "{name} components are invalid"
    );
    Ok(())
}

fn validate_v2_ladder_authority(value: &Value) -> Result<()> {
    let map = object(value, "v2 ladder authority")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "rotatingEvidenceSha256",
            "sourceGenerationIndex",
            "panelId",
            "stageOrder",
            "outerTail",
            "workerContractSha256",
            "executionEngineCommit",
            "stages",
            "ladderAuthoritySha256",
        ],
        "v2 ladder authority",
    )?;
    ensure!(
        string(map, "schemaVersion")? == V5_LADDER_AUTHORITY_SCHEMA
            && canonical_sha256_without_object_field(value, "ladderAuthoritySha256")?
                == string(map, "ladderAuthoritySha256")?
            && array(map, "stageOrder")?
                == &[
                    Value::String("validation".into()),
                    Value::String("scrutiny".into())
                ],
        "v2 ladder authority identity/order drifted"
    );
    require_sha(
        string(map, "rotatingEvidenceSha256")?,
        "ladder rotating evidence",
    )?;
    require_sha(
        string(map, "workerContractSha256")?,
        "ladder worker contract",
    )?;
    ensure!(
        map.get("sourceGenerationIndex")
            .and_then(Value::as_u64)
            .unwrap_or(0)
            > 0
            && !string(map, "panelId")?.is_empty(),
        "v2 ladder source generation/panel is invalid"
    );
    let stages = object(
        map.get("stages")
            .ok_or_else(|| anyhow!("v2 ladder stages missing"))?,
        "v2 ladder stages",
    )?;
    exact_keys(stages, &["validation", "scrutiny"], "v2 ladder stages")?;
    for (stage, source_kind, campaign_role, months, role, limit) in [
        (
            "validation",
            "generation_finalizer_commit",
            "evidence_ladder_validation",
            12_u64,
            "validation",
            128_u64,
        ),
        (
            "scrutiny",
            "qd_archive_reducer_result",
            "evidence_ladder_scrutiny",
            36_u64,
            "scrutiny",
            32_u64,
        ),
    ] {
        let binding = object(
            stages
                .get(stage)
                .ok_or_else(|| anyhow!("v2 ladder {stage} stage missing"))?,
            &format!("v2 ladder {stage} stage"),
        )?;
        exact_keys(
            binding,
            &[
                "stage",
                "sourceArchiveAuthorityKind",
                "campaignRole",
                "window",
                "requestedHorizonMonths",
                "evidenceRole",
                "candidateLimit",
                "templatePreparationPath",
                "templatePreparationSha256",
                "templateAuthorityId",
                "constructionCatalogPath",
                "constructionCatalogSha256",
                "archivePolicyAuthority",
                "behaviorAttributionRequirement",
            ],
            &format!("v2 ladder {stage} stage"),
        )?;
        ensure!(
            binding.get("stage").and_then(Value::as_str) == Some(stage)
                && binding
                    .get("sourceArchiveAuthorityKind")
                    .and_then(Value::as_str)
                    == Some(source_kind)
                && binding.get("campaignRole").and_then(Value::as_str) == Some(campaign_role)
                && binding
                    .get("requestedHorizonMonths")
                    .and_then(Value::as_u64)
                    == Some(months)
                && binding.get("evidenceRole").and_then(Value::as_str) == Some(role)
                && binding.get("candidateLimit").and_then(Value::as_u64) == Some(limit),
            "v2 ladder {stage} fixed mapping drifted"
        );
        validate_continuous_month_window(
            binding
                .get("window")
                .ok_or_else(|| anyhow!("v2 ladder {stage} window missing"))?,
            months,
            stage,
        )?;
        require_sha(
            string(binding, "templatePreparationSha256")?,
            &format!("v2 ladder {stage} template"),
        )?;
        require_sha(
            string(binding, "templateAuthorityId")?,
            &format!("v2 ladder {stage} template authority"),
        )?;
        require_sha(
            string(binding, "constructionCatalogSha256")?,
            &format!("v2 ladder {stage} catalog"),
        )?;
        validate_v5_archive_authority(
            binding
                .get("archivePolicyAuthority")
                .ok_or_else(|| anyhow!("v2 ladder archive policy missing"))?,
        )?;
        validate_v5_behavior_requirement(
            binding
                .get("behaviorAttributionRequirement")
                .ok_or_else(|| anyhow!("v2 ladder behavior requirement missing"))?,
        )?;
    }
    Ok(())
}

fn ladder_materialization_inventory(root: &Path, include_transaction: bool) -> Result<Value> {
    let mut paths = vec![
        "validation-12m-template-preparation.json",
        "validation-authority.json",
        "scrutiny-36m-template-preparation.json",
        "scrutiny-authority.json",
        "ladder-authority.json",
        "materialization-result.json",
    ];
    if include_transaction {
        paths.push("materialization-transaction.json");
    }
    paths
        .into_iter()
        .map(|relative| {
            let path = root.join(relative);
            require_regular_path_without_symlink_ancestors(&path, "ladder materialization output")?;
            Ok(json!({"relativePath":relative,"rawSha256":file_sha256(&path)?}))
        })
        .collect::<Result<Vec<_>>>()
        .map(Value::Array)
}

fn write_ladder_materialization_commit(
    root: &Path,
    manifest_sha: &str,
    rotating_sha: &str,
    generation: u64,
    panel_id: &str,
    source_finalizer_receipt_sha: &str,
    ladder: &Value,
    stages: &BTreeMap<String, PreparationDescriptor>,
) -> Result<()> {
    let ladder_sha = string(object(ladder, "ladder authority")?, "ladderAuthoritySha256")?;
    let mut transaction = json!({
        "schemaVersion":V5_LADDER_MATERIALIZATION_TRANSACTION_SCHEMA,
        "manifestSha256":manifest_sha,
        "rotatingEvidenceSha256":rotating_sha,
        "sourceGenerationIndex":generation,
        "panelId":panel_id,
        "sourceFinalizerReceiptSha256":source_finalizer_receipt_sha,
        "ladderAuthoritySha256":ladder_sha,
        "validationTemplatePreparationSha256":stages["validation"].semantic_sha,
        "validationTemplateAuthorityId":stages["validation"].authority_id,
        "scrutinyTemplatePreparationSha256":stages["scrutiny"].semantic_sha,
        "scrutinyTemplateAuthorityId":stages["scrutiny"].authority_id,
        "outputInventory":ladder_materialization_inventory(root,false)?,
    });
    transaction["transactionSha256"] = Value::String(canonical_sha256(&transaction)?);
    write_once_pretty(&root.join("materialization-transaction.json"), &transaction)?;
    let mut receipt = json!({
        "schemaVersion":V5_LADDER_MATERIALIZATION_RECEIPT_SCHEMA,
        "manifestSha256":manifest_sha,
        "transactionSha256":transaction["transactionSha256"],
        "rotatingEvidenceSha256":rotating_sha,
        "sourceGenerationIndex":generation,
        "panelId":panel_id,
        "sourceFinalizerReceiptSha256":source_finalizer_receipt_sha,
        "ladderAuthoritySha256":ladder_sha,
        "validationTemplatePreparationSha256":stages["validation"].semantic_sha,
        "validationTemplateAuthorityId":stages["validation"].authority_id,
        "scrutinyTemplatePreparationSha256":stages["scrutiny"].semantic_sha,
        "scrutinyTemplateAuthorityId":stages["scrutiny"].authority_id,
        "outputInventory":ladder_materialization_inventory(root,true)?,
    });
    receipt["receiptSha256"] = Value::String(canonical_sha256(&receipt)?);
    write_once_pretty(&root.join("materialization-receipt.json"), &receipt)
}

fn reopen_v5_ladder_materialization(root: &Path, manifest_sha: &str) -> Result<Value> {
    let receipt = read_pretty_json(
        &root.join("materialization-receipt.json"),
        "v2 ladder materialization receipt",
    )?;
    let map = object(&receipt, "v2 ladder materialization receipt")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "manifestSha256",
            "transactionSha256",
            "rotatingEvidenceSha256",
            "sourceGenerationIndex",
            "panelId",
            "sourceFinalizerReceiptSha256",
            "ladderAuthoritySha256",
            "validationTemplatePreparationSha256",
            "validationTemplateAuthorityId",
            "scrutinyTemplatePreparationSha256",
            "scrutinyTemplateAuthorityId",
            "outputInventory",
            "receiptSha256",
        ],
        "v2 ladder materialization receipt",
    )?;
    ensure!(
        string(map, "schemaVersion")? == V5_LADDER_MATERIALIZATION_RECEIPT_SCHEMA
            && string(map, "manifestSha256")? == manifest_sha
            && canonical_sha256_without_object_field(&receipt, "receiptSha256")?
                == string(map, "receiptSha256")?
            && map.get("outputInventory") == Some(&ladder_materialization_inventory(root, true)?),
        "v2 ladder materialization receipt identity/output drifted"
    );
    let transaction = read_pretty_json(
        &root.join("materialization-transaction.json"),
        "v2 ladder materialization transaction",
    )?;
    let transaction_map = object(&transaction, "v2 ladder materialization transaction")?;
    exact_keys(
        transaction_map,
        &[
            "schemaVersion",
            "manifestSha256",
            "rotatingEvidenceSha256",
            "sourceGenerationIndex",
            "panelId",
            "sourceFinalizerReceiptSha256",
            "ladderAuthoritySha256",
            "validationTemplatePreparationSha256",
            "validationTemplateAuthorityId",
            "scrutinyTemplatePreparationSha256",
            "scrutinyTemplateAuthorityId",
            "outputInventory",
            "transactionSha256",
        ],
        "v2 ladder materialization transaction",
    )?;
    ensure!(
        string(transaction_map, "schemaVersion")? == V5_LADDER_MATERIALIZATION_TRANSACTION_SCHEMA
            && string(transaction_map, "manifestSha256")? == manifest_sha
            && canonical_sha256_without_object_field(&transaction, "transactionSha256")?
                == string(map, "transactionSha256")?
            && string(transaction_map, "transactionSha256")? == string(map, "transactionSha256")?
            && transaction_map.get("outputInventory")
                == Some(&ladder_materialization_inventory(root, false)?),
        "v2 ladder materialization transaction drifted"
    );
    let ladder = read_pretty_json(&root.join("ladder-authority.json"), "v2 ladder authority")?;
    validate_v2_ladder_authority(&ladder)?;
    ensure!(
        string(
            object(&ladder, "v2 ladder authority")?,
            "ladderAuthoritySha256"
        )? == string(map, "ladderAuthoritySha256")?,
        "v2 ladder materialization authority receipt drifted"
    );
    let result = read_pretty_json(
        &root.join("materialization-result.json"),
        "v2 ladder materialization result",
    )?;
    let result_map = object(&result, "v2 ladder materialization result")?;
    exact_keys(
        result_map,
        &[
            "schemaVersion",
            "manifestSha256",
            "rotatingEvidenceSha256",
            "sourceGenerationIndex",
            "panelId",
            "ladderAuthoritySha256",
            "validationTemplatePreparationSha256",
            "validationTemplateAuthorityId",
            "scrutinyTemplatePreparationSha256",
            "scrutinyTemplateAuthorityId",
            "outputRoot",
        ],
        "v2 ladder materialization result",
    )?;
    ensure!(
        string(result_map, "schemaVersion")? == V5_LADDER_MATERIALIZATION_RESULT_SCHEMA
            && string(result_map, "manifestSha256")? == manifest_sha
            && result_map.get("ladderAuthoritySha256") == map.get("ladderAuthoritySha256")
            && result_map.get("sourceGenerationIndex") == map.get("sourceGenerationIndex")
            && result_map.get("panelId") == map.get("panelId"),
        "v2 ladder materialization result receipt drifted"
    );
    Ok(result)
}

/// Freeze an entire direction-aware v5 campaign from compact, immutable
/// sources.  The legacy Python freezer remains an oracle, but is not called
/// by this path and cannot supply an unsealed candidate list or task matrix.
#[derive(Clone)]
struct LadderArchiveAuthority {
    kind: String,
    receipt_sha256: String,
    validation_freeze_receipt_sha256: Option<String>,
    validation_tail_authority_sha256: Option<String>,
    archive: Value,
    archive_sha256: String,
    archive_raw_sha256: String,
    archive_size_bytes: u64,
}

/// v3 is intentionally separate from the historical v2 adapter.  It trusts
/// only one of two receipt-last archive authorities and creates the compact
/// population sidecar from rich archive members itself.  Thus retained members
/// need not (and normally will not) be present in a current proposal sidecar.
fn execute_v5_ladder_archive_freeze_manifest(spec: &Map<String, Value>) -> Result<Value> {
    let expected = [
        "schemaVersion",
        "archiveAuthority",
        "ladderStage",
        "ladderCandidateLimit",
        "ladderAuthority",
        "templatePreparationPath",
        "templatePreparationSha256",
        "constructionCatalogPath",
        "constructionCatalogSha256",
        "outputRoot",
        "executionEngineCommit",
        "workerContractSha256",
        "campaignRole",
        "panelId",
        "rotatingEvidence",
        "archivePolicyAuthority",
        "behaviorAttributionRequirement",
        "nativeRuntimeAuthority",
        "nativeRuntimeAuthoritySha256",
        "manifestSha256",
    ];
    exact_keys(spec, &expected, "v3 archive-native ladder manifest")?;
    let manifest_sha = v3_ladder_manifest_sha256(spec)?;
    ensure!(
        string(spec, "manifestSha256")? == manifest_sha,
        "v3 archive-native ladder manifest identity drifted"
    );
    let root = PathBuf::from(string(spec, "outputRoot")?);
    fs::create_dir_all(&root).context("create v3 ladder campaign root")?;
    let receipt_path = root.join("ladder-freeze-receipt.json");
    if receipt_path.exists() {
        return reopen_v3_ladder_freeze(&root, spec, &manifest_sha);
    }

    let stage = ladder_stage_and_bindings(spec, true)?;
    let limit = spec
        .get("ladderCandidateLimit")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("ladder candidate limit is invalid"))? as usize;
    let authority = reopen_ladder_archive_authority(
        spec.get("archiveAuthority")
            .ok_or_else(|| anyhow!("v3 archive authority missing"))?,
    )?;
    ensure!(
        (stage == "validation" && authority.kind == "generation_finalizer_commit")
            || (stage == "scrutiny" && authority.kind == "qd_archive_reducer_result"),
        "v3 ladder stage/archive authority kind binding drifted"
    );
    let members = ladder_members_from_archive(&authority.archive, limit)?;
    let candidates = members
        .iter()
        .cloned()
        .map(validate_ladder_rich_member)
        .collect::<Result<Vec<_>>>()?;
    let generation = object(&authority.archive, "authenticated archive")?
        .get("generationIndex")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("authenticated archive generationIndex is invalid"))?;
    ensure!(
        generation > 0,
        "authenticated archive generationIndex must be positive"
    );
    let ladder_map = object(
        spec.get("ladderAuthority")
            .ok_or_else(|| anyhow!("sealed ladder authority missing"))?,
        "sealed ladder authority",
    )?;
    ensure!(
        ladder_map
            .get("sourceGenerationIndex")
            .and_then(Value::as_u64)
            == Some(generation),
        "v3 ladder source generation binding drifted"
    );

    // This sidecar is a durable derivative, never an input authority.  The
    // shared v5 writer still consumes its established shape, avoiding a second
    // task-matrix implementation while keeping the input boundary archive-only.
    let population = ladder_archive_population(generation, &candidates)?;
    let population_path = root.join("ladder-archive-population.json");
    write_once_pretty(&population_path, &population)?;
    let selection =
        materialize_ladder_selection_from_archive(&root, spec, generation, &candidates)?;
    let selection_path = root.join("cohort-selection.json");
    let mut delegated = spec.clone();
    delegated.remove("archiveAuthority");
    delegated.remove("ladderStage");
    delegated.remove("ladderCandidateLimit");
    delegated.remove("ladderAuthority");
    delegated.remove("manifestSha256");
    delegated.insert(
        "schemaVersion".into(),
        Value::String(V5_LADDER_FREEZE_MANIFEST_SCHEMA.into()),
    );
    delegated.insert(
        "evaluationPopulationPath".into(),
        Value::String(population_path.to_string_lossy().into_owned()),
    );
    delegated.insert(
        "evaluationPopulationSha256".into(),
        Value::String(file_sha256(&population_path)?),
    );
    delegated.insert(
        "cohortSelectionPath".into(),
        Value::String(selection_path.to_string_lossy().into_owned()),
    );
    delegated.insert(
        "ladderInputSha256".into(),
        Value::String(canonical_sha256(&json!({
            "archiveAuthorityKind": authority.kind,
            "archiveAuthorityReceiptSha256": authority.receipt_sha256,
            "validationFreezeReceiptSha256": authority.validation_freeze_receipt_sha256,
            "validationTailAuthoritySha256": authority.validation_tail_authority_sha256,
            "archiveSha256": authority.archive_sha256,
            "archiveRawSha256": authority.archive_raw_sha256,
            "archiveSizeBytes": authority.archive_size_bytes,
            "ladderStage": stage,
            "ladderCandidateLimit": limit,
            "ladderAuthority": spec.get("ladderAuthority").cloned().unwrap_or(Value::Null),
        }))?),
    );
    let delegated_sha = v5_manifest_sha256(&delegated)?;
    delegated.insert(
        "manifestSha256".into(),
        Value::String(delegated_sha.clone()),
    );
    let native = execute_v5_freeze_manifest(&delegated)?;
    let result = json!({
        "schemaVersion":V5_LADDER_ARCHIVE_FREEZE_RESULT_SCHEMA,
        "manifestSha256":manifest_sha,
        "archiveAuthorityKind":authority.kind,
        "archiveAuthorityReceiptSha256":authority.receipt_sha256,
        "validationFreezeReceiptSha256":authority.validation_freeze_receipt_sha256,
        "validationTailAuthoritySha256":authority.validation_tail_authority_sha256,
        "archiveSha256":authority.archive_sha256,
        "ladderStage":stage,
        "ladderCandidateLimit":limit,
        "selectionSha256":selection.get("selectionSha256").cloned().unwrap_or(Value::Null),
        "campaignSha256":native.get("campaignSha256").cloned().unwrap_or(Value::Null),
        "authorityId":native.get("authorityId").cloned().unwrap_or(Value::Null),
        "taskMatrixSha256":native.get("taskMatrixSha256").cloned().unwrap_or(Value::Null),
    });
    write_once_pretty(&root.join("ladder-freeze-result.json"), &result)?;
    write_v3_ladder_freeze_commit(
        &root,
        &manifest_sha,
        &authority,
        &stage,
        limit,
        string(
            object(
                spec.get("ladderAuthority")
                    .ok_or_else(|| anyhow!("sealed ladder authority missing"))?,
                "sealed ladder authority",
            )?,
            "ladderAuthoritySha256",
        )?,
        &selection,
        &native,
    )?;
    Ok(result)
}

fn ladder_stage_and_bindings(spec: &Map<String, Value>, verify_template: bool) -> Result<String> {
    let stage = string(spec, "ladderStage")?.to_owned();
    ensure!(
        matches!(stage.as_str(), "validation" | "scrutiny"),
        "ladder stage is invalid"
    );
    let limit = spec
        .get("ladderCandidateLimit")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("ladder candidate limit is invalid"))?;
    ensure!(limit > 0, "ladder candidate limit is invalid");
    let ladder = spec
        .get("ladderAuthority")
        .ok_or_else(|| anyhow!("sealed ladder authority missing"))?;
    let map = object(ladder, "sealed ladder authority")?;
    validate_v2_ladder_authority(ladder)?;
    ensure!(
        array(map, "stageOrder")?
            .iter()
            .map(Value::as_str)
            .collect::<Vec<_>>()
            == vec![Some("validation"), Some("scrutiny")],
        "sealed ladder authority stage order drifted"
    );
    let binding = object(
        object(
            map.get("stages")
                .ok_or_else(|| anyhow!("sealed ladder authority stages missing"))?,
            "sealed ladder stages",
        )?
        .get(&stage)
        .ok_or_else(|| anyhow!("sealed ladder authority stage missing"))?,
        "sealed ladder stage",
    )?;
    ensure!(
        binding.get("candidateLimit").and_then(Value::as_u64) == Some(limit),
        "sealed ladder stage limit drifted"
    );
    let expected_source_kind = if stage == "validation" {
        "generation_finalizer_commit"
    } else {
        "qd_archive_reducer_result"
    };
    let expected_role = if stage == "validation" {
        "evidence_ladder_validation"
    } else {
        "evidence_ladder_scrutiny"
    };
    ensure!(
        binding.get("stage").and_then(Value::as_str) == Some(stage.as_str())
            && binding
                .get("sourceArchiveAuthorityKind")
                .and_then(Value::as_str)
                == Some(expected_source_kind)
            && binding.get("campaignRole").and_then(Value::as_str) == Some(expected_role)
            && spec.get("campaignRole") == binding.get("campaignRole")
            && spec.get("panelId") == map.get("panelId")
            && spec.get("workerContractSha256") == map.get("workerContractSha256")
            && spec.get("executionEngineCommit") == map.get("executionEngineCommit")
            && spec
                .get("rotatingEvidence")
                .and_then(Value::as_object)
                .and_then(|rotating| rotating.get("rotatingEvidenceSha256"))
                == map.get("rotatingEvidenceSha256")
            && spec
                .get("rotatingEvidence")
                .and_then(Value::as_object)
                .and_then(|rotating| rotating.get("outerTail"))
                == map.get("outerTail"),
        "sealed ladder source/stage/window/panel mapping drifted"
    );
    for (manifest_field, authority_field) in [
        ("templatePreparationPath", "templatePreparationPath"),
        ("templatePreparationSha256", "templatePreparationSha256"),
        ("constructionCatalogPath", "constructionCatalogPath"),
        ("constructionCatalogSha256", "constructionCatalogSha256"),
        ("archivePolicyAuthority", "archivePolicyAuthority"),
        (
            "behaviorAttributionRequirement",
            "behaviorAttributionRequirement",
        ),
    ] {
        ensure!(
            spec.get(manifest_field) == binding.get(authority_field),
            "sealed ladder stage {manifest_field} binding drifted"
        );
    }
    if verify_template {
        let template_path = PathBuf::from(string(spec, "templatePreparationPath")?);
        let template = read_bound_historical_pretty_semantic_json(
            &template_path,
            string(spec, "templatePreparationSha256")?,
            "sealed ladder template preparation",
        )?;
        ensure!(
            array(
                object(&template, "sealed ladder template")?,
                "developmentWindows"
            )? == std::slice::from_ref(
                binding
                    .get("window")
                    .ok_or_else(|| anyhow!("sealed ladder stage window missing"))?,
            ),
            "sealed ladder continuous stage window drifted"
        );
        let native_authority = build_native_authority(&template)?;
        ensure!(
            native_authority.get("authorityId") == binding.get("templateAuthorityId"),
            "sealed ladder template authority binding drifted"
        );
    }
    Ok(stage)
}

fn reopen_ladder_archive_authority(value: &Value) -> Result<LadderArchiveAuthority> {
    let map = object(value, "v3 archive authority")?;
    let kind = string(map, "kind")?.to_owned();
    if kind == "generation_finalizer_commit" {
        exact_keys(
            map,
            &["kind", "receiptPath", "receiptSha256"],
            "v3 validation archive authority",
        )?;
    } else if kind == "qd_archive_reducer_result" {
        exact_keys(
            map,
            &[
                "kind",
                "receiptPath",
                "receiptSha256",
                "validationFreezeReceiptPath",
                "validationFreezeReceiptSha256",
                "validationTailAuthorityPath",
                "validationTailAuthoritySha256",
            ],
            "v3 scrutiny archive authority",
        )?;
    } else {
        return Err(anyhow!("v3 archive authority kind is unsupported"));
    }
    let receipt_sha = string(map, "receiptSha256")?.to_owned();
    require_sha(&receipt_sha, "v3 archive authority receipt sha")?;
    let receipt_path = PathBuf::from(string(map, "receiptPath")?);
    ensure!(
        !receipt_path
            .components()
            .any(|component| matches!(component, Component::ParentDir)),
        "v3 archive authority receipt path is unsafe"
    );
    require_regular_path_without_symlink_ancestors(&receipt_path, "v3 archive authority receipt")?;
    let receipt = read_canonical_json_line(&receipt_path, "v3 archive authority receipt")?;
    let receipt_map = object(&receipt, "v3 archive authority receipt")?;
    let archive_path = receipt_path
        .parent()
        .ok_or_else(|| anyhow!("v3 archive authority receipt has no parent"))?
        .join("archive.json");
    require_regular_path_without_symlink_ancestors(&archive_path, "v3 archive authority archive")?;
    let (archive_sha, raw_sha, size) = match kind.as_str() {
        "generation_finalizer_commit" => {
            ensure!(
                string(receipt_map, "schemaVersion")? == "temporal_qd_generation_commit_v1"
                    && canonical_sha256_without_object_field(&receipt, "commitSha256")?
                        == receipt_sha
                    && string(receipt_map, "commitSha256")? == receipt_sha,
                "generation finalizer commit identity drifted"
            );
            let descriptor = object(
                receipt_map
                    .get("parentArchive")
                    .ok_or_else(|| anyhow!("generation finalizer commit lacks parentArchive"))?,
                "generation finalizer parentArchive",
            )?;
            ensure!(
                string(descriptor, "path")? == "archive.json",
                "generation finalizer parentArchive path drifted"
            );
            (
                string(descriptor, "archiveSha256")?.to_owned(),
                string(descriptor, "fileSha256")?.to_owned(),
                descriptor
                    .get("bytes")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| anyhow!("generation finalizer parentArchive bytes invalid"))?,
            )
        }
        "qd_archive_reducer_result" => {
            ensure!(
                string(receipt_map, "schemaVersion")?
                    == "temporal_qd_native_archive_reduction_result_v1"
                    && string(receipt_map, "status")? == "completed"
                    && string(receipt_map, "archivePath")? == "archive.json"
                    && canonical_sha256_without_object_field(&receipt, "resultSha256")?
                        == receipt_sha
                    && string(receipt_map, "resultSha256")? == receipt_sha,
                "archive reducer result identity drifted"
            );
            (
                string(receipt_map, "archiveSha256")?.to_owned(),
                string(receipt_map, "archiveRawSha256")?.to_owned(),
                receipt_map
                    .get("archiveSizeBytes")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| anyhow!("archive reducer archive size invalid"))?,
            )
        }
        _ => unreachable!("archive authority kind validated above"),
    };
    require_sha(&archive_sha, "archive authority archive sha")?;
    require_sha(&raw_sha, "archive authority archive raw sha")?;
    ensure!(
        fs::metadata(&archive_path)?.len() == size && file_sha256(&archive_path)? == raw_sha,
        "archive authority archive byte binding drifted"
    );
    let archive = read_canonical_json_line(&archive_path, "archive authority archive")?;
    ensure!(
        object(&archive, "archive authority archive")?
            .get("schemaVersion")
            .and_then(Value::as_str)
            == Some("temporal_qd_archive_v3")
            && canonical_sha256_without_object_field(&archive, "archiveSha256")? == archive_sha,
        "archive authority archive semantic binding drifted"
    );
    let (validation_freeze_receipt_sha256, validation_tail_authority_sha256) =
        if kind == "qd_archive_reducer_result" {
            let (freeze_sha, tail_sha) = validate_scrutiny_archive_provenance(map, receipt_map)?;
            (Some(freeze_sha), Some(tail_sha))
        } else {
            (None, None)
        };
    Ok(LadderArchiveAuthority {
        kind,
        receipt_sha256: receipt_sha,
        validation_freeze_receipt_sha256,
        validation_tail_authority_sha256,
        archive,
        archive_sha256: archive_sha,
        archive_raw_sha256: raw_sha,
        archive_size_bytes: size,
    })
}

fn validate_scrutiny_archive_provenance(
    descriptor: &Map<String, Value>,
    reducer_receipt: &Map<String, Value>,
) -> Result<(String, String)> {
    let freeze_sha = string(descriptor, "validationFreezeReceiptSha256")?.to_owned();
    let tail_sha = string(descriptor, "validationTailAuthoritySha256")?.to_owned();
    require_sha(&freeze_sha, "validation ladder-freeze receipt")?;
    require_sha(&tail_sha, "validation tail authority")?;
    let freeze_path = PathBuf::from(string(descriptor, "validationFreezeReceiptPath")?);
    let tail_path = PathBuf::from(string(descriptor, "validationTailAuthorityPath")?);
    for (path, name) in [
        (&freeze_path, "validation ladder-freeze receipt"),
        (&tail_path, "validation tail authority"),
    ] {
        ensure!(
            !path
                .components()
                .any(|component| matches!(component, Component::ParentDir)),
            "{name} path is unsafe"
        );
        require_regular_path_without_symlink_ancestors(path, name)?;
    }
    let freeze = read_pretty_json(&freeze_path, "validation ladder-freeze receipt")?;
    let freeze_map = object(&freeze, "validation ladder-freeze receipt")?;
    ensure!(
        string(freeze_map, "schemaVersion")? == V5_LADDER_ARCHIVE_FREEZE_RECEIPT_SCHEMA
            && string(freeze_map, "ladderStage")? == "validation"
            && string(freeze_map, "archiveAuthorityKind")? == "generation_finalizer_commit"
            && canonical_sha256_without_object_field(&freeze, "receiptSha256")? == freeze_sha
            && string(freeze_map, "receiptSha256")? == freeze_sha,
        "scrutiny source validation ladder-freeze receipt drifted"
    );
    let tail = read_canonical_json_line(&tail_path, "validation tail authority")?;
    let tail_map = object(&tail, "validation tail authority")?;
    ensure!(
        string(tail_map, "schemaVersion")? == "temporal_qd_tail_authority_receipt_v1"
            && canonical_sha256_without_object_field(&tail, "tailAuthoritySha256")? == tail_sha
            && string(tail_map, "tailAuthoritySha256")? == tail_sha
            && reducer_receipt.get("tailAuthoritySha256") == Some(&Value::String(tail_sha.clone()))
            && tail_map.get("taskMatrixSha256") == freeze_map.get("taskMatrixSha256")
            && tail_map.get("populationSha256") == freeze_map.get("cohortPopulationSha256"),
        "scrutiny archive is not cryptographically rooted in validation output"
    );
    ensure!(
        reducer_receipt.get("generationIndex") == tail_map.get("generationIndex"),
        "scrutiny reducer/validation tail generation drifted"
    );
    Ok((freeze_sha, tail_sha))
}

/// Archive authority descriptors never get to route through a symlinked
/// directory.  Checking only the leaf would otherwise allow `archive.json`
/// to escape the receipt's publication tree through a symlinked parent.
fn require_regular_path_without_symlink_ancestors(path: &Path, name: &str) -> Result<()> {
    ensure!(path.is_file(), "{name} is not a regular file");
    let mut current = Some(path);
    while let Some(entry) = current {
        if entry.as_os_str().is_empty() {
            break;
        }
        ensure!(
            !fs::symlink_metadata(entry)?.file_type().is_symlink(),
            "{name} path contains a symlink"
        );
        current = entry.parent();
    }
    Ok(())
}

fn ladder_members_from_archive(archive: &Value, limit: usize) -> Result<Vec<Value>> {
    let selected = ladder_cohort_from_archive(archive, limit)?;
    // `ladder_cohort_from_archive` returns the nested candidate; recover the
    // corresponding authenticated member by its candidate ID and require a
    // unique occurrence.  This keeps its established round-robin ordering.
    let mut members = BTreeMap::new();
    for cell in array(object(archive, "authenticated archive")?, "cells")? {
        for member in array(object(cell, "authenticated archive cell")?, "members")? {
            if ladder_quality_member(member) {
                let id = string(
                    object(member, "authenticated archive member")?,
                    "candidateId",
                )?
                .to_owned();
                ensure!(
                    members.insert(id, member.clone()).is_none(),
                    "authenticated archive has duplicate quality candidate IDs"
                );
            }
        }
    }
    selected
        .into_iter()
        .map(|candidate| {
            let id = string(
                object(&candidate, "selected archive candidate")?,
                "candidateId",
            )?;
            members
                .remove(id)
                .ok_or_else(|| anyhow!("selected archive member is missing"))
        })
        .collect()
}

fn validate_ladder_rich_member(member: Value) -> Result<Value> {
    let map = object(&member, "selected archive member")?;
    let candidate = map
        .get("candidate")
        .cloned()
        .ok_or_else(|| anyhow!("selected archive member lacks rich candidate"))?;
    let candidate_map = object(&candidate, "selected archive rich candidate")?;
    for field in [
        "candidateId",
        "candidateIdentitySha256",
        "programSha256",
        "sourceProfileSha256",
    ] {
        ensure!(
            map.get(field) == candidate_map.get(field),
            "selected archive member {field} binding drifted"
        );
    }
    let profile = candidate_map
        .get("sourceProfile")
        .ok_or_else(|| anyhow!("selected archive rich candidate lacks sourceProfile"))?;
    ensure!(
        canonical_sha256(profile)? == string(candidate_map, "sourceProfileSha256")?,
        "selected archive rich candidate profile identity drifted"
    );
    if let Some(member_profile) = map.get("sourceProfile") {
        ensure!(
            canonical_sha256(member_profile)? == canonical_sha256(profile)?,
            "selected archive member sourceProfile binding drifted"
        );
    }
    Ok(candidate)
}

fn ladder_archive_population(generation: u64, candidates: &[Value]) -> Result<Value> {
    let semantic = json!({"schemaVersion":"temporal_qd_evaluation_population_v1","generationIndex":generation,"candidates":candidates});
    let population_sha = canonical_sha256(&semantic)?;
    let mut value = json!({"schemaVersion":"temporal_qd_evaluation_population_v1","generationIndex":generation,"candidates":candidates,"populationSha256":population_sha});
    let sha = canonical_sha256(&value)?;
    value["evaluationPopulationSha256"] = Value::String(sha);
    Ok(value)
}

fn materialize_ladder_selection_from_archive(
    root: &Path,
    spec: &Map<String, Value>,
    generation: u64,
    candidates: &[Value],
) -> Result<Value> {
    let mut rows = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let map = object(candidate, "selected archive candidate")?;
        let mut row = json!({"schemaVersion":COHORT_PROJECTION_ROW_SCHEMA,"candidateId":string(map,"candidateId")?,"candidateIdentitySha256":string(map,"candidateIdentitySha256")?,"candidate":candidate});
        row["projectionRowSha256"] = Value::String(canonical_sha256(&row)?);
        rows.push(row);
    }
    let projection = root.join("cohort-selection.jsonl");
    let mut bytes = Vec::new();
    for row in &rows {
        bytes.extend_from_slice(&canonical_json_line(row)?);
    }
    write_once_bytes(&projection, &bytes)?;
    let mut ids = rows
        .iter()
        .map(|row| string(object(row, "projection row")?, "candidateId").map(ToOwned::to_owned))
        .collect::<Result<Vec<_>>>()?;
    ids.sort();
    ids.dedup();
    ensure!(
        ids.len() == rows.len(),
        "archive ladder projection has duplicate candidate IDs"
    );
    let rotating_sha = string(
        object(
            spec.get("rotatingEvidence")
                .ok_or_else(|| anyhow!("ladder rotating evidence missing"))?,
            "ladder rotating evidence",
        )?,
        "rotatingEvidenceSha256",
    )?;
    let mut selection = json!({"schemaVersion":COHORT_SELECTION_SCHEMA,"contractVersion":"temporal_qd_v5_native_ladder_selection_v3","generationIndex":generation,"campaignRole":string(spec,"campaignRole")?,"panelId":string(spec,"panelId")?,"rotatingEvidenceSha256":rotating_sha,"candidateIds":ids,"candidateProjection":{"relativePath":"cohort-selection.jsonl","rawSha256":file_sha256(&projection)?,"sizeBytes":fs::metadata(&projection)?.len(),"recordCount":rows.len(),"rowSchema":COHORT_PROJECTION_ROW_SCHEMA},"sourceBindings":{}});
    selection["selectionSha256"] = Value::String(canonical_sha256(&selection)?);
    write_once_pretty(&root.join("cohort-selection.json"), &selection)?;
    Ok(selection)
}

fn v3_ladder_manifest_sha256(spec: &Map<String, Value>) -> Result<String> {
    let mut semantic = spec.clone();
    semantic.remove("manifestSha256");
    semantic.remove("outputRoot");
    semantic.remove("templatePreparationPath");
    semantic.remove("constructionCatalogPath");
    if let Some(authority) = semantic
        .get_mut("archiveAuthority")
        .and_then(Value::as_object_mut)
    {
        authority.remove("receiptPath");
        authority.remove("validationFreezeReceiptPath");
        authority.remove("validationTailAuthorityPath");
    }
    Ok(canonical_sha256(&Value::Object(semantic))?)
}

fn v3_ladder_inventory(root: &Path, include_transaction: bool) -> Result<Value> {
    let mut paths = vec![
        "ladder-archive-population.json",
        "cohort-selection.json",
        "cohort-selection.jsonl",
        V5_CAMPAIGN_COHORT_POPULATION_RELATIVE_PATH,
        V5_CAMPAIGN_TASK_PACK_RELATIVE_PATH,
        V5_CAMPAIGN_INPUT_CHECKPOINT_PATH,
        "ladder-freeze-result.json",
    ];
    if include_transaction {
        paths.push("ladder-freeze-transaction.json");
    }
    let mut rows = Vec::with_capacity(paths.len());
    for relative_path in paths {
        let path = root.join(relative_path);
        ensure!(
            path.is_file() && !fs::symlink_metadata(&path)?.file_type().is_symlink(),
            "v3 ladder output inventory is missing or symlinked: {relative_path}"
        );
        rows.push(json!({"relativePath":relative_path,"rawSha256":file_sha256(&path)?}));
    }
    Ok(Value::Array(rows))
}

fn write_v3_ladder_freeze_commit(
    root: &Path,
    manifest_sha: &str,
    authority: &LadderArchiveAuthority,
    stage: &str,
    limit: usize,
    ladder_authority_sha: &str,
    selection: &Value,
    native: &Value,
) -> Result<()> {
    let selection_map = object(selection, "v3 ladder selection")?;
    let projection_sha = string(
        object(
            selection_map
                .get("candidateProjection")
                .ok_or_else(|| anyhow!("v3 ladder selection projection missing"))?,
            "v3 ladder selection projection",
        )?,
        "rawSha256",
    )?;
    let cohort = read_pretty_json(
        &root.join("cohort-population.json"),
        "v3 ladder cohort population",
    )?;
    let cohort_sha = string(
        object(&cohort, "v3 ladder cohort population")?,
        "populationSha256",
    )?;
    let campaign_input =
        open_v5_campaign_input_checkpoint(&root.join(V5_CAMPAIGN_INPUT_CHECKPOINT_PATH))?;
    let campaign_input_checkpoint_sha = campaign_input.checkpoint_sha256.as_str();
    let mut transaction = json!({
        "schemaVersion":V5_LADDER_ARCHIVE_FREEZE_TRANSACTION_SCHEMA,"manifestSha256":manifest_sha,
        "archiveAuthorityKind":authority.kind,"archiveAuthorityReceiptSha256":authority.receipt_sha256,
        "validationFreezeReceiptSha256":authority.validation_freeze_receipt_sha256,"validationTailAuthoritySha256":authority.validation_tail_authority_sha256,
        "archiveSha256":authority.archive_sha256,"archiveRawSha256":authority.archive_raw_sha256,"archiveSizeBytes":authority.archive_size_bytes,
        "ladderStage":stage,"ladderCandidateLimit":limit,"ladderAuthoritySha256":ladder_authority_sha,"selectionSha256":string(selection_map,"selectionSha256")?,"projectionRawSha256":projection_sha,
        "cohortPopulationSha256":cohort_sha,"campaignInputCheckpointSha256":campaign_input_checkpoint_sha,
        "campaignSha256":native.get("campaignSha256").cloned().unwrap_or(Value::Null),"authorityId":native.get("authorityId").cloned().unwrap_or(Value::Null),
        "evaluationIdentitySha256":native.get("evaluationIdentitySha256").cloned().unwrap_or(Value::Null),"taskMatrixSha256":native.get("taskMatrixSha256").cloned().unwrap_or(Value::Null),"taskCount":native.get("taskCount").cloned().unwrap_or(Value::Null),
        "outputInventory":v3_ladder_inventory(root, false)?,
    });
    transaction["transactionSha256"] = Value::String(canonical_sha256(&transaction)?);
    write_once_pretty(&root.join("ladder-freeze-transaction.json"), &transaction)?;
    let mut receipt = json!({
        "schemaVersion":V5_LADDER_ARCHIVE_FREEZE_RECEIPT_SCHEMA,"manifestSha256":manifest_sha,"transactionSha256":transaction["transactionSha256"],
        "archiveAuthorityKind":authority.kind,"archiveAuthorityReceiptSha256":authority.receipt_sha256,
        "validationFreezeReceiptSha256":authority.validation_freeze_receipt_sha256,"validationTailAuthoritySha256":authority.validation_tail_authority_sha256,
        "archiveSha256":authority.archive_sha256,"archiveRawSha256":authority.archive_raw_sha256,"archiveSizeBytes":authority.archive_size_bytes,
        "ladderStage":stage,"ladderCandidateLimit":limit,"ladderAuthoritySha256":ladder_authority_sha,"selectionSha256":string(selection_map,"selectionSha256")?,"projectionRawSha256":projection_sha,
        "cohortPopulationSha256":cohort_sha,"campaignInputCheckpointSha256":campaign_input_checkpoint_sha,"campaignSha256":native.get("campaignSha256").cloned().unwrap_or(Value::Null),"authorityId":native.get("authorityId").cloned().unwrap_or(Value::Null),
        "evaluationIdentitySha256":native.get("evaluationIdentitySha256").cloned().unwrap_or(Value::Null),"taskMatrixSha256":native.get("taskMatrixSha256").cloned().unwrap_or(Value::Null),"taskCount":native.get("taskCount").cloned().unwrap_or(Value::Null),
        "outputInventory":v3_ladder_inventory(root, true)?,
    });
    receipt["receiptSha256"] = Value::String(canonical_sha256(&receipt)?);
    write_once_pretty(&root.join("ladder-freeze-receipt.json"), &receipt)
}

fn reopen_v3_ladder_freeze(
    root: &Path,
    spec: &Map<String, Value>,
    manifest_sha: &str,
) -> Result<Value> {
    let receipt = read_pretty_json(
        &root.join("ladder-freeze-receipt.json"),
        "v3 ladder freeze receipt",
    )?;
    let map = object(&receipt, "v3 ladder freeze receipt")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "manifestSha256",
            "transactionSha256",
            "archiveAuthorityKind",
            "archiveAuthorityReceiptSha256",
            "validationFreezeReceiptSha256",
            "validationTailAuthoritySha256",
            "archiveSha256",
            "archiveRawSha256",
            "archiveSizeBytes",
            "ladderStage",
            "ladderCandidateLimit",
            "ladderAuthoritySha256",
            "selectionSha256",
            "projectionRawSha256",
            "cohortPopulationSha256",
            "campaignInputCheckpointSha256",
            "campaignSha256",
            "authorityId",
            "evaluationIdentitySha256",
            "taskMatrixSha256",
            "taskCount",
            "outputInventory",
            "receiptSha256",
        ],
        "v3 ladder freeze receipt",
    )?;
    ensure!(
        string(map, "schemaVersion")? == V5_LADDER_ARCHIVE_FREEZE_RECEIPT_SCHEMA
            && string(map, "manifestSha256")? == manifest_sha
            && canonical_sha256_without_object_field(&receipt, "receiptSha256")?
                == string(map, "receiptSha256")?
            && map.get("outputInventory") == Some(&v3_ladder_inventory(root, true)?),
        "v3 ladder freeze receipt identity/output binding drifted"
    );
    let transaction = read_pretty_json(
        &root.join("ladder-freeze-transaction.json"),
        "v3 ladder freeze transaction",
    )?;
    let tx = object(&transaction, "v3 ladder freeze transaction")?;
    exact_keys(
        tx,
        &[
            "schemaVersion",
            "manifestSha256",
            "archiveAuthorityKind",
            "archiveAuthorityReceiptSha256",
            "validationFreezeReceiptSha256",
            "validationTailAuthoritySha256",
            "archiveSha256",
            "archiveRawSha256",
            "archiveSizeBytes",
            "ladderStage",
            "ladderCandidateLimit",
            "ladderAuthoritySha256",
            "selectionSha256",
            "projectionRawSha256",
            "cohortPopulationSha256",
            "campaignInputCheckpointSha256",
            "campaignSha256",
            "authorityId",
            "evaluationIdentitySha256",
            "taskMatrixSha256",
            "taskCount",
            "outputInventory",
            "transactionSha256",
        ],
        "v3 ladder freeze transaction",
    )?;
    ensure!(
        string(tx, "schemaVersion")? == V5_LADDER_ARCHIVE_FREEZE_TRANSACTION_SCHEMA
            && canonical_sha256_without_object_field(&transaction, "transactionSha256")?
                == string(map, "transactionSha256")?
            && string(tx, "transactionSha256")? == string(map, "transactionSha256")?
            && tx.get("outputInventory") == Some(&v3_ladder_inventory(root, false)?)
            && tx.get("validationFreezeReceiptSha256") == map.get("validationFreezeReceiptSha256")
            && tx.get("validationTailAuthoritySha256") == map.get("validationTailAuthoritySha256")
            && tx.get("archiveAuthorityReceiptSha256") == map.get("archiveAuthorityReceiptSha256")
            && tx.get("taskMatrixSha256") == map.get("taskMatrixSha256")
            && tx.get("cohortPopulationSha256") == map.get("cohortPopulationSha256")
            && tx.get("campaignInputCheckpointSha256") == map.get("campaignInputCheckpointSha256"),
        "v3 ladder freeze transaction identity/output binding drifted"
    );
    let campaign_input =
        open_v5_campaign_input_checkpoint(&root.join(V5_CAMPAIGN_INPUT_CHECKPOINT_PATH))?;
    ensure!(
        string(map, "campaignInputCheckpointSha256")? == campaign_input.checkpoint_sha256,
        "v3 ladder campaign-input checkpoint binding drifted"
    );
    let stage = ladder_stage_and_bindings(spec, false)?;
    let authority = object(
        spec.get("archiveAuthority")
            .ok_or_else(|| anyhow!("v3 archive authority missing"))?,
        "v3 archive authority",
    )?;
    ensure!(
        string(map, "archiveAuthorityKind")? == string(authority, "kind")?
            && string(map, "archiveAuthorityReceiptSha256")? == string(authority, "receiptSha256")?
            && map.get("validationFreezeReceiptSha256")
                == Some(
                    authority
                        .get("validationFreezeReceiptSha256")
                        .unwrap_or(&Value::Null),
                )
            && map.get("validationTailAuthoritySha256")
                == Some(
                    authority
                        .get("validationTailAuthoritySha256")
                        .unwrap_or(&Value::Null),
                )
            && string(map, "ladderStage")? == stage
            && map.get("ladderCandidateLimit") == spec.get("ladderCandidateLimit")
            && string(map, "ladderAuthoritySha256")?
                == string(
                    object(
                        spec.get("ladderAuthority")
                            .ok_or_else(|| anyhow!("sealed ladder authority missing"))?,
                        "sealed ladder authority",
                    )?,
                    "ladderAuthoritySha256",
                )?,
        "v3 ladder freeze source/stage binding drifted"
    );
    let result = read_pretty_json(&root.join("ladder-freeze-result.json"), "v3 ladder result")?;
    let result_map = object(&result, "v3 ladder result")?;
    exact_keys(
        result_map,
        &[
            "schemaVersion",
            "manifestSha256",
            "archiveAuthorityKind",
            "archiveAuthorityReceiptSha256",
            "validationFreezeReceiptSha256",
            "validationTailAuthoritySha256",
            "archiveSha256",
            "ladderStage",
            "ladderCandidateLimit",
            "selectionSha256",
            "campaignSha256",
            "authorityId",
            "taskMatrixSha256",
        ],
        "v3 ladder result",
    )?;
    ensure!(
        string(result_map, "schemaVersion")? == V5_LADDER_ARCHIVE_FREEZE_RESULT_SCHEMA
            && string(result_map, "manifestSha256")? == manifest_sha
            && result_map.get("archiveAuthorityReceiptSha256")
                == map.get("archiveAuthorityReceiptSha256")
            && result_map.get("validationFreezeReceiptSha256")
                == map.get("validationFreezeReceiptSha256")
            && result_map.get("validationTailAuthoritySha256")
                == map.get("validationTailAuthoritySha256")
            && result_map.get("taskMatrixSha256") == map.get("taskMatrixSha256"),
        "v3 ladder result receipt binding drifted"
    );
    Ok(result)
}

fn execute_v5_ladder_freeze_manifest(spec: &Map<String, Value>) -> Result<Value> {
    let mut delegated = spec.clone();
    let root = PathBuf::from(string(spec, "outputRoot")?);
    fs::create_dir_all(&root).context("create ladder campaign root")?;
    let reduction_result_path = PathBuf::from(string(spec, "finalArchiveReductionResultPath")?);
    let reduction_result_sha = string(spec, "finalArchiveReductionResultSha256")?;
    let reduction_result =
        read_canonical_json_line(&reduction_result_path, "archive reduction result")?;
    let reduction_map = object(&reduction_result, "archive reduction result")?;
    ensure!(
        string(reduction_map, "schemaVersion")? == "temporal_qd_native_archive_reduction_result_v1"
            && string(reduction_map, "status")? == "completed"
            && string(reduction_map, "archivePath")? == "archive.json"
            && canonical_sha256_without_object_field(&reduction_result, "resultSha256")?
                == reduction_result_sha
            && string(reduction_map, "resultSha256")? == reduction_result_sha,
        "archive reduction result identity drifted"
    );
    let archive_path = PathBuf::from(string(spec, "finalArchivePath")?);
    let archive_sha = string(spec, "finalArchiveSha256")?;
    require_sha(archive_sha, "final archive sha")?;
    let archive_raw_sha = string(spec, "finalArchiveRawSha256")?;
    require_sha(archive_raw_sha, "final archive raw sha")?;
    let archive_size = spec
        .get("finalArchiveSizeBytes")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("final archive size is invalid"))?;
    ensure!(
        string(reduction_map, "archiveSha256")? == archive_sha
            && string(reduction_map, "archiveRawSha256")? == archive_raw_sha
            && reduction_map
                .get("archiveSizeBytes")
                .and_then(Value::as_u64)
                == Some(archive_size)
            && reduction_result_path
                .parent()
                .map(|parent| parent.join("archive.json"))
                == Some(archive_path.clone()),
        "archive reduction result/archive path binding drifted"
    );
    let stage = string(spec, "ladderStage")?;
    ensure!(
        matches!(stage, "validation" | "scrutiny"),
        "ladder stage is invalid"
    );
    let limit = spec
        .get("ladderCandidateLimit")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("ladder candidate limit is invalid"))? as usize;
    ensure!(limit > 0, "ladder candidate limit is invalid");
    let ladder = spec
        .get("ladderAuthority")
        .ok_or_else(|| anyhow!("sealed ladder authority missing"))?;
    let ladder_map = object(ladder, "sealed ladder authority")?;
    ensure!(
        string(ladder_map, "schemaVersion")? == V5_LADDER_AUTHORITY_SCHEMA
            && canonical_sha256_without_object_field(ladder, "ladderAuthoritySha256")?
                == string(ladder_map, "ladderAuthoritySha256")?,
        "sealed ladder authority identity drifted"
    );
    let order = array(ladder_map, "stageOrder")?;
    ensure!(
        order.iter().map(Value::as_str).collect::<Vec<_>>()
            == vec![Some("validation"), Some("scrutiny")],
        "sealed ladder authority stage order drifted"
    );
    let stages = object(
        ladder_map
            .get("stages")
            .ok_or_else(|| anyhow!("sealed ladder authority stages missing"))?,
        "sealed ladder stages",
    )?;
    let binding = object(
        stages
            .get(stage)
            .ok_or_else(|| anyhow!("sealed ladder authority stage missing"))?,
        "sealed ladder stage",
    )?;
    ensure!(
        binding.get("candidateLimit").and_then(Value::as_u64) == Some(limit as u64),
        "sealed ladder stage limit drifted"
    );
    for (manifest_field, authority_field) in [
        ("templatePreparationPath", "templatePreparationPath"),
        ("templatePreparationSha256", "templatePreparationSha256"),
        ("constructionCatalogPath", "constructionCatalogPath"),
        ("constructionCatalogSha256", "constructionCatalogSha256"),
        ("archivePolicyAuthority", "archivePolicyAuthority"),
        (
            "behaviorAttributionRequirement",
            "behaviorAttributionRequirement",
        ),
    ] {
        ensure!(
            spec.get(manifest_field) == binding.get(authority_field),
            "sealed ladder stage {manifest_field} binding drifted"
        );
    }
    let selection_path = root.join("cohort-selection.json");
    let selection_receipt_path = root.join("ladder-selection-receipt.json");
    ensure!(
        string(spec, "ladderInputSha256")?
            == canonical_sha256(&json!({
                "finalArchiveSha256": archive_sha,
                "finalArchiveRawSha256": archive_raw_sha,
                "finalArchiveSizeBytes": archive_size,
                "finalArchiveReductionResultSha256": reduction_result_sha,
                "ladderStage": stage,
                "ladderCandidateLimit": limit,
                "ladderAuthority": ladder,
            }))?,
        "sealed ladder input identity drifted"
    );
    // The shared v5 freezer consumes only its common semantic body.  Ladder
    // source bindings are retained in the outer self-hashed manifest and the
    // native selection receipt, never passed as tolerated extra fields.
    for field in [
        "finalArchivePath",
        "finalArchiveReductionResultPath",
        "finalArchiveReductionResultSha256",
        "finalArchiveSha256",
        "finalArchiveRawSha256",
        "finalArchiveSizeBytes",
        "ladderStage",
        "ladderCandidateLimit",
        "ladderAuthority",
    ] {
        delegated.remove(field);
    }
    if selection_receipt_path.exists() {
        reopen_ladder_selection_receipt(
            &selection_receipt_path,
            &selection_path,
            archive_sha,
            archive_raw_sha,
            reduction_result_sha,
            string(ladder_map, "ladderAuthoritySha256")?,
            stage,
            limit,
        )?;
        delegated.insert(
            "cohortSelectionPath".into(),
            Value::String(selection_path.to_string_lossy().into_owned()),
        );
        return finish_ladder_delegation(delegated, stage, archive_sha);
    }
    ensure!(
        !fs::symlink_metadata(&archive_path)?
            .file_type()
            .is_symlink()
            && fs::metadata(&archive_path)?.is_file()
            && fs::metadata(&archive_path)?.len() == archive_size
            && file_sha256(&archive_path)? == archive_raw_sha,
        "sealed final archive byte binding drifted"
    );
    let archive = read_pretty_json(&archive_path, "sealed final archive")?;
    let archive_map = object(&archive, "sealed final archive")?;
    ensure!(
        archive_map.get("schemaVersion").and_then(Value::as_str) == Some("temporal_qd_archive_v3")
            && canonical_sha256_without_object_field(&archive, "archiveSha256")? == archive_sha,
        "sealed final archive identity drifted"
    );
    // Native `_ladder_cohort`: quality-only, per-cell rank order, then a
    // deterministic cell round-robin.  The selected projection must match it
    // exactly, so a stale/coerced cohort cannot enter validation or scrutiny.
    let expected = ladder_cohort_from_archive(&archive, limit)?;
    materialize_ladder_selection(
        &root,
        spec,
        &expected,
        archive_sha,
        archive_raw_sha,
        reduction_result_sha,
        string(ladder_map, "ladderAuthoritySha256")?,
        stage,
        limit,
    )?;
    delegated.insert(
        "cohortSelectionPath".into(),
        Value::String(selection_path.to_string_lossy().into_owned()),
    );
    finish_ladder_delegation(delegated, stage, archive_sha)
}

fn finish_ladder_delegation(
    delegated: Map<String, Value>,
    stage: &str,
    archive_sha: &str,
) -> Result<Value> {
    // Keep the ladder schema in the delegated semantic manifest.  The shared
    // freezer accepts the same exact v2 body, and preserving this discriminator
    // keeps the manifest receipt bound to validation/scrutiny intent.
    let mut result = execute_v5_freeze_manifest(&delegated)?;
    let map = result.as_object_mut().expect("v5 result object");
    map.insert(
        "schemaVersion".into(),
        Value::String(V5_LADDER_FREEZE_RESULT_SCHEMA.into()),
    );
    map.insert("ladderStage".into(), Value::String(stage.into()));
    map.insert(
        "finalArchiveSha256".into(),
        Value::String(archive_sha.into()),
    );
    Ok(result)
}

fn materialize_ladder_selection(
    root: &Path,
    spec: &Map<String, Value>,
    archive_candidates: &[Value],
    archive_sha: &str,
    archive_raw_sha: &str,
    reduction_result_sha: &str,
    ladder_authority_sha: &str,
    stage: &str,
    limit: usize,
) -> Result<()> {
    let evaluation_path = PathBuf::from(string(spec, "evaluationPopulationPath")?);
    let evaluation = read_bound_historical_pretty_json(
        &evaluation_path,
        string(spec, "evaluationPopulationSha256")?,
        "ladder evaluation population",
    )?;
    let evaluation_map = object(&evaluation, "ladder evaluation population")?;
    let generation = evaluation_map
        .get("generationIndex")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("ladder evaluation generation is invalid"))?;
    let mut available = BTreeMap::new();
    for candidate in array(evaluation_map, "candidates")? {
        let candidate = object(candidate, "ladder evaluation candidate")?;
        let id = string(candidate, "candidateId")?.to_owned();
        ensure!(
            available.insert(id, candidate.clone()).is_none(),
            "ladder evaluation population has duplicate candidate IDs"
        );
    }
    let mut projection_rows = Vec::with_capacity(archive_candidates.len());
    for archive_candidate in archive_candidates {
        let archive_candidate = object(archive_candidate, "ladder archive candidate")?;
        let id = string(archive_candidate, "candidateId")?;
        let candidate = available.get(id).ok_or_else(|| {
            anyhow!("ladder archive candidate is absent from evaluation population")
        })?;
        let candidate_map = candidate;
        for field in [
            "candidateIdentitySha256",
            "programSha256",
            "sourceProfileSha256",
        ] {
            ensure!(
                archive_candidate.get(field) == candidate_map.get(field),
                "ladder archive candidate {field} binding drifted"
            );
        }
        ensure!(
            archive_candidate.get("sourceProfile").is_none()
                || canonical_sha256(archive_candidate.get("sourceProfile").expect("checked"))?
                    == canonical_sha256(
                        candidate_map
                            .get("sourceProfile")
                            .ok_or_else(|| anyhow!("ladder evaluation candidate lacks profile"))?
                    )?,
            "ladder archive candidate profile binding drifted"
        );
        let mut row = json!({"schemaVersion":COHORT_PROJECTION_ROW_SCHEMA,"candidateId":id,"candidateIdentitySha256":candidate_map.get("candidateIdentitySha256").cloned().ok_or_else(|| anyhow!("ladder candidate identity missing"))?,"candidate":candidate});
        let sha = canonical_sha256(&row)?;
        row["projectionRowSha256"] = Value::String(sha);
        projection_rows.push(row);
    }
    let projection_path = root.join("cohort-selection.jsonl");
    let mut bytes = Vec::new();
    for row in &projection_rows {
        bytes.extend_from_slice(&temporal_qd_contract::canonical_json_line(row)?);
    }
    write_once_bytes(&projection_path, &bytes)?;
    let mut ids = projection_rows
        .iter()
        .map(|row| {
            row.get("candidateId")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .ok_or_else(|| anyhow!("ladder projection candidate ID missing"))
        })
        .collect::<Result<Vec<_>>>()?;
    ids.sort();
    ids.dedup();
    ensure!(
        ids.len() == projection_rows.len(),
        "ladder projection has duplicate candidate IDs"
    );
    let rotating_sha = string(
        object(
            spec.get("rotatingEvidence")
                .ok_or_else(|| anyhow!("ladder rotating evidence missing"))?,
            "ladder rotating evidence",
        )?,
        "rotatingEvidenceSha256",
    )?;
    let mut selection = json!({"schemaVersion":COHORT_SELECTION_SCHEMA,"contractVersion":"temporal_qd_v5_native_ladder_selection_v1","generationIndex":generation,"campaignRole":string(spec,"campaignRole")?,"panelId":string(spec,"panelId")?,"rotatingEvidenceSha256":rotating_sha,"candidateIds":ids,"candidateProjection":{"relativePath":"cohort-selection.jsonl","rawSha256":file_sha256(&projection_path)?,"sizeBytes":fs::metadata(&projection_path)?.len(),"recordCount":projection_rows.len(),"rowSchema":COHORT_PROJECTION_ROW_SCHEMA},"sourceBindings":{}});
    let selection_sha = canonical_sha256(&selection)?;
    selection["selectionSha256"] = Value::String(selection_sha.clone());
    let selection_path = root.join("cohort-selection.json");
    write_once_pretty(&selection_path, &selection)?;
    let mut receipt = json!({"schemaVersion":"temporal_qd_v5_native_ladder_selection_receipt_v1","finalArchiveSha256":archive_sha,"finalArchiveRawSha256":archive_raw_sha,"archiveReductionResultSha256":reduction_result_sha,"ladderAuthoritySha256":ladder_authority_sha,"ladderStage":stage,"ladderCandidateLimit":limit,"selectionSha256":selection_sha,"projectionRawSha256":file_sha256(&projection_path)?});
    let receipt_sha = canonical_sha256(&receipt)?;
    receipt["receiptSha256"] = Value::String(receipt_sha);
    write_once_pretty(&root.join("ladder-selection-receipt.json"), &receipt)
}

fn reopen_ladder_selection_receipt(
    receipt_path: &Path,
    selection_path: &Path,
    archive_sha: &str,
    archive_raw_sha: &str,
    reduction_result_sha: &str,
    ladder_authority_sha: &str,
    stage: &str,
    limit: usize,
) -> Result<()> {
    let receipt = read_pretty_json(receipt_path, "ladder selection receipt")?;
    let map = object(&receipt, "ladder selection receipt")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "finalArchiveSha256",
            "finalArchiveRawSha256",
            "archiveReductionResultSha256",
            "ladderAuthoritySha256",
            "ladderStage",
            "ladderCandidateLimit",
            "selectionSha256",
            "projectionRawSha256",
            "receiptSha256",
        ],
        "ladder selection receipt",
    )?;
    ensure!(
        string(map, "schemaVersion")? == "temporal_qd_v5_native_ladder_selection_receipt_v1"
            && string(map, "finalArchiveSha256")? == archive_sha
            && string(map, "finalArchiveRawSha256")? == archive_raw_sha
            && string(map, "archiveReductionResultSha256")? == reduction_result_sha
            && string(map, "ladderAuthoritySha256")? == ladder_authority_sha
            && string(map, "ladderStage")? == stage
            && map.get("ladderCandidateLimit").and_then(Value::as_u64) == Some(limit as u64)
            && canonical_sha256_without_object_field(&receipt, "receiptSha256")?
                == string(map, "receiptSha256")?,
        "ladder selection receipt binding drifted"
    );
    let selection = read_pretty_json(selection_path, "native ladder selection")?;
    let selection_map = object(&selection, "native ladder selection")?;
    ensure!(
        string(selection_map, "selectionSha256")? == string(map, "selectionSha256")?
            && canonical_sha256_without_object_field(&selection, "selectionSha256")?
                == string(map, "selectionSha256")?,
        "native ladder selection identity drifted"
    );
    let projection = selection_path
        .parent()
        .ok_or_else(|| anyhow!("native ladder selection has no parent"))?
        .join("cohort-selection.jsonl");
    ensure!(
        file_sha256(&projection)? == string(map, "projectionRawSha256")?,
        "native ladder selection projection drifted"
    );
    let _ = load_sealed_cohort_selection(selection_path)?;
    Ok(())
}

/// Rust equivalent of the legacy `_ladder_cohort` selection primitive.
pub fn ladder_cohort_from_archive(archive: &Value, limit: usize) -> Result<Vec<Value>> {
    let map = object(archive, "sealed final archive")?;
    let mut buckets = Vec::new();
    let mut cells = array(map, "cells")?.clone();
    cells.sort_by_key(|cell| {
        cell.get("cellId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned()
    });
    for cell in cells {
        let mut rows = array(object(&cell, "archive cell")?, "members")?
            .iter()
            .filter(|member| ladder_quality_member(member))
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by(ladder_member_order);
        buckets.push(rows);
    }
    let mut selected = Vec::new();
    while selected.len() < limit && buckets.iter().any(|bucket| !bucket.is_empty()) {
        for bucket in &mut buckets {
            if selected.len() == limit {
                break;
            }
            if !bucket.is_empty() {
                let member = bucket.remove(0);
                selected.push(
                    member
                        .get("candidate")
                        .cloned()
                        .ok_or_else(|| anyhow!("quality archive member lacks candidate"))?,
                );
            }
        }
    }
    Ok(selected)
}
fn ladder_quality_member(value: &Value) -> bool {
    let Some(row) = value.as_object() else {
        return false;
    };
    let valid = row.get("finiteDataValidity").and_then(Value::as_object);
    row.get("archiveLane").and_then(Value::as_str) == Some("quality")
        && valid
            .and_then(|v| v.get("isFiniteData"))
            .and_then(Value::as_bool)
            == Some(true)
        && valid
            .and_then(|v| v.get("passesSupportGate"))
            .and_then(Value::as_bool)
            == Some(true)
        && valid
            .and_then(|v| v.get("validForQuality"))
            .and_then(Value::as_bool)
            == Some(true)
        && row
            .get("objectives")
            .and_then(Value::as_object)
            .and_then(|v| v.get("worstWindowConservativeNetR"))
            .and_then(Value::as_f64)
            .is_some_and(|v| v >= 0.0)
}
fn ladder_member_order(left: &Value, right: &Value) -> std::cmp::Ordering {
    let robust = |value: &Value, field: &str| {
        value
            .get("robustObjectives")
            .and_then(Value::as_object)
            .and_then(|row| row.get(field))
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
    };
    // This is the robust-objective branch of Python's `_parent_member_order`.
    // `total_cmp` preserves a deterministic order even for malformed NaNs.
    robust(right, "worstWindowConservativeNetR")
        .total_cmp(&robust(left, "worstWindowConservativeNetR"))
        .then_with(|| robust(left, "drawdown").total_cmp(&robust(right, "drawdown")))
        .then_with(|| robust(left, "costDrag").total_cmp(&robust(right, "costDrag")))
        .then_with(|| robust(right, "novelty").total_cmp(&robust(left, "novelty")))
        .then_with(|| {
            left.get("candidateId")
                .and_then(Value::as_str)
                .cmp(&right.get("candidateId").and_then(Value::as_str))
        })
}

fn execute_v5_freeze_manifest(spec: &Map<String, Value>) -> Result<Value> {
    let mut expected = vec![
        "schemaVersion",
        "evaluationPopulationPath",
        "evaluationPopulationSha256",
        "cohortSelectionPath",
        "templatePreparationPath",
        "templatePreparationSha256",
        "constructionCatalogPath",
        "constructionCatalogSha256",
        "outputRoot",
        "executionEngineCommit",
        "workerContractSha256",
        "campaignRole",
        "panelId",
        "rotatingEvidence",
        "archivePolicyAuthority",
        "behaviorAttributionRequirement",
        "nativeRuntimeAuthority",
        "nativeRuntimeAuthoritySha256",
        "manifestSha256",
    ];
    if spec.contains_key("ladderInputSha256") {
        expected.push("ladderInputSha256");
    }
    exact_keys(spec, &expected, "v5 native campaign-freeze manifest")?;
    let manifest_sha = v5_manifest_sha256(spec)?;
    ensure!(
        string(spec, "manifestSha256")? == manifest_sha,
        "v5 native campaign-freeze manifest identity drifted"
    );
    let runtime_authority = spec
        .get("nativeRuntimeAuthority")
        .ok_or_else(|| anyhow!("v5 native runtime authority missing"))?;
    validate_v5_runtime_authority(runtime_authority)?;
    ensure!(
        canonical_sha256(runtime_authority)? == string(spec, "nativeRuntimeAuthoritySha256")?,
        "v5 native runtime authority identity drifted"
    );
    let evaluation_path = PathBuf::from(string(spec, "evaluationPopulationPath")?);
    let evaluation_sha = string(spec, "evaluationPopulationSha256")?;
    require_sha(evaluation_sha, "evaluation population raw sha")?;
    // Current-v5 qd-batch publishes this authenticated sidecar as one
    // compact canonical JSON line plus LF.  It is a producer/consumer ABI:
    // do not route it through the legacy Python-pretty document reader or
    // rewrite its bytes before task dispatch.
    let evaluation =
        read_bound_canonical_json(&evaluation_path, evaluation_sha, "evaluation population")?;
    let evaluation_map = object(&evaluation, "evaluation population")?;
    ensure!(
        string(evaluation_map, "schemaVersion")? == "temporal_qd_evaluation_population_v1",
        "v5 freezer requires a native evaluation-population sidecar"
    );
    ensure!(
        canonical_sha256_without_object_field(&evaluation, "evaluationPopulationSha256")?
            == string(evaluation_map, "evaluationPopulationSha256")?,
        "evaluation population semantic identity drifted"
    );
    let generation = evaluation_map
        .get("generationIndex")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("evaluation population generationIndex is invalid"))?;
    ensure!(generation > 0, "v5 generation must be positive");

    let role = string(spec, "campaignRole")?;
    ensure!(
        matches!(
            role,
            "proposal_current_panel"
                | "retained_parent_current_panel"
                | "prior_panel_backfill"
                | "evidence_ladder_validation"
                | "evidence_ladder_scrutiny"
        ),
        "v5 campaign role is invalid"
    );
    let panel_id = string(spec, "panelId")?;
    ensure!(!panel_id.is_empty(), "v5 panel ID is empty");
    let rotating = spec
        .get("rotatingEvidence")
        .ok_or_else(|| anyhow!("v5 rotating evidence missing"))?;
    validate_v5_rotating(rotating, generation, panel_id, role)?;
    let rotating_sha = string(
        object(rotating, "rotating evidence")?,
        "rotatingEvidenceSha256",
    )?;
    let archive = spec
        .get("archivePolicyAuthority")
        .ok_or_else(|| anyhow!("v5 archive authority missing"))?;
    validate_v5_archive_authority(archive)?;
    let behavior = spec
        .get("behaviorAttributionRequirement")
        .ok_or_else(|| anyhow!("v5 behavior requirement missing"))?;
    validate_v5_behavior_requirement(behavior)?;

    let template_path = PathBuf::from(string(spec, "templatePreparationPath")?);
    let template = read_bound_current_v5_pretty_semantic_json(
        &template_path,
        string(spec, "templatePreparationSha256")?,
        "template preparation",
    )?;
    let template_map = object(&template, "template preparation")?;
    ensure!(
        string(template_map, "schemaVersion")? == "temporal_graph_candidate_window_preparation_v1",
        "v5 template preparation schema is incompatible"
    );
    let catalog_path = PathBuf::from(string(spec, "constructionCatalogPath")?);
    let catalog = read_bound_current_v5_pretty_semantic_json(
        &catalog_path,
        string(spec, "constructionCatalogSha256")?,
        "construction catalog",
    )?;
    let root = PathBuf::from(string(spec, "outputRoot")?);
    fs::create_dir_all(&root).context("create v5 campaign root")?;
    let checkpoint_path = root.join(V5_CAMPAIGN_INPUT_CHECKPOINT_PATH);
    if checkpoint_path.exists() {
        let checkpoint = open_v5_campaign_input_checkpoint(&checkpoint_path)?;
        ensure!(
            checkpoint.manifest_sha256 == manifest_sha
                && checkpoint.native_runtime_authority_sha256
                    == string(spec, "nativeRuntimeAuthoritySha256")?,
            "campaign-input checkpoint manifest/runtime binding drifted"
        );
        return Ok(v5_campaign_input_result(&checkpoint, true));
    }
    let commit = string(spec, "executionEngineCommit")?
        .trim()
        .to_ascii_lowercase();
    ensure!(
        commit.len() == 40
            && commit
                .bytes()
                .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v)),
        "v5 execution commit must be a full SHA"
    );
    let worker_sha = string(spec, "workerContractSha256")?
        .trim()
        .to_ascii_lowercase();
    require_sha(&worker_sha, "v5 worker contract sha")?;

    // A proposal campaign derives its current-panel cohort directly from the
    // compact native sidecar.  Re-evaluation campaigns instead admit only the
    // prefinalizer's sealed JSONL selection; no ambient archive scan exists.
    let selection_path = spec
        .get("cohortSelectionPath")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from);
    let (source_candidates, population_sha, evaluation_population_sha, proposal_population, cohort) =
        if let Some(selection) = selection_path {
            ensure!(
                role != "proposal_current_panel",
                "proposal-current-panel must derive from the sealed evaluation population"
            );
            let candidates = if string(spec, "schemaVersion")? == V5_LADDER_FREEZE_MANIFEST_SCHEMA {
                // The ladder's frozen v1 selection/receipt ABI is intentionally
                // unchanged and remains isolated from rotating v2 handoffs.
                load_sealed_cohort_selection(&selection)?
            } else {
                load_v2_non_proposal_task_selection(
                    &selection,
                    generation,
                    None,
                    role,
                    panel_id,
                    rotating_sha,
                )?
            };
            // The selection is a sealed compact projection, but it is not an
            // authority to silently substitute a different candidate universe.
            // Rebind it to the final evaluation-population sidecar before any
            // profile/evidence work begins.  This is the native equivalent of
            // the ladder's old `_ladder_cohort` -> `_ladder_population`
            // hand-off: identifiers alone are insufficient; the candidate,
            // program, and profile identities must all agree.
            if string(spec, "schemaVersion")? == V5_LADDER_FREEZE_MANIFEST_SCHEMA {
                validate_selection_evaluation_binding(&candidates, evaluation_map)?;
                let selection_value = read_pretty_json(&selection, "cohort selection")?;
                let selection_map = object(&selection_value, "cohort selection")?;
                ensure!(
                    selection_map.get("generationIndex").and_then(Value::as_u64)
                        == Some(generation)
                        && selection_map.get("campaignRole").and_then(Value::as_str) == Some(role)
                        && selection_map.get("panelId").and_then(Value::as_str) == Some(panel_id)
                        && selection_map
                            .get("rotatingEvidenceSha256")
                            .and_then(Value::as_str)
                            == Some(rotating_sha),
                    "ladder cohort selection binding drifted"
                );
            }
            let cohort = make_cohort(generation, panel_id, role, rotating_sha, candidates.clone())?;
            (
                candidates,
                string(object(&cohort, "cohort")?, "populationSha256")?.to_owned(),
                None,
                false,
                cohort,
            )
        } else {
            ensure!(
                role == "proposal_current_panel",
                "only proposal-current-panel may omit a sealed cohort selection"
            );
            let cohort =
                derive_g1_proposal_cohort(&evaluation, generation, panel_id, rotating_sha)?;
            let candidates = array(evaluation_map, "candidates")?.clone();
            (
                candidates,
                string(evaluation_map, "populationSha256")?.to_owned(),
                Some(string(evaluation_map, "evaluationPopulationSha256")?.to_owned()),
                true,
                cohort,
            )
        };
    write_once_pretty(&root.join("cohort-population.json"), &cohort)?;
    ensure!(
        !source_candidates.is_empty(),
        "v5 candidate source is empty"
    );

    let worker = object(
        template_map
            .get("workerContract")
            .ok_or_else(|| anyhow!("template worker contract missing"))?,
        "template worker contract",
    )?;
    let mut worker_contract = Value::Object(worker.clone());
    worker_contract
        .as_object_mut()
        .expect("object")
        .insert("workerContractSha256".into(), Value::String(worker_sha));
    let windows = array(template_map, "developmentWindows")?.clone();
    let exemplar = array(template_map, "candidates")?
        .first()
        .ok_or_else(|| anyhow!("template candidates missing"))?;
    let exemplar = object(exemplar, "template exemplar")?;
    let instrument = string(exemplar, "instrument")?.to_owned();
    let timeframe = string(exemplar, "timeframe")?.trim().to_ascii_uppercase();
    let bar_limit = exemplar
        .get("barLimit")
        .cloned()
        .ok_or_else(|| anyhow!("template bar limit missing"))?;
    let input_rows = array(exemplar, "windowInputs")?;
    let mut plans = BTreeMap::new();
    for row in input_rows {
        let row = object(row, "template window input")?;
        plans.insert(
            string(row, "windowId")?.to_owned(),
            row.get("evidencePlan")
                .cloned()
                .ok_or_else(|| anyhow!("template evidence plan missing"))?,
        );
    }
    ensure!(
        plans.len() == windows.len(),
        "template must bind every rotating window exactly once"
    );

    let mut finite = Vec::with_capacity(source_candidates.len());
    let mut evaluation_candidates = Vec::with_capacity(source_candidates.len());
    let evidence_context =
        build_v5_evidence_context(&template, &catalog_path, &catalog, &worker_contract)?;
    for raw in &source_candidates {
        let candidate = object(raw, "v5 source candidate")?;
        let id = string(candidate, "candidateId")?.to_owned();
        let profile = candidate
            .get("sourceProfile")
            .cloned()
            .ok_or_else(|| anyhow!("v5 source candidate lacks profile"))?;
        let profile_sha = string(candidate, "sourceProfileSha256")?.to_owned();
        require_sha(&profile_sha, "v5 source profile sha")?;
        ensure!(
            canonical_sha256(&profile)? == profile_sha,
            "v5 source profile identity drifted"
        );
        let mut window_inputs = Vec::with_capacity(windows.len());
        let mut window_plans = Vec::with_capacity(windows.len());
        for window in &windows {
            let window = object(window, "template development window")?;
            let window_id = string(window, "windowId")?;
            let plan = plans
                .get(window_id)
                .ok_or_else(|| anyhow!("template lacks window plan"))?;
            let binding = object(
                object(plan, "template evidence plan")?
                    .get("lake_window_binding")
                    .ok_or_else(|| anyhow!("template evidence plan lacks lake binding"))?,
                "template lake binding",
            )?;
            validate_catalog_lake_containment(
                &profile,
                &catalog,
                &Value::Object(binding.clone()),
                &timeframe,
            )?;
            let rotated = rotate_evidence_plan_native(plan, &profile, &profile_sha, &timeframe)?;
            let rotated_map = object(&rotated, "rotated evidence plan")?;
            let plan_id = string(rotated_map, "plan_id")?.to_owned();
            let semantic = string(
                object(
                    rotated_map
                        .get("lake_window_binding")
                        .ok_or_else(|| anyhow!("rotated binding missing"))?,
                    "rotated lake binding",
                )?,
                "window_semantic_sha256",
            )?
            .to_owned();
            window_inputs.push(json!({"windowId":window_id,"evidencePlan":rotated,"evidencePlanId":plan_id,"lakeWindowSemanticSha256":semantic}));
            let rotated = object(window_inputs.last().expect("window input"), "window input")?
                .get("evidencePlan")
                .expect("plan");
            let rotated = object(rotated, "rotated plan")?;
            let lake = object(
                rotated.get("lake_window_binding").expect("binding"),
                "binding",
            )?;
            window_plans.push(json!({"planId":plan_id,"analysisWindowStart":rotated.get("analysis_window_start").cloned().unwrap_or(Value::Null),"analysisWindowEnd":rotated.get("analysis_window_end").cloned().unwrap_or(Value::Null),"coveragePolicy":rotated.get("coverage_policy").cloned().unwrap_or(Value::Null),"windowSemanticSha256":semantic,"request":lake.get("request").cloned().unwrap_or(Value::Null),"profileSnapshotSha256":profile_sha}));
        }
        // `preparation.json` deliberately remains the Python-oracle shape:
        // evidencePlanId/lakeWindowSemanticSha256 are authority-normalized
        // derivatives, not authored preparation fields.
        let preparation_inputs = window_inputs
            .iter()
            .map(|row| {
                json!({
                    "windowId": row.get("windowId").cloned().unwrap_or(Value::Null),
                    "evidencePlan": row.get("evidencePlan").cloned().unwrap_or(Value::Null),
                })
            })
            .collect::<Vec<_>>();
        finite.push(json!({"candidateId":id,"sourceProfile":profile,"sourceProfileSha256":profile_sha,"instrument":instrument,"timeframe":timeframe,"barLimit":bar_limit,"windowInputs":preparation_inputs}));
        let profile = object(finite.last().expect("finite candidate"), "finite candidate")?
            .get("sourceProfile")
            .expect("profile");
        let canonical_identity = panel_scoped_identity(
            candidate,
            &evidence_context,
            rotating_sha,
            panel_id,
            generation,
            role,
        )?;
        evaluation_candidates.push(json!({"candidateId":id,"candidateIdentitySha256":candidate.get("candidateIdentitySha256").cloned().unwrap_or(Value::Null),"programSha256":candidate.get("programSha256").cloned().unwrap_or(Value::Null),"canonicalEvidenceIdentitySha256":canonical_identity,"sourceProfileSha256":profile_sha,"canonicalGraphSha256":canonical_sha256(object(profile, "source profile")?.get("graph").ok_or_else(|| anyhow!("source profile graph missing"))?)?,"executionConfigSha256":canonical_sha256(object(profile, "source profile")?.get("executionConfig").unwrap_or(&json!({})))?,"instrument":instrument,"timeframe":timeframe,"barLimit":bar_limit,"windowPlans":window_plans}));
    }
    let task_count = finite
        .len()
        .checked_mul(windows.len())
        .ok_or_else(|| anyhow!("v5 task count overflow"))?;
    let bounds_template = object(
        template_map
            .get("bounds")
            .ok_or_else(|| anyhow!("template bounds missing"))?,
        "template bounds",
    )?;
    let deadline = bounds_template
        .get("deadlineSeconds")
        .and_then(Value::as_f64)
        .ok_or_else(|| anyhow!("template deadline is invalid"))?;
    let preparation = json!({"schemaVersion":"temporal_graph_candidate_window_preparation_v1","authorityLabel":format!("{}-qd-generation-{generation}", string(template_map, "authorityLabel")?),"workerContract":worker_contract,"candidates":finite,"developmentWindows":windows,"prohibitedEvidence":template_map.get("prohibitedEvidence").cloned().ok_or_else(|| anyhow!("template prohibited evidence missing"))?,"bounds":{"maxCandidates":source_candidates.len(),"maxDevelopmentWindows":windows.len(),"maxTasks":task_count,"maxAttempts":bounds_template.get("maxAttempts").cloned().ok_or_else(|| anyhow!("template maxAttempts missing"))?,"deadlineSeconds":deadline}});
    let authority = build_native_authority(&preparation)?;
    let authority_map = object(&authority, "v5 authority")?;
    let catalog_identity = json!({"path":catalog_path.to_string_lossy(),"catalogSha256":string(spec, "constructionCatalogSha256")?});
    let mut identity = json!({"schemaVersion":"temporal_qd_evaluation_identity_v3","policyName":object(archive, "archive authority")?.get("policyName").cloned().unwrap_or(Value::Null),"policySha256":object(archive, "archive authority")?.get("policySha256").cloned().unwrap_or(Value::Null),"populationSha256":population_sha,"constructionCatalog":catalog_identity,"templatePreparationSha256":string(spec, "templatePreparationSha256")?,"workerContract":preparation.get("workerContract").cloned().unwrap_or(Value::Null),"executionEngineCommit":commit,"costViews":{"none":{"spreadBps":0.0,"slippageBps":0.0,"commissionBps":0.0},"research_conservative":{"spreadBps":2.0,"slippageBps":1.0,"commissionBps":0.5}},"predeclaredEvidenceContext":evidence_context,"predeclaredEvidenceContextSha256":object(&evidence_context, "evidence context")?.get("predeclaredEvidenceContextSha256").cloned().unwrap_or(Value::Null),"warmupAndEligibilityPolicy":{"coveragePolicy":"require_complete","barLimitBoundPerCandidate":true,"workerContractOwnsIndicatorWarmup":true,"reservedEvidencePermitted":false},"evaluationSeeds":[],"campaignRole":role,"proposalPopulation":proposal_population,"panelId":panel_id,"rotatingEvidence":rotating,"candidates":evaluation_candidates,"archivePolicyAuthority":archive,"behaviorAttributionRequirement":behavior});
    if let Some(value) = evaluation_population_sha {
        identity
            .as_object_mut()
            .expect("identity")
            .insert("evaluationPopulationSha256".into(), Value::String(value));
    }
    if let Some(policy) = evaluation_map
        .get("bidirectionalPairPolicy")
        .and_then(Value::as_object)
    {
        let mut policy = policy.clone();
        policy.remove("policySha256");
        identity
            .as_object_mut()
            .expect("identity")
            .insert("bidirectionalPairPolicy".into(), Value::Object(policy));
    }
    let identity_sha = canonical_sha256(&identity)?;
    identity.as_object_mut().expect("identity").insert(
        "evaluationIdentitySha256".into(),
        Value::String(identity_sha.clone()),
    );

    let screening_root = root.join("screening-run");
    fs::create_dir_all(&screening_root).context("create v5 screening-run root")?;
    let generated = stream_v5_task_pack(
        authority_map,
        string(authority_map, "authorityId")?,
        &screening_root,
        Some(behavior),
    )?;
    ensure!(
        generated.task_count == task_count,
        "campaign task-pack cardinality drifted from the frozen cohort"
    );
    let mut campaign = json!({"schemaVersion":"temporal_qd_screening_campaign_v3","generationIndex":generation,"populationSha256":population_sha,"constructionCatalog":catalog_identity,"preparationSha256":canonical_sha256(&preparation)? ,"authorityId":string(authority_map, "authorityId")?,"taskMatrixSha256":generated.task_matrix_sha256.clone(),"candidateCount":source_candidates.len(),"windowCount":windows.len(),"taskCount":task_count,"campaignRole":role,"proposalPopulation":proposal_population,"evaluationIdentitySha256":identity_sha,"marketEvidenceScope":"predeclared_development_windows_only","reservedEvidencePermitted":false,"rotatingEvidenceSha256":rotating_sha,"panelId":panel_id,"archivePolicyAuthority":archive,"behaviorAttributionRequirement":behavior});
    if let Some(value) = identity.get("evaluationPopulationSha256") {
        campaign
            .as_object_mut()
            .expect("campaign")
            .insert("evaluationPopulationSha256".into(), value.clone());
    }
    if let Some(value) = identity.get("bidirectionalPairPolicy") {
        campaign
            .as_object_mut()
            .expect("campaign")
            .insert("bidirectionalPairPolicy".into(), value.clone());
    }
    let campaign_sha = canonical_sha256(&campaign)?;
    let task_pack_path = root.join(V5_CAMPAIGN_TASK_PACK_RELATIVE_PATH);
    let cohort_population_path = root.join(V5_CAMPAIGN_COHORT_POPULATION_RELATIVE_PATH);
    let task_pack_bytes = fs::metadata(&task_pack_path)?.len();
    ensure!(
        task_pack_bytes == generated.bytes && file_sha256(&task_pack_path)? == generated.raw_sha256,
        "campaign task-pack byte identity drifted before checkpoint commit"
    );
    let cohort_population_bytes = fs::metadata(&cohort_population_path)?.len();
    let cohort_population_sha256 =
        string(object(&cohort, "cohort")?, "populationSha256")?.to_owned();
    let mut checkpoint = json!({
        "schemaVersion": V5_CAMPAIGN_INPUT_CHECKPOINT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "manifestSha256": manifest_sha,
        "nativeRuntimeAuthoritySha256": string(spec, "nativeRuntimeAuthoritySha256")?,
        "generationIndex": generation,
        "campaignRole": role,
        "panelId": panel_id,
        "authorityId": string(authority_map, "authorityId")?,
        "campaignSha256": campaign_sha,
        "evaluationIdentitySha256": identity_sha,
        "taskMatrixSha256": generated.task_matrix_sha256.clone(),
        "candidateCount": source_candidates.len(),
        "windowCount": windows.len(),
        "taskCount": task_count,
        "tasks": {
            "relativePath": V5_CAMPAIGN_TASK_PACK_RELATIVE_PATH,
            "rawSha256": generated.raw_sha256.clone(),
            "sizeBytes": task_pack_bytes,
            "recordCount": generated.task_count,
            "taskMatrixSha256": generated.task_matrix_sha256.clone(),
        },
        "cohortPopulation": {
            "relativePath": V5_CAMPAIGN_COHORT_POPULATION_RELATIVE_PATH,
            "rawSha256": file_sha256(&cohort_population_path)?,
            "sizeBytes": cohort_population_bytes,
            "populationSha256": cohort_population_sha256,
        },
        "sourceInputs": {
            "evaluationPopulationRawSha256": evaluation_sha,
            "templatePreparationSha256": string(spec, "templatePreparationSha256")?,
            "constructionCatalogSha256": string(spec, "constructionCatalogSha256")?,
            "preparationSha256": canonical_sha256(&preparation)?,
        },
        "artifactMetrics": {
            "payloadFileCount": 2,
            "payloadBytes": task_pack_bytes + cohort_population_bytes,
            "taskPackBytes": task_pack_bytes,
            "cohortPopulationBytes": cohort_population_bytes,
        },
    });
    let checkpoint_sha256 = canonical_sha256(&checkpoint)?;
    checkpoint
        .as_object_mut()
        .expect("campaign-input checkpoint")
        .insert("checkpointSha256".into(), Value::String(checkpoint_sha256));
    write_once_current_v5_compact_json(&checkpoint_path, &checkpoint)?;
    let checkpoint = open_v5_campaign_input_checkpoint(&checkpoint_path)?;
    Ok(v5_campaign_input_result(&checkpoint, false))
}

/// Historical ladder paths retain their Python-pretty document ABI.  Current
/// v5 proposal/evolved publications must use `read_bound_canonical_json`.
fn read_bound_historical_pretty_json(
    path: &Path,
    expected_raw_sha: &str,
    name: &str,
) -> Result<Value> {
    require_sha(expected_raw_sha, name)?;
    ensure!(path.is_file(), "{name} path is not a file");
    ensure!(
        file_sha256(path)? == expected_raw_sha,
        "{name} raw file identity drifted"
    );
    read_pretty_json(path, name)
}

/// Read the exact compact, LF-terminated sidecar produced by the current v5
/// Rust qd-batch publisher.  The raw SHA binds the producer bytes; the
/// canonical-line check rejects a semantically equivalent pretty rewrite so
/// this boundary cannot quietly re-introduce Python serialization as an ABI.
fn read_bound_canonical_json(path: &Path, expected_raw_sha: &str, name: &str) -> Result<Value> {
    require_sha(expected_raw_sha, name)?;
    ensure!(path.is_file(), "{name} path is not a file");
    ensure!(
        file_sha256(path)? == expected_raw_sha,
        "{name} raw file identity drifted"
    );
    read_canonical_json_line(path, name)
}

/// Bind a sealed prefinalizer cohort projection back to the sealed evaluation
/// population that produced the final archive.  The cohort JSONL carries rich
/// candidates so it can be streamed without reopening an ambient archive, but
/// those rows must never be allowed to replace the sealed population identity.
fn validate_selection_evaluation_binding(
    selected: &[Value],
    evaluation: &Map<String, Value>,
) -> Result<()> {
    let mut available = BTreeMap::<String, &Map<String, Value>>::new();
    for row in array(evaluation, "candidates")? {
        let row = object(row, "evaluation population candidate")?;
        let id = string(row, "candidateId")?.to_owned();
        ensure!(
            available.insert(id, row).is_none(),
            "evaluation population has duplicate candidate IDs"
        );
    }
    for row in selected {
        let selected = object(row, "selected cohort candidate")?;
        let id = string(selected, "candidateId")?;
        let source = available.get(id).ok_or_else(|| {
            anyhow!("selected cohort candidate is absent from evaluation population")
        })?;
        for field in [
            "candidateIdentitySha256",
            "programSha256",
            "sourceProfileSha256",
        ] {
            ensure!(
                selected.get(field) == source.get(field),
                "selected cohort {field} binding drifted"
            );
        }
        let selected_profile = selected
            .get("sourceProfile")
            .ok_or_else(|| anyhow!("selected cohort candidate lacks source profile"))?;
        let source_profile = source
            .get("sourceProfile")
            .ok_or_else(|| anyhow!("evaluation population candidate lacks source profile"))?;
        ensure!(
            canonical_sha256(selected_profile)? == canonical_sha256(source_profile)?,
            "selected cohort source profile binding drifted"
        );
    }
    Ok(())
}

/// Historical ladder/materialization documents retain their Python-pretty LF
/// serialization contract. Current rotating-materializer inputs use the
/// separate CRLF-or-LF reader below.
fn read_bound_historical_pretty_semantic_json(
    path: &Path,
    expected_semantic_sha: &str,
    name: &str,
) -> Result<Value> {
    require_sha(expected_semantic_sha, name)?;
    let value = read_pretty_json(path, name)?;
    ensure!(
        canonical_sha256(&value)? == expected_semantic_sha,
        "{name} semantic identity drifted"
    );
    Ok(value)
}

/// Consume a semantic input produced by the current rotating materializer.
/// Its public Python-pretty representation is platform-native: CRLF on
/// Windows and LF on cross-platform producers.  Both spellings are accepted
/// only when they exactly match the canonical Python formatter and the
/// manifest-bound semantic identity; arbitrary whitespace rewrites are not a
/// serialization fallback.
fn read_bound_current_v5_pretty_semantic_json(
    path: &Path,
    expected_semantic_sha: &str,
    name: &str,
) -> Result<Value> {
    require_sha(expected_semantic_sha, name)?;
    let raw = fs::read(path).with_context(|| format!("open {name}"))?;
    let value: Value = serde_json::from_slice(&raw).with_context(|| format!("parse {name}"))?;
    ensure!(
        python_pretty_json_line(&value, JsonNewline::Crlf)? == raw
            || python_pretty_json_line(&value, JsonNewline::Lf)? == raw,
        "{name} must be canonical Python-pretty JSON plus CRLF or LF"
    );
    ensure!(
        canonical_sha256(&value)? == expected_semantic_sha,
        "{name} semantic identity drifted"
    );
    Ok(value)
}
fn make_cohort(
    generation: u64,
    panel: &str,
    role: &str,
    rotating_sha: &str,
    candidates: Vec<Value>,
) -> Result<Value> {
    let mut cohort = json!({"schemaVersion":ROTATING_COHORT_POPULATION_SCHEMA,"generationIndex":generation,"panelId":panel,"cohortRole":role,"rotatingEvidenceSha256":rotating_sha,"candidateCount":candidates.len(),"candidates":candidates,"proposalPopulation":role == "proposal_current_panel"});
    let sha = canonical_sha256(&cohort)?;
    cohort
        .as_object_mut()
        .expect("cohort")
        .insert("populationSha256".into(), Value::String(sha));
    Ok(cohort)
}
fn validate_v5_archive_authority(value: &Value) -> Result<()> {
    let map = object(value, "v5 archive policy authority")?;
    exact_keys(
        map,
        &["qdVersion", "policyName", "policySha256", "frozenPolicy"],
        "v5 archive policy authority",
    )?;
    ensure!(
        string(map, "qdVersion")? == "temporal_qd_evolution_v3",
        "v5 archive authority version drifted"
    );
    ensure!(
        string(map, "policyName")? == "stage5e7_v5_direction_aware_breeding_archive",
        "v5 archive authority policy is not direction-aware"
    );
    require_sha(string(map, "policySha256")?, "v5 archive policy sha")
}
fn validate_v5_behavior_requirement(value: &Value) -> Result<()> {
    let map = object(value, "v5 behavior attribution requirement")?;
    let supplied = string(map, "requirementSha256")?;
    require_sha(supplied, "v5 behavior requirement sha")?;
    ensure!(
        canonical_sha256_without_object_field(value, "requirementSha256")? == supplied,
        "v5 behavior requirement identity drifted"
    );
    ensure!(
        map.get("schemaVersion").and_then(Value::as_str)
            == Some("temporal_qd_behavior_attribution_requirement_v1")
            && map.get("required").and_then(Value::as_bool) == Some(true),
        "v5 behavior requirement is incompatible"
    );
    Ok(())
}
fn validate_v5_rotating(
    value: &Value,
    generation: u64,
    panel_id: &str,
    campaign_role: &str,
) -> Result<()> {
    let map = object(value, "v5 rotating evidence")?;
    let supplied = string(map, "rotatingEvidenceSha256")?;
    require_sha(supplied, "v5 rotating evidence sha")?;
    ensure!(
        canonical_sha256_without_object_field(value, "rotatingEvidenceSha256")? == supplied,
        "v5 rotating evidence identity drifted"
    );
    let panels = array(map, "panels")?;
    ensure!(!panels.is_empty(), "v5 rotating evidence has no panels");
    let cycle = object(
        map.get("absoluteGenerationMapping")
            .ok_or_else(|| anyhow!("v5 rotating mapping missing"))?,
        "v5 rotating mapping",
    )?
    .get("cycleLength")
    .and_then(Value::as_u64)
    .ok_or_else(|| anyhow!("v5 rotating cycle missing"))?;
    ensure!(
        cycle as usize == panels.len(),
        "v5 rotating cycle/panel count drifted"
    );
    let current_panel = panels[((generation - 1) % cycle) as usize]
        .get("panelId")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("v5 current rotating panel lacks panelId"))?;
    let panel_exists = panels
        .iter()
        .any(|panel| panel.get("panelId").and_then(Value::as_str) == Some(panel_id));
    ensure!(panel_exists, "v5 panel is absent from rotating evidence");
    if campaign_role == "prior_panel_backfill" {
        // A backfill is authorized by the prefinalizer's v2 selection receipt
        // and must target a non-current panel from the frozen rotation.
        ensure!(
            panel_id != current_panel,
            "v5 prior-panel backfill cannot target the current panel"
        );
    } else {
        ensure!(
            panel_id == current_panel,
            "v5 panel does not match absolute generation"
        );
    }
    Ok(())
}
fn build_v5_evidence_context(
    template: &Value,
    catalog_path: &Path,
    catalog: &Value,
    worker: &Value,
) -> Result<Value> {
    let map = object(template, "template")?;
    let exemplar = object(
        array(map, "candidates")?
            .first()
            .ok_or_else(|| anyhow!("template candidates missing"))?,
        "template candidate",
    )?;
    let mut ordered = Vec::new();
    let inputs = array(exemplar, "windowInputs")?;
    for window in array(map, "developmentWindows")? {
        let window_map = object(window, "development window")?;
        let id = string(window_map, "windowId")?;
        let mut plan = inputs
            .iter()
            .find_map(|row| {
                row.as_object()
                    .filter(|row| row.get("windowId").and_then(Value::as_str) == Some(id))
                    .and_then(|row| row.get("evidencePlan"))
                    .cloned()
            })
            .ok_or_else(|| anyhow!("template evidence plan missing"))?;
        let plan_map = object(&plan, "template evidence plan")?.clone();
        let plan_object = plan.as_object_mut().expect("plan");
        for key in [
            "plan_id",
            "planId",
            "profile_snapshot_sha256",
            "profileSnapshotSha256",
            "execution_cell_sha256",
            "executionCellSha256",
            "lake_manifest_sha256",
            "lakeManifestSha256",
        ] {
            plan_object.remove(key);
        }
        ordered.push(json!({"windowId":id,"window":window,"evidencePlanSemantic":plan}));
        drop(plan_map);
    }
    let mut context = json!({"schemaVersion":"temporal_qd_predeclared_evidence_context_v3","baseDecisionTimeframe":string(exemplar, "timeframe")?.trim().to_ascii_uppercase(),"orderedWindowPlanSemantic":ordered,"workerContractSha256":object(worker, "worker")?.get("workerContractSha256").cloned().unwrap_or(Value::Null),"constructionCatalog":{"path":catalog_path.to_string_lossy(),"catalogSha256":canonical_sha256(catalog)?},"costViews":{"none":{"spreadBps":0.0,"slippageBps":0.0,"commissionBps":0.0},"research_conservative":{"spreadBps":2.0,"slippageBps":1.0,"commissionBps":0.5}}});
    let sha = canonical_sha256(&context)?;
    context.as_object_mut().expect("context").insert(
        "predeclaredEvidenceContextSha256".into(),
        Value::String(sha),
    );
    Ok(context)
}
fn panel_scoped_identity(
    candidate: &Map<String, Value>,
    context: &Value,
    rotating_sha: &str,
    panel: &str,
    generation: u64,
    role: &str,
) -> Result<String> {
    let context_sha = string(
        object(context, "evidence context")?,
        "predeclaredEvidenceContextSha256",
    )?;
    for key in [
        "candidateIdentitySha256",
        "programSha256",
        "sourceProfileSha256",
    ] {
        require_sha(string(candidate, key)?, key)?;
    }
    Ok(canonical_sha256(
        &json!({"schemaVersion":"temporal_qd_panel_scoped_evaluation_identity_v1","candidateIdentitySha256":string(candidate, "candidateIdentitySha256")?,"programSha256":string(candidate, "programSha256")?,"sourceProfileSha256":string(candidate, "sourceProfileSha256")?,"rotatingEvidenceSha256":rotating_sha,"panelId":panel,"absoluteGenerationIndex":generation,"campaignRole":role,"panelEvidenceContextSha256":context_sha}),
    )?)
}
fn build_native_authority(preparation: &Value) -> Result<Value> {
    let prep = object(preparation, "v5 preparation")?;
    let mut candidates = Vec::new();
    for raw in array(prep, "candidates")? {
        let raw = object(raw, "v5 preparation candidate")?;
        let mut candidate = Map::new();
        for key in [
            "candidateId",
            "sourceProfile",
            "sourceProfileSha256",
            "instrument",
            "timeframe",
            "barLimit",
        ] {
            candidate.insert(
                key.to_owned(),
                raw.get(key)
                    .cloned()
                    .ok_or_else(|| anyhow!("preparation candidate lacks {key}"))?,
            );
        }
        let mut inputs = Vec::new();
        for input in array(raw, "windowInputs")? {
            let input = object(input, "v5 window input")?;
            let plan = input
                .get("evidencePlan")
                .cloned()
                .ok_or_else(|| anyhow!("evidence plan missing"))?;
            let plan_map = object(&plan, "authority evidence plan")?;
            let plan_id = string(plan_map, "plan_id")?;
            let binding = object(
                plan_map
                    .get("lake_window_binding")
                    .ok_or_else(|| anyhow!("authority lake binding missing"))?,
                "authority lake binding",
            )?;
            inputs.push(json!({"windowId":string(input, "windowId")?,"evidencePlan":plan,"evidencePlanId":plan_id,"lakeWindowSemanticSha256":string(binding, "window_semantic_sha256")?}));
        }
        candidate.insert("windowInputs".into(), Value::Array(inputs));
        candidates.push(Value::Object(candidate));
    }
    let normalized = json!({"schemaVersion":"temporal_graph_candidate_window_preparation_v1","authorityLabel":prep.get("authorityLabel").cloned().unwrap_or(Value::Null),"workerContract":prep.get("workerContract").cloned().unwrap_or(Value::Null),"bounds":prep.get("bounds").cloned().unwrap_or(Value::Null),"prohibitedEvidence":prep.get("prohibitedEvidence").cloned().unwrap_or(Value::Null),"developmentWindows":prep.get("developmentWindows").cloned().unwrap_or(Value::Null),"candidates":candidates});
    let preparation_sha = canonical_sha256(&normalized)?;
    let mut authority = json!({"schemaVersion":AUTHORITY_SCHEMA,"authorityLabel":normalized.get("authorityLabel").cloned().unwrap_or(Value::Null),"preparationSha256":preparation_sha,"workerContract":normalized.get("workerContract").cloned().unwrap_or(Value::Null),"bounds":normalized.get("bounds").cloned().unwrap_or(Value::Null),"taskContract":{"taskKind":TASK_KIND,"jobSchema":JOB_SCHEMA,"capability":TASK_CAPABILITY,"resultSchema":"temporal_graph_candidate_window_result_v1","costViews":["research_conservative","none"],"requiredWorkerCapabilities":REQUIRED_CAPABILITIES},"prohibitedEvidence":normalized.get("prohibitedEvidence").cloned().unwrap_or(Value::Null),"developmentWindows":normalized.get("developmentWindows").cloned().unwrap_or(Value::Null),"candidates":normalized.get("candidates").cloned().unwrap_or(Value::Null),"executionPolicy":{"controllerOwns":["generation","validation","checkpoint","journal","resume","dedup","materialization","basic_selection"],"workerOnly":["evaluate_immutable_job"],"mutationEnginePermitted":false,"longEconomicSearchPermitted":false,"reservedEvidencePermitted":false}});
    let authority_sha = canonical_sha256(&authority)?;
    authority
        .as_object_mut()
        .expect("authority")
        .insert("authorityId".into(), Value::String(authority_sha));
    Ok(authority)
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(prefixed(hash.finalize()))
}

/// Paths are transport-only.  The manifest identity deliberately binds all
/// byte/semantic inputs and runtime authority, while allowing a receipt to be
/// replayed from a relocated output tree.
fn v5_manifest_sha256(spec: &Map<String, Value>) -> Result<String> {
    let mut semantic = spec.clone();
    semantic.remove("manifestSha256");
    semantic.remove("outputRoot");
    for key in [
        "evaluationPopulationPath",
        "cohortSelectionPath",
        "templatePreparationPath",
        "constructionCatalogPath",
        "finalArchivePath",
        "finalArchiveReductionResultPath",
        "finalArchiveReductionResultSha256",
        "finalArchiveSha256",
        "finalArchiveRawSha256",
        "finalArchiveSizeBytes",
        "ladderStage",
        "ladderCandidateLimit",
        "ladderAuthority",
    ] {
        semantic.remove(key);
    }
    Ok(canonical_sha256(&Value::Object(semantic))?)
}

fn validate_v5_runtime_authority(value: &Value) -> Result<()> {
    let map = object(value, "v5 native runtime authority")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "runtimeEpoch",
            "binaryRole",
            "binarySha256",
        ],
        "v5 native runtime authority",
    )?;
    ensure!(
        string(map, "schemaVersion")? == V5_RUNTIME_AUTHORITY_SCHEMA
            && string(map, "runtimeEpoch")? == "temporal_qd_native_campaign_freeze_epoch_v2"
            && string(map, "binaryRole")? == "temporal-qd-campaign-freeze",
        "v5 native runtime authority role/epoch is incompatible"
    );
    require_sha(string(map, "binarySha256")?, "v5 native binary sha")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: &str) -> Value {
        json!({"archiveLane":"quality","candidateId":id,"finiteDataValidity":{"isFiniteData":true,"passesSupportGate":true,"validForQuality":true},"objectives":{"worstWindowConservativeNetR":1.0},"candidate":{"candidateId":id}})
    }

    #[test]
    fn ladder_cohort_round_robins_cells_and_breaks_ties_by_candidate_id() {
        let archive = json!({"cells":[
            {"cellId":"b", "members":[member("b-1"), member("b-0")]},
            {"cellId":"a", "members":[member("a-1"), member("a-0")]}
        ]});
        let ids = ladder_cohort_from_archive(&archive, 3)
            .unwrap()
            .into_iter()
            .map(|row| row["candidateId"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["a-0", "b-0", "a-1"]);
    }

    fn test_sha(seed: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(seed.as_bytes());
        prefixed(digest.finalize())
    }

    #[test]
    fn current_v5_task_lets_worker_derive_normalized_temporal_profile_binding() -> Result<()> {
        let profile = json!({
            "version": "v3",
            "instruments": ["EURUSD"],
            "executionConfig": {"managementLibrary": {"version": "temporal_management_v1"}},
        });
        let profile_sha = canonical_sha256(&profile)?;
        let semantic_sha = test_sha("window-semantic");
        let authority = json!({
            "workerContract": {
                "workerContractSha256": test_sha("worker"),
                "workerContractSchema": "replay-worker-contract-v2",
            },
            "bounds": {"deadlineSeconds": 60.0, "maxAttempts": 2},
        });
        let candidate = json!({
            "candidateId": "candidate_1",
            "sourceProfileSha256": profile_sha,
            "sourceProfile": profile,
            "instrument": "EURUSD",
            "timeframe": "M5",
            "barLimit": 5000,
            "windowInputs": [{
                "windowId": "window-1",
                "evidencePlanId": test_sha("evidence-plan"),
                "lakeWindowSemanticSha256": semantic_sha,
                "evidencePlan": {"schema_version": "fuzzfolio.replay-evidence-plan.v2"},
            }],
        });
        let window = json!({
            "windowId": "window-1",
            "analysisWindowStart": "2024-01-01T00:00:00Z",
            "analysisWindowEnd": "2024-04-01T00:00:00Z",
        });
        let task = build_task(
            authority.as_object().unwrap(),
            &test_sha("authority"),
            candidate.as_object().unwrap(),
            window.as_object().unwrap(),
            None,
        )?;
        let payload = object(
            task.get("payload")
                .ok_or_else(|| anyhow!("task payload missing"))?,
            "task payload",
        )?;
        assert!(!payload.contains_key("raw_source_profile_sha256"));
        assert!(!payload.contains_key("temporal_source_profile_sha256"));
        Ok(())
    }

    /// Shape the two current qd-batch producer variants at the boundary.  G0
    /// carries its bootstrap binding; evolved publications deliberately do
    /// not.  Both are compact canonical JSON sidecars with the same public
    /// evaluation-population schema and self-hash rule.
    fn qd_batch_evaluation_population_fixture(generation: u64, g0: bool) -> Value {
        let profile = json!({
            "instruments": ["EURUSD"],
            "executionConfig": {"exitPolicy": {"selectedCell": {"stopLossPercent": 0.5}}},
            "graph": {"nodes": []},
        });
        let profile_sha = canonical_sha256(&profile).unwrap();
        let candidate = json!({
            "candidateId": if g0 { "g0-candidate" } else { "evolved-candidate" },
            "candidateIdentitySha256": test_sha(if g0 { "g0-identity" } else { "evolved-identity" }),
            "programSha256": test_sha(if g0 { "g0-program" } else { "evolved-program" }),
            "sourceProfileSha256": profile_sha,
            "sourceProfile": profile,
        });
        let mut value = json!({
            "schemaVersion": "temporal_qd_evaluation_population_v1",
            "generationIndex": generation,
            "candidateCount": 1,
            "populationSha256": test_sha(if g0 { "g0-population" } else { "evolved-population" }),
            "populationFileSha256": test_sha(if g0 { "g0-population-file" } else { "evolved-population-file" }),
            "pairGenerationConfigSha256": test_sha("pair-config"),
            "policyName": "stage5e7_v5_direction_aware_breeding_archive",
            "policySha256": test_sha("policy"),
            "bidirectionalPairPolicy": {"schemaVersion": "test-pair-policy-v1"},
            "pairPolicySha256": test_sha("pair-policy"),
            "operatorImplementationSha256": test_sha("operator"),
            "predeclaredEvidenceContextSha256": null,
            "candidates": [candidate],
            "proposalAttempts": if g0 { 1 } else { 3 },
            "funnelEntries": [],
        });
        if g0 {
            value["g0Bootstrap"] = json!({
                "constructionPoolIdentitySha256": test_sha("construction-pool"),
                "acceptedPoolSha256": test_sha("accepted-pool"),
                "selectionSha256": test_sha("selection"),
                "ledgerSha256": test_sha("ledger"),
            });
        }
        value["evaluationPopulationSha256"] = Value::String(
            canonical_sha256_without_object_field(&value, "evaluationPopulationSha256").unwrap(),
        );
        value
    }

    #[test]
    fn current_v5_consumes_compact_qd_batch_g0_and_evolved_populations_without_rewrite()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        for (label, generation, g0) in [("g0", 1_u64, true), ("evolved", 2_u64, false)] {
            let population = qd_batch_evaluation_population_fixture(generation, g0);
            let compact_path = root
                .path()
                .join(format!("{label}-evaluation-population.json"));
            write_canonical(&compact_path, &population);
            let compact_raw_sha = file_sha256(&compact_path)?;

            let bound = read_bound_canonical_json(
                &compact_path,
                &compact_raw_sha,
                "current-v5 qd-batch evaluation population",
            )?;
            assert_eq!(bound, population);
            let cohort =
                derive_g1_proposal_cohort(&bound, generation, "panel-a", &test_sha("rotating"))?;
            assert_eq!(cohort["candidateCount"], 1);
            assert_eq!(
                cohort["candidates"][0]["candidateId"],
                population["candidates"][0]["candidateId"]
            );

            // Pretty JSON is semantically identical, but it is a legacy
            // serialization convention and must not become a current-v5
            // producer/consumer fallback.
            let pretty_path = root.path().join(format!("{label}-pretty.json"));
            write_pretty(&pretty_path, &population);
            let pretty_raw_sha = file_sha256(&pretty_path)?;
            assert!(
                read_bound_canonical_json(
                    &pretty_path,
                    &pretty_raw_sha,
                    "current-v5 pretty rewrite",
                )
                .unwrap_err()
                .to_string()
                .contains("canonical JSON followed by LF")
            );

            // A compact-byte mutation cannot ride on the sealed producer raw
            // hash even if it is otherwise well-formed canonical JSON.
            let mut tampered = population.clone();
            tampered["candidates"][0]["programSha256"] = json!(test_sha("tampered-program"));
            let tampered_path = root.path().join(format!("{label}-tampered.json"));
            write_canonical(&tampered_path, &tampered);
            assert!(
                read_bound_canonical_json(
                    &tampered_path,
                    &compact_raw_sha,
                    "current-v5 tampered evaluation population",
                )
                .unwrap_err()
                .to_string()
                .contains("raw file identity drifted")
            );
        }
        Ok(())
    }

    #[test]
    fn current_v5_accepts_rotating_materializer_pretty_crlf_or_lf_but_not_whitespace_substitution()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let window = json!({
            "windowId": "panel-1-window",
            "analysisWindowStart": "2024-01-01T00:00:00Z",
            "analysisWindowEnd": "2024-04-01T00:00:00Z",
        });
        // This mirrors the externally materialized panel preparation's
        // candidate/window/attestation structure rather than a tiny generic
        // JSON object.  The catalog is the other current-v5 path input.
        let template = materialization_preparation(&window, "training", 3, None);
        let catalog = json!({
            "schemaVersion": "temporal_qd_construction_catalog_v1",
            "timeframes": {"M5": {"minimumBars": 5000}},
            "indicators": [{"id": "RSI_MEAN_REVERSION", "timeframes": ["M5"]}],
        });

        for (label, document) in [("template", template), ("catalog", catalog)] {
            let expected = canonical_sha256(&document)?;
            for (newline, suffix) in [(JsonNewline::Crlf, "crlf"), (JsonNewline::Lf, "lf")] {
                let path = root.path().join(format!("{label}-{suffix}.json"));
                fs::write(&path, python_pretty_json_line(&document, newline)?)?;
                assert_eq!(
                    read_bound_current_v5_pretty_semantic_json(
                        &path,
                        &expected,
                        "rotating materializer input",
                    )?,
                    document
                );
            }

            // Valid JSON with non-producer indentation is not an accepted
            // spelling, even though its semantic SHA is unchanged.
            let whitespace_path = root.path().join(format!("{label}-whitespace.json"));
            let whitespace =
                String::from_utf8(python_pretty_json_line(&document, JsonNewline::Crlf)?)?
                    .replace("\r\n  \"", "\r\n    \"");
            fs::write(&whitespace_path, whitespace)?;
            assert!(
                read_bound_current_v5_pretty_semantic_json(
                    &whitespace_path,
                    &expected,
                    "rotating materializer whitespace substitution",
                )
                .unwrap_err()
                .to_string()
                .contains("canonical Python-pretty JSON")
            );

            // A well-formed, producer-spelled reserialization with changed
            // content cannot replace the manifest-bound semantic identity.
            let mut substituted = document.clone();
            substituted["substitutionProbe"] = json!(label);
            let substitution_path = root.path().join(format!("{label}-substituted.json"));
            fs::write(
                &substitution_path,
                python_pretty_json_line(&substituted, JsonNewline::Crlf)?,
            )?;
            assert!(
                read_bound_current_v5_pretty_semantic_json(
                    &substitution_path,
                    &expected,
                    "rotating materializer semantic substitution",
                )
                .unwrap_err()
                .to_string()
                .contains("semantic identity drifted")
            );
        }
        Ok(())
    }

    #[test]
    fn current_v5_campaign_input_checkpoint_is_compact_restart_safe_and_tamper_evident()
    -> Result<()> {
        let root = tempfile::tempdir()?;
        let root_path = root.path();
        let screening = root_path.join("screening-run");
        fs::create_dir_all(&screening)?;
        let manifest_sha = test_sha("manifest");
        let runtime_sha = test_sha("runtime");
        let campaign_sha = test_sha("campaign");
        let authority_id = test_sha("authority");
        let identity_sha = test_sha("evaluation-identity");
        let task_matrix_sha = test_sha("task-matrix");

        let task_pack_path = screening.join("tasks.jsonl");
        fs::write(
            &task_pack_path,
            canonical_json_line(&json!({"taskId":"task-1"}))?,
        )?;

        let mut cohort = json!({
            "schemaVersion": ROTATING_COHORT_POPULATION_SCHEMA,
            "generationIndex": 1,
            "panelId": "panel-a",
            "cohortRole": "proposal_current_panel",
            "rotatingEvidenceSha256": test_sha("rotating"),
            "candidateCount": 1,
            "candidates": [{"candidateId": "candidate-a"}],
            "proposalPopulation": true,
        });
        let cohort_sha = canonical_sha256(&cohort)?;
        cohort["populationSha256"] = Value::String(cohort_sha.clone());
        let cohort_path = root_path.join("cohort-population.json");
        write_once_pretty(&cohort_path, &cohort)?;

        let task_bytes = fs::metadata(&task_pack_path)?.len();
        let cohort_bytes = fs::metadata(&cohort_path)?.len();
        let mut checkpoint = json!({
            "schemaVersion": V5_CAMPAIGN_INPUT_CHECKPOINT_SCHEMA,
            "contractVersion": CONTRACT_VERSION,
            "manifestSha256": manifest_sha,
            "nativeRuntimeAuthoritySha256": runtime_sha,
            "generationIndex": 1,
            "campaignRole": "proposal_current_panel",
            "panelId": "panel-a",
            "authorityId": authority_id,
            "campaignSha256": campaign_sha,
            "evaluationIdentitySha256": identity_sha,
            "taskMatrixSha256": task_matrix_sha,
            "candidateCount": 1,
            "windowCount": 1,
            "taskCount": 1,
            "tasks": {
                "relativePath": V5_CAMPAIGN_TASK_PACK_RELATIVE_PATH,
                "rawSha256": file_sha256(&task_pack_path)?,
                "sizeBytes": task_bytes,
                "recordCount": 1,
                "taskMatrixSha256": task_matrix_sha,
            },
            "cohortPopulation": {
                "relativePath": V5_CAMPAIGN_COHORT_POPULATION_RELATIVE_PATH,
                "rawSha256": file_sha256(&cohort_path)?,
                "sizeBytes": cohort_bytes,
                "populationSha256": cohort_sha,
            },
            "sourceInputs": {
                "evaluationPopulationRawSha256": test_sha("evaluation-raw"),
                "templatePreparationSha256": test_sha("template"),
                "constructionCatalogSha256": test_sha("catalog"),
                "preparationSha256": test_sha("preparation"),
            },
            "artifactMetrics": {
                "payloadFileCount": 2,
                "payloadBytes": task_bytes + cohort_bytes,
                "taskPackBytes": task_bytes,
                "cohortPopulationBytes": cohort_bytes,
            },
        });
        checkpoint["checkpointSha256"] = Value::String(canonical_sha256(&checkpoint)?);
        let checkpoint_path = root_path.join(V5_CAMPAIGN_INPUT_CHECKPOINT_PATH);
        write_once_current_v5_compact_json(&checkpoint_path, &checkpoint)?;

        let opened = open_v5_campaign_input_checkpoint(&checkpoint_path)?;
        assert_eq!(opened.checkpoint_sha256, checkpoint["checkpointSha256"]);
        assert_eq!(opened.task_count, 1);
        assert_eq!(opened.candidate_count, 1);
        let result = v5_campaign_input_result(&opened, true);
        assert_eq!(result["restart"], true);
        assert_eq!(result["artifactMetrics"]["payloadFileCount"], 2);
        for retired in [
            "preparation.json",
            "authority.json",
            "evaluation-identity.json",
            "campaign.json",
            "screening-run/authority.json",
            "screening-run/checkpoint.json",
            "native-freeze-result.json",
            "native-freeze-transaction.json",
            "native-freeze-receipt.json",
        ] {
            assert!(!root_path.join(retired).exists());
        }

        let mut tampered = checkpoint;
        tampered["taskCount"] = Value::from(2_u64);
        write_once_current_v5_compact_json(&root_path.join("tampered.json"), &tampered)?;
        assert!(open_v5_campaign_input_checkpoint(&root_path.join("tampered.json")).is_err());
        Ok(())
    }

    fn archive_with_rich_member() -> Value {
        let profile = json!({"graph":{"nodes":[]},"executionConfig":{}});
        let profile_sha = canonical_sha256(&profile).unwrap();
        let candidate = json!({
            "candidateId":"retained-history", "candidateIdentitySha256":test_sha("identity"),
            "programSha256":test_sha("program"), "sourceProfileSha256":profile_sha,
            "sourceProfile":profile,
        });
        let member = json!({
            "archiveLane":"quality", "candidateId":"retained-history",
            "candidateIdentitySha256":candidate["candidateIdentitySha256"], "programSha256":candidate["programSha256"],
            "sourceProfileSha256":candidate["sourceProfileSha256"], "candidate":candidate,
            "finiteDataValidity":{"isFiniteData":true,"passesSupportGate":true,"validForQuality":true},
            "objectives":{"worstWindowConservativeNetR":1.0},
        });
        let mut archive = json!({"schemaVersion":"temporal_qd_archive_v3","generationIndex":7,"cells":[{"cellId":"a","members":[member]}]});
        archive["archiveSha256"] = Value::String(canonical_sha256(&archive).unwrap());
        archive
    }

    fn write_canonical(path: &Path, value: &Value) {
        fs::write(path, canonical_json_line(value).unwrap()).unwrap();
    }

    fn write_pretty(path: &Path, value: &Value) {
        write_once_pretty(path, value).unwrap();
    }

    fn materialization_preparation(
        window: &Value,
        role: &str,
        months: u64,
        attestation: Option<String>,
    ) -> Value {
        let profile = json!({
            "instruments":["EURUSD"],
            "executionConfig":{"exitPolicy":{"selectedCell":{"stopLossPercent":0.5}}},
        });
        let profile_sha = canonical_sha256(&profile).unwrap();
        let window_map = window.as_object().unwrap();
        let mut binding = json!({
            "schema_version":"fuzzfolio.market-data-window-binding.v1",
            "semantic_contract_id":"fuzzfolio.canonical-bars.semantic-digest.v2",
            "window_semantic_sha256":test_sha(&format!("{role}-window")),
            "attestation_sha256":attestation,
            "creation_global_coverage_sha256":null,
            "creation_source_coverage_sha256":null,
            "legacy_selection_manifest_sha256":null,
            "request":{
                "schema_version":"fuzzfolio.market-data-window-request.v1",
                "dataset":"bars",
                "pairs":["EURUSD"],
                "timeframes":["M5"],
                "data_start":window_map["analysisWindowStart"],
                "data_end":window_map["analysisWindowEnd"],
                "coverage_policy":"require_complete",
            },
        });
        if role == "training" {
            binding["attestation_sha256"] = Value::String(test_sha("quarter-attestation"));
        }
        let mut plan = json!({
            "schema_version":"fuzzfolio.replay-evidence-plan.v2",
            "profile_snapshot_sha256":profile_sha,
            "analysis_window_start":window_map["analysisWindowStart"],
            "analysis_window_end":window_map["analysisWindowEnd"],
            "selection_data_end":window_map["analysisWindowEnd"],
            "data_availability_cutoff":window_map["analysisWindowEnd"],
            "evidence_role":role,
            "requested_horizon_months":months,
            "coverage_policy":"require_complete",
            "execution_cell_sha256":canonical_sha256(&profile["executionConfig"]["exitPolicy"]["selectedCell"]).unwrap(),
            "lake_window_binding":binding,
        });
        plan["plan_id"] = Value::String(canonical_sha256(&plan).unwrap());
        let mut preparation = json!({
            "schemaVersion":"temporal_graph_candidate_window_preparation_v1",
            "authorityLabel":format!("test-{role}"),
            "workerContract":{"workerContractSha256":test_sha("worker"),"workerContractSchema":"replay-worker-contract-v1"},
            "candidates":[{
                "candidateId":"authorized-exemplar",
                "sourceProfile":profile,
                "sourceProfileSha256":profile_sha,
                "instrument":"EURUSD",
                "timeframe":"M5",
                "barLimit":5000,
                "windowInputs":[{"windowId":window_map["windowId"],"evidencePlan":plan}],
            }],
            "developmentWindows":[window],
            "prohibitedEvidence":[{"windowId":"outer-tail","analysisWindowStart":"2026-01-01T00:00:00Z","analysisWindowEnd":"9999-12-31T00:00:00Z","reason":"reserved"}],
            "bounds":{"maxCandidates":1,"maxDevelopmentWindows":1,"maxTasks":1,"maxAttempts":2,"deadlineSeconds":60.0},
        });
        // Keep the borrowed `window` above as a JSON value, not a nested ref.
        preparation["developmentWindows"] = Value::Array(vec![window.clone()]);
        preparation
    }

    fn prep_descriptor(path: &Path, preparation: &Value) -> Value {
        let authority = build_native_authority(preparation).unwrap();
        json!({
            "path":path,
            "preparationSha256":canonical_sha256(preparation).unwrap(),
            "authorityId":authority["authorityId"],
        })
    }

    #[test]
    fn v2_materialization_is_restart_safe_and_rejects_unfresh_or_substituted_templates() {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join("input");
        fs::create_dir_all(&input).unwrap();
        let validation_window = json!({"windowId":"validation-12m","analysisWindowStart":"2024-01-01T00:00:00Z","analysisWindowEnd":"2025-01-01T00:00:00Z"});
        let scrutiny_window = json!({"windowId":"scrutiny-36m","analysisWindowStart":"2022-01-01T00:00:00Z","analysisWindowEnd":"2025-01-01T00:00:00Z"});
        let quarter_window = json!({"windowId":"quarter","analysisWindowStart":"2023-01-01T00:00:00Z","analysisWindowEnd":"2023-04-01T00:00:00Z"});
        let panel = materialization_preparation(&quarter_window, "training", 3, None);
        let panel_path = input.join("panel.json");
        write_pretty(&panel_path, &panel);
        let panel_descriptor = prep_descriptor(&panel_path, &panel);
        let mut contract = json!({
            "schemaVersion":"temporal_qd_rotating_evidence_v1",
            "panels":[{"panelId":"panel-1"}],
            "absoluteGenerationMapping":{"schemaVersion":"temporal_qd_absolute_panel_phase_v1","firstGenerationIndex":1,"cycleLength":1,"mapping":"one_based_modulo_cycle"},
            "panelTemplates":{"panel-1":{"path":panel_path,"preparationSha256":panel_descriptor["preparationSha256"],"authorityId":panel_descriptor["authorityId"]}},
            "researchScrutiny":{"validation":{"window":validation_window,"months":12,"overlapsDevelopmentPermitted":true},"scrutiny":{"window":scrutiny_window,"months":36,"overlapsDevelopmentPermitted":true},"selectionInput":false,"label":"overlapping_research_scrutiny_not_untouched"},
            "outerTail":{"analysisWindowStart":"2026-01-01T00:00:00Z","touched":false,"selectionInput":false,"label":"only_untouched_evidence"},
        });
        contract["rotatingEvidenceSha256"] = Value::String(canonical_sha256(&contract).unwrap());
        let contract_path = input.join("rotating-contract.json");
        write_pretty(&contract_path, &contract);
        let catalog = json!({"timeframes":{"M5":{}},"indicators":[]});
        let catalog_path = input.join("catalog.json");
        write_pretty(&catalog_path, &catalog);
        let mut capability = json!({"schemaVersion":"temporal_qd_catalog_capability_envelope_v1","admittedTimeframes":["M5"]});
        capability["capabilityEnvelopeSha256"] =
            Value::String(canonical_sha256(&capability).unwrap());
        let mut rotating_materialization = json!({
            "schemaVersion":"temporal_qd_rotating_evidence_materialization_v1",
            "curriculumSha256":contract["rotatingEvidenceSha256"],
            "rotatingEvidence":contract,
            "constructionCatalog":{"path":catalog_path,"catalogSha256":canonical_sha256(&catalog).unwrap()},
            "workerContract":{"workerContractSha256":test_sha("worker"),"workerContractSchema":"replay-worker-contract-v1"},
            "catalogCapabilityEnvelope":capability,
            "templates":{"panel-1":{"path":panel_path,"preparationSha256":panel_descriptor["preparationSha256"],"authorityId":panel_descriptor["authorityId"]}},
            "quarters":[{"remoteBinding":{"attestation_sha256":test_sha("quarter-attestation")}}],
            "outerTail":{"analysisWindowStart":"2026-01-01T00:00:00Z","touched":false,"label":"sole_untouched_evidence"},
            "remoteAttestationRequired":true,
        });
        rotating_materialization["materializationSha256"] =
            Value::String(canonical_sha256(&rotating_materialization).unwrap());
        let rotating_materialization_path = input.join("rotating-materialization.json");
        write_pretty(&rotating_materialization_path, &rotating_materialization);
        let archive = archive_with_rich_member();
        let archive_path = input.join("archive.json");
        write_canonical(&archive_path, &archive);
        let mut finalizer = json!({"schemaVersion":"temporal_qd_generation_commit_v1","parentArchive":{"path":"archive.json","archiveSha256":archive["archiveSha256"],"fileSha256":file_sha256(&archive_path).unwrap(),"bytes":fs::metadata(&archive_path).unwrap().len()}});
        finalizer["commitSha256"] = Value::String(canonical_sha256(&finalizer).unwrap());
        let finalizer_path = input.join("generation-commit.json");
        write_canonical(&finalizer_path, &finalizer);
        let validation = materialization_preparation(
            &validation_window,
            "validation",
            12,
            Some(test_sha("fresh-validation")),
        );
        let scrutiny = materialization_preparation(
            &scrutiny_window,
            "scrutiny",
            36,
            Some(test_sha("fresh-scrutiny")),
        );
        let validation_path = input.join("validation.json");
        let scrutiny_path = input.join("scrutiny.json");
        write_pretty(&validation_path, &validation);
        write_pretty(&scrutiny_path, &scrutiny);
        let output = temp.path().join("output");
        let archive_policy = json!({"qdVersion":"temporal_qd_evolution_v3","policyName":"stage5e7_v5_direction_aware_breeding_archive","policySha256":test_sha("policy"),"frozenPolicy":{}});
        let mut behavior = json!({"schemaVersion":"temporal_qd_behavior_attribution_requirement_v1","required":true});
        behavior["requirementSha256"] = Value::String(canonical_sha256(&behavior).unwrap());
        let mut manifest = json!({
            "schemaVersion":V5_LADDER_MATERIALIZATION_MANIFEST_SCHEMA,
            "rotatingEvidenceContract":{"path":contract_path,"rotatingEvidenceSha256":contract["rotatingEvidenceSha256"]},
            "rotatingEvidenceMaterialization":{"path":rotating_materialization_path,"materializationSha256":rotating_materialization["materializationSha256"]},
            "sourceFinalizerAuthority":{"kind":"generation_finalizer_commit","receiptPath":finalizer_path,"receiptSha256":finalizer["commitSha256"]},
            "panelTemplatePreparation":panel_descriptor,
            "constructionCatalog":{"path":catalog_path,"catalogSha256":canonical_sha256(&catalog).unwrap()},
            "stageTemplatePreparations":{"validation":prep_descriptor(&validation_path,&validation),"scrutiny":prep_descriptor(&scrutiny_path,&scrutiny)},
            "workerContractSha256":test_sha("worker"),
            "executionEngineCommit":"a".repeat(40),
            "archivePolicyAuthority":archive_policy,
            "behaviorAttributionRequirement":behavior,
            "outputRoot":output,
        });
        manifest["manifestSha256"] = Value::String(canonical_sha256(&manifest).unwrap());
        let result =
            execute_v5_ladder_materialization_manifest(manifest.as_object().unwrap()).unwrap();
        assert_eq!(result["sourceGenerationIndex"], 7);
        let authority: Value =
            serde_json::from_slice(&fs::read(output.join("ladder-authority.json")).unwrap())
                .unwrap();
        assert_eq!(authority["schemaVersion"], V5_LADDER_AUTHORITY_SCHEMA);
        assert_eq!(authority["stages"]["validation"]["candidateLimit"], 128);
        assert_eq!(authority["stages"]["scrutiny"]["candidateLimit"], 32);

        // A null lake attestation is not an authority.  This fails before any
        // output can be published even when the template and descriptor hash.
        let bad_root = temp.path().join("bad-null-attestation");
        let mut bad_validation = validation.clone();
        bad_validation["candidates"][0]["windowInputs"][0]["evidencePlan"]["lake_window_binding"]
            ["attestation_sha256"] = Value::Null;
        let bad_path = temp.path().join("bad-validation.json");
        write_pretty(&bad_path, &bad_validation);
        let mut bad_manifest = manifest.clone();
        bad_manifest["outputRoot"] = json!(bad_root);
        bad_manifest["stageTemplatePreparations"]["validation"] =
            prep_descriptor(&bad_path, &bad_validation);
        bad_manifest
            .as_object_mut()
            .unwrap()
            .remove("manifestSha256");
        bad_manifest["manifestSha256"] = Value::String(canonical_sha256(&bad_manifest).unwrap());
        assert!(
            execute_v5_ladder_materialization_manifest(bad_manifest.as_object().unwrap())
                .unwrap_err()
                .to_string()
                .contains("attestation")
        );

        let mut bad_semantic = validation.clone();
        let bad_semantic_plan =
            &mut bad_semantic["candidates"][0]["windowInputs"][0]["evidencePlan"];
        bad_semantic_plan["lake_window_binding"]["window_semantic_sha256"] = json!("not-a-sha");
        bad_semantic_plan.as_object_mut().unwrap().remove("plan_id");
        bad_semantic_plan["plan_id"] = Value::String(canonical_sha256(bad_semantic_plan).unwrap());
        let bad_semantic_path = temp.path().join("bad-window-semantic.json");
        write_pretty(&bad_semantic_path, &bad_semantic);
        let mut bad_semantic_manifest = manifest.clone();
        bad_semantic_manifest["outputRoot"] = json!(temp.path().join("bad-window-semantic"));
        bad_semantic_manifest["stageTemplatePreparations"]["validation"] =
            prep_descriptor(&bad_semantic_path, &bad_semantic);
        bad_semantic_manifest
            .as_object_mut()
            .unwrap()
            .remove("manifestSha256");
        bad_semantic_manifest["manifestSha256"] =
            Value::String(canonical_sha256(&bad_semantic_manifest).unwrap());
        assert!(
            execute_v5_ladder_materialization_manifest(bad_semantic_manifest.as_object().unwrap())
                .unwrap_err()
                .to_string()
                .contains("semantic")
        );

        let mut late_start = validation.clone();
        let late_start_plan = &mut late_start["candidates"][0]["windowInputs"][0]["evidencePlan"];
        late_start_plan["lake_window_binding"]["request"]["data_start"] =
            json!("2024-02-01T00:00:00Z");
        late_start_plan.as_object_mut().unwrap().remove("plan_id");
        late_start_plan["plan_id"] = Value::String(canonical_sha256(late_start_plan).unwrap());
        let late_start_path = temp.path().join("late-data-start.json");
        write_pretty(&late_start_path, &late_start);
        let mut late_start_manifest = manifest.clone();
        late_start_manifest["outputRoot"] = json!(temp.path().join("late-data-start"));
        late_start_manifest["stageTemplatePreparations"]["validation"] =
            prep_descriptor(&late_start_path, &late_start);
        late_start_manifest
            .as_object_mut()
            .unwrap()
            .remove("manifestSha256");
        late_start_manifest["manifestSha256"] =
            Value::String(canonical_sha256(&late_start_manifest).unwrap());
        assert!(
            execute_v5_ladder_materialization_manifest(late_start_manifest.as_object().unwrap())
                .unwrap_err()
                .to_string()
                .contains("full-window")
        );

        let mut master = panel.clone();
        master["authorityLabel"] = json!("unauthorized-master-template");
        let master_path = temp.path().join("master.json");
        write_pretty(&master_path, &master);
        let mut master_manifest = manifest.clone();
        master_manifest["outputRoot"] = json!(temp.path().join("bad-master"));
        master_manifest["panelTemplatePreparation"] = prep_descriptor(&master_path, &master);
        master_manifest
            .as_object_mut()
            .unwrap()
            .remove("manifestSha256");
        master_manifest["manifestSha256"] =
            Value::String(canonical_sha256(&master_manifest).unwrap());
        assert!(
            execute_v5_ladder_materialization_manifest(master_manifest.as_object().unwrap())
                .unwrap_err()
                .to_string()
                .contains("substitution")
        );

        // Receipt-last restart is local: every upstream artifact may vanish.
        for entry in fs::read_dir(&input).unwrap() {
            fs::remove_file(entry.unwrap().path()).unwrap();
        }
        assert_eq!(
            execute_v5_ladder_materialization_manifest(manifest.as_object().unwrap()).unwrap(),
            result
        );
    }

    #[test]
    fn v3_archive_authority_accepts_finalizer_and_reducer_and_materializes_retained_member() {
        let root = tempfile::tempdir().unwrap();
        let archive = archive_with_rich_member();
        let archive_path = root.path().join("archive.json");
        write_canonical(&archive_path, &archive);
        let raw_sha = file_sha256(&archive_path).unwrap();
        let bytes = fs::metadata(&archive_path).unwrap().len();

        let mut commit = json!({"schemaVersion":"temporal_qd_generation_commit_v1","parentArchive":{"path":"archive.json","archiveSha256":archive["archiveSha256"],"fileSha256":raw_sha,"bytes":bytes}});
        commit["commitSha256"] = Value::String(canonical_sha256(&commit).unwrap());
        let commit_path = root.path().join("generation-commit.json");
        write_canonical(&commit_path, &commit);
        let finalizer = reopen_ladder_archive_authority(&json!({"kind":"generation_finalizer_commit","receiptPath":commit_path,"receiptSha256":commit["commitSha256"]})).unwrap();
        assert_eq!(finalizer.archive_sha256, archive["archiveSha256"]);
        let selected = ladder_members_from_archive(&finalizer.archive, 1).unwrap();
        assert_eq!(
            validate_ladder_rich_member(selected[0].clone()).unwrap()["candidateId"],
            "retained-history"
        );

        let task_sha = test_sha("validation-task-matrix");
        let population_sha = test_sha("validation-population");
        let mut validation_freeze = json!({
            "schemaVersion":V5_LADDER_ARCHIVE_FREEZE_RECEIPT_SCHEMA,
            "ladderStage":"validation",
            "archiveAuthorityKind":"generation_finalizer_commit",
            "taskMatrixSha256":task_sha,
            "cohortPopulationSha256":population_sha,
        });
        validation_freeze["receiptSha256"] =
            Value::String(canonical_sha256(&validation_freeze).unwrap());
        let validation_freeze_path = root.path().join("validation-freeze-receipt.json");
        write_once_pretty(&validation_freeze_path, &validation_freeze).unwrap();
        let mut tail = json!({
            "schemaVersion":"temporal_qd_tail_authority_receipt_v1",
            "generationIndex":7,
            "taskMatrixSha256":task_sha,
            "populationSha256":population_sha,
        });
        tail["tailAuthoritySha256"] = Value::String(canonical_sha256(&tail).unwrap());
        let tail_path = root.path().join("validation-tail-authority.json");
        write_canonical(&tail_path, &tail);
        let mut reducer = json!({"schemaVersion":"temporal_qd_native_archive_reduction_result_v1","status":"completed","archivePath":"archive.json","archiveSha256":archive["archiveSha256"],"archiveRawSha256":raw_sha,"archiveSizeBytes":bytes,"generationIndex":7,"tailAuthoritySha256":tail["tailAuthoritySha256"]});
        reducer["resultSha256"] = Value::String(canonical_sha256(&reducer).unwrap());
        let reducer_path = root.path().join("archive-reduction-result.json");
        write_canonical(&reducer_path, &reducer);
        let reduced = reopen_ladder_archive_authority(&json!({
            "kind":"qd_archive_reducer_result",
            "receiptPath":reducer_path,
            "receiptSha256":reducer["resultSha256"],
            "validationFreezeReceiptPath":validation_freeze_path,
            "validationFreezeReceiptSha256":validation_freeze["receiptSha256"],
            "validationTailAuthorityPath":tail_path,
            "validationTailAuthoritySha256":tail["tailAuthoritySha256"],
        }))
        .unwrap();
        assert_eq!(
            reduced.archive_raw_sha256,
            file_sha256(&archive_path).unwrap()
        );
        assert!(
            reopen_ladder_archive_authority(&json!({
                "kind":"qd_archive_reducer_result",
                "receiptPath":reducer_path,
                "receiptSha256":reducer["resultSha256"],
                "validationFreezeReceiptPath":validation_freeze_path,
                "validationFreezeReceiptSha256":validation_freeze["receiptSha256"],
                "validationTailAuthorityPath":tail_path,
                "validationTailAuthoritySha256":test_sha("substituted-validation-tail"),
            }))
            .is_err(),
            "scrutiny must reject a reducer detached from the validation tail"
        );

        let traversing = root
            .path()
            .parent()
            .unwrap()
            .join(root.path().file_name().unwrap())
            .join("..")
            .join(root.path().file_name().unwrap())
            .join("generation-commit.json");
        assert!(
            reopen_ladder_archive_authority(&json!({
                "kind":"generation_finalizer_commit", "receiptPath":traversing,
                "receiptSha256":commit["commitSha256"],
            }))
            .is_err()
        );

        let mut forged_archive = archive.clone();
        forged_archive["cells"][0]["members"][0]["candidate"]["programSha256"] =
            Value::String(test_sha("forged-program"));
        forged_archive["cells"][0]["members"][0]["programSha256"] =
            forged_archive["cells"][0]["members"][0]["candidate"]["programSha256"].clone();
        forged_archive["archiveSha256"] = Value::String(
            canonical_sha256_without_object_field(&forged_archive, "archiveSha256").unwrap(),
        );
        write_canonical(&archive_path, &forged_archive);
        let mut forged_commit = commit.clone();
        forged_commit["parentArchive"]["archiveSha256"] = forged_archive["archiveSha256"].clone();
        forged_commit["parentArchive"]["fileSha256"] =
            Value::String(file_sha256(&archive_path).unwrap());
        forged_commit["parentArchive"]["bytes"] = json!(fs::metadata(&archive_path).unwrap().len());
        forged_commit["commitSha256"] = Value::String(
            canonical_sha256_without_object_field(&forged_commit, "commitSha256").unwrap(),
        );
        write_canonical(&commit_path, &forged_commit);
        assert!(
            reopen_ladder_archive_authority(&json!({
                "kind":"generation_finalizer_commit", "receiptPath":commit_path,
                "receiptSha256":commit["commitSha256"],
            }))
            .is_err()
        );
    }

    #[test]
    fn v3_rich_member_rejects_missing_or_substituted_identity() {
        let archive = archive_with_rich_member();
        let member = archive["cells"][0]["members"][0].clone();
        let mut missing = member.clone();
        missing.as_object_mut().unwrap().remove("candidate");
        assert!(validate_ladder_rich_member(missing).is_err());
        let mut changed = member;
        changed["candidate"]["programSha256"] = Value::String(test_sha("other-program"));
        assert!(validate_ladder_rich_member(changed).is_err());
    }

    #[test]
    fn v5_manifest_identity_is_path_invariant_but_binds_runtime_role_epoch() -> Result<()> {
        let runtime = json!({
            "schemaVersion": V5_RUNTIME_AUTHORITY_SCHEMA,
            "runtimeEpoch": "temporal_qd_native_campaign_freeze_epoch_v2",
            "binaryRole": "temporal-qd-campaign-freeze",
            "binarySha256": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        });
        let mut one = Map::new();
        one.insert("outputRoot".into(), json!("C:/one"));
        one.insert(
            "evaluationPopulationPath".into(),
            json!("C:/one/evaluation.json"),
        );
        one.insert("nativeRuntimeAuthority".into(), runtime.clone());
        one.insert(
            "nativeRuntimeAuthoritySha256".into(),
            json!(canonical_sha256(&runtime)?),
        );
        let first = v5_manifest_sha256(&one)?;
        one.insert("outputRoot".into(), json!("D:/two"));
        one.insert(
            "evaluationPopulationPath".into(),
            json!("D:/two/evaluation.json"),
        );
        assert_eq!(first, v5_manifest_sha256(&one)?);
        let mut changed = runtime.clone();
        changed["runtimeEpoch"] = json!("temporal_qd_native_campaign_freeze_epoch_other");
        one.insert("nativeRuntimeAuthority".into(), changed.clone());
        one.insert(
            "nativeRuntimeAuthoritySha256".into(),
            json!(canonical_sha256(&changed)?),
        );
        assert_ne!(first, v5_manifest_sha256(&one)?);
        assert!(validate_v5_runtime_authority(&changed).is_err());
        Ok(())
    }

    #[test]
    fn production_router_rejects_the_superseded_v1_v5_manifest() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "temporal-qd-v5-freeze-v1-rejection-{}.json",
            std::process::id()
        ));
        fs::write(
            &path,
            python_pretty_json_line(
                &json!({"schemaVersion":"temporal_qd_v5_native_campaign_freeze_manifest_v1"}),
                JsonNewline::Lf,
            )?,
        )?;
        assert!(execute_manifest(&path).is_err());
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn v5_cohort_marks_only_proposal_current_panel_as_proposal_population() -> Result<()> {
        let candidate = json!({"candidateId":"candidate-a"});
        assert_eq!(
            make_cohort(
                1,
                "panel-a",
                "proposal_current_panel",
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                vec![candidate.clone()]
            )?["proposalPopulation"],
            true
        );
        assert_eq!(
            make_cohort(
                1,
                "panel-a",
                "retained_parent_current_panel",
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                vec![candidate.clone()]
            )?["proposalPopulation"],
            false
        );
        assert_eq!(
            make_cohort(
                1,
                "panel-a",
                "prior_panel_backfill",
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                vec![candidate]
            )?["proposalPopulation"],
            false
        );
        Ok(())
    }

    #[test]
    fn v5_rotating_panel_admission_distinguishes_current_and_prior_backfill() -> Result<()> {
        let mut rotating = json!({
            "schemaVersion":"temporal_qd_rotating_evidence_v1",
            "panels":[{"panelId":"panel-1"},{"panelId":"panel-2"}],
            "absoluteGenerationMapping":{
                "schemaVersion":"temporal_qd_absolute_panel_phase_v1",
                "firstGenerationIndex":1,
                "cycleLength":2,
                "mapping":"one_based_modulo_cycle"
            }
        });
        rotating["rotatingEvidenceSha256"] = Value::String(canonical_sha256(&rotating)?);

        validate_v5_rotating(&rotating, 2, "panel-2", "proposal_current_panel")?;
        validate_v5_rotating(&rotating, 2, "panel-1", "prior_panel_backfill")?;
        assert!(validate_v5_rotating(&rotating, 2, "panel-1", "proposal_current_panel").is_err());
        assert!(validate_v5_rotating(&rotating, 2, "panel-2", "prior_panel_backfill").is_err());
        assert!(validate_v5_rotating(&rotating, 2, "foreign", "prior_panel_backfill").is_err());
        Ok(())
    }
}
