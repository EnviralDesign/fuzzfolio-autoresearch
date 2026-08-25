//! Native campaign-output checkpoint boundary.
//!
//! A fresh run opens one committed campaign-input checkpoint and one packed
//! gateway receipt, admits every remote result exactly once, reduces the tail,
//! and commits three scientific payloads plus one campaign-output checkpoint.
//! Normal restart reopens that checkpoint; the retired source-build and
//! campaign-seal receipt chains are not part of the production path.

#![recursion_limit = "256"]

mod campaign_output;
pub use campaign_output::{
    CHECKPOINT_PATH as CAMPAIGN_OUTPUT_CHECKPOINT_PATH,
    CHECKPOINT_SCHEMA as CAMPAIGN_OUTPUT_CHECKPOINT_SCHEMA, CampaignOutputCheckpoint,
    MANIFEST_PATH as CAMPAIGN_OUTPUT_MANIFEST_PATH,
    MANIFEST_SCHEMA as CAMPAIGN_OUTPUT_MANIFEST_SCHEMA, OPERATION as CAMPAIGN_OUTPUT_OPERATION,
    RESULT_SCHEMA as CAMPAIGN_OUTPUT_RESULT_SCHEMA,
    execute_manifest as execute_campaign_output_manifest,
    open_checkpoint as open_campaign_output_checkpoint,
};

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use base64::Engine as _;
use flate2::{Compression, GzBuilder, read::GzDecoder};
use serde_json::{Map, Number, Value, json};
use sha2::{Digest, Sha256};
use temporal_qd_campaign_freeze::validate_precompiled_execution_receipt;
use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_json_bytes, canonical_json_line, canonical_sha256,
    canonical_sha256_without_object_field,
};

pub const CAMPAIGN_SEAL_SCHEMA: &str = "temporal_qd_campaign_seal_v1";
pub const TRANSACTION_SCHEMA: &str = "temporal_qd_generation_tail_transaction_v1";
pub const DIRECTIONAL_INDEX_PATH: &str = "tail-result-index-v4.json";
pub const INVENTORY_PATH: &str = "raw-result-inventory.jsonl";
pub const TAIL_MANIFEST_PATH: &str = "tail-reduction-manifest.json";

const DIRECTIONAL_ENTRY_SCHEMA: &str = "temporal_qd_tail_result_index_entry_v4";
const DIRECTIONAL_INDEX_SCHEMA: &str = "temporal_qd_tail_result_index_v4";
const DIRECTIONAL_TAIL_AUTHORITY_SCHEMA: &str = "temporal_qd_v5_directional_tail_authority_v1";
const RAW_ROTATING_PROVENANCE_SCHEMA: &str = "temporal_qd_v5_raw_rotating_provenance_v1";
const STAGE_SCHEMA: &str = "temporal_qd_tail_stage_projection_v1";
const ADMITTED_SCHEMA_V1: &str = "temporal_graph_candidate_window_result_v1";
const ADMITTED_SCHEMA_V2: &str = "temporal_graph_candidate_window_result_v2";
const REJECTED_SCHEMA_V1: &str = "temporal_graph_candidate_window_rejected_result_v1";
const JOB_SCHEMA_V2: &str = "temporal_graph_candidate_window_job_v2";

/// Terminal disposition after the exact candidate-window raw-result admission
/// that the campaign seal uses before it makes a task result restartable.
///
/// This is intentionally a small public seam for bounded gateway dispatchers:
/// callers supply one immutable task-manifest row and one worker material. It
/// does not construct a projection, accept an envelope, or relax any of the
/// campaign-seal evidence checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateWindowResultAdmission {
    Admitted,
    Rejected,
}

/// Admit one terminal candidate-window material against its immutable task
/// manifest row.
///
/// The source task/binding is reconstructed exactly from the native freezer's
/// task row, then passed through the same binding, v3 evidence, worker
/// artifact, and warmup-rejection validators used by `build_index_and_inventory`.
/// This lets an online dispatcher durably admit an individual result without
/// accumulating a whole raw-result collection in memory.
pub fn admit_candidate_window_task_result(
    task_manifest_row: &Value,
    material: &Value,
) -> Result<CandidateWindowResultAdmission> {
    let source_task = source_task_from_manifest_row(task_manifest_row)?;
    let material_map = object(material, "candidate-window task material")?;
    verify_result_binding(material, &source_task)?;
    match text(material_map, "schema_version")?.as_str() {
        ADMITTED_SCHEMA_V1 | ADMITTED_SCHEMA_V2 => {
            validate_admitted_candidate_window_result(material, &source_task)?;
            Ok(CandidateWindowResultAdmission::Admitted)
        }
        REJECTED_SCHEMA_V1 => {
            validate_warmup_rejected_candidate_window_result(material, &source_task)?;
            Ok(CandidateWindowResultAdmission::Rejected)
        }
        schema => bail!("candidate-window material schema is not admitted: {schema}"),
    }
}

#[derive(Clone, Debug)]
struct Manifest {
    runtime_authority_sha256: String,
    evaluation_path: PathBuf,
    evaluation_sha256: String,
    generation_index: u64,
    minimum_total_trades: i64,
    minimum_trades_per_window: i64,
    cap_trades: i64,
    provisional_limit: usize,
    manifest_sha256: String,
}

#[derive(Clone, Debug)]
struct SourceTask {
    manifest_row: Value,
    task: Value,
    task_payload: Value,
    raw_result_path: PathBuf,
    raw_result_offset_bytes: Option<u64>,
    raw_result_length_bytes: Option<u64>,
    raw_ref: Value,
    binding: Value,
}

#[derive(Clone, Debug)]
struct Source {
    authority_id: String,
    authority_sha256: String,
    task_matrix_sha256: String,
    task_manifest_sha256: String,
    checkpoint_sha256: String,
    include_funnel: bool,
    tasks: Vec<SourceTask>,
    source_sha256: String,
}

#[derive(Debug, Default)]
struct ReadMetrics {
    blob_bytes: u64,
    uncompressed_bytes: u64,
    semantic_bytes: u64,
    raw_reads: u64,
}

fn read_canonical_value(path: &Path, label: &str) -> Result<Value> {
    let path = existing_file(path, label)?;
    let raw = fs::read(path)?;
    let value: Value = serde_json::from_slice(&raw).with_context(|| format!("parse {label}"))?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "{label} must be canonical JSON plus LF"
    );
    Ok(value)
}

fn sha_file(path: &Path) -> Result<String> {
    let path = existing_file(path, "receipt-bound file")?;
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
    Ok(format!("sha256:{:x}", hash.finalize()))
}

fn safe_task_id(value: &str) -> Result<String> {
    ensure!(
        value.len() <= 160
            && value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && value
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric()
                    || matches!(byte, b'.' | b'_' | b':' | b'-')),
        "gateway task id is unsafe"
    );
    Ok(value.to_owned())
}

/// Reconstruct the compact `SourceTask` projection used by the campaign seal
/// from exactly one row emitted by the native task-matrix freezer.  Keeping
/// this private prevents a dispatcher from manufacturing alternate bindings;
/// the public admission function above is the only supported entry point.
fn source_task_from_manifest_row(task_manifest_row: &Value) -> Result<SourceTask> {
    let row = object(task_manifest_row, "immutable task manifest row")?;
    exact_keys(
        row,
        &[
            "task_id",
            "lane_id",
            "attempt_id",
            "task_kind",
            "payload",
            "required_worker_capabilities",
            "deadline_seconds",
            "max_attempts",
        ],
        "immutable task manifest row",
    )?;
    let task_id = safe_identifier(field(row, "task_id")?, "task manifest task_id")?;
    ensure!(
        field(row, "attempt_id")? == &Value::String(task_id.clone()),
        "task manifest attempt_id must equal task_id"
    );
    ensure!(
        field(row, "task_kind")? == &Value::String("temporal_graph_candidate_window".into()),
        "task manifest task_kind is incompatible"
    );
    safe_identifier(field(row, "lane_id")?, "task manifest lane_id")?;
    nonnegative_integral_number(row, "deadline_seconds")?;
    unsigned(row, "max_attempts")?;
    ensure!(
        field(row, "required_worker_capabilities")?.is_array(),
        "task manifest required_worker_capabilities must be an array"
    );

    let payload = field(row, "payload")?.clone();
    let payload_map = object(&payload, "immutable task manifest payload")?;
    ensure!(
        field(payload_map, "job_id")? == &Value::String(task_id.clone()),
        "task manifest payload job_id must equal task_id"
    );
    safe_identifier(field(payload_map, "job_id")?, "task manifest job_id")?;
    let candidate_id = text(payload_map, "candidate_id")?;
    let authority_id = text(payload_map, "authority_id")?;
    let lake_window_semantic_sha256 = text(payload_map, "lake_window_semantic_sha256")?;
    let shared_observation_stream_id = text(payload_map, "shared_observation_stream_id")?;
    let analysis_window_start = text(payload_map, "analysis_window_start")?;
    let analysis_window_end = text(payload_map, "analysis_window_end")?;
    let evidence_plan = object(
        field(payload_map, "evidence_plan")?,
        "immutable task manifest evidence plan",
    )?;
    let evidence_plan_id = sha_field(evidence_plan, "plan_id")?;
    nonnegative_value(
        field(payload_map, "bar_limit")?,
        "task manifest bar_limit",
        1,
    )?;

    let task_payload_sha256 = canonical_sha256(&payload)?;
    let task = json!({
        "taskId": task_id,
        "candidateId": candidate_id,
        "analysisWindowStart": analysis_window_start,
        "analysisWindowEnd": analysis_window_end,
        "evidencePlanSemanticSha256": evidence_plan_id,
        "taskPayloadSha256": task_payload_sha256,
    });
    let binding = json!({
        "taskKind": "temporal_graph_candidate_window",
        "jobId": payload_map.get("job_id").cloned().expect("checked job_id"),
        "authorityId": authority_id,
        "candidateId": payload_map.get("candidate_id").cloned().expect("checked candidate_id"),
        "evidencePlanId": evidence_plan.get("plan_id").cloned().expect("checked plan_id"),
        "lakeWindowSemanticSha256": lake_window_semantic_sha256,
        "sharedObservationStreamId": shared_observation_stream_id,
    });
    Ok(SourceTask {
        manifest_row: task_manifest_row.clone(),
        task,
        task_payload: payload,
        // The online dispatcher does not have (or need) a raw gzip path/ref at
        // this admission point.  Those are created after material admission.
        raw_result_path: PathBuf::new(),
        raw_result_offset_bytes: None,
        raw_result_length_bytes: None,
        raw_ref: Value::Null,
        binding,
    })
}

fn build_index_and_inventory(source: &Source) -> Result<(Value, Vec<u8>, ReadMetrics)> {
    let mut entries = Vec::with_capacity(source.tasks.len());
    let mut inventory = Vec::new();
    let mut metrics = ReadMetrics::default();
    for source_task in &source.tasks {
        let mut file = File::open(&source_task.raw_result_path).with_context(|| {
            format!("open raw result: {}", source_task.raw_result_path.display())
        })?;
        if let Some(offset) = source_task.raw_result_offset_bytes {
            file.seek(SeekFrom::Start(offset)).with_context(|| {
                format!(
                    "seek packed raw result: {}",
                    source_task.raw_result_path.display()
                )
            })?;
        }
        metrics.raw_reads += 1;
        let reader: Box<dyn Read> = match source_task.raw_result_length_bytes {
            Some(length) => Box::new(file.take(length)),
            None => Box::new(file),
        };
        let mut decoder = GzDecoder::new(HashingReader::new(reader));
        let mut uncompressed = Vec::new();
        decoder
            .read_to_end(&mut uncompressed)
            .context("inflate raw result")?;
        let mut raw_reader = decoder.into_inner();
        // Include anything after the gzip trailer in the blob identity. A
        // suffix therefore fails the immutable representation binding.
        std::io::copy(&mut raw_reader, &mut std::io::sink())?;
        let (blob_sha256, blob_bytes) = raw_reader.finish();
        verify_raw_blob(&blob_sha256, blob_bytes, &source_task.raw_ref)?;
        metrics.blob_bytes = metrics
            .blob_bytes
            .checked_add(blob_bytes)
            .ok_or_else(|| anyhow!("blob byte overflow"))?;
        let result: Value = serde_json::from_slice(&uncompressed).context("parse raw result")?;
        let semantic = canonical_json_bytes(&result).context("canonicalize raw result")?;
        verify_uncompressed_and_semantic(&uncompressed, &semantic, &source_task.raw_ref)?;
        metrics.uncompressed_bytes = metrics
            .uncompressed_bytes
            .checked_add(uncompressed.len() as u64)
            .ok_or_else(|| anyhow!("uncompressed byte overflow"))?;
        metrics.semantic_bytes = metrics
            .semantic_bytes
            .checked_add(semantic.len() as u64)
            .ok_or_else(|| anyhow!("semantic byte overflow"))?;
        verify_result_binding(&result, source_task)?;
        match text(object(&result, "raw result")?, "schema_version")?.as_str() {
            ADMITTED_SCHEMA_V1 | ADMITTED_SCHEMA_V2 => {
                validate_admitted_candidate_window_result(&result, source_task)?;
            }
            REJECTED_SCHEMA_V1 => {
                validate_warmup_rejected_candidate_window_result(&result, source_task)?;
            }
            schema => bail!("candidate-window material schema is not admitted: {schema}"),
        }
        let entry = build_entry(
            &result,
            source_task,
            source.include_funnel,
            true,
            DIRECTIONAL_ENTRY_SCHEMA,
        )?;
        let inventory_row = json!({
            "schemaVersion": "temporal_qd_raw_result_inventory_entry_v1",
            "taskId": text(object(&source_task.task, "source task")?, "taskId")?,
            "rawResultRef": source_task.raw_ref,
        });
        inventory.extend_from_slice(&canonical_json_line(&inventory_row)?);
        entries.push(entry);
    }
    let mut index = json!({
        "schemaVersion": DIRECTIONAL_INDEX_SCHEMA,
        "authorityId": source.authority_id,
        "authoritySha256": source.authority_sha256,
        "taskMatrixSha256": source.task_matrix_sha256,
        "taskManifestSha256": source.task_manifest_sha256,
        "checkpointSha256": source.checkpoint_sha256,
        "taskCount": entries.len(),
        "funnelProjectionIncluded": source.include_funnel,
        "sourceResultBlobBytes": metrics.blob_bytes,
        "entries": entries,
    });
    add_identity(&mut index, "tailResultIndexSha256")?;
    Ok((index, inventory, metrics))
}

fn validate_admitted_candidate_window_result(
    result: &Value,
    source_task: &SourceTask,
) -> Result<()> {
    let result_map = object(result, "admitted candidate-window result")?;
    let schema = text(result_map, "schema_version")?;
    match schema.as_str() {
        ADMITTED_SCHEMA_V1 => {
            let task_payload = object(
                &source_task.task_payload,
                "admitted candidate-window task payload",
            )?;
            ensure!(
                task_payload.get("schema_version").and_then(Value::as_str) != Some(JOB_SCHEMA_V2),
                "candidate-window result v1 is not admitted for task v2"
            );
            ensure!(
                !result_map.contains_key("precompiled_profile_execution_receipt")
                    && !result_map.contains_key("runtime_program_identity_attestation"),
                "candidate-window result v1 must not carry v2 execution authority"
            );
        }
        ADMITTED_SCHEMA_V2 => {
            ensure!(
                result_map.contains_key("precompiled_profile_execution_receipt"),
                "candidate-window result v2 lacks its precompiled execution receipt"
            );
            ensure!(
                result_map.contains_key("runtime_program_identity_attestation"),
                "candidate-window result v2 lacks its runtime program identity attestation"
            );
            validate_precompiled_execution_receipt(&source_task.manifest_row, result)
                .context("candidate-window result v2 receipt/runtime admission failed")?;
        }
        _ => bail!("raw admitted result schema is invalid: {schema}"),
    }
    validate_v3_candidate_window_result(
        result,
        Some(&source_task.task),
        Some(&source_task.task_payload),
    )?;
    validate_worker_material_identity(result)?;
    Ok(())
}

fn verify_raw_blob(blob_sha256: &str, blob_bytes: u64, raw_ref: &Value) -> Result<()> {
    let map = object(raw_ref, "raw result ref")?;
    ensure!(
        text(map, "schemaVersion")? == "temporal_qd_tail_raw_result_ref_v1",
        "raw result ref schema is incompatible"
    );
    ensure!(
        text(map, "codec")? == "gzip-json-v1",
        "raw result codec is unsupported"
    );
    ensure!(
        unsigned(map, "blobSizeBytes")? == blob_bytes,
        "raw result blob size drifted"
    );
    ensure!(
        sha_field(map, "blobSha256")? == blob_sha256,
        "raw result blob identity drifted"
    );
    Ok(())
}

fn verify_uncompressed_and_semantic(
    uncompressed: &[u8],
    semantic: &[u8],
    raw_ref: &Value,
) -> Result<()> {
    let map = object(raw_ref, "raw result ref")?;
    ensure!(
        unsigned(map, "uncompressedSizeBytes")? == uncompressed.len() as u64,
        "raw result uncompressed size drifted"
    );
    ensure!(
        sha_field(map, "uncompressedSha256")? == sha_bytes(uncompressed),
        "raw result uncompressed identity drifted"
    );
    ensure!(
        unsigned(map, "semanticSizeBytes")? == semantic.len() as u64,
        "raw result semantic size drifted"
    );
    ensure!(
        sha_field(map, "resultSha256")? == sha_bytes(semantic),
        "raw result semantic identity drifted"
    );
    Ok(())
}

fn verify_result_binding(result: &Value, source_task: &SourceTask) -> Result<()> {
    let result = object(result, "raw result")?;
    let binding = object(&source_task.binding, "raw result binding")?;
    let mapping = [
        ("task_kind", "taskKind"),
        ("job_id", "jobId"),
        ("authority_id", "authorityId"),
        ("candidate_id", "candidateId"),
        ("evidence_plan_id", "evidencePlanId"),
        ("lake_window_semantic_sha256", "lakeWindowSemanticSha256"),
        ("shared_observation_stream_id", "sharedObservationStreamId"),
    ];
    for (raw_key, binding_key) in mapping {
        ensure!(
            field(result, raw_key)? == field(binding, binding_key)?,
            "raw result binding drifted for {raw_key}"
        );
    }
    ensure!(
        field(result, "candidate_id")?
            == field(object(&source_task.task, "source task")?, "candidateId")?,
        "raw candidate identity drifted"
    );
    Ok(())
}

// This is deliberately kept beside the raw-result admission boundary rather
// than the projection code below.  A projection is not evidence validation:
// every admitted v3 result must satisfy the same contract Python enforced
// before a campaign seal can make the result restartable.
fn validate_v3_candidate_window_result(
    result: &Value,
    task: Option<&Value>,
    task_payload: Option<&Value>,
) -> Result<()> {
    let root = object(result, "candidate-window result")?;
    ensure!(
        candidate_window_is_v3(root),
        "candidate-window result is not Stage 5E7-v3 evidence"
    );
    let evidence = object(
        field(root, "evidence_contract")?,
        "worker material evidence_contract",
    )?;
    ensure!(
        text(evidence, "schema_version")? == "temporal_graph_candidate_window_evidence_contract_v1",
        "candidate-window v3 evidence contract schema is required"
    );
    let start = stamp(
        field(root, "analysis_window_start")?,
        "worker material analysis_window_start",
    )?;
    let end = stamp(
        field(root, "analysis_window_end")?,
        "worker material analysis_window_end",
    )?;
    ensure!(
        start < end,
        "candidate-window analysis interval must be half-open and nonempty"
    );
    ensure!(
        stamp(
            field(evidence, "analysis_window_start")?,
            "evidence analysis_window_start"
        )? == start
            && stamp(
                field(evidence, "analysis_window_end")?,
                "evidence analysis_window_end"
            )? == end
            && field(evidence, "analysis_window_end_exclusive")? == &Value::Bool(true),
        "candidate-window evidence interval is incomplete or inconsistent"
    );
    if let Some(task) = task {
        let task = object(task, "source task identity")?;
        ensure!(
            stamp(
                field(task, "analysisWindowStart")?,
                "task analysis_window_start"
            )? == start
                && stamp(
                    field(task, "analysisWindowEnd")?,
                    "task analysis_window_end"
                )? == end,
            "candidate-window result interval does not match task"
        );
    }

    let source_profile = sha_value(
        field(root, "source_profile_snapshot_sha256")?,
        "worker material source_profile_snapshot_sha256",
    )?;
    let resolved_profile = sha_value(
        field(root, "resolved_profile_snapshot_sha256")?,
        "worker material resolved_profile_snapshot_sha256",
    )?;
    let resolved_program = sha_value(
        field(root, "program_sha256")?,
        "worker material program_sha256",
    )?;
    let requested = nonnegative_value(
        field(evidence, "requested_bar_limit")?,
        "evidence requested_bar_limit",
        1,
    )?;
    let effective = nonnegative_value(
        field(evidence, "effective_bar_limit")?,
        "evidence effective_bar_limit",
        1,
    )?;
    ensure!(
        effective >= requested,
        "candidate-window effective bar limit is below requested limit"
    );
    if let Some(task) = task_payload {
        let task = object(task, "source immutable task payload")?;
        ensure!(
            nonnegative_value(field(task, "bar_limit")?, "task bar_limit", 1)? == requested,
            "candidate-window requested bar limit does not match task"
        );
    }
    let count = nonnegative_value(
        field(evidence, "observation_count")?,
        "evidence observation_count",
        1,
    )?;
    let first = stamp(
        field(evidence, "first_admitted_observation_timestamp")?,
        "evidence first observation",
    )?;
    let last = stamp(
        field(evidence, "last_admitted_observation_timestamp")?,
        "evidence last observation",
    )?;
    ensure!(
        start <= first && first <= last && last < end,
        "candidate-window admitted observation endpoints are not complete half-open evidence"
    );
    let warmup = object(
        field(evidence, "warmup_sufficiency")?,
        "evidence warmup_sufficiency",
    )?;
    ensure!(
        field(evidence, "warmup_sufficient")? == &Value::Bool(true)
            && warmup.get("sufficient") == Some(&Value::Bool(true)),
        "candidate-window strict warmup evidence is insufficient"
    );
    ensure!(
        warmup.get("source").and_then(Value::as_str) != Some("prebuilt_stream"),
        "candidate-window strict warmup evidence must be measured, not a prebuilt-stream fallback"
    );
    let excluded_provisional = nonnegative_value(
        field(evidence, "excluded_provisional_count")?,
        "evidence excluded_provisional_count",
        0,
    )?;
    let excluded_outside = nonnegative_value(
        field(evidence, "excluded_outside_analysis_window_count")?,
        "evidence excluded_outside_analysis_window_count",
        0,
    )?;
    let summary = object(
        field(root, "observation_summary")?,
        "worker material observation_summary",
    )?;
    ensure!(
        nonnegative_value(
            field(summary, "observation_count")?,
            "observation summary count",
            1
        )? == count
            && stamp(
                field(summary, "first_bar_start")?,
                "observation summary first"
            )? == first
            && stamp(
                field(summary, "last_bar_start")?,
                "observation summary last"
            )? == last,
        "candidate-window actual observation evidence disagrees with contract"
    );
    let diagnostics = object(field(root, "diagnostics")?, "worker material diagnostics")?;
    for (key, expected) in [
        ("observation_count", Value::Number(count.into())),
        ("requested_bar_limit", Value::Number(requested.into())),
        ("effective_bar_limit", Value::Number(effective.into())),
        ("warmup_sufficient", Value::Bool(true)),
        ("warmup_sufficiency", Value::Object(warmup.clone())),
        (
            "first_admitted_observation_timestamp",
            Value::String(
                stamp(
                    field(evidence, "first_admitted_observation_timestamp")?,
                    "evidence first observation",
                )?
                .render(),
            ),
        ),
        (
            "last_admitted_observation_timestamp",
            Value::String(
                stamp(
                    field(evidence, "last_admitted_observation_timestamp")?,
                    "evidence last observation",
                )?
                .render(),
            ),
        ),
        (
            "excluded_provisional_count",
            Value::Number(excluded_provisional.into()),
        ),
        (
            "excluded_outside_analysis_window_count",
            Value::Number(excluded_outside.into()),
        ),
    ] {
        // Python compares canonicalized timestamp strings here, so preserve
        // exact JSON equality for all non-stamp values and normalized stamps.
        if key.ends_with("timestamp") {
            let actual = stamp(field(diagnostics, key)?, &format!("diagnostics {key}"))?;
            let wanted = match expected {
                Value::String(value) => value,
                _ => unreachable!(),
            };
            ensure!(
                actual.render() == wanted,
                "candidate-window diagnostics {key} does not match evidence contract"
            );
        } else {
            ensure!(
                field(diagnostics, key)? == &expected,
                "candidate-window diagnostics {key} does not match evidence contract"
            );
        }
    }

    let root_stream = sha_value(
        field(root, "observation_stream_sha256")?,
        "worker material observation_stream_sha256",
    )?;
    let cost_results = object(
        field(root, "cost_view_results")?,
        "worker material cost_view_results",
    )?;
    ensure!(
        cost_results.len() == 2
            && cost_results.contains_key("research_conservative")
            && cost_results.contains_key("none"),
        "candidate-window v3 result requires both cost views"
    );
    let mut paths = Vec::with_capacity(2);
    for view in ["research_conservative", "none"] {
        let item = object(
            field(cost_results, view)?,
            &format!("worker {view} cost view"),
        )?;
        if let Some(label) = item.get("cost_view") {
            ensure!(
                label == &Value::String(view.into()),
                "candidate-window {view} cost view label is inconsistent"
            );
        }
        ensure!(
            sha_value(
                field(item, "observation_stream_sha256")?,
                &format!("{view} cost-view stream")
            )? == root_stream,
            "candidate-window cost view observation identity mismatch"
        );
        let replay = object(
            field(item, "replay_result")?,
            &format!("{view} replay result"),
        )?;
        ensure!(
            sha_value(
                field(replay, "streamSha256")?,
                &format!("{view} replay stream")
            )? == root_stream,
            "candidate-window replay observation identity mismatch"
        );
        ensure!(
            sha_value(
                field(replay, "profileSnapshotSha256")?,
                &format!("{view} replay profile snapshot")
            )? == resolved_profile,
            "candidate-window replay resolved profile identity mismatch"
        );
        ensure!(
            sha_value(
                field(replay, "programSha256")?,
                &format!("{view} replay program")
            )? == resolved_program,
            "candidate-window replay program identity mismatch"
        );
        let metrics = object(field(replay, "metrics")?, &format!("{view} metrics"))?;
        ensure!(
            nonnegative_value(
                field(metrics, "observationsProcessed")?,
                &format!("{view} observationsProcessed"),
                0
            )? == count,
            "candidate-window replay observation count disagrees with evidence"
        );
        validate_terminal_metrics(metrics, view, last)?;
        paths.push(cost_view_path_sha256(replay, &format!("{view} replay"))?);
    }
    ensure!(
        paths[0] == paths[1],
        "candidate-window cost views diverged in non-cost route/path evidence"
    );
    ensure!(
        field(diagnostics, "cost_view_decision_path_sha256")? == &Value::String(paths[0].clone())
            && field(diagnostics, "cost_view_path_parity")? == &Value::String("matched".into())
            && field(diagnostics, "cost_view_count")? == &Value::Number(2.into())
            && field(diagnostics, "shared_stream_required")? == &Value::Bool(true),
        "candidate-window cost-view parity diagnostics are incomplete or inconsistent"
    );
    // Keep the authored source profile identity checked even though it is
    // intentionally distinct from the executable resolved profile/program.
    let _ = source_profile;
    Ok(())
}

/// `_result_material` in the Python oracle performs these final admission
/// checks after the v3 semantic validator.  The seal consumes the material
/// itself (not its worker envelope), so routing was already bound by
/// `verify_result_binding`; this preserves the artifact identity checks.
fn validate_worker_material_identity(result: &Value) -> Result<()> {
    let root = object(result, "worker material result")?;
    let supplied_sha = sha_value(
        field(root, "artifact_sha256")?,
        "worker material artifact_sha256",
    )?;
    let supplied_size = nonnegative_value(
        field(root, "artifact_size_bytes")?,
        "worker material artifact_size_bytes",
        1,
    )?;
    ensure!(
        canonical_json_bytes(result)?.len() as u64 == supplied_size,
        "worker material artifact byte count mismatch"
    );
    let mut identity = root.clone();
    identity.remove("artifact_sha256");
    identity.remove("artifact_size_bytes");
    let diagnostics = object(
        field(&identity, "diagnostics")?,
        "worker material diagnostics",
    )?;
    let mut diagnostics = diagnostics.clone();
    diagnostics.remove("artifact_size_bytes");
    identity.insert("diagnostics".into(), Value::Object(diagnostics));
    ensure!(
        canonical_sha256(&Value::Object(identity))? == supplied_sha,
        "worker material artifact identity mismatch"
    );
    Ok(())
}

/// Exact port of Python's `validate_warmup_rejected_candidate_window_result`.
/// A rejected row is not a partial replay: it has a tightly bounded outcome
/// schema, immutable task provenance, and its own self-hashed artifact form.
fn validate_warmup_rejected_candidate_window_result(
    result: &Value,
    source_task: &SourceTask,
) -> Result<()> {
    let material = object(result, "warmup rejection material")?;
    ensure!(
        material.get("schema_version") == Some(&Value::String(REJECTED_SCHEMA_V1.into())),
        "candidate-window result is not a warmup rejection"
    );
    ensure!(
        material.get("task_kind") == Some(&Value::String("temporal_graph_candidate_window".into())),
        "warmup rejection task kind is invalid"
    );
    let outcome = object(
        field(material, "evaluation_outcome")?,
        "warmup rejection outcome",
    )?;
    let common = [
        "schema_version",
        "disposition",
        "reason_code",
        "replay_executed",
        "worker_attempt_id",
        "worker_lease_id",
        "worker_error",
        "worker_error_sha256",
        "worker_completion_sha256",
    ];
    let v1 = outcome.get("schema_version")
        == Some(&Value::String(
            "temporal_candidate_window_rejection_v1".into(),
        ));
    let v2 = outcome.get("schema_version")
        == Some(&Value::String(
            "temporal_candidate_window_rejection_v2".into(),
        ));
    let expected_len = common.len() + usize::from(v2);
    ensure!(
        (v1 || v2)
            && outcome.len() == expected_len
            && common.iter().all(|key| outcome.contains_key(*key))
            && (!v2 || outcome.contains_key("replay_completed"))
            && outcome.get("disposition") == Some(&Value::String("rejected".into())),
        "warmup rejection outcome is invalid"
    );
    ensure!(
        (v1 && outcome.get("reason_code")
            == Some(&Value::String("aligned_scoring_warmup_insufficient".into()))
            && outcome.get("replay_executed") == Some(&Value::Bool(false)))
            || (v2
                && outcome.get("reason_code")
                    == Some(&Value::String(
                        "duplicate_break_even_execution_invariant".into()
                    ))
                && outcome.get("replay_executed") == Some(&Value::Bool(true))
                && outcome.get("replay_completed") == Some(&Value::Bool(false))),
        "rejection replay execution state is invalid"
    );
    safe_identifier(
        field(outcome, "worker_attempt_id")?,
        "warmup rejection worker attempt",
    )?;
    safe_identifier(
        field(outcome, "worker_lease_id")?,
        "warmup rejection worker lease",
    )?;
    let error = field(outcome, "worker_error")?;
    ensure!(
        canonical_sha256(error)?
            == sha_value(
                field(outcome, "worker_error_sha256")?,
                "warmup rejection error hash",
            )?,
        "warmup rejection error identity mismatch"
    );
    sha_value(
        field(outcome, "worker_completion_sha256")?,
        "warmup rejection completion hash",
    )?;

    let binding = object(&source_task.binding, "warmup rejection task binding")?;
    let task = object(&source_task.task, "warmup rejection source task")?;
    for (material_key, binding_key) in [
        ("job_id", "jobId"),
        ("authority_id", "authorityId"),
        ("candidate_id", "candidateId"),
        ("evidence_plan_id", "evidencePlanId"),
        ("lake_window_semantic_sha256", "lakeWindowSemanticSha256"),
        ("shared_observation_stream_id", "sharedObservationStreamId"),
    ] {
        text(material, material_key)?;
        ensure!(
            field(material, material_key)? == field(binding, binding_key)?,
            "warmup rejection does not match task"
        );
    }
    for (material_key, task_key) in [
        ("analysis_window_start", "analysisWindowStart"),
        ("analysis_window_end", "analysisWindowEnd"),
    ] {
        text(material, material_key)?;
        ensure!(
            field(material, material_key)? == field(task, task_key)?,
            "warmup rejection does not match task"
        );
    }
    let mut artifact = material.clone();
    let supplied_sha = sha_value(
        artifact.remove("artifact_sha256").as_ref().ok_or_else(|| {
            anyhow!("warmup rejection artifact hash must be an exact sha256 identity")
        })?,
        "warmup rejection artifact hash",
    )?;
    let supplied_size = nonnegative_value(
        artifact
            .remove("artifact_size_bytes")
            .as_ref()
            .ok_or_else(|| anyhow!("warmup rejection artifact byte count is invalid"))?,
        "warmup rejection artifact byte count",
        1,
    )?;
    ensure!(
        canonical_sha256(&Value::Object(artifact))? == supplied_sha,
        "warmup rejection artifact identity mismatch"
    );
    ensure!(
        canonical_json_bytes(result)?.len() as u64 == supplied_size,
        "warmup rejection artifact byte count mismatch"
    );
    Ok(())
}

fn safe_identifier(value: &Value, name: &str) -> Result<String> {
    let value = python_string(Some(value)).trim().to_owned();
    ensure!(
        !value.is_empty()
            && value.len() <= 160
            && value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && value.as_bytes().iter().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            }),
        "{name} must be a safe explicit identifier"
    );
    Ok(value)
}

fn candidate_window_is_v3(root: &Map<String, Value>) -> bool {
    root.contains_key("evidence_contract")
        || root
            .get("cost_view_results")
            .and_then(Value::as_object)
            .is_some_and(|views| {
                views.values().any(|item| {
                    item.as_object()
                        .and_then(|row| row.get("replay_result"))
                        .and_then(Value::as_object)
                        .and_then(|r| r.get("metrics"))
                        .and_then(Value::as_object)
                        .is_some_and(|metrics| {
                            [
                                "terminalValuation",
                                "terminalAdjustedTotalGrossR",
                                "terminalAdjustedTotalNetR",
                                "terminalAdjustedTotalExecutionCostPercent",
                                "terminalAdjustedEquityCurveR",
                                "terminalAdjustedMaxDrawdownR",
                            ]
                            .iter()
                            .any(|key| metrics.contains_key(*key))
                        })
                })
            })
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Stamp {
    microseconds: i128,
}
impl Stamp {
    fn render(self) -> String {
        self.microseconds.to_string()
    }
}

// Python accepts ISO-8601 offsets then compares UTC instants.  The campaign
// emits canonical Z timestamps, but this parser deliberately accepts the same
// offset form so admission cannot be bypassed by an alternate spelling.
fn stamp(value: &Value, name: &str) -> Result<Stamp> {
    let raw = value
        .as_str()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| anyhow!("{name} must be an ISO timestamp"))?;
    let separator = raw
        .char_indices()
        .find(|(_, value)| matches!(value, 'T' | 't' | ' '))
        .map(|(index, _)| index)
        .ok_or_else(|| anyhow!("{name} must be an ISO timestamp"))?;
    let (date, time_zone_with_separator) = raw.split_at(separator);
    let time_zone = &time_zone_with_separator[1..];
    ensure!(
        date.len() == 10 && date.as_bytes()[4] == b'-' && date.as_bytes()[7] == b'-',
        "{name} must be an ISO timestamp"
    );
    let mut d = date.split('-');
    let year: i64 = d
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| anyhow!("{name} must be an ISO timestamp"))?;
    let month: u32 = d
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| anyhow!("{name} must be an ISO timestamp"))?;
    let day: u32 = d
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| anyhow!("{name} must be an ISO timestamp"))?;
    ensure!(
        d.next().is_none()
            && (1..=12).contains(&month)
            && day >= 1
            && day <= days_in_month(year, month),
        "{name} must be an ISO timestamp"
    );
    let (time, offset) = if let Some(time) = time_zone
        .strip_suffix('Z')
        .or_else(|| time_zone.strip_suffix('z'))
    {
        (time, 0i64)
    } else {
        let pos = time_zone
            .rfind(['+', '-'])
            .ok_or_else(|| anyhow!("{name} must include a timezone"))?;
        let (time, zone) = time_zone.split_at(pos);
        let sign = if zone.starts_with('+') { 1 } else { -1 };
        let mut z = zone[1..].split(':');
        let zh: i64 = z
            .next()
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| anyhow!("{name} must be an ISO timestamp"))?;
        let zm: i64 = z
            .next()
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| anyhow!("{name} must be an ISO timestamp"))?;
        ensure!(
            z.next().is_none() && zh <= 23 && zm <= 59,
            "{name} must be an ISO timestamp"
        );
        (time, sign * (zh * 3600 + zm * 60))
    };
    ensure!(
        time.len() >= 8 && time.as_bytes()[2] == b':' && time.as_bytes()[5] == b':',
        "{name} must be an ISO timestamp"
    );
    let mut t = time.split(':');
    let hour: i64 = t
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| anyhow!("{name} must be an ISO timestamp"))?;
    let minute: i64 = t
        .next()
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| anyhow!("{name} must be an ISO timestamp"))?;
    let second_part = t
        .next()
        .ok_or_else(|| anyhow!("{name} must be an ISO timestamp"))?;
    ensure!(
        t.next().is_none() && hour < 24 && minute < 60,
        "{name} must be an ISO timestamp"
    );
    let (second_raw, fractional_microseconds) = match second_part.split_once('.') {
        Some((seconds, fraction)) => {
            ensure!(
                !seconds.is_empty()
                    && (fraction.is_empty() || fraction.len() == 3 || fraction.len() == 6)
                    && fraction.bytes().all(|byte| byte.is_ascii_digit()),
                "{name} must be an ISO timestamp"
            );
            let micros = match fraction.len() {
                0 => 0,
                3 => {
                    fraction
                        .parse::<i128>()
                        .map_err(|_| anyhow!("{name} must be an ISO timestamp"))?
                        * 1_000
                }
                6 => fraction
                    .parse::<i128>()
                    .map_err(|_| anyhow!("{name} must be an ISO timestamp"))?,
                _ => unreachable!(),
            };
            (seconds, micros)
        }
        None => (second_part, 0),
    };
    ensure!(second_raw.len() == 2, "{name} must be an ISO timestamp");
    let second: i64 = second_raw
        .parse()
        .map_err(|_| anyhow!("{name} must be an ISO timestamp"))?;
    ensure!(second < 60, "{name} must be an ISO timestamp");
    let utc_seconds =
        days_from_civil(year, month, day) * 86400 + hour * 3600 + minute * 60 + second - offset;
    Ok(Stamp {
        microseconds: i128::from(utc_seconds) * 1_000_000 + fractional_microseconds,
    })
}
fn leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}
fn days_in_month(year: i64, month: u32) -> u32 {
    [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ][(month - 1) as usize]
}
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = y - if m <= 2 { 1 } else { 0 };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = m as i64 + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn nonnegative_value(value: &Value, name: &str, minimum: u64) -> Result<u64> {
    value
        .as_u64()
        .filter(|v| *v >= minimum)
        .ok_or_else(|| anyhow!("{name} must be an integer greater than or equal to {minimum}"))
}
fn sha_value(value: &Value, name: &str) -> Result<String> {
    let map = Map::from_iter([(name.to_owned(), value.clone())]);
    sha_field(&map, name)
}
fn numeric(value: &Value, name: &str) -> Result<f64> {
    if value.is_boolean() {
        return Err(anyhow!("{name} must be numeric"));
    }
    let parsed = value
        .as_f64()
        .or_else(|| value.as_str().and_then(|v| v.parse().ok()))
        .filter(|v: &f64| v.is_finite());
    parsed.ok_or_else(|| anyhow!("{name} must be finite"))
}
fn same_number(left: f64, right: f64, name: &str) -> Result<()> {
    ensure!(
        (left - right).abs() <= 1e-9f64.max(1e-9 * left.abs().max(right.abs())),
        "{name} is inconsistent"
    );
    Ok(())
}

fn validate_terminal_metrics(
    metrics: &Map<String, Value>,
    view: &str,
    expected_last: Stamp,
) -> Result<()> {
    let label = format!("{view} metrics");
    let terminal = object(
        field(metrics, "terminalValuation")?,
        &format!("{label}.terminalValuation"),
    )?;
    ensure!(
        terminal.get("schemaVersion")
            == Some(&Value::String("temporal_terminal_valuation_v1".into())),
        "{label} terminal valuation schema is required"
    );
    ensure!(
        terminal.get("policy") == Some(&Value::String("leave_open_mark_to_market_v1".into())),
        "{label} terminal valuation policy is required"
    );
    for key in [
        "lastCompletedBarId",
        "lastCompletedBarStart",
        "lastCompletedBarClose",
    ] {
        ensure!(
            terminal
                .get(key)
                .and_then(Value::as_str)
                .is_some_and(|v| !v.trim().is_empty()),
            "{label} terminal valuation {key} is required"
        );
    }
    ensure!(
        stamp(
            field(terminal, "lastCompletedBarStart")?,
            &format!("{label}.terminalValuation.lastCompletedBarStart")
        )? == expected_last,
        "{label} terminal valuation endpoint disagrees with evidence"
    );
    stamp(
        field(terminal, "lastCompletedBarClose")?,
        &format!("{label}.terminalValuation.lastCompletedBarClose"),
    )?;
    let status = text(terminal, "positionStatus")?;
    ensure!(
        matches!(status.as_str(), "no_open_position" | "open_position_marked"),
        "{label} terminal position status is invalid"
    );
    let pending = text(terminal, "pendingEffectStatus")?;
    ensure!(
        matches!(pending.as_str(), "none" | "unresolved"),
        "{label} terminal pending-effect status is invalid"
    );
    ensure!(
        terminal.get("closedTradeCountDelta") == Some(&Value::Number(0.into())),
        "{label} terminal valuation must not add closed trades"
    );
    ensure!(
        numeric(
            field(terminal, "markPrice")?,
            &format!("{label}.terminalValuation.markPrice")
        )? > 0.0,
        "{label} terminal mark price must be positive"
    );
    let unresolved_position = field(metrics, "unresolvedPosition")?
        .as_bool()
        .ok_or_else(|| anyhow!("{label} unresolved status flags are required"))?;
    let unresolved_pending = field(metrics, "unresolvedPendingEffect")?
        .as_bool()
        .ok_or_else(|| anyhow!("{label} unresolved status flags are required"))?;
    let has_position = status == "open_position_marked";
    ensure!(
        unresolved_position == has_position,
        "{label} terminal position status disagrees with replay"
    );
    let pending_unresolved = pending == "unresolved";
    ensure!(
        unresolved_pending == pending_unresolved,
        "{label} terminal pending status disagrees with replay"
    );
    let treatment = if pending_unresolved {
        "canceled_for_terminal_valuation_only"
    } else {
        "not_applicable"
    };
    ensure!(
        terminal.get("pendingEffectCancellationTreatment")
            == Some(&Value::String(treatment.into())),
        "{label} terminal pending treatment is inconsistent"
    );
    let raw_gross = numeric(
        field(metrics, "totalGrossR")?,
        &format!("{label}.totalGrossR"),
    )?;
    let raw_net = numeric(field(metrics, "totalNetR")?, &format!("{label}.totalNetR"))?;
    let raw_cost = numeric(
        field(metrics, "totalExecutionCostPercent")?,
        &format!("{label}.totalExecutionCostPercent"),
    )?;
    let adjusted_gross = numeric(
        field(metrics, "terminalAdjustedTotalGrossR")?,
        &format!("{label}.terminalAdjustedTotalGrossR"),
    )?;
    let adjusted_net = numeric(
        field(metrics, "terminalAdjustedTotalNetR")?,
        &format!("{label}.terminalAdjustedTotalNetR"),
    )?;
    let adjusted_cost = numeric(
        field(metrics, "terminalAdjustedTotalExecutionCostPercent")?,
        &format!("{label}.terminalAdjustedTotalExecutionCostPercent"),
    )?;
    let adjusted_drawdown = numeric(
        field(metrics, "terminalAdjustedMaxDrawdownR")?,
        &format!("{label}.terminalAdjustedMaxDrawdownR"),
    )?;
    ensure!(
        raw_cost >= 0.0 && adjusted_cost >= 0.0 && adjusted_drawdown >= 0.0,
        "{label} terminal economics must be nonnegative where required"
    );
    let curve = field(metrics, "terminalAdjustedEquityCurveR")?
        .as_array()
        .ok_or_else(|| anyhow!("{label}.terminalAdjustedEquityCurveR must be an array"))?;
    ensure!(
        !curve.is_empty(),
        "{label}.terminalAdjustedEquityCurveR must be a nonempty array"
    );
    let mut peak: f64 = 0.0;
    let mut dd: f64 = 0.0;
    let mut last: f64 = 0.0;
    for (index, value) in curve.iter().enumerate() {
        let value = numeric(
            value,
            &format!("{label}.terminalAdjustedEquityCurveR[{index}]"),
        )?;
        peak = peak.max(value);
        dd = dd.max(peak - value);
        last = value;
    }
    same_number(
        last,
        adjusted_net,
        &format!("{label} terminal adjusted equity end"),
    )?;
    same_number(
        dd,
        adjusted_drawdown,
        &format!("{label} terminal adjusted drawdown"),
    )?;
    let (terminal_gross, terminal_net, exit_cost) = if has_position {
        for key in ["positionId", "direction", "grossR", "netR"] {
            ensure!(
                terminal.get(key).is_some_and(|v| !v.is_null()),
                "{label} terminal open position is incomplete"
            );
        }
        sha_value(
            field(terminal, "positionId")?,
            &format!("{label}.terminalValuation.positionId"),
        )?;
        ensure!(
            matches!(text(terminal, "direction")?.as_str(), "long" | "short"),
            "{label} terminal direction is invalid"
        );
        (
            numeric(
                field(terminal, "grossR")?,
                &format!("{label}.terminalValuation.grossR"),
            )?,
            numeric(
                field(terminal, "netR")?,
                &format!("{label}.terminalValuation.netR"),
            )?,
            numeric(
                field(terminal, "exitCostPercent")?,
                &format!("{label}.terminalValuation.exitCostPercent"),
            )?,
        )
    } else {
        ensure!(
            ["positionId", "direction", "grossR", "netR"]
                .iter()
                .all(|key| terminal.get(*key).is_none_or(Value::is_null)),
            "{label} no-position terminal valuation must be zero"
        );
        let exit = numeric(
            field(terminal, "exitCostPercent")?,
            &format!("{label}.terminalValuation.exitCostPercent"),
        )?;
        ensure!(
            exit == 0.0,
            "{label} no-position terminal valuation must not charge exit cost"
        );
        (0.0, 0.0, exit)
    };
    ensure!(
        exit_cost >= 0.0,
        "{label} terminal exit cost must be nonnegative"
    );
    same_number(
        raw_gross + terminal_gross,
        adjusted_gross,
        &format!("{label} terminal gross total"),
    )?;
    same_number(
        raw_net + terminal_net,
        adjusted_net,
        &format!("{label} terminal net total"),
    )?;
    same_number(
        raw_cost + exit_cost,
        adjusted_cost,
        &format!("{label} terminal cost total"),
    )
}

fn path_rows<'a>(
    value: Option<&'a Value>,
    name: &str,
    keys: &[&str],
    optional: &[&str],
) -> Result<Vec<&'a Map<String, Value>>> {
    let rows = value
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("{name} must be an array"))?;
    rows.iter()
        .enumerate()
        .map(|(i, row)| {
            let row = object(row, &format!("{name}[{i}]"))?;
            ensure!(
                keys.iter().all(|key| row.contains_key(*key)),
                "{name}[{i}] is missing non-cost path evidence"
            );
            if keys.contains(&"intentIds") {
                ensure!(
                    row.get("intentIds").and_then(Value::as_array).is_some(),
                    "{name}[{i}].intentIds must be an array"
                );
            }
            let _ = optional;
            Ok(row)
        })
        .collect()
}
fn pick(row: &Map<String, Value>, key: &str) -> Value {
    row.get(key).cloned().unwrap_or(Value::Null)
}
fn trailing_projection(row: Option<&Value>) -> Result<Value> {
    let Some(row) = row.filter(|value| !value.is_null()) else {
        return Ok(Value::Null);
    };
    let row = object(row, "trailing state")?;
    Ok(json!({
        "policy_sha256":pick(row,"policySha256"), "active":row.get("active").cloned().unwrap_or(Value::Bool(false)),
        "suspended":row.get("suspended").cloned().unwrap_or(Value::Bool(false)), "activation_count":row.get("activationCount").cloned().unwrap_or(json!(0)),
        "activation_clock_index":pick(row,"activationClockIndex"), "deactivation_count":row.get("deactivationCount").cloned().unwrap_or(json!(0)),
        "pending_stop_price":pick(row,"pendingStopPrice"), "pending_anchor_price":pick(row,"pendingAnchorPrice"), "pending_clock_index":pick(row,"pendingClockIndex"),
        "update_count":row.get("updateCount").cloned().unwrap_or(json!(0)), "last_applied_anchor_price":pick(row,"lastAppliedAnchorPrice"),
        "owns_current_stop":row.get("ownsCurrentStop").cloned().unwrap_or(Value::Bool(false)),
    }))
}
fn position_projection(row: Option<&Value>) -> Result<Value> {
    let Some(row) = row.filter(|value| !value.is_null()) else {
        return Ok(Value::Null);
    };
    let row = object(row, "final execution position")?;
    let mut out = Map::new();
    for (out_key, source) in [
        ("instrument", "instrument"),
        ("direction", "direction"),
        ("management_plan_id", "managementPlanId"),
        ("management_plan_sha256", "managementPlanSha256"),
        ("entry_bar_id", "entryBarId"),
        ("entry_time", "entryTime"),
        ("entry_clock_index", "entryClockIndex"),
        ("entry_price", "entryPrice"),
        ("stop_loss_percent", "stopLossPercent"),
        ("reward_multiple", "rewardMultiple"),
        ("take_profit_percent", "takeProfitPercent"),
        ("initial_stop_price", "initialStopPrice"),
        ("initial_target_price", "initialTargetPrice"),
        ("stop_price", "stopPrice"),
        ("target_price", "targetPrice"),
        ("last_management_clock_index", "lastManagementClockIndex"),
        ("max_favorable_excursion_r", "maxFavorableExcursionR"),
        ("max_adverse_excursion_r", "maxAdverseExcursionR"),
    ] {
        out.insert(out_key.into(), pick(row, source));
    }
    out.insert(
        "break_even_applied".into(),
        row.get("breakEvenApplied")
            .cloned()
            .unwrap_or(Value::Bool(false)),
    );
    out.insert(
        "stop_update_count".into(),
        row.get("stopUpdateCount").cloned().unwrap_or(json!(0)),
    );
    out.insert(
        "target_update_count".into(),
        row.get("targetUpdateCount").cloned().unwrap_or(json!(0)),
    );
    out.insert(
        "max_favorable_excursion_r".into(),
        row.get("maxFavorableExcursionR")
            .cloned()
            .unwrap_or(number(0.0)),
    );
    out.insert(
        "max_adverse_excursion_r".into(),
        row.get("maxAdverseExcursionR")
            .cloned()
            .unwrap_or(number(0.0)),
    );
    out.insert("trailing".into(), trailing_projection(row.get("trailing"))?);
    Ok(Value::Object(out))
}
fn pending_projection(row: Option<&Value>) -> Result<Value> {
    let Some(row) = row.filter(|value| !value.is_null()) else {
        return Ok(Value::Null);
    };
    let row = object(row, "final pending effect")?;
    let intent = object(field(row, "intent")?, "final pending effect intent")?;
    let parameters = object(
        intent
            .get("parameters")
            .unwrap_or(&Value::Object(Map::new())),
        "pending intent parameters",
    )?
    .clone();
    let scalars = object(
        row.get("scheduledManagementScalars")
            .unwrap_or(&Value::Object(Map::new())),
        "pending scalar snapshot",
    )?
    .clone();
    Ok(
        json!({"transition_id":pick(intent,"transitionId"),"action_ordinal":pick(intent,"actionOrdinal"),"action_kind":pick(intent,"actionKind"),"timing_class":pick(intent,"timingClass"),"parameters":parameters,"scheduled_clock_index":pick(row,"scheduledClockIndex"),"eligible_clock_index":pick(row,"eligibleClockIndex"),"expected_graph_state_id":pick(row,"expectedGraphStateId"),"scheduled_management_scalars":scalars}),
    )
}
fn final_execution_projection(row: Option<&Value>) -> Result<Value> {
    let Some(row) = row.filter(|value| !value.is_null()) else {
        return Ok(Value::Null);
    };
    let row = object(row, "final execution state")?;
    Ok(
        json!({"instrument":pick(row,"instrument"),"direction":pick(row,"direction"),"last_execution_reason":pick(row,"lastExecutionReason"),"last_close_reason":pick(row,"lastCloseReason"),"last_market_bar_id":pick(row,"lastMarketBarId"),"last_bar_start":pick(row,"lastBarStart"),"last_clock_index":pick(row,"lastClockIndex"),"position":position_projection(row.get("position"))?,"pending_effect":pending_projection(row.get("pendingEffect"))?}),
    )
}
fn cost_view_path_sha256(replay: &Map<String, Value>, name: &str) -> Result<String> {
    let graphs = path_rows(
        replay.get("graphTraces"),
        &format!("{name}.graphTraces"),
        &[
            "eventSequence",
            "eventClass",
            "priorStateId",
            "nextStateId",
            "transitionId",
            "reasonCode",
            "intentIds",
        ],
        &[],
    )?;
    let executions = path_rows(
        replay.get("executionTraces"),
        &format!("{name}.executionTraces"),
        &[
            "eventSequence",
            "clockIndex",
            "marketBarId",
            "phase",
            "effectKind",
            "status",
            "actionKind",
            "reasonCode",
            "price",
            "positionId",
            "tradeId",
        ],
        &[],
    )?;
    let trade_optional = [
        "managementPlanId",
        "managementPlanSha256",
        "stopLossPercent",
        "rewardMultiple",
        "takeProfitPercent",
        "initialStopPrice",
        "initialTargetPrice",
        "finalStopPrice",
        "targetPrice",
        "trailing",
        "breakEvenApplied",
        "stopUpdateCount",
        "targetUpdateCount",
        "lastManagementClockIndex",
        "maxFavorableExcursionR",
        "maxAdverseExcursionR",
    ];
    let trades = path_rows(
        replay.get("trades"),
        &format!("{name}.trades"),
        &[
            "direction",
            "entryBarId",
            "exitBarId",
            "entryPhase",
            "exitPhase",
            "entryTime",
            "exitTime",
            "entryClockIndex",
            "exitClockIndex",
            "entryPrice",
            "exitPrice",
            "closeReason",
            "holdingBars",
            "holdingHours",
        ],
        &trade_optional,
    )?;
    let graph_path: Vec<Value> = graphs.into_iter().map(|row| json!({"event_sequence":pick(row,"eventSequence"),"event_class":pick(row,"eventClass"),"prior_state_id":pick(row,"priorStateId"),"next_state_id":pick(row,"nextStateId"),"transition_id":pick(row,"transitionId"),"reason_code":pick(row,"reasonCode"),"intent_count":row.get("intentIds").and_then(Value::as_array).map_or(0, Vec::len)})).collect();
    let execution_path: Vec<Value> = executions.into_iter().map(|row| json!({"event_sequence":pick(row,"eventSequence"),"clock_index":pick(row,"clockIndex"),"market_bar_id":pick(row,"marketBarId"),"phase":pick(row,"phase"),"effect_kind":pick(row,"effectKind"),"status":pick(row,"status"),"action_kind":pick(row,"actionKind"),"reason_code":pick(row,"reasonCode"),"price":pick(row,"price"),"position_present":row.get("positionId").is_some_and(|v| !v.is_null()),"trade_present":row.get("tradeId").is_some_and(|v| !v.is_null())})).collect();
    let mut trade_path = Vec::with_capacity(trades.len());
    for row in trades {
        trade_path.push(json!({"direction":pick(row,"direction"),"management_plan_id":pick(row,"managementPlanId"),"management_plan_sha256":pick(row,"managementPlanSha256"),"entry_bar_id":pick(row,"entryBarId"),"exit_bar_id":pick(row,"exitBarId"),"entry_phase":pick(row,"entryPhase"),"exit_phase":pick(row,"exitPhase"),"entry_time":pick(row,"entryTime"),"exit_time":pick(row,"exitTime"),"entry_clock_index":pick(row,"entryClockIndex"),"exit_clock_index":pick(row,"exitClockIndex"),"entry_price":pick(row,"entryPrice"),"exit_price":pick(row,"exitPrice"),"stop_loss_percent":pick(row,"stopLossPercent"),"reward_multiple":pick(row,"rewardMultiple"),"take_profit_percent":pick(row,"takeProfitPercent"),"initial_stop_price":pick(row,"initialStopPrice"),"initial_target_price":pick(row,"initialTargetPrice"),"final_stop_price":pick(row,"finalStopPrice"),"target_price":pick(row,"targetPrice"),"trailing":trailing_projection(row.get("trailing"))?,"break_even_applied":row.get("breakEvenApplied").cloned().unwrap_or(Value::Bool(false)),"stop_update_count":row.get("stopUpdateCount").cloned().unwrap_or(Value::Number(0.into())),"target_update_count":row.get("targetUpdateCount").cloned().unwrap_or(Value::Number(0.into())),"last_management_clock_index":pick(row,"lastManagementClockIndex"),"max_favorable_excursion_r":row.get("maxFavorableExcursionR").cloned().unwrap_or(number(0.0)),"max_adverse_excursion_r":row.get("maxAdverseExcursionR").cloned().unwrap_or(number(0.0)),"close_reason":pick(row,"closeReason"),"holding_bars":pick(row,"holdingBars"),"holding_hours":pick(row,"holdingHours")}));
    }
    Ok(canonical_sha256(
        &json!({"schema_version":"temporal_graph_cost_view_path_v3","graph_path":graph_path,"execution_path":execution_path,"trade_path":trade_path,"final_execution_state":final_execution_projection(replay.get("finalExecutionState"))?}),
    )?)
}

fn build_entry(
    result: &Value,
    source_task: &SourceTask,
    include_funnel: bool,
    direction_aware: bool,
    entry_schema: &str,
) -> Result<Value> {
    let result_map = object(result, "raw result")?;
    let task = object(&source_task.task, "source task")?;
    let schema = text(result_map, "schema_version")?;
    ensure!(
        schema == ADMITTED_SCHEMA_V1
            || schema == ADMITTED_SCHEMA_V2
            || schema == REJECTED_SCHEMA_V1,
        "raw result schema is unsupported"
    );
    let record = window_record(result)?;
    ensure!(
        field(&record, "candidateId")? == field(task, "candidateId")?,
        "window record candidate binding drifted"
    );
    ensure!(
        field(&record, "analysisWindowStart")? == field(task, "analysisWindowStart")?,
        "window record start binding drifted"
    );
    ensure!(
        field(&record, "analysisWindowEnd")? == field(task, "analysisWindowEnd")?,
        "window record end binding drifted"
    );
    let raw_ref = object(&source_task.raw_ref, "raw result ref")?;
    let task_id = field(task, "taskId")?.clone();
    let result_sha = field(raw_ref, "resultSha256")?.clone();
    let mut entry = json!({
        "schemaVersion": entry_schema,
        "task": source_task.task,
        "rawResultRef": source_task.raw_ref,
        "rawTaskProvenance": { "taskId": task_id.clone(), "resultSha256": result_sha.clone() },
    });
    let entry_map = entry.as_object_mut().expect("entry is object");
    if record.get("evaluationRejected") == Some(&Value::Bool(true)) {
        entry_map.insert("rejection".into(), field(&record, "rejection")?.clone());
    } else {
        if direction_aware {
            let behavior = record
                .get("realizedBehavior")
                .ok_or_else(|| anyhow!("directional tail record lacks realized behavior"))?;
            entry_map.insert(
                "rawRotatingProvenance".into(),
                json!({
                    "schemaVersion": RAW_ROTATING_PROVENANCE_SCHEMA,
                    "taskId": task_id,
                    "resultSha256": result_sha,
                    "observationStreamSha256": field(&record, "observationStreamSha256")?,
                    "conservativeReplayStreamSha256": conservative_replay_stream_sha256(result)?,
                    "realizedBehaviorSha256": canonical_sha256(behavior)?,
                }),
            );
        }
        entry_map.insert(
            "stageProjection".into(),
            stage_projection(&Value::Object(record.clone()))?,
        );
        entry_map.insert(
            "rotatingEvidenceMetrics".into(),
            json!({
                "conservativeNetR": field(&record, "conservativeNetR")?,
                "noCostNetR": field(&record, "noCostNetR")?,
                "maxDrawdownR": field(&record, "maxDrawdownR")?,
                "closedTrades": field(&record, "trades")?,
                "observations": field(&record, "observations")?,
                "v3Admissible": field(&record, "v3Admissible")?,
                "resolvedProgramSha256": field(&record, "resolvedProgramSha256")?,
                "resolvedProfileSnapshotSha256": field(&record, "resolvedProfileSnapshotSha256")?,
                "sourceProfileSnapshotSha256": field(&record, "sourceProfileSnapshotSha256")?,
            }),
        );
        if include_funnel {
            entry_map.insert(
                "funnelProjection".into(),
                funnel_projection(result, &record, field(raw_ref, "resultSha256")?)?,
            );
        }
    }
    add_identity(&mut entry, "entrySha256")?;
    Ok(entry)
}

fn window_record(result: &Value) -> Result<Map<String, Value>> {
    let root = object(result, "raw result")?;
    if text(root, "schema_version")? == REJECTED_SCHEMA_V1 {
        let outcome = object(
            field(root, "evaluation_outcome")?,
            "warmup rejection outcome",
        )?;
        let reason = text(outcome, "reason_code")?;
        return Ok(json!({
            "economicsBasis": format!("not_evaluated_{reason}"),
            "v3Admissible": false,
            "evaluationRejected": true,
            "rejection": Value::Object(outcome.clone()),
            "candidateId": python_string(root.get("candidate_id")),
            "windowId": format!("{}/{}", python_string(root.get("analysis_window_start")), python_string(root.get("analysis_window_end"))),
            "analysisWindowStart": root.get("analysis_window_start").cloned().unwrap_or(Value::Null),
            "analysisWindowEnd": root.get("analysis_window_end").cloned().unwrap_or(Value::Null),
        }).as_object().expect("object").clone());
    }
    ensure!(
        matches!(
            text(root, "schema_version")?.as_str(),
            ADMITTED_SCHEMA_V1 | ADMITTED_SCHEMA_V2
        ),
        "raw admitted result schema is invalid"
    );
    let cost_views = object(field(root, "cost_view_results")?, "cost view results")?;
    ensure!(
        cost_views.len() == 2
            && cost_views.contains_key("research_conservative")
            && cost_views.contains_key("none"),
        "candidate result must contain exactly both cost views"
    );
    let conservative_replay = replay(cost_views, "research_conservative")?;
    let no_cost_replay = replay(cost_views, "none")?;
    ensure!(
        conservative_replay.get("streamSha256") == no_cost_replay.get("streamSha256")
            && conservative_replay.get("streamSha256") == root.get("observation_stream_sha256"),
        "cost views do not share exact observation stream"
    );
    let conservative = object(
        field(conservative_replay, "metrics")?,
        "conservative metrics",
    )?;
    let no_cost = object(field(no_cost_replay, "metrics")?, "no-cost metrics")?;
    let v3 = root.contains_key("evidence_contract")
        || [conservative, no_cost].iter().any(|metrics| {
            [
                "terminalValuation",
                "terminalAdjustedTotalGrossR",
                "terminalAdjustedTotalNetR",
                "terminalAdjustedTotalExecutionCostPercent",
                "terminalAdjustedEquityCurveR",
                "terminalAdjustedMaxDrawdownR",
            ]
            .iter()
            .any(|key| metrics.contains_key(*key))
        });
    ensure!(
        v3,
        "native campaign seal currently requires v3 admitted evidence"
    );
    let raw_conservative = finite_metric(conservative, "totalNetR", 0.0)?;
    let raw_no_cost = finite_metric(no_cost, "totalNetR", 0.0)?;
    let raw_gross = finite_metric(conservative, "totalGrossR", 0.0)?;
    let raw_drawdown = finite_metric(conservative, "maxDrawdownR", 0.0)?;
    let conservative_terminal = terminal_economics(conservative)?;
    let no_cost_terminal = terminal_economics(no_cost)?;
    let economic_conservative = f64_field(&conservative_terminal, "terminalAdjustedNetR")?;
    let economic_no_cost = f64_field(&no_cost_terminal, "terminalAdjustedNetR")?;
    let economic_gross = f64_field(&conservative_terminal, "terminalAdjustedGrossR")?;
    let economic_drawdown = f64_field(&conservative_terminal, "terminalAdjustedMaxDrawdownR")?;
    let economic_curve = field(&conservative_terminal, "terminalAdjustedEquityCurveR")?.clone();
    let raw_curve = finite_array(conservative.get("equityCurveR"))?;
    let trades = conservative_replay
        .get("trades")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("conservative replay trades must be an array"))?;
    let mut entry_hours: BTreeMap<String, u64> = BTreeMap::new();
    let mut mfe = Vec::new();
    let mut mae = Vec::new();
    let mut holding = Vec::new();
    for trade in trades {
        let Some(trade) = trade.as_object() else {
            continue;
        };
        if let Some(value) = trade
            .get("entryTime")
            .and_then(Value::as_str)
            .and_then(iso_hour)
        {
            *entry_hours.entry(format!("{value:02}")).or_default() += 1;
        }
        if let Some(value) = finite_optional(trade.get("maxFavorableExcursionR")) {
            mfe.push(value);
        }
        if let Some(value) = finite_optional(trade.get("maxAdverseExcursionR")) {
            mae.push(value);
        }
        if let Some(value) = trade.get("holdingBars").and_then(Value::as_u64) {
            holding.push(value);
        }
    }
    let evidence_contract = field(root, "evidence_contract")?.clone();
    let evidence = object(&evidence_contract, "evidence contract")?;
    let endpoints = json!({
        "analysisWindowStart": field(evidence, "analysis_window_start")?,
        "analysisWindowEnd": field(evidence, "analysis_window_end")?,
        "firstAdmittedObservationTimestamp": field(evidence, "first_admitted_observation_timestamp")?,
        "lastAdmittedObservationTimestamp": field(evidence, "last_admitted_observation_timestamp")?,
        "observationCount": field(evidence, "observation_count")?,
        "requestedBarLimit": field(evidence, "requested_bar_limit")?,
        "effectiveBarLimit": field(evidence, "effective_bar_limit")?,
    });
    let mut record = Map::new();
    macro_rules! put {
        ($key:expr, $value:expr) => {
            record.insert($key.into(), $value);
        };
    }
    put!(
        "economicsBasis",
        Value::String("stage5e7_v3_terminal_adjusted".into())
    );
    put!("v3Admissible", Value::Bool(true));
    put!(
        "candidateId",
        Value::String(python_string(root.get("candidate_id")))
    );
    put!(
        "windowId",
        Value::String(format!(
            "{}/{}",
            python_string(root.get("analysis_window_start")),
            python_string(root.get("analysis_window_end"))
        ))
    );
    for (out, input) in [
        ("analysisWindowStart", "analysis_window_start"),
        ("analysisWindowEnd", "analysis_window_end"),
        (
            "sourceProfileSnapshotSha256",
            "source_profile_snapshot_sha256",
        ),
        (
            "resolvedProfileSnapshotSha256",
            "resolved_profile_snapshot_sha256",
        ),
        ("resolvedProgramSha256", "program_sha256"),
        ("programSha256", "program_sha256"),
        ("observationStreamSha256", "observation_stream_sha256"),
    ] {
        put!(out, root.get(input).cloned().unwrap_or(Value::Null));
    }
    put!(
        "observations",
        integer_or_zero(conservative.get("observationsProcessed"))?
    );
    put!("trades", integer_or_zero(conservative.get("tradesClosed"))?);
    put!("wins", integer_or_zero(conservative.get("wins"))?);
    put!("losses", integer_or_zero(conservative.get("losses"))?);
    put!(
        "flatTrades",
        integer_or_zero(conservative.get("flatTrades"))?
    );
    for (key, value) in [
        ("conservativeNetR", economic_conservative),
        ("noCostNetR", economic_no_cost),
        ("grossR", economic_gross),
        ("maxDrawdownR", economic_drawdown),
        ("rawClosedConservativeNetR", raw_conservative),
        ("rawClosedNoCostNetR", raw_no_cost),
        ("rawClosedGrossR", raw_gross),
        ("rawClosedMaxDrawdownR", raw_drawdown),
        ("rawClosedCostViewDeltaR", raw_no_cost - raw_conservative),
        ("terminalAdjustedConservativeNetR", economic_conservative),
        ("terminalAdjustedNoCostNetR", economic_no_cost),
        ("terminalAdjustedGrossR", economic_gross),
        ("terminalAdjustedMaxDrawdownR", economic_drawdown),
        (
            "terminalAdjustedCostViewDeltaR",
            economic_no_cost - economic_conservative,
        ),
    ] {
        put!(key, number(value));
    }
    put!("rawClosedEquityCurveR", Value::Array(raw_curve));
    put!("terminalAdjustedEquityCurveR", economic_curve.clone());
    put!("conservativeTerminal", Value::Object(conservative_terminal));
    put!("noCostTerminal", Value::Object(no_cost_terminal));
    put!("evidenceContract", evidence_contract);
    put!("evidenceContractEndpoints", endpoints);
    put!(
        "averageHoldingBars",
        conservative
            .get("averageHoldingBars")
            .cloned()
            .unwrap_or(Value::Null)
    );
    put!(
        "holdingBars",
        Value::Array(
            holding
                .iter()
                .map(|value| Value::Number((*value).into()))
                .collect()
        )
    );
    put!(
        "medianHoldingBars",
        if holding.is_empty() {
            Value::Null
        } else {
            number(median(&holding))
        }
    );
    put!(
        "exposureRatio",
        number(finite_metric(conservative, "exposureRatio", 0.0)?)
    );
    put!(
        "transitionEntropy",
        number(finite_metric(conservative, "transitionEntropy", 0.0)?)
    );
    put!(
        "winRate",
        conservative.get("winRate").cloned().unwrap_or(Value::Null)
    );
    put!(
        "profitFactor",
        conservative
            .get("profitFactor")
            .cloned()
            .unwrap_or(Value::Null)
    );
    for key in [
        "actionCounts",
        "closeReasonCounts",
        "stateOccupancy",
        "transitionCounts",
    ] {
        put!(
            key,
            conservative
                .get(key)
                .filter(|value| truthy(value))
                .cloned()
                .unwrap_or_else(|| json!({}))
        );
    }
    put!("entryHourCounts", serde_json::to_value(entry_hours)?);
    put!("averageMfeR", number(average(&mfe)));
    put!("averageMaeR", number(average(&mae)));
    put!("equityCurveR", economic_curve);
    // This exact raw replay projection is intentionally built inside the
    // native raw-read boundary.  It is never supplied by Python as a rich
    // result projection: the later reducer consumes only the authenticated
    // compact stage record.
    put!(
        "realizedBehavior",
        build_window_realized_behavior(
            conservative_replay,
            conservative,
            &format!(
                "{}/{}",
                python_string(root.get("analysis_window_start")),
                python_string(root.get("analysis_window_end")),
            ),
        )?
    );
    Ok(record)
}

fn conservative_replay_stream_sha256(result: &Value) -> Result<Value> {
    let root = object(result, "raw result")?;
    let cost_views = object(field(root, "cost_view_results")?, "cost view results")?;
    Ok(field(replay(cost_views, "research_conservative")?, "streamSha256")?.clone())
}

#[derive(Default)]
struct WindowBehaviorSide {
    closed_trades: i64,
    wins: i64,
    losses: i64,
    flat_trades: i64,
    gross_r: f64,
    net_r: f64,
    cost_r: f64,
    holding_bars: i64,
    holding_hours: f64,
    close_reason_counts: BTreeMap<String, i64>,
    action_counts: BTreeMap<String, i64>,
    transition_counts: BTreeMap<String, i64>,
    trade_sequence: Vec<Value>,
    terminal_status_counts: BTreeMap<String, i64>,
    terminal_direction_count: i64,
    conflict_abstentions: i64,
}

fn build_window_realized_behavior(
    replay_value: &Map<String, Value>,
    metrics: &Map<String, Value>,
    window_id: &str,
) -> Result<Value> {
    let trades = replay_value
        .get("trades")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("conservative replay trades must be an array"))?;
    let reported = nonnegative_i64_value(
        metrics.get("tradesClosed").unwrap_or(&Value::from(0)),
        "metrics tradesClosed",
    )?;
    let observations = nonnegative_i64_value(
        metrics
            .get("observationsProcessed")
            .unwrap_or(&Value::from(0)),
        "metrics observationsProcessed",
    )?;
    let mut sides = BTreeMap::from([
        ("long", WindowBehaviorSide::default()),
        ("short", WindowBehaviorSide::default()),
    ]);
    let mut trade_sides = BTreeMap::<String, String>::new();
    let mut position_sides = BTreeMap::<String, String>::new();
    let mut complete_economics = !trades.is_empty() && trades.len() as i64 == reported;
    for (index, raw) in trades.iter().enumerate() {
        let raw = object(raw, &format!("replay trades[{index}]"))?;
        let direction = raw
            .get("direction")
            .and_then(Value::as_str)
            .filter(|value| matches!(*value, "long" | "short"))
            .ok_or_else(|| anyhow!("replay trades[{index}] direction must be long or short"))?;
        let side = sides.get_mut(direction).expect("fixed directions");
        if let Some(value) = raw.get("tradeId").filter(|v| !v.is_null()) {
            trade_sides.insert(python_string(Some(value)), direction.to_owned());
        }
        if let Some(value) = raw.get("positionId").filter(|v| !v.is_null()) {
            position_sides.insert(python_string(Some(value)), direction.to_owned());
        }
        let gross_present = raw.contains_key("grossR");
        let net_present = raw.contains_key("netR");
        ensure!(
            gross_present == net_present,
            "replay trades[{index}] grossR/netR must be paired"
        );
        let (gross_r, net_r) = if gross_present {
            (
                finite_required(
                    field(raw, "grossR")?,
                    &format!("replay trades[{index}] grossR"),
                )?,
                finite_required(field(raw, "netR")?, &format!("replay trades[{index}] netR"))?,
            )
        } else {
            complete_economics = false;
            (0.0, 0.0)
        };
        let holding = nonnegative_i64_value(
            raw.get("holdingBars").unwrap_or(&Value::from(0)),
            &format!("replay trades[{index}] holdingBars"),
        )?;
        let hours = finite_required(
            raw.get("holdingHours").unwrap_or(&Value::from(0.0)),
            &format!("replay trades[{index}] holdingHours"),
        )?;
        ensure!(
            hours >= 0.0,
            "replay trades[{index}] holdingHours must be nonnegative"
        );
        side.closed_trades += 1;
        side.gross_r += gross_r;
        side.net_r += net_r;
        // Preserve Python's per-trade operation order. Computing this only
        // from accumulated totals changes IEEE-754 output bytes on real tails.
        side.cost_r += gross_r - net_r;
        side.holding_bars += holding;
        side.holding_hours += hours;
        if net_r > 0.0 {
            side.wins += 1;
        } else if net_r < 0.0 {
            side.losses += 1;
        } else {
            side.flat_trades += 1;
        }
        increment_behavior_count(
            &mut side.close_reason_counts,
            python_or_unknown(raw.get("closeReason")),
        )?;
        side.trade_sequence.push(json!({
            "entryClockIndex": nonnegative_i64_value(raw.get("entryClockIndex").unwrap_or(&Value::from(0)), &format!("replay trades[{index}] entryClockIndex"))?,
            "exitClockIndex": nonnegative_i64_value(raw.get("exitClockIndex").unwrap_or(&Value::from(0)), &format!("replay trades[{index}] exitClockIndex"))?,
            "entryTime": raw.get("entryTime").cloned().unwrap_or(Value::Null),
            "exitTime": raw.get("exitTime").cloned().unwrap_or(Value::Null),
            "holdingBars": holding, "holdingHours": hours,
            "closeReason": python_or_unknown(raw.get("closeReason")),
            "grossR": gross_r, "netR": net_r,
        }));
    }

    let mut conflict_abstentions = 0i64;
    let mut unattributed_conflicts = 0i64;
    for (key, label, target_is_action) in [
        ("executionTraces", "actionKind", true),
        ("graphTraces", "transitionId", false),
    ] {
        let traces = replay_value
            .get(key)
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("conservative replay {key} must be an array"))?;
        for (index, trace) in traces.iter().enumerate() {
            let trace = object(trace, &format!("conservative replay {key}[{index}]"))?;
            let side_name = trace_side(trace, &trade_sides, &position_sides)?;
            if let (Some(side_name), Some(value)) = (side_name.as_deref(), trace.get(label)) {
                if !value.is_null() {
                    let target = sides.get_mut(side_name).expect("fixed direction");
                    if target_is_action {
                        increment_behavior_count(
                            &mut target.action_counts,
                            python_or_unknown(Some(value)),
                        )?;
                    } else {
                        increment_behavior_count(
                            &mut target.transition_counts,
                            python_or_unknown(Some(value)),
                        )?;
                    }
                }
            }
            if trace_is_conflict_abstention(trace) {
                conflict_abstentions += 1;
                if let Some(side_name) = side_name {
                    sides
                        .get_mut(side_name.as_str())
                        .expect("fixed direction")
                        .conflict_abstentions += 1;
                } else {
                    unattributed_conflicts += 1;
                }
            }
        }
    }

    let mut terminal = json!({"positionStatus": "unavailable", "direction": null});
    if let Some(value) = metrics
        .get("terminalValuation")
        .filter(|value| !value.is_null())
    {
        let terminal_map = object(value, "metrics terminalValuation")?;
        let status = python_or_unknown(terminal_map.get("positionStatus"));
        let direction = terminal_map
            .get("direction")
            .cloned()
            .unwrap_or(Value::Null);
        terminal = json!({"positionStatus": status, "direction": direction});
        if !direction.is_null() {
            let direction = direction
                .as_str()
                .filter(|value| matches!(*value, "long" | "short"))
                .ok_or_else(|| anyhow!("terminal direction must be long or short"))?;
            let side = sides.get_mut(direction).expect("fixed direction");
            side.terminal_direction_count += 1;
            increment_behavior_count(&mut side.terminal_status_counts, status)?;
            side.gross_r += finite_required(
                terminal_map.get("grossR").unwrap_or(&Value::Null),
                "terminal grossR",
            )?;
            side.net_r += finite_required(
                terminal_map.get("netR").unwrap_or(&Value::Null),
                "terminal netR",
            )?;
            side.cost_r += finite_required(
                terminal_map.get("grossR").unwrap_or(&Value::Null),
                "terminal grossR",
            )? - finite_required(
                terminal_map.get("netR").unwrap_or(&Value::Null),
                "terminal netR",
            )?;
        } else {
            ensure!(
                matches!(status.as_str(), "no_open_position" | "none" | "unknown"),
                "terminal position status requires a direction"
            );
        }
    }
    if complete_economics {
        let expected_gross = finite_required(
            metrics.get("totalGrossR").unwrap_or(&Value::Null),
            "metrics totalGrossR",
        )?;
        let expected_net = finite_required(
            metrics.get("totalNetR").unwrap_or(&Value::Null),
            "metrics totalNetR",
        )?;
        let terminal_direction = terminal.get("direction").and_then(Value::as_str);
        let terminal_map = metrics.get("terminalValuation").and_then(Value::as_object);
        let terminal_gross = if terminal_direction.is_some() {
            finite_required(
                terminal_map
                    .and_then(|map| map.get("grossR"))
                    .unwrap_or(&Value::Null),
                "terminal grossR",
            )?
        } else {
            0.0
        };
        let terminal_net = if terminal_direction.is_some() {
            finite_required(
                terminal_map
                    .and_then(|map| map.get("netR"))
                    .unwrap_or(&Value::Null),
                "terminal netR",
            )?
        } else {
            0.0
        };
        let gross: f64 = sides.values().map(|side| side.gross_r).sum();
        let net: f64 = sides.values().map(|side| side.net_r).sum();
        ensure!(
            (gross - terminal_gross - expected_gross).abs() <= 1e-9
                && (net - terminal_net - expected_net).abs() <= 1e-9,
            "replay trade economics do not reconcile with metrics"
        );
    }
    let mut rendered_sides = Map::new();
    for name in ["long", "short"] {
        let side = sides.get(name).expect("fixed direction");
        rendered_sides.insert(name.to_owned(), json!({
            "closedTrades": side.closed_trades, "wins": side.wins, "losses": side.losses, "flatTrades": side.flat_trades,
            "grossR": side.gross_r, "netR": side.net_r, "costR": side.cost_r, "holdingBars": side.holding_bars, "holdingHours": side.holding_hours,
            "closeReasonCounts": side.close_reason_counts, "actionCounts": side.action_counts, "transitionCounts": side.transition_counts,
            "tradeSequence": side.trade_sequence, "terminalStatusCounts": side.terminal_status_counts,
            "terminalDirectionCount": side.terminal_direction_count, "conflictAbstentions": side.conflict_abstentions,
            "active": side.closed_trades != 0 || side.terminal_direction_count != 0,
            "exposureProxy": if observations == 0 { 0.0 } else { side.holding_bars as f64 / observations as f64 },
            "averageHoldingBars": if side.closed_trades == 0 { 0.0 } else { side.holding_bars as f64 / side.closed_trades as f64 },
            "closeReasonDistribution": behavior_distribution(&side.close_reason_counts),
            "actionDistribution": behavior_distribution(&side.action_counts),
            "transitionDistribution": behavior_distribution(&side.transition_counts),
        }));
    }
    Ok(json!({
        "schemaVersion": "temporal_realized_behavior_v1", "windowId": window_id,
        "reportedClosedTrades": reported, "materializedClosedTrades": trades.len(), "unattributedClosedTrades": 0,
        "observations": observations, "terminal": terminal, "conflictAbstentions": conflict_abstentions,
        "unattributedConflictAbstentions": unattributed_conflicts, "sides": rendered_sides,
    }))
}

fn nonnegative_i64_value(value: &Value, label: &str) -> Result<i64> {
    value
        .as_i64()
        .filter(|value| *value >= 0)
        .ok_or_else(|| anyhow!("{label} must be nonnegative"))
}
fn finite_required(value: &Value, label: &str) -> Result<f64> {
    value
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| anyhow!("{label} must be finite"))
}
fn increment_behavior_count(counts: &mut BTreeMap<String, i64>, key: String) -> Result<()> {
    let value = counts.entry(key).or_default();
    *value = value
        .checked_add(1)
        .ok_or_else(|| anyhow!("realized behavior count overflow"))?;
    Ok(())
}
fn python_or_unknown(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "unknown".to_owned(),
        Some(Value::String(value)) if value.is_empty() => "unknown".to_owned(),
        Some(value) => python_string(Some(value)),
    }
}
fn behavior_distribution(counts: &BTreeMap<String, i64>) -> Value {
    let total: i64 = counts.values().sum();
    if total <= 0 {
        return json!({});
    }
    Value::Object(
        counts
            .iter()
            .map(|(key, count)| (key.clone(), number(*count as f64 / total as f64)))
            .collect(),
    )
}
fn trace_side(
    trace: &Map<String, Value>,
    trades: &BTreeMap<String, String>,
    positions: &BTreeMap<String, String>,
) -> Result<Option<String>> {
    if let Some(direction) = trace.get("direction").filter(|value| !value.is_null()) {
        return direction
            .as_str()
            .filter(|value| matches!(*value, "long" | "short"))
            .map(ToOwned::to_owned)
            .map(Some)
            .ok_or_else(|| anyhow!("replay trace direction must be long or short"));
    }
    for (key, lookup) in [("tradeId", trades), ("positionId", positions)] {
        if let Some(value) = trace.get(key).filter(|value| !value.is_null()) {
            if let Some(direction) = lookup.get(&python_string(Some(value))) {
                return Ok(Some(direction.clone()));
            }
        }
    }
    Ok(None)
}
fn trace_is_conflict_abstention(trace: &Map<String, Value>) -> bool {
    let values = ["transitionId", "reasonCode", "actionKind", "status"]
        .into_iter()
        .map(|key| python_string(trace.get(key)).to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ");
    values.contains("conflict") && (values.contains("abstain") || values.contains("reject"))
}

fn replay<'a>(cost_views: &'a Map<String, Value>, key: &str) -> Result<&'a Map<String, Value>> {
    let view = object(field(cost_views, key)?, "cost view")?;
    object(field(view, "replay_result")?, "replay result")
}

fn terminal_economics(metrics: &Map<String, Value>) -> Result<Map<String, Value>> {
    let terminal_value = field(metrics, "terminalValuation")?.clone();
    let terminal = object(&terminal_value, "terminal valuation")?;
    let mut out = Map::new();
    out.insert("terminalValuation".into(), terminal_value.clone());
    for (output, input) in [
        ("terminalPolicy", "policy"),
        ("terminalPolicySchemaVersion", "schemaVersion"),
        ("terminalLastCompletedBarId", "lastCompletedBarId"),
        ("terminalLastCompletedBarStart", "lastCompletedBarStart"),
        ("terminalLastCompletedBarClose", "lastCompletedBarClose"),
        ("terminalPositionStatus", "positionStatus"),
        ("terminalPendingEffectStatus", "pendingEffectStatus"),
        (
            "terminalPendingEffectCancellationTreatment",
            "pendingEffectCancellationTreatment",
        ),
    ] {
        out.insert(
            output.into(),
            terminal.get(input).cloned().unwrap_or(Value::Null),
        );
    }
    for (output, input, default_on_null) in [
        ("terminalMarkPrice", "markPrice", false),
        ("terminalGrossR", "grossR", true),
        ("terminalNetR", "netR", true),
        ("terminalExitCostPercent", "exitCostPercent", false),
    ] {
        let value = if default_on_null && terminal.get(input).is_none_or(Value::is_null) {
            0.0
        } else {
            finite_metric(terminal, input, 0.0)?
        };
        out.insert(output.into(), number(value));
    }
    for (output, input) in [
        ("terminalAdjustedGrossR", "terminalAdjustedTotalGrossR"),
        ("terminalAdjustedNetR", "terminalAdjustedTotalNetR"),
        (
            "terminalAdjustedExecutionCostPercent",
            "terminalAdjustedTotalExecutionCostPercent",
        ),
        (
            "terminalAdjustedMaxDrawdownR",
            "terminalAdjustedMaxDrawdownR",
        ),
    ] {
        out.insert(output.into(), number(finite_metric(metrics, input, 0.0)?));
    }
    out.insert(
        "terminalAdjustedEquityCurveR".into(),
        Value::Array(finite_array(metrics.get("terminalAdjustedEquityCurveR"))?),
    );
    Ok(out)
}

fn stage_projection(record: &Value) -> Result<Value> {
    let semantic = canonical_json_bytes(record)?;
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::new(9));
    encoder.write_all(&semantic)?;
    let blob = encoder.finish()?;
    Ok(json!({
        "schemaVersion": STAGE_SCHEMA,
        "codec": "gzip-canonical-json-v1",
        "semanticSha256": sha_bytes(&semantic),
        "semanticSizeBytes": semantic.len(),
        "blobBase64": base64::engine::general_purpose::STANDARD.encode(blob),
    }))
}

fn funnel_projection(
    result: &Value,
    record: &Map<String, Value>,
    result_sha: &Value,
) -> Result<Value> {
    let root = object(result, "raw result")?;
    let replay = replay(
        object(field(root, "cost_view_results")?, "cost views")?,
        "research_conservative",
    )?;
    let traces = replay
        .get("executionTraces")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("execution traces must be an array"))?;
    let trades = replay
        .get("trades")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("trades must be an array"))?;
    let (mut scheduled, mut accepted, mut rejected, mut canceled, mut changed) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    for trace in traces {
        let trace = object(trace, "execution trace")?;
        let status = trace
            .get("status")
            .filter(|value| truthy(value))
            .and_then(Value::as_str)
            .unwrap_or("");
        scheduled += u64::from(status == "scheduled");
        rejected += u64::from(status == "rejected");
        canceled += u64::from(status == "canceled");
        let applied = matches!(status, "filled" | "applied" | "closed");
        accepted += u64::from(applied);
        changed += u64::from(applied);
    }
    let activation = scheduled + accepted + rejected + canceled;
    let rejected_total = rejected + canceled;
    let behavior = json!({
        "windowId": field(record, "windowId")?, "resultSha256": result_sha,
        "activationCount": activation, "acceptedIntentOrEffectCount": accepted,
        "rejectedIntentOrEffectCount": rejected_total, "canceledIntentOrEffectCount": canceled,
        "positionChangeCount": changed, "tradeCloseCount": trades.len(),
        "neverActivated": activation == 0 && accepted == 0 && rejected_total == 0 && canceled == 0 && changed == 0 && trades.is_empty(),
    });
    Ok(json!({
        "resultBehavior": behavior,
        "terminalAdjustedConservativeNetR": field(record, "terminalAdjustedConservativeNetR")?,
        "terminalAdjustedMaxDrawdownR": field(record, "terminalAdjustedMaxDrawdownR")?,
    }))
}

fn build_campaign_seal(
    manifest: &Manifest,
    source: &Source,
    index: &Value,
    inventory_sha256: String,
    inventory_bytes: u64,
    metrics: &ReadMetrics,
) -> Result<Value> {
    let index_sha = sha_field(object(index, "tail result index")?, "tailResultIndexSha256")?;
    let mut seal = json!({
        "schemaVersion": CAMPAIGN_SEAL_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "manifestSha256": manifest.manifest_sha256,
        "sourceSha256": source.source_sha256,
        "authorityId": source.authority_id,
        "authoritySha256": source.authority_sha256,
        "taskMatrixSha256": source.task_matrix_sha256,
        "taskManifestSha256": source.task_manifest_sha256,
        "checkpointSha256": source.checkpoint_sha256,
        "taskCount": source.tasks.len(),
        "rawResultReadCount": metrics.raw_reads,
        "sourceResultBlobBytes": metrics.blob_bytes,
        "sourceResultUncompressedBytes": metrics.uncompressed_bytes,
        "sourceResultSemanticBytes": metrics.semantic_bytes,
        "tailResultIndex": { "path": DIRECTIONAL_INDEX_PATH, "sha256": index_sha },
        "rawResultInventory": { "path": INVENTORY_PATH, "sha256": inventory_sha256, "bytes": inventory_bytes },
    });
    seal.as_object_mut().expect("campaign seal object").insert(
        "runtimeAuthoritySha256".into(),
        json!(&manifest.runtime_authority_sha256),
    );
    add_identity(&mut seal, "campaignSealSha256")?;
    Ok(seal)
}

fn run_tail_transaction(
    output_dir: &Path,
    manifest: &Manifest,
    source: &Source,
    seal: &Value,
) -> Result<Value> {
    let seal_map = object(seal, "campaign seal")?;
    let index_sha = descriptor_sha(field(seal_map, "tailResultIndex")?)?;
    let mut tail_manifest = json!({
        "schemaVersion": temporal_qd_tail_reducer::MANIFEST_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "operation": temporal_qd_tail_reducer::OPERATION,
        "evaluationPopulationPath": absolute_string(&manifest.evaluation_path)?,
        "evaluationPopulationSha256": manifest.evaluation_sha256,
        "tailResultIndexPath": absolute_string(&output_dir.join(DIRECTIONAL_INDEX_PATH))?,
        "tailResultIndexSha256": index_sha,
        "generationIndex": manifest.generation_index,
        "minimumTotalTrades": manifest.minimum_total_trades,
        "minimumTradesPerWindow": manifest.minimum_trades_per_window,
        "capTrades": manifest.cap_trades,
        "provisionalLimit": manifest.provisional_limit,
        "resultPath": temporal_qd_tail_reducer::RESULT_PATH,
    });
    tail_manifest
        .as_object_mut()
        .expect("tail reduction manifest object")
        .insert(
            "runtimeAuthoritySha256".into(),
            json!(&manifest.runtime_authority_sha256),
        );
    add_identity(&mut tail_manifest, "manifestSha256")?;
    publish_bytes_once(
        output_dir,
        TAIL_MANIFEST_PATH,
        &canonical_json_line(&tail_manifest)?,
    )?;
    let reduction =
        temporal_qd_tail_reducer::execute_manifest(&output_dir.join(TAIL_MANIFEST_PATH))?;
    let reduction_map = object(&reduction, "tail reduction result")?;
    let mut transaction = json!({
        "schemaVersion": TRANSACTION_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "manifestSha256": manifest.manifest_sha256,
        "sourceSha256": source.source_sha256,
        "campaignSealSha256": sha_field(seal_map, "campaignSealSha256")?,
        "tailResultIndexSha256": descriptor_sha(field(seal_map, "tailResultIndex")?)?,
        "tailReductionManifestSha256": sha_field(object(&tail_manifest, "tail reduction manifest")?, "manifestSha256")?,
        "tailReductionResultSha256": sha_field(reduction_map, "resultSha256")?,
        "evaluatedMembers": field(reduction_map, "evaluatedMembers")?,
        "provisional": field(reduction_map, "provisional")?,
    });
    transaction
        .as_object_mut()
        .expect("generation tail transaction object")
        .insert(
            "runtimeAuthoritySha256".into(),
            json!(&manifest.runtime_authority_sha256),
        );
    add_identity(&mut transaction, "transactionSha256")?;
    Ok(transaction)
}

fn descriptor_sha(value: &Value) -> Result<String> {
    sha_field(object(value, "artifact descriptor")?, "sha256")
}

fn publish_bytes_once(output_dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let destination = output_dir.join(name);
    if destination.exists() {
        ensure!(
            fs::read(&destination)? == bytes,
            "refusing divergent existing immutable artifact: {}",
            destination.display()
        );
        return Ok(());
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    // Keep the temporary basename short.  Current-v5 output roots can already
    // be close to the classic Windows MAX_PATH boundary, and repeating the
    // destination basename here made the temporary path needlessly longer.
    let temporary = output_dir.join(format!(".p{}.{}.tmp", std::process::id(), nanos));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    match fs::rename(&temporary, &destination) {
        Ok(()) => {
            sync_directory(output_dir)?;
            Ok(())
        }
        Err(_error) if destination.exists() => {
            let identical = fs::read(&destination)? == bytes;
            let _ = fs::remove_file(&temporary);
            ensure!(
                identical,
                "refusing divergent existing immutable artifact: {}",
                destination.display()
            );
            sync_directory(output_dir)?;
            Ok(())
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            Err(error).with_context(|| {
                format!(
                    "rename synced immutable artifact into place: {}",
                    destination.display()
                )
            })
        }
    }
}

#[cfg(not(windows))]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn sync_directory(path: &Path) -> Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const GENERIC_READ: u32 = 0x8000_0000;
    const GENERIC_WRITE: u32 = 0x4000_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    let mut options = OpenOptions::new();
    options
        .access_mode(GENERIC_READ | GENERIC_WRITE)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    let directory = match options.open(path) {
        Ok(directory) => directory,
        Err(error) if matches!(error.raw_os_error(), Some(1 | 5 | 50 | 87)) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    match directory.sync_all() {
        Err(error) if matches!(error.raw_os_error(), Some(1 | 5 | 50 | 87)) => Ok(()),
        outcome => outcome.map_err(Into::into),
    }
}

fn add_identity(value: &mut Value, field_name: &str) -> Result<()> {
    let identity = canonical_sha256(value)?;
    value
        .as_object_mut()
        .ok_or_else(|| anyhow!("identity target must be object"))?
        .insert(field_name.into(), Value::String(identity));
    Ok(())
}

fn absolute_string(path: &Path) -> Result<String> {
    Ok(path
        .canonicalize()
        .with_context(|| format!("resolve path: {}", path.display()))?
        .to_string_lossy()
        .into_owned())
}

fn sha_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

struct HashingReader<R> {
    inner: R,
    digest: Sha256,
    bytes: u64,
}

impl<R> HashingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(self) -> (String, u64) {
        let digest = self.digest.finalize();
        (format!("sha256:{digest:x}"), self.bytes)
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.digest.update(&buffer[..read]);
        self.bytes = self.bytes.saturating_add(read as u64);
        Ok(read)
    }
}

fn finite_metric(map: &Map<String, Value>, key: &str, default: f64) -> Result<f64> {
    match map.get(key) {
        None | Some(Value::Null) => Ok(default),
        Some(value) => value
            .as_f64()
            .filter(|v| v.is_finite())
            .ok_or_else(|| anyhow!("{key} must be finite numeric")),
    }
}

fn finite_optional(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(|value| {
            if value.is_boolean() {
                None
            } else {
                value.as_f64()
            }
        })
        .filter(|value| value.is_finite())
}

fn finite_array(value: Option<&Value>) -> Result<Vec<Value>> {
    let Some(value) = value.filter(|value| truthy(value)) else {
        return Ok(Vec::new());
    };
    let rows = value
        .as_array()
        .ok_or_else(|| anyhow!("finite metric array must be an array"))?;
    rows.iter()
        .map(|item| {
            item.as_f64()
                .filter(|value| value.is_finite())
                .map(number)
                .ok_or_else(|| anyhow!("metric array contains non-finite numeric value"))
        })
        .collect()
}

fn integer_or_zero(value: Option<&Value>) -> Result<Value> {
    let Some(value) = value.filter(|value| truthy(value)) else {
        return Ok(Value::Number(0.into()));
    };
    if let Some(number) = value.as_i64() {
        return Ok(Value::Number(number.into()));
    }
    if let Some(number) = value.as_u64() {
        return Ok(Value::Number(number.into()));
    }
    Err(anyhow!("integer metric is invalid"))
}

fn f64_field(map: &Map<String, Value>, key: &str) -> Result<f64> {
    field(map, key)?
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| anyhow!("{key} must be finite numeric"))
}

fn number(value: f64) -> Value {
    Value::Number(Number::from_f64(value).expect("finite"))
}
fn average(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}
fn median(values: &[u64]) -> f64 {
    let mut ordered = values.to_vec();
    ordered.sort_unstable();
    if ordered.len() % 2 == 1 {
        ordered[ordered.len() / 2] as f64
    } else {
        (ordered[ordered.len() / 2 - 1] as f64 + ordered[ordered.len() / 2] as f64) / 2.0
    }
}
fn iso_hour(value: &str) -> Option<u8> {
    let bytes = value.as_bytes();
    if bytes.len() < 13 || bytes.get(10) != Some(&b'T') {
        return None;
    }
    let hour = std::str::from_utf8(&bytes[11..13])
        .ok()?
        .parse::<u8>()
        .ok()?;
    (hour < 24).then_some(hour)
}
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
fn python_string(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "None".into(),
        Some(Value::Bool(true)) => "True".into(),
        Some(Value::Bool(false)) => "False".into(),
        Some(Value::String(v)) => v.clone(),
        Some(v) => v.to_string(),
    }
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{label} must be an object"))
}
fn field<'a>(map: &'a Map<String, Value>, key: &str) -> Result<&'a Value> {
    map.get(key).ok_or_else(|| anyhow!("object lacks {key}"))
}
fn text(map: &Map<String, Value>, key: &str) -> Result<String> {
    field(map, key)?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("{key} must be a nonempty string"))
}
fn unsigned(map: &Map<String, Value>, key: &str) -> Result<u64> {
    field(map, key)?
        .as_u64()
        .ok_or_else(|| anyhow!("{key} must be a nonnegative integer"))
}
fn nonnegative_integral_number(map: &Map<String, Value>, key: &str) -> Result<()> {
    let value = field(map, key)?;
    if value.as_u64().is_some() {
        return Ok(());
    }
    let number = value
        .as_f64()
        .filter(|number| number.is_finite() && *number >= 0.0 && number.fract() == 0.0)
        .ok_or_else(|| anyhow!("{key} must be a nonnegative integral number"))?;
    ensure!(
        number <= u64::MAX as f64,
        "{key} must be a nonnegative integral number"
    );
    Ok(())
}
fn nonnegative(map: &Map<String, Value>, key: &str) -> Result<i64> {
    field(map, key)?
        .as_i64()
        .filter(|value| *value >= 0)
        .ok_or_else(|| anyhow!("{key} must be a nonnegative integer"))
}
fn sha_field(map: &Map<String, Value>, key: &str) -> Result<String> {
    let value = text(map, key)?;
    ensure!(
        value.len() == 71
            && value.starts_with("sha256:")
            && value.as_bytes()[7..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)),
        "{key} must be lowercase SHA-256"
    );
    Ok(value)
}
fn exact_keys(map: &Map<String, Value>, keys: &[&str], label: &str) -> Result<()> {
    ensure!(
        map.len() == keys.len() && keys.iter().all(|key| map.contains_key(*key)),
        "{label} fields are not exact"
    );
    Ok(())
}
fn existing_file(path: &Path, label: &str) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{label} is unavailable: {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "{label} must be a real regular file"
    );
    path.canonicalize()
        .with_context(|| format!("resolve {label}: {}", path.display()))
}
fn ensure_real_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{label} is unavailable: {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "{label} must be a real directory"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SHA_B: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SHA_C: &str = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    #[test]
    fn immutable_publish_uses_short_rename_temp_and_is_restart_safe() -> Result<()> {
        let root = tempfile::tempdir()?;
        let target_parent_len = 215usize;
        let padding_len = target_parent_len
            .saturating_sub(root.path().to_string_lossy().len() + 1)
            .max(1);
        let output = root.path().join("x".repeat(padding_len));
        fs::create_dir(&output)?;
        let destination = output.join(TAIL_MANIFEST_PATH);
        assert!(destination.to_string_lossy().len() < 260);
        let old_style_temp = output.join(format!(
            ".{TAIL_MANIFEST_PATH}.12345.1234567890123456789.tmp"
        ));
        let new_style_temp = output.join(".p12345.1234567890123456789.tmp");
        assert!(old_style_temp.to_string_lossy().len() >= 260);
        assert!(new_style_temp.to_string_lossy().len() < 260);

        publish_bytes_once(&output, TAIL_MANIFEST_PATH, b"first\n")?;
        assert_eq!(fs::read(&destination)?, b"first\n");
        assert!(fs::read_dir(&output)?.all(|entry| {
            !entry
                .expect("directory entry")
                .file_name()
                .to_string_lossy()
                .starts_with(".p")
        }));

        publish_bytes_once(&output, TAIL_MANIFEST_PATH, b"first\n")?;
        let error = publish_bytes_once(&output, TAIL_MANIFEST_PATH, b"different\n")
            .expect_err("divergent immutable output must fail");
        assert!(error.to_string().contains("refusing divergent existing"));
        assert_eq!(fs::read(&destination)?, b"first\n");
        Ok(())
    }

    fn valid_v3_result() -> Value {
        let start = "2024-01-01T00:00:00Z";
        let last = "2024-01-31T23:55:00Z";
        let stream = SHA_A;
        let terminal = json!({"schemaVersion":"temporal_terminal_valuation_v1","policy":"leave_open_mark_to_market_v1","positionStatus":"no_open_position","lastCompletedBarId":"EURUSD:M5:fixture","lastCompletedBarStart":last,"lastCompletedBarClose":last,"markPrice":1.101,"exitCostPercent":0.0,"pendingEffectStatus":"none","pendingEffectCancellationTreatment":"not_applicable","closedTradeCountDelta":0});
        let metrics = json!({"observationsProcessed":10,"tradesClosed":3,"totalGrossR":1.2,"totalNetR":1.0,"totalExecutionCostPercent":0.0,"unresolvedPosition":false,"unresolvedPendingEffect":false,"terminalValuation":terminal,"terminalAdjustedTotalGrossR":1.2,"terminalAdjustedTotalNetR":1.0,"terminalAdjustedTotalExecutionCostPercent":0.0,"terminalAdjustedEquityCurveR":[1.0],"terminalAdjustedMaxDrawdownR":0.0});
        let replay = json!({"streamSha256":stream,"profileSnapshotSha256":SHA_B,"programSha256":SHA_C,"graphTraces":[],"executionTraces":[],"trades":[],"metrics":metrics});
        let evidence = json!({"schema_version":"temporal_graph_candidate_window_evidence_contract_v1","analysis_window_start":start,"analysis_window_end":"2024-02-01T00:00:00Z","analysis_window_end_exclusive":true,"requested_bar_limit":100,"effective_bar_limit":120,"observation_count":10,"first_admitted_observation_timestamp":start,"last_admitted_observation_timestamp":last,"warmup_sufficient":true,"warmup_sufficiency":{"sufficient":true,"source":"aligned_scoring"},"excluded_provisional_count":1,"excluded_outside_analysis_window_count":2});
        let path = cost_view_path_sha256(replay.as_object().unwrap(), "fixture").unwrap();
        json!({"schema_version":ADMITTED_SCHEMA_V1,"candidate_id":"fixture","analysis_window_start":start,"analysis_window_end":"2024-02-01T00:00:00Z","source_profile_snapshot_sha256":SHA_A,"resolved_profile_snapshot_sha256":SHA_B,"program_sha256":SHA_C,"observation_stream_sha256":stream,"observation_summary":{"observation_count":10,"first_bar_start":start,"last_bar_start":last},"evidence_contract":evidence,"cost_view_results":{"research_conservative":{"cost_view":"research_conservative","observation_stream_sha256":stream,"replay_result":replay},"none":{"cost_view":"none","observation_stream_sha256":stream,"replay_result":replay}},"diagnostics":{"observation_count":10,"requested_bar_limit":100,"effective_bar_limit":120,"warmup_sufficient":true,"warmup_sufficiency":{"sufficient":true,"source":"aligned_scoring"},"first_admitted_observation_timestamp":start,"last_admitted_observation_timestamp":last,"excluded_provisional_count":1,"excluded_outside_analysis_window_count":2,"cost_view_decision_path_sha256":path,"cost_view_path_parity":"matched","cost_view_count":2,"shared_stream_required":true}})
    }

    fn rejection_task() -> SourceTask {
        SourceTask {
            manifest_row: Value::Null,
            task: json!({"taskId":"fixture-task","candidateId":"fixture","analysisWindowStart":"2024-01-01T00:00:00Z","analysisWindowEnd":"2024-02-01T00:00:00Z","evidencePlanSemanticSha256":SHA_A,"taskPayloadSha256":SHA_B}),
            task_payload: Value::Null,
            raw_result_path: PathBuf::from("fixture-result.json.gz"),
            raw_result_offset_bytes: None,
            raw_result_length_bytes: None,
            raw_ref: Value::Null,
            binding: json!({"taskKind":"temporal_graph_candidate_window","jobId":"fixture-job","authorityId":"fixture-authority","candidateId":"fixture","evidencePlanId":SHA_A,"lakeWindowSemanticSha256":SHA_B,"sharedObservationStreamId":"fixture-stream"}),
        }
    }

    fn freeze_rejected_artifact(mut value: Value) -> Value {
        let mut identity = value.as_object().expect("rejection object").clone();
        identity.remove("artifact_sha256");
        identity.remove("artifact_size_bytes");
        value["artifact_sha256"] =
            json!(canonical_sha256(&Value::Object(identity)).expect("rejection hash"));
        value["artifact_size_bytes"] = json!(1);
        loop {
            let bytes = canonical_json_bytes(&value).expect("rejection bytes").len() as u64;
            if value["artifact_size_bytes"].as_u64() == Some(bytes) {
                break value;
            }
            value["artifact_size_bytes"] = json!(bytes);
        }
    }

    fn valid_rejection(v2: bool) -> Value {
        let error = json!({"errorType":"AlignedScoringWarmupInsufficientError","detail":"fixture"});
        let mut outcome = json!({"schema_version":if v2 { "temporal_candidate_window_rejection_v2" } else { "temporal_candidate_window_rejection_v1" },"disposition":"rejected","reason_code":if v2 { "duplicate_break_even_execution_invariant" } else { "aligned_scoring_warmup_insufficient" },"replay_executed":v2,"worker_attempt_id":"attempt-1","worker_lease_id":"lease-1","worker_error":error,"worker_error_sha256":canonical_sha256(&json!({"errorType":"AlignedScoringWarmupInsufficientError","detail":"fixture"})).expect("error hash"),"worker_completion_sha256":SHA_C});
        if v2 {
            outcome["replay_completed"] = json!(false);
        }
        freeze_rejected_artifact(
            json!({"schema_version":REJECTED_SCHEMA_V1,"task_kind":"temporal_graph_candidate_window","job_id":"fixture-job","authority_id":"fixture-authority","candidate_id":"fixture","evidence_plan_id":SHA_A,"lake_window_semantic_sha256":SHA_B,"shared_observation_stream_id":"fixture-stream","analysis_window_start":"2024-01-01T00:00:00Z","analysis_window_end":"2024-02-01T00:00:00Z","evaluation_outcome":outcome}),
        )
    }

    fn assert_rejected_after(mut value: Value, mutate: impl FnOnce(&mut Value)) {
        mutate(&mut value);
        assert!(validate_v3_candidate_window_result(&value, None, None).is_err());
    }

    fn freeze_worker_artifact(mut value: Value) -> Value {
        value["artifact_size_bytes"] = json!(1);
        let mut identity = value.as_object().expect("material object").clone();
        identity.remove("artifact_sha256");
        identity.remove("artifact_size_bytes");
        value["artifact_sha256"] =
            json!(canonical_sha256(&Value::Object(identity)).expect("artifact identity"));
        loop {
            let bytes = canonical_json_bytes(&value)
                .expect("canonical material")
                .len() as u64;
            if value["artifact_size_bytes"].as_u64() == Some(bytes) {
                break value;
            }
            value["artifact_size_bytes"] = json!(bytes);
        }
    }

    fn manifest_task_row() -> Value {
        json!({
            "task_id":"fixture-task",
            "lane_id":"fixture",
            "attempt_id":"fixture-task",
            "task_kind":"temporal_graph_candidate_window",
            "payload":{
                "job_id":"fixture-task",
                "candidate_id":"fixture",
                "authority_id":"fixture-authority",
                "evidence_plan":{"plan_id":SHA_A},
                "lake_window_semantic_sha256":SHA_B,
                "shared_observation_stream_id":"fixture-stream",
                "analysis_window_start":"2024-01-01T00:00:00Z",
                "analysis_window_end":"2024-02-01T00:00:00Z",
                "bar_limit":100
            },
            "required_worker_capabilities":[],
            "deadline_seconds":30.0,
            "max_attempts":1
        })
    }

    fn bind_worker_material(mut material: Value) -> Value {
        material["task_kind"] = json!("temporal_graph_candidate_window");
        material["job_id"] = json!("fixture-task");
        material["authority_id"] = json!("fixture-authority");
        material["evidence_plan_id"] = json!(SHA_A);
        material["lake_window_semantic_sha256"] = json!(SHA_B);
        material["shared_observation_stream_id"] = json!("fixture-stream");
        material
    }

    fn exact_v2_worker_fixture() -> Result<(Value, Value)> {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../fixtures/v2_1_exact_worker_fixture.json"
        ))
        .context("parse exact V2.1 worker fixture")?;
        Ok((fixture["task"].clone(), fixture["result"].clone()))
    }

    fn offline_source(root: &Path, task_row: &Value, result: &Value) -> Result<Source> {
        let semantic = canonical_json_bytes(result)?;
        let uncompressed = semantic.clone();
        let mut encoder = GzBuilder::new()
            .mtime(0)
            .operating_system(255)
            .write(Vec::new(), Compression::best());
        encoder.write_all(&uncompressed)?;
        let blob = encoder.finish()?;
        let path = root.join("results.pack");
        fs::write(&path, &blob)?;

        let mut task = source_task_from_manifest_row(task_row)?;
        task.raw_result_path = path;
        task.raw_result_offset_bytes = Some(0);
        task.raw_result_length_bytes = Some(blob.len() as u64);
        task.raw_ref = json!({
            "schemaVersion":"temporal_qd_tail_raw_result_ref_v1",
            "relativePath":"results.pack",
            "resultSha256":sha_bytes(&semantic),
            "codec":"gzip-json-v1",
            "semanticSizeBytes":semantic.len(),
            "uncompressedSha256":sha_bytes(&uncompressed),
            "uncompressedSizeBytes":uncompressed.len(),
            "blobSha256":sha_bytes(&blob),
            "blobSizeBytes":blob.len(),
        });
        Ok(Source {
            authority_id: SHA_A.into(),
            authority_sha256: SHA_A.into(),
            task_matrix_sha256: SHA_B.into(),
            task_manifest_sha256: SHA_C.into(),
            checkpoint_sha256: SHA_A.into(),
            include_funnel: false,
            tasks: vec![task],
            source_sha256: SHA_B.into(),
        })
    }

    #[test]
    fn public_task_row_admission_reuses_campaign_seal_validators() -> Result<()> {
        let task = manifest_task_row();
        let admitted = freeze_worker_artifact(bind_worker_material(valid_v3_result()));
        assert_eq!(
            admit_candidate_window_task_result(&task, &admitted)?,
            CandidateWindowResultAdmission::Admitted
        );

        let mut rejected = valid_rejection(false);
        rejected["job_id"] = json!("fixture-task");
        let rejected = freeze_rejected_artifact(rejected);
        assert_eq!(
            admit_candidate_window_task_result(&task, &rejected)?,
            CandidateWindowResultAdmission::Rejected
        );

        let mut tampered = admitted;
        tampered["job_id"] = json!("other-task");
        assert!(admit_candidate_window_task_result(&task, &tampered).is_err());
        Ok(())
    }

    #[test]
    fn offline_index_reduction_accepts_exact_v2_and_preserves_raw_receipt_provenance() -> Result<()>
    {
        let (task, result) = exact_v2_worker_fixture()?;
        let directory = tempfile::TempDir::new()?;
        let source = offline_source(directory.path(), &task, &result)?;
        let (index, inventory, metrics) = build_index_and_inventory(&source)?;
        assert_eq!(index["taskCount"], json!(1));
        assert_eq!(index["entries"].as_array().map(Vec::len), Some(1));
        assert_eq!(metrics.raw_reads, 1);
        assert!(!inventory.is_empty());

        let raw_path = &source.tasks[0].raw_result_path;
        let mut decoded = Vec::new();
        GzDecoder::new(File::open(raw_path)?).read_to_end(&mut decoded)?;
        let reopened: Value = serde_json::from_slice(&decoded)?;
        assert_eq!(reopened, result);
        assert!(
            reopened
                .get("precompiled_profile_execution_receipt")
                .is_some()
        );
        assert!(
            reopened
                .get("runtime_program_identity_attestation")
                .is_some()
        );

        let mut tampered = result;
        tampered["cost_view_results"]["research_conservative"]["replay_result"]["programSha256"] =
            json!(SHA_A);
        let tampered_directory = tempfile::TempDir::new()?;
        let tampered_source = offline_source(tampered_directory.path(), &task, &tampered)?;
        let error = build_index_and_inventory(&tampered_source)
            .expect_err("offline reduction must reject runtime-program drift before projection");
        assert!(
            format!("{error:#}")
                .contains("candidate-window result v2 receipt/runtime admission failed")
        );
        Ok(())
    }

    #[test]
    fn frozen_python_v3_validator_parity_families_fail_closed() -> Result<()> {
        let valid = valid_v3_result();
        validate_v3_candidate_window_result(&valid, None, None)?;
        // Each case is independently based on the Python v3 validator fixture
        // shape, then tampers one admission family rather than a prevalidated
        // live artifact.
        assert_rejected_after(valid.clone(), |v| {
            v["evidence_contract"]["schema_version"] = json!("wrong-schema")
        });
        assert_rejected_after(valid.clone(), |v| {
            v["evidence_contract"]["last_admitted_observation_timestamp"] =
                json!("2024-02-01T00:00:00Z")
        });
        assert_rejected_after(valid.clone(), |v| {
            v["evidence_contract"]["warmup_sufficiency"]["source"] = json!("prebuilt_stream")
        });
        assert_rejected_after(valid.clone(), |v| {
            v["evidence_contract"]["effective_bar_limit"] = json!(99)
        });
        assert_rejected_after(valid.clone(), |v| {
            v["observation_summary"]["observation_count"] = json!(9)
        });
        assert_rejected_after(valid.clone(), |v| {
            v["cost_view_results"]["none"]["observation_stream_sha256"] = json!(SHA_B)
        });
        assert_rejected_after(valid.clone(), |v| {
            v["cost_view_results"]
                .as_object_mut()
                .expect("cost views")
                .remove("none");
        });
        assert_rejected_after(valid.clone(), |v| {
            v["cost_view_results"]["none"]["replay_result"]["profileSnapshotSha256"] = json!(SHA_A)
        });
        assert_rejected_after(valid.clone(), |v| {
            v["cost_view_results"]["none"]["replay_result"]["metrics"]["terminalAdjustedTotalNetR"] =
                json!(1.1)
        });
        assert_rejected_after(valid.clone(), |v| {
            v["cost_view_results"]["research_conservative"]["replay_result"]["metrics"]["terminalValuation"] =
                json!({})
        });
        assert_rejected_after(valid.clone(), |v| {
            v["diagnostics"]["cost_view_decision_path_sha256"] = json!(SHA_A)
        });
        assert_rejected_after(valid.clone(), |v| {
            v["cost_view_results"]["none"]["replay_result"]["graphTraces"] = json!([{}])
        });
        Ok(())
    }

    #[test]
    fn immutable_task_payload_binds_requested_bar_limit() -> Result<()> {
        let valid = valid_v3_result();
        let task = json!({
            "analysisWindowStart":"2024-01-01T00:00:00Z",
            "analysisWindowEnd":"2024-02-01T00:00:00Z"
        });
        let payload = json!({"bar_limit":100});
        validate_v3_candidate_window_result(&valid, Some(&task), Some(&payload))?;
        let tampered = json!({"bar_limit":101});
        assert!(validate_v3_candidate_window_result(&valid, Some(&task), Some(&tampered)).is_err());
        Ok(())
    }

    #[test]
    fn frozen_python_worker_artifact_identity_rejects_size_and_hash_tamper() -> Result<()> {
        let valid = freeze_worker_artifact(valid_v3_result());
        validate_v3_candidate_window_result(&valid, None, None)?;
        validate_worker_material_identity(&valid)?;
        let mut size_tamper = valid.clone();
        size_tamper["artifact_size_bytes"] = json!(2);
        assert!(validate_worker_material_identity(&size_tamper).is_err());
        let mut hash_tamper = valid;
        hash_tamper["artifact_sha256"] = json!(SHA_B);
        assert!(validate_worker_material_identity(&hash_tamper).is_err());
        Ok(())
    }

    #[test]
    fn frozen_python_rejected_result_contract_accepts_v1_v2_and_rejects_tampers() -> Result<()> {
        let task = rejection_task();
        for v2 in [false, true] {
            let valid = valid_rejection(v2);
            validate_warmup_rejected_candidate_window_result(&valid, &task)?;
            let mut bad_outcome = valid.clone();
            bad_outcome["evaluation_outcome"]["unexpected"] = json!(true);
            assert!(validate_warmup_rejected_candidate_window_result(&bad_outcome, &task).is_err());
            let mut bad_reason = valid.clone();
            bad_reason["evaluation_outcome"]["reason_code"] = json!("other");
            assert!(validate_warmup_rejected_candidate_window_result(&bad_reason, &task).is_err());
            let mut bad_lease = valid.clone();
            bad_lease["evaluation_outcome"]["worker_lease_id"] = json!("unsafe lease");
            assert!(validate_warmup_rejected_candidate_window_result(&bad_lease, &task).is_err());
            let mut bad_error_hash = valid.clone();
            bad_error_hash["evaluation_outcome"]["worker_error_sha256"] = json!(SHA_A);
            assert!(
                validate_warmup_rejected_candidate_window_result(&bad_error_hash, &task).is_err()
            );
            let mut bad_completion_hash = valid.clone();
            bad_completion_hash["evaluation_outcome"]["worker_completion_sha256"] =
                json!("not-a-hash");
            assert!(
                validate_warmup_rejected_candidate_window_result(&bad_completion_hash, &task)
                    .is_err()
            );
            let mut bad_job = valid.clone();
            bad_job["job_id"] = json!("other-job");
            assert!(validate_warmup_rejected_candidate_window_result(&bad_job, &task).is_err());
            let mut bad_task = valid.clone();
            bad_task["analysis_window_end"] = json!("2024-03-01T00:00:00Z");
            assert!(validate_warmup_rejected_candidate_window_result(&bad_task, &task).is_err());
            let mut bad_sha = valid.clone();
            bad_sha["artifact_sha256"] = json!(SHA_A);
            assert!(validate_warmup_rejected_candidate_window_result(&bad_sha, &task).is_err());
            let mut bad_size = valid;
            bad_size["artifact_size_bytes"] = json!(1);
            assert!(validate_warmup_rejected_candidate_window_result(&bad_size, &task).is_err());
        }
        Ok(())
    }

    #[test]
    fn stage_projection_gzip_is_byte_exact_with_python_zlib_oracle() -> Result<()> {
        let projection = stage_projection(&json!({"a": 1, "b": "x"}))?;
        let encoded = projection["blobBase64"].as_str().expect("base64 string");
        let blob = base64::engine::general_purpose::STANDARD.decode(encoded)?;
        assert_eq!(
            hex(&blob),
            "1f8b08000000000002ffab564a54b232d4514a52b252aa50aa050054277ff20f000000"
        );
        assert_eq!(projection["semanticSizeBytes"], 15);
        Ok(())
    }

    #[test]
    fn explicit_null_final_execution_children_match_python_none_projection() -> Result<()> {
        assert_eq!(trailing_projection(Some(&Value::Null))?, Value::Null);
        assert_eq!(position_projection(Some(&Value::Null))?, Value::Null);
        assert_eq!(pending_projection(Some(&Value::Null))?, Value::Null);
        assert_eq!(final_execution_projection(Some(&Value::Null))?, Value::Null);
        Ok(())
    }

    #[test]
    fn raw_blob_verification_rejects_identity_tamper() -> Result<()> {
        let raw = b"not-a-real-gzip";
        let reference = json!({
            "schemaVersion": "temporal_qd_tail_raw_result_ref_v1",
            "relativePath": "results/task.json.gz",
            "resultSha256": sha_bytes(b"semantic"),
            "codec": "gzip-json-v1",
            "semanticSizeBytes": 8,
            "uncompressedSha256": sha_bytes(b"pretty"),
            "uncompressedSizeBytes": 6,
            "blobSha256": sha_bytes(raw),
            "blobSizeBytes": raw.len(),
        });
        verify_raw_blob(&sha_bytes(raw), raw.len() as u64, &reference)?;
        let mut tampered = raw.to_vec();
        tampered[0] ^= 1;
        assert!(verify_raw_blob(&sha_bytes(&tampered), tampered.len() as u64, &reference).is_err());
        Ok(())
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}
