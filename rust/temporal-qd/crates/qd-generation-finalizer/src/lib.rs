//! Restartable native transaction from sealed rotating-evidence inputs to a
//! committed generation boundary.
//!
//! Python remains the semantic oracle and campaign launcher. This crate is a
//! historical parity prototype for the deterministic final half: it rebuilds
//! cumulative evidence, projects the rich parent archive, and emits a compact
//! commit whose restart hashes every bound output. It is not a production
//! cutover authority yet: the supervisor gateway still sources funnel/record
//! material from Python's completed boundary, and externally supplied
//! auxiliary receipts are rejected until they carry reopenable descriptors.

#![recursion_limit = "512"]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_json_line, canonical_sha256, canonical_sha256_without_object_field,
};
use temporal_qd_tail_reducer::aggregate_realized_behavior;

pub const SOURCE_SCHEMA: &str = "temporal_qd_generation_finalization_source_v2";
const PREVIOUS_PARENT_SUMMARY_SCHEMA: &str = "temporal_qd_previous_parent_archive_summary_v1";
pub const MANIFEST_SCHEMA: &str = "temporal_qd_generation_finalization_manifest_v2";
pub const PLAN_SCHEMA: &str = "temporal_qd_auxiliary_evidence_plan_v1";
pub const RECEIPT_SCHEMA: &str = "temporal_qd_auxiliary_campaign_receipt_v1";
pub const COMMIT_SCHEMA: &str = "temporal_qd_generation_commit_v1";
pub const EXECUTION_SCHEMA: &str = "temporal_qd_generation_finalization_execution_v1";
pub const OPERATION: &str = "finalize_rotating_generation";
pub const PLAN_PATH: &str = "auxiliary-evidence-plan.json";
pub const CUMULATIVE_PATH: &str = "evidence/cumulative-archive.json";
pub const ARCHIVE_PATH: &str = "archive.json";
pub const CHECKPOINT_PATH: &str = "evidence/checkpoint.json";
pub const LEDGER_PATH: &str = "evidence/generation-ledger.json";
pub const RECORD_PATH: &str = "generation-record.json";
pub const STATE_PATCH_PATH: &str = "generation-state-patch.json";
pub const STATE_APPLICATION_SIDECAR_PATH: &str = "generation-state-application-sidecar.json";
pub const FUNNEL_PATH: &str = "generation-funnel.json";
pub const FUNNEL_SNAPSHOT_PATH: &str = "generation-funnel-snapshot.json";
pub const COMMIT_PATH: &str = "generation-commit.json";
pub const STATE_APPLICATION_SIDECAR_SCHEMA: &str =
    "temporal_qd_v5_generation_state_application_sidecar_v1";
pub const FAST_EPHEMERAL_SOURCE_SCHEMA: &str =
    "temporal_qd_v5_fast_ephemeral_finalization_source_v1";
pub const FAST_EPHEMERAL_MANIFEST_SCHEMA: &str =
    "temporal_qd_v5_fast_ephemeral_finalization_manifest_v1";
pub const FAST_EPHEMERAL_RESULT_SCHEMA: &str =
    "temporal_qd_v5_fast_ephemeral_finalization_result_v1";

const ROTATING_SCHEMA: &str = "temporal_qd_rotating_evidence_v1";
const COHORT_SCHEMA: &str = "temporal_qd_current_panel_evaluation_cohort_v1";
const PROVISIONAL_SCHEMA: &str = "temporal_qd_provisional_survivors_v1";
const BUNDLE_SCHEMA: &str = "temporal_qd_candidate_panel_evidence_bundle_v1";
const WINDOW_SCHEMA: &str = "temporal_qd_candidate_window_evidence_v1";
const CUMULATIVE_SCHEMA: &str = "temporal_qd_cumulative_breeder_archive_v1";
const CHECKPOINT_SCHEMA: &str = "temporal_qd_rotating_evidence_checkpoint_v1";
const ARCHIVE_SCHEMA: &str = "temporal_qd_archive_v3";
const QD_VERSION: &str = "temporal_qd_evolution_v3";
const QD_POLICY_NAME: &str = "stage5e7_v4_corrected_descriptor_archive";
const QD_POLICY_SHA256: &str =
    "sha256:f4ab045e2962aea0ac3336122592205a7d42274df4695ac51415e9b2facca2bd";
const DIRECTIONAL_QD_POLICY_NAME: &str = "stage5e7_v5_direction_aware_breeding_archive";
const DIRECTIONAL_QD_POLICY_SHA256: &str =
    "sha256:c8ea30b0a9d2825844d4267be9e4ccf82f36dc43a741ac061d41508fe486c3da";
const FUNNEL_SOURCE_SCHEMA: &str = "temporal_qd_native_funnel_reduction_source_v1";
const FUNNEL_SCHEMA: &str = "temporal_generation_funnel_v1";
const FUNNEL_SNAPSHOT_SCHEMA: &str = "temporal_generation_funnel_supervisor_snapshot_v1";
const CAMPAIGN_SEAL_SCHEMA: &str = "temporal_qd_campaign_seal_v1";
const TAIL_TRANSACTION_SCHEMA: &str = "temporal_qd_generation_tail_transaction_v1";
const TAIL_REDUCTION_SCHEMA: &str = "temporal_qd_native_tail_reduction_result_v1";
const AUXILIARY_BUNDLE_ARTIFACT_SCHEMA: &str = "temporal_qd_auxiliary_panel_bundles_v1";
const ROTATING_CAMPAIGN_RECEIPT_SCHEMA: &str = "temporal_qd_v5_rotating_campaign_receipt_v2";

#[derive(Clone, Debug)]
struct Manifest {
    runtime_authority_sha256: String,
    semantic_authority_sha256: String,
    source_path: PathBuf,
    source_sha256: String,
    manifest_sha256: String,
}

#[derive(Clone, Debug)]
struct Source {
    value: Value,
    generation_index: u64,
    rotating_sha: String,
    current_panel_id: String,
    required_panel_ids: Vec<String>,
    breeder_width: usize,
    cohort: Value,
    provisional: Value,
    baseline_bundles: Vec<Value>,
    complete_bundle_snapshot: bool,
    receipts: Vec<Value>,
    prior_cumulative: Option<Value>,
    previous_parent: Value,
    archive_policy: Value,
    rich_members: Vec<Value>,
    current_member_count: u64,
    cell_capacity: usize,
    campaigns: Vec<Value>,
    funnel_source: Value,
    state_basis: Value,
    semantic_authority_sha256: String,
    runtime_authority_sha256: String,
    completed_generation_records: Vec<Value>,
    proposal_state_authority: Value,
    expected_plan: Option<Value>,
    source_sha256: String,
}

struct FinalizedOutputs<'a> {
    cumulative: &'a Value,
    archive: &'a Value,
    funnel: &'a Value,
    funnel_snapshot: &'a Value,
    checkpoint: &'a Value,
    ledger: &'a Value,
}

pub fn execute_manifest(manifest_path: &Path) -> Result<Value> {
    let started = Instant::now();
    let manifest_path = existing_file(manifest_path, "generation finalization manifest")?;
    let output_dir = manifest_path.parent().context("manifest has no parent")?;
    let manifest_raw = fs::read(&manifest_path)?;
    let manifest_value: Value =
        serde_json::from_slice(&manifest_raw).context("parse generation finalization manifest")?;
    if text(&manifest_value, "schemaVersion").ok() == Some(FAST_EPHEMERAL_MANIFEST_SCHEMA) {
        return execute_fast_ephemeral_manifest(
            &manifest_path,
            output_dir,
            &manifest_raw,
            &manifest_value,
            started,
        );
    }
    let commit_path = output_dir.join(COMMIT_PATH);
    let manifest = parse_manifest(&manifest_raw, output_dir)?;
    if commit_path.exists() {
        let commit = read_self_hashed(&commit_path, "commitSha256", COMMIT_SCHEMA)?;
        ensure!(
            sha(&commit, "manifestSha256")? == manifest.manifest_sha256,
            "committed manifest binding drifted"
        );
        ensure!(
            sha(&commit, "sourceSha256")? == manifest.source_sha256,
            "committed source binding drifted"
        );
        ensure!(
            sha(&commit, "runtimeAuthoritySha256")? == manifest.runtime_authority_sha256,
            "committed runtime authority binding drifted"
        );
        ensure!(
            sha(&commit, "semanticAuthoritySha256")? == manifest.semantic_authority_sha256,
            "committed semantic authority binding drifted"
        );
        validate_commit_outputs(output_dir, &commit)?;
        validate_state_application_sidecar(output_dir, &manifest, &commit)?;
        return Ok(restart_execution(&manifest, commit, started));
    }
    let source = load_source(&manifest).context("load generation finalization source")?;
    let plan = build_auxiliary_plan(&source).context("build generation finalization plan")?;

    if let Some(expected) = &source.expected_plan {
        ensure!(
            expected == &plan,
            "bound auxiliary plan differs from recomputed obligations"
        );
    } else {
        ensure!(
            source.receipts.is_empty(),
            "auxiliary receipts require an exact bound plan"
        );
    }
    publish_value_once(output_dir, PLAN_PATH, &plan)?;

    if !array(&plan, "obligations")?.is_empty() && source.receipts.is_empty() {
        return Ok(json!({
            "schemaVersion": EXECUTION_SCHEMA,
            "status": "awaiting_auxiliary_results",
            "sourceSha256": source.source_sha256,
            "manifestSha256": manifest.manifest_sha256,
            "auxiliaryPlanSha256": text(&plan, "planSha256")?,
            "obligationCount": array(&plan, "obligations")?.len(),
            "elapsedMilliseconds": started.elapsed().as_millis() as u64,
        }));
    }

    let bundles = admit_receipts(&source, &plan)?;
    let cumulative = build_cumulative_archive(&source, &bundles)?;
    let archive = build_parent_archive(&source, &cumulative)?;
    let (funnel, funnel_snapshot) = build_funnel(&source, &archive)?;
    let checkpoint = build_checkpoint(&source, &cumulative, &archive)?;
    let ledger = build_ledger(&source, &plan, &cumulative, &archive, &checkpoint)?;
    let record = build_generation_record(
        &source,
        &plan,
        &FinalizedOutputs {
            cumulative: &cumulative,
            archive: &archive,
            funnel: &funnel,
            funnel_snapshot: &funnel_snapshot,
            checkpoint: &checkpoint,
            ledger: &ledger,
        },
    )?;
    let state_patch = build_state_patch(&source, &record)?;

    publish_value_once(output_dir, CUMULATIVE_PATH, &cumulative)?;
    publish_value_once(output_dir, ARCHIVE_PATH, &archive)?;
    publish_value_once(output_dir, FUNNEL_PATH, &funnel)?;
    publish_value_once(output_dir, FUNNEL_SNAPSHOT_PATH, &funnel_snapshot)?;
    publish_value_once(output_dir, CHECKPOINT_PATH, &checkpoint)?;
    publish_value_once(output_dir, LEDGER_PATH, &ledger)?;
    publish_value_once(output_dir, RECORD_PATH, &record)?;
    publish_value_once(output_dir, STATE_PATCH_PATH, &state_patch)?;

    let mut commit = json!({
        "schemaVersion": COMMIT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "sourceSha256": source.source_sha256,
        "manifestSha256": manifest.manifest_sha256,
        "generationIndex": source.generation_index,
        "auxiliaryPlanSha256": text(&plan, "planSha256")?,
        "cumulativeArchive": descriptor(CUMULATIVE_PATH, &cumulative, "archiveSha256")?,
        "parentArchive": descriptor(ARCHIVE_PATH, &archive, "archiveSha256")?,
        "generationFunnel": descriptor(FUNNEL_PATH, &funnel, "artifactSha256")?,
        "generationFunnelSnapshot": descriptor(FUNNEL_SNAPSHOT_PATH, &funnel_snapshot, "snapshotSha256")?,
        "checkpoint": descriptor(CHECKPOINT_PATH, &checkpoint, "checkpointSha256")?,
        "ledger": descriptor(LEDGER_PATH, &ledger, "ledgerSha256")?,
        "generationRecord": descriptor(RECORD_PATH, &record, "generationRecordSha256")?,
        "statePatch": descriptor(STATE_PATCH_PATH, &state_patch, "statePatchSha256")?,
        "restartValidation": "compact_commit_and_output_hashes",
        "rawResultReads": 0,
    });
    object_mut(&mut commit, "generation commit")?.insert(
        "runtimeAuthoritySha256".into(),
        json!(manifest.runtime_authority_sha256),
    );
    object_mut(&mut commit, "generation commit")?.insert(
        "semanticAuthoritySha256".into(),
        json!(manifest.semantic_authority_sha256),
    );
    add_self_hash(&mut commit, "commitSha256")?;
    publish_value_once(output_dir, COMMIT_PATH, &commit)?;
    let sidecar =
        build_state_application_sidecar(&source, &manifest, &commit, &record, &state_patch)?;
    publish_value_once(output_dir, STATE_APPLICATION_SIDECAR_PATH, &sidecar)?;
    Ok(execution(&source, &manifest, &plan, commit, false, started))
}

fn execute_fast_ephemeral_manifest(
    manifest_path: &Path,
    output_dir: &Path,
    manifest_raw: &[u8],
    manifest_value: &Value,
    started: Instant,
) -> Result<Value> {
    ensure!(
        canonical_json_line(manifest_value)? == manifest_raw,
        "fast-ephemeral finalization manifest must be canonical JSON plus LF"
    );
    exact_keys(
        object(manifest_value, "fast-ephemeral finalization manifest")?,
        &[
            "schemaVersion",
            "operation",
            "runtimeAuthoritySha256",
            "sourcePath",
            "sourceSha256",
            "resultPath",
            "manifestSha256",
        ],
        "fast-ephemeral finalization manifest",
    )?;
    ensure!(
        text(manifest_value, "schemaVersion")? == FAST_EPHEMERAL_MANIFEST_SCHEMA
            && text(manifest_value, "operation")? == "finalize_fast_ephemeral_rotating_generation"
            && text(manifest_value, "resultPath")? == "fast-ephemeral-result.json",
        "fast-ephemeral finalization manifest is incompatible"
    );
    verify_self_hash(
        manifest_value,
        "manifestSha256",
        "fast-ephemeral finalization manifest",
    )?;
    sha(manifest_value, "runtimeAuthoritySha256")?;
    let source_path = PathBuf::from(text(manifest_value, "sourcePath")?);
    ensure!(
        source_path.is_absolute()
            && source_path.parent() == Some(output_dir)
            && source_path == output_dir.join("source.json"),
        "fast-ephemeral finalization source path is not fixed"
    );
    let source_raw = fs::read(&source_path).context("read fast-ephemeral finalization source")?;
    let source_value: Value =
        serde_json::from_slice(&source_raw).context("parse fast-ephemeral finalization source")?;
    ensure!(
        canonical_json_line(&source_value)? == source_raw,
        "fast-ephemeral finalization source must be canonical JSON plus LF"
    );
    ensure!(
        sha(&source_value, "sourceSha256")? == sha(manifest_value, "sourceSha256")?,
        "fast-ephemeral manifest/source identity drifted"
    );
    let source = load_fast_ephemeral_source(source_value)?;
    let (cumulative, archive) = reduce_fast_ephemeral_loaded_source(&source)?;
    let cumulative_path = output_dir.join(CUMULATIVE_PATH);
    let archive_path = output_dir.join(ARCHIVE_PATH);
    write_fast_value(
        &cumulative_path,
        &cumulative,
        "fast-ephemeral cumulative archive",
    )?;
    write_fast_value(&archive_path, &archive, "fast-ephemeral parent archive")?;
    let parent_schedule = member(
        member(&archive, "rotatingEvidenceTransaction")?,
        "parentSchedule",
    )?
    .clone();
    let mut result = json!({
        "schemaVersion": FAST_EPHEMERAL_RESULT_SCHEMA,
        "executionMode": "fast-ephemeral-v1",
        "generationIndex": source.generation_index,
        "manifestSha256": sha(manifest_value,"manifestSha256")?,
        "sourceSha256": source.source_sha256,
        "cumulativeArchive": descriptor(CUMULATIVE_PATH, &cumulative, "archiveSha256")?,
        "parentArchive": descriptor(ARCHIVE_PATH, &archive, "archiveSha256")?,
        "parentSchedule": parent_schedule,
        "candidateCount": source.current_member_count,
        "memberCount": unsigned(&archive,"memberCount")?,
        "occupiedCellCount": unsigned(&archive,"occupiedCellCount")?,
        "newCellCount": unsigned(&archive,"newCellCount")?,
        "elapsedMilliseconds": started.elapsed().as_millis() as u64
    });
    add_self_hash(&mut result, "resultSha256")?;
    write_fast_value(
        &output_dir.join("fast-ephemeral-result.json"),
        &result,
        "fast-ephemeral finalization result",
    )?;
    let _ = manifest_path;
    Ok(result)
}

/// Apply the exact historical fast-ephemeral reducer to one sealed source.
///
/// This is deliberately a narrow research seam: it opens the same source
/// contract and calls the same cumulative and parent-archive builders as the
/// production fast-ephemeral manifest path, but leaves publication and path
/// binding to the caller. Counterfactual work may use it only after proving
/// byte-for-byte Variant-0 parity against a sealed source.
pub fn reduce_fast_ephemeral_source(source_value: Value) -> Result<(Value, Value)> {
    let source = load_fast_ephemeral_source(source_value)?;
    reduce_fast_ephemeral_loaded_source(&source)
}

fn reduce_fast_ephemeral_loaded_source(source: &Source) -> Result<(Value, Value)> {
    let plan = build_auxiliary_plan(source)?;
    ensure!(
        array(&plan, "obligations")?.is_empty(),
        "fast-ephemeral finalization source lacks complete panel evidence"
    );
    let bundles = admit_receipts(source, &plan)?;
    let cumulative = build_cumulative_archive(source, &bundles)?;
    let archive = build_parent_archive(source, &cumulative)?;
    Ok((cumulative, archive))
}

fn load_fast_ephemeral_source(value: Value) -> Result<Source> {
    exact_keys(
        object(&value, "fast-ephemeral finalization source")?,
        &[
            "schemaVersion",
            "generationIndex",
            "rotatingEvidence",
            "cohort",
            "provisional",
            "selectedRichMembers",
            "candidatePanelBundles",
            "previousCumulativeArchive",
            "previousParentArchiveSummary",
            "archivePolicy",
            "sourceSha256",
        ],
        "fast-ephemeral finalization source",
    )?;
    ensure!(
        text(&value, "schemaVersion")? == FAST_EPHEMERAL_SOURCE_SCHEMA,
        "fast-ephemeral finalization source schema is incompatible"
    );
    verify_self_hash(&value, "sourceSha256", "fast-ephemeral finalization source")?;
    let generation_index = unsigned(&value, "generationIndex")?;
    let rotating = member(&value, "rotatingEvidence")?.clone();
    ensure!(
        text(&rotating, "schemaVersion")? == ROTATING_SCHEMA,
        "fast-ephemeral rotating contract is incompatible"
    );
    verify_self_hash(&rotating, "rotatingEvidenceSha256", "rotating contract")?;
    let rotating_sha = sha(&rotating, "rotatingEvidenceSha256")?;
    let panels = array(&rotating, "panels")?;
    let cycle = unsigned(
        member(&rotating, "absoluteGenerationMapping")?,
        "cycleLength",
    )? as usize;
    ensure!(
        cycle > 0 && cycle == panels.len(),
        "rotating panel cycle is invalid"
    );
    let current_panel_id =
        text(&panels[(generation_index as usize - 1) % cycle], "panelId")?.to_owned();
    let mut required_panel_ids = Vec::new();
    for index in 0..generation_index as usize {
        let panel = text(&panels[index % cycle], "panelId")?.to_owned();
        if !required_panel_ids.contains(&panel) {
            required_panel_ids.push(panel);
        }
    }
    let breeder_width = unsigned(member(&rotating, "robustSelection")?, "breederWidth")? as usize;
    let cohort = member(&value, "cohort")?.clone();
    verify_self_hash(&cohort, "cohortSha256", "cohort")?;
    ensure!(
        unsigned(&cohort, "generationIndex")? == generation_index
            && sha(&cohort, "rotatingEvidenceSha256")? == rotating_sha
            && text(&cohort, "panelId")? == current_panel_id,
        "fast-ephemeral cohort binding drifted"
    );
    let provisional = member(&value, "provisional")?.clone();
    verify_self_hash(&provisional, "provisionalSha256", "provisional")?;
    ensure!(
        unsigned(&provisional, "generationIndex")? == generation_index
            && sha(&provisional, "cohortSha256")? == sha(&cohort, "cohortSha256")?,
        "fast-ephemeral provisional binding drifted"
    );
    let prior_cumulative = nullable_member(&value, "previousCumulativeArchive")?.cloned();
    if let Some(previous) = &prior_cumulative {
        verify_self_hash(previous, "archiveSha256", "previous cumulative archive")?;
    }
    let previous_parent = member(&value, "previousParentArchiveSummary")?.clone();
    validate_previous_parent_summary(&previous_parent)?;
    let archive_policy = member(&value, "archivePolicy")?.clone();
    verify_self_hash(
        &archive_policy,
        "policyBindingSha256",
        "archive policy binding",
    )?;
    validate_corrected_archive_policy(&archive_policy)?;
    let rich_members =
        validate_selected_rich_members(&value, generation_index, &cohort, &provisional)?;
    let baseline_bundles = array(&value, "candidatePanelBundles")?.to_vec();
    let current_member_count = array(&cohort, "candidates")?.len() as u64;
    let cell_capacity = derive_cell_capacity(&archive_policy)?;
    let source_sha256 = sha(&value, "sourceSha256")?;
    Ok(Source {
        value,
        generation_index,
        rotating_sha,
        current_panel_id,
        required_panel_ids,
        breeder_width,
        cohort,
        provisional,
        baseline_bundles,
        complete_bundle_snapshot: true,
        receipts: Vec::new(),
        prior_cumulative,
        previous_parent,
        archive_policy,
        rich_members,
        current_member_count,
        cell_capacity,
        campaigns: Vec::new(),
        funnel_source: Value::Null,
        state_basis: Value::Null,
        semantic_authority_sha256:
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
        runtime_authority_sha256:
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
        completed_generation_records: Vec::new(),
        proposal_state_authority: Value::Null,
        expected_plan: None,
        source_sha256,
    })
}

fn write_fast_value(path: &Path, value: &Value, name: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {name} parent"))?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create {name}"))?;
    file.write_all(&canonical_json_line(value)?)
        .with_context(|| format!("write {name}"))?;
    file.flush().with_context(|| format!("flush {name}"))?;
    Ok(())
}

fn parse_manifest(raw: &[u8], output_dir: &Path) -> Result<Manifest> {
    let value: Value = serde_json::from_slice(raw).context("parse finalization manifest")?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "manifest must be canonical JSON plus LF"
    );
    let map = object(&value, "manifest")?;
    let manifest_keys = [
        "schemaVersion",
        "contractVersion",
        "operation",
        "runtimeAuthoritySha256",
        "semanticAuthoritySha256",
        "sourcePath",
        "sourceSha256",
        "resultPath",
        "manifestSha256",
    ];
    exact_keys(map, &manifest_keys, "manifest")?;
    ensure!(
        text(&value, "schemaVersion")? == MANIFEST_SCHEMA,
        "unsupported manifest schema"
    );
    ensure!(
        text(&value, "contractVersion")? == CONTRACT_VERSION,
        "native contract version mismatch"
    );
    ensure!(
        text(&value, "operation")? == OPERATION,
        "unsupported operation"
    );
    ensure!(
        text(&value, "resultPath")? == COMMIT_PATH,
        "result path must be fixed"
    );
    let hash = sha(&value, "manifestSha256")?;
    ensure!(
        canonical_sha256_without_object_field(&value, "manifestSha256")? == hash,
        "manifest identity mismatch"
    );
    let source_path = PathBuf::from(text(&value, "sourcePath")?);
    ensure!(
        source_path.is_absolute(),
        "finalization source execution path must be absolute"
    );
    ensure!(
        source_path
            .parent()
            .context("finalization source has no parent")?
            .canonicalize()?
            == output_dir.canonicalize()?,
        "finalization source escapes finalization root"
    );
    Ok(Manifest {
        runtime_authority_sha256: sha(&value, "runtimeAuthoritySha256")?,
        semantic_authority_sha256: sha(&value, "semanticAuthoritySha256")?,
        source_path,
        source_sha256: sha(&value, "sourceSha256")?,
        manifest_sha256: hash,
    })
}

fn load_source(manifest: &Manifest) -> Result<Source> {
    let path = existing_file_under(
        &manifest.source_path,
        manifest
            .source_path
            .parent()
            .context("source has no parent")?,
        "finalization source",
    )?;
    let raw = fs::read(path)?;
    let value: Value = serde_json::from_slice(&raw).context("parse finalization source")?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "source must be canonical JSON plus LF"
    );
    ensure!(
        text(&value, "schemaVersion")? == SOURCE_SCHEMA,
        "unsupported source schema"
    );
    ensure!(
        text(&value, "contractVersion")? == CONTRACT_VERSION,
        "source contract version mismatch"
    );
    exact_keys(
        object(&value, "finalization source")?,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "semanticAuthoritySha256",
            "runtimeAuthoritySha256",
            "stateBasis",
            "completedGenerationRecords",
            "proposalStateAuthority",
            "rotatingEvidence",
            "cohort",
            "provisional",
            "panelCoverage",
            "selectedRichMembers",
            "baselineCandidatePanelBundles",
            "previousCumulativeArchive",
            "previousParentArchiveSummary",
            "archivePolicy",
            "admittedCampaignLedger",
            "funnelReductionSource",
            "sourceSha256",
        ],
        "finalization source",
    )?;
    let source_sha = sha(&value, "sourceSha256")?;
    ensure!(
        source_sha == manifest.source_sha256,
        "manifest/source identity mismatch"
    );
    ensure!(
        canonical_sha256_without_object_field(&value, "sourceSha256")? == source_sha,
        "source identity mismatch"
    );
    let semantic_authority_sha256 = sha(&value, "semanticAuthoritySha256")?;
    let runtime_authority_sha256 = sha(&value, "runtimeAuthoritySha256")?;
    ensure!(
        semantic_authority_sha256 == manifest.semantic_authority_sha256,
        "source semantic authority binding mismatch"
    );
    ensure!(
        runtime_authority_sha256 == manifest.runtime_authority_sha256,
        "source runtime authority binding mismatch"
    );
    let generation_index = unsigned(&value, "generationIndex")?;
    ensure!(generation_index > 0, "generation index must be positive");
    let rotating = member(&value, "rotatingEvidence")?.clone();
    ensure!(
        text(&rotating, "schemaVersion")? == ROTATING_SCHEMA,
        "unsupported rotating contract"
    );
    verify_self_hash(&rotating, "rotatingEvidenceSha256", "rotating contract")?;
    let rotating_sha = sha(&rotating, "rotatingEvidenceSha256")?;
    let panels = array(&rotating, "panels")?;
    let cycle = unsigned(
        member(&rotating, "absoluteGenerationMapping")?,
        "cycleLength",
    )? as usize;
    ensure!(
        cycle > 0 && cycle == panels.len(),
        "rotating panel cycle is invalid"
    );
    let current_panel_id =
        text(&panels[(generation_index as usize - 1) % cycle], "panelId")?.to_owned();
    let mut required_panel_ids = Vec::new();
    for index in 0..generation_index as usize {
        let id = text(&panels[index % cycle], "panelId")?.to_owned();
        if !required_panel_ids.contains(&id) {
            required_panel_ids.push(id);
        }
    }
    let breeder_width = unsigned(member(&rotating, "robustSelection")?, "breederWidth")? as usize;
    ensure!(breeder_width > 0, "breeder width must be positive");
    let cohort = member(&value, "cohort")?.clone();
    verify_self_hash(&cohort, "cohortSha256", "cohort")?;
    ensure!(
        text(&cohort, "schemaVersion")? == COHORT_SCHEMA
            && unsigned(&cohort, "generationIndex")? == generation_index
            && sha(&cohort, "rotatingEvidenceSha256")? == rotating_sha
            && text(&cohort, "panelId")? == current_panel_id,
        "cohort binding mismatch"
    );
    let provisional = member(&value, "provisional")?.clone();
    verify_self_hash(&provisional, "provisionalSha256", "provisional")?;
    ensure!(
        text(&provisional, "schemaVersion")? == PROVISIONAL_SCHEMA
            && unsigned(&provisional, "generationIndex")? == generation_index
            && sha(&provisional, "cohortSha256")? == sha(&cohort, "cohortSha256")?,
        "provisional binding mismatch"
    );
    let baseline_bundles = array(&value, "baselineCandidatePanelBundles")?.to_vec();
    // V2 is emitted only after the rotating prefinalizer has admitted an
    // exact full snapshot.  The finalizer never accepts externally supplied
    // receipts or an unchecked partial/auxiliary plan.
    let complete_bundle_snapshot = true;
    let receipts = Vec::new();
    let prior_cumulative = nullable_member(&value, "previousCumulativeArchive")?.cloned();
    if let Some(previous) = &prior_cumulative {
        verify_self_hash(previous, "archiveSha256", "previous cumulative archive")?;
    }
    let previous_parent = member(&value, "previousParentArchiveSummary")?.clone();
    validate_previous_parent_summary(&previous_parent)?;
    let archive_policy = member(&value, "archivePolicy")?.clone();
    verify_self_hash(
        &archive_policy,
        "policyBindingSha256",
        "archive policy binding",
    )?;
    validate_corrected_archive_policy(&archive_policy)?;
    let rich_members =
        validate_selected_rich_members(&value, generation_index, &cohort, &provisional)?;
    let current_member_count = array(&cohort, "candidates")?.len() as u64;
    let cell_capacity = derive_cell_capacity(&archive_policy)?;
    let expected_plan = None;
    let funnel_source = member(&value, "funnelReductionSource")?.clone();
    ensure!(
        text(&funnel_source, "schemaVersion")? == FUNNEL_SOURCE_SCHEMA,
        "unsupported funnel reduction source"
    );
    verify_self_hash(
        &funnel_source,
        "funnelSourceSha256",
        "funnel reduction source",
    )?;
    let state_basis = member(&value, "stateBasis")?.clone();
    validate_state_basis(&state_basis, generation_index)?;
    let completed_generation_records = array(&value, "completedGenerationRecords")?.to_vec();
    ensure!(
        canonical_sha256(&Value::Array(completed_generation_records.clone()))?
            == sha(&state_basis, "completedGenerationsSha256")?,
        "completed generation records drifted from state basis"
    );
    for record in &completed_generation_records {
        verify_self_hash(record, "generationRecordSha256", "prior generation record")?;
    }
    let proposal_state_authority = member(&value, "proposalStateAuthority")?.clone();
    validate_proposal_state_authority(&proposal_state_authority)?;
    let campaigns = validate_admitted_campaign_ledger(
        member(&value, "admittedCampaignLedger")?,
        generation_index,
        &rotating_sha,
        &cohort,
        &provisional,
    )?;
    validate_panel_coverage(
        member(&value, "panelCoverage")?,
        generation_index,
        &rotating_sha,
        &cohort,
        &provisional,
        &required_panel_ids,
        &baseline_bundles,
    )?;
    Ok(Source {
        value: value.clone(),
        generation_index,
        rotating_sha,
        current_panel_id,
        required_panel_ids,
        breeder_width,
        cohort,
        provisional,
        baseline_bundles,
        complete_bundle_snapshot,
        receipts,
        prior_cumulative,
        previous_parent,
        archive_policy,
        rich_members,
        current_member_count,
        cell_capacity,
        campaigns,
        funnel_source,
        state_basis,
        semantic_authority_sha256,
        runtime_authority_sha256,
        completed_generation_records,
        proposal_state_authority,
        expected_plan,
        source_sha256: source_sha,
    })
}

fn validate_corrected_archive_policy(value: &Value) -> Result<()> {
    object(value, "archive policy binding")?;
    ensure!(
        text(value, "schemaVersion")? == "temporal_qd_archive_policy_binding_v1"
            && text(value, "qdVersion")? == QD_VERSION,
        "archive policy binding schema is invalid"
    );
    let name = text(value, "policyName")?;
    let policy_sha = sha(value, "policySha256")?;
    let expected_sha = match name {
        QD_POLICY_NAME => QD_POLICY_SHA256,
        DIRECTIONAL_QD_POLICY_NAME => DIRECTIONAL_QD_POLICY_SHA256,
        _ => {
            return Err(anyhow!(
                "archive policy binding is not a recognized v4/v5 policy"
            ));
        }
    };
    ensure!(
        policy_sha == expected_sha
            && canonical_sha256(member(value, "frozenPolicy")?)? == expected_sha,
        "archive policy frozen identity mismatch"
    );
    Ok(())
}

fn derive_cell_capacity(archive_policy: &Value) -> Result<usize> {
    let capacity = unsigned(
        member(member(archive_policy, "frozenPolicy")?, "archive")?,
        "defaultCellCapacity",
    )? as usize;
    ensure!(
        capacity > 0,
        "frozen archive policy cell capacity must be positive"
    );
    Ok(capacity)
}

fn validate_previous_parent_summary(summary: &Value) -> Result<()> {
    let map = object(summary, "previous parent summary")?;
    let mut keys = vec![
        "schemaVersion",
        "archiveSha256",
        "candidateCountSeen",
        "memberCount",
        "cellIds",
        "summarySha256",
    ];
    if map.contains_key("bidirectionalPairPolicy") {
        keys.push("bidirectionalPairPolicy");
    }
    exact_keys(map, &keys, "previous parent summary")?;
    ensure!(
        text(summary, "schemaVersion")? == PREVIOUS_PARENT_SUMMARY_SCHEMA,
        "unsupported previous parent summary schema"
    );
    sha(summary, "archiveSha256")?;
    unsigned(summary, "candidateCountSeen")?;
    unsigned(summary, "memberCount")?;
    let cell_ids = array(summary, "cellIds")?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("previous parent cell ID must be text"))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut sorted = cell_ids.clone();
    sorted.sort();
    sorted.dedup();
    ensure!(
        cell_ids == sorted,
        "previous parent cell IDs must be unique and sorted"
    );
    if let Some(pair_policy) = map.get("bidirectionalPairPolicy") {
        object(pair_policy, "previous parent bidirectional pair policy")?;
    }
    verify_self_hash(summary, "summarySha256", "previous parent summary")
}

fn validate_state_basis(value: &Value, generation_index: u64) -> Result<()> {
    let map = object(value, "generation state basis")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "configSha256",
            "generationIndex",
            "completedGenerationsSha256",
            "uniqueCandidatesEvaluated",
            "workerTasksCompleted",
            "nextImmigrantContinuationOrdinal",
            "uniqueIdentityCounts",
            "duplicateCounters",
            "proposalSlotCounters",
            "stateBasisSha256",
        ],
        "generation state basis",
    )?;
    ensure!(
        text(value, "schemaVersion")? == "temporal_qd_v5_generation_state_basis_v1"
            && unsigned(value, "generationIndex")? == generation_index,
        "state basis schema or generation drifted"
    );
    verify_self_hash(value, "stateBasisSha256", "generation state basis")?;
    for key in [
        "uniqueCandidatesEvaluated",
        "workerTasksCompleted",
        "nextImmigrantContinuationOrdinal",
    ] {
        unsigned(value, key)?;
    }
    Ok(())
}

fn validate_proposal_state_authority(value: &Value) -> Result<()> {
    exact_keys(
        object(value, "proposal state authority")?,
        &[
            "generationKind",
            "proposalManifestSha256",
            "proposalReceiptSha256",
            "generationJournalSha256",
            "inputIdentityLedgerSha256",
            "outputIdentityLedgerRelativePath",
            "outputIdentityLedgerSha256",
            "outputIdentityLedgerFileSha256",
        ],
        "proposal state authority",
    )?;
    let generation_kind = text(value, "generationKind")?;
    ensure!(
        matches!(generation_kind, "g0" | "evolved"),
        "proposal state authority generation kind is invalid"
    );
    for field in [
        "proposalManifestSha256",
        "proposalReceiptSha256",
        "generationJournalSha256",
        "outputIdentityLedgerSha256",
        "outputIdentityLedgerFileSha256",
    ] {
        sha(value, field)?;
    }
    let input = member(value, "inputIdentityLedgerSha256")?;
    match generation_kind {
        "g0" => ensure!(input.is_null(), "G0 must not have an input identity ledger"),
        "evolved" => {
            sha(value, "inputIdentityLedgerSha256")?;
        }
        _ => unreachable!(),
    }
    let relative = Path::new(text(value, "outputIdentityLedgerRelativePath")?);
    ensure!(
        !relative.is_absolute()
            && relative
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_))),
        "identity ledger output path must be a safe relative path"
    );
    Ok(())
}

fn validate_selected_rich_members(
    source: &Value,
    generation_index: u64,
    cohort: &Value,
    provisional: &Value,
) -> Result<Vec<Value>> {
    let value = member(source, "selectedRichMembers")?;
    exact_keys(
        object(value, "selected rich members")?,
        &[
            "schemaVersion",
            "generationIndex",
            "cohortSha256",
            "provisionalSha256",
            "members",
            "memberCount",
            "selectedRichMembersSha256",
        ],
        "selected rich members",
    )?;
    ensure!(
        text(value, "schemaVersion")? == "temporal_qd_selected_rich_members_v1"
            && unsigned(value, "generationIndex")? == generation_index
            && sha(value, "cohortSha256")? == sha(cohort, "cohortSha256")?
            && sha(value, "provisionalSha256")? == sha(provisional, "provisionalSha256")?,
        "selected rich member binding drifted"
    );
    verify_self_hash(value, "selectedRichMembersSha256", "selected rich members")?;
    let members = array(value, "members")?.to_vec();
    ensure!(
        unsigned(value, "memberCount")? == members.len() as u64,
        "selected rich member count drifted"
    );
    let provisional_ids = provisional_map_from_value(provisional)?;
    let mut seen = BTreeSet::new();
    for member_value in &members {
        let id = text(member_value, "candidateId")?;
        let provisional_member = provisional_ids
            .get(id)
            .ok_or_else(|| anyhow!("selected rich member is not provisional"))?;
        for field in [
            "candidateIdentitySha256",
            "programSha256",
            "profileSnapshotSha256",
        ] {
            ensure!(
                sha(member_value, field)? == sha(provisional_member, field)?,
                "selected rich member identity drifted"
            );
        }
        ensure!(
            seen.insert(id.to_owned()),
            "selected rich member repeats candidate"
        );
    }
    Ok(members)
}

fn provisional_map_from_value(provisional: &Value) -> Result<BTreeMap<String, Value>> {
    let mut result = BTreeMap::new();
    for candidate in array(provisional, "candidates")? {
        let id = text(candidate, "candidateId")?.to_owned();
        ensure!(
            result.insert(id, candidate.clone()).is_none(),
            "provisional repeats candidate"
        );
    }
    Ok(result)
}

fn validate_admitted_campaign_ledger(
    value: &Value,
    generation_index: u64,
    rotating_sha: &str,
    cohort: &Value,
    provisional: &Value,
) -> Result<Vec<Value>> {
    exact_keys(
        object(value, "admitted campaign ledger")?,
        &[
            "schemaVersion",
            "generationIndex",
            "rotatingEvidenceSha256",
            "cohortSha256",
            "provisionalSha256",
            "campaigns",
            "admittedCampaignLedgerSha256",
        ],
        "admitted campaign ledger",
    )?;
    ensure!(
        text(value, "schemaVersion")? == "temporal_qd_v5_admitted_campaign_ledger_v1"
            && unsigned(value, "generationIndex")? == generation_index
            && sha(value, "rotatingEvidenceSha256")? == rotating_sha
            && sha(value, "cohortSha256")? == sha(cohort, "cohortSha256")?
            && sha(value, "provisionalSha256")? == sha(provisional, "provisionalSha256")?,
        "admitted campaign ledger binding drifted"
    );
    verify_self_hash(
        value,
        "admittedCampaignLedgerSha256",
        "admitted campaign ledger",
    )?;
    let campaigns = array(value, "campaigns")?.to_vec();
    let mut roles = BTreeSet::new();
    for campaign in &campaigns {
        exact_keys(
            object(campaign, "admitted campaign ledger entry")?,
            &[
                "campaignRole",
                "panelId",
                "semanticReceiptSha256",
                "receiptSha256",
                "receipt",
            ],
            "admitted campaign ledger entry",
        )?;
        let role = text(campaign, "campaignRole")?;
        let panel = text(campaign, "panelId")?;
        ensure!(
            roles.insert((role.to_owned(), panel.to_owned())),
            "campaign ledger repeats role/panel"
        );
        let receipt = member(campaign, "receipt")?;
        ensure!(
            text(receipt, "schemaVersion")? == ROTATING_CAMPAIGN_RECEIPT_SCHEMA
                && unsigned(receipt, "generationIndex")? == generation_index
                && sha(receipt, "rotatingEvidenceSha256")? == rotating_sha
                && text(receipt, "campaignRole")? == role
                && text(receipt, "panelId")? == panel
                && sha(receipt, "semanticReceiptSha256")?
                    == sha(campaign, "semanticReceiptSha256")?
                && sha(receipt, "receiptSha256")? == sha(campaign, "receiptSha256")?,
            "campaign ledger receipt binding drifted"
        );
        verify_self_hash(receipt, "receiptSha256", "campaign receipt")?;
        unsigned(member(receipt, "campaignFreeze")?, "taskCount")?;
    }
    Ok(campaigns)
}

fn validate_panel_coverage(
    value: &Value,
    generation_index: u64,
    rotating_sha: &str,
    cohort: &Value,
    provisional: &Value,
    required_panels: &[String],
    bundles: &[Value],
) -> Result<()> {
    exact_keys(
        object(value, "panel coverage")?,
        &[
            "schemaVersion",
            "generationIndex",
            "rotatingEvidenceSha256",
            "cohortSha256",
            "provisionalSha256",
            "requiredPanelIds",
            "candidatePanelBundleSha256",
            "coverage",
            "panelCoverageSha256",
        ],
        "panel coverage",
    )?;
    ensure!(
        text(value, "schemaVersion")? == "temporal_qd_v5_panel_coverage_v1"
            && unsigned(value, "generationIndex")? == generation_index
            && sha(value, "rotatingEvidenceSha256")? == rotating_sha
            && sha(value, "cohortSha256")? == sha(cohort, "cohortSha256")?
            && sha(value, "provisionalSha256")? == sha(provisional, "provisionalSha256")?
            && sha(value, "candidatePanelBundleSha256")?
                == canonical_sha256(&Value::Array(bundles.to_vec()))?,
        "panel coverage binding drifted"
    );
    verify_self_hash(value, "panelCoverageSha256", "panel coverage")?;
    let panels = string_array(value, "requiredPanelIds")?;
    ensure!(
        panels == required_panels,
        "panel coverage required panel ordering drifted"
    );
    let provisional_ids = provisional_map_from_value(provisional)?;
    let coverage = object(member(value, "coverage")?, "panel coverage map")?;
    ensure!(
        coverage.len() == provisional_ids.len(),
        "panel coverage candidate count drifted"
    );
    for candidate in provisional_ids.keys() {
        let observed = member(member(value, "coverage")?, candidate)?;
        ensure!(
            string_array(observed, "panelIds")? == panels,
            "panel coverage is incomplete"
        );
    }
    Ok(())
}

fn build_auxiliary_plan(source: &Source) -> Result<Value> {
    let provisional = provisional_map(source)?;
    let mut available: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    if !source.complete_bundle_snapshot
        && let Some(previous) = &source.prior_cumulative
    {
        for bundle in array(previous, "candidatePanelBundles")? {
            validate_bundle(source, bundle)?;
            available
                .entry(text(bundle, "candidateId")?.to_owned())
                .or_default()
                .insert(text(bundle, "panelId")?.to_owned());
        }
    }
    for bundle in &source.baseline_bundles {
        validate_bundle(source, bundle)?;
        let candidate = text(bundle, "candidateId")?.to_owned();
        ensure!(
            provisional.contains_key(&candidate),
            "baseline bundle candidate is not provisional"
        );
        available
            .entry(candidate)
            .or_default()
            .insert(text(bundle, "panelId")?.to_owned());
    }
    let mut obligations = Vec::new();
    for candidate_id in provisional.keys() {
        for panel_id in &source.required_panel_ids {
            if !available
                .get(candidate_id)
                .is_some_and(|v| v.contains(panel_id))
            {
                obligations.push(json!({"candidateId": candidate_id, "panelId": panel_id}));
            }
        }
    }
    let mut by_panel: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in &obligations {
        by_panel
            .entry(text(row, "panelId")?.to_owned())
            .or_default()
            .push(text(row, "candidateId")?.to_owned());
    }
    let campaigns = by_panel
        .into_iter()
        .map(|(panel, candidates)| {
            json!({
                "role": "cumulative_backfill",
                "panelId": panel,
                "candidateIds": candidates,
                "candidateCount": candidates.len(),
            })
        })
        .collect::<Vec<_>>();
    let mut output = json!({
        "schemaVersion": PLAN_SCHEMA,
        "sourceGenerationIndex": source.generation_index,
        "rotatingEvidenceSha256": source.rotating_sha,
        "cohortSha256": sha(&source.cohort, "cohortSha256")?,
        "provisionalSha256": sha(&source.provisional, "provisionalSha256")?,
        "requiredPanelIds": source.required_panel_ids,
        "obligations": obligations,
        "obligationCount": obligations.len(),
        "campaigns": campaigns,
        "campaignCount": campaigns.len(),
    });
    add_self_hash(&mut output, "planSha256")?;
    Ok(output)
}

fn admit_receipts(source: &Source, plan: &Value) -> Result<BTreeMap<String, Vec<Value>>> {
    let provisional = provisional_map(source)?;
    let mut result: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    if !source.complete_bundle_snapshot
        && let Some(previous) = &source.prior_cumulative
    {
        for bundle in array(previous, "candidatePanelBundles")? {
            if provisional.contains_key(text(bundle, "candidateId")?) {
                insert_bundle(source, &mut result, bundle.clone())?;
            }
        }
    }
    for bundle in &source.baseline_bundles {
        insert_bundle(source, &mut result, bundle.clone())?;
    }
    let obligations = array(plan, "obligations")?
        .iter()
        .map(|row| {
            Ok((
                text(row, "candidateId")?.to_owned(),
                text(row, "panelId")?.to_owned(),
            ))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let mut received = BTreeSet::new();
    for receipt in &source.receipts {
        ensure!(
            text(receipt, "schemaVersion")? == RECEIPT_SCHEMA,
            "unsupported auxiliary receipt schema"
        );
        verify_self_hash(receipt, "receiptSha256", "auxiliary receipt")?;
        ensure!(
            text(receipt, "role")? == "cumulative_backfill",
            "unsupported auxiliary campaign role"
        );
        let panel_id = text(receipt, "panelId")?.to_owned();
        let candidate_ids = array(receipt, "candidateIds")?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow!("receipt candidate id is invalid"))
            })
            .collect::<Result<Vec<_>>>()?;
        let bundle_artifact = reopen_auxiliary_receipt(receipt)?;
        ensure!(
            text(&bundle_artifact, "panelId")? == panel_id,
            "receipt bundle artifact panel mismatch"
        );
        ensure!(
            member(&bundle_artifact, "campaignBinding")? == member(receipt, "campaignBinding")?,
            "receipt campaign binding differs from reopened artifact"
        );
        let bundles = array(&bundle_artifact, "candidatePanelBundles")?;
        ensure!(
            bundles.len() == candidate_ids.len(),
            "receipt candidate/bundle count mismatch"
        );
        let mut actual = Vec::new();
        for bundle in bundles {
            ensure!(
                text(bundle, "panelId")? == panel_id,
                "receipt bundle panel mismatch"
            );
            let candidate_id = text(bundle, "candidateId")?.to_owned();
            ensure!(
                obligations.contains(&(candidate_id.clone(), panel_id.clone())),
                "receipt fulfills no frozen obligation"
            );
            ensure!(
                received.insert((candidate_id.clone(), panel_id.clone())),
                "duplicate auxiliary obligation receipt"
            );
            actual.push(candidate_id);
            insert_bundle(source, &mut result, bundle.clone())?;
        }
        actual.sort();
        let mut expected = candidate_ids;
        expected.sort();
        ensure!(
            actual == expected,
            "receipt candidate list differs from bundles"
        );
    }
    ensure!(
        received == obligations,
        "auxiliary receipts do not fulfill the exact plan"
    );
    for candidate in provisional.keys() {
        let panels = result
            .get(candidate)
            .context("provisional candidate lacks evidence")?
            .iter()
            .map(|b| text(b, "panelId").map(str::to_owned))
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            panels == source.required_panel_ids,
            "candidate does not have exact ordered panel coverage"
        );
    }
    Ok(result)
}

fn reopen_auxiliary_receipt(receipt: &Value) -> Result<Value> {
    let seal_descriptor = member(receipt, "campaignSeal")?;
    let transaction_descriptor = member(receipt, "tailTransaction")?;
    let bundle_descriptor = member(receipt, "bundleArtifact")?;
    let seal = read_descriptor_bound_value(
        seal_descriptor,
        "campaignSealSha256",
        CAMPAIGN_SEAL_SCHEMA,
        "auxiliary campaign seal",
    )?;
    let transaction = read_descriptor_bound_value(
        transaction_descriptor,
        "transactionSha256",
        TAIL_TRANSACTION_SCHEMA,
        "auxiliary tail transaction",
    )?;
    ensure!(
        sha(&transaction, "campaignSealSha256")? == sha(&seal, "campaignSealSha256")?,
        "auxiliary transaction does not bind reopened campaign seal"
    );
    let transaction_path = existing_file(
        &PathBuf::from(text(transaction_descriptor, "path")?),
        "auxiliary tail transaction",
    )?;
    let reduction_path = transaction_path
        .parent()
        .context("auxiliary transaction has no parent")?
        .join("tail-reduction-result.json");
    let reduction = read_self_hashed(&reduction_path, "resultSha256", TAIL_REDUCTION_SCHEMA)?;
    ensure!(
        sha(&reduction, "resultSha256")? == sha(&transaction, "tailReductionResultSha256")?,
        "auxiliary transaction reduction identity drifted"
    );
    ensure!(
        member(&reduction, "evaluatedMembers")? == member(&transaction, "evaluatedMembers")?
            && member(&reduction, "provisional")? == member(&transaction, "provisional")?,
        "auxiliary transaction reduction projection drifted"
    );
    validate_evaluated_members_descriptor(
        transaction_path
            .parent()
            .context("auxiliary transaction has no parent")?,
        member(&transaction, "evaluatedMembers")?,
    )?;
    let bundle_artifact = read_descriptor_bound_value(
        bundle_descriptor,
        "bundleArtifactSha256",
        AUXILIARY_BUNDLE_ARTIFACT_SCHEMA,
        "auxiliary panel bundle artifact",
    )?;
    ensure!(
        sha(&bundle_artifact, "campaignSealSha256")? == sha(&seal, "campaignSealSha256")?
            && sha(&bundle_artifact, "tailTransactionSha256")?
                == sha(&transaction, "transactionSha256")?
            && sha(&bundle_artifact, "tailReductionResultSha256")?
                == sha(&reduction, "resultSha256")?
            && sha(&bundle_artifact, "evaluatedMembersSha256")?
                == canonical_sha256(member(&transaction, "evaluatedMembers")?)?,
        "auxiliary panel bundles do not bind reopened campaign transaction"
    );
    ensure!(
        text(&bundle_artifact, "panelId")? == text(receipt, "panelId")?
            && member(&bundle_artifact, "candidateIds")? == member(receipt, "candidateIds")?,
        "auxiliary receipt candidate/panel binding drifted"
    );
    Ok(bundle_artifact)
}

fn read_descriptor_bound_value(
    descriptor: &Value,
    identity_field: &str,
    schema: &str,
    name: &str,
) -> Result<Value> {
    let path = existing_file(&PathBuf::from(text(descriptor, "path")?), name)?;
    let value = read_self_hashed(&path, identity_field, schema)?;
    ensure!(
        sha(&value, identity_field)? == sha(descriptor, "sha256")?,
        "{name} descriptor identity drifted"
    );
    Ok(value)
}

fn validate_evaluated_members_descriptor(root: &Path, evaluated: &Value) -> Result<()> {
    let members = member(evaluated, "membersFile")?;
    let relative = PathBuf::from(text(members, "path")?);
    ensure!(
        relative.components().count() == 1,
        "evaluated members path must be a fixed sibling"
    );
    let path = existing_file(&root.join(relative), "evaluated members JSONL")?;
    let metadata = fs::metadata(&path)?;
    ensure!(
        metadata.len() == unsigned(members, "sizeBytes")?,
        "evaluated members size drifted"
    );
    ensure!(
        sha256_file(&path)? == sha(members, "rawSha256")?,
        "evaluated members content drifted"
    );
    let raw = fs::read(&path)?;
    ensure!(
        raw.last() == Some(&b'\n')
            && raw.iter().filter(|byte| **byte == b'\n').count()
                == unsigned(members, "recordCount")? as usize,
        "evaluated members record count drifted"
    );
    Ok(())
}

fn insert_bundle(
    source: &Source,
    output: &mut BTreeMap<String, Vec<Value>>,
    bundle: Value,
) -> Result<()> {
    validate_bundle(source, &bundle)?;
    let candidate = text(&bundle, "candidateId")?.to_owned();
    let panel = text(&bundle, "panelId")?.to_owned();
    let rows = output.entry(candidate).or_default();
    ensure!(
        !rows
            .iter()
            .any(|v| text(v, "panelId").ok() == Some(panel.as_str())),
        "duplicate candidate/panel bundle"
    );
    rows.push(bundle);
    rows.sort_by(|a, b| {
        panel_order(source, text(a, "panelId").unwrap_or(""))
            .cmp(&panel_order(source, text(b, "panelId").unwrap_or("")))
    });
    Ok(())
}

fn validate_bundle(source: &Source, bundle: &Value) -> Result<()> {
    ensure!(
        text(bundle, "schemaVersion")? == BUNDLE_SCHEMA,
        "unsupported bundle schema"
    );
    verify_self_hash(bundle, "bundleSha256", "candidate panel bundle")?;
    ensure!(
        sha(bundle, "rotatingEvidenceSha256")? == source.rotating_sha,
        "bundle curriculum mismatch"
    );
    let candidate_id = text(bundle, "candidateId")?;
    let provisional = provisional_map(source)?;
    let candidate = provisional
        .get(candidate_id)
        .context("bundle candidate is not a provisional survivor")?;
    ensure!(
        sha(bundle, "candidateIdentitySha256")? == sha(candidate, "candidateIdentitySha256")?,
        "bundle candidate identity differs from provisional survivor"
    );
    ensure!(
        sha(bundle, "programSha256")? == sha(candidate, "programSha256")?,
        "bundle program identity differs from provisional survivor"
    );
    let panel_id = text(bundle, "panelId")?;
    ensure!(
        source.required_panel_ids.iter().any(|v| v == panel_id),
        "bundle panel is not required"
    );
    let panel = array(member(&source.value, "rotatingEvidence")?, "panels")?
        .iter()
        .find(|p| text(p, "panelId").ok() == Some(panel_id))
        .context("bundle panel missing from contract")?;
    let expected = array(panel, "windowIds")?
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_owned())
        .collect::<Vec<_>>();
    let records = array(bundle, "windowEvidence")?;
    ensure!(
        records.len() == expected.len(),
        "bundle window count mismatch"
    );
    let mut actual = Vec::new();
    for record in records {
        ensure!(
            text(record, "schemaVersion")? == WINDOW_SCHEMA,
            "unsupported window evidence schema"
        );
        verify_self_hash(record, "recordSha256", "window evidence")?;
        ensure!(
            text(record, "candidateId")? == candidate_id && text(record, "panelId")? == panel_id,
            "window evidence bundle binding mismatch"
        );
        ensure!(
            sha(record, "candidateIdentitySha256")? == sha(bundle, "candidateIdentitySha256")?
                && sha(record, "programSha256")? == sha(bundle, "programSha256")?,
            "window evidence candidate/program binding mismatch"
        );
        actual.push(text(record, "windowId")?.to_owned());
        for field in [
            "sourceProfileSnapshotSha256",
            "resolvedProfileSnapshotSha256",
            "resolvedProgramSha256",
        ] {
            sha(member(record, "metrics")?, field)?;
        }
    }
    let mut expected_sorted = expected;
    expected_sorted.sort();
    actual.sort();
    ensure!(
        actual == expected_sorted,
        "bundle lacks exact panel windows"
    );
    Ok(())
}

fn build_cumulative_archive(
    source: &Source,
    bundles: &BTreeMap<String, Vec<Value>>,
) -> Result<Value> {
    let provisional = provisional_map(source)?;
    let mut members = Vec::new();
    for (candidate_id, candidate) in &provisional {
        let candidate_bundles = bundles
            .get(candidate_id)
            .context("missing candidate bundles")?;
        let mut windows = Vec::new();
        let mut source_profiles = BTreeSet::new();
        let mut resolved_profiles = BTreeSet::new();
        let mut resolved_programs = BTreeSet::new();
        let mut realized_behavior_windows = Vec::new();
        for panel_id in &source.required_panel_ids {
            let bundle = candidate_bundles
                .iter()
                .find(|b| text(b, "panelId").ok() == Some(panel_id))
                .context("required panel bundle missing")?;
            for record in array(bundle, "windowEvidence")? {
                let metrics = member(record, "metrics")?;
                let conservative = number_f64(metrics, "conservativeNetR")
                    .or_else(|_| number_f64(metrics, "netR"))?;
                let source_profile = sha(metrics, "sourceProfileSnapshotSha256")?;
                let resolved_profile = sha(metrics, "resolvedProfileSnapshotSha256")?;
                let resolved_program = sha(metrics, "resolvedProgramSha256")?;
                source_profiles.insert(source_profile.clone());
                resolved_profiles.insert(resolved_profile.clone());
                resolved_programs.insert(resolved_program.clone());
                let realized_behavior = metrics
                    .get("realizedBehavior")
                    .filter(|value| value.is_object())
                    .context("direction-aware cumulative evidence lacks realized behavior")?;
                realized_behavior_windows
                    .push(json!({"realizedBehavior": realized_behavior.clone()}));
                windows.push(json!({
                    "panelId": panel_id,
                    "windowId": text(record,"windowId")?,
                    "conservativeNetR": conservative,
                    "noCostNetR": optional_f64(metrics,"noCostNetR")?.unwrap_or(conservative),
                    "maxDrawdownR": optional_f64(metrics,"maxDrawdownR")?.unwrap_or(0.0),
                    "closedTrades": optional_i64(metrics,"closedTrades")?.or(optional_i64(metrics,"trades")?).unwrap_or(0),
                    "sourceProfileSnapshotSha256": source_profile,
                    "resolvedProfileSnapshotSha256": resolved_profile,
                    "resolvedProgramSha256": resolved_program,
                }));
            }
        }
        ensure!(
            source_profiles == BTreeSet::from([sha(candidate, "profileSnapshotSha256")?])
                && resolved_profiles.len() == 1
                && resolved_programs.len() == 1,
            "candidate execution identity changed across panels"
        );
        let months = required_months(source)?;
        let panel_hashes = source
            .required_panel_ids
            .iter()
            .map(|panel| {
                candidate_bundles
                    .iter()
                    .find(|b| text(b, "panelId").ok() == Some(panel))
                    .and_then(|b| sha(b, "bundleSha256").ok())
                    .map(Value::String)
                    .ok_or_else(|| anyhow!("missing bundle hash"))
            })
            .collect::<Result<Vec<_>>>()?;
        let cumulative_realized_behavior = aggregate_realized_behavior(&realized_behavior_windows)
            .context("aggregate cumulative realized behavior")?;
        members.push(json!({
            "candidateId": candidate_id,
            "candidateIdentitySha256": sha(candidate,"candidateIdentitySha256")?,
            "programSha256": sha(candidate,"programSha256")?,
            "cellId": text(candidate,"cellId")?,
            "currentPanelRank": number_f64(candidate,"currentPanelRank")?,
            "coveredMonths": months,
            "windowMetrics": windows,
            "panelBundleSha256s": panel_hashes,
            "sourceProfileSnapshotSha256": source_profiles.iter().next().unwrap(),
            "resolvedProfileSnapshotSha256": resolved_profiles.iter().next().unwrap(),
            "resolvedProgramSha256": resolved_programs.iter().next().unwrap(),
            "novelty": optional_f64(candidate,"novelty")?.unwrap_or(0.0),
            "cumulativeRealizedBehavior": cumulative_realized_behavior,
            "currentPanelId": source.current_panel_id,
            "requiredPanelIds": source.required_panel_ids,
            "panelBundles": panel_hashes,
        }));
    }
    let policy = member(
        member(&source.value, "rotatingEvidence")?,
        "robustSelection",
    )?
    .get("policy")
    .context("robust policy missing")?
    .clone();
    verify_self_hash(&policy, "policySha256", "robust policy")?;
    let direction_policy = member(
        member(
            member(&source.archive_policy, "frozenPolicy")?,
            "directionSelection",
        )?,
        "selectionPolicy",
    )?;
    let direction_policy_sha256 = sha(
        member(
            member(&source.archive_policy, "frozenPolicy")?,
            "directionSelection",
        )?,
        "selectionPolicySha256",
    )?;
    ensure!(
        canonical_sha256(direction_policy)? == direction_policy_sha256,
        "direction-selection policy identity drifted"
    );
    let classified = classify(members, &policy, direction_policy, source.breeder_width)?;
    let mut output = json!({
        "schemaVersion": CUMULATIVE_SCHEMA,
        "mode": "replace",
        "rotatingEvidenceSha256": source.rotating_sha,
        "generationIndex": source.generation_index,
        "currentPanelId": source.current_panel_id,
        "requiredPanelIds": source.required_panel_ids,
        "previousArchiveSha256": source.prior_cumulative.as_ref().map(|v| sha(v,"archiveSha256")).transpose()?,
        "members": classified.members,
        "candidatePanelBundles": bundles.values().flatten().cloned().collect::<Vec<_>>(),
        "robustBreederPolicy": policy,
        "breederWidth": source.breeder_width,
        "qualityCandidateIds": classified.quality_ids,
        "frontierCandidateIds": classified.frontier_ids,
        "qualityMemberCount": classified.quality_ids.len(),
        "frontierMemberCount": classified.frontier_ids.len(),
        "staleAggregateCarryPermitted": false,
    });
    add_self_hash(&mut output, "archiveSha256")?;
    Ok(output)
}

struct Classified {
    members: Vec<Value>,
    quality_ids: Vec<String>,
    frontier_ids: Vec<String>,
}

fn classify(
    mut rows: Vec<Value>,
    policy: &Value,
    direction_policy: &Value,
    width: usize,
) -> Result<Classified> {
    let min_active = number_f64(policy, "minimumActiveWindowFraction")?;
    let min_trades = number_f64(policy, "minimumAverageClosedTradesPerCandidateMonth")?;
    let mut quality = Vec::new();
    let mut frontier = Vec::new();
    let mut base_rows = Vec::new();
    for mut row in rows.drain(..) {
        base_rows.push(row.clone());
        let windows = array(&row, "windowMetrics")?;
        ensure!(!windows.is_empty(), "candidate has no windows");
        let window_count = windows.len();
        let mut net = Vec::new();
        let mut drawdowns = Vec::new();
        let mut cost_drag = Vec::new();
        let mut active = 0usize;
        let mut trades = 0.0;
        for w in windows {
            let n = number_f64(w, "conservativeNetR")?;
            let d = number_f64(w, "maxDrawdownR")?.max(0.0);
            let t = number_f64(w, "closedTrades")?;
            net.push(n);
            drawdowns.push(d);
            cost_drag.push(number_f64(w, "noCostNetR")? - n);
            if t > 0.0 {
                active += 1
            };
            trades += t;
        }
        let sum_net = net.iter().sum::<f64>();
        let median = median(&net);
        let worst = net.iter().copied().fold(f64::INFINITY, f64::min);
        let max_dd = drawdowns.iter().copied().fold(0.0, f64::max);
        let drag = cost_drag.iter().sum::<f64>();
        let novelty = number_f64(&row, "novelty")?;
        let months = number_f64(&row, "coveredMonths")?;
        object_mut(&mut row,"candidate")?.insert("robustSupport".into(),json!({"activeWindowFraction": active as f64/window_count as f64,"averageClosedTradesPerMonth":trades/months,"coveredWindowCount":window_count,"coveredMonths":months}));
        object_mut(&mut row,"candidate")?.insert("robustEconomics".into(),json!({"cumulativeConservativeNetR":sum_net,"medianWindowConservativeNetR":median,"worstWindowConservativeNetR":worst,"maximumWindowDrawdownR":max_dd,"cumulativeCostDragR":drag}));
        object_mut(&mut row,"candidate")?.insert("robustObjectives".into(),json!({"worstWindowConservativeNetR":worst,"drawdown":max_dd,"costDrag":drag,"novelty":novelty}));
        let supported =
            active as f64 / window_count as f64 >= min_active && trades / months >= min_trades;
        let direction_eligible = direction_selection_eligible(
            member(&row, "cumulativeRealizedBehavior")?,
            direction_policy,
        )?;
        if supported && direction_eligible && sum_net > 0.0 && median > 0.0 {
            quality.push(row)
        } else if supported && direction_eligible {
            frontier.push(row)
        }
    }
    quality = pareto(quality, width)?;
    let frontier_cap = ((width as f64) * number_f64(policy, "frontierMaximumFraction")?) as usize;
    frontier = pareto(frontier, frontier_cap.min(width - quality.len()))?;
    let quality_ids = quality
        .iter()
        .map(|v| text(v, "candidateId").map(str::to_owned))
        .collect::<Result<Vec<_>>>()?;
    let frontier_ids = frontier
        .iter()
        .map(|v| text(v, "candidateId").map(str::to_owned))
        .collect::<Result<Vec<_>>>()?;
    let mut classified = BTreeMap::new();
    for (lane, values) in [("quality", &quality), ("frontier", &frontier)] {
        for row in values {
            let mut v = row.clone();
            let map = v.as_object_mut().unwrap();
            map.insert("robustBreederLane".into(), json!(lane));
            map.insert("robustBreederEligible".into(), json!(true));
            classified.insert(text(row, "candidateId")?.to_owned(), v);
        }
    }
    let members = base_rows
        .into_iter()
        .map(|row| {
            let id = text(&row, "candidateId").unwrap_or("");
            if let Some(selected) = classified.get(id) {
                selected.clone()
            } else {
                let mut unsupported = row;
                let map = unsupported.as_object_mut().expect("candidate is object");
                map.insert("robustBreederLane".into(), json!("unsupported"));
                map.insert("robustBreederEligible".into(), json!(false));
                unsupported
            }
        })
        .collect();
    Ok(Classified {
        members,
        quality_ids,
        frontier_ids,
    })
}

fn direction_selection_eligible(behavior: &Value, policy: &Value) -> Result<bool> {
    ensure!(
        text(behavior, "schemaVersion")? == "temporal_realized_behavior_v1",
        "direction realized behavior schema is unsupported"
    );
    let window_count = unsigned(behavior, "windowCount")?;
    let minimum_active_windows = unsigned(policy, "minimum_active_windows_per_side")?;
    ensure!(
        window_count >= minimum_active_windows && window_count > 0,
        "direction behavior windowCount cannot meet the active-window contract"
    );
    let minimum_closed_trades = unsigned(policy, "minimum_closed_trades_per_side")?;
    let minimum_acceptable_net = number_f64(policy, "minimum_acceptable_side_net_r")?;
    let harmful_net = number_f64(policy, "harmful_opposite_net_r")?;
    ensure!(
        harmful_net < minimum_acceptable_net,
        "direction harmful-side threshold is invalid"
    );
    let side = |name: &str| -> Result<(bool, bool, bool)> {
        let row = member(member(behavior, "sides")?, name)?;
        let closed = unsigned(row, "closedTrades")?;
        let active_windows = unsigned(row, "activeWindowCount")?;
        ensure!(
            active_windows <= window_count,
            "direction active-window count exceeds behavior window count"
        );
        let active_fraction = number_f64(row, "activeWindowFraction")?;
        ensure!(
            (active_fraction - active_windows as f64 / window_count as f64).abs() <= 1e-12,
            "direction active-window evidence is inconsistent"
        );
        let gross = number_f64(row, "grossR")?;
        let net = number_f64(row, "netR")?;
        let cost = number_f64(row, "costR")?;
        ensure!(
            (gross - net - cost).abs() <= 1e-9,
            "direction gross/net/cost R does not reconcile"
        );
        let terminal = unsigned(row, "terminalDirectionCount")?;
        ensure!(
            row.get("active").and_then(Value::as_bool) == Some(closed > 0 || terminal > 0),
            "direction active flag is inconsistent"
        );
        let supported = closed >= minimum_closed_trades && active_windows >= minimum_active_windows;
        Ok((
            supported,
            supported && net >= minimum_acceptable_net,
            supported && net <= harmful_net,
        ))
    };
    let (long_supported, long_acceptable, long_harmful) = side("long")?;
    let (short_supported, short_acceptable, short_harmful) = side("short")?;
    if (long_acceptable && short_harmful) || (short_acceptable && long_harmful) {
        return Ok(false);
    }
    Ok(long_acceptable && (short_acceptable || !short_supported)
        || (short_acceptable && !long_supported))
}

fn pareto(mut rows: Vec<Value>, capacity: usize) -> Result<Vec<Value>> {
    if capacity == 0 || rows.is_empty() {
        return Ok(Vec::new());
    }
    rows.sort_by(|a, b| {
        text(a, "candidateId")
            .unwrap_or("")
            .cmp(text(b, "candidateId").unwrap_or(""))
    });
    let vectors = rows.iter().map(objectives).collect::<Result<Vec<_>>>()?;
    let n = rows.len();
    let mut dominates = vec![Vec::new(); n];
    let mut dominated = vec![0usize; n];
    for l in 0..n {
        for r in l + 1..n {
            let ld = dominates_vec(vectors[l], vectors[r]);
            let rd = dominates_vec(vectors[r], vectors[l]);
            if ld {
                dominates[l].push(r);
                dominated[r] += 1
            } else if rd {
                dominates[r].push(l);
                dominated[l] += 1
            }
        }
    }
    let mut fronts = Vec::new();
    let mut current = (0..n).filter(|i| dominated[*i] == 0).collect::<Vec<_>>();
    while !current.is_empty() {
        current.sort_by_key(|i| text(&rows[*i], "candidateId").unwrap_or("").to_owned());
        fronts.push(current.clone());
        let mut next = Vec::new();
        for i in current {
            for target in &dominates[i] {
                dominated[*target] -= 1;
                if dominated[*target] == 0 {
                    next.push(*target)
                }
            }
        }
        current = next;
    }
    let mut selected = Vec::new();
    for (front_index, front) in fronts.iter().enumerate() {
        let mut distances = BTreeMap::new();
        for i in front {
            distances.insert(*i, 0.0f64);
        }
        if front.len() <= 2 {
            for i in front {
                distances.insert(*i, f64::INFINITY);
            }
        } else {
            for (d, _) in vectors[0].iter().enumerate() {
                let mut ordered = front.clone();
                ordered.sort_by(|a, b| {
                    vectors[*a][d].total_cmp(&vectors[*b][d]).then_with(|| {
                        text(&rows[*a], "candidateId")
                            .unwrap_or("")
                            .cmp(text(&rows[*b], "candidateId").unwrap_or(""))
                    })
                });
                *distances.get_mut(&ordered[0]).unwrap() = f64::INFINITY;
                *distances.get_mut(ordered.last().unwrap()).unwrap() = f64::INFINITY;
                let low = vectors[ordered[0]][d];
                let high = vectors[*ordered.last().unwrap()][d];
                if high > low {
                    for pos in 1..ordered.len() - 1 {
                        let i = ordered[pos];
                        if !distances[&i].is_infinite() {
                            *distances.get_mut(&i).unwrap() += (vectors[ordered[pos + 1]][d]
                                - vectors[ordered[pos - 1]][d])
                                / (high - low)
                        }
                    }
                }
            }
        }
        let mut ordered = front.clone();
        ordered.sort_by(|a, b| {
            cmp_desc(distances[a], distances[b])
                .then_with(|| {
                    cmp_desc(
                        number_f64(
                            member(&rows[*a], "robustEconomics").unwrap(),
                            "cumulativeConservativeNetR",
                        )
                        .unwrap(),
                        number_f64(
                            member(&rows[*b], "robustEconomics").unwrap(),
                            "cumulativeConservativeNetR",
                        )
                        .unwrap(),
                    )
                })
                .then_with(|| {
                    cmp_desc(
                        number_f64(
                            member(&rows[*a], "robustEconomics").unwrap(),
                            "medianWindowConservativeNetR",
                        )
                        .unwrap(),
                        number_f64(
                            member(&rows[*b], "robustEconomics").unwrap(),
                            "medianWindowConservativeNetR",
                        )
                        .unwrap(),
                    )
                })
                .then_with(|| {
                    text(&rows[*a], "candidateId")
                        .unwrap_or("")
                        .cmp(text(&rows[*b], "candidateId").unwrap_or(""))
                })
        });
        for i in ordered.into_iter().take(capacity - selected.len()) {
            let mut row = rows[i].clone();
            let m = row.as_object_mut().unwrap();
            m.insert("robustParetoFront".into(), json!(front_index));
            m.insert(
                "robustCrowdingDistance".into(),
                if distances[&i].is_infinite() {
                    Value::Null
                } else {
                    json!(distances[&i])
                },
            );
            selected.push(row);
        }
        if selected.len() >= capacity {
            break;
        }
    }
    Ok(selected)
}

fn build_parent_archive(source: &Source, cumulative: &Value) -> Result<Value> {
    let quality = array(cumulative, "qualityCandidateIds")?
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_owned())
        .collect::<BTreeSet<_>>();
    let frontier = array(cumulative, "frontierCandidateIds")?
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_owned())
        .collect::<BTreeSet<_>>();
    ensure!(quality.is_disjoint(&frontier), "breeder lanes overlap");
    let allowed = quality.union(&frontier).cloned().collect::<BTreeSet<_>>();
    let rich = source
        .rich_members
        .iter()
        .map(|r| Ok((text(r, "candidateId")?.to_owned(), r.clone())))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let cumulative_members = array(cumulative, "members")?
        .iter()
        .map(|r| Ok((text(r, "candidateId")?.to_owned(), r.clone())))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut groups: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for candidate_id in &allowed {
        let mut row = rich
            .get(candidate_id)
            .context("selected breeder lacks rich member")?
            .clone();
        let c = cumulative_members
            .get(candidate_id)
            .context("selected breeder lacks cumulative member")?;
        ensure!(
            sha(member(&row, "candidate")?, "candidateIdentitySha256")?
                == sha(c, "candidateIdentitySha256")?
                && sha(member(&row, "candidate")?, "programSha256")? == sha(c, "programSha256")?,
            "rich breeder genome mismatch"
        );
        let lane = if quality.contains(candidate_id) {
            "quality"
        } else {
            "rotating_frontier"
        };
        let robust = member(c, "robustObjectives")?.clone();
        let cumulative_behavior = member(c, "cumulativeRealizedBehavior")?.clone();
        let cumulative_behavior_sha256 = sha(&cumulative_behavior, "identitySha256")?.to_owned();
        let structural =
            optional_f64(member(&row, "objectives")?, "structuralComplexity")?.unwrap_or(0.0);
        {
            let map = row.as_object_mut().unwrap();
            map.insert("objectives".into(),json!({"worstWindowConservativeNetR":number_f64(&robust,"worstWindowConservativeNetR")?,"maximumDrawdownR":number_f64(&robust,"drawdown")?,"structuralComplexity":structural}));
            map.insert("archiveLane".into(), json!(lane));
            map.insert(
                "retentionReason".into(),
                json!(if lane == "quality" {
                    "cumulative_robust_quality"
                } else {
                    "bounded_cumulative_frontier_fallback"
                }),
            );
            map.insert("robustBreederEligible".into(), json!(true));
            map.insert("cumulativeEvidence".into(), c.clone());
            map.insert(
                "cumulativeEvidenceArchiveSha256".into(),
                json!(sha(cumulative, "archiveSha256")?),
            );
            map.insert("robustObjectives".into(), robust);
            map.insert("paretoFront".into(), Value::Null);
            map.insert("crowdingDistance".into(), Value::Null);
        }
        let aggregate = object_mut(&mut row, "aggregate")?;
        aggregate.insert("realizedBehavior".into(), cumulative_behavior);
        aggregate.insert(
            "behaviorIdentitySha256".into(),
            Value::String(cumulative_behavior_sha256),
        );
        let cell = text(member(&row, "descriptor")?, "cellId")?.to_owned();
        groups.entry(cell).or_default().push(row);
    }
    let mut cells = Vec::new();
    for (cell_id, mut rows) in groups {
        let before = rows.len();
        let quality_before = rows
            .iter()
            .filter(|r| text(r, "archiveLane").ok() == Some("quality"))
            .count();
        rows.sort_by(parent_member_cmp);
        rows.truncate(source.cell_capacity);
        let descriptor = member(&rows[0], "descriptor")?.clone();
        rows.sort_by(|a, b| {
            text(a, "candidateId")
                .unwrap_or("")
                .cmp(text(b, "candidateId").unwrap_or(""))
        });
        cells.push(json!({"cellId":cell_id,"descriptor":descriptor,"candidateCountBeforeCapacity":before,"qualityEligibleCountBeforeCapacity":quality_before,"negativeNoveltyEligibleCountBeforeCapacity":0,"observationalCountBeforeCapacity":0,"breedingEligibleMemberCount":rows.len(),"negativeNoveltyMemberCount":0,"selectionVisitCount":0,"offspringAttemptCount":0,"members":rows}));
    }
    let parent_count = cells
        .iter()
        .map(|c| array(c, "members").map(|v| v.len()))
        .collect::<Result<Vec<_>>>()?
        .iter()
        .sum::<usize>();
    ensure!(
        parent_count <= source.breeder_width,
        "parent archive exceeds breeder width"
    );
    // Parent count is evidence of supported source material, not a cap on
    // offspring.  The next generation freezes its evaluated 80/20 quota and
    // samples these valid parents with replacement.
    let mut schedule = json!({"schemaVersion":"temporal_qd_rotating_parent_schedule_v2","breederWidth":source.breeder_width,"breederParentCount":parent_count,"minimumImmigrantNumerator":1,"minimumImmigrantDenominator":5,"parentSampling":"with_replacement_supported_parents_v1","unsupportedParentPolicy":"immigrant_only_authority_bound_v1","schedulingMethod":"accepted_quota_prefix_balance_v1"});
    add_self_hash(&mut schedule, "scheduleSha256")?;
    let old_cells = array(&source.previous_parent, "cellIds")?
        .iter()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();
    let new_cells = cells
        .iter()
        .map(|c| text(c, "cellId").unwrap_or(""))
        .collect::<BTreeSet<_>>();
    let policy = object(&source.archive_policy, "archive policy")?;
    let mut archive = json!({"schemaVersion":ARCHIVE_SCHEMA,"qdVersion":policy.get("qdVersion").context("qdVersion missing")?,"policyName":policy.get("policyName").context("policyName missing")?,"policySha256":policy.get("policySha256").context("policySha missing")?,"frozenPolicy":policy.get("frozenPolicy").context("frozen policy missing")?,"generationIndex":source.generation_index,"populationSha256":canonical_sha256(&json!({"cumulativeArchiveSha256":sha(cumulative,"archiveSha256")?,"candidateIds":allowed}))?,"resultSetSha256":sha(cumulative,"archiveSha256")?,"previousArchiveSha256":sha(&source.previous_parent,"archiveSha256")?,"cellCapacity":source.cell_capacity,"candidateCountSeen":unsigned(&source.previous_parent,"candidateCountSeen")?+source.current_member_count,"candidateCountReducedThisGeneration":source.current_member_count,"occupiedCellCount":cells.len(),"newCellCount":new_cells.difference(&old_cells).count(),"memberCount":parent_count,"qualityMemberCount":cells.iter().flat_map(|c|array(c,"members").unwrap()).filter(|r|text(r,"archiveLane").ok()==Some("quality")).count(),"observationalMemberCount":0,"negativeNoveltyMemberCount":0,"paretoAdmissionCount":allowed.len(),"paretoEvictionCount":unsigned(&source.previous_parent,"memberCount")?.saturating_sub(allowed.len() as u64),"rotatingEvidenceTransaction":{"schemaVersion":"temporal_qd_rotating_parent_projection_v1","cumulativeArchiveSha256":sha(cumulative,"archiveSha256")?,"rotatingEvidenceSha256":source.rotating_sha,"requiredPanelIds":source.required_panel_ids,"mode":"replace","frontierFallbackPermitted":true,"parentSchedule":schedule},"cells":cells});
    if let Some(pair) = source.previous_parent.get("bidirectionalPairPolicy") {
        archive
            .as_object_mut()
            .unwrap()
            .insert("bidirectionalPairPolicy".into(), pair.clone());
    }
    add_self_hash(&mut archive, "archiveSha256")?;
    Ok(archive)
}

fn build_funnel(source: &Source, archive: &Value) -> Result<(Value, Value)> {
    let reduction = &source.funnel_source;
    let policy = member(reduction, "completenessPolicy")?.clone();
    ensure!(
        text(&policy, "schemaVersion")? == "temporal_generation_funnel_completeness_v1",
        "unsupported funnel completeness policy"
    );
    let proposal_accounting = member(reduction, "proposalAccounting")?.clone();
    let attempts = array(reduction, "proposalAttempts")?;
    ensure!(
        !attempts.is_empty(),
        "funnel attempt ledger must not be empty"
    );

    let mut normalized_attempts = Vec::with_capacity(attempts.len());
    let mut ordinals = BTreeSet::new();
    let mut identities = BTreeSet::new();
    let mut attempt_candidates = BTreeMap::new();
    let mut disposition_counts = BTreeMap::<String, u64>::new();
    let mut origin_counts = BTreeMap::<String, u64>::new();
    for raw in attempts {
        let ordinal = unsigned(raw, "proposalOrdinal")?;
        let identity = sha(raw, "attemptIdentitySha256")?;
        let origin = text(raw, "originKind")?.to_owned();
        let disposition = text(raw, "disposition")?.to_owned();
        ensure!(ordinals.insert(ordinal), "duplicate funnel attempt ordinal");
        ensure!(
            identities.insert(identity.clone()),
            "duplicate funnel attempt identity"
        );
        let candidate = object(raw, "funnel attempt")?.get("candidateId");
        let normalized = if let Some(candidate_id) = candidate.and_then(Value::as_str) {
            ensure!(!candidate_id.is_empty(), "empty attempt candidate id");
            let raw_sha = sha(raw, "rawSourceProfileSha256")?;
            ensure!(
                attempt_candidates
                    .insert(candidate_id.to_owned(), raw_sha.clone())
                    .is_none(),
                "candidate appears in multiple attempts"
            );
            json!({
                "proposalOrdinal": ordinal,
                "attemptIdentitySha256": identity,
                "originKind": origin,
                "disposition": disposition,
                "candidateId": candidate_id,
                "rawSourceProfileSha256": raw_sha,
            })
        } else {
            ensure!(
                object(raw, "funnel attempt")?
                    .get("rawSourceProfileSha256")
                    .is_none(),
                "candidate-free attempt has a source identity"
            );
            json!({
                "proposalOrdinal": ordinal,
                "attemptIdentitySha256": identity,
                "originKind": origin,
                "disposition": disposition,
            })
        };
        *disposition_counts.entry(disposition).or_default() += 1;
        *origin_counts.entry(origin).or_default() += 1;
        normalized_attempts.push(normalized);
    }
    ensure!(
        ordinals == (0..attempts.len() as u64).collect(),
        "funnel attempt ordinals are not contiguous"
    );
    normalized_attempts.sort_by_key(|row| unsigned(row, "proposalOrdinal").unwrap_or(u64::MAX));
    ensure!(
        count_map(member(&proposal_accounting, "dispositionCounts")?)? == disposition_counts,
        "funnel proposal disposition accounting mismatch"
    );
    ensure!(
        count_map(member(&proposal_accounting, "originProposalCounts")?)? == origin_counts,
        "funnel proposal origin accounting mismatch"
    );
    let mut attempt_ledger = json!({
        "attemptCount": normalized_attempts.len(),
        "materializedCandidateCount": attempt_candidates.len(),
        "nonMaterializedAttemptCount": normalized_attempts.len() - attempt_candidates.len(),
        "attemptDispositionCounts": disposition_counts,
        "attemptOriginCounts": origin_counts,
        "attempts": normalized_attempts,
    });
    add_self_hash(&mut attempt_ledger, "attemptLedgerSha256")?;

    let mut candidates = array(reduction, "candidateStageRows")?.clone();
    let pre_archive_projection = object(reduction, "funnel reduction source")?
        .get("preArchiveProjection")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if pre_archive_projection {
        finalize_pre_archive_funnel_candidates(&mut candidates, archive)?;
    }
    ensure!(
        candidates.len() == attempt_candidates.len(),
        "funnel candidate/attempt materialization mismatch"
    );
    let mut candidate_ids = BTreeSet::new();
    let mut previous_id: Option<String> = None;
    let mut stage_counts = BTreeMap::<String, u64>::new();
    let mut terminal_counts = BTreeMap::<String, u64>::new();
    let mut operator_counts = BTreeMap::<String, u64>::new();
    let mut motif_counts = BTreeMap::<String, u64>::new();
    let mut direction_counts = BTreeMap::<String, u64>::new();
    for candidate in &candidates {
        let candidate_id = text(candidate, "candidateId")?.to_owned();
        ensure!(
            previous_id.as_ref().is_none_or(|old| old < &candidate_id),
            "funnel candidates must be uniquely sorted"
        );
        previous_id = Some(candidate_id.clone());
        ensure!(
            candidate_ids.insert(candidate_id.clone()),
            "duplicate funnel candidate"
        );
        let identity = member(candidate, "identity")?;
        ensure!(
            text(identity, "candidateId")? == candidate_id,
            "funnel candidate identity mismatch"
        );
        let raw_sha = sha(identity, "rawSourceProfileSha256")?;
        ensure!(
            attempt_candidates.get(&candidate_id) == Some(&raw_sha),
            "funnel candidate does not match its attempt"
        );
        validate_optional_identity_shas(identity)?;
        let stages = member(candidate, "stages")?;
        validate_stage_chain(stages, candidate)?;
        for (stage, record) in object(stages, "funnel stages")? {
            let outcome = text(record, "outcome")?;
            *stage_counts
                .entry(format!("{stage}:{outcome}"))
                .or_default() += 1;
        }
        let terminal = text(candidate, "terminalDisposition")?.to_owned();
        *terminal_counts.entry(terminal).or_default() += 1;
        for operator in string_array(candidate, "operatorIds")? {
            *operator_counts.entry(operator).or_default() += 1;
        }
        for motif in string_array(candidate, "motifIds")? {
            *motif_counts.entry(motif).or_default() += 1;
        }
        *direction_counts
            .entry(text(candidate, "direction")?.to_owned())
            .or_default() += 1;
    }
    ensure!(
        candidate_ids == attempt_candidates.keys().cloned().collect(),
        "funnel materialized candidate set mismatch"
    );

    let mut artifact = json!({
        "schemaVersion": FUNNEL_SCHEMA,
        "completenessPolicy": policy,
        "proposalAccounting": proposal_accounting,
        "attemptLedger": attempt_ledger,
        "attemptToMaterializedAttrition": {
            "attempted": attempts.len(),
            "materializedCandidates": attempt_candidates.len(),
            "notMaterialized": attempts.len() - attempt_candidates.len(),
            "attemptDispositionCounts": disposition_counts,
        },
        "candidateCount": candidates.len(),
        "candidates": candidates,
        "stageCounts": stage_counts,
        "terminalDispositionCounts": terminal_counts,
        "operatorBreakdown": operator_counts,
        "motifBreakdown": motif_counts,
        "directionBreakdown": direction_counts,
    });
    add_self_hash(&mut artifact, "artifactSha256")?;
    let mut snapshot = json!({
        "schemaVersion": FUNNEL_SNAPSHOT_SCHEMA,
        "funnelArtifactSha256": sha(&artifact, "artifactSha256")?,
        "candidateCount": candidates.len(),
        "terminalDispositionCounts": terminal_counts,
        "candidateTerminals": candidates.iter().map(|row| json!({
            "candidateId": text(row, "candidateId").unwrap(),
            "terminalDisposition": text(row, "terminalDisposition").unwrap(),
        })).collect::<Vec<_>>(),
    });
    add_self_hash(&mut snapshot, "snapshotSha256")?;
    Ok((artifact, snapshot))
}

fn finalize_pre_archive_funnel_candidates(candidates: &mut [Value], archive: &Value) -> Result<()> {
    let mut retained = BTreeMap::<String, String>::new();
    for cell in array(archive, "cells")? {
        for member in array(cell, "members")? {
            let candidate_id = text(member, "candidateId")?.to_owned();
            ensure!(
                retained
                    .insert(candidate_id, canonical_sha256(member)?)
                    .is_none(),
                "archive contains duplicate funnel candidate"
            );
        }
    }
    for candidate in candidates {
        let candidate_id = text(candidate, "candidateId")?.to_owned();
        let candidate_map = object_mut(candidate, "pre-archive funnel candidate")?;
        let member_sha = retained.get(&candidate_id);
        {
            let identity = object_mut(
                candidate_map
                    .get_mut("identity")
                    .context("pre-archive funnel identity missing")?,
                "pre-archive funnel identity",
            )?;
            identity.insert(
                "archiveMemberIdentitySha256".into(),
                member_sha.map_or(Value::Null, |value| json!(value)),
            );
        }
        let terminal = {
            let stages = object_mut(
                candidate_map
                    .get_mut("stages")
                    .context("pre-archive funnel stages missing")?,
                "pre-archive funnel stages",
            )?;
            stages.remove("archiveRetention");
            stages.remove("exploratoryRetention");
            if member_sha.is_some() {
                ensure!(
                    stages.contains_key("activationQuality"),
                    "retained funnel candidate lacks activation evidence"
                );
                stages.insert(
                    "activationQuality".into(),
                    json!({"outcome":"recorded","qualityDisposition":"eligible","reasons":[]}),
                );
                stages.insert(
                    "archiveRetention".into(),
                    json!({"outcome":"retained","reasons":[]}),
                );
                Some("retained")
            } else if stages
                .get("activationQuality")
                .and_then(|row| row.get("outcome"))
                .and_then(Value::as_str)
                == Some("recorded")
            {
                stages.insert(
                    "archiveRetention".into(),
                    json!({"outcome":"not_retained","reasons":["not_selected_by_archive"]}),
                );
                Some("not_retained")
            } else {
                None
            }
        };
        if let Some(terminal) = terminal {
            candidate_map.insert("terminalDisposition".into(), json!(terminal));
        }
    }
    Ok(())
}

fn validate_optional_identity_shas(identity: &Value) -> Result<()> {
    for field in [
        "resolvedProfileSha256",
        "programSha256",
        "validationReportSha256",
        "validationIdentitySha256",
        "canonicalEvidenceIdentitySha256",
        "archiveMemberIdentitySha256",
    ] {
        if let Some(value) = object(identity, "funnel identity")?.get(field) {
            if !value.is_null() {
                sha(identity, field)?;
            }
        }
    }
    Ok(())
}

fn validate_stage_chain(stages: &Value, candidate: &Value) -> Result<()> {
    let stages = object(stages, "funnel stages")?;
    ensure!(
        stages
            .get("proposed")
            .and_then(|row| row.get("outcome"))
            .and_then(Value::as_str)
            == Some("proposed"),
        "funnel candidate lacks proposed stage"
    );
    let terminal = text(candidate, "terminalDisposition")?;
    let static_outcome = stages
        .get("staticallyReachable")
        .context("missing static reachability stage")?
        .get("outcome")
        .and_then(Value::as_str)
        .context("invalid static outcome")?;
    if static_outcome == "rejected" {
        ensure!(
            terminal == "static_reachability_rejected",
            "invalid static terminal"
        );
        ensure!(stages.len() == 2, "post-terminal static stages present");
        return Ok(());
    }
    let native = stage_outcome(stages, "nativeValid")?;
    if native == "rejected" {
        ensure!(
            terminal == "native_validation_rejected",
            "invalid native terminal"
        );
        return Ok(());
    }
    let admission = stage_outcome(stages, "uniqueAdmitted")?;
    if admission == "rejected_duplicate" {
        ensure!(
            terminal == "duplicate_rejected",
            "invalid duplicate terminal"
        );
        return Ok(());
    }
    stage_outcome(stages, "syntheticEvidence")?;
    let evaluated = stage_outcome(stages, "evaluated")?;
    if evaluated != "evaluated" {
        ensure!(
            terminal == format!("evaluation_{evaluated}"),
            "invalid evaluation terminal"
        );
        return Ok(());
    }
    let activation = stage_outcome(stages, "activationQuality")?;
    if activation == "recorded" {
        let retention = stage_outcome(stages, "archiveRetention")?;
        ensure!(terminal == retention, "invalid retention terminal");
    } else {
        ensure!(
            terminal == format!("activation_{activation}"),
            "invalid activation terminal"
        );
        if stages.contains_key("exploratoryRetention") {
            ensure!(
                activation == "quality_rejected",
                "invalid exploratory retention"
            );
        }
    }
    Ok(())
}

fn stage_outcome<'a>(stages: &'a Map<String, Value>, stage: &str) -> Result<&'a str> {
    stages
        .get(stage)
        .with_context(|| format!("missing funnel stage {stage}"))?
        .get("outcome")
        .and_then(Value::as_str)
        .with_context(|| format!("invalid funnel stage {stage}"))
}

fn string_array(value: &Value, key: &str) -> Result<Vec<String>> {
    let values = array(value, key)?;
    ensure!(!values.is_empty(), "{key} must not be empty");
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("{key} must contain nonempty strings"))
        })
        .collect()
}

fn count_map(value: &Value) -> Result<BTreeMap<String, u64>> {
    object(value, "count map")?
        .iter()
        .map(|(key, value)| {
            Ok((
                key.clone(),
                value
                    .as_u64()
                    .ok_or_else(|| anyhow!("count map value must be unsigned"))?,
            ))
        })
        .collect()
}

fn campaign_bindings(source: &Source) -> Result<Vec<Value>> {
    let mut campaigns = source.campaigns.clone();
    for receipt in &source.receipts {
        campaigns.push(member(receipt, "campaignBinding")?.clone());
    }
    Ok(campaigns)
}

fn build_checkpoint(source: &Source, cumulative: &Value, archive: &Value) -> Result<Value> {
    let mut ids = array(&source.provisional, "candidates")?
        .iter()
        .map(|r| text(r, "candidateId").map(str::to_owned))
        .collect::<Result<Vec<_>>>()?;
    ids.sort();
    ids.dedup();
    let campaigns = campaign_bindings(source)?;
    let campaigns_value = Value::Array(campaigns);
    let mut v = json!({"schemaVersion":CHECKPOINT_SCHEMA,"rotatingEvidenceSha256":source.rotating_sha,"generationIndex":source.generation_index,"panelId":source.current_panel_id,"requiredPanelIds":source.required_panel_ids,"stage":"cumulative_archive","cohortSha256":sha(&source.cohort,"cohortSha256")?,"provisionalCandidateIds":ids,"cumulativeArchiveSha256":sha(cumulative,"archiveSha256")?,"stageArtifacts":{"parentArchiveSha256":sha(archive,"archiveSha256")?,"campaignsSha256":canonical_sha256(&campaigns_value)?}});
    add_self_hash(&mut v, "checkpointSha256")?;
    Ok(v)
}
fn build_ledger(
    source: &Source,
    _plan: &Value,
    cumulative: &Value,
    archive: &Value,
    checkpoint: &Value,
) -> Result<Value> {
    validate_cohort_partition(source)?;
    let campaigns = campaign_bindings(source)?;
    let mut v = json!({"schemaVersion":"temporal_qd_rotating_generation_ledger_v1","rotatingEvidenceSha256":source.rotating_sha,"generationIndex":source.generation_index,"panelId":source.current_panel_id,"cohortSha256":sha(&source.cohort,"cohortSha256")?,"proposalCandidateIds":member(&source.cohort,"newProposalCandidateIds")?,"retainedParentEvaluationCandidateIds":member(&source.cohort,"retainedParentEvaluationCandidateIds")?,"proposalOnlyFunnelReporting":true,"campaigns":campaigns,"provisionalSha256":sha(&source.provisional,"provisionalSha256")?,"cumulativeArchiveSha256":sha(cumulative,"archiveSha256")?,"checkpointSha256":sha(checkpoint,"checkpointSha256")?,"parentArchiveSha256":sha(archive,"archiveSha256")?});
    add_self_hash(&mut v, "ledgerSha256")?;
    Ok(v)
}
fn build_generation_record(
    source: &Source,
    plan: &Value,
    outputs: &FinalizedOutputs<'_>,
) -> Result<Value> {
    // Publication and transition bases are derived from the sealed v5 state
    // basis and the prefinalizer's admitted result.  V1 accepted these as an
    // unchecked Python `finalizerContext`, which allowed a valid archive to be
    // paired with arbitrary state/accounting metadata.
    let mut v = json!({
        "schemaVersion":"temporal_qd_generation_record_v2",
        "generationIndex":source.generation_index,
        // The historical supervisor contract counts the sealed proposal
        // evaluation population, including terminal evaluation rejections.
        // The current-panel cohort is post-reduction and can be smaller; it
        // may also contain retained-parent reevaluations, which are not new
        // proposals. The proposal-only funnel is the authenticated authority
        // for this generation counter.
        "candidateCount":unsigned(outputs.funnel, "candidateCount")?,
        "totalGenerationTaskCount":campaign_task_count(&source.campaigns)?,
        "stateBasisSha256":sha(&source.state_basis, "stateBasisSha256")?,
        "semanticAuthoritySha256":source.semantic_authority_sha256,
        "runtimeAuthoritySha256":source.runtime_authority_sha256,
    });
    let m = object_mut(&mut v, "generation record")?;
    m.insert("generationIndex".into(), json!(source.generation_index));
    m.insert(
        "archiveSha256".into(),
        json!(sha(outputs.archive, "archiveSha256")?),
    );
    m.insert(
        "resultSetSha256".into(),
        json!(sha(outputs.cumulative, "archiveSha256")?),
    );
    m.insert(
        "rotatingEvidenceLedgerSha256".into(),
        json!(sha(outputs.ledger, "ledgerSha256")?),
    );
    m.insert(
        "rotatingEvidenceCheckpointSha256".into(),
        json!(sha(outputs.checkpoint, "checkpointSha256")?),
    );
    m.insert(
        "cumulativeArchiveSha256".into(),
        json!(sha(outputs.cumulative, "archiveSha256")?),
    );
    m.insert(
        "auxiliaryPlanSha256".into(),
        json!(sha(plan, "planSha256")?),
    );
    m.insert(
        "generationFunnelArtifactSha256".into(),
        json!(sha(outputs.funnel, "artifactSha256")?),
    );
    m.insert(
        "generationFunnelSnapshotSha256".into(),
        json!(sha(outputs.funnel_snapshot, "snapshotSha256")?),
    );
    m.insert("archivePath".into(), json!(ARCHIVE_PATH));
    for (field, key) in [
        ("occupiedCellCount", "occupiedCellCount"),
        ("newCellCount", "newCellCount"),
        ("qualityMemberCount", "qualityMemberCount"),
        ("observationalMemberCount", "observationalMemberCount"),
        ("negativeNoveltyMemberCount", "negativeNoveltyMemberCount"),
        ("paretoAdmissionCount", "paretoAdmissionCount"),
        ("paretoEvictionCount", "paretoEvictionCount"),
    ] {
        m.insert(field.into(), member(outputs.archive, key)?.clone());
    }
    let mut frontier_count = 0;
    for cell in array(outputs.archive, "cells")? {
        frontier_count += array(cell, "members")?
            .iter()
            .filter(|member| text(member, "archiveLane").ok() == Some("rotating_frontier"))
            .count();
    }
    m.insert("frontierMemberCount".into(), json!(frontier_count));
    m.insert(
        "parentSchedule".into(),
        member(
            member(outputs.archive, "rotatingEvidenceTransaction")?,
            "parentSchedule",
        )?
        .clone(),
    );
    m.insert("artifacts".into(), build_artifact_ledger(source, outputs)?);
    add_self_hash(&mut v, "generationRecordSha256")?;
    Ok(v)
}

fn build_artifact_ledger(source: &Source, outputs: &FinalizedOutputs<'_>) -> Result<Value> {
    let mut artifacts = json!({
        "schemaVersion":"temporal_qd_generation_artifacts_v2",
        "semanticAuthoritySha256":source.semantic_authority_sha256,
    });
    let map = object_mut(&mut artifacts, "artifact ledger base")?;
    map.insert(
        "archive".into(),
        semantic_artifact_descriptor(ARCHIVE_PATH, outputs.archive, "archiveSha256")?,
    );
    map.insert(
        "generationFunnel".into(),
        semantic_artifact_descriptor(FUNNEL_PATH, outputs.funnel, "artifactSha256")?,
    );
    map.insert(
        "generationFunnelSnapshot".into(),
        outputs.funnel_snapshot.clone(),
    );
    map.insert(
        "rotatingEvidenceLedger".into(),
        semantic_artifact_descriptor(LEDGER_PATH, outputs.ledger, "ledgerSha256")?,
    );
    map.insert(
        "rotatingEvidenceCheckpoint".into(),
        semantic_artifact_descriptor(CHECKPOINT_PATH, outputs.checkpoint, "checkpointSha256")?,
    );
    map.insert(
        "cumulativeBreederArchive".into(),
        semantic_artifact_descriptor(CUMULATIVE_PATH, outputs.cumulative, "archiveSha256")?,
    );
    Ok(artifacts)
}

fn semantic_artifact_descriptor(path: &str, value: &Value, field: &str) -> Result<Value> {
    let mut descriptor = json!({"path":path,"sha256":canonical_sha256(value)?});
    object_mut(&mut descriptor, "artifact descriptor")?
        .insert(field.into(), json!(sha(value, field)?));
    Ok(descriptor)
}
fn build_state_patch(source: &Source, record: &Value) -> Result<Value> {
    let selected_new_proposal_count = validate_cohort_partition(source)?;
    let new_proposal_count = unsigned(record, "candidateCount")?;
    ensure!(
        new_proposal_count >= selected_new_proposal_count,
        "generation record proposal count is smaller than the selected proposal cohort"
    );
    let mut completed = source.completed_generation_records.clone();
    completed.push(record.clone());
    let mut v = json!({
        "schemaVersion":"temporal_qd_generation_state_patch_v2",
        "stateBasisSha256":sha(&source.state_basis, "stateBasisSha256")?,
        "generationIndex":source.generation_index,
        "nextGenerationIndex":source.generation_index.checked_add(1).context("generation index overflow")?,
        "nextStage":"generation_proposal",
        "uniqueCandidatesEvaluated":unsigned(&source.state_basis, "uniqueCandidatesEvaluated")?.checked_add(new_proposal_count).context("candidate accounting overflow")?,
        "workerTasksCompleted":unsigned(&source.state_basis, "workerTasksCompleted")?.checked_add(campaign_task_count(&source.campaigns)?).context("task accounting overflow")?,
        "nextImmigrantContinuationOrdinal":unsigned(&source.state_basis, "nextImmigrantContinuationOrdinal")?,
        "uniqueIdentityCounts":member(&source.state_basis, "uniqueIdentityCounts")?,
        "duplicateCounters":member(&source.state_basis, "duplicateCounters")?,
        "proposalSlotCounters":member(&source.state_basis, "proposalSlotCounters")?,
        "completedGenerationsSha256":canonical_sha256(&Value::Array(completed))?,
        "generationRecordSha256":sha(record, "generationRecordSha256")?,
        "generationRecord":record,
        "semanticAuthoritySha256":source.semantic_authority_sha256,
        "runtimeAuthoritySha256":source.runtime_authority_sha256,
    });
    add_self_hash(&mut v, "statePatchSha256")?;
    Ok(v)
}

fn validate_cohort_partition(source: &Source) -> Result<u64> {
    ensure!(
        !member(&source.cohort, "parentReevaluationIsProposal")?
            .as_bool()
            .unwrap_or(true),
        "parent reevaluation must not be counted as a proposal"
    );
    let ids = |key: &str| -> Result<BTreeSet<String>> {
        array(&source.cohort, key)?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow!("{key} must contain nonempty candidate IDs"))
            })
            .collect()
    };
    let proposal = ids("newProposalCandidateIds")?;
    ensure!(
        proposal.len() == array(&source.cohort, "newProposalCandidateIds")?.len(),
        "new proposal candidate IDs repeat"
    );
    let retained = ids("retainedParentEvaluationCandidateIds")?;
    ensure!(
        retained.len() == array(&source.cohort, "retainedParentEvaluationCandidateIds")?.len(),
        "retained parent candidate IDs repeat"
    );
    ensure!(
        proposal.is_disjoint(&retained),
        "cohort proposal/parent roles overlap"
    );
    let candidates = array(&source.cohort, "candidates")?;
    let candidate_ids = candidates
        .iter()
        .map(|candidate| text(candidate, "candidateId").map(str::to_owned))
        .collect::<Result<BTreeSet<_>>>()?;
    ensure!(
        candidate_ids.len() == candidates.len(),
        "cohort candidates repeat"
    );
    ensure!(
        proposal.union(&retained).cloned().collect::<BTreeSet<_>>() == candidate_ids,
        "cohort candidates do not exactly partition proposal and retained-parent roles"
    );
    Ok(proposal.len() as u64)
}

fn campaign_task_count(campaigns: &[Value]) -> Result<u64> {
    campaigns.iter().try_fold(0_u64, |total, campaign| {
        let task_count = unsigned(campaign, "taskCount")
            .or_else(|_| unsigned(member(campaign, "campaignFreeze")?, "taskCount"))
            .or_else(|_| {
                unsigned(
                    member(member(campaign, "receipt")?, "campaignFreeze")?,
                    "taskCount",
                )
            })?;
        total
            .checked_add(task_count)
            .context("campaign task count overflow")
    })
}

fn build_state_application_sidecar(
    source: &Source,
    manifest: &Manifest,
    commit: &Value,
    record: &Value,
    patch: &Value,
) -> Result<Value> {
    ensure!(
        member(patch, "generationRecord")? == record
            && sha(patch, "generationRecordSha256")? == sha(record, "generationRecordSha256")?,
        "state patch does not contain the exact Rust generation record"
    );
    let authority = &source.proposal_state_authority;
    let generation_kind = text(authority, "generationKind")?;
    let finalization = json!({
        "sourceSha256":source.source_sha256,
        "manifestSha256":manifest.manifest_sha256,
        "commitSha256":sha(commit, "commitSha256")?,
        "generationRecordSha256":sha(record, "generationRecordSha256")?,
        "statePatchSha256":sha(patch, "statePatchSha256")?,
    });
    let next_state = json!({
        "stage":"generation_proposal",
        "currentGenerationIndex":unsigned(patch, "nextGenerationIndex")?,
        "uniqueCandidatesEvaluated":unsigned(patch, "uniqueCandidatesEvaluated")?,
        "workerTasksCompleted":unsigned(patch, "workerTasksCompleted")?,
        "nextImmigrantContinuationOrdinal":unsigned(patch, "nextImmigrantContinuationOrdinal")?,
        "uniqueIdentityCounts":member(patch, "uniqueIdentityCounts")?,
        "duplicateCounters":member(patch, "duplicateCounters")?,
        "proposalSlotCounters":member(patch, "proposalSlotCounters")?,
        "completedGenerationsSha256":sha(patch, "completedGenerationsSha256")?,
    });
    let identity_ledger_promotion = json!({
        "inputIdentityLedgerSha256":member(authority, "inputIdentityLedgerSha256")?,
        "outputRelativePath":member(authority, "outputIdentityLedgerRelativePath")?,
        "outputIdentityLedgerSha256":sha(authority, "outputIdentityLedgerSha256")?,
        "outputIdentityLedgerFileSha256":sha(authority, "outputIdentityLedgerFileSha256")?,
    });
    let mut sidecar = json!({
        "schemaVersion":STATE_APPLICATION_SIDECAR_SCHEMA,
        "contractVersion":CONTRACT_VERSION,
        "generationIndex":source.generation_index,
        "generationKind":generation_kind,
        "configSha256":sha(&source.state_basis, "configSha256")?,
        "stateBasisSha256":sha(&source.state_basis, "stateBasisSha256")?,
        "completedGenerationsBeforeSha256":sha(&source.state_basis, "completedGenerationsSha256")?,
        "semanticAuthoritySha256":source.semantic_authority_sha256,
        "runtimeAuthoritySha256":source.runtime_authority_sha256,
        "finalization":finalization,
        "proposalStateAuthority":{
            "proposalManifestSha256":sha(authority, "proposalManifestSha256")?,
            "proposalReceiptSha256":sha(authority, "proposalReceiptSha256")?,
            "generationJournalSha256":sha(authority, "generationJournalSha256")?,
        },
        "nextState":next_state,
        "identityLedgerPromotion":identity_ledger_promotion,
    });
    add_self_hash(&mut sidecar, "sidecarSha256")?;
    validate_state_application_sidecar_value(&sidecar, source, manifest, commit, record, patch)?;
    Ok(sidecar)
}

fn validate_state_application_sidecar(
    root: &Path,
    manifest: &Manifest,
    commit: &Value,
) -> Result<()> {
    let sidecar_path = output_path(root, STATE_APPLICATION_SIDECAR_PATH)?;
    if !sidecar_path.exists() {
        // Compact historical commits predate the v2 state patch. They are
        // restart-checkable output bundles, but cannot represent a v2 state
        // application boundary. A v2 patch without its receipt-last sidecar
        // is always rejected.
        let patch_bytes = fs::read(output_path(root, STATE_PATCH_PATH)?)?;
        let patch_value: Value = serde_json::from_slice(&patch_bytes).unwrap_or(Value::Null);
        ensure!(
            text(&patch_value, "schemaVersion").ok()
                != Some("temporal_qd_generation_state_patch_v2"),
            "v2 state patch lacks its state application sidecar"
        );
        return Ok(());
    }
    let sidecar = read_self_hashed(
        &sidecar_path,
        "sidecarSha256",
        STATE_APPLICATION_SIDECAR_SCHEMA,
    )?;
    validate_state_application_sidecar_shape(&sidecar)?;
    let record = read_self_hashed(
        &output_path(root, RECORD_PATH)?,
        "generationRecordSha256",
        "temporal_qd_generation_record_v2",
    )?;
    let patch = read_self_hashed(
        &output_path(root, STATE_PATCH_PATH)?,
        "statePatchSha256",
        "temporal_qd_generation_state_patch_v2",
    )?;
    let finalization = member(&sidecar, "finalization")?;
    ensure!(
        sha(finalization, "sourceSha256")? == manifest.source_sha256
            && sha(finalization, "manifestSha256")? == manifest.manifest_sha256
            && sha(finalization, "commitSha256")? == sha(commit, "commitSha256")?
            && sha(finalization, "generationRecordSha256")?
                == sha(&record, "generationRecordSha256")?
            && sha(finalization, "statePatchSha256")? == sha(&patch, "statePatchSha256")?,
        "state application sidecar finalization binding drifted"
    );
    ensure!(
        member(&patch, "generationRecord")? == &record
            && sha(&patch, "generationRecordSha256")? == sha(&record, "generationRecordSha256")?,
        "state application sidecar patch/record drifted"
    );
    ensure!(
        sha(&sidecar, "runtimeAuthoritySha256")? == manifest.runtime_authority_sha256
            && sha(&sidecar, "semanticAuthoritySha256")? == manifest.semantic_authority_sha256
            && unsigned(&sidecar, "generationIndex")? == unsigned(commit, "generationIndex")?,
        "state application sidecar authority or generation drifted"
    );
    Ok(())
}

fn validate_state_application_sidecar_shape(sidecar: &Value) -> Result<()> {
    exact_keys(
        object(sidecar, "state application sidecar")?,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "generationKind",
            "configSha256",
            "stateBasisSha256",
            "completedGenerationsBeforeSha256",
            "semanticAuthoritySha256",
            "runtimeAuthoritySha256",
            "finalization",
            "proposalStateAuthority",
            "nextState",
            "identityLedgerPromotion",
            "sidecarSha256",
        ],
        "state application sidecar",
    )?;
    exact_keys(
        object(member(sidecar, "finalization")?, "sidecar finalization")?,
        &[
            "sourceSha256",
            "manifestSha256",
            "commitSha256",
            "generationRecordSha256",
            "statePatchSha256",
        ],
        "sidecar finalization",
    )?;
    exact_keys(
        object(
            member(sidecar, "proposalStateAuthority")?,
            "sidecar proposal authority",
        )?,
        &[
            "proposalManifestSha256",
            "proposalReceiptSha256",
            "generationJournalSha256",
        ],
        "sidecar proposal authority",
    )?;
    exact_keys(
        object(member(sidecar, "nextState")?, "sidecar next state")?,
        &[
            "stage",
            "currentGenerationIndex",
            "uniqueCandidatesEvaluated",
            "workerTasksCompleted",
            "nextImmigrantContinuationOrdinal",
            "uniqueIdentityCounts",
            "duplicateCounters",
            "proposalSlotCounters",
            "completedGenerationsSha256",
        ],
        "sidecar next state",
    )?;
    exact_keys(
        object(
            member(sidecar, "identityLedgerPromotion")?,
            "sidecar identity promotion",
        )?,
        &[
            "inputIdentityLedgerSha256",
            "outputRelativePath",
            "outputIdentityLedgerSha256",
            "outputIdentityLedgerFileSha256",
        ],
        "sidecar identity promotion",
    )?;
    ensure!(
        matches!(text(sidecar, "generationKind")?, "g0" | "evolved"),
        "sidecar generation kind is invalid"
    );
    ensure!(
        text(
            member(sidecar, "identityLedgerPromotion")?,
            "outputRelativePath"
        )? == "proposal/v5-native/identity-ledger.json",
        "sidecar identity ledger output path drifted"
    );
    Ok(())
}

fn validate_state_application_sidecar_value(
    sidecar: &Value,
    source: &Source,
    manifest: &Manifest,
    commit: &Value,
    record: &Value,
    patch: &Value,
) -> Result<()> {
    exact_keys(
        object(sidecar, "state application sidecar")?,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "generationKind",
            "configSha256",
            "stateBasisSha256",
            "completedGenerationsBeforeSha256",
            "semanticAuthoritySha256",
            "runtimeAuthoritySha256",
            "finalization",
            "proposalStateAuthority",
            "nextState",
            "identityLedgerPromotion",
            "sidecarSha256",
        ],
        "state application sidecar",
    )?;
    verify_self_hash(sidecar, "sidecarSha256", "state application sidecar")?;
    exact_keys(
        object(member(sidecar, "finalization")?, "sidecar finalization")?,
        &[
            "sourceSha256",
            "manifestSha256",
            "commitSha256",
            "generationRecordSha256",
            "statePatchSha256",
        ],
        "sidecar finalization",
    )?;
    exact_keys(
        object(
            member(sidecar, "proposalStateAuthority")?,
            "sidecar proposal authority",
        )?,
        &[
            "proposalManifestSha256",
            "proposalReceiptSha256",
            "generationJournalSha256",
        ],
        "sidecar proposal authority",
    )?;
    exact_keys(
        object(member(sidecar, "nextState")?, "sidecar next state")?,
        &[
            "stage",
            "currentGenerationIndex",
            "uniqueCandidatesEvaluated",
            "workerTasksCompleted",
            "nextImmigrantContinuationOrdinal",
            "uniqueIdentityCounts",
            "duplicateCounters",
            "proposalSlotCounters",
            "completedGenerationsSha256",
        ],
        "sidecar next state",
    )?;
    exact_keys(
        object(
            member(sidecar, "identityLedgerPromotion")?,
            "sidecar identity ledger promotion",
        )?,
        &[
            "inputIdentityLedgerSha256",
            "outputRelativePath",
            "outputIdentityLedgerSha256",
            "outputIdentityLedgerFileSha256",
        ],
        "sidecar identity ledger promotion",
    )?;
    ensure!(
        text(sidecar, "contractVersion")? == CONTRACT_VERSION
            && unsigned(sidecar, "generationIndex")? == source.generation_index
            && sha(sidecar, "configSha256")? == sha(&source.state_basis, "configSha256")?
            && sha(sidecar, "stateBasisSha256")? == sha(&source.state_basis, "stateBasisSha256")?
            && sha(sidecar, "completedGenerationsBeforeSha256")?
                == sha(&source.state_basis, "completedGenerationsSha256")?
            && sha(sidecar, "semanticAuthoritySha256")? == manifest.semantic_authority_sha256
            && sha(sidecar, "runtimeAuthoritySha256")? == manifest.runtime_authority_sha256,
        "state application sidecar authority drifted"
    );
    let next = member(sidecar, "nextState")?;
    ensure!(
        text(next, "stage")? == "generation_proposal"
            && unsigned(next, "currentGenerationIndex")? == unsigned(patch, "nextGenerationIndex")?
            && unsigned(next, "uniqueCandidatesEvaluated")?
                == unsigned(patch, "uniqueCandidatesEvaluated")?
            && unsigned(next, "workerTasksCompleted")? == unsigned(patch, "workerTasksCompleted")?
            && unsigned(next, "nextImmigrantContinuationOrdinal")?
                == unsigned(patch, "nextImmigrantContinuationOrdinal")?
            && member(next, "uniqueIdentityCounts")? == member(patch, "uniqueIdentityCounts")?
            && member(next, "duplicateCounters")? == member(patch, "duplicateCounters")?
            && member(next, "proposalSlotCounters")? == member(patch, "proposalSlotCounters")?,
        "state application sidecar next state is not the absolute Rust patch"
    );
    let mut completed = source.completed_generation_records.clone();
    completed.push(record.clone());
    ensure!(
        sha(next, "completedGenerationsSha256")? == canonical_sha256(&Value::Array(completed))?,
        "state application sidecar completed generation root drifted"
    );
    let finalization = member(sidecar, "finalization")?;
    ensure!(
        sha(finalization, "sourceSha256")? == source.source_sha256
            && sha(finalization, "manifestSha256")? == manifest.manifest_sha256
            && sha(finalization, "commitSha256")? == sha(commit, "commitSha256")?
            && sha(finalization, "generationRecordSha256")?
                == sha(record, "generationRecordSha256")?
            && sha(finalization, "statePatchSha256")? == sha(patch, "statePatchSha256")?,
        "state application sidecar finalization substitution"
    );
    Ok(())
}

fn execution(
    source: &Source,
    manifest: &Manifest,
    plan: &Value,
    commit: Value,
    restart: bool,
    started: Instant,
) -> Value {
    json!({"schemaVersion":EXECUTION_SCHEMA,"status":"committed","sourceSha256":source.source_sha256,"manifestSha256":manifest.manifest_sha256,"generationIndex":source.generation_index,"auxiliaryPlanSha256":sha(plan,"planSha256").unwrap(),"commitSha256":sha(&commit,"commitSha256").unwrap(),"restart":restart,"rawResultReads":0,"elapsedMilliseconds":started.elapsed().as_millis() as u64,"commit":commit})
}
fn restart_execution(manifest: &Manifest, commit: Value, started: Instant) -> Value {
    json!({"schemaVersion":EXECUTION_SCHEMA,"status":"committed","sourceSha256":manifest.source_sha256,"manifestSha256":manifest.manifest_sha256,"generationIndex":unsigned(&commit,"generationIndex").unwrap(),"auxiliaryPlanSha256":sha(&commit,"auxiliaryPlanSha256").unwrap(),"commitSha256":sha(&commit,"commitSha256").unwrap(),"restart":true,"restartValidation":"compact_commit_and_output_hashes","rawResultReads":0,"elapsedMilliseconds":started.elapsed().as_millis() as u64,"commit":commit})
}

fn provisional_map(source: &Source) -> Result<BTreeMap<String, Value>> {
    let rows = array(&source.provisional, "candidates")?;
    ensure!(
        unsigned(&source.provisional, "candidateCount")? == rows.len() as u64,
        "provisional count mismatch"
    );
    let mut out = BTreeMap::new();
    for row in rows {
        let id = text(row, "candidateId")?.to_owned();
        ensure!(
            out.insert(id, row.clone()).is_none(),
            "duplicate provisional candidate"
        );
        for f in [
            "candidateIdentitySha256",
            "programSha256",
            "profileSnapshotSha256",
        ] {
            sha(row, f)?;
        }
        number_f64(row, "currentPanelRank")?;
        text(row, "cellId")?;
    }
    Ok(out)
}
fn required_months(source: &Source) -> Result<u64> {
    let panels = array(member(&source.value, "rotatingEvidence")?, "panels")?;
    source
        .required_panel_ids
        .iter()
        .map(|id| {
            panels
                .iter()
                .find(|p| text(p, "panelId").ok() == Some(id))
                .context("required panel missing")
                .and_then(|p| unsigned(p, "totalMonths"))
        })
        .sum()
}
fn panel_order(source: &Source, panel: &str) -> usize {
    source
        .required_panel_ids
        .iter()
        .position(|v| v == panel)
        .unwrap_or(usize::MAX)
}
fn objectives(v: &Value) -> Result<[f64; 4]> {
    let o = member(v, "robustObjectives")?;
    Ok([
        number_f64(o, "worstWindowConservativeNetR")?,
        -number_f64(o, "drawdown")?,
        -number_f64(o, "costDrag")?,
        number_f64(o, "novelty")?,
    ])
}
fn dominates_vec(a: [f64; 4], b: [f64; 4]) -> bool {
    a.iter().zip(b).all(|(x, y)| *x >= y) && a.iter().zip(b).any(|(x, y)| *x > y)
}
fn median(values: &[f64]) -> f64 {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    if v.len() % 2 == 1 {
        v[v.len() / 2]
    } else {
        (v[v.len() / 2 - 1] + v[v.len() / 2]) / 2.0
    }
}
fn cmp_desc(a: f64, b: f64) -> Ordering {
    b.total_cmp(&a)
}
fn parent_member_cmp(a: &Value, b: &Value) -> Ordering {
    let quality_a = text(a, "archiveLane").ok() == Some("quality");
    let quality_b = text(b, "archiveLane").ok() == Some("quality");
    quality_b
        .cmp(&quality_a)
        .then_with(|| {
            unsigned(
                member(a, "cumulativeEvidence").unwrap(),
                "robustParetoFront",
            )
            .unwrap_or(0)
            .cmp(
                &unsigned(
                    member(b, "cumulativeEvidence").unwrap(),
                    "robustParetoFront",
                )
                .unwrap_or(0),
            )
        })
        .then_with(|| {
            let x = optional_f64(
                member(a, "cumulativeEvidence").unwrap(),
                "robustCrowdingDistance",
            )
            .unwrap_or(None)
            .unwrap_or(f64::INFINITY);
            let y = optional_f64(
                member(b, "cumulativeEvidence").unwrap(),
                "robustCrowdingDistance",
            )
            .unwrap_or(None)
            .unwrap_or(f64::INFINITY);
            cmp_desc(x, y)
        })
        .then_with(|| {
            cmp_desc(
                number_f64(
                    member(a, "robustObjectives").unwrap(),
                    "worstWindowConservativeNetR",
                )
                .unwrap_or(0.0),
                number_f64(
                    member(b, "robustObjectives").unwrap(),
                    "worstWindowConservativeNetR",
                )
                .unwrap_or(0.0),
            )
        })
        .then_with(|| {
            number_f64(member(a, "robustObjectives").unwrap(), "drawdown")
                .unwrap_or(0.0)
                .total_cmp(
                    &number_f64(member(b, "robustObjectives").unwrap(), "drawdown").unwrap_or(0.0),
                )
        })
        .then_with(|| {
            number_f64(member(a, "robustObjectives").unwrap(), "costDrag")
                .unwrap_or(0.0)
                .total_cmp(
                    &number_f64(member(b, "robustObjectives").unwrap(), "costDrag").unwrap_or(0.0),
                )
        })
        .then_with(|| {
            cmp_desc(
                number_f64(member(a, "robustObjectives").unwrap(), "novelty").unwrap_or(0.0),
                number_f64(member(b, "robustObjectives").unwrap(), "novelty").unwrap_or(0.0),
            )
        })
        .then_with(|| {
            cmp_desc(
                number_f64(member(a, "cumulativeEvidence").unwrap(), "currentPanelRank")
                    .unwrap_or(0.0),
                number_f64(member(b, "cumulativeEvidence").unwrap(), "currentPanelRank")
                    .unwrap_or(0.0),
            )
        })
        .then_with(|| {
            text(a, "candidateId")
                .unwrap_or("")
                .cmp(text(b, "candidateId").unwrap_or(""))
        })
}

fn descriptor(path: &str, value: &Value, field: &str) -> Result<Value> {
    let bytes = canonical_json_line(value)?;
    Ok(
        json!({"path":path,field:sha(value,field)?,"bytes":bytes.len(),"fileSha256":sha256_bytes(&bytes)}),
    )
}

fn validate_commit_outputs(root: &Path, commit: &Value) -> Result<()> {
    for (descriptor_name, expected_path) in [
        ("cumulativeArchive", CUMULATIVE_PATH),
        ("parentArchive", ARCHIVE_PATH),
        ("generationFunnel", FUNNEL_PATH),
        ("generationFunnelSnapshot", FUNNEL_SNAPSHOT_PATH),
        ("checkpoint", CHECKPOINT_PATH),
        ("ledger", LEDGER_PATH),
        ("generationRecord", RECORD_PATH),
        ("statePatch", STATE_PATCH_PATH),
    ] {
        let descriptor = member(commit, descriptor_name)?;
        ensure!(
            text(descriptor, "path")? == expected_path,
            "committed descriptor path drifted"
        );
        let path = output_path(root, expected_path)?;
        ensure!(
            path.is_file(),
            "committed output is missing: {expected_path}"
        );
        ensure!(
            !fs::symlink_metadata(&path)?.file_type().is_symlink(),
            "committed output must not be a symlink: {expected_path}"
        );
        let metadata = fs::metadata(&path)?;
        ensure!(
            metadata.len() == unsigned(descriptor, "bytes")?,
            "committed output size drifted: {expected_path}"
        );
        ensure!(
            sha256_file(&path)? == sha(descriptor, "fileSha256")?,
            "committed output content drifted: {expected_path}"
        );
    }
    Ok(())
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    // Keep this heap-backed: the Windows release binary's main thread has a
    // much smaller stack than Rust's test threads, and a 1 MiB local buffer
    // can overflow before restart validation gets a chance to report errors.
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("sha256:{:x}", hash.finalize()))
}
fn add_self_hash(value: &mut Value, field: &str) -> Result<()> {
    let hash = canonical_sha256(value)?;
    object_mut(value, "self-hashed object")?.insert(field.into(), Value::String(hash));
    Ok(())
}
fn verify_self_hash(value: &Value, field: &str, name: &str) -> Result<()> {
    let supplied = sha(value, field)?;
    ensure!(
        canonical_sha256_without_object_field(value, field)? == supplied,
        "{name} identity mismatch"
    );
    Ok(())
}
fn read_self_hashed(path: &Path, field: &str, schema: &str) -> Result<Value> {
    let bytes = fs::read(path)?;
    let v: Value = serde_json::from_slice(&bytes)?;
    ensure!(
        canonical_json_line(&v)? == bytes,
        "committed file is noncanonical"
    );
    ensure!(
        text(&v, "schemaVersion")? == schema,
        "committed schema mismatch"
    );
    verify_self_hash(&v, field, "committed object")?;
    Ok(v)
}
fn publish_value_once(root: &Path, name: &str, value: &Value) -> Result<()> {
    let bytes = canonical_json_line(value)?;
    let path = output_path(root, name)?;
    if path.exists() {
        ensure!(
            !fs::symlink_metadata(&path)?.file_type().is_symlink(),
            "immutable output {name} must not be a symlink"
        );
        ensure!(fs::read(&path)? == bytes, "immutable output {name} differs");
        return Ok(());
    }
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let parent = path.parent().context("output has no parent")?;
    // The output root is intentionally descriptive and can be deep.  Keep the
    // temporary basename short so Windows publication does not exceed the
    // classic path-length boundary merely by repeating the output filename.
    let tmp = parent.join(format!(".p{}.{}.tmp", std::process::id(), nonce));
    let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    f.write_all(&bytes)?;
    f.sync_all()?;
    drop(f);
    match fs::rename(&tmp, &path) {
        Ok(()) => {
            sync_directory(parent)?;
            Ok(())
        }
        Err(_error) if path.exists() => {
            let identical = fs::read(&path)? == bytes;
            let _ = fs::remove_file(&tmp);
            ensure!(identical, "immutable output {name} differs");
            sync_directory(parent)?;
            Ok(())
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            Err(e).with_context(|| {
                format!(
                    "rename synced immutable output into place: {}",
                    path.display()
                )
            })
        }
    }
}

fn output_path(root: &Path, name: &str) -> Result<PathBuf> {
    let relative = Path::new(name);
    ensure!(
        !relative.is_absolute()
            && relative
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_))),
        "output path must be a safe relative path"
    );
    let root = root
        .canonicalize()
        .context("canonicalize finalization root")?;
    let mut current = root.clone();
    let components = relative.components().collect::<Vec<_>>();
    ensure!(!components.is_empty(), "output path is empty");
    for component in &components[..components.len() - 1] {
        current.push(component.as_os_str());
        if current.exists() {
            ensure!(
                !fs::symlink_metadata(&current)?.file_type().is_symlink() && current.is_dir(),
                "output directory must be a real directory"
            );
        } else {
            fs::create_dir(&current)?;
        }
    }
    let path = root.join(relative);
    ensure!(path.starts_with(&root), "output escapes finalization root");
    Ok(path)
}
#[cfg(not(windows))]
fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)?.sync_all()?;
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
fn existing_file(path: &Path, name: &str) -> Result<PathBuf> {
    ensure!(path.is_file(), "{name} is missing");
    Ok(path.canonicalize()?)
}

fn existing_file_under(path: &Path, root: &Path, name: &str) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "{name} execution path must be absolute");
    let root = root.canonicalize()?;
    let resolved = existing_file(path, name)?;
    ensure!(
        resolved.starts_with(&root),
        "{name} escapes finalization root"
    );
    Ok(resolved)
}
fn object<'a>(v: &'a Value, name: &str) -> Result<&'a Map<String, Value>> {
    v.as_object()
        .ok_or_else(|| anyhow!("{name} must be an object"))
}
fn object_mut<'a>(v: &'a mut Value, name: &str) -> Result<&'a mut Map<String, Value>> {
    v.as_object_mut()
        .ok_or_else(|| anyhow!("{name} must be an object"))
}
fn member<'a>(v: &'a Value, key: &str) -> Result<&'a Value> {
    object(v, "object")?
        .get(key)
        .ok_or_else(|| anyhow!("missing {key}"))
}
fn nullable_member<'a>(v: &'a Value, key: &str) -> Result<Option<&'a Value>> {
    let x = member(v, key)?;
    Ok((!x.is_null()).then_some(x))
}
fn text<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    member(v, key)?
        .as_str()
        .ok_or_else(|| anyhow!("{key} must be text"))
}
fn array<'a>(v: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    member(v, key)?
        .as_array()
        .ok_or_else(|| anyhow!("{key} must be an array"))
}
fn unsigned(v: &Value, key: &str) -> Result<u64> {
    member(v, key)?
        .as_u64()
        .ok_or_else(|| anyhow!("{key} must be unsigned"))
}
fn sha(v: &Value, key: &str) -> Result<String> {
    let s = text(v, key)?;
    ensure!(
        s.len() == 71
            && s.starts_with("sha256:")
            && s[7..]
                .bytes()
                .all(|b| b.is_ascii_hexdigit()
                    && (!b.is_ascii_alphabetic() || b.is_ascii_lowercase())),
        "{key} is not a canonical sha256"
    );
    Ok(s.to_owned())
}
fn number_f64(v: &Value, key: &str) -> Result<f64> {
    let n = member(v, key)?
        .as_f64()
        .ok_or_else(|| anyhow!("{key} must be numeric"))?;
    ensure!(n.is_finite(), "{key} must be finite");
    Ok(n)
}
fn optional_f64(v: &Value, key: &str) -> Result<Option<f64>> {
    match object(v, "object")?.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(x) => {
            let n = x.as_f64().ok_or_else(|| anyhow!("{key} must be numeric"))?;
            ensure!(n.is_finite(), "{key} must be finite");
            Ok(Some(n))
        }
    }
}
fn optional_i64(v: &Value, key: &str) -> Result<Option<i64>> {
    match object(v, "object")?.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(x) => Ok(Some(
            x.as_i64().ok_or_else(|| anyhow!("{key} must be integer"))?,
        )),
    }
}
fn exact_keys(map: &Map<String, Value>, keys: &[&str], name: &str) -> Result<()> {
    let actual = map.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = keys.iter().copied().collect::<BTreeSet<_>>();
    ensure!(actual == expected, "{name} fields differ");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const HASH_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HASH_B: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const HASH_C: &str = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    #[test]
    fn immutable_value_publish_uses_rename_and_is_restart_safe() -> Result<()> {
        let root = tempdir()?;
        let value = json!({"value": 1});
        publish_value_once(root.path(), RECORD_PATH, &value)?;
        let path = root.path().join(RECORD_PATH);
        let expected = canonical_json_line(&value)?;
        assert_eq!(fs::read(&path)?, expected);

        publish_value_once(root.path(), RECORD_PATH, &value)?;
        let error = publish_value_once(root.path(), RECORD_PATH, &json!({"value": 2}))
            .expect_err("divergent immutable output must fail");
        assert!(error.to_string().contains("immutable output"));
        assert_eq!(fs::read(path)?, expected);
        Ok(())
    }

    #[test]
    fn derives_scalar_cell_capacity_from_the_frozen_archive_policy() {
        let policy = json!({
            "frozenPolicy": {"archive": {"defaultCellCapacity": 4}}
        });
        assert_eq!(derive_cell_capacity(&policy).unwrap(), 4);
    }

    #[test]
    fn previous_parent_summary_requires_complete_sorted_archive_accounting() {
        let mut summary = json!({
            "schemaVersion": PREVIOUS_PARENT_SUMMARY_SCHEMA,
            "archiveSha256": HASH_A,
            "candidateCountSeen": 11,
            "memberCount": 2,
            "cellIds": ["a", "z"],
            "bidirectionalPairPolicy": {"schemaVersion":"fixture"}
        });
        add_self_hash(&mut summary, "summarySha256").unwrap();
        validate_previous_parent_summary(&summary).unwrap();

        let mut reordered = summary.clone();
        reordered["cellIds"] = json!(["z", "a"]);
        add_self_hash(&mut reordered, "summarySha256").unwrap();
        assert!(validate_previous_parent_summary(&reordered).is_err());

        let mut incomplete = summary;
        incomplete
            .as_object_mut()
            .unwrap()
            .remove("candidateCountSeen");
        add_self_hash(&mut incomplete, "summarySha256").unwrap();
        assert!(validate_previous_parent_summary(&incomplete).is_err());
    }

    fn synthetic_bundle(candidate: &Value, panel_id: &str, window_id: &str) -> Value {
        let inactive_side = json!({
            "closedTrades":0,"wins":0,"losses":0,"flatTrades":0,
            "grossR":0.0,"netR":0.0,"costR":0.0,"holdingBars":0,"holdingHours":0.0,
            "activeWindowCount":0,"closeReasonCounts":{},"actionCounts":{},
            "transitionCounts":{},"terminalStatusCounts":{},"terminalDirectionCount":0,
            "conflictAbstentions":0,"tradeSequence":[],"active":false,
            "activeWindowFraction":0.0,"exposureProxy":0.0,"averageHoldingBars":0.0,
            "closeReasonDistribution":{},"actionDistribution":{},"transitionDistribution":{},
        });
        let mut active_side = inactive_side.clone();
        active_side["closedTrades"] = json!(20);
        active_side["wins"] = json!(20);
        active_side["grossR"] = json!(1.1);
        active_side["netR"] = json!(1.0);
        active_side["costR"] = json!(0.1);
        active_side["activeWindowCount"] = json!(1);
        active_side["active"] = json!(true);
        active_side["activeWindowFraction"] = json!(1.0);
        let realized_behavior = json!({
            "schemaVersion":"temporal_realized_behavior_v1","windowId":window_id,
            "reportedClosedTrades":20,"materializedClosedTrades":20,
            "unattributedClosedTrades":0,"observations":100,"terminal":{},
            "conflictAbstentions":0,"unattributedConflictAbstentions":0,
            "sides":{"long":active_side,"short":inactive_side},
        });
        let mut record = json!({
            "schemaVersion": WINDOW_SCHEMA,
            "candidateId": text(candidate,"candidateId").unwrap(),
            "candidateIdentitySha256": sha(candidate,"candidateIdentitySha256").unwrap(),
            "programSha256": sha(candidate,"programSha256").unwrap(),
            "panelId": panel_id,
            "windowId": window_id,
            "analysisWindowStart": "2020-01-01T00:00:00Z",
            "analysisWindowEnd": "2020-04-01T00:00:00Z",
            "evidencePlanSemanticSha256": HASH_A,
            "metrics": {
                "conservativeNetR": 1.0,
                "noCostNetR": 1.1,
                "maxDrawdownR": 0.2,
                "closedTrades": 20,
                "sourceProfileSnapshotSha256": HASH_C,
                "resolvedProfileSnapshotSha256": HASH_C,
                "resolvedProgramSha256": HASH_B,
                "realizedBehavior": realized_behavior,
            },
            "evidenceDigestSha256": HASH_A,
            "rawTaskProvenance": {"authorityId":HASH_A,"taskMatrixSha256":HASH_A,"taskId":format!("{panel_id}-{window_id}"),"resultSha256":HASH_A},
        });
        add_self_hash(&mut record, "recordSha256").unwrap();
        let mut bundle = json!({
            "schemaVersion": BUNDLE_SCHEMA,
            "rotatingEvidenceSha256": HASH_A,
            "candidateId": text(candidate,"candidateId").unwrap(),
            "candidateIdentitySha256": HASH_A,
            "programSha256": HASH_B,
            "panelId": panel_id,
            "windowEvidenceDigests": [{"windowId":window_id,"evidenceDigestSha256":HASH_A,"recordSha256":sha(&record,"recordSha256").unwrap()}],
            "windowEvidence": [record.clone()],
            "rawTaskProvenance": [{"windowId":window_id,"authorityId":HASH_A,"taskMatrixSha256":HASH_A,"taskId":format!("{panel_id}-{window_id}"),"resultSha256":HASH_A}],
        });
        add_self_hash(&mut bundle, "bundleSha256").unwrap();
        bundle
    }

    fn synthetic_source(baseline: Vec<Value>, receipts: Vec<Value>) -> Source {
        let policy = {
            let mut value = json!({
                "schemaVersion":"temporal_qd_robust_breeder_policy_v1",
                "minimumAverageClosedTradesPerCandidateMonth":4.0,
                "minimumActiveWindowFraction":0.75,
                "qualityRequiresPositiveCumulativeConservativeNetR":true,
                "qualityRequiresPositiveMedianWindowConservativeNetR":true,
                "frontierMaximumFraction":0.2,
                "worstWindowConservativeNetRIsHardGate":false,
                "drawdownIsHardGate":false,
                "objectiveDimensions":["worstWindowConservativeNetR","drawdown","costDrag","novelty"],
            });
            add_self_hash(&mut value, "policySha256").unwrap();
            value
        };
        let mut rotating = json!({
            "schemaVersion":ROTATING_SCHEMA,
            "panels":[
                {"panelId":"panel-1","windowIds":["w1"],"windows":[{"windowId":"w1"}],"totalMonths":3},
                {"panelId":"panel-2","windowIds":["w2"],"windows":[{"windowId":"w2"}],"totalMonths":3}
            ],
            "absoluteGenerationMapping":{"cycleLength":2},
            "robustSelection":{"breederWidth":1,"policy":policy},
        });
        add_self_hash(&mut rotating, "rotatingEvidenceSha256").unwrap();
        // Test helpers deliberately bind the synthetic curriculum to the
        // fixed hash used by their evidence bundles.
        rotating["rotatingEvidenceSha256"] = json!(HASH_A);
        let candidate = json!({
            "candidateId":"candidate-1","candidateIdentitySha256":HASH_A,
            "programSha256":HASH_B,"profileSnapshotSha256":HASH_C,
            "cellId":"cell-1","currentPanelRank":1.0,"novelty":0.0,
        });
        let mut cohort = json!({
            "schemaVersion":COHORT_SCHEMA,"rotatingEvidenceSha256":HASH_A,
            "generationIndex":2,"panelId":"panel-2","candidates":[{"candidateId":"candidate-1","candidateIdentitySha256":HASH_A,"cohortRole":"new_proposal"}],
            "newProposalCandidateIds":["candidate-1"],"retainedParentEvaluationCandidateIds":[],"parentReevaluationIsProposal":false,
        });
        add_self_hash(&mut cohort, "cohortSha256").unwrap();
        let mut provisional = json!({
            "schemaVersion":PROVISIONAL_SCHEMA,"generationIndex":2,"panelId":"panel-2",
            "cohortSha256":sha(&cohort,"cohortSha256").unwrap(),"candidateCount":1,"candidates":[candidate],
        });
        add_self_hash(&mut provisional, "provisionalSha256").unwrap();
        Source {
            value: json!({"rotatingEvidence":rotating}),
            generation_index: 2,
            rotating_sha: HASH_A.into(),
            current_panel_id: "panel-2".into(),
            required_panel_ids: vec!["panel-1".into(), "panel-2".into()],
            breeder_width: 1,
            cohort,
            provisional,
            baseline_bundles: baseline,
            complete_bundle_snapshot: false,
            receipts,
            prior_cumulative: None,
            previous_parent: Value::Null,
            archive_policy: Value::Null,
            rich_members: Vec::new(),
            current_member_count: 1,
            cell_capacity: 1,
            campaigns: Vec::new(),
            funnel_source: Value::Null,
            state_basis: json!({}),
            semantic_authority_sha256: HASH_B.into(),
            runtime_authority_sha256: HASH_C.into(),
            completed_generation_records: Vec::new(),
            proposal_state_authority: json!({}),
            expected_plan: None,
            source_sha256: HASH_A.into(),
        }
    }

    fn current_policy_parity_source() -> Source {
        let candidate = json!({
            "candidateId":"candidate-1","candidateIdentitySha256":HASH_A,
            "programSha256":HASH_B,"profileSnapshotSha256":HASH_C,
        });
        let mut source = synthetic_source(
            vec![
                synthetic_bundle(&candidate, "panel-1", "w1"),
                synthetic_bundle(&candidate, "panel-2", "w2"),
            ],
            Vec::new(),
        );
        source.complete_bundle_snapshot = true;
        source.previous_parent = {
            let mut value = json!({
                "schemaVersion":"temporal_qd_previous_parent_archive_summary_v1",
                "archiveSha256":HASH_A,"candidateCountSeen":0,"memberCount":0,"cellIds":[],
            });
            add_self_hash(&mut value, "summarySha256").unwrap();
            value
        };
        // This is a bounded v4-shaped authority projection.  Production v2
        // validates its full known frozen-policy hash before reaching these
        // unchanged builders; the builder parity gate only needs the same
        // authenticated policy fields that determine emitted archive bytes.
        let direction_policy = json!({
            "schemaVersion":"temporal_direction_selection_policy_v1",
            "minimum_closed_trades_per_side":1,
            "minimum_active_windows_per_side":1,
            "minimum_acceptable_side_net_r":0.0,
            "harmful_opposite_net_r":-0.25,
        });
        source.archive_policy = json!({
            "schemaVersion":"temporal_qd_archive_policy_binding_v1",
            "qdVersion":QD_VERSION,"policyName":QD_POLICY_NAME,"policySha256":QD_POLICY_SHA256,
            "frozenPolicy":{
                "archive":{"defaultCellCapacity":1},
                "directionSelection":{
                    "selectionPolicy":direction_policy,
                    "selectionPolicySha256":canonical_sha256(&direction_policy).unwrap(),
                },
            },
        });
        source.rich_members = vec![json!({
            "candidateId":"candidate-1",
            "candidate":candidate,
            "descriptor":{"cellId":"cell-1"},
            "objectives":{"structuralComplexity":1.0},
            "aggregate":{},
        })];
        source.funnel_source = {
            let mut value = json!({
                "schemaVersion":FUNNEL_SOURCE_SCHEMA,
                "completenessPolicy":{"schemaVersion":"temporal_generation_funnel_completeness_v1"},
                "proposalAccounting":{"dispositionCounts":{"materialized":1},"originProposalCounts":{"immigrant":1}},
                "proposalAttempts":[{"proposalOrdinal":0,"attemptIdentitySha256":HASH_A,"originKind":"immigrant","disposition":"materialized","candidateId":"candidate-1","rawSourceProfileSha256":HASH_C}],
                "candidateStageRows":[{"candidateId":"candidate-1","identity":{"candidateId":"candidate-1","rawSourceProfileSha256":HASH_C},"stages":{"proposed":{"outcome":"proposed"},"staticallyReachable":{"outcome":"rejected"}},"terminalDisposition":"static_reachability_rejected","operatorIds":["seed"],"motifIds":["seed"],"direction":"long"}],
            });
            add_self_hash(&mut value, "funnelSourceSha256").unwrap();
            value
        };
        source
    }
    #[test]
    fn dominance_is_strict_and_multidimensional() {
        assert!(dominates_vec([2.0, 1.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0]));
        assert!(!dominates_vec([2.0, 0.0, 1.0, 1.0], [1.0, 1.0, 1.0, 1.0]));
        assert!(!dominates_vec([1.0; 4], [1.0; 4]));
    }
    #[test]
    fn median_matches_even_and_odd_oracle() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), 2.5);
    }

    #[test]
    fn compact_restart_needs_no_source_and_tamper_fails_closed() {
        let root = tempdir().unwrap();
        let source_sha = HASH_A;
        let mut manifest = json!({
            "schemaVersion":MANIFEST_SCHEMA,"contractVersion":CONTRACT_VERSION,
            "operation":OPERATION,"runtimeAuthoritySha256":HASH_B,"semanticAuthoritySha256":HASH_C,
            "sourcePath":root.path().join("absent-source.json").to_string_lossy(),
            "sourceSha256":source_sha,"resultPath":COMMIT_PATH,
        });
        add_self_hash(&mut manifest, "manifestSha256").unwrap();
        let manifest_path = root.path().join("manifest.json");
        fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();
        let mut commit = json!({
            "schemaVersion":COMMIT_SCHEMA,"contractVersion":CONTRACT_VERSION,
            "sourceSha256":source_sha,"manifestSha256":sha(&manifest,"manifestSha256").unwrap(),
            "runtimeAuthoritySha256":HASH_B,"semanticAuthoritySha256":HASH_C,
            "generationIndex":1,"auxiliaryPlanSha256":HASH_B,
        });
        for (descriptor_name, output_path) in [
            ("cumulativeArchive", CUMULATIVE_PATH),
            ("parentArchive", ARCHIVE_PATH),
            ("generationFunnel", FUNNEL_PATH),
            ("generationFunnelSnapshot", FUNNEL_SNAPSHOT_PATH),
            ("checkpoint", CHECKPOINT_PATH),
            ("ledger", LEDGER_PATH),
            ("generationRecord", RECORD_PATH),
            ("statePatch", STATE_PATCH_PATH),
        ] {
            let bytes = b"{}\n";
            let output = root.path().join(output_path);
            fs::create_dir_all(output.parent().unwrap()).unwrap();
            fs::write(output, bytes).unwrap();
            commit[descriptor_name] = json!({
                "path":output_path,"bytes":bytes.len(),"fileSha256":sha256_bytes(bytes)
            });
        }
        add_self_hash(&mut commit, "commitSha256").unwrap();
        let commit_path = root.path().join(COMMIT_PATH);
        fs::write(&commit_path, canonical_json_line(&commit).unwrap()).unwrap();
        let result = execute_manifest(&manifest_path).unwrap();
        assert_eq!(
            result["restartValidation"],
            "compact_commit_and_output_hashes"
        );
        assert_eq!(result["restart"], true);
        fs::remove_file(root.path().join(ARCHIVE_PATH)).unwrap();
        assert!(execute_manifest(&manifest_path).is_err());
        fs::write(root.path().join(ARCHIVE_PATH), b"{}\n").unwrap();
        commit["generationIndex"] = json!(2);
        fs::write(&commit_path, canonical_json_line(&commit).unwrap()).unwrap();
        assert!(execute_manifest(&manifest_path).is_err());
    }

    #[test]
    fn v2_manifest_and_source_are_exact_and_execution_paths_cannot_escape() {
        let root = tempdir().unwrap();
        let source_path = root.path().join("source.json");
        let mut source = json!({
            "schemaVersion":SOURCE_SCHEMA,
            "contractVersion":CONTRACT_VERSION,
            "finalizerContext":{"unchecked":"must-not-pass"},
        });
        add_self_hash(&mut source, "sourceSha256").unwrap();
        fs::write(&source_path, canonical_json_line(&source).unwrap()).unwrap();

        let mut manifest = json!({
            "schemaVersion":MANIFEST_SCHEMA,"contractVersion":CONTRACT_VERSION,
            "operation":OPERATION,"runtimeAuthoritySha256":HASH_B,"semanticAuthoritySha256":HASH_C,
            "sourcePath":source_path.to_string_lossy(),"sourceSha256":sha(&source,"sourceSha256").unwrap(),
            "resultPath":COMMIT_PATH,
        });
        add_self_hash(&mut manifest, "manifestSha256").unwrap();
        let manifest_path = root.path().join("manifest.json");
        fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();
        assert!(
            execute_manifest(&manifest_path).is_err(),
            "source unknown fields must fail closed"
        );

        let mut escaped = manifest.clone();
        escaped["sourcePath"] = json!(
            root.path()
                .parent()
                .unwrap()
                .join("source.json")
                .to_string_lossy()
        );
        escaped.as_object_mut().unwrap().remove("manifestSha256");
        add_self_hash(&mut escaped, "manifestSha256").unwrap();
        assert!(parse_manifest(&canonical_json_line(&escaped).unwrap(), root.path()).is_err());

        let mut unknown = manifest;
        unknown["unchecked"] = json!(true);
        unknown.as_object_mut().unwrap().remove("manifestSha256");
        add_self_hash(&mut unknown, "manifestSha256").unwrap();
        assert!(parse_manifest(&canonical_json_line(&unknown).unwrap(), root.path()).is_err());
    }

    #[test]
    fn v2_sealed_bases_and_coverage_reject_self_rehashed_drift() {
        let mut state_basis = json!({
            "schemaVersion":"temporal_qd_v5_generation_state_basis_v1",
            "configSha256":HASH_A,"generationIndex":2,"completedGenerationsSha256":HASH_B,
            "uniqueCandidatesEvaluated":3,"workerTasksCompleted":4,"nextImmigrantContinuationOrdinal":5,
            "uniqueIdentityCounts":{},"duplicateCounters":{},"proposalSlotCounters":{},
        });
        add_self_hash(&mut state_basis, "stateBasisSha256").unwrap();
        validate_state_basis(&state_basis, 2).unwrap();
        state_basis["generationIndex"] = json!(3);
        state_basis
            .as_object_mut()
            .unwrap()
            .remove("stateBasisSha256");
        add_self_hash(&mut state_basis, "stateBasisSha256").unwrap();
        assert!(validate_state_basis(&state_basis, 2).is_err());

        let mut cohort = json!({"schemaVersion":COHORT_SCHEMA,"generationIndex":2,"rotatingEvidenceSha256":HASH_A,"panelId":"panel-2","candidates":[],"newProposalCandidateIds":[],"retainedParentEvaluationCandidateIds":[],"parentReevaluationIsProposal":false});
        add_self_hash(&mut cohort, "cohortSha256").unwrap();
        let candidate = json!({"candidateId":"candidate-1","candidateIdentitySha256":HASH_A,"programSha256":HASH_B,"profileSnapshotSha256":HASH_C});
        let mut provisional = json!({"schemaVersion":PROVISIONAL_SCHEMA,"generationIndex":2,"panelId":"panel-2","cohortSha256":sha(&cohort,"cohortSha256").unwrap(),"candidateCount":1,"candidates":[candidate]});
        add_self_hash(&mut provisional, "provisionalSha256").unwrap();
        let bundles = vec![json!({"candidateId":"candidate-1","panelId":"panel-1"})];
        let mut coverage = json!({
            "schemaVersion":"temporal_qd_v5_panel_coverage_v1","generationIndex":2,
            "rotatingEvidenceSha256":HASH_A,"cohortSha256":sha(&cohort,"cohortSha256").unwrap(),
            "provisionalSha256":sha(&provisional,"provisionalSha256").unwrap(),
            "requiredPanelIds":["panel-1"],"candidatePanelBundleSha256":canonical_sha256(&Value::Array(bundles.clone())).unwrap(),
            "coverage":{"candidate-1":{"panelIds":["panel-1"]}},
        });
        add_self_hash(&mut coverage, "panelCoverageSha256").unwrap();
        validate_panel_coverage(
            &coverage,
            2,
            HASH_A,
            &cohort,
            &provisional,
            &["panel-1".into()],
            &bundles,
        )
        .unwrap();
        coverage["candidatePanelBundleSha256"] = json!(HASH_C);
        coverage
            .as_object_mut()
            .unwrap()
            .remove("panelCoverageSha256");
        add_self_hash(&mut coverage, "panelCoverageSha256").unwrap();
        assert!(
            validate_panel_coverage(
                &coverage,
                2,
                HASH_A,
                &cohort,
                &provisional,
                &["panel-1".into()],
                &bundles
            )
            .is_err()
        );
    }

    #[test]
    fn current_policy_fixture_keeps_stable_outputs_byte_identical_across_operational_roots() {
        let source_a = current_policy_parity_source();
        let mut source_b = source_a.clone();
        // These are execution observations, never source fields. They model
        // a different output root and dispatcher cap while exercising the
        // unchanged semantic builders from the same sealed result.
        source_b.source_sha256 = HASH_C.into();
        source_b.value["executionOnly"] = json!({"outputRoot":"D:/scratch/run-b","workerCap":1});

        let finalize = |source: &Source| -> (Value, Value, Value, Value, Value) {
            let plan = build_auxiliary_plan(source).unwrap();
            let bundles = admit_receipts(source, &plan).unwrap();
            let cumulative = build_cumulative_archive(source, &bundles).unwrap();
            let archive = build_parent_archive(source, &cumulative).unwrap();
            let (funnel, _) = build_funnel(source, &archive).unwrap();
            let checkpoint = build_checkpoint(source, &cumulative, &archive).unwrap();
            let ledger = build_ledger(source, &plan, &cumulative, &archive, &checkpoint).unwrap();
            (cumulative, archive, funnel, ledger, checkpoint)
        };
        let left = finalize(&source_a);
        let right = finalize(&source_b);
        for (expected, actual) in [
            (&left.0, &right.0),
            (&left.1, &right.1),
            (&left.2, &right.2),
            (&left.3, &right.3),
            (&left.4, &right.4),
        ] {
            assert_eq!(
                canonical_json_line(expected).unwrap(),
                canonical_json_line(actual).unwrap()
            );
        }

        let root_a = tempdir().unwrap();
        let root_b = tempdir().unwrap();
        for (name, value) in [
            (CUMULATIVE_PATH, &left.0),
            (ARCHIVE_PATH, &left.1),
            (FUNNEL_PATH, &left.2),
            (LEDGER_PATH, &left.3),
            (CHECKPOINT_PATH, &left.4),
        ] {
            publish_value_once(root_a.path(), name, value).unwrap();
            publish_value_once(root_b.path(), name, value).unwrap();
            assert_eq!(
                fs::read(output_path(root_a.path(), name).unwrap()).unwrap(),
                fs::read(output_path(root_b.path(), name).unwrap()).unwrap(),
                "operational output root changed a semantic artifact"
            );
        }
    }

    #[test]
    fn fast_ephemeral_research_seam_uses_the_same_native_builders() {
        let source = current_policy_parity_source();
        let plan = build_auxiliary_plan(&source).unwrap();
        let bundles = admit_receipts(&source, &plan).unwrap();
        let expected_cumulative = build_cumulative_archive(&source, &bundles).unwrap();
        let expected_archive = build_parent_archive(&source, &expected_cumulative).unwrap();

        let (actual_cumulative, actual_archive) =
            reduce_fast_ephemeral_loaded_source(&source).unwrap();

        assert_eq!(
            canonical_json_line(&actual_cumulative).unwrap(),
            canonical_json_line(&expected_cumulative).unwrap()
        );
        assert_eq!(
            canonical_json_line(&actual_archive).unwrap(),
            canonical_json_line(&expected_archive).unwrap()
        );
    }

    #[test]
    fn state_application_sidecar_uses_absolute_rust_state_and_rejects_substitution() {
        let mut source = synthetic_source(Vec::new(), Vec::new());
        let mut basis = json!({
            "schemaVersion":"temporal_qd_v5_generation_state_basis_v1","configSha256":HASH_A,
            "generationIndex":2,"completedGenerationsSha256":canonical_sha256(&Value::Array(Vec::new())).unwrap(),
            "uniqueCandidatesEvaluated":10,"workerTasksCompleted":20,"nextImmigrantContinuationOrdinal":30,
            "uniqueIdentityCounts":{"candidateIdentity":10},"duplicateCounters":{"candidateIdentity":1},"proposalSlotCounters":{"acceptedUniqueProposalSlots":10},
        });
        add_self_hash(&mut basis, "stateBasisSha256").unwrap();
        source.state_basis = basis;
        source.completed_generation_records = Vec::new();
        source.proposal_state_authority = json!({
            "generationKind":"g0","proposalManifestSha256":HASH_A,"proposalReceiptSha256":HASH_B,"generationJournalSha256":HASH_C,
            "inputIdentityLedgerSha256":null,"outputIdentityLedgerRelativePath":"proposal/v5-native/identity-ledger.json",
            "outputIdentityLedgerSha256":HASH_A,"outputIdentityLedgerFileSha256":HASH_B,
        });
        validate_proposal_state_authority(&source.proposal_state_authority).unwrap();
        let mut record =
            json!({"schemaVersion":"temporal_qd_generation_record_v2","generationIndex":2});
        add_self_hash(&mut record, "generationRecordSha256").unwrap();
        let mut patch = json!({
            "schemaVersion":"temporal_qd_generation_state_patch_v2","generationIndex":2,"nextGenerationIndex":3,
            "uniqueCandidatesEvaluated":11,"workerTasksCompleted":23,"nextImmigrantContinuationOrdinal":30,
            "uniqueIdentityCounts":{"candidateIdentity":10},"duplicateCounters":{"candidateIdentity":1},"proposalSlotCounters":{"acceptedUniqueProposalSlots":10},
            "completedGenerationsSha256":canonical_sha256(&Value::Array(vec![record.clone()])).unwrap(),
            "generationRecordSha256":sha(&record,"generationRecordSha256").unwrap(),"generationRecord":record,
        });
        add_self_hash(&mut patch, "statePatchSha256").unwrap();
        let manifest = Manifest {
            runtime_authority_sha256: HASH_C.into(),
            semantic_authority_sha256: HASH_B.into(),
            source_path: PathBuf::new(),
            source_sha256: HASH_A.into(),
            manifest_sha256: HASH_B.into(),
        };
        source.runtime_authority_sha256 = HASH_C.into();
        source.semantic_authority_sha256 = HASH_B.into();
        source.source_sha256 = HASH_A.into();
        let mut commit = json!({"schemaVersion":COMMIT_SCHEMA,"commitSha256":HASH_C});
        commit["commitSha256"] =
            json!(canonical_sha256_without_object_field(&commit, "commitSha256").unwrap());
        let sidecar = build_state_application_sidecar(
            &source,
            &manifest,
            &commit,
            member(&patch, "generationRecord").unwrap(),
            &patch,
        )
        .unwrap();
        assert_eq!(sidecar["nextState"]["uniqueCandidatesEvaluated"], 11);
        assert_eq!(sidecar["nextState"]["workerTasksCompleted"], 23);
        assert_eq!(
            sidecar["identityLedgerPromotion"]["inputIdentityLedgerSha256"],
            Value::Null
        );
        let mut substituted = sidecar;
        substituted["finalization"]["commitSha256"] = json!(HASH_A);
        substituted.as_object_mut().unwrap().remove("sidecarSha256");
        add_self_hash(&mut substituted, "sidecarSha256").unwrap();
        assert!(
            validate_state_application_sidecar_value(
                &substituted,
                &source,
                &manifest,
                &commit,
                member(&patch, "generationRecord").unwrap(),
                &patch
            )
            .is_err()
        );

        source.proposal_state_authority["generationKind"] = json!("evolved");
        assert!(validate_proposal_state_authority(&source.proposal_state_authority).is_err());
    }

    #[test]
    fn state_accounting_counts_only_new_proposals_and_rejects_cohort_role_tamper() {
        let mut source = synthetic_source(Vec::new(), Vec::new());
        let retained = json!({"candidateId":"parent-1","candidateIdentitySha256":HASH_B,"cohortRole":"retained_parent"});
        source.cohort["candidates"] = json!([
            {"candidateId":"candidate-1","candidateIdentitySha256":HASH_A,"cohortRole":"new_proposal"},
            retained,
        ]);
        source.cohort["newProposalCandidateIds"] = json!(["candidate-1"]);
        source.cohort["retainedParentEvaluationCandidateIds"] = json!(["parent-1"]);
        source
            .cohort
            .as_object_mut()
            .unwrap()
            .remove("cohortSha256");
        add_self_hash(&mut source.cohort, "cohortSha256").unwrap();
        assert_eq!(validate_cohort_partition(&source).unwrap(), 1);

        let mut basis = json!({
            "schemaVersion":"temporal_qd_v5_generation_state_basis_v1","configSha256":HASH_A,"generationIndex":2,
            "completedGenerationsSha256":canonical_sha256(&Value::Array(Vec::new())).unwrap(),
            "uniqueCandidatesEvaluated":10,"workerTasksCompleted":20,"nextImmigrantContinuationOrdinal":0,
            "uniqueIdentityCounts":{},"duplicateCounters":{},"proposalSlotCounters":{},
        });
        add_self_hash(&mut basis, "stateBasisSha256").unwrap();
        source.state_basis = basis;
        let mut record =
            json!({"schemaVersion":"temporal_qd_generation_record_v2","candidateCount":2});
        add_self_hash(&mut record, "generationRecordSha256").unwrap();
        let patch = build_state_patch(&source, &record).unwrap();
        // One selected proposal plus one terminal evaluation rejection still
        // consumes two unique evaluation slots; the retained parent consumes
        // neither.
        assert_eq!(patch["uniqueCandidatesEvaluated"], 12);

        let mut undercounted = record.clone();
        undercounted["candidateCount"] = json!(0);
        undercounted
            .as_object_mut()
            .unwrap()
            .remove("generationRecordSha256");
        add_self_hash(&mut undercounted, "generationRecordSha256").unwrap();
        assert!(build_state_patch(&source, &undercounted).is_err());

        for (proposal, retained, candidates) in [
            (
                json!(["candidate-1", "candidate-1"]),
                json!(["parent-1"]),
                source.cohort["candidates"].clone(),
            ),
            (
                json!(["candidate-1", "parent-1"]),
                json!(["parent-1"]),
                source.cohort["candidates"].clone(),
            ),
            (
                json!(["candidate-1"]),
                json!([]),
                source.cohort["candidates"].clone(),
            ),
        ] {
            let mut tampered = source.clone();
            tampered.cohort["newProposalCandidateIds"] = proposal;
            tampered.cohort["retainedParentEvaluationCandidateIds"] = retained;
            tampered.cohort["candidates"] = candidates;
            assert!(validate_cohort_partition(&tampered).is_err());
        }
    }

    #[cfg(windows)]
    #[test]
    fn v2_output_layout_rejects_symlinked_evidence_directory() {
        use std::os::windows::fs::symlink_dir;

        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();
        symlink_dir(outside.path(), root.path().join("evidence")).unwrap();
        assert!(output_path(root.path(), CUMULATIVE_PATH).is_err());
    }

    #[test]
    fn synthetic_multi_panel_receipt_is_exact_and_tamper_evident() {
        let root = tempdir().unwrap();
        let candidate = json!({
            "candidateId":"candidate-1","candidateIdentitySha256":HASH_A,
            "programSha256":HASH_B,"profileSnapshotSha256":HASH_C,
        });
        let panel_1 = synthetic_bundle(&candidate, "panel-1", "w1");
        let panel_2 = synthetic_bundle(&candidate, "panel-2", "w2");
        let source = synthetic_source(vec![panel_2.clone()], Vec::new());
        let plan = build_auxiliary_plan(&source).unwrap();
        assert_eq!(unsigned(&plan, "obligationCount").unwrap(), 1);
        assert_eq!(plan["obligations"][0]["panelId"], "panel-1");
        assert!(admit_receipts(&source, &plan).is_err());

        let members_bytes = b"{}\n";
        let members_path = root.path().join("evaluated-members.jsonl");
        fs::write(&members_path, members_bytes).unwrap();
        let evaluated = json!({
            "membersFile":{
                "path":"evaluated-members.jsonl",
                "rawSha256":sha256_bytes(members_bytes),
                "sizeBytes":members_bytes.len(),
                "recordCount":1
            }
        });
        let provisional = json!({"candidateCount":1,"candidates":[{"candidateId":"candidate-1"}]});
        let mut seal = json!({"schemaVersion":CAMPAIGN_SEAL_SCHEMA});
        add_self_hash(&mut seal, "campaignSealSha256").unwrap();
        let seal_path = root.path().join("campaign-seal-result.json");
        let seal_bytes = canonical_json_line(&seal).unwrap();
        fs::write(&seal_path, &seal_bytes).unwrap();
        let mut reduction = json!({
            "schemaVersion":TAIL_REDUCTION_SCHEMA,
            "evaluatedMembers":evaluated,
            "provisional":provisional,
        });
        add_self_hash(&mut reduction, "resultSha256").unwrap();
        let reduction_path = root.path().join("tail-reduction-result.json");
        fs::write(&reduction_path, canonical_json_line(&reduction).unwrap()).unwrap();
        let mut transaction = json!({
            "schemaVersion":TAIL_TRANSACTION_SCHEMA,
            "campaignSealSha256":sha(&seal,"campaignSealSha256").unwrap(),
            "tailReductionResultSha256":sha(&reduction,"resultSha256").unwrap(),
            "evaluatedMembers":evaluated,
            "provisional":provisional,
        });
        add_self_hash(&mut transaction, "transactionSha256").unwrap();
        let transaction_path = root.path().join("generation-tail-transaction-result.json");
        let transaction_bytes = canonical_json_line(&transaction).unwrap();
        fs::write(&transaction_path, &transaction_bytes).unwrap();
        let campaign_binding = json!({"role":"cumulative_backfill"});
        let mut bundle_artifact = json!({
            "schemaVersion":AUXILIARY_BUNDLE_ARTIFACT_SCHEMA,
            "panelId":"panel-1","candidateIds":["candidate-1"],
            "campaignBinding":campaign_binding,
            "campaignSealSha256":sha(&seal,"campaignSealSha256").unwrap(),
            "tailTransactionSha256":sha(&transaction,"transactionSha256").unwrap(),
            "tailReductionResultSha256":sha(&reduction,"resultSha256").unwrap(),
            "evaluatedMembersSha256":canonical_sha256(&evaluated).unwrap(),
            "candidatePanelBundles":[panel_1.clone()],
        });
        add_self_hash(&mut bundle_artifact, "bundleArtifactSha256").unwrap();
        let bundle_path = root.path().join("auxiliary-panel-bundles.json");
        let bundle_bytes = canonical_json_line(&bundle_artifact).unwrap();
        fs::write(&bundle_path, &bundle_bytes).unwrap();
        let mut receipt = json!({
            "schemaVersion":RECEIPT_SCHEMA,"role":"cumulative_backfill",
            "panelId":"panel-1","candidateIds":["candidate-1"],
            "campaignBinding":campaign_binding,
            "campaignSeal":{"path":seal_path.to_string_lossy(),"sha256":sha(&seal,"campaignSealSha256").unwrap()},
            "tailTransaction":{"path":transaction_path.to_string_lossy(),"sha256":sha(&transaction,"transactionSha256").unwrap()},
            "bundleArtifact":{"path":bundle_path.to_string_lossy(),"sha256":sha(&bundle_artifact,"bundleArtifactSha256").unwrap()},
        });
        add_self_hash(&mut receipt, "receiptSha256").unwrap();
        let complete = synthetic_source(vec![panel_2.clone()], vec![receipt.clone()]);
        let admitted = admit_receipts(&complete, &plan).unwrap();
        assert_eq!(admitted["candidate-1"].len(), 2);
        assert_eq!(admitted["candidate-1"][0]["panelId"], "panel-1");
        assert_eq!(admitted["candidate-1"][1]["panelId"], "panel-2");

        fs::write(&bundle_path, b"{}\n").unwrap();
        assert!(admit_receipts(&complete, &plan).is_err());
        fs::write(&bundle_path, &bundle_bytes).unwrap();
        fs::write(&members_path, b"tampered\n").unwrap();
        assert!(admit_receipts(&complete, &plan).is_err());
        fs::write(&members_path, members_bytes).unwrap();
        fs::write(&seal_path, b"{}\n").unwrap();
        assert!(admit_receipts(&complete, &plan).is_err());
    }

    #[test]
    fn bundle_identity_and_program_must_match_provisional_survivor() {
        let candidate = json!({
            "candidateId":"candidate-1","candidateIdentitySha256":HASH_A,
            "programSha256":HASH_B,"profileSnapshotSha256":HASH_C,
        });
        let source = synthetic_source(Vec::new(), Vec::new());

        let mut identity_tamper = synthetic_bundle(&candidate, "panel-1", "w1");
        identity_tamper["candidateIdentitySha256"] = json!(HASH_C);
        identity_tamper
            .as_object_mut()
            .unwrap()
            .remove("bundleSha256");
        add_self_hash(&mut identity_tamper, "bundleSha256").unwrap();
        assert!(validate_bundle(&source, &identity_tamper).is_err());

        let mut program_tamper = synthetic_bundle(&candidate, "panel-1", "w1");
        program_tamper["programSha256"] = json!(HASH_C);
        program_tamper
            .as_object_mut()
            .unwrap()
            .remove("bundleSha256");
        add_self_hash(&mut program_tamper, "bundleSha256").unwrap();
        assert!(validate_bundle(&source, &program_tamper).is_err());
    }

    #[test]
    fn funnel_stage_chain_rejects_post_terminal_padding() {
        let candidate = json!({
            "terminalDisposition":"static_reachability_rejected",
            "stages":{
                "proposed":{"outcome":"proposed","reasons":[]},
                "staticallyReachable":{"outcome":"rejected","reasons":["no_path"]},
                "nativeValid":{"outcome":"valid","reasons":[]}
            }
        });
        assert!(validate_stage_chain(&candidate["stages"], &candidate).is_err());
    }
}
