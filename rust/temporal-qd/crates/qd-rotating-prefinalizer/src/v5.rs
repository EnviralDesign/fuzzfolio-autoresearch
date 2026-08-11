//! Frozen native-v5 rotating prefinalizer transaction.
//!
//! This module is intentionally a narrow file transaction: manifests only
//! carry authenticated bindings, while all population-shaped data is reopened
//! from native JSONL receipts.  In particular, no candidate array is accepted
//! from a supervisor manifest.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_json_line, canonical_sha256, canonical_sha256_without_object_field,
};

use crate::{campaign_receipt, core_receipt, funnel_source};

pub const BASE_MANIFEST_SCHEMA: &str = "temporal_qd_v5_rotating_prefinalizer_manifest_v1";
pub const RESUME_MANIFEST_SCHEMA: &str = "temporal_qd_v5_rotating_prefinalizer_resume_manifest_v1";
pub const BASE_MANIFEST_SCHEMA_V2: &str = "temporal_qd_v5_rotating_prefinalizer_manifest_v2";
pub const RESUME_MANIFEST_SCHEMA_V2: &str =
    "temporal_qd_v5_rotating_prefinalizer_resume_manifest_v2";
pub const RESULT_SCHEMA: &str = "temporal_qd_v5_rotating_prefinalizer_result_v1";
pub const TASK_PLAN_SCHEMA: &str = "temporal_qd_v5_rotating_prefinalizer_task_plan_v2";
pub const NON_PROPOSAL_SELECTION_SCHEMA: &str = "temporal_qd_v5_non_proposal_task_selection_v2";
pub const NON_PROPOSAL_SELECTION_RECEIPT_SCHEMA: &str =
    "temporal_qd_v5_non_proposal_task_selection_receipt_v2";
pub const EXECUTION_RECEIPT_SCHEMA: &str =
    "temporal_qd_v5_rotating_prefinalizer_execution_receipt_v2";
pub const EXECUTION_SCHEMA: &str = "temporal_qd_v5_rotating_prefinalizer_execution_v2";
pub const EXECUTION_RECEIPT_PATH: &str = "execution-receipt.json";
const INPUT_MANIFEST_DESCRIPTOR_SCHEMA: &str =
    "temporal_qd_v5_prefinalizer_input_manifest_descriptor_v1";
const INTERNAL_RESULT_DESCRIPTOR_SCHEMA: &str =
    "temporal_qd_v5_prefinalizer_internal_result_descriptor_v1";
const SELECTION_DOCUMENT_DESCRIPTOR_SCHEMA: &str =
    "temporal_qd_v5_prefinalizer_selection_document_descriptor_v1";
const SELECTION_RECEIPT_DESCRIPTOR_SCHEMA: &str =
    "temporal_qd_v5_prefinalizer_selection_receipt_descriptor_v1";
const FINALIZER_SOURCE_DESCRIPTOR_SCHEMA: &str =
    "temporal_qd_v5_prefinalizer_finalizer_source_descriptor_v1";
const FINALIZER_MANIFEST_DESCRIPTOR_SCHEMA: &str =
    "temporal_qd_v5_prefinalizer_finalizer_manifest_descriptor_v1";
const OPERATION: &str = "prepare_native_v5_rotating_generation";
const RESUME_OPERATION: &str = "resume_native_v5_rotating_generation";
const PREVIOUS_PARENT_SUMMARY_SCHEMA: &str = "temporal_qd_previous_parent_archive_summary_v1";

pub fn execute_manifest(path: &Path) -> Result<Value> {
    let path = existing(path, "v5 prefinalizer manifest")?;
    let root = path
        .parent()
        .ok_or_else(|| anyhow!("v5 manifest has no parent"))?;
    let manifest = read_canonical(&path, "v5 prefinalizer manifest")?;
    let result_path = root.join("result.json");
    let receipt_path = root.join(EXECUTION_RECEIPT_PATH);
    if receipt_path.exists() {
        let result = validate_committed_execution(&path, root, &receipt_path)?;
        return Ok(
            json!({"schemaVersion":"temporal_qd_v5_rotating_prefinalizer_execution_v1","restart":true,"result":result}),
        );
    }
    if result_path.exists() {
        let result = read_canonical(&result_path, "v5 prefinalizer result")?;
        let prepared = authenticated_prepare(root, &manifest)?;
        ensure!(
            result == prepared.result,
            "committed result is not rederived from its authenticated manifest chain"
        );
        if let Some((_, source, finalizer)) = &prepared.ready {
            publish_finalizer_outputs(&prepared.finalizer_output_root, source, finalizer)?;
        }
        materialize_non_proposal_task_selections(root, &result)?;
        validate_task_sidecars(root, &result)?;
        publish_execution_receipt(&path, root, &result)?;
        return Ok(
            json!({"schemaVersion":"temporal_qd_v5_rotating_prefinalizer_execution_v1","restart":true,"result":result}),
        );
    }
    let prepared = authenticated_prepare(root, &manifest)?;
    // Sidecars are written before the final result.  All writes are content
    // addressed and idempotent, so a crash at any boundary has one outcome.
    for (descriptor, rows) in &prepared.task_sidecars {
        publish_task_sidecar(root, descriptor, rows)?;
    }
    publish(root.join("task-plan.json"), &prepared.plan)?;
    if let Some((rows, source, finalizer)) = &prepared.ready {
        let desc = publish_jsonl(root.join("selected-rich-members.jsonl"), rows)?;
        ensure!(
            source["selectedRichMembers"]["memberCount"] == desc["recordCount"],
            "selected member write count drifted"
        );
        publish_finalizer_outputs(&prepared.finalizer_output_root, source, finalizer)?;
    }
    publish(result_path, &prepared.result)?;
    // The result is the immutable authority the v2 task-selection document
    // binds.  Selection documents follow it and their receipts are the final
    // writes, so a restart can finish an interrupted handoff without ever
    // accepting a half-written receipt.
    materialize_non_proposal_task_selections(root, &prepared.result)?;
    validate_task_sidecars(root, &prepared.result)?;
    publish_execution_receipt(&path, root, &prepared.result)?;
    Ok(
        json!({"schemaVersion":"temporal_qd_v5_rotating_prefinalizer_execution_v1","restart":false,"result":prepared.result}),
    )
}

/// Current supervisor ABI.  The historical full-result return remains on
/// `execute_manifest` for Rust parity callers; the process entry point uses
/// this bounded wrapper exclusively.
pub fn execute_manifest_compact(path: &Path) -> Result<Value> {
    let execution = execute_manifest(path)?;
    let manifest_path = existing(path, "v5 prefinalizer manifest")?;
    let root = manifest_path
        .parent()
        .ok_or_else(|| anyhow!("v5 manifest has no parent"))?;
    let receipt = read_canonical(
        &root.join(EXECUTION_RECEIPT_PATH),
        "v5 prefinalizer execution receipt",
    )?;
    validate_execution_receipt(&manifest_path, root, &receipt)?;
    Ok(json!({
        "schemaVersion": EXECUTION_SCHEMA,
        "restart": member(&execution, "restart")?,
        "receipt": receipt,
    }))
}

fn publish_execution_receipt(manifest_path: &Path, root: &Path, result: &Value) -> Result<()> {
    let receipt = expected_execution_receipt(manifest_path, root, result)?;
    // This is deliberately the last write in the transaction.  Its presence
    // means every internal result, task handoff, or finalizer handoff named by
    // it has already been reopened and byte-checked.
    publish(root.join(EXECUTION_RECEIPT_PATH), &receipt)
}

fn validate_committed_execution(
    manifest_path: &Path,
    root: &Path,
    receipt_path: &Path,
) -> Result<Value> {
    ensure!(
        receipt_path == root.join(EXECUTION_RECEIPT_PATH),
        "execution receipt path is not fixed"
    );
    ensure!(
        receipt_path.is_file() && !fs::symlink_metadata(receipt_path)?.file_type().is_symlink(),
        "execution receipt is missing or is a symlink"
    );
    let receipt = read_canonical(receipt_path, "v5 prefinalizer execution receipt")?;
    validate_execution_receipt(manifest_path, root, &receipt)
}

fn validate_execution_receipt(manifest_path: &Path, root: &Path, receipt: &Value) -> Result<Value> {
    exact(
        receipt,
        &[
            "schemaVersion",
            "contractVersion",
            "inputManifest",
            "internalResult",
            "status",
            "generationIndex",
            "roundIndex",
            "semanticAuthoritySha256",
            "baseManifestSha256",
            "previousResultSha256",
            "taskPlanSha256",
            "taskCount",
            "taskSelections",
            "finalizerSource",
            "finalizerManifest",
            "receiptSha256",
        ],
        "v5 prefinalizer execution receipt",
    )?;
    ensure!(
        text(receipt, "schemaVersion")? == EXECUTION_RECEIPT_SCHEMA
            && text(receipt, "contractVersion")? == CONTRACT_VERSION,
        "v5 prefinalizer execution receipt schema/version invalid"
    );
    self_hash(
        receipt,
        "receiptSha256",
        "v5 prefinalizer execution receipt",
    )?;

    let manifest = read_exact_json_descriptor(
        member(receipt, "inputManifest")?,
        INPUT_MANIFEST_DESCRIPTOR_SCHEMA,
        manifest_path,
        "manifestSha256",
        "input manifest",
    )?;
    ensure!(
        hash_of(&manifest, "manifestSha256")?
            == hash_of(member(receipt, "inputManifest")?, "manifestSha256")?,
        "input manifest semantic hash drifted"
    );

    let result_path = root.join("result.json");
    let result = read_exact_json_descriptor(
        member(receipt, "internalResult")?,
        INTERNAL_RESULT_DESCRIPTOR_SCHEMA,
        &result_path,
        "resultSha256",
        "internal result",
    )?;
    validate_result(&result)?;

    validate_result_outputs(root, &result)?;
    let expected = expected_execution_receipt(manifest_path, root, &result)?;
    ensure!(
        receipt == &expected,
        "committed execution receipt/output closure drifted"
    );
    Ok(result)
}

fn expected_execution_receipt(manifest_path: &Path, root: &Path, result: &Value) -> Result<Value> {
    validate_result(result)?;
    validate_result_outputs(root, result)?;
    let manifest = read_canonical(manifest_path, "v5 prefinalizer manifest")?;
    self_hash(&manifest, "manifestSha256", "v5 prefinalizer manifest")?;
    ensure!(
        hash_of(&manifest, "manifestSha256")? == hash_of(result, "manifestSha256")?,
        "internal result does not bind the executed manifest"
    );
    let plan = member(result, "taskPlan")?;
    let mut tasks = array(plan, "tasks")?.iter().collect::<Vec<_>>();
    tasks.sort_by_key(|task| unsigned(task, "taskOrdinal").unwrap_or(u64::MAX));
    for (expected, task) in tasks.iter().enumerate() {
        ensure!(
            unsigned(task, "taskOrdinal")? == expected as u64,
            "task ordinals are not deterministic and contiguous"
        );
    }
    let task_selections = tasks
        .into_iter()
        .map(|task| task_selection_execution_descriptor(root, task))
        .collect::<Result<Vec<_>>>()?;

    let (finalizer_source, finalizer_manifest) = if text(result, "status")? == "ready_for_finalizer"
    {
        let source = member(result, "finalizerSource")?;
        let manifest = member(result, "finalizerManifest")?;
        (
            exact_json_descriptor(
                FINALIZER_SOURCE_DESCRIPTOR_SCHEMA,
                Path::new(text(source, "path")?),
                "sourceSha256",
                hash_of(source, "sha256")?,
                "finalizer source",
            )?,
            exact_json_descriptor(
                FINALIZER_MANIFEST_DESCRIPTOR_SCHEMA,
                Path::new(text(manifest, "path")?),
                "manifestSha256",
                hash_of(manifest, "sha256")?,
                "finalizer manifest",
            )?,
        )
    } else {
        ensure!(
            member(result, "finalizerSource")?.is_null()
                && member(result, "finalizerManifest")?.is_null(),
            "awaiting result names finalizer outputs"
        );
        (Value::Null, Value::Null)
    };

    let mut receipt = json!({
        "schemaVersion":EXECUTION_RECEIPT_SCHEMA,
        "contractVersion":CONTRACT_VERSION,
        "inputManifest":exact_json_descriptor(
            INPUT_MANIFEST_DESCRIPTOR_SCHEMA,
            manifest_path,
            "manifestSha256",
            hash_of(&manifest,"manifestSha256")?,
            "input manifest",
        )?,
        "internalResult":exact_json_descriptor(
            INTERNAL_RESULT_DESCRIPTOR_SCHEMA,
            &root.join("result.json"),
            "resultSha256",
            hash_of(result,"resultSha256")?,
            "internal result",
        )?,
        "status":text(result,"status")?,
        "generationIndex":unsigned(result,"generationIndex")?,
        "roundIndex":unsigned(result,"roundIndex")?,
        "semanticAuthoritySha256":hash_of(result,"semanticAuthoritySha256")?,
        "baseManifestSha256":hash_of(result,"baseManifestSha256")?,
        "previousResultSha256":member(result,"previousResultSha256")?,
        "taskPlanSha256":hash_of(plan,"taskPlanSha256")?,
        "taskCount":task_selections.len(),
        "taskSelections":task_selections,
        "finalizerSource":finalizer_source,
        "finalizerManifest":finalizer_manifest,
    });
    add(&mut receipt, "receiptSha256")?;
    Ok(receipt)
}

fn task_selection_execution_descriptor(root: &Path, task: &Value) -> Result<Value> {
    let document_path = safe_output_path(root, text(task, "selectionDocumentRelativePath")?)?;
    let receipt_path = safe_output_path(root, text(task, "selectionReceiptRelativePath")?)?;
    let document = read_canonical(&document_path, "v2 task selection document")?;
    let selection_receipt = read_canonical(&receipt_path, "v2 task selection receipt")?;
    let rows = member(member(task, "cohortSelection")?, "candidateRows")?;
    Ok(json!({
        "taskOrdinal":unsigned(task,"taskOrdinal")?,
        "campaignRole":text(task,"campaignRole")?,
        "panelId":text(task,"panelId")?,
        "candidateCount":unsigned(rows,"recordCount")?,
        "candidateSetSha256":hash_of(task,"candidateSetSha256")?,
        "selectionDocument":exact_json_descriptor(
            SELECTION_DOCUMENT_DESCRIPTOR_SCHEMA,
            &document_path,
            "selectionDocumentSha256",
            hash_of(&document,"selectionDocumentSha256")?,
            "selection document",
        )?,
        "selectionReceipt":exact_json_descriptor(
            SELECTION_RECEIPT_DESCRIPTOR_SCHEMA,
            &receipt_path,
            "receiptSha256",
            hash_of(&selection_receipt,"receiptSha256")?,
            "selection receipt",
        )?,
    }))
}

fn exact_json_descriptor(
    schema: &str,
    path: &Path,
    semantic_field: &str,
    semantic_sha: &str,
    name: &str,
) -> Result<Value> {
    ensure!(path.is_absolute(), "{name} path must be absolute");
    ensure!(
        !path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir)),
        "{name} path contains an alias"
    );
    ensure!(path.is_file(), "{name} is not a file");
    ensure!(
        !fs::symlink_metadata(path)?.file_type().is_symlink(),
        "{name} symlink is forbidden"
    );
    let canonical = fs::canonicalize(path)?;
    ensure!(canonical == path, "{name} path is not canonical");
    let raw = fs::read(path)?;
    let value = read_canonical(path, name)?;
    self_hash(&value, semantic_field, name)?;
    ensure!(
        hash_of(&value, semantic_field)? == semantic_sha,
        "{name} semantic hash drifted"
    );
    let mut descriptor = json!({
        "schemaVersion":schema,
        "path":path,
        "rawSha256":format!("sha256:{:x}",Sha256::digest(&raw)),
        "sizeBytes":raw.len(),
    });
    descriptor
        .as_object_mut()
        .expect("descriptor object")
        .insert(semantic_field.to_owned(), json!(semantic_sha));
    Ok(descriptor)
}

fn read_exact_json_descriptor(
    descriptor: &Value,
    schema: &str,
    expected_path: &Path,
    semantic_field: &str,
    name: &str,
) -> Result<Value> {
    exact(
        descriptor,
        &[
            "schemaVersion",
            "path",
            "rawSha256",
            "sizeBytes",
            semantic_field,
        ],
        &format!("{name} descriptor"),
    )?;
    ensure!(
        text(descriptor, "schemaVersion")? == schema,
        "{name} descriptor schema invalid"
    );
    let path = Path::new(text(descriptor, "path")?);
    ensure!(path == expected_path, "{name} descriptor path drifted");
    let expected = exact_json_descriptor(
        schema,
        path,
        semantic_field,
        hash_of(descriptor, semantic_field)?,
        name,
    )?;
    ensure!(
        descriptor == &expected,
        "{name} descriptor file identity drifted"
    );
    read_canonical(path, name)
}

fn validate_result_outputs(root: &Path, result: &Value) -> Result<()> {
    let task_plan_path = safe_output_path(root, "task-plan.json")?;
    ensure!(task_plan_path.is_file(), "v5 task plan is missing");
    ensure!(
        !fs::symlink_metadata(&task_plan_path)?
            .file_type()
            .is_symlink(),
        "v5 task plan symlink is forbidden"
    );
    ensure!(
        read_canonical(&task_plan_path, "v5 task plan")? == *member(result, "taskPlan")?,
        "v5 task plan/result drifted"
    );
    validate_task_sidecars(root, result)?;
    if text(result, "status")? == "ready_for_finalizer" {
        let selected = member(result, "selectedRichMembers")?;
        let selected_rows = array(selected, "members")?;
        let selected_path = safe_output_path(root, "selected-rich-members.jsonl")?;
        ensure!(
            selected_path.is_file(),
            "selected rich member sidecar is missing"
        );
        ensure!(
            !fs::symlink_metadata(&selected_path)?
                .file_type()
                .is_symlink(),
            "selected rich member sidecar symlink is forbidden"
        );
        let expected = selected_rows
            .iter()
            .map(canonical_json_line)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .concat();
        ensure!(
            fs::read(&selected_path)? == expected,
            "selected rich member sidecar/result drifted"
        );
    }
    Ok(())
}

fn authenticated_prepare(root: &Path, manifest: &Value) -> Result<Prepared> {
    if matches!(
        text(manifest, "schemaVersion")?,
        BASE_MANIFEST_SCHEMA | BASE_MANIFEST_SCHEMA_V2
    ) {
        let base = validate_base(manifest, root).context("validate v5 base manifest")?;
        prepare(root, manifest, &base, Vec::new(), 0, None).context("prepare v5 base transaction")
    } else {
        let (base, receipts, round, previous) = validate_resume(manifest, root)?;
        prepare(root, manifest, &base, receipts, round, Some(previous))
    }
}

struct Base {
    value: Value,
    generation: u64,
    semantic: String,
    runtime: String,
    rotating: Value,
    state: Value,
    proposal_receipt: Value,
    prior_parent: Value,
    parent_members: Vec<Value>,
    prior_cumulative: Value,
    archive_policy: Value,
    funnel: Value,
    completed_generation_records: Value,
    proposal_state_authority: Value,
    finalizer_output_root: PathBuf,
}
struct Prepared {
    plan: Value,
    result: Value,
    ready: Option<(Vec<Value>, Value, Value)>,
    task_sidecars: Vec<(Value, Vec<Value>)>,
    finalizer_output_root: PathBuf,
}

fn validate_base(v: &Value, root: &Path) -> Result<Base> {
    exact(
        v,
        &[
            "schemaVersion",
            "contractVersion",
            "operation",
            "generationIndex",
            "supervisorConfigBinding",
            "stateBasis",
            "completedGenerationRecords",
            "proposalStateAuthority",
            "proposalConstructionBinding",
            "previousParentArchiveBinding",
            "previousCumulativeArchiveBinding",
            "proposalCampaignReceiptBinding",
            "finalizerOutputRoot",
            "runtimeAuthoritySha256",
            "semanticAuthoritySha256",
            "manifestSha256",
        ],
        "v5 base manifest",
    )?;
    let schema = text(v, "schemaVersion")?;
    ensure!(
        matches!(schema, BASE_MANIFEST_SCHEMA | BASE_MANIFEST_SCHEMA_V2)
            && text(v, "contractVersion")? == CONTRACT_VERSION
            && text(v, "operation")? == OPERATION
            && unsigned(v, "generationIndex")? > 0,
        "v5 base manifest schema/version is invalid"
    );
    self_hash(v, "manifestSha256", "v5 base manifest")?;
    let generation = unsigned(v, "generationIndex")?;
    let finalizer_output_root = sealed_finalizer_output_root(text(v, "finalizerOutputRoot")?)?;
    let state = member(v, "stateBasis")?.clone();
    validate_state(&state, generation)?;
    let completed_generation_records = member(v, "completedGenerationRecords")?.clone();
    validate_completed_generation_records(&completed_generation_records, &state)?;
    reject_rich_lists(v)?;
    let proposal_receipt = reopen_receipt(member(v, "proposalCampaignReceiptBinding")?, root)?;
    ensure!(
        text(&proposal_receipt, "campaignRole")? == "proposal_current_panel"
            && unsigned(&proposal_receipt, "generationIndex")? == generation,
        "proposal campaign receipt role/generation drifted"
    );
    let construction = member(v, "proposalConstructionBinding")?;
    let rotating = find_schema(
        member(v, "supervisorConfigBinding")?,
        "temporal_qd_rotating_evidence_v1",
    )
    .or_else(|| find_schema(construction, "temporal_qd_rotating_evidence_v1"))
    .ok_or_else(|| anyhow!("base manifest lacks frozen rotating evidence authority"))?;
    self_hash(&rotating, "rotatingEvidenceSha256", "rotating evidence")?;
    let policy = find_schema(
        member(v, "supervisorConfigBinding")?,
        "temporal_qd_archive_policy_binding_v1",
    )
    .or_else(|| find_schema(construction, "temporal_qd_archive_policy_binding_v1"))
    .ok_or_else(|| anyhow!("base manifest lacks archivePolicyAuthority"))?;
    self_hash(&policy, "policyBindingSha256", "archive policy authority")?;
    let parent_binding = member(v, "previousParentArchiveBinding")?;
    let parent_input = reopen_value(parent_binding, root)?;
    if let Some(parent) = &parent_input {
        ensure!(
            hash_of(parent_binding, "archiveSha256")? == hash_of(parent, "archiveSha256")?,
            "previous parent archive descriptor/archive identity drifted"
        );
        self_hash(parent, "archiveSha256", "previous parent archive")?;
    }
    let parent_members = parent_input
        .as_ref()
        .map(parent_member_rows)
        .transpose()?
        .unwrap_or_default();
    let parent = derive_parent_summary(parent_input)?;
    let cumulative_binding = member(v, "previousCumulativeArchiveBinding")?;
    let cumulative = reopen_value(cumulative_binding, root)?.unwrap_or(Value::Null);
    if !cumulative.is_null() {
        ensure!(
            hash_of(cumulative_binding, "archiveSha256")? == hash_of(&cumulative, "archiveSha256")?,
            "previous cumulative archive descriptor/archive identity drifted"
        );
        self_hash(&cumulative, "archiveSha256", "previous cumulative archive")?;
    }
    // The assembled source is a projection, never an authority by itself.
    // Bind both the sealed v2 input and its exact deterministic assembly so a
    // self-rehashed injected source cannot change the proposal funnel.
    let proposal_state_authority = member(v, "proposalStateAuthority")?.clone();
    validate_proposal_state_authority(&proposal_state_authority, construction)?;
    let funnel_input = member(construction, "funnelReductionInput")?;
    ensure!(
        text(funnel_input, "schemaVersion")? == funnel_source::INPUT_SCHEMA,
        "proposal construction funnel input schema drifted"
    );
    let funnel = if schema == BASE_MANIFEST_SCHEMA_V2 {
        let binding = member(construction, "funnelAssemblyReceiptBinding")?;
        ensure!(
            construction.get("funnelReductionSource").is_none(),
            "v2 proposal construction forbids inline funnel reduction source"
        );
        funnel_source::reopen_assembly_receipt(funnel_input, binding, root)?
    } else {
        ensure!(
            construction.get("funnelAssemblyReceiptBinding").is_none(),
            "historical v1 proposal construction cannot carry a v2 funnel assembly receipt"
        );
        let funnel = member(construction, "funnelReductionSource")?.clone();
        let rebuilt_funnel = funnel_source::assemble(funnel_input)?;
        ensure!(
            rebuilt_funnel == funnel,
            "proposal construction funnel source differs from sealed input assembly"
        );
        funnel
    };
    validate_funnel_attempt_authority(funnel_input, &proposal_state_authority, construction)?;
    let semantic_projection = json!({"schemaVersion":"temporal_qd_v5_rotating_prefinalizer_semantic_authority_v1","generationIndex":generation,"supervisorConfigSha256":binding_hash(member(v,"supervisorConfigBinding")?, &["supervisorConfigSha256","configSha256"] )?,"generationConfigSha256":binding_hash(member(v,"supervisorConfigBinding")?, &["generationConfigSha256"] )?,"stateBasisSha256":hash_of(&state,"stateBasisSha256")?,"completedGenerationRecordsSha256":canonical_sha256(&completed_generation_records)?,"proposalStateAuthority":proposal_state_authority,"proposalSemanticRoots":semantic_roots(member(v,"proposalConstructionBinding")?)?,"identityLedgerSha256":binding_hash(member(v,"proposalConstructionBinding")?, &["identityLedgerSha256"] )?,"previousParentArchiveSha256":binding_identity(member(v,"previousParentArchiveBinding")?)?,"previousCumulativeArchiveSha256":binding_identity(member(v,"previousCumulativeArchiveBinding")?)?,"proposalCampaignSemanticReceiptSha256":hash_of(&proposal_receipt,"semanticReceiptSha256")?});
    ensure!(
        canonical_sha256(&semantic_projection)? == hash_of(v, "semanticAuthoritySha256")?,
        "v5 semantic authority hash drifted"
    );
    Ok(Base {
        value: v.clone(),
        generation,
        semantic: hash_of(v, "semanticAuthoritySha256")?.to_owned(),
        runtime: hash_of(v, "runtimeAuthoritySha256")?.to_owned(),
        rotating,
        state,
        proposal_receipt,
        prior_parent: parent,
        parent_members,
        prior_cumulative: cumulative,
        archive_policy: policy,
        funnel,
        completed_generation_records,
        proposal_state_authority,
        finalizer_output_root,
    })
}

fn validate_funnel_attempt_authority(
    input: &Value,
    state: &Value,
    construction: &Value,
) -> Result<()> {
    let authority = member(input, "proposalAttemptAuthority")?;
    let (_, _, g0, receipt) = core_receipt::load_proposal_attempt_authority(authority)?;
    let invocation = member(construction, "nativeV5Invocation")?;
    ensure!(
        hash_of(&receipt, "proposalResultSha256")?
            == hash_of(member(invocation, "proposalResult")?, "semanticSha256")?
            && hash_of(&receipt, "proposalReceiptSha256")?
                == hash_of(invocation, "proposalReceiptSha256")?
            && hash_of(&receipt, "proposalReceiptSha256")?
                == hash_of(state, "proposalReceiptSha256")?
            && hash_of(&receipt, "outputInventorySha256")?
                == hash_of(invocation, "outputInventorySha256")?,
        "funnel attempt receipt differs from proposal invocation authority"
    );
    ensure!(
        (g0 && text(state, "generationKind")? == "g0")
            || (!g0 && text(state, "generationKind")? == "evolved"),
        "funnel attempt receipt kind differs from proposal state authority"
    );
    if g0 {
        ensure!(
            hash_of(&receipt, "proposalManifestSha256")?
                == hash_of(member(invocation, "proposalManifest")?, "semanticSha256")?
                && hash_of(&receipt, "proposalManifestSha256")?
                    == hash_of(state, "proposalManifestSha256")?
                && unsigned(&receipt, "generationIndex")? == unsigned(input, "generationIndex")?,
            "G0 funnel attempt receipt differs from proposal manifest authority"
        );
    }
    Ok(())
}

fn validate_resume(v: &Value, root: &Path) -> Result<(Base, Vec<Value>, u64, Value)> {
    exact(
        v,
        &[
            "schemaVersion",
            "contractVersion",
            "operation",
            "baseManifestBinding",
            "roundIndex",
            "previousResultBinding",
            "newCampaignReceiptBindings",
            "runtimeAuthoritySha256",
            "manifestSha256",
        ],
        "v5 resume manifest",
    )?;
    let schema = text(v, "schemaVersion")?;
    ensure!(
        matches!(schema, RESUME_MANIFEST_SCHEMA | RESUME_MANIFEST_SCHEMA_V2)
            && text(v, "contractVersion")? == CONTRACT_VERSION
            && text(v, "operation")? == RESUME_OPERATION,
        "v5 resume manifest schema/version is invalid"
    );
    self_hash(v, "manifestSha256", "v5 resume manifest")?;
    let base_value = reopen_required(member(v, "baseManifestBinding")?, root, "base manifest")?;
    ensure!(
        (schema == RESUME_MANIFEST_SCHEMA
            && text(&base_value, "schemaVersion")? == BASE_MANIFEST_SCHEMA)
            || (schema == RESUME_MANIFEST_SCHEMA_V2
                && text(&base_value, "schemaVersion")? == BASE_MANIFEST_SCHEMA_V2),
        "resume/base manifest version drifted"
    );
    let base = validate_base(&base_value, root)?;
    ensure!(
        hash_of(v, "runtimeAuthoritySha256")? == base.runtime,
        "resume runtime authority drifted"
    );
    let previous_binding = member(v, "previousResultBinding")?;
    let previous_path = safe_path(root, text(previous_binding, "path")?)?;
    let previous = reopen_required(previous_binding, root, "previous result")?;
    validate_result(&previous)?;
    ensure!(
        hash_of(&previous, "baseManifestSha256")? == hash_of(&base.value, "manifestSha256")?
            && unsigned(v, "roundIndex")? == unsigned(&previous, "roundIndex")? + 1,
        "resume chain is discontinuous"
    );
    let previous_root = previous_path
        .parent()
        .ok_or_else(|| anyhow!("previous result binding has no parent"))?;
    let mut receipts = receipts_from_result(&previous)?;
    for receipt in &receipts {
        campaign_receipt::validate_receipt(receipt)?;
        // Reopen its bound streams now; a self-rehashed prior result cannot
        // introduce a receipt whose authenticated member/bundle bytes differ.
        let _ = receipt_rows(receipt, "evaluatedMembers", root)?;
        let _ = receipt_rows(receipt, "candidatePanelBundles", root)?;
    }
    let new_bindings = array(v, "newCampaignReceiptBindings")?;
    let mut new_receipts = Vec::new();
    for binding in new_bindings {
        let r = reopen_receipt(binding, root)?;
        ensure!(
            unsigned(&r, "generationIndex")? == base.generation,
            "resume campaign generation drifted"
        );
        new_receipts.push(r);
    }
    validate_resume_task_completions(
        &previous,
        previous_root,
        &new_receipts,
        root,
        base.generation,
    )?;
    receipts.extend(new_receipts);
    ensure_unique_receipts(&receipts)?;
    Ok((base, receipts, unsigned(v, "roundIndex")?, previous))
}

fn prepare(
    root: &Path,
    manifest: &Value,
    base: &Base,
    mut receipts: Vec<Value>,
    round: u64,
    previous: Option<Value>,
) -> Result<Prepared> {
    if receipts.is_empty() {
        receipts.push(base.proposal_receipt.clone());
    }
    ensure_unique_receipts(&receipts)?;
    let panels = array(&base.rotating, "panels")?;
    let cycle = unsigned(
        member(&base.rotating, "absoluteGenerationMapping")?,
        "cycleLength",
    )? as usize;
    ensure!(
        cycle > 0 && cycle == panels.len(),
        "rotating panel authority is invalid"
    );
    let current = text(&panels[(base.generation as usize - 1) % cycle], "panelId")?.to_owned();
    let mut required = Vec::new();
    for i in 0..base.generation as usize {
        let p = text(&panels[i % cycle], "panelId")?.to_owned();
        if !required.contains(&p) {
            required.push(p)
        }
    }
    let mut members = BTreeMap::new();
    let mut member_records = BTreeMap::new();
    // Current-panel campaigns own candidate selection. Prior-panel backfills
    // only add historical evidence for an already selected candidate; their
    // aggregate is panel-local and must never replace currentPanelRank or the
    // selected rich-member record.
    for r in &receipts {
        let role = text(r, "campaignRole")?;
        if role == "prior_panel_backfill" {
            continue;
        }
        ensure!(
            matches!(
                role,
                "proposal_current_panel" | "retained_parent_current_panel"
            ),
            "current-panel member receipt has an unsupported campaign role"
        );
        for raw in receipt_rows(r, "evaluatedMembers", root)? {
            let row = candidate_with_member_selection_fields(&raw)?;
            let id = text(&row, "candidateId")?.to_owned();
            let ident = hash_of(&row, "candidateIdentitySha256")?.to_owned();
            if let Some(old) = members.insert(id.clone(), row.clone()) {
                ensure!(
                    hash_of(&old, "candidateIdentitySha256")? == ident,
                    "conflicting duplicate candidate identity"
                );
            }
            if let Some(old) = member_records.insert(id.clone(), raw.clone()) {
                let old_candidate = old.get("candidate").unwrap_or(&old);
                ensure!(
                    hash_of(old_candidate, "candidateIdentitySha256")? == ident,
                    "conflicting duplicate evaluated-member record identity"
                );
            }
        }
    }
    for r in &receipts {
        if text(r, "campaignRole")? != "prior_panel_backfill" {
            continue;
        }
        for raw in receipt_rows(r, "evaluatedMembers", root)? {
            let row = backfill_candidate_with_current_panel_selection_fields(&raw)?;
            let id = text(&row, "candidateId")?;
            let selected = members.get(id).ok_or_else(|| {
                anyhow!("prior-panel backfill candidate was not current-panel selected")
            })?;
            validate_backfill_candidate_projection(selected, &row)?;
        }
    }
    let (proposal_member_ids, retained_parent_member_ids) =
        admitted_member_role_ids(&receipts, root)?;
    // Retained parents are not smuggled into the proposal population.  A
    // proposal only supersedes one when its authenticated identity agrees;
    // every other parent must first receive the current panel.
    let mut pending_parents = Vec::new();
    if !base.parent_members.is_empty() {
        for raw in &base.parent_members {
            let row = raw.get("candidate").unwrap_or(raw).clone();
            let id = text(&row, "candidateId")?.to_owned();
            let identity = hash_of(&row, "candidateIdentitySha256")?;
            match members.get(&id) {
                Some(proposal) => ensure!(
                    hash_of(proposal, "candidateIdentitySha256")? == identity,
                    "parent/proposal identity conflict"
                ),
                // Completion is per parent, never a receipt-wide boolean: a
                // partial retained campaign must leave every omitted parent
                // outstanding.
                None if !retained_parent_member_ids.contains(&id) => pending_parents.push(row),
                None => {}
            }
        }
    }
    if !pending_parents.is_empty() {
        let cohort = make_cohort(
            base.generation,
            &current,
            hash_of(&base.rotating, "rotatingEvidenceSha256")?,
            members.values().collect(),
            &proposal_member_ids,
            &retained_parent_member_ids,
        )?;
        let provisional = make_provisional(base.generation, &current, &cohort, &[], 0)?;
        let (pending_task, pending_descriptor) = task(
            0,
            "retained_parent_current_panel",
            &current,
            &pending_parents,
            base.semantic.as_str(),
            round,
            hash_of(&base.rotating, "rotatingEvidenceSha256")?,
            retained_parent_source_authority(base, &pending_parents)?,
        )?;
        let sidecars = vec![(pending_descriptor, pending_parents.clone())];
        let plan = make_plan(
            base.semantic.as_str(),
            base.generation,
            round,
            "retained_parent_current_panel",
            vec![pending_task],
        )?;
        let ledger = make_ledger(
            base.generation,
            hash_of(&base.rotating, "rotatingEvidenceSha256")?,
            &cohort,
            &provisional,
            &receipts,
        )?;
        let coverage = make_coverage(
            base.generation,
            hash_of(&base.rotating, "rotatingEvidenceSha256")?,
            &cohort,
            &provisional,
            &required,
            &BTreeMap::new(),
        )?;
        let mut result = json!({"schemaVersion":RESULT_SCHEMA,"contractVersion":CONTRACT_VERSION,"baseManifestSha256":hash_of(&base.value,"manifestSha256")?,"manifestSha256":hash_of(manifest,"manifestSha256")?,"semanticAuthoritySha256":base.semantic,"roundIndex":round,"previousResultSha256":previous.as_ref().map(|x|hash_of(x,"resultSha256").map(str::to_owned)).transpose()?,"generationIndex":base.generation,"status":"awaiting_retained_parent_current_panel","admittedCampaignLedger":ledger,"cohort":cohort,"provisional":provisional,"panelCoverage":coverage,"taskPlan":plan,"funnelReductionSource":base.funnel,"selectedRichMembers":Value::Null,"finalizerSource":Value::Null,"finalizerManifest":Value::Null});
        add(&mut result, "resultSha256")?;
        return Ok(Prepared {
            plan: result["taskPlan"].clone(),
            result,
            ready: None,
            task_sidecars: sidecars,
            finalizer_output_root: base.finalizer_output_root.clone(),
        });
    }
    let cohort = make_cohort(
        base.generation,
        &current,
        hash_of(&base.rotating, "rotatingEvidenceSha256")?,
        members.values().collect(),
        &proposal_member_ids,
        &retained_parent_member_ids,
    )?;
    let max = member(&base.rotating, "provisionalReduction")
        .ok()
        .and_then(|x| unsigned(x, "maxCandidates").ok())
        .unwrap_or(members.len() as u64) as usize;
    let provisional = make_provisional(
        base.generation,
        &current,
        &cohort,
        &members.values().cloned().collect::<Vec<_>>(),
        max,
    )?;
    let selected = array(&provisional, "candidates")?
        .iter()
        .map(|row| {
            members
                .get(text(row, "candidateId")?)
                .cloned()
                .ok_or_else(|| {
                    anyhow!("provisional candidate is absent from authenticated members")
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let bundles = all_bundles(base, &receipts, root)?;
    let missing: Vec<String> = required
        .iter()
        .filter(|p| {
            selected.iter().any(|m| {
                !bundles.contains_key(&(
                    text(m, "candidateId").unwrap_or("?").to_owned(),
                    (*p).clone(),
                ))
            })
        })
        .cloned()
        .collect();
    ensure!(
        !missing.iter().any(|panel| panel == &current),
        "selected candidate lacks authoritative current-panel bundle; it must not be relabelled as prior-panel backfill"
    );
    let (status, tasks, sidecars) = if selected.is_empty() || missing.is_empty() {
        ("ready_for_finalizer", Vec::new(), Vec::new())
    } else {
        let mut tasks = Vec::new();
        let mut sidecars = Vec::new();
        for (i, panel) in missing.iter().enumerate() {
            let panel_missing = selected
                .iter()
                .filter(|candidate| {
                    !bundles.contains_key(&(
                        text(candidate, "candidateId").unwrap_or("?").to_owned(),
                        panel.clone(),
                    ))
                })
                .cloned()
                .collect::<Vec<_>>();
            ensure!(
                !panel_missing.is_empty(),
                "prior-panel backfill task has no missing candidates"
            );
            let (task, descriptor) = task(
                i as u64,
                "prior_panel_backfill",
                panel,
                &panel_missing,
                base.semantic.as_str(),
                round,
                hash_of(&base.rotating, "rotatingEvidenceSha256")?,
                prior_panel_backfill_source_authority(
                    &cohort,
                    &provisional,
                    &ledger_for_task_authority(
                        base.generation,
                        hash_of(&base.rotating, "rotatingEvidenceSha256")?,
                        &cohort,
                        &provisional,
                        &receipts,
                    )?,
                    previous.as_ref(),
                )?,
            )?;
            tasks.push(task);
            sidecars.push((descriptor, panel_missing));
        }
        ("awaiting_prior_panel_backfill", tasks, sidecars)
    };
    let plan = make_plan(
        base.semantic.as_str(),
        base.generation,
        round,
        if status == "ready_for_finalizer" {
            "complete"
        } else {
            "prior_panel_backfill"
        },
        tasks,
    )?;
    let ledger = make_ledger(
        base.generation,
        hash_of(&base.rotating, "rotatingEvidenceSha256")?,
        &cohort,
        &provisional,
        &receipts,
    )?;
    let coverage = make_coverage(
        base.generation,
        hash_of(&base.rotating, "rotatingEvidenceSha256")?,
        &cohort,
        &provisional,
        &required,
        &bundles,
    )?;
    let rich = make_rich(
        base.generation,
        &cohort,
        &provisional,
        &selected,
        &member_records,
    )?;
    let ready = if status == "ready_for_finalizer" {
        let ordered_bundles = bundles.into_values().collect::<Vec<_>>();
        let rich_rows = array(&rich, "members")?.clone();
        let mut source = json!({"schemaVersion":"temporal_qd_generation_finalization_source_v2","contractVersion":CONTRACT_VERSION,"generationIndex":base.generation,"semanticAuthoritySha256":base.semantic,"runtimeAuthoritySha256":base.runtime,"stateBasis":base.state,"completedGenerationRecords":base.completed_generation_records,"proposalStateAuthority":base.proposal_state_authority,"rotatingEvidence":base.rotating,"cohort":cohort,"provisional":provisional,"panelCoverage":coverage,"selectedRichMembers":rich,"baselineCandidatePanelBundles":ordered_bundles,"previousCumulativeArchive":base.prior_cumulative,"previousParentArchiveSummary":base.prior_parent,"archivePolicy":base.archive_policy,"admittedCampaignLedger":ledger,"funnelReductionSource":base.funnel});
        add(&mut source, "sourceSha256")?;
        let froot = &base.finalizer_output_root;
        let mut fm = json!({"schemaVersion":"temporal_qd_generation_finalization_manifest_v2","contractVersion":CONTRACT_VERSION,"operation":"finalize_rotating_generation","runtimeAuthoritySha256":base.runtime,"semanticAuthoritySha256":base.semantic,"sourcePath":froot.join("source.json"),"sourceSha256":hash_of(&source,"sourceSha256")?,"resultPath":"generation-commit.json"});
        add(&mut fm, "manifestSha256")?;
        Some((rich_rows, source, fm))
    } else {
        None
    };
    let source_path = base.finalizer_output_root.join("source.json");
    let manifest_path = base.finalizer_output_root.join("manifest.json");
    let mut result = json!({"schemaVersion":RESULT_SCHEMA,"contractVersion":CONTRACT_VERSION,"baseManifestSha256":hash_of(&base.value,"manifestSha256")?,"manifestSha256":hash_of(manifest,"manifestSha256")?,"semanticAuthoritySha256":base.semantic,"roundIndex":round,"previousResultSha256":previous.as_ref().map(|x|hash_of(x,"resultSha256").map(str::to_owned)).transpose()?,"generationIndex":base.generation,"status":status,"admittedCampaignLedger":ledger,"cohort":cohort,"provisional":provisional,"panelCoverage":coverage,"taskPlan":plan,"funnelReductionSource":base.funnel,"selectedRichMembers":ready.as_ref().map(|_|rich.clone()),"finalizerSource":ready.as_ref().map(|x|json!({"path":source_path,"sha256":hash_of(&x.1,"sourceSha256").unwrap()})),"finalizerManifest":ready.as_ref().map(|x|json!({"path":manifest_path,"sha256":hash_of(&x.2,"manifestSha256").unwrap()}))});
    add(&mut result, "resultSha256")?;
    validate_result(&result)?;
    Ok(Prepared {
        plan,
        result,
        ready,
        task_sidecars: sidecars,
        finalizer_output_root: base.finalizer_output_root.clone(),
    })
}

fn make_cohort(
    g: u64,
    p: &str,
    rot: &str,
    rows: Vec<&Value>,
    proposal_ids: &BTreeSet<String>,
    retained_parent_ids: &BTreeSet<String>,
) -> Result<Value> {
    let mut cs = Vec::new();
    let mut new_proposal_ids = Vec::new();
    let mut retained_ids = Vec::new();
    for r in rows {
        let id = text(r, "candidateId")?;
        // A proposal receipt explicitly supersedes a matching retained-parent
        // receipt; that precedence is established by campaign role, never by
        // a heuristic over candidate IDs.
        let role = if proposal_ids.contains(id) {
            new_proposal_ids.push(id.to_owned());
            "new_proposal"
        } else if retained_parent_ids.contains(id) {
            retained_ids.push(id.to_owned());
            "retained_parent_current_panel"
        } else {
            return Err(anyhow!("cohort member lacks an admitted campaign role"));
        };
        cs.push(json!({"candidateId":id,"candidateIdentitySha256":hash_of(r,"candidateIdentitySha256")?,"cohortRole":role}));
    }
    let mut v = json!({"schemaVersion":"temporal_qd_current_panel_evaluation_cohort_v1","rotatingEvidenceSha256":rot,"generationIndex":g,"panelId":p,"candidates":cs,"newProposalCandidateIds":new_proposal_ids,"retainedParentEvaluationCandidateIds":retained_ids,"parentReevaluationIsProposal":false});
    add(&mut v, "cohortSha256")?;
    Ok(v)
}
fn make_provisional(g: u64, p: &str, c: &Value, rows: &[Value], limit: usize) -> Result<Value> {
    let mut counts = BTreeMap::<String, usize>::new();
    let mut groups = BTreeMap::<String, Vec<&Value>>::new();
    for row in rows {
        let cell = text(row, "cellId")?.to_owned();
        *counts.entry(cell.clone()).or_default() += 1;
        groups.entry(cell).or_default().push(row);
    }
    for values in groups.values_mut() {
        values.sort_by(|left, right| {
            right["currentPanelRank"]
                .as_f64()
                .unwrap_or(f64::NEG_INFINITY)
                .total_cmp(
                    &left["currentPanelRank"]
                        .as_f64()
                        .unwrap_or(f64::NEG_INFINITY),
                )
                .then_with(|| {
                    text(left, "candidateId")
                        .unwrap()
                        .cmp(text(right, "candidateId").unwrap())
                })
        });
    }
    let mut selected = Vec::new();
    loop {
        let mut added = false;
        for group in groups.values_mut() {
            if selected.len() == limit {
                break;
            }
            if !group.is_empty() {
                selected.push(group.remove(0));
                added = true;
            }
        }
        if !added || selected.len() == limit {
            break;
        }
    }
    let mut cs = Vec::new();
    for r in selected {
        let cell = text(r, "cellId")?;
        cs.push(json!({"candidateId":text(r,"candidateId")?,"candidateIdentitySha256":hash_of(r,"candidateIdentitySha256")?,"programSha256":hash_of(r,"programSha256")?,"profileSnapshotSha256":hash_of(r,"profileSnapshotSha256")?,"cellId":cell,"costView":"research_conservative","currentPanelRank":r["currentPanelRank"],"novelty":1.0 / *counts.get(cell).expect("selection cell count") as f64}));
    }
    let mut v = json!({"schemaVersion":"temporal_qd_provisional_survivors_v1","generationIndex":g,"panelId":p,"cohortSha256":hash_of(c,"cohortSha256")?,"candidateCount":cs.len(),"candidates":cs});
    add(&mut v, "provisionalSha256")?;
    Ok(v)
}
fn task(
    ord: u64,
    role: &str,
    panel: &str,
    rows: &[Value],
    authority: &str,
    round: u64,
    rotating: &str,
    source_authority: Value,
) -> Result<(Value, Value)> {
    let set=canonical_sha256(&Value::Array(rows.iter().map(|r|json!({"candidateId":text(r,"candidateId").unwrap_or(""),"candidateIdentitySha256":hash_of(r,"candidateIdentitySha256").unwrap_or("")})).collect()))?;
    let relative = format!("task-candidates/round-{round}-task-{ord}.jsonl");
    let descriptor = task_sidecar_descriptor(&relative, rows, &set, authority)?;
    let selection = json!({"schemaVersion":"temporal_qd_v5_native_rich_candidate_selection_v2","candidateSetSha256":set,"candidateRows":descriptor});
    let selection_document_relative_path =
        format!("task-selections/round-{round}-task-{ord}.selection.json");
    let selection_receipt_relative_path =
        format!("task-selections/round-{round}-task-{ord}.receipt.json");
    let mut v = json!({"taskOrdinal":ord,"campaignRole":role,"panelId":panel,"rotatingEvidenceSha256":rotating,"cohortSelection":selection,"candidateCount":rows.len(),"candidateSetSha256":set,"sourceAuthority":source_authority,"selectionDocumentSchema":NON_PROPOSAL_SELECTION_SCHEMA,"selectionDocumentRelativePath":selection_document_relative_path,"selectionReceiptRelativePath":selection_receipt_relative_path});
    add(&mut v, "taskSha256")?;
    let descriptor = member(member(&v, "cohortSelection")?, "candidateRows")?.clone();
    Ok((v, descriptor))
}

fn retained_parent_source_authority(base: &Base, rows: &[Value]) -> Result<Value> {
    let proof = rows
        .iter()
        .map(|row| {
            Ok(json!({"candidateId":text(row,"candidateId")?,"candidateIdentitySha256":hash_of(row,"candidateIdentitySha256")?,"programSha256":hash_of(row,"programSha256")?,"profileSnapshotSha256":hash_of(row,"profileSnapshotSha256")?}))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(
        json!({"schemaVersion":"temporal_qd_v5_retained_parent_archive_member_proof_v1","previousParentArchiveSha256":binding_identity(member(&base.value,"previousParentArchiveBinding")?)?,"candidateMemberProof":proof}),
    )
}

fn ledger_for_task_authority(
    generation: u64,
    rotating: &str,
    cohort: &Value,
    provisional: &Value,
    receipts: &[Value],
) -> Result<Value> {
    make_ledger(generation, rotating, cohort, provisional, receipts)
}

fn prior_panel_backfill_source_authority(
    cohort: &Value,
    provisional: &Value,
    ledger: &Value,
    previous: Option<&Value>,
) -> Result<Value> {
    let proof = array(ledger, "campaigns")?
        .iter()
        .map(|campaign| {
            Ok(json!({"campaignRole":text(campaign,"campaignRole")?,"panelId":text(campaign,"panelId")?,"semanticReceiptSha256":hash_of(campaign,"semanticReceiptSha256")?,"receiptSha256":hash_of(campaign,"receiptSha256")?}))
        })
        .collect::<Result<Vec<_>>>()?;
    let selected_candidate_proof = array(provisional, "candidates")?
        .iter()
        .map(|row| {
            Ok(json!({"candidateId":text(row,"candidateId")?,"candidateIdentitySha256":hash_of(row,"candidateIdentitySha256")?,"programSha256":hash_of(row,"programSha256")?,"profileSnapshotSha256":hash_of(row,"profileSnapshotSha256")?}))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(
        json!({"schemaVersion":"temporal_qd_v5_prior_panel_backfill_source_authority_v1","cohortSha256":hash_of(cohort,"cohortSha256")?,"provisionalSha256":hash_of(provisional,"provisionalSha256")?,"admittedCampaignLedgerSha256":hash_of(ledger,"admittedCampaignLedgerSha256")?,"priorResultSha256":previous.map(|value|hash_of(value,"resultSha256").map(str::to_owned)).transpose()?,"priorReceiptProof":proof,"selectedCandidateProof":selected_candidate_proof}),
    )
}
fn make_plan(s: &str, g: u64, r: u64, phase: &str, t: Vec<Value>) -> Result<Value> {
    let mut v = json!({"schemaVersion":TASK_PLAN_SCHEMA,"contractVersion":CONTRACT_VERSION,"semanticAuthoritySha256":s,"generationIndex":g,"roundIndex":r,"phase":phase,"tasks":t,"taskCount":t.len()});
    add(&mut v, "taskPlanSha256")?;
    Ok(v)
}

/// Materialize the authenticated, non-proposal handoff after the result is
/// sealed.  The document is intentionally a separate file: the task plan can
/// name a deterministic path without creating a task/result hash cycle.
fn materialize_non_proposal_task_selections(root: &Path, result: &Value) -> Result<()> {
    validate_result(result)?;
    let plan = member(result, "taskPlan")?;
    self_hash(plan, "taskPlanSha256", "v5 task plan")?;
    for task in array(plan, "tasks")? {
        let role = text(task, "campaignRole")?;
        ensure!(
            matches!(
                role,
                "retained_parent_current_panel" | "prior_panel_backfill"
            ),
            "only non-proposal roles may have a v2 selection document"
        );
        self_hash(task, "taskSha256", "v5 task")?;
        let document_path = safe_output_path(root, text(task, "selectionDocumentRelativePath")?)?;
        let receipt_path = safe_output_path(root, text(task, "selectionReceiptRelativePath")?)?;
        let document = selection_document(result, plan, task)?;
        publish(document_path, &document)?;
        let receipt = selection_receipt(&document)?;
        // Receipt-last: this is the final durable statement of a handoff.
        publish(receipt_path, &receipt)?;
    }
    Ok(())
}

fn selection_document(result: &Value, plan: &Value, task: &Value) -> Result<Value> {
    let mut document = json!({
        "schemaVersion": NON_PROPOSAL_SELECTION_SCHEMA,
        "prefinalizerResultSha256":hash_of(result,"resultSha256")?,
        "taskPlanSha256":hash_of(plan,"taskPlanSha256")?,
        "taskSha256":hash_of(task,"taskSha256")?,
        "semanticAuthoritySha256":hash_of(plan,"semanticAuthoritySha256")?,
        "generationIndex":unsigned(plan,"generationIndex")?,
        "roundIndex":unsigned(plan,"roundIndex")?,
        "campaignRole":text(task,"campaignRole")?,
        "panelId":text(task,"panelId")?,
        "rotatingEvidenceSha256":hash_of(task,"rotatingEvidenceSha256")?,
        "candidateSetSha256":hash_of(task,"candidateSetSha256")?,
        "candidateRows":member(member(task,"cohortSelection")?,"candidateRows")?,
        "sourceAuthority":member(task,"sourceAuthority")?,
        "selectionReceiptRelativePath":text(task,"selectionReceiptRelativePath")?,
    });
    add(&mut document, "selectionDocumentSha256")?;
    Ok(document)
}

fn selection_receipt(document: &Value) -> Result<Value> {
    let mut receipt = json!({
        "schemaVersion": NON_PROPOSAL_SELECTION_RECEIPT_SCHEMA,
        "selectionDocumentSha256":hash_of(document,"selectionDocumentSha256")?,
        "prefinalizerResultSha256":hash_of(document,"prefinalizerResultSha256")?,
        "taskPlanSha256":hash_of(document,"taskPlanSha256")?,
        "taskSha256":hash_of(document,"taskSha256")?,
        "semanticAuthoritySha256":hash_of(document,"semanticAuthoritySha256")?,
        "generationIndex":unsigned(document,"generationIndex")?,
        "roundIndex":unsigned(document,"roundIndex")?,
        "campaignRole":text(document,"campaignRole")?,
        "panelId":text(document,"panelId")?,
        "rotatingEvidenceSha256":hash_of(document,"rotatingEvidenceSha256")?,
        "candidateSetSha256":hash_of(document,"candidateSetSha256")?,
        "candidateRowsSha256":hash_of(member(document,"candidateRows")?,"descriptorSha256")?,
    });
    add(&mut receipt, "receiptSha256")?;
    Ok(receipt)
}

/// Reauthenticate a v2 non-proposal task handoff for the campaign freezer.
/// This does not trust a self-hashed result/plan: it reopens the prefinalizer
/// manifest, validates its base/resume chain, and deterministically rebuilds
/// the result before comparing the exact task/document/receipt bytes.
pub fn validate_v2_task_selection_handoff(path: &Path) -> Result<Vec<Value>> {
    ensure!(path.is_file(), "v2 task selection document is not a file");
    ensure!(
        !fs::symlink_metadata(path)?.file_type().is_symlink(),
        "v2 task selection document symlink is forbidden"
    );
    let selection_root = path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| anyhow!("v2 task selection path has no prefinalizer root"))?;
    let relative = path
        .strip_prefix(selection_root)
        .map_err(|_| anyhow!("v2 task selection path escapes prefinalizer root"))?;
    ensure!(
        relative
            .components()
            .all(|part| matches!(part, Component::Normal(_))),
        "v2 task selection relative path is unsafe"
    );
    let relative_for_plan = portable_task_plan_relative_path(relative)?;
    let manifest = read_canonical(
        &selection_root.join("manifest.json"),
        "v2 prefinalizer manifest",
    )?;
    let prepared = authenticated_prepare(selection_root, &manifest)?;
    let result = read_canonical(
        &selection_root.join("result.json"),
        "v2 prefinalizer result",
    )?;
    ensure!(
        result == prepared.result,
        "v2 prefinalizer result is not rederived from authenticated authority"
    );
    let plan = member(&result, "taskPlan")?;
    let task = array(plan, "tasks")?
        .iter()
        .find(|task| {
            task.get("selectionDocumentRelativePath")
                .and_then(Value::as_str)
                == Some(relative_for_plan.as_str())
        })
        .ok_or_else(|| anyhow!("v2 task selection is not named by its sealed task plan"))?;
    let expected_document = selection_document(&result, plan, task)?;
    let document = read_canonical(path, "v2 task selection document")?;
    ensure!(
        document == expected_document,
        "v2 task selection document drifted"
    );
    let receipt_path =
        safe_output_path(selection_root, text(task, "selectionReceiptRelativePath")?)?;
    let receipt = read_canonical(&receipt_path, "v2 task selection receipt")?;
    ensure!(
        receipt == selection_receipt(&document)?,
        "v2 task selection receipt drifted"
    );
    let descriptor = member(member(task, "cohortSelection")?, "candidateRows")?;
    validate_task_descriptor(selection_root, descriptor, None)?;
    rows(
        descriptor,
        selection_root,
        "v2 task selected candidate rows",
    )
}

fn portable_task_plan_relative_path(path: &Path) -> Result<String> {
    path.components()
        .map(|component| match component {
            Component::Normal(value) => value
                .to_str()
                .map(ToOwned::to_owned)
                .ok_or_else(|| anyhow!("v2 task selection path is not UTF-8")),
            _ => Err(anyhow!("v2 task selection relative path is unsafe")),
        })
        .collect::<Result<Vec<_>>>()
        .map(|components| components.join("/"))
}
fn make_ledger(g: u64, rot: &str, c: &Value, p: &Value, rs: &[Value]) -> Result<Value> {
    let campaigns=rs.iter().map(|r|json!({"campaignRole":text(r,"campaignRole").unwrap(),"panelId":text(r,"panelId").unwrap(),"semanticReceiptSha256":hash_of(r,"semanticReceiptSha256").unwrap(),"receiptSha256":hash_of(r,"receiptSha256").unwrap(),"receipt":r})).collect::<Vec<_>>();
    let mut v = json!({"schemaVersion":"temporal_qd_v5_admitted_campaign_ledger_v1","generationIndex":g,"rotatingEvidenceSha256":rot,"cohortSha256":hash_of(c,"cohortSha256")?,"provisionalSha256":hash_of(p,"provisionalSha256")?,"campaigns":campaigns});
    add(&mut v, "admittedCampaignLedgerSha256")?;
    Ok(v)
}
fn make_coverage(
    g: u64,
    rot: &str,
    c: &Value,
    p: &Value,
    panels: &[String],
    bs: &BTreeMap<(String, String), Value>,
) -> Result<Value> {
    let mut map = Map::new();
    for row in array(p, "candidates")? {
        let id = text(row, "candidateId")?.to_owned();
        let got = panels
            .iter()
            .filter(|x| bs.contains_key(&(id.clone(), (*x).clone())))
            .cloned()
            .collect::<Vec<_>>();
        map.insert(id, json!({"panelIds":got}));
    }
    let b = Value::Array(bs.values().cloned().collect());
    let mut v = json!({"schemaVersion":"temporal_qd_v5_panel_coverage_v1","generationIndex":g,"rotatingEvidenceSha256":rot,"cohortSha256":hash_of(c,"cohortSha256")?,"provisionalSha256":hash_of(p,"provisionalSha256")?,"requiredPanelIds":panels,"candidatePanelBundleSha256":canonical_sha256(&b)?,"coverage":map});
    add(&mut v, "panelCoverageSha256")?;
    Ok(v)
}
fn make_rich(
    g: u64,
    c: &Value,
    p: &Value,
    rows: &[Value],
    records: &BTreeMap<String, Value>,
) -> Result<Value> {
    let members = rows
        .iter()
        .map(|candidate| {
            let id = text(candidate, "candidateId")?;
            let record = records
                .get(id)
                .ok_or_else(|| anyhow!("selected candidate lacks evaluated-member record"))?;
            let nested = record.get("candidate").unwrap_or(record);
            ensure!(
                hash_of(nested, "candidateIdentitySha256")?
                    == hash_of(candidate, "candidateIdentitySha256")?,
                "selected rich member candidate identity drifted"
            );
            let mut lifted = record.clone();
            let top = lifted
                .as_object_mut()
                .ok_or_else(|| anyhow!("evaluated-member record must be an object"))?;
            // The finalizer consumes rich evaluated-member records but also
            // requires these identity fields at the rich-row boundary.  Copy
            // only authenticated nested values and reject a conflicting
            // pre-existing lift.
            for field in [
                "candidateId",
                "candidateIdentitySha256",
                "programSha256",
                "profileSnapshotSha256",
            ] {
                let value = member(nested, field)?.clone();
                if let Some(existing) = top.get(field) {
                    ensure!(
                        existing == &value,
                        "selected rich member top-level/nested {field} drifted"
                    );
                } else {
                    top.insert(field.to_owned(), value);
                }
            }
            Ok(lifted)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut v = json!({"schemaVersion":"temporal_qd_selected_rich_members_v1","generationIndex":g,"cohortSha256":hash_of(c,"cohortSha256")?,"provisionalSha256":hash_of(p,"provisionalSha256")?,"members":members,"memberCount":rows.len()});
    add(&mut v, "selectedRichMembersSha256")?;
    Ok(v)
}

fn receipt_rows(r: &Value, kind: &str, root: &Path) -> Result<Vec<Value>> {
    let exec = member(r, "executionBindings")?;
    let key = if kind == "evaluatedMembers" {
        "evaluatedMembersJsonl"
    } else {
        "candidatePanelBundlesJsonl"
    };
    // The receipt's semantic descriptor owns the row count/schema; its
    // execution binding owns only the reopenable path/raw file identity.
    let bound = member(exec, key)?;
    let semantic = member(r, kind)?;
    let d = json!({
        "path": text(bound,"path")?, "rawSha256": hash_of(bound,"rawSha256")?,
        "sizeBytes": unsigned(bound,"sizeBytes")?, "recordCount": unsigned(semantic,"recordCount")?,
    });
    let rows = rows(&d, root, kind)?;
    let expected_schema = text(semantic, "rowSchema")?;
    for row in &rows {
        ensure!(
            text(row, "schemaVersion")? == expected_schema,
            "{kind} row schema drifted"
        );
        match kind {
            "evaluatedMembers" => {
                let candidate = member(row, "candidate")?;
                hash_of(candidate, "candidateIdentitySha256")?;
                hash_of(candidate, "programSha256")?;
                hash_of(candidate, "profileSnapshotSha256")?;
            }
            "candidatePanelBundles" => self_hash(row, "bundleSha256", "campaign bundle row")?,
            _ => unreachable!(),
        }
    }
    Ok(rows)
}
fn admitted_member_role_ids(
    receipts: &[Value],
    root: &Path,
) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
    let mut proposal = BTreeSet::new();
    let mut retained = BTreeSet::new();
    for receipt in receipts {
        let target = match text(receipt, "campaignRole")? {
            "proposal_current_panel" => &mut proposal,
            "retained_parent_current_panel" => &mut retained,
            "prior_panel_backfill" => continue,
            _ => return Err(anyhow!("admitted receipt has unknown campaign role")),
        };
        for row in receipt_rows(receipt, "evaluatedMembers", root)? {
            let candidate = row.get("candidate").unwrap_or(&row);
            ensure!(
                target.insert(text(candidate, "candidateId")?.to_owned()),
                "admitted campaign role repeats a candidate"
            );
        }
    }
    Ok((proposal, retained))
}
fn candidate_with_member_selection_fields(raw: &Value) -> Result<Value> {
    let mut candidate = raw.get("candidate").unwrap_or(raw).clone();
    if raw.get("descriptor").is_none() && raw.get("aggregate").is_none() {
        ensure!(
            candidate.get("cellId").and_then(Value::as_str).is_some()
                && candidate
                    .get("currentPanelRank")
                    .and_then(Value::as_f64)
                    .is_some(),
            "evaluated member lacks descriptor/aggregate and candidate selection fields"
        );
        return Ok(candidate);
    }
    let descriptor = member(raw, "descriptor")?;
    let aggregate = member(raw, "aggregate")?;
    let cell = text(descriptor, "cellId")?;
    let rank = member(aggregate, "totalConservativeNetR")?;
    ensure!(
        rank.as_f64().is_some(),
        "evaluated member rank must be finite numeric"
    );
    let map = candidate
        .as_object_mut()
        .ok_or_else(|| anyhow!("evaluated member candidate must be object"))?;
    if let Some(existing) = map.get("cellId") {
        ensure!(
            existing.as_str() == Some(cell),
            "candidate/member cell drifted"
        );
    } else {
        map.insert("cellId".into(), Value::String(cell.to_owned()));
    }
    if let Some(existing) = map.get("currentPanelRank") {
        ensure!(
            existing == rank,
            "candidate/member current panel rank drifted"
        );
    } else {
        map.insert("currentPanelRank".into(), rank.clone());
    }
    Ok(candidate)
}
fn backfill_candidate_with_current_panel_selection_fields(raw: &Value) -> Result<Value> {
    let candidate = raw.get("candidate").unwrap_or(raw).clone();
    let cell = candidate
        .get("cellId")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("prior-panel backfill candidate lacks current-panel cell"))?;
    ensure!(
        !cell.is_empty(),
        "prior-panel backfill candidate current-panel cell is empty"
    );
    ensure!(
        candidate
            .get("currentPanelRank")
            .and_then(Value::as_f64)
            .is_some(),
        "prior-panel backfill candidate lacks finite current-panel rank"
    );
    match (raw.get("descriptor"), raw.get("aggregate")) {
        (Some(descriptor), Some(aggregate)) => {
            text(descriptor, "cellId")?;
            ensure!(
                member(aggregate, "totalConservativeNetR")?
                    .as_f64()
                    .is_some(),
                "prior-panel backfill aggregate rank must be finite numeric"
            );
        }
        (None, None) => {}
        _ => {
            return Err(anyhow!(
                "prior-panel backfill evaluated member has partial descriptor/aggregate"
            ));
        }
    }
    Ok(candidate)
}
fn validate_backfill_candidate_projection(selected: &Value, backfill: &Value) -> Result<()> {
    ensure!(
        selected == backfill,
        "prior-panel backfill candidate selection projection drifted"
    );
    Ok(())
}
fn all_bundles(
    base: &Base,
    rs: &[Value],
    root: &Path,
) -> Result<BTreeMap<(String, String), Value>> {
    let mut out = BTreeMap::new();
    // A cumulative archive is sealed finalizer output.  Reuse only bundles
    // whose candidate is an authenticated retained parent; proposals never
    // gain historical coverage by ID collision.
    if !base.prior_cumulative.is_null() {
        let retained = base
            .parent_members
            .iter()
            .map(|row| row.get("candidate").unwrap_or(row))
            .map(|candidate| {
                Ok((
                    text(candidate, "candidateId")?.to_owned(),
                    candidate.clone(),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        for bundle in array(&base.prior_cumulative, "candidatePanelBundles")? {
            let id = text(bundle, "candidateId")?;
            let Some(candidate) = retained.get(id) else {
                continue;
            };
            validate_bundle(base, bundle, Some(candidate))?;
            let key = (id.to_owned(), text(bundle, "panelId")?.to_owned());
            ensure!(
                out.insert(key, bundle.clone()).is_none(),
                "prior cumulative archive repeats a candidate panel bundle"
            );
        }
    }
    for r in rs {
        let member_ids = receipt_rows(r, "evaluatedMembers", root)?
            .into_iter()
            .map(|row| row.get("candidate").unwrap_or(&row).clone())
            .map(|row| text(&row, "candidateId").map(str::to_owned))
            .collect::<Result<BTreeSet<_>>>()?;
        for b in receipt_rows(r, "candidatePanelBundles", root)? {
            ensure!(
                member_ids.contains(text(&b, "candidateId")?),
                "campaign receipt contains a bundle for a non-cohort candidate"
            );
            ensure!(
                text(&b, "panelId")? == text(r, "panelId")?,
                "campaign receipt bundle panel relabelled"
            );
            let candidate = receipt_rows(r, "evaluatedMembers", root)?
                .into_iter()
                .find_map(|row| {
                    let candidate = row.get("candidate").unwrap_or(&row);
                    (text(candidate, "candidateId").ok() == Some(text(&b, "candidateId").ok()?))
                        .then_some(candidate.clone())
                })
                .ok_or_else(|| anyhow!("campaign receipt bundle candidate is absent"))?;
            validate_bundle(base, &b, Some(&candidate))?;
            let k = (
                text(&b, "candidateId")?.to_owned(),
                text(&b, "panelId")?.to_owned(),
            );
            if let Some(old) = out.insert(k, b.clone()) {
                ensure!(
                    old == b,
                    "candidate panel bundle conflicts with sealed prior evidence"
                );
            }
        }
    }
    let mut resolved = BTreeMap::<String, (String, String)>::new();
    for bundle in out.values() {
        for record in array(bundle, "windowEvidence")? {
            let metrics = member(record, "metrics")?;
            let value = (
                hash_of(metrics, "resolvedProfileSnapshotSha256")?.to_owned(),
                hash_of(metrics, "resolvedProgramSha256")?.to_owned(),
            );
            let id = text(bundle, "candidateId")?.to_owned();
            if let Some(previous) = resolved.insert(id, value.clone()) {
                ensure!(
                    previous == value,
                    "candidate panel bundles disagree on resolved program/profile identity"
                );
            }
        }
    }
    Ok(out)
}

fn validate_bundle(base: &Base, bundle: &Value, candidate: Option<&Value>) -> Result<()> {
    ensure!(
        text(bundle, "schemaVersion")? == "temporal_qd_candidate_panel_evidence_bundle_v1",
        "candidate panel bundle schema is invalid"
    );
    self_hash(bundle, "bundleSha256", "candidate panel bundle")?;
    ensure!(
        hash_of(bundle, "rotatingEvidenceSha256")?
            == hash_of(&base.rotating, "rotatingEvidenceSha256")?,
        "candidate panel bundle rotating authority drifted"
    );
    if let Some(candidate) = candidate {
        ensure!(
            hash_of(bundle, "candidateIdentitySha256")?
                == hash_of(candidate, "candidateIdentitySha256")?
                && hash_of(bundle, "programSha256")? == hash_of(candidate, "programSha256")?,
            "candidate panel bundle candidate identity drifted"
        );
    }
    let panel_id = text(bundle, "panelId")?;
    let panel = array(&base.rotating, "panels")?
        .iter()
        .find(|row| text(row, "panelId").ok() == Some(panel_id))
        .ok_or_else(|| anyhow!("candidate panel bundle names an unknown panel"))?;
    let expected_windows = array(panel, "windows")?
        .iter()
        .map(|window| text(window, "windowId").map(str::to_owned))
        .collect::<Result<BTreeSet<_>>>()?;
    let records = array(bundle, "windowEvidence")?;
    ensure!(
        records.len() == expected_windows.len(),
        "candidate panel bundle window count drifted"
    );
    let mut actual = BTreeSet::new();
    for record in records {
        ensure!(
            text(record, "schemaVersion")? == "temporal_qd_candidate_window_evidence_v1",
            "candidate panel window evidence schema is invalid"
        );
        self_hash(record, "recordSha256", "candidate panel window evidence")?;
        ensure!(
            text(record, "candidateId")? == text(bundle, "candidateId")?
                && text(record, "panelId")? == panel_id
                && hash_of(record, "candidateIdentitySha256")?
                    == hash_of(bundle, "candidateIdentitySha256")?
                && hash_of(record, "programSha256")? == hash_of(bundle, "programSha256")?,
            "candidate panel window evidence binding drifted"
        );
        if let Some(candidate) = candidate {
            ensure!(
                hash_of(member(record, "metrics")?, "sourceProfileSnapshotSha256")?
                    == hash_of(candidate, "profileSnapshotSha256")?,
                "candidate panel window source profile drifted"
            );
        }
        ensure!(
            actual.insert(text(record, "windowId")?.to_owned()),
            "candidate panel bundle repeats a window"
        );
    }
    ensure!(
        actual == expected_windows,
        "candidate panel bundle windows drifted"
    );
    Ok(())
}
fn ensure_unique_receipts(rs: &[Value]) -> Result<()> {
    let mut pairs = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for r in rs {
        campaign_receipt::validate_receipt(r)?;
        ensure!(
            pairs.insert((
                text(r, "campaignRole")?.to_owned(),
                text(r, "panelId")?.to_owned()
            )),
            "duplicate campaign role/panel receipt"
        );
        ensure!(
            ids.insert(hash_of(r, "receiptSha256")?.to_owned()),
            "campaign receipt reuse"
        );
    }
    Ok(())
}
fn receipts_from_result(v: &Value) -> Result<Vec<Value>> {
    let cs = array(member(v, "admittedCampaignLedger")?, "campaigns")?;
    let mut out = Vec::new();
    for c in cs {
        let r = member(c, "receipt")?.clone();
        out.push(r)
    }
    Ok(out)
}
fn validate_resume_task_completions(
    previous: &Value,
    previous_root: &Path,
    receipts: &[Value],
    receipt_root: &Path,
    generation: u64,
) -> Result<()> {
    let plan = member(previous, "taskPlan")?;
    let tasks = array(plan, "tasks")?;
    ensure!(
        receipts.len() == tasks.len() && !tasks.is_empty(),
        "resume must provide every outstanding task receipt exactly once"
    );
    let mut expected = BTreeMap::new();
    for task in tasks {
        let document_path =
            safe_output_path(previous_root, text(task, "selectionDocumentRelativePath")?)?;
        let document = read_canonical(&document_path, "outstanding task selection document")?;
        ensure!(
            document.get("taskSha256") == task.get("taskSha256")
                && document.get("candidateSetSha256") == task.get("candidateSetSha256"),
            "outstanding task selection document/task drifted"
        );
        expected.insert(hash_of(task, "taskSha256")?.to_owned(), (task, document));
    }
    let mut completed = BTreeSet::new();
    for receipt in receipts {
        ensure!(
            unsigned(receipt, "generationIndex")? == generation,
            "resume receipt generation drifted"
        );
        let source = member(receipt, "cohortSource")?;
        ensure!(
            text(source, "kind")? == "sealed_cohort_selection",
            "resume receipt must bind a sealed task selection"
        );
        let selection_sha = hash_of(source, "selectionSha256")?;
        let (_, task, _document) = expected
            .iter()
            .find_map(|(sha, (task, document))| {
                (document
                    .get("selectionDocumentSha256")
                    .and_then(Value::as_str)
                    == Some(selection_sha))
                .then_some((sha, *task, document))
            })
            .ok_or_else(|| anyhow!("resume receipt selection is not an outstanding task"))?;
        let task_sha = hash_of(task, "taskSha256")?.to_owned();
        ensure!(
            completed.insert(task_sha),
            "resume receipt completes a task twice"
        );
        ensure!(
            text(receipt, "campaignRole")? == text(task, "campaignRole")?
                && text(receipt, "panelId")? == text(task, "panelId")?
                && hash_of(receipt, "rotatingEvidenceSha256")?
                    == hash_of(task, "rotatingEvidenceSha256")?
                && source.get("candidateCount").and_then(Value::as_u64)
                    == Some(unsigned(task, "candidateCount")?),
            "resume receipt task role/panel/rotating/count drifted"
        );
        let wanted = rows(
            member(member(task, "cohortSelection")?, "candidateRows")?,
            previous_root,
            "outstanding task candidates",
        )?
        .into_iter()
        .map(|row| {
            Ok((
                text(&row, "candidateId")?.to_owned(),
                hash_of(&row, "candidateIdentitySha256")?.to_owned(),
            ))
        })
        .collect::<Result<BTreeSet<_>>>()?;
        let got = receipt_rows(receipt, "evaluatedMembers", receipt_root)?
            .into_iter()
            .map(|raw| {
                let candidate = raw.get("candidate").unwrap_or(&raw);
                Ok((
                    text(candidate, "candidateId")?.to_owned(),
                    hash_of(candidate, "candidateIdentitySha256")?.to_owned(),
                ))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        ensure!(
            wanted == got,
            "resume receipt candidate set differs from outstanding task"
        );
    }
    ensure!(
        completed.len() == expected.len(),
        "resume leaves an outstanding task incomplete"
    );
    Ok(())
}

fn validate_result(v: &Value) -> Result<()> {
    exact(
        v,
        &[
            "schemaVersion",
            "contractVersion",
            "baseManifestSha256",
            "manifestSha256",
            "semanticAuthoritySha256",
            "roundIndex",
            "previousResultSha256",
            "generationIndex",
            "status",
            "admittedCampaignLedger",
            "cohort",
            "provisional",
            "panelCoverage",
            "taskPlan",
            "funnelReductionSource",
            "selectedRichMembers",
            "finalizerSource",
            "finalizerManifest",
            "resultSha256",
        ],
        "v5 result",
    )?;
    ensure!(
        text(v, "schemaVersion")? == RESULT_SCHEMA
            && text(v, "contractVersion")? == CONTRACT_VERSION
            && matches!(
                text(v, "status")?,
                "awaiting_retained_parent_current_panel"
                    | "awaiting_prior_panel_backfill"
                    | "ready_for_finalizer"
            ),
        "v5 result schema/status invalid"
    );
    self_hash(v, "resultSha256", "v5 result")?;
    Ok(())
}
fn validate_state(v: &Value, g: u64) -> Result<()> {
    exact(
        v,
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
        "state basis",
    )?;
    ensure!(
        text(v, "schemaVersion")? == "temporal_qd_v5_generation_state_basis_v1"
            && unsigned(v, "generationIndex")? == g,
        "state basis schema/generation drifted"
    );
    self_hash(v, "stateBasisSha256", "state basis")
}

fn validate_completed_generation_records(records: &Value, state: &Value) -> Result<()> {
    let rows = records
        .as_array()
        .ok_or_else(|| anyhow!("completed generation records must be an array"))?;
    ensure!(
        canonical_sha256(records)? == hash_of(state, "completedGenerationsSha256")?,
        "completed generation records drift from state basis"
    );
    for row in rows {
        ensure!(
            text(row, "schemaVersion")? == "temporal_qd_generation_record_v2",
            "completed generation record schema is invalid"
        );
        self_hash(row, "generationRecordSha256", "completed generation record")?;
    }
    Ok(())
}

fn validate_proposal_state_authority(authority: &Value, construction: &Value) -> Result<()> {
    exact(
        authority,
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
    let kind = text(authority, "generationKind")?;
    ensure!(
        matches!(kind, "g0" | "evolved"),
        "proposal state generation kind is invalid"
    );
    for key in [
        "proposalManifestSha256",
        "proposalReceiptSha256",
        "generationJournalSha256",
        "outputIdentityLedgerSha256",
        "outputIdentityLedgerFileSha256",
    ] {
        hash_of(authority, key)?;
    }
    match kind {
        "g0" => ensure!(
            member(authority, "inputIdentityLedgerSha256")?.is_null(),
            "G0 input ledger must be null"
        ),
        "evolved" => {
            hash_of(authority, "inputIdentityLedgerSha256")?;
        }
        _ => unreachable!(),
    }
    ensure!(
        text(authority, "outputIdentityLedgerRelativePath")?
            == "proposal/v5-native/identity-ledger.json",
        "proposal state output identity ledger path drifted"
    );
    let invocation = member(construction, "nativeV5Invocation")?;
    let manifest = member(invocation, "proposalManifest")?;
    let manifest_value = reopen_invocation_document(manifest, "proposal manifest")?;
    let result_value =
        reopen_invocation_document(member(invocation, "proposalResult")?, "proposal result")?;
    ensure!(
        hash_of(manifest, "semanticSha256")? == hash_of(authority, "proposalManifestSha256")?
            && hash_of(invocation, "proposalReceiptSha256")?
                == hash_of(authority, "proposalReceiptSha256")?
            && hash_of(construction, "proposalReceiptSha256")?
                == hash_of(authority, "proposalReceiptSha256")?,
        "proposal manifest/receipt authority differs from invocation closure"
    );
    ensure!(
        hash_of(manifest, "semanticSha256")? == hash_of(&manifest_value, "manifestSha256")?,
        "reopened proposal manifest semantic identity drifted"
    );
    let result_hash = ["resultSha256", "proposalResultSha256"]
        .iter()
        .find_map(|field| hash_of(&result_value, field).ok())
        .ok_or_else(|| anyhow!("reopened proposal result lacks semantic identity"))?;
    ensure!(
        hash_of(member(invocation, "proposalResult")?, "semanticSha256")? == result_hash,
        "reopened proposal result semantic identity drifted"
    );
    ensure!(
        (kind == "g0"
            && text(invocation, "schemaVersion")?
                == "temporal_qd_native_v5_g0_invocation_descriptor_v1")
            || (kind == "evolved"
                && text(invocation, "schemaVersion")?
                    == "temporal_qd_native_v5_evolved_invocation_descriptor_v1"),
        "proposal state generation kind differs from invocation family"
    );
    let roots = semantic_roots(construction)?;
    ensure!(
        hash_of(&roots, "generationJournalSha256")?
            == hash_of(authority, "generationJournalSha256")?,
        "proposal state generation journal differs from semantic roots"
    );
    ensure!(
        hash_of(&roots, "proposalReceiptSha256")? == hash_of(authority, "proposalReceiptSha256")?,
        "proposal state receipt differs from semantic roots"
    );
    let ledger = member(construction, "identityLedger")?;
    ensure!(
        hash_of(ledger, "semanticSha256")? == hash_of(authority, "outputIdentityLedgerSha256")?
            && hash_of(ledger, "fileSha256")?
                == hash_of(authority, "outputIdentityLedgerFileSha256")?,
        "proposal state output identity ledger differs from construction descriptor"
    );
    if kind == "evolved" {
        ensure!(
            hash_of(construction, "inputIdentityLedgerSha256")?
                == hash_of(authority, "inputIdentityLedgerSha256")?,
            "evolved proposal input identity ledger differs from construction closure"
        );
    }
    Ok(())
}

fn reopen_invocation_document(descriptor: &Value, name: &str) -> Result<Value> {
    exact(
        descriptor,
        &[
            "schemaVersion",
            "documentSchemaVersion",
            "relativePath",
            "absolutePath",
            "semanticSha256",
            "fileSha256",
            "byteLength",
        ],
        name,
    )?;
    ensure!(
        text(descriptor, "schemaVersion")?
            == "temporal_qd_native_v5_invocation_document_descriptor_v1",
        "{name} descriptor schema is invalid: {}",
        text(descriptor, "schemaVersion")?
    );
    let path = PathBuf::from(text(descriptor, "absolutePath")?);
    ensure!(
        path.is_absolute() && path.is_file(),
        "{name} descriptor path is invalid"
    );
    ensure!(
        !path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir)),
        "{name} descriptor path has an alias"
    );
    ensure!(
        !fs::symlink_metadata(&path)?.file_type().is_symlink(),
        "{name} descriptor symlink is forbidden"
    );
    let relative = Path::new(text(descriptor, "relativePath")?);
    ensure!(
        relative
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
            && path.file_name() == relative.file_name(),
        "{name} descriptor relative/absolute path drifted"
    );
    let raw = fs::read(&path)?;
    ensure!(
        raw.len() as u64 == unsigned(descriptor, "byteLength")?
            && file_hash(&path) == hash_of(descriptor, "fileSha256")?,
        "{name} descriptor file identity drifted"
    );
    let value: Value = serde_json::from_slice(&raw).context("parse invocation document")?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "{name} invocation document is not canonical JSON plus LF"
    );
    ensure!(
        text(&value, "schemaVersion")? == text(descriptor, "documentSchemaVersion")?,
        "{name} invocation document schema drifted"
    );
    let semantic_field = match text(descriptor, "documentSchemaVersion")? {
        "temporal_qd_native_v5_proposal_construction_manifest_v1" => "manifestSha256",
        "temporal_qd_native_v5_proposal_construction_result_v5"
        | "temporal_qd_native_v5_evolved_construction_result_v3" => "resultSha256",
        schema => {
            return Err(anyhow!(
                "{name} invocation document schema is unsupported: {schema}"
            ));
        }
    };
    self_hash(&value, semantic_field, name)?;
    ensure!(
        hash_of(&value, semantic_field)? == hash_of(descriptor, "semanticSha256")?,
        "{name} descriptor semantic identity drifted"
    );
    Ok(value)
}

fn reopen_receipt(b: &Value, root: &Path) -> Result<Value> {
    let r = reopen_required(b, root, "campaign receipt")?;
    campaign_receipt::validate_receipt(&r)?;
    Ok(r)
}
fn reopen_value(b: &Value, root: &Path) -> Result<Option<Value>> {
    if b.is_null() {
        return Ok(None);
    };
    if b.is_object() && b.get("path").is_none() {
        return Ok(Some(b.clone()));
    }
    Ok(Some(reopen_required(b, root, "binding")?))
}
fn reopen_required(d: &Value, root: &Path, name: &str) -> Result<Value> {
    let p = safe_path(root, text(d, "path")?)?;
    ensure!(
        fs::metadata(&p)?.len() == unsigned(d, "sizeBytes")?,
        "{name} size drifted"
    );
    ensure!(
        file_hash(&p) == hash_of(d, "rawSha256")?,
        "{name} raw hash drifted"
    );
    read_canonical(&p, name)
}
fn rows(d: &Value, root: &Path, name: &str) -> Result<Vec<Value>> {
    let p = safe_path(root, text(d, "path")?)?;
    ensure!(
        fs::metadata(&p)?.len() == unsigned(d, "sizeBytes")?
            && file_hash(&p) == hash_of(d, "rawSha256")?,
        "{name} descriptor drifted"
    );
    let raw = fs::read(p)?;
    let out = raw
        .split_inclusive(|x| *x == b'\n')
        .filter(|l| !l.is_empty())
        .map(|l| {
            ensure!(l.ends_with(b"\n"), "{name} lacks LF");
            let value: Value =
                serde_json::from_slice(&l[..l.len() - 1]).context("parse native JSONL")?;
            ensure!(
                canonical_json_line(&value)? == l,
                "{name} row is not canonical JSON plus LF"
            );
            Ok(value)
        })
        .collect::<Result<Vec<Value>>>()?;
    ensure!(
        out.len() as u64 == unsigned(d, "recordCount")?,
        "{name} record count drifted"
    );
    Ok(out)
}
fn safe_path(root: &Path, s: &str) -> Result<PathBuf> {
    let p = Path::new(s);
    ensure!(
        !p.components()
            .any(|x| matches!(x, Component::ParentDir | Component::CurDir)),
        "path traversal/alias is forbidden"
    );
    let joined = if p.is_absolute() {
        p.to_path_buf()
    } else {
        root.join(p)
    };
    ensure!(joined.is_file(), "bound path is not a file");
    ensure_no_symlink_components(&joined, "execution binding")?;
    ensure!(
        !fs::symlink_metadata(&joined)?.file_type().is_symlink(),
        "symlink execution binding is forbidden"
    );
    Ok(joined)
}
fn safe_output_path(root: &Path, s: &str) -> Result<PathBuf> {
    let path = Path::new(s);
    ensure!(
        !s.is_empty()
            && !path.is_absolute()
            && path
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "output path is unsafe"
    );
    let joined = root.join(path);
    let mut cursor = root.to_path_buf();
    for part in path.components() {
        let Component::Normal(part) = part else {
            unreachable!()
        };
        cursor.push(part);
        if cursor.exists() {
            ensure!(
                !fs::symlink_metadata(&cursor)?.file_type().is_symlink(),
                "output path contains a symlink"
            );
        }
    }
    Ok(joined)
}
fn sealed_finalizer_output_root(value: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    ensure!(path.is_absolute(), "finalizer output root must be absolute");
    ensure!(
        !path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir)),
        "finalizer output root must not contain path aliases"
    );
    ensure!(path.is_dir(), "finalizer output root is not a directory");
    for ancestor in path.ancestors() {
        ensure!(
            !fs::symlink_metadata(ancestor)?.file_type().is_symlink(),
            "finalizer output root contains a symlink"
        );
    }
    let canonical = fs::canonicalize(path)?;
    ensure!(
        canonical == path,
        "finalizer output root must be its canonical absolute path"
    );
    Ok(canonical)
}
fn publish_finalizer_outputs(root: &Path, source: &Value, manifest: &Value) -> Result<()> {
    let root = sealed_finalizer_output_root(
        root.to_str()
            .ok_or_else(|| anyhow!("finalizer output root is not UTF-8"))?,
    )?;
    let source_path = root.join("source.json");
    let manifest_path = root.join("manifest.json");
    for path in [&source_path, &manifest_path] {
        if path.exists() {
            ensure!(
                !fs::symlink_metadata(path)?.file_type().is_symlink(),
                "finalizer output artifact symlink is forbidden"
            );
        }
    }
    publish(source_path, source)?;
    // The manifest is receipt-last and cannot be written until its source is
    // already sealed at the immutable generation finalization root.
    publish(manifest_path, manifest)
}
fn find_schema(v: &Value, schema: &str) -> Option<Value> {
    if v.get("schemaVersion").and_then(Value::as_str) == Some(schema) {
        return Some(v.clone());
    }
    match v {
        Value::Object(m) => m.values().find_map(|x| find_schema(x, schema)),
        Value::Array(a) => a.iter().find_map(|x| find_schema(x, schema)),
        _ => None,
    }
}
fn semantic_roots(v: &Value) -> Result<Value> {
    member(v, "proposalSemanticRoots")
        .cloned()
        .or_else(|_| member(v, "semanticRoots").cloned())
}
fn binding_hash(v: &Value, names: &[&str]) -> Result<String> {
    for n in names {
        if let Ok(x) = hash_of(v, n) {
            return Ok(x.to_owned());
        }
    }
    Err(anyhow!("binding lacks required semantic hash"))
}
fn binding_identity(v: &Value) -> Result<String> {
    if v.is_null() {
        return Ok(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".into(),
        );
    }
    for n in [
        "semanticSha256",
        "archiveSha256",
        "summarySha256",
        "rawSha256",
    ] {
        if let Ok(x) = hash_of(v, n) {
            return Ok(x.to_owned());
        }
    }
    canonical_sha256(v).map_err(Into::into)
}
fn derive_parent_summary(input: Option<Value>) -> Result<Value> {
    let Some(value) = input else {
        return Err(anyhow!(
            "v5 finalization requires an authenticated previous parent archive"
        ));
    };
    if value.get("summarySha256").is_some() {
        validate_parent_summary(&value)?;
        return Ok(value);
    }
    let archive = hash_of(&value, "archiveSha256")?;
    let candidate_count_seen = unsigned(&value, "candidateCountSeen")?;
    let member_count = unsigned(&value, "memberCount")?;
    let mut cell_ids = array(&value, "cells")?
        .iter()
        .map(|cell| text(cell, "cellId").map(str::to_owned))
        .collect::<Result<Vec<_>>>()?;
    cell_ids.sort();
    ensure!(
        cell_ids.windows(2).all(|pair| pair[0] != pair[1]),
        "previous parent archive contains duplicate cell IDs"
    );
    let counted_members = array(&value, "cells")?
        .iter()
        .map(|cell| array(cell, "members").map(|members| members.len() as u64))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum::<u64>();
    ensure!(
        counted_members == member_count,
        "previous parent archive member count drifted"
    );
    let mut summary = json!({
        "schemaVersion": PREVIOUS_PARENT_SUMMARY_SCHEMA,
        "archiveSha256": archive,
        "candidateCountSeen": candidate_count_seen,
        "memberCount": member_count,
        "cellIds": cell_ids,
    });
    if let Some(pair_policy) = value.get("bidirectionalPairPolicy") {
        summary
            .as_object_mut()
            .expect("summary is an object")
            .insert("bidirectionalPairPolicy".into(), pair_policy.clone());
    }
    add(&mut summary, "summarySha256")?;
    validate_parent_summary(&summary)?;
    Ok(summary)
}

fn validate_parent_summary(summary: &Value) -> Result<()> {
    let mut keys = vec![
        "schemaVersion",
        "archiveSha256",
        "candidateCountSeen",
        "memberCount",
        "cellIds",
        "summarySha256",
    ];
    if summary.get("bidirectionalPairPolicy").is_some() {
        keys.push("bidirectionalPairPolicy");
    }
    exact(summary, &keys, "previous parent archive summary")?;
    ensure!(
        text(summary, "schemaVersion")? == PREVIOUS_PARENT_SUMMARY_SCHEMA,
        "unsupported previous parent archive summary schema"
    );
    hash_of(summary, "archiveSha256")?;
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
    if let Some(pair_policy) = summary.get("bidirectionalPairPolicy") {
        ensure!(
            pair_policy.is_object(),
            "previous parent bidirectional pair policy must be an object"
        );
    }
    self_hash(summary, "summarySha256", "previous parent summary")
}
fn parent_member_rows(value: &Value) -> Result<Vec<Value>> {
    if let Some(rows) = value.get("members").and_then(Value::as_array) {
        return Ok(rows.clone());
    }
    if let Some(cells) = value.get("cells").and_then(Value::as_array) {
        return Ok(cells
            .iter()
            .flat_map(|cell| {
                cell.get("members")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .cloned()
            })
            .collect());
    }
    Ok(Vec::new())
}
fn reject_rich_lists(v: &Value) -> Result<()> {
    fn walk(v: &Value) -> bool {
        match v {
            Value::Object(m) => m.iter().any(|(k, x)| {
                (matches!(
                    k.as_str(),
                    "proposalMembers" | "retainedParents" | "candidates" | "members"
                ) && x.is_array())
                    || walk(x)
            }),
            Value::Array(a) => a.iter().any(walk),
            _ => false,
        }
    }
    ensure!(
        !walk(v),
        "Python rich candidate lists are forbidden in the v5 transaction manifest"
    );
    Ok(())
}
fn publish(path: PathBuf, v: &Value) -> Result<()> {
    let b = canonical_json_line(v)?;
    if path.exists() {
        ensure!(
            fs::read(&path)? == b,
            "write-once output differs: {}",
            path.display()
        );
        return Ok(());
    }
    if let Some(p) = path.parent() {
        fs::create_dir_all(p)?
    }
    let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
    f.write_all(&b)?;
    f.sync_all()?;
    Ok(())
}
fn publish_jsonl(path: PathBuf, rows: &[Value]) -> Result<Value> {
    let mut b = Vec::new();
    for r in rows {
        b.extend(canonical_json_line(r)?)
    }
    if path.exists() {
        ensure!(fs::read(&path)? == b, "write-once JSONL differs")
    } else {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        f.write_all(&b)?;
        f.sync_all()?
    }
    Ok(
        json!({"path":"selected-rich-members.jsonl","rawSha256":format!("sha256:{:x}",Sha256::digest(&b)),"sizeBytes":b.len(),"recordCount":rows.len()}),
    )
}
fn task_sidecar_descriptor(
    relative: &str,
    rows: &[Value],
    set: &str,
    authority: &str,
) -> Result<Value> {
    let bytes = rows
        .iter()
        .flat_map(|row| canonical_json_line(row).unwrap())
        .collect::<Vec<_>>();
    let mut value = json!({"schemaVersion":"temporal_qd_v5_native_rich_candidate_jsonl_descriptor_v1","path":relative,"rawSha256":format!("sha256:{:x}",Sha256::digest(&bytes)),"sizeBytes":bytes.len(),"recordCount":rows.len(),"rowSchema":"temporal_qd_selected_rich_candidate_v1","candidateSetSha256":set,"inputAuthoritySha256":authority});
    add(&mut value, "descriptorSha256")?;
    Ok(value)
}
fn publish_task_sidecar(root: &Path, descriptor: &Value, rows: &[Value]) -> Result<()> {
    validate_task_descriptor(root, descriptor, None)?;
    let relative = text(descriptor, "path")?;
    let path = root.join(relative);
    ensure!(
        rows.len() as u64 == unsigned(descriptor, "recordCount")?
            && candidate_set(rows)? == hash_of(descriptor, "candidateSetSha256")?,
        "task candidate sidecar descriptor/content drifted"
    );
    if path.exists() {
        validate_task_descriptor(root, descriptor, None)?;
    } else {
        fs::create_dir_all(
            path.parent()
                .context("task candidate sidecar has no parent")?,
        )?;
        let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
        let mut digest = Sha256::new();
        let mut size = 0u64;
        for row in rows {
            let bytes = canonical_json_line(row)?;
            f.write_all(&bytes)?;
            digest.update(&bytes);
            size += bytes.len() as u64;
        }
        f.sync_all()?;
        ensure!(
            format!("sha256:{:x}", digest.finalize()) == hash_of(descriptor, "rawSha256")?
                && size == unsigned(descriptor, "sizeBytes")?,
            "task candidate sidecar streamed bytes drifted"
        );
    }
    Ok(())
}
fn candidate_set(rows: &[Value]) -> Result<String> {
    canonical_sha256(&Value::Array(rows.iter().map(|r| Ok(json!({"candidateId":text(r,"candidateId")?,"candidateIdentitySha256":hash_of(r,"candidateIdentitySha256")?}))).collect::<Result<Vec<_>>>()?)).map_err(Into::into)
}
fn validate_task_descriptor(root: &Path, d: &Value, expected_path: Option<&str>) -> Result<()> {
    exact(
        d,
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
        "task candidate descriptor",
    )?;
    ensure!(
        text(d, "schemaVersion")? == "temporal_qd_v5_native_rich_candidate_jsonl_descriptor_v1"
            && text(d, "rowSchema")? == "temporal_qd_selected_rich_candidate_v1",
        "task candidate descriptor schema invalid"
    );
    self_hash(d, "descriptorSha256", "task candidate descriptor")?;
    let relative = text(d, "path")?;
    ensure!(
        !relative.is_empty()
            && !Path::new(relative).is_absolute()
            && Path::new(relative)
                .components()
                .all(|c| matches!(c, Component::Normal(_))),
        "task candidate descriptor path invalid"
    );
    if let Some(expected) = expected_path {
        ensure!(relative == expected, "task candidate descriptor path alias")
    }
    let path = safe_output_path(root, relative)?;
    if path.exists() {
        ensure!(
            path.is_file() && !fs::symlink_metadata(&path)?.file_type().is_symlink(),
            "task candidate sidecar symlink or non-file is forbidden"
        );
        let raw = fs::read(&path)?;
        ensure!(
            raw.len() as u64 == unsigned(d, "sizeBytes")?
                && format!("sha256:{:x}", Sha256::digest(&raw)) == hash_of(d, "rawSha256")?,
            "task candidate sidecar binding drifted"
        );
        let mut rows = Vec::new();
        for line in raw.split_inclusive(|b| *b == b'\n') {
            ensure!(
                line.ends_with(b"\n") && line.len() > 1,
                "task candidate sidecar row invalid"
            );
            let row: Value = serde_json::from_slice(&line[..line.len() - 1])?;
            ensure!(
                canonical_json_line(&row)? == line,
                "task candidate sidecar row noncanonical"
            );
            rows.push(row);
        }
        ensure!(
            rows.len() as u64 == unsigned(d, "recordCount")?
                && candidate_set(&rows)? == hash_of(d, "candidateSetSha256")?,
            "task candidate sidecar candidates drifted"
        );
    }
    Ok(())
}
fn validate_task_sidecars(root: &Path, result: &Value) -> Result<()> {
    let plan = member(result, "taskPlan")?;
    self_hash(plan, "taskPlanSha256", "v5 task plan")?;
    let round = unsigned(plan, "roundIndex")?;
    for task in array(plan, "tasks")? {
        self_hash(task, "taskSha256", "v5 task")?;
        let ord = unsigned(task, "taskOrdinal")?;
        let expected = format!("task-candidates/round-{round}-task-{ord}.jsonl");
        let descriptor = member(member(task, "cohortSelection")?, "candidateRows")?;
        ensure!(
            root.join(text(descriptor, "path")?).is_file(),
            "task candidate sidecar is missing"
        );
        ensure!(
            hash_of(descriptor, "candidateSetSha256")? == hash_of(task, "candidateSetSha256")?
                && hash_of(descriptor, "inputAuthoritySha256")?
                    == hash_of(plan, "semanticAuthoritySha256")?,
            "task candidate descriptor authority drifted"
        );
        validate_task_descriptor(root, descriptor, Some(&expected))?;
        let document_path = safe_output_path(root, text(task, "selectionDocumentRelativePath")?)?;
        let receipt_path = safe_output_path(root, text(task, "selectionReceiptRelativePath")?)?;
        ensure!(
            document_path.is_file() && receipt_path.is_file(),
            "v2 task selection handoff is incomplete"
        );
        ensure!(
            !fs::symlink_metadata(&document_path)?
                .file_type()
                .is_symlink()
                && !fs::symlink_metadata(&receipt_path)?
                    .file_type()
                    .is_symlink(),
            "v2 task selection symlink is forbidden"
        );
        let expected_document = selection_document(result, plan, task)?;
        let document = read_canonical(&document_path, "v2 task selection document")?;
        ensure!(
            document == expected_document,
            "v2 task selection document drifted"
        );
        let expected_receipt = selection_receipt(&document)?;
        let receipt = read_canonical(&receipt_path, "v2 task selection receipt")?;
        ensure!(
            receipt == expected_receipt,
            "v2 task selection receipt drifted"
        );
    }
    Ok(())
}
fn exact(v: &Value, keys: &[&str], name: &str) -> Result<()> {
    let m = v
        .as_object()
        .ok_or_else(|| anyhow!("{name} must be object"))?;
    ensure!(
        m.len() == keys.len() && keys.iter().all(|k| m.contains_key(*k)),
        "{name} has unknown or missing fields"
    );
    Ok(())
}
fn member<'a>(v: &'a Value, k: &str) -> Result<&'a Value> {
    v.get(k).ok_or_else(|| anyhow!("missing {k}"))
}
fn array<'a>(v: &'a Value, k: &str) -> Result<&'a Vec<Value>> {
    member(v, k)?
        .as_array()
        .ok_or_else(|| anyhow!("{k} must be array"))
}
fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    member(v, k)?
        .as_str()
        .ok_or_else(|| anyhow!("{k} must be string"))
}
fn unsigned(v: &Value, k: &str) -> Result<u64> {
    member(v, k)?
        .as_u64()
        .ok_or_else(|| anyhow!("{k} must be unsigned"))
}
fn hash_of<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    let x = text(v, k)?;
    ensure!(
        x.starts_with("sha256:") && x.len() == 71,
        "{k} must be sha256"
    );
    Ok(x)
}
fn self_hash(v: &Value, k: &str, n: &str) -> Result<()> {
    let computed = canonical_sha256_without_object_field(v, k)?;
    let supplied = hash_of(v, k)?;
    ensure!(
        computed == supplied,
        "{n} self hash drifted: computed {computed}, supplied {supplied}"
    );
    Ok(())
}
fn add(v: &mut Value, k: &str) -> Result<()> {
    let h = canonical_sha256(v)?;
    v.as_object_mut()
        .unwrap()
        .insert(k.into(), Value::String(h));
    Ok(())
}
fn read_canonical(p: &Path, n: &str) -> Result<Value> {
    let raw = fs::read(p)?;
    let v: Value = serde_json::from_slice(&raw).with_context(|| format!("parse {n}"))?;
    ensure!(
        canonical_json_line(&v)? == raw,
        "{n} must be canonical JSON plus LF"
    );
    Ok(v)
}
fn existing(p: &Path, n: &str) -> Result<PathBuf> {
    ensure!(p.is_file(), "{n} is not a file");
    ensure_no_symlink_components(p, n)?;
    ensure!(
        !fs::symlink_metadata(p)?.file_type().is_symlink(),
        "{n} symlink is forbidden"
    );
    let canonical = fs::canonicalize(p)?;
    ensure!(canonical.is_file(), "{n} canonical target is not a file");
    Ok(canonical)
}
fn ensure_no_symlink_components(path: &Path, name: &str) -> Result<()> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut cursor = PathBuf::new();
    for component in absolute.components() {
        cursor.push(component.as_os_str());
        if cursor.exists() {
            ensure!(
                !fs::symlink_metadata(&cursor)?.file_type().is_symlink(),
                "{name} path contains a symlink"
            );
        }
    }
    Ok(())
}
fn file_hash(p: &Path) -> String {
    format!(
        "sha256:{:x}",
        Sha256::digest(fs::read(p).unwrap_or_default())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn parent_summary_preserves_archive_accounting_and_sorted_cells() {
        let archive = json!({
            "archiveSha256": HASH,
            "candidateCountSeen": 7,
            "memberCount": 2,
            "cells": [
                {"cellId":"z","members":[{"candidateId":"b"}]},
                {"cellId":"a","members":[{"candidateId":"a"}]}
            ],
            "bidirectionalPairPolicy": {"schemaVersion":"fixture"}
        });
        let summary = derive_parent_summary(Some(archive)).unwrap();
        assert_eq!(
            text(&summary, "schemaVersion").unwrap(),
            PREVIOUS_PARENT_SUMMARY_SCHEMA
        );
        assert_eq!(unsigned(&summary, "candidateCountSeen").unwrap(), 7);
        assert_eq!(unsigned(&summary, "memberCount").unwrap(), 2);
        assert_eq!(
            array(&summary, "cellIds").unwrap(),
            &vec![json!("a"), json!("z")]
        );
        validate_parent_summary(&summary).unwrap();
    }

    #[test]
    fn parent_summary_rejects_incomplete_or_reordered_authority() {
        let mut incomplete = json!({
            "schemaVersion": PREVIOUS_PARENT_SUMMARY_SCHEMA,
            "archiveSha256": HASH,
            "memberCount": 0,
            "cellIds": []
        });
        add(&mut incomplete, "summarySha256").unwrap();
        assert!(validate_parent_summary(&incomplete).is_err());

        let mut reordered = json!({
            "schemaVersion": PREVIOUS_PARENT_SUMMARY_SCHEMA,
            "archiveSha256": HASH,
            "candidateCountSeen": 0,
            "memberCount": 0,
            "cellIds": ["z", "a"]
        });
        add(&mut reordered, "summarySha256").unwrap();
        assert!(validate_parent_summary(&reordered).is_err());
    }

    #[test]
    fn task_plan_relative_paths_use_portable_forward_slashes() {
        let path = Path::new("task-selections").join("round-0-task-0.selection.json");
        assert_eq!(
            portable_task_plan_relative_path(&path).unwrap(),
            "task-selections/round-0-task-0.selection.json"
        );
    }

    #[test]
    fn prior_panel_aggregate_cannot_replace_or_forge_current_panel_selection() {
        let current_raw = json!({
            "candidate": {
                "candidateId": "a",
                "candidateIdentitySha256": HASH,
                "programSha256": HASH,
                "profileSnapshotSha256": HASH
            },
            "descriptor": {"cellId": "current-cell"},
            "aggregate": {"totalConservativeNetR": 3.5}
        });
        let selected = candidate_with_member_selection_fields(&current_raw).unwrap();
        let backfill_raw = json!({
            "candidate": selected,
            "descriptor": {"cellId": "prior-cell"},
            "aggregate": {"totalConservativeNetR": -7.0}
        });
        let backfill =
            backfill_candidate_with_current_panel_selection_fields(&backfill_raw).unwrap();
        validate_backfill_candidate_projection(&selected, &backfill).unwrap();
        assert_eq!(backfill["cellId"], "current-cell");
        assert_eq!(backfill["currentPanelRank"], 3.5);

        let mut forged = backfill;
        forged["currentPanelRank"] = json!(-7.0);
        assert!(validate_backfill_candidate_projection(&selected, &forged).is_err());
    }
}
