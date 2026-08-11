//! Native v5 compact-tail to rotating-panel receipt production.
//!
//! This owns the formerly-Python-only bridge between a sealed v4 directional
//! tail and the pre-finalizer.  It reads no raw replay blob or stage
//! projection: all input facts are authenticated compact receipts.  Member
//! JSONL is streamed once and the retained state is one candidate plus its
//! bounded panel windows (besides the receipt being written).

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Map, Value, json};
use sha2::Digest;
use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_json_bytes, canonical_json_line, canonical_sha256,
    canonical_sha256_without_object_field,
};

use super::{array, exact_keys, file_sha, member, object, sha, text, unsigned};

pub const INPUT_SCHEMA: &str = "temporal_qd_v5_rotating_panel_bundle_input_v2";
pub const RECEIPT_SCHEMA: &str = "temporal_qd_v5_rotating_panel_bundle_receipt_v1";
pub const SIDECAR_DESCRIPTOR_SCHEMA: &str =
    "temporal_qd_v5_candidate_panel_bundle_sidecar_descriptor_v1";
pub const SIDECAR_RECEIPT_SCHEMA: &str = "temporal_qd_v5_candidate_panel_bundle_sidecar_receipt_v1";
pub const SIDECAR_RESULT_SCHEMA: &str = "temporal_qd_v5_candidate_panel_bundle_sidecar_result_v1";
const V4_INDEX_SCHEMA: &str = "temporal_qd_tail_result_index_v4";
const V4_ENTRY_SCHEMA: &str = "temporal_qd_tail_result_index_entry_v4";
const AUTHORITY_SCHEMA: &str = "temporal_qd_v5_directional_tail_authority_v1";
const TAIL_AUTHORITY_RECEIPT_SCHEMA: &str = "temporal_qd_tail_authority_receipt_v1";
const TAIL_RESULT_SCHEMA: &str = "temporal_qd_native_tail_reduction_result_v1";
const BUNDLE_SCHEMA: &str = "temporal_qd_candidate_panel_evidence_bundle_v1";
const WINDOW_SCHEMA: &str = "temporal_qd_candidate_window_evidence_v1";

/// Build a self-hashed panel receipt from only v4 compact evidence.
pub fn build(input: &Value) -> Result<Value> {
    validate_input(input)?;
    let generation = unsigned(input, "generationIndex")?;
    let authority = member(input, "directionalTailAuthority")?;
    validate_authority(authority, generation)?;
    let seal = member(input, "campaignSeal")?;
    validate_campaign_seal(seal)?;
    let tail = load_tail_authority(member(input, "tailAuthority")?)?;
    validate_tail_chain(&tail, seal, authority, generation)?;
    let panel = member(input, "panel")?;
    let windows = panel_windows(panel)?;
    let rotating = member(input, "rotatingEvidence")?;
    let rotating_sha = sha(rotating, "rotatingEvidenceSha256")?.to_owned();
    let candidates = stream_members(&tail.members_path, &tail.members_descriptor)?;
    ensure!(
        !candidates.is_empty(),
        "v5 campaign has no evaluated members"
    );
    let index = load_v4_index(member(input, "tailResultIndex")?, seal, authority, &tail)?;
    let bundles = build_bundles(&candidates, &index, panel, &windows, &rotating_sha)?;
    let descriptor = object(member(input, "tailResultIndex")?, "v4 tail descriptor")?;
    let mut source = json!({
        "schemaVersion": "temporal_qd_v5_rotating_compact_evidence_source_v1",
        "tailAuthority": authority,
        "tailResultIndex": {
            "schemaVersion": "temporal_qd_v5_tail_result_index_v4_descriptor_v1",
            "relativePath": text_from(descriptor, "relativePath", "v4 tail descriptor")?,
            "tailResultIndexSha256": sha(&index, "tailResultIndexSha256")?,
        },
    });
    add_hash(&mut source, "compactEvidenceSourceSha256")?;
    let mut receipt = json!({
        "schemaVersion": RECEIPT_SCHEMA,
        "role": text(input, "campaignRole")?,
        "campaignSeal": seal,
        "compactEvidenceSource": source,
        "candidatePanelBundles": bundles,
    });
    add_hash(&mut receipt, "receiptSha256")?;
    Ok(receipt)
}

/// Immutable output/restart boundary.  Reopening a committed receipt verifies
/// just its canonical self-hash and never reopens the campaign JSONL or index.
pub fn build_to_path(input_path: &Path, output_path: &Path) -> Result<Value> {
    let raw = fs::read(input_path).context("read v5 rotating panel input")?;
    let input: Value = serde_json::from_slice(&raw).context("parse v5 rotating panel input")?;
    ensure!(
        canonical_json_line(&input)? == raw,
        "v5 panel input must be canonical JSON plus LF"
    );
    validate_input(&input)?;
    if output_path.is_file() {
        let raw = fs::read(output_path).context("read committed v5 panel receipt")?;
        let receipt: Value =
            serde_json::from_slice(&raw).context("parse committed v5 panel receipt")?;
        ensure!(
            canonical_json_line(&receipt)? == raw,
            "committed v5 panel receipt must be canonical JSON plus LF"
        );
        ensure!(
            text(&receipt, "schemaVersion")? == RECEIPT_SCHEMA,
            "committed panel receipt schema drifted"
        );
        ensure!(
            canonical_sha256_without_object_field(&receipt, "receiptSha256")?
                == sha(&receipt, "receiptSha256")?,
            "committed panel receipt self hash drifted"
        );
        return Ok(receipt);
    }
    let receipt = build(&input)?;
    let bytes = canonical_json_line(&receipt)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    let mut output = options
        .open(output_path)
        .context("create v5 panel receipt")?;
    output.write_all(&bytes)?;
    output.sync_all()?;
    Ok(receipt)
}

/// Emits the rich panel bundles as their canonical LF-delimited transport
/// representation.  The companion receipt binds the sidecar to the sealed
/// panel input and to the existing rich panel receipt identity.  On restart,
/// only the small receipt and JSONL sidecar are opened; campaign inputs are
/// not replayed.
pub fn build_sidecar_to_path(
    input_path: &Path,
    sidecar_path: &Path,
    receipt_path: &Path,
) -> Result<Value> {
    let raw = fs::read(input_path).context("read v5 panel sidecar input")?;
    let input: Value = serde_json::from_slice(&raw).context("parse v5 panel sidecar input")?;
    ensure!(
        canonical_json_line(&input)? == raw,
        "v5 panel sidecar input must be canonical JSON plus LF"
    );
    validate_input(&input)?;
    let input_sha = sha(&input, "inputSha256")?.to_owned();
    if receipt_path.is_file() {
        let receipt = read_sidecar_receipt(receipt_path)?;
        ensure!(
            sha(&receipt, "inputSha256")? == input_sha,
            "immutable panel bundle sidecar input differs"
        );
        validate_sidecar_receipt(&receipt, sidecar_path)?;
        return sidecar_result(&input_sha, &receipt);
    }
    ensure!(
        !sidecar_path.exists(),
        "panel bundle sidecar exists without its receipt"
    );
    let panel_receipt = build(&input)?;
    let bundles = array(&panel_receipt, "candidatePanelBundles")?;
    let descriptor = write_sidecar(sidecar_path, bundles)?;
    let mut receipt = json!({
        "schemaVersion": SIDECAR_RECEIPT_SCHEMA,
        "inputSha256": input_sha,
        "panelReceiptSha256": sha(&panel_receipt, "receiptSha256")?,
        "campaignSealSha256": sha(member(&input, "campaignSeal")?, "campaignSealSha256")?,
        "tailAuthoritySha256": sha(member(&input, "directionalTailAuthority")?, "tailAuthoritySha256")?,
        "candidatePanelBundles": descriptor,
    });
    add_hash(&mut receipt, "receiptSha256")?;
    let bytes = canonical_json_line(&receipt)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(receipt_path)
        .context("create panel bundle sidecar receipt")?;
    output.write_all(&bytes)?;
    output.sync_all()?;
    sidecar_result(&input_sha, &receipt)
}

fn write_sidecar(path: &Path, bundles: &[Value]) -> Result<Value> {
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .context("create panel bundle sidecar")?;
    let mut hasher = sha2::Sha256::new();
    let mut size = 0usize;
    for bundle in bundles {
        validate_bundle_row(bundle)?;
        let bytes = canonical_json_line(bundle)?;
        output.write_all(&bytes)?;
        hasher.update(&bytes);
        size += bytes.len();
    }
    output.sync_all()?;
    let mut descriptor = json!({
        "schemaVersion": SIDECAR_DESCRIPTOR_SCHEMA,
        "path": path,
        "rawSha256": format!("sha256:{:x}", hasher.finalize()),
        "sizeBytes": size,
        "recordCount": bundles.len(),
        "rowSchema": BUNDLE_SCHEMA,
    });
    add_hash(&mut descriptor, "descriptorSha256")?;
    Ok(descriptor)
}

fn read_sidecar_receipt(path: &Path) -> Result<Value> {
    let raw = fs::read(path).context("read committed panel bundle sidecar receipt")?;
    let receipt: Value =
        serde_json::from_slice(&raw).context("parse panel bundle sidecar receipt")?;
    ensure!(
        canonical_json_line(&receipt)? == raw,
        "committed panel bundle sidecar receipt must be canonical JSON plus LF"
    );
    Ok(receipt)
}

fn validate_sidecar_receipt(receipt: &Value, expected_sidecar: &Path) -> Result<()> {
    exact_keys(
        object(receipt, "panel bundle sidecar receipt")?,
        &[
            "schemaVersion",
            "inputSha256",
            "panelReceiptSha256",
            "campaignSealSha256",
            "tailAuthoritySha256",
            "candidatePanelBundles",
            "receiptSha256",
        ],
        "panel bundle sidecar receipt",
    )?;
    ensure!(
        text(receipt, "schemaVersion")? == SIDECAR_RECEIPT_SCHEMA
            && canonical_sha256_without_object_field(receipt, "receiptSha256")?
                == sha(receipt, "receiptSha256")?,
        "panel bundle sidecar receipt is invalid"
    );
    for field in [
        "inputSha256",
        "panelReceiptSha256",
        "campaignSealSha256",
        "tailAuthoritySha256",
    ] {
        sha(receipt, field)?;
    }
    let descriptor = member(receipt, "candidatePanelBundles")?;
    exact_keys(
        object(descriptor, "panel bundle sidecar descriptor")?,
        &[
            "schemaVersion",
            "path",
            "rawSha256",
            "sizeBytes",
            "recordCount",
            "rowSchema",
            "descriptorSha256",
        ],
        "panel bundle sidecar descriptor",
    )?;
    ensure!(
        text(descriptor, "schemaVersion")? == SIDECAR_DESCRIPTOR_SCHEMA
            && text(descriptor, "rowSchema")? == BUNDLE_SCHEMA
            && text(descriptor, "path")? == expected_sidecar.to_string_lossy()
            && canonical_sha256_without_object_field(descriptor, "descriptorSha256")?
                == sha(descriptor, "descriptorSha256")?,
        "panel bundle sidecar descriptor is invalid"
    );
    let path = Path::new(text(descriptor, "path")?);
    ensure!(path.is_file(), "panel bundle sidecar is missing");
    let raw = fs::read(path)?;
    ensure!(
        raw.len() as u64 == unsigned(descriptor, "sizeBytes")?
            && file_sha(path)? == sha(descriptor, "rawSha256")?,
        "panel bundle sidecar file binding drifted"
    );
    let mut count = 0u64;
    let mut candidates = std::collections::BTreeSet::new();
    for line in raw.split_inclusive(|byte| *byte == b'\n') {
        ensure!(
            line.ends_with(b"\n") && line.len() > 1,
            "panel bundle sidecar has invalid row"
        );
        let row: Value = serde_json::from_slice(&line[..line.len() - 1])?;
        ensure!(
            canonical_json_line(&row)? == line,
            "panel bundle sidecar row is not canonical"
        );
        validate_bundle_row(&row)?;
        ensure!(
            candidates.insert(text(&row, "candidateId")?.to_owned()),
            "panel bundle sidecar repeats candidate"
        );
        count += 1;
    }
    ensure!(
        count == unsigned(descriptor, "recordCount")?,
        "panel bundle sidecar record count drifted"
    );
    Ok(())
}

fn validate_bundle_row(value: &Value) -> Result<()> {
    ensure!(
        text(value, "schemaVersion")? == BUNDLE_SCHEMA
            && canonical_sha256_without_object_field(value, "bundleSha256")?
                == sha(value, "bundleSha256")?,
        "panel bundle sidecar row is invalid"
    );
    Ok(())
}

fn sidecar_result(input_sha: &str, receipt: &Value) -> Result<Value> {
    let mut result = json!({
        "schemaVersion": SIDECAR_RESULT_SCHEMA,
        "inputSha256": input_sha,
        "receiptSha256": sha(receipt, "receiptSha256")?,
        "candidatePanelBundles": member(receipt, "candidatePanelBundles")?,
    });
    add_hash(&mut result, "resultSha256")?;
    Ok(result)
}

struct TailInput {
    root: PathBuf,
    value: Value,
    result: Value,
    members_path: PathBuf,
    members_descriptor: Value,
}

fn load_tail_authority(reference: &Value) -> Result<TailInput> {
    let reference_map = object(reference, "tail authority reference")?;
    exact_keys(
        reference_map,
        &["receiptPath", "receiptSha256"],
        "tail authority reference",
    )?;
    let supplied_path = PathBuf::from(text(reference, "receiptPath")?);
    ensure!(
        supplied_path.file_name().and_then(|name| name.to_str()) == Some("tail-authority.json")
            && !supplied_path
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir)),
        "tail authority receipt path is invalid"
    );
    let receipt_path = real_file(&supplied_path, "tail authority receipt")?;
    let root = receipt_path
        .parent()
        .context("tail authority receipt has no parent")?
        .to_path_buf();
    real_directory(&root, "tail authority receipt root")?;
    let raw = fs::read(&receipt_path)?;
    let value: Value = serde_json::from_slice(&raw).context("parse tail authority receipt")?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "tail authority receipt must be canonical JSON plus LF"
    );
    let map = object(&value, "tail authority receipt")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "generationIndex",
            "tailReductionManifestSha256",
            "evaluationPopulationSha256",
            "populationSha256",
            "tailResultIndexSha256",
            "taskMatrixSha256",
            "resultSetSha256",
            "runtimeAuthoritySha256",
            "tailReductionResult",
            "evaluatedMembers",
            "tailAuthoritySha256",
        ],
        "tail authority receipt",
    )?;
    ensure!(
        text(&value, "schemaVersion")? == TAIL_AUTHORITY_RECEIPT_SCHEMA
            && canonical_sha256_without_object_field(&value, "tailAuthoritySha256")?
                == sha(&value, "tailAuthoritySha256")?
            && sha(&value, "tailAuthoritySha256")? == sha(reference, "receiptSha256")?,
        "tail authority receipt identity drifted"
    );
    let result_descriptor = object(
        member(&value, "tailReductionResult")?,
        "tail reduction result descriptor",
    )?;
    exact_keys(
        result_descriptor,
        &["path", "rawSha256", "sizeBytes", "resultSha256"],
        "tail reduction result descriptor",
    )?;
    ensure!(
        text(member(&value, "tailReductionResult")?, "path")? == "tail-reduction-result.json",
        "tail reduction result path is not fixed"
    );
    let result_path = real_file(
        &root.join("tail-reduction-result.json"),
        "tail reduction result",
    )?;
    ensure!(
        result_path.parent() == Some(root.as_path()),
        "tail reduction result escaped its receipt root"
    );
    let result_raw = fs::read(&result_path)?;
    ensure!(
        result_raw.len() as u64 == unsigned(member(&value, "tailReductionResult")?, "sizeBytes")?
            && file_sha(&result_path)? == sha(member(&value, "tailReductionResult")?, "rawSha256")?,
        "tail reduction result raw binding drifted"
    );
    let result: Value =
        serde_json::from_slice(&result_raw).context("parse tail reduction result")?;
    ensure!(
        canonical_json_line(&result)? == result_raw
            && text(&result, "schemaVersion")? == TAIL_RESULT_SCHEMA
            && canonical_sha256_without_object_field(&result, "resultSha256")?
                == sha(&result, "resultSha256")?
            && sha(&result, "resultSha256")?
                == sha(member(&value, "tailReductionResult")?, "resultSha256")?,
        "tail reduction result identity drifted"
    );
    for (result_field, authority_field) in [
        ("generationIndex", "generationIndex"),
        ("manifestSha256", "tailReductionManifestSha256"),
        ("evaluationPopulationSha256", "evaluationPopulationSha256"),
        ("populationSha256", "populationSha256"),
        ("tailResultIndexSha256", "tailResultIndexSha256"),
        ("taskMatrixSha256", "taskMatrixSha256"),
        ("resultSetSha256", "resultSetSha256"),
        ("runtimeAuthoritySha256", "runtimeAuthoritySha256"),
    ] {
        ensure!(
            member(&result, result_field)? == member(&value, authority_field)?,
            "tail reduction result {result_field} binding drifted"
        );
    }
    let members_descriptor = member(&value, "evaluatedMembers")?.clone();
    let members_map = object(&members_descriptor, "evaluated members descriptor")?;
    exact_keys(
        members_map,
        &["path", "rawSha256", "sizeBytes", "recordCount"],
        "evaluated members descriptor",
    )?;
    ensure!(
        text(&members_descriptor, "path")? == "evaluated-members.jsonl",
        "evaluated members path is not fixed"
    );
    let members_path = real_file(&root.join("evaluated-members.jsonl"), "evaluated members")?;
    ensure!(
        members_path.parent() == Some(root.as_path())
            && fs::metadata(&members_path)?.len() == unsigned(&members_descriptor, "sizeBytes")?
            && file_sha(&members_path)? == sha(&members_descriptor, "rawSha256")?,
        "evaluated members raw binding drifted"
    );
    let result_members = member(member(&result, "evaluatedMembers")?, "membersFile")?;
    ensure!(
        member(result_members, "path")? == member(&members_descriptor, "path")?
            && member(result_members, "rawSha256")? == member(&members_descriptor, "rawSha256")?
            && member(result_members, "sizeBytes")? == member(&members_descriptor, "sizeBytes")?
            && member(result_members, "recordCount")?
                == member(&members_descriptor, "recordCount")?,
        "tail result/authority evaluated-member descriptor drifted"
    );
    Ok(TailInput {
        root,
        value,
        result,
        members_path,
        members_descriptor,
    })
}

fn validate_input(value: &Value) -> Result<()> {
    let map = object(value, "v5 rotating panel input")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "campaignRole",
            "campaignSeal",
            "tailAuthority",
            "tailResultIndex",
            "directionalTailAuthority",
            "rotatingEvidence",
            "panel",
            "inputSha256",
        ],
        "v5 rotating panel input",
    )?;
    ensure!(
        text(value, "schemaVersion")? == INPUT_SCHEMA
            && text(value, "contractVersion")? == CONTRACT_VERSION,
        "v5 rotating panel input schema/version is invalid"
    );
    ensure!(
        unsigned(value, "generationIndex")? > 0,
        "v5 rotating panel generation must be positive"
    );
    ensure!(
        matches!(
            text(value, "campaignRole")?,
            "proposal_current_panel" | "retained_parent_current_panel" | "prior_panel_backfill"
        ),
        "v5 rotating receipt role is invalid"
    );
    ensure!(
        canonical_sha256_without_object_field(value, "inputSha256")? == sha(value, "inputSha256")?,
        "v5 rotating panel input self hash drifted"
    );
    let descriptor = object(member(value, "tailResultIndex")?, "v4 tail descriptor")?;
    exact_keys(
        descriptor,
        &[
            "path",
            "relativePath",
            "rawSha256",
            "sizeBytes",
            "tailResultIndexSha256",
        ],
        "v4 tail descriptor",
    )?;
    let relative = text_from(descriptor, "relativePath", "v4 tail descriptor")?;
    ensure!(
        relative == "tail-result-index-v4.json",
        "v5 rotating receipt tail index path is invalid"
    );
    let authority_ref = object(member(value, "tailAuthority")?, "tail authority reference")?;
    exact_keys(
        authority_ref,
        &["receiptPath", "receiptSha256"],
        "tail authority reference",
    )?;
    sha(member(value, "tailAuthority")?, "receiptSha256")?;
    Ok(())
}

fn validate_authority(value: &Value, generation: u64) -> Result<()> {
    let map = object(value, "v5 tail authority")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "generationIndex",
            "runtimeAuthoritySha256",
            "tailResultIndexSchema",
            "tailResultEntrySchema",
            "rawRotatingProvenanceSchema",
            "tailAuthoritySha256",
        ],
        "v5 tail authority",
    )?;
    ensure!(
        text(value, "schemaVersion")? == AUTHORITY_SCHEMA
            && unsigned(value, "generationIndex")? == generation
            && text(value, "tailResultIndexSchema")? == V4_INDEX_SCHEMA
            && text(value, "tailResultEntrySchema")? == V4_ENTRY_SCHEMA
            && text(value, "rawRotatingProvenanceSchema")?
                == "temporal_qd_v5_raw_rotating_provenance_v1",
        "v5 tail authority is invalid"
    );
    ensure!(
        canonical_sha256_without_object_field(value, "tailAuthoritySha256")?
            == sha(value, "tailAuthoritySha256")?,
        "v5 tail authority self hash drifted"
    );
    sha(value, "runtimeAuthoritySha256")?;
    Ok(())
}

fn validate_campaign_seal(value: &Value) -> Result<()> {
    exact_keys(
        object(value, "v5 campaign seal")?,
        &[
            "schemaVersion",
            "contractVersion",
            "manifestSha256",
            "runtimeAuthoritySha256",
            "sourceSha256",
            "authorityId",
            "authoritySha256",
            "taskMatrixSha256",
            "taskManifestSha256",
            "checkpointSha256",
            "taskCount",
            "rawResultReadCount",
            "sourceResultBlobBytes",
            "sourceResultUncompressedBytes",
            "sourceResultSemanticBytes",
            "tailResultIndex",
            "rawResultInventory",
            "campaignSealSha256",
        ],
        "v5 campaign seal",
    )?;
    ensure!(
        text(value, "schemaVersion")? == "temporal_qd_campaign_seal_v1"
            && text(value, "contractVersion")? == CONTRACT_VERSION,
        "v5 rotating receipt lacks native campaign seal"
    );
    ensure!(
        canonical_sha256_without_object_field(value, "campaignSealSha256")?
            == sha(value, "campaignSealSha256")?,
        "v5 campaign seal self hash drifted"
    );
    let index = member(value, "tailResultIndex")?;
    ensure!(
        text(index, "path")? == "tail-result-index-v4.json",
        "v3/raw campaign seal index is forbidden on v5"
    );
    sha(index, "sha256")?;
    Ok(())
}

fn validate_tail_chain(
    tail: &TailInput,
    seal: &Value,
    authority: &Value,
    generation: u64,
) -> Result<()> {
    let receipt = &tail.value;
    let result = &tail.result;
    ensure!(
        unsigned(receipt, "generationIndex")? == generation
            && unsigned(result, "generationIndex")? == generation,
        "tail authority generation binding drifted"
    );
    ensure!(
        sha(receipt, "runtimeAuthoritySha256")? == sha(authority, "runtimeAuthoritySha256")?
            && sha(result, "runtimeAuthoritySha256")? == sha(authority, "runtimeAuthoritySha256")?
            && sha(seal, "runtimeAuthoritySha256")? == sha(authority, "runtimeAuthoritySha256")?,
        "tail authority runtime binding drifted"
    );
    ensure!(
        sha(receipt, "tailResultIndexSha256")? == sha(member(seal, "tailResultIndex")?, "sha256")?
            && sha(result, "tailResultIndexSha256")?
                == sha(member(seal, "tailResultIndex")?, "sha256")?,
        "tail authority campaign/index binding drifted"
    );
    ensure!(
        sha(receipt, "taskMatrixSha256")? == sha(seal, "taskMatrixSha256")?
            && sha(result, "taskMatrixSha256")? == sha(seal, "taskMatrixSha256")?,
        "tail authority campaign task-matrix binding drifted"
    );
    Ok(())
}

fn stream_members(path: &Path, descriptor_value: &Value) -> Result<BTreeMap<String, Value>> {
    let descriptor = object(descriptor_value, "members file")?;
    ensure!(
        fs::metadata(path)?.len() == unsigned_from(descriptor, "sizeBytes", "members file")?,
        "sealed evaluated members size drifted"
    );
    ensure!(
        file_sha(path)? == sha_from(descriptor, "rawSha256", "members file")?,
        "sealed evaluated members identity drifted"
    );
    let mut output = BTreeMap::new();
    let mut count = 0u64;
    for line in BufReader::new(File::open(path)?).lines() {
        let line = line?;
        ensure!(!line.is_empty(), "sealed evaluated members has blank row");
        let row: Value = serde_json::from_str(&line).context("parse sealed evaluated member")?;
        ensure!(
            canonical_json_line(&row)? == format!("{line}\n").as_bytes(),
            "sealed evaluated member is not canonical"
        );
        let candidate = member(&row, "candidate")?.clone();
        validate_candidate(&candidate)?;
        let id = text(&candidate, "candidateId")?.to_owned();
        ensure!(
            output.insert(id, candidate).is_none(),
            "sealed evaluated members repeats candidate"
        );
        count += 1;
    }
    ensure!(
        count == unsigned_from(descriptor, "recordCount", "members file")?,
        "sealed evaluated member count drifted"
    );
    Ok(output)
}

fn load_v4_index(
    descriptor: &Value,
    seal: &Value,
    authority: &Value,
    tail: &TailInput,
) -> Result<Value> {
    let supplied_path = Path::new(text(descriptor, "path")?);
    ensure!(
        !supplied_path
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir)),
        "v4 tail result index path traverses its authority root"
    );
    let path = real_file(supplied_path, "v4 tail result index")?;
    let expected_path = real_file(
        &tail.root.join("tail-result-index-v4.json"),
        "fixed v4 tail result index",
    )?;
    ensure!(
        path == expected_path && path.parent() == Some(tail.root.as_path()),
        "v4 tail result index is not the fixed authority sibling"
    );
    ensure!(
        fs::metadata(&path)?.len() == unsigned(descriptor, "sizeBytes")?,
        "v4 tail index size drifted"
    );
    ensure!(
        file_sha(&path)? == sha(descriptor, "rawSha256")?,
        "v4 tail index raw identity drifted"
    );
    let raw = fs::read(&path)?;
    let index: Value = serde_json::from_slice(&raw).context("parse v4 tail result index")?;
    ensure!(
        canonical_json_bytes(&index)? == raw,
        "v4 tail index must be canonical JSON without LF"
    );
    let root = object(&index, "v4 tail result index")?;
    exact_keys(
        root,
        &[
            "schemaVersion",
            "authorityId",
            "authoritySha256",
            "taskMatrixSha256",
            "taskManifestSha256",
            "checkpointSha256",
            "taskCount",
            "funnelProjectionIncluded",
            "sourceResultBlobBytes",
            "entries",
            "tailResultIndexSha256",
        ],
        "v4 tail result index",
    )?;
    ensure!(
        text(&index, "schemaVersion")? == V4_INDEX_SCHEMA,
        "v3/raw tail result index is forbidden on v5"
    );
    ensure!(
        canonical_sha256_without_object_field(&index, "tailResultIndexSha256")?
            == sha(&index, "tailResultIndexSha256")?,
        "v4 tail index self hash drifted"
    );
    ensure!(
        sha(&index, "tailResultIndexSha256")? == sha(member(seal, "tailResultIndex")?, "sha256")?
            && sha(&index, "tailResultIndexSha256")? == sha(descriptor, "tailResultIndexSha256")?
            && sha(&index, "tailResultIndexSha256")? == sha(&tail.value, "tailResultIndexSha256")?,
        "v5 campaign seal/index semantic binding drifted"
    );
    ensure!(
        unsigned(&index, "taskCount")? == array(&index, "entries")?.len() as u64,
        "v4 tail index accounting drifted"
    );
    ensure!(
        member(&index, "funnelProjectionIncluded")? == &Value::Bool(true),
        "v4 tail index lacks funnel projection"
    );
    // Enforce this is the exact directional authority and never an accidental
    // v4-shaped legacy index.
    ensure!(
        sha(&index, "taskMatrixSha256")? == sha(&tail.value, "taskMatrixSha256")?
            && sha(&index, "taskMatrixSha256")? == sha(seal, "taskMatrixSha256")?,
        "v5 tail index task-matrix binding drifted"
    );
    sha(&index, "authoritySha256")?;
    sha(authority, "tailAuthoritySha256")?;
    Ok(index)
}

fn build_bundles(
    candidates: &BTreeMap<String, Value>,
    index: &Value,
    panel: &Value,
    windows: &BTreeMap<String, (String, String)>,
    rotating_sha: &str,
) -> Result<Vec<Value>> {
    let mut evidence = BTreeMap::<String, BTreeMap<String, Value>>::new();
    for entry in array(index, "entries")? {
        let entry_map = object(entry, "v4 tail entry")?;
        let rejected = entry_map.contains_key("rejection");
        let expected = if rejected {
            &[
                "schemaVersion",
                "task",
                "rawResultRef",
                "rawTaskProvenance",
                "rejection",
                "entrySha256",
            ] as &[_]
        } else {
            &[
                "schemaVersion",
                "task",
                "rawResultRef",
                "rawTaskProvenance",
                "stageProjection",
                "rotatingEvidenceMetrics",
                "funnelProjection",
                "rawRotatingProvenance",
                "entrySha256",
            ] as &[_]
        };
        exact_keys(entry_map, expected, "v4 tail entry")?;
        ensure!(
            text(entry, "schemaVersion")? == V4_ENTRY_SCHEMA,
            "v4 tail entry schema is invalid"
        );
        ensure!(
            canonical_sha256_without_object_field(entry, "entrySha256")?
                == sha(entry, "entrySha256")?,
            "v4 tail entry self hash drifted"
        );
        let task = member(entry, "task")?;
        let candidate_id = text(task, "candidateId")?;
        let Some(candidate) = candidates.get(candidate_id) else {
            continue;
        };
        if rejected {
            continue;
        }
        let (window_id, _) = windows
            .iter()
            .find(|(_, (start, end))| {
                text(task, "analysisWindowStart").ok() == Some(start.as_str())
                    && text(task, "analysisWindowEnd").ok() == Some(end.as_str())
            })
            .map(|(id, bounds)| (id.clone(), bounds.clone()))
            .context("v4 tail task is outside panel")?;
        let metrics = member(entry, "rotatingEvidenceMetrics")?;
        ensure!(
            text(metrics, "sourceProfileSnapshotSha256")?
                == text(candidate, "profileSnapshotSha256")?,
            "v5 directional source profile binding drifted"
        );
        let record = window_record(candidate, panel, &window_id, task, metrics, entry, index)?;
        ensure!(
            evidence
                .entry(candidate_id.to_owned())
                .or_default()
                .insert(window_id, record)
                .is_none(),
            "v4 tail repeats candidate/window"
        );
    }
    ensure!(
        evidence.len() == candidates.len(),
        "v5 directional tail population coverage mismatch"
    );
    let mut bundles = Vec::with_capacity(candidates.len());
    for (candidate_id, candidate) in candidates {
        let rows = evidence
            .remove(candidate_id)
            .context("v5 directional candidate lacks complete panel coverage")?;
        ensure!(
            rows.len() == windows.len() && rows.keys().eq(windows.keys()),
            "v5 directional candidate lacks complete panel coverage"
        );
        bundles.push(bundle(
            candidate,
            panel,
            rows.into_values().collect(),
            rotating_sha,
        )?);
    }
    Ok(bundles)
}

fn window_record(
    candidate: &Value,
    panel: &Value,
    window_id: &str,
    task: &Value,
    metrics: &Value,
    entry: &Value,
    index: &Value,
) -> Result<Value> {
    let binding = candidate_binding(candidate)?;
    let (start, end) = panel_windows(panel)?
        .remove(window_id)
        .context("panel window is missing")?;
    ensure!(
        text(task, "analysisWindowStart")? == start && text(task, "analysisWindowEnd")? == end,
        "v4 tail window binding drifted"
    );
    let raw = member(entry, "rawResultRef")?;
    let provenance = member(entry, "rawTaskProvenance")?;
    ensure!(
        text(provenance, "taskId")? == text(task, "taskId")?
            && text(provenance, "resultSha256")? == text(raw, "resultSha256")?,
        "v4 raw task provenance drifted"
    );
    let mut raw_provenance = json!({
        "authorityId": text(index, "authorityId")?, "taskMatrixSha256": sha(index, "taskMatrixSha256")?,
        "taskId": text(provenance, "taskId")?, "resultSha256": sha(provenance, "resultSha256")?,
        "rawRotatingProvenanceSha256": canonical_sha256(member(entry, "rawRotatingProvenance")?)?,
    });
    let mut material = json!({
        "schemaVersion": WINDOW_SCHEMA,
        "candidateId": binding.id, "candidateIdentitySha256": binding.identity, "programSha256": binding.program,
        "rawSourceProfileSha256": binding.source_profile, "normalizedProfileSnapshotSha256": binding.snapshot,
        "panelId": text(panel, "panelId")?, "windowId": window_id, "analysisWindowStart": start, "analysisWindowEnd": end,
        "evidencePlanSemanticSha256": sha(task, "evidencePlanSemanticSha256")?, "metrics": metrics,
    });
    add_hash(&mut material, "evidenceDigestSha256")?;
    material
        .as_object_mut()
        .expect("object")
        .insert("rawTaskProvenance".into(), raw_provenance.take());
    add_hash(&mut material, "recordSha256")?;
    Ok(material)
}

fn bundle(
    candidate: &Value,
    panel: &Value,
    mut rows: Vec<Value>,
    rotating_sha: &str,
) -> Result<Value> {
    let binding = candidate_binding(candidate)?;
    rows.sort_by(|a, b| {
        text(a, "windowId")
            .unwrap_or("")
            .cmp(text(b, "windowId").unwrap_or(""))
    });
    let mut output = json!({
        "schemaVersion": BUNDLE_SCHEMA, "rotatingEvidenceSha256": rotating_sha,
        "candidateId": binding.id, "candidateIdentitySha256": binding.identity, "programSha256": binding.program,
        "rawSourceProfileSha256": binding.source_profile, "normalizedProfileSnapshotSha256": binding.snapshot,
        "panelId": text(panel, "panelId")?,
        "windowEvidenceDigests": rows.iter().map(|row| json!({"windowId": text(row, "windowId").unwrap(), "evidenceDigestSha256": sha(row, "evidenceDigestSha256").unwrap(), "recordSha256": sha(row, "recordSha256").unwrap()})).collect::<Vec<_>>(),
        "windowEvidence": rows,
    });
    let provenance = array(&output, "windowEvidence")?.iter().map(|row| Ok(json!({"windowId": text(row, "windowId")?, "authorityId": text(member(row, "rawTaskProvenance")?, "authorityId")?, "taskMatrixSha256": sha(member(row, "rawTaskProvenance")?, "taskMatrixSha256")?, "taskId": text(member(row, "rawTaskProvenance")?, "taskId")?, "resultSha256": sha(member(row, "rawTaskProvenance")?, "resultSha256")?, "rawRotatingProvenanceSha256": sha(member(row, "rawTaskProvenance")?, "rawRotatingProvenanceSha256")?}))).collect::<Result<Vec<_>>>()?;
    output
        .as_object_mut()
        .expect("object")
        .insert("rawTaskProvenance".into(), Value::Array(provenance));
    add_hash(&mut output, "bundleSha256")?;
    Ok(output)
}

struct Binding {
    id: String,
    identity: String,
    program: String,
    source_profile: String,
    snapshot: String,
}
fn candidate_binding(candidate: &Value) -> Result<Binding> {
    let id = text(candidate, "candidateId")?.to_owned();
    ensure!(!id.is_empty(), "candidate lacks candidateId");
    let identity = sha(candidate, "candidateIdentitySha256")?.to_owned();
    let program = sha(candidate, "programSha256")?.to_owned();
    let snapshot = sha(candidate, "profileSnapshotSha256")?.to_owned();
    let source_profile = candidate
        .get("sourceProfileSha256")
        .map(|_| sha(candidate, "sourceProfileSha256"))
        .transpose()?
        .unwrap_or(&snapshot)
        .to_owned();
    if candidate.get("sourceProfile").is_some() && candidate.get("sourceProfileSha256").is_some() {
        ensure!(
            canonical_sha256(member(candidate, "sourceProfile")?)? == source_profile,
            "candidate raw/authored source profile identity mismatch"
        );
    }
    Ok(Binding {
        id,
        identity,
        program,
        source_profile,
        snapshot,
    })
}
fn validate_candidate(candidate: &Value) -> Result<()> {
    let _ = candidate_binding(candidate)?;
    Ok(())
}

fn panel_windows(panel: &Value) -> Result<BTreeMap<String, (String, String)>> {
    ensure!(
        !text(panel, "panelId")?.is_empty(),
        "panel lacks panel identity"
    );
    let mut output = BTreeMap::new();
    for window in array(panel, "windows")? {
        let id = text(window, "windowId")?.to_owned();
        let start = text(window, "analysisWindowStart")?.to_owned();
        let end = text(window, "analysisWindowEnd")?.to_owned();
        ensure!(
            !id.is_empty() && start < end && output.insert(id, (start, end)).is_none(),
            "panel windows are invalid"
        );
    }
    ensure!(!output.is_empty(), "panel has no windows");
    Ok(output)
}
fn field<'a>(map: &'a Map<String, Value>, key: &str) -> Result<&'a Value> {
    map.get(key).ok_or_else(|| anyhow!("object lacks {key}"))
}
fn text_from<'a>(map: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a str> {
    field(map, key)?
        .as_str()
        .ok_or_else(|| anyhow!("{label} {key} must be string"))
}
fn unsigned_from(map: &Map<String, Value>, key: &str, label: &str) -> Result<u64> {
    field(map, key)?
        .as_u64()
        .ok_or_else(|| anyhow!("{label} {key} must be unsigned"))
}
fn sha_from<'a>(map: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a str> {
    let value = text_from(map, key, label)?;
    ensure!(
        value.starts_with("sha256:") && value.len() == 71,
        "{label} {key} must be digest"
    );
    Ok(value)
}
fn real_file(path: &Path, label: &str) -> Result<PathBuf> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("read {label}: {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "{label} must be a real regular file"
    );
    path.canonicalize()
        .with_context(|| format!("resolve {label}: {}", path.display()))
}
fn real_directory(path: &Path, label: &str) -> Result<PathBuf> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("read {label}: {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "{label} must be a real directory"
    );
    path.canonicalize()
        .with_context(|| format!("resolve {label}: {}", path.display()))
}
fn add_hash(value: &mut Value, field: &str) -> Result<()> {
    let hash = canonical_sha256(value)?;
    value
        .as_object_mut()
        .context("self-hashed value must be object")?
        .insert(field.into(), Value::String(hash));
    Ok(())
}
